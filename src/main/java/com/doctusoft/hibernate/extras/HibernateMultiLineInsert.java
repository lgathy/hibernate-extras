package com.doctusoft.hibernate.extras;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.StaleStateException;
import org.hibernate.engine.internal.Versioning;
import org.hibernate.engine.jdbc.spi.JdbcCoordinator;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.ExecuteUpdateResultCheckStyle;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.id.*;
import org.hibernate.internal.AbstractSharedSessionContract;
import org.hibernate.jdbc.TooManyRowsAffectedException;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.entity.SingleTableEntityPersister;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.tuple.InMemoryValueGenerationStrategy;
import org.hibernate.tuple.ValueGenerator;
import org.hibernate.tuple.entity.EntityMetamodel;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.function.*;

import static java.util.Objects.*;

@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class HibernateMultiLineInsert {
    
    public static final HibernateMultiLineInsert lookup(EntityPersister persister) {
        requireNonNull(persister, "persister");
        if (!isSingleTable(persister)) {
            // there whould be too many special cases unsupported for multi-table multi-line inserts
            return null;
        }
        if (persister.getEntityMetamodel().isDynamicInsert()) {
            // shouldn't risk dynamic insertable entities due to DB-level default value inconsistencies
            // TODO this could by an option
            return null;
        }
        IdentifierGenerator identifierGenerator = persister.getIdentifierGenerator();
        if (!SUPPORTED_ID_GENERATORS.contains(identifierGenerator.getClass())) {
            // it's only safe to insert multiple lines in one statement, if the ids are set/generated prior insertion
            // (without calling the DB)
            return null;
        }
        EntityPersisterSpy persisterSpy = EntityPersisterSpy.spyOn(persister);
        if (persisterSpy.insertCallable) {
            // multi-line syntax is not applicable for custom stored procedure calls
            return null;
        }
        MultiLineSqlInsert sqlInsert = MultiLineSqlInsert.tryParse(persisterSpy.sqlInsertString);
        if (sqlInsert == null) {
            // some unknown insert syntax, e.G. a provided customSqlInsert
            return null;
        }
        return new HibernateMultiLineInsert(
            (SingleTableEntityPersister) persister,
            persisterSpy,
            identifierGenerator,
            sqlInsert);
    }
    
    private static boolean isSingleTable(EntityPersister persister) {
        if (persister instanceof SingleTableEntityPersister) {
            return ((SingleTableEntityPersister) persister).getTableSpan() == 1;
        }
        return false;
    }
    
    SingleTableEntityPersister persister;
    EntityPersisterSpy persisterSpy;
    IdentifierGenerator identifierGenerator;
    MultiLineSqlInsert sqlInsert;
    
    public void insertInBatch(Session session, Object[] entities) {
        
        int countEntities = entities.length;
        if (countEntities == 0) return;
        
        AbstractSharedSessionContract sessionImpl = (AbstractSharedSessionContract) session;
        EntityMetamodel entityMetamodel = persister.getEntityMetamodel();
        List<BiConsumer<Object, Object[]>> preInsertInMemoryValueGenerators = new ArrayList<>();
        if (entityMetamodel.isVersioned()) {
            preInsertInMemoryValueGenerators.add((entity, fields) -> {
                boolean substitute = Versioning.seedVersion(
                    fields,
                    persister.getVersionProperty(),
                    persister.getVersionType(),
                    sessionImpl
                );
                if (substitute) {
                    persister.setPropertyValues(entity, fields);
                }
            });
        }
        if (entityMetamodel.hasPreInsertGeneratedValues()) {
            InMemoryValueGenerationStrategy[] strategies = entityMetamodel.getInMemoryValueGenerationStrategies();
            for (int i = 0; i < strategies.length; i++) {
                InMemoryValueGenerationStrategy strategy = strategies[i];
                if (strategy != null && strategy.getGenerationTiming().includesInsert()) {
                    final int iAttr = i;
                    final ValueGenerator valueGenerator = strategy.getValueGenerator();
                    preInsertInMemoryValueGenerators.add((object, fields) -> {
                        fields[iAttr] = valueGenerator.generateValue(session, object);
                        persister.setPropertyValue(object, iAttr, fields[iAttr]);
                    });
                }
            }
        }
        
        String sql = sqlInsert.createMultiLineInsertString(countEntities);
        
        Serializable[] ids = new Serializable[countEntities];
        Object[][] fields = new Object[countEntities][];
        for (int i = 0; i < countEntities; ++i) {
            Object entity = entities[i];
            ids[i] = identifierGenerator.generate(sessionImpl, entity);
            fields[i] = persister.getPropertyValues(entity);
            Object[] entityFields = fields[i];
            preInsertInMemoryValueGenerators.forEach(generator -> generator.accept(entity, entityFields));
        }
        
        JdbcCoordinator jdbcCoordinator = sessionImpl.getJdbcCoordinator();
        boolean[] propertyInsertability = persister.getPropertyInsertability();
        try {
            boolean callable = false;
            PreparedStatement insert = jdbcCoordinator
                .getStatementPreparer()
                .prepareStatement(sql, callable);
            
            try {
                int idx = 1;
                // Write the values of fields onto the prepared statement - we MUST use the state at the time the
                // insert was issued (cos of foreign key constraints). Not necessarily the object's current state
                for (int i = 0; i < countEntities; ++i) {
                    idx = persisterSpy.dehydrate(sessionImpl, ids[i], fields[i], propertyInsertability, insert, idx);
                }
                int rowCount = jdbcCoordinator
                    .getResultSetReturn()
                    .executeUpdate(insert);
                if (countEntities > rowCount) {
                    throw new StaleStateException("Unexpected row count: " + rowCount + "; expected: " + countEntities);
                }
                if (countEntities < rowCount) {
                    String msg = "Unexpected row count: " + rowCount + "; expected: " + countEntities;
                    throw new TooManyRowsAffectedException(msg, countEntities, rowCount);
                }
            } finally {
                jdbcCoordinator.getLogicalConnection().getResourceRegistry().release(insert);
                jdbcCoordinator.afterStatementExecution();
            }
        } catch (SQLException e) {
            ServiceRegistryImplementor serviceRegistry = sessionImpl.getFactory().getServiceRegistry();
            throw serviceRegistry
                .getService(JdbcServices.class)
                .getSqlExceptionHelper()
                .convert(e, "could not insert: " + MessageHelper.infoString(persister), sql);
        }
    }
    
    @Value
    private static class EntityPersisterSpy {
        
        public static EntityPersisterSpy spyOn(EntityPersister persister) {
            return new EntityPersisterSpy(
                requireNonNull(persister),
                readField(persister, SQL_INSERT_STRINGS, String[].class)[0],
                readField(persister, INSERT_CALLABLE, boolean[].class)[0],
                readField(persister, INSERT_RESULT_CHECK_STYLES, ExecuteUpdateResultCheckStyle[].class)[0],
                readField(persister, PROPERTY_COLUMN_INSERTABLE, boolean[][].class)
            );
        }
        
        EntityPersister persister;
        String sqlInsertString;
        boolean insertCallable;
        ExecuteUpdateResultCheckStyle insertResultCheckStyle;
        boolean[][] propertyColumnInsertable;
        
        public int dehydrate(
            SharedSessionContractImplementor sessionImpl,
            Serializable id,
            Object[] fields,
            boolean[] includeProperty,
            PreparedStatement ps,
            int index) throws SQLException, HibernateException {
            Object rowId = null;
            int tableSpan = 0;
            boolean isUpdate = false;
            try {
                Object returnValue = DEHYDRATE.invoke(persister, id, fields, rowId, includeProperty,
                    propertyColumnInsertable, tableSpan, ps, sessionImpl, index, isUpdate);
                return (Integer) returnValue;
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
        
        static Class<?> CLASS = AbstractEntityPersister.class;
        
        static Method DEHYDRATE = lookupDehydrateMethod();
        
        private static Method lookupDehydrateMethod() {
            try {
                Method method = CLASS.getDeclaredMethod("dehydrate",
                    Serializable.class, // id
                    Object[].class, // fields
                    Object.class, // rowId = null
                    boolean[].class, // includeProperty
                    boolean[][].class, // includeColumns
                    int.class, // j alias: tableSpan = 0
                    PreparedStatement.class,
                    SharedSessionContractImplementor.class,
                    int.class, // index
                    boolean.class // isUpdate = false
                );
                method.setAccessible(true);
                return method;
            } catch (NoSuchMethodException e) {
                throw Throwables.propagate(e);
            }
        }
        
        static Field lookupField(String name) {
            try {
                Field field = CLASS.getDeclaredField(name);
                field.setAccessible(true);
                return field;
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
        
        static <T> T readField(Object object, Field field, Class<T> returnType) {
            try {
                Object value = field.get(object);
                return returnType.cast(value);
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
        
        static Field INSERT_CALLABLE = lookupField("insertCallable");
        static Field INSERT_RESULT_CHECK_STYLES = lookupField("insertResultCheckStyles");
        static Field PROPERTY_COLUMN_INSERTABLE = lookupField("propertyColumnInsertable");
        static Field SQL_INSERT_STRINGS = lookupField("sqlInsertStrings");
        
    }
    
    private static final ImmutableSet<Class<? extends IdentifierGenerator>> SUPPORTED_ID_GENERATORS = ImmutableSet.of(
        Assigned.class, GUIDGenerator.class, UUIDGenerator.class, UUIDHexGenerator.class
    );
    
}
