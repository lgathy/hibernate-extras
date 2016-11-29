package com.doctusoft.hibernate.extras;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;

import java.util.regex.*;

@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class MultiLineSqlInsert {
    
    String insertPart;
    String valuesPart;
    
    public static MultiLineSqlInsert tryParse(String sqlInsertString) {
        if (sqlInsertString == null) {
            // no insert statement to work on
            return null;
        }
        Matcher matcher = SPLITTER.matcher(sqlInsertString);
        if (!matcher.find()) {
            // non-conform insert statement cannot be transformed into multi-line format
            return null;
        }
        StringBuffer buf = new StringBuffer();
        matcher.appendReplacement(buf, matcher.group(1));
        String insertPart = buf.toString();
        buf.setLength(0);
        buf.append(matcher.group(2));
        if (matcher.find()) {
            // unexpected: multiple values part in the insert sql
            return null;
        }
        matcher.appendTail(buf);
        String valuesPart = buf.toString();
        return new MultiLineSqlInsert(insertPart, valuesPart);
    }
    
    public String createMultiLineInsertString(int countEntities) {
        int length = insertPart.length() + countEntities * (1 + valuesPart.length());
        StringBuilder builder = new StringBuilder(length);
        builder.append(insertPart);
        builder.append(valuesPart);
        for (int i = 1; i < countEntities; ++i) {
            builder.append(',').append(valuesPart);
        }
        return builder.toString();
    }
    
    static Pattern SPLITTER = Pattern.compile("(\\sVALUES)(\\s+[(])", Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);
    
}
