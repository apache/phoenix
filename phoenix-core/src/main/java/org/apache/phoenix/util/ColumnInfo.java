/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */

package org.apache.phoenix.util;

import java.sql.Types;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PVarchar;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * ColumnInfo used to store Column Name and its associated PDataType
 */
public class ColumnInfo {

    /** Separator used for the toString representation */
    private static final String STR_SEPARATOR = ":";

    private final String columnName;
    private final int sqlType;
  
    private final int precision;
    private final int scale;
    
    public ColumnInfo(String columnName, int sqlType) {
        this(columnName, sqlType, -1);
    }
    
    public ColumnInfo(String columnName, int sqlType, int maxLength) {
        this(columnName, sqlType, maxLength, -1);
    }

    public ColumnInfo(String columnName, int sqlType, int precision, int scale) {
        Preconditions.checkNotNull(columnName, "columnName cannot be null");
        Preconditions.checkArgument(!columnName.isEmpty(), "columnName cannot be empty");
        if(!columnName.startsWith(SchemaUtil.ESCAPE_CHARACTER)) {
            columnName = SchemaUtil.getEscapedFullColumnName(columnName);
        }
        this.columnName = columnName;
        this.sqlType = sqlType;
        this.precision = precision;
        this.scale = scale;
    }

    public String getColumnName() {
        return columnName;
    }

    public int getSqlType() {
        return sqlType;
    }

    public PDataType getPDataType() {
        return PDataType.fromTypeId(sqlType);
    }
    
    /**
     * Returns the column name without the associated Column Family. 
     * @return
     */
    public String getDisplayName() {
    	final String unescapedColumnName = SchemaUtil.getUnEscapedFullColumnName(columnName);
        int index = unescapedColumnName.indexOf(QueryConstants.NAME_SEPARATOR);
        if (index < 0) {
            return unescapedColumnName; 
        }
        return unescapedColumnName.substring(index+1).trim();
    }
    
    public String toTypeString() {
        PDataType dataType = getPDataType();
        if (precision < 0) {
            return dataType.getSqlTypeName();
        }
        if (dataType == PDecimal.INSTANCE) {
            StringBuilder builder = new StringBuilder();
            builder.append(dataType.getSqlTypeName());
            builder.append('(').append(precision).append(',');
            builder.append(scale < 0 ? 0 : Math.min(precision, scale)).append(')');
            return builder.toString();
        }
        if (dataType == PChar.INSTANCE || dataType == PVarchar.INSTANCE) {
            StringBuilder builder = new StringBuilder();
            builder.append(dataType.getSqlTypeName());
            builder.append('(').append(precision).append(')');
          return builder.toString();
        }
        return dataType.getSqlTypeName();
    }

    @Override
    public String toString() {
        return toTypeString() + STR_SEPARATOR + columnName ;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ColumnInfo that = (ColumnInfo) o;

        if (sqlType != that.sqlType) return false;
        if (!columnName.equals(that.columnName)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = columnName.hashCode();
        result = 31 * result + sqlType;
        return result;
    }

    /**
     * Instantiate a {@code ColumnInfo} from a string representation created by the {@link
     * #toString()} method.
     *
     * @param stringRepresentation string representation of a ColumnInfo
     * @return the corresponding ColumnInfo
     * @throws java.lang.IllegalArgumentException if the given string representation cannot be
     * parsed
     */
    public static ColumnInfo fromString(String stringRepresentation) {
        List<String> components =
                Lists.newArrayList(stringRepresentation.split(":",2));
        
        if (components.size() != 2) {
            throw new IllegalArgumentException("Unparseable string: " + stringRepresentation);
        }

        String typePart = components.get(0);
        String columnName = components.get(1);
        if (!typePart.contains("(")) {
            return new ColumnInfo(
                    columnName,
                    PDataType.fromSqlTypeName(typePart).getSqlType());
        }
        Matcher matcher = Pattern.compile("([^\\(]+)\\((\\d+)(?:,(\\d+))?\\)").matcher(typePart);
        if (!matcher.matches() || matcher.groupCount() > 3) {
            throw new IllegalArgumentException("Unparseable type string: " + typePart);
        }
        int sqlType = PDataType.fromSqlTypeName(matcher.group(1)).getSqlType();
        if (matcher.group(3) == null) {
            assert sqlType == Types.CHAR || sqlType == Types.VARCHAR;
            int maxLength = Integer.parseInt(matcher.group(2));
            return new ColumnInfo(columnName, sqlType, maxLength);
        }
        assert sqlType == Types.DECIMAL;
        int precision = Integer.parseInt(matcher.group(2));
        int scale = Integer.parseInt(matcher.group(3));
        return new ColumnInfo(columnName, sqlType, precision, scale);
    }
    
    public int getMaxLength() {
        return precision;
    }

    public int getPrecision() {
        return precision;
    }
    
    public int getScale() {
        return scale;
    }
}
