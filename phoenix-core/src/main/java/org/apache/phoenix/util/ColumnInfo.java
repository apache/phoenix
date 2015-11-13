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
import org.apache.phoenix.schema.types.*;

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
  
    private final Integer precision;
    private final Integer scale;
    
    public static ColumnInfo create(String columnName, int sqlType, Integer maxLength, Integer scale) {
        if(scale != null) {
            assert(maxLength != null); // If we have a scale, we should always have a maxLength
            scale = Math.min(maxLength, scale);
            return new ColumnInfo(columnName, sqlType, maxLength, scale);
        }
        if (maxLength != null) {
            return new ColumnInfo(columnName, sqlType, maxLength);
        }
        return new ColumnInfo(columnName, sqlType);
    }
    
    public ColumnInfo(String columnName, int sqlType) {
        this(columnName, sqlType, null);
    }
    
    public ColumnInfo(String columnName, int sqlType, Integer maxLength) {
        this(columnName, sqlType, maxLength, null);
    }

    public ColumnInfo(String columnName, int sqlType, Integer precision, Integer scale) {
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

    // Return the proper SQL type string, taking into account possible array, length and scale parameters
    public String toTypeString() {
        return PhoenixRuntime.getSqlTypeName(getPDataType(), getMaxLength(), getScale());
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
        if (precision != that.precision) return false;
        if (scale != that.scale) return false;
        if (!columnName.equals(that.columnName)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = columnName.hashCode();
        result = 31 * result + (precision << 2) + (scale << 1) + sqlType;
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
                Lists.newArrayList(stringRepresentation.split(":", 2));

        if (components.size() != 2) {
            throw new IllegalArgumentException("Unparseable string: " + stringRepresentation);
        }

        String[] typeParts = components.get(0).split(" ");
        String columnName = components.get(1);

        Integer maxLength = null;
        Integer scale = null;
        if (typeParts[0].contains("(")) {
            Matcher matcher = Pattern.compile("([^\\(]+)\\((\\d+)(?:,(\\d+))?\\)").matcher(typeParts[0]);
            if (!matcher.matches() || matcher.groupCount() > 3) {
                throw new IllegalArgumentException("Unparseable type string: " + typeParts[0]);
            }
            maxLength = Integer.valueOf(matcher.group(2));
            if (matcher.group(3) != null) {
                scale = Integer.valueOf(matcher.group(3));
            }
            // Drop the (N) or (N,N) from the original type
            typeParts[0] = matcher.group(1);
        }

        // Create the PDataType from the sql type name, including the second 'ARRAY' part if present
        PDataType dataType;
        if(typeParts.length < 2) {
            dataType = PDataType.fromSqlTypeName(typeParts[0]);
        }
        else {
            dataType = PDataType.fromSqlTypeName(typeParts[0] + " " + typeParts[1]);
        }
                
        return ColumnInfo.create(columnName, dataType.getSqlType(), maxLength, scale);
    }
    
    public Integer getMaxLength() {
        return precision;
    }

    public Integer getPrecision() {
        return precision;
    }
    
    public Integer getScale() {
        return scale;
    }
}
