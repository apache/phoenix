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

import java.util.List;

import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.types.PDataType;

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

    public ColumnInfo(String columnName, int sqlType) {
        Preconditions.checkNotNull(columnName, "columnName cannot be null");
        Preconditions.checkArgument(!columnName.isEmpty(), "columnName cannot be empty");
        if(!columnName.startsWith(SchemaUtil.ESCAPE_CHARACTER)) {
            columnName = SchemaUtil.getEscapedFullColumnName(columnName);
        }
        this.columnName = columnName;
        this.sqlType = sqlType;
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

    @Override
    public String toString() {
        return getPDataType().getSqlTypeName() + STR_SEPARATOR + columnName ;
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

        return new ColumnInfo(
                components.get(1),
                PDataType.fromSqlTypeName(components.get(0)).getSqlType());
    }

}
