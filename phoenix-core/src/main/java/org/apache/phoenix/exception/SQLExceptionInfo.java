/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.exception;

import java.sql.SQLException;

import org.apache.phoenix.util.SchemaUtil;


/**
 * Object serves as a closure of all coordinate information for SQLException messages.
 * 
 * 
 * @since 1.0
 */
public class SQLExceptionInfo {

    /**
     * Constants used in naming exception location.
     */
    public static final String SCHEMA_NAME = "schemaName";
    public static final String TABLE_NAME = "tableName";
    public static final String FAMILY_NAME = "familyName";
    public static final String COLUMN_NAME = "columnName";

    private final Throwable rootCause;
    private final SQLExceptionCode code; // Should always have one.
    private final String message;
    private final String schemaName;
    private final String tableName;
    private final String familyName;
    private final String columnName;

    public static class Builder {

        private Throwable rootCause;
        private SQLExceptionCode code; // Should always have one.
        private String message;
        private String schemaName;
        private String tableName;
        private String familyName;
        private String columnName;

        public Builder(SQLExceptionCode code) {
            this.code = code;
        }

        public Builder setRootCause(Throwable t) {
            this.rootCause = t;
            return this;
        }

        public Builder setMessage(String message) {
            this.message = message;
            return this;
        }

        public Builder setSchemaName(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        public Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder setFamilyName(String familyName) {
            this.familyName = familyName;
            return this;
        }

        public Builder setColumnName(String columnName) {
            this.columnName = columnName;
            return this;
        }

        public SQLExceptionInfo build() {
            return new SQLExceptionInfo(this);
        }

        @Override
        public String toString() {
            return code.toString();
        }
    }

    private SQLExceptionInfo(Builder builder) {
        code = builder.code;
        rootCause = builder.rootCause;
        message = builder.message;
        schemaName = builder.schemaName;
        tableName = builder.tableName;
        familyName = builder.familyName;
        columnName = builder.columnName;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(code.toString());
        if (message != null) {
            builder.append(" ").append(message);
        }
        String columnDisplayName = SchemaUtil.getMetaDataEntityName(schemaName, tableName, familyName, columnName);
        if (columnName != null) {
            builder.append(" ").append(COLUMN_NAME).append("=").append(columnDisplayName);
        } else if (familyName != null) {
            builder.append(" ").append(FAMILY_NAME).append("=").append(columnDisplayName);
        } else if (tableName != null) {
            builder.append(" ").append(TABLE_NAME).append("=").append(columnDisplayName);
        } else if (schemaName != null) {
            builder.append(" ").append(SCHEMA_NAME).append("=").append(columnDisplayName);
        }
        return builder.toString();
    }

    public SQLException buildException() {
        return code.getExceptionFactory().newException(this);
    }

    public Throwable getRootCause() {
        return rootCause;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getFamilyName() {
        return familyName;
    }

    public String getColumnName() {
        return columnName;
    }

    public SQLExceptionCode getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

}
