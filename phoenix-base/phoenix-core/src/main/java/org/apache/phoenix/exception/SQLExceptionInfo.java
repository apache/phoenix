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
    public static final String FUNCTION_NAME = "functionName";
    public static final String MAX_MUTATION_SIZE = "maxMutationSize";
    public static final String MUTATION_SIZE = "mutationSize";
    public static final String MAX_MUTATION_SIZE_BYTES = "maxMutationSizeBytes";
    public static final String MUTATION_SIZE_BYTES = "mutationSizeBytes";
    public static final String MAX_PHOENIX_COLUMN_SIZE_BYTES = "maxPhoenixColumnSizeBytes";
    public static final String PHOENIX_COLUMN_SIZE_BYTES = "phoenixColumnSizeBytes";

    private final Throwable rootCause;
    private final SQLExceptionCode code; // Should always have one.
    private final String message;
    private final String schemaName;
    private final String tableName;
    private final String familyName;
    private final String columnName;
    private final String functionName;
    private final int maxMutationSize;
    private final int mutationSize;
    private final long maxMutationSizeBytes;
    private final long mutationSizeBytes;
    private final int phoenixColumnSizeBytes;
    private final int maxPhoenixColumnSizeBytes;

    public static class Builder {

        private Throwable rootCause;
        private SQLExceptionCode code; // Should always have one.
        private String message;
        private String schemaName;
        private String tableName;
        private String familyName;
        private String columnName;
        private String functionName;
        private int maxMutationSize;
        private int mutationSize;
        private long maxMutationSizeBytes;
        private long mutationSizeBytes;
        private int phoenixColumnSizeBytes;
        private int maxPhoenixColumnSizeBytes;

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

        public Builder setFunctionName(String functionName) {
            this.functionName = functionName;
            return this;
        }

        public Builder setMaxMutationSize(int maxMutationSize) {
            this.maxMutationSize = maxMutationSize;
            return this;
        }

        public Builder setMutationSize(int mutationSize) {
            this.mutationSize = mutationSize;
            return this;
        }

        public Builder setMaxMutationSizeBytes(long maxMutationSizeBytes) {
            this.maxMutationSizeBytes = maxMutationSizeBytes;
            return this;
        }

        public Builder setMutationSizeBytes(long mutationSizeBytes) {
            this.mutationSizeBytes = mutationSizeBytes;
            return this;
        }

        public Builder setPhoenixColumnSizeBytes(int phoenixColumnSizeBytes) {
            this.phoenixColumnSizeBytes = phoenixColumnSizeBytes;
            return this;
        }

        public Builder setMaxPhoenixColumnSizeBytes(int maxPhoenixColumnSizeBytes) {
            this.maxPhoenixColumnSizeBytes = maxPhoenixColumnSizeBytes;
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
        functionName = builder.functionName;
        maxMutationSize = builder.maxMutationSize;
        mutationSize = builder.mutationSize;
        maxMutationSizeBytes = builder.maxMutationSizeBytes;
        mutationSizeBytes = builder.mutationSizeBytes;
        maxPhoenixColumnSizeBytes = builder.maxPhoenixColumnSizeBytes;
        phoenixColumnSizeBytes = builder.phoenixColumnSizeBytes;
    }

    @Override
    public String toString() {
        String baseMessage = code.toString();
        StringBuilder builder = new StringBuilder(baseMessage);
        if (message != null) {
            if (message.startsWith(baseMessage)) {
                builder.append(message.substring(baseMessage.length()));
            } else {
                builder.append(" ").append(message);
            }
        }
        if (functionName != null) {
            builder.append(" ").append(FUNCTION_NAME).append("=").append(functionName);
            return builder.toString();
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
        if (maxMutationSize != 0) {
            builder.append(" ").append(MAX_MUTATION_SIZE).append("=").append(maxMutationSize);
            builder.append(" ").append(MUTATION_SIZE).append("=").append(mutationSize);
        } else if (maxMutationSizeBytes != 0) {
            builder.append(" ").append(MAX_MUTATION_SIZE_BYTES).append("=").
                    append(maxMutationSizeBytes);
            builder.append(" ").append(MUTATION_SIZE_BYTES).append("=").append(mutationSizeBytes);
        }
        if (maxPhoenixColumnSizeBytes != 0) {
            builder.append(" ").append(MAX_PHOENIX_COLUMN_SIZE_BYTES).append("=").append(maxPhoenixColumnSizeBytes);
            builder.append(" ").append(PHOENIX_COLUMN_SIZE_BYTES).append("=").append(phoenixColumnSizeBytes);
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

    public String getFunctionName() {
        return functionName;
    }
    
    public SQLExceptionCode getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public int getMaxMutationSize() {
        return maxMutationSize;
    }

    public int getMutationSize() {
        return mutationSize;
    }

    public long getMaxMutationSizeBytes() {
        return maxMutationSizeBytes;
    }

    public long getMutationSizeBytes() {
        return mutationSizeBytes;
    }

    public int getMaxPhoenixColumnSizeBytes() {
        return maxPhoenixColumnSizeBytes;
    }

    public int getPhoenixColumnSizeBytes() {
        return phoenixColumnSizeBytes;
    }
}
