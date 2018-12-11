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
package org.apache.phoenix.spark.datasource.v2.writer;

import org.apache.spark.sql.types.StructType;

import java.io.Serializable;

public class PhoenixDataSourceWriteOptions implements Serializable {

    private final String tableName;
    private final String zkUrl;
    private final String tenantId;
    private final String scn;
    private final StructType schema;
    private final boolean skipNormalizingIdentifier;

    private PhoenixDataSourceWriteOptions(String tableName, String zkUrl, String scn, String tenantId,
                                          StructType schema, boolean skipNormalizingIdentifier) {
        this.tableName = tableName;
        this.zkUrl = zkUrl;
        this.scn = scn;
        this.tenantId = tenantId;
        this.schema = schema;
        this.skipNormalizingIdentifier = skipNormalizingIdentifier;
    }

    public String getScn() {
        return scn;
    }

    public String getZkUrl() {
        return zkUrl;
    }

    public String getTenantId() {
        return tenantId;
    }

    public StructType getSchema() {
        return schema;
    }

    public String getTableName() {
        return tableName;
    }

    public boolean skipNormalizingIdentifier() {
        return skipNormalizingIdentifier;
    }

    public static class Builder {
        private String tableName;
        private String zkUrl;
        private String scn;
        private String tenantId;
        private StructType schema;
        private boolean skipNormalizingIdentifier;

        public Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder setZkUrl(String zkUrl) {
            this.zkUrl = zkUrl;
            return this;
        }

        public Builder setScn(String scn) {
            this.scn = scn;
            return this;
        }

        public Builder setTenantId(String tenantId) {
            this.tenantId = tenantId;
            return this;
        }

        public Builder setSchema(StructType schema) {
            this.schema = schema;
            return this;
        }

        public Builder setSkipNormalizingIdentifier(boolean skipNormalizingIdentifier) {
            this.skipNormalizingIdentifier = skipNormalizingIdentifier;
            return this;
        }

        public PhoenixDataSourceWriteOptions build() {
            return new PhoenixDataSourceWriteOptions(tableName, zkUrl, scn, tenantId, schema, skipNormalizingIdentifier);
        }
    }
}