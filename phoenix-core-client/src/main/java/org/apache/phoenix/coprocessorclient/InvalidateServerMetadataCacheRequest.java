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
package org.apache.phoenix.coprocessorclient;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.util.SchemaUtil;

public class InvalidateServerMetadataCacheRequest {
    private final byte[] tenantId;
    private final byte[] schemaName;
    private final byte[] tableName;

    public InvalidateServerMetadataCacheRequest(byte[] tenantId, byte[] schemaName,
                                                byte[] tableName) {
        this.tenantId = tenantId;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public byte[] getTenantId() {
        return tenantId;
    }

    public byte[] getSchemaName() {
        return schemaName;
    }

    public byte[] getTableName() {
        return tableName;
    }

    @Override
    public String toString() {
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        return "tenantId = " + Bytes.toString(tenantId)
                + ", table name = " + fullTableName;
    }
}
