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
package org.apache.phoenix.coprocessor;

public class InvalidateServerMetadataCacheRequest {
    private final byte[] tenantId;
    private final byte[] schema;
    private final byte[] name;

    public InvalidateServerMetadataCacheRequest(byte[] tenantId, byte[] schema, byte[] name) {
        this.tenantId = tenantId;
        this.schema = schema;
        this.name = name;
    }

    public byte[] getTenantId() {
        return tenantId;
    }

    public byte[] getSchemaName() {
        return schema;
    }

    public byte[] getTableName() {
        return name;
    }
}
