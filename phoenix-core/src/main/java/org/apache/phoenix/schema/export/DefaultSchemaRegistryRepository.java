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
package org.apache.phoenix.schema.export;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.EnvironmentEdgeManager;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Default in-memory implementation of SchemaRegistryRepository. Not intended for production use
 */
public class DefaultSchemaRegistryRepository implements SchemaRegistryRepository {
    public static final String DEFAULT_SCHEMA_NAME = "default_schema";
    public static final String DEFAULT_TENANT_ID = "global";

    private static final String SEPARATOR = "*";

    Map<String, String> schemaMap = new HashMap<String, String>();

    @Override
    public void init(Configuration conf) throws IOException {

    }

    @Override
    public String exportSchema(SchemaWriter writer, PTable table) throws IOException {
        String schemaId = getSchemaId(table);
        schemaMap.put(schemaId, writer.exportSchema(table));
        return schemaId;
    }

    @Override
    public String getSchemaById(String schemaId) throws IOException {
        return schemaMap.get(schemaId);
    }

    @Override
    public String getSchemaByTable(PTable table) throws IOException {
        return schemaMap.get(getSchemaId(table));
    }

    @Override
    public void close() throws IOException {
        schemaMap.clear();
    }

    public static String getSchemaId(PTable table) {
        String schemaMetadataName = getSchemaMetadataName(table);
        String version = table.getSchemaVersion() != null ? table.getSchemaVersion() :
            table.getLastDDLTimestamp() != null ? table.getLastDDLTimestamp().toString() :
                Long.toString(EnvironmentEdgeManager.currentTimeMillis());

        //tenant*schema*table*version-id
        return String.format("%s" + SEPARATOR + "%s", schemaMetadataName,
            version);
    }

    private static String getSchemaMetadataName(PTable table) {
        String schemaGroup = getSchemaGroup(table);
        return schemaGroup + SEPARATOR + table.getTableName().getString();
    }

    private static String getSchemaGroup(PTable table) {
        String tenantId = (table.getTenantId() != null) ? table.getTenantId().getString() :
            DEFAULT_TENANT_ID;
        String schemaName = table.getSchemaName().getString();
        if (schemaName == null) {
            schemaName = DEFAULT_SCHEMA_NAME;
        }
        return tenantId + SEPARATOR + schemaName;
    }
}
