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
package org.apache.phoenix.end2end;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.hadoop.hbase.client.Table;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MutationCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;

public class MetaDataEndPointIT extends ParallelStatsDisabledIT {
    @Test
	public void testUpdateIndexState() throws Throwable {
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        String indexName1 = generateUniqueName();
        final String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullIndexName1 = SchemaUtil.getTableName(schemaName, indexName1);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + fullTableName + "(k INTEGER PRIMARY KEY, v1 INTEGER, v2 INTEGER) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true, GUIDE_POSTS_WIDTH=1000");
            conn.createStatement().execute("CREATE INDEX " + indexName1 + " ON " + fullTableName + " (v1) INCLUDE (v2)");
            conn.commit();
            Table metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            MutationCode code = IndexUtil.updateIndexState(fullIndexName1, 0L, metaTable, PIndexState.DISABLE).getMutationCode();
            assertEquals(MutationCode.TABLE_ALREADY_EXISTS, code);
            long ts = EnvironmentEdgeManager.currentTimeMillis();
            code = IndexUtil.updateIndexState(fullIndexName1, ts, metaTable, PIndexState.INACTIVE).getMutationCode();
            assertEquals(MutationCode.UNALLOWED_TABLE_MUTATION, code);
        }
	}
}
