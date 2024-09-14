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
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.metrics2.lib.DynamicMetricsRegistry;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol.MutationCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.monitoring.IndexMetricsIT;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.metrics.MetricsMetadataSource;
import org.apache.phoenix.schema.metrics.MetricsMetadataSourceFactory;
import org.apache.phoenix.schema.metrics.MetricsMetadataSourceImpl;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
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

    @Test
    public void testMetadataMetricsOfCreateTable() throws Throwable {
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        final String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl())) {
            MetricsMetadataSourceImpl metricsSource = (MetricsMetadataSourceImpl) MetricsMetadataSourceFactory.getMetadataMetricsSource();
            DynamicMetricsRegistry registry = metricsSource.getMetricsRegistry();
            long expectedCreateTableCount =
                    IndexMetricsIT.getCounterValueByName(MetricsMetadataSource.CREATE_TABLE_COUNT, registry);
            long expectedCacheUsedSize =
                    IndexMetricsIT.getCounterValueByName(MetricsMetadataSource.METADATA_CACHE_ESTIMATED_USED_SIZE, registry);
            long expectedCacheAddCount =
                    IndexMetricsIT.getCounterValueByName(MetricsMetadataSource.METADATA_CACHE_ADD_COUNT, registry);

            String ddl = "CREATE TABLE " + fullTableName + "(k INTEGER PRIMARY KEY, v1 INTEGER, " +
                    "v2 INTEGER) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true, GUIDE_POSTS_WIDTH=1000";
            conn.createStatement().execute(ddl);

            expectedCreateTableCount += 1;
            IndexMetricsIT.verifyCounterWithValue(MetricsMetadataSource.CREATE_TABLE_COUNT,
                    registry, expectedCreateTableCount);

            PTable table = conn.getTable(fullTableName);
            expectedCacheUsedSize += table.getEstimatedSize();
            IndexMetricsIT.verifyCounterWithValue(MetricsMetadataSource.METADATA_CACHE_ESTIMATED_USED_SIZE,
                    registry, expectedCacheUsedSize);

            expectedCacheAddCount += 1;
            IndexMetricsIT.verifyCounterWithValue(MetricsMetadataSource.METADATA_CACHE_ADD_COUNT,
                    registry, expectedCacheAddCount);
        }
    }

    @Test
    public void testMetadataMetricsOfDropTable() throws Throwable {
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        final String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl())) {
            MetricsMetadataSourceImpl metricsSource = (MetricsMetadataSourceImpl) MetricsMetadataSourceFactory.getMetadataMetricsSource();
            DynamicMetricsRegistry registry = metricsSource.getMetricsRegistry();

            String ddl = "CREATE TABLE " + fullTableName + "(k INTEGER PRIMARY KEY, v1 INTEGER, " +
                    "v2 INTEGER) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true, GUIDE_POSTS_WIDTH=1000";
            conn.createStatement().execute(ddl);

            long expectedDropTableCount =
                    IndexMetricsIT.getCounterValueByName(MetricsMetadataSource.DROP_TABLE_COUNT, registry);
            long expectedCacheUsedSize =
                    IndexMetricsIT.getCounterValueByName(MetricsMetadataSource.METADATA_CACHE_ESTIMATED_USED_SIZE, registry);
            PTable table = conn.getTable(fullTableName);

            ddl = "DROP TABLE " + fullTableName;
            conn.createStatement().execute(ddl);

            expectedDropTableCount += 1;
            IndexMetricsIT.verifyCounterWithValue(MetricsMetadataSource.DROP_TABLE_COUNT,
                    registry, expectedDropTableCount);

            expectedCacheUsedSize -= table.getEstimatedSize();
            IndexMetricsIT.verifyCounterWithValue(MetricsMetadataSource.METADATA_CACHE_ESTIMATED_USED_SIZE,
                    registry, expectedCacheUsedSize);
        }
    }

    @Test
    public void testMetadataMetricsOfAlterTableAddCol() throws Throwable {
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        final String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl())) {
            MetricsMetadataSourceImpl metricsSource = (MetricsMetadataSourceImpl) MetricsMetadataSourceFactory.getMetadataMetricsSource();
            DynamicMetricsRegistry registry = metricsSource.getMetricsRegistry();

            String ddl = "CREATE TABLE " + fullTableName +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string)) ";
            conn.createStatement().execute(ddl);

            long expectedDropTableCount =
                    IndexMetricsIT.getCounterValueByName(MetricsMetadataSource.ALTER_ADD_COLUMN_COUNT, registry);
            long prevCacheUsedSize =
                    IndexMetricsIT.getCounterValueByName(MetricsMetadataSource.METADATA_CACHE_ESTIMATED_USED_SIZE, registry);

            ddl = "ALTER TABLE " + fullTableName + " ADD b_string VARCHAR  NULL PRIMARY KEY  ";
            conn.createStatement().execute(ddl);

            expectedDropTableCount += 1;
            IndexMetricsIT.verifyCounterWithValue(MetricsMetadataSource.ALTER_ADD_COLUMN_COUNT,
                    registry, expectedDropTableCount);

            long currCacheUsedSize =
                    IndexMetricsIT.getCounterValueByName(MetricsMetadataSource.METADATA_CACHE_ESTIMATED_USED_SIZE, registry);
            assertTrue(currCacheUsedSize >= prevCacheUsedSize);
        }
    }
}
