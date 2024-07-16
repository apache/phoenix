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
package org.apache.phoenix.cache;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessorclient.metrics.MetricsMetadataCachingSource;
import org.apache.phoenix.coprocessorclient.metrics.MetricsPhoenixCoprocessorSourceFactory;
import org.apache.phoenix.end2end.IndexToolIT;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.PhoenixRegionServerEndpointTestImpl;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.monitoring.GlobalClientMetrics;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static org.apache.hadoop.hbase.coprocessor.CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY;
import static org.apache.phoenix.query.ConnectionQueryServicesImpl.INVALIDATE_SERVER_METADATA_CACHE_EX_MESSAGE;
import static org.apache.phoenix.query.QueryServices.PHOENIX_METADATA_INVALIDATE_CACHE_ENABLED;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

// End to end tests for metadata caching re-design.
@Category(NeedsOwnMiniClusterTest.class)
public class ServerMetadataCacheIT extends ParallelStatsDisabledIT {

    private final Random RANDOM = new Random(42);

    private static ServerName serverName;

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(QueryServices.DEFAULT_UPDATE_CACHE_FREQUENCY_ATRRIB, "NEVER");
        props.put(QueryServices.LAST_DDL_TIMESTAMP_VALIDATION_ENABLED, Boolean.toString(true));
        props.put(PHOENIX_METADATA_INVALIDATE_CACHE_ENABLED, Boolean.toString(true));
        props.put(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB,
                Long.toString(Long.MAX_VALUE));
        props.put(QueryServices.TASK_HANDLING_INITIAL_DELAY_MS_ATTRIB,
                Long.toString(Long.MAX_VALUE));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
        assertEquals(1, getUtility().getHBaseCluster().getNumLiveRegionServers());
        serverName = getUtility().getHBaseCluster().getRegionServer(0).getServerName();
    }

    @Before
    public void resetMetrics() {
        GlobalClientMetrics.GLOBAL_CLIENT_STALE_METADATA_CACHE_EXCEPTION_COUNTER.getMetric().reset();
    }

    @After
    public void resetMetadataCache() {
        ServerMetadataCacheTestImpl.resetCache();
    }

    /**
     * Get the server metadata cache instance from the endpoint loaded on the region server.
     */
    private ServerMetadataCacheTestImpl getServerMetadataCache() {
        String phoenixRegionServerEndpoint = config.get(REGIONSERVER_COPROCESSOR_CONF_KEY);
        assertNotNull(phoenixRegionServerEndpoint);
        RegionServerCoprocessor coproc = getUtility().getHBaseCluster()
                .getRegionServer(0)
                .getRegionServerCoprocessorHost()
                .findCoprocessor(phoenixRegionServerEndpoint);
        assertNotNull(coproc);
        ServerMetadataCache cache = ((PhoenixRegionServerEndpointTestImpl)coproc).getServerMetadataCache();
        assertNotNull(cache);
        return (ServerMetadataCacheTestImpl)cache;
    }

    /**
     * Make sure cache is working fine for base table.
     * @throws Exception
     */
    @Test
    public void testCacheForBaseTable() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableNameStr =  generateUniqueName();
        PTable pTable;
        // use a spyed ConnectionQueryServices so we can verify calls to getTable
        ConnectionQueryServices spyCQS = Mockito.spy(driver.getConnectionQueryServices(getUrl(),
                PropertiesUtil.deepCopy(TEST_PROPERTIES)));
        try(Connection conn = spyCQS.connect(getUrl(), props)) {
            conn.setAutoCommit(false);
            // Create a test table.
            createTable(conn, tableNameStr);
            pTable = PhoenixRuntime.getTableNoCache(conn,
                    tableNameStr);// --> First call to CQSI#getTable
            ServerMetadataCacheTestImpl cache = getServerMetadataCache();
            // Override the connection to use in ServerMetadataCache
            cache.setConnectionForTesting(conn);
            byte[] tableName = Bytes.toBytes(tableNameStr);
            long lastDDLTimestampFromCache = cache.getLastDDLTimestampForTable(
                    null, null, tableName); // --> Second call to CQSI#getTable
            // Make sure the lastDDLTimestamp are the same.
            assertEquals(pTable.getLastDDLTimestamp().longValue(), lastDDLTimestampFromCache);
            // Verify that we made 2 calls to CQSI#getTable.
            verify(spyCQS, times(2)).getTable(
                    any(), any(),  eq(tableName), anyLong(), anyLong());
            // Make the same call 2 times to make sure it returns from the cache.
            cache.getLastDDLTimestampForTable(null, null, tableName);
            cache.getLastDDLTimestampForTable(null, null, tableName);
            // Both the above 2 calls were served from the cache.
            verify(spyCQS, times(2)).getTable(
                    any(), any(),  eq(tableName), anyLong(), anyLong());
        }
    }

    /**
     * Make sure cache is working fine for global view.
     * @throws Exception
     */
    @Test
    public void testCacheForGlobalView() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableNameStr =  generateUniqueName();
        PTable viewTable;
        // use a spyed ConnectionQueryServices so we can verify calls to getTable
        ConnectionQueryServices spyCQS = Mockito.spy(driver.getConnectionQueryServices(getUrl(),
                PropertiesUtil.deepCopy(TEST_PROPERTIES)));
        try (Connection conn = spyCQS.connect(getUrl(), props)) {
            conn.setAutoCommit(false);
            // Create a test table.
            createTable(conn, tableNameStr);
            // Create view on table.
            String whereClause = " WHERE v1 = 1000";
            String viewNameStr = generateUniqueName();
            createViewWhereClause(conn, tableNameStr, viewNameStr, whereClause);
            viewTable = PhoenixRuntime.getTableNoCache(conn, viewNameStr);  // --> First call to CQSI#getTable
            ServerMetadataCacheTestImpl cache = getServerMetadataCache();;
            // Override the connection to use in ServerMetadataCache
            cache.setConnectionForTesting(conn);

            long lastDDLTimestampFromCache = cache.getLastDDLTimestampForTable(
                    null, null, Bytes.toBytes(viewNameStr)); // --> Second call to CQSI#getTable

            byte[] viewNameBytes = Bytes.toBytes(viewNameStr);
            // Make sure the lastDDLTimestamp are the same.
            assertEquals(viewTable.getLastDDLTimestamp().longValue(), lastDDLTimestampFromCache);
            // Verify that we made 2 calls to CQSI#getTable.
            verify(spyCQS, times(2)).getTable(
                    any(), any(),  eq(viewNameBytes), anyLong(), anyLong());
            // Make the same call 2 times to make sure it returns from the cache.
            cache.getLastDDLTimestampForTable(null, null, viewNameBytes);
            cache.getLastDDLTimestampForTable(null, null, viewNameBytes);
            verify(spyCQS, times(2)).getTable(
                    any(), any(),  eq(viewNameBytes), anyLong(), anyLong());
        }
    }

    /**
     * Make sure cache is working fine for tenant view.
     * @throws Exception
     */
    @Test
    public void testCacheForTenantView() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableNameStr = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            // Create a test table.
            createTable(conn, tableNameStr);
        }
        String tenantId = "T_" + generateUniqueName();
        Properties tenantProps = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        PTable tenantViewTable;
        // Create view on table.
        String whereClause = " WHERE v1 = 1000";
        String tenantViewNameStr = generateUniqueName();
        ConnectionQueryServices spyCQS = Mockito.spy(driver.getConnectionQueryServices(getUrl(),
                PropertiesUtil.deepCopy(TEST_PROPERTIES)));
        try (Connection conn = spyCQS.connect(getUrl(), tenantProps)) {
            createViewWhereClause(conn, tableNameStr, tenantViewNameStr, whereClause);
            tenantViewTable = PhoenixRuntime.getTableNoCache(conn,
                    tenantViewNameStr);  // --> First call to CQSI#getTable
            ServerMetadataCacheTestImpl cache = getServerMetadataCache();;
            // Override the connection to use in ServerMetadataCache
            cache.setConnectionForTesting(conn);
            byte[] tenantIDBytes = Bytes.toBytes(tenantId);
            long lastDDLTimestampFromCache = cache.getLastDDLTimestampForTable(tenantIDBytes,
                    null, Bytes.toBytes(tenantViewNameStr)); // --> Second call to CQSI#getTable
            assertEquals(tenantViewTable.getLastDDLTimestamp().longValue(),
                    lastDDLTimestampFromCache);
            byte[] tenantViewNameBytes = Bytes.toBytes(tenantViewNameStr);
            // Verify that we made 2 calls to CQSI#getTable.
            verify(spyCQS, times(2)).getTable(
                    any(), any(),  eq(tenantViewNameBytes), anyLong(), anyLong());
            // Make the same call 2 times to make sure it returns from the cache.
            cache.getLastDDLTimestampForTable(tenantIDBytes,
                    null, Bytes.toBytes(tenantViewNameStr));
            cache.getLastDDLTimestampForTable(tenantIDBytes,
                    null, Bytes.toBytes(tenantViewNameStr));
            verify(spyCQS, times(2)).getTable(
                    any(), any(),  eq(tenantViewNameBytes), anyLong(), anyLong());
        }
    }

    /**
     * Make sure we are invalidating the cache for table with no tenant connection, no schema name
     * and valid table name.
     * @throws Exception
     */
    @Test
    public void testInvalidateCacheForBaseTable() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableNameStr =  generateUniqueName();
        PTable pTable;
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            // Create a test table.
            createTable(conn, tableNameStr);
            pTable = PhoenixRuntime.getTableNoCache(conn, tableNameStr);
            ServerMetadataCacheTestImpl cache = getServerMetadataCache();;
            // Override the connection to use in ServerMetadataCache
            cache.setConnectionForTesting(conn);
            byte[] tableName = Bytes.toBytes(tableNameStr);
            long lastDDLTimestampFromCache = cache.getLastDDLTimestampForTable(
                    null, null, tableName);
            assertEquals(pTable.getLastDDLTimestamp().longValue(), lastDDLTimestampFromCache);
            // Invalidate the cache for this table.
            cache.invalidate(null, null, tableName);
            assertNull(cache.getLastDDLTimestampForTableFromCacheOnly(null, null, tableName));
        }
    }

    /**
     * Make sure we are invalidating the cache for table with no tenant connection,
     * valid schema name and table name.
     * @throws Exception
     */
    @Test
    public void testInvalidateCacheForBaseTableWithSchemaName() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String schemaName = generateUniqueName();
        String tableName =  generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        PTable pTable;
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            // Create a test table.
            createTable(conn, fullTableName);
            pTable = PhoenixRuntime.getTableNoCache(conn, fullTableName);
            ServerMetadataCacheTestImpl cache = getServerMetadataCache();;
            // Override the connection to use in ServerMetadataCache
            cache.setConnectionForTesting(conn);
            byte[] tableNameBytes = Bytes.toBytes(fullTableName);
            long lastDDLTimestampFromCache = cache.getLastDDLTimestampForTable(
                    null, Bytes.toBytes(schemaName), Bytes.toBytes(tableName));
            assertEquals(pTable.getLastDDLTimestamp().longValue(), lastDDLTimestampFromCache);
            // Invalidate the cache for this table.
            cache.invalidate(null, Bytes.toBytes(schemaName), Bytes.toBytes(tableName));
            assertNull(cache.getLastDDLTimestampForTableFromCacheOnly(null,
                    Bytes.toBytes(schemaName), Bytes.toBytes(tableName)));
        }
    }

  /**
     * Make sure we are invalidating the cache for view with tenant connection.
     * @throws Exception
     */
    @Test
    public void testInvalidateCacheForTenantView() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableNameStr = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            // Create a test table.
            createTable(conn, tableNameStr);
        }
        String tenantId = "T_" + generateUniqueName();
        Properties tenantProps = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        PTable tenantViewTable;
        // Create view on table.
        String whereClause = " WHERE V1 = 1000";
        String tenantViewNameStr = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), tenantProps)) {
            createViewWhereClause(conn, tableNameStr, tenantViewNameStr, whereClause);
            tenantViewTable = PhoenixRuntime.getTableNoCache(conn, tenantViewNameStr);
            ServerMetadataCacheTestImpl cache = getServerMetadataCache();;
            // Override the connection to use in ServerMetadataCache
            cache.setConnectionForTesting(conn);
            byte[] tenantIDBytes = Bytes.toBytes(tenantId);
            byte[] tenantViewNameBytes = Bytes.toBytes(tenantViewNameStr);
            long lastDDLTimestampFromCache = cache.getLastDDLTimestampForTable(
                    tenantIDBytes, null, tenantViewNameBytes);
            assertEquals(tenantViewTable.getLastDDLTimestamp().longValue(),
                    lastDDLTimestampFromCache);
            // Invalidate the cache for this table.
            cache.invalidate(tenantIDBytes, null, tenantViewNameBytes);
            assertNull(cache.getLastDDLTimestampForTableFromCacheOnly(
                    tenantIDBytes, null, tenantViewNameBytes));
        }
    }

    /**
     * Make sure we are invalidating the cache for table with no tenant connection, no schema name
     * and valid table name when we run alter statement.
     * @throws Exception
     */
    @Test
    public void testInvalidateCacheForBaseTableWithAlterStatement() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableNameStr =  generateUniqueName();
        byte[] tableNameBytes = Bytes.toBytes(tableNameStr);
        PTable pTable;
        ServerMetadataCacheTestImpl cache = getServerMetadataCache();;
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            // Create a test table.
            createTable(conn, tableNameStr);
            pTable = PhoenixRuntime.getTableNoCache(conn, tableNameStr);
            long lastDDLTimestamp = pTable.getLastDDLTimestamp();
            assertEquals(lastDDLTimestamp,
                    cache.getLastDDLTimestampForTable(null, null, tableNameBytes));
            String alterDDLStmt = "ALTER TABLE " + tableNameStr + " SET DISABLE_WAL = true";
            conn.createStatement().execute(alterDDLStmt);
            // The above alter statement will invalidate the last ddl timestamp from metadata cache.
            // Notice that we are using cache#getLastDDLTimestampForTableFromCacheOnly which will
            // read the last ddl timestamp only from the cache and return null if not present in
            // the cache.
            assertNull(cache.getLastDDLTimestampForTableFromCacheOnly(null, null, tableNameBytes));
            // This will load the cache with the latest last ddl timestamp value.
            long lastDDLTimestampAfterAlterStmt = cache.getLastDDLTimestampForTable(null,
                    null, tableNameBytes);
            assertNotNull(lastDDLTimestampAfterAlterStmt);
            // Make sure that the last ddl timestamp value after ALTER statement
            // is greater than previous one.
            assertTrue(lastDDLTimestampAfterAlterStmt > lastDDLTimestamp);
        }
    }

  /**
     * Make sure we are invalidating the cache for table with no tenant connection, no schema name
     * and valid table name when we run drop table statement.
     * @throws Exception
     */
    @Test
    public void testInvalidateCacheForBaseTableWithDropTableStatement() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableNameStr =  generateUniqueName();
        byte[] tableNameBytes = Bytes.toBytes(tableNameStr);
        PTable pTable;
        ServerMetadataCacheTestImpl cache = getServerMetadataCache();;
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            // Create a test table.
            createTable(conn, tableNameStr);
            pTable = PhoenixRuntime.getTableNoCache(conn, tableNameStr);
            long lastDDLTimestamp = pTable.getLastDDLTimestamp();
            assertEquals(lastDDLTimestamp,
                    cache.getLastDDLTimestampForTable(null, null, tableNameBytes));
            assertNotNull(cache.getLastDDLTimestampForTableFromCacheOnly(null, null,
                    tableNameBytes));
            String alterDDLStmt = "DROP TABLE " + tableNameStr;
            conn.createStatement().execute(alterDDLStmt);
            // The above alter statement will invalidate the last ddl timestamp from metadata cache.
            // Notice that we are using cache#getLastDDLTimestampForTableFromCacheOnly which will
            // read the last ddl timestamp only from the cache and return null if not present in
            // the cache.
            assertNull(cache.getLastDDLTimestampForTableFromCacheOnly(null, null, tableNameBytes));
        }
    }

    /**
     * Make sure we are invalidating the cache for table with no tenant connection, no schema name
     * and valid table name when we run update index statement.
     * @throws Exception
     */
    @Test
    public void testInvalidateCacheForBaseTableWithUpdateIndexStatement() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String url = QueryUtil.getConnectionUrl(props, config, "client");
        String tableNameStr = "TBL_" + generateUniqueName();
        String indexNameStr = "IND_" + generateUniqueName();
        byte[] indexNameBytes = Bytes.toBytes(indexNameStr);
        PTable indexTable;
        ServerMetadataCacheTestImpl cache = getServerMetadataCache();;
        try (Connection conn = DriverManager.getConnection(url, props)) {
            conn.setAutoCommit(false);
            // Create a test table.
            createTable(conn, tableNameStr);
            String indexDDLStmt = "CREATE INDEX " + indexNameStr + " ON " + tableNameStr + "(v1)";
            conn.createStatement().execute(indexDDLStmt);
            TestUtil.waitForIndexState(conn, indexNameStr, PIndexState.ACTIVE);
            indexTable = PhoenixRuntime.getTableNoCache(conn, indexNameStr);
            long lastDDLTimestamp = indexTable.getLastDDLTimestamp();
            assertEquals(lastDDLTimestamp,
                    cache.getLastDDLTimestampForTable(null, null, indexNameBytes));
            Thread.sleep(1);
            // Disable an index. This should change the LAST_DDL_TIMESTAMP.
            String disableIndexDDL = "ALTER INDEX " + indexNameStr + " ON " + tableNameStr
                    + " DISABLE";
            conn.createStatement().execute(disableIndexDDL);
            TestUtil.waitForIndexState(conn, indexNameStr, PIndexState.DISABLE);
            // The above alter index statement will invalidate the last ddl timestamp from metadata
            // cache. Notice that we are using cache#getLastDDLTimestampForTableFromCacheOnly which
            // will read the last ddl timestamp only from the cache and return null if not present
            // in the cache.
            assertNull(cache.getLastDDLTimestampForTableFromCacheOnly(null, null, indexNameBytes));
            // This will load the cache with the latest last ddl timestamp value.
            long lastDDLTimestampAfterUpdateIndexStmt = cache.getLastDDLTimestampForTable(null,
                    null, indexNameBytes);
            assertNotNull(lastDDLTimestampAfterUpdateIndexStmt);
            // Make sure that the last ddl timestamp value after ALTER statement
            // is greater than previous one.
            assertTrue(lastDDLTimestampAfterUpdateIndexStmt > lastDDLTimestamp);
        }
    }

    /**
     *  Test that we invalidate the cache for parent table and update the last ddl timestamp
     *  of the parent table while we add an index.
     *  Test that we invalidate the cache for parent table and index when we drop an index.
     *  Also we update the last ddl timestamp for parent table when we drop an index.
     * @throws Exception
     */
    @Test
    public void testUpdateLastDDLTimestampTableAfterIndexCreation() throws Exception {
        String tableName = generateUniqueName();
        byte[] tableNameBytes = Bytes.toBytes(tableName);
        String indexName = generateUniqueName();
        byte[] indexNameBytes = Bytes.toBytes(indexName);
        ServerMetadataCacheTestImpl cache = getServerMetadataCache();;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(true);
            createTable(conn, tableName);
            long tableLastDDLTimestampBeforeIndexCreation = getLastDDLTimestamp(tableName);
            // Populate the cache
            assertNotNull(cache.getLastDDLTimestampForTable(null, null, tableNameBytes));
            Thread.sleep(1);
            createIndex(conn, tableName, indexName, "v1");
            // Make sure that we have invalidated the last ddl timestamp for parent table
            // on all regionservers after we create an index.
            assertNull(cache.getLastDDLTimestampForTableFromCacheOnly(null, null, tableNameBytes));
            long tableLastDDLTimestampAfterIndexCreation = getLastDDLTimestamp(tableName);
            assertNotNull(tableLastDDLTimestampAfterIndexCreation);
            assertTrue(tableLastDDLTimestampAfterIndexCreation >
                    tableLastDDLTimestampBeforeIndexCreation);
            long indexLastDDLTimestampAfterCreation = getLastDDLTimestamp(indexName);
            // Make sure that last ddl timestamp is cached on the regionserver.
            assertNotNull(indexLastDDLTimestampAfterCreation);
            // Adding a sleep for 1 ms so that we get new last ddl timestamp.
            Thread.sleep(1);
            dropIndex(conn, tableName, indexName);
            // Make sure that we invalidate the cache on regionserver for base table and an index
            // after we dropped an index.
            assertNull(cache.getLastDDLTimestampForTableFromCacheOnly(null, null, tableNameBytes));
            assertNull(cache.getLastDDLTimestampForTableFromCacheOnly(null, null, indexNameBytes));
            long tableLastDDLTimestampAfterIndexDeletion = getLastDDLTimestamp(tableName);
            // Verify that last ddl timestamp after index deletion is greater than
            // the previous last ddl timestamp.
            assertNotNull(tableLastDDLTimestampAfterIndexDeletion);
            assertTrue(tableLastDDLTimestampAfterIndexDeletion >
                    tableLastDDLTimestampAfterIndexCreation);
        }
    }

    /**
     *  Test that we invalidate the cache of the immediate parent view
     *  and update the last ddl timestamp of the immediate parent view while we add an index.
     *  Test that we invalidate the cache for parent view and view index when we drop an index.
     *  Also we update the last ddl timestamp for parent view when we drop an index.
     * @throws Exception
     */
    @Test
    public void testUpdateLastDDLTimestampViewAfterIndexCreation() throws Exception {
        String tableName = "T_" + generateUniqueName();
        String globalViewName = "GV_" + generateUniqueName();
        byte[] globalViewNameBytes = Bytes.toBytes(globalViewName);
        String globalViewIndexName = "GV_IDX_" + generateUniqueName();
        byte[] globalViewIndexNameBytes = Bytes.toBytes(globalViewIndexName);

        ServerMetadataCacheTestImpl cache = getServerMetadataCache();;
        try(Connection conn = DriverManager.getConnection(getUrl());
            Statement stmt = conn.createStatement()) {
            String whereClause = " WHERE v1 < 1000";
            createTable(conn, tableName);
            createViewWhereClause(conn, tableName, globalViewName, whereClause);
            // Populate the cache
            assertNotNull(cache.getLastDDLTimestampForTable(null, null, globalViewNameBytes));
            long viewLastDDLTimestampBeforeIndexCreation = getLastDDLTimestamp(globalViewName);
            createIndex(conn, globalViewName, globalViewIndexName, "v1");

            // Make sure that we have invalidated the last ddl timestamp for parent global view
            // on all regionserver after we create a view index.
            assertNull(cache.getLastDDLTimestampForTableFromCacheOnly(null, null,
                    globalViewNameBytes));
            long viewLastDDLTimestampAfterIndexCreation = getLastDDLTimestamp(globalViewName);
            assertTrue(viewLastDDLTimestampAfterIndexCreation >
                    viewLastDDLTimestampBeforeIndexCreation);
            long indexLastDDLTimestampAfterCreation = getLastDDLTimestamp(globalViewIndexName);
            // Make sure that last ddl timestamp is cached on the regionserver.
            assertNotNull(indexLastDDLTimestampAfterCreation);
            // Adding a sleep for 1 ms so that we get new last ddl timestamp.
            Thread.sleep(1);
            dropIndex(conn, globalViewName, globalViewIndexName);
            // Make sure that we invalidate the cache on regionservers for view and its index after
            // we drop a view index.
            assertNull(cache.getLastDDLTimestampForTableFromCacheOnly(null, null,
                    globalViewNameBytes));
            assertNull(cache.getLastDDLTimestampForTableFromCacheOnly(null, null,
                    globalViewIndexNameBytes));
            long viewLastDDLTimestampAfterIndexDeletion = getLastDDLTimestamp(globalViewName);
            // Verify that last ddl timestamp of view after index deletion is greater than
            // the previous last ddl timestamp.
            assertNotNull(viewLastDDLTimestampAfterIndexDeletion);
            assertTrue(viewLastDDLTimestampAfterIndexDeletion >
                    viewLastDDLTimestampAfterIndexCreation);
        }
    }

    /**
     * Client-1 creates a table, upserts data and alters the table.
     * Client-2 queries the table before and after the alter.
     * Check queries work successfully in both cases and verify number of addTable invocations.
     */
    @Test
    public void testSelectQueryWithOldDDLTimestamp() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String url1 = QueryUtil.getConnectionUrl(props, config, "client1");
        String url2 = QueryUtil.getConnectionUrl(props, config, "client2");
        String tableName = generateUniqueName();
        ConnectionQueryServices spyCqs1 = Mockito.spy(driver.getConnectionQueryServices(url1, props));
        ConnectionQueryServices spyCqs2 = Mockito.spy(driver.getConnectionQueryServices(url2, props));
        int expectedNumCacheUpdates;

        try (Connection conn1 = spyCqs1.connect(url1, props);
             Connection conn2 = spyCqs2.connect(url2, props)) {

            // create table with UCF=never and upsert data using client-1
            createTable(conn1, tableName);
            upsert(conn1, tableName, true);

            // select query from client-2 works to populate client side metadata cache
            // there should be 1 update to the client cache
            query(conn2, tableName);
            expectedNumCacheUpdates = 1;
            Mockito.verify(spyCqs2, Mockito.times(expectedNumCacheUpdates))
                    .addTable(any(PTable.class), anyLong());

            // add column using client-1 to update last ddl timestamp
            alterTableAddColumn(conn1, tableName, "newCol1");

            // reset the spy CQSI object
            Mockito.reset(spyCqs2);

            // select query from client-2 with old ddl timestamp works
            // there should be one update to the client cache
            //verify client got a StaleMetadataCacheException
            query(conn2, tableName);
            expectedNumCacheUpdates = 1;
            Mockito.verify(spyCqs2, Mockito.times(expectedNumCacheUpdates))
                    .addTable(any(PTable.class), anyLong());
            Assert.assertEquals("Client should have encountered a StaleMetadataCacheException",
                    1, GlobalClientMetrics.GLOBAL_CLIENT_STALE_METADATA_CACHE_EXCEPTION_COUNTER.getMetric().getValue());

            // select query from client-2 with latest ddl timestamp works
            // there should be no more updates to client cache
            //verify client did not get another StaleMetadataCacheException
            query(conn2, tableName);
            Mockito.verify(spyCqs2, Mockito.times(expectedNumCacheUpdates))
                    .addTable(any(PTable.class), anyLong());
            Assert.assertEquals("Client should have encountered a StaleMetadataCacheException",
                    1, GlobalClientMetrics.GLOBAL_CLIENT_STALE_METADATA_CACHE_EXCEPTION_COUNTER.getMetric().getValue());
        }
    }

    /**
     * Test DDL timestamp validation retry logic in case of any exception
     * from Server other than StaleMetadataCacheException.
     */
    @Test
    public void testSelectQueryServerSideExceptionInValidation() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String url1 = QueryUtil.getConnectionUrl(props, config, "client1");
        String url2 = QueryUtil.getConnectionUrl(props, config, "client2");
        String tableName = generateUniqueName();
        ConnectionQueryServices spyCqs1 = Mockito.spy(driver.getConnectionQueryServices(url1, props));
        ConnectionQueryServices spyCqs2 = Mockito.spy(driver.getConnectionQueryServices(url2, props));
        ServerMetadataCacheTestImpl cache = null;

        try (Connection conn1 = spyCqs1.connect(url1, props);
             Connection conn2 = spyCqs2.connect(url2, props)) {

            // create table and upsert using client-1
            createTable(conn1, tableName);
            upsert(conn1, tableName, true);

            // Instrument ServerMetadataCache to throw a SQLException once
            cache = getServerMetadataCache();;
            ServerMetadataCacheTestImpl spyCache = Mockito.spy(cache);
            Mockito.doThrow(new SQLException("FAIL")).doCallRealMethod().when(spyCache)
                    .getLastDDLTimestampForTable(any(), any(), eq(Bytes.toBytes(tableName)));
            ServerMetadataCacheTestImpl.setInstance(serverName, spyCache);

            // query using client-2 should succeed
            query(conn2, tableName);

            // verify live region servers were refreshed
            Mockito.verify(spyCqs2, Mockito.times(1)).refreshLiveRegionServers();
        }
    }

    /**
     * Test Select query works when ddl timestamp validation with old timestamp encounters an exception.
     * Verify that the list of live region servers was refreshed when ddl timestamp validation is retried.
     * Verify that the client cache was updated after encountering StaleMetadataCacheException.
     */
    @Test
    public void testSelectQueryWithOldDDLTimestampWithExceptionRetry() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String url1 = QueryUtil.getConnectionUrl(props, config, "client1");
        String url2 = QueryUtil.getConnectionUrl(props, config, "client2");
        String tableName = generateUniqueName();
        ConnectionQueryServices spyCqs1 = Mockito.spy(driver.getConnectionQueryServices(url1, props));
        ConnectionQueryServices spyCqs2 = Mockito.spy(driver.getConnectionQueryServices(url2, props));
        int expectedNumCacheUpdates;
        ServerMetadataCacheTestImpl cache = null;

        try (Connection conn1 = spyCqs1.connect(url1, props);
             Connection conn2 = spyCqs2.connect(url2, props)) {

            // create table and upsert using client-1
            createTable(conn1, tableName);
            upsert(conn1, tableName, true);

            // query using client-2 to populate cache
            query(conn2, tableName);
            expectedNumCacheUpdates = 1;
            Mockito.verify(spyCqs2, Mockito.times(expectedNumCacheUpdates))
                    .addTable(any(PTable.class), anyLong());

            // add column using client-1 to update last ddl timestamp
            alterTableAddColumn(conn1, tableName, "newCol1");

            // reset the spy CQSI object
            Mockito.reset(spyCqs2);

            // Instrument ServerMetadataCache to throw a SQLException once
            cache = getServerMetadataCache();;
            ServerMetadataCacheTestImpl spyCache = Mockito.spy(cache);
            Mockito.doThrow(new SQLException("FAIL")).doCallRealMethod().when(spyCache)
                    .getLastDDLTimestampForTable(any(), any(), eq(Bytes.toBytes(tableName)));
            ServerMetadataCacheTestImpl.setInstance(serverName, spyCache);

            // query using client-2 should succeed, one cache update
            query(conn2, tableName);
            expectedNumCacheUpdates = 1;
            Mockito.verify(spyCqs2, Mockito.times(expectedNumCacheUpdates))
                    .addTable(any(PTable.class), anyLong());

            // verify live region servers were refreshed
            Mockito.verify(spyCqs2, Mockito.times(1)).refreshLiveRegionServers();
        }
    }

    /**
     * Test Select Query fails in case DDL timestamp validation throws SQLException twice.
     */
    @Test
    public void testSelectQueryFails() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String url1 = QueryUtil.getConnectionUrl(props, config, "client1");
        String url2 = QueryUtil.getConnectionUrl(props, config, "client2");
        String tableName = generateUniqueName();
        ConnectionQueryServices spyCqs1 = Mockito.spy(driver.getConnectionQueryServices(url1, props));
        ConnectionQueryServices spyCqs2 = Mockito.spy(driver.getConnectionQueryServices(url2, props));
        ServerMetadataCacheTestImpl cache = null;

        try (Connection conn1 = spyCqs1.connect(url1, props);
             Connection conn2 = spyCqs2.connect(url2, props)) {

            // create table and upsert using client-1
            createTable(conn1, tableName);
            upsert(conn1, tableName, true);

            // Instrument ServerMetadataCache to throw a SQLException twice
            cache = getServerMetadataCache();;
            ServerMetadataCacheTestImpl spyCache = Mockito.spy(cache);
            SQLException e = new SQLException("FAIL");
            Mockito.doThrow(e).when(spyCache)
                    .getLastDDLTimestampForTable(any(), any(), eq(Bytes.toBytes(tableName)));
            ServerMetadataCacheTestImpl.setInstance(serverName, spyCache);

            // query using client-2 should fail
            query(conn2, tableName);
            Assert.fail("Query should have thrown Exception");
        }
        catch (Exception e) {
            Assert.assertTrue("SQLException was not thrown when last ddl timestamp validation encountered errors twice.", e instanceof SQLException);
        }
    }


    /**
     * Client-1 creates a table, 2 level of views on it and alters the first level view.
     * Client-2 queries the second level view, verify that there were 3 cache updates in client-2,
     * one each for the two views and base table.
     */
    @Test
    public void testSelectQueryOnView() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String url1 = QueryUtil.getConnectionUrl(props, config, "client1");
        String url2 = QueryUtil.getConnectionUrl(props, config, "client2");
        String tableName = generateUniqueName();
        ConnectionQueryServices spyCqs1 = Mockito.spy(driver.getConnectionQueryServices(url1, props));
        ConnectionQueryServices spyCqs2 = Mockito.spy(driver.getConnectionQueryServices(url2, props));
        int expectedNumCacheUpdates;

        try (Connection conn1 = spyCqs1.connect(url1, props);
             Connection conn2 = spyCqs2.connect(url2, props)) {

            // create table using client-1
            createTable(conn1, tableName);
            upsert(conn1, tableName, true);

            // create 2 level of views using client-1
            String view1 = generateUniqueName();
            String view2 = generateUniqueName();
            createView(conn1, tableName, view1);
            createView(conn1, view1, view2);

            // query second level view using client-2
            query(conn2, view2);
            expectedNumCacheUpdates = 3; // table, view1, view2
            Mockito.verify(spyCqs2, Mockito.times(expectedNumCacheUpdates))
                    .addTable(any(PTable.class), anyLong());

            // alter first level view using client-1 to update its last ddl timestamp
            alterViewAddColumn(conn1, view1, "foo");

            // reset the spy CQSI object
            Mockito.reset(spyCqs2);

            // query second level view
            query(conn2, view2);

            // verify there was a getTable RPC for the view and all its ancestors
            Mockito.verify(spyCqs2, Mockito.times(1)).getTable(eq(null),
                    any(byte[].class), eq(PVarchar.INSTANCE.toBytes(tableName)),
                    anyLong(), anyLong());
            Mockito.verify(spyCqs2, Mockito.times(1)).getTable(eq(null),
                    any(byte[].class), eq(PVarchar.INSTANCE.toBytes(view1)),
                    anyLong(), anyLong());
            Mockito.verify(spyCqs2, Mockito.times(1)).getTable(eq(null),
                    any(byte[].class), eq(PVarchar.INSTANCE.toBytes(view2)),
                    anyLong(), anyLong());

            // verify that the view and all its ancestors were updated in the client cache
            expectedNumCacheUpdates = 3; // table, view1, view2
            Mockito.verify(spyCqs2, Mockito.times(expectedNumCacheUpdates))
                    .addTable(any(PTable.class), anyLong());
        }
    }

    /**
     * Verify queries on system tables work as we will validate last ddl timestamps for them also.
     */
    @Test
    public void testSelectQueryOnSystemTables() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String url = QueryUtil.getConnectionUrl(props, config, "client");
        ConnectionQueryServices cqs = driver.getConnectionQueryServices(url, props);

        try (Connection conn = cqs.connect(url, props)) {
            query(conn, PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME);
            query(conn, PhoenixDatabaseMetaData.SYSTEM_TASK_NAME);
            query(conn, PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME);
            query(conn, PhoenixDatabaseMetaData.SYSTEM_LOG_NAME);
        }
    }

    /**
     * https://issues.apache.org/jira/browse/PHOENIX-7167
     * Use the default connection to query system tables to confirm
     * that the PTable object for SYSTEM tables is correctly bootstrapped.
     */
    @Test
    public void testSystemTablesBootstrap() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String url = QueryUtil.getConnectionUrl(props, config);
        ConnectionQueryServices cqs = driver.getConnectionQueryServices(url, props);

        try (Connection conn = cqs.connect(url, props)) {
            query(conn, PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME);
            query(conn, PhoenixDatabaseMetaData.SYSTEM_TASK_NAME);
            query(conn, PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME);
            query(conn, PhoenixDatabaseMetaData.SYSTEM_LOG_NAME);
        }
    }

    /**
     * Test that a client does not see TableNotFoundException when trying to validate
     * LAST_DDL_TIMESTAMP for a view and its parent after the table was altered and removed from
     * the client's cache.
     */
    @Test
    public void testQueryViewAfterParentRemovedFromCache() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String url = QueryUtil.getConnectionUrl(props, config);
        ConnectionQueryServices cqs = driver.getConnectionQueryServices(url, props);
        String tableName = generateUniqueName();
        String viewName = generateUniqueName();
        try (Connection conn = cqs.connect(url, props)) {
            createTable(conn, tableName);
            createView(conn, tableName, viewName);
            query(conn, viewName);
            // this removes the parent table from the client cache
            alterTableDropColumn(conn, tableName, "v2");
            query(conn, viewName);
        } catch (TableNotFoundException e) {
            fail("TableNotFoundException should not be encountered by client.");
        }
    }

    /**
     * Test query on index with stale last ddl timestamp.
     * Client-1 creates a table and an index on it. Client-2 queries table to populate its cache.
     * Client-1 alters a property on the index. Client-2 queries the table again.
     * Verify that the second query works and the index metadata was updated in the client cache.
     */
    @Test
    public void testSelectQueryAfterAlterIndex() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String url1 = QueryUtil.getConnectionUrl(props, config, "client1");
        String url2 = QueryUtil.getConnectionUrl(props, config, "client2");
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        ConnectionQueryServices spyCqs1 = Mockito.spy(driver.getConnectionQueryServices(url1, props));
        ConnectionQueryServices spyCqs2 = Mockito.spy(driver.getConnectionQueryServices(url2, props));

        try (Connection conn1 = spyCqs1.connect(url1, props);
             Connection conn2 = spyCqs2.connect(url2, props)) {

            //client-1 creates a table and an index on it
            createTable(conn1, tableName);
            createIndex(conn1, tableName, indexName, "v1");
            TestUtil.waitForIndexState(conn1, indexName, PIndexState.ACTIVE);

            //client-2 populates its cache, 1 getTable and 1 addTable call for the table
            query(conn2, tableName);
            Mockito.verify(spyCqs2, Mockito.times(1)).getTable(eq(null),
                    any(byte[].class), eq(PVarchar.INSTANCE.toBytes(tableName)),
                    anyLong(), anyLong());
            Mockito.verify(spyCqs2, Mockito.times(1))
                    .addTable(any(PTable.class), anyLong());

            //client-1 updates index property
            alterIndexChangeState(conn1, tableName, indexName, " REBUILD");

            //client-2's query using the index should work
            PhoenixStatement stmt = conn2.createStatement().unwrap(PhoenixStatement.class);
            stmt.executeQuery("SELECT k FROM " + tableName + " WHERE v1=1");
            Assert.assertEquals("Query on secondary key should have used index.", indexName, stmt.getQueryPlan().getTableRef().getTable().getTableName().toString());

            //verify client-2 cache was updated with the index and base table metadata
            Mockito.verify(spyCqs2, Mockito.times(2)).getTable(eq(null),
                    any(byte[].class), eq(PVarchar.INSTANCE.toBytes(tableName)),
                    anyLong(), anyLong());
            Mockito.verify(spyCqs2, Mockito.times(1)).getTable(eq(null),
                    any(byte[].class), eq(PVarchar.INSTANCE.toBytes(indexName)),
                    anyLong(), anyLong());
            Mockito.verify(spyCqs2, Mockito.times(3))
                    .addTable(any(PTable.class), anyLong());

            //client-2 queries again with latest metadata
            //verify no more getTable/addTable calls
            queryWithIndex(conn2, tableName);
            Mockito.verify(spyCqs2, Mockito.times(2)).getTable(eq(null),
                    any(byte[].class), eq(PVarchar.INSTANCE.toBytes(tableName)),
                    anyLong(), anyLong());
            Mockito.verify(spyCqs2, Mockito.times(3))
                    .addTable(any(PTable.class), anyLong());
        }
    }

    /**
     * Test that a client can learn about a newly created index.
     * Client-1 creates a table, client-2 queries the table to populate its cache.
     * Client-1 creates an index on the table. Client-2 queries the table using the index.
     * Verify that client-2 uses the index for the query.
     */
    @Test
    public void testSelectQueryAddIndex() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String url1 = QueryUtil.getConnectionUrl(props, config, "client1");
        String url2 = QueryUtil.getConnectionUrl(props, config, "client2");
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        ConnectionQueryServices spyCqs1 = Mockito.spy(driver.getConnectionQueryServices(url1, props));
        ConnectionQueryServices spyCqs2 = Mockito.spy(driver.getConnectionQueryServices(url2, props));

        try (Connection conn1 = spyCqs1.connect(url1, props);
             Connection conn2 = spyCqs2.connect(url2, props)) {

            //client-1 creates table
            createTable(conn1, tableName);

            //client-2 populates its cache
            query(conn2, tableName);

            //client-1 creates an index on the table
            createIndex(conn1, tableName, indexName, "v1");
            TestUtil.waitForIndexState(conn1, indexName, PIndexState.ACTIVE);

            //client-2 query should be able to use this index
            PhoenixStatement stmt = conn2.createStatement().unwrap(PhoenixStatement.class);
            ResultSet rs = stmt.executeQuery("SELECT k FROM " + tableName + " WHERE v1=1");
            Assert.assertEquals("Query on secondary key should have used index.", indexName, stmt.getQueryPlan().getContext().getCurrentTable().getTable().getName().getString());
        }
    }

    /**
     * Test that a client can learn about a dropped index.
     * Client-1 creates a table and an index, client-2 queries the table to populate its cache.
     * Client-1 drops the index. Client-2 queries the table with index hint.
     * Verify that client-2 uses the data table for the query.
     */
    @Test
    public void testSelectQueryDropIndex() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String url1 = QueryUtil.getConnectionUrl(props, config, "client1");
        String url2 = QueryUtil.getConnectionUrl(props, config, "client2");
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        ConnectionQueryServices spyCqs1 = Mockito.spy(driver.getConnectionQueryServices(url1, props));
        ConnectionQueryServices spyCqs2 = Mockito.spy(driver.getConnectionQueryServices(url2, props));

        try (Connection conn1 = spyCqs1.connect(url1, props);
             Connection conn2 = spyCqs2.connect(url2, props)) {

            //client-1 creates table and index on it
            createTable(conn1, tableName);
            createIndex(conn1, tableName, indexName, "v1");

            //client-2 populates its cache
            query(conn2, tableName);

            //client-1 drops the index
            dropIndex(conn1, tableName, indexName);

            //client-2 queries should use data table and not run into table not found error even when index hint is given
            PhoenixStatement stmt = conn2.createStatement().unwrap(PhoenixStatement.class);
            ResultSet rs = stmt.executeQuery("SELECT /*+ INDEX(" + tableName + " " + indexName + ") */ * FROM " + tableName + " WHERE v1=1");
            Assert.assertEquals("Query should have used data table since index was dropped", tableName, stmt.getQueryPlan().getContext().getCurrentTable().getTable().getName().getString());
        }
    }

    /**
     * Test the case when a client upserts into multiple tables before calling commit.
     * Verify that last ddl timestamp was validated for all involved tables only once.
     */
    @Test
    public void testUpsertMultipleTablesWithOldDDLTimestamp() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String url1 = QueryUtil.getConnectionUrl(props, config, "client1");
        String url2 = QueryUtil.getConnectionUrl(props, config, "client2");
        String tableName1 = generateUniqueName();
        String tableName2 = generateUniqueName();
        ConnectionQueryServices spyCqs1 = Mockito.spy(driver.getConnectionQueryServices(url1, props));
        ConnectionQueryServices spyCqs2 = Mockito.spy(driver.getConnectionQueryServices(url2, props));

        try (Connection conn1 = spyCqs1.connect(url1, props);
             Connection conn2 = spyCqs2.connect(url2, props)) {

            //client-1 creates 2 tables
            createTable(conn1, tableName1);
            createTable(conn1, tableName2);

            //client-2 populates its cache, 1 getTable call for each table
            query(conn2, tableName1);
            query(conn2, tableName2);

            //client-1 alters one of the tables
            alterTableAddColumn(conn1, tableName2, "col3");

            //client-2 upserts multiple rows to both tables before calling commit
            //verify the table metadata was fetched for each table
            multiTableUpsert(conn2, tableName1, tableName2);
            Mockito.verify(spyCqs2, Mockito.times(2)).getTable(eq(null),
                    any(byte[].class), eq(PVarchar.INSTANCE.toBytes(tableName1)),
                    anyLong(), anyLong());
            Mockito.verify(spyCqs2, Mockito.times(2)).getTable(eq(null),
                    any(byte[].class), eq(PVarchar.INSTANCE.toBytes(tableName2)),
                    anyLong(), anyLong());
        }
    }

    /**
     * Test upserts into a multi-level view hierarchy.
     */
    @Test
    public void testUpsertViewWithOldDDLTimestamp() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String url1 = QueryUtil.getConnectionUrl(props, config, "client1");
        String url2 = QueryUtil.getConnectionUrl(props, config, "client2");
        String tableName = generateUniqueName();
        String viewName1 = generateUniqueName();
        String viewName2 = generateUniqueName();
        ConnectionQueryServices spyCqs1 = Mockito.spy(driver.getConnectionQueryServices(url1, props));
        ConnectionQueryServices spyCqs2 = Mockito.spy(driver.getConnectionQueryServices(url2, props));

        try (Connection conn1 = spyCqs1.connect(url1, props);
             Connection conn2 = spyCqs2.connect(url2, props)) {

            //client-1 creates a table and views
            createTable(conn1, tableName);
            createView(conn1, tableName, viewName1);
            createView(conn1, viewName1, viewName2);

            //client-2 populates its cache, 1 getTable RPC each for table, view1, view2
            query(conn2, viewName2);
            Mockito.verify(spyCqs2, Mockito.times(1)).getTable(eq(null),
                    any(byte[].class), eq(PVarchar.INSTANCE.toBytes(tableName)),
                    anyLong(), anyLong());
            Mockito.verify(spyCqs2, Mockito.times(1)).getTable(eq(null),
                    any(byte[].class), eq(PVarchar.INSTANCE.toBytes(viewName1)),
                    anyLong(), anyLong());
            Mockito.verify(spyCqs2, Mockito.times(1)).getTable(eq(null),
                    any(byte[].class), eq(PVarchar.INSTANCE.toBytes(viewName2)),
                    anyLong(), anyLong());

            //client-1 alters first level view
            alterViewAddColumn(conn1, viewName1, "col3");

            //client-2 upserts into second level view
            //verify there was a getTable RPC for the view and all its ancestors
            //verify that the client got a StaleMetadataCacheException
            upsert(conn2, viewName2, true);

            Mockito.verify(spyCqs2, Mockito.times(2)).getTable(eq(null),
                    any(byte[].class), eq(PVarchar.INSTANCE.toBytes(tableName)),
                    anyLong(), anyLong());
            Mockito.verify(spyCqs2, Mockito.times(2)).getTable(eq(null),
                    any(byte[].class), eq(PVarchar.INSTANCE.toBytes(viewName1)),
                    anyLong(), anyLong());
            Mockito.verify(spyCqs2, Mockito.times(2)).getTable(eq(null),
                    any(byte[].class), eq(PVarchar.INSTANCE.toBytes(viewName2)),
                    anyLong(), anyLong());
            Assert.assertEquals("Client should have encountered a StaleMetadataCacheException",
                    1, GlobalClientMetrics.GLOBAL_CLIENT_STALE_METADATA_CACHE_EXCEPTION_COUNTER.getMetric().getValue());
            //client-2 upserts into first level view
            //verify no getTable RPCs
            //verify that the client did not get a StaleMetadataCacheException
            upsert(conn2, viewName1, true);

            Mockito.verify(spyCqs2, Mockito.times(2)).getTable(eq(null),
                    any(byte[].class), eq(PVarchar.INSTANCE.toBytes(tableName)),
                    anyLong(), anyLong());
            Mockito.verify(spyCqs2, Mockito.times(2)).getTable(eq(null),
                    any(byte[].class), eq(PVarchar.INSTANCE.toBytes(viewName1)),
                    anyLong(), anyLong());
            Mockito.verify(spyCqs2, Mockito.times(2)).getTable(eq(null),
                    any(byte[].class), eq(PVarchar.INSTANCE.toBytes(viewName2)),
                    anyLong(), anyLong());
            Assert.assertEquals("Client should not have encountered another StaleMetadataCacheException",
                    1, GlobalClientMetrics.GLOBAL_CLIENT_STALE_METADATA_CACHE_EXCEPTION_COUNTER.getMetric().getValue());
        }
    }

    /**
     * Test that upserts into a table which was dropped throws a TableNotFoundException.
     */
    @Test
    public void testUpsertDroppedTable() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String url1 = QueryUtil.getConnectionUrl(props, config, "client1");
        String url2 = QueryUtil.getConnectionUrl(props, config, "client2");
        String tableName = generateUniqueName();
        ConnectionQueryServices spyCqs1 = Mockito.spy(driver.getConnectionQueryServices(url1, props));
        ConnectionQueryServices spyCqs2 = Mockito.spy(driver.getConnectionQueryServices(url2, props));

        try (Connection conn1 = spyCqs1.connect(url1, props);
             Connection conn2 = spyCqs2.connect(url2, props)) {

            // client-1 creates tables and executes upserts
            createTable(conn1, tableName);
            upsert(conn1, tableName, false);
            upsert(conn1, tableName, false);
            upsert(conn1, tableName, false);

            // client-2 drops the table
            conn2.createStatement().execute("DROP TABLE " + tableName);

            //client-1 commits
            conn1.commit();
            Assert.fail("Commit should have failed with TableNotFoundException");
        }
        catch (Exception e) {
            Assert.assertTrue("TableNotFoundException was not thrown when table was dropped concurrently with upserts.", e instanceof TableNotFoundException);
        }
    }

    /**
     * Client-1 creates a table and executes some upserts.
     * Client-2 drops a column for which client-1 had executed upserts.
     * Client-1 calls commit. Verify that client-1 gets ColumnNotFoundException
     */
    @Test
    public void testUpsertDroppedTableColumn() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String url1 = QueryUtil.getConnectionUrl(props, config, "client1");
        String url2 = QueryUtil.getConnectionUrl(props, config, "client2");
        String tableName = generateUniqueName();
        ConnectionQueryServices spyCqs1 = Mockito.spy(driver.getConnectionQueryServices(url1, props));
        ConnectionQueryServices spyCqs2 = Mockito.spy(driver.getConnectionQueryServices(url2, props));

        try (Connection conn1 = spyCqs1.connect(url1, props);
             Connection conn2 = spyCqs2.connect(url2, props)) {

            // client-1 creates tables and executes upserts
            createTable(conn1, tableName);
            upsert(conn1, tableName, false);
            upsert(conn1, tableName, false);
            upsert(conn1, tableName, false);

            // client-2 drops a column
            alterTableDropColumn(conn2, tableName, "v1");

            //client-1 commits
            conn1.commit();
            Assert.fail("Commit should have failed with ColumnNotFoundException");
        }
        catch (Exception e) {
            Assert.assertTrue("ColumnNotFoundException was not thrown when column was dropped concurrently with upserts.", e instanceof ColumnNotFoundException);
        }
    }

    /**
     * Client-1 creates a table and executes some upserts.
     * Client-2 adds a column to the table.
     * Client-1 calls commit. Verify that client-1 does not get any errors.
     */
    @Test
    public void testUpsertAddTableColumn() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String url1 = QueryUtil.getConnectionUrl(props, config, "client1");
        String url2 = QueryUtil.getConnectionUrl(props, config, "client2");
        String tableName = generateUniqueName();
        ConnectionQueryServices spyCqs1 = Mockito.spy(driver.getConnectionQueryServices(url1, props));
        ConnectionQueryServices spyCqs2 = Mockito.spy(driver.getConnectionQueryServices(url2, props));

        try (Connection conn1 = spyCqs1.connect(url1, props);
             Connection conn2 = spyCqs2.connect(url2, props)) {

            // client-1 creates tables and executes upserts
            createTable(conn1, tableName);
            upsert(conn1, tableName, false);
            upsert(conn1, tableName, false);
            upsert(conn1, tableName, false);

            // client-2 adds a column
            alterTableAddColumn(conn2, tableName, "v5");

            //client-1 commits
            conn1.commit();
        }
    }

    /**
     * Client-1 creates a table and executes some upserts.
     * Client-2 creates an index on the table.
     * Client-1 calls commit. Verify that index mutations were correctly generated
     */
    @Test
    public void testConcurrentUpsertIndexCreation() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String url1 = QueryUtil.getConnectionUrl(props, config, "client1");
        String url2 = QueryUtil.getConnectionUrl(props, config, "client2");
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        ConnectionQueryServices spyCqs1 = Mockito.spy(driver.getConnectionQueryServices(url1, props));
        ConnectionQueryServices spyCqs2 = Mockito.spy(driver.getConnectionQueryServices(url2, props));

        try (Connection conn1 = spyCqs1.connect(url1, props);
             Connection conn2 = spyCqs2.connect(url2, props)) {

            // client-1 creates tables and executes upserts
            createTable(conn1, tableName);
            upsert(conn1, tableName, false);
            upsert(conn1, tableName, false);
            upsert(conn1, tableName, false);

            // client-2 creates an index
            createIndex(conn2, tableName, indexName, "v1");

            //client-1 commits
            upsert(conn1, tableName, false);
            upsert(conn1, tableName, false);
            conn1.commit();

            //verify index rows
            int tableCount, indexCount;
            ResultSet rs = conn1.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableName);
            rs.next();
            tableCount = rs.getInt(1);

            rs = conn1.createStatement().executeQuery("SELECT COUNT(*) FROM " + indexName);
            rs.next();
            indexCount = rs.getInt(1);

            Assert.assertEquals("All index mutations were not generated when index was created concurrently with upserts.", tableCount, indexCount);
        }
    }

    /**
     * Client-1 creates a table, index and executes some upserts.
     * Client-2 drops the index on the table.
     * Client-1 calls commit. Verify that client-1 does not see any errors
     */
    @Test
    public void testConcurrentUpsertDropIndex() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String url1 = QueryUtil.getConnectionUrl(props, config, "client1");
        String url2 = QueryUtil.getConnectionUrl(props, config, "client2");
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        ConnectionQueryServices spyCqs1 = Mockito.spy(driver.getConnectionQueryServices(url1, props));
        ConnectionQueryServices spyCqs2 = Mockito.spy(driver.getConnectionQueryServices(url2, props));

        try (Connection conn1 = spyCqs1.connect(url1, props);
             Connection conn2 = spyCqs2.connect(url2, props)) {

            // client-1 creates tables, index and executes upserts
            createTable(conn1, tableName);
            createIndex(conn1, tableName, indexName, "v1");
            upsert(conn1, tableName, false);
            upsert(conn1, tableName, false);
            upsert(conn1, tableName, false);

            // client-2 drops the index
            dropIndex(conn2, tableName, indexName);

            //client-1 commits
            upsert(conn1, tableName, false);
            upsert(conn1, tableName, false);
            conn1.commit();
        }
    }
    /**
     * Client-1 creates a table, index in disabled state and executes some upserts.
     * Client-2 marks the index as Rebuild.
     * Client-1 calls commit. Verify that index mutations were correctly generated
     */
    @Test
    public void testConcurrentUpsertIndexStateChange() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String url1 = QueryUtil.getConnectionUrl(props, config, "client1");
        String url2 = QueryUtil.getConnectionUrl(props, config, "client2");
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        ConnectionQueryServices spyCqs1 = Mockito.spy(driver.getConnectionQueryServices(url1, props));
        ConnectionQueryServices spyCqs2 = Mockito.spy(driver.getConnectionQueryServices(url2, props));

        try (Connection conn1 = spyCqs1.connect(url1, props);
             Connection conn2 = spyCqs2.connect(url2, props)) {

            // client-1 creates tables and executes upserts
            createTable(conn1, tableName);
            createIndex(conn1, tableName, indexName, "v1");
            alterIndexChangeState(conn1, tableName, indexName, " DISABLE");
            upsert(conn1, tableName, false);
            upsert(conn1, tableName, false);
            upsert(conn1, tableName, false);

            // client-2 creates an index
            alterIndexChangeState(conn2, tableName, indexName, " REBUILD");

            //client-1 commits
            upsert(conn1, tableName, false);
            upsert(conn1, tableName, false);
            conn1.commit();

            //verify index rows
            int tableCount, indexCount;
            ResultSet rs = conn1.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableName);
            rs.next();
            tableCount = rs.getInt(1);

            rs = conn1.createStatement().executeQuery("SELECT COUNT(*) FROM " + indexName);
            rs.next();
            indexCount = rs.getInt(1);

            Assert.assertEquals("All index mutations were not generated when index was created concurrently with upserts.", tableCount, indexCount);
        }
    }

    /**
     * Test that a client can not create an index on a column after another client dropped the column.
     */
    @Test
    public void testClientCannotCreateIndexOnDroppedColumn() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String url1 = QueryUtil.getConnectionUrl(props, config, "client1");
        String url2 = QueryUtil.getConnectionUrl(props, config, "client2");
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        ConnectionQueryServices spyCqs1 = Mockito.spy(driver.getConnectionQueryServices(url1, props));
        ConnectionQueryServices spyCqs2 = Mockito.spy(driver.getConnectionQueryServices(url2, props));

        try (Connection conn1 = spyCqs1.connect(url1, props);
             Connection conn2 = spyCqs2.connect(url2, props)) {
            createTable(conn1, tableName);
            alterTableDropColumn(conn2, tableName, "v2");
            createIndex(conn1, tableName, indexName, "v2");
            fail("Client should not be able to create index on dropped column.");
        }
        catch (ColumnNotFoundException expected) {
        }
    }

    /**
     * Test that upserts into a view whose parent was dropped throws a TableNotFoundException.
     */
    @Test
    public void testConcurrentUpsertDropView() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String url1 = QueryUtil.getConnectionUrl(props, config, "client1");
        String url2 = QueryUtil.getConnectionUrl(props, config, "client2");
        String tableName = generateUniqueName();
        String viewName1 = generateUniqueName();
        String viewName2 = generateUniqueName();
        ConnectionQueryServices spyCqs1 = Mockito.spy(driver.getConnectionQueryServices(url1, props));
        ConnectionQueryServices spyCqs2 = Mockito.spy(driver.getConnectionQueryServices(url2, props));

        try (Connection conn1 = spyCqs1.connect(url1, props);
             Connection conn2 = spyCqs2.connect(url2, props)) {

            //client-1 creates tables and views
            createTable(conn1, tableName);
            createView(conn1, tableName, viewName1);
            createView(conn1, viewName1, viewName2);

            //client-2 upserts into second level view
            upsert(conn2, viewName2, false);

            //client-1 drop first level view
            dropView(conn1, viewName1, true);

            //client-2 upserts into second level view and commits
            upsert(conn2, viewName2, true);
        }
        catch (Exception e) {
            Assert.assertTrue("TableNotFoundException was not thrown when parent view " +
                            "was dropped (cascade) concurrently with upserts.",
                    e instanceof TableNotFoundException);
        }
    }

    /**
     * Test server side metrics are populated correctly.
     * Client-1 creates a table and creates an index on it.
     * Client-2 queries the table.
     */
    @Test
    public void testServerSideMetrics() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String url1 = QueryUtil.getConnectionUrl(props, config, "client1");
        String url2 = QueryUtil.getConnectionUrl(props, config, "client2");
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        ConnectionQueryServices cqs1 = driver.getConnectionQueryServices(url1, props);
        ConnectionQueryServices cqs2 = driver.getConnectionQueryServices(url2, props);
        MetricsMetadataCachingSource metricsSource
                = MetricsPhoenixCoprocessorSourceFactory.getInstance().getMetadataCachingSource();

        //take a snapshot of current metric values
        MetricsMetadataCachingSource.MetadataCachingMetricValues oldMetricValues
                = metricsSource.getCurrentMetricValues();

        long cacheHit = 0;
        long cacheMiss = 0;
        long validateDDLRequestCount = 0;
        long cacheInvOpsCount = 0;
        long cacheInvSuccessCount = 0;
        long cacheInvFailureCount = 0;
        long cacheInvRpcTimeCount = 0;
        long cacheInvTotalTimeCount = 0;

        try (Connection conn1 = cqs1.connect(url1, props);
             Connection conn2 = cqs2.connect(url2, props)) {

            // no metric changes
            createTable(conn1, tableName);

            // client validates table, regionserver does not find table in its cache
            query(conn2, tableName);
            validateDDLRequestCount++;
            cacheMiss++;

            // last_ddl_timestamp is bumped for the table
            // cache invalidation operation succeeds for table
            // cache invalidation operation succeeds for index state change
            // only one region server in tests for cache invalidation RPC
            createIndex(conn1, tableName, indexName, "v1");
            cacheInvOpsCount += 2;
            cacheInvRpcTimeCount += 2;
            cacheInvTotalTimeCount += 2;
            cacheInvSuccessCount += 2;

            // client validates only table since it does not know about the index yet
            // regionserver does not find table in its cache
            query(conn2, tableName);
            validateDDLRequestCount++;
            cacheMiss++;

            // client validates both index and table this time
            // regionserver finds table but does not find index in its cache
            query(conn2, tableName);
            validateDDLRequestCount++;
            cacheHit++; //table
            cacheMiss++; //index

            // client validates index and table again
            // regionserver finds both index and table in its cache
            query(conn2, tableName);
            validateDDLRequestCount++;
            cacheHit += 2;

            MetricsMetadataCachingSource.MetadataCachingMetricValues newMetricValues
                    = metricsSource.getCurrentMetricValues();

            assertEquals("Incorrect number of cache hits on region server.", cacheHit,
                    newMetricValues.getCacheHitCount() - oldMetricValues.getCacheHitCount());

            assertEquals("Incorrect number of cache misses on region server.", cacheMiss,
                newMetricValues.getCacheMissCount() - oldMetricValues.getCacheMissCount());

            assertEquals("Incorrect number of validate ddl timestamp requests.",
                    validateDDLRequestCount,
                    newMetricValues.getValidateDDLTimestampRequestsCount()
                            - oldMetricValues.getValidateDDLTimestampRequestsCount());

            assertEquals("Incorrect number of cache invalidation ops count.",
                    cacheInvOpsCount,
                    newMetricValues.getCacheInvalidationOpsCount()
                            - oldMetricValues.getCacheInvalidationOpsCount());

            assertEquals("Incorrect number of successful cache invalidation ops count.",
                    cacheInvSuccessCount,
                    newMetricValues.getCacheInvalidationSuccessCount()
                            - oldMetricValues.getCacheInvalidationSuccessCount());

            assertEquals("Incorrect number of failed cache invalidation ops count.",
                    cacheInvFailureCount,
                    newMetricValues.getCacheInvalidationFailureCount()
                            - oldMetricValues.getCacheInvalidationFailureCount());

            assertEquals("Incorrect number of cache invalidation RPC times.",
                    cacheInvRpcTimeCount,
                    newMetricValues.getCacheInvalidationRpcTimeCount()
                            - oldMetricValues.getCacheInvalidationRpcTimeCount());

            assertEquals("Incorrect number of cache invalidation total times.",
                    cacheInvTotalTimeCount,
                    newMetricValues.getCacheInvalidationTotalTimeCount()
                            - oldMetricValues.getCacheInvalidationTotalTimeCount());
        }
    }

    /*
        Tests that invalidate server metadata cache fails on a non server connection.
     */
    @Test
    public void testInvalidateMetadataCacheOnNonServerConnection() {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (PhoenixConnection conn = DriverManager.getConnection(getUrl(), props)
                .unwrap(PhoenixConnection.class)) {
            ConnectionQueryServices cqs = conn.getQueryServices();
            cqs.invalidateServerMetadataCache(null);
            fail("Shouldn't come here");
        } catch (Throwable t) {
            assertNotNull(t);
            assertTrue(t.getMessage().contains(INVALIDATE_SERVER_METADATA_CACHE_EX_MESSAGE));
        }
    }

    /**
     * Test that a query on the column of a view which was previously dropped
     * throws a ColumnNotFoundException. Use the same client to drop the column.
     */
    @Test
    public void testDroppedTableColumnNotVisibleToViewUsingSameClient() throws Exception {
        testDroppedTableColumnNotVisibleToView(true);
    }

    /**
     * Test that a query on the column of a view which was previously dropped
     * throws a ColumnNotFoundException. Use a different client to drop the column.
     */
    @Test
    public void testDroppedTableColumnNotVisibleToViewUsingDifferentClients() throws Exception {
        testDroppedTableColumnNotVisibleToView(false);
    }

    public void testDroppedTableColumnNotVisibleToView(boolean useSameClient) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String url1 = QueryUtil.getConnectionUrl(props, config, "client1");
        String url2 = QueryUtil.getConnectionUrl(props, config, "client2");
        String tableName = generateUniqueName();
        String viewName1 = generateUniqueName();
        String viewName2 = generateUniqueName();
        ConnectionQueryServices cqs1 = driver.getConnectionQueryServices(url1, props);
        ConnectionQueryServices cqs2 = driver.getConnectionQueryServices(url2, props);
        try (Connection conn = cqs1.connect(url1, props);
             Connection conn2 = useSameClient ? conn : cqs2.connect(url2, props)) {
            createTable(conn, tableName);
            createView(conn, tableName, viewName1);
            createView(conn, viewName1, viewName2);
            query(conn2, viewName2);

            alterTableDropColumn(conn, tableName, "v2");
            query(conn2, tableName);

            conn2.createStatement().execute("SELECT v2 FROM " + viewName2);
            fail("Column dropped from base table should not be visible to view.");
        } catch (ColumnNotFoundException expected) {
        }
    }

    /**
     * Test that ancestor->last_ddl_timestamp is populated in a new client.
     * @throws Exception
     */
    @Test
    public void testAncestorLastDDLMapPopulatedInDifferentClient() throws Exception {
        String SCHEMA1 = generateUniqueName();
        String SCHEMA2 = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String baseTable = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String index = generateUniqueName();
        String view = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
        String viewIndex = generateUniqueName();
        String baseTable2 = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String index2 = generateUniqueName();
        String view2 = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
        String viewIndex2 = generateUniqueName();
        String url1 = QueryUtil.getConnectionUrl(props, config, "client1");
        String url2 = QueryUtil.getConnectionUrl(props, config, "client2");
        ConnectionQueryServices cqs1 = driver.getConnectionQueryServices(url1, props);
        ConnectionQueryServices cqs2 = driver.getConnectionQueryServices(url2, props);
        try (Connection conn = cqs1.connect(url1, props);
             Connection conn2 = cqs2.connect(url2, props)) {
            //client-1 creates tables, views, indexes and view indexes
            createTable(conn, baseTable);
            createView(conn, baseTable, view);
            createIndex(conn, baseTable, index, "v2");
            createIndex(conn, view, viewIndex, "v1");
            createTable(conn, baseTable2);
            createView(conn, baseTable2, view2);
            createIndex(conn, baseTable2, index2, "v2");
            createIndex(conn, view2, viewIndex2, "v1");

            //client-2 queries the view
            query(conn2, view);

            PTable basePTable = PhoenixRuntime.getTable(conn2, baseTable);
            PTable viewPTable = PhoenixRuntime.getTable(conn2, view);
            PTable viewIndexPTable = PhoenixRuntime.getTable(conn2, SchemaUtil.getTableName(SCHEMA2, viewIndex));
            PTable indexPTable = PhoenixRuntime.getTable(conn2, SchemaUtil.getTableName(SCHEMA1, index));

            //verify view has base table in ancestor map
            Map<PTableKey,Long> map = viewPTable.getAncestorLastDDLTimestampMap();
            assertEquals(basePTable.getLastDDLTimestamp(), map.get(basePTable.getKey()));

            //verify view index has base table and view in ancestor map
            map = viewIndexPTable.getAncestorLastDDLTimestampMap();
            assertEquals(2, map.size());
            assertEquals(basePTable.getLastDDLTimestamp(), map.get(basePTable.getKey()));
            assertEquals(viewPTable.getLastDDLTimestamp(), map.get(viewPTable.getKey()));

            //verify index has only base table in ancestor map
            map = indexPTable.getAncestorLastDDLTimestampMap();
            assertEquals(1, map.size());
            assertEquals(basePTable.getLastDDLTimestamp(), map.get(basePTable.getKey()));

            //also verify index PTable within base table has the map
            assertEquals(1, basePTable.getIndexes().size());
            map = basePTable.getIndexes().get(0).getAncestorLastDDLTimestampMap();
            assertEquals(1, map.size());
            assertEquals(basePTable.getLastDDLTimestamp(), map.get(basePTable.getKey()));

            //verify client-2 sees maps directly through PhoenixRuntime, no query on baseTable2 or view2
            PTable basePTable2 = PhoenixRuntime.getTable(conn2, baseTable2);
            map = basePTable2.getAncestorLastDDLTimestampMap();
            assertEquals(0, map.size());
            assertEquals(1, basePTable2.getIndexes().size());
            map = basePTable2.getIndexes().get(0).getAncestorLastDDLTimestampMap();
            assertEquals(basePTable2.getLastDDLTimestamp(), map.get(basePTable2.getKey()));

            PTable viewPTable2 = PhoenixRuntime.getTable(conn2, view2);
            map = viewPTable2.getAncestorLastDDLTimestampMap();
            assertEquals(basePTable2.getLastDDLTimestamp(), map.get(basePTable2.getKey()));
            assertEquals(2, viewPTable2.getIndexes().size());
            for (PTable indexOfView : viewPTable2.getIndexes()) {
                // inherited index
                if (indexOfView.getTableName().getString().equals(index2)) {
                    map = indexOfView.getAncestorLastDDLTimestampMap();
                    assertEquals(basePTable2.getLastDDLTimestamp(), map.get(basePTable2.getKey()));
                } else {
                    // view index
                    map = indexOfView.getAncestorLastDDLTimestampMap();
                    assertEquals(basePTable2.getLastDDLTimestamp(), map.get(basePTable2.getKey()));
                    assertEquals(viewPTable2.getLastDDLTimestamp(), map.get(viewPTable2.getKey()));
                }
            }
        }
    }

    /**
     * Test that tenant connections are able to learn about state change of an inherited index
     * on their tenant views with different names.
     */
    @Test
    public void testInheritedIndexOnTenantViewsDifferentNames() throws Exception {
        testInheritedIndexOnTenantViews(false);
    }

    /**
     * Test that tenant connections are able to learn about state change of an inherited index
     * on their tenant views with same names.
     */
    @Test
    public void testInheritedIndexOnTenantViewsSameNames() throws Exception {
        testInheritedIndexOnTenantViews(true);
    }

    public void testInheritedIndexOnTenantViews(boolean sameTenantViewNames) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String url = QueryUtil.getConnectionUrl(props, config, "client1");
        ConnectionQueryServices cqs = driver.getConnectionQueryServices(url, props);
        String baseTableName =  generateUniqueName();
        String globalViewName = generateUniqueName();
        String globalViewIndexName =  generateUniqueName();
        String tenantViewName1 =  generateUniqueName();
        String tenantViewName2 =  sameTenantViewNames ? tenantViewName1 : generateUniqueName();
        try (Connection conn = cqs.connect(url, props)) {
            // create table, view and view index
            conn.createStatement().execute("CREATE TABLE " + baseTableName +
                    " (TENANT_ID CHAR(9) NOT NULL, KP CHAR(3) NOT NULL, PK CHAR(3) NOT NULL, KV CHAR(2), KV2 CHAR(2) " +
                    "CONSTRAINT PK PRIMARY KEY(TENANT_ID, KP, PK)) MULTI_TENANT=true,UPDATE_CACHE_FREQUENCY=NEVER");
            conn.createStatement().execute("CREATE VIEW " + globalViewName +
                    " AS SELECT * FROM " + baseTableName + " WHERE  KP = '001'");
            conn.createStatement().execute("CREATE INDEX " + globalViewIndexName + " on " +
                    globalViewName + " (KV) " + " INCLUDE (KV2) ASYNC");
            String tenantId1 = "tenantId1";
            String tenantId2 = "tenantId2";
            Properties tenantProps1 = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            Properties tenantProps2 = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            tenantProps1.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId1);
            tenantProps2.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId2);

            //create tenant views and upsert one row, this updates all the timestamps in the client's cache
            try (Connection tenantConn1 = cqs.connect(url, tenantProps1);
                 Connection tenantConn2 = cqs.connect(url, tenantProps2)) {
                tenantConn1.createStatement().execute("CREATE VIEW " + tenantViewName1 + " AS SELECT * FROM " + globalViewName);
                tenantConn1.createStatement().execute("UPSERT INTO " + tenantViewName1 + " (PK, KV, KV2) VALUES " + "('PK1', 'KV', '01')");
                tenantConn1.commit();

                tenantConn2.createStatement().execute("CREATE VIEW " + tenantViewName2 + " AS SELECT * FROM " + globalViewName);
                tenantConn2.createStatement().execute("UPSERT INTO " + tenantViewName2 + " (PK, KV, KV2) VALUES " + "('PK2', 'KV', '02')");
                tenantConn2.commit();
            }
            // build global view index
            IndexToolIT.runIndexTool(false, "", globalViewName,
                    globalViewIndexName);

            // query on secondary key should use inherited index for all tenant views.
            try (Connection tenantConn1 = cqs.connect(url, tenantProps1);
                 Connection tenantConn2 = cqs.connect(url, tenantProps2)) {

                String query1 = "SELECT KV2 FROM  " + tenantViewName1 + " WHERE KV = 'KV'";
                String query2 = "SELECT KV2 FROM  " + tenantViewName2 + " WHERE KV = 'KV'";

                ResultSet rs = tenantConn1.createStatement().executeQuery(query1);
                assertPlan((PhoenixResultSet) rs,  "",
                        tenantViewName1 + QueryConstants.CHILD_VIEW_INDEX_NAME_SEPARATOR + globalViewIndexName);
                assertTrue(rs.next());
                assertEquals("01", rs.getString(1));

                rs = tenantConn2.createStatement().executeQuery(query2);
                assertPlan((PhoenixResultSet) rs,  "",
                        tenantViewName2 + QueryConstants.CHILD_VIEW_INDEX_NAME_SEPARATOR + globalViewIndexName);
                assertTrue(rs.next());
                assertEquals("02", rs.getString(1));
            }
        }
    }

    /**
     * Test that client always refreshes its cache for DDL operations.
     * create view -> refresh base table
     * create child view -> refresh parent view and base table
     * add/drop column on table -> refresh table
     * add/drop column on view -> refresh view and base table
     */
    @Test
    public void testCacheUpdatedBeforeDDLOperations() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String url1 = QueryUtil.getConnectionUrl(props, config, "client1");
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        String viewName = generateUniqueName();
        String childViewName = generateUniqueName();
        int numTableRPCs = 0, numViewRPCs = 0;
        ConnectionQueryServices spyCqs1 = Mockito.spy(driver.getConnectionQueryServices(url1, props));
        try (Connection conn1 = spyCqs1.connect(url1, props)) {
            // create table
            createTable(conn1, tableName);

            // create index, getTable RPCs for base table
            createIndex(conn1, tableName, indexName, "v2");
            // getting current time,
            // create index(compile+execute),
            // alter index state after building(compile+execute)
            numTableRPCs += 5;
            assertNumGetTableRPC(spyCqs1, tableName, numTableRPCs);


            // create a view, getTable RPC for base table
            createView(conn1, tableName, viewName);
            numTableRPCs++;
            assertNumGetTableRPC(spyCqs1, tableName, numTableRPCs);

            // create a child view, getTable RPC for parent view and base table
            createView(conn1, viewName, childViewName);
            numTableRPCs++;
            numViewRPCs++;
            assertNumGetTableRPC(spyCqs1, tableName, numTableRPCs);
            assertNumGetTableRPC(spyCqs1, viewName, numViewRPCs);


            // add and drop column, 2 getTable RPCs for base table
            alterTableAddColumn(conn1, tableName, "newcol1");
            numTableRPCs++;
            alterTableDropColumn(conn1, tableName, "newcol1");
            numTableRPCs++;
            assertNumGetTableRPC(spyCqs1, tableName, numTableRPCs);

            // add and drop column, 2 getTable RPCs for view
            alterViewAddColumn(conn1, viewName, "newcol2");
            numViewRPCs++;
            numTableRPCs++;
            alterViewDropColumn(conn1, viewName, "newcol2");
            numViewRPCs++;
            numTableRPCs++;
            assertNumGetTableRPC(spyCqs1, viewName, numViewRPCs);
            assertNumGetTableRPC(spyCqs1, tableName, numTableRPCs);
        }
    }


    //Helper methods
    public static void assertNumGetTableRPC(ConnectionQueryServices spyCqs, String tableName, int numExpectedRPCs) throws SQLException {
        Mockito.verify(spyCqs, Mockito.times(numExpectedRPCs)).getTable(eq(null),
                any(byte[].class), eq(PVarchar.INSTANCE.toBytes(tableName)),
                anyLong(), anyLong());
    }
    public static void assertPlan(PhoenixResultSet rs, String schemaName, String tableName) {
        PTable table = rs.getContext().getCurrentTable().getTable();
        assertTrue(table.getSchemaName().getString().equals(schemaName) &&
                table.getTableName().getString().equals(tableName));
    }

    private long getLastDDLTimestamp(String tableName) throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        // Need to use different connection than what is used for creating table or indexes.
        String url = QueryUtil.getConnectionUrl(props, config, "client1");
        try (Connection conn = DriverManager.getConnection(url)) {
            PTable table = PhoenixRuntime.getTableNoCache(conn, tableName);
            return table.getLastDDLTimestamp();
        }
    }

    private void createTable(Connection conn, String tableName) throws SQLException {
        conn.createStatement().execute("CREATE TABLE " + tableName
                + "(k INTEGER NOT NULL PRIMARY KEY, v1 INTEGER, v2 INTEGER)");
    }

    private void createView(Connection conn, String parentName, String viewName) throws SQLException {
        conn.createStatement().execute("CREATE VIEW " + viewName + " AS SELECT * FROM " + parentName);
    }

    private void createViewWhereClause(Connection conn, String parentName, String viewName, String whereClause) throws SQLException {
        conn.createStatement().execute("CREATE VIEW " + viewName +
                " AS SELECT * FROM "+ parentName + whereClause);
    }

    private void createIndex(Connection conn, String tableName, String indexName, String col) throws SQLException {
        conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName + "(" + col + ")");
    }

    private void upsert(Connection conn, String tableName, boolean doCommit) throws SQLException {
        conn.createStatement().execute("UPSERT INTO " + tableName +
                " (k, v1, v2) VALUES ("+  RANDOM.nextInt() +", " + RANDOM.nextInt() + ", " + RANDOM.nextInt() +")");
        if (doCommit) {
            conn.commit();
        }
    }

    private void query(Connection conn, String tableName) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableName);
        rs.next();
    }

    private void queryWithIndex(Connection conn, String tableName) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("SELECT k FROM " + tableName + " WHERE v1=1");
        rs.next();
    }

    private void alterTableAddColumn(Connection conn, String tableName, String columnName) throws SQLException {
        conn.createStatement().execute("ALTER TABLE " + tableName + " ADD IF NOT EXISTS "
                + columnName + " INTEGER");
    }

    private void alterTableDropColumn(Connection conn, String tableName, String columnName) throws SQLException {
        conn.createStatement().execute("ALTER TABLE " + tableName + " DROP COLUMN " + columnName);
    }

    private void alterViewAddColumn(Connection conn, String viewName, String columnName) throws SQLException {
        conn.createStatement().execute("ALTER VIEW " + viewName + " ADD IF NOT EXISTS "
                + columnName + " INTEGER");
    }

    private void alterViewDropColumn(Connection conn, String viewName, String columnName) throws SQLException {
        conn.createStatement().execute("ALTER VIEW " + viewName + " DROP COLUMN  " + columnName);
    }

    private void alterIndexChangeState(Connection conn, String tableName, String indexName, String state) throws SQLException, InterruptedException {
        conn.createStatement().execute("ALTER INDEX " + indexName + " ON " + tableName + state);
    }

    private void dropIndex(Connection conn, String tableName, String indexName) throws SQLException {
        conn.createStatement().execute("DROP INDEX " + indexName + " ON " + tableName);
    }

    private void dropView(Connection conn, String viewName, boolean cascade) throws SQLException {
        String sql = "DROP VIEW " + viewName;
        if (cascade) {
            sql += " CASCADE";
        }
        conn.createStatement().execute(sql);
    }

    private void multiTableUpsert(Connection conn, String tableName1, String tableName2) throws SQLException {
        conn.createStatement().execute("UPSERT INTO " + tableName1 +
                " (k, v1, v2) VALUES ("+  RANDOM.nextInt() +", " + RANDOM.nextInt() + ", " + RANDOM.nextInt() +")");
        conn.createStatement().execute("UPSERT INTO " + tableName1 +
                " (k, v1, v2) VALUES ("+  RANDOM.nextInt() +", " + RANDOM.nextInt() + ", " + RANDOM.nextInt() +")");
        conn.createStatement().execute("UPSERT INTO " + tableName2 +
                " (k, v1, v2) VALUES ("+  RANDOM.nextInt() +", " + RANDOM.nextInt() + ", " + RANDOM.nextInt() +")");
        conn.createStatement().execute("UPSERT INTO " + tableName1 +
                " (k, v1, v2) VALUES ("+  RANDOM.nextInt() +", " + RANDOM.nextInt() + ", " + RANDOM.nextInt() +")");
        conn.createStatement().execute("UPSERT INTO " + tableName2 +
                " (k, v1, v2) VALUES ("+  RANDOM.nextInt() +", " + RANDOM.nextInt() + ", " + RANDOM.nextInt() +")");
        conn.commit();
    }
}
