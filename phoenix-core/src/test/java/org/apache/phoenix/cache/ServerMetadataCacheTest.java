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

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.PhoenixRegionServerEndpoint;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.ConnectionProperty;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
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
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static org.apache.hadoop.hbase.coprocessor.CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@Category(ParallelStatsDisabledIT.class)
public class ServerMetadataCacheTest extends ParallelStatsDisabledIT {

    private final Random RANDOM = new Random(42);
    private final long NEVER = (long) ConnectionProperty.UPDATE_CACHE_FREQUENCY.getValue("NEVER");
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerMetadataCacheTest.class);

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(REGIONSERVER_COPROCESSOR_CONF_KEY,
                PhoenixRegionServerEndpoint.class.getName());
        props.put(QueryServices.LAST_DDL_TIMESTAMP_VALIDATION_ENABLED, Boolean.toString(true));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @After
    public void resetMetadataCache() {
        ServerMetadataCache.resetCache();
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
            createTable(conn, tableNameStr, NEVER);
            pTable = PhoenixRuntime.getTableNoCache(conn,
                    tableNameStr);// --> First call to CQSI#getTable
            ServerMetadataCache cache = ServerMetadataCache.getInstance(config);
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
            createTable(conn, tableNameStr, NEVER);
            // Create view on table.
            String whereClause = " WHERE v1 = 1000";
            String viewNameStr = generateUniqueName();
            createViewWhereClause(conn, tableNameStr, viewNameStr, whereClause);
            viewTable = PhoenixRuntime.getTableNoCache(conn, viewNameStr);  // --> First call to CQSI#getTable
            ServerMetadataCache cache = ServerMetadataCache.getInstance(config);
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
            createTable(conn, tableNameStr, NEVER);
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
            ServerMetadataCache cache = ServerMetadataCache.getInstance(config);
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
            createTable(conn, tableNameStr, NEVER);
            pTable = PhoenixRuntime.getTableNoCache(conn, tableNameStr);
            ServerMetadataCache cache = ServerMetadataCache.getInstance(config);
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
            createTable(conn, fullTableName, NEVER);
            pTable = PhoenixRuntime.getTableNoCache(conn, fullTableName);
            ServerMetadataCache cache = ServerMetadataCache.getInstance(config);
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
            createTable(conn, tableNameStr, NEVER);
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
            ServerMetadataCache cache = ServerMetadataCache.getInstance(config);
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
        ServerMetadataCache cache = ServerMetadataCache.getInstance(config);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            // Create a test table.
            createTable(conn, tableNameStr, NEVER);
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
        ServerMetadataCache cache = ServerMetadataCache.getInstance(config);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            // Create a test table.
            createTable(conn, tableNameStr, NEVER);
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
        String tableNameStr = "TBL_" + generateUniqueName();
        String indexNameStr = "IND_" + generateUniqueName();
        byte[] indexNameBytes = Bytes.toBytes(indexNameStr);
        PTable indexTable;
        ServerMetadataCache cache = ServerMetadataCache.getInstance(config);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            // Create a test table.
            createTable(conn, tableNameStr, NEVER);
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
            createTable(conn1, tableName, NEVER);
            upsert(conn1, tableName);

            // select query from client-2 works to populate client side metadata cache
            // there should be 1 update to the client cache
            query(conn2, tableName);
            expectedNumCacheUpdates = 1;
            Mockito.verify(spyCqs2, Mockito.times(expectedNumCacheUpdates))
                    .addTable(any(PTable.class), anyLong());

            // add column using client-1 to update last ddl timestamp
            alterTableAddColumn(conn1, tableName, "newCol1");

            // invalidate region server cache
            // TODO: remove this call after PHOENIX-6968 is committed.
            ServerMetadataCache.resetCache();

            // reset the spy CQSI object
            Mockito.reset(spyCqs2);

            // select query from client-2 with old ddl timestamp works
            // there should be one update to the client cache
            query(conn2, tableName);
            expectedNumCacheUpdates = 1;
            Mockito.verify(spyCqs2, Mockito.times(expectedNumCacheUpdates))
                    .addTable(any(PTable.class), anyLong());

            // select query from client-2 with latest ddl timestamp works
            // there should be no more updates to client cache
            query(conn2, tableName);
            Mockito.verify(spyCqs2, Mockito.times(expectedNumCacheUpdates))
                    .addTable(any(PTable.class), anyLong());
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
        ServerMetadataCache cache = null;

        try (Connection conn1 = spyCqs1.connect(url1, props);
             Connection conn2 = spyCqs2.connect(url2, props)) {

            // create table and upsert using client-1
            createTable(conn1, tableName, NEVER);
            upsert(conn1, tableName);

            // Instrument ServerMetadataCache to throw a SQLException once
            cache = ServerMetadataCache.getInstance(config);
            ServerMetadataCache spyCache = Mockito.spy(cache);
            Mockito.doThrow(new SQLException("FAIL")).doCallRealMethod().when(spyCache)
                    .getLastDDLTimestampForTable(any(), any(), eq(Bytes.toBytes(tableName)));
            ServerMetadataCache.setInstance(spyCache);

            // query using client-2 should succeed
            query(conn2, tableName);

            // verify live region servers were refreshed
            Mockito.verify(spyCqs2, Mockito.times(1)).refreshLiveRegionServers();
        }
    }

    /**
     * Test Select query with old ddl timestamp and ddl timestamp validation encounters an exception.
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
        ServerMetadataCache cache = null;

        try (Connection conn1 = spyCqs1.connect(url1, props);
             Connection conn2 = spyCqs2.connect(url2, props)) {

            // create table and upsert using client-1
            createTable(conn1, tableName, NEVER);
            upsert(conn1, tableName);

            // query using client-2 to populate cache
            query(conn2, tableName);
            expectedNumCacheUpdates = 1;
            Mockito.verify(spyCqs2, Mockito.times(expectedNumCacheUpdates))
                    .addTable(any(PTable.class), anyLong());

            // add column using client-1 to update last ddl timestamp
            alterTableAddColumn(conn1, tableName, "newCol1");

            // invalidate region server cache
            // TODO: remove this call after PHOENIX-6968 is committed.
            ServerMetadataCache.resetCache();

            // reset the spy CQSI object
            Mockito.reset(spyCqs2);

            // Instrument ServerMetadataCache to throw a SQLException once
            cache = ServerMetadataCache.getInstance(config);
            ServerMetadataCache spyCache = Mockito.spy(cache);
            Mockito.doThrow(new SQLException("FAIL")).doCallRealMethod().when(spyCache)
                    .getLastDDLTimestampForTable(any(), any(), eq(Bytes.toBytes(tableName)));
            ServerMetadataCache.setInstance(spyCache);

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
        ServerMetadataCache cache = null;

        try (Connection conn1 = spyCqs1.connect(url1, props);
             Connection conn2 = spyCqs2.connect(url2, props)) {

            // create table and upsert using client-1
            createTable(conn1, tableName, NEVER);
            upsert(conn1, tableName);

            // Instrument ServerMetadataCache to throw a SQLException twice
            cache = ServerMetadataCache.getInstance(config);
            ServerMetadataCache spyCache = Mockito.spy(cache);
            SQLException e = new SQLException("FAIL");
            Mockito.doThrow(e).doThrow(e).when(spyCache)
                    .getLastDDLTimestampForTable(any(), any(), eq(Bytes.toBytes(tableName)));
            ServerMetadataCache.setInstance(spyCache);

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
            createTable(conn1, tableName, NEVER);
            upsert(conn1, tableName);

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

            // invalidate region server cache
            // TODO: remove this call after PHOENIX-6968 is committed.
            ServerMetadataCache.resetCache();

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


    private void createViewWhereClause(Connection conn, String parentName, String viewName, String whereClause) throws SQLException {
        conn.createStatement().execute("CREATE VIEW " + viewName +
                " AS SELECT * FROM "+ parentName + whereClause);
    }


    //Helper methods

    private void createTable(Connection conn, String tableName, long updateCacheFrequency) throws SQLException {
        conn.createStatement().execute("CREATE TABLE " + tableName
                + "(k INTEGER NOT NULL PRIMARY KEY, v1 INTEGER, v2 INTEGER)"
                + (updateCacheFrequency == 0 ? "" : "UPDATE_CACHE_FREQUENCY="+updateCacheFrequency));
    }

    private void createView(Connection conn, String parentName, String viewName) throws SQLException {
        conn.createStatement().execute("CREATE VIEW " + viewName + " AS SELECT * FROM " + parentName);
    }

    private void upsert(Connection conn, String tableName) throws SQLException {
        conn.createStatement().execute("UPSERT INTO " + tableName +
                " (k, v1, v2) VALUES ("+  RANDOM.nextInt() +", " + RANDOM.nextInt() + ", " + RANDOM.nextInt() +")");
        conn.commit();
    }

    private void query(Connection conn, String tableName) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableName);
        rs.next();
    }

    private void alterTableAddColumn(Connection conn, String tableName, String columnName) throws SQLException {
        conn.createStatement().execute("ALTER TABLE " + tableName + " ADD IF NOT EXISTS "
                + columnName + " INTEGER");
    }

    private void alterViewAddColumn(Connection conn, String viewName, String columnName) throws SQLException {
        conn.createStatement().execute("ALTER VIEW " + viewName + " ADD IF NOT EXISTS "
                + columnName + " INTEGER");
    }
}
