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
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

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
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerMetadataCacheTest.class);

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
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
            String ddl = getCreateTableStmt(tableNameStr);
            // Create a test table.
            conn.createStatement().execute(ddl);
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
            String ddl = getCreateTableStmt(tableNameStr);
            // Create a test table.
            conn.createStatement().execute(ddl);
            // Create view on table.
            String whereClause = " WHERE COL1 = 1000";
            String viewNameStr = generateUniqueName();
            conn.createStatement().execute(getCreateViewStmt(viewNameStr, tableNameStr, whereClause));
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
            String ddl = getCreateTableStmt(tableNameStr);
            // Create a test table.
            conn.createStatement().execute(ddl);
        }
        String tenantId = "T_" + generateUniqueName();
        Properties tenantProps = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        PTable tenantViewTable;
        // Create view on table.
        String whereClause = " WHERE COL1 = 1000";
        String tenantViewNameStr = generateUniqueName();
        ConnectionQueryServices spyCQS = Mockito.spy(driver.getConnectionQueryServices(getUrl(),
                PropertiesUtil.deepCopy(TEST_PROPERTIES)));
        try (Connection conn = spyCQS.connect(getUrl(), tenantProps)) {
            conn.createStatement().execute(getCreateViewStmt(tenantViewNameStr,
                    tableNameStr, whereClause));
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
            String ddl = getCreateTableStmt(tableNameStr);
            // Create a test table.
            conn.createStatement().execute(ddl);
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
            String ddl = getCreateTableStmt(fullTableName);
            // Create a test table.
            conn.createStatement().execute(ddl);
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
            String ddl = getCreateTableStmt(tableNameStr);
            // Create a test table.
            conn.createStatement().execute(ddl);
        }
        String tenantId = "T_" + generateUniqueName();
        Properties tenantProps = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        PTable tenantViewTable;
        // Create view on table.
        String whereClause = " WHERE COL1 = 1000";
        String tenantViewNameStr = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), tenantProps)) {
            conn.createStatement().execute(getCreateViewStmt(tenantViewNameStr,
                    tableNameStr, whereClause));
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
            String ddl = getCreateTableStmt(tableNameStr);
            // Create a test table.
            conn.createStatement().execute(ddl);
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
            String ddl = getCreateTableStmt(tableNameStr);
            // Create a test table.
            conn.createStatement().execute(ddl);
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
            String ddl = getCreateTableStmt(tableNameStr);
            // Create a test table.
            conn.createStatement().execute(ddl);
            String indexDDLStmt = "CREATE INDEX " + indexNameStr + " ON " + tableNameStr + "(col1)";
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
        ServerMetadataCache cache = ServerMetadataCache.getInstance(config);
        String ddl =
                "create table  " + tableName + " ( k integer PRIMARY KEY," + " v1 integer,"
                        + " v2 integer)";
        String createIndexDDL = "create index  " + indexName + " on " + tableName + " (v1)";
        String dropIndexDDL = "DROP INDEX " + indexName + " ON " + tableName;
        try (Connection conn = DriverManager.getConnection(getUrl());
             Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(true);
            stmt.execute(ddl);
            long tableLastDDLTimestampBeforeIndexCreation = getLastDDLTimestamp(tableName);
            // Populate the cache
            assertNotNull(cache.getLastDDLTimestampForTable(null, null, tableNameBytes));
            Thread.sleep(1);
            stmt.execute(createIndexDDL);
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
            Thread.sleep(1);
            stmt.execute(dropIndexDDL);
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
     * @throws Exception
     */
    @Test
    public void testUpdateLastDDLTimestampViewAfterIndexCreation() throws Exception {
        String tableName = "T_" + generateUniqueName();
        String globalViewName = "GV_" + generateUniqueName();
        byte[] globalViewNameBytes = Bytes.toBytes(globalViewName);
        String globalViewIndexName = "GV_IDX_" + generateUniqueName();
        byte[] globalViewIndexNameBytes = Bytes.toBytes(globalViewIndexName);

        ServerMetadataCache cache = ServerMetadataCache.getInstance(config);
        try(Connection conn = DriverManager.getConnection(getUrl());
            Statement stmt = conn.createStatement()) {
            String whereClause = " WHERE COL1 < 1000";
            String tableDDLStmt = getCreateTableStmt(tableName);
            String viewDDLStmt = getCreateViewStmt(globalViewName, tableName, whereClause);
            String viewIdxDDLStmt = getCreateViewIndexStmt(globalViewIndexName, globalViewName,
                    "COL1");
            String dropIndexDDL = "DROP INDEX " + globalViewIndexName + " ON " + globalViewName;
            stmt.execute(tableDDLStmt);
            stmt.execute(viewDDLStmt);
            // Populate the cache
            assertNotNull(cache.getLastDDLTimestampForTable(null, null, globalViewNameBytes));
            long viewLastDDLTimestampBeforeIndexCreation = getLastDDLTimestamp(globalViewName);
            stmt.execute(viewIdxDDLStmt);

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
            Thread.sleep(1);
            stmt.execute(dropIndexDDL);
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

    public long getLastDDLTimestamp(String tableName) throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        // Need to use different connection than what is used for creating table or indexes.
        String url = QueryUtil.getConnectionUrl(props, config, "client1");
        try (Connection conn = DriverManager.getConnection(url)) {
            PTable table = PhoenixRuntime.getTableNoCache(conn, tableName);
            return table.getLastDDLTimestamp();
        }
    }


    private String getCreateTableStmt(String tableName) {
        return   "CREATE TABLE " + tableName +
                "  (a_string varchar not null, col1 integer" +
                "  CONSTRAINT pk PRIMARY KEY (a_string)) ";
    }

    private String getCreateViewStmt(String viewName, String fullTableName, String whereClause) {
        String viewStmt =  "CREATE VIEW " + viewName +
                " AS SELECT * FROM "+ fullTableName + whereClause;
        return  viewStmt;
    }

    private String getCreateViewIndexStmt(String indexName, String viewName, String indexColumn) {
        String viewIndexName =
                "CREATE INDEX " + indexName + " ON " + viewName + "(" + indexColumn + ")";
        return viewIndexName;
    }
}
