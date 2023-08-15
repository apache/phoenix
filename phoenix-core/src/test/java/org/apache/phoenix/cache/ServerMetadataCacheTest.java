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
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@Category(NeedsOwnMiniClusterTest.class)
public class ServerMetadataCacheTest extends BaseTest {
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
}
