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


import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.cache.ServerMetadataCache;
import org.apache.phoenix.coprocessor.PhoenixRegionServerEndpoint;
import org.apache.phoenix.exception.PhoenixIOException;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.ConnectionProperty;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static org.apache.hadoop.hbase.coprocessor.CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;

@Category({NeedsOwnMiniClusterTest.class })
public class ServerMetadataCachingIT extends BaseTest {

    private final Random RANDOM = new Random(42);
    private final long NEVER = (long) ConnectionProperty.UPDATE_CACHE_FREQUENCY.getValue("NEVER");

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(REGIONSERVER_COPROCESSOR_CONF_KEY,
                PhoenixRegionServerEndpoint.class.getName());
        props.put(QueryServices.LAST_DDL_TIMESTAMP_VALIDATION_ENABLED, Boolean.toString(true));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

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
            ServerMetadataCache.resetCache();

            // select query from client-2 with old ddl timestamp works
            // there should be one more update to the client cache
            query(conn2, tableName);
            expectedNumCacheUpdates += 1;
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
        finally {
            // reset cache instance so that it does not interfere with any other test
            ServerMetadataCache.setInstance(cache);
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
            ServerMetadataCache.resetCache();

            // Instrument ServerMetadataCache to throw a SQLException once
            cache = ServerMetadataCache.getInstance(config);
            ServerMetadataCache spyCache = Mockito.spy(cache);
            Mockito.doThrow(new SQLException("FAIL")).doCallRealMethod().when(spyCache)
                    .getLastDDLTimestampForTable(any(), any(), eq(Bytes.toBytes(tableName)));
            ServerMetadataCache.setInstance(spyCache);

            // query using client-2 should succeed, one additional cache update
            query(conn2, tableName);
            expectedNumCacheUpdates += 1;
            Mockito.verify(spyCqs2, Mockito.times(expectedNumCacheUpdates))
                    .addTable(any(PTable.class), anyLong());

            // verify live region servers were refreshed
            Mockito.verify(spyCqs2, Mockito.times(1)).refreshLiveRegionServers();
        }
        finally {
            // reset cache instance so that it does not interfere with any other test
            ServerMetadataCache.setInstance(cache);
        }
    }

    /**
     * Test Select Query fails in case Admin API throws IOException twice.
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

            // Instrument ServerMetadataCache to throw a SQLException once
            cache = ServerMetadataCache.getInstance(config);
            ServerMetadataCache spyCache = Mockito.spy(cache);
            Mockito.doThrow(new SQLException("FAIL")).doCallRealMethod().when(spyCache)
                    .getLastDDLTimestampForTable(any(), any(), eq(Bytes.toBytes(tableName)));
            ServerMetadataCache.setInstance(spyCache);

            // instrument CQSI to throw a SQLException once when live region servers are refreshed
            Mockito.doThrow(new SQLException("FAIL")).when(spyCqs2).refreshLiveRegionServers();

            // query using client-2 should fail
            query(conn2, tableName);
            Assert.fail("Query should have thrown Exception");
        }
        catch (Exception e) {
            Assert.assertTrue("PhoenixIOException was not thrown when CQSI.getAdmin encountered errors.", e instanceof PhoenixIOException);
        }
        finally {
            // reset cache instance so that it does not interfere with any other test
            ServerMetadataCache.setInstance(cache);
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
            ServerMetadataCache.resetCache();

            // query second level view
            query(conn2, view2);
            expectedNumCacheUpdates += 3; // table, view1
            Mockito.verify(spyCqs2, Mockito.times(expectedNumCacheUpdates))
                    .addTable(any(PTable.class), anyLong());
        }
    }
}
