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
package org.apache.phoenix.query;

import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.monitoring.GlobalClientMetrics;
import org.apache.phoenix.schema.*;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.RunUntilFailure;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

@RunWith(RunUntilFailure.class)
@Category(NeedsOwnMiniClusterTest.class)
public class MetaDataCachingIT extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetaDataCachingIT.class);
    private final Random RAND = new Random(11);

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        // We set here a tiny cache to verify that even if the total size of the cache is just enough to hold
        // system tables and Phoenix is still functional. Please note the cache weight for system tables is set to
        // zero to allow insertion of system tables even when the cache reaches its maximum weight.
        props.put(QueryServices.MAX_CLIENT_METADATA_CACHE_SIZE_ATTRIB, "50000");
        props.put(QueryServices.CLIENT_CACHE_ENCODING, "object");
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    protected void createTable(Connection conn, String tableName, long updateCacheFrequency) throws SQLException {
        conn.createStatement().execute("CREATE TABLE " + tableName
                + "(k INTEGER NOT NULL PRIMARY KEY, v1 INTEGER, v2 INTEGER, v3 VARCHAR, v4 Date, "
                +" v5 BIGINT, v6 SMALLINT)" + (updateCacheFrequency == 0 ? "" : "UPDATE_CACHE_FREQUENCY="+updateCacheFrequency));
    }

    private void upsert(Connection conn, String tableName) throws SQLException {
        conn.createStatement().execute("UPSERT INTO " + tableName +
                " (k, v1, v1) VALUES ("+  RAND.nextInt() +", " + RAND.nextInt() + ", " + RAND.nextInt() +")");
        conn.commit();
    }

    private void query(Connection conn, String tableName) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableName);
        rs.next();
    }

    private String[] simulateWorkload(String testName, int numTables, int numThreads, int numMaxDML) throws Exception {
        String[] tableNames = new String[numTables];
        for (int i = 0; i < numTables; i++) {
            tableNames[i] = generateUniqueName();
            try (Connection conn = DriverManager.getConnection(getUrl())) {
                createTable(conn, tableNames[i], (i%2) == 0 ? 0 : 100000);
            }
        }

        final CountDownLatch doneSignal = new CountDownLatch(numThreads);
        Runnable[] runnables = new Runnable[numThreads];
        for (int i = 0; i < numThreads; i++) {
            runnables[i] = new Runnable() {

                @Override public void run() {
                    try (Connection conn = DriverManager.getConnection(getUrl())) {
                        for (int i = 0; i < numMaxDML; i++) {
                            upsert(conn, tableNames[RAND.nextInt(numTables)]);
                            query(conn, tableNames[RAND.nextInt(numTables)]);
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    } finally {
                        doneSignal.countDown();
                    }
                }

            };
        }
        for (int i = 0; i < numThreads; i++) {
            Thread t = new Thread(runnables[i]);
            t.start();
        }

        assertTrue("Ran out of time for test " + testName, doneSignal.await(120, TimeUnit.SECONDS));
        return tableNames;
    }

    @Test
    public void testSystemTablesAreInCache() throws Exception {
        simulateWorkload("testSystemTablesAreInCache", 10, 10, 10);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            ResultSet rs = conn.getMetaData().getTables("", null, null, new String[]{PTableType.SYSTEM.toString()});
            PMetaData pMetaData = conn.unwrap(PhoenixConnection.class).getMetaDataCache();
            while (rs.next()) {
                String tableName = rs.getString(PhoenixDatabaseMetaData.TABLE_SCHEM) + QueryConstants.NAME_SEPARATOR + rs.getString(PhoenixDatabaseMetaData.TABLE_NAME);
                try {
                    pMetaData.getTableRef(new PTableKey(null, tableName));
                }
                catch (TableNotFoundException e) {
                    fail("System table " + tableName + " should be in the cache");
                }
            }
        }
    }

    /*
    TODO: The tables with zero update cache frequency should not be inserted to the cache. However, Phoenix
    uses the cache as the temporary memory during all operations currently. When this behavior changes,
    this test should be updated with the appropriate number of hits/misses.
     */
    @Test
    public void testGlobalClientCacheMetrics() throws Exception {
        int numThreads = 5;
        int numTables = 1;
        int numMaxDML = 2;

        GlobalClientMetrics.GLOBAL_CLIENT_METADATA_CACHE_MISS_COUNTER.getMetric().reset();
        GlobalClientMetrics.GLOBAL_CLIENT_METADATA_CACHE_HIT_COUNTER.getMetric().reset();

        simulateWorkload("testGlobalClientCacheMetrics", numTables, numThreads, numMaxDML);

        // only 1 miss when the table is created
        assertEquals("Incorrect number of client metadata cache misses",
                1, GlobalClientMetrics.GLOBAL_CLIENT_METADATA_CACHE_MISS_COUNTER.getMetric().getValue());

        // (2 hits per upsert + 1 hit per select) per thread
        assertEquals("Incorrect number of client metadata cache hits",
                3*numMaxDML*numThreads, GlobalClientMetrics.GLOBAL_CLIENT_METADATA_CACHE_HIT_COUNTER.getMetric().getValue());
    }

    /*
    The tables with zero update cache frequency should not be inserted to the cache. However, Phoenix
    uses the cache as the temporary memory during all operations currently. When this behavior changes,
    this test can be enabled.
     */
    @Ignore
    @Test
    public void testCacheShouldBeUsedOnlyForConfiguredTables() throws Exception {
        String[] tableNames = simulateWorkload("testCacheShouldBeUsedOnlyForConfiguredTables", 25, 10, 4);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            int hitCount = 0;
            for (int i = 0; i < tableNames.length; i++) {
                PMetaData metaDataCache = conn.unwrap(PhoenixConnection.class).getMetaDataCache();
                PTableRef tableRef = null;
                try {
                    tableRef = metaDataCache.getTableRef(new PTableKey(null, tableNames[i]));
                } catch (TableNotFoundException e) {}
                if (i%2 == 0) {
                    // Cache should not be used for the odd numbered tables
                    assertTrue(tableRef == null);
                } else if (tableRef != null) {
                    hitCount++;
                }
            }
            assertTrue(hitCount > 0);
        }
    }
}
