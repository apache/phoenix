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
package org.apache.phoenix.end2end.index;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionStatesCount;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.exception.PhoenixParserException;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.iterate.ScanningResultPostDummyResultCaller;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.phoenix.end2end.index.GlobalIndexCheckerIT.assertExplainPlan;
import static org.apache.phoenix.end2end.index.GlobalIndexCheckerIT.assertExplainPlanWithLimit;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class UncoveredGlobalIndexRegionScanner2IT extends BaseTest {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(UncoveredGlobalIndexRegionScanner2IT.class);

    private final boolean uncovered;
    private final boolean salted;

    public UncoveredGlobalIndexRegionScanner2IT(boolean uncovered, boolean salted) {
        this.uncovered = uncovered;
        this.salted = salted;
    }

    protected static boolean hasTestStarted = false;
    protected static int countOfDummyResults = 0;
    protected static final Set<String> TABLE_NAMES = new HashSet<>();

    protected static class TestScanningResultPostDummyResultCaller extends
            ScanningResultPostDummyResultCaller {

        @Override
        public void postDummyProcess() {
            if (hasTestStarted && (countOfDummyResults++ % 3) == 0 &&
                    (countOfDummyResults < 17 ||
                            countOfDummyResults > 28 && countOfDummyResults < 40)) {
                LOGGER.info("Moving regions of tables {}. current count of dummy results: {}",
                        TABLE_NAMES, countOfDummyResults);
                TABLE_NAMES.forEach(table -> {
                    try {
                        moveRegionsOfTable(table);
                    } catch (Exception e) {
                        LOGGER.error("Unable to move regions of table: {}", table);
                    }
                });
            }
        }
    }

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
                Integer.toString(60 * 60)); // An hour
        props.put(QueryServices.USE_STATS_FOR_PARALLELIZATION, Boolean.toString(false));
        props.put(QueryServices.PHOENIX_SERVER_PAGE_SIZE_MS, Long.toString(0));
        props.put(QueryServices.TESTS_MINI_CLUSTER_NUM_REGION_SERVERS, String.valueOf(2));
        props.put(HConstants.HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY, String.valueOf(1));
        props.put(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB,
                Long.toString(0));
        props.put(QueryServices.PHOENIX_POST_DUMMY_PROCESS,
                TestScanningResultPostDummyResultCaller.class.getName());
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @AfterClass
    public static synchronized void freeResources() throws Exception {
        BaseTest.freeResourcesIfBeyondThreshold();
    }

    protected static void moveRegionsOfTable(String tableName)
            throws IOException {
        try (AsyncConnection asyncConnection =
                     ConnectionFactory.createAsyncConnection(getUtility().getConfiguration())
                             .get()) {
            AsyncAdmin admin = asyncConnection.getAdmin();
            List<ServerName> servers =
                    new ArrayList<>(admin.getRegionServers().get());
            ServerName server1 = servers.get(0);
            ServerName server2 = servers.get(1);
            List<RegionInfo> regionsOnServer1;
            regionsOnServer1 = admin.getRegions(server1).get();
            List<RegionInfo> regionsOnServer2;
            regionsOnServer2 = admin.getRegions(server2).get();
            regionsOnServer1.forEach(regionInfo -> {
                if (regionInfo.getTable().equals(TableName.valueOf(tableName))) {
                    try {
                        for (int i = 0; i < 2; i++) {
                            RegionStatesCount regionStatesCount =
                                    admin.getClusterMetrics().get().getTableRegionStatesCount()
                                            .get(TableName.valueOf(tableName));
                            if (regionStatesCount.getRegionsInTransition() == 0 &&
                                    regionStatesCount.getOpenRegions() ==
                                            regionStatesCount.getTotalRegions()) {
                                LOGGER.info("Moving region {} to {}",
                                        regionInfo.getRegionNameAsString(), server2);
                                admin.move(regionInfo.getEncodedNameAsBytes(), server2).get(3,
                                        TimeUnit.SECONDS);
                                break;
                            } else {
                                LOGGER.info("Table {} has some region(s) in RIT or not online",
                                        tableName);
                            }
                        }
                    } catch (InterruptedException | ExecutionException | TimeoutException e) {
                        LOGGER.error("Something went wrong", e);
                        throw new RuntimeException(e);
                    }
                }
            });
            regionsOnServer2.forEach(regionInfo -> {
                if (regionInfo.getTable().equals(TableName.valueOf(tableName))) {
                    try {
                        for (int i = 0; i < 2; i++) {
                            RegionStatesCount regionStatesCount =
                                    admin.getClusterMetrics().get().getTableRegionStatesCount()
                                            .get(TableName.valueOf(tableName));
                            if (regionStatesCount.getRegionsInTransition() == 0 &&
                                    regionStatesCount.getOpenRegions() ==
                                            regionStatesCount.getTotalRegions()) {
                                admin.move(regionInfo.getEncodedNameAsBytes(), server1)
                                        .get(3, TimeUnit.SECONDS);
                                LOGGER.info("Moving region {} to {}",
                                        regionInfo.getRegionNameAsString(), server1);
                                break;
                            } else {
                                LOGGER.info("Table {} has some region(s) in RIT or not online",
                                        tableName);
                            }
                        }
                    } catch (InterruptedException | ExecutionException | TimeoutException e) {
                        LOGGER.error("Something went wrong", e);
                        throw new RuntimeException(e);
                    }
                }
            });
        } catch (Exception e) {
            LOGGER.error("Something went wrong..", e);
        }
    }

    @Before
    public void setUp() throws Exception {
        hasTestStarted = true;
    }

    @After
    public void tearDown() throws Exception {
        countOfDummyResults = 0;
        TABLE_NAMES.clear();
        hasTestStarted = false;
    }

    @Parameterized.Parameters(
            name = "uncovered={0},salted={1}")
    public static synchronized Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][]{
                {false, false}, {false, true}, {true, false}, {true, true}
        });
    }

    private void populateTable(String tableName) throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("create table " + tableName +
                " (id varchar(10) not null primary key, val1 varchar(10), val2 varchar(10)," +
                " val3 varchar(10))" + (salted ? " SALT_BUCKETS=4" : ""));
        conn.createStatement()
                .execute("upsert into " + tableName + " values ('a', 'ab', 'abc', 'abcd')");
        conn.commit();
        conn.createStatement()
                .execute("upsert into " + tableName + " values ('b', 'bc', 'bcd', 'bcde')");
        conn.commit();
        conn.close();
    }

    @Test
    public void testDDL() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            String indexTableName = generateUniqueName();
            conn.createStatement().execute("create table " + dataTableName +
                    " (id varchar(10) not null primary key, val1 varchar(10), val2 varchar(10)," +
                    " val3 varchar(10))" + (salted ? " SALT_BUCKETS=4" : ""));
            if (uncovered) {
                // The INCLUDE clause should not be allowed
                try {
                    conn.createStatement().execute("CREATE UNCOVERED INDEX " + indexTableName
                            + " on " + dataTableName + " (val1) INCLUDE (val2)");
                    Assert.fail();
                } catch (PhoenixParserException e) {
                    // Expected
                }
                // The LOCAL keyword should not be allowed with UNCOVERED
                try {
                    conn.createStatement().execute("CREATE UNCOVERED LOCAL INDEX " + indexTableName
                            + " on " + dataTableName);
                    Assert.fail();
                } catch (PhoenixParserException e) {
                    // Expected
                }
            } else {
                // The INCLUDE clause should be allowed
                conn.createStatement().execute("CREATE INDEX " + indexTableName
                        + " on " + dataTableName + " (val1) INCLUDE (val2)");
            }
        }
    }

    @Test
    public void testDDLWithPhoenixRowTimestamp() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            conn.createStatement().execute("create table " + dataTableName +
                    " (id varchar(10) not null primary key)" + (salted ? " SALT_BUCKETS=4" : ""));
            if (uncovered) {
                conn.createStatement().execute("CREATE UNCOVERED INDEX IDX_" + dataTableName
                        + " on " + dataTableName + " (PHOENIX_ROW_TIMESTAMP())");
            } else {
                conn.createStatement().execute("CREATE INDEX IDX_" + dataTableName
                        + " on " + dataTableName + " (PHOENIX_ROW_TIMESTAMP())");
                conn.createStatement().execute("CREATE LOCAL INDEX IDX_LOCAL_" + dataTableName
                        + " on " + dataTableName + " (PHOENIX_ROW_TIMESTAMP())");
            }
        }
    }

    @Test
    public void testUncoveredQueryWithPhoenixRowTimestamp() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            String indexTableName = generateUniqueName();
            TABLE_NAMES.add(dataTableName);
            TABLE_NAMES.add(indexTableName);
            Timestamp initial = new Timestamp(EnvironmentEdgeManager.currentTimeMillis() - 1);
            conn.createStatement().execute("create table " + dataTableName +
                    " (id varchar(10) not null primary key, val1 varchar(10), val2 varchar(10), " +
                    " val3 varchar(10))" + (salted ? " SALT_BUCKETS=4" : ""));
            conn.createStatement()
                    .execute("upsert into " + dataTableName + " values ('a', 'ab', 'abc', 'abcd')");
            conn.commit();
            Timestamp before = new Timestamp(EnvironmentEdgeManager.currentTimeMillis());
            // Sleep 1ms to get a different row timestamps
            Thread.sleep(1);
            conn.createStatement()
                    .execute("upsert into " + dataTableName + " values ('b', 'bc', 'bcd', 'bcde')");
            conn.commit();
            Timestamp after = new Timestamp(EnvironmentEdgeManager.currentTimeMillis() + 1);
            conn.createStatement().execute(
                    "CREATE " + (uncovered ? "UNCOVERED " : " ") + "INDEX " + indexTableName
                            + " on " + dataTableName + " (val1, PHOENIX_ROW_TIMESTAMP()) ");

            String timeZoneID = Calendar.getInstance().getTimeZone().getID();
            // Write a query to get the val2 = 'bc' with a time range query
            String query = "SELECT" +
                    (uncovered ? " " : "/*+ INDEX(" + dataTableName + " " + indexTableName + ")*/ ")
                    + "val1, val2, PHOENIX_ROW_TIMESTAMP() from " + dataTableName
                    + " WHERE val1 = 'bc' AND " + "PHOENIX_ROW_TIMESTAMP() > TO_DATE('"
                    + before.toString() + "','yyyy-MM-dd HH:mm:ss.SSS', '"
                    + timeZoneID + "') AND " + "PHOENIX_ROW_TIMESTAMP() < TO_DATE('" + after
                    + "','yyyy-MM-dd HH:mm:ss.SSS', '" + timeZoneID + "')";
            // Verify that we will read from the index table
            assertExplainPlan(conn, query, dataTableName, indexTableName);
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals("bc", rs.getString(1));
            assertEquals("bcd", rs.getString(2));
            assertTrue(rs.getTimestamp(3).after(before));
            assertTrue(rs.getTimestamp(3).before(after));
            assertFalse(rs.next());
            // Count the number of index rows
            rs = conn.createStatement().executeQuery("SELECT COUNT(*) from " + indexTableName);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            // Add one more row with val2 ='bc' and check this does not change the result of the previous
            // query
            // Sleep 1ms to get a different row timestamps
            Thread.sleep(1);
            conn.createStatement()
                    .execute("upsert into " + dataTableName + " values ('c', 'bc', 'ccc', 'cccc')");
            conn.commit();
            assertExplainPlan(conn, query, dataTableName, indexTableName);
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals("bc", rs.getString(1));
            assertEquals("bcd", rs.getString(2));
            assertTrue(rs.getTimestamp(3).after(before));
            assertTrue(rs.getTimestamp(3).before(after));
            assertFalse(rs.next());
            // Write a time range query to get the last row with val2 ='bc'
            query = "SELECT" +
                    (uncovered ? " " : "/*+ INDEX(" + dataTableName + " " + indexTableName + ")*/ ")
                    + "val1, val2, PHOENIX_ROW_TIMESTAMP() from " + dataTableName +
                    " WHERE val1 = 'bc' AND " + "PHOENIX_ROW_TIMESTAMP() > TO_DATE('" + after
                    + "','yyyy-MM-dd HH:mm:ss.SSS', '" + timeZoneID + "')";
            // Verify that we will read from the index table
            assertExplainPlan(conn, query, dataTableName, indexTableName);
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals("bc", rs.getString(1));
            assertEquals("ccc", rs.getString(2));
            assertTrue(rs.getTimestamp(3).after(after));
            assertFalse(rs.next());
            // Verify that we can execute the same query without using the index
            String noIndexQuery =
                    "SELECT /*+ NO_INDEX */ val1, val2, PHOENIX_ROW_TIMESTAMP() from " +
                            dataTableName + " WHERE val1 = 'bc' AND " +
                            "PHOENIX_ROW_TIMESTAMP() > TO_DATE('" + after +
                            "','yyyy-MM-dd HH:mm:ss.SSS', '" + timeZoneID + "')";
            // Verify that we will read from the data table
            rs = conn.createStatement().executeQuery("EXPLAIN " + noIndexQuery);
            String explainPlan = QueryUtil.getExplainPlan(rs);
            assertTrue(explainPlan.contains("FULL SCAN OVER " + dataTableName));
            rs = conn.createStatement().executeQuery(noIndexQuery);
            assertTrue(rs.next());
            assertEquals("bc", rs.getString(1));
            assertEquals("ccc", rs.getString(2));
            assertTrue(rs.getTimestamp(3).after(after));
            after = rs.getTimestamp(3);
            assertFalse(rs.next());
            // Sleep 1ms to get a different row timestamps
            Thread.sleep(1);
            conn.createStatement()
                    .execute("upsert into " + dataTableName + " values ('d', 'de', 'def', 'defg')");
            conn.commit();

            query = "SELECT" +
                    (uncovered ? " " : "/*+ INDEX(" + dataTableName + " " + indexTableName + ")*/ ")
                    + " val1, val2, PHOENIX_ROW_TIMESTAMP()  from " + dataTableName
                    + " WHERE val1 = 'de'";
            // Verify that we will read from the index table
            assertExplainPlan(conn, query, dataTableName, indexTableName);
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals("de", rs.getString(1));
            assertEquals("def", rs.getString(2));
            assertTrue(rs.getTimestamp(3).after(after));
            assertFalse(rs.next());
            conn.createStatement().execute("DROP INDEX " + indexTableName + " on " +
                    dataTableName);
            conn.commit();
            // Add a new index where the index row key starts with PHOENIX_ROW_TIMESTAMP()
            indexTableName = generateUniqueName();
            conn.createStatement().execute(
                    "CREATE " + (uncovered ? "UNCOVERED " : " ") + "INDEX " + indexTableName
                            + " on " + dataTableName + " (PHOENIX_ROW_TIMESTAMP()) ");
            conn.commit();
            // Add one more row
            // Sleep 1ms to get a different row timestamps
            Thread.sleep(1);
            conn.createStatement()
                    .execute("upsert into " + dataTableName + " values ('e', 'ae', 'efg', 'efgh')");
            conn.commit();
            // Write a query to get all the rows in the order of their timestamps
            query = "SELECT" +
                    (uncovered ? " " : "/*+ INDEX(" + dataTableName + " " + indexTableName + ")*/ ")
                    + " id, val1, val2, PHOENIX_ROW_TIMESTAMP() from " + dataTableName + " WHERE "
                    + "PHOENIX_ROW_TIMESTAMP() > TO_DATE('" + initial
                    + "','yyyy-MM-dd HH:mm:ss.SSS', '" + timeZoneID + "')";
            // Verify that we will read from the index table
            assertExplainPlan(conn, query, dataTableName, indexTableName);
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());

            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);

            assertEquals("a", rs.getString(1));
            assertEquals("ab", rs.getString(2));
            assertEquals("abc", rs.getString(3));
            assertTrue(rs.next());

            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);

            assertEquals("b", rs.getString(1));
            assertEquals("bc", rs.getString(2));
            assertEquals("bcd", rs.getString(3));
            assertTrue(rs.next());

            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);

            assertEquals("c", rs.getString(1));
            assertEquals("bc", rs.getString(2));
            assertEquals("ccc", rs.getString(3));
            assertTrue(rs.next());
            assertEquals("d", rs.getString(1));
            assertEquals("de", rs.getString(2));
            assertEquals("def", rs.getString(3));
            assertTrue(rs.next());
            assertEquals("e", rs.getString(1));
            assertEquals("ae", rs.getString(2));
            assertEquals("efg", rs.getString(3));
            assertFalse(rs.next());

            // Sleep 1ms to get a different row timestamps
            Thread.sleep(1);
            conn.createStatement()
                    .execute("upsert into " + dataTableName + " values ('a', 'ab', 'abc', 'abcd')");
            conn.commit();
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());

            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);

            assertEquals("b", rs.getString(1));
            assertEquals("bc", rs.getString(2));
            assertEquals("bcd", rs.getString(3));
            assertTrue(rs.next());

            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);

            assertEquals("c", rs.getString(1));
            assertEquals("bc", rs.getString(2));
            assertEquals("ccc", rs.getString(3));
            assertTrue(rs.next());
            assertEquals("d", rs.getString(1));
            assertEquals("de", rs.getString(2));
            assertEquals("def", rs.getString(3));
            assertTrue(rs.next());

            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);

            assertEquals("e", rs.getString(1));
            assertEquals("ae", rs.getString(2));
            assertEquals("efg", rs.getString(3));
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals("ab", rs.getString(2));
            assertEquals("abc", rs.getString(3));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testUncoveredQueryWithPhoenixRowTimestampAndAllPkCols() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            String indexTableName = generateUniqueName();
            TABLE_NAMES.add(dataTableName);
            TABLE_NAMES.add(indexTableName);
            Timestamp initial = new Timestamp(EnvironmentEdgeManager.currentTimeMillis() - 1);
            conn.createStatement().execute("create table " + dataTableName +
                    " (id varchar(10), val1 varchar(10), val2 varchar(10), " +
                    " val3 varchar(10) constraint pk primary key(id, val1, val2, val3))" +
                    (salted ? " SALT_BUCKETS=4" : ""));
            conn.createStatement().execute("upsert into " + dataTableName
                    + " values ('a', 'ab', 'abc', 'abcd')");
            conn.commit();
            Timestamp before = new Timestamp(EnvironmentEdgeManager.currentTimeMillis());
            // Sleep 1ms to get a different row timestamps
            Thread.sleep(1);
            conn.createStatement()
                    .execute("upsert into " + dataTableName + " values ('b', 'bc', 'bcd', 'bcde')");
            conn.commit();
            Timestamp after = new Timestamp(EnvironmentEdgeManager.currentTimeMillis() + 1);
            conn.createStatement().execute(
                    "CREATE " + (uncovered ? "UNCOVERED " : " ") + "INDEX " + indexTableName
                            + " on " + dataTableName + " (val1, PHOENIX_ROW_TIMESTAMP()) ");

            String timeZoneID = Calendar.getInstance().getTimeZone().getID();
            // Write a query to get the val2 = 'bc' with a time range query
            String query = "SELECT" +
                    (uncovered ? " " : "/*+ INDEX(" + dataTableName + " " + indexTableName + ")*/ ")
                    + "val1, val2, PHOENIX_ROW_TIMESTAMP() from " + dataTableName
                    + " WHERE val1 = 'bc' AND " + "PHOENIX_ROW_TIMESTAMP() > TO_DATE('"
                    + before.toString() + "','yyyy-MM-dd HH:mm:ss.SSS', '"
                    + timeZoneID + "') AND " + "PHOENIX_ROW_TIMESTAMP() < TO_DATE('" + after
                    + "','yyyy-MM-dd HH:mm:ss.SSS', '" + timeZoneID + "')";
            // Verify that we will read from the index table
            assertExplainPlan(conn, query, dataTableName, indexTableName);
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals("bc", rs.getString(1));
            assertEquals("bcd", rs.getString(2));
            assertTrue(rs.getTimestamp(3).after(before));
            assertTrue(rs.getTimestamp(3).before(after));
            assertFalse(rs.next());
            // Count the number of index rows
            rs = conn.createStatement().executeQuery("SELECT COUNT(*) from " + indexTableName);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            // Add one more row with val2 ='bc' and check this does not change the result of the previous
            // query
            // Sleep 1ms to get a different row timestamps
            Thread.sleep(1);
            conn.createStatement()
                    .execute("upsert into " + dataTableName + " values ('c', 'bc', 'ccc', 'cccc')");
            conn.commit();
            assertExplainPlan(conn, query, dataTableName, indexTableName);
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("bc", rs.getString(1));
            assertEquals("bcd", rs.getString(2));
            assertTrue(rs.getTimestamp(3).after(before));
            assertTrue(rs.getTimestamp(3).before(after));
            assertFalse(rs.next());
            // Write a time range query to get the last row with val2 ='bc'
            query = "SELECT" +
                    (uncovered ? " " : "/*+ INDEX(" + dataTableName + " " + indexTableName + ")*/ ")
                    + "val1, val2, PHOENIX_ROW_TIMESTAMP() from " + dataTableName +
                    " WHERE val1 = 'bc' AND " + "PHOENIX_ROW_TIMESTAMP() > TO_DATE('" + after
                    + "','yyyy-MM-dd HH:mm:ss.SSS', '" + timeZoneID + "')";
            // Verify that we will read from the index table
            assertExplainPlan(conn, query, dataTableName, indexTableName);
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("bc", rs.getString(1));
            assertEquals("ccc", rs.getString(2));
            assertTrue(rs.getTimestamp(3).after(after));
            assertFalse(rs.next());
            // Verify that we can execute the same query without using the index
            String noIndexQuery =
                    "SELECT /*+ NO_INDEX */ val1, val2, PHOENIX_ROW_TIMESTAMP() from " +
                            dataTableName + " WHERE val1 = 'bc' AND " +
                            "PHOENIX_ROW_TIMESTAMP() > TO_DATE('" + after +
                            "','yyyy-MM-dd HH:mm:ss.SSS', '" + timeZoneID + "')";
            // Verify that we will read from the data table
            rs = conn.createStatement().executeQuery("EXPLAIN " + noIndexQuery);
            String explainPlan = QueryUtil.getExplainPlan(rs);
            assertTrue(explainPlan.contains(salted ? "RANGE" :
                    "FULL" + " SCAN OVER " + dataTableName));
            rs = conn.createStatement().executeQuery(noIndexQuery);
            assertTrue(rs.next());
            assertEquals("bc", rs.getString(1));
            assertEquals("ccc", rs.getString(2));
            assertTrue(rs.getTimestamp(3).after(after));
            after = rs.getTimestamp(3);
            assertFalse(rs.next());
            // Sleep 1ms to get a different row timestamps
            Thread.sleep(1);
            conn.createStatement()
                    .execute("upsert into " + dataTableName + " values ('d', 'de', 'def', 'defg')");
            conn.commit();

            query = "SELECT" +
                    (uncovered ? " " : "/*+ INDEX(" + dataTableName + " " + indexTableName + ")*/ ")
                    + " val1, val2, PHOENIX_ROW_TIMESTAMP()  from " + dataTableName
                    + " WHERE val1 = 'de'";
            // Verify that we will read from the index table
            assertExplainPlan(conn, query, dataTableName, indexTableName);
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("de", rs.getString(1));
            assertEquals("def", rs.getString(2));
            assertTrue(rs.getTimestamp(3).after(after));
            assertFalse(rs.next());
            conn.createStatement().execute("DROP INDEX " + indexTableName + " on " +
                    dataTableName);
            conn.commit();
            // Add a new index where the index row key starts with PHOENIX_ROW_TIMESTAMP()
            indexTableName = generateUniqueName();
            conn.createStatement().execute(
                    "CREATE " + (uncovered ? "UNCOVERED " : " ") + "INDEX " + indexTableName
                            + " on " + dataTableName + " (PHOENIX_ROW_TIMESTAMP()) ");
            conn.commit();
            // Add one more row
            // Sleep 1ms to get a different row timestamps
            Thread.sleep(1);
            conn.createStatement()
                    .execute("upsert into " + dataTableName + " values ('e', 'ae', 'efg', 'efgh')");
            conn.commit();
            // Write a query to get all the rows in the order of their timestamps
            query = "SELECT" +
                    (uncovered ? " " : "/*+ INDEX(" + dataTableName + " " + indexTableName + ")*/ ")
                    + " id, val1, val2, PHOENIX_ROW_TIMESTAMP() from " + dataTableName + " WHERE "
                    + "PHOENIX_ROW_TIMESTAMP() > TO_DATE('" + initial
                    + "','yyyy-MM-dd HH:mm:ss.SSS', '" + timeZoneID + "')";
            // Verify that we will read from the index table
            assertExplainPlan(conn, query, dataTableName, indexTableName);
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);

            assertEquals("a", rs.getString(1));
            assertEquals("ab", rs.getString(2));
            assertEquals("abc", rs.getString(3));
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);

            assertEquals("b", rs.getString(1));
            assertEquals("bc", rs.getString(2));
            assertEquals("bcd", rs.getString(3));
            assertTrue(rs.next());
            assertEquals("c", rs.getString(1));
            assertEquals("bc", rs.getString(2));
            assertEquals("ccc", rs.getString(3));
            assertTrue(rs.next());
            assertEquals("d", rs.getString(1));
            assertEquals("de", rs.getString(2));
            assertEquals("def", rs.getString(3));
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals("e", rs.getString(1));
            assertEquals("ae", rs.getString(2));
            assertEquals("efg", rs.getString(3));
            assertFalse(rs.next());

            // Sleep 1ms to get a different row timestamps
            Thread.sleep(1);
            conn.createStatement()
                    .execute("upsert into " + dataTableName + " values ('a', 'ab', 'abc', 'abcd')");
            conn.commit();
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals("b", rs.getString(1));
            assertEquals("bc", rs.getString(2));
            assertEquals("bcd", rs.getString(3));
            assertTrue(rs.next());
            assertEquals("c", rs.getString(1));
            assertEquals("bc", rs.getString(2));
            assertEquals("ccc", rs.getString(3));
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals("d", rs.getString(1));
            assertEquals("de", rs.getString(2));
            assertEquals("def", rs.getString(3));
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals("e", rs.getString(1));
            assertEquals("ae", rs.getString(2));
            assertEquals("efg", rs.getString(3));
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals("ab", rs.getString(2));
            assertEquals("abc", rs.getString(3));
            assertFalse(rs.next());
        }
    }

    private void assertIndexTableNotSelected(Connection conn, String dataTableName,
                                             String indexTableName, String sql)
            throws Exception {
        try {
            assertExplainPlan(conn, sql, dataTableName, indexTableName);
            throw new RuntimeException(
                    "The index table should not be selected without an index hint");
        } catch (AssertionError error) {
            //expected
        }
    }

    @Test
    public void testUncoveredQuery() throws Exception {
        String dataTableName = generateUniqueName();
        populateTable(
                dataTableName); // with two rows ('a', 'ab', 'abc', 'abcd') and ('b', 'bc', 'bcd', 'bcde')
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String indexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE " + (uncovered ? "UNCOVERED " : " ") + "INDEX "
                    + indexTableName + " on " + dataTableName + " (val1) " +
                    (uncovered ? "" : "INCLUDE (val2)"));
            TABLE_NAMES.add(dataTableName);
            TABLE_NAMES.add(indexTableName);
            String selectSql;
            int limit = 10;
            if (!uncovered) {
                // Verify that without an index hint, a covered index table is not selected
                assertIndexTableNotSelected(conn, dataTableName, indexTableName,
                        "SELECT val3 from " + dataTableName +
                                " WHERE val1 = 'bc' AND (val2 = 'bcd' OR val3 ='bcde') LIMIT 10");
                // Verify that with index hint, we will read from the index table
                // even though val3 is not included by the index table
                selectSql = "SELECT /*+ INDEX(" + dataTableName + " " + indexTableName +
                        ")*/ val2, val3 from "
                        + dataTableName +
                        " WHERE val1 = 'bc' AND (val2 = 'bcd' OR val3 ='bcde') LIMIT " + limit;
                assertExplainPlanWithLimit(conn, selectSql, dataTableName, indexTableName, limit);
            } else {
                // Verify that an index hint is not necessary for an uncovered index
                selectSql = "SELECT  val2, val3 from " + dataTableName
                        + " WHERE val1 = 'bc' AND (val2 = 'bcd' OR val3 ='bcde') LIMIT " + limit;
                assertExplainPlanWithLimit(conn, selectSql, dataTableName, indexTableName, limit);
            }

            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals("bcd", rs.getString(1));
            assertEquals("bcde", rs.getString(2));
            assertFalse(rs.next());
            conn.createStatement().execute("DROP INDEX " + indexTableName + " on " + dataTableName);
            conn.commit();
            // Create an index does not include any columns
            indexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE " + (uncovered ? "UNCOVERED " : " ") + "INDEX "
                    + indexTableName + " on " + dataTableName + " (val1)");
            conn.commit();

            if (!uncovered) {
                // Verify that without hint, a covered index table is not selected
                assertIndexTableNotSelected(conn, dataTableName, indexTableName,
                        "SELECT id from " + dataTableName +
                                " WHERE val1 = 'bc' AND (val2 = 'bcd' OR val3 ='bcde')");
                selectSql = "SELECT /*+ INDEX(" + dataTableName + " " + indexTableName +
                        ")*/ id from " + dataTableName
                        + " WHERE val1 = 'bc' AND (val2 = 'bcd' OR val3 ='bcde')";
            } else {
                selectSql = "SELECT id from " + dataTableName +
                        " WHERE val1 = 'bc' AND (val2 = 'bcd' OR val3 ='bcde')";
            }
            // Verify that we will read from the index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
            assertFalse(rs.next());

            // Add another row and run a group by query where the uncovered index should be used
            conn.createStatement().execute("upsert into " + dataTableName +
                    " (id, val1, val2, val3) values ('c', 'ab','cde', 'cdef')");
            conn.commit();
            if (!uncovered) {
                // Verify that without an index hint, an uncovered index table is not selected
                assertIndexTableNotSelected(conn, dataTableName, indexTableName,
                        "SELECT count(val3) from " + dataTableName +
                                " where val1 > '0' GROUP BY val1");
                selectSql = "SELECT /*+ INDEX(" + dataTableName + " " + indexTableName +
                        ")*/ count(val3) from "
                        + dataTableName + " where val1 > '0' GROUP BY val1";
            } else {
                selectSql = "SELECT count(val3) from " + dataTableName +
                        " where val1 > '0' GROUP BY val1";
            }
            // Verify that we will read from the index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals(2, rs.getInt(1));
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());
            if (!uncovered) {
                selectSql = "SELECT /*+ INDEX(" + dataTableName + " " + indexTableName +
                        ")*/ count(val3) from "
                        + dataTableName + " where val1 > '0'";
            } else {
                selectSql = "SELECT count(val3) from " + dataTableName + " where val1 > '0'";
            }
            // Verify that we will read from the index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            // Run an order by query where the uncovered index should be used
            if (!uncovered) {
                // Verify that without hint, the index table is not selected
                assertIndexTableNotSelected(conn, dataTableName, indexTableName,
                        "SELECT val3 from " + dataTableName + " where val1 > '0' ORDER BY val1");
                selectSql = "SELECT /*+ INDEX(" + dataTableName + " " + indexTableName +
                        ")*/ val3 from "
                        + dataTableName + " where val1 > '0' ORDER BY val1";
            } else {
                selectSql = "SELECT val3 from " + dataTableName + " where val1 > '0' ORDER BY val1";
            }
            // Verify that we will read from the index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals("abcd", rs.getString(1));
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals("cdef", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("bcde", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testPartialIndexUpdate() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            conn.createStatement().execute("create table " + dataTableName +
                    " (id varchar not null primary key, " +
                    "val1 varchar, val2 varchar, val3 varchar, val4 varchar)" +
                    (salted ? " SALT_BUCKETS=4" : ""));
            conn.createStatement().execute(
                    "upsert into " + dataTableName + " values ('b', 'bc', 'bcd', 'bcde', 'bcdef')");
            conn.commit();
            String indexTableName = generateUniqueName();
            TABLE_NAMES.add(dataTableName);
            TABLE_NAMES.add(indexTableName);
            conn.createStatement().execute("CREATE " + (uncovered ? "UNCOVERED " : " ") + "INDEX "
                    + indexTableName + " on " + dataTableName + " (val1, val2) " +
                    (uncovered ? "" : "INCLUDE (val3)"));
            conn.createStatement()
                    .execute("upsert into " + dataTableName + " (id, val2) values ('b', 'bcdd')");
            conn.commit();
            String selectSql;
            if (!uncovered) {
                // Verify that without an index hint, a covered index table is not selected
                assertIndexTableNotSelected(conn, dataTableName, indexTableName,
                        "SELECT val4 from " + dataTableName +
                                " WHERE val1 = 'bc' AND val2 = 'bcdd'");
                // Verify that with index hint, we will read from the index table even though val4
                // is not included by the index table
                selectSql = "SELECT /*+ INDEX(" + dataTableName + " " + indexTableName +
                        ")*/ val4 from "
                        + dataTableName + " WHERE val1 = 'bc' AND val2 = 'bcdd'";
                assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
            } else {
                // Verify that an index hint is not necessary for an uncovered index
                selectSql = "SELECT  val4 from " + dataTableName
                        + " WHERE val1 = 'bc' AND val2 = 'bcdd'";
                assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
            }

            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals("bcdef", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSkipScanFilter() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            conn.createStatement().execute("CREATE TABLE " + dataTableName
                    + "(k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, v1 INTEGER, "
                    + "v2 INTEGER, v3 INTEGER "
                    + "CONSTRAINT pk PRIMARY KEY (k1,k2)) "
                    + " COLUMN_ENCODED_BYTES = 0, VERSIONS=1" + (salted ? ", SALT_BUCKETS=4" : ""));
            TestUtil.addCoprocessor(conn, dataTableName, ScanFilterRegionObserver.class);
            ScanFilterRegionObserver.resetCount();
            String indexTableName = generateUniqueName();
            TABLE_NAMES.add(dataTableName);
            TABLE_NAMES.add(indexTableName);
            conn.createStatement().execute("CREATE " + (uncovered ? "UNCOVERED " : " ") + "INDEX "
                    + indexTableName + " ON " + dataTableName + "(v1)" +
                    (uncovered ? "" : "include (v2)"));
            final int nIndexValues = 97;
            final Random RAND = new Random(7);
            final int batchSize = 100;
            for (int i = 0; i < 10000; i++) {
                conn.createStatement().execute(
                        "UPSERT INTO " + dataTableName + " VALUES (" + i + ", 1, "
                                + (RAND.nextInt() % nIndexValues) + ", "
                                + RAND.nextInt() + ", 1)");
                if ((i % batchSize) == 0) {
                    conn.commit();
                }
            }
            conn.commit();
            String selectSql = "SELECT" +
                    (uncovered ? " " : "/*+ INDEX(" + dataTableName + " " + indexTableName + ")*/ ")
                    + "SUM(v3) from " + dataTableName + " GROUP BY v1";

            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            int sum = 0;
            while (rs.next()) {
                if (sum % 1500 == 0) {
                    moveRegionsOfTable(dataTableName);
                    moveRegionsOfTable(indexTableName);
                }
                sum += rs.getInt(1);
            }
            assertEquals(10000, sum);
            // UncoveredGlobalIndexRegionScanner uses the skip scan filter to retrieve data table
            // rows. Verify that the skip scan filter is used
            assertTrue(ScanFilterRegionObserver.count.get() >= 10000);
        }
    }

    @Test
    public void testCount() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            conn.createStatement().execute("CREATE TABLE " + dataTableName
                    + "(k1 BIGINT NOT NULL, k2 BIGINT NOT NULL, v1 INTEGER, "
                    + "v2 INTEGER, v3 BIGINT "
                    + "CONSTRAINT pk PRIMARY KEY (k1,k2)) "
                    + " VERSIONS=1, IMMUTABLE_ROWS=TRUE" + (salted ? ", SALT_BUCKETS=4" : ""));
            String indexTableName = generateUniqueName();
            TABLE_NAMES.add(dataTableName);
            TABLE_NAMES.add(indexTableName);
            conn.createStatement().execute(
                    "CREATE " + (uncovered ? "UNCOVERED " : " ") + "INDEX " + indexTableName +
                            " ON "
                            + dataTableName + "(v1)");
            final int nIndexValues = 9;
            final Random RAND = new Random(7);
            final int batchSize = 1000;
            for (int i = 0; i < 100000; i++) {
                conn.createStatement().execute(
                        "UPSERT INTO " + dataTableName + " VALUES (" + i + ", 1, "
                                + (RAND.nextInt() % nIndexValues) + ", "
                                + RAND.nextInt() + ", " + RAND.nextInt() + ")");
                if ((i % batchSize) == 0) {
                    conn.commit();
                }
            }
            conn.commit();
            String selectSql =
                    "SELECT /*+ NO INDEX */  Count(v3) from " + dataTableName + " where v1 = 5";
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            long count = rs.getLong(1);
            selectSql = "SELECT" +
                    (uncovered ? " " : "/*+ INDEX(" + dataTableName + " " + indexTableName + ")*/ ")
                    + "Count(v3) from " + dataTableName + " where v1 = 5";
            // Verify that we will read from the index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals(count, rs.getInt(1));
        }
    }

    @Test
    public void testFailDataTableRowUpdate() throws Exception {
        String dataTableName = generateUniqueName();
        populateTable(
                dataTableName); // with two rows ('a', 'ab', 'abc', 'abcd') and ('b', 'bc', 'bcd', 'bcde')
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String indexTableName = generateUniqueName();
            TABLE_NAMES.add(dataTableName);
            TABLE_NAMES.add(indexTableName);
            conn.createStatement().execute("CREATE " + (uncovered ? "UNCOVERED " : " ") + "INDEX "
                    + indexTableName + " on " + dataTableName + " (val1)");
            // Configure IndexRegionObserver to fail the data table update
            // and check that this does not impact the correctness
            IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
            conn.createStatement()
                    .execute("upsert into " + dataTableName + " (id, val2) values ('a', 'abcc')");
            try {
                conn.commit();
                fail();
            } catch (Exception e) {
                // this is expected
            }
            IndexRegionObserver.setFailDataTableUpdatesForTesting(false);

            conn.createStatement()
                    .execute("upsert into " + dataTableName + " (id, val3) values ('a', 'abcdd')");
            conn.commit();
            String selectSql = "SELECT" + (uncovered ? " " : "/*+ INDEX(" + dataTableName + " "
                    + indexTableName + ")*/ ") + "val2, val3 from " + dataTableName
                    + " WHERE val1  = 'ab'";
            // Verify that we will read from the first index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals("abc", rs.getString(1));
            assertEquals("abcdd", rs.getString(2));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testFailPostIndexDeleteUpdate() throws Exception {
        String dataTableName = generateUniqueName();
        populateTable(
                dataTableName); // with two rows ('a', 'ab', 'abc', 'abcd') and ('b', 'bc', 'bcd', 'bcde')
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String indexTableName = generateUniqueName();
            TABLE_NAMES.add(dataTableName);
            TABLE_NAMES.add(indexTableName);
            conn.createStatement().execute("CREATE " + (uncovered ? "UNCOVERED " : " ") + "INDEX "
                    + indexTableName + " on " + dataTableName + " (val1)");
            String selectSql = "SELECT id from " + dataTableName + " WHERE val1  = 'ab'";

            // Verify that we will read from the index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertFalse(rs.next());
            // Configure IndexRegionObserver to fail the last write phase (i.e., the post index update phase) where
            // index rows are deleted and check that this does not impact the correctness
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(true);
            String dml = "DELETE from " + dataTableName + " WHERE id  = 'a'";
            assertEquals(1, conn.createStatement().executeUpdate(dml));
            conn.commit();
            // The index rows are actually not deleted yet because IndexRegionObserver failed delete operation.
            dml = "DELETE from " + dataTableName + " WHERE val1  = 'ab'";
            // This DML will scan the Index table and detect invalid index rows. This will trigger read repair which
            // result in deleting these rows since the corresponding data table rows are deleted already. So,
            // the number of rows to be deleted by the "DELETE" DML will be zero since the rows deleted by read repair
            // will not be visible to the DML
            assertEquals(0, conn.createStatement().executeUpdate(dml));
            rs = conn.createStatement().executeQuery(selectSql);
            assertFalse(rs.next());

            rs = conn.createStatement().executeQuery("SELECT count(*) from " + indexTableName);
            assertTrue(rs.next());
            assertEquals(1, rs.getLong(1));
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
        }
    }

    public static class ScanFilterRegionObserver extends SimpleRegionObserver {
        public static final AtomicInteger count = new AtomicInteger(0);

        public static void resetCount() {
            count.set(0);
        }

        @Override
        public void preScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c,
                                   final Scan scan) {
            if (scan.getFilter() instanceof SkipScanFilter) {
                List<List<KeyRange>> slots = ((SkipScanFilter) scan.getFilter()).getSlots();
                for (List<KeyRange> ranges : slots) {
                    count.addAndGet(ranges.size());
                }
            }
        }
    }
}
