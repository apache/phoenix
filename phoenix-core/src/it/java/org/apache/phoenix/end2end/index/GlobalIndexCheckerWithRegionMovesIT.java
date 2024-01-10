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
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionStatesCount;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.end2end.IndexToolIT;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.iterate.ScanningResultPostDummyResultCaller;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.thirdparty.com.google.common.base.Strings;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.phoenix.mapreduce.PhoenixJobCounters.INPUT_RECORDS;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_BEYOND_MAXLOOKBACK_INVALID_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_OLD_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_UNKNOWN_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_UNVERIFIED_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_VALID_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.REBUILT_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.SCANNED_DATA_ROW_COUNT;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class GlobalIndexCheckerWithRegionMovesIT extends BaseTest {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(GlobalIndexCheckerWithRegionMovesIT.class);
    private final boolean async;
    private String indexDDLOptions;
    private String tableDDLOptions;
    private StringBuilder optionBuilder;
    private StringBuilder indexOptionBuilder;
    private final boolean encoded;

    public GlobalIndexCheckerWithRegionMovesIT(boolean async, boolean encoded) {
        this.async = async;
        this.encoded = encoded;
    }

    private static boolean hasTestStarted = false;
    private static int countOfDummyResults = 0;
    private static final Set<String> TABLE_NAMES = new HashSet<>();

    private static class TestScanningResultPostDummyResultCaller extends
            ScanningResultPostDummyResultCaller {

        @Override
        public void postDummyProcess() {
            if (hasTestStarted && (countOfDummyResults++ % 4) == 0 &&
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
        props.put(QueryServices.PHOENIX_POST_DUMMY_PROCESS,
                TestScanningResultPostDummyResultCaller.class.getName());
        props.put(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB,
                Long.toString(0));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    protected static void moveRegionsOfTable(String tableName)
            throws IOException {
        Admin admin = getUtility().getAdmin();
        List<ServerName> servers =
                new ArrayList<>(admin.getRegionServers());
        ServerName server1 = servers.get(0);
        ServerName server2 = servers.get(1);
        List<RegionInfo> regionsOnServer1;
        try {
            regionsOnServer1 = admin.getRegions(server1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        List<RegionInfo> regionsOnServer2;
        try {
            regionsOnServer2 = admin.getRegions(server2);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        regionsOnServer1.forEach(regionInfo -> {
            if (regionInfo.getTable().equals(TableName.valueOf(tableName))) {
                try {
                    for (int i = 0; i < 2; i++) {
                        RegionStatesCount regionStatesCount =
                                admin.getClusterMetrics().getTableRegionStatesCount()
                                        .get(TableName.valueOf(tableName));
                        if (regionStatesCount.getRegionsInTransition() == 0 &&
                                regionStatesCount.getOpenRegions() ==
                                        regionStatesCount.getTotalRegions()) {
                            LOGGER.info("Moving region {} to {}",
                                    regionInfo.getRegionNameAsString(), server2);
                            admin.move(regionInfo.getEncodedNameAsBytes(), server2);
                            break;
                        } else {
                            LOGGER.info("Table {} has some region(s) in RIT or not online",
                                    tableName);
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        regionsOnServer2.forEach(regionInfo -> {
            if (regionInfo.getTable().equals(TableName.valueOf(tableName))) {
                try {
                    for (int i = 0; i < 2; i++) {
                        RegionStatesCount regionStatesCount =
                                admin.getClusterMetrics().getTableRegionStatesCount()
                                        .get(TableName.valueOf(tableName));
                        if (regionStatesCount.getRegionsInTransition() == 0 &&
                                regionStatesCount.getOpenRegions() ==
                                        regionStatesCount.getTotalRegions()) {
                            admin.move(regionInfo.getEncodedNameAsBytes(), server1);
                            LOGGER.info("Moving region {} to {}",
                                    regionInfo.getRegionNameAsString(), server1);
                            break;
                        } else {
                            LOGGER.info("Table {} has some region(s) in RIT or not online",
                                    tableName);
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    @Before
    public void beforeTest() {
        hasTestStarted = true;
        optionBuilder = new StringBuilder();
        indexOptionBuilder = new StringBuilder();
        if (!encoded) {
            optionBuilder.append(" COLUMN_ENCODED_BYTES=0");
        } else {
            indexOptionBuilder.append(
                    " IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
        }
        this.tableDDLOptions = optionBuilder.toString();
        this.indexDDLOptions = indexOptionBuilder.toString();
    }

    @Parameters(name = "async={0},encoded={1}")
    public static synchronized Collection<Object[]> data() {
        List<Object[]> list = Lists.newArrayListWithExpectedSize(4);
        boolean[] values = new boolean[]{true, false};
        for (boolean async : values) {
            for (boolean encoded : values) {
                list.add(new Object[]{async, encoded});
            }
        }
        return list;
    }

    @After
    public void unsetFailForTesting() throws Exception {
        countOfDummyResults = 0;
        TABLE_NAMES.clear();
        hasTestStarted = false;
        boolean refCountLeaked = isAnyStoreRefCountLeaked();
        IndexRegionObserver.setFailPreIndexUpdatesForTesting(false);
        IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
        IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
        assertFalse("refCount leaked", refCountLeaked);
    }

    public static void assertExplainPlan(Connection conn, String selectSql,
                                         String dataTableFullName, String indexTableFullName)
            throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
        String actualExplainPlan = QueryUtil.getExplainPlan(rs);
        IndexToolIT.assertExplainPlan(false, actualExplainPlan, dataTableFullName,
                indexTableFullName);
    }

    public static void assertExplainPlanWithLimit(Connection conn, String selectSql,
                                                  String dataTableFullName,
                                                  String indexTableFullName, int limit)
            throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
        String actualExplainPlan = QueryUtil.getExplainPlan(rs);
        IndexToolIT.assertExplainPlan(false, actualExplainPlan, dataTableFullName,
                indexTableFullName);
        String expectedLimitPlan = String.format("SERVER %d ROW LIMIT", limit);
        assertTrue(actualExplainPlan + "\n expected to contain \n" + expectedLimitPlan,
                actualExplainPlan.contains(expectedLimitPlan));
    }

    private void populateTable(String tableName) throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("create table " + tableName +
                " (id varchar(10) not null primary key, val1 varchar(10), val2 varchar(10), val3 varchar(10))" +
                tableDDLOptions);
        conn.createStatement()
                .execute("upsert into " + tableName + " values ('a', 'ab', 'abc', 'abcd')");
        conn.commit();
        conn.createStatement()
                .execute("upsert into " + tableName + " values ('b', 'bc', 'bcd', 'bcde')");
        conn.commit();
        conn.close();
    }

    @Test
    public void testDelete() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            populateTable(
                    dataTableName); // with two rows ('a', 'ab', 'abc', 'abcd') and ('b', 'bc', 'bcd', 'bcde')
            String dml = "DELETE from " + dataTableName + " WHERE id  = 'a'";
            assertEquals(1, conn.createStatement().executeUpdate(dml));
            conn.commit();
            String indexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE INDEX " + indexTableName + " on " +
                    dataTableName + " (val1) include (val2, val3)" + (async ? "ASYNC" : "") +
                    this.indexDDLOptions);
            TABLE_NAMES.add(dataTableName);
            TABLE_NAMES.add(indexTableName);
            if (async) {
                // run the index MR job.
                IndexToolIT.runIndexTool(false, null, dataTableName, indexTableName);
            }
            // Count the number of index rows
            String query = "SELECT COUNT(*) from " + indexTableName;
            // There should be one row in the index table
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            // Add rows and check everything is still okay
            verifyTableHealth(conn, dataTableName, indexTableName);
        }
    }

    @Test
    public void testPhoenixRowTimestamp() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            String indexTableName = generateUniqueName();
            TABLE_NAMES.add(dataTableName);
            TABLE_NAMES.add(indexTableName);
            Timestamp initial = new Timestamp(EnvironmentEdgeManager.currentTimeMillis() - 1);
            conn.createStatement().execute("create table " + dataTableName +
                    " (id varchar(10) not null primary key, val1 varchar(10), val2 varchar(10), val3 varchar(10))" +
                    tableDDLOptions);
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
            conn.createStatement().execute("CREATE INDEX " + indexTableName + " on " +
                    dataTableName + " (val1, PHOENIX_ROW_TIMESTAMP()) " + "include (val2, val3) " +
                    (async ? "ASYNC" : "") + this.indexDDLOptions);
            if (async) {
                // Run the index MR job to rebuild the index and verify that index is built correctly
                IndexToolIT.runIndexTool(false, null, dataTableName,
                        indexTableName, null, 0, IndexTool.IndexVerifyType.AFTER);
            }

            String timeZoneID = Calendar.getInstance().getTimeZone().getID();
            // Write a query to get the val2 = 'bc' with a time range query
            String query = "SELECT  val1, val2, PHOENIX_ROW_TIMESTAMP() from " + dataTableName +
                    " WHERE val1 = 'bc' AND " +
                    "PHOENIX_ROW_TIMESTAMP() > TO_DATE('" + before.toString() +
                    "','yyyy-MM-dd HH:mm:ss.SSS', '" + timeZoneID + "') AND " +
                    "PHOENIX_ROW_TIMESTAMP() < TO_DATE('" + after.toString() +
                    "','yyyy-MM-dd HH:mm:ss.SSS', '" + timeZoneID + "')";
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
            query = "SELECT  val1, val2, PHOENIX_ROW_TIMESTAMP() from " + dataTableName +
                    " WHERE val1 = 'bc' AND " +
                    "PHOENIX_ROW_TIMESTAMP() > TO_DATE('" + after.toString() +
                    "','yyyy-MM-dd HH:mm:ss.SSS', '" + timeZoneID + "')";
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
                            "PHOENIX_ROW_TIMESTAMP() > TO_DATE('" + after.toString() +
                            "','yyyy-MM-dd HH:mm:ss.SSS', '" + timeZoneID + "')";
            // Verify that we will read from the data table
            rs = conn.createStatement().executeQuery("EXPLAIN " + noIndexQuery);
            String explainPlan = QueryUtil.getExplainPlan(rs);
            assertTrue(explainPlan.contains("FULL SCAN OVER " + dataTableName));
            rs = conn.createStatement().executeQuery(noIndexQuery);
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals("bc", rs.getString(1));
            assertEquals("ccc", rs.getString(2));
            assertTrue(rs.getTimestamp(3).after(after));
            after = rs.getTimestamp(3);
            assertFalse(rs.next());
            // Add an unverified index row
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(true);
            // Sleep 1ms to get a different row timestamps
            Thread.sleep(1);
            conn.createStatement()
                    .execute("upsert into " + dataTableName + " values ('d', 'de', 'def', 'defg')");
            conn.commit();
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
            // Make sure that we can repair the unverified row
            query = "SELECT  val1, val2, PHOENIX_ROW_TIMESTAMP()  from " + dataTableName +
                    " WHERE val1 = 'de'";
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
            // Add a new index where the index row key starts with PHOENIX_ROW_TIMESTAMP()
            indexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE INDEX " + indexTableName + " on " +
                    dataTableName + " (PHOENIX_ROW_TIMESTAMP()) " + "include (val1, val2, val3) " +
                    (async ? "ASYNC" : "") + this.indexDDLOptions);
            if (async) {
                // Run the index MR job to rebuild the index and verify that index is built correctly
                IndexToolIT.runIndexTool(false, null, dataTableName,
                        indexTableName, null, 0, IndexTool.IndexVerifyType.AFTER);
            }
            // Add one more row
            // Sleep 1ms to get a different row timestamps
            Thread.sleep(1);
            conn.createStatement()
                    .execute("upsert into " + dataTableName + " values ('e', 'ae', 'efg', 'efgh')");
            conn.commit();
            // Write a query to get all the rows in the order of their timestamps
            query = "SELECT  val1, val2, PHOENIX_ROW_TIMESTAMP() from " + dataTableName +
                    " WHERE " +
                    "PHOENIX_ROW_TIMESTAMP() > TO_DATE('" + initial.toString() +
                    "','yyyy-MM-dd HH:mm:ss.SSS', '" + timeZoneID + "')";
            // Verify that we will read from the index table
            assertExplainPlan(conn, query, dataTableName, indexTableName);
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals("ab", rs.getString(1));
            assertEquals("abc", rs.getString(2));
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals("bc", rs.getString(1));
            assertEquals("bcd", rs.getString(2));
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals("bc", rs.getString(1));
            assertEquals("ccc", rs.getString(2));
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals("de", rs.getString(1));
            assertEquals("def", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("ae", rs.getString(1));
            assertEquals("efg", rs.getString(2));
            assertFalse(rs.next());
            conn.createStatement().execute("DROP INDEX " + indexTableName + " on " +
                    dataTableName);
            // Run the previous test on an uncovered global index
            indexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE INDEX " + indexTableName + " on " +
                    dataTableName + " (PHOENIX_ROW_TIMESTAMP())" +
                    (async ? "ASYNC" : "") + this.indexDDLOptions);
            if (async) {
                // Run the index MR job to rebuild the index and verify that index is built correctly
                IndexToolIT.runIndexTool(false, null, dataTableName,
                        indexTableName, null, 0, IndexTool.IndexVerifyType.AFTER);
            }
            // Verify that without hint, the index table is not selected
            assertIndexTableNotSelected(conn, dataTableName, indexTableName, query);

            // Verify that we will read from the index table with the index hint
            query = "SELECT /*+ INDEX(" + dataTableName + " " + indexTableName + ")*/ " +
                    "val1, val2, PHOENIX_ROW_TIMESTAMP() from " + dataTableName + " WHERE " +
                    "PHOENIX_ROW_TIMESTAMP() > TO_DATE('" + initial.toString() +
                    "','yyyy-MM-dd HH:mm:ss.SSS', '" + timeZoneID + "')";

            assertExplainPlan(conn, query, dataTableName, indexTableName);
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals("ab", rs.getString(1));
            assertEquals("abc", rs.getString(2));
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals("bc", rs.getString(1));
            assertEquals("bcd", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("bc", rs.getString(1));
            assertEquals("ccc", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("de", rs.getString(1));
            assertEquals("def", rs.getString(2));
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals("ae", rs.getString(1));
            assertEquals("efg", rs.getString(2));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testDeleteNonExistingRow() throws Exception {
        if (async) {
            // No need to run the same test twice one for async = true and the other for async = false
            return;
        }
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            populateTable(
                    dataTableName); // with two rows ('a', 'ab', 'abc', 'abcd') and ('b', 'bc', 'bcd', 'bcde')
            String indexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE INDEX " + indexTableName + " on " +
                    dataTableName + " (val1) include (val2, val3)" + this.indexDDLOptions);
            TABLE_NAMES.add(dataTableName);
            TABLE_NAMES.add(indexTableName);

            String dml = "DELETE from " + dataTableName + " WHERE id  = 'a'";
            conn.createStatement().executeUpdate(dml);
            conn.commit();
            // Attempt to delete a row that does not exist
            conn.createStatement().executeUpdate(dml);
            conn.commit();
            // Make sure this delete attempt did not make the index and data table inconsistent
            IndexToolIT.runIndexTool(false, "", dataTableName, indexTableName, null,
                    0, IndexTool.IndexVerifyType.ONLY);
        }
    }

    @Test
    public void testIndexRowWithoutEmptyColumn() throws Exception {
        if (async) {
            // No need to run the same test twice one for async = true and the other for async = false
            return;
        }
        long scn;
        String dataTableName = generateUniqueName();
        String indexTableName = generateUniqueName();
        TABLE_NAMES.add(dataTableName);
        TABLE_NAMES.add(indexTableName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            populateTable(
                    dataTableName); // with two rows ('a', 'ab', 'abc', 'abcd') and ('b', 'bc', 'bcd', 'bcde')
            conn.createStatement().execute("CREATE INDEX " + indexTableName + " on " +
                    dataTableName + " (val1) include (val2, val3)" + this.indexDDLOptions);
            scn = EnvironmentEdgeManager.currentTimeMillis();
            // Configure IndexRegionObserver to fail the data write phase
            IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
            conn.createStatement().execute(
                    "upsert into " + dataTableName + " values ('a', 'abc','abcc', 'abccd')");
            commitWithException(conn);
            // The above upsert will create an unverified index row with row key {'abc', 'a'} and will make the existing
            // index row with row key {'ab', 'a'} unverified
            // Configure IndexRegionObserver to allow the data write phase
            IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
            // Do major compaction which will remove the empty column cell from the index row with row key {'ab', 'a'}
            TestUtil.doMajorCompaction(conn, indexTableName);
        }
        Properties props = new Properties();
        props.setProperty("CurrentSCN", Long.toString(scn));
        try (Connection connWithSCN = DriverManager.getConnection(getUrl(), props)) {
            String selectSql = "SELECT * from " + dataTableName + " WHERE val1  = 'ab'";
            // Verify that we will read from the index table
            assertExplainPlan(connWithSCN, selectSql, dataTableName, indexTableName);
            ResultSet rs = connWithSCN.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals("a", rs.getString(1));
            assertEquals("ab", rs.getString(2));
            assertEquals("abc", rs.getString(3));
            assertEquals("abcd", rs.getString(4));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testLimitWithUnverifiedRows() throws Exception {
        if (async) {
            return;
        }
        String dataTableName = generateUniqueName();
        populateTable(
                dataTableName); // with two rows ('a', 'ab', 'abc', 'abcd') and ('b', 'bc', 'bcd', 'bcde')
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String indexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE INDEX " + indexTableName + " on " +
                    dataTableName + " (val1) include (val2, val3)" + this.indexDDLOptions);
            TABLE_NAMES.add(dataTableName);
            TABLE_NAMES.add(indexTableName);
            conn.commit();
            // Read all index rows and rewrite them back directly. This will overwrite existing rows with newer
            // timestamps and set the empty column to value "x". This will make them unverified
            conn.createStatement().execute("UPSERT INTO " + indexTableName + " SELECT * FROM " +
                    indexTableName);
            conn.commit();
            // Verified that read repair will not reduce the number of rows returned for LIMIT queries
            String selectSql = "SELECT * from " + indexTableName + " LIMIT 1";
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals("ab", rs.getString(1));
            assertEquals("a", rs.getString(2));
            assertEquals("abc", rs.getString(3));
            assertEquals("abcd", rs.getString(4));
            assertFalse(rs.next());
            selectSql = "SELECT val3 from " + dataTableName + " WHERE val1 = 'bc' LIMIT 1";
            // Verify that we will read from the index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals("bcde", rs.getString(1));
            assertFalse(rs.next());
            // Configure IndexRegionObserver to fail the last write phase (i.e., the post index update phase) where the verify flag is set
            // to true and/or index rows are deleted and check that this does not impact the correctness
            IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
            // This is to cover the case where there is no data table row for an unverified index row
            conn.createStatement().execute(
                    "upsert into " + dataTableName + " (id, val1, val2) values ('c', 'aa','cde')");
            commitWithException(conn);
            IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
            // Verified that read repair will not reduce the number of rows returned for LIMIT queries
            selectSql = "SELECT * from " + indexTableName + " LIMIT 1";
            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals("ab", rs.getString(1));
            assertEquals("a", rs.getString(2));
            assertEquals("abc", rs.getString(3));
            assertEquals("abcd", rs.getString(4));
            assertFalse(rs.next());
            // Add rows and check everything is still okay
            verifyTableHealth(conn, dataTableName, indexTableName);

        }
    }

    private void assertIndexTableNotSelected(Connection conn, String dataTableName,
                                             String indexTableName, String sql)
            throws Exception {
        try {
            assertExplainPlan(conn, sql, dataTableName, indexTableName);
            throw new AssertionError(
                    "The index table should not be selected without an index hint");
        } catch (AssertionError error) {
            //expected
        }
    }

    @Test
    public void testSimulateConcurrentUpdates() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            TABLE_NAMES.add(dataTableName);
            populateTable(
                    dataTableName); // with two rows ('a', 'ab', 'abc', 'abcd') and ('b', 'bc', 'bcd', 'bcde')
            String indexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE INDEX " + indexTableName + " on " +
                    dataTableName + " (val1) include (val2, val3)" + (async ? "ASYNC" : "") +
                    this.indexDDLOptions);
            TABLE_NAMES.add(indexTableName);
            if (async) {
                // run the index MR job.
                IndexToolIT.runIndexTool(false, null, dataTableName, indexTableName);
            }
            // For the concurrent updates on the same row, the last write phase is ignored.
            // Configure IndexRegionObserver to fail the last write phase (i.e., the post index update phase) where the
            // verify flag is set to true and/or index rows are deleted and check that this does not impact the
            // correctness.
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(true);
            // Do multiple updates on the same data row
            conn.createStatement()
                    .execute("upsert into " + dataTableName + " (id, val2) values ('a', 'abcc')");
            conn.createStatement()
                    .execute("upsert into " + dataTableName + " (id, val1) values ('a', 'aa')");
            conn.commit();
            // The expected state of the index table is  {('aa', 'a', 'abcc', 'abcd'), ('bc', 'b', 'bcd', 'bcde')}
            // Do more multiple updates on the same data row
            conn.createStatement().execute(
                    "upsert into " + dataTableName + " (id, val1, val3) values ('a', null, null)");
            conn.createStatement()
                    .execute("upsert into " + dataTableName + " (id, val1) values ('a', 'ab')");
            conn.createStatement()
                    .execute("upsert into " + dataTableName + " (id, val1) values ('b', 'ab')");
            conn.createStatement().execute(
                    "upsert into " + dataTableName + " (id, val1, val2) values ('b', 'ab', null)");
            conn.commit();
            // Now the expected state of the index table is  {('ab', 'a', 'abcc' , null), ('ab', 'b', null, 'bcde')}
            ResultSet rs = conn.createStatement().executeQuery("SELECT * from " + indexTableName);
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals("ab", rs.getString(1));
            assertEquals("a", rs.getString(2));
            assertEquals("abcc", rs.getString(3));
            assertNull(rs.getString(4));
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals("ab", rs.getString(1));
            assertEquals("b", rs.getString(2));
            assertNull(rs.getString(3));
            assertEquals("bcde", rs.getString(4));
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
            conn.createStatement().execute("CREATE INDEX " + indexTableName + " on " +
                    dataTableName + " (val1) include (val2, val3)" + (async ? "ASYNC" : "") +
                    this.indexDDLOptions);
            TABLE_NAMES.add(dataTableName);
            TABLE_NAMES.add(indexTableName);
            if (async) {
                // run the index MR job.
                IndexToolIT.runIndexTool(false, null, dataTableName, indexTableName);
            }
            String selectSql = "SELECT id from " + dataTableName + " WHERE val1  = 'ab'";
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals("a", rs.getString(1));
            assertFalse(rs.next());

            // Configure IndexRegionObserver to fail the last write phase (i.e., the post index update phase) where the verify flag is set
            // to true and/or index rows are deleted and check that this does not impact the correctness
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(true);
            String dml = "DELETE from " + dataTableName + " WHERE id  = 'a'";
            assertEquals(1, conn.createStatement().executeUpdate(dml));
            conn.commit();

            // The index rows are actually not deleted yet because IndexRegionObserver failed delete operation. However, they are
            // made unverified in the pre index update phase (i.e., the first write phase)
            dml = "DELETE from " + dataTableName + " WHERE val1  = 'ab'";
            // This DML will scan the Index table and detect unverified index rows. This will trigger read repair which
            // result in deleting these rows since the corresponding data table rows are deleted already. So, the number of
            // rows to be deleted by the "DELETE" DML will be zero since the rows deleted by read repair will not be visible
            // to the DML
            assertEquals(0, conn.createStatement().executeUpdate(dml));

            // Count the number of index rows
            String query = "SELECT COUNT(*) from " + indexTableName;
            // There should be one row in the index table
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            // Add rows and check everything is still okay
            verifyTableHealth(conn, dataTableName, indexTableName);
        }
    }

    @Test
    public void testPartialRowUpdateForMutable() throws Exception {
        String dataTableName = generateUniqueName();
        populateTable(
                dataTableName); // with two rows ('a', 'ab', 'abc', 'abcd') and ('b', 'bc', 'bcd', 'bcde')
        Connection conn = DriverManager.getConnection(getUrl());
        String indexTableName = generateUniqueName();
        conn.createStatement().execute("CREATE INDEX " + indexTableName + " on " +
                dataTableName + " (val1) include (val2, val3)" + (async ? "ASYNC" : "") +
                this.indexDDLOptions);
        TABLE_NAMES.add(dataTableName);
        TABLE_NAMES.add(indexTableName);
        if (async) {
            // run the index MR job.
            IndexToolIT.runIndexTool(false, null, dataTableName, indexTableName);
        }
        conn.createStatement()
                .execute("upsert into " + dataTableName + " (id, val2) values ('a', 'abcc')");
        conn.commit();
        conn.createStatement()
                .execute("upsert into " + dataTableName + " (id, val2) values ('c', 'cde')");
        conn.commit();
        String selectSql = "SELECT * from " + dataTableName + " WHERE val1  = 'ab'";
        // Verify that we will read from the index table
        assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
        ResultSet rs = conn.createStatement().executeQuery(selectSql);
        assertTrue(rs.next());
        moveRegionsOfTable(dataTableName);
        moveRegionsOfTable(indexTableName);
        assertEquals("a", rs.getString(1));
        assertEquals("ab", rs.getString(2));
        assertEquals("abcc", rs.getString(3));
        assertEquals("abcd", rs.getString(4));
        assertFalse(rs.next());

        conn.createStatement().execute(
                "upsert into " + dataTableName + " (id, val1, val3) values ('a', 'ab', 'abcdd')");
        conn.commit();
        // Verify that we will read from the index table
        assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
        rs = conn.createStatement().executeQuery(selectSql);
        assertTrue(rs.next());
        moveRegionsOfTable(dataTableName);
        moveRegionsOfTable(indexTableName);
        assertEquals("a", rs.getString(1));
        assertEquals("ab", rs.getString(2));
        assertEquals("abcc", rs.getString(3));
        assertEquals("abcdd", rs.getString(4));
        assertFalse(rs.next());
        conn.close();
    }

    /**
     * Phoenix allows immutable tables with indexes to be overwritten partially as long as the indexed columns are not
     * updated during partial overwrites. However, there is no check/enforcement for this. The immutable index mutations
     * are prepared at the client side without reading existing data table rows. This means the index mutations
     * prepared by the client will be partial when the data table row mutations are partial. The new indexing design
     * assumes index rows are always full and all cells within an index row have the same timestamp. On the read path,
     * GlobalIndexChecker returns only the cells with the most recent timestamp of the row. This means that if the
     * client updates the same row multiple times, the client will read back only the most recent update which could be
     * partial. To support the partial updates for immutable indexes, GlobalIndexChecker allows cells with different
     * timestamps to be be returned to the client for immutable tables even though this breaks the design assumption.
     * This test is to verify the partial row update support for immutable tables.
     * <p>
     * Allowing partial overwrites on immutable indexes is a broken model in the first place. Assuring correctness in
     * the presence of failures does not seem possible without using something similar to the solution for mutable
     * indexes.
     * <p>
     * Immutable indexes should be used only for really immutable tables and for these tables, overwrites
     * should be due to failures and should be idempotent. For everything else, mutable tables should be used
     *
     * @throws Exception
     */
    @Test
    public void testPartialRowUpdateForImmutable() throws Exception {
        if (async || encoded) {
            // No need to run this test more than once
            return;
        }
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            conn.createStatement().execute("create table " + dataTableName +
                    " (id varchar(10) not null primary key, val1 varchar(10), val2 varchar(10), val3 varchar(10))" +
                    " IMMUTABLE_ROWS=true, IMMUTABLE_STORAGE_SCHEME=" +
                    PTableImpl.ImmutableStorageScheme.ONE_CELL_PER_COLUMN);
            conn.createStatement()
                    .execute("upsert into " + dataTableName + " values ('a', 'ab', 'abc', 'abcd')");
            conn.commit();
            String indexTableName = generateUniqueName();
            TABLE_NAMES.add(dataTableName);
            TABLE_NAMES.add(indexTableName);
            conn.createStatement().execute("CREATE INDEX " + indexTableName + " on " +
                    dataTableName + " (val1) include (val2, val3)" + this.indexDDLOptions);
            conn.createStatement().execute("upsert into " + dataTableName +
                    " (id, val1, val2) values ('a', 'ab', 'abcc')");
            conn.commit();
            String selectSql = "SELECT * from " + dataTableName + " WHERE val1  = 'ab'";
            // Verify that we will read from the index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals("a", rs.getString(1));
            assertEquals("ab", rs.getString(2));
            assertEquals("abcc", rs.getString(3));
            assertEquals("abcd", rs.getString(4));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testFailPreIndexRowUpdate() throws Exception {
        String dataTableName = generateUniqueName();
        populateTable(
                dataTableName); // with two rows ('a', 'ab', 'abc', 'abcd') and ('b', 'bc', 'bcd', 'bcde')
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String indexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE INDEX " + indexTableName + " on " +
                    dataTableName + " (val1) include (val2, val3)" + (async ? "ASYNC" : "") +
                    this.indexDDLOptions);
            TABLE_NAMES.add(dataTableName);
            TABLE_NAMES.add(indexTableName);
            if (async) {
                // run the index MR job.
                IndexToolIT.runIndexTool(false, null, dataTableName, indexTableName);
            }
            // Configure IndexRegionObserver to fail the first write phase (i.e., the pre index update phase). This should not
            // lead to any change on index or data table rows
            IndexRegionObserver.setFailPreIndexUpdatesForTesting(true);
            conn.createStatement()
                    .execute("upsert into " + dataTableName + " (id, val2) values ('a', 'abcc')");
            commitWithException(conn);
            conn.createStatement().execute(
                    "upsert into " + dataTableName + " (id, val1, val2) values ('c', 'cd','cde')");
            commitWithException(conn);
            IndexRegionObserver.setFailPreIndexUpdatesForTesting(false);
            String selectSql = "SELECT val2, val3 from " + dataTableName + " WHERE val1  = 'ab'";
            // Verify that we will read from the index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals("abc", rs.getString(1));
            assertEquals("abcd", rs.getString(2));
            assertFalse(rs.next());
            // Add rows and check everything is still okay
            verifyTableHealth(conn, dataTableName, indexTableName);
        }
    }

    @Test
    public void testFailPostIndexRowUpdate() throws Exception {
        String dataTableName = generateUniqueName();
        populateTable(
                dataTableName); // with two rows ('a', 'ab', 'abc', 'abcd') and ('b', 'bc', 'bcd', 'bcde')
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String indexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE INDEX " + indexTableName + " on " +
                    dataTableName + " (val1) include (val2, val3)" + (async ? "ASYNC" : "") +
                    this.indexDDLOptions);
            TABLE_NAMES.add(dataTableName);
            TABLE_NAMES.add(indexTableName);
            if (async) {
                // run the index MR job.
                IndexToolIT.runIndexTool(false, null, dataTableName, indexTableName);
            }
            // Configure IndexRegionObserver to fail the last write phase (i.e., the post index update phase) where the verify flag is set
            // to true and/or index rows are deleted and check that this does not impact the correctness
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(true);
            conn.createStatement()
                    .execute("upsert into " + dataTableName + " (id, val2) values ('a', 'abcc')");
            conn.commit();
            conn.createStatement().execute(
                    "upsert into " + dataTableName + " (id, val1, val2) values ('c', 'cd','cde')");
            conn.commit();
            IndexTool indexTool =
                    IndexToolIT.runIndexTool(false, "", dataTableName, indexTableName, null, 0,
                            IndexTool.IndexVerifyType.ONLY);
            assertEquals(3, indexTool.getJob().getCounters().findCounter(INPUT_RECORDS).getValue());
            assertEquals(3, indexTool.getJob().getCounters().findCounter(SCANNED_DATA_ROW_COUNT)
                    .getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(REBUILT_INDEX_ROW_COUNT)
                    .getValue());
            assertEquals(3, indexTool.getJob().getCounters()
                    .findCounter(BEFORE_REBUILD_VALID_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters()
                    .findCounter(BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters()
                    .findCounter(BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters()
                    .findCounter(BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters()
                    .findCounter(BEFORE_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT)
                    .getValue());
            assertEquals(0, indexTool.getJob().getCounters()
                    .findCounter(BEFORE_REBUILD_BEYOND_MAXLOOKBACK_INVALID_INDEX_ROW_COUNT)
                    .getValue());
            assertEquals(2, indexTool.getJob().getCounters()
                    .findCounter(BEFORE_REBUILD_UNVERIFIED_INDEX_ROW_COUNT).getValue());
            assertEquals(0,
                    indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_OLD_INDEX_ROW_COUNT)
                            .getValue());
            assertEquals(0, indexTool.getJob().getCounters()
                    .findCounter(BEFORE_REBUILD_UNKNOWN_INDEX_ROW_COUNT).getValue());
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
            String selectSql = "SELECT val2, val3 from " + dataTableName +
                    " WHERE val1  = 'ab' and val2 = 'abcc'";
            // Verify that we will read from the index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals("abcc", rs.getString(1));
            assertEquals("abcd", rs.getString(2));
            assertFalse(rs.next());
            // Add rows and check everything is still okay
            verifyTableHealth(conn, dataTableName, indexTableName);
        }
    }

    @Test
    public void testUnverifiedValuesAreNotVisible() throws Exception {
        if (async) {
            // No need to run the same test twice one for async = true and the other for async = false
            return;
        }
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            conn.createStatement().execute("create table " + dataTableName +
                    " (id varchar(10) not null primary key, val1 varchar(10), val2 varchar(10), val3 varchar(10))" +
                    tableDDLOptions);
            String indexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE INDEX " + indexTableName + " on " +
                    dataTableName + " (val1) include (val2, val3)" + this.indexDDLOptions);
            TABLE_NAMES.add(dataTableName);
            TABLE_NAMES.add(indexTableName);

            // Configure IndexRegionObserver to fail the data write phase
            IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
            conn.createStatement()
                    .execute("upsert into " + dataTableName + " values ('a', 'ab','abc', 'abcd')");
            commitWithException(conn);
            // The above upsert will create an unverified index row
            // Configure IndexRegionObserver to allow the data write phase
            IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
            // Insert the same row with missing value for val3
            conn.createStatement().execute(
                    "upsert into " + dataTableName + " (id, val1, val2) values ('a', 'ab','abc')");
            conn.commit();
            // At this moment val3 in the data table row has null value
            String selectSql = "SELECT val3 from " + dataTableName + " WHERE val1  = 'ab'";
            // Verify that we will read from the index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            // Verify that we do not read from the unverified row
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals(null, rs.getString(1));
            assertFalse(rs.next());
            // Add rows and check everything is still okay
            verifyTableHealth(conn, dataTableName, indexTableName);
        }
    }

    @Test
    public void testUnverifiedRowRepair() throws Exception {
        if (async) {
            // No need to run the same test twice one for async = true and the other for async = false
            return;
        }
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            conn.createStatement().execute("create table " + dataTableName +
                    " (id varchar(10) not null primary key, a.val1 varchar(10), b.val2 varchar(10), c.val3 varchar(10))" +
                    tableDDLOptions);
            String indexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE INDEX " + indexTableName + " on " +
                    dataTableName + " (val1) include (val2, val3)" + this.indexDDLOptions);
            TABLE_NAMES.add(dataTableName);
            TABLE_NAMES.add(indexTableName);

            // Configure IndexRegionObserver to fail the data write phase
            IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
            conn.createStatement().execute("upsert into " + dataTableName +
                    " (id, val1, val3) values ('a', 'ab','abcde')");
            commitWithException(conn);
            // The above upsert will create an unverified index row
            // Configure IndexRegionObserver to allow the data write phase
            IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
            // Verify that this row is not visible
            String selectSql = "SELECT * from " + dataTableName + " WHERE val1  = 'ab'";
            // Verify that we will read from the index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            // Verify that we do not read from the unverified row
            assertFalse(rs.next());
            // Insert the same row with a value for val3
            conn.createStatement()
                    .execute("upsert into " + dataTableName + " values ('a', 'ab','abc', 'abcd')");
            conn.commit();
            // At this moment val3 in the data table row should not have null value
            selectSql = "SELECT val3 from " + dataTableName + " WHERE val1  = 'ab'";
            // Verify that we will read from the index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals("abcd", rs.getString(1));
            assertFalse(rs.next());
            // Configure IndexRegionObserver to fail the data write phase
            IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
            conn.createStatement().execute("upsert into " + dataTableName +
                    " (id, val1, val3) values ('a', 'ab','abcde')");
            commitWithException(conn);
            // The above upsert will create an unverified index row
            // Configure IndexRegionObserver to allow the data write phase
            IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
            // Verify that this row is still read back correctly
            for (int i = 0; i < 2; i++) {
                selectSql = "SELECT val3 from " + dataTableName + " WHERE val1  = 'ab'";
                // Verify that we will read from the index table
                assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
                rs = conn.createStatement().executeQuery(selectSql);
                // Verify that the repair of the unverified row did not affect the valid index row
                assertTrue(rs.next());
                moveRegionsOfTable(dataTableName);
                moveRegionsOfTable(indexTableName);
                assertEquals("abcd", rs.getString(1));
                assertFalse(rs.next());
            }
            // Add rows and check everything is still okay
            verifyTableHealth(conn, dataTableName, indexTableName);
        }
    }

    @Test
    public void testOnePhaseOverwiteFollowingTwoPhaseWrite() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            populateTable(
                    dataTableName); // with two rows ('a', 'ab', 'abc', 'abcd') and ('b', 'bc', 'bcd', 'bcde')
            String indexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE INDEX " + indexTableName + "1 on " +
                    dataTableName + " (val1) include (val2, val3)" + (async ? "ASYNC" : "") +
                    this.indexDDLOptions);
            conn.createStatement().execute("CREATE INDEX " + indexTableName + "2 on " +
                    dataTableName + " (val2) include (val1, val3)" + (async ? "ASYNC" : "") +
                    this.indexDDLOptions);
            TABLE_NAMES.add(dataTableName);
            TABLE_NAMES.add(indexTableName + "1");
            TABLE_NAMES.add(indexTableName + "2");
            if (async) {
                // run the index MR job.
                IndexToolIT.runIndexTool(false, null, dataTableName, indexTableName + "1");
                IndexToolIT.runIndexTool(false, null, dataTableName, indexTableName + "2");
            }
            // Two Phase write. This write is recoverable
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(true);
            conn.createStatement()
                    .execute("upsert into " + dataTableName + " values ('c', 'cd', 'cde', 'cdef')");
            conn.commit();
            // One Phase write. This write is not recoverable
            IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
            conn.createStatement().execute(
                    "upsert into " + dataTableName + " values ('c', 'cd', 'cdee', 'cdfg')");
            commitWithException(conn);
            // Let three phase writes happen as in the normal case
            IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
            String selectSql = "SELECT val2, val3 from " + dataTableName + " WHERE val1  = 'cd'";
            // Verify that we will read from the first index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName + "1");
            // Verify the first write is visible but the second one is not
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName + "1");
            moveRegionsOfTable(indexTableName + "2");
            assertEquals("cde", rs.getString(1));
            assertEquals("cdef", rs.getString(2));
            assertFalse(rs.next());
            // Add rows and check everything is still okay
            verifyTableHealth(conn, dataTableName, indexTableName);
        }
    }

    @Test
    public void testOnePhaseOverwrite() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            String indexTableName = generateUniqueName();
            TABLE_NAMES.add(dataTableName);
            TABLE_NAMES.add(indexTableName);
            createTableAndIndexes(conn, dataTableName, indexTableName);
            // Configure IndexRegionObserver to skip the last two write phase (i.e., the data table update and post index
            // update phase) and check that this does not impact the correctness (one overwrite)
            IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(true);
            conn.createStatement().execute("upsert into " + dataTableName +
                    " (id, val2, val3) values ('a', 'abcc', 'abccc')");
            commitWithException(conn);
            IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
            // Read only one column and verify that this is sufficient for the read repair to fix
            // all the columns of the unverified index row that was generated due to doing only one phase write above
            String selectSql = "SELECT val2 from " + dataTableName + " WHERE val1  = 'ab'";
            // Verify that we will read from the first index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName + "1");
            // Verify that one phase write has no effect
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals("abc", rs.getString(1));
            assertFalse(rs.next());
            // Now read the other column and verify that it is also fixed
            selectSql = "SELECT val3 from " + dataTableName + " WHERE val1  = 'ab'";
            // Verify that we will read from the first index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName + "1");
            // Verify that one phase write has no effect
            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals("abcd", rs.getString(1));
            assertFalse(rs.next());
            selectSql = "SELECT val2, val3 from " + dataTableName + " WHERE val2  = 'abcc'";
            // Verify that we will read from the second index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName + "2");
            rs = conn.createStatement().executeQuery(selectSql);
            // Verify that one phase writes have no effect
            assertFalse(rs.next());
            // Configure IndexRegionObserver to skip the last two write phase (i.e., the data table update and post index
            // update phase) and check that this does not impact the correctness  (two overwrites)
            IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(true);
            conn.createStatement()
                    .execute("upsert into " + dataTableName + " (id, val2) values ('a', 'abccc')");
            commitWithException(conn);
            conn.createStatement()
                    .execute("upsert into " + dataTableName + " (id, val2) values ('a', 'abcccc')");
            commitWithException(conn);
            IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
            selectSql = "SELECT val2, val3 from " + dataTableName + " WHERE val1  = 'ab'";
            // Verify that we will read from the first index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName + "1");
            // Verify that one phase writes have no effect
            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals("abc", rs.getString(1));
            assertEquals("abcd", rs.getString(2));
            assertFalse(rs.next());
            selectSql = "SELECT val2, val3 from " + dataTableName + " WHERE val2  = 'abccc'";
            // Verify that we will read from the second index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName + "2");
            rs = conn.createStatement().executeQuery(selectSql);
            // Verify that one phase writes have no effect
            assertFalse(rs.next());
            selectSql = "SELECT val2, val3 from " + dataTableName + " WHERE val2  = 'abcccc'";
            // Verify that we will read from the second index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName + "2");
            rs = conn.createStatement().executeQuery(selectSql);
            // Verify that one phase writes have no effect
            assertFalse(rs.next());
            // Add rows and check everything is still okay
            verifyTableHealth(conn, dataTableName, indexTableName);
        }
    }

    private void createTableAndIndexes(Connection conn, String dataTableName,
                                       String indexTableName) throws Exception {
        createTableAndIndexes(conn, dataTableName, indexTableName, 1);
    }

    private void createTableAndIndexes(Connection conn, String dataTableName,
                                       String indexTableName, int indexVersions) throws Exception {
        populateTable(
                dataTableName); // with two rows ('a', 'ab', 'abc', 'abcd') and ('b', 'bc', 'bcd', 'bcde')
        conn.createStatement().execute("CREATE INDEX " + indexTableName + "1 on " +
                dataTableName + " (val1) include (val2, val3)" + (async ? "ASYNC" : "") +
                " VERSIONS=" + indexVersions +
                (Strings.isNullOrEmpty(this.indexDDLOptions) ? "" : "," + this.indexDDLOptions));
        conn.createStatement().execute("CREATE INDEX " + indexTableName + "2 on " +
                dataTableName + " (val2) include (val1, val3)" + (async ? "ASYNC" : "") +
                " VERSIONS=" + indexVersions +
                (Strings.isNullOrEmpty(this.indexDDLOptions) ? "" : "," + this.indexDDLOptions));
        conn.commit();
        if (async) {
            // run the index MR job.
            IndexToolIT.runIndexTool(false, null, dataTableName, indexTableName + "1");
            IndexToolIT.runIndexTool(false, null, dataTableName, indexTableName + "2");
        }
    }

    @Test
    public void testFailDataTableAndPostIndexRowUpdate() throws Exception {

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            String indexName = generateUniqueName();
            TABLE_NAMES.add(dataTableName);
            TABLE_NAMES.add(indexName);
            createTableAndIndexes(conn, dataTableName, indexName);
            // Configure IndexRegionObserver to fail the last two write phase (i.e., the data table update and post index update phase)
            // and check that this does not impact the correctness
            IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(true);
            conn.createStatement()
                    .execute("upsert into " + dataTableName + " (id, val2) values ('a', 'abcc')");
            commitWithException(conn);
            IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
            conn.createStatement()
                    .execute("upsert into " + dataTableName + " (id, val3) values ('a', 'abcdd')");
            conn.commit();
            String selectSql = "SELECT val2, val3 from " + dataTableName + " WHERE val1  = 'ab'";
            // Verify that we will read from the first index table
            assertExplainPlan(conn, selectSql, dataTableName, indexName + "1");
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexName);
            assertEquals("abc", rs.getString(1));
            assertEquals("abcdd", rs.getString(2));
            assertFalse(rs.next());
            selectSql = "SELECT val2, val3 from " + dataTableName + " WHERE val2  = 'abc'";
            // Verify that we will read from the second index table
            assertExplainPlan(conn, selectSql, dataTableName, indexName + "2");
            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());

            assertEquals("abc", rs.getString(1));
            assertEquals("abcdd", rs.getString(2));
            assertFalse(rs.next());
            // Add rows and check everything is still okay
            verifyTableHealth(conn, dataTableName, indexName);
        }
    }

    @Test
    public void testUnverifiedIndexRowWithFilter() throws Exception {
        if (async) {
            // No need to run the same test twice one for async = true and the other for async = false
            return;
        }
        // running this test with PagingFilter disabled
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.PHOENIX_SERVER_PAGING_ENABLED_ATTRIB,
                String.valueOf(false));
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String dataTableName = generateUniqueName();
            String indexName = generateUniqueName();
            TABLE_NAMES.add(dataTableName);
            TABLE_NAMES.add(indexName);
            conn.createStatement().execute("create table " + dataTableName +
                    " (id integer primary key, name varchar, status integer, val varchar)" +
                    tableDDLOptions);
            conn.commit();
            conn.createStatement().execute("create index " + indexName +
                    " ON " + dataTableName + "(name) include (status, val)");
            conn.createStatement().execute(
                    "upsert into " + dataTableName + " values (1, 'tom', 1, 'blah')");
            conn.createStatement().execute(
                    "upsert into " + dataTableName + " values (2, 'jerry', 2, 'jee')");
            conn.commit();

            // fail phase 2
            IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
            // update row (1, 'tom', 1) -> (1, 'tom', 2)
            conn.createStatement().execute(
                    "upsert into " + dataTableName + " (id, status) values (1, 2)");
            commitWithException(conn);

            // unverified row doesn't match the filter status = 1 but the previous verified
            // version of the same row matches the filter
            String selectSql =
                    "SELECT * from " + dataTableName + " WHERE name = 'tom' AND status = 1";
            // Verify that we will read from the index table
            assertExplainPlan(conn, selectSql, dataTableName, indexName);
            try (ResultSet rs = conn.createStatement().executeQuery(selectSql)) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            } catch (AssertionError e) {
                TestUtil.dumpTable(conn, TableName.valueOf(indexName));
                throw e;
            }

            // fail phase 2
            IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
            // update row (1, 'tom', 1) -> (1, 'tom', 2)
            conn.createStatement().execute(
                    "upsert into " + dataTableName + " (id, status) values (1, 2)");
            commitWithException(conn);
            // unverified row matches the filter status = 2 but the previous verified
            // version of the same row does not match the filter
            selectSql = "SELECT * from " + dataTableName + " WHERE name = 'tom' AND status = 2";
            try (ResultSet rs = conn.createStatement().executeQuery(selectSql)) {
                assertFalse(rs.next());
            } catch (AssertionError e) {
                TestUtil.dumpTable(conn, TableName.valueOf(indexName));
                throw e;
            }

            // fail phase 2
            IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
            // new row (3, 'tom', 1)
            conn.createStatement().execute(
                    "upsert into " + dataTableName + " (id, name, status) values (3, 'tom', 1)");
            commitWithException(conn);

            // Test aggregate query
            selectSql = "SELECT count(*) from " + dataTableName + " WHERE name = 'tom' and  id > 1";
            // Verify that we will read from the index table
            assertExplainPlan(conn, selectSql, dataTableName, indexName);
            try (ResultSet rs = conn.createStatement().executeQuery(selectSql)) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            } catch (AssertionError e) {
                TestUtil.dumpTable(conn, TableName.valueOf(indexName));
                throw e;
            }
        }
    }

    @Test
    public void testUnverifiedIndexRowWithSkipScanFilter() throws Exception {
        if (async) {
            // No need to run the same test twice one for async = true and the other for async = false
            return;
        }

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            String indexName = generateUniqueName();
            TABLE_NAMES.add(dataTableName);
            TABLE_NAMES.add(indexName);
            populateTable(
                    dataTableName); // with two rows ('a', 'ab', 'abc', 'abcd') and ('b', 'bc', 'bcd', 'bcde')
            conn.createStatement().execute("CREATE INDEX " + indexName + " on " +
                    dataTableName + " (val1, val2) include (val3)" + this.indexDDLOptions);
            conn.commit();

            // fail phase 2
            IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
            // update row ('b', 'bc', 'bcd', 'bcde') -> ('b', 'bc', 'bcdd', 'bcde')
            conn.createStatement().execute(
                    "upsert into " + dataTableName + " (id, val1) values ('b', 'bcc')");
            commitWithException(conn);

            String selectSql =
                    "SELECT id, val1, val3 from " + dataTableName + " WHERE val1 IN ('ab', 'bcc') ";
            // Verify that we will read from the index table
            try (ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql)) {
                String actualExplainPlan = QueryUtil.getExplainPlan(rs);
                String expectedExplainPlan =
                        String.format("SKIP SCAN ON 2 KEYS OVER %s", indexName);
                assertTrue(actualExplainPlan.contains(expectedExplainPlan));
            }
            try (ResultSet rs = conn.createStatement().executeQuery(selectSql)) {
                assertTrue(rs.next());
                assertEquals("a", rs.getString("id"));
                assertEquals("ab", rs.getString("val1"));
                assertEquals("abcd", rs.getString("val3"));
                moveRegionsOfTable(dataTableName);
                moveRegionsOfTable(indexName);
                assertFalse(rs.next());
            } catch (AssertionError e) {
                TestUtil.dumpTable(conn, TableName.valueOf(indexName));
                throw e;
            }

            // fail phase 2
            IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
            // update row ('b', 'bc', 'bcd', 'bcde') -> ('b', 'bc', 'bcd', 'bcdf')
            conn.createStatement().execute(
                    "upsert into " + dataTableName + " (id, val3) values ('b', 'bcdf')");
            commitWithException(conn);

            selectSql = "SELECT id, val3 from " + dataTableName +
                    " WHERE val1 IN ('bc') AND val2 IN ('bcd', 'xcdf') AND val3 = 'bcde' ";
            // Verify that we will read from the index table
            try (ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql)) {
                String actualExplainPlan = QueryUtil.getExplainPlan(rs);
                String expectedExplainPlan =
                        String.format("SKIP SCAN ON 2 KEYS OVER %s", indexName);
                String filter = "SERVER FILTER BY";
                assertTrue(String.format("actual=%s", actualExplainPlan),
                        actualExplainPlan.contains(expectedExplainPlan));
                assertTrue(String.format("actual=%s", actualExplainPlan),
                        actualExplainPlan.contains(filter));
            }
            try (ResultSet rs = conn.createStatement().executeQuery(selectSql)) {
                assertTrue(rs.next());
                moveRegionsOfTable(dataTableName);
                moveRegionsOfTable(indexName);
                assertEquals("b", rs.getString("id"));
                assertEquals("bcde", rs.getString("val3"));
                assertFalse(rs.next());
            } catch (AssertionError e) {
                TestUtil.dumpTable(conn, TableName.valueOf(indexName));
                throw e;
            }
        }
    }

    @Test
    public void testUnverifiedIndexRowWithFirstKeyOnlyFilter() throws Exception {
        if (async) {
            // No need to run the same test twice one for async = true and the other for async = false
            return;
        }

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            String indexName = generateUniqueName();
            TABLE_NAMES.add(dataTableName);
            TABLE_NAMES.add(indexName);
            populateTable(
                    dataTableName); // with two rows ('a', 'ab', 'abc', 'abcd') and ('b', 'bc', 'bcd', 'bcde')
            conn.createStatement().execute("CREATE INDEX " + indexName + " on " +
                    dataTableName + " (val1, id, val2, val3) " + this.indexDDLOptions);
            conn.commit();

            // fail phase 2
            IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
            // update row ('b', 'bc', 'bcd', 'bcde') -> ('b', 'bc', 'bcdd', 'bcde')
            conn.createStatement().execute(
                    "upsert into " + dataTableName + " (id, val3) values ('b', 'bcdf')");
            commitWithException(conn);

            String selectSql = "SELECT id, val3 from " + dataTableName +
                    " WHERE val1 = 'bc' and val2 = 'bcd' ";
            // Verify that we will read from the index table
            try (ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql)) {
                String actualExplainPlan = QueryUtil.getExplainPlan(rs);
                String expectedExplainPlan = String.format("RANGE SCAN OVER %s", indexName);
                String filter = String.format("SERVER FILTER BY %s ONLY AND",
                        encoded ? "FIRST KEY" : "EMPTY COLUMN");
                assertTrue(String.format("actual=%s", actualExplainPlan),
                        actualExplainPlan.contains(expectedExplainPlan));
                assertTrue(String.format("actual=%s", actualExplainPlan),
                        actualExplainPlan.contains(filter));
            }
            try (ResultSet rs = conn.createStatement().executeQuery(selectSql)) {
                assertTrue(rs.next());
                moveRegionsOfTable(dataTableName);
                moveRegionsOfTable(indexName);
                assertEquals("b", rs.getString("id"));
                assertEquals("bcde", rs.getString("val3"));
                assertFalse(rs.next());
            } catch (AssertionError e) {
                TestUtil.dumpTable(conn, TableName.valueOf(indexName));
                throw e;
            }
        }
    }

    @Test
    public void testViewIndexRowUpdate() throws Exception {
        if (async) {
            // No need to run the same test twice one for async = true and the other for async = false
            return;
        }
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            // Create a base table
            String dataTableName = generateUniqueName();
            conn.createStatement().execute("create table " + dataTableName +
                    " (oid varchar(10) not null, kp char(3) not null, val1 varchar(10)" +
                    "CONSTRAINT pk PRIMARY KEY (oid, kp)) COLUMN_ENCODED_BYTES=0, MULTI_TENANT=true");
            // Create a view on the base table
            String viewName = generateUniqueName();
            conn.createStatement().execute("CREATE VIEW " + viewName + " (id char(10) not null, " +
                    "val2 varchar, val3 varchar, CONSTRAINT pk PRIMARY KEY (id)) AS SELECT * FROM " +
                    dataTableName +
                    " WHERE kp = '0EC'");
            // Create an index on the view
            String indexName = generateUniqueName();
            TABLE_NAMES.add(dataTableName);
            TABLE_NAMES.add("_IDX_" + dataTableName);
            conn.createStatement().execute("CREATE INDEX " + indexName + " on " +
                    viewName + " (val2) include (val3)" + this.indexDDLOptions);
            Properties props = new Properties();
            props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, "o1");
            try (Connection tenantConn = DriverManager.getConnection(getUrl(), props)) {
                // Configure IndexRegionObserver to fail the last write phase (i.e., the post index update phase)
                // This will leave index rows unverified
                // Create a view of the view
                String childViewName = generateUniqueName();
                tenantConn.createStatement()
                        .execute("CREATE VIEW " + childViewName + " (zid CHAR(15)) " +
                                "AS SELECT * FROM " + viewName);
                // Create another view of the child view
                String grandChildViewName = generateUniqueName();
                tenantConn.createStatement()
                        .execute("CREATE VIEW " + grandChildViewName + " (val4 CHAR(15)) " +
                                "AS SELECT * FROM " + childViewName);
                IndexRegionObserver.setFailPostIndexUpdatesForTesting(true);
                tenantConn.createStatement().execute("upsert into " + childViewName +
                        " (zid, id, val1, val2, val3) VALUES('z1','1', 'a1','b1','c1')");
                tenantConn.commit();
                IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
                // Activate read repair
                String selectSql = "select val3 from  " + childViewName + " WHERE val2  = 'b1'";
                ResultSet rs = tenantConn.createStatement().executeQuery(selectSql);
                assertTrue(rs.next());
                assertEquals("c1", rs.getString("val3"));
                // Configure IndexRegionObserver to fail the last write phase (i.e., the post index update phase)
                // This will leave index rows unverified
                IndexRegionObserver.setFailPostIndexUpdatesForTesting(true);
                tenantConn.createStatement().execute("upsert into " + grandChildViewName +
                        " (zid, id, val2, val3, val4) VALUES('z1', '2', 'b2', 'c2', 'd1')");
                tenantConn.commit();
                IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
                // Activate read repair
                selectSql = "select id, val3 from  " + grandChildViewName + " WHERE val2  = 'b2'";
                rs = tenantConn.createStatement().executeQuery(selectSql);
                assertTrue(rs.next());
                assertEquals("2", rs.getString("id"));
                assertEquals("c2", rs.getString("val3"));
            }
        }
    }

    @Test
    public void testOnDuplicateKeyWithIndex() throws Exception {
        if (async || encoded) { // run only once with single cell encoding enabled
            return;
        }
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            String indexTableName = generateUniqueName();
            populateTable(
                    dataTableName); // with two rows ('a', 'ab', 'abc', 'abcd') and ('b', 'bc', 'bcd', 'bcde')
            conn.createStatement().execute("CREATE INDEX " + indexTableName + " on " +
                    dataTableName + " (val1) include (val2, val3)" + this.indexDDLOptions);
            TABLE_NAMES.add(dataTableName);
            TABLE_NAMES.add(indexTableName);

            conn.commit();
            String upsertSql =
                    "UPSERT INTO " + dataTableName + " VALUES ('a') ON DUPLICATE KEY UPDATE " +
                            "val1 = val1 || val1, val2 = val2 || val2";
            conn.createStatement().execute(upsertSql);
            conn.commit();
            String selectSql = "SELECT * from " + dataTableName + " WHERE val1 = 'abab'";
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertEquals("a", rs.getString(1));
            assertEquals("abab", rs.getString(2));
            assertEquals("abcabc", rs.getString(3));
            assertEquals("abcd", rs.getString(4));
            assertFalse(rs.next());
        }
    }

    static private void commitWithException(Connection conn) {
        try {
            conn.commit();
            IndexRegionObserver.setFailPreIndexUpdatesForTesting(false);
            IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
            fail();
        } catch (Exception e) {
            // this is expected
        }
    }

    static private void verifyTableHealth(Connection conn, String dataTableName,
                                          String indexTableName) throws Exception {
        // Add two rows and check everything is still okay
        conn.createStatement()
                .execute("upsert into " + dataTableName + " values ('a', 'ab', 'abc', 'abcd')");
        conn.createStatement()
                .execute("upsert into " + dataTableName + " values ('z', 'za', 'zab', 'zabc')");
        conn.commit();
        String selectSql = "SELECT * from " + dataTableName + " WHERE val1  = 'ab'";
        ///Verify that we will read from the index table
        assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
        ResultSet rs = conn.createStatement().executeQuery(selectSql);
        assertTrue(rs.next());
        moveRegionsOfTable(dataTableName);
        moveRegionsOfTable(indexTableName);
        assertEquals("a", rs.getString(1));
        assertEquals("ab", rs.getString(2));
        assertEquals("abc", rs.getString(3));
        assertEquals("abcd", rs.getString(4));
        assertFalse(rs.next());
        selectSql = "SELECT * from " + dataTableName + " WHERE val1  = 'za'";
        ///Verify that we will read from the index table
        assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
        rs = conn.createStatement().executeQuery(selectSql);
        conn.commit();
        assertTrue(rs.next());
        moveRegionsOfTable(dataTableName);
        moveRegionsOfTable(indexTableName);
        assertEquals("z", rs.getString(1));
        assertEquals("za", rs.getString(2));
        assertEquals("zab", rs.getString(3));
        assertEquals("zabc", rs.getString(4));
        assertFalse(rs.next());
    }
}
