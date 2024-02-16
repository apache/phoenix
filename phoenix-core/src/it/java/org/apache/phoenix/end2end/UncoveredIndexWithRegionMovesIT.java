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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionStatesCount;
import org.apache.phoenix.iterate.ScanningResultPostDummyResultCaller;
import org.apache.phoenix.monitoring.MetricType;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.phoenix.end2end.index.GlobalIndexCheckerIT.assertExplainPlan;
import static org.apache.phoenix.end2end.index.GlobalIndexCheckerIT.assertExplainPlanWithLimit;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_PAGED_ROWS_COUNTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Uncovered index tests that include some region moves while performing rs#next.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class UncoveredIndexWithRegionMovesIT extends ParallelStatsDisabledIT {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(UncoveredIndexWithRegionMovesIT.class);

    private static boolean hasTestStarted = false;
    private static int countOfDummyResults = 0;
    private static final List<String> TABLE_NAMES = Collections.synchronizedList(new ArrayList<>());

    private static class TestScanningResultPostDummyResultCaller extends
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
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }
    }

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(2);
        props.put(QueryServices.PHOENIX_SERVER_PAGE_SIZE_MS, Long.toString(0));
        props.put(QueryServices.COLLECT_REQUEST_LEVEL_METRICS, String.valueOf(true));
        props.put(QueryServices.TESTS_MINI_CLUSTER_NUM_REGION_SERVERS, String.valueOf(2));
        props.put(HConstants.HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY, String.valueOf(1));
        props.put(QueryServices.PHOENIX_POST_DUMMY_PROCESS,
                TestScanningResultPostDummyResultCaller.class.getName());
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @After
    public void tearDown() throws Exception {
        TABLE_NAMES.clear();
        hasTestStarted = false;
        countOfDummyResults = 0;
    }

    private void assertServerPagingMetric(String tableName, ResultSet rs, boolean isPaged)
            throws SQLException {
        Map<String, Map<MetricType, Long>> metrics = PhoenixRuntime.getRequestReadMetricInfo(rs);
        for (Map.Entry<String, Map<MetricType, Long>> entry : metrics.entrySet()) {
            assertEquals(String.format("Got %s", entry.getKey()), tableName, entry.getKey());
            Map<MetricType, Long> metricValues = entry.getValue();
            Long pagedRowsCntr = metricValues.get(MetricType.PAGED_ROWS_COUNTER);
            assertNotNull(pagedRowsCntr);
            if (isPaged) {
                assertTrue(String.format("Got %d", pagedRowsCntr), pagedRowsCntr > 0);
            } else {
                assertEquals(String.format("Got %d", pagedRowsCntr), 0, (long) pagedRowsCntr);
            }
        }
        assertTrue(GLOBAL_PAGED_ROWS_COUNTER.getMetric().getValue() > 0);
    }

    private static void moveRegionsOfTable(String tableName)
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

    @Test
    public void testUncoveredQueryWithGroupBy() throws Exception {
        hasTestStarted = true;
        String dataTableName = generateUniqueName();
        populateTable(
                dataTableName); // with two rows ('a', 'ab', 'abc', 'abcd') and ('b', 'bc', 'bcd', 'bcde')
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String indexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE UNCOVERED INDEX "
                    + indexTableName + " on " + dataTableName + " (val1) ");
            TABLE_NAMES.add(dataTableName);
            TABLE_NAMES.add(indexTableName);
            String selectSql;
            int limit = 10;
            // Verify that an index hint is not necessary for an uncovered index
            selectSql = "SELECT  val2, val3 from " + dataTableName +
                    " WHERE val1 = 'bc' AND (val2 = 'bcd' OR val3 ='bcde') LIMIT " + limit;
            assertExplainPlanWithLimit(conn, selectSql, dataTableName, indexTableName, limit);

            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertTrue(rs.next());
            assertEquals("bcd", rs.getString(1));
            assertEquals("bcde", rs.getString(2));
            assertFalse(rs.next());
            assertServerPagingMetric(indexTableName, rs, true);

            // Add another row and run a group by query where the uncovered index should be used
            conn.createStatement().execute("upsert into " + dataTableName
                    + " (id, val1, val2, val3) values ('c', 'ab','cde', 'cdef')");
            conn.commit();

            selectSql = "SELECT count(val3) from " + dataTableName
                    + " where val1 > '0' GROUP BY val1";
            // Verify that we will read from the index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
            rs = conn.createStatement().executeQuery(selectSql);

            TestUtil.dumpTable(conn, TableName.valueOf(dataTableName));
            TestUtil.dumpTable(conn, TableName.valueOf(indexTableName));

            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertFalse(rs.next());

            selectSql = "SELECT count(val3) from " + dataTableName + " where val1 > '0'";
            // Verify that we will read from the index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));

            // Run an order by query where the uncovered index should be used
            selectSql = "SELECT val3 from " + dataTableName + " where val1 > '0' ORDER BY val1";
            // Verify that we will read from the index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
            rs = conn.createStatement().executeQuery(selectSql);
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertTrue(rs.next());
            assertEquals("abcd", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("cdef", rs.getString(1));
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertTrue(rs.next());
            assertEquals("bcde", rs.getString(1));
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertFalse(rs.next());
        }
    }

    @Test
    public void testUncoveredQuery() throws Exception {
        testUncoveredUtil(false);
    }

    @Test
    public void testUncoveredQueryWithLimit() throws Exception {
        testUncoveredUtil(true);
    }

    private void testUncoveredUtil(boolean limit) throws Exception {
        hasTestStarted = true;
        String dataTableName = generateUniqueName();
        populateTable(dataTableName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String indexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE UNCOVERED INDEX "
                    + indexTableName + " on " + dataTableName + " (val1) ");
            conn.createStatement().execute("upsert into " + dataTableName
                    + " (id, val1, val2, val3) values ('c', 'cd','cde', 'cdef')");
            conn.createStatement().execute("upsert into " + dataTableName
                    + " (id, val1, val2, val3) values ('d', 'de','de1', 'de11')");
            conn.createStatement().execute("upsert into " + dataTableName
                    + " (id, val1, val2, val3) values ('e', 'ef','ef1', 'ef11')");
            conn.createStatement().execute("upsert into " + dataTableName
                    + " (id, val1, val2, val3) values ('f', 'fg','fg1', 'fg11')");
            conn.createStatement().execute("upsert into " + dataTableName
                    + " (id, val1, val2, val3) values ('g', 'gh','gh1', 'gh11')");
            conn.createStatement().execute("upsert into " + dataTableName
                    + " (id, val1, val2, val3) values ('h', 'hi','hi1', 'hi11')");
            conn.createStatement().execute("upsert into " + dataTableName
                    + " (id, val1, val2, val3) values ('i', 'ij','ij1', 'ij11')");
            conn.createStatement().execute("upsert into " + dataTableName
                    + " (id, val1, val2, val3) values ('j', 'jk','jk1', 'jk11')");
            conn.createStatement().execute("upsert into " + dataTableName
                    + " (id, val1, val2, val3) values ('k', 'kl','kl1', 'kl11')");
            conn.commit();
            TABLE_NAMES.add(dataTableName);
            TABLE_NAMES.add(indexTableName);
            String selectSql;

            // Verify that an index hint is not necessary for an uncovered index
            selectSql = "SELECT  val2, val3 from " + dataTableName +
                    " WHERE val1 IS NOT NULL" + (limit ? " LIMIT 15" : "");

            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertTrue(rs.next());
            assertEquals("abc", rs.getString(1));
            assertEquals("abcd", rs.getString(2));
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertTrue(rs.next());
            assertEquals("bcd", rs.getString(1));
            assertEquals("bcde", rs.getString(2));
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertTrue(rs.next());
            assertEquals("cde", rs.getString(1));
            assertEquals("cdef", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("de1", rs.getString(1));
            assertEquals("de11", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("ef1", rs.getString(1));
            assertEquals("ef11", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("fg1", rs.getString(1));
            assertEquals("fg11", rs.getString(2));
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertTrue(rs.next());
            assertEquals("gh1", rs.getString(1));
            assertEquals("gh11", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("hi1", rs.getString(1));
            assertEquals("hi11", rs.getString(2));
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
            assertTrue(rs.next());
            assertEquals("ij1", rs.getString(1));
            assertEquals("ij11", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("jk1", rs.getString(1));
            assertEquals("jk11", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("kl1", rs.getString(1));
            assertEquals("kl11", rs.getString(2));
            assertFalse(rs.next());
            assertServerPagingMetric(indexTableName, rs, true);
        }
    }

    private void populateTable(String tableName) throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("create table " + tableName +
                " (id varchar(10) not null primary key, val1 varchar(10), val2 varchar(10)," +
                " val3 varchar(10))");
        conn.createStatement().execute("upsert into " + tableName
                + " values ('a', 'ab', 'abc', 'abcd')");
        conn.commit();
        conn.createStatement().execute("upsert into " + tableName
                + " values ('b', 'bc', 'bcd', 'bcde')");
        conn.commit();
        conn.close();
    }
}
