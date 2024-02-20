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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionStatesCount;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.iterate.ScanningResultPostDummyResultCaller;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.QueryBuilder;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * Base class for tests whose methods run in parallel with
 * 1. Statistics enabled on server side (QueryServices#STATS_COLLECTION_ENABLED is true)
 * 2. Guide Post Width for all relevant tables is 0. Stats are disabled at table level.
 * <p>
 * See {@link org.apache.phoenix.schema.stats.NoOpStatsCollectorIT} for tests that disable
 * stats collection from server side.
 * <p>
 * You must create unique names using {@link #generateUniqueName()} for each
 * table and sequence used to prevent collisions.
 * <p>
 * Any class extending this would also observe multiple region moves while tests are running.
 */
public abstract class ParallelStatsDisabledWithRegionMovesIT extends BaseTest {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(ParallelStatsDisabledWithRegionMovesIT.class);

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

    protected static void splitAllRegionsOfTable(String tableName, int splitAtRow) {
        try (AsyncConnection asyncConnection =
                     ConnectionFactory.createAsyncConnection(getUtility().getConfiguration())
                             .get()) {
            AsyncAdmin admin = asyncConnection.getAdmin();
            List<RegionInfo> regionsOfTable =
                    admin.getRegions(TableName.valueOf(tableName)).get();
            regionsOfTable.forEach(regionInfo -> {
                if (regionInfo.getTable().equals(TableName.valueOf(tableName))) {
                    try {
                        for (int i = 0; i < 5; i++) {
                            RegionStatesCount regionStatesCount =
                                    admin.getClusterMetrics().get().getTableRegionStatesCount()
                                            .get(TableName.valueOf(tableName));
                            byte[] splitPoint;
                            if (regionStatesCount.getRegionsInTransition() == 0 &&
                                    regionStatesCount.getOpenRegions() ==
                                            regionStatesCount.getTotalRegions()) {
                                try (Table table =
                                             utility.getConnection()
                                                     .getTable(TableName.valueOf(tableName))) {
                                    try (ResultScanner resultScanner = table.getScanner(
                                                    new Scan())) {
                                        Result result = null;
                                        for (int rowCount = 0; rowCount < splitAtRow; rowCount++) {
                                            result = resultScanner.next();
                                            if (result == null) {
                                                LOGGER.info("Table {} has only {} rows, splitting" +
                                                                " at row number {} not possible",
                                                        tableName, rowCount, splitAtRow);
                                                return;
                                            }
                                        }
                                        splitPoint = result == null ? null :
                                                ByteUtil.closestPossibleRowAfter(result.getRow());
                                    }
                                }
                                LOGGER.info("Splitting region {}",
                                        regionInfo.getRegionNameAsString());
                                admin.flushRegion(regionInfo.getRegionName()).get(3,
                                        TimeUnit.SECONDS);
                                admin.splitRegion(regionInfo.getRegionName(), splitPoint).get(4,
                                        TimeUnit.SECONDS);
                                break;
                            } else {
                                LOGGER.info("Table {} has some region(s) in RIT or not online",
                                        tableName);
                                Thread.sleep(1000);
                            }
                        }
                    } catch (InterruptedException | ExecutionException | TimeoutException |
                             IOException e) {
                        LOGGER.error("Something went wrong", e);
                        throw new RuntimeException(e);
                    }
                }
            });
        } catch (Exception e) {
            LOGGER.error("Something went wrong..", e);
        }
    }

    protected ResultSet executeQueryThrowsException(Connection conn, QueryBuilder queryBuilder,
                                                    String expectedPhoenixExceptionMsg,
                                                    String expectedSparkExceptionMsg) {
        ResultSet rs = null;
        try {
            rs = executeQuery(conn, queryBuilder);
            fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(expectedPhoenixExceptionMsg));
        }
        return rs;
    }

    public static void validateQueryPlan(Connection conn, QueryBuilder queryBuilder,
                                         String expectedPhoenixPlan, String expectedSparkPlan)
            throws SQLException {
        if (StringUtils.isNotBlank(expectedPhoenixPlan)) {
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + queryBuilder.build());
            assertEquals(expectedPhoenixPlan, QueryUtil.getExplainPlan(rs));
        }
    }

    protected static void assertResultSetWithRegionMoves(ResultSet rs, Object[][] rows,
                                                         String tableName)
            throws Exception {
        for (int rowIndex = 0; rowIndex < rows.length; rowIndex++) {
            assertTrue("rowIndex:[" + rowIndex + "] rs.next error!", rs.next());
            if (rowIndex == 0) {
                moveRegionsOfTable(tableName);
            }
            for (int columnIndex = 1; columnIndex <= rows[rowIndex].length; columnIndex++) {
                Object realValue = rs.getObject(columnIndex);
                Object expectedValue = rows[rowIndex][columnIndex - 1];
                if (realValue == null) {
                    assertNull("rowIndex:[" + rowIndex + "],columnIndex:[" + columnIndex + "]",
                            expectedValue);
                } else {
                    assertEquals("rowIndex:[" + rowIndex + "],columnIndex:[" + columnIndex + "]",
                            expectedValue,
                            realValue
                    );
                }
            }
        }
        assertFalse(rs.next());
    }

}
