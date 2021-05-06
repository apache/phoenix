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
package org.apache.phoenix.monitoring;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.phoenix.end2end.BaseUniqueNamesOwnClusterIT;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.exception.PhoenixIOException;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.CommitException;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.ConfigurationFactory;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.ConnectionQueryServicesImpl;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesTestImpl;
import org.apache.phoenix.util.DelayedOrFailingRegionServer;
import org.apache.phoenix.util.EnvironmentEdge;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.InstanceResolver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME;
import static org.apache.phoenix.exception.SQLExceptionCode.DATA_EXCEEDS_MAX_CAPACITY;
import static org.apache.phoenix.exception.SQLExceptionCode.GET_TABLE_REGIONS_FAIL;
import static org.apache.phoenix.exception.SQLExceptionCode.OPERATION_TIMED_OUT;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_MUTATION_BYTES;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_QUERY_TIME;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_SCAN_BYTES;
import static org.apache.phoenix.monitoring.MetricType.DELETE_AGGREGATE_FAILURE_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.DELETE_AGGREGATE_SUCCESS_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.DELETE_BATCH_FAILED_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.DELETE_BATCH_FAILED_SIZE;
import static org.apache.phoenix.monitoring.MetricType.DELETE_COMMIT_TIME;
import static org.apache.phoenix.monitoring.MetricType.DELETE_FAILED_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.DELETE_MUTATION_BYTES;
import static org.apache.phoenix.monitoring.MetricType.DELETE_MUTATION_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.DELETE_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.DELETE_SQL_QUERY_TIME;
import static org.apache.phoenix.monitoring.MetricType.DELETE_SUCCESS_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.MUTATION_BATCH_FAILED_SIZE;
import static org.apache.phoenix.monitoring.MetricType.MUTATION_BATCH_SIZE;
import static org.apache.phoenix.monitoring.MetricType.MUTATION_BYTES;
import static org.apache.phoenix.monitoring.MetricType.QUERY_FAILED_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.QUERY_POINTLOOKUP_FAILED_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.QUERY_POINTLOOKUP_TIMEOUT_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.QUERY_SCAN_FAILED_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.QUERY_SCAN_TIMEOUT_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.QUERY_TIMEOUT_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.RESULT_SET_TIME_MS;
import static org.apache.phoenix.monitoring.MetricType.SCAN_BYTES;
import static org.apache.phoenix.monitoring.MetricType.SELECT_AGGREGATE_FAILURE_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.SELECT_AGGREGATE_SUCCESS_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.SELECT_FAILED_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.SELECT_POINTLOOKUP_FAILED_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.SELECT_POINTLOOKUP_SUCCESS_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.SELECT_SCAN_FAILED_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.SELECT_SCAN_SUCCESS_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.SELECT_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.SELECT_SQL_QUERY_TIME;
import static org.apache.phoenix.monitoring.MetricType.SELECT_SUCCESS_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.UPSERT_AGGREGATE_FAILURE_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.UPSERT_AGGREGATE_SUCCESS_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.UPSERT_BATCH_FAILED_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.UPSERT_BATCH_FAILED_SIZE;
import static org.apache.phoenix.monitoring.MetricType.UPSERT_COMMIT_TIME;
import static org.apache.phoenix.monitoring.MetricType.UPSERT_FAILED_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.UPSERT_MUTATION_BYTES;
import static org.apache.phoenix.monitoring.MetricType.UPSERT_MUTATION_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.UPSERT_SQL_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.UPSERT_SQL_QUERY_TIME;
import static org.apache.phoenix.monitoring.MetricType.UPSERT_SUCCESS_SQL_COUNTER;
import static org.apache.phoenix.monitoring.PhoenixMetricsIT.POINT_LOOKUP_SELECT_QUERY;
import static org.apache.phoenix.monitoring.PhoenixMetricsIT.RANGE_SCAN_SELECT_QUERY;
import static org.apache.phoenix.monitoring.PhoenixMetricsIT.createTableAndInsertValues;
import static org.apache.phoenix.monitoring.PhoenixMetricsIT.doPointDeleteFromTable;
import static org.apache.phoenix.monitoring.PhoenixMetricsIT.doDeleteAllFromTable;
import static org.apache.phoenix.util.DelayedOrFailingRegionServer.INJECTED_EXCEPTION_STRING;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
import static org.apache.phoenix.util.PhoenixRuntime.clearTableLevelMetrics;
import static org.apache.phoenix.util.PhoenixRuntime.getOverAllReadRequestMetricInfo;
import static org.apache.phoenix.util.PhoenixRuntime.getPhoenixTableClientMetrics;
import static org.apache.phoenix.util.PhoenixRuntime.getRequestReadMetricInfo;
import static org.apache.phoenix.util.PhoenixRuntime.getWriteMetricInfoForMutationsSinceLastReset;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

@Category(NeedsOwnMiniClusterTest.class)
public class PhoenixTableLevelMetricsIT extends BaseUniqueNamesOwnClusterIT {

    private static final String
            CREATE_TABLE_DDL =
            "CREATE TABLE %s (K VARCHAR(%d) NOT NULL" + " PRIMARY KEY, V VARCHAR)";
    private static final String UPSERT_DML = "UPSERT INTO %s VALUES (?, ?)";
    private static final String KEY = "key";
    private static final String VALUE = "value";
    private static boolean failExecuteQueryAndClientSideDeletes;
    private static long injectDelay;
    private static HBaseTestingUtility hbaseTestUtil;

    @BeforeClass public static void doSetup() throws Exception {
        final Configuration conf = HBaseConfiguration.create();
        conf.set(QueryServices.TABLE_LEVEL_METRICS_ENABLED, String.valueOf(true));
        conf.set(QueryServices.METRIC_PUBLISHER_ENABLED, String.valueOf(true));
        conf.set(QueryServices.COLLECT_REQUEST_LEVEL_METRICS, String.valueOf(true));

        InstanceResolver.clearSingletons();
        // Override to get required config for static fields loaded that require HBase config
        InstanceResolver.getSingleton(ConfigurationFactory.class, new ConfigurationFactory() {

            @Override public Configuration getConfiguration() {
                return conf;
            }

            @Override public Configuration getConfiguration(Configuration confToClone) {
                Configuration copy = new Configuration(conf);
                copy.addResource(confToClone);
                return copy;
            }
        });
        hbaseTestUtil = new HBaseTestingUtility();
        hbaseTestUtil.startMiniCluster(1, 1, null, null, DelayedOrFailingRegionServer.class);
        // establish url and quorum. Need to use PhoenixDriver and not PhoenixTestDriver
        String zkQuorum = "localhost:" + hbaseTestUtil.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + zkQuorum;

        // Add our own driver
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(BaseTest.DRIVER_CLASS_NAME_ATTRIB, PhoenixMetricsTestingDriver.class.getName());
        initAndRegisterTestDriver(url, new ReadOnlyProps(props.entrySet().iterator()));
    }

    @AfterClass public static void tearDownMiniCluster() {
        try {
            if (hbaseTestUtil != null) {
                hbaseTestUtil.shutdownMiniCluster();
            }
        } catch (Exception e) {
            // ignore
        }
    }

    /**
     * Assert table-level metrics related to SELECT queries
     *
     * @param tableName                           table name
     * @param isPointLookup                       true if it is a point lookup
     * @param expectedSqlSuccessCt                expected number of successes related to query execution
     * @param expectedSqlFailureCt                expected number of failures related to query execution
     * @param expectedMinTimeElapsed              minimum expected time elapsed during query execution
     * @param hasResultSetIterationStarted        true if we have actually started issuing the scan(s) and
     *                                            iterating over results via ResultSet.next() calls
     * @param expectedResultSetIterFailedCounter  expected number of failures related to rs.next()
     * @param expectedResultSetIterTimeoutCounter expected number of timeouts related to rs.next()
     * @param rs                                  current ResultSet which we can use to check table-level metric values against
     *                                            the ReadMetricQueue and OverallQueryMetrics. Null indicates that rs iteration
     *                                            has not started yet due to an exception in the executeMutation step itself
     */
    static void assertSelectQueryTableMetrics(final String tableName, final boolean isPointLookup,
            final long expectedSelectAggregateSuccessCt,
            final long expectedSelectAggregateFailureCt, final long expectedSqlSuccessCt,
            final long expectedSqlFailureCt, final long expectedMinTimeElapsed,
            final boolean hasResultSetIterationStarted,
            final long expectedResultSetIterFailedCounter,
            final long expectedResultSetIterTimeoutCounter, final ResultSet rs)
            throws SQLException {
        // The resultSet must be closed since we modify certain timing related metrics when calling rs.close()
        if (hasResultSetIterationStarted) {
            assertTrue(rs != null && rs.isClosed());
        } else {
            assertTrue(rs == null || rs.isBeforeFirst());
        }
        assertFalse(getPhoenixTableClientMetrics().isEmpty());
        assertFalse(getPhoenixTableClientMetrics().get(tableName).isEmpty());
        final long expectedTimeToFetchAllRecordsRsNext =
                rs == null ? 0 : getOverAllReadRequestMetricInfo(rs).get(RESULT_SET_TIME_MS);
        final long expectedScanBytes =
                rs == null || rs.isBeforeFirst() ? 0 :
                        getRequestReadMetricInfo(rs).get(tableName).get(SCAN_BYTES);
        final long expectedSqlCounter = expectedSqlSuccessCt + expectedSqlFailureCt;

        for (PhoenixTableMetric metric : getPhoenixTableClientMetrics().get(tableName)) {
            assertMetricValue(metric, SELECT_SQL_COUNTER, expectedSqlCounter, CompareOp.EQ);
            assertMetricValue(metric, SELECT_AGGREGATE_FAILURE_SQL_COUNTER,
                    expectedSelectAggregateFailureCt, CompareOp.EQ);
            assertMetricValue(metric, SELECT_AGGREGATE_SUCCESS_SQL_COUNTER,
                    expectedSelectAggregateSuccessCt, CompareOp.EQ);
            assertMetricValue(metric, SELECT_SUCCESS_SQL_COUNTER, expectedSqlSuccessCt,
                    CompareOp.EQ);
            assertMetricValue(metric, isPointLookup ?
                    SELECT_POINTLOOKUP_SUCCESS_SQL_COUNTER :
                    SELECT_SCAN_SUCCESS_SQL_COUNTER, expectedSqlSuccessCt, CompareOp.EQ);
            assertMetricValue(metric, SELECT_FAILED_SQL_COUNTER, expectedSqlFailureCt,
                    CompareOp.EQ);
            assertMetricValue(metric, isPointLookup ?
                    SELECT_POINTLOOKUP_FAILED_SQL_COUNTER :
                    SELECT_SCAN_FAILED_SQL_COUNTER, expectedSqlFailureCt, CompareOp.EQ);
            assertMetricValue(metric, SELECT_SQL_QUERY_TIME, expectedMinTimeElapsed, CompareOp.GT);

            if (hasResultSetIterationStarted) {
                if (expectedResultSetIterFailedCounter == 0) {
                    assertMetricValue(metric, SCAN_BYTES, 0, CompareOp.GT);
                    assertMetricValue(metric, RESULT_SET_TIME_MS, 0, CompareOp.GT);
                } else {
                    assertMetricValue(metric, SCAN_BYTES, 0, CompareOp.EQ);
                }
                assertMetricValue(metric, SCAN_BYTES, expectedScanBytes, CompareOp.EQ);
                assertMetricValue(metric, QUERY_FAILED_COUNTER, expectedResultSetIterFailedCounter,
                        CompareOp.EQ);
                assertMetricValue(metric, isPointLookup ?
                                QUERY_POINTLOOKUP_FAILED_COUNTER :
                                QUERY_SCAN_FAILED_COUNTER, expectedResultSetIterFailedCounter,
                        CompareOp.EQ);
                assertMetricValue(metric, QUERY_TIMEOUT_COUNTER,
                        expectedResultSetIterTimeoutCounter, CompareOp.EQ);
                assertMetricValue(metric, isPointLookup ?
                                QUERY_POINTLOOKUP_TIMEOUT_COUNTER :
                                QUERY_SCAN_TIMEOUT_COUNTER, expectedResultSetIterTimeoutCounter,
                        CompareOp.EQ);
                assertMetricValue(metric, RESULT_SET_TIME_MS, expectedTimeToFetchAllRecordsRsNext,
                        CompareOp.EQ);
            } else {
                assertMetricValue(metric, SCAN_BYTES, 0, CompareOp.EQ);
                assertMetricValue(metric, QUERY_FAILED_COUNTER, 0, CompareOp.EQ);
                assertMetricValue(metric, isPointLookup ?
                        QUERY_POINTLOOKUP_FAILED_COUNTER :
                        QUERY_SCAN_FAILED_COUNTER, 0, CompareOp.EQ);
                assertMetricValue(metric, QUERY_TIMEOUT_COUNTER, 0, CompareOp.EQ);
                assertMetricValue(metric, isPointLookup ?
                        QUERY_POINTLOOKUP_TIMEOUT_COUNTER :
                        QUERY_SCAN_TIMEOUT_COUNTER, 0, CompareOp.EQ);
                assertMetricValue(metric, RESULT_SET_TIME_MS, 0, CompareOp.EQ);
            }
        }
    }

    /**
     * Assert table-level metrics related to UPSERT and DELETE queries
     *
     * @param tableName                             table name
     * @param expectedUpsertOrDeleteSuccessSqlCt    expected number of successes for upsert or delete
     *                                              mutation execution
     * @param expectedUpsertOrDeleteFailedSqlCt     expected number of failures for upsert or delete
     *                                              mutation execution
     * @param expectedMinUpsertOrDeleteSqlQueryTime minimum expected time elapsed during upsert or
     *                                              delete mutation execution
     * @param hasMutationBeenExplicitlyCommitted    true if conn.commit() was explicitly called,
     *                                              false if connection was autoCommit = true or if
     *                                              conn.commit() was not called
     * @param expectedMutBatchSize                  total number of mutations to be committed
     * @param expectedMinUpsertOrDeleteCommitTime   minimum expected time taken to commit upsert or
     *                                              delete mutations
     * @param expectedUpsertOrDeleteBatchFailedSize expected total size of upsert or delete mutation
     *                                              batches that failed to commit
     * @param writeMutMetrics                       write mutation metrics object
     * @param conn                                  connection object. Note: this method must be called after connection close
     *                                              since that's where we populate table-level write metrics
     */
    private static void assertMutationTableMetrics(final boolean isUpsert, final String tableName,
            final long expectedUpsertOrDeleteSuccessSqlCt,
            final long expectedUpsertOrDeleteFailedSqlCt,
            final long expectedMinUpsertOrDeleteSqlQueryTime,
            final boolean hasMutationBeenExplicitlyCommitted, final long expectedMutBatchSize,
            final long expectedMinUpsertOrDeleteCommitTime,
            final long expectedUpsertOrDeleteBatchFailedSize,
            final long expectedUpsertOrDeleteAggregateSuccessCt,
            final long expectedUpsertOrDeleteAggregateFailureCt,
            final Map<MetricType, Long> writeMutMetrics, final Connection conn)
            throws SQLException {
        assertTrue(conn != null && conn.isClosed());
        assertFalse(hasMutationBeenExplicitlyCommitted && writeMutMetrics == null);
        assertFalse(getPhoenixTableClientMetrics().isEmpty());
        assertFalse(getPhoenixTableClientMetrics().get(tableName).isEmpty());

        final long
                expectedUpsertOrDeleteSqlCt =
                expectedUpsertOrDeleteSuccessSqlCt + expectedUpsertOrDeleteFailedSqlCt;

        for (PhoenixTableMetric metric : getPhoenixTableClientMetrics().get(tableName)) {
            // executeMutation() related metrics:
            assertMetricValue(metric, isUpsert ? UPSERT_SQL_COUNTER : DELETE_SQL_COUNTER,
                    expectedUpsertOrDeleteSqlCt, CompareOp.EQ);
            assertMetricValue(metric,
                    isUpsert ? UPSERT_SUCCESS_SQL_COUNTER : DELETE_SUCCESS_SQL_COUNTER,
                    expectedUpsertOrDeleteSuccessSqlCt, CompareOp.EQ);
            assertMetricValue(metric,
                    isUpsert ? UPSERT_FAILED_SQL_COUNTER : DELETE_FAILED_SQL_COUNTER,
                    expectedUpsertOrDeleteFailedSqlCt, CompareOp.EQ);
            assertMetricValue(metric, isUpsert ? UPSERT_SQL_QUERY_TIME : DELETE_SQL_QUERY_TIME,
                    expectedMinUpsertOrDeleteSqlQueryTime, CompareOp.GTEQ);

            if (hasMutationBeenExplicitlyCommitted) {
                // conn.commit() related metrics
                assertMetricValue(metric, MUTATION_BATCH_SIZE,
                        writeMutMetrics.get(MUTATION_BATCH_SIZE), CompareOp.EQ);
                assertMetricValue(metric, MUTATION_BATCH_SIZE, expectedMutBatchSize, CompareOp.EQ);
                assertMetricValue(metric, MUTATION_BYTES, writeMutMetrics.get(MUTATION_BYTES),
                        CompareOp.EQ);
                assertMetricValue(metric, MUTATION_BATCH_FAILED_SIZE,
                        writeMutMetrics.get(MUTATION_BATCH_FAILED_SIZE), CompareOp.EQ);
                assertMetricValue(metric, MUTATION_BATCH_FAILED_SIZE,
                        expectedUpsertOrDeleteBatchFailedSize, CompareOp.EQ);

                assertMetricValue(metric, isUpsert ? UPSERT_COMMIT_TIME : DELETE_COMMIT_TIME,
                        writeMutMetrics.get(isUpsert ? UPSERT_COMMIT_TIME : DELETE_COMMIT_TIME),
                        CompareOp.EQ);
                if (expectedUpsertOrDeleteAggregateSuccessCt > 0) {
                    assertMetricValue(metric, isUpsert ?
                                    UPSERT_AGGREGATE_SUCCESS_SQL_COUNTER :
                                    DELETE_AGGREGATE_SUCCESS_SQL_COUNTER,
                            expectedUpsertOrDeleteAggregateSuccessCt, CompareOp.EQ);
                }
                if (expectedUpsertOrDeleteAggregateFailureCt > 0) {
                    assertMetricValue(metric, isUpsert ?
                                    UPSERT_AGGREGATE_FAILURE_SQL_COUNTER :
                                    DELETE_AGGREGATE_FAILURE_SQL_COUNTER,
                            expectedUpsertOrDeleteAggregateFailureCt, CompareOp.EQ);
                }
                if (expectedUpsertOrDeleteBatchFailedSize > 0) {
                    assertMetricValue(metric, isUpsert ? UPSERT_COMMIT_TIME : DELETE_COMMIT_TIME, 0,
                            CompareOp.EQ);
                    assertMetricValue(metric,
                            isUpsert ? UPSERT_MUTATION_SQL_COUNTER : DELETE_MUTATION_SQL_COUNTER, 0,
                            CompareOp.EQ);
                } else {
                    assertMetricValue(metric, isUpsert ? UPSERT_COMMIT_TIME : DELETE_COMMIT_TIME,
                            expectedMinUpsertOrDeleteCommitTime, CompareOp.GTEQ);
                    assertMetricValue(metric,
                            isUpsert ? UPSERT_MUTATION_SQL_COUNTER : DELETE_MUTATION_SQL_COUNTER,
                            expectedMutBatchSize, CompareOp.EQ);
                }
                assertMetricValue(metric, isUpsert ? UPSERT_MUTATION_BYTES : DELETE_MUTATION_BYTES,
                        writeMutMetrics
                                .get(isUpsert ? UPSERT_MUTATION_BYTES : DELETE_MUTATION_BYTES),
                        CompareOp.EQ);
                assertMetricValue(metric,
                        isUpsert ? UPSERT_MUTATION_SQL_COUNTER : DELETE_MUTATION_SQL_COUNTER,
                        writeMutMetrics.get(isUpsert ?
                                UPSERT_MUTATION_SQL_COUNTER :
                                DELETE_MUTATION_SQL_COUNTER), CompareOp.EQ);
                assertMetricValue(metric,
                        isUpsert ? UPSERT_BATCH_FAILED_SIZE : DELETE_BATCH_FAILED_SIZE,
                        writeMutMetrics.get(isUpsert ?
                                UPSERT_BATCH_FAILED_SIZE :
                                DELETE_BATCH_FAILED_SIZE), CompareOp.EQ);
                assertMetricValue(metric,
                        isUpsert ? UPSERT_BATCH_FAILED_SIZE : DELETE_BATCH_FAILED_SIZE,
                        expectedUpsertOrDeleteBatchFailedSize, CompareOp.EQ);
                assertMetricValue(metric,
                        isUpsert ? UPSERT_BATCH_FAILED_COUNTER : DELETE_BATCH_FAILED_COUNTER,
                        writeMutMetrics.get(isUpsert ?
                                UPSERT_BATCH_FAILED_COUNTER :
                                DELETE_BATCH_FAILED_COUNTER), CompareOp.EQ);
            }
        }
    }

    /**
     * Checks that if the metric is of the passed in type, it has the expected value
     * (based on the CompareOp). If the metric type is different than checkType, ignore
     *
     * @param m            metric to check
     * @param checkType    type to check for
     * @param compareValue value to compare against
     * @param op           CompareOp
     */
    private static void assertMetricValue(Metric m, MetricType checkType, long compareValue,
            CompareOp op) {
        if (m.getMetricType().equals(checkType)) {
            switch (op) {
            case EQ:
                assertEquals(compareValue, m.getValue());
                break;
            case LT:
                assertTrue(m.getValue() < compareValue);
                break;
            case LTEQ:
                assertTrue(m.getValue() <= compareValue);
                break;
            case GT:
                assertTrue(m.getValue() > compareValue);
                break;
            case GTEQ:
                assertTrue(m.getValue() >= compareValue);
                break;
            }
        }
    }

    @Before public void resetTableLevelMetrics() {
        clearTableLevelMetrics();
        failExecuteQueryAndClientSideDeletes = false;
        injectDelay = 0L;
        // Need to reset otherwise tests that inject their own clock may cause tests run after them
        // to flake
        EnvironmentEdgeManager.reset();
        DelayedOrFailingRegionServer.setDelayEnabled(false);
        DelayedOrFailingRegionServer.injectFailureForRegionOfTable(null);

    }

    @Test public void testTableLevelMetricsforSuccessfulPointLookupQuery() throws Exception {
        String tableName = generateUniqueName();
        ResultSet rs;
        try (Connection conn = getConnFromTestDriver()) {
            createTableAndInsertValues(tableName, false, false, 20, true, conn, false);
        }
        try (Connection conn = getConnFromTestDriver(); Statement stmt = conn.createStatement()) {
            rs = stmt.executeQuery(String.format(POINT_LOOKUP_SELECT_QUERY, tableName));
            assertSelectQueryTableMetrics(tableName, true, 0, 0, 1, 0, 0, false, 0, 0, rs);
            rs.next();
        } // Note that connection close will close the underlying rs too
        assertSelectQueryTableMetrics(tableName, true, 1, 0, 1, 0, 0, true, 0, 0, rs);
    }

    @Test public void testTableLevelMetricsforSuccessfulScanQuery() throws Exception {
        String tableName = generateUniqueName();
        ResultSet rs;
        try (Connection conn = getConnFromTestDriver()) {
            createTableAndInsertValues(tableName, false, false, 20, true, conn, false);
        }
        try (Connection conn = getConnFromTestDriver(); Statement stmt = conn.createStatement()) {
            rs = stmt.executeQuery(String.format(RANGE_SCAN_SELECT_QUERY, tableName));
            assertSelectQueryTableMetrics(tableName, false, 0, 0, 1, 0, 0, false, 0, 0, rs);
            while (rs.next()) {
                //  do nothing
            }
        }
        assertSelectQueryTableMetrics(tableName, false, 1, 0, 1, 0, 0, true, 0, 0, rs);
    }

    @Test public void testTableLevelMetricsforFailingSelectQuery() throws Exception {
        String tableName = generateUniqueName();
        try (Connection conn = getConnFromTestDriver()) {
            createTableAndInsertValues(tableName, false, false, 10, true, conn, false);
        }
        try (Connection conn = getConnFromTestDriver(); Statement stmt = conn.createStatement()) {
            failExecuteQueryAndClientSideDeletes = true;
            try {
                stmt.executeQuery(String.format(POINT_LOOKUP_SELECT_QUERY, tableName));
                fail();
            } catch (SQLException sqlE) {
                assertEquals(GET_TABLE_REGIONS_FAIL.getErrorCode(), sqlE.getErrorCode());
                assertSelectQueryTableMetrics(tableName, true, 0, 1, 0, 1, 0, false, 0, 0, null);
            }
        }
    }

    @Test public void testTableLevelMetricsforDelayedSelectQuery() throws Exception {
        String tableName = generateUniqueName();
        ResultSet rs;
        try (Connection conn = getConnFromTestDriver()) {
            createTableAndInsertValues(tableName, false, false, 10, true, conn, false);
        }
        try (Connection conn = getConnFromTestDriver(); Statement stmt = conn.createStatement()) {
            injectDelay = 1000;
            rs = stmt.executeQuery(String.format(POINT_LOOKUP_SELECT_QUERY, tableName));
            assertSelectQueryTableMetrics(tableName, true, 0, 0, 1, 0, injectDelay, false, 0, 0,
                    rs);
            rs.next();
        }
        assertSelectQueryTableMetrics(tableName, true, 1, 0, 1, 0, injectDelay, true, 0, 0, rs);
    }

    @Test public void testTableLevelMetricsForSelectFetchResultsTimeout() throws SQLException {
        String tableName = generateUniqueName();
        final int queryTimeout = 10; //seconds
        ResultSet rs;
        try (Connection conn = getConnFromTestDriver()) {
            createTableAndInsertValues(tableName, false, false, 2, true, conn, false);
        }
        try (Connection conn = getConnFromTestDriver(); Statement stmt = conn.createStatement()) {
            stmt.setQueryTimeout(queryTimeout);
            rs = stmt.executeQuery(String.format(POINT_LOOKUP_SELECT_QUERY, tableName));
            assertSelectQueryTableMetrics(tableName, true, 0, 0, 1, 0, 0, false, 0, 0, rs);
            // Make the query time out with a longer delay than the set query timeout value (in ms)
            MyClock clock = new MyClock(10, queryTimeout * 2 * 1000);
            EnvironmentEdgeManager.injectEdge(clock);
            try {
                rs.next();
                fail();
            } catch (SQLException e) {
                assertEquals(OPERATION_TIMED_OUT.getErrorCode(), e.getErrorCode());
            }
        }
        assertSelectQueryTableMetrics(tableName, true, 0, 1, 1, 0, 0, true, 1, 1, rs);
    }

    @Test public void testTableLevelMetricsForSelectFetchResultsTimeoutSlowScanner()
            throws SQLException {
        String tableName = generateUniqueName();
        final int queryTimeout = 10; //seconds
        ResultSet rs;
        try (Connection conn = getConnFromTestDriver()) {
            createTableAndInsertValues(tableName, false, false, 10, true, conn, false);
        }
        try (Connection conn = getConnFromTestDriver(); Statement stmt = conn.createStatement()) {
            stmt.setQueryTimeout(queryTimeout);
            rs = stmt.executeQuery(String.format(POINT_LOOKUP_SELECT_QUERY, tableName));
            assertSelectQueryTableMetrics(tableName, true, 0, 0, 1, 0, 0, false, 0, 0, rs);

            // Delay the RS scanner by a value greater than the query timeout
            DelayedOrFailingRegionServer.setDelayEnabled(true);
            DelayedOrFailingRegionServer.setDelayScan(queryTimeout * 1000 + 1); // ms
            try {
                rs.next();
                fail();
            } catch (SQLException e) {
                assertEquals(OPERATION_TIMED_OUT.getErrorCode(), e.getErrorCode());
                assertTrue(getOverAllReadRequestMetricInfo(rs).get(RESULT_SET_TIME_MS)
                        >= queryTimeout * 1000);
            }
        }
        assertSelectQueryTableMetrics(tableName, true, 0, 1, 1, 0, 0, true, 1, 1, rs);
    }

    @Test public void testTableLevelMetricsForSelectFetchResultsServerSideFailure()
            throws SQLException {
        String tableName = generateUniqueName();
        ResultSet rs;
        try (Connection conn = getConnFromTestDriver()) {
            createTableAndInsertValues(tableName, false, false, 10, true, conn, false);
        }
        try (Connection conn = getConnFromTestDriver(); Statement stmt = conn.createStatement()) {
            rs = stmt.executeQuery(String.format(POINT_LOOKUP_SELECT_QUERY, tableName));
            assertSelectQueryTableMetrics(tableName, true, 0, 0, 1, 0, 0, false, 0, 0, rs);

            // Inject a failure during the scan operation on the server-side
            DelayedOrFailingRegionServer.injectFailureForRegionOfTable(tableName);
            try {
                while (rs.next()) {
                    // do nothing
                }
                fail();
            } catch (PhoenixIOException e) {
                Throwable doNotRetryIOException = null;
                for (Throwable t = e.getCause(); t != null; t = t.getCause()) {
                    if (t instanceof DoNotRetryIOException) {
                        doNotRetryIOException = t;
                        break;
                    }
                }
                assertNotNull(doNotRetryIOException);
                assertTrue(doNotRetryIOException.getMessage().contains(INJECTED_EXCEPTION_STRING));
            }
        }
        assertSelectQueryTableMetrics(tableName, true, 0, 1, 1, 0, 0, true, 1, 0, rs);
    }

    @Test public void testTableLevelMetricsForUpsert() throws Throwable {
        String tableName = generateUniqueName();
        int numRows = 10000;
        Connection conn = null;
        Throwable exception = null;
        try {
            conn = getConnFromTestDriver();
            createTableAndInsertValues(tableName, true, false, numRows, true, conn, false);
        } catch (Throwable t) {
            exception = t;
        } finally {
            // Otherwise the test fails with an error from assertions below instead of the real exception
            if (exception != null) {
                throw exception;
            }
            assertNotNull("Failed to get a connection!", conn);
            // Get write metrics before closing the connection since that clears those metrics
            Map<MetricType, Long>
                    writeMutMetrics =
                    getWriteMetricInfoForMutationsSinceLastReset(conn).get(tableName);
            conn.close();
            // Must be asserted after connection close since that's where
            // we populate table-level metrics
            assertMutationTableMetrics(true, tableName, numRows, 0, 0, true, numRows, 0, 0, 1, 0,
                    writeMutMetrics, conn);
        }
    }

    @Test public void testTableLevelMetricsForBatchUpserts() throws Throwable {
        String tableName = generateUniqueName();
        int numRows = 20;
        Connection conn = null;
        Throwable exception = null;
        try {
            conn = getConnFromTestDriver();
            createTableAndInsertValues(tableName, true, false, numRows, true, conn, true);
        } catch (Throwable t) {
            exception = t;
        } finally {
            // Otherwise the test fails with an error from assertions below instead of the real exception
            if (exception != null) {
                throw exception;
            }
            assertNotNull("Failed to get a connection!", conn);
            // Get write metrics before closing the connection since that clears those metrics
            Map<MetricType, Long>
                    writeMutMetrics =
                    getWriteMetricInfoForMutationsSinceLastReset(conn).get(tableName);
            conn.close();
            assertMutationTableMetrics(true, tableName, numRows, 0, 0, true, numRows, 0, 0, 1, 0,
                    writeMutMetrics, conn);
        }
    }

    @Test public void testTableLevelMetricsAutoCommitTrueUpsert() throws Throwable {
        String tableName = generateUniqueName();
        String ddl = String.format(CREATE_TABLE_DDL, tableName, 20);
        int numRows = 10;
        try (Connection conn = getConnFromTestDriver(); Statement stmt = conn.createStatement()) {
            stmt.execute(ddl);
        }
        String dml = String.format(UPSERT_DML, tableName);
        Connection conn = null;
        Throwable exception = null;
        try {
            conn = getConnFromTestDriver();
            try (PreparedStatement prepStmt = conn.prepareStatement(dml)) {
                conn.setAutoCommit(true);
                for (int i = 0; i < numRows; i++) {
                    prepStmt.setString(1, KEY + i);
                    prepStmt.setString(2, VALUE + i);
                    prepStmt.executeUpdate();
                }
            }
        } catch (Throwable t) {
            exception = t;
        } finally {
            // Otherwise the test fails with an error from assertions below instead of the real exception
            if (exception != null) {
                throw exception;
            }
            assertNotNull("Failed to get a connection!", conn);
            Map<MetricType, Long>
                    writeMutMetrics =
                    getWriteMetricInfoForMutationsSinceLastReset(conn).get(tableName);
            conn.close();
            // Time taken during executeMutation should be longer than the actual
            // mutation commit time since autoCommit was on
            assertMutationTableMetrics(true, tableName, numRows, 0,
                    writeMutMetrics.get(UPSERT_COMMIT_TIME), true, numRows, 0, 0, numRows, 0,
                    writeMutMetrics, conn);
        }
    }

    @Test public void testTableLevelMetricsforFailingUpsert() throws Throwable {
        String tableName = generateUniqueName();
        // Restrict the key to just 2 characters so that we fail later
        String ddl = String.format(CREATE_TABLE_DDL, tableName, 2);
        try (Connection conn = getConnFromTestDriver(); Statement stmt = conn.createStatement()) {
            stmt.execute(ddl);
        }
        String dml = String.format(UPSERT_DML, tableName);
        Connection conn = null;
        Throwable exception = null;
        try {
            conn = getConnFromTestDriver();
            try (PreparedStatement prepStmt = conn.prepareStatement(dml)) {
                prepStmt.setString(1, KEY);
                prepStmt.setString(2, VALUE);
                try {
                    prepStmt.executeUpdate();
                    fail();
                } catch (SQLException sqlE) {
                    assertEquals(DATA_EXCEEDS_MAX_CAPACITY.getErrorCode(), sqlE.getErrorCode());
                }
            }
        } catch (Throwable t) {
            exception = t;
        } finally {
            // Otherwise the test fails with an error from assertions below instead of the real exception
            if (exception != null) {
                throw exception;
            }
            assertNotNull("Failed to get a connection!", conn);
            conn.close();
            assertMutationTableMetrics(true, tableName, 0, 1, 0, false, 0, 0, 0, 1, 0, null, conn);
        }
    }

    @Test public void testTableLevelMetricsforUpsertSqlTime() throws Throwable {
        String tableName = generateUniqueName();
        String ddl = String.format(CREATE_TABLE_DDL, tableName, 10);
        int numRows = 10;
        long delay = 300;
        try (Connection conn = getConnFromTestDriver(); Statement stmt = conn.createStatement()) {
            stmt.execute(ddl);
        }
        String dml = String.format(UPSERT_DML, tableName);
        Connection conn = null;
        Throwable exception = null;
        try {
            conn = getConnFromTestDriver();
            try (PreparedStatement prepStmt = conn.prepareStatement(dml)) {
                MyClock clock = new MyClock(10, delay);
                EnvironmentEdgeManager.injectEdge(clock);
                for (int i = 0; i < numRows; i++) {
                    prepStmt.setString(1, KEY + i);
                    prepStmt.setString(2, VALUE + i);
                    prepStmt.executeUpdate();
                }
            }
            conn.commit();
        } catch (Throwable t) {
            exception = t;
        } finally {
            // Otherwise the test fails with an error from assertions below instead of the real exception
            if (exception != null) {
                throw exception;
            }
            assertNotNull("Failed to get a connection!", conn);
            Map<MetricType, Long>
                    writeMutMetrics =
                    getWriteMetricInfoForMutationsSinceLastReset(conn).get(tableName);
            conn.close();
            assertMutationTableMetrics(true, tableName, numRows, 0, delay, true, numRows, 0, 0, 1,
                    0, writeMutMetrics, conn);
        }
    }

    @Test public void testTableLevelMetricsUpsertCommitFailedWithAutoCommitTrue() throws Throwable {
        String tableName = generateUniqueName();
        String ddl = String.format(CREATE_TABLE_DDL, tableName, 10);
        int numRows = 10;
        try (Connection conn = getConnFromTestDriver(); Statement stmt = conn.createStatement()) {
            stmt.execute(ddl);
        }
        String dml = String.format(UPSERT_DML, tableName);
        Connection conn = null;
        Throwable exception = null;
        try {
            conn = getConnFromTestDriver();
            conn.setAutoCommit(true);
            DelayedOrFailingRegionServer.injectFailureForRegionOfTable(tableName);
            try (PreparedStatement prepStmt = conn.prepareStatement(dml)) {
                for (int i = 0; i < numRows; i++) {
                    prepStmt.setString(1, KEY + i);
                    prepStmt.setString(2, VALUE + i);
                    prepStmt.executeUpdate();
                }
            }
        } catch (CommitException e) {
            Throwable retriesExhaustedEx = null;
            for (Throwable t = e.getCause(); t != null; t = t.getCause()) {
                if (t instanceof RetriesExhaustedWithDetailsException) {
                    retriesExhaustedEx = t;
                    break;
                }
            }
            assertNotNull(retriesExhaustedEx);
            assertTrue(retriesExhaustedEx.getMessage().contains(INJECTED_EXCEPTION_STRING));
        } catch (Throwable t) {
            exception = t;
        } finally {
            // Otherwise the test fails with an error from assertions below instead of the real exception
            if (exception != null) {
                throw exception;
            }
            assertNotNull("Failed to get a connection!", conn);
            Map<MetricType, Long>
                    writeMutMetrics =
                    getWriteMetricInfoForMutationsSinceLastReset(conn).get(tableName);
            conn.close();
            assertMutationTableMetrics(true, tableName, 0, 1, 0, true, 1, 0, 1, 0, 1,
                    writeMutMetrics, conn);
        }
    }

    @Test public void testTableLevelMetricsUpsertCommitFailed() throws Throwable {
        String tableName = generateUniqueName();
        String ddl = String.format(CREATE_TABLE_DDL, tableName, 10);
        int numRows = 10;
        try (Connection conn = getConnFromTestDriver(); Statement stmt = conn.createStatement()) {
            stmt.execute(ddl);
        }
        String dml = String.format(UPSERT_DML, tableName);
        Connection conn = null;
        Throwable exception = null;
        try {
            conn = getConnFromTestDriver();
            try (PreparedStatement prepStmt = conn.prepareStatement(dml)) {
                for (int i = 0; i < numRows; i++) {
                    prepStmt.setString(1, KEY + i);
                    prepStmt.setString(2, VALUE + i);
                    prepStmt.executeUpdate();
                }
            }
            DelayedOrFailingRegionServer.injectFailureForRegionOfTable(tableName);
            try {
                conn.commit();
                fail();
            } catch (CommitException e) {
                Throwable retriesExhaustedEx = null;
                for (Throwable t = e.getCause(); t != null; t = t.getCause()) {
                    if (t instanceof RetriesExhaustedWithDetailsException) {
                        retriesExhaustedEx = t;
                        break;
                    }
                }
                assertNotNull(retriesExhaustedEx);
                assertTrue(retriesExhaustedEx.getMessage().contains(INJECTED_EXCEPTION_STRING));
            }
        } catch (Throwable t) {
            exception = t;
        } finally {
            // Otherwise the test fails with an error from assertions below instead of the real exception
            if (exception != null) {
                throw exception;
            }
            assertNotNull("Failed to get a connection!", conn);
            Map<MetricType, Long>
                    writeMutMetrics =
                    getWriteMetricInfoForMutationsSinceLastReset(conn).get(tableName);
            conn.close();
            assertMutationTableMetrics(true, tableName, numRows, 0, 0, true, numRows, 0, numRows, 0,
                    1, writeMutMetrics, conn);
        }
    }

    @Test public void testUpsertCommitTimeSlowRS() throws Throwable {
        String tableName = generateUniqueName();
        String ddl = String.format(CREATE_TABLE_DDL, tableName, 10);
        int numRows = 10;
        final int delayRs = 5000; // ms
        try (Connection conn = getConnFromTestDriver(); Statement stmt = conn.createStatement()) {
            stmt.execute(ddl);
        }
        String dml = String.format(UPSERT_DML, tableName);
        Connection conn = null;
        Throwable exception = null;
        try {
            conn = getConnFromTestDriver();
            try (PreparedStatement prepStmt = conn.prepareStatement(dml)) {
                for (int i = 0; i < numRows; i++) {
                    prepStmt.setString(1, KEY + i);
                    prepStmt.setString(2, VALUE + i);
                    prepStmt.executeUpdate();
                }
            }
            DelayedOrFailingRegionServer.setDelayEnabled(true);
            DelayedOrFailingRegionServer.setDelayMultiOp(delayRs);
            conn.commit();
        } catch (Throwable t) {
            exception = t;
        } finally {
            // Otherwise the test fails with an error from assertions below instead of the real exception
            if (exception != null) {
                throw exception;
            }
            assertNotNull("Failed to get a connection!", conn);
            Map<MetricType, Long>
                    writeMutMetrics =
                    getWriteMetricInfoForMutationsSinceLastReset(conn).get(tableName);
            conn.close();
            assertMutationTableMetrics(true, tableName, numRows, 0, 0, true, numRows, delayRs, 0, 1,
                    0, writeMutMetrics, conn);
        }
    }

    @Test public void testTableLevelMetricsForPointDelete() throws Throwable {
        String tableName = generateUniqueName();
        int numRows = 15;
        Connection conn = null;
        Throwable exception = null;
        try {
            conn = getConnFromTestDriver();
            createTableAndInsertValues(tableName, true, true, numRows, true, conn, false);
            // Reset metrics from the upserts
            PhoenixRuntime.resetMetrics(conn);
            clearTableLevelMetrics();
            doPointDeleteFromTable(tableName, conn);
            conn.commit();
        } catch (Throwable t) {
            exception = t;
        } finally {
            // Otherwise the test fails with an error from assertions below instead of the real exception
            if (exception != null) {
                throw exception;
            }
            assertNotNull("Failed to get a connection!", conn);
            // Get write metrics before closing the connection since that clears those metrics
            Map<MetricType, Long>
                    writeMutMetrics =
                    getWriteMetricInfoForMutationsSinceLastReset(conn).get(tableName);
            conn.close();
            assertMutationTableMetrics(false, tableName, 1, 0, 0, true, 1, 0, 0, 1, 0,
                    writeMutMetrics, conn);
        }
    }

    @Test public void testTableLevelMetricsForDeleteAll() throws Throwable {
        String tableName = generateUniqueName();
        int numRows = 15;
        Connection conn = null;
        Throwable exception = null;
        try {
            conn = getConnFromTestDriver();
            createTableAndInsertValues(tableName, true, true, numRows, true, conn, false);
            // Reset metrics from the upserts
            PhoenixRuntime.resetMetrics(conn);
            clearTableLevelMetrics();
            doDeleteAllFromTable(tableName, conn);
            conn.commit();
        } catch (Throwable t) {
            exception = t;
        } finally {
            // Otherwise the test fails with an error from assertions below instead of the real exception
            if (exception != null) {
                throw exception;
            }
            assertNotNull("Failed to get a connection!", conn);
            // Get write metrics before closing the connection since that clears those metrics
            Map<MetricType, Long>
                    writeMutMetrics =
                    getWriteMetricInfoForMutationsSinceLastReset(conn).get(tableName);
            conn.close();
            assertMutationTableMetrics(false, tableName, 1, 0, 0, true, numRows, 0, 0, 1, 0,
                    writeMutMetrics, conn);
        }
    }

    @Test public void testTableLevelMetricsAutoCommitTrueDelete() throws Throwable {
        String tableName = generateUniqueName();
        int numRows = 15;
        Connection conn = null;
        Throwable exception = null;
        try (Connection ddlAndUpsertConn = getConnFromTestDriver()) {
            createTableAndInsertValues(tableName, true, true, numRows, true, ddlAndUpsertConn,
                    false);
            // Reset metrics from the upserts
            PhoenixRuntime.resetMetrics(ddlAndUpsertConn);
            clearTableLevelMetrics();
        }
        try {
            conn = getConnFromTestDriver();
            conn.setAutoCommit(true);
            doPointDeleteFromTable(tableName, conn);
        } catch (Throwable t) {
            exception = t;
        } finally {
            // Otherwise the test fails with an error from assertions below instead of the real exception
            if (exception != null) {
                throw exception;
            }
            assertNotNull("Failed to get a connection!", conn);
            // Get write metrics before closing the connection since that clears those metrics
            Map<MetricType, Long>
                    writeMutMetrics =
                    getWriteMetricInfoForMutationsSinceLastReset(conn).get(tableName);
            // When autoCommit = true, deletes happen on the server and so mutation metrics are not
            // accumulated for those mutations
            assertNull(writeMutMetrics);
            conn.close();
            assertMutationTableMetrics(false, tableName, 1, 0, 0, false, 0, 0, 0, 0, 0,
                    writeMutMetrics, conn);
        }
    }

    @Test public void testTableLevelMetricsforFailingDelete() throws Throwable {
        String tableName = generateUniqueName();
        int numRows = 15;
        Connection conn = null;
        Throwable exception = null;
        try {
            conn = getConnFromTestDriver();
            createTableAndInsertValues(tableName, true, true, numRows, true, conn, false);
            // Reset metrics from the upserts
            PhoenixRuntime.resetMetrics(conn);
            clearTableLevelMetrics();

            failExecuteQueryAndClientSideDeletes = true;
            try {
                doPointDeleteFromTable(tableName, conn);
                fail();
            } catch (SQLException sqlE) {
                assertEquals(GET_TABLE_REGIONS_FAIL.getErrorCode(), sqlE.getErrorCode());
            }
        } catch (Throwable t) {
            exception = t;
        } finally {
            // Otherwise the test fails with an error from assertions below instead of the real exception
            if (exception != null) {
                throw exception;
            }
            assertNotNull("Failed to get a connection!", conn);
            // Get write metrics before closing the connection since that clears those metrics
            Map<MetricType, Long>
                    writeMutMetrics =
                    getWriteMetricInfoForMutationsSinceLastReset(conn).get(tableName);
            assertNull(writeMutMetrics);
            conn.close();
            assertMutationTableMetrics(false, tableName, 0, 1, 0, false, 0, 0, 0, 0, 1, null, conn);
        }
    }

    @Test public void testTableLevelMetricsforDelayedDeleteQuery() throws Throwable {
        String tableName = generateUniqueName();
        Connection conn = null;
        Throwable exception = null;
        try {
            conn = getConnFromTestDriver();
            createTableAndInsertValues(tableName, false, true, 10, true, conn, false);
            // Reset metrics from the upserts
            PhoenixRuntime.resetMetrics(conn);
            clearTableLevelMetrics();

            injectDelay = 3000;
            doPointDeleteFromTable(tableName, conn);
            conn.commit();
        } catch (Throwable t) {
            exception = t;
        } finally {
            // Otherwise the test fails with an error from assertions below instead of the real exception
            if (exception != null) {
                throw exception;
            }
            assertNotNull("Failed to get a connection!", conn);
            // Get write metrics before closing the connection since that clears those metrics
            Map<MetricType, Long>
                    writeMutMetrics =
                    getWriteMetricInfoForMutationsSinceLastReset(conn).get(tableName);
            conn.close();
            assertMutationTableMetrics(false, tableName, 1, 0, injectDelay, true, 1, 0, 0, 1, 0,
                    writeMutMetrics, conn);
        }
    }

    @Test public void testTableLevelMetricsDeleteCommitFailed() throws Throwable {
        String tableName = generateUniqueName();
        int numRows = 15;
        Connection conn = null;
        Throwable exception = null;
        try {
            conn = getConnFromTestDriver();
            createTableAndInsertValues(tableName, true, true, numRows, true, conn, false);
            // Reset metrics from the upserts
            PhoenixRuntime.resetMetrics(conn);
            clearTableLevelMetrics();
            doDeleteAllFromTable(tableName, conn);

            DelayedOrFailingRegionServer.injectFailureForRegionOfTable(tableName);
            try {
                conn.commit();
                fail();
            } catch (CommitException e) {
                Throwable retriesExhaustedEx = null;
                for (Throwable t = e.getCause(); t != null; t = t.getCause()) {
                    if (t instanceof RetriesExhaustedWithDetailsException) {
                        retriesExhaustedEx = t;
                        break;
                    }
                }
                assertNotNull(retriesExhaustedEx);
                assertTrue(retriesExhaustedEx.getMessage().contains(INJECTED_EXCEPTION_STRING));
            }
        } catch (Throwable t) {
            exception = t;
        } finally {
            // Otherwise the test fails with an error from assertions below instead of the real exception
            if (exception != null) {
                throw exception;
            }
            assertNotNull("Failed to get a connection!", conn);
            // Get write metrics before closing the connection since that clears those metrics
            Map<MetricType, Long>
                    writeMutMetrics =
                    getWriteMetricInfoForMutationsSinceLastReset(conn).get(tableName);
            conn.close();
            assertMutationTableMetrics(false, tableName, 1, 0, 0, true, numRows, 0, numRows, 0, 1,
                    writeMutMetrics, conn);
        }
    }

    @Test public void testDeleteCommitTimeSlowRS() throws Throwable {
        String tableName = generateUniqueName();
        int numRows = 15;
        Connection conn = null;
        Throwable exception = null;
        final int delayRs = 5000; //ms
        try {
            conn = getConnFromTestDriver();
            createTableAndInsertValues(tableName, true, true, numRows, true, conn, false);
            // Reset metrics from the upserts
            PhoenixRuntime.resetMetrics(conn);
            clearTableLevelMetrics();
            doDeleteAllFromTable(tableName, conn);

            DelayedOrFailingRegionServer.setDelayEnabled(true);
            DelayedOrFailingRegionServer.setDelayMultiOp(delayRs);
            conn.commit();
        } catch (Throwable t) {
            exception = t;
        } finally {
            // Otherwise the test fails with an error from assertions below instead of the real exception
            if (exception != null) {
                throw exception;
            }
            assertNotNull("Failed to get a connection!", conn);
            // Get write metrics before closing the connection since that clears those metrics
            Map<MetricType, Long>
                    writeMutMetrics =
                    getWriteMetricInfoForMutationsSinceLastReset(conn).get(tableName);
            conn.close();
            assertMutationTableMetrics(false, tableName, 1, 0, 0, true, numRows, delayRs, 0, 1, 0,
                    writeMutMetrics, conn);
        }
    }

    private Connection getConnFromTestDriver() throws SQLException {
        Connection conn = DriverManager.getConnection(url);
        assertTrue(conn.unwrap(PhoenixConnection.class)
                .getQueryServices() instanceof PhoenixMetricsTestingQueryServices);
        return conn;
    }

    private long getMetricFromTableMetrics(String tableName, MetricType type) {
        Long value = TableMetricsManager.getMetricValue(tableName, type);
        Assert.assertNotNull(value);
        return value;
    }

    private enum CompareOp {
        LT, EQ, GT, LTEQ, GTEQ
    }

    private static class MyClock extends EnvironmentEdge {
        private final long delay;
        private AtomicLong time;

        public MyClock(long time, long delay) {
            this.time = new AtomicLong(time);
            this.delay = delay;
        }

        @Override public long currentTime() {
            long currentTime = this.time.get();
            this.time.addAndGet(this.delay);
            return currentTime;
        }
    }

    /**
     * Custom QueryServices object which we can use to inject failures and delays
     */
    private static class PhoenixMetricsTestingQueryServices extends ConnectionQueryServicesImpl {

        PhoenixMetricsTestingQueryServices(QueryServices services,
                PhoenixEmbeddedDriver.ConnectionInfo connectionInfo, Properties info) {
            super(services, connectionInfo, info);
        }

        // Make plan.iterator() fail (ultimately calls CQSI.getAllTableRegions())
        @Override public List<HRegionLocation> getAllTableRegions(byte[] tableName)
                throws SQLException {
            if (failExecuteQueryAndClientSideDeletes) {
                throw new SQLExceptionInfo.Builder(GET_TABLE_REGIONS_FAIL).build().buildException();
            }
            try {
                Thread.sleep(injectDelay);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return super.getAllTableRegions(tableName);
        }

    }

    /**
     * Custom driver to return a custom QueryServices object
     */
    public static class PhoenixMetricsTestingDriver extends PhoenixTestDriver {
        private ConnectionQueryServices cqs;
        private ReadOnlyProps overrideProps;

        public PhoenixMetricsTestingDriver(ReadOnlyProps props) {
            overrideProps = props;
        }

        @Override public boolean acceptsURL(String url) {
            return true;
        }

        @Override public synchronized ConnectionQueryServices getConnectionQueryServices(String url,
                Properties info) throws SQLException {
            if (cqs == null) {
                cqs =
                        new PhoenixMetricsTestingQueryServices(
                                new QueryServicesTestImpl(getDefaultProps(), overrideProps),
                                ConnectionInfo.create(url), info);
                cqs.init(url, info);
            }
            return cqs;
        }
    }
}