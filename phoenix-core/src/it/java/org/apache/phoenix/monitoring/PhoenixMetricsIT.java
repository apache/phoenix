/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.phoenix.monitoring;

import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_RETRIES_NUMBER;
import static org.apache.hadoop.hbase.HConstants.ZK_SESSION_TIMEOUT;
import static org.apache.phoenix.exception.SQLExceptionCode.OPERATION_TIMED_OUT;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_FAILED_QUERY_COUNTER;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_FAILED_PHOENIX_CONNECTIONS;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HBASE_COUNT_BYTES_REGION_SERVER_RESULTS;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HBASE_COUNT_MILLS_BETWEEN_NEXTS;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HBASE_COUNT_RPC_CALLS;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HBASE_COUNT_SCANNED_REGIONS;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HCONNECTIONS_COUNTER;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_MUTATION_BATCH_FAILED_COUNT;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_MUTATION_BATCH_SIZE;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_MUTATION_BYTES;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_MUTATION_COMMIT_TIME;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_MUTATION_INDEX_COMMIT_FAILURE_COUNT;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_MUTATION_SQL_COUNTER;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_NUM_PARALLEL_SCANS;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_OPEN_PHOENIX_CONNECTIONS;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_PHOENIX_CONNECTIONS_ATTEMPTED_COUNTER;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_PHOENIX_CONNECTIONS_THROTTLED_COUNTER;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_QUERY_SERVICES_COUNTER;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_QUERY_TIME;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_QUERY_TIMEOUT_COUNTER;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_REJECTED_TASK_COUNTER;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_SCAN_BYTES;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_SELECT_SQL_COUNTER;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_SPOOL_FILE_COUNTER;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_TASK_END_TO_END_TIME;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_TASK_EXECUTION_TIME;
import static org.apache.phoenix.monitoring.MetricType.COUNT_MILLS_BETWEEN_NEXTS;
import static org.apache.phoenix.monitoring.MetricType.DELETE_COMMIT_TIME;
import static org.apache.phoenix.monitoring.MetricType.MEMORY_CHUNK_BYTES;
import static org.apache.phoenix.monitoring.MetricType.MUTATION_COMMIT_TIME;
import static org.apache.phoenix.monitoring.MetricType.QUERY_TIMEOUT_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.TASK_END_TO_END_TIME;
import static org.apache.phoenix.monitoring.MetricType.TASK_EXECUTED_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.TASK_EXECUTION_TIME;
import static org.apache.phoenix.monitoring.MetricType.TASK_QUEUE_WAIT_TIME;
import static org.apache.phoenix.monitoring.MetricType.UPSERT_COMMIT_TIME;
import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static org.apache.phoenix.util.PhoenixRuntime.UPSERT_BATCH_SIZE_ATTRIB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hbase.thirdparty.com.google.common.base.Joiner;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.log.LogLevel;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.EnvironmentEdge;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests that
 * 1. Phoenix Global metrics are exposed via
 *   a. PhoenixRuntime b. Hadoop-Metrics2 defined sinks
 * 2. Phoenix Request level metrics are exposed via
 *   a. PhoenixRuntime
 */
@Category(NeedsOwnMiniClusterTest.class)
public class PhoenixMetricsIT extends BasePhoenixMetricsIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixMetricsIT.class);

    private static final String DDL = "CREATE TABLE IF NOT EXISTS %s ("
            + "  A CHAR(18) NOT NULL, "
            + "  B VARCHAR(30) NOT NULL, "
            + "  C VARCHAR(255) NOT NULL, "
            + "  D VARCHAR(255) NOT NULL, "
            + "  E VARBINARY, "
            + "  F INTEGER, "
            + "  G BOOLEAN, "
            + "  H VARCHAR(30), "
            + "  I DATE, "
            + "  J DATE, "
            + "  CONSTRAINT PK PRIMARY KEY (A, B, C, D))"
            + " IMMUTABLE_ROWS=true, VERSIONS=1, MULTI_TENANT=true,"
            + " DISABLE_TABLE_SOR=true, TTL=864000";
    private static final String UPSERT_VALUES_DML = "UPSERT INTO %s (A, B, C, D, F, G, I)"
            + " VALUES (?, ?, ?, ?, ?, ?, ?)";
    private static final String UPSERT_SELECT_DML = "UPSERT INTO %s (A, B, C, D)"
            + " SELECT A, B, C, D FROM %s";
    private static final String POINT_DELETE_DML =
            "DELETE FROM %s WHERE A=? AND B=? AND C=? AND D=? AND G=FALSE";
    private static final String DELETE_ALL_DML = "DELETE FROM %s";

    private static final List<MetricType> mutationMetricsToSkip =
            Lists.newArrayList(MUTATION_COMMIT_TIME, UPSERT_COMMIT_TIME, DELETE_COMMIT_TIME);
    private static final List<MetricType> readMetricsToSkip =
            Lists.newArrayList(TASK_QUEUE_WAIT_TIME, TASK_EXECUTION_TIME, TASK_END_TO_END_TIME,
                    COUNT_MILLS_BETWEEN_NEXTS);
    private static final String CUSTOM_URL_STRING = "SESSION";
    private static final AtomicInteger numConnections = new AtomicInteger(0);
    static final String POINT_LOOKUP_SELECT_QUERY = "SELECT J, G, E, (NOW() - I)*24*60*60*1000 FROM"
            + " %s WHERE A='keyA1' AND B='keyB1' AND C='keyC1' AND D='keyD1'";
    static final String RANGE_SCAN_SELECT_QUERY = "SELECT A, B, C FROM"
            + " %s WHERE A='keyA1' AND B='keyB1' AND C > 'keyC0'";

    private static class MyClock extends EnvironmentEdge {
        private long time;
        private final long delay;

        public MyClock (long time, long delay) {
            this.time = time;
            this.delay = delay;
        }

        @Override
        public long currentTime() {
            long currentTime = this.time;
            this.time += this.delay;
            return currentTime;
        }
    }

    @Test
    public void testResetGlobalPhoenixMetrics() throws Exception {
        resetGlobalMetrics();
        for (GlobalMetric m : PhoenixRuntime.getGlobalPhoenixClientMetrics()) {
            assertTrue(m instanceof GlobalMetricImpl);
            assertEquals(0, m.getValue());
            assertEquals(0, m.getNumberOfSamples());
        }
        assertTrue(verifyMetricsFromSink());
    }

    @Test
    public void testGlobalPhoenixMetricsForQueries() throws Exception {
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            createTableAndInsertValues(tableName, true, false, 10, true, conn, false);
        }
        resetGlobalMetrics(); // we want to count metrics related only to the below query
        Connection conn = DriverManager.getConnection(getUrl());
        String query = "SELECT * FROM " + tableName;
        ResultSet rs = conn.createStatement().executeQuery(query);
        while (rs.next()) {
            rs.getString(1);
            rs.getString(2);
        }
        assertEquals(1, GLOBAL_NUM_PARALLEL_SCANS.getMetric().getValue());
        assertEquals(1, GLOBAL_SELECT_SQL_COUNTER.getMetric().getValue());
        assertEquals(0, GLOBAL_REJECTED_TASK_COUNTER.getMetric().getValue());
        assertEquals(0, GLOBAL_QUERY_TIMEOUT_COUNTER.getMetric().getValue());
        assertEquals(0, GLOBAL_FAILED_QUERY_COUNTER.getMetric().getValue());
        assertEquals(0, GLOBAL_SPOOL_FILE_COUNTER.getMetric().getValue());
        assertEquals(0, GLOBAL_MUTATION_BATCH_SIZE.getMetric().getValue());
        assertEquals(0, GLOBAL_MUTATION_BYTES.getMetric().getValue());
        assertEquals(0, GLOBAL_MUTATION_BATCH_FAILED_COUNT.getMetric().getValue());
        assertEquals(0, GLOBAL_MUTATION_INDEX_COMMIT_FAILURE_COUNT.getMetric().getValue());

        assertTrue(GLOBAL_SCAN_BYTES.getMetric().getValue() > 0);
        assertTrue(GLOBAL_QUERY_TIME.getMetric().getValue() > 0);
        assertTrue(GLOBAL_TASK_END_TO_END_TIME.getMetric().getValue() > 0);
        assertTrue(GLOBAL_TASK_EXECUTION_TIME.getMetric().getValue() > 0);

        assertTrue(GLOBAL_HBASE_COUNT_RPC_CALLS.getMetric().getValue() > 0);
        assertTrue(GLOBAL_HBASE_COUNT_MILLS_BETWEEN_NEXTS.getMetric().getValue() > 0);
        assertTrue(GLOBAL_HBASE_COUNT_BYTES_REGION_SERVER_RESULTS.getMetric().getValue() > 0);
        assertTrue(GLOBAL_HBASE_COUNT_SCANNED_REGIONS.getMetric().getValue() > 0);

        assertTrue(verifyMetricsFromSink());
    }

    @Test
    public void testGlobalPhoenixMetricsForMutations() throws Exception {
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            createTableAndInsertValues(tableName, true, false, 10, true, conn, false);
        }
        assertEquals(10, GLOBAL_MUTATION_BATCH_SIZE.getMetric().getValue());
        assertEquals(10, GLOBAL_MUTATION_SQL_COUNTER.getMetric().getValue());
        assertTrue(GLOBAL_MUTATION_BYTES.getMetric().getValue() > 0);
        assertTrue(GLOBAL_MUTATION_COMMIT_TIME.getMetric().getValue() > 0);
        assertEquals(0, GLOBAL_NUM_PARALLEL_SCANS.getMetric().getValue());
        assertEquals(0, GLOBAL_SELECT_SQL_COUNTER.getMetric().getValue());
        assertEquals(0, GLOBAL_REJECTED_TASK_COUNTER.getMetric().getValue());
        assertEquals(0, GLOBAL_QUERY_TIMEOUT_COUNTER.getMetric().getValue());
        assertEquals(0, GLOBAL_FAILED_QUERY_COUNTER.getMetric().getValue());
        assertEquals(0, GLOBAL_SPOOL_FILE_COUNTER.getMetric().getValue());
        assertEquals(0, GLOBAL_MUTATION_BATCH_FAILED_COUNT.getMetric().getValue());
        assertEquals(0, GLOBAL_MUTATION_INDEX_COMMIT_FAILURE_COUNT.getMetric().getValue());

        assertTrue(verifyMetricsFromSink());
    }

    @Test
    public void testGlobalPhoenixMetricsForUpsertSelect() throws Exception {
        String tableFrom = generateUniqueName();
        String tableTo = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            createTableAndInsertValues(tableFrom, true, false, 10, true, conn, false);
        }
        resetGlobalMetrics();
        String ddl = String.format(DDL, tableTo);
        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement()) {
            stmt.execute(ddl);
            resetGlobalMetrics();
            stmt.executeUpdate(String.format(UPSERT_SELECT_DML, tableTo, tableFrom));
            conn.commit();
        }
        assertEquals(10, GLOBAL_MUTATION_BATCH_SIZE.getMetric().getValue());
        assertEquals(1, GLOBAL_MUTATION_SQL_COUNTER.getMetric().getValue());
        assertEquals(1, GLOBAL_NUM_PARALLEL_SCANS.getMetric().getValue());
        assertEquals(0, GLOBAL_QUERY_TIME.getMetric().getValue());
        assertTrue(GLOBAL_SCAN_BYTES.getMetric().getValue() > 0);
        assertTrue(GLOBAL_MUTATION_BYTES.getMetric().getValue() > 0);
        assertTrue(GLOBAL_MUTATION_COMMIT_TIME.getMetric().getValue() > 0);
        assertEquals(0, GLOBAL_SELECT_SQL_COUNTER.getMetric().getValue());
        assertEquals(0, GLOBAL_REJECTED_TASK_COUNTER.getMetric().getValue());
        assertEquals(0, GLOBAL_QUERY_TIMEOUT_COUNTER.getMetric().getValue());
        assertEquals(0, GLOBAL_FAILED_QUERY_COUNTER.getMetric().getValue());
        assertEquals(0, GLOBAL_SPOOL_FILE_COUNTER.getMetric().getValue());
        assertEquals(0, GLOBAL_MUTATION_BATCH_FAILED_COUNT.getMetric().getValue());
        assertEquals(0, GLOBAL_MUTATION_INDEX_COMMIT_FAILURE_COUNT.getMetric().getValue());

        assertTrue(GLOBAL_HBASE_COUNT_RPC_CALLS.getMetric().getValue() > 0);
        assertTrue(GLOBAL_HBASE_COUNT_MILLS_BETWEEN_NEXTS.getMetric().getValue() > 0);
        assertTrue(GLOBAL_HBASE_COUNT_BYTES_REGION_SERVER_RESULTS.getMetric().getValue() > 0);
        assertTrue(GLOBAL_HBASE_COUNT_SCANNED_REGIONS.getMetric().getValue() > 0);

        assertTrue(verifyMetricsFromSink());
    }

    static void resetGlobalMetrics() {
        for (GlobalMetric m : PhoenixRuntime.getGlobalPhoenixClientMetrics()) {
            m.reset();
        }
    }

    // Phoenix Client Metrics are transported via Hadoop-metrics2 sink
    // The test sink is defined at GlobalPhoenixMetricsTestSink
    // Configuration for Hadoop-metrics2 comes from hadoop-metrics2.properties file located in test/resources
    private boolean verifyMetricsFromSink() {
        Map<String, Long> expectedMetrics = new HashMap<>();
        for (GlobalMetric m : PhoenixRuntime.getGlobalPhoenixClientMetrics()) {
            expectedMetrics.put(m.getMetricType().name(), m.getValue());
        }

        for (int i = 0; i < MAX_RETRIES; i++) {
            LOGGER.info("Verifying Global Metrics from Hadoop Sink, Retry: " + (i + 1));
            if (verifyMetricsFromSinkOnce(expectedMetrics)) {
                LOGGER.info("Values from Hadoop Metrics Sink match actual values");
                return true;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    private boolean verifyMetricsFromSinkOnce(Map<String, Long> expectedMetrics) {
        synchronized (GlobalPhoenixMetricsTestSink.lock) {
            for (AbstractMetric metric : GlobalPhoenixMetricsTestSink.metrics) {
                if (expectedMetrics.containsKey(metric.name())) {
                    Long value = expectedMetrics.get(metric.name());
                    if (value != null) {
                        long expectedValue = value;
                        long actualValue = metric.value().longValue();
                        if (expectedValue != actualValue) {
                            LOGGER.warn("Metric from Hadoop Sink: "
                                    + metric.name() + " didn't match expected.");
                            return false;
                        }
                        expectedMetrics.remove(metric.name());
                    }
                }
            }
        }
        assertEquals("Metric expected but not present in Hadoop Metrics Sink "
                + "(GlobalPhoenixMetricsTestSink)", 0, expectedMetrics.size());
        return true;
    }

    static void createTableAndInsertValues(String tableName,
            boolean resetGlobalMetricsAfterTableCreate, boolean resetTableMetricsAfterTableCreate,
            int numRows, boolean commit, Connection conn, boolean batchUpserts) throws SQLException {

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(String.format(DDL, tableName));
        }
        conn.commit();
        if (resetGlobalMetricsAfterTableCreate) {
            resetGlobalMetrics();
        }

        if (resetTableMetricsAfterTableCreate) {
            PhoenixRuntime.clearTableLevelMetrics();
        }
        // executing upserts/mutations.
        try (PreparedStatement stmt = conn.prepareStatement(
                String.format(UPSERT_VALUES_DML, tableName))) {
            for (int i = 1; i <= numRows; i++) {
                stmt.setString(1, "keyA" + i);
                stmt.setString(2, "keyB" + i);
                stmt.setString(3, "keyC" + i);
                stmt.setString(4, "keyD" + i);
                stmt.setBoolean(6, false);
                // Set non-null for just half of the rows to ensure there's no problem with a null
                // date when we query using the SELECT_QUERY
                if (i % 2 == 0) {
                    stmt.setInt(5, i);
                    stmt.setDate(7, new Date(System.currentTimeMillis()));
                } else {
                    stmt.setNull(5, Types.INTEGER);
                    stmt.setNull(7, Types.DATE);
                }
                if (batchUpserts) {
                    stmt.addBatch();
                    if (i % 3 == 0) {
                        stmt.executeBatch();
                    }
                } else {
                    stmt.executeUpdate();
                }
            }
            // Explicitly execute the batch to make sure we haven't missed any batches above
            if (batchUpserts) {
                stmt.executeBatch();
            }
            if (commit) {
                conn.commit();
            }
        }
    }

    static void doPointDeleteFromTable(String tableName, Connection conn) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(
                String.format(POINT_DELETE_DML, tableName))) {
            stmt.setString(1, "keyA1");
            stmt.setString(2, "keyB1");
            stmt.setString(3, "keyC1");
            stmt.setString(4, "keyD1");
            stmt.executeUpdate();
        }
    }

    static void doDeleteAllFromTable(String tableName, Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(String.format(DELETE_ALL_DML, tableName));
        }
    }

    @Test
    public void testOverallQueryMetricsForSelect() throws Exception {
        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE " + tableName + " (K VARCHAR NOT NULL PRIMARY KEY, V VARCHAR)" + " SALT_BUCKETS = 6";
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(ddl);
    }

    // See PHOENIX-5101
    @Test
    public void testMetricsLargeQuery() throws Exception {
        String tableName = "MY_TABLE";
        String ddl = "CREATE TABLE " + tableName + " (K VARCHAR NOT NULL PRIMARY KEY, V VARCHAR)";
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(ddl);
        long numRows = 18750;
        insertRowsInTable(tableName, numRows);
        String query = "SELECT * FROM " + tableName;
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        while (rs.next()) {}
        rs.close();
    }

    @Test
    public void testMetricsForSelectFetchResultsTimeout() throws SQLException {
        String tableName = generateUniqueName();
        final int queryTimeout = 10; //seconds

        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement()) {
            createTableAndInsertValues(tableName, true, true, 10, true, conn, false);
            stmt.setQueryTimeout(queryTimeout);
            ResultSet rs = stmt.executeQuery(String.format("SELECT * FROM %s", tableName));
            // Make the query time out with a longer delay than the set query timeout value (in ms)
            MyClock clock = new MyClock(10, queryTimeout * 2 * 1000);
            EnvironmentEdgeManager.injectEdge(clock);
            try {
                rs.next();
                fail();
            } catch (SQLException e) {
                assertEquals(OPERATION_TIMED_OUT.getErrorCode(), e.getErrorCode());
            }
            Map<MetricType, Long> overallReadMetrics =
                    PhoenixRuntime.getOverAllReadRequestMetricInfo(rs);
            assertEquals(1L, (long)overallReadMetrics.get(QUERY_TIMEOUT_COUNTER));
            assertEquals(1L, GLOBAL_QUERY_TIMEOUT_COUNTER.getMetric().getValue());
        } finally {
            EnvironmentEdgeManager.reset();
        }
    }

    @Test
    public void testReadMetricsForSelect() throws Exception {
        String tableName = generateUniqueName();
        long numSaltBuckets = 6;
        String ddl = "CREATE TABLE " + tableName + " (K VARCHAR NOT NULL PRIMARY KEY, V VARCHAR)" + " SALT_BUCKETS = "
                + numSaltBuckets;
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(ddl);

        long numRows = 1000;
        long numExpectedTasks = numSaltBuckets;
        insertRowsInTable(tableName, numRows);

        String query = "SELECT * FROM " + tableName;
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        PhoenixResultSet resultSetBeingTested = rs.unwrap(PhoenixResultSet.class);
        changeInternalStateForTesting(resultSetBeingTested);
        while (resultSetBeingTested.next()) {}
        resultSetBeingTested.close();
        Set<String> expectedTableNames = Sets.newHashSet(tableName);
        assertReadMetricValuesForSelectSql(Lists.newArrayList(numRows), Lists.newArrayList(numExpectedTasks),
                resultSetBeingTested, expectedTableNames);
    }

    @Test
    public void testMetricsForUpsert() throws Exception {
        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE " + tableName + " (K VARCHAR NOT NULL PRIMARY KEY, V VARCHAR)" + " SALT_BUCKETS = 6";
        Connection ddlConn = DriverManager.getConnection(getUrl());
        ddlConn.createStatement().execute(ddl);
        ddlConn.close();

        int numRows = 10;
        Connection conn = insertRowsInTable(tableName, numRows);
        PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);
        Map<String, Map<MetricType, Long>> mutationMetrics = PhoenixRuntime.getWriteMetricInfoForMutationsSinceLastReset(pConn);
        for (Entry<String, Map<MetricType, Long>> entry : mutationMetrics.entrySet()) {
            String t = entry.getKey();
            assertEquals("Table names didn't match!", tableName, t);
            Map<MetricType, Long> p = entry.getValue();
            assertEquals("There should have been sixteen metrics", 16, p.size());
            boolean mutationBatchSizePresent = false;
            boolean mutationCommitTimePresent = false;
            boolean mutationBytesPresent = false;
            boolean mutationBatchFailedPresent = false;
            for (Entry<MetricType, Long> metric : p.entrySet()) {
                MetricType metricType = metric.getKey();
                long metricValue = metric.getValue();
                if (metricType.equals(MetricType.MUTATION_BATCH_SIZE)) {
                    assertEquals("Mutation batch sizes didn't match!", numRows, metricValue);
                    mutationBatchSizePresent = true;
                } else if (metricType.equals(MetricType.MUTATION_COMMIT_TIME)) {
                    assertTrue("Mutation commit time should be greater than zero", metricValue > 0);
                    mutationCommitTimePresent = true;
                } else if (metricType.equals(MetricType.MUTATION_BYTES)) {
                    assertTrue("Mutation bytes size should be greater than zero", metricValue > 0);
                    mutationBytesPresent = true;
                } else if (metricType.equals(MetricType.MUTATION_BATCH_FAILED_SIZE)) {
                    assertEquals("Zero failed mutations expected", 0, metricValue);
                    mutationBatchFailedPresent = true;
                }
            }
            assertTrue(mutationBatchSizePresent);
            assertTrue(mutationCommitTimePresent);
            assertTrue(mutationBytesPresent);
            assertTrue(mutationBatchFailedPresent);
        }
        Map<String, Map<MetricType, Long>> readMetrics = PhoenixRuntime.getReadMetricInfoForMutationsSinceLastReset(pConn);
        assertEquals("Read metrics should be empty", 0, readMetrics.size());
    }

    @Test
    public void testMetricsForUpsertSelect() throws Exception {
        String tableName1 = generateUniqueName();
        long table1SaltBuckets = 6;
        String ddl = "CREATE TABLE " + tableName1 + " (K VARCHAR NOT NULL PRIMARY KEY, V VARCHAR)" + " SALT_BUCKETS = "
                + table1SaltBuckets;
        Connection ddlConn = DriverManager.getConnection(getUrl());
        ddlConn.createStatement().execute(ddl);
        ddlConn.close();
        int numRows = 10;
        insertRowsInTable(tableName1, numRows);

        String tableName2 = generateUniqueName();
        ddl = "CREATE TABLE " + tableName2 + " (K VARCHAR NOT NULL PRIMARY KEY, V VARCHAR)" + " SALT_BUCKETS = 10";
        ddlConn = DriverManager.getConnection(getUrl());
        ddlConn.createStatement().execute(ddl);
        ddlConn.close();

        Connection conn = DriverManager.getConnection(getUrl());
        String upsertSelect = "UPSERT INTO " + tableName2 + " SELECT * FROM " + tableName1;
        conn.createStatement().executeUpdate(upsertSelect);
        conn.commit();
        PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);

        Map<String, Map<MetricType, Long>> mutationMetrics = PhoenixRuntime.getWriteMetricInfoForMutationsSinceLastReset(pConn);
        assertMutationMetrics(tableName2, numRows, true, mutationMetrics);
        Map<String, Map<MetricType, Long>> readMetrics = PhoenixRuntime.getReadMetricInfoForMutationsSinceLastReset(pConn);
        assertReadMetricsForMutatingSql(tableName1, table1SaltBuckets, readMetrics);
    }

    @Test
    public void testMetricsForDelete() throws Exception {
        String tableName = generateUniqueName();
        long tableSaltBuckets = 6;
        String ddl = "CREATE TABLE " + tableName + " (K VARCHAR NOT NULL PRIMARY KEY, V VARCHAR)" + " SALT_BUCKETS = "
                + tableSaltBuckets;
        try(Connection ddlConn = DriverManager.getConnection(getUrl())) {
            ddlConn.createStatement().execute(ddl);
        }
        int numRows = 10;
        insertRowsInTable(tableName, numRows);
        try(Connection conn = DriverManager.getConnection(getUrl())) {
            String delete = "DELETE FROM " + tableName;
            conn.createStatement().execute(delete);
            conn.commit();
            Map<String, Map<MetricType, Long>> mutationMetrics = PhoenixRuntime.getWriteMetricInfoForMutationsSinceLastReset(conn);
            assertMutationMetrics(tableName, numRows, false, mutationMetrics);
            Map<String, Map<MetricType, Long>> readMetrics = PhoenixRuntime.getReadMetricInfoForMutationsSinceLastReset(conn);
            assertReadMetricsForMutatingSql(tableName, tableSaltBuckets, readMetrics);
        }
    }

    @Test
    public void testNoMetricsCollectedForConnection() throws Exception {
        String tableName = generateUniqueName();
        long tableSaltBuckets = 6;
        String ddl = "CREATE TABLE " + tableName + " (K VARCHAR NOT NULL PRIMARY KEY, V VARCHAR)" + " SALT_BUCKETS = "
                + tableSaltBuckets;
        Connection ddlConn = DriverManager.getConnection(getUrl());
        ddlConn.createStatement().execute(ddl);
        ddlConn.close();
        int numRows = 10;
        insertRowsInTable(tableName, numRows);
        Properties props = new Properties();
        props.setProperty(QueryServices.COLLECT_REQUEST_LEVEL_METRICS, "false");
        props.setProperty(QueryServices.LOG_LEVEL, LogLevel.OFF.name());
        Connection conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {}
        rs.close();
        Map<String, Map<MetricType, Long>> readMetrics = PhoenixRuntime.getRequestReadMetricInfo(rs);
        assertTrue("No read metrics should have been generated", readMetrics.size() == 0);
        conn.createStatement().executeUpdate("UPSERT INTO " + tableName + " VALUES ('KEY', 'VALUE')");
        conn.commit();
        Map<String, Map<MetricType, Long>> writeMetrics = PhoenixRuntime.getWriteMetricInfoForMutationsSinceLastReset(conn);
        assertTrue("No write metrics should have been generated", writeMetrics.size() == 0);
    }

    @Test
    public void testMetricsForUpsertWithAutoCommit() throws Exception {
        String tableName = generateUniqueName();
        long tableSaltBuckets = 6;
        String ddl = "CREATE TABLE " + tableName + " (K VARCHAR NOT NULL PRIMARY KEY, V VARCHAR)" + " SALT_BUCKETS = "
                + tableSaltBuckets;
        try (Connection ddlConn = DriverManager.getConnection(getUrl())) {
            ddlConn.createStatement().execute(ddl);
        }

        String upsert = "UPSERT INTO " + tableName + " VALUES (?, ?)";
        int numRows = 10;
        Map<String, Map<MetricType, Long>> mutationMetricsForAutoCommitOff = null;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(false);
            upsertRows(upsert, numRows, conn);
            conn.commit();
            mutationMetricsForAutoCommitOff = PhoenixRuntime.getWriteMetricInfoForMutationsSinceLastReset(conn);
        }

        // Insert rows now with auto-commit on
        Map<String, Map<MetricType, Long>> mutationMetricsAutoCommitOn = null;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(true);
            upsertRows(upsert, numRows, conn);
            mutationMetricsAutoCommitOn = PhoenixRuntime.getWriteMetricInfoForMutationsSinceLastReset(conn);
        }
        // Verify that the mutation metrics are same for both cases
        assertMetricsAreSame(mutationMetricsForAutoCommitOff, mutationMetricsAutoCommitOn, mutationMetricsToSkip);
    }

    private void upsertRows(String upsert, int numRows, Connection conn) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(upsert);
        for (int i = 1; i <= numRows; i++) {
            stmt.setString(1, "key" + i);
            stmt.setString(2, "value" + i);
            stmt.executeUpdate();
        }
    }

    @Test
    public void testMetricsForDeleteWithAutoCommit() throws Exception {
        String tableName = generateUniqueName();
        long tableSaltBuckets = 6;
        String ddl = "CREATE TABLE " + tableName + " (K VARCHAR NOT NULL PRIMARY KEY, V VARCHAR)" + " SALT_BUCKETS = "
                + tableSaltBuckets;
        try (Connection ddlConn = DriverManager.getConnection(getUrl())) {
            ddlConn.createStatement().execute(ddl);
        }

        String upsert = "UPSERT INTO " + tableName + " VALUES (?, ?)";
        int numRows = 10;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(false);
            upsertRows(upsert, numRows, conn);
            conn.commit();
        }

        String delete = "DELETE FROM " + tableName;
        // Delete rows now with auto-commit off
        Map<String, Map<MetricType, Long>> deleteMetricsWithAutoCommitOff = null;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(false);
            conn.createStatement().executeUpdate(delete);
            deleteMetricsWithAutoCommitOff = PhoenixRuntime.getWriteMetricInfoForMutationsSinceLastReset(conn);
        }

        // Upsert the rows back
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(false);
            upsertRows(upsert, numRows, conn);
            conn.commit();
        }

        // Now delete rows with auto-commit on
        Map<String, Map<MetricType, Long>> deleteMetricsWithAutoCommitOn = null;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate(delete);
            deleteMetricsWithAutoCommitOn = PhoenixRuntime.getWriteMetricInfoForMutationsSinceLastReset(conn);
        }

        // Verify that the mutation metrics are same for both cases.
        assertMetricsAreSame(deleteMetricsWithAutoCommitOff, deleteMetricsWithAutoCommitOn, mutationMetricsToSkip);
    }

    @Test
    public void testMetricsForUpsertSelectWithAutoCommit() throws Exception {
        String tableName1 = generateUniqueName();
        long table1SaltBuckets = 6;
        String ddl = "CREATE TABLE " + tableName1 + " (K BIGINT NOT NULL PRIMARY KEY ROW_TIMESTAMP, V VARCHAR)"
                + " SALT_BUCKETS = " + table1SaltBuckets + ", IMMUTABLE_ROWS = true";
        Connection ddlConn = DriverManager.getConnection(getUrl());
        ddlConn.createStatement().execute(ddl);
        ddlConn.close();
        int numRows = 10;
        String dml = "UPSERT INTO " + tableName1 + " VALUES (?, ?)";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            PreparedStatement stmt = conn.prepareStatement(dml);
            for (int i = 1; i <= numRows; i++) {
                stmt.setLong(1, i);
                stmt.setString(2, "value" + i);
                stmt.executeUpdate();
            }
            conn.commit();
        }

        String tableName2 = generateUniqueName();
        ddl = "CREATE TABLE " + tableName2 + " (K BIGINT NOT NULL PRIMARY KEY ROW_TIMESTAMP, V VARCHAR)"
                + " SALT_BUCKETS = 10" + ", IMMUTABLE_ROWS = true";
        ddlConn = DriverManager.getConnection(getUrl());
        ddlConn.createStatement().execute(ddl);
        String indexName = generateUniqueName();
        ddl = "CREATE INDEX " + indexName + " ON " + tableName2 + " (V)";
        ddlConn.createStatement().execute(ddl);
        ddlConn.close();

        String upsertSelect = "UPSERT INTO " + tableName2 + " SELECT * FROM " + tableName1;

        Map<String, Map<MetricType, Long>> mutationMetricsAutoCommitOff = null;
        Map<String, Map<MetricType, Long>> readMetricsAutoCommitOff = null;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(false);
            conn.createStatement().executeUpdate(upsertSelect);
            conn.commit();
            PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);
            mutationMetricsAutoCommitOff = PhoenixRuntime.getWriteMetricInfoForMutationsSinceLastReset(pConn);
            readMetricsAutoCommitOff = PhoenixRuntime.getReadMetricInfoForMutationsSinceLastReset(pConn);
        }

        Map<String, Map<MetricType, Long>> mutationMetricsAutoCommitOn = null;
        Map<String, Map<MetricType, Long>> readMetricsAutoCommitOn = null;

        int autoCommitBatchSize = numRows + 1; // batchsize = 11 is less than numRows and is not a divisor of batchsize
        Properties props = new Properties();
        props.setProperty(UPSERT_BATCH_SIZE_ATTRIB, Integer.toString(autoCommitBatchSize));
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate(upsertSelect);
            PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);
            mutationMetricsAutoCommitOn = PhoenixRuntime.getWriteMetricInfoForMutationsSinceLastReset(pConn);
            readMetricsAutoCommitOn = PhoenixRuntime.getReadMetricInfoForMutationsSinceLastReset(pConn);
        }
        assertMetricsAreSame(mutationMetricsAutoCommitOff, mutationMetricsAutoCommitOn, mutationMetricsToSkip);
        assertMetricsAreSame(readMetricsAutoCommitOff, readMetricsAutoCommitOn, readMetricsToSkip);

        autoCommitBatchSize = numRows - 1; // batchsize = 9 is less than numRows and is not a divisor of batchsize
        props = new Properties();
        props.setProperty(UPSERT_BATCH_SIZE_ATTRIB, Integer.toString(autoCommitBatchSize));
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate(upsertSelect);
            PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);
            mutationMetricsAutoCommitOn = PhoenixRuntime.getWriteMetricInfoForMutationsSinceLastReset(pConn);
            readMetricsAutoCommitOn = PhoenixRuntime.getReadMetricInfoForMutationsSinceLastReset(pConn);
        }
        assertMetricsAreSame(mutationMetricsAutoCommitOff, mutationMetricsAutoCommitOn, mutationMetricsToSkip);
        assertMetricsAreSame(readMetricsAutoCommitOff, readMetricsAutoCommitOn, readMetricsToSkip);

        autoCommitBatchSize = numRows;
        props = new Properties();
        props.setProperty(UPSERT_BATCH_SIZE_ATTRIB, Integer.toString(autoCommitBatchSize));
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate(upsertSelect);
            PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);
            mutationMetricsAutoCommitOn = PhoenixRuntime.getWriteMetricInfoForMutationsSinceLastReset(pConn);
            readMetricsAutoCommitOn = PhoenixRuntime.getReadMetricInfoForMutationsSinceLastReset(pConn);
        }
        assertMetricsAreSame(mutationMetricsAutoCommitOff, mutationMetricsAutoCommitOn, mutationMetricsToSkip);
        assertMetricsAreSame(readMetricsAutoCommitOff, readMetricsAutoCommitOn, readMetricsToSkip);

        autoCommitBatchSize = 2; // multiple batches of equal size
        props = new Properties();
        props.setProperty(UPSERT_BATCH_SIZE_ATTRIB, Integer.toString(autoCommitBatchSize));
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate(upsertSelect);
            PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);
            mutationMetricsAutoCommitOn = PhoenixRuntime.getWriteMetricInfoForMutationsSinceLastReset(pConn);
            readMetricsAutoCommitOn = PhoenixRuntime.getReadMetricInfoForMutationsSinceLastReset(pConn);
        }
        assertMetricsAreSame(mutationMetricsAutoCommitOff, mutationMetricsAutoCommitOn, mutationMetricsToSkip);
        assertMetricsAreSame(readMetricsAutoCommitOff, readMetricsAutoCommitOff, readMetricsToSkip);
    }

    @Test
    public void testMutationMetricsWhenUpsertingToMultipleTables() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String table1 = generateUniqueName();
            createTableAndInsertValues(table1, false, false, 10, true, conn, false);
            String table2 = generateUniqueName();
            createTableAndInsertValues(table2, false, false, 10, true, conn, false);
            String table3 = generateUniqueName();
            createTableAndInsertValues(table3, false, false, 10, true, conn, false);
            Map<String, Map<MetricType, Long>> mutationMetrics = PhoenixRuntime.getWriteMetricInfoForMutationsSinceLastReset(conn);
            assertTrue("Mutation metrics not present for " + table1, mutationMetrics.get(table1) != null);
            assertTrue("Mutation metrics not present for " + table2, mutationMetrics.get(table2) != null);
            assertTrue("Mutation metrics not present for " + table3, mutationMetrics.get(table3) != null);
            assertMetricsHaveSameValues(mutationMetrics.get(table1), mutationMetrics.get(table2), mutationMetricsToSkip);
            assertMetricsHaveSameValues(mutationMetrics.get(table1), mutationMetrics.get(table3), mutationMetricsToSkip);
        }
    }

    @Test
    public void createViewWithWhereConditionNoConnLeak() throws SQLException {
        resetGlobalMetrics();
        String tableName = generateUniqueName();
        String viewName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + tableName +
                    " (K INTEGER PRIMARY KEY, V VARCHAR(10))");
            conn.createStatement().execute("CREATE VIEW " + viewName +
                    " AS SELECT * FROM " + tableName + " WHERE K = 1");
        }
        assertTrue(PhoenixRuntime.areGlobalClientMetricsBeingCollected());
        assertEquals(0, GLOBAL_OPEN_PHOENIX_CONNECTIONS.getMetric().getValue());
    }

    @Test
    public void testClosingConnectionClearsMetrics() throws Exception {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(getUrl());
            createTableAndInsertValues(generateUniqueName(), false, false, 10, true, conn, false);
            assertTrue("Mutation metrics not present", PhoenixRuntime.getWriteMetricInfoForMutationsSinceLastReset(conn).size() > 0);
        } finally {
            if (conn != null) {
                conn.close();
                assertTrue("Closing connection didn't clear metrics",
                        PhoenixRuntime.getWriteMetricInfoForMutationsSinceLastReset(conn).size() == 0);
            }
        }
    }

    @Test
    public void testMetricsForUpsertingIntoImmutableTableWithIndices() throws Exception {
        String dataTable = generateUniqueName();
        String tableDdl = "CREATE TABLE "
                + dataTable
                + " (K1 VARCHAR NOT NULL, K2 VARCHAR NOT NULL, V1 INTEGER, V2 INTEGER, V3 INTEGER CONSTRAINT NAME_PK PRIMARY KEY(K1, K2)) IMMUTABLE_ROWS = true";
        String index1 = generateUniqueName() + "_IDX";
        String index1Ddl = "CREATE INDEX " + index1 + " ON " + dataTable + " (V1) include (V2)";
        String index2 = generateUniqueName() + "_IDX";
        String index2Ddl = "CREATE INDEX " + index2 + " ON " + dataTable + " (V2) include (V3)";
        String index3 = generateUniqueName() + "_IDX";
        String index3Ddl = "CREATE INDEX " + index3 + " ON " + dataTable + " (V3) include (V1)";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(tableDdl);
            conn.createStatement().execute(index1Ddl);
            conn.createStatement().execute(index2Ddl);
            conn.createStatement().execute(index3Ddl);
        }
        String upsert = "UPSERT INTO " + dataTable + " VALUES (?, ?, ?, ?, ?)";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            /*
             * Upsert data into table. Because the table is immutable, mutations for updating the indices on it are
             * handled by the client itself. So mutation metrics should include mutations for the indices as well as the
             * data table.
             */
            PreparedStatement stmt = conn.prepareStatement(upsert);
            for (int i = 1; i < 10; i++) {
                stmt.setString(1, "key1" + i);
                stmt.setString(2, "key2" + i);
                stmt.setInt(3, i);
                stmt.setInt(4, i);
                stmt.setInt(5, i);
                stmt.executeUpdate();
            }
            conn.commit();
            Map<String, Map<MetricType, Long>> metrics = PhoenixRuntime.getWriteMetricInfoForMutationsSinceLastReset(conn);
            assertTrue(metrics.get(dataTable).size() > 0);
            assertTrue(metrics.get(index1).size() > 0);
            assertTrue(metrics.get(index2).size() > 0);
            assertMetricsHaveSameValues(metrics.get(index1), metrics.get(index2), mutationMetricsToSkip);
            assertTrue(metrics.get(index3).size() > 0);
            assertMetricsHaveSameValues(metrics.get(index1), metrics.get(index3), mutationMetricsToSkip);
        }
    }

    @Test
    public void testOpenConnectionsCounter() throws Exception {
        long numOpenConnections = GLOBAL_OPEN_PHOENIX_CONNECTIONS.getMetric().getValue();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            assertEquals(numOpenConnections + 1, GLOBAL_OPEN_PHOENIX_CONNECTIONS.getMetric().getValue());
        }
        assertEquals(numOpenConnections, GLOBAL_OPEN_PHOENIX_CONNECTIONS.getMetric().getValue());
    }

    private void createTableAndInsertValues(boolean commit, int numRows, Connection conn, String tableName)
            throws SQLException {
        String ddl = "CREATE TABLE " + tableName + " (K VARCHAR NOT NULL PRIMARY KEY, V VARCHAR)";
        conn.createStatement().execute(ddl);
        // executing 10 upserts/mutations.
        String dml = "UPSERT INTO " + tableName + " VALUES (?, ?)";
        PreparedStatement stmt = conn.prepareStatement(dml);
        for (int i = 1; i <= numRows; i++) {
            stmt.setString(1, "key" + i);
            stmt.setString(2, "value" + i);
            stmt.executeUpdate();
        }
        if (commit) {
            conn.commit();
        }
    }

    private void assertMetricsAreSame(Map<String, Map<MetricType, Long>> metric1, Map<String, Map<MetricType, Long>> metric2,
            List<MetricType> metricsToSkip) {
        assertTrue("The two metrics have different or unequal number of table names ",
                metric1.keySet().equals(metric2.keySet()));
        for (Entry<String, Map<MetricType, Long>> entry : metric1.entrySet()) {
            Map<MetricType, Long> metricNameValueMap1 = entry.getValue();
            Map<MetricType, Long> metricNameValueMap2 = metric2.get(entry.getKey());
            assertMetricsHaveSameValues(metricNameValueMap1, metricNameValueMap2, metricsToSkip);
        }
    }

    private void assertMetricsHaveSameValues(Map<MetricType, Long> metricNameValueMap1,
            Map<MetricType, Long> metricNameValueMap2, List<MetricType> metricsToSkip) {
        assertTrue("The two metrics have different or unequal number of metric names ", metricNameValueMap1.keySet()
                .equals(metricNameValueMap2.keySet()));
        for (Entry<MetricType, Long> entry : metricNameValueMap1.entrySet()) {
            MetricType metricType = entry.getKey();
            if (!metricsToSkip.contains(metricType)) {
                assertEquals("Unequal values for metric " + metricType, entry.getValue(),
                        metricNameValueMap2.get(metricType));
            }
        }
    }

    private void changeInternalStateForTesting(PhoenixResultSet rs) throws NoSuchFieldException,
            SecurityException, IllegalArgumentException, IllegalAccessException {
        // get and set the internal state for testing purposes.
        // TODO use a spy ?
        ReadMetricQueue testMetricsQueue = new TestReadMetricsQueue(LogLevel.OFF,true);

        Field rsQueueField = PhoenixResultSet.class.getDeclaredField("readMetricsQueue");
        rsQueueField.setAccessible(true);
        rsQueueField.set(rs, testMetricsQueue);

        StatementContext ctx = rs.getContext();
        Field ctxQueueField = StatementContext.class.getDeclaredField("readMetricsQueue");
        ctxQueueField.setAccessible(true);
        // Default realm for MiniKDC
        ctxQueueField.set(ctx, testMetricsQueue);
    }

    private void assertReadMetricValuesForSelectSql(ArrayList<Long> numRows, ArrayList<Long> numExpectedTasks,
            PhoenixResultSet resultSetBeingTested, Set<String> expectedTableNames) throws SQLException {
        Map<String, Map<MetricType, Long>> metrics = PhoenixRuntime.getRequestReadMetricInfo(resultSetBeingTested);
        int counter = 0;
        for (Entry<String, Map<MetricType, Long>> entry : metrics.entrySet()) {
            String tableName = entry.getKey();
            expectedTableNames.remove(tableName);
            Map<MetricType, Long> metricValues = entry.getValue();
            boolean taskCounterMetricsPresent = false;
            boolean taskExecutionTimeMetricsPresent = false;
            boolean memoryMetricsPresent = false;
            for (Entry<MetricType, Long> pair : metricValues.entrySet()) {
                MetricType metricType = pair.getKey();
                long metricValue = pair.getValue();
                long numTask = numExpectedTasks.get(counter);
                if (metricType.equals(TASK_EXECUTED_COUNTER)) {
                    assertEquals(numTask, metricValue);
                    taskCounterMetricsPresent = true;
                } else if (metricType.equals(TASK_EXECUTION_TIME)) {
                    assertEquals(numTask * TASK_EXECUTION_TIME_DELTA, metricValue);
                    taskExecutionTimeMetricsPresent = true;
                } else if (metricType.equals(MEMORY_CHUNK_BYTES)) {
                    assertEquals(numTask * MEMORY_CHUNK_BYTES_DELTA, metricValue);
                    memoryMetricsPresent = true;
                }
            }
            counter++;
            assertTrue(taskCounterMetricsPresent);
            assertTrue(taskExecutionTimeMetricsPresent);
            assertTrue(memoryMetricsPresent);
        }
        PhoenixRuntime.resetMetrics(resultSetBeingTested);
        assertTrue("Metrics not found tables " + Joiner.on(",").join(expectedTableNames),
                expectedTableNames.size() == 0);
    }

    // number of records read should be number of bytes at the end
    public static final long SCAN_BYTES_DELTA = 1;

    // total task execution time should be numTasks * TASK_EXECUTION_TIME_DELTA
    public static final long TASK_EXECUTION_TIME_DELTA = 10;

    // total task execution time should be numTasks * TASK_EXECUTION_TIME_DELTA
    public static final long MEMORY_CHUNK_BYTES_DELTA = 100;

    private class TestReadMetricsQueue extends ReadMetricQueue {

        public TestReadMetricsQueue(LogLevel connectionLogLevel, boolean isRequestMetricsEnabled) {
            super(isRequestMetricsEnabled, connectionLogLevel);
        }

        @Override
        public CombinableMetric getMetric(MetricType type) {
            switch (type) {
            case SCAN_BYTES:
                return new CombinableMetricImpl(type) {

                    @Override
                    public void change(long delta) {
                        super.change(SCAN_BYTES_DELTA);
                    }
                };
            case TASK_EXECUTION_TIME:
                return new CombinableMetricImpl(type) {

                    @Override
                    public void change(long delta) {
                        super.change(TASK_EXECUTION_TIME_DELTA);
                    }
                };
            case MEMORY_CHUNK_BYTES:
                return new CombinableMetricImpl(type) {

                    @Override
                    public void change(long delta) {
                        super.change(MEMORY_CHUNK_BYTES_DELTA);
                    }
                };
            }
            return super.getMetric(type);
        }
    }

    @Test
    public void testGetConnectionsForSameUrlConcurrently()  throws Exception {
        // establish url and quorum. Need to use PhoenixDriver and not PhoenixTestDriver
        String zkQuorum = "localhost:" + getUtility().getZkCluster().getClientPort();
        String url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;
        ExecutorService exec = Executors.newFixedThreadPool(10);
        try {
            GLOBAL_HCONNECTIONS_COUNTER.getMetric().reset();
            GLOBAL_QUERY_SERVICES_COUNTER.getMetric().reset();
            assertEquals(0, GLOBAL_HCONNECTIONS_COUNTER.getMetric().getValue());
            assertEquals(0, GLOBAL_QUERY_SERVICES_COUNTER.getMetric().getValue());
            List<Callable<Connection>> callables = new ArrayList<>(100);
            List<Future<Connection>> futures = new ArrayList<>(100);
            int expectedHConnections = numConnections.get() > 0 ? 0 : 1;
            for (int i = 1; i <= 100; i++) {
                Callable<Connection> c = new GetConnectionCallable(url);
                callables.add(c);
                futures.add(exec.submit(c));
            }
            for (int i = 0; i < futures.size(); i++) {
                Connection c = futures.get(i).get();
                try {
                    c.close();
                } catch (Exception ignore) {}
            }
            assertEquals(expectedHConnections, GLOBAL_HCONNECTIONS_COUNTER.getMetric().getValue());
            assertEquals(expectedHConnections, GLOBAL_QUERY_SERVICES_COUNTER.getMetric().getValue());
        } finally {
            exec.shutdownNow();
        }
    }

    @Test
    public void testGetConnectionsThrottledForSameUrl() throws Exception {
        int attemptedPhoenixConnections = 11;
        int maxConnections = attemptedPhoenixConnections -1;
        List<Connection> connections = Lists.newArrayList();
        String zkQuorum = "localhost:" + getUtility().getZkCluster().getClientPort();
        String url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum +
                ':' +  CUSTOM_URL_STRING + '=' + "throttletest";

        Properties props = new Properties();
        props.setProperty(QueryServices.CLIENT_CONNECTION_MAX_ALLOWED_CONNECTIONS, Integer.toString(maxConnections));

        GLOBAL_HCONNECTIONS_COUNTER.getMetric().reset();
        GLOBAL_QUERY_SERVICES_COUNTER.getMetric().reset();
        GLOBAL_PHOENIX_CONNECTIONS_ATTEMPTED_COUNTER.getMetric().reset();
        GLOBAL_PHOENIX_CONNECTIONS_THROTTLED_COUNTER.getMetric().reset();
        boolean wasThrottled = false;
        try {
            for (int k = 0; k < attemptedPhoenixConnections; k++) {
                connections.add(DriverManager.getConnection(url, props));
            }
        } catch (SQLException se) {
            wasThrottled = true;
            assertEquals(SQLExceptionCode.NEW_CONNECTION_THROTTLED.getErrorCode(), se.getErrorCode());
        } finally {
            for (Connection c : connections) {
                c.close();
            }
        }
        assertEquals(1, GLOBAL_QUERY_SERVICES_COUNTER.getMetric().getValue());
        assertTrue("No connection was throttled!", wasThrottled);
        assertEquals(1, GLOBAL_PHOENIX_CONNECTIONS_THROTTLED_COUNTER.getMetric().getValue());
        assertEquals(maxConnections, connections.size());
        assertTrue("Not all connections were attempted!",
                attemptedPhoenixConnections <= GLOBAL_PHOENIX_CONNECTIONS_ATTEMPTED_COUNTER.getMetric().getValue());
        connections.clear();
        //now check that we decremented the counter for the connections we just released
        try {
            for (int k = 0; k < maxConnections; k++){
                connections.add(DriverManager.getConnection(url, props));
            }
        } catch(SQLException se) {
            if (se.getErrorCode() == (SQLExceptionCode.NEW_CONNECTION_THROTTLED).getErrorCode()){
                fail("Connection was throttled when it shouldn't be!");
            }
        } finally {
            for (Connection c : connections) {
                c.close();
            }
        }
        assertEquals(maxConnections, connections.size());
    }

    @Test
    public void testGetConnectionsFailedCounter() throws Exception {
        int attemptedPhoenixConnections = 7;
        //3 Failed connections and 1 throttled connection
        int maxConnections = attemptedPhoenixConnections - 4;
        List<Connection> connections = Lists.newArrayList();
        String zkQuorum = "localhost:" + getUtility().getZkCluster().getClientPort();
        String url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum +
                ':' +  CUSTOM_URL_STRING + '=' + "FailedCounterTest";
        Properties props = new Properties();
        props.setProperty(QueryServices.CLIENT_CONNECTION_MAX_ALLOWED_CONNECTIONS, Integer.toString(maxConnections));
        Properties props1 = new Properties(props);
        props1.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Integer.toString(-1));
        Properties props2 = new Properties(props);
        //Will create IllegalArgumentException while parsing loglevel
        props2.setProperty(QueryServices.LOG_LEVEL, "notKnown");

        GLOBAL_QUERY_SERVICES_COUNTER.getMetric().reset();
        GLOBAL_PHOENIX_CONNECTIONS_ATTEMPTED_COUNTER.getMetric().reset();
        GLOBAL_PHOENIX_CONNECTIONS_THROTTLED_COUNTER.getMetric().reset();
        GLOBAL_FAILED_PHOENIX_CONNECTIONS.getMetric().reset();
        try {
            for (int i = 0; i < attemptedPhoenixConnections; i++) {
                try {
                    if (i == 0) {
                        connections.add(DriverManager.getConnection(url, props2));
                    } else if (i % 3 == 0) {
                        connections.add(DriverManager.getConnection(url, props1));
                    } else {
                        connections.add(DriverManager.getConnection(url, props));
                    }
                } catch (SQLException se) {
                    if (i == 0) {
                        assertEquals(0, se.getErrorCode());
                    } else if (i % 3 == 0) {
                        assertEquals(SQLExceptionCode.INVALID_SCN.getErrorCode(), se.getErrorCode());
                    } else {
                        assertEquals(SQLExceptionCode.NEW_CONNECTION_THROTTLED.getErrorCode(), se.getErrorCode());
                    }
                }
            }
        } finally {
            for (Connection c : connections) {
                c.close();
            }
        }
        assertEquals(1, GLOBAL_QUERY_SERVICES_COUNTER.getMetric().getValue());
        assertEquals(1, GLOBAL_PHOENIX_CONNECTIONS_THROTTLED_COUNTER.getMetric().getValue());
        assertEquals(3, GLOBAL_FAILED_PHOENIX_CONNECTIONS.getMetric().getValue());
        //While initializing CQS we are creating one more metaConnection increasing Attempted_Counter by +1 during 1st conn.
        assertTrue("Not all connections were attempted!",
                attemptedPhoenixConnections <= GLOBAL_PHOENIX_CONNECTIONS_ATTEMPTED_COUNTER.getMetric().getValue());


        //Test case for bad url
        props1.setProperty(ZK_SESSION_TIMEOUT, Integer.toString(100));
        props1.setProperty(HBASE_CLIENT_RETRIES_NUMBER, Integer.toString(2));
        props1.setProperty("zookeeper.recovery.retry", Integer.toString(2));
        try {
            DriverManager.getConnection(PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + "jdbcthisIsBadZk", props1);
        } catch (Exception e) {
            assertEquals(4, GLOBAL_FAILED_PHOENIX_CONNECTIONS.getMetric().getValue());
            assertEquals(attemptedPhoenixConnections + 1, GLOBAL_PHOENIX_CONNECTIONS_ATTEMPTED_COUNTER.getMetric().getValue());
            return;
        }
        fail();
    }

    @Test
    public void testGetConnectionsForDifferentTenantsConcurrently()  throws Exception {
        // establish url and quorum. Need to use PhoenixDriver and not PhoenixTestDriver
        String zkQuorum = "localhost:" + getUtility().getZkCluster().getClientPort();
        String url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;
        ExecutorService exec = Executors.newFixedThreadPool(10);
        try {
            GLOBAL_HCONNECTIONS_COUNTER.getMetric().reset();
            GLOBAL_QUERY_SERVICES_COUNTER.getMetric().reset();
            assertEquals(0, GLOBAL_HCONNECTIONS_COUNTER.getMetric().getValue());
            assertEquals(0, GLOBAL_QUERY_SERVICES_COUNTER.getMetric().getValue());
            int expectedHConnections = numConnections.get() > 0 ? 0 : 1;
            List<Callable<Connection>> callables = new ArrayList<>(100);
            List<Future<Connection>> futures = new ArrayList<>(100);
            for (int i = 1; i <= 100; i++) {
                String tenantUrl = url + ';' + TENANT_ID_ATTRIB + '=' + i;
                Callable<Connection> c = new GetConnectionCallable(tenantUrl + ";");
                callables.add(c);
                futures.add(exec.submit(c));
            }
            for (int i = 0; i < futures.size(); i++) {
                Connection c = futures.get(i).get();
                try {
                    c.close();
                } catch (Exception ignore) {}
            }
            assertEquals(expectedHConnections, GLOBAL_HCONNECTIONS_COUNTER.getMetric().getValue());
            assertEquals(expectedHConnections, GLOBAL_QUERY_SERVICES_COUNTER.getMetric().getValue());
        } finally {
            exec.shutdownNow();
        }
    }

    @Test
    public void testGetConnectionsWithDifferentJDBCParamsConcurrently()  throws Exception {
        DriverManager.registerDriver(PhoenixDriver.INSTANCE);
        ExecutorService exec = Executors.newFixedThreadPool(4);
        // establish url and quorum. Need to use PhoenixDriver and not PhoenixTestDriver
        String zkQuorum = "localhost:" + getUtility().getZkCluster().getClientPort();
        String baseUrl = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;
        int numConnections = 20;
        List<Callable<Connection>> callables = new ArrayList<>(numConnections);
        List<Future<Connection>> futures = new ArrayList<>(numConnections);
        try {
            GLOBAL_HCONNECTIONS_COUNTER.getMetric().reset();
            GLOBAL_QUERY_SERVICES_COUNTER.getMetric().reset();
            assertEquals(0, GLOBAL_HCONNECTIONS_COUNTER.getMetric().getValue());
            assertEquals(0, GLOBAL_QUERY_SERVICES_COUNTER.getMetric().getValue());
            for (int i = 1; i <= numConnections; i++) {
                String customUrl = baseUrl + ':' +  CUSTOM_URL_STRING + '=' + i;
                Callable<Connection> c = new GetConnectionCallable(customUrl + ";");
                callables.add(c);
                futures.add(exec.submit(c));
            }
            for (int i = 0; i < futures.size(); i++) {
                futures.get(i).get();
            }
            assertEquals(numConnections, GLOBAL_HCONNECTIONS_COUNTER.getMetric().getValue());
            assertEquals(numConnections, GLOBAL_QUERY_SERVICES_COUNTER.getMetric().getValue());
        } finally {
            exec.shutdownNow();
            for (int i = 0; i < futures.size(); i++) {
                try {
                    Connection c = futures.get(i).get();
                    // close the query services instance because we created a lot of HConnections.
                    c.unwrap(PhoenixConnection.class).getQueryServices().close();
                    c.close();
                } catch (Exception ignore) {}
            }
        }
    }

    private static class GetConnectionCallable implements Callable<Connection> {
        private final String url;
        GetConnectionCallable(String url) {
            this.url = url;
        }
        @Override
        public Connection call() throws Exception {
            Connection c = DriverManager.getConnection(url);
            if (!url.contains(CUSTOM_URL_STRING)) {
                // check to detect whether a connection was established using the PhoenixDriver
                // This is used in our tests to figure out whether a new hconnection and query
                // services will be created.
                numConnections.incrementAndGet();
            }
            return c;
        }
    }


}
