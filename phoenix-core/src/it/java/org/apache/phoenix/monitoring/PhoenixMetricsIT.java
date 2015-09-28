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

import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_FAILED_QUERY_COUNTER;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_MUTATION_BATCH_SIZE;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_MUTATION_BYTES;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_MUTATION_COMMIT_TIME;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_MUTATION_SQL_COUNTER;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_NUM_PARALLEL_SCANS;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_QUERY_TIME;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_QUERY_TIMEOUT_COUNTER;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_REJECTED_TASK_COUNTER;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_SCAN_BYTES;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_SELECT_SQL_COUNTER;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_SPOOL_FILE_COUNTER;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_TASK_END_TO_END_TIME;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_TASK_EXECUTION_TIME;
import static org.apache.phoenix.monitoring.MetricType.MEMORY_CHUNK_BYTES;
import static org.apache.phoenix.monitoring.MetricType.SCAN_BYTES;
import static org.apache.phoenix.monitoring.MetricType.TASK_EXECUTED_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.TASK_EXECUTION_TIME;
import static org.apache.phoenix.util.PhoenixRuntime.UPSERT_BATCH_SIZE_ATTRIB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.end2end.BaseOwnClusterHBaseManagedTimeIT;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class PhoenixMetricsIT extends BaseOwnClusterHBaseManagedTimeIT {

    private static final List<String> mutationMetricsToSkip = Lists
            .newArrayList(MetricType.MUTATION_COMMIT_TIME.name());
    private static final List<String> readMetricsToSkip = Lists.newArrayList(MetricType.TASK_QUEUE_WAIT_TIME.name(),
            MetricType.TASK_EXECUTION_TIME.name(), MetricType.TASK_END_TO_END_TIME.name());

    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        // Enable request metric collection at the driver level
        props.put(QueryServices.COLLECT_REQUEST_LEVEL_METRICS, String.valueOf(true));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Test
    public void testResetGlobalPhoenixMetrics() {
        resetGlobalMetrics();
        for (GlobalMetric m : PhoenixRuntime.getGlobalPhoenixClientMetrics()) {
            assertEquals(0, m.getTotalSum());
            assertEquals(0, m.getNumberOfSamples());
        }
    }

    @Test
    public void testGlobalPhoenixMetricsForQueries() throws Exception {
        createTableAndInsertValues("T", true);
        resetGlobalMetrics(); // we want to count metrics related only to the below query
        Connection conn = DriverManager.getConnection(getUrl());
        String query = "SELECT * FROM T";
        ResultSet rs = conn.createStatement().executeQuery(query);
        while (rs.next()) {
            rs.getString(1);
            rs.getString(2);
        }
        assertEquals(1, GLOBAL_NUM_PARALLEL_SCANS.getMetric().getTotalSum());
        assertEquals(1, GLOBAL_SELECT_SQL_COUNTER.getMetric().getTotalSum());
        assertEquals(0, GLOBAL_REJECTED_TASK_COUNTER.getMetric().getTotalSum());
        assertEquals(0, GLOBAL_QUERY_TIMEOUT_COUNTER.getMetric().getTotalSum());
        assertEquals(0, GLOBAL_FAILED_QUERY_COUNTER.getMetric().getTotalSum());
        assertEquals(0, GLOBAL_SPOOL_FILE_COUNTER.getMetric().getTotalSum());
        assertEquals(0, GLOBAL_MUTATION_BATCH_SIZE.getMetric().getTotalSum());
        assertEquals(0, GLOBAL_MUTATION_BYTES.getMetric().getTotalSum());
        assertEquals(0, GLOBAL_MUTATION_COMMIT_TIME.getMetric().getTotalSum());

        assertTrue(GLOBAL_SCAN_BYTES.getMetric().getTotalSum() > 0);
        assertTrue(GLOBAL_QUERY_TIME.getMetric().getTotalSum() > 0);
        assertTrue(GLOBAL_TASK_END_TO_END_TIME.getMetric().getTotalSum() > 0);
        assertTrue(GLOBAL_TASK_EXECUTION_TIME.getMetric().getTotalSum() > 0);
    }

    @Test
    public void testGlobalPhoenixMetricsForMutations() throws Exception {
        createTableAndInsertValues("T", true);
        assertEquals(10, GLOBAL_MUTATION_BATCH_SIZE.getMetric().getTotalSum());
        assertEquals(10, GLOBAL_MUTATION_SQL_COUNTER.getMetric().getTotalSum());
        assertTrue(GLOBAL_MUTATION_BYTES.getMetric().getTotalSum() > 0);
        assertTrue(GLOBAL_MUTATION_COMMIT_TIME.getMetric().getTotalSum() > 0);
        assertEquals(0, GLOBAL_NUM_PARALLEL_SCANS.getMetric().getTotalSum());
        assertEquals(0, GLOBAL_SELECT_SQL_COUNTER.getMetric().getTotalSum());
        assertEquals(0, GLOBAL_REJECTED_TASK_COUNTER.getMetric().getTotalSum());
        assertEquals(0, GLOBAL_QUERY_TIMEOUT_COUNTER.getMetric().getTotalSum());
        assertEquals(0, GLOBAL_FAILED_QUERY_COUNTER.getMetric().getTotalSum());
        assertEquals(0, GLOBAL_SPOOL_FILE_COUNTER.getMetric().getTotalSum());
    }

    @Test
    public void testGlobalPhoenixMetricsForUpsertSelect() throws Exception {
        createTableAndInsertValues("T", true);
        resetGlobalMetrics();
        String ddl = "CREATE TABLE T2 (K VARCHAR NOT NULL PRIMARY KEY, V VARCHAR)";
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(ddl);
        resetGlobalMetrics();
        String dml = "UPSERT INTO T2 (K, V) SELECT K, V FROM T";
        conn.createStatement().executeUpdate(dml);
        conn.commit();
        assertEquals(10, GLOBAL_MUTATION_BATCH_SIZE.getMetric().getTotalSum());
        assertEquals(1, GLOBAL_MUTATION_SQL_COUNTER.getMetric().getTotalSum());
        assertEquals(1, GLOBAL_NUM_PARALLEL_SCANS.getMetric().getTotalSum());
        assertEquals(0, GLOBAL_QUERY_TIME.getMetric().getTotalSum());
        assertTrue(GLOBAL_SCAN_BYTES.getMetric().getTotalSum() > 0);
        assertTrue(GLOBAL_MUTATION_BYTES.getMetric().getTotalSum() > 0);
        assertTrue(GLOBAL_MUTATION_COMMIT_TIME.getMetric().getTotalSum() > 0);
        assertEquals(0, GLOBAL_SELECT_SQL_COUNTER.getMetric().getTotalSum());
        assertEquals(0, GLOBAL_REJECTED_TASK_COUNTER.getMetric().getTotalSum());
        assertEquals(0, GLOBAL_QUERY_TIMEOUT_COUNTER.getMetric().getTotalSum());
        assertEquals(0, GLOBAL_FAILED_QUERY_COUNTER.getMetric().getTotalSum());
        assertEquals(0, GLOBAL_SPOOL_FILE_COUNTER.getMetric().getTotalSum());
    }

    private static void resetGlobalMetrics() {
        for (GlobalMetric m : PhoenixRuntime.getGlobalPhoenixClientMetrics()) {
            m.reset();
        }
    }

    private static void createTableAndInsertValues(String tableName, boolean resetGlobalMetricsAfterTableCreate)
            throws Exception {
        String ddl = "CREATE TABLE " + tableName + " (K VARCHAR NOT NULL PRIMARY KEY, V VARCHAR)";
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(ddl);
        if (resetGlobalMetricsAfterTableCreate) {
            resetGlobalMetrics();
        }
        // executing 10 upserts/mutations.
        String dml = "UPSERT INTO " + tableName + " VALUES (?, ?)";
        PreparedStatement stmt = conn.prepareStatement(dml);
        for (int i = 1; i <= 10; i++) {
            stmt.setString(1, "key" + i);
            stmt.setString(2, "value" + i);
            stmt.executeUpdate();
        }
        conn.commit();
    }

    @Test
    public void testOverallQueryMetricsForSelect() throws Exception {
        String tableName = "SCANMETRICS";
        String ddl = "CREATE TABLE " + tableName + " (K VARCHAR NOT NULL PRIMARY KEY, V VARCHAR)" + " SALT_BUCKETS = 6";
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(ddl);
    }

    @Test
    public void testReadMetricsForSelect() throws Exception {
        String tableName = "READMETRICSFORSELECT";
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
        String tableName = "UPSERTMETRICS";
        String ddl = "CREATE TABLE " + tableName + " (K VARCHAR NOT NULL PRIMARY KEY, V VARCHAR)" + " SALT_BUCKETS = 6";
        Connection ddlConn = DriverManager.getConnection(getUrl());
        ddlConn.createStatement().execute(ddl);
        ddlConn.close();

        int numRows = 10;
        Connection conn = insertRowsInTable(tableName, numRows);
        PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);
        Map<String, Map<String, Long>> mutationMetrics = PhoenixRuntime.getWriteMetricsForMutationsSinceLastReset(pConn);
        for (Entry<String, Map<String, Long>> entry : mutationMetrics.entrySet()) {
            String t = entry.getKey();
            assertEquals("Table names didn't match!", tableName, t);
            Map<String, Long> p = entry.getValue();
            assertEquals("There should have been three metrics", 3, p.size());
            boolean mutationBatchSizePresent = false;
            boolean mutationCommitTimePresent = false;
            boolean mutationBytesPresent = false;
            for (Entry<String, Long> metric : p.entrySet()) {
                String metricName = metric.getKey();
                long metricValue = metric.getValue();
                if (metricName.equals(MetricType.MUTATION_BATCH_SIZE.name())) {
                    assertEquals("Mutation batch sizes didn't match!", numRows, metricValue);
                    mutationBatchSizePresent = true;
                } else if (metricName.equals(MetricType.MUTATION_COMMIT_TIME.name())) {
                    assertTrue("Mutation commit time should be greater than zero", metricValue > 0);
                    mutationCommitTimePresent = true;
                } else if (metricName.equals(MetricType.MUTATION_BYTES.name())) {
                    assertTrue("Mutation bytes size should be greater than zero", metricValue > 0);
                    mutationBytesPresent = true;
                }
            }
            assertTrue(mutationBatchSizePresent);
            assertTrue(mutationCommitTimePresent);
            assertTrue(mutationBytesPresent);
        }
        Map<String, Map<String, Long>> readMetrics = PhoenixRuntime.getReadMetricsForMutationsSinceLastReset(pConn);
        assertEquals("Read metrics should be empty", 0, readMetrics.size());
    }

    @Test
    public void testMetricsForUpsertSelect() throws Exception {
        String tableName1 = "UPSERTFROM";
        long table1SaltBuckets = 6;
        String ddl = "CREATE TABLE " + tableName1 + " (K VARCHAR NOT NULL PRIMARY KEY, V VARCHAR)" + " SALT_BUCKETS = "
                + table1SaltBuckets;
        Connection ddlConn = DriverManager.getConnection(getUrl());
        ddlConn.createStatement().execute(ddl);
        ddlConn.close();
        int numRows = 10;
        insertRowsInTable(tableName1, numRows);

        String tableName2 = "UPSERTTO";
        ddl = "CREATE TABLE " + tableName2 + " (K VARCHAR NOT NULL PRIMARY KEY, V VARCHAR)" + " SALT_BUCKETS = 10";
        ddlConn = DriverManager.getConnection(getUrl());
        ddlConn.createStatement().execute(ddl);
        ddlConn.close();

        Connection conn = DriverManager.getConnection(getUrl());
        String upsertSelect = "UPSERT INTO " + tableName2 + " SELECT * FROM " + tableName1;
        conn.createStatement().executeUpdate(upsertSelect);
        conn.commit();
        PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);

        Map<String, Map<String, Long>> mutationMetrics = PhoenixRuntime.getWriteMetricsForMutationsSinceLastReset(pConn);
        assertMutationMetrics(tableName2, numRows, mutationMetrics);
        Map<String, Map<String, Long>> readMetrics = PhoenixRuntime.getReadMetricsForMutationsSinceLastReset(pConn);
        assertReadMetricsForMutatingSql(tableName1, table1SaltBuckets, readMetrics);
    }

    @Test
    public void testMetricsForDelete() throws Exception {
        String tableName = "DELETEMETRICS";
        long tableSaltBuckets = 6;
        String ddl = "CREATE TABLE " + tableName + " (K VARCHAR NOT NULL PRIMARY KEY, V VARCHAR)" + " SALT_BUCKETS = "
                + tableSaltBuckets;
        Connection ddlConn = DriverManager.getConnection(getUrl());
        ddlConn.createStatement().execute(ddl);
        ddlConn.close();
        int numRows = 10;
        insertRowsInTable(tableName, numRows);
        Connection conn = DriverManager.getConnection(getUrl());
        String delete = "DELETE FROM " + tableName;
        conn.createStatement().execute(delete);
        conn.commit();
        PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);
        Map<String, Map<String, Long>> mutationMetrics = PhoenixRuntime.getWriteMetricsForMutationsSinceLastReset(pConn);
        assertMutationMetrics(tableName, numRows, mutationMetrics);

        Map<String, Map<String, Long>> readMetrics = PhoenixRuntime.getReadMetricsForMutationsSinceLastReset(pConn);
        assertReadMetricsForMutatingSql(tableName, tableSaltBuckets, readMetrics);
    }

    @Test
    public void testNoMetricsCollectedForConnection() throws Exception {
        String tableName = "NOMETRICS";
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
        Connection conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName);
        while (rs.next()) {}
        rs.close();
        Map<String, Map<String, Long>> readMetrics = PhoenixRuntime.getRequestReadMetrics(rs);
        assertTrue("No read metrics should have been generated", readMetrics.size() == 0);
        conn.createStatement().executeUpdate("UPSERT INTO " + tableName + " VALUES ('KEY', 'VALUE')");
        conn.commit();
        Map<String, Map<String, Long>> writeMetrics = PhoenixRuntime.getWriteMetricsForMutationsSinceLastReset(conn);
        assertTrue("No write metrics should have been generated", writeMetrics.size() == 0);
    }

    @Test
    public void testMetricsForUpsertWithAutoCommit() throws Exception {
        String tableName = "VERIFYUPSERTAUTOCOMMIT";
        long tableSaltBuckets = 6;
        String ddl = "CREATE TABLE " + tableName + " (K VARCHAR NOT NULL PRIMARY KEY, V VARCHAR)" + " SALT_BUCKETS = "
                + tableSaltBuckets;
        try (Connection ddlConn = DriverManager.getConnection(getUrl())) {
            ddlConn.createStatement().execute(ddl);
        }

        String upsert = "UPSERT INTO " + tableName + " VALUES (?, ?)";
        int numRows = 10;
        Map<String, Map<String, Long>> mutationMetricsForAutoCommitOff = null;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(false);
            upsertRows(upsert, numRows, conn);
            conn.commit();
            mutationMetricsForAutoCommitOff = PhoenixRuntime.getWriteMetricsForMutationsSinceLastReset(conn);
        }

        // Insert rows now with auto-commit on
        Map<String, Map<String, Long>> mutationMetricsAutoCommitOn = null;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(true);
            upsertRows(upsert, numRows, conn);
            mutationMetricsAutoCommitOn = PhoenixRuntime.getWriteMetricsForMutationsSinceLastReset(conn);
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
        String tableName = "VERIFYDELETEAUTOCOMMIT";
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
        Map<String, Map<String, Long>> deleteMetricsWithAutoCommitOff = null;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(false);
            conn.createStatement().executeUpdate(delete);
            deleteMetricsWithAutoCommitOff = PhoenixRuntime.getWriteMetricsForMutationsSinceLastReset(conn);
        }

        // Upsert the rows back
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(false);
            upsertRows(upsert, numRows, conn);
            conn.commit();
        }

        // Now delete rows with auto-commit on
        Map<String, Map<String, Long>> deleteMetricsWithAutoCommitOn = null;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate(delete);
            deleteMetricsWithAutoCommitOn = PhoenixRuntime.getWriteMetricsForMutationsSinceLastReset(conn);
        }

        // Verify that the mutation metrics are same for both cases.
        assertMetricsAreSame(deleteMetricsWithAutoCommitOff, deleteMetricsWithAutoCommitOn, mutationMetricsToSkip);
    }

    @Test
    public void testMetricsForUpsertSelectWithAutoCommit() throws Exception {
        String tableName1 = "UPSERTFROMAUTOCOMMIT";
        long table1SaltBuckets = 6;
        String ddl = "CREATE TABLE " + tableName1 + " (K VARCHAR NOT NULL PRIMARY KEY, V VARCHAR)" + " SALT_BUCKETS = "
                + table1SaltBuckets;
        Connection ddlConn = DriverManager.getConnection(getUrl());
        ddlConn.createStatement().execute(ddl);
        ddlConn.close();
        int numRows = 10;
        insertRowsInTable(tableName1, numRows);

        String tableName2 = "UPSERTTOAUTCOMMIT";
        ddl = "CREATE TABLE " + tableName2 + " (K VARCHAR NOT NULL PRIMARY KEY, V VARCHAR)" + " SALT_BUCKETS = 10";
        ddlConn = DriverManager.getConnection(getUrl());
        ddlConn.createStatement().execute(ddl);
        ddlConn.close();

        String upsertSelect = "UPSERT INTO " + tableName2 + " SELECT * FROM " + tableName1;

        Map<String, Map<String, Long>> mutationMetricsAutoCommitOff = null;
        Map<String, Map<String, Long>> readMetricsAutoCommitOff = null;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(false);
            conn.createStatement().executeUpdate(upsertSelect);
            conn.commit();
            PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);
            mutationMetricsAutoCommitOff = PhoenixRuntime.getWriteMetricsForMutationsSinceLastReset(pConn);
            readMetricsAutoCommitOff = PhoenixRuntime.getReadMetricsForMutationsSinceLastReset(pConn);
        }

        Map<String, Map<String, Long>> mutationMetricsAutoCommitOn = null;
        Map<String, Map<String, Long>> readMetricsAutoCommitOn = null;

        int autoCommitBatchSize = numRows + 1; // batchsize = 11 is less than numRows and is not a divisor of batchsize
        Properties props = new Properties();
        props.setProperty(UPSERT_BATCH_SIZE_ATTRIB, Integer.toString(autoCommitBatchSize));
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate(upsertSelect);
            PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);
            mutationMetricsAutoCommitOn = PhoenixRuntime.getWriteMetricsForMutationsSinceLastReset(pConn);
            readMetricsAutoCommitOn = PhoenixRuntime.getReadMetricsForMutationsSinceLastReset(pConn);
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
            mutationMetricsAutoCommitOn = PhoenixRuntime.getWriteMetricsForMutationsSinceLastReset(pConn);
            readMetricsAutoCommitOn = PhoenixRuntime.getReadMetricsForMutationsSinceLastReset(pConn);
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
            mutationMetricsAutoCommitOn = PhoenixRuntime.getWriteMetricsForMutationsSinceLastReset(pConn);
            readMetricsAutoCommitOn = PhoenixRuntime.getReadMetricsForMutationsSinceLastReset(pConn);
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
            mutationMetricsAutoCommitOn = PhoenixRuntime.getWriteMetricsForMutationsSinceLastReset(pConn);
            readMetricsAutoCommitOn = PhoenixRuntime.getReadMetricsForMutationsSinceLastReset(pConn);
        }
        assertMetricsAreSame(mutationMetricsAutoCommitOff, mutationMetricsAutoCommitOn, mutationMetricsToSkip);
        assertMetricsAreSame(readMetricsAutoCommitOff, readMetricsAutoCommitOff, readMetricsToSkip);
    }

    @Test
    public void testMutationMetricsWhenUpsertingToMultipleTables() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String table1 = "TABLE1";
            createTableAndInsertValues(true, 10, conn, table1);
            String table2 = "TABLE2";
            createTableAndInsertValues(true, 10, conn, table2);
            String table3 = "TABLE3";
            createTableAndInsertValues(true, 10, conn, table3);
            Map<String, Map<String, Long>> mutationMetrics = PhoenixRuntime.getWriteMetricsForMutationsSinceLastReset(conn);
            assertTrue("Mutation metrics not present for " + table1, mutationMetrics.get(table1) != null);
            assertTrue("Mutation metrics not present for " + table2, mutationMetrics.get(table2) != null);
            assertTrue("Mutation metrics not present for " + table3, mutationMetrics.get(table3) != null);
            assertMetricsHaveSameValues(mutationMetrics.get(table1), mutationMetrics.get(table2), mutationMetricsToSkip);
            assertMetricsHaveSameValues(mutationMetrics.get(table1), mutationMetrics.get(table3), mutationMetricsToSkip);
        }
    }

    @Test
    public void testClosingConnectionClearsMetrics() throws Exception {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(getUrl());
            createTableAndInsertValues(true, 10, conn, "clearmetrics");
            assertTrue("Mutation metrics not present", PhoenixRuntime.getWriteMetricsForMutationsSinceLastReset(conn).size() > 0);
        } finally {
            if (conn != null) {
                conn.close();
                assertTrue("Closing connection didn't clear metrics",
                        PhoenixRuntime.getWriteMetricsForMutationsSinceLastReset(conn).size() == 0);
            }
        }
    }

    @Test
    public void testMetricsForUpsertingIntoImmutableTableWithIndices() throws Exception {
        String dataTable = "IMMTABLEWITHINDICES";
        String tableDdl = "CREATE TABLE "
                + dataTable
                + " (K1 VARCHAR NOT NULL, K2 VARCHAR NOT NULL, V1 INTEGER, V2 INTEGER, V3 INTEGER CONSTRAINT NAME_PK PRIMARY KEY(K1, K2)) IMMUTABLE_ROWS = true";
        String index1 = "I1";
        String index1Ddl = "CREATE INDEX " + index1 + " ON " + dataTable + " (V1) include (V2)";
        String index2 = "I2";
        String index2Ddl = "CREATE INDEX " + index2 + " ON " + dataTable + " (V2) include (V3)";
        String index3 = "I3";
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
            Map<String, Map<String, Long>> metrics = PhoenixRuntime.getWriteMetricsForMutationsSinceLastReset(conn);
            assertTrue(metrics.get(dataTable).size() > 0);
            assertTrue(metrics.get(index1).size() > 0);
            assertTrue(metrics.get(index2).size() > 0);
            assertMetricsHaveSameValues(metrics.get(index1), metrics.get(index2), mutationMetricsToSkip);
            assertTrue(metrics.get(index3).size() > 0);
            assertMetricsHaveSameValues(metrics.get(index1), metrics.get(index3), mutationMetricsToSkip);
        }
    }
    
    @Test
    public void testMetricsForUpsertSelectSameTable() throws Exception {
        String tableName = "UPSERTSAME";
        long table1SaltBuckets = 6;
        String ddl = "CREATE TABLE " + tableName + " (K VARCHAR NOT NULL PRIMARY KEY, V VARCHAR)" + " SALT_BUCKETS = "
                + table1SaltBuckets;
        Connection ddlConn = DriverManager.getConnection(getUrl());
        ddlConn.createStatement().execute(ddl);
        ddlConn.close();
        int numRows = 10;
        insertRowsInTable(tableName, numRows);

        Connection conn = DriverManager.getConnection(getUrl());
        conn.setAutoCommit(false);
        String upsertSelect = "UPSERT INTO " + tableName + " SELECT * FROM " + tableName;
        conn.createStatement().executeUpdate(upsertSelect);
        conn.commit();
        PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);
        
        Map<String, Map<String, Long>> mutationMetrics = PhoenixRuntime.getWriteMetricsForMutationsSinceLastReset(pConn);
        // Because auto-commit is off, upsert select into the same table will run on the client.
        // So we should have client side read and write metrics available.
        assertMutationMetrics(tableName, numRows, mutationMetrics);
        Map<String, Map<String, Long>> readMetrics = PhoenixRuntime.getReadMetricsForMutationsSinceLastReset(pConn);
        assertReadMetricsForMutatingSql(tableName, table1SaltBuckets, readMetrics);
        PhoenixRuntime.resetMetrics(pConn);
        // With autocommit on, still, this upsert select runs on the client side.
        conn.setAutoCommit(true);
        conn.createStatement().executeUpdate(upsertSelect);
        Map<String, Map<String, Long>> autoCommitMutationMetrics = PhoenixRuntime.getWriteMetricsForMutationsSinceLastReset(pConn);
        Map<String, Map<String, Long>> autoCommitReadMetrics = PhoenixRuntime.getReadMetricsForMutationsSinceLastReset(pConn);
        assertMetricsAreSame(mutationMetrics, autoCommitMutationMetrics, mutationMetricsToSkip);
        assertMetricsAreSame(readMetrics, autoCommitReadMetrics, readMetricsToSkip);
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

    private void assertMetricsAreSame(Map<String, Map<String, Long>> metric1, Map<String, Map<String, Long>> metric2,
            List<String> metricsToSkip) {
        assertTrue("The two metrics have different or unequal number of table names ",
                metric1.keySet().equals(metric2.keySet()));
        for (Entry<String, Map<String, Long>> entry : metric1.entrySet()) {
            Map<String, Long> metricNameValueMap1 = entry.getValue();
            Map<String, Long> metricNameValueMap2 = metric2.get(entry.getKey());
            assertMetricsHaveSameValues(metricNameValueMap1, metricNameValueMap2, metricsToSkip);
        }
    }

    private void assertMetricsHaveSameValues(Map<String, Long> metricNameValueMap1,
            Map<String, Long> metricNameValueMap2, List<String> metricsToSkip) {
        assertTrue("The two metrics have different or unequal number of metric names ", metricNameValueMap1.keySet()
                .equals(metricNameValueMap2.keySet()));
        for (Entry<String, Long> entry : metricNameValueMap1.entrySet()) {
            String metricName = entry.getKey();
            if (!metricsToSkip.contains(metricName)) {
                assertEquals("Unequal values for metric " + metricName, entry.getValue(),
                        metricNameValueMap2.get(metricName));
            }
        }
    }

    private void changeInternalStateForTesting(PhoenixResultSet rs) {
        // get and set the internal state for testing purposes.
        ReadMetricQueue testMetricsQueue = new TestReadMetricsQueue(true);
        StatementContext ctx = (StatementContext)Whitebox.getInternalState(rs, "context");
        Whitebox.setInternalState(ctx, "readMetricsQueue", testMetricsQueue);
        Whitebox.setInternalState(rs, "readMetricsQueue", testMetricsQueue);
    }

    private void assertReadMetricValuesForSelectSql(ArrayList<Long> numRows, ArrayList<Long> numExpectedTasks,
            PhoenixResultSet resultSetBeingTested, Set<String> expectedTableNames) throws SQLException {
        Map<String, Map<String, Long>> metrics = PhoenixRuntime.getRequestReadMetrics(resultSetBeingTested);
        int counter = 0;
        for (Entry<String, Map<String, Long>> entry : metrics.entrySet()) {
            String tableName = entry.getKey();
            expectedTableNames.remove(tableName);
            Map<String, Long> metricValues = entry.getValue();
            boolean scanMetricsPresent = false;
            boolean taskCounterMetricsPresent = false;
            boolean taskExecutionTimeMetricsPresent = false;
            boolean memoryMetricsPresent = false;
            for (Entry<String, Long> pair : metricValues.entrySet()) {
                String metricName = pair.getKey();
                long metricValue = pair.getValue();
                long n = numRows.get(counter);
                long numTask = numExpectedTasks.get(counter);
                if (metricName.equals(SCAN_BYTES.name())) {
                    // we are using a SCAN_BYTES_DELTA of 1. So number of scan bytes read should be number of rows read
                    assertEquals(n, metricValue);
                    scanMetricsPresent = true;
                } else if (metricName.equals(TASK_EXECUTED_COUNTER.name())) {
                    assertEquals(numTask, metricValue);
                    taskCounterMetricsPresent = true;
                } else if (metricName.equals(TASK_EXECUTION_TIME.name())) {
                    assertEquals(numTask * TASK_EXECUTION_TIME_DELTA, metricValue);
                    taskExecutionTimeMetricsPresent = true;
                } else if (metricName.equals(MEMORY_CHUNK_BYTES.name())) {
                    assertEquals(numTask * MEMORY_CHUNK_BYTES_DELTA, metricValue);
                    memoryMetricsPresent = true;
                }
            }
            counter++;
            assertTrue(scanMetricsPresent);
            assertTrue(taskCounterMetricsPresent);
            assertTrue(taskExecutionTimeMetricsPresent);
            assertTrue(memoryMetricsPresent);
        }
        PhoenixRuntime.resetMetrics(resultSetBeingTested);
        assertTrue("Metrics not found tables " + Joiner.on(",").join(expectedTableNames),
                expectedTableNames.size() == 0);
    }

    private Connection insertRowsInTable(String tableName, long numRows) throws SQLException {
        String dml = "UPSERT INTO " + tableName + " VALUES (?, ?)";
        Connection conn = DriverManager.getConnection(getUrl());
        PreparedStatement stmt = conn.prepareStatement(dml);
        for (int i = 1; i <= numRows; i++) {
            stmt.setString(1, "key" + i);
            stmt.setString(2, "value" + i);
            stmt.executeUpdate();
        }
        conn.commit();
        return conn;
    }

    // number of records read should be number of bytes at the end
    public static final long SCAN_BYTES_DELTA = 1;

    // total task execution time should be numTasks * TASK_EXECUTION_TIME_DELTA
    public static final long TASK_EXECUTION_TIME_DELTA = 10;

    // total task execution time should be numTasks * TASK_EXECUTION_TIME_DELTA
    public static final long MEMORY_CHUNK_BYTES_DELTA = 100;

    private class TestReadMetricsQueue extends ReadMetricQueue {

        public TestReadMetricsQueue(boolean isRequestMetricsEnabled) {
            super(isRequestMetricsEnabled);
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

    private void assertReadMetricsForMutatingSql(String tableName, long tableSaltBuckets,
            Map<String, Map<String, Long>> readMetrics) {
        assertTrue("No read metrics present when there should have been!", readMetrics.size() > 0);
        int numTables = 0;
        for (Entry<String, Map<String, Long>> entry : readMetrics.entrySet()) {
            String t = entry.getKey();
            assertEquals("Table name didn't match for read metrics", tableName, t);
            numTables++;
            Map<String, Long> p = entry.getValue();
            assertTrue("No read metrics present when there should have been", p.size() > 0);
            for (Entry<String, Long> metric : p.entrySet()) {
                String metricName = metric.getKey();
                long metricValue = metric.getValue();
                if (metricName.equals(TASK_EXECUTED_COUNTER.name())) {
                    assertEquals(tableSaltBuckets, metricValue);
                } else if (metricName.equals(SCAN_BYTES.name())) {
                    assertTrue("Scan bytes read should be greater than zero", metricValue > 0);
                }
            }
        }
        assertEquals("There should have been read metrics only for one table: " + tableName, 1, numTables);
    }

    private void assertMutationMetrics(String tableName, int numRows, Map<String, Map<String, Long>> mutationMetrics) {
        assertTrue("No mutation metrics present when there should have been", mutationMetrics.size() > 0);
        for (Entry<String, Map<String, Long>> entry : mutationMetrics.entrySet()) {
            String t = entry.getKey();
            assertEquals("Table name didn't match for mutation metrics", tableName, t);
            Map<String, Long> p = entry.getValue();
            assertEquals("There should have been three metrics", 3, p.size());
            for (Entry<String, Long> metric : p.entrySet()) {
                String metricName = metric.getKey();
                long metricValue = metric.getValue();
                if (metricName.equals(MetricType.MUTATION_BATCH_SIZE.name())) {
                    assertEquals("Mutation batch sizes didn't match!", numRows, metricValue);
                } else if (metricName.equals(MetricType.MUTATION_COMMIT_TIME.name())) {
                    assertTrue("Mutation commit time should be greater than zero", metricValue > 0);
                } else if (metricName.equals(MetricType.MUTATION_BYTES.name())) {
                    assertTrue("Mutation bytes size should be greater than zero", metricValue > 0);
                }
            }
        }
    }

}
