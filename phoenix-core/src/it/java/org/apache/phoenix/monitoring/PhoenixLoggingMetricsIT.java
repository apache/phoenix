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

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.LoggingPhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixMetricsLog;
import org.apache.phoenix.jdbc.LoggingPhoenixResultSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Category(NeedsOwnMiniClusterTest.class)
public class PhoenixLoggingMetricsIT extends BasePhoenixMetricsIT {

    private static final int NUM_ROWS = 10;

    private final Map<MetricType, Long> overAllQueryMetricsMap = Maps.newHashMap();
    private final Map<String, Map<MetricType, Long>> requestReadMetricsMap = Maps.newHashMap();
    private final Map<String, Map<MetricType, Long>> mutationWriteMetricsMap = Maps.newHashMap();
    private final Map<String, Map<MetricType, Long>> mutationReadMetricsMap = Maps.newHashMap();

    private String tableName1;
    private String tableName2;
    private LoggingPhoenixConnection loggedConn;
    private String loggedSql;
    private int logOverAllReadRequestMetricsFuncCallCount;
    private int logRequestReadMetricsFuncCallCount;

    @Before
    public void beforeTest() throws Exception {
        clearAllTestMetricMaps();
        tableName1 = generateUniqueName();
        String ddl = "CREATE TABLE " + tableName1 + " (K VARCHAR NOT NULL PRIMARY KEY, V VARCHAR)";
        Connection setupConn = DriverManager.getConnection(getUrl());
        setupConn.createStatement().execute(ddl);
        setupConn.close();
        insertRowsInTable(tableName1, NUM_ROWS);

        tableName2 = generateUniqueName();
        ddl = "CREATE TABLE " + tableName2 + " (K VARCHAR NOT NULL PRIMARY KEY, V VARCHAR)";
        setupConn = DriverManager.getConnection(getUrl());
        setupConn.createStatement().execute(ddl);
        setupConn.close();

        Connection testConn = DriverManager.getConnection(getUrl());
        loggedConn = getLoggingPhoenixConnection(testConn);
    }

    @Test
    public void testResultSetTypeForQueries() throws Exception {
        String tableName3 = generateUniqueName();

        String create = "CREATE TABLE " + tableName3 + " (K INTEGER PRIMARY KEY)";
        assertNull(executeAndGetResultSet(create));

        String upsert = "UPSERT INTO " + tableName3 + " VALUES (42)";
        assertNull(executeAndGetResultSet(upsert));

        String select = "SELECT * FROM " + tableName3;
        assertTrue(executeAndGetResultSet(select) instanceof LoggingPhoenixResultSet);

        String createView = "CREATE VIEW TEST_VIEW (K1 INTEGER) AS SELECT * FROM " + tableName3;
        assertNull(executeAndGetResultSet(createView));

        String createIndex = "CREATE INDEX TEST_INDEX ON " + tableName3 + " (K)";
        assertNull(executeAndGetResultSet(createIndex));

        String dropIndex = "DROP INDEX TEST_INDEX ON " + tableName3;
        assertNull(executeAndGetResultSet(dropIndex));

        String dropView = "DROP VIEW TEST_VIEW";
        assertNull(executeAndGetResultSet(dropView));

        String dropTable = "DROP TABLE " + tableName3;
        assertNull(executeAndGetResultSet(dropTable));
    }

    @Test
    public void testPhoenixMetricsLoggedOnCommit() throws Exception {
        // run SELECT to verify read metrics are logged
        String query = "SELECT * FROM " + tableName1;
        ResultSet rs = upsertRows(query);
        verifyQueryLevelMetricsLogging(query, rs);

        // run UPSERT SELECT to verify mutation metrics are logged
        String upsertSelect = "UPSERT INTO " + tableName2 + " SELECT * FROM " + tableName1;
        loggedConn.createStatement().executeUpdate(upsertSelect);

        // Assert that metrics are logged upon commit
        loggedConn.commit();
        assertTrue("Mutation write metrics for not found for " + tableName2,
                mutationWriteMetricsMap.get(tableName2).size() > 0);
        assertMutationMetrics(tableName2, NUM_ROWS, true, mutationWriteMetricsMap);
        assertTrue("Mutation read metrics for not found for " + tableName1,
                mutationReadMetricsMap.get(tableName1).size() > 0);
        assertReadMetricsForMutatingSql(tableName1, 1, mutationReadMetricsMap);

        clearAllTestMetricMaps();

        // Assert that metrics logging happens only once
        loggedConn.close();
        assertTrue("Mutation write metrics are not logged again.",
                mutationWriteMetricsMap.size() == 0);
        assertTrue("Mutation read metrics are not logged again.",
                mutationReadMetricsMap.size() == 0);

        clearAllTestMetricMaps();

        // Assert that metrics logging happens only once again
        loggedConn.close();
        assertTrue("Mutation write metrics are not logged again.",
                mutationWriteMetricsMap.size() == 0);
        assertTrue("Mutation read metrics are not logged again.",
                mutationReadMetricsMap.size() == 0);
    }

    @Test
    public void testPhoenixMetricsLoggedOnClose() throws Exception {
        // run SELECT to verify read metrics are logged
        String query = "SELECT * FROM " + tableName1;

        ResultSet rs = upsertRows(query);
        verifyQueryLevelMetricsLogging(query, rs);

        // run UPSERT SELECT to verify mutation metrics are logged
        String upsertSelect = "UPSERT INTO " + tableName2 + " SELECT * FROM " + tableName1;
        loggedConn.createStatement().executeUpdate(upsertSelect);

        // Autocommit is turned off by default
        // Hence mutation metrics are not expected during connection close
        loggedConn.close();
        assertTrue("Mutation write metrics are not logged for " + tableName2,
                mutationWriteMetricsMap.size() == 0);
        assertTrue("Mutation read metrics for not found for " + tableName1,
                mutationReadMetricsMap.get(tableName1).size() > 0);
        assertReadMetricsForMutatingSql(tableName1, 1, mutationReadMetricsMap);

        clearAllTestMetricMaps();

        loggedConn.close();
        assertTrue("Mutation write metrics are not logged again.",
                mutationWriteMetricsMap.size() == 0);
        assertTrue("Mutation read metrics are not logged again.",
                mutationReadMetricsMap.size() == 0);
    }

    /**
     * This test is added to verify if metrics are being logged in case
     * auto commit is set to true.
     */
    @Test
    public void testPhoenixMetricsLoggedOnAutoCommitTrue() throws Exception {
        loggedConn.setAutoCommit(true);

        String query = "SELECT * FROM " + tableName1;
        ResultSet rs = upsertRows(query);
        verifyQueryLevelMetricsLogging(query, rs);

        // run UPSERT SELECT to verify mutation metrics are logged
        String upsertSelect = "UPSERT INTO " + tableName2 + " SELECT * FROM " + tableName1;
        loggedConn.createStatement().executeUpdate(upsertSelect);

        assertTrue("Mutation write metrics are not logged for " + tableName2,
                mutationWriteMetricsMap.get(tableName2).size()  > 0);
        assertTrue("Mutation read metrics are not found for " + tableName1,
                mutationReadMetricsMap.get(tableName1).size() > 0);

        clearAllTestMetricMaps();

        loggedConn.createStatement().execute(query);
        assertTrue("Read metrics found for " + tableName1,
                mutationReadMetricsMap.size() == 0);
        loggedConn.createStatement().execute(upsertSelect);

        assertTrue("Mutation write metrics are not logged for " + tableName2
                + " in createStatement",mutationWriteMetricsMap.get(tableName2).size()  > 0);
        assertTrue("Mutation read metrics are not found for " + tableName1
                + " in createStatement",mutationReadMetricsMap.get(tableName1).size() > 0);

        clearAllTestMetricMaps();

        loggedConn.prepareStatement(query).executeQuery();
        assertTrue("Read metrics found for " + tableName1,
                mutationReadMetricsMap.size() == 0);

        loggedConn.prepareStatement(upsertSelect).executeUpdate();
        assertTrue("Mutation write metrics are not logged for " + tableName2
                + " in prepareStatement",mutationWriteMetricsMap.get(tableName2).size()  > 0);
        assertTrue("Mutation read metrics are not found for " + tableName1
                + " in prepareStatement",mutationReadMetricsMap.get(tableName1).size() > 0);


    }

    private ResultSet executeAndGetResultSet(String query) throws Exception {
        Statement stmt = loggedConn.createStatement();
        stmt.execute(query);
        return stmt.getResultSet();
    }

    private ResultSet upsertRows(String query) throws SQLException {
        Statement stmt = loggedConn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        assertTrue(rs instanceof LoggingPhoenixResultSet);
        int rowsRetrievedCounter = 0;
        while (rs.next()) {
            rowsRetrievedCounter++;
        }
        rs.close();
        assertTrue(rowsRetrievedCounter == NUM_ROWS);
        return rs;
    }

    private void verifyQueryLevelMetricsLogging(String query , ResultSet rs) throws SQLException {
        assertTrue("Read metrics for not found for " + tableName1,
                requestReadMetricsMap.get(tableName1).size() > 0);
        assertTrue("Logged query doesn't match actual query", loggedSql.equals(query));
        assertTrue(logOverAllReadRequestMetricsFuncCallCount == 1);
        assertTrue(logRequestReadMetricsFuncCallCount == 1);

        assertTrue("Overall read metrics for not found ", overAllQueryMetricsMap.size() > 0);
        assertTrue("Logged query doesn't match actual query", loggedSql.equals(query));

        rs.close();
        assertTrue(logOverAllReadRequestMetricsFuncCallCount == 1);
        assertTrue(logRequestReadMetricsFuncCallCount == 1);
    }

    void clearAllTestMetricMaps() {
        overAllQueryMetricsMap.clear();
        requestReadMetricsMap.clear();
        mutationWriteMetricsMap.clear();
        mutationReadMetricsMap.clear();
    }

    LoggingPhoenixConnection getLoggingPhoenixConnection(Connection conn) {
        return new LoggingPhoenixConnection(conn, new PhoenixMetricsLog() {
            @Override
            public void logOverAllReadRequestMetrics(
                    Map<MetricType, Long> overAllQueryMetrics, String sql) {
                overAllQueryMetricsMap.putAll(overAllQueryMetrics);
                loggedSql = sql;
                logOverAllReadRequestMetricsFuncCallCount++;
            }

            @Override
            public void logRequestReadMetrics(
                    Map<String, Map<MetricType, Long>> requestReadMetrics, String sql) {
                requestReadMetricsMap.putAll(requestReadMetrics);
                loggedSql = sql;
                logRequestReadMetricsFuncCallCount++;
            }

            @Override
            public void logWriteMetricsfoForMutationsSinceLastReset(
                    Map<String, Map<MetricType, Long>> mutationWriteMetrics) {
                mutationWriteMetricsMap.putAll(mutationWriteMetrics);
            }

            @Override
            public void logReadMetricInfoForMutationsSinceLastReset(
                    Map<String, Map<MetricType, Long>> mutationReadMetrics) {
                mutationReadMetricsMap.putAll(mutationReadMetrics);
            }
        });
    }
}