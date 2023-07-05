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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.BIND_PARAMETERS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CLIENT_IP;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.EXCEPTION_TRACE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.EXPLAIN_PLAN;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.GLOBAL_SCAN_DETAILS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.NO_OF_RESULTS_ITERATED;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.QUERY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.QUERY_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.QUERY_STATUS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SCAN_METRICS_JSON;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.START_TIME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_LOG_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TENANT_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.USER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.log.LogLevel;
import org.apache.phoenix.log.QueryLogger;
import org.apache.phoenix.log.QueryStatus;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.EnvironmentEdge;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;


@Category(NeedsOwnMiniClusterTest.class)
public class QueryLoggerIT extends BaseTest {


    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        // Enable request metric collection at the driver level
        props.put(QueryServices.COLLECT_REQUEST_LEVEL_METRICS, String.valueOf(true));
        // disable renewing leases as this will force spooling to happen.
        props.put(QueryServices.RENEW_LEASE_ENABLED, String.valueOf(false));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
        // need the non-test driver for some tests that check number of hconnections, etc.
        DriverManager.registerDriver(PhoenixDriver.INSTANCE);
    } 
    
    private static class MyClock extends EnvironmentEdge {
        public volatile long time;

        public MyClock (long time) {
            this.time = time;
        }

        @Override
        public long currentTime() {
            return time;
        }
    }
    

    @Test
    public void testDebugLogs() throws Exception {
        String tableName = generateUniqueName();
        createTableAndInsertValues(tableName, true);
        Properties props= new Properties();
        props.setProperty(QueryServices.LOG_LEVEL, LogLevel.DEBUG.name());
        Connection conn = DriverManager.getConnection(getUrl(),props);
        assertEquals(conn.unwrap(PhoenixConnection.class).getLogLevel(),LogLevel.DEBUG);
        String query = "SELECT * FROM " + tableName;
        StatementContext context;
        try (ResultSet rs = conn.createStatement().executeQuery(query)) {
            context = ((PhoenixResultSet) rs).getContext();
            while (rs.next()) {
                rs.getString(1);
                rs.getString(2);
            }
        }
        String queryId = context.getQueryLogger().getQueryId();

        String logQuery = "SELECT * FROM " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_LOG_TABLE + "\"";
        int delay = 5000;

        // sleep for sometime to let query log committed
        Thread.sleep(delay);
        try (ResultSet explainRS = conn.createStatement().executeQuery("Explain with regions " + query);
             ResultSet rs = conn.createStatement().executeQuery(logQuery)) {
            boolean foundQueryLog = false;

            while (rs.next()) {
                if (rs.getString(QUERY_ID).equals(queryId)) {
                    foundQueryLog = true;
                    assertEquals(rs.getString(BIND_PARAMETERS), null);
                    assertEquals(rs.getString(USER), System.getProperty("user.name"));
                    assertEquals(rs.getString(CLIENT_IP), InetAddress.getLocalHost().getHostAddress());
                    assertEquals(rs.getString(EXPLAIN_PLAN), QueryUtil.getExplainPlan(explainRS));
                    assertEquals(rs.getString(GLOBAL_SCAN_DETAILS), context.getScan().toJSON());
                    assertEquals(rs.getLong(NO_OF_RESULTS_ITERATED), 10);
                    assertEquals(rs.getString(QUERY), query);
                    assertEquals(rs.getString(QUERY_STATUS), QueryStatus.COMPLETED.toString());
                    assertEquals(rs.getString(TENANT_ID), null);
                    assertTrue(rs.getString(SCAN_METRICS_JSON) == null);
                    assertEquals(rs.getString(EXCEPTION_TRACE), null);
                } else {
                    //confirm we are not logging system queries
                    assertFalse(rs.getString(QUERY).toString().contains(SYSTEM_CATALOG_SCHEMA));
                }
            }
            assertTrue(foundQueryLog);
            conn.close();
        }
    }
    
    @Test
    public void testLogSampling() throws Exception {
        String tableName = generateUniqueName();
        createTableAndInsertValues(tableName, true);
        Properties props= new Properties();
        props.setProperty(QueryServices.LOG_LEVEL, LogLevel.DEBUG.name());
        props.setProperty(QueryServices.LOG_SAMPLE_RATE, "0.5");
        Connection conn = DriverManager.getConnection(getUrl(),props);
        assertEquals(conn.unwrap(PhoenixConnection.class).getLogLevel(),LogLevel.DEBUG);
        String query = "SELECT * FROM " + tableName;
        int count=100;
        for (int i = 0; i < count; i++) {
            try (ResultSet rs = conn.createStatement().executeQuery(query)) {
                while (rs.next()) {

                }
            }
        }
        String logQuery = "SELECT * FROM " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_LOG_TABLE + "\"";
        
        int delay = 5000;

        // sleep for sometime to let query log committed
        Thread.sleep(delay);
        ResultSet rs = conn.createStatement().executeQuery(logQuery);
        int logCount=0;
        while (rs.next()) {
            logCount++;
        }
        
        //sampling rate is 0.5 , but with lesser count, uniformity of thread random may not be perfect, so taking 0.75 for comparison 
        assertTrue(logCount != 0 && logCount < count * 0.75);
        conn.close();
    }
    
    @Test
    public void testInfoLogs() throws Exception{
        String tableName = generateUniqueName();
        createTableAndInsertValues(tableName, true);
        Properties props= new Properties();
        props.setProperty(QueryServices.LOG_LEVEL, LogLevel.INFO.name());
        Connection conn = DriverManager.getConnection(getUrl(),props);
        assertEquals(conn.unwrap(PhoenixConnection.class).getLogLevel(),LogLevel.INFO);
        String query = "SELECT * FROM " + tableName;
        StatementContext context;
        try (ResultSet rs = conn.createStatement().executeQuery(query)) {
            context = ((PhoenixResultSet) rs).getContext();
            while (rs.next()) {
                rs.getString(1);
                rs.getString(2);
            }
        }
        String queryId = context.getQueryLogger().getQueryId();

        String logQuery = "SELECT * FROM " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_LOG_TABLE + "\"";
        int delay = 5000;

        // sleep for sometime to let query log committed
        Thread.sleep(delay);
        try (ResultSet rs = conn.createStatement().executeQuery(logQuery)) {
            boolean foundQueryLog = false;
            while (rs.next()) {
                if (rs.getString(QUERY_ID).equals(queryId)) {
                    foundQueryLog = true;
                    assertEquals(rs.getString(USER), System.getProperty("user.name"));
                    assertEquals(rs.getString(CLIENT_IP), InetAddress.getLocalHost().getHostAddress());
                    assertEquals(rs.getString(EXPLAIN_PLAN), null);
                    assertEquals(rs.getString(GLOBAL_SCAN_DETAILS), null);
                    assertEquals(rs.getLong(NO_OF_RESULTS_ITERATED), 10);
                    assertEquals(rs.getString(QUERY), query);
                    assertEquals(rs.getString(QUERY_STATUS), QueryStatus.COMPLETED.toString());
                    assertEquals(rs.getString(TENANT_ID), null);
                }
            }
            assertTrue(foundQueryLog);
            conn.close();
        }
    }
    
    @Test
    public void testWithLoggingOFF() throws Exception{
        String tableName = generateUniqueName();
        createTableAndInsertValues(tableName, true);
        Properties props= new Properties();
        props.setProperty(QueryServices.LOG_LEVEL, LogLevel.OFF.name());
        Connection conn = DriverManager.getConnection(getUrl(),props);
        assertEquals(conn.unwrap(PhoenixConnection.class).getLogLevel(),LogLevel.OFF);

        // delete old data
        conn.createStatement().executeUpdate("delete from " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_LOG_TABLE + "\"");
        conn.commit();

        String query = "SELECT * FROM " + tableName;
        ResultSet rs = conn.createStatement().executeQuery(query);
        StatementContext context = ((PhoenixResultSet)rs).getContext();
        assertEquals(context.getQueryLogger(), QueryLogger.NO_OP_INSTANCE);
        while (rs.next()) {
            rs.getString(1);
            rs.getString(2);
        }

        String logQuery = "SELECT count(*) FROM " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_LOG_TABLE + "\"";
        int delay = 5000;

        // sleep for sometime to let query log committed
        Thread.sleep(delay);
        rs = conn.createStatement().executeQuery(logQuery);
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 0);
        assertFalse(rs.next());
        conn.close();
    }
    

    @Test
    public void testPreparedStatementWithTrace() throws Exception{
        testPreparedStatement(LogLevel.TRACE);   
    }
    
    @Test
    public void testPreparedStatementWithDebug() throws Exception{
        testPreparedStatement(LogLevel.DEBUG);
    }
            
    private void testPreparedStatement(LogLevel loglevel) throws Exception{
        String tableName = generateUniqueName();
        createTableAndInsertValues(tableName, true);
        Properties props= new Properties();
        props.setProperty(QueryServices.LOG_LEVEL, loglevel.name());
        Connection conn = DriverManager.getConnection(getUrl(),props);
        assertEquals(conn.unwrap(PhoenixConnection.class).getLogLevel(),loglevel);
        final MyClock clock = new MyClock(100);
        EnvironmentEdgeManager.injectEdge(clock);
        try{
            String query = "SELECT * FROM " + tableName +" where V = ?";
            StatementContext context;
            PreparedStatement pstmt = conn.prepareStatement(query);
            pstmt.setString(1, "value5");
            try (ResultSet rs = pstmt.executeQuery()) {
                 context = ((PhoenixResultSet) rs).getContext();
                while (rs.next()) {
                    rs.getString(1);
                    rs.getString(2);
                }
            }
            String queryId = context.getQueryLogger().getQueryId();

            String logQuery = "SELECT * FROM " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_LOG_TABLE + "\"";
            int delay = 5000;

            // sleep for sometime to let query log committed
            Thread.sleep(delay);
            String explainQuery = "EXPLAIN WITH REGIONS " + "SELECT * FROM " + tableName + " where V = 'value5'";
            try (ResultSet explainRS = conn.createStatement()
                    .executeQuery(explainQuery);
                 ResultSet rs = conn.createStatement().executeQuery(logQuery)) {
                boolean foundQueryLog = false;
                while (rs.next()) {
                    if (rs.getString(QUERY_ID).equals(queryId)) {
                        foundQueryLog = true;
                        assertEquals(rs.getString(BIND_PARAMETERS), loglevel == LogLevel.TRACE ? "value5" : null);
                        assertEquals(rs.getString(USER), System.getProperty("user.name"));
                        assertEquals(rs.getString(CLIENT_IP), InetAddress.getLocalHost().getHostAddress());
                        assertEquals(rs.getString(EXPLAIN_PLAN), QueryUtil.getExplainPlan(explainRS));
                        assertEquals(rs.getString(GLOBAL_SCAN_DETAILS), context.getScan().toJSON());
                        assertEquals(rs.getLong(NO_OF_RESULTS_ITERATED), 1);
                        assertEquals(rs.getString(QUERY), query);
                        assertEquals(rs.getString(QUERY_STATUS), QueryStatus.COMPLETED.toString());
                        assertTrue(LogLevel.TRACE == loglevel ? rs.getString(SCAN_METRICS_JSON).contains("scanMetrics")
                                : rs.getString(SCAN_METRICS_JSON) == null);
                        assertEquals(rs.getTimestamp(START_TIME).getTime(), 100);
                        assertEquals(rs.getString(TENANT_ID), null);
                    }
                }
                assertTrue(foundQueryLog);
                conn.close();
            }
        }finally {
            EnvironmentEdgeManager.injectEdge(null);
        }
    }
    
    
    
    @Test
    public void testFailedQuery() throws Exception {
        String tableName = generateUniqueName();
        Properties props = new Properties();
        props.setProperty(QueryServices.LOG_LEVEL, LogLevel.DEBUG.name());
        Connection conn = DriverManager.getConnection(getUrl(), props);
        assertEquals(conn.unwrap(PhoenixConnection.class).getLogLevel(), LogLevel.DEBUG);
        // Table does not exists
        String query = "SELECT * FROM " + tableName;

        try {
            conn.createStatement().executeQuery(query);
            fail();
        } catch (SQLException e) {
            assertEquals(e.getErrorCode(), SQLExceptionCode.TABLE_UNDEFINED.getErrorCode());
        }
        String logQuery = "SELECT * FROM " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_LOG_TABLE + "\"";
        int delay = 5000;

        // sleep for sometime to let query log committed
        Thread.sleep(delay);
        ResultSet rs = conn.createStatement().executeQuery(logQuery);
        boolean foundQueryLog = false;
        while (rs.next()) {
            if (QueryStatus.FAILED.name().equals(rs.getString(QUERY_STATUS))) {
                foundQueryLog = true;
                assertEquals(rs.getString(USER), System.getProperty("user.name"));
                assertEquals(rs.getString(CLIENT_IP), InetAddress.getLocalHost().getHostAddress());
                assertEquals(rs.getString(EXPLAIN_PLAN), null);
                assertEquals(rs.getString(GLOBAL_SCAN_DETAILS), null);
                assertEquals(rs.getLong(NO_OF_RESULTS_ITERATED), 0);
                assertEquals(rs.getString(QUERY), query);
                assertTrue(rs.getString(EXCEPTION_TRACE).contains(SQLExceptionCode.TABLE_UNDEFINED.getMessage()));
            }
        }
        assertTrue(foundQueryLog);
        conn.close();
    }

    private static void createTableAndInsertValues(String tableName, boolean resetGlobalMetricsAfterTableCreate)
            throws Exception {
        String ddl = "CREATE TABLE " + tableName + " (K VARCHAR NOT NULL PRIMARY KEY, V VARCHAR)";
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(ddl);
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
    
}
