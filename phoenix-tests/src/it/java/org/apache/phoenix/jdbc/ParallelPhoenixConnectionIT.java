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
package org.apache.phoenix.jdbc;

import static org.apache.hadoop.test.GenericTestUtils.waitFor;
import static org.apache.phoenix.jdbc.HighAvailabilityGroup.PHOENIX_HA_GROUP_ATTR;
import static org.apache.phoenix.jdbc.HighAvailabilityPolicy.PARALLEL;
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.HBaseTestingUtilityPair.doTestWhenOneHBaseDown;
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.doTestBasicOperationsWithConnection;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HA_PARALLEL_CONNECTION_CREATED_COUNTER;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HA_PARALLEL_CONNECTION_ERROR_COUNTER;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HA_PARALLEL_POOL1_TASK_END_TO_END_TIME;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HA_PARALLEL_POOL1_TASK_EXECUTED_COUNTER;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HA_PARALLEL_POOL1_TASK_EXECUTION_TIME;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HA_PARALLEL_POOL1_TASK_QUEUE_WAIT_TIME;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HA_PARALLEL_POOL1_TASK_REJECTED_COUNTER;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HA_PARALLEL_POOL2_TASK_END_TO_END_TIME;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HA_PARALLEL_POOL2_TASK_EXECUTED_COUNTER;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HA_PARALLEL_POOL2_TASK_EXECUTION_TIME;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HA_PARALLEL_POOL2_TASK_QUEUE_WAIT_TIME;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HA_PARALLEL_POOL2_TASK_REJECTED_COUNTER;
import static org.apache.phoenix.query.QueryServices.AUTO_COMMIT_ATTRIB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.test.GenericTestUtils;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.ClusterRoleRecord.ClusterRole;
import org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.HBaseTestingUtilityPair;
import org.apache.phoenix.jdbc.PhoenixStatement.Operation;
import org.apache.phoenix.log.LogLevel;
import org.apache.phoenix.monitoring.GlobalMetric;
import org.apache.phoenix.monitoring.MetricType;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test failover basics for {@link ParallelPhoenixConnection}.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class ParallelPhoenixConnectionIT {
    private static final Logger LOG = LoggerFactory.getLogger(ParallelPhoenixConnectionIT.class);
    private static final HBaseTestingUtilityPair CLUSTERS = new HBaseTestingUtilityPair();
    private static final Properties GLOBAL_PROPERTIES = new Properties();

    @Rule
    public TestName testName = new TestName();

    /** Client properties to create a connection per test. */
    private Properties clientProperties;
    /** HA group for this test. */
    private HighAvailabilityGroup haGroup;
    /** Table name per test case. */
    private String tableName;
    /** HA Group name for this test. */
    private String haGroupName;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        CLUSTERS.start();
        DriverManager.registerDriver(PhoenixDriver.INSTANCE);
        GLOBAL_PROPERTIES.setProperty(AUTO_COMMIT_ATTRIB, "true");
        GLOBAL_PROPERTIES.setProperty(QueryServices.COLLECT_REQUEST_LEVEL_METRICS, String.valueOf(true));
        GLOBAL_PROPERTIES.setProperty(QueryServices.LOG_LEVEL, LogLevel.DEBUG.name()); //Need logging for query metrics
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        DriverManager.deregisterDriver(PhoenixDriver.INSTANCE);
        CLUSTERS.close();
    }

    @Before
    public void setup() throws Exception {
        haGroupName = testName.getMethodName();
        clientProperties = new Properties(GLOBAL_PROPERTIES);
        clientProperties.setProperty(PHOENIX_HA_GROUP_ATTR, haGroupName);
        // Make first cluster ACTIVE
        CLUSTERS.initClusterRole(haGroupName, PARALLEL);

        haGroup = HighAvailabilityTestingUtility.getHighAvailibilityGroup(CLUSTERS.getJdbcHAUrl(), clientProperties);
        LOG.info("Initialized haGroup {} with URL {}", haGroup, CLUSTERS.getJdbcHAUrl());
        tableName = testName.getMethodName();
        CLUSTERS.createTableOnClusterPair(tableName);
    }

    /**
     * Test Phoenix connection creation and basic operations with HBase cluster pair.
     */
    @Test
    public void testOperationUsingConnection() throws Exception {
        try (Connection conn = getParallelConnection()) {
            doTestBasicOperationsWithConnection(conn, tableName, haGroupName);
        }
    }

    @Test
    public void testUserPrincipal() throws Exception {
        try (Connection conn = getParallelConnection()) {
            ParallelPhoenixConnection pr = conn.unwrap(ParallelPhoenixConnection.class);
            ParallelPhoenixContext context = pr.getContext();
            HighAvailabilityGroup.HAGroupInfo group = context.getHaGroup().getGroupInfo();
            if (CLUSTERS.getUrl1().compareTo(CLUSTERS.getUrl2()) <= 0) {
                Assert.assertEquals(CLUSTERS.getJdbcUrl1(), group.getJDBCUrl1());
                Assert.assertEquals(CLUSTERS.getJdbcUrl2(), group.getJDBCUrl2());
            } else {
                Assert.assertEquals(CLUSTERS.getJdbcUrl2(), group.getJDBCUrl1());
                Assert.assertEquals(CLUSTERS.getJdbcUrl1(), group.getJDBCUrl2());
            }
            ConnectionQueryServices cqsi;
            // verify connection#1
            cqsi = PhoenixDriver.INSTANCE.getConnectionQueryServices(group.getJDBCUrl1(), clientProperties);
            Assert.assertEquals(HBaseTestingUtilityPair.PRINCIPAL, cqsi.getUserName());
            PhoenixConnection pConn = pr.getFutureConnection1().get();
            ConnectionQueryServices cqsiFromConn = pConn.getQueryServices();
            Assert.assertEquals(HBaseTestingUtilityPair.PRINCIPAL, cqsiFromConn.getUserName());
            Assert.assertTrue(cqsi == cqsiFromConn);
            // verify connection#2
            cqsi = PhoenixDriver.INSTANCE.getConnectionQueryServices(group.getJDBCUrl2(), clientProperties);
            Assert.assertEquals(HBaseTestingUtilityPair.PRINCIPAL, cqsi.getUserName());
            pConn = pr.getFutureConnection2().get();
            cqsiFromConn = pConn.getQueryServices();
            Assert.assertEquals(HBaseTestingUtilityPair.PRINCIPAL, cqsiFromConn.getUserName());
            Assert.assertTrue(cqsi == cqsiFromConn);
        }
    }

    /**
     * Test Phoenix connection creation and basic operations with HBase cluster(s) unavailable.
     */
    @Test
    public void testCluster1Unavailable() throws Exception {
        doTestWhenOneHBaseDown(CLUSTERS.getHBaseCluster1(), () -> {
            CLUSTERS.logClustersStates();
            try (Connection conn = getParallelConnection()) {
                doTestBasicOperationsWithConnection(conn, tableName, haGroupName);
            }
        });
    }

    /**
     * Test Phoenix connection creation and basic operations with HBase one cluster is OFFLINE role.
     */
    @Test
    public void testCluster1OfflineRole() throws Exception {
        CLUSTERS.transitClusterRole(haGroup, ClusterRole.OFFLINE, ClusterRole.ACTIVE);
        try (Connection conn = getParallelConnection()) {
            doTestBasicOperationsWithConnection(conn, tableName, haGroupName);
        }
    }

    /**
     * Test Phoenix connection creation and basic operations.
     */
    @Test
    public void testPreparedStatementsBasic() throws Exception {
        String upsertSQL = String.format("UPSERT INTO %s VALUES(?, ?)", tableName);
        try (Connection conn = getParallelConnection()) {
            PreparedStatement preparedStatement = conn.prepareStatement(upsertSQL);
            for (int i = 0; i < 100; i++) {
                preparedStatement.setInt(1,i);
                preparedStatement.setInt(2,i);

                preparedStatement.execute();
            }
            assertOperationTypeForStatement(preparedStatement, Operation.UPSERT);
        }
        CLUSTERS.checkReplicationComplete();

        //ensure values on both clusters
        try (Connection conn = CLUSTERS.getCluster1Connection();
             Statement statement = conn.createStatement();
             ResultSet rs = statement.executeQuery(String.format("SELECT COUNT(*) FROM %s", tableName))) {
            assertTrue(rs.next());
            assertEquals(100, rs.getInt(1));
        }

        //ensure values on both clusters
        try (Connection conn = CLUSTERS.getCluster2Connection();
             Statement statement = conn.createStatement();
             ResultSet rs = statement.executeQuery(String.format("SELECT COUNT(*) FROM %s", tableName))) {
            assertTrue(rs.next());
            assertEquals(100, rs.getInt(1));
        }

        //Get a few via parallel
        try (Connection conn = getParallelConnection();
             PreparedStatement preparedStatement = conn.prepareStatement(String.format("SELECT v FROM %s WHERE id IN (1,3,7,19) ",tableName));
             ResultSet rs = preparedStatement.executeQuery()) {
            assertOperationTypeForStatement(preparedStatement, Operation.QUERY);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(7, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(19, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    /**
     * Test Phoenix connection creation and basic operations.
     */
    @Test
    public void testClusterBasic() throws Exception {
        try (Connection conn = getParallelConnection()) {
            for(int i = 0; i < 100; i++) {
                try (Statement statement = conn.createStatement()) {
                    String upsertSQL = String.format("UPSERT INTO %s VALUES(%d, %d)", tableName, i,i);
                    statement.executeUpdate(upsertSQL);
                }
            }

        }
        CLUSTERS.checkReplicationComplete();

        //ensure values on both clusters
        try (Connection conn = CLUSTERS.getCluster1Connection();
             Statement statement = conn.createStatement();
             ResultSet rs = statement.executeQuery(String.format("SELECT COUNT(*) FROM %s",tableName))) {
            assertOperationTypeForStatement(statement, Operation.QUERY);
            assertTrue(rs.next());
            assertEquals(100, rs.getInt(1));
        }
        try (Connection conn = CLUSTERS.getCluster2Connection();
             Statement statement = conn.createStatement();
             ResultSet rs = statement.executeQuery(String.format("SELECT COUNT(*) FROM %s",tableName))) {
            assertTrue(rs.next());
            assertEquals(100, rs.getInt(1));
        }


        //Get a few via parallel
        try (Connection conn = getParallelConnection();
             Statement statement = conn.createStatement();
             ResultSet rs = statement.executeQuery(String.format("SELECT v FROM %s WHERE id IN (1,3,7,19) ",tableName))) {
            assertOperationTypeForStatement(statement, Operation.QUERY);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(7, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(19, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    /**
     * Test Phoenix connection post close does not allow use
     */
    @Test
    public void testClosedConnectionNotReusable() throws Exception {
        try (Connection conn = getParallelConnection()) {
            doTestBasicOperationsWithConnection(conn, tableName, haGroupName);
            Statement statement1 = conn.createStatement();
            ResultSet rs = statement1.executeQuery(String.format("SELECT v FROM %s ",tableName));

            conn.close();
            try {
                Statement statement2 = conn.createStatement();
                fail("Should not reach this point");
            } catch (Exception e) {
                LOG.error("Exception expected: ",e);
            }
            try {
                rs.next();
                fail("Should not reach this point");
            } catch (Exception e) {

            }
        }
    }

    @Test
    public void testConnectionErrorCount() throws Exception {

        doTestWhenOneHBaseDown(CLUSTERS.getHBaseCluster1(), () -> {
            CLUSTERS.logClustersStates();
            GLOBAL_HA_PARALLEL_CONNECTION_CREATED_COUNTER.getMetric().reset();
            GLOBAL_HA_PARALLEL_CONNECTION_ERROR_COUNTER.getMetric().reset();
            try (Connection conn = getParallelConnection()) {
                doTestBasicOperationsWithConnection(conn, tableName, haGroupName);
            }
            assertEquals(1,GLOBAL_HA_PARALLEL_CONNECTION_CREATED_COUNTER.getMetric().getValue());
            assertEquals(0,GLOBAL_HA_PARALLEL_CONNECTION_ERROR_COUNTER.getMetric().getValue());

            try (Connection conn = getParallelConnection()) {
                /* Determine which cluster is down from a phoenix HA point of view and close the other */

                CompletableFuture<PhoenixConnection> pConn = null;
                int downClientPort = CLUSTERS.getHBaseCluster1().getZkCluster().getClientPort();
                if(((ParallelPhoenixConnection) conn).getContext().getHaGroup().getRoleRecord().getZk1().contains(String.valueOf(downClientPort))) {
                    pConn = ((ParallelPhoenixConnection) conn).futureConnection2;
                } else {
                    pConn = ((ParallelPhoenixConnection) conn).futureConnection1;
                }

                //Close the connection this will cause "failures", may have to retry wait for this
                try {
                    pConn.get().close();
                } catch (Exception e) {
                    LOG.error("Unexpected Exception in future connection get/close",e);
                    throw e;
                }
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeQuery(String.format("SELECT v FROM %s WHERE id = %d", tableName, 0));
                }
                fail();
            } catch (Exception e) {
                //should fail
            }
            assertEquals(2,GLOBAL_HA_PARALLEL_CONNECTION_CREATED_COUNTER.getMetric().getValue());
            assertEquals(1,GLOBAL_HA_PARALLEL_CONNECTION_ERROR_COUNTER.getMetric().getValue());
        });
    }

    /**
     * Test Phoenix connection metrics.
     */
    @Test
    public void testMetrics() throws Exception {
        // CLUSTERS.disableCluster(1);
        CLUSTERS.logClustersStates();
        try (Connection conn = getParallelConnection()) {
            PhoenixRuntime.resetMetrics(conn);

            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(String.format("UPSERT INTO %s VALUES(%d, 1984)", tableName, 0));
                conn.commit();
            }
            waitForCompletion(conn);
            Map<String, Map<MetricType, Long>> metrics = PhoenixRuntime.getWriteMetricInfoForMutationsSinceLastReset(conn);

            assertEquals(2, metrics.size()); //table metrics and parallel_phoenix_metrics
            Map<MetricType, Long> parallelMetrics = metrics.get(ParallelPhoenixContext.PARALLEL_PHOENIX_METRICS);

            assertEquals(0, parallelMetrics.get(MetricType.HA_PARALLEL_COUNT_FAILED_OPERATIONS_ACTIVE_CLUSTER).longValue());
            assertEquals(0, parallelMetrics.get(MetricType.HA_PARALLEL_COUNT_FAILED_OPERATIONS_STANDBY_CLUSTER).longValue());

            //We have 5 operations, createStatement, executeUpdate, commit, statement.close
            assertEquals(4, parallelMetrics.get(MetricType.HA_PARALLEL_COUNT_OPERATIONS_ACTIVE_CLUSTER).longValue());
            assertEquals(4, parallelMetrics.get(MetricType.HA_PARALLEL_COUNT_OPERATIONS_STANDBY_CLUSTER).longValue());
            assertEquals(4, parallelMetrics.get(MetricType.HA_PARALLEL_COUNT_USED_OPERATIONS_ACTIVE_CLUSTER).longValue() + parallelMetrics.get(MetricType.HA_PARALLEL_COUNT_USED_OPERATIONS_STANDBY_CLUSTER).longValue());

            PhoenixRuntime.resetMetrics(conn);

            //Close the 1st connection this will cause "failures"
            ((ParallelPhoenixConnection) conn).futureConnection1.get().close();

            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(String.format("UPSERT INTO %s VALUES(%d, 1984)", tableName, 0));
                conn.commit();
            }

            waitForCompletion(conn);
            metrics = PhoenixRuntime.getWriteMetricInfoForMutationsSinceLastReset(conn);

            assertEquals(2, metrics.size());
            parallelMetrics = metrics.get(ParallelPhoenixContext.PARALLEL_PHOENIX_METRICS);

            // 1 failure is for the createStatement which fails since the connection is closed,
            // all the following operations don't get run due to strict chaining.
            assertEquals(1, parallelMetrics.get(MetricType.HA_PARALLEL_COUNT_FAILED_OPERATIONS_ACTIVE_CLUSTER).longValue());
            assertEquals(0, parallelMetrics.get(MetricType.HA_PARALLEL_COUNT_FAILED_OPERATIONS_STANDBY_CLUSTER).longValue());

            //We have 4 operations, createStatement, executeUpdate, commit, statement.close
            assertEquals(1, parallelMetrics.get(MetricType.HA_PARALLEL_COUNT_OPERATIONS_ACTIVE_CLUSTER).longValue());
            assertEquals(4, parallelMetrics.get(MetricType.HA_PARALLEL_COUNT_OPERATIONS_STANDBY_CLUSTER).longValue());
            assertEquals(4, parallelMetrics.get(MetricType.HA_PARALLEL_COUNT_USED_OPERATIONS_STANDBY_CLUSTER).longValue());
        }
        try (Connection conn = getParallelConnection()) {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(String.format("SELECT * FROM  %s ", tableName))) {
                rs.next();
                rs.getInt(1);
                rs.getInt(2);
                rs.next();

                Map<String, Map<MetricType, Long>> metrics = PhoenixRuntime.getRequestReadMetricInfo(rs);
                assertEquals(2, metrics.size());
                Map<MetricType, Long>  tableMetrics = metrics.get(ParallelPhoenixContext.PARALLEL_PHOENIX_METRICS);

                assertEquals(0, tableMetrics.get(MetricType.HA_PARALLEL_COUNT_FAILED_OPERATIONS_ACTIVE_CLUSTER).longValue());
                assertEquals(0, tableMetrics.get(MetricType.HA_PARALLEL_COUNT_FAILED_OPERATIONS_STANDBY_CLUSTER).longValue());

                //We have 3 operations, createStatement, executeQuery, and next but next doesn't bind so doesn't count toward the operations currently
                assertEquals(2, tableMetrics.get(MetricType.HA_PARALLEL_COUNT_OPERATIONS_ACTIVE_CLUSTER).longValue());
                assertEquals(2, tableMetrics.get(MetricType.HA_PARALLEL_COUNT_OPERATIONS_STANDBY_CLUSTER).longValue());
                assertEquals(3, tableMetrics.get(MetricType.HA_PARALLEL_COUNT_USED_OPERATIONS_ACTIVE_CLUSTER).longValue() + tableMetrics.get(MetricType.HA_PARALLEL_COUNT_USED_OPERATIONS_STANDBY_CLUSTER).longValue());

            }

        }
    }

    /**
     * Test Phoenix connection metrics when no metrics have been generated
     */
    @Test
    public void testNoMetrics() throws Exception {
        try (Connection conn = getParallelConnection()) {
            Map<String, Map<MetricType, Long>> metrics = PhoenixRuntime.getReadMetricInfoForMutationsSinceLastReset(conn);
            assertEquals(1, metrics.size());
            Map<MetricType, Long> tableMetrics = metrics.get(ParallelPhoenixContext.PARALLEL_PHOENIX_METRICS);

            waitForCompletion(conn);

            assertEquals(6, tableMetrics.size());

            assertEquals(0, tableMetrics.get(MetricType.HA_PARALLEL_COUNT_FAILED_OPERATIONS_ACTIVE_CLUSTER).longValue());
            assertEquals(0, tableMetrics.get(MetricType.HA_PARALLEL_COUNT_FAILED_OPERATIONS_STANDBY_CLUSTER).longValue());

            //We have 0 operations
            assertEquals(0, tableMetrics.get(MetricType.HA_PARALLEL_COUNT_OPERATIONS_ACTIVE_CLUSTER).longValue() +
                    tableMetrics.get(MetricType.HA_PARALLEL_COUNT_OPERATIONS_STANDBY_CLUSTER).longValue());
            assertEquals(0, tableMetrics.get(MetricType.HA_PARALLEL_COUNT_USED_OPERATIONS_ACTIVE_CLUSTER).longValue() +
                    tableMetrics.get(MetricType.HA_PARALLEL_COUNT_USED_OPERATIONS_STANDBY_CLUSTER).longValue());
        }
    }

    @Test
    public void testGlobalClientExecutorServiceMetrics() throws Exception {
        try (Connection conn = getParallelConnection()) {
            resetGlobalClientMetrics();

            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(String.format("UPSERT INTO %s VALUES(%d, 1984)", tableName, 0));
                conn.commit();
            }
            assertTrue(conn instanceof ParallelPhoenixConnection);
            ParallelPhoenixContext context = ((ParallelPhoenixConnection) conn).getContext();
            waitFor(() -> context.getChainOnConn1().isDone(), 100, 5000);
            waitFor(() -> context.getChainOnConn2().isDone(), 100, 5000);
            assertTrue(GLOBAL_HA_PARALLEL_POOL1_TASK_EXECUTED_COUNTER.getMetric().getValue() > 0);
            assertTrue(
                    GLOBAL_HA_PARALLEL_POOL1_TASK_QUEUE_WAIT_TIME.getMetric().getNumberOfSamples() > 0);
            assertTrue(GLOBAL_HA_PARALLEL_POOL1_TASK_QUEUE_WAIT_TIME.getMetric().getValue() >= 0);
            assertTrue(
                    GLOBAL_HA_PARALLEL_POOL1_TASK_END_TO_END_TIME.getMetric().getNumberOfSamples() > 0);
            assertTrue(GLOBAL_HA_PARALLEL_POOL1_TASK_END_TO_END_TIME.getMetric().getValue() >= 0);
            assertTrue(GLOBAL_HA_PARALLEL_POOL1_TASK_EXECUTION_TIME.getMetric().getNumberOfSamples() > 0);
            assertTrue(GLOBAL_HA_PARALLEL_POOL1_TASK_EXECUTION_TIME.getMetric().getValue() >= 0);
            assertEquals(0, GLOBAL_HA_PARALLEL_POOL1_TASK_REJECTED_COUNTER.getMetric().getValue());

            assertTrue(GLOBAL_HA_PARALLEL_POOL2_TASK_EXECUTED_COUNTER.getMetric().getValue() > 0);
            assertTrue(
                GLOBAL_HA_PARALLEL_POOL2_TASK_QUEUE_WAIT_TIME.getMetric().getNumberOfSamples() > 0);
            assertTrue(GLOBAL_HA_PARALLEL_POOL2_TASK_QUEUE_WAIT_TIME.getMetric().getValue() >= 0);
            assertTrue(
                GLOBAL_HA_PARALLEL_POOL2_TASK_END_TO_END_TIME.getMetric().getNumberOfSamples() > 0);
            assertTrue(GLOBAL_HA_PARALLEL_POOL2_TASK_END_TO_END_TIME.getMetric().getValue() >= 0);
            assertTrue(
                GLOBAL_HA_PARALLEL_POOL2_TASK_EXECUTION_TIME.getMetric().getNumberOfSamples() > 0);
            assertTrue(GLOBAL_HA_PARALLEL_POOL2_TASK_EXECUTION_TIME.getMetric().getValue() >= 0);
            assertEquals(0, GLOBAL_HA_PARALLEL_POOL2_TASK_REJECTED_COUNTER.getMetric().getValue());
        }
    }

    private void resetGlobalClientMetrics() {
        for (GlobalMetric m : PhoenixRuntime.getGlobalPhoenixClientMetrics()) {
            m.reset();
        }
    }

    /**
     * Test Phoenix connection metadata differs but no issue
     */
    @Test
    public void testSeparateMetadata() throws Exception {
        //make a table on the 2nd cluster
        String tableName = "TABLE_" + testName.getMethodName();
        try(Connection conn = CLUSTERS.getCluster2Connection()) {

            String ddl = "CREATE TABLE " + tableName + " ( MYKEY VARCHAR NOT NULL, MYVALUE VARCHAR CONSTRAINT PK_DATA PRIMARY KEY (MYKEY))";
            try(Statement stmt = conn.createStatement()) {
                stmt.execute(ddl);
            }
            try(Statement stmt = conn.createStatement()){
                stmt.execute("UPSERT INTO " + tableName + " VALUES('hi','bye')");
            }
            conn.commit();
        }

        //Get a few via parallel
        try (Connection conn = getParallelConnection();
             Statement statement = conn.createStatement();
             ResultSet rs = statement.executeQuery(String.format("SELECT * FROM %s",tableName))) {
            assertTrue(rs.next());
        }
    }

    /**
     * Helper method to test Operation type of Phoenix Statement provided by
     * PhoenixMonitoredStatement#getUpdateOperation method.
     */
    private static void assertOperationTypeForStatement(Statement statement, Operation expectedUpdateOpType) throws SQLException {
        PhoenixMonitoredStatement stmt = statement.unwrap(PhoenixMonitoredStatement.class);
        assertEquals(expectedUpdateOpType, stmt.getUpdateOperation());
    }

    /**
     * Returns a Parallel Phoenix Connection
     * @return Parallel Phoenix Connection
     * @throws SQLException
     */
    private Connection getParallelConnection() throws SQLException {
        return DriverManager.getConnection(CLUSTERS.getJdbcHAUrl(), clientProperties);
    }

    void waitForCompletion(Connection conn) throws Exception {
        ParallelPhoenixContext context = ((ParallelPhoenixConnection) conn).getContext();
        Thread.sleep(200);
        GenericTestUtils.waitFor(() -> context.getChainOnConn1().isDone(),100,30000);
        GenericTestUtils.waitFor(() -> context.getChainOnConn2().isDone(),100,30000);
        //The final metrics for selection are actually outside of the chain so for now adding a sleep
        Thread.sleep(200);
    }
}
