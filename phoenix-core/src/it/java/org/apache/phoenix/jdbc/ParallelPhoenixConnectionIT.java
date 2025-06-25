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
import static org.apache.phoenix.query.BaseTest.extractThreadPoolExecutorFromCQSI;
import static org.apache.phoenix.query.QueryServices.AUTO_COMMIT_ATTRIB;
import static org.apache.phoenix.query.QueryServices.CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED;
import static org.apache.phoenix.query.QueryServices.CQSI_THREAD_POOL_ALLOW_CORE_THREAD_TIMEOUT;
import static org.apache.phoenix.query.QueryServices.CQSI_THREAD_POOL_CORE_POOL_SIZE;
import static org.apache.phoenix.query.QueryServices.CQSI_THREAD_POOL_ENABLED;
import static org.apache.phoenix.query.QueryServices.CQSI_THREAD_POOL_KEEP_ALIVE_SECONDS;
import static org.apache.phoenix.query.QueryServices.CQSI_THREAD_POOL_MAX_QUEUE;
import static org.apache.phoenix.query.QueryServices.CQSI_THREAD_POOL_MAX_THREADS;
import static org.apache.phoenix.query.QueryServices.CQSI_THREAD_POOL_METRICS_ENABLED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.doAnswer;

import java.sql.Connection;
import java.lang.reflect.Field;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.exception.MutationBlockedIOException;
import org.apache.phoenix.jdbc.ClusterRoleRecord.ClusterRole;
import org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.HBaseTestingUtilityPair;
import org.apache.phoenix.jdbc.PhoenixStatement.Operation;
import org.apache.phoenix.log.LogLevel;
import org.apache.phoenix.monitoring.GlobalMetric;
import org.apache.phoenix.monitoring.HTableThreadPoolHistograms;
import org.apache.phoenix.monitoring.HTableThreadPoolMetricsManager;
import org.apache.phoenix.monitoring.HistogramDistribution;
import org.apache.phoenix.monitoring.MetricType;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.ConnectionQueryServicesImpl;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;
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
        CLUSTERS.getHBaseCluster1().getConfiguration().setBoolean(CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED, true);
        CLUSTERS.getHBaseCluster2().getConfiguration().setBoolean(CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED, true);
        CLUSTERS.getHBaseCluster1().getConfiguration().setInt("hbase.client.retries.number", 0);
        CLUSTERS.getHBaseCluster2().getConfiguration().setInt("hbase.client.retries.number", 0);
        CLUSTERS.start();
        DriverManager.registerDriver(PhoenixDriver.INSTANCE);
        DriverManager.registerDriver(new PhoenixTestDriver());
        GLOBAL_PROPERTIES.setProperty(AUTO_COMMIT_ATTRIB, "true");
        GLOBAL_PROPERTIES.setProperty(QueryServices.COLLECT_REQUEST_LEVEL_METRICS, String.valueOf(true));
        GLOBAL_PROPERTIES.setProperty(QueryServices.LOG_LEVEL, LogLevel.DEBUG.name()); //Need logging for query metrics
        GLOBAL_PROPERTIES.setProperty(CQSI_THREAD_POOL_ENABLED, String.valueOf(true));
        GLOBAL_PROPERTIES.setProperty(CQSI_THREAD_POOL_KEEP_ALIVE_SECONDS, String.valueOf(13));
        GLOBAL_PROPERTIES.setProperty(CQSI_THREAD_POOL_CORE_POOL_SIZE, String.valueOf(17));
        GLOBAL_PROPERTIES.setProperty(CQSI_THREAD_POOL_MAX_THREADS, String.valueOf(19));
        GLOBAL_PROPERTIES.setProperty(CQSI_THREAD_POOL_MAX_QUEUE, String.valueOf(23));
        GLOBAL_PROPERTIES.setProperty(CQSI_THREAD_POOL_ALLOW_CORE_THREAD_TIMEOUT,
            String.valueOf(true));
        GLOBAL_PROPERTIES.setProperty("hbase.client.retries.number", "0");
        GLOBAL_PROPERTIES.setProperty(CQSI_THREAD_POOL_METRICS_ENABLED, String.valueOf(true));
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
        tableName = testName.getMethodName().toUpperCase();
        CLUSTERS.createTableOnClusterPair(haGroup, tableName);
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
            PhoenixConnection pConn;
            PhoenixConnection pConn2;
            if (CLUSTERS.getJdbcUrl1(haGroup).equals(pr.getFutureConnection1().get().getURL())) {
                assertEquals(CLUSTERS.getJdbcUrl2(haGroup), pr.getFutureConnection2().get().getURL());
                pConn = pr.getFutureConnection1().get();
                pConn2 = pr.getFutureConnection2().get();
            } else {
                assertEquals(CLUSTERS.getJdbcUrl1(haGroup), pr.getFutureConnection2().get().getURL());
                assertEquals(CLUSTERS.getJdbcUrl2(haGroup), pr.getFutureConnection1().get().getURL());
                pConn = pr.getFutureConnection2().get();
                pConn2 = pr.getFutureConnection1().get();
            }

            ConnectionQueryServices cqsi;
            // verify connection#1
            cqsi = PhoenixDriver.INSTANCE.getConnectionQueryServices(CLUSTERS.getJdbcUrl1(haGroup), clientProperties);
            Assert.assertEquals(HBaseTestingUtilityPair.PRINCIPAL, cqsi.getUserName());
            ConnectionQueryServices cqsiFromConn = pConn.getQueryServices();
            Assert.assertEquals(HBaseTestingUtilityPair.PRINCIPAL, cqsiFromConn.getUserName());
            Assert.assertSame(cqsi, cqsiFromConn);
            // verify connection#2
            cqsi = PhoenixDriver.INSTANCE.getConnectionQueryServices(CLUSTERS.getJdbcUrl2(haGroup), clientProperties);
            Assert.assertEquals(HBaseTestingUtilityPair.PRINCIPAL, cqsi.getUserName());
            cqsiFromConn = pConn2.getQueryServices();
            Assert.assertEquals(HBaseTestingUtilityPair.PRINCIPAL, cqsiFromConn.getUserName());
            Assert.assertSame(cqsi, cqsiFromConn);
        }
    }

    @Test
    public void testDifferentCQSIThreadPoolsForParallelConnection() throws Exception {
        try (Connection conn = getParallelConnection()) {
            ParallelPhoenixConnection pr = conn.unwrap(ParallelPhoenixConnection.class);
            PhoenixConnection pConn;
            PhoenixConnection pConn2;
            if (CLUSTERS.getJdbcUrl1(haGroup).equals(pr.getFutureConnection1().get().getURL())) {
                pConn = pr.getFutureConnection1().get();
                pConn2 = pr.getFutureConnection2().get();
            } else {
                pConn = pr.getFutureConnection2().get();
                pConn2 = pr.getFutureConnection1().get();
            }

            // verify connection#1
            ConnectionQueryServices cqsi = PhoenixDriver.INSTANCE.getConnectionQueryServices(CLUSTERS.getJdbcUrl1(haGroup), clientProperties);
            ConnectionQueryServices cqsiFromConn = pConn.getQueryServices();
            // Check that same ThreadPoolExecutor object is used for CQSIs
            ThreadPoolExecutor threadPoolExecutor1 = extractThreadPoolExecutorFromCQSI(cqsi);
            Assert.assertSame(threadPoolExecutor1, extractThreadPoolExecutorFromCQSI(cqsiFromConn));

            // verify connection#2
            cqsi = PhoenixDriver.INSTANCE.getConnectionQueryServices(CLUSTERS.getJdbcUrl2(haGroup), clientProperties);
            cqsiFromConn = pConn2.getQueryServices();
            Assert.assertSame(cqsi, cqsiFromConn);
            // Check that same ThreadPoolExecutor object is used for CQSIs
            ThreadPoolExecutor threadPoolExecutor2 = extractThreadPoolExecutorFromCQSI(cqsi);
            Assert.assertSame(extractThreadPoolExecutorFromCQSI(cqsi), extractThreadPoolExecutorFromCQSI(cqsiFromConn));

            // Check that both threadPools for parallel connections are different.
            assertNotSame(threadPoolExecutor1, threadPoolExecutor2);
        }
    }

    @Test
    public void testCqsiThreadPoolMetricsForParallelConnection() throws Exception {
        try (Connection conn = getParallelConnection()) {
            ParallelPhoenixConnection pr = conn.unwrap(ParallelPhoenixConnection.class);

            // Get details of connection#1
            PhoenixConnection pConn1 = pr.getFutureConnection1().get();
            Configuration config1 = pConn1.getQueryServices().getConfiguration();
            String zkQuorum1 = config1.get(HConstants.ZOOKEEPER_QUORUM);
            String principal1 = config1.get(QueryServices.QUERY_SERVICES_NAME);

            // Get details of connection#2
            PhoenixConnection pConn2 = pr.getFutureConnection2().get();
            Configuration config2 = pConn2.getQueryServices().getConfiguration();
            String zkQuorum2 = config2.get(HConstants.ZOOKEEPER_QUORUM);
            String principal2 = config2.get(QueryServices.QUERY_SERVICES_NAME);

            // Slow down connection#1
            CountDownLatch latch = new CountDownLatch(1);
            slowDownConnection(pr, pr.getFutureConnection1(), "futureConnection1", latch);

            try (Statement stmt = conn.createStatement()) {
                HTableThreadPoolMetricsManager.getHistogramsForAllThreadPools();
                try (ResultSet rs =
                    stmt.executeQuery(String.format("SELECT COUNT(*) FROM %s", tableName))) {
                    assertTrue(rs.next());
                    assertEquals(0, rs.getInt(1));
                    assertFalse(rs.next());
                }

                Map<String, List<HistogramDistribution>> htableHistograms =
                    HTableThreadPoolMetricsManager.getHistogramsForAllThreadPools();

                // Assert connection#1 CQSI thread pool metrics
                String conn1HistogramKey = getHistogramKey(config1);
                assertHTableThreadPoolHistograms(htableHistograms.get(conn1HistogramKey),
                    conn1HistogramKey, false, zkQuorum1, principal1);

                // Assert connection#2 CQSI thread pool metrics
                String conn2HistogramKey = getHistogramKey(config2);
                assertHTableThreadPoolHistograms(htableHistograms.get(conn2HistogramKey),
                    conn2HistogramKey, true, zkQuorum2, principal2);

                // Assert that the CQSI thread pool metrics for both connections are different
                Assert.assertNotEquals(conn1HistogramKey, conn2HistogramKey);
            } finally {
                latch.countDown();
            }
        }
    }

    private void slowDownConnection(ParallelPhoenixConnection pr,
        CompletableFuture<PhoenixConnection> pConn, String futureConnectionField,
        CountDownLatch latch) throws Exception {
        Assert.assertTrue(futureConnectionField.equals("futureConnection1")
            || futureConnectionField.equals("futureConnection2"));

        PhoenixConnection spy = Mockito.spy(pConn.get());
        doAnswer((invocation) -> {
            // Block the statement creation until the latch is counted down
            latch.await();
            return invocation.callRealMethod();
        }).when(spy).createStatement();

        // Replace the existing CompletableFuture with the spied CompletableFuture
        Field futureField = ParallelPhoenixConnection.class.getDeclaredField(futureConnectionField);
        futureField.setAccessible(true);
        CompletableFuture<PhoenixConnection> spiedFuture = CompletableFuture.completedFuture(spy);
        futureField.set(pr, spiedFuture);

        // Verify that the spied CompletableFuture has been setup correctly
        if (futureConnectionField.equals("futureConnection1")) {
            Assert.assertSame(spy, pr.getFutureConnection1().get());
        } else {
            Assert.assertSame(spy, pr.getFutureConnection2().get());
        }
    }

    private String getHistogramKey(Configuration config) throws SQLException {
        String url =
            QueryUtil.getConnectionUrl(clientProperties, config, HBaseTestingUtilityPair.PRINCIPAL);
        return ConnectionInfo.createNoLogin(url, null, null).toUrl();
    }

    private void assertHTableThreadPoolHistograms(List<HistogramDistribution> histograms,
        String histogramKey, boolean isUsed, String zkQuorum, String principal) {
        Assert.assertNotNull(histograms);
        assertEquals(2, histograms.size());
        for (HistogramDistribution histogram : histograms) {
            if (isUsed) {
                assertTrue(histogram.getCount() > 0);
            } else {
                assertEquals(0, histogram.getCount());
            }
            assertEquals(zkQuorum,
                histogram.getTags().get(HTableThreadPoolHistograms.Tag.servers.name()));
            assertEquals(principal,
                histogram.getTags().get(HTableThreadPoolHistograms.Tag.cqsiName.name()));
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
     * Test Phoenix connection creation and basic operations with HBase both cluster is ACTIVE_TO_STANDBY role.
     */
    @Test
    public void testBothClusterATSRole() throws Exception {
        CLUSTERS.transitClusterRole(haGroup, ClusterRole.ACTIVE_TO_STANDBY, ClusterRole.ACTIVE_TO_STANDBY);
        try (Connection conn = getParallelConnection()) {
            doTestBasicOperationsWithConnection(conn, tableName, haGroupName);
            fail("Expected MutationBlockedIOException to be thrown");
        } catch (SQLException e) {
            assertTrue(containsMutationBlockedException(e));
        } finally {
            CLUSTERS.transitClusterRole(haGroup, ClusterRole.ACTIVE, ClusterRole.STANDBY);
        }
    }

    /**
     * Test Phoenix connection creation and
     * basic operations with HBase one cluster is ACTIVE_TO_STANDBY role
     * and other in ACTIVE role.
     */
    @Test
    public void testOneClusterATSRoleWithActive() throws Exception {
        testOneClusterATSRole(ClusterRole.ACTIVE);
    }

    /**
     * Test Phoenix connection creation and
     * basic operations with HBase one cluster is ACTIVE_TO_STANDBY role
     * and other in STANDBY role.
     */
    @Test
    public void testOneClusterATSRoleWithStandby() throws Exception {
        testOneClusterATSRole(ClusterRole.STANDBY);
    }

    private void testOneClusterATSRole(ClusterRole otherRole) throws Exception {
        CLUSTERS.transitClusterRole(haGroup, ClusterRole.ACTIVE_TO_STANDBY, otherRole);
        try (Connection conn = getParallelConnection()) {
            doTestBasicOperationsWithConnection(conn, tableName, haGroupName);
        } catch (SQLException e) {
            fail("Expected no exception to be thrown as one cluster is "
                    + "in ACTIVE_TO_STANDBY and other in " + otherRole);
        } finally {
            CLUSTERS.transitClusterRole(haGroup, ClusterRole.ACTIVE, ClusterRole.STANDBY);
        }
    }

    private boolean containsMutationBlockedException(SQLException e) {
        Throwable cause = e.getCause();
        // Recursively check for MutationBlockedIOException buried in exception stack.
        while (cause != null) {
            if (cause instanceof RetriesExhaustedWithDetailsException) {
                RetriesExhaustedWithDetailsException re = (RetriesExhaustedWithDetailsException) cause;
                return re.getCause(0) instanceof MutationBlockedIOException;
            }
            cause = cause.getCause();
        }
        return false;
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
        try (Connection conn = CLUSTERS.getCluster1Connection(haGroup);
             Statement statement = conn.createStatement();
             ResultSet rs = statement.executeQuery(String.format("SELECT COUNT(*) FROM %s", tableName))) {
            assertTrue(rs.next());
            assertEquals(100, rs.getInt(1));
        }

        //ensure values on both clusters
        try (Connection conn = CLUSTERS.getCluster2Connection(haGroup);
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
        try (Connection conn = CLUSTERS.getCluster1Connection(haGroup);
             Statement statement = conn.createStatement();
             ResultSet rs = statement.executeQuery(String.format("SELECT COUNT(*) FROM %s",tableName))) {
            assertOperationTypeForStatement(statement, Operation.QUERY);
            assertTrue(rs.next());
            assertEquals(100, rs.getInt(1));
        }
        try (Connection conn = CLUSTERS.getCluster2Connection(haGroup);
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
                if(((ParallelPhoenixConnection) conn).getContext().getHaGroup().getRoleRecord().
                        getUrl1().contains(String.valueOf(downClientPort))) {
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
        String tableName = "TABLE_" + RandomStringUtils.randomAlphabetic(10);
        try(Connection conn = CLUSTERS.getCluster2Connection(haGroup)) {

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
     * This is to make sure all Phoenix connections are closed when registryType changes
     * of a CRR , ZK --> MASTER
     *
     * Test with many connections.
     */
    @Test
    public void testAllWrappedConnectionsClosedAfterRegistryChangeToMaster() throws Exception {
        short numberOfConnections = 10;
        List<Connection> connectionList = new ArrayList<>(numberOfConnections);
        for (short i = 0; i < numberOfConnections; i++) {
            connectionList.add(getParallelConnection());
        }
        ConnectionQueryServicesImpl cqsi = (ConnectionQueryServicesImpl) PhoenixDriver.INSTANCE.
                getConnectionQueryServices(CLUSTERS.getJdbcUrl1(haGroup), clientProperties);
        ConnectionInfo connInfo = ConnectionInfo.create(CLUSTERS.getJdbcUrl1(haGroup),
                PhoenixDriver.INSTANCE.getQueryServices().getProps(), clientProperties);

        ClusterRoleRecord.RegistryType newRegistry = ClusterRoleRecord.RegistryType.MASTER;
        CLUSTERS.transitClusterRoleRecordRegistry(haGroup, newRegistry);

        for (short i = 0; i < numberOfConnections; i++) {
            LOG.info("Asserting connection number {}", i);
            ParallelPhoenixConnection conn = ((ParallelPhoenixConnection) connectionList.get(i));
            assertFalse(conn.isClosed());
            assertTrue(conn.futureConnection1.get().isClosed());
            assertTrue(conn.futureConnection2.get().isClosed());
        }
        //CQSI should be closed
        try {
            cqsi.checkClosed();
            fail("Should have thrown an exception as cqsi should be closed");
        } catch (IllegalStateException e) {
            //Exception cqsi should have been invalidated as well
            assertFalse(PhoenixDriver.INSTANCE.checkIfCQSIIsInCache(connInfo));
        } catch (Exception e) {
            fail("Should have thrown on IllegalStateException as cqsi should be closed");
        }
    }

    /**
     * This is to make sure all Phoenix connections are closed when registryType changes
     * of a CRR. ZK --> RPC
     *
     * Test with many connections.
     */
    @Test(timeout = 300000)
    public void testAllWrappedConnectionsClosedAfterRegistryChangeToRpc() throws Exception {
        short numberOfConnections = 10;
        List<Connection> connectionList = new ArrayList<>(numberOfConnections);
        for (short i = 0; i < numberOfConnections; i++) {
            connectionList.add(getParallelConnection());
        }
        ConnectionQueryServicesImpl cqsi = (ConnectionQueryServicesImpl) PhoenixDriver.INSTANCE.
                getConnectionQueryServices(CLUSTERS.getJdbcUrl1(haGroup), clientProperties);
        ConnectionInfo connInfo = ConnectionInfo.create(CLUSTERS.getJdbcUrl1(haGroup),
                PhoenixDriver.INSTANCE.getQueryServices().getProps(), clientProperties);

        ClusterRoleRecord.RegistryType newRegistry = ClusterRoleRecord.RegistryType.RPC;
        //RPC Registry is only there in hbase version greater than 2.5.0
        assumeTrue(VersionInfo.compareVersion(VersionInfo.getVersion(), "2.5.0")>=0);
        CLUSTERS.transitClusterRoleRecordRegistry(haGroup, newRegistry);

        for (short i = 0; i < numberOfConnections; i++) {
            LOG.info("Asserting connection number {}", i);
            ParallelPhoenixConnection conn = ((ParallelPhoenixConnection) connectionList.get(i));
            assertFalse(conn.isClosed());
            assertTrue(conn.futureConnection1.get().isClosed());
            assertTrue(conn.futureConnection2.get().isClosed());
        }
        //CQSI should be closed
        try {
            cqsi.checkClosed();
            fail("Should have thrown an exception as cqsi should be closed");
        } catch (IllegalStateException e) {
            //Exception cqsi should have been invalidated as well
            assertFalse(PhoenixDriver.INSTANCE.checkIfCQSIIsInCache(connInfo));
        } catch (Exception e) {
            fail("Should have thrown on IllegalStateException as cqsi should be closed");
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
