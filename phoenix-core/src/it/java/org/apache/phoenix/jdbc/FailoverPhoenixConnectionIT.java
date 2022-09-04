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
import static org.apache.phoenix.exception.SQLExceptionCode.CANNOT_ESTABLISH_CONNECTION;
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.HBaseTestingUtilityPair.doTestWhenOneZKDown;
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.doTestBasicOperationsWithConnection;
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.HBaseTestingUtilityPair;
import static org.apache.phoenix.jdbc.HighAvailabilityGroup.PHOENIX_HA_GROUP_ATTR;
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.doTestBasicOperationsWithStatement;
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.getHighAvailibilityGroup;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.exception.FailoverSQLException;
import org.apache.phoenix.jdbc.ClusterRoleRecord.ClusterRole;
import org.apache.phoenix.monitoring.MetricType;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.ConnectionQueryServicesImpl;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test failover basics for {@link FailoverPhoenixConnection}.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class FailoverPhoenixConnectionIT {
    private static final Logger LOG = LoggerFactory.getLogger(FailoverPhoenixConnectionIT.class);
    private static final HBaseTestingUtilityPair CLUSTERS = new HBaseTestingUtilityPair();

    @Rule
    public final TestName testName = new TestName();

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
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        DriverManager.deregisterDriver(PhoenixDriver.INSTANCE);
        CLUSTERS.close();
    }

    @Before
    public void setup() throws Exception {
        haGroupName = testName.getMethodName();
        clientProperties = HighAvailabilityTestingUtility.getHATestProperties();
        clientProperties.setProperty(PHOENIX_HA_GROUP_ATTR, haGroupName);

        // Make first cluster ACTIVE
        CLUSTERS.initClusterRole(haGroupName, HighAvailabilityPolicy.FAILOVER);

        haGroup = getHighAvailibilityGroup(CLUSTERS.getJdbcUrl(), clientProperties);
        LOG.info("Initialized haGroup {} with URL {}", haGroup, CLUSTERS.getJdbcUrl());
        tableName = testName.getMethodName().toUpperCase();
        CLUSTERS.createTableOnClusterPair(tableName);
    }

    @After
    public void tearDown() throws Exception {
        try {
            haGroup.close();
            PhoenixDriver.INSTANCE
                    .getConnectionQueryServices(CLUSTERS.getUrl1(), haGroup.getProperties())
                    .close();
            PhoenixDriver.INSTANCE
                    .getConnectionQueryServices(CLUSTERS.getUrl2(), haGroup.getProperties())
                    .close();
        } catch (Exception e) {
            LOG.error("Fail to tear down the HA group and the CQS. Will ignore", e);
        }
    }

    /**
     * Test Phoenix connection creation and basic operations with HBase cluster pair.
     */
    @Test(timeout = 300000)
    public void testOperationUsingConnection() throws Exception {
        try (Connection conn = createFailoverConnection()) {
            doTestBasicOperationsWithConnection(conn, tableName, haGroupName);
        }
    }

    /**
     * Test close() once more should not fail, as the second close should be a no-op.
     */
    @Test(timeout = 300000)
    public void testCloseConnectionOnceMore() throws Exception {
        Connection conn = createFailoverConnection();
        doTestBasicOperationsWithConnection(conn, tableName, haGroupName);
        conn.close();
        conn.close(); // this is NOT duplicate code, but instead this is essential for this test.
    }

    /**
     * Tests that new Phoenix connections are not created during failover.
     */
    @Test(timeout = 300000)
    public void testConnectionCreationFailsIfNoActiveCluster() throws Exception {
        try (Connection conn = createFailoverConnection()) {
            doTestBasicOperationsWithConnection(conn, tableName, haGroupName);
        }

        CLUSTERS.transitClusterRole(haGroup, ClusterRole.STANDBY, ClusterRole.STANDBY);

        try {
            createFailoverConnection();
            fail("Should have failed because neither cluster is ACTIVE");
        } catch (SQLException e) {
            LOG.info("Got expected exception when creating new connection", e);
            assertEquals(CANNOT_ESTABLISH_CONNECTION.getErrorCode(), e.getErrorCode());
        } // all other type of exception will fail this test.
    }

    /**
     * Tests new Phoenix connections are created if one cluster is OFFLINE and the other ACTIVE.
     */
    @Test(timeout = 300000)
    public void testConnectionOneOfflineOneActive() throws Exception {
        CLUSTERS.transitClusterRole(haGroup, ClusterRole.OFFLINE, ClusterRole.ACTIVE);

        try (Connection conn = createFailoverConnection()) {
            doTestBasicOperationsWithConnection(conn, tableName, haGroupName);
        }
    }

    /**
     * Test that failover can finish according to the policy.
     *
     * In this test case, there is no existing CQS or open connections to close.
     *
     * @see #testFailoverCanFinishWhenOneZKDownWithCQS
     */
    @Test(timeout = 300000)
    public void testFailoverCanFinishWhenOneZKDown() throws Exception {
        doTestWhenOneZKDown(CLUSTERS.getHBaseCluster1(), () -> {
            // Because cluster1 is down, any version record will only be updated in cluster2.
            // Become ACTIVE cluster is still ACTIVE in current version record, no CQS to be closed.
            CLUSTERS.transitClusterRole(haGroup, ClusterRole.ACTIVE, ClusterRole.OFFLINE);

            // Usually making this cluster STANDBY will close the CQS and all existing connections.
            // The HA group was created in setup() but no CQS is yet opened for this ACTIVE cluster.
            // As a result there is neither the CQS nor existing opened connections.
            // In this case, the failover should finish instead of failing to get/close the CQS.
            CLUSTERS.transitClusterRole(haGroup, ClusterRole.STANDBY, ClusterRole.ACTIVE);

            try (Connection conn = createFailoverConnection()) {
                FailoverPhoenixConnection failoverConn = (FailoverPhoenixConnection) conn;
                assertEquals(CLUSTERS.getUrl2(), failoverConn.getWrappedConnection().getURL());
                doTestBasicOperationsWithConnection(conn, tableName, haGroupName);
            }
        });
    }

    /**
     * Test that policy can close bad CQS and then finish failover according to the policy.
     *
     * Internally, the failover will get the CQS for this HA group and try to close all connections.
     * However, the CQS may be in a bad state since the target ZK cluster is down. This test is for
     * the scenario that when CQS is in bad state, failover should finish transition so HA is ready.
     *
     * @see #testFailoverCanFinishWhenOneZKDown
     */
    @Test(timeout = 300000)
    public void testFailoverCanFinishWhenOneZKDownWithCQS() throws Exception {
        doTestWhenOneZKDown(CLUSTERS.getHBaseCluster1(), () -> {
            // Try to make a connection to current ACTIVE cluster, which should fail.
            try {
                createFailoverConnection();
                fail("Should have failed since ACTIVE ZK '" + CLUSTERS.getUrl1() + "' is down");
            } catch (SQLException e) {
                LOG.info("Got expected exception when ACTIVE ZK cluster is down", e);
            }

            // As the target ZK is down, now existing CQS if any is in a bad state. In this case,
            // the failover should still finish. After all, this is the most important use case the
            // failover is designed for.
            CLUSTERS.transitClusterRole(haGroup, ClusterRole.STANDBY, ClusterRole.ACTIVE);

            try (Connection conn = createFailoverConnection()) {
                FailoverPhoenixConnection failoverConn = (FailoverPhoenixConnection) conn;
                assertEquals(CLUSTERS.getUrl2(), failoverConn.getWrappedConnection().getURL());
                doTestBasicOperationsWithConnection(conn, tableName, haGroupName);
            }
        });
    }

    /**
     * Tests the scenario where ACTIVE ZK cluster restarts.
     *
     * When ACTIVE ZK cluster shutdown, client can not connect to this HA group.
     * After failover, client can connect to this HA group (against the new ACTIVE cluster2).
     * After restarts and fails back, client can connect to this HA group (against cluster 1).
     */
    @Test(timeout = 300000)
    public void testConnectionWhenActiveZKRestarts() throws Exception {
        doTestWhenOneZKDown(CLUSTERS.getHBaseCluster1(), () -> {
            try {
                createFailoverConnection();
                fail("Should have failed since ACTIVE ZK cluster was shutdown");
            } catch (SQLException e) {
                LOG.info("Got expected exception when ACTIVE ZK cluster is down", e);
            }

            // So on-call engineer comes into play, and triggers a failover
            // Because cluster1 is down, new version record will only be updated in cluster2
            CLUSTERS.transitClusterRole(haGroup, ClusterRole.STANDBY, ClusterRole.ACTIVE);

            // After failover, the FailoverPhoenixConnection should go to the second cluster
            try (Connection conn = createFailoverConnection()) {
                doTestBasicOperationsWithConnection(conn, tableName, haGroupName);
            }
        });

        // After cluster1 comes back, new connection should still go to cluster2.
        // This is the case where ZK1 believes itself is ACTIVE, with role record v1
        // while ZK2 believes itself is ACTIVE, with role record v2.
        // Client should believe ZK2 is ACTIVE in this case because high version wins.
        LOG.info("Testing failover connection when both clusters are up and running");
        try (Connection conn = createFailoverConnection()) {
            FailoverPhoenixConnection failoverConn = conn.unwrap(FailoverPhoenixConnection.class);
            assertEquals(CLUSTERS.getUrl2(), failoverConn.getWrappedConnection().getURL());
            doTestBasicOperationsWithConnection(conn, tableName, haGroupName);
        }

        LOG.info("Testing failover back to cluster1 when bot clusters are up and running");
        // Failover back to the first cluster since it is healthy after restart
        CLUSTERS.transitClusterRole(haGroup, ClusterRole.ACTIVE, ClusterRole.STANDBY);
        // After ACTIVE ZK restarts and fail back, this should still work
        try (Connection conn = createFailoverConnection()) {
            doTestBasicOperationsWithConnection(conn, tableName, haGroupName);
        }
    }

    /**
     * Tests the scenario where STANDBY ZK cluster restarts.
     *
     * When STANDBY ZK cluster shutdown, client can still connect to this HA group.
     * After failover, client can not connect to this HA group (ACTIVE cluster  2 is down).
     * After cluster 2 (ACTIVE) restarts, client can connect to this HA group.
     */
    @Test(timeout = 300000)
    public void testConnectionWhenStandbyZKRestarts() throws Exception {
        doTestWhenOneZKDown(CLUSTERS.getHBaseCluster2(), () -> {
            try (Connection conn = createFailoverConnection()) {
                doTestBasicOperationsWithConnection(conn, tableName, haGroupName);
            }

            // Innocent on-call engineer triggers a failover to second cluster
            CLUSTERS.transitClusterRole(haGroup, ClusterRole.STANDBY, ClusterRole.ACTIVE);

            try {
                createFailoverConnection();
                fail("Should have failed since ACTIVE ZK cluster was shutdown");
            } catch (SQLException e) {
                LOG.info("Got expected exception when ACTIVE ZK cluster {} was shutdown",
                        CLUSTERS.getUrl2(), e);
            }
        });

        // After the second cluster (ACTIVE) ZK restarts, this should still work
        try (Connection conn = createFailoverConnection()) {
            doTestBasicOperationsWithConnection(conn, tableName, haGroupName);
        }
    }

    /**
     * Tests the scenario where two ZK clusters both restart.
     *
     * Client can not connect to this HA group when two ZK clusters are both down.
     * When STANDBY ZK cluster2 first restarts, client still can not connect to this HA group.
     * After failover, client can connect to this HA group because cluster 2 is ACTIVE and healthy.
     */
    @Test(timeout = 300000)
    public void testConnectionWhenTwoZKRestarts() throws Exception {
      doTestWhenOneZKDown(CLUSTERS.getHBaseCluster1(), () -> {
          doTestWhenOneZKDown(CLUSTERS.getHBaseCluster2(), () -> {
              try {
                  createFailoverConnection();
                  fail("Should have failed since ACTIVE ZK cluster was shutdown");
              } catch (SQLException e) {
                  LOG.info("Got expected exception when both clusters are down", e);
              }
          });

          // Client cannot connect to HA group because cluster 2 is still STANDBY after restarted
          try {
              createFailoverConnection();
              fail("Should have failed since ACTIVE ZK cluster was shutdown");
          } catch (SQLException e) {
              LOG.info("Got expected exception when ACTIVE ZK cluster {} was shutdown",
                      CLUSTERS.getUrl2(), e);
          }

          // So on-call engineer comes into play, and triggers a failover
          CLUSTERS.transitClusterRole(haGroup, ClusterRole.STANDBY, ClusterRole.ACTIVE);

          // After the second cluster (ACTIVE) ZK restarts, this should still work
          try (Connection conn = createFailoverConnection()) {
              doTestBasicOperationsWithConnection(conn, tableName, haGroupName);
          }
      });
    }

    /**
     * Tests that new Phoenix connections are not created if both clusters are OFFLINE.
     */
    @Test(timeout = 300000)
    public void testConnectionCreationFailsIfBothClustersOffline() throws Exception {
        CLUSTERS.transitClusterRole(haGroup, ClusterRole.OFFLINE, ClusterRole.OFFLINE);

        try {
            createFailoverConnection();
            fail("Should have failed because both clusters are OFFLINE");
        } catch (SQLException e) {
            LOG.info("Got expected exception when creating new connection", e);
            assertEquals(CANNOT_ESTABLISH_CONNECTION.getErrorCode(), e.getErrorCode());
        } // all other type of exception will fail this test.
    }

    /**
     * Tests that existing wrapped Phoenix connection is closed in the Failover event.
     */
    @Test(timeout = 300000)
    public void testWrappedConnectionClosedAfterStandby() throws Exception {
        Connection conn = createFailoverConnection();
        doTestBasicOperationsWithConnection(conn, tableName, haGroupName);

        CLUSTERS.transitClusterRole(haGroup, ClusterRole.STANDBY, ClusterRole.ACTIVE);

        // The wrapped connection is still against the first cluster, and is closed
        PhoenixConnection pc = ((FailoverPhoenixConnection)conn).getWrappedConnection();
        assertNotNull(pc);
        assertEquals(CLUSTERS.getUrl1(), pc.getURL());
        assertTrue(pc.isClosed());
        doTestActionShouldFailBecauseOfFailover(conn::createStatement);
    }

    /**
     * Tests that existing Phoenix statement is closed when cluster transits into STANDBY.
     */
    @Test(timeout = 300000)
    public void testStatementClosedAfterStandby() throws Exception {
        Connection conn = createFailoverConnection();
        Statement stmt = conn.createStatement();
        doTestBasicOperationsWithStatement(conn, stmt, tableName);

        CLUSTERS.transitClusterRole(haGroup, ClusterRole.STANDBY, ClusterRole.ACTIVE);

        assertFalse(conn.isClosed());
        assertTrue(stmt.isClosed());
        doTestActionShouldFailBecauseOfFailover(
                () -> stmt.executeQuery("SELECT * FROM " + tableName));
    }

    /**
     * Tests non-HA connection (vanilla Phoenix connection) is intact when cluster role transits.
     *
     * The reason is that, high availability group has its own CQSI which tracks only those Phoenix
     * connections that are wrapped by failover connections.
     */
    @Test(timeout = 300000)
    public void testNonHAConnectionNotClosedAfterFailover() throws Exception {
        String firstUrl = String.format("jdbc:phoenix:%s", CLUSTERS.getUrl1());
        // This is a vanilla Phoenix connection without using high availability (HA) feature.
        Connection phoenixConn = DriverManager.getConnection(firstUrl, new Properties());
        Connection failoverConn = createFailoverConnection();
        PhoenixConnection wrappedConn = ((FailoverPhoenixConnection) failoverConn)
                .getWrappedConnection();

        assertFalse(phoenixConn.isClosed());
        assertFalse(failoverConn.isClosed());
        assertFalse(wrappedConn.isClosed());

        CLUSTERS.transitClusterRole(haGroup, ClusterRole.STANDBY, ClusterRole.ACTIVE);

        assertFalse(phoenixConn.isClosed()); // normal Phoenix connection is not closed
        assertFalse(failoverConn.isClosed()); // failover connection is not closed by close() method
        assertTrue(wrappedConn.isClosed());
    }

    /**
     * Tests that one HA group cluster role transit will not affect connections in other HA groups.
     */
    @Test(timeout = 300000)
    public void testOtherHAGroupConnectionUnchanged() throws Exception {
        Connection conn = createFailoverConnection();
        PhoenixConnection wrappedConn = ((FailoverPhoenixConnection) conn).getWrappedConnection();
        // Following we create a new HA group and create a connection against this HA group
        String haGroupName2 = haGroup.getGroupInfo().getName() + "2";
        CLUSTERS.initClusterRole(haGroupName2, HighAvailabilityPolicy.FAILOVER);
        Properties clientProperties2 = new Properties(clientProperties);
        clientProperties2.setProperty(PHOENIX_HA_GROUP_ATTR, haGroupName2);
        Connection conn2 = DriverManager.getConnection(CLUSTERS.getJdbcUrl(), clientProperties2);
        PhoenixConnection wrappedConn2 = ((FailoverPhoenixConnection) conn2).getWrappedConnection();

        assertFalse(wrappedConn.isClosed());
        assertFalse(wrappedConn2.isClosed());

        CLUSTERS.transitClusterRole(haGroup, ClusterRole.STANDBY, ClusterRole.ACTIVE);

        assertTrue(wrappedConn.isClosed());
        assertFalse(wrappedConn2.isClosed());
    }

    /**
     * Test that failover can finish even if one connection can not be closed.
     *
     * When once cluster becomes STANDBY from ACTIVE, all its connections and the associated CQS
     * will get closed asynchronously. In case of errors when closing those connections and CQS,
     * the HA group is still able to transit to target state after the maximum timeout.
     * Closing the existing connections is guaranteed with best effort and timeout in favor of
     * improved availability.
     *
     * @see #testFailoverTwice which fails over back to the first cluster
     */
    @Test(timeout = 300000)
    public void testFailoverCanFinishWhenOneConnectionGotStuckClosing() throws Exception {
        Connection conn = createFailoverConnection();
        doTestBasicOperationsWithConnection(conn, tableName, haGroupName);
        assertEquals(CLUSTERS.getUrl1(),  // active connection is against the first cluster
                conn.unwrap(FailoverPhoenixConnection.class).getWrappedConnection().getURL());

        // Spy the wrapped connection
        Connection wrapped = conn.unwrap(FailoverPhoenixConnection.class).getWrappedConnection();
        Connection spy = Mockito.spy(wrapped);
        final CountDownLatch latch = new CountDownLatch(1);
        // Make close() stuck before closing
        doAnswer((invocation) -> {
            latch.await();
            invocation.callRealMethod();
            return null;
        }).when(spy).close();
        ConnectionQueryServices cqs = PhoenixDriver.INSTANCE
                .getConnectionQueryServices(CLUSTERS.getUrl1(), clientProperties);
        // replace the wrapped connection with the spied connection in CQS
        cqs.removeConnection(wrapped.unwrap(PhoenixConnection.class));
        cqs.addConnection(spy.unwrap(PhoenixConnection.class));

        // (ACTIVE, STANDBY) -> (STANDBY, ACTIVE)
        // The transition will finish as we set PHOENIX_HA_TRANSITION_TIMEOUT_MS_KEY for this class
        // even though the spied connection is stuck at the latch when closing
        CLUSTERS.transitClusterRole(haGroup, ClusterRole.STANDBY, ClusterRole.ACTIVE);

        // Verify the spied object has been called once
        verify(spy, times(1)).close();
        // The spy is not closed because the real method was blocked by latch
        assertFalse(spy.isClosed());
        // connection is not closed as Phoenix HA does not close failover connections.
        assertFalse(conn.isClosed());

        try (Connection conn2 = createFailoverConnection()) {
            doTestBasicOperationsWithConnection(conn2, tableName, haGroupName);
            assertEquals(CLUSTERS.getUrl2(), // active connection is against the second cluster
                    conn2.unwrap(FailoverPhoenixConnection.class).getWrappedConnection().getURL());
        }

        latch.countDown();
        conn.close();
        // The CQS should be closed eventually.
        waitFor(() -> {
            try {
                ((ConnectionQueryServicesImpl) cqs).checkClosed();
                return false;
            } catch (IllegalStateException e) {
                LOG.info("CQS got closed as we get expected exception.", e);
                return true;
            }
        }, 100, 10_000);
    }

    /**
     * This is to make sure all Phoenix connections are closed when cluster becomes STANDBY.
     *
     * Test with many connections.
     */
    @Test(timeout = 300000)
    public void testAllWrappedConnectionsClosedAfterStandby() throws Exception {
        short numberOfConnections = 10;
        List<Connection> connectionList = new ArrayList<>(numberOfConnections);
        for (short i = 0; i < numberOfConnections; i++) {
            connectionList.add(createFailoverConnection());
        }

        CLUSTERS.transitClusterRole(haGroup, ClusterRole.STANDBY, ClusterRole.ACTIVE);

        for (short i = 0; i < numberOfConnections; i++) {
            LOG.info("Asserting connection number {}", i);
            FailoverPhoenixConnection conn = ((FailoverPhoenixConnection) connectionList.get(i));
            assertFalse(conn.isClosed());
            assertTrue(conn.getWrappedConnection().isClosed());
        }
    }

    /**
     * This is to make sure all Phoenix connections are closed when cluster becomes STANDBY.
     *
     * Test with many connections.
     */
    @Test(timeout = 300000)
    public void testAllWrappedConnectionsClosedAfterStandbyAsync() throws Exception {
        short numberOfThreads = 10;
        // Test thread waits for half of connections to be created before triggering a failover
        CountDownLatch latchToTransitRole = new CountDownLatch(numberOfThreads /  2);
        // Clients wait for failover to finish before creating more connections
        CountDownLatch latchToCreateMoreConnections = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        List<Future<Connection>> connections = new ArrayList<>(numberOfThreads);
        for (short i = 0; i < numberOfThreads; i++) {
            Future<Connection> future = executor.submit(() -> {
                if (latchToTransitRole.getCount() <= 0) {
                    latchToCreateMoreConnections.await();
                }
                Connection conn = createFailoverConnection();
                latchToTransitRole.countDown();
                return conn;
            });
            connections.add(future);
        }

        latchToTransitRole.await();
        CLUSTERS.transitClusterRole(haGroup, ClusterRole.STANDBY, ClusterRole.STANDBY);
        latchToCreateMoreConnections.countDown();

        waitFor(() -> {
            for (Future<Connection> future : connections) {
                if (!future.isDone()) {
                    return false;
                }
                try {
                    Connection conn = future.get(100, TimeUnit.MILLISECONDS);
                    FailoverPhoenixConnection failoverConn = (FailoverPhoenixConnection) conn;
                    if (!failoverConn.getWrappedConnection().isClosed()) {
                        return false;
                    }
                } catch (Exception e) {
                    LOG.info("Got exception when getting client connection; ignored", e);
                }
            }
            return true;
        }, 100, 60_000);
    }

    /**
     * Test all Phoenix connections are closed when ZK is down and its role becomes STANDBY.
     *
     * This tests with many connections, as {@link #testAllWrappedConnectionsClosedAfterStandby()}.
     * The difference is that, the ACTIVE cluster first shuts down and after the standby cluster is set to active
     */
    @Test(timeout = 300000)
    public void testAllWrappedConnectionsClosedAfterStandbyAndZKDownAsync() throws Exception {
        final short numberOfThreads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        final List<Future<Connection>> connections = new ArrayList<>(numberOfThreads);
        // Add a good connection before shutting down and failing over
        connections.add(executor.submit(this::createFailoverConnection));

        doTestWhenOneZKDown(CLUSTERS.getHBaseCluster1(), () -> {
            LOG.info("Since cluster1 is down, now failing over to cluster2");
            // Create parallel clients that would use the CQSI that failover policy tries to close
            for (short i = 1; i < numberOfThreads; i++) {
                connections.add(executor.submit(this::createFailoverConnection));
            }
            // The ACTIVE cluster goes down, and then on-call engineer triggers failover.
            CLUSTERS.transitClusterRole(haGroup, ClusterRole.STANDBY, ClusterRole.ACTIVE);
        });

        waitFor(() -> {
            for (Future<Connection> future : connections) {
                if (!future.isDone()) {
                    return false;
                }
                try {
                    Connection c = future.get(100, TimeUnit.MILLISECONDS);
                    PhoenixConnection pc = ((FailoverPhoenixConnection) c).getWrappedConnection();
                    if (!pc.isClosed() && !pc.getURL().equals(CLUSTERS.getUrl2())) {
                        fail("Found one connection to cluster1 but it is not closed");
                    }
                } catch (Exception e) {
                    LOG.info("Got exception when getting client connection; ignored", e);
                }
            }
            return true;
        }, 100, 120_000);
    }

    /**
     * Test that new Phoenix connection can be created after cluster role finishes transition.
     *
     * Application may enable HA failover feature but it does not call failover() explicitly. In
     * that case, retrying the business logic will request a new JDBC connection. This connection
     * will connect to the new ACTIVE cluster, if any.
     */
    @Test(timeout = 300000)
    public void testNewPhoenixConnectionAfterFailover() throws Exception {
        try (Connection conn = createFailoverConnection()) {
            doTestBasicOperationsWithConnection(conn, tableName, haGroupName);
        }

        // Make the second cluster the active one.
        CLUSTERS.transitClusterRole(haGroup, ClusterRole.STANDBY, ClusterRole.ACTIVE);

        try (Connection conn = createFailoverConnection()) {
            doTestBasicOperationsWithConnection(conn, tableName, haGroupName);
        }
    }

    /**
     * Test that we can failover to one cluster and then failover back.
     *
     * @see #testConnectionCreationFailsIfNoActiveCluster
     * @see #testFailoverCanFinishWhenOneConnectionGotStuckClosing
     */
    @Test(timeout = 300000)
    public void testFailoverTwice() throws Exception {
        try (Connection conn = createFailoverConnection()) {
            doTestBasicOperationsWithConnection(conn, tableName, haGroupName);
            assertEquals(CLUSTERS.getUrl1(), // active connection is against the first cluster
                    conn.unwrap(FailoverPhoenixConnection.class).getWrappedConnection().getURL());
        }

        // Make the second cluster the active one.
        CLUSTERS.transitClusterRole(haGroup, ClusterRole.STANDBY, ClusterRole.ACTIVE);

        try (Connection conn = createFailoverConnection()) {
            doTestBasicOperationsWithConnection(conn, tableName, haGroupName);
            assertEquals(CLUSTERS.getUrl2(), // active connection is against the second cluster
                    conn.unwrap(FailoverPhoenixConnection.class).getWrappedConnection().getURL());
        }

        // Failover back to the first cluster.
        CLUSTERS.transitClusterRole(haGroup, ClusterRole.ACTIVE, ClusterRole.STANDBY);

        try (Connection conn = createFailoverConnection()) {
            doTestBasicOperationsWithConnection(conn, tableName, haGroupName);
            assertEquals(CLUSTERS.getUrl1(), // active connection is against the first cluster
                    conn.unwrap(FailoverPhoenixConnection.class).getWrappedConnection().getURL());
        }
    }

    /**
     * Test that we can failover Phoenix connection explicitly.
     */
    @Test(timeout = 300000)
    public void testFailoverConnectionExplicitly() throws Exception {
        Connection conn = createFailoverConnection();
        doTestBasicOperationsWithConnection(conn, tableName, haGroupName);

        // Make the second cluster ACTIVE will not change the wrapped connection.
        CLUSTERS.transitClusterRole(haGroup, ClusterRole.STANDBY, ClusterRole.ACTIVE);
        doTestActionShouldFailBecauseOfFailover(conn::createStatement);

        // failover explicitly
        FailoverPhoenixConnection.failover(conn, 30_000);
        doTestBasicOperationsWithConnection(conn, tableName, haGroupName);

        // failover explicitly once more (failover back)
        CLUSTERS.transitClusterRole(haGroup, ClusterRole.ACTIVE, ClusterRole.STANDBY);
        FailoverPhoenixConnection.failover(conn, 30_000);
        doTestBasicOperationsWithConnection(conn, tableName, haGroupName);
    }

    /**
     * Test that it times out to failover explicitly when two clusters are STANDBY.
     */
    @Test(timeout = 300000)
    public void testFailoverConnectionExplicitlyTimeout() throws Exception {
        Connection conn = createFailoverConnection();
        doTestBasicOperationsWithConnection(conn, tableName, haGroupName);

        CLUSTERS.transitClusterRole(haGroup, ClusterRole.STANDBY, ClusterRole.STANDBY);

        try {
            // failover explicitly
            FailoverPhoenixConnection.failover(conn, 10_000);
            fail("Should have failed since two clusters are both in STANDBY role");
        } catch (FailoverSQLException e) {
            LOG.info("Got expected exception when failover explicitly", e);
        }
    }

    /**
     * Test tenant specific connection creation and basic operations.
     */
    @Test(timeout = 300000)
    public void testTenantSpecificPhoenixConnection() throws Exception {
        tableName = tableName + "Tenant";
        CLUSTERS.createTenantSpecificTable(tableName);

        clientProperties.setProperty("TenantId", "mytenant");
        Connection tenantConn = createFailoverConnection();
        doTestBasicOperationsWithConnection(tenantConn, tableName, haGroupName);

        // Make the second cluster ACTIVE will not change the wrapped connection.
        CLUSTERS.transitClusterRole(haGroup, ClusterRole.STANDBY, ClusterRole.ACTIVE);
        doTestActionShouldFailBecauseOfFailover(tenantConn::createStatement);

        // Application can always create new connections after cluster role transition
        try (Connection newTenantConn = createFailoverConnection()) {
            doTestBasicOperationsWithConnection(newTenantConn, tableName, haGroupName);
        }
    }

    /**
     * Test failover automatically happens with {@link FailoverPolicy.FailoverToActivePolicy}.
     */
    @Test(timeout = 300000)
    public void testStatementWithActiveFailoverPolicy() throws Exception {
        clientProperties.setProperty(FailoverPolicy.PHOENIX_HA_FAILOVER_POLICY_ATTR, "active");

        final Connection conn = createFailoverConnection();
        final Statement stmt1 = conn.createStatement();
        doTestBasicOperationsWithStatement(conn, stmt1, tableName);

        // Make the second cluster the active one.
        CLUSTERS.transitClusterRole(haGroup, ClusterRole.STANDBY, ClusterRole.ACTIVE);

        assertFalse(conn.isClosed());
        assertTrue(stmt1.isClosed());

        // Creating new statement will always work as long as there is ACTIVE cluster because the
        // failover connection can failover internal wrapped phoenix connection automatically
        final Statement stmt2 = conn.createStatement();
        doTestBasicOperationsWithStatement(conn, stmt2, tableName);
    }

    @Test(timeout = 300000)
    public void testFailoverMetrics() throws Exception {
        Connection conn = createFailoverConnection();
        // paranoid; let us just reset
        PhoenixRuntime.resetMetrics(conn);
        assertTrue(PhoenixRuntime.getWriteMetricInfoForMutationsSinceLastReset(conn).isEmpty());
        // operation on connection: upsert once
        doTestBasicOperationsWithConnection(conn, tableName, haGroupName);
        doVerifyMetrics(conn, 1L);

        // Failover the HA group
        CLUSTERS.transitClusterRole(haGroup, ClusterRole.STANDBY, ClusterRole.ACTIVE);
        // wrapped connection should have been closed; but "conn" is not so we can still get metrics
        doVerifyMetrics(conn, 1L);
        // failover the connection explicitly
        FailoverPhoenixConnection.failover(conn, 30_000);
        doVerifyMetrics(conn, 1L);
        // operation on connection: upsert once more
        doTestBasicOperationsWithConnection(conn, tableName, haGroupName);
        doVerifyMetrics(conn, 2L);
        // reset metrics
        PhoenixRuntime.resetMetrics(conn);
        assertTrue(PhoenixRuntime.getWriteMetricInfoForMutationsSinceLastReset(conn).isEmpty());
        // upsert once more
        doTestBasicOperationsWithConnection(conn, tableName, haGroupName);
        doVerifyMetrics(conn, 1L);

        // close failover connection will reset metrics
        conn.close();
        assertTrue(PhoenixRuntime.getWriteMetricInfoForMutationsSinceLastReset(conn).isEmpty());
    }

    /**
     * Helper method to verify that the failover connection has expected mutation metrics.
     *
     * @param conn the failover Phoenix connection
     * @param expectedUpsert number of upsert mutation sql counter
     * @throws SQLException if fails to read the metrics
     */
    private void doVerifyMetrics(Connection conn, long expectedUpsert) throws SQLException {
        Map<String, Map<MetricType, Long>> mutation =
                PhoenixRuntime.getWriteMetricInfoForMutationsSinceLastReset(conn);
        assertFalse(mutation.isEmpty());
        assertTrue(mutation.containsKey(tableName));
        Long upsertMetric = mutation.get(tableName).get(MetricType.UPSERT_MUTATION_SQL_COUNTER);
        assertEquals(expectedUpsert, upsertMetric.longValue());
        assertTrue(PhoenixRuntime.getReadMetricInfoForMutationsSinceLastReset(conn).isEmpty());
    }

    /**
     * Create a failover connection using {@link #clientProperties}.
     */
    private Connection createFailoverConnection() throws SQLException {
        return DriverManager.getConnection(CLUSTERS.getJdbcUrl(), clientProperties);
    }

    @FunctionalInterface
    private interface Action {
        void execute() throws Exception;
    }

    /**
     * Assert a JDBC connection is closed after failover.
     */
    private static void doTestActionShouldFailBecauseOfFailover(Action action) throws Exception {
        try {
            action.execute();
            fail("Should have failed because the connection is closed");
        } catch (FailoverSQLException fe) {
            LOG.info("Got expected failover exception after connection is closed.", fe);
        } catch (SQLException e) {
            LOG.info("Will fail the test if its cause is not FailoverSQLException", e);
            assertTrue(e.getCause() instanceof FailoverSQLException);
            LOG.info("Got expected failover exception after connection is closed.", e);
        } // all other type of exception will fail this test.
    }
}
