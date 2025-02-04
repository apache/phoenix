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
        import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.HBaseTestingUtilityPair.doTestWhenOneZKDown;
        import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.doTestBasicOperationsWithConnection;
        import static org.apache.phoenix.jdbc.HighAvailabilityGroup.PHOENIX_HA_GROUP_ATTR;
        import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.getHighAvailibilityGroup;
        import static org.junit.Assert.assertEquals;
        import static org.junit.Assert.assertFalse;
        import static org.junit.Assert.assertTrue;
        import static org.junit.Assert.fail;

        import java.sql.Connection;
        import java.sql.DriverManager;
        import java.sql.SQLException;
        import java.util.ArrayList;
        import java.util.Arrays;
        import java.util.Collection;
        import java.util.List;
        import java.util.Properties;
        import java.util.concurrent.ExecutorService;
        import java.util.concurrent.Executors;
        import java.util.concurrent.Future;
        import java.util.concurrent.TimeUnit;

        import org.apache.commons.lang3.RandomStringUtils;
        import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
        import org.junit.After;
        import org.junit.AfterClass;
        import org.junit.Before;
        import org.junit.BeforeClass;
        import org.junit.Rule;
        import org.junit.Test;
        import org.junit.experimental.categories.Category;
        import org.junit.rules.TestName;
        import org.junit.runner.RunWith;
        import org.junit.runners.Parameterized;
        import org.slf4j.Logger;
        import org.slf4j.LoggerFactory;

/**
 * Test failover basics for {@link FailoverPhoenixConnection}.
 */
@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class FailoverPhoenixConnection2IT {
    private static final Logger LOG = LoggerFactory.getLogger(FailoverPhoenixConnectionIT.class);
    private static final HighAvailabilityTestingUtility.HBaseTestingUtilityPair CLUSTERS = new HighAvailabilityTestingUtility.HBaseTestingUtilityPair();

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
    private final ClusterRoleRecord.RegistryType registryType;

    public FailoverPhoenixConnection2IT(ClusterRoleRecord.RegistryType registryType) {
        this.registryType = registryType;
    }

    @Parameterized.Parameters(name="ClusterRoleRecord_registryType={0}")
    public static Collection<Object> data() {
        return Arrays.asList(new Object[] {
                ClusterRoleRecord.RegistryType.ZK,
                ClusterRoleRecord.RegistryType.MASTER,
                ClusterRoleRecord.RegistryType.RPC,
                null //For Backward Compatibility
        });
    }

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
        if (registryType == null) {
            CLUSTERS.initClusterRole(haGroupName, HighAvailabilityPolicy.FAILOVER);
        } else {
            CLUSTERS.initClusterRole(haGroupName, HighAvailabilityPolicy.FAILOVER, registryType);
        }

        haGroup = getHighAvailibilityGroup(CLUSTERS.getJdbcHAUrl(), clientProperties);
        LOG.info("Initialized haGroup {} with URL {}", haGroup, CLUSTERS.getJdbcHAUrl());
        tableName = RandomStringUtils.randomAlphabetic(10).toUpperCase();
        CLUSTERS.createTableOnClusterPair(haGroup, tableName);
    }

    @After
    public void tearDown() throws Exception {
        try {
            haGroup.close();
            PhoenixDriver.INSTANCE
                    .getConnectionQueryServices(CLUSTERS.getJdbcUrl1(haGroup), haGroup.getProperties())
                    .close();
            PhoenixDriver.INSTANCE
                    .getConnectionQueryServices(CLUSTERS.getJdbcUrl2(haGroup), haGroup.getProperties())
                    .close();
        } catch (Exception e) {
            LOG.error("Fail to tear down the HA group and the CQS. Will ignore", e);
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
            CLUSTERS.transitClusterRole(haGroup, ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.OFFLINE);

            // Usually making this cluster STANDBY will close the CQS and all existing connections.
            // The HA group was created in setup() but no CQS is yet opened for this ACTIVE cluster.
            // As a result there is neither the CQS nor existing opened connections.
            // In this case, the failover should finish instead of failing to get/close the CQS.
            CLUSTERS.transitClusterRole(haGroup, ClusterRoleRecord.ClusterRole.STANDBY, ClusterRoleRecord.ClusterRole.ACTIVE);

            try (Connection conn = createFailoverConnection()) {
                FailoverPhoenixConnection failoverConn = (FailoverPhoenixConnection) conn;
                assertEquals(CLUSTERS.getJdbcUrl2(haGroup), failoverConn.getWrappedConnection().getURL());
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
                fail("Should have failed since ACTIVE ZK '" + CLUSTERS.getZkUrl1() + "' is down");
            } catch (SQLException e) {
                LOG.info("Got expected exception when ACTIVE ZK cluster is down", e);
            }

            // As the target ZK is down, now existing CQS if any is in a bad state. In this case,
            // the failover should still finish. After all, this is the most important use case the
            // failover is designed for.
            CLUSTERS.transitClusterRole(haGroup, ClusterRoleRecord.ClusterRole.STANDBY, ClusterRoleRecord.ClusterRole.ACTIVE);

            try (Connection conn = createFailoverConnection()) {
                FailoverPhoenixConnection failoverConn = (FailoverPhoenixConnection) conn;
                assertEquals(CLUSTERS.getJdbcUrl2(haGroup), failoverConn.getWrappedConnection().getURL());
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
        // This creates the cqsi for the active cluster upfront.
        // If we don't do that then later when we try to transition
        // the cluster role it tries to create cqsi for the cluster
        // which is down and that takes forever causing timeouts
        try (Connection conn = createFailoverConnection()) {
            doTestBasicOperationsWithConnection(conn, tableName, haGroupName);
        }
        doTestWhenOneZKDown(CLUSTERS.getHBaseCluster1(), () -> {
            try {
                try (Connection conn = createFailoverConnection()) {
                    doTestBasicOperationsWithConnection(conn, tableName, haGroupName);
                }
                fail("Should have failed since ACTIVE ZK cluster was shutdown");
            } catch (SQLException e) {
                LOG.info("Got expected exception when ACTIVE ZK cluster is down", e);
            }

            // So on-call engineer comes into play, and triggers a failover
            // Because cluster1 is down, new version record will only be updated in cluster2
            CLUSTERS.transitClusterRole(haGroup, ClusterRoleRecord.ClusterRole.STANDBY, ClusterRoleRecord.ClusterRole.ACTIVE);

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
            assertEquals(CLUSTERS.getJdbcUrl2(haGroup), failoverConn.getWrappedConnection().getURL());
            doTestBasicOperationsWithConnection(conn, tableName, haGroupName);
        }

        LOG.info("Testing failover back to cluster1 when bot clusters are up and running");
        // Failover back to the first cluster since it is healthy after restart
        CLUSTERS.transitClusterRole(haGroup, ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY);
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
            CLUSTERS.transitClusterRole(haGroup, ClusterRoleRecord.ClusterRole.STANDBY, ClusterRoleRecord.ClusterRole.ACTIVE);

            try {
                createFailoverConnection();
                fail("Should have failed since ACTIVE ZK cluster was shutdown");
            } catch (SQLException e) {
                LOG.info("Got expected exception when ACTIVE ZK cluster {} was shutdown",
                        CLUSTERS.getZkUrl2(), e);
            }
        });

        // After the second cluster (ACTIVE) ZK restarts, this should still work
        // For registryType MASTER and RPC this will fail as new miniCluster will
        // have different port for master refreshing CRR with new url
        if (registryType == ClusterRoleRecord.RegistryType.RPC ||
                registryType == ClusterRoleRecord.RegistryType.MASTER) {
            CLUSTERS.refreshClusterRoleRecordAfterClusterRestart(haGroup,
                    ClusterRoleRecord.ClusterRole.STANDBY, ClusterRoleRecord.ClusterRole.ACTIVE);
        }
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
                        CLUSTERS.getZkUrl2(), e);
            }

            // So on-call engineer comes into play, and triggers a failover
            CLUSTERS.transitClusterRole(haGroup, ClusterRoleRecord.ClusterRole.STANDBY, ClusterRoleRecord.ClusterRole.ACTIVE);

            // After the second cluster (ACTIVE) ZK restarts, this should still work
            try (Connection conn = createFailoverConnection()) {
                doTestBasicOperationsWithConnection(conn, tableName, haGroupName);
            }
        });
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

        CLUSTERS.transitClusterRole(haGroup, ClusterRoleRecord.ClusterRole.STANDBY, ClusterRoleRecord.ClusterRole.ACTIVE);

        for (short i = 0; i < numberOfConnections; i++) {
            LOG.info("Asserting connection number {}", i);
            FailoverPhoenixConnection conn = ((FailoverPhoenixConnection) connectionList.get(i));
            assertFalse(conn.isClosed());
            assertTrue(conn.getWrappedConnection().isClosed());
        }
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
            CLUSTERS.transitClusterRole(haGroup, ClusterRoleRecord.ClusterRole.STANDBY, ClusterRoleRecord.ClusterRole.ACTIVE);
        });

        waitFor(() -> {
            for (Future<Connection> future : connections) {
                if (!future.isDone()) {
                    return false;
                }
                try {
                    Connection c = future.get(100, TimeUnit.MILLISECONDS);
                    PhoenixConnection pc = ((FailoverPhoenixConnection) c).getWrappedConnection();
                    if (!pc.isClosed() && !pc.getURL().equals(CLUSTERS.getZkUrl2())) {
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
     * This is to make sure all Phoenix connections are closed when registryType changes
     * of a CRR.
     * scenarios
     *   1) ZK --> RPC
     *   2) null (ZK) --> MASTER
     *   3) MASTER --> ZK
     *   4) RPC --> ZK
     *
     * Test with many connections.
     */
    @Test(timeout = 300000)
    public void testAllWrappedConnectionsClosedAfterRegistryChange() throws Exception {
        short numberOfConnections = 10;
        List<Connection> connectionList = new ArrayList<>(numberOfConnections);
        for (short i = 0; i < numberOfConnections; i++) {
            connectionList.add(createFailoverConnection());
        }
        ClusterRoleRecord.RegistryType newRegistry = null;
        if (registryType == null) {
            newRegistry = ClusterRoleRecord.RegistryType.MASTER;
        } else if (registryType == ClusterRoleRecord.RegistryType.ZK) {
            newRegistry = ClusterRoleRecord.RegistryType.RPC;
        } else if (registryType == ClusterRoleRecord.RegistryType.MASTER) {
            newRegistry = ClusterRoleRecord.RegistryType.ZK;
        } else if (registryType == ClusterRoleRecord.RegistryType.RPC) {
            newRegistry = ClusterRoleRecord.RegistryType.ZK;
        }
        CLUSTERS.transitClusterRoleRecordRegistry(haGroup, newRegistry);

        for (short i = 0; i < numberOfConnections; i++) {
            LOG.info("Asserting connection number {}", i);
            FailoverPhoenixConnection conn = ((FailoverPhoenixConnection) connectionList.get(i));
            assertFalse(conn.isClosed());
            assertTrue(conn.getWrappedConnection().isClosed());
        }
    }

    /**
     * This is to make sure all Phoenix connections are closed when registryType changes
     * of a CRR.
     * scenarios
     *   1) ZK --> MASTER
     *   2) null (ZK) --> RPC
     *   3) MASTER --> null
     *   4) RPC --> null
     *
     * Test with many connections.
     */
    @Test(timeout = 300000)
    public void testAllWrappedConnectionsClosedAfterRegistryChange2() throws Exception {
        short numberOfConnections = 10;
        List<Connection> connectionList = new ArrayList<>(numberOfConnections);
        for (short i = 0; i < numberOfConnections; i++) {
            connectionList.add(createFailoverConnection());
        }
        ClusterRoleRecord.RegistryType newRegistry = null;
        if (registryType == null) {
            newRegistry = ClusterRoleRecord.RegistryType.RPC;
        } else if (registryType == ClusterRoleRecord.RegistryType.ZK) {
            newRegistry = ClusterRoleRecord.RegistryType.MASTER;
        }
        CLUSTERS.transitClusterRoleRecordRegistry(haGroup, newRegistry);

        for (short i = 0; i < numberOfConnections; i++) {
            LOG.info("Asserting connection number {}", i);
            FailoverPhoenixConnection conn = ((FailoverPhoenixConnection) connectionList.get(i));
            assertFalse(conn.isClosed());
            assertTrue(conn.getWrappedConnection().isClosed());
        }
    }

    /**
     * Testing connection closure for roleRecord change from
     * (URL1, ACTIVE, URL2, STANDBY) -> (URL3, ACTIVE, URL2, STANDBY)
     * All wrapped connections should have been closed
     */
    @Test(timeout = 300000)
    public void testAllWrappedConnectionsClosedAfterActiveURLChange() throws Exception {
        short numberOfConnections = 10;
        //Create FailoverPhoenixConnections with default urls
        List<Connection> connectionList = new ArrayList<>(numberOfConnections);
        for (short i = 0; i < numberOfConnections; i++) {
            connectionList.add(createFailoverConnection());
        }

        //Restart Cluster 1 with new ports
        CLUSTERS.restartCluster1();
        //Basically applying new url for cluster 1
        CLUSTERS.refreshClusterRoleRecordAfterClusterRestart(haGroup,
                ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY);
        for (short i = 0; i < numberOfConnections; i++) {
            LOG.info("Asserting connection number {}", i);
            FailoverPhoenixConnection conn = ((FailoverPhoenixConnection) connectionList.get(i));
            assertFalse(conn.isClosed());
            assertTrue(conn.getWrappedConnection().isClosed());
            conn.close();
        }
    }

    /**
     * Testing connection closure for roleRecord change from
     * (URL1, ACTIVE, URL2, STANDBY) -> (URL1, ACTIVE, URL3, STANDBY)
     * It should not have affected connections at all
     */
    @Test(timeout = 300000)
    public void testAllWrappedConnectionsNotClosedAfterStandbyURLChange() throws Exception {
        short numberOfConnections = 10;
        //Create FailoverPhoenixConnections with default urls
        List<Connection> connectionList = new ArrayList<>(numberOfConnections);
        for (short i = 0; i < numberOfConnections; i++) {
            connectionList.add(createFailoverConnection());
        }

        //Restart Cluster 2 with new ports
        CLUSTERS.restartCluster2();
        //Basically applying new url for cluster 2
        CLUSTERS.refreshClusterRoleRecordAfterClusterRestart(haGroup,
                ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY);
        for (short i = 0; i < numberOfConnections; i++) {
            LOG.info("Asserting connection number {}", i);
            FailoverPhoenixConnection conn = ((FailoverPhoenixConnection) connectionList.get(i));
            assertFalse(conn.isClosed());
            assertFalse(conn.getWrappedConnection().isClosed());
            conn.close();
        }
    }

    /**
     * Create a failover connection using {@link #clientProperties}.
     */
    private Connection createFailoverConnection() throws SQLException {
        return DriverManager.getConnection(CLUSTERS.getJdbcHAUrl(), clientProperties);
    }
}
