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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.ClusterRoleRecord.ClusterRole;
import org.apache.phoenix.util.JDBCUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.test.GenericTestUtils.waitFor;
import static org.apache.phoenix.exception.SQLExceptionCode.CANNOT_ESTABLISH_CONNECTION;
import static org.apache.phoenix.jdbc.HighAvailabilityGroup.*;
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.doTestBasicOperationsWithConnection;
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.getHighAvailibilityGroup;
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.sleepThreadFor;
import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

@Category(NeedsOwnMiniClusterTest.class)
public class HighAvailabilityGroup2IT {
    private static final Logger LOG = LoggerFactory.getLogger(HighAvailabilityGroup2IT.class);
    private static final HighAvailabilityTestingUtility.HBaseTestingUtilityPair CLUSTERS = new HighAvailabilityTestingUtility.HBaseTestingUtilityPair();

    /** Client properties to create a connection per test. */
    private Properties clientProperties;
    /** JDBC connection string for this test HA group. */
    private String jdbcHAUrl;
    /** Failover HA group for to test. */
    private HighAvailabilityGroup haGroup;
    private HAURLInfo haURLInfo;
    /** HA Group name for this test. */
    private String haGroupName;

    @Rule
    public final TestName testName = new TestName();
    @Rule
    public final Timeout globalTimeout = new Timeout(180, TimeUnit.SECONDS);
    private final ClusterRoleRecord.RegistryType registryType = ClusterRoleRecord.RegistryType.RPC;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        CLUSTERS.start();
        DriverManager.registerDriver(PhoenixDriver.INSTANCE);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        HighAvailabilityGroup.CURATOR_CACHE.invalidateAll();
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
        jdbcHAUrl = CLUSTERS.getJdbcHAUrl();
        haURLInfo = HighAvailabilityGroup.getUrlInfo(jdbcHAUrl, clientProperties);
        haGroup = getHighAvailibilityGroup(jdbcHAUrl,clientProperties);
    }

    @After
    public void tearDown() throws Exception {
        haGroup.close();
        try {
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
     * Test that if STANDBY HBase cluster is down, the connect should work.
     */
    @Test
    public void testCanConnectWhenStandbyHBaseClusterDown() throws Exception {
        CLUSTERS.doTestWhenOneHBaseDown(CLUSTERS.getHBaseCluster2(), () -> {
            // HA group is already initialized
            Connection connection = haGroup.connect(clientProperties, haURLInfo);
            assertNotNull(connection);
            assertNotNull(connection.unwrap(FailoverPhoenixConnection.class));
        });
    }

    /**
     * Test that if STANDBY ZK cluster is down, the connect should work.
     *
     * This differs from {@link #testCanConnectWhenStandbyHBaseClusterDown} because this stops the
     * ZK cluster, not the HBase cluster.
     */
    @Test
    public void testCanConnectWhenStandbyZKClusterDown() throws Exception {
        CLUSTERS.doTestWhenOneZKDown(CLUSTERS.getHBaseCluster2(), () -> {
            // Clear the HA Cache
            HighAvailabilityGroup.CURATOR_CACHE.invalidateAll();

            // HA group is already initialized
            Connection connection = haGroup.connect(clientProperties, haURLInfo);
            assertNotNull(connection);
            assertNotNull(connection.unwrap(FailoverPhoenixConnection.class));
        });
    }

    /**
     * Test that if STANDBY HBase cluster is down, connect to new HA group should work.
     *
     * This test covers only HBase cluster is down, and both ZK clusters are still healthy so
     * clients will be able to get latest clusters role record from both clusters. This tests a new
     * HA group which will get initialized during the STANDBY HBase cluster down time.
     */
    @Test
    public void testCanConnectNewGroupWhenStandbyHBaseClusterDown() throws Exception {
        CLUSTERS.doTestWhenOneHBaseDown(CLUSTERS.getHBaseCluster2(), () -> {
            // get and initialize a new HA group when cluster2 is down
            String haGroupName2 = testName.getMethodName() + RandomStringUtils.randomAlphabetic(3);
            clientProperties.setProperty(PHOENIX_HA_GROUP_ATTR, haGroupName2);
            CLUSTERS.initClusterRoleRecordFor1Cluster(haGroupName2, HighAvailabilityPolicy.FAILOVER);
            Optional<HighAvailabilityGroup> haGroup2 = Optional.empty();
            try {
                HAURLInfo haURLInfo = HighAvailabilityGroup.getUrlInfo(jdbcHAUrl, clientProperties);
                haGroup2 = HighAvailabilityGroup.get(jdbcHAUrl, clientProperties);
                assertTrue(haGroup2.isPresent());
                assertNotSame(haGroup2.get(), haGroup);
                assertTrue(haGroup2.get().getRoleRecord().getRole1().isActive());
                // get a new connection in this new HA group; should be pointing to ACTIVE cluster1
                try (Connection connection = haGroup2.get().connect(clientProperties, haURLInfo)) {
                    assertNotNull(connection);
                    assertNotNull(connection.unwrap(FailoverPhoenixConnection.class));
                }
            } finally {
                haGroup2.ifPresent(HighAvailabilityGroup::close);
            }
        });
    }

    /**
     * Test that if STANDBY cluster ZK service is down, connect to new HA group should work.
     *
     * This differs from {@link #testCanConnectNewGroupWhenStandbyHBaseClusterDown} because this is
     * testing scenarios when STANDBY ZK cluster is down.
     */
    @Test
    public void testCanConnectNewGroupWhenStandbyZKClusterDown() throws Exception {
        // get and initialize a new HA group when cluster2 is down
        String haGroupName2 = testName.getMethodName() + RandomStringUtils.randomAlphabetic(3);
        clientProperties.setProperty(PHOENIX_HA_GROUP_ATTR, haGroupName2);
        CLUSTERS.initClusterRole(haGroupName2, HighAvailabilityPolicy.FAILOVER);

        CLUSTERS.doTestWhenOneZKDown(CLUSTERS.getHBaseCluster2(), () -> {
            Optional<HighAvailabilityGroup> haGroup2 = Optional.empty();
            try {
                HAURLInfo haURLInfo = HighAvailabilityGroup.getUrlInfo(jdbcHAUrl, clientProperties);
                haGroup2 = HighAvailabilityGroup.get(jdbcHAUrl, clientProperties);
                assertTrue(haGroup2.isPresent());
                assertNotSame(haGroup2.get(), haGroup);
                // get a new connection in this new HA group; should be pointing to ACTIVE cluster1
                Connection connection = haGroup2.get().connect(clientProperties, haURLInfo);
                assertNotNull(connection);
                assertNotNull(connection.unwrap(FailoverPhoenixConnection.class));
            } finally {
                haGroup2.ifPresent(HighAvailabilityGroup::close);
            }
        });
    }

    /**
     * Test it can not establish active connection to the ACTIVE HBase cluster if it is down.
     */
    @Test
    public void testCanNotEstablishConnectionWhenActiveHBaseClusterDown() throws Exception {
        CLUSTERS.doTestWhenOneHBaseDown(CLUSTERS.getHBaseCluster1(), () -> {
            try {
                haGroup.connectActive(clientProperties, haURLInfo);
                fail("Should have failed because ACTIVE HBase cluster is down.");
            } catch (SQLException e) {
                LOG.info("Got expected exception when ACTIVE HBase cluster is down", e);
                assertEquals(CANNOT_ESTABLISH_CONNECTION.getErrorCode(), e.getErrorCode());
            }
        });
    }

    /**
     * Test that client can not establish connection to when the ACTIVE ZK cluster is down,
     * while client can establish active connection after active ZK cluster restarts.
     *
     * This differs from the {@link #testCanNotEstablishConnectionWhenActiveHBaseClusterDown()}
     * because this is for ZK cluster down while the other is for HBase cluster down.
     */
    @Test
    public void testConnectActiveWhenActiveZKClusterRestarts() throws Exception {
        CLUSTERS.doTestWhenOneZKDown(CLUSTERS.getHBaseCluster1(), () -> {
            try {
                haGroup.connectActive(clientProperties, haURLInfo);
                fail("Should have failed because of ACTIVE ZK cluster is down.");
            } catch (SQLException e) {
                LOG.info("Got expected exception when ACTIVE ZK cluster is down", e);
                assertEquals(CANNOT_ESTABLISH_CONNECTION.getErrorCode(), e.getErrorCode());
            }
        });

        try (Connection conn = haGroup.connectActive(clientProperties, haURLInfo)) {
            assertNotNull(conn);
            LOG.info("Successfully connect to HA group {} after restarting ACTIVE ZK", haGroup);
        } // all other exceptions will fail the test
    }

    /**
     * Test that HA connection request will fall back to the first cluster when HA group fails
     * to initialize due to missing cluster role record (CRR).
     */
    @Test
    public void testFallbackToSingleConnection() throws Exception {
        final String tableName = RandomStringUtils.randomAlphabetic(10);
        CLUSTERS.createTableOnClusterPair(haGroup, tableName);

        String haGroupName2 = testName.getMethodName() + RandomStringUtils.randomAlphabetic(3);
        clientProperties.setProperty(PHOENIX_HA_GROUP_ATTR, haGroupName2);
        clientProperties.setProperty(PHOENIX_HA_FALLBACK_CLUSTER_KEY, CLUSTERS.getJdbcUrl1(haGroup));
        // no cluster role record for the HA group with name haGroupName2
        try (Connection conn = DriverManager.getConnection(jdbcHAUrl, clientProperties)) {
            // connection is PhoenixConnection instead of HA connection (failover or parallel)
            assertTrue(conn instanceof PhoenixConnection);
            // Deterministically talking to the first cluster: first means "smaller" URL
            String firstClusterUrl = CLUSTERS.getJdbcUrl1(haGroup);
            assertEquals(firstClusterUrl, ((PhoenixConnection) conn).getURL());
            doTestBasicOperationsWithConnection(conn, tableName, haGroupName2);
        }

        // disable fallback feature even if the cluster role record is missing for this group
        clientProperties.setProperty(PHOENIX_HA_SHOULD_FALLBACK_WHEN_MISSING_CRR_KEY, "false");
        try {
            DriverManager.getConnection(jdbcHAUrl, clientProperties);
            fail("Should have failed when disabling fallback to single cluster");
        } catch (SQLException e) {
            LOG.info("Got expected exception when disabling fallback");
        }
        clientProperties.remove(PHOENIX_HA_SHOULD_FALLBACK_WHEN_MISSING_CRR_KEY);

        clientProperties.remove(PHOENIX_HA_FALLBACK_CLUSTER_KEY);
        //If we don't have a fallback cluster key then connection creation should fail
        try {
            DriverManager.getConnection(jdbcHAUrl, clientProperties);
            fail("Should have failed when fallback cluster key is missing");
        } catch (SQLException e) {
            LOG.info("Should have failed when fallback cluster key is missing");
        }


        // Now create the cluster role record
        CLUSTERS.initClusterRole(haGroupName2, HighAvailabilityPolicy.FAILOVER);
        // And invalidate the negative cache. We are doing it manually because the default time for
        // expireAfterWrite is too long for testing. We do not need to test the cache per se.
        HighAvailabilityGroup.MISSING_CRR_GROUPS_CACHE.invalidateAll();

        // After it is expired in the negative cache, the HA group will start serving HA connections
        try (Connection conn = DriverManager.getConnection(jdbcHAUrl, clientProperties)) {
            assertTrue(conn instanceof FailoverPhoenixConnection);
            doTestBasicOperationsWithConnection(conn, tableName, haGroupName2);
        }
    }

    /**
     * Test that HA connection request will not fall back to the first cluster when one ZK is down.
     */
    @Test
    public void testNotFallbackToSingleConnection() throws Exception {
        final String tableName = RandomStringUtils.randomAlphabetic(10);
        CLUSTERS.createTableOnClusterPair(haGroup, tableName);
        String haGroupName2 = testName.getMethodName() + RandomStringUtils.randomAlphabetic(3);
        clientProperties.setProperty(PHOENIX_HA_GROUP_ATTR, haGroupName2);
        clientProperties.setProperty(PHOENIX_HA_FALLBACK_CLUSTER_KEY, CLUSTERS.getJdbcUrl1(haGroup));

        CLUSTERS.doTestWhenOneZKDown(CLUSTERS.getHBaseCluster1(), () -> {
            try {
                DriverManager.getConnection(jdbcHAUrl, clientProperties);
                fail("Should have failed since one HA group can not initialized. Not falling back");
            } catch (SQLException e) {
                LOG.info("Got expected exception as HA group fails to initialize", e);
            }
        });

        // After cluster 2 is up and running again, fallback to single cluster will work
        try (Connection conn = DriverManager.getConnection(jdbcHAUrl, clientProperties)) {
            // connection is PhoenixConnection instead of HA connection (failover or parallel)
            assertTrue(conn instanceof PhoenixConnection);
        }
    }

    /**
     * Test that poller should be running when we detect a non-active role record and should stop
     * when we detect an active role record
     */
    @Test
    public void testPollerShouldBeRunning() throws Exception {
        // get and initialize a new HA group
        String haGroupName2 = testName.getMethodName() + RandomStringUtils.randomAlphabetic(3);
        clientProperties.setProperty(PHOENIX_HA_GROUP_ATTR, haGroupName2);
        CLUSTERS.initClusterRole(haGroupName2, HighAvailabilityPolicy.FAILOVER);
        HighAvailabilityGroup haGroup2 = HighAvailabilityGroup.get(jdbcHAUrl, clientProperties).get();
        //url1 should be active and url2 should be standby
        assertTrue(haGroup2.getRoleRecord().getRole(JDBCUtil.formatUrl(CLUSTERS.getMasterAddress1(), registryType)).isActive());
        assertEquals(haGroup2.getRoleRecord().getRole(JDBCUtil.formatUrl(CLUSTERS.getMasterAddress2(), registryType)), ClusterRole.STANDBY);

        //transit url1 to standby and refresh the roleRecord as well which should have started the poller
        //as there is no ACTIVE role in the new record
        CLUSTERS.transitClusterRole(haGroup2, ClusterRole.STANDBY, ClusterRole.STANDBY);
        assertEquals(haGroup2.getRoleRecord().getRole(JDBCUtil.formatUrl(CLUSTERS.getMasterAddress1(), registryType)), ClusterRole.STANDBY);
        assertEquals(haGroup2.getRoleRecord().getRole(JDBCUtil.formatUrl(CLUSTERS.getMasterAddress2(), registryType)), ClusterRole.STANDBY);

        //transit url1 to active and url2 to standby and don't refresh the roleRecord of haGroup2
        CLUSTERS.transitClusterRole(haGroup2, ClusterRole.ACTIVE, ClusterRole.STANDBY, false);

        //Sleep to let poller to detect the change and refresh the roleRecord
        sleepThreadFor(5000);

        assertEquals(haGroup2.getRoleRecord().getRole(JDBCUtil.formatUrl(CLUSTERS.getMasterAddress1(), registryType)), ClusterRole.ACTIVE);
        assertEquals(haGroup2.getRoleRecord().getRole(JDBCUtil.formatUrl(CLUSTERS.getMasterAddress2(), registryType)), ClusterRole.STANDBY);

        //transit url1 to standby and url2 to active and don't refresh the roleRecord of haGroup2
        CLUSTERS.transitClusterRole(haGroup2, ClusterRole.STANDBY, ClusterRole.ACTIVE, false);
        //Poller should have stopped and roleRecord should not have changed
        assertEquals(haGroup2.getRoleRecord().getRole(JDBCUtil.formatUrl(CLUSTERS.getMasterAddress1(), registryType)), ClusterRole.ACTIVE);
        assertEquals(haGroup2.getRoleRecord().getRole(JDBCUtil.formatUrl(CLUSTERS.getMasterAddress2(), registryType)), ClusterRole.STANDBY);

    }

}
