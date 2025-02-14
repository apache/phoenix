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
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.HBaseTestingUtilityPair.doTestWhenOneHBaseDown;
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.HBaseTestingUtilityPair.doTestWhenOneZKDown;
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.doTestBasicOperationsWithConnection;
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.getHighAvailibilityGroup;
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
    private final ClusterRoleRecord.RegistryType registryType = ClusterRoleRecord.RegistryType.ZK;

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
        doTestWhenOneHBaseDown(CLUSTERS.getHBaseCluster2(), () -> {
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
        doTestWhenOneZKDown(CLUSTERS.getHBaseCluster2(), () -> {
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
        doTestWhenOneHBaseDown(CLUSTERS.getHBaseCluster2(), () -> {
            // get and initialize a new HA group when cluster2 is down
            String haGroupName2 = testName.getMethodName() + RandomStringUtils.randomAlphabetic(3);
            clientProperties.setProperty(PHOENIX_HA_GROUP_ATTR, haGroupName2);
            CLUSTERS.initClusterRole(haGroupName2, HighAvailabilityPolicy.FAILOVER);
            Optional<HighAvailabilityGroup> haGroup2 = Optional.empty();
            try {
                HAURLInfo haURLInfo = HighAvailabilityGroup.getUrlInfo(jdbcHAUrl, clientProperties);
                haGroup2 = HighAvailabilityGroup.get(jdbcHAUrl, clientProperties);
                assertTrue(haGroup2.isPresent());
                assertNotSame(haGroup2.get(), haGroup);
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

        doTestWhenOneZKDown(CLUSTERS.getHBaseCluster2(), () -> {
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
        doTestWhenOneHBaseDown(CLUSTERS.getHBaseCluster1(), () -> {
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
        doTestWhenOneZKDown(CLUSTERS.getHBaseCluster1(), () -> {
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
     * Test when one ZK starts after the HA group has been initialized.
     *
     * In this case, both cluster role managers will start and apply discovered cluster role record.
     */
    @Test
    public void testOneZKStartsAfterInit() throws Exception {
        String haGroupName2 = testName.getMethodName() + RandomStringUtils.randomAlphabetic(3);
        clientProperties.setProperty(PHOENIX_HA_GROUP_ATTR, haGroupName2);

        // create cluster role records with different versions on two ZK clusters
        final String zpath = ZKPaths.PATH_SEPARATOR + haGroupName2;
        ClusterRoleRecord record1 = new ClusterRoleRecord(
                haGroupName2, HighAvailabilityPolicy.FAILOVER, registryType,
                CLUSTERS.getURL(1, registryType), ClusterRoleRecord.ClusterRole.ACTIVE,
                CLUSTERS.getURL(2, registryType), ClusterRoleRecord.ClusterRole.STANDBY,
                1);
        CLUSTERS.createCurator1().create().forPath(zpath, ClusterRoleRecord.toJson(record1));
        ClusterRoleRecord record2 = new ClusterRoleRecord(
                record1.getHaGroupName(), record1.getPolicy(), record1.getRegistryType(),
                record1.getUrl1(), record1.getRole1(),
                record1.getUrl2(), record1.getRole2(),
                record1.getVersion() + 1); // record2 is newer
        CLUSTERS.createCurator2().create().forPath(zpath, ClusterRoleRecord.toJson(record2));

        doTestWhenOneZKDown(CLUSTERS.getHBaseCluster2(), () -> {
            Optional<HighAvailabilityGroup> haGroup2 = HighAvailabilityGroup.get(jdbcHAUrl, clientProperties);
            assertTrue(haGroup2.isPresent());
            assertNotSame(haGroup2.get(), haGroup);
            // apparently the HA group cluster role record should be from the healthy cluster
            assertEquals(record1, haGroup2.get().getRoleRecord());
        });

        // When ZK2 is connected, its cluster role manager should apply newer cluster role record
        waitFor(() -> {
            try {
                Optional<HighAvailabilityGroup> haGroup2 = HighAvailabilityGroup.get(jdbcHAUrl, clientProperties);
                return haGroup2.isPresent() && record2.equals(haGroup2.get().getRoleRecord());
            } catch (SQLException e) {
                LOG.warn("Fail to get HA group {}", haGroupName2);
                return false;
            }
        }, 100, 30_000);

        // clean up HA group 2
        HighAvailabilityGroup.get(jdbcHAUrl, clientProperties).ifPresent(HighAvailabilityGroup::close);
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

        doTestWhenOneZKDown(CLUSTERS.getHBaseCluster2(), () -> {
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
}
