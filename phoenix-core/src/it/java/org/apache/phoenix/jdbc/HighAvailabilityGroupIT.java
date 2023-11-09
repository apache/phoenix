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
import static org.apache.phoenix.jdbc.HighAvailabilityGroup.*;
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.HBaseTestingUtilityPair.doTestWhenOneHBaseDown;
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.HBaseTestingUtilityPair.doTestWhenOneZKDown;
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.doTestBasicOperationsWithConnection;
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.getHighAvailibilityGroup;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.ClusterRoleRecord.ClusterRole;
import org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.HBaseTestingUtilityPair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for {@link HighAvailabilityGroup} with mini clusters.
 *
 * @see HighAvailabilityGroupTestIT
 */
@SuppressWarnings("UnstableApiUsage")
@Category(NeedsOwnMiniClusterTest.class)
public class HighAvailabilityGroupIT {
    private static final Logger LOG = LoggerFactory.getLogger(HighAvailabilityGroupIT.class);
    private static final HBaseTestingUtilityPair CLUSTERS = new HBaseTestingUtilityPair();

    /** Client properties to create a connection per test. */
    private Properties clientProperties;
    /** JDBC connection string for this test HA group. */
    private String jdbcUrl;
    /** Failover HA group for to test. */
    private HighAvailabilityGroup haGroup;
    /** HA Group name for this test. */
    private String haGroupName;

    @Rule
    public final TestName testName = new TestName();
    @Rule
    public final Timeout globalTimeout = new Timeout(180, TimeUnit.SECONDS);

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
        jdbcUrl = CLUSTERS.getJdbcHAUrl();
        haGroup = getHighAvailibilityGroup(jdbcUrl,clientProperties);
    }

    @After
    public void tearDown() throws Exception {
        haGroup.close();
        try {
            PhoenixDriver.INSTANCE
                    .getConnectionQueryServices(CLUSTERS.getJdbcUrl1(), haGroup.getProperties())
                    .close();
            PhoenixDriver.INSTANCE
                    .getConnectionQueryServices(CLUSTERS.getJdbcUrl2(), haGroup.getProperties())
                    .close();
        } catch (Exception e) {
            LOG.error("Fail to tear down the HA group and the CQS. Will ignore", e);
        }
    }

    /**
     * Test get static method.
     */
    @Test
    public void testGet() throws Exception {
        // Client will get the same HighAvailabilityGroup using the same information as key
        Optional<HighAvailabilityGroup> haGroup2 = Optional.empty();
        try {
            haGroup2 = HighAvailabilityGroup.get(jdbcUrl, clientProperties);
            assertTrue(haGroup2.isPresent());
            assertSame(haGroup, haGroup2.get());
        } finally {
            haGroup2.ifPresent(HighAvailabilityGroup::close);
        }

        // Client will get a different HighAvailabilityGroup when group name is different
        String haGroupName3 = testName.getMethodName() + RandomStringUtils.randomAlphabetic(3);
        CLUSTERS.initClusterRole(haGroupName3, HighAvailabilityPolicy.FAILOVER);
        clientProperties.setProperty(PHOENIX_HA_GROUP_ATTR, haGroupName3);
        Optional<HighAvailabilityGroup> haGroup3 = Optional.empty();
        try {
            haGroup3 = HighAvailabilityGroup.get(jdbcUrl, clientProperties);
            assertTrue(haGroup3.isPresent());
            assertNotSame(haGroup, haGroup3.get());
        } finally {
            haGroup3.ifPresent(HighAvailabilityGroup::close);
        }

        // Client will get the same HighAvailabilityGroup using the same information as key again
        clientProperties.setProperty(PHOENIX_HA_GROUP_ATTR, haGroup.getGroupInfo().getName());
        Optional<HighAvailabilityGroup> haGroup4 = Optional.empty();
        try {
            haGroup4 = HighAvailabilityGroup.get(jdbcUrl, clientProperties);
            assertTrue(haGroup4.isPresent());
            assertSame(haGroup, haGroup4.get());
        } finally {
            haGroup4.ifPresent(HighAvailabilityGroup::close);
        }
    }

    /**
     * Test that HA group should see latest version of cluster role record.
     */
    @Test
    public void testGetWithDifferentRecordVersion() throws Exception {
        String haGroupName2 = testName.getMethodName() + RandomStringUtils.randomAlphabetic(3);
        clientProperties.setProperty(PHOENIX_HA_GROUP_ATTR, haGroupName2);

        // create cluster role records with different versions on two ZK clusters
        final String zpath = ZKPaths.PATH_SEPARATOR + haGroupName2;
        ClusterRoleRecord record1 = new ClusterRoleRecord(
                haGroupName2, HighAvailabilityPolicy.FAILOVER,
                CLUSTERS.getUrl1(), ClusterRole.ACTIVE,
                CLUSTERS.getUrl2(), ClusterRole.STANDBY,
                1);
        CLUSTERS.createCurator1().create().forPath(zpath, ClusterRoleRecord.toJson(record1));
        ClusterRoleRecord record2 = new ClusterRoleRecord(
                record1.getHaGroupName(), record1.getPolicy(),
                record1.getZk1(), record1.getRole1(),
                record1.getZk2(), record1.getRole2(),
                record1.getVersion() + 1); // record2 is newer
        CLUSTERS.createCurator2().create().forPath(zpath, ClusterRoleRecord.toJson(record2));

        Optional<HighAvailabilityGroup> haGroup2 = Optional.empty();
        try {
            haGroup2 = HighAvailabilityGroup.get(jdbcUrl, clientProperties);
            assertTrue(haGroup2.isPresent());
            assertNotSame(haGroup2.get(), haGroup);
            // HA group should see latest version when both role managers are started
            HighAvailabilityGroup finalHaGroup = haGroup2.get();
            waitFor(() -> record2.equals(finalHaGroup.getRoleRecord()), 100, 30_000);
        } finally {
            haGroup2.ifPresent(HighAvailabilityGroup::close);
        }
    }

    /**
     * Test that client can get an HA group when ACTIVE ZK cluster is down.
     *
     * NOTE: we can not test with existing HA group because {@link HighAvailabilityGroup#get} would
     * get the cached object, which has also been initialized.
     *
     * The reason this works is because getting an HA group depends on one ZK watcher connects to a
     * ZK cluster to get the associated cluster role record. It does not depend on both ZK cluster
     * being up and running. It does not actually create HBase connection either.
     */
    @Test
    public void testCanGetHaGroupWhenActiveZKClusterDown() throws Exception {
        String haGroupName2 = testName.getMethodName() + 2;
        clientProperties.setProperty(PHOENIX_HA_GROUP_ATTR, haGroupName2);
        CLUSTERS.initClusterRole(haGroupName2, HighAvailabilityPolicy.FAILOVER);

      doTestWhenOneZKDown(CLUSTERS.getHBaseCluster1(), () -> {
          HighAvailabilityGroup haGroup2 = null;
          try {
              haGroup2 = getHighAvailibilityGroup(jdbcUrl, clientProperties);
              LOG.info("Can get the new HA group {} after both ZK clusters restart", haGroup2);
          } finally {
              if (haGroup2 != null) {
                  haGroup2.close();
              }
          }
      });
    }

    /**
     * Test that client can not get an HA group when both ZK clusters are down.
     *
     * NOTE: we can not test with existing HA group because {@link HighAvailabilityGroup#get} would
     * get the cached object, which has also been initialized.
     *
     * What if two HBase clusters instead of two ZK clusters are down? We may still get a new HA
     * group because creating and initializing HA group do not set up the HBase connection, which
     * should indeed fail when ACTIVE HBase cluster is down.
     */
    @Test
    public void testCanNotGetHaGroupWhenTwoZKClustersDown() throws Exception {
        String haGroupName2 = testName.getMethodName() + 2;
        clientProperties.setProperty(PHOENIX_HA_GROUP_ATTR, haGroupName2);
        CLUSTERS.initClusterRole(haGroupName2, HighAvailabilityPolicy.FAILOVER);

        doTestWhenOneZKDown(CLUSTERS.getHBaseCluster1(), () ->
            doTestWhenOneZKDown(CLUSTERS.getHBaseCluster2(), () -> {
                try {
                    HighAvailabilityGroup.get(jdbcUrl, clientProperties);
                    fail("Should have failed because both ZK cluster were shutdown!");
                } catch (SQLException e) {
                    LOG.info("Got expected SQLException because both ZK clusters are down", e);
                    assertEquals(CANNOT_ESTABLISH_CONNECTION.getErrorCode(), e.getErrorCode());
                    assertTrue(e.getCause() instanceof IOException);
                }
            })
        );

        HighAvailabilityGroup haGroup2 = null;
        try {
            haGroup2 = getHighAvailibilityGroup(jdbcUrl, clientProperties);
            LOG.info("Can get the new HA group {} after both ZK clusters restart", haGroup2);
        } finally {
            if (haGroup2 != null) {
                haGroup2.close();
            }
        }
    }

    @Test(timeout = (4 * HighAvailabilityGroup.PHOENIX_HA_ZK_CONNECTION_TIMEOUT_MS_DEFAULT))
    public void testGetHaGroupFailsFastWhenBothZKClusterDownFromBeginning() {
        String haGroupName = testName.getMethodName();
        clientProperties.setProperty(PHOENIX_HA_GROUP_ATTR, haGroupName);
        String badJdbcUrl = String.format("jdbc:phoenix:[%s|%s]", "127.0.0.1:0", "127.0.0.1:1");
        LOG.info("Start testing HighAvailabilityGroup::get() when both ZK clusters are down...");
        try {
            HighAvailabilityGroup.get(badJdbcUrl, clientProperties);
            fail("Should always throw an exception.");
        } catch (SQLException e){
            LOG.info("Got expected exception", e);
            assertEquals(CANNOT_ESTABLISH_CONNECTION.getErrorCode(), e.getErrorCode());
        } finally {
            LOG.info("Stop testing HighAvailabilityGroup::get() when both ZK clusters are down...");
            long maxTime = (4 * HighAvailabilityGroup.PHOENIX_HA_ZK_CONNECTION_TIMEOUT_MS_DEFAULT);
        }
    }

    /**
     * Test that it should fail fast to get HA group if the cluster role information is not there.
     */
    @Test
    public void testGetShouldFailWithoutClusterRoleData() throws SQLException {
        String invalidHaGroupName = testName.getMethodName() + RandomStringUtils.randomAlphanumeric(3);
        clientProperties.setProperty(PHOENIX_HA_GROUP_ATTR, invalidHaGroupName);
        assertFalse(HighAvailabilityGroup.get(jdbcUrl, clientProperties).isPresent());
    }

    /**
     * Test with invalid High Availability connection string.
     */
    @Test
    public void testGetShouldFailWithNonHAJdbcString() {
        final String oldJdbcString = CLUSTERS.getJdbcUrl1();
        try {
            HighAvailabilityGroup.get(oldJdbcString, clientProperties);
            fail("Should have failed with invalid connection string '" + oldJdbcString + "'");
        } catch (SQLException e) {
            LOG.info("Got expected exception with invalid connection string {}", oldJdbcString, e);
            assertEquals(SQLExceptionCode.MALFORMED_CONNECTION_URL.getErrorCode(), e.getErrorCode());
        }
    }

    /**
     * Test that we can connect to this HA group to get a JDBC connection.
     */
    @Test
    public void testConnect() throws SQLException {
        Connection connection = haGroup.connect(clientProperties);
        assertNotNull(connection);
        assertNotNull(connection.unwrap(FailoverPhoenixConnection.class));
    }

    /**
     * Test connect to one cluster and returns a Phoenix connection which can be wrapped.
     */
    @Test
    public void testConnectToOneCluster() throws SQLException {
        final String url = CLUSTERS.getJdbcUrl1();
        PhoenixConnection connection = haGroup.connectToOneCluster(url, clientProperties);
        assertEquals(url, connection.getURL());

        try {
            haGroup.connectToOneCluster(null, clientProperties);
            fail("Should have failed since null is not in any HA group");
        } catch (Exception e) {
            LOG.info("Got expected exception with invalid null host url", e);
        }

        final String randomHostUrl = String.format("%s:%d",
                RandomStringUtils.randomAlphabetic(4), RandomUtils.nextInt(0,65536));
        try {
            haGroup.connectToOneCluster(randomHostUrl, clientProperties);
            fail("Should have failed since '" + randomHostUrl + "' is not in HA group " + haGroup);
        } catch (IllegalArgumentException e) {
            LOG.info("Got expected exception with invalid host url '{}'", randomHostUrl, e);
        }
    }

    /**
     * Test that it can connect to a given cluster in this HA group after ZK service restarts.
     *
     * Method {@link HighAvailabilityGroup#connectToOneCluster(String, Properties)} is used by
     * Phoenix HA framework to connect to one specific HBase cluster in this HA group.  The cluster
     * may not necessarily be in ACTIVE role.  For example, parallel HA connection needs to connect
     * to both clusters. This tests that it can connect to a specific ZK cluster after ZK restarts.
     */
    @Test
    public void testConnectToOneClusterAfterZKRestart() throws Exception {
        final String tableName = testName.getMethodName();
        CLUSTERS.createTableOnClusterPair(tableName);

        final String url1 = CLUSTERS.getUrl1();
        final String jdbcUrlToCluster1 = CLUSTERS.getJdbcUrl1();
        doTestWhenOneZKDown(CLUSTERS.getHBaseCluster1(), () -> {
            try {
                DriverManager.getConnection(jdbcUrlToCluster1);
            } catch (SQLException e) {
                LOG.info("Got expected IOException when creating Phoenix connection", e);
            }
        });

        // test with plain JDBC connection after cluster restarts
        try (Connection conn = DriverManager.getConnection(jdbcUrlToCluster1)) {
            doTestBasicOperationsWithConnection(conn, tableName, null);
        }
        // test with HA group to get connection to one cluster
        try (Connection conn = haGroup.connectToOneCluster(jdbcUrlToCluster1, clientProperties)) {
            doTestBasicOperationsWithConnection(conn, tableName, haGroupName);
        }
    }

    /**
     * Test {@link HighAvailabilityGroup#isActive(PhoenixConnection)}.
     */
    @Test
    public void testIsConnectionActive() throws Exception {
        PhoenixConnection conn1 = haGroup.connectToOneCluster(CLUSTERS.getUrl1(), clientProperties);
        assertTrue(haGroup.isActive(conn1));
        PhoenixConnection conn2 = haGroup.connectToOneCluster(CLUSTERS.getUrl2(), clientProperties);
        assertFalse(haGroup.isActive(conn2));

        CLUSTERS.transitClusterRole(haGroup, ClusterRole.STANDBY, ClusterRole.ACTIVE);

        assertFalse(haGroup.isActive(conn1));
        assertTrue(haGroup.isActive(conn2));
    }

    /**
     * Test that when node changes, the high availability group will detect and issue state change.
     */
    @Test
    public void testNodeChange() throws Exception {
        assertEquals(ClusterRole.ACTIVE, haGroup.getRoleRecord().getRole(CLUSTERS.getUrl1()));
        assertEquals(ClusterRole.STANDBY, haGroup.getRoleRecord().getRole(CLUSTERS.getUrl2()));

        CLUSTERS.transitClusterRole(haGroup, ClusterRole.STANDBY, ClusterRole.ACTIVE);

        assertEquals(ClusterRole.STANDBY, haGroup.getRoleRecord().getRole(CLUSTERS.getUrl1()));
        assertEquals(ClusterRole.ACTIVE, haGroup.getRoleRecord().getRole(CLUSTERS.getUrl2()));
    }

    /**
     * Test that if STANDBY HBase cluster is down, the connect should work.
     */
    @Test
    public void testCanConnectWhenStandbyHBaseClusterDown() throws Exception {
        doTestWhenOneHBaseDown(CLUSTERS.getHBaseCluster2(), () -> {
            // HA group is already initialized
            Connection connection = haGroup.connect(clientProperties);
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
            Connection connection = haGroup.connect(clientProperties);
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
                haGroup2 = HighAvailabilityGroup.get(jdbcUrl, clientProperties);
                assertTrue(haGroup2.isPresent());
                assertNotSame(haGroup2.get(), haGroup);
                // get a new connection in this new HA group; should be pointing to ACTIVE cluster1
                try (Connection connection = haGroup2.get().connect(clientProperties)) {
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
               haGroup2 = HighAvailabilityGroup.get(jdbcUrl, clientProperties);
               assertTrue(haGroup2.isPresent());
               assertNotSame(haGroup2.get(), haGroup);
               // get a new connection in this new HA group; should be pointing to ACTIVE cluster1
               Connection connection = haGroup2.get().connect(clientProperties);
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
                haGroup.connectActive(clientProperties);
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
                haGroup.connectActive(clientProperties);
                fail("Should have failed because of ACTIVE ZK cluster is down.");
            } catch (SQLException e) {
                LOG.info("Got expected exception when ACTIVE ZK cluster is down", e);
                assertEquals(CANNOT_ESTABLISH_CONNECTION.getErrorCode(), e.getErrorCode());
            }
        });

        try (Connection conn = haGroup.connectActive(clientProperties)) {
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
                haGroupName2, HighAvailabilityPolicy.FAILOVER,
                CLUSTERS.getUrl1(), ClusterRole.ACTIVE,
                CLUSTERS.getUrl2(), ClusterRole.STANDBY,
                1);
        CLUSTERS.createCurator1().create().forPath(zpath, ClusterRoleRecord.toJson(record1));
        ClusterRoleRecord record2 = new ClusterRoleRecord(
                record1.getHaGroupName(), record1.getPolicy(),
                record1.getZk1(), record1.getRole1(),
                record1.getZk2(), record1.getRole2(),
                record1.getVersion() + 1); // record2 is newer
        CLUSTERS.createCurator2().create().forPath(zpath, ClusterRoleRecord.toJson(record2));

        doTestWhenOneZKDown(CLUSTERS.getHBaseCluster2(), () -> {
            Optional<HighAvailabilityGroup> haGroup2 = HighAvailabilityGroup.get(jdbcUrl, clientProperties);
            assertTrue(haGroup2.isPresent());
            assertNotSame(haGroup2.get(), haGroup);
            // apparently the HA group cluster role record should be from the healthy cluster
            assertEquals(record1, haGroup2.get().getRoleRecord());
        });

        // When ZK2 is connected, its cluster role manager should apply newer cluster role record
        waitFor(() -> {
            try {
                Optional<HighAvailabilityGroup> haGroup2 = HighAvailabilityGroup.get(jdbcUrl, clientProperties);
                return haGroup2.isPresent() && record2.equals(haGroup2.get().getRoleRecord());
            } catch (SQLException e) {
                LOG.warn("Fail to get HA group {}", haGroupName2);
                return false;
            }
        }, 100, 30_000);

        // clean up HA group 2
        HighAvailabilityGroup.get(jdbcUrl, clientProperties).ifPresent(HighAvailabilityGroup::close);
    }

    /**
     * Test that HA connection request will fall back to the first cluster when HA group fails
     * to initialize due to missing cluster role record (CRR).
     */
    @Test
    public void testFallbackToSingleConnection() throws Exception {
        final String tableName = testName.getMethodName();
        CLUSTERS.createTableOnClusterPair(tableName);

        String haGroupName2 = testName.getMethodName() + RandomStringUtils.randomAlphabetic(3);
        clientProperties.setProperty(PHOENIX_HA_GROUP_ATTR, haGroupName2);
        // no cluster role record for the HA group with name haGroupName2
        try (Connection conn = DriverManager.getConnection(jdbcUrl, clientProperties)) {
            // connection is PhoenixConnection instead of HA connection (failover or parallel)
            assertTrue(conn instanceof PhoenixConnection);
            // Deterministically talking to the first cluster: first means "smaller" URL
            String firstClusterUrl = CLUSTERS.getUrl1();
            if (CLUSTERS.getUrl1().compareTo(CLUSTERS.getUrl2()) > 0) {
                firstClusterUrl = CLUSTERS.getUrl2();
            }
            assertEquals(firstClusterUrl, ((PhoenixConnection) conn).getURL());
            doTestBasicOperationsWithConnection(conn, tableName, haGroupName2);
        }

        // disable fallback feature even if the cluster role record is missing for this group
        clientProperties.setProperty(PHOENIX_HA_SHOULD_FALLBACK_WHEN_MISSING_CRR_KEY, "false");
        try {
            DriverManager.getConnection(jdbcUrl, clientProperties);
            fail("Should have failed when disabling fallback to single cluster");
        } catch (SQLException e) {
            LOG.info("Got expected exception when disabling fallback");
        }
        clientProperties.remove(PHOENIX_HA_SHOULD_FALLBACK_WHEN_MISSING_CRR_KEY);

        // Now create the cluster role record
        CLUSTERS.initClusterRole(haGroupName2, HighAvailabilityPolicy.FAILOVER);
        // And invalidate the negative cache. We are doing it manually because the default time for
        // expireAfterWrite is too long for testing. We do not need to test the cache per se.
        HighAvailabilityGroup.MISSING_CRR_GROUPS_CACHE.invalidateAll();

        // After it is expired in the negative cache, the HA group will start serving HA connections
        try (Connection conn = DriverManager.getConnection(jdbcUrl, clientProperties)) {
           assertTrue(conn instanceof FailoverPhoenixConnection);
           doTestBasicOperationsWithConnection(conn, tableName, haGroupName2);
        }
    }

    /**
     * Test that HA connection request will not fall back to the first cluster when one ZK is down.
     */
    @Test
    public void testNotFallbackToSingleConnection() throws Exception {
        final String tableName = testName.getMethodName();
        CLUSTERS.createTableOnClusterPair(tableName);
        String haGroupName2 = testName.getMethodName() + RandomStringUtils.randomAlphabetic(3);
        clientProperties.setProperty(PHOENIX_HA_GROUP_ATTR, haGroupName2);

        doTestWhenOneZKDown(CLUSTERS.getHBaseCluster2(), () -> {
            try {
                DriverManager.getConnection(jdbcUrl, clientProperties);
                fail("Should have failed since one HA group can not initialized. Not falling back");
            } catch (SQLException e) {
                LOG.info("Got expected exception as HA group fails to initialize", e);
            }
        });

        // After cluster 2 is up and running again, fallback to single cluster will work
        try (Connection conn = DriverManager.getConnection(jdbcUrl, clientProperties)) {
            // connection is PhoenixConnection instead of HA connection (failover or parallel)
            assertTrue(conn instanceof PhoenixConnection);
        }
    }
}
