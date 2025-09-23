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

import static org.apache.phoenix.exception.SQLExceptionCode.CANNOT_ESTABLISH_CONNECTION;
import static org.apache.phoenix.jdbc.HighAvailabilityGroup.*;
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.doTestBasicOperationsWithConnection;
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.getHighAvailibilityGroup;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.exception.PhoenixIOException;
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
        jdbcHAUrl = CLUSTERS.getJdbcHAUrl();

        // Make first cluster ACTIVE
        CLUSTERS.initClusterRole(haGroupName, HighAvailabilityPolicy.FAILOVER);
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
     * Test get static method.
     */
    @Test
    public void testGet() throws Exception {
        // Client will get the same HighAvailabilityGroup using the same information as key
        Optional<HighAvailabilityGroup> haGroup2 = Optional.empty();
        try {
            haGroup2 = HighAvailabilityGroup.get(jdbcHAUrl, clientProperties);
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
            haGroup3 = HighAvailabilityGroup.get(jdbcHAUrl, clientProperties);
            assertTrue(haGroup3.isPresent());
            assertNotSame(haGroup, haGroup3.get());
        } finally {
            haGroup3.ifPresent(HighAvailabilityGroup::close);
        }

        // Client will get the same HighAvailabilityGroup using the same information as key again
        clientProperties.setProperty(PHOENIX_HA_GROUP_ATTR, haGroup.getGroupInfo().getName());
        Optional<HighAvailabilityGroup> haGroup4 = Optional.empty();
        try {
            haGroup4 = HighAvailabilityGroup.get(jdbcHAUrl, clientProperties);
            assertTrue(haGroup4.isPresent());
            assertSame(haGroup, haGroup4.get());
        } finally {
            haGroup4.ifPresent(HighAvailabilityGroup::close);
        }
    }

    /**
     * Test HAGroup.get() method to get same HAGroups if we have different principals only and different HAGroups
     * if anything else in the key changes
     * @throws Exception
     */
    @Test
    public void testGetWithDifferentPrincipals() throws Exception {
        //Client will get same HAGroup if we have difference in principal only but JDBCURLs should have different
        //principals
        assertJDBCUrlForGivenHAGroup(haGroup, haURLInfo, null);

        //Try creating new HAGroup with same params except Principal
        Optional<HighAvailabilityGroup> haGroup2 = Optional.empty();
        try {
            String principal = RandomStringUtils.randomAlphabetic(5);
            String haUrl2 = CLUSTERS.getJdbcHAUrl(principal);
            HAURLInfo haURLInfo2 = HighAvailabilityGroup.getUrlInfo(haUrl2, clientProperties);
            haGroup2 = HighAvailabilityGroup.get(haUrl2, clientProperties);
            assertTrue(haGroup2.isPresent());
            //We should get same HAGroup as we have mapping of <HAGroupName, urls> -> HAGroup
            assertSame(haGroup, haGroup2.get());

            //URLs we are getting for haGroup2 should have newer principal i.e. Current HAURLInfo should have new
            //principal instead default PRINCIPAL
            assertJDBCUrlForGivenHAGroup(haGroup2.get(), haURLInfo2, principal);
        } finally {
            haGroup2.ifPresent(HighAvailabilityGroup::close);
        }

        // Client will get a different HighAvailabilityGroup when group name is different and with same principal as
        // default HAGroup
        String haGroupName3 = testName.getMethodName() + RandomStringUtils.randomAlphabetic(3);
        CLUSTERS.initClusterRole(haGroupName3, HighAvailabilityPolicy.FAILOVER);
        clientProperties.setProperty(PHOENIX_HA_GROUP_ATTR, haGroupName3);
        Optional<HighAvailabilityGroup> haGroup3 = Optional.empty();
        Optional<HighAvailabilityGroup> haGroup4 = Optional.empty();
        try {
            HAURLInfo haurlInfo3 = HighAvailabilityGroup.getUrlInfo(jdbcHAUrl, clientProperties);
            haGroup3 = HighAvailabilityGroup.get(jdbcHAUrl, clientProperties);
            assertTrue(haGroup3.isPresent());
            assertNotSame(haGroup, haGroup3.get());

            assertNotSame(haGroup.getGroupInfo(), haGroup3.get().getGroupInfo());

            //URLs we are getting for haGroup3 should have same principal as default PRINCIPAL.
            assertJDBCUrlForGivenHAGroup(haGroup3.get(), haurlInfo3, null);

            // should get same ha Group without principal
            String haUrl4 = CLUSTERS.getJdbcHAUrlWithoutPrincipal();
            HAURLInfo haURLInfo4 = HighAvailabilityGroup.getUrlInfo(haUrl4, clientProperties);
            haGroup4 = HighAvailabilityGroup.get(haUrl4, clientProperties);
            assertTrue(haGroup4.isPresent());
            assertNotSame(haGroup, haGroup4.get());
            assertSame(haGroup3.get(), haGroup4.get());

            assertNotSame(haGroup.getGroupInfo(), haGroup4.get().getGroupInfo());
            assertSame(haGroup3.get().getGroupInfo(), haGroup4.get().getGroupInfo());
            assertNotEquals(haurlInfo3, haURLInfo4);

        } finally {
            haGroup3.ifPresent(HighAvailabilityGroup::close);
            haGroup4.ifPresent(HighAvailabilityGroup::close);
        }

        // Client will get the same HighAvailabilityGroup using the same information as key again
        clientProperties.setProperty(PHOENIX_HA_GROUP_ATTR, haGroup.getGroupInfo().getName());
        Optional<HighAvailabilityGroup> haGroup5 = Optional.empty();
        try {
            //Again using a random principal which should be used now for generating jdbcUrls
            String principal = RandomStringUtils.randomAlphabetic(5);
            String haUrl5 = CLUSTERS.getJdbcHAUrl(principal);
            HAURLInfo haURLInfo5 = HighAvailabilityGroup.getUrlInfo(haUrl5, clientProperties);
            haGroup5 = HighAvailabilityGroup.get(haUrl5, clientProperties);
            assertTrue(haGroup5.isPresent());
            assertSame(haGroup, haGroup5.get());

            //URLs we are getting for haGroup4 should have newer principal i.e. Current HAURLInfo should have new
            //principal instead default PRINCIPAL
            assertJDBCUrlForGivenHAGroup(haGroup5.get(), haURLInfo5, principal);

        } finally {
            haGroup5.ifPresent(HighAvailabilityGroup::close);
        }

    }

    @Test
    public void testHAGroupMappings() throws Exception {

        //Try creating new HAGroup with same params except Principal
        Optional<HighAvailabilityGroup> haGroup2 = Optional.empty();
        try {
            String principal = RandomStringUtils.randomAlphabetic(5);
            String haUrl2 = CLUSTERS.getJdbcHAUrl(principal);
            HAURLInfo haURLInfo2 = HighAvailabilityGroup.getUrlInfo(haUrl2, clientProperties);
            haGroup2 = HighAvailabilityGroup.get(haUrl2, clientProperties);
            assertTrue(haGroup2.isPresent());
            //We should get same HAGroup as we have mapping of <HAGroupName, urls> -> HAGroup
            assertSame(haGroup, haGroup2.get());
            //We should have 2 values on URLS mapping for the given haGroup/haGroup2.
            assertEquals(2, URLS.get(haGroup.getGroupInfo()).size());
            assertTrue(URLS.get(haGroup.getGroupInfo()).contains(haURLInfo2));

        } finally {
            haGroup2.ifPresent(HighAvailabilityGroup::close);
        }

        //Create 2 more different urls connecting to a different HAGroup
        String haGroupName3 = testName.getMethodName() + RandomStringUtils.randomAlphabetic(3);
        CLUSTERS.initClusterRole(haGroupName3, HighAvailabilityPolicy.FAILOVER);
        clientProperties.setProperty(PHOENIX_HA_GROUP_ATTR, haGroupName3);
        Optional<HighAvailabilityGroup> haGroup3 = Optional.empty();
        Optional<HighAvailabilityGroup> haGroup4 = Optional.empty();
        try {
            HAURLInfo haurlInfo3 = HighAvailabilityGroup.getUrlInfo(jdbcHAUrl, clientProperties);
            haGroup3 = HighAvailabilityGroup.get(jdbcHAUrl, clientProperties);
            assertTrue(haGroup3.isPresent());
            assertNotSame(haGroup, haGroup3.get());
            assertNotSame(haGroup.getGroupInfo(), haGroup3.get().getGroupInfo());
            assertEquals(1, URLS.get(haGroup3.get().getGroupInfo()).size());


            // should get same ha Group without principal
            String haUrl4 = CLUSTERS.getJdbcHAUrlWithoutPrincipal();
            HAURLInfo haURLInfo4 = HighAvailabilityGroup.getUrlInfo(haUrl4, clientProperties);
            haGroup4 = HighAvailabilityGroup.get(haUrl4, clientProperties);
            assertTrue(haGroup4.isPresent());
            assertNotSame(haGroup, haGroup4.get());
            assertSame(haGroup3.get(), haGroup4.get());
            assertEquals(2, URLS.get(haGroup4.get().getGroupInfo()).size());

            assertNotEquals(haurlInfo3, haURLInfo4);

        } finally {
            haGroup3.ifPresent(HighAvailabilityGroup::close);
            haGroup4.ifPresent(HighAvailabilityGroup::close);
        }

        // Client will get the same HighAvailabilityGroup using the same information as key again
        clientProperties.setProperty(PHOENIX_HA_GROUP_ATTR, haGroup.getGroupInfo().getName());
        Optional<HighAvailabilityGroup> haGroup5 = Optional.empty();
        try {
            String haUrl5 = CLUSTERS.getJdbcHAUrl();
            HAURLInfo haURLInfo5 = HighAvailabilityGroup.getUrlInfo(haUrl5, clientProperties);
            haGroup5 = HighAvailabilityGroup.get(haUrl5, clientProperties);
            assertTrue(haGroup5.isPresent());
            assertSame(haGroup, haGroup5.get());

            //haURLInfo5 should be same as global one and URLS mapping should not change so
            //set mapping for global HAGroupInfo should have 2 values
            assertEquals(2, URLS.get(haGroup.getGroupInfo()).size());
            assertTrue(URLS.get(haGroup.getGroupInfo()).contains(haURLInfo5));
            assertEquals(haURLInfo5, haURLInfo);


        } finally {
            haGroup5.ifPresent(HighAvailabilityGroup::close);
        }

    }

    /**
     * Test that client can get an HA group when ACTIVE HBase cluster is down.
     *
     * NOTE: we can not test with existing HA group because {@link HighAvailabilityGroup#get} would
     * get the cached object, which has also been initialized.
     *
     * The reason this works is because getting an HA group depends on doing rpc calls to both clusters
     * to get the ClusterRoleRecord. If one cluster fails, the other cluster should be able to return
     * clusterRoleRecord and start the HA group.
     */
    @Test
    public void testCanGetHaGroupWhenOneHBaseClusterDown() throws Exception {
        String haGroupName2 = testName.getMethodName() + 2;
        clientProperties.setProperty(PHOENIX_HA_GROUP_ATTR, haGroupName2);
        CLUSTERS.initClusterRole(haGroupName2, HighAvailabilityPolicy.FAILOVER);

        //Try with ACTIVE HBase cluster down
        CLUSTERS.doTestWhenOneZKDown(CLUSTERS.getHBaseCluster1(), () -> {
            Optional<HighAvailabilityGroup> haGroup2 = null;
            try {
                haGroup2 = HighAvailabilityGroup.get(jdbcHAUrl, clientProperties);
                LOG.info("Can get the new HA group {} after both ZK clusters restart", haGroup2);
            } finally {
                if (haGroup2 != null) {
                    haGroup2.get().close();
                    //Clear caches
                    URLS.remove(haGroup2.get().getGroupInfo());
                    GROUPS.remove(haGroup2.get().getGroupInfo());
                }
            }
        });

        //Try with STANDBY HBase cluster down
        CLUSTERS.doTestWhenOneHBaseDown(CLUSTERS.getHBaseCluster2(), () -> {
            Optional<HighAvailabilityGroup> haGroup2 = null;
            try {
                haGroup2 = HighAvailabilityGroup.get(jdbcHAUrl, clientProperties);
                LOG.info("Can get the new HA group {} after both ZK clusters restart", haGroup2);
            } finally {
                if (haGroup2 != null) {
                    haGroup2.get().close();
                    //Clear caches
                    URLS.remove(haGroup2.get().getGroupInfo());
                    GROUPS.remove(haGroup2.get().getGroupInfo());
                }
            }
        });

    }

    /**
     * Test that client can not get an HA group when both HBase clusters are down.
     *
     * NOTE: we can not test with existing HA group because {@link HighAvailabilityGroup#get} would
     * get the cached object, which has also been initialized.
     *
     * What if two HBase clusters instead of two ZK clusters are down? We may still get a new HA
     * group because creating and initializing HA group do not set up the HBase connection, which
     * should indeed fail when ACTIVE HBase cluster is down.
     */
    @Test
    public void testCanNotGetHaGroupWhenTwoHBaseClustersDown() throws Exception {
        String haGroupName2 = testName.getMethodName() + 2;
        clientProperties.setProperty(PHOENIX_HA_GROUP_ATTR, haGroupName2);
        CLUSTERS.initClusterRole(haGroupName2, HighAvailabilityPolicy.FAILOVER);

        CLUSTERS.doTestWhenOneZKDown(CLUSTERS.getHBaseCluster1(), () ->
            CLUSTERS.doTestWhenOneZKDown(CLUSTERS.getHBaseCluster2(), () -> {
                try {
                    HighAvailabilityGroup.get(jdbcHAUrl, clientProperties);
                    fail("Should have failed because both ZK cluster were shutdown!");
                } catch (SQLException e) {
                    LOG.info("Got expected SQLException because both ZK clusters are down", e);
                    assertEquals(CANNOT_ESTABLISH_CONNECTION.getErrorCode(), e.getErrorCode());
                    assertTrue(e.getCause() instanceof PhoenixIOException);
                    assertTrue(e.getCause().getCause() instanceof RetriesExhaustedException);
                }
            })
        );

        HighAvailabilityGroup haGroup2 = null;
        try {
            haGroup2 = getHighAvailibilityGroup(jdbcHAUrl, clientProperties);
            LOG.info("Can get the new HA group {} after both ZK clusters restart", haGroup2);
        } finally {
            if (haGroup2 != null) {
                haGroup2.close();
            }
        }
    }

    /**
     * Test that it should fail fast to get HA group if the cluster role information is not there.
     */
    @Test
    public void testGetShouldFailWithoutClusterRoleData() throws SQLException {
        String invalidHaGroupName = testName.getMethodName() + RandomStringUtils.randomAlphanumeric(3);
        clientProperties.setProperty(PHOENIX_HA_GROUP_ATTR, invalidHaGroupName);
        assertFalse(HighAvailabilityGroup.get(jdbcHAUrl, clientProperties).isPresent());
    }

    /**
     * Test with invalid High Availability connection string.
     */
    @Test
    public void testGetShouldFailWithNonHAJdbcString() {
        final String oldJdbcString = CLUSTERS.getJdbcUrl1(haGroup);
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
        Connection connection = haGroup.connect(clientProperties, haURLInfo);
        assertNotNull(connection);
        assertNotNull(connection.unwrap(FailoverPhoenixConnection.class));
    }

    /**
     * Test connect to one cluster and returns a Phoenix connection which can be wrapped.
     */
    @Test
    public void testConnectToOneCluster() throws SQLException {
        final String url = CLUSTERS.getJdbcUrl1(haGroup);
        PhoenixConnection connection = haGroup.connectToOneCluster(url, clientProperties, haURLInfo);
        assertEquals(url, connection.getURL());

        try {
            haGroup.connectToOneCluster(null, clientProperties, haURLInfo);
            fail("Should have failed since null is not in any HA group");
        } catch (Exception e) {
            LOG.info("Got expected exception with invalid null host url", e);
        }

        final String randomHostUrl = String.format("%s:%d",
                RandomStringUtils.randomAlphabetic(4), RandomUtils.nextInt(0,65536));
        try {
            haGroup.connectToOneCluster(randomHostUrl, clientProperties, haURLInfo);
            fail("Should have failed since '" + randomHostUrl + "' is not in HA group " + haGroup);
        } catch (SQLException e) {
            LOG.info("Got expected exception with invalid host url '{}'", randomHostUrl, e);
        }
    }

    /**
     * Test that it can connect to a given cluster in this HA group after ZK service restarts.
     *
     * Method {@link HighAvailabilityGroup#connectToOneCluster(String, Properties, HAURLInfo)} is used by
     * Phoenix HA framework to connect to one specific HBase cluster in this HA group.  The cluster
     * may not necessarily be in ACTIVE role.  For example, parallel HA connection needs to connect
     * to both clusters. This tests that it can connect to a specific ZK cluster after ZK restarts.
     */
    @Test
    public void testConnectToOneClusterAfterZKRestart() throws Exception {
        final String tableName = RandomStringUtils.randomAlphabetic(10);
        CLUSTERS.createTableOnClusterPair(haGroup, tableName);

        final String jdbcUrlToCluster1 = CLUSTERS.getJdbcUrl1(haGroup);
        CLUSTERS.doTestWhenOneZKDown(CLUSTERS.getHBaseCluster1(), () -> {
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
        try (Connection conn = haGroup.connectToOneCluster(jdbcUrlToCluster1, clientProperties, haURLInfo)) {
            doTestBasicOperationsWithConnection(conn, tableName, haGroupName);
        }
    }

    /**
     * Test {@link HighAvailabilityGroup#isActive(PhoenixConnection)}.
     */
    @Test
    public void testIsConnectionActive() throws Exception {
        PhoenixConnection conn1 = haGroup.connectToOneCluster(CLUSTERS.getJdbcUrl1(haGroup), clientProperties, haURLInfo);
        assertTrue(haGroup.isActive(conn1));
        PhoenixConnection conn2 = haGroup.connectToOneCluster(CLUSTERS.getJdbcUrl2(haGroup), clientProperties, haURLInfo);
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
        assertEquals(ClusterRole.ACTIVE, haGroup.getRoleRecord().getRole(CLUSTERS.getURL(1, registryType)));
        assertEquals(ClusterRole.STANDBY, haGroup.getRoleRecord().getRole(CLUSTERS.getURL(2, registryType)));

        CLUSTERS.transitClusterRole(haGroup, ClusterRole.STANDBY, ClusterRole.ACTIVE);

        assertEquals(ClusterRole.STANDBY, haGroup.getRoleRecord().getRole(CLUSTERS.getURL(1, registryType)));
        assertEquals(ClusterRole.ACTIVE, haGroup.getRoleRecord().getRole(CLUSTERS.getURL(2, registryType)));
    }

    //private helper methods
    private void assertJDBCUrlForGivenHAGroup(HighAvailabilityGroup haGroup,
                                              HAURLInfo haURLInfo, String testPrincipal) {
        String testUrl1 = testPrincipal == null ? CLUSTERS.getJdbcUrl1(haGroup) :
                CLUSTERS.getJdbcUrl1(haGroup, testPrincipal);
        String testUrl2 = testPrincipal == null ? CLUSTERS.getJdbcUrl2(haGroup) :
                CLUSTERS.getJdbcUrl2(haGroup, testPrincipal);
        String testHAUrl = testPrincipal == null ? CLUSTERS.getJdbcHAUrl() :
                CLUSTERS.getJdbcHAUrl(testPrincipal);

        //As roleRecord store url in ascending order it might be the case that minicluster2 have
        //port smaller than minicluster1 and urls are stored in reverse order.
        assertTrue(getJdbcUrl1ForGivenHAGroup(haGroup, haURLInfo).equals(testUrl1) ||
                getJdbcUrl1ForGivenHAGroup(haGroup, haURLInfo).equals(testUrl2));
        assertTrue(getJdbcUrl2ForGivenHAGroup(haGroup, haURLInfo).equals(testUrl1) ||
                getJdbcUrl2ForGivenHAGroup(haGroup, haURLInfo).equals(testUrl2));
        assertEquals(testHAUrl, getJdbcHAUrlForGivenHAGroup(haURLInfo));
    }

    private String getJdbcUrl1ForGivenHAGroup(HighAvailabilityGroup haGroup, HAURLInfo haURLInfo) {
        return HighAvailabilityGroup.getJDBCUrl(haGroup.getRoleRecord().getUrl1(), haURLInfo,
                haGroup.getRoleRecord().getRegistryType());
    }

    private String getJdbcUrl2ForGivenHAGroup(HighAvailabilityGroup haGroup, HAURLInfo haURLInfo) {
        return HighAvailabilityGroup.getJDBCUrl(haGroup.getRoleRecord().getUrl2(), haURLInfo,
                haGroup.getRoleRecord().getRegistryType());
    }

    private String getJdbcHAUrlForGivenHAGroup(HAURLInfo haURLInfo) {
        return HighAvailabilityGroup.getJDBCHAUrl(CLUSTERS.getMasterAddress1(), CLUSTERS.getMasterAddress2(), haURLInfo);
    }
}
