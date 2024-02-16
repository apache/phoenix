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

import static org.apache.phoenix.jdbc.HighAvailabilityGroup.HAGroupInfo;
import static org.apache.phoenix.jdbc.HighAvailabilityGroup.PHOENIX_HA_GROUP_ATTR;
import static org.apache.phoenix.jdbc.HighAvailabilityGroup.State.READY;
import static org.apache.phoenix.jdbc.HighAvailabilityGroup.State.UNINITIALIZED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.ExistsBuilder;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.ClusterRoleRecord.ClusterRole;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.ConnectionQueryServicesImpl;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit test for {@link HighAvailabilityGroup}.
 *
 * This does not require a mini HBase cluster.  Instead, it uses mocked components.
 *
 * @see HighAvailabilityGroupIT
 */
@Category(NeedsOwnMiniClusterTest.class)
public class HighAvailabilityGroupTestIT {

    // TODO: This is not an IT but can't run in parallel with other UTs, refactor
    // This test cannot be run in parallel since it registers/deregisters driver

    private static final Logger LOG = LoggerFactory.getLogger(HighAvailabilityGroupTestIT.class);
    private static final String ZK1 = "zk1-1\\:2181,zk1-2\\:2181::/hbase";
    private static final String ZK2 = "zk2-1\\:2181,zk2-2\\:2181::/hbase";
    private static final PhoenixEmbeddedDriver DRIVER = mock(PhoenixEmbeddedDriver.class);

    /** The client properties to create a JDBC connection. */
    private final Properties clientProperties = new Properties();
    /** The mocked cluster role record of the HA group. */
    private final ClusterRoleRecord record = mock(ClusterRoleRecord.class);
    /** The HA group to test. This is spied but not mocked. */
    private HighAvailabilityGroup haGroup;

    @Rule
    public final TestName testName = new TestName();
    @Rule
    public final Timeout globalTimeout= new Timeout(300, TimeUnit.SECONDS);

    @BeforeClass
    public static void setupBeforeClass() throws SQLException {
        // Mock a connection
        PhoenixConnection connection = mock(PhoenixConnection.class);
        when(connection.getURL()).thenReturn(ZK1);

        // Mock a CQS
        ConnectionQueryServices cqs = mock(ConnectionQueryServicesImpl.class);
        when(cqs.connect(anyString(), any(Properties.class))).thenReturn(connection);

        // Register the mocked PhoenixEmbeddedDriver
        when(DRIVER.acceptsURL(startsWith(PhoenixRuntime.JDBC_PROTOCOL))).thenReturn(true);
        when(DRIVER.getConnectionQueryServices(anyString(), any(Properties.class))).thenReturn(cqs);
        DriverManager.registerDriver(DRIVER);

        // Unregister the PhoenixDriver so that all Phoenix JDBC requests will get mocked
        DriverManager.deregisterDriver(PhoenixDriver.INSTANCE);
    }

    @Before
    public void init() {
        final String haGroupName = testName.getMethodName();
        // By default the HA policy is FAILOVER
        when(record.getPolicy()).thenReturn(HighAvailabilityPolicy.FAILOVER);
        when(record.getHaGroupName()).thenReturn(haGroupName);
        // Make ZK1 ACTIVE
        when(record.getActiveUrl()).thenReturn(Optional.of(ZK1));
        when(record.getRole(eq(ZK1))).thenReturn(ClusterRole.ACTIVE);

        clientProperties.setProperty(PHOENIX_HA_GROUP_ATTR, haGroupName);

        HAGroupInfo haGroupInfo = new HAGroupInfo(haGroupName, ZK1, ZK2);
        haGroup = spy(new HighAvailabilityGroup(haGroupInfo, clientProperties, record, READY));
    }

    /**
     * Test that the HA group can be connected to get a JDBC connection.
     *
     * The JDBC connection is from the shim layer, which wraps Phoenix Connections.
     */
    @Test
    public void testConnect() throws SQLException {
        final Connection conn = haGroup.connect(clientProperties);
        assertTrue(conn instanceof FailoverPhoenixConnection);
        FailoverPhoenixConnection failoverConnection = conn.unwrap(FailoverPhoenixConnection.class);
        assertNotNull(failoverConnection);
        // Verify that the failover should have connected to ACTIVE cluster once
        verify(haGroup, times(1)).connectActive(any(Properties.class));
        verify(haGroup, times(1)).connectToOneCluster(anyString(), eq(clientProperties));
        verify(DRIVER, atLeastOnce()).getConnectionQueryServices(anyString(), eq(clientProperties));

        when(record.getPolicy()).thenReturn(HighAvailabilityPolicy.PARALLEL);
        // get a new connection from this HA group
        final Connection conn2 = haGroup.connect(clientProperties);
        assertTrue(conn2 instanceof ParallelPhoenixConnection);
    }

    /**
     * Test that the HA group can not be connected when it is not ready.
     */
    @Test
    public void testConnectShouldFailWhenNotReady() throws SQLException {
        final HAGroupInfo info = haGroup.getGroupInfo();
        haGroup = spy(new HighAvailabilityGroup(info, clientProperties, record, UNINITIALIZED));
        try {
            haGroup.connect(clientProperties);
            fail("Should have failed since HA group is not READY!");
        } catch (SQLException e) {
            LOG.info("Got expected exception", e);
            assertEquals(SQLExceptionCode.CANNOT_ESTABLISH_CONNECTION.getErrorCode(),
                    e.getErrorCode());
            verify(DRIVER, never()).getConnectionQueryServices(anyString(), eq(clientProperties));
        }
    }

    /**
     * Test that it can connect to a cluster in the HA group to get a PhoenixConnection.
     */
    @Test
    public void testConnectToOneCluster() throws SQLException {
        // test with JDBC string
        final String jdbcString = String.format("jdbc:phoenix:%s", ZK1);
        haGroup.connectToOneCluster(jdbcString, clientProperties);
        verify(DRIVER, times(1)).getConnectionQueryServices(anyString(), eq(clientProperties));

        // test with only ZK string
        haGroup.connectToOneCluster(ZK1, clientProperties);
        verify(DRIVER, times(2)).getConnectionQueryServices(anyString(), eq(clientProperties));
    }

    /**
     * Test that when cluster role is not connectable (e.g OFFLINE or UNKNOWN), can not connect it.
     */
    @Test
    public void testConnectToOneClusterShouldFailIfNotConnectable() throws SQLException {
        when(record.getRole(eq(ZK1))).thenReturn(ClusterRole.UNKNOWN);
        // test with JDBC string and UNKNOWN cluster role
        final String jdbcString = String.format("jdbc:phoenix:%s", ZK1);
        try {
            haGroup.connectToOneCluster(jdbcString, clientProperties);
            fail("Should have failed because cluster is in UNKNOWN role");
        } catch (SQLException e) { // expected exception
            assertEquals(SQLExceptionCode.HA_CLUSTER_CAN_NOT_CONNECT.getErrorCode(),
                    e.getErrorCode());
        }
        verify(DRIVER, never()).getConnectionQueryServices(anyString(), eq(clientProperties));

        // test with only ZK string and OFFLINE cluster role
        when(record.getRole(eq(ZK1))).thenReturn(ClusterRole.OFFLINE);
        try {
            haGroup.connectToOneCluster(jdbcString, clientProperties);
            fail("Should have failed because cluster is in OFFLINE role");
        } catch (SQLException e) { // expected exception
            assertEquals(SQLExceptionCode.HA_CLUSTER_CAN_NOT_CONNECT.getErrorCode(),
                    e.getErrorCode());
        }
        verify(DRIVER, never()).getConnectionQueryServices(anyString(), eq(clientProperties));
    }

    /**
     * Test {@link HighAvailabilityGroup#connectToOneCluster} with invalid connection string.
     */
    @Test (expected = IllegalArgumentException.class)
    public void testConnectToOneClusterShouldFailWithNonHAJdbcString() throws SQLException {
        final String jdbcString = "jdbc:phoenix:dummyhost";
        haGroup.connectToOneCluster(jdbcString, clientProperties);
        verify(DRIVER, never()).getConnectionQueryServices(anyString(), eq(clientProperties));
    }

    /**
     * Test {@link HighAvailabilityGroup#connectToOneCluster} with a connection string that doesn't match.
     */
    @Test
    public void testConnectToOneClusterShouldNotFailWithDifferentHostOrderJdbcString() throws SQLException {
        // test with JDBC string
        final String hosts = "zk1-2,zk1-1:2181:/hbase";
        final String jdbcString = String.format("jdbc:phoenix+zk:%s", hosts);
        haGroup.connectToOneCluster(jdbcString, clientProperties);
        verify(DRIVER, times(1)).getConnectionQueryServices(eq(String.format("jdbc:phoenix+zk:%s",ZK1)), eq(clientProperties));
    }

    /**
     * Test {@link HighAvailabilityGroup#get} should fail with empty High Availability group name.
     */
    @Test
    public void testGetShouldFailWithoutHAGroupName() throws SQLException {
        String jdbcString = String.format("jdbc:phoenix:[%s|%s]", ZK1, ZK2);
        Properties properties = new Properties(); // without HA group name
        try {
            HighAvailabilityGroup.get(jdbcString, properties);
            fail("Should have fail because the HA group name is not set");
        } catch (SQLException e) {
            LOG.info("Got expected exception when HA group name is empty", e);
            assertEquals(SQLExceptionCode.HA_INVALID_PROPERTIES.getErrorCode(), e.getErrorCode());
        } // all other exceptions should fail this test
        verify(DRIVER, never()).getConnectionQueryServices(anyString(), eq(properties));
    }

    /**
     * Test that the HA group knows a phoenix connection is connected to ACTIVE cluster.
     */
    @Test
    public void testIsConnectionActive() throws SQLException {
        assertFalse(haGroup.isActive(null));
        PhoenixConnection connection = haGroup.connectToOneCluster(ZK1, clientProperties);
        assertTrue(haGroup.isActive(connection));
    }

    /**
     * Test that when missing cluster role records, the HA connection request will fall back to the
     * single cluster connection.
     */
    @SuppressWarnings("UnstableApiUsage")
    @Test
    public void testNegativeCacheWhenMissingClusterRoleRecords() throws Exception {
        String haGroupName2 = testName.getMethodName() + RandomStringUtils.randomAlphabetic(3);
        clientProperties.setProperty(PHOENIX_HA_GROUP_ATTR, haGroupName2);
        HAGroupInfo haGroupInfo2 = new HAGroupInfo(haGroupName2, ZK1, ZK2);
        HighAvailabilityGroup haGroup2 = spy(new HighAvailabilityGroup(haGroupInfo2, clientProperties, null, READY));
        doThrow(new RuntimeException("Mocked Exception when init HA group 2"))
                .when(haGroup2).init();
        HighAvailabilityGroup.GROUPS.put(haGroupInfo2, haGroup2);

        String jdbcString = String.format("jdbc:phoenix:[%s|%s]", ZK1, ZK2);
        // Getting HA group will get exception due to (mocked) ZK connection error
        try {
            HighAvailabilityGroup.get(jdbcString, clientProperties);
            fail("Should have fail because the HA group fails to init and ZK is not connectable");
        } catch (SQLException e) {
            //Expected
        } catch (Exception e) {
            fail("Unexpected Exception" + e.toString());
        }

        // Make ZK connectable and the cluster role record be missing
        CuratorFramework curator = mock(CuratorFramework.class);
        when(curator.blockUntilConnected(anyInt(), any(TimeUnit.class))).thenReturn(true);
        HighAvailabilityGroup.CURATOR_CACHE.put(ZK2, curator);
        HighAvailabilityGroup.CURATOR_CACHE.put(ZK1, curator);

        ExistsBuilder eb = mock(ExistsBuilder.class);
        when(eb.forPath(anyString())).thenReturn(null);
        when(curator.checkExists()).thenReturn(eb);

        // Getting HA group will not throw exception but instead will return empty optional
        for (int i = 0; i < 100; i++) {
            assertFalse(HighAvailabilityGroup.get(jdbcString, clientProperties).isPresent());
        }

        // After 100 connection request, the actual time that init() is called should still be one
        // Why is that? Remember the negative cache.
        verify(haGroup2, times(1)).init();
    }
}
