/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.HBaseTestingUtilityPair.PRINCIPAL;
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.HBaseTestingUtilityPair.doTestWhenOneHBaseDown;
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.doTestBasicOperationsWithConnection;
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.getHighAvailibilityGroup;
import static org.apache.phoenix.query.QueryServices.CONNECTION_QUERY_SERVICE_METRICS_ENABLED;
import static org.apache.phoenix.util.PhoenixRuntime.clearAllConnectionQueryServiceMetrics;
import static org.junit.Assert.*;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.ClusterRoleRecord.RegistryType;
import org.apache.phoenix.monitoring.ConnectionQueryServicesMetric;
import org.apache.phoenix.monitoring.MetricType;
import org.apache.phoenix.query.ConfigurationFactory;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.ConnectionQueryServicesImpl;
import org.apache.phoenix.util.InstanceResolver;
import org.apache.phoenix.util.PhoenixRuntime;
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
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test HA basics for {@link ClusterRoleRecord} with {@link RegistryType} as MASTER.
 */
@RunWith(Parameterized.class)
@Category(NeedsOwnMiniClusterTest.class)
public class HAConnectionWithMasterAndRPCRegistryIT {

  private static final Logger LOG =
    LoggerFactory.getLogger(HAConnectionWithMasterAndRPCRegistryIT.class);
  private static final HighAvailabilityTestingUtility.HBaseTestingUtilityPair CLUSTERS =
    new HighAvailabilityTestingUtility.HBaseTestingUtilityPair();

  @Rule
  public final TestName testName = new TestName();
  /** Client properties to create a connection per test. */
  private Properties parallelClientProperties;
  private Properties failoverClientProperties;
  /** HA group for this test. */
  private HighAvailabilityGroup parallelHAGroup;
  private HighAvailabilityGroup failoverHAGroup;
  /** Table name per test case. */
  private String tableName;
  /** HA Group name for this test. */
  private String parallelHAGroupName;
  private String failoverHAGroupName;
  /** Registry Type for this test */
  private final RegistryType registryType;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    CLUSTERS.start();
    DriverManager.registerDriver(PhoenixDriver.INSTANCE);
    InstanceResolver.clearSingletons();
    InstanceResolver.getSingleton(ConfigurationFactory.class, new ConfigurationFactory() {
      @Override
      public Configuration getConfiguration() {
        Configuration conf = HBaseConfiguration.create();
        conf.set(CONNECTION_QUERY_SERVICE_METRICS_ENABLED, String.valueOf(true));
        return conf;
      }

      @Override
      public Configuration getConfiguration(Configuration confToClone) {
        Configuration conf = HBaseConfiguration.create();
        conf.set(CONNECTION_QUERY_SERVICE_METRICS_ENABLED, String.valueOf(true));
        Configuration copy = new Configuration(conf);
        copy.addResource(confToClone);
        return copy;
      }
    });
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    DriverManager.deregisterDriver(PhoenixDriver.INSTANCE);
    CLUSTERS.close();
  }

  @Before
  public void setup() throws Exception {
    if (registryType == ClusterRoleRecord.RegistryType.RPC) {
      assumeTrue(VersionInfo.compareVersion(VersionInfo.getVersion(), "2.5.0") >= 0);
    }
    parallelHAGroupName = testName.getMethodName() + "_" + HighAvailabilityPolicy.PARALLEL.name();
    failoverHAGroupName = testName.getMethodName() + "_" + HighAvailabilityPolicy.FAILOVER.name();
    parallelClientProperties = HighAvailabilityTestingUtility.getHATestProperties();
    parallelClientProperties.setProperty(PHOENIX_HA_GROUP_ATTR, parallelHAGroupName);

    failoverClientProperties = HighAvailabilityTestingUtility.getHATestProperties();
    failoverClientProperties.setProperty(PHOENIX_HA_GROUP_ATTR, failoverHAGroupName);

    // Make first cluster ACTIVE
    CLUSTERS.initClusterRole(parallelHAGroupName, HighAvailabilityPolicy.PARALLEL, registryType);
    CLUSTERS.initClusterRole(failoverHAGroupName, HighAvailabilityPolicy.FAILOVER, registryType);

    parallelHAGroup = getHighAvailibilityGroup(CLUSTERS.getJdbcHAUrl(), parallelClientProperties);
    LOG.info("Initialized haGroup {} with URL {}", parallelHAGroup, CLUSTERS.getJdbcHAUrl());
    failoverHAGroup = getHighAvailibilityGroup(CLUSTERS.getJdbcHAUrl(), failoverClientProperties);
    LOG.info("Initialized haGroup {} with URL {}", failoverHAGroup, CLUSTERS.getJdbcHAUrl());
    tableName = RandomStringUtils.randomAlphabetic(10).toUpperCase();
    CLUSTERS.createTableOnClusterPair(parallelHAGroup, tableName);
  }

  @After
  public void tearDown() throws Exception {
    try {
      parallelHAGroup.close();
      failoverHAGroup.close();
      PhoenixDriver.INSTANCE.getConnectionQueryServices(CLUSTERS.getJdbcUrl1(parallelHAGroup),
        parallelHAGroup.getProperties()).close();
      PhoenixDriver.INSTANCE.getConnectionQueryServices(CLUSTERS.getJdbcUrl2(parallelHAGroup),
        parallelHAGroup.getProperties()).close();
    } catch (Exception e) {
      LOG.error("Fail to tear down the HA group and the CQS. Will ignore", e);
    }
  }

  @Parameterized.Parameters(name = "ClusterRoleRecord_registryType={0}")
  public static Collection<Object> data() {
    return Arrays.asList(
      new Object[] { ClusterRoleRecord.RegistryType.MASTER, ClusterRoleRecord.RegistryType.RPC, });
  }

  public HAConnectionWithMasterAndRPCRegistryIT(ClusterRoleRecord.RegistryType registryType) {
    this.registryType = registryType;
  }

  /**
   * Test that failover can finish even if one connection can not be closed. When once cluster
   * becomes STANDBY from ACTIVE, all its connections and the associated CQS will get closed
   * asynchronously. In case of errors when closing those connections and CQS, the HA group is still
   * able to transit to target state after the maximum timeout. Closing the existing connections is
   * guaranteed with the best effort and timeout in favor of improved availability.
   * {@link FailoverPhoenixConnectionIT#testFailoverTwice()} which fails over back to the first
   * cluster
   */
  @Test(timeout = 300000)
  public void testFailoverCanFinishWhenOneConnectionGotStuckClosing() throws Exception {
    Connection conn = getFailoverConnection();
    doTestBasicOperationsWithConnection(conn, tableName, failoverHAGroupName);
    assertEquals(CLUSTERS.getJdbcUrl1(failoverHAGroup), // active connection is against the first
                                                        // cluster
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
      .getConnectionQueryServices(CLUSTERS.getJdbcUrl1(failoverHAGroup), failoverClientProperties);
    // replace the wrapped connection with the spied connection in CQS
    cqs.removeConnection(wrapped.unwrap(PhoenixConnection.class));
    cqs.addConnection(spy.unwrap(PhoenixConnection.class));

    // (ACTIVE, STANDBY) -> (STANDBY, ACTIVE)
    // The transition will finish as we set PHOENIX_HA_TRANSITION_TIMEOUT_MS_KEY for this class
    // even though the spied connection is stuck at the latch when closing
    CLUSTERS.transitClusterRole(failoverHAGroup, ClusterRoleRecord.ClusterRole.STANDBY,
      ClusterRoleRecord.ClusterRole.ACTIVE);

    // Verify the spied object has been called once
    verify(spy, times(1)).close();
    // The spy is not closed because the real method was blocked by latch
    assertFalse(spy.isClosed());
    // connection is not closed as Phoenix HA does not close failover connections.
    assertFalse(conn.isClosed());

    try (Connection conn2 = getFailoverConnection()) {
      doTestBasicOperationsWithConnection(conn2, tableName, failoverHAGroupName);
      assertEquals(CLUSTERS.getJdbcUrl2(failoverHAGroup), // active connection is against the second
                                                          // cluster
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
   * Tests the scenario where ACTIVE HBase cluster restarts. When ACTIVE HBase cluster shutdown,
   * client can not connect to this HA group. After failover, client can connect to this HA group
   * (against the new ACTIVE cluster2). After restarts and fails back, client can connect to this HA
   * group (against cluster 1).
   */
  @Test(timeout = 300000)
  public void testConnectionWhenActiveHBaseRestarts() throws Exception {
    // This creates the cqsi for the active cluster upfront.
    // If we don't do that then later when we try to transition
    // the cluster role it tries to create cqsi for the cluster
    // which is down and that takes forever causing timeouts
    try (Connection conn = getFailoverConnection()) {
      doTestBasicOperationsWithConnection(conn, tableName, failoverHAGroupName);
    }
    doTestWhenOneHBaseDown(CLUSTERS.getHBaseCluster1(), () -> {
      try {
        try (Connection conn = getFailoverConnection()) {
          doTestBasicOperationsWithConnection(conn, tableName, failoverHAGroupName);
        }
        fail("Should have failed since ACTIVE ZK cluster was shutdown");
      } catch (SQLException e) {
        LOG.info("Got expected exception when ACTIVE ZK cluster is down", e);
      }

      // So on-call engineer comes into play, and triggers a failover
      // Because cluster1 is down, new version record will only be updated in cluster2
      CLUSTERS.transitClusterRole(failoverHAGroup, ClusterRoleRecord.ClusterRole.STANDBY,
        ClusterRoleRecord.ClusterRole.ACTIVE);

      // After failover, the FailoverPhoenixConnection should go to the second cluster
      try (Connection conn = getFailoverConnection()) {
        doTestBasicOperationsWithConnection(conn, tableName, failoverHAGroupName);
      }
    });

    LOG.info("Testing failover connection when both clusters are up and running");
    try (Connection conn = getFailoverConnection()) {
      FailoverPhoenixConnection failoverConn = conn.unwrap(FailoverPhoenixConnection.class);
      assertEquals(CLUSTERS.getJdbcUrl2(failoverHAGroup),
        failoverConn.getWrappedConnection().getURL());
      doTestBasicOperationsWithConnection(conn, tableName, failoverHAGroupName);
    }

    LOG.info("Testing failover back to cluster1 when bot clusters are up and running");
    // Failover back to the first cluster since it is healthy after restart
    CLUSTERS.transitClusterRole(failoverHAGroup, ClusterRoleRecord.ClusterRole.ACTIVE,
      ClusterRoleRecord.ClusterRole.STANDBY);
    // After ACTIVE ZK restarts and fail back, this should still work
    try (Connection conn = getFailoverConnection()) {
      doTestBasicOperationsWithConnection(conn, tableName, failoverHAGroupName);
    }
  }

  /**
   * Tests the scenario where STANDBY HBase cluster restarts. When STANDBY HBase cluster shutdown,
   * client can still connect to this HA group. After failover, client can not connect to this HA
   * group (ACTIVE cluster 2 is down). After cluster 2 (ACTIVE) restarts, client can connect to this
   * HA group.
   */
  @Test(timeout = 300000)
  public void testConnectionWhenStandbyHBaseRestarts() throws Exception {
    doTestWhenOneHBaseDown(CLUSTERS.getHBaseCluster2(), () -> {
      try (Connection conn = getFailoverConnection()) {
        doTestBasicOperationsWithConnection(conn, tableName, failoverHAGroupName);
      }

      // Innocent on-call engineer triggers a failover to second cluster
      CLUSTERS.transitClusterRole(failoverHAGroup, ClusterRoleRecord.ClusterRole.STANDBY,
        ClusterRoleRecord.ClusterRole.ACTIVE);

      try {
        getFailoverConnection();
        fail("Should have failed since ACTIVE HBase cluster was shutdown");
      } catch (SQLException e) {
        LOG.info("Got expected exception when ACTIVE HBase cluster {} was shutdown",
          CLUSTERS.getZkUrl2(), e);
      }
    });

    /* As restarting HBase minicluster it changes the port of HMasters need to refresh the CRR */
    CLUSTERS.refreshClusterRoleRecordAfterClusterRestart(failoverHAGroup,
      ClusterRoleRecord.ClusterRole.STANDBY, ClusterRoleRecord.ClusterRole.ACTIVE);
    try (Connection conn = getFailoverConnection()) {
      doTestBasicOperationsWithConnection(conn, tableName, failoverHAGroupName);
    }
  }

  /**
   * Tests the scenario where two HBase clusters both restart. Client can not connect to this HA
   * group when two HBase clusters are both down. When STANDBY HBase cluster2 first restarts, client
   * still can not connect to this HA group. After failover, client can connect to this HA group
   * because cluster 2 is ACTIVE and healthy.
   */
  @Test(timeout = 300000)
  public void testConnectionWhenTwoHBaseRestarts() throws Exception {
    doTestWhenOneHBaseDown(CLUSTERS.getHBaseCluster1(), () -> {
      doTestWhenOneHBaseDown(CLUSTERS.getHBaseCluster2(), () -> {
        try {
          getFailoverConnection();
          fail("Should have failed since ACTIVE HBase cluster was shutdown");
        } catch (SQLException e) {
          LOG.info("Got expected exception when both clusters are down", e);
        }
      });

      // Client cannot connect to HA group because cluster 2 is still STANDBY after restarted
      try {
        getFailoverConnection();
        fail("Should have failed since ACTIVE HBase cluster was shutdown");
      } catch (SQLException e) {
        LOG.info("Got expected exception when ACTIVE HBase cluster {} was shutdown",
          CLUSTERS.getZkUrl2(), e);
      }

      // So on-call engineer comes into play, and triggers a failover
      CLUSTERS.transitClusterRole(failoverHAGroup, ClusterRoleRecord.ClusterRole.STANDBY,
        ClusterRoleRecord.ClusterRole.ACTIVE);

      // After the second cluster (ACTIVE) HBase restarts, this should still work
      try (Connection conn = getFailoverConnection()) {
        doTestBasicOperationsWithConnection(conn, tableName, failoverHAGroupName);
      }
    });
  }

  /**
   * Testing connection closure for roleRecord change from (URL1, ACTIVE, URL2, STANDBY) -> (URL3,
   * ACTIVE, URL2, STANDBY) All wrapped connections should have been closed
   */
  @Test(timeout = 300000)
  public void testAllWrappedConnectionsClosedAfterActiveURLChange() throws Exception {
    short numberOfConnections = 5;
    // Create FailoverPhoenixConnections and ParallelPhoenixConnections with default urls
    List<Connection> connectionList = new ArrayList<>(numberOfConnections);
    List<Connection> parallelConnectionList = new ArrayList<>(numberOfConnections);
    for (short i = 0; i < numberOfConnections; i++) {
      connectionList.add(getFailoverConnection());
      parallelConnectionList.add(getParallelConnection());
    }
    // url of cluster which is being restarted
    String url = CLUSTERS.getJdbcUrl1(parallelHAGroup);
    // Restart Cluster 1 with new ports
    CLUSTERS.restartCluster1();
    // Basically applying new url for cluster 1
    CLUSTERS.refreshClusterRoleRecordAfterClusterRestart(failoverHAGroup,
      ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY);
    CLUSTERS.refreshClusterRoleRecordAfterClusterRestart(parallelHAGroup,
      ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY);
    for (short i = 0; i < numberOfConnections; i++) {
      LOG.info("Asserting connection number {}", i);
      FailoverPhoenixConnection conn = ((FailoverPhoenixConnection) connectionList.get(i));
      assertFalse(conn.isClosed());
      assertTrue(conn.getWrappedConnection().isClosed());
      conn.close();

      ParallelPhoenixConnection conn2 = ((ParallelPhoenixConnection) parallelConnectionList.get(i));
      assertFalse(conn2.isClosed());
      // Only connections to cluster 1 should be closed
      if (conn2.futureConnection1.get().getURL().equals(url)) {
        assertTrue(conn2.futureConnection1.get().isClosed());
        assertFalse(conn2.futureConnection2.get().isClosed());
      } else {
        assertTrue(conn2.futureConnection2.get().isClosed());
        assertFalse(conn2.futureConnection1.get().isClosed());
      }
      conn2.close();
    }
  }

  /**
   * Testing connection closure for roleRecord change from (URL1, ACTIVE, URL2, STANDBY) -> (URL1,
   * ACTIVE, URL3, STANDBY) It should not have affected connections at all
   */
  @Test(timeout = 300000)
  public void testAllWrappedConnectionsNotClosedAfterStandbyURLChange() throws Exception {
    short numberOfConnections = 5;
    // Create FailoverPhoenixConnections and ParallelPhoenixConnections with default urls
    List<Connection> connectionList = new ArrayList<>(numberOfConnections);
    List<Connection> parallelConnectionList = new ArrayList<>(numberOfConnections);
    for (short i = 0; i < numberOfConnections; i++) {
      connectionList.add(getFailoverConnection());
      parallelConnectionList.add(getParallelConnection());
    }

    // url of cluster which is being restarted
    String url = CLUSTERS.getJdbcUrl2(parallelHAGroup);
    // Restart Cluster 2 with new ports
    CLUSTERS.restartCluster2();
    // Basically applying new url for cluster 2
    CLUSTERS.refreshClusterRoleRecordAfterClusterRestart(failoverHAGroup,
      ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY);
    CLUSTERS.refreshClusterRoleRecordAfterClusterRestart(parallelHAGroup,
      ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY);
    for (short i = 0; i < numberOfConnections; i++) {
      LOG.info("Asserting connection number {}", i);
      FailoverPhoenixConnection conn = ((FailoverPhoenixConnection) connectionList.get(i));
      assertFalse(conn.isClosed());
      assertFalse(conn.getWrappedConnection().isClosed());
      conn.close();

      ParallelPhoenixConnection conn2 = ((ParallelPhoenixConnection) parallelConnectionList.get(i));
      assertFalse(conn2.isClosed());
      // Only connections to cluster 2 should be closed
      if (conn2.futureConnection1.get().getURL().equals(url)) {
        assertTrue(conn2.futureConnection1.get().isClosed());
        assertFalse(conn2.futureConnection2.get().isClosed());
      } else {
        assertTrue(conn2.futureConnection2.get().isClosed());
        assertFalse(conn2.futureConnection1.get().isClosed());
      }
      conn2.close();
    }
  }

  /**
   * This is to make sure all Phoenix connections are closed when registryType changes of a CRR.
   * MASTER --> ZK RPC --> MASTER Test with many connections.
   */
  @Test(timeout = 300000)
  public void testAllWrappedConnectionsClosedAfterRegistryChange() throws Exception {
    short numberOfConnections = 5;
    // Create FailoverPhoenixConnections and ParallelPhoenixConnections with default urls
    List<Connection> connectionList = new ArrayList<>(numberOfConnections);
    List<Connection> parallelConnectionList = new ArrayList<>(numberOfConnections);
    for (short i = 0; i < numberOfConnections; i++) {
      connectionList.add(getFailoverConnection());
      parallelConnectionList.add(getParallelConnection());
    }

    ClusterRoleRecord.RegistryType newRegistry;
    if (registryType == RegistryType.MASTER) {
      newRegistry = RegistryType.ZK;
    } else {
      newRegistry = RegistryType.MASTER;
    }

    ConnectionQueryServicesImpl cqsiF = (ConnectionQueryServicesImpl) PhoenixDriver.INSTANCE
      .getConnectionQueryServices(CLUSTERS.getJdbcUrl1(failoverHAGroup), failoverClientProperties);
    ConnectionQueryServicesImpl cqsiP = (ConnectionQueryServicesImpl) PhoenixDriver.INSTANCE
      .getConnectionQueryServices(CLUSTERS.getJdbcUrl1(parallelHAGroup), parallelClientProperties);
    ConnectionInfo connInfoF = ConnectionInfo.create(CLUSTERS.getJdbcUrl1(failoverHAGroup),
      PhoenixDriver.INSTANCE.getQueryServices().getProps(), failoverClientProperties);
    ConnectionInfo connInfoP = ConnectionInfo.create(CLUSTERS.getJdbcUrl1(parallelHAGroup),
      PhoenixDriver.INSTANCE.getQueryServices().getProps(), failoverClientProperties);

    CLUSTERS.transitClusterRoleRecordRegistry(failoverHAGroup, newRegistry);
    CLUSTERS.transitClusterRoleRecordRegistry(parallelHAGroup, newRegistry);

    for (short i = 0; i < numberOfConnections; i++) {
      LOG.info("Asserting connection number {}", i);
      FailoverPhoenixConnection conn = ((FailoverPhoenixConnection) connectionList.get(i));
      assertFalse(conn.isClosed());
      assertTrue(conn.getWrappedConnection().isClosed());

      ParallelPhoenixConnection conn2 = ((ParallelPhoenixConnection) parallelConnectionList.get(i));
      assertFalse(conn2.isClosed());
      assertTrue(conn2.futureConnection1.get().isClosed());
      assertTrue(conn2.futureConnection1.get().isClosed());
      conn2.close();
    }

    // Both CQSI should be closed
    try {
      cqsiF.checkClosed();
      fail("Should have thrown an exception as cqsi should be closed");
    } catch (IllegalStateException e) {
      // Exception cqsi should have been invalidated as well
      assertFalse(PhoenixDriver.INSTANCE.checkIfCQSIIsInCache(connInfoF));
    } catch (Exception e) {
      fail("Should have thrown on IllegalStateException as cqsi should be closed");
    }

    try {
      cqsiP.checkClosed();
      fail("Should have thrown an exception as cqsi should be closed");
    } catch (IllegalStateException e) {
      // Exception cqsi should have been invalidated as well
      assertFalse(PhoenixDriver.INSTANCE.checkIfCQSIIsInCache(connInfoP));
    } catch (Exception e) {
      fail("Should have thrown on IllegalStateException as cqsi should be closed");
    }
  }

  /**
   * This is to make sure all Phoenix connections are closed when registryType changes of a CRR.
   * MASTER --> RPC RPC --> ZK Test with many connections.
   */
  @Test(timeout = 300000)
  public void testAllWrappedConnectionsClosedAfterRegistryChange2() throws Exception {
    short numberOfConnections = 5;
    // Create FailoverPhoenixConnections and ParallelPhoenixConnections with default urls
    List<Connection> connectionList = new ArrayList<>(numberOfConnections);
    List<Connection> parallelConnectionList = new ArrayList<>(numberOfConnections);
    for (short i = 0; i < numberOfConnections; i++) {
      connectionList.add(getFailoverConnection());
      parallelConnectionList.add(getParallelConnection());
    }

    ClusterRoleRecord.RegistryType newRegistry;
    if (registryType == RegistryType.MASTER) {
      newRegistry = RegistryType.RPC;
      // RPC Registry is only there in hbase version greater than 2.5.0
      assumeTrue(VersionInfo.compareVersion(VersionInfo.getVersion(), "2.5.0") >= 0);
    } else {
      newRegistry = RegistryType.ZK;
    }
    ConnectionQueryServicesImpl cqsiF = (ConnectionQueryServicesImpl) PhoenixDriver.INSTANCE
      .getConnectionQueryServices(CLUSTERS.getJdbcUrl1(failoverHAGroup), failoverClientProperties);
    ConnectionQueryServicesImpl cqsiP = (ConnectionQueryServicesImpl) PhoenixDriver.INSTANCE
      .getConnectionQueryServices(CLUSTERS.getJdbcUrl1(parallelHAGroup), parallelClientProperties);
    ConnectionInfo connInfoF = ConnectionInfo.create(CLUSTERS.getJdbcUrl1(failoverHAGroup),
      PhoenixDriver.INSTANCE.getQueryServices().getProps(), failoverClientProperties);
    ConnectionInfo connInfoP = ConnectionInfo.create(CLUSTERS.getJdbcUrl1(parallelHAGroup),
      PhoenixDriver.INSTANCE.getQueryServices().getProps(), failoverClientProperties);

    CLUSTERS.transitClusterRoleRecordRegistry(failoverHAGroup, newRegistry);
    CLUSTERS.transitClusterRoleRecordRegistry(parallelHAGroup, newRegistry);

    for (short i = 0; i < numberOfConnections; i++) {
      LOG.info("Asserting connection number {}", i);
      FailoverPhoenixConnection conn = ((FailoverPhoenixConnection) connectionList.get(i));
      assertFalse(conn.isClosed());
      assertTrue(conn.getWrappedConnection().isClosed());

      ParallelPhoenixConnection conn2 = ((ParallelPhoenixConnection) parallelConnectionList.get(i));
      assertFalse(conn2.isClosed());
      assertTrue(conn2.futureConnection1.get().isClosed());
      assertTrue(conn2.futureConnection1.get().isClosed());
      conn2.close();
    }

    // Both CQSI should be closed
    try {
      cqsiF.checkClosed();
      fail("Should have thrown an exception as cqsi should be closed");
    } catch (IllegalStateException e) {
      // Exception cqsi should have been invalidated as well
      assertFalse(PhoenixDriver.INSTANCE.checkIfCQSIIsInCache(connInfoF));
    } catch (Exception e) {
      fail("Should have thrown on IllegalStateException as cqsi should be closed");
    }

    try {
      cqsiP.checkClosed();
      fail("Should have thrown an exception as cqsi should be closed");
    } catch (IllegalStateException e) {
      // Exception cqsi should have been invalidated as well
      assertFalse(PhoenixDriver.INSTANCE.checkIfCQSIIsInCache(connInfoP));
    } catch (Exception e) {
      fail("Should have thrown on IllegalStateException as cqsi should be closed");
    }
  }

  @Test
  public void testConnectionCreationDuration() throws SQLException {
    clearAllConnectionQueryServiceMetrics();
    Connection conn = getParallelConnection();
    validateConnectionCreationTime(conn);

    conn = getFailoverConnection();
    validateConnectionCreationTime(conn);
  }

  private void validateConnectionCreationTime(Connection connection) {
    Map<String, List<ConnectionQueryServicesMetric>> metrics = PhoenixRuntime.getAllConnectionQueryServicesMetrics();
    assertNotNull(connection);
    for (ConnectionQueryServicesMetric metric : metrics.get(PRINCIPAL)) {
      if (metric.getMetricType().equals(MetricType.PHOENIX_CONNECTION_CREATION_TIME_MS)) {
        assertNotEquals(0, metric.getValue());
      }
    }
  }

  /**
   * Create a failover connection using {@link #failoverClientProperties}.
   */
  private Connection getFailoverConnection() throws SQLException {
    return DriverManager.getConnection(CLUSTERS.getJdbcHAUrl(), failoverClientProperties);
  }

  /**
   * Returns a Parallel Phoenix Connection
   * @return Parallel Phoenix Connection
   */
  private Connection getParallelConnection() throws SQLException {
    return DriverManager.getConnection(CLUSTERS.getJdbcHAUrl(), parallelClientProperties);
  }

}
