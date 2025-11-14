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
package org.apache.phoenix.end2end;

import static org.apache.phoenix.jdbc.HAGroupStoreClient.ZK_CONSISTENT_HA_GROUP_RECORD_NAMESPACE;
import static org.apache.phoenix.jdbc.PhoenixHAAdmin.getLocalZkUrl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import com.google.protobuf.RpcCallback;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.OnlineRegions;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.PhoenixRegionServerEndpoint;
import org.apache.phoenix.coprocessor.generated.RegionServerEndpointProtos;
import org.apache.phoenix.jdbc.ClusterRoleRecord;
import org.apache.phoenix.jdbc.HAGroupStoreRecord;
import org.apache.phoenix.jdbc.HAGroupStoreRecord.HAGroupState;
import org.apache.phoenix.jdbc.HighAvailabilityPolicy;
import org.apache.phoenix.jdbc.HighAvailabilityTestingUtility;
import org.apache.phoenix.jdbc.PhoenixHAAdmin;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.HAGroupStoreTestUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

@Category({ NeedsOwnMiniClusterTest.class })
public class PhoenixRegionServerEndpointWithConsistentFailoverIT extends BaseTest {

  private static final Logger LOGGER
    = LoggerFactory.getLogger(PhoenixRegionServerEndpointWithConsistentFailoverIT.class);
  private static final Long ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS = 5000L;
  private static final HighAvailabilityTestingUtility.HBaseTestingUtilityPair CLUSTERS =
    new HighAvailabilityTestingUtility.HBaseTestingUtilityPair();
  private String zkUrl;
  private String peerZkUrl;

  @Rule
  public TestName testName = new TestName();

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
    setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    // Set prewarm enabled to true for cluster 1 and false for cluster 2 for comparison.
    CLUSTERS.getHBaseCluster1().getConfiguration().setBoolean(
        QueryServices.HA_GROUP_STORE_CLIENT_PREWARM_ENABLED, true);
    CLUSTERS.getHBaseCluster2().getConfiguration().setBoolean(
        QueryServices.HA_GROUP_STORE_CLIENT_PREWARM_ENABLED, false);
    CLUSTERS.start();
  }

  @AfterClass
  public static synchronized void doTeardown() throws Exception {
    CLUSTERS.close();
  }

  @Before
  public void setUp() throws Exception {
    peerZkUrl = CLUSTERS.getZkUrl2();
    HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(testName.getMethodName(), CLUSTERS.getZkUrl1(), CLUSTERS.getZkUrl2(),
      CLUSTERS.getMasterAddress1(), CLUSTERS.getMasterAddress2(),
      ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY,
      null);
    HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(testName.getMethodName(), CLUSTERS.getZkUrl2(), CLUSTERS.getZkUrl1(),
      CLUSTERS.getMasterAddress2(), CLUSTERS.getMasterAddress1(),
      ClusterRoleRecord.ClusterRole.STANDBY, ClusterRoleRecord.ClusterRole.ACTIVE,
      null);

  }

  @Test
  public void testHAGroupStoreClientPrewarming() throws Exception {
    // Use a different HA group name to avoid interference with setUp() method
    String haGroupName = testName.getMethodName() + "_test";

    // There is a race condition between when RegionServerEndpoint Coproc starts and
    // when the HAGroupStoreRecord is inserted into the system table.
    // To handle this condition and get predictable results, we will insert the HAGroupStoreRecord into the system table first.
    // Once the HAGroupStoreRecord is inserted into the system table, we will start the RegionServerEndpoint Coproc again.
    // This will ensure that the RegionServerEndpoint Coproc starts after the HAGroupStoreRecord is inserted into the system table.
    HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName, CLUSTERS.getZkUrl1(), CLUSTERS.getZkUrl2(),
        CLUSTERS.getMasterAddress1(), CLUSTERS.getMasterAddress2(),
        ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY,
        null);

    HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName, CLUSTERS.getZkUrl1(), CLUSTERS.getZkUrl2(),
        CLUSTERS.getMasterAddress1(), CLUSTERS.getMasterAddress2(),
        ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY,
        CLUSTERS.getZkUrl2());

    // Get RegionServer instances from both clusters
    HRegionServer regionServer1 = CLUSTERS.getHBaseCluster1().getHBaseCluster().getRegionServer(0);
    PhoenixRegionServerEndpoint coprocessor1 = getPhoenixRegionServerEndpoint(regionServer1);

    // Start the RegionServerEndpoint Coproc for cluster 1
    coprocessor1.start(getTestCoprocessorEnvironment(CLUSTERS.getHBaseCluster1().getConfiguration()));

    HRegionServer regionServer2 = CLUSTERS.getHBaseCluster2().getHBaseCluster().getRegionServer(0);
    PhoenixRegionServerEndpoint coprocessor2 = getPhoenixRegionServerEndpoint(regionServer2);

    // Start the RegionServerEndpoint Coproc for cluster 2
    coprocessor2.start(getTestCoprocessorEnvironment(CLUSTERS.getHBaseCluster2().getConfiguration()));

    // Wait for prewarming to complete on cluster 1 (cluster 2 won't prewarm)
    Thread.sleep(5000);

    // Expected records for each cluster
    ClusterRoleRecord expectedRecord1 = buildExpectedClusterRoleRecord(haGroupName,
        ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.UNKNOWN);
    ClusterRoleRecord expectedRecord2 = buildExpectedClusterRoleRecord(haGroupName,
        ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY);

    // Test Cluster 1 WITH prewarming
    ServerRpcController controller1 = new ServerRpcController();
    long startTimeCluster1 = System.currentTimeMillis();
    executeGetClusterRoleRecordAndVerify(coprocessor1, controller1,
        haGroupName, expectedRecord1, false);
    long timeCluster1 = System.currentTimeMillis() - startTimeCluster1;
    LOGGER.info("Cluster 1 WITH prewarming (after restart, prewarmed at startup): {} ms for {}",
        timeCluster1, CLUSTERS.getZkUrl1());

    // Test Cluster 2 WITHOUT prewarming
    ServerRpcController controller2 = new ServerRpcController();
    long startTimeCluster2 = System.currentTimeMillis();
    executeGetClusterRoleRecordAndVerify(coprocessor2, controller2,
        haGroupName, expectedRecord2, false);
    long timeCluster2 = System.currentTimeMillis() - startTimeCluster2;
    LOGGER.info("Cluster 2 WITHOUT prewarming (after restart, cold start): {} ms for {}",
        timeCluster2, CLUSTERS.getZkUrl2());

    // Compare performance
    LOGGER.info("Performance comparison: Cluster 1 (prewarmed) took {} ms, " +
            "Cluster 2 (not prewarmed) took {} ms",
        timeCluster1, timeCluster2);
    LOGGER.info("Performance improvement: {} ms faster with prewarming",
        (timeCluster2 - timeCluster1));

    // Prewarmed cluster should be faster than non-prewarmed cluster
    assert (timeCluster1 < timeCluster2) :
        String.format("Prewarmed cluster (cluster 1: %d ms) should be faster than " +
            "non-prewarmed cluster (cluster 2: %d ms)", timeCluster1, timeCluster2);
  }

  @Test
  public void testGetClusterRoleRecordAndInvalidate() throws Exception {
    String haGroupName = testName.getMethodName();
    HRegionServer regionServer = CLUSTERS.getHBaseCluster1().getHBaseCluster().getRegionServer(0);
    PhoenixRegionServerEndpoint coprocessor = getPhoenixRegionServerEndpoint(regionServer);
    assertNotNull(coprocessor);
    ServerRpcController controller = new ServerRpcController();

    try (PhoenixHAAdmin peerHAAdmin = new PhoenixHAAdmin(
      CLUSTERS.getHBaseCluster2().getConfiguration(), ZK_CONSISTENT_HA_GROUP_RECORD_NAMESPACE)) {
      HAGroupStoreRecord peerHAGroupStoreRecord =
        new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, haGroupName,
          HAGroupState.STANDBY, 0L, HighAvailabilityPolicy.FAILOVER.toString(),
          CLUSTERS.getZkUrl2(), CLUSTERS.getMasterAddress2(), CLUSTERS.getMasterAddress1(), 0L);
      peerHAAdmin.createHAGroupStoreRecordInZooKeeper(peerHAGroupStoreRecord);
    }
    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

    // First getClusterRoleRecord to check if HAGroupStoreClient is working as expected
    ClusterRoleRecord expectedRecord = buildExpectedClusterRoleRecord(haGroupName,
      ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY);
    executeGetClusterRoleRecordAndVerify(coprocessor, controller, haGroupName, expectedRecord,
      false);

    // Delete the HAGroupStoreRecord from ZK
    try (PhoenixHAAdmin haAdmin =
      new PhoenixHAAdmin(CLUSTERS.getHBaseCluster1().getConfiguration(), ZK_CONSISTENT_HA_GROUP_RECORD_NAMESPACE)) {
      haAdmin.deleteHAGroupStoreRecordInZooKeeper(haGroupName);
    }
    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
    // Delete the row from System Table
    HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(haGroupName, CLUSTERS.getZkUrl1());

    // Expect exception when getting ClusterRoleRecord because the HAGroupStoreRecord is not found
    // in ZK
    controller = new ServerRpcController();
    executeGetClusterRoleRecordAndVerify(coprocessor, controller, haGroupName, expectedRecord,
      true);

    // Update the row
    HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(testName.getMethodName(), CLUSTERS.getZkUrl1(),
      peerZkUrl, CLUSTERS.getMasterAddress1(), CLUSTERS.getMasterAddress2(),
      ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY, ClusterRoleRecord.ClusterRole.STANDBY, null);

    // Now Invalidate the Cache
    controller = new ServerRpcController();
    coprocessor.invalidateHAGroupStoreClient(controller,
      getInvalidateHAGroupStoreClientRequest(haGroupName), null);
    assertFalse(controller.failed());

    // Local Cluster Role will be updated to ACTIVE_TO_STANDBY as cache is invalidated
    controller = new ServerRpcController();
    ClusterRoleRecord expectedRecordAfterInvalidation = buildExpectedClusterRoleRecord(haGroupName,
      ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY, ClusterRoleRecord.ClusterRole.STANDBY);
    executeGetClusterRoleRecordAndVerify(coprocessor, controller, haGroupName,
      expectedRecordAfterInvalidation, false);
  }

  private ClusterRoleRecord buildExpectedClusterRoleRecord(String haGroupName,
    ClusterRoleRecord.ClusterRole localRole, ClusterRoleRecord.ClusterRole peerRole) {
    return new ClusterRoleRecord(haGroupName, HighAvailabilityPolicy.FAILOVER,
      CLUSTERS.getMasterAddress1(), localRole, CLUSTERS.getMasterAddress2(), peerRole, 1);
  }

  private void executeGetClusterRoleRecordAndVerify(PhoenixRegionServerEndpoint coprocessor,
    ServerRpcController controller, String haGroupName, ClusterRoleRecord expectedRecord,
    boolean expectControllerFail) {
    RpcCallback<RegionServerEndpointProtos.GetClusterRoleRecordResponse> rpcCallback =
      createValidationCallback(haGroupName, expectedRecord);
    coprocessor.getClusterRoleRecord(controller, getClusterRoleRecordRequest(haGroupName),
      rpcCallback);
    assertEquals(expectControllerFail, controller.failed());
  }

  private RpcCallback<RegionServerEndpointProtos.GetClusterRoleRecordResponse>
    createValidationCallback(String haGroupName, ClusterRoleRecord expectedRecord) {
    return response -> {
      assertNotNull(response);
      ClusterRoleRecord actual = new ClusterRoleRecord(haGroupName,
        HighAvailabilityPolicy.valueOf(response.getPolicy().toStringUtf8()),
        response.getUrl1().toStringUtf8(),
        ClusterRoleRecord.ClusterRole.valueOf(response.getRole1().toStringUtf8()),
        response.getUrl2().toStringUtf8(),
        ClusterRoleRecord.ClusterRole.valueOf(response.getRole2().toStringUtf8()),
        response.getVersion());
      assertEquals(actual, expectedRecord);
    };
  }

  private PhoenixRegionServerEndpoint getPhoenixRegionServerEndpoint(HRegionServer regionServer) {
    PhoenixRegionServerEndpoint coproc = regionServer.getRegionServerCoprocessorHost()
      .findCoprocessor(PhoenixRegionServerEndpoint.class);
    return coproc;
  }

  private RegionServerEndpointProtos.GetClusterRoleRecordRequest
    getClusterRoleRecordRequest(String haGroupName) {
    RegionServerEndpointProtos.GetClusterRoleRecordRequest.Builder requestBuilder =
      RegionServerEndpointProtos.GetClusterRoleRecordRequest.newBuilder();
    requestBuilder.setHaGroupName(ByteStringer.wrap(Bytes.toBytes(haGroupName)));
    return requestBuilder.build();
  }

  private RegionServerEndpointProtos.InvalidateHAGroupStoreClientRequest
    getInvalidateHAGroupStoreClientRequest(String haGroupName) {
    RegionServerEndpointProtos.InvalidateHAGroupStoreClientRequest.Builder requestBuilder =
      RegionServerEndpointProtos.InvalidateHAGroupStoreClientRequest.newBuilder();
    requestBuilder.setHaGroupName(ByteStringer.wrap(Bytes.toBytes(haGroupName)));
    return requestBuilder.build();
  }

  private RegionServerCoprocessorEnvironment getTestCoprocessorEnvironment(Configuration conf) {
    return new RegionServerCoprocessorEnvironment() {

      @Override
      public int getVersion() {
        return 0;
      }

      @Override
      public String getHBaseVersion() {
        return "";
      }

      @Override
      public RegionServerCoprocessor getInstance() {
        return null;
      }

      @Override
      public int getPriority() {
        return 0;
      }

      @Override
      public int getLoadSequence() {
        return 0;
      }

      @Override
      public Configuration getConfiguration() {
        return conf;
      }

      @Override
      public ClassLoader getClassLoader() {
        return null;
      }

      @Override
      public ServerName getServerName() {
        return null;
      }

      @Override
      public OnlineRegions getOnlineRegions() {
        return null;
      }

      @Override
      public Connection getConnection() {
        return null;
      }

      @Override
      public Connection createConnection(Configuration conf) throws IOException {
        return null;
      }

      @Override
      public MetricRegistry getMetricRegistryForRegionServer() {
        return null;
      }
    };
  }
}
