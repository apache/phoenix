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
package org.apache.phoenix.end2end;

import com.google.protobuf.RpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.PhoenixRegionServerEndpoint;
import org.apache.phoenix.coprocessor.generated.RegionServerEndpointProtos;
import org.apache.phoenix.jdbc.ClusterRoleRecord;
import org.apache.phoenix.jdbc.HAGroupStoreRecord;
import org.apache.phoenix.jdbc.HighAvailabilityPolicy;
import org.apache.phoenix.jdbc.HighAvailabilityTestingUtility;
import org.apache.phoenix.jdbc.PhoenixHAAdmin;
import org.apache.phoenix.jdbc.HAGroupStoreRecord.HAGroupState;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.HAGroupStoreTestUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.util.Map;

import static org.apache.phoenix.jdbc.HAGroupStoreClient.ZK_CONSISTENT_HA_NAMESPACE;
import static org.apache.phoenix.jdbc.PhoenixHAAdmin.getLocalZkUrl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

@Category({NeedsOwnMiniClusterTest.class })
public class PhoenixRegionServerEndpointITWithConsistentFailover extends BaseTest {

    private static final Long ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS = 5000L;
    private static final HighAvailabilityTestingUtility.HBaseTestingUtilityPair CLUSTERS = new HighAvailabilityTestingUtility.HBaseTestingUtilityPair();
    private String zkUrl;
    private String peerZkUrl;

    @Rule
    public TestName testName = new TestName();

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
        CLUSTERS.start();
    }

    @Before
    public void setUp() throws Exception {
        zkUrl = getLocalZkUrl(config);
        peerZkUrl = CLUSTERS.getZkUrl2();
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(testName.getMethodName(), zkUrl, peerZkUrl,
                ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY, null);
    }

    @Test
    public void testGetClusterRoleRecordAndInvalidate() throws Exception {
        String haGroupName = testName.getMethodName();
        HRegionServer regionServer = utility.getMiniHBaseCluster().getRegionServer(0);
        PhoenixRegionServerEndpoint coprocessor = getPhoenixRegionServerEndpoint(regionServer);
        assertNotNull(coprocessor);
        ServerRpcController controller = new ServerRpcController();

        try (PhoenixHAAdmin peerHAAdmin = new PhoenixHAAdmin(CLUSTERS.getHBaseCluster2().getConfiguration(), ZK_CONSISTENT_HA_NAMESPACE)) {
            HAGroupStoreRecord peerHAGroupStoreRecord = new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, haGroupName, HAGroupState.STANDBY);
            peerHAAdmin.createHAGroupStoreRecordInZooKeeper(peerHAGroupStoreRecord);
        }
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // First getClusterRoleRecord to check if HAGroupStoreClient is working as expected
        ClusterRoleRecord expectedRecord = buildExpectedClusterRoleRecord(haGroupName, ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY);
        executeGetClusterRoleRecordAndVerify(coprocessor, controller, haGroupName, expectedRecord);

        // Change the role of local cluster to ACTIVE_TO_STANDBY in System Table
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(testName.getMethodName(), zkUrl, peerZkUrl,
        ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY, ClusterRoleRecord.ClusterRole.STANDBY, null);

        // Cluster Role will still be same as before as cache is not invalidated yet
        executeGetClusterRoleRecordAndVerify(coprocessor, controller, haGroupName, expectedRecord);

        // Now Invalidate the Cache
        coprocessor.invalidateHAGroupStoreClient(controller, getInvalidateHAGroupStoreClientRequest(haGroupName), null);
        assertFalse(controller.failed());

        // Local Cluster Role will be updated to ACTIVE_TO_STANDBY as cache is invalidated
        ClusterRoleRecord expectedRecordAfterInvalidation = buildExpectedClusterRoleRecord(haGroupName, ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY, ClusterRoleRecord.ClusterRole.STANDBY);
        executeGetClusterRoleRecordAndVerify(coprocessor, controller, haGroupName, expectedRecordAfterInvalidation);
    }

    private ClusterRoleRecord buildExpectedClusterRoleRecord(String haGroupName, ClusterRoleRecord.ClusterRole localRole, ClusterRoleRecord.ClusterRole peerRole) {
        return new ClusterRoleRecord(haGroupName, HighAvailabilityPolicy.FAILOVER, zkUrl, localRole, peerZkUrl, peerRole, 1);
    }

    private void executeGetClusterRoleRecordAndVerify(PhoenixRegionServerEndpoint coprocessor, ServerRpcController controller,
                                                      String haGroupName, ClusterRoleRecord expectedRecord) {
        RpcCallback<RegionServerEndpointProtos.GetClusterRoleRecordResponse> rpcCallback = createValidationCallback(haGroupName, expectedRecord);
        coprocessor.getClusterRoleRecord(controller, getClusterRoleRecordRequest(haGroupName), rpcCallback);
        assertFalse(controller.failed());
    }

    private RpcCallback<RegionServerEndpointProtos.GetClusterRoleRecordResponse> createValidationCallback(String haGroupName, ClusterRoleRecord expectedRecord) {
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
        PhoenixRegionServerEndpoint coproc = regionServer
                .getRegionServerCoprocessorHost()
                .findCoprocessor(PhoenixRegionServerEndpoint.class);
        return coproc;
    }

    private RegionServerEndpointProtos.GetClusterRoleRecordRequest getClusterRoleRecordRequest(String haGroupName) {
        RegionServerEndpointProtos.GetClusterRoleRecordRequest.Builder requestBuilder
                = RegionServerEndpointProtos.GetClusterRoleRecordRequest.newBuilder();
        requestBuilder.setHaGroupName(ByteStringer.wrap(Bytes.toBytes(haGroupName)));
        return requestBuilder.build();
    }

    private RegionServerEndpointProtos.InvalidateHAGroupStoreClientRequest getInvalidateHAGroupStoreClientRequest(String haGroupName) {
        RegionServerEndpointProtos.InvalidateHAGroupStoreClientRequest.Builder requestBuilder
                = RegionServerEndpointProtos.InvalidateHAGroupStoreClientRequest.newBuilder();
        requestBuilder.setBroadcastUpdate(false);
        requestBuilder.setHaGroupName(ByteStringer.wrap(Bytes.toBytes(haGroupName)));
        return  requestBuilder.build();
    }
}
