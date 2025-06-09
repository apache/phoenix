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

import static org.apache.hadoop.hbase.GenericTestUtils.waitFor;
import static org.apache.phoenix.jdbc.PhoenixHAAdminTool.RET_REPAIR_FOUND_INCONSISTENCIES;
import static org.apache.phoenix.jdbc.PhoenixHAAdminTool.RET_SUCCESS;
import static org.apache.phoenix.jdbc.PhoenixHAAdminTool.RET_SYNC_ERROR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Optional;
import java.util.Properties;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.HAGroupStore.ClusterRole;
import org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.HBaseTestingUtilityPair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests for {@link PhoenixHAAdminTool}.
 *
 * @see PhoenixHAAdminToolTest
 */
@Category(NeedsOwnMiniClusterTest.class)
public class PhoenixHAAdminToolIT {
    private static final Logger LOG = LoggerFactory.getLogger(PhoenixHAAdminToolIT.class);
    private static final HBaseTestingUtilityPair CLUSTERS = new HBaseTestingUtilityPair();
    private static final PrintStream STDOUT = System.out;
    private static final ByteArrayOutputStream STDOUT_CAPTURE = new ByteArrayOutputStream();

    private String haGroupName;
    private HAGroupStore haGroupStoreV1, haGroupStoreV2; // two versions of haGroupStore for the same HA group
    private PhoenixHAAdminTool admin; // the HA admin to test; it's against cluster1.

    @Rule
    public final TestName testName = new TestName();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        CLUSTERS.start();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        getCurator1().close();
        getCurator2().close();
        CLUSTERS.close();
    }

    @Before
    public void setup() throws Exception {
        admin = new PhoenixHAAdminTool();
        admin.setConf(CLUSTERS.getHBaseCluster1().getConfiguration());
        haGroupName = testName.getMethodName();
        haGroupStoreV1 = new HAGroupStore(
                haGroupName, HighAvailabilityPolicy.FAILOVER,
                CLUSTERS.getZkUrl1(), ClusterRole.ACTIVE,
                CLUSTERS.getZkUrl2(), ClusterRole.STANDBY,
                1);
        String jsonFileName = HAGroupStoreTest.createJsonFileWithHaGroupStores(haGroupStoreV1);
        int ret = admin.run(new String[]{"-m", jsonFileName});
        if(ret != RET_SUCCESS) {
            throw new RuntimeException("Failed to create initial haGroupStores");
        }
        // the V2 haGroupStore is for the same HA group; it is created but not populated yet
        haGroupStoreV2 = new HAGroupStore(
                haGroupName, HighAvailabilityPolicy.FAILOVER,
                CLUSTERS.getZkUrl1(), ClusterRole.STANDBY,
                CLUSTERS.getZkUrl2(), ClusterRole.ACTIVE,
                2);
    }

    @After
    public void after() {
        // reset STDOUT in case it was captured for testing
        System.setOut(STDOUT);
    }

    /**
     * Test that the initial HAGroupStore on ZK is populated to clients correctly.
     */
    @Test(timeout = 180000)
    public void testCreateDataOnZookeeper() throws Exception {
        doVerifyClusterRole(haGroupStoreV1);
    }

    /**
     * Test that sync the same HAGroupStore work since it is no-op.
     */
    @Test(timeout = 180000)
    public void testUpdateSameDataOnZookeeper() throws Exception {
        String jsonFileName = HAGroupStoreTest.createJsonFileWithHaGroupStores(haGroupStoreV1);
        int ret = admin.run(new String[]{"-m", jsonFileName});
        assertEquals(RET_SUCCESS, ret);
        doVerifyClusterRole(haGroupStoreV1);
    }

    /**
     * Test that the updated cluster role is populated to clients correctly.
     */
    @Test(timeout = 180000)
    public void testUpdateDataOnZookeeper() throws Exception {
        String jsonFileName = HAGroupStoreTest.createJsonFileWithHaGroupStores(haGroupStoreV2);
        int ret = admin.run(new String[]{"-m", jsonFileName});
        assertEquals(RET_SUCCESS, ret);
        // Eventually HA group should have see this updated HAGroupStore from ZK
        doVerifyClusterRole(haGroupStoreV2);
    }

    /**
     * Test that the HA admin can support multiple HAGroupStores for different groups.
     */
    @Test(timeout = 180000)
    public void testCreateOrUpdateDataOnZookeeperForMultipleHAGroups() throws Exception {
        // Note it is for a different HA group, while haGroupStoreV2 is for the same HA group as haGroupStoreV1
        String haGroupName2 = haGroupName + 2;
        HAGroupStore haGroupStore2 = new HAGroupStore(
                haGroupName2, HighAvailabilityPolicy.FAILOVER,
                CLUSTERS.getZkUrl1(), ClusterRole.ACTIVE,
                CLUSTERS.getZkUrl2(), ClusterRole.STANDBY,
                1);
        // For haGroupName it's update and for haGroupName2 it's create.
        String jsonFileName = HAGroupStoreTest.createJsonFileWithHaGroupStores(haGroupStoreV2, haGroupStore2);
        int ret = admin.run(new String[]{"-m", jsonFileName});
        assertEquals(RET_SUCCESS, ret);
        doVerifyClusterRole(haGroupStoreV2);
        doVerifyClusterRole(haGroupStore2);
    }

    @Test(timeout = 180000)
    public void testListAllHAGroupStoresOnZookeeper() throws Exception {
        System.setOut(new PrintStream(STDOUT_CAPTURE));
        int ret = admin.run(new String[]{"-l"});
        assertEquals(RET_SUCCESS, ret);
        assertStdoutShouldHaveHaGroup(haGroupStoreV1);
    }

    private void assertStdoutShouldHaveHaGroup(HAGroupStore haGroupStore) {
        LOG.info("Got stdout: \n++++++++\n{}++++++++\n", STDOUT_CAPTURE.toString());
        assertTrue(STDOUT_CAPTURE.toString().contains(haGroupStore.getHaGroupName()));
    }

    /**
     * Test that --repair command options works.
     */
    @Test(timeout = 180000)
    public void testRepair() throws Exception {
        // no-op since both ZK nodes are the same initially after setup()
        int ret = admin.run(new String[]{"--repair"});
        assertEquals(RET_SUCCESS, ret);

        // Update ZK1 with newer version
        String zpath = ZKPaths.PATH_SEPARATOR + haGroupName;
        PhoenixHAAdminToolIT.this.getCurator1().setData().forPath(zpath, HAGroupStore.toJson(haGroupStoreV2));
        doVerifyClusterRole(getCurator1(), haGroupStoreV2);
        doVerifyClusterRole(PhoenixHAAdminToolIT.this.getCurator2(), haGroupStoreV1); // ZK2 still has old version
        ret = admin.run(new String[]{"--repair"}); // admin is created using cluster1 configuration
        assertEquals(RET_SUCCESS, ret);
        doVerifyClusterRole(getCurator1(), haGroupStoreV2);
        doVerifyClusterRole(getCurator2(), haGroupStoreV2);

        HAGroupStore haGroupStoreV3 = new HAGroupStore(
                haGroupName, HighAvailabilityPolicy.FAILOVER,
                CLUSTERS.getZkUrl1(), ClusterRole.ACTIVE,
                CLUSTERS.getZkUrl2(), ClusterRole.STANDBY,
                3);
        getCurator2().setData().forPath(zpath, HAGroupStore.toJson(haGroupStoreV3));
        doVerifyClusterRole(getCurator1(), haGroupStoreV2);
        doVerifyClusterRole(getCurator2(), haGroupStoreV3); // ZK2 has newer version
        ret = admin.run(new String[]{"--repair"}); // admin is created using cluster1 configuration
        assertEquals(RET_SUCCESS, ret);
        doVerifyClusterRole(getCurator1(), haGroupStoreV3);
        doVerifyClusterRole(getCurator2(), haGroupStoreV3);
    }

    /**
     * Test that --repair should report inconsistent haGroupStores.
     */
    @Test(timeout = 180000)
    public void testRepairGotInconsistentHaGroupStores() throws Exception {
        // Set ZK1 node with different HA policy and cluster roles but the same version v1
        String zpath = ZKPaths.PATH_SEPARATOR + haGroupName;
        try {
            HAGroupStore haGroupStoreDifferent = new HAGroupStore(
                    haGroupName, HighAvailabilityPolicy.PARALLEL,
                    CLUSTERS.getZkUrl1(), ClusterRole.STANDBY,
                    CLUSTERS.getZkUrl2(), ClusterRole.STANDBY,
                    1);
            getCurator1().setData().forPath(zpath, HAGroupStore.toJson(haGroupStoreDifferent));
            doVerifyClusterRole(getCurator1(), haGroupStoreDifferent);

            System.setOut(new PrintStream(STDOUT_CAPTURE)); // capture stdout
            int ret = ToolRunner.run(admin, new String[] { "--repair" });
            assertEquals(RET_REPAIR_FOUND_INCONSISTENCIES, ret);
            assertStdoutShouldHaveHaGroup(haGroupStoreV1); // should be reported back
        } finally {
            // reset for this HA group so that other tests will not see inconsistent haGroupStores.
            getCurator1().setData().forPath(zpath, HAGroupStore.toJson(haGroupStoreV1));
        }
    }

    /**
     * Test that updating two ZK clusters should fail if the first cluster is not healthy.
     *
     * The first cluster is the new STANDBY and previously it was ACTIVE. So it should be updated
     * first. If it is down, the update will fail and skip updating second cluster.
     */
    @Test(timeout = 180000)
    public void testUpdateDataOnZookeeperShouldFailWhenActiveZkClusterDown() throws Exception {
        System.setOut(new PrintStream(STDOUT_CAPTURE));
        int zkClientPort = CLUSTERS.getHBaseCluster1().getZkCluster().getClientPort();
        try {
            LOG.info("Shutting down the first HBase cluster...");
            CLUSTERS.getHBaseCluster1().shutdownMiniZKCluster();

            String jsonFileName = HAGroupStoreTest.createJsonFileWithHaGroupStores(haGroupStoreV2);
            int ret = admin.run(new String[]{"-m", jsonFileName});
            assertEquals(RET_SYNC_ERROR, ret);
            assertStdoutShouldHaveHaGroup(haGroupStoreV1);
            // cluster2 should still have the V1 haGroupStore because we should update cluster1 first
            doVerifyClusterRole(getCurator2(), haGroupStoreV1);
            // can not test cluster1 because it is still down
        } finally {
            CLUSTERS.getHBaseCluster1().startMiniZKCluster(1, zkClientPort);
        }
    }

    /**
     * Test that updating two ZK clusters forcefully.
     *
     * The first cluster is the new STANDBY and previously it was ACTIVE. So it should be updated
     * first. If it is down, the other cluster should still be updated if we update forcefully.
     */
    @Test(timeout = 180000)
    public void testUpdateDataOnZookeeperForcefulWhenActiveZKClusterDown() throws Exception {
        System.setOut(new PrintStream(STDOUT_CAPTURE));
        int zkClientPort = CLUSTERS.getHBaseCluster1().getZkCluster().getClientPort();
        try {
            LOG.info("Shutting down the first HBase cluster...");
            CLUSTERS.getHBaseCluster1().shutdownMiniZKCluster();

            String jsonFileName = HAGroupStoreTest.createJsonFileWithHaGroupStores(haGroupStoreV2);
            int ret = admin.run(new String[]{"-m", jsonFileName, "-F"});
            assertEquals(RET_SYNC_ERROR, ret);
            assertStdoutShouldHaveHaGroup(haGroupStoreV2);
            // cluster2 should have been updated forcefully
            doVerifyClusterRole(getCurator2(), haGroupStoreV2);
            // can not test cluster1 because it is still down
        } finally {
            CLUSTERS.getHBaseCluster1().startMiniZKCluster(1, zkClientPort);
        }
    }

    /**
     * Helper to verify cluster role is good on both ZK sides by comparing with the given haGroupStore.
     *
     * This is a side-effect free code. It only checks if the data on ZK is the same as given data.
     */
    private static void doVerifyClusterRole(HAGroupStore haGroupStore) throws Exception {
        doVerifyClusterRole(getCurator1(), haGroupStore);
        doVerifyClusterRole(getCurator2(), haGroupStore);
    }

    /**
     * Helper to verify cluster role is good on one ZK side by comparing with the given haGroupStore.
     *
     * This differs from above method since it is using only one curator ZK client for one cluster.
     * It internally will retry and timeout after some time, e.g. 15 seconds.
     */
    private static void doVerifyClusterRole(CuratorFramework curator,
            HAGroupStore haGroupStore) throws Exception {
        waitFor(() -> {
            try {
                String path = ZKPaths.PATH_SEPARATOR + haGroupStore.getHaGroupName();
                byte[] data = curator.getData().forPath(path);
                Optional<HAGroupStore> haGroupStoreFromZk = HAGroupStore.fromJson(data);
                return haGroupStoreFromZk.isPresent() && haGroupStoreFromZk.get().equals(haGroupStore);
            } catch (Exception e) {
                LOG.info("Got exception while waiting for znode is up to date: {}", e.getMessage());
                return false;
            }
        }, 1_000, 15_000);
    }

    private static CuratorFramework getCurator1() throws IOException {
        return HighAvailabilityGroup.getCurator(CLUSTERS.getZkUrl1(), new Properties());
    }

    private static CuratorFramework getCurator2() throws IOException {
        return HighAvailabilityGroup.getCurator(CLUSTERS.getZkUrl2(), new Properties());
    }
}
