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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.exception.InvalidClusterRoleTransitionException;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;

import static org.apache.phoenix.jdbc.HAGroupStoreClient.ZK_CONSISTENT_HA_NAMESPACE;
import static org.apache.phoenix.jdbc.PhoenixHAAdmin.toPath;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Integration tests for {@link HAGroupStoreClient}
 */
@Category(NeedsOwnMiniClusterTest.class)
public class HAGroupStoreClientIT extends BaseTest {

    private static final Long ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS = 5000L;
    private static final HighAvailabilityTestingUtility.HBaseTestingUtilityPair CLUSTERS = new HighAvailabilityTestingUtility.HBaseTestingUtilityPair();
    private PhoenixHAAdmin haAdmin;
    private PhoenixHAAdmin peerHaAdmin;

    @Rule
    public TestName testName = new TestName();

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
        CLUSTERS.start();
    }

    @Before
    public void before() throws Exception {
        haAdmin = new PhoenixHAAdmin(CLUSTERS.getHBaseCluster1().getConfiguration(), ZK_CONSISTENT_HA_NAMESPACE);
        peerHaAdmin = new PhoenixHAAdmin(CLUSTERS.getHBaseCluster2().getConfiguration(), ZK_CONSISTENT_HA_NAMESPACE);
        haAdmin.getCurator().delete().quietly().forPath(toPath(testName.getMethodName()));
        peerHaAdmin.getCurator().delete().quietly().forPath(toPath(testName.getMethodName()));
    }

    @After
    public void after() throws Exception {
        haAdmin.getCurator().delete().quietly().forPath(toPath(testName.getMethodName()));
        peerHaAdmin.getCurator().delete().quietly().forPath(toPath(testName.getMethodName()));
        haAdmin.close();
        peerHaAdmin.close();
    }

    @Test
    public void testHAGroupStoreClientChangingPeerZKUrlToNull() throws Exception {
        String haGroupName = testName.getMethodName();
        String peerZKUrl = CLUSTERS.getZkUrl2();
        HAGroupStoreRecord record = new HAGroupStoreRecord("v1.0", haGroupName,
                ClusterRoleRecord.ClusterRole.ACTIVE, 1, "FAILOVER", System.currentTimeMillis(), peerZKUrl);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstance(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName);

        HAGroupStoreRecord currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
        assert currentRecord != null && currentRecord.getClusterRole() == ClusterRoleRecord.ClusterRole.ACTIVE;

        // Check that peerPathChildrenCache is not null in HAGroupStoreClient via reflection
        Field peerPathChildrenCache = HAGroupStoreClient.class.getDeclaredField("peerPathChildrenCache");
        peerPathChildrenCache.setAccessible(true);
        assertNotNull(peerPathChildrenCache.get(haGroupStoreClient));

        record = new HAGroupStoreRecord("v1.0", haGroupName,
                ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY, 2, "FAILOVER", System.currentTimeMillis(), null);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Check that peerPathChildrenCache is null in HAGroupStoreClient via reflection
        assertNull(peerPathChildrenCache.get(haGroupStoreClient));

        // Check that haGroupStoreClient is working as normal
        currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
        assert currentRecord != null && currentRecord.getClusterRole() == ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY;
    }

    @Test
    public void testHAGroupStoreClientChangingPeerZKUrlToInvalidUrlToValidUrl() throws Exception {
        String haGroupName = testName.getMethodName();
        String peerZKUrl = CLUSTERS.getZkUrl2();
        HAGroupStoreRecord record = new HAGroupStoreRecord("v1.0", haGroupName,
                ClusterRoleRecord.ClusterRole.ACTIVE, 1, "FAILOVER", System.currentTimeMillis(), peerZKUrl);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstance(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName);

        HAGroupStoreRecord currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
        assert currentRecord != null && currentRecord.getClusterRole() == ClusterRoleRecord.ClusterRole.ACTIVE;

        // Check that peerPathChildrenCache is not null in HAGroupStoreClient via reflection
        Field peerPathChildrenCache = HAGroupStoreClient.class.getDeclaredField("peerPathChildrenCache");
        peerPathChildrenCache.setAccessible(true);
        assertNotNull(peerPathChildrenCache.get(haGroupStoreClient));

        record = new HAGroupStoreRecord("v1.0", haGroupName,
                ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY, 2, "FAILOVER", System.currentTimeMillis(), "randompeer:2181");
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Check that peerPathChildrenCache is null in HAGroupStoreClient via reflection
        assertNull(peerPathChildrenCache.get(haGroupStoreClient));

        // Check that haGroupStoreClient is working as normal
        currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
        assert currentRecord != null && currentRecord.getClusterRole() == ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY;


        record = new HAGroupStoreRecord("v1.0", haGroupName,
                ClusterRoleRecord.ClusterRole.STANDBY, 3, "FAILOVER", System.currentTimeMillis(), peerZKUrl);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS*2);

        currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
        assert currentRecord != null && currentRecord.getClusterRole() == ClusterRoleRecord.ClusterRole.STANDBY;

        // Check that peerPathChildrenCache is not null in HAGroupStoreClient via reflection
        peerPathChildrenCache.setAccessible(true);
        assertNotNull(peerPathChildrenCache.get(haGroupStoreClient));
    }

    @Test
    public void testHAGroupStoreClientWithoutPeerZK() throws Exception {
        String haGroupName = testName.getMethodName();
        HAGroupStoreRecord record = new HAGroupStoreRecord("v1.0", haGroupName,
                ClusterRoleRecord.ClusterRole.ACTIVE, 1, "FAILOVER", System.currentTimeMillis(), null);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstance(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName);

        HAGroupStoreRecord currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
        assert currentRecord != null && currentRecord.getClusterRole() == ClusterRoleRecord.ClusterRole.ACTIVE;

        // Check that peerPathChildrenCache is null in HAGroupStoreClient via reflection
        Field peerPathChildrenCache = HAGroupStoreClient.class.getDeclaredField("peerPathChildrenCache");
        peerPathChildrenCache.setAccessible(true);
        assertNull(peerPathChildrenCache.get(haGroupStoreClient));
    }

    @Test
    public void testHAGroupStoreClientWithSingleHAGroupStoreRecord() throws Exception {
        String haGroupName = testName.getMethodName();
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstance(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName);

        // Create and store HAGroupStoreRecord with ACTIVE role, including peer ZK URL
        String peerZKUrl = CLUSTERS.getZkUrl2();
        HAGroupStoreRecord record = new HAGroupStoreRecord("v1.0", haGroupName,
                ClusterRoleRecord.ClusterRole.ACTIVE, 1, "FAILOVER", System.currentTimeMillis(), peerZKUrl);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        HAGroupStoreRecord currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
        assert currentRecord != null && currentRecord.getClusterRole() == ClusterRoleRecord.ClusterRole.ACTIVE;

        // Now Update HAGroupStoreRecord so that current cluster has state ACTIVE_TO_STANDBY
        record = new HAGroupStoreRecord("v1.0", haGroupName,
                ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY, 2, "FAILOVER", System.currentTimeMillis(), peerZKUrl);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        // Check that now the cluster should be in ActiveToStandby
        currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
        assert currentRecord != null && currentRecord.getClusterRole() == ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY;

        // Change it back to ACTIVE so that cluster is not in ACTIVE_TO_STANDBY state
        record = new HAGroupStoreRecord("v1.0", haGroupName,
                ClusterRoleRecord.ClusterRole.ACTIVE, 3, "FAILOVER", System.currentTimeMillis(), peerZKUrl);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
        assert currentRecord != null && currentRecord.getClusterRole() == ClusterRoleRecord.ClusterRole.ACTIVE;

        // Change it again to ACTIVE_TO_STANDBY so that we can validate watcher works repeatedly
        record = new HAGroupStoreRecord("v1.0", haGroupName,
                ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY, 4, "FAILOVER", System.currentTimeMillis(), peerZKUrl);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
        assert currentRecord != null && currentRecord.getClusterRole() == ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY;

        // Change it back to ACTIVE to verify transition works
        record = new HAGroupStoreRecord("v1.0", haGroupName,
                ClusterRoleRecord.ClusterRole.ACTIVE, 5, "FAILOVER", System.currentTimeMillis(), peerZKUrl);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
        assert currentRecord != null && currentRecord.getClusterRole() == ClusterRoleRecord.ClusterRole.ACTIVE;
    }


    /**
     * Helper method to create or update HAGroupStoreRecord on ZooKeeper
     */
    private void createOrUpdateHAGroupStoreRecordOnZookeeper(PhoenixHAAdmin haAdmin, String haGroupName, HAGroupStoreRecord record) throws Exception {
        String path = toPath(haGroupName);
        if (haAdmin.getCurator().checkExists().forPath(path) == null) {
            haAdmin.createHAGroupStoreRecordInZooKeeper(record);
        } else {
            final Pair<HAGroupStoreRecord, Stat> currentRecord = haAdmin.getHAGroupStoreRecordInZooKeeper(haGroupName);
            if (currentRecord.getRight() != null && currentRecord.getLeft() != null) {
                haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, record, currentRecord.getRight().getVersion());
            } else {
                throw new IOException("Current HAGroupStoreRecord in ZK is null, cannot update HAGroupStoreRecord " + haGroupName);
            }
        }
    }

    @Test
    public void testHAGroupStoreClientWithMultipleHAGroupStoreRecords() throws Exception {
        String haGroupName1 = testName.getMethodName() + "1";
        String haGroupName2 = testName.getMethodName() + "2";
        String peerZKUrl = CLUSTERS.getZkUrl2();

        // Setup initial HAGroupStoreRecords
        HAGroupStoreRecord record1 = new HAGroupStoreRecord("v1.0", haGroupName1,
                ClusterRoleRecord.ClusterRole.ACTIVE, 1, "FAILOVER", System.currentTimeMillis(), peerZKUrl);
        HAGroupStoreRecord record2 = new HAGroupStoreRecord("v1.0", haGroupName2,
                ClusterRoleRecord.ClusterRole.ACTIVE, 1, "PARALLEL", System.currentTimeMillis(), peerZKUrl);

        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName1, record1);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName2, record2);

        HAGroupStoreClient haGroupStoreClient1 = HAGroupStoreClient.getInstance(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName1);
        HAGroupStoreClient haGroupStoreClient2 = HAGroupStoreClient.getInstance(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName2);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        HAGroupStoreRecord currentRecord1 = haGroupStoreClient1.getHAGroupStoreRecord();
        HAGroupStoreRecord currentRecord2 = haGroupStoreClient2.getHAGroupStoreRecord();
        assert currentRecord1 != null && currentRecord1.getClusterRole() == ClusterRoleRecord.ClusterRole.ACTIVE;
        assert currentRecord2 != null && currentRecord2.getClusterRole() == ClusterRoleRecord.ClusterRole.ACTIVE;

        // Now Update HAGroupStoreRecord so that current cluster has state ACTIVE_TO_STANDBY for only 1 record
        record1 = new HAGroupStoreRecord("v1.0", haGroupName1,
                ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY, 2, "FAILOVER", System.currentTimeMillis(), peerZKUrl);
        record2 = new HAGroupStoreRecord("v1.0", haGroupName2,
                ClusterRoleRecord.ClusterRole.ACTIVE, 2, "PARALLEL", System.currentTimeMillis(), peerZKUrl);

        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName1, record1);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName2, record2);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        // Check that now the cluster should be in ActiveToStandby
        currentRecord1 = haGroupStoreClient1.getHAGroupStoreRecord();
        currentRecord2 = haGroupStoreClient2.getHAGroupStoreRecord();
        assert currentRecord1 != null && currentRecord1.getClusterRole() == ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY;
        assert currentRecord2 != null && currentRecord2.getClusterRole() == ClusterRoleRecord.ClusterRole.ACTIVE;

        // Change it back to ACTIVE so that cluster is not in ACTIVE_TO_STANDBY state
        record1 = new HAGroupStoreRecord("v1.0", haGroupName1,
                ClusterRoleRecord.ClusterRole.ACTIVE, 3, "FAILOVER", System.currentTimeMillis(), peerZKUrl);
        record2 = new HAGroupStoreRecord("v1.0", haGroupName2,
                ClusterRoleRecord.ClusterRole.ACTIVE, 3, "PARALLEL", System.currentTimeMillis(), peerZKUrl);

        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName1, record1);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName2, record2);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        currentRecord1 = haGroupStoreClient1.getHAGroupStoreRecord();
        currentRecord2 = haGroupStoreClient2.getHAGroupStoreRecord();
        assert currentRecord1 != null && currentRecord1.getClusterRole() == ClusterRoleRecord.ClusterRole.ACTIVE;
        assert currentRecord2 != null && currentRecord2.getClusterRole() == ClusterRoleRecord.ClusterRole.ACTIVE;

        // Change other record to ACTIVE_TO_STANDBY and one in ACTIVE state so that we can validate watcher works repeatedly
        record1 = new HAGroupStoreRecord("v1.0", haGroupName1,
                ClusterRoleRecord.ClusterRole.ACTIVE, 4, "FAILOVER", System.currentTimeMillis(), peerZKUrl);
        record2 = new HAGroupStoreRecord("v1.0", haGroupName2,
                ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY, 4, "PARALLEL", System.currentTimeMillis(), peerZKUrl);

        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName1, record1);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName2, record2);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        currentRecord1 = haGroupStoreClient1.getHAGroupStoreRecord();
        currentRecord2 = haGroupStoreClient2.getHAGroupStoreRecord();
        assert currentRecord1 != null && currentRecord1.getClusterRole() == ClusterRoleRecord.ClusterRole.ACTIVE;
        assert currentRecord2 != null && currentRecord2.getClusterRole() == ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY;
    }

    @Test
    public void testMultiThreadedAccessToHACache() throws Exception {
        String haGroupName = testName.getMethodName();
        String peerZKUrl = CLUSTERS.getZkUrl2();

        // Setup initial HAGroupStoreRecord
        HAGroupStoreRecord record = new HAGroupStoreRecord("v1.0", haGroupName,
                ClusterRoleRecord.ClusterRole.ACTIVE, 1, "FAILOVER", System.currentTimeMillis(), peerZKUrl);

        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstance(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        int threadCount = 10;
        final CountDownLatch latch = new CountDownLatch(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    HAGroupStoreRecord currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
                    assert currentRecord != null && currentRecord.getClusterRole() == ClusterRoleRecord.ClusterRole.ACTIVE;
                    latch.countDown();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
        assert latch.await(10, TimeUnit.SECONDS);

        // Update HAGroupStoreRecord
        record = new HAGroupStoreRecord("v1.0", haGroupName,
                ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY, 2, "FAILOVER", System.currentTimeMillis(), peerZKUrl);

        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        final CountDownLatch latch2 = new CountDownLatch(threadCount);
        executor = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    HAGroupStoreRecord currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
                    assert currentRecord != null && currentRecord.getClusterRole() == ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY;
                    latch2.countDown();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
        assert latch2.await(10, TimeUnit.SECONDS);
    }

    @Test
    public void testHAGroupStoreClientWithRootPathDeletion() throws Exception {
        String haGroupName = testName.getMethodName();
        String peerZKUrl = CLUSTERS.getZkUrl2();

        HAGroupStoreRecord record1 = new HAGroupStoreRecord("v1.0", haGroupName,
                ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY, 1, "FAILOVER", System.currentTimeMillis(), peerZKUrl);

        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record1);
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstance(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        HAGroupStoreRecord currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
        assert currentRecord != null && currentRecord.getClusterRole() == ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY;

        haAdmin.getCurator().delete().deletingChildrenIfNeeded().forPath(toPath(StringUtils.EMPTY));
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
        assert currentRecord == null;

        record1 = new HAGroupStoreRecord("v1.0", haGroupName,
                ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY, 2, "FAILOVER", System.currentTimeMillis(), peerZKUrl);

        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record1);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
        assert currentRecord != null && currentRecord.getClusterRole() == ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY;
    }

    @Test
    public void testThrowsExceptionWithZKDisconnectionAndThenConnection() throws Exception {
        String haGroupName = testName.getMethodName();
        String peerZKUrl = CLUSTERS.getZkUrl2();

        // Setup initial HAGroupStoreRecord
        HAGroupStoreRecord record = new HAGroupStoreRecord("v1.0", haGroupName,
                ClusterRoleRecord.ClusterRole.ACTIVE, 1, "FAILOVER", System.currentTimeMillis(), peerZKUrl);

        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstance(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        HAGroupStoreRecord currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
        assert currentRecord != null && currentRecord.getClusterRole() == ClusterRoleRecord.ClusterRole.ACTIVE;

        // Shutdown the ZK Cluster to simulate CONNECTION_SUSPENDED event
        CLUSTERS.getHBaseCluster1().shutdownMiniZKCluster();

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        //Check that HAGroupStoreClient instance is not healthy and throws IOException
        assertThrows(IOException.class, () -> haGroupStoreClient.getHAGroupStoreRecord());
        // Check that the HAGroupStoreClient instance is not healthy via reflection
        Field isHealthyField = HAGroupStoreClient.class.getDeclaredField("isHealthy");
        isHealthyField.setAccessible(true);
        assertFalse((boolean)isHealthyField.get(haGroupStoreClient));

        // Start ZK on the same port to simulate CONNECTION_RECONNECTED event
        CLUSTERS.getHBaseCluster1().startMiniZKCluster(1,
                Integer.parseInt(CLUSTERS.getHBaseCluster1().getConfiguration().get("hbase.zookeeper.property.clientPort")));

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        //Check that HAGroupStoreClient instance is back to healthy and provides correct response
        currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
        assert currentRecord != null && currentRecord.getClusterRole() == ClusterRoleRecord.ClusterRole.ACTIVE;
        // Check that the HAGroupStoreClient instance is healthy via reflection
        assertTrue((boolean)isHealthyField.get(haGroupStoreClient));
    }

    @Test
    public void testHAGroupStoreClientWithDifferentZKURLFormats() throws Exception {
        String haGroupName1 = testName.getMethodName() + "1";
        String haGroupName2 = testName.getMethodName() + "2";
        String haGroupName3 = testName.getMethodName() + "3";
        String peerZKUrl = CLUSTERS.getZkUrl2();

        final String zkClientPort = CLUSTERS.getHBaseCluster1().getConfiguration().get("hbase.zookeeper.property.clientPort");
        // Setup initial HAGroupStoreRecords with different ZK URL formats
        final String format1 = "127.0.0.1\\:"+zkClientPort+"::/hbase"; // 127.0.0.1\:53228::/hbase
        final String format2 = "127.0.0.1:"+zkClientPort+"::/hbase"; //   127.0.0.1:53228::/hbase
        final String format3 = "127.0.0.1\\:"+zkClientPort+":/hbase";   // 127.0.0.1\:53228:/hbase

        HAGroupStoreRecord record1 = new HAGroupStoreRecord("v1.0", haGroupName1,
                ClusterRoleRecord.ClusterRole.ACTIVE, 1, "PARALLEL", System.currentTimeMillis(), peerZKUrl);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName1, record1);

        HAGroupStoreRecord record2 = new HAGroupStoreRecord("v1.0", haGroupName2,
                ClusterRoleRecord.ClusterRole.STANDBY, 1, "PARALLEL", System.currentTimeMillis(), peerZKUrl);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName2, record2);

        HAGroupStoreRecord record3 = new HAGroupStoreRecord("v1.0", haGroupName3,
                ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY, 1, "PARALLEL", System.currentTimeMillis(), peerZKUrl);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName3, record3);

        HAGroupStoreClient haGroupStoreClient1 = HAGroupStoreClient.getInstance(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName1);
        HAGroupStoreClient haGroupStoreClient2 = HAGroupStoreClient.getInstance(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName2);
        HAGroupStoreClient haGroupStoreClient3 = HAGroupStoreClient.getInstance(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName3);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        HAGroupStoreRecord currentRecord1 = haGroupStoreClient1.getHAGroupStoreRecord();
        HAGroupStoreRecord currentRecord2 = haGroupStoreClient2.getHAGroupStoreRecord();
        HAGroupStoreRecord currentRecord3 = haGroupStoreClient3.getHAGroupStoreRecord();
        assert currentRecord1 != null && currentRecord1.getClusterRole() == ClusterRoleRecord.ClusterRole.ACTIVE;
        assert currentRecord2 != null && currentRecord2.getClusterRole() == ClusterRoleRecord.ClusterRole.STANDBY;
        assert currentRecord3 != null && currentRecord3.getClusterRole() == ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY;
    }

    // Tests for setHAGroupStatusIfNeeded method

    @Test
    public void testSetHAGroupStatusIfNeededCreateNewRecord() throws Exception {
        String haGroupName = testName.getMethodName();

        // Create HAGroupStoreClient without any existing record
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstance(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName);

        // This should fail because no record exists and fetchHAGroupStoreRecordFromSystemTable returns null
        try {
            haGroupStoreClient.setHAGroupStatusIfNeeded(ClusterRoleRecord.ClusterRole.ACTIVE);
            fail("Expected IOException for missing system table record");
        } catch (IOException e) {
            assertTrue("Exception should mention system table", e.getMessage().contains("system table"));
        }
    }

    @Test
    public void testSetHAGroupStatusIfNeededUpdateExistingRecord() throws Exception {
        String haGroupName = testName.getMethodName();
        String peerZKUrl = CLUSTERS.getZkUrl2();

        // Create initial record
        HAGroupStoreRecord initialRecord = new HAGroupStoreRecord("v1.0", haGroupName,
                ClusterRoleRecord.ClusterRole.ACTIVE, 1, "FAILOVER", System.currentTimeMillis(), peerZKUrl);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, initialRecord);

        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstance(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Verify initial state
        HAGroupStoreRecord currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
        assertEquals(ClusterRoleRecord.ClusterRole.ACTIVE, currentRecord.getClusterRole());

        // Update to STANDBY (this should succeed as it's a valid transition)
        haGroupStoreClient.setHAGroupStatusIfNeeded(ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Verify the record was updated
        currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
        assertEquals(ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY, currentRecord.getClusterRole());

        // Verify the version was incremented
        HAGroupStoreRecord updatedRecord = haGroupStoreClient.getHAGroupStoreRecord();
        assertEquals(2, updatedRecord.getVersion());
    }

    @Test
    public void testSetHAGroupStatusIfNeededNoUpdateWhenNotNeeded() throws Exception {
        String haGroupName = testName.getMethodName();
        String peerZKUrl = CLUSTERS.getZkUrl2();

        // Create initial record with current timestamp
        HAGroupStoreRecord initialRecord = new HAGroupStoreRecord("v1.0", haGroupName,
                ClusterRoleRecord.ClusterRole.ACTIVE, 1, "FAILOVER", System.currentTimeMillis(), peerZKUrl);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, initialRecord);

        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstance(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName);

        // Get the current record
        HAGroupStoreRecord currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
        long originalVersion = currentRecord.getVersion();
        long originalTimestamp = currentRecord.getLastUpdatedTimeInMs();

        // Try to set to ACTIVE_NOT_IN_SYNC immediately (should not update due to timing)
        haGroupStoreClient.setHAGroupStatusIfNeeded(ClusterRoleRecord.ClusterRole.ACTIVE_NOT_IN_SYNC);

        // Add sleep if due to any bug the update might have gone through and we can assert below this.
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        // Verify no update occurred
        HAGroupStoreRecord afterRecord = haGroupStoreClient.getHAGroupStoreRecord();
        assertEquals("Version should not change", originalVersion, afterRecord.getVersion());
        assertEquals("Timestamp should not change", originalTimestamp, afterRecord.getLastUpdatedTimeInMs());
        assertEquals("Role should not change", ClusterRoleRecord.ClusterRole.ACTIVE, afterRecord.getClusterRole());
    }

    @Test
    public void testSetHAGroupStatusIfNeededWithTimingLogic() throws Exception {
        String haGroupName = testName.getMethodName();
        String peerZKUrl = CLUSTERS.getZkUrl2();

        // Create initial record with old timestamp to ensure timing check passes
        long oldTimestamp = System.currentTimeMillis() - 10000; // 10 seconds ago
        HAGroupStoreRecord initialRecord = new HAGroupStoreRecord("v1.0", haGroupName,
                ClusterRoleRecord.ClusterRole.ACTIVE, 1, "FAILOVER", oldTimestamp, peerZKUrl);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, initialRecord);

        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstance(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Update to ACTIVE_NOT_IN_SYNC (should succeed due to old timestamp)
        haGroupStoreClient.setHAGroupStatusIfNeeded(ClusterRoleRecord.ClusterRole.ACTIVE_NOT_IN_SYNC);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Verify the record was updated
        HAGroupStoreRecord updatedRecord = haGroupStoreClient.getHAGroupStoreRecord();
        assertEquals(ClusterRoleRecord.ClusterRole.ACTIVE_NOT_IN_SYNC, updatedRecord.getClusterRole());
        assertEquals(2, updatedRecord.getVersion());
        assertTrue("Timestamp should be updated", updatedRecord.getLastUpdatedTimeInMs() > oldTimestamp);
    }

    @Test
    public void testSetHAGroupStatusIfNeededWithInvalidTransition() throws Exception {
        String haGroupName = testName.getMethodName();
        String peerZKUrl = CLUSTERS.getZkUrl2();

        // Create initial record with ACTIVE state
        HAGroupStoreRecord initialRecord = new HAGroupStoreRecord("v1.0", haGroupName,
                ClusterRoleRecord.ClusterRole.ACTIVE, 1, "FAILOVER", System.currentTimeMillis() - 10000, peerZKUrl);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, initialRecord);

        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstance(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Try to transition to STANDBY (invalid transition from ACTIVE)
        try {
            haGroupStoreClient.setHAGroupStatusIfNeeded(ClusterRoleRecord.ClusterRole.STANDBY);
            fail("Expected InvalidClusterRoleTransitionException");
        } catch (InvalidClusterRoleTransitionException e) {
            // Should get InvalidClusterRoleTransitionException (might be wrapped)
            assertTrue("Exception should be about invalid transition",
                    e.getMessage().contains("Cannot transition from ACTIVE to STANDBY") ||
                            e.getCause() != null && e.getCause().getMessage().contains("Cannot transition from ACTIVE to STANDBY"));
        }
    }

    @Test
    public void testSetHAGroupStatusIfNeededWithUnhealthyClient() throws Exception {
        String haGroupName = testName.getMethodName();

        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstance(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName);

        // Make client unhealthy by accessing private field
        Field isHealthyField = HAGroupStoreClient.class.getDeclaredField("isHealthy");
        isHealthyField.setAccessible(true);
        isHealthyField.set(haGroupStoreClient, false);

        // Try to set status on unhealthy client
        try {
            haGroupStoreClient.setHAGroupStatusIfNeeded(ClusterRoleRecord.ClusterRole.ACTIVE);
            fail("Expected IOException for unhealthy client");
        } catch (IOException e) {
            assertTrue("Exception should mention unhealthy client", e.getMessage().contains("not healthy"));
        }
    }


    @Test
    public void testSetHAGroupStatusIfNeededMultipleTransitions() throws Exception {
        String haGroupName = testName.getMethodName();
        String peerZKUrl = CLUSTERS.getZkUrl2();

        // Create initial record with old timestamp
        HAGroupStoreRecord initialRecord = new HAGroupStoreRecord("v1.0", haGroupName,
                ClusterRoleRecord.ClusterRole.ACTIVE, 1, "FAILOVER", System.currentTimeMillis() - 10000, peerZKUrl);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, initialRecord);

        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstance(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // First transition: ACTIVE -> ACTIVE_TO_STANDBY
        haGroupStoreClient.setHAGroupStatusIfNeeded(ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        HAGroupStoreRecord afterFirst = haGroupStoreClient.getHAGroupStoreRecord();
        assertEquals(ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY, afterFirst.getClusterRole());
        assertEquals(2, afterFirst.getVersion());

        // Wait and make another transition: ACTIVE_TO_STANDBY -> STANDBY
        Thread.sleep(100); // Small delay to ensure timestamp difference
        haGroupStoreClient.setHAGroupStatusIfNeeded(ClusterRoleRecord.ClusterRole.STANDBY);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        HAGroupStoreRecord afterSecond = haGroupStoreClient.getHAGroupStoreRecord();
        assertEquals(ClusterRoleRecord.ClusterRole.STANDBY, afterSecond.getClusterRole());
        assertEquals(3, afterSecond.getVersion());
    }

    /**
     * This test verifies that the updates coming via PathChildrenCacheListener are in order in which updates are sent to ZK
     * @throws Exception
     */
    @Test
    public void testHAGroupStoreClientWithMultiThreadedUpdates() throws Exception {
        String haGroupName = testName.getMethodName();
        String peerZKUrl = CLUSTERS.getZkUrl2();

        // Number of threads to execute
        int threadCount = 5;

        // Capture versions of records in a list(recordEventVersions) in order they are received.
        List<Integer> recordEventVersions = new ArrayList<>();
        CountDownLatch eventsLatch = new CountDownLatch(threadCount);
        PathChildrenCacheListener pathChildrenCacheListener = (client, event) -> {
            if(event.getData() != null && event.getData().getData() != null && HAGroupStoreRecord.fromJson(event.getData().getData()).isPresent()) {
                HAGroupStoreRecord record = HAGroupStoreRecord.fromJson(event.getData().getData()).get();
                if (record.getHaGroupName().equals(haGroupName)) {
                    recordEventVersions.add((int)record.getVersion());
                    eventsLatch.countDown();
                }
            }
        };

        // Start a new HAGroupStoreClient with custom listener.
        new HAGroupStoreClient(CLUSTERS.getHBaseCluster1().getConfiguration(), pathChildrenCacheListener, null, haGroupName);

        // Create multiple threads for update to ZK.
        final CountDownLatch updateLatch = new CountDownLatch(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        // List captures the order of events that are sent.
        List<Integer> updateList = new ArrayList<>();

        // Create a queue which can be polled to send updates to ZK.
        ConcurrentLinkedQueue<HAGroupStoreRecord> updateQueue = new ConcurrentLinkedQueue<>();
        for(int i = 0; i < threadCount; i++) {
            updateQueue.add(createHAGroupStoreRecord(haGroupName, peerZKUrl, i+1));
            updateList.add(i+1);
        }

        // Submit updates to ZK.
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    synchronized (HAGroupStoreClientIT.class) {
                        HAGroupStoreRecord record = Objects.requireNonNull(updateQueue.poll());
                        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);
                    }
                    updateLatch.countDown();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        // Check if updates are sent and updates are received.
        assert eventsLatch.await(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS*threadCount, TimeUnit.MILLISECONDS);
        assert updateLatch.await(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS*threadCount, TimeUnit.MILLISECONDS);

        // Assert that the order of updates is same as order of events.
        assert updateList.equals(recordEventVersions);
    }

    private HAGroupStoreRecord createHAGroupStoreRecord(String haGroupName, String peerZKUrl, Integer version) {
        return new HAGroupStoreRecord("v1.0", haGroupName,
                ClusterRoleRecord.ClusterRole.ACTIVE, version, "PARALLEL", System.currentTimeMillis(), peerZKUrl);
    }

}