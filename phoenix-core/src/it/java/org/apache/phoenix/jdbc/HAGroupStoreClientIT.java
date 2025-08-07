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
import org.apache.phoenix.util.HAGroupStoreTestUtil;
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
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
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

import static org.apache.hadoop.hbase.HConstants.DEFAULT_ZK_SESSION_TIMEOUT;
import static org.apache.phoenix.jdbc.HAGroupStoreClient.ZK_CONSISTENT_HA_NAMESPACE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_HA_GROUP_NAME;
import static org.apache.phoenix.jdbc.PhoenixHAAdmin.getLocalZkUrl;
import static org.apache.phoenix.jdbc.PhoenixHAAdmin.toPath;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_ZK;
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
    private String zkUrl;
    private String peerZKUrl;

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
        zkUrl = getLocalZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration());
        // Clean existing records in system table
        List<String> haGroupNames = HAGroupStoreClient.getHAGroupNames(zkUrl);
        for (String groupName : haGroupNames) {
            HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(groupName, zkUrl);
        }
        // Insert a HAGroupStoreRecord into the system table
        String haGroupName = testName.getMethodName();
        this.peerZKUrl = CLUSTERS.getZkUrl2();
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName, this.zkUrl, this.peerZKUrl, ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY, null);
    }

    @After
    public void after() throws Exception {
        haAdmin.getCurator().delete().quietly().forPath(toPath(testName.getMethodName()));
        peerHaAdmin.getCurator().delete().quietly().forPath(toPath(testName.getMethodName()));
        haAdmin.close();
        peerHaAdmin.close();
        HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(testName.getMethodName(), zkUrl);

    }

    @Test
    public void testHAGroupStoreClientWithBothNullZKUrl() throws Exception {
        String haGroupName = testName.getMethodName();
        // Clean existing record
        HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(haGroupName, zkUrl);
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName, null, null, ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY, this.zkUrl);
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName, zkUrl);
        assertNull(haGroupStoreClient);
    }

    @Test
    public void testHAGroupStoreClientChangingPeerZKUrlToNullUrlToValidUrlToInvalidUrl() throws Exception {
        String haGroupName = testName.getMethodName();
        HAGroupStoreRecord record = new HAGroupStoreRecord("v1.0", haGroupName,
                HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName, zkUrl);

        HAGroupStoreRecord currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
        assert currentRecord != null && currentRecord.getHAGroupState() == HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC;

        // Base case: Check that peerPathChildrenCache is not null in HAGroupStoreClient via reflection
        Field peerPathChildrenCache = HAGroupStoreClient.class.getDeclaredField("peerPathChildrenCache");
        peerPathChildrenCache.setAccessible(true);
        assertNotNull(peerPathChildrenCache.get(haGroupStoreClient));

        // Clean existing record
        HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(haGroupName, zkUrl);
        // Now update peerZKUrl to null and rebuild
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName, this.zkUrl, null, ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY, null);
        try {
            haGroupStoreClient.rebuild(false);
            fail("Should have thrown NullPointerException");
        } catch (NullPointerException npe) {
            // This exception is expected here.
        }

        // Now update System table to contain valid peer ZK URL and also change local cluster role to STANDBY
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName,this.zkUrl, this.peerZKUrl, ClusterRoleRecord.ClusterRole.STANDBY, ClusterRoleRecord.ClusterRole.ACTIVE, null);
        haGroupStoreClient.rebuild(false);
        currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
        assertNotNull(currentRecord);
        assertEquals(HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY, currentRecord.getHAGroupState());

        // Check that peerPathChildrenCache is not null now in HAGroupStoreClient via reflection
        assertNotNull(peerPathChildrenCache.get(haGroupStoreClient));

        // Now update local HAGroupStoreRecord to STANDBY to verify that HAGroupStoreClient is working as normal
        record = new HAGroupStoreRecord("v1.0", haGroupName,
                HAGroupStoreRecord.HAGroupState.STANDBY);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
        assertNotNull(currentRecord);
        assertEquals(HAGroupStoreRecord.HAGroupState.STANDBY, currentRecord.getHAGroupState());


        // Now update peerZKUrl to invalid but non-null url and rebuild
        // This URL can also be considered unreachable url due to a connectivity issue.
        String invalidUrl = "invalidURL";
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName, this.zkUrl, invalidUrl, ClusterRoleRecord.ClusterRole.STANDBY, ClusterRoleRecord.ClusterRole.ACTIVE, null);
        haGroupStoreClient.rebuild(false);
        currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
        assertNotNull(currentRecord);
        assertEquals(HAGroupStoreRecord.HAGroupState.STANDBY, currentRecord.getHAGroupState());

        // Check CRR, the role for peer cluster with invalid url should be UNKNOWN.
        ClusterRoleRecord clusterRoleRecord = haGroupStoreClient.getClusterRoleRecord();
        assertNotNull(clusterRoleRecord);
        ClusterRoleRecord expected = new ClusterRoleRecord(haGroupName,
                HighAvailabilityPolicy.FAILOVER,
                this.zkUrl,
                ClusterRoleRecord.ClusterRole.STANDBY,
                invalidUrl,
                ClusterRoleRecord.ClusterRole.UNKNOWN,
                1);
        assertEquals(expected, clusterRoleRecord);

        // Check that peerPathChildrenCache is null now in HAGroupStoreClient via reflection
        assertNull(peerPathChildrenCache.get(haGroupStoreClient));


    }

    @Test
    public void testHAGroupStoreClientWithoutPeerZK() throws Exception {
        String haGroupName = testName.getMethodName();
        // Clean existing record
        HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(haGroupName, this.zkUrl);
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName,this.zkUrl, null, ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY, null);
        HAGroupStoreRecord record = new HAGroupStoreRecord("v1.0", haGroupName,
                HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);
        assertNull(HAGroupStoreClient.getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName, zkUrl));
    }

    @Test
    public void testHAGroupStoreClientWithSingleHAGroupStoreRecord() throws Exception {
        String haGroupName = testName.getMethodName();
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName, zkUrl);

        // Create and store HAGroupStoreRecord with ACTIVE state
        HAGroupStoreRecord record = new HAGroupStoreRecord("v1.0", haGroupName,
                HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        HAGroupStoreRecord currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
        assert currentRecord != null && currentRecord.getHAGroupState() == HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC;

        // Now Update HAGroupStoreRecord so that current cluster has state ACTIVE_TO_STANDBY
        record = new HAGroupStoreRecord("v1.0", haGroupName,
                HAGroupStoreRecord.HAGroupState.ACTIVE_TO_STANDBY);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        // Check that now the cluster should be in ActiveToStandby
        currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
        assert currentRecord != null && currentRecord.getHAGroupState() == HAGroupStoreRecord.HAGroupState.ACTIVE_TO_STANDBY;

        // Change it back to ACTIVE so that cluster is not in ACTIVE_TO_STANDBY state
        record = new HAGroupStoreRecord("v1.0", haGroupName,
                HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
        assert currentRecord != null && currentRecord.getHAGroupState() == HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC;

        // Change it again to ACTIVE_TO_STANDBY so that we can validate watcher works repeatedly
        record = new HAGroupStoreRecord("v1.0", haGroupName,
                HAGroupStoreRecord.HAGroupState.ACTIVE_TO_STANDBY);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
        assert currentRecord != null && currentRecord.getHAGroupState() == HAGroupStoreRecord.HAGroupState.ACTIVE_TO_STANDBY;

        // Change it back to ACTIVE to verify transition works
        record = new HAGroupStoreRecord("v1.0", haGroupName,
                HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
        assert currentRecord != null && currentRecord.getHAGroupState() == HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC;
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
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName1,this.zkUrl, this.peerZKUrl, ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY, null);
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName2,this.zkUrl, this.peerZKUrl, ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY, null);

        // Setup initial HAGroupStoreRecords
        HAGroupStoreRecord record1 = new HAGroupStoreRecord("v1.0", haGroupName1,
                HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);
        HAGroupStoreRecord record2 = new HAGroupStoreRecord("v1.0", haGroupName2,
                HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);

        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName1, record1);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName2, record2);

        HAGroupStoreClient haGroupStoreClient1 = HAGroupStoreClient.getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName1, zkUrl);
        HAGroupStoreClient haGroupStoreClient2 = HAGroupStoreClient.getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName2, zkUrl);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        HAGroupStoreRecord currentRecord1 = haGroupStoreClient1.getHAGroupStoreRecord();
        HAGroupStoreRecord currentRecord2 = haGroupStoreClient2.getHAGroupStoreRecord();
        assert currentRecord1 != null && currentRecord1.getHAGroupState() == HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC;
        assert currentRecord2 != null && currentRecord2.getHAGroupState() == HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC;

        // Now Update HAGroupStoreRecord so that current cluster has state ACTIVE_TO_STANDBY for only 1 record
        record1 = new HAGroupStoreRecord("v1.0", haGroupName1,
                HAGroupStoreRecord.HAGroupState.ACTIVE_TO_STANDBY);
        record2 = new HAGroupStoreRecord("v1.0", haGroupName2,
                HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);

        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName1, record1);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName2, record2);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        // Check that now the cluster should be in ActiveToStandby
        currentRecord1 = haGroupStoreClient1.getHAGroupStoreRecord();
        currentRecord2 = haGroupStoreClient2.getHAGroupStoreRecord();
        assert currentRecord1 != null && currentRecord1.getHAGroupState() == HAGroupStoreRecord.HAGroupState.ACTIVE_TO_STANDBY;
        assert currentRecord2 != null && currentRecord2.getHAGroupState() == HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC;

        // Change it back to ACTIVE so that cluster is not in ACTIVE_TO_STANDBY state
        record1 = new HAGroupStoreRecord("v1.0", haGroupName1,
                HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);
        record2 = new HAGroupStoreRecord("v1.0", haGroupName2,
                HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);

        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName1, record1);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName2, record2);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        currentRecord1 = haGroupStoreClient1.getHAGroupStoreRecord();
        currentRecord2 = haGroupStoreClient2.getHAGroupStoreRecord();
        assert currentRecord1 != null && currentRecord1.getHAGroupState() == HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC;
        assert currentRecord2 != null && currentRecord2.getHAGroupState() == HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC;

        // Change other record to ACTIVE_TO_STANDBY and one in ACTIVE state so that we can validate watcher works repeatedly
        record1 = new HAGroupStoreRecord("v1.0", haGroupName1,
                HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);
        record2 = new HAGroupStoreRecord("v1.0", haGroupName2,
                HAGroupStoreRecord.HAGroupState.ACTIVE_TO_STANDBY);

        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName1, record1);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName2, record2);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        currentRecord1 = haGroupStoreClient1.getHAGroupStoreRecord();
        currentRecord2 = haGroupStoreClient2.getHAGroupStoreRecord();
        assert currentRecord1 != null && currentRecord1.getHAGroupState() == HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC;
        assert currentRecord2 != null && currentRecord2.getHAGroupState() == HAGroupStoreRecord.HAGroupState.ACTIVE_TO_STANDBY;
    }

    @Test
    public void testMultiThreadedAccessToHACache() throws Exception {
        String haGroupName = testName.getMethodName();

        // Setup initial HAGroupStoreRecord
        HAGroupStoreRecord record = new HAGroupStoreRecord("v1.0", haGroupName,
                HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);

        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName, zkUrl);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        int threadCount = 10;
        final CountDownLatch latch = new CountDownLatch(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    HAGroupStoreRecord currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
                    assert currentRecord != null && currentRecord.getHAGroupState() == HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC;
                    latch.countDown();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
        assert latch.await(10, TimeUnit.SECONDS);

        // Update HAGroupStoreRecord
        record = new HAGroupStoreRecord("v1.0", haGroupName,
                HAGroupStoreRecord.HAGroupState.ACTIVE_TO_STANDBY);

        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        final CountDownLatch latch2 = new CountDownLatch(threadCount);
        executor = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    HAGroupStoreRecord currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
                    assert currentRecord != null && currentRecord.getHAGroupState() == HAGroupStoreRecord.HAGroupState.ACTIVE_TO_STANDBY;
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

        HAGroupStoreRecord record1 = new HAGroupStoreRecord("v1.0", haGroupName,
                HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);

        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record1);
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName, zkUrl);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        HAGroupStoreRecord currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
        assertNotNull(currentRecord);
        assertEquals(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC, currentRecord.getHAGroupState());

        haAdmin.getCurator().delete().deletingChildrenIfNeeded().forPath(toPath(StringUtils.EMPTY));
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        currentRecord = haGroupStoreClient.getHAGroupStoreRecord();

        // The record should be automatically rebuilt from System Table as it is not in ZK
        assertNotNull(currentRecord);
        assertEquals(HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC, currentRecord.getHAGroupState());

        record1 = new HAGroupStoreRecord("v1.0", haGroupName,
                HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC_TO_STANDBY);

        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record1);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
        assertNotNull(currentRecord);
        assertEquals(HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC_TO_STANDBY, currentRecord.getHAGroupState());
    }

    @Test
    public void testThrowsExceptionWithZKDisconnectionAndThenConnection() throws Exception {
        String haGroupName = testName.getMethodName();

        // Setup initial HAGroupStoreRecord
        HAGroupStoreRecord record = new HAGroupStoreRecord("v1.0", haGroupName,
                HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);

        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName, zkUrl);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        HAGroupStoreRecord currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
        assert currentRecord != null && currentRecord.getHAGroupState() == HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC;

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
        assert currentRecord != null && currentRecord.getHAGroupState() == HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC;
        // Check that the HAGroupStoreClient instance is healthy via reflection
        assertTrue((boolean)isHealthyField.get(haGroupStoreClient));
    }



    // Tests for setHAGroupStatusIfNeeded method
    @Test
    public void testSetHAGroupStatusIfNeededDeleteZKAndSystemTableRecord() throws Exception {
        String haGroupName = testName.getMethodName();

        // Create HAGroupStoreClient without any existing record
        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName, zkUrl);

        // Delete the record from ZK
        haAdmin.getCurator().delete().deletingChildrenIfNeeded().forPath(toPath(haGroupName));
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Delete the record from System Table
        try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(JDBC_PROTOCOL_ZK + JDBC_PROTOCOL_SEPARATOR + zkUrl);
             Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM " + SYSTEM_HA_GROUP_NAME + " WHERE HA_GROUP_NAME = '" + haGroupName + "'");
            conn.commit();
        }

        // This should fail because no record exists in either ZK or System Table
        try {
            haGroupStoreClient.setHAGroupStatusIfNeeded(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);
            fail("Expected IOException for missing system table record");
        } catch (IOException e) {
            assertTrue("Exception should mention system table", e.getMessage().contains("Current HAGroupStoreRecordStat in cache is null, "
                    + "cannot update HAGroupStoreRecord, the record should be initialized in System Table first"));
        }
    }

    @Test
    public void testSetHAGroupStatusIfNeededUpdateExistingRecord() throws Exception {
        String haGroupName = testName.getMethodName();

        // Create initial record
        HAGroupStoreRecord initialRecord = new HAGroupStoreRecord("v1.0", haGroupName,
                HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, initialRecord);

        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName, zkUrl);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Verify initial state
        HAGroupStoreRecord currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
        assertEquals(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC, currentRecord.getHAGroupState());

        // Update to STANDBY (this should succeed as it's a valid transition)
        haGroupStoreClient.setHAGroupStatusIfNeeded(HAGroupStoreRecord.HAGroupState.ACTIVE_TO_STANDBY);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Verify the record was updated
        currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
        assertEquals(HAGroupStoreRecord.HAGroupState.ACTIVE_TO_STANDBY, currentRecord.getHAGroupState());
    }

    @Test
    public void testSetHAGroupStatusIfNeededNoUpdateWhenNotNeeded() throws Exception {
        String haGroupName = testName.getMethodName();

        // Create initial record with current timestamp
        HAGroupStoreRecord initialRecord = new HAGroupStoreRecord("v1.0", haGroupName,
                HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, initialRecord);
        Stat initialRecordInZKStat = haAdmin.getHAGroupStoreRecordInZooKeeper(haGroupName).getRight();
        int initialRecordVersion = initialRecordInZKStat.getVersion();

        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName, zkUrl);

        // Get the current record
        HAGroupStoreRecord currentRecord = haGroupStoreClient.getHAGroupStoreRecord();

        // Try to set to ACTIVE_IN_SYNC immediately (should not update due to timing)
        haGroupStoreClient.setHAGroupStatusIfNeeded(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);

        // Add sleep if due to any bug the update might have gone through and we can assert below this.
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        // Verify no update occurred
        HAGroupStoreRecord afterRecord = haGroupStoreClient.getHAGroupStoreRecord();
        Stat afterRecordInZKStat = haAdmin.getHAGroupStoreRecordInZooKeeper(haGroupName).getRight();
        int afterRecordVersion = afterRecordInZKStat.getVersion();

        assertEquals(initialRecordVersion, afterRecordVersion);
        assertEquals("State should not change", HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC, afterRecord.getHAGroupState());
    }

    @Test
    public void testSetHAGroupStatusIfNeededWithTimingLogic() throws Exception {
        String haGroupName = testName.getMethodName();
        // Create initial record
        HAGroupStoreRecord initialRecord = new HAGroupStoreRecord("v1.0", haGroupName,
                HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, initialRecord);

        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName, zkUrl);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS + DEFAULT_ZK_SESSION_TIMEOUT);

        haGroupStoreClient.setHAGroupStatusIfNeeded(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Verify the record was updated
        HAGroupStoreRecord updatedRecord = haGroupStoreClient.getHAGroupStoreRecord();
        assertEquals(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC, updatedRecord.getHAGroupState());
    }

    @Test
    public void testSetHAGroupStatusIfNeededWithInvalidTransition() throws Exception {
        String haGroupName = testName.getMethodName();

        // Create initial record with ACTIVE state
        HAGroupStoreRecord initialRecord = new HAGroupStoreRecord("v1.0", haGroupName,
                HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, initialRecord);

        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName, zkUrl);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Try to transition to STANDBY (invalid transition from ACTIVE)
        try {
            haGroupStoreClient.setHAGroupStatusIfNeeded(HAGroupStoreRecord.HAGroupState.STANDBY);
            fail("Expected InvalidClusterRoleTransitionException");
        } catch (InvalidClusterRoleTransitionException e) {
            // Should get InvalidClusterRoleTransitionException (might be wrapped)
            assertTrue("Exception should be about invalid transition",
                    e.getMessage().contains("Cannot transition from ACTIVE_IN_SYNC to STANDBY") ||
                            e.getCause() != null && e.getCause().getMessage().contains("Cannot transition from ACTIVE_IN_SYNC to STANDBY"));
        }
    }

    @Test
    public void testSetHAGroupStatusIfNeededWithUnhealthyClient() throws Exception {
        String haGroupName = testName.getMethodName();

        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName, zkUrl);

        // Make client unhealthy by accessing private field
        Field isHealthyField = HAGroupStoreClient.class.getDeclaredField("isHealthy");
        isHealthyField.setAccessible(true);
        isHealthyField.set(haGroupStoreClient, false);

        // Try to set status on unhealthy client
        try {
            haGroupStoreClient.setHAGroupStatusIfNeeded(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);
            fail("Expected IOException for unhealthy client");
        } catch (IOException e) {
            assertTrue("Exception should mention unhealthy client", e.getMessage().contains("not healthy"));
        }
    }


    @Test
    public void testSetHAGroupStatusIfNeededMultipleTransitions() throws Exception {
        String haGroupName = testName.getMethodName();

        // Create initial record with old timestamp
        HAGroupStoreRecord initialRecord = new HAGroupStoreRecord("v1.0", haGroupName,
                HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, initialRecord);

        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName, zkUrl);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // First transition: ACTIVE -> ACTIVE_TO_STANDBY
        haGroupStoreClient.setHAGroupStatusIfNeeded(HAGroupStoreRecord.HAGroupState.ACTIVE_TO_STANDBY);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        HAGroupStoreRecord afterFirst = haGroupStoreClient.getHAGroupStoreRecord();
        assertEquals(HAGroupStoreRecord.HAGroupState.ACTIVE_TO_STANDBY, afterFirst.getHAGroupState());

        // Wait and make another transition: ACTIVE_TO_STANDBY -> STANDBY
        Thread.sleep(100); // Small delay to ensure timestamp difference
        haGroupStoreClient.setHAGroupStatusIfNeeded(HAGroupStoreRecord.HAGroupState.STANDBY);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        HAGroupStoreRecord afterSecond = haGroupStoreClient.getHAGroupStoreRecord();
        assertEquals(HAGroupStoreRecord.HAGroupState.STANDBY, afterSecond.getHAGroupState());
    }

    /**
     * This test verifies that the updates coming via PathChildrenCacheListener are in order in which updates are sent to ZK
     * @throws Exception
     */
    @Test
    public void testHAGroupStoreClientWithMultiThreadedUpdates() throws Exception {
        String haGroupName = testName.getMethodName();

        // Number of threads to execute
        int threadCount = 5;

        // Capture versions of records in a list(recordEventVersions) in order they are received.
        List<Integer> recordEventVersions = new ArrayList<>();
        CountDownLatch eventsLatch = new CountDownLatch(threadCount);
        PathChildrenCacheListener pathChildrenCacheListener = (client, event) -> {
            if(event.getData() != null && event.getData().getData() != null && HAGroupStoreRecord.fromJson(event.getData().getData()).isPresent()) {
                HAGroupStoreRecord record = HAGroupStoreRecord.fromJson(event.getData().getData()).get();
                if (record.getHaGroupName().equals(haGroupName)) {
                    recordEventVersions.add(event.getData().getStat().getVersion());
                    eventsLatch.countDown();
                }
            }
        };

        // Start a new HAGroupStoreClient with custom listener.
        new HAGroupStoreClient(CLUSTERS.getHBaseCluster1().getConfiguration(),
                pathChildrenCacheListener, null, haGroupName, zkUrl);

        // Create multiple threads for update to ZK.
        final CountDownLatch updateLatch = new CountDownLatch(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        // List captures the order of events that are sent.
        List<Integer> updateList = new ArrayList<>();

        // Create a queue which can be polled to send updates to ZK.
        ConcurrentLinkedQueue<HAGroupStoreRecord> updateQueue = new ConcurrentLinkedQueue<>();
        for(int i = 0; i < threadCount; i++) {
            updateQueue.add(createHAGroupStoreRecord(haGroupName));
            updateList.add(i);
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

    @Test
    public void testGetClusterRoleRecordNormalCase() throws Exception {
        String haGroupName = testName.getMethodName();
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName, this.zkUrl, this.peerZKUrl,
                ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY, null);

        // Create HAGroupStoreRecord for local cluster
        HAGroupStoreRecord localRecord = new HAGroupStoreRecord("v1.0", haGroupName,
                HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, localRecord);

        // Create HAGroupStoreRecord for peer cluster
        HAGroupStoreRecord peerRecord = new HAGroupStoreRecord("v1.0", haGroupName,
                HAGroupStoreRecord.HAGroupState.STANDBY);
        createOrUpdateHAGroupStoreRecordOnZookeeper(peerHaAdmin, haGroupName, peerRecord);

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName, zkUrl);
        assertNotNull(haGroupStoreClient);

        // Test getClusterRoleRecord
        ClusterRoleRecord clusterRoleRecord = haGroupStoreClient.getClusterRoleRecord();
        assertNotNull(clusterRoleRecord);
        ClusterRoleRecord expectedClusterRoleRecord = new ClusterRoleRecord(haGroupName,
                HighAvailabilityPolicy.FAILOVER, zkUrl, ClusterRoleRecord.ClusterRole.ACTIVE, peerZKUrl,
                ClusterRoleRecord.ClusterRole.STANDBY, 1);
        assertEquals(expectedClusterRoleRecord, clusterRoleRecord);
    }


    @Test
    public void testGetClusterRoleRecordWithValidPeerZKUrlButNoPeerRecord() throws Exception {
        String haGroupName = testName.getMethodName();
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName, this.zkUrl, this.peerZKUrl,
                ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY, null);

        // Create HAGroupStoreRecord for local cluster only (no peer record)
        HAGroupStoreRecord localRecord = new HAGroupStoreRecord("v1.0", haGroupName,
                HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);
        createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, localRecord);

        // Explicitly ensure no peer record exists
        String peerPath = toPath(haGroupName);
        if (peerHaAdmin.getCurator().checkExists().forPath(peerPath) != null) {
            peerHaAdmin.getCurator().delete().quietly().forPath(peerPath);
        }

        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient.getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName, zkUrl);
        assertNotNull(haGroupStoreClient);

        // Test getClusterRoleRecord when peer ZK URL is valid but no peer HAGroupStoreRecord exists in peer ZK
        ClusterRoleRecord clusterRoleRecord = haGroupStoreClient.getClusterRoleRecord();
        assertNotNull(clusterRoleRecord);
        ClusterRoleRecord expectedClusterRoleRecord = new ClusterRoleRecord(haGroupName,
                HighAvailabilityPolicy.FAILOVER, zkUrl, ClusterRoleRecord.ClusterRole.ACTIVE, this.peerZKUrl,
                ClusterRoleRecord.ClusterRole.UNKNOWN, 1);
        assertEquals(expectedClusterRoleRecord, clusterRoleRecord);
    }

    private HAGroupStoreRecord createHAGroupStoreRecord(String haGroupName) {
        return new HAGroupStoreRecord("v1.0", haGroupName,
                HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);
    }

    // Tests for getHAGroupNames static method
    @Test
    public void testGetHAGroupNamesWithSingleGroup() throws Exception {
        String haGroupName = testName.getMethodName();
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName, this.zkUrl, this.peerZKUrl,
                ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY, null);

        List<String> haGroupNames = HAGroupStoreClient.getHAGroupNames(zkUrl);

        assertNotNull("HA group names list should not be null", haGroupNames);
        assertTrue("HA group names list should contain the test group", haGroupNames.contains(haGroupName));

        // Clean up
        HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(haGroupName, zkUrl);
    }

    @Test
    public void testGetHAGroupNamesWithMultipleGroups() throws Exception {
        // Delete old HA entry created by Before method
        HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(testName.getMethodName(), this.zkUrl);
        // Verify that there are no groups returned
        assertEquals(0, HAGroupStoreClient.getHAGroupNames(zkUrl).size());


        String haGroupName1 = testName.getMethodName() + "_1";
        String haGroupName2 = testName.getMethodName() + "_2";
        String haGroupName3 = testName.getMethodName() + "_3";
        String haGroupName4 = testName.getMethodName() + "_4";

        // Insert multiple HA group records
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName1, this.zkUrl, this.peerZKUrl,
                ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY, null);
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName2, this.zkUrl, this.peerZKUrl,
                ClusterRoleRecord.ClusterRole.STANDBY, ClusterRoleRecord.ClusterRole.ACTIVE, null);
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName3, this.zkUrl, this.peerZKUrl,
                ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY, null);
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName4, "bad_zk_url", this.peerZKUrl,
                ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY, this.zkUrl);

        List<String> haGroupNames = HAGroupStoreClient.getHAGroupNames(zkUrl);

        assertNotNull("HA group names list should not be null", haGroupNames);
        assertTrue("HA group names list should contain haGroupName1", haGroupNames.contains(haGroupName1));
        assertTrue("HA group names list should contain haGroupName2", haGroupNames.contains(haGroupName2));
        assertTrue("HA group names list should contain haGroupName3", haGroupNames.contains(haGroupName3));
        assertTrue("HA group names list should not contain haGroupName4", !haGroupNames.contains(haGroupName4));
        assertEquals(3, haGroupNames.size());


        // Clean up
        HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(haGroupName1, zkUrl);
        HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(haGroupName2, zkUrl);
        HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(haGroupName3, zkUrl);
    }

    @Test
    public void testGetHAGroupNamesWithEmptyTable() throws Exception {
        // First, delete any existing records that might interfere
        List<String> existingGroups = HAGroupStoreClient.getHAGroupNames(zkUrl);
        for (String groupName : existingGroups) {
            if (groupName.startsWith("test")) { // Only clean up test groups
                HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(groupName, zkUrl);
            }
        }

        // Verify the table is empty of test records
        List<String> haGroupNames = HAGroupStoreClient.getHAGroupNames(zkUrl);
        long testGroupCount = haGroupNames.stream()
                .filter(name -> name.startsWith("test"))
                .count();
        assertEquals("Should have no test groups", 0, testGroupCount);
    }

    @Test
    public void testGetHAGroupNamesAfterDeletingGroups() throws Exception {
        String haGroupName1 = testName.getMethodName() + "_delete_1";
        String haGroupName2 = testName.getMethodName() + "_delete_2";

        // Insert HA group records
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName1, this.zkUrl, this.peerZKUrl,
                ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY, null);
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName2, this.zkUrl, this.peerZKUrl,
                ClusterRoleRecord.ClusterRole.STANDBY, ClusterRoleRecord.ClusterRole.ACTIVE, null);

        // Verify both groups exist
        List<String> haGroupNames = HAGroupStoreClient.getHAGroupNames(zkUrl);
        assertTrue("Should contain haGroupName1", haGroupNames.contains(haGroupName1));
        assertTrue("Should contain haGroupName2", haGroupNames.contains(haGroupName2));

        // Delete one group
        HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(haGroupName1, zkUrl);

        // Verify only one group remains
        haGroupNames = HAGroupStoreClient.getHAGroupNames(zkUrl);
        assertFalse("Should not contain deleted haGroupName1", haGroupNames.contains(haGroupName1));
        assertTrue("Should still contain haGroupName2", haGroupNames.contains(haGroupName2));

        // Clean up remaining group
        HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(haGroupName2, zkUrl);

        // Verify the group is gone
        haGroupNames = HAGroupStoreClient.getHAGroupNames(zkUrl);
        assertFalse("Should not contain deleted haGroupName2", haGroupNames.contains(haGroupName2));
    }

    @Test
    public void testGetHAGroupNamesWithInvalidZkUrl() throws Exception {
        String invalidZkUrl = "invalid:2181";

        try {
            HAGroupStoreClient.getHAGroupNames(invalidZkUrl);
            fail("Expected SQLException for invalid ZK URL");
        } catch (SQLException e) {
            // Expected - the connection should fail with invalid ZK URL
            assertNotNull("SQLException should have a message", e.getMessage());
        }
    }

    @Test
    public void testGetHAGroupNamesWithNullZkUrl() throws Exception {
        try {
            HAGroupStoreClient.getHAGroupNames(null);
            fail("Expected SQLException for null ZK URL");
        } catch (SQLException e) {
            // Expected - the connection should fail with null ZK URL
            assertNotNull("SQLException should have a message", e.getMessage());
        }
    }


}