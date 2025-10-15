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


import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.phoenix.jdbc.HAGroupStoreClient.ZK_CONSISTENT_HA_GROUP_RECORD_NAMESPACE;
import static org.apache.phoenix.jdbc.HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC;
import static org.apache.phoenix.jdbc.PhoenixHAAdmin.getLocalZkUrl;
import static org.apache.phoenix.jdbc.PhoenixHAAdmin.toPath;
import static org.apache.phoenix.query.QueryServices.CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Integration tests for HA Group State Change subscription functionality.
 * Tests the new subscription system where HAGroupStoreClient directly manages
 * subscriptions and HAGroupStoreManager acts as a passthrough.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class HAGroupStateSubscriptionIT extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(HAGroupStateSubscriptionIT.class);

    @Rule
    public TestName testName = new TestName();

    private PhoenixHAAdmin haAdmin;
    private PhoenixHAAdmin peerHaAdmin;
    private static final Long ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS = 2000L;
    private String zkUrl;
    private String peerZKUrl;
    private static final HighAvailabilityTestingUtility.HBaseTestingUtilityPair CLUSTERS = new HighAvailabilityTestingUtility.HBaseTestingUtilityPair();

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(2);
        props.put(CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED, "true");
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
        CLUSTERS.start();
    }

    @Before
    public void before() throws Exception {
        haAdmin = new PhoenixHAAdmin(config, ZK_CONSISTENT_HA_GROUP_RECORD_NAMESPACE);
        zkUrl = getLocalZkUrl(config);
        this.peerZKUrl = CLUSTERS.getZkUrl2();
        peerHaAdmin = new PhoenixHAAdmin(peerZKUrl, config, ZK_CONSISTENT_HA_GROUP_RECORD_NAMESPACE);

        // Clean up existing HAGroupStoreRecords
        try {
            List<String> haGroupNames = HAGroupStoreClient.getHAGroupNames(zkUrl);
            for (String haGroupName : haGroupNames) {
                haAdmin.getCurator().delete().quietly().forPath(toPath(haGroupName));
                peerHaAdmin.getCurator().delete().quietly().forPath(toPath(haGroupName));
            }

        } catch (Exception e) {
            // Ignore cleanup errors
        }
        // Remove any existing entries in the system table
        HAGroupStoreTestUtil.deleteAllHAGroupRecordsInSystemTable(zkUrl);

        // Insert a HAGroupStoreRecord into the system table
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(testName.getMethodName(), zkUrl, peerZKUrl,
                ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY, null);
    }

    // ========== Multi-Cluster & Basic Subscription Tests ==========

    @Test
    public void testDifferentTargetStatesPerCluster() throws Exception {
        String haGroupName = testName.getMethodName();
        HAGroupStoreManager manager = HAGroupStoreManager.getInstance(config);

        // Track notifications
        AtomicInteger localNotifications = new AtomicInteger(0);
        AtomicInteger peerNotifications = new AtomicInteger(0);
        AtomicReference<ClusterType> lastLocalClusterType = new AtomicReference<>();
        AtomicReference<ClusterType> lastPeerClusterType = new AtomicReference<>();

        // Create listeners for different target states
        HAGroupStateListener localListener = (groupName, fromState, toState, modifiedTime, clusterType, lastSyncStateTimeInMs) -> {
            LOGGER.info("Local target state listener called: {} -> {} on {}", fromState, toState, clusterType);
            if (fromState == ACTIVE_NOT_IN_SYNC && toState == HAGroupState.STANDBY_TO_ACTIVE && clusterType == ClusterType.LOCAL) {
                localNotifications.incrementAndGet();
                lastLocalClusterType.set(clusterType);
                LOGGER.info("Local target state listener called: {} -> {} on {}", fromState, toState, clusterType);
            }
        };

        HAGroupStateListener peerListener = (groupName, fromState, toState, modifiedTime, clusterType, lastSyncStateTimeInMs) -> {
            LOGGER.info("Peer target state listener called: {} -> {} on {}", fromState, toState, clusterType);
            if (fromState == null && toState == ACTIVE_NOT_IN_SYNC && clusterType == ClusterType.PEER) {
                peerNotifications.incrementAndGet();
                lastPeerClusterType.set(clusterType);
                LOGGER.info("Peer target state listener called: {} -> {} on {}", fromState, toState, clusterType);
            }
        };

        // Subscribe to different target states on different clusters
        manager.subscribeToTargetState(haGroupName, HAGroupState.STANDBY_TO_ACTIVE, ClusterType.LOCAL, localListener);
        manager.subscribeToTargetState(haGroupName, ACTIVE_NOT_IN_SYNC, ClusterType.PEER, peerListener);

        // Trigger transition to STANDBY_TO_ACTIVE on LOCAL cluster
        HAGroupStoreRecord localRecord = new HAGroupStoreRecord("1.0", haGroupName, HAGroupState.STANDBY_TO_ACTIVE, null, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.zkUrl, this.peerZKUrl, 0L);
        haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, localRecord, 0);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Trigger transition to STANDBY on PEER cluster
        HAGroupStoreRecord peerRecord = new HAGroupStoreRecord("1.0", haGroupName, ACTIVE_NOT_IN_SYNC, null, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.zkUrl, this.peerZKUrl, 0L);
        peerHaAdmin.createHAGroupStoreRecordInZooKeeper(peerRecord);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Verify no cross-cluster triggering
        assertEquals("Local cluster should receive its target state notification", 1, localNotifications.get());
        assertEquals("Peer cluster should receive its target state notification", 1, peerNotifications.get());
        assertEquals("Local notification should have LOCAL cluster type", ClusterType.LOCAL, lastLocalClusterType.get());
        assertEquals("Peer notification should have PEER cluster type", ClusterType.PEER, lastPeerClusterType.get());

    }

    @Test
    public void testUnsubscribeSpecificCluster() throws Exception {
        String haGroupName = testName.getMethodName();
        HAGroupStoreManager manager = HAGroupStoreManager.getInstance(config);

        // Track notifications
        AtomicInteger totalNotifications = new AtomicInteger(0);
        AtomicReference<ClusterType> lastClusterType = new AtomicReference<>();

        AtomicReference<HAGroupState> lastFromState = new AtomicReference<>();

        HAGroupStateListener listener = (groupName, fromState, toState, modifiedTime, clusterType, lastSyncStateTimeInMs) -> {
            // Check for specific transition: ACTIVE -> STANDBY on PEER cluster
            if (fromState == HAGroupState.ACTIVE_IN_SYNC && toState == HAGroupState.STANDBY && clusterType == ClusterType.PEER) {
                totalNotifications.incrementAndGet();
                lastClusterType.set(clusterType);
                lastFromState.set(fromState);
                LOGGER.info("Listener called for specific transition: {} -> {} on {}", fromState, toState, clusterType);
            }
        };

        // Subscribe to STANDBY target state on both clusters
        manager.subscribeToTargetState(haGroupName, HAGroupState.STANDBY, ClusterType.LOCAL, listener);
        manager.subscribeToTargetState(haGroupName, HAGroupState.STANDBY, ClusterType.PEER, listener);

        // Unsubscribe from LOCAL cluster only
        manager.unsubscribeFromTargetState(haGroupName, HAGroupState.STANDBY, ClusterType.LOCAL, listener);

        // First, establish ACTIVE_IN_SYNC state on PEER cluster
        HAGroupStoreRecord peerActiveRecord = new HAGroupStoreRecord("1.0", haGroupName, HAGroupState.ACTIVE_IN_SYNC, null, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.zkUrl, this.peerZKUrl, 0L);
        peerHaAdmin.createHAGroupStoreRecordInZooKeeper(peerActiveRecord);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Should receive no notifications yet (we're looking for ACTIVE_IN_SYNC -> STANDBY transition)
        assertEquals("Should receive no notifications for ACTIVE_IN_SYNC state", 0, totalNotifications.get());

        // Now trigger transition from ACTIVE_IN_SYNC to STANDBY on PEER → should call listener
        HAGroupStoreRecord peerStandbyRecord = new HAGroupStoreRecord("1.0", haGroupName, HAGroupState.STANDBY, null, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.zkUrl, this.peerZKUrl, 0L);
        peerHaAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, peerStandbyRecord, 0);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Verify the specific transition was detected
        assertEquals("Should receive notification for ACTIVE_IN_SYNC -> STANDBY transition", 1, totalNotifications.get());
        assertEquals("Notification should be from PEER cluster", ClusterType.PEER, lastClusterType.get());
        assertEquals("FromState should be ACTIVE_IN_SYNC", HAGroupState.ACTIVE_IN_SYNC, lastFromState.get());

    }

    // ========== Multiple Listeners Tests ==========

    @Test
    public void testMultipleListenersMultipleClusters() throws Exception {
        String haGroupName = testName.getMethodName();
        HAGroupStoreManager manager = HAGroupStoreManager.getInstance(config);

        // Track notifications from multiple listeners
        AtomicInteger listener1LocalNotifications = new AtomicInteger(0);
        AtomicInteger listener2LocalNotifications = new AtomicInteger(0);
        AtomicInteger listener1PeerNotifications = new AtomicInteger(0);
        AtomicInteger listener2PeerNotifications = new AtomicInteger(0);

        HAGroupStateListener listener1 = (groupName, fromState, toState, modifiedTime, clusterType, lastSyncStateTimeInMs) -> {
            if (toState == HAGroupState.DEGRADED_STANDBY) {
                if (clusterType == ClusterType.LOCAL) {
                    listener1LocalNotifications.incrementAndGet();
                } else {
                    listener1PeerNotifications.incrementAndGet();
                }
                LOGGER.info("Listener1 called: {} -> {} on {}", fromState, toState, clusterType);
            }
        };

        HAGroupStateListener listener2 = (groupName, fromState, toState, modifiedTime, clusterType, lastSyncStateTimeInMs) -> {
            if (toState == HAGroupState.DEGRADED_STANDBY) {
                if (clusterType == ClusterType.LOCAL) {
                    listener2LocalNotifications.incrementAndGet();
                } else {
                    listener2PeerNotifications.incrementAndGet();
                }
                LOGGER.info("Listener2 called: {} -> {} on {}", fromState, toState, clusterType);
            }
        };

        // Register multiple listeners for same target state on both clusters
        manager.subscribeToTargetState(haGroupName, HAGroupState.DEGRADED_STANDBY, ClusterType.LOCAL, listener1);
        manager.subscribeToTargetState(haGroupName, HAGroupState.DEGRADED_STANDBY, ClusterType.LOCAL, listener2);
        manager.subscribeToTargetState(haGroupName, HAGroupState.DEGRADED_STANDBY, ClusterType.PEER, listener1);
        manager.subscribeToTargetState(haGroupName, HAGroupState.DEGRADED_STANDBY, ClusterType.PEER, listener2);

        // Trigger transition to DEGRADED_STANDBY on LOCAL
        HAGroupStoreRecord localRecord = new HAGroupStoreRecord("1.0", haGroupName, HAGroupState.DEGRADED_STANDBY, null, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.zkUrl, this.peerZKUrl, 0L);
        haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, localRecord, 0);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Trigger transition to DEGRADED_STANDBY on PEER
        HAGroupStoreRecord peerRecord = new HAGroupStoreRecord("1.0", haGroupName, HAGroupState.DEGRADED_STANDBY, null, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.zkUrl, this.peerZKUrl, 0L);
        peerHaAdmin.createHAGroupStoreRecordInZooKeeper(peerRecord);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Verify all listeners called for each cluster
        assertEquals("Listener1 should receive LOCAL notification", 1, listener1LocalNotifications.get());
        assertEquals("Listener2 should receive LOCAL notification", 1, listener2LocalNotifications.get());
        assertEquals("Listener1 should receive PEER notification", 1, listener1PeerNotifications.get());
        assertEquals("Listener2 should receive PEER notification", 1, listener2PeerNotifications.get());

    }

    @Test
    public void testSameListenerDifferentTargetStates() throws Exception {
        String haGroupName = testName.getMethodName();
        HAGroupStoreManager manager = HAGroupStoreManager.getInstance(config);

        // Track which target states were reached
        AtomicInteger stateANotifications = new AtomicInteger(0);
        AtomicInteger stateBNotifications = new AtomicInteger(0);
        AtomicReference<ClusterType> lastStateAClusterType = new AtomicReference<>();
        AtomicReference<ClusterType> lastStateBClusterType = new AtomicReference<>();

        HAGroupStateListener sharedListener = (groupName, fromState, toState, modifiedTime, clusterType, lastSyncStateTimeInMs) -> {
            if (fromState == ACTIVE_NOT_IN_SYNC && toState == HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY && clusterType == ClusterType.LOCAL) {
                stateANotifications.incrementAndGet();
                lastStateAClusterType.set(clusterType);
                LOGGER.info("Shared listener - Target State A: {} -> {} on {}", fromState, toState, clusterType);
            } else if (fromState == null && toState == HAGroupState.ACTIVE_IN_SYNC && clusterType == ClusterType.PEER) {
                stateBNotifications.incrementAndGet();
                lastStateBClusterType.set(clusterType);
                LOGGER.info("Shared listener - Target State B: {} -> {} on {}", fromState, toState, clusterType);
            }
        };

        // Register same listener for different target states on different clusters
        manager.subscribeToTargetState(haGroupName, HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY, ClusterType.LOCAL, sharedListener);
        manager.subscribeToTargetState(haGroupName, HAGroupState.ACTIVE_IN_SYNC, ClusterType.PEER, sharedListener);

        // Trigger target state A on LOCAL
        HAGroupStoreRecord localRecord = new HAGroupStoreRecord("1.0", haGroupName, HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY, null, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.zkUrl, this.peerZKUrl, 0L);
        haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, localRecord, 0);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Trigger target state B on PEER
        HAGroupStoreRecord peerRecord = new HAGroupStoreRecord("1.0", haGroupName, HAGroupState.ACTIVE_IN_SYNC, null, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.zkUrl, this.peerZKUrl, 0L);
        peerHaAdmin.createHAGroupStoreRecordInZooKeeper(peerRecord);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Verify listener called for each appropriate target state/cluster combination
        assertEquals("Should receive target state A notification", 1, stateANotifications.get());
        assertEquals("Should receive target state B notification", 1, stateBNotifications.get());
        assertEquals("Target state A should be from LOCAL cluster", ClusterType.LOCAL, lastStateAClusterType.get());
        assertEquals("Target state B should be from PEER cluster", ClusterType.PEER, lastStateBClusterType.get());
    }

    // ========== Edge Cases & Error Handling ==========

    @Test
    public void testSubscriptionToNonExistentHAGroup() throws Exception {
        String nonExistentHAGroup = "nonExistentGroup_" + testName.getMethodName();
        HAGroupStoreManager manager = HAGroupStoreManager.getInstance(config);

        HAGroupStateListener listener = (groupName, fromState, toState, modifiedTime, clusterType, lastSyncStateTimeInMs) -> {
            // Should not be called
        };

        // Try to subscribe to non-existent HA group
        try {
            manager.subscribeToTargetState(nonExistentHAGroup, HAGroupState.STANDBY, ClusterType.LOCAL, listener);
            fail("Expected IOException for non-existent HA group");
        } catch (IOException e) {
            assertTrue("Exception should mention the HA group name", e.getMessage().contains(nonExistentHAGroup));
            LOGGER.info("Correctly caught exception for non-existent HA group: {}", e.getMessage());
        }
    }

    @Test
    public void testListenerExceptionIsolation() throws Exception {
        String haGroupName = testName.getMethodName();
        HAGroupStoreManager manager = HAGroupStoreManager.getInstance(config);

        // Track notifications
        AtomicInteger goodListener1Notifications = new AtomicInteger(0);
        AtomicInteger goodListener2Notifications = new AtomicInteger(0);
        AtomicInteger badListenerCalls = new AtomicInteger(0);

        HAGroupStateListener goodListener1 = (groupName, fromState, toState, modifiedTime, clusterType, lastSyncStateTimeInMs) -> {
            if (fromState == ACTIVE_NOT_IN_SYNC && toState == HAGroupState.ACTIVE_IN_SYNC) {
                goodListener1Notifications.incrementAndGet();
                LOGGER.info("Good listener 1 called: {} -> {} on {}", fromState, toState, clusterType);
            }
        };

        HAGroupStateListener badListener = (groupName, fromState, toState, modifiedTime, clusterType, lastSyncStateTimeInMs) -> {
            if (fromState == ACTIVE_NOT_IN_SYNC && toState == HAGroupState.ACTIVE_IN_SYNC) {
                badListenerCalls.incrementAndGet();
                LOGGER.info("Bad listener called: {} -> {}, about to throw exception", fromState, toState);
                throw new RuntimeException("Test exception from bad listener");
            }
        };

        HAGroupStateListener goodListener2 = (groupName, fromState, toState, modifiedTime, clusterType, lastSyncStateTimeInMs) -> {
            if (fromState == ACTIVE_NOT_IN_SYNC && toState == HAGroupState.ACTIVE_IN_SYNC) {
                goodListener2Notifications.incrementAndGet();
                LOGGER.info("Good listener 2 called: {} -> {} on {}", fromState, toState, clusterType);
            }
        };

        // Register listeners - bad listener in the middle
        manager.subscribeToTargetState(haGroupName, HAGroupState.ACTIVE_IN_SYNC, ClusterType.LOCAL, goodListener1);
        manager.subscribeToTargetState(haGroupName, HAGroupState.ACTIVE_IN_SYNC, ClusterType.LOCAL, badListener);
        manager.subscribeToTargetState(haGroupName, HAGroupState.ACTIVE_IN_SYNC, ClusterType.LOCAL, goodListener2);

        // Trigger transition to target state
        HAGroupStoreRecord transitionRecord = new HAGroupStoreRecord("1.0", haGroupName, HAGroupState.ACTIVE_IN_SYNC, null, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.zkUrl, this.peerZKUrl, 0L);
        haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, transitionRecord, 0);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Verify all listeners were called despite exception in bad listener
        assertEquals("Good listener 1 should be called", 1, goodListener1Notifications.get());
        assertEquals("Good listener 2 should be called", 1, goodListener2Notifications.get());
        assertEquals("Bad listener should be called", 1, badListenerCalls.get());
    }

    // ========== Performance & Concurrency Tests ==========

    @Test
    public void testConcurrentMultiClusterSubscriptions() throws Exception {
        String haGroupName = testName.getMethodName();
        HAGroupStoreManager manager = HAGroupStoreManager.getInstance(config);

        final int threadCount = 10;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch completionLatch = new CountDownLatch(threadCount);
        final AtomicInteger successfulSubscriptions = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        // Create concurrent subscription tasks
        for (int i = 0; i < threadCount; i++) {
            final int threadIndex = i;
            executor.submit(() -> {
                try {
                    startLatch.await(); // Wait for all threads to be ready

                    HAGroupStateListener listener = (groupName, fromState, toState, modifiedTime, clusterType, lastSyncStateTimeInMs) -> {
                        LOGGER.debug("Thread {} listener called: {} -> {} on {}", threadIndex, fromState, toState, clusterType);
                    };

                    // Half subscribe to LOCAL, half to PEER
                    ClusterType clusterType = (threadIndex % 2 == 0) ? ClusterType.LOCAL : ClusterType.PEER;

                    manager.subscribeToTargetState(haGroupName, HAGroupState.ACTIVE_IN_SYNC, clusterType, listener);
                    successfulSubscriptions.incrementAndGet();

                    // Also test unsubscribe
                    manager.unsubscribeFromTargetState(haGroupName, HAGroupState.ACTIVE_IN_SYNC, clusterType, listener);

                } catch (Exception e) {
                    LOGGER.error("Thread {} failed", threadIndex, e);
                } finally {
                    completionLatch.countDown();
                }
            });
        }

        // Start all threads simultaneously
        startLatch.countDown();

        // Wait for completion
        assertTrue("All threads should complete within timeout", completionLatch.await(30, TimeUnit.SECONDS));
        assertEquals("All threads should successfully subscribe", threadCount, successfulSubscriptions.get());

        executor.shutdown();
    }

    @Test
    public void testHighFrequencyMultiClusterChanges() throws Exception {
        String haGroupName = testName.getMethodName();
        HAGroupStoreManager manager = HAGroupStoreManager.getInstance(config);

        // Track notifications
        AtomicInteger localNotifications = new AtomicInteger(0);
        AtomicInteger peerNotifications = new AtomicInteger(0);

        HAGroupStateListener listener = (groupName, fromState, toState, modifiedTime, clusterType, lastSyncStateTimeInMs) -> {
            if (clusterType == ClusterType.LOCAL) {
                localNotifications.incrementAndGet();
            } else {
                peerNotifications.incrementAndGet();
            }
            LOGGER.debug("High frequency listener: {} -> {} on {}", fromState, toState, clusterType);
        };

        // Subscribe to target state on both clusters
        manager.subscribeToTargetState(haGroupName, HAGroupState.STANDBY, ClusterType.LOCAL, listener);
        manager.subscribeToTargetState(haGroupName, HAGroupState.STANDBY, ClusterType.PEER, listener);

        // Rapidly alternate state changes on both clusters
        final int changeCount = 5;
        HAGroupStoreRecord initialPeerRecord = new HAGroupStoreRecord("1.0", haGroupName,HAGroupState.DEGRADED_STANDBY, null, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.zkUrl, this.peerZKUrl, 0L);
        peerHaAdmin.createHAGroupStoreRecordInZooKeeper(initialPeerRecord);

        for (int i = 0; i < changeCount; i++) {
            // Change local cluster
            HAGroupStoreRecord localRecord = new HAGroupStoreRecord("1.0", haGroupName,
                    (i % 2 == 0) ? HAGroupState.STANDBY : HAGroupState.DEGRADED_STANDBY, null, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.zkUrl, this.peerZKUrl, 0L);
            haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, localRecord, -1);

            // Change peer cluster
            HAGroupStoreRecord peerRecord = new HAGroupStoreRecord("1.0", haGroupName,
                    (i % 2 == 0) ? HAGroupState.STANDBY : HAGroupState.STANDBY_TO_ACTIVE, null, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.zkUrl, this.peerZKUrl, 0L);
            peerHaAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, peerRecord, -1);

            // Small delay between changes
            Thread.sleep(500);
        }

        // Final wait for all events to propagate
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Verify transitions detected on both clusters
        // Expected: 3 transitions to STANDBY state (i=0,2,4 → STANDBY)
        assertEquals("Should detect exactly 3 local cluster transitions to STANDBY", 3, localNotifications.get());
        assertEquals("Should detect exactly 3 peer cluster transitions to STANDBY", 3, peerNotifications.get());

        LOGGER.info("Detected {} local and {} peer notifications", localNotifications.get(), peerNotifications.get());

    }

    // ========== Cleanup & Resource Management Tests ==========

    @Test
    public void testSubscriptionCleanupPerCluster() throws Exception {
        String haGroupName = testName.getMethodName();
        HAGroupStoreManager manager = HAGroupStoreManager.getInstance(config);

        // Track notifications to verify functionality
        AtomicInteger localActiveNotifications = new AtomicInteger(0);
        AtomicInteger peerActiveNotifications = new AtomicInteger(0);
        AtomicInteger localStandbyNotifications = new AtomicInteger(0);
        AtomicInteger peerStandbyNotifications = new AtomicInteger(0);

        // Create listeners that track which ones are called
        HAGroupStateListener listener1 = (groupName, fromState, toState, modifiedTime, clusterType, lastSyncStateTimeInMs) -> {
            if (toState == HAGroupState.ACTIVE_IN_SYNC && clusterType == ClusterType.LOCAL) {
                localActiveNotifications.incrementAndGet();
                LOGGER.info("Listener1 LOCAL ACTIVE_IN_SYNC: {} -> {}", fromState, toState);
            }
        };

        HAGroupStateListener listener2 = (groupName, fromState, toState, modifiedTime, clusterType, lastSyncStateTimeInMs) -> {
            if (toState == HAGroupState.ACTIVE_IN_SYNC && clusterType == ClusterType.LOCAL) {
                localActiveNotifications.incrementAndGet();
                LOGGER.info("Listener2 LOCAL ACTIVE_IN_SYNC: {} -> {}", fromState, toState);
            } else if (toState == HAGroupState.STANDBY_TO_ACTIVE && clusterType == ClusterType.PEER) {
                peerActiveNotifications.incrementAndGet();
                LOGGER.info("Listener2 PEER STANDBY_TO_ACTIVE: {} -> {}", fromState, toState);
            }
        };

        HAGroupStateListener listener3 = (groupName, fromState, toState, modifiedTime, clusterType, lastSyncStateTimeInMs) -> {
            if (toState == HAGroupState.STANDBY_TO_ACTIVE && clusterType == ClusterType.PEER) {
                peerActiveNotifications.incrementAndGet();
                LOGGER.info("Listener3 PEER STANDBY_TO_ACTIVE: {} -> {}", fromState, toState);
            }
        };

        HAGroupStateListener listener4 = (groupName, fromState, toState, modifiedTime, clusterType, lastSyncStateTimeInMs) -> {
            if (toState == HAGroupState.ACTIVE_IN_SYNC && clusterType == ClusterType.LOCAL) {
                localStandbyNotifications.incrementAndGet();
                LOGGER.info("Listener4 LOCAL ACTIVE_IN_SYNC: {} -> {}", fromState, toState);
            } else if (toState == HAGroupState.STANDBY_TO_ACTIVE && clusterType == ClusterType.PEER) {
                peerStandbyNotifications.incrementAndGet();
                LOGGER.info("Listener4 PEER STANDBY_TO_ACTIVE: {} -> {}", fromState, toState);
            }
        };

        // Subscribe listeners to both clusters for target states
        manager.subscribeToTargetState(haGroupName, HAGroupState.ACTIVE_IN_SYNC, ClusterType.LOCAL, listener1);
        manager.subscribeToTargetState(haGroupName, HAGroupState.ACTIVE_IN_SYNC, ClusterType.LOCAL, listener2);
        manager.subscribeToTargetState(haGroupName, HAGroupState.STANDBY_TO_ACTIVE, ClusterType.PEER, listener2);
        manager.subscribeToTargetState(haGroupName, HAGroupState.STANDBY_TO_ACTIVE, ClusterType.PEER, listener3);
        manager.subscribeToTargetState(haGroupName, HAGroupState.ACTIVE_IN_SYNC, ClusterType.LOCAL, listener4);
        manager.subscribeToTargetState(haGroupName, HAGroupState.STANDBY_TO_ACTIVE, ClusterType.PEER, listener4);

        // Test initial functionality - trigger ACTIVE_IN_SYNC on LOCAL
        HAGroupStoreRecord localActiveRecord = new HAGroupStoreRecord("1.0", haGroupName, HAGroupState.ACTIVE_IN_SYNC, null, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.zkUrl, this.peerZKUrl, 0L);
        haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, localActiveRecord, 0);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Should have 2 notifications for LOCAL ACTIVE_NOT_IN_SYNC (listener1 + listener2)
        assertEquals("Should have 2 LOCAL ACTIVE_IN_SYNC notifications initially", 2, localActiveNotifications.get());

        // Test initial functionality - trigger STANDBY_TO_ACTIVE on PEER
        HAGroupStoreRecord peerActiveRecord = new HAGroupStoreRecord("1.0", haGroupName, HAGroupState.STANDBY_TO_ACTIVE, null, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.zkUrl, this.peerZKUrl, 0L);
        peerHaAdmin.createHAGroupStoreRecordInZooKeeper(peerActiveRecord);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Should have 2 notifications for PEER STANDBY_TO_ACTIVE (listener2 + listener3)
        assertEquals("Should have 2 PEER STANDBY_TO_ACTIVE notifications initially", 2, peerActiveNotifications.get());

        // Reset counters for cleanup testing
        localActiveNotifications.set(0);
        peerActiveNotifications.set(0);

        // Unsubscribe selectively
        manager.unsubscribeFromTargetState(haGroupName, HAGroupState.ACTIVE_IN_SYNC, ClusterType.LOCAL, listener1);
        manager.unsubscribeFromTargetState(haGroupName, HAGroupState.STANDBY_TO_ACTIVE, ClusterType.PEER, listener2);
        manager.unsubscribeFromTargetState(haGroupName, HAGroupState.ACTIVE_IN_SYNC, ClusterType.LOCAL, listener4);

        // Test after partial unsubscribe - trigger ACTIVE_IN_SYNC on LOCAL again by first changing to some other state.
        HAGroupStoreRecord localActiveRecord2 = new HAGroupStoreRecord("1.0", haGroupName, ACTIVE_NOT_IN_SYNC, null, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.zkUrl, this.peerZKUrl, 0L);
        haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, localActiveRecord2, 1);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        HAGroupStoreRecord localActiveRecord3 = new HAGroupStoreRecord("1.0", haGroupName, HAGroupState.ACTIVE_IN_SYNC, null, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.zkUrl, this.peerZKUrl, 0L);
        haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, localActiveRecord3, 2);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Should have only 1 notification for LOCAL ACTIVE_IN_SYNC (only listener2 remains)
        assertEquals("Should have 1 LOCAL ACTIVE_IN_SYNC notification after partial unsubscribe", 1, localActiveNotifications.get());

        // Test after partial unsubscribe - trigger STANDBY_TO_ACTIVE on PEER again
        HAGroupStoreRecord peerActiveRecord2 = new HAGroupStoreRecord("1.0", haGroupName, HAGroupState.STANDBY, null, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.zkUrl, this.peerZKUrl, 0L);
        peerHaAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, peerActiveRecord2, 0);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        HAGroupStoreRecord peerActiveRecord3 = new HAGroupStoreRecord("1.0", haGroupName, HAGroupState.STANDBY_TO_ACTIVE, null, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.zkUrl, this.peerZKUrl, 0L);
        peerHaAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, peerActiveRecord3, 1);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Should have only 1 notification for PEER STANDBY_TO_ACTIVE (only listener3 remains)
        assertEquals("Should have 1 PEER STANDBY_TO_ACTIVE notification after partial unsubscribe", 1, peerActiveNotifications.get());

        // Reset counters again
        localActiveNotifications.set(0);
        peerActiveNotifications.set(0);

        // Unsubscribe all remaining listeners
        manager.unsubscribeFromTargetState(haGroupName, ACTIVE_NOT_IN_SYNC, ClusterType.LOCAL, listener2);
        manager.unsubscribeFromTargetState(haGroupName, ACTIVE_NOT_IN_SYNC, ClusterType.PEER, listener3);
        manager.unsubscribeFromTargetState(haGroupName, HAGroupState.STANDBY, ClusterType.PEER, listener4);

        // Test after complete unsubscribe - trigger ACTIVE_NOT_IN_SYNC on both clusters
        HAGroupStoreRecord localActiveRecord4 = new HAGroupStoreRecord("1.0", haGroupName, ACTIVE_NOT_IN_SYNC, null, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.zkUrl, this.peerZKUrl, 0L);
        haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, localActiveRecord4, 3);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        HAGroupStoreRecord peerActiveRecord4 = new HAGroupStoreRecord("1.0", haGroupName, ACTIVE_NOT_IN_SYNC, null, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.zkUrl, this.peerZKUrl, 0L);
        peerHaAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, peerActiveRecord4, 2);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Should have no notifications after complete unsubscribe
        assertEquals("Should have 0 LOCAL ACTIVE_NOT_IN_SYNC notifications after complete unsubscribe", 0, localActiveNotifications.get());
        assertEquals("Should have 0 PEER ACTIVE_NOT_IN_SYNC notifications after complete unsubscribe", 0, peerActiveNotifications.get());

        // Test that new subscriptions still work properly
        AtomicInteger newSubscriptionNotifications = new AtomicInteger(0);
        HAGroupStateListener newTestListener = (groupName, fromState, toState, modifiedTime, clusterType, lastSyncStateTimeInMs) -> {
            if (toState == HAGroupState.STANDBY && clusterType == ClusterType.LOCAL) {
                newSubscriptionNotifications.incrementAndGet();
                LOGGER.info("New subscription triggered: {} -> {} on {} at {}", fromState, toState, clusterType, modifiedTime);
            }
        };

        // Subscribe with new test listener
        manager.subscribeToTargetState(haGroupName, HAGroupState.STANDBY, ClusterType.LOCAL, newTestListener);

        // Trigger STANDBY state and verify new subscription works
        HAGroupStoreRecord standbyRecord = new HAGroupStoreRecord("1.0", haGroupName, HAGroupState.STANDBY, null, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.zkUrl, this.peerZKUrl, 0L);
        haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, standbyRecord, 4);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Expected: exactly 1 notification for the new subscription
        assertEquals("New subscription should receive exactly 1 notification", 1, newSubscriptionNotifications.get());

        LOGGER.info("Subscription cleanup test completed successfully with {} notifications from new subscription",
                   newSubscriptionNotifications.get());

    }

    /**
     * Helper method to access private subscription maps via reflection
     */
    @SuppressWarnings("unchecked")
    private ConcurrentHashMap<String, CopyOnWriteArraySet<HAGroupStateListener>> getSubscriptionMap(HAGroupStoreClient client, String fieldName) throws Exception {
        Field field = HAGroupStoreClient.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return (ConcurrentHashMap<String, CopyOnWriteArraySet<HAGroupStateListener>>) field.get(client);
    }
}
