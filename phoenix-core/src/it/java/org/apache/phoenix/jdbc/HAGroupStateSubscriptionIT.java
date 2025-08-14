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

import java.lang.reflect.Field;
import java.sql.SQLException;
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

import static org.apache.phoenix.jdbc.HAGroupStoreClient.ZK_CONSISTENT_HA_NAMESPACE;
import static org.apache.phoenix.jdbc.PhoenixHAAdmin.getLocalZkUrl;
import static org.apache.phoenix.jdbc.PhoenixHAAdmin.toPath;
import static org.apache.phoenix.query.QueryServices.CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
        haAdmin = new PhoenixHAAdmin(config, ZK_CONSISTENT_HA_NAMESPACE);
        zkUrl = getLocalZkUrl(config);
        this.peerZKUrl = CLUSTERS.getZkUrl2();

        // Clean up existing HAGroupStoreRecords
        try {
            List<String> haGroupNames = HAGroupStoreClient.getHAGroupNames(zkUrl);
            for (String haGroupName : haGroupNames) {
                haAdmin.getCurator().delete().quietly().forPath(toPath(haGroupName));
            }
        } catch (Exception e) {
            // Ignore cleanup errors
        }
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(testName.getMethodName(), zkUrl, peerZKUrl,
                ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY, null);
    }

    // ========== Multi-Cluster & Basic Subscription Tests ==========

    @Test
    public void testTargetStateBothClusters() throws Exception {
        String haGroupName = testName.getMethodName();
        HAGroupStoreManager manager = HAGroupStoreManager.getInstance(config);

        // Track notifications received
        AtomicInteger localNotifications = new AtomicInteger(0);
        AtomicInteger peerNotifications = new AtomicInteger(0);
        AtomicReference<ClusterType> lastLocalClusterType = new AtomicReference<>();
        AtomicReference<ClusterType> lastPeerClusterType = new AtomicReference<>();

        // Create listeners for different target states
        HAGroupStateListener localListener = (groupName, fromState, toState, clusterType) -> {
            if (toState == HAGroupState.ACTIVE_IN_SYNC && clusterType == ClusterType.LOCAL) {
                localNotifications.incrementAndGet();
                lastLocalClusterType.set(clusterType);
                LOGGER.info("Local listener called: {} -> {} on {}", fromState, toState, clusterType);
            }
        };

        HAGroupStateListener peerListener = (groupName, fromState, toState, clusterType) -> {
            if (toState == HAGroupState.DEGRADED_STANDBY_FOR_READER && clusterType == ClusterType.PEER) {
                peerNotifications.incrementAndGet();
                lastPeerClusterType.set(clusterType);
                LOGGER.info("Peer listener called: {} -> {} on {}", fromState, toState, clusterType);
            }
        };

        // Subscribe to different target states on different clusters
        manager.subscribeToTargetState(haGroupName, HAGroupState.ACTIVE_IN_SYNC, ClusterType.LOCAL, localListener);
        manager.subscribeToTargetState(haGroupName, HAGroupState.DEGRADED_STANDBY_FOR_READER, ClusterType.PEER, peerListener);

        // Trigger transition to ACTIVE_IN_SYNC on LOCAL cluster
        HAGroupStoreRecord localRecord = new HAGroupStoreRecord("1.0", haGroupName, HAGroupState.ACTIVE_IN_SYNC);
        haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, localRecord, 0);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Need to set up peer record first
        // For peer cluster, we need to create the record in peer ZK
        PhoenixHAAdmin peerHaAdmin = new PhoenixHAAdmin(peerZKUrl, config, ZK_CONSISTENT_HA_NAMESPACE);
        HAGroupStoreRecord initialPeerRecord = new HAGroupStoreRecord("1.0", haGroupName, HAGroupState.STANDBY);
        peerHaAdmin.createHAGroupStoreRecordInZooKeeper(initialPeerRecord);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Trigger transition to DEGRADED_STANDBY_FOR_READER on PEER cluster
        HAGroupStoreRecord updatedPeerRecord = new HAGroupStoreRecord("1.0", haGroupName, HAGroupState.DEGRADED_STANDBY_FOR_READER);
        peerHaAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, updatedPeerRecord, 0);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Verify each cluster got its own notification
        assertEquals("Local cluster should receive notification", 1, localNotifications.get());
        assertEquals("Peer cluster should receive notification", 1, peerNotifications.get());
        assertEquals("Local notification should have LOCAL cluster type", ClusterType.LOCAL, lastLocalClusterType.get());
        assertEquals("Peer notification should have PEER cluster type", ClusterType.PEER, lastPeerClusterType.get());

        // Cleanup
        peerHaAdmin.close();
    }

    @Test
    public void testDifferentTransitionsPerCluster() throws Exception {
        String haGroupName = testName.getMethodName();
        HAGroupStoreManager manager = HAGroupStoreManager.getInstance(config);

        // Track notifications
        AtomicInteger localNotifications = new AtomicInteger(0);
        AtomicInteger peerNotifications = new AtomicInteger(0);
        AtomicReference<ClusterType> lastLocalClusterType = new AtomicReference<>();
        AtomicReference<ClusterType> lastPeerClusterType = new AtomicReference<>();

        // Create listeners for different specific transitions
        HAGroupStateListener localListener = (groupName, fromState, toState, clusterType) -> {
            if (fromState == HAGroupState.STANDBY && toState == HAGroupState.STANDBY_TO_ACTIVE && clusterType == ClusterType.LOCAL) {
                localNotifications.incrementAndGet();
                lastLocalClusterType.set(clusterType);
                LOGGER.info("Local transition listener called: {} -> {} on {}", fromState, toState, clusterType);
            }
        };

        HAGroupStateListener peerListener = (groupName, fromState, toState, clusterType) -> {
            if (fromState == HAGroupState.DEGRADED_STANDBY_FOR_READER && toState == HAGroupState.STANDBY && clusterType == ClusterType.PEER) {
                peerNotifications.incrementAndGet();
                lastPeerClusterType.set(clusterType);
                LOGGER.info("Peer transition listener called: {} -> {} on {}", fromState, toState, clusterType);
            }
        };

        // Subscribe to different transitions on different clusters
        manager.subscribeToTransition(haGroupName, HAGroupState.STANDBY, HAGroupState.STANDBY_TO_ACTIVE, ClusterType.LOCAL, localListener);
        manager.subscribeToTransition(haGroupName, HAGroupState.DEGRADED_STANDBY_FOR_READER, HAGroupState.STANDBY, ClusterType.PEER, peerListener);

        // Set initial states first
        HAGroupStoreRecord initialLocalRecord = new HAGroupStoreRecord("1.0", haGroupName, HAGroupState.STANDBY);
        haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, initialLocalRecord, 0);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        PhoenixHAAdmin peerHaAdmin = new PhoenixHAAdmin(peerZKUrl, config, ZK_CONSISTENT_HA_NAMESPACE);
        HAGroupStoreRecord initialPeerRecord = new HAGroupStoreRecord("1.0", haGroupName, HAGroupState.DEGRADED_STANDBY_FOR_READER);
        peerHaAdmin.createHAGroupStoreRecordInZooKeeper(initialPeerRecord);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Trigger transition A on LOCAL cluster
        HAGroupStoreRecord localTransitionRecord = new HAGroupStoreRecord("1.0", haGroupName, HAGroupState.STANDBY_TO_ACTIVE);
        haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, localTransitionRecord, 1);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Trigger transition B on PEER cluster
        HAGroupStoreRecord peerTransitionRecord = new HAGroupStoreRecord("1.0", haGroupName, HAGroupState.STANDBY);
        peerHaAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, peerTransitionRecord, 0);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Verify no cross-cluster triggering
        assertEquals("Local cluster should receive its transition notification", 1, localNotifications.get());
        assertEquals("Peer cluster should receive its transition notification", 1, peerNotifications.get());
        assertEquals("Local notification should have LOCAL cluster type", ClusterType.LOCAL, lastLocalClusterType.get());
        assertEquals("Peer notification should have PEER cluster type", ClusterType.PEER, lastPeerClusterType.get());

        // Cleanup
        peerHaAdmin.close();
    }

    @Test
    public void testUnsubscribeSpecificCluster() throws Exception {
        String haGroupName = testName.getMethodName();
        HAGroupStoreManager manager = HAGroupStoreManager.getInstance(config);

        // Track notifications
        AtomicInteger totalNotifications = new AtomicInteger(0);
        AtomicReference<ClusterType> lastClusterType = new AtomicReference<>();

        HAGroupStateListener listener = (groupName, fromState, toState, clusterType) -> {
            if (fromState == HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY && toState == HAGroupState.STANDBY) {
                totalNotifications.incrementAndGet();
                lastClusterType.set(clusterType);
                LOGGER.info("Listener called: {} -> {} on {}", fromState, toState, clusterType);
            }
        };

        // Subscribe to same transition on both clusters
        manager.subscribeToTransition(haGroupName, HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY, HAGroupState.STANDBY, ClusterType.LOCAL, listener);
        manager.subscribeToTransition(haGroupName, HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY, HAGroupState.STANDBY, ClusterType.PEER, listener);

        // Unsubscribe from LOCAL cluster only
        manager.unsubscribeFromTransition(haGroupName, HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY, HAGroupState.STANDBY, ClusterType.LOCAL, listener);

        // Set initial states
        HAGroupStoreRecord initialRecord = new HAGroupStoreRecord("1.0", haGroupName, HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY);
        haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, initialRecord, 0);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        PhoenixHAAdmin peerHaAdmin = new PhoenixHAAdmin(peerZKUrl, config, ZK_CONSISTENT_HA_NAMESPACE);
        peerHaAdmin.createHAGroupStoreRecordInZooKeeper(initialRecord);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Trigger transition on LOCAL → should NOT call listener
        HAGroupStoreRecord localRecord = new HAGroupStoreRecord("1.0", haGroupName, HAGroupState.STANDBY);
        haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, localRecord, 1);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        assertEquals("Should receive no notifications from LOCAL cluster", 0, totalNotifications.get());

        // Trigger transition on PEER → should call listener
        HAGroupStoreRecord peerRecord = new HAGroupStoreRecord("1.0", haGroupName, HAGroupState.STANDBY);
        peerHaAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, peerRecord, 0);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        assertEquals("Should receive notification from PEER cluster", 1, totalNotifications.get());
        assertEquals("Notification should be from PEER cluster", ClusterType.PEER, lastClusterType.get());

        // Cleanup
        peerHaAdmin.close();
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

        HAGroupStateListener listener1 = (groupName, fromState, toState, clusterType) -> {
            if (fromState == HAGroupState.DEGRADED_STANDBY_FOR_WRITER && toState == HAGroupState.DEGRADED_STANDBY) {
                if (clusterType == ClusterType.LOCAL) {
                    listener1LocalNotifications.incrementAndGet();
                } else {
                    listener1PeerNotifications.incrementAndGet();
                }
                LOGGER.info("Listener1 called: {} -> {} on {}", fromState, toState, clusterType);
            }
        };

        HAGroupStateListener listener2 = (groupName, fromState, toState, clusterType) -> {
            if (fromState == HAGroupState.DEGRADED_STANDBY_FOR_WRITER && toState == HAGroupState.DEGRADED_STANDBY) {
                if (clusterType == ClusterType.LOCAL) {
                    listener2LocalNotifications.incrementAndGet();
                } else {
                    listener2PeerNotifications.incrementAndGet();
                }
                LOGGER.info("Listener2 called: {} -> {} on {}", fromState, toState, clusterType);
            }
        };

        // Register multiple listeners for same transition on both clusters
        manager.subscribeToTransition(haGroupName, HAGroupState.DEGRADED_STANDBY_FOR_WRITER, HAGroupState.DEGRADED_STANDBY, ClusterType.LOCAL, listener1);
        manager.subscribeToTransition(haGroupName, HAGroupState.DEGRADED_STANDBY_FOR_WRITER, HAGroupState.DEGRADED_STANDBY, ClusterType.LOCAL, listener2);
        manager.subscribeToTransition(haGroupName, HAGroupState.DEGRADED_STANDBY_FOR_WRITER, HAGroupState.DEGRADED_STANDBY, ClusterType.PEER, listener1);
        manager.subscribeToTransition(haGroupName, HAGroupState.DEGRADED_STANDBY_FOR_WRITER, HAGroupState.DEGRADED_STANDBY, ClusterType.PEER, listener2);

        // Set initial states
        HAGroupStoreRecord initialRecord = new HAGroupStoreRecord("1.0", haGroupName, HAGroupState.DEGRADED_STANDBY_FOR_WRITER);
        haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, initialRecord, 0);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        PhoenixHAAdmin peerHaAdmin = new PhoenixHAAdmin(peerZKUrl, config, ZK_CONSISTENT_HA_NAMESPACE);
        peerHaAdmin.createHAGroupStoreRecordInZooKeeper(initialRecord);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Trigger transition on LOCAL
        HAGroupStoreRecord localRecord = new HAGroupStoreRecord("1.0", haGroupName, HAGroupState.DEGRADED_STANDBY);
        haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, localRecord, 1);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Trigger transition on PEER
        HAGroupStoreRecord peerRecord = new HAGroupStoreRecord("1.0", haGroupName, HAGroupState.DEGRADED_STANDBY);
        peerHaAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, peerRecord, 0);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Verify all listeners called for each cluster
        assertEquals("Listener1 should receive LOCAL notification", 1, listener1LocalNotifications.get());
        assertEquals("Listener2 should receive LOCAL notification", 1, listener2LocalNotifications.get());
        assertEquals("Listener1 should receive PEER notification", 1, listener1PeerNotifications.get());
        assertEquals("Listener2 should receive PEER notification", 1, listener2PeerNotifications.get());

        // Cleanup
        peerHaAdmin.close();
    }

    @Test
    public void testSameListenerDifferentClusters() throws Exception {
        String haGroupName = testName.getMethodName();
        HAGroupStoreManager manager = HAGroupStoreManager.getInstance(config);

        // Track which transitions were called
        AtomicInteger transitionANotifications = new AtomicInteger(0);
        AtomicInteger transitionBNotifications = new AtomicInteger(0);
        AtomicReference<ClusterType> lastTransitionAClusterType = new AtomicReference<>();
        AtomicReference<ClusterType> lastTransitionBClusterType = new AtomicReference<>();

        HAGroupStateListener sharedListener = (groupName, fromState, toState, clusterType) -> {
            if (fromState == HAGroupState.ACTIVE_NOT_IN_SYNC_TO_STANDBY && toState == HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY && clusterType == ClusterType.LOCAL) {
                transitionANotifications.incrementAndGet();
                lastTransitionAClusterType.set(clusterType);
                LOGGER.info("Shared listener - Transition A: {} -> {} on {}", fromState, toState, clusterType);
            } else if (fromState == HAGroupState.ABORT_TO_ACTIVE_IN_SYNC && toState == HAGroupState.ACTIVE_IN_SYNC && clusterType == ClusterType.PEER) {
                transitionBNotifications.incrementAndGet();
                lastTransitionBClusterType.set(clusterType);
                LOGGER.info("Shared listener - Transition B: {} -> {} on {}", fromState, toState, clusterType);
            }
        };

        // Register same listener for different transitions on different clusters
        manager.subscribeToTransition(haGroupName, HAGroupState.ACTIVE_NOT_IN_SYNC_TO_STANDBY, HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY, ClusterType.LOCAL, sharedListener);
        manager.subscribeToTransition(haGroupName, HAGroupState.ABORT_TO_ACTIVE_IN_SYNC, HAGroupState.ACTIVE_IN_SYNC, ClusterType.PEER, sharedListener);

        // Set initial states
        HAGroupStoreRecord initialLocalRecord = new HAGroupStoreRecord("1.0", haGroupName, HAGroupState.ACTIVE_NOT_IN_SYNC_TO_STANDBY);
        haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, initialLocalRecord, 0);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        PhoenixHAAdmin peerHaAdmin = new PhoenixHAAdmin(peerZKUrl, config, ZK_CONSISTENT_HA_NAMESPACE);
        HAGroupStoreRecord initialPeerRecord = new HAGroupStoreRecord("1.0", haGroupName, HAGroupState.ABORT_TO_ACTIVE_IN_SYNC);
        peerHaAdmin.createHAGroupStoreRecordInZooKeeper(initialPeerRecord);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Trigger transition A on LOCAL
        HAGroupStoreRecord localTransitionRecord = new HAGroupStoreRecord("1.0", haGroupName, HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY);
        haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, localTransitionRecord, 1);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Trigger transition B on PEER
        HAGroupStoreRecord peerTransitionRecord = new HAGroupStoreRecord("1.0", haGroupName, HAGroupState.ACTIVE_IN_SYNC);
        peerHaAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, peerTransitionRecord, 0);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Verify listener called for each appropriate transition/cluster combination
        assertEquals("Should receive transition A notification", 1, transitionANotifications.get());
        assertEquals("Should receive transition B notification", 1, transitionBNotifications.get());
        assertEquals("Transition A should be from LOCAL cluster", ClusterType.LOCAL, lastTransitionAClusterType.get());
        assertEquals("Transition B should be from PEER cluster", ClusterType.PEER, lastTransitionBClusterType.get());

        // Cleanup
        peerHaAdmin.close();
    }

    // ========== Edge Cases & Error Handling ==========

    @Test
    public void testSubscriptionToNonExistentHAGroup() throws Exception {
        String nonExistentHAGroup = "nonExistentGroup_" + testName.getMethodName();
        HAGroupStoreManager manager = HAGroupStoreManager.getInstance(config);

        HAGroupStateListener listener = (groupName, fromState, toState, clusterType) -> {
            // Should not be called
        };

        // Try to subscribe to non-existent HA group
        try {
            manager.subscribeToTransition(nonExistentHAGroup, HAGroupState.OFFLINE, HAGroupState.STANDBY, ClusterType.LOCAL, listener);
            fail("Expected SQLException for non-existent HA group");
        } catch (SQLException e) {
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

        HAGroupStateListener goodListener1 = (groupName, fromState, toState, clusterType) -> {
            goodListener1Notifications.incrementAndGet();
            LOGGER.info("Good listener 1 called: {} -> {} on {}", fromState, toState, clusterType);
        };

        HAGroupStateListener badListener = (groupName, fromState, toState, clusterType) -> {
            badListenerCalls.incrementAndGet();
            LOGGER.info("Bad listener called, about to throw exception");
            throw new RuntimeException("Test exception from bad listener");
        };

        HAGroupStateListener goodListener2 = (groupName, fromState, toState, clusterType) -> {
            goodListener2Notifications.incrementAndGet();
            LOGGER.info("Good listener 2 called: {} -> {} on {}", fromState, toState, clusterType);
        };

        // Register listeners - bad listener in the middle
        manager.subscribeToTransition(haGroupName, HAGroupState.ACTIVE_WITH_OFFLINE_PEER, HAGroupState.ACTIVE_NOT_IN_SYNC, ClusterType.LOCAL, goodListener1);
        manager.subscribeToTransition(haGroupName, HAGroupState.ACTIVE_WITH_OFFLINE_PEER, HAGroupState.ACTIVE_NOT_IN_SYNC, ClusterType.LOCAL, badListener);
        manager.subscribeToTransition(haGroupName, HAGroupState.ACTIVE_WITH_OFFLINE_PEER, HAGroupState.ACTIVE_NOT_IN_SYNC, ClusterType.LOCAL, goodListener2);

        // Set initial state and trigger transition
        HAGroupStoreRecord initialRecord = new HAGroupStoreRecord("1.0", haGroupName, HAGroupState.ACTIVE_WITH_OFFLINE_PEER);
        haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, initialRecord, 0);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        HAGroupStoreRecord transitionRecord = new HAGroupStoreRecord("1.0", haGroupName, HAGroupState.ACTIVE_NOT_IN_SYNC);
        haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, transitionRecord, 1);
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

                    HAGroupStateListener listener = (groupName, fromState, toState, clusterType) -> {
                        LOGGER.debug("Thread {} listener called: {} -> {} on {}", threadIndex, fromState, toState, clusterType);
                    };

                    // Half subscribe to LOCAL, half to PEER
                    ClusterType clusterType = (threadIndex % 2 == 0) ? ClusterType.LOCAL : ClusterType.PEER;

                    manager.subscribeToTransition(haGroupName, HAGroupState.STANDBY_TO_ACTIVE, HAGroupState.ACTIVE_IN_SYNC, clusterType, listener);
                    successfulSubscriptions.incrementAndGet();

                    // Also test unsubscribe
                    manager.unsubscribeFromTransition(haGroupName, HAGroupState.STANDBY_TO_ACTIVE, HAGroupState.ACTIVE_IN_SYNC, clusterType, listener);

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

        HAGroupStateListener listener = (groupName, fromState, toState, clusterType) -> {
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
        PhoenixHAAdmin peerHaAdmin = new PhoenixHAAdmin(peerZKUrl, config, ZK_CONSISTENT_HA_NAMESPACE);
        HAGroupStoreRecord initialPeerRecord = new HAGroupStoreRecord("1.0", haGroupName,HAGroupState.DEGRADED_STANDBY_FOR_WRITER);
        peerHaAdmin.createHAGroupStoreRecordInZooKeeper(initialPeerRecord);

        for (int i = 0; i < changeCount; i++) {
            // Change local cluster
            HAGroupStoreRecord localRecord = new HAGroupStoreRecord("1.0", haGroupName,
                    (i % 2 == 0) ? HAGroupState.STANDBY : HAGroupState.DEGRADED_STANDBY_FOR_READER);
            haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, localRecord, -1);

            // Change peer cluster
            HAGroupStoreRecord peerRecord = new HAGroupStoreRecord("1.0", haGroupName,
                    (i % 2 == 0) ? HAGroupState.STANDBY : HAGroupState.DEGRADED_STANDBY_FOR_WRITER);
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

        // Cleanup
        peerHaAdmin.close();
    }

    // ========== Cleanup & Resource Management Tests ==========

    @Test
    public void testSubscriptionCleanupPerCluster() throws Exception {
        String haGroupName = testName.getMethodName();
        HAGroupStoreManager manager = HAGroupStoreManager.getInstance(config);

        // Create listeners
        HAGroupStateListener listener1 = (groupName, fromState, toState, clusterType) -> {};
        HAGroupStateListener listener2 = (groupName, fromState, toState, clusterType) -> {};
        HAGroupStateListener listener3 = (groupName, fromState, toState, clusterType) -> {};
        HAGroupStateListener listener4 = (groupName, fromState, toState, clusterType) -> {};

        // Get client to access internal maps via reflection
        HAGroupStoreClient client = HAGroupStoreClient.getInstanceForZkUrl(config, haGroupName, zkUrl);
        assertNotNull("Client should exist", client);

        // Use reflection to access subscription maps
        ConcurrentHashMap<String, CopyOnWriteArraySet<HAGroupStateListener>> specificTransitionSubscribers = getSubscriptionMap(client, "specificTransitionSubscribers");
        ConcurrentHashMap<String, CopyOnWriteArraySet<HAGroupStateListener>> targetStateSubscribers = getSubscriptionMap(client, "targetStateSubscribers");

        // Verify maps are initially empty
        assertEquals("Specific transition subscribers map should be empty initially", 0, specificTransitionSubscribers.size());
        assertEquals("Target state subscribers map should be empty initially", 0, targetStateSubscribers.size());

        // Subscribe listeners to both clusters for specific transitions
        manager.subscribeToTransition(haGroupName, HAGroupState.ACTIVE_NOT_IN_SYNC_WITH_OFFLINE_PEER, HAGroupState.ACTIVE_NOT_IN_SYNC, ClusterType.LOCAL, listener1);
        manager.subscribeToTransition(haGroupName, HAGroupState.ACTIVE_NOT_IN_SYNC_WITH_OFFLINE_PEER, HAGroupState.ACTIVE_NOT_IN_SYNC, ClusterType.LOCAL, listener2);
        manager.subscribeToTransition(haGroupName, HAGroupState.ACTIVE_NOT_IN_SYNC_WITH_OFFLINE_PEER, HAGroupState.ACTIVE_NOT_IN_SYNC, ClusterType.PEER, listener2);
        manager.subscribeToTransition(haGroupName, HAGroupState.ACTIVE_NOT_IN_SYNC_WITH_OFFLINE_PEER, HAGroupState.ACTIVE_NOT_IN_SYNC, ClusterType.PEER, listener3);

        // Subscribe listeners for target states
        manager.subscribeToTargetState(haGroupName, HAGroupState.STANDBY, ClusterType.LOCAL, listener4);
        manager.subscribeToTargetState(haGroupName, HAGroupState.STANDBY, ClusterType.PEER, listener4);

        // Verify maps are properly populated after subscriptions
        assertEquals("Should have 2 entries in specific transition map (LOCAL and PEER)", 2, specificTransitionSubscribers.size());
        assertEquals("Should have 2 entries in target state map (LOCAL and PEER)", 2, targetStateSubscribers.size());

        // Check specific transition subscribers
        String localTransitionKey = "LOCAL:ACTIVE_NOT_IN_SYNC_WITH_OFFLINE_PEER:ACTIVE_NOT_IN_SYNC";
        String peerTransitionKey = "PEER:ACTIVE_NOT_IN_SYNC_WITH_OFFLINE_PEER:ACTIVE_NOT_IN_SYNC";

        assertTrue("Should contain LOCAL transition key", specificTransitionSubscribers.containsKey(localTransitionKey));
        assertTrue("Should contain PEER transition key", specificTransitionSubscribers.containsKey(peerTransitionKey));

        assertEquals("LOCAL transition should have 2 listeners", 2, specificTransitionSubscribers.get(localTransitionKey).size());
        assertEquals("PEER transition should have 2 listeners", 2, specificTransitionSubscribers.get(peerTransitionKey).size());

        // Check target state subscribers
        String localTargetKey = "LOCAL:STANDBY";
        String peerTargetKey = "PEER:STANDBY";

        assertTrue("Should contain LOCAL target state key", targetStateSubscribers.containsKey(localTargetKey));
        assertTrue("Should contain PEER target state key", targetStateSubscribers.containsKey(peerTargetKey));

        assertEquals("LOCAL target state should have 1 listener", 1, targetStateSubscribers.get(localTargetKey).size());
        assertEquals("PEER target state should have 1 listener", 1, targetStateSubscribers.get(peerTargetKey).size());

        // Unsubscribe selectively
        manager.unsubscribeFromTransition(haGroupName, HAGroupState.ACTIVE_NOT_IN_SYNC_WITH_OFFLINE_PEER, HAGroupState.ACTIVE_NOT_IN_SYNC, ClusterType.LOCAL, listener1);
        manager.unsubscribeFromTransition(haGroupName, HAGroupState.ACTIVE_NOT_IN_SYNC_WITH_OFFLINE_PEER, HAGroupState.ACTIVE_NOT_IN_SYNC, ClusterType.PEER, listener2);
        manager.unsubscribeFromTargetState(haGroupName, HAGroupState.STANDBY, ClusterType.LOCAL, listener4);

        // Verify maps are properly updated after unsubscriptions
        assertEquals("Should still have 2 entries in specific transition map", 2, specificTransitionSubscribers.size());
        assertEquals("Should still have 2 entries in target state map", 1, targetStateSubscribers.size());

        // Check specific transition subscribers after unsubscribe
        assertEquals("LOCAL transition should have 1 listener after unsubscribe", 1, specificTransitionSubscribers.get(localTransitionKey).size());
        assertEquals("PEER transition should have 1 listener after unsubscribe", 1, specificTransitionSubscribers.get(peerTransitionKey).size());

        // Check target state subscribers after unsubscribe
        assertNull("LOCAL target state should have 0 listeners after unsubscribe", targetStateSubscribers.get(localTargetKey));
        assertEquals("PEER target state should have 1 listener after unsubscribe", 1, targetStateSubscribers.get(peerTargetKey).size());

        // Unsubscribe all remaining listeners
        manager.unsubscribeFromTransition(haGroupName, HAGroupState.ACTIVE_NOT_IN_SYNC_WITH_OFFLINE_PEER, HAGroupState.ACTIVE_NOT_IN_SYNC, ClusterType.LOCAL, listener2);
        manager.unsubscribeFromTransition(haGroupName, HAGroupState.ACTIVE_NOT_IN_SYNC_WITH_OFFLINE_PEER, HAGroupState.ACTIVE_NOT_IN_SYNC, ClusterType.PEER, listener3);
        manager.unsubscribeFromTargetState(haGroupName, HAGroupState.STANDBY, ClusterType.PEER, listener4);

        // Verify all listener sets are empty but keys may still exist
        assertNull("LOCAL transition should have 0 listeners after all unsubscribes", specificTransitionSubscribers.get(localTransitionKey));
        assertNull("PEER transition should have 0 listeners after all unsubscribes", specificTransitionSubscribers.get(peerTransitionKey));
        assertNull("PEER target state should have 0 listeners after all unsubscribes", targetStateSubscribers.get(peerTargetKey));

        // Test that new subscriptions still work properly
        AtomicInteger newSubscriptionNotifications = new AtomicInteger(0);
        HAGroupStateListener newTestListener = (groupName, fromState, toState, clusterType) -> {
            newSubscriptionNotifications.incrementAndGet();
            LOGGER.info("New subscription triggered: {} -> {} on {}", fromState, toState, clusterType);
        };

        // Subscribe with new test listener
        manager.subscribeToTransition(haGroupName, HAGroupState.ACTIVE_NOT_IN_SYNC_WITH_OFFLINE_PEER, HAGroupState.ACTIVE_NOT_IN_SYNC, ClusterType.LOCAL, newTestListener);

        // Verify the map is updated with new subscription
        assertEquals("LOCAL transition should have 1 listener after new subscription", 1, specificTransitionSubscribers.get(localTransitionKey).size());

        // Trigger transition and verify it works
        HAGroupStoreRecord initialRecord = new HAGroupStoreRecord("1.0", haGroupName, HAGroupState.ACTIVE_NOT_IN_SYNC_WITH_OFFLINE_PEER);
        haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, initialRecord, 0);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        HAGroupStoreRecord transitionRecord = new HAGroupStoreRecord("1.0", haGroupName, HAGroupState.ACTIVE_NOT_IN_SYNC);
        haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, transitionRecord, 1);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Expected: exactly 1 transition from ACTIVE_NOT_IN_SYNC_WITH_OFFLINE_PEER to ACTIVE_NOT_IN_SYNC
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
