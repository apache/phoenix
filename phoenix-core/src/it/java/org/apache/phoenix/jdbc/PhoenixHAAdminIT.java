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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.exception.StaleHAGroupStoreRecordVersionException;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.phoenix.jdbc.HAGroupStoreClient.ZK_CONSISTENT_HA_NAMESPACE;
import static org.apache.phoenix.jdbc.PhoenixHAAdmin.toPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Integration tests for {@link PhoenixHAAdmin} HAGroupStoreRecord operations
 */
@Category(NeedsOwnMiniClusterTest.class)
public class PhoenixHAAdminIT extends BaseTest {

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
        cleanupTestZnodes();
    }

    @After
    public void after() throws Exception {
        cleanupTestZnodes();
        if (haAdmin != null) {
            haAdmin.close();
        }
        if (peerHaAdmin != null) {
            peerHaAdmin.close();
        }
    }

    private void cleanupTestZnodes() throws Exception {
        haAdmin.getCurator().delete().quietly().forPath(toPath(testName.getMethodName()));
        peerHaAdmin.getCurator().delete().quietly().forPath(toPath(testName.getMethodName()));
    }

    @Test
    public void testCreateHAGroupStoreRecordInZooKeeper() throws Exception {
        String haGroupName = testName.getMethodName();
        String peerZKUrl = CLUSTERS.getZkUrl2();

        HAGroupStoreRecord record = new HAGroupStoreRecord(
                "v1.0",
                haGroupName,
                ClusterRoleRecord.ClusterRole.ACTIVE,
                1,
                "FAILOVER",
                System.currentTimeMillis(),
                peerZKUrl
        );

        // Create the record in ZooKeeper
        haAdmin.createHAGroupStoreRecordInZooKeeper(record);

        // Verify the record was created by reading it back
        byte[] data = haAdmin.getCurator().getData().forPath(toPath(haGroupName));
        HAGroupStoreRecord savedRecord = HAGroupStoreRecord.fromJson(data).get();

        assertEquals(record.getHaGroupName(), savedRecord.getHaGroupName());
        assertEquals(record.getClusterRole(), savedRecord.getClusterRole());
        assertEquals(record.getVersion(), savedRecord.getVersion());
        assertEquals(record.getPolicy(), savedRecord.getPolicy());
        assertEquals(record.getPeerZKUrl(), savedRecord.getPeerZKUrl());
    }

    @Test
    public void testCreateHAGroupStoreRecordInZooKeeperWithExistingNode() throws Exception {
        String haGroupName = testName.getMethodName();
        String peerZKUrl = CLUSTERS.getZkUrl2();

        HAGroupStoreRecord record1 = new HAGroupStoreRecord(
                "v1.0",
                haGroupName,
                ClusterRoleRecord.ClusterRole.ACTIVE,
                1,
                "FAILOVER",
                System.currentTimeMillis(),
                peerZKUrl
        );

        // Create the first record
        haAdmin.createHAGroupStoreRecordInZooKeeper(record1);

        // Try to create again with different data
        HAGroupStoreRecord record2 = new HAGroupStoreRecord(
                "v1.0",
                haGroupName,
                ClusterRoleRecord.ClusterRole.STANDBY,
                2,
                "FAILOVER",
                System.currentTimeMillis(),
                peerZKUrl
        );

        // This should throw an exception due to NodeExistsException handling
        try {
            haAdmin.createHAGroupStoreRecordInZooKeeper(record2);
            fail("Expected NodeExistsException");
        } catch (IOException e) {
            // Expected exception
            assertTrue(e.getCause() instanceof KeeperException.NodeExistsException);
            assertTrue(e.getMessage().contains("Failed to create HAGroupStoreRecord for HA group"));
        }

        // Verify the original record is still there (not overwritten)
        byte[] data = haAdmin.getCurator().getData().forPath(toPath(haGroupName));
        HAGroupStoreRecord savedRecord = HAGroupStoreRecord.fromJson(data).get();

        assertEquals(record1.getClusterRole(), savedRecord.getClusterRole());
        assertEquals(record1.getVersion(), savedRecord.getVersion());
    }

    @Test
    public void testUpdateHAGroupStoreRecordInZooKeeper() throws Exception {
        String haGroupName = testName.getMethodName();
        String peerZKUrl = CLUSTERS.getZkUrl2();

        // Create initial record
        HAGroupStoreRecord initialRecord = new HAGroupStoreRecord(
                "v1.0",
                haGroupName,
                ClusterRoleRecord.ClusterRole.ACTIVE,
                1,
                "FAILOVER",
                System.currentTimeMillis(),
                peerZKUrl
        );

        haAdmin.createHAGroupStoreRecordInZooKeeper(initialRecord);

        // Get the current stat for version checking
        Stat stat = haAdmin.getCurator().checkExists().forPath(toPath(haGroupName));
        assertNotNull(stat);
        int currentVersion = stat.getVersion();

        // Update the record
        HAGroupStoreRecord updatedRecord = new HAGroupStoreRecord(
                "v1.0",
                haGroupName,
                ClusterRoleRecord.ClusterRole.STANDBY,
                2,
                "FAILOVER",
                System.currentTimeMillis(),
                peerZKUrl
        );

        haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, updatedRecord, currentVersion);

        // Verify the record was updated
        byte[] data = haAdmin.getCurator().getData().forPath(toPath(haGroupName));
        HAGroupStoreRecord savedRecord = HAGroupStoreRecord.fromJson(data).get();

        assertEquals(updatedRecord.getClusterRole(), savedRecord.getClusterRole());
        assertEquals(updatedRecord.getVersion(), savedRecord.getVersion());
    }

    @Test
    public void testUpdateHAGroupStoreRecordInZooKeeperWithStaleVersion() throws Exception {
        String haGroupName = testName.getMethodName();
        String peerZKUrl = CLUSTERS.getZkUrl2();

        // Create initial record
        HAGroupStoreRecord initialRecord = new HAGroupStoreRecord(
                "v1.0",
                haGroupName,
                ClusterRoleRecord.ClusterRole.ACTIVE,
                1,
                "FAILOVER",
                System.currentTimeMillis(),
                peerZKUrl
        );

        haAdmin.createHAGroupStoreRecordInZooKeeper(initialRecord);

        // Get the current stat for version checking
        Stat stat = haAdmin.getCurator().checkExists().forPath(toPath(haGroupName));
        assertNotNull(stat);
        int currentVersion = stat.getVersion();

        // Update the record with current version (should succeed)
        HAGroupStoreRecord updatedRecord = new HAGroupStoreRecord(
                "v1.0",
                haGroupName,
                ClusterRoleRecord.ClusterRole.STANDBY,
                2,
                "FAILOVER",
                System.currentTimeMillis(),
                peerZKUrl
        );

        haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, updatedRecord, currentVersion);

        // Try to update again with the same (now stale) version - should fail
        HAGroupStoreRecord anotherUpdate = new HAGroupStoreRecord(
                "v1.0",
                haGroupName,
                ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY,
                3,
                "FAILOVER",
                System.currentTimeMillis(),
                peerZKUrl
        );

        try {
            haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, anotherUpdate, currentVersion);
            fail("Expected StaleHAGroupStoreRecordVersionException");
        } catch (StaleHAGroupStoreRecordVersionException e) {
            // Expected exception
            assertTrue(e.getMessage().contains("with cached stat version"));
        }
    }

    @Test
    public void testGetHAGroupStoreRecordInZooKeeper() throws Exception {
        String haGroupName = testName.getMethodName();
        String peerZKUrl = CLUSTERS.getZkUrl2();

        // Create initial record
        HAGroupStoreRecord initialRecord = new HAGroupStoreRecord(
                "v1.0",
                haGroupName,
                ClusterRoleRecord.ClusterRole.ACTIVE,
                1,
                "FAILOVER",
                System.currentTimeMillis(),
                peerZKUrl
        );

        haAdmin.createHAGroupStoreRecordInZooKeeper(initialRecord);

        // Get the record and stat
        Pair<HAGroupStoreRecord, Stat> result = haAdmin.getHAGroupStoreRecordInZooKeeper(haGroupName);

        assertNotNull(result);
        assertNotNull(result.getLeft());
        assertNotNull(result.getRight());

        HAGroupStoreRecord retrievedRecord = result.getLeft();
        Stat stat = result.getRight();

        assertEquals(initialRecord.getHaGroupName(), retrievedRecord.getHaGroupName());
        assertEquals(initialRecord.getClusterRole(), retrievedRecord.getClusterRole());
        assertEquals(initialRecord.getVersion(), retrievedRecord.getVersion());
        assertEquals(initialRecord.getPolicy(), retrievedRecord.getPolicy());
        assertEquals(initialRecord.getPeerZKUrl(), retrievedRecord.getPeerZKUrl());

        // Verify stat is valid
        assertTrue(stat.getVersion() >= 0);
        assertTrue(stat.getCtime() > 0);
        assertTrue(stat.getMtime() > 0);
    }

    @Test
    public void testGetHAGroupStoreRecordInZooKeeperNonExistentNode() throws Exception {
        String haGroupName = testName.getMethodName();

        try {
            haAdmin.getHAGroupStoreRecordInZooKeeper(haGroupName);
            fail("Expected IOException for non-existent node");
        } catch (IOException e) {
            // Expected exception
            assertTrue(e.getMessage().contains("Failed to get HAGroupStoreRecord"));
        }
    }

    @Test
    public void testCompleteWorkflowCreateUpdateGet() throws Exception {
        String haGroupName = testName.getMethodName();
        String peerZKUrl = CLUSTERS.getZkUrl2();

        // Step 1: Create initial record
        HAGroupStoreRecord initialRecord = new HAGroupStoreRecord(
                "v1.0",
                haGroupName,
                ClusterRoleRecord.ClusterRole.ACTIVE,
                1,
                "FAILOVER",
                System.currentTimeMillis(),
                peerZKUrl
        );

        haAdmin.createHAGroupStoreRecordInZooKeeper(initialRecord);

        // Step 2: Get the record
        Pair<HAGroupStoreRecord, Stat> result = haAdmin.getHAGroupStoreRecordInZooKeeper(haGroupName);
        HAGroupStoreRecord retrievedRecord = result.getLeft();
        Stat stat = result.getRight();

        assertEquals(initialRecord.getHaGroupName(), retrievedRecord.getHaGroupName());
        assertEquals(initialRecord.getClusterRole(), retrievedRecord.getClusterRole());
        assertEquals(initialRecord.getVersion(), retrievedRecord.getVersion());

        // Step 3: Update the record
        HAGroupStoreRecord updatedRecord = new HAGroupStoreRecord(
                "v1.0",
                haGroupName,
                ClusterRoleRecord.ClusterRole.STANDBY,
                2,
                "FAILOVER",
                System.currentTimeMillis(),
                peerZKUrl
        );

        haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, updatedRecord, stat.getVersion());

        // Step 4: Get the updated record
        Pair<HAGroupStoreRecord, Stat> updatedResult = haAdmin.getHAGroupStoreRecordInZooKeeper(haGroupName);
        HAGroupStoreRecord finalRecord = updatedResult.getLeft();
        Stat finalStat = updatedResult.getRight();

        assertEquals(updatedRecord.getClusterRole(), finalRecord.getClusterRole());
        assertEquals(updatedRecord.getVersion(), finalRecord.getVersion());

        // Verify stat version increased
        assertTrue(finalStat.getVersion() > stat.getVersion());
    }

    @Test
    public void testMultiThreadedUpdatesConcurrentVersionConflict() throws Exception {
        String haGroupName = testName.getMethodName();
        String peerZKUrl = CLUSTERS.getZkUrl2();

        // Create initial record
        HAGroupStoreRecord initialRecord = new HAGroupStoreRecord(
                "v1.0",
                haGroupName,
                ClusterRoleRecord.ClusterRole.ACTIVE,
                1,
                "FAILOVER",
                System.currentTimeMillis(),
                peerZKUrl
        );

        haAdmin.createHAGroupStoreRecordInZooKeeper(initialRecord);

        // Get the current stat for version checking
        Pair<HAGroupStoreRecord, Stat> result = haAdmin.getHAGroupStoreRecordInZooKeeper(haGroupName);
        int currentVersion = result.getRight().getVersion();

        // Number of threads to run concurrently
        int threadCount = 5;
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(threadCount);

        // Counters for tracking results
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger staleVersionExceptionCount = new AtomicInteger(0);
        AtomicInteger otherExceptionCount = new AtomicInteger(0);

        // Submit multiple threads that will all try to update with the same version
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executorService.submit(() -> {
                try {
                    // Wait for all threads to be ready
                    startLatch.await();

                    // Create a unique update record for this thread
                    HAGroupStoreRecord updatedRecord = new HAGroupStoreRecord(
                            "v1.0",
                            haGroupName,
                            ClusterRoleRecord.ClusterRole.STANDBY,
                            2 + threadId, // Different version for each thread
                            "FAILOVER",
                            System.currentTimeMillis(),
                            peerZKUrl
                    );

                    // All threads use the same currentVersion, causing conflicts
                    haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, updatedRecord, currentVersion);
                    successCount.incrementAndGet();

                } catch (StaleHAGroupStoreRecordVersionException e) {
                    staleVersionExceptionCount.incrementAndGet();
                } catch (Exception e) {
                    otherExceptionCount.incrementAndGet();
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        // Start all threads at the same time
        startLatch.countDown();

        // Wait for all threads to complete
        assertTrue("Threads did not complete within timeout",
                finishLatch.await(10, TimeUnit.SECONDS));

        executorService.shutdown();
        assertTrue("ExecutorService did not shutdown within timeout",
                executorService.awaitTermination(5, TimeUnit.SECONDS));

        // Verify results
        assertEquals("No other exceptions should occur", 0, otherExceptionCount.get());
        assertEquals("Exactly one thread should succeed", 1, successCount.get());
        assertEquals("All other threads should get stale version exception",
                threadCount - 1, staleVersionExceptionCount.get());

        // Verify the final state - should contain the update from the successful thread
        Pair<HAGroupStoreRecord, Stat> finalResult = haAdmin.getHAGroupStoreRecordInZooKeeper(haGroupName);
        HAGroupStoreRecord finalRecord = finalResult.getLeft();
        Stat finalStat = finalResult.getRight();

        // The successful update should have changed the role to STANDBY
        assertEquals(ClusterRoleRecord.ClusterRole.STANDBY, finalRecord.getClusterRole());

        // The ZooKeeper version should have increased
        assertTrue("ZooKeeper version should have increased", finalStat.getVersion() > currentVersion);
    }

    @Test
    public void testMultiThreadedUpdatesWithDifferentVersions() throws Exception {
        String haGroupName = testName.getMethodName();
        String peerZKUrl = CLUSTERS.getZkUrl2();

        // Create initial record
        HAGroupStoreRecord initialRecord = new HAGroupStoreRecord(
                "v1.0",
                haGroupName,
                ClusterRoleRecord.ClusterRole.ACTIVE,
                1,
                "FAILOVER",
                System.currentTimeMillis(),
                peerZKUrl
        );

        haAdmin.createHAGroupStoreRecordInZooKeeper(initialRecord);

        // Number of threads to run sequentially (each gets the latest version)
        int threadCount = 10;
        ExecutorService executorService = Executors.newFixedThreadPool(1); // Single thread pool for sequential execution
        CountDownLatch finishLatch = new CountDownLatch(threadCount);

        // Counter for tracking results
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);

        // Submit multiple threads that will update sequentially
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executorService.submit(() -> {
                try {
                    // Get the current version for this thread
                    Pair<HAGroupStoreRecord, Stat> currentResult = haAdmin.getHAGroupStoreRecordInZooKeeper(haGroupName);
                    int currentVersion = currentResult.getRight().getVersion();

                    // Create update record for this thread
                    HAGroupStoreRecord updatedRecord = new HAGroupStoreRecord(
                            "v1.0",
                            haGroupName,
                            threadId % 2 == 0 ? ClusterRoleRecord.ClusterRole.ACTIVE : ClusterRoleRecord.ClusterRole.STANDBY,
                            2 + threadId,
                            "FAILOVER",
                            System.currentTimeMillis(),
                            peerZKUrl
                    );

                    // Update with the current version
                    haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, updatedRecord, currentVersion);
                    successCount.incrementAndGet();

                } catch (Exception e) {
                    failureCount.incrementAndGet();
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        // Wait for all threads to complete
        assertTrue("Threads did not complete within timeout",
                finishLatch.await(15, TimeUnit.SECONDS));

        executorService.shutdown();
        assertTrue("ExecutorService did not shutdown within timeout",
                executorService.awaitTermination(5, TimeUnit.SECONDS));

        // Verify results - all should succeed since they each get the latest version
        assertEquals("All threads should succeed when using correct versions",
                threadCount, successCount.get());
        assertEquals("No threads should fail", 0, failureCount.get());

        // Verify the final state
        Pair<HAGroupStoreRecord, Stat> finalResult = haAdmin.getHAGroupStoreRecordInZooKeeper(haGroupName);
        HAGroupStoreRecord finalRecord = finalResult.getLeft();

        // The final record should have the version from the last update
        assertEquals(threadCount + 1, finalRecord.getVersion());
    }


}