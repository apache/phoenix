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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.exception.InvalidClusterRoleTransitionException;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.HAGroupStoreTestUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.phoenix.jdbc.HAGroupStoreRecord.DEFAULT_RECORD_VERSION;
import static org.apache.hadoop.hbase.HConstants.DEFAULT_ZK_SESSION_TIMEOUT;
import static org.apache.hadoop.hbase.HConstants.ZK_SESSION_TIMEOUT;
import static org.apache.phoenix.jdbc.HAGroupStoreClient.ZK_CONSISTENT_HA_GROUP_STATE_NAMESPACE;
import static org.apache.phoenix.jdbc.HAGroupStoreClient.ZK_SESSION_TIMEOUT_MULTIPLIER;
import static org.apache.phoenix.jdbc.PhoenixHAAdmin.getLocalZkUrl;
import static org.apache.phoenix.jdbc.PhoenixHAAdmin.toPath;
import static org.apache.phoenix.query.QueryServices.CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Integration tests for {@link HAGroupStoreManager}.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class HAGroupStoreManagerIT extends BaseTest {

    @Rule
    public TestName testName = new TestName();

    private PhoenixHAAdmin haAdmin;
    private static final Long ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS = 2000L;
    private String zkUrl;
    private String peerZKUrl;
    private static final HighAvailabilityTestingUtility.HBaseTestingUtilityPair CLUSTERS = new HighAvailabilityTestingUtility.HBaseTestingUtilityPair();
    private final String defaultProtocolVersion = "1.0";

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(2);
        props.put(CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED, "true");
        props.put(ZK_SESSION_TIMEOUT, String.valueOf(30*1000));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
        CLUSTERS.start();
    }

    @Before
    public void before() throws Exception {
        haAdmin = new PhoenixHAAdmin(config, ZK_CONSISTENT_HA_GROUP_STATE_NAMESPACE);
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

    @Test
    public void testMutationBlockingWithSingleHAGroup() throws Exception {
        String haGroupName = testName.getMethodName();
        HAGroupStoreManager haGroupStoreManager = HAGroupStoreManager.getInstance(config);

        // Initially no mutation should be blocked
        assertFalse(haGroupStoreManager.isMutationBlocked(haGroupName));

        // Update to ACTIVE_TO_STANDBY role (should block mutations)
        HAGroupStoreRecord transitionRecord = new HAGroupStoreRecord(
                defaultProtocolVersion, haGroupName, HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY, DEFAULT_RECORD_VERSION);

        haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, transitionRecord, 0);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Now mutations should be blocked
        assertTrue(haGroupStoreManager.isMutationBlocked(haGroupName));
    }

    @Test
    public void testMutationBlockingWithMultipleHAGroups() throws Exception {
        String haGroupName1 = testName.getMethodName() + "_1";
        String haGroupName2 = testName.getMethodName() + "_2";
        HAGroupStoreManager haGroupStoreManager = HAGroupStoreManager.getInstance(config);

        // Create two HA groups with ACTIVE and ACTIVE_NOT_IN_SYNC roles
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName1, zkUrl,
                this.peerZKUrl, ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.ACTIVE, null);
        HAGroupStoreRecord activeRecord1 = new HAGroupStoreRecord(
                defaultProtocolVersion, haGroupName1, HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC, DEFAULT_RECORD_VERSION);
        haAdmin.createHAGroupStoreRecordInZooKeeper(activeRecord1);

        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName2, zkUrl,
                this.peerZKUrl, ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.ACTIVE, null);

        // No mutations should be blocked
        assertFalse(haGroupStoreManager.isMutationBlocked(haGroupName1));
        assertFalse(haGroupStoreManager.isMutationBlocked(haGroupName2));

        // Update only second group to ACTIVE_NOT_IN_SYNC_TO_STANDBY
        HAGroupStoreRecord transitionRecord2 = new HAGroupStoreRecord(
                defaultProtocolVersion, haGroupName2, HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC_TO_STANDBY, DEFAULT_RECORD_VERSION);

        haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName2, transitionRecord2, 0);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Global mutations should be blocked due to second group
        assertFalse(haGroupStoreManager.isMutationBlocked(haGroupName1));
        assertTrue(haGroupStoreManager.isMutationBlocked(haGroupName2));
    }

    @Test
    public void testGetHAGroupStoreRecord() throws Exception {
        String haGroupName = testName.getMethodName();
        HAGroupStoreManager haGroupStoreManager = HAGroupStoreManager.getInstance(config);

        // Get record from HAGroupStoreManager
        Optional<HAGroupStoreRecord> recordOpt = haGroupStoreManager.getHAGroupStoreRecord(haGroupName);
        // Should be present
        assertTrue(recordOpt.isPresent());

        // Delete record from System Table
        HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(haGroupName, zkUrl);
        // Delete record from ZK
        haAdmin.deleteHAGroupStoreRecordInZooKeeper(haGroupName);
        // Sleep for propagation time so that it is now reflected in cache.
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        // Get record from HAGroupStoreManager
        recordOpt = haGroupStoreManager.getHAGroupStoreRecord(haGroupName);
        // Should not be present
        assertFalse(recordOpt.isPresent());

        // Create record again in System Table
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName, zkUrl, this.peerZKUrl,
                ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY, null);
        // Now it should be present
        Optional<HAGroupStoreRecord>  retrievedOpt = haGroupStoreManager.getHAGroupStoreRecord(haGroupName);
        assertTrue(retrievedOpt.isPresent());

        // Get MTime from HAAdmin for equality verification below.
        Pair<HAGroupStoreRecord, Stat> currentRecordAndStat = haAdmin.getHAGroupStoreRecordInZooKeeper(haGroupName);

        // Complete object comparison field-by-field
        assertEquals(haGroupName, retrievedOpt.get().getHaGroupName());
        assertEquals(HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC, retrievedOpt.get().getHAGroupState());
        Long lastSyncStateTimeInMs = retrievedOpt.get().getLastSyncStateTimeInMs();
        Long mtime = currentRecordAndStat.getRight().getMtime();
        // Allow a small margin of error
        assertTrue(Math.abs(lastSyncStateTimeInMs - mtime) <= 1);
        assertEquals(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, retrievedOpt.get().getProtocolVersion());
    }

    @Test
    public void testGetPeerHAGroupStoreRecord() throws Exception {
        String haGroupName = testName.getMethodName();
        HAGroupStoreManager haGroupStoreManager = HAGroupStoreManager.getInstance(config);

        // Initially, peer record should not be present
        Optional<HAGroupStoreRecord> peerRecordOpt = haGroupStoreManager.getPeerHAGroupStoreRecord(haGroupName);
        assertFalse(peerRecordOpt.isPresent());

        // Create a peer HAAdmin to create records in peer cluster
        PhoenixHAAdmin peerHaAdmin = new PhoenixHAAdmin(CLUSTERS.getHBaseCluster2().getConfiguration(),
                ZK_CONSISTENT_HA_GROUP_STATE_NAMESPACE);

        try {
            // Create a HAGroupStoreRecord in the peer cluster
            HAGroupStoreRecord peerRecord = new HAGroupStoreRecord(
                    defaultProtocolVersion, haGroupName, HAGroupStoreRecord.HAGroupState.STANDBY, DEFAULT_RECORD_VERSION);

            peerHaAdmin.createHAGroupStoreRecordInZooKeeper(peerRecord);
            Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

            // Now peer record should be present
            peerRecordOpt = haGroupStoreManager.getPeerHAGroupStoreRecord(haGroupName);
            assertTrue(peerRecordOpt.isPresent());

            // Verify the peer record details
            HAGroupStoreRecord retrievedPeerRecord = peerRecordOpt.get();
            assertEquals(haGroupName, retrievedPeerRecord.getHaGroupName());
            assertEquals(HAGroupStoreRecord.HAGroupState.STANDBY, retrievedPeerRecord.getHAGroupState());
            assertEquals(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, retrievedPeerRecord.getProtocolVersion());

            // Delete peer record
            peerHaAdmin.deleteHAGroupStoreRecordInZooKeeper(haGroupName);
            Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

            // Peer record should no longer be present
            peerRecordOpt = haGroupStoreManager.getPeerHAGroupStoreRecord(haGroupName);
            assertFalse(peerRecordOpt.isPresent());

            // Create peer record again with different state
            HAGroupStoreRecord newPeerRecord = new HAGroupStoreRecord(
                    defaultProtocolVersion, haGroupName, HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY_FOR_READER, DEFAULT_RECORD_VERSION);

            peerHaAdmin.createHAGroupStoreRecordInZooKeeper(newPeerRecord);
            Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

            // Verify the updated peer record
            peerRecordOpt = haGroupStoreManager.getPeerHAGroupStoreRecord(haGroupName);
            assertTrue(peerRecordOpt.isPresent());
            assertEquals(HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY_FOR_READER,
                    peerRecordOpt.get().getHAGroupState());

        } finally {
            // Clean up peer record
            try {
                peerHaAdmin.deleteHAGroupStoreRecordInZooKeeper(haGroupName);
            } catch (Exception e) {
                // Ignore cleanup errors
            }
            peerHaAdmin.close();
        }
    }

    @Test
    public void testGetPeerHAGroupStoreRecordWhenHAGroupNotInSystemTable() throws Exception {
        String haGroupName = testName.getMethodName();
        HAGroupStoreManager haGroupStoreManager = HAGroupStoreManager.getInstance(config);

        // Try to get peer record for an HA group that doesn't exist in system table
        Optional<HAGroupStoreRecord> peerRecordOpt = haGroupStoreManager.getPeerHAGroupStoreRecord(haGroupName);
        assertFalse("Peer record should not be present for non-existent HA group", peerRecordOpt.isPresent());
    }

    @Test
    public void testInvalidateHAGroupStoreClient() throws Exception {
        String haGroupName = testName.getMethodName();
        HAGroupStoreManager haGroupStoreManager = HAGroupStoreManager.getInstance(config);

        // Create a HAGroupStoreRecord first
        HAGroupStoreRecord record = new HAGroupStoreRecord(
                defaultProtocolVersion, haGroupName, HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC, DEFAULT_RECORD_VERSION);

        haAdmin.createHAGroupStoreRecordInZooKeeper(record);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Ensure we can get the record
        Optional<HAGroupStoreRecord> recordOpt = haGroupStoreManager.getHAGroupStoreRecord(haGroupName);
        assertTrue(recordOpt.isPresent());

        // Invalidate the specific HA group client
        haGroupStoreManager.invalidateHAGroupStoreClient(haGroupName, false);

        // Should still be able to get the record after invalidation
        recordOpt = haGroupStoreManager.getHAGroupStoreRecord(haGroupName);
        assertTrue(recordOpt.isPresent());

        // Test global invalidation
        haGroupStoreManager.invalidateHAGroupStoreClient(false);

        // Should still be able to get the record after global invalidation
        recordOpt = haGroupStoreManager.getHAGroupStoreRecord(haGroupName);
        assertTrue(recordOpt.isPresent());
    }

    @Test
    public void testMutationBlockDisabled() throws Exception {
        String haGroupName = testName.getMethodName();

        // Create configuration with mutation block disabled
        Configuration conf = new Configuration();
        conf.set(CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED, "false");
        conf.set(HConstants.ZOOKEEPER_QUORUM, getLocalZkUrl(config));

        // Set the HAGroupStoreManager instance to null via reflection to force recreation
        Field field = HAGroupStoreManager.class.getDeclaredField("haGroupStoreManagerInstance");
        field.setAccessible(true);
        field.set(null, null);

        HAGroupStoreManager haGroupStoreManager = HAGroupStoreManager.getInstance(config);
        // Create HAGroupStoreRecord with ACTIVE_IN_SYNC_TO_STANDBY role
        HAGroupStoreRecord transitionRecord = new HAGroupStoreRecord(
                defaultProtocolVersion, haGroupName, HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY, DEFAULT_RECORD_VERSION);

        haAdmin.createHAGroupStoreRecordInZooKeeper(transitionRecord);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Mutations should not be blocked even with ACTIVE_TO_STANDBY role
        assertFalse(haGroupStoreManager.isMutationBlocked(haGroupName));

        // Set the HAGroupStoreManager instance back to null via reflection to force recreation for other tests
        field.set(null, null);
    }

    @Test
    public void testSetHAGroupStatusToStoreAndForward() throws Exception {
        String haGroupName = testName.getMethodName();
        HAGroupStoreManager haGroupStoreManager = HAGroupStoreManager.getInstance(config);

        // Create an initial HAGroupStoreRecord with ACTIVE status
        HAGroupStoreRecord initialRecord = new HAGroupStoreRecord(
                defaultProtocolVersion, haGroupName, HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC, DEFAULT_RECORD_VERSION);

        haAdmin.createHAGroupStoreRecordInZooKeeper(initialRecord);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Set the HA group status to store and forward (ACTIVE_NOT_IN_SYNC)
        haGroupStoreManager.setHAGroupStatusToStoreAndForward(haGroupName);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Verify the status was updated to ACTIVE_NOT_IN_SYNC
        Optional<HAGroupStoreRecord> updatedRecordOpt = haGroupStoreManager.getHAGroupStoreRecord(haGroupName);
        assertTrue(updatedRecordOpt.isPresent());
        HAGroupStoreRecord updatedRecord = updatedRecordOpt.get();
        assertEquals(HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC, updatedRecord.getHAGroupState());
        assertNotNull(updatedRecord.getLastSyncStateTimeInMs());

        // Set the HA group status to store and forward again and verify
        // that getLastSyncStateTimeInMs is same (ACTIVE_NOT_IN_SYNC)
        // The time should only update when we move to AIS to ANIS
        haGroupStoreManager.setHAGroupStatusToStoreAndForward(haGroupName);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
        Optional<HAGroupStoreRecord> updatedRecordOpt2 = haGroupStoreManager.getHAGroupStoreRecord(haGroupName);
        assertTrue(updatedRecordOpt2.isPresent());
        HAGroupStoreRecord updatedRecord2 = updatedRecordOpt.get();
        assertEquals(HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC, updatedRecord2.getHAGroupState());
        assertEquals(updatedRecord.getLastSyncStateTimeInMs(), updatedRecord2.getLastSyncStateTimeInMs());
    }

    @Test
    public void testSetHAGroupStatusToSync() throws Exception {
        String haGroupName = testName.getMethodName();
        HAGroupStoreManager haGroupStoreManager = HAGroupStoreManager.getInstance(config);

        // Initial record should be present in ACTIVE_NOT_IN_SYNC status
        HAGroupStoreRecord initialRecord = haGroupStoreManager.getHAGroupStoreRecord(haGroupName).orElse(null);
        assertNotNull(initialRecord);
        assertEquals(HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC, initialRecord.getHAGroupState());
        assertNotNull(initialRecord.getLastSyncStateTimeInMs());

        // Set the HA group status to sync (ACTIVE), we need to wait for ZK_SESSION_TIMEOUT * Multiplier
        Thread.sleep((long) Math.ceil(config.getLong(ZK_SESSION_TIMEOUT, DEFAULT_ZK_SESSION_TIMEOUT)
                * ZK_SESSION_TIMEOUT_MULTIPLIER));
        haGroupStoreManager.setHAGroupStatusToSync(haGroupName);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Verify the state was updated to ACTIVE_IN_SYNC
        Optional<HAGroupStoreRecord> updatedRecordOpt = haGroupStoreManager.getHAGroupStoreRecord(haGroupName);
        assertTrue(updatedRecordOpt.isPresent());
        HAGroupStoreRecord updatedRecord = updatedRecordOpt.get();
        assertEquals(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC, updatedRecord.getHAGroupState());
        assertNull(updatedRecord.getLastSyncStateTimeInMs());
    }

    @Test
    public void testGetHAGroupNamesFiltersCorrectlyByZkUrl() throws Exception {
        HAGroupStoreManager haGroupStoreManager = HAGroupStoreManager.getInstance(config);

        List<String> initialHAGroupNames = haGroupStoreManager.getHAGroupNames();

        // Create HA groups with current zkUrl as ZK_URL_1
        String haGroupWithCurrentZkUrl = testName.getMethodName() + "_current_zk";
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupWithCurrentZkUrl, zkUrl, this.peerZKUrl,
                ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY, null);

        // Create HA group with current zkUrl as ZK_URL_2 (swapped)
        String haGroupWithCurrentZkUrlAsPeer = testName.getMethodName() + "_current_as_peer";
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupWithCurrentZkUrlAsPeer, this.peerZKUrl, zkUrl,
                ClusterRoleRecord.ClusterRole.STANDBY, ClusterRoleRecord.ClusterRole.ACTIVE, zkUrl);

        // Create HA group with different zkUrl (should not appear in results)
        String differentZkUrl = "localhost:2182:/different";
        String haGroupWithDifferentZkUrl = testName.getMethodName() + "_different_zk";
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupWithDifferentZkUrl, differentZkUrl, "localhost:2183:/other",
                ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY, zkUrl);

        // Get HA group names - should only return groups where current zkUrl matches ZK_URL_1 or ZK_URL_2
        List<String> filteredHAGroupNames = haGroupStoreManager.getHAGroupNames();

        // Extract only new groups from filteredHAGroupNames
        List<String> newHAGroupNames = filteredHAGroupNames.stream()
        .filter(name -> !initialHAGroupNames.contains(name))
        .collect(Collectors.toList());

        // Check size of filteredHAGroupNames
        assertEquals(2, newHAGroupNames.size());


        // Should contain groups where current zkUrl is involved
        assertTrue("Should contain HA group with current zkUrl as ZK_URL_1",
                newHAGroupNames.contains(haGroupWithCurrentZkUrl));
        assertTrue("Should contain HA group with current zkUrl as ZK_URL_2",
                newHAGroupNames.contains(haGroupWithCurrentZkUrlAsPeer));
        // Should NOT contain HA group with different zkUrl
        assertFalse("Should NOT contain HA group with different zkUrl",
                newHAGroupNames.contains(haGroupWithDifferentZkUrl));


        // Clean up
        HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(haGroupWithCurrentZkUrl, zkUrl);
        HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(haGroupWithCurrentZkUrlAsPeer, zkUrl);
        HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(haGroupWithDifferentZkUrl, zkUrl);
    }

    @Test
    public void testGetHAGroupNamesWhenNoMatchingZkUrl() throws Exception {
        HAGroupStoreManager haGroupStoreManager = HAGroupStoreManager.getInstance(config);

        // Clean up existing HA group created in before()
        String testHAGroupName = testName.getMethodName();
        HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(testHAGroupName, zkUrl);

        // Create HA groups with completely different zkUrls
        String differentZkUrl1 = "localhost:2182:/different1";
        String differentZkUrl2 = "localhost:2183:/different2";
        String haGroupWithDifferentZkUrls = testName.getMethodName() + "_different";
        HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupWithDifferentZkUrls, differentZkUrl1, differentZkUrl2,
                ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY, zkUrl);

        // Get HA group names - should not contain the group with different zkUrls
        List<String> filteredHAGroupNames = haGroupStoreManager.getHAGroupNames();

        // Should NOT contain the HA group with different zkUrls
        assertFalse("Should NOT contain HA group with different zkUrls",
                filteredHAGroupNames.contains(haGroupWithDifferentZkUrls));

        // Clean up
        HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(haGroupWithDifferentZkUrls, zkUrl);

    }

    @Test
    public void testSetReaderToDegraded() throws Exception {
        String haGroupName = testName.getMethodName();
        HAGroupStoreManager haGroupStoreManager = HAGroupStoreManager.getInstance(config);

        // Update the auto-created record to STANDBY state for testing
        HAGroupStoreRecord standbyRecord = new HAGroupStoreRecord(
                defaultProtocolVersion, haGroupName, HAGroupStoreRecord.HAGroupState.STANDBY, DEFAULT_RECORD_VERSION);

        // Get the record to initialize ZNode from HAGroup so that we can artificially update it via HAAdmin
        Optional<HAGroupStoreRecord> currentRecord = haGroupStoreManager.getHAGroupStoreRecord(haGroupName);
        assertTrue(currentRecord.isPresent());

        // Update via HAAdmin
        haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, standbyRecord, 0);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Call setReaderToDegraded
        haGroupStoreManager.setReaderToDegraded(haGroupName);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Verify the status was updated to DEGRADED_STANDBY_FOR_READER
        Optional<HAGroupStoreRecord> updatedRecordOpt = haGroupStoreManager.getHAGroupStoreRecord(haGroupName);
        assertTrue(updatedRecordOpt.isPresent());
        HAGroupStoreRecord updatedRecord = updatedRecordOpt.get();
        assertEquals(HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY_FOR_READER, updatedRecord.getHAGroupState());

        // Test transition from DEGRADED_STANDBY_FOR_WRITER to DEGRADED_STANDBY
        HAGroupStoreRecord degradedWriterRecord = new HAGroupStoreRecord(
                defaultProtocolVersion, haGroupName, HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY_FOR_WRITER, DEFAULT_RECORD_VERSION);

        haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, degradedWriterRecord, 2);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Call setReaderToDegraded again
        haGroupStoreManager.setReaderToDegraded(haGroupName);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Verify the status was updated to DEGRADED_STANDBY
        updatedRecordOpt = haGroupStoreManager.getHAGroupStoreRecord(haGroupName);
        assertTrue(updatedRecordOpt.isPresent());
        updatedRecord = updatedRecordOpt.get();
        assertEquals(HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY, updatedRecord.getHAGroupState());
    }

    @Test
    public void testSetReaderToHealthy() throws Exception {
        String haGroupName = testName.getMethodName();
        HAGroupStoreManager haGroupStoreManager = HAGroupStoreManager.getInstance(config);

        // Get the record to initialize ZNode from HAGroup so that we can artificially update it via HAAdmin
        Optional<HAGroupStoreRecord> currentRecord = haGroupStoreManager.getHAGroupStoreRecord(haGroupName);
        assertTrue(currentRecord.isPresent());

        // Update the auto-created record to DEGRADED_STANDBY_FOR_READER state for testing
        HAGroupStoreRecord degradedReaderRecord = new HAGroupStoreRecord(
                "1.0", haGroupName, HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY_FOR_READER);

        haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, degradedReaderRecord, 0);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Call setReaderToHealthy
        haGroupStoreManager.setReaderToHealthy(haGroupName);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Verify the status was updated to STANDBY
        Optional<HAGroupStoreRecord> updatedRecordOpt = haGroupStoreManager.getHAGroupStoreRecord(haGroupName);
        assertTrue(updatedRecordOpt.isPresent());
        HAGroupStoreRecord updatedRecord = updatedRecordOpt.get();
        assertEquals(HAGroupStoreRecord.HAGroupState.STANDBY, updatedRecord.getHAGroupState());

        // Test transition from DEGRADED_STANDBY to DEGRADED_STANDBY_FOR_WRITER
        HAGroupStoreRecord degradedRecord = new HAGroupStoreRecord(
                "1.0", haGroupName, HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY);

        haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, degradedRecord, 2);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Call setReaderToHealthy again
        haGroupStoreManager.setReaderToHealthy(haGroupName);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Verify the status was updated to DEGRADED_STANDBY_FOR_WRITER
        updatedRecordOpt = haGroupStoreManager.getHAGroupStoreRecord(haGroupName);
        assertTrue(updatedRecordOpt.isPresent());
        updatedRecord = updatedRecordOpt.get();
        assertEquals(HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY_FOR_WRITER, updatedRecord.getHAGroupState());
    }

    @Test
    public void testReaderStateTransitionInvalidStates() throws Exception {
        String haGroupName = testName.getMethodName();
        HAGroupStoreManager haGroupStoreManager = HAGroupStoreManager.getInstance(config);

        // Get the record to initialize ZNode from HAGroup so that we can artificially update it via HAAdmin
        Optional<HAGroupStoreRecord> currentRecord = haGroupStoreManager.getHAGroupStoreRecord(haGroupName);
        assertTrue(currentRecord.isPresent());

        // Update the auto-created record to ACTIVE_IN_SYNC state (invalid for both operations)
        HAGroupStoreRecord activeRecord = new HAGroupStoreRecord(
                "1.0", haGroupName, HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);

        haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, activeRecord, 0);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Test setReaderToDegraded with invalid state
        try {
            haGroupStoreManager.setReaderToDegraded(haGroupName);
            fail("Expected InvalidClusterRoleTransitionException for setReaderToDegraded with ACTIVE_IN_SYNC state");
        } catch (InvalidClusterRoleTransitionException e) {
            // Expected behavior
            assertTrue("Exception should mention the invalid transition",
                      e.getMessage().contains("ACTIVE_IN_SYNC") && e.getMessage().contains("DEGRADED_STANDBY_FOR_READER"));
        }

        // Test setReaderToHealthy with invalid state
        try {
            haGroupStoreManager.setReaderToHealthy(haGroupName);
            fail("Expected InvalidClusterRoleTransitionException for setReaderToHealthy with ACTIVE_IN_SYNC state");
        } catch (InvalidClusterRoleTransitionException e) {
            // Expected behavior
            assertTrue("Exception should mention the invalid transition",
                      e.getMessage().contains("ACTIVE_IN_SYNC") && e.getMessage().contains("STANDBY"));
        }
    }

}