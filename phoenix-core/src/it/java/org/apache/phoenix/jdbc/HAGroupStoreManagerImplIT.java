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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.phoenix.jdbc.HAGroupStoreClient.ZK_CONSISTENT_HA_NAMESPACE;
import static org.apache.phoenix.jdbc.PhoenixHAAdmin.getLocalZkUrl;
import static org.apache.phoenix.jdbc.PhoenixHAAdmin.toPath;
import static org.apache.phoenix.query.QueryServices.CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Integration tests for {@link HAGroupStoreManagerImpl}.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class HAGroupStoreManagerImplIT extends BaseTest {

    @Rule
    public TestName testName = new TestName();

    private PhoenixHAAdmin haAdmin;
    private static final Long ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS = 2000L;

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(2);
        props.put(CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED, "true");
        props.put(QueryServices.HA_GROUP_STORE_MANAGER_IMPL_CLASS, org.apache.phoenix.jdbc.HAGroupStoreManagerImpl.class.getName());
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Before
    public void before() throws Exception {
        haAdmin = new PhoenixHAAdmin(config, ZK_CONSISTENT_HA_NAMESPACE);

        // Clean up existing HAGroupStoreRecords
        try {
            List<String> haGroupNames = HAGroupStoreClient.getHAGroupNames(config);
            for (String haGroupName : haGroupNames) {
                haAdmin.getCurator().delete().quietly().forPath(toPath(haGroupName));
            }
        } catch (Exception e) {
            // Ignore cleanup errors
        }
    }

    @Test
    public void testHAGroupStoreManagerCreation() throws Exception {
        Optional<HAGroupStoreManager> managerOpt = HAGroupStoreManagerFactory.getInstance(config);

        assertTrue(managerOpt.isPresent());
        assertTrue(managerOpt.get() instanceof HAGroupStoreManagerImpl);
    }

    @Test
    public void testMutationBlockingWithSingleHAGroup() throws Exception {
        String haGroupName = testName.getMethodName();
        Optional<HAGroupStoreManager> managerOpt = HAGroupStoreManagerFactory.getInstance(config);
        assertTrue(managerOpt.isPresent());
        HAGroupStoreManager haGroupStoreManager = managerOpt.get();

        // Initially no mutation should be blocked
        assertFalse(haGroupStoreManager.isMutationBlocked(config));
        assertFalse(haGroupStoreManager.isMutationBlocked(config, haGroupName));

        // Create HAGroupStoreRecord with ACTIVE role
        HAGroupStoreRecord activeRecord = new HAGroupStoreRecord(
                "1.0", haGroupName, ClusterRoleRecord.ClusterRole.ACTIVE,
                1, "FAILOVER", System.currentTimeMillis(), "peer1:2181,peer2:2181");

        haAdmin.createHAGroupStoreRecordInZooKeeper(activeRecord);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Still no mutation blocking with ACTIVE role
        assertFalse(haGroupStoreManager.isMutationBlocked(config));
        assertFalse(haGroupStoreManager.isMutationBlocked(config, haGroupName));

        // Update to ACTIVE_TO_STANDBY role (should block mutations)
        HAGroupStoreRecord transitionRecord = new HAGroupStoreRecord(
                "1.0", haGroupName, ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY,
                2, "FAILOVER", System.currentTimeMillis(), "peer1:2181,peer2:2181");

        haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, transitionRecord, 0);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Now mutations should be blocked
        assertTrue(haGroupStoreManager.isMutationBlocked(config));
        assertTrue(haGroupStoreManager.isMutationBlocked(config, haGroupName));
    }

    @Test
    public void testMutationBlockingWithMultipleHAGroups() throws Exception {
        String haGroupName1 = testName.getMethodName() + "_1";
        String haGroupName2 = testName.getMethodName() + "_2";
        Optional<HAGroupStoreManager> managerOpt = HAGroupStoreManagerFactory.getInstance(config);
        assertTrue(managerOpt.isPresent());
        HAGroupStoreManager haGroupStoreManager = managerOpt.get();

        // Create two HA groups with ACTIVE and ACTIVE_NOT_IN_SYNC roles
        HAGroupStoreRecord activeRecord1 = new HAGroupStoreRecord(
                "1.0", haGroupName1, ClusterRoleRecord.ClusterRole.ACTIVE,
                1, "FAILOVER", System.currentTimeMillis(), "peer1:2181");

        HAGroupStoreRecord activeRecord2 = new HAGroupStoreRecord(
                "1.0", haGroupName2, ClusterRoleRecord.ClusterRole.ACTIVE_NOT_IN_SYNC,
                1, "PARALLEL", System.currentTimeMillis(), "peer2:2181");

        haAdmin.createHAGroupStoreRecordInZooKeeper(activeRecord1);
        haAdmin.createHAGroupStoreRecordInZooKeeper(activeRecord2);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // No mutations should be blocked
        assertFalse(haGroupStoreManager.isMutationBlocked(config));
        assertFalse(haGroupStoreManager.isMutationBlocked(config, haGroupName1));
        assertFalse(haGroupStoreManager.isMutationBlocked(config, haGroupName2));

        // Update only second group to ACTIVE_NOT_IN_SYNC_TO_STANDBY
        HAGroupStoreRecord transitionRecord2 = new HAGroupStoreRecord(
                "1.0", haGroupName2, ClusterRoleRecord.ClusterRole.ACTIVE_NOT_IN_SYNC_TO_STANDBY,
                2, "PARALLEL", System.currentTimeMillis(), "peer2:2181");

        haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName2, transitionRecord2, 0);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Global mutations should be blocked due to second group
        assertTrue(haGroupStoreManager.isMutationBlocked(config));
        assertFalse(haGroupStoreManager.isMutationBlocked(config, haGroupName1));
        assertTrue(haGroupStoreManager.isMutationBlocked(config, haGroupName2));
    }

    @Test
    public void testHAGroupStoreRecord() throws Exception {
        String haGroupName = testName.getMethodName();
        Optional<HAGroupStoreManager> managerOpt = HAGroupStoreManagerFactory.getInstance(config);
        assertTrue(managerOpt.isPresent());
        HAGroupStoreManager haGroupStoreManager = managerOpt.get();

        // Test getting non-existent record
        Optional<HAGroupStoreRecord> recordOpt = haGroupStoreManager.getHAGroupStoreRecord(config, haGroupName);
        assertFalse(recordOpt.isPresent());

        long fixedTimestamp = 1640995200000L; // Fixed timestamp for consistent comparison

        // Create sample record
        HAGroupStoreRecord record = new HAGroupStoreRecord(
                "1.0", haGroupName, ClusterRoleRecord.ClusterRole.ACTIVE,
                1, "FAILOVER", fixedTimestamp, "peer1:2181,peer2:2181");

        // Store record, then retrieve and compare
        haAdmin.createHAGroupStoreRecordInZooKeeper(record);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        Optional<HAGroupStoreRecord> retrievedOpt = haGroupStoreManager.getHAGroupStoreRecord(config, haGroupName);
        assertTrue(retrievedOpt.isPresent());

        HAGroupStoreRecord retrieved = retrievedOpt.get();

        // Complete object comparison instead of field-by-field
        assertEquals(record, retrieved);
    }

    @Test
    public void testInvalidateHAGroupStoreClient() throws Exception {
        String haGroupName = testName.getMethodName();
        Optional<HAGroupStoreManager> managerOpt = HAGroupStoreManagerFactory.getInstance(config);
        assertTrue(managerOpt.isPresent());
        HAGroupStoreManager haGroupStoreManager = managerOpt.get();

        // Create a HAGroupStoreRecord first
        HAGroupStoreRecord record = new HAGroupStoreRecord(
                "1.0", haGroupName, ClusterRoleRecord.ClusterRole.ACTIVE,
                1, "FAILOVER", System.currentTimeMillis(), null);

        haAdmin.createHAGroupStoreRecordInZooKeeper(record);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Ensure we can get the record
        Optional<HAGroupStoreRecord> recordOpt = haGroupStoreManager.getHAGroupStoreRecord(config, haGroupName);
        assertTrue(recordOpt.isPresent());

        // Invalidate the specific HA group client
        haGroupStoreManager.invalidateHAGroupStoreClient(config, haGroupName);

        // Should still be able to get the record after invalidation
        recordOpt = haGroupStoreManager.getHAGroupStoreRecord(config, haGroupName);
        assertTrue(recordOpt.isPresent());

        // Test global invalidation
        haGroupStoreManager.invalidateHAGroupStoreClient(config);

        // Should still be able to get the record after global invalidation
        recordOpt = haGroupStoreManager.getHAGroupStoreRecord(config, haGroupName);
        assertTrue(recordOpt.isPresent());
    }

    @Test
    public void testMutationBlockDisabled() throws Exception {
        String haGroupName = testName.getMethodName();
        // Create configuration with mutation block disabled
        Configuration conf = new Configuration();
        conf.set(CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED, "false");
        conf.set(QueryServices.HA_GROUP_STORE_MANAGER_IMPL_CLASS, org.apache.phoenix.jdbc.HAGroupStoreManagerImpl.class.getName());
        conf.set(HConstants.ZOOKEEPER_QUORUM, getLocalZkUrl(config));


        Optional<HAGroupStoreManager> managerOpt = HAGroupStoreManagerFactory.getInstance(conf);
        assertTrue(managerOpt.isPresent());
        HAGroupStoreManager haGroupStoreManager = managerOpt.get();

        // Create HAGroupStoreRecord with ACTIVE_TO_STANDBY role
        HAGroupStoreRecord transitionRecord = new HAGroupStoreRecord(
                "1.0", haGroupName, ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY,
                1, "FAILOVER", System.currentTimeMillis(), "peer1:2181");

        haAdmin.createHAGroupStoreRecordInZooKeeper(transitionRecord);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Mutations should not be blocked even with ACTIVE_TO_STANDBY role
        assertFalse(haGroupStoreManager.isMutationBlocked(config));
        assertFalse(haGroupStoreManager.isMutationBlocked(config, haGroupName));
    }

    @Test
    public void testSetHAGroupStatusToStoreAndForward() throws Exception {
        String haGroupName = testName.getMethodName();
        Optional<HAGroupStoreManager> managerOpt = HAGroupStoreManagerFactory.getInstance(config);
        assertTrue(managerOpt.isPresent());
        HAGroupStoreManager haGroupStoreManager = managerOpt.get();

        // Create an initial HAGroupStoreRecord with ACTIVE status
        HAGroupStoreRecord initialRecord = new HAGroupStoreRecord(
                "1.0", haGroupName, ClusterRoleRecord.ClusterRole.ACTIVE,
                1, "FAILOVER", System.currentTimeMillis(), "peer1:2181");

        haAdmin.createHAGroupStoreRecordInZooKeeper(initialRecord);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Set the HA group status to store and forward (ACTIVE_NOT_IN_SYNC)
        haGroupStoreManager.setHAGroupStatusToStoreAndForward(config, haGroupName);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Verify the status was updated to ACTIVE_NOT_IN_SYNC
        Optional<HAGroupStoreRecord> updatedRecordOpt = haGroupStoreManager.getHAGroupStoreRecord(config, haGroupName);
        assertTrue(updatedRecordOpt.isPresent());
        HAGroupStoreRecord updatedRecord = updatedRecordOpt.get();
        assertEquals(ClusterRoleRecord.ClusterRole.ACTIVE_NOT_IN_SYNC, updatedRecord.getClusterRole());
        assertEquals(2, updatedRecord.getVersion()); // Version should be incremented
    }

    @Test
    public void testSetHAGroupStatusRecordToSync() throws Exception {
        String haGroupName = testName.getMethodName();
        Optional<HAGroupStoreManager> managerOpt = HAGroupStoreManagerFactory.getInstance(config);
        assertTrue(managerOpt.isPresent());
        HAGroupStoreManager haGroupStoreManager = managerOpt.get();

        // Create an initial HAGroupStoreRecord with ACTIVE_NOT_IN_SYNC status
        HAGroupStoreRecord initialRecord = new HAGroupStoreRecord(
                "1.0", haGroupName, ClusterRoleRecord.ClusterRole.ACTIVE_NOT_IN_SYNC,
                1, "FAILOVER", System.currentTimeMillis(), "peer1:2181");

        haAdmin.createHAGroupStoreRecordInZooKeeper(initialRecord);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Set the HA group status to sync (ACTIVE)
        haGroupStoreManager.setHAGroupStatusRecordToSync(config, haGroupName);
        Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

        // Verify the status was updated to ACTIVE
        Optional<HAGroupStoreRecord> updatedRecordOpt = haGroupStoreManager.getHAGroupStoreRecord(config, haGroupName);
        assertTrue(updatedRecordOpt.isPresent());
        HAGroupStoreRecord updatedRecord = updatedRecordOpt.get();
        assertEquals(ClusterRoleRecord.ClusterRole.ACTIVE, updatedRecord.getClusterRole());
        assertEquals(2, updatedRecord.getVersion()); // Version should be incremented
    }

    @Test
    public void testSetHAGroupStatusMethodsWithNonExistentGroup() throws Exception {
        String nonExistentGroupName = testName.getMethodName() + "_nonexistent";
        Optional<HAGroupStoreManager> managerOpt = HAGroupStoreManagerFactory.getInstance(config);
        assertTrue(managerOpt.isPresent());
        HAGroupStoreManager haGroupStoreManager = managerOpt.get();

        // Test setHAGroupStatusToStoreAndForward with non-existent group
        try {
            haGroupStoreManager.setHAGroupStatusToStoreAndForward(config, nonExistentGroupName);
            fail("Expected IOException to be thrown");
        } catch (IOException e) {
            assertEquals("HAGroupStoreRecord not found in system table for HA group " + nonExistentGroupName, e.getMessage());
        }

        // Test setHAGroupStatusRecordToSync with non-existent group
        try {
            haGroupStoreManager.setHAGroupStatusRecordToSync(config, nonExistentGroupName);
            fail("Expected IOException to be thrown");
        } catch (IOException e) {
            assertEquals("HAGroupStoreRecord not found in system table for HA group " + nonExistentGroupName, e.getMessage());
        }
    }
}