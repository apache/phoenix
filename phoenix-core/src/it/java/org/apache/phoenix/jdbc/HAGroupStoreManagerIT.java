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
package org.apache.phoenix.jdbc;

import static org.apache.phoenix.jdbc.HAGroupStoreClient.ZK_CONSISTENT_HA_NAMESPACE;
import static org.apache.phoenix.jdbc.PhoenixHAAdmin.getLocalZkUrl;
import static org.apache.phoenix.jdbc.PhoenixHAAdmin.toPath;
import static org.apache.phoenix.query.QueryServices.CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.util.HAGroupStoreTestUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

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
  private static final HighAvailabilityTestingUtility.HBaseTestingUtilityPair CLUSTERS =
    new HighAvailabilityTestingUtility.HBaseTestingUtilityPair();

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
    HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(testName.getMethodName(), zkUrl,
      peerZKUrl, ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY, null);
  }

  @Test
  public void testMutationBlockingWithSingleHAGroup() throws Exception {
    String haGroupName = testName.getMethodName();
    HAGroupStoreManager haGroupStoreManager = HAGroupStoreManager.getInstance(config);

    // Initially no mutation should be blocked
    assertFalse(haGroupStoreManager.isMutationBlocked(haGroupName));

    // Update to ACTIVE_TO_STANDBY role (should block mutations)
    HAGroupStoreRecord transitionRecord = new HAGroupStoreRecord("1.0", haGroupName,
      HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY);

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
    HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName1, zkUrl, this.peerZKUrl,
      ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.ACTIVE, null);
    HAGroupStoreRecord activeRecord1 =
      new HAGroupStoreRecord("1.0", haGroupName1, HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);
    haAdmin.createHAGroupStoreRecordInZooKeeper(activeRecord1);

    HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName2, zkUrl, this.peerZKUrl,
      ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.ACTIVE, null);

    // No mutations should be blocked
    assertFalse(haGroupStoreManager.isMutationBlocked(haGroupName1));
    assertFalse(haGroupStoreManager.isMutationBlocked(haGroupName2));

    // Update only second group to ACTIVE_NOT_IN_SYNC_TO_STANDBY
    HAGroupStoreRecord transitionRecord2 = new HAGroupStoreRecord("1.0", haGroupName2,
      HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC_TO_STANDBY);

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
    Optional<HAGroupStoreRecord> retrievedOpt =
      haGroupStoreManager.getHAGroupStoreRecord(haGroupName);
    assertTrue(retrievedOpt.isPresent());

    // Record for comparison
    HAGroupStoreRecord record = new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION,
      haGroupName, HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC);

    // Complete object comparison instead of field-by-field
    assertEquals(record, retrievedOpt.get());
  }

  @Test
  public void testInvalidateHAGroupStoreClient() throws Exception {
    String haGroupName = testName.getMethodName();
    HAGroupStoreManager haGroupStoreManager = HAGroupStoreManager.getInstance(config);

    // Create a HAGroupStoreRecord first
    HAGroupStoreRecord record =
      new HAGroupStoreRecord("1.0", haGroupName, HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);

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

    HAGroupStoreManager haGroupStoreManager = HAGroupStoreManager.getInstance(config);

    // Create HAGroupStoreRecord with ACTIVE_TO_STANDBY role
    HAGroupStoreRecord transitionRecord = new HAGroupStoreRecord("1.0", haGroupName,
      HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY);

    haAdmin.createHAGroupStoreRecordInZooKeeper(transitionRecord);
    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

    // Mutations should not be blocked even with ACTIVE_TO_STANDBY role
    assertFalse(haGroupStoreManager.isMutationBlocked(haGroupName));
  }

  @Test
  public void testSetHAGroupStatusToStoreAndForward() throws Exception {
    String haGroupName = testName.getMethodName();
    HAGroupStoreManager haGroupStoreManager = HAGroupStoreManager.getInstance(config);

    // Create an initial HAGroupStoreRecord with ACTIVE status
    HAGroupStoreRecord initialRecord =
      new HAGroupStoreRecord("1.0", haGroupName, HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);

    haAdmin.createHAGroupStoreRecordInZooKeeper(initialRecord);
    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

    // Set the HA group status to store and forward (ACTIVE_NOT_IN_SYNC)
    haGroupStoreManager.setHAGroupStatusToStoreAndForward(haGroupName);
    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

    // Verify the status was updated to ACTIVE_NOT_IN_SYNC
    Optional<HAGroupStoreRecord> updatedRecordOpt =
      haGroupStoreManager.getHAGroupStoreRecord(haGroupName);
    assertTrue(updatedRecordOpt.isPresent());
    HAGroupStoreRecord updatedRecord = updatedRecordOpt.get();
    assertEquals(HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC,
      updatedRecord.getHAGroupState());
  }

  @Test
  public void testSetHAGroupStatusRecordToSync() throws Exception {
    String haGroupName = testName.getMethodName();
    HAGroupStoreManager haGroupStoreManager = HAGroupStoreManager.getInstance(config);

    // Create an initial HAGroupStoreRecord with ACTIVE_NOT_IN_SYNC status
    HAGroupStoreRecord initialRecord = new HAGroupStoreRecord("1.0", haGroupName,
      HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC);

    haAdmin.createHAGroupStoreRecordInZooKeeper(initialRecord);
    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

    // Set the HA group status to sync (ACTIVE)
    haGroupStoreManager.setHAGroupStatusRecordToSync(haGroupName);
    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

    // Verify the status was updated to ACTIVE
    Optional<HAGroupStoreRecord> updatedRecordOpt =
      haGroupStoreManager.getHAGroupStoreRecord(haGroupName);
    assertTrue(updatedRecordOpt.isPresent());
    HAGroupStoreRecord updatedRecord = updatedRecordOpt.get();
    assertEquals(ClusterRoleRecord.ClusterRole.ACTIVE, updatedRecord.getClusterRole());
  }

  @Test
  public void testGetHAGroupNamesFiltersCorrectlyByZkUrl() throws Exception {
    HAGroupStoreManager haGroupStoreManager = HAGroupStoreManager.getInstance(config);

    List<String> initialHAGroupNames = haGroupStoreManager.getHAGroupNames();

    // Create HA groups with current zkUrl as ZK_URL_1
    String haGroupWithCurrentZkUrl = testName.getMethodName() + "_current_zk";
    HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupWithCurrentZkUrl, zkUrl,
      this.peerZKUrl, ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY,
      null);

    // Create HA group with current zkUrl as ZK_URL_2 (swapped)
    String haGroupWithCurrentZkUrlAsPeer = testName.getMethodName() + "_current_as_peer";
    HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupWithCurrentZkUrlAsPeer,
      this.peerZKUrl, zkUrl, ClusterRoleRecord.ClusterRole.STANDBY,
      ClusterRoleRecord.ClusterRole.ACTIVE, zkUrl);

    // Create HA group with different zkUrl (should not appear in results)
    String differentZkUrl = "localhost:2182:/different";
    String haGroupWithDifferentZkUrl = testName.getMethodName() + "_different_zk";
    HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupWithDifferentZkUrl, differentZkUrl,
      "localhost:2183:/other", ClusterRoleRecord.ClusterRole.ACTIVE,
      ClusterRoleRecord.ClusterRole.STANDBY, zkUrl);

    // Get HA group names - should only return groups where current zkUrl matches ZK_URL_1 or
    // ZK_URL_2
    List<String> filteredHAGroupNames = haGroupStoreManager.getHAGroupNames();

    // Extract only new groups from filteredHAGroupNames
    List<String> newHAGroupNames = filteredHAGroupNames.stream()
      .filter(name -> !initialHAGroupNames.contains(name)).collect(Collectors.toList());

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
    HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupWithDifferentZkUrls,
      differentZkUrl1, differentZkUrl2, ClusterRoleRecord.ClusterRole.ACTIVE,
      ClusterRoleRecord.ClusterRole.STANDBY, zkUrl);

    // Get HA group names - should not contain the group with different zkUrls
    List<String> filteredHAGroupNames = haGroupStoreManager.getHAGroupNames();

    // Should NOT contain the HA group with different zkUrls
    assertFalse("Should NOT contain HA group with different zkUrls",
      filteredHAGroupNames.contains(haGroupWithDifferentZkUrls));

    // Clean up
    HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(haGroupWithDifferentZkUrls, zkUrl);

  }

}
