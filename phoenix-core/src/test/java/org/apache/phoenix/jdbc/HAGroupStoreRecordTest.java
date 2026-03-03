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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.phoenix.util.JacksonUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit test for {@link HAGroupStoreRecord}.
 */
public class HAGroupStoreRecordTest {
  private static final Logger LOG = LoggerFactory.getLogger(HAGroupStoreRecordTest.class);
  private static final String PROTOCOL_VERSION = "1.0";
  private static final String TEST_HDFS_URL = "hdfs://hdfsUrl";
  private static final String TEST_PEER_HDFS_URL = "hdfs://peerHdfsUrl";

  @Rule
  public final TestName testName = new TestName();

  /**
   * Helper method to create a temp JSON file with the given array of HA group store records.
   */
  public static String createJsonFileWithRecords(HAGroupStoreRecord record) throws IOException {
    File file = File.createTempFile("phoenix.ha.group.store.records", ".test.json");
    file.deleteOnExit();
    JacksonUtil.getObjectWriterPretty().writeValue(file, record);
    LOG.info("Prepared the JSON file for testing, file:{}, content:\n{}", file,
      FileUtils.readFileToString(file, "UTF-8"));
    return file.getPath();
  }

  @Test
  public void testReadWriteJsonToFile() throws IOException {
    HAGroupStoreRecord record = getHAGroupStoreRecord(testName.getMethodName(), PROTOCOL_VERSION,
      HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC, HighAvailabilityPolicy.FAILOVER.toString(),
      "peerZKUrl", "clusterUrl", "peerClusterUrl", 0L, TEST_HDFS_URL, TEST_PEER_HDFS_URL);
    String fileName = createJsonFileWithRecords(record);
    String fileContent = FileUtils.readFileToString(new File(fileName), "UTF-8");
    assertTrue(fileContent.contains(record.getHaGroupName()));
    assertTrue(fileContent.contains(record.getProtocolVersion()));
    assertTrue(fileContent.contains(record.getHAGroupState().toString()));
    assertTrue(fileContent.contains(String.valueOf(record.getAdminCRRVersion())));
    // Create a new record from file
    Optional<HAGroupStoreRecord> record2 = HAGroupStoreRecord.fromJson(fileContent.getBytes());
    assertTrue(record2.isPresent());
    // Check if same info
    assertTrue(record.hasSameInfo(record2.get()));
  }

  @Test
  public void testToAndFromJson() throws IOException {
    HAGroupStoreRecord record = getHAGroupStoreRecord(testName.getMethodName(), PROTOCOL_VERSION,
      HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC, HighAvailabilityPolicy.FAILOVER.toString(),
      "peerZKUrl", "clusterUrl", "peerClusterUrl", 0L, TEST_HDFS_URL, TEST_PEER_HDFS_URL);
    byte[] bytes = HAGroupStoreRecord.toJson(record);
    Optional<HAGroupStoreRecord> record2 = HAGroupStoreRecord.fromJson(bytes);
    assertTrue(record2.isPresent());
    assertEquals(record, record2.get());
  }

  @Test
  public void testFromJsonWithNullBytes() {
    Optional<HAGroupStoreRecord> record = HAGroupStoreRecord.fromJson(null);
    assertFalse(record.isPresent());
  }

  @Test
  public void testFromJsonWithInvalidJson() {
    byte[] invalidJson = "invalid json".getBytes();
    Optional<HAGroupStoreRecord> record = HAGroupStoreRecord.fromJson(invalidJson);
    assertFalse(record.isPresent());
  }

  @Test
  public void testHasSameInfo() {
    String haGroupName = testName.getMethodName();
    HAGroupStoreRecord record1 = getHAGroupStoreRecord(haGroupName, PROTOCOL_VERSION,
      HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC, HighAvailabilityPolicy.FAILOVER.toString(),
      "peerZKUrl", "clusterUrl", "peerClusterUrl", 0L, TEST_HDFS_URL, TEST_PEER_HDFS_URL);
    HAGroupStoreRecord record2 = getHAGroupStoreRecord(haGroupName, PROTOCOL_VERSION,
      HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC, HighAvailabilityPolicy.FAILOVER.toString(),
      "peerZKUrl", "clusterUrl", "peerClusterUrl", 0L, TEST_HDFS_URL, TEST_PEER_HDFS_URL);

    assertTrue(record1.hasSameInfo(record2)); // Same core info despite different state
    assertTrue(record1.hasSameInfo(record1)); // reflexive
    assertTrue(record2.hasSameInfo(record1)); // symmetric
  }

  @Test
  public void testHasSameInfoNegative() {
    String haGroupName = testName.getMethodName();
    HAGroupStoreRecord record = getHAGroupStoreRecord(haGroupName, PROTOCOL_VERSION,
      HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC, HighAvailabilityPolicy.FAILOVER.toString(),
      "peerZKUrl", "clusterUrl", "peerClusterUrl", 0L, TEST_HDFS_URL, TEST_PEER_HDFS_URL);

    // Different protocol version
    HAGroupStoreRecord recordDifferentProtocol = getHAGroupStoreRecord(haGroupName, "2.0",
      HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC, HighAvailabilityPolicy.FAILOVER.toString(),
      "peerZKUrl", "clusterUrl", "peerClusterUrl", 0L, TEST_HDFS_URL, TEST_PEER_HDFS_URL);
    assertFalse(record.hasSameInfo(recordDifferentProtocol));
    assertFalse(recordDifferentProtocol.hasSameInfo(record));

    // Different HA group name
    String haGroupName2 = haGroupName + RandomStringUtils.randomAlphabetic(2);
    HAGroupStoreRecord record2 = getHAGroupStoreRecord(haGroupName2, PROTOCOL_VERSION,
      HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC, HighAvailabilityPolicy.FAILOVER.toString(),
      "peerZKUrl", "clusterUrl", "peerClusterUrl", 0L, TEST_HDFS_URL, TEST_PEER_HDFS_URL);
    assertFalse(record.hasSameInfo(record2));
    assertFalse(record2.hasSameInfo(record));

    // Different HA group state
    HAGroupStoreRecord recordDifferentState = getHAGroupStoreRecord(haGroupName, PROTOCOL_VERSION,
      HAGroupStoreRecord.HAGroupState.STANDBY, HighAvailabilityPolicy.FAILOVER.toString(),
      "peerZKUrl", "clusterUrl", "peerClusterUrl", 0L, TEST_HDFS_URL, TEST_PEER_HDFS_URL);
    assertFalse(record.hasSameInfo(recordDifferentState));
    assertFalse(recordDifferentState.hasSameInfo(record));
  }

  @Test
  public void testGetters() {
    String haGroupName = testName.getMethodName();
    String protocolVersion = "1.5";
    HAGroupStoreRecord.HAGroupState haGroupState = HAGroupStoreRecord.HAGroupState.STANDBY;
    HAGroupStoreRecord record = getHAGroupStoreRecord(haGroupName, protocolVersion, haGroupState,
      HighAvailabilityPolicy.FAILOVER.toString(), "peerZKUrl", "clusterUrl", "peerClusterUrl", 0L,
      TEST_HDFS_URL, TEST_PEER_HDFS_URL);

    assertEquals(haGroupName, record.getHaGroupName());
    assertEquals(protocolVersion, record.getProtocolVersion());
    assertEquals(haGroupState, record.getHAGroupState());
    assertEquals(0L, record.getAdminCRRVersion());
    assertEquals(haGroupState.getClusterRole(), record.getClusterRole());
  }

  @Test
  public void testEqualsAndHashCode() {
    String haGroupName = testName.getMethodName();
    HAGroupStoreRecord record1 = getHAGroupStoreRecord(haGroupName, PROTOCOL_VERSION,
      HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC, HighAvailabilityPolicy.FAILOVER.toString(),
      "peerZKUrl", "clusterUrl", "peerClusterUrl", 0L, TEST_HDFS_URL, TEST_PEER_HDFS_URL);
    HAGroupStoreRecord record2 = getHAGroupStoreRecord(haGroupName, PROTOCOL_VERSION,
      HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC, HighAvailabilityPolicy.FAILOVER.toString(),
      "peerZKUrl", "clusterUrl", "peerClusterUrl", 0L, TEST_HDFS_URL, TEST_PEER_HDFS_URL);
    HAGroupStoreRecord record3 = getHAGroupStoreRecord(haGroupName, PROTOCOL_VERSION,
      HAGroupStoreRecord.HAGroupState.STANDBY, HighAvailabilityPolicy.FAILOVER.toString(),
      "peerZKUrl", "clusterUrl", "peerClusterUrl", 0L, TEST_HDFS_URL, TEST_PEER_HDFS_URL); // Different
                                                                                           // state

    // Test equals
    assertEquals(record1, record2); // symmetric
    assertEquals(record2, record1); // symmetric
    assertNotEquals(record1, record3); // different state
    assertNotEquals(null, record1); // null comparison
    assertNotEquals("not a record", record1); // different type

    // Test hashCode
    assertEquals(record1.hashCode(), record2.hashCode()); // equal objects have same hash
    assertNotEquals(record1.hashCode(), record3.hashCode()); // different objects likely have
                                                             // different hash
  }

  @Test
  public void testToString() {
    HAGroupStoreRecord record = getHAGroupStoreRecord(testName.getMethodName(), PROTOCOL_VERSION,
      HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC, HighAvailabilityPolicy.FAILOVER.toString(),
      "peerZKUrl", "clusterUrl", "peerClusterUrl", 0L, TEST_HDFS_URL, TEST_PEER_HDFS_URL);
    String toString = record.toString();

    // Verify all fields are present in toString
    assertTrue(toString.contains(record.getHaGroupName()));
    assertTrue(toString.contains(record.getProtocolVersion()));
    assertTrue(toString.contains(record.getHAGroupState().toString()));
    assertTrue(toString.contains(String.valueOf(record.getAdminCRRVersion())));
  }

  @Test
  public void testToPrettyString() {
    HAGroupStoreRecord record = getHAGroupStoreRecord(testName.getMethodName(), PROTOCOL_VERSION,
      HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC, HighAvailabilityPolicy.FAILOVER.toString(),
      "peerZKUrl", "clusterUrl", "peerClusterUrl", 0L, TEST_HDFS_URL, TEST_PEER_HDFS_URL);
    LOG.info("toString(): {}", record.toString());
    LOG.info("toPrettyString:\n{}", record.toPrettyString());
    assertNotEquals(record.toString(), record.toPrettyString());
    assertTrue(record.toPrettyString().contains(record.getHaGroupName()));
  }

  @Test(expected = NullPointerException.class)
  public void testConstructorWithNullHaGroupName() {
    getHAGroupStoreRecord(null, PROTOCOL_VERSION, HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC,
      HighAvailabilityPolicy.FAILOVER.toString(), "peerZKUrl", "clusterUrl", "peerClusterUrl", 0L,
      TEST_HDFS_URL, TEST_PEER_HDFS_URL);
  }

  @Test(expected = NullPointerException.class)
  public void testConstructorWithNullHAGroupState() {
    getHAGroupStoreRecord(testName.getMethodName(), PROTOCOL_VERSION, null,
      HighAvailabilityPolicy.FAILOVER.toString(), "peerZKUrl", "clusterUrl", "peerClusterUrl", 0L,
      TEST_HDFS_URL, TEST_PEER_HDFS_URL);
  }

  // Tests for HAGroupState enum
  @Test
  public void testHAGroupStateGetClusterRole() {
    // Test that each HAGroupState maps to the correct ClusterRole
    assertEquals(ClusterRoleRecord.ClusterRole.ACTIVE,
      HAGroupStoreRecord.HAGroupState.ABORT_TO_ACTIVE_IN_SYNC.getClusterRole());
    assertEquals(ClusterRoleRecord.ClusterRole.STANDBY,
      HAGroupStoreRecord.HAGroupState.ABORT_TO_STANDBY.getClusterRole());
    assertEquals(ClusterRoleRecord.ClusterRole.ACTIVE,
      HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC.getClusterRole());
    assertEquals(ClusterRoleRecord.ClusterRole.ACTIVE,
      HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC.getClusterRole());
    assertEquals(ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY,
      HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC_TO_STANDBY.getClusterRole());
    assertEquals(ClusterRoleRecord.ClusterRole.ACTIVE,
      HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC_WITH_OFFLINE_PEER.getClusterRole());
    assertEquals(ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY,
      HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY.getClusterRole());
    assertEquals(ClusterRoleRecord.ClusterRole.ACTIVE,
      HAGroupStoreRecord.HAGroupState.ACTIVE_WITH_OFFLINE_PEER.getClusterRole());
    assertEquals(ClusterRoleRecord.ClusterRole.STANDBY,
      HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY.getClusterRole());
    assertEquals(ClusterRoleRecord.ClusterRole.OFFLINE,
      HAGroupStoreRecord.HAGroupState.OFFLINE.getClusterRole());
    assertEquals(ClusterRoleRecord.ClusterRole.STANDBY,
      HAGroupStoreRecord.HAGroupState.STANDBY.getClusterRole());
    assertEquals(ClusterRoleRecord.ClusterRole.STANDBY_TO_ACTIVE,
      HAGroupStoreRecord.HAGroupState.STANDBY_TO_ACTIVE.getClusterRole());
    assertEquals(ClusterRoleRecord.ClusterRole.UNKNOWN,
      HAGroupStoreRecord.HAGroupState.UNKNOWN.getClusterRole());
  }

  @Test
  public void testHAGroupStateValidTransitions() {
    // Test valid transitions for ACTIVE_NOT_IN_SYNC
    assertTrue(HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC
      .isTransitionAllowed(HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC));
    assertTrue(HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC
      .isTransitionAllowed(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC));
    assertTrue(HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC
      .isTransitionAllowed(HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC_TO_STANDBY));
    assertTrue(HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC
      .isTransitionAllowed(HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC_WITH_OFFLINE_PEER));

    // Test valid transitions for ACTIVE
    assertTrue(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC
      .isTransitionAllowed(HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC));
    assertTrue(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC
      .isTransitionAllowed(HAGroupStoreRecord.HAGroupState.ACTIVE_WITH_OFFLINE_PEER));
    assertTrue(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC
      .isTransitionAllowed(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY));

    // Test valid transitions for STANDBY
    assertTrue(HAGroupStoreRecord.HAGroupState.STANDBY
      .isTransitionAllowed(HAGroupStoreRecord.HAGroupState.STANDBY_TO_ACTIVE));

    // Test valid transitions for ACTIVE_TO_STANDBY
    assertTrue(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY
      .isTransitionAllowed(HAGroupStoreRecord.HAGroupState.ABORT_TO_ACTIVE_IN_SYNC));
    assertTrue(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY
      .isTransitionAllowed(HAGroupStoreRecord.HAGroupState.STANDBY));

    // Test valid transitions for STANDBY_TO_ACTIVE
    assertTrue(HAGroupStoreRecord.HAGroupState.STANDBY_TO_ACTIVE
      .isTransitionAllowed(HAGroupStoreRecord.HAGroupState.ABORT_TO_STANDBY));
    assertTrue(HAGroupStoreRecord.HAGroupState.STANDBY_TO_ACTIVE
      .isTransitionAllowed(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC));
  }

  @Test
  public void testHAGroupStateInvalidTransitions() {
    // Test invalid transitions - ACTIVE cannot directly go to STANDBY
    assertFalse(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC
      .isTransitionAllowed(HAGroupStoreRecord.HAGroupState.STANDBY));

    // Test invalid transitions - STANDBY cannot directly go to ACTIVE
    assertFalse(HAGroupStoreRecord.HAGroupState.STANDBY
      .isTransitionAllowed(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC));

    // Test invalid transitions - OFFLINE has no allowed transitions (manual recovery needed)
    assertFalse(HAGroupStoreRecord.HAGroupState.OFFLINE
      .isTransitionAllowed(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC));
    assertFalse(HAGroupStoreRecord.HAGroupState.OFFLINE
      .isTransitionAllowed(HAGroupStoreRecord.HAGroupState.STANDBY));
    assertFalse(HAGroupStoreRecord.HAGroupState.OFFLINE
      .isTransitionAllowed(HAGroupStoreRecord.HAGroupState.UNKNOWN));

    // Test invalid transitions - UNKNOWN has no allowed transitions (manual recovery needed)
    assertFalse(HAGroupStoreRecord.HAGroupState.UNKNOWN
      .isTransitionAllowed(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC));
    assertFalse(HAGroupStoreRecord.HAGroupState.UNKNOWN
      .isTransitionAllowed(HAGroupStoreRecord.HAGroupState.STANDBY));
    assertFalse(HAGroupStoreRecord.HAGroupState.UNKNOWN
      .isTransitionAllowed(HAGroupStoreRecord.HAGroupState.OFFLINE));
  }

  @Test
  public void testHAGroupStateFromBytesValidValues() {
    // Test valid enum names (case insensitive)
    assertEquals(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC,
      HAGroupStoreRecord.HAGroupState.from("ACTIVE_IN_SYNC".getBytes()));
    assertEquals(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC,
      HAGroupStoreRecord.HAGroupState.from("active_in_sync".getBytes()));
    assertEquals(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC,
      HAGroupStoreRecord.HAGroupState.from("Active_in_Sync".getBytes()));

    assertEquals(HAGroupStoreRecord.HAGroupState.STANDBY,
      HAGroupStoreRecord.HAGroupState.from("STANDBY".getBytes()));
    assertEquals(HAGroupStoreRecord.HAGroupState.STANDBY,
      HAGroupStoreRecord.HAGroupState.from("standby".getBytes()));

    assertEquals(HAGroupStoreRecord.HAGroupState.OFFLINE,
      HAGroupStoreRecord.HAGroupState.from("OFFLINE".getBytes()));

    assertEquals(HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC,
      HAGroupStoreRecord.HAGroupState.from("ACTIVE_NOT_IN_SYNC".getBytes()));
  }

  @Test
  public void testHAGroupStateFromBytesInvalidValues() {
    // Test invalid enum names return UNKNOWN
    assertEquals(HAGroupStoreRecord.HAGroupState.UNKNOWN,
      HAGroupStoreRecord.HAGroupState.from("INVALID_STATE".getBytes()));
    assertEquals(HAGroupStoreRecord.HAGroupState.UNKNOWN,
      HAGroupStoreRecord.HAGroupState.from("".getBytes()));
    assertEquals(HAGroupStoreRecord.HAGroupState.UNKNOWN,
      HAGroupStoreRecord.HAGroupState.from("null".getBytes()));
    assertEquals(HAGroupStoreRecord.HAGroupState.UNKNOWN,
      HAGroupStoreRecord.HAGroupState.from("ACTIV".getBytes())); // typo
    assertEquals(HAGroupStoreRecord.HAGroupState.UNKNOWN,
      HAGroupStoreRecord.HAGroupState.from("ACTIVE_EXTRA".getBytes())); // extra text
    assertEquals(HAGroupStoreRecord.HAGroupState.UNKNOWN,
      HAGroupStoreRecord.HAGroupState.from(null));

  }

  // Private Helper Methods
  private HAGroupStoreRecord getHAGroupStoreRecord(String haGroupName, String protocolVersion,
    HAGroupStoreRecord.HAGroupState haGroupState, String policy, String peerZKUrl,
    String clusterUrl, String peerClusterUrl, long adminCRRVersion, String hdfsUrl,
    String peerHdfsUrl) {
    return new HAGroupStoreRecord(protocolVersion, haGroupName, haGroupState, 0L, policy, peerZKUrl,
      clusterUrl, peerClusterUrl, hdfsUrl, peerHdfsUrl, adminCRRVersion);
  }
}
