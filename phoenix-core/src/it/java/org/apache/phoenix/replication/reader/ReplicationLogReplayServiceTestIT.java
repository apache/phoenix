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
package org.apache.phoenix.replication.reader;

import static org.apache.phoenix.jdbc.HAGroupStoreClient.ZK_CONSISTENT_HA_GROUP_RECORD_NAMESPACE;
import static org.apache.phoenix.jdbc.PhoenixHAAdmin.getLocalZkUrl;
import static org.apache.phoenix.jdbc.PhoenixHAAdmin.toPath;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.ClusterRoleRecord;
import org.apache.phoenix.jdbc.HABaseIT;
import org.apache.phoenix.jdbc.HAGroupStoreClient;
import org.apache.phoenix.jdbc.PhoenixHAAdmin;
import org.apache.phoenix.util.HAGroupStoreTestUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.mockito.Mockito;

@Category(NeedsOwnMiniClusterTest.class)
public class ReplicationLogReplayServiceTestIT extends HABaseIT {

  @ClassRule
  public static TemporaryFolder testFolder = new TemporaryFolder();

  private String zkUrl;
  private String peerZkUrl;
  private FileSystem localFs;
  private URI standbyUri;
  private PhoenixHAAdmin haAdmin;
  private PhoenixHAAdmin peerHaAdmin;

  @Rule
  public TestName testName = new TestName();

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    CLUSTERS.start();
  }

  @Before
  public void setUp() throws Exception {
    zkUrl = getLocalZkUrl(conf1);
    peerZkUrl = CLUSTERS.getZkUrl2();
    localFs = FileSystem.getLocal(conf1);
    standbyUri = testFolder.getRoot().toURI();
    haAdmin = new PhoenixHAAdmin(zkUrl, conf1, ZK_CONSISTENT_HA_GROUP_RECORD_NAMESPACE);
    peerHaAdmin = new PhoenixHAAdmin(peerZkUrl, conf2, ZK_CONSISTENT_HA_GROUP_RECORD_NAMESPACE);
    cleanupHAGroupState();

    // Set the required configuration for ReplicationLogReplay
    conf1.set(ReplicationLogReplay.REPLICATION_LOG_REPLAY_HDFS_URL_KEY, standbyUri.toString());
    // Enable replication replay service
    conf1.setBoolean(ReplicationLogReplayService.PHOENIX_REPLICATION_REPLAY_ENABLED, true);

  }

  /**
   * Tests getConsistencyPoint method of ReplicationLogReplayService with multiple HA groups.
   * Verifies that it returns the minimum consistency point across all HA groups.
   */
  @Test
  public void testGetConsistencyPointMultipleGroups() throws IOException, SQLException {
    final String haGroupName1 = testName.getMethodName() + "_1";
    final String haGroupName2 = testName.getMethodName() + "_2";

    // Insert HAGroupStoreRecords into the system table
    HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName1, zkUrl, peerZkUrl,
      CLUSTERS.getMasterAddress1(), CLUSTERS.getMasterAddress2(),
      ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY, null);

    HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName2, zkUrl, peerZkUrl,
      CLUSTERS.getMasterAddress1(), CLUSTERS.getMasterAddress2(),
      ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY, null);

    // Set up consistency points for both groups
    long consistencyPoint1 = 1704153600000L; // 2024-01-02 00:00:00
    long consistencyPoint2 = 1704240000000L; // 2024-01-03 00:00:00

    // Create testable replays with mocked consistency points
    TestableReplicationLogReplay testableReplay1 =
      new TestableReplicationLogReplay(conf1, haGroupName1);
    testableReplay1.setConsistencyPoint(consistencyPoint1);

    TestableReplicationLogReplay testableReplay2 =
      new TestableReplicationLogReplay(conf1, haGroupName2);
    testableReplay2.setConsistencyPoint(consistencyPoint2);

    // Create a spy on ReplicationLogReplayService and mock getReplicationLogReplay
    ReplicationLogReplayService service = ReplicationLogReplayService.getInstance(conf1);
    ReplicationLogReplayService serviceSpy = Mockito.spy(service);

    Mockito.doReturn(testableReplay1).when(serviceSpy).getReplicationLogReplay(haGroupName1);
    Mockito.doReturn(testableReplay2).when(serviceSpy).getReplicationLogReplay(haGroupName2);

    // Call getConsistencyPoint
    long consistencyPoint = serviceSpy.getConsistencyPoint();

    // Should return the minimum consistency point across all groups
    // Since consistencyPoint1 < consistencyPoint2, consistencyPoint should be consistencyPoint1
    assertEquals("Consistency point should be the minimum across all HA groups", consistencyPoint1,
      consistencyPoint);
  }

  /**
   * Tests getConsistencyPoint method of ReplicationLogReplayService with a single HA group.
   * Verifies that it returns the consistency point for that group.
   */
  @Test
  public void testGetConsistencyPointSingleGroup() throws IOException, SQLException {
    final String haGroupName = testName.getMethodName();

    // Insert HAGroupStoreRecord into the system table
    HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName, zkUrl, peerZkUrl,
      CLUSTERS.getMasterAddress1(), CLUSTERS.getMasterAddress2(),
      ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY, null);

    // Set up consistency point for the group
    long consistencyPoint = 1704153600000L; // 2024-01-02 00:00:00

    // Create testable replay with mocked consistency point
    TestableReplicationLogReplay testableReplay =
      new TestableReplicationLogReplay(conf1, haGroupName);
    testableReplay.setConsistencyPoint(consistencyPoint);

    // Create a spy on ReplicationLogReplayService and mock getReplicationLogReplay
    ReplicationLogReplayService service = ReplicationLogReplayService.getInstance(conf1);
    ReplicationLogReplayService serviceSpy = Mockito.spy(service);

    Mockito.doReturn(testableReplay).when(serviceSpy).getReplicationLogReplay(haGroupName);

    // Call getConsistencyPoint
    long result = serviceSpy.getConsistencyPoint();

    // Should return the consistency point for the single group
    // Since we start with currentTime and compare with Math.min, it should return the group's
    // consistency point
    assertEquals("Consistency point should match the single HA group's consistency point",
      consistencyPoint, result);
  }

  /**
   * Tests getConsistencyPoint method when no HA groups exist. Should return currentTime (initial
   * value) when there are no groups.
   */
  @Test
  public void testGetConsistencyPointWithNoGroups() throws IOException, SQLException {
    // Ensure no HA groups exist
    cleanupHAGroupState();

    // Mock current time
    long mockedCurrentTime = 1704153600000L; // 2024-01-02 00:00:00
    EnvironmentEdge edge = () -> mockedCurrentTime;
    EnvironmentEdgeManager.injectEdge(edge);

    try {
      // Get ReplicationLogReplayService instance
      ReplicationLogReplayService service = ReplicationLogReplayService.getInstance(conf1);

      // Call getConsistencyPoint
      long consistencyPoint = service.getConsistencyPoint();

      // Should return mocked currentTime when no groups exist (since it's initialized with
      // currentTime)
      assertEquals("Consistency point should equal mocked currentTime when no HA groups exist",
        mockedCurrentTime, consistencyPoint);
    } finally {
      EnvironmentEdgeManager.reset();
    }
  }

  private void cleanupHAGroupState() throws SQLException {
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
  }

  /**
   * Testable implementation of ReplicationLogReplay for testing. Allows mocking the
   * getConsistencyPoint method via getReplicationReplayLogDiscovery().
   */
  private static class TestableReplicationLogReplay extends ReplicationLogReplay {
    private ReplicationLogDiscoveryReplay mockDiscovery;

    public TestableReplicationLogReplay(org.apache.hadoop.conf.Configuration conf,
      String haGroupName) {
      super(conf, haGroupName);
      // Create a mock discovery that we can configure
      mockDiscovery = Mockito.mock(ReplicationLogDiscoveryReplay.class);
    }

    public void setConsistencyPoint(long consistencyPoint) {
      try {
        when(mockDiscovery.getConsistencyPoint()).thenReturn(consistencyPoint);
      } catch (IOException e) {
        // This shouldn't happen during mock setup, but handle it just in case
        throw new RuntimeException("Failed to set consistency point", e);
      }
    }

    @Override
    protected ReplicationLogDiscoveryReplay getReplicationReplayLogDiscovery() {
      return mockDiscovery;
    }
  }
}
