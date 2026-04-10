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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.sql.SQLException;
import org.apache.hadoop.fs.Path;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.ClusterRoleRecord;
import org.apache.phoenix.jdbc.HABaseIT;
import org.apache.phoenix.util.HAGroupStoreTestUtil;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category(NeedsOwnMiniClusterTest.class)
public class ReplicationLogReplayTestIT extends HABaseIT {
  private String zkUrl;
  private String peerZkUrl;

  @Rule
  public TestName testName = new TestName();

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    CLUSTERS.start();
  }

  @Before
  public void setUp() throws Exception {
    zkUrl = CLUSTERS.getZkUrl1();
    peerZkUrl = CLUSTERS.getZkUrl2();
  }

  @Test
  public void testInit() throws Exception {
    final String haGroupName = testName.getMethodName();

    // Create TestableReplicationReplay instance
    ReplicationLogReplay replicationLogReplay = new ReplicationLogReplay(conf1, haGroupName);

    // Insert a HAGroupStoreRecord into the system table
    HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName, zkUrl, peerZkUrl,
      CLUSTERS.getMasterAddress1(), CLUSTERS.getMasterAddress2(),
      ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY, null,
      CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2());

    URI rootURI = new URI(CLUSTERS.getHdfsUrl1());
    // Call init method
    replicationLogReplay.init();

    // 1. Ensure filesystem and rootURI are initialized correctly
    assertNotNull("FileSystem should be initialized", replicationLogReplay.getFileSystem());
    assertNotNull("RootURI should be initialized", replicationLogReplay.getRootURI());
    assertEquals("RootURI should match the configured URI", rootURI,
      replicationLogReplay.getRootURI());

    // 2. Ensure expected haGroupFilesPath is created
    Path expectedHaGroupFilesPath = new Path(rootURI.getPath(), haGroupName);
    assertTrue("HA group files path should be created",
      replicationLogReplay.getFileSystem().exists(expectedHaGroupFilesPath));

    // 3. Ensure replicationReplayLogDiscovery is initialized correctly
    assertNotNull("ReplicationLogDiscoveryReplay should be initialized",
      replicationLogReplay.getReplicationReplayLogDiscovery());
  }

  @Test
  public void testReplicationReplayInstanceCaching() throws SQLException {
    final String haGroupName1 = "testHAGroup_1";
    final String haGroupName2 = "testHAGroup_2";

    HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName1, zkUrl, peerZkUrl,
      CLUSTERS.getMasterAddress1(), CLUSTERS.getMasterAddress2(),
      ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY, null,
      CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2());

    HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName2, zkUrl, peerZkUrl,
      CLUSTERS.getMasterAddress1(), CLUSTERS.getMasterAddress2(),
      ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY, null,
      CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2());

    // Get instances for the first HA group
    ReplicationLogReplay group1Instance1 = ReplicationLogReplay.get(conf1, haGroupName1);
    ReplicationLogReplay group1Instance2 = ReplicationLogReplay.get(conf1, haGroupName1);

    // Verify same instance is returned for same haGroupName
    assertNotNull("ReplicationLogReplay should not be null", group1Instance1);
    assertNotNull("ReplicationLogReplay should not be null", group1Instance2);
    assertSame("Same instance should be returned for same haGroup", group1Instance1,
      group1Instance2);

    // Get instance for a different HA group
    ReplicationLogReplay group2Instance1 = ReplicationLogReplay.get(conf1, haGroupName2);
    assertNotNull("ReplicationLogReplay should not be null", group2Instance1);
    assertNotSame("Different instance should be returned for different haGroup", group2Instance1,
      group1Instance1);

    // Verify multiple calls still return cached instances
    ReplicationLogReplay group1Instance3 = ReplicationLogReplay.get(conf1, haGroupName1);
    ReplicationLogReplay group2Instance2 = ReplicationLogReplay.get(conf1, haGroupName2);
    assertSame("Cached instance should be returned", group1Instance3, group1Instance1);
    assertSame("Cached instance should be returned", group2Instance2, group2Instance1);
  }

  @Test
  public void testReplicationReplayCacheRemovalOnClose() throws SQLException {
    final String haGroupName = testName.getMethodName();

    HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName, zkUrl, peerZkUrl,
      CLUSTERS.getMasterAddress1(), CLUSTERS.getMasterAddress2(),
      ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY, null,
      CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2());

    // Get initial instance
    ReplicationLogReplay group1Instance1 = ReplicationLogReplay.get(conf1, haGroupName);
    assertNotNull("ReplicationLogReplay should not be null", group1Instance1);

    // Verify cached instance is returned
    ReplicationLogReplay group1Instance2 = ReplicationLogReplay.get(conf1, haGroupName);
    assertSame("Same instance should be returned before close", group1Instance2, group1Instance1);

    // Close the replay instance
    group1Instance1.close();

    // Get instance after close - should be a new instance
    ReplicationLogReplay group1Instance3 = ReplicationLogReplay.get(conf1, haGroupName);
    assertNotNull("ReplicationLogReplay should not be null after close", group1Instance3);
    assertNotSame("New instance should be created after close", group1Instance1, group1Instance3);
    assertEquals("HA Group ID should match", haGroupName, group1Instance3.getHaGroupName());

    // Clean up
    group1Instance3.close();
  }
}
