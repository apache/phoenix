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

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.phoenix.replication.ReplicationLogTracker;
import org.apache.phoenix.replication.ReplicationRound;
import org.apache.phoenix.replication.ReplicationShardDirectoryManager;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogTracker;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogTrackerReplayImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link ReplicationLogDiscoveryReplay#getConsistencyPoint()} focused on
 * round alignment of the minimum IN-PROGRESS timestamp (PHOENIX-7938). Runs entirely on the
 * local filesystem; no mini cluster required.
 */
public class ReplicationLogDiscoveryReplayConsistencyPointTest {

  private static final String HA_GROUP_NAME = "testGroup";
  // Round duration default is 60s = 60000ms; ROUND_START is aligned to a round boundary.
  private static final long ROUND_MILLIS = 60000L;
  private static final long ROUND_START = 1704153600000L; // divisible by 60000

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();

  private Configuration conf;
  private FileSystem localFs;
  private TestableTracker tracker;
  private ReplicationLogDiscoveryReplay discovery;

  @Before
  public void setUp() throws IOException {
    conf = HBaseConfiguration.create();
    localFs = FileSystem.getLocal(conf);
    URI rootURI = new Path(testFolder.getRoot().getAbsolutePath()).toUri();
    Path newFilesDirectory =
      new Path(new Path(rootURI.getPath(), HA_GROUP_NAME), ReplicationLogReplay.IN_DIRECTORY_NAME);
    ReplicationShardDirectoryManager shardManager =
      new ReplicationShardDirectoryManager(conf, localFs, newFilesDirectory);
    MetricsReplicationLogTracker metrics = new MetricsReplicationLogTrackerReplayImpl(HA_GROUP_NAME);
    tracker = new TestableTracker(conf, HA_GROUP_NAME, shardManager, metrics);
    tracker.init();
    discovery = new ReplicationLogDiscoveryReplay(tracker);
  }

  @After
  public void tearDown() throws IOException {
    if (tracker != null) {
      tracker.close();
    }
    localFs.delete(new Path(testFolder.getRoot().toURI()), true);
  }

  /** Creates an empty in-progress log file with the given timestamp encoded in its name. */
  private void createInProgressFile(long timestamp) throws IOException {
    Path inProgressDir = tracker.getInProgressDirPath();
    localFs.mkdirs(inProgressDir);
    localFs.create(new Path(inProgressDir, timestamp + "_rs-1_uuid.plog"), true).close();
  }

  /**
   * Ticket scenario: within round N a later file (T+30s) is moved to IN-PROGRESS while an older
   * sibling (T+5s) is still waiting in the IN directory. The consistency point must align down to
   * the round start (T), not the raw minimum IN-PROGRESS timestamp (T+30s).
   */
  @Test
  public void testSyncStateAlignsMinInProgressTimestampToRoundStart() throws IOException {
    // Only the later file (T+30s) is in IN-PROGRESS here; the older sibling that would still be in
    // the IN directory is not materialized because this unit test asserts purely on the alignment
    // of the raw min IN-PROGRESS timestamp down to the round start (the IT covers the IN sibling).
    createInProgressFile(ROUND_START + 30000L);
    ReplicationRound roundN = new ReplicationRound(ROUND_START, ROUND_START + ROUND_MILLIS);
    discovery.setLastRoundInSync(roundN);
    discovery.setReplicationReplayState(ReplicationLogDiscoveryReplay.ReplicationReplayState.SYNC);

    assertEquals("Consistency point must align to round start, not the raw min in-progress ts",
      ROUND_START, discovery.getConsistencyPoint());
  }

  /** A min IN-PROGRESS timestamp exactly on a round boundary is returned unchanged. */
  @Test
  public void testSyncStateMinOnRoundBoundaryReturnsBoundary() throws IOException {
    createInProgressFile(ROUND_START);
    ReplicationRound roundN = new ReplicationRound(ROUND_START, ROUND_START + ROUND_MILLIS);
    discovery.setLastRoundInSync(roundN);
    discovery.setReplicationReplayState(ReplicationLogDiscoveryReplay.ReplicationReplayState.SYNC);

    assertEquals(ROUND_START, discovery.getConsistencyPoint());
  }

  /**
   * When IN-PROGRESS files span two rounds (e.g. a retried file from round N-1 plus a round-N
   * file), the minimum wins and is aligned to the earlier round's start.
   */
  @Test
  public void testSyncStateMultipleRoundsUsesEarlierRoundStart() throws IOException {
    long earlierRoundStart = ROUND_START - ROUND_MILLIS;
    createInProgressFile(earlierRoundStart + 15000L); // round N-1 (retried file)
    createInProgressFile(ROUND_START + 40000L);       // round N
    ReplicationRound roundN = new ReplicationRound(ROUND_START, ROUND_START + ROUND_MILLIS);
    discovery.setLastRoundInSync(roundN);
    discovery.setReplicationReplayState(ReplicationLogDiscoveryReplay.ReplicationReplayState.SYNC);

    assertEquals(earlierRoundStart, discovery.getConsistencyPoint());
  }

  /** Testable tracker exposing the protected in-progress directory path. */
  private static class TestableTracker extends ReplicationLogTracker {
    TestableTracker(Configuration conf, String haGroupName,
      ReplicationShardDirectoryManager shardManager, MetricsReplicationLogTracker metrics) {
      super(conf, haGroupName, shardManager, metrics);
    }

    @Override
    public Path getInProgressDirPath() {
      return super.getInProgressDirPath();
    }
  }
}
