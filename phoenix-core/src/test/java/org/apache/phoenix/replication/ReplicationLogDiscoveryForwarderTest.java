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
package org.apache.phoenix.replication;

import static org.apache.phoenix.replication.ReplicationLogGroup.ReplicationMode.STORE_AND_FORWARD;
import static org.apache.phoenix.replication.ReplicationLogGroup.ReplicationMode.SYNC;
import static org.apache.phoenix.replication.ReplicationShardDirectoryManager.PHOENIX_REPLICATION_ROUND_DURATION_SECONDS_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;

import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.phoenix.jdbc.HAGroupStoreRecord.HAGroupState;
import org.apache.phoenix.replication.ReplicationLogGroup.ReplicationMode;
import org.apache.phoenix.replication.log.LogFileTestUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationLogDiscoveryForwarderTest extends ReplicationLogBaseTest {
  private static final Logger LOG =
    LoggerFactory.getLogger(ReplicationLogDiscoveryForwarderTest.class);

  public ReplicationLogDiscoveryForwarderTest() {
    // we want to start in STORE_AND_FORWARD mode
    super(HAGroupState.ACTIVE_NOT_IN_SYNC);
  }

  @Override
  protected void overrideConf(Configuration conf) {
    conf.setInt(PHOENIX_REPLICATION_ROUND_DURATION_SECONDS_KEY, 20);
  }

  @Override
  protected boolean useAlignedRotation() {
    return true;
  }

  @Before
  public void setUp() throws IOException {
    ReplicationMode mode = logGroup.getMode();
    Assert.assertTrue(mode.equals(STORE_AND_FORWARD));
  }

  @Test
  public void testLogForwardingAndTransitionBackToSyncMode() throws Exception {
    final String tableName = "TESTTBL";
    final long count = 100L;
    int roundDurationSeconds = logGroup.getLocalShardManager().getReplicationRoundDurationSeconds();

    doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) {
        // explicitly set the replication mode to SYNC
        logGroup.setMode(SYNC);
        try {
          logGroup.sync();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        return 0L;
      }
    }).when(haGroupStoreManager).setHAGroupStatusToSync(haGroupName);

    for (long id = 1; id <= count; ++id) {
      Mutation put = LogFileTestUtil.newPut("row_" + id, id, 2);
      logGroup.append(tableName, id, put);
    }
    logGroup.sync();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<Boolean> future = executor.submit(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          ReplicationLogTracker logTracker = logGroup.getLogForwarder().getReplicationLogTracker();
          while (true) {
            try {
              if (Thread.currentThread().isInterrupted()) {
                LOG.info("Task interrupted, exiting");
                return false;
              }
              int newFileCount = logTracker.getNewFiles().size();
              int inProgressCount = logTracker.getInProgressFiles().size();
              if (newFileCount == 0 && inProgressCount == 0) {
                // wait for the mode transition to finish
                Thread.sleep(2000);
                LOG.info("All files processed");
                return true;
              }
              LOG.info("New files = {} In-progress files = {}", newFileCount, inProgressCount);
              Thread.sleep(roundDurationSeconds * 1000);
            } catch (InterruptedException e) {
              LOG.info("Task received InterruptedException, exiting");
              Thread.currentThread().interrupt(); // Re-interrupt the thread
              return false;
            }
          }
        }
      });
      try {
        Boolean ret = future.get(120, TimeUnit.SECONDS);
        assertTrue(ret);
        // we should have switched back to the SYNC mode
        assertEquals(SYNC, logGroup.getMode());
        // the log forwarder should not be running since we are in SYNC mode
        assertFalse(logGroup.getLogForwarder().isRunning);
      } catch (TimeoutException e) {
        LOG.info("Task timed out, cancelling it");
        future.cancel(true);
        fail("Task timed out");
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("Task failed", e);
        fail("Task failed");
      }
    } finally {
      executor.shutdownNow();
    }
  }

  /**
   * Two source files that share a timestamp but originate from different RegionServers must forward
   * to two distinct destinations on the peer. The forwarder keys the dst on the origin writer
   * identity; keying it on the forwarding RS instead would collapse both onto one dst and reject
   * the second with a PathExistsException, wedging SYNC re-entry.
   */
  @Test
  public void testForwardPreservesOriginServerIdentity() throws Exception {
    ReplicationLogDiscoveryForwarder forwarder = logGroup.getLogForwarder();
    ReplicationLogTracker localTracker = forwarder.getReplicationLogTracker();
    ReplicationShardDirectoryManager localShardManager = logGroup.getLocalShardManager();
    ReplicationShardDirectoryManager peerShardManager = logGroup.getOrCreatePeerShardManager();

    // Same timestamp, different origin servers -- the collision scenario
    long ts = EnvironmentEdgeManager.currentTimeMillis();
    String originA = "10.244.1.10,16020,1784436416001";
    String originB = "10.244.2.10,16020,1784436416002";

    Path shardDir = localShardManager.getShardDirectory(ts);
    localFs.mkdirs(shardDir);
    Path srcA =
      markInProgressSource(localTracker, new Path(shardDir, ts + "_" + originA + ".plog"));
    Path srcB =
      markInProgressSource(localTracker, new Path(shardDir, ts + "_" + originB + ".plog"));

    forwarder.processFile(srcA);
    forwarder.processFile(srcB);

    // Both files must land on the peer under their own origin identity -- no collision.
    Path peerShardDir = peerShardManager.getShardDirectory(ts);
    FileSystem peerFs = peerShardManager.getFileSystem();
    Set<String> peerNames = new HashSet<>();
    for (FileStatus s : peerFs.listStatus(peerShardDir)) {
      peerNames.add(s.getPath().getName());
    }
    assertEquals("Both origin files should be forwarded to distinct destinations", 2,
      peerNames.size());
    assertTrue("Peer should contain the file from originA",
      peerNames.contains(ts + "_" + originA + ".plog"));
    assertTrue("Peer should contain the file from originB",
      peerNames.contains(ts + "_" + originB + ".plog"));
  }

  /** Creates an empty source file and moves it to the in-progress dir, as the forwarder expects. */
  private Path markInProgressSource(ReplicationLogTracker tracker, Path file) throws IOException {
    localFs.create(file, true).close();
    Optional<Path> inProgress = tracker.markInProgress(file);
    assertTrue("markInProgress should succeed", inProgress.isPresent());
    return inProgress.get();
  }

  @Test
  public void testSyncModeUpdateWaitTime() throws Exception {
    final long[] waitTime = { 8L };

    doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) {
        long ret = 0L;
        if (waitTime[0] > 0) {
          ret = waitTime[0];
          // reset to 0
          waitTime[0] = 0;
        } else {
          // explicitly set the replication mode to SYNC
          logGroup.setMode(SYNC);
          try {
            logGroup.sync();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
        return ret;
      }
    }).when(haGroupStoreManager).setHAGroupStatusToSync(haGroupName);

    long deadline = EnvironmentEdgeManager.currentTimeMillis() + 120_000;
    while (logGroup.getMode() != SYNC && EnvironmentEdgeManager.currentTimeMillis() < deadline) {
      Thread.sleep(500);
    }
    assertEquals(SYNC, logGroup.getMode());
  }

  /**
   * Tests that the forwarder retries peer shard manager creation when the peer is initially
   * unavailable. On the first attempt, getOrCreatePeerShardManager throws; the file is marked
   * failed and retried via in-progress processing. On the retry the peer becomes available and
   * forwarding succeeds.
   */
  @Test
  public void testForwarderRetriesPeerCreation() throws Exception {
    final String tableName = "TBLFWDRETRY";
    final long count = 10L;

    // Ensure in-progress files are immediately eligible for retry and always processed
    conf.setInt(ReplicationLogDiscovery.REPLICATION_IN_PROGRESS_FILE_MIN_AGE_SECONDS_KEY, 0);
    conf.setDouble(
      ReplicationLogDiscoveryForwarder.REPLICATION_FORWARDER_IN_PROGRESS_PROCESSING_PROBABILITY_KEY,
      100.0);
    // Recreate the log group with the updated config
    recreateLogGroup();
    assertEquals(STORE_AND_FORWARD, logGroup.getMode());

    // Make getOrCreatePeerShardManager fail on the first call, then succeed on subsequent calls
    doThrow(new IOException("Peer namenode unavailable")).doCallRealMethod().when(logGroup)
      .getOrCreatePeerShardManager();

    doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) {
        logGroup.setMode(SYNC);
        try {
          logGroup.sync();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        return 0L;
      }
    }).when(haGroupStoreManager).setHAGroupStatusToSync(haGroupName);

    // Write some data so the forwarder has files to process
    for (long id = 1; id <= count; ++id) {
      Mutation put = LogFileTestUtil.newPut("row_" + id, id, 2);
      logGroup.append(tableName, id, put);
    }
    logGroup.sync();

    // Wait for the forwarder to eventually succeed after retrying peer creation
    long deadline = EnvironmentEdgeManager.currentTimeMillis() + 120_000;
    while (logGroup.getMode() != SYNC && EnvironmentEdgeManager.currentTimeMillis() < deadline) {
      Thread.sleep(500);
    }
    assertEquals(SYNC, logGroup.getMode());
  }
}
