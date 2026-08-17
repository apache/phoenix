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
import static org.apache.phoenix.replication.ReplicationLogGroup.ReplicationMode.SYNC_AND_FORWARD;
import static org.apache.phoenix.replication.ReplicationShardDirectoryManager.PHOENIX_REPLICATION_ROUND_DURATION_SECONDS_KEY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.phoenix.jdbc.HAGroupStoreRecord.HAGroupState;
import org.apache.phoenix.replication.ReplicationLogGroup.ReplicationMode;
import org.apache.phoenix.replication.log.LogFileTestUtil;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogForwarderSourceFactory;
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
      if (s.isFile()) {
        peerNames.add(s.getPath().getName());
      }
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

  /** Reads the full contents of a file into a byte array. */
  private byte[] readAll(FileSystem fs, Path file) throws IOException {
    try (FSDataInputStream in = fs.open(file)) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] buf = new byte[4096];
      int n;
      while ((n = in.read(buf)) != -1) {
        out.write(buf, 0, n);
      }
      return out.toByteArray();
    }
  }

  /**
   * Sets up a single in-progress source file on the local cluster and returns its in-progress path.
   * The peer shard manager is created eagerly so its FileSystem can be spied by the caller.
   */
  private Path setUpSource(byte[] contents) throws IOException {
    ReplicationLogTracker localTracker = logGroup.getLogForwarder().getReplicationLogTracker();
    ReplicationShardDirectoryManager localShardManager = logGroup.getLocalShardManager();
    long ts = EnvironmentEdgeManager.currentTimeMillis();
    String origin = "10.244.1.10,16020,1784436416001";
    Path shardDir = localShardManager.getShardDirectory(ts);
    localFs.mkdirs(shardDir);
    Path src = new Path(shardDir, ts + "_" + origin + ".plog");
    try (FSDataOutputStream out = localFs.create(src, true)) {
      out.write(contents);
    }
    Optional<Path> inProgress = localTracker.markInProgress(src);
    assertTrue("markInProgress should succeed", inProgress.isPresent());
    return inProgress.get();
  }

  /** The final .plog dst on the peer for the given local in-progress source file. */
  private Path peerDstFor(Path inProgressSrc) throws IOException {
    ReplicationLogTracker localTracker = logGroup.getLogForwarder().getReplicationLogTracker();
    long ts = localTracker.getFileTimestamp(inProgressSrc);
    String origin = localTracker.getServerName(inProgressSrc);
    return logGroup.getOrCreatePeerShardManager().getWriterPath(ts, origin);
  }

  private Path stagingFor(Path dst) throws IOException {
    return logGroup.getOrCreatePeerShardManager().getStagingPath(dst);
  }

  /**
   * Installs a spy peer FileSystem so tests can intercept rename()/exists(). The peer shard manager
   * (a spy) is returned by the spied logGroup, and its getFileSystem() returns the spy FS. Callers
   * use it to stub rename/exists and to assert on peer-side state.
   */
  private FileSystem installPeerFsSpy() throws IOException {
    ReplicationShardDirectoryManager spyPeer = spy(logGroup.getOrCreatePeerShardManager());
    FileSystem peerFs = spy(spyPeer.getFileSystem());
    doReturn(peerFs).when(spyPeer).getFileSystem();
    doReturn(spyPeer).when(logGroup).getOrCreatePeerShardManager();
    return peerFs;
  }

  /**
   * Core regression guard for the cross-cluster lease race: while the forwarder is copying bytes,
   * the file must exist only under the non-replay-eligible .staging subdirectory; the replay-
   * eligible .plog appears in the shard directory only after the atomic rename. We intercept
   * rename() and assert the invariant at the moment just before the final publish.
   */
  @Test
  public void testForwardPublishesOnlyAfterRename() throws Exception {
    byte[] contents = "some-log-bytes".getBytes();
    Path src = setUpSource(contents);
    Path dst = peerDstFor(src);
    Path staging = stagingFor(dst);

    ReplicationShardDirectoryManager peerShardManager = logGroup.getOrCreatePeerShardManager();
    FileSystem peerFs = installPeerFsSpy();
    ReplicationLogTracker peerTracker =
      new ReplicationLogTracker(conf, haGroupName, peerShardManager,
        MetricsReplicationLogForwarderSourceFactory.getInstanceForTracker(haGroupName));

    final boolean[] invariantHeld = { false };
    doAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        // At this point the bytes are fully staged but not yet published.
        assertTrue("staging file must exist before rename", peerFs.exists(staging));
        assertFalse(".plog must NOT exist before rename", peerFs.exists(dst));
        assertTrue("replay must see no eligible file while staging",
          peerTracker.getNewFiles().isEmpty());
        invariantHeld[0] = true;
        return (Boolean) invocation.callRealMethod();
      }
    }).when(peerFs).rename(eq(staging), eq(dst));

    logGroup.getLogForwarder().processFile(src);

    assertTrue("rename interceptor should have run", invariantHeld[0]);
    assertTrue(".plog must exist after rename", peerFs.exists(dst));
    assertFalse("staging file must be gone after rename", peerFs.exists(staging));
    assertArrayEquals("published content must match source", contents, readAll(peerFs, dst));
  }

  /**
   * Retry after a crash between the rename and markCompleted (failure-mode row #5): the final .plog
   * already exists and replay has not yet consumed it. processFile must treat this as
   * already-delivered -- no throw, redundant staging dropped, dst untouched.
   */
  @Test
  public void testForwardRetryOntoExistingDestinationSucceeds() throws Exception {
    byte[] contents = "retry-onto-existing".getBytes();
    Path src = setUpSource(contents);
    Path dst = peerDstFor(src);
    Path staging = stagingFor(dst);

    // The local test FileSystem's rename overwrites dst and returns true (POSIX semantics), unlike
    // HDFS which returns false when dst exists. Stub rename to return false so we exercise the
    // production already-delivered branch.
    FileSystem peerFs = installPeerFsSpy();
    doReturn(false).when(peerFs).rename(eq(staging), eq(dst));

    // Simulate a prior successful publish of this same logical file.
    byte[] existing = "already-there".getBytes();
    try (FSDataOutputStream out = peerFs.create(dst, true)) {
      out.write(existing);
    }

    logGroup.getLogForwarder().processFile(src);

    assertTrue("dst should still exist", peerFs.exists(dst));
    assertFalse("staging file should be cleaned up", peerFs.exists(staging));
    assertArrayEquals("dst must be left untouched (not overwritten)", existing,
      readAll(peerFs, dst));
  }

  /**
   * Orphan reclamation (failure-mode rows #2/#3): a stale staging file left by a crashed prior
   * attempt is overwritten by the retry's copy (overwrite=true) and then published.
   */
  @Test
  public void testForwardReclaimsOrphanStagingFile() throws Exception {
    byte[] contents = "fresh-content".getBytes();
    Path src = setUpSource(contents);
    Path dst = peerDstFor(src);
    Path staging = stagingFor(dst);

    FileSystem peerFs = logGroup.getOrCreatePeerShardManager().getFileSystem();
    // Pre-create a stale/garbage staging file.
    try (FSDataOutputStream out = peerFs.create(staging, true)) {
      out.write("stale-garbage-bytes".getBytes());
    }

    logGroup.getLogForwarder().processFile(src);

    assertTrue("dst should be published", peerFs.exists(dst));
    assertFalse("staging file should be gone", peerFs.exists(staging));
    assertArrayEquals("dst content must be the fresh source content, not the orphan", contents,
      readAll(peerFs, dst));
  }

  /**
   * A genuine rename failure (rename returns false and dst does not exist) must throw and leave dst
   * uncreated so the source is retried from out_progress. The partial staging file is left behind
   * and reclaimed by the retry's overwrite=true copy.
   */
  @Test
  public void testForwardRenameFailureLeavesSourceForRetry() throws Exception {
    byte[] contents = "will-fail-rename".getBytes();
    Path src = setUpSource(contents);
    Path dst = peerDstFor(src);
    Path staging = stagingFor(dst);

    FileSystem peerFs = installPeerFsSpy();
    doReturn(false).when(peerFs).rename(eq(staging), eq(dst));

    try {
      logGroup.getLogForwarder().processFile(src);
      fail("processFile should have thrown on rename failure");
    } catch (IOException expected) {
      // expected
    }

    assertFalse("dst must not be created on failure", peerFs.exists(dst));
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

  /**
   * Builds a forwarder over a mock tracker so processNoMoreRoundsLeft can be driven directly with a
   * controlled in-progress / next-round file state.
   */
  private ReplicationLogDiscoveryForwarder forwarderWithMockTracker(ReplicationLogTracker tracker) {
    ReplicationShardDirectoryManager shardManager = mock(ReplicationShardDirectoryManager.class);
    // stubs read by the ReplicationLogDiscovery constructor
    doReturn(conf).when(tracker).getConf();
    doReturn(haGroupName).when(tracker).getHaGroupName();
    doReturn(shardManager).when(tracker).getReplicationShardDirectoryManager();
    doReturn(20).when(shardManager).getReplicationRoundDurationSeconds();
    doReturn(new ReplicationRound(0, 20_000)).when(shardManager).getNextRound(any());
    ReplicationLogDiscoveryForwarder forwarder =
      new ReplicationLogDiscoveryForwarder(logGroup, tracker);
    forwarder.setLastRoundProcessed(new ReplicationRound(0, 20_000));
    return forwarder;
  }

  /**
   * The mode flip to SYNC_AND_FORWARD is gated only on an empty in-progress directory, not on the
   * next round's shard scan. An idle RS whose own (and its peers') live rotation writer keeps the
   * next-round scan non-empty must still promote its own mode; otherwise it stays pinned in
   * STORE_AND_FORWARD and wedges the group at ACTIVE_NOT_IN_SYNC.
   */
  @Test
  public void testModeFlipsWhenInProgressEmptyEvenIfNextRoundHasFiles() throws Exception {
    ReplicationLogTracker tracker = mock(ReplicationLogTracker.class);
    doReturn(Collections.emptyList()).when(tracker).getInProgressFiles();
    // next round is non-empty (a live rotation writer masquerading as pending work)
    doReturn(Collections.singletonList(new Path("out/shard/000/1_rs2.plog"))).when(tracker)
      .getNewFilesForRound(any());
    ReplicationLogDiscoveryForwarder forwarder = forwarderWithMockTracker(tracker);

    forwarder.processNoMoreRoundsLeft();

    // mode promoted despite the non-empty next-round scan ...
    verify(logGroup, times(1)).checkAndSetModeAndNotify(STORE_AND_FORWARD, SYNC_AND_FORWARD);
    assertEquals(SYNC_AND_FORWARD, logGroup.getMode());
    // ... but the shared in-sync claim is withheld because the full guard is not satisfied
    verify(logGroup, never()).setHAGroupStatusToSync();
  }

  /**
   * A non-empty in-progress directory means this RS has claimed-but-stuck files (its forward path
   * is unhealthy), so it must neither promote its own mode nor claim the group is in sync.
   */
  @Test
  public void testNoModeFlipWhenInProgressNotEmpty() throws Exception {
    ReplicationLogTracker tracker = mock(ReplicationLogTracker.class);
    doReturn(Collections.singletonList(new Path("out_progress/1_rs2_uuid_2.plog"))).when(tracker)
      .getInProgressFiles();
    doReturn(Collections.emptyList()).when(tracker).getNewFilesForRound(any());
    ReplicationLogDiscoveryForwarder forwarder = forwarderWithMockTracker(tracker);

    forwarder.processNoMoreRoundsLeft();

    verify(logGroup, never()).checkAndSetModeAndNotify(STORE_AND_FORWARD, SYNC_AND_FORWARD);
    assertEquals(STORE_AND_FORWARD, logGroup.getMode());
    verify(logGroup, never()).setHAGroupStatusToSync();
  }

  /**
   * Fully caught up: in-progress empty and no new files for the ongoing round. Both the mode flip
   * and the shared in-sync claim fire.
   */
  @Test
  public void testStatusFlipsWhenFullyCaughtUp() throws Exception {
    ReplicationLogTracker tracker = mock(ReplicationLogTracker.class);
    doReturn(Collections.emptyList()).when(tracker).getInProgressFiles();
    doReturn(Collections.emptyList()).when(tracker).getNewFilesForRound(any());
    doReturn(0L).when(logGroup).setHAGroupStatusToSync();
    ReplicationLogDiscoveryForwarder forwarder = forwarderWithMockTracker(tracker);

    forwarder.processNoMoreRoundsLeft();

    verify(logGroup, times(1)).checkAndSetModeAndNotify(STORE_AND_FORWARD, SYNC_AND_FORWARD);
    assertEquals(SYNC_AND_FORWARD, logGroup.getMode());
    verify(logGroup, times(1)).setHAGroupStatusToSync();
  }

  /**
   * The mode flip is self-validating: promoting to SYNC_AND_FORWARD drives
   * SyncAndForwardModeImpl.onEnter, which must reach the peer. If the peer is unreachable the flip
   * bounces back to STORE_AND_FORWARD, so an optimistic promotion against a dead peer does not
   * leave the group stuck advertising a healthy forward path. In-progress is empty (so the
   * promotion fires) but getOrCreatePeerShardManager always throws (dead peer).
   */
  @Test
  public void testModeFlipBouncesBackWhenPeerUnreachable() throws Exception {
    ReplicationLogTracker tracker = mock(ReplicationLogTracker.class);
    doReturn(Collections.emptyList()).when(tracker).getInProgressFiles();
    doReturn(Collections.emptyList()).when(tracker).getNewFilesForRound(any());
    // dead peer: onEnter for SYNC_AND_FORWARD can never reach the peer
    doThrow(new IOException("Peer namenode unavailable")).when(logGroup)
      .getOrCreatePeerShardManager();
    ReplicationLogDiscoveryForwarder forwarder = forwarderWithMockTracker(tracker);

    forwarder.processNoMoreRoundsLeft();

    // the promotion is attempted ...
    verify(logGroup, times(1)).checkAndSetModeAndNotify(STORE_AND_FORWARD, SYNC_AND_FORWARD);
    // ... but onEnter's failed peer reach bounces the mode back asynchronously on the disruptor
    long deadline = EnvironmentEdgeManager.currentTimeMillis() + 120_000;
    while (
      logGroup.getMode() != STORE_AND_FORWARD
        && EnvironmentEdgeManager.currentTimeMillis() < deadline
    ) {
      Thread.sleep(100);
    }
    assertEquals(STORE_AND_FORWARD, logGroup.getMode());
  }
}
