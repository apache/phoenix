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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.phoenix.replication.ReplicationLogTracker;
import org.apache.phoenix.replication.ReplicationShardDirectoryManager;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogDiscoveryReplay;
import org.junit.After;
import org.junit.Test;

/**
 * Recording-site tests for the replay lag metrics that drive the real
 * {@link ReplicationLogDiscoveryReplay#processFile(Path, boolean)} rather than exercising the
 * metric source in isolation. They cover:
 * <ul>
 * <li>the {@code getRoundEligibleTime} math the lag samples are anchored to, including a file whose
 * creation timestamp lands exactly on a round boundary (owned by the earlier round);</li>
 * <li>first-claim gating: pickup lag is recorded on the new-files path (firstClaim=true) and not on
 * an in-progress reclaim (firstClaim=false);</li>
 * <li>failure handling: a file that throws mid-replay still records pickup lag but never records an
 * end-to-end lag sample.</li>
 * </ul>
 * The round duration is stubbed to 60s and the waiting buffer to 15%, giving a round of 60000ms and
 * a buffer of 9000ms. A file created at 100000ms is owned by the round ending at 120000ms and thus
 * becomes eligible at 120000 + 9000 = 129000ms; a file created exactly on the 120000ms boundary is
 * owned by that same earlier round and is also eligible at 129000ms.
 */
public class ReplicationLogDiscoveryReplayProcessFileTest {

  private static final long ROUND_SECONDS = 60L;
  /** Eligible instant for a file whose owning round ends at 120000ms: 120000 + 9000 buffer. */
  private static final long ELIGIBLE_MS = 129000L;
  private static final Path FILE = new Path("/replication/inprogress/1_rs_uuid_135000.plog");

  @After
  public void tearDown() {
    EnvironmentEdgeManager.reset();
  }

  /**
   * First claim of a file whose creation timestamp is strictly inside a round: both pickup lag
   * (rename - eligible) and end-to-end lag (now - eligible) are recorded.
   */
  @Test
  public void testFirstClaimRecordsPickupAndEndToEndLag() throws IOException {
    ReplicationLogTracker tracker = mock(ReplicationLogTracker.class);
    MetricsReplicationLogDiscoveryReplay metrics = mock(MetricsReplicationLogDiscoveryReplay.class);
    when(tracker.getFileTimestamp(any())).thenReturn(100000L);
    when(tracker.getRenameTimestamp(any())).thenReturn(Optional.of(135000L));
    injectNow(140000L);

    newReplay(tracker, metrics).processFile(FILE, true);

    // eligible = 129000; pickup = 135000 - 129000; endToEnd = 140000 - 129000.
    verify(metrics).updatePickupLag(135000L - ELIGIBLE_MS);
    verify(metrics).updateEndToEndReplayLag(140000L - ELIGIBLE_MS);
  }

  /**
   * A file created exactly on a round boundary is owned by the earlier round (inclusive round
   * bounds), so its eligibility is the boundary itself plus the buffer, not a full round later.
   * Regression guard for the off-by-one: the old formula anchored to 189000ms, which clamped this
   * pickup lag to 0; the corrected formula anchors to 129000ms and records the real 1000ms.
   */
  @Test
  public void testFirstClaimOnRoundBoundaryAnchorsToOwningRound() throws IOException {
    ReplicationLogTracker tracker = mock(ReplicationLogTracker.class);
    MetricsReplicationLogDiscoveryReplay metrics = mock(MetricsReplicationLogDiscoveryReplay.class);
    when(tracker.getFileTimestamp(any())).thenReturn(120000L);
    when(tracker.getRenameTimestamp(any())).thenReturn(Optional.of(130000L));
    injectNow(140000L);

    newReplay(tracker, metrics).processFile(FILE, true);

    verify(metrics).updatePickupLag(130000L - ELIGIBLE_MS);
    verify(metrics).updateEndToEndReplayLag(140000L - ELIGIBLE_MS);
  }

  /**
   * An in-progress reclaim (firstClaim=false) must not record pickup lag -- markInProgress
   * re-stamps a fresh rename timestamp on every reclaim, so a pickup sample here would measure
   * eligible -> latest-reclaim, not eligible -> first-pickup -- but end-to-end lag is still
   * recorded.
   */
  @Test
  public void testReclaimDoesNotRecordPickupLag() throws IOException {
    ReplicationLogTracker tracker = mock(ReplicationLogTracker.class);
    MetricsReplicationLogDiscoveryReplay metrics = mock(MetricsReplicationLogDiscoveryReplay.class);
    when(tracker.getFileTimestamp(any())).thenReturn(100000L);
    injectNow(140000L);

    newReplay(tracker, metrics).processFile(FILE, false);

    verify(metrics, never()).updatePickupLag(anyLong());
    verify(metrics).updateEndToEndReplayLag(140000L - ELIGIBLE_MS);
  }

  /**
   * When replay throws, pickup lag has already been recorded (before the replay call) but the
   * end-to-end lag sample -- which represents a completed replay -- is never recorded, and the
   * failure propagates.
   */
  @Test
  public void testReplayFailureRecordsPickupButNotEndToEndLag() throws IOException {
    ReplicationLogTracker tracker = mock(ReplicationLogTracker.class);
    MetricsReplicationLogDiscoveryReplay metrics = mock(MetricsReplicationLogDiscoveryReplay.class);
    when(tracker.getFileTimestamp(any())).thenReturn(100000L);
    when(tracker.getRenameTimestamp(any())).thenReturn(Optional.of(135000L));
    injectNow(140000L);
    TestReplay replay = newReplay(tracker, metrics);
    replay.failReplay = true;

    assertThrows(IOException.class, () -> replay.processFile(FILE, true));

    verify(metrics).updatePickupLag(135000L - ELIGIBLE_MS);
    verify(metrics, never()).updateEndToEndReplayLag(anyLong());
  }

  /**
   * A malformed in-progress file name (getFileTimestamp throws NumberFormatException) must surface
   * as an IOException -- the type processOneRandomFile catches and routes through markFailed --
   * rather than an unchecked exception that would escape the per-file handler and abort the whole
   * sweep. Nothing is replayed and no lag sample is recorded.
   */
  @Test
  public void testMalformedFileNameSurfacesAsIOException() throws IOException {
    ReplicationLogTracker tracker = mock(ReplicationLogTracker.class);
    MetricsReplicationLogDiscoveryReplay metrics = mock(MetricsReplicationLogDiscoveryReplay.class);
    when(tracker.getFileTimestamp(any())).thenThrow(new NumberFormatException("bad name"));
    TestReplay replay = newReplay(tracker, metrics);

    assertThrows(IOException.class, () -> replay.processFile(FILE, true));

    assertFalse("replay must not run when the file name cannot be parsed", replay.replayInvoked);
    verify(metrics, never()).updatePickupLag(anyLong());
    verify(metrics, never()).updateEndToEndReplayLag(anyLong());
  }

  private static void injectNow(long now) {
    EnvironmentEdgeManager.injectEdge(new EnvironmentEdge() {
      @Override
      public long currentTime() {
        return now;
      }
    });
  }

  private static TestReplay newReplay(ReplicationLogTracker tracker,
    MetricsReplicationLogDiscoveryReplay metrics) {
    ReplicationShardDirectoryManager shardManager = mock(ReplicationShardDirectoryManager.class);
    when(tracker.getReplicationShardDirectoryManager()).thenReturn(shardManager);
    when(shardManager.getReplicationRoundDurationSeconds()).thenReturn((int) ROUND_SECONDS);
    return new TestReplay(tracker, metrics);
  }

  /**
   * Minimal subclass that stands in a mocked metrics source and short-circuits the actual log-file
   * replay, so only the lag-recording logic in {@code processFile} is under test. Overriding
   * {@link #getWaitingBufferPercentage()} to a constant keeps the base constructor off the (unset)
   * Configuration, fixing the buffer at 15% of the round.
   */
  private static final class TestReplay extends ReplicationLogDiscoveryReplay {

    private final MetricsReplicationLogDiscoveryReplay replayMetricsMock;
    private boolean failReplay;
    private boolean replayInvoked;

    TestReplay(ReplicationLogTracker tracker,
      MetricsReplicationLogDiscoveryReplay replayMetricsMock) {
      super(tracker);
      this.replayMetricsMock = replayMetricsMock;
    }

    @Override
    public double getWaitingBufferPercentage() {
      return 15.0;
    }

    @Override
    protected MetricsReplicationLogDiscoveryReplay getReplayMetrics() {
      return replayMetricsMock;
    }

    @Override
    protected void replayLogFile(Path path) throws IOException {
      replayInvoked = true;
      if (failReplay) {
        throw new IOException("injected replay failure");
      }
    }
  }
}
