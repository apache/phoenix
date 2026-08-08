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
import static org.apache.phoenix.replication.ReplicationLogGroup.ReplicationMode.SYNC_AND_FORWARD;

import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogDiscovery;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogForwarderSourceFactory;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * ReplicationLogDiscoveryForwarder manages the forwarding of the replication log from the fallback
 * cluster to the remote cluster.
 */
public class ReplicationLogDiscoveryForwarder extends ReplicationLogDiscovery {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationLogDiscoveryForwarder.class);

  public static final String REPLICATION_LOG_COPY_THROUGHPUT_BYTES_PER_MS_KEY =
    "phoenix.replication.log.copy.throughput.bytes.per.ms";
  // TODO: come up with a better default after testing
  public static final double DEFAULT_LOG_COPY_THROUGHPUT_BYTES_PER_MS = 0.1;

  /**
   * Configuration key for waiting buffer percentage
   */
  public static final String REPLICATION_FORWARDER_WAITING_BUFFER_PERCENTAGE_KEY =
    "phoenix.replication.forwarder.waiting.buffer.percentage";

  /**
   * Configuration key for in-progress directory processing probability (percentage)
   */
  public static final String REPLICATION_FORWARDER_IN_PROGRESS_PROCESSING_PROBABILITY_KEY =
    "phoenix.replication.forwarder.in.progress.processing.probability";

  /**
   * Default probability (in percentage) for processing files from the out-progress directory.
   * Higher than the base default so a file left behind by a peer's failed claim is retried sooner.
   */
  public static final double DEFAULT_FORWARDER_IN_PROGRESS_PROCESSING_PROBABILITY = 20.0;

  private final ReplicationLogGroup logGroup;
  private final double copyThroughputThresholdBytesPerMs;
  // the timestamp (in future) at which we will attempt to set the HAGroup state to SYNC
  private long syncUpdateTS;

  /**
   * Create a tracker for the replication logs in the fallback cluster.
   * @param logGroup HAGroup
   */
  private static ReplicationLogTracker createLogTracker(ReplicationLogGroup logGroup) {
    ReplicationShardDirectoryManager localShardManager = logGroup.getLocalShardManager();
    return new ReplicationLogTracker(logGroup.conf, logGroup.getHAGroupName(), localShardManager,
      MetricsReplicationLogForwarderSourceFactory.getInstanceForTracker(logGroup.getHAGroupName()));
  }

  public ReplicationLogDiscoveryForwarder(ReplicationLogGroup logGroup) {
    this(logGroup, createLogTracker(logGroup));
  }

  @VisibleForTesting
  ReplicationLogDiscoveryForwarder(ReplicationLogGroup logGroup, ReplicationLogTracker tracker) {
    super(tracker);
    this.logGroup = logGroup;
    this.copyThroughputThresholdBytesPerMs = conf.getDouble(
      REPLICATION_LOG_COPY_THROUGHPUT_BYTES_PER_MS_KEY, DEFAULT_LOG_COPY_THROUGHPUT_BYTES_PER_MS);
    // initialize to 0
    this.syncUpdateTS = 0;
  }

  @Override
  public String getExecutorThreadNameFormat() {
    return "ReplicationLogDiscoveryForwarder-" + logGroup.getHAGroupName() + "-%d";
  }

  public void init() throws IOException {
    replicationLogTracker.init();
    // Initialize the discovery only. Forwarding begins only when we switch to the
    // STORE_AND_FORWARD mode or SYNC_AND_FORWARD mode.
    super.init();
  }

  @Override
  protected void processFile(Path src) throws IOException {
    FileSystem srcFS = replicationLogTracker.getFileSystem();
    FileStatus srcStat = srcFS.getFileStatus(src);
    long ts = replicationLogTracker.getFileTimestamp(srcStat.getPath());
    // Preserve the origin writer identity so the forwarded file is byte-identical to what the
    // origin RS would have written natively. Re-deriving the name from (ts, forwardingServer) can
    // collapse two distinct source files sharing a timestamp onto one dst, wedging SYNC re-entry.
    String originServerName = replicationLogTracker.getServerName(srcStat.getPath());
    ReplicationShardDirectoryManager remoteShardManager = logGroup.getOrCreatePeerShardManager();
    FileSystem dstFS = remoteShardManager.getFileSystem();
    Path dst = remoteShardManager.getWriterPath(ts, originServerName);
    // Stage inside the shard's .staging subdirectory so replay never picks up a half-written file
    // and force-recovers the lease: every listing skips subdirectories via FileStatus.isFile().
    // Publish atomically via a same-shard rename up to dst once the bytes are fully written.
    Path staging = remoteShardManager.getStagingPath(dst);
    long startTime = EnvironmentEdgeManager.currentTimeMillis();
    // overwrite=true reclaims any orphan staging file left by a prior crashed attempt of this same
    // logical file (dst is keyed on the stable (ts, originServerName)). A copy failure, or a failed
    // rename when dst is absent, propagates: the source stays in out_progress and is retried, and
    // that retry re-copies with overwrite=true, reclaiming any partial staging file left behind.
    FileUtil.copy(srcFS, srcStat, dstFS, staging, false, true, conf);
    if (!dstFS.rename(staging, dst)) {
      // rename returned false. If dst already exists, a prior attempt of this logical file
      // published
      // it and replay has not yet consumed it (the retry raced ahead of replay). This is not
      // exactly-once dedup: if replay already deleted dst, exists is false and we throw so the
      // source is retried, re-publishing dst (at-least-once; safe because replay is idempotent).
      if (dstFS.exists(dst)) {
        LOG.info("Destination {} already present (retry raced ahead of replay) for src={}", dst,
          src);
        // The publish is already complete, so cleaning up the redundant staging file is best
        // effort: this path returns normally and the source is then marked completed, so there is
        // no later retry to reclaim it via overwrite=true. A failure here only leaks a staging
        // file (invisible to replay); it must never demote an already-delivered publish.
        try {
          if (!dstFS.delete(staging, false)) {
            LOG.warn("Could not delete redundant staging file {} (dst {} already published for "
              + "src={}); it is orphaned and will not be reclaimed", staging, dst, src);
          }
        } catch (IOException e) {
          LOG.warn("Best-effort cleanup of staging file {} failed after dst {} was published "
            + "for src={}", staging, dst, src, e);
        }
      } else {
        throw new IOException("Failed to rename staging file " + staging + " to " + dst);
      }
    }
    // successfully copied and published the file
    long endTime = EnvironmentEdgeManager.currentTimeMillis();
    long copyTime = endTime - startTime;
    LOG.info("Copying file src={} dst={} size={} took {}ms", src, dst, srcStat.getLen(), copyTime);
    if (
      logGroup.getMode() == STORE_AND_FORWARD
        && isLogCopyThroughputAboveThreshold(srcStat.getLen(), copyTime)
    ) {
      // start recovery by switching to SYNC_AND_FORWARD
      logGroup.checkAndSetModeAndNotify(STORE_AND_FORWARD, SYNC_AND_FORWARD);
    }
  }

  @Override
  protected void processNoMoreRoundsLeft() throws IOException {
    // A non-empty in-progress directory means this RS has claimed-but-stuck files, so its forward
    // path to the peer is unhealthy: neither promote its own mode nor claim the group is in sync.
    if (!replicationLogTracker.getInProgressFiles().isEmpty()) {
      LOG.info("In-progress directory not empty for {}, skipping mode promotion and sync claim",
        logGroup);
      return;
    }

    // No stuck files, so promote this RS's own mode on that signal alone. Gating on the next
    // round's
    // shard would pin an idle RS forever: it is a shared directory holding every co-active RS's
    // live
    // rotation writer. The flip is self-validating — SyncAndForwardModeImpl.onEnter must reach the
    // peer, so a bad promotion bounces back to STORE_AND_FORWARD.
    logGroup.checkAndSetModeAndNotify(STORE_AND_FORWARD, SYNC_AND_FORWARD);

    // The shared in-sync claim additionally requires no new files for the ongoing round.
    if (
      !replicationLogTracker.getNewFilesForRound(replicationLogTracker
        .getReplicationShardDirectoryManager().getNextRound(getLastRoundProcessed())).isEmpty()
    ) {
      LOG.info("New files present for the next round for {}, skipping sync claim", logGroup);
      return;
    }
    LOG.info("Processed all the replication log files for {}", logGroup);
    if (syncUpdateTS <= EnvironmentEdgeManager.currentTimeMillis()) {
      try {
        long waitTime = logGroup.setHAGroupStatusToSync();
        if (waitTime != 0) {
          syncUpdateTS = EnvironmentEdgeManager.currentTimeMillis() + waitTime;
          LOG.info("HAGroup {} will try to update HA state to sync at {}", logGroup, syncUpdateTS);
        } else {
          LOG.info("HAGroup {} updated HA state to SYNC", logGroup);
        }
      } catch (Exception e) {
        LOG.info("Could not update status to sync for {}", logGroup, e);
      }
    }
  }

  /**
   * Determine if the throughput is above the configured threshold. If it is, then we can switch to
   * the SYNC_AND_FORWARD mode
   * @param fileSize in bytes
   * @param copyTime in ms
   * @return True if the throughput is good else false
   */
  private boolean isLogCopyThroughputAboveThreshold(long fileSize, long copyTime) {
    double actualThroughputBytesPerMs = copyTime != 0 ? ((double) fileSize) / copyTime : 0;
    return actualThroughputBytesPerMs >= copyThroughputThresholdBytesPerMs;
  }

  @Override
  protected MetricsReplicationLogDiscovery createMetricsSource() {
    return MetricsReplicationLogForwarderSourceFactory
      .getInstanceForDiscovery(logGroup.getHAGroupName());
  }

  @VisibleForTesting
  protected ReplicationLogTracker getReplicationLogTracker() {
    return replicationLogTracker;
  }

  @Override
  public double getWaitingBufferPercentage() {
    return getConf().getDouble(REPLICATION_FORWARDER_WAITING_BUFFER_PERCENTAGE_KEY,
      DEFAULT_WAITING_BUFFER_PERCENTAGE);
  }

  @Override
  public double getInProgressDirectoryProcessProbability() {
    return getConf().getDouble(REPLICATION_FORWARDER_IN_PROGRESS_PROCESSING_PROBABILITY_KEY,
      DEFAULT_FORWARDER_IN_PROGRESS_PROCESSING_PROBABILITY);
  }
}
