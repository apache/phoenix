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
  public static final double DEFAULT_LOG_COPY_THROUGHPUT_BYTES_PER_MS = 1.0;

  /**
   * Configuration key for waiting buffer percentage
   */
  public static final String REPLICATION_FORWARDER_WAITING_BUFFER_PERCENTAGE_KEY =
    "phoenix.replication.forwarder.waiting.buffer.percentage";

  private final ReplicationLogGroup logGroup;
  private final double copyThroughputThresholdBytesPerMs;
  private ReplicationShardDirectoryManager standbyShardManager;
  // the timestamp (in future) at which we will attempt to set the HAGroup state to SYNC
  private long syncUpdateTS;

  public ReplicationLogDiscoveryForwarder(ReplicationLogGroup logGroup,
    ReplicationLogTracker forwardingLogTracker) {
    super(forwardingLogTracker);
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
    // Initialize the discovery only. Forwarding begins only when we switch to the
    // STORE_AND_FORWARD mode or SYNC_AND_FORWARD mode.
    super.init();
  }

  @Override
  protected void processFile(Path src) throws IOException {
    FileSystem srcFS = replicationLogTracker.getFileSystem();
    FileStatus srcStat = srcFS.getFileStatus(src);
    long ts = replicationLogTracker.getFileTimestamp(srcStat.getPath());
    if (standbyShardManager == null) {
      standbyShardManager = logGroup.createStandbyShardManager();
    }
    Path dst = standbyShardManager.getWriterPath(ts, logGroup.getServerName().getServerName());
    long startTime = EnvironmentEdgeManager.currentTimeMillis();
    FileUtil.copy(srcFS, srcStat, standbyShardManager.getFileSystem(), dst, false, false, conf);
    // successfully copied the file
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
    // check if we are caught up so that we can transition to SYNC state
    // we are caught up when there are no files currently in the out progress directory
    // and no new files exist for ongoing round
    if (
      replicationLogTracker.getInProgressFiles().isEmpty()
        && replicationLogTracker.getNewFilesForRound(replicationLogTracker
          .getReplicationShardDirectoryManager().getNextRound(getLastRoundProcessed())).isEmpty()
    ) {
      LOG.info("Processed all the replication log files for {}", logGroup);
      // if this RS is still in STORE_AND_FORWARD mode like when it didn't process any file
      // move this RS to SYNC_AND_FORWARD
      logGroup.checkAndSetModeAndNotify(STORE_AND_FORWARD, SYNC_AND_FORWARD);

      if (syncUpdateTS <= EnvironmentEdgeManager.currentTimeMillis()) {
        try {
          long waitTime = logGroup.setHAGroupStatusToSync();
          if (waitTime != 0) {
            syncUpdateTS = EnvironmentEdgeManager.currentTimeMillis() + waitTime;
            LOG.info("HAGroup {} will try to update HA state to sync at {}", logGroup,
              syncUpdateTS);
          } else {
            LOG.info("HAGroup {} updated HA state to SYNC", logGroup);
          }
        } catch (Exception e) {
          LOG.info("Could not update status to sync for {}", logGroup, e);
        }
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
}
