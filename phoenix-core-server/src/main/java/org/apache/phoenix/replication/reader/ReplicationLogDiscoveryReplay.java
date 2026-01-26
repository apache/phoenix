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

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.phoenix.exception.InvalidClusterRoleTransitionException;
import org.apache.phoenix.jdbc.ClusterType;
import org.apache.phoenix.jdbc.HAGroupStateListener;
import org.apache.phoenix.jdbc.HAGroupStoreManager;
import org.apache.phoenix.jdbc.HAGroupStoreRecord;
import org.apache.phoenix.replication.ReplicationLogDiscovery;
import org.apache.phoenix.replication.ReplicationLogTracker;
import org.apache.phoenix.replication.ReplicationRound;
import org.apache.phoenix.replication.ReplicationShardDirectoryManager;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogDiscovery;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogDiscoveryReplayImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * State-aware implementation of ReplicationLogDiscovery for HA replication replay on standby
 * clusters. This class extends the base ReplicationLogDiscovery with support for three replication
 * states: - SYNC: Normal synchronized processing where both lastRoundProcessed and lastRoundInSync
 * advance together - DEGRADED: Degraded mode where lastRoundProcessed advances but lastRoundInSync
 * is preserved - SYNCED_RECOVERY: Recovery mode that rewinds to lastRoundInSync and re-processes
 * from that point Key features: - Uses getFirstRoundToProcess() to start replay from
 * lastRoundInSync (not just from lastRoundProcessed) - Dynamically responds to HA state changes via
 * listeners during replay execution - Maintains separate tracking of lastRoundProcessed and
 * lastRoundInSync for recovery scenarios - Integrates with HAGroupStoreManager for cluster state
 * coordination
 */
public class ReplicationLogDiscoveryReplay extends ReplicationLogDiscovery {

  private static final Logger LOG = LoggerFactory.getLogger(ReplicationLogDiscoveryReplay.class);

  public static final String EXECUTOR_THREAD_NAME_FORMAT =
    "Phoenix-ReplicationLogDiscoveryReplay-%d";

  /**
   * Configuration key for replay interval in seconds
   */
  public static final String REPLICATION_REPLAY_INTERVAL_SECONDS_KEY =
    "phoenix.replication.replay.interval.seconds";

  /**
   * Configuration key for shutdown timeout in seconds
   */
  public static final String REPLICATION_REPLAY_SHUTDOWN_TIMEOUT_SECONDS_KEY =
    "phoenix.replication.replay.executor.shutdown.timeout.seconds";

  /**
   * Configuration key for executor thread count
   */
  public static final String REPLICATION_REPLAY_EXECUTOR_THREAD_COUNT_KEY =
    "phoenix.replication.replay.executor.thread.count";

  /**
   * Configuration key for in-progress directory processing probability
   */
  public static final String REPLICATION_REPLAY_IN_PROGRESS_DIRECTORY_PROCESSING_PROBABILITY_KEY =
    "phoenix.replication.replay.in.progress.directory.processing.probability";

  /**
   * Configuration key for waiting buffer percentage
   */
  public static final String REPLICATION_REPLAY_WAITING_BUFFER_PERCENTAGE_KEY =
    "phoenix.replication.replay.waiting.buffer.percentage";

  /**
   * Default replay interval in seconds. Controls how frequently the replay process runs.
   */
  public static final long DEFAULT_REPLAY_INTERVAL_SECONDS = 60;

  /**
   * Default shutdown timeout in seconds. Maximum time to wait for executor service to shutdown
   * gracefully.
   */
  public static final long DEFAULT_SHUTDOWN_TIMEOUT_SECONDS = 30;

  /**
   * Default number of executor threads for processing replication log files.
   */
  public static final int DEFAULT_EXECUTOR_THREAD_COUNT = 1;

  /**
   * Default probability (in percentage) for processing in-progress directory during each replay
   * cycle.
   */
  public static final double DEFAULT_IN_PROGRESS_DIRECTORY_PROCESSING_PROBABILITY = 5.0;

  /**
   * Default waiting buffer percentage. Buffer time is calculated as this percentage of round time.
   */
  public static final double DEFAULT_WAITING_BUFFER_PERCENTAGE = 15.0;

  private ReplicationRound lastRoundInSync;

  // AtomicReference ensures listener updates are visible to replay thread
  private final AtomicReference<ReplicationReplayState> replicationReplayState =
    new AtomicReference<>(ReplicationReplayState.NOT_INITIALIZED);

  private final AtomicBoolean failoverPending = new AtomicBoolean(false);

  public ReplicationLogDiscoveryReplay(
    final ReplicationLogTracker replicationLogReplayFileTracker) {
    super(replicationLogReplayFileTracker);
  }

  @Override
  public void init() throws IOException {

    LOG.info("Initializing ReplicationLogDiscoveryReplay for haGroup: {}", haGroupName);

    HAGroupStateListener degradedListener =
      (groupName, fromState, toState, modifiedTime, clusterType, lastSyncStateTimeInMs) -> {
        if (
          clusterType == ClusterType.LOCAL
            && HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY.equals(toState)
        ) {
          replicationReplayState.set(ReplicationReplayState.DEGRADED);
          LOG.info("Cluster degraded detected for {}. replicationReplayState={}", haGroupName,
            ReplicationReplayState.DEGRADED);
        }
      };

    HAGroupStateListener recoveryListener =
      (groupName, fromState, toState, modifiedTime, clusterType, lastSyncStateTimeInMs) -> {
        if (
          clusterType == ClusterType.LOCAL
            && HAGroupStoreRecord.HAGroupState.STANDBY.equals(toState)
        ) {
          replicationReplayState.set(ReplicationReplayState.SYNCED_RECOVERY);
          LOG.info("Cluster recovered detected for {}. replicationReplayState={}", haGroupName,
            getReplicationReplayState());
        }
      };

    HAGroupStateListener triggerFailoverListner =
      (groupName, fromState, toState, modifiedTime, clusterType, lastSyncStateTimeInMs) -> {
        if (
          clusterType == ClusterType.LOCAL
            && HAGroupStoreRecord.HAGroupState.STANDBY_TO_ACTIVE.equals(toState)
        ) {
          failoverPending.set(true);
          LOG.info(
            "Failover trigger detected for {}. replicationReplayState={}. "
              + "Setting failover pending to {}",
            haGroupName, getReplicationReplayState(), failoverPending.get());
        }
      };

    HAGroupStateListener abortFailoverListner =
      (groupName, fromState, toState, modifiedTime, clusterType, lastSyncStateTimeInMs) -> {
        if (
          clusterType == ClusterType.LOCAL
            && HAGroupStoreRecord.HAGroupState.ABORT_TO_STANDBY.equals(toState)
        ) {
          failoverPending.set(false);
          LOG.info(
            "Failover abort detected for {}. replicationReplayState={}. "
              + "Setting failover pending to {}",
            haGroupName, getReplicationReplayState(), failoverPending.get());
        }
      };

    HAGroupStoreManager haGroupStoreManager = HAGroupStoreManager.getInstance(conf);

    // Subscribe degraded states
    haGroupStoreManager.subscribeToTargetState(haGroupName,
      HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY, ClusterType.LOCAL, degradedListener);

    // Subscribe recovery/healthy states
    haGroupStoreManager.subscribeToTargetState(haGroupName, HAGroupStoreRecord.HAGroupState.STANDBY,
      ClusterType.LOCAL, recoveryListener);

    // Subscribe to trigger failover state
    haGroupStoreManager.subscribeToTargetState(haGroupName,
      HAGroupStoreRecord.HAGroupState.STANDBY_TO_ACTIVE, ClusterType.LOCAL, triggerFailoverListner);

    // Subscribe to abort failover state
    haGroupStoreManager.subscribeToTargetState(haGroupName,
      HAGroupStoreRecord.HAGroupState.ABORT_TO_STANDBY, ClusterType.LOCAL, abortFailoverListner);

    super.init();
  }

  @Override
  protected void processFile(Path path) throws IOException {
    LOG.info("Starting to process file {}", path);
    ReplicationLogProcessor.get(getConf(), getHaGroupName())
      .processLogFile(getReplicationLogFileTracker().getFileSystem(), path);
  }

  /**
   * Initializes lastRoundProcessed and lastRoundInSync based on HA group state. For DEGRADED states
   * (DEGRADED_STANDBY, DEGRADED_STANDBY_FOR_WRITER): - Sets replicationReplayState to DEGRADED -
   * Initializes lastRoundProcessed from minimum of: in-progress files, new files, or current time -
   * Initializes lastRoundInSync from minimum of: lastSyncStateTimeInMs (from HA Store) or minimum
   * timestamp from IN and IN PROGRESS files - This ensures lastRoundInSync represents the last
   * known good sync point before degradation For SYNC states (STANDBY): - Sets
   * replicationReplayState to SYNC - Calls parent's initializeLastRoundProcessed() to initialize
   * lastRoundProcessed - Sets lastRoundInSync equal to lastRoundProcessed (both are in sync)
   * @throws IOException if there's an error reading HA group state or file timestamps
   */
  @Override
  protected void initializeLastRoundProcessed() throws IOException {
    LOG.info("Initializing last round processed for haGroup: {}", haGroupName);
    HAGroupStoreRecord haGroupStoreRecord = getHAGroupRecord();
    LOG.info("Found HA Group state during initialization as {} for haGroup: {}",
      haGroupStoreRecord.getHAGroupState(), haGroupName);
    if (
      HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY.equals(haGroupStoreRecord.getHAGroupState())
    ) {
      replicationReplayState.compareAndSet(ReplicationReplayState.NOT_INITIALIZED,
        ReplicationReplayState.DEGRADED);
      long minimumTimestampFromFiles = EnvironmentEdgeManager.currentTime();
      Optional<Long> minTimestampFromInProgressFiles = getMinTimestampFromInProgressFiles();
      Optional<Long> minTimestampFromNewFiles = getMinTimestampFromNewFiles();
      if (minTimestampFromInProgressFiles.isPresent()) {
        LOG.info("Found minimum timestamp from IN PROGRESS files as {}",
          minTimestampFromInProgressFiles.get());
        minimumTimestampFromFiles =
          Math.min(minimumTimestampFromFiles, minTimestampFromInProgressFiles.get());
      }
      if (minTimestampFromNewFiles.isPresent()) {
        LOG.info("Found minimum timestamp from IN files as {}", minTimestampFromNewFiles.get());
        minimumTimestampFromFiles =
          Math.min(minimumTimestampFromFiles, minTimestampFromNewFiles.get());
      }
      this.lastRoundProcessed = replicationLogTracker.getReplicationShardDirectoryManager()
        .getReplicationRoundFromEndTime(minimumTimestampFromFiles);
      this.lastRoundInSync =
        replicationLogTracker.getReplicationShardDirectoryManager().getReplicationRoundFromEndTime(
          Math.min(haGroupStoreRecord.getLastSyncStateTimeInMs(), minimumTimestampFromFiles));
    } else {
      replicationReplayState.compareAndSet(ReplicationReplayState.NOT_INITIALIZED,
        ReplicationReplayState.SYNC);
      super.initializeLastRoundProcessed();
      this.lastRoundInSync =
        new ReplicationRound(lastRoundProcessed.getStartTime(), lastRoundProcessed.getEndTime());
    }
    LOG.info(
      "Initialized last round processed as {}, last round in sync as {} and "
        + "replication replay state as {}",
      lastRoundProcessed, lastRoundInSync, replicationReplayState);

    // Update the failoverPending variable during initialization
    if (
      HAGroupStoreRecord.HAGroupState.STANDBY_TO_ACTIVE.equals(haGroupStoreRecord.getHAGroupState())
    ) {
      failoverPending.compareAndSet(false, true);
    }
  }

  /**
   * Executes a replay operation with state-aware processing for HA replication scenarios. This
   * method extends the base replay() by handling three replication states: 1. SYNC: Normal
   * processing - Updates both lastRoundProcessed and lastRoundInSync - Both pointers advance
   * together, indicating cluster is fully synchronized 2. DEGRADED: Degraded mode processing -
   * Updates only lastRoundProcessed (advances in memory) - Does NOT update lastRoundInSync
   * (preserves last known good sync point) - Allows processing to continue during degradation
   * without losing sync reference 3. SYNCED_RECOVERY: Recovery mode - Rewinds lastRoundProcessed
   * back to lastRoundInSync - Transitions to SYNC state - Re-processes rounds from last known good
   * sync point to ensure data consistency The first round is retrieved using
   * getFirstRoundToProcess() (starts from lastRoundInSync), subsequent rounds use
   * getNextRoundToProcess() (starts from lastRoundProcessed). State transitions can occur
   * dynamically via HA group listeners during replay execution.
   * @throws IOException if there's an error during replay processing
   */
  @Override
  public void replay() throws IOException {
    LOG.info("Starting replay with lastRoundProcessed={}, lastRoundInSync={}", lastRoundProcessed,
      lastRoundInSync);

    if (LOG.isDebugEnabled()) {
      try {
        LOG.debug("Consistency point for HAGroup: {} before starting the replay is {}.",
          haGroupName, getConsistencyPoint());
      } catch (IOException exception) {
        LOG.warn("Failed to get the consistency point for HA Group: {}", haGroupName, exception);
      }
    }

    Optional<ReplicationRound> optionalNextRound = getFirstRoundToProcess();
    LOG.info("Found first round to process as {} for haGroup: {}", optionalNextRound, haGroupName);
    while (optionalNextRound.isPresent()) {
      ReplicationRound replicationRound = optionalNextRound.get();
      try {
        processRound(replicationRound);
      } catch (IOException e) {
        LOG.error("Failed processing replication round {}. Will retry in next " + "scheduled run.",
          replicationRound, e);
        break; // stop this run, retry later
      }

      // Always read the latest listener state
      ReplicationReplayState currentState = replicationReplayState.get();

      switch (currentState) {
        case SYNCED_RECOVERY:
          // Rewind to last in-sync round
          LOG.info("SYNCED_RECOVERY detected, rewinding with lastRoundInSync={}", lastRoundInSync);
          Optional<ReplicationRound> firstRoundToProcess = getFirstRoundToProcess();
          LOG.info("Calculated first round to process after SYNCED_RECOVERY as" + "{}",
            firstRoundToProcess);
          firstRoundToProcess.ifPresent(round -> setLastRoundProcessed(
            replicationLogTracker.getReplicationShardDirectoryManager().getPreviousRound(round)));
          // Only reset to NORMAL if state hasn't been flipped to DEGRADED
          replicationReplayState.compareAndSet(ReplicationReplayState.SYNCED_RECOVERY,
            ReplicationReplayState.SYNC);
          break;

        case SYNC:
          // Normal processing, update last round processed and in-sync
          setLastRoundProcessed(replicationRound);
          setLastRoundInSync(replicationRound);
          LOG.info(
            "Processed round {} successfully, lastRoundProcessed={}, " + "lastRoundInSync={}",
            replicationRound, lastRoundProcessed, lastRoundInSync);
          break;

        case DEGRADED:
          // Only update last round processed, and NOT last round in sync
          setLastRoundProcessed(replicationRound);
          LOG.info(
            "Processed round {} successfully with cluster in DEGRADED "
              + "state, lastRoundProcessed={}, lastRoundInSync={}",
            replicationRound, lastRoundProcessed, lastRoundInSync);
          break;

        default:
          throw new IllegalStateException("Unexpected state: " + currentState);
      }

      if (LOG.isDebugEnabled()) {
        try {
          LOG.debug("Consistency point for HAGroup: {} after processing round: {} is {}",
            haGroupName, replicationRound, getConsistencyPoint());
        } catch (IOException exception) {
          LOG.warn(
            "Failed to get the consistency point for HA Group: {} after processing round: {}",
            haGroupName, replicationRound, exception);
        }
      }

      optionalNextRound = getNextRoundToProcess();
    }

    if (!optionalNextRound.isPresent() && shouldTriggerFailover()) {
      LOG.info(
        "No more rounds to process, lastRoundInSync={}, lastRoundProcessed={}. "
          + "Failover is triggered & in progress directory is empty. "
          + "Attempting to mark cluster state as {}",
        lastRoundInSync, lastRoundProcessed, HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);
      triggerFailover();
    }
  }

  /**
   * Returns the first replication round to process based on lastRoundInSync. Unlike
   * getNextRoundToProcess() which uses lastRoundProcessed, this method uses lastRoundInSync to
   * ensure replay starts from the last known synchronized point. This is critical for recovery
   * scenarios where lastRoundProcessed may be ahead of lastRoundInSync.
   * @return Optional containing the first round to process, or empty if not enough time has passed
   */
  private Optional<ReplicationRound> getFirstRoundToProcess() throws IOException {
    ReplicationRound lastRoundInSync = getLastRoundInSync();
    long lastRoundEndTimestamp = lastRoundInSync.getEndTime();
    if (lastRoundInSync.getStartTime() == 0) {
      Optional<Long> optionalMinimumNewFilesTimestamp = getMinTimestampFromNewFiles();
      lastRoundEndTimestamp =
        replicationLogTracker.getReplicationShardDirectoryManager().getNearestRoundStartTimestamp(
          optionalMinimumNewFilesTimestamp.orElseGet(EnvironmentEdgeManager::currentTime));
    }
    long currentTime = EnvironmentEdgeManager.currentTime();
    if (currentTime - lastRoundEndTimestamp < roundTimeMills + bufferMillis) {
      // nothing more to process
      return Optional.empty();
    }
    return Optional
      .of(new ReplicationRound(lastRoundEndTimestamp, lastRoundEndTimestamp + roundTimeMills));
  }

  @Override
  protected MetricsReplicationLogDiscovery createMetricsSource() {
    return new MetricsReplicationLogDiscoveryReplayImpl(haGroupName);
  }

  @Override
  public String getExecutorThreadNameFormat() {
    return EXECUTOR_THREAD_NAME_FORMAT;
  }

  @Override
  public long getReplayIntervalSeconds() {
    return getConf().getLong(REPLICATION_REPLAY_INTERVAL_SECONDS_KEY,
      DEFAULT_REPLAY_INTERVAL_SECONDS);
  }

  @Override
  public long getShutdownTimeoutSeconds() {
    return getConf().getLong(REPLICATION_REPLAY_SHUTDOWN_TIMEOUT_SECONDS_KEY,
      DEFAULT_SHUTDOWN_TIMEOUT_SECONDS);
  }

  @Override
  public int getExecutorThreadCount() {
    return getConf().getInt(REPLICATION_REPLAY_EXECUTOR_THREAD_COUNT_KEY,
      DEFAULT_EXECUTOR_THREAD_COUNT);
  }

  @Override
  public double getInProgressDirectoryProcessProbability() {
    return getConf().getDouble(REPLICATION_REPLAY_IN_PROGRESS_DIRECTORY_PROCESSING_PROBABILITY_KEY,
      DEFAULT_IN_PROGRESS_DIRECTORY_PROCESSING_PROBABILITY);
  }

  @Override
  public double getWaitingBufferPercentage() {
    return getConf().getDouble(REPLICATION_REPLAY_WAITING_BUFFER_PERCENTAGE_KEY,
      DEFAULT_WAITING_BUFFER_PERCENTAGE);
  }

  protected ReplicationRound getLastRoundInSync() {
    return lastRoundInSync;
  }

  protected ReplicationReplayState getReplicationReplayState() {
    return replicationReplayState.get();
  }

  protected void setLastRoundInSync(ReplicationRound lastRoundInSync) {
    this.lastRoundInSync = lastRoundInSync;
  }

  protected void setReplicationReplayState(ReplicationReplayState replicationReplayState) {
    this.replicationReplayState.set(replicationReplayState);
  }

  protected void setFailoverPending(boolean failoverPending) {
    this.failoverPending.set(failoverPending);
  }

  protected boolean getFailoverPending() {
    return this.failoverPending.get();
  }

  protected HAGroupStoreRecord getHAGroupRecord() throws IOException {
    Optional<HAGroupStoreRecord> optionalHAGroupStateRecord =
      HAGroupStoreManager.getInstance(conf).getHAGroupStoreRecord(haGroupName);
    if (!optionalHAGroupStateRecord.isPresent()) {
      throw new IOException("HAGroupStoreRecord not found for HA Group: " + haGroupName);
    }
    return optionalHAGroupStateRecord.get();
  }

  /**
   * Determines whether failover should be triggered based on completion criteria. Failover is safe
   * to trigger when all of the following conditions are met: 1. A failover has been requested
   * (failoverPending is true) 2. No files are currently in the in-progress directory 3. No new
   * files exist from the next round to process up to the current timestamp round. The third
   * condition checks for new files in the range from nextRoundToProcess (derived from
   * getLastRoundProcessed()) to currentTimestampRound (derived from current time). This ensures all
   * replication logs up to the current time have been processed before transitioning the cluster
   * from STANDBY to ACTIVE state.
   * @return true if all conditions are met and failover should be triggered, false otherwise
   * @throws IOException if there's an error checking file status
   */
  protected boolean shouldTriggerFailover() throws IOException {
    LOG.debug("Checking if failover should be triggered. failoverPending={}", failoverPending);
    // Check if failover has been requested
    if (!failoverPending.get()) {
      LOG.debug("Failover not triggered. failoverPending is false.");
      return false;
    }
    // Check if in-progress directory is empty
    boolean isInProgressDirectoryEmpty = replicationLogTracker.getInProgressFiles().isEmpty();
    if (!isInProgressDirectoryEmpty) {
      LOG.debug("Failover not triggered. In progress directory is not empty.");
      return false;
    }
    // Check if there are any new files from next round to current timestamp round
    ReplicationShardDirectoryManager replicationShardDirectoryManager =
      replicationLogTracker.getReplicationShardDirectoryManager();
    ReplicationRound nextRoundToProcess =
      replicationShardDirectoryManager.getNextRound(getLastRoundProcessed());
    ReplicationRound currentTimestampRound = replicationShardDirectoryManager
      .getReplicationRoundFromStartTime(EnvironmentEdgeManager.currentTime());
    LOG.debug("Checking the new files from next round {} to current timestamp round {}.",
      nextRoundToProcess, currentTimestampRound);
    boolean isInDirectoryEmpty =
      replicationLogTracker.getNewFiles(nextRoundToProcess, currentTimestampRound).isEmpty();

    if (!isInDirectoryEmpty) {
      LOG.debug(
        "Failover not triggered. New files exist from next round to current " + "timestamp round.");
      return false;
    }

    LOG.info("Failover can be triggered.");
    return true;
  }

  protected void triggerFailover() {
    try {
      HAGroupStoreManager.getInstance(conf).setHAGroupStatusToSync(haGroupName);
      failoverPending.set(false);
    } catch (InvalidClusterRoleTransitionException invalidClusterRoleTransitionException) {
      LOG.warn(
        "Failed to update the cluster state due to"
          + "InvalidClusterRoleTransitionException. Setting failoverPending" + "to false.",
        invalidClusterRoleTransitionException);
      failoverPending.set(false);
    } catch (Exception exception) {
      LOG.error("Failed to update the cluster state.", exception);
    }
  }

  public enum ReplicationReplayState {
    NOT_INITIALIZED, // not initialized yet
    SYNC, // fully in sync / standby
    DEGRADED, // degraded for writer
    SYNCED_RECOVERY // came back from degraded → standby, needs rewind
  }

  /**
   * Returns the consistency point timestamp based on the current replication replay state. The
   * consistency point in a standby cluster is defined as the timestamp such that all mutations
   * whose timestamp less than this consistency point timestamp have been replayed
   * @return The consistency point timestamp in milliseconds
   * @throws IOException if the consistency point cannot be determined based on current state
   */
  public long getConsistencyPoint() throws IOException {

    ReplicationReplayState currentState = replicationReplayState.get();
    long consistencyPoint = 0L;

    switch (currentState) {
      case SYNC:
        // In SYNC state: prefer minimum timestamp from in-progress files (if any),
        // otherwise use lastRoundInSync end time
        Optional<Long> optionalMinTimestampInProgressTimestamp =
          getMinTimestampFromInProgressFiles();
        if (optionalMinTimestampInProgressTimestamp.isPresent()) {
          // Use minimum timestamp from in-progress files as consistency point
          consistencyPoint = optionalMinTimestampInProgressTimestamp.get();
        } else if (lastRoundInSync != null) {
          // Use lastRoundInSync end time if no in-progress files
          // Since we are in sync mode, both lastRoundProcessed and lastRoundInSync would be same.
          // However, using lastRoundInSync to be on safe side.
          consistencyPoint = lastRoundInSync.getEndTime();
        } else {
          throw new IOException(
            "Not able to derive consistency point because In Progress directory is empty and lastRoundInSync is not initialized.");
        }
        break;
      case DEGRADED:
      case SYNCED_RECOVERY:
        // In DEGRADED or SYNCED_RECOVERY state: use lastRoundInSync end time
        // (the last known sync point before degradation/recovery)
        if (lastRoundInSync != null) {
          consistencyPoint = lastRoundInSync.getEndTime();
        } else {
          throw new IOException(
            "Not able to derive consistency point because lastRoundInSync is not initialized.");
        }
        break;
      default:
        // Invalid or uninitialized state
        throw new IOException(
          "Not able to derive consistency point for current state: " + currentState);
    }

    return consistencyPoint;
  }
}
