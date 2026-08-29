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
import org.apache.phoenix.replication.metrics.MetricsReplicationLogDiscoveryReplay;
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

  private volatile ReplicationRound lastRoundInSync;

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

    // The LOCAL HA-state listeners below run synchronously on the HA store's state-change
    // notification callback, so they must not block: each does only fast, wait-free work
    // (an atomic state transition plus logging). Blocking here would stall delivery of
    // subsequent HA state notifications for this group.
    HAGroupStateListener degradedListener =
      (groupName, fromState, toState, modifiedTime, clusterType, lastSyncStateTimeInMs) -> {
        if (
          clusterType == ClusterType.LOCAL
            && HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY.equals(toState)
        ) {
          // Unconditional set (not compareAndSet): degradation is an authoritative, fail-closed
          // signal. Whatever the current state (SYNC, SYNCED_RECOVERY, or even NOT_INITIALIZED if a
          // degrade lands mid-init), replay must stop advancing the sync point. There is no single
          // valid "from" state to CAS against, so a CAS would silently drop legitimate degrade
          // signals arriving from the other states. Contrast triggerFailoverListner below, which is
          // deliberately conditional (compareAndSet from DEGRADED only) so it cannot clobber a
          // healthy SYNC or an already-pending SYNCED_RECOVERY.
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
          // compareAndSet from DEGRADED only, not an unconditional set(): DEGRADED is the only
          // state whose lastRoundInSync lags lastRoundProcessed, hence the only state a recovery
          // to STANDBY must rewind from. A STANDBY event that lands while already SYNC (e.g.
          // ABORT_TO_STANDBY -> STANDBY after a prior rewind, or a cache-reconnect redelivery)
          // would otherwise flip SYNC -> SYNCED_RECOVERY and needlessly re-process the frontier
          // round. Symmetric with triggerFailoverListner below; contrast degradedListener above,
          // whose unconditional fail-closed set() is intentional.
          boolean rewindScheduled = replicationReplayState
            .compareAndSet(ReplicationReplayState.DEGRADED, ReplicationReplayState.SYNCED_RECOVERY);
          LOG.info(
            "Cluster recovered detected for {}. replicationReplayState={}, rewindScheduled={}",
            haGroupName, getReplicationReplayState(), rewindScheduled);
        }
      };

    HAGroupStateListener triggerFailoverListner =
      (groupName, fromState, toState, modifiedTime, clusterType, lastSyncStateTimeInMs) -> {
        if (
          clusterType == ClusterType.LOCAL
            && HAGroupStoreRecord.HAGroupState.STANDBY_TO_ACTIVE.equals(toState)
        ) {
          // Direct DEGRADED_STANDBY -> STANDBY_TO_ACTIVE skips the STANDBY event that normally
          // drives recovery. If we are DEGRADED, schedule the rewind so replay() re-syncs from
          // lastRoundInSync before shouldTriggerFailover() (which gates on SYNC) can promote.
          //
          // Arm failoverPending BEFORE the state CAS. This ordering is a contract with the
          // init-window reconcile in initializeLastRoundProcessed(): if this listener fires while
          // init still holds NOT_INITIALIZED, the CAS below no-ops and only the arm survives, so
          // arming first guarantees the reconcile sees the arm whenever it still sees DEGRADED.
          //
          // compareAndSet, not set: a listener firing while already SYNC (healthy failover) or
          // SYNCED_RECOVERY (rewind already pending) must not clobber a good state.
          failoverPending.set(true);
          boolean rewindScheduled = replicationReplayState
            .compareAndSet(ReplicationReplayState.DEGRADED, ReplicationReplayState.SYNCED_RECOVERY);
          LOG.info(
            "Failover trigger detected for {}. replicationReplayState={}, rewindScheduled={}. "
              + "Setting failover pending to {}",
            haGroupName, getReplicationReplayState(), rewindScheduled, failoverPending.get());
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
  protected void processFile(Path path, boolean firstClaim) throws IOException {
    LOG.info("Starting to process file {} (firstClaim={})", path, firstClaim);
    ReplicationLogTracker tracker = getReplicationLogFileTracker();
    final long fileTimestamp;
    try {
      fileTimestamp = tracker.getFileTimestamp(path);
    } catch (NumberFormatException e) {
      // A malformed file name cannot be anchored to a round. getFileTimestamp is validated on the
      // new-files path (getNewFilesForRound skips names that fail to parse) but not on the reclaim
      // path, so convert the unchecked parse failure into the IOException that every other per-file
      // failure uses. That keeps a single bad name isolated to processOneRandomFile's catch (marked
      // failed and retry-counted) instead of escaping as a RuntimeException that aborts the whole
      // in-progress sweep for the cycle.
      throw new IOException("Cannot extract timestamp from replication log file name: " + path, e);
    }
    long roundEligibleTime = getRoundEligibleTime(fileTimestamp);
    // Pickup lag (eligible -> claimed) is recorded only on the first claim (the new-files path).
    // markInProgress re-stamps a fresh rename timestamp on every reclaim of an already-in-progress
    // file, so recording on the in-progress path (firstClaim=false) would sample eligible ->
    // latest-reclaim, not eligible -> first-pickup, and multi-count the same file across sweeps.
    // Skip if the in-progress name carries no rename timestamp (should not happen for a file that
    // reached processFile).
    if (firstClaim) {
      Optional<Long> renameTs = tracker.getRenameTimestamp(path);
      if (renameTs.isPresent()) {
        getReplayMetrics().updatePickupLag(Math.max(0L, renameTs.get() - roundEligibleTime));
      } else {
        // A first-claim file was just renamed into the in-progress directory by markInProgress,
        // which always stamps a rename timestamp, so this should not happen. Drop the pickup-lag
        // sample rather than record a bogus one, but leave a breadcrumb -- getRenameTimestamp only
        // logs on a malformed 4-part name, not on an unexpectedly short name.
        LOG.warn("No rename timestamp on first-claim file {}; skipping pickup-lag sample", path);
      }
    }
    replayLogFile(path);
    // End-to-end lag (eligible -> replay done): recorded only after a successful replayLogFile
    // return. On failure replayLogFile throws and logFileReplayFailureCount already fires, so an
    // end-to-end lag sample for an unfinished replay would be misleading. Every successfully
    // replayed file is sampled here -- including rotation-only / zero-mutation files, which the
    // mutationsPerFile histogram deliberately excludes -- so the two distributions cover different
    // file populations.
    getReplayMetrics().updateEndToEndReplayLag(
      Math.max(0L, EnvironmentEdgeManager.currentTime() - roundEligibleTime));
  }

  /**
   * Replays a single log file through the {@link ReplicationLogProcessor}. Extracted as a seam so
   * the lag-recording logic in {@link #processFile(Path, boolean)} can be unit-tested without a
   * live processor or file system.
   * @param path the in-progress log file to replay
   * @throws IOException if replay fails; the caller then skips the end-to-end lag sample
   */
  protected void replayLogFile(Path path) throws IOException {
    ReplicationLogProcessor.get(getConf(), getHaGroupName())
      .processLogFile(getReplicationLogFileTracker().getFileSystem(), path);
  }

  /**
   * Returns the wall-clock instant (ms) at which the round owning a file with the given creation
   * timestamp became eligible for processing: the round's end boundary plus the waiting buffer.
   * This mirrors the eligibility gate used by {@link #getNextRoundToProcess()} (a round is eligible
   * once {@code currentTime >= roundEnd + bufferMillis}) and is the zero-reference for the
   * replay-lag metrics, so they exclude the fixed built-in wait rather than measuring raw file age.
   * <p>
   * Round bounds are inclusive at both ends (see {@code getNewFilesForRound}), so consecutive
   * rounds share a boundary and a file whose creation timestamp lands exactly on a round boundary
   * ({@code creationTs % roundTimeMills == 0}) matches both the earlier round (as its end) and the
   * later round (as its start). The earlier round runs first and claims the file, so the owning
   * round's end is {@code creationTs} itself in that case, not {@code creationTs + roundTimeMills}.
   * Anchoring to the later round would over-count the eligibility by one full round and clamp the
   * resulting lag to zero.
   * @param creationTs the file creation timestamp (first component of the log file name)
   * @return the round-eligible wall-clock instant in milliseconds
   */
  private long getRoundEligibleTime(long creationTs) {
    long roundStart = replicationLogTracker.getReplicationShardDirectoryManager()
      .getNearestRoundStartTimestamp(creationTs);
    long owningRoundEnd = (creationTs == roundStart) ? creationTs : roundStart + roundTimeMills;
    return owningRoundEnd + bufferMillis;
  }

  /**
   * Initializes lastRoundProcessed and lastRoundInSync based on the persisted HA group state.
   * <ul>
   * <li>DEGRADED_STANDBY: sets replicationReplayState to DEGRADED and initializes both rounds from
   * the last known good sync point via {@link #initLastRoundsFromLastSyncPoint(HAGroupStoreRecord)}
   * (lastRoundInSync from the minimum of lastSyncStateTimeInMs and the file frontier, so it
   * represents the last consistent point before degradation).</li>
   * <li>STANDBY_TO_ACTIVE: a restart while already mid-failover (e.g. after a direct
   * DEGRADED_STANDBY -&gt; STANDBY_TO_ACTIVE transition). Initializes both rounds from the last
   * known good sync point, then sets replicationReplayState to SYNCED_RECOVERY only when a rewind
   * is actually pending (lastRoundInSync behind lastRoundProcessed) so the first replay() rewinds
   * before failover can promote; when the rounds are already equal it initializes directly to SYNC
   * so promotion is not delayed by a round window.</li>
   * <li>Other states (e.g. STANDBY): sets replicationReplayState to SYNC, delegates to the parent
   * to initialize lastRoundProcessed, and sets lastRoundInSync equal to lastRoundProcessed.</li>
   * </ul>
   * When the state is STANDBY_TO_ACTIVE, failoverPending is also armed.
   * @throws IOException if there's an error reading HA group state or file timestamps
   */
  @Override
  protected void initializeLastRoundProcessed() throws IOException {
    LOG.info("Initializing last round processed for haGroup: {}", haGroupName);
    // Sample current time BEFORE reading the HA group state, so if the group transitions (e.g.
    // SYNC -> DEGRADED_STANDBY) during init, the starting round still anchors to when init began
    // rather than to a later point after the state read and file scans.
    long frontierStartTime = EnvironmentEdgeManager.currentTime();
    HAGroupStoreRecord haGroupStoreRecord = getHAGroupRecord();
    HAGroupStoreRecord.HAGroupState haGroupState = haGroupStoreRecord.getHAGroupState();
    LOG.info("Found HA Group state during initialization as {} for haGroup: {}", haGroupState,
      haGroupName);
    // Each branch below sets the initial state with compareAndSet(NOT_INITIALIZED, ...), not a
    // plain set: the LOCAL state listeners are subscribed in init() before this runs, so a
    // concurrent LOCAL transition may have already advanced the state off NOT_INITIALIZED. When
    // that happens the CAS no-ops and we deliberately keep the listener's value -- a live
    // transition is more recent (and thus more authoritative) than the record snapshot read above.
    if (HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY.equals(haGroupState)) {
      replicationReplayState.compareAndSet(ReplicationReplayState.NOT_INITIALIZED,
        ReplicationReplayState.DEGRADED);
      initLastRoundsFromLastSyncPoint(haGroupStoreRecord, frontierStartTime);
    } else if (HAGroupStoreRecord.HAGroupState.STANDBY_TO_ACTIVE.equals(haGroupState)) {
      // Restarted while already in STANDBY_TO_ACTIVE (e.g. an RS bounce after a direct
      // DEGRADED_STANDBY -> STANDBY_TO_ACTIVE transition). No listener fires on a fresh process, so
      // initialize as if recovering: lastRoundInSync at the last good sync point,
      // lastRoundProcessed
      // from the file frontier.
      initLastRoundsFromLastSyncPoint(haGroupStoreRecord, frontierStartTime);
      // Enter SYNCED_RECOVERY only when there is real rewind work (lastRoundInSync behind
      // lastRoundProcessed) so the first replay() rewinds before shouldTriggerFailover() promotes.
      // When the rounds are equal (the common "already fully replayed, RS bounced before
      // setHAGroupStatusToSync() completed" case) there is nothing to rewind, so initialize
      // directly to SYNC. Otherwise the SYNCED_RECOVERY -> SYNC CAS (which only fires inside the
      // round-processing loop) would not run until a round becomes time-eligible, delaying
      // promotion by ~one round window.
      ReplicationReplayState initialState =
        lastRoundInSync.getEndTime() < lastRoundProcessed.getEndTime()
          ? ReplicationReplayState.SYNCED_RECOVERY
          : ReplicationReplayState.SYNC;
      // Distinct, greppable signal that this process restarted mid-failover, and which sub-case it
      // hit, so a restart-driven promotion is diagnosable from logs (not inferred from the generic
      // init summary below).
      if (initialState == ReplicationReplayState.SYNCED_RECOVERY) {
        LOG.info(
          "Restart into STANDBY_TO_ACTIVE with rewind pending for haGroup {}: lastRoundInSync={} "
            + "behind lastRoundProcessed={}; entering SYNCED_RECOVERY to re-sync before promotion.",
          haGroupName, lastRoundInSync, lastRoundProcessed);
      } else {
        LOG.info("Restart into STANDBY_TO_ACTIVE with nothing to rewind for haGroup {}: "
          + "lastRoundInSync == lastRoundProcessed ({}); entering SYNC, promotion eligible "
          + "immediately.", haGroupName, lastRoundProcessed);
      }
      replicationReplayState.compareAndSet(ReplicationReplayState.NOT_INITIALIZED, initialState);
    } else {
      replicationReplayState.compareAndSet(ReplicationReplayState.NOT_INITIALIZED,
        ReplicationReplayState.SYNC);
      super.initializeLastRoundProcessed(frontierStartTime);
      this.lastRoundInSync =
        new ReplicationRound(lastRoundProcessed.getStartTime(), lastRoundProcessed.getEndTime());
    }
    LOG.info(
      "Initialized last round processed as {}, last round in sync as {} and "
        + "replication replay state as {}",
      lastRoundProcessed, lastRoundInSync, replicationReplayState);

    // Arm failoverPending during initialization when restarting mid-failover. Plain set (not
    // compareAndSet): it starts false and this is the only initializer, so there is no prior value
    // to guard; this also matches triggerFailoverListner, which arms the same flag with set(true).
    if (HAGroupStoreRecord.HAGroupState.STANDBY_TO_ACTIVE.equals(haGroupState)) {
      failoverPending.set(true);
    }

    // Reconcile the init-window race between the (possibly stale) record read above and the live
    // LOCAL listeners subscribed in init(). A direct DEGRADED_STANDBY -> STANDBY_TO_ACTIVE flip can
    // fire triggerFailoverListner while init still holds NOT_INITIALIZED: its CAS(DEGRADED,
    // SYNCED_RECOVERY) no-ops and only failoverPending survives; then the DEGRADED_STANDBY branch's
    // CAS(NOT_INITIALIZED, DEGRADED) lands, leaving state DEGRADED with a failover pending. Nothing
    // re-fires (the record is already STANDBY_TO_ACTIVE; a direct failover never revisits STANDBY),
    // so shouldTriggerFailover() -- gated on SYNC -- never promotes until an RS restart heals it.
    // Finish the promotion the listener intended. No-op (no warn) on a normal STANDBY_TO_ACTIVE
    // restart (state already SYNCED_RECOVERY or SYNC) and skipped for a legitimate DEGRADED standby
    // (failoverPending false). triggerFailoverListner arms failoverPending before its CAS, so a
    // still-DEGRADED state here guarantees the arm is visible, closing the window.
    if (failoverPending.get()) {
      if (
        replicationReplayState.compareAndSet(ReplicationReplayState.DEGRADED,
          ReplicationReplayState.SYNCED_RECOVERY)
      ) {
        LOG.warn(
          "Reconciled init-window failover race for haGroup {}: state was DEGRADED with a pending "
            + "failover (a DEGRADED_STANDBY -> STANDBY_TO_ACTIVE flip raced init); promoted to "
            + "SYNCED_RECOVERY so replay can rewind from lastRoundInSync and then promote.",
          haGroupName);
      }
    }
  }

  /**
   * Initializes {@link #lastRoundProcessed} and {@link #lastRoundInSync} from the last known good
   * sync point. Used by the DEGRADED_STANDBY and STANDBY_TO_ACTIVE branches, which both need a
   * rewind to the last consistent point. lastRoundProcessed is derived from the minimum timestamp
   * across IN-PROGRESS and IN files (or {@code frontierStartTime} when no files exist);
   * lastRoundInSync is derived from the minimum of the record's lastSyncStateTimeInMs and that file
   * frontier, so it never sits ahead of the last synced data. When lastSyncStateTimeInMs is 0 (no
   * known sync point), it falls back to the file frontier so lastRoundInSync collapses onto
   * lastRoundProcessed instead of rewinding to the epoch.
   * @param haGroupStoreRecord the persisted HA group record supplying lastSyncStateTimeInMs
   * @param frontierStartTime  current time sampled at the start of initialization; the file
   *                           frontier upper bound, and the sole basis when no files exist
   */
  private void initLastRoundsFromLastSyncPoint(HAGroupStoreRecord haGroupStoreRecord,
    long frontierStartTime) throws IOException {
    long minimumTimestampFromFiles = frontierStartTime;
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
    // A lastSyncStateTimeInMs of 0 means "no known sync point" (never synced, or a record that
    // predates the field). Do NOT feed 0 into the min: getReplicationRoundFromEndTime(0) returns
    // ReplicationRound(0, 0), which would rewind SYNCED_RECOVERY to the epoch and drive the
    // consistency point to 0 (retain-everything). Fall back to the file frontier so lastRoundInSync
    // collapses onto lastRoundProcessed (no rewind) when there is no known sync point.
    long lastSyncStateTimeInMs = haGroupStoreRecord.getLastSyncStateTimeInMs();
    long lastSyncBasis = lastSyncStateTimeInMs > 0L
      ? Math.min(lastSyncStateTimeInMs, minimumTimestampFromFiles)
      : minimumTimestampFromFiles;
    this.lastRoundInSync = replicationLogTracker.getReplicationShardDirectoryManager()
      .getReplicationRoundFromEndTime(lastSyncBasis);
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

    // Update consistency point metric at the start of replay
    try {
      long consistencyPoint = getConsistencyPoint();
      LOG.debug("Consistency point for HAGroup: {} before starting the replay is {}.", haGroupName,
        consistencyPoint);
      getReplayMetrics().updateConsistencyPoint(consistencyPoint);
    } catch (IOException exception) {
      LOG.warn("Failed to get the consistency point for HA Group: {} at start of replay",
        haGroupName, exception);
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

      // Update consistency point metric after processing each round
      try {
        long consistencyPoint = getConsistencyPoint();
        LOG.debug("Consistency point for HAGroup: {} after processing round: {} is {}", haGroupName,
          replicationRound, consistencyPoint);
        getReplayMetrics().updateConsistencyPoint(consistencyPoint);
      } catch (IOException exception) {
        LOG.warn("Failed to get the consistency point for HA Group: {} after processing round: {}",
          haGroupName, replicationRound, exception);
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

  /**
   * Returns the replay-specific metrics interface.
   * @return MetricsReplicationLogDiscoveryReplay instance
   */
  protected MetricsReplicationLogDiscoveryReplay getReplayMetrics() {
    return (MetricsReplicationLogDiscoveryReplay) getMetrics();
  }

  @Override
  public String getExecutorThreadNameFormat() {
    return EXECUTOR_THREAD_NAME_FORMAT;
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

  /**
   * Effective HA record used to decide the replay mode at startup. A STANDBY whose peer cluster is
   * not currently visible is reported as DEGRADED_STANDBY, so this RegionServer starts failed
   * closed until the peer is confirmed reachable. Runtime degrade/recover transitions arrive
   * through the LOCAL state subscribers registered in {@link #init()}.
   */
  protected HAGroupStoreRecord getHAGroupRecord() throws IOException {
    Optional<HAGroupStoreRecord> optionalHAGroupStateRecord =
      HAGroupStoreManager.getInstance(conf).getEffectiveHAGroupStoreRecord(haGroupName);
    if (!optionalHAGroupStateRecord.isPresent()) {
      throw new IOException("HAGroupStoreRecord not found for HA Group: " + haGroupName);
    }
    return optionalHAGroupStateRecord.get();
  }

  /**
   * Determines whether failover should be triggered based on completion criteria. Failover is safe
   * to trigger when all of the following conditions are met: 1. A failover has been requested
   * (failoverPending is true) 2. The replication replay state is SYNC (not SYNCED_RECOVERY,
   * DEGRADED, or NOT_INITIALIZED) 3. No files are currently in the in-progress directory 4. No new
   * files exist from the next round to process up to the current timestamp round. The fourth
   * condition checks for new files in the range from nextRoundToProcess (derived from
   * getLastRoundProcessed()) to currentTimestampRound (derived from current time). This ensures all
   * replication logs up to the current time have been processed and any pending rewind has
   * completed before transitioning the cluster from STANDBY to ACTIVE state.
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
    // Check if replay state is SYNC; block failover during SYNCED_RECOVERY (rewind pending),
    // DEGRADED, or NOT_INITIALIZED to prevent bypassing the rewind logic
    if (replicationReplayState.get() != ReplicationReplayState.SYNC) {
      LOG.debug("Failover not triggered. Replay state is {}, not SYNC.",
        replicationReplayState.get());
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
   * whose timestamp is less than this consistency point timestamp have been replayed.
   * <p>
   * In SYNC state with files in progress, the minimum IN-PROGRESS timestamp is aligned down to the
   * start time of the round it belongs to. Files within a round are moved to IN-PROGRESS in random
   * order, so the minimum IN-PROGRESS timestamp may not be the oldest file of its round (an older
   * sibling can still be waiting in the IN directory). Every file of a round has a timestamp
   * greater than or equal to the round start, and earlier rounds are fully replayed first, so the
   * round start is a safe exclusive upper bound that never advances past unreplayed files - without
   * listing the IN directories.
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
          // Align the minimum in-progress timestamp down to the start of the round it belongs to,
          // so the consistency point never advances past older, still-unreplayed files of the same
          // round that are waiting in the IN directory (files are picked in random order).
          long minTimestampInProgress = optionalMinTimestampInProgressTimestamp.get();
          consistencyPoint = replicationLogTracker.getReplicationShardDirectoryManager()
            .getNearestRoundStartTimestamp(minTimestampInProgress);
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
