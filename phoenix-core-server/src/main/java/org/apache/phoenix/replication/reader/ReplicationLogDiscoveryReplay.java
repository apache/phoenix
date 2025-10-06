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
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.phoenix.jdbc.ClusterType;
import org.apache.phoenix.jdbc.HAGroupStateListener;
import org.apache.phoenix.jdbc.HAGroupStoreManager;
import org.apache.phoenix.jdbc.HAGroupStoreRecord;
import org.apache.phoenix.replication.ReplicationLogDiscovery;
import org.apache.phoenix.replication.ReplicationLogTracker;
import org.apache.phoenix.replication.ReplicationRound;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogDiscovery;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogDiscoveryReplayImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * State-aware implementation of ReplicationLogDiscovery for HA replication replay on standby clusters.
 * 
 * This class extends the base ReplicationLogDiscovery with support for three replication states:
 * - SYNC: Normal synchronized processing where both lastRoundProcessed and lastRoundInSync advance together
 * - DEGRADED: Degraded mode where lastRoundProcessed advances but lastRoundInSync is preserved
 * - SYNCED_RECOVERY: Recovery mode that rewinds to lastRoundInSync and re-processes from that point
 * 
 * Key features:
 * - Uses getFirstRoundToProcess() to start replay from lastRoundInSync (not just from lastRoundProcessed)
 * - Dynamically responds to HA state changes via listeners during replay execution
 * - Maintains separate tracking of lastRoundProcessed and lastRoundInSync for recovery scenarios
 * - Integrates with HAGroupStoreManager for cluster state coordination
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
     * Default waiting buffer percentage. Buffer time is calculated as this percentage of round
     * time.
     */
    public static final double DEFAULT_WAITING_BUFFER_PERCENTAGE = 15.0;

    private ReplicationRound lastRoundInSync;

    // AtomicReference ensures listener updates are visible to replay thread
    private final AtomicReference<ReplicationReplayState> replicationReplayState =
            new AtomicReference<>(ReplicationReplayState.NOT_INITIALIZED);

    public static final List<HAGroupStoreRecord.HAGroupState> WRITER_DEGRADED_STATES = Arrays.asList(HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY_FOR_WRITER, HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY);

    public ReplicationLogDiscoveryReplay(final ReplicationLogTracker
        replicationLogReplayFileTracker) {
        super(replicationLogReplayFileTracker);
    }

    @Override
    public void init() throws IOException {
        HAGroupStateListener degradedListener = (groupName, toState, modifiedTime, clusterType) -> {
            if (clusterType == ClusterType.LOCAL && WRITER_DEGRADED_STATES.contains(toState)) {
                replicationReplayState.set(ReplicationReplayState.DEGRADED);
                LOG.info("Cluster degraded detected for {}. replicationReplayState={}", haGroupName, ReplicationReplayState.DEGRADED);
            }
        };

        HAGroupStateListener recoveryListener = (groupName, toState, modifiedTime, clusterType) -> {
            if (clusterType == ClusterType.LOCAL && !WRITER_DEGRADED_STATES.contains(toState)) {
                replicationReplayState.set(ReplicationReplayState.SYNCED_RECOVERY);
                LOG.info("Cluster recovered detected for {}. replicationReplayState={}", haGroupName, ReplicationReplayState.SYNCED_RECOVERY);
            }
        };

        HAGroupStoreManager haGroupStoreManager = HAGroupStoreManager.getInstance(conf);

        // Subscribe degraded states
        haGroupStoreManager.subscribeToTargetState(haGroupName, HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY_FOR_WRITER, ClusterType.LOCAL, degradedListener);
        haGroupStoreManager.subscribeToTargetState(haGroupName, HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY, ClusterType.LOCAL, degradedListener);

        // Subscribe recovery/healthy states
        haGroupStoreManager.subscribeToTargetState(haGroupName, HAGroupStoreRecord.HAGroupState.STANDBY, ClusterType.LOCAL, recoveryListener);

        // TODO: Add recoveryLister for DEGRADED_STANDBY_FOR_READER state when it goes from DEGRADED_STANDBY -> DEGRADED_STANDBY_FOR_READER.
        // For this need to have source state as part of callback method, because we only want to re-calculate currentRoundTimestamp when cluster
        // switch from DEGRADED_STANDBY -> DEGRADED_STANDBY_FOR_READER (and not when STANDBY -> DEGRADED_STANDBY_FOR_READER)

        super.init();
    }

    @Override
    protected void processFile(Path path) throws IOException {
        LOG.info("Starting to process file {}", path);
        ReplicationLogProcessor.get(getConf(), getHaGroupName())
            .processLogFile(getReplicationLogFileTracker().getFileSystem(), path);
    }

    /**
     * Initializes lastRoundProcessed and lastRoundInSync based on HA group state.
     * 
     * For DEGRADED states (DEGRADED_STANDBY, DEGRADED_STANDBY_FOR_WRITER):
     * - Sets replicationReplayState to DEGRADED
     * - Initializes lastRoundProcessed from minimum of: in-progress files, new files, or current time
     * - Initializes lastRoundInSync from minimum of: lastSyncStateTimeInMs (from HA Store) or minimum
     *   timestamp from IN and IN PROGRESS files
     * - This ensures lastRoundInSync represents the last known good sync point before degradation
     * 
     * For SYNC states (STANDBY):
     * - Sets replicationReplayState to SYNC
     * - Calls parent's initializeLastRoundProcessed() to initialize lastRoundProcessed
     * - Sets lastRoundInSync equal to lastRoundProcessed (both are in sync)
     * 
     * @throws IOException if there's an error reading HA group state or file timestamps
     */
    @Override
    protected void initializeLastRoundProcessed() throws IOException {
        HAGroupStoreRecord haGroupStoreRecord = getHAGroupRecord();
        if (WRITER_DEGRADED_STATES.contains(haGroupStoreRecord.getHAGroupState())) {
            replicationReplayState.compareAndSet(ReplicationReplayState.NOT_INITIALIZED, ReplicationReplayState.DEGRADED);
            long minimumTimestampFromFiles = EnvironmentEdgeManager.currentTime();
            Optional<Long> minTimestampFromInProgressFiles = getMinTimestampFromInProgressFiles();
            Optional<Long> minTimestampFromNewFiles = getMinTimestampFromNewFiles();
            if (minTimestampFromInProgressFiles.isPresent()) {
                LOG.info("Found minimum timestamp from IN PROGRESS files as {}", minTimestampFromInProgressFiles.get());
                minimumTimestampFromFiles = Math.min(minimumTimestampFromFiles, minTimestampFromInProgressFiles.get());
            }
            if(minTimestampFromNewFiles.isPresent()) {
                LOG.info("Found minimum timestamp from IN files as {}", minTimestampFromNewFiles.get());
                minimumTimestampFromFiles = Math.min(minimumTimestampFromFiles, minTimestampFromNewFiles.get());
            }
            this.lastRoundProcessed = replicationLogTracker.getReplicationShardDirectoryManager()
                    .getReplicationRoundFromEndTime(minimumTimestampFromFiles);
            this.lastRoundInSync = replicationLogTracker.getReplicationShardDirectoryManager()
                    .getReplicationRoundFromEndTime(Math.min(haGroupStoreRecord.getLastSyncStateTimeInMs(), minimumTimestampFromFiles));
        } else {
            replicationReplayState.compareAndSet(ReplicationReplayState.NOT_INITIALIZED, ReplicationReplayState.SYNC);
            super.initializeLastRoundProcessed();
            this.lastRoundInSync = new ReplicationRound(lastRoundProcessed.getStartTime(), lastRoundProcessed.getEndTime());
        }
        LOG.info("Initialized last round processed as {}, last round in sync as {} and replication replay state as {}", lastRoundProcessed, lastRoundInSync, replicationReplayState);
    }

    /**
     * Executes a replay operation with state-aware processing for HA replication scenarios.
     * 
     * This method extends the base replay() by handling three replication states:
     * 
     * 1. SYNC: Normal processing
     *    - Updates both lastRoundProcessed and lastRoundInSync
     *    - Both pointers advance together, indicating cluster is fully synchronized
     * 
     * 2. DEGRADED: Degraded mode processing
     *    - Updates only lastRoundProcessed (advances in memory)
     *    - Does NOT update lastRoundInSync (preserves last known good sync point)
     *    - Allows processing to continue during degradation without losing sync reference
     * 
     * 3. SYNCED_RECOVERY: Recovery mode
     *    - Rewinds lastRoundProcessed back to lastRoundInSync
     *    - Transitions to SYNC state
     *    - Re-processes rounds from last known good sync point to ensure data consistency
     * 
     * The first round is retrieved using getFirstRoundToProcess() (starts from lastRoundInSync),
     * subsequent rounds use getNextRoundToProcess() (starts from lastRoundProcessed).
     * 
     * State transitions can occur dynamically via HA group listeners during replay execution.
     * 
     * @throws IOException if there's an error during replay processing
     */
    @Override
    public void replay() throws IOException {
        LOG.info("Starting replay with lastRoundProcessed={}, lastRoundInSync={}", lastRoundProcessed, lastRoundInSync);
        Optional<ReplicationRound> optionalNextRound = getFirstRoundToProcess();
        while (optionalNextRound.isPresent()) {
            ReplicationRound replicationRound = optionalNextRound.get();
            try {
                processRound(replicationRound);
            } catch (IOException e) {
                LOG.error("Failed processing replication round {}. Will retry in next scheduled run.", replicationRound, e);
                break; // stop this run, retry later
            }

            // Always read latest listener state
            ReplicationReplayState currentState = replicationReplayState.get();

            switch (currentState) {
                case SYNCED_RECOVERY:
                    // Rewind to last in-sync round
                    LOG.info("SYNCED_RECOVERY detected, rewinding to lastRoundInSync={}", lastRoundInSync);
                    setLastRoundProcessed(lastRoundInSync);
                    // Only reset to NORMAL if state hasn't been flipped to DEGRADED
                    replicationReplayState.compareAndSet(ReplicationReplayState.SYNCED_RECOVERY, ReplicationReplayState.SYNC);
                    break;

                case SYNC:
                    // Normal processing, update last round processed and in-sync
                    setLastRoundProcessed(replicationRound);
                    setLastRoundInSync(replicationRound);
                    LOG.info("Processed round {} successfully, lastRoundProcessed={}, lastRoundInSync={}", replicationRound, lastRoundProcessed, lastRoundInSync);
                    break;

                case DEGRADED:
                    // Only update last round processed, and NOT last round in sync
                    setLastRoundProcessed(replicationRound);
                    LOG.info("Processed round {} successfully with cluster in DEGRADED state, lastRoundProcessed={}, lastRoundInSync={}", replicationRound, lastRoundProcessed, lastRoundInSync);
                    break;
            }
            optionalNextRound = getNextRoundToProcess();
        }
    }

    /**
     * Returns the first replication round to process based on lastRoundInSync.
     * Unlike getNextRoundToProcess() which uses lastRoundProcessed, this method uses lastRoundInSync
     * to ensure replay starts from the last known synchronized point. This is critical for recovery
     * scenarios where lastRoundProcessed may be ahead of lastRoundInSync.
     * 
     * @return Optional containing the first round to process, or empty if not enough time has passed
     */
    private Optional<ReplicationRound> getFirstRoundToProcess() {
        long lastRoundEndTimestamp = getLastRoundInSync().getEndTime();
        long currentTime = EnvironmentEdgeManager.currentTime();
        if (currentTime - lastRoundEndTimestamp < roundTimeMills + bufferMillis) {
            // nothing more to process
            return Optional.empty();
        }
        return Optional.of(new ReplicationRound(lastRoundEndTimestamp, lastRoundEndTimestamp + roundTimeMills));
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
        return getConf().getDouble(
            REPLICATION_REPLAY_IN_PROGRESS_DIRECTORY_PROCESSING_PROBABILITY_KEY,
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

    protected HAGroupStoreRecord getHAGroupRecord() throws IOException {
        Optional<HAGroupStoreRecord> optionalHAGroupStateRecord = HAGroupStoreManager.getInstance(conf).getHAGroupStoreRecord(haGroupName);
        if(!optionalHAGroupStateRecord.isPresent()) {
            throw new IOException("HAGroupStoreRecord not found for HA Group: " + haGroupName);
        }
        return optionalHAGroupStateRecord.get();
    }

    public enum ReplicationReplayState {
        NOT_INITIALIZED, // not initialized yet
        SYNC,          // fully in sync / standby
        DEGRADED,        // degraded for writer
        SYNCED_RECOVERY  // came back from degraded → standby, needs rewind
    }
}
