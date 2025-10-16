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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogDiscovery;
import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for discovering and processing replication log files in a round-by-round manner.
 * 
 * This class provides the core framework for:
 * - Discovering replication log files from configured directories (new files and in-progress files)
 * - Processing files in time-based rounds with configurable duration and buffer periods
 * - Tracking progress via lastRoundProcessed to enable resumable processing
 * - Scheduling periodic replay operations via a configurable executor service
 * 
 * Round-based Processing:
 * - Files are organized into replication rounds based on timestamps
 * - Each round represents a time window (e.g., 1 minute) of replication activity
 * - Processing waits for round completion + buffer time before processing to ensure all files are available
 * 
 * Subclasses must implement:
 * - processFile(Path): Defines how individual replication log files are processed
 * - createMetricsSource(): Provides metrics tracking for monitoring
 * - Configuration methods: Thread counts, intervals, probabilities, etc.
 * 
 * File Processing Flow:
 * 1. Discover new files for the current round
 * 2. Mark files as in-progress (move to in-progress directory)
 * 3. Process each file via abstract processFile() method
 * 4. Mark successfully processed files as completed (delete from in-progress)
 * 5. Update lastRoundProcessed to track progress
 */
public abstract class ReplicationLogDiscovery {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationLogDiscovery.class);

    /**
     * Default number of threads in the executor pool for processing replication logs
     */
    private static final int DEFAULT_EXECUTOR_THREAD_COUNT = 1;

    /**
     * Default thread name format for executor threads
     */
    private static final String DEFAULT_EXECUTOR_THREAD_NAME_FORMAT = "ReplicationLogDiscovery-%d";

    /**
     * Default interval in seconds between replay operations
     */
    private static final long DEFAULT_REPLAY_INTERVAL_SECONDS = 10;

    /**
     * Default timeout in seconds for graceful shutdown of the executor service
     */
    private static final long DEFAULT_SHUTDOWN_TIMEOUT_SECONDS = 30;

    /**
     * Default probability (in percentage) for processing files from in-progress directory
     */
    private static final double DEFAULT_IN_PROGRESS_DIRECTORY_PROCESSING_PROBABILITY = 5.0;

    /**
     * Default buffer percentage for waiting time between processing rounds
     */
    private static final double DEFAULT_WAITING_BUFFER_PERCENTAGE = 15.0;

    protected final Configuration conf;
    protected final String haGroupName;
    protected final ReplicationLogTracker replicationLogTracker;
    protected ScheduledExecutorService scheduler;
    protected volatile boolean isRunning = false;
    protected ReplicationRound lastRoundProcessed;
    protected MetricsReplicationLogDiscovery metrics;
    protected long roundTimeMills;
    protected long bufferMillis;

    public ReplicationLogDiscovery(final ReplicationLogTracker replicationLogTracker) {
        this.replicationLogTracker = replicationLogTracker;
        this.haGroupName = replicationLogTracker.getHaGroupName();
        this.conf = replicationLogTracker.getConf();
        this.roundTimeMills = replicationLogTracker.getReplicationShardDirectoryManager()
                .getReplicationRoundDurationSeconds() * 1000L;
        this.bufferMillis = (long) (roundTimeMills * getWaitingBufferPercentage() / 100.0);
    }

    public void init() throws IOException {
        initializeLastRoundProcessed();
        this.metrics = createMetricsSource();
    }

    public void close() {
        if (this.metrics != null) {
            this.metrics.close();
        }
    }

    /**
     * Starts the replication log discovery service by initializing the scheduler and scheduling
     * periodic replay operations. Creates a thread pool with configured thread count and
     * schedules replay tasks at fixed intervals.
     * @throws IOException if there's an error during initialization
     */
    public void start() throws IOException {
        synchronized (this) {
            if (isRunning) {
                LOG.warn("ReplicationLogDiscovery is already running for group: {}", haGroupName);
                return;
            }
            // Initialize and schedule the executors
            scheduler = Executors.newScheduledThreadPool(getExecutorThreadCount(),
                new ThreadFactoryBuilder()
                    .setNameFormat(getExecutorThreadNameFormat()).build());
            scheduler.scheduleAtFixedRate(() -> {
                try {
                    replay();
                } catch (IOException e) {
                    LOG.error("Error during replay", e);
                }
            }, 0, getReplayIntervalSeconds(), TimeUnit.SECONDS);

            isRunning = true;
            LOG.info("ReplicationLogDiscovery started for group: {}", haGroupName);
        }
    }

    /**
     * Stops the replication log discovery service by shutting down the scheduler gracefully.
     * Waits for the configured shutdown timeout before forcing shutdown if necessary.
     * @throws IOException if there's an error during shutdown
     */
    public void stop() throws IOException {
        ScheduledExecutorService schedulerToShutdown;

        synchronized (this) {
            if (!isRunning) {
                LOG.warn("ReplicationLogDiscovery is not running for group: {}", haGroupName);
                return;
            }

            isRunning = false;
            schedulerToShutdown = scheduler;
        }

        if (schedulerToShutdown != null && !schedulerToShutdown.isShutdown()) {
            schedulerToShutdown.shutdown();
            try {
                if (!schedulerToShutdown.awaitTermination(getShutdownTimeoutSeconds(),
                    TimeUnit.SECONDS)) {
                    schedulerToShutdown.shutdownNow();
                }
            } catch (InterruptedException e) {
                schedulerToShutdown.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        LOG.info("ReplicationLogDiscovery stopped for group: {}", haGroupName);
    }

    /**
     * Executes a replay operation for the next set of replication rounds.
     * 
     * This method continuously retrieves and processes rounds using getNextRoundToProcess() until:
     * - No more rounds are ready to process (not enough time has elapsed), or
     * - An error occurs during processing (will retry in next scheduled run)
     * 
     * For each round:
     * 1. Calls processRound() to handle new files and optionally in-progress files
     * 2. Updates lastRoundProcessed to mark progress
     * 3. Retrieves the next round to process
     * 
     * @throws IOException if there's an error during replay processing
     */
    public void replay() throws IOException {
        Optional<ReplicationRound> optionalNextRound = getNextRoundToProcess();
        while (optionalNextRound.isPresent()) {
            ReplicationRound replicationRound = optionalNextRound.get();
            try {
                processRound(replicationRound);
            } catch (IOException e) {
                LOG.error("Failed processing replication round {}. Will retry in next scheduled run.", replicationRound, e);
                break; // stop this run, retry later
            }
            setLastRoundProcessed(replicationRound);
            optionalNextRound = getNextRoundToProcess();
        }
    }

    /**
     * Returns the next replication round to process based on lastRoundProcessed.
     * Ensures sufficient time (round duration + buffer) has elapsed before returning the next round.
     * 
     * @return Optional containing the next round to process, or empty if not enough time has passed
     */
    protected Optional<ReplicationRound> getNextRoundToProcess() {
        long lastRoundEndTimestamp = getLastRoundProcessed().getEndTime();
        long currentTime = EnvironmentEdgeManager.currentTime();
        if (currentTime - lastRoundEndTimestamp < roundTimeMills + bufferMillis) {
            // nothing more to process
            return Optional.empty();
        }
        return Optional.of(new ReplicationRound(lastRoundEndTimestamp, lastRoundEndTimestamp + roundTimeMills));
    }

    /**
     * Processes a single replication round by handling new files and optionally in-progress files.
     * Always processes new files for the round, and conditionally processes in-progress files
     * based on probability.
     * @param replicationRound - The replication round to process
     * @throws IOException if there's an error during round processing
     */
    protected void processRound(ReplicationRound replicationRound) throws IOException {
        LOG.info("Starting to process round: {}", replicationRound);
        // Increment the number of rounds processed
        getMetrics().incrementNumRoundsProcessed();

        // Process new files for the round
        processNewFilesForRound(replicationRound);
        if (shouldProcessInProgressDirectory()) {
            // Conditionally process the in progress files for the round
            processInProgressDirectory();
        }
        LOG.info("Finished processing round: {}", replicationRound);
    }

    /**
     * Determines whether to process in-progress directory files based on configured probability.
     * Uses random number generation to decide if in-progress files should be processed in this
     * cycle.
     * @return true if in-progress directory should be processed, false otherwise
     */
    protected boolean shouldProcessInProgressDirectory() {
        return ThreadLocalRandom.current().nextDouble(100.0)
                < getInProgressDirectoryProcessProbability();
    }

    /**
     * Processes all new files for a specific replication round.
     * Continuously processes files until no new files remain for the round.
     * @param replicationRound - The replication round for which to process new files
     * @throws IOException if there's an error during file processing
     */
    protected void processNewFilesForRound(ReplicationRound replicationRound) throws IOException {
        LOG.info("Starting new files processing for round: {}", replicationRound);
        long startTime = EnvironmentEdgeManager.currentTime();
        List<Path> files = replicationLogTracker.getNewFilesForRound(replicationRound);
        LOG.info("Number of new files for round {} is {}", replicationRound, files.size());
        while (!files.isEmpty()) {
            processOneRandomFile(files);
            files = replicationLogTracker.getNewFilesForRound(replicationRound);
        }
        long duration = EnvironmentEdgeManager.currentTime() - startTime;
        LOG.info("Finished new files processing for round: {} in {}ms", replicationRound, duration);
        getMetrics().updateTimeToProcessNewFiles(duration);
    }

    /**
     * Processes all files in the in-progress directory.
     * Continuously processes files until no in-progress files remain.
     * @throws IOException if there's an error during file processing
     */
    protected void processInProgressDirectory() throws IOException {
        // Increase the count for number of times in progress directory is processed
        getMetrics().incrementNumInProgressDirectoryProcessed();
        LOG.info("Starting in progress directory processing");
        long startTime = EnvironmentEdgeManager.currentTime();
        long oldestTimestampToProcess = replicationLogTracker.getReplicationShardDirectoryManager()
                .getNearestRoundStartTimestamp(EnvironmentEdgeManager.currentTime())
                - getReplayIntervalSeconds() * 1000L;
        List<Path> files = replicationLogTracker.getOlderInProgressFiles(oldestTimestampToProcess);
        LOG.info("Number of In Progress files with oldestTimestampToProcess {} is {}", oldestTimestampToProcess, files.size());
        while (!files.isEmpty()) {
            processOneRandomFile(files);
            files = replicationLogTracker.getOlderInProgressFiles(oldestTimestampToProcess);
        }
        long duration = EnvironmentEdgeManager.currentTime() - startTime;
        getMetrics().updateTimeToProcessInProgressFiles(duration);
    }

    /**
     * Processes a single random file from the provided list.
     * Marks the file as in-progress, processes it, and marks it as completed or failed.
     * @param files - List of files from which to select and process one randomly
     */
    private void processOneRandomFile(final List<Path> files) throws IOException {
        // Pick a random file and process it
        Path file = files.get(ThreadLocalRandom.current().nextInt(files.size()));
        Optional<Path> optionalInProgressFilePath = Optional.empty();
        try {
            optionalInProgressFilePath = replicationLogTracker.markInProgress(file);
            if (optionalInProgressFilePath.isPresent()) {
                processFile(file);
                replicationLogTracker.markCompleted(optionalInProgressFilePath.get());
            }
        } catch (IOException exception) {
            LOG.error("Failed to process the file {}", file, exception);
            optionalInProgressFilePath.ifPresent(replicationLogTracker::markFailed);
            // Not throwing this exception because next time another random file will be retried.
            // If it's persistent failure for in_progress directory,
            // cluster state should to be DEGRADED_STANDBY_FOR_READER.
        }
    }

    /**
     * Handles the processing of a single file.
     * @param path - The file to be processed
     * @throws IOException if there's an error during file processing
     */
    protected abstract void processFile(Path path) throws IOException;

    /** Creates a new metrics source for monitoring operations. */
    protected abstract MetricsReplicationLogDiscovery createMetricsSource();

    /**
     * Initializes lastRoundProcessed based on minimum timestamp from
     * 1. In-progress files (highest priority) - indicates partially processed rounds
     * 2. New files (medium priority) - indicates unprocessed rounds waiting to be replayed
     * 3. Current time (fallback) - used when no files exist, starts from current time
     * 
     * The minimum timestamp is converted to a replication round using getReplicationRoundFromEndTime(),
     * which rounds down to the nearest round boundary to ensure we start from a complete round.
     * 
     * @throws IOException if there's an error reading file timestamps
     */
    protected void initializeLastRoundProcessed() throws IOException {
        Optional<Long> minTimestampFromInProgressFiles =
                getMinTimestampFromInProgressFiles();
        if (minTimestampFromInProgressFiles.isPresent()) {
            LOG.info("Initializing lastRoundProcessed from IN PROGRESS files with minimum timestamp as {}", minTimestampFromInProgressFiles.get());
            this.lastRoundProcessed = replicationLogTracker.getReplicationShardDirectoryManager()
                    .getReplicationRoundFromEndTime(minTimestampFromInProgressFiles.get());
        } else {
            Optional<Long> minTimestampFromNewFiles = getMinTimestampFromNewFiles();
            if (minTimestampFromNewFiles.isPresent()) {
                LOG.info("Initializing lastRoundProcessed from IN files with minimum timestamp as {}", minTimestampFromNewFiles.get());
                this.lastRoundProcessed = replicationLogTracker.getReplicationShardDirectoryManager()
                        .getReplicationRoundFromEndTime(minTimestampFromNewFiles.get());
            } else {
                long currentTime = EnvironmentEdgeManager.currentTime();
                LOG.info("Initializing lastRoundProcessed from current time {}", currentTime);
                this.lastRoundProcessed = replicationLogTracker.getReplicationShardDirectoryManager()
                        .getReplicationRoundFromEndTime(EnvironmentEdgeManager.currentTime());
            }
        }
    }

    /**
     * Get minimum timestamp from in progress files. If no in progress files, return empty.
     * @return minimum timestamp from in progress files.
     */
    protected Optional<Long> getMinTimestampFromInProgressFiles() throws IOException {
        List<Path> inProgressFiles = replicationLogTracker.getInProgressFiles();
        if (inProgressFiles.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(getMinTimestampFromFiles(inProgressFiles));
    }

    /**
     * Get minimum timestamp from new files. If no new files, return empty.
     * @return minimum timestamp from new files.
     */
    protected Optional<Long> getMinTimestampFromNewFiles() throws IOException {
        List<Path> newFiles = replicationLogTracker.getNewFiles();
        if (newFiles.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(getMinTimestampFromFiles(newFiles));
    }

    private long getMinTimestampFromFiles(List<Path> files) {
        long minTimestamp = org.apache.hadoop.hbase.util.EnvironmentEdgeManager.currentTime();
        for (Path file : files) {
            minTimestamp = Math.min(minTimestamp, replicationLogTracker.getFileTimestamp(file));
        }
        return minTimestamp;
    }

    /**
     * Returns the executor thread count. Subclasses can override this method to provide
     * custom name format.
     * @return the executor thread count (default: 1).
     */
    public int getExecutorThreadCount() {
        return DEFAULT_EXECUTOR_THREAD_COUNT;
    }

    /**
     * Returns the executor thread name format. Subclasses can override this method to provide
     * custom name format.
     * @return the executor thread name format (default: ReplicationLogDiscovery-%d).
     */
    public String getExecutorThreadNameFormat() {
        return DEFAULT_EXECUTOR_THREAD_NAME_FORMAT;
    }

    /**
     * Returns the replay interval in seconds. Subclasses can override this method to provide
     * custom intervals.
     * @return The replay interval in seconds (default: 10 seconds).
     */
    public long getReplayIntervalSeconds() {
        return DEFAULT_REPLAY_INTERVAL_SECONDS;
    }

    /**
     * Returns the shutdown timeout in seconds. Subclasses can override this method to provide
     * custom timeout values.
     * @return The shutdown timeout in seconds (default: 30 seconds).
     */
    public long getShutdownTimeoutSeconds() {
        return DEFAULT_SHUTDOWN_TIMEOUT_SECONDS;
    }

    /**
     * Returns the probability (in percentage) for processing files from in-progress directory.
     * Subclasses can override this method to provide custom probabilities.
     * @return The probability (default 5.0%)
     */
    public double getInProgressDirectoryProcessProbability() {
        return DEFAULT_IN_PROGRESS_DIRECTORY_PROCESSING_PROBABILITY;
    }

    /**
     * Returns the buffer percentage for calculating buffer time. Subclasses can override this
     * method to provide custom buffer percentages.
     * @return The buffer percentage (default 15.0%)
     */
    public double getWaitingBufferPercentage() {
        return DEFAULT_WAITING_BUFFER_PERCENTAGE;
    }

    public ReplicationLogTracker getReplicationLogFileTracker() {
        return this.replicationLogTracker;
    }

    public Configuration getConf() {
        return this.conf;
    }

    public String getHaGroupName() {
        return this.haGroupName;
    }

    public boolean isRunning() {
        return isRunning;
    }

    public ReplicationRound getLastRoundProcessed() {
        return lastRoundProcessed;
    }

    public void setLastRoundProcessed(final ReplicationRound replicationRound) {
        this.lastRoundProcessed = replicationRound;
    }

    public MetricsReplicationLogDiscovery getMetrics() {
        return this.metrics;
    }
}
