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
import org.apache.phoenix.replication.metrics.MetricsReplicationLogDiscovery;
import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for discovering and processing replication log files (to be implemented for
 * replication replay on target and store and forward mode on source).
 * It manages the lifecycle of replication log processing including start/stop of replication log discovery,
 * scheduling periodic replay operations, and processing files from both new and in-progress directories.
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
    protected ReplicationRound lastRoundInSync;
    protected MetricsReplicationLogDiscovery metrics;

    public ReplicationLogDiscovery(final ReplicationLogTracker replicationLogTracker) {
        this.replicationLogTracker = replicationLogTracker;
        this.haGroupName = replicationLogTracker.getHaGroupName();
        this.conf = replicationLogTracker.getConf();
    }

    public void init() throws IOException {
        initializeLastRoundInSync();
        this.metrics = createMetricsSource();
    }

    public void close() {
        if(this.metrics != null) {
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
     * Executes a replay operation for next set of rounds. Retrieves rounds to process and
     * processes each round sequentially.
     * @throws IOException if there's an error during replay processing
     */
    public void replay() throws IOException {
        List<ReplicationRound> replicationRoundList = getRoundsToProcess();
        LOG.info("Number of rounds to process: {}", replicationRoundList.size());
        for(ReplicationRound replicationRound : replicationRoundList) {
            processRound(replicationRound);
            updateStatePostRoundCompletion(replicationRound);
        }
    }

    /**
     * Determines which replication rounds need to be processed based on current time and last sync
     * timestamp.
     * @return List of replication rounds that need to be processed
     */
    protected List<ReplicationRound> getRoundsToProcess() {
        long currentTime = EnvironmentEdgeManager.currentTimeMillis();
        long previousRoundEndTime = getLastRoundInSync().getEndTime();
        long roundTimeMills = replicationLogTracker.getReplicationShardDirectoryManager()
            .getReplicationRoundDurationSeconds() * 1000L;
        long bufferMillis = (long) (roundTimeMills * getWaitingBufferPercentage() / 100.0);
        final List<ReplicationRound> replicationRounds = new ArrayList<>();
        for(long startTime = previousRoundEndTime;
            startTime < currentTime - roundTimeMills - bufferMillis;
            startTime += roundTimeMills) {
            replicationRounds.add(replicationLogTracker.getReplicationShardDirectoryManager()
                .getReplicationRoundFromStartTime(startTime));
        }
        return replicationRounds;
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
        if(shouldProcessInProgressDirectory()) {
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
        return ThreadLocalRandom.current().nextDouble(100.0) <
            getInProgressDirectoryProcessProbability();
    }

    /**
     * Processes all new files for a specific replication round.
     * Continuously processes files until no new files remain for the round.
     * @param replicationRound - The replication round for which to process new files
     * @throws IOException if there's an error during file processing
     */
    protected void processNewFilesForRound(ReplicationRound replicationRound) throws IOException {
        LOG.info("Starting new files processing for round: {}", replicationRound);
        long startTime = EnvironmentEdgeManager.currentTimeMillis();
        List<Path> files = replicationLogTracker.getNewFilesForRound(replicationRound);
        LOG.trace("Number of new files for round {} is {}", replicationRound, files.size());
        while(!files.isEmpty()) {
            processOneRandomFile(files);
            files = replicationLogTracker.getNewFilesForRound(replicationRound);
        }
        long duration = EnvironmentEdgeManager.currentTimeMillis() - startTime;
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
        long startTime = EnvironmentEdgeManager.currentTimeMillis();
        List<Path> files = replicationLogTracker.getInProgressFiles();
        LOG.info("Number of new files for in_progress: {}", files.size());
        while(!files.isEmpty()) {
            processOneRandomFile(files);
            files = replicationLogTracker.getInProgressFiles();
        }
        long duration = EnvironmentEdgeManager.currentTimeMillis() - startTime;
        LOG.info("Starting in progress directory processing in {}ms", duration);
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
            System.out.println("Found optional in progress as " + optionalInProgressFilePath.isPresent());
            if(optionalInProgressFilePath.isPresent()) {
                System.out.println("Starting processing");
                processFile(file);
                System.out.println("Finished processing");
                replicationLogTracker.markCompleted(optionalInProgressFilePath.get());
            }
        } catch (IOException exception) {
            LOG.error("Failed to process the file {}", file, exception);
            optionalInProgressFilePath.ifPresent(replicationLogTracker::markFailed);
            // Not throwing this exception because next time another random file will be retried.
            // If it's persistent failure for in_progress directory, cluster state to be updated accordingly.
        }
    }

    /**
     * Handles the processing of a single file.
     * @param path - The file to be processed
     * @throws IOException if there's an error during file processing
     */
    protected abstract void processFile(Path path) throws IOException;

    protected void updateStatePostRoundCompletion(ReplicationRound replicationRound)
        throws IOException {
        setLastRoundInSync(replicationRound);
    }

    /** Creates a new metrics source for monitoring operations. */
    protected abstract MetricsReplicationLogDiscovery createMetricsSource();

    protected void initializeLastRoundInSync() throws IOException {
        Optional<Long> minTimestampFromInProgressFiles =
                getMinTimestampFromInProgressFiles();
        if (minTimestampFromInProgressFiles.isPresent()) {
            this.lastRoundInSync = replicationLogTracker.getReplicationShardDirectoryManager()
                    .getReplicationRoundFromEndTime(minTimestampFromInProgressFiles.get());
        } else {
            Optional<Long> minTimestampFromNewFiles =
                    getMinTimestampFromNewFiles();
            if (minTimestampFromNewFiles.isPresent()) {
                this.lastRoundInSync = replicationLogTracker.getReplicationShardDirectoryManager()
                        .getReplicationRoundFromEndTime(minTimestampFromNewFiles.get());
            } else {
                this.lastRoundInSync = replicationLogTracker.getReplicationShardDirectoryManager()
                        .getReplicationRoundFromEndTime(org.apache.hadoop.hbase.util.EnvironmentEdgeManager.currentTime());
            }
        }

    }

    protected Optional<Long> getMinTimestampFromInProgressFiles() throws IOException {
        List<Path> inProgressFiles = replicationLogTracker.getInProgressFiles();
        if(inProgressFiles.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(getMinTimestampFromFiles(inProgressFiles));
    }

    protected Optional<Long> getMinTimestampFromNewFiles() throws IOException {
        List<Path> newFiles = replicationLogTracker.getNewFiles();
        if(newFiles.isEmpty()) {
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

    public ReplicationRound getLastRoundInSync() {
        return lastRoundInSync;
    }

    public void setLastRoundInSync(final ReplicationRound replicationRound) {
        this.lastRoundInSync = replicationRound;
    }

    public MetricsReplicationLogDiscovery getMetrics() {
        return this.metrics;
    }
}
