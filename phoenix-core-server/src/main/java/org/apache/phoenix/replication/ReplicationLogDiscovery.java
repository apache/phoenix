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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogDiscovery;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogFileTracker;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;

/**
 * Abstract base class for discovering and processing replication log files.
 * Manages the lifecycle of replication log processing including starting/stopping the discovery service,
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
    protected final ReplicationLogFileTracker replicationLogFileTracker;
    protected final ReplicationStateTracker replicationStateTracker;
    protected ScheduledExecutorService scheduler;
    protected volatile boolean isRunning = false;
    protected MetricsReplicationLogDiscovery metrics;

    public ReplicationLogDiscovery(final ReplicationLogFileTracker replicationLogFileTracker, final ReplicationStateTracker replicationStateTracker) {
        this.replicationLogFileTracker = replicationLogFileTracker;
        this.replicationStateTracker = replicationStateTracker;
        this.haGroupName = replicationLogFileTracker.getHaGroupName();
        this.conf = replicationLogFileTracker.getConf();
    }

    public void init() {
        this.metrics = createMetricsSource();
    }

    public void close() {
        if(this.metrics != null) {
            this.metrics.close();
        }
    }

    /**
     * Starts the replication log discovery service by initializing the scheduler and scheduling periodic replay operations.
     * Creates a thread pool with configured thread count and schedules replay tasks at fixed intervals.
     * @throws IOException if there's an error during initialization
     */
    public void start() throws IOException {
        synchronized (this) {
            if (isRunning) {
                LOG.warn("ReplicationLogDiscovery is already running for group: {}", haGroupName);
                return;
            }
            // Initialize and schedule the executors
            scheduler = Executors.newScheduledThreadPool(getExecutorThreadCount(), new ThreadFactoryBuilder()
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
        ScheduledExecutorService schedulerToShutdown = null;
        
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
                if (!schedulerToShutdown.awaitTermination(getShutdownTimeoutSeconds(), TimeUnit.SECONDS)) {
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
     * Executes a replay operation for next set of rounds. Retrieves rounds to process and processes each round sequentially.
     * @throws IOException if there's an error during replay processing
     */
    public void replay() throws IOException {
        List<ReplicationRound> replicationRoundList = getRoundsToProcess();
        LOG.info("Number of rounds to process: {}", replicationRoundList.size());
        for(ReplicationRound replicationRound : replicationRoundList) {
            processRound(replicationRound);
        }
    }

    /**
     * Determines which replication rounds need to be processed based on current time and last processed round.
     * @return List of replication rounds that need to be processed
     */
    protected List<ReplicationRound> getRoundsToProcess() {
        long currentTime = EnvironmentEdgeManager.currentTimeMillis();
        long previousRoundEndTime = replicationStateTracker.getLastSuccessfullyProcessedRound().getEndTime();
        long roundTimeMills = replicationLogFileTracker.getReplicationShardDirectoryManager().getReplicationRoundDurationSeconds() * 1000L;
        long bufferMillis = (long) (roundTimeMills * getWaitingBufferPercentage() / 100.0);
        final List<ReplicationRound> replicationRounds = new ArrayList<>();
        for(long startTime = previousRoundEndTime; startTime < currentTime - roundTimeMills - bufferMillis; startTime += roundTimeMills) {
            replicationRounds.add(replicationLogFileTracker.getReplicationShardDirectoryManager().getReplicationRoundFromStartTime(startTime));
        }
        return replicationRounds;
    }

    /**
     * Processes a single replication round by handling new files and optionally in-progress files.
     * Always processes new files for the round, and conditionally processes in-progress files based on probability.
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
     * Uses random number generation to decide if in-progress files should be processed in this cycle.
     * @return true if in-progress directory should be processed, false otherwise
     */
    protected boolean shouldProcessInProgressDirectory() {
        return ThreadLocalRandom.current().nextDouble(100.0) < getInProgressDirectoryProcessProbability();
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
        List<Path> files = replicationLogFileTracker.getNewFilesForRound(replicationRound);
        LOG.trace("Number of new files for round {} is {}", replicationRound, files.size());
        while(!files.isEmpty()) {
            try {
                processOneRandomFile(files);
            } catch (IOException exception) {
                LOG.error("Failed to process the file for round with start time {} and end time {}", replicationRound.getStartTime(), replicationRound.getEndTime(), exception);
            }
            files = replicationLogFileTracker.getNewFilesForRound(replicationRound);
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
        List<Path> files = replicationLogFileTracker.getInProgressFiles();
        LOG.info("Number of new files for in_progress: {}", files.size());
        while(!files.isEmpty()) {
            try {
                processOneRandomFile(files);
            } catch (IOException exception) {
                LOG.error("Failed to process the file for in progress directory", exception);
            }
            files = replicationLogFileTracker.getInProgressFiles();
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
            optionalInProgressFilePath = replicationLogFileTracker.markInProgress(file);
            if(optionalInProgressFilePath.isPresent()) {
                processFile(file);
                replicationLogFileTracker.markCompleted(optionalInProgressFilePath.get());
            }
        } catch (IOException exception) {
            LOG.error("Failed to process the file {}", file, exception);
            optionalInProgressFilePath.ifPresent(replicationLogFileTracker::markFailed);
            throw exception;
        }
    }

    /**
     * Processes a single file.
     * Subclasses must implement this method to provide custom file processing logic.
     * @param path - The file to be processed
     * @throws IOException if there's an error during file processing
     */
    protected abstract void processFile(Path path) throws IOException;

    /** Creates a new metrics source for monitoring operations. */
    protected abstract MetricsReplicationLogDiscovery createMetricsSource();

    public ReplicationStateTracker getReplicationStateTracker() {
        return this.replicationStateTracker;
    }

    public ReplicationLogFileTracker getReplicationLogFileTracker() {
        return this.replicationLogFileTracker;
    }

    public Configuration getConf() {
        return this.conf;
    }

    public String getHaGroupName() {
        return this.haGroupName;
    }

    public int getExecutorThreadCount() {
        return DEFAULT_EXECUTOR_THREAD_COUNT;
    }

    public String getExecutorThreadNameFormat() {
        return DEFAULT_EXECUTOR_THREAD_NAME_FORMAT;
    }

    /**
     * Returns the replay interval in seconds. Subclasses can override this method to provide custom intervals.
     * @return The replay interval in seconds
     */
    public long getReplayIntervalSeconds() {
        return DEFAULT_REPLAY_INTERVAL_SECONDS;
    }

    /**
     * Returns the shutdown timeout in seconds. Subclasses can override this method to provide custom timeout values.
     * @return The shutdown timeout in seconds
     */
    public long getShutdownTimeoutSeconds() {
        return DEFAULT_SHUTDOWN_TIMEOUT_SECONDS;
    }

    public double getInProgressDirectoryProcessProbability() {
        return DEFAULT_IN_PROGRESS_DIRECTORY_PROCESSING_PROBABILITY;
    }

    /**
     * Returns the buffer percentage for calculating buffer time. Subclasses can override this method 
     * to provide custom buffer percentages.
     * @return The buffer percentage (default 15.0%)
     */
    public double getWaitingBufferPercentage() {
        return DEFAULT_WAITING_BUFFER_PERCENTAGE;
    }

    public boolean isRunning() {
        return isRunning;
    }

    public MetricsReplicationLogDiscovery getMetrics() {
        return this.metrics;
    }
}
