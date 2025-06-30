package org.apache.phoenix.replication.reader;

import org.apache.hadoop.fs.Path;
import org.apache.phoenix.replication.ReplicationLogDiscovery;
import org.apache.phoenix.replication.ReplicationStateTracker;

import java.io.IOException;

public class ReplicationReplayLogDiscovery extends ReplicationLogDiscovery {

    private static final String EXECUTOR_THREAD_NAME_FORMAT = "Phoenix-Replication-Replay-%d";

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

    // Default values (same as parent class)
    
    /**
     * Default replay interval in seconds. Controls how frequently the replay process runs.
     */
    private static final long DEFAULT_REPLAY_INTERVAL_SECONDS = 60;
    
    /**
     * Default shutdown timeout in seconds. Maximum time to wait for executor service to shutdown gracefully.
     */
    private static final long DEFAULT_SHUTDOWN_TIMEOUT_SECONDS = 30;
    
    /**
     * Default number of executor threads for processing replication log files.
     */
    private static final int DEFAULT_EXECUTOR_THREAD_COUNT = 1;
    
    /**
     * Default probability (in percentage) for processing in-progress directory during each replay cycle.
     */
    private static final double DEFAULT_IN_PROGRESS_DIRECTORY_PROCESSING_PROBABILITY = 5.0;
    
    /**
     * Default waiting buffer percentage. Buffer time is calculated as this percentage of round time.
     */
    private static final double DEFAULT_WAITING_BUFFER_PERCENTAGE = 15.0;

    public ReplicationReplayLogDiscovery(final ReplicationLogReplayFileTracker replicationLogReplayFileTracker, final ReplicationStateTracker replicationStateTracker) {
        super(replicationLogReplayFileTracker, replicationStateTracker);
    }

    @Override
    protected void processFile(Path path) throws IOException {
        System.out.println("Processing file " + path);
        ReplicationLogProcessor.get(getConf(), getHaGroupName()).processLogFile(getReplicationLogFileTracker().getFileSystem(), path);
    }

    @Override
    public String getExecutorThreadNameFormat() {
        return EXECUTOR_THREAD_NAME_FORMAT;
    }

    @Override
    public long getReplayIntervalSeconds() {
        return getConf().getLong(REPLICATION_REPLAY_INTERVAL_SECONDS_KEY, DEFAULT_REPLAY_INTERVAL_SECONDS);
    }

    @Override
    public long getShutdownTimeoutSeconds() {
        return getConf().getLong(REPLICATION_REPLAY_SHUTDOWN_TIMEOUT_SECONDS_KEY, DEFAULT_SHUTDOWN_TIMEOUT_SECONDS);
    }

    @Override
    public int getExecutorThreadCount() {
        return getConf().getInt(REPLICATION_REPLAY_EXECUTOR_THREAD_COUNT_KEY, DEFAULT_EXECUTOR_THREAD_COUNT);
    }

    @Override
    public double getInProgressDirectoryProcessProbability() {
        return getConf().getDouble(REPLICATION_REPLAY_IN_PROGRESS_DIRECTORY_PROCESSING_PROBABILITY_KEY, DEFAULT_IN_PROGRESS_DIRECTORY_PROCESSING_PROBABILITY);
    }

    @Override
    public double getWaitingBufferPercentage() {
        return getConf().getDouble(REPLICATION_REPLAY_WAITING_BUFFER_PERCENTAGE_KEY, DEFAULT_WAITING_BUFFER_PERCENTAGE);
    }
}
