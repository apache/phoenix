package org.apache.phoenix.replication.metrics;

import org.apache.hadoop.hbase.metrics.BaseSource;

/** Interface for metrics related to ReplicationLogProcessor operations. */
public interface MetricsReplicationLogProcessor extends BaseSource {

    String METRICS_NAME = "ReplicationLogProcessor";
    String METRICS_CONTEXT = "phoenix";
    String METRICS_DESCRIPTION = "Metrics about Replication Log Processor for an HA Group";
    String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;

    String FAILED_MUTATIONS_COUNT = "failedMutationsCount";
    String FAILED_MUTATIONS_COUNT_DESC = "Number of failed mutations";
    String FAILED_BATCH_COUNT = "failedBatchCount";
    String FAILED_BATCH_COUNT_DESC = "Number of failed batches";
    String LOG_FILE_REPLAY_FAILURE_COUNT = "logFileReplayFailureCount";
    String LOG_FILE_REPLAY_FAILURE_COUNT_DESC = "Number of files failed to replay";
    String LOG_FILE_REPLAY_SUCCESS_COUNT = "logFileReplaySuccessCount";
    String LOG_FILE_REPLAY_SUCCESS_COUNT_DESC = "Number of files successfully to replayed";
    String BATCH_REPLAY_TIME = "batchReplayTimeMs";
    String BATCH_REPLAY_TIME_DESC = "Histogram of time taken for replaying a batch of log file in milliseconds";
    String LOG_FILE_REPLAY_TIME = "logFileReplayTimeMs";
    String LOG_FILE_REPLAY_TIME_DESC = "Histogram of time taken for replaying a log file in milliseconds";

    /**
     * Increments the counter for failed mutations.
     * This counter tracks the number of mutations that failed during processing.
     */
    void incrementFailedMutationsCount(long delta);

    /**
     * Increments the counter for log file replay failures.
     * This counter tracks the number of log files that failed to replay.
     */
    void incrementFailedBatchCount();

    /**
     * Increments the counter for log file replay failures.
     * This counter tracks the number of log files that failed to replay.
     */
    void incrementLogFileReplayFailureCount();

    /**
     * Increments the counter for log file replay successes.
     * This counter tracks the number of log files that were successfully replayed.
     */
    void incrementLogFileReplaySuccessCount();

    /**
     * Update the time taken for replaying a batch of mutations in milliseconds.
     * @param timeMs Time taken in milliseconds
     */
    void updateBatchReplayTime(long timeMs);

    /**
     * Update the time taken for replaying a log file in milliseconds.
     * @param timeMs Time taken in milliseconds
     */
    void updateLogFileReplayTime(long timeMs);

    /**
     * Unregister this metrics source.
     */
    void close();

    // Get current values for testing
    ReplicationLogProcessorMetricValues getCurrentMetricValues();
}
