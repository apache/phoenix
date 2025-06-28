package org.apache.phoenix.replication.metrics;

/** Class to hold the values of all metrics tracked by the ReplicationLogProcessor metric source. */
public class ReplicationLogProcessorMetricValues {

    private final long failedMutationsCount;
    private final long logFileReplayFailureCount;
    private final long logFileReplaySuccessCount;
    private final long logFileReplayTime;
    private final long logFileBatchReplayTime;

    public ReplicationLogProcessorMetricValues(long failedMutationsCount, long logFileReplayFailureCount, long logFileReplaySuccessCount, long logFileReplayTime, long logFileBatchReplayTime) {
        this.failedMutationsCount = failedMutationsCount;
        this.logFileReplayFailureCount = logFileReplayFailureCount;
        this.logFileReplaySuccessCount = logFileReplaySuccessCount;
        this.logFileReplayTime = logFileReplayTime;
        this.logFileBatchReplayTime = logFileBatchReplayTime;
    }

    public long getFailedMutationsCount() {
        return failedMutationsCount;
    }

    public long getLogFileReplayFailureCount() {
        return logFileReplayFailureCount;
    }

    public long getLogFileReplaySuccessCount() {
        return logFileReplaySuccessCount;
    }

    public long getLogFileReplayTime() {
        return logFileReplayTime;
    }

    public long getLogFileBatchReplayTime() {
        return logFileBatchReplayTime;
    }
}
