package org.apache.phoenix.replication.metrics;

/** Class to hold the values of all metrics tracked by the ReplicationLogFileTracker metrics source. */
public class ReplicationLogFileTrackerMetricValues {

    private final long markFileInProgressRequestCount;
    private final long markFileCompletedRequestCount;
    private final long markFileFailedRequestCount;
    private final long markFileCompletedRequestFailedCount;
    private final long markFileInProgressTimeMs;
    private final long markFileCompletedTimeMs;
    private final long markFileFailedTimeMs;

    public ReplicationLogFileTrackerMetricValues(long markFileInProgressRequestCount, long markFileCompletedRequestCount,
        long markFileFailedRequestCount, long markFileCompletedRequestFailedCount,
        long markFileInProgressTimeMs, long markFileCompletedTimeMs, long markFileFailedTimeMs) {
        this.markFileInProgressRequestCount = markFileInProgressRequestCount;
        this.markFileCompletedRequestCount = markFileCompletedRequestCount;
        this.markFileFailedRequestCount = markFileFailedRequestCount;
        this.markFileCompletedRequestFailedCount = markFileCompletedRequestFailedCount;
        this.markFileInProgressTimeMs = markFileInProgressTimeMs;
        this.markFileCompletedTimeMs = markFileCompletedTimeMs;
        this.markFileFailedTimeMs = markFileFailedTimeMs;
    }

    public long getMarkFileInProgressRequestCount() {
        return markFileInProgressRequestCount;
    }

    public long getMarkFileCompletedRequestCount() {
        return markFileCompletedRequestCount;
    }

    public long getMarkFileFailedRequestCount() {
        return markFileFailedRequestCount;
    }

    public long getMarkFileCompletedRequestFailedCount() {
        return markFileCompletedRequestFailedCount;
    }

    public long getMarkFileInProgressTimeMs() {
        return markFileInProgressTimeMs;
    }

    public long getMarkFileCompletedTimeMs() {
        return markFileCompletedTimeMs;
    }

    public long getMarkFileFailedTimeMs() {
        return markFileFailedTimeMs;
    }

}
