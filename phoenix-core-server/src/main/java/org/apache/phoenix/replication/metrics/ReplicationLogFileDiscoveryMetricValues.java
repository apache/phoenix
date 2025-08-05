package org.apache.phoenix.replication.metrics;

/** Class to hold the values of all metrics tracked by the ReplicationLogDiscovery metrics source. */
public class ReplicationLogFileDiscoveryMetricValues {

    private final long numRoundsProcessed;
    private final long numInProgressDirectoryProcessed;
    private final long timeToProcessNewFilesMs;
    private final long timeToProcessInProgressFilesMs;

    public ReplicationLogFileDiscoveryMetricValues(long numRoundsProcessed, long numInProgressDirectoryProcessed,
                                                   long timeToProcessNewFilesMs, long timeToProcessInProgressFilesMs) {
        this.numRoundsProcessed = numRoundsProcessed;
        this.numInProgressDirectoryProcessed = numInProgressDirectoryProcessed;
        this.timeToProcessNewFilesMs = timeToProcessNewFilesMs;
        this.timeToProcessInProgressFilesMs = timeToProcessInProgressFilesMs;
    }

    public long getNumRoundsProcessed() {
        return numRoundsProcessed;
    }

    public long getNumInProgressDirectoryProcessed() {
        return numInProgressDirectoryProcessed;
    }

    public long getTimeToProcessNewFilesMs() {
        return timeToProcessNewFilesMs;
    }

    public long getTimeToProcessInProgressFilesMs() {
        return timeToProcessInProgressFilesMs;
    }

}
