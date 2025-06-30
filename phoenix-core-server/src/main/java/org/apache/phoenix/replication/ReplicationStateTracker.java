package org.apache.phoenix.replication;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import java.io.IOException;
import java.util.List;

public class ReplicationStateTracker {

    private ReplicationRound lastSuccessfullyProcessedReplicationRound;

    public void init(ReplicationLogFileTracker replicationLogFileTracker) throws IOException {
        initLastSuccessfullyProcessedRound(replicationLogFileTracker);
    }

    protected void initLastSuccessfullyProcessedRound(final ReplicationLogFileTracker replicationLogFileTracker) throws IOException {
        // First check in-progress directory
        List<Path> inProgressFiles = replicationLogFileTracker.getInProgressFiles();
        if (!inProgressFiles.isEmpty()) {
            long minTimestamp = getMinTimestampFromFiles(replicationLogFileTracker, inProgressFiles);
            System.out.println("Found inprogress minTimestamp as " + minTimestamp);
            this.lastSuccessfullyProcessedReplicationRound = replicationLogFileTracker.getReplicationShardDirectoryManager().getReplicationRoundFromEndTime(minTimestamp);
            return;
        }

        // If no in-progress files, check IN directory
        // Get files from all shard directories in the IN directory
        List<Path> newFiles = replicationLogFileTracker.getNewFiles();
        if (!newFiles.isEmpty()) {
            long minTimestamp = getMinTimestampFromFiles(replicationLogFileTracker, newFiles);
            System.out.println("Found in minTimestamp as " + minTimestamp);
            this.lastSuccessfullyProcessedReplicationRound = replicationLogFileTracker.getReplicationShardDirectoryManager().getReplicationRoundFromEndTime(minTimestamp);
            return;
        }

        // If no files found, set it to current time
        this.lastSuccessfullyProcessedReplicationRound = replicationLogFileTracker.getReplicationShardDirectoryManager().getReplicationRoundFromEndTime(EnvironmentEdgeManager.currentTime());
    }

    private long getMinTimestampFromFiles(ReplicationLogFileTracker replicationLogFileTracker, List<Path> files) {
        long minTimestamp = EnvironmentEdgeManager.currentTime();
        for (Path file : files) {
            minTimestamp = Math.min(minTimestamp, replicationLogFileTracker.getFileTimestamp(file));
        }
        return minTimestamp;
    }

    public ReplicationRound getLastSuccessfullyProcessedRound() {
        return lastSuccessfullyProcessedReplicationRound;
    }
}
