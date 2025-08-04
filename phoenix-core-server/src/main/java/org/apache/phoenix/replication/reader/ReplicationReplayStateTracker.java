package org.apache.phoenix.replication.reader;

import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.phoenix.replication.ReplicationLogFileTracker;
import org.apache.phoenix.replication.ReplicationStateTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

public class ReplicationReplayStateTracker extends ReplicationStateTracker {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationReplayStateTracker.class);

    public void init(final ReplicationLogFileTracker replicationLogFileTracker) throws IOException {

        // TODO: Update this to check from HA Store Client
        boolean isDegradedStandByForWriter = false;

        if(isDegradedStandByForWriter) {
            // TODO: Set the last successfully processed replication round to the last store-and-forward starting timestamp is obtained from the HA store record cached by HA Store Manager
        } else {
            Optional<Long> minTimestampFromInProgressFiles = getMinTimestampFromInProgressFiles(replicationLogFileTracker);
            if(minTimestampFromInProgressFiles.isPresent()) {
                this.lastRoundInSync = replicationLogFileTracker.getReplicationShardDirectoryManager().getReplicationRoundFromEndTime(minTimestampFromInProgressFiles.get());
            } else {
                Optional<Long> minTimestampFromNewFiles = getMinTimestampFromNewFiles(replicationLogFileTracker);
                if(minTimestampFromNewFiles.isPresent()) {
                    this.lastRoundInSync = replicationLogFileTracker.getReplicationShardDirectoryManager().getReplicationRoundFromEndTime(minTimestampFromNewFiles.get());
                } else {
                    this.lastRoundInSync = replicationLogFileTracker.getReplicationShardDirectoryManager().getReplicationRoundFromEndTime(EnvironmentEdgeManager.currentTime());
                }
            }
        }
        LOG.info("Initialized last round in sync as {}", lastRoundInSync);
    }

}
