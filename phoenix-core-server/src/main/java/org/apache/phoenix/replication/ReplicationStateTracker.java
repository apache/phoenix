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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * This class tracks the last successfully processed replication round for single HA Group
 */
public class ReplicationStateTracker {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationStateTracker.class);

    private ReplicationRound lastSuccessfullyProcessedReplicationRound;

    public void init(ReplicationLogFileTracker replicationLogFileTracker) throws IOException {
        initLastSuccessfullyProcessedRound(replicationLogFileTracker);
    }

    protected void initLastSuccessfullyProcessedRound(final ReplicationLogFileTracker replicationLogFileTracker) throws IOException {
        // First check in-progress directory
        List<Path> inProgressFiles = replicationLogFileTracker.getInProgressFiles();
        if (!inProgressFiles.isEmpty()) {
            long minTimestamp = getMinTimestampFromFiles(replicationLogFileTracker, inProgressFiles);
            this.lastSuccessfullyProcessedReplicationRound = replicationLogFileTracker.getReplicationShardDirectoryManager().getReplicationRoundFromEndTime(minTimestamp);
            return;
        }

        // If no in-progress files, check IN directory
        // Get files from all shard directories in the IN directory
        List<Path> newFiles = replicationLogFileTracker.getNewFiles();
        if (!newFiles.isEmpty()) {
            long minTimestamp = getMinTimestampFromFiles(replicationLogFileTracker, newFiles);
            this.lastSuccessfullyProcessedReplicationRound = replicationLogFileTracker.getReplicationShardDirectoryManager().getReplicationRoundFromEndTime(minTimestamp);
            return;
        }

        // If no files found, set it to current time
        this.lastSuccessfullyProcessedReplicationRound = replicationLogFileTracker.getReplicationShardDirectoryManager().getReplicationRoundFromEndTime(EnvironmentEdgeManager.currentTime());

        LOG.info("Initialized lastSuccessfullyProcessedReplicationRound as {}", lastSuccessfullyProcessedReplicationRound);
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
