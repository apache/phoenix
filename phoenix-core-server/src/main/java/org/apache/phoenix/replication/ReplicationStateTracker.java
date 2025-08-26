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
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * This class tracks the last round in sync for replication.
 */
public abstract class ReplicationStateTracker {

    protected ReplicationRound lastRoundInSync;

    public abstract void init(ReplicationLogFileTracker replicationLogFileTracker) 
        throws IOException;

    protected Optional<Long> getMinTimestampFromInProgressFiles(
        final ReplicationLogFileTracker replicationLogFileTracker) throws IOException {
        List<Path> inProgressFiles = replicationLogFileTracker.getInProgressFiles();
        if(inProgressFiles.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(getMinTimestampFromFiles(replicationLogFileTracker, inProgressFiles));
    }

    protected Optional<Long> getMinTimestampFromNewFiles(
        final ReplicationLogFileTracker replicationLogFileTracker) throws IOException {
        List<Path> newFiles = replicationLogFileTracker.getNewFiles();
        if(newFiles.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(getMinTimestampFromFiles(replicationLogFileTracker, newFiles));
    }

    private long getMinTimestampFromFiles(ReplicationLogFileTracker replicationLogFileTracker, 
        List<Path> files) {
        long minTimestamp = EnvironmentEdgeManager.currentTime();
        for (Path file : files) {
            minTimestamp = Math.min(minTimestamp, replicationLogFileTracker.getFileTimestamp(file));
        }
        return minTimestamp;
    }

    public ReplicationRound getLastRoundInSync() {
        return lastRoundInSync;
    }

    public void setLastRoundInSync(final ReplicationRound replicationRound) {
        this.lastRoundInSync = replicationRound;
    }
}
