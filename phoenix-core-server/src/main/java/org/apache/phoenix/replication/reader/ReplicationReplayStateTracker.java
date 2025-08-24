/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.replication.reader;

import java.io.IOException;
import java.util.Optional;

import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.phoenix.replication.ReplicationLogFileTracker;
import org.apache.phoenix.replication.ReplicationStateTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationReplayStateTracker extends ReplicationStateTracker {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationReplayStateTracker.class);

    public void init(final ReplicationLogFileTracker replicationLogFileTracker) throws IOException {

        // TODO: Update this to check from HA Store Client
        boolean isDegradedStandByForWriter = false;

        if (isDegradedStandByForWriter) {
            // TODO: Set the last successfully processed replication round to the last 
            // store-and-forward 
            // starting timestamp is obtained from the HA store record cached by HA Store Manager
        } else {
            Optional<Long> minTimestampFromInProgressFiles = 
                getMinTimestampFromInProgressFiles(replicationLogFileTracker);
            if (minTimestampFromInProgressFiles.isPresent()) {
                this.lastRoundInSync = replicationLogFileTracker.getReplicationShardDirectoryManager()
                    .getReplicationRoundFromEndTime(minTimestampFromInProgressFiles.get());
            } else {
                Optional<Long> minTimestampFromNewFiles = 
                getMinTimestampFromNewFiles(replicationLogFileTracker);
                if (minTimestampFromNewFiles.isPresent()) {
                    this.lastRoundInSync = replicationLogFileTracker.getReplicationShardDirectoryManager()
                    .getReplicationRoundFromEndTime(minTimestampFromNewFiles.get());
                } else {
                    this.lastRoundInSync = replicationLogFileTracker.getReplicationShardDirectoryManager()
                        .getReplicationRoundFromEndTime(EnvironmentEdgeManager.currentTime());
                }
            }
        }
        LOG.info("Initialized last round in sync as {}", lastRoundInSync);
    }

}
