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

import static org.apache.phoenix.replication.ReplicationLogGroup.ReplicationMode.STORE_AND_FORWARD;
import static org.apache.phoenix.replication.ReplicationLogGroup.ReplicationMode.SYNC;
import static org.apache.phoenix.replication.ReplicationLogGroup.ReplicationMode.SYNC_AND_FORWARD;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.phoenix.jdbc.ClusterType;
import org.apache.phoenix.jdbc.HAGroupStateListener;
import org.apache.phoenix.jdbc.HAGroupStoreManager;
import org.apache.phoenix.jdbc.HAGroupStoreRecord;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogDiscovery;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogForwarderSourceFactory;
import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ReplicationLogDiscoveryForwarder manages the forwarding of the replication log
 * from the fallback cluster to the remote cluster.
 */
public class ReplicationLogDiscoveryForwarder extends ReplicationLogDiscovery {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicationLogDiscoveryForwarder.class);

    private final ReplicationLogGroup logGroup;

    /**
     * Create a tracker for the replication logs in the fallback cluster.
     *
     * @param logGroup HAGroup
     * @return ReplicationLogTracker
     */
    private static ReplicationLogTracker createLogTracker(ReplicationLogGroup logGroup) {
        ReplicationShardDirectoryManager localShardManager = logGroup.getFallbackShardManager();
        return new ReplicationLogTracker(
                logGroup.conf,
                logGroup.getHAGroupName(),
                localShardManager,
                MetricsReplicationLogForwarderSourceFactory.
                        getInstanceForTracker(logGroup.getHAGroupName()));
    }

    public ReplicationLogDiscoveryForwarder(ReplicationLogGroup logGroup) {
        super(createLogTracker(logGroup));
        this.logGroup = logGroup;
    }

    @Override
    public String getExecutorThreadNameFormat() {
        return "ReplicationLogDiscoveryForwarder-" + logGroup.getHAGroupName() + "-%d";
    }

    public void init() throws IOException {
        replicationLogTracker.init();
        // Initialize the discovery only. Forwarding begins only when we switch to the
        // STORE_AND_FORWARD mode or SYNC_AND_FORWARD mode.
        super.init();

        // Set up a listener to the ACTIVE_NOT_IN_SYNC state. This is needed because whenever any
        // RS switches to STORE_AND_FORWARD mode, other RS's in the cluster must move out of SYNC
        // mode.
        HAGroupStateListener activeNotInSync = (groupName,
                                                fromState,
                                                toState,
                                                modifiedTime,
                                                clusterType,
                                                lastSyncStateTimeInMs) -> {
            if (clusterType == ClusterType.LOCAL && HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC.equals(toState)) {
                LOG.info("Received ACTIVE_NOT_IN_SYNC event for {}", logGroup);
                // If the current mode is SYNC only then switch to SYNC_AND_FORWARD mode
                if (logGroup.checkAndSetMode(SYNC, SYNC_AND_FORWARD)) {
                    // replication mode switched, notify the event handler
                    try {
                        logGroup.sync();
                    } catch (IOException e) {
                        LOG.info("Failed to send sync event to {}", logGroup);
                    }
                }
            }
        };

        // Set up a listener to the ACTIVE_IN_SYNC state. This is needed because when the RS
        // switches back to SYNC mode, the other RS's in the cluster must move out of
        // SYNC_AND_FORWARD mode to SYNC mode.
        HAGroupStateListener activeInSync = (groupName,
                                                fromState,
                                                toState,
                                                modifiedTime,
                                                clusterType,
                                                lastSyncStateTimeInMs) -> {
            if (clusterType == ClusterType.LOCAL && HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC.equals(toState)) {
                LOG.info("Received ACTIVE_IN_SYNC event for {}", logGroup);
                // Set the current mode to SYNC
                if (logGroup.checkAndSetMode(SYNC_AND_FORWARD, SYNC)) {
                    // replication mode switched, notify the event handler
                    try {
                        logGroup.sync();
                    } catch (IOException e) {
                        LOG.info("Failed to send sync event to {}", logGroup);
                    }
                }
            }
        };

        HAGroupStoreManager haGroupStoreManager = logGroup.getHAGroupStoreManager();
        haGroupStoreManager.subscribeToTargetState(logGroup.getHAGroupName(),
                HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC, ClusterType.LOCAL, activeNotInSync);
        haGroupStoreManager.subscribeToTargetState(logGroup.getHAGroupName(),
                HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC, ClusterType.LOCAL, activeInSync);
    }

    @Override
    protected void processFile(Path src) throws IOException {
        FileSystem srcFS = replicationLogTracker.getFileSystem();
        FileStatus srcStat = srcFS.getFileStatus(src);
        long ts = EnvironmentEdgeManager.currentTimeMillis();
        ReplicationShardDirectoryManager remoteShardManager = logGroup.getStandbyShardManager();
        Path dst = remoteShardManager.getWriterPath(ts, logGroup.getServerName().getServerName());
        long startTime = EnvironmentEdgeManager.currentTimeMillis();
        FileUtil.copy(srcFS, srcStat, remoteShardManager.getFileSystem(), dst, false, false, conf);
        // successfully copied the file
        long endTime = EnvironmentEdgeManager.currentTimeMillis();
        long copyTime = endTime - startTime;
        LOG.info("Copying file src={} dst={} size={} took {}ms", src, dst, srcStat.getLen(), copyTime);
        if (logGroup.getMode() == STORE_AND_FORWARD &&
                isLogCopyThroughputAboveThreshold(srcStat.getLen(), copyTime)) {
            // start recovery by switching to SYNC_AND_FORWARD
            if (logGroup.checkAndSetMode(STORE_AND_FORWARD, SYNC_AND_FORWARD)) {
                // replication mode switched, notify the event handler
                try {
                    logGroup.sync();
                } catch (IOException e) {
                    LOG.info("Failed to send sync event to {}", logGroup);
                }
            }
        }
    }

    @Override
    protected void processNoMoreRoundsLeft() throws IOException {
        // check if we are caught up so that we can transition to SYNC state
        // we are caught up when there are no files currently in the out progress directory
        // and no new files exist for ongoing round
        if (replicationLogTracker.getInProgressFiles().isEmpty()
                && replicationLogTracker.getNewFilesForRound(replicationLogTracker
                .getReplicationShardDirectoryManager()
                .getNextRound(getLastRoundProcessed())).isEmpty()) {
            LOG.info("Processed all the replication log files for {}", logGroup);
            // TODO ensure the mTime on the group store record is older than the wait sync timeout
            logGroup.setHAGroupStatusToSync();
        }
    }

    /**
     * Determine if the throughput is above the configured threshold. If it is, then we can switch
     * to the SYNC_AND_FORWARD mode
     *
     * @param fileSize
     * @param copyTime
     * @return True if the throughput is good else false
     */
    private boolean isLogCopyThroughputAboveThreshold(long fileSize, long copyTime) {
        // TODO: calculate throughput and check if is above configured threshold
        return true;
    }

    @Override
    protected MetricsReplicationLogDiscovery createMetricsSource() {
        return MetricsReplicationLogForwarderSourceFactory.
                getInstanceForDiscovery(logGroup.getHAGroupName());
    }

    @VisibleForTesting
    protected ReplicationLogTracker getReplicationLogTracker() {
        return replicationLogTracker;
    }
}
