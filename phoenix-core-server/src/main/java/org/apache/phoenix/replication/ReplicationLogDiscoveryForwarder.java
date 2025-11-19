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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.phoenix.jdbc.ClusterType;
import org.apache.phoenix.jdbc.HAGroupStateListener;
import org.apache.phoenix.jdbc.HAGroupStoreManager;
import org.apache.phoenix.jdbc.HAGroupStoreRecord;
import org.apache.phoenix.replication.ReplicationLogGroup.ReplicationMode;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogDiscovery;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogForwarderSourceFactory;
import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.phoenix.replication.ReplicationLogGroup.ReplicationMode.STORE_AND_FORWARD;
import static org.apache.phoenix.replication.ReplicationLogGroup.ReplicationMode.SYNC;
import static org.apache.phoenix.replication.ReplicationLogGroup.ReplicationMode.SYNC_AND_FORWARD;

/**
 * ReplicationLogDiscoveryForwarder manages the forwarding of the replication log
 * from the fallback cluster to the remote cluster.
 */
public class ReplicationLogDiscoveryForwarder extends ReplicationLogDiscovery {
    private static final Logger LOG =
            LoggerFactory.getLogger(ReplicationLogDiscoveryForwarder.class);

    public static final String REPLICATION_LOG_COPY_THROUGHPUT_BYTES_PER_MS_KEY =
            "phoenix.replication.log.copy.throughput.bytes.per.ms";
    // TODO: come up with a better default after testing
    public static final double DEFAULT_LOG_COPY_THROUGHPUT_BYTES_PER_MS = 1.0;

    private final ReplicationLogGroup logGroup;
    private final double copyThroughputThresholdBytesPerMs;
    // the timestamp (in future) at which we will attempt to set the HAGroup state to SYNC
    private long syncUpdateTS;

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
        this.copyThroughputThresholdBytesPerMs =
                conf.getDouble(REPLICATION_LOG_COPY_THROUGHPUT_BYTES_PER_MS_KEY,
                DEFAULT_LOG_COPY_THROUGHPUT_BYTES_PER_MS);
        // initialize to 0
        this.syncUpdateTS = 0;
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
            if (clusterType == ClusterType.LOCAL
                    && HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC.equals(toState)) {
                LOG.info("Received ACTIVE_NOT_IN_SYNC event for {}", logGroup);
                // If the current mode is SYNC only then switch to SYNC_AND_FORWARD mode
                checkAndSetModeAndNotify(SYNC, SYNC_AND_FORWARD);
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
            if (clusterType == ClusterType.LOCAL
                    && HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC.equals(toState)) {
                LOG.info("Received ACTIVE_IN_SYNC event for {}", logGroup);
                // Set the current mode to SYNC
                checkAndSetModeAndNotify(SYNC_AND_FORWARD, SYNC);
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
        long ts = replicationLogTracker.getFileTimestamp(srcStat.getPath());
        ReplicationShardDirectoryManager remoteShardManager = logGroup.getStandbyShardManager();
        Path dst = remoteShardManager.getWriterPath(ts, logGroup.getServerName().getServerName());
        long startTime = EnvironmentEdgeManager.currentTimeMillis();
        FileUtil.copy(srcFS, srcStat, remoteShardManager.getFileSystem(), dst, false, false, conf);
        // successfully copied the file
        long endTime = EnvironmentEdgeManager.currentTimeMillis();
        long copyTime = endTime - startTime;
        LOG.info("Copying file src={} dst={} size={} took {}ms", src, dst, srcStat.getLen(), copyTime);
        if (logGroup.getMode() == STORE_AND_FORWARD
                && isLogCopyThroughputAboveThreshold(srcStat.getLen(), copyTime)) {
            // start recovery by switching to SYNC_AND_FORWARD
            checkAndSetModeAndNotify(STORE_AND_FORWARD, SYNC_AND_FORWARD);
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
            // if this RS is still in STORE_AND_FORWARD mode like when it didn't process any file
            // move this RS to SYNC_AND_FORWARD
            checkAndSetModeAndNotify(STORE_AND_FORWARD, SYNC_AND_FORWARD);

            if (syncUpdateTS <= EnvironmentEdgeManager.currentTimeMillis()) {
                try {
                    long waitTime = logGroup.setHAGroupStatusToSync();
                    if (waitTime != 0) {
                        syncUpdateTS = EnvironmentEdgeManager.currentTimeMillis() + waitTime;
                        LOG.info("HAGroup {} will try to update HA state to sync at {}",
                                logGroup, syncUpdateTS);
                    } else {
                        LOG.info("HAGroup {} updated HA state to SYNC", logGroup);
                    }
                } catch (Exception e) {
                    LOG.info("Could not update status to sync for {}", logGroup, e);
                }
            }
        }
    }

    /**
     * Determine if the throughput is above the configured threshold. If it is, then we can switch
     * to the SYNC_AND_FORWARD mode
     *
     * @param fileSize in bytes
     * @param copyTime in ms
     * @return True if the throughput is good else false
     */
    private boolean isLogCopyThroughputAboveThreshold(long fileSize, long copyTime) {
        double actualThroughputBytesPerMs = copyTime != 0 ? ((double) fileSize)/copyTime : 0;
        return actualThroughputBytesPerMs >= copyThroughputThresholdBytesPerMs;
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

    /**
     * Helper API to check and set the replication mode and then notify the disruptor
     */
    private boolean checkAndSetModeAndNotify(ReplicationMode expectedMode, ReplicationMode newMode) {
        boolean ret = logGroup.checkAndSetMode(expectedMode, newMode);
        if (ret) {
            // replication mode switched, notify the event handler
            try {
                logGroup.sync();
            } catch (IOException e) {
                LOG.info("Failed to notify event handler for {}", logGroup, e);
            }
        }
        return ret;
    }
}
