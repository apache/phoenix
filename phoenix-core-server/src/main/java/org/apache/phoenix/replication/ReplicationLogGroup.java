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
package org.apache.phoenix.replication;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogGroupSource;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogGroupSourceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ReplicationLogGroup manages a group of replication logs for a given HA Group.
 * <p>
 * This class provides an API for replication operations and delegates to either synchronous
 * replication (StandbyLogGroupWriter) or store-and-forward replication
 * (StoreAndForwardLogGroupWriter) based on the current replication mode.
 * <p>
 * Key features:
 * <ul>
 *   <li>Manages multiple replication logs for an HA Group</li>
 *   <li>Provides append() and sync() API for higher layers</li>
 *   <li>Delegates to appropriate writer implementation based on replication mode</li>
 *   <li>Thread-safe operations</li>
 * </ul>
 * <p>
 * The class delegates actual replication work to implementations of ReplicationLogGroupWriter:
 * <ul>
 *   <li>StandbyLogGroupWriter: Synchronous replication to standby cluster</li>
 *   <li>StoreAndForwardLogGroupWriter: Local storage with forwarding when available</li>
 * </ul>
 */
public class ReplicationLogGroup {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationLogGroup.class);

    // Configuration constants from original ReplicationLog
    public static final String REPLICATION_STANDBY_HDFS_URL_KEY =
        "phoenix.replication.log.standby.hdfs.url";
    public static final String REPLICATION_FALLBACK_HDFS_URL_KEY =
        "phoenix.replication.log.fallback.hdfs.url";
    public static final String REPLICATION_NUM_SHARDS_KEY = "phoenix.replication.log.shards";
    public static final int DEFAULT_REPLICATION_NUM_SHARDS = 1000;
    public static final int MAX_REPLICATION_NUM_SHARDS = 100000;
    public static final String REPLICATION_LOG_ROTATION_SIZE_BYTES_KEY =
        "phoenix.replication.log.rotation.size.bytes";
    public static final long DEFAULT_REPLICATION_LOG_ROTATION_SIZE_BYTES = 256 * 1024 * 1024L;
    public static final String REPLICATION_LOG_ROTATION_SIZE_PERCENTAGE_KEY =
        "phoenix.replication.log.rotation.size.percentage";
    public static final double DEFAULT_REPLICATION_LOG_ROTATION_SIZE_PERCENTAGE = 0.95;
    public static final String REPLICATION_LOG_COMPRESSION_ALGORITHM_KEY =
        "phoenix.replication.log.compression";
    public static final String DEFAULT_REPLICATION_LOG_COMPRESSION_ALGORITHM = "NONE";
    public static final String REPLICATION_LOG_RINGBUFFER_SIZE_KEY =
        "phoenix.replication.log.ringbuffer.size";
    public static final int DEFAULT_REPLICATION_LOG_RINGBUFFER_SIZE = 1024 * 32;
    public static final String REPLICATION_LOG_SYNC_TIMEOUT_KEY =
        "phoenix.replication.log.sync.timeout.ms";
    public static final long DEFAULT_REPLICATION_LOG_SYNC_TIMEOUT = 1000 * 30;
    public static final String REPLICATION_LOG_SYNC_RETRIES_KEY =
        "phoenix.replication.log.sync.retries";
    public static final int DEFAULT_REPLICATION_LOG_SYNC_RETRIES = 5;
    public static final String REPLICATION_LOG_ROTATION_RETRIES_KEY =
        "phoenix.replication.log.rotation.retries";
    public static final int DEFAULT_REPLICATION_LOG_ROTATION_RETRIES = 5;
    public static final String REPLICATION_LOG_RETRY_DELAY_MS_KEY =
        "phoenix.replication.log.retry.delay.ms";
    public static final long DEFAULT_REPLICATION_LOG_RETRY_DELAY_MS = 100L;

    public static final String SHARD_DIR_FORMAT = "%05d";
    public static final String FILE_NAME_FORMAT = "%d-%s.plog";

    /** Cache of ReplicationLogGroup instances by HA Group ID */
    protected static final ConcurrentHashMap<String, ReplicationLogGroup> INSTANCES =
        new ConcurrentHashMap<>();

    protected final Configuration conf;
    protected final ServerName serverName;
    protected final String haGroupName;
    protected ReplicationLogGroupWriter remoteWriter;
    protected ReplicationLogGroupWriter localWriter;
    protected ReplicationMode mode;
    protected volatile boolean closed = false;
    protected final MetricsReplicationLogGroupSource metrics;

    /**
     * Tracks the current replication mode of the ReplicationLog.
     * <p>
     * The replication mode determines how mutations are handled:
     * <ul>
     *   <li>SYNC: Normal operation where mutations are written directly to the standby cluster's
     *   HDFS.
     *   This is the default and primary mode of operation.</li>
     *   <li>STORE_AND_FORWARD: Fallback mode when the standby cluster's HDFS is unavailable.
     *   Mutations are stored locally and will be forwarded when connectivity is restored.</li>
     *   <li>SYNC_AND_FORWARD: Transitional mode where new mutations are written directly to the
     *   standby cluster while concurrently draining the local queue of previously stored
     *   mutations.</li>
     * </ul>
     * <p>
     * Mode transitions occur automatically based on the availability of the standby cluster's HDFS
     * and the state of the local mutation queue.
     */
    protected enum ReplicationMode {
        /**
         * Normal operation where mutations are written directly to the standby cluster's HDFS.
         * This is the default and primary mode of operation.
         */
        SYNC,

        /**
         * Fallback mode when the standby cluster's HDFS is unavailable. Mutations are stored
         * locally and will be forwarded when connectivity is restored.
         */
        STORE_AND_FORWARD,

        /**
         * Transitional mode where new mutations are written directly to the standby cluster
         * while concurrently draining the local queue of previously stored mutations. This mode
         * is entered when connectivity to the standby cluster is restored and there are still
         * mutations in the local queue.
         */
        SYNC_AND_FORWARD;
    }

    /**
     * Get or create a ReplicationLogGroup instance for the given HA Group.
     *
     * @param conf Configuration object
     * @param serverName The server name
     * @param haGroupName The HA Group name
     * @return ReplicationLogGroup instance
     * @throws RuntimeException if initialization fails
     */
    public static ReplicationLogGroup get(Configuration conf, ServerName serverName,
            String haGroupName) {
        return INSTANCES.computeIfAbsent(haGroupName, k -> {
            try {
                ReplicationLogGroup group = new ReplicationLogGroup(conf, serverName, haGroupName);
                group.init();
                return group;
            } catch (IOException e) {
                LOG.error("Failed to create ReplicationLogGroup for HA Group: {}", haGroupName, e);
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Protected constructor for ReplicationLogGroup.
     *
     * @param conf Configuration object
     * @param serverName The server name
     * @param haGroupName The HA Group name
     */
    protected ReplicationLogGroup(Configuration conf, ServerName serverName, String haGroupName) {
        this.conf = conf;
        this.serverName = serverName;
        this.haGroupName = haGroupName;
        this.metrics = createMetricsSource();
    }

    /**
     * Initialize the ReplicationLogGroup by creating the appropriate writer implementation.
     *
     * @throws IOException if initialization fails
     */
    protected void init() throws IOException {
        // We need the local writer created first if we intend to fall back to it should the init
        // of the remote writer fail.
        localWriter = createLocalWriter();
        // Initialize the remote writer and set the mode to SYNC. TODO: switch instead of set
        mode = ReplicationMode.SYNC;
        remoteWriter = createRemoteWriter();
        // TODO: Switch the initial mode to STORE_AND_FORWARD if the remote writer fails to
        // initialize.
        LOG.info("Started ReplicationLogGroup for HA Group: {}", haGroupName);
    }

    /**
     * Get the name for this HA Group.
     *
     * @return The name for this HA Group
     */
    public String getHaGroupName() {
        return haGroupName;
    }

    protected Configuration getConfiguration() {
        return conf;
    }

    protected ServerName getServerName() {
        return serverName;
    }

    /**
     * Append a mutation to the replication log group. This operation is normally non-blocking
     * unless the ring buffer is full.
     *
     * @param tableName The name of the HBase table the mutation applies to
     * @param commitId The commit identifier (e.g., SCN) associated with the mutation
     * @param mutation The HBase Mutation (Put or Delete) to be logged
     * @throws IOException If the operation fails
     */
    public void append(String tableName, long commitId, Mutation mutation) throws IOException {
        if (closed) {
            throw new IOException("Closed");
        }
        long startTime = System.nanoTime();
        try {
            switch (mode) {
            case SYNC:
                // In sync mode, we only write to the remote writer.
                try {
                    remoteWriter.append(tableName, commitId, mutation);
                } catch (IOException e) {
                    // TODO: If the remote writer fails, we must switch to store and forward.
                    LOG.warn("Mode switching not implemented");
                    throw e;
                }
                break;
            case SYNC_AND_FORWARD:
                // In sync and forward mode, we write to only the remote writer, while in the
                // background we are draining the local queue.
                try {
                    remoteWriter.append(tableName, commitId, mutation);
                } catch (IOException e) {
                    // TODO: If the remote writer fails again, we must switch back to store and
                    // forward.
                    LOG.warn("Mode switching not implemented");
                    throw e;
                }
                break;
            case STORE_AND_FORWARD:
                // In store and forward mode, we append to the local writer. If we fail it's a
                // critical failure.
                localWriter.append(tableName, commitId, mutation);
                // TODO: Probe the state of the remoteWriter. Can we switch back?
                // TODO: This suggests the ReplicationLogGroupWriter interface should have a status
                // probe API.
                break;
            default:
                throw new IllegalStateException("Invalid replication mode: " + mode);
            }
        } finally {
            metrics.updateAppendTime(System.nanoTime() - startTime);
        }
    }

    /**
     * Ensure all previously appended records are durably persisted. This method blocks until the
     * sync operation completes or fails.
     *
     * @throws IOException If the sync operation fails
     */
    public void sync() throws IOException {
        if (closed) {
            throw new IOException("Closed");
        }
        long startTime = System.nanoTime();
        try {
            switch (mode) {
            case SYNC:
                // In sync mode, we only write to the remote writer.
                try {
                    remoteWriter.sync();
                } catch (IOException e) {
                    // TODO: If the remote writer fails, we must switch to store and forward.
                    LOG.warn("Mode switching not implemented");
                    throw e;
                }
                break;
            case SYNC_AND_FORWARD:
                // In sync and forward mode, we write to only the remote writer, while in the
                // background we are draining the local queue.
                try {
                    remoteWriter.sync();
                } catch (IOException e) {
                    // TODO: If the remote writer fails again, we must switch back to store and
                    // forward.
                    LOG.warn("Mode switching not implemented");
                    throw e;
                }
                break;
            case STORE_AND_FORWARD:
                // In store and forward mode, we sync the local writer. If we fail it's a critical
                // failure.
                localWriter.sync();
                // TODO: Probe the state of the remoteWriter. Can we switch back?
                // TODO: This suggests the ReplicationLogGroupWriter interface should have a
                // status probe API.
                break;
            default:
                throw new IllegalStateException("Invalid replication mode: " + mode);
            }
        } finally {
            metrics.updateSyncTime(System.nanoTime() - startTime);
        }
    }

    /**
     * Check if this ReplicationLogGroup is closed.
     *
     * @return true if closed, false otherwise
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Close the ReplicationLogGroup and all associated resources. This method is thread-safe and
     * can be called multiple times.
     */
    public void close() {
        if (closed) {
            return;
        }
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            // Remove from instances cache
            INSTANCES.remove(haGroupName);
            // Close the writers, remote first. If there are any problems closing the remote writer
            // the pending writes will be sent to the local writer instead, during the appropriate
            // mode switch.
            closeWriter(remoteWriter);
            closeWriter(localWriter);
            metrics.close();
            LOG.info("Closed ReplicationLogGroup for HA Group: {}", haGroupName);
        }
    }

    /**
     * Switch the replication mode.
     *
     * @param mode The new replication mode
     * @param reason The reason for the mode switch
     * @throws IOException If the mode switch fails
     */
    public void switchMode(ReplicationMode mode, Throwable reason) throws IOException {
        // TODO: Implement mode switching guardrails and transition logic.
        // TODO: We will be interacting with the HA Group Store to switch modes.

        // TODO: Drain the disruptor ring from the remote writer to the local writer when making
        // transitions from SYNC or SYNC_AND_FORWARD to STORE_AND_FORWARD.

        throw new UnsupportedOperationException("Mode switching is not implemented");
    }

    /** Get the current metrics source for monitoring operations. */
    public MetricsReplicationLogGroupSource getMetrics() {
        return metrics;
    }

    /** Create a new metrics source for monitoring operations. */
    protected MetricsReplicationLogGroupSource createMetricsSource() {
        return new MetricsReplicationLogGroupSourceImpl(haGroupName);
    }

    /** Close the given writer. */
    protected void closeWriter(ReplicationLogGroupWriter writer) {
        if (writer != null) {
            writer.close();
        }
    }

    /** Create the remote (synchronous) writer. Mainly for tests. */
    protected ReplicationLogGroupWriter createRemoteWriter() throws IOException {
        ReplicationLogGroupWriter writer = new StandbyLogGroupWriter(this);
        writer.init();
        return writer;
    }

    /** Create the local (store and forward) writer. Mainly for tests. */
    protected ReplicationLogGroupWriter createLocalWriter() throws IOException {
        ReplicationLogGroupWriter writer = new StoreAndForwardLogGroupWriter(this);
        writer.init();
        return writer;
    }

    /** Returns the currently active writer. Mainly for tests. */
    protected ReplicationLogGroupWriter getActiveWriter() {
        switch (mode) {
        case SYNC:
            return remoteWriter;
        case SYNC_AND_FORWARD:
            return remoteWriter;
        case STORE_AND_FORWARD:
            return localWriter;
        default:
            throw new IllegalStateException("Invalid replication mode: " + mode);
        }
    }
}
