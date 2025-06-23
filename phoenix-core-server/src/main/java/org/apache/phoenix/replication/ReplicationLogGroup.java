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
    public static final String REPLICATION_LOG_ROTATION_TIME_MS_KEY =
        "phoenix.replication.log.rotation.time.ms";
    public static final long DEFAULT_REPLICATION_LOG_ROTATION_TIME_MS = 60 * 1000L;
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
    private static final ConcurrentHashMap<String, ReplicationLogGroup> INSTANCES =
        new ConcurrentHashMap<>();

    private final Configuration conf;
    private final ServerName serverName;
    private final String haGroupId;
    private volatile ReplicationLogGroupWriter writer;
    private volatile boolean closed = false;

    /**
     * The current replication mode. Always SYNC for now.
     * <p>TODO: Implement mode transitions to STORE_AND_FORWARD when standby becomes unavailable.
     * <p>TODO: Implement mode transitions to SYNC_AND_FORWARD when draining queue.
     */
    protected volatile ReplicationMode currentMode = ReplicationMode.SYNC;

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
         * is entered when connectivity to the standby cluster is restored while there are still
         * mutations in the local queue.
         */
        SYNC_AND_FORWARD;
    }

    /**
     * Get or create a ReplicationLogGroup instance for the given HA Group.
     *
     * @param conf Configuration object
     * @param serverName The server name
     * @param haGroupId The HA Group identifier
     * @return ReplicationLogGroup instance
     * @throws RuntimeException if initialization fails
     */
    public static ReplicationLogGroup get(Configuration conf, ServerName serverName,
            String haGroupId) {
        return INSTANCES.computeIfAbsent(haGroupId, k -> {
            try {
                ReplicationLogGroup group = new ReplicationLogGroup(conf, serverName, haGroupId);
                group.init();
                return group;
            } catch (IOException e) {
                LOG.error("Failed to create ReplicationLogGroup for HA Group: {}", haGroupId, e);
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Protected constructor for ReplicationLogGroup.
     *
     * @param conf Configuration object
     * @param serverName The server name
     * @param haGroupId The HA Group identifier
     */
    protected ReplicationLogGroup(Configuration conf, ServerName serverName, String haGroupId) {
        this.conf = conf;
        this.serverName = serverName;
        this.haGroupId = haGroupId;
    }

    /**
     * Initialize the ReplicationLogGroup by creating the appropriate writer implementation.
     *
     * @throws IOException if initialization fails
     */
    protected void init() throws IOException {
        // Start with synchronous replication (StandbyLogGroupWriter). Later we can add logic to
        // determine the appropriate writer based on configuration or HA Group state.
        writer = new StandbyLogGroupWriter(conf, serverName, haGroupId);
        writer.init();
        LOG.info("Initialized ReplicationLogGroup for HA Group: {}", haGroupId);
    }

    /**
     * Get the current metrics source for monitoring operations.
     *
     * @return MetricsReplicationLogSource instance
     */
    public MetricsReplicationLogGroupSource getMetrics() {
        return writer != null ? writer.getMetrics() : null;
    }

    /**
     * Get the HA Group ID managed by this instance.
     *
     * @return HA Group ID
     */
    public String getHaGroupId() {
        return haGroupId;
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
        writer.append(tableName, commitId, mutation);
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
        writer.sync();
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
     * Close the ReplicationLogGroup and all associated resources.
     * This method is thread-safe and can be called multiple times.
     */
    public void close() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            closeWriter(writer);
            // Remove from instances cache
            INSTANCES.remove(haGroupId);
            LOG.info("Closed ReplicationLogGroup for HA Group: {}", haGroupId);
        }
    }

    /**
     * Close the given writer.
     *
     * @param writer The writer to close
     */
    protected void closeWriter(ReplicationLogGroupWriter writer) {
        if (writer != null) {
            writer.close();
        }
    }

    /**
     * Switch the writer implementation (e.g., from synchronous to store-and-forward). This method
     * is thread-safe and ensures proper cleanup of the old writer.
     *
     * @param writer The new writer implementation
     * @throws IOException if the switch fails
     */
    protected void switchWriter(ReplicationLogGroupWriter writer) throws IOException {
        synchronized (this) {
            if (closed) {
                throw new IOException("Closed");
            }
            ReplicationLogGroupWriter oldWriter = this.writer;
            LOG.info("Switching writer for HA Group {} from {} to {}", haGroupId,
                oldWriter.getClass().getSimpleName(), writer.getClass().getSimpleName());
            try {
                // Initialize the new writer first
                writer.init();
                // Switch to the new writer
                this.writer = writer;
                closeWriter(oldWriter);
            } catch (IOException e) {
                // If switching failed, ensure we clean up the new writer
                closeWriter(writer);
                throw e;
            }
        }
    }
}
