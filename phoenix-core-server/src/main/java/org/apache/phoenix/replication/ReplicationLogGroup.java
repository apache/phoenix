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

import static org.apache.hadoop.hbase.HConstants.DEFAULT_ZK_SESSION_TIMEOUT;
import static org.apache.hadoop.hbase.HConstants.ZK_SESSION_TIMEOUT;
import static org.apache.phoenix.replication.ReplicationLogGroup.LogEvent.EVENT_TYPE_DATA;
import static org.apache.phoenix.replication.ReplicationLogGroup.LogEvent.EVENT_TYPE_SYNC;
import static org.apache.phoenix.replication.ReplicationLogGroup.ReplicationMode.INIT;
import static org.apache.phoenix.replication.ReplicationLogGroup.ReplicationMode.STORE_AND_FORWARD;
import static org.apache.phoenix.replication.ReplicationLogGroup.ReplicationMode.SYNC;
import static org.apache.phoenix.replication.ReplicationLogGroup.ReplicationMode.SYNC_AND_FORWARD;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.jdbc.HAGroupStoreManager;
import org.apache.phoenix.jdbc.HAGroupStoreRecord;
import org.apache.phoenix.jdbc.HAGroupStoreRecord.HAGroupState;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogGroupSource;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogGroupSourceImpl;
import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.base.Throwables;
import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * ReplicationLogGroup manages replication logs for a given HA Group.
 * <p>
 * This class provides an API for replication operations and delegates to either synchronous
 * replication or store-and-forward replication based on the current replication mode.
 * </p>
 * Architecture Overview:
 * <pre>
 *  ┌──────────────────────────────────────────────────────────────────────┐
 *  │                       ReplicationLogGroup                            │
 *  │                                                                      │
 *  │  ┌─────────────┐     ┌────────────────────────────────────────────┐  │
 *  │  │             │     │                                            │  │
 *  │  │  Producers  │     │  Disruptor Ring Buffer                     │  │
 *  │  │  (append/   │────▶│  ┌─────────┐ ┌─────────┐ ┌─────────┐       │  │
 *  │  │   sync)     │     │  │ Event 1 │ │ Event 2 │ │ Event 3 │ ...   │  │
 *  │  │             │     │  └─────────┘ └─────────┘ └─────────┘       │  │
 *  │  └─────────────┘     └────────────────────────────────────────────┘  │
 *  │                                │                                     │
 *  │                                ▼                                     │
 *  │  ┌─────────────────────────────────────────────────────────────┐     │
 *  │  │                                                             │     │
 *  │  │  LogEventHandler                                            │     │
 *  │  │  - Batch Management                                         │     │
 *  │  │  - Replay events on errors                                  │     │
 *  │  │  - Mode Transitions                                         │     │
 *  │  │                                                             │     │
 *  │  │  ┌──────────────────────────────────────────────────────┐   │     │
 *  │  │  │  ReplicationModeImpl                                 │   │     │
 *  │  │  │  - Mode entry/exit                                   │   │     │
 *  │  │  │  - Failure Handling                                  │   │     │
 *  │  │  │                                                      │   │     │
 *  │  │  │  ┌────────────────────────────────────────────────┐  │   │     │
 *  │  │  │  │  ReplicationLog                                │  │   │     │
 *  │  │  │  │  ┌──────────────────────────────────────────┐  │  │   │     │
 *  │  │  │  │  │  LogFileWriter                           │  │  │   │     │
 *  │  │  │  │  │  - File Management                       │  │  │   │     │
 *  │  │  │  │  │  - Compression                           │  │  │   │     │
 *  │  │  │  │  │  - HDFS Operations                       │  │  │   │     │
 *  │  │  │  │  └──────────────────────────────────────────┘  │  │   │     │
 *  │  │  │  └────────────────────────────────────────────────┘  │   │     │
 *  │  │  └──────────────────────────────────────────────────────┘   │     │
 *  │  └─────────────────────────────────────────────────────────────┘     │
 *  └──────────────────────────────────────────────────────────────────────┘
 * </pre>
 * Key features:
 * <ul>
 *   <li>Provides append() and sync() API for higher layers</li>
 *   <li>Manages the replication mode</li>
 *   <li>Delegates the append and sync events to the current replication mode</li>
 *   <li>Thread-safe operations</li>
 * </ul>
 * <p>
 * A high-performance ring buffer decouples the API from the complexity of writer management.
 * Callers of append and sync generally return quickly, except for sync, where the writer must
 * suspend the caller until the sync operation is successful (or times out). An internal single
 * threaded process handles these events, encapsulating the complexity of batching mutations for
 * efficiency, consolidating multiple in-flight syncs and mode transitions.
 */
public class ReplicationLogGroup {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationLogGroup.class);

    // Configuration constants from original ReplicationLog
    public static final String REPLICATION_STANDBY_HDFS_URL_KEY =
        "phoenix.replication.log.standby.hdfs.url";
    public static final String REPLICATION_FALLBACK_HDFS_URL_KEY =
        "phoenix.replication.log.fallback.hdfs.url";
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
    public static final String REPLICATION_LOG_SYNC_RETRIES_KEY =
        "phoenix.replication.log.sync.retries";
    public static final int DEFAULT_REPLICATION_LOG_SYNC_RETRIES = 4;
    public static final String REPLICATION_LOG_ROTATION_RETRIES_KEY =
        "phoenix.replication.log.rotation.retries";
    public static final int DEFAULT_REPLICATION_LOG_ROTATION_RETRIES = 5;
    public static final String REPLICATION_LOG_RETRY_DELAY_MS_KEY =
        "phoenix.replication.log.retry.delay.ms";
    public static final long DEFAULT_REPLICATION_LOG_RETRY_DELAY_MS = 100L;
    private static final long DEFAULT_HDFS_WRITE_RPC_TIMEOUT_MS = 30*1000;

    public static final String STANDBY_DIR = "in";
    public static final String FALLBACK_DIR = "out";

    /** Cache of ReplicationLogGroup instances by HA Group ID */
    protected static final ConcurrentHashMap<String, ReplicationLogGroup> INSTANCES =
        new ConcurrentHashMap<>();

    protected final Configuration conf;
    protected final ServerName serverName;
    protected final String haGroupName;
    protected final HAGroupStoreManager haGroupStoreManager;
    protected final MetricsReplicationLogGroupSource metrics;
    protected ReplicationShardDirectoryManager standbyShardManager;
    protected ReplicationShardDirectoryManager fallbackShardManager;
    protected ReplicationLogDiscoveryForwarder logForwarder;
    protected long syncTimeoutMs;
    protected volatile boolean closed = false;

    /**
     * The replication mode determines how mutations are handled.
     * Mode transitions occur automatically based on the availability of the standby cluster's HDFS
     * and the state of the local mutation queue.
     */
    public enum ReplicationMode {
        /**
         * Dummy mode before we transition to the mode based on the HAGroupStore state
         */
        INIT {
            @Override
            ReplicationModeImpl createModeImpl(ReplicationLogGroup logGroup) {
                return new Init(logGroup);
            }
        },

        /**
         * Normal operation where mutations are written directly to the standby cluster's HDFS.
         * This is the default and primary mode of operation.
         */
        SYNC {
            @Override
            ReplicationModeImpl createModeImpl(ReplicationLogGroup logGroup) {
                return new SyncModeImpl(logGroup);
            }
        },

        /**
         * Fallback mode when the standby cluster's HDFS is unavailable. Mutations are stored
         * locally and will be forwarded when connectivity is restored.
         */
        STORE_AND_FORWARD {
            @Override
            ReplicationModeImpl createModeImpl(ReplicationLogGroup logGroup) {
                return new StoreAndForwardModeImpl(logGroup);
            }
        },

        /**
         * Transitional mode where new mutations are written directly to the standby cluster
         * while concurrently draining the local queue of previously stored mutations. This mode
         * is entered when connectivity to the standby cluster is restored and there are still
         * mutations in the local queue.
         */
        SYNC_AND_FORWARD {
            @Override
            ReplicationModeImpl createModeImpl(ReplicationLogGroup logGroup) {
                return new SyncAndForwardModeImpl(logGroup);
            }
        };

        abstract ReplicationModeImpl createModeImpl(ReplicationLogGroup logGroup);
    }
    // Tracks the current replication mode of the ReplicationLogGroup.
    private AtomicReference<ReplicationMode> mode;

    /*
     * Dummy mode before we transition to the mode based on the HAGroupStore state
     */
    private static class Init extends ReplicationModeImpl {
        Init(ReplicationLogGroup logGroup) {
            super(logGroup);
        }

        @Override
        void onEnter() throws IOException {}

        @Override
        void onExit(boolean gracefulShutdown) {}

        @Override
        ReplicationMode onFailure(Throwable e) throws IOException {
            throw new UnsupportedOperationException("Not supported for " + this);
        }

        @Override
        void append(Record r) throws IOException {
            throw new UnsupportedOperationException("Not supported for " + this);
        }

        @Override
        void sync() throws IOException {
            throw new UnsupportedOperationException("Not supported for " + this);
        }

        @Override
        ReplicationMode getMode() {
            return INIT;
        }
    }

    private static final ImmutableMap<ReplicationMode, EnumSet<ReplicationMode>> allowedTransition =
            Maps.immutableEnumMap(ImmutableMap.of(
                    INIT, EnumSet.of(SYNC, STORE_AND_FORWARD),
                    SYNC, EnumSet.of(STORE_AND_FORWARD, SYNC_AND_FORWARD),
                    STORE_AND_FORWARD, EnumSet.of(SYNC_AND_FORWARD),
                    SYNC_AND_FORWARD, EnumSet.of(SYNC, STORE_AND_FORWARD))
            );


    /** Event structure for the Disruptor ring buffer containing data and sync operations. */
    protected static class LogEvent {
        protected static final EventFactory<LogEvent> EVENT_FACTORY = LogEvent::new;

        protected int type;
        protected Record record;
        protected CompletableFuture<Void> syncFuture; // Used only for SYNC events
        protected long timestampNs; // Timestamp when event was created

        public static final byte EVENT_TYPE_DATA = 0;
        public static final byte EVENT_TYPE_SYNC = 1;

        public void setValues(int type, Record record, CompletableFuture<Void> syncFuture) {
            this.type = type;
            this.record = record;
            this.syncFuture = syncFuture;
            this.timestampNs = System.nanoTime();
        }
    }

    protected static class Record {
        public String tableName;
        public long commitId;
        public Mutation mutation;

        public Record(String tableName, long commitId, Mutation mutation) {
            this.tableName = tableName;
            this.commitId = commitId;
            this.mutation = mutation;
        }
    }
    protected Disruptor<LogEvent> disruptor;
    protected RingBuffer<LogEvent> ringBuffer;
    protected LogEventHandler eventHandler;
    // Used to inform the disruptor event thread whether this is a graceful or a forced shutdown
    private final AtomicBoolean gracefulShutdownEventHandlerFlag = new AtomicBoolean();

    /**
     * Get or create a ReplicationLogGroup instance for the given HA Group.
     *
     * @param conf Configuration object
     * @param serverName The server name
     * @param haGroupName The HA Group name
     * @return ReplicationLogGroup instance
     * @throws RuntimeException if initialization fails
     */
    public static ReplicationLogGroup get(Configuration conf,
                                          ServerName serverName,
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
     * Get or create a ReplicationLogGroup instance for the given HA Group.
     * Used mainly for testing
     *
     * @param conf Configuration object
     * @param serverName The server name
     * @param haGroupName The HA Group name
     * @param haGroupStoreManager HA Group Store Manager instance
     * @return ReplicationLogGroup instance
     * @throws RuntimeException if initialization fails
     */
    public static ReplicationLogGroup get(Configuration conf,
                                          ServerName serverName,
                                          String haGroupName,
                                          HAGroupStoreManager haGroupStoreManager) {
        return INSTANCES.computeIfAbsent(haGroupName, k -> {
            try {
                ReplicationLogGroup group = new ReplicationLogGroup(conf,
                        serverName, haGroupName, haGroupStoreManager);
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
    protected ReplicationLogGroup(Configuration conf,
                                  ServerName serverName,
                                  String haGroupName) {
        this(conf, serverName, haGroupName, HAGroupStoreManager.getInstance(conf));
    }

    /**
     * Protected constructor for ReplicationLogGroup.
     *
     * @param conf Configuration object
     * @param serverName The server name
     * @param haGroupName The HA Group name
     * @param haGroupStoreManager HA Group Store Manager instance
     */
    protected ReplicationLogGroup(Configuration conf,
                                  ServerName serverName,
                                  String haGroupName,
                                  HAGroupStoreManager haGroupStoreManager) {
        this.conf = conf;
        this.serverName = serverName;
        this.haGroupName = haGroupName;
        this.haGroupStoreManager = haGroupStoreManager;
        this.metrics = createMetricsSource();
        this.mode = new AtomicReference<>(INIT);
    }

    /**
     * Initialize the ReplicationLogGroup
     *
     * @throws IOException if initialization fails
     */
    protected void init() throws IOException {
        // First initialize the shard managers
        this.standbyShardManager = createStandbyShardManager();
        this.fallbackShardManager = createFallbackShardManager();
        // Initialize the replication log forwarder. The log forwarder is only activated when
        // we switch to STORE_AND_FORWARD or SYNC_AND_FORWARD mode
        this.logForwarder = new ReplicationLogDiscoveryForwarder(this);
        this.logForwarder.init();
        // Initialize the replication mode based on the HAGroupStore state
        initializeReplicationMode();
        // Use the override value if provided in the config, else use a derived value
        this.syncTimeoutMs = conf.getLong(REPLICATION_LOG_SYNC_TIMEOUT_KEY,
                calculateSyncTimeout());
        // Initialize the disruptor so that we start processing events
        initializeDisruptor();
        LOG.info("HAGroup {} started with mode={}", this, mode);
    }

    /**
     * Calculate how long the application thread should wait for a sync to finish.
     * The application thread here is the write rpc handler thread. It takes into account
     * the number of retries, pause between successive attempts, dfs write timeout and zk
     * session timeouts.
     *
     * @return sync timeout in ms
    */
    protected long calculateSyncTimeout() {
        int maxAttempts = conf.getInt(REPLICATION_LOG_SYNC_RETRIES_KEY,
                DEFAULT_REPLICATION_LOG_SYNC_RETRIES) + 1;
        long retryDelayMs = conf.getLong(REPLICATION_LOG_RETRY_DELAY_MS_KEY,
                DEFAULT_REPLICATION_LOG_RETRY_DELAY_MS);
        long wrtiteRpcTimeout = conf.getLong(DFSConfigKeys.DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY,
                DEFAULT_HDFS_WRITE_RPC_TIMEOUT_MS);
        // account for HAGroupStore update when we switch replication mode
        long zkTimeoutMs = conf.getLong(ZK_SESSION_TIMEOUT, DEFAULT_ZK_SESSION_TIMEOUT);
        long totalRpcTimeout =  maxAttempts*wrtiteRpcTimeout + (maxAttempts - 1)*retryDelayMs;
        return 2*totalRpcTimeout + zkTimeoutMs;
    }

    /**
     * Initialize the replication mode based on the HAGroupStore state
     *
     * @throws IOException
     */
    protected void initializeReplicationMode() throws IOException {
        Optional<HAGroupStoreRecord> haGroupStoreRecord =
                haGroupStoreManager.getHAGroupStoreRecord(haGroupName);
        if (haGroupStoreRecord.isPresent()) {
            HAGroupStoreRecord record = haGroupStoreRecord.get();
            HAGroupState haGroupState = record.getHAGroupState();
            if (haGroupState.equals(HAGroupState.ACTIVE_IN_SYNC)) {
                setMode(SYNC);
            } else if (haGroupState.equals(HAGroupState.ACTIVE_NOT_IN_SYNC)) {
                setMode(STORE_AND_FORWARD);
            } else {
                String message = String.format("HAGroup %s got an unexpected state %s while " +
                        "initializing mode", this, haGroupState);
                LOG.error(message);
                throw new IOException(message);
            }
        } else {
            String message = String.format("HAGroup %s got an empty group store record while " +
                    "initializing mode", this);
            LOG.error(message);
            throw new IOException(message);
        }
    }

    /** Initialize the Disruptor. */
    @SuppressWarnings("unchecked")
    protected void initializeDisruptor() throws IOException {
        int ringBufferSize = conf.getInt(REPLICATION_LOG_RINGBUFFER_SIZE_KEY,
                DEFAULT_REPLICATION_LOG_RINGBUFFER_SIZE);
        disruptor = new Disruptor<>(LogEvent.EVENT_FACTORY, ringBufferSize,
                new ThreadFactoryBuilder()
                        .setNameFormat("ReplicationLogGroup-" + getHAGroupName() + "-%d")
                        .setDaemon(true).build(),
                ProducerType.MULTI, new YieldingWaitStrategy());
        eventHandler = new LogEventHandler();
        eventHandler.init();
        disruptor.handleEventsWith(eventHandler);
        LogExceptionHandler exceptionHandler = new LogExceptionHandler();
        disruptor.setDefaultExceptionHandler(exceptionHandler);
        ringBuffer = disruptor.start();
    }

    /**
     * Append a mutation to the log. This method is non-blocking and returns quickly, unless the
     * ring buffer is full. The actual write happens asynchronously. We expect multiple append()
     * calls followed by a sync(). The appends will be batched by the Disruptor. Should the ring
     * buffer become full, which is not expected under normal operation but could (and should)
     * happen if the log file writer is unable to make progress, due to a HDFS level disruption.
     * Should we enter that condition this method will block until the append can be inserted.
     * <p>
     * An internal error may trigger fail-stop behavior. Subsequent to fail-stop, this method will
     * throw an IOException("Closed"). No further appends are allowed.
     *
     * @param tableName The name of the HBase table the mutation applies to.
     * @param commitId  The commit identifier (e.g., SCN) associated with the mutation.
     * @param mutation  The HBase Mutation (Put or Delete) to be logged.
     * @throws IOException If the writer is closed or if the ring buffer is full.
     */
    public void append(String tableName, long commitId, Mutation mutation) throws IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Append: table={}, commitId={}, mutation={}", tableName, commitId, mutation);
        }
        if (closed) {
            throw new IOException("Closed");
        }
        long startTime = System.nanoTime();
        try {
            // ringBuffer.next() claims the next sequence number. Because we initialize the Disruptor
            // with ProducerType.MULTI and the blocking YieldingWaitStrategy this call WILL BLOCK if
            // the ring buffer is full, thus providing backpressure to the callers.
            long sequence = ringBuffer.next();
            try {
                LogEvent event = ringBuffer.get(sequence);
                event.setValues(EVENT_TYPE_DATA, new Record(tableName, commitId, mutation), null);
            } finally {
                // Update ring buffer events metric
                ringBuffer.publish(sequence);
            }
        } finally {
            metrics.updateAppendTime(System.nanoTime() - startTime);
        }
    }

    /**
     * Ensures all previously appended records are durably persisted. This method blocks until the
     * sync operation completes or fails, potentially after internal retries. All in flight appends
     * are batched and provided to the underlying LogWriter, which will then be synced. If there is
     * a problem syncing the LogWriter we will retry, up to the retry limit, rolling the writer for
     * each retry. If the operation is still not successful and if we are in SYNC mode or in
     * SYNC_AND_FORWARD mode then we switch the mode to STORE_AND_FORWARD and try again. If the
     * sync operation still fails after switching to the STORE_AND_FORWARD mode then we treat this
     * as a fatal error and abort the region server.
     * <p>
     * An internal error may trigger fail-stop behavior. Subsequent to fail-stop, this method will
     * throw an IOException("Closed"). No further syncs are allowed.
     * <p>

     * @throws IOException If the sync operation fails after retries, or if interrupted.
     */
    public void sync() throws IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Sync");
        }
        if (closed) {
            throw new IOException("Closed");
        }
        long startTime = System.nanoTime();
        try {
            syncInternal();
        } finally {
            metrics.updateSyncTime(System.nanoTime() - startTime);
        }
    }

    /**
     * Internal implementation of sync that publishes a sync event to the ring buffer and waits
     * for completion.
     */
    protected void syncInternal() throws IOException {
        CompletableFuture<Void> syncFuture = new CompletableFuture<>();
        long sequence = ringBuffer.next();
        try {
            LogEvent event = ringBuffer.get(sequence);
            event.setValues(EVENT_TYPE_SYNC, null, syncFuture);
        } finally {
            ringBuffer.publish(sequence);
        }
        LOG.trace("Published EVENT_TYPE_SYNC at sequence {}", sequence);
        try {
            // Wait for the event handler to process up to and including this sync event
            syncFuture.get(syncTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("Interrupted while waiting for sync");
        } catch (ExecutionException e) {
            // After exhausting all attempts to sync to the standby cluster we switch mode
            // and then retry again. If that also fails, it is a fatal error
            String message = String.format("HAGroup %s sync operation failed", this);
            LOG.error(message, e);
            abort(message, e);
        } catch (TimeoutException e) {
            String message = String.format("HAGroup %s sync operation timed out", this);
            LOG.error(message);
            // sync timeout is a fatal error
            abort(message, e);
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
     * Force closes the log group upon an unrecoverable internal error.
     * This is a fail-stop behavior: once called, the log group is marked as closed,
     * the Disruptor is halted, and all subsequent append() and sync() calls will
     * throw an IOException("Closed"). This ensures that no further operations are attempted on a
     * log group that has encountered a critical error.
     */
    protected void closeOnError() {
        if (closed) {
            return;
        }
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }
        // Directly halt the disruptor. shutdown() would wait for events to drain. We are expecting
        // that will not work.
        gracefulShutdownEventHandlerFlag.set(false);
        disruptor.halt();
        metrics.close();
        LOG.info("HAGroup {} closed on error", this);
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
            // Sync before shutting down to flush all pending appends.
            try {
                syncInternal();
                gracefulShutdownEventHandlerFlag.set(true);
                disruptor.shutdown(); // Wait for a clean shutdown.
            } catch (IOException e) {
                LOG.warn("Error during final sync on close", e);
                gracefulShutdownEventHandlerFlag.set(false);
                disruptor.halt(); // Go directly to halt.
            }
            // TODO revisit close logic and the below comment
            // We must wait for the disruptor before closing the writers.
            metrics.close();
            LOG.info("HAGroup {} closed", this);
        }
    }

    /**
     * Switch the replication mode to the new mode
     *
     * @param newReplicationMode The new replication mode
     * @return previous replication mode
     */
    protected ReplicationMode setMode(ReplicationMode newReplicationMode) {
        ReplicationMode previous = mode.getAndUpdate( current -> newReplicationMode);
        if (previous != newReplicationMode) {
            LOG.info("HAGroup {} switched from {} to {}", this, previous, newReplicationMode);
        }
        return previous;
    }

    /**
     * Switch the replication mode only if the current mode matches the expected mode.
     *
     * @param expectedReplicationMode
     * @param newReplicationMode
     * @return true if the mode was updated else false
     */
    protected boolean checkAndSetMode(ReplicationMode expectedReplicationMode,
                                      ReplicationMode newReplicationMode) {
        boolean updated = mode.compareAndSet(expectedReplicationMode, newReplicationMode);
        if (updated) {
            LOG.info("HAGroup {} conditionally switched from {} to {}", this,
                    expectedReplicationMode, newReplicationMode);
        } else {
            LOG.info("HAGroup {} ignoring attempt to switch replication mode to {} " +
                    "because expected={} != actual={}", this, newReplicationMode,
                    expectedReplicationMode, getMode());
        }
        return updated;
    }

    /** Get the current replication mode */
    protected ReplicationMode getMode() {
        return mode.get();
    }

    /** Get the current metrics source for monitoring operations. */
    public MetricsReplicationLogGroupSource getMetrics() {
        return metrics;
    }

    /** Create a new metrics source for monitoring operations. */
    protected MetricsReplicationLogGroupSource createMetricsSource() {
        return new MetricsReplicationLogGroupSourceImpl(haGroupName);
    }

    /**
     * Get the name for this HA Group.
     *
     * @return The name for this HA Group
     */
    public String getHAGroupName() {
        return haGroupName;
    }

    protected HAGroupStoreManager getHAGroupStoreManager() {
        return haGroupStoreManager;
    }

    protected Configuration getConfiguration() {
        return conf;
    }

    protected ServerName getServerName() {
        return serverName;
    }

    @Override
    public String toString() {
        return getHAGroupName();
    }

    /**
     * Creates the top level directory on the cluster determined by the URI
     *
     * @param urlKey Config property for the URI
     * @param logDirName Top level directory underneath which the shards will be created
     *
     * @return ReplicationShardDirectoryManager
     * @throws IOException
     */
    private ReplicationShardDirectoryManager createShardManager(
            String urlKey, String logDirName) throws IOException {
        URI rootURI = getLogURI(urlKey);
        FileSystem fs = getFileSystem(rootURI);
        LOG.info("HAGroup {} initialized filesystem at {}", this, rootURI);
        // root dir path is <URI>/<HAGroupName>/[in|out]
        Path rootDirPath = new Path(new Path(rootURI.getPath(), getHAGroupName()), logDirName);
        if (!fs.exists(rootDirPath)) {
            LOG.info("HAGroup {} creating root directory at {}", this, rootDirPath);
            if (!fs.mkdirs(rootDirPath)) {
                throw new IOException("Failed to create directory: " + rootDirPath);
            }
        }
        return new ReplicationShardDirectoryManager(conf, fs, rootDirPath);
    }

    /** create shard manager for the standby cluster */
    protected ReplicationShardDirectoryManager createStandbyShardManager() throws IOException {
        return createShardManager(REPLICATION_STANDBY_HDFS_URL_KEY, STANDBY_DIR);
    }

    /** create shard manager for the fallback cluster */
    protected ReplicationShardDirectoryManager createFallbackShardManager() throws IOException {
        return createShardManager(REPLICATION_FALLBACK_HDFS_URL_KEY, FALLBACK_DIR);
    }

    /** return shard manager for the standby cluster */
    protected ReplicationShardDirectoryManager getStandbyShardManager() {
        return standbyShardManager;
    }

    /** return shard manager for the fallback cluster */
    protected ReplicationShardDirectoryManager getFallbackShardManager() {
        return fallbackShardManager;
    }

    private URI getLogURI(String urlKey) throws IOException {
        String urlString = conf.get(urlKey);
        if (urlString == null || urlString.trim().isEmpty()) {
            throw new IOException("HDFS URL not configured: " + urlKey);
        }
        try {
            return new URI(urlString);
        } catch (URISyntaxException e) {
            throw new IOException("Invalid HDFS URL: " + urlString, e);
        }
    }

    private FileSystem getFileSystem(URI uri) throws IOException {
        return FileSystem.get(uri, conf);
    }

    /** Create the standby(synchronous) writer */
    protected ReplicationLog createStandbyLog() throws IOException {
        return new ReplicationLog(this, standbyShardManager);
    }

    /** Create the fallback writer */
    protected ReplicationLog createFallbackLog() throws IOException {
        return new ReplicationLog(this, fallbackShardManager);
    }

    /** Returns the log forwarder for this replication group */
    protected ReplicationLogDiscoveryForwarder getLogForwarder() {
        return logForwarder;
    }

    /** Returns the currently active writer. Mainly for tests. */
    protected ReplicationLog getActiveLog() {
        return eventHandler.getCurrentModeImpl().getReplicationLog();
    }

    protected void setHAGroupStatusToStoreAndForward() throws Exception {
        try {
            haGroupStoreManager.setHAGroupStatusToStoreAndForward(haGroupName);
        }
        catch (Exception ex) {
            LOG.info("HAGroup {} failed to set status to STORE_AND_FORWARD", this, ex);
            throw ex;
        }
    }

    protected void setHAGroupStatusToSync() throws IOException {
        try {
            haGroupStoreManager.setHAGroupStatusToSync(haGroupName);
        } catch (IOException ex) {
            // TODO logging
            throw ex;
        }
        catch (Exception ex) {
            // TODO logging
            throw new IOException(ex);
        }
    }

    /**
     * Abort when we hit a fatal error
     *
     * @param reason
     * @param cause
     */
    protected void abort(String reason, Throwable cause) {
        // TODO better to use abort using RegionServerServices
        String msg = "***** ABORTING region server: " + reason + " *****";
        if (cause != null) {
            msg += "\nCause:\n" + Throwables.getStackTraceAsString(cause);
        }
        LOG.error(msg);
        if (cause != null) {
            throw new RuntimeException(msg, cause);
        } else {
            throw new RuntimeException(msg);
        }
    }

    /**
     * Handles events from the Disruptor,
     */
    protected class LogEventHandler implements EventHandler<LogEvent>, LifecycleAware {
        private final List<CompletableFuture<Void>> pendingSyncFutures = new ArrayList<>();
        // Current replication mode implementation which will handle the events
        private ReplicationModeImpl currentModeImpl;

        public LogEventHandler() {
        }

        public void init() throws IOException {
            initializeMode(getMode());
        }

        @VisibleForTesting
        public ReplicationModeImpl getCurrentModeImpl() {
            return currentModeImpl;
        }

        private void initializeMode(ReplicationMode newMode) throws IOException {
            try {
                currentModeImpl = newMode.createModeImpl(ReplicationLogGroup.this);
                currentModeImpl.onEnter();
            } catch (IOException e) {
                LOG.error("HAGroup {} couldn't initialize mode {}",
                        ReplicationLogGroup.this, currentModeImpl, e);
                updateModeOnFailure(e);
            }
        }

        private void updateModeOnFailure(IOException e) throws IOException {
            // send the failed event to the current mode
            ReplicationMode newMode = currentModeImpl.onFailure(e);
            setMode(newMode);
            currentModeImpl.onExit(true);
            initializeMode(newMode);
        }

        /**
         * Processes all pending sync operations by syncing the current writer and completing
         * their associated futures. This method is called when we are ready to process a set of
         * consolidated sync requests and performs the following steps:
         * <ol>
         *   <li>Syncs the current writer to ensure all data is durably written.</li>
         *   <li>Completes all pending sync futures successfully.</li>
         *   <li>Clears the list of pending sync futures.</li>
         *   <li>Clears the current batch of records since they have been successfully synced.</li>
         * </ol>
         * @param sequence The sequence number of the last processed event
         * @throws IOException if the sync operation fails
         */
        private void processPendingSyncs(long sequence) throws IOException {
            if (pendingSyncFutures.isEmpty()) {
                return;
            }
            // call sync on the current mode
            currentModeImpl.sync();
            // Complete all pending sync futures
            for (CompletableFuture<Void> future : pendingSyncFutures) {
                future.complete(null);
            }
            pendingSyncFutures.clear();
            LOG.info("Sync operation completed successfully up to sequence {}", sequence);
        }

        /**
         * Fails all pending sync operations with the given exception. This method is called when
         * we encounter an unrecoverable error during the sync of the inner writer. It completes
         * all pending sync futures that were consolidated exceptionally.
         * <p>
         * Note: This method does not clear the currentBatch list. The currentBatch must be
         * preserved as it contains records that may need to be replayed if we successfully
         * rotate to a new writer.
         *
         * @param sequence The sequence number of the last processed event
         * @param e The IOException that caused the failure
         */
        private void failPendingSyncs(long sequence, IOException e) {
            if (pendingSyncFutures.isEmpty()) {
                return;
            }
            for (CompletableFuture<Void> future : pendingSyncFutures) {
                future.completeExceptionally(e);
            }
            pendingSyncFutures.clear();
            LOG.warn("Failed to process syncs at sequence {}", sequence, e);
        }

        /**
         * Handle the failure while processing an event
         *
         * @param failedEvent Event which triggered the failure
         * @param sequence Sequence number of the failed event
         * @param cause Reason of failure
         */
        private void onFailure(LogEvent failedEvent,
                               long sequence,
                               IOException cause) throws IOException {
            // fetch the in-flight appends
            List<Record> unsyncedAppends = currentModeImpl.log.getCurrentBatch();
            // try updating the mode
            updateModeOnFailure(cause);
            // retry the batch after updating the mode
            replayBatch(unsyncedAppends);
            // retry the failed event
            replayFailedEvent(failedEvent, sequence);
        }

        /** Replay all append events which were not yet synced */
        private void replayBatch(List<Record> unsyncedAppends) throws IOException {
            for (Record r : unsyncedAppends) {
                currentModeImpl.append(r);
            }
        }

        /** Retry the failed event after switching mode */
        private void replayFailedEvent(LogEvent failedEvent,
                                       long sequence) throws IOException {
            // now retry the event which failed
            // only need to retry append event since for sync event we have already added the
            // sync event future to the pending future list before the sync event can potentially
            // fail.
            if (failedEvent.type == EVENT_TYPE_DATA) {
                currentModeImpl.append(failedEvent.record);
            }
            processPendingSyncs(sequence);
        }

        /**
         * Processes a single event from the Disruptor ring buffer. This method handles both data
         * and sync events.
         * <p>
         * For data events, it:
         * <ol>
         *   <li>Sends the append event to the current mode.</li>
         *   <li>Processes any pending syncs if this is the end of a batch.</li>
         * </ol>
         * <p>
         * For sync events, it:
         * <ol>
         *   <li>Adds the sync future to the pending list.</li>
         *   <li>Processes any pending syncs if this is the end of a batch.</li>
         * </ol>
         * <p>
         * If an IOException occurs, it sends the failure event to the current replication mode
         * and then switches to the new mode. After switching to the new mode, it replays the
         * pending batch of un-synced appends and the failed event.
         * </p>
         *
         * @param event The event to process
         * @param sequence The sequence number of the event
         * @param endOfBatch Whether this is the last event in the current batch
         * @throws Exception if the operation fails after all retries
         */
        @Override
        public void onEvent(LogEvent event, long sequence, boolean endOfBatch) throws Exception {
            // Calculate time spent in ring buffer
            long currentTimeNs = System.nanoTime();
            long ringBufferTimeNs = currentTimeNs - event.timestampNs;
            metrics.updateRingBufferTime(ringBufferTimeNs);
            try {
                switch (event.type) {
                    case EVENT_TYPE_DATA:
                        currentModeImpl.append(event.record);
                        // Process any pending syncs at the end of batch.
                        if (endOfBatch) {
                            processPendingSyncs(sequence);
                        }
                        return;
                    case EVENT_TYPE_SYNC:
                        // Add this sync future to the pending list
                        // OK, to add the same future multiple times when we rewind the batch
                        // as completing an already completed future is a no-op
                        pendingSyncFutures.add(event.syncFuture);
                        // Process any pending syncs at the end of batch.
                        if (endOfBatch) {
                            processPendingSyncs(sequence);
                        }
                        // after a successful sync check the mode set on the replication group
                        // Doing the mode check on sync points makes the implementation more robust
                        // since we can guarantee that all unsynced appends have been flushed to the
                        // replication log before we switch the replication mode
                        ReplicationMode newMode = getMode();
                        if (newMode != currentModeImpl.getMode()) {
                            // some other thread switched the mode on the replication group
                            LOG.info("Mode switched at sequence {} from {} to {}",
                                    sequence, currentModeImpl, newMode);
                            // call exit on the last mode here since we can guarantee that the lastMode
                            // is not processing any event like append/sync because this is the only thread
                            // that is consuming the events from the ring buffer and handing them off to the
                            // mode
                            currentModeImpl.onExit(true);
                            initializeMode(newMode);
                        }
                        return;
                    default:
                        throw new UnsupportedOperationException("Unknown event type: "
                                + event.type);
                }
            } catch (IOException e) {
                try {
                    LOG.info("Failed to process event at sequence {} on mode {}", sequence, currentModeImpl, e);
                    onFailure(event, sequence, e);
                } catch (Exception fatalEx) {
                    // Either we failed to switch the mode or we are in STORE_AND_FORWARD mode
                    // and got an exception. This is a fatal exception so halt the disruptor
                    // fail the pending sync events with the original exception
                    failPendingSyncs(sequence, e);
                    // halt the disruptor with the fatal exception
                    throw fatalEx;
                }
            }
        }

        @Override
        public void onStart() {
            // no-op
        }

        @Override
        public void onShutdown() {
            boolean isGracefulShutdown = gracefulShutdownEventHandlerFlag.get();
            LOG.info("HAGroup {} shutting down event handler graceful={}",
                    ReplicationLogGroup.this, isGracefulShutdown);
            currentModeImpl.onExit(isGracefulShutdown);
        }
    }

    /**
     * Handler for critical errors during the Disruptor lifecycle that closes the writer to prevent
     * data loss.
     */
    protected class LogExceptionHandler implements ExceptionHandler<LogEvent> {
        @Override
        public void handleEventException(Throwable e, long sequence, LogEvent event) {
            String message = "Exception processing sequence " + sequence + "  for event " + event;
            LOG.error(message, e);
            closeOnError();
        }

        @Override
        public void handleOnStartException(Throwable e) {
            LOG.error("Exception during Disruptor startup", e);
            closeOnError();
        }

        @Override
        public void handleOnShutdownException(Throwable e) {
            // Should not happen, but if it does, the regionserver is aborting or shutting down.
            LOG.error("Exception during Disruptor shutdown", e);
            closeOnError();
        }
    }
}
