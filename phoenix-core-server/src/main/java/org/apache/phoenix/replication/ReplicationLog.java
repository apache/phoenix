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
import java.io.InterruptedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.phoenix.replication.log.LogFileWriter;
import org.apache.phoenix.replication.log.LogFileWriterContext;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogSource;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogSourceImpl;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * Manages the lifecycle of replication log files on the active cluster side. It handles log
 * rotation based on time and size thresholds and provides the currently active LogFileWriter.
 * This class is intended to be thread-safe.
 * <p>
 * Architecture Overview:
 * <pre>
 * ┌──────────────────────────────────────────────────────────────────────┐
 * │                           ReplicationLog                             │
 * │                                                                      │
 * │  ┌─────────────┐     ┌────────────────────────────────────────────┐  │
 * │  │             │     │                                            │  │
 * │  │  Producers  │     │  Disruptor Ring Buffer                     │  │
 * │  │  (append/   │────▶│  ┌─────────┐ ┌─────────┐ ┌─────────┐       │  │
 * │  │   sync)     │     │  │ Event 1 │ │ Event 2 │ │ Event 3 │ ...   │  │
 * │  │             │     │  └─────────┘ └─────────┘ └─────────┘       │  │
 * │  └─────────────┘     └────────────────────────────────────────────┘  │
 * │                                │                                     │
 * │                                │                                     │
 * │                                ▼                                     │
 * │  ┌─────────────────────────────────────────────────────────────┐     │
 * │  │                                                             │     │
 * │  │  LogEventHandler                                            │     │
 * │  │  ┌──────────────────────────────────────────────────────┐   │     │
 * │  │  │                                                      │   │     │
 * │  │  │  - Batch Management                                  │   │     │
 * │  │  │  - Writer Rotation                                   │   │     │
 * │  │  │  - Error Handling                                    │   │     │
 * │  │  │  - Mode Transitions                                  │   │     │
 * │  │  │                                                      │   │     │
 * │  │  └──────────────────────────────────────────────────────┘   │     │
 * │  │                             │                               │     │
 * │  │                             ▼                               │     │
 * │  │  ┌──────────────────────────────────────────────────────┐   │     │
 * │  │  │                                                      │   │     │
 * │  │  │  LogFileWriter                                       │   │     │
 * │  │  │  - File Management                                   │   │     │
 * │  │  │  - Compression                                       │   │     │
 * │  │  │  - HDFS Operations                                   │   │     │
 * │  │  │                                                      │   │     │
 * │  │  └──────────────────────────────────────────────────────┘   │     │
 * │  └─────────────────────────────────────────────────────────────┘     │
 * └──────────────────────────────────────────────────────────────────────┘
 * </pre>
 * <p>
 * The Disruptor provides a high-performance ring buffer that decouples the API from the complexity
 * of writer management. Producers (callers of append/sync) simply publish events to the ring
 * buffer and generally return quickly, except for sync(), where the writer will suspend the caller
 * until the sync operation is successful. The LogEventHandler processes these events, handling the
 * complexity of batching mutations for efficiency, rotating writers based on time or size, error
 * handling and retries, and mode transitions for store-and-forward.
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = { "EI_EXPOSE_REP", "EI_EXPOSE_REP2",
    "MS_EXPOSE_REP" }, justification = "Intentional")
public class ReplicationLog {

    /** The path on the standby HDFS where log files should be written. (The "IN" directory.) */
    public static final String REPLICATION_STANDBY_HDFS_URL_KEY =
        "phoenix.replication.log.standby.hdfs.url";
    /**
     * The path on the active HDFS where log files should be written when we have fallen back to
     * store and forward mode. (The "OUT" directory.)
     */
    public static final String REPLICATION_FALLBACK_HDFS_URL_KEY =
        "phoenix.replication.log.fallback.hdfs.url";
    /**
     * The number of shards (subfolders) to maintain in the "IN" directory.
     * <p>
     * Shard directories have the format shard-NNNNN, e.g. shard-00001. The maximum value is
     * 100000.
     */
    public static final String REPLICATION_NUM_SHARDS_KEY = "phoenix.replication.log.shards";
    public static final int DEFAULT_REPLICATION_NUM_SHARDS = 1000;
    public static final int MAX_REPLICATION_NUM_SHARDS = 100000;
    /** Replication log rotation time trigger, default is 1 minute */
    public static final String REPLICATION_LOG_ROTATION_TIME_MS_KEY =
        "phoenix.replication.log.rotation.time.ms";
    public static final long DEFAULT_REPLICATION_LOG_ROTATION_TIME_MS = 60 * 1000L;
    /** Replication log rotation size trigger, default is 256 MB */
    public static final String REPLICATION_LOG_ROTATION_SIZE_BYTES_KEY =
        "phoenix.replication.log.rotation.size.bytes";
    public static final long DEFAULT_REPLICATION_LOG_ROTATION_SIZE_BYTES = 256 * 1024 * 1024L;
    /** Replication log rotation size trigger percentage, default is 0.95 */
    public static final String REPLICATION_LOG_ROTATION_SIZE_PERCENTAGE_KEY =
        "phoenix.replication.log.rotation.size.percentage";
    public static final double DEFAULT_REPLICATION_LOG_ROTATION_SIZE_PERCENTAGE = 0.95;
    /** Replication log compression, default is "NONE" */
    public static final String REPLICATION_LOG_COMPRESSION_ALGORITHM_KEY =
        "phoenix.replication.log.compression";
    public static final String DEFAULT_REPLICATION_LOG_COMPRESSION_ALGORITHM = "NONE";
    public static final String REPLICATION_LOG_RINGBUFFER_SIZE_KEY =
        "phoenix.replication.log.ringbuffer.size";
    public static final int DEFAULT_REPLICATION_LOG_RINGBUFFER_SIZE = 1024 * 32;  // Too big?
    public static final String REPLICATION_LOG_SYNC_TIMEOUT_KEY =
        "phoenix.replication.log.sync.timeout.ms";
    public static final long DEFAULT_REPLICATION_LOG_SYNC_TIMEOUT = 1000 * 30;
    public static final String REPLICATION_LOG_SYNC_RETRIES_KEY =
        "phoenix.replication.log.sync.retries";
    public static final int DEFAULT_REPLICATION_LOG_SYNC_RETRIES = 5;
    public static final String REPLICATION_LOG_ROTATION_RETRIES_KEY =
        "phoenix.replication.log.rotation.retries";
    public static final int DEFAULT_REPLICATION_LOG_ROTATION_RETRIES = 5;

    public static final String SHARD_DIR_FORMAT = "shard%05d";
    public static final String FILE_NAME_FORMAT = "%d-%s.plog";

    static final byte EVENT_TYPE_DATA = 0;
    static final byte EVENT_TYPE_SYNC = 1;

    static final Logger LOG = LoggerFactory.getLogger(ReplicationLog.class);

    protected static volatile ReplicationLog instance;

    protected final Configuration conf;
    protected final ServerName serverName;
    protected FileSystem standbyFs;
    protected FileSystem fallbackFs; // For store-and-forward (future use)
    protected int numShards;
    protected URI standbyUrl;
    protected URI fallbackUrl; // For store-and-forward (future use)
    protected final long rotationTimeMs;
    protected final long rotationSizeBytes;
    protected final int maxRotationRetries;
    protected final Compression.Algorithm compression;
    protected final ReentrantLock lock = new ReentrantLock();
    protected volatile LogFileWriter currentWriter; // Current writer
    protected final AtomicLong lastRotationTime = new AtomicLong();
    protected final AtomicLong writerGeneration = new AtomicLong();
    protected final AtomicLong rotationFailures = new AtomicLong(0);
    protected ScheduledExecutorService rotationExecutor;
    protected final int ringBufferSize;
    protected final long syncTimeoutMs;
    protected Disruptor<LogEvent> disruptor;
    protected RingBuffer<LogEvent> ringBuffer;
    protected final ConcurrentHashMap<Path, Object> shardMap = new ConcurrentHashMap<>();
    protected final MetricsReplicationLogSource metrics;
    protected volatile boolean isClosed = false;

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

    /** The reason for requesting a log rotation. */
    protected enum RotationReason {
        /** Rotation requested due to time threshold being exceeded. */
        TIME,
        /** Rotation requested due to size threshold being exceeded. */
        SIZE,
        /** Rotation requested due to an error condition. */
        ERROR;
    }

    /**
     * The current replication mode. Always SYNC for now.
     * <p>TODO: Implement mode transitions to STORE_AND_FORWARD when standby becomes unavailable.
     * <p>TODO: Implement mode transitions to SYNC_AND_FORWARD when draining queue.
     */
    protected volatile ReplicationMode currentMode = ReplicationMode.SYNC;

    // TODO: Add configuration keys for store-and-forward behavior
    // - Maximum retry attempts before switching to store-and-forward
    // - Retry delay between attempts
    // - Queue drain batch size
    // - Queue drain interval

    // TODO: Add state tracking fields
    // - Queue of pending changes when in store-and-forward mode
    // - Timestamp of last successful standby write
    // - Error count for tracking consecutive failures

    // TODO: Add methods for state transitions
    // - switchToStoreAndForward() - Called when standby becomes unavailable
    // - switchToSync() - Called when standby becomes available again
    // - drainQueue() - Background task to process queued changes

    // TODO: Enhance error handling in LogEventHandler
    // - Track consecutive failures
    // - Switch to store-and-forward after max retries

    // TODO: Implement queue management for store-and-forward mode
    // - Implement queue persistence to handle RegionServer restarts
    // - Implement queue draining when in SYNC_AND_FORWARD state
    // - Implement automatic recovery from temporary network issues
    // - Add configurable thresholds for switching to store-and-forward based on write latency
    // - Add circuit breaker pattern to prevent overwhelming the standby cluster
    // - Add queue size limits and backpressure mechanisms
    // - Add queue metrics for monitoring (queue size, oldest entry age, etc.)

    // TODO: Enhance metrics for replication health monitoring
    // - Add metrics for replication lag between active and standby
    // - Track time spent in each replication mode (SYNC, STORE_AND_FORWARD, SYNC_AND_FORWARD)
    // - Monitor queue drain rate and backlog size
    // - Track consecutive failures and mode transition events

    /**
     * Gets the singleton instance of the ReplicationLogManager using the lazy initializer pattern.
     * Initializes the instance if it hasn't been created yet.
     * @param conf Configuration object.
     * @param serverName The server name.
     * @return The singleton ReplicationLogManager instance.
     * @throws IOException If initialization fails.
     */
    public static ReplicationLog get(Configuration conf, ServerName serverName)
          throws IOException {
        if (instance == null) {
            synchronized (ReplicationLog.class) {
                if (instance == null) {
                    // Complete initialization before assignment
                    ReplicationLog logManager = new ReplicationLog(conf, serverName);
                    logManager.init();
                    instance = logManager;
                }
            }
        }
        return instance;
    }

    protected ReplicationLog(Configuration conf, ServerName serverName) {
        this.conf = conf;
        this.serverName = serverName;
        this.rotationTimeMs = conf.getLong(REPLICATION_LOG_ROTATION_TIME_MS_KEY,
            DEFAULT_REPLICATION_LOG_ROTATION_TIME_MS);
        long rotationSize = conf.getLong(REPLICATION_LOG_ROTATION_SIZE_BYTES_KEY,
            DEFAULT_REPLICATION_LOG_ROTATION_SIZE_BYTES);
        double rotationSizePercent = conf.getDouble(REPLICATION_LOG_ROTATION_SIZE_PERCENTAGE_KEY,
            DEFAULT_REPLICATION_LOG_ROTATION_SIZE_PERCENTAGE);
        this.rotationSizeBytes = (long) (rotationSize * rotationSizePercent);
        this.maxRotationRetries = conf.getInt(REPLICATION_LOG_ROTATION_RETRIES_KEY,
            DEFAULT_REPLICATION_LOG_ROTATION_RETRIES);
        this.numShards = conf.getInt(REPLICATION_NUM_SHARDS_KEY, DEFAULT_REPLICATION_NUM_SHARDS);
        String compressionName = conf.get(REPLICATION_LOG_COMPRESSION_ALGORITHM_KEY,
            DEFAULT_REPLICATION_LOG_COMPRESSION_ALGORITHM);
        Compression.Algorithm compression = Compression.Algorithm.NONE;
        if (!DEFAULT_REPLICATION_LOG_COMPRESSION_ALGORITHM.equalsIgnoreCase(compressionName)) {
            try {
                compression = Compression.getCompressionAlgorithmByName(compressionName);
            } catch (IllegalArgumentException e) {
                LOG.warn("Unknown compression type " + compressionName + ", using NONE", e);
            }
        }
        this.compression = compression;
        this.ringBufferSize = conf.getInt(REPLICATION_LOG_RINGBUFFER_SIZE_KEY,
            DEFAULT_REPLICATION_LOG_RINGBUFFER_SIZE);
        this.syncTimeoutMs = conf.getLong(REPLICATION_LOG_SYNC_TIMEOUT_KEY,
            DEFAULT_REPLICATION_LOG_SYNC_TIMEOUT);
        this.metrics = createMetricsSource();
    }

    /** Creates a new metrics source for monitoring replication log operations. */
    protected MetricsReplicationLogSource createMetricsSource() {
        return new MetricsReplicationLogSourceImpl();
    }

    /** Returns the metrics source for monitoring replication log operations. */
    public MetricsReplicationLogSource getMetrics() {
        return metrics;
    }

    @SuppressWarnings("unchecked")
    public void init() throws IOException {
        if (numShards > MAX_REPLICATION_NUM_SHARDS) {
            throw new IllegalArgumentException(REPLICATION_NUM_SHARDS_KEY + " is " + numShards
                + ", but the limit is " + MAX_REPLICATION_NUM_SHARDS);
        }
        initializeFileSystems();
        // Start time based rotation.
        lastRotationTime.set(EnvironmentEdgeManager.currentTimeMillis());
        startRotationExecutor();
        // Create the initial writer. Do this before we call LogEventHandler.init().
        currentWriter = createNewWriter(standbyFs, standbyUrl);
        // Initialize the Disruptor. We use ProducerType.MULTI because multiple handlers might
        // call append concurrently. We use YieldingWaitStrategy for low latency. When the ring
        // buffer is full (controlled by REPLICATION_WRITER_RINGBUFFER_SIZE_KEY), producers
        // calling ringBuffer.next() will effectively block (by yielding/spinning), creating
        // backpressure on the callers. This ensures appends don't proceed until there is space.
        disruptor = new Disruptor<>(LogEvent.EVENT_FACTORY, ringBufferSize,
            new ThreadFactoryBuilder().setNameFormat("ReplicationLogEventHandler-%d")
                .setDaemon(true).build(),
            ProducerType.MULTI, new YieldingWaitStrategy());
        LogEventHandler eventHandler = new LogEventHandler();
        eventHandler.init();
        disruptor.handleEventsWith(eventHandler);
        LogExceptionHandler exceptionHandler = new LogExceptionHandler();
        disruptor.setDefaultExceptionHandler(exceptionHandler);
        ringBuffer = disruptor.start();
        LOG.info("ReplicationLogWriter started with ring buffer size {}", ringBufferSize);
    }

    /**
     * Append a mutation to the log. This method is non-blocking and returns quickly, unless the
     * ring buffer is full. The actual write happens asynchronously. We expect multiple append()
     * calls followed by a sync(). The appends will be batched by the Disruptor. Should the ring
     * buffer become full, which is not expected under normal operation but could (and should)
     * happen if the log file writer is unable to make progress, due to a HDFS level disruption.
     * Should we enter that condition this method will block until the append can be inserted.
     * @param tableName The name of the HBase table the mutation applies to.
     * @param commitId  The commit identifier (e.g., SCN) associated with the mutation.
     * @param mutation  The HBase Mutation (Put or Delete) to be logged.
     * @throws IOException If the writer is closed or if the ring buffer is full.
     */
    public void append(String tableName, long commitId, Mutation mutation) throws IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Append: table={}, commitId={}, mutation={}", tableName, commitId, mutation);
        }
        if (isClosed) {
            throw new IOException("Closed");
        }
        long startTime = System.nanoTime();
        // ringBuffer.next() claims the next sequence number. Because we initialize the Disruptor
        // with ProducerType.MULTI and the blocking YieldingWaitStrategy this call WILL BLOCK if
        // the ring buffer is full, thus providing backpressure to the callers.
        long sequence = ringBuffer.next();
        try {
            LogEvent event = ringBuffer.get(sequence);
            event.setValues(EVENT_TYPE_DATA, new Record(tableName, commitId, mutation), null);
            metrics.updateAppendTime(System.nanoTime() - startTime);
        } finally {
            // Update ring buffer events metric
            ringBuffer.publish(sequence);
        }
    }

    /**
     * Ensures all previously appended records are durably persisted. This method blocks until the
     * sync operation completes or fails, potentially after internal retries. All in flight appends
     * are batched and provided to the underlying LogWriter, which will then be synced. If there is
     * a problem syncing the LogWriter we will retry, up to the retry limit, rolling the writer for
     * each retry.
     * <p>
     * NOTE: When the ReplicationLogManager is capable of switching between synchronous and
     * fallback (store-and-forward) writers, then this will be pretty bullet proof. Right now we
     * will still try to roll the synchronous writer a few times before giving up.
     * @throws IOException If the sync operation fails after retries, or if interrupted.
     */
    public void sync() throws IOException {
        if (isClosed) {
            throw new IOException("Closed");
        }
        syncInternal();
    }

    /**
     * Internal implementation of sync that publishes a sync event to the ring buffer and waits
     * for completion.
     */
    protected void syncInternal() throws IOException {
        long startTime = System.nanoTime();
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
            metrics.updateSyncTime(System.nanoTime() - startTime);
        } catch (InterruptedException e) {
            // Almost certainly the regionserver is shutting down or aborting.
            // TODO: Do we need to do more here?
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("Interrupted while waiting for sync");
        } catch (ExecutionException e) {
            LOG.error("Sync operation failed", e.getCause());
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else {
                throw new IOException("Sync operation failed", e.getCause());
            }
        } catch (TimeoutException e) {
            String message = "Sync operation timed out";
            LOG.error(message);
            throw new IOException(message, e);
        }
    }

    /** Initializes the standby and fallback filesystems and creates their log directories. */
    protected void initializeFileSystems() throws IOException {
        String standbyUrlString = conf.get(REPLICATION_STANDBY_HDFS_URL_KEY);
        if (standbyUrlString == null) {
            throw new IOException(REPLICATION_STANDBY_HDFS_URL_KEY + " is not configured");
        }
        // Only validate that the URI is well formed. We should not assume the scheme must be
        // "hdfs" because perhaps the operator will substitute another FileSystem implementation
        // for DistributedFileSystem.
        try {
            this.standbyUrl = new URI(standbyUrlString);
        } catch (URISyntaxException e) {
            throw new IOException(REPLICATION_STANDBY_HDFS_URL_KEY + " is not valid", e);
        }
        String fallbackUrlString = conf.get(REPLICATION_FALLBACK_HDFS_URL_KEY);
        if (fallbackUrlString != null) {
            // Only validate that the URI is well formed, as above.
            try {
                this.fallbackUrl = new URI(fallbackUrlString);
            } catch (URISyntaxException e) {
                throw new IOException(REPLICATION_FALLBACK_HDFS_URL_KEY + " is not valid", e);
            }
            this.fallbackFs = getFileSystem(fallbackUrl);
            Path fallbackLogDir = new Path(fallbackUrl.getPath());
            if (!fallbackFs.exists(fallbackLogDir)) {
                LOG.info("Creating directory {}", fallbackUrlString);
                if (!this.fallbackFs.mkdirs(fallbackLogDir)) {
                    throw new IOException("Failed to create directory: " + fallbackUrlString);
                }
            }
        } else {
            // We support a synchronous replication only option if store-and-forward configuration
            // keys are missing. This is outside the scope of the design spec but potentially
            // useful for testing and also allows an operator to prefer failover consistency and
            // simplicity over availability, even if that is not recommended. Log it at WARN level
            // to focus appropriate attention. (Should it be ERROR?)
            LOG.warn("Fallback not configured ({}), store-and-forward DISABLED.",
                REPLICATION_FALLBACK_HDFS_URL_KEY);
            this.fallbackFs = null;
        }
        // Configuration is sorted, and possibly store-and-forward directories have been created,
        // now create the standby side directories as needed.
        this.standbyFs = getFileSystem(standbyUrl);
        Path standbyLogDir = new Path(standbyUrl.getPath());
        if (!standbyFs.exists(standbyLogDir)) {
            LOG.info("Creating directory {}", standbyUrlString);
            if (!standbyFs.mkdirs(standbyLogDir)) {
                throw new IOException("Failed to create directory: " + standbyUrlString);
            }
        }
    }

    /** Gets a FileSystem instance for the given URI using the current configuration. */
    protected FileSystem getFileSystem(URI uri) throws IOException {
        return FileSystem.get(uri, conf);
    }

    /** Calculates the interval for checking log rotation based on the configured rotation time. */
    protected long getRotationCheckInterval(long rotationTimeMs) {
        long interval;
        if (rotationTimeMs > 0) {
            interval = rotationTimeMs / 4;
        } else {
            // If rotation time is not configured or invalid, use a sensible default like 10 seconds
            interval = 10000L;
        }
        return interval;
    }

    /** Starts the background task for time-based log rotation. */
    protected void startRotationExecutor() {
        Preconditions.checkState(rotationExecutor == null, "Rotation executor already started");
        rotationExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
            .setNameFormat("ReplicationLogRotator-%d").setDaemon(true).build());
        long interval = getRotationCheckInterval(rotationTimeMs);
        rotationExecutor.scheduleWithFixedDelay(new LogRotationTask(), interval, interval,
            TimeUnit.MILLISECONDS);
    }

    /** Stops the background task for time-based log rotation. */
    protected void stopRotationExecutor() {
        if (rotationExecutor != null) {
            rotationExecutor.shutdownNow();
            rotationExecutor = null;
        }
    }

    /** Gets the current writer, rotating it if necessary based on size thresholds. */
    protected LogFileWriter getWriter() throws IOException {
        lock.lock();
        try {
            if (shouldRotate()) {
                rotateLog(RotationReason.SIZE);
            }
            return currentWriter;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Checks if the current log file needs to be rotated based on time or size. Must be called
     * under lock.
     * @return true if rotation is needed, false otherwise.
     * @throws IOException If an error occurs checking the file size.
     */
    protected boolean shouldRotate() throws IOException {
        if (currentWriter == null) {
            LOG.warn("Current writer is null, forcing rotation.");
            return true;
        }
        // Check time threshold
        long now = EnvironmentEdgeManager.currentTimeMillis();
        long last = lastRotationTime.get();
        if (now - last >= rotationTimeMs) {
            LOG.debug("Rotating log file due to time threshold ({} ms elapsed, threshold {} ms)",
                now - last, rotationTimeMs);
            return true;
        }

        // Check size threshold (using actual file size for accuracy)
        long currentSize = currentWriter.getLength();
        if (currentSize >= rotationSizeBytes) {
            LOG.debug("Rotating log file due to size threshold ({} bytes, threshold {} bytes)",
                currentSize, rotationSizeBytes);
            return true;
        }

        return false;
    }

    /**
     * Closes the current log writer and opens a new one, updating rotation metrics.
     * <p>
     * This method handles the rotation of log files, which can be triggered by:
     * <ul>
     *   <li>Time threshold exceeded (TIME)</li>
     *   <li>Size threshold exceeded (SIZE)</li>
     *   <li>Error condition requiring rotation (ERROR)</li>
     * </ul>
     * <p>
     * The method implements retry logic for handling rotation failures.  If rotation fails, it
     * retries up to maxRotationRetries times. If the number of failures exceeds
     * maxRotationRetries, an exception is thrown. Otherwise, it logs a warning and continues with
     * the current writer.
     * <p>
     * The method is thread-safe and uses a lock to ensure atomic rotation operations.
     *
     * @param reason The reason for requesting log rotation
     * @return The new LogFileWriter instance if rotation succeeded, or the current writer if
     * rotation failed
     * @throws IOException if rotation fails after exceeding maxRotationRetries
     */
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "UL_UNRELEASED_LOCK",
        justification = "False positive")
    protected LogFileWriter rotateLog(RotationReason reason) throws IOException {
        lock.lock();
        try {
            // Try to get the new writer first. If it fails we continue using the current writer.
            // Increment the writer generation
            LogFileWriter newWriter = createNewWriter(standbyFs, standbyUrl);
            LOG.debug("Created new writer: {}", newWriter);
            // Close the current writer
            if (currentWriter != null) {
                LOG.debug("Closing current writer: {}", currentWriter);
                closeWriter(currentWriter);
            }
            currentWriter = newWriter;
            lastRotationTime.set(EnvironmentEdgeManager.currentTimeMillis());
            rotationFailures.set(0);
            metrics.incrementRotationCount();
            switch (reason) {
            case TIME:
                metrics.incrementTimeBasedRotationCount();
                break;
            case SIZE:
                metrics.incrementSizeBasedRotationCount();
                break;
            case ERROR:
                metrics.incrementErrorBasedRotationCount();
                break;
            }
        } catch (IOException e) {
            // If we fail to rotate the log, we increment the failure counter. If we have exceeded
            // the maximum number of retries, we close the log and throw the exception. Otherwise
            // we log a warning and continue.
            metrics.incrementRotationFailureCount();
            long numFailures = rotationFailures.getAndIncrement();
            if (numFailures >= maxRotationRetries) {
                LOG.warn("Failed to rotate log (attempt {}/{}), closing log", numFailures,
                    maxRotationRetries, e);
                closeOnError();
                throw e;
            }
            LOG.warn("Failed to rotate log (attempt {}/{}), retrying...", numFailures,
                maxRotationRetries, e);
        } finally {
            lock.unlock();
        }
        return currentWriter;
    }

    /**
     * Creates a new log file path in a sharded directory structure based on server name and
     * timestamp.
     */
    protected Path makeWriterPath(FileSystem fs, URI url) throws IOException {
        long timestamp = EnvironmentEdgeManager.currentTimeMillis();
        // To have all logs for a given regionserver appear in the same shard, hash only the
        // serverName. However we expect some regionservers will have significantly more load than
        // others so we instead distribute the logs over all of the shards randomly for a more even
        // overall distribution by also hashing the timestamp.
        int shard = (serverName.hashCode() ^ Long.hashCode(timestamp)) % numShards;
        Path shardPath = new Path(url.getPath(), String.format(SHARD_DIR_FORMAT, shard));
        // Ensure the shard directory exists. We track which shard directories we have probed or
        // created to avoid a round trip to the namenode for repeats.
        IOException[] exception = new IOException[1];
        shardMap.computeIfAbsent(shardPath, p -> {
            try {
                if (!fs.exists(p)) {
                    if (!fs.mkdirs(p)) {
                        throw new IOException("Could not create path: " + p);
                    }
                }
            } catch (IOException e) {
                exception[0] = e;
            }
            return p;
        });
        // If we faced an exception in computeIfAbsent, throw it
        if (exception[0] != null) {
            throw exception[0];
        }
        Path filePath = new Path(shardPath, String.format(FILE_NAME_FORMAT, timestamp, serverName));
        return filePath;
    }

    /** Creates and initializes a new LogFileWriter for the given filesystem and URL. */
    protected LogFileWriter createNewWriter(FileSystem fs, URI url) throws IOException {
        Path filePath = makeWriterPath(fs, url);
        LogFileWriterContext writerContext = new LogFileWriterContext(conf).setFileSystem(fs)
            .setFilePath(filePath).setCompression(compression);
        LogFileWriter newWriter = new LogFileWriter();
        try {
            newWriter.init(writerContext);
            newWriter.setGeneration(writerGeneration.incrementAndGet());
        } catch (IOException e) {
            LOG.error("Failed to initialize new LogFileWriter for path {}", filePath, e);
            throw e;
        }
        return newWriter;
    }

    /** Closes the given writer, logging any errors that occur during close. */
    protected void closeWriter(LogFileWriter writer) {
        if (writer == null) {
            return;
        }
        try {
            writer.close();
        } catch (IOException e) {
            // For now, just log and continue
            LOG.error("Error closing log writer: " + writer, e);
        }
    }

    /** Force closes the log upon an unrecoverable internal error. */
    protected void closeOnError() {
        lock.lock();
        try {
            if (isClosed) {
                return;
            }
            isClosed = true;
        } finally {
            lock.unlock();
        }
        // Stop the time based rotation check.
        stopRotationExecutor();
        // We expect a final sync will not work. Just close the inner writer.
        closeWriter(currentWriter);
        // Directly halt the disruptor. shutdown() would wait for events to drain. We are expecting
        // that will not work.
        disruptor.halt();
    }

    /** Closes the log. */
    public void close() {
        lock.lock();
        try {
            if (isClosed) {
                return;
            }
            isClosed = true;
        } finally {
            lock.unlock();
        }
        // Stop the time based rotation check.
        stopRotationExecutor();
        // Sync before shutting down to flush all pending appends.
        try {
            syncInternal();
            disruptor.shutdown(); // Wait for a clean shutdown.
        } catch (IOException e) {
            LOG.warn("Error during final sync on close", e);
            disruptor.halt(); // Go directly to halt.
        }
        // We must for the disruptor before closing the current writer.
        closeWriter(currentWriter);
    }

    /** Implements time based rotation independent of in-line checking. */
    protected class LogRotationTask implements Runnable {
        @Override
        public void run() {
            if (isClosed) {
                return;
            }
            // Use tryLock with a timeout to avoid blocking indefinitely if another thread holds
            // the lock for an unexpectedly long time (e.g., during a problematic rotation).
            boolean acquired = false;
            try {
                // Wait a short time for the lock
                acquired = lock.tryLock(1, TimeUnit.SECONDS);
                if (acquired) {
                    // Check only the time condition here, size is handled by getWriter
                    long now = EnvironmentEdgeManager.currentTimeMillis();
                    long last = lastRotationTime.get();
                    if (!isClosed && now - last >= rotationTimeMs) {
                        LOG.debug("Time based rotation needed ({} ms elapsed, threshold {} ms).",
                              now - last, rotationTimeMs);
                        try {
                            rotateLog(RotationReason.TIME); // rotateLog updates lastRotationTime
                        } catch (IOException e) {
                            LOG.error("Failed to rotate log, currentWriter is {}", currentWriter,
                                e);
                            // More robust error handling goes here once the store-and-forward
                            // fallback is implemented. For now we just log the error and continue.
                        }
                    }
                } else {
                    LOG.warn("LogRotationTask could not acquire lock, skipping check this time.");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // Preserve interrupt status
                LOG.warn("LogRotationTask interrupted while trying to acquire lock.");
            } finally {
                if (acquired) {
                    lock.unlock();
                }
            }
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

    /** Event structure for the Disruptor ring buffer containing data and sync operations. */
    protected static class LogEvent {
        protected static final EventFactory<LogEvent> EVENT_FACTORY = LogEvent::new;

        protected int type;
        protected Record record;
        protected CompletableFuture<Void> syncFuture; // Used only for SYNC events
        protected long timestampNs; // Timestamp when event was created

        public void setValues(int type, Record record, CompletableFuture<Void> syncFuture) {
            this.type = type;
            this.record = record;
            this.syncFuture = syncFuture;
            this.timestampNs = System.nanoTime();
        }
    }

    /**
     * Handles events from the Disruptor, managing batching, writer rotation, and error handling.
     */
    protected class LogEventHandler implements EventHandler<LogEvent> {
        protected final int maxRetries; // Configurable max retries for sync
        protected final List<Record> currentBatch = new ArrayList<>();
        protected final List<CompletableFuture<Void>> pendingSyncFutures = new ArrayList<>();
        protected LogFileWriter writer;
        protected long generation;

        protected LogEventHandler() {
            this.maxRetries = conf.getInt(REPLICATION_LOG_SYNC_RETRIES_KEY,
                DEFAULT_REPLICATION_LOG_SYNC_RETRIES);
        }

        protected void init() throws IOException {
            this.writer = getWriter();
            this.generation = writer.getGeneration();
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
         *
         * @param sequence The sequence number of the last processed event
         * @throws IOException if the sync operation fails
         */
        protected void processPendingSyncs(long sequence) throws IOException {
            if (pendingSyncFutures.isEmpty()) {
              return;
            }
            writer.sync();
            // Complete all pending sync futures
            for (CompletableFuture<Void> future : pendingSyncFutures) {
                future.complete(null);
            }
            pendingSyncFutures.clear();
            // Sync completed, clear the list of in-flight appends.
            currentBatch.clear();
            LOG.trace("Sync operation completed successfully up to sequence {}", sequence);
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
        protected void failPendingSyncs(long sequence, IOException e) {
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
         * Processes a single event from the Disruptor ring buffer. This method handles both data
         * and sync events, with retry logic for handling IO failures.
         * <p>
         * For data events, it:
         * <ol>
         *   <li>Checks if the writer has been rotated and replays any in-flight records.</li>
         *   <li>Appends the record to the current writer.</li>
         *   <li>Adds the record to the current batch for potential replay.</li>
         *   <li>Processes any pending syncs if this is the end of a batch.</li>
         * </ol>
         * <p>
         * For sync events, it:
         * <ol>
         *   <li>Adds the sync future to the pending list.</li>
         *   <li>Processes any pending syncs if this is the end of a batch.</li>
         * </ol>
         * If an IOException occurs, the method will attempt to rotate the writer and retry the
         * operation up to the configured maximum number of retries. If all retries fail, it will
         * fail all pending syncs and throw the exception.
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
            writer = getWriter();
            int attempt = 0;
            while (attempt < maxRetries) {
                try {
                    if (writer.getGeneration() > generation) {
                        generation = writer.getGeneration();
                        // If the writer has been rotated, we need to replay the current batch of
                        // in-flight appends into the new writer.
                        if (!currentBatch.isEmpty()) {
                            LOG.trace("Writer has been rotated, replaying in-flight batch");
                            for (Record r: currentBatch) {
                                writer.append(r.tableName,  r.commitId,  r.mutation);
                            }
                        }
                    }
                    switch (event.type) {
                    case EVENT_TYPE_DATA:
                        writer.append(event.record.tableName, event.record.commitId,
                            event.record.mutation);
                        // Add to current batch only after we succeed at appending, so we don't
                        // replay it twice.
                        currentBatch.add(event.record);
                        // Process any pending syncs at the end of batch.
                        if (endOfBatch) {
                            processPendingSyncs(sequence);
                        }
                        return;
                    case EVENT_TYPE_SYNC:
                        // Add this sync future to the pending list.
                        pendingSyncFutures.add(event.syncFuture);
                        // Process any pending syncs at the end of batch.
                        if (endOfBatch) {
                            processPendingSyncs(sequence);
                        }
                        return;
                    default:
                        throw new UnsupportedOperationException("Unknown event type: "
                            + event.type);
                    }
                } catch (IOException e) {
                    // IO exception, force a rotation.
                    LOG.debug("Attempt " + (attempt + 1) + "/" + maxRetries + " failed", e);
                    attempt++;
                    if (attempt > maxRetries) {
                        failPendingSyncs(sequence, e);
                        throw e;
                    }
                    writer = rotateLog(RotationReason.ERROR);
                }
            }
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
