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
 * TODO: This class will switch between active (synchronous) and store-and-forward fallback
 * writer depending on error handling/retry strategy.
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = { "EI_EXPOSE_REP", "EI_EXPOSE_REP2" },
    justification = "Intentional")
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

    private static final String SHARD_DIR_FORMAT = "shard%05d";
    private static final String FILE_NAME_FORMAT = "%d-%s.plog";

    private static final byte EVENT_TYPE_DATA = 0;
    private static final byte EVENT_TYPE_SYNC = 1;

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationLog.class);

    private static volatile ReplicationLog instance;

    private final Configuration conf;
    private final ServerName serverName;
    protected FileSystem standbyFs;
    protected FileSystem fallbackFs; // For store-and-forward (future use)
    protected int numShards;
    protected URI standbyUrl;
    protected URI fallbackUrl; // For store-and-forward (future use)
    private final long rotationTimeMs;
    private final long rotationSizeBytes;
    private final Compression.Algorithm compression;
    private final ReentrantLock lock = new ReentrantLock();
    protected volatile LogFileWriter currentWriter; // Current writer
    protected final AtomicLong lastRotationTime = new AtomicLong();
    protected final AtomicLong writerGeneration = new AtomicLong();
    private ScheduledExecutorService rotationExecutor;
    private final int ringBufferSize;
    private final long syncTimeoutMs;
    private Disruptor<LogEvent> disruptor;
    private RingBuffer<LogEvent> ringBuffer;
    private final ConcurrentHashMap<Path,Object> shardMap = new ConcurrentHashMap<>();

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
    }

    @SuppressWarnings("unchecked")
    public void init() throws IOException {
        if (numShards > MAX_REPLICATION_NUM_SHARDS) {
            throw new IllegalArgumentException(REPLICATION_NUM_SHARDS_KEY + " is " + numShards
                + ", but the limit is " + MAX_REPLICATION_NUM_SHARDS);
        }
        initializeFileSystems();
        // Initialize the Disruptor. We use ProducerType.MULTI because multiple handlers might
        // call append concurrently. We use YieldingWaitStrategy for low latency. When the ring
        // buffer is full (controlled by REPLICATION_WRITER_RINGBUFFER_SIZE_KEY), producers
        // calling ringBuffer.next() will effectively block (by yielding/spinning), creating
        // backpressure on the callers. This ensures appends don't proceed until there is space.
        disruptor = new Disruptor<>(LogEvent.EVENT_FACTORY, ringBufferSize,
            new ThreadFactoryBuilder().setNameFormat("ReplicationLogEventHandler-%d")
                .setDaemon(true).build(),
            ProducerType.MULTI, new YieldingWaitStrategy());
        disruptor.handleEventsWith(new LogEventHandler());
        disruptor.setDefaultExceptionHandler(new LogExceptionHandler());
        ringBuffer = disruptor.start();
        LOG.info("ReplicationLogWriter started with ring buffer size {}", ringBufferSize);
        // For now we just initialize currentWriter
        currentWriter = createNewWriter(standbyFs, standbyUrl);
        lastRotationTime.set(EnvironmentEdgeManager.currentTimeMillis());
        startRotationExecutor();
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
        if (currentWriter == null) {
            throw new IOException("Closed");
        }
        // ringBuffer.next() claims the next sequence number. Because we initialize the Disruptor
        // with ProducerType.MULTI and the blocking YieldingWaitStrategy this call WILL BLOCK if
        // the ring buffer is full, thus providing backpressure to the callers.
        long sequence = ringBuffer.next();
        try {
            LogEvent event = ringBuffer.get(sequence);
            event.setValues(EVENT_TYPE_DATA, new Record(tableName, commitId, mutation), null, sequence);
        } finally {
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
        if (currentWriter == null) {
            throw new IOException("Closed");
        }
        CompletableFuture<Void> syncFuture = new CompletableFuture<>();
        long sequence;
        sequence = ringBuffer.next();
        try {
            LogEvent event = ringBuffer.get(sequence);
            // Publish a special SYNC event
            event.setValues(EVENT_TYPE_SYNC, null, syncFuture, sequence);
        } finally {
            ringBuffer.publish(sequence);
        }
        LOG.trace("Published EVENT_TYPE_SYNC at sequence {}", sequence);
        try {
            // Wait for the event handler to process up to and including this sync event
            syncFuture.get(syncTimeoutMs, TimeUnit.MILLISECONDS);
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
            this.fallbackFs = FileSystem.get(fallbackUrl, conf);
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
        this.standbyFs = FileSystem.get(standbyUrl, conf);
        Path standbyLogDir = new Path(standbyUrl.getPath());
        if (!standbyFs.exists(standbyLogDir)) {
            LOG.info("Creating directory {}", standbyUrlString);
            if (!standbyFs.mkdirs(standbyLogDir)) {
                throw new IOException("Failed to create directory: " + standbyUrlString);
            }
        }
    }

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

    // Starts the background task for time-based log rotation
    protected void startRotationExecutor() {
        Preconditions.checkState(rotationExecutor == null, "Rotation executor already started");
        rotationExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
            .setNameFormat("ReplicationLogRotator-%d").setDaemon(true).build());
        long interval = getRotationCheckInterval(rotationTimeMs);
        rotationExecutor.scheduleWithFixedDelay(new LogRotationTask(), interval, interval,
            TimeUnit.MILLISECONDS);
    }

    // Stop the background task for time-based log rotation
    protected void stopRotationExecutor() {
        if (rotationExecutor != null) {
            rotationExecutor.shutdownNow();
            rotationExecutor = null;
        }
    }

    protected LogFileWriter getWriter() throws IOException {
        lock.lock();
        try {
            if (shouldRotate()) {
                rotateLog();
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
     * Closes the current log writer and opens a new one.
     * @throws IOException If closing the old writer or creating the new one fails.
     */
    protected LogFileWriter rotateLog() throws IOException {
        lock.lock(); // Lock is reentrant, so this is fine.
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
            return currentWriter;
        } catch (IOException e) {
            LOG.warn("Exception while attempting to rotate the log writer", e);
            throw e; // Rethrow
        } finally {
            // Update the last rotation time no matter what. We will try again next time. We do
            // this so we are not constantly trying to rotate the log instead of making progress
            // with the existing writer when there is some hopefully transient issue during
            // rolling.
            // TODO: Escalate error response after some consecutive number of failed rolls.
            lastRotationTime.set(EnvironmentEdgeManager.currentTimeMillis());
            lock.unlock();
        }
    }

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

    /**
     * Creates and initializes a new LogFileWriter.
     * @param fs  The FileSystem to write to.
     * @param url A URI pointing to the root log directory.
     * @return The newly created and initialized LogFileWriter.
     * @throws IOException If an error occurs during file creation or writer initialization.
     */
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
        // Stop the time based rotation check.
        stopRotationExecutor();
        // We expect a final sync will not work. Just close the inner writer.
        if (currentWriter != null) {
            closeWriter(currentWriter);
            currentWriter = null;
        }
        // Directly halt the disruptor. shutdown() would wait for events to drain. We are expecting
        // that will not work.
        disruptor.halt();
    }

    /** Closes the log. */
    public void close() {
        // Stop the time based rotation check.
        stopRotationExecutor();
        // We use 'currentWriter' to determine if we have already gracefully closed the current
        // writer.
        if (currentWriter != null) {
            // Sync before shutting down to flush all pending appends.
            try {
                sync();
                disruptor.shutdown(); // Wait for a clean shutdown.
            } catch (IOException e) {
                LOG.warn("Error during final sync on close", e);
                disruptor.halt(); // Go directly to halt.
            }
            closeWriter(currentWriter);
            currentWriter = null;
        }
    }

    /** Implements time based rotation independent of in-line checking during append(). */
    protected class LogRotationTask implements Runnable {
        @Override
        public void run() {
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
                    if (currentWriter != null && now - last >= rotationTimeMs) {
                        LOG.debug("Time based rotation needed ({} ms elapsed, threshold {} ms).",
                              now - last, rotationTimeMs);
                        try {
                            rotateLog(); // rotateLog updates lastRotationTime
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

    // Event structure for the Disruptor ring buffer
    protected static class LogEvent {
        public static final EventFactory<LogEvent> EVENT_FACTORY = LogEvent::new;

        public byte type;
        public Record record;
        public CompletableFuture<Void> syncFuture; // Used only for SYNC events
        public long sequence; // Sequence number for tracking

        public void setValues(byte type, Record record, CompletableFuture<Void> syncFuture,
                long sequence) {
            this.type = type;
            this.record = record;
            this.syncFuture = syncFuture;
            this.sequence = sequence;
        }
    }

    // Handles events from the Disruptor
    protected class LogEventHandler implements EventHandler<LogEvent> {
        private final int maxRetries; // Configurable max retries for sync
        private final List<Record> currentBatch = new ArrayList<>();

        LogEventHandler() {
            this.maxRetries = conf.getInt(REPLICATION_LOG_SYNC_RETRIES_KEY,
                DEFAULT_REPLICATION_LOG_SYNC_RETRIES);
        }

        @SuppressWarnings("resource")
        @Override
        public void onEvent(LogEvent event, long sequence, boolean endOfBatch) throws Exception {
            CompletableFuture<Void> futureToComplete = null;
            int attempt = 0;
            LogFileWriter writer = getWriter();
            long generation = writer.getGeneration();
            while (attempt < maxRetries) {
                try {
                    // If the writer has been rotated, we need to replay the current batch of
                    // in-flight appends into the new writer.
                    if (writer.getGeneration() > generation) {
                        generation = writer.getGeneration();
                        for (Record r: currentBatch) {
                            writer.append(r.tableName,  r.commitId,  r.mutation);
                        }
                    }
                    switch (event.type) {
                    case EVENT_TYPE_DATA:
                        writer.append(event.record.tableName, event.record.commitId,
                            event.record.mutation);
                        // Add to current batch only after we succeed at appending, so we don't
                        // replay it twice.
                        currentBatch.add(event.record);
                        return;
                    case EVENT_TYPE_SYNC:
                        futureToComplete = event.syncFuture;
                        writer.sync();
                        // Sync completed, clear the list of in-flight appends.
                        currentBatch.clear();
                        futureToComplete.complete(null);
                        futureToComplete = null;
                        LOG.trace("Sync operation completed successfully up to sequence {}",
                            event.sequence);
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
                        // Throw if we have exceeded our allowable retries.
                        throw e;
                    }
                    writer = rotateLog();
                }
            }
        }
    }

    /**
     * Handler for critical errors during the Disruptor lifecycle. We handle such errors by
     * immediately closing the writer. This prevents Disruptor level issues from causing silent
     * data loss. Users of the writer will immediately notice it is closed by the resulting
     * IOExceptions and can take whatever steps are necessary to recover (or abort).
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
