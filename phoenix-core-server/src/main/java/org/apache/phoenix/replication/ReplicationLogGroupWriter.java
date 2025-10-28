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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.replication.log.LogFileWriter;
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
 * Base class for replication log group writers.
 * <p>
 * This abstract class contains most of the common functionality for managing replication logs
 * including the disruptor ring buffer, log rotation, file system management, and metrics.
 * Concrete implementations provide specific replication behavior (synchronous vs store-and-
 * forward).
 * <p>
 * Architecture Overview:
 * <pre>
 * ┌──────────────────────────────────────────────────────────────────────┐
 * │                       ReplicationLogGroup                            │
 * │                                                                      │
 * │  ┌─────────────┐     ┌────────────────────────────────────────────┐  │
 * │  │             │     │                                            │  │
 * │  │  Producers  │     │  Disruptor Ring Buffer                     │  │
 * │  │  (append/   │────▶│  ┌─────────┐ ┌─────────┐ ┌─────────┐       │  │
 * │  │   sync)     │     │  │ Event 1 │ │ Event 2 │ │ Event 3 │ ...   │  │
 * │  │             │     │  └─────────┘ └─────────┘ └─────────┘       │  │
 * │  └─────────────┘     └────────────────────────────────────────────┘  │
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
 * A high-performance ring buffer decouples the API from the complexity of writer management.
 * Callers of append and sync generally return quickly, except for sync, where the writer must
 * suspend the caller until the sync operation is successful (or times out). An internal single
 * threaded process handles these events, encapsulating the complexity of batching mutations for
 * efficiency, consolidating multiple in-flight syncs, rotating writers based on time or size,
 * error handling and retries, and mode transitions.
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = { "EI_EXPOSE_REP", "EI_EXPOSE_REP2",
    "MS_EXPOSE_REP" }, justification = "Intentional")
public abstract class ReplicationLogGroupWriter {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationLogGroupWriter.class);

    protected final ReplicationLogGroup logGroup;
    protected final long rotationTimeMs;
    protected final long rotationSizeBytes;
    protected final int maxRotationRetries;
    protected final Compression.Algorithm compression;
    protected final int ringBufferSize;
    protected final long syncTimeoutMs;
    protected final ReentrantLock lock = new ReentrantLock();
    protected volatile LogFileWriter currentWriter;
    protected final AtomicLong lastRotationTime = new AtomicLong();
    protected final AtomicLong writerGeneration = new AtomicLong();
    protected final AtomicLong rotationFailures = new AtomicLong(0);
    protected ScheduledExecutorService rotationExecutor;
    protected Disruptor<LogEvent> disruptor;
    protected RingBuffer<LogEvent> ringBuffer;
    protected volatile boolean closed = false;

    /** The reason for requesting a log rotation. */
    protected enum RotationReason {
        /** Rotation requested due to time threshold being exceeded. */
        TIME,
        /** Rotation requested due to size threshold being exceeded. */
        SIZE,
        /** Rotation requested due to an error condition. */
        ERROR;
    }

    protected static final byte EVENT_TYPE_DATA = 0;
    protected static final byte EVENT_TYPE_SYNC = 1;

    protected ReplicationLogGroupWriter(ReplicationLogGroup logGroup) {
        this.logGroup = logGroup;
        Configuration conf = logGroup.getConfiguration();
        this.rotationTimeMs =
            conf.getLong(QueryServices.REPLICATION_LOG_ROTATION_TIME_MS_KEY,
                QueryServicesOptions.DEFAULT_REPLICATION_LOG_ROTATION_TIME_MS);
        long rotationSize =
            conf.getLong(ReplicationLogGroup.REPLICATION_LOG_ROTATION_SIZE_BYTES_KEY,
                ReplicationLogGroup.DEFAULT_REPLICATION_LOG_ROTATION_SIZE_BYTES);
        double rotationSizePercent =
            conf.getDouble(ReplicationLogGroup.REPLICATION_LOG_ROTATION_SIZE_PERCENTAGE_KEY,
                ReplicationLogGroup.DEFAULT_REPLICATION_LOG_ROTATION_SIZE_PERCENTAGE);
        this.rotationSizeBytes = (long) (rotationSize * rotationSizePercent);
        this.maxRotationRetries =
            conf.getInt(ReplicationLogGroup.REPLICATION_LOG_ROTATION_RETRIES_KEY,
                ReplicationLogGroup.DEFAULT_REPLICATION_LOG_ROTATION_RETRIES);
        String compressionName =
            conf.get(ReplicationLogGroup.REPLICATION_LOG_COMPRESSION_ALGORITHM_KEY,
                ReplicationLogGroup.DEFAULT_REPLICATION_LOG_COMPRESSION_ALGORITHM);
        Compression.Algorithm compression = Compression.Algorithm.NONE;
        if (!compressionName.equals(
                ReplicationLogGroup.DEFAULT_REPLICATION_LOG_COMPRESSION_ALGORITHM)) {
            try {
                compression = Compression.getCompressionAlgorithmByName(compressionName);
            } catch (IllegalArgumentException e) {
                LOG.warn("Unknown compression type " + compressionName + ", using NONE", e);
            }
        }
        this.compression = compression;
        this.ringBufferSize = conf.getInt(ReplicationLogGroup.REPLICATION_LOG_RINGBUFFER_SIZE_KEY,
            ReplicationLogGroup.DEFAULT_REPLICATION_LOG_RINGBUFFER_SIZE);
        this.syncTimeoutMs = conf.getLong(ReplicationLogGroup.REPLICATION_LOG_SYNC_TIMEOUT_KEY,
            ReplicationLogGroup.DEFAULT_REPLICATION_LOG_SYNC_TIMEOUT);
    }

    /** Initialize the writer. */
    public void init() throws IOException {
        initializeFileSystems();
        // Start time based rotation.
        lastRotationTime.set(EnvironmentEdgeManager.currentTimeMillis());
        startRotationExecutor();
        // Create the initial writer. Do this before we initialize the Disruptor.
        currentWriter = createNewWriter();
        initializeDisruptor();
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
    }

    /**
     * Ensures all previously appended records are durably persisted. This method blocks until the
     * sync operation completes or fails, potentially after internal retries. All in flight appends
     * are batched and provided to the underlying LogWriter, which will then be synced. If there is
     * a problem syncing the LogWriter we will retry, up to the retry limit, rolling the writer for
     * each retry.
     * <p>
     * An internal error may trigger fail-stop behavior. Subsequent to fail-stop, this method will
     * throw an IOException("Closed"). No further syncs are allowed.
     * <p>
     * NOTE: When the ReplicationLogManager is capable of switching between synchronous and
     * fallback (store-and-forward) writers, then this will be pretty bullet proof. Right now we
     * will still try to roll the synchronous writer a few times before giving up.
     * @throws IOException If the sync operation fails after retries, or if interrupted.
     */
    public void sync() throws IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Sync");
        }
        if (closed) {
            throw new IOException("Closed");
        }
        syncInternal();
    }

    /** Initialize file systems needed by this writer implementation. */
    protected abstract void initializeFileSystems() throws IOException;

    /**
     * Create a new log writer for rotation.
     */
    protected abstract LogFileWriter createNewWriter() throws IOException;

    /** Initialize the Disruptor. */
    @SuppressWarnings("unchecked")
    protected void initializeDisruptor() throws IOException {
        disruptor = new Disruptor<>(LogEvent.EVENT_FACTORY, ringBufferSize,
            new ThreadFactoryBuilder()
                .setNameFormat("ReplicationLogGroupWriter-" + logGroup.getHaGroupName() + "-%d")
                .setDaemon(true).build(),
            ProducerType.MULTI, new YieldingWaitStrategy());
        LogEventHandler eventHandler = new LogEventHandler();
        eventHandler.init();
        disruptor.handleEventsWith(eventHandler);
        LogExceptionHandler exceptionHandler = new LogExceptionHandler();
        disruptor.setDefaultExceptionHandler(exceptionHandler);
        ringBuffer = disruptor.start();
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

    protected void startRotationExecutor() {
        long rotationCheckInterval = getRotationCheckInterval(rotationTimeMs);
        rotationExecutor = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat("ReplicationLogRotation-" + logGroup.getHaGroupName() + "-%d")
                .setDaemon(true).build());
        rotationExecutor.scheduleAtFixedRate(new LogRotationTask(), rotationCheckInterval,
            rotationCheckInterval, TimeUnit.MILLISECONDS);
        LOG.debug("Started rotation executor with interval {}ms", rotationCheckInterval);
    }

    protected long getRotationCheckInterval(long rotationTimeMs) {
        long interval = Math.max(10 * 1000L, Math.min(60 * 1000L, rotationTimeMs / 10));
        return interval;
    }

    protected void stopRotationExecutor() {
        if (rotationExecutor != null) {
            rotationExecutor.shutdown();
            try {
                if (!rotationExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    rotationExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                rotationExecutor.shutdownNow();
            }
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
            LogFileWriter newWriter = createNewWriter();
            LOG.debug("Created new writer: {}", newWriter);
            // Close the current writer
            if (currentWriter != null) {
                LOG.debug("Closing current writer: {}", currentWriter);
                closeWriter(currentWriter);
            }
            currentWriter = newWriter;
            lastRotationTime.set(EnvironmentEdgeManager.currentTimeMillis());
            rotationFailures.set(0);
            logGroup.getMetrics().incrementRotationCount();
            switch (reason) {
            case TIME:
                logGroup.getMetrics().incrementTimeBasedRotationCount();
                break;
            case SIZE:
                logGroup.getMetrics().incrementSizeBasedRotationCount();
                break;
            case ERROR:
                logGroup.getMetrics().incrementErrorBasedRotationCount();
                break;
            }
        } catch (IOException e) {
            // If we fail to rotate the log, we increment the failure counter. If we have exceeded
            // the maximum number of retries, we close the log and throw the exception. Otherwise
            // we log a warning and continue.
            logGroup.getMetrics().incrementRotationFailureCount();
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

    /** Close the currentWriter.
     * Needed by tests so that we can close the log file and then read it
     */
    protected void closeCurrentWriter() {
        lock.lock();
        try {
            closeWriter(currentWriter);
            currentWriter = null;
        } finally {
            lock.unlock();
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
     * Force closes the log upon an unrecoverable internal error. This is a fail-stop behavior:
     * once called, the log is marked as closed, the Disruptor is halted, and all subsequent
     * append() and sync() calls will throw an IOException("Closed"). This ensures that no
     * further operations are attempted on a log that has encountered a critical error.
     */
    protected void closeOnError() {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
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
            if (closed) {
                return;
            }
            closed = true;
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

    protected FileSystem getFileSystem(URI uri) throws IOException {
        return FileSystem.get(uri, logGroup.getConfiguration());
    }

    /** Implements time based rotation independent of in-line checking. */
    protected class LogRotationTask implements Runnable {
        @Override
        public void run() {
            if (closed) {
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
                    if (!closed && now - last >= rotationTimeMs) {
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
        protected final int maxRetries;    // Configurable max retries for sync
        protected final long retryDelayMs; // Configurable delay between retries
        protected final List<Record> currentBatch = new ArrayList<>();
        protected final List<CompletableFuture<Void>> pendingSyncFutures = new ArrayList<>();
        protected LogFileWriter writer;
        protected long generation;

        protected LogEventHandler() {
            Configuration conf = logGroup.getConfiguration();
            this.maxRetries = conf.getInt(ReplicationLogGroup.REPLICATION_LOG_SYNC_RETRIES_KEY,
                ReplicationLogGroup.DEFAULT_REPLICATION_LOG_SYNC_RETRIES);
            this.retryDelayMs =
                conf.getLong(ReplicationLogGroup.REPLICATION_LOG_RETRY_DELAY_MS_KEY,
                    ReplicationLogGroup.DEFAULT_REPLICATION_LOG_RETRY_DELAY_MS);
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
         * <p>
         * The retry logic includes a configurable delay between attempts to prevent tight loops
         * when there are persistent HDFS issues. This delay helps mitigate the risk of rapid
         * cycling through writers when the underlying storage system is experiencing problems.
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
            logGroup.getMetrics().updateRingBufferTime(ringBufferTimeNs);
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
                    if (attempt >= maxRetries) {
                        failPendingSyncs(sequence, e);
                        throw e;
                    }
                    attempt++;
                    // Add delay before retrying to prevent tight loops
                    try {
                        Thread.sleep(retryDelayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new InterruptedIOException("Interrupted during retry delay");
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
