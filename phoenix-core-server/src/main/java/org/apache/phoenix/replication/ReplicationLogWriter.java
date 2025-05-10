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

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.phoenix.replication.log.LogFile;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
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
 * Responsible for writing replication log records using a Disruptor for high throughput and low
 * latency appends. It obtains the current active {@link LogFileWriter} from the
 * {@link ReplicationLogManager} and appends records to it via a dedicated handler thread. Handles
 * sync failures by rolling the writer and then by retrying the in-flight appends with the new
 * writer.
 * <p>
 * A caller will call append() one or more times to insert mutations into the ring buffer. It will
 * then call sync() to ensure that all of the mutations pending in the ring buffer are committed to
 * the file. This allows for potentially high throughput through batching while also enabling a
 * caller to be certain their append()ed mutations, and potentially others, are synced to the
 * replication log, before continuing.
 */
public class ReplicationLogWriter implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationLogWriter.class);

    public static final String REPLICATION_WRITER_RINGBUFFER_SIZE_KEY =
        "phoenix.replication.writer.ringbuffer.size";
    public static final int DEFAULT_REPLICATION_WRITER_RINGBUFFER_SIZE = 1024 * 32;  // Too big?
    public static final String REPLICATION_WRITER_SYNC_TIMEOUT_KEY =
        "phoenix.replication.writer.sync.timeout.ms";
    public static final long DEFAULT_REPLICATION_WRITER_SYNC_TIMEOUT = 1000 * 30;
    // Note that the total potential time we might spend waiting for a sync is
    // SYNC_TIMEOUT * SYNC_RETRIES + SYNC_RETRY_PAUSE * SYNC_RETRIES-1 ~= 94 seconds.
    public static final String REPLICATION_WRITER_SYNC_RETRIES_KEY =
        "phoenix.replication.writer.sync.retries";
    public static final int DEFAULT_REPLICATION_WRITER_SYNC_RETRIES = 5;
    public static final String REPLICATION_WRITER_SYNC_RETRY_PAUSE_KEY =
        "phoenix.replication.writer.sync.retry.pause.ms";
    public static final long DEFAULT_REPLICATION_WRITER_SYNC_RETRY_PAUSE = 100;

    private static final byte EVENT_TYPE_DATA = 0;
    private static final byte EVENT_TYPE_SYNC = 1;

    private final Configuration conf;
    private final ReplicationLogManager logManager;
    private final int ringBufferSize;
    private final long syncTimeoutMs;
    private Disruptor<LogEvent> disruptor;
    private RingBuffer<LogEvent> ringBuffer;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public ReplicationLogWriter(Configuration conf, ReplicationLogManager logManager) {
        this.conf = conf;
        this.logManager = logManager;
        this.ringBufferSize = conf.getInt(REPLICATION_WRITER_RINGBUFFER_SIZE_KEY,
            DEFAULT_REPLICATION_WRITER_RINGBUFFER_SIZE);
        this.syncTimeoutMs = conf.getLong(REPLICATION_WRITER_SYNC_TIMEOUT_KEY,
            DEFAULT_REPLICATION_WRITER_SYNC_TIMEOUT);
    }

    @SuppressWarnings("unchecked")
    public void init() {
        // Initialize the Disruptor. We use ProducerType.MULTI because multiple handlers might
        // call append concurrently. We use YieldingWaitStrategy for low latency. When the ring
        // buffer is full (controlled by REPLICATION_WRITER_RINGBUFFER_SIZE_KEY), producers
        // calling ringBuffer.next() will effectively block (by yielding/spinning), creating
        // backpressure on the callers. This ensures appends don't proceed until there is space.
        disruptor = new Disruptor<>(LogEvent.EVENT_FACTORY, ringBufferSize,
            new ThreadFactoryBuilder().setNameFormat("ReplicationLogEventHandler-%d")
                .setDaemon(true).build(),
            ProducerType.MULTI, new YieldingWaitStrategy());
        disruptor.handleEventsWith(new LogEventHandler(conf, logManager));
        disruptor.setDefaultExceptionHandler(new LogExceptionHandler());
        ringBuffer = disruptor.start();
        started.set(true);
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
        if (closed.get()) {
            throw new IOException("Closed");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("Append: table={}, commitId={}, mutation={}", tableName, commitId, mutation);
        }
        // ringBuffer.next() claims the next sequence number. Because we initialize the Disruptor
        // with ProducerType.MULTI and the blocking YieldingWaitStrategy this call WILL BLOCK if
        // the ring buffer is full, thus providing backpressure to the callers.
        long sequence = ringBuffer.next();
        try {
            LogEvent event = ringBuffer.get(sequence);
            event.setValues(EVENT_TYPE_DATA, tableName, commitId, mutation, null, sequence);
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
        if (closed.get()) {
            throw new IOException("Closed");
        }
        internalSync();
    }

    protected void internalSync() throws IOException {
        LOG.trace("Sync");
        CompletableFuture<Void> syncFuture = new CompletableFuture<>();
        long sequence;
        sequence = ringBuffer.next();
        try {
            LogEvent event = ringBuffer.get(sequence);
            // Publish a special SYNC event
            event.setValues(EVENT_TYPE_SYNC, null, -1, null, syncFuture, sequence);
        } finally {
            ringBuffer.publish(sequence);
        }

        try {
            // Wait for the event handler to process up to and including this sync event
            syncFuture.get(syncTimeoutMs, TimeUnit.MILLISECONDS);
            LOG.debug("Sync operation completed successfully up to sequence {}", sequence);
        } catch (InterruptedException e) {
            // Almost certainly the regionserver is shutting down or aborting.
            // TODO: Do we need to do more here?
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("Interrupted while waiting for sync");
        } catch (ExecutionException e) {
            LOG.error("Sync operation failed", e.getCause());
            // Let's trigger a regionserver abort here for the convenience of the caller. An
            // alternative would be to let the caller handle it, even perhaps letting the exception
            // propagate to HBase core code.
            logManager.abort("Sync operation failed", e.getCause());
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else {
                throw new IOException("Sync operation failed", e.getCause());
            }
        } catch (TimeoutException e) {
            LOG.error("Sync operation timed out", e);
            // Let's trigger a regionserver abort here for the convenience of the caller. An
            // alternative would be to let the caller handle it, even perhaps letting the exception
            // propagate to HBase core code.
            logManager.abort("Sync operation timed out", e.getCause());
            throw new IOException("Sync operation failed", e);
        }
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            if (started.get()) {
                try {
                    // Sync before shutting down to flush all pending appends.
                    internalSync();
                } catch (IOException e) {
                    LOG.warn("Error during final sync on close", e);
                    logManager.abort("Error during final sync on close", e);
                }
                // Shut down the disruptor gracefully
                try {
                    disruptor.shutdown(1, TimeUnit.MINUTES);
                } catch (com.lmax.disruptor.TimeoutException e) {
                    // Abort the regionserver if we timed out trying to shut down.
                    LOG.error("Timeout during disruptor shutdown", e);
                    logManager.abort("Timeout during disruptor shutdown", e);
                }
            }
            LOG.info("ReplicationLogWriter closed");
        }
    }

    // Event structure for the Disruptor ring buffer
    static class LogEvent {
        public static final EventFactory<LogEvent> EVENT_FACTORY = LogEvent::new;

        private byte type;
        private String tableName;
        private long commitId;
        private Mutation mutation;
        private CompletableFuture<Void> syncFuture; // Used only for SYNC events
        private long sequence; // Sequence number for tracking

        public void setValues(byte type, String tableName, long commitId, Mutation mutation,
              CompletableFuture<Void> syncFuture, long sequence) {
            this.type = type;
            this.tableName = tableName;
            this.commitId = commitId;
            this.mutation = mutation;
            this.syncFuture = syncFuture;
            this.sequence = sequence;
        }
    }

    // Handles events from the Disruptor
    static class LogEventHandler implements EventHandler<LogEvent> {
        private final ReplicationLogManager logManager;
        private LogFile.Writer currentWriter;
        private final List<LogEvent> currentBatch = new ArrayList<>();
        private final int maxRetries; // Configurable max retries for sync
        private final long retryPauseMs; // Configurable pause between retries
        // Lock and condition for coordinating sync completion/failure with the sync() method
        private final ReentrantLock syncLock = new ReentrantLock();
        private CompletableFuture<Void> pendingSyncFuture = null;
        private IOException syncException = null;

        LogEventHandler(Configuration conf, ReplicationLogManager logManager) {
            this.logManager = logManager;
            this.maxRetries = conf.getInt(REPLICATION_WRITER_SYNC_RETRIES_KEY,
                DEFAULT_REPLICATION_WRITER_SYNC_RETRIES);
            this.retryPauseMs = conf.getLong(REPLICATION_WRITER_SYNC_RETRY_PAUSE_KEY,
                DEFAULT_REPLICATION_WRITER_SYNC_RETRY_PAUSE);
        }

        @Override
        public void onEvent(LogEvent event, long sequence, boolean endOfBatch) throws Exception {
            if (event.type == EVENT_TYPE_DATA) {
                currentBatch.add(event);
            } else if (event.type == EVENT_TYPE_SYNC) {
                // Prepare to signal the sync caller
                syncLock.lock();
                try {
                    Preconditions.checkState(pendingSyncFuture == null,
                        "Received concurrent SYNC events");
                    pendingSyncFuture = event.syncFuture;
                    syncException = null; // Reset exception for this sync attempt
                } finally {
                    syncLock.unlock();
                }
            }
            // Process the batch if it's the end of the Disruptor batch OR if a sync is requested
            if (endOfBatch || event.type == EVENT_TYPE_SYNC) {
                processCurrentBatch();
            }
        }

        private void processCurrentBatch() {
            if (currentBatch.isEmpty() && pendingSyncFuture == null) {
                // Nothing to do
                return;
            }
            List<LogEvent> batchToProcess = new ArrayList<>(currentBatch);
            currentBatch.clear();
            CompletableFuture<Void> futureToComplete = null;
            syncLock.lock();
            try {
                if (pendingSyncFuture != null) {
                    futureToComplete = pendingSyncFuture;
                    pendingSyncFuture = null; // Consume the pending sync request
                }
            } finally {
                syncLock.unlock();
            }
            boolean syncSuccessful = attemptWriteAndSyncWithRetries(batchToProcess);
            if (futureToComplete != null) {
                if (syncSuccessful) {
                    futureToComplete.complete(null);
                } else {
                    // syncException should be set by attemptWriteAndSyncWithRetries
                    futureToComplete.completeExceptionally(syncException != null ? syncException
                        : new IOException("Sync failed after retries"));
                }
            }
        }

        private boolean attemptWriteAndSyncWithRetries(List<LogEvent> batch) {
            int attempt = 0;
            while (attempt <= maxRetries) {
                try {
                    currentWriter = logManager.getWriter();
                    for (LogEvent e : batch) {
                        currentWriter.append(e.tableName, e.commitId, e.mutation);
                    }
                    currentWriter.sync();
                    return true; // Success
                } catch (ReplicationLogManager.StaleLogWriterException e) {
                    // The log was rotated out from under us. No worries, just retry.
                    LOG.debug("Attempt " + (attempt + 1) + "/" + (maxRetries + 1)
                        + " to write/sync batch failed", e);
                } catch (IOException e) {
                    LOG.debug("Attempt " + (attempt + 1) + "/" + (maxRetries + 1)
                        + " to write/sync batch failed", e);
                    syncException = e; // Store the last exception
                    if (attempt >= maxRetries) {
                        String message =
                            String.format("Sync failed after %d retries for batch at sequence %d",
                                maxRetries + 1, batch.isEmpty() ? "N/A" : batch.get(0).sequence);
                        LOG.error(message, e);
                        return false; // Max retries reached
                    }
                    try {
                        // Force writer rotation and get a new writer for the next attempt.
                        currentWriter = logManager.rotateLog();
                        Thread.sleep(retryPauseMs);
                    } catch (IOException | InterruptedException ex) {
                        if (ex instanceof InterruptedException) {
                            Thread.currentThread().interrupt();
                        }
                        String message = "Failed to recover from sync failure";
                        LOG.error(message, ex);
                        syncException = new IOException(message, ex);
                        return false; // Cannot recover by rotating
                    }
                }
                attempt++;
            }
            return false; // Should not be reached, but satisfies compiler
        }
    }

    // We abort the regionserver when facing any internal Disruptor lifecycle errors.
    class LogExceptionHandler implements ExceptionHandler<LogEvent> {
        @Override
        public void handleEventException(Throwable e, long sequence, LogEvent event) {
            String message = "Exception processing sequence " + sequence + "  for event " + event;
            LOG.error(message, e);
            logManager.abort(message, e);
        }

        @Override
        public void handleOnStartException(Throwable e) {
            LOG.error("Exception during Disruptor startup", e);
            logManager.abort("Exception during Disruptor startup", e);
        }

        @Override
        public void handleOnShutdownException(Throwable e) {
            // Should not happen, but if it does, the regionserver is aborting or shutting down.
            // Doing more might be tricky. If it is aborting, then we are good and need do nothing,
            // although asking for an abort during an abort is supported. If it is shutting down
            // cleanly, we have a problem and should abort, but we may have lost data.
            LOG.error("Exception during Disruptor shutdown", e);
            logManager.abort("Exception during Disruptor shutdown", e);
        }
    }

}
