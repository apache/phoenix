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
import java.io.InterruptedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.phoenix.replication.ReplicationLogGroup.Record;
import org.apache.phoenix.replication.log.LogFileWriter;
import org.apache.phoenix.replication.log.LogFileWriterContext;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * ReplicationLog manages the underlying replication log.
 * <p>
 * This class implements the functionality for managing the replication log including log rotation
 * based on time or size, retries on file operations and the lifecycle of the underlying
 * LogFileWriter.
 * </p>
 * <p>
 * All rotation (scheduled round-boundary ticks, on-demand size, and error recovery) is handled by
 * {@link LogRotationTask} which creates a new writer on a background thread and stages it in
 * {@code pendingWriter}. The disruptor consumer thread drains the staged writer inside
 * {@link #apply} before each attempt, swaps the pointer, replays any unsynced appends, and submits
 * the old writer to {@code closeExecutor} for async close.
 * </p>
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value = { "EI_EXPOSE_REP", "EI_EXPOSE_REP2", "MS_EXPOSE_REP" }, justification = "Intentional")
public class ReplicationLog {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationLog.class);

  protected final ReplicationLogGroup logGroup;
  protected final long rotationTimeMs;
  protected final long rotationSizeBytes;
  protected final long fsBlockSize;
  protected final int maxRotationRetries;
  protected final Compression.Algorithm compression;
  protected final int maxAttempts;
  protected final long retryDelayMs;
  // Underlying file writer — only mutated by the disruptor consumer thread.
  protected volatile LogFileWriter currentWriter;
  protected final AtomicLong writerGeneration = new AtomicLong();
  protected final AtomicLong rotationFailures = new AtomicLong(0);
  // Staged writer created by the background LogRotationTask, drained by checkAndReplaceWriter().
  private final AtomicReference<LogFileWriter> pendingWriter = new AtomicReference<>();
  // Latch set by apply() on the retry path before calling requestRotation(); counted down by
  // LogRotationTask in a finally block so apply() can wait (with timeout) for a fresh writer.
  private volatile CountDownLatch rotationStagedLatch;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  // Single gate for rotation submission. Set by requestRotation()'s CAS before queuing a task,
  // cleared in LogRotationTask's finally. Both scheduled ticks and on-demand callers go through
  // requestRotation(), so a request that arrives while a rotation is queued or running is a
  // no-op — preventing the duplicate-writer bug where an in-flight scheduled rotation and a
  // concurrent size-triggered request both stage writers and the second closes the first.
  @VisibleForTesting
  final AtomicBoolean rotationRequested = new AtomicBoolean(false);
  private final ExecutorService closeExecutor;
  protected ScheduledExecutorService rotationExecutor;
  // Manages the creation of the actual log file in the shard directory
  protected ReplicationShardDirectoryManager replicationShardDirectoryManager;
  // List of in-flight appends which are successful but haven't been synced yet
  private final List<Record> currentBatch = new ArrayList<>();
  // Tracks the generation of the writer that currentBatch was appended to.
  // Used to detect writer swaps and trigger replay of unsynced appends.
  private long generation;

  public ReplicationLog(ReplicationLogGroup logGroup,
    ReplicationShardDirectoryManager shardManager) {
    this.logGroup = logGroup;
    Configuration conf = logGroup.getConfiguration();
    this.maxAttempts = conf.getInt(ReplicationLogGroup.REPLICATION_LOG_SYNC_RETRIES_KEY,
      ReplicationLogGroup.DEFAULT_REPLICATION_LOG_SYNC_RETRIES) + 1;
    this.retryDelayMs = conf.getLong(ReplicationLogGroup.REPLICATION_LOG_RETRY_DELAY_MS_KEY,
      ReplicationLogGroup.DEFAULT_REPLICATION_LOG_RETRY_DELAY_MS);
    this.rotationTimeMs = shardManager.getReplicationRoundDurationSeconds() * 1000L;
    long configuredRotationSize =
      conf.getLong(ReplicationLogGroup.REPLICATION_LOG_ROTATION_SIZE_BYTES_KEY,
        ReplicationLogGroup.DEFAULT_REPLICATION_LOG_ROTATION_SIZE_BYTES);
    double rotationSizePercent =
      conf.getDouble(ReplicationLogGroup.REPLICATION_LOG_ROTATION_SIZE_PERCENTAGE_KEY,
        ReplicationLogGroup.DEFAULT_REPLICATION_LOG_ROTATION_SIZE_PERCENTAGE);
    this.rotationSizeBytes = (long) (configuredRotationSize * rotationSizePercent);
    this.fsBlockSize = conf.getLong(ReplicationLogGroup.REPLICATION_LOG_FS_BLOCK_SIZE_BYTES_KEY,
      ReplicationLogGroup.DEFAULT_REPLICATION_LOG_FS_BLOCK_SIZE_BYTES);
    this.maxRotationRetries = conf.getInt(ReplicationLogGroup.REPLICATION_LOG_ROTATION_RETRIES_KEY,
      ReplicationLogGroup.DEFAULT_REPLICATION_LOG_ROTATION_RETRIES);
    String compressionName = conf.get(ReplicationLogGroup.REPLICATION_LOG_COMPRESSION_ALGORITHM_KEY,
      ReplicationLogGroup.DEFAULT_REPLICATION_LOG_COMPRESSION_ALGORITHM);
    Compression.Algorithm compression = Compression.Algorithm.NONE;
    if (
      !compressionName.equals(ReplicationLogGroup.DEFAULT_REPLICATION_LOG_COMPRESSION_ALGORITHM)
    ) {
      try {
        compression = Compression.getCompressionAlgorithmByName(compressionName);
      } catch (IllegalArgumentException e) {
        LOG.warn("Unknown compression type {}, using NONE", compressionName, e);
      }
    }
    this.compression = compression;
    this.replicationShardDirectoryManager = shardManager;
    this.closeExecutor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true)
      .setNameFormat("Close-ReplicationLog-Writer-" + logGroup.getHAGroupName() + "-%d").build());
  }

  /** Initialize the writer. */
  public void init() throws IOException {
    startRotationExecutor();
    currentWriter = createNewWriter();
    generation = currentWriter.getGeneration();
  }

  /** Creates and initializes a new LogFileWriter. */
  protected LogFileWriter createNewWriter() throws IOException {
    long timestamp = EnvironmentEdgeManager.currentTimeMillis();
    Path filePath = replicationShardDirectoryManager.getWriterPath(timestamp,
      logGroup.getServerName().getServerName());
    LogFileWriterContext writerContext = new LogFileWriterContext(logGroup.getConfiguration())
      .setFileSystem(replicationShardDirectoryManager.getFileSystem()).setFilePath(filePath)
      .setCompression(compression).setFsBlockSize(fsBlockSize);
    LogFileWriter newWriter = new LogFileWriter();
    newWriter.init(writerContext);
    newWriter.setGeneration(writerGeneration.incrementAndGet());
    LOG.info("Created new writer: {}", newWriter);
    return newWriter;
  }

  protected void startRotationExecutor() {
    long now = EnvironmentEdgeManager.currentTimeMillis();
    long currentRoundStart = replicationShardDirectoryManager.getNearestRoundStartTimestamp(now);
    long initialDelay = computeInitialDelay(now, currentRoundStart, rotationTimeMs);
    startRotationExecutor(initialDelay);
  }

  protected void startRotationExecutor(long initialDelay) {
    rotationExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
      .setNameFormat("ReplicationLogRotation-" + logGroup.getHAGroupName() + "-%d").setDaemon(true)
      .build());
    // Scheduled ticks route through requestRotation() so they share the same CAS gate as on-demand
    // size-triggered rotations — only one rotation can be queued or running at a time.
    rotationExecutor.scheduleAtFixedRate(this::requestRotation, initialDelay, rotationTimeMs,
      TimeUnit.MILLISECONDS);
    LOG.info("Started rotation executor with initial delay {}ms and interval {}ms", initialDelay,
      rotationTimeMs);
  }

  @VisibleForTesting
  static long computeInitialDelay(long now, long currentRoundStart, long rotationTimeMs) {
    return currentRoundStart + rotationTimeMs - now;
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

  /**
   * Checks if the current log file needs to be rotated based on size.
   * @return true if rotation is needed, false otherwise.
   * @throws IOException If an error occurs checking the file size.
   */
  protected boolean shouldRotateForSize() throws IOException {
    if (currentWriter == null) {
      LOG.warn("Current writer is null, forcing rotation.");
      return true;
    }
    long currentSize = currentWriter.getLength();
    if (currentSize >= rotationSizeBytes) {
      LOG.info("Rotating log file {} due to size threshold ({} bytes, threshold {} bytes)",
        currentWriter, currentSize, rotationSizeBytes);
      return true;
    }
    return false;
  }

  @VisibleForTesting
  protected LogFileWriter getWriter() {
    checkAndReplaceWriter(false);
    return currentWriter;
  }

  /**
   * Drains any staged pendingWriter, swapping it in as currentWriter.
   * @param asyncClose if true, old writer is submitted to closeExecutor for async close; if false,
   *                   old writer is closed synchronously on the calling thread.
   */
  protected void checkAndReplaceWriter(boolean asyncClose) {
    LogFileWriter newWriter = pendingWriter.getAndSet(null);
    if (newWriter != null) {
      LogFileWriter oldWriter = currentWriter;
      currentWriter = newWriter;
      LOG.info("Swapped writer from {} to {}, asyncClose={}", oldWriter, newWriter, asyncClose);
      if (asyncClose) {
        submitClose(oldWriter);
      } else {
        closeWriter(oldWriter);
      }
    }
  }

  /**
   * Submits a {@link LogRotationTask} to the executor. The CAS gate ensures only one rotation can
   * be queued or in flight at a time — if the flag is already set, a task is already pending or
   * running. Both scheduled ticks and on-demand size-triggered callers go through this method.
   */
  private void requestRotation() {
    if (rotationRequested.compareAndSet(false, true)) {
      try {
        rotationExecutor.execute(new LogRotationTask());
      } catch (java.util.concurrent.RejectedExecutionException e) {
        LOG.info("Rotation executor shut down, skipping rotation", e);
        rotationRequested.set(false);
      }
    }
  }

  /**
   * Requests a rotation if the current writer exceeds the size threshold. Skips when a writer is
   * already staged but not yet drained — the consumer hasn't swapped in the new writer yet, so
   * {@code currentWriter} still reports the old (over-threshold) length and a fresh request would
   * stage a redundant writer that the next task immediately closes.
   */
  private void requestRotationIfOversized() throws IOException {
    if (isClosed() || rotationRequested.get() || pendingWriter.get() != null) {
      return;
    }
    if (shouldRotateForSize()) {
      requestRotation();
    }
  }

  private void submitClose(LogFileWriter writer) {
    if (writer == null) {
      return;
    }
    closeExecutor.execute(() -> closeWriter(writer));
  }

  /** Closes the given writer, logging any errors that occur during close. */
  private void closeWriter(LogFileWriter writer) {
    if (writer == null) {
      return;
    }
    LOG.info("Closing writer: {}", writer);
    try {
      writer.close();
    } catch (IOException e) {
      LOG.error("Error closing log writer: {}", writer, e);
    }
  }

  /**
   * Check if this replication log is closed.
   * @return true if closed, false otherwise
   */
  public boolean isClosed() {
    return closed.get();
  }

  private interface Action {
    void action(LogFileWriter writer) throws IOException;
  }

  private void replayCurrentBatch() throws IOException {
    if (currentBatch.isEmpty()) {
      return;
    }
    LOG.info("Replaying {} unsynced records into new writer {}", currentBatch.size(),
      currentWriter);
    for (Record r : currentBatch) {
      currentWriter.append(r.tableName, r.commitId, r.mutation);
    }
  }

  private void apply(Action action) throws IOException {
    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      checkAndReplaceWriter(true);
      if (isClosed()) {
        throw new IOException("Closed");
      }
      try {
        if (currentWriter.getGeneration() > generation) {
          replayCurrentBatch();
          generation = currentWriter.getGeneration();
        }
        action.action(currentWriter);
        break;
      } catch (IOException e) {
        LOG.debug("Attempt {}/{} failed", attempt, maxAttempts, e);
        if (attempt == maxAttempts) {
          throw e;
        }
        // Each retry runs on a fresh writer. Stage a latch, request rotation, and wait briefly
        // for the LogRotationTask to count the latch down after staging a new pendingWriter.
        CountDownLatch latch = new CountDownLatch(1);
        rotationStagedLatch = latch;
        requestRotation();
        try {
          latch.await(retryDelayMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new InterruptedIOException("Interrupted during retry delay");
        }
      }
    }
  }

  protected void append(Record r) throws IOException {
    final boolean[] blockSynced = { false };
    apply(writer -> {
      blockSynced[0] = writer.append(r.tableName, r.commitId, r.mutation);
    });
    // Add to current batch only after we succeed at appending
    currentBatch.add(r);
    if (blockSynced[0]) {
      // The block-full sync included this record — all records up to here are durable
      currentBatch.clear();
      // Check size only after currentBatch is empty: if a rotation stages a new writer mid-batch,
      // unsynced records are replayed into it and could exceed the threshold immediately,
      // triggering another rotation and forming a loop. Checking only when the batch is empty
      // ensures size-based rotations don't cascade.
      requestRotationIfOversized();
    }
  }

  protected void append(String tableName, long commitId, Mutation mutation) throws IOException {
    apply(writer -> writer.append(tableName, commitId, mutation));
  }

  protected void sync() throws IOException {
    apply(LogFileWriter::sync);
    currentBatch.clear();
    // See note in append(Record): size check runs only when currentBatch is empty so a replay
    // into a freshly-staged writer cannot trigger a cascading rotation.
    requestRotationIfOversized();
  }

  /**
   * Return the current batch of in-flight appends. It is used when we switch replication mode.
   * @return List of in-flight successful append records
   */
  protected List<Record> getCurrentBatch() {
    return currentBatch;
  }

  /**
   * Closes the log with a bounded duration. Subsequent append() and sync() calls will throw
   * IOException("Closed"). Safe to call multiple times — only the first invocation performs
   * cleanup.
   * @param graceful true for graceful shutdown, false for error/forced shutdown
   */
  public void close(boolean graceful) {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    stopRotationExecutor();
    LogFileWriter staged = pendingWriter.getAndSet(null);
    if (staged != null) {
      submitClose(staged);
    }
    submitClose(currentWriter);
    closeExecutor.shutdown();
    try {
      if (!closeExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
        LOG.warn("Close executor did not terminate in 10s, abandoning writer closes");
        closeExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      closeExecutor.shutdownNow();
    }
  }

  @VisibleForTesting
  protected void forceRotation() {
    new LogRotationTask().run();
  }

  protected FileSystem getFileSystem(URI uri) throws IOException {
    return FileSystem.get(uri, logGroup.getConfiguration());
  }

  /**
   * Creates a new writer on a background thread and stages it in {@code pendingWriter} for the
   * consumer thread to drain inside {@link #apply}. Submitted via {@link #requestRotation()} from
   * both the scheduled tick and on-demand size-triggered callers.
   * <p>
   * {@code rotationRequested} stays set for the entire duration of run() (cleared in finally), so
   * any request that arrives while this task is creating/staging a writer is rejected by
   * {@link #requestRotation()}'s CAS and not duplicated.
   */
  protected class LogRotationTask implements Runnable {
    @Override
    public void run() {
      if (closed.get()) {
        return;
      }
      boolean staged = false;
      long startNs = System.nanoTime();
      try {
        LogFileWriter newWriter = createNewWriter();
        LogFileWriter undrained = pendingWriter.getAndSet(newWriter);
        staged = true;
        if (undrained != null) {
          closeWriter(undrained);
        }
        rotationFailures.set(0);
        logGroup.getMetrics().incrementRotationCount();
      } catch (Throwable t) {
        logGroup.getMetrics().incrementRotationFailureCount();
        long numFailures = rotationFailures.incrementAndGet();
        if (numFailures >= maxRotationRetries) {
          LOG.error("Too many rotation failures ({}/{}), closing log", numFailures,
            maxRotationRetries, t);
          close(false);
        } else {
          LOG.error("Failed to create new writer for rotation (attempt {}/{}), retrying...",
            numFailures, maxRotationRetries, t);
        }
      } finally {
        // Time both success and failure paths so slow rotations are visible even when they fail.
        logGroup.getMetrics().updateRotationTime(System.nanoTime() - startNs);
        // Clear last so requestRotation()'s CAS suppresses duplicates throughout this run.
        rotationRequested.set(false);
        CountDownLatch latch = rotationStagedLatch;
        if (latch != null) {
          latch.countDown();
          rotationStagedLatch = null;
        }
        if (staged) {
          // Wake an idle consumer so it drains pendingWriter before the reader's round buffer
          // expires. Non-blocking — see ReplicationLogGroup#publishSwapEvent.
          logGroup.publishSwapEvent();
        }
      }
    }
  }
}
