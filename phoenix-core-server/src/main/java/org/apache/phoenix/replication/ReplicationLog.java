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
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final AtomicBoolean rotationRequested = new AtomicBoolean(false);
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
    long rotationSize = conf.getLong(ReplicationLogGroup.REPLICATION_LOG_ROTATION_SIZE_BYTES_KEY,
      ReplicationLogGroup.DEFAULT_REPLICATION_LOG_ROTATION_SIZE_BYTES);
    double rotationSizePercent =
      conf.getDouble(ReplicationLogGroup.REPLICATION_LOG_ROTATION_SIZE_PERCENTAGE_KEY,
        ReplicationLogGroup.DEFAULT_REPLICATION_LOG_ROTATION_SIZE_PERCENTAGE);
    this.rotationSizeBytes = (long) (rotationSize * rotationSizePercent);
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
      .setCompression(compression);
    LogFileWriter newWriter = new LogFileWriter();
    newWriter.init(writerContext);
    newWriter.setGeneration(writerGeneration.incrementAndGet());
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
    rotationExecutor.scheduleAtFixedRate(new LogRotationTask(), initialDelay, rotationTimeMs,
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
   * Submits an on-demand {@link LogRotationTask} to the executor. The compareAndSet avoids
   * submitting duplicate tasks — if the flag is already set, a task is already queued.
   */
  private void requestRotation() {
    if (rotationRequested.compareAndSet(false, true)) {
      try {
        rotationExecutor.execute(new LogRotationTask());
      } catch (java.util.concurrent.RejectedExecutionException e) {
        LOG.info("Rotation executor shut down, skipping on-demand rotation", e);
        rotationRequested.set(false);
      }
    }
  }

  /**
   * Requests an on-demand rotation if the current writer exceeds the size threshold.
   */
  private void requestRotationIfNeeded() throws IOException {
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
        requestRotationIfNeeded();
        break;
      } catch (IOException e) {
        LOG.debug("Attempt {}/{} failed", attempt, maxAttempts, e);
        if (attempt == maxAttempts) {
          closeOnError();
          throw e;
        }
        // First failure retries on the same writer (transient). Second failure
        // requests a new writer to recover from non-transient stream errors.
        if (attempt > 1) {
          requestRotation();
        }
        try {
          Thread.sleep(retryDelayMs);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new InterruptedIOException("Interrupted during retry delay");
        }
      }
    }
  }

  protected void append(Record r) throws IOException {
    apply(writer -> writer.append(r.tableName, r.commitId, r.mutation));
    // Add to current batch only after we succeed at appending
    currentBatch.add(r);
  }

  protected void append(String tableName, long commitId, Mutation mutation) throws IOException {
    apply(writer -> writer.append(tableName, commitId, mutation));
  }

  protected void sync() throws IOException {
    apply(LogFileWriter::sync);
    currentBatch.clear();
  }

  /**
   * Return the current batch of in-flight appends. It is used when we switch replication mode.
   * @return List of in-flight successful append records
   */
  protected List<Record> getCurrentBatch() {
    return currentBatch;
  }

  /**
   * Force closes the log upon an unrecoverable internal error. This is a fail-stop behavior: once
   * called, the log is marked as closed, the Disruptor is halted, and all subsequent append() and
   * sync() calls will throw an IOException("Closed"). This ensures that no further operations are
   * attempted on a log that has encountered a critical error.
   */
  protected void closeOnError() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    stopRotationExecutor();
    closeExecutor.shutdownNow();
    LogFileWriter staged = pendingWriter.getAndSet(null);
    if (staged != null) {
      closeWriter(staged);
    }
    closeWriter(currentWriter);
  }

  /** Closes the log. */
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    stopRotationExecutor();
    closeExecutor.shutdown();
    try {
      if (!closeExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        closeExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      closeExecutor.shutdownNow();
    }
    LogFileWriter staged = pendingWriter.getAndSet(null);
    if (staged != null) {
      closeWriter(staged);
    }
    closeWriter(currentWriter);
  }

  @VisibleForTesting
  protected void forceRotation() {
    new LogRotationTask().run();
    checkAndReplaceWriter(false);
  }

  protected FileSystem getFileSystem(URI uri) throws IOException {
    return FileSystem.get(uri, logGroup.getConfiguration());
  }

  /**
   * Creates a new writer on a background thread and stages it in {@code pendingWriter} for the
   * consumer thread to drain inside {@link #apply}. Invoked both by the scheduled rotation executor
   * at round boundaries and on-demand via {@link #requestRotation()}.
   */
  protected class LogRotationTask implements Runnable {
    @Override
    public void run() {
      if (closed.get()) {
        return;
      }
      rotationRequested.compareAndSet(true, false);

      try {
        LogFileWriter newWriter = createNewWriter();
        LogFileWriter undrained = pendingWriter.getAndSet(newWriter);
        if (undrained != null) {
          closeWriter(undrained);
        }
        rotationFailures.set(0);
        logGroup.getMetrics().incrementRotationCount();
      } catch (IOException e) {
        logGroup.getMetrics().incrementRotationFailureCount();
        long numFailures = rotationFailures.incrementAndGet();
        if (numFailures >= maxRotationRetries) {
          LOG.error("Too many rotation failures ({}/{}), closing log", numFailures,
            maxRotationRetries, e);
          closeOnError();
        } else {
          LOG.info("Failed to create new writer for rotation (attempt {}/{}), retrying...",
            numFailures, maxRotationRetries, e);
        }
      }
    }
  }
}
