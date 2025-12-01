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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
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

import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * ReplicationLog manages the underlying replication log.
 * <p>
 * This class implements the functionality for managing the replication log including log rotation
 * based on time or size, retries on file operations and the lifecycle of the underlying
 * LogFileWriter.
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
  protected final ReentrantLock lock = new ReentrantLock();
  protected final int maxAttempts; // Configurable max attempts for sync
  protected final long retryDelayMs; // Configurable delay between attempts
  // Underlying file writer
  protected volatile LogFileWriter currentWriter;
  protected final AtomicLong lastRotationTime = new AtomicLong();
  protected final AtomicLong writerGeneration = new AtomicLong();
  protected final AtomicLong rotationFailures = new AtomicLong(0);
  protected ScheduledExecutorService rotationExecutor;
  protected volatile boolean closed = false;
  // Manages the creation of the actual log file in the shard directory
  protected ReplicationShardDirectoryManager replicationShardDirectoryManager;
  // List of in-flight appends which are successful but haven't been synced yet
  private final List<Record> currentBatch = new ArrayList<>();
  // Current version of the writer being used for file operations. It is needed for detecting
  // when the writer changes because of rotation while we are in the middle of a write operation.
  private long generation;

  public ReplicationLog(ReplicationLogGroup logGroup,
    ReplicationShardDirectoryManager shardManager) {
    this.logGroup = logGroup;
    Configuration conf = logGroup.getConfiguration();
    this.maxAttempts = conf.getInt(ReplicationLogGroup.REPLICATION_LOG_SYNC_RETRIES_KEY,
      ReplicationLogGroup.DEFAULT_REPLICATION_LOG_SYNC_RETRIES) + 1;
    this.retryDelayMs = conf.getLong(ReplicationLogGroup.REPLICATION_LOG_RETRY_DELAY_MS_KEY,
      ReplicationLogGroup.DEFAULT_REPLICATION_LOG_RETRY_DELAY_MS);
    this.rotationTimeMs = conf.getLong(ReplicationLogGroup.REPLICATION_LOG_ROTATION_TIME_MS_KEY,
      ReplicationLogGroup.DEFAULT_REPLICATION_LOG_ROTATION_TIME_MS);
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
        LOG.warn("Unknown compression type " + compressionName + ", using NONE", e);
      }
    }
    this.compression = compression;
    this.replicationShardDirectoryManager = shardManager;
  }

  /** The reason for requesting a log rotation. */
  protected enum RotationReason {
    /** Rotation requested due to time threshold being exceeded. */
    TIME,
    /** Rotation requested due to size threshold being exceeded. */
    SIZE,
    /** Rotation requested due to an error condition. */
    ERROR
  }

  /** Initialize the writer. */
  public void init() throws IOException {
    // Start time based rotation.
    lastRotationTime.set(EnvironmentEdgeManager.currentTimeMillis());
    startRotationExecutor();
    // Create the initial writer
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
    long rotationCheckInterval = getRotationCheckInterval(rotationTimeMs);
    rotationExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
      .setNameFormat("ReplicationLogRotation-" + logGroup.getHAGroupName() + "-%d").setDaemon(true)
      .build());
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
   * Checks if the current log file needs to be rotated based on time or size. Must be called under
   * lock.
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
   * <li>Time threshold exceeded (TIME)</li>
   * <li>Size threshold exceeded (SIZE)</li>
   * <li>Error condition requiring rotation (ERROR)</li>
   * </ul>
   * <p>
   * The method implements retry logic for handling rotation failures. If rotation fails, it retries
   * up to maxRotationRetries times. If the number of failures exceeds maxRotationRetries, an
   * exception is thrown. Otherwise, it logs a warning and continues with the current writer.
   * <p>
   * The method is thread-safe and uses a lock to ensure atomic rotation operations.
   * @param reason The reason for requesting log rotation
   * @return The new LogFileWriter instance if rotation succeeded, or the current writer if rotation
   *         failed
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
      closeWriter(currentWriter);
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
      LOG.warn("Failed to rotate log (attempt {}/{}), retrying...", numFailures, maxRotationRetries,
        e);
    } finally {
      lock.unlock();
    }
    return currentWriter;
  }

  /** Closes the given writer, logging any errors that occur during close. */
  private void closeWriter(LogFileWriter writer) {
    if (writer == null) {
      return;
    }
    LOG.debug("Closing writer: {}", writer);
    try {
      writer.close();
    } catch (IOException e) {
      // For now, just log and continue
      LOG.error("Error closing log writer: " + writer, e);
    }
  }

  /**
   * Check if this replication log is closed.
   * @return true if closed, false otherwise
   */
  public boolean isClosed() {
    return closed;
  }

  private interface Action {
    void action(LogFileWriter writer) throws IOException;
  }

  private void apply(Action action) throws IOException {
    LogFileWriter writer = getWriter();
    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      if (isClosed()) {
        throw new IOException("Closed");
      }
      try {
        if (writer.getGeneration() > generation) {
          generation = writer.getGeneration();
          // If the writer has been rotated, we need to replay the current batch of
          // in-flight appends into the new writer.
          if (!currentBatch.isEmpty()) {
            LOG.trace("Writer has been rotated, replaying in-flight batch");
            for (Record r : currentBatch) {
              writer.append(r.tableName, r.commitId, r.mutation);
            }
          }
        }
        action.action(writer);
        break;
      } catch (IOException e) {
        // IO exception, force a rotation.
        LOG.debug("Attempt " + attempt + "/" + maxAttempts + " failed", e);
        if (attempt == maxAttempts) {
          // TODO: Add log
          closeOnError();
          throw e;
        }
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

  protected void append(Record r) throws IOException {
    apply(writer -> writer.append(r.tableName, r.commitId, r.mutation));
    // Add to current batch only after we succeed at appending
    currentBatch.add(r);
  }

  protected void append(String tableName, long commitId, Mutation mutation) throws IOException {
    apply(writer -> writer.append(tableName, commitId, mutation));
  }

  protected void sync() throws IOException {
    apply(writer -> writer.sync());
    // Sync completed, clear the list of in-flight appends.
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
            LOG.debug("Time based rotation needed ({} ms elapsed, threshold {} ms).", now - last,
              rotationTimeMs);
            try {
              rotateLog(RotationReason.TIME); // rotateLog updates lastRotationTime
            } catch (IOException e) {
              LOG.error("Failed to rotate log, currentWriter is {}", currentWriter, e);
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
}
