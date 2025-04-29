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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.phoenix.replication.log.LogFile;
import org.apache.phoenix.replication.log.LogFileWriter;
import org.apache.phoenix.replication.log.LogFileWriterContext;
import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the lifecycle of replication log files on the active cluster side. It handles log
 * rotation based on time and size thresholds and provides the currently active LogFileWriter.
 * This class is intended to be thread-safe.
 * <p>
 * TODO: This class will switch between active (synchronous) and store-and-forward fallback
 * writer depending on error handling/retry strategy.
 */
public class ReplicationLogManager implements Closeable {

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

    public static final String SHARD_DIR_FORMAT = "shard-%04d";
    public static final String FILE_NAME_FORMAT = "%d-%s.plog";

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationLogManager.class);

    private static volatile ReplicationLogManager INSTANCE;

    private final Configuration conf;
    private final RegionServerServices rsServices;
    protected FileSystem standbyFs;
    protected FileSystem fallbackFs; // For store-and-forward (future use)
    protected int numShards;
    protected URI standbyUrl;
    protected URI fallbackUrl; // For store-and-forward (future use)
    private final long rotationTimeMs;
    private final long rotationSizeBytes;
    private final Compression.Algorithm compression;
    private final ReentrantLock lock = new ReentrantLock();
    protected volatile LogFile.Writer currentWriter; // Current writer
    //private volatile LogFile.Writer activeWriter; // Current active side writer (future use)
    //private volatile LogFile.Writer fallbackWriter; // Current fallback writer (future use)
    protected volatile long lastRotationTime;
    protected volatile long writerGeneration;
    private ScheduledExecutorService rotationExecutor;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final ConcurrentHashMap<Path,Object> shardMap = new ConcurrentHashMap<>();

    /**
     * Gets the singleton instance of the ReplicationLogManager using the lazy initializer pattern.
     * Initializes the instance if it hasn't been created yet.
     * @param conf Configuration object.
     * @return The singleton ReplicationLogManager instance.
     * @throws IOException If initialization fails.
     */
    public static ReplicationLogManager getInstance(Configuration conf,
            RegionServerServices rsServices) throws IOException {
        if (INSTANCE == null) {
            synchronized (ReplicationLogManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ReplicationLogManager(conf, rsServices);
                    INSTANCE.init();
                }
            }
        }
        return INSTANCE;
    }

    protected ReplicationLogManager(Configuration conf, RegionServerServices rsServices) {
        this.conf = conf;
        this.rsServices = rsServices;
        this.rotationTimeMs = conf.getLong(REPLICATION_LOG_ROTATION_TIME_MS_KEY,
            DEFAULT_REPLICATION_LOG_ROTATION_TIME_MS);
        long rotationSize = conf.getLong(REPLICATION_LOG_ROTATION_SIZE_BYTES_KEY,
            DEFAULT_REPLICATION_LOG_ROTATION_SIZE_BYTES);
        double rotationSizePercent = conf.getDouble(REPLICATION_LOG_ROTATION_SIZE_PERCENTAGE_KEY,
            DEFAULT_REPLICATION_LOG_ROTATION_SIZE_PERCENTAGE);
        this.rotationSizeBytes = (long) (rotationSize * rotationSizePercent);
        this.numShards = conf.getInt(REPLICATION_NUM_SHARDS_KEY, DEFAULT_REPLICATION_NUM_SHARDS);
        if (numShards > MAX_REPLICATION_NUM_SHARDS) {
            throw new IllegalArgumentException(REPLICATION_NUM_SHARDS_KEY + " is " + numShards
                + ", but the limit is " + MAX_REPLICATION_NUM_SHARDS);
        }
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
    }

    public void init() throws IOException {
        if (started.get()) {
            return;
        }
        initializeFileSystems();
        // For now we just initialize currentWriter
        currentWriter = new Writer(createNewWriter(standbyFs, standbyUrl), writerGeneration);
        lastRotationTime = EnvironmentEdgeManager.currentTimeMillis();
        startRotationExecutor();
        started.set(true);
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

    private long getRotationCheckInterval(long rotationTimeMs) {
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
        rotationExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
            .setNameFormat("ReplicationLogRotator-%d").setDaemon(true).build());
        long interval = getRotationCheckInterval(rotationTimeMs);
        rotationExecutor.scheduleWithFixedDelay(new LogRotationTask(), interval, interval,
            TimeUnit.MILLISECONDS);
    }

    // Stop the background task for time-based log rotation
    protected void stopRotationExecutor() {
        rotationExecutor.shutdownNow();
    }

    /**
     * Gets the current LogFileWriter, performing log rotation if necessary. This method is
     * thread-safe.
     * @return The current LogFileWriter instance.
     * @throws IOException If an error occurs during log rotation or writer creation.
     */
    public LogFile.Writer getWriter() throws IOException {
        lock.lock();
        try {
            if (closed.get()) {
                throw new IOException("Closed");
            }
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
        if (now - lastRotationTime >= rotationTimeMs) {
            LOG.debug("Rotating log file due to time threshold ({} ms elapsed, threshold {} ms)",
                now - lastRotationTime, rotationTimeMs);
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
    protected LogFile.Writer rotateLog() throws IOException {
        lock.lock(); // Lock is reentrant, so this is fine.
        try {
            // Close the current writer
            if (currentWriter != null) {
                LOG.debug("Closing current writer: {}", currentWriter);
                closeWriter(currentWriter);
                currentWriter = null;
            }
            currentWriter = new Writer(createNewWriter(standbyFs, standbyUrl), writerGeneration);
            LOG.debug("Created new writer: {}", currentWriter);
            // Increment the writer generation
            writerGeneration++;
            lastRotationTime = EnvironmentEdgeManager.currentTimeMillis();
            return currentWriter;
        } finally {
            lock.unlock();
        }
    }

    protected Path makeWriterPath(FileSystem fs, URI url) throws IOException {
        String serverName = rsServices.getServerName().toString();
        long timestamp = EnvironmentEdgeManager.currentTimeMillis();
        // To have all logs for a given regionserver appear in the same shard, hash only the
        // serverName. However we expect some regionservers will have significantly more load than
        // others so we instead distribute the logs over all of the shards randomly for a more even
        // overall distribution by also hashing the timestamp.
        int shard = (serverName.hashCode() ^ Long.hashCode(timestamp)) % numShards;
        Path shardPath = new Path(url.getPath(), String.format(SHARD_DIR_FORMAT, shard));
        // Ensure the shard directory exists. We track which shard directories we have probed or
        // created to avoid a round trip to the namenode for repeats.
        IOException exception[] = new IOException[1];
        shardMap.computeIfAbsent(shardPath, (p) -> {
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
    protected LogFile.Writer createNewWriter(FileSystem fs, URI url) throws IOException {
        Path filePath = makeWriterPath(fs, url);
        LogFileWriterContext writerContext = new LogFileWriterContext(conf).setFileSystem(fs)
            .setFilePath(filePath).setCompression(compression);
        LogFile.Writer newWriter = new LogFileWriter();
        try {
            newWriter.init(writerContext);
        } catch (IOException e) {
            LOG.error("Failed to initialize new LogFileWriter for path {}", filePath, e);
            throw e;
        }
        return newWriter;
    }

    protected void closeWriter(LogFile.Writer writer) {
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

    /** Shuts down the manager, closing the log writers. */
    public void close() {
        if (closed.compareAndSet(false, true)) {
            lock.lock();
            try {
                stopRotationExecutor();
                closeWriter(currentWriter);
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * Abort the regionserver.
     * @param why The reason for the abort
     */
    protected void abort(String why) {
        rsServices.abort(why);
    }

    /**
     * Abort the regionserver.
     * @param why The reason for the abort
     * @param t A throwable related to the reason for the abort
     */
    protected void abort(String why, Throwable t) {
        rsServices.abort(why, t);
    }

    /**
     * A wrapper around a LogFileWriter instance obtained from ReplicationLogManager. It holds the
     * generation stamp associated with the writer instance at the time of acquisition and
     * validates it before performing critical operations like sync.
     */
    protected class Writer implements LogFile.Writer {

        protected LogFile.Writer delegate;
        protected long generation;

        Writer(LogFile.Writer delegate, long generation) {
            this.delegate = delegate;
            this.generation = generation;
        }

        @Override
        public void init(LogFileWriterContext context) throws IOException {
            delegate.init(context);
        }

        @Override
        public void append(String tableName, long commitId, Mutation mutation) throws IOException {
            // No need to acquire the lock just to check the generation stamp in append.
            long current = writerGeneration;
            if (this.generation != current) {
                throw new StaleLogWriterException(
                    "Log writer rotated during operation. Expected generation " + generation
                        + ", found " + current);
            }
            delegate.append(tableName, commitId, mutation);
        }

        /**
         * Performs a sync operation, but only after verifying that the writer's generation stamp
         * is still valid. Acquires the manager's lock during the check and the delegate sync
         * operation. It is therefore impossible for a sync of the underlying writer and a log
         * roll to happen concurrently. We are guaranteed the sync will succeed (or fail) without
         * interference from the manager.
         * @throws IOException             if the underlying sync fails.
         * @throws StaleLogWriterException if the writer instance has been rotated
         *                                 out since it was acquired.
         */
        @Override
        public void sync() throws IOException {
            lock.lock(); // Acquire the manager's lock so sync doesn't race with a roll.
            try {
                long current = writerGeneration;
                if (this.generation != current) {
                    throw new StaleLogWriterException(
                        "Log writer rotated during operation. Expected generation " + generation
                            + ", found " + current);
                }
                // Generation is still valid, proceed with sync on the delegate writer
                if (LOG.isTraceEnabled()) {
                     LOG.trace("Syncing writer {} with generation {}", delegate, generation);
                }
                delegate.sync();
                if (LOG.isTraceEnabled()) {
                     LOG.trace("Sync successful for writer {} with generation {}", delegate, generation);
                }
            } finally {
                lock.unlock(); // Release the manager's lock
            }
        }

        @Override
        public long getLength() throws IOException {
            return delegate.getLength();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        @Override
        public String toString() {
            return "ReplicationLogManager.Writer [delegate=" + delegate + ", generation="
                + generation + "]";
        }
    }

    /**
     * Thrown when the current writer is stale (has been rolled and closed). The caller must get
     * again the current writer instance from the manager and retry what it was doing, which was
     * presumably the processing of a batch of mutations followed by a sync.
     */
    protected static class StaleLogWriterException extends IOException {

        private static final long serialVersionUID = 1L;

        public StaleLogWriterException(String message) {
            super(message);
        }

    }

    /** Implements time based rotation independent of in-line checking during append(). */
    protected class LogRotationTask implements Runnable {
        @Override
        public void run() {
            if (closed.get()) {
                return;
            }
            // Use tryLock with a timeout to avoid blocking indefinitely if another thread holds
            // the lock for an unexpectedly long time (e.g., during a problematic rotation).
            boolean acquired = false;
            try {
                // Wait a short time for the lock
                acquired = lock.tryLock(1, TimeUnit.SECONDS);
                if (acquired) {
                    if (closed.get()) {
                        // Double-check closed status after acquiring lock
                        return;
                    }
                    // Check only the time condition here, size is handled by getWriter
                    long now = EnvironmentEdgeManager.currentTimeMillis();
                    if (currentWriter != null && now - lastRotationTime >= rotationTimeMs) {
                        LOG.debug("Time based rotation needed ({} ms elapsed, threshold {} ms).",
                              now - lastRotationTime, rotationTimeMs);
                        try {
                            rotateLog(); // rotateLog updates lastRotationTime
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
