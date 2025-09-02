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
package org.apache.phoenix.replication.reader;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LeaseRecoverable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.util.RecoverLeaseFSUtils;
import org.apache.phoenix.replication.log.InvalidLogTrailerException;
import org.apache.phoenix.replication.log.LogFile;
import org.apache.phoenix.replication.log.LogFileReader;
import org.apache.phoenix.replication.log.LogFileReaderContext;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogProcessor;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogProcessorImpl;
import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationLogProcessor implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationLogProcessor.class);

    /**
     * The maximum count of mutations to process in single batch while reading replication log file
     */
    public static final String REPLICATION_STANDBY_LOG_REPLAY_BATCH_SIZE =
            "phoenix.replication.log.standby.replay.batch.size";

    /**
     * The default batch size for reading the replication log file.
     * Assuming each log record to be 10 KB (un-compressed) and allow at max 64 MB of
     * in-memory records to be processed
     */
    public static final int DEFAULT_REPLICATION_STANDBY_LOG_REPLAY_BATCH_SIZE = 6400;

    /**
     * The maximum total size of mutations to process in single batch while reading
     * replication log file
     */
    public static final String REPLICATION_STANDBY_LOG_REPLAY_BATCH_SIZE_BYTES =
            "phoenix.replication.log.standby.replay.batch.size.bytes";

    /**
     * The default batch size in bytes for reading the replication log file (64 MB)
     */
    public static final long DEFAULT_REPLICATION_STANDBY_LOG_REPLAY_BATCH_SIZE_BYTES =
            64 * 1024 * 1024L;

    /**
     * The number of threads to apply mutations via async hbase client
     */
    public static final String REPLICATION_STANDBY_LOG_REPLAY_THREAD_POOL_SIZE =
            "phoenix.replication.log.standby.replay.thread.pool.size";

    /**
     * The default number of threads for applying mutations via async hbase client
     */
    public static final int DEFAULT_REPLICATION_STANDBY_LOG_REPLAY_THREAD_POOL_SIZE = 5;

    /**
     * The maximum number of retries for HBase client operations while applying the mutations
     */
    public static final String REPLICATION_STANDBY_HBASE_CLIENT_RETRIES_COUNT =
            "phoenix.replication.standby.hbase.client.retries.number";

    /**
     * The default number of retries for HBase client operations while applying the mutations.
     */
    public static final int DEFAULT_REPLICATION_STANDBY_HBASE_CLIENT_RETRIES_COUNT = 4;

    /**
     * The timeout for HBase client operations while applying the mutations.
     */
    public static final String REPLICATION_STANDBY_HBASE_CLIENT_OPERATION_TIMEOUT_MS =
            "phoenix.replication.standby.hbase.client.operations.timeout";

    /**
     * The default timeout for HBase client operations while applying the mutations.
     */
    public static final long DEFAULT_REPLICATION_STANDBY_HBASE_CLIENT_OPERATION_TIMEOUT_MS = 8000;

    /**
     * The maximum number of retry attempts for failed batch operations.
     */
    public static final String REPLICATION_STANDBY_BATCH_RETRY_COUNT =
            "phoenix.replication.standby.batch.retry.count";

    /**
     * The default number of retry attempts for failed batch operations.
     */
    public static final int DEFAULT_REPLICATION_STANDBY_BATCH_RETRY_COUNT = 2;

    /**
     * The maximum delay for retry attempts in milliseconds.
     */
    public static final String REPLICATION_STANDBY_BATCH_RETRY_MAX_DELAY_MS =
            "phoenix.replication.standby.batch.retry.max.delay.ms";

    /**
     * The default maximum delay for retry attempts in milliseconds.
     */
    public static final long DEFAULT_REPLICATION_STANDBY_BATCH_RETRY_MAX_DELAY_MS = 10000;

    private final String haGroupName;

    private final Configuration conf;

    private final ExecutorService executorService;

    /**
     * This {@link AsyncConnection} is used for handling mutations
     */
    private volatile AsyncConnection asyncConnection;

    private final int batchSize;

    private final long batchSizeBytes;

    private final int batchRetryCount;

    private final long maxRetryDelayMs;

    private final MetricsReplicationLogProcessor metrics;

    /** Cache of ReplicationLogGroup instances by HA Group Name */
    private static final ConcurrentHashMap<String, ReplicationLogProcessor> INSTANCES =
            new ConcurrentHashMap<>();

    /**
     * Get or create a ReplicationLogProcessor instance for the given HA Group.
     *
     * @param conf Configuration object
     * @param haGroupName The HA Group name
     * @return ReplicationLogProcessor instance
     */
    public static ReplicationLogProcessor get(Configuration conf, String haGroupName) {
        return INSTANCES.computeIfAbsent(haGroupName,
            k -> new ReplicationLogProcessor(conf, haGroupName));
    }

    /**
     * Creates a new ReplicationLogProcessor with the given configuration and executor service.
     * @param conf The configuration to use
     * @param haGroupName The HA group name
     */
    protected ReplicationLogProcessor(final Configuration conf, final String haGroupName) {
        // Create a copy of configuration as some of the properties would be
        // overridden
        this.conf = HBaseConfiguration.create(conf);
        this.haGroupName = haGroupName;
        this.batchSize = this.conf.getInt(REPLICATION_STANDBY_LOG_REPLAY_BATCH_SIZE,
                DEFAULT_REPLICATION_STANDBY_LOG_REPLAY_BATCH_SIZE);
        this.batchSizeBytes = this.conf.getLong(REPLICATION_STANDBY_LOG_REPLAY_BATCH_SIZE_BYTES,
                DEFAULT_REPLICATION_STANDBY_LOG_REPLAY_BATCH_SIZE_BYTES);
        this.batchRetryCount = this.conf.getInt(REPLICATION_STANDBY_BATCH_RETRY_COUNT,
                DEFAULT_REPLICATION_STANDBY_BATCH_RETRY_COUNT);
        this.maxRetryDelayMs = this.conf.getLong(REPLICATION_STANDBY_BATCH_RETRY_MAX_DELAY_MS,
                DEFAULT_REPLICATION_STANDBY_BATCH_RETRY_MAX_DELAY_MS);
        final int threadPoolSize = this.conf.getInt(REPLICATION_STANDBY_LOG_REPLAY_THREAD_POOL_SIZE,
                DEFAULT_REPLICATION_STANDBY_LOG_REPLAY_THREAD_POOL_SIZE);
        decorateConf();
        this.metrics = createMetricsSource();
        this.executorService = Executors.newFixedThreadPool(threadPoolSize,
            new ThreadFactoryBuilder()
                .setNameFormat("Phoenix-Replication-Log-Processor-" + haGroupName + "-%d")
                .build());
    }

    /**
     * Decorate the Configuration object to make replication more receptive to delays by
     * reducing the timeout and number of retries.
     */
    private void decorateConf() {
        this.conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
                this.conf.getInt(REPLICATION_STANDBY_HBASE_CLIENT_RETRIES_COUNT,
                        DEFAULT_REPLICATION_STANDBY_HBASE_CLIENT_RETRIES_COUNT));
        this.conf.setLong(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
                this.conf.getLong(REPLICATION_STANDBY_HBASE_CLIENT_OPERATION_TIMEOUT_MS,
                        DEFAULT_REPLICATION_STANDBY_HBASE_CLIENT_OPERATION_TIMEOUT_MS));
    }

    public void processLogFile(FileSystem fs, Path filePath) throws IOException {

        // Map from Table Name to List of Mutations
        Map<TableName, List<Mutation>> tableToMutationsMap = new HashMap<>();

        // Track the total number of processed records from input log file
        long totalProcessed = 0;

        // Track the current batch size as records will be processed in batch size of
        // {@link REPLICATION_STANDBY_LOG_REPLAY_BATCH_SIZE}
        long currentBatchSize = 0;

        // Track the current batch size in bytes
        long currentBatchSizeBytes = 0;

        LogFileReader logFileReader = null;

        long startTime = System.currentTimeMillis();

        try {
            // Create the LogFileReader for given path
            logFileReader = createLogFileReader(fs, filePath);

            if(logFileReader == null) {
                // This is an empty file, assume processed successfully and return
                LOG.warn("Found empty file to process {}", filePath);
                return;
            }

            for (LogFile.Record record : logFileReader) {
                final TableName tableName = TableName.valueOf(record.getHBaseTableName());
                final Mutation mutation = record.getMutation();

                tableToMutationsMap.computeIfAbsent(tableName, k -> new ArrayList<>())
                        .add(mutation);

                // Increment current batch size and current batch size bytes
                currentBatchSize++;
                currentBatchSizeBytes += mutation.heapSize();

                // Process when we reach either the batch count or size limit
                if (currentBatchSize >= getBatchSize()
                        || currentBatchSizeBytes >= getBatchSizeBytes()) {
                    processReplicationLogBatch(tableToMutationsMap);
                    totalProcessed += currentBatchSize;
                    tableToMutationsMap.clear();
                    currentBatchSize = 0;
                    currentBatchSizeBytes = 0;
                }
            }

            // Process any remaining mutations
            if (currentBatchSize > 0) {
                processReplicationLogBatch(tableToMutationsMap);
                totalProcessed += currentBatchSize;
            }

            LOG.info("Completed processing log file {}. Total mutations processed: {}",
                    logFileReader.getContext().getFilePath(), totalProcessed);
            getMetrics().incrementLogFileReplaySuccessCount();
        } catch (Exception e) {
            LOG.error("Error while processing replication log file", e);
            getMetrics().incrementLogFileReplayFailureCount();
            throw new IOException("Failed to process log file " + filePath, e);
        } finally {
            closeReader(logFileReader);
            // Update log file replay time metric
            long endTime = System.currentTimeMillis();
            getMetrics().updateLogFileReplayTime(endTime - startTime);
        }
    }

    /**
     * Creates a LogFileReader for the specified file path.
     * Validates that the file exists and initializes the reader with the given
     * file system and path.
     * @param fs The file system to use for reading
     * @param filePath The path to the log file
     * @return A configured LogFileReader instance
     * @throws IOException if the file doesn't exist or initialization fails
     */
    protected LogFileReader createLogFileReader(FileSystem fs, Path filePath) throws IOException {
        // Ensure that file exists. If we face exception while checking the path itself,
        // method would throw same exception back to the caller
        if (!fs.exists(filePath)) {
            throw new IOException("Log file does not exist: " + filePath);
        }
        LogFileReader logFileReader = new LogFileReader();
        LogFileReaderContext logFileReaderContext = new LogFileReaderContext(conf)
                .setFileSystem(fs).setFilePath(filePath);
        boolean isClosed = isFileClosed(fs, filePath);
        if(isClosed) {
            // As file is closed, ensure that the file has a valid header and trailer
            logFileReader.init(logFileReaderContext);
            return logFileReader;
        } else {
            LOG.warn("Found un-closed file {}. Starting lease recovery.", filePath);
            recoverLease(fs, filePath);
            if (fs.getFileStatus(filePath).getLen() <= 0) {
                // Found empty file, returning null LogReader
                return null;
            }
            try {
                // Acquired the lease, try to create reader with validation both header and trailer
                logFileReader.init(logFileReaderContext);
                return logFileReader;
            } catch (InvalidLogTrailerException invalidLogTrailerException) {
                // If trailer is corrupt (or missing), try to create reader without trailer validation
                LOG.warn("Invalid Trailer for file {}",
                        filePath, invalidLogTrailerException);
                logFileReaderContext.setValidateTrailer(false);
                logFileReader.init(logFileReaderContext);
                return logFileReader;
            } catch (IOException exception) {
                LOG.error("Failed to initialize new LogFileReader for path {}",
                        filePath, exception);
                throw exception;
            }
        }
    }

    protected void recoverLease(final FileSystem fs, final Path filePath) throws IOException {
        RecoverLeaseFSUtils.recoverFileLease(fs, filePath, conf);
    }

    protected boolean isFileClosed(final FileSystem fs, final Path filePath) throws IOException {
        boolean isClosed;
        try {
            isClosed = ((LeaseRecoverable) fs).isFileClosed(filePath);
        } catch (ClassCastException classCastException) {
            // If filesystem is not of type LeaseRecoverable, assume file is always closed
            isClosed = true;
        }
        return isClosed;
    }

    /**
     * Closes the given writer, logging any errors that occur during close.
     */
    protected  void closeReader(LogFileReader logFileReader) {
        if (logFileReader == null) {
            return;
        }
        try {
            logFileReader.close();
        } catch (IOException exception) {
            LOG.error("Error while closing LogFileReader: {}",
                    logFileReader, exception);
        }
    }

    protected void processReplicationLogBatch(
            Map<TableName, List<Mutation>> tableMutationMap) throws IOException {

        if (tableMutationMap == null || tableMutationMap.isEmpty()) {
            return;
        }

        // Track failed operations for retry
        Map<TableName, List<Mutation>> currentOperations = tableMutationMap;
        IOException lastError = null;

        int attempt = 0;

        long startTime = System.currentTimeMillis();

        while (attempt <= batchRetryCount && !currentOperations.isEmpty()) {
            if (attempt > 0) {
                LOG.warn("Retrying failed batch operations, attempt {} of {}",
                        attempt, batchRetryCount);
            }

            try {
                // Apply mutations and get any failed operations
                ApplyMutationBatchResult result = applyMutations(currentOperations);

                // If no failures, we're done
                if (!result.hasFailures()) {
                    return;
                }

                // Update current operations for next retry
                currentOperations = result.getFailedMutations();
                lastError = new IOException("Failed to apply the mutations", result.getException());
            } catch (IOException e) {
                lastError = e;
            }
            attempt++;
            getMetrics().incrementFailedBatchCount();
            // Add delay between retries (exponential backoff)
            if (attempt <= batchRetryCount && !currentOperations.isEmpty()) {
                try {
                    long delayMs = calculateRetryDelay(attempt);
                    Thread.sleep(delayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted during retry delay", e);
                }
            }
        }
        // Update batch replay time metrics
        long endTime = System.currentTimeMillis();
        getMetrics().updateBatchReplayTime(endTime - startTime);

        // If we still have failed operations after all retries, throw the last error
        if (!currentOperations.isEmpty() && lastError != null) {
            LOG.error("Failed to process batch operations after {} retries. Failed tables: {}",
                    batchRetryCount, currentOperations.keySet());
            throw lastError;
        }
    }

    /**
     * Calculates the delay time for retry attempts using exponential backoff.
     * @param attempt The current retry attempt number (0-based)
     * @return The delay time in milliseconds
     */
    protected long calculateRetryDelay(int attempt) {
        return Math.min(1000L * (1L << attempt), maxRetryDelayMs);
    }

    /**
     * Applies mutations to HBase tables and returns any failed operations.
     * @param tableMutationMap Map of table names to their mutations
     * @return ApplyMutationBatchResult containing failed mutations and any exceptions
     * @throws IOException if there's an error applying mutations
     */
    protected ApplyMutationBatchResult applyMutations(
            Map<TableName, List<Mutation>> tableMutationMap) throws IOException {

        if (tableMutationMap == null || tableMutationMap.isEmpty()) {
            return new ApplyMutationBatchResult(Collections.emptyMap(), null);
        }

        Map<TableName, List<Mutation>> failedOperations = new HashMap<>();
        Map<TableName, Future<?>> futures = new HashMap<>();
        Exception lastException = null;

        // Submit batch operations
        for (Map.Entry<TableName, List<Mutation>> entry : tableMutationMap.entrySet()) {
            TableName tableName = entry.getKey();
            List<Mutation> mutations = entry.getValue();
            AsyncTable<?> table = getAsyncConnection()
                    .getTable(tableName, executorService);
            futures.put(tableName, table.batchAll(mutations));
        }

        // Check results and track failures
        for (Map.Entry<TableName, Future<?>> entry : futures.entrySet()) {
            TableName tableName = entry.getKey();
            Future<?> future = entry.getValue();
            try {
                FutureUtils.get(future);
            } catch (IOException e) {
                // Add failed mutations to retry list
                failedOperations.put(tableName, tableMutationMap.get(tableName));
                getMetrics().incrementFailedMutationsCount(tableMutationMap.get(tableName).size());
                LOG.debug("Failed to apply mutations for table {}: {}", tableName, e.getMessage());
                lastException = e;
            }
        }

        return new ApplyMutationBatchResult(failedOperations, lastException);
    }

    /**
     * Return the {@link AsyncConnection} which is used for applying mutations.
     * It ensures to create a new connection ONLY when it's not previously initialized
     * or was closed
     */
    private AsyncConnection getAsyncConnection() throws IOException {
        AsyncConnection existingAsyncConnection = asyncConnection;
        if (existingAsyncConnection == null || existingAsyncConnection.isClosed()) {
            synchronized (this) {
                existingAsyncConnection = asyncConnection;
                if (existingAsyncConnection == null || existingAsyncConnection.isClosed()) {
                    /**
                     * Get the AsyncConnection immediately.
                     */
                    existingAsyncConnection = FutureUtils.get(
                            ConnectionFactory.createAsyncConnection(conf));
                    asyncConnection = existingAsyncConnection;
                }
            }
        }
        return existingAsyncConnection;
    }

    /**
     * Closes the {@link AsyncConnection} and releases all associated resources.
     * @throws IOException if there's an error closing the AsyncConnection
     */
    @Override
    public void close() throws IOException {
        synchronized (this) {
            // Close the async connection
            if (asyncConnection != null && !asyncConnection.isClosed()) {
                asyncConnection.close();
            }
            asyncConnection = null;
            // Shutdown the executor service
            if (executorService != null && !executorService.isShutdown()) {
                executorService.shutdownNow();
            }
            // Remove the instance from cache
            INSTANCES.remove(haGroupName);
        }
    }

    /** Creates a new metrics source for monitoring operations. */
    protected MetricsReplicationLogProcessor createMetricsSource() {
        return new MetricsReplicationLogProcessorImpl(haGroupName);
    }

    /** Returns the metrics source for monitoring replication log operations. */
    public MetricsReplicationLogProcessor getMetrics() {
        return metrics;
    }

    public int getBatchSize() {
        return this.batchSize;
    }

    public long getBatchSizeBytes() {
        return this.batchSizeBytes;
    }

    public int getHBaseClientRetriesCount() {
        return this.conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
                DEFAULT_REPLICATION_STANDBY_HBASE_CLIENT_RETRIES_COUNT);
    }

    public long getHBaseClientOperationTimeout() {
        return this.conf.getLong(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
                DEFAULT_REPLICATION_STANDBY_HBASE_CLIENT_OPERATION_TIMEOUT_MS);
    }

    public int getBatchRetryCount() {
        return this.batchRetryCount;
    }

    public long getMaxRetryDelayMs() {
        return this.maxRetryDelayMs;
    }

    public String getHaGroupName() {
        return this.haGroupName;
    }

    protected ExecutorService getExecutorService() {
        return this.executorService;
    }

    /**
     * Result class for batch mutation operations containing failed mutations and any exceptions.
     */
    protected static class ApplyMutationBatchResult {
        private final Map<TableName, List<Mutation>> failedMutations;
        private final Exception exception;

        public ApplyMutationBatchResult(final Map<TableName, List<Mutation>> failedMutations,
                final Exception exception) {
            this.failedMutations = failedMutations != null
                    ? Collections.unmodifiableMap(failedMutations) : Collections.emptyMap();
            this.exception = exception;
        }

        public Map<TableName, List<Mutation>> getFailedMutations() {
            return failedMutations;
        }

        public Exception getException() {
            return exception;
        }

        public boolean hasFailures() {
            return !failedMutations.isEmpty();
        }
    }
}
