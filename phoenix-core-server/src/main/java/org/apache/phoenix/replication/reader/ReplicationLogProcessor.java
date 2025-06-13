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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.phoenix.replication.log.LogFile;
import org.apache.phoenix.replication.log.LogFileReader;
import org.apache.phoenix.replication.log.LogFileReaderContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationLogProcessor {

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
    public static final int DEFAULT_REPLICATION_STANDBY_HBASE_CLIENT_OPERATION_TIMEOUT_MS = 10000;

    private final Configuration conf;

    private final ExecutorService executorService;

    /**
     * This {@link AsyncConnection} is used for handling mutations
     */
    private volatile AsyncConnection asyncConnection;

    private final Object asyncConnectionLock = new Object();

    private final int batchSize;

    /**
     * Creates a new ReplicationLogProcessor with the given configuration and executor service.
     * @param conf The configuration to use
     * @param executorService The executor service for processing mutations
     * @throws IOException if initialization fails
     */
    public ReplicationLogProcessor(final Configuration conf,
            final ExecutorService executorService) {
        // Create a copy of configuration as some of the properties would be
        // overridden
        this.conf = HBaseConfiguration.create(conf);
        this.executorService = executorService;
        this.batchSize = this.conf.getInt(REPLICATION_STANDBY_LOG_REPLAY_BATCH_SIZE,
                DEFAULT_REPLICATION_STANDBY_LOG_REPLAY_BATCH_SIZE);
        decorateConf();
    }

    /**
     * Decorate the Configuration object to make replication more receptive to delays by
     * reducing the timeout and number of retries.
     */
    private void decorateConf() {
        this.conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
                this.conf.getInt(REPLICATION_STANDBY_HBASE_CLIENT_RETRIES_COUNT,
                        DEFAULT_REPLICATION_STANDBY_HBASE_CLIENT_RETRIES_COUNT));
        this.conf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
                this.conf.getInt(REPLICATION_STANDBY_HBASE_CLIENT_OPERATION_TIMEOUT_MS,
                        DEFAULT_REPLICATION_STANDBY_HBASE_CLIENT_OPERATION_TIMEOUT_MS));
    }

    public void processLogFile(FileSystem fs, Path filePath) throws IOException {

        // Map from Table Name to List of Mutations
        Map<String, List<Mutation>> tableToMutationsMap = new HashMap<>();

        // Track the total number of processed records from input log file
        long totalProcessed = 0;

        // Track the current batch size as records will be processed in batch size of
        // {@link REPLICATION_STANDBY_LOG_REPLAY_BATCH_SIZE}
        long currentBatchSize = 0;

        LogFileReader logFileReader = null;

        try {
            // Create the LogFileReader for given path
            logFileReader = createLogFileReader(fs, filePath);

            for (LogFile.Record record : logFileReader) {

                final String tableName = record.getHBaseTableName();
                final Mutation mutation = record.getMutation();

                tableToMutationsMap.computeIfAbsent(tableName, k -> new ArrayList<>())
                        .add(mutation);
                currentBatchSize++;

                // Process when we reach the batch size and reset the batch size and
                // table to mutations map
                if (currentBatchSize >= getBatchSize()) {
                    processReplicationLogBatch(tableToMutationsMap);
                    totalProcessed += currentBatchSize;
                    tableToMutationsMap.clear();
                    currentBatchSize = 0;
                }
            }

            // Process any remaining mutations
            if (currentBatchSize > 0) {
                processReplicationLogBatch(tableToMutationsMap);
                totalProcessed += currentBatchSize;
            }

            LOG.info("Completed processing log file {}. Total mutations processed: {}",
                    logFileReader.getContext().getFilePath(), totalProcessed);

        } catch (Exception e) {
            LOG.error("Error while processing replication log file", e);
            throw new IOException("Failed to process log file " + filePath, e);
        } finally {
            closeReader(logFileReader);
        }
    }

    protected LogFileReader createLogFileReader(FileSystem fs, Path filePath) throws IOException {
        // Ensure that file exists. If we face exception while checking the path itself,
        // method would throw same exception back to the caller
        if (!fs.exists(filePath)) {
            throw new IOException("Log file does not exist: " + filePath);
        }
        LogFileReader logFileReader = new LogFileReader();
        try {
            LogFileReaderContext logFileReaderContext = new LogFileReaderContext(conf)
                    .setFileSystem(fs).setFilePath(filePath);
            logFileReader.init(logFileReaderContext);
        } catch (IOException exception) {
            LOG.error("Failed to initialize new LogFileReader for path {}",
                    filePath, exception);
            throw exception;
        }
        return logFileReader;
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
            Map<String, List<Mutation>> tableMutationMap) throws IOException {

        if (tableMutationMap == null || tableMutationMap.isEmpty()) {
            return;
        }

        List<Future<?>> futures = new ArrayList<>();
        for (Map.Entry<String, List<Mutation>> entry : tableMutationMap.entrySet()) {
            String tableName = entry.getKey();
            List<Mutation> mutations = entry.getValue();
            AsyncTable<?> table = getAsyncConnection()
                    .getTable(TableName.valueOf(tableName), executorService);
            futures.add(table.batchAll(mutations));
        }

        IOException error = null;

        for (Future<?> future : futures) {
            try {
                FutureUtils.get(future);
            } catch (RetriesExhaustedException e) {
                IOException ioe = e;
                if (error == null) {
                    error = ioe;
                } else {
                    error.addSuppressed(ioe);
                }
            }
        }

        if (error != null) {
            throw error;
        }
    }

    public int getBatchSize() {
        return this.batchSize;
    }

    public int getHBaseClientRetriesCount() {
        return this.conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
                DEFAULT_REPLICATION_STANDBY_HBASE_CLIENT_RETRIES_COUNT);
    }

    public int getHBaseClientOperationTimeout() {
        return this.conf.getInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
                DEFAULT_REPLICATION_STANDBY_HBASE_CLIENT_OPERATION_TIMEOUT_MS);
    }

    /**
     * Return the {@link AsyncConnection} which is used for applying mutations.
     * It ensures to create a new connection ONLY when it's not previously initialized
     * or was closed
     */
    private AsyncConnection getAsyncConnection() throws IOException {
        AsyncConnection asyncConnection = this.asyncConnection;
        if (asyncConnection == null || asyncConnection.isClosed()) {
            synchronized (asyncConnectionLock) {
                asyncConnection = this.asyncConnection;
                if (asyncConnection == null || asyncConnection.isClosed()) {
                    /**
                     * Get the AsyncConnection immediately.
                     */
                    asyncConnection = FutureUtils.get(
                            ConnectionFactory.createAsyncConnection(conf));
                    this.asyncConnection = asyncConnection;
                }
            }
        }
        return asyncConnection;
    }
}
