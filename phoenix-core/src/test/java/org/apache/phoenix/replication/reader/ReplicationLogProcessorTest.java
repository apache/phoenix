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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.replication.log.LogFileReader;
import org.apache.phoenix.replication.log.LogFileReaderContext;
import org.apache.phoenix.replication.log.LogFileTestUtil;
import org.apache.phoenix.replication.log.LogFileWriter;
import org.apache.phoenix.replication.log.LogFileWriterContext;
import org.apache.phoenix.replication.metrics.ReplicationLogProcessorMetricValues;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.StringUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class ReplicationLogProcessorTest extends ParallelStatsDisabledIT {

    private static final String CREATE_TABLE_SQL_STATEMENT = "CREATE TABLE %s (ID VARCHAR PRIMARY KEY, " +
            "COL_1 VARCHAR, COL_2 VARCHAR, COL_3 BIGINT)";

    private static final String UPSERT_SQL_STATEMENT = "upsert into %s values ('%s', '%s', '%s', %s)";

    private static final String PRINCIPAL = "replicationLogProcessor";

    private final String testHAGroupId = "testHAGroupId";

    @ClassRule
    public static TemporaryFolder testFolder = new TemporaryFolder();

    private static Configuration conf;
    private static FileSystem localFs;

    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        conf = getUtility().getConfiguration();
        localFs = FileSystem.getLocal(conf);
    }

    /**
     * Tests successful creation of LogFileReader with a properly formatted log file.
     */
    @Test
    public void testCreateLogFileReaderWithValidLogFile() throws IOException {
        // Test with valid log file
        Path validFilePath = new Path(testFolder.newFile("valid_log_file").toURI());
        String tableName = "T_" + generateUniqueName();

        // Create a valid log file with proper structure and one record
        LogFileWriter writer = initLogFileWriter(validFilePath);

        // Add a mutation to make it a proper log file with data
        Mutation put = LogFileTestUtil.newPut("testRow", 1, 1);
        writer.append(tableName, 1, put);
        writer.sync();
        writer.close();

        // Verify file exists and has content
        assertTrue("Valid log file should exist", localFs.exists(validFilePath));
        assertTrue("Valid log file should have content", localFs.getFileStatus(validFilePath).getLen() > 0);

        // Test createLogFileReader with valid file - should succeed
        ReplicationLogProcessor replicationLogProcessor = new ReplicationLogProcessor(conf, testHAGroupId);
        LogFileReader reader = replicationLogProcessor.createLogFileReader(localFs, validFilePath);

        // Verify reader is created successfully
        assertNotNull("Reader should not be null for valid file", reader);
        assertNotNull("Reader context should not be null", reader.getContext());
        assertEquals("File path should match", validFilePath, reader.getContext().getFilePath());
        assertEquals("File system should match", localFs, reader.getContext().getFileSystem());

        // Verify we can read from the reader
        assertTrue("Reader should have records", reader.iterator().hasNext());

        // Clean up
        reader.close();
        replicationLogProcessor.close();
    }

    /**
     * Tests error handling when attempting to create LogFileReader with a non-existent file.
     */
    @Test
    public void testCreateLogFileReaderWithNonExistentFile() throws IOException {
        Path nonExistentPath = new Path(testFolder.toString(), "non_existent_file");
        ReplicationLogProcessor replicationLogProcessor = new ReplicationLogProcessor(conf, testHAGroupId);
        try {
            replicationLogProcessor.createLogFileReader(localFs, nonExistentPath);
            fail("Should throw IOException for non-existent file");
        } catch (IOException e) {
            assertTrue("Error message should mention file does not exist and file path name",
                    e.getMessage().contains("Log file does not exist: " + nonExistentPath));
        } finally {
            replicationLogProcessor.close();
        }
    }

    /**
     * Tests error handling when attempting to create LogFileReader with an invalid/corrupted file.
     */
    @Test
    public void testCreateLogFileReaderWithInvalidLogFile() throws IOException {
        Path invalidFilePath = new Path(testFolder.newFile("invalid_file").toURI());
        localFs.create(invalidFilePath).close(); // Create empty file
        ReplicationLogProcessor replicationLogProcessor = new ReplicationLogProcessor(conf, testHAGroupId);
        try {
            replicationLogProcessor.createLogFileReader(localFs, invalidFilePath);
            fail("Should throw IOException for invalid file");
        } catch (IOException e) {
            // Should throw some kind of IOException when trying to read header
            assertTrue("Should throw IOException", true);
        } finally {
            // Delete the invalid file
            localFs.delete(invalidFilePath);
            replicationLogProcessor.close();
        }
    }

    /**
     * Tests the closeReader method with both null and valid LogFileReader instances.
     */
    @Test
    public void testCloseReader() throws IOException {
        ReplicationLogProcessor replicationLogProcessor = new ReplicationLogProcessor(conf, testHAGroupId);
        replicationLogProcessor.closeReader(null);
        Path filePath = new Path(testFolder.newFile("testCloseReader").toURI());
        String tableName = "T_" + generateUniqueName();

        // Create a valid log file with proper structure and one record
        LogFileWriter writer = initLogFileWriter(filePath);

        // Add a mutation to make it a proper log file with data
        Mutation put = LogFileTestUtil.newPut("testRow", 1, 1);
        writer.append(tableName, 1, put);
        writer.sync();
        writer.close();

        // Test with valid reader
        LogFileReader reader = Mockito.spy(new LogFileReader());

        reader.init(new LogFileReaderContext(conf)
                .setFileSystem(localFs)
                .setFilePath(filePath));

        replicationLogProcessor.closeReader(reader);
        replicationLogProcessor.close();

        // Ensure reader's close method is called only once
        Mockito.verify(reader, Mockito.times(1)).close();
    }

    /**
     * Tests the calculateRetryDelay method with different configurations.
     */
    @Test
    public void testCalculateRetryDelay() throws IOException {
        // Test with default configuration
        ReplicationLogProcessor replicationLogProcessor = new ReplicationLogProcessor(conf, testHAGroupId);

        // Test exponential backoff pattern with default max delay (10 seconds)
        assertEquals("First retry should have 1 second delay", 1000L, replicationLogProcessor.calculateRetryDelay(0));
        assertEquals("Second retry should have 2 second delay", 2000L, replicationLogProcessor.calculateRetryDelay(1));
        assertEquals("Third retry should have 4 second delay", 4000L, replicationLogProcessor.calculateRetryDelay(2));
        assertEquals("Fourth retry should have 8 second delay", 8000L, replicationLogProcessor.calculateRetryDelay(3));
        assertEquals("Fifth retry should be capped at 10 seconds", 10000L, replicationLogProcessor.calculateRetryDelay(4));
        assertEquals("Sixth retry should be capped at 10 seconds", 10000L, replicationLogProcessor.calculateRetryDelay(5));

        // Clean up
        replicationLogProcessor.close();

        // Test with custom max delay configuration
        Configuration customConf = new Configuration(conf);
        long customMaxDelay = 5000L; // 5 seconds
        customConf.setLong(ReplicationLogProcessor.REPLICATION_STANDBY_BATCH_RETRY_MAX_DELAY_MS, customMaxDelay);

        ReplicationLogProcessor customProcessor = new ReplicationLogProcessor(customConf, testHAGroupId);

        // Test exponential backoff pattern with custom max delay
        assertEquals("First retry should have 1 second delay", 1000L, customProcessor.calculateRetryDelay(0));
        assertEquals("Second retry should have 2 second delay", 2000L, customProcessor.calculateRetryDelay(1));
        assertEquals("Third retry should have 4 second delay", 4000L, customProcessor.calculateRetryDelay(2));
        assertEquals("Fourth retry should be capped at 5 seconds", 5000L, customProcessor.calculateRetryDelay(3));
        assertEquals("Fifth retry should be capped at 5 seconds", 5000L, customProcessor.calculateRetryDelay(4));

        // Clean up
        customProcessor.close();

        // Test with very small max delay
        Configuration smallDelayConf = new Configuration(conf);
        long smallMaxDelay = 1500L; // 1.5 seconds
        smallDelayConf.setLong(ReplicationLogProcessor.REPLICATION_STANDBY_BATCH_RETRY_MAX_DELAY_MS, smallMaxDelay);

        ReplicationLogProcessor smallDelayProcessor = new ReplicationLogProcessor(smallDelayConf, testHAGroupId);

        assertEquals("First retry should have 1 second delay", 1000L, smallDelayProcessor.calculateRetryDelay(0));
        assertEquals("Second retry should be capped at 1.5 seconds", 1500L, smallDelayProcessor.calculateRetryDelay(1));
        assertEquals("Third retry should be capped at 1.5 seconds", 1500L, smallDelayProcessor.calculateRetryDelay(2));

        // Clean up
        smallDelayProcessor.close();
    }

    /**
     * Tests that configuration parameters are properly read and applied.
     */
    @Test
    public void testReplicationLogProcessorConfiguration() throws IOException {
        // Test that all default configurations are used when no custom configuration is provided
        ReplicationLogProcessor replicationLogProcessor = new ReplicationLogProcessor(conf, testHAGroupId);

        // Validate default batch size
        assertEquals("Default batch size should be used",
                ReplicationLogProcessor.DEFAULT_REPLICATION_STANDBY_LOG_REPLAY_BATCH_SIZE,
                replicationLogProcessor.getBatchSize());

        // Validate default HBase client retries count
        assertEquals("Default HBase client retries count should be used",
                ReplicationLogProcessor.DEFAULT_REPLICATION_STANDBY_HBASE_CLIENT_RETRIES_COUNT,
                replicationLogProcessor.getHBaseClientRetriesCount());

        // Validate default HBase client operation timeout
        assertEquals("Default HBase client operation timeout should be used",
                ReplicationLogProcessor.DEFAULT_REPLICATION_STANDBY_HBASE_CLIENT_OPERATION_TIMEOUT_MS,
                replicationLogProcessor.getHBaseClientOperationTimeout());

        // Validate default batch retry count
        assertEquals("Default batch retry count should be used",
                ReplicationLogProcessor.DEFAULT_REPLICATION_STANDBY_BATCH_RETRY_COUNT,
                replicationLogProcessor.getBatchRetryCount());

        // Validate default max retry delay
        assertEquals("Default max retry delay should be used",
                ReplicationLogProcessor.DEFAULT_REPLICATION_STANDBY_BATCH_RETRY_MAX_DELAY_MS,
                replicationLogProcessor.getMaxRetryDelayMs());

        // Validate default thread pool size
        ExecutorService executorService = replicationLogProcessor.getExecutorService();
        assertNotNull("Executor service must not be null", executorService);
        assertEquals("Default thread pool size should be used",
                ReplicationLogProcessor.DEFAULT_REPLICATION_STANDBY_LOG_REPLAY_THREAD_POOL_SIZE,
                ((ThreadPoolExecutor) executorService).getCorePoolSize());

        // Clean up
        replicationLogProcessor.close();

        // Test that all custom configurations are honored
        Configuration customConf = new Configuration(conf);

        // Set custom values for all configuration parameters
        int customBatchSize = 1000;
        int customRetriesCount = 6;
        long customOperationTimeout = 15000L;
        int customBatchRetryCount = 5;
        long customMaxRetryDelay = 20000L;
        int customThreadPoolSize = 10;

        customConf.setInt(ReplicationLogProcessor.REPLICATION_STANDBY_LOG_REPLAY_BATCH_SIZE, customBatchSize);
        customConf.setInt(ReplicationLogProcessor.REPLICATION_STANDBY_HBASE_CLIENT_RETRIES_COUNT, customRetriesCount);
        customConf.setLong(ReplicationLogProcessor.REPLICATION_STANDBY_HBASE_CLIENT_OPERATION_TIMEOUT_MS, customOperationTimeout);
        customConf.setInt(ReplicationLogProcessor.REPLICATION_STANDBY_BATCH_RETRY_COUNT, customBatchRetryCount);
        customConf.setLong(ReplicationLogProcessor.REPLICATION_STANDBY_BATCH_RETRY_MAX_DELAY_MS, customMaxRetryDelay);
        customConf.setInt(ReplicationLogProcessor.REPLICATION_STANDBY_LOG_REPLAY_THREAD_POOL_SIZE, customThreadPoolSize);

        ReplicationLogProcessor customProcessor = new ReplicationLogProcessor(customConf, testHAGroupId);

        // Validate all custom configurations are honored
        assertEquals("Custom batch size should be honored",
                customBatchSize, customProcessor.getBatchSize());

        assertEquals("Custom HBase client retries count should be honored",
                customRetriesCount, customProcessor.getHBaseClientRetriesCount());

        assertEquals("Custom HBase client operation timeout should be honored",
                customOperationTimeout, customProcessor.getHBaseClientOperationTimeout());

        assertEquals("Custom batch retry count should be honored",
                customBatchRetryCount, customProcessor.getBatchRetryCount());

        assertEquals("Custom max retry delay should be honored",
                customMaxRetryDelay, customProcessor.getMaxRetryDelayMs());

        // Validate custom thread pool size
        ExecutorService customExecutorService = customProcessor.getExecutorService();
        assertNotNull("Executor service must not be null", customExecutorService);
        assertEquals("Custom thread pool size should be used",
                customThreadPoolSize, ((ThreadPoolExecutor) customExecutorService).getCorePoolSize());

        // Clean up
        customProcessor.close();
    }

    /**
     * Tests end-to-end processing of a valid log file with mutations for multiple tables.
     */
    @Test
    public void testProcessLogFileForValidLogFile() throws Exception {
        final String table1Name = "T_" + generateUniqueName();
        final String table2Name = "T_" + generateUniqueName();
        final Path filePath = new Path(testFolder.newFile("testProcessLogFileEnd2End").toURI());
        LogFileWriter writer = initLogFileWriter(filePath);
        ReplicationLogProcessor replicationLogProcessor = new ReplicationLogProcessor(conf, testHAGroupId);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(String.format(CREATE_TABLE_SQL_STATEMENT, table1Name));
            conn.createStatement().execute(String.format(CREATE_TABLE_SQL_STATEMENT, table2Name));
            PhoenixConnection phoenixConnection = conn.unwrap(PhoenixConnection.class);

            List<Mutation> table1Mutations = generateHBaseMutations(phoenixConnection, 2, table1Name, 100L, "a");
            List<Mutation> table2Mutations = generateHBaseMutations(phoenixConnection, 5, table2Name, 101L, "b");
            table1Mutations.forEach(mutation -> {
                try {
                    writer.append(table1Name, mutation.hashCode(), mutation);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            table2Mutations.forEach(mutation -> {
                try {
                    writer.append(table2Name, mutation.hashCode(), mutation);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            writer.sync();
            writer.close();

            replicationLogProcessor.processLogFile(localFs, filePath);

            validate(table1Name, table1Mutations);
            validate(table2Name, table2Mutations);

            // Ensure metrics are correctly populated
            ReplicationLogProcessorMetricValues metricValues = replicationLogProcessor.getMetrics().getCurrentMetricValues();
            assertEquals("Invalid log file success count", 1, metricValues.getLogFileReplaySuccessCount());
            assertEquals("There must not be any failed mutations", 0, metricValues.getFailedMutationsCount());
            assertEquals("There must not be any failed files", 0, metricValues.getLogFileReplayFailureCount());

        } finally {
            replicationLogProcessor.close();
        }
    }

    /**
     * Tests error handling when attempting to process a non-existent log file.
     */
    @Test
    public void testProcessLogFileWithNonExistentFile() throws Exception {
        // Create a path to a file that doesn't exist
        Path nonExistentFilePath = new Path(testFolder.getRoot().getAbsolutePath(), "non_existent_log_file.log");
        // Verify the file doesn't exist
        assertFalse("Non-existent file should not exist", localFs.exists(nonExistentFilePath));

        ReplicationLogProcessor replicationLogProcessor = new ReplicationLogProcessor(conf, testHAGroupId);
        // Attempt to process non-existent file - should throw IOException
        try {
            replicationLogProcessor.processLogFile(localFs, nonExistentFilePath);
            fail("Should throw IOException for non-existent file");
        } catch (IOException e) {
            // Expected behavior - non-existent file should cause IOException
            assertTrue("Should throw IOException for non-existent file", true);
            // Ensure metrics are correctly populated
            ReplicationLogProcessorMetricValues metricValues = replicationLogProcessor.getMetrics().getCurrentMetricValues();
            assertEquals("Invalid log file success count", 0, metricValues.getLogFileReplaySuccessCount());
            assertEquals("There must 1 failed replication log file", 1, metricValues.getLogFileReplayFailureCount());
        } finally {
            replicationLogProcessor.close();
        }
    }

    /**
     * Tests processing of empty log files (files with header/trailer but no mutation records).
     */
    @Test
    public void testProcessLogFileWithEmptyFile() throws Exception {
        final Path emptyFilePath = new Path(testFolder.newFile("testProcessLogFileEmpty").toURI());
        LogFileWriter writer = initLogFileWriter(emptyFilePath);

        // Close the writer without adding any records - this creates a valid empty log file
        writer.close();

        // Verify file exists and has some content (header + trailer)
        assertTrue("Empty log file should exist", localFs.exists(emptyFilePath));
        assertTrue("Empty log file should have header/trailer content", localFs.getFileStatus(emptyFilePath).getLen() > 0);

        // Process the empty log file - should not throw any exceptions
        ReplicationLogProcessor replicationLogProcessor = new ReplicationLogProcessor(conf, testHAGroupId);
        try {
            replicationLogProcessor.processLogFile(localFs, emptyFilePath);
            // If we reach here, the empty file was processed successfully
            assertTrue("Processing empty log file should complete without errors", true);
            // Ensure metrics are correctly populated
            ReplicationLogProcessorMetricValues metricValues = replicationLogProcessor.getMetrics().getCurrentMetricValues();
            assertEquals("Invalid log file success count", 1, metricValues.getLogFileReplaySuccessCount());
            assertEquals("There must not be any failed mutations", 0, metricValues.getFailedMutationsCount());
            assertEquals("There must not be any failed files", 0, metricValues.getLogFileReplayFailureCount());
        } catch (Exception e) {
            fail("Processing empty log file should not throw exception: " + e.getMessage());
        } finally {
            replicationLogProcessor.close();
        }
    }

    /**
     * Tests processing of log files that were not closed, ensuring it's successful.
     */
    @Test
    public void testProcessLogFileForUnClosedFile() throws Exception {
        final String tableNameString = "T1_" + generateUniqueName();
        final Path emptyFilePath = new Path(testFolder.newFile("testProcessLogFileForUnClosedFile").toURI());
        LogFileWriter writer = initLogFileWriter(emptyFilePath);

        // Add one mutation
        Mutation put = LogFileTestUtil.newPut("row1", 3L, 4);
        writer.append(tableNameString, 1, put);
        writer.sync();

        ReplicationLogProcessor spyProcessor = Mockito.spy(new ReplicationLogProcessor(conf, testHAGroupId));

        // Create argument captor to capture the actual parameters passed to processReplicationLogBatch
        ArgumentCaptor<Map<TableName, List<Mutation>>> mapCaptor =
                ArgumentCaptor.forClass(Map.class);

        Mockito.doNothing().when(spyProcessor).processReplicationLogBatch(mapCaptor.capture());

        // Process the file without closing - should not throw any exceptions
        spyProcessor.processLogFile(localFs, emptyFilePath);

        // Verify processReplicationLogBatch was called the expected number of times
        Mockito.verify(spyProcessor, Mockito.times(1))
                .processReplicationLogBatch(Mockito.any(Map.class));

        // Validate the captured parameters
        Map<TableName, List<Mutation>> capturedMap = mapCaptor.getValue();
        assertNotNull("Captured map should not be null", capturedMap);
        assertEquals("Should have exactly one table", 1, capturedMap.size());

        // Verify the table name
        TableName expectedTableName = TableName.valueOf(tableNameString);
        assertTrue("Map should contain the expected table", capturedMap.containsKey(expectedTableName));

        // Verify the mutations list
        List<Mutation> mutations = capturedMap.get(expectedTableName);
        assertNotNull("Mutations list should not be null", mutations);
        assertEquals("Should have exactly one mutation", 1, mutations.size());

        // Verify the mutation details
        Mutation capturedMutation = mutations.get(0);
        assertTrue("Mutation should be a Put", capturedMutation instanceof Put);
        LogFileTestUtil.assertMutationEquals("Invalid put", put, capturedMutation);

        // Clean up
        spyProcessor.close();
    }

    /**
     * Tests batching logic with various record counts and batch sizes.
     */
    @Test
    public void testProcessLogFileBatchingLogic() throws Exception {
        // Test multiple batching scenarios to ensure the logic works correctly

        // Test case 1: General case where total records don't align perfectly with batch size
        testProcessLogFileBatching(10, 3);

        // Test case 2: Edge case where total records exactly matches batch size
        testProcessLogFileBatching(5, 5);

        // Test case 3: Single record with large batch size
        testProcessLogFileBatching(1, 10);

        // Test case 4: Multiple full batches
        testProcessLogFileBatching(12, 4);
    }

    /**
     * Tests batching logic when processing log files with mutations for multiple tables.
     */
    @Test
    public void testProcessLogFileWithMultipleTables() throws Exception {
        final Path multiTableBatchFilePath = new Path(testFolder.newFile("testProcessLogFileWithMultipleTables").toURI());
        final String table1Name = "T1_" + generateUniqueName();
        final String table2Name = "T2_" + generateUniqueName();
        final int batchSize = 4;
        final int recordsPerTable = 3;
        final int totalRecords = recordsPerTable * 2; // 6 total records
        final int expectedBatchCalls = (totalRecords + batchSize - 1) / batchSize; // 2 calls

        // Store mutations for validation
        List<Mutation> table1Mutations = new ArrayList<>();
        List<Mutation> table2Mutations = new ArrayList<>();

        // Create log file with mutations for multiple tables
        LogFileWriter writer = initLogFileWriter(multiTableBatchFilePath);

        // Add mutations alternating between tables using LogFileTestUtil
        for (int i = 0; i < recordsPerTable; i++) {
            // Add mutation for table1
            Mutation put1 = LogFileTestUtil.newPut("row1_" + i, (i * 2) + 1, (i * 2) + 1);
            table1Mutations.add(put1);
            writer.append(table1Name, (i * 2) + 1, put1);
            writer.sync();

            // Add mutation for table2
            Mutation put2 = LogFileTestUtil.newPut("row2_" + i, (i * 2) + 2, (i * 2) + 2);
            table2Mutations.add(put2);
            writer.append(table2Name, (i * 2) + 2, put2);
            writer.sync();
        }
        writer.close();

        // Create processor with custom batch size and spy on it
        Configuration testConf = new Configuration(conf);
        testConf.setInt(ReplicationLogProcessor.REPLICATION_STANDBY_LOG_REPLAY_BATCH_SIZE, batchSize);

        ReplicationLogProcessor replicationLogProcessor = new ReplicationLogProcessor(testConf, testHAGroupId);

        // Validate that the batch size is correctly set
        assertEquals("Batch size should be set correctly", batchSize, replicationLogProcessor.getBatchSize());

        ReplicationLogProcessor spyProcessor = Mockito.spy(replicationLogProcessor);

        // Store captured arguments manually to avoid reference issues
        List<Map<TableName, List<Mutation>>> capturedArguments = new ArrayList<>();

        // Mock processReplicationLogBatch to capture deep copies
        Mockito.doAnswer(invocation -> {
            // Capture deep copy of mutations
            Map<TableName, List<Mutation>> originalMap = invocation.getArgument(0);
            Map<TableName, List<Mutation>> deepCopy = new HashMap<>(originalMap);
            capturedArguments.add(deepCopy);
            return null;
        }).when(spyProcessor).processReplicationLogBatch(Mockito.any(Map.class));

        // Process the log file
        spyProcessor.processLogFile(localFs, multiTableBatchFilePath);

        // Verify processReplicationLogBatch was called the expected number of times
        Mockito.verify(spyProcessor, Mockito.times(expectedBatchCalls))
                .processReplicationLogBatch(Mockito.any(Map.class));

        // Validate the captured parameters using our manually captured arguments
        assertEquals("Should have captured " + expectedBatchCalls + " batch calls",
                expectedBatchCalls, capturedArguments.size());

        // Validate each batch call individually
        TableName expectedTable1Name = TableName.valueOf(table1Name);
        TableName expectedTable2Name = TableName.valueOf(table2Name);

        // First batch should contain 4 mutations (batch size = 4)
        // Based on alternating pattern: table1[0], table2[0], table1[1], table2[1]
        Map<TableName, List<Mutation>> firstBatch = capturedArguments.get(0);
        assertNotNull("First batch should not be null", firstBatch);

        // Validate first batch contains both tables
        assertTrue("First batch should contain table1", firstBatch.containsKey(expectedTable1Name));
        assertTrue("First batch should contain table2", firstBatch.containsKey(expectedTable2Name));

        List<Mutation> firstBatchTable1 = firstBatch.get(expectedTable1Name);
        List<Mutation> firstBatchTable2 = firstBatch.get(expectedTable2Name);

        assertNotNull("First batch table1 mutations should not be null", firstBatchTable1);
        assertNotNull("First batch table2 mutations should not be null", firstBatchTable2);

        // Validate first batch mutation counts
        assertEquals("First batch should have 2 mutations for table1", 2, firstBatchTable1.size());
        assertEquals("First batch should have 2 mutations for table2", 2, firstBatchTable2.size());

        // Validate first batch mutation content
        LogFileTestUtil.assertMutationEquals("First batch table1 mutation 0 mismatch",
                table1Mutations.get(0), firstBatchTable1.get(0));
        LogFileTestUtil.assertMutationEquals("First batch table1 mutation 1 mismatch",
                table1Mutations.get(1), firstBatchTable1.get(1));
        LogFileTestUtil.assertMutationEquals("First batch table2 mutation 0 mismatch",
                table2Mutations.get(0), firstBatchTable2.get(0));
        LogFileTestUtil.assertMutationEquals("First batch table2 mutation 1 mismatch",
                table2Mutations.get(1), firstBatchTable2.get(1));

        // Second batch should contain 2 mutations (remaining records)
        // Based on alternating pattern: table1[2], table2[2]
        Map<TableName, List<Mutation>> secondBatch = capturedArguments.get(1);
        assertNotNull("Second batch should not be null", secondBatch);

        // Validate second batch contains both tables
        assertTrue("Second batch should contain table1", secondBatch.containsKey(expectedTable1Name));
        assertTrue("Second batch should contain table2", secondBatch.containsKey(expectedTable2Name));

        List<Mutation> secondBatchTable1 = secondBatch.get(expectedTable1Name);
        List<Mutation> secondBatchTable2 = secondBatch.get(expectedTable2Name);

        assertNotNull("Second batch table1 mutations should not be null", secondBatchTable1);
        assertNotNull("Second batch table2 mutations should not be null", secondBatchTable2);

        // Validate second batch mutation counts
        assertEquals("Second batch should have 1 mutation for table1", 1, secondBatchTable1.size());
        assertEquals("Second batch should have 1 mutation for table2", 1, secondBatchTable2.size());

        // Validate second batch mutation content
        LogFileTestUtil.assertMutationEquals("Second batch table1 mutation 0 mismatch",
                table1Mutations.get(2), secondBatchTable1.get(0));
        LogFileTestUtil.assertMutationEquals("Second batch table2 mutation 0 mismatch",
                table2Mutations.get(2), secondBatchTable2.get(0));

        // Ensure metrics are correctly populated
        ReplicationLogProcessorMetricValues metricValues = spyProcessor.getMetrics().getCurrentMetricValues();
        assertEquals("Invalid log file success count", 1, metricValues.getLogFileReplaySuccessCount());
        assertEquals("There must not be any failed mutations", 0, metricValues.getFailedMutationsCount());
        assertEquals("There must not be any failed files", 0, metricValues.getLogFileReplayFailureCount());

        // Clean up
        spyProcessor.close();
    }

    /**
     * Tests processing an empty mutation map - should complete without errors.
     */
    @Test
    public void testApplyMutationsWithEmptyMap() throws IOException {
        // Test with empty map - should not throw any exception
        Map<TableName, List<Mutation>> emptyMap = new HashMap<>();

        ReplicationLogProcessor replicationLogProcessor = new ReplicationLogProcessor(conf, testHAGroupId);
        try {
            ReplicationLogProcessor.ApplyMutationBatchResult applyMutationBatchResult = replicationLogProcessor.applyMutations(emptyMap);
            assertNotNull("Apply mutations result must not be null", applyMutationBatchResult);
            assertNull("Apply mutations result exception should be null", applyMutationBatchResult.getException());
            Map<TableName, List<Mutation>> failedMutations = applyMutationBatchResult.getFailedMutations();
            assertNotNull("Failed mutations must not be null", failedMutations);
            assertTrue("Failed mutations must be empty", failedMutations.isEmpty());
            // Should not throw any exception
        } catch (Exception e) {
            fail("Should not throw exception for empty map: " + e.getMessage());
        } finally {
            replicationLogProcessor.close();
        }
    }

    /**
     * Tests applyMutations method with three tables where two succeed and one fails.
     * This test verifies that the method properly handles partial failure scenarios
     * and returns only the failed mutations in the result map.
     */
    @Test
    public void testApplyMutationsWithPartialFailures() throws Exception {
        final String table1 = "T_" + generateUniqueName();
        final String table2 = "T_" + generateUniqueName();
        final String table3 = "T_" + generateUniqueName();
        final TableName tableName1 = TableName.valueOf(table1);
        final TableName tableName2 = TableName.valueOf(table2);
        final TableName tableName3 = TableName.valueOf(table3);
        Map<TableName, List<Mutation>> tableMutationsMap = new HashMap<>();
        ReplicationLogProcessor replicationLogProcessor = new ReplicationLogProcessor(conf, testHAGroupId);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            // Create first table (will succeed)
            conn.createStatement().execute(String.format(CREATE_TABLE_SQL_STATEMENT, table1));
            // Create second table (will succeed)
            conn.createStatement().execute(String.format(CREATE_TABLE_SQL_STATEMENT, table2));
            // Create third table (will be disabled to simulate failure)
            conn.createStatement().execute(String.format(CREATE_TABLE_SQL_STATEMENT, table3));

            PhoenixConnection phoenixConnection = conn.unwrap(PhoenixConnection.class);

            // Generate mutations for the first table (will succeed)
            List<Mutation> mutations1 = generateHBaseMutations(phoenixConnection, 2, table1, 10L, "a");
            tableMutationsMap.put(tableName1, mutations1);

            // Generate mutations for the second table (will succeed)
            List<Mutation> mutations2 = generateHBaseMutations(phoenixConnection, 2, table2, 20L, "b");
            tableMutationsMap.put(tableName2, mutations2);

            // Generate mutations for the third table (will fail)
            List<Mutation> mutations3 = generateHBaseMutations(phoenixConnection, 2, table3, 30L, "c");
            tableMutationsMap.put(tableName3, mutations3);

            // Disable the third table to simulate failure
            Admin admin = phoenixConnection.getQueryServices().getAdmin();
            admin.disableTable(tableName3);

            // Apply mutations - should have partial failures
            ReplicationLogProcessor.ApplyMutationBatchResult applyMutationBatchResult = replicationLogProcessor.applyMutations(tableMutationsMap);
            assertNotNull("Apply mutations result must not be null", applyMutationBatchResult);
            assertNotNull("Apply mutations result exception must not be null", applyMutationBatchResult.getException());
            assertTrue("Invalid exception message", applyMutationBatchResult.getException().getMessage().contains(NotServingRegionException.class.getSimpleName()));
            Map<TableName, List<Mutation>> failedMutations = applyMutationBatchResult.getFailedMutations();

            // Verify failed mutations map is not null and contains the failed table
            assertNotNull("Failed mutations map should not be null", failedMutations);
            assertFalse("Some mutations should have failed", failedMutations.isEmpty());

            // Verify that table3 mutations failed
            assertTrue("Table3 mutations should be in failed mutations",
                    failedMutations.containsKey(tableName3));
            assertEquals("Table3 should have all its mutations failed",
                    mutations3.size(), failedMutations.get(tableName3).size());
            for(int i = 0; i < mutations3.size(); i++) {
                LogFileTestUtil.assertMutationEquals("Mutations modified by applyMutations method",
                        mutations3.get(i), failedMutations.get(TableName.valueOf(table3)).get(i));
            }

            // Verify that table1 and table2 mutations succeeded (not in failed mutations)
            assertFalse("Table1 mutations should not be in failed mutations",
                    failedMutations.containsKey(tableName1));
            assertFalse("Table2 mutations should not be in failed mutations",
                    failedMutations.containsKey(tableName2));

            // Verify mutations were actually applied to the successful tables
            validate(table1, mutations1);
            validate(table2, mutations2);

            // Ensure metrics are correctly populated
            ReplicationLogProcessorMetricValues metricValues = replicationLogProcessor.getMetrics().getCurrentMetricValues();
            assertEquals("Failed mutations count should match table3 mutations", mutations3.size(), metricValues.getFailedMutationsCount());
        } finally {
            replicationLogProcessor.close();
        }
    }

    /**
     * Tests applyMutations method with two tables that both succeed in one go.
     * This test verifies that the method can handle multiple tables successfully
     * and returns an empty failed mutations map.
     */
    @Test
    public void testApplyMutationsSuccess() throws Exception {
        final String table1 = "T_" + generateUniqueName();
        final String table2 = "T_" + generateUniqueName();
        Map<TableName, List<Mutation>> tableMutationsMap = new HashMap<>();
        // Create processor and apply mutations
        ReplicationLogProcessor replicationLogProcessor = new ReplicationLogProcessor(conf, testHAGroupId);

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            // Create first table
            conn.createStatement().execute(String.format(CREATE_TABLE_SQL_STATEMENT, table1));
            // Create second table
            conn.createStatement().execute(String.format(CREATE_TABLE_SQL_STATEMENT, table2));

            PhoenixConnection phoenixConnection = conn.unwrap(PhoenixConnection.class);

            // Generate mutations for the first table
            List<Mutation> mutations1 = generateHBaseMutations(phoenixConnection, 3, table1, 10L, "a");
            tableMutationsMap.put(TableName.valueOf(table1), mutations1);

            // Generate mutations for the second table
            List<Mutation> mutations2 = generateHBaseMutations(phoenixConnection, 2, table2, 20L, "b");
            tableMutationsMap.put(TableName.valueOf(table2), mutations2);

            // Apply mutations - should succeed for both tables
            ReplicationLogProcessor.ApplyMutationBatchResult applyMutationBatchResult = replicationLogProcessor.applyMutations(tableMutationsMap);
            assertNotNull("Apply mutations result must not be null", applyMutationBatchResult);
            assertNull("Apply mutations result exception should be null", applyMutationBatchResult.getException());
            Map<TableName, List<Mutation>> failedMutations = applyMutationBatchResult.getFailedMutations();

            // Verify no mutations failed
            assertNotNull("Failed mutations map should not be null", failedMutations);
            assertTrue("No mutations should have failed", failedMutations.isEmpty());

            // Verify that table1 mutations were applied successfully
            validate(table1, mutations1);

            // Verify that table2 mutations were applied successfully
            validate(table2, mutations2);

            // Ensure metrics are correctly populated
            ReplicationLogProcessorMetricValues metricValues = replicationLogProcessor.getMetrics().getCurrentMetricValues();
            assertEquals("Failed mutations count must be 0", 0, metricValues.getFailedMutationsCount());
        } finally {
            replicationLogProcessor.close();
        }
    }

    /**
     * Tests applyMutations method with two tables that both fail.
     * This test verifies that the method properly handles complete failure scenarios
     * and returns all mutations in the failed mutations map.
     */
    @Test
    public void testApplyMutationsFailure() throws Exception {
        final String table1 = "T_" + generateUniqueName();
        final String table2 = "T_" + generateUniqueName();
        Map<TableName, List<Mutation>> tableMutationsMap = new HashMap<>();
        ReplicationLogProcessor replicationLogProcessor = new ReplicationLogProcessor(conf, testHAGroupId);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            // Create first table
            conn.createStatement().execute(String.format(CREATE_TABLE_SQL_STATEMENT, table1));
            // Create second table
            conn.createStatement().execute(String.format(CREATE_TABLE_SQL_STATEMENT, table2));

            PhoenixConnection phoenixConnection = conn.unwrap(PhoenixConnection.class);

            // Generate mutations for the first table
            List<Mutation> table1Mutations = generateHBaseMutations(phoenixConnection, 2, table1, 10L, "a");
            tableMutationsMap.put(TableName.valueOf(table1), table1Mutations);

            // Generate mutations for the second table
            List<Mutation> table2Mutations = generateHBaseMutations(phoenixConnection, 3, table2, 20L, "b");
            tableMutationsMap.put(TableName.valueOf(table2), table2Mutations);

            // Disable regions for both tables to simulate complete failure
            Admin admin = phoenixConnection.getQueryServices().getAdmin();

            // Disable region for table1
            List<HRegionInfo> regions1 = admin.getTableRegions(TableName.valueOf(table1));
            if (!regions1.isEmpty()) {
                admin.unassign(regions1.get(0).getRegionName(), true);
            }

            // Disable region for table2
            List<HRegionInfo> regions2 = admin.getTableRegions(TableName.valueOf(table2));
            if (!regions2.isEmpty()) {
                admin.unassign(regions2.get(0).getRegionName(), true);
            }

            // Apply mutations - should fail for both tables
            ReplicationLogProcessor.ApplyMutationBatchResult applyMutationBatchResult = replicationLogProcessor.applyMutations(tableMutationsMap);
            assertNotNull("Apply mutations result must not be null", applyMutationBatchResult);
            assertNotNull("Apply mutations result exception must not be null", applyMutationBatchResult.getException());
            assertTrue("Invalid exception message", applyMutationBatchResult.getException().getMessage().contains(NotServingRegionException.class.getSimpleName()));
            Map<TableName, List<Mutation>> failedMutations = applyMutationBatchResult.getFailedMutations();

            // Verify failed mutations map contains both tables
            assertNotNull("Failed mutations map should not be null", failedMutations);
            assertFalse("Some mutations should have failed", failedMutations.isEmpty());
            assertEquals("Should have 2 failed tables", 2, failedMutations.size());

            // Verify that table1 mutations failed
            assertTrue("Table1 mutations should be in failed mutations",
                    failedMutations.containsKey(TableName.valueOf(table1)));
            assertEquals("Table1 should have all its mutations failed",
                    table1Mutations.size(), failedMutations.get(TableName.valueOf(table1)).size());
            for(int i = 0; i < table1Mutations.size(); i++) {
                LogFileTestUtil.assertMutationEquals("Table1 mutation mismatch",
                        table1Mutations.get(i), failedMutations.get(TableName.valueOf(table1)).get(i));
            }

            // Verify that table2 mutations failed
            assertTrue("Table2 mutations should be in failed mutations",
                    failedMutations.containsKey(TableName.valueOf(table2)));
            assertEquals("Table2 should have all its mutations failed",
                    table2Mutations.size(), failedMutations.get(TableName.valueOf(table2)).size());
            for(int i = 0; i < table2Mutations.size(); i++) {
                LogFileTestUtil.assertMutationEquals("Table2 mutation mismatch",
                        table2Mutations.get(i), failedMutations.get(TableName.valueOf(table2)).get(i));
            }

            // Ensure metrics are correctly populated
            ReplicationLogProcessorMetricValues metricValues = replicationLogProcessor.getMetrics().getCurrentMetricValues();
            assertEquals("Failed mutations count mismatch", table1Mutations.size() + table2Mutations.size(), metricValues.getFailedMutationsCount());
        } finally {
            replicationLogProcessor.close();
        }
    }

    /**
     * Tests processReplicationLogBatch method with multiple tables where all mutations succeed in one go.
     */
    @Test
    public void testProcessReplicationLogBatchWithMultipleTablesSuccess() throws Exception {
        final String table1 = "T_" + generateUniqueName();
        final String table2 = "T_" + generateUniqueName();
        Map<TableName, List<Mutation>> tableMutationsMap = new HashMap<>();

        // Create test mutations for multiple tables
        List<Mutation> mutations1 = new ArrayList<>();
        mutations1.add(LogFileTestUtil.newPut("row1_table1", 1L, 1));
        mutations1.add(LogFileTestUtil.newPut("row2_table1", 2L, 2));
        tableMutationsMap.put(TableName.valueOf(table1), mutations1);

        List<Mutation> mutations2 = new ArrayList<>();
        mutations2.add(LogFileTestUtil.newPut("row1_table2", 3L, 3));
        mutations2.add(LogFileTestUtil.newDelete("row2_table2", 4L, 4));
        tableMutationsMap.put(TableName.valueOf(table2), mutations2);

        // Create processor and spy on it
        ReplicationLogProcessor replicationLogProcessor = new ReplicationLogProcessor(conf, testHAGroupId);
        ReplicationLogProcessor spyProcessor = Mockito.spy(replicationLogProcessor);

        // Mock applyMutations to return empty failed mutations map (all succeed)
        ReplicationLogProcessor.ApplyMutationBatchResult successApplyMutationsResult = new ReplicationLogProcessor.ApplyMutationBatchResult(Collections.emptyMap(), null);
        Mockito.doReturn(successApplyMutationsResult).when(spyProcessor).applyMutations(Mockito.anyMap());

        // Call processReplicationLogBatch - should succeed without retries
        spyProcessor.processReplicationLogBatch(tableMutationsMap);

        // Verify applyMutations was called exactly once with the correct parameters
        Mockito.verify(spyProcessor, Mockito.times(1)).applyMutations(Mockito.anyMap());

        // Capture the argument passed to applyMutations
        ArgumentCaptor<Map<TableName, List<Mutation>>> argumentCaptor = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(spyProcessor).applyMutations(argumentCaptor.capture());

        // Verify the captured argument contains both tables with correct mutations
        Map<TableName, List<Mutation>> capturedMap = argumentCaptor.getValue();
        assertNotNull("Captured map should not be null", capturedMap);
        assertEquals("Should have 2 tables", 2, capturedMap.size());

        // Verify table1 mutations
        assertTrue("Should contain table1", capturedMap.containsKey(TableName.valueOf(table1)));
        List<Mutation> capturedMutations1 = capturedMap.get(TableName.valueOf(table1));
        assertEquals("Table1 should have 2 mutations", 2, capturedMutations1.size());
        LogFileTestUtil.assertMutationEquals("Table1 mutation 1 mismatch", mutations1.get(0), capturedMutations1.get(0));
        LogFileTestUtil.assertMutationEquals("Table1 mutation 2 mismatch", mutations1.get(1), capturedMutations1.get(1));

        // Verify table2 mutations
        assertTrue("Should contain table2", capturedMap.containsKey(TableName.valueOf(table2)));
        List<Mutation> capturedMutations2 = capturedMap.get(TableName.valueOf(table2));
        assertEquals("Table2 should have 2 mutations", 2, capturedMutations2.size());
        LogFileTestUtil.assertMutationEquals("Table2 mutation 1 mismatch", mutations2.get(0), capturedMutations2.get(0));
        LogFileTestUtil.assertMutationEquals("Table2 mutation 2 mismatch", mutations2.get(1), capturedMutations2.get(1));

        // Clean up
        spyProcessor.close();
    }

    /**
     * Tests processReplicationLogBatch method with persistent failure for one table.
     * One table's region is brought down to simulate persistent failure while another table succeeds.
     * This tests the retry logic and failure handling behavior.
     */
    @Test
    public void testProcessReplicationLogBatchWithPersistentFailure() throws Exception {
        final String table1 = "T_" + generateUniqueName();
        final String table2 = "T_" + generateUniqueName();
        Map<TableName, List<Mutation>> tableMutationsMap = new HashMap<>();

        // Create processor and spy on it
        ReplicationLogProcessor replicationLogProcessor = new ReplicationLogProcessor(conf, testHAGroupId);
        ReplicationLogProcessor spyProcessor = Mockito.spy(replicationLogProcessor);

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            // Create first table (will succeed)
            conn.createStatement().execute(String.format(CREATE_TABLE_SQL_STATEMENT, table1));
            // Create second table (will have region down to simulate failure)
            conn.createStatement().execute(String.format(CREATE_TABLE_SQL_STATEMENT, table2));

            PhoenixConnection phoenixConnection = conn.unwrap(PhoenixConnection.class);

            // Generate mutations for the first table (will succeed)
            List<Mutation> table1Mutations = generateHBaseMutations(phoenixConnection, 2, table1, 10L, "a");
            tableMutationsMap.put(TableName.valueOf(table1), table1Mutations);

            // Generate mutations for the second table (will fail due to region down)
            List<Mutation> table2Mutations = generateHBaseMutations(phoenixConnection, 2, table2, 20L, "b");
            tableMutationsMap.put(TableName.valueOf(table2), table2Mutations);

            // Bring down a region for the second table to simulate persistent failure
            Admin admin = phoenixConnection.getQueryServices().getAdmin();
            TableName table2TableName = TableName.valueOf(table2);

            // Get regions for the table and disable one of them
            List<HRegionInfo> regions = admin.getTableRegions(table2TableName);
            if (!regions.isEmpty()) {
                // Disable the first region to simulate failure
                admin.unassign(regions.get(0).getRegionName(), true);
            }

            // Capture all calls to applyMutations
            List<Map<TableName, List<Mutation>>> capturedCalls = new ArrayList<>();
            Mockito.doAnswer(invocation -> {
                Map<TableName, List<Mutation>> originalMap = invocation.getArgument(0);
                Map<TableName, List<Mutation>> deepCopy = new HashMap<>(originalMap);
                capturedCalls.add(deepCopy);
                return invocation.callRealMethod();
            }).when(spyProcessor).applyMutations(Mockito.anyMap());

            try {
                spyProcessor.processReplicationLogBatch(tableMutationsMap);
                fail("Should throw IOException for non-existent file");
            } catch (IOException e) {
                assertTrue("IOException must be thrown due to persistent failures", true);
            }


            // Get the expected number of retries from configuration
            int maxRetries = conf.getInt(ReplicationLogProcessor.REPLICATION_STANDBY_BATCH_RETRY_COUNT,
                    ReplicationLogProcessor.DEFAULT_REPLICATION_STANDBY_BATCH_RETRY_COUNT);
            int expectedCalls = maxRetries + 1; // Initial call + retries

            // Verify the exact number of retries
            Mockito.verify(spyProcessor, Mockito.times(expectedCalls)).applyMutations(Mockito.anyMap());
            assertEquals("Should have made " + expectedCalls + " calls to applyMutations", expectedCalls, capturedCalls.size());

            // First call should contain both tables
            Map<TableName, List<Mutation>> call = capturedCalls.get(0);
            assertEquals("First call should have 2 tables", 2, call.size());

            // Verify table1 mutations are present and correct
            assertTrue("First call should contain table1", call.containsKey(TableName.valueOf(table1)));
            List<Mutation> callMutations1 = call.get(TableName.valueOf(table1));
            assertEquals("Mismatch in number of mutations for table1", table1Mutations.size(), callMutations1.size());
            for(int i = 0; i<table1Mutations.size(); i++) {
                LogFileTestUtil.assertMutationEquals("Mutation mismatch for table1", table1Mutations.get(i), callMutations1.get(i));
            }

            // Verify table2 mutations are present and correct
            assertTrue("First call should contain table2", call.containsKey(TableName.valueOf(table2)));
            List<Mutation> callMutations2 = call.get(TableName.valueOf(table2));
            assertEquals("Mismatch in number of mutations for table2", table2Mutations.size(), callMutations2.size());
            for(int mutationIndex = 0; mutationIndex<table1Mutations.size(); mutationIndex++) {
                LogFileTestUtil.assertMutationEquals("Mutation mismatch for table2", table2Mutations.get(mutationIndex), callMutations2.get(mutationIndex));
            }

            // Verify that retry calls should contain only table2 (the failed table)
            for (int callIndex = 1; callIndex < capturedCalls.size(); callIndex++) {
                call = capturedCalls.get(callIndex);
                assertNotNull("Call " + callIndex + " should not be null", call);
                assertEquals("Retry call " + callIndex + " should have 1 table", 1, call.size());

                // Verify table1 is NOT present in retry calls
                assertFalse("Retry call " + callIndex + " should not contain table1",
                        call.containsKey(TableName.valueOf(table1)));

                // Verify table2 mutations are present and correct
                assertTrue("First call should contain table2", call.containsKey(TableName.valueOf(table2)));
                callMutations2 = call.get(TableName.valueOf(table2));
                assertEquals("Mismatch in number of mutations for table2", table2Mutations.size(), callMutations2.size());
                for(int mutationIndex = 0; mutationIndex<table1Mutations.size(); mutationIndex++) {
                    LogFileTestUtil.assertMutationEquals("Mutation mismatch for table2", table2Mutations.get(mutationIndex), callMutations2.get(mutationIndex));
                }

                // Ensure metrics are correctly populated
                ReplicationLogProcessorMetricValues metricValues = spyProcessor.getMetrics().getCurrentMetricValues();
                assertEquals("Invalid failed mutations count", table2Mutations.size() * 3, metricValues.getFailedMutationsCount());
            }
        } finally {
            spyProcessor.close();
        }
    }

    /**
     * Tests processReplicationLogBatch method with intermittent failure where a region comes back up during retries.
     * This test verifies that the retry logic works correctly when failures are temporary and eventually resolve.
     */
    @Test
    public void testProcessReplicationLogBatchWithIntermittentFailure() throws Exception {
        final String table1 = "T_" + generateUniqueName();
        final String table2 = "T_" + generateUniqueName();
        Map<TableName, List<Mutation>> tableMutationsMap = new HashMap<>();

        // Create processor and spy on it
        ReplicationLogProcessor replicationLogProcessor = new ReplicationLogProcessor(conf, testHAGroupId);
        ReplicationLogProcessor spyProcessor = Mockito.spy(replicationLogProcessor);

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            // Create first table (will succeed)
            conn.createStatement().execute(String.format(CREATE_TABLE_SQL_STATEMENT, table1));
            // Create second table (will have intermittent failure)
            conn.createStatement().execute(String.format(CREATE_TABLE_SQL_STATEMENT, table2));

            PhoenixConnection phoenixConnection = conn.unwrap(PhoenixConnection.class);

            // Generate mutations for the first table (will succeed)
            List<Mutation> mutations1 = generateHBaseMutations(phoenixConnection, 2, table1, 10L, "a");
            tableMutationsMap.put(TableName.valueOf(table1), mutations1);

            // Generate mutations for the second table (will have intermittent failure)
            List<Mutation> mutations2 = generateHBaseMutations(phoenixConnection, 2, table2, 20L, "b");
            tableMutationsMap.put(TableName.valueOf(table2), mutations2);

            // Verify table2 has exactly 1 region
            Admin admin = phoenixConnection.getQueryServices().getAdmin();
            TableName tableName2TableName = TableName.valueOf(table2);
            List<HRegionInfo> regions = admin.getTableRegions(tableName2TableName);
            assertEquals("Table2 should have exactly 1 region", 1, regions.size());

            // Bring down the region to simulate intermittent failure
            HRegionInfo regionToDisable = regions.get(0);
            admin.unassign(regionToDisable.getRegionName(), true);

            // Capture all calls to applyMutations
            List<Map<TableName, List<Mutation>>> capturedCalls = new ArrayList<>();
            AtomicInteger callCount = new AtomicInteger(0);

            Mockito.doAnswer(invocation -> {
                Map<TableName, List<Mutation>> originalMap = invocation.getArgument(0);
                Map<TableName, List<Mutation>> deepCopy = new HashMap<>(originalMap);
                capturedCalls.add(deepCopy);

                int currentCall = callCount.incrementAndGet();

                // After 2 retries, bring the region back up
                if (currentCall == 3 && regionToDisable != null) {
                    try {
                        admin.assign(regionToDisable.getRegionName());
                    } catch (Exception e) {
                        // Ignore if region is already assigned
                    }
                }

                // Call the real applyMutations method
                return invocation.callRealMethod();
            }).when(spyProcessor).applyMutations(Mockito.anyMap());

            try {
                spyProcessor.processReplicationLogBatch(tableMutationsMap);
                // Should succeed after retries
            } catch (IOException e) {
                fail("Should not throw IOException as mutations should eventually succeed");
            }

            // Expected calls: 1 initial + 2 retries (since table2 succeeds on 3rd attempt)
            int expectedCalls = 3;

            // Verify the exact number of calls
            Mockito.verify(spyProcessor, Mockito.times(expectedCalls)).applyMutations(Mockito.anyMap());
            assertEquals("Should have made " + expectedCalls + " calls to applyMutations", expectedCalls, capturedCalls.size());

            // First call should contain both tables
            Map<TableName, List<Mutation>> firstCall = capturedCalls.get(0);
            assertEquals("First call should have 2 tables", 2, firstCall.size());

            // Verify table1 mutations are present and correct in first call
            assertTrue("First call should contain table1", firstCall.containsKey(TableName.valueOf(table1)));
            List<Mutation> firstCallMutations1 = firstCall.get(TableName.valueOf(table1));
            assertEquals("Mismatch in number of mutations for table1", mutations1.size(), firstCallMutations1.size());
            for(int i = 0; i < mutations1.size(); i++) {
                LogFileTestUtil.assertMutationEquals("Mutation mismatch for table1", mutations1.get(i), firstCallMutations1.get(i));
            }

            // Verify table2 mutations are present and correct in first call
            assertTrue("First call should contain table2", firstCall.containsKey(TableName.valueOf(table2)));
            List<Mutation> firstCallMutations2 = firstCall.get(TableName.valueOf(table2));
            assertEquals("Mismatch in number of mutations for table2", mutations2.size(), firstCallMutations2.size());
            for(int i = 0; i < mutations2.size(); i++) {
                LogFileTestUtil.assertMutationEquals("Mutation mismatch for table2", mutations2.get(i), firstCallMutations2.get(i));
            }

            // Verify that retry calls (1 and 2) should contain only table2 (the failed table)
            for (int callIndex = 1; callIndex < 3; callIndex++) {
                Map<TableName, List<Mutation>> retryCall = capturedCalls.get(callIndex);
                assertNotNull("Call " + callIndex + " should not be null", retryCall);
                assertEquals("Retry call " + callIndex + " should have 1 table", 1, retryCall.size());

                // Verify table1 is NOT present in retry calls
                assertFalse("Retry call " + callIndex + " should not contain table1",
                        retryCall.containsKey(TableName.valueOf(table1)));

                // Verify table2 mutations are present and correct
                assertTrue("Retry call " + callIndex + " should contain table2", retryCall.containsKey(TableName.valueOf(table2)));
                List<Mutation> retryCallMutations2 = retryCall.get(TableName.valueOf(table2));
                assertEquals("Mismatch in number of mutations for table2", mutations2.size(), retryCallMutations2.size());
                for(int i = 0; i < mutations2.size(); i++) {
                    LogFileTestUtil.assertMutationEquals("Mutation mismatch for table2", mutations2.get(i), retryCallMutations2.get(i));
                }
            }

            // Verify that table1 mutations were applied successfully
            validate(table1, mutations1);

            // Verify that table2 mutations were eventually applied successfully
            validate(table2, mutations2);

            // Ensure metrics are correctly populated
            ReplicationLogProcessorMetricValues metricValues = spyProcessor.getMetrics().getCurrentMetricValues();
            assertEquals("Invalid failed mutations count", mutations2.size() * 2, metricValues.getFailedMutationsCount());
        } finally {
            spyProcessor.close();
        }
    }

    /**
     * Helper method to test batching scenarios with different record counts and batch sizes
     */
    private void testProcessLogFileBatching(int totalRecords, int batchSize) throws Exception {
        final Path batchTestFilePath = new Path(testFolder.newFile("test_" + new Random(1000)).toURI());
        final String tableName = "T_" + generateUniqueName();
        final int expectedBatchCalls = (totalRecords + batchSize - 1) / batchSize; // Ceiling division

        // Create log file with specific number of records
        LogFileWriter writer = initLogFileWriter(batchTestFilePath);
        List<Mutation> originalMutations = new ArrayList<>();

        // Add exactly totalRecords mutations to the log file using LogFileTestUtil
        for (int i = 0; i < totalRecords; i++) {
            Mutation put = LogFileTestUtil.newPut("row" + i, i + 1, i + 1);
            originalMutations.add(put);
            writer.append(tableName, i + 1, put);
            writer.sync();
        }
        writer.close();

        // Create a configuration with custom batch size
        Configuration testConf = new Configuration(conf);
        testConf.setInt(ReplicationLogProcessor.REPLICATION_STANDBY_LOG_REPLAY_BATCH_SIZE, batchSize);

        // Create processor with custom batch size and spy on it
        ReplicationLogProcessor spyProcessor = Mockito.spy(new ReplicationLogProcessor(testConf, testHAGroupId));

        // Validate that the batch size is correctly set
        assertEquals("Batch size incorrectly set", batchSize, spyProcessor.getBatchSize());

        // Store captured arguments manually to avoid reference issues
        List<Map<TableName, List<Mutation>>> capturedArguments = new ArrayList<>();
        AtomicInteger processReplicationLogBatchCount = new AtomicInteger(0);

        // Mock applyMutations to capture arguments
        Mockito.doAnswer(invocation -> {
            // Capture deep copy of arguments
            Map<TableName, List<Mutation>> originalMap = invocation.getArgument(0);
            Map<TableName, List<Mutation>> deepCopy = new HashMap<>(originalMap);
            capturedArguments.add(deepCopy);
            return null;
        }).when(spyProcessor).processReplicationLogBatch(Mockito.anyMap());

        // Process the log file
        spyProcessor.processLogFile(localFs, batchTestFilePath);

        // Verify processReplicationLogBatch was called the expected number of times
        Mockito.verify(spyProcessor, Mockito.times(expectedBatchCalls))
                .processReplicationLogBatch(Mockito.any(Map.class));

        // Validate the captured parameters using our manually captured arguments
        assertEquals("Should have captured " + expectedBatchCalls + " batch calls",
                expectedBatchCalls, capturedArguments.size());

        // Validate each batch call individually
        TableName expectedTableName = TableName.valueOf(tableName);
        int mutationIndex = 0;

        for (int batchIndex = 0; batchIndex < expectedBatchCalls; batchIndex++) {
            Map<TableName, List<Mutation>> batch = capturedArguments.get(batchIndex);
            assertNotNull("Batch " + batchIndex + " should not be null", batch);

            // Validate batch contains the expected table
            assertTrue("Batch " + batchIndex + " should contain table " + tableName,
                    batch.containsKey(expectedTableName));

            List<Mutation> batchMutations = batch.get(expectedTableName);
            assertNotNull("Batch " + batchIndex + " mutations should not be null", batchMutations);

            // Calculate expected mutations in this batch
            int expectedMutationsInBatch = Math.min(batchSize, totalRecords - mutationIndex);
            assertEquals("Batch " + batchIndex + " should have " + expectedMutationsInBatch + " mutations",
                    expectedMutationsInBatch, batchMutations.size());

            // Validate each mutation in the batch
            for (int i = 0; i < expectedMutationsInBatch; i++) {
                LogFileTestUtil.assertMutationEquals("Batch " + batchIndex + " mutation " + i + " mismatch",
                        originalMutations.get(mutationIndex), batchMutations.get(i));
                mutationIndex++;
            }
        }

        // Ensure all mutations were processed
        assertEquals("All mutations should have been processed", totalRecords, mutationIndex);

        // Clean up
        spyProcessor.close();
    }

    /**
     * Tests that ReplicationLogProcessor.get() returns the same instance for the same haGroupId.
     * Verifies that multiple calls with the same parameters return the cached instance.
     */
    @Test
    public void testReplicationLogProcessorInstanceCaching() throws Exception {
        final String haGroupId1 = "testHAGroup_1";
        final String haGroupId2 = "testHAGroup_2";

        // Get instances for the first HA group
        ReplicationLogProcessor group1Instance1 = ReplicationLogProcessor.get(conf, haGroupId1);
        ReplicationLogProcessor group1Instance2 = ReplicationLogProcessor.get(conf, haGroupId1);

        // Verify same instance is returned for same haGroupId
        assertNotNull("ReplicationLogProcessor should not be null", group1Instance1);
        assertNotNull("ReplicationLogProcessor should not be null", group1Instance2);
        assertSame("Same instance should be returned for same haGroupId", group1Instance1, group1Instance2);
        assertEquals("HA Group ID should match", haGroupId1, group1Instance1.getHaGroupId());

        // Get instance for a different HA group
        ReplicationLogProcessor group2Instance1 = ReplicationLogProcessor.get(conf, haGroupId2);
        assertNotNull("ReplicationLogProcessor should not be null", group2Instance1);
        assertNotSame("Different instance should be returned for different haGroupId", group2Instance1, group1Instance1);
        assertEquals("HA Group ID should match", haGroupId2, group2Instance1.getHaGroupId());

        // Verify multiple calls still return cached instances
        ReplicationLogProcessor group1Instance3 = ReplicationLogProcessor.get(conf, haGroupId1);
        ReplicationLogProcessor group2Instance2 = ReplicationLogProcessor.get(conf, haGroupId2);
        assertSame("Cached instance should be returned", group1Instance3, group1Instance1);
        assertSame("Cached instance should be returned", group2Instance2, group2Instance1);

        // Clean up
        group1Instance1.close();
        group2Instance1.close();
    }

    /**
     * Tests that close() removes the instance from the cache.
     * Verifies that after closing, a new call to get() creates a new instance.
     */
    @Test
    public void testReplicationLogProcessorCacheRemovalOnClose() throws Exception {
        final String haGroupId = "testHAGroup";

        // Get initial instance
        ReplicationLogProcessor group1Instance1 = ReplicationLogProcessor.get(conf, haGroupId);
        assertNotNull("ReplicationLogProcessor should not be null", group1Instance1);
        assertFalse("Executor service must not be shut down", group1Instance1.getExecutorService().isShutdown());

        // Verify cached instance is returned
        ReplicationLogProcessor group1Instance2 = ReplicationLogProcessor.get(conf, haGroupId);
        assertSame("Same instance should be returned before close", group1Instance2, group1Instance1);

        // Close the group
        group1Instance1.close();
        assertTrue("Executor service must be shut down", group1Instance1.getExecutorService().isShutdown());

        // Get instance after close - should be a new instance
        ReplicationLogProcessor group1Instance3 = ReplicationLogProcessor.get(conf, haGroupId);
        assertNotNull("ReplicationLogProcessor should not be null after close", group1Instance3);
        assertFalse("Executor service must not be shut down", group1Instance3.getExecutorService().isShutdown());
        assertNotSame("New instance should be created after close", group1Instance1, group1Instance3);
        assertEquals("HA Group ID should match", haGroupId, group1Instance3.getHaGroupId());

        // Clean up
        group1Instance3.close();
    }

    private LogFileWriter initLogFileWriter(Path filePath) throws IOException {
        LogFileWriter writer = new LogFileWriter();
        LogFileWriterContext writerContext = new LogFileWriterContext(conf).setFileSystem(localFs)
                .setFilePath(filePath);
        writer.init(writerContext);
        return writer;
    }

    private Connection getConnection() throws Exception {
        return getConnection(PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES));
    }

    private List<Mutation> generateHBaseMutations(final PhoenixConnection phoenixConnection, final int rows, final String tableName, final long timestamp, final String rowKeyPrefix) throws Exception {
        List<Mutation> mutations = new ArrayList<>();
        int randomNumber = new Random().nextInt(1000000);
        for(int i = 0; i<rows; i++) {
            final String dml = String.format(UPSERT_SQL_STATEMENT, tableName, rowKeyPrefix+randomNumber+i, "b"+randomNumber+i, "c"+randomNumber+i, i+1);
            phoenixConnection.createStatement().execute(dml);
            Iterator<Pair<byte[], List<Mutation>>> iterator = phoenixConnection.getMutationState().toMutations();
            while (iterator.hasNext()) {
                Pair<byte[], List<Mutation>> mutationPair = iterator.next();
                for(Mutation mutation : mutationPair.getSecond()) {
                    if (mutation instanceof Put) {
                        Put put = (Put) mutation;
                        Put newPut = new Put(put.getRow());
                        newPut.setTimestamp(timestamp);
                        // Copy cells with mutation timestamp
                        for (Cell cell : put.getFamilyCellMap().values().stream().flatMap(List::stream).collect(Collectors.toList())) {
                            newPut.add(cloneCellWithCustomTimestamp(cell, timestamp));
                        }
                        mutations.add(newPut);
                    } else if (mutation instanceof Delete) {
                        Delete delete = (Delete) mutation;
                        Delete newDelete = new Delete(delete.getRow());
                        newDelete.setTimestamp(delete.getTimestamp());
                        // Copy cells with mutation timestamp
                        for (Cell cell : delete.getFamilyCellMap().values().stream().flatMap(List::stream).collect(Collectors.toList())) {
                            newDelete.add(cloneCellWithCustomTimestamp(cell, timestamp));
                        }
                        mutations.add(newDelete);
                    }
                }
            }
        }
        return mutations;
    }

    private Connection getConnection(Properties props) throws Exception {
        props.setProperty(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
        // Force real driver to be used as the test one doesn't handle creating
        // more than one ConnectionQueryService
        props.setProperty(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB, StringUtil.EMPTY_STRING);
        // Create new ConnectionQueryServices so that we can set DROP_METADATA_ATTRIB
        String url = QueryUtil.getConnectionUrl(props, config, PRINCIPAL);
        return DriverManager.getConnection(url, props);
    }

    /**
     * Validates that the given mutations have been correctly applied to the specified table
     * by creating a temporary table, applying the mutations, and comparing the results.
     */
    private void validate(String tableName, List<Mutation> mutations) throws IOException {
        // Create a temporary table with the same schema
        String tempTableName = tableName + "_temp";
        try (Connection conn = getConnection()) {
            // Get the table descriptor of the original table
            Table originalTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(tableName));
            TableDescriptor originalDesc = originalTable.getDescriptor();

            // Create temporary table with same schema
            Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tempTableName));
            for (ColumnFamilyDescriptor cf : originalDesc.getColumnFamilies()) {
                builder.setColumnFamily(cf);
            }
            admin.createTable(builder.build());

            // Apply mutations to temporary table
            Table tempTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(tempTableName));

            // Apply all mutations in a batch
            tempTable.batch(mutations, null);

            // Compare data between original and temporary tables
            Scan scan = new Scan();
            scan.setRaw(true); // Enable raw scan to see delete markers

            ResultScanner originalScanner = originalTable.getScanner(scan);
            ResultScanner tempScanner = tempTable.getScanner(scan);

            Map<String, Result> originalResults = new HashMap<>();
            Map<String, Result> tempResults = new HashMap<>();

            // Collect results from original table
            for (Result result : originalScanner) {
                String rowKey = Bytes.toString(result.getRow());
                originalResults.put(rowKey, result);
            }

            // Collect results from temporary table
            for (Result result : tempScanner) {
                String rowKey = Bytes.toString(result.getRow());
                tempResults.put(rowKey, result);
            }

            // Compare results
            assertEquals("Number of rows should match", originalResults.size(), tempResults.size());

            for (Map.Entry<String, Result> entry : originalResults.entrySet()) {
                String rowKey = entry.getKey();
                Result originalResult = entry.getValue();
                Result tempResult = tempResults.get(rowKey);

                assertNotNull("Row " + rowKey + " should exist in temporary table", tempResult);

                // Compare cells using sets approach for better performance
                Cell[] originalCells = originalResult.rawCells();
                Cell[] tempCells = tempResult.rawCells();

                assertEquals("Number of cells should match for row " + rowKey,
                        originalCells.length, tempCells.length);

                // Use sets comparison approach - convert cells to comparable format
                java.util.Set<String> originalCellSet = new java.util.HashSet<>();
                java.util.Set<String> tempCellSet = new java.util.HashSet<>();

                // Create string representations of cells for set comparison
                for (Cell cell : originalCells) {
                    originalCellSet.add(CellUtil.toString(cell, false));
                }

                for (Cell cell : tempCells) {
                    tempCellSet.add(CellUtil.toString(cell, false));
                }

                // Compare sets directly
                assertEquals("Cell sets do not match for row " + rowKey, originalCellSet, tempCellSet);
            }

            // Cleanup
            originalScanner.close();
            tempScanner.close();
            originalTable.close();
            tempTable.close();
            admin.disableTable(TableName.valueOf(tempTableName));
            admin.deleteTable(TableName.valueOf(tempTableName));
        } catch (Exception e) {
            throw new IOException("Failed to validate mutations: " + e.getMessage(), e);
        }
    }

    private Cell cloneCellWithCustomTimestamp(Cell cell, long timestamp) {
        return CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
                .setRow(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())
                .setFamily(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength())
                .setQualifier(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())
                .setTimestamp(timestamp) // Use mutation timestamp for all cells
                .setType(cell.getType())
                .setValue(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength())
                .build();
    }
}
