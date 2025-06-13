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
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellUtil;
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
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.StringUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationLogProcessorTest extends ParallelStatsDisabledIT {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationLogProcessorTest.class);

    private static final String CREATE_TABLE_SQL_STATEMENT = "CREATE TABLE %s (ID VARCHAR PRIMARY KEY, " +
            "COL_1 VARCHAR, COL_2 VARCHAR, COL_3 BIGINT)";

    private static final String UPSERT_SQL_STATEMENT = "upsert into %s values ('%s', '%s', '%s', %s)";

    private static final String PRINCIPAL = "replicationLogProcessor";

    @ClassRule
    public static TemporaryFolder testFolder = new TemporaryFolder();

    private static Configuration conf;
    private static FileSystem localFs;
    private static ExecutorService executorService;

    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        conf = getUtility().getConfiguration();
        localFs = FileSystem.getLocal(conf);
        executorService = Executors.newSingleThreadExecutor();
    }

    @AfterClass
    public static void tearDownAfterClass() {
        if(executorService != null) {
            executorService.shutdown();
        }
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
        ReplicationLogProcessor replicationLogProcessor = new ReplicationLogProcessor(conf, executorService);
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
    }

    /**
     * Tests error handling when attempting to create LogFileReader with a non-existent file.
     */
    @Test
    public void testCreateLogFileReaderWithNonExistentFile() {
        Path nonExistentPath = new Path(testFolder.toString(), "non_existent_file");
        try {
            ReplicationLogProcessor replicationLogProcessor = new ReplicationLogProcessor(conf, executorService);
            replicationLogProcessor.createLogFileReader(localFs, nonExistentPath);
            fail("Should throw IOException for non-existent file");
        } catch (IOException e) {
            assertTrue("Error message should mention file does not exist and file path name",
                    e.getMessage().contains("Log file does not exist: " + nonExistentPath));
        }
    }

    /**
     * Tests error handling when attempting to create LogFileReader with an invalid/corrupted file.
     */
    @Test
    public void testCreateLogFileReaderWithInvalidLogFile() throws IOException {
        Path invalidFilePath = new Path(testFolder.newFile("invalid_file").toURI());
        localFs.create(invalidFilePath).close(); // Create empty file
        try {
            ReplicationLogProcessor replicationLogProcessor = new ReplicationLogProcessor(conf, executorService);
            replicationLogProcessor.createLogFileReader(localFs, invalidFilePath);
            fail("Should throw IOException for invalid file");
        } catch (IOException e) {
            // Should throw some kind of IOException when trying to read header
            assertTrue("Should throw IOException", true);
        } finally {
            // Delete the invalid file
            localFs.delete(invalidFilePath);
        }
    }

    /**
     * Tests the closeReader method with both null and valid LogFileReader instances.
     */
    @Test
    public void testCloseReader() throws IOException {
        ReplicationLogProcessor replicationLogProcessor = new ReplicationLogProcessor(conf, executorService);
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

        // Ensure reader's close method is called only once
        Mockito.verify(reader, Mockito.times(1)).close();
    }

    /**
     * Tests processing an empty mutation map - should complete without errors.
     */
    @Test
    public void testProcessReplicationLogBatchWithEmptyMap() {
        Map<String, List<Mutation>> emptyMap = new HashMap<>();

        ReplicationLogProcessor replicationLogProcessor = new ReplicationLogProcessor(conf, executorService);
        // Process empty batch - should not throw any exceptions and should return immediately
        try {
            replicationLogProcessor.processReplicationLogBatch(emptyMap);
            // If we reach here, the empty map was processed successfully
            assertTrue("Processing empty map should complete without errors", true);
        } catch (Exception e) {
            fail("Processing empty map should not throw exception: " + e.getMessage());
        }
    }

    /**
     * Tests exception handling when attempting to process mutations for non-existent tables.
     */
    @Test
    public void testProcessReplicationLogBatchExceptionsMessageIsCorrect() {
        Map<String, List<Mutation>> tableMutationsMap = new HashMap<>();
        Mutation mutation = LogFileTestUtil.newPut("abc", 6L, 5);
        tableMutationsMap.put("NON_EXISTENT_TABLE", Collections.singletonList(mutation));
        ReplicationLogProcessor replicationLogProcessor = new ReplicationLogProcessor(conf, executorService);
        try {
            replicationLogProcessor.processReplicationLogBatch(tableMutationsMap);
            fail("Should throw TableNotFoundException for non-existent table");
        } catch (IOException exception) {
            assertTrue("Error message should mention file does not exist and file path name",
                    exception.getMessage().contains("TableNotFoundException"));
        }
    }

    /**
     * Tests behavior when HBase operations fail (simulated by disabling table).
     */
    @Test
    public void testProcessReplicationLogBatchWithHBaseFailure() throws Exception {
        final String tableName = "T_" + generateUniqueName();
        Map<String, List<Mutation>> tableMutationsMap = new HashMap<>();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            // Create table first
            conn.createStatement().execute(String.format(CREATE_TABLE_SQL_STATEMENT, tableName));
            PhoenixConnection phoenixConnection = conn.unwrap(PhoenixConnection.class);
            // Generate some mutations for the table
            List<Mutation> mutations = generateHBaseMutations(phoenixConnection, 2, tableName, 10L);
            tableMutationsMap.put(tableName, mutations);
            TableName hbaseTableName = TableName.valueOf(tableName);
            try (Admin admin = phoenixConnection.getQueryServices().getAdmin()) {
                // Disable the table to simulate HBase failure
                admin.disableTable(hbaseTableName);
                LOG.info("Disabled table {} to simulate HBase failure", tableName);

                ReplicationLogProcessor replicationLogProcessor = new ReplicationLogProcessor(conf, executorService);
                // Attempt to process mutations on disabled table - should fail
                try {
                    replicationLogProcessor.processReplicationLogBatch(tableMutationsMap);
                    fail("Should throw IOException when trying to apply mutations to disabled table");
                } catch (IOException e) {
                    // Expected behavior - disabled table should cause IOException
                    assertTrue("Should throw IOException for disabled table", true);
                    LOG.info("Expected IOException caught when processing mutations on disabled table: " + e.getMessage());
                }
            }
        }
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
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(String.format(CREATE_TABLE_SQL_STATEMENT, table1Name));
            conn.createStatement().execute(String.format(CREATE_TABLE_SQL_STATEMENT, table2Name));
            PhoenixConnection phoenixConnection = conn.unwrap(PhoenixConnection.class);

            List<Mutation> table1Mutations = generateHBaseMutations(phoenixConnection, 2, table1Name, 100L);
            List<Mutation> table2Mutations = generateHBaseMutations(phoenixConnection, 5, table2Name, 101L);
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

            ReplicationLogProcessor replicationLogProcessor = new ReplicationLogProcessor(conf, executorService);
            replicationLogProcessor.processLogFile(localFs, filePath);

            validate(table1Name, table1Mutations);
            validate(table2Name, table2Mutations);
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

        ReplicationLogProcessor replicationLogProcessor = new ReplicationLogProcessor(conf, executorService);
        // Attempt to process non-existent file - should throw IOException
        try {
            replicationLogProcessor.processLogFile(localFs, nonExistentFilePath);
            fail("Should throw IOException for non-existent file");
        } catch (IOException e) {
            // Expected behavior - non-existent file should cause IOException
            assertTrue("Should throw IOException for non-existent file", true);
        }
    }

    /**
     * Tests batching logic when processing log files with mutations for multiple tables.
     */
    @Test
    public void testProcessLogFileBatchingWithMultipleTables() throws Exception {
        final Path multiTableBatchFilePath = new Path(testFolder.newFile("testMultiTableBatch").toURI());
        final String table1Name = "T1_" + generateUniqueName();
        final String table2Name = "T2_" + generateUniqueName();
        final int batchSize = 4;
        final int recordsPerTable = 3;
        final int totalRecords = recordsPerTable * 2; // 6 total records
        final int expectedBatchCalls = (totalRecords + batchSize - 1) / batchSize; // 2 calls
        // Create log file with mutations for multiple tables
        LogFileWriter writer = initLogFileWriter(multiTableBatchFilePath);
        // Add mutations alternating between tables using LogFileTestUtil
        for (int i = 0; i < recordsPerTable; i++) {
            // Add mutation for table1
            Mutation put1 = LogFileTestUtil.newPut("row1_" + i, (i * 2) + 1, (i * 2) + 1);
            writer.append(table1Name, (i * 2) + 1, put1);
            writer.sync();
            // Add mutation for table2
            Mutation put2 = LogFileTestUtil.newPut("row2_" + i, (i * 2) + 2, (i * 2) + 2);
            writer.append(table2Name, (i * 2) + 2, put2);
            writer.sync();
        }
        writer.close();
        // Create processor with custom batch size and spy on it
        Configuration testConf = new Configuration(conf);
        testConf.setInt(ReplicationLogProcessor.REPLICATION_STANDBY_LOG_REPLAY_BATCH_SIZE, batchSize);

        ReplicationLogProcessor replicationLogProcessor = new ReplicationLogProcessor(testConf, executorService);
        // Validate that the batch size is correctly set
        assertEquals("Batch size should be set correctly", batchSize, replicationLogProcessor.getBatchSize());
        ReplicationLogProcessor spyProcessor = Mockito.spy(replicationLogProcessor);
        // Mock the processReplicationLogBatch method
        Mockito.doNothing().when(spyProcessor).processReplicationLogBatch(Mockito.any(Map.class));
        // Process the log file
        spyProcessor.processLogFile(localFs, multiTableBatchFilePath);
        // Verify processReplicationLogBatch was called the expected number of times
        Mockito.verify(spyProcessor, Mockito.times(expectedBatchCalls))
            .processReplicationLogBatch(Mockito.any(Map.class));
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
        ReplicationLogProcessor replicationLogProcessor = new ReplicationLogProcessor(conf, executorService);
        try {
            replicationLogProcessor.processLogFile(localFs, emptyFilePath);
            // If we reach here, the empty file was processed successfully
            assertTrue("Processing empty log file should complete without errors", true);
        } catch (Exception e) {
            fail("Processing empty log file should not throw exception: " + e.getMessage());
        }
    }

    /**
     * Tests processing of log files that were not closed, ensuring it's successful.
     */
    @Test
    public void testProcessLogFileForUnClosedFile() throws Exception {
        final Path emptyFilePath = new Path(testFolder.newFile("testProcessLogFileEmpty").toURI());
        LogFileWriter writer = initLogFileWriter(emptyFilePath);

        // Add one mutation
        Mutation put = LogFileTestUtil.newPut("row1", 3L, 4);
        writer.append("table", 1, put);
        writer.sync();

        // Process the file without closing - should not throw any exceptions
        ReplicationLogProcessor spyProcessor = Mockito.spy(new ReplicationLogProcessor(conf, executorService));
        Mockito.doNothing().when(spyProcessor).processReplicationLogBatch(Mockito.any(Map.class));

        spyProcessor.processLogFile(localFs, emptyFilePath);

        // Verify processReplicationLogBatch was called the expected number of times
        Mockito.verify(spyProcessor, Mockito.times(1))
                .processReplicationLogBatch(Mockito.any(Map.class));
    }

    /**
     * Tests that configuration parameters are properly read and applied.
     */
    @Test
    public void testReplicationLogProcessorConfiguration() {
        // Test that all default configurations are used when no custom configuration is provided
        ReplicationLogProcessor defaultProcessor = new ReplicationLogProcessor(conf, executorService);

        // Validate default batch size
        assertEquals("Default batch size should be used",
                ReplicationLogProcessor.DEFAULT_REPLICATION_STANDBY_LOG_REPLAY_BATCH_SIZE,
                defaultProcessor.getBatchSize());

        // Validate default HBase client retries count
        assertEquals("Default HBase client retries count should be used",
                ReplicationLogProcessor.DEFAULT_REPLICATION_STANDBY_HBASE_CLIENT_RETRIES_COUNT,
                defaultProcessor.getHBaseClientRetriesCount());

        // Validate default HBase client operation timeout
        assertEquals("Default HBase client operation timeout should be used",
                ReplicationLogProcessor.DEFAULT_REPLICATION_STANDBY_HBASE_CLIENT_OPERATION_TIMEOUT_MS,
                defaultProcessor.getHBaseClientOperationTimeout());

        // Test that all custom configurations are honored
        Configuration customConf = new Configuration(conf);

        // Set custom values for all configuration parameters
        int customBatchSize = 1000;
        int customRetriesCount = 6;
        int customOperationTimeout = 15000;

        customConf.setInt(ReplicationLogProcessor.REPLICATION_STANDBY_LOG_REPLAY_BATCH_SIZE, customBatchSize);
        customConf.setInt(ReplicationLogProcessor.REPLICATION_STANDBY_HBASE_CLIENT_RETRIES_COUNT, customRetriesCount);
        customConf.setInt(ReplicationLogProcessor.REPLICATION_STANDBY_HBASE_CLIENT_OPERATION_TIMEOUT_MS, customOperationTimeout);

        ReplicationLogProcessor customProcessor = new ReplicationLogProcessor(customConf, executorService);

        // Validate all custom configurations are honored
        assertEquals("Custom batch size should be honored",
                customBatchSize, customProcessor.getBatchSize());

        assertEquals("Custom HBase client retries count should be honored",
                customRetriesCount, customProcessor.getHBaseClientRetriesCount());

        assertEquals("Custom HBase client operation timeout should be honored",
                customOperationTimeout, customProcessor.getHBaseClientOperationTimeout());
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
     * Helper method to test batching scenarios with different record counts and batch sizes
     */
    private void testProcessLogFileBatching(int totalRecords, int batchSize) throws Exception {
        final Path batchTestFilePath = new Path(testFolder.newFile("test_" + new Random(1000)).toURI());
        final String tableName = "T_" + generateUniqueName();
        final int expectedBatchCalls = (totalRecords + batchSize - 1) / batchSize; // Ceiling division

        // Create log file with specific number of records
        LogFileWriter writer = initLogFileWriter(batchTestFilePath);

        // Add exactly totalRecords mutations to the log file using LogFileTestUtil
        for (int i = 0; i < totalRecords; i++) {
            Mutation put = LogFileTestUtil.newPut("row" + i, i + 1, i + 1);
            writer.append(tableName, i + 1, put);
            writer.sync();
        }
        writer.close();

        // Create a configuration with custom batch size
        Configuration testConf = new Configuration(conf);
        testConf.setInt(ReplicationLogProcessor.REPLICATION_STANDBY_LOG_REPLAY_BATCH_SIZE, batchSize);

        // Create processor with custom batch size and spy on it
        ReplicationLogProcessor testProcessor = new ReplicationLogProcessor(testConf, executorService);

        // Validate that the batch size is correctly set
        assertEquals("Batch size incorrectly set", batchSize, testProcessor.getBatchSize());

        ReplicationLogProcessor spyProcessor = Mockito.spy(testProcessor);

        // Mock the processReplicationLogBatch method to do nothing but track calls
        Mockito.doNothing().when(spyProcessor).processReplicationLogBatch(Mockito.any(Map.class));

        // Process the log file
        spyProcessor.processLogFile(localFs, batchTestFilePath);

        // Verify processReplicationLogBatch was called the expected number of times
        Mockito.verify(spyProcessor, Mockito.times(expectedBatchCalls))
                .processReplicationLogBatch(Mockito.any(Map.class));
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

    private List<Mutation> generateHBaseMutations(final PhoenixConnection phoenixConnection, final int rows, final String tableName, final long timestamp) throws Exception {
        List<Mutation> mutations = new ArrayList<>();
        int randomNumber = new Random().nextInt(1000000);
        for(int i = 0; i<rows; i++) {
            final String dml = String.format(UPSERT_SQL_STATEMENT, tableName, "a"+randomNumber+i, "b"+randomNumber+i, "c"+randomNumber+i, i+1);
            phoenixConnection.createStatement().execute(dml);
            Iterator<Pair<byte[], List<Mutation>>> iterator = phoenixConnection.getMutationState().toMutations();
            while (iterator.hasNext()) {
                Pair<byte[], List<Mutation>> mutationPair = iterator.next();
                mutationPair.getSecond().forEach(mutation -> mutation.setTimestamp(timestamp));
                mutations.addAll(mutationPair.getSecond());
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

            // Create new mutations with cell timestamps set to mutation timestamp
            List<Mutation> newMutations = new ArrayList<>();
            for (Mutation mutation : mutations) {
                if (mutation instanceof Put) {
                    Put put = (Put) mutation;
                    Put newPut = new Put(put.getRow());
                    newPut.setTimestamp(put.getTimestamp());
                    // Copy cells with mutation timestamp
                    for (Cell cell : put.getFamilyCellMap().values().stream().flatMap(List::stream).collect(Collectors.toList())) {
                        newPut.add(cloneCellWithCustomTimestamp(cell, mutation.getTimestamp()));
                    }
                    newMutations.add(newPut);
                } else if (mutation instanceof Delete) {
                    Delete delete = (Delete) mutation;
                    Delete newDelete = new Delete(delete.getRow());
                    newDelete.setTimestamp(delete.getTimestamp());
                    // Copy cells with mutation timestamp
                    for (Cell cell : delete.getFamilyCellMap().values().stream().flatMap(List::stream).collect(Collectors.toList())) {
                        newDelete.add(cloneCellWithCustomTimestamp(cell, mutation.getTimestamp()));
                    }
                    newMutations.add(newDelete);
                }
            }

            // Apply all mutations in a batch
            tempTable.batch(newMutations, null);

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
