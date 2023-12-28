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
package org.apache.phoenix.end2end.index;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.mapreduce.index.IndexVerificationOutputRepository;
import org.apache.phoenix.mapreduce.index.IndexVerificationOutputRepository.IndexVerificationErrorType;
import org.apache.phoenix.mapreduce.index.IndexVerificationOutputRow;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.phoenix.coprocessorclient.MetaDataProtocol.DEFAULT_LOG_TTL;
import static org.apache.phoenix.mapreduce.index.IndexVerificationOutputRepository.IndexVerificationErrorType.BEYOND_MAX_LOOKBACK_INVALID;
import static org.apache.phoenix.mapreduce.index.IndexVerificationOutputRepository.IndexVerificationErrorType.BEYOND_MAX_LOOKBACK_MISSING;
import static org.apache.phoenix.mapreduce.index.IndexVerificationOutputRepository.IndexVerificationErrorType.INVALID_ROW;
import static org.apache.phoenix.mapreduce.index.IndexVerificationOutputRepository.OUTPUT_TABLE_NAME_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexVerificationOutputRepository.PHASE_AFTER_VALUE;
import static org.apache.phoenix.mapreduce.index.IndexVerificationOutputRepository.PHASE_BEFORE_VALUE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

@Category(ParallelStatsDisabledTest.class)
public class IndexVerificationOutputRepositoryIT extends ParallelStatsDisabledIT {

    @BeforeClass
    public static synchronized void setupClass() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB, "100000000");
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Test
    public void testReadIndexVerificationOutputRow() throws Exception {
        String expectedErrorMessage = "I am an error message";
        byte[] expectedValue = Bytes.toBytes("ab");
        byte[] actualValue = Bytes.toBytes("ac");
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String tableName = "T" + generateUniqueName();
            byte[] tableNameBytes = Bytes.toBytes(tableName);
            String indexName = "I" + generateUniqueName();
            createTableAndIndexes(conn, tableName, indexName);
            byte[] indexNameBytes = Bytes.toBytes(indexName);
            IndexVerificationOutputRepository outputRepository =
                new IndexVerificationOutputRepository(indexNameBytes, conn);
            outputRepository.createOutputTable(conn);
            populateTable(conn, tableName);
            byte[] dataRowKey = getRowKey(conn, tableNameBytes);
            byte[] indexRowKey = getRowKey(conn, indexNameBytes);
            long dataRowTs = getTimestamp(conn, tableNameBytes);
            long indexRowTs = getTimestamp(conn, indexNameBytes);
            long scanMaxTs = EnvironmentEdgeManager.currentTimeMillis();
            outputRepository.logToIndexToolOutputTable(dataRowKey, indexRowKey, dataRowTs,
                indexRowTs, expectedErrorMessage, expectedValue, actualValue,
                scanMaxTs, tableNameBytes, true,
                INVALID_ROW);
            //now increment the scan time by 1 and do it again
            outputRepository.logToIndexToolOutputTable(dataRowKey, indexRowKey, dataRowTs,
                indexRowTs, expectedErrorMessage, expectedValue, actualValue,
                scanMaxTs +1, tableNameBytes, false,
                INVALID_ROW);
            //make sure we only get the first row back
            IndexVerificationOutputRow expectedRow = buildVerificationRow(dataRowKey, indexRowKey, dataRowTs,
                indexRowTs, expectedErrorMessage, expectedValue, actualValue,
                scanMaxTs, tableNameBytes, indexNameBytes, PHASE_BEFORE_VALUE, INVALID_ROW);
            verifyOutputRow(outputRepository, scanMaxTs, indexNameBytes, expectedRow);
            //make sure we get the second row back
            IndexVerificationOutputRow secondExpectedRow = buildVerificationRow(dataRowKey,
                indexRowKey, dataRowTs,
                indexRowTs, expectedErrorMessage, expectedValue, actualValue,
                scanMaxTs + 1, tableNameBytes, indexNameBytes, PHASE_AFTER_VALUE, INVALID_ROW);
            verifyOutputRow(outputRepository, scanMaxTs+1, indexNameBytes, secondExpectedRow);
        }

    }

    @Test
    public void testTTLOnOutputTable() throws SQLException, IOException {
        String mockString = "mock_value";
        byte[] mockStringBytes = Bytes.toBytes(mockString);

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Table hTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(
                OUTPUT_TABLE_NAME_BYTES);

            IndexVerificationOutputRepository
                    outputRepository =
                    new IndexVerificationOutputRepository(mockStringBytes, conn);

            outputRepository.createOutputTable(conn);
            TestUtil.assertTableHasTtl(conn, TableName.valueOf(OUTPUT_TABLE_NAME_BYTES), DEFAULT_LOG_TTL);
            ManualEnvironmentEdge customClock = new ManualEnvironmentEdge();
            customClock.setValue(EnvironmentEdgeManager.currentTimeMillis());
            EnvironmentEdgeManager.injectEdge(customClock);
            outputRepository.logToIndexToolOutputTable(mockStringBytes, mockStringBytes,
                    1, 2, mockString, mockStringBytes, mockStringBytes,
                    EnvironmentEdgeManager.currentTimeMillis(), mockStringBytes, true,
                INVALID_ROW);
            customClock.incrementValue(1L);
            Assert.assertEquals(1, TestUtil.getRowCount(hTable, false));

            customClock.incrementValue(1000*(DEFAULT_LOG_TTL+5));
            EnvironmentEdgeManager.injectEdge(customClock);
            int count = TestUtil.getRowCount(hTable, false);

            Assert.assertEquals(0, count);
        } finally {
            EnvironmentEdgeManager.reset();
        }
    }

    @Test
    public void testDisableLoggingBefore() throws SQLException, IOException {
        IndexTool.IndexDisableLoggingType disableLoggingVerifyType = IndexTool.IndexDisableLoggingType.BEFORE;
        boolean expectedBefore = false;
        boolean expectedAfter = true;
        verifyDisableLogging(disableLoggingVerifyType, expectedBefore, expectedAfter, INVALID_ROW);
    }

    @Test
    public void testDisableLoggingAfter() throws SQLException, IOException {
        IndexTool.IndexDisableLoggingType disableLoggingVerifyType = IndexTool.IndexDisableLoggingType.AFTER;
        boolean expectedBefore = true;
        boolean expectedAfter = false;
        verifyDisableLogging(disableLoggingVerifyType, expectedBefore, expectedAfter, INVALID_ROW);
    }

    @Test
    public void testDisableLoggingBoth() throws SQLException, IOException {
        IndexTool.IndexDisableLoggingType disableLoggingVerifyType = IndexTool.IndexDisableLoggingType.BOTH;
        boolean expectedBefore = false;
        boolean expectedAfter = false;
        verifyDisableLogging(disableLoggingVerifyType, expectedBefore, expectedAfter, INVALID_ROW);
    }

    @Test
    public void testDisableLoggingNone() throws SQLException, IOException {
        IndexTool.IndexDisableLoggingType disableLoggingVerifyType = IndexTool.IndexDisableLoggingType.NONE;
        boolean expectedBefore = true;
        boolean expectedAfter = true;
        verifyDisableLogging(disableLoggingVerifyType, expectedBefore, expectedAfter, INVALID_ROW);
    }

    @Test
    public void testDisableLoggingBeyondMaxLookback() throws SQLException, IOException {
        IndexTool.IndexDisableLoggingType disableLoggingVerifyType = IndexTool.IndexDisableLoggingType.NONE;
        boolean expectedBefore = false;
        boolean expectedAfter = false;
        verifyDisableLogging(disableLoggingVerifyType, expectedBefore, expectedAfter,
            BEYOND_MAX_LOOKBACK_INVALID, false);
        verifyDisableLogging(disableLoggingVerifyType, expectedBefore, expectedAfter,
            BEYOND_MAX_LOOKBACK_MISSING, false);

        expectedBefore = true;
        expectedAfter = true;
        verifyDisableLogging(disableLoggingVerifyType, expectedBefore, expectedAfter,
            BEYOND_MAX_LOOKBACK_INVALID, true);
        verifyDisableLogging(disableLoggingVerifyType, expectedBefore, expectedAfter,
            BEYOND_MAX_LOOKBACK_MISSING, true);
    }

    public void verifyDisableLogging(IndexTool.IndexDisableLoggingType disableLoggingVerifyType,
                                     boolean expectedBefore, boolean expectedAfter,
                                     IndexVerificationErrorType errorType) throws SQLException, IOException {
        verifyDisableLogging(disableLoggingVerifyType, expectedBefore, expectedAfter, errorType,
            true);
    }

    public void verifyDisableLogging(IndexTool.IndexDisableLoggingType disableLoggingVerifyType,
                                     boolean expectedBefore, boolean expectedAfter,
                                     IndexVerificationErrorType errorType,
                                     boolean shouldLogBeyondMaxLookback) throws SQLException,
        IOException {
        Table mockOutputTable = Mockito.mock(Table.class);
        Table mockIndexTable = Mockito.mock(Table.class);
        when(mockIndexTable.getName()).thenReturn(TableName.valueOf("testDisableLoggingIndexName"));
        IndexVerificationOutputRepository outputRepository =
            new IndexVerificationOutputRepository(mockOutputTable,
                mockIndexTable, disableLoggingVerifyType);
        outputRepository.setShouldLogBeyondMaxLookback(shouldLogBeyondMaxLookback);
        byte[] dataRowKey = Bytes.toBytes("dataRowKey");
        byte[] indexRowKey = Bytes.toBytes("indexRowKey");
        long dataRowTs = EnvironmentEdgeManager.currentTimeMillis();
        long indexRowTs = EnvironmentEdgeManager.currentTimeMillis();
        String errorMsg = "";
        byte[] expectedValue = Bytes.toBytes("expectedValue");
        byte[] actualValue = Bytes.toBytes("actualValue");
        long scanMaxTs = EnvironmentEdgeManager.currentTimeMillis();
        byte[] tableName = Bytes.toBytes("testDisableLoggingTableName");

        outputRepository.logToIndexToolOutputTable(dataRowKey, indexRowKey, dataRowTs, indexRowTs
            , errorMsg, expectedValue, actualValue, scanMaxTs, tableName, true, errorType);
        outputRepository.logToIndexToolOutputTable(dataRowKey, indexRowKey, dataRowTs, indexRowTs
            , errorMsg, expectedValue, actualValue, scanMaxTs, tableName, false, errorType);
        int expectedRowsLogged = 0;
        if (expectedBefore && expectedAfter) {
            expectedRowsLogged = 2;
        } else if (expectedBefore || expectedAfter) {
            expectedRowsLogged = 1;
        }
        Mockito.verify(mockOutputTable, Mockito.times(expectedRowsLogged)).
            put(Mockito.any(Put.class));
    }

    public void verifyOutputRow(IndexVerificationOutputRepository outputRepository, long scanMaxTs,
                                byte[] indexNameBytes, IndexVerificationOutputRow expectedRow)
        throws IOException {
        List<IndexVerificationOutputRow> actualRows =
            outputRepository.getOutputRows(scanMaxTs, indexNameBytes);
        assertNotNull(actualRows);
        assertEquals(1, actualRows.size());
        assertEquals(expectedRow, actualRows.get(0));
    }

    private IndexVerificationOutputRow buildVerificationRow(byte[] dataRowKey, byte[] indexRowKey,
                                                            long dataRowTs, long indexRowTs,
                                                            String expectedErrorMessage,
                                                            byte[] expectedValue, byte[] actualValue,
                                                            long scanMaxTs,
                                                            byte[] tableNameBytes,
                                                            byte[] indexNameBytes,
                                                            byte[] phaseBeforeValue,
                                                            IndexVerificationErrorType errorType) {
        IndexVerificationOutputRow.IndexVerificationOutputRowBuilder builder =
            new IndexVerificationOutputRow.IndexVerificationOutputRowBuilder();
        return builder.setDataTableRowKey(dataRowKey).
            setIndexTableRowKey(indexRowKey).
            setScanMaxTimestamp(dataRowTs).
            setDataTableRowTimestamp(dataRowTs).
            setIndexTableRowTimestamp(indexRowTs).
            setErrorMessage(Bytes.toString(
                IndexVerificationOutputRepository.
                    getErrorMessageBytes(expectedErrorMessage, expectedValue, actualValue))).
            setExpectedValue(expectedValue).
            setActualValue(actualValue).
            setScanMaxTimestamp(scanMaxTs).
            setDataTableName(Bytes.toString(tableNameBytes)).
            setIndexTableName(Bytes.toString(indexNameBytes)).
            setPhaseValue(phaseBeforeValue).
            setErrorType(errorType).
            build();
    }

    private byte[] getRowKey(Connection conn, byte[] tableNameBytes)
        throws SQLException, IOException {
        Scan scan = new Scan();
        Table table =
            conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(tableNameBytes);
        ResultScanner scanner = table.getScanner(scan);
        Result r = scanner.next();
        return r.getRow();
    }

    private long getTimestamp(Connection conn, byte[] tableNameBytes) throws SQLException,
        IOException {
        Scan scan = new Scan();
        Table table =
            conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(tableNameBytes);
        ResultScanner scanner = table.getScanner(scan);
        Result r = scanner.next();
        return r.listCells().get(0).getTimestamp();
    }

    private void createTable(Connection conn, String tableName) throws Exception {
        conn.createStatement().execute("create table " + tableName +
            " (id varchar(10) not null primary key, val1 varchar(10), val2 varchar(10), " +
            "val3 varchar(10))");
    }

    private void populateTable(Connection conn, String tableName) throws Exception {
        conn.createStatement().execute("upsert into " + tableName + " values ('a', 'ab', 'abc', 'abcd')");
        conn.commit();
    }

    private void createTableAndIndexes(Connection conn, String dataTableName,
                                       String indexTableName) throws Exception {
        createTable(conn, dataTableName);
        conn.createStatement().execute("CREATE INDEX " + indexTableName + " on " +
            dataTableName + " (val1) include (val2, val3)");
        conn.commit();
    }

    @After
    public void dropOutputTable() throws Exception {
        try(Connection conn = DriverManager.getConnection(getUrl())) {
            ConnectionQueryServices queryServices = conn.unwrap(PhoenixConnection.class).getQueryServices();
            Admin admin = queryServices.getAdmin();
            TableName outputTableName = TableName.valueOf(OUTPUT_TABLE_NAME_BYTES);
            if (admin.tableExists(outputTableName)) {
                admin.disableTable(outputTableName);
                admin.deleteTable(outputTableName);
            }
        }
        EnvironmentEdgeManager.reset();
    }
}
