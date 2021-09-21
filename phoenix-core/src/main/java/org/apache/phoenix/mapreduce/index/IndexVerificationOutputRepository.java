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
package org.apache.phoenix.mapreduce.index;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.hbase.index.table.HTableFactory;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.ByteUtil;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class IndexVerificationOutputRepository implements AutoCloseable {
    public static final byte[] ROW_KEY_SEPARATOR_BYTE = Bytes.toBytes("|");

    private Table indexTable;
    private byte[] indexName;
    private Table outputTable;
    private IndexTool.IndexDisableLoggingType disableLoggingVerifyType =
        IndexTool.IndexDisableLoggingType.NONE;
    private boolean shouldLogBeyondMaxLookback = true;

    public final static String OUTPUT_TABLE_NAME = "PHOENIX_INDEX_TOOL";
    public final static byte[] OUTPUT_TABLE_NAME_BYTES = Bytes.toBytes(OUTPUT_TABLE_NAME);
    public final static byte[] OUTPUT_TABLE_COLUMN_FAMILY = QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES;

    public final static String DATA_TABLE_NAME = "DTName";
    public final static byte[] DATA_TABLE_NAME_BYTES = Bytes.toBytes(DATA_TABLE_NAME);
    public final static String INDEX_TABLE_NAME = "ITName";
    public final static byte[] INDEX_TABLE_NAME_BYTES = Bytes.toBytes(INDEX_TABLE_NAME);
    public final static String DATA_TABLE_ROW_KEY = "DTRowKey";
    public final static byte[] DATA_TABLE_ROW_KEY_BYTES = Bytes.toBytes(DATA_TABLE_ROW_KEY);
    public final static String INDEX_TABLE_ROW_KEY = "ITRowKey";
    public final static byte[] INDEX_TABLE_ROW_KEY_BYTES = Bytes.toBytes(INDEX_TABLE_ROW_KEY);
    public final static String DATA_TABLE_TS = "DTTS";
    public final static byte[] DATA_TABLE_TS_BYTES = Bytes.toBytes(DATA_TABLE_TS);
    public final static String INDEX_TABLE_TS = "ITTS";
    public final static byte[] INDEX_TABLE_TS_BYTES = Bytes.toBytes(INDEX_TABLE_TS);
    public final static String ERROR_MESSAGE = "Error";
    public final static byte[] ERROR_MESSAGE_BYTES = Bytes.toBytes(ERROR_MESSAGE);
    public static final String ERROR_TYPE = "ErrorType";
    public static final byte[] ERROR_TYPE_BYTES = Bytes.toBytes(ERROR_TYPE);

    public static final String  VERIFICATION_PHASE = "Phase";
    public final static byte[] VERIFICATION_PHASE_BYTES = Bytes.toBytes(VERIFICATION_PHASE);
    public final static String EXPECTED_VALUE = "ExpectedValue";
    public final static byte[] EXPECTED_VALUE_BYTES = Bytes.toBytes(EXPECTED_VALUE);
    public final static String ACTUAL_VALUE = "ActualValue";
    public final static byte[] ACTUAL_VALUE_BYTES = Bytes.toBytes(ACTUAL_VALUE);
    public static final byte[] E_VALUE_PREFIX_BYTES = Bytes.toBytes(" E:");
    public static final byte[] A_VALUE_PREFIX_BYTES = Bytes.toBytes(" A:");
    public static final int PREFIX_LENGTH = 3;
    public static final int TOTAL_PREFIX_LENGTH = 6;
    public static final byte[] PHASE_BEFORE_VALUE = Bytes.toBytes("BEFORE");
    public static final byte[] PHASE_AFTER_VALUE = Bytes.toBytes("AFTER");


    public enum IndexVerificationErrorType {
        INVALID_ROW,
        MISSING_ROW,
        EXTRA_ROW,
        EXTRA_CELLS,
        BEYOND_MAX_LOOKBACK_INVALID,
        BEYOND_MAX_LOOKBACK_MISSING,
        UNKNOWN
    }
    /**
     * Only usable for the create table / read path or for testing. Use setOutputTable and
     * setIndexTable first to write.
     */
    public IndexVerificationOutputRepository() {

    }

    @VisibleForTesting
    public IndexVerificationOutputRepository(byte[] indexName, Connection conn) throws SQLException {
        ConnectionQueryServices queryServices =
            conn.unwrap(PhoenixConnection.class).getQueryServices();
        outputTable = queryServices.getTable(OUTPUT_TABLE_NAME_BYTES);
        indexTable = queryServices.getTable(indexName);
    }

    @VisibleForTesting
    public IndexVerificationOutputRepository(Table outputTable, Table indexTable,
                                             IndexTool.IndexDisableLoggingType disableLoggingVerifyType) throws SQLException {
        this.outputTable = outputTable;
        this.indexTable = indexTable;
        this.disableLoggingVerifyType = disableLoggingVerifyType;
    }

    public IndexVerificationOutputRepository(byte[] indexName, HTableFactory hTableFactory,
                                             IndexTool.IndexDisableLoggingType disableLoggingVerifyType) throws IOException {
        this.indexName = indexName;
        outputTable = hTableFactory.getTable(new ImmutableBytesPtr(OUTPUT_TABLE_NAME_BYTES));
        indexTable = hTableFactory.getTable(new ImmutableBytesPtr(indexName));
        this.disableLoggingVerifyType = disableLoggingVerifyType;
    }

    public void setShouldLogBeyondMaxLookback(boolean shouldLogBeyondMaxLookback) {
        this.shouldLogBeyondMaxLookback = shouldLogBeyondMaxLookback;
    }

    public static byte[] generateOutputTableRowKey(long ts, byte[] indexTableName, byte[] dataRowKey ) {
        byte[] keyPrefix = Bytes.toBytes(Long.toString(ts));
        byte[] rowKey;
        int targetOffset = 0;
        // The row key for the output table : timestamp | index table name | data row key
        rowKey = new byte[keyPrefix.length + ROW_KEY_SEPARATOR_BYTE.length + indexTableName.length +
            ROW_KEY_SEPARATOR_BYTE.length + dataRowKey.length];
        Bytes.putBytes(rowKey, targetOffset, keyPrefix, 0, keyPrefix.length);
        targetOffset += keyPrefix.length;
        Bytes.putBytes(rowKey, targetOffset, ROW_KEY_SEPARATOR_BYTE, 0, ROW_KEY_SEPARATOR_BYTE.length);
        targetOffset += ROW_KEY_SEPARATOR_BYTE.length;
        Bytes.putBytes(rowKey, targetOffset, indexTableName, 0, indexTableName.length);
        targetOffset += indexTableName.length;
        Bytes.putBytes(rowKey, targetOffset, ROW_KEY_SEPARATOR_BYTE, 0, ROW_KEY_SEPARATOR_BYTE.length);
        targetOffset += ROW_KEY_SEPARATOR_BYTE.length;
        Bytes.putBytes(rowKey, targetOffset, dataRowKey, 0, dataRowKey.length);
        return rowKey;
    }

    /**
     * Generates partial row key for use in a Scan to get all rows for an index verification
     */
    private static byte[] generatePartialOutputTableRowKey(long ts, byte[] indexTableName){
        byte[] keyPrefix = Bytes.toBytes(Long.toString(ts));
        byte[] partialRowKey;
        int targetOffset = 0;
        // The row key for the output table : timestamp | index table name | data row key
        partialRowKey = new byte[keyPrefix.length + ROW_KEY_SEPARATOR_BYTE.length + indexTableName.length];
        Bytes.putBytes(partialRowKey, targetOffset, keyPrefix, 0, keyPrefix.length);
        targetOffset += keyPrefix.length;
        Bytes.putBytes(partialRowKey, targetOffset, ROW_KEY_SEPARATOR_BYTE, 0, ROW_KEY_SEPARATOR_BYTE.length);
        targetOffset += ROW_KEY_SEPARATOR_BYTE.length;
        Bytes.putBytes(partialRowKey, targetOffset, indexTableName, 0, indexTableName.length);
        return partialRowKey;
    }

    public void createOutputTable(Connection connection) throws IOException, SQLException {
        ConnectionQueryServices queryServices = connection.unwrap(PhoenixConnection.class).getQueryServices();
        try (Admin admin = queryServices.getAdmin()) {
            TableName outputTableName = TableName.valueOf(OUTPUT_TABLE_NAME);
            if (!admin.tableExists(outputTableName)) {
                ColumnFamilyDescriptor columnDescriptor =
                    ColumnFamilyDescriptorBuilder
                        .newBuilder(OUTPUT_TABLE_COLUMN_FAMILY)
                        .setTimeToLive(MetaDataProtocol.DEFAULT_LOG_TTL)
                        .build();
                TableDescriptor tableDescriptor = TableDescriptorBuilder
                    .newBuilder(TableName.valueOf(OUTPUT_TABLE_NAME))
                    .setColumnFamily(columnDescriptor).build();
                admin.createTable(tableDescriptor);
                outputTable = admin.getConnection().getTable(outputTableName);
            }
        }
    }

    @VisibleForTesting
    public void logToIndexToolOutputTable(byte[] dataRowKey, byte[] indexRowKey, long dataRowTs,
                                          long indexRowTs,
                                          String errorMsg, byte[] expectedValue, byte[] actualValue,
                                          long scanMaxTs, byte[] tableName,
                                          boolean isBeforeRebuild,
                                          IndexVerificationErrorType errorType)
        throws IOException {
        if (shouldLogOutput(isBeforeRebuild, errorType)) {
            byte[] rowKey = generateOutputTableRowKey(scanMaxTs, indexTable.getName().toBytes(), dataRowKey);
            Put put = new Put(rowKey);
            put.addColumn(OUTPUT_TABLE_COLUMN_FAMILY, DATA_TABLE_NAME_BYTES, tableName);
            put.addColumn(OUTPUT_TABLE_COLUMN_FAMILY, INDEX_TABLE_NAME_BYTES, indexName);
            put.addColumn(OUTPUT_TABLE_COLUMN_FAMILY, DATA_TABLE_TS_BYTES, Bytes.toBytes(Long.toString(dataRowTs)));

            put.addColumn(OUTPUT_TABLE_COLUMN_FAMILY, INDEX_TABLE_ROW_KEY_BYTES, indexRowKey);
            put.addColumn(OUTPUT_TABLE_COLUMN_FAMILY, INDEX_TABLE_TS_BYTES, Bytes.toBytes(Long.toString(indexRowTs)));
            byte[] errorMessageBytes;
            if (expectedValue != null) {
                errorMessageBytes = getErrorMessageBytes(errorMsg, expectedValue, actualValue);
                put.addColumn(OUTPUT_TABLE_COLUMN_FAMILY, EXPECTED_VALUE_BYTES, expectedValue);
                put.addColumn(OUTPUT_TABLE_COLUMN_FAMILY, ACTUAL_VALUE_BYTES, actualValue);
            } else {
                errorMessageBytes = Bytes.toBytes(errorMsg);
            }
            put.addColumn(OUTPUT_TABLE_COLUMN_FAMILY, ERROR_MESSAGE_BYTES, errorMessageBytes);
            put.addColumn(OUTPUT_TABLE_COLUMN_FAMILY, ERROR_TYPE_BYTES,
                Bytes.toBytes(errorType.toString()));
            if (isBeforeRebuild) {
                put.addColumn(OUTPUT_TABLE_COLUMN_FAMILY, VERIFICATION_PHASE_BYTES, PHASE_BEFORE_VALUE);
            } else {
                put.addColumn(OUTPUT_TABLE_COLUMN_FAMILY, VERIFICATION_PHASE_BYTES, PHASE_AFTER_VALUE);
            }
            outputTable.put(put);
        }
    }

    public boolean shouldLogOutput(boolean isBeforeRebuild, IndexVerificationErrorType errorType) {
        return shouldLogOutputForVerifyType(isBeforeRebuild) &&
            shouldLogOutputForErrorType(errorType);
    }

    private boolean shouldLogOutputForVerifyType(boolean isBeforeRebuild) {
        if (disableLoggingVerifyType.equals(IndexTool.IndexDisableLoggingType.BOTH)) {
            return false;
        }
        if (disableLoggingVerifyType.equals(IndexTool.IndexDisableLoggingType.NONE)) {
            return true;
        }
        if (isBeforeRebuild &&
            (disableLoggingVerifyType.equals(IndexTool.IndexDisableLoggingType.AFTER))) {
            return true;
        }
        if (!isBeforeRebuild && disableLoggingVerifyType.equals(IndexTool.IndexDisableLoggingType.BEFORE)) {
            return true;
        }
        return false;
    }

    private boolean shouldLogOutputForErrorType(IndexVerificationErrorType errorType) {
        if (errorType != null &&
            (errorType.equals(IndexVerificationErrorType.BEYOND_MAX_LOOKBACK_INVALID) ||
                errorType.equals(IndexVerificationErrorType.BEYOND_MAX_LOOKBACK_MISSING))){
            return shouldLogBeyondMaxLookback;
        }
        return true;
    }

    public static byte[] getErrorMessageBytes(String errorMsg, byte[] expectedValue, byte[] actualValue) {
        byte[] errorMessageBytes;
        errorMessageBytes = new byte[errorMsg.length() + expectedValue.length + actualValue.length +
            TOTAL_PREFIX_LENGTH];
        Bytes.putBytes(errorMessageBytes, 0, Bytes.toBytes(errorMsg), 0, errorMsg.length());
        int length = errorMsg.length();
        Bytes.putBytes(errorMessageBytes, length, E_VALUE_PREFIX_BYTES, 0, PREFIX_LENGTH);
        length += PREFIX_LENGTH;
        Bytes.putBytes(errorMessageBytes, length, expectedValue, 0, expectedValue.length);
        length += expectedValue.length;
        Bytes.putBytes(errorMessageBytes, length, A_VALUE_PREFIX_BYTES, 0, PREFIX_LENGTH);
        length += PREFIX_LENGTH;
        Bytes.putBytes(errorMessageBytes, length, actualValue, 0, actualValue.length);
        return errorMessageBytes;
    }

    public List<IndexVerificationOutputRow> getOutputRows(long ts, byte[] indexName)
        throws IOException {
        Iterator<IndexVerificationOutputRow> iter = getOutputRowIterator(ts, indexName);
        return getIndexVerificationOutputRows(iter);
    }

    @VisibleForTesting
    public List<IndexVerificationOutputRow> getAllOutputRows() throws IOException {
        Iterator<IndexVerificationOutputRow> iter = getOutputRowIteratorForAllRows();
        return getIndexVerificationOutputRows(iter);
    }

    private List<IndexVerificationOutputRow> getIndexVerificationOutputRows(Iterator<IndexVerificationOutputRow> iter) {
        List<IndexVerificationOutputRow> outputRowList = new ArrayList<IndexVerificationOutputRow>();
        while (iter.hasNext()){
            outputRowList.add(iter.next());
        }
        return outputRowList;
    }

    public Iterator<IndexVerificationOutputRow> getOutputRowIterator(long ts, byte[] indexName)
        throws IOException {
        Scan scan = new Scan();
        byte[] partialKey = generatePartialOutputTableRowKey(ts, indexName);
        scan.setStartRow(partialKey);
        scan.setStopRow(ByteUtil.calculateTheClosestNextRowKeyForPrefix(partialKey));
        ResultScanner scanner = outputTable.getScanner(scan);
        return new IndexVerificationOutputRowIterator(scanner.iterator());
    }

    @VisibleForTesting
    public Iterator<IndexVerificationOutputRow> getOutputRowIteratorForAllRows()
        throws IOException {
        Scan scan = new Scan();
        ResultScanner scanner = outputTable.getScanner(scan);
        return new IndexVerificationOutputRowIterator(scanner.iterator());
    }

    public static IndexVerificationOutputRow getOutputRowFromResult(Result result) {
        IndexVerificationOutputRow.IndexVerificationOutputRowBuilder builder =
            new IndexVerificationOutputRow.IndexVerificationOutputRowBuilder();
        byte[] rowKey = result.getRow();
        //rowkey is scanTs + SEPARATOR_BYTE + indexTableName + SEPARATOR_BYTE + dataTableRowKey
        byte[][] rowKeySplit = ByteUtil.splitArrayBySeparator(rowKey, ROW_KEY_SEPARATOR_BYTE[0]);
        builder.setScanMaxTimestamp(Long.parseLong(Bytes.toString(rowKeySplit[0])));
        builder.setIndexTableName(Bytes.toString(rowKeySplit[1]));
        builder.setDataTableRowKey(rowKeySplit[2]);

        builder.setDataTableName(Bytes.toString(result.getValue(OUTPUT_TABLE_COLUMN_FAMILY,
            DATA_TABLE_NAME_BYTES)));
        builder.setIndexTableRowKey(result.getValue(OUTPUT_TABLE_COLUMN_FAMILY,
            INDEX_TABLE_ROW_KEY_BYTES));
        builder.setDataTableRowTimestamp(Long.parseLong(Bytes.toString(result.getValue(OUTPUT_TABLE_COLUMN_FAMILY,
            DATA_TABLE_TS_BYTES))));
        builder.setIndexTableRowTimestamp(Long.parseLong(Bytes.toString(result.getValue(OUTPUT_TABLE_COLUMN_FAMILY,
            INDEX_TABLE_TS_BYTES))));
        builder.setErrorMessage(Bytes.toString(result.getValue(OUTPUT_TABLE_COLUMN_FAMILY,
            ERROR_MESSAGE_BYTES)));
        //actual and expected value might not be present, but will just set to null if not
        builder.setExpectedValue(result.getValue(OUTPUT_TABLE_COLUMN_FAMILY, EXPECTED_VALUE_BYTES));
        builder.setActualValue(result.getValue(OUTPUT_TABLE_COLUMN_FAMILY, ACTUAL_VALUE_BYTES));
        builder.setPhaseValue(result.getValue(OUTPUT_TABLE_COLUMN_FAMILY, VERIFICATION_PHASE_BYTES));
        IndexVerificationErrorType errorType;
        try {
          errorType =
              IndexVerificationErrorType.valueOf(
                  Bytes.toString(result.getValue(OUTPUT_TABLE_COLUMN_FAMILY, ERROR_TYPE_BYTES)));
        } catch (Throwable e) {
            //in case we have a cast exception because an incompatible version of the enum produced
            //the row, or an earlier version that didn't record error types, it's better to mark
            // the error type unknown and move on rather than fail
            errorType = IndexVerificationErrorType.UNKNOWN;
        }
        builder.setErrorType(errorType);
        return builder.build();
    }

    public void close() throws IOException {
        if (outputTable != null) {
            outputTable.close();
        }
        if (indexTable != null) {
            indexTable.close();
        }
    }

    public static class IndexVerificationOutputRowIterator implements Iterator<IndexVerificationOutputRow> {
        Iterator<Result> delegate;
        public IndexVerificationOutputRowIterator(Iterator<Result> delegate){
            this.delegate = delegate;
        }
        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public IndexVerificationOutputRow next() {
            Result result = delegate.next();
            if (result == null) {
                return null;
            } else {
                return getOutputRowFromResult(result);
            }
        }

        @Override
        public void remove() {
            delegate.remove();
        }

    }

    public void setIndexTable(Table indexTable) {
        this.indexTable = indexTable;
    }

    public void setOutputTable(Table outputTable) {
        this.outputTable = outputTable;
    }
}
