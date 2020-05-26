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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
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

public class IndexVerificationOutputRepository {
    public static final byte[] ROW_KEY_SEPARATOR_BYTE = Bytes.toBytes("|");
    private Table indexHTable;
    private byte[] indexName;
    private Table outputHTable;
    public final static String OUTPUT_TABLE_NAME = "PHOENIX_INDEX_TOOL";
    public final static byte[] OUTPUT_TABLE_NAME_BYTES = Bytes.toBytes(OUTPUT_TABLE_NAME);
    public final static byte[] OUTPUT_TABLE_COLUMN_FAMILY = QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES;

    public final static String DATA_TABLE_NAME = "DTName";
    public final static byte[] DATA_TABLE_NAME_BYTES = Bytes.toBytes(DATA_TABLE_NAME);
    public static String INDEX_TABLE_NAME = "ITName";
    public final static byte[] INDEX_TABLE_NAME_BYTES = Bytes.toBytes(INDEX_TABLE_NAME);
    public static String DATA_TABLE_ROW_KEY = "DTRowKey";
    public final static byte[] DATA_TABLE_ROW_KEY_BYTES = Bytes.toBytes(DATA_TABLE_ROW_KEY);
    public static String INDEX_TABLE_ROW_KEY = "ITRowKey";
    public final static byte[] INDEX_TABLE_ROW_KEY_BYTES = Bytes.toBytes(INDEX_TABLE_ROW_KEY);
    public static String DATA_TABLE_TS = "DTTS";
    public final static byte[] DATA_TABLE_TS_BYTES = Bytes.toBytes(DATA_TABLE_TS);
    public static String INDEX_TABLE_TS = "ITTS";
    public final static byte[] INDEX_TABLE_TS_BYTES = Bytes.toBytes(INDEX_TABLE_TS);
    public static String ERROR_MESSAGE = "Error";
    public final static byte[] ERROR_MESSAGE_BYTES = Bytes.toBytes(ERROR_MESSAGE);

    public static String  VERIFICATION_PHASE = "Phase";
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

    /**
     * Only usable for the read path or for testing
     */
    public IndexVerificationOutputRepository(){

    }

    @VisibleForTesting
    public IndexVerificationOutputRepository(byte[] indexName, Connection conn) throws SQLException {
        ConnectionQueryServices queryServices =
            conn.unwrap(PhoenixConnection.class).getQueryServices();
        outputHTable = queryServices.getTable(OUTPUT_TABLE_NAME_BYTES);
        indexHTable = queryServices.getTable(indexName);
    }

    public IndexVerificationOutputRepository(byte[] indexName, HTableFactory hTableFactory) throws IOException {
        this.indexName = indexName;
        outputHTable = hTableFactory.getTable(new ImmutableBytesPtr(OUTPUT_TABLE_NAME_BYTES));
        indexHTable = hTableFactory.getTable(new ImmutableBytesPtr(indexName));
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
        Admin admin = queryServices.getAdmin();
        if (!admin.tableExists(TableName.valueOf(OUTPUT_TABLE_NAME))) {
            HTableDescriptor tableDescriptor = new
                HTableDescriptor(TableName.valueOf(OUTPUT_TABLE_NAME));
            tableDescriptor.setValue(HColumnDescriptor.TTL,
                String.valueOf(MetaDataProtocol.DEFAULT_LOG_TTL));
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(OUTPUT_TABLE_COLUMN_FAMILY);
            tableDescriptor.addFamily(columnDescriptor);
            admin.createTable(tableDescriptor);
        }
    }
        
    @VisibleForTesting
    public void logToIndexToolOutputTable(byte[] dataRowKey, byte[] indexRowKey, long dataRowTs,
                                          long indexRowTs,
                                          String errorMsg, byte[] expectedValue, byte[] actualValue,
                                          long scanMaxTs, byte[] tableName, boolean isBeforeRebuild)
        throws IOException {
        byte[] rowKey = generateOutputTableRowKey(scanMaxTs, indexHTable.getName().toBytes(), dataRowKey);
        Put put = new Put(rowKey);
        put.addColumn(OUTPUT_TABLE_COLUMN_FAMILY, DATA_TABLE_NAME_BYTES,
            scanMaxTs, tableName);
        put.addColumn(OUTPUT_TABLE_COLUMN_FAMILY, INDEX_TABLE_NAME_BYTES,
            scanMaxTs, indexName);
        put.addColumn(OUTPUT_TABLE_COLUMN_FAMILY, DATA_TABLE_TS_BYTES,
            scanMaxTs, Bytes.toBytes(Long.toString(dataRowTs)));

        put.addColumn(OUTPUT_TABLE_COLUMN_FAMILY, INDEX_TABLE_ROW_KEY_BYTES,
            scanMaxTs, indexRowKey);
        put.addColumn(OUTPUT_TABLE_COLUMN_FAMILY, INDEX_TABLE_TS_BYTES,
            scanMaxTs, Bytes.toBytes(Long.toString(indexRowTs)));
        byte[] errorMessageBytes;
        if (expectedValue != null) {
            errorMessageBytes = getErrorMessageBytes(errorMsg, expectedValue, actualValue);
            put.addColumn(OUTPUT_TABLE_COLUMN_FAMILY, EXPECTED_VALUE_BYTES, expectedValue);
            put.addColumn(OUTPUT_TABLE_COLUMN_FAMILY, ACTUAL_VALUE_BYTES, actualValue);
        } else {
            errorMessageBytes = Bytes.toBytes(errorMsg);
        }
        put.addColumn(OUTPUT_TABLE_COLUMN_FAMILY, ERROR_MESSAGE_BYTES, scanMaxTs, errorMessageBytes);
        if (isBeforeRebuild) {
            put.addColumn(OUTPUT_TABLE_COLUMN_FAMILY, VERIFICATION_PHASE_BYTES, scanMaxTs, PHASE_BEFORE_VALUE);
        } else {
            put.addColumn(OUTPUT_TABLE_COLUMN_FAMILY, VERIFICATION_PHASE_BYTES, scanMaxTs, PHASE_AFTER_VALUE);
        }
        outputHTable.put(put);
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
        scan.withStartRow(partialKey);
        scan.withStopRow(ByteUtil.calculateTheClosestNextRowKeyForPrefix(partialKey));
        ResultScanner scanner = outputHTable.getScanner(scan);
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
        return builder.build();
    }

    public void close() throws IOException {
        if (outputHTable != null) {
            outputHTable.close();
        }
        if (indexHTable != null) {
            indexHTable.close();
        }
    }

    public class IndexVerificationOutputRowIterator implements Iterator<IndexVerificationOutputRow> {
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
}
