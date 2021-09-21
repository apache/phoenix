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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.IndexToolVerificationResult;
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

public class IndexVerificationResultRepository implements AutoCloseable {

    public static final String RUN_STATUS_SKIPPED = "Skipped";
    public static final String RUN_STATUS_EXECUTED = "Executed";
    private Table resultTable;
    private Table indexTable;
    public static final String ROW_KEY_SEPARATOR = "|";
    public static final byte[] ROW_KEY_SEPARATOR_BYTE = Bytes.toBytes(ROW_KEY_SEPARATOR);
    public final static String RESULT_TABLE_NAME = "PHOENIX_INDEX_TOOL_RESULT";
    public final static byte[] RESULT_TABLE_NAME_BYTES = Bytes.toBytes(RESULT_TABLE_NAME);
    public final static byte[] RESULT_TABLE_COLUMN_FAMILY = QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES;
    public final static String SCANNED_DATA_ROW_COUNT = "ScannedDataRowCount";
    public final static byte[] SCANNED_DATA_ROW_COUNT_BYTES = Bytes.toBytes(SCANNED_DATA_ROW_COUNT);
    public final static String REBUILT_INDEX_ROW_COUNT = "RebuiltIndexRowCount";
    public final static byte[] REBUILT_INDEX_ROW_COUNT_BYTES = Bytes.toBytes(REBUILT_INDEX_ROW_COUNT);
    public final static String BEFORE_REBUILD_VALID_INDEX_ROW_COUNT =
        "BeforeRebuildValidIndexRowCount";
    public final static byte[] BEFORE_REBUILD_VALID_INDEX_ROW_COUNT_BYTES = Bytes.toBytes(BEFORE_REBUILD_VALID_INDEX_ROW_COUNT);
    private static final String INDEX_TOOL_RUN_STATUS = "IndexToolRunStatus";
    public final static byte[] INDEX_TOOL_RUN_STATUS_BYTES = Bytes.toBytes(INDEX_TOOL_RUN_STATUS);
    public final static String BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT =
        "BeforeRebuildExpiredIndexRowCount";
    public final static byte[] BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT_BYTES = Bytes.toBytes(BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT);
    public final static String BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT =
        "BeforeRebuildMissingIndexRowCount";
    public final static byte[] BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT_BYTES = Bytes.toBytes(BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT);
    public final static String BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT =
        "BeforeRebuildInvalidIndexRowCount";
    public final static byte[] BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_BYTES = Bytes.toBytes(BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT);
    public final static String BEFORE_REBUILD_UNVERIFIED_INDEX_ROW_COUNT =
            "BeforeRebuildUnverifiedIndexRowCount";
    public final static byte[] BEFORE_REBUILD_UNVERIFIED_INDEX_ROW_COUNT_BYTES = Bytes.toBytes(BEFORE_REBUILD_UNVERIFIED_INDEX_ROW_COUNT);
    public final static String BEFORE_REBUILD_OLD_INDEX_ROW_COUNT =
            "BeforeRebuildOldIndexRowCount";
    public final static byte[] BEFORE_REBUILD_OLD_INDEX_ROW_COUNT_BYTES = Bytes.toBytes(BEFORE_REBUILD_OLD_INDEX_ROW_COUNT);
    public final static String BEFORE_REBUILD_UNKNOWN_INDEX_ROW_COUNT =
            "BeforeRebuildUnknownIndexRowCount";
    public final static byte[] BEFORE_REBUILD_UNKNOWN_INDEX_ROW_COUNT_BYTES = Bytes.toBytes(BEFORE_REBUILD_UNKNOWN_INDEX_ROW_COUNT);
    public final static String AFTER_REBUILD_VALID_INDEX_ROW_COUNT =
        "AfterRebuildValidIndexRowCount";
    public final static byte[] AFTER_REBUILD_VALID_INDEX_ROW_COUNT_BYTES = Bytes.toBytes(AFTER_REBUILD_VALID_INDEX_ROW_COUNT);
    public final static String AFTER_REBUILD_EXPIRED_INDEX_ROW_COUNT =
        "AfterRebuildExpiredIndexRowCount";
    public final static byte[] AFTER_REBUILD_EXPIRED_INDEX_ROW_COUNT_BYTES = Bytes.toBytes(AFTER_REBUILD_EXPIRED_INDEX_ROW_COUNT);
    public final static String AFTER_REBUILD_MISSING_INDEX_ROW_COUNT =
        "AfterRebuildMissingIndexRowCount";
    public final static byte[] AFTER_REBUILD_MISSING_INDEX_ROW_COUNT_BYTES = Bytes.toBytes(AFTER_REBUILD_MISSING_INDEX_ROW_COUNT);
    public final static String AFTER_REBUILD_INVALID_INDEX_ROW_COUNT =
        "AfterRebuildInvalidIndexRowCount";
    public final static byte[] AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_BYTES = Bytes.toBytes(AFTER_REBUILD_INVALID_INDEX_ROW_COUNT);
    public final static String BEFORE_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT =
        "BeforeRebuildBeyondMaxLookBackMissingIndexRowCount";
    public final static byte[] BEFORE_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT_BYTES =
        Bytes.toBytes(BEFORE_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT);
    public final static String BEFORE_REBUILD_BEYOND_MAXLOOKBACK_INVALID_INDEX_ROW_COUNT =
        "BeforeRebuildBeyondMaxLookBackInvalidIndexRowCount";
    public final static byte[] BEFORE_REBUILD_BEYOND_MAXLOOKBACK_INVALID_INDEX_ROW_COUNT_BYTES =
        Bytes.toBytes(BEFORE_REBUILD_BEYOND_MAXLOOKBACK_INVALID_INDEX_ROW_COUNT);

    public final static String AFTER_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT =
        "AfterRebuildBeyondMaxLookBackMissingIndexRowCount";
    public final static byte[] AFTER_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT_BYTES =
        Bytes.toBytes(AFTER_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT);
    public final static String AFTER_REBUILD_BEYOND_MAXLOOKBACK_INVALID_INDEX_ROW_COUNT =
        "AfterRebuildBeyondMaxLookBackInvalidIndexRowCount";
    public final static byte[] AFTER_REBUILD_BEYOND_MAXLOOKBACK_INVALID_INDEX_ROW_COUNT_BYTES =
        Bytes.toBytes(AFTER_REBUILD_BEYOND_MAXLOOKBACK_INVALID_INDEX_ROW_COUNT);

    public final static String BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_EXTRA_CELLS = "BeforeRebuildInvalidIndexRowCountCozExtraCells";
    public final static byte[] BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_EXTRA_CELLS_BYTES = Bytes.toBytes(BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_EXTRA_CELLS);
    public final static String BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_MISSING_CELLS = "BeforeRebuildInvalidIndexRowCountCozMissingCells";
    public final static byte[] BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_MISSING_CELLS_BYTES = Bytes.toBytes(BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_MISSING_CELLS);

    public final static String AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_EXTRA_CELLS = "AfterRebuildInvalidIndexRowCountCozExtraCells";
    public final static byte[] AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_EXTRA_CELLS_BYTES = Bytes.toBytes(AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_EXTRA_CELLS);
    public final static String AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_MISSING_CELLS = "AfterRebuildInvalidIndexRowCountCozMissingCells";
    public final static byte[] AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_MISSING_CELLS_BYTES = Bytes.toBytes(AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_MISSING_CELLS);

    public final static String BEFORE_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT = "BeforeRepairExtraVerifiedIndexRowCount";
    public final static byte[] BEFORE_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT_BYTES =
        Bytes.toBytes(BEFORE_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT);
    public final static String BEFORE_REPAIR_EXTRA_UNVERIFIED_INDEX_ROW_COUNT = "BeforeRepairExtraUnverifiedIndexRowCount";
    public final static byte[] BEFORE_REPAIR_EXTRA_UNVERIFIED_INDEX_ROW_COUNT_BYTES =
        Bytes.toBytes(BEFORE_REPAIR_EXTRA_UNVERIFIED_INDEX_ROW_COUNT);

    public final static String AFTER_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT = "AfterRepairExtraVerifiedIndexRowCount";
    public final static byte[] AFTER_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT_BYTES =
        Bytes.toBytes(AFTER_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT);
    public final static String AFTER_REPAIR_EXTRA_UNVERIFIED_INDEX_ROW_COUNT = "AfterRepairExtraUnverifiedIndexRowCount";
    public final static byte[] AFTER_REPAIR_EXTRA_UNVERIFIED_INDEX_ROW_COUNT_BYTES =
        Bytes.toBytes(AFTER_REPAIR_EXTRA_UNVERIFIED_INDEX_ROW_COUNT);

    /***
     * Only usable for read / create methods. To write use setResultTable and setIndexTable first
     */
    public IndexVerificationResultRepository(){

    }

    public IndexVerificationResultRepository(Connection conn, byte[] indexNameBytes) throws SQLException {
        resultTable = getTable(conn, RESULT_TABLE_NAME_BYTES);
        indexTable = getTable(conn, indexNameBytes);
    }

    public IndexVerificationResultRepository(byte[] indexName,
                                             HTableFactory hTableFactory) throws IOException {
        resultTable = hTableFactory.getTable(new ImmutableBytesPtr(RESULT_TABLE_NAME_BYTES));
        indexTable = hTableFactory.getTable(new ImmutableBytesPtr(indexName));
    }

    public void createResultTable(Connection connection) throws IOException, SQLException {
        ConnectionQueryServices queryServices = connection.unwrap(PhoenixConnection.class).getQueryServices();
        try (Admin admin = queryServices.getAdmin()) {
            TableName resultTableName = TableName.valueOf(RESULT_TABLE_NAME);
            if (!admin.tableExists(resultTableName)) {
                ColumnFamilyDescriptor columnDescriptor =
                    ColumnFamilyDescriptorBuilder
                        .newBuilder(RESULT_TABLE_COLUMN_FAMILY)
                        .setTimeToLive(MetaDataProtocol.DEFAULT_LOG_TTL)
                        .build();
                TableDescriptor tableDescriptor =
                    TableDescriptorBuilder.newBuilder(resultTableName)
                        .setColumnFamily(columnDescriptor).build();
                admin.createTable(tableDescriptor);
                resultTable = admin.getConnection().getTable(resultTableName);
            }
        }
    }

    private static byte[] generatePartialResultTableRowKey(long ts, byte[] indexTableName) {
        byte[] keyPrefix = Bytes.toBytes(Long.toString(ts));
        int targetOffset = 0;
        // The row key for the result table : timestamp | index table name | datable table region name |
        //                                    scan start row | scan stop row
        byte[] partialRowKey = new byte[keyPrefix.length + ROW_KEY_SEPARATOR_BYTE.length
            + indexTableName.length];
        Bytes.putBytes(partialRowKey, targetOffset, keyPrefix, 0, keyPrefix.length);
        targetOffset += keyPrefix.length;
        Bytes.putBytes(partialRowKey, targetOffset, ROW_KEY_SEPARATOR_BYTE, 0, ROW_KEY_SEPARATOR_BYTE.length);
        targetOffset += ROW_KEY_SEPARATOR_BYTE.length;
        Bytes.putBytes(partialRowKey, targetOffset, indexTableName, 0, indexTableName.length);
        return partialRowKey;
    }

    private static byte[] generateResultTableRowKey(long ts, byte[] indexTableName,  byte [] regionName,
                                                    byte[] startRow, byte[] stopRow) {
        byte[] keyPrefix = Bytes.toBytes(Long.toString(ts));
        int targetOffset = 0;
        // The row key for the result table : timestamp | index table name | datable table region name |
        //                                    scan start row | scan stop row
        byte[] rowKey = new byte[keyPrefix.length + ROW_KEY_SEPARATOR_BYTE.length + indexTableName.length +
            ROW_KEY_SEPARATOR_BYTE.length + regionName.length + ROW_KEY_SEPARATOR_BYTE.length +
            startRow.length + ROW_KEY_SEPARATOR_BYTE.length + stopRow.length];
        Bytes.putBytes(rowKey, targetOffset, keyPrefix, 0, keyPrefix.length);
        targetOffset += keyPrefix.length;
        Bytes.putBytes(rowKey, targetOffset, ROW_KEY_SEPARATOR_BYTE, 0, ROW_KEY_SEPARATOR_BYTE.length);
        targetOffset += ROW_KEY_SEPARATOR_BYTE.length;
        Bytes.putBytes(rowKey, targetOffset, indexTableName, 0, indexTableName.length);
        targetOffset += indexTableName.length;
        Bytes.putBytes(rowKey, targetOffset, ROW_KEY_SEPARATOR_BYTE, 0, ROW_KEY_SEPARATOR_BYTE.length);
        targetOffset += ROW_KEY_SEPARATOR_BYTE.length;
        Bytes.putBytes(rowKey, targetOffset, regionName, 0, regionName.length);
        targetOffset += regionName.length;
        Bytes.putBytes(rowKey, targetOffset, ROW_KEY_SEPARATOR_BYTE, 0, ROW_KEY_SEPARATOR_BYTE.length);
        targetOffset += ROW_KEY_SEPARATOR_BYTE.length;
        Bytes.putBytes(rowKey, targetOffset, startRow, 0, startRow.length);
        targetOffset += startRow.length;
        Bytes.putBytes(rowKey, targetOffset, ROW_KEY_SEPARATOR_BYTE, 0, ROW_KEY_SEPARATOR_BYTE.length);
        targetOffset += ROW_KEY_SEPARATOR_BYTE.length;
        Bytes.putBytes(rowKey, targetOffset, stopRow, 0, stopRow.length);
        return rowKey;
    }

    public void logToIndexToolResultTable(IndexToolVerificationResult verificationResult,
            IndexTool.IndexVerifyType verifyType, byte[] region) throws IOException {
            logToIndexToolResultTable(verificationResult, verifyType, region, false);
    }

    public void logToIndexToolResultTable(IndexToolVerificationResult verificationResult,
                                          IndexTool.IndexVerifyType verifyType, byte[] region, boolean skipped) throws IOException {
        long scanMaxTs = verificationResult.getScanMaxTs();
        byte[] rowKey = generateResultTableRowKey(scanMaxTs, indexTable.getName().toBytes(),
            region, verificationResult.getStartRow(),
            verificationResult.getStopRow());
        Put put = new Put(rowKey);
        put.addColumn(RESULT_TABLE_COLUMN_FAMILY, SCANNED_DATA_ROW_COUNT_BYTES,
                Bytes.toBytes(Long.toString(verificationResult.getScannedDataRowCount())));
        put.addColumn(RESULT_TABLE_COLUMN_FAMILY, REBUILT_INDEX_ROW_COUNT_BYTES,
                Bytes.toBytes(Long.toString(verificationResult.getRebuiltIndexRowCount())));
        put.addColumn(RESULT_TABLE_COLUMN_FAMILY, INDEX_TOOL_RUN_STATUS_BYTES,
                Bytes.toBytes(skipped ? RUN_STATUS_SKIPPED : RUN_STATUS_EXECUTED));
        if (verifyType == IndexTool.IndexVerifyType.BEFORE || verifyType == IndexTool.IndexVerifyType.BOTH ||
            verifyType == IndexTool.IndexVerifyType.ONLY) {
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_VALID_INDEX_ROW_COUNT_BYTES,
                    Bytes.toBytes(Long.toString(verificationResult.getBeforeRebuildValidIndexRowCount())));
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT_BYTES,
                Bytes.toBytes(Long.toString(verificationResult.getBeforeRebuildExpiredIndexRowCount())));
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT_BYTES,
                Bytes.toBytes(Long.toString(verificationResult.getBeforeRebuildMissingIndexRowCount())));
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_BYTES,
                Bytes.toBytes(Long.toString(verificationResult.getBeforeRebuildInvalidIndexRowCount())));
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT_BYTES,
                Bytes.toBytes(Long.toString(verificationResult.getBefore().getBeyondMaxLookBackMissingIndexRowCount())));
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_BEYOND_MAXLOOKBACK_INVALID_INDEX_ROW_COUNT_BYTES,
                Bytes.toBytes(Long.toString(verificationResult.getBefore().getBeyondMaxLookBackInvalidIndexRowCount())));
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_EXTRA_CELLS_BYTES,
                Bytes.toBytes(Long.toString(verificationResult.getBeforeIndexHasExtraCellsCount())));
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_MISSING_CELLS_BYTES,
                Bytes.toBytes(Long.toString(verificationResult.getBeforeIndexHasMissingCellsCount())));
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_UNVERIFIED_INDEX_ROW_COUNT_BYTES,
                    Bytes.toBytes(Long.toString(verificationResult.getBeforeRebuildUnverifiedIndexRowCount())));
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_OLD_INDEX_ROW_COUNT_BYTES,
                    Bytes.toBytes(Long.toString(verificationResult.getBeforeRebuildOldIndexRowCount())));
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_UNKNOWN_INDEX_ROW_COUNT_BYTES,
                    Bytes.toBytes(Long.toString(verificationResult.getBeforeRebuildUnknownIndexRowCount())));
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, BEFORE_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT_BYTES,
                Bytes.toBytes(Long.toString(verificationResult.getBeforeRepairExtraVerifiedIndexRowCount())));
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, BEFORE_REPAIR_EXTRA_UNVERIFIED_INDEX_ROW_COUNT_BYTES,
                Bytes.toBytes(Long.toString(verificationResult.getBeforeRepairExtraUnverifiedIndexRowCount())));
        }
        if (verifyType == IndexTool.IndexVerifyType.AFTER || verifyType == IndexTool.IndexVerifyType.BOTH) {
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_VALID_INDEX_ROW_COUNT_BYTES,
                Bytes.toBytes(Long.toString(verificationResult.getAfterRebuildValidIndexRowCount())));
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_EXPIRED_INDEX_ROW_COUNT_BYTES,
                Bytes.toBytes(Long.toString(verificationResult.getAfterRebuildExpiredIndexRowCount())));
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_MISSING_INDEX_ROW_COUNT_BYTES,
                Bytes.toBytes(Long.toString(verificationResult.getAfterRebuildMissingIndexRowCount())));
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_BYTES,
                Bytes.toBytes(Long.toString(verificationResult.getAfterRebuildInvalidIndexRowCount())));
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT_BYTES,
                Bytes.toBytes(Long.toString(verificationResult.getAfter().getBeyondMaxLookBackMissingIndexRowCount())));
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_BEYOND_MAXLOOKBACK_INVALID_INDEX_ROW_COUNT_BYTES,
                Bytes.toBytes(Long.toString(verificationResult.getAfter().getBeyondMaxLookBackInvalidIndexRowCount())));
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_EXTRA_CELLS_BYTES,
                Bytes.toBytes(Long.toString(verificationResult.getAfterIndexHasExtraCellsCount())));
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_MISSING_CELLS_BYTES,
                Bytes.toBytes(Long.toString(verificationResult.getAfterIndexHasMissingCellsCount())));
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, AFTER_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT_BYTES,
                Bytes.toBytes(Long.toString(verificationResult.getAfterRepairExtraVerifiedIndexRowCount())));
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, AFTER_REPAIR_EXTRA_UNVERIFIED_INDEX_ROW_COUNT_BYTES,
                Bytes.toBytes(Long.toString(verificationResult.getAfterRepairExtraUnverifiedIndexRowCount())));
        }
        resultTable.put(put);
    }

    public Table getTable(Connection conn, byte[] tableName) throws SQLException {
        return conn.unwrap(PhoenixConnection.class).getQueryServices()
                .getTable(tableName);
    }

    /**
     * Get aggregated verification results from
     * {@link #aggregateVerificationResult(Table, IndexToolVerificationResult,
     * Scan)}
     * Provided Table reference should be closed by caller.
     *
     * @param htable Table reference. It is caller's responsibility to close
     *     this reference.
     * @param ts timestamp used for Scan's startRow
     * @return Verification result
     * @throws IOException if something goes wrong while retrieving verification
     *     results.
     */
    public IndexToolVerificationResult getVerificationResult(Table htable, long ts)
        throws IOException {
        byte[] startRowKey = Bytes.toBytes(Long.toString(ts));
        byte[] stopRowKey = ByteUtil.calculateTheClosestNextRowKeyForPrefix(startRowKey);
        IndexToolVerificationResult verificationResult = new IndexToolVerificationResult(ts);
        Scan scan = new Scan();
        scan.setStartRow(startRowKey);
        scan.setStopRow(stopRowKey);
        return aggregateVerificationResult(htable, verificationResult, scan);
    }

    private IndexToolVerificationResult aggregateVerificationResult(
            Table hTable, IndexToolVerificationResult verificationResult,
            Scan scan) throws IOException {
        try (ResultScanner scanner = hTable.getScanner(scan)) {
            for (Result result = scanner.next(); result != null;
                    result = scanner.next()) {
                boolean isFirst = true;
                for (Cell cell : result.rawCells()) {
                    if (isFirst) {
                        byte[][] rowKeyParts = ByteUtil.splitArrayBySeparator(
                            result.getRow(), ROW_KEY_SEPARATOR_BYTE[0]);
                        verificationResult.setStartRow(rowKeyParts[3]);
                        verificationResult.setStopRow(rowKeyParts[4]);
                        isFirst = false;
                    }
                    verificationResult.update(cell);
                }
            }
        }
        return verificationResult;
    }

    public IndexToolVerificationResult getVerificationResult(Connection conn,
            long ts, byte[] indexTableName) throws IOException, SQLException {
        try (Table hTable = getTable(conn, RESULT_TABLE_NAME_BYTES)) {
            byte[] startRowKey = generatePartialResultTableRowKey(ts,
                indexTableName);
            byte[] stopRowKey = ByteUtil.calculateTheClosestNextRowKeyForPrefix(
                startRowKey);
            IndexToolVerificationResult verificationResult =
                new IndexToolVerificationResult(ts);
            Scan scan = new Scan();
            scan.setStartRow(startRowKey);
            scan.setStopRow(stopRowKey);
            return aggregateVerificationResult(hTable, verificationResult,
                scan);
        }
    }

    private IndexToolVerificationResult getVerificationResult(Table htable, byte [] oldRowKey, Scan scan )
            throws IOException {
        IndexToolVerificationResult verificationResult = null;
        Result result = htable.get(new Get(oldRowKey));
        if(!result.isEmpty()) {
            byte[][] rowKeyParts = ByteUtil.splitArrayBySeparator(result.getRow(), ROW_KEY_SEPARATOR_BYTE[0]);
            verificationResult = new IndexToolVerificationResult(scan);
            verificationResult.setStartRow(rowKeyParts[3]);
            verificationResult.setStopRow(rowKeyParts[4]);
            for (Cell cell : result.rawCells()) {
                verificationResult.update(cell);
            }
        }
        return verificationResult;
    }

    public void close() throws IOException {
        if (resultTable != null) {
            resultTable.close();
        }
        if (indexTable != null) {
            indexTable.close();
        }
    }

    public void setResultTable(Table resultTable) {
        this.resultTable = resultTable;
    }

    public void setIndexTable(Table indexTable) {
        this.indexTable = indexTable;
    }

    public IndexToolVerificationResult getVerificationResult(Long ts, Scan scan, Region region, byte[] indexTableName) throws IOException {
        byte [] rowKey = generateResultTableRowKey(ts,
                indexTableName, region.getRegionInfo().getRegionName(),
                scan.getStartRow(), scan.getStopRow());
         return getVerificationResult(resultTable, rowKey, scan);

    }
}

