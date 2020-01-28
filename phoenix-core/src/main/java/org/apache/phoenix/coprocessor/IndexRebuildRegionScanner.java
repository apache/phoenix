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
package org.apache.phoenix.coprocessor;

import static org.apache.phoenix.hbase.index.IndexRegionObserver.VERIFIED_BYTES;
import static org.apache.phoenix.hbase.index.IndexRegionObserver.removeEmptyColumn;
import static org.apache.phoenix.hbase.index.write.AbstractParallelWriterIndexCommitter.INDEX_WRITER_KEEP_ALIVE_TIME_CONF_KEY;
import static org.apache.phoenix.mapreduce.index.IndexTool.AFTER_REBUILD_EXPIRED_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexTool.AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexTool.AFTER_REBUILD_MISSING_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexTool.AFTER_REBUILD_VALID_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexTool.BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexTool.BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexTool.BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexTool.BEFORE_REBUILD_VALID_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexTool.REBUILT_INDEX_ROW_COUNT_BYTES;
import static org.apache.phoenix.mapreduce.index.IndexTool.RESULT_TABLE_COLUMN_FAMILY;
import static org.apache.phoenix.mapreduce.index.IndexTool.SCANNED_DATA_ROW_COUNT_BYTES;
import static org.apache.phoenix.query.QueryConstants.AGG_TIMESTAMP;
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN;
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN_FAMILY;
import static org.apache.phoenix.query.QueryConstants.UNGROUPED_AGG_ROW_KEY;
import static org.apache.phoenix.query.QueryServices.INDEX_REBUILD_PAGE_SIZE_IN_ROWS;
import static org.apache.phoenix.query.QueryServices.MUTATE_BATCH_SIZE_ATTRIB;
import static org.apache.phoenix.query.QueryServices.MUTATE_BATCH_SIZE_BYTES_ATTRIB;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.phoenix.cache.ServerCacheClient;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.hbase.index.ValueGetter;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.parallel.EarlyExitFailure;
import org.apache.phoenix.hbase.index.parallel.Task;
import org.apache.phoenix.hbase.index.parallel.TaskBatch;
import org.apache.phoenix.hbase.index.parallel.TaskRunner;
import org.apache.phoenix.hbase.index.parallel.ThreadPoolBuilder;
import org.apache.phoenix.hbase.index.parallel.ThreadPoolManager;
import org.apache.phoenix.hbase.index.parallel.WaitForCompletionTaskRunner;
import org.apache.phoenix.hbase.index.util.GenericKeyValueBuilder;
import org.apache.phoenix.index.GlobalIndexChecker;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.util.PhoenixKeyValueUtil;
import org.apache.phoenix.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class IndexRebuildRegionScanner extends BaseRegionScanner {

    public static class VerificationResult {
        public static class PhaseResult {
            private long validIndexRowCount = 0;
            private long expiredIndexRowCount = 0;
            private long missingIndexRowCount = 0;
            private long invalidIndexRowCount = 0;

            public void add(PhaseResult phaseResult) {
                validIndexRowCount += phaseResult.validIndexRowCount;
                expiredIndexRowCount += phaseResult.expiredIndexRowCount;
                missingIndexRowCount += phaseResult.missingIndexRowCount;
                invalidIndexRowCount += phaseResult.invalidIndexRowCount;
            }

            public long getTotalCount() {
                return validIndexRowCount + expiredIndexRowCount + missingIndexRowCount + invalidIndexRowCount;
            }

            @Override
            public String toString() {
                return "PhaseResult{" +
                        "validIndexRowCount=" + validIndexRowCount +
                        ", expiredIndexRowCount=" + expiredIndexRowCount +
                        ", missingIndexRowCount=" + missingIndexRowCount +
                        ", invalidIndexRowCount=" + invalidIndexRowCount +
                        '}';
            }
        }

        private long scannedDataRowCount = 0;
        private long rebuiltIndexRowCount = 0;
        private PhaseResult before = new PhaseResult();
        private PhaseResult after = new PhaseResult();

        @Override
        public String toString() {
            return "VerificationResult{" +
                    "scannedDataRowCount=" + scannedDataRowCount +
                    ", rebuiltIndexRowCount=" + rebuiltIndexRowCount +
                    ", before=" + before +
                    ", after=" + after +
                    '}';
        }

        public long getScannedDataRowCount() {
            return scannedDataRowCount;
        }

        public long getRebuiltIndexRowCount() {
            return rebuiltIndexRowCount;
        }

        public long getBeforeRebuildValidIndexRowCount() {
            return before.validIndexRowCount;
        }

        public long getBeforeRebuildExpiredIndexRowCount() {
            return before.expiredIndexRowCount;
        }

        public long getBeforeRebuildInvalidIndexRowCount() {
            return before.invalidIndexRowCount;
        }

        public long getBeforeRebuildMissingIndexRowCount() {
            return before.missingIndexRowCount;
        }

        public long getAfterRebuildValidIndexRowCount() {
            return after.validIndexRowCount;
        }

        public long getAfterRebuildExpiredIndexRowCount() {
            return after.expiredIndexRowCount;
        }

        public long getAfterRebuildInvalidIndexRowCount() {
            return after.invalidIndexRowCount;
        }

        public long getAfterRebuildMissingIndexRowCount() {
            return after.missingIndexRowCount;
        }

        private void addScannedDataRowCount(long count) {
            this.scannedDataRowCount += count;
        }

        private void addRebuiltIndexRowCount(long count) {
            this.rebuiltIndexRowCount += count;
        }

        private void addBeforeRebuildValidIndexRowCount(long count) {
            before.validIndexRowCount += count;
        }

        private void addBeforeRebuildExpiredIndexRowCount(long count) {
            before.expiredIndexRowCount += count;
        }

        private void addBeforeRebuildMissingIndexRowCount(long count) {
            before.missingIndexRowCount += count;
        }

        private void addBeforeRebuildInvalidIndexRowCount(long count) {
            before.invalidIndexRowCount += count;
        }

        private void addAfterRebuildValidIndexRowCount(long count) {
            after.validIndexRowCount += count;
        }

        private void addAfterRebuildExpiredIndexRowCount(long count) {
            after.expiredIndexRowCount += count;
        }

        private void addAfterRebuildMissingIndexRowCount(long count) {
            after.missingIndexRowCount += count;
        }

        private void addAfterRebuildInvalidIndexRowCount(long count) {
            after.invalidIndexRowCount += count;
        }

        private static boolean isAfterRebuildInvalidIndexRowCount(Cell cell) {
            if (Bytes.compareTo(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                    AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_BYTES, 0,
                    AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_BYTES.length) == 0) {
                return true;
            }
            return false;
        }

        private long getValue(Cell cell) {
            return Long.parseLong(Bytes.toString(cell.getValueArray(),
                    cell.getValueOffset(), cell.getValueLength()));
        }

        private void update(Cell cell) {
            if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, SCANNED_DATA_ROW_COUNT_BYTES)) {
                addScannedDataRowCount(getValue(cell));
            } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, REBUILT_INDEX_ROW_COUNT_BYTES)) {
                addRebuiltIndexRowCount(getValue(cell));
            } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_VALID_INDEX_ROW_COUNT_BYTES)) {
                addBeforeRebuildValidIndexRowCount(getValue(cell));
            } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT_BYTES)) {
                addBeforeRebuildExpiredIndexRowCount(getValue(cell));
            } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT_BYTES)) {
                addBeforeRebuildMissingIndexRowCount(getValue(cell));
            } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_BYTES)) {
                addBeforeRebuildInvalidIndexRowCount(getValue(cell));
            } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_VALID_INDEX_ROW_COUNT_BYTES)) {
                addAfterRebuildValidIndexRowCount(getValue(cell));
            } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_EXPIRED_INDEX_ROW_COUNT_BYTES)) {
                addAfterRebuildExpiredIndexRowCount(getValue(cell));
            } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_MISSING_INDEX_ROW_COUNT_BYTES)) {
                addAfterRebuildMissingIndexRowCount(getValue(cell));
            } else if (CellUtil.matchingColumn(cell, RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_BYTES)) {
                addAfterRebuildInvalidIndexRowCount(getValue(cell));
            }
        }

        public static byte[] calculateTheClosestNextRowKeyForPrefix(byte[] rowKeyPrefix) {
            // Essentially we are treating it like an 'unsigned very very long' and doing +1 manually.
            // Search for the place where the trailing 0xFFs start
            int offset = rowKeyPrefix.length;
            while (offset > 0) {
                if (rowKeyPrefix[offset - 1] != (byte) 0xFF) {
                    break;
                }
                offset--;
            }
            if (offset == 0) {
                // We got an 0xFFFF... (only FFs) stopRow value which is
                // the last possible prefix before the end of the table.
                // So set it to stop at the 'end of the table'
                return HConstants.EMPTY_END_ROW;
            }
            // Copy the right length of the original
            byte[] newStopRow = Arrays.copyOfRange(rowKeyPrefix, 0, offset);
            // And increment the last one
            newStopRow[newStopRow.length - 1]++;
            return newStopRow;
        }

        public static VerificationResult getVerificationResult(Table hTable, long ts)
                throws IOException {
            VerificationResult verificationResult = new VerificationResult();
            byte[] startRowKey = Bytes.toBytes(Long.toString(ts));
            byte[] stopRowKey = calculateTheClosestNextRowKeyForPrefix(startRowKey);
            Scan scan = new Scan();
            scan.setStartRow(startRowKey);
            scan.setStopRow(stopRowKey);
            ResultScanner scanner = hTable.getScanner(scan);
            for (Result result = scanner.next(); result != null; result = scanner.next()) {
                for (Cell cell : result.rawCells()) {
                    verificationResult.update(cell);
                }
            }
            return verificationResult;
        }

        public boolean isVerificationFailed(IndexTool.IndexVerifyType verifyType) {
            if (verifyType == IndexTool.IndexVerifyType.BEFORE || verifyType == IndexTool.IndexVerifyType.NONE) {
                return false;
            }
            if (verifyType == IndexTool.IndexVerifyType.ONLY) {
                if (before.validIndexRowCount + before.expiredIndexRowCount != scannedDataRowCount) {
                    return true;
                }
            }
            if (verifyType == IndexTool.IndexVerifyType.BOTH || verifyType == IndexTool.IndexVerifyType.AFTER) {
                if (after.invalidIndexRowCount + after.missingIndexRowCount > 0) {
                    return true;
                }
                if (before.validIndexRowCount + before.expiredIndexRowCount +
                        after.expiredIndexRowCount + after.validIndexRowCount != scannedDataRowCount) {
                    return true;
                }
            }
            return false;
        }

        public void add(VerificationResult verificationResult) {
            scannedDataRowCount += verificationResult.scannedDataRowCount;
            rebuiltIndexRowCount += verificationResult.rebuiltIndexRowCount;
            before.add(verificationResult.before);
            after.add(verificationResult.after);
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexRebuildRegionScanner.class);
    public static final String NUM_CONCURRENT_INDEX_VERIFY_THREADS_CONF_KEY = "index.verify.threads.max";
    private static final int DEFAULT_CONCURRENT_INDEX_VERIFY_THREADS = 17;
    public static final String INDEX_VERIFY_ROW_COUNTS_PER_TASK_CONF_KEY = "index.verify.threads.max";
    private static final int DEFAULT_INDEX_VERIFY_ROW_COUNTS_PER_TASK = 2048;
    private long pageSizeInRows = Long.MAX_VALUE;
    private int rowCountPerTask;
    private boolean hasMore;
    private final int maxBatchSize;
    private UngroupedAggregateRegionObserver.MutationList mutations;
    private final long maxBatchSizeBytes;
    private final long blockingMemstoreSize;
    private final byte[] clientVersionBytes;
    private byte[] indexMetaData;
    private boolean useProto = true;
    private Scan scan;
    private RegionScanner innerScanner;
    private Region region;
    private IndexMaintainer indexMaintainer;
    private byte[] indexRowKey = null;
    private Table indexHTable = null;
    private Table outputHTable = null;
    private Table resultHTable = null;
    private IndexTool.IndexVerifyType verifyType = IndexTool.IndexVerifyType.NONE;
    private boolean verify = false;
    private Map<byte[], Put> indexKeyToDataPutMap;
    private Map<byte[], Put> dataKeyToDataPutMap;
    private TaskRunner pool;
    private TaskBatch<Boolean> tasks;
    private String exceptionMessage;
    private UngroupedAggregateRegionObserver ungroupedAggregateRegionObserver;
    private RegionCoprocessorEnvironment env;
    private int indexTableTTL;
    private VerificationResult verificationResult;
    private boolean isBeforeRebuilt = true;

    IndexRebuildRegionScanner (final RegionScanner innerScanner, final Region region, final Scan scan,
                               final RegionCoprocessorEnvironment env,
                               UngroupedAggregateRegionObserver ungroupedAggregateRegionObserver) throws IOException {
        super(innerScanner);
        final Configuration config = env.getConfiguration();
        if (scan.getAttribute(BaseScannerRegionObserver.INDEX_REBUILD_PAGING) != null) {
            pageSizeInRows = config.getLong(INDEX_REBUILD_PAGE_SIZE_IN_ROWS,
                    QueryServicesOptions.DEFAULT_INDEX_REBUILD_PAGE_SIZE_IN_ROWS);
        }
        maxBatchSize = config.getInt(MUTATE_BATCH_SIZE_ATTRIB, QueryServicesOptions.DEFAULT_MUTATE_BATCH_SIZE);
        mutations = new UngroupedAggregateRegionObserver.MutationList(maxBatchSize);
        maxBatchSizeBytes = config.getLong(MUTATE_BATCH_SIZE_BYTES_ATTRIB,
                QueryServicesOptions.DEFAULT_MUTATE_BATCH_SIZE_BYTES);
        blockingMemstoreSize = UngroupedAggregateRegionObserver.getBlockingMemstoreSize(region, config);
        clientVersionBytes = scan.getAttribute(BaseScannerRegionObserver.CLIENT_VERSION);
        indexMetaData = scan.getAttribute(PhoenixIndexCodec.INDEX_PROTO_MD);
        if (indexMetaData == null) {
            useProto = false;
            indexMetaData = scan.getAttribute(PhoenixIndexCodec.INDEX_MD);
        }
        if (!scan.isRaw()) {
            // No need to deserialize index maintainers when the scan is raw. Raw scan is used by partial rebuilds
            List<IndexMaintainer> maintainers = IndexMaintainer.deserialize(indexMetaData, true);
            indexMaintainer = maintainers.get(0);
        }
        this.scan = scan;
        this.innerScanner = innerScanner;
        this.region = region;
        this.env = env;
        this.ungroupedAggregateRegionObserver = ungroupedAggregateRegionObserver;
        indexRowKey = scan.getAttribute(BaseScannerRegionObserver.INDEX_ROW_KEY);
        byte[] valueBytes = scan.getAttribute(BaseScannerRegionObserver.INDEX_REBUILD_VERIFY_TYPE);
        if (valueBytes != null) {
            verificationResult = new VerificationResult();
            verifyType = IndexTool.IndexVerifyType.fromValue(valueBytes);
            if (verifyType != IndexTool.IndexVerifyType.NONE) {
                verify = true;
                // Create the following objects only for rebuilds by IndexTool
                indexHTable = ServerUtil.ConnectionFactory.getConnection(ServerUtil.ConnectionType.INDEX_WRITER_CONNECTION,
                        env).getTable(TableName.valueOf(indexMaintainer.getIndexTableName()));
                indexTableTTL = indexHTable.getDescriptor().getColumnFamilies()[0].getTimeToLive();
                outputHTable = ServerUtil.ConnectionFactory.getConnection(ServerUtil.ConnectionType.INDEX_WRITER_CONNECTION,
                        env).getTable(TableName.valueOf(IndexTool.OUTPUT_TABLE_NAME_BYTES));
                resultHTable = ServerUtil.ConnectionFactory.getConnection(ServerUtil.ConnectionType.INDEX_WRITER_CONNECTION,
                        env).getTable(TableName.valueOf(IndexTool.RESULT_TABLE_NAME_BYTES));
                indexKeyToDataPutMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
                dataKeyToDataPutMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
                pool = new WaitForCompletionTaskRunner(ThreadPoolManager.getExecutor(
                        new ThreadPoolBuilder("IndexVerify",
                                env.getConfiguration()).setMaxThread(NUM_CONCURRENT_INDEX_VERIFY_THREADS_CONF_KEY,
                                DEFAULT_CONCURRENT_INDEX_VERIFY_THREADS).setCoreTimeout(
                                INDEX_WRITER_KEEP_ALIVE_TIME_CONF_KEY), env));
                rowCountPerTask = config.getInt(INDEX_VERIFY_ROW_COUNTS_PER_TASK_CONF_KEY,
                        DEFAULT_INDEX_VERIFY_ROW_COUNTS_PER_TASK);
            }
        }
    }

    @Override
    public RegionInfo getRegionInfo() {
        return region.getRegionInfo();
    }

    @Override
    public boolean isFilterDone() { return false; }

    private void logToIndexToolResultTable() throws IOException {
        long scanMaxTs = scan.getTimeRange().getMax();
        byte[] keyPrefix = Bytes.toBytes(Long.toString(scanMaxTs));
        byte[] regionName = Bytes.toBytes(region.getRegionInfo().getRegionNameAsString());
        byte[] rowKey = new byte[keyPrefix.length + regionName.length];
        // The row key for the result table is the max timestamp of the scan + the table region name
        Bytes.putBytes(rowKey, 0, keyPrefix, 0, keyPrefix.length);
        Bytes.putBytes(rowKey, keyPrefix.length, regionName, 0, regionName.length);
        Put put = new Put(rowKey);
        put.addColumn(RESULT_TABLE_COLUMN_FAMILY, IndexTool.SCAN_STOP_ROW_KEY_BYTES,
                scanMaxTs, scan.getStopRow());
        put.addColumn(RESULT_TABLE_COLUMN_FAMILY, SCANNED_DATA_ROW_COUNT_BYTES,
                scanMaxTs, Bytes.toBytes(Long.toString(verificationResult.scannedDataRowCount)));
        put.addColumn(RESULT_TABLE_COLUMN_FAMILY, REBUILT_INDEX_ROW_COUNT_BYTES,
                scanMaxTs, Bytes.toBytes(Long.toString(verificationResult.rebuiltIndexRowCount)));
        if (verifyType == IndexTool.IndexVerifyType.BEFORE || verifyType == IndexTool.IndexVerifyType.BOTH ||
                verifyType == IndexTool.IndexVerifyType.ONLY) {
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_VALID_INDEX_ROW_COUNT_BYTES,
                    scanMaxTs, Bytes.toBytes(Long.toString(verificationResult.before.validIndexRowCount)));
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT_BYTES,
                    scanMaxTs, Bytes.toBytes(Long.toString(verificationResult.before.expiredIndexRowCount)));
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT_BYTES,
                    scanMaxTs, Bytes.toBytes(Long.toString(verificationResult.before.missingIndexRowCount)));
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_BYTES,
                    scanMaxTs, Bytes.toBytes(Long.toString(verificationResult.before.invalidIndexRowCount)));
        }
        if (verifyType == IndexTool.IndexVerifyType.AFTER || verifyType == IndexTool.IndexVerifyType.BOTH) {
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_VALID_INDEX_ROW_COUNT_BYTES,
                    scanMaxTs, Bytes.toBytes(Long.toString(verificationResult.after.validIndexRowCount)));
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_EXPIRED_INDEX_ROW_COUNT_BYTES,
                    scanMaxTs, Bytes.toBytes(Long.toString(verificationResult.after.expiredIndexRowCount)));
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_MISSING_INDEX_ROW_COUNT_BYTES,
                    scanMaxTs, Bytes.toBytes(Long.toString(verificationResult.after.missingIndexRowCount)));
            put.addColumn(RESULT_TABLE_COLUMN_FAMILY, AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_BYTES,
                    scanMaxTs, Bytes.toBytes(Long.toString(verificationResult.after.invalidIndexRowCount)));
        }
        resultHTable.put(put);
    }

    @Override
    public void close() throws IOException {
        innerScanner.close();
        if (verify) {
            try {
                logToIndexToolResultTable();
            } finally {
                this.pool.stop("IndexRebuildRegionScanner is closing");
                indexHTable.close();
                outputHTable.close();
                resultHTable.close();
            }
        }
    }

    private void setMutationAttributes(Mutation m, byte[] uuidValue) {
        m.setAttribute(useProto ? PhoenixIndexCodec.INDEX_PROTO_MD : PhoenixIndexCodec.INDEX_MD, indexMetaData);
        m.setAttribute(PhoenixIndexCodec.INDEX_UUID, uuidValue);
        m.setAttribute(BaseScannerRegionObserver.REPLAY_WRITES,
                BaseScannerRegionObserver.REPLAY_INDEX_REBUILD_WRITES);
        m.setAttribute(BaseScannerRegionObserver.CLIENT_VERSION, clientVersionBytes);
        // Since we're replaying existing mutations, it makes no sense to write them to the wal
        m.setDurability(Durability.SKIP_WAL);
    }

    private Delete generateDeleteMarkers(Put put) {
        Set<ColumnReference> allColumns = indexMaintainer.getAllColumns();
        int cellCount = put.size();
        if (cellCount == allColumns.size() + 1) {
            // We have all the columns for the index table. So, no delete marker is needed
            return null;
        }
        Set<ColumnReference> includedColumns = Sets.newLinkedHashSetWithExpectedSize(cellCount);
        long ts = 0;
        for (List<Cell> cells : put.getFamilyCellMap().values()) {
            if (cells == null) {
                break;
            }
            for (Cell cell : cells) {
                includedColumns.add(new ColumnReference(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell)));
                if (ts < cell.getTimestamp()) {
                    ts = cell.getTimestamp();
                }
            }
        }
        Delete del = null;
        for (ColumnReference column : allColumns) {
            if (!includedColumns.contains(column)) {
                if (del == null) {
                    del = new Delete(put.getRow());
                }
                del.addColumns(column.getFamily(), column.getQualifier(), ts);
            }
        }
        return del;
    }

    private byte[] commitIfReady(byte[] uuidValue, UngroupedAggregateRegionObserver.MutationList mutationList) throws IOException {
        if (ServerUtil.readyToCommit(mutationList.size(), mutationList.byteSize(), maxBatchSize, maxBatchSizeBytes)) {
            ungroupedAggregateRegionObserver.checkForRegionClosingOrSplitting();
            ungroupedAggregateRegionObserver.commitBatchWithRetries(region, mutationList, blockingMemstoreSize);
            uuidValue = ServerCacheClient.generateId();
            mutationList.clear();
        }
        return uuidValue;
    }

    private class SimpleValueGetter implements ValueGetter {
        final ImmutableBytesWritable valuePtr = new ImmutableBytesWritable();
        final Put put;
        SimpleValueGetter (final Put put) {
            this.put = put;
        }
        @Override
        public ImmutableBytesWritable getLatestValue(ColumnReference ref, long ts) throws IOException {
            List<Cell> cellList = put.get(ref.getFamily(), ref.getQualifier());
            if (cellList == null || cellList.isEmpty()) {
                return null;
            }
            Cell cell = cellList.get(0);
            valuePtr.set(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            return valuePtr;
        }

        @Override
        public byte[] getRowKey() {
            return put.getRow();
        }

    }

    private byte[] getIndexRowKey(final Put dataRow) throws IOException {
        ValueGetter valueGetter = new SimpleValueGetter(dataRow);
        byte[] builtIndexRowKey = indexMaintainer.buildRowKey(valueGetter, new ImmutableBytesWritable(dataRow.getRow()),
                null, null, HConstants.LATEST_TIMESTAMP);
        return builtIndexRowKey;
    }

    private boolean checkIndexRow(final byte[] indexRowKey, final Put put) throws IOException {
        byte[] builtIndexRowKey = getIndexRowKey(put);
        if (Bytes.compareTo(builtIndexRowKey, 0, builtIndexRowKey.length,
                indexRowKey, 0, indexRowKey.length) != 0) {
            return false;
        }
        return true;
    }

    private void logToIndexToolOutputTable(byte[] dataRowKey, byte[] indexRowKey, long dataRowTs, long indexRowTs,
                                           String errorMsg) throws IOException {
        logToIndexToolOutputTable(dataRowKey, indexRowKey, dataRowTs, indexRowTs,
                errorMsg, null, null);

    }

    private void logToIndexToolOutputTable(byte[] dataRowKey, byte[] indexRowKey, long dataRowTs, long indexRowTs,
                                           String errorMsg, byte[] expectedValue,  byte[] actualValue) throws IOException {
        final byte[] E_VALUE_PREFIX_BYTES = Bytes.toBytes(" E:");
        final byte[] A_VALUE_PREFIX_BYTES = Bytes.toBytes(" A:");
        final int PREFIX_LENGTH = 3;
        final int TOTAL_PREFIX_LENGTH = 6;
        final byte[] PHASE_BEFORE_VALUE = Bytes.toBytes("BEFORE");
        final byte[] PHASE_AFTER_VALUE = Bytes.toBytes("AFTER");
        long scanMaxTs = scan.getTimeRange().getMax();
        byte[] keyPrefix = Bytes.toBytes(Long.toString(scanMaxTs));
        byte[] rowKey;
        // The row key for the output table is the max timestamp of the scan + data row key
        if (dataRowKey != null) {
            rowKey = new byte[keyPrefix.length + dataRowKey.length];
            Bytes.putBytes(rowKey, 0, keyPrefix, 0, keyPrefix.length);
            Bytes.putBytes(rowKey, keyPrefix.length, dataRowKey, 0, dataRowKey.length);
        } else {
            rowKey = new byte[keyPrefix.length];
            Bytes.putBytes(rowKey, 0, keyPrefix, 0, keyPrefix.length);
        }
        Put put = new Put(rowKey);
        put.addColumn(IndexTool.OUTPUT_TABLE_COLUMN_FAMILY, IndexTool.DATA_TABLE_NAME_BYTES,
                scanMaxTs, region.getRegionInfo().getTable().getName());
        put.addColumn(IndexTool.OUTPUT_TABLE_COLUMN_FAMILY, IndexTool.INDEX_TABLE_NAME_BYTES,
                scanMaxTs, indexMaintainer.getIndexTableName());
        if (dataRowKey != null) {
            put.addColumn(IndexTool.OUTPUT_TABLE_COLUMN_FAMILY, IndexTool.DATA_TABLE_TS_BYTES,
                    scanMaxTs, Bytes.toBytes(Long.toString(dataRowTs)));
        }
        if (indexRowKey != null) {
            put.addColumn(IndexTool.OUTPUT_TABLE_COLUMN_FAMILY, IndexTool.INDEX_TABLE_ROW_KEY_BYTES,
                    scanMaxTs, indexRowKey);
            put.addColumn(IndexTool.OUTPUT_TABLE_COLUMN_FAMILY, IndexTool.INDEX_TABLE_TS_BYTES,
                    scanMaxTs, Bytes.toBytes(Long.toString(indexRowTs)));
        }
        byte[] errorMessageBytes;
        if (expectedValue != null) {
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

        }
        else {
            errorMessageBytes = Bytes.toBytes(errorMsg);
        }
        put.addColumn(IndexTool.OUTPUT_TABLE_COLUMN_FAMILY, IndexTool.ERROR_MESSAGE_BYTES, scanMaxTs, errorMessageBytes);
        if (isBeforeRebuilt) {
            put.addColumn(IndexTool.OUTPUT_TABLE_COLUMN_FAMILY, IndexTool.VERIFICATION_PHASE_BYTES, scanMaxTs, PHASE_BEFORE_VALUE);
        } else {
            put.addColumn(IndexTool.OUTPUT_TABLE_COLUMN_FAMILY, IndexTool.VERIFICATION_PHASE_BYTES, scanMaxTs, PHASE_AFTER_VALUE);
        }
        outputHTable.put(put);
    }

    private long getMaxTimestamp(Result result) {
        long ts = 0;
        for (Cell cell : result.rawCells()) {
            if (ts < cell.getTimestamp()) {
                ts = cell.getTimestamp();
            }
        }
        return ts;
    }

    private long getMaxTimestamp(Put put) {
        long ts = 0;
        for (List<Cell> cells : put.getFamilyCellMap().values()) {
            if (cells == null) {
                break;
            }
            for (Cell cell : cells) {
                if (ts < cell.getTimestamp()) {
                    ts = cell.getTimestamp();
                }
            }
        }
        return ts;
    }

    private boolean verifySingleIndexRow(Result indexRow, final Put dataRow) throws IOException {
        ValueGetter valueGetter = new SimpleValueGetter(dataRow);
        long ts = getMaxTimestamp(dataRow);
        Put indexPut = indexMaintainer.buildUpdateMutation(GenericKeyValueBuilder.INSTANCE,
                valueGetter, new ImmutableBytesWritable(dataRow.getRow()), ts, null, null);
        if (indexPut == null) {
            String errorMsg = "Empty index update";
            logToIndexToolOutputTable(dataRow.getRow(), indexRow.getRow(), ts, getMaxTimestamp(indexRow), errorMsg);
            return false;
        }
        else {
            // Remove the empty column prepared by Index codec as we need to change its value
            removeEmptyColumn(indexPut, indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                    indexMaintainer.getEmptyKeyValueQualifier());
        }
        // Add the empty column
        indexPut.addColumn(indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                indexMaintainer.getEmptyKeyValueQualifier(), ts, VERIFIED_BYTES);
        int cellCount = 0;
        long currentTime = EnvironmentEdgeManager.currentTime();
        for (List<Cell> cells : indexPut.getFamilyCellMap().values()) {
            if (cells == null) {
                break;
            }
            for (Cell expectedCell : cells) {
                byte[] family = CellUtil.cloneFamily(expectedCell);
                byte[] qualifier = CellUtil.cloneQualifier(expectedCell);
                Cell actualCell = indexRow.getColumnLatestCell(family, qualifier);
                if (actualCell == null) {
                    // Check if cell expired as per the current server's time and data table ttl
                    // Index table should have the same ttl as the data table, hence we might not
                    // get a value back from index if it has already expired between our rebuild and
                    // verify
                    // TODO: have a metric to update for these cases
                    if (isTimestampBeforeTTL(currentTime, expectedCell.getTimestamp())) {
                        continue;
                    }
                    String errorMsg = " Missing cell " + Bytes.toString(family) + ":" +
                            Bytes.toString(qualifier);
                    logToIndexToolOutputTable(dataRow.getRow(), indexRow.getRow(), ts, getMaxTimestamp(indexRow), errorMsg);
                    return false;
                }
                // Check all columns
                if (!CellUtil.matchingValue(actualCell, expectedCell)) {
                    String errorMsg = "Not matching value for " + Bytes.toString(family) + ":" +
                            Bytes.toString(qualifier);
                    logToIndexToolOutputTable(dataRow.getRow(), indexRow.getRow(), ts, getMaxTimestamp(indexRow),
                            errorMsg, CellUtil.cloneValue(expectedCell), CellUtil.cloneValue(actualCell));
                    return false;
                } else if (!CellUtil.matchingTimestamp(actualCell, expectedCell)) {
                    String errorMsg = "Not matching timestamp for " + Bytes.toString(family) + ":" +
                            Bytes.toString(qualifier) + " E: " + expectedCell.getTimestamp() + " A: " +
                            actualCell.getTimestamp();
                    logToIndexToolOutputTable(dataRow.getRow(), indexRow.getRow(), ts, getMaxTimestamp(indexRow),
                            errorMsg, null, null);
                    return false;
                }
                cellCount++;
            }
        }
        if (cellCount != indexRow.rawCells().length) {
            String errorMsg = "Expected to find " + cellCount + " cells but got "
                    + indexRow.rawCells().length + " cells";
            logToIndexToolOutputTable(dataRow.getRow(), indexRow.getRow(), ts, getMaxTimestamp(indexRow), errorMsg);
            return false;
        }
        return true;
    }

    private void verifyIndexRows(List<KeyRange> keys, Map<byte[], Put> perTaskDataKeyToDataPutMap,
                                 VerificationResult.PhaseResult verificationPhaseResult) throws IOException {
        int expectedRowCount = keys.size();
        ScanRanges scanRanges = ScanRanges.createPointLookup(keys);
        Scan indexScan = new Scan();
        indexScan.setTimeRange(scan.getTimeRange().getMin(), scan.getTimeRange().getMax());
        scanRanges.initializeScan(indexScan);
        SkipScanFilter skipScanFilter = scanRanges.getSkipScanFilter();
        indexScan.setFilter(skipScanFilter);
        int rowCount = 0;
        try (ResultScanner resultScanner = indexHTable.getScanner(indexScan)) {
            for (Result result = resultScanner.next(); (result != null); result = resultScanner.next()) {
                Put dataPut = indexKeyToDataPutMap.get(result.getRow());
                if (dataPut == null) {
                    // This should never happen
                    String errorMsg = "Missing data row";
                    logToIndexToolOutputTable(null, result.getRow(), 0, getMaxTimestamp(result), errorMsg);
                    exceptionMessage = "Index verify failed - Missing data row - " + indexHTable.getName();
                    throw new IOException(exceptionMessage);
                }
                if (verifySingleIndexRow(result, dataPut)) {
                    verificationPhaseResult.validIndexRowCount++;
                    perTaskDataKeyToDataPutMap.remove(dataPut.getRow());
                } else {
                    verificationPhaseResult.invalidIndexRowCount++;
                }
                rowCount++;
            }
        } catch (Throwable t) {
            ServerUtil.throwIOException(indexHTable.getName().toString(), t);
        }
        // Check if any expected rows from index(which we didn't get) are already expired due to TTL
        // TODO: metrics for expired rows
        if (!perTaskDataKeyToDataPutMap.isEmpty()) {
            Iterator<Entry<byte[], Put>> itr = perTaskDataKeyToDataPutMap.entrySet().iterator();
            long currentTime = EnvironmentEdgeManager.currentTime();
            while(itr.hasNext()) {
                Entry<byte[], Put> entry = itr.next();
                long ts = getMaxTimestamp(entry.getValue());
                if (isTimestampBeforeTTL(currentTime, ts)) {
                    itr.remove();
                    rowCount++;
                    verificationPhaseResult.expiredIndexRowCount++;
                }
            }
        }
        if (rowCount != expectedRowCount) {
            for (Map.Entry<byte[], Put> entry : perTaskDataKeyToDataPutMap.entrySet()) {
                String errorMsg = "Missing index row";
                logToIndexToolOutputTable(entry.getKey(), null, getMaxTimestamp(entry.getValue()),
                        0, errorMsg);
            }
            verificationPhaseResult.missingIndexRowCount += expectedRowCount - rowCount;
        }
    }

    private boolean isTimestampBeforeTTL(long currentTime, long tsToCheck) {
        if (indexTableTTL == HConstants.FOREVER) {
            return false;
        }
        return tsToCheck < (currentTime - (long) indexTableTTL * 1000);
    }

    private void addVerifyTask(final List<KeyRange> keys, final Map<byte[], Put> perTaskDataKeyToDataPutMap,
                               final VerificationResult.PhaseResult verificationPhaseResult) {
        tasks.add(new Task<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                try {
                    if (Thread.currentThread().isInterrupted()) {
                        exceptionMessage = "Pool closed, not attempting to verify index rows! " + indexHTable.getName();
                        throw new IOException(exceptionMessage);
                    }
                    verifyIndexRows(keys, perTaskDataKeyToDataPutMap, verificationPhaseResult);
                } catch (Exception e) {
                    throw e;
                }
                return Boolean.TRUE;
            }
        });
    }

    private void parallelizeIndexVerify(VerificationResult.PhaseResult verificationPhaseResult) throws IOException {
        for (Mutation mutation : mutations) {
            indexKeyToDataPutMap.put(getIndexRowKey((Put)mutation), (Put)mutation);
        }
        int taskCount = (indexKeyToDataPutMap.size() + rowCountPerTask - 1) / rowCountPerTask;
        tasks = new TaskBatch<>(taskCount);
        List<Map<byte[], Put>> dataPutMapList = new ArrayList<>(taskCount);
        List<VerificationResult.PhaseResult> verificationPhaseResultList = new ArrayList<>(taskCount);
        List<KeyRange> keys = new ArrayList<>(rowCountPerTask);
        Map<byte[], Put> perTaskDataKeyToDataPutMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
        dataPutMapList.add(perTaskDataKeyToDataPutMap);
        VerificationResult.PhaseResult perTaskVerificationPhaseResult = new VerificationResult.PhaseResult();
        verificationPhaseResultList.add(perTaskVerificationPhaseResult);
        for (Map.Entry<byte[], Put> entry: indexKeyToDataPutMap.entrySet()) {
            keys.add(PVarbinary.INSTANCE.getKeyRange(entry.getKey()));
            perTaskDataKeyToDataPutMap.put(entry.getValue().getRow(), entry.getValue());
            if (keys.size() == rowCountPerTask) {
                addVerifyTask(keys, perTaskDataKeyToDataPutMap, perTaskVerificationPhaseResult);
                keys = new ArrayList<>(rowCountPerTask);
                perTaskDataKeyToDataPutMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
                dataPutMapList.add(perTaskDataKeyToDataPutMap);
                perTaskVerificationPhaseResult = new VerificationResult.PhaseResult();
                verificationPhaseResultList.add(perTaskVerificationPhaseResult);
            }
        }
        if (keys.size() > 0) {
            addVerifyTask(keys, perTaskDataKeyToDataPutMap, perTaskVerificationPhaseResult);
        }
        List<Boolean> taskResultList = null;
        try {
            LOGGER.debug("Waiting on index verify tasks to complete...");
            taskResultList = this.pool.submitUninterruptible(tasks);
        } catch (ExecutionException e) {
            throw new RuntimeException("Should not fail on the results while using a WaitForCompletionTaskRunner", e);
        } catch (EarlyExitFailure e) {
            throw new RuntimeException("Stopped while waiting for batch, quitting!", e);
        }
        for (Boolean result : taskResultList) {
            if (result == null) {
                // there was a failure
                throw new IOException(exceptionMessage);
            }
        }
        if (verifyType == IndexTool.IndexVerifyType.BEFORE || verifyType == IndexTool.IndexVerifyType.BOTH) {
            for (Map<byte[], Put> dataPutMap : dataPutMapList) {
                dataKeyToDataPutMap.putAll(dataPutMap);
            }
        }
        for (VerificationResult.PhaseResult result : verificationPhaseResultList) {
            verificationPhaseResult.add(result);
        }
    }

    private void rebuildIndexRows(UngroupedAggregateRegionObserver.MutationList mutationList) throws IOException {
        byte[] uuidValue = ServerCacheClient.generateId();
        UngroupedAggregateRegionObserver.MutationList currentMutationList =
                new UngroupedAggregateRegionObserver.MutationList(maxBatchSize);
        for (Mutation mutation : mutationList) {
            Put put = (Put) mutation;
            currentMutationList.add(mutation);
            setMutationAttributes(put, uuidValue);
            uuidValue = commitIfReady(uuidValue, currentMutationList);
            Delete deleteMarkers = generateDeleteMarkers(put);
            if (deleteMarkers != null) {
                setMutationAttributes(deleteMarkers, uuidValue);
                currentMutationList.add(deleteMarkers);
                uuidValue = commitIfReady(uuidValue, currentMutationList);
            }
        }
        if (!currentMutationList.isEmpty()) {
            ungroupedAggregateRegionObserver.checkForRegionClosingOrSplitting();
            ungroupedAggregateRegionObserver.commitBatchWithRetries(region, currentMutationList, blockingMemstoreSize);
        }
    }

    private void verifyAndOrRebuildIndex() throws IOException {
        VerificationResult nextVerificationResult = new VerificationResult();
        nextVerificationResult.scannedDataRowCount = mutations.size();
        if (verifyType == IndexTool.IndexVerifyType.AFTER || verifyType == IndexTool.IndexVerifyType.NONE) {
            // For these options we start with rebuilding index rows
            rebuildIndexRows(mutations);
            nextVerificationResult.rebuiltIndexRowCount = mutations.size();
            isBeforeRebuilt = false;
        }
        if (verifyType == IndexTool.IndexVerifyType.NONE) {
            return;
        }
        if (verifyType == IndexTool.IndexVerifyType.BEFORE || verifyType == IndexTool.IndexVerifyType.BOTH ||
                verifyType == IndexTool.IndexVerifyType.ONLY) {
            VerificationResult.PhaseResult verificationPhaseResult = new VerificationResult.PhaseResult();
            // For these options we start with verifying index rows
            parallelizeIndexVerify(verificationPhaseResult);
            nextVerificationResult.before.add(verificationPhaseResult);
            if (mutations.size() != verificationPhaseResult.getTotalCount()) {
                throw new DoNotRetryIOException(
                        "mutations.size() != verificationPhaseResult.getTotalCount() at the before phase " +
                                nextVerificationResult + " dataKeyToDataPutMap.size() = " + dataKeyToDataPutMap.size());
            }
        }
        if (verifyType == IndexTool.IndexVerifyType.BEFORE || verifyType == IndexTool.IndexVerifyType.BOTH) {
            // For these options, we have identified the rows to be rebuilt and now need to rebuild them
            // At this point, dataKeyToDataPutMap includes mapping only for the rows to be rebuilt
            mutations.clear();
            for (Map.Entry<byte[], Put> entry: dataKeyToDataPutMap.entrySet()) {
                mutations.add(entry.getValue());
            }
            rebuildIndexRows(mutations);
            nextVerificationResult.rebuiltIndexRowCount += mutations.size();
            isBeforeRebuilt = false;
        }

        if (verifyType == IndexTool.IndexVerifyType.AFTER || verifyType == IndexTool.IndexVerifyType.BOTH) {
            // We have rebuilt index row and now we need to verify them
            indexKeyToDataPutMap.clear();
            VerificationResult.PhaseResult verificationPhaseResult = new VerificationResult.PhaseResult();
            parallelizeIndexVerify(verificationPhaseResult);
            nextVerificationResult.after.add(verificationPhaseResult);
            if (mutations.size() != verificationPhaseResult.getTotalCount()) {
                throw new DoNotRetryIOException(
                        "mutations.size() != verificationPhaseResult.getTotalCount() at the after phase " +
                                nextVerificationResult + " dataKeyToDataPutMap.size() = " + dataKeyToDataPutMap.size());
            }
        }
        indexKeyToDataPutMap.clear();
        verificationResult.add(nextVerificationResult);
    }

    @Override
    public boolean next(List<Cell> results) throws IOException {
        Cell lastCell = null;
        int rowCount = 0;
        region.startRegionOperation();
        try {
            // Partial rebuilds by MetadataRegionObserver use raw scan. Inline verification is not supported for them
            boolean partialRebuild = scan.isRaw();
            byte[] uuidValue = ServerCacheClient.generateId();
            synchronized (innerScanner) {
                do {
                    List<Cell> row = new ArrayList<Cell>();
                    hasMore = innerScanner.nextRaw(row);
                    if (!row.isEmpty()) {
                        lastCell = row.get(0);
                        Put put = null;
                        Delete del = null;
                        for (Cell cell : row) {
                            if (KeyValue.Type.codeToType(cell.getTypeByte()) == KeyValue.Type.Put) {
                                if (put == null) {
                                    put = new Put(CellUtil.cloneRow(cell));
                                    mutations.add(put);
                                }
                                put.add(cell);
                            } else {
                                if (del == null) {
                                    del = new Delete(CellUtil.cloneRow(cell));
                                    mutations.add(del);
                                }
                                del.addDeleteMarker(cell);
                            }
                        }
                        if (partialRebuild) {
                            if (put != null) {
                                setMutationAttributes(put, uuidValue);
                            }
                            if (del != null) {
                                setMutationAttributes(del, uuidValue);
                            }
                            uuidValue = commitIfReady(uuidValue, mutations);
                        }
                        if (indexRowKey != null) {
                            if (put != null) {
                                setMutationAttributes(put, uuidValue);
                            }
                            Delete deleteMarkers = generateDeleteMarkers(put);
                            if (deleteMarkers != null) {
                                setMutationAttributes(deleteMarkers, uuidValue);
                                mutations.add(deleteMarkers);
                                uuidValue = commitIfReady(uuidValue, mutations);
                            }
                            // GlobalIndexChecker passed the index row key. This is to build a single index row.
                            // Check if the data table row we have just scanned matches with the index row key.
                            // If not, there is no need to build the index row from this data table row,
                            // and just return zero row count.
                            if (checkIndexRow(indexRowKey, put)) {
                                rowCount = GlobalIndexChecker.RebuildReturnCode.INDEX_ROW_EXISTS.getValue();
                            } else {
                                rowCount = GlobalIndexChecker.RebuildReturnCode.NO_INDEX_ROW.getValue();
                            }
                            break;
                        }
                        rowCount++;
                    }
                } while (hasMore && rowCount < pageSizeInRows);
                if (!partialRebuild && indexRowKey == null) {
                    verifyAndOrRebuildIndex();
                } else {
                    if (!mutations.isEmpty()) {
                        ungroupedAggregateRegionObserver.checkForRegionClosingOrSplitting();
                        ungroupedAggregateRegionObserver.commitBatchWithRetries(region, mutations, blockingMemstoreSize);
                    }
                }
            }
        } catch (IOException e) {
            LOGGER.error("IOException during rebuilding: " + Throwables.getStackTraceAsString(e));
            throw e;
        } finally {
            region.closeRegionOperation();
            mutations.clear();
            if (verify) {
              indexKeyToDataPutMap.clear();
              dataKeyToDataPutMap.clear();
            }
        }
        byte[] rowCountBytes = PLong.INSTANCE.toBytes(Long.valueOf(rowCount));
        final Cell aggKeyValue;
        if (lastCell == null) {
            aggKeyValue = PhoenixKeyValueUtil.newKeyValue(UNGROUPED_AGG_ROW_KEY, SINGLE_COLUMN_FAMILY,
                    SINGLE_COLUMN, AGG_TIMESTAMP, rowCountBytes, 0, rowCountBytes.length);
        } else {
            aggKeyValue = PhoenixKeyValueUtil.newKeyValue(CellUtil.cloneRow(lastCell), SINGLE_COLUMN_FAMILY,
                    SINGLE_COLUMN, AGG_TIMESTAMP, rowCountBytes, 0, rowCountBytes.length);
        }
        results.add(aggKeyValue);
        return hasMore;
    }

    @Override
    public long getMaxResultSize() {
        return scan.getMaxResultSize();
    }
}