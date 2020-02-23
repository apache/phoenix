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

import static org.apache.phoenix.hbase.index.IndexRegionObserver.UNVERIFIED_BYTES;
import static org.apache.phoenix.hbase.index.IndexRegionObserver.VERIFIED_BYTES;
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
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.Lists;
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
import org.apache.hadoop.hbase.util.Pair;
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
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.IndexManagementUtil;
import org.apache.phoenix.index.GlobalIndexChecker;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.PhoenixKeyValueUtil;
import org.apache.phoenix.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

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
            } else if (verifyType == IndexTool.IndexVerifyType.ONLY) {
                if (before.invalidIndexRowCount + before.missingIndexRowCount > 0) {
                    return true;
                }
            } else if (verifyType == IndexTool.IndexVerifyType.BOTH || verifyType == IndexTool.IndexVerifyType.AFTER) {
                if (after.invalidIndexRowCount + after.missingIndexRowCount > 0) {
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
    private Map<byte[], List<Mutation>> indexKeyToMutationMap;
    private Map<byte[], Pair<Put, Delete>> dataKeyToMutationMap;
    private TaskRunner pool;
    private TaskBatch<Boolean> tasks;
    private String exceptionMessage;
    private UngroupedAggregateRegionObserver ungroupedAggregateRegionObserver;
    private RegionCoprocessorEnvironment env;
    private int indexTableTTL;
    private VerificationResult verificationResult;
    private boolean isBeforeRebuilt = true;
    private boolean partialRebuild = false;
    private int  singleRowRebuildReturnCode;
    private Map<byte[], NavigableSet<byte[]>> familyMap;
    private byte[][] viewConstants;

    IndexRebuildRegionScanner(final RegionScanner innerScanner, final Region region, final Scan scan,
                              final RegionCoprocessorEnvironment env,
                              UngroupedAggregateRegionObserver ungroupedAggregateRegionObserver) throws IOException {
        super(innerScanner);
        final Configuration config = env.getConfiguration();
        if (scan.getAttribute(BaseScannerRegionObserver.INDEX_REBUILD_PAGING) != null) {
            pageSizeInRows = config.getLong(INDEX_REBUILD_PAGE_SIZE_IN_ROWS,
                    QueryServicesOptions.DEFAULT_INDEX_REBUILD_PAGE_SIZE_IN_ROWS);
        } else {
            partialRebuild = true;
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
        List<IndexMaintainer> maintainers = IndexMaintainer.deserialize(indexMetaData, true);
        indexMaintainer = maintainers.get(0);
        this.scan = scan;
        familyMap = scan.getFamilyMap();
        if (familyMap.isEmpty()) {
            familyMap = null;
        }

        this.innerScanner = innerScanner;
        this.region = region;
        this.env = env;
        this.ungroupedAggregateRegionObserver = ungroupedAggregateRegionObserver;
        indexRowKey = scan.getAttribute(BaseScannerRegionObserver.INDEX_ROW_KEY);
        if (indexRowKey != null) {
            setReturnCodeForSingleRowRebuild();
        }
        byte[] valueBytes = scan.getAttribute(BaseScannerRegionObserver.INDEX_REBUILD_VERIFY_TYPE);
        if (valueBytes != null) {
            verificationResult = new VerificationResult();
            verifyType = IndexTool.IndexVerifyType.fromValue(valueBytes);
            if (verifyType != IndexTool.IndexVerifyType.NONE) {
                verify = true;
                viewConstants = IndexUtil.deserializeViewConstantsFromScan(scan);
                // Create the following objects only for rebuilds by IndexTool
                indexHTable = ServerUtil.ConnectionFactory.getConnection(ServerUtil.ConnectionType.INDEX_WRITER_CONNECTION,
                        env).getTable(TableName.valueOf(indexMaintainer.getIndexTableName()));
                indexTableTTL = indexHTable.getDescriptor().getColumnFamilies()[0].getTimeToLive();
                outputHTable = ServerUtil.ConnectionFactory.getConnection(ServerUtil.ConnectionType.INDEX_WRITER_CONNECTION,
                        env).getTable(TableName.valueOf(IndexTool.OUTPUT_TABLE_NAME_BYTES));
                resultHTable = ServerUtil.ConnectionFactory.getConnection(ServerUtil.ConnectionType.INDEX_WRITER_CONNECTION,
                        env).getTable(TableName.valueOf(IndexTool.RESULT_TABLE_NAME_BYTES));
                indexKeyToMutationMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
                dataKeyToMutationMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
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

    private void setReturnCodeForSingleRowRebuild() throws IOException {
        try (RegionScanner scanner = region.getScanner(scan)) {
            List<Cell> row = new ArrayList<>();
            scanner.next(row);
            // Check if the data table row we have just scanned matches with the index row key.
            // If not, there is no need to build the index row from this data table row,
            // and just return zero row count.
            if (row.isEmpty()) {
                singleRowRebuildReturnCode = GlobalIndexChecker.RebuildReturnCode.NO_DATA_ROW.getValue();
            } else {
                Put put = new Put(CellUtil.cloneRow(row.get(0)));
                for (Cell cell : row) {
                    put.add(cell);
                }
                if (checkIndexRow(indexRowKey, put)) {
                    singleRowRebuildReturnCode = GlobalIndexChecker.RebuildReturnCode.INDEX_ROW_EXISTS.getValue();
                } else {
                    singleRowRebuildReturnCode = GlobalIndexChecker.RebuildReturnCode.NO_INDEX_ROW.getValue();
                }
            }
        }
    }

    @Override
    public RegionInfo getRegionInfo() {
        return region.getRegionInfo();
    }

    @Override
    public boolean isFilterDone() {
        return false;
    }

    private void logToIndexToolResultTable() throws IOException {
        long scanMaxTs = scan.getTimeRange().getMax();
        byte[] keyPrefix = Bytes.toBytes(Long.toString(scanMaxTs));
        byte[] regionName = Bytes.toBytes(region.getRegionInfo().getRegionNameAsString());
        // The row key for the result table is the max timestamp of the scan + the table region name + scan start row
        // + scan stop row
        byte[] rowKey = new byte[keyPrefix.length + regionName.length + scan.getStartRow().length +
                scan.getStopRow().length];
        Bytes.putBytes(rowKey, 0, keyPrefix, 0, keyPrefix.length);
        Bytes.putBytes(rowKey, keyPrefix.length, regionName, 0, regionName.length);
        Bytes.putBytes(rowKey, keyPrefix.length + regionName.length, scan.getStartRow(), 0,
                scan.getStartRow().length);
        Bytes.putBytes(rowKey, keyPrefix.length + regionName.length + scan.getStartRow().length,
                scan.getStopRow(), 0, scan.getStopRow().length);
        Put put = new Put(rowKey);
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

    private byte[] commitIfReady(byte[] uuidValue, UngroupedAggregateRegionObserver.MutationList mutationList) throws IOException {
        if (ServerUtil.readyToCommit(mutationList.size(), mutationList.byteSize(), maxBatchSize, maxBatchSizeBytes)) {
            ungroupedAggregateRegionObserver.checkForRegionClosingOrSplitting();
            ungroupedAggregateRegionObserver.commitBatchWithRetries(region, mutationList, blockingMemstoreSize);
            uuidValue = ServerCacheClient.generateId();
            mutationList.clear();
        }
        return uuidValue;
    }

    public static class SimpleValueGetter implements ValueGetter {
        final ImmutableBytesWritable valuePtr = new ImmutableBytesWritable();
        final Put put;

        public SimpleValueGetter(final Put put) {
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
                                           String errorMsg, byte[] expectedValue, byte[] actualValue) throws IOException {
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

        } else {
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

    private static long getMaxTimestamp(Mutation m) {
        long ts = 0;
        for (List<Cell> cells : m.getFamilyCellMap().values()) {
            if (cells == null) {
                continue;
            }
            for (Cell cell : cells) {
                if (ts < cell.getTimestamp()) {
                    ts = cell.getTimestamp();
                }
            }
        }
        return ts;
    }

    private boolean isMatchingMutation(Mutation expected, Mutation actual) throws IOException{
        if (getTimestamp(expected) != getTimestamp(actual)) {
            String errorMsg = "Not matching timestamp";
            byte[] dataKey = indexMaintainer.buildDataRowKey(new ImmutableBytesWritable(expected.getRow()), viewConstants);
            logToIndexToolOutputTable(dataKey, expected.getRow(), getTimestamp(expected), getTimestamp(actual),
                    errorMsg, null, null);
            return false;
        }
        for (List<Cell> cells : expected.getFamilyCellMap().values()) {
            if (cells == null) {
                continue;
            }
            for (Cell expectedCell : cells) {
                byte[] family = CellUtil.cloneFamily(expectedCell);
                byte[] qualifier = CellUtil.cloneQualifier(expectedCell);
                List<Cell> cellList = actual.get(family, qualifier);
                Cell actualCell = (cellList != null && !cellList.isEmpty()) ? cellList.get(0) : null;
                if (actualCell == null ||
                        !CellUtil.matchingType(expectedCell, actualCell)) {
                    byte[] dataKey = indexMaintainer.buildDataRowKey(new ImmutableBytesWritable(expected.getRow()), viewConstants);
                    String errorMsg = " Missing cell " + Bytes.toString(family) + ":" + Bytes.toString(qualifier);
                    logToIndexToolOutputTable(dataKey, expected.getRow(), getTimestamp(expected), getTimestamp(actual), errorMsg);
                    return false;
                }
                if (!CellUtil.matchingValue(actualCell, expectedCell)) {
                    if (Bytes.compareTo(family, indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary()) == 0 &&
                            Bytes.compareTo(qualifier, indexMaintainer.getEmptyKeyValueQualifier()) == 0) {
                        if (Bytes.compareTo(actualCell.getValueArray(), actualCell.getValueOffset(), actualCell.getValueLength(),
                                UNVERIFIED_BYTES, 0, UNVERIFIED_BYTES.length) == 0) {
                            continue;
                        }
                    }
                    String errorMsg = "Not matching value for " + Bytes.toString(family) + ":" + Bytes.toString(qualifier);
                    byte[] dataKey = indexMaintainer.buildDataRowKey(new ImmutableBytesWritable(expected.getRow()), viewConstants);
                    logToIndexToolOutputTable(dataKey, expected.getRow(), getTimestamp(expected), getTimestamp(actual),
                            errorMsg, CellUtil.cloneValue(expectedCell), CellUtil.cloneValue(actualCell));
                    return false;
                }
            }
        }
        return true;
    }

    private boolean isVerified(Mutation mutation) throws IOException {
        if (mutation instanceof Delete) {
            return false;
        }
        List<Cell> cellList = mutation.get(indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                indexMaintainer.getEmptyKeyValueQualifier());
        Cell cell = (cellList != null && !cellList.isEmpty()) ? cellList.get(0) : null;
        if (cell == null) {
            throw new DoNotRetryIOException("No empty column cell");
        }
        if (Bytes.compareTo(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(),
                VERIFIED_BYTES, 0, VERIFIED_BYTES.length) == 0) {
            return true;
        }
        return false;
    }

    /**
     * This is to reorder the mutations in descending order by the tuple of timestamp and mutation type where
     * put comes before delete
     */
    public static final Comparator<Mutation> MUTATION_TS_DESC_COMPARATOR = new Comparator<Mutation>() {
        @Override
        public int compare(Mutation o1, Mutation o2) {
            long ts1 = getTimestamp(o1);
            long ts2 = getTimestamp(o2);
            if (ts1 > ts2) {
                return -1;
            }
            if (ts1 < ts2) {
                return 1;
            }
            if (o1 instanceof Put && o2 instanceof Delete) {
                return -1;
            }
            if (o1 instanceof Delete && o2 instanceof Put) {
                return 1;
            }
            return 0;
        }
    };

    private boolean verifySingleIndexRow(Result indexRow, VerificationResult.PhaseResult verificationPhaseResult)
            throws IOException {
        List<Mutation> expectedMutationList = indexKeyToMutationMap.get(indexRow.getRow());
        if (expectedMutationList == null) {
            throw new DoNotRetryIOException("No expected mutation");
        }
        Put put = null;
        Delete del = null;
        for (Cell cell : indexRow.rawCells()) {
            if (KeyValue.Type.codeToType(cell.getTypeByte()) == KeyValue.Type.Put) {
                if (put == null) {
                    put = new Put(CellUtil.cloneRow(cell));
                }
                put.add(cell);
            } else {
                if (del == null) {
                    del = new Delete(CellUtil.cloneRow(cell));
                }
                del.addDeleteMarker(cell);
            }
        }
        List<Mutation> actualMutationList = getMutationsWithSameTS(put, del);
        if (actualMutationList == null || actualMutationList.isEmpty()) {
            throw new DoNotRetryIOException("actualMutationList is null or empty");
        }
        Collections.sort(expectedMutationList, MUTATION_TS_DESC_COMPARATOR);
        Collections.sort(actualMutationList, MUTATION_TS_DESC_COMPARATOR);
        long currentTime = EnvironmentEdgeManager.currentTime();
        int actualMutationIndex = 0;
        for (Mutation expectedMutation : expectedMutationList) {
            // Check if cell expired as per the current server's time and data table ttl
            // Index table should have the same ttl as the data table, hence we might not
            // get a value back from index if it has already expired between our rebuild and
            // verify
            // TODO: have a metric to update for these cases
            if (isTimestampBeforeTTL(currentTime, getTimestamp(expectedMutation))) {
                verificationPhaseResult.expiredIndexRowCount++;
                return true;
            }
            // Skip expected delete mutations
            if (expectedMutation instanceof Delete) {
                continue;
            }
            if (actualMutationIndex == actualMutationList.size()) {
                // No more actual mutation is left to verify
                break;
            }
            Mutation actualMutation = actualMutationList.get(actualMutationIndex);
            // Skip newer actual unverified or delete mutations
            while (getTimestamp(expectedMutation) < getTimestamp(actualMutation) && !isVerified(actualMutation)) {
                // We assume this mutation is a single phase update (i.e., the data write phase failed)
                // if the assumption is not correct the rest of check will not pass
                // This means concurrent mutation marked the index row unverified with their timestamps
                // We can skip these index rows
                actualMutationIndex++;
                if (actualMutationIndex == actualMutationList.size()) {
                    break;
                }
                actualMutation = actualMutationList.get(actualMutationIndex);
            }
            if (actualMutationIndex == actualMutationList.size()) {
                break;
            }
            if (isMatchingMutation(expectedMutation, actualMutation)) {
                actualMutationIndex++;
                continue;
            }
            verificationPhaseResult.invalidIndexRowCount++;
            return false;
        }
        // Skip all the unverified rows
        while (actualMutationIndex < actualMutationList.size()) {
            if (!isVerified(actualMutationList.get(actualMutationIndex))) {
                actualMutationIndex++;
                continue;
            }
            else {
                break;
            }
        }
        if (actualMutationIndex < actualMutationList.size()) {
            byte[] dataKey = indexMaintainer.buildDataRowKey(new ImmutableBytesWritable(indexRow.getRow()), viewConstants);
            String errorMsg = "Expected to find " + actualMutationIndex + " mutations but got "
                    + actualMutationList.size();
            logToIndexToolOutputTable(dataKey, indexRow.getRow(),
                    getTimestamp(expectedMutationList.get(expectedMutationList.size() - 1)),
                    getTimestamp(actualMutationList.get(actualMutationList.size() - 1)), errorMsg);
            verificationPhaseResult.invalidIndexRowCount++;
            return false;
        }
        verificationPhaseResult.validIndexRowCount++;
        return true;
    }

    private static long getMaxTimestamp(Pair<Put, Delete> pair) {
        Put put = pair.getFirst();
        long ts1 = 0;
        if (put != null) {
            ts1 = getMaxTimestamp((Mutation)put);
        }
        Delete del = pair.getSecond();
        long ts2 = 0;
        if (del != null) {
            ts1 = getMaxTimestamp((Mutation)del);
        }
        return (ts1 > ts2) ? ts1 : ts2;
    }

    private void verifyIndexRows(List<KeyRange> keys,
                                 VerificationResult.PhaseResult verificationPhaseResult) throws IOException {
        List<KeyRange> invalidKeys = new ArrayList<>();
        ScanRanges scanRanges = ScanRanges.createPointLookup(keys);
        Scan indexScan = new Scan();
        indexScan.setTimeRange(scan.getTimeRange().getMin(), scan.getTimeRange().getMax());
        scanRanges.initializeScan(indexScan);
        /*SkipScanFilter skipScanFilter = scanRanges.getSkipScanFilter();
        indexScan.setFilter(new SkipScanFilter(skipScanFilter, true));*/
        indexScan.setRaw(true);
        indexScan.readAllVersions();
        try (ResultScanner resultScanner = indexHTable.getScanner(indexScan)) {
            for (Result result = resultScanner.next(); (result != null); result = resultScanner.next()) {
                KeyRange keyRange = PVarbinary.INSTANCE.getKeyRange(result.getRow());
                if (!keys.contains(keyRange)) {
                    continue;
                }
                if (!verifySingleIndexRow(result, verificationPhaseResult)) {
                    invalidKeys.add(keyRange);
                }
                keys.remove(keyRange);
            }
        } catch (Throwable t) {
            ServerUtil.throwIOException(indexHTable.getName().toString(), t);
        }
        // Check if any expected rows from index(which we didn't get) are already expired due to TTL
        // TODO: metrics for expired rows
        if (!keys.isEmpty()) {
            Iterator<KeyRange> itr = keys.iterator();
            long currentTime = EnvironmentEdgeManager.currentTime();
            while(itr.hasNext()) {
                KeyRange keyRange = itr.next();
                byte[] key = keyRange.getLowerRange();
                List<Mutation> mutationList = indexKeyToMutationMap.get(key);
                if (isTimestampBeforeTTL(currentTime, getTimestamp(mutationList.get(mutationList.size() - 1)))) {
                    itr.remove();
                    verificationPhaseResult.expiredIndexRowCount++;
                }
            }
        }
        if (keys.size() > 0) {
            for (KeyRange keyRange : keys) {
                String errorMsg = "Missing index row";
                byte[] key = keyRange.getLowerRange();
                List<Mutation> mutationList = indexKeyToMutationMap.get(key);
                byte[] dataKey = indexMaintainer.buildDataRowKey(new ImmutableBytesWritable(keyRange.getLowerRange()), viewConstants);
                logToIndexToolOutputTable(dataKey,
                        keyRange.getLowerRange(),
                        getMaxTimestamp(dataKeyToMutationMap.get(dataKey)),
                        getTimestamp(mutationList.get(mutationList.size() - 1)), errorMsg);
            }
            verificationPhaseResult.missingIndexRowCount += keys.size();
        }
        keys.addAll(invalidKeys);
    }

    private boolean isTimestampBeforeTTL(long currentTime, long tsToCheck) {
        if (indexTableTTL == HConstants.FOREVER) {
            return false;
        }
        return tsToCheck < (currentTime - (long) indexTableTTL * 1000);
    }

    private void addVerifyTask(final List<KeyRange> keys,
                               final VerificationResult.PhaseResult verificationPhaseResult) {
        tasks.add(new Task<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                try {
                    if (Thread.currentThread().isInterrupted()) {
                        exceptionMessage = "Pool closed, not attempting to verify index rows! " + indexHTable.getName();
                        throw new IOException(exceptionMessage);
                    }
                    verifyIndexRows(keys, verificationPhaseResult);
                } catch (Exception e) {
                    throw e;
                }
                return Boolean.TRUE;
            }
        });
    }

    private void parallelizeIndexVerify(VerificationResult.PhaseResult verificationPhaseResult) throws IOException {
        int taskCount = (indexKeyToMutationMap.size() + rowCountPerTask - 1) / rowCountPerTask;
        tasks = new TaskBatch<>(taskCount);
        List<List<KeyRange>> listOfKeyRangeList = new ArrayList<>(taskCount);
        List<VerificationResult.PhaseResult> verificationPhaseResultList = new ArrayList<>(taskCount);
        List<KeyRange> keys = new ArrayList<>(rowCountPerTask);
        listOfKeyRangeList.add(keys);
        VerificationResult.PhaseResult perTaskVerificationPhaseResult = new VerificationResult.PhaseResult();
        verificationPhaseResultList.add(perTaskVerificationPhaseResult);
        for (byte[] indexKey: indexKeyToMutationMap.keySet()) {
            keys.add(PVarbinary.INSTANCE.getKeyRange(indexKey));
            if (keys.size() == rowCountPerTask) {
                addVerifyTask(keys, perTaskVerificationPhaseResult);
                keys = new ArrayList<>(rowCountPerTask);
                listOfKeyRangeList.add(keys);
                perTaskVerificationPhaseResult = new VerificationResult.PhaseResult();
                verificationPhaseResultList.add(perTaskVerificationPhaseResult);
            }
        }
        if (keys.size() > 0) {
            addVerifyTask(keys, perTaskVerificationPhaseResult);
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
        for (VerificationResult.PhaseResult result : verificationPhaseResultList) {
            verificationPhaseResult.add(result);
        }
        if (verifyType == IndexTool.IndexVerifyType.BEFORE || verifyType == IndexTool.IndexVerifyType.BOTH) {
            Map<byte[], Pair<Put, Delete>> newDataKeyToMutationMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
            for (List<KeyRange> keyRangeList : listOfKeyRangeList) {
                for (KeyRange keyRange : keyRangeList) {
                    byte[] dataKey = indexMaintainer.buildDataRowKey(new ImmutableBytesWritable(keyRange.getLowerRange()), viewConstants);
                    newDataKeyToMutationMap.put(dataKey, dataKeyToMutationMap.get(dataKey));
                }
            }
            dataKeyToMutationMap.clear();
            dataKeyToMutationMap = newDataKeyToMutationMap;
        }
    }

    private void rebuildIndexRows(UngroupedAggregateRegionObserver.MutationList mutationList) throws IOException {
        byte[] uuidValue = ServerCacheClient.generateId();
        UngroupedAggregateRegionObserver.MutationList currentMutationList =
                new UngroupedAggregateRegionObserver.MutationList(maxBatchSize);
        Put put = null;
        for (Mutation mutation : mutationList) {
            if (mutation instanceof Put) {
                if (put != null) {
                    // back to back put, i.e., no delete in between. we can commit the previous put
                    uuidValue = commitIfReady(uuidValue, currentMutationList);
                }
                currentMutationList.add(mutation);
                setMutationAttributes(mutation, uuidValue);
                put = (Put)mutation;
            } else {
                currentMutationList.add(mutation);
                setMutationAttributes(mutation, uuidValue);
                uuidValue = commitIfReady(uuidValue, currentMutationList);
                put = null;
            }
        }
        if (!currentMutationList.isEmpty()) {
            ungroupedAggregateRegionObserver.checkForRegionClosingOrSplitting();
            ungroupedAggregateRegionObserver.commitBatchWithRetries(region, currentMutationList, blockingMemstoreSize);
        }
    }

    private void verifyAndOrRebuildIndex() throws IOException {
        VerificationResult nextVerificationResult = new VerificationResult();
        nextVerificationResult.scannedDataRowCount = dataKeyToMutationMap.size();
        if (verifyType == IndexTool.IndexVerifyType.AFTER || verifyType == IndexTool.IndexVerifyType.NONE) {
            // For these options we start with rebuilding index rows
            rebuildIndexRows(mutations);
            nextVerificationResult.rebuiltIndexRowCount = dataKeyToMutationMap.size();
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
        }
        if (verifyType == IndexTool.IndexVerifyType.BEFORE || verifyType == IndexTool.IndexVerifyType.BOTH) {
            // For these options, we have identified the rows to be rebuilt and now need to rebuild them
            // At this point, dataKeyToDataPutMap includes mapping only for the rows to be rebuilt
            mutations.clear();

            for (Map.Entry<byte[], Pair<Put, Delete>> entry: dataKeyToMutationMap.entrySet()) {
                if (entry.getValue().getFirst() != null) {
                    mutations.add(entry.getValue().getFirst());
                }
                if (entry.getValue().getSecond() != null) {
                    mutations.add(entry.getValue().getSecond());
                }
            }
            rebuildIndexRows(mutations);
            nextVerificationResult.rebuiltIndexRowCount += dataKeyToMutationMap.size();
            isBeforeRebuilt = false;
        }

        if (verifyType == IndexTool.IndexVerifyType.AFTER || verifyType == IndexTool.IndexVerifyType.BOTH) {
            // We have rebuilt index row and now we need to verify them
            VerificationResult.PhaseResult verificationPhaseResult = new VerificationResult.PhaseResult();
            indexKeyToMutationMap.clear();
            for (Map.Entry<byte[], Pair<Put, Delete>> entry: dataKeyToMutationMap.entrySet()) {
                prepareIndexMutations(entry.getValue().getFirst(), entry.getValue().getSecond());
            }
            parallelizeIndexVerify(verificationPhaseResult);
            nextVerificationResult.after.add(verificationPhaseResult);
        }
        verificationResult.add(nextVerificationResult);
    }

    private boolean isColumnIncluded(Cell cell) {
        byte[] family = CellUtil.cloneFamily(cell);
        if (!familyMap.containsKey(family)) {
            return false;
        }
        NavigableSet<byte[]> set = familyMap.get(family);
        if (set == null || set.isEmpty()) {
            return true;
        }
        byte[] qualifier = CellUtil.cloneQualifier(cell);
        return set.contains(qualifier);
    }

    public static long getTimestamp(Mutation m) {
        for (List<Cell> cells : m.getFamilyCellMap().values()) {
            for (Cell cell : cells) {
                return cell.getTimestamp();
            }
        }
        throw new IllegalStateException("No cell found");
    }


    /**
     * This is to reorder the mutations in ascending order by the tuple of timestamp and mutation type where
     * delete comes before put
     */
    public static final Comparator<Mutation> MUTATION_TS_COMPARATOR = new Comparator<Mutation>() {
        @Override
        public int compare(Mutation o1, Mutation o2) {
            long ts1 = getTimestamp(o1);
            long ts2 = getTimestamp(o2);
            if (ts1 < ts2) {
                return -1;
            }
            if (ts1 > ts2) {
                return 1;
            }
            if (o1 instanceof Delete && o2 instanceof Put) {
                return -1;
            }
            if (o1 instanceof Put && o2 instanceof Delete) {
                return 1;
            }
            return 0;
        }
    };

    public static List<Mutation> getMutationsWithSameTS(Put put, Delete del) {
        List<Mutation> mutationList = Lists.newArrayListWithExpectedSize(2);
        if (put != null) {
            mutationList.add(put);
        }
        if (del != null) {
            mutationList.add(del);
        }
        // Group the cells within a mutation based on their timestamps and create a separate mutation for each group
        mutationList = (List<Mutation>) IndexManagementUtil.flattenMutationsByTimestamp(mutationList);
        // Reorder the mutations on the same row so that delete comes before put when they have the same timestamp
        Collections.sort(mutationList, MUTATION_TS_COMPARATOR);
        return mutationList;
    }

    private static Put prepareIndexPut(IndexMaintainer indexMaintainer, ImmutableBytesPtr rowKeyPtr,
                                       ValueGetter mergedRowVG, long ts, boolean isRebuild,
                                       byte[] regionStartKey, byte[] regionEndKey)
            throws IOException {
        Put indexPut = indexMaintainer.buildUpdateMutation(GenericKeyValueBuilder.INSTANCE,
                mergedRowVG, rowKeyPtr, ts, null, null);
        if (indexPut == null) {
            // No covered column. Just prepare an index row with the empty column
            byte[] indexRowKey = indexMaintainer.buildRowKey(mergedRowVG, rowKeyPtr,
                    regionStartKey, regionEndKey, HConstants.LATEST_TIMESTAMP);
            indexPut = new Put(indexRowKey);
        }
        indexPut.addColumn(indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                indexMaintainer.getEmptyKeyValueQualifier(), ts, VERIFIED_BYTES);
        return indexPut;
    }

    public static void removeColumn(Put put, Cell deleteCell) {
        byte[] family = CellUtil.cloneFamily(deleteCell);
        List<Cell> cellList = put.getFamilyCellMap().get(family);
        if (cellList == null) {
            return;
        }
        Iterator<Cell> cellIterator = cellList.iterator();
        while (cellIterator.hasNext()) {
            Cell cell = cellIterator.next();
            if (CellUtil.matchingQualifier(cell, deleteCell)) {
                cellIterator.remove();
                if (cellList.isEmpty()) {
                    put.getFamilyCellMap().remove(family);
                }
                return;
            }
        }
    }

    public static void merge(Put current, Put previous) throws IOException {
        for (List<Cell> cells : previous.getFamilyCellMap().values()) {
            for (Cell cell : cells) {
                if (!current.has(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell))) {
                    current.add(cell);
                }
            }
        }
    }

    public static Put mergeNew(Put current, Put previous) throws IOException {
        Put next = new Put(current);
        merge(next, previous);
        return next;
    }

    /**
     * Generate the index update for a data row from the mutation that are obtained by merging the previous data row
     * state with the pending row mutation for index rebuild. This method is called only for global indexes.
     * pendingMutations is a sorted list of data table mutations that are used to replay index table mutations.
     * This list is sorted in ascending order by the tuple of row key, timestamp and mutation type where delete comes
     * after put.
     */
    public static List<Mutation> prepareIndexMutationsForRebuild(IndexMaintainer indexMaintainer,
                                                                 Put dataPut, Delete dataDel) throws IOException {
        List<Mutation> dataMutations = getMutationsWithSameTS(dataPut, dataDel);
        List<Mutation> indexMutations = Lists.newArrayListWithExpectedSize(dataMutations.size());
        // The row key ptr of the data table row for which we will build index rows here
        ImmutableBytesPtr rowKeyPtr = (dataPut != null) ? new ImmutableBytesPtr(dataPut.getRow()) :
                new ImmutableBytesPtr(dataDel.getRow());
        // Start with empty data table row
        Put currentDataRow = null;
        // The index row key corresponding to the current data row
        byte[] IndexRowKeyForCurrentDataRow = null;
        for (Mutation mutation : dataMutations) {
            ValueGetter currentDataRowVG = (currentDataRow == null) ? null : new IndexRebuildRegionScanner.SimpleValueGetter(currentDataRow);
            long ts = getTimestamp(mutation);
            if (mutation instanceof Put) {
                // Add this put on top of the current data row state to get the next data row state
                Put nextDataRow = (currentDataRow == null) ? new Put((Put)mutation) : mergeNew((Put)mutation, currentDataRow);
                ValueGetter nextDataRowVG = new IndexRebuildRegionScanner.SimpleValueGetter(nextDataRow);
                Put indexPut = prepareIndexPut(indexMaintainer, rowKeyPtr, nextDataRowVG, ts, true, null, null);
                indexMutations.add(indexPut);
                // Delete the current index row if the new index key is different than the current one
                if (IndexRowKeyForCurrentDataRow != null) {
                    if (Bytes.compareTo(indexPut.getRow(), IndexRowKeyForCurrentDataRow) != 0) {
                        Delete del = new Delete(IndexRowKeyForCurrentDataRow, ts);
                        del.addFamily(indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(), ts);
                        indexMutations.add(del);
                    }
                }
                // For the next iteration
                currentDataRow = nextDataRow;
                IndexRowKeyForCurrentDataRow = indexPut.getRow();
            } else {
                if (currentDataRow != null) {
                    // We apply the delete column mutations only on the current data row state here as they are already
                    // applied for pending mutations before. Deletes are handled before puts on the same row since
                    // data mutations are sorted so. For the index table, we are only interested if the current data row
                    // is deleted or not. There is no need to apply column deletes to index rows since index rows are
                    // always full rows and all the cells in an index row have the same timestamp value. Because of
                    // this index rows versions do not share cells.
                    boolean toBeDeleted = false;
                    for (List<Cell> cells : mutation.getFamilyCellMap().values()) {
                        for (Cell cell : cells) {
                            switch (cell.getType()) {
                                case DeleteFamily:
                                case DeleteFamilyVersion:
                                    toBeDeleted = true;
                                    break;
                                case DeleteColumn:
                                case Delete:
                                    removeColumn(currentDataRow, cell);
                            }
                        }
                    }
                    if (toBeDeleted) {
                        Delete del = new Delete(IndexRowKeyForCurrentDataRow, ts);
                        del.addFamily(indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(), ts);
                        indexMutations.add(del);
                        currentDataRow = null;
                        IndexRowKeyForCurrentDataRow = null;
                    }
                }
            }
        }
        return indexMutations;
    }

    private void prepareIndexMutations(Put put, Delete del) throws IOException{
        List<Mutation> indexMutations = prepareIndexMutationsForRebuild(indexMaintainer, put, del);
        for (Mutation mutation : indexMutations) {
            byte[] indexRowKey = mutation.getRow();
            List<Mutation> mutationList = indexKeyToMutationMap.get(indexRowKey);
            if (mutationList == null) {
                mutationList = new ArrayList<>();
                mutationList.add(mutation);
                indexKeyToMutationMap.put(indexRowKey, mutationList);
            } else {
                mutationList.add(mutation);
            }
        }
    }

    @Override
    public boolean next(List<Cell> results) throws IOException {
        if (indexRowKey != null &&
                singleRowRebuildReturnCode == GlobalIndexChecker.RebuildReturnCode.NO_DATA_ROW.getValue()) {
            byte[] rowCountBytes =
                    PLong.INSTANCE.toBytes(Long.valueOf(singleRowRebuildReturnCode));
            final Cell aggKeyValue = PhoenixKeyValueUtil.newKeyValue(UNGROUPED_AGG_ROW_KEY, SINGLE_COLUMN_FAMILY,
                    SINGLE_COLUMN, AGG_TIMESTAMP, rowCountBytes, 0, rowCountBytes.length);
            results.add(aggKeyValue);
            return false;
        }
        Cell lastCell = null;
        int rowCount = 0;
        region.startRegionOperation();
        try {
            byte[] uuidValue = ServerCacheClient.generateId();
            synchronized (innerScanner) {
                do {
                    List<Cell> row = new ArrayList<Cell>();
                    hasMore = innerScanner.nextRaw(row);
                    if (!row.isEmpty()) {
                        lastCell = row.get(0); // lastCell is any cell from the last visited row
                        Put put = null;
                        Delete del = null;
                        for (Cell cell : row) {
                            if (KeyValue.Type.codeToType(cell.getTypeByte()) == KeyValue.Type.Put) {
                                if (!partialRebuild && familyMap != null && !isColumnIncluded(cell)) {
                                    continue;
                                }
                                if (put == null) {
                                    put = new Put(CellUtil.cloneRow(cell));
                                }
                                put.add(cell);
                            } else {
                                if (del == null) {
                                    del = new Delete(CellUtil.cloneRow(cell));
                                }
                                del.addDeleteMarker(cell);
                            }
                        }
                        if (put == null && del == null) {
                            continue;
                        }
                        // Always add the put first and then delete for a given row. This simplifies the logic in
                        // IndexRegionObserver
                        if (put != null) {
                            mutations.add(put);
                        }
                        if (del != null) {
                            mutations.add(del);
                        }
                        if (!verify) {
                            if (put != null) {
                                setMutationAttributes(put, uuidValue);
                            }
                            if (del != null) {
                                setMutationAttributes(del, uuidValue);
                            }
                            uuidValue = commitIfReady(uuidValue, mutations);
                        } else {
                            byte[] dataKey = (put != null) ? put.getRow() : del.getRow();
                            prepareIndexMutations(put, del);
                            dataKeyToMutationMap.put(dataKey, new Pair<Put, Delete>(put, del));
                        }
                        rowCount++;
                    }
                } while (hasMore && rowCount < pageSizeInRows);
                if (!mutations.isEmpty()) {
                    if (verify) {
                        verifyAndOrRebuildIndex();
                    } else {
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
              dataKeyToMutationMap.clear();
              indexKeyToMutationMap.clear();
            }
        }
        if (indexRowKey != null) {
            rowCount = (int) singleRowRebuildReturnCode;
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