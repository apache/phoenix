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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
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

import static org.apache.phoenix.hbase.index.IndexRegionObserver.VERIFIED_BYTES;
import static org.apache.phoenix.hbase.index.IndexRegionObserver.removeEmptyColumn;
import static org.apache.phoenix.hbase.index.write.AbstractParallelWriterIndexCommitter.INDEX_WRITER_KEEP_ALIVE_TIME_CONF_KEY;
import static org.apache.phoenix.query.QueryConstants.AGG_TIMESTAMP;
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN;
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN_FAMILY;
import static org.apache.phoenix.query.QueryConstants.UNGROUPED_AGG_ROW_KEY;
import static org.apache.phoenix.query.QueryServices.INDEX_REBUILD_PAGE_SIZE_IN_ROWS;
import static org.apache.phoenix.query.QueryServices.MUTATE_BATCH_SIZE_ATTRIB;
import static org.apache.phoenix.query.QueryServices.MUTATE_BATCH_SIZE_BYTES_ATTRIB;

public class IndexRebuildRegionScanner extends BaseRegionScanner {
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
    private boolean verify = false;
    private boolean onlyVerify = false;
    private Map<byte[], Put> indexKeyToDataPutMap;
    private TaskRunner pool;
    private TaskBatch<Boolean> tasks;
    private String exceptionMessage;
    private UngroupedAggregateRegionObserver ungroupedAggregateRegionObserver;
    private RegionCoprocessorEnvironment env;

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
            List<IndexMaintainer> maintainers = IndexMaintainer.deserialize(indexMetaData, true);
            indexMaintainer = maintainers.get(0);
        }
        this.scan = scan;
        this.innerScanner = innerScanner;
        this.region = region;
        this.env = env;
        this.ungroupedAggregateRegionObserver = ungroupedAggregateRegionObserver;
        indexRowKey = scan.getAttribute(BaseScannerRegionObserver.INDEX_ROW_KEY);
        if (scan.getAttribute(BaseScannerRegionObserver.INDEX_REBUILD_VERIFY) != null ||
                scan.getAttribute(BaseScannerRegionObserver.INDEX_REBUILD_ONLY_VERIFY) != null) {
            verify = true;
            if (scan.getAttribute(BaseScannerRegionObserver.INDEX_REBUILD_ONLY_VERIFY) != null) {
                onlyVerify = true;
            }
            indexHTable = ServerUtil.ConnectionFactory.getConnection(ServerUtil.ConnectionType.INDEX_WRITER_CONNECTION,
                    env).getTable(TableName.valueOf(indexMaintainer.getIndexTableName()));
            indexKeyToDataPutMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
            pool = new WaitForCompletionTaskRunner(ThreadPoolManager.getExecutor(
                    new ThreadPoolBuilder("IndexVerify",
                            env.getConfiguration()).setMaxThread(NUM_CONCURRENT_INDEX_VERIFY_THREADS_CONF_KEY,
                            DEFAULT_CONCURRENT_INDEX_VERIFY_THREADS).setCoreTimeout(
                            INDEX_WRITER_KEEP_ALIVE_TIME_CONF_KEY), env));
            tasks = new TaskBatch<>(DEFAULT_CONCURRENT_INDEX_VERIFY_THREADS);
            rowCountPerTask = config.getInt(INDEX_VERIFY_ROW_COUNTS_PER_TASK_CONF_KEY,
                    DEFAULT_INDEX_VERIFY_ROW_COUNTS_PER_TASK);
        }
    }

    @Override
    public RegionInfo getRegionInfo() {
        return region.getRegionInfo();
    }

    @Override
    public boolean isFilterDone() { return hasMore; }

    @Override
    public void close() throws IOException {
        innerScanner.close();
        if (verify) {
            this.pool.stop("IndexRebuildRegionScanner is closing");
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

    private Delete generateDeleteMarkers(List<Cell> row) {
        Set<ColumnReference> allColumns = indexMaintainer.getAllColumns();
        if (row.size() == allColumns.size() + 1) {
            // We have all the columns for the index table plus the empty column. So, no delete marker is needed
            return null;
        }
        Set<ColumnReference> includedColumns = Sets.newLinkedHashSetWithExpectedSize(row.size());
        long ts = 0;
        for (Cell cell : row) {
            includedColumns.add(new ColumnReference(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell)));
            if (ts < cell.getTimestamp()) {
                ts = cell.getTimestamp();
            }
        }
        byte[] rowKey;
        Delete del = null;
        for (ColumnReference column : allColumns) {
            if (!includedColumns.contains(column)) {
                if (del == null) {
                    Cell cell = row.get(0);
                    rowKey = CellUtil.cloneRow(cell);
                    del = new Delete(rowKey);
                }
                del.addColumns(column.getFamily(), column.getQualifier(), ts);
            }
        }
        return del;
    }

    private void addToBeVerifiedIndexRows() throws IOException {
        for (Mutation mutation : mutations) {
            if (mutation instanceof Put) {
                indexKeyToDataPutMap.put(getIndexRowKey((Put)mutation), (Put)mutation);
            }
        }
    }

    private byte[] commitIfReady(byte[] uuidValue) throws IOException {
        if (ServerUtil.readyToCommit(mutations.size(), mutations.byteSize(), maxBatchSize, maxBatchSizeBytes)) {
            ungroupedAggregateRegionObserver.checkForRegionClosing();
            ungroupedAggregateRegionObserver.commitBatchWithRetries(region, mutations, blockingMemstoreSize);
            uuidValue = ServerCacheClient.generateId();
            if (verify) {
                addToBeVerifiedIndexRows();
            }
            mutations.clear();
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
                                           String errorMsg) {
        try (Table hTable = ServerUtil.ConnectionFactory.getConnection(ServerUtil.ConnectionType.INDEX_WRITER_CONNECTION,
                env).getTable(TableName.valueOf(IndexTool.OUTPUT_TABLE_NAME))) {
            byte[] rowKey;
            if (dataRowKey != null) {
                rowKey = new byte[Long.BYTES + dataRowKey.length];
                Bytes.putLong(rowKey, 0, scan.getTimeRange().getMax());
                Bytes.putBytes(rowKey, Long.BYTES, dataRowKey, 0, dataRowKey.length);
            } else {
                rowKey = new byte[Long.BYTES];
                Bytes.putLong(rowKey, 0, scan.getTimeRange().getMax());
            }
            Put put = new Put(rowKey);
            long scanMaxTs = scan.getTimeRange().getMax();
            put.addColumn(IndexTool.OUTPUT_TABLE_COLUMN_FAMILY, IndexTool.DATA_TABLE_NAME_BYTES,
                    scanMaxTs, region.getRegionInfo().getTable().getName());
            put.addColumn(IndexTool.OUTPUT_TABLE_COLUMN_FAMILY, IndexTool.INDEX_TABLE_NAME_BYTES,
                    scanMaxTs, indexMaintainer.getIndexTableName());
            if (dataRowKey != null) {
                put.addColumn(IndexTool.OUTPUT_TABLE_COLUMN_FAMILY, IndexTool.DATA_TABLE_TS_BYTES,
                        scanMaxTs, Bytes.toBytes(dataRowTs));
            }
            if (indexRowKey != null) {
                put.addColumn(IndexTool.OUTPUT_TABLE_COLUMN_FAMILY, IndexTool.INDEX_TABLE_ROW_KEY_BYTES,
                        scanMaxTs, indexRowKey);
                put.addColumn(IndexTool.OUTPUT_TABLE_COLUMN_FAMILY, IndexTool.INDEX_TABLE_TS_BYTES,
                        scanMaxTs, Bytes.toBytes(indexRowTs));
            }
            put.addColumn(IndexTool.OUTPUT_TABLE_COLUMN_FAMILY, IndexTool.ERROR_MESSAGE_BYTES,
                    scanMaxTs, Bytes.toBytes(errorMsg));
            hTable.put(put);
        } catch (IOException e) {
            exceptionMessage = "LogToIndexToolOutputTable failed " + e;
        }
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

    private void verifySingleIndexRow(Result indexRow, final Put dataRow) throws IOException {
        ValueGetter valueGetter = new SimpleValueGetter(dataRow);
        long ts = 0;
        for (List<Cell> cells : dataRow.getFamilyCellMap().values()) {
            if (cells == null) {
                break;
            }
            for (Cell cell : cells) {
                if (ts < cell.getTimestamp()) {
                    ts = cell.getTimestamp();
                }
            }
        }
        Put indexPut = indexMaintainer.buildUpdateMutation(GenericKeyValueBuilder.INSTANCE,
                valueGetter, new ImmutableBytesWritable(dataRow.getRow()), ts, null, null);
        if (indexPut == null) {
            // This means the index row does not have any covered columns. We just need to check if the index row
            // has only one cell (which is the empty column cell)
            if (indexRow.rawCells().length == 1) {
                return;
            }
            String errorMsg = "Expected to find only empty column cell but got "
                    + indexRow.rawCells().length;
            exceptionMessage = "Index verify failed - " + errorMsg + indexHTable.getName();
            logToIndexToolOutputTable(dataRow.getRow(), indexRow.getRow(), ts, getMaxTimestamp(indexRow), errorMsg);
            throw new IOException(exceptionMessage);
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
        for (List<Cell> cells : indexPut.getFamilyCellMap().values()) {
            if (cells == null) {
                break;
            }
            for (Cell expectedCell : cells) {
                byte[] family = CellUtil.cloneFamily(expectedCell);
                byte[] qualifier = CellUtil.cloneQualifier(expectedCell);
                Cell actualCell = indexRow.getColumnLatestCell(family, qualifier);
                if (actualCell == null) {
                    exceptionMessage = "Index verify failed - Missing cell " + indexHTable.getName();
                    String errorMsg = " Missing cell - " + Bytes.toStringBinary(family) + ":" +
                            Bytes.toStringBinary(qualifier);
                    logToIndexToolOutputTable(dataRow.getRow(), indexRow.getRow(), ts, getMaxTimestamp(indexRow), errorMsg);
                    if (onlyVerify) {
                        return;
                    }
                    throw new IOException(exceptionMessage);
                }
                // Check all columns
                if (!CellUtil.matchingValue(actualCell, expectedCell)) {
                    exceptionMessage = "Index verify failed - Not matching cell value - " + indexHTable.getName();
                    String errorMsg = "Not matching cell value - " + Bytes.toStringBinary(family) + ":" +
                            Bytes.toStringBinary(qualifier) + " - Expected: " +
                            Bytes.toStringBinary(CellUtil.cloneValue(expectedCell)) + " - Actual: " +
                            Bytes.toStringBinary(CellUtil.cloneValue(actualCell));
                    logToIndexToolOutputTable(dataRow.getRow(), indexRow.getRow(), ts, getMaxTimestamp(indexRow), errorMsg);
                    if (onlyVerify) {
                        return;
                    }
                    throw new IOException(exceptionMessage);
                }
                cellCount++;
            }
        }
        if (cellCount != indexRow.rawCells().length) {
            String errorMsg = "Expected to find " + cellCount + " cells but got "
                    + indexRow.rawCells().length + " cells";
            exceptionMessage = "Index verify failed - " + errorMsg + " - " + indexHTable.getName();
            logToIndexToolOutputTable(dataRow.getRow(), indexRow.getRow(), ts, getMaxTimestamp(indexRow), errorMsg);
            if (!onlyVerify) {
                throw new IOException(exceptionMessage);
            }
        }
    }

    private void verifyIndexRows(ArrayList<KeyRange> keys) throws IOException {
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
                    exceptionMessage = "Index verify failed - Missing data row - " + indexHTable.getName();
                    String errorMsg = "Missing data row";
                    logToIndexToolOutputTable(null, result.getRow(), 0, getMaxTimestamp(result), errorMsg);
                    if (!onlyVerify) {
                        throw new IOException(exceptionMessage);
                    }
                }
                verifySingleIndexRow(result, dataPut);
                rowCount++;
            }
        } catch (Throwable t) {
            ServerUtil.throwIOException(indexHTable.getName().toString(), t);
        }
        if (rowCount != expectedRowCount) {
            String errorMsg = "Missing index rows - Expected: " + expectedRowCount +
                    " Actual: " + rowCount;
                    exceptionMessage = "Index verify failed - " + errorMsg + " - " + indexHTable.getName();
            logToIndexToolOutputTable(null, null, 0, 0, errorMsg);
            if (!onlyVerify) {
                throw new IOException(exceptionMessage);
            }
        }
    }

    private void addVerifyTask(final ArrayList<KeyRange> keys) {
        tasks.add(new Task<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                try {
                    if (Thread.currentThread().isInterrupted()) {
                        exceptionMessage = "Pool closed, not attempting to verify index rows! " + indexHTable.getName();
                        throw new IOException(exceptionMessage);
                    }
                    verifyIndexRows(keys);
                } catch (Exception e) {
                    throw e;
                }
                return Boolean.TRUE;
            }
        });
    }

    @Override
    public boolean next(List<Cell> results) throws IOException {
        int rowCount = 0;
        region.startRegionOperation();
        try {
            byte[] uuidValue = ServerCacheClient.generateId();
            synchronized (innerScanner) {
                do {
                    List<Cell> row = new ArrayList<Cell>();
                    hasMore = innerScanner.nextRaw(row);
                    if (!row.isEmpty()) {
                        Put put = null;
                        Delete del = null;
                        for (Cell cell : row) {
                            if (KeyValue.Type.codeToType(cell.getTypeByte()) == KeyValue.Type.Put) {
                                if (put == null) {
                                    put = new Put(CellUtil.cloneRow(cell));
                                    setMutationAttributes(put, uuidValue);
                                    mutations.add(put);
                                }
                                put.add(cell);
                            } else {
                                if (del == null) {
                                    del = new Delete(CellUtil.cloneRow(cell));
                                    setMutationAttributes(del, uuidValue);
                                    mutations.add(del);
                                }
                                del.addDeleteMarker(cell);
                            }
                        }
                        if (onlyVerify) {
                            rowCount++;
                            continue;
                        }
                        uuidValue = commitIfReady(uuidValue);
                        if (!scan.isRaw()) {
                            Delete deleteMarkers = generateDeleteMarkers(row);
                            if (deleteMarkers != null) {
                                setMutationAttributes(deleteMarkers, uuidValue);
                                mutations.add(deleteMarkers);
                                uuidValue = commitIfReady(uuidValue);
                            }
                        }
                        if (indexRowKey != null) {
                            // GlobalIndexChecker passed the index row key. This is to build a single index row.
                            // Check if the data table row we have just scanned matches with the index row key.
                            // If not, there is no need to build the index row from this data table row,
                            // and just return zero row count.
                            if (checkIndexRow(indexRowKey, put)) {
                                rowCount = GlobalIndexChecker.RebuildReturnCode.INDEX_ROW_EXISTS.getValue();
                            }
                            else {
                                rowCount = GlobalIndexChecker.RebuildReturnCode.NO_INDEX_ROW.getValue();
                            }
                            break;
                        }
                        rowCount++;
                    }

                } while (hasMore && rowCount < pageSizeInRows);
                if (!mutations.isEmpty() && !onlyVerify) {
                    ungroupedAggregateRegionObserver.checkForRegionClosing();
                    ungroupedAggregateRegionObserver.commitBatchWithRetries(region, mutations, blockingMemstoreSize);
                    if (verify) {
                        addToBeVerifiedIndexRows();
                    }
                }
            }
        } catch (IOException e) {
            hasMore = false;
            LOGGER.error("IOException during rebuilding: " + Throwables.getStackTraceAsString(e));
            throw e;
        } finally {
            region.closeRegionOperation();
        }
        if (verify) {
            if (onlyVerify) {
                addToBeVerifiedIndexRows();
            }
            ArrayList<KeyRange> keys = new ArrayList<>(rowCountPerTask);
            for (byte[] key : indexKeyToDataPutMap.keySet()) {
                keys.add(PVarbinary.INSTANCE.getKeyRange(key));
                if (keys.size() == rowCountPerTask) {
                    addVerifyTask(keys);
                    keys = new ArrayList<>(rowCountPerTask);
                }
            }
            if (keys.size() > 0) {
                addVerifyTask(keys);
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
            finally {
                indexKeyToDataPutMap.clear();
                tasks.getTasks().clear();
            }
            for (Boolean result : taskResultList) {
                if (result == null) {
                    // there was a failure
                    throw new IOException(exceptionMessage);
                }
            }
        }
        byte[] rowCountBytes = PLong.INSTANCE.toBytes(Long.valueOf(rowCount));
        final Cell aggKeyValue = PhoenixKeyValueUtil.newKeyValue(UNGROUPED_AGG_ROW_KEY, SINGLE_COLUMN_FAMILY,
                SINGLE_COLUMN, AGG_TIMESTAMP, rowCountBytes, 0, rowCountBytes.length);
        results.add(aggKeyValue);
        return hasMore;
    }

    @Override
    public long getMaxResultSize() {
        return scan.getMaxResultSize();
    }
}