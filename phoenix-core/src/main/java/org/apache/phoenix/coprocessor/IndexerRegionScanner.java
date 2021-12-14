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

import static org.apache.phoenix.hbase.index.write.AbstractParallelWriterIndexCommitter.INDEX_WRITER_KEEP_ALIVE_TIME_CONF_KEY;
import static org.apache.phoenix.query.QueryConstants.AGG_TIMESTAMP;
import static org.apache.phoenix.query.QueryConstants.EMPTY_COLUMN_VALUE_BYTES;
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN;
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN_FAMILY;
import static org.apache.phoenix.query.QueryConstants.UNGROUPED_AGG_ROW_KEY;
import static org.apache.phoenix.util.ScanUtil.isDummy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
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
import org.apache.phoenix.hbase.index.parallel.EarlyExitFailure;
import org.apache.phoenix.hbase.index.parallel.Task;
import org.apache.phoenix.hbase.index.parallel.TaskBatch;
import org.apache.phoenix.hbase.index.parallel.ThreadPoolBuilder;
import org.apache.phoenix.hbase.index.parallel.ThreadPoolManager;
import org.apache.phoenix.hbase.index.parallel.WaitForCompletionTaskRunner;
import org.apache.phoenix.hbase.index.util.GenericKeyValueBuilder;

import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.util.PhoenixKeyValueUtil;
import org.apache.phoenix.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.base.Throwables;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

public class IndexerRegionScanner extends GlobalIndexRegionScanner {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexerRegionScanner.class);
    protected Map<byte[], Put> indexKeyToDataPutMap;
    protected UngroupedAggregateRegionObserver.MutationList mutations;
    private boolean partialRebuild = false;

    IndexerRegionScanner (final RegionScanner innerScanner, final Region region, final Scan scan,
                          final RegionCoprocessorEnvironment env,
                          UngroupedAggregateRegionObserver ungroupedAggregateRegionObserver) throws IOException {
        super(innerScanner, region, scan, env, ungroupedAggregateRegionObserver);
        indexHTable = hTableFactory.getTable(new ImmutableBytesPtr(indexMaintainer.getIndexTableName()));
        indexTableTTL = indexHTable.getTableDescriptor().getColumnFamilies()[0].getTimeToLive();
        pool = new WaitForCompletionTaskRunner(ThreadPoolManager.getExecutor(
                new ThreadPoolBuilder("IndexVerify",
                        env.getConfiguration()).setMaxThread(NUM_CONCURRENT_INDEX_VERIFY_THREADS_CONF_KEY,
                        DEFAULT_CONCURRENT_INDEX_VERIFY_THREADS).setCoreTimeout(
                        INDEX_WRITER_KEEP_ALIVE_TIME_CONF_KEY), env));
        if (scan.getAttribute(BaseScannerRegionObserver.INDEX_REBUILD_PAGING) == null) {
            partialRebuild = true;
        }
        if (verify) {
            indexKeyToDataPutMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
            verificationResult = new IndexToolVerificationResult(scan);
            verificationResultRepository =
                    new IndexVerificationResultRepository(indexMaintainer.getIndexTableName(), hTableFactory);
        } else {
            mutations = new UngroupedAggregateRegionObserver.MutationList(maxBatchSize);
        }
    }

    @Override
    public RegionInfo getRegionInfo() {
        return region.getRegionInfo();
    }

    @Override
    public boolean isFilterDone() { return false; }

    @Override
    public void close() throws IOException {
        innerScanner.close();
        try {
            if (verify) {
                verificationResultRepository.logToIndexToolResultTable(verificationResult,
                        IndexTool.IndexVerifyType.ONLY, region.getRegionInfo().getRegionName());
            }
        } finally {
            this.pool.stop("IndexerRegionScanner is closing");
            if (verify) {
                verificationResultRepository.close();
            }
            super.close();
        }
    }

    private boolean verifySingleIndexRow(Result indexRow, final Put dataRow,
            IndexToolVerificationResult.PhaseResult verificationPhaseResult) throws IOException {
        ValueGetter valueGetter = new SimpleValueGetter(dataRow);
        long ts = getMaxTimestamp(dataRow);
        Put indexPut = indexMaintainer.buildUpdateMutation(GenericKeyValueBuilder.INSTANCE,
                valueGetter, new ImmutableBytesWritable(dataRow.getRow()), ts, null, null, false);

        if (indexPut == null) {
            // This means the data row does not have any covered column values
            indexPut = new Put(indexRow.getRow());
        }
        // Add the empty column
        indexPut.addColumn(indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                indexMaintainer.getEmptyKeyValueQualifier(), ts, EMPTY_COLUMN_VALUE_BYTES);

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

                    // or if cell timestamp is beyond maxlookback
                    if (isTimestampBeforeTTL(indexTableTTL, currentTime, expectedCell.getTimestamp())) {
                        continue;
                    }

                    return false;
                }
                if (actualCell.getTimestamp() < ts) {
                    // Skip older cells since a Phoenix index row is composed of cells with the same timestamp
                    continue;
                }
                // Check all columns
                if (!CellUtil.matchingValue(actualCell, expectedCell) || actualCell.getTimestamp() != ts) {
                    if(isTimestampBeyondMaxLookBack(maxLookBackInMills, currentTime, actualCell.getTimestamp())) {
                        verificationPhaseResult
                                .setBeyondMaxLookBackInvalidIndexRowCount(verificationPhaseResult
                                        .getBeyondMaxLookBackInvalidIndexRowCount()+1);
                        continue;
                    }
                    return false;
                }
                cellCount++;
            }
        }
        return cellCount == indexRow.rawCells().length;
    }

    private void verifyIndexRows(List<KeyRange> keys, Map<byte[], Put> perTaskDataKeyToDataPutMap,
            IndexToolVerificationResult.PhaseResult verificationPhaseResult) throws IOException {
        ScanRanges scanRanges = ScanRanges.createPointLookup(keys);
        Scan indexScan = new Scan();
        indexScan.setTimeRange(scan.getTimeRange().getMin(), scan.getTimeRange().getMax());
        scanRanges.initializeScan(indexScan);
        SkipScanFilter skipScanFilter = scanRanges.getSkipScanFilter();
        indexScan.setFilter(skipScanFilter);
        indexScan.setCacheBlocks(false);
        try (ResultScanner resultScanner = indexHTable.getScanner(indexScan)) {
            for (Result result = resultScanner.next(); (result != null); result = resultScanner.next()) {
                Put dataPut = indexKeyToDataPutMap.get(result.getRow());
                if (dataPut == null) {
                    // This should never happen
                    exceptionMessage = "Index verify failed - Missing data row - " + indexHTable.getName();
                    throw new IOException(exceptionMessage);
                }
                if (verifySingleIndexRow(result, dataPut, verificationPhaseResult)) {
                    verificationPhaseResult.setValidIndexRowCount(verificationPhaseResult.getValidIndexRowCount()+1);
                } else {
                    verificationPhaseResult.setInvalidIndexRowCount(verificationPhaseResult.getInvalidIndexRowCount()+1);
                }
                perTaskDataKeyToDataPutMap.remove(dataPut.getRow());
            }
        } catch (Throwable t) {
            ServerUtil.throwIOException(indexHTable.getName().toString(), t);
        }
        // Check if any expected rows from index(which we didn't get) are already expired due to TTL
        if (!perTaskDataKeyToDataPutMap.isEmpty()) {
            Iterator<Entry<byte[], Put>> itr = perTaskDataKeyToDataPutMap.entrySet().iterator();
            long currentTime = EnvironmentEdgeManager.currentTime();
            while(itr.hasNext()) {
                Entry<byte[], Put> entry = itr.next();
                long ts = getMaxTimestamp(entry.getValue());
                if (isTimestampBeforeTTL(indexTableTTL, currentTime, ts)) {
                    itr.remove();
                    verificationPhaseResult.setExpiredIndexRowCount(verificationPhaseResult.getExpiredIndexRowCount()+1);
                }
            }
        }
        // Check if any expected rows from index(which we didn't get) are beyond max look back and have been compacted away
        if (!perTaskDataKeyToDataPutMap.isEmpty()) {
            for (Entry<byte[], Put> entry : perTaskDataKeyToDataPutMap.entrySet()) {
                Put put = entry.getValue();
                long ts = getMaxTimestamp(put);
                long currentTime = EnvironmentEdgeManager.currentTime();
                if (isTimestampBeyondMaxLookBack(maxLookBackInMills, currentTime, ts)) {
                    verificationPhaseResult.
                            setBeyondMaxLookBackMissingIndexRowCount(verificationPhaseResult.getBeyondMaxLookBackMissingIndexRowCount() + 1);
                } else {
                    verificationPhaseResult.setMissingIndexRowCount(
                            verificationPhaseResult.getMissingIndexRowCount() + 1);
                }
            }
        }
    }

    private void addVerifyTask(TaskBatch<Boolean> tasks, final List<KeyRange> keys, final Map<byte[], Put> perTaskDataKeyToDataPutMap,
            final IndexToolVerificationResult.PhaseResult verificationPhaseResult) {
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

    private void parallelizeIndexVerify(IndexToolVerificationResult.PhaseResult verificationPhaseResult) throws IOException {
        int taskCount = (indexKeyToDataPutMap.size() + rowCountPerTask - 1) / rowCountPerTask;
        TaskBatch<Boolean> tasks = new TaskBatch<>(taskCount);

        List<Map<byte[], Put>> dataPutMapList = new ArrayList<>(taskCount);
        List<IndexToolVerificationResult.PhaseResult> verificationPhaseResultList = new ArrayList<>(taskCount);
        List<KeyRange> keys = new ArrayList<>(rowCountPerTask);

        Map<byte[], Put> perTaskDataKeyToDataPutMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);

        dataPutMapList.add(perTaskDataKeyToDataPutMap);

        IndexToolVerificationResult.PhaseResult perTaskVerificationPhaseResult = new IndexToolVerificationResult.PhaseResult();
        verificationPhaseResultList.add(perTaskVerificationPhaseResult);

        for (Map.Entry<byte[], Put> entry: indexKeyToDataPutMap.entrySet()) {
            keys.add(PVarbinary.INSTANCE.getKeyRange(entry.getKey()));
            perTaskDataKeyToDataPutMap.put(entry.getValue().getRow(), entry.getValue());
            if (keys.size() == rowCountPerTask) {
                addVerifyTask(tasks, keys, perTaskDataKeyToDataPutMap, perTaskVerificationPhaseResult);
                keys = new ArrayList<>(rowCountPerTask);
                perTaskDataKeyToDataPutMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
                dataPutMapList.add(perTaskDataKeyToDataPutMap);
                perTaskVerificationPhaseResult = new IndexToolVerificationResult.PhaseResult();
                verificationPhaseResultList.add(perTaskVerificationPhaseResult);
            }
        }
        if (keys.size() > 0) {
            addVerifyTask(tasks, keys, perTaskDataKeyToDataPutMap, perTaskVerificationPhaseResult);
        }
        Pair<List<Boolean>, List<Future<Boolean>>> resultsAndFutures = null;
        try {
            LOGGER.debug("Waiting on index verify tasks to complete...");
            resultsAndFutures = this.pool.submitUninterruptible(tasks);
        } catch (ExecutionException e) {
            throw new RuntimeException("Should not fail on the results while using a WaitForCompletionTaskRunner", e);
        } catch (EarlyExitFailure e) {
            throw new RuntimeException("Stopped while waiting for batch, quitting!", e);
        }
        int index = 0;
        for (Boolean result : resultsAndFutures.getFirst()) {
            if (result == null) {
                Throwable cause = ServerUtil.getExceptionFromFailedFuture(resultsAndFutures.getSecond().get(index));
                // there was a failure
                throw new IOException(exceptionMessage, cause);
            }
            index++;
        }
        for (IndexToolVerificationResult.PhaseResult result : verificationPhaseResultList) {
            verificationPhaseResult.add(result);
        }
    }

    private void verifyIndex() throws IOException {
        IndexToolVerificationResult nextVerificationResult = new IndexToolVerificationResult(scan);
        nextVerificationResult.setScannedDataRowCount(indexKeyToDataPutMap.size());
        IndexToolVerificationResult.PhaseResult verificationPhaseResult = new IndexToolVerificationResult.PhaseResult();
        // For these options we start with verifying index rows
        parallelizeIndexVerify(verificationPhaseResult);
        nextVerificationResult.getBefore().add(verificationPhaseResult);
        indexKeyToDataPutMap.clear();
        verificationResult.add(nextVerificationResult);
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

    @Override
    public boolean next(List<Cell> results) throws IOException {
        Cell lastCell = null;
        int rowCount = 0;
        region.startRegionOperation();
        try {
            synchronized (innerScanner) {
                byte[] uuidValue = ServerCacheClient.generateId();
                do {
                    List<Cell> row = new ArrayList<>();
                    hasMore = innerScanner.nextRaw(row);
                    if (!row.isEmpty()) {
                        lastCell = row.get(0); // lastCell is any cell from the last visited row
                        if (isDummy(row)) {
                            break;
                        }
                        Put put = null;
                        Delete del = null;
                        for (Cell cell : row) {
                            if (cell.getType().equals(Cell.Type.Put)) {
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
                                del.add(cell);
                            }
                        }
                        if (put == null && del == null) {
                            continue;
                        }
                        if (!verify) {
                            if (put != null) {
                                setMutationAttributes(put, uuidValue);
                                mutations.add(put);
                            }
                            if (del != null) {
                                setMutationAttributes(del, uuidValue);
                                mutations.add(del);
                            }
                            uuidValue = commitIfReady(uuidValue, mutations);
                        } else {
                            indexKeyToDataPutMap
                                    .put(getIndexRowKey(indexMaintainer, put), put);
                        }
                        rowCount++;

                    }
                } while (hasMore && rowCount < pageSizeInRows);
                if (verify) {
                    verifyIndex();
                } else if (!mutations.isEmpty()) {
                    ungroupedAggregateRegionObserver.checkForRegionClosingOrSplitting();
                    ungroupedAggregateRegionObserver.commitBatchWithRetries(region, mutations, blockingMemstoreSize);
                    mutations.clear();
                }
            }
        } catch (IOException e) {
            LOGGER.error(String.format("IOException during rebuilding: %s", Throwables.getStackTraceAsString(e)));
            throw e;
        } finally {
            region.closeRegionOperation();
            if (verify) {
                indexKeyToDataPutMap.clear();
            } else {
                mutations.clear();
            }
        }
        byte[] rowCountBytes = PLong.INSTANCE.toBytes(Long.valueOf(rowCount));
        final Cell aggKeyValue;
        if (lastCell == null) {
            aggKeyValue = PhoenixKeyValueUtil.newKeyValue(UNGROUPED_AGG_ROW_KEY,
                SINGLE_COLUMN_FAMILY,
                    SINGLE_COLUMN, AGG_TIMESTAMP, rowCountBytes,0, rowCountBytes.length);
        } else {
            aggKeyValue = PhoenixKeyValueUtil.newKeyValue(CellUtil.cloneRow(lastCell),
                SINGLE_COLUMN_FAMILY,
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
