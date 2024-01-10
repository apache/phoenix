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

import static org.apache.phoenix.query.QueryConstants.AGG_TIMESTAMP;
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN;
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN_FAMILY;
import static org.apache.phoenix.query.QueryConstants.UNGROUPED_AGG_ROW_KEY;
import static org.apache.phoenix.util.ScanUtil.isDummy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.HBaseFactoryProvider;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.hbase.index.parallel.Task;
import org.apache.phoenix.hbase.index.parallel.TaskBatch;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.GlobalIndexChecker;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.util.ClientUtil;
import org.apache.phoenix.util.PhoenixKeyValueUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a data table region scanner which scans data table rows locally. From the data table rows, expected
 * index table mutations are generated. These expected index mutations are used for both rebuilding index table
 * rows and also verifying them. The HBase client available to region servers are used to update or verify index
 * table rows.
 */
public class IndexRebuildRegionScanner extends GlobalIndexRegionScanner {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexRebuildRegionScanner.class);
    private static boolean ignoreIndexRebuildForTesting  = false;
    private static boolean throwExceptionForRebuild  = false;
    public static void setIgnoreIndexRebuildForTesting(boolean ignore) { ignoreIndexRebuildForTesting = ignore; }
    public static void setThrowExceptionForRebuild(boolean throwException) { throwExceptionForRebuild = throwException; }
    private int singleRowRebuildReturnCode;


    @VisibleForTesting
    public IndexRebuildRegionScanner(final RegionScanner innerScanner,
                                     final Region region,
                                     final Scan scan,
                                     final RegionCoprocessorEnvironment env,
                                     final UngroupedAggregateRegionObserver ungroupedAggregateRegionObserver)
            throws IOException {
        super(innerScanner, region, scan, env, ungroupedAggregateRegionObserver);

        indexHTable = hTableFactory.getTable(new ImmutableBytesPtr(indexMaintainer.getIndexTableName()));
        indexTableTTL = indexHTable.getDescriptor().getColumnFamilies()[0].getTimeToLive();
        indexRowKeyforReadRepair = scan.getAttribute(BaseScannerRegionObserverConstants.INDEX_ROW_KEY);
        if (indexRowKeyforReadRepair != null) {
            setReturnCodeForSingleRowRebuild();
            pageSizeInRows = 1;
            return;
        }
        try (org.apache.hadoop.hbase.client.Connection connection =
                     HBaseFactoryProvider.getHConnectionFactory().createConnection(env.getConfiguration())) {
            regionEndKeys = connection.getRegionLocator(indexHTable.getName()).getEndKeys();
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
                if (indexMaintainer.checkIndexRow(indexRowKeyforReadRepair, put)) {
                    singleRowRebuildReturnCode = GlobalIndexChecker.RebuildReturnCode.INDEX_ROW_EXISTS.getValue();
                } else {
                    singleRowRebuildReturnCode = GlobalIndexChecker.RebuildReturnCode.NO_INDEX_ROW.getValue();
                }
            }
        }
    }


    protected void commitBatch(List<Mutation> indexUpdates) throws IOException, InterruptedException {
        ungroupedAggregateRegionObserver.checkForRegionClosingOrSplitting();
        indexHTable.batch(indexUpdates, null);
    }

    protected void rebuildIndexRows(Map<byte[], List<Mutation>> indexMutationMap,
                                    List<Mutation> indexRowsToBeDeleted,
                                    IndexToolVerificationResult verificationResult) throws IOException {
        if (ignoreIndexRebuildForTesting) {
            return;
        }
        if (throwExceptionForRebuild) {
            throw new IOException("Exception for testing. Something happened");
        }
        updateIndexRows(indexMutationMap, indexRowsToBeDeleted, verificationResult);
    }


    private Map<byte[], List<Mutation>> populateActualIndexMutationMap(Map<byte[], List<Mutation>> expectedIndexMutationMap) throws IOException {
        Map<byte[], List<Mutation>> actualIndexMutationMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
        Scan indexScan = prepareIndexScan(expectedIndexMutationMap);
        try (ResultScanner resultScanner = indexHTable.getScanner(indexScan)) {
            for (Result result = resultScanner.next(); (result != null); result = resultScanner.next()) {
                ungroupedAggregateRegionObserver.checkForRegionClosingOrSplitting();
                List<Mutation> mutationList = prepareActualIndexMutations(result);
                actualIndexMutationMap.put(result.getRow(), mutationList);
            }
        } catch (Throwable t) {
            ClientUtil.throwIOException(indexHTable.getName().toString(), t);
        }
        return actualIndexMutationMap;
    }


    private void rebuildAndOrVerifyIndexRows(Map<byte[], List<Mutation>> expectedIndexMutationMap,
                                             Set<byte[]> mostRecentIndexRowKeys,
                                             IndexToolVerificationResult verificationResult) throws IOException {
        List<Mutation> indexRowsToBeDeleted = new ArrayList<>();
        if (verifyType == IndexTool.IndexVerifyType.NONE) {
            rebuildIndexRows(expectedIndexMutationMap, indexRowsToBeDeleted, verificationResult);
            return;
        }
        if (verifyType == IndexTool.IndexVerifyType.ONLY) {
            Map<byte[], List<Mutation>> actualIndexMutationMap = populateActualIndexMutationMap(expectedIndexMutationMap);
            verifyIndexRows(actualIndexMutationMap, expectedIndexMutationMap, mostRecentIndexRowKeys, Collections.EMPTY_LIST, verificationResult.getBefore(), true);
            return;
        }
        if (verifyType == IndexTool.IndexVerifyType.BEFORE) {
            Map<byte[], List<Mutation>> actualIndexMutationMap = populateActualIndexMutationMap(expectedIndexMutationMap);
            verifyIndexRows(actualIndexMutationMap, expectedIndexMutationMap, mostRecentIndexRowKeys, indexRowsToBeDeleted, verificationResult.getBefore(), true);
            if (!expectedIndexMutationMap.isEmpty() || !indexRowsToBeDeleted.isEmpty()) {
                rebuildIndexRows(expectedIndexMutationMap, indexRowsToBeDeleted, verificationResult);
            }
            return;
        }
        if (verifyType == IndexTool.IndexVerifyType.AFTER) {
            rebuildIndexRows(expectedIndexMutationMap, Collections.EMPTY_LIST, verificationResult);
            Map<byte[], List<Mutation>> actualIndexMutationMap = populateActualIndexMutationMap(expectedIndexMutationMap);
            verifyIndexRows(actualIndexMutationMap, expectedIndexMutationMap, mostRecentIndexRowKeys, Collections.EMPTY_LIST, verificationResult.getAfter(), false);
            return;
        }
        if (verifyType == IndexTool.IndexVerifyType.BOTH) {
            Map<byte[], List<Mutation>> actualIndexMutationMap = populateActualIndexMutationMap(expectedIndexMutationMap);
            verifyIndexRows(actualIndexMutationMap,expectedIndexMutationMap, mostRecentIndexRowKeys, indexRowsToBeDeleted, verificationResult.getBefore(), true);
            if (!expectedIndexMutationMap.isEmpty() || !indexRowsToBeDeleted.isEmpty()) {
                rebuildIndexRows(expectedIndexMutationMap, indexRowsToBeDeleted, verificationResult);
            }
            if (!expectedIndexMutationMap.isEmpty()) {
                actualIndexMutationMap = populateActualIndexMutationMap(expectedIndexMutationMap);
                verifyIndexRows(actualIndexMutationMap, expectedIndexMutationMap, mostRecentIndexRowKeys, Collections.EMPTY_LIST, verificationResult.getAfter(), false);
            }
        }
    }

    private void addRebuildAndOrVerifyTask(TaskBatch<Boolean> tasks,
                                           final Map<byte[], List<Mutation>> indexMutationMap,
                                           final Set<byte[]> mostRecentIndexRowKeys,
                                           final IndexToolVerificationResult verificationResult) {
        tasks.add(new Task<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                try {
                    //in HBase 1.x we could check if the coproc environment was closed or aborted,
                    //but in HBase 2.x the coproc environment can't check region server services
                    if (Thread.currentThread().isInterrupted()) {
                        exceptionMessage = "Pool closed, not attempting to rebuild and/or verify index rows! " + indexHTable.getName();
                        throw new IOException(exceptionMessage);
                    }
                    rebuildAndOrVerifyIndexRows(indexMutationMap, mostRecentIndexRowKeys, verificationResult);
                } catch (Exception e) {
                    throw e;
                }
                return Boolean.TRUE;
            }
        });
    }

    public static List<Map<byte[], List<Mutation>>> getPerTaskIndexMutationMaps(
            TreeMap<byte[], List<Mutation>> indexMutationMap, byte[][] endKeys, int maxMapSize) {
        List<Map<byte[], List<Mutation>>> mapList = new ArrayList<>();
        int regionCount = endKeys.length;
        int regionIndex = 0;
        byte[] indexKey = indexMutationMap.firstKey();
        Map<byte[], List<Mutation>> perTaskIndexMutationMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
        mapList.add(perTaskIndexMutationMap);
        // Find the region including the first index key
        while (regionIndex < regionCount - 1 && Bytes.BYTES_COMPARATOR.compare(indexKey, endKeys[regionIndex]) > 0) {
            regionIndex++;
        }
        for (Map.Entry<byte[], List<Mutation>> entry: indexMutationMap.entrySet()) {
            indexKey = entry.getKey();
            if (perTaskIndexMutationMap.size() == maxMapSize ||
                    (regionIndex < regionCount - 1 && Bytes.BYTES_COMPARATOR.compare(indexKey, endKeys[regionIndex]) > 0)) {
                perTaskIndexMutationMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
                mapList.add(perTaskIndexMutationMap);
                // Find the region including indexKey
                while (regionIndex < regionCount - 1 && Bytes.BYTES_COMPARATOR.compare(indexKey, endKeys[regionIndex]) > 0) {
                    regionIndex++;
                }
            }
            perTaskIndexMutationMap.put(indexKey, entry.getValue());
        }
        return mapList;
    }

    private void verifyAndOrRebuildIndex(Map<byte[], List<Mutation>> indexMutationMap,
                                         Set<byte[]> mostRecentIndexRowKeys) throws IOException {
        if (indexMutationMap.size() == 0) {
            return;
        }
        List<Map<byte[], List<Mutation>>> mapList = getPerTaskIndexMutationMaps((TreeMap)indexMutationMap,
                regionEndKeys, rowCountPerTask);
        int taskCount = mapList.size();
        TaskBatch<Boolean> tasks = new TaskBatch<>(taskCount);
        List<IndexToolVerificationResult> verificationResultList = new ArrayList<>(taskCount);
        for (int i = 0; i < taskCount; i++) {
            IndexToolVerificationResult perTaskVerificationResult = new IndexToolVerificationResult(scan);
            verificationResultList.add(perTaskVerificationResult);
            addRebuildAndOrVerifyTask(tasks, mapList.get(i), mostRecentIndexRowKeys, perTaskVerificationResult);
        }
        submitTasks(tasks);
        if (verify) {
            for (IndexToolVerificationResult result : verificationResultList) {
                verificationResult.add(result);
            }
        }
    }

    @Override
    public boolean next(List<Cell> results) throws IOException {
        if (indexRowKeyforReadRepair != null &&
                singleRowRebuildReturnCode == GlobalIndexChecker.RebuildReturnCode.NO_DATA_ROW.getValue()) {
            byte[] rowCountBytes =
                    PLong.INSTANCE.toBytes((long) singleRowRebuildReturnCode);
            byte[] rowKey;
            byte[] startKey = scan.getStartRow().length > 0 ? scan.getStartRow() :
                    region.getRegionInfo().getStartKey();
            byte[] endKey = scan.getStopRow().length > 0 ? scan.getStopRow() :
                    region.getRegionInfo().getEndKey();
            final boolean isIncompatibleClient =
                    ScanUtil.isIncompatibleClientForServerReturnValidRowKey(scan);
            if (!isIncompatibleClient) {
                rowKey = ByteUtil.getLargestPossibleRowKeyInRange(startKey, endKey);
                if (rowKey == null) {
                    if (scan.includeStartRow()) {
                        rowKey = startKey;
                    } else if (scan.includeStopRow()) {
                        rowKey = endKey;
                    } else {
                        rowKey = HConstants.EMPTY_END_ROW;
                    }
                }
            } else {
                rowKey = UNGROUPED_AGG_ROW_KEY;
            }
            final Cell aggKeyValue = PhoenixKeyValueUtil.newKeyValue(rowKey,
                    SINGLE_COLUMN_FAMILY,
                    SINGLE_COLUMN, AGG_TIMESTAMP, rowCountBytes, 0, rowCountBytes.length);
            results.add(aggKeyValue);
            return false;
        }
        Map<byte[], List<Mutation>> indexMutationMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
        Set<byte[]> mostRecentIndexRowKeys = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        Cell lastCell = null;
        int dataRowCount = 0;
        int indexMutationCount = 0;
        region.startRegionOperation();
        RegionScanner localScanner = null;
        try {
            localScanner = getLocalScanner();
            if (localScanner == null) {
                return false;
            }
            synchronized (localScanner) {
                if (!shouldVerify()) {
                    skipped = true;
                    return false;
                }
                do {
                    /*
                        If region is closing and there are large number of rows being verified/rebuilt with IndexTool,
                        not having this check will impact/delay the region closing -- affecting the availability
                        as this method holds the read lock on the region.
                    * */
                    ungroupedAggregateRegionObserver.checkForRegionClosingOrSplitting();
                    List<Cell> row = new ArrayList<>();
                    hasMore = localScanner.nextRaw(row);
                    if (!row.isEmpty()) {
                        lastCell = row.get(0); // lastCell is any cell from the last visited row
                        if (isDummy(row)) {
                            break;
                        }
                        Put put = null;
                        Delete del = null;
                        for (Cell cell : row) {
                            if (cell.getType().equals(Cell.Type.Put)) {
                                if (familyMap != null && !isColumnIncluded(cell)) {
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
                        indexMutationCount += prepareIndexMutations(put, del, indexMutationMap, mostRecentIndexRowKeys);
                        dataRowCount++;
                    }
                } while (hasMore && indexMutationCount < pageSizeInRows
                        && dataRowCount < pageSizeInRows);
                if (!indexMutationMap.isEmpty()) {
                    if (indexRowKeyforReadRepair != null) {
                        rebuildIndexRows(indexMutationMap, Collections.EMPTY_LIST, verificationResult);
                    } else {
                        verifyAndOrRebuildIndex(indexMutationMap, mostRecentIndexRowKeys);
                    }
                }
                if (verify) {
                    verificationResult.setScannedDataRowCount(verificationResult.getScannedDataRowCount() + dataRowCount);
                }
            }
        } catch (Throwable e) {
            LOGGER.error("Exception in IndexRebuildRegionScanner for region "
                    + region.getRegionInfo().getRegionNameAsString(), e);
            this.shouldRetry = true;
            throw e;
        } finally {
            region.closeRegionOperation();
            if (localScanner!=null && localScanner!=innerScanner) {
                localScanner.close();
            }
        }
        if (indexRowKeyforReadRepair != null) {
            dataRowCount = singleRowRebuildReturnCode;
        }
        if (minTimestamp != 0) {
            nextStartKey = ByteUtil.calculateTheClosestNextRowKeyForPrefix(CellUtil.cloneRow(lastCell));
        }
        byte[] rowCountBytes = PLong.INSTANCE.toBytes((long) dataRowCount);
        final Cell aggKeyValue;
        if (lastCell == null) {
            byte[] rowKey;
            byte[] startKey = scan.getStartRow().length > 0 ? scan.getStartRow() :
                    region.getRegionInfo().getStartKey();
            byte[] endKey = scan.getStopRow().length > 0 ? scan.getStopRow() :
                    region.getRegionInfo().getEndKey();
            final boolean isIncompatibleClient =
                    ScanUtil.isIncompatibleClientForServerReturnValidRowKey(scan);
            if (!isIncompatibleClient) {
                rowKey = ByteUtil.getLargestPossibleRowKeyInRange(startKey, endKey);
                if (rowKey == null) {
                    if (scan.includeStartRow()) {
                        rowKey = startKey;
                    } else if (scan.includeStopRow()) {
                        rowKey = endKey;
                    } else {
                        rowKey = HConstants.EMPTY_END_ROW;
                    }
                }
            } else {
                rowKey = UNGROUPED_AGG_ROW_KEY;
            }
            aggKeyValue = PhoenixKeyValueUtil.newKeyValue(rowKey,
                    SINGLE_COLUMN_FAMILY,
                    SINGLE_COLUMN, AGG_TIMESTAMP, rowCountBytes, 0, rowCountBytes.length);
        } else {
            aggKeyValue = PhoenixKeyValueUtil.newKeyValue(CellUtil.cloneRow(lastCell),
                SINGLE_COLUMN_FAMILY,
                    SINGLE_COLUMN, AGG_TIMESTAMP, rowCountBytes, 0, rowCountBytes.length);
        }
        results.add(aggKeyValue);
        return hasMore || hasMoreIncr;
    }
}
