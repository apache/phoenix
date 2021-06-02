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

import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.PHYSICAL_DATA_TABLE_NAME;
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

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.HBaseFactoryProvider;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.hbase.index.parallel.Task;
import org.apache.phoenix.hbase.index.parallel.TaskBatch;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.util.PhoenixKeyValueUtil;
import org.apache.phoenix.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is an index table region scanner which scans index table rows locally and then extracts data table row keys
 * from them. Using the data table row keys, the data table rows are scanned using the HBase client available to
 * region servers. From the data table rows, expected index table mutations are generated. These expected
 * index mutations are used for both repairing the index table rows and verifying them.
 */
public class IndexRepairRegionScanner extends GlobalIndexRegionScanner {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexRepairRegionScanner.class);

    public IndexRepairRegionScanner(final RegionScanner innerScanner,
                                     final Region region,
                                     final Scan scan,
                                     final RegionCoprocessorEnvironment env,
                                     final UngroupedAggregateRegionObserver ungroupedAggregateRegionObserver)
            throws IOException {
        super(innerScanner, region, scan, env, ungroupedAggregateRegionObserver);

        byte[] dataTableName = scan.getAttribute(PHYSICAL_DATA_TABLE_NAME);
        dataHTable = hTableFactory.getTable(new ImmutableBytesPtr(dataTableName));
        indexTableTTL = region.getTableDescriptor().getColumnFamilies()[0].getTimeToLive();
        try (org.apache.hadoop.hbase.client.Connection connection =
                     HBaseFactoryProvider.getHConnectionFactory().createConnection(env.getConfiguration())) {
            regionEndKeys = connection.getRegionLocator(dataHTable.getName()).getEndKeys();
        }
    }

    @Override
    public byte[] getDataTableName() {
        return dataHTable.getName().toBytes();
    }

    public void prepareExpectedIndexMutations(Result dataRow, Map<byte[], List<Mutation>> expectedIndexMutationMap) throws IOException {
        Put put = null;
        Delete del = null;
        for (Cell cell : dataRow.rawCells()) {
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
        List<Mutation> indexMutations = prepareIndexMutationsForRebuild(indexMaintainer, put, del);
        Collections.reverse(indexMutations);
        for (Mutation mutation : indexMutations) {
            byte[] indexRowKey = mutation.getRow();
            List<Mutation> mutationList = expectedIndexMutationMap.get(indexRowKey);
            if (mutationList == null) {
                mutationList = new ArrayList<>();
                mutationList.add(mutation);
                expectedIndexMutationMap.put(indexRowKey, mutationList);
            } else {
                mutationList.add(mutation);
            }
        }
    }

    protected void commitBatch(List<Mutation> indexUpdates) throws IOException, InterruptedException {
        ungroupedAggregateRegionObserver.checkForRegionClosingOrSplitting();
        region.batchMutate(indexUpdates.toArray(new Mutation[indexUpdates.size()]));
    }

    protected void repairIndexRows(Map<byte[], List<Mutation>> indexMutationMap,
                                    List<Mutation> indexRowsToBeDeleted,
                                    IndexToolVerificationResult verificationResult) throws IOException {
        updateIndexRows(indexMutationMap, indexRowsToBeDeleted, verificationResult);
    }

    private Map<byte[], List<Mutation>> populateExpectedIndexMutationMap(Set<byte[]> dataRowKeys) throws IOException {
        Map<byte[], List<Mutation>> expectedIndexMutationMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
        List<KeyRange> keys = new ArrayList<>(dataRowKeys.size());
        for (byte[] indexKey: dataRowKeys) {
            keys.add(PVarbinary.INSTANCE.getKeyRange(indexKey));
        }
        ScanRanges scanRanges = ScanRanges.createPointLookup(keys);
        Scan dataScan = new Scan();
        dataScan.setTimeRange(scan.getTimeRange().getMin(), scan.getTimeRange().getMax());
        scanRanges.initializeScan(dataScan);
        if(isRawFilterSupported) {
            SkipScanFilter skipScanFilter = scanRanges.getSkipScanFilter();
            dataScan.setFilter(new SkipScanFilter(skipScanFilter, true));
        }
        dataScan.setRaw(true);
        dataScan.setMaxVersions();
        dataScan.setCacheBlocks(false);
        try (ResultScanner resultScanner = dataHTable.getScanner(dataScan)) {
            for (Result result = resultScanner.next(); (result != null); result = resultScanner.next()) {
                ungroupedAggregateRegionObserver.checkForRegionClosingOrSplitting();
                prepareExpectedIndexMutations(result, expectedIndexMutationMap);
            }
        } catch (Throwable t) {
            ServerUtil.throwIOException(dataHTable.getName().toString(), t);
        }
        return expectedIndexMutationMap;
    }

    private Map<byte[], List<Mutation>> populateActualIndexMutationMap(Map<byte[], List<Mutation>> expectedIndexMutationMap) throws IOException {
        Map<byte[], List<Mutation>> actualIndexMutationMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
        Scan indexScan = prepareIndexScan(expectedIndexMutationMap);
        try (RegionScanner regionScanner = region.getScanner(indexScan)) {
            do {
                ungroupedAggregateRegionObserver.checkForRegionClosingOrSplitting();
                List<Cell> row = new ArrayList<Cell>();
                hasMore = regionScanner.nextRaw(row);
                if (!row.isEmpty()) {
                    populateIndexMutationFromIndexRow(row, actualIndexMutationMap);
                }
            } while (hasMore);
        } catch (Throwable t) {
            ServerUtil.throwIOException(region.getRegionInfo().getRegionNameAsString(), t);
        }
        return actualIndexMutationMap;
    }

    private Map<byte[], List<Mutation>> populateActualIndexMutationMap() throws IOException {
        Map<byte[], List<Mutation>> actualIndexMutationMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
        Scan indexScan = new Scan();
        indexScan.setTimeRange(scan.getTimeRange().getMin(), scan.getTimeRange().getMax());
        indexScan.setRaw(true);
        indexScan.setMaxVersions();
        indexScan.setCacheBlocks(false);
        try (RegionScanner regionScanner = region.getScanner(indexScan)) {
            do {
                ungroupedAggregateRegionObserver.checkForRegionClosingOrSplitting();
                List<Cell> row = new ArrayList<Cell>();
                hasMore = regionScanner.nextRaw(row);
                if (!row.isEmpty()) {
                    populateIndexMutationFromIndexRow(row, actualIndexMutationMap);
                }
            } while (hasMore);
        } catch (Throwable t) {
            ServerUtil.throwIOException(region.getRegionInfo().getRegionNameAsString(), t);
        }
        return actualIndexMutationMap;
    }

    private void repairAndOrVerifyIndexRows(Set<byte[]> dataRowKeys,
                                            Map<byte[], List<Mutation>> actualIndexMutationMap,
                                            IndexToolVerificationResult verificationResult) throws IOException {
        List<Mutation> indexRowsToBeDeleted = new ArrayList<>();
        Map<byte[], List<Mutation>> expectedIndexMutationMap = populateExpectedIndexMutationMap(dataRowKeys);
        if (verifyType == IndexTool.IndexVerifyType.NONE) {
            repairIndexRows(expectedIndexMutationMap, indexRowsToBeDeleted, verificationResult);
            return;
        }
        if (verifyType == IndexTool.IndexVerifyType.ONLY) {
            verifyIndexRows(actualIndexMutationMap, expectedIndexMutationMap, Collections.EMPTY_SET, indexRowsToBeDeleted, verificationResult.getBefore(), true);
            return;
        }
        if (verifyType == IndexTool.IndexVerifyType.BEFORE) {
            verifyIndexRows(actualIndexMutationMap, expectedIndexMutationMap, Collections.EMPTY_SET, indexRowsToBeDeleted, verificationResult.getBefore(), true);
            if (!expectedIndexMutationMap.isEmpty() || !indexRowsToBeDeleted.isEmpty()) {
                repairIndexRows(expectedIndexMutationMap, indexRowsToBeDeleted, verificationResult);
            }
            return;
        }
        if (verifyType == IndexTool.IndexVerifyType.AFTER) {
            repairIndexRows(expectedIndexMutationMap, Collections.EMPTY_LIST, verificationResult);
            actualIndexMutationMap = populateActualIndexMutationMap();
            verifyIndexRows(actualIndexMutationMap, expectedIndexMutationMap, Collections.EMPTY_SET, indexRowsToBeDeleted, verificationResult.getAfter(), false);
            return;
        }
        if (verifyType == IndexTool.IndexVerifyType.BOTH) {
            verifyIndexRows(actualIndexMutationMap, expectedIndexMutationMap, Collections.EMPTY_SET, indexRowsToBeDeleted, verificationResult.getBefore(), true);
            if (!expectedIndexMutationMap.isEmpty() || !indexRowsToBeDeleted.isEmpty()) {
                repairIndexRows(expectedIndexMutationMap, indexRowsToBeDeleted, verificationResult);
            }
            if (!expectedIndexMutationMap.isEmpty()) {
                actualIndexMutationMap = populateActualIndexMutationMap(expectedIndexMutationMap);
                verifyIndexRows(actualIndexMutationMap, expectedIndexMutationMap, Collections.EMPTY_SET, Collections.EMPTY_LIST, verificationResult.getAfter(), false);
            }
        }
    }

    private void addRepairAndOrVerifyTask(TaskBatch<Boolean> tasks,
                                          final Set<byte[]> dataRowKeys,
                                          final Map<byte[], List<Mutation>> actualIndexMutationMap,
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
                    repairAndOrVerifyIndexRows(dataRowKeys, actualIndexMutationMap, verificationResult);
                } catch (Exception e) {
                    throw e;
                }
                return Boolean.TRUE;
            }
        });
    }

    public static List<Set<byte[]>> getPerTaskDataRowKeys(TreeSet<byte[]> dataRowKeys,
                                                          byte[][] endKeys, int maxSetSize) {
        List<Set<byte[]>> setList = new ArrayList<>();
        int regionCount = endKeys.length;
        int regionIndex = 0;
        byte[] indexKey = dataRowKeys.first();
        Set<byte[]> perTaskDataRowKeys = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        setList.add(perTaskDataRowKeys);
        // Find the region including the first data row key
        while (regionIndex < regionCount - 1 && Bytes.BYTES_COMPARATOR.compare(indexKey, endKeys[regionIndex]) > 0) {
            regionIndex++;
        }
        for (byte[] dataRowKey: dataRowKeys) {
            indexKey = dataRowKey;
            if (perTaskDataRowKeys.size() == maxSetSize ||
                    (regionIndex < regionCount - 1 && Bytes.BYTES_COMPARATOR.compare(indexKey, endKeys[regionIndex]) > 0)) {
                perTaskDataRowKeys = new TreeSet<>(Bytes.BYTES_COMPARATOR);
                setList.add(perTaskDataRowKeys);
                // Find the region including indexKey
                while (regionIndex < regionCount - 1 && Bytes.BYTES_COMPARATOR.compare(indexKey, endKeys[regionIndex]) > 0) {
                    regionIndex++;
                }
            }
            perTaskDataRowKeys.add(dataRowKey);
        }
        return setList;
    }

    private Set<byte[]> getDataRowKeys(Map<byte[], List<Mutation>> indexMutationMap) {
        Set<byte[]> dataRowKeys = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        for (byte[] indexRowKey: indexMutationMap.keySet()) {
            byte[] dataRowKey = indexMaintainer.buildDataRowKey(new ImmutableBytesWritable(indexRowKey), viewConstants);
            dataRowKeys.add(dataRowKey);
        }
        return dataRowKeys;
    }

    /**
     * @param indexMutationMap actual index mutations for a page
     * @param dataRowKeysSetList List of per-task data row keys
     * @return For each set of data row keys, split the acutal index mutation map into
     * a per-task index mutation map and return the list of all index mutation maps.
     */
    private List<Map<byte[], List<Mutation>>> getPerTaskIndexMutationMap(
            Map<byte[], List<Mutation>> indexMutationMap, List<Set<byte[]>> dataRowKeysSetList) {
        List<Map<byte[], List<Mutation>>> mapList = Lists.newArrayListWithExpectedSize(dataRowKeysSetList.size());
        for (int i = 0; i < dataRowKeysSetList.size(); ++i) {
            Map<byte[], List<Mutation>> perTaskIndexMutationMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
            mapList.add(perTaskIndexMutationMap);
        }
        for (Map.Entry<byte[], List<Mutation>> entry : indexMutationMap.entrySet()) {
            byte[] indexRowKey = entry.getKey();
            List<Mutation> actualMutationList = entry.getValue();
            byte[] dataRowKey = indexMaintainer.buildDataRowKey(new ImmutableBytesWritable(indexRowKey), viewConstants);
            for (int i = 0; i < dataRowKeysSetList.size(); ++i) {
                if (dataRowKeysSetList.get(i).contains(dataRowKey)) {
                    mapList.get(i).put(indexRowKey, actualMutationList);
                    break;
                }
            }
        }
        return mapList;
    }

    private void verifyAndOrRepairIndex(Map<byte[], List<Mutation>> actualIndexMutationMap) throws IOException {
        if (actualIndexMutationMap.size() == 0) {
            return;
        }
        Set<byte[]> dataRowKeys = getDataRowKeys(actualIndexMutationMap);
        List<Set<byte[]>> setList = getPerTaskDataRowKeys((TreeSet<byte[]>) dataRowKeys,
                regionEndKeys, rowCountPerTask);
        List<Map<byte[], List<Mutation>>> indexMutationMapList = getPerTaskIndexMutationMap(actualIndexMutationMap, setList);
        int taskCount = setList.size();
        TaskBatch<Boolean> tasks = new TaskBatch<>(taskCount);
        List<IndexToolVerificationResult> verificationResultList = new ArrayList<>(taskCount);
        for (int i = 0; i < taskCount; i++) {
            IndexToolVerificationResult perTaskVerificationResult = new IndexToolVerificationResult(scan);
            verificationResultList.add(perTaskVerificationResult);
            addRepairAndOrVerifyTask(tasks, setList.get(i), indexMutationMapList.get(i), perTaskVerificationResult);
        }
        submitTasks(tasks);
        if (verify) {
            for (IndexToolVerificationResult result : verificationResultList) {
                verificationResult.add(result);
            }
        }
    }

    private int populateIndexMutationFromIndexRow(List<Cell> row, Map<byte[], List<Mutation>> indexMutationMap)
            throws IOException {
        Put put = null;
        Delete del = null;
        for (Cell cell : row) {
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
        byte[] indexRowKey;
        if (put != null) {
            indexRowKey = put.getRow();
        } else if (del != null) {
            indexRowKey = del.getRow();
        }
        else {
            return 0;
        }
        List<Mutation> mutationList = getMutationsWithSameTS(put, del, MUTATION_TS_DESC_COMPARATOR);
        indexMutationMap.put(indexRowKey, mutationList);
        return mutationList.size();
    }

    @Override
    public boolean next(List<Cell> results) throws IOException {
        Map<byte[], List<Mutation>> indexMutationMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
        Cell lastCell = null;
        int rowCount = 0;
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
                     */
                    ungroupedAggregateRegionObserver.checkForRegionClosingOrSplitting();
                    List<Cell> row = new ArrayList<Cell>();
                    hasMore = localScanner.nextRaw(row);
                    if (!row.isEmpty()) {
                        lastCell = row.get(0); // lastCell is any cell from the last visited row
                        if (isDummy(row)) {
                            break;
                        }
                        indexMutationCount += populateIndexMutationFromIndexRow(row, indexMutationMap);
                        rowCount++;
                    }
                } while (hasMore && indexMutationCount < pageSizeInRows);
                if (!indexMutationMap.isEmpty()) {
                    verifyAndOrRepairIndex(indexMutationMap);
                }
                if (verify) {
                    verificationResult.setScannedDataRowCount(verificationResult.getScannedDataRowCount() + rowCount);
                }
            }
        } catch (Throwable e) {
            LOGGER.error("Exception in IndexRepairRegionScanner for region "
                    + region.getRegionInfo().getRegionNameAsString(), e);
            throw e;
        } finally {
            region.closeRegionOperation();
            if (localScanner!=null && localScanner!=innerScanner) {
                localScanner.close();
            }
        }

        if (minTimestamp != 0) {
            nextStartKey = ByteUtil.calculateTheClosestNextRowKeyForPrefix(CellUtil.cloneRow(lastCell));
        }
        byte[] rowCountBytes = PLong.INSTANCE.toBytes(Long.valueOf(rowCount));
        final Cell aggKeyValue;
        if (lastCell == null) {
            aggKeyValue = PhoenixKeyValueUtil.newKeyValue(UNGROUPED_AGG_ROW_KEY,
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
