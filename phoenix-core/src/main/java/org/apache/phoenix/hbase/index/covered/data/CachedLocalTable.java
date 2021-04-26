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
package org.apache.phoenix.hbase.index.covered.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.IndexManagementUtil;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.index.PhoenixIndexMetaData;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.types.PVarbinary;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.collect.Sets;

public class CachedLocalTable implements LocalHBaseState {

    private final Map<ImmutableBytesPtr, List<Cell>> rowKeyPtrToCells;
    private final Region region;

    private CachedLocalTable(Map<ImmutableBytesPtr, List<Cell>> rowKeyPtrToCells, Region region) {
        this.rowKeyPtrToCells = rowKeyPtrToCells;
        this.region = region;
    }

    @Override
    public List<Cell> getCurrentRowState(
            Mutation mutation,
            Collection<? extends ColumnReference> columnReferences,
            boolean ignoreNewerMutations) throws IOException {

        if(ignoreNewerMutations) {
            return doScan(mutation, columnReferences);
        }

        byte[] rowKey = mutation.getRow();
        return this.rowKeyPtrToCells.get(new ImmutableBytesPtr(rowKey));
    }

    private List<Cell> doScan(Mutation mutation, Collection<? extends ColumnReference> columnReferences) throws IOException {
        byte[] rowKey = mutation.getRow();
        // need to use a scan here so we can get raw state, which Get doesn't provide.
        Scan scan = IndexManagementUtil.newLocalStateScan(Collections.singletonList(columnReferences));
        scan.setStartRow(rowKey);
        scan.setStopRow(rowKey);

        // Provides a means of client indicating that newer cells should not be considered,
        // enabling mutations to be replayed to partially rebuild the index when a write fails.
        // When replaying mutations we want the oldest timestamp (as anything newer we be replayed)
        //long ts = getOldestTimestamp(m.getFamilyCellMap().values());
        long ts = getMutationTimestampWhenAllCellTimestampIsSame(mutation);
        scan.setTimeRange(0,ts);

        try (RegionScanner regionScanner = region.getScanner(scan)) {
            List<Cell> cells = new ArrayList<Cell>(1);
            boolean more = regionScanner.next(cells);
            assert !more : "Got more than one result when scanning"
                + " a single row in the primary table!";

            return cells;
         }
    }

    @VisibleForTesting
    public static CachedLocalTable build(Map<ImmutableBytesPtr, List<Cell>> rowKeyPtrToCells) {
        return new CachedLocalTable(rowKeyPtrToCells, null);
    }

    public static CachedLocalTable build(
            Collection<? extends Mutation> dataTableMutationsWithSameRowKeyAndTimestamp,
            final PhoenixIndexMetaData indexMetaData,
            Region region) throws IOException {
        if(indexMetaData.getReplayWrite() != null)
        {
            return new CachedLocalTable(Collections.emptyMap(), region);
        }
        return preScanAllRequiredRows(dataTableMutationsWithSameRowKeyAndTimestamp, indexMetaData, region);
    }

    /**
     * Pre-scan all the required rows before we building the indexes for the dataTableMutationsWithSameRowKeyAndTimestamp
     * parameter.
     * Note: When we calling this method, for single mutation in the dataTableMutationsWithSameRowKeyAndTimestamp
     * parameter, all cells in the mutation have the same rowKey and timestamp.
     * @param dataTableMutationsWithSameRowKeyAndTimestamp
     * @param indexMetaData
     * @param region
     * @throws IOException
     */
    public static CachedLocalTable preScanAllRequiredRows(
            Collection<? extends Mutation> dataTableMutationsWithSameRowKeyAndTimestamp,
            PhoenixIndexMetaData indexMetaData,
            Region region) throws IOException {
        Set<KeyRange> keys = new HashSet<KeyRange>(dataTableMutationsWithSameRowKeyAndTimestamp.size());
        for (Mutation mutation : dataTableMutationsWithSameRowKeyAndTimestamp) {
          if (indexMetaData.requiresPriorRowState(mutation)) {
            keys.add(PVarbinary.INSTANCE.getKeyRange(mutation.getRow()));
          }
        }
        if (keys.isEmpty()) {
            return new CachedLocalTable(Collections.emptyMap(), region);
        }

        List<IndexMaintainer> indexTableMaintainers = indexMetaData.getIndexMaintainers();
        Set<ColumnReference> getterColumnReferences = Sets.newHashSet();
        for (IndexMaintainer indexTableMaintainer : indexTableMaintainers) {
            getterColumnReferences.addAll(
                    indexTableMaintainer.getAllColumns());
        }

        getterColumnReferences.add(new ColumnReference(
                indexTableMaintainers.get(0).getDataEmptyKeyValueCF(),
                indexTableMaintainers.get(0).getEmptyKeyValueQualifier()));

        Scan scan = IndexManagementUtil.newLocalStateScan(
                Collections.singletonList(getterColumnReferences));
        ScanRanges scanRanges = ScanRanges.createPointLookup(new ArrayList<KeyRange>(keys));
        scanRanges.initializeScan(scan);
        SkipScanFilter skipScanFilter = scanRanges.getSkipScanFilter();

        if(indexMetaData.getReplayWrite() != null) {
            /**
             * Because of previous {@link IndexManagementUtil#flattenMutationsByTimestamp}(which is called
             * in {@link IndexRegionObserver#groupMutations} or {@link Indexer#preBatchMutateWithExceptions}),
             * for single mutation in the dataTableMutationsWithSameRowKeyAndTimestamp, all cells in the mutation
             * have the same rowKey and timestamp.
             */
            long timestamp = getMaxTimestamp(dataTableMutationsWithSameRowKeyAndTimestamp);
            scan.setTimeRange(0, timestamp);
            scan.setFilter(new SkipScanFilter(skipScanFilter, true));
        } else {
            assert scan.isRaw();
            scan.setMaxVersions(1);
            scan.setFilter(skipScanFilter);
        }

        Map<ImmutableBytesPtr, List<Cell>> rowKeyPtrToCells =
                new HashMap<ImmutableBytesPtr, List<Cell>>();
        try (RegionScanner scanner = region.getScanner(scan)) {
            boolean more = true;
            while(more) {
                List<Cell> cells = new ArrayList<Cell>();
                more = scanner.next(cells);
                if (cells.isEmpty()) {
                    continue;
                }
                Cell cell = cells.get(0);
                byte[] rowKey = CellUtil.cloneRow(cell);
                rowKeyPtrToCells.put(new ImmutableBytesPtr(rowKey), cells);
            }
        }

        return new CachedLocalTable(rowKeyPtrToCells, region);
    }

    private static long getMaxTimestamp(Collection<? extends Mutation> dataTableMutationsWithSameRowKeyAndTimestamp) {
        long maxTimestamp = Long.MIN_VALUE;
        for(Mutation mutation : dataTableMutationsWithSameRowKeyAndTimestamp) {
            /**
             * all the cells in this mutation have the same timestamp.
             */
            long timestamp = getMutationTimestampWhenAllCellTimestampIsSame(mutation);
            if(timestamp > maxTimestamp) {
                maxTimestamp = timestamp;
            }
        }
        return maxTimestamp;
    }

    private static long getMutationTimestampWhenAllCellTimestampIsSame(Mutation mutation) {
        return mutation.getFamilyCellMap().values().iterator().next().get(0).getTimestamp();
    }
}
