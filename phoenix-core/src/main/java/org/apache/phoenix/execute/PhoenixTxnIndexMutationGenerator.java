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
package org.apache.phoenix.execute;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.hbase.index.MultiMutation;
import org.apache.phoenix.hbase.index.ValueGetter;
import org.apache.phoenix.hbase.index.covered.IndexMetaData;
import org.apache.phoenix.hbase.index.covered.IndexUpdate;
import org.apache.phoenix.hbase.index.covered.TableState;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.covered.update.ColumnTracker;
import org.apache.phoenix.hbase.index.covered.update.IndexedColumnGroup;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.transaction.PhoenixTransactionContext;
import org.apache.phoenix.transaction.PhoenixTransactionContext.PhoenixVisibilityLevel;
import org.apache.phoenix.util.PhoenixKeyValueUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;


public class PhoenixTxnIndexMutationGenerator {

    private static final Log LOG = LogFactory.getLog(PhoenixTxnIndexMutationGenerator.class);

    private final PhoenixConnection connection;
    private final PhoenixTransactionContext phoenixTransactionContext;

    PhoenixTxnIndexMutationGenerator(PhoenixConnection connection, PhoenixTransactionContext phoenixTransactionContext) {
        this.phoenixTransactionContext = phoenixTransactionContext;
        this.connection = connection;
    }

    private static void addMutation(Map<ImmutableBytesPtr, MultiMutation> mutations, ImmutableBytesPtr row, Mutation m) {
        MultiMutation stored = mutations.get(row);
        // we haven't seen this row before, so add it
        if (stored == null) {
            stored = new MultiMutation(row);
            mutations.put(row, stored);
        }
        stored.addAll(m);
    }

    public List<Mutation> getIndexUpdates(final PTable table, PTable index, List<Mutation> dataMutations) throws IOException, SQLException {

        if (dataMutations.isEmpty()) {
            return new ArrayList<Mutation>();
        }

        Map<String,byte[]> updateAttributes = dataMutations.get(0).getAttributesMap();
        boolean replyWrite = (BaseScannerRegionObserver.ReplayWrite.fromBytes(updateAttributes.get(BaseScannerRegionObserver.REPLAY_WRITES)) != null);
        byte[] txRollbackAttribute = updateAttributes.get(PhoenixTransactionContext.TX_ROLLBACK_ATTRIBUTE_KEY);

        IndexMaintainer maintainer = index.getIndexMaintainer(table, connection);

        boolean isRollback = txRollbackAttribute!=null;
        boolean isImmutable = index.isImmutableRows();
        ResultScanner currentScanner = null;
        Table txTable = null;
        // Collect up all mutations in batch
        Map<ImmutableBytesPtr, MultiMutation> mutations =
                new HashMap<ImmutableBytesPtr, MultiMutation>();
        Map<ImmutableBytesPtr, MultiMutation> findPriorValueMutations;
        if (isImmutable && !isRollback) {
            findPriorValueMutations = new HashMap<ImmutableBytesPtr, MultiMutation>();
        } else {
            findPriorValueMutations = mutations;
        }
        // Collect the set of mutable ColumnReferences so that we can first
        // run a scan to get the current state. We'll need this to delete
        // the existing index rows.
        int estimatedSize = 10;
        Set<ColumnReference> mutableColumns = Sets.newHashSetWithExpectedSize(estimatedSize);
        // For transactional tables, we use an index maintainer
        // to aid in rollback if there's a KeyValue column in the index. The alternative would be
        // to hold on to all uncommitted index row keys (even ones already sent to HBase) on the
        // client side.
        Set<ColumnReference> allColumns = maintainer.getAllColumns();
        mutableColumns.addAll(allColumns);

        for(final Mutation m : dataMutations) {
            // add the mutation to the batch set
            ImmutableBytesPtr row = new ImmutableBytesPtr(m.getRow());
            // if we have no non PK columns, no need to find the prior values

            boolean requiresPriorRowState =  !isImmutable || (maintainer.isRowDeleted(m) && !maintainer.getIndexedColumns().isEmpty());
            if (mutations != findPriorValueMutations && requiresPriorRowState) {
                addMutation(findPriorValueMutations, row, m);
            }
            addMutation(mutations, row, m);
        }
        
        List<Mutation> indexUpdates = new ArrayList<Mutation>(mutations.size() * 2);
        try {
            // Track if we have row keys with Delete mutations (or Puts that are
            // Tephra's Delete marker). If there are none, we don't need to do the scan for
            // prior versions, if there are, we do. Since rollbacks always have delete mutations,
            // this logic will work there too.
            if (!findPriorValueMutations.isEmpty()) {
                List<KeyRange> keys = Lists.newArrayListWithExpectedSize(mutations.size());
                for (ImmutableBytesPtr ptr : findPriorValueMutations.keySet()) {
                    keys.add(PVarbinary.INSTANCE.getKeyRange(ptr.copyBytesIfNecessary()));
                }
                Scan scan = new Scan();
                // Project all mutable columns
                for (ColumnReference ref : mutableColumns) {
                    scan.addColumn(ref.getFamily(), ref.getQualifier());
                }
                /*
                 * Indexes inherit the storage scheme of the data table which means all the indexes have the same
                 * storage scheme and empty key value qualifier. Note that this assumption would be broken if we start
                 * supporting new indexes over existing data tables to have a different storage scheme than the data
                 * table.
                 */
                byte[] emptyKeyValueQualifier = maintainer.getEmptyKeyValueQualifier();
                
                // Project empty key value column
                scan.addColumn(maintainer.getDataEmptyKeyValueCF(), emptyKeyValueQualifier);
                ScanRanges scanRanges = ScanRanges.create(SchemaUtil.VAR_BINARY_SCHEMA, Collections.singletonList(keys), ScanUtil.SINGLE_COLUMN_SLOT_SPAN, KeyRange.EVERYTHING_RANGE, null, true, -1);
                scanRanges.initializeScan(scan);
                txTable =  connection.getQueryServices().getTable(table.getPhysicalName().getBytes());
                // For rollback, we need to see all versions, including
                // the last committed version as there may be multiple
                // checkpointed versions.
                SkipScanFilter filter = scanRanges.getSkipScanFilter();
                if (isRollback) {
                    filter = new SkipScanFilter(filter,true);
                    phoenixTransactionContext.setVisibilityLevel(PhoenixVisibilityLevel.SNAPSHOT_ALL);
                }
                scan.setFilter(filter);
                currentScanner = txTable.getScanner(scan);
            }
            if (isRollback) {
                processRollback(maintainer, txRollbackAttribute, currentScanner, mutableColumns, indexUpdates, mutations, replyWrite, table);
            } else {
                processMutation(maintainer, txRollbackAttribute, currentScanner, mutableColumns, indexUpdates, mutations, findPriorValueMutations, replyWrite, table);
            }
        } finally {
            if (txTable != null) txTable.close();
        }
        
        return indexUpdates;
    }

    private void processMutation(IndexMaintainer maintainer,
            byte[] txRollbackAttribute,
            ResultScanner scanner,
            Set<ColumnReference> upsertColumns,
            Collection<Mutation> indexUpdates,
            Map<ImmutableBytesPtr, MultiMutation> mutations,
            Map<ImmutableBytesPtr, MultiMutation> mutationsToFindPreviousValue,
            boolean replyWrite,
            final PTable table) throws IOException, SQLException {
        if (scanner != null) {
            Result result;
            ColumnReference emptyColRef = new ColumnReference(maintainer
                    .getDataEmptyKeyValueCF(), maintainer.getEmptyKeyValueQualifier());
            // Process existing data table rows by removing the old index row and adding the new index row
            while ((result = scanner.next()) != null) {
                Mutation m = mutationsToFindPreviousValue.remove(new ImmutableBytesPtr(result.getRow()));
                TxTableState state = new TxTableState(upsertColumns, phoenixTransactionContext.getWritePointer(), m, emptyColRef, result);
                generateDeletes(indexUpdates, txRollbackAttribute, state, maintainer, replyWrite, table);
                generatePuts(indexUpdates, state, maintainer, replyWrite, table);
            }
        }
        // Process new data table by adding new index rows
        for (Mutation m : mutations.values()) {
            TxTableState state = new TxTableState(upsertColumns, phoenixTransactionContext.getWritePointer(), m);
            generatePuts(indexUpdates, state, maintainer, replyWrite, table);
            generateDeletes(indexUpdates, txRollbackAttribute, state, maintainer, replyWrite, table);
        }
    }

    private void processRollback(IndexMaintainer maintainer,
            byte[] txRollbackAttribute,
            ResultScanner scanner,
            Set<ColumnReference> mutableColumns,
            Collection<Mutation> indexUpdates,
            Map<ImmutableBytesPtr, MultiMutation> mutations,
            boolean replyWrite,
            final PTable table) throws IOException, SQLException {
        if (scanner != null) {
            Result result;
            // Loop through last committed row state plus all new rows associated with current transaction
            // to generate point delete markers for all index rows that were added. We don't have Tephra
            // manage index rows in change sets because we don't want to be hit with the additional
            // memory hit and do not need to do conflict detection on index rows.
            ColumnReference emptyColRef = new ColumnReference(maintainer.getDataEmptyKeyValueCF(), maintainer.getEmptyKeyValueQualifier());
            while ((result = scanner.next()) != null) {
                Mutation m = mutations.remove(new ImmutableBytesPtr(result.getRow()));
                // Sort by timestamp, type, cf, cq so we can process in time batches from oldest to newest
                // (as if we're "replaying" them in time order).
                List<Cell> cells = result.listCells();
                Collections.sort(cells, new Comparator<Cell>() {

                    @Override
                    public int compare(Cell o1, Cell o2) {
                        int c = Longs.compare(o1.getTimestamp(), o2.getTimestamp());
                        if (c != 0) return c;
                        c = o1.getTypeByte() - o2.getTypeByte();
                        if (c != 0) return c;
                        c = Bytes.compareTo(o1.getFamilyArray(), o1.getFamilyOffset(), o1.getFamilyLength(), o1.getFamilyArray(), o1.getFamilyOffset(), o1.getFamilyLength());
                        if (c != 0) return c;
                        return Bytes.compareTo(o1.getQualifierArray(), o1.getQualifierOffset(), o1.getQualifierLength(), o1.getQualifierArray(), o1.getQualifierOffset(), o1.getQualifierLength());
                    }

                });
                int i = 0;
                int nCells = cells.size();
                Result oldResult = null, newResult;
                long readPtr = phoenixTransactionContext.getReadPointer();
                do {
                    boolean hasPuts = false;
                    LinkedList<Cell> singleTimeCells = Lists.newLinkedList();
                    long writePtr;
                    Cell cell = cells.get(i);
                    do {
                        hasPuts |= cell.getTypeByte() == KeyValue.Type.Put.getCode();
                        writePtr = cell.getTimestamp();
                        ListIterator<Cell> it = singleTimeCells.listIterator();
                        do {
                            // Add at the beginning of the list to match the expected HBase
                            // newest to oldest sort order (which TxTableState relies on
                            // with the Result.getLatestColumnValue() calls). However, we
                            // still want to add Cells in the expected order for each time
                            // bound as otherwise we won't find it in our old state.
                            it.add(cell);
                        } while (++i < nCells && (cell = cells.get(i)).getTimestamp() == writePtr);
                    } while (i < nCells && cell.getTimestamp() <= readPtr);

                    // Generate point delete markers for the prior row deletion of the old index value.
                    // The write timestamp is the next timestamp, not the current timestamp,
                    // as the earliest cells are the current values for the row (and we don't
                    // want to delete the current row).
                    if (oldResult != null) {
                        TxTableState state = new TxTableState(mutableColumns, writePtr, m, emptyColRef, oldResult);
                        generateDeletes(indexUpdates, txRollbackAttribute, state, maintainer, replyWrite, table);
                    }
                    // Generate point delete markers for the new index value.
                    // If our time batch doesn't have Puts (i.e. we have only Deletes), then do not
                    // generate deletes. We would have generated the delete above based on the state
                    // of the previous row. The delete markers do not give us the state we need to
                    // delete.
                    if (hasPuts) {
                        newResult = Result.create(singleTimeCells);
                        // First row may represent the current state which we don't want to delete
                        if (writePtr > readPtr) {
                            TxTableState state = new TxTableState(mutableColumns, writePtr, m, emptyColRef, newResult);
                            generateDeletes(indexUpdates, txRollbackAttribute, state, maintainer, replyWrite, table);
                        }
                        oldResult = newResult;
                    } else {
                        oldResult = null;
                    }
                } while (i < nCells);
            }
        }
    }

    private Iterable<IndexUpdate> getIndexUpserts(TableState state, IndexMaintainer maintainer, boolean replyWrite, final PTable table) throws IOException, SQLException {
        if (maintainer.isRowDeleted(state.getPendingUpdate())) {
            return Collections.emptyList();
        }
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        ptr.set(state.getCurrentRowKey());
        List<IndexUpdate> indexUpdates = Lists.newArrayList();
        Pair<ValueGetter, IndexUpdate> statePair = state.getIndexUpdateState(maintainer.getAllColumns(), replyWrite, false, null);
        ValueGetter valueGetter = statePair.getFirst();
        IndexUpdate indexUpdate = statePair.getSecond();
        indexUpdate.setTable(maintainer.isLocalIndex() ? table.getName().getBytes() : maintainer.getIndexTableName());

        byte[] regionStartKey = null;
        byte[] regionEndkey = null;
        if(maintainer.isLocalIndex()) {
            HRegionLocation tableRegionLocation = connection.getQueryServices().getTableRegionLocation(table.getPhysicalName().getBytes(), state.getCurrentRowKey());
            regionStartKey = tableRegionLocation.getRegionInfo().getStartKey();
            regionEndkey = tableRegionLocation.getRegionInfo().getEndKey();
        }

        Put put = maintainer.buildUpdateMutation(PhoenixIndexCodec.KV_BUILDER, valueGetter, ptr, state.getCurrentTimestamp(), regionStartKey, regionEndkey);
        indexUpdate.setUpdate(put);
        indexUpdates.add(indexUpdate);

        return indexUpdates;
    }

    private Iterable<IndexUpdate> getIndexDeletes(TableState state, IndexMaintainer maintainer, boolean replyWrite, final PTable table) throws IOException, SQLException {
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        ptr.set(state.getCurrentRowKey());
        List<IndexUpdate> indexUpdates = Lists.newArrayList();
        // For transactional tables, we use an index maintainer
        // to aid in rollback if there's a KeyValue column in the index. The alternative would be
        // to hold on to all uncommitted index row keys (even ones already sent to HBase) on the
        // client side.
        Set<ColumnReference> cols = Sets.newHashSet(maintainer.getAllColumns());
        cols.add(new ColumnReference(maintainer.getDataEmptyKeyValueCF(), maintainer.getEmptyKeyValueQualifier()));
        Pair<ValueGetter, IndexUpdate> statePair = state.getIndexUpdateState(cols, replyWrite, true, null);
        ValueGetter valueGetter = statePair.getFirst();
        if (valueGetter!=null) {
            IndexUpdate indexUpdate = statePair.getSecond();
            indexUpdate.setTable(maintainer.isLocalIndex() ? table.getName().getBytes() : maintainer.getIndexTableName());

            byte[] regionStartKey = null;
            byte[] regionEndkey = null;
            if(maintainer.isLocalIndex()) {
                HRegionLocation tableRegionLocation = connection.getQueryServices().getTableRegionLocation(table.getPhysicalName().getBytes(), state.getCurrentRowKey());
                regionStartKey = tableRegionLocation.getRegionInfo().getStartKey();
                regionEndkey = tableRegionLocation.getRegionInfo().getEndKey();
            }

            Delete delete = maintainer.buildDeleteMutation(PhoenixIndexCodec.KV_BUILDER, valueGetter, ptr, state.getPendingUpdate(),
                    state.getCurrentTimestamp(), regionStartKey, regionEndkey);
            indexUpdate.setUpdate(delete);
            indexUpdates.add(indexUpdate);
        }
        return indexUpdates;
    }

    private void generateDeletes(Collection<Mutation> indexUpdates,
            byte[] attribValue,
            TxTableState state,
            IndexMaintainer maintainer,
            boolean replyWrite,
            final PTable table) throws IOException, SQLException {
        Iterable<IndexUpdate> deletes = getIndexDeletes(state, maintainer, replyWrite, table);
        for (IndexUpdate delete : deletes) {
            if (delete.isValid()) {
                delete.getUpdate().setAttribute(PhoenixTransactionContext.TX_ROLLBACK_ATTRIBUTE_KEY, attribValue);
                indexUpdates.add(delete.getUpdate());
            }
        }
    }

    private boolean generatePuts(Collection<Mutation> indexUpdates,
            TxTableState state,
            IndexMaintainer maintainer,
            boolean replyWrite,
            final PTable table) throws IOException, SQLException {
        state.applyMutation();
        Iterable<IndexUpdate> puts = getIndexUpserts(state, maintainer, replyWrite, table);
        boolean validPut = false;
        for (IndexUpdate put : puts) {
            if (put.isValid()) {
                indexUpdates.add(put.getUpdate());
                validPut = true;
            }
        }
        return validPut;
    }


    private static class TxTableState implements TableState {
        private final Mutation mutation;
        private final long currentTimestamp;
        private final List<Cell> pendingUpdates;
        private final Set<ColumnReference> indexedColumns;
        private final Map<ColumnReference, ImmutableBytesWritable> valueMap;
        
        private TxTableState(Set<ColumnReference> indexedColumns, long currentTimestamp, Mutation mutation) {
            this.currentTimestamp = currentTimestamp;
            this.indexedColumns = indexedColumns;
            this.mutation = mutation;
            int estimatedSize = indexedColumns.size();
            this.valueMap = Maps.newHashMapWithExpectedSize(estimatedSize);
            this.pendingUpdates = Lists.newArrayListWithExpectedSize(estimatedSize);
            try {
                CellScanner scanner = mutation.cellScanner();
                while (scanner.advance()) {
                    Cell cell = scanner.current();
                    pendingUpdates.add(PhoenixKeyValueUtil.maybeCopyCell(cell));
                }
            } catch (IOException e) {
                throw new RuntimeException(e); // Impossible
            }
        }

        public TxTableState(Set<ColumnReference> indexedColumns, long currentTimestamp, Mutation m, ColumnReference emptyColRef, Result r) {
            this(indexedColumns, currentTimestamp, m);

            for (ColumnReference ref : indexedColumns) {
                Cell cell = r.getColumnLatestCell(ref.getFamily(), ref.getQualifier());
                if (cell != null) {
                    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                    ptr.set(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    valueMap.put(ref, ptr);
                }
            }
        }

        @Override
        public RegionCoprocessorEnvironment getEnvironment() {
            return null;
        }

        @Override
        public long getCurrentTimestamp() {
            return currentTimestamp;
        }

        @Override
        public byte[] getCurrentRowKey() {
            return mutation.getRow();
        }

        @Override
        public List<? extends IndexedColumnGroup> getIndexColumnHints() {
            return Collections.emptyList();
        }

        private void applyMutation() {
            for (Cell cell : pendingUpdates) {
                if (cell.getTypeByte() == KeyValue.Type.Delete.getCode() || cell.getTypeByte() == KeyValue.Type.DeleteColumn.getCode()) {
                    ColumnReference ref = new ColumnReference(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(), cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                    valueMap.remove(ref);
                } else if (cell.getTypeByte() == KeyValue.Type.DeleteFamily.getCode() || cell.getTypeByte() == KeyValue.Type.DeleteFamilyVersion.getCode()) {
                    for (ColumnReference ref : indexedColumns) {
                        if (ref.matchesFamily(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength())) {
                            valueMap.remove(ref);
                        }
                    }
                } else if (cell.getTypeByte() == KeyValue.Type.Put.getCode()){
                    ColumnReference ref = new ColumnReference(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(), cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                    if (indexedColumns.contains(ref)) {
                        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                        ptr.set(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                        valueMap.put(ref, ptr);
                    }
                } else {
                    throw new IllegalStateException("Unexpected mutation type for " + cell);
                }
            }
        }
        
        @Override
        public Collection<Cell> getPendingUpdate() {
            return pendingUpdates;
        }

        @Override
        public Pair<ValueGetter, IndexUpdate> getIndexUpdateState(Collection<? extends ColumnReference> indexedColumns, boolean ignoreNewerMutations, boolean returnNullScannerIfRowNotFound, IndexMetaData indexMetaData)
                throws IOException {
            // TODO: creating these objects over and over again is wasteful
            ColumnTracker tracker = new ColumnTracker(indexedColumns);
            ValueGetter getter = new ValueGetter() {

                @Override
                public ImmutableBytesWritable getLatestValue(ColumnReference ref, long ts) throws IOException {
                    return valueMap.get(ref);
                }

                @Override
                public byte[] getRowKey() {
                    return mutation.getRow();
                }
                
            };
            Pair<ValueGetter, IndexUpdate> pair = new Pair<ValueGetter, IndexUpdate>(getter, new IndexUpdate(tracker));
            return pair;
        }
    }
}
