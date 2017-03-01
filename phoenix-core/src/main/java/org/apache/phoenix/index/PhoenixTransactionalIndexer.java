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
package org.apache.phoenix.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.htrace.Span;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.hbase.index.MultiMutation;
import org.apache.phoenix.hbase.index.ValueGetter;
import org.apache.phoenix.hbase.index.covered.IndexUpdate;
import org.apache.phoenix.hbase.index.covered.TableState;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.covered.update.ColumnTracker;
import org.apache.phoenix.hbase.index.covered.update.IndexedColumnGroup;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.write.IndexWriter;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.trace.TracingUtils;
import org.apache.phoenix.trace.util.NullSpan;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ServerUtil;
import org.apache.phoenix.util.TransactionUtil;
import org.apache.tephra.Transaction;
import org.apache.tephra.Transaction.VisibilityLevel;
import org.apache.tephra.TxConstants;
import org.apache.tephra.hbase.TransactionAwareHTable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;

/**
 * Do all the work of managing index updates for a transactional table from a single coprocessor. Since the transaction
 * manager essentially time orders writes through conflict detection, the logic to maintain a secondary index is quite a
 * bit simpler than the non transactional case. For example, there's no need to muck with the WAL, as failure scenarios
 * are handled by aborting the transaction.
 */
public class PhoenixTransactionalIndexer extends BaseRegionObserver {

    private static final Log LOG = LogFactory.getLog(PhoenixTransactionalIndexer.class);

    private PhoenixIndexCodec codec;
    private IndexWriter writer;
    private boolean stopped;

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        final RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment)e;
        String serverName = env.getRegionServerServices().getServerName().getServerName();
        codec = new PhoenixIndexCodec();
        codec.initialize(env);

        // setup the actual index writer
        this.writer = new IndexWriter(env, serverName + "-tx-index-writer");
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        if (this.stopped) { return; }
        this.stopped = true;
        String msg = "TxIndexer is being stopped";
        this.writer.stop(msg);
    }

    private static Iterator<Mutation> getMutationIterator(final MiniBatchOperationInProgress<Mutation> miniBatchOp) {
        return new Iterator<Mutation>() {
            private int i = 0;
            
            @Override
            public boolean hasNext() {
                return i < miniBatchOp.size();
            }

            @Override
            public Mutation next() {
                return miniBatchOp.getOperation(i++);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
            
        };
    }
    @Override
    public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
            MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {

        Mutation m = miniBatchOp.getOperation(0);
        if (!codec.isEnabled(m)) {
            super.preBatchMutate(c, miniBatchOp);
            return;
        }

        Map<String,byte[]> updateAttributes = m.getAttributesMap();
        PhoenixIndexMetaData indexMetaData = new PhoenixIndexMetaData(c.getEnvironment(),updateAttributes);
        byte[] txRollbackAttribute = m.getAttribute(TxConstants.TX_ROLLBACK_ATTRIBUTE_KEY);
        Collection<Pair<Mutation, byte[]>> indexUpdates = null;
        // get the current span, or just use a null-span to avoid a bunch of if statements
        try (TraceScope scope = Trace.startSpan("Starting to build index updates")) {
            Span current = scope.getSpan();
            if (current == null) {
                current = NullSpan.INSTANCE;
            }

            // get the index updates for all elements in this batch
            indexUpdates = getIndexUpdates(c.getEnvironment(), indexMetaData, getMutationIterator(miniBatchOp), txRollbackAttribute);

            current.addTimelineAnnotation("Built index updates, doing preStep");
            TracingUtils.addAnnotation(current, "index update count", indexUpdates.size());

            // no index updates, so we are done
            if (!indexUpdates.isEmpty()) {
                this.writer.write(indexUpdates, true);
            }
        } catch (Throwable t) {
            String msg = "Failed to update index with entries:" + indexUpdates;
            LOG.error(msg, t);
            ServerUtil.throwIOException(msg, t);
        }
    }

    public static void addMutation(Map<ImmutableBytesPtr, MultiMutation> mutations, ImmutableBytesPtr row, Mutation m) {
        MultiMutation stored = mutations.get(row);
        // we haven't seen this row before, so add it
        if (stored == null) {
            stored = new MultiMutation(row);
            mutations.put(row, stored);
        }
        stored.addAll(m);
    }
    
    private Collection<Pair<Mutation, byte[]>> getIndexUpdates(RegionCoprocessorEnvironment env, PhoenixIndexMetaData indexMetaData, Iterator<Mutation> mutationIterator, byte[] txRollbackAttribute) throws IOException {
        Transaction tx = indexMetaData.getTransaction();
        if (tx == null) {
            throw new NullPointerException("Expected to find transaction in metadata for " + env.getRegionInfo().getTable().getNameAsString());
        }
        boolean isRollback = txRollbackAttribute!=null;
        boolean isImmutable = indexMetaData.isImmutableRows();
        ResultScanner currentScanner = null;
        TransactionAwareHTable txTable = null;
        // Collect up all mutations in batch
        Map<ImmutableBytesPtr, MultiMutation> mutations =
                new HashMap<ImmutableBytesPtr, MultiMutation>();
        Map<ImmutableBytesPtr, MultiMutation> findPriorValueMutations;
        if (isImmutable && !isRollback) {
            findPriorValueMutations = new HashMap<ImmutableBytesPtr, MultiMutation>();
        } else {
            findPriorValueMutations = mutations;
        }
        while(mutationIterator.hasNext()) {
            Mutation m = mutationIterator.next();
            // add the mutation to the batch set
            ImmutableBytesPtr row = new ImmutableBytesPtr(m.getRow());
            if (mutations != findPriorValueMutations && isDeleteMutation(m)) {
                addMutation(findPriorValueMutations, row, m);
            }
            addMutation(mutations, row, m);
        }
        
        // Collect the set of mutable ColumnReferences so that we can first
        // run a scan to get the current state. We'll need this to delete
        // the existing index rows.
        List<IndexMaintainer> indexMaintainers = indexMetaData.getIndexMaintainers();
        int estimatedSize = indexMaintainers.size() * 10;
        Set<ColumnReference> mutableColumns = Sets.newHashSetWithExpectedSize(estimatedSize);
        for (IndexMaintainer indexMaintainer : indexMaintainers) {
            // For transactional tables, we use an index maintainer
            // to aid in rollback if there's a KeyValue column in the index. The alternative would be
            // to hold on to all uncommitted index row keys (even ones already sent to HBase) on the
            // client side.
            Set<ColumnReference> allColumns = indexMaintainer.getAllColumns();
            mutableColumns.addAll(allColumns);
        }

        Collection<Pair<Mutation, byte[]>> indexUpdates = new ArrayList<Pair<Mutation, byte[]>>(mutations.size() * 2 * indexMaintainers.size());
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
                byte[] emptyKeyValueQualifier = indexMaintainers.get(0).getEmptyKeyValueQualifier();
                
                // Project empty key value column
                scan.addColumn(indexMaintainers.get(0).getDataEmptyKeyValueCF(), emptyKeyValueQualifier);
                ScanRanges scanRanges = ScanRanges.create(SchemaUtil.VAR_BINARY_SCHEMA, Collections.singletonList(keys), ScanUtil.SINGLE_COLUMN_SLOT_SPAN, KeyRange.EVERYTHING_RANGE, null, true, -1);
                scanRanges.initializeScan(scan);
                TableName tableName = env.getRegion().getRegionInfo().getTable();
                HTableInterface htable = env.getTable(tableName);
                txTable = new TransactionAwareHTable(htable);
                txTable.startTx(tx);
                // For rollback, we need to see all versions, including
                // the last committed version as there may be multiple
                // checkpointed versions.
                SkipScanFilter filter = scanRanges.getSkipScanFilter();
                if (isRollback) {
                    filter = new SkipScanFilter(filter,true);
                    tx.setVisibility(VisibilityLevel.SNAPSHOT_ALL);
                }
                scan.setFilter(filter);
                currentScanner = txTable.getScanner(scan);
            }
            if (isRollback) {
                processRollback(env, indexMetaData, txRollbackAttribute, currentScanner, tx, mutableColumns, indexUpdates, mutations);
            } else {
                processMutation(env, indexMetaData, txRollbackAttribute, currentScanner, tx, mutableColumns, indexUpdates, mutations, findPriorValueMutations);
            }
        } finally {
            if (txTable != null) txTable.close();
        }
        
        return indexUpdates;
    }

    private static boolean isDeleteMutation(Mutation m) {
        for (Map.Entry<byte[],List<Cell>> cellMap : m.getFamilyCellMap().entrySet()) {
            for (Cell cell : cellMap.getValue()) {
                if (cell.getTypeByte() != KeyValue.Type.Put.getCode() || TransactionUtil.isDelete(cell)) {
                    return true;
                }
            }
        }
        return false;
    }

    private void processMutation(RegionCoprocessorEnvironment env,
            PhoenixIndexMetaData indexMetaData, byte[] txRollbackAttribute,
            ResultScanner scanner,
            Transaction tx, 
            Set<ColumnReference> upsertColumns, 
            Collection<Pair<Mutation, byte[]>> indexUpdates,
            Map<ImmutableBytesPtr, MultiMutation> mutations,
            Map<ImmutableBytesPtr, MultiMutation> mutationsToFindPreviousValue) throws IOException {
        if (scanner != null) {
            Result result;
            ColumnReference emptyColRef = new ColumnReference(indexMetaData.getIndexMaintainers().get(0)
                    .getDataEmptyKeyValueCF(), indexMetaData.getIndexMaintainers().get(0).getEmptyKeyValueQualifier());
            // Process existing data table rows by removing the old index row and adding the new index row
            while ((result = scanner.next()) != null) {
                Mutation m = mutationsToFindPreviousValue.remove(new ImmutableBytesPtr(result.getRow()));
                TxTableState state = new TxTableState(env, upsertColumns, indexMetaData.getAttributes(), tx.getWritePointer(), m, emptyColRef, result);
                generateDeletes(indexMetaData, indexUpdates, txRollbackAttribute, state);
                generatePuts(indexMetaData, indexUpdates, state);
            }
        }
        // Process new data table by adding new index rows
        for (Mutation m : mutations.values()) {
            TxTableState state = new TxTableState(env, upsertColumns, indexMetaData.getAttributes(), tx.getWritePointer(), m);
            generatePuts(indexMetaData, indexUpdates, state);
        }
    }

    private void processRollback(RegionCoprocessorEnvironment env,
            PhoenixIndexMetaData indexMetaData, byte[] txRollbackAttribute,
            ResultScanner scanner,
            Transaction tx, Set<ColumnReference> mutableColumns,
            Collection<Pair<Mutation, byte[]>> indexUpdates,
            Map<ImmutableBytesPtr, MultiMutation> mutations) throws IOException {
        if (scanner != null) {
            Result result;
            // Loop through last committed row state plus all new rows associated with current transaction
            // to generate point delete markers for all index rows that were added. We don't have Tephra
            // manage index rows in change sets because we don't want to be hit with the additional
            // memory hit and do not need to do conflict detection on index rows.
            ColumnReference emptyColRef = new ColumnReference(indexMetaData.getIndexMaintainers().get(0).getDataEmptyKeyValueCF(), indexMetaData.getIndexMaintainers().get(0).getEmptyKeyValueQualifier());
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
                long readPtr = tx.getReadPointer();
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
                        } while (++i < nCells && (cell=cells.get(i)).getTimestamp() == writePtr);
                    } while (i < nCells && cell.getTimestamp() <= readPtr);
                    
                    // Generate point delete markers for the prior row deletion of the old index value.
                    // The write timestamp is the next timestamp, not the current timestamp,
                    // as the earliest cells are the current values for the row (and we don't
                    // want to delete the current row).
                    if (oldResult != null) {
                        TxTableState state = new TxTableState(env, mutableColumns, indexMetaData.getAttributes(), writePtr, m, emptyColRef, oldResult);
                        generateDeletes(indexMetaData, indexUpdates, txRollbackAttribute, state);
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
                            TxTableState state = new TxTableState(env, mutableColumns, indexMetaData.getAttributes(), writePtr, m, emptyColRef, newResult);
                            generateDeletes(indexMetaData, indexUpdates, txRollbackAttribute, state);
                        }
                        oldResult = newResult;
                    } else {
                        oldResult = null;
                    }
                } while (i < nCells);
            }
        }
    }

    private void generateDeletes(PhoenixIndexMetaData indexMetaData,
            Collection<Pair<Mutation, byte[]>> indexUpdates,
            byte[] attribValue, TxTableState state) throws IOException {
        Iterable<IndexUpdate> deletes = codec.getIndexDeletes(state, indexMetaData);
        for (IndexUpdate delete : deletes) {
            if (delete.isValid()) {
                delete.getUpdate().setAttribute(TxConstants.TX_ROLLBACK_ATTRIBUTE_KEY, attribValue);
                indexUpdates.add(new Pair<Mutation, byte[]>(delete.getUpdate(),delete.getTableName()));
            }
        }
    }

    private boolean generatePuts(
            PhoenixIndexMetaData indexMetaData,
            Collection<Pair<Mutation, byte[]>> indexUpdates,
            TxTableState state)
            throws IOException {
        state.applyMutation();
        Iterable<IndexUpdate> puts = codec.getIndexUpserts(state, indexMetaData);
        boolean validPut = false;
        for (IndexUpdate put : puts) {
            if (put.isValid()) {
                indexUpdates.add(new Pair<Mutation, byte[]>(put.getUpdate(),put.getTableName()));
                validPut = true;
            }
        }
        return validPut;
    }


    private static class TxTableState implements TableState {
        private final Mutation mutation;
        private final long currentTimestamp;
        private final RegionCoprocessorEnvironment env;
        private final Map<String, byte[]> attributes;
        private final List<KeyValue> pendingUpdates;
        private final Set<ColumnReference> indexedColumns;
        private final Map<ColumnReference, ImmutableBytesWritable> valueMap;
        
        private TxTableState(RegionCoprocessorEnvironment env, Set<ColumnReference> indexedColumns, Map<String, byte[]> attributes, long currentTimestamp, Mutation mutation) {
            this.env = env;
            this.currentTimestamp = currentTimestamp;
            this.indexedColumns = indexedColumns;
            this.attributes = attributes;
            this.mutation = mutation;
            int estimatedSize = indexedColumns.size();
            this.valueMap = Maps.newHashMapWithExpectedSize(estimatedSize);
            this.pendingUpdates = Lists.newArrayListWithExpectedSize(estimatedSize);
            try {
                CellScanner scanner = mutation.cellScanner();
                while (scanner.advance()) {
                    Cell cell = scanner.current();
                    pendingUpdates.add(KeyValueUtil.ensureKeyValue(cell));
                }
            } catch (IOException e) {
                throw new RuntimeException(e); // Impossible
            }
        }
        
        public TxTableState(RegionCoprocessorEnvironment env, Set<ColumnReference> indexedColumns, Map<String, byte[]> attributes, long currentTimestamp, Mutation m, ColumnReference emptyColRef, Result r) {
            this(env, indexedColumns, attributes, currentTimestamp, m);

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
            return env;
        }

        @Override
        public long getCurrentTimestamp() {
            return currentTimestamp;
        }

        @Override
        public Map<String, byte[]> getUpdateAttributes() {
            return attributes;
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
        public Collection<KeyValue> getPendingUpdate() {
            return pendingUpdates;
        }

        @Override
        public Pair<ValueGetter, IndexUpdate> getIndexUpdateState(Collection<? extends ColumnReference> indexedColumns, boolean ignoreNewerMutations, boolean returnNullScannerIfRowNotFound)
                throws IOException {
            // TODO: creating these objects over and over again is wasteful
            ColumnTracker tracker = new ColumnTracker(indexedColumns);
            ValueGetter getter = new ValueGetter() {

                @Override
                public ImmutableBytesWritable getLatestValue(ColumnReference ref) throws IOException {
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
