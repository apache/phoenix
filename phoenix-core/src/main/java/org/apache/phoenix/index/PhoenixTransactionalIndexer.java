/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.phoenix.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import co.cask.tephra.Transaction;
import co.cask.tephra.hbase98.TransactionAwareHTable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
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
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.hbase.index.MultiMutation;
import org.apache.phoenix.hbase.index.ValueGetter;
import org.apache.phoenix.hbase.index.covered.IndexUpdate;
import org.apache.phoenix.hbase.index.covered.TableState;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.covered.update.ColumnTracker;
import org.apache.phoenix.hbase.index.covered.update.IndexedColumnGroup;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.IndexManagementUtil;
import org.apache.phoenix.hbase.index.write.IndexWriter;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.trace.TracingUtils;
import org.apache.phoenix.trace.util.NullSpan;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.cloudera.htrace.Span;
import org.cloudera.htrace.Trace;
import org.cloudera.htrace.TraceScope;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

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

    @Override
    public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
            MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {

        Mutation m = miniBatchOp.getOperation(0);
        if (!codec.isEnabled(m)) {
            super.preBatchMutate(c, miniBatchOp);
            return;
        }

        Collection<Pair<Mutation, byte[]>> indexUpdates = null;
        // get the current span, or just use a null-span to avoid a bunch of if statements
        try (TraceScope scope = Trace.startSpan("Starting to build index updates")) {
            Span current = scope.getSpan();
            if (current == null) {
                current = NullSpan.INSTANCE;
            }

            // get the index updates for all elements in this batch
            indexUpdates = getIndexUpdates(c.getEnvironment(), miniBatchOp);

            current.addTimelineAnnotation("Built index updates, doing preStep");
            TracingUtils.addAnnotation(current, "index update count", indexUpdates.size());

            // no index updates, so we are done
            if (!indexUpdates.isEmpty()) {
                this.writer.write(indexUpdates);
            }
        } catch (Throwable t) {
            LOG.error("Failed to update index with entries:" + indexUpdates, t);
            IndexManagementUtil.rethrowIndexingException(t);
        }
    }

    private Collection<Pair<Mutation, byte[]>> getIndexUpdates(RegionCoprocessorEnvironment env, MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
        // Collect the set of mutable ColumnReferences so that we can first
        // run a scan to get the current state. We'll need this to delete
        // the existing index rows.
        Map<String,byte[]> updateAttributes = miniBatchOp.getOperation(0).getAttributesMap();
        PhoenixIndexMetaData indexMetaData = new PhoenixIndexMetaData(env,updateAttributes);
        Transaction tx = indexMetaData.getTransaction();
        assert(tx != null);
        List<IndexMaintainer> indexMaintainers = indexMetaData.getIndexMaintainers();
        Set<ColumnReference> mutableColumns = Sets.newHashSetWithExpectedSize(indexMaintainers.size() * 10);
        for (IndexMaintainer indexMaintainer : indexMaintainers) {
            if (!indexMaintainer.isImmutableRows()) {
                mutableColumns.addAll(indexMaintainer.getAllColumns());
            }
        }
        ResultScanner scanner = null;
        TransactionAwareHTable txTable = null;
        
        // Collect up all mutations in batch
        Map<ImmutableBytesPtr, MultiMutation> mutations =
                new HashMap<ImmutableBytesPtr, MultiMutation>();
        for (int i = 0; i < miniBatchOp.size(); i++) {
            Mutation m = miniBatchOp.getOperation(i);
            // add the mutation to the batch set
            ImmutableBytesPtr row = new ImmutableBytesPtr(m.getRow());
            MultiMutation stored = mutations.get(row);
            // we haven't seen this row before, so add it
            if (stored == null) {
                stored = new MultiMutation(row);
                mutations.put(row, stored);
            }
            stored.addAll(m);
        }
        
        try {
            if (!mutableColumns.isEmpty()) {
                List<KeyRange> keys = Lists.newArrayListWithExpectedSize(mutations.size());
                for (ImmutableBytesPtr ptr : mutations.keySet()) {
                    keys.add(PVarbinary.INSTANCE.getKeyRange(ptr.copyBytesIfNecessary()));
                }
                Scan scan = new Scan();
                ScanRanges scanRanges = ScanRanges.create(SchemaUtil.VAR_BINARY_SCHEMA, Collections.singletonList(keys), ScanUtil.SINGLE_COLUMN_SLOT_SPAN);
                scanRanges.initializeScan(scan);
                scan.setFilter(scanRanges.getSkipScanFilter());
                TableName tableName = env.getRegion().getRegionInfo().getTable();
                HTableInterface htable = env.getTable(tableName);
                txTable = new TransactionAwareHTable(htable);
                txTable.startTx(tx);
                scanner = txTable.getScanner(scan);
            }
        } finally {
            if (txTable != null) txTable.close();
        }
        
        Collection<Pair<Mutation, byte[]>> indexUpdates = new ArrayList<Pair<Mutation, byte[]>>(mutations.size() * 2 * indexMaintainers.size());
        if (scanner == null) {
            for (Mutation m : mutations.values()) {
                TxTableState state = new TxTableState(env, mutableColumns, updateAttributes, tx.getWritePointer(), m);
                state.applyMutation(m);
                Iterable<IndexUpdate> updates = codec.getIndexUpserts(state, indexMetaData);
                for (IndexUpdate update : updates) {
                    indexUpdates.add(new Pair<Mutation, byte[]>(update.getUpdate(),update.getTableName()));
                }
            }
        } else {
            Result result;
            while ((result = scanner.next()) != null) {
                TxTableState state = new TxTableState(env, mutableColumns, updateAttributes, tx.getWritePointer(), result);
                Iterable<IndexUpdate> deletes = codec.getIndexDeletes(state, indexMetaData);
                for (IndexUpdate delete : deletes) {
                    indexUpdates.add(new Pair<Mutation, byte[]>(delete.getUpdate(),delete.getTableName()));
                }
                Mutation m = mutations.get(new ImmutableBytesPtr(result.getRow()));
                state.applyMutation(m);
                Iterable<IndexUpdate> updates = codec.getIndexUpserts(state, indexMetaData);
                for (IndexUpdate update : updates) {
                    indexUpdates.add(new Pair<Mutation, byte[]>(update.getUpdate(),update.getTableName()));
                }
            }
        }
        return indexUpdates;
    }


    private static class TxTableState implements TableState {
        private final byte[] rowKey;
        private final long currentTimestamp;
        private final RegionCoprocessorEnvironment env;
        private final Map<String, byte[]> attributes;
        private final List<Cell> pendingUpdates;
        private final Set<ColumnReference> indexedColumns;
        private final Map<ColumnReference, ImmutableBytesWritable> valueMap;
        
        private TxTableState(RegionCoprocessorEnvironment env, Set<ColumnReference> indexedColumns, Map<String, byte[]> attributes, long currentTimestamp, byte[] rowKey) {
            this.env = env;
            this.currentTimestamp = currentTimestamp;
            this.indexedColumns = indexedColumns;
            this.attributes = attributes;
            this.rowKey = rowKey;
            int estimatedSize = indexedColumns.size();
            this.valueMap = Maps.newHashMapWithExpectedSize(estimatedSize);
            this.pendingUpdates = Lists.newArrayListWithExpectedSize(estimatedSize);
        }
        
        public TxTableState(RegionCoprocessorEnvironment env, Set<ColumnReference> indexedColumns, Map<String, byte[]> attributes, long currentTimestamp, Mutation m) {
            this(env, indexedColumns, attributes, currentTimestamp, m.getRow());
            applyMutation(m);
        }
        
        public TxTableState(RegionCoprocessorEnvironment env, Set<ColumnReference> indexedColumns, Map<String, byte[]> attributes, long currentTimestamp, Result r) {
            this(env, indexedColumns, attributes, currentTimestamp, r.getRow());

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
            return rowKey;
        }

        @Override
        public List<? extends IndexedColumnGroup> getIndexColumnHints() {
            return Collections.emptyList();
        }

        public void applyMutation(Mutation m) {
            if (m instanceof Delete) {
                valueMap.clear();
            } else {
                CellScanner scanner = m.cellScanner();
                try {
                    while (scanner.advance()) {
                        Cell cell = scanner.current();
                        if (cell.getTypeByte() == KeyValue.Type.DeleteColumn.getCode()) {
                            ColumnReference ref = new ColumnReference(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(), cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                            valueMap.remove(ref);
                        } else if (cell.getTypeByte() == KeyValue.Type.DeleteFamily.getCode()) {
                            for (ColumnReference ref : indexedColumns) {
                                if (ref.matchesFamily(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength())) {
                                    valueMap.remove(ref);
                                }
                            }
                        } else {
                            ColumnReference ref = new ColumnReference(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(), cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                            ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                            ptr.set(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                            valueMap.put(ref, ptr);
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e); // Impossible
                }
            }
        }
        
        @Override
        public Collection<Cell> getPendingUpdate() {
            return pendingUpdates;
        }

        @Override
        public Pair<ValueGetter, IndexUpdate> getIndexUpdateState(Collection<? extends ColumnReference> indexedColumns)
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
                    return rowKey;
                }
                
            };
            Pair<ValueGetter, IndexUpdate> pair = new Pair<ValueGetter, IndexUpdate>(getter, new IndexUpdate(tracker));
            return pair;
        }
    }
}
