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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import co.cask.tephra.Transaction;
import co.cask.tephra.Transaction.VisibilityLevel;
import co.cask.tephra.TxConstants;
import co.cask.tephra.hbase98.TransactionAwareHTable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.htrace.Span;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.apache.phoenix.compile.ScanRanges;
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
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.trace.TracingUtils;
import org.apache.phoenix.trace.util.NullSpan;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ServerUtil;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Key;

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
                this.writer.write(indexUpdates);
            }
        } catch (Throwable t) {
            String msg = "Failed to update index with entries:" + indexUpdates;
            LOG.error(msg, t);
            ServerUtil.throwIOException(msg, t);
        }
    }

    private Collection<Pair<Mutation, byte[]>> getIndexUpdates(RegionCoprocessorEnvironment env, PhoenixIndexMetaData indexMetaData, Iterator<Mutation> mutationIterator, byte[] txRollbackAttribute) throws IOException {
        ResultScanner currentScanner = null;
        ResultScanner previousScanner = null;
        TransactionAwareHTable txTable = null;
        // Collect up all mutations in batch
        Map<ImmutableBytesPtr, MultiMutation> mutations =
                new HashMap<ImmutableBytesPtr, MultiMutation>();
        while(mutationIterator.hasNext()) {
            Mutation m = mutationIterator.next();
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
        
        // Collect the set of mutable ColumnReferences so that we can first
        // run a scan to get the current state. We'll need this to delete
        // the existing index rows.
        Transaction tx = indexMetaData.getTransaction();
        assert(tx != null);
        List<IndexMaintainer> indexMaintainers = indexMetaData.getIndexMaintainers();
        Set<ColumnReference> mutableColumns = Sets.newHashSetWithExpectedSize(indexMaintainers.size() * 10);
        for (IndexMaintainer indexMaintainer : indexMaintainers) {
            // For transactional tables, we use an index maintainer
            // to aid in rollback if there's a KeyValue column in the index. The alternative would be
            // to hold on to all uncommitted index row keys (even ones already sent to HBase) on the
            // client side.
                mutableColumns.addAll(indexMaintainer.getAllColumns());
        }

        Collection<Pair<Mutation, byte[]>> indexUpdates = new ArrayList<Pair<Mutation, byte[]>>(mutations.size() * 2 * indexMaintainers.size());
        try {
            if (!mutableColumns.isEmpty()) {
                List<KeyRange> keys = Lists.newArrayListWithExpectedSize(mutations.size());
                for (ImmutableBytesPtr ptr : mutations.keySet()) {
                    keys.add(PVarbinary.INSTANCE.getKeyRange(ptr.copyBytesIfNecessary()));
                }
                Scan scan = new Scan();
                // Project all mutable columns
                for (ColumnReference ref : mutableColumns) {
                    scan.addColumn(ref.getFamily(), ref.getQualifier());
                }
                // Project empty key value column
                scan.addColumn(indexMaintainers.get(0).getDataEmptyKeyValueCF(), QueryConstants.EMPTY_COLUMN_BYTES);
                ScanRanges scanRanges = ScanRanges.create(SchemaUtil.VAR_BINARY_SCHEMA, Collections.singletonList(keys), ScanUtil.SINGLE_COLUMN_SLOT_SPAN, KeyRange.EVERYTHING_RANGE, null, true, -1);
                scanRanges.initializeScan(scan);
                scan.setFilter(scanRanges.getSkipScanFilter());
                TableName tableName = env.getRegion().getRegionInfo().getTable();
                HTableInterface htable = env.getTable(tableName);
                txTable = new TransactionAwareHTable(htable);
                txTable.startTx(tx);
                currentScanner = txTable.getScanner(scan);
                if (txRollbackAttribute!=null) {
	                tx.setVisibility(VisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT);
	                previousScanner = txTable.getScanner(scan);
                }
            }
            // In case of rollback we have to do two scans, one with VisibilityLevel.SNAPSHOT to see the current state of the row 
            // and another with VisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT to see the previous state of the row
            // so that we can rollback a previous delete + put 
            processScanner(env, indexMetaData, txRollbackAttribute, previousScanner, mutations, tx, mutableColumns, indexUpdates, false);
            processScanner(env, indexMetaData, txRollbackAttribute, currentScanner, mutations, tx, mutableColumns, indexUpdates, true);
            for (Mutation m : mutations.values()) {
            	long timestamp = getTimestamp(txRollbackAttribute, tx.getWritePointer(), m);
            	TxTableState state = new TxTableState(env, mutableColumns, indexMetaData.getAttributes(), timestamp, m);
            	// if we did not generate valid put, we might have to generate a delete
                if (!generatePuts(indexMetaData, indexUpdates, state)) {
                	generateDeletes(indexMetaData, indexUpdates, txRollbackAttribute, state);
                }
            }
        } finally {
            if (txTable != null) txTable.close();
        }
        
        return indexUpdates;
    }

	private void processScanner(RegionCoprocessorEnvironment env,
			PhoenixIndexMetaData indexMetaData, byte[] txRollbackAttribute,
			ResultScanner scanner,
			Map<ImmutableBytesPtr, MultiMutation> mutations, Transaction tx,
			Set<ColumnReference> mutableColumns,
			Collection<Pair<Mutation, byte[]>> indexUpdates, boolean removeMutation) throws IOException {
		if (scanner != null) {
		    Result result;
		    ColumnReference emptyColRef = new ColumnReference(indexMetaData.getIndexMaintainers().get(0).getDataEmptyKeyValueCF(), QueryConstants.EMPTY_COLUMN_BYTES);
		    while ((result = scanner.next()) != null) {
		        Mutation m = removeMutation ? mutations.remove(new ImmutableBytesPtr(result.getRow())) : mutations.get(new ImmutableBytesPtr(result.getRow()));
		        long timestamp = getTimestamp(txRollbackAttribute, tx.getWritePointer(), m);
		        TxTableState state = new TxTableState(env, mutableColumns, indexMetaData.getAttributes(), timestamp, m, emptyColRef, result);
		        generateDeletes(indexMetaData, indexUpdates, txRollbackAttribute, state);
		        generatePuts(indexMetaData, indexUpdates, state);
		    }
		}
	}

	private long getTimestamp(byte[] txRollbackAttribute, long txnWritePointer, Mutation m) {
		if (txRollbackAttribute==null) {
			return txnWritePointer;
		}
		// if this is a rollback generate mutations with the same timestamp as the data row mutation as the timestamp might be 
        // different from the current txn write pointer because of check points
		long mutationTimestamp = txnWritePointer;
		for (Entry<byte[], List<Cell>> entry : m.getFamilyCellMap().entrySet()) {
			mutationTimestamp = entry.getValue().get(0).getTimestamp();
			break;
		}
		return mutationTimestamp;
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

	boolean generatePuts(
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
            /*
            Cell cell = r.getColumnLatestCell(emptyColRef.getFamily(), emptyColRef.getQualifier());
            if (cell != null) {
                ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                ptr.set(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                valueMap.put(emptyColRef, ptr);
            }
            */
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

        public void applyMutation() {
            /*if (mutation instanceof Delete) {
                valueMap.clear();
            } else */ {
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
        }
        
        @Override
        public Collection<KeyValue> getPendingUpdate() {
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
                    return mutation.getRow();
                }
                
            };
            Pair<ValueGetter, IndexUpdate> pair = new Pair<ValueGetter, IndexUpdate>(getter, new IndexUpdate(tracker));
            return pair;
        }
    }
}
