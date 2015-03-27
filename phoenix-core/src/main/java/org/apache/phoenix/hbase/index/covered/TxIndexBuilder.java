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
package org.apache.phoenix.hbase.index.covered;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import co.cask.tephra.Transaction;
import co.cask.tephra.hbase98.TransactionAwareHTable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.hbase.index.ValueGetter;
import org.apache.phoenix.hbase.index.builder.BaseIndexBuilder;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.covered.update.ColumnTracker;
import org.apache.phoenix.hbase.index.covered.update.IndexedColumnGroup;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class TxIndexBuilder extends BaseIndexBuilder {
    private static final Log LOG = LogFactory.getLog(TxIndexBuilder.class);
    public static final String TRANSACTION = "TRANSACTION";
    private TransactionAwareHTable txTable;
    
    @Override
    public void setup(RegionCoprocessorEnvironment env) throws IOException {
        super.setup(env);
        HTableInterface htable = env.getTable(env.getRegion().getRegionInfo().getTable());
        this.txTable = new TransactionAwareHTable(htable); // TODO: close?
    }

    @Override
    public void stop(String why) {
        try {
            if (this.txTable != null) txTable.close();
        } catch (IOException e) {
            LOG.warn("Unable to close txTable", e);
        } finally {
            super.stop(why);
        }
    }

    @Override
    public Collection<Pair<Mutation, byte[]>> getIndexUpdate(Mutation mutation) throws IOException {
        // get the index updates for this current batch
        TxTableState state = new TxTableState(mutation);
        codec.setContext(state, mutation);
        Transaction tx = (Transaction)state.getContext().get(TRANSACTION);
        state.setCurrentTransaction(tx);
        Collection<Pair<Mutation, byte[]>> indexUpdates = Lists.newArrayListWithExpectedSize(2);
        Iterable<IndexUpdate> deletes = codec.getIndexDeletes(state);
        for (IndexUpdate delete : deletes) {
            indexUpdates.add(new Pair<Mutation, byte[]>(delete.getUpdate(),delete.getTableName()));
        }
        state.addPendingUpdates(mutation);
        // TODO: state may need to maintain the old state with the new state super imposed
        // An alternate easier way would be to calculate the state after the data mutations
        // have been applied.
        Iterable<IndexUpdate> updates = codec.getIndexUpserts(state);
        for (IndexUpdate update : updates) {
            indexUpdates.add(new Pair<Mutation, byte[]>(update.getUpdate(),update.getTableName()));
        }
        return indexUpdates;
    }

    private class TxTableState implements TableState {
        private Put put;
        private Map<String, byte[]> attributes;
        private List<Cell> pendingUpdates = Lists.newArrayList();
        private Transaction transaction;
        private final Map<String,Object> context = Maps.newHashMap();
        
        public TxTableState(Mutation m) {
            this.put = new Put(m.getRow());
            this.attributes = m.getAttributesMap();
        }
        
        @Override
        public RegionCoprocessorEnvironment getEnvironment() {
            return env;
        }

        @Override
        public long getCurrentTimestamp() {
            return transaction.getReadPointer();
        }

        @Override
        public Map<String, byte[]> getUpdateAttributes() {
            return attributes;
        }

        @Override
        public byte[] getCurrentRowKey() {
            return put.getRow();
        }

        @Override
        public List<? extends IndexedColumnGroup> getIndexColumnHints() {
            return Collections.emptyList();
        }

        public void addPendingUpdate(Cell cell) throws IOException {
            put.add(cell);
        }
        
        public void addPendingUpdates(Mutation m) throws IOException {
            if (m instanceof Delete) {
                put.getFamilyCellMap().clear();
            } else {
                CellScanner scanner = m.cellScanner();
                while (scanner.advance()) {
                    Cell cell = scanner.current();
                    if (cell.getTypeByte() == KeyValue.Type.DeleteColumn.getCode()) {
                        byte[] family = CellUtil.cloneFamily(cell);
                        byte[] qualifier = CellUtil.cloneQualifier(cell);
                        put.add(family, qualifier, HConstants.EMPTY_BYTE_ARRAY);
                    } else if (cell.getTypeByte() == KeyValue.Type.DeleteFamily.getCode()) {
                        byte[] family = CellUtil.cloneFamily(cell);
                        put.getFamilyCellMap().remove(family);
                    } else {
                        put.add(cell);
                    }
                }
            }
        }
        
        @Override
        public Collection<Cell> getPendingUpdate() {
            return pendingUpdates;
        }

        public void setCurrentTransaction(Transaction tx) {
            this.transaction = tx;
        }

        @Override
        public Pair<ValueGetter, IndexUpdate> getIndexUpdateState(Collection<? extends ColumnReference> indexedColumns)
                throws IOException {
            ColumnTracker tracker = new ColumnTracker(indexedColumns);
            IndexUpdate indexUpdate = new IndexUpdate(tracker);
            final byte[] rowKey = getCurrentRowKey();
            if (!pendingUpdates.isEmpty()) {
                final Map<ColumnReference, ImmutableBytesPtr> valueMap = Maps.newHashMapWithExpectedSize(pendingUpdates
                        .size());
                for (Cell kv : pendingUpdates) {
                    // create new pointers to each part of the kv
                    ImmutableBytesPtr value = new ImmutableBytesPtr(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength());
                    valueMap.put(new ColumnReference(kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength(), kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength()), value);
                }
                ValueGetter getter = new ValueGetter() {
                    @Override
                    public ImmutableBytesWritable getLatestValue(ColumnReference ref) {
                        // TODO: from IndexMaintainer we return null if ref is empty key value. Needed?
                        return valueMap.get(ref);
                    }
                    @Override
                    public byte[] getRowKey() {
                        return rowKey;
                    }
                };
                return new Pair<ValueGetter, IndexUpdate>(getter, indexUpdate);
            }
            // Establish initial state of table by finding the old values
            // We'll apply the Mutation to this next
            Get get = new Get(rowKey);
            get.setMaxVersions();
            for (ColumnReference ref : indexedColumns) {
                get.addColumn(ref.getFamily(), ref.getQualifier());
            }
            txTable.startTx(transaction);
            final Result result = txTable.get(get);
            ValueGetter getter = new ValueGetter() {

                @Override
                public ImmutableBytesWritable getLatestValue(ColumnReference ref) throws IOException {
                    Cell cell = result.getColumnLatestCell(ref.getFamily(), ref.getQualifier());
                    if (cell == null) {
                        return null;
                    }
                    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                    ptr.set(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    return ptr;
                }

                @Override
                public byte[] getRowKey() {
                    return rowKey;
                }
                
            };
            for (ColumnReference ref : indexedColumns) {
                Cell cell = result.getColumnLatestCell(ref.getFamily(), ref.getQualifier());
                if (cell != null) {
                    addPendingUpdate(cell);
                }
            }
            return new Pair<ValueGetter, IndexUpdate>(getter, indexUpdate);
        }

        @Override
        public Map<String, Object> getContext() {
            return context;
        }
        
    }

    @Override
    protected boolean useRawScanToPrimeBlockCache() {
        return true;
    }
}
