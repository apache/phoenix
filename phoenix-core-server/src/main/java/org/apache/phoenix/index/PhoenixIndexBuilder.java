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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.coprocessor.generated.PTableProtos;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.ReplayWrite;
import org.apache.phoenix.exception.DataExceedsCapacityException;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.expression.visitor.StatelessTraverseAllExpressionVisitor;
import org.apache.phoenix.hbase.index.covered.IndexMetaData;
import org.apache.phoenix.hbase.index.covered.NonTxIndexBuilder;
import org.apache.phoenix.hbase.index.util.GenericKeyValueBuilder;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PRow;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.tuple.MultiKeyValueTuple;
import org.apache.phoenix.util.ByteUtil;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

/**
 * Index builder for covered-columns index that ties into phoenix for faster use.
 */
public class PhoenixIndexBuilder extends NonTxIndexBuilder {
    private PhoenixIndexMetaDataBuilder indexMetaDataBuilder;
    
    @Override
    public void setup(RegionCoprocessorEnvironment env) throws IOException {
        super.setup(env);
        this.indexMetaDataBuilder = new PhoenixIndexMetaDataBuilder(env);
    }
    
    private static List<Cell> flattenCells(Mutation m, int estimatedSize) throws IOException {
        List<Cell> flattenedCells = Lists.newArrayListWithExpectedSize(estimatedSize);
        flattenCells(m, flattenedCells);
        return flattenedCells;
    }
    
    private static void flattenCells(Mutation m, List<Cell> flattenedCells) throws IOException {
        for (List<Cell> cells : m.getFamilyCellMap().values()) {
            flattenedCells.addAll(cells);
        }
    }
    
    @Override
    public PhoenixIndexMetaData getIndexMetaData(MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
        return indexMetaDataBuilder.getIndexMetaData(miniBatchOp);
    }

    protected PhoenixIndexCodec getCodec() {
        return (PhoenixIndexCodec)codec;
    }

    @Override
    public void batchStarted(MiniBatchOperationInProgress<Mutation> miniBatchOp, IndexMetaData context) throws IOException {
    }
    
    @Override
    public boolean isAtomicOp(Mutation m) {
        return m.getAttribute(PhoenixIndexBuilderHelper.ATOMIC_OP_ATTRIB) != null;
    }

    private static void transferCells(Mutation source, Mutation target) {
        target.getFamilyCellMap().putAll(source.getFamilyCellMap());
    }
    private static void transferAttributes(Mutation source, Mutation target) {
        for (Map.Entry<String, byte[]> entry : source.getAttributesMap().entrySet()) {
            target.setAttribute(entry.getKey(), entry.getValue());
        }
    }
    private static List<Mutation> convertIncrementToPutInSingletonList(Increment inc) {
        byte[] rowKey = inc.getRow();
        Put put = new Put(rowKey);
        transferCells(inc, put);
        transferAttributes(inc, put);
        return Collections.<Mutation>singletonList(put);
    }
    
    @Override
    public List<Mutation> executeAtomicOp(Increment inc) throws IOException {
        byte[] opBytes = inc.getAttribute(PhoenixIndexBuilderHelper.ATOMIC_OP_ATTRIB);
        if (opBytes == null) { // Unexpected
            return null;
        }
        inc.setAttribute(PhoenixIndexBuilderHelper.ATOMIC_OP_ATTRIB, null);
        Put put = null;
        Delete delete = null;
        // We cannot neither use the time stamp in the Increment to set the Get time range
        // nor set the Put/Delete time stamp and have this be atomic as HBase does not
        // handle that. Though we disallow using ON DUPLICATE KEY clause when the
        // CURRENT_SCN is set, we still may have a time stamp set as of when the table
        // was resolved on the client side. We need to ignore this as well due to limitations
        // in HBase, but this isn't too bad as the time will be very close the the current
        // time anyway.
        long ts = HConstants.LATEST_TIMESTAMP;
        byte[] rowKey = inc.getRow();
        final Get get = new Get(rowKey);
        if (PhoenixIndexBuilderHelper.isDupKeyIgnore(opBytes)) {
            get.setFilter(new FirstKeyOnlyFilter());
            try (RegionScanner scanner = this.env.getRegion().getScanner(new Scan(get))) {
                List<Cell> cells = new ArrayList<>();
                scanner.next(cells);
                return cells.isEmpty()
                        ? convertIncrementToPutInSingletonList(inc)
                        : Collections.<Mutation>emptyList();
            }
        }
        ByteArrayInputStream stream = new ByteArrayInputStream(opBytes);
        DataInputStream input = new DataInputStream(stream);
        boolean skipFirstOp = input.readBoolean();
        short repeat = input.readShort();
        final int[] estimatedSizeHolder = {0};
        List<Pair<PTable, List<Expression>>> operations = Lists.newArrayListWithExpectedSize(3);
        while (true) {
            ExpressionVisitor<Void> visitor = new StatelessTraverseAllExpressionVisitor<Void>() {
                @Override
                public Void visit(KeyValueColumnExpression expression) {
                    get.addColumn(expression.getColumnFamily(), expression.getColumnQualifier());
                    estimatedSizeHolder[0]++;
                    return null;
                }
            };
            try {
                int nExpressions = WritableUtils.readVInt(input);
                List<Expression>expressions = Lists.newArrayListWithExpectedSize(nExpressions);
                for (int i = 0; i < nExpressions; i++) {
                    Expression expression = ExpressionType.values()[WritableUtils.readVInt(input)].newInstance();
                    expression.readFields(input);
                    expressions.add(expression);
                    expression.accept(visitor);                    
                }
                PTableProtos.PTable tableProto = PTableProtos.PTable.parseDelimitedFrom(input);
                PTable table = PTableImpl.createFromProto(tableProto);
                operations.add(new Pair<>(table, expressions));
            } catch (EOFException e) {
                break;
            }
        }
        int estimatedSize = estimatedSizeHolder[0];
        if (get.getFamilyMap().isEmpty()) {
            get.setFilter(new FirstKeyOnlyFilter());
        }
        MultiKeyValueTuple tuple;
        List<Cell> flattenedCells = null;
        List<Cell>cells = ((HRegion)this.env.getRegion()).get(get, false);
        if (cells.isEmpty()) {
            if (skipFirstOp) {
                if (operations.size() <= 1 && repeat <= 1) {
                    return convertIncrementToPutInSingletonList(inc);
                }
                repeat--; // Skip first operation (if first wasn't ON DUPLICATE KEY IGNORE)
            }
            // Base current state off of new row
            flattenedCells = flattenCells(inc, estimatedSize);
            tuple = new MultiKeyValueTuple(flattenedCells);
        } else {
            // Base current state off of existing row
            tuple = new MultiKeyValueTuple(cells);
        }
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        for (int opIndex = 0; opIndex < operations.size(); opIndex++) {
            Pair<PTable, List<Expression>> operation = operations.get(opIndex);
            PTable table = operation.getFirst();
            List<Expression> expressions = operation.getSecond();
            for (int j = 0; j < repeat; j++) { // repeater loop
                ptr.set(rowKey);
                // Sort the list of cells (if they've been flattened in which case they're not necessarily
                // ordered correctly). We only need the list sorted if the expressions are going to be
                // executed, not when the outer loop is exited. Hence we do it here, at the top of the loop.
                if (flattenedCells != null) {
                    Collections.sort(flattenedCells,CellComparatorImpl.COMPARATOR);
                }
                PRow row = table.newRow(GenericKeyValueBuilder.INSTANCE, ts, ptr, false);
                int adjust = table.getBucketNum() == null ? 1 : 2;
                for (int i = 0; i < expressions.size(); i++) {
                    Expression expression = expressions.get(i);
                    ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
                    expression.evaluate(tuple, ptr);
                    PColumn column = table.getColumns().get(i + adjust);
                    Object value = expression.getDataType().toObject(ptr, column.getSortOrder());
                    // We are guaranteed that the two column will have the
                    // same type.
                    if (!column.getDataType().isSizeCompatible(ptr, value, column.getDataType(),
                            expression.getSortOrder(), expression.getMaxLength(), expression.getScale(),
                            column.getMaxLength(), column.getScale())) {
                        throw new DataExceedsCapacityException(column.getDataType(), column.getMaxLength(),
                                column.getScale(), column.getName().getString());
                    }
                    column.getDataType().coerceBytes(ptr, value, expression.getDataType(), expression.getMaxLength(),
                        expression.getScale(), expression.getSortOrder(),column.getMaxLength(), column.getScale(),
                        column.getSortOrder(), table.rowKeyOrderOptimizable());
                    byte[] bytes = ByteUtil.copyKeyBytesIfNecessary(ptr);
                    row.setValue(column, bytes);
                }
                flattenedCells = Lists.newArrayListWithExpectedSize(estimatedSize);
                List<Mutation> mutations = row.toRowMutations();
                for (Mutation source : mutations) {
                    flattenCells(source, flattenedCells);
                }
                tuple.setKeyValues(flattenedCells);
            }
            // Repeat only applies to first statement
            repeat = 1;
        }
        
        List<Mutation> mutations = Lists.newArrayListWithExpectedSize(2);
        for (int i = 0; i < tuple.size(); i++) {
            Cell cell = tuple.getValue(i);
            if (cell.getType() == Cell.Type.Put) {
                if (put == null) {
                    put = new Put(rowKey);
                    transferAttributes(inc, put);
                    mutations.add(put);
                }
                put.add(cell);
            } else {
                if (delete == null) {
                    delete = new Delete(rowKey);
                    transferAttributes(inc, delete);
                    mutations.add(delete);
                }
                delete.add(cell);
            }
        }
        return mutations;
    }

    @Override
    public ReplayWrite getReplayWrite(Mutation m) {
        return PhoenixIndexMetaData.getReplayWrite(m.getAttributesMap());
    }
    
}