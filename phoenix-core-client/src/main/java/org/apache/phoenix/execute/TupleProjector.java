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

import static org.apache.phoenix.coprocessorclient.ScanRegionObserverConstants.WILDCARD_SCAN_INCLUDES_DYNAMIC_COLUMNS;
import static org.apache.phoenix.coprocessorclient.ScanRegionObserverConstants.DYN_COLS_METADATA_CELL_QUALIFIER;
import static org.apache.phoenix.query.QueryConstants.VALUE_COLUMN_FAMILY;
import static org.apache.phoenix.query.QueryConstants.VALUE_COLUMN_QUALIFIER;
import static org.apache.phoenix.schema.types.PDataType.TRUE_BYTES;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.compile.ColumnProjector;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.coprocessor.generated.PTableProtos;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.schema.KeyValueSchema;
import org.apache.phoenix.schema.KeyValueSchema.KeyValueSchemaBuilder;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.ProjectedColumn;
import org.apache.phoenix.schema.ValueBitSet;
import org.apache.phoenix.schema.tuple.BaseTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PhoenixKeyValueUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;

import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;

public class TupleProjector {    
    private static final String SCAN_PROJECTOR = "scanProjector";

    private final KeyValueSchema schema;
    private final Expression[] expressions;
    private ValueBitSet valueSet;
    private final ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    
    private static final byte[] OLD_VALUE_COLUMN_QUALIFIER = new byte[0];
    
    public TupleProjector(RowProjector rowProjector) {
        List<? extends ColumnProjector> columnProjectors = rowProjector.getColumnProjectors();
        int count = columnProjectors.size();
        KeyValueSchemaBuilder builder = new KeyValueSchemaBuilder(0);
        expressions = new Expression[count];
        for (int i = 0; i < count; i++) {
            Expression expression = columnProjectors.get(i).getExpression();
            builder.addField(expression);
            expressions[i] = expression;
        }
        schema = builder.build();
        valueSet = ValueBitSet.newInstance(schema);
    }

    public TupleProjector(Expression[] expressions) {
        this.expressions = expressions;
        KeyValueSchemaBuilder builder = new KeyValueSchemaBuilder(0);
        for (int i = 0; i < expressions.length; i++) {
            builder.addField(expressions[i]);
        }
        schema = builder.build();
        valueSet = ValueBitSet.newInstance(schema);
    }
    
    public TupleProjector(PTable projectedTable) throws SQLException {
        Preconditions.checkArgument(projectedTable.getType() == PTableType.PROJECTED);
    	List<PColumn> columns = projectedTable.getColumns();
    	this.expressions = new Expression[columns.size() - projectedTable.getPKColumns().size()];
    	KeyValueSchemaBuilder builder = new KeyValueSchemaBuilder(0);
    	int i = 0;
        for (PColumn column : columns) {
        	if (!SchemaUtil.isPKColumn(column)) {
        		builder.addField(column);
        		expressions[i++] = ((ProjectedColumn) column).getSourceColumnRef().newColumnExpression();
        	}
        }
        schema = builder.build();
        valueSet = ValueBitSet.newInstance(schema);
    }
    
    public TupleProjector(KeyValueSchema schema, Expression[] expressions) {
    	this.schema = schema;
    	this.expressions = expressions;
    	this.valueSet = ValueBitSet.newInstance(schema);
    }
    
    public void setValueBitSet(ValueBitSet bitSet) {
        this.valueSet = bitSet;
    }
    
    public static void serializeProjectorIntoScan(Scan scan, TupleProjector projector,
            boolean projectDynColsInWildcardQueries) {
        scan.setAttribute(SCAN_PROJECTOR, serializeProjectorIntoBytes(projector));
        if (projectDynColsInWildcardQueries) {
            scan.setAttribute(WILDCARD_SCAN_INCLUDES_DYNAMIC_COLUMNS, TRUE_BYTES);
        }
    }

    /**
     * Serialize the projector into a byte array
     * @param projector projector to serialize
     * @return byte array
     */
    private static byte[] serializeProjectorIntoBytes(TupleProjector projector) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            DataOutputStream output = new DataOutputStream(stream);
            projector.schema.write(output);
            int count = projector.expressions.length;
            WritableUtils.writeVInt(output, count);
            for (int i = 0; i < count; i++) {
                WritableUtils.writeVInt(output,
                        ExpressionType.valueOf(projector.expressions[i]).ordinal());
                projector.expressions[i].write(output);
            }
            return stream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    public static TupleProjector deserializeProjectorFromScan(Scan scan) {
        return deserializeProjectorFromBytes(scan.getAttribute(SCAN_PROJECTOR));
    }

    /**
     * Deserialize the byte array to form a projector
     * @param proj byte array to deserialize
     * @return projector
     */
    private static TupleProjector deserializeProjectorFromBytes(byte[] proj) {
        if (proj == null) {
            return null;
        }
        ByteArrayInputStream stream = new ByteArrayInputStream(proj);
        try {
            DataInputStream input = new DataInputStream(stream);
            KeyValueSchema schema = new KeyValueSchema();
            schema.readFields(input);
            int count = WritableUtils.readVInt(input);
            Expression[] expressions = new Expression[count];
            for (int i = 0; i < count; i++) {
                int ordinal = WritableUtils.readVInt(input);
                expressions[i] = ExpressionType.values()[ordinal].newInstance();
                expressions[i].readFields(input);
            }
            return new TupleProjector(schema, expressions);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Iterate over the list of cells returned from the scan and return a tuple projector for the
     * dynamic columns by parsing the metadata stored for the list of dynamic columns
     * @param result list of cells
     * @param dynCols list of dynamic columns to be populated
     * @param dynColCells list of cells corresponding to dynamic columns to be populated
     * @return The tuple projector corresponding to dynamic columns or null if there are no dynamic
     * columns to process
     * @throws InvalidProtocolBufferException Thrown if there is an error parsing byte[] to protobuf
     */
    public static TupleProjector getDynamicColumnsTupleProjector(List<Cell> result,
            List<PColumn> dynCols, List<Cell> dynColCells) throws InvalidProtocolBufferException {
        Set<Pair<ByteBuffer, ByteBuffer>> dynColCellQualifiers = new HashSet<>();
        populateDynColsFromResult(result, dynCols, dynColCellQualifiers);
        if (dynCols.isEmpty()) {
            return null;
        }
        populateDynamicColumnCells(result, dynColCellQualifiers, dynColCells);
        if (dynColCells.isEmpty()) {
            return null;
        }
        KeyValueSchema dynColsSchema = PhoenixRuntime.buildKeyValueSchema(dynCols);
        Expression[] expressions = new Expression[dynCols.size()];
        for (int i = 0; i < dynCols.size(); i++) {
            expressions[i] = new KeyValueColumnExpression(dynCols.get(i));
        }
        return new TupleProjector(dynColsSchema, expressions);
    }

    /**
     * Populate cells corresponding to dynamic columns
     * @param result list of cells
     * @param dynColCellQualifiers Set of <column family, column qualifier> pairs corresponding to
     *                             cells of dynamic columns
     * @param dynColCells Populated list of cells corresponding to dynamic columns
     */
    private static void populateDynamicColumnCells(List<Cell> result,
            Set<Pair<ByteBuffer, ByteBuffer>> dynColCellQualifiers, List<Cell> dynColCells) {
        for (Cell c : result) {
            Pair famQualPair = new Pair<>(ByteBuffer.wrap(CellUtil.cloneFamily(c)),
                    ByteBuffer.wrap(CellUtil.cloneQualifier(c)));
            if (dynColCellQualifiers.contains(famQualPair)) {
                dynColCells.add(c);
            }
        }
    }

    /**
     * Iterate over the list of cells and populate dynamic columns
     * @param result list of cells
     * @param dynCols Populated list of PColumns corresponding to dynamic columns
     * @param dynColCellQualifiers Populated set of <column family, column qualifier> pairs
     *                             for the cells in the list, which correspond to dynamic columns
     * @throws InvalidProtocolBufferException Thrown if there is an error parsing byte[] to protobuf
     */
    private static void populateDynColsFromResult(List<Cell> result, List<PColumn> dynCols,
            Set<Pair<ByteBuffer, ByteBuffer>> dynColCellQualifiers)
    throws InvalidProtocolBufferException {
        for (Cell c : result) {
            byte[] qual = CellUtil.cloneQualifier(c);
            byte[] fam = CellUtil.cloneFamily(c);
            int index = Bytes.indexOf(qual, DYN_COLS_METADATA_CELL_QUALIFIER);

            // Contains dynamic column metadata, so add it to the list of dynamic columns
            if (index != -1) {
                byte[] dynColMetaDataProto = CellUtil.cloneValue(c);
                dynCols.add(PColumnImpl.createFromProto(
                        PTableProtos.PColumn.parseFrom(dynColMetaDataProto)));
                // Add the <fam, qualifier> pair for the actual dynamic column. The column qualifier
                // of the dynamic column is got by parsing out the known bytes from the shadow cell
                // containing the metadata for that column i.e.
                // DYN_COLS_METADATA_CELL_QUALIFIER<actual column qualifier>
                byte[] dynColQual = Arrays.copyOfRange(qual,
                        index + DYN_COLS_METADATA_CELL_QUALIFIER.length, qual.length);
                dynColCellQualifiers.add(
                        new Pair<>(ByteBuffer.wrap(fam), ByteBuffer.wrap(dynColQual)));
            }
        }
    }
    
    public static class ProjectedValueTuple extends BaseTuple {
        ImmutableBytesWritable keyPtr = new ImmutableBytesWritable();
        long timestamp;
        ImmutableBytesWritable projectedValue = new ImmutableBytesWritable();
        int bitSetLen;
        Cell keyValue;

        public ProjectedValueTuple(Tuple keyBase, long timestamp, byte[] projectedValue, int valueOffset, int valueLength, int bitSetLen) {
            keyBase.getKey(this.keyPtr);
            this.timestamp = timestamp;
            this.projectedValue.set(projectedValue, valueOffset, valueLength);
            this.bitSetLen = bitSetLen;
        }

        public ProjectedValueTuple(byte[] keyBuffer, int keyOffset, int keyLength, long timestamp, byte[] projectedValue, int valueOffset, int valueLength, int bitSetLen) {
            this.keyPtr.set(keyBuffer, keyOffset, keyLength);
            this.timestamp = timestamp;
            this.projectedValue.set(projectedValue, valueOffset, valueLength);
            this.bitSetLen = bitSetLen;
        }
        
        public ImmutableBytesWritable getKeyPtr() {
            return keyPtr;
        }
        
        public long getTimestamp() {
            return timestamp;
        }
        
        public ImmutableBytesWritable getProjectedValue() {
            return projectedValue;
        }
        
        public int getBitSetLength() {
            return bitSetLen;
        }
        
        @Override
        public void getKey(ImmutableBytesWritable ptr) {
            ptr.set(keyPtr.get(), keyPtr.getOffset(), keyPtr.getLength());
        }

        @Override
        public Cell mergeWithDynColsListBytesAndGetValue(int index, byte[] dynColsList) {
            if (index != 0) {
                throw new IndexOutOfBoundsException(Integer.toString(index));
            }
            if (dynColsList == null || dynColsList.length == 0) {
                return getValue(VALUE_COLUMN_FAMILY, VALUE_COLUMN_QUALIFIER);
            }
            // We put the known reserved bytes before the serialized list of dynamic column
            // PColumns to easily parse out the column list on the client
            byte[] concatBytes = ByteUtil.concat(projectedValue.get(),
                    DYN_COLS_METADATA_CELL_QUALIFIER, dynColsList);
            ImmutableBytesWritable projectedValueWithDynColsListBytes =
                    new ImmutableBytesWritable(concatBytes);
            keyValue = PhoenixKeyValueUtil.newKeyValue(keyPtr.get(), keyPtr.getOffset(),
                    keyPtr.getLength(), VALUE_COLUMN_FAMILY, VALUE_COLUMN_QUALIFIER, timestamp,
                    projectedValueWithDynColsListBytes.get(),
                    projectedValueWithDynColsListBytes.getOffset(),
                    projectedValueWithDynColsListBytes.getLength());
            return keyValue;
        }

        @Override
        public Cell getValue(int index) {
            if (index != 0) {
                throw new IndexOutOfBoundsException(Integer.toString(index));
            }
            return getValue(VALUE_COLUMN_FAMILY, VALUE_COLUMN_QUALIFIER);
        }

        @Override
        public Cell getValue(byte[] family, byte[] qualifier) {
            if (keyValue == null) {
                keyValue = PhoenixKeyValueUtil.newKeyValue(keyPtr.get(), keyPtr.getOffset(), keyPtr.getLength(), 
                        VALUE_COLUMN_FAMILY, VALUE_COLUMN_QUALIFIER, timestamp, projectedValue.get(), projectedValue.getOffset(), projectedValue.getLength());
            }
            return keyValue;
        }

        @Override
        public boolean getValue(byte[] family, byte[] qualifier,
                ImmutableBytesWritable ptr) {
            ptr.set(projectedValue.get(), projectedValue.getOffset(), projectedValue.getLength());
            return true;
        }

        @Override
        public boolean isImmutable() {
            return true;
        }

        @Override
        public int size() {
            return 1;
        }
    }
    
    public static class OldProjectedValueTuple extends ProjectedValueTuple {

        public OldProjectedValueTuple(byte[] keyBuffer, int keyOffset, int keyLength, long timestamp,
                byte[] projectedValue, int valueOffset, int valueLength, int bitSetLen) {
            super(keyBuffer, keyOffset, keyLength, timestamp, projectedValue, valueOffset, valueLength, bitSetLen);
        }

        public OldProjectedValueTuple(Tuple keyBase, long timestamp, byte[] projectedValue, int valueOffset,
                int valueLength, int bitSetLen) {
            super(keyBase, timestamp, projectedValue, valueOffset, valueLength, bitSetLen);
        }

        @Override
        public Cell getValue(int index) {
            if (index != 0) { throw new IndexOutOfBoundsException(Integer.toString(index)); }
            return getValue(VALUE_COLUMN_FAMILY, OLD_VALUE_COLUMN_QUALIFIER);
        }

        @Override
        public Cell getValue(byte[] family, byte[] qualifier) {
            if (keyValue == null) {
                keyValue = PhoenixKeyValueUtil.newKeyValue(keyPtr.get(), keyPtr.getOffset(), keyPtr.getLength(),
                        VALUE_COLUMN_FAMILY, OLD_VALUE_COLUMN_QUALIFIER, timestamp, projectedValue.get(),
                        projectedValue.getOffset(), projectedValue.getLength());
            }
            return keyValue;
        }
        
    }
    
    public ProjectedValueTuple projectResults(Tuple tuple) {
    	byte[] bytesValue = schema.toBytes(tuple, getExpressions(), valueSet, ptr);
    	Cell base = tuple.getValue(0);
        return new ProjectedValueTuple(base.getRowArray(), base.getRowOffset(), base.getRowLength(), base.getTimestamp(), bytesValue, 0, bytesValue.length, valueSet.getEstimatedLength());
    }
    
    public ProjectedValueTuple projectResults(Tuple tuple, boolean useNewValueQualifier) {
        long maxTS = tuple.getValue(0).getTimestamp();
        int nCells = tuple.size();
        for (int i = 1; i < nCells; i++) {
            long ts = tuple.getValue(i).getTimestamp();
            if (ts > maxTS) {
                maxTS = ts;
            }
        }
        byte[] bytesValue = schema.toBytes(tuple, getExpressions(), valueSet, ptr);
        Cell base = tuple.getValue(0);
        if (useNewValueQualifier) {
            return new ProjectedValueTuple(base.getRowArray(), base.getRowOffset(), base.getRowLength(), maxTS, bytesValue, 0, bytesValue.length, valueSet.getEstimatedLength());
        } else {
            return new OldProjectedValueTuple(base.getRowArray(), base.getRowOffset(), base.getRowLength(), maxTS, bytesValue, 0, bytesValue.length, valueSet.getEstimatedLength());
        }
    }
    
    public static void decodeProjectedValue(Tuple tuple, ImmutableBytesWritable ptr) throws IOException {
        boolean b = tuple.getValue(VALUE_COLUMN_FAMILY, VALUE_COLUMN_QUALIFIER, ptr);
        if (!b) {
            // fall back to use the old value column qualifier for backward compatibility
            b = tuple.getValue(VALUE_COLUMN_FAMILY, OLD_VALUE_COLUMN_QUALIFIER, ptr);
        }
        if (!b) throw new IOException("Trying to decode a non-projected value.");
    }
    
    public static ProjectedValueTuple mergeProjectedValue(ProjectedValueTuple dest,
            ValueBitSet destBitSet, Tuple src, ValueBitSet srcBitSet, int offset,
            boolean useNewValueColumnQualifier) throws IOException {
    	ImmutableBytesWritable destValue = dest.getProjectedValue();
        int origDestBitSetLen = dest.getBitSetLength();
    	destBitSet.clear();
    	destBitSet.or(destValue, origDestBitSetLen);
    	ImmutableBytesWritable srcValue = null;
    	int srcValueLen = 0;
    	if (src != null) {
    	    srcValue = new ImmutableBytesWritable();
    	    decodeProjectedValue(src, srcValue);
    	    srcBitSet.clear();
    	    srcBitSet.or(srcValue);
    	    int origSrcBitSetLen = srcBitSet.getEstimatedLength();
    	    for (int i = 0; i <= srcBitSet.getMaxSetBit(); i++) {
    	        if (srcBitSet.get(i)) {
    	            destBitSet.set(offset + i);
    	        }
    	    }
    	    srcValueLen = srcValue.getLength() - origSrcBitSetLen;
    	}
    	int destBitSetLen = destBitSet.getEstimatedLength();
    	byte[] merged = new byte[destValue.getLength() - origDestBitSetLen + srcValueLen + destBitSetLen];
    	int o = Bytes.putBytes(merged, 0, destValue.get(), destValue.getOffset(), destValue.getLength() - origDestBitSetLen);
    	if (src != null) {
    	    o = Bytes.putBytes(merged, o, srcValue.get(), srcValue.getOffset(), srcValueLen);
    	}
    	destBitSet.toBytes(merged, o);
        return useNewValueColumnQualifier ? new ProjectedValueTuple(dest, dest.getTimestamp(), merged, 0, merged.length, destBitSetLen) : 
            new OldProjectedValueTuple(dest, dest.getTimestamp(), merged, 0, merged.length, destBitSetLen);
    }

    public KeyValueSchema getSchema() {
        return schema;
    }

    public Expression[] getExpressions() {
        return expressions;
    }

    public ValueBitSet getValueBitSet() {
        return valueSet;
    }
    
    @Override
    public String toString() {
        return "TUPLE-PROJECTOR {" + Arrays.toString(expressions) + " ==> " + schema.toString() + "}";
    }
}

