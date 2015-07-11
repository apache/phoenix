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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.compile.ColumnProjector;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;
import org.apache.phoenix.schema.KeyValueSchema;
import org.apache.phoenix.schema.KeyValueSchema.KeyValueSchemaBuilder;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.ProjectedColumn;
import org.apache.phoenix.schema.ValueBitSet;
import org.apache.phoenix.schema.tuple.BaseTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.KeyValueUtil;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.base.Preconditions;

public class TupleProjector {    
    public static final byte[] VALUE_COLUMN_FAMILY = Bytes.toBytes("_v");
    public static final byte[] VALUE_COLUMN_QUALIFIER = new byte[0];
    
    private static final String SCAN_PROJECTOR = "scanProjector";
    
    private final KeyValueSchema schema;
    private final Expression[] expressions;
    private ValueBitSet valueSet;
    private final ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    
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
    
    public TupleProjector(PTable projectedTable) {
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
    
    public static void serializeProjectorIntoScan(Scan scan, TupleProjector projector) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            DataOutputStream output = new DataOutputStream(stream);
            projector.schema.write(output);
            int count = projector.expressions.length;
            WritableUtils.writeVInt(output, count);
            for (int i = 0; i < count; i++) {
            	WritableUtils.writeVInt(output, ExpressionType.valueOf(projector.expressions[i]).ordinal());
            	projector.expressions[i].write(output);
            }
            scan.setAttribute(SCAN_PROJECTOR, stream.toByteArray());
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
        byte[] proj = scan.getAttribute(SCAN_PROJECTOR);
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
    
    public static class ProjectedValueTuple extends BaseTuple {
        private ImmutableBytesWritable keyPtr = new ImmutableBytesWritable();
        private long timestamp;
        private ImmutableBytesWritable projectedValue = new ImmutableBytesWritable();
        private int bitSetLen;
        private KeyValue keyValue;

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
        public KeyValue getValue(int index) {
            if (index != 0) {
                throw new IndexOutOfBoundsException(Integer.toString(index));
            }
            return getValue(VALUE_COLUMN_FAMILY, VALUE_COLUMN_QUALIFIER);
        }

        @Override
        public KeyValue getValue(byte[] family, byte[] qualifier) {
            if (keyValue == null) {
                keyValue = KeyValueUtil.newKeyValue(keyPtr.get(), keyPtr.getOffset(), keyPtr.getLength(), 
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
    
    public ProjectedValueTuple projectResults(Tuple tuple) {
    	byte[] bytesValue = schema.toBytes(tuple, getExpressions(), valueSet, ptr);
    	Cell base = tuple.getValue(0);
        return new ProjectedValueTuple(base.getRowArray(), base.getRowOffset(), base.getRowLength(), base.getTimestamp(), bytesValue, 0, bytesValue.length, valueSet.getEstimatedLength());
    }
    
    public static void decodeProjectedValue(Tuple tuple, ImmutableBytesWritable ptr) throws IOException {
    	boolean b = tuple.getValue(VALUE_COLUMN_FAMILY, VALUE_COLUMN_QUALIFIER, ptr);
        if (!b)
            throw new IOException("Trying to decode a non-projected value.");
    }
    
    public static ProjectedValueTuple mergeProjectedValue(ProjectedValueTuple dest, KeyValueSchema destSchema, ValueBitSet destBitSet,
    		Tuple src, KeyValueSchema srcSchema, ValueBitSet srcBitSet, int offset) throws IOException {
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
        return new ProjectedValueTuple(dest, dest.getTimestamp(), merged, 0, merged.length, destBitSetLen);
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

