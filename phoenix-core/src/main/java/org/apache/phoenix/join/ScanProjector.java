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
package org.apache.phoenix.join;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.compile.JoinCompiler.ProjectedPTableWrapper;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;
import org.apache.phoenix.schema.KeyValueSchema;
import org.apache.phoenix.schema.KeyValueSchema.KeyValueSchemaBuilder;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.ValueBitSet;
import org.apache.phoenix.schema.tuple.BaseTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.KeyValueUtil;
import org.apache.phoenix.util.SchemaUtil;

public class ScanProjector {    
    public static final byte[] VALUE_COLUMN_FAMILY = Bytes.toBytes("_v");
    public static final byte[] VALUE_COLUMN_QUALIFIER = new byte[0];
    
    private static final String SCAN_PROJECTOR = "scanProjector";
    
    private final KeyValueSchema schema;
    private final Expression[] expressions;
    private ValueBitSet valueSet;
    private final ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    
    public ScanProjector(ProjectedPTableWrapper projected) {
    	List<PColumn> columns = projected.getTable().getColumns();
    	expressions = new Expression[columns.size() - projected.getTable().getPKColumns().size()];
    	// we do not count minNullableIndex for we might do later merge.
    	KeyValueSchemaBuilder builder = new KeyValueSchemaBuilder(0);
    	int i = 0;
        for (PColumn column : projected.getTable().getColumns()) {
        	if (!SchemaUtil.isPKColumn(column)) {
        		builder.addField(column);
        		expressions[i++] = projected.getSourceExpression(column);
        	}
        }
        schema = builder.build();
        valueSet = ValueBitSet.newInstance(schema);
    }
    
    private ScanProjector(KeyValueSchema schema, Expression[] expressions) {
    	this.schema = schema;
    	this.expressions = expressions;
    	this.valueSet = ValueBitSet.newInstance(schema);
    }
    
    public void setValueBitSet(ValueBitSet bitSet) {
        this.valueSet = bitSet;
    }
    
    public static void serializeProjectorIntoScan(Scan scan, ScanProjector projector) {
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
    
    public static ScanProjector deserializeProjectorFromScan(Scan scan) {
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
            return new ScanProjector(schema, expressions);
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
        private byte[] projectedValue;
        private int bitSetLen;
        private KeyValue keyValue;

        private ProjectedValueTuple(byte[] keyBuffer, int keyOffset, int keyLength, long timestamp, byte[] projectedValue, int bitSetLen) {
            this.keyPtr.set(keyBuffer, keyOffset, keyLength);
            this.timestamp = timestamp;
            this.projectedValue = projectedValue;
            this.bitSetLen = bitSetLen;
        }
        
        public ImmutableBytesWritable getKeyPtr() {
            return keyPtr;
        }
        
        public long getTimestamp() {
            return timestamp;
        }
        
        public byte[] getProjectedValue() {
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
                        VALUE_COLUMN_FAMILY, VALUE_COLUMN_QUALIFIER, timestamp, projectedValue, 0, projectedValue.length);
            }
            return keyValue;
        }

        @Override
        public boolean getValue(byte[] family, byte[] qualifier,
                ImmutableBytesWritable ptr) {
            ptr.set(projectedValue);
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
    	byte[] bytesValue = schema.toBytes(tuple, expressions, valueSet, ptr);
    	Cell base = tuple.getValue(0);
        return new ProjectedValueTuple(base.getRowArray(), base.getRowOffset(), base.getRowLength(), base.getTimestamp(), bytesValue, valueSet.getEstimatedLength());
    }
    
    public static void decodeProjectedValue(Tuple tuple, ImmutableBytesWritable ptr) throws IOException {
    	boolean b = tuple.getValue(VALUE_COLUMN_FAMILY, VALUE_COLUMN_QUALIFIER, ptr);
        if (!b)
            throw new IOException("Trying to decode a non-projected value.");
    }
    
    public static ProjectedValueTuple mergeProjectedValue(ProjectedValueTuple dest, KeyValueSchema destSchema, ValueBitSet destBitSet,
    		Tuple src, KeyValueSchema srcSchema, ValueBitSet srcBitSet, int offset) throws IOException {
    	ImmutableBytesWritable destValue = new ImmutableBytesWritable(dest.getProjectedValue());
    	destBitSet.clear();
    	destBitSet.or(destValue);
    	int origDestBitSetLen = dest.getBitSetLength();
    	ImmutableBytesWritable srcValue = new ImmutableBytesWritable();
    	decodeProjectedValue(src, srcValue);
    	srcBitSet.clear();
    	srcBitSet.or(srcValue);
    	int origSrcBitSetLen = srcBitSet.getEstimatedLength();
    	for (int i = 0; i < srcBitSet.getMaxSetBit(); i++) {
    		if (srcBitSet.get(i)) {
    			destBitSet.set(offset + i);
    		}
    	}
    	int destBitSetLen = destBitSet.getEstimatedLength();
    	byte[] merged = new byte[destValue.getLength() - origDestBitSetLen + srcValue.getLength() - origSrcBitSetLen + destBitSetLen];
    	int o = Bytes.putBytes(merged, 0, destValue.get(), destValue.getOffset(), destValue.getLength() - origDestBitSetLen);
    	o = Bytes.putBytes(merged, o, srcValue.get(), srcValue.getOffset(), srcValue.getLength() - origSrcBitSetLen);
    	destBitSet.toBytes(merged, o);
    	ImmutableBytesWritable keyPtr = dest.getKeyPtr();
        return new ProjectedValueTuple(keyPtr.get(), keyPtr.getOffset(), keyPtr.getLength(), dest.getTimestamp(), merged, destBitSetLen);
    }
}
