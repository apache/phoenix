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
package org.apache.phoenix.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.RowKeyValueAccessor;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.ByteUtil;


/**
 * 
 * Class to access a value stored in the row key
 *
 * 
 * @since 0.1
 */
public class RowKeyColumnExpression  extends ColumnExpression {
    private PDataType fromType;
    private RowKeyValueAccessor accessor;
    protected final String name;
    private int offset;
    
    public RowKeyColumnExpression() {
        name = null; // Only on client
    }
    
    private RowKeyColumnExpression(PDatum datum, RowKeyValueAccessor accessor, PDataType fromType, String name) {
        super(datum);
        this.accessor = accessor;
        this.fromType = fromType;
        this.name = name;
    }
    
    public RowKeyColumnExpression(PDatum datum, RowKeyValueAccessor accessor) {
        this(datum, accessor, datum.getDataType(), datum.toString());
    }
    
    public RowKeyColumnExpression(PDatum datum, RowKeyValueAccessor accessor, String name) {
        this(datum, accessor, datum.getDataType(), name);
    }
    
    public RowKeyColumnExpression(PDatum datum, RowKeyValueAccessor accessor, PDataType fromType) {
        this(datum, accessor, fromType, datum.toString());
    }
    
    /**
     * Used to set an offset to be skipped from the start of a the row key. Used by
     * local indexing to skip the region start key bytes.
     * @param offset the number of bytes to offset accesses to row key columns
     */
    public void setOffset(int offset) {
        this.offset = offset;
    }
    
    public int getPosition() {
        return accessor.getIndex();
    }
    
    public String getName() {
        return name;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((accessor == null) ? 0 : accessor.hashCode());
        return result;
    }

    @Override
    public String toString() {
        return name == null ? "PK[" + accessor.getIndex() + "]" : name;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!super.equals(obj)) return false;
        if (getClass() != obj.getClass()) return false;
        RowKeyColumnExpression other = (RowKeyColumnExpression)obj;
        return accessor.equals(other.accessor);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        tuple.getKey(ptr);
        int offset = accessor.getOffset(ptr.get(), ptr.getOffset() + this.offset);
        // Null is represented in the last expression of a multi-part key 
        // by the bytes not being present.
        int maxOffset = ptr.getOffset() + ptr.getLength();
        if (offset < maxOffset) {
            byte[] buffer = ptr.get();
            int byteSize = -1;
            // FIXME: fixedByteSize <= maxByteSize ? fixedByteSize : 0 required because HBase passes bogus keys to filter to position scan (HBASE-6562)
            if (fromType.isFixedWidth()) {
                Integer maxLength = getMaxLength();
                byteSize = maxLength == null ? fromType.getByteSize() : maxLength;
                byteSize = byteSize <= maxOffset ? byteSize : 0;
            }
            int length = byteSize >= 0 ? byteSize  : accessor.getLength(buffer, offset, maxOffset);
            // In the middle of the key, an empty variable length byte array represents null
            if (length > 0) {
                ptr.set(buffer,offset,length);
                type.coerceBytes(ptr, fromType, getSortOrder(), getSortOrder());
            } else {
                ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
            }
            return true;
        }
        return false;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        accessor = new RowKeyValueAccessor();
        accessor.readFields(input);
        fromType = type; // fromType only needed on client side
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        accessor.write(output);
    }
    
    @Override
    public final <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }
    
    /**
     * Since we may never have encountered a key value column of interest, but the
     * expression may evaluate to true just based on the row key columns, we need
     * to do a final evaluation. An example of when this would be required is:
     *     SELECT a FROM t WHERE a = 5 OR b = 2
     * in the case where a is a PK column, b is a KV column and no b KV is found.
     */
    @Override
    public boolean requiresFinalEvaluation() {
        return true;
    }
}
