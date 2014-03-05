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
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.RowKeyValueAccessor;
import org.apache.phoenix.schema.tuple.Tuple;
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
    
    public int getPosition() {
        return accessor.getIndex();
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
        int offset = accessor.getOffset(ptr.get(), ptr.getOffset());
        // Null is represented in the last expression of a multi-part key 
        // by the bytes not being present.
        int maxOffset = ptr.getOffset() + ptr.getLength();
        if (offset < maxOffset) {
            byte[] buffer = ptr.get();
            int fixedByteSize = -1;
            // FIXME: fixedByteSize <= maxByteSize ? fixedByteSize : 0 required because HBase passes bogus keys to filter to position scan (HBASE-6562)
            if (fromType.isFixedWidth()) {
                fixedByteSize = getByteSize();
                fixedByteSize = fixedByteSize <= maxOffset ? fixedByteSize : 0;
            }
            int length = fixedByteSize >= 0 ? fixedByteSize  : accessor.getLength(buffer, offset, maxOffset);
            // In the middle of the key, an empty variable length byte array represents null
            if (length > 0) {
                /*
                if (type == fromType) {
                    ptr.set(buffer,offset,length);
                } else {
                    ptr.set(type.toBytes(type.toObject(buffer, offset, length, fromType)));
                }
                */
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
}
