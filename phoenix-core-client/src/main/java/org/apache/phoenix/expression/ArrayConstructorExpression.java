/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.phoenix.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PArrayDataType;
import org.apache.phoenix.schema.types.PArrayDataTypeEncoder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.TrustedByteArrayOutputStream;

/**
 * Creates an expression for Upsert with Values/Select using ARRAY
 */
public class ArrayConstructorExpression extends BaseCompoundExpression {
    private PDataType baseType;
    private int position = -1;
    private Object[] elements;
    private final ImmutableBytesWritable valuePtr = new ImmutableBytesWritable();
    private int estimatedSize = 0;
    private boolean rowKeyOrderOptimizable;
    
    public ArrayConstructorExpression() {
    }

    public ArrayConstructorExpression(List<Expression> children, PDataType baseType, boolean rowKeyOrderOptimizable) {
        super(children);
        init(baseType, rowKeyOrderOptimizable);
    }

    public ArrayConstructorExpression clone(List<Expression> children) {
        return new ArrayConstructorExpression(children, this.baseType, this.rowKeyOrderOptimizable);
    }
    
    private void init(PDataType baseType, boolean rowKeyOrderOptimizable) {
        this.baseType = baseType;
        this.rowKeyOrderOptimizable = rowKeyOrderOptimizable;
        elements = new Object[getChildren().size()];
        valuePtr.set(ByteUtil.EMPTY_BYTE_ARRAY);
        estimatedSize = PArrayDataType.estimateSize(this.children.size(), this.baseType);
    }

    @Override
    public PDataType getDataType() {
        return PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE);
    }

    @Override
    public void reset() {
        super.reset();
        position = 0;
        Arrays.fill(elements, null);
        valuePtr.set(ByteUtil.EMPTY_BYTE_ARRAY);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (position == elements.length) {
            ptr.set(valuePtr.get(), valuePtr.getOffset(), valuePtr.getLength());
            return true;
        }
        TrustedByteArrayOutputStream byteStream = new TrustedByteArrayOutputStream(estimatedSize);
        DataOutputStream oStream = new DataOutputStream(byteStream);
        PArrayDataTypeEncoder builder =
                new PArrayDataTypeEncoder(byteStream, oStream, children.size(), baseType, getSortOrder(), rowKeyOrderOptimizable, PArrayDataType.SORTABLE_SERIALIZATION_VERSION);
        for (int i = position >= 0 ? position : 0; i < elements.length; i++) {
            Expression child = children.get(i);
            if (!child.evaluate(tuple, ptr)) {
                if (tuple != null && !tuple.isImmutable()) {
                    if (position >= 0) position = i;
                    return false;
                }
            } else {
                builder.appendValue(ptr.get(), ptr.getOffset(), ptr.getLength());
            }
        }
        if (position >= 0) position = elements.length;
        byte[] bytes = builder.encode();
        ptr.set(bytes, 0, bytes.length);
        valuePtr.set(ptr.get(), ptr.getOffset(), ptr.getLength());
        return true;
    }


    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        boolean rowKeyOrderOptimizable = false;
        int baseTypeOrdinal = WritableUtils.readVInt(input);
        if (baseTypeOrdinal < 0) {
            rowKeyOrderOptimizable = true;
            baseTypeOrdinal = -(baseTypeOrdinal+1);
        }
        init(PDataType.values()[baseTypeOrdinal], rowKeyOrderOptimizable);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        if (rowKeyOrderOptimizable) {
            WritableUtils.writeVInt(output, -(baseType.ordinal()+1));
        } else {
            WritableUtils.writeVInt(output, baseType.ordinal());
        }
    }
    
    @Override
    public boolean requiresFinalEvaluation() {
        return true;
    }

    @Override
    public final <T> T accept(ExpressionVisitor<T> visitor) {
        List<T> l = acceptChildren(visitor, visitor.visitEnter(this));
        T t = visitor.visitLeave(this, l);
        if (t == null) {
            t = visitor.defaultReturn(this, l);
        }
        return t;
    }
    
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder(PArrayDataType.ARRAY_TYPE_SUFFIX + "[");
        if (children.size()==0)
            return buf.append("]").toString();
        for (int i = 0; i < children.size() - 1; i++) {
            buf.append(children.get(i) + ",");
        }
        buf.append(children.get(children.size()-1) + "]");
        return buf.toString();
    }

}