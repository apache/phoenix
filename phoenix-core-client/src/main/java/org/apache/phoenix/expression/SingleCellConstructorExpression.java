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
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.ColumnValueEncoder;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarbinary;

/**
 * Expression used to create a single cell containing all the column values for a column family
 */
public class SingleCellConstructorExpression extends BaseCompoundExpression {
    
    private ImmutableStorageScheme immutableStorageScheme;
    
    public SingleCellConstructorExpression(ImmutableStorageScheme immutableStorageScheme, List<Expression> children) {
        super(children);
        this.immutableStorageScheme = immutableStorageScheme;
    }

    @Override
    public PDataType getDataType() {
        return PVarbinary.INSTANCE;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        ColumnValueEncoder encoderDecoder = immutableStorageScheme.getEncoder(children.size());
        for (int i=0; i < children.size(); i++) {
            Expression child = children.get(i);
            if (!child.evaluate(tuple, ptr)) {
                encoderDecoder.appendAbsentValue();
            } else {
                encoderDecoder.appendValue(ptr.get(), ptr.getOffset(), ptr.getLength());
            }
        }
        byte[] bytes = encoderDecoder.encode();
        ptr.set(bytes, 0, bytes.length);
        return true;
    }


    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        this.immutableStorageScheme = WritableUtils.readEnum(input, ImmutableStorageScheme.class);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        WritableUtils.writeEnum(output, immutableStorageScheme);
    }
    
    @Override
    public boolean requiresFinalEvaluation() {
        return true;
    }
    
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder("[");
        if (children.size()==0)
            return buf.append("]").toString();
        for (int i = 0; i < children.size() - 1; i++) {
            buf.append(children.get(i) + ",");
        }
        buf.append(children.get(children.size()-1) + "]");
        return buf.toString();
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

    public SingleCellConstructorExpression clone(List<Expression> children) {
        return new SingleCellConstructorExpression(immutableStorageScheme, children);
    }
}