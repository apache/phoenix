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
package org.apache.phoenix.expression.function;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.BaseCompoundExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PArrayDataType;
import org.apache.phoenix.schema.types.PDataType;

public class ArrayElemRefExpression extends BaseCompoundExpression {

    private int index;

    public ArrayElemRefExpression() {
    }
    
    public ArrayElemRefExpression(List<Expression> children) {
        super(children);
    }

    public void setIndex(int index) {
        this.index = index;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression arrayExpr = children.get(0);
        return PArrayDataType.positionAtArrayElement(tuple, ptr, index, arrayExpr, getDataType(), getMaxLength());
    }

    @Override
    public Integer getMaxLength() {
        return this.children.get(0).getMaxLength();
    }

    @Override
    public PDataType getDataType() {
        return PDataType.fromTypeId(children.get(0).getDataType().getSqlType() - PDataType.ARRAY_TYPE_BASE);
    }
    
    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
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
}