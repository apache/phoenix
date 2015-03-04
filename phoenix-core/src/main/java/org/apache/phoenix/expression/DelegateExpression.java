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
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;

public class DelegateExpression implements Expression {
    private final Expression delegate;
    
    public DelegateExpression(Expression delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean isNullable() {
        return delegate.isNullable();
    }

    @Override
    public PDataType getDataType() {
        return delegate.getDataType();
    }

    @Override
    public Integer getMaxLength() {
        return delegate.getMaxLength();
    }

    @Override
    public Integer getScale() {
        return delegate.getScale();
    }

    @Override
    public SortOrder getSortOrder() {
        return delegate.getSortOrder();
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        delegate.readFields(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        delegate.write(output);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        return delegate.evaluate(tuple, ptr);
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return delegate.accept(visitor);
    }

    @Override
    public List<Expression> getChildren() {
        return delegate.getChildren();
    }

    @Override
    public void reset() {
        delegate.reset();
    }

    @Override
    public boolean isStateless() {
        return delegate.isStateless();
    }

    @Override
    public Determinism getDeterminism() {
        return delegate.getDeterminism();
    }

    @Override
    public boolean requiresFinalEvaluation() {
        return delegate.requiresFinalEvaluation();
    }

}
