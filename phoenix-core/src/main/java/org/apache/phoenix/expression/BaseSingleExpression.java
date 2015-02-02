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

import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;

import com.google.common.collect.ImmutableList;


/**
 * 
 * Base class for expressions which have a single child expression
 *
 * 
 * @since 0.1
 */
public abstract class BaseSingleExpression extends BaseExpression {

    protected List<Expression> children;
    
    public BaseSingleExpression() {
    }

    public BaseSingleExpression(Expression expression) {
        this(ImmutableList.of(expression));
    }

    public BaseSingleExpression(List<Expression> children) {
        this.children = children;
    }

    @Override
    public List<Expression> getChildren() {
        return children;
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
        Expression expression = ExpressionType.values()[WritableUtils.readVInt(input)].newInstance();
        expression.readFields(input);
        children = ImmutableList.of(expression);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        WritableUtils.writeVInt(output, ExpressionType.valueOf(children.get(0)).ordinal());
        children.get(0).write(output);
    }

    @Override
    public boolean isNullable() {
        return children.get(0).isNullable();
    }

    @Override
    public void reset() {
        children.get(0).reset();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + children.get(0).hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        BaseSingleExpression other = (BaseSingleExpression)obj;
        if (!children.get(0).equals(other.children.get(0))) return false;
        return true;
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        List<T> l = acceptChildren(visitor, null);
        if (l.isEmpty()) {
            return visitor.defaultReturn(this, l);
        }
        return l.get(0);
    }
    
    public Expression getChild() {
        return children.get(0);
    }
    
    @Override
    public boolean requiresFinalEvaluation() {
        return children.get(0).requiresFinalEvaluation();
    }
}
