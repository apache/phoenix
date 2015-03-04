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
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.ExpressionUtil;


/**
 * 
 * Implementation of IS NULL and IS NOT NULL expression
 *
 */
public class IsNullExpression extends BaseSingleExpression {
    private boolean isNegate;

    public static Expression create(Expression child, boolean negate, ImmutableBytesWritable ptr) throws SQLException {
        if (!child.isNullable()) {
            return LiteralExpression.newConstant(negate, PBoolean.INSTANCE, child.getDeterminism());
        }
        if (ExpressionUtil.isConstant(child)) {
            boolean evaluated = child.evaluate(null, ptr);
            return LiteralExpression.newConstant(negate ^ (!evaluated || ptr.getLength() == 0), PBoolean.INSTANCE, child.getDeterminism());
        }
        return new IsNullExpression(child, negate);
    }
    
    public IsNullExpression() {
    }
    
    private IsNullExpression(Expression expression, boolean negate) {
        super(expression);
        this.isNegate = negate;
    }

    public IsNullExpression(List<Expression> children, boolean negate) {
        super(children);
        this.isNegate = negate;
    }

    public IsNullExpression clone(List<Expression> children) {
        return new IsNullExpression(children, this.isNegate());
    }
    
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        boolean evaluated = getChild().evaluate(tuple, ptr);
        if (evaluated) {
            ptr.set(isNegate ^ ptr.getLength() == 0 ? PDataType.TRUE_BYTES : PDataType.FALSE_BYTES);
            return true;
        }
        if (tuple.isImmutable()) {
            ptr.set(isNegate ? PDataType.FALSE_BYTES : PDataType.TRUE_BYTES);
            return true;
        }
        
        return false;
    }

    public boolean isNegate() {
        return isNegate;
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        isNegate = input.readBoolean();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        output.writeBoolean(isNegate);
    }

    @Override
    public PDataType getDataType() {
        return PBoolean.INSTANCE;
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
        StringBuilder buf = new StringBuilder(children.get(0).toString());
        if (isNegate) {
            buf.append(" IS NOT NULL");
        } else {
            buf.append(" IS NULL");
        }
        return buf.toString();
    }
    
    @Override
    public boolean requiresFinalEvaluation() {
        return super.requiresFinalEvaluation() || !this.isNegate();
    }
}
