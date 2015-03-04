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

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;


/**
 * 
 * Implementation of the NOT operator that negates it's
 * single boolean child expression.
 *
 * 
 * @since 0.1
 */
public class NotExpression extends BaseSingleExpression {

    public static Expression create(Expression child, ImmutableBytesWritable ptr) throws SQLException {
        if (child.getDataType() != PBoolean.INSTANCE) {
            throw TypeMismatchException.newException(child.getDataType(), PBoolean.INSTANCE, "NOT");
        }
        if (child.isStateless()) {
            if (!child.evaluate(null, ptr) || ptr.getLength() == 0) {
                return LiteralExpression.newConstant(null, PBoolean.INSTANCE, child.getDeterminism());
            }
            return LiteralExpression.newConstant(!(Boolean) PBoolean.INSTANCE.toObject(ptr), PBoolean.INSTANCE, child.getDeterminism());
        }
        return new NotExpression(child);
    }
    
    public NotExpression() {
    }

    public NotExpression(Expression expression) {
        super(expression);
    }

    public NotExpression(List<Expression> l) {
        super(l);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (!getChild().evaluate(tuple, ptr)) {
            return false;
        }
        if (ptr.getLength() == 0) {
            return true;
        }
        
        ptr.set(Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(ptr)) ? PDataType.FALSE_BYTES : PDataType.TRUE_BYTES);
        return true;
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
        StringBuilder buf = new StringBuilder("NOT (");
        buf.append(children.get(0).toString());
        buf.append(")");
        return buf.toString();
    }
}
