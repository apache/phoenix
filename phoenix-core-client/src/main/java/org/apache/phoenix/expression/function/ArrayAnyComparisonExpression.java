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

import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.BaseCompoundExpression;
import org.apache.phoenix.expression.ComparisonExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PArrayDataType;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;

public class ArrayAnyComparisonExpression extends BaseCompoundExpression {
    public ArrayAnyComparisonExpression () {
    }
    public ArrayAnyComparisonExpression(List<Expression> children) {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression arrayKVExpression = children.get(0);
        if (!arrayKVExpression.evaluate(tuple, ptr)) {
            return false;
        } else if (ptr.getLength() == 0) { return true; }
        int length = PArrayDataType.getArrayLength(ptr,
                PDataType.fromTypeId(children.get(0).getDataType().getSqlType() - PDataType.ARRAY_TYPE_BASE),
                arrayKVExpression.getMaxLength());
        boolean elementAvailable = false;
        for (int i = 0; i < length; i++) {
            Expression comparisonExpr = children.get(1);
            Expression arrayElemRef = ((ComparisonExpression)comparisonExpr).getChildren().get(1);
            ((ArrayElemRefExpression)arrayElemRef).setIndex(i + 1);
            comparisonExpr.evaluate(tuple, ptr);
            if (expectedReturnResult(resultFound(ptr))) { return result(); }
            elementAvailable = true;
        }
        if (!elementAvailable) { return false; }
        return true;
    }
    protected boolean resultFound(ImmutableBytesWritable ptr) {
        if(Bytes.equals(ptr.get(), PDataType.TRUE_BYTES)) {
            return true;
        }
        return false;
    }
    
    protected boolean result() {
        return true;
    }
    
    protected boolean expectedReturnResult(boolean result) {
        return true == result;
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
}