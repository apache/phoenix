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


import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ByteUtil;


/**
 * 
 * Implementation for || string concatenation expression.
 * 
 * @since 0.1
 */

public class StringConcatExpression extends BaseCompoundExpression {
    public StringConcatExpression() {
    }

    public StringConcatExpression(List<Expression> children) {
        super(children);
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder("(");
        for (int i = 0; i < children.size() - 1; i++) {
            buf.append(children.get(i) + " || ");
        }
        buf.append(children.get(children.size()-1));
        buf.append(')');
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

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        byte[] result = ByteUtil.EMPTY_BYTE_ARRAY;
        for (int i=0; i<children.size(); i++) {
            if (children.get(i).getDataType() == null || !children.get(i).evaluate(tuple, ptr)) {
                continue;
            }
            PDataType childType = children.get(i).getDataType();
            SortOrder sortOrder = children.get(i).getSortOrder();
            // We could potentially not invert the bytes, but we might as well since we're allocating
            // additional space here anyway.
            if (childType.isCoercibleTo(PVarchar.INSTANCE)) {
                result = ByteUtil.concat(result, ByteUtil.concat(sortOrder, ptr));
            } else {
                result = ByteUtil.concat(result, PVarchar.INSTANCE.toBytes(childType.toObject(ptr, sortOrder).toString()));
            }
        }
        ptr.set(result);
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PVarchar.INSTANCE;
    }
}
