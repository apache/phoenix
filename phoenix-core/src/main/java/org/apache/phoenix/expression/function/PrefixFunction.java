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

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.compile.KeyPart;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.util.ByteUtil;

abstract public class PrefixFunction extends ScalarFunction {
    public PrefixFunction() {
    }

    public PrefixFunction(List<Expression> children) {
        super(children);
    }

    @Override
    public int getKeyFormationTraversalIndex() {
        return preservesOrder() == OrderPreserving.NO ? NO_TRAVERSAL : 0;
    }
    
    protected boolean extractNode() {
        return false;
    }

    private static byte[] evaluateExpression(Expression rhs) {
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        rhs.evaluate(null, ptr);
        byte[] key = ByteUtil.copyKeyBytesIfNecessary(ptr);
        return key;
    }
    
    @Override
    public KeyPart newKeyPart(final KeyPart childPart) {
        return new KeyPart() {
            private final List<Expression> extractNodes = extractNode() ? Collections.<Expression>singletonList(PrefixFunction.this) : Collections.<Expression>emptyList();

            @Override
            public PColumn getColumn() {
                return childPart.getColumn();
            }

            @Override
            public List<Expression> getExtractNodes() {
                return extractNodes;
            }

            @Override
            public KeyRange getKeyRange(CompareOp op, Expression rhs) {
                byte[] key;
                KeyRange range;
                PDataType type = getColumn().getDataType();
                switch (op) {
                case EQUAL:
                    key = evaluateExpression(rhs);
                    range = type.getKeyRange(key, true, ByteUtil.nextKey(key), false);
                    break;
                case GREATER:
                    key = evaluateExpression(rhs);
                    range = type.getKeyRange(ByteUtil.nextKey(key), true, KeyRange.UNBOUND, false);
                    break;
                case LESS_OR_EQUAL:
                    key = evaluateExpression(rhs);
                    range = type.getKeyRange(KeyRange.UNBOUND, false, ByteUtil.nextKey(key), false);
                    break;
                default:
                    return childPart.getKeyRange(op, rhs);
                }
                Integer length = getColumn().getMaxLength();
                return length == null || !type.isFixedWidth() ? range : range.fill(length);
            }
        };
    }


}
