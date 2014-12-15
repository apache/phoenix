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
import org.apache.phoenix.compile.KeyPart;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.StringUtil;

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
                byte[] lowerRange = KeyRange.UNBOUND;
                byte[] upperRange = KeyRange.UNBOUND;
                boolean lowerInclusive = true;
                PDataType type = getColumn().getDataType();
                switch (op) {
                case EQUAL:
                    lowerRange = evaluateExpression(rhs);
                    upperRange = ByteUtil.nextKey(lowerRange);
                    break;
                case GREATER:
                    lowerRange = ByteUtil.nextKey(evaluateExpression(rhs));
                    break;
                case LESS_OR_EQUAL:
                    upperRange = ByteUtil.nextKey(evaluateExpression(rhs));
                    lowerInclusive = false;
                    break;
                default:
                    return childPart.getKeyRange(op, rhs);
                }
                Integer length = getColumn().getMaxLength();
                if (type.isFixedWidth() && length != null) {
                    if (lowerRange != KeyRange.UNBOUND) {
                        lowerRange = StringUtil.padChar(lowerRange, length);
                    }
                    if (upperRange != KeyRange.UNBOUND) {
                        upperRange = StringUtil.padChar(upperRange, length);
                    }
                }
                return KeyRange.getKeyRange(lowerRange, lowerInclusive, upperRange, false);
            }
        };
    }


}
