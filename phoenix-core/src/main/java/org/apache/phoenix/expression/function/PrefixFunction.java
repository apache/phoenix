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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.phoenix.compile.KeyPart;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;
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
                PColumn column = getColumn();
                Integer length = column.getMaxLength();
                if (type.isFixedWidth()) {
                    if (length != null) { // Sanity check - shouldn't be necessary
                        // Don't pad based on current sort order, but instead use our
                        // minimum byte as otherwise we'll end up skipping rows in
                        // the case of descending, since rows with more padding appear
                        // *after* rows with no padding.
                        if (lowerRange != KeyRange.UNBOUND) {
                            lowerRange = type.pad(lowerRange, length, SortOrder.ASC);
                        }
                        if (upperRange != KeyRange.UNBOUND) {
                            upperRange = type.pad(upperRange, length, SortOrder.ASC);
                        }
                    }
                } else if (column.getSortOrder() == SortOrder.DESC && getTable().rowKeyOrderOptimizable()) {
                    // Append a zero byte if descending since a \xFF byte will be appended to the lowerRange
                    // causing rows to be skipped that should be included. For example, with rows 'ab', 'a',
                    // a lowerRange of 'a\xFF' would skip 'ab', while 'a\x00\xFF' would not.
                    if (lowerRange != KeyRange.UNBOUND) {
                        lowerRange = Arrays.copyOf(lowerRange, lowerRange.length+1);
                        lowerRange[lowerRange.length-1] = QueryConstants.SEPARATOR_BYTE;
                    }
                }
                return KeyRange.getKeyRange(lowerRange, lowerInclusive, upperRange, false);
            }

            @Override
            public PTable getTable() {
                return childPart.getTable();
            }
        };
    }


}
