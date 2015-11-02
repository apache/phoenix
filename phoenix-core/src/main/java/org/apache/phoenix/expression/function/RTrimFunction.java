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

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.compile.KeyPart;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.StringUtil;


/**
 * 
 * Implementation of the RTrim(<string>) build-in function. It removes from the right end of
 * <string> space character and other function bytes in single byte utf8 characters set 
 * 
 * 
 * @since 0.1
 */
@BuiltInFunction(name=RTrimFunction.NAME, args={
    @Argument(allowedTypes={PVarchar.class})})
public class RTrimFunction extends ScalarFunction {
    public static final String NAME = "RTRIM";

    public RTrimFunction() { }

    public RTrimFunction(List<Expression> children) throws SQLException {
        super(children);
    }

    private Expression getStringExpression() {
        return children.get(0);
    }

    @Override
    public SortOrder getSortOrder() {
        return children.get(0).getSortOrder();
    }    

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        // Starting from the end of the byte, look for all single bytes at the end of the string
        // that is below SPACE_UTF8 (space and control characters) or above (control chars).
        if (!getStringExpression().evaluate(tuple, ptr)) {
            return false;
        }
        if (ptr.getLength() == 0) {
            ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
            return true;
        }
        byte[] string = ptr.get();
        int offset = ptr.getOffset();
        int length = ptr.getLength();
        
        SortOrder sortOrder = getStringExpression().getSortOrder();
        int i = StringUtil.getFirstNonBlankCharIdxFromEnd(string, offset, length, sortOrder);
        if (i == offset - 1) {
            ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
            return true;
            }
        ptr.set(string, offset, i - offset + 1);
        return true;
    }

    @Override
    public OrderPreserving preservesOrder() {
        return OrderPreserving.YES_IF_LAST;
    }

    @Override
    public int getKeyFormationTraversalIndex() {
        return 0;
    }

    @Override
    public KeyPart newKeyPart(final KeyPart childPart) {
        return new KeyPart() {
            @Override
            public KeyRange getKeyRange(CompareOp op, Expression rhs) {
                byte[] lowerRange = KeyRange.UNBOUND;
                byte[] upperRange = KeyRange.UNBOUND;
                boolean lowerInclusive = true;
                boolean upperInclusive = false;
                
                PDataType type = getColumn().getDataType();
                SortOrder sortOrder = getColumn().getSortOrder();
                switch (op) {
                case LESS_OR_EQUAL:
                    lowerInclusive = false;
                case EQUAL:
                    upperRange = evaluateExpression(rhs);
                    if (op == CompareOp.EQUAL) {
                        lowerRange = upperRange;
                    }
                    if (sortOrder == SortOrder.ASC || !getTable().rowKeyOrderOptimizable()) {
                        upperRange = Arrays.copyOf(upperRange, upperRange.length + 1);
                        upperRange[upperRange.length-1] = StringUtil.SPACE_UTF8;
                        ByteUtil.nextKey(upperRange, upperRange.length);
                    } else {
                        upperInclusive = true;
                        if (op == CompareOp.LESS_OR_EQUAL) {
                            // Nothing more to do here, as the biggest value for DESC
                            // will be the RHS value.
                            break;
                        }
                        /*
                         * Somewhat tricky to get the range correct for the DESC equality case.
                         * The lower range is the RHS value followed by any number of inverted spaces.
                         * We need to add a zero byte as the lower range will have an \xFF byte
                         * appended to it and otherwise we'd skip past any rows where there is more
                         * than one space following the RHS.
                         * The upper range should span up to and including the RHS value. We need
                         * to add our own \xFF as otherwise this will look like a degenerate query
                         * since the lower would be bigger than the upper range.
                         */
                        lowerRange = Arrays.copyOf(lowerRange, lowerRange.length + 2);
                        lowerRange[lowerRange.length-2] = StringUtil.INVERTED_SPACE_UTF8;
                        lowerRange[lowerRange.length-1] = QueryConstants.SEPARATOR_BYTE;
                        upperRange = Arrays.copyOf(upperRange, upperRange.length + 1);
                        upperRange[upperRange.length-1] = QueryConstants.DESC_SEPARATOR_BYTE;
                    }
                    break;
                default:
                    // TOOD: Is this ok for DESC?
                    return childPart.getKeyRange(op, rhs);
                }
                Integer length = getColumn().getMaxLength();
                if (type.isFixedWidth() && length != null) {
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
                return KeyRange.getKeyRange(lowerRange, lowerInclusive, upperRange, upperInclusive);
            }

            @Override
            public List<Expression> getExtractNodes() {
                // We cannot extract the node, as we may have false positives with trailing
                // non blank characters such as 'foo  bar' where the RHS constant is 'foo'.
                return Collections.<Expression>emptyList();
            }

            @Override
            public PColumn getColumn() {
                return childPart.getColumn();
            }

            @Override
            public PTable getTable() {
                return childPart.getTable();
            }
        };
    }

    @Override
    public Integer getMaxLength() {
        return getStringExpression().getMaxLength();
    }

    @Override
    public PDataType getDataType() {
        return PVarchar.INSTANCE;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
