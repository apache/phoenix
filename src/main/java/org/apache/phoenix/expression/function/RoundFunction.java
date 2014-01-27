/*
 * Copyright 2010 The Apache Software Foundation
 *
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Date;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;

import org.apache.phoenix.compile.KeyPart;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PDataType.PDataCodec;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ByteUtil;


/**
 * 
 * Function used to bucketize date/time values by rounding them to
 * an even increment.  Usage:
 * ROUND(<date/time col ref>,<'day'|'hour'|'minute'|'second'|'millisecond'>,<optional integer multiplier>)
 * The integer multiplier is optional and is used to do rollups to a partial time unit (i.e. 10 minute rollup)
 * The function returns a {@link org.apache.phoenix.schema.PDataType#DATE}
 *
 * @author jtaylor
 * @since 0.1
 */
@BuiltInFunction(name=RoundFunction.NAME, args= {
    @Argument(allowedTypes={PDataType.DATE}),
    @Argument(enumeration="TimeUnit"),
    @Argument(allowedTypes={PDataType.INTEGER}, isConstant=true, defaultValue="1")} )
public class RoundFunction extends ScalarFunction {
    public static final String NAME = "ROUND";
    private long divBy;
    
    private static final long[] TIME_UNIT_MS = new long[] {
        24 * 60 * 60 * 1000,
        60 * 60 * 1000,
        60 * 1000,
        1000,
        1
    };

    public RoundFunction() {
    }
    
    public RoundFunction(List<Expression> children) throws SQLException {
        super(children.subList(0, 1));
        Object timeUnitValue = ((LiteralExpression)children.get(1)).getValue();
        Object multiplierValue = ((LiteralExpression)children.get(2)).getValue();
        if (timeUnitValue != null && multiplierValue != null) {
            TimeUnit timeUnit = TimeUnit.valueOf(timeUnitValue.toString().toUpperCase());
            int multiplier = ((Number)multiplierValue).intValue();
            divBy = multiplier * TIME_UNIT_MS[timeUnit.ordinal()];
        }
    }
    
    protected long getRoundUpAmount() {
        return divBy/2;
    }
    
    private long roundTime(long time) {
        long value;
        long halfDivBy = getRoundUpAmount();
        if (time <= Long.MAX_VALUE - halfDivBy) { // If no overflow, add
            value = (time + halfDivBy) / divBy;
        } else { // Else subtract and add one
            value = (time - halfDivBy) / divBy + 1;
        }
        return value * divBy;
    }
    
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        // If divBy is 0 this means <time unit> or <multiplier> was null
        if (divBy != 0 && children.get(0).evaluate(tuple, ptr)) {
            long time = getDataType().getCodec().decodeLong(ptr, children.get(0).getColumnModifier());
            long value = roundTime(time);
            // TODO: use temporary buffer instead and have way for caller to check if copying is necessary
            byte[] byteValue = getDataType().toBytes(new Date(value));
            ptr.set(byteValue);
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        long roundUpAmount = this.getRoundUpAmount();
        result = prime * result + (int)(divBy ^ (divBy >>> 32));
        result = prime * result + (int)(roundUpAmount ^ (roundUpAmount >>> 32));
        result = prime * result + children.get(0).hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        RoundFunction other = (RoundFunction)obj;
        if (divBy != other.divBy) return false;
        if (getRoundUpAmount() != other.getRoundUpAmount()) return false;
        return children.get(0).equals(other.children.get(0));
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        divBy = WritableUtils.readVLong(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        WritableUtils.writeVLong(output, divBy);
    }

    @Override
    public final PDataType getDataType() {
        return PDataType.DATE;
    }
    
    @Override
    public Integer getByteSize() {
        return children.get(0).getByteSize();
    }

    @Override
    public boolean isNullable() {
        return children.get(0).isNullable() || divBy == 0;
    }
    
    @Override
    public OrderPreserving preservesOrder() {
        return OrderPreserving.YES;
    }

    @Override
    public int getKeyFormationTraversalIndex() {
        return 0;
    }

    /**
     * Form the key range from the key to the key right before or at the
     * next rounded value.
     */
    @Override
    public KeyPart newKeyPart(final KeyPart childPart) {
        return new KeyPart() {
            private final List<Expression> extractNodes = Collections.<Expression>singletonList(RoundFunction.this);

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
                PDataType type = getColumn().getDataType();
                ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                rhs.evaluate(null, ptr);
                byte[] key = ByteUtil.copyKeyBytesIfNecessary(ptr);
                // No need to take into account column modifier, because ROUND
                // always forces the value to be in ascending order
                PDataCodec codec = type.getCodec();
                int offset = ByteUtil.isInclusive(op) ? 1 : 0;
                long value = codec.decodeLong(key, 0, null);
                byte[] nextKey = new byte[type.getByteSize()];
                switch (op) {
                case EQUAL:
                    // If the value isn't evenly divisible by the div amount, then it
                    // can't possibly be equal to any rounded value. For example, if you
                    // had ROUND(dateCol,'DAY') = TO_DATE('2013-01-01 23:00:00')
                    // it could never be equal, since date constant isn't at a day
                    // boundary.
                    if (value % divBy != 0) {
                        return KeyRange.EMPTY_RANGE;
                    }
                    codec.encodeLong(value + divBy, nextKey, 0);
                    return type.getKeyRange(key, true, nextKey, false);
                case GREATER:
                case GREATER_OR_EQUAL:
                    codec.encodeLong((value + divBy - offset)/divBy*divBy, nextKey, 0);
                    return type.getKeyRange(nextKey, true, KeyRange.UNBOUND, false);
                case LESS:
                case LESS_OR_EQUAL:
                    codec.encodeLong((value + divBy - (1 -offset))/divBy*divBy, nextKey, 0);
                    return type.getKeyRange(KeyRange.UNBOUND, false, nextKey, false);
                default:
                    return childPart.getKeyRange(op, rhs);
                }
            }
        };
    }

    @Override
    public String getName() {
        return NAME;
    }
}
