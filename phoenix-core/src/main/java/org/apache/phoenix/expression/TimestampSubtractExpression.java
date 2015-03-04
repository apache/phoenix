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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PUnsignedTimestamp;
import org.apache.phoenix.util.DateUtil;
/**
 * 
 * Class to encapsulate subtraction arithmetic for {@link org.apache.phoenix.schema.types.PTimestamp}.
 *
 * 
 * @since 2.1.3
 */
public class TimestampSubtractExpression extends SubtractExpression {

    public TimestampSubtractExpression() {
    }

    public TimestampSubtractExpression(List<Expression> children) {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        BigDecimal finalResult = BigDecimal.ZERO;
        
        for(int i=0; i<children.size(); i++) {
            if (!children.get(i).evaluate(tuple, ptr)) {
                return false;
            }
            if (ptr.getLength() == 0) {
                return true;
            }
            BigDecimal value;
            PDataType type = children.get(i).getDataType();
            SortOrder sortOrder = children.get(i).getSortOrder();
            if(type == PTimestamp.INSTANCE || type == PUnsignedTimestamp.INSTANCE) {
                value = (BigDecimal)(PDecimal.INSTANCE.toObject(ptr, type, sortOrder));
            } else if (type.isCoercibleTo(PDecimal.INSTANCE)) {
                value = (((BigDecimal) PDecimal.INSTANCE.toObject(ptr, sortOrder)).multiply(BD_MILLIS_IN_DAY)).setScale(6, RoundingMode.HALF_UP);
            } else if (type.isCoercibleTo(PDouble.INSTANCE)) {
                value = ((BigDecimal.valueOf(type.getCodec().decodeDouble(ptr, sortOrder))).multiply(BD_MILLIS_IN_DAY)).setScale(6, RoundingMode.HALF_UP);
            } else {
                value = BigDecimal.valueOf(type.getCodec().decodeLong(ptr, sortOrder));
            }
            if (i == 0) {
                finalResult = value;
            } else {
                finalResult = finalResult.subtract(value);
            }
        }
        Timestamp ts = DateUtil.getTimestamp(finalResult);
        byte[] resultPtr = new byte[getDataType().getByteSize()];
        PTimestamp.INSTANCE.toBytes(ts, resultPtr, 0);
        ptr.set(resultPtr);
        return true;
    }

    @Override
    public final PDataType getDataType() {
        return PTimestamp.INSTANCE;
    }

    @Override
    public ArithmeticExpression clone(List<Expression> children) {
        return new TimestampSubtractExpression(children);
    }
}
