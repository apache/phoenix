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
import java.sql.Timestamp;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.google.common.collect.Lists;
import org.apache.phoenix.expression.CoerceExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PUnsignedDate;
import org.apache.phoenix.schema.types.PUnsignedTimestamp;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDataType.PDataCodec;
import org.apache.phoenix.schema.tuple.Tuple;

/**
 * 
 * Class encapsulating the process for rounding off a column/literal of 
 * type {@link org.apache.phoenix.schema.types.PTimestamp}
 * This class only supports rounding off the milliseconds that is for
 * {@link TimeUnit#MILLISECOND}. If you want more options of rounding like 
 * using {@link TimeUnit#HOUR} use {@link RoundDateExpression}
 *
 * 
 * @since 3.0.0
 */

public class RoundTimestampExpression extends RoundDateExpression {
    
    private static final long HALF_OF_NANOS_IN_MILLI = java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(1)/2;

    public RoundTimestampExpression() {}
    
    private RoundTimestampExpression(List<Expression> children) {
        super(children);
    }
    
    public static Expression create (List<Expression> children) throws SQLException {
        Expression firstChild = children.get(0);
        PDataType firstChildDataType = firstChild.getDataType();
        String timeUnit = (String)((LiteralExpression)children.get(1)).getValue();
        LiteralExpression multiplierExpr = (LiteralExpression)children.get(2);
        
        /*
         * When rounding off timestamp to milliseconds, nanos play a part only when the multiplier value
         * is equal to 1. This is because for cases when multiplier value is greater than 1, number of nanos/multiplier
         * will always be less than half the nanos in a millisecond. 
         */
        if((timeUnit == null || TimeUnit.MILLISECOND.toString().equalsIgnoreCase(timeUnit)) && ((Number)multiplierExpr.getValue()).intValue() == 1) {
            return new RoundTimestampExpression(children);
        }
        // Coerce TIMESTAMP to DATE, as the nanos has no affect
        List<Expression> newChildren = Lists.newArrayListWithExpectedSize(children.size());
        newChildren.add(CoerceExpression.create(firstChild, firstChildDataType == PTimestamp.INSTANCE ?
            PDate.INSTANCE : PUnsignedDate.INSTANCE));
        newChildren.addAll(children.subList(1, children.size()));
        return RoundDateExpression.create(newChildren);
    }
    
    @Override
    protected PDataCodec getKeyRangeCodec(PDataType columnDataType) {
        return columnDataType == PTimestamp.INSTANCE
                ? PDate.INSTANCE.getCodec()
                : columnDataType == PUnsignedTimestamp.INSTANCE
                    ? PUnsignedDate.INSTANCE.getCodec()
                    : super.getKeyRangeCodec(columnDataType);
    }
    
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (children.get(0).evaluate(tuple, ptr)) {
            SortOrder sortOrder = children.get(0).getSortOrder();
            PDataType dataType = getDataType();
            int nanos = dataType.getNanos(ptr, sortOrder);
            if(nanos >= HALF_OF_NANOS_IN_MILLI) {
                long timeMillis = dataType.getMillis(ptr, sortOrder);
                Timestamp roundedTs = new Timestamp(timeMillis + 1);
                byte[] byteValue = dataType.toBytes(roundedTs);
                ptr.set(byteValue);
            }
            return true; // for timestamp we only support rounding up the milliseconds. 
        }
        return false;
    }

}
