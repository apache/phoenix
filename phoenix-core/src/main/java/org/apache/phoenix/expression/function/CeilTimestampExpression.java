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
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PDataType.PDataCodec;
import org.apache.phoenix.schema.tuple.Tuple;

/**
 * 
 * Class encapsulating the CEIL operation on {@link org.apache.phoenix.schema.PDataType#TIMESTAMP}
 * This class only supports CEIL {@link TimeUnit#MILLISECOND}. If you want more options of CEIL like 
 * using {@link TimeUnit#HOUR} use {@link CeilDateExpression}
 * 
 * 
 * @since 3.0.0
 */
public class CeilTimestampExpression extends CeilDateExpression {
    
    public CeilTimestampExpression() {}
    
    private CeilTimestampExpression(List<Expression> children) {
        super(children);
    }
    
    /**
     * Creates a {@link CeilTimestampExpression} that uses {@link TimeUnit#MILLISECOND} 
     * as the time unit for rounding. 
     */
    public static CeilTimestampExpression create(Expression expr, int multiplier) throws SQLException {
        List<Expression> childExprs = Lists.newArrayList(expr, getTimeUnitExpr(TimeUnit.MILLISECOND), getMultiplierExpr(multiplier));
        return new CeilTimestampExpression(childExprs); 
    }
    
    public static Expression create(List<Expression> children) throws SQLException {
        Expression firstChild = children.get(0);
        PDataType firstChildDataType = firstChild.getDataType();
        String timeUnit = (String)((LiteralExpression)children.get(1)).getValue();
        if(TimeUnit.MILLISECOND.toString().equalsIgnoreCase(timeUnit)) {
            return new CeilTimestampExpression(children);
        }
        // Coerce TIMESTAMP to DATE, as the nanos has no affect
        List<Expression> newChildren = Lists.newArrayListWithExpectedSize(children.size());
        newChildren.add(CoerceExpression.create(firstChild, firstChildDataType == PDataType.TIMESTAMP ? PDataType.DATE : PDataType.UNSIGNED_DATE));
        newChildren.addAll(children.subList(1, children.size()));
        return CeilDateExpression.create(newChildren);
    }
    
    /**
     * Creates a {@link CeilTimestampExpression} that uses {@link TimeUnit#MILLISECOND} 
     * as the time unit for rounding. 
     */
    public static CeilTimestampExpression create (Expression expr) throws SQLException {
        return create(expr, 1);
    }

    @Override
    protected PDataCodec getKeyRangeCodec(PDataType columnDataType) {
        return columnDataType == PDataType.TIMESTAMP 
                ? PDataType.DATE.getCodec() 
                : columnDataType == PDataType.UNSIGNED_TIMESTAMP 
                    ? PDataType.UNSIGNED_DATE.getCodec() 
                    : super.getKeyRangeCodec(columnDataType);
    }
    
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (children.get(0).evaluate(tuple, ptr)) {
            SortOrder sortOrder = children.get(0).getSortOrder();
            PDataType dataType = getDataType();
            int nanos = dataType.getNanos(ptr, sortOrder);
            if (nanos > 0) {
                long millis = dataType.getMillis(ptr, sortOrder); 
                Timestamp roundedTs = new Timestamp(millis + 1);
                byte[] byteValue = dataType.toBytes(roundedTs);
                ptr.set(byteValue);
            }
            return true; // for timestamp we only support rounding up the milliseconds.
        }
        return false;
    }   

}
