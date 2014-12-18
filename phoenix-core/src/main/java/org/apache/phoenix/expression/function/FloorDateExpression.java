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
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.phoenix.expression.CoerceExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PUnsignedDate;
import org.apache.phoenix.schema.types.PUnsignedTimestamp;

/**
 * 
 * Class encapsulating the FLOOR operation on 
 * a column/literal of type {@link org.apache.phoenix.schema.types.PDate}.
 *
 * 
 * @since 3.0.0
 */
public class FloorDateExpression extends RoundDateExpression {
    
    public FloorDateExpression() {}
    
    private FloorDateExpression(List<Expression> children) {
        super(children);
    }
    
    public static Expression create(List<Expression> children) throws SQLException {
        Expression firstChild = children.get(0);
        PDataType firstChildDataType = firstChild.getDataType();
        if (firstChildDataType == PTimestamp.INSTANCE || firstChildDataType == PUnsignedTimestamp.INSTANCE){
            // Coerce TIMESTAMP to DATE, as the nanos has no affect
            List<Expression> newChildren = Lists.newArrayListWithExpectedSize(children.size());
            newChildren.add(CoerceExpression.create(firstChild, firstChildDataType == PTimestamp.INSTANCE ? PDate.INSTANCE : PUnsignedDate.INSTANCE));
            newChildren.addAll(children.subList(1, children.size()));
            children = newChildren;
        }
        return new FloorDateExpression(children);
    }
    
    /**
     * @param timeUnit - unit of time to round up to.
     * Creates a {@link FloorDateExpression} with default multiplier of 1.
     */
    public static Expression create(Expression expr, TimeUnit timeUnit) throws SQLException {
        return create(expr, timeUnit, 1);
    }
    
    /**
     * @param timeUnit - unit of time to round up to
     * @param multiplier - determines the roll up window size.
     * Create a {@link FloorDateExpression}. 
     */
    public static Expression create(Expression expr, TimeUnit timeUnit, int multiplier) throws SQLException {
        Expression timeUnitExpr = getTimeUnitExpr(timeUnit);
        Expression defaultMultiplierExpr = getMultiplierExpr(multiplier);
        List<Expression> expressions = Lists.newArrayList(expr, timeUnitExpr, defaultMultiplierExpr);
        return create(expressions);
    }
   
    @Override
    protected long getRoundUpAmount() {
        return 0;
    }
    
    @Override
    public String getName() {
        return FloorFunction.NAME;
    }
}
