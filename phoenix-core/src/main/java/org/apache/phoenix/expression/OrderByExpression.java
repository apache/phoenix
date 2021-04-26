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

import static org.apache.phoenix.thirdparty.com.google.common.base.Preconditions.checkNotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.compile.OrderByCompiler;
import org.apache.phoenix.compile.OrderPreservingTracker.Info;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.execute.AggregatePlan;

/**
 * A container for a column that appears in ORDER BY clause.
 */
public class OrderByExpression implements Writable {
    private Expression expression;
    private boolean isNullsLast;
    private boolean isAscending;
    
    public OrderByExpression() {
    }

    private OrderByExpression(Expression expression, boolean isNullsLast, boolean isAcending) {
        checkNotNull(expression);
        this.expression = expression;
        this.isNullsLast = isNullsLast;
        this.isAscending = isAcending;
    }

    /**
     * If {@link Expression#getSortOrder()} is {@link SortOrder#DESC},the isAscending of returned new is reversed,but isNullsLast is untouched.
     * @param orderByExpression
     * @return
     */
    public static OrderByExpression convertIfExpressionSortOrderDesc(OrderByExpression orderByExpression) {
        return createByCheckIfExpressionSortOrderDesc(
                orderByExpression.getExpression(),
                orderByExpression.isNullsLast(),
                orderByExpression.isAscending());
    }

    /**
     * If {@link Expression#getSortOrder()} is {@link SortOrder#DESC},reverse the isAscending,but isNullsLast is untouched.
     * A typical case is in OrderByCompiler#compile to get the compiled OrderByExpression to used for OrderedResultIterator.
     * @param expression
     * @param isNullsLast
     * @param isAscending
     * @return
     */
    public static OrderByExpression createByCheckIfExpressionSortOrderDesc(Expression expression, boolean isNullsLast, boolean isAscending) {
        if(expression.getSortOrder() == SortOrder.DESC) {
            isAscending = !isAscending;
        }
        return new OrderByExpression(expression, isNullsLast, isAscending);
    }

    /**
     * If orderByReverse is true, reverse the isNullsLast and isAscending.
     * A typical case is in AggregatePlan.OrderingResultIteratorFactory#newIterator
     * @param expression
     * @param isNullsLast
     * @param isAscending
     * @param orderByReverse
     * @return
     */
    public static OrderByExpression createByCheckIfOrderByReverse(Expression expression, boolean isNullsLast, boolean isAscending, boolean orderByReverse) {
        if(orderByReverse) {
            isNullsLast = !isNullsLast;
            isAscending = !isAscending;
        }
        return new OrderByExpression(expression, isNullsLast, isAscending);
    }

    /**
     * Create OrderByExpression from expression,isNullsLast is the default value "false",isAscending is based on {@link Expression#getSortOrder()}.
     * If orderByReverse is true, reverses the isNullsLast and isAscending.
     * @param expression
     * @param orderByReverse
     * @return
     */
    public static OrderByExpression convertExpressionToOrderByExpression(Expression expression, boolean orderByReverse) {
      return convertExpressionToOrderByExpression(expression, null, orderByReverse);
    }

    /**
     * Create OrderByExpression from expression, if the orderPreservingTrackInfo is not null, use isNullsLast and isAscending from orderPreservingTrackInfo.
     * If orderByReverse is true, reverses the isNullsLast and isAscending.
     * @param expression
     * @param orderPreservingTrackInfo
     * @param orderByReverse
     * @return
     */
    public static OrderByExpression convertExpressionToOrderByExpression(
            Expression expression,
            Info orderPreservingTrackInfo,
            boolean orderByReverse) {
        boolean isNullsLast = false;
        boolean isAscending = expression.getSortOrder() == SortOrder.ASC;
        if(orderPreservingTrackInfo != null) {
            isNullsLast = orderPreservingTrackInfo.isNullsLast();
            isAscending = orderPreservingTrackInfo.isAscending();
        }
        return OrderByExpression.createByCheckIfOrderByReverse(expression, isNullsLast, isAscending, orderByReverse);
    }

    public Expression getExpression() {
        return expression;
    }
    
    public boolean isNullsLast() {
        return isNullsLast;
    }
    
    public boolean isAscending() {
        return isAscending;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o != null && this.getClass() == o.getClass()) {
            OrderByExpression that = (OrderByExpression)o;
            return isNullsLast == that.isNullsLast
                && isAscending == that.isAscending
                && expression.equals(that.expression);
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (isNullsLast ? 0 : 1);
        result = prime * result + (isAscending ? 0 : 1);
        result = prime * result + expression.hashCode();
        return result;
    }
    
    @Override
    public String toString() {
        Expression e = this.getExpression();
        boolean isNullsLast = this.isNullsLast;
        boolean isAscending = this.isAscending;
        // Flip back here based on sort order, as the compiler
        // flips this, but we want to display the original back
        // to the user.
        if (e.getSortOrder() == SortOrder.DESC) {
            isAscending = !isAscending;
        }
        return e + (isAscending ? "" : " DESC") + (isNullsLast ? " NULLS LAST" : "");
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
        this.isNullsLast = input.readBoolean();
        this.isAscending = input.readBoolean();
        expression = ExpressionType.values()[WritableUtils.readVInt(input)].newInstance();
        expression.readFields(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeBoolean(isNullsLast);
        output.writeBoolean(isAscending);
        WritableUtils.writeVInt(output, ExpressionType.valueOf(expression).ordinal());
        expression.write(output);
    }

}