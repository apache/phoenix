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
package org.apache.phoenix.compile;


import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.TrackOrderPreservingExpressionCompiler.Ordering;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.OrderByExpression;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.parse.OrderByNode;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.SortOrder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Validates ORDER BY clause and builds up a list of referenced columns.
 * 
 * 
 * @since 0.1
 */
public class OrderByCompiler {
    public static class OrderBy {
        public static final OrderBy EMPTY_ORDER_BY = new OrderBy(Collections.<OrderByExpression>emptyList());
        /**
         * Used to indicate that there was an ORDER BY, but it was optimized out because
         * rows are already returned in this order. 
         */
        public static final OrderBy FWD_ROW_KEY_ORDER_BY = new OrderBy(Collections.<OrderByExpression>emptyList());
        public static final OrderBy REV_ROW_KEY_ORDER_BY = new OrderBy(Collections.<OrderByExpression>emptyList());
        
        private final List<OrderByExpression> orderByExpressions;
        
        private OrderBy(List<OrderByExpression> orderByExpressions) {
            this.orderByExpressions = ImmutableList.copyOf(orderByExpressions);
        }

        public List<OrderByExpression> getOrderByExpressions() {
            return orderByExpressions;
        }
    }
    /**
     * Gets a list of columns in the ORDER BY clause
     * @param context the query context for tracking various states
     * associated with the given select statement
     * @param statement TODO
     * @param groupBy the list of columns in the GROUP BY clause
     * @param limit the row limit or null if no limit
     * @return the compiled ORDER BY clause
     * @throws SQLException
     */
    public static OrderBy compile(StatementContext context,
                                  FilterableStatement statement,
                                  GroupBy groupBy, Integer limit, 
                                  boolean isInRowKeyOrder) throws SQLException {
        List<OrderByNode> orderByNodes = statement.getOrderBy();
        if (orderByNodes.isEmpty()) {
            return OrderBy.EMPTY_ORDER_BY;
        }
        // accumulate columns in ORDER BY
        TrackOrderPreservingExpressionCompiler visitor = 
                new TrackOrderPreservingExpressionCompiler(context, groupBy, 
                        orderByNodes.size(), Ordering.ORDERED, null);
        LinkedHashSet<OrderByExpression> orderByExpressions = Sets.newLinkedHashSetWithExpectedSize(orderByNodes.size());
        for (OrderByNode node : orderByNodes) {
            boolean isAscending = node.isAscending();
            Expression expression = node.getNode().accept(visitor);
            if (!expression.isStateless() && visitor.addEntry(expression, isAscending ? SortOrder.ASC : SortOrder.DESC)) {
                // Detect mix of aggregate and non aggregates (i.e. ORDER BY txns, SUM(txns)
                if (!visitor.isAggregate()) {
                    if (statement.isAggregate() || statement.isDistinct()) {
                        // Detect ORDER BY not in SELECT DISTINCT: SELECT DISTINCT count(*) FROM t ORDER BY x
                        if (statement.isDistinct()) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.ORDER_BY_NOT_IN_SELECT_DISTINCT)
                            .setMessage(expression.toString()).build().buildException();
                        }
                        ExpressionCompiler.throwNonAggExpressionInAggException(expression.toString());
                    }
                }
                if (expression.getSortOrder() == SortOrder.DESC) {
                    isAscending = !isAscending;
                }
                OrderByExpression orderByExpression = new OrderByExpression(expression, node.isNullsLast(), isAscending);
                orderByExpressions.add(orderByExpression);
            }
            visitor.reset();
        }
       
        if (orderByExpressions.isEmpty()) {
            return OrderBy.EMPTY_ORDER_BY;
        }
        // If we're ordering by the order returned by the scan, we don't need an order by
        if (isInRowKeyOrder && visitor.isOrderPreserving()) {
            if (visitor.isReverse()) {
                // Don't use reverse scan if we're using a skip scan, as our skip scan doesn't support this yet.
                // REV_ROW_KEY_ORDER_BY scan would not take effect for a projected table, so don't return it for such table types.
                if (context.getConnection().getQueryServices().getProps().getBoolean(QueryServices.USE_REVERSE_SCAN_ATTRIB, QueryServicesOptions.DEFAULT_USE_REVERSE_SCAN)
                        && !context.getScanRanges().useSkipScanFilter()
                        && context.getCurrentTable().getTable().getType() != PTableType.PROJECTED
                        && context.getCurrentTable().getTable().getType() != PTableType.SUBQUERY) {
                    return OrderBy.REV_ROW_KEY_ORDER_BY;
                }
            } else {
                return OrderBy.FWD_ROW_KEY_ORDER_BY;
            }
        }

        return new OrderBy(Lists.newArrayList(orderByExpressions.iterator()));
    }


    private OrderByCompiler() {
    }
}