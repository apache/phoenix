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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;

import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderPreservingTracker.Ordering;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.OrderByExpression;
import org.apache.phoenix.iterate.OrderedResultIterator;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.parse.OrderByNode;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.RowValueConstructorOffsetNotAllowedInQueryException;
import org.apache.phoenix.schema.RowValueConstructorOffsetNotCoercibleException;

import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Sets;

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
        
        public OrderBy(List<OrderByExpression> orderByExpressions) {
            this.orderByExpressions = ImmutableList.copyOf(orderByExpressions);
        }

        public List<OrderByExpression> getOrderByExpressions() {
            return orderByExpressions;
        }

        public boolean isEmpty() {
            return this.orderByExpressions == null || this.orderByExpressions.isEmpty();
        }

        public static List<OrderBy> wrapForOutputOrderBys(OrderBy orderBy) {
            assert orderBy != OrderBy.FWD_ROW_KEY_ORDER_BY && orderBy != OrderBy.REV_ROW_KEY_ORDER_BY;
            if(orderBy == null || orderBy == OrderBy.EMPTY_ORDER_BY) {
                return Collections.<OrderBy> emptyList();
            }
            return Collections.<OrderBy> singletonList(orderBy);
        }

        /**
         * When we compile {@link OrderByNode} in {@link OrderByCompiler#compile}, we invoke {@link OrderByExpression#createByCheckIfExpressionSortOrderDesc}
         * to get the compiled {@link OrderByExpression} for using it in {@link OrderedResultIterator}, but for {@link QueryPlan#getOutputOrderBys()},
         * the returned {@link OrderByExpression} is used for {@link OrderPreservingTracker}, so we should invoke {@link OrderByExpression#createByCheckIfExpressionSortOrderDesc}
         * again to the actual {@link OrderByExpression}.
         * @return
         */
        public static OrderBy convertCompiledOrderByToOutputOrderBy(OrderBy orderBy) {
            if(orderBy.isEmpty()) {
                return orderBy;
            }
            List<OrderByExpression> orderByExpressions = orderBy.getOrderByExpressions();
            List<OrderByExpression> newOrderByExpressions = new ArrayList<OrderByExpression>(orderByExpressions.size());
            for(OrderByExpression orderByExpression : orderByExpressions) {
                OrderByExpression newOrderByExpression =
                        OrderByExpression.convertIfExpressionSortOrderDesc(orderByExpression);
                newOrderByExpressions.add(newOrderByExpression);
            }
            return new OrderBy(newOrderByExpressions);
        }

        public static boolean equalsForOutputOrderBy(OrderBy orderBy1, OrderBy orderBy2) {
            return Objects.equals(orderBy1.orderByExpressions, orderBy2.orderByExpressions);
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
                                  SelectStatement statement,
                                  GroupBy groupBy,
                                  Integer limit,
                                  CompiledOffset offset,
                                  RowProjector rowProjector,
                                  QueryPlan innerQueryPlan,
                                  Expression whereExpression) throws SQLException {
        List<OrderByNode> orderByNodes = statement.getOrderBy();
        if (orderByNodes.isEmpty()) {
            return OrderBy.EMPTY_ORDER_BY;
        }
        // for ungroupedAggregates as GROUP BY expression, check against an empty group by
        ExpressionCompiler compiler;
        if (groupBy.isUngroupedAggregate()) {
            compiler = new StatelessExpressionCompiler(context, GroupBy.EMPTY_GROUP_BY);
        } else {
            compiler = new ExpressionCompiler(context, groupBy);
        }
        OrderPreservingTracker tracker = null;
        if(isTrackOrderByPreserving(statement)) {
            // accumulate columns in ORDER BY
            tracker = new OrderPreservingTracker(
                            context,
                            groupBy,
                            Ordering.ORDERED,
                            orderByNodes.size(),
                            null,
                            innerQueryPlan,
                            whereExpression);
        }

        LinkedHashSet<OrderByExpression> orderByExpressions = Sets.newLinkedHashSetWithExpectedSize(orderByNodes.size());
        for (OrderByNode node : orderByNodes) {
            Expression expression = null;
            if (node.isIntegerLiteral()) {
                if (rowProjector == null) {
                    throw new IllegalStateException(
                            "rowProjector is null when there is LiteralParseNode in orderByNodes");
                }
                Integer index = node.getValueIfIntegerLiteral();
                assert index != null;
                int size = rowProjector.getColumnProjectors().size();
                if (index > size || index <= 0 ) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.PARAM_INDEX_OUT_OF_BOUND)
                    .build().buildException();
                }
                expression = rowProjector.getColumnProjector(index-1).getExpression();
            } else {
                expression = node.getNode().accept(compiler);
                // Detect mix of aggregate and non aggregates (i.e. ORDER BY txns, SUM(txns)
                if (!expression.isStateless() && !compiler.isAggregate()) {
                    if (statement.isAggregate() || statement.isDistinct()) {
                        // Detect ORDER BY not in SELECT DISTINCT: SELECT DISTINCT count(*) FROM t ORDER BY x
                        if (statement.isDistinct()) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.ORDER_BY_NOT_IN_SELECT_DISTINCT)
                            .setMessage(expression.toString()).build().buildException();
                        }
                        ExpressionCompiler.throwNonAggExpressionInAggException(expression.toString());
                    }
                }
            }
            if (!expression.isStateless()) {
                boolean isAscending = node.isAscending();
                boolean isNullsLast = node.isNullsLast();
                if(tracker != null) {
                    tracker.track(expression, isAscending, isNullsLast);
                }
                /**
                 * If we have a schema where column A is DESC, reverse the sort order
                 * since this is the order they actually are in.
                 * Reverse is required because the compiled OrderByExpression is used in {@link OrderedResultIterator},
                 * {@link OrderedResultIterator} implements the compare based on binary representation, not the decoded value of corresponding dataType.
                 */
                OrderByExpression orderByExpression = OrderByExpression.createByCheckIfExpressionSortOrderDesc(
                        expression,
                        isNullsLast,
                        isAscending);
                orderByExpressions.add(orderByExpression);
            }
            compiler.reset();
        }

        //If we are not ordered we shouldn't be using RVC Offset
        //I think this makes sense for the pagination case but perhaps we can relax this for
        //other use cases.
        //Note If the table is salted we still mark as row ordered in this code path
        if (offset.getByteOffset().isPresent() && orderByExpressions.isEmpty()) {
            throw new RowValueConstructorOffsetNotAllowedInQueryException(
                    "RVC OFFSET requires either forceRowKeyOrder or explict ORDERBY with row key order");
        }

        // we can remove ORDER BY clauses in case of only COUNT(DISTINCT...) clauses
        if (orderByExpressions.isEmpty() || groupBy.isUngroupedAggregate()) {
            return OrderBy.EMPTY_ORDER_BY;
        }
        // If we're ordering by the order returned by the scan, we don't need an order by
        if (tracker != null && tracker.isOrderPreserving()) {
            if (tracker.isReverse()) {
                // Don't use reverse scan if:
                // 1) we're using a skip scan, as our skip scan doesn't support this yet.
                // 2) we have the FORWARD_SCAN hint set to choose to keep loading of column
                //    families on demand versus doing a reverse scan
                // REV_ROW_KEY_ORDER_BY scan would not take effect for a projected table, so don't return it for such table types.
                if (context.getConnection().getQueryServices().getProps().getBoolean(QueryServices.USE_REVERSE_SCAN_ATTRIB, QueryServicesOptions.DEFAULT_USE_REVERSE_SCAN)
                        && !context.getScanRanges().useSkipScanFilter()
                        && context.getCurrentTable().getTable().getType() != PTableType.PROJECTED
                        && context.getCurrentTable().getTable().getType() != PTableType.SUBQUERY
                        && !statement.getHint().hasHint(Hint.FORWARD_SCAN)) {
                    if(offset.getByteOffset().isPresent()){
                        throw new SQLException("Do not allow non-pk ORDER BY with RVC OFFSET");
                    }
                    return OrderBy.REV_ROW_KEY_ORDER_BY;
                }
            } else {
                return OrderBy.FWD_ROW_KEY_ORDER_BY;
            }
        }
        //If we were in row order this would be optimized out above
        if(offset.getByteOffset().isPresent()){
            throw new RowValueConstructorOffsetNotCoercibleException("Do not allow non-pk ORDER BY with RVC OFFSET");
        }
        return new OrderBy(Lists.newArrayList(orderByExpressions.iterator()));
    }

    public static boolean isTrackOrderByPreserving(SelectStatement selectStatement) {
        return !selectStatement.isUnion();
    }

    private OrderByCompiler() {
    }
}
