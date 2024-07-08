/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.phoenix.compile;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.execute.SortMergeJoinPlan;
import org.apache.phoenix.expression.CoerceExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.OrderByExpression;
import org.apache.phoenix.expression.ProjectedColumnExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.expression.RowValueConstructorExpression;
import org.apache.phoenix.expression.function.FunctionExpression.OrderPreserving;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.expression.visitor.StatelessTraverseAllExpressionVisitor;
import org.apache.phoenix.expression.visitor.StatelessTraverseNoExpressionVisitor;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.ExpressionUtil;

import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.phoenix.thirdparty.com.google.common.collect.Iterators;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

/**
 * <pre>
 * Determines if the natural key order of the rows returned by the scan
 * will match the order of the expressions. For GROUP BY, if order is preserved we can use
 * an optimization during server-side aggregation to do the aggregation on-the-fly versus
 * keeping track of each distinct group. We can only do this optimization if all the rows
 * for each group will be contiguous. For ORDER BY, we can drop the ORDER BY statement if
 * the order is preserved.
 * 
 * There are mainly four changes for refactoring this class after PHOENIX-5148:
 * 1.Add a {@link #getInputOrderBys} method to determine the input OrderBys through
 *   innerQueryPlan's output OrderBys, GroupBy of current QueryPlan or the rowKeyColumns of
 *   current table.
 *
 * 2.Because the innerQueryPlan may have multiple output OrderBys(eg. {@link SortMergeJoinPlan}),
 *   so many stateful member variables (such as orderPreservingTrackInfos, isOrderPreserving
 *   and isReverse etc.) are extracted to a new inner class {@link TrackOrderByContext}, {@link TrackOrderByContext}
 *   is used to track if a single Input OrderBy matches the target OrderByExpressions in {@link #isOrderPreserving()}
 *   method, and once we found a  {@link TrackOrderByContext} satisfied {@link #isOrderPreserving()},
 *   {@link OrderPreservingTracker#selectedTrackOrderByContext} member variable is set to this {@link TrackOrderByContext},
 *   then we can use this {@link #selectedTrackOrderByContext} to serve the {@link OrderPreservingTracker#getOrderPreservingTrackInfos}
 *   and {@link #isReverse()} methods etc.
 *   BTW. at most only one {@link TrackOrderByContext} can meet {@link #isOrderPreserving()} is true.
 *
 * 3.Add ascending and nullsLast to the inner class {@link Info} , and extracted complete ordering
 *   information in {@link Info} class by the inner {@link TrackOrderPreservingExpressionVisitor} class,
 *   so we can inferring alignment between the target OrderByExpressions and the input OrderBys based on
 *   {@link Info}, not on the row keys like the original method does.
 *
 * 4.{@link OrderPreservingTracker#getOrderPreservingTrackInfos} could extract partial continuous ordering columns which start from the
 *   first column even if {@link OrderPreservingTracker#isOrderPreserving} is false.
 * </pre>
 */
public class OrderPreservingTracker {
    public enum Ordering {ORDERED, UNORDERED};

    public static class Info {
        private final OrderPreserving orderPreserving;
        private final int pkPosition;
        private final int slotSpan;
        private final boolean ascending;
        private final boolean nullsLast;
        private TrackingOrderByExpression trackingOrderByExpression;

        public Info(int pkPosition, boolean ascending, boolean nullsLast) {
            this.pkPosition = pkPosition;
            this.orderPreserving = OrderPreserving.YES;
            this.slotSpan = 1;
            this.ascending = ascending;
            this.nullsLast = nullsLast;
        }

        public Info(int rowKeyColumnPosition, int rowKeySlotSpan, OrderPreserving orderPreserving, boolean ascending, boolean nullsLast) {
            this.pkPosition = rowKeyColumnPosition;
            this.slotSpan = rowKeySlotSpan;
            this.orderPreserving = orderPreserving;
            this.ascending = ascending;
            this.nullsLast = nullsLast;
        }

        public static List<Expression> extractExpressions(List<Info> orderPreservingTrackInfos) {
            List<Expression> newExpressions = new ArrayList<Expression>(orderPreservingTrackInfos.size());
            for(Info trackInfo : orderPreservingTrackInfos) {
                newExpressions.add(trackInfo.getExpression());
            }
            return newExpressions;
        }

        public Expression getExpression() {
            return this.trackingOrderByExpression.expression;
        }

        public boolean isAscending() {
            return ascending;
        }

        public boolean isNullsLast() {
            return nullsLast;
        }
    }
    private final StatementContext context;
    private final GroupBy groupBy;
    private final Ordering ordering;
    private final int pkPositionOffset;
    private Expression whereExpression;
    private List<TrackingOrderByExpression> trackingOrderByExpressions =
            new LinkedList<TrackingOrderByExpression>();
    private final List<TrackOrderByContext> trackOrderByContexts;
    private TrackOrderByContext selectedTrackOrderByContext = null;
    private final List<OrderBy> inputOrderBys;

    public OrderPreservingTracker(StatementContext context, GroupBy groupBy, Ordering ordering, int nNodes) throws SQLException {
        this(context, groupBy, ordering, nNodes, null, null, null);
    }

    public OrderPreservingTracker(
            StatementContext context,
            GroupBy groupBy,
            Ordering ordering,
            int nNodes,
            List<OrderBy> inputOrderBys,
            QueryPlan innerQueryPlan,
            Expression whereExpression) throws SQLException {

        this.context = context;
        this.groupBy = groupBy;
        this.ordering = ordering;
        this.whereExpression = whereExpression;
        if (inputOrderBys != null) {
            this.inputOrderBys = inputOrderBys;
            this.pkPositionOffset = 0;
        } else {
            Pair<List<OrderBy>, Integer> orderBysAndRowKeyColumnOffset =
                    getInputOrderBys(innerQueryPlan, groupBy, context);
            this.inputOrderBys = orderBysAndRowKeyColumnOffset.getFirst();
            this.pkPositionOffset = orderBysAndRowKeyColumnOffset.getSecond();
        }

        if (this.inputOrderBys.isEmpty()) {
            this.trackOrderByContexts = Collections.emptyList();
            return;
        }

        this.trackOrderByContexts = new ArrayList<TrackOrderByContext>(this.inputOrderBys.size());
        for(OrderBy inputOrderBy : this.inputOrderBys) {
            this.trackOrderByContexts.add(
                    new TrackOrderByContext(nNodes, inputOrderBy));
        }
    }

    /**
     * Infer input OrderBys, if the innerQueryPlan is null, we make the OrderBys from the pk columns of {@link PTable}.
     */
    private static Pair<List<OrderBy>, Integer> getInputOrderBys(
            QueryPlan innerQueryPlan,
            GroupBy groupBy,
            StatementContext statementContext) throws SQLException {
        if (!groupBy.isEmpty()) {
            return Pair.newPair(
                    Collections.singletonList(
                            ExpressionUtil.convertGroupByToOrderBy(groupBy, false)), 0);
        }
        if (innerQueryPlan != null) {
            return Pair.newPair(innerQueryPlan.getOutputOrderBys(), 0);
        }

        TableRef tableRef = statementContext.getResolver().getTables().get(0);
        if (!tableRef.getTable().rowKeyOrderOptimizable()) {
            return Pair.newPair(Collections.<OrderBy>emptyList(), 0);
        }
        PhoenixConnection phoenixConnection = statementContext.getConnection();
        Pair<OrderBy, Integer> orderByAndRowKeyColumnOffset =
                ExpressionUtil.getOrderByFromTable(tableRef, phoenixConnection, false);
        OrderBy orderBy = orderByAndRowKeyColumnOffset.getFirst();
        Integer rowKeyColumnOffset = orderByAndRowKeyColumnOffset.getSecond();
        return Pair.newPair(
                    orderBy != OrderBy.EMPTY_ORDER_BY
                    ? Collections.singletonList(orderBy)
                    : Collections.<OrderBy>emptyList(),
                    rowKeyColumnOffset);
    }

    private class TrackOrderByContext {
        private final List<Info> orderPreservingTrackedInfos;
        private boolean isOrderPreserving = true;
        private Boolean isReverse = null;
        private int orderPreservingColumnCount = 0;
        private int orderedTrackedInfosCount = 0;
        private final TrackOrderPreservingExpressionVisitor trackOrderPreservingExpressionVisitor;
        private final OrderBy inputOrderBy;
        private int trackingOrderByExpressionCount = 0;
        private boolean isOrderPreservingCalled = false;

        TrackOrderByContext(int orderByNodeCount, OrderBy inputOrderBy) {
            this.trackOrderPreservingExpressionVisitor = new TrackOrderPreservingExpressionVisitor(inputOrderBy);
            this.orderPreservingTrackedInfos = Lists.newArrayListWithExpectedSize(orderByNodeCount);
            this.inputOrderBy = inputOrderBy;
        }

        public void track(List<TrackingOrderByExpression> trackingOrderByExpressions) {
            this.trackingOrderByExpressionCount = trackingOrderByExpressions.size();
            trackingOrderByExpressions.forEach(trackingOrderByExpression -> {
                Expression expression = trackingOrderByExpression.expression;
                Info trackedInfo = expression.accept(trackOrderPreservingExpressionVisitor);
                if (trackedInfo != null) {
                    trackedInfo.trackingOrderByExpression = trackingOrderByExpression;
                    orderPreservingTrackedInfos.add(trackedInfo);
                }
            });
        }

        private void checkAscendingAndNullsLast(Info trackedInfo) {
            TrackingOrderByExpression trackingOrderByExpression =
                    trackedInfo.trackingOrderByExpression;
            Expression expression = trackingOrderByExpression.expression;
            Boolean isAscending = trackingOrderByExpression.isAscending;
            Boolean isNullsLast = trackingOrderByExpression.isNullsLast;

            // If the expression is sorted in a different order than the specified sort order
            // then the expressions are not order preserving.
            if (isAscending != null && trackedInfo.ascending != isAscending.booleanValue()) {
                if (isReverse == null) {
                    isReverse = true;
                } else if (!isReverse){
                    isOrderPreserving = false;
                    isReverse = false;
                    return;
                }
            } else {
                if (isReverse == null) {
                    isReverse = false;
                } else if (isReverse){
                    isOrderPreserving = false;
                    isReverse = false;
                    return;
                }
            }

            assert isReverse != null;
            if (isNullsLast != null && expression.isNullable()) {
                if (trackedInfo.nullsLast == isNullsLast.booleanValue()
                        && isReverse.booleanValue()
                    || trackedInfo.nullsLast != isNullsLast.booleanValue()
                        && !isReverse.booleanValue()) {
                    isOrderPreserving = false;
                    isReverse = false;
                    return;
                }
            }
        }

        /**
         * Only valid AFTER call to isOrderPreserving.
         * This value represents the input column count of {@link TrackOrderByContext#inputOrderBy}
         * corresponding to longest continuous ordering columns returned by
         * {@link TrackOrderByContext#getOrderPreservingTrackInfos}, it may not equal to the size
         * of {@link TrackOrderByContext#getOrderPreservingTrackInfos}.
         */
        public int getOrderPreservingColumnCount() {
            if (!isOrderPreservingCalled) {
                throw new IllegalStateException(
                        "getOrderPreservingColumnCount must be called after isOrderPreserving is called!");
            }
            return orderPreservingColumnCount;
        }

        /**
         * Only valid AFTER call to isOrderPreserving
         */
        public List<Info> getOrderPreservingTrackInfos() {
            if (!isOrderPreservingCalled) {
                throw new IllegalStateException(
                        "getOrderPreservingTrackInfos must be called after isOrderPreserving is called!");
            }
            if (this.isOrderPreserving) {
                return ImmutableList.copyOf(this.orderPreservingTrackedInfos);
            }
            if (this.orderedTrackedInfosCount <= 0) {
                return Collections.<Info> emptyList();
            }
            return ImmutableList.copyOf(
                    this.orderPreservingTrackedInfos.subList(
                            0, this.orderedTrackedInfosCount));
        }

        public boolean isOrderPreserving() {
            if (this.isOrderPreservingCalled) {
                return isOrderPreserving;
            }

            if (ordering == Ordering.UNORDERED) {
                // Sort by position
                Collections.sort(orderPreservingTrackedInfos, new Comparator<Info>() {
                    @Override
                    public int compare(Info o1, Info o2) {
                        int cmp = o1.pkPosition - o2.pkPosition;
                        if (cmp != 0) return cmp;
                        // After pk position, sort on reverse OrderPreserving ordinal: NO, YES_IF_LAST, YES
                        // In this way, if we have an ORDER BY over a YES_IF_LAST followed by a YES, we'll
                        // allow subsequent columns to be ordered.
                        return o2.orderPreserving.ordinal() - o1.orderPreserving.ordinal();
                    }
                });
            }
            // Determine if there are any gaps in the PK columns (in which case we don't need
            // to sort in the coprocessor because the keys will already naturally be in sorted
            // order.
            int prevSlotSpan = 1;
            int prevPos =  -1;
            OrderPreserving prevOrderPreserving = OrderPreserving.YES;
            this.orderedTrackedInfosCount = 0;
            for (int i = 0; i < orderPreservingTrackedInfos.size(); i++) {
                Info entry = orderPreservingTrackedInfos.get(i);
                int pos = entry.pkPosition;
                this.checkAscendingAndNullsLast(entry);
                isOrderPreserving = isOrderPreserving &&
                        entry.orderPreserving != OrderPreserving.NO &&
                        prevOrderPreserving == OrderPreserving.YES &&
                        (pos == prevPos ||
                         pos - prevSlotSpan == prevPos  ||
                         hasEqualityConstraints(prevPos + prevSlotSpan, pos));
                if (!isOrderPreserving) {
                    break;
                }
                this.orderedTrackedInfosCount++;
                prevPos = pos;
                prevSlotSpan = entry.slotSpan;
                prevOrderPreserving = entry.orderPreserving;
            }
            isOrderPreserving = isOrderPreserving
                    && this.orderPreservingTrackedInfos.size()
                    == this.trackingOrderByExpressionCount;
            orderPreservingColumnCount = prevPos + prevSlotSpan + pkPositionOffset;
            this.isOrderPreservingCalled = true;
            return isOrderPreserving;
        }

        private boolean hasEqualityConstraints(int startPos, int endPos) {
            ScanRanges ranges = context.getScanRanges();
            // If a GROUP BY is being done, then the rows are ordered according to the GROUP BY key,
            // not by the original row key order of the table (see PHOENIX-3451).
            // We check each GROUP BY expression to see if it only references columns that are
            // matched by equality constraints, in which case the expression itself would be constant.
            for (int pos = startPos; pos < endPos; pos++) {
                Expression expressionToCheckConstant = this.getExpressionToCheckConstant(pos);
                IsConstantVisitor visitor = new IsConstantVisitor(ranges, whereExpression);
                Boolean isConstant = expressionToCheckConstant.accept(visitor);
                if (!Boolean.TRUE.equals(isConstant)) {
                    return false;
                }
            }
            return true;
        }

        public boolean isReverse() {
            if (!isOrderPreservingCalled) {
                throw new IllegalStateException(
                        "isReverse must be called after isOrderPreserving is called!");
            }
            if (!isOrderPreserving) {
                throw new IllegalStateException(
                        "isReverse should only be called when isOrderPreserving is true!");
            }
            return Boolean.TRUE.equals(isReverse);
        }

        private Expression getExpressionToCheckConstant(int columnIndex) {
            if (!groupBy.isEmpty()) {
                List<Expression> groupByExpressions = groupBy.getExpressions();
                assert columnIndex < groupByExpressions.size();
                return  groupByExpressions.get(columnIndex);
            }

            assert columnIndex < inputOrderBy.getOrderByExpressions().size();
            return inputOrderBy.getOrderByExpressions().get(columnIndex).getExpression();
        }
    }

    private static class TrackingOrderByExpression
    {
        private Expression expression;
        private Boolean isAscending;
        private Boolean isNullsLast;

        TrackingOrderByExpression(
                Expression expression, Boolean isAscending, Boolean isNullsLast) {
            this.expression = expression;
            this.isAscending = isAscending;
            this.isNullsLast = isNullsLast;
        }
    }

    public void track(Expression expression) {
        track(expression, null, null);
    }

    public void track(Expression expression, Boolean isAscending, Boolean isNullsLast) {
        TrackingOrderByExpression trackingOrderByExpression =
                new TrackingOrderByExpression(expression, isAscending, isNullsLast);
        this.trackingOrderByExpressions.add(trackingOrderByExpression);
    }

    /**
     * Only valid AFTER call to isOrderPreserving.
     * This value represents the input column count corresponding to longest continuous ordering
     * columns returned by {@link OrderPreservingTracker#getOrderPreservingTrackInfos}, it may not
     * equal to the size of {@link OrderPreservingTracker#getOrderPreservingTrackInfos}.
     */
    public int getOrderPreservingColumnCount() {
        if(this.selectedTrackOrderByContext == null) {
            return 0;
        }
        return this.selectedTrackOrderByContext.getOrderPreservingColumnCount();
    }

    /**
     * Only valid AFTER call to isOrderPreserving
     */
    public List<Info> getOrderPreservingTrackInfos() {
        if(this.selectedTrackOrderByContext == null) {
            return Collections.<Info> emptyList();
        }
        return this.selectedTrackOrderByContext.getOrderPreservingTrackInfos();
    }

    public boolean isOrderPreserving() {
        if (this.selectedTrackOrderByContext != null) {
            return this.selectedTrackOrderByContext.isOrderPreserving();
        }

        if (this.trackOrderByContexts.isEmpty()) {
           return false;
        }

        if (this.trackingOrderByExpressions.isEmpty()) {
            return false;
        }

        /**
         * at most only one TrackOrderByContext can meet isOrderPreserving is true
         */
        for(TrackOrderByContext trackOrderByContext : this.trackOrderByContexts) {
            trackOrderByContext.track(trackingOrderByExpressions);
            if(trackOrderByContext.isOrderPreserving()) {
               this.selectedTrackOrderByContext = trackOrderByContext;
               break;
            }

            if(this.selectedTrackOrderByContext == null) {
                this.selectedTrackOrderByContext = trackOrderByContext;
            }
        }
        return this.selectedTrackOrderByContext.isOrderPreserving();
    }

    public boolean isReverse() {
        if(this.selectedTrackOrderByContext == null) {
            throw new IllegalStateException("isReverse should only be called when isOrderPreserving is true!");
        }
        return this.selectedTrackOrderByContext.isReverse();
    }

    /**
     * 
     * Determines if an expression is held constant. Only works for columns in the PK currently,
     * as we only track whether PK columns are held constant.
     *
     */
    private static class IsConstantVisitor extends StatelessTraverseAllExpressionVisitor<Boolean> {
        private final ScanRanges scanRanges;
        private final Expression whereExpression;
        
        public IsConstantVisitor(ScanRanges scanRanges, Expression whereExpression) {
           this.scanRanges = scanRanges;
           this.whereExpression = whereExpression;
       }
        
        @Override
        public Boolean defaultReturn(Expression node, List<Boolean> returnValues) {
            if (!ExpressionUtil.isContantForStatement(node) ||
                    returnValues.size() < node.getChildren().size()) {
                return Boolean.FALSE;
            }
            for (Boolean returnValue : returnValues) {
                if (!returnValue) {
                    return Boolean.FALSE;
                }
            }
            return Boolean.TRUE;
        }

        @Override
        public Boolean visit(RowKeyColumnExpression node) {
            return scanRanges.hasEqualityConstraint(node.getPosition());
        }

        @Override
        public Boolean visit(LiteralExpression node) {
            return Boolean.TRUE;
        }

        @Override
        public Boolean visit(KeyValueColumnExpression keyValueColumnExpression) {
            return ExpressionUtil.isColumnExpressionConstant(keyValueColumnExpression, whereExpression);
        }
         @Override
        public Boolean visit(ProjectedColumnExpression projectedColumnExpression) {
            return ExpressionUtil.isColumnExpressionConstant(projectedColumnExpression, whereExpression);
        }
    }
    /**
     * 
     * Visitor used to determine if order is preserved across a list of expressions (GROUP BY or ORDER BY expressions)
     *
     */
    private static class TrackOrderPreservingExpressionVisitor extends StatelessTraverseNoExpressionVisitor<Info> {
        private Map<Expression, Pair<Integer,OrderByExpression>> expressionToPositionAndOrderByExpression;

        public TrackOrderPreservingExpressionVisitor(OrderBy orderBy) {
            if(orderBy.isEmpty()) {
                this.expressionToPositionAndOrderByExpression = Collections.<Expression, Pair<Integer,OrderByExpression>> emptyMap();
                return;
            }
            List<OrderByExpression> orderByExpressions = orderBy.getOrderByExpressions();
            this.expressionToPositionAndOrderByExpression = new HashMap<Expression, Pair<Integer,OrderByExpression>>(orderByExpressions.size());
            int index = 0;
            for(OrderByExpression orderByExpression : orderByExpressions) {
                this.expressionToPositionAndOrderByExpression.put(
                        orderByExpression.getExpression(),
                        new Pair<Integer,OrderByExpression>(index++, orderByExpression));
            }
        }

        @Override
        public Info defaultReturn(Expression expression, List<Info> childInfos) {
            return match(expression);
        }

        @Override
        public Info visit(RowKeyColumnExpression rowKeyColumnExpression) {
            return match(rowKeyColumnExpression);
        }

        @Override
        public Info visit(KeyValueColumnExpression keyValueColumnExpression) {
            return match(keyValueColumnExpression);
        }

        @Override
        public Info visit(ProjectedColumnExpression projectedColumnExpression) {
            return match(projectedColumnExpression);
        }

        private Info match(Expression expression)
        {
            Pair<Integer,OrderByExpression> positionAndOrderByExpression = this.expressionToPositionAndOrderByExpression.get(expression);
            if(positionAndOrderByExpression == null) {
                return null;
            }
            return new Info(
                    positionAndOrderByExpression.getFirst(),
                    positionAndOrderByExpression.getSecond().isAscending(),
                    positionAndOrderByExpression.getSecond().isNullsLast());
        }

        @Override
        public Iterator<Expression> visitEnter(ScalarFunction node) {
            return node.preservesOrder() == OrderPreserving.NO ? Collections.<Expression> emptyIterator() : Iterators
                    .singletonIterator(node.getChildren().get(node.getKeyFormationTraversalIndex()));
        }

        @Override
        public Info visitLeave(ScalarFunction node, List<Info> l) {
            if (l.isEmpty()) { return null; }
            Info info = l.get(0);
            // Keep the minimum value between this function and the current value,
            // so that we never increase OrderPreserving from NO or YES_IF_LAST.
            OrderPreserving orderPreserving = OrderPreserving.values()[Math.min(node.preservesOrder().ordinal(), info.orderPreserving.ordinal())];
            Expression childExpression = node.getChildren().get(
                    node.getKeyFormationTraversalIndex());
            boolean sortOrderIsSame = node.getSortOrder() == childExpression.getSortOrder();
            if (orderPreserving == info.orderPreserving && sortOrderIsSame) {
                return info;
            }
            return new Info(
                    info.pkPosition,
                    info.slotSpan,
                    orderPreserving,
                    sortOrderIsSame ? info.ascending : !info.ascending,
                    info.nullsLast);
        }

        @Override
        public Iterator<Expression> visitEnter(CoerceExpression node) {
            return node.getChildren().iterator();
        }

        @Override
        public Info visitLeave(CoerceExpression node, List<Info> l) {
            if (l.isEmpty()) { return null; }
            return l.get(0);
        }
        
        @Override
        public Iterator<Expression> visitEnter(RowValueConstructorExpression node) {
            return node.getChildren().iterator();
        }

        @Override
        public Info visitLeave(RowValueConstructorExpression node, List<Info> l) {
            // Child expression returned null and was filtered, so not order preserving
            if (l.size() != node.getChildren().size()) { return null; }
            Info firstInfo = l.get(0);
            Info lastInfo = firstInfo;
            // Check that pkPos are consecutive which is the only way a RVC can be order preserving
            for (int i = 1; i < l.size(); i++) {
                // not order preserving since it's not last
                if (lastInfo.orderPreserving == OrderPreserving.YES_IF_LAST) { return null; }
                Info info = l.get(i);
                // not order preserving since there's a gap in the pk
                if (info.pkPosition != lastInfo.pkPosition + 1) {
                    return null;
                 }
                if(info.ascending != lastInfo.ascending) {
                    return null;
                }
                if(info.nullsLast != lastInfo.nullsLast) {
                    return null;
                }
                lastInfo = info;
            }
            return new Info(firstInfo.pkPosition, l.size(), lastInfo.orderPreserving, lastInfo.ascending, lastInfo.nullsLast);
        }
    }
}
