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
 * There are mainly three changes for refactoring this class in PHOENIX-5148:
 * 1.added a {@link #getInputOrderBys} method to determine the input OrderBys by the combination
 *   of innerQueryPlan's output OrderBys ， GroupBy of current QueryPlan and the rowKeyColumns of current table.
 *
 * 2.because the innerQueryPlan may have multiple output OrderBys(for {@link SortMergeJoinPlan}),
 *   so I extracted many stateful member variables (such as orderPreservingTrackInfos, isOrderPreserving
 *   and isReverse etc.) to a new inner class {@link TrackOrderByContext}, {@link TrackOrderByContext}
 *   is used to track if a single Input OrderBy matches the target OrderByExpressions in {@link #isOrderPreserving()}
 *   method, and once we found a  {@link TrackOrderByContext} satisfied {@link #isOrderPreserving()},
 *   {@link #selectedTrackOrderByContext} member variable is set to this {@link TrackOrderByContext},
 *   then we can use this {@link #selectedTrackOrderByContext} to implement the {@link #getOrderPreservingTrackInfos()}
 *   and {@link #isReverse()} methods etc.
 *   BTW. at most only one {@link TrackOrderByContext} can meet {@link #isOrderPreserving()} is true.
 *
 * 3.added ascending and nullsLast to the inner class {@link Info} , and extracted complete ordering
 *   information in {@link Info} class by the inner {@link TrackOrderPreservingExpressionVisitor} class,
 *   so we can inferring alignment between the target OrderByExpressions and the input OrderBys based on
 *   {@link Info} in {@link TrackOrderByContext#doTrack} method, not on the row keys like the original
 *   {@link #track} method does.
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
        private Expression expression;

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
                newExpressions.add(trackInfo.expression);
            }
            return newExpressions;
        }

        public Expression getExpression() {
            return expression;
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
    private int pkPositionOffset = 0;
    private Expression whereExpression;
    private List<TrackOrderByCell> trackOrderByCells = new LinkedList<TrackOrderByCell>();
    private List<TrackOrderByContext> trackOrderByContexts = Collections.<TrackOrderByContext> emptyList();
    private TrackOrderByContext selectedTrackOrderByContext = null;
    private List<OrderBy> inputOrderBys = Collections.<OrderBy> emptyList();

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
        boolean isOrderPreserving = false;
        if (groupBy.isEmpty() && inputOrderBys == null) {
            PTable table = context.getResolver().getTables().get(0).getTable();
            isOrderPreserving = table.rowKeyOrderOptimizable();
        } else {
            isOrderPreserving = true;
        }
        this.groupBy = groupBy;
        this.ordering = ordering;
        this.whereExpression = whereExpression;
        if(inputOrderBys != null) {
            this.inputOrderBys = inputOrderBys;
        } else {
            this.getInputOrderBys(innerQueryPlan, groupBy, context);
        }
        if(this.inputOrderBys.isEmpty()) {
            return;
        }
        this.trackOrderByContexts = new ArrayList<TrackOrderByContext>(this.inputOrderBys.size());
        for(OrderBy inputOrderBy : this.inputOrderBys) {
            this.trackOrderByContexts.add(
                    new TrackOrderByContext(isOrderPreserving, nNodes, inputOrderBy));
        }
    }

    /**
     * Infer input OrderBys, if the innerQueryPlan is null, we make the OrderBys from the pk columns of {@link PTable}.
     * @param innerQueryPlan
     * @param groupBy
     * @param statementContext
     * @throws SQLException
     */
    private void getInputOrderBys(QueryPlan innerQueryPlan, GroupBy groupBy, StatementContext statementContext) throws SQLException {
        if(!groupBy.isEmpty()) {
            this.inputOrderBys = Collections.singletonList(ExpressionUtil.convertGroupByToOrderBy(groupBy, false));
            return;
        }
        if(innerQueryPlan != null) {
            this.inputOrderBys = innerQueryPlan.getOutputOrderBys();
            return;
        }
        this.inputOrderBys = Collections.<OrderBy> emptyList();
        TableRef tableRef = statementContext.getResolver().getTables().get(0);
        PhoenixConnection phoenixConnection = statementContext.getConnection();
        Pair<OrderBy,Integer> orderByAndRowKeyColumnOffset =
                ExpressionUtil.getOrderByFromTable(tableRef, phoenixConnection, false);
        OrderBy orderBy = orderByAndRowKeyColumnOffset.getFirst();
        this.pkPositionOffset = orderByAndRowKeyColumnOffset.getSecond();
        if(orderBy != OrderBy.EMPTY_ORDER_BY) {
            this.inputOrderBys = Collections.singletonList(orderBy);
        }
    }

    private class TrackOrderByContext {
        private List<Info> orderPreservingTrackInfos;
        private boolean isOrderPreserving = true;
        private Boolean isReverse = null;
        private int orderPreservingColumnCount = 0;
        private final TrackOrderPreservingExpressionVisitor trackOrderPreservingExpressionVisitor;
        private OrderBy inputOrderBy = null;

        public TrackOrderByContext(boolean isOrderPreserving, int orderByNodeCount, OrderBy inputOrderBy) {
            this.isOrderPreserving = isOrderPreserving;
            this.trackOrderPreservingExpressionVisitor = new TrackOrderPreservingExpressionVisitor(inputOrderBy);
            this.orderPreservingTrackInfos = Lists.newArrayListWithExpectedSize(orderByNodeCount);
            this.inputOrderBy = inputOrderBy;
        }

        public void track(List<TrackOrderByCell> trackOrderByCells) {
            for(TrackOrderByCell trackOrderByCell : trackOrderByCells) {
                doTrack(trackOrderByCell.expression,
                        trackOrderByCell.isAscending,
                        trackOrderByCell.isNullsLast);
            }
        }

        private void doTrack(Expression expression, Boolean isAscending, Boolean isNullsLast) {
            if (!isOrderPreserving) {
               return;
            }
            Info trackInfo = expression.accept(trackOrderPreservingExpressionVisitor);
            if (trackInfo == null) {
                isOrderPreserving = false;
                return;
            }
            // If the expression is sorted in a different order than the specified sort order
            // then the expressions are not order preserving.
            if (isAscending != null && trackInfo.ascending != isAscending.booleanValue()) {
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

            if (isNullsLast!=null && expression.isNullable()) {
                if ((trackInfo.nullsLast == isNullsLast.booleanValue()) && isReverse.booleanValue() ||
                    (trackInfo.nullsLast != isNullsLast.booleanValue()) && !isReverse.booleanValue()) {
                    isOrderPreserving = false;
                    isReverse = false;
                    return;
                }
            }
            trackInfo.expression = expression;
            orderPreservingTrackInfos.add(trackInfo);
        }

        /*
         * Only valid AFTER call to isOrderPreserving
         */
        public int getOrderPreservingColumnCount() {
            return orderPreservingColumnCount;
        }

        /**
         * Only valid AFTER call to isOrderPreserving
         */
        public List<Info> getOrderPreservingTrackInfos() {
            if(this.isOrderPreserving) {
                return ImmutableList.copyOf(this.orderPreservingTrackInfos);
            }
            int orderPreservingColumnCountToUse = this.orderPreservingColumnCount - pkPositionOffset;
            if(orderPreservingColumnCountToUse <= 0) {
                return Collections.<Info> emptyList();
            }
            return ImmutableList.copyOf(this.orderPreservingTrackInfos.subList(0, orderPreservingColumnCountToUse));
        }

        public boolean isOrderPreserving() {
            if (!isOrderPreserving) {
                return false;
            }
            if (ordering == Ordering.UNORDERED) {
                // Sort by position
                Collections.sort(orderPreservingTrackInfos, new Comparator<Info>() {
                    @Override
                    public int compare(Info o1, Info o2) {
                        int cmp = o1.pkPosition-o2.pkPosition;
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
            for (int i = 0; i < orderPreservingTrackInfos.size(); i++) {
                Info entry = orderPreservingTrackInfos.get(i);
                int pos = entry.pkPosition;
                isOrderPreserving = isOrderPreserving &&
                        entry.orderPreserving != OrderPreserving.NO &&
                        prevOrderPreserving == OrderPreserving.YES &&
                        (pos == prevPos ||
                         pos - prevSlotSpan == prevPos  ||
                         hasEqualityConstraints(prevPos+prevSlotSpan, pos));
                if(!isOrderPreserving) {
                    break;
                }
                prevPos = pos;
                prevSlotSpan = entry.slotSpan;
                prevOrderPreserving = entry.orderPreserving;
            }
            orderPreservingColumnCount = prevPos + prevSlotSpan + pkPositionOffset;
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

    private static class TrackOrderByCell
    {
        private Expression expression;
        private Boolean isAscending;
        private Boolean isNullsLast;

        public TrackOrderByCell(Expression expression,Boolean isAscending, Boolean isNullsLast) {
            this.expression = expression;
            this.isAscending = isAscending;
            this.isNullsLast = isNullsLast;
        }
    }

    public void track(Expression expression) {
        track(expression, null, null);
    }

    public void track(Expression expression, Boolean isAscending, Boolean isNullsLast) {
        TrackOrderByCell trackOrderByContext =
                new TrackOrderByCell(expression, isAscending, isNullsLast);
        this.trackOrderByCells.add(trackOrderByContext);
    }

    /*
     * Only valid AFTER call to isOrderPreserving
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
        if(this.selectedTrackOrderByContext != null) {
            throw new IllegalStateException("isOrderPreserving should be called only once");
        }

        if(this.trackOrderByContexts.isEmpty()) {
           return false;
        }

        if(this.trackOrderByCells.isEmpty()) {
            return false;
        }

        /**
         * at most only one TrackOrderByContext can meet isOrderPreserving is true
         */
        for(TrackOrderByContext trackOrderByContext : this.trackOrderByContexts) {
            trackOrderByContext.track(trackOrderByCells);
            if(trackOrderByContext.isOrderPreserving()) {
               this.selectedTrackOrderByContext = trackOrderByContext;
               break;
            }

            if(this.selectedTrackOrderByContext == null) {
                this.selectedTrackOrderByContext = trackOrderByContext;
            }
        }
        return this.selectedTrackOrderByContext.isOrderPreserving;
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
