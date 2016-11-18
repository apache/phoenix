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

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.CoerceExpression;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.ProjectedColumnExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.expression.RowValueConstructorExpression;
import org.apache.phoenix.expression.function.FunctionExpression.OrderPreserving;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.expression.visitor.StatelessTraverseAllExpressionVisitor;
import org.apache.phoenix.expression.visitor.StatelessTraverseNoExpressionVisitor;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.SortOrder;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

/**
 * Determines if the natural key order of the rows returned by the scan
 * will match the order of the expressions. For GROUP BY, if order is preserved we can use
 * an optimization during server-side aggregation to do the aggregation on-the-fly versus
 * keeping track of each distinct group. We can only do this optimization if all the rows
 * for each group will be contiguous. For ORDER BY, we can drop the ORDER BY statement if
 * the order is preserved.
 * 
 */
public class OrderPreservingTracker {
    public enum Ordering {ORDERED, UNORDERED};

    public static class Info {
        public final OrderPreserving orderPreserving;
        public final int pkPosition;
        public final int slotSpan;

        public Info(int pkPosition) {
            this.pkPosition = pkPosition;
            this.orderPreserving = OrderPreserving.YES;
            this.slotSpan = 1;
        }

        public Info(Info info, OrderPreserving orderPreserving) {
            this.pkPosition = info.pkPosition;
            this.slotSpan = info.slotSpan;
            this.orderPreserving = orderPreserving;
        }

        public Info(Info info, int slotSpan, OrderPreserving orderPreserving) {
            this.pkPosition = info.pkPosition;
            this.slotSpan = slotSpan;
            this.orderPreserving = orderPreserving;
        }
    }
    private final StatementContext context;
    private final TrackOrderPreservingExpressionVisitor visitor;
    private final GroupBy groupBy;
    private final Ordering ordering;
    private final int pkPositionOffset;
    private final List<Info> orderPreservingInfos;
    private final TupleProjector projector;
    private boolean isOrderPreserving = true;
    private Boolean isReverse = null;
    private int orderPreservingColumnCount = 0;
    
    public OrderPreservingTracker(StatementContext context, GroupBy groupBy, Ordering ordering, int nNodes) {
        this(context, groupBy, ordering, nNodes, null);
    }
    
    public OrderPreservingTracker(StatementContext context, GroupBy groupBy, Ordering ordering, int nNodes, TupleProjector projector) {
        this.context = context;
        if (groupBy.isEmpty()) {
            PTable table = context.getResolver().getTables().get(0).getTable();
            this.isOrderPreserving = table.rowKeyOrderOptimizable();
            boolean isSalted = table.getBucketNum() != null;
            boolean isMultiTenant = context.getConnection().getTenantId() != null && table.isMultiTenant();
            boolean isSharedViewIndex = table.getViewIndexId() != null;
            // TODO: util for this offset, as it's computed in numerous places
            this.pkPositionOffset = (isSalted ? 1 : 0) + (isMultiTenant ? 1 : 0) + (isSharedViewIndex ? 1 : 0);
        } else {
            this.isOrderPreserving = true;
            this.pkPositionOffset = 0;
        }
        this.groupBy = groupBy;
        this.visitor = new TrackOrderPreservingExpressionVisitor(projector);
        this.orderPreservingInfos = Lists.newArrayListWithExpectedSize(nNodes);
        this.ordering = ordering;
        this.projector = projector;
    }
    
    public void track(Expression node) {
        SortOrder sortOrder = node.getSortOrder();
        track(node, sortOrder, null);
    }
    
    public void track(Expression node, SortOrder sortOrder, Boolean isNullsLast) {
        if (isOrderPreserving) {
            Info info = node.accept(visitor);
            if (info == null) {
                isOrderPreserving = false;
            } else {
                // If the expression is sorted in a different order than the specified sort order
                // then the expressions are not order preserving.
                if (node.getSortOrder() != sortOrder) {
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
                if (isNullsLast!=null && node.isNullable()) {
                    if (!Boolean.valueOf(isNullsLast).equals(isReverse)) {
                        isOrderPreserving = false;
                        isReverse = false;
                        return;
                    }
                }
                orderPreservingInfos.add(info);
            }
        }
    }
    
    /*
     * Only valid AFTER call to isOrderPreserving
     */
    public int getOrderPreservingColumnCount() {
        return orderPreservingColumnCount;
    }

    public boolean isOrderPreserving() {
        if (!isOrderPreserving) {
            return false;
        }
        if (ordering == Ordering.UNORDERED) {
            // Sort by position
            Collections.sort(orderPreservingInfos, new Comparator<Info>() {
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
        int prevPos = pkPositionOffset - 1;
        OrderPreserving prevOrderPreserving = OrderPreserving.YES;
        for (int i = 0; i < orderPreservingInfos.size() && isOrderPreserving; i++) {
            Info entry = orderPreservingInfos.get(i);
            int pos = entry.pkPosition;
            isOrderPreserving &= entry.orderPreserving != OrderPreserving.NO && prevOrderPreserving == OrderPreserving.YES && (pos == prevPos || pos - prevSlotSpan == prevPos  || hasEqualityConstraints(prevPos+prevSlotSpan, pos));
            prevPos = pos;
            prevSlotSpan = entry.slotSpan;
            prevOrderPreserving = entry.orderPreserving;
        }
        orderPreservingColumnCount = prevPos + prevSlotSpan;
        return isOrderPreserving;
    }
    
    private boolean hasEqualityConstraints(int startPos, int endPos) {
        ScanRanges ranges = context.getScanRanges();
        // If a GROUP BY is being done, then the rows are ordered according to the GROUP BY key,
        // not by the original row key order of the table (see PHOENIX-3451).
        // We check each GROUP BY expression to see if it only references columns that are
        // matched by equality constraints, in which case the expression itself would be constant.
        // FIXME: this only recognizes row key columns that are held constant, not all columns.
        // FIXME: we should optimize out any GROUP BY or ORDER BY expression which is deemed to
        // be held constant based on the WHERE clause.
        if (!groupBy.isEmpty()) {
            for (int pos = startPos; pos < endPos; pos++) {
                IsConstantVisitor visitor = new IsConstantVisitor(this.projector, ranges);
                List<Expression> groupByExpressions = groupBy.getExpressions();
                if (pos >= groupByExpressions.size()) { // sanity check - shouldn't be necessary
                    return false;
                }
                Expression groupByExpression = groupByExpressions.get(pos);
                if ( groupByExpression.getDeterminism().ordinal() > Determinism.PER_STATEMENT.ordinal() ) {
                    return false;
                }
                Boolean isConstant = groupByExpression.accept(visitor);
                if (!Boolean.TRUE.equals(isConstant)) {
                    return false;
                }
            }
            return true;
        }
        for (int pos = startPos; pos < endPos; pos++) {
            if (!ranges.hasEqualityConstraint(pos)) {
                return false;
            }
        }
        return true;
    }
    
    public boolean isReverse() {
        return Boolean.TRUE.equals(isReverse);
    }

    /**
     * 
     * Determines if an expression is held constant. Only works for columns in the PK currently,
     * as we only track whether PK columns are held constant.
     *
     */
    private static class IsConstantVisitor extends StatelessTraverseAllExpressionVisitor<Boolean> {
        private final TupleProjector projector;
        private final ScanRanges scanRanges;
        
        public IsConstantVisitor(TupleProjector projector, ScanRanges scanRanges) {
            this.projector = projector;
            this.scanRanges = scanRanges;
        }
        
        @Override
        public Boolean defaultReturn(Expression node, List<Boolean> returnValues) {
            if (node.getDeterminism().ordinal() > Determinism.PER_STATEMENT.ordinal() || 
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
        public Boolean visit(ProjectedColumnExpression node) {
            if (projector == null) {
                return super.visit(node);
            }
            Expression expression = projector.getExpressions()[node.getPosition()];
            // Only look one level down the projection.
            if (expression instanceof ProjectedColumnExpression) {
                return super.visit(node);
            }
            return expression.accept(this);
        }
    }
    /**
     * 
     * Visitor used to determine if order is preserved across a list of expressions (GROUP BY or ORDER BY expressions)
     *
     */
    private static class TrackOrderPreservingExpressionVisitor extends StatelessTraverseNoExpressionVisitor<Info> {
        private final TupleProjector projector;
        
        public TrackOrderPreservingExpressionVisitor(TupleProjector projector) {
            this.projector = projector;
        }
        
        @Override
        public Info visit(RowKeyColumnExpression node) {
            return new Info(node.getPosition());
        }

        @Override
        public Info visit(ProjectedColumnExpression node) {
            if (projector == null) {
                return super.visit(node);
            }
            Expression expression = projector.getExpressions()[node.getPosition()];
            // Only look one level down the projection.
            if (expression instanceof ProjectedColumnExpression) {
                return super.visit(node);
            }
            return expression.accept(this);
        }

        @Override
        public Iterator<Expression> visitEnter(ScalarFunction node) {
            return node.preservesOrder() == OrderPreserving.NO ? Iterators.<Expression> emptyIterator() : Iterators
                    .singletonIterator(node.getChildren().get(node.getKeyFormationTraversalIndex()));
        }

        @Override
        public Info visitLeave(ScalarFunction node, List<Info> l) {
            if (l.isEmpty()) { return null; }
            Info info = l.get(0);
            // Keep the minimum value between this function and the current value,
            // so that we never increase OrderPreserving from NO or YES_IF_LAST.
            OrderPreserving orderPreserving = OrderPreserving.values()[Math.min(node.preservesOrder().ordinal(), info.orderPreserving.ordinal())];
            if (orderPreserving == info.orderPreserving) {
                return info;
            }
            return new Info(info, orderPreserving);
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
                if (info.pkPosition != lastInfo.pkPosition + 1) { return null; }
                lastInfo = info;
            }
            return new Info(firstInfo, l.size(), lastInfo.orderPreserving);
        }
    }
}
