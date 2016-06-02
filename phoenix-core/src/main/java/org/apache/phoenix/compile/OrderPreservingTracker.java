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
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ProjectedColumnExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.expression.RowValueConstructorExpression;
import org.apache.phoenix.expression.function.FunctionExpression.OrderPreserving;
import org.apache.phoenix.expression.function.ScalarFunction;
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
    private boolean isOrderPreserving = true;
    private Boolean isReverse = null;
    private int orderPreservingColumnCount = 0;
    
    public OrderPreservingTracker(StatementContext context, GroupBy groupBy, Ordering ordering, int nNodes) {
        this(context, groupBy, ordering, nNodes, null);
    }
    
    public OrderPreservingTracker(StatementContext context, GroupBy groupBy, Ordering ordering, int nNodes, TupleProjector projector) {
        this.context = context;
        int pkPositionOffset = 0;
        PTable table = context.getResolver().getTables().get(0).getTable();
        isOrderPreserving = table.rowKeyOrderOptimizable();
        if (groupBy.isEmpty()) { // FIXME: would the below table have any of these set in the case of a GROUP BY?
            boolean isSalted = table.getBucketNum() != null;
            boolean isMultiTenant = context.getConnection().getTenantId() != null && table.isMultiTenant();
            boolean isSharedViewIndex = table.getViewIndexId() != null;
            // TODO: util for this offset, as it's computed in numerous places
            pkPositionOffset = (isSalted ? 1 : 0) + (isMultiTenant ? 1 : 0) + (isSharedViewIndex ? 1 : 0);
        }
        this.pkPositionOffset = pkPositionOffset;
        this.groupBy = groupBy;
        this.visitor = new TrackOrderPreservingExpressionVisitor(projector);
        this.orderPreservingInfos = Lists.newArrayListWithExpectedSize(nNodes);
        this.ordering = ordering;
    }
    
    public void track(Expression node) {
        SortOrder sortOrder = node.getSortOrder();
        track(node, sortOrder, sortOrder != SortOrder.getDefault());
    }
    
    public void track(Expression node, SortOrder sortOrder, boolean isNullsLast) {
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
                        /*
                         * When a GROUP BY is not order preserving, we cannot do a reverse
                         * scan to eliminate the ORDER BY since our server-side scan is not
                         * ordered in that case.
                         */
                        if (!groupBy.isEmpty() && !groupBy.isOrderPreserving()) {
                            isOrderPreserving = false;
                            isReverse = false;
                            return;
                        }
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
                if (node.isNullable()) {
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
