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
import java.util.Comparator;
import java.util.List;

import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.expression.function.FunctionExpression;
import org.apache.phoenix.expression.function.FunctionExpression.OrderPreserving;
import org.apache.phoenix.parse.CaseParseNode;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.DivideParseNode;
import org.apache.phoenix.parse.MultiplyParseNode;
import org.apache.phoenix.parse.SubtractParseNode;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.SortOrder;
import com.google.common.collect.Lists;

/**
 * Visitor that builds the expressions of a GROUP BY and ORDER BY clause. While traversing
 * the parse node tree, the visitor also determines if the natural key order of the scan
 * will match the order of the expressions. For GROUP BY, if order is preserved we can use
 * an optimization during server-side aggregation to do the aggregation on-the-fly versus
 * keeping track of each distinct group. We can only do this optimization if all the rows
 * for each group will be contiguous. For ORDER BY, we can drop the ORDER BY statement if
 * the order is preserved.
 * 
 */
public class TrackOrderPreservingExpressionCompiler extends ExpressionCompiler {
    public enum Ordering {ORDERED, UNORDERED};
    
    private final List<Entry> entries;
    private final Ordering ordering;
    private final int positionOffset;
    private final TupleProjector tupleProjector; // for derived-table query compilation
    private OrderPreserving orderPreserving = OrderPreserving.YES;
    private ColumnRef columnRef;
    private boolean isOrderPreserving = true;
    private Boolean isReverse;
    
    TrackOrderPreservingExpressionCompiler(StatementContext context, GroupBy groupBy, int expectedEntrySize, Ordering ordering, TupleProjector tupleProjector) {
        super(context, groupBy);
        PTable table = context.getResolver().getTables().get(0).getTable();
        boolean isSalted = table.getBucketNum() != null;
        boolean isMultiTenant = context.getConnection().getTenantId() != null && table.isMultiTenant();
        boolean isSharedViewIndex = table.getViewIndexId() != null;
        // TODO: util for this offset, as it's computed in numerous places
        positionOffset = (isSalted ? 1 : 0) + (isMultiTenant ? 1 : 0) + (isSharedViewIndex ? 1 : 0);
        entries = Lists.newArrayListWithExpectedSize(expectedEntrySize);
        this.ordering = ordering;
        this.tupleProjector = tupleProjector;
    }
    
    public Boolean isReverse() {
        return isReverse;
    }

    public boolean isOrderPreserving() {
        if (!isOrderPreserving) {
            return false;
        }
        if (ordering == Ordering.UNORDERED) {
            // Sort by position
            Collections.sort(entries, new Comparator<Entry>() {
                @Override
                public int compare(Entry o1, Entry o2) {
                    return o1.getPkPosition()-o2.getPkPosition();
                }
            });
        }
        // Determine if there are any gaps in the PK columns (in which case we don't need
        // to sort in the coprocessor because the keys will already naturally be in sorted
        // order.
        int prevPos = positionOffset - 1;
        OrderPreserving prevOrderPreserving = OrderPreserving.YES;
        for (int i = 0; i < entries.size() && isOrderPreserving; i++) {
            Entry entry = entries.get(i);
            int pos = entry.getPkPosition();
            isOrderPreserving &= (entry.getOrderPreserving() != OrderPreserving.NO) && (pos == prevPos || ((pos - 1 == prevPos) && (prevOrderPreserving == OrderPreserving.YES)));
            prevPos = pos;
            prevOrderPreserving = entries.get(i).getOrderPreserving();
        }
        return isOrderPreserving;
    }
    
    @Override
    protected Expression addExpression(Expression expression) {
        // TODO: have FunctionExpression visitor instead and remove this cast
        if (expression instanceof FunctionExpression) {
            // Keep the minimum value between this function and the current value,
            // so that we never increase OrderPreserving from NO or YES_IF_LAST.
            orderPreserving = OrderPreserving.values()[Math.min(orderPreserving.ordinal(), ((FunctionExpression)expression).preservesOrder().ordinal())];
        }
        return super.addExpression(expression);
    }

    @Override
    public boolean visitEnter(CaseParseNode node) throws SQLException {
        orderPreserving = OrderPreserving.NO;
        return super.visitEnter(node);
    }
    
    @Override
    public boolean visitEnter(DivideParseNode node) throws SQLException {
        // A divide expression may not preserve row order.
        // For example: GROUP BY 1/x
        orderPreserving = OrderPreserving.NO;
        return super.visitEnter(node);
    }

    @Override
    public boolean visitEnter(SubtractParseNode node) throws SQLException {
        // A subtract expression may not preserve row order.
        // For example: GROUP BY 10 - x
        orderPreserving = OrderPreserving.NO;
        return super.visitEnter(node);
    }

    @Override
    public boolean visitEnter(MultiplyParseNode node) throws SQLException {
        // A multiply expression may not preserve row order.
        // For example: GROUP BY -1 * x
        orderPreserving = OrderPreserving.NO;
        return super.visitEnter(node);
    }

    @Override
    public void reset() {
        super.reset();
        columnRef = null;
        orderPreserving = OrderPreserving.YES;
    }
    
    @Override
    protected ColumnRef resolveColumn(ColumnParseNode node) throws SQLException {
        ColumnRef ref = super.resolveColumn(node);
        // If we encounter any non PK column, then we can't aggregate on-the-fly
        // because the distinct groups have no correlation to the KV column value
        if (getColumnPKPosition(ref) < 0) {
            orderPreserving = OrderPreserving.NO;
        }
        
        if (columnRef == null) {
            columnRef = ref;
        } else if (!columnRef.equals(ref)) {
            // If we encounter more than one column reference in an expression,
            // we can't assume the result of the expression will be key ordered.
            // For example GROUP BY a * b
            orderPreserving = OrderPreserving.NO;
        }
        return ref;
    }
    
    private int getColumnPKPosition(ColumnRef ref) {
        if (tupleProjector != null && ref.getTable().getType() == PTableType.SUBQUERY) {
            Expression expression = tupleProjector.getExpressions()[ref.getColumnPosition()];
            if (expression instanceof RowKeyColumnExpression) {
                return ((RowKeyColumnExpression) expression).getPosition();
            }
        }
        
        return ref.getPKSlotPosition();
    }

    public boolean addEntry(Expression expression) {
        if (expression instanceof LiteralExpression) {
            return false;
        }
        isOrderPreserving &= (orderPreserving != OrderPreserving.NO);
        entries.add(new Entry(expression, columnRef, orderPreserving));
        return true;
    }
    
    public boolean addEntry(Expression expression, SortOrder sortOrder) {
        // If the expression is sorted in a different order than the specified sort order
        // then the expressions are not order preserving.
        if (expression.getSortOrder() != sortOrder) {
            if (isReverse == null) {
                isReverse = true;
            } else if (!isReverse){
                orderPreserving = OrderPreserving.NO;
            }
        } else {
            if (isReverse == null) {
                isReverse = false;
            } else if (isReverse){
                orderPreserving = OrderPreserving.NO;
            }
        }
        return addEntry(expression);
    }
    
    public List<Entry> getEntries() {
        return entries;
    }

    public class Entry {
        private final Expression expression;
        private final ColumnRef columnRef;
        private final OrderPreserving orderPreserving;
        
        private Entry(Expression expression, ColumnRef columnRef, OrderPreserving orderPreserving) {
            this.expression = expression;
            this.columnRef = columnRef;
            this.orderPreserving = orderPreserving;
        }

        public Expression getExpression() {
            return expression;
        }

        public int getPkPosition() {
            return getColumnPKPosition(columnRef);
        }

        public int getColumnPosition() {
            return columnRef.getColumnPosition();
        }

        public OrderPreserving getOrderPreserving() {
            return orderPreserving;
        }
    }
}