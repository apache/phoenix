/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.phoenix.util;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.ExpressionCompiler;
import org.apache.phoenix.compile.OrderPreservingTracker.Info;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.expression.ColumnExpression;
import org.apache.phoenix.expression.ComparisonExpression;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.IsNullExpression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.OrderByExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.expression.visitor.StatelessTraverseAllExpressionVisitor;
import org.apache.phoenix.expression.visitor.StatelessTraverseNoExpressionVisitor;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.ProjectedColumn;
import org.apache.phoenix.schema.RowKeyValueAccessor;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;

public class ExpressionUtil {
	private ExpressionUtil() {
	}

	public static boolean isConstant(Expression expression) {
		return (expression.isStateless() && isContantForStatement(expression));
	}

   /**
    * this method determines if expression is constant if all children of it are constants.
    * @param expression
    * @return
    */
    public static boolean isContantForStatement(Expression expression) {
        return  (expression.getDeterminism() == Determinism.ALWAYS
                || expression.getDeterminism() == Determinism.PER_STATEMENT);
    }

    public static LiteralExpression getConstantExpression(Expression expression, ImmutableBytesWritable ptr)
            throws SQLException {
        Object value = null;
        PDataType type = expression.getDataType();
        if (expression.evaluate(null, ptr) && ptr.getLength() != 0) {
            value = type.toObject(ptr.get(), ptr.getOffset(), ptr.getLength(), type, expression.getSortOrder(), expression.getMaxLength(), expression.getScale());
        }
        return LiteralExpression.newConstant(value, type, expression.getDeterminism());
    }

    public static boolean isNull(Expression expression, ImmutableBytesWritable ptr) {
        return isConstant(expression) && (!expression.evaluate(null, ptr) || ptr.getLength() == 0);
    }

    public static LiteralExpression getNullExpression(Expression expression) throws SQLException {
        return LiteralExpression.newConstant(null, expression.getDataType(), expression.getDeterminism());
    }
    
    public static boolean evaluatesToTrue(Expression expression) {
        if (isConstant(expression)) {
            ImmutableBytesWritable ptr = new ImmutableBytesWritable();
            expression.evaluate(null, ptr);
            return Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(ptr));
        }
        return false;
    }

    public static boolean isPkPositionChanging(TableRef tableRef, List<Expression> projectedExpressions) throws SQLException {
        for (int i = 0; i < tableRef.getTable().getPKColumns().size(); i++) {
            PColumn column = tableRef.getTable().getPKColumns().get(i);
            Expression source = projectedExpressions.get(i);
            if (source == null || !source
                    .equals(new ColumnRef(tableRef, column.getPosition()).newColumnExpression())) { return true; }
        }
        return false;
    }

    /**
     * check the whereExpression to see if the columnExpression is constant.
     * eg. for {@code "where a = 3 and b > 9" }, a is constant,but b is not.
     * @param columnExpression
     * @param whereExpression
     * @return
     */
    public static boolean isColumnExpressionConstant(ColumnExpression columnExpression, Expression whereExpression) {
        if(whereExpression == null) {
            return false;
        }
        IsColumnConstantExpressionVisitor isColumnConstantExpressionVisitor =
                new IsColumnConstantExpressionVisitor(columnExpression);
        whereExpression.accept(isColumnConstantExpressionVisitor);
        return isColumnConstantExpressionVisitor.isConstant();
    }

    private static class IsColumnConstantExpressionVisitor extends StatelessTraverseNoExpressionVisitor<Void> {
        private final Expression columnExpression ;
        private Expression firstRhsConstantExpression = null;
        private int rhsConstantCount = 0;
        private boolean isNullExpressionVisited = false;

        public IsColumnConstantExpressionVisitor(Expression columnExpression) {
            this.columnExpression = columnExpression;
        }
        /**
         * only consider and,for "where a = 3 or b = 9", neither a or b is constant.
         */
        @Override
        public Iterator<Expression> visitEnter(AndExpression andExpression) {
            if(rhsConstantCount > 1) {
                return null;
            }
            return andExpression.getChildren().iterator();
        }
        /**
         * <pre>
         * We just consider {@link ComparisonExpression} because:
         * 1.for {@link InListExpression} as "a in ('2')", the {@link InListExpression} is rewritten to
         *  {@link ComparisonExpression} in {@link InListExpression#create}
         * 2.for {@link RowValueConstructorExpression} as "(a,b)=(1,2)",{@link RowValueConstructorExpression}
         *   is rewritten to {@link ComparisonExpression} in {@link ComparisonExpression#create}
         * 3.not consider {@link CoerceExpression}, because for "where cast(a as integer)=2", when a is double,
         *   a is not constant.
         * </pre>
         */
        @Override
        public Iterator<Expression> visitEnter(ComparisonExpression comparisonExpression) {
            if(rhsConstantCount > 1) {
                return null;
            }
            if(comparisonExpression.getFilterOp() != CompareOp.EQUAL) {
                return null;
            }
            Expression lhsExpresssion = comparisonExpression.getChildren().get(0);
            if(!this.columnExpression.equals(lhsExpresssion)) {
                return null;
            }
            Expression rhsExpression = comparisonExpression.getChildren().get(1);
            if(rhsExpression == null) {
                return null;
            }
            Boolean isConstant = rhsExpression.accept(new IsCompositeLiteralExpressionVisitor());
            if(isConstant != null && isConstant.booleanValue()) {
                checkConstantValue(rhsExpression);
            }
            return null;
        }

        public boolean isConstant() {
            return this.rhsConstantCount == 1;
        }

        @Override
        public Iterator<Expression> visitEnter(IsNullExpression isNullExpression) {
            if(rhsConstantCount > 1) {
                return null;
            }
            if(isNullExpression.isNegate()) {
                return null;
            }
            Expression lhsExpresssion = isNullExpression.getChildren().get(0);
            if(!this.columnExpression.equals(lhsExpresssion)) {
                return null;
            }
            this.checkConstantValue(null);
            return null;
        }

        private void checkConstantValue(Expression rhsExpression) {
            if(!this.isNullExpressionVisited && this.firstRhsConstantExpression == null) {
                this.firstRhsConstantExpression = rhsExpression;
                rhsConstantCount++;
                if(rhsExpression == null) {
                    this.isNullExpressionVisited = true;
                }
                return;
            }

            if(!isExpressionEquals(this.isNullExpressionVisited ? null : this.firstRhsConstantExpression, rhsExpression)) {
                rhsConstantCount++;
                return;
            }
        }

        private static boolean isExpressionEquals(Expression oldExpression,Expression newExpression) {
            if(oldExpression == null) {
                if(newExpression == null) {
                    return true;
                }
                return ExpressionUtil.isNull(newExpression, new ImmutableBytesWritable());
            }
            if(newExpression == null) {
                return ExpressionUtil.isNull(oldExpression, new ImmutableBytesWritable());
            }
            return oldExpression.equals(newExpression);
        }
    }

    private static class IsCompositeLiteralExpressionVisitor extends StatelessTraverseAllExpressionVisitor<Boolean> {
        @Override
        public Boolean defaultReturn(Expression expression, List<Boolean> childResultValues) {
            if (!ExpressionUtil.isContantForStatement(expression) ||
                    childResultValues.size() < expression.getChildren().size()) {
                return Boolean.FALSE;
            }
            for (Boolean childResultValue : childResultValues) {
                if (!childResultValue) {
                    return Boolean.FALSE;
                }
            }
            return Boolean.TRUE;
        }
        @Override
        public Boolean visit(LiteralExpression literalExpression) {
            return Boolean.TRUE;
        }
    }

    /**
     * <pre>
     * Infer OrderBys from the rowkey columns of {@link PTable},for projected table may be there is no rowkey columns,
     * so we should move forward to inspect {@link ProjectedColumn} by {@link #getOrderByFromProjectedTable}.
     * The second part of the return pair is the rowkey column offset we must skip when we create OrderBys, because for table with salted/multiTenant/viewIndexId,
     * some leading rowkey columns should be skipped.
     * </pre>
     * @param tableRef
     * @param phoenixConnection
     * @param orderByReverse
     * @return
     * @throws SQLException
     */
    public static Pair<OrderBy,Integer> getOrderByFromTable(
            TableRef tableRef,
            PhoenixConnection phoenixConnection,
            boolean orderByReverse) throws SQLException {

        PTable table = tableRef.getTable();
        Pair<OrderBy,Integer> orderByAndRowKeyColumnOffset =
                getOrderByFromTableByRowKeyColumn(table, phoenixConnection, orderByReverse);
        if(orderByAndRowKeyColumnOffset.getFirst() != OrderBy.EMPTY_ORDER_BY) {
            return orderByAndRowKeyColumnOffset;
        }
        if(table.getType() == PTableType.PROJECTED) {
            orderByAndRowKeyColumnOffset =
                    getOrderByFromProjectedTable(tableRef, phoenixConnection, orderByReverse);
            if(orderByAndRowKeyColumnOffset.getFirst() != OrderBy.EMPTY_ORDER_BY) {
                return orderByAndRowKeyColumnOffset;
            }
        }
        return new Pair<OrderBy,Integer>(OrderBy.EMPTY_ORDER_BY, 0);
    }

    /**
     * Infer OrderBys from the rowkey columns of {@link PTable}.
     * The second part of the return pair is the rowkey column offset we must skip when we create OrderBys, because for table with salted/multiTenant/viewIndexId,
     * some leading rowkey columns should be skipped.
     * @param table
     * @param phoenixConnection
     * @param orderByReverse
     * @return
     */
    public static Pair<OrderBy,Integer> getOrderByFromTableByRowKeyColumn(
            PTable table,
            PhoenixConnection phoenixConnection,
            boolean orderByReverse) {
        Pair<List<RowKeyColumnExpression>,Integer> rowKeyColumnExpressionsAndRowKeyColumnOffset =
                ExpressionUtil.getRowKeyColumnExpressionsFromTable(table, phoenixConnection);
        List<RowKeyColumnExpression> rowKeyColumnExpressions = rowKeyColumnExpressionsAndRowKeyColumnOffset.getFirst();
        int rowKeyColumnOffset = rowKeyColumnExpressionsAndRowKeyColumnOffset.getSecond();
        if(rowKeyColumnExpressions.isEmpty()) {
            return new Pair<OrderBy,Integer>(OrderBy.EMPTY_ORDER_BY,0);
        }
        return new Pair<OrderBy,Integer>(
                convertRowKeyColumnExpressionsToOrderBy(rowKeyColumnExpressions, orderByReverse),
                rowKeyColumnOffset);
    }

    /**
     * For projected table may be there is no rowkey columns,
     * so we should move forward to inspect {@link ProjectedColumn} to check if the source column is rowkey column.
     * The second part of the return pair is the rowkey column offset we must skip when we create OrderBys, because for table with salted/multiTenant/viewIndexId,
     * some leading rowkey columns should be skipped.
     * @param projectedTableRef
     * @param phoenixConnection
     * @param orderByReverse
     * @return
     * @throws SQLException
     */
    public static Pair<OrderBy,Integer> getOrderByFromProjectedTable(
            TableRef projectedTableRef,
            PhoenixConnection phoenixConnection,
            boolean orderByReverse) throws SQLException {

        PTable projectedTable = projectedTableRef.getTable();
        assert projectedTable.getType() == PTableType.PROJECTED;
        TableRef sourceTableRef = null;
        TreeMap<Integer,ColumnRef> sourceRowKeyColumnIndexToProjectedColumnRef =
                new TreeMap<Integer, ColumnRef>();

        for(PColumn column : projectedTable.getColumns()) {
            if(!(column instanceof ProjectedColumn)) {
                continue;
            }
            ProjectedColumn projectedColumn = (ProjectedColumn)column;
            ColumnRef sourceColumnRef = projectedColumn.getSourceColumnRef();
            TableRef currentSourceTableRef = sourceColumnRef.getTableRef();
            if(sourceTableRef == null) {
                sourceTableRef = currentSourceTableRef;
            }
            else if(!sourceTableRef.equals(currentSourceTableRef)) {
                return new Pair<OrderBy,Integer>(OrderBy.EMPTY_ORDER_BY, 0);
            }
            int sourceRowKeyColumnIndex = sourceColumnRef.getPKSlotPosition();
            if(sourceRowKeyColumnIndex >= 0) {
                ColumnRef projectedColumnRef =
                        new ColumnRef(projectedTableRef, projectedColumn.getPosition());
                sourceRowKeyColumnIndexToProjectedColumnRef.put(
                        Integer.valueOf(sourceRowKeyColumnIndex), projectedColumnRef);
            }
        }

        if(sourceTableRef == null) {
            return new Pair<OrderBy,Integer>(OrderBy.EMPTY_ORDER_BY, 0);
        }

        final int sourceRowKeyColumnOffset = getRowKeyColumnOffset(sourceTableRef.getTable(), phoenixConnection);
        List<OrderByExpression> orderByExpressions = new LinkedList<OrderByExpression>();
        int matchedSourceRowKeyColumnOffset = sourceRowKeyColumnOffset;
        for(Entry<Integer,ColumnRef> entry : sourceRowKeyColumnIndexToProjectedColumnRef.entrySet()) {
            int currentRowKeyColumnOffset = entry.getKey();
            if(currentRowKeyColumnOffset < matchedSourceRowKeyColumnOffset) {
                continue;
            }
            else if(currentRowKeyColumnOffset == matchedSourceRowKeyColumnOffset) {
                matchedSourceRowKeyColumnOffset++;
            }
            else {
                break;
            }

            ColumnRef projectedColumnRef = entry.getValue();
            Expression projectedValueColumnExpression = projectedColumnRef.newColumnExpression();
            OrderByExpression orderByExpression =
                    OrderByExpression.convertExpressionToOrderByExpression(projectedValueColumnExpression, orderByReverse);
            orderByExpressions.add(orderByExpression);
        }

        if(orderByExpressions.isEmpty()) {
            return new Pair<OrderBy,Integer>(OrderBy.EMPTY_ORDER_BY, 0);
        }
        return new Pair<OrderBy,Integer>(new OrderBy(orderByExpressions), sourceRowKeyColumnOffset);
    }

    /**
     * For table with salted/multiTenant/viewIndexId,some leading rowkey columns should be skipped.
     * @param table
     * @param phoenixConnection
     * @return
     */
    public static int getRowKeyColumnOffset(PTable table, PhoenixConnection phoenixConnection) {
        boolean isSalted = table.getBucketNum() != null;
        boolean isMultiTenant = phoenixConnection.getTenantId() != null && table.isMultiTenant();
        boolean isSharedViewIndex = table.getViewIndexId() != null;
        return (isSalted ? 1 : 0) + (isMultiTenant ? 1 : 0) + (isSharedViewIndex ? 1 : 0);
    }

    /**
     * Create {@link RowKeyColumnExpression} from {@link PTable}.
     * The second part of the return pair is the rowkey column offset we must skip when we create OrderBys, because for table with salted/multiTenant/viewIndexId,
     * some leading rowkey columns should be skipped.
     * @param table
     * @param phoenixConnection
     * @return
     */
    public static Pair<List<RowKeyColumnExpression>,Integer> getRowKeyColumnExpressionsFromTable(PTable table, PhoenixConnection phoenixConnection) {
        int pkPositionOffset = getRowKeyColumnOffset(table, phoenixConnection);
        List<PColumn> pkColumns = table.getPKColumns();
        if(pkPositionOffset >= pkColumns.size()) {
            return new Pair<List<RowKeyColumnExpression>,Integer>(Collections.<RowKeyColumnExpression> emptyList(), 0);
        }
        List<RowKeyColumnExpression> rowKeyColumnExpressions = new ArrayList<RowKeyColumnExpression>(pkColumns.size() - pkPositionOffset);
        for(int index = pkPositionOffset; index < pkColumns.size(); index++) {
            RowKeyColumnExpression rowKeyColumnExpression =
                    new RowKeyColumnExpression(pkColumns.get(index), new RowKeyValueAccessor(pkColumns, index));
            rowKeyColumnExpressions.add(rowKeyColumnExpression);
        }
        return new Pair<List<RowKeyColumnExpression>,Integer>(rowKeyColumnExpressions, pkPositionOffset);
    }

    /**
     * Create OrderByExpression by RowKeyColumnExpression,isNullsLast is the default value "false",isAscending is based on {@link Expression#getSortOrder()}.
     * If orderByReverse is true, reverse the isNullsLast and isAscending.
     * @param rowKeyColumnExpressions
     * @param orderByReverse
     * @return
     */
    public static OrderBy convertRowKeyColumnExpressionsToOrderBy(List<RowKeyColumnExpression> rowKeyColumnExpressions, boolean orderByReverse) {
        return convertRowKeyColumnExpressionsToOrderBy(
                rowKeyColumnExpressions, Collections.<Info> emptyList(), orderByReverse);
    }

    /**
     * Create OrderByExpression by RowKeyColumnExpression, if the orderPreservingTrackInfos is not null, use isNullsLast and isAscending from orderPreservingTrackInfos.
     * If orderByReverse is true, reverse the isNullsLast and isAscending.
     * @param rowKeyColumnExpressions
     * @param orderPreservingTrackInfos
     * @param orderByReverse
     * @return
     */
    public static OrderBy convertRowKeyColumnExpressionsToOrderBy(
            List<RowKeyColumnExpression> rowKeyColumnExpressions,
            List<Info> orderPreservingTrackInfos,
            boolean orderByReverse) {
        if(rowKeyColumnExpressions.isEmpty()) {
            return OrderBy.EMPTY_ORDER_BY;
        }
        List<OrderByExpression> orderByExpressions = new ArrayList<OrderByExpression>(rowKeyColumnExpressions.size());
        Iterator<Info> orderPreservingTrackInfosIter = null;
        if(orderPreservingTrackInfos != null && orderPreservingTrackInfos.size() > 0) {
            if(orderPreservingTrackInfos.size() != rowKeyColumnExpressions.size()) {
                throw new IllegalStateException(
                        "orderPreservingTrackInfos.size():[" + orderPreservingTrackInfos.size() +
                        "] should equals rowKeyColumnExpressions.size():[" + rowKeyColumnExpressions.size()+"]!");
            }
            orderPreservingTrackInfosIter = orderPreservingTrackInfos.iterator();
        }
        for(RowKeyColumnExpression rowKeyColumnExpression : rowKeyColumnExpressions) {
            Info orderPreservingTrackInfo = null;
            if(orderPreservingTrackInfosIter != null) {
                assert orderPreservingTrackInfosIter.hasNext();
                orderPreservingTrackInfo = orderPreservingTrackInfosIter.next();
            }
            OrderByExpression orderByExpression =
                    OrderByExpression.convertExpressionToOrderByExpression(rowKeyColumnExpression, orderPreservingTrackInfo, orderByReverse);
            orderByExpressions.add(orderByExpression);
        }
        return new OrderBy(orderByExpressions);
    }

    /**
     * Convert the GroupBy to OrderBy, expressions in GroupBy should be converted to {@link RowKeyColumnExpression}.
     * @param groupBy
     * @param orderByReverse
     * @return
     */
    public static OrderBy convertGroupByToOrderBy(GroupBy groupBy, boolean orderByReverse) {
        if(groupBy.isEmpty()) {
            return OrderBy.EMPTY_ORDER_BY;
        }
        List<RowKeyColumnExpression> rowKeyColumnExpressions = convertGroupByToRowKeyColumnExpressions(groupBy);
        List<Info> orderPreservingTrackInfos = Collections.<Info> emptyList();
        if(groupBy.isOrderPreserving()) {
            orderPreservingTrackInfos = groupBy.getOrderPreservingTrackInfos();
        }
        return convertRowKeyColumnExpressionsToOrderBy(rowKeyColumnExpressions, orderPreservingTrackInfos, orderByReverse);
    }

    /**
     * Convert the expressions in GroupBy to {@link RowKeyColumnExpression}, the convert logic is same as {@link ExpressionCompiler#wrapGroupByExpression}.
     * @param groupBy
     * @return
     */
    public static List<RowKeyColumnExpression> convertGroupByToRowKeyColumnExpressions(GroupBy groupBy) {
        if(groupBy.isEmpty()) {
            return Collections.<RowKeyColumnExpression> emptyList();
        }
        List<Expression> groupByExpressions = groupBy.getExpressions();
        List<RowKeyColumnExpression> rowKeyColumnExpressions = new ArrayList<RowKeyColumnExpression>(groupByExpressions.size());
        int columnIndex = 0;
        for(Expression groupByExpression : groupByExpressions) {
            RowKeyColumnExpression rowKeyColumnExpression =
                    convertGroupByExpressionToRowKeyColumnExpression(groupBy, groupByExpression, columnIndex++);
            rowKeyColumnExpressions.add(rowKeyColumnExpression);
        }
        return rowKeyColumnExpressions;
    }

    /**
     * Convert the expressions in GroupBy to {@link RowKeyColumnExpression}, a typical case is in {@link ExpressionCompiler#wrapGroupByExpression}.
     * @param groupBy
     * @param originalExpression
     * @param groupByColumnIndex
     * @return
     */
    public static RowKeyColumnExpression convertGroupByExpressionToRowKeyColumnExpression(
            GroupBy groupBy,
            Expression originalExpression,
            int groupByColumnIndex) {
        RowKeyValueAccessor rowKeyValueAccessor = new RowKeyValueAccessor(groupBy.getKeyExpressions(), groupByColumnIndex);
        return new RowKeyColumnExpression(
                originalExpression,
                rowKeyValueAccessor,
                groupBy.getKeyExpressions().get(groupByColumnIndex).getDataType());
    }
}
