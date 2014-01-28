/*
 * Copyright 2010 The Apache Software Foundation
 *
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.coprocessor.GroupedAggregateRegionObserver;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.CoerceExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.aggregator.ClientAggregators;
import org.apache.phoenix.expression.aggregator.ServerAggregators;
import org.apache.phoenix.expression.function.SingleAggregateFunction;
import org.apache.phoenix.expression.visitor.SingleAggregateFunctionVisitor;
import org.apache.phoenix.parse.AliasedNode;
import org.apache.phoenix.parse.BindParseNode;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.FamilyWildcardParseNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.SequenceValueParseNode;
import org.apache.phoenix.parse.WildcardParseNode;
import org.apache.phoenix.schema.ArgumentTypeMismatchException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.ViewType;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.SizedUtil;


/**
 * 
 * Class that iterates through expressions in SELECT clause and adds projected
 * columns to scan.
 *
 * @author jtaylor
 * @since 0.1
 */
public class ProjectionCompiler {
    
    private ProjectionCompiler() {
    }
    
    private static void projectAllColumnFamilies(PTable table, Scan scan) {
        // Will project all known/declared column families
        scan.getFamilyMap().clear();
        for (PColumnFamily family : table.getColumnFamilies()) {
            scan.addFamily(family.getName().getBytes());
        }
    }

    private static void projectColumnFamily(PTable table, Scan scan, byte[] family) {
        // Will project all colmuns for given CF
        scan.addFamily(family);
    }
    
    public static RowProjector compile(StatementContext context, SelectStatement statement, GroupBy groupBy) throws SQLException  {
        return compile(context, statement, groupBy, Collections.<PColumn>emptyList());
    }
    
    private static void projectAllTableColumns(StatementContext context, TableRef tableRef, List<Expression> projectedExpressions, List<ExpressionProjector> projectedColumns) throws SQLException {
        PTable table = tableRef.getTable();
        int posOffset = table.getBucketNum() == null ? 0 : 1;
        // In SELECT *, don't include tenant column for tenant connection
        if (tableRef.getTable().isMultiTenant() && context.getConnection().getTenantId() != null) {
            posOffset++;
        }
        for (int i = posOffset; i < table.getColumns().size(); i++) {
            ColumnRef ref = new ColumnRef(tableRef,i);
            Expression expression = ref.newColumnExpression();
            projectedExpressions.add(expression);
            String colName = ref.getColumn().getName().getString();
            boolean isCaseSensitive = !SchemaUtil.normalizeIdentifier(colName).equals(colName);
            projectedColumns.add(new ExpressionProjector(colName, table.getName().getString(), expression, isCaseSensitive));
        }
    }
    
    private static void projectAllIndexColumns(StatementContext context, TableRef tableRef, List<Expression> projectedExpressions, List<ExpressionProjector> projectedColumns) throws SQLException {
        PTable index = tableRef.getTable();
        PTable table = context.getConnection().getPMetaData().getTable(index.getParentName().getString());
        int tableOffset = table.getBucketNum() == null ? 0 : 1;
        int indexOffset = index.getBucketNum() == null ? 0 : 1;
        if (index.getColumns().size()-indexOffset != table.getColumns().size()-tableOffset) {
            // We'll end up not using this by the optimizer, so just throw
            throw new ColumnNotFoundException(WildcardParseNode.INSTANCE.toString());
        }
        for (int i = tableOffset; i < table.getColumns().size(); i++) {
            PColumn tableColumn = table.getColumns().get(i);
            PColumn indexColumn = index.getColumn(IndexUtil.getIndexColumnName(tableColumn));
            ColumnRef ref = new ColumnRef(tableRef,indexColumn.getPosition());
            Expression expression = ref.newColumnExpression();
            projectedExpressions.add(expression);
            String colName = tableColumn.getName().getString();
            boolean isCaseSensitive = !SchemaUtil.normalizeIdentifier(colName).equals(colName);
            ExpressionProjector projector = new ExpressionProjector(colName, table.getName().getString(), expression, isCaseSensitive);
            projectedColumns.add(projector);
        }
    }
    
    private static void projectTableColumnFamily(StatementContext context, String cfName, TableRef tableRef, List<Expression> projectedExpressions, List<ExpressionProjector> projectedColumns) throws SQLException {
        PTable table = tableRef.getTable();
        PColumnFamily pfamily = table.getColumnFamily(cfName);
        for (PColumn column : pfamily.getColumns()) {
            ColumnRef ref = new ColumnRef(tableRef, column.getPosition());
            Expression expression = ref.newColumnExpression();
            projectedExpressions.add(expression);
            String colName = column.getName().toString();
            boolean isCaseSensitive = !SchemaUtil.normalizeIdentifier(colName).equals(colName);
            projectedColumns.add(new ExpressionProjector(colName, table.getName()
                    .getString(), expression, isCaseSensitive));
        }
    }

    private static void projectIndexColumnFamily(StatementContext context, String cfName, TableRef tableRef, List<Expression> projectedExpressions, List<ExpressionProjector> projectedColumns) throws SQLException {
        PTable index = tableRef.getTable();
        PTable table = context.getConnection().getPMetaData().getTable(index.getParentName().getString());
        PColumnFamily pfamily = table.getColumnFamily(cfName);
        for (PColumn column : pfamily.getColumns()) {
            PColumn indexColumn = index.getColumn(IndexUtil.getIndexColumnName(column));
            ColumnRef ref = new ColumnRef(tableRef, indexColumn.getPosition());
            Expression expression = ref.newColumnExpression();
            projectedExpressions.add(expression);
            String colName = column.getName().toString();
            boolean isCaseSensitive = !SchemaUtil.normalizeIdentifier(colName).equals(colName);
            projectedColumns.add(new ExpressionProjector(colName, 
                    table.getName().getString(), expression, isCaseSensitive));
        }
    }
    
    /**
     * Builds the projection for the scan
     * @param context query context kept between compilation of different query clauses
     * @param statement TODO
     * @param groupBy compiled GROUP BY clause
     * @param targetColumns list of columns, parallel to aliasedNodes, that are being set for an
     * UPSERT SELECT statement. Used to coerce expression types to the expected target type.
     * @return projector used to access row values during scan
     * @throws SQLException 
     */
    public static RowProjector compile(StatementContext context, SelectStatement statement, GroupBy groupBy, List<? extends PDatum> targetColumns) throws SQLException {
        List<AliasedNode> aliasedNodes = statement.getSelect();
        // Setup projected columns in Scan
        SelectClauseVisitor selectVisitor = new SelectClauseVisitor(context, groupBy);
        List<ExpressionProjector> projectedColumns = new ArrayList<ExpressionProjector>();
        TableRef tableRef = context.getResolver().getTables().get(0);
        PTable table = tableRef.getTable();
        boolean isWildcard = false;
        Scan scan = context.getScan();
        int index = 0;
        List<Expression> projectedExpressions = Lists.newArrayListWithExpectedSize(aliasedNodes.size());
        List<byte[]> projectedFamilies = Lists.newArrayListWithExpectedSize(aliasedNodes.size());
        for (AliasedNode aliasedNode : aliasedNodes) {
            ParseNode node = aliasedNode.getNode();
            // TODO: visitor?
            if (node instanceof WildcardParseNode) {
                if (statement.isAggregate()) {
                    ExpressionCompiler.throwNonAggExpressionInAggException(node.toString());
                }
                isWildcard = true;
                if (tableRef.getTable().getType() == PTableType.INDEX && ((WildcardParseNode)node).isRewrite()) {
                	projectAllIndexColumns(context, tableRef, projectedExpressions, projectedColumns);
                } else {
                    projectAllTableColumns(context, tableRef, projectedExpressions, projectedColumns);
                }
            } else if (node instanceof  FamilyWildcardParseNode){
                // Project everything for SELECT cf.*
                // TODO: support cf.* expressions for multiple tables the same way with *.
                String cfName = ((FamilyWildcardParseNode) node).getName();
                // Delay projecting to scan, as when any other column in the column family gets
                // added to the scan, it overwrites that we want to project the entire column
                // family. Instead, we do the projection at the end.
                // TODO: consider having a ScanUtil.addColumn and ScanUtil.addFamily to work
                // around this, as this code depends on this function being the last place where
                // columns are projected (which is currently true, but could change).
                projectedFamilies.add(Bytes.toBytes(cfName));
                if (tableRef.getTable().getType() == PTableType.INDEX && ((FamilyWildcardParseNode)node).isRewrite()) {
                    projectIndexColumnFamily(context, cfName, tableRef, projectedExpressions, projectedColumns);
                } else {
                    projectTableColumnFamily(context, cfName, tableRef, projectedExpressions, projectedColumns);
                }
            } else {
                Expression expression = node.accept(selectVisitor);
                projectedExpressions.add(expression);
                if (index < targetColumns.size()) {
                    PDatum targetColumn = targetColumns.get(index);
                    if (targetColumn.getDataType() != expression.getDataType()) {
                        PDataType targetType = targetColumn.getDataType();
                        // Check if coerce allowed using more relaxed isCastableTo check, since we promote INTEGER to LONG 
                        // during expression evaluation and then convert back to INTEGER on UPSERT SELECT (and we don't have
                        // (an actual value we can specifically check against).
                        if (expression.getDataType() != null && !expression.getDataType().isCastableTo(targetType)) {
                            throw new ArgumentTypeMismatchException(targetType, expression.getDataType(), "column: " + targetColumn);
                        }
                        expression = CoerceExpression.create(expression, targetType);
                    }
                }
                if (node instanceof BindParseNode) {
                    context.getBindManager().addParamMetaData((BindParseNode)node, expression);
                }
                if (!node.isStateless()) {
                    if (!selectVisitor.isAggregate() && statement.isAggregate()) {
                        ExpressionCompiler.throwNonAggExpressionInAggException(expression.toString());
                    }
                }
                String columnAlias = aliasedNode.getAlias() != null ? aliasedNode.getAlias() : SchemaUtil.normalizeIdentifier(aliasedNode.getNode().getAlias());
                boolean isCaseSensitive = aliasedNode.getAlias() != null ? aliasedNode.isCaseSensitve() : (columnAlias != null ? SchemaUtil.isCaseSensitive(aliasedNode.getNode().getAlias()) : selectVisitor.isCaseSensitive);
                String name = columnAlias == null ? expression.toString() : columnAlias;
                projectedColumns.add(new ExpressionProjector(name, table.getName().getString(), expression, isCaseSensitive));
            }
            selectVisitor.reset();
            index++;
        }

        table = context.getCurrentTable().getTable(); // switch to current table for scan projection
        // TODO make estimatedByteSize more accurate by counting the joined columns.
        int estimatedKeySize = table.getRowKeySchema().getEstimatedValueLength();
        int estimatedByteSize = 0;
        for (Map.Entry<byte[],NavigableSet<byte[]>> entry : scan.getFamilyMap().entrySet()) {
            PColumnFamily family = table.getColumnFamily(entry.getKey());
            if (entry.getValue() == null) {
                for (PColumn column : family.getColumns()) {
                    Integer byteSize = column.getByteSize();
                    estimatedByteSize += SizedUtil.KEY_VALUE_SIZE + estimatedKeySize + (byteSize == null ? RowKeySchema.ESTIMATED_VARIABLE_LENGTH_SIZE : byteSize);
                }
            } else {
                for (byte[] cq : entry.getValue()) {
                    PColumn column = family.getColumn(cq);
                    Integer byteSize = column.getByteSize();
                    estimatedByteSize += SizedUtil.KEY_VALUE_SIZE + estimatedKeySize + (byteSize == null ? RowKeySchema.ESTIMATED_VARIABLE_LENGTH_SIZE : byteSize);
                }
            }
        }
        
        selectVisitor.compile();
        // Since we don't have the empty key value in read-only tables,
        // we must project everything.
        boolean isProjectEmptyKeyValue = table.getType() != PTableType.VIEW && table.getViewType() != ViewType.MAPPED && !isWildcard;
        if (isProjectEmptyKeyValue) {
            for (byte[] family : projectedFamilies) {
                projectColumnFamily(table, scan, family);       
            }
        } else {
            /* 
             * TODO: this could be optimized by detecting:
             * - if a column is projected that's not in the where clause
             * - if a column is grouped by that's not in the where clause
             * - if we're not using IS NULL or CASE WHEN expressions
             */
             projectAllColumnFamilies(table,scan);
        }
        return new RowProjector(projectedColumns, estimatedByteSize, isProjectEmptyKeyValue);
    }
        
    private static class SelectClauseVisitor extends ExpressionCompiler {
        private static int getMinNullableIndex(List<SingleAggregateFunction> aggFuncs, boolean isUngroupedAggregation) {
            int minNullableIndex = aggFuncs.size();
            for (int i = 0; i < aggFuncs.size(); i++) {
                SingleAggregateFunction aggFunc = aggFuncs.get(i);
                if (isUngroupedAggregation ? aggFunc.getAggregator().isNullable() : aggFunc.getAggregatorExpression().isNullable()) {
                    minNullableIndex = i;
                    break;
                }
            }
            return minNullableIndex;
        }
        
        /**
         * Track whether or not the projection expression is case sensitive. We use this
         * information to determine whether or not we normalize the column name passed
         */
        private boolean isCaseSensitive;
        private int elementCount;
        
        private SelectClauseVisitor(StatementContext context, GroupBy groupBy) {
            super(context, groupBy);
            reset();
        }


        /**
         * Compiles projection by:
         * 1) Adding RowCount aggregate function if not present when limiting rows. We need this
         *    to track how many rows have been scanned.
         * 2) Reordering aggregation functions (by putting fixed length aggregates first) to
         *    optimize the positional access of the aggregated value.
         */
        private void compile() throws SQLException {
            final Set<SingleAggregateFunction> aggFuncSet = Sets.newHashSetWithExpectedSize(context.getExpressionManager().getExpressionCount());
    
            Iterator<Expression> expressions = context.getExpressionManager().getExpressions();
            while (expressions.hasNext()) {
                Expression expression = expressions.next();
                expression.accept(new SingleAggregateFunctionVisitor() {
                    @Override
                    public Iterator<Expression> visitEnter(SingleAggregateFunction function) {
                        aggFuncSet.add(function);
                        return Iterators.emptyIterator();
                    }
                });
            }
            if (aggFuncSet.isEmpty() && groupBy.isEmpty()) {
                return;
            }
            List<SingleAggregateFunction> aggFuncs = new ArrayList<SingleAggregateFunction>(aggFuncSet);
            Collections.sort(aggFuncs, SingleAggregateFunction.SCHEMA_COMPARATOR);
    
            int minNullableIndex = getMinNullableIndex(aggFuncs,groupBy.isEmpty());
            context.getScan().setAttribute(GroupedAggregateRegionObserver.AGGREGATORS, ServerAggregators.serialize(aggFuncs, minNullableIndex));
            ClientAggregators clientAggregators = new ClientAggregators(aggFuncs, minNullableIndex);
            context.getAggregationManager().setAggregators(clientAggregators);
        }
        
        @Override
        public void reset() {
            super.reset();
            elementCount = 0;
            isCaseSensitive = true;
        }
        
        @Override
        protected ColumnRef resolveColumn(ColumnParseNode node) throws SQLException {
            ColumnRef ref = super.resolveColumn(node);
            isCaseSensitive = isCaseSensitive && node.isCaseSensitive();
            return ref;
        }
        
        @Override
        public void addElement(List<Expression> l, Expression element) {
            elementCount++;
            isCaseSensitive &= elementCount == 1;
            super.addElement(l, element);
        }
        
        @Override
        public Expression visit(SequenceValueParseNode node) throws SQLException {
            if (aggregateFunction != null) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_USE_OF_NEXT_VALUE_FOR)
                .setSchemaName(node.getTableName().getSchemaName())
                .setTableName(node.getTableName().getTableName()).build().buildException();
            }
            return context.getSequenceManager().newSequenceReference(node);
        }
    }
}
