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
import java.sql.SQLFeatureNotSupportedException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.visitor.KeyValueExpressionVisitor;
import org.apache.phoenix.filter.MultiCFCQKeyValueComparisonFilter;
import org.apache.phoenix.filter.MultiCQKeyValueComparisonFilter;
import org.apache.phoenix.filter.RowKeyComparisonFilter;
import org.apache.phoenix.filter.SingleCFCQKeyValueComparisonFilter;
import org.apache.phoenix.filter.SingleCQKeyValueComparisonFilter;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.StatelessTraverseAllParseNodeVisitor;
import org.apache.phoenix.parse.SubqueryParseNode;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;


/**
 *
 * Class to build the filter of a scan
 *
 * 
 * @since 0.1
 */
public class WhereCompiler {
    protected static final ParseNodeFactory NODE_FACTORY = new ParseNodeFactory();

    private WhereCompiler() {
    }

    public static Expression compile(StatementContext context, FilterableStatement statement) throws SQLException {
        return compile(context, statement, null, null);
    }

    public static Expression compile(StatementContext context, ParseNode whereNode) throws SQLException {
        WhereExpressionCompiler viewWhereCompiler = new WhereExpressionCompiler(context, true);
        return whereNode.accept(viewWhereCompiler);
    }
    
    /**
     * Pushes where clause filter expressions into scan by building and setting a filter.
     * @param context the shared context during query compilation
     * @param statement TODO
     * @throws SQLException if mismatched types are found, bind value do not match binds,
     * or invalid function arguments are encountered.
     * @throws SQLFeatureNotSupportedException if an unsupported expression is encountered.
     * @throws ColumnNotFoundException if column name could not be resolved
     * @throws AmbiguousColumnException if an unaliased column name is ambiguous across multiple tables
     */
    public static Expression compile(StatementContext context, FilterableStatement statement, ParseNode viewWhere, Set<SubqueryParseNode> subqueryNodes) throws SQLException {
        return compile(context, statement, viewWhere, Collections.<Expression>emptyList(), false, subqueryNodes);
    }
    
    /**
     * Optimize scan ranges by applying dynamically generated filter expressions.
     * @param context the shared context during query compilation
     * @param statement TODO
     * @throws SQLException if mismatched types are found, bind value do not match binds,
     * or invalid function arguments are encountered.
     * @throws SQLFeatureNotSupportedException if an unsupported expression is encountered.
     * @throws ColumnNotFoundException if column name could not be resolved
     * @throws AmbiguousColumnException if an unaliased column name is ambiguous across multiple tables
     */    
    public static Expression compile(StatementContext context, FilterableStatement statement, ParseNode viewWhere, List<Expression> dynamicFilters, boolean hashJoinOptimization, Set<SubqueryParseNode> subqueryNodes) throws SQLException {
        ParseNode where = statement.getWhere();
        if (subqueryNodes != null) { // if the subqueryNodes passed in is null, we assume there will be no sub-queries in the WHERE clause.
            SubqueryParseNodeVisitor subqueryVisitor = new SubqueryParseNodeVisitor(context, subqueryNodes);
            if (where != null) {
                where.accept(subqueryVisitor);
            }
            if (viewWhere != null) {
                viewWhere.accept(subqueryVisitor);
            }
            if (!subqueryNodes.isEmpty()) {
                return null;
            }
        }
        
        Set<Expression> extractedNodes = Sets.<Expression>newHashSet();
        WhereExpressionCompiler whereCompiler = new WhereExpressionCompiler(context);
        Expression expression = where == null ? LiteralExpression.newConstant(true, PBoolean.INSTANCE,Determinism.ALWAYS) : where.accept(whereCompiler);
        if (whereCompiler.isAggregate()) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.AGGREGATE_IN_WHERE).build().buildException();
        }
        if (expression.getDataType() != PBoolean.INSTANCE) {
            throw TypeMismatchException.newException(PBoolean.INSTANCE, expression.getDataType(), expression.toString());
        }
        if (viewWhere != null) {
            WhereExpressionCompiler viewWhereCompiler = new WhereExpressionCompiler(context, true);
            Expression viewExpression = viewWhere.accept(viewWhereCompiler);
            expression = AndExpression.create(Lists.newArrayList(expression, viewExpression));
        }
        if (!dynamicFilters.isEmpty()) {
            List<Expression> filters = Lists.newArrayList(expression);
            filters.addAll(dynamicFilters);
            expression = AndExpression.create(filters);
        }
        
        if (context.getCurrentTable().getTable().getType() != PTableType.PROJECTED && context.getCurrentTable().getTable().getType() != PTableType.SUBQUERY) {
            expression = WhereOptimizer.pushKeyExpressionsToScan(context, statement, expression, extractedNodes);
        }
        setScanFilter(context, statement, expression, whereCompiler.disambiguateWithFamily, hashJoinOptimization);

        return expression;
    }
    
    private static class WhereExpressionCompiler extends ExpressionCompiler {
        private boolean disambiguateWithFamily;

        WhereExpressionCompiler(StatementContext context) {
            super(context, true);
        }

        WhereExpressionCompiler(StatementContext context, boolean resolveViewConstants) {
            super(context, resolveViewConstants);
        }

        @Override
        public Expression visit(ColumnParseNode node) throws SQLException {
            ColumnRef ref = resolveColumn(node);
            TableRef tableRef = ref.getTableRef();
            if (tableRef.equals(context.getCurrentTable()) && !SchemaUtil.isPKColumn(ref.getColumn())) {
                // track the where condition columns. Later we need to ensure the Scan in HRS scans these column CFs
                context.addWhereCoditionColumn(ref.getColumn().getFamilyName().getBytes(), ref.getColumn().getName()
                        .getBytes());
            }
            return ref.newColumnExpression(node.isTableNameCaseSensitive(), node.isCaseSensitive());
        }

        @Override
        protected ColumnRef resolveColumn(ColumnParseNode node) throws SQLException {
            ColumnRef ref = super.resolveColumn(node);
            PTable table = ref.getTable();
            // if current table in the context is local index and table in column reference is global means
            // the column is not present in the local index. If where condition contains the column 
            // not present in the index then we need to go through main table for each row in index and get the
            // missing column which is like full scan of index table and data table. Which is
            // inefficient. Then we can skip this plan.
            if (context.getCurrentTable().getTable().getIndexType() == IndexType.LOCAL
                    && (table.getIndexType() == null || table.getIndexType() == IndexType.GLOBAL)) {
                throw new ColumnNotFoundException(ref.getColumn().getName().getString());
            }
            // Track if we need to compare KeyValue during filter evaluation
            // using column family. If the column qualifier is enough, we
            // just use that.
            try {
                if (!SchemaUtil.isPKColumn(ref.getColumn())) {
                    table.getColumn(ref.getColumn().getName().getString());
                }
            } catch (AmbiguousColumnException e) {
                disambiguateWithFamily = true;
            }
            return ref;
         }
    }

    private static final class Counter {
        public enum Count {NONE, SINGLE, MULTIPLE};
        private Count count = Count.NONE;
        private KeyValueColumnExpression column;

        public void increment(KeyValueColumnExpression column) {
            switch (count) {
                case NONE:
                    count = Count.SINGLE;
                    this.column = column;
                    break;
                case SINGLE:
                    count = column.equals(this.column) ? Count.SINGLE : Count.MULTIPLE;
                    break;
                case MULTIPLE:
                    break;

            }
        }
        public Count getCount() {
            return count;
        }
    }

    /**
     * Sets the start/stop key range based on the whereClause expression.
     * @param context the shared context during query compilation
     * @param whereClause the final where clause expression.
     */
    private static void setScanFilter(StatementContext context, FilterableStatement statement, Expression whereClause, boolean disambiguateWithFamily, boolean hashJoinOptimization) {
        Scan scan = context.getScan();

        if (LiteralExpression.isFalse(whereClause)) {
            context.setScanRanges(ScanRanges.NOTHING);
        } else if (whereClause != null && !LiteralExpression.isTrue(whereClause) && !hashJoinOptimization) {
            Filter filter = null;
            final Counter counter = new Counter();
            whereClause.accept(new KeyValueExpressionVisitor() {

                @Override
                public Iterator<Expression> defaultIterator(Expression node) {
                    // Stop traversal once we've found multiple KeyValue columns
                    if (counter.getCount() == Counter.Count.MULTIPLE) {
                        return Iterators.emptyIterator();
                    }
                    return super.defaultIterator(node);
                }

                @Override
                public Void visit(KeyValueColumnExpression expression) {
                    counter.increment(expression);
                    return null;
                }
            });
            switch (counter.getCount()) {
            case NONE:
                PTable table = context.getResolver().getTables().get(0).getTable();
                byte[] essentialCF = table.getType() == PTableType.VIEW 
                        ? ByteUtil.EMPTY_BYTE_ARRAY 
                        : SchemaUtil.getEmptyColumnFamily(table);
                filter = new RowKeyComparisonFilter(whereClause, essentialCF);
                break;
            case SINGLE:
                filter = disambiguateWithFamily ? new SingleCFCQKeyValueComparisonFilter(whereClause) : new SingleCQKeyValueComparisonFilter(whereClause);
                break;
            case MULTIPLE:
                filter = disambiguateWithFamily ? new MultiCFCQKeyValueComparisonFilter(whereClause) : new MultiCQKeyValueComparisonFilter(whereClause);
                break;
            }
            scan.setFilter(filter);
        }

        ScanRanges scanRanges = context.getScanRanges();
        if (scanRanges.useSkipScanFilter()) {
            ScanUtil.andFilterAtBeginning(scan, scanRanges.getSkipScanFilter());
        }
    }
    
    private static class SubqueryParseNodeVisitor extends StatelessTraverseAllParseNodeVisitor {
        private final StatementContext context;
        private final Set<SubqueryParseNode> subqueryNodes;
        
        SubqueryParseNodeVisitor(StatementContext context, Set<SubqueryParseNode> subqueryNodes) {
            this.context = context;
            this.subqueryNodes = subqueryNodes;
        }
        
        @Override
        public Void visit(SubqueryParseNode node) throws SQLException {
            SelectStatement select = node.getSelectNode();
            if (!context.isSubqueryResultAvailable(select)) {
                this.subqueryNodes.add(node);
            }
            return null;                
        }
        
    }
}
