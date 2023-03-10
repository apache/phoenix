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

import static org.apache.phoenix.util.EncodedColumnsUtil.isPossibleToUseEncodedCQFilter;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.phoenix.thirdparty.com.google.common.base.Optional;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.visitor.KeyValueExpressionVisitor;
import org.apache.phoenix.filter.MultiCFCQKeyValueComparisonFilter;
import org.apache.phoenix.filter.MultiCQKeyValueComparisonFilter;
import org.apache.phoenix.filter.MultiEncodedCQKeyValueComparisonFilter;
import org.apache.phoenix.filter.RowKeyComparisonFilter;
import org.apache.phoenix.filter.SingleCFCQKeyValueComparisonFilter;
import org.apache.phoenix.filter.SingleCQKeyValueComparisonFilter;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.StatelessTraverseAllParseNodeVisitor;
import org.apache.phoenix.parse.SubqueryParseNode;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTable.QualifierEncodingScheme;
import org.apache.phoenix.schema.PTable.ViewType;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.ExpressionUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Sets;


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
        return compile(context, statement, null, null, Optional.<byte[]>absent());
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
    public static Expression compile(StatementContext context, FilterableStatement statement, ParseNode viewWhere, Set<SubqueryParseNode> subqueryNodes, Optional<byte[]> minOffset) throws SQLException {
        return compile(context, statement, viewWhere, Collections.<Expression>emptyList(), subqueryNodes, minOffset);
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
    public static Expression compile(StatementContext context, FilterableStatement statement, ParseNode viewWhere, List<Expression> dynamicFilters, Set<SubqueryParseNode> subqueryNodes, Optional<byte[]> minOffset) throws SQLException {
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
            Set<HintNode.Hint> hints = null;
            if(statement.getHint() != null){
                hints = statement.getHint().getHints();
            }
            expression = WhereOptimizer.pushKeyExpressionsToScan(context, hints, expression, extractedNodes, minOffset);
        }
        setScanFilter(context, statement, expression, whereCompiler.disambiguateWithFamily);

        return expression;
    }
    
    public static class WhereExpressionCompiler extends ExpressionCompiler {
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
            Expression newColumnExpression = ref.newColumnExpression(node.isTableNameCaseSensitive(), node.isCaseSensitive());
            if (tableRef.equals(context.getCurrentTable()) && !SchemaUtil.isPKColumn(ref.getColumn())) {
                byte[] cq = tableRef.getTable().getImmutableStorageScheme() == ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS 
                		? QueryConstants.SINGLE_KEYVALUE_COLUMN_QUALIFIER_BYTES : ref.getColumn().getColumnQualifierBytes();
                // track the where condition columns. Later we need to ensure the Scan in HRS scans these column CFs
                context.addWhereConditionColumn(ref.getColumn().getFamilyName().getBytes(), cq);
            }
			return newColumnExpression;
        }

        @Override
        protected ColumnRef resolveColumn(ColumnParseNode node) throws SQLException {
            ColumnRef ref = super.resolveColumn(node);
            if (disambiguateWithFamily) {
                return ref;
            }
            PTable table = ref.getTable();
            // Track if we need to compare KeyValue during filter evaluation
            // using column family. If the column qualifier is enough, we
            // just use that.
            if (!SchemaUtil.isPKColumn(ref.getColumn())) {
                if (!EncodedColumnsUtil.usesEncodedColumnNames(table)
                    || ref.getColumn().isDynamic()) {
                    try {
                        table.getColumnForColumnName(ref.getColumn().getName().getString());
                    } catch (AmbiguousColumnException e) {
                        disambiguateWithFamily = true;
                    }
                } else {
                    for (PColumnFamily columnFamily : table.getColumnFamilies()) {
                        if (columnFamily.getName().equals(ref.getColumn().getFamilyName())) {
                            continue;
                        }
                        try {
                            table.getColumnForColumnQualifier(columnFamily.getName().getBytes(),
                                ref.getColumn().getColumnQualifierBytes());
                            // If we find the same qualifier name with different columnFamily,
                            // then set disambiguateWithFamily to true
                            disambiguateWithFamily = true;
                            break;
                        } catch (ColumnNotFoundException ignore) {
                        }
                    }
                }
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
        
        public KeyValueColumnExpression getColumn() {
            return column;
        }
    }

    /**
     * Sets the start/stop key range based on the whereClause expression.
     * @param context the shared context during query compilation
     * @param whereClause the final where clause expression.
     */
    private static void setScanFilter(StatementContext context, FilterableStatement statement, Expression whereClause, boolean disambiguateWithFamily) {
        Scan scan = context.getScan();

        if (LiteralExpression.isBooleanFalseOrNull(whereClause)) {
            context.setScanRanges(ScanRanges.NOTHING);
        } else if (context.getCurrentTable().getTable().getIndexType() == IndexType.LOCAL
                || (context.getCurrentTable().getTable().getIndexType() == IndexType.GLOBAL
                && context.isUncoveredIndex())) {
            if (whereClause != null && !ExpressionUtil.evaluatesToTrue(whereClause)) {
                // pass any extra where as scan attribute so it can be evaluated after all
                // columns from the main CF have been merged in
                ByteArrayOutputStream stream = new ByteArrayOutputStream();
                try {
                    DataOutputStream output = new DataOutputStream(stream);
                    WritableUtils.writeVInt(output, ExpressionType.valueOf(whereClause).ordinal());
                    whereClause.write(output);
                    stream.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                scan.setAttribute(BaseScannerRegionObserver.INDEX_FILTER, stream.toByteArray());

                // this is needed just for ExplainTable, since de-serializing an expression does not restore
                // its display properties, and that cannot be changed, due to backwards compatibility
                scan.setAttribute(BaseScannerRegionObserver.INDEX_FILTER_STR,
                        Bytes.toBytes(whereClause.toString()));
            }
        } else if (whereClause != null && !ExpressionUtil.evaluatesToTrue(whereClause)) {
            Filter filter = null;
            final Counter counter = new Counter();
            whereClause.accept(new KeyValueExpressionVisitor() {

                @Override
                public Iterator<Expression> defaultIterator(Expression node) {
                    // Stop traversal once we've found multiple KeyValue columns
                    if (counter.getCount() == Counter.Count.MULTIPLE) {
                        return Collections.emptyIterator();
                    }
                    return super.defaultIterator(node);
                }

                @Override
                public Void visit(KeyValueColumnExpression expression) {
                    counter.increment(expression);
                    return null;
                }
            });
            PTable table = context.getCurrentTable().getTable();
            QualifierEncodingScheme encodingScheme = table.getEncodingScheme();
            ImmutableStorageScheme storageScheme = table.getImmutableStorageScheme();
            Counter.Count count = counter.getCount();
            boolean allCFs = false;
            byte[] essentialCF = null;
            if (counter.getCount() == Counter.Count.SINGLE && whereClause.requiresFinalEvaluation() ) {
                if (table.getViewType() == ViewType.MAPPED) {
                    allCFs = true;
                } else {
                    byte[] emptyCF = SchemaUtil.getEmptyColumnFamily(table);
                    if (Bytes.compareTo(emptyCF, counter.getColumn().getColumnFamily()) != 0) {
                        essentialCF = emptyCF;
                        count = Counter.Count.MULTIPLE;
                    }
                }
            }
            switch (count) {
            case NONE:
                essentialCF = table.getType() == PTableType.VIEW 
                        ? ByteUtil.EMPTY_BYTE_ARRAY 
                        : SchemaUtil.getEmptyColumnFamily(table);
                filter = new RowKeyComparisonFilter(whereClause, essentialCF);
                break;
            case SINGLE:
                filter = disambiguateWithFamily 
                    ? new SingleCFCQKeyValueComparisonFilter(whereClause) 
                    : new SingleCQKeyValueComparisonFilter(whereClause);
                break;
            case MULTIPLE:
                filter = isPossibleToUseEncodedCQFilter(encodingScheme, storageScheme) 
                    ? new MultiEncodedCQKeyValueComparisonFilter(whereClause, encodingScheme, allCFs, essentialCF) 
                    : (disambiguateWithFamily 
                        ? new MultiCFCQKeyValueComparisonFilter( whereClause, allCFs, essentialCF) 
                        : new MultiCQKeyValueComparisonFilter(whereClause, allCFs, essentialCF));
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
