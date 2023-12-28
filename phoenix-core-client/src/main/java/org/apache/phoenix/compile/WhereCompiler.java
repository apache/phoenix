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

import static org.apache.hadoop.hbase.CompareOperator.EQUAL;
import static org.apache.hadoop.hbase.CompareOperator.GREATER;
import static org.apache.hadoop.hbase.CompareOperator.GREATER_OR_EQUAL;
import static org.apache.hadoop.hbase.CompareOperator.LESS;
import static org.apache.hadoop.hbase.CompareOperator.LESS_OR_EQUAL;
import static org.apache.hadoop.hbase.CompareOperator.NOT_EQUAL;
import static org.apache.phoenix.util.EncodedColumnsUtil.isPossibleToUseEncodedCQFilter;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.phoenix.expression.AddExpression;
import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.expression.ArrayConstructorExpression;
import org.apache.phoenix.expression.BaseTerminalExpression;
import org.apache.phoenix.expression.CaseExpression;
import org.apache.phoenix.expression.CoerceExpression;
import org.apache.phoenix.expression.ColumnExpression;
import org.apache.phoenix.expression.ComparisonExpression;
import org.apache.phoenix.expression.CorrelateVariableFieldAccessExpression;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.DivideExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;
import org.apache.phoenix.expression.InListExpression;
import org.apache.phoenix.expression.IsNullExpression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.LikeExpression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.ModulusExpression;
import org.apache.phoenix.expression.MultiplyExpression;
import org.apache.phoenix.expression.NotExpression;
import org.apache.phoenix.expression.OrExpression;
import org.apache.phoenix.expression.ProjectedColumnExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.expression.RowValueConstructorExpression;
import org.apache.phoenix.expression.SingleCellColumnExpression;
import org.apache.phoenix.expression.SingleCellConstructorExpression;
import org.apache.phoenix.expression.StringConcatExpression;
import org.apache.phoenix.expression.SubtractExpression;
import org.apache.phoenix.expression.function.ArrayAnyComparisonExpression;
import org.apache.phoenix.expression.function.ArrayElemRefExpression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.expression.function.SingleAggregateFunction;
import org.apache.phoenix.expression.visitor.TraverseAllExpressionVisitor;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.StatelessTraverseAllParseNodeVisitor;
import org.apache.phoenix.parse.SubqueryParseNode;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.thirdparty.com.google.common.base.Optional;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.visitor.KeyValueExpressionVisitor;
import org.apache.phoenix.filter.MultiCFCQKeyValueComparisonFilter;
import org.apache.phoenix.filter.MultiCQKeyValueComparisonFilter;
import org.apache.phoenix.filter.MultiEncodedCQKeyValueComparisonFilter;
import org.apache.phoenix.filter.RowKeyComparisonFilter;
import org.apache.phoenix.filter.SingleCFCQKeyValueComparisonFilter;
import org.apache.phoenix.filter.SingleCQKeyValueComparisonFilter;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTable.QualifierEncodingScheme;
import org.apache.phoenix.schema.PTable.ViewType;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Sets;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.ExpressionUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;

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

        public WhereExpressionCompiler(StatementContext context) {
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
                || (IndexUtil.isGlobalIndex(context.getCurrentTable().getTable())
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
                scan.setAttribute(BaseScannerRegionObserverConstants.INDEX_FILTER, stream.toByteArray());

                // this is needed just for ExplainTable, since de-serializing an expression does not restore
                // its display properties, and that cannot be changed, due to backwards compatibility
                scan.setAttribute(BaseScannerRegionObserverConstants.INDEX_FILTER_STR,
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


    public static Expression transformDNF(ParseNode where, StatementContext statementContext)
            throws SQLException {
        if (where == null) {
            return null;
        }
        StatementContext context = new StatementContext(statementContext);
        context.setResolver(FromCompiler.getResolver(context.getCurrentTable()));
        Expression expression = where.accept(new WhereExpressionCompiler(context));
        Expression dnf = expression.accept(new DNFExpressionRewriter());
        return dnf;
    }

    /**
     * Rewrites an expression in DNF (Disjunctive Normal Form). To do that
     * (1) it transforms operators like RVC, IN, and BETWEEN to their AND/OR equivalents,
     * (2) eliminate double negations and apply DeMorgan rule, i.e.,
     *      NOT (A AND B) = NOT A OR NOT B and  NOT (A OR B) = NOT A AND NOT B, and
     * (3) distributes AND over OR, i.e.,
     *      (A OR B) AND (C OR D) = (A AND C) OR (A AND D) OR (B AND C) OR (B AND D).
     */
    public static class DNFExpressionRewriter extends TraverseAllExpressionVisitor<Expression> {
        /**
         * Flattens nested AND expressions.
         * For example A > 10 AND (B = 10 AND C > 0) is an AndExpression with two children that are
         * A > 10 and (B = 10 AND C > 0). Note the second child is another AndExpression. This is
         * flattened as an AndExpression ( A > 10 AND B = 10 AND C > 0) with three
         * children that are  A > 10, B = 10, and C > 0.
         *
         */

        private static AndExpression flattenAnd(List<Expression> l) {
            for (Expression e : l) {
                if (e instanceof AndExpression) {
                    List<Expression> flattenedList = new ArrayList<>(l.size()
                            + e.getChildren().size());
                    for (Expression child : l) {
                        if (child instanceof AndExpression) {
                            flattenedList.addAll(child.getChildren());
                        } else {
                            flattenedList.add(child);
                        }
                    }
                    return new AndExpression(flattenedList);
                }
            }
            return new AndExpression(l);
        }

        /**
         * Flattens nested OR expressions.
         * For example A > 10 OR (B = 10 OR C > 0) is an OrExpression with two children that are
         * A > 10 and (B = 10 OR C > 0). Note the second child is another OrExpression. This is
         * flattened as an OrExpression  ( A > 10 OR B = 10 OR C > 0) with three
         * children that are  A > 10, B = 10, and C > 0.
         *
         */
        private static OrExpression flattenOr(List<Expression> l) {
            for (Expression e : l) {
                if (e instanceof OrExpression) {
                    List<Expression> flattenedList = new ArrayList<>(l.size()
                            + e.getChildren().size());
                    for (Expression child : l) {
                        if (child instanceof OrExpression) {
                            flattenedList.addAll(child.getChildren());
                        } else {
                            flattenedList.add(child);
                        }
                    }
                    return new OrExpression(flattenedList);
                }
            }
            return new OrExpression(l);
        }

        /**
         * Flattens nested AND expressions and then distributes AND over OR.
         *
         */
        @Override public Expression visitLeave(AndExpression node, List<Expression> l) {
            AndExpression andExpression = flattenAnd(l);

            boolean foundOrChild = false;
            int i;
            Expression child = null;
            List<Expression> andChildren = andExpression.getChildren();
            for (i = 0; i < andChildren.size(); i++) {
                child = andChildren.get(i);
                if (child instanceof OrExpression) {
                    foundOrChild = true;
                    break;
                }
            }

            if (foundOrChild) {
                List<Expression> flattenedList = new ArrayList<>(andChildren.size() - 1);
                for (int j = 0; j < andChildren.size(); j++) {
                    if (i != j) {
                        flattenedList.add(andChildren.get(j));
                    }
                }
                List<Expression> orList = new ArrayList<>(child.getChildren().size());
                for (Expression grandChild : child.getChildren()) {
                    List<Expression> andList = new ArrayList<>(l.size());
                    andList.addAll(flattenedList);
                    andList.add(grandChild);
                    orList.add(visitLeave(new AndExpression(andList), andList));
                }
                return visitLeave(new OrExpression(orList), orList);
            }
            return andExpression;
        }
        @Override public Expression visitLeave(OrExpression node, List<Expression> l) {
            return flattenOr(l);
        }

        @Override public Expression visitLeave(ScalarFunction node, List<Expression> l) {
            return node;
        }

        private static ComparisonExpression createComparisonExpression(CompareOperator op,
                Expression lhs, Expression rhs) {
            List<Expression> children = new ArrayList<>(2);
            children.add(lhs);
            children.add(rhs);
            return new ComparisonExpression(children, op);
        }

        @Override public Expression visitLeave(ComparisonExpression node, List<Expression> l) {
            if (l == null || l.isEmpty()) {
                return node;
            }
            Expression lhs = l.get(0);
            Expression rhs = l.get(1);
            if (!(lhs instanceof RowValueConstructorExpression)
                    || !(rhs instanceof RowValueConstructorExpression)) {
                return new ComparisonExpression(l, node.getFilterOp());
            }

            // Rewrite RVC in DNF (Disjunctive Normal Form)
            // For example
            // (A, B, C ) op (a, b, c) where op is == or != equals to
            // (A != a and B != b and C != c)
            // (A, B, C ) op (a, b, c) where op is <, <=, >, or >= is equals to
            // (A == a and B == b and C op c) or (A == a and  B op b) or A op c

            int childCount = lhs.getChildren().size();
            if (node.getFilterOp() == EQUAL
                    || node.getFilterOp() == NOT_EQUAL) {
                List<Expression> andList = new ArrayList<>(childCount);
                for (int i = 0; i < childCount; i++) {
                    andList.add(createComparisonExpression(node.getFilterOp(),
                            lhs.getChildren().get(i),
                            rhs.getChildren().get(i)));
                }
                return new AndExpression(andList);
            }
            List<Expression> orList = new ArrayList<>(childCount);
            for (int i = 0; i < childCount; i++) {
                List<Expression> andList = new ArrayList<>(childCount);
                int j;
                for (j = 0; j < childCount - i - 1; j++) {
                    andList.add(createComparisonExpression(EQUAL, lhs.getChildren().get(j),
                            rhs.getChildren().get(j)));
                }
                andList.add(createComparisonExpression(node.getFilterOp(), lhs.getChildren().get(j),
                        rhs.getChildren().get(j)));
                orList.add(new AndExpression(andList));
            }
            return new OrExpression(orList);
        }

        @Override public Expression visitLeave(LikeExpression node, List<Expression> l) {
            return node;
        }

        @Override public Expression visitLeave(SingleAggregateFunction node, List<Expression> l) {
            return node;
        }

        @Override public Expression visitLeave(CaseExpression node, List<Expression> l) {
            return node;
        }

        private static Expression negate(ComparisonExpression node) {
            CompareOperator op = node.getFilterOp();
            Expression lhs = node.getChildren().get(0);
            Expression rhs = node.getChildren().get(1);
            switch (op) {
            case LESS:
                return createComparisonExpression(GREATER_OR_EQUAL, lhs, rhs);
            case LESS_OR_EQUAL:
                return createComparisonExpression(GREATER, lhs, rhs);
            case EQUAL:
                return createComparisonExpression(NOT_EQUAL, lhs, rhs);
            case NOT_EQUAL:
                return createComparisonExpression(EQUAL, lhs, rhs);
            case GREATER_OR_EQUAL:
                return createComparisonExpression(LESS, lhs, rhs);
            case GREATER:
                return createComparisonExpression(LESS_OR_EQUAL, lhs, rhs);
            default:
                throw new IllegalArgumentException("Unexpected CompareOp of " + op);
            }
        }
        private static List<Expression> negateChildren(List<Expression> children) {
            List<Expression> list = new ArrayList<>(children.size());
            for (Expression child : children) {
                if (child instanceof ComparisonExpression) {
                    list.add(negate((ComparisonExpression) child));
                } else if (child instanceof OrExpression) {
                    list.add(negate((OrExpression) child));
                } else if (child instanceof AndExpression) {
                    list.add(negate((AndExpression) child));
                } else if (child instanceof ColumnExpression) {
                    list.add(new NotExpression(child));
                } else if (child instanceof NotExpression) {
                    list.add(child.getChildren().get(0));
                } else {
                    throw new IllegalArgumentException("Unexpected Instance of " + child);
                }
            }
            return list;
        }
        private static Expression negate(OrExpression node) {
            return new AndExpression(negateChildren(node.getChildren()));
        }

        private static Expression negate(AndExpression node) {
            return new OrExpression(negateChildren(node.getChildren()));
        }
        @Override public Expression visitLeave(NotExpression node, List<Expression> l) {
            Expression child = l.get(0);
            if (child instanceof OrExpression) {
                return negate((OrExpression) child);
            } else if (child instanceof AndExpression) {
                return negate((AndExpression) child);
            } else if (child instanceof ComparisonExpression) {
                return negate((ComparisonExpression) child);
            } else if (child instanceof NotExpression) {
                return child.getChildren().get(0);
            } else if (child instanceof IsNullExpression) {
                return new IsNullExpression(ImmutableList.of(l.get(0).getChildren().get(0)),
                        !((IsNullExpression) child).isNegate());
            } else {
                return new NotExpression(child);
            }
        }

        private Expression transformInList(InListExpression node, boolean negate,
                List<Expression> l) {
            List<Expression> list = new ArrayList<>(node.getKeyExpressions().size());
            for (Expression element : node.getKeyExpressions()) {
                if (negate) {
                    list.add(createComparisonExpression(NOT_EQUAL, l.get(0), element));
                } else {
                    list.add(createComparisonExpression(EQUAL, l.get(0), element));
                }
            }
            if (negate) {
                return new AndExpression(list);
            } else {
                return new OrExpression(list);
            }
        }

        @Override public Expression visitLeave(InListExpression node, List<Expression> l) {
            Expression inList = transformInList(node, false, l);
            Expression firstElement = inList.getChildren().get(0);
            // Check if inList includes RVC expressions. If so, rewrite them
            if (firstElement instanceof ComparisonExpression
                    && firstElement.getChildren().get(0) instanceof RowValueConstructorExpression) {
                List<Expression> list = new ArrayList<>(node.getKeyExpressions().size());
                for (Expression e : inList.getChildren()) {
                    list.add(visitLeave((ComparisonExpression) e, e.getChildren()));
                }
                if (inList instanceof OrExpression) {
                    return visitLeave(new OrExpression(list), list);
                } else {
                    return visitLeave(new AndExpression(list), list);
                }
            } else {
                return inList;
            }
        }

        @Override public Expression visitLeave(IsNullExpression node, List<Expression> l) {
            return node;
        }

        @Override public Expression visitLeave(SubtractExpression node, List<Expression> l) {
            return node;
        }

        @Override public Expression visitLeave(MultiplyExpression node, List<Expression> l) {
            return node;
        }

        @Override public Expression visitLeave(AddExpression node, List<Expression> l) {
            return node;
        }

        @Override public Expression visitLeave(DivideExpression node, List<Expression> l) {
            return node;
        }

        @Override public Expression visitLeave(CoerceExpression node, List<Expression> l) {
            return node;
        }

        @Override
        public Expression visitLeave(ArrayConstructorExpression node, List<Expression> l) {
            return node;
        }

        @Override
        public Expression visitLeave(SingleCellConstructorExpression node, List<Expression> l) {
            return node;
        }

        @Override public Expression visit(CorrelateVariableFieldAccessExpression node) {
            return node;
        }

        @Override public Expression visit(LiteralExpression node) {
            return node;
        }

        @Override public Expression visit(RowKeyColumnExpression node) {
            return node;
        }

        @Override public Expression visit(KeyValueColumnExpression node) {
            return node;
        }

        @Override public Expression visit(SingleCellColumnExpression node) {
            return node;
        }

        @Override public Expression visit(ProjectedColumnExpression node) {
            return node;
        }

        @Override public Expression visit(SequenceValueExpression node) {
            return node;
        }

        @Override public Expression visitLeave(StringConcatExpression node, List<Expression> l) {
            return node;
        }

        @Override
        public Expression visitLeave(RowValueConstructorExpression node, List<Expression> l) {
            return node;
        }

        @Override public Expression visitLeave(ModulusExpression node, List<Expression> l) {
            return node;
        }

        @Override
        public Expression visitLeave(ArrayAnyComparisonExpression node, List<Expression> l) {
            return node;
        }

        @Override public Expression visitLeave(ArrayElemRefExpression node, List<Expression> l) {
            return node;
        }
    }

    public static LiteralExpression getLiteralExpression(Expression node) {
        while (!node.getChildren().isEmpty()) {
            node = node.getChildren().get(0);
        }
        if (node instanceof LiteralExpression) {
            return (LiteralExpression) node;
        }
        throw new IllegalArgumentException("Unexpected instance type for " + node);
    }


    public static BaseTerminalExpression getBaseTerminalExpression(Expression node) {
        while (!node.getChildren().isEmpty()) {
            node = node.getChildren().get(0);
        }
        if (node instanceof BaseTerminalExpression) {
            return (BaseTerminalExpression) node;
        }
        throw new IllegalArgumentException("Unexpected instance type for " + node);
    }

    /**
     * Determines if nodeA is contained by nodeB.
     *
     * nodeB contains nodeA if every conjunct of nodeB contains at least one conjunct of nodeA.
     *
     * Example 1: nodeA is contained by nodeB where
     *      nodeA = (A > 5) and (A < 10) and (B > 0) and C = 5, and
     *      nodeB = (A > 0)
     *
     * Example 2: nodeA is not contained by nodeB since C < 0 does not contain any of A's conjuncts
     * where
     *      nodeA = (A > 5) and (A < 10) and (B > 0) and C = 5, and
     *      nodeB = (A > 0) and (C < 0)
     *
     * @param nodeA is a simple term or AndExpression constructed from simple terms
     * @param nodeB is a simple term or AndExpression constructed from simple terms
     * @return true if nodeA is contained by nodeB.
     */
    private static boolean contained(Expression nodeA, Expression nodeB) {
        if (nodeB instanceof AndExpression) {
            for (Expression childB : nodeB.getChildren()) {
                if (nodeA instanceof AndExpression) {
                    boolean contains = false;
                    for (Expression childA : nodeA.getChildren()) {
                        if (childB.contains(childA)) {
                            contains = true;
                            break;
                        }
                    }
                    if (!contains) {
                        return false;
                    }
                } else {
                    // node A is a simple term
                    if (!childB.contains(nodeA)) {
                        return false;
                    }
                }
            }
        } else {
            // node B is a simple term
            if (nodeA instanceof AndExpression) {
                boolean contains = false;
                for (Expression childA : nodeA.getChildren()) {
                    if (nodeB.contains(childA)) {
                        contains = true;
                        break;
                    }
                }
                if (!contains) {
                    return false;
                }
            } else {
                // Both nodeA and nodeB are simple terms
                if (!nodeB.contains(nodeA)) {
                    return false;
                }
            }
        }
        return true;
    }
    /**
     * Determines if node is contained in one of the elements of l
     *
     * @param node is a simple term or AndExpression constructed from simple terms
     * @param l is a list of nodes where a node is a simple term or AndExpression constructed from
     *          simple terms
     * @return true if an element of the list contains node
     */
    private static boolean contained(Expression node, List<Expression> l) {
        for (Expression e : l) {
            if (contained(node, e)) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsDisjunct(Expression nodeA, Expression nodeB)
            throws SQLException {
        // nodeB is a disjunct, that is, either an AND expression or a simple term
        if (nodeA instanceof OrExpression) {
            // node A is an OR expression. The following check if nodeB is contained by
            // any of the disjuncts of nodeA
            if (!contained(nodeB, nodeA.getChildren())) {
                return false;
            }
        } else {
            // Both nodeA and nodeB are either an AND expression or a simple term (e.g., C < 5)
            if (!contained(nodeB, nodeA)) {
                return false;
            }
        }
        return true;
    }
    /**
     * Determines if nodeA contains/implies nodeB. Both nodeA and B are DNF (Disjunctive Normal
     * Form) expressions. nodeA contains nodeB if every disjunct of nodeB is contained
     * by a nodeA disjunct. A disjunct x contains another disjunct y if every conjunct of x
     * contains at least one conjunct of y.
     *
     * Example:
     * nodeA: (A > 0 AND B > 0) OR C < 5
     * nodeB: (A = 5 AND B > 1) OR (A = 3 AND C = 1)
     *
     * Disjuncts of nodeA: (A > 0 AND B > 0) and C < 5
     * Disjuncts of nodeB: (A = 5 AND B > 1) and (A = 3 AND C = 1)
     *
     * Conjuncts of (A > 0 AND B > 0): A > 0 and B > 0
     * Conjuncts of C < 5 : C < 5
     *
     * nodeA contains node B because every disjunct of nodeB is contained
     * by a nodeA disjunct. The first disjunct (A = 5 AND B > 1) is contained by the disjunct
     * (A > 0 AND B > 0). The second disjunct (A = 3 AND C = 1) is contained by C < 5. Please node
     * a disjunct x contains another disjunct y if every conjunct of x contains at least one
     * conjunct of y as in the example above.
     *
     * @param nodeA is an expression in DNF
     * @param nodeB is an expression in DNF
     * @return true if nodeA contains/implies nodeB
     * @throws SQLException
     */
    public static boolean contains(Expression nodeA, Expression nodeB) throws SQLException {
        if (nodeA == null) {
            return true;
        } else if (nodeB == null) {
            return false;
        }
        if (nodeB instanceof OrExpression) {
            // Check if every disjunct of nodeB is contained by a nodeA disjunct
            for (Expression childB : nodeB.getChildren()) {
                if (!containsDisjunct(nodeA, childB)) {
                    return false;
                }
            }
            return true;
        } else {
            // nodeB is either an AND expression or a simple term
            return containsDisjunct(nodeA, nodeB);
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
