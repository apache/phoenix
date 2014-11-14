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
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.JoinCompiler.JoinSpec;
import org.apache.phoenix.compile.JoinCompiler.JoinTable;
import org.apache.phoenix.compile.JoinCompiler.JoinedTableColumnResolver;
import org.apache.phoenix.compile.JoinCompiler.PTableWrapper;
import org.apache.phoenix.compile.JoinCompiler.ProjectedPTableWrapper;
import org.apache.phoenix.compile.JoinCompiler.Table;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.execute.AggregatePlan;
import org.apache.phoenix.execute.ClientAggregatePlan;
import org.apache.phoenix.execute.ClientScanPlan;
import org.apache.phoenix.execute.HashJoinPlan;
import org.apache.phoenix.execute.HashJoinPlan.HashSubPlan;
import org.apache.phoenix.execute.HashJoinPlan.WhereClauseSubPlan;
import org.apache.phoenix.execute.ScanPlan;
import org.apache.phoenix.execute.SortMergeJoinPlan;
import org.apache.phoenix.execute.TupleProjectionPlan;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.RowValueConstructorExpression;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.iterate.ParallelIteratorFactory;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.join.HashJoinInfo;
import org.apache.phoenix.parse.AliasedNode;
import org.apache.phoenix.parse.EqualParseNode;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.parse.JoinTableNode.JoinType;
import org.apache.phoenix.parse.OrderByNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.SubqueryParseNode;
import org.apache.phoenix.parse.TableNode;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.ScanUtil;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;



/**
 *
 * Class used to build an executable query plan
 *
 *
 * @since 0.1
 */
public class QueryCompiler {
    private static final ParseNodeFactory NODE_FACTORY = new ParseNodeFactory();
    /*
     * Not using Scan.setLoadColumnFamiliesOnDemand(true) because we don't
     * want to introduce a dependency on 0.94.5 (where this feature was
     * introduced). This will do the same thing. Once we do have a
     * dependency on 0.94.5 or above, switch this around.
     */
    private static final String LOAD_COLUMN_FAMILIES_ON_DEMAND_ATTR = "_ondemand_";
    private final PhoenixStatement statement;
    private final Scan scan;
    private final Scan originalScan;
    private final ColumnResolver resolver;
    private final SelectStatement select;
    private final List<? extends PDatum> targetColumns;
    private final ParallelIteratorFactory parallelIteratorFactory;
    private final SequenceManager sequenceManager;
    private final boolean useSortMergeJoin;

    public QueryCompiler(PhoenixStatement statement, SelectStatement select, ColumnResolver resolver) throws SQLException {
        this(statement, select, resolver, Collections.<PDatum>emptyList(), null, new SequenceManager(statement));
    }

    public QueryCompiler(PhoenixStatement statement, SelectStatement select, ColumnResolver resolver, List<? extends PDatum> targetColumns, ParallelIteratorFactory parallelIteratorFactory, SequenceManager sequenceManager) throws SQLException {
        this.statement = statement;
        this.select = select;
        this.resolver = resolver;
        this.scan = new Scan();
        this.targetColumns = targetColumns;
        this.parallelIteratorFactory = parallelIteratorFactory;
        this.sequenceManager = sequenceManager;
        this.useSortMergeJoin = select.getHint().hasHint(Hint.USE_SORT_MERGE_JOIN);
        if (statement.getConnection().getQueryServices().getLowestClusterHBaseVersion() >= PhoenixDatabaseMetaData.ESSENTIAL_FAMILY_VERSION_THRESHOLD) {
            this.scan.setAttribute(LOAD_COLUMN_FAMILIES_ON_DEMAND_ATTR, QueryConstants.TRUE);
        }
        if (select.getHint().hasHint(Hint.NO_CACHE)) {
            scan.setCacheBlocks(false);
        }

        scan.setCaching(statement.getFetchSize());
        this.originalScan = ScanUtil.newScan(scan);
    }

    /**
     * Builds an executable query plan from a parsed SQL statement
     * @return executable query plan
     * @throws SQLException if mismatched types are found, bind value do not match binds,
     * or invalid function arguments are encountered.
     * @throws SQLFeatureNotSupportedException if an unsupported construct is encountered
     * @throws TableNotFoundException if table name not found in schema
     * @throws ColumnNotFoundException if column name could not be resolved
     * @throws AmbiguousColumnException if an unaliased column name is ambiguous across multiple tables
     */
    public QueryPlan compile() throws SQLException{
        SelectStatement select = this.select;
        List<Object> binds = statement.getParameters();
        StatementContext context = new StatementContext(statement, resolver, scan, sequenceManager);
        if (select.isJoin()) {
            select = JoinCompiler.optimize(statement, select, resolver);
            if (this.select != select) {
                ColumnResolver resolver = FromCompiler.getResolverForQuery(select, statement.getConnection());
                context = new StatementContext(statement, resolver, scan, sequenceManager);
            }
            JoinTable joinTable = JoinCompiler.compile(statement, select, context.getResolver());
            return compileJoinQuery(context, binds, joinTable, false, false, null);
        } else {
            return compileSingleQuery(context, select, binds, false, true);
        }
    }
    
    /*
     * Call compileJoinQuery() for join queries recursively down to the leaf JoinTable nodes.
     * This matches the input JoinTable node against patterns in the following order:
     * 1. A (leaf JoinTable node, which can be a named table reference or a subquery of any kind.)
     *    Returns the compilation result of a single table scan or of an independent subquery.
     * 2. Matching either of (when hint USE_SORT_MERGE_JOIN not specified):
     *        1) A LEFT/INNER JOIN B
     *        2) A LEFT/INNER JOIN B (LEFT/INNER JOIN C)+, if hint NO_STAR_JOIN not specified
     *        where A can be a named table reference or a flat subquery, and B, C, ... can be a named
     *        table reference, a sub-join or a subquery of any kind.
     *    Returns a HashJoinPlan{scan: A, hash: B, C, ...}.
     * 3. Matching pattern:
     *        A RIGHT/INNER JOIN B (when hint USE_SORT_MERGE_JOIN not specified)
     *        where B can be a named table reference or a flat subquery, and A can be a named table
     *        reference, a sub-join or a subquery of any kind.
     *    Returns a HashJoinPlan{scan: B, hash: A}.
     *    NOTE that "A LEFT/RIGHT/INNER/FULL JOIN B RIGHT/INNER JOIN C" is viewed as
     *    "(A LEFT/RIGHT/INNER/FULL JOIN B) RIGHT/INNER JOIN C" here, which means the left part in the
     *    parenthesis is considered a sub-join.
     *    viewed as a sub-join.
     * 4. All the rest that do not qualify for previous patterns or conditions, including FULL joins.
     *    Returns a SortMergeJoinPlan, the sorting part of which is pushed down to the JoinTable nodes
     *    of both sides as order-by clauses.
     * NOTE that SEMI or ANTI joins are treated the same way as LEFT joins in JoinTable pattern matching.
     *    
     * If no join algorithm hint is provided, according to the above compilation process, a join query 
     * plan can probably consist of both HashJoinPlan and SortMergeJoinPlan which may enclose each other.
     * TODO 1) Use table statistics to guide the choice of join plans.
     *      2) Make it possible to hint a certain join algorithm for a specific join step.
     */
    @SuppressWarnings("unchecked")
    protected QueryPlan compileJoinQuery(StatementContext context, List<Object> binds, JoinTable joinTable, boolean asSubquery, boolean projectPKColumns, List<OrderByNode> orderBy) throws SQLException {
        byte[] emptyByteArray = new byte[0];
        List<JoinSpec> joinSpecs = joinTable.getJoinSpecs();
        if (joinSpecs.isEmpty()) {
            Table table = joinTable.getTable();
            SelectStatement subquery = table.getAsSubquery(orderBy);
            if (!table.isSubselect()) {
                ProjectedPTableWrapper projectedTable = table.createProjectedTable(!projectPKColumns);
                TupleProjector.serializeProjectorIntoScan(context.getScan(), projectedTable.createTupleProjector());
                context.setCurrentTable(table.getTableRef());
                context.setResolver(projectedTable.createColumnResolver());
                table.projectColumns(context.getScan());
                return compileSingleQuery(context, subquery, binds, asSubquery, !asSubquery);
            }
            QueryPlan plan = compileSubquery(subquery);
            ProjectedPTableWrapper projectedTable = table.createProjectedTable(plan.getProjector());
            context.setResolver(projectedTable.createColumnResolver());
            return new TupleProjectionPlan(plan, projectedTable.createTupleProjector(), table.compilePostFilterExpression(context));
        }

        boolean[] starJoinVector;
        if (!this.useSortMergeJoin && (starJoinVector = joinTable.getStarJoinVector()) != null) {
            Table table = joinTable.getTable();
            ProjectedPTableWrapper initialProjectedTable;
            TableRef tableRef;
            SelectStatement query;
            if (!table.isSubselect()) {
                initialProjectedTable = table.createProjectedTable(!projectPKColumns);
                tableRef = table.getTableRef();
                table.projectColumns(context.getScan());
                query = joinTable.getAsSingleSubquery(table.getAsSubquery(orderBy), asSubquery);
            } else {
                SelectStatement subquery = table.getAsSubquery(orderBy);
                QueryPlan plan = compileSubquery(subquery);
                initialProjectedTable = table.createProjectedTable(plan.getProjector());
                tableRef = plan.getTableRef();
                context.getScan().setFamilyMap(plan.getContext().getScan().getFamilyMap());
                query = joinTable.getAsSingleSubquery((SelectStatement) plan.getStatement(), asSubquery);
            }
            context.setCurrentTable(tableRef);
            PTableWrapper projectedTable = initialProjectedTable;
            int count = joinSpecs.size();
            ImmutableBytesPtr[] joinIds = new ImmutableBytesPtr[count];
            List<Expression>[] joinExpressions = new List[count];
            JoinType[] joinTypes = new JoinType[count];
            PTable[] tables = new PTable[count];
            int[] fieldPositions = new int[count];
            HashSubPlan[] subPlans = new HashSubPlan[count];
            fieldPositions[0] = projectedTable.getTable().getColumns().size() - projectedTable.getTable().getPKColumns().size();
            boolean forceProjection = table.isSubselect();
            boolean needsProject = forceProjection || asSubquery;
            for (int i = 0; i < count; i++) {
                JoinSpec joinSpec = joinSpecs.get(i);
                Scan subScan = ScanUtil.newScan(originalScan);
                StatementContext subContext = new StatementContext(statement, context.getResolver(), subScan, new SequenceManager(statement));
                QueryPlan joinPlan = compileJoinQuery(subContext, binds, joinSpec.getJoinTable(), true, true, null);
                boolean hasPostReference = joinSpec.getJoinTable().hasPostReference();
                if (hasPostReference) {
                    PTableWrapper subProjTable = ((JoinedTableColumnResolver) subContext.getResolver()).getPTableWrapper();
                    tables[i] = subProjTable.getTable();
                    projectedTable = projectedTable.mergeProjectedTables(subProjTable, joinSpec.getType());
                    needsProject = true;
                } else {
                    tables[i] = null;
                }
                if (!starJoinVector[i]) {
                    needsProject = true;
                }
                context.setResolver((!forceProjection && starJoinVector[i]) ? joinTable.getOriginalResolver() : projectedTable.createColumnResolver());
                joinIds[i] = new ImmutableBytesPtr(emptyByteArray); // place-holder
                Pair<List<Expression>, List<Expression>> joinConditions = joinSpec.compileJoinConditions(context, subContext, true);
                joinExpressions[i] = joinConditions.getFirst();
                List<Expression> hashExpressions = joinConditions.getSecond();
                Pair<Expression, Expression> keyRangeExpressions = new Pair<Expression, Expression>(null, null);
                boolean complete = getKeyExpressionCombinations(keyRangeExpressions, context, joinTable.getStatement(), tableRef, joinSpec.getType(), joinExpressions[i], hashExpressions);
                Expression keyRangeLhsExpression = keyRangeExpressions.getFirst();
                Expression keyRangeRhsExpression = keyRangeExpressions.getSecond();
                boolean hasFilters = joinSpec.getJoinTable().hasFilters();
                boolean optimized = complete && hasFilters;
                joinTypes[i] = joinSpec.getType();
                if (i < count - 1) {
                    fieldPositions[i + 1] = fieldPositions[i] + (tables[i] == null ? 0 : (tables[i].getColumns().size() - tables[i].getPKColumns().size()));
                }
                subPlans[i] = new HashSubPlan(i, joinPlan, optimized ? null : hashExpressions, joinSpec.isSingleValueOnly(), keyRangeLhsExpression, keyRangeRhsExpression, hasFilters);
            }
            if (needsProject) {
                TupleProjector.serializeProjectorIntoScan(context.getScan(), initialProjectedTable.createTupleProjector());
            }
            context.setResolver(needsProject ? projectedTable.createColumnResolver() : joinTable.getOriginalResolver());
            QueryPlan plan = compileSingleQuery(context, query, binds, asSubquery, !asSubquery && joinTable.isAllLeftJoin());
            Expression postJoinFilterExpression = joinTable.compilePostFilterExpression(context, table);
            Integer limit = null;
            if (query.getLimit() != null && !query.isAggregate() && !query.isDistinct() && query.getOrderBy().isEmpty()) {
                limit = LimitCompiler.compile(context, query);
            }
            HashJoinInfo joinInfo = new HashJoinInfo(projectedTable.getTable(), joinIds, joinExpressions, joinTypes, starJoinVector, tables, fieldPositions, postJoinFilterExpression, limit, forceProjection);
            return HashJoinPlan.create(joinTable.getStatement(), plan, joinInfo, subPlans);
        }

        JoinSpec lastJoinSpec = joinSpecs.get(joinSpecs.size() - 1);
        JoinType type = lastJoinSpec.getType();
        if (!this.useSortMergeJoin 
                && (type == JoinType.Right || type == JoinType.Inner) 
                && lastJoinSpec.getJoinTable().getJoinSpecs().isEmpty()
                && lastJoinSpec.getJoinTable().getTable().isFlat()) {
            JoinTable rhsJoinTable = lastJoinSpec.getJoinTable();
            Table rhsTable = rhsJoinTable.getTable();
            JoinTable lhsJoin = joinTable.getSubJoinTableWithoutPostFilters();
            Scan subScan = ScanUtil.newScan(originalScan);
            StatementContext lhsCtx = new StatementContext(statement, context.getResolver(), subScan, new SequenceManager(statement));
            QueryPlan lhsPlan = compileJoinQuery(lhsCtx, binds, lhsJoin, true, true, null);
            PTableWrapper lhsProjTable = ((JoinedTableColumnResolver) lhsCtx.getResolver()).getPTableWrapper();
            ProjectedPTableWrapper rhsProjTable;
            TableRef rhsTableRef;
            SelectStatement rhs;
            if (!rhsTable.isSubselect()) {
                rhsProjTable = rhsTable.createProjectedTable(!projectPKColumns);
                rhsTableRef = rhsTable.getTableRef();
                rhsTable.projectColumns(context.getScan());
                rhs = rhsJoinTable.getAsSingleSubquery(rhsTable.getAsSubquery(orderBy), asSubquery);
            } else {
                SelectStatement subquery = rhsTable.getAsSubquery(orderBy);
                QueryPlan plan = compileSubquery(subquery);
                rhsProjTable = rhsTable.createProjectedTable(plan.getProjector());
                rhsTableRef = plan.getTableRef();
                context.getScan().setFamilyMap(plan.getContext().getScan().getFamilyMap());
                rhs = rhsJoinTable.getAsSingleSubquery((SelectStatement) plan.getStatement(), asSubquery);
            }
            context.setCurrentTable(rhsTableRef);
            boolean forceProjection = rhsTable.isSubselect();
            context.setResolver(forceProjection ? rhsProjTable.createColumnResolver() : joinTable.getOriginalResolver());
            ImmutableBytesPtr[] joinIds = new ImmutableBytesPtr[] {new ImmutableBytesPtr(emptyByteArray)};
            Pair<List<Expression>, List<Expression>> joinConditions = lastJoinSpec.compileJoinConditions(lhsCtx, context, true);
            List<Expression> joinExpressions = joinConditions.getSecond();
            List<Expression> hashExpressions = joinConditions.getFirst();
            boolean needsMerge = lhsJoin.hasPostReference();
            boolean needsProject = forceProjection || asSubquery || needsMerge;
            PTable lhsTable = needsMerge ? lhsProjTable.getTable() : null;
            int fieldPosition = needsMerge ? rhsProjTable.getTable().getColumns().size() - rhsProjTable.getTable().getPKColumns().size() : 0;
            PTableWrapper projectedTable = needsMerge ? rhsProjTable.mergeProjectedTables(lhsProjTable, type == JoinType.Right ? JoinType.Left : type) : rhsProjTable;
            if (needsProject) {
                TupleProjector.serializeProjectorIntoScan(context.getScan(), rhsProjTable.createTupleProjector());
            }
            context.setResolver(needsProject ? projectedTable.createColumnResolver() : joinTable.getOriginalResolver());
            QueryPlan rhsPlan = compileSingleQuery(context, rhs, binds, asSubquery, !asSubquery && type == JoinType.Right);
            Expression postJoinFilterExpression = joinTable.compilePostFilterExpression(context, rhsTable);
            Integer limit = null;
            if (rhs.getLimit() != null && !rhs.isAggregate() && !rhs.isDistinct() && rhs.getOrderBy().isEmpty()) {
                limit = LimitCompiler.compile(context, rhs);
            }
            HashJoinInfo joinInfo = new HashJoinInfo(projectedTable.getTable(), joinIds, new List[] {joinExpressions}, new JoinType[] {type == JoinType.Right ? JoinType.Left : type}, new boolean[] {true}, new PTable[] {lhsTable}, new int[] {fieldPosition}, postJoinFilterExpression, limit, forceProjection);
            Pair<Expression, Expression> keyRangeExpressions = new Pair<Expression, Expression>(null, null);
            getKeyExpressionCombinations(keyRangeExpressions, context, joinTable.getStatement(), rhsTableRef, type, joinExpressions, hashExpressions);
            return HashJoinPlan.create(joinTable.getStatement(), rhsPlan, joinInfo, new HashSubPlan[] {new HashSubPlan(0, lhsPlan, hashExpressions, false, keyRangeExpressions.getFirst(), keyRangeExpressions.getSecond(), lhsJoin.hasFilters())});
        }

        JoinTable lhsJoin = joinTable.getSubJoinTableWithoutPostFilters();
        JoinTable rhsJoin = lastJoinSpec.getJoinTable();        
        if (type == JoinType.Right) {
            JoinTable temp = lhsJoin;
            lhsJoin = rhsJoin;
            rhsJoin = temp;
        }
        
        List<EqualParseNode> joinConditionNodes = lastJoinSpec.getOnConditions();
        List<OrderByNode> lhsOrderBy = Lists.<OrderByNode> newArrayListWithExpectedSize(joinConditionNodes.size());
        List<OrderByNode> rhsOrderBy = Lists.<OrderByNode> newArrayListWithExpectedSize(joinConditionNodes.size());
        for (EqualParseNode condition : joinConditionNodes) {
            lhsOrderBy.add(NODE_FACTORY.orderBy(type == JoinType.Right ? condition.getRHS() : condition.getLHS(), false, true));
            rhsOrderBy.add(NODE_FACTORY.orderBy(type == JoinType.Right ? condition.getLHS() : condition.getRHS(), false, true));
        }
        
        Scan lhsScan = ScanUtil.newScan(originalScan);
        StatementContext lhsCtx = new StatementContext(statement, context.getResolver(), lhsScan, new SequenceManager(statement));
        boolean preserveRowkey = !projectPKColumns && type != JoinType.Full;
        QueryPlan lhsPlan = compileJoinQuery(lhsCtx, binds, lhsJoin, true, !preserveRowkey, lhsOrderBy);
        PTableWrapper lhsProjTable = ((JoinedTableColumnResolver) lhsCtx.getResolver()).getPTableWrapper();
        boolean isInRowKeyOrder = preserveRowkey && lhsPlan.getOrderBy().getOrderByExpressions().isEmpty();
        
        Scan rhsScan = ScanUtil.newScan(originalScan);
        StatementContext rhsCtx = new StatementContext(statement, context.getResolver(), rhsScan, new SequenceManager(statement));
        QueryPlan rhsPlan = compileJoinQuery(rhsCtx, binds, rhsJoin, true, true, rhsOrderBy);
        PTableWrapper rhsProjTable = ((JoinedTableColumnResolver) rhsCtx.getResolver()).getPTableWrapper();
        
        Pair<List<Expression>, List<Expression>> joinConditions = lastJoinSpec.compileJoinConditions(type == JoinType.Right ? rhsCtx : lhsCtx, type == JoinType.Right ? lhsCtx : rhsCtx, false);
        List<Expression> lhsKeyExpressions = type == JoinType.Right ? joinConditions.getSecond() : joinConditions.getFirst();
        List<Expression> rhsKeyExpressions = type == JoinType.Right ? joinConditions.getFirst() : joinConditions.getSecond();
        
        boolean needsMerge = rhsJoin.hasPostReference();
        PTable rhsTable = needsMerge ? rhsProjTable.getTable() : null;
        int fieldPosition = needsMerge ? lhsProjTable.getTable().getColumns().size() - lhsProjTable.getTable().getPKColumns().size() : 0;
        PTableWrapper projectedTable = needsMerge ? lhsProjTable.mergeProjectedTables(rhsProjTable, type == JoinType.Right ? JoinType.Left : type) : lhsProjTable;

        ColumnResolver resolver = projectedTable.createColumnResolver();
        TableRef tableRef = ((JoinedTableColumnResolver) resolver).getTableRef();
        StatementContext subCtx = new StatementContext(statement, resolver, ScanUtil.newScan(originalScan), new SequenceManager(statement));
        subCtx.setCurrentTable(tableRef);
        QueryPlan innerPlan = new SortMergeJoinPlan(subCtx, joinTable.getStatement(), tableRef, type == JoinType.Right ? JoinType.Left : type, lhsPlan, rhsPlan, lhsKeyExpressions, rhsKeyExpressions, projectedTable.getTable(), lhsProjTable.getTable(), rhsTable, fieldPosition);
        context.setCurrentTable(tableRef);
        context.setResolver(resolver);
        TableNode from = NODE_FACTORY.namedTable(tableRef.getTableAlias(), NODE_FACTORY.table(tableRef.getTable().getSchemaName().getString(), tableRef.getTable().getTableName().getString()));
        ParseNode where = joinTable.getPostFiltersCombined();
        SelectStatement select = asSubquery ? NODE_FACTORY.select(from, joinTable.getStatement().getHint(), false, Collections.<AliasedNode> emptyList(), where, null, null, orderBy, null, 0, false, joinTable.getStatement().hasSequence())
                : NODE_FACTORY.select(joinTable.getStatement(), from, where);
        
        return compileSingleFlatQuery(context, select, binds, asSubquery, false, innerPlan, null, isInRowKeyOrder);
    }

    private boolean getKeyExpressionCombinations(Pair<Expression, Expression> combination, StatementContext context, SelectStatement select, TableRef table, JoinType type, final List<Expression> joinExpressions, final List<Expression> hashExpressions) throws SQLException {
        if (type != JoinType.Inner && type != JoinType.Semi)
            return false;

        Scan scanCopy = ScanUtil.newScan(context.getScan());
        StatementContext contextCopy = new StatementContext(statement, context.getResolver(), scanCopy, new SequenceManager(statement));
        contextCopy.setCurrentTable(table);
        List<Expression> lhsCombination = Lists.<Expression> newArrayList();
        boolean complete = WhereOptimizer.getKeyExpressionCombination(lhsCombination, contextCopy, select, joinExpressions);
        if (lhsCombination.isEmpty())
            return false;

        List<Expression> rhsCombination = Lists.newArrayListWithExpectedSize(lhsCombination.size());
        for (int i = 0; i < lhsCombination.size(); i++) {
            Expression lhs = lhsCombination.get(i);
            for (int j = 0; j < joinExpressions.size(); j++) {
                if (lhs == joinExpressions.get(j)) {
                    rhsCombination.add(hashExpressions.get(j));
                    break;
                }
            }
        }

        if (lhsCombination.size() == 1) {
            combination.setFirst(lhsCombination.get(0));
            combination.setSecond(rhsCombination.get(0));
        } else {
            combination.setFirst(new RowValueConstructorExpression(lhsCombination, false));
            combination.setSecond(new RowValueConstructorExpression(rhsCombination, false));
        }

        return type == JoinType.Semi && complete;
    }

    protected QueryPlan compileSubquery(SelectStatement subquery) throws SQLException {
        subquery = SubselectRewriter.flatten(subquery, this.statement.getConnection());
        ColumnResolver resolver = FromCompiler.getResolverForQuery(subquery, this.statement.getConnection());
        subquery = StatementNormalizer.normalize(subquery, resolver);
        SelectStatement transformedSubquery = SubqueryRewriter.transform(subquery, resolver, this.statement.getConnection());
        if (transformedSubquery != subquery) {
            resolver = FromCompiler.getResolverForQuery(transformedSubquery, this.statement.getConnection());
            subquery = StatementNormalizer.normalize(transformedSubquery, resolver);
        }
        QueryPlan plan = new QueryCompiler(this.statement, subquery, resolver).compile();
        return statement.getConnection().getQueryServices().getOptimizer().optimize(statement, plan);
    }

    protected QueryPlan compileSingleQuery(StatementContext context, SelectStatement select, List<Object> binds, boolean asSubquery, boolean allowPageFilter) throws SQLException{
        SelectStatement innerSelect = select.getInnerSelectStatement();
        if (innerSelect == null) {
            return compileSingleFlatQuery(context, select, binds, asSubquery, allowPageFilter, null, null, true);
        }

        QueryPlan innerPlan = compileSubquery(innerSelect);
        TupleProjector tupleProjector = new TupleProjector(innerPlan.getProjector());
        innerPlan = new TupleProjectionPlan(innerPlan, tupleProjector, null);

        // Replace the original resolver and table with those having compiled type info.
        TableRef tableRef = context.getResolver().getTables().get(0);
        ColumnResolver resolver = FromCompiler.getResolverForCompiledDerivedTable(statement.getConnection(), tableRef, innerPlan.getProjector());
        context.setResolver(resolver);
        tableRef = resolver.getTables().get(0);
        context.setCurrentTable(tableRef);

        return compileSingleFlatQuery(context, select, binds, asSubquery, allowPageFilter, innerPlan, tupleProjector, innerPlan.getOrderBy().getOrderByExpressions().isEmpty());
    }

    protected QueryPlan compileSingleFlatQuery(StatementContext context, SelectStatement select, List<Object> binds, boolean asSubquery, boolean allowPageFilter, QueryPlan innerPlan, TupleProjector innerPlanTupleProjector, boolean isInRowKeyOrder) throws SQLException{
        PhoenixConnection connection = statement.getConnection();
        ColumnResolver resolver = context.getResolver();
        TableRef tableRef = context.getCurrentTable();
        PTable table = tableRef.getTable();

        ParseNode viewWhere = null;
        if (table.getViewStatement() != null) {
            viewWhere = new SQLParser(table.getViewStatement()).parseQuery().getWhere();
        }
        Integer limit = LimitCompiler.compile(context, select);

        GroupBy groupBy = GroupByCompiler.compile(context, select, innerPlanTupleProjector, isInRowKeyOrder);
        // Optimize the HAVING clause by finding any group by expressions that can be moved
        // to the WHERE clause
        select = HavingCompiler.rewrite(context, select, groupBy);
        Expression having = HavingCompiler.compile(context, select, groupBy);
        // Don't pass groupBy when building where clause expression, because we do not want to wrap these
        // expressions as group by key expressions since they're pre, not post filtered.
        if (innerPlan == null) {
            context.setResolver(FromCompiler.getResolverForQuery(select, connection));
        }
        Set<SubqueryParseNode> subqueries = Sets.<SubqueryParseNode> newHashSet();
        Expression where = WhereCompiler.compile(context, select, viewWhere, subqueries);
        context.setResolver(resolver); // recover resolver
        OrderBy orderBy = OrderByCompiler.compile(context, select, groupBy, limit, isInRowKeyOrder); 
        RowProjector projector = ProjectionCompiler.compile(context, select, groupBy, asSubquery ? Collections.<PDatum>emptyList() : targetColumns);

        // Final step is to build the query plan
        int maxRows = statement.getMaxRows();
        if (maxRows > 0) {
            if (limit != null) {
                limit = Math.min(limit, maxRows);
            } else {
                limit = maxRows;
            }
        }

        QueryPlan plan = innerPlan;
        if (plan == null) {
            ParallelIteratorFactory parallelIteratorFactory = asSubquery ? null : this.parallelIteratorFactory;
            plan = select.isAggregate() || select.isDistinct() ?
                      new AggregatePlan(context, select, tableRef, projector, limit, orderBy, parallelIteratorFactory, groupBy, having)
                    : new ScanPlan(context, select, tableRef, projector, limit, orderBy, parallelIteratorFactory, allowPageFilter);
        }
        if (!subqueries.isEmpty()) {
            int count = subqueries.size();
            WhereClauseSubPlan[] subPlans = new WhereClauseSubPlan[count];
            int i = 0;
            for (SubqueryParseNode subqueryNode : subqueries) {
                SelectStatement stmt = subqueryNode.getSelectNode();
                subPlans[i++] = new WhereClauseSubPlan(compileSubquery(stmt), stmt, subqueryNode.expectSingleRow());
            }
            plan = HashJoinPlan.create(select, plan, null, subPlans);
        }

        if (innerPlan != null) {
            if (LiteralExpression.isTrue(where)) {
                where = null; // we do not pass "true" as filter
            }
            plan =  select.isAggregate() || select.isDistinct() ?
                      new ClientAggregatePlan(context, select, tableRef, projector, limit, where, orderBy, groupBy, having, plan)
                    : new ClientScanPlan(context, select, tableRef, projector, limit, where, orderBy, plan);

        }

        return plan;
    }
}


