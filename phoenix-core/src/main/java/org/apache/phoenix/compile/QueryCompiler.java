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

import static org.apache.phoenix.query.QueryServices.WILDCARD_QUERY_DYNAMIC_COLS_ATTRIB;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_WILDCARD_QUERY_DYNAMIC_COLS_ATTRIB;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.phoenix.thirdparty.com.google.common.base.Optional;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compat.hbase.HbaseCompatCapabilities;
import org.apache.phoenix.compat.hbase.coprocessor.CompatBaseScannerRegionObserver;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.JoinCompiler.JoinSpec;
import org.apache.phoenix.compile.JoinCompiler.JoinTable;
import org.apache.phoenix.compile.JoinCompiler.Table;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.AggregatePlan;
import org.apache.phoenix.execute.BaseQueryPlan;
import org.apache.phoenix.execute.ClientAggregatePlan;
import org.apache.phoenix.execute.ClientScanPlan;
import org.apache.phoenix.execute.HashJoinPlan;
import org.apache.phoenix.execute.HashJoinPlan.HashSubPlan;
import org.apache.phoenix.execute.HashJoinPlan.WhereClauseSubPlan;
import org.apache.phoenix.execute.LiteralResultIterationPlan;
import org.apache.phoenix.execute.ScanPlan;
import org.apache.phoenix.execute.SortMergeJoinPlan;
import org.apache.phoenix.execute.TupleProjectionPlan;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.execute.UnionPlan;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.RowValueConstructorExpression;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.iterate.ParallelIteratorFactory;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.join.HashJoinInfo;
import org.apache.phoenix.optimize.Cost;
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
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.RowValueConstructorOffsetNotCoercibleException;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ParseNodeUtil;
import org.apache.phoenix.util.ParseNodeUtil.RewriteResult;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ScanUtil;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Sets;


/**
 *
 * Class used to build an executable query plan
 *
 *
 * @since 0.1
 */
public class QueryCompiler {
    private static final ParseNodeFactory NODE_FACTORY = new ParseNodeFactory();
    private final PhoenixStatement statement;
    private final Scan scan;
    private final Scan originalScan;
    private final ColumnResolver resolver;
    private final BindManager bindManager;
    private final SelectStatement select;
    private final List<? extends PDatum> targetColumns;
    private final ParallelIteratorFactory parallelIteratorFactory;
    private final SequenceManager sequenceManager;
    private final boolean projectTuples;
    private final boolean noChildParentJoinOptimization;
    private final boolean usePersistentCache;
    private final boolean optimizeSubquery;
    private final Map<TableRef, QueryPlan> dataPlans;
    private final boolean costBased;

    public QueryCompiler(PhoenixStatement statement, SelectStatement select, ColumnResolver resolver, boolean projectTuples, boolean optimizeSubquery, Map<TableRef, QueryPlan> dataPlans) throws SQLException {
        this(statement, select, resolver, Collections.<PDatum>emptyList(), null, new SequenceManager(statement), projectTuples, optimizeSubquery, dataPlans);
    }

    public QueryCompiler(PhoenixStatement statement, SelectStatement select, ColumnResolver resolver, BindManager bindManager, boolean projectTuples, boolean optimizeSubquery, Map<TableRef, QueryPlan> dataPlans) throws SQLException {
        this(statement, select, resolver, bindManager, Collections.<PDatum>emptyList(), null, new SequenceManager(statement), projectTuples, optimizeSubquery, dataPlans);
    }

    public QueryCompiler(PhoenixStatement statement, SelectStatement select, ColumnResolver resolver, List<? extends PDatum> targetColumns, ParallelIteratorFactory parallelIteratorFactory, SequenceManager sequenceManager, boolean projectTuples, boolean optimizeSubquery, Map<TableRef, QueryPlan> dataPlans) throws SQLException {
        this(statement, select, resolver, new BindManager(statement.getParameters()), targetColumns, parallelIteratorFactory, sequenceManager, projectTuples, optimizeSubquery, dataPlans);
    }

    public QueryCompiler(PhoenixStatement statement, SelectStatement select, ColumnResolver resolver, BindManager bindManager, List<? extends PDatum> targetColumns, ParallelIteratorFactory parallelIteratorFactory, SequenceManager sequenceManager, boolean projectTuples, boolean optimizeSubquery, Map<TableRef, QueryPlan> dataPlans) throws SQLException {
        this.statement = statement;
        this.select = select;
        this.resolver = resolver;
        this.bindManager = bindManager;
        this.scan = new Scan();
        this.targetColumns = targetColumns;
        this.parallelIteratorFactory = parallelIteratorFactory;
        this.sequenceManager = sequenceManager;
        this.projectTuples = projectTuples;
        this.noChildParentJoinOptimization = select.getHint().hasHint(Hint.NO_CHILD_PARENT_JOIN_OPTIMIZATION) || select.getHint().hasHint(Hint.USE_PERSISTENT_CACHE);
        this.usePersistentCache = select.getHint().hasHint(Hint.USE_PERSISTENT_CACHE);
        ConnectionQueryServices services = statement.getConnection().getQueryServices();
        this.costBased = services.getProps().getBoolean(QueryServices.COST_BASED_OPTIMIZER_ENABLED, QueryServicesOptions.DEFAULT_COST_BASED_OPTIMIZER_ENABLED);
        scan.setLoadColumnFamiliesOnDemand(true);
        if (select.getHint().hasHint(Hint.NO_CACHE)) {
            scan.setCacheBlocks(false);
        }

        scan.setCaching(statement.getFetchSize());
        this.originalScan = ScanUtil.newScan(scan);
        this.optimizeSubquery = optimizeSubquery;
        this.dataPlans = dataPlans == null ? Collections.<TableRef, QueryPlan>emptyMap() : dataPlans;
    }

    public QueryCompiler(PhoenixStatement statement, SelectStatement select, ColumnResolver resolver, List<? extends PDatum> targetColumns, ParallelIteratorFactory parallelIteratorFactory, SequenceManager sequenceManager) throws SQLException {
        this(statement, select, resolver, targetColumns, parallelIteratorFactory, sequenceManager, true, false, null);
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
        verifySCN();
        QueryPlan plan;
        if (select.isUnion()) {
            plan = compileUnionAll(select);
        } else {
            plan = compileSelect(select);
        }
        return plan;
    }

    private void verifySCN() throws SQLException {
        if (!HbaseCompatCapabilities.isMaxLookbackTimeSupported()) {
            return;
        }
        PhoenixConnection conn = statement.getConnection();
        if (conn.isRunningUpgrade()) {
            // PHOENIX-6179 : if upgrade is going on, we don't need to
            // perform MaxLookBackAge check
            return;
        }
        Long scn = conn.getSCN();
        if (scn == null) {
            return;
        }
        long maxLookBackAgeInMillis =
            CompatBaseScannerRegionObserver.getMaxLookbackInMillis(conn.getQueryServices().
            getConfiguration());
        long now = EnvironmentEdgeManager.currentTimeMillis();
        if (maxLookBackAgeInMillis > 0 && now - maxLookBackAgeInMillis > scn){
            throw new SQLExceptionInfo.Builder(
                SQLExceptionCode.CANNOT_QUERY_TABLE_WITH_SCN_OLDER_THAN_MAX_LOOKBACK_AGE)
                .build().buildException();
        }
    }

    public QueryPlan compileUnionAll(SelectStatement select) throws SQLException { 
        List<SelectStatement> unionAllSelects = select.getSelects();
        List<QueryPlan> plans = new ArrayList<QueryPlan>();

        for (int i=0; i < unionAllSelects.size(); i++ ) {
            SelectStatement subSelect = unionAllSelects.get(i);
            // Push down order-by and limit into sub-selects.
            if (!select.getOrderBy().isEmpty() || select.getLimit() != null) {
                if (select.getOffset() == null) {
                    subSelect = NODE_FACTORY.select(subSelect, select.getOrderBy(), select.getLimit(), null);
                } else {
                    subSelect = NODE_FACTORY.select(subSelect, select.getOrderBy(), null, null);
                }
            }
            QueryPlan subPlan = compileSubquery(subSelect, true);
            plans.add(subPlan);
        }
        TableRef tableRef = UnionCompiler.contructSchemaTable(statement, plans,
            select.hasWildcard() ? null : select.getSelect());
        ColumnResolver resolver = FromCompiler.getResolver(tableRef);
        StatementContext context = new StatementContext(statement, resolver, bindManager, scan, sequenceManager);
        QueryPlan plan = compileSingleFlatQuery(
                context,
                select,
                statement.getParameters(),
                false,
                false,
                null,
                false,
                true);
        plan = new UnionPlan(context, select, tableRef, plan.getProjector(), plan.getLimit(),
            plan.getOffset(), plan.getOrderBy(), GroupBy.EMPTY_GROUP_BY, plans,
            context.getBindManager().getParameterMetaData());
        return plan;
    }

    public QueryPlan compileSelect(SelectStatement select) throws SQLException{
        List<Object> binds = statement.getParameters();
        StatementContext context = new StatementContext(statement, resolver, bindManager, scan, sequenceManager);
        if (select.isJoin()) {
            JoinTable joinTable = JoinCompiler.compile(statement, select, context.getResolver());
            return compileJoinQuery(context, binds, joinTable, false, false, null);
        } else {
            return compileSingleQuery(context, select, binds, false, true);
        }
    }

    /**
     * Call compileJoinQuery() for join queries recursively down to the leaf JoinTable nodes.
     * If it is a leaf node, call compileSingleFlatQuery() or compileSubquery(), otherwise:
     *      1) If option COST_BASED_OPTIMIZER_ENABLED is on and stats are available, return the
     *         join plan with the best cost. Note that the "best" plan is only locally optimal,
     *         and might or might not be globally optimal.
     *      2) Otherwise, return the join plan compiled with the default strategy.
     * @see JoinCompiler.JoinTable#getApplicableJoinStrategies()
     */
    protected QueryPlan compileJoinQuery(StatementContext context, List<Object> binds, JoinTable joinTable, boolean asSubquery, boolean projectPKColumns, List<OrderByNode> orderBy) throws SQLException {
        if (joinTable.getJoinSpecs().isEmpty()) {
            Table table = joinTable.getLeftTable();
            SelectStatement subquery = table.getAsSubquery(orderBy);
            if (!table.isSubselect()) {
                context.setCurrentTable(table.getTableRef());
                PTable projectedTable = table.createProjectedTable(!projectPKColumns, context);
                TupleProjector projector = new TupleProjector(projectedTable);
                boolean wildcardIncludesDynamicCols = context.getConnection().getQueryServices()
                        .getConfiguration().getBoolean(WILDCARD_QUERY_DYNAMIC_COLS_ATTRIB,
                                DEFAULT_WILDCARD_QUERY_DYNAMIC_COLS_ATTRIB);
                TupleProjector.serializeProjectorIntoScan(context.getScan(), projector,
                        wildcardIncludesDynamicCols);
                context.setResolver(FromCompiler.getResolverForProjectedTable(projectedTable, context.getConnection(), subquery.getUdfParseNodes()));
                table.projectColumns(context.getScan());
                return compileSingleFlatQuery(
                        context,
                        subquery,
                        binds,
                        asSubquery,
                        !asSubquery,
                        null,
                        true,
                        false);
            }
            QueryPlan plan = compileSubquery(subquery, false);
            PTable projectedTable = table.createProjectedTable(plan.getProjector());
            context.setResolver(FromCompiler.getResolverForProjectedTable(projectedTable, context.getConnection(), subquery.getUdfParseNodes()));
            return new TupleProjectionPlan(
                    plan,
                    new TupleProjector(plan.getProjector()),
                    context,
                    null);
        }

        List<JoinCompiler.Strategy> strategies = joinTable.getApplicableJoinStrategies();
        assert strategies.size() > 0;
        if (!costBased || strategies.size() == 1) {
            return compileJoinQuery(
                    strategies.get(0), context, binds, joinTable, asSubquery, projectPKColumns, orderBy);
        }

        QueryPlan bestPlan = null;
        Cost bestCost = null;
        for (JoinCompiler.Strategy strategy : strategies) {
            StatementContext newContext = new StatementContext(
                    context.getStatement(), context.getResolver(), context.getBindManager(), new Scan(), context.getSequenceManager());
            QueryPlan plan = compileJoinQuery(
                    strategy, newContext, binds, joinTable, asSubquery, projectPKColumns, orderBy);
            Cost cost = plan.getCost();
            if (bestPlan == null || cost.compareTo(bestCost) < 0) {
                bestPlan = plan;
                bestCost = cost;
            }
        }
        context.setResolver(bestPlan.getContext().getResolver());
        context.setCurrentTable(bestPlan.getContext().getCurrentTable());
        return bestPlan;
    }

    protected QueryPlan compileJoinQuery(JoinCompiler.Strategy strategy, StatementContext context, List<Object> binds, JoinTable joinTable, boolean asSubquery, boolean projectPKColumns, List<OrderByNode> orderBy) throws SQLException {
        byte[] emptyByteArray = new byte[0];
        List<JoinSpec> joinSpecs = joinTable.getJoinSpecs();
        boolean wildcardIncludesDynamicCols = context.getConnection().getQueryServices()
                .getConfiguration().getBoolean(WILDCARD_QUERY_DYNAMIC_COLS_ATTRIB,
                        DEFAULT_WILDCARD_QUERY_DYNAMIC_COLS_ATTRIB);
        switch (strategy) {
            case HASH_BUILD_RIGHT: {
                boolean[] starJoinVector = joinTable.getStarJoinVector();
                Table table = joinTable.getLeftTable();
                PTable initialProjectedTable;
                TableRef tableRef;
                SelectStatement query;
                TupleProjector tupleProjector;
                if (!table.isSubselect()) {
                    context.setCurrentTable(table.getTableRef());
                    initialProjectedTable = table.createProjectedTable(!projectPKColumns, context);
                    tableRef = table.getTableRef();
                    table.projectColumns(context.getScan());
                    query = joinTable.getAsSingleSubquery(table.getAsSubquery(orderBy), asSubquery);
                    tupleProjector = new TupleProjector(initialProjectedTable);
                } else {
                    SelectStatement subquery = table.getAsSubquery(orderBy);
                    QueryPlan plan = compileSubquery(subquery, false);
                    initialProjectedTable = table.createProjectedTable(plan.getProjector());
                    tableRef = plan.getTableRef();
                    context.getScan().setFamilyMap(plan.getContext().getScan().getFamilyMap());
                    query = joinTable.getAsSingleSubquery((SelectStatement) plan.getStatement(), asSubquery);
                    tupleProjector = new TupleProjector(plan.getProjector());
                }
                context.setCurrentTable(tableRef);
                PTable projectedTable = initialProjectedTable;
                int count = joinSpecs.size();
                ImmutableBytesPtr[] joinIds = new ImmutableBytesPtr[count];
                List<Expression>[] joinExpressions = new List[count];
                JoinType[] joinTypes = new JoinType[count];
                PTable[] tables = new PTable[count];
                int[] fieldPositions = new int[count];
                StatementContext[] subContexts = new StatementContext[count];
                QueryPlan[] subPlans = new QueryPlan[count];
                HashSubPlan[] hashPlans = new HashSubPlan[count];
                fieldPositions[0] = projectedTable.getColumns().size() - projectedTable.getPKColumns().size();
                for (int i = 0; i < count; i++) {
                    JoinSpec joinSpec = joinSpecs.get(i);
                    Scan subScan = ScanUtil.newScan(originalScan);
                    subContexts[i] = new StatementContext(statement, context.getResolver(), context.getBindManager(), subScan, new SequenceManager(statement));
                    subPlans[i] = compileJoinQuery(
                            subContexts[i],
                            binds,
                            joinSpec.getRhsJoinTable(),
                            true,
                            true,
                            null);
                    boolean hasPostReference = joinSpec.getRhsJoinTable().hasPostReference();
                    if (hasPostReference) {
                        tables[i] = subContexts[i].getResolver().getTables().get(0).getTable();
                        projectedTable = JoinCompiler.joinProjectedTables(projectedTable, tables[i], joinSpec.getType());
                    } else {
                        tables[i] = null;
                    }
                }
                for (int i = 0; i < count; i++) {
                    JoinSpec joinSpec = joinSpecs.get(i);
                    context.setResolver(FromCompiler.getResolverForProjectedTable(projectedTable, context.getConnection(), query.getUdfParseNodes()));
                    joinIds[i] = new ImmutableBytesPtr(emptyByteArray); // place-holder
                    Pair<List<Expression>, List<Expression>> joinConditions = joinSpec.compileJoinConditions(context, subContexts[i], strategy);
                    joinExpressions[i] = joinConditions.getFirst();
                    List<Expression> hashExpressions = joinConditions.getSecond();
                    Pair<Expression, Expression> keyRangeExpressions = new Pair<Expression, Expression>(null, null);
                    boolean optimized = getKeyExpressionCombinations(
                            keyRangeExpressions,
                            context,
                            joinTable.getOriginalJoinSelectStatement(),
                            tableRef,
                            joinSpec.getType(),
                            joinExpressions[i],
                            hashExpressions);
                    Expression keyRangeLhsExpression = keyRangeExpressions.getFirst();
                    Expression keyRangeRhsExpression = keyRangeExpressions.getSecond();
                    joinTypes[i] = joinSpec.getType();
                    if (i < count - 1) {
                        fieldPositions[i + 1] = fieldPositions[i] + (tables[i] == null ? 0 : (tables[i].getColumns().size() - tables[i].getPKColumns().size()));
                    }
                    hashPlans[i] = new HashSubPlan(i, subPlans[i], optimized ? null : hashExpressions, joinSpec.isSingleValueOnly(), usePersistentCache, keyRangeLhsExpression, keyRangeRhsExpression);
                }
                TupleProjector.serializeProjectorIntoScan(context.getScan(), tupleProjector,
                        wildcardIncludesDynamicCols);
                QueryPlan plan = compileSingleFlatQuery(
                        context,
                        query,
                        binds,
                        asSubquery,
                        !asSubquery && joinTable.isAllLeftJoin(),
                        null, true, false);
                Expression postJoinFilterExpression = joinTable.compilePostFilterExpression(context);
                Integer limit = null;
                Integer offset = null;
                if (!query.isAggregate() && !query.isDistinct() && query.getOrderBy().isEmpty()) {
                    limit = plan.getLimit();
                    offset = plan.getOffset();
                }
                HashJoinInfo joinInfo = new HashJoinInfo(projectedTable, joinIds, joinExpressions, joinTypes,
                        starJoinVector, tables, fieldPositions, postJoinFilterExpression, QueryUtil.getOffsetLimit(limit, offset));
                return HashJoinPlan.create(joinTable.getOriginalJoinSelectStatement(), plan, joinInfo, hashPlans);
            }
            case HASH_BUILD_LEFT: {
                JoinSpec lastJoinSpec = joinSpecs.get(joinSpecs.size() - 1);
                JoinType type = lastJoinSpec.getType();
                JoinTable rhsJoinTable = lastJoinSpec.getRhsJoinTable();
                Table rhsTable = rhsJoinTable.getLeftTable();
                JoinTable lhsJoin = joinTable.createSubJoinTable(statement.getConnection());
                Scan subScan = ScanUtil.newScan(originalScan);
                StatementContext lhsCtx = new StatementContext(statement, context.getResolver(), context.getBindManager(), subScan, new SequenceManager(statement));
                QueryPlan lhsPlan = compileJoinQuery(lhsCtx, binds, lhsJoin, true, true, null);
                PTable rhsProjTable;
                TableRef rhsTableRef;
                SelectStatement rhs;
                TupleProjector tupleProjector;
                if (!rhsTable.isSubselect()) {
                    context.setCurrentTable(rhsTable.getTableRef());
                    rhsProjTable = rhsTable.createProjectedTable(!projectPKColumns, context);
                    rhsTableRef = rhsTable.getTableRef();
                    rhsTable.projectColumns(context.getScan());
                    rhs = rhsJoinTable.getAsSingleSubquery(rhsTable.getAsSubquery(orderBy), asSubquery);
                    tupleProjector = new TupleProjector(rhsProjTable);
                } else {
                    SelectStatement subquery = rhsTable.getAsSubquery(orderBy);
                    QueryPlan plan = compileSubquery(subquery, false);
                    rhsProjTable = rhsTable.createProjectedTable(plan.getProjector());
                    rhsTableRef = plan.getTableRef();
                    context.getScan().setFamilyMap(plan.getContext().getScan().getFamilyMap());
                    rhs = rhsJoinTable.getAsSingleSubquery((SelectStatement) plan.getStatement(), asSubquery);
                    tupleProjector = new TupleProjector(plan.getProjector());
                }
                context.setCurrentTable(rhsTableRef);
                context.setResolver(FromCompiler.getResolverForProjectedTable(rhsProjTable, context.getConnection(), rhs.getUdfParseNodes()));
                ImmutableBytesPtr[] joinIds = new ImmutableBytesPtr[]{new ImmutableBytesPtr(emptyByteArray)};
                Pair<List<Expression>, List<Expression>> joinConditions = lastJoinSpec.compileJoinConditions(lhsCtx, context, strategy);
                List<Expression> joinExpressions = joinConditions.getSecond();
                List<Expression> hashExpressions = joinConditions.getFirst();
                boolean needsMerge = lhsJoin.hasPostReference();
                PTable lhsTable = needsMerge ? lhsCtx.getResolver().getTables().get(0).getTable() : null;
                int fieldPosition = needsMerge ? rhsProjTable.getColumns().size() - rhsProjTable.getPKColumns().size() : 0;
                PTable projectedTable = needsMerge ? JoinCompiler.joinProjectedTables(rhsProjTable, lhsTable, type == JoinType.Right ? JoinType.Left : type) : rhsProjTable;
                TupleProjector.serializeProjectorIntoScan(context.getScan(), tupleProjector,
                        wildcardIncludesDynamicCols);
                context.setResolver(FromCompiler.getResolverForProjectedTable(projectedTable, context.getConnection(), rhs.getUdfParseNodes()));
                QueryPlan rhsPlan = compileSingleFlatQuery(
                        context,
                        rhs,
                        binds,
                        asSubquery,
                        !asSubquery && type == JoinType.Right,
                        null,
                        true,
                        false);
                Expression postJoinFilterExpression = joinTable.compilePostFilterExpression(context);
                Integer limit = null;
                Integer offset = null;
                if (!rhs.isAggregate() && !rhs.isDistinct() && rhs.getOrderBy().isEmpty()) {
                    limit = rhsPlan.getLimit();
                    offset = rhsPlan.getOffset();
                }
                HashJoinInfo joinInfo = new HashJoinInfo(projectedTable, joinIds, new List[]{joinExpressions},
                        new JoinType[]{type == JoinType.Right ? JoinType.Left : type}, new boolean[]{true},
                        new PTable[]{lhsTable}, new int[]{fieldPosition}, postJoinFilterExpression, QueryUtil.getOffsetLimit(limit, offset));
                boolean usePersistentCache = joinTable.getOriginalJoinSelectStatement().getHint().hasHint(Hint.USE_PERSISTENT_CACHE);
                Pair<Expression, Expression> keyRangeExpressions = new Pair<Expression, Expression>(null, null);
                getKeyExpressionCombinations(
                        keyRangeExpressions,
                        context,
                        joinTable.getOriginalJoinSelectStatement(),
                        rhsTableRef,
                        type,
                        joinExpressions,
                        hashExpressions);
                return HashJoinPlan.create(
                        joinTable.getOriginalJoinSelectStatement(),
                        rhsPlan,
                        joinInfo,
                        new HashSubPlan[]{
                                new HashSubPlan(
                                        0,
                                        lhsPlan,
                                        hashExpressions,
                                        false,
                                        usePersistentCache,
                                        keyRangeExpressions.getFirst(),
                                        keyRangeExpressions.getSecond())});
            }
            case SORT_MERGE: {
                JoinTable lhsJoin =  joinTable.createSubJoinTable(statement.getConnection());
                JoinSpec lastJoinSpec = joinSpecs.get(joinSpecs.size() - 1);
                JoinType type = lastJoinSpec.getType();
                JoinTable rhsJoin = lastJoinSpec.getRhsJoinTable();
                if (type == JoinType.Right) {
                    JoinTable temp = lhsJoin;
                    lhsJoin = rhsJoin;
                    rhsJoin = temp;
                }

                List<EqualParseNode> joinConditionNodes = lastJoinSpec.getOnConditions();
                List<OrderByNode> lhsOrderBy = Lists.<OrderByNode>newArrayListWithExpectedSize(joinConditionNodes.size());
                List<OrderByNode> rhsOrderBy = Lists.<OrderByNode>newArrayListWithExpectedSize(joinConditionNodes.size());
                for (EqualParseNode condition : joinConditionNodes) {
                    lhsOrderBy.add(NODE_FACTORY.orderBy(type == JoinType.Right ? condition.getRHS() : condition.getLHS(), false, true));
                    rhsOrderBy.add(NODE_FACTORY.orderBy(type == JoinType.Right ? condition.getLHS() : condition.getRHS(), false, true));
                }

                Scan lhsScan = ScanUtil.newScan(originalScan);
                StatementContext lhsCtx = new StatementContext(statement, context.getResolver(), context.getBindManager(), lhsScan, new SequenceManager(statement));
                boolean preserveRowkey = !projectPKColumns && type != JoinType.Full;
                QueryPlan lhsPlan = compileJoinQuery(lhsCtx, binds, lhsJoin, true, !preserveRowkey, lhsOrderBy);
                PTable lhsProjTable = lhsCtx.getResolver().getTables().get(0).getTable();

                Scan rhsScan = ScanUtil.newScan(originalScan);
                StatementContext rhsCtx = new StatementContext(statement, context.getResolver(), context.getBindManager(), rhsScan, new SequenceManager(statement));
                QueryPlan rhsPlan = compileJoinQuery(rhsCtx, binds, rhsJoin, true, true, rhsOrderBy);
                PTable rhsProjTable = rhsCtx.getResolver().getTables().get(0).getTable();

                Pair<List<Expression>, List<Expression>> joinConditions = lastJoinSpec.compileJoinConditions(type == JoinType.Right ? rhsCtx : lhsCtx, type == JoinType.Right ? lhsCtx : rhsCtx, strategy);
                List<Expression> lhsKeyExpressions = type == JoinType.Right ? joinConditions.getSecond() : joinConditions.getFirst();
                List<Expression> rhsKeyExpressions = type == JoinType.Right ? joinConditions.getFirst() : joinConditions.getSecond();

                boolean needsMerge = rhsJoin.hasPostReference();
                int fieldPosition = needsMerge ? lhsProjTable.getColumns().size() - lhsProjTable.getPKColumns().size() : 0;
                PTable projectedTable = needsMerge ? JoinCompiler.joinProjectedTables(lhsProjTable, rhsProjTable, type == JoinType.Right ? JoinType.Left : type) : lhsProjTable;

                ColumnResolver resolver = FromCompiler.getResolverForProjectedTable(projectedTable, context.getConnection(), joinTable.getOriginalJoinSelectStatement().getUdfParseNodes());
                TableRef tableRef = resolver.getTables().get(0);
                StatementContext subCtx = new StatementContext(statement, resolver, context.getBindManager(), ScanUtil.newScan(originalScan), new SequenceManager(statement));
                subCtx.setCurrentTable(tableRef);
                QueryPlan innerPlan = new SortMergeJoinPlan(
                        subCtx,
                        joinTable.getOriginalJoinSelectStatement(),
                        tableRef,
                        type == JoinType.Right ? JoinType.Left : type,
                        lhsPlan,
                        rhsPlan,
                        new Pair<List<Expression>,List<Expression>>(lhsKeyExpressions, rhsKeyExpressions),
                        rhsKeyExpressions,
                        projectedTable,
                        lhsProjTable,
                        needsMerge ? rhsProjTable : null,
                        fieldPosition,
                        lastJoinSpec.isSingleValueOnly(),
                        new Pair<List<OrderByNode>,List<OrderByNode>>(lhsOrderBy, rhsOrderBy));
                context.setCurrentTable(tableRef);
                context.setResolver(resolver);
                TableNode from = NODE_FACTORY.namedTable(tableRef.getTableAlias(), NODE_FACTORY.table(tableRef.getTable().getSchemaName().getString(), tableRef.getTable().getTableName().getString()));
                ParseNode where = joinTable.getPostFiltersCombined();
                SelectStatement select = asSubquery ?
                        NODE_FACTORY.select(
                                from,
                                joinTable.getOriginalJoinSelectStatement().getHint(),
                                false,
                                Collections.<AliasedNode>emptyList(),
                                where,
                                null,
                                null,
                                orderBy,
                                null,
                                null,
                                0,
                                false,
                                joinTable.getOriginalJoinSelectStatement().hasSequence(),
                                Collections.<SelectStatement>emptyList(),
                                joinTable.getOriginalJoinSelectStatement().getUdfParseNodes()) :
                         NODE_FACTORY.select(
                                 joinTable.getOriginalJoinSelectStatement(),
                                 from,
                                 where);

                return compileSingleFlatQuery(
                        context,
                        select,
                        binds,
                        asSubquery,
                        false,
                        innerPlan,
                        true,
                        false);
            }
            default:
                throw new IllegalArgumentException("Invalid join strategy '" + strategy + "'");
        }
    }

    private boolean getKeyExpressionCombinations(Pair<Expression, Expression> combination, StatementContext context, SelectStatement select, TableRef table, JoinType type, final List<Expression> joinExpressions, final List<Expression> hashExpressions) throws SQLException {
        if ((type != JoinType.Inner && type != JoinType.Semi) || this.noChildParentJoinOptimization)
            return false;

        Scan scanCopy = ScanUtil.newScan(context.getScan());
        StatementContext contextCopy = new StatementContext(statement, context.getResolver(), context.getBindManager(), scanCopy, new SequenceManager(statement));
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

    protected QueryPlan compileSubquery(
            SelectStatement subquerySelectStatement,
            boolean pushDownMaxRows) throws SQLException {
        PhoenixConnection phoenixConnection = this.statement.getConnection();
        RewriteResult rewriteResult =
                ParseNodeUtil.rewrite(subquerySelectStatement, phoenixConnection);
        int maxRows = this.statement.getMaxRows();
        this.statement.setMaxRows(pushDownMaxRows ? maxRows : 0); // overwrite maxRows to avoid its impact on inner queries.
        QueryPlan queryPlan = new QueryCompiler(
                this.statement,
                rewriteResult.getRewrittenSelectStatement(),
                rewriteResult.getColumnResolver(),
                bindManager,
                false,
                optimizeSubquery,
                null).compile();
        if (optimizeSubquery) {
            queryPlan = statement.getConnection().getQueryServices().getOptimizer().optimize(
                    statement,
                    queryPlan);
        }
        this.statement.setMaxRows(maxRows); // restore maxRows.
        return queryPlan;
    }

    protected QueryPlan compileSingleQuery(StatementContext context, SelectStatement select, List<Object> binds, boolean asSubquery, boolean allowPageFilter) throws SQLException{
        SelectStatement innerSelect = select.getInnerSelectStatement();
        if (innerSelect == null) {
            return compileSingleFlatQuery(context, select, binds, asSubquery, allowPageFilter, null, false, false);
        }

        if((innerSelect.getOffset() != null && (!innerSelect.getOffset().isIntegerOffset()) ||
                select.getOffset() != null && !select.getOffset().isIntegerOffset())) {
            throw new SQLException("RVC Offset not allowed with subqueries.");
        }

        QueryPlan innerPlan = compileSubquery(innerSelect, false);
        RowProjector innerQueryPlanRowProjector = innerPlan.getProjector();
        TupleProjector tupleProjector = new TupleProjector(innerQueryPlanRowProjector);

        // Replace the original resolver and table with those having compiled type info.
        TableRef tableRef = context.getResolver().getTables().get(0);
        ColumnResolver resolver = FromCompiler.getResolverForCompiledDerivedTable(statement.getConnection(), tableRef, innerQueryPlanRowProjector);
        context.setResolver(resolver);
        tableRef = resolver.getTables().get(0);
        context.setCurrentTable(tableRef);
        innerPlan = new TupleProjectionPlan(innerPlan, tupleProjector, context, null);

        return compileSingleFlatQuery(context, select, binds, asSubquery, allowPageFilter, innerPlan, false, false);
    }

    protected QueryPlan compileSingleFlatQuery(
            StatementContext context,
            SelectStatement select,
            List<Object> binds,
            boolean asSubquery,
            boolean allowPageFilter,
            QueryPlan innerPlan,
            boolean inJoin,
            boolean inUnion) throws SQLException {
        boolean isApplicable = true;
        PTable projectedTable = null;
        if (this.projectTuples) {
            projectedTable = TupleProjectionCompiler.createProjectedTable(select, context);
            if (projectedTable != null) {
                context.setResolver(FromCompiler.getResolverForProjectedTable(projectedTable, context.getConnection(), select.getUdfParseNodes()));
            }
        }
        
        ColumnResolver resolver = context.getResolver();
        TableRef tableRef = context.getCurrentTable();
        PTable table = tableRef.getTable();

        ParseNode viewWhere = null;
        if (table.getViewStatement() != null) {
            viewWhere = new SQLParser(table.getViewStatement()).parseQuery().getWhere();
        }
        Integer limit = LimitCompiler.compile(context, select);

        CompiledOffset compiledOffset = null;
        Integer offset = null;
        try {
            compiledOffset = OffsetCompiler.getOffsetCompiler().compile(context, select, inJoin, inUnion);
            offset = compiledOffset.getIntegerOffset().orNull();
        } catch(RowValueConstructorOffsetNotCoercibleException e){
            //This current plan is not executable
            compiledOffset = new CompiledOffset(Optional.<Integer>absent(),Optional.<byte[]>absent());
            isApplicable = false;
        }

        GroupBy groupBy = GroupByCompiler.compile(context, select);
        // Optimize the HAVING clause by finding any group by expressions that can be moved
        // to the WHERE clause
        select = HavingCompiler.rewrite(context, select, groupBy);
        Expression having = HavingCompiler.compile(context, select, groupBy);
        // Don't pass groupBy when building where clause expression, because we do not want to wrap these
        // expressions as group by key expressions since they're pre, not post filtered.
        if (innerPlan == null && !tableRef.equals(resolver.getTables().get(0))) {
        	context.setResolver(FromCompiler.getResolver(context.getConnection(), tableRef, select.getUdfParseNodes()));
        }
        Set<SubqueryParseNode> subqueries = Sets.<SubqueryParseNode> newHashSet();
        Expression where = WhereCompiler.compile(context, select, viewWhere, subqueries, compiledOffset.getByteOffset());
        // Recompile GROUP BY now that we've figured out our ScanRanges so we know
        // definitively whether or not we'll traverse in row key order.
        groupBy = groupBy.compile(context, innerPlan, where);
        context.setResolver(resolver); // recover resolver
        boolean wildcardIncludesDynamicCols = context.getConnection().getQueryServices()
                .getConfiguration().getBoolean(WILDCARD_QUERY_DYNAMIC_COLS_ATTRIB,
                        DEFAULT_WILDCARD_QUERY_DYNAMIC_COLS_ATTRIB);
        RowProjector projector = ProjectionCompiler.compile(context, select, groupBy,
                asSubquery ? Collections.emptyList() : targetColumns, where,
                wildcardIncludesDynamicCols);
        OrderBy orderBy = OrderByCompiler.compile(
                context,
                select,
                groupBy,
                limit,
                compiledOffset,
                projector,
                innerPlan,
                where);
        context.getAggregationManager().compile(context, groupBy);
        // Final step is to build the query plan
        if (!asSubquery) {
            int maxRows = statement.getMaxRows();
            if (maxRows > 0) {
                if (limit != null) {
                    limit = Math.min(limit, maxRows);
                } else {
                    limit = maxRows;
                }
            }
        }

        if (projectedTable != null) {
            TupleProjector.serializeProjectorIntoScan(context.getScan(),
                    new TupleProjector(projectedTable), wildcardIncludesDynamicCols &&
                            projector.projectDynColsInWildcardQueries());
        }
        
        QueryPlan plan = innerPlan;
        QueryPlan dataPlan = dataPlans.get(tableRef);
        if (plan == null) {
            ParallelIteratorFactory parallelIteratorFactory = asSubquery ? null : this.parallelIteratorFactory;
            plan = select.getFrom() == null
                    ? new LiteralResultIterationPlan(context, select, tableRef, projector, limit, offset, orderBy,
                            parallelIteratorFactory)
                    : (select.isAggregate() || select.isDistinct()
                            ? new AggregatePlan(context, select, tableRef, projector, limit, offset, orderBy,
                                    parallelIteratorFactory, groupBy, having, dataPlan)
                            : new ScanPlan(context, select, tableRef, projector, limit, offset, orderBy,
                                    parallelIteratorFactory, allowPageFilter, dataPlan, compiledOffset.getByteOffset()));
        }
        SelectStatement planSelect = asSubquery ? select : this.select;
        if (!subqueries.isEmpty()) {
            int count = subqueries.size();
            WhereClauseSubPlan[] subPlans = new WhereClauseSubPlan[count];
            int i = 0;
            for (SubqueryParseNode subqueryNode : subqueries) {
                SelectStatement stmt = subqueryNode.getSelectNode();
                subPlans[i++] = new WhereClauseSubPlan(compileSubquery(stmt, false), stmt, subqueryNode.expectSingleRow());
            }
            plan = HashJoinPlan.create(planSelect, plan, null, subPlans);
        }

        if (innerPlan != null) {
            if (LiteralExpression.isTrue(where)) {
                where = null; // we do not pass "true" as filter
            }
            plan = select.isAggregate() || select.isDistinct()
                    ? new ClientAggregatePlan(context, planSelect, tableRef, projector, limit, offset, where, orderBy,
                            groupBy, having, plan)
                    : new ClientScanPlan(context, planSelect, tableRef, projector, limit, offset, where, orderBy, plan);

        }

        if(plan instanceof BaseQueryPlan){
            ((BaseQueryPlan) plan).setApplicable(isApplicable);
        }
        return plan;
    }
}
