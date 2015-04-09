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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.JoinCompiler.JoinSpec;
import org.apache.phoenix.compile.JoinCompiler.JoinTable;
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
import org.apache.phoenix.execute.UnionPlan;
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
import org.apache.phoenix.schema.TableNotFoundException;
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
    private final boolean projectTuples;
    private final boolean useSortMergeJoin;
    private final boolean noChildParentJoinOptimization;

    public QueryCompiler(PhoenixStatement statement, SelectStatement select, ColumnResolver resolver) throws SQLException {
        this(statement, select, resolver, Collections.<PDatum>emptyList(), null, new SequenceManager(statement), true);
    }

    public QueryCompiler(PhoenixStatement statement, SelectStatement select, ColumnResolver resolver, boolean projectTuples) throws SQLException {
        this(statement, select, resolver, Collections.<PDatum>emptyList(), null, new SequenceManager(statement), projectTuples);
    }

    public QueryCompiler(PhoenixStatement statement, SelectStatement select, ColumnResolver resolver, List<? extends PDatum> targetColumns, ParallelIteratorFactory parallelIteratorFactory, SequenceManager sequenceManager, boolean projectTuples) throws SQLException {
        this.statement = statement;
        this.select = select;
        this.resolver = resolver;
        this.scan = new Scan();
        this.targetColumns = targetColumns;
        this.parallelIteratorFactory = parallelIteratorFactory;
        this.sequenceManager = sequenceManager;
        this.projectTuples = projectTuples;
        this.useSortMergeJoin = select.getHint().hasHint(Hint.USE_SORT_MERGE_JOIN);
        this.noChildParentJoinOptimization = select.getHint().hasHint(Hint.NO_CHILD_PARENT_JOIN_OPTIMIZATION);
        if (statement.getConnection().getQueryServices().getLowestClusterHBaseVersion() >= PhoenixDatabaseMetaData.ESSENTIAL_FAMILY_VERSION_THRESHOLD) {
            this.scan.setAttribute(LOAD_COLUMN_FAMILIES_ON_DEMAND_ATTR, QueryConstants.TRUE);
        }
        if (select.getHint().hasHint(Hint.NO_CACHE)) {
            scan.setCacheBlocks(false);
        }

        scan.setCaching(statement.getFetchSize());
        this.originalScan = ScanUtil.newScan(scan);
    }

    public QueryCompiler(PhoenixStatement statement, SelectStatement select, ColumnResolver resolver, List<? extends PDatum> targetColumns, ParallelIteratorFactory parallelIteratorFactory, SequenceManager sequenceManager) throws SQLException {
        this(statement, select, resolver, targetColumns, parallelIteratorFactory, sequenceManager, true);
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
        QueryPlan plan;
        if (select.isUnion()) {
            plan = compileUnionAll(select);
        } else {
            plan = compileSelect(select);
        }
        return plan;
    }

    public QueryPlan compileUnionAll(SelectStatement select) throws SQLException { 
        List<SelectStatement> unionAllSelects = select.getSelects();
        List<QueryPlan> plans = new ArrayList<QueryPlan>();

        for (int i=0; i < unionAllSelects.size(); i++ ) {
            SelectStatement subSelect = unionAllSelects.get(i);
            // Push down order-by and limit into sub-selects.
            if (!select.getOrderBy().isEmpty() || select.getLimit() != null) {
                subSelect = NODE_FACTORY.select(subSelect, select.getOrderBy(), select.getLimit());
            }
            QueryPlan subPlan = compileSubquery(subSelect, true);
            TupleProjector projector = new TupleProjector(subPlan.getProjector());
            subPlan = new TupleProjectionPlan(subPlan, projector, null);
            plans.add(subPlan);
        }
        UnionCompiler.checkProjectionNumAndTypes(plans);

        TableRef tableRef = UnionCompiler.contructSchemaTable(statement, plans.get(0), select.hasWildcard() ? null : select.getSelect());
        ColumnResolver resolver = FromCompiler.getResolver(tableRef);
        StatementContext context = new StatementContext(statement, resolver, scan, sequenceManager);

        QueryPlan plan = compileSingleFlatQuery(context, select, statement.getParameters(), false, false, null, null, false);
        plan =  new UnionPlan(context, select, tableRef, plan.getProjector(), plan.getLimit(), plan.getOrderBy(), GroupBy.EMPTY_GROUP_BY, plans, null); 
        return plan;
    }

    public QueryPlan compileSelect(SelectStatement select) throws SQLException{
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
                context.setCurrentTable(table.getTableRef());
                PTable projectedTable = table.createProjectedTable(!projectPKColumns, context);
                TupleProjector.serializeProjectorIntoScan(context.getScan(), new TupleProjector(projectedTable));
                context.setResolver(FromCompiler.getResolverForProjectedTable(projectedTable));
                table.projectColumns(context.getScan());
                return compileSingleQuery(context, subquery, binds, asSubquery, !asSubquery);
            }
            QueryPlan plan = compileSubquery(subquery, false);
            PTable projectedTable = table.createProjectedTable(plan.getProjector());
            context.setResolver(FromCompiler.getResolverForProjectedTable(projectedTable));
            return new TupleProjectionPlan(plan, new TupleProjector(plan.getProjector()), table.compilePostFilterExpression(context));
        }

        boolean[] starJoinVector;
        if (!this.useSortMergeJoin && (starJoinVector = joinTable.getStarJoinVector()) != null) {
            Table table = joinTable.getTable();
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
            HashSubPlan[] subPlans = new HashSubPlan[count];
            fieldPositions[0] = projectedTable.getColumns().size() - projectedTable.getPKColumns().size();
            for (int i = 0; i < count; i++) {
                JoinSpec joinSpec = joinSpecs.get(i);
                Scan subScan = ScanUtil.newScan(originalScan);
                StatementContext subContext = new StatementContext(statement, context.getResolver(), subScan, new SequenceManager(statement));
                QueryPlan joinPlan = compileJoinQuery(subContext, binds, joinSpec.getJoinTable(), true, true, null);
                boolean hasPostReference = joinSpec.getJoinTable().hasPostReference();
                if (hasPostReference) {
                    tables[i] = subContext.getResolver().getTables().get(0).getTable();
                    projectedTable = JoinCompiler.joinProjectedTables(projectedTable, tables[i], joinSpec.getType());
                } else {
                    tables[i] = null;
                }
                context.setResolver(FromCompiler.getResolverForProjectedTable(projectedTable));
                joinIds[i] = new ImmutableBytesPtr(emptyByteArray); // place-holder
                Pair<List<Expression>, List<Expression>> joinConditions = joinSpec.compileJoinConditions(context, subContext, true);
                joinExpressions[i] = joinConditions.getFirst();
                List<Expression> hashExpressions = joinConditions.getSecond();
                Pair<Expression, Expression> keyRangeExpressions = new Pair<Expression, Expression>(null, null);
                boolean optimized = getKeyExpressionCombinations(keyRangeExpressions, context, joinTable.getStatement(), tableRef, joinSpec.getType(), joinExpressions[i], hashExpressions);
                Expression keyRangeLhsExpression = keyRangeExpressions.getFirst();
                Expression keyRangeRhsExpression = keyRangeExpressions.getSecond();
                joinTypes[i] = joinSpec.getType();
                if (i < count - 1) {
                    fieldPositions[i + 1] = fieldPositions[i] + (tables[i] == null ? 0 : (tables[i].getColumns().size() - tables[i].getPKColumns().size()));
                }
                subPlans[i] = new HashSubPlan(i, joinPlan, optimized ? null : hashExpressions, joinSpec.isSingleValueOnly(), keyRangeLhsExpression, keyRangeRhsExpression);
            }
            TupleProjector.serializeProjectorIntoScan(context.getScan(), tupleProjector);
            QueryPlan plan = compileSingleQuery(context, query, binds, asSubquery, !asSubquery && joinTable.isAllLeftJoin());
            Expression postJoinFilterExpression = joinTable.compilePostFilterExpression(context, table);
            Integer limit = null;
            if (!query.isAggregate() && !query.isDistinct() && query.getOrderBy().isEmpty()) {
                limit = plan.getLimit();
            }
            HashJoinInfo joinInfo = new HashJoinInfo(projectedTable, joinIds, joinExpressions, joinTypes, starJoinVector, tables, fieldPositions, postJoinFilterExpression, limit);
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
            context.setResolver(FromCompiler.getResolverForProjectedTable(rhsProjTable));
            ImmutableBytesPtr[] joinIds = new ImmutableBytesPtr[] {new ImmutableBytesPtr(emptyByteArray)};
            Pair<List<Expression>, List<Expression>> joinConditions = lastJoinSpec.compileJoinConditions(lhsCtx, context, true);
            List<Expression> joinExpressions = joinConditions.getSecond();
            List<Expression> hashExpressions = joinConditions.getFirst();
            boolean needsMerge = lhsJoin.hasPostReference();
            PTable lhsTable = needsMerge ? lhsCtx.getResolver().getTables().get(0).getTable() : null;
            int fieldPosition = needsMerge ? rhsProjTable.getColumns().size() - rhsProjTable.getPKColumns().size() : 0;
            PTable projectedTable = needsMerge ? JoinCompiler.joinProjectedTables(rhsProjTable, lhsTable, type == JoinType.Right ? JoinType.Left : type) : rhsProjTable;
            TupleProjector.serializeProjectorIntoScan(context.getScan(), tupleProjector);
            context.setResolver(FromCompiler.getResolverForProjectedTable(projectedTable));
            QueryPlan rhsPlan = compileSingleQuery(context, rhs, binds, asSubquery, !asSubquery && type == JoinType.Right);
            Expression postJoinFilterExpression = joinTable.compilePostFilterExpression(context, rhsTable);
            Integer limit = null;
            if (!rhs.isAggregate() && !rhs.isDistinct() && rhs.getOrderBy().isEmpty()) {
                limit = rhsPlan.getLimit();
            }
            HashJoinInfo joinInfo = new HashJoinInfo(projectedTable, joinIds, new List[] {joinExpressions}, new JoinType[] {type == JoinType.Right ? JoinType.Left : type}, new boolean[] {true}, new PTable[] {lhsTable}, new int[] {fieldPosition}, postJoinFilterExpression, limit);
            Pair<Expression, Expression> keyRangeExpressions = new Pair<Expression, Expression>(null, null);
            getKeyExpressionCombinations(keyRangeExpressions, context, joinTable.getStatement(), rhsTableRef, type, joinExpressions, hashExpressions);
            return HashJoinPlan.create(joinTable.getStatement(), rhsPlan, joinInfo, new HashSubPlan[] {new HashSubPlan(0, lhsPlan, hashExpressions, false, keyRangeExpressions.getFirst(), keyRangeExpressions.getSecond())});
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
        PTable lhsProjTable = lhsCtx.getResolver().getTables().get(0).getTable();
        boolean isInRowKeyOrder = preserveRowkey && lhsPlan.getOrderBy().getOrderByExpressions().isEmpty();
        
        Scan rhsScan = ScanUtil.newScan(originalScan);
        StatementContext rhsCtx = new StatementContext(statement, context.getResolver(), rhsScan, new SequenceManager(statement));
        QueryPlan rhsPlan = compileJoinQuery(rhsCtx, binds, rhsJoin, true, true, rhsOrderBy);
        PTable rhsProjTable = rhsCtx.getResolver().getTables().get(0).getTable();
        
        Pair<List<Expression>, List<Expression>> joinConditions = lastJoinSpec.compileJoinConditions(type == JoinType.Right ? rhsCtx : lhsCtx, type == JoinType.Right ? lhsCtx : rhsCtx, false);
        List<Expression> lhsKeyExpressions = type == JoinType.Right ? joinConditions.getSecond() : joinConditions.getFirst();
        List<Expression> rhsKeyExpressions = type == JoinType.Right ? joinConditions.getFirst() : joinConditions.getSecond();
        
        boolean needsMerge = rhsJoin.hasPostReference();
        int fieldPosition = needsMerge ? lhsProjTable.getColumns().size() - lhsProjTable.getPKColumns().size() : 0;
        PTable projectedTable = needsMerge ? JoinCompiler.joinProjectedTables(lhsProjTable, rhsProjTable, type == JoinType.Right ? JoinType.Left : type) : lhsProjTable;

        ColumnResolver resolver = FromCompiler.getResolverForProjectedTable(projectedTable);
        TableRef tableRef = resolver.getTables().get(0);
        StatementContext subCtx = new StatementContext(statement, resolver, ScanUtil.newScan(originalScan), new SequenceManager(statement));
        subCtx.setCurrentTable(tableRef);
        QueryPlan innerPlan = new SortMergeJoinPlan(subCtx, joinTable.getStatement(), tableRef, type == JoinType.Right ? JoinType.Left : type, lhsPlan, rhsPlan, lhsKeyExpressions, rhsKeyExpressions, projectedTable, lhsProjTable, needsMerge ? rhsProjTable : null, fieldPosition, lastJoinSpec.isSingleValueOnly());
        context.setCurrentTable(tableRef);
        context.setResolver(resolver);
        TableNode from = NODE_FACTORY.namedTable(tableRef.getTableAlias(), NODE_FACTORY.table(tableRef.getTable().getSchemaName().getString(), tableRef.getTable().getTableName().getString()));
        ParseNode where = joinTable.getPostFiltersCombined();
        SelectStatement select = asSubquery ? NODE_FACTORY.select(from, joinTable.getStatement().getHint(), false, Collections.<AliasedNode> emptyList(), where, null, null, orderBy, null, 0, false, joinTable.getStatement().hasSequence(), Collections.<SelectStatement>emptyList())
                : NODE_FACTORY.select(joinTable.getStatement(), from, where);
        
        return compileSingleFlatQuery(context, select, binds, asSubquery, false, innerPlan, null, isInRowKeyOrder);
    }

    private boolean getKeyExpressionCombinations(Pair<Expression, Expression> combination, StatementContext context, SelectStatement select, TableRef table, JoinType type, final List<Expression> joinExpressions, final List<Expression> hashExpressions) throws SQLException {
        if ((type != JoinType.Inner && type != JoinType.Semi) || this.noChildParentJoinOptimization)
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

    protected QueryPlan compileSubquery(SelectStatement subquery, boolean pushDownMaxRows) throws SQLException {
        PhoenixConnection connection = this.statement.getConnection();
        subquery = SubselectRewriter.flatten(subquery, connection);
        ColumnResolver resolver = FromCompiler.getResolverForQuery(subquery, connection);
        subquery = StatementNormalizer.normalize(subquery, resolver);
        SelectStatement transformedSubquery = SubqueryRewriter.transform(subquery, resolver, connection);
        if (transformedSubquery != subquery) {
            resolver = FromCompiler.getResolverForQuery(transformedSubquery, connection);
            subquery = StatementNormalizer.normalize(transformedSubquery, resolver);
        }
        int maxRows = this.statement.getMaxRows();
        this.statement.setMaxRows(pushDownMaxRows ? maxRows : 0); // overwrite maxRows to avoid its impact on inner queries.
        QueryPlan plan = new QueryCompiler(this.statement, subquery, resolver, false).compile();
        plan = statement.getConnection().getQueryServices().getOptimizer().optimize(statement, plan);
        this.statement.setMaxRows(maxRows); // restore maxRows.
        return plan;
    }

    protected QueryPlan compileSingleQuery(StatementContext context, SelectStatement select, List<Object> binds, boolean asSubquery, boolean allowPageFilter) throws SQLException{
        SelectStatement innerSelect = select.getInnerSelectStatement();
        if (innerSelect == null) {
            return compileSingleFlatQuery(context, select, binds, asSubquery, allowPageFilter, null, null, true);
        }

        QueryPlan innerPlan = compileSubquery(innerSelect, false);
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
        PTable projectedTable = null;
        if (this.projectTuples) {
            projectedTable = TupleProjectionCompiler.createProjectedTable(select, context);
            if (projectedTable != null) {
                context.setResolver(FromCompiler.getResolverForProjectedTable(projectedTable));
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

        GroupBy groupBy = GroupByCompiler.compile(context, select, innerPlanTupleProjector, isInRowKeyOrder);
        // Optimize the HAVING clause by finding any group by expressions that can be moved
        // to the WHERE clause
        select = HavingCompiler.rewrite(context, select, groupBy);
        Expression having = HavingCompiler.compile(context, select, groupBy);
        // Don't pass groupBy when building where clause expression, because we do not want to wrap these
        // expressions as group by key expressions since they're pre, not post filtered.
        if (innerPlan == null && !tableRef.equals(resolver.getTables().get(0))) {
            context.setResolver(FromCompiler.getResolverForQuery(select, this.statement.getConnection()));
        }
        Set<SubqueryParseNode> subqueries = Sets.<SubqueryParseNode> newHashSet();
        Expression where = WhereCompiler.compile(context, select, viewWhere, subqueries);
        context.setResolver(resolver); // recover resolver
        OrderBy orderBy = OrderByCompiler.compile(context, select, groupBy, limit, isInRowKeyOrder); 
        RowProjector projector = ProjectionCompiler.compile(context, select, groupBy, asSubquery ? Collections.<PDatum>emptyList() : targetColumns);
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
            TupleProjector.serializeProjectorIntoScan(context.getScan(), new TupleProjector(projectedTable));
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
                subPlans[i++] = new WhereClauseSubPlan(compileSubquery(stmt, false), stmt, subqueryNode.expectSingleRow());
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


