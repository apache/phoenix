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
import org.apache.phoenix.execute.BasicQueryPlan;
import org.apache.phoenix.execute.HashJoinPlan;
import org.apache.phoenix.execute.ScanPlan;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.iterate.ParallelIterators.ParallelIteratorFactory;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.join.HashJoinInfo;
import org.apache.phoenix.join.TupleProjector;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.parse.JoinTableNode.JoinType;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.ScanUtil;



/**
 * 
 * Class used to build an executable query plan
 *
 * 
 * @since 0.1
 */
public class QueryCompiler {
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
    
    public QueryCompiler(PhoenixStatement statement, SelectStatement select, ColumnResolver resolver) throws SQLException {
        this(statement, select, resolver, Collections.<PDatum>emptyList(), null);
    }
    
    public QueryCompiler(PhoenixStatement statement, SelectStatement select, ColumnResolver resolver, List<? extends PDatum> targetColumns, ParallelIteratorFactory parallelIteratorFactory) throws SQLException {
        this.statement = statement;
        this.select = select;
        this.resolver = resolver;
        this.scan = new Scan();
        this.targetColumns = targetColumns;
        this.parallelIteratorFactory = parallelIteratorFactory;
        if (statement.getConnection().getQueryServices().getLowestClusterHBaseVersion() >= PhoenixDatabaseMetaData.ESSENTIAL_FAMILY_VERSION_THRESHOLD) {
            this.scan.setAttribute(LOAD_COLUMN_FAMILIES_ON_DEMAND_ATTR, QueryConstants.TRUE);
        }
        if (select.getHint().hasHint(Hint.NO_CACHE)) {
            scan.setCacheBlocks(false);
        }

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
        StatementContext context = new StatementContext(statement, resolver, scan);
        if (select.isJoin()) {
            select = JoinCompiler.optimize(statement, select, resolver);
            if (this.select != select) {
                ColumnResolver resolver = FromCompiler.getResolverForQuery(select, statement.getConnection());
                context = new StatementContext(statement, resolver, scan);
            }
            JoinTable joinTable = JoinCompiler.compile(statement, select, context.getResolver());
            return compileJoinQuery(context, binds, joinTable, false);
        } else {
            return compileSingleQuery(context, select, binds, parallelIteratorFactory, true);
        }
    }
    
    @SuppressWarnings("unchecked")
    protected QueryPlan compileJoinQuery(StatementContext context, List<Object> binds, JoinTable joinTable, boolean asSubquery) throws SQLException {
        byte[] emptyByteArray = new byte[0];
        List<JoinSpec> joinSpecs = joinTable.getJoinSpecs();
        if (joinSpecs.isEmpty()) {
            Table table = joinTable.getTable();
            SelectStatement subquery = table.getAsSubquery();
            if (!table.isSubselect()) {
                ProjectedPTableWrapper projectedTable = table.createProjectedTable(!asSubquery);
                TupleProjector.serializeProjectorIntoScan(context.getScan(), projectedTable.createTupleProjector());
                context.setCurrentTable(table.getTableRef());
                context.setResolver(projectedTable.createColumnResolver());
                table.projectColumns(context.getScan());
                return compileSingleQuery(context, subquery, binds, null, true);
            }
            QueryPlan plan = compileSubquery(subquery);
            ProjectedPTableWrapper projectedTable = table.createProjectedTable(plan.getProjector());
            context.setResolver(projectedTable.createColumnResolver());
            context.setClientTupleProjector(projectedTable.createTupleProjector());
            return plan;
        }
        
        boolean[] starJoinVector = joinTable.getStarJoinVector();
        if (starJoinVector != null) {
            Table table = joinTable.getTable();
            ProjectedPTableWrapper initialProjectedTable;
            TableRef tableRef;
            SelectStatement query;
            if (!table.isSubselect()) {
                initialProjectedTable = table.createProjectedTable(!asSubquery);
                tableRef = table.getTableRef();
                table.projectColumns(context.getScan());
                query = joinTable.getAsSingleSubquery(table.getAsSubquery(), asSubquery);
            } else {
                SelectStatement subquery = table.getAsSubquery();
                QueryPlan plan = compileSubquery(subquery);
                initialProjectedTable = table.createProjectedTable(plan.getProjector());
                tableRef = plan.getTableRef();
                context.getScan().setFamilyMap(plan.getContext().getScan().getFamilyMap());
                query = joinTable.getAsSingleSubquery((SelectStatement) plan.getStatement(), asSubquery);
            }
            PTableWrapper projectedTable = initialProjectedTable;
            int count = joinSpecs.size();
            ImmutableBytesPtr[] joinIds = new ImmutableBytesPtr[count];
            List<Expression>[] joinExpressions = new List[count];
            List<Expression>[] hashExpressions = new List[count];
            JoinType[] joinTypes = new JoinType[count];
            PTable[] tables = new PTable[count];
            int[] fieldPositions = new int[count];
            QueryPlan[] joinPlans = new QueryPlan[count];
            TupleProjector[] clientProjectors = new TupleProjector[count];
            fieldPositions[0] = projectedTable.getTable().getColumns().size() - projectedTable.getTable().getPKColumns().size();
            boolean forceProjection = table.isSubselect();
            boolean needsProject = forceProjection || asSubquery;
            for (int i = 0; i < count; i++) {
                JoinSpec joinSpec = joinSpecs.get(i);
                Scan subScan = ScanUtil.newScan(originalScan);
                StatementContext subContext = new StatementContext(statement, context.getResolver(), subScan);
                joinPlans[i] = compileJoinQuery(subContext, binds, joinSpec.getJoinTable(), true);
                ColumnResolver resolver = subContext.getResolver();
                clientProjectors[i] = subContext.getClientTupleProjector();
                boolean hasPostReference = joinSpec.getJoinTable().hasPostReference();
                if (hasPostReference) {
                    PTableWrapper subProjTable = ((JoinedTableColumnResolver) (resolver)).getPTableWrapper();
                    tables[i] = subProjTable.getTable();
                    projectedTable = projectedTable.mergeProjectedTables(subProjTable, joinSpec.getType() == JoinType.Inner);
                    needsProject = true;
                } else {
                    tables[i] = null;
                }
                if (!starJoinVector[i]) {
                    needsProject = true;
                }
                ColumnResolver leftResolver = (!forceProjection && starJoinVector[i]) ? joinTable.getOriginalResolver() : projectedTable.createColumnResolver();
                joinIds[i] = new ImmutableBytesPtr(emptyByteArray); // place-holder
                Pair<List<Expression>, List<Expression>> joinConditions = joinSpec.compileJoinConditions(context, leftResolver, resolver);
                joinExpressions[i] = joinConditions.getFirst();
                hashExpressions[i] = joinConditions.getSecond();
                joinTypes[i] = joinSpec.getType();
                if (i < count - 1) {
                    fieldPositions[i + 1] = fieldPositions[i] + (tables[i] == null ? 0 : (tables[i].getColumns().size() - tables[i].getPKColumns().size()));
                }
            }
            if (needsProject) {
                TupleProjector.serializeProjectorIntoScan(context.getScan(), initialProjectedTable.createTupleProjector());
            }
            context.setCurrentTable(tableRef);
            context.setResolver(needsProject ? projectedTable.createColumnResolver() : joinTable.getOriginalResolver());
            BasicQueryPlan plan = compileSingleQuery(context, query, binds, parallelIteratorFactory, joinTable.isAllLeftJoin());
            Expression postJoinFilterExpression = joinTable.compilePostFilterExpression(context);
            Integer limit = null;
            if (query.getLimit() != null && !query.isAggregate() && !query.isDistinct() && query.getOrderBy().isEmpty()) {
                limit = LimitCompiler.compile(context, query);
            }
            HashJoinInfo joinInfo = new HashJoinInfo(projectedTable.getTable(), joinIds, joinExpressions, joinTypes, starJoinVector, tables, fieldPositions, postJoinFilterExpression, limit, forceProjection);
            return new HashJoinPlan(joinTable.getStatement(), plan, joinInfo, hashExpressions, joinPlans, clientProjectors);
        }
        
        JoinSpec lastJoinSpec = joinSpecs.get(joinSpecs.size() - 1);
        JoinType type = lastJoinSpec.getType();
        if (type == JoinType.Full)
            throw new SQLFeatureNotSupportedException("Full joins not supported.");
        
        if (type == JoinType.Right || type == JoinType.Inner) {
            if (!lastJoinSpec.getJoinTable().getJoinSpecs().isEmpty())
                throw new SQLFeatureNotSupportedException("Right join followed by sub-join is not supported.");
            
            JoinTable rhsJoinTable = lastJoinSpec.getJoinTable();
            Table rhsTable = rhsJoinTable.getTable();
            JoinTable lhsJoin = joinTable.getSubJoinTableWithoutPostFilters();
            Scan subScan = ScanUtil.newScan(originalScan);
            StatementContext lhsCtx = new StatementContext(statement, context.getResolver(), subScan);
            QueryPlan lhsPlan = compileJoinQuery(lhsCtx, binds, lhsJoin, true);
            ColumnResolver lhsResolver = lhsCtx.getResolver();
            TupleProjector clientProjector = lhsCtx.getClientTupleProjector();
            PTableWrapper lhsProjTable = ((JoinedTableColumnResolver) (lhsResolver)).getPTableWrapper();
            ProjectedPTableWrapper rhsProjTable;
            TableRef rhsTableRef;
            SelectStatement rhs;
            if (!rhsTable.isSubselect()) {
                rhsProjTable = rhsTable.createProjectedTable(!asSubquery);
                rhsTableRef = rhsTable.getTableRef();
                rhsTable.projectColumns(context.getScan());
                rhs = rhsJoinTable.getAsSingleSubquery(rhsTable.getAsSubquery(), asSubquery);
            } else {
                SelectStatement subquery = rhsTable.getAsSubquery();
                QueryPlan plan = compileSubquery(subquery);
                rhsProjTable = rhsTable.createProjectedTable(plan.getProjector());
                rhsTableRef = plan.getTableRef();
                context.getScan().setFamilyMap(plan.getContext().getScan().getFamilyMap());
                rhs = rhsJoinTable.getAsSingleSubquery((SelectStatement) plan.getStatement(), asSubquery);
            }
            boolean forceProjection = rhsTable.isSubselect();
            ColumnResolver rhsResolver = forceProjection ? rhsProjTable.createColumnResolver() : joinTable.getOriginalResolver();
            ImmutableBytesPtr[] joinIds = new ImmutableBytesPtr[] {new ImmutableBytesPtr(emptyByteArray)};
            Pair<List<Expression>, List<Expression>> joinConditions = lastJoinSpec.compileJoinConditions(context, lhsResolver, rhsResolver);
            List<Expression> joinExpressions = joinConditions.getSecond();
            List<Expression> hashExpressions = joinConditions.getFirst();
            int fieldPosition = rhsProjTable.getTable().getColumns().size() - rhsProjTable.getTable().getPKColumns().size();
            PTableWrapper projectedTable = rhsProjTable.mergeProjectedTables(lhsProjTable, type == JoinType.Inner);
            TupleProjector.serializeProjectorIntoScan(context.getScan(), rhsProjTable.createTupleProjector());
            context.setCurrentTable(rhsTableRef);
            context.setResolver(projectedTable.createColumnResolver());
            BasicQueryPlan rhsPlan = compileSingleQuery(context, rhs, binds, parallelIteratorFactory, type == JoinType.Right);
            Expression postJoinFilterExpression = joinTable.compilePostFilterExpression(context);
            Integer limit = null;
            if (rhs.getLimit() != null && !rhs.isAggregate() && !rhs.isDistinct() && rhs.getOrderBy().isEmpty()) {
                limit = LimitCompiler.compile(context, rhs);
            }
            HashJoinInfo joinInfo = new HashJoinInfo(projectedTable.getTable(), joinIds, new List[] {joinExpressions}, new JoinType[] {type == JoinType.Inner ? type : JoinType.Left}, new boolean[] {true}, new PTable[] {lhsProjTable.getTable()}, new int[] {fieldPosition}, postJoinFilterExpression, limit, forceProjection);
            return new HashJoinPlan(joinTable.getStatement(), rhsPlan, joinInfo, new List[] {hashExpressions}, new QueryPlan[] {lhsPlan}, new TupleProjector[] {clientProjector});
        }
        
        // Do not support queries like "A right join B left join C" with hash-joins.
        throw new SQLFeatureNotSupportedException("Joins with pattern 'A right join B left join C' not supported.");
    }
    
    protected QueryPlan compileSubquery(SelectStatement subquery) throws SQLException {
        ColumnResolver resolver = FromCompiler.getResolverForQuery(subquery, this.statement.getConnection());
        subquery = StatementNormalizer.normalize(subquery, resolver);
        QueryPlan plan = new QueryCompiler(this.statement, subquery, resolver).compile();
        return statement.getConnection().getQueryServices().getOptimizer().optimize(statement, plan);
    }
    
    protected BasicQueryPlan compileSingleQuery(StatementContext context, SelectStatement select, List<Object> binds, ParallelIteratorFactory parallelIteratorFactory, boolean allowPageFilter) throws SQLException{
        PhoenixConnection connection = statement.getConnection();
        ColumnResolver resolver = context.getResolver();
        TableRef tableRef = context.getCurrentTable();
        PTable table = tableRef.getTable();
        
        // TODO PHOENIX-944. See DerivedTableIT for a list of unsupported cases.
        if (table.getType() == PTableType.SUBQUERY)
            throw new SQLFeatureNotSupportedException("Complex nested queries not supported.");
        
        ParseNode viewWhere = null;
        if (table.getViewStatement() != null) {
            viewWhere = new SQLParser(table.getViewStatement()).parseQuery().getWhere();
        }
        Integer limit = LimitCompiler.compile(context, select);

        GroupBy groupBy = GroupByCompiler.compile(context, select);
        // Optimize the HAVING clause by finding any group by expressions that can be moved
        // to the WHERE clause
        select = HavingCompiler.rewrite(context, select, groupBy);
        Expression having = HavingCompiler.compile(context, select, groupBy);
        // Don't pass groupBy when building where clause expression, because we do not want to wrap these
        // expressions as group by key expressions since they're pre, not post filtered.
        context.setResolver(FromCompiler.getResolverForQuery(select, connection));
        WhereCompiler.compile(context, select, viewWhere);
        context.setResolver(resolver); // recover resolver
        OrderBy orderBy = OrderByCompiler.compile(context, select, groupBy, limit); 
        RowProjector projector = ProjectionCompiler.compile(context, select, groupBy, targetColumns);
        
        // Final step is to build the query plan
        int maxRows = statement.getMaxRows();
        if (maxRows > 0) {
            if (limit != null) {
                limit = Math.min(limit, maxRows);
            } else {
                limit = maxRows;
            }
        }
        if (select.isAggregate() || select.isDistinct()) {
            return new AggregatePlan(context, select, tableRef, projector, limit, orderBy, parallelIteratorFactory, groupBy, having);
        } else {
            return new ScanPlan(context, select, tableRef, projector, limit, orderBy, parallelIteratorFactory, allowPageFilter);
        }
    }
}


