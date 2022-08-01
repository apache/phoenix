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

package org.apache.phoenix.optimize;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.ColumnProjector;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.ExpressionCompiler;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.IndexStatementRewriter;
import org.apache.phoenix.compile.JoinCompiler;
import org.apache.phoenix.compile.QueryCompiler;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.SequenceManager;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.StatementNormalizer;
import org.apache.phoenix.compile.SubqueryRewriter;
import org.apache.phoenix.iterate.ParallelIteratorFactory;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.AliasedNode;
import org.apache.phoenix.parse.AndParseNode;
import org.apache.phoenix.parse.AndRewriterBooleanParseNodeVisitor;
import org.apache.phoenix.parse.BindTableNode;
import org.apache.phoenix.parse.BooleanParseNodeVisitor;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.DerivedTableNode;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.parse.IndexExpressionParseNodeRewriter;
import org.apache.phoenix.parse.JoinTableNode;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.ParseNodeRewriter;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.parse.TableNode;
import org.apache.phoenix.parse.TableNodeVisitor;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.RowValueConstructorOffsetNotCoercibleException;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.ParseNodeUtil;
import org.apache.phoenix.util.ParseNodeUtil.RewriteResult;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

public class QueryOptimizer {
    private static final ParseNodeFactory FACTORY = new ParseNodeFactory();

    private final QueryServices services;
    private final boolean useIndexes;
    private final boolean costBased;
    private long indexPendingDisabledThreshold;

    public QueryOptimizer(QueryServices services) {
        this.services = services;
        this.useIndexes = this.services.getProps().getBoolean(QueryServices.USE_INDEXES_ATTRIB, QueryServicesOptions.DEFAULT_USE_INDEXES);
        this.costBased = this.services.getProps().getBoolean(QueryServices.COST_BASED_OPTIMIZER_ENABLED, QueryServicesOptions.DEFAULT_COST_BASED_OPTIMIZER_ENABLED);
        this.indexPendingDisabledThreshold = this.services.getProps().getLong(QueryServices.INDEX_PENDING_DISABLE_THRESHOLD,
            QueryServicesOptions.DEFAULT_INDEX_PENDING_DISABLE_THRESHOLD);
    }

    public QueryPlan optimize(PhoenixStatement statement, QueryPlan dataPlan) throws SQLException {
        if (dataPlan.getTableRef() == null) {
            return dataPlan;
        }
        return optimize(dataPlan, statement, Collections.<PColumn>emptyList(), null);
    }

    public QueryPlan optimize(PhoenixStatement statement, SelectStatement select) throws SQLException {
        return optimize(statement, select, FromCompiler.getResolverForQuery(select, statement.getConnection()), Collections.<PColumn>emptyList(), null);
    }

    public QueryPlan optimize(PhoenixStatement statement, SelectStatement select, ColumnResolver resolver, List<? extends PDatum> targetColumns, ParallelIteratorFactory parallelIteratorFactory) throws SQLException {
        QueryCompiler compiler = new QueryCompiler(statement, select, resolver, targetColumns, parallelIteratorFactory, new SequenceManager(statement));
        QueryPlan dataPlan = compiler.compile();
        return optimize(dataPlan, statement, targetColumns, parallelIteratorFactory);
    }
    
    public QueryPlan optimize(QueryPlan dataPlan, PhoenixStatement statement, List<? extends PDatum> targetColumns, ParallelIteratorFactory parallelIteratorFactory) throws SQLException {
        List<QueryPlan> plans = getApplicablePlans(dataPlan, statement, targetColumns, parallelIteratorFactory, true);
        return plans.get(0);
    }
    
    public List<QueryPlan> getBestPlan(QueryPlan dataPlan, PhoenixStatement statement, SelectStatement select, ColumnResolver resolver, List<? extends PDatum> targetColumns, ParallelIteratorFactory parallelIteratorFactory) throws SQLException {
        return getApplicablePlans(dataPlan, statement, targetColumns, parallelIteratorFactory, true);
    }
    
    public List<QueryPlan> getApplicablePlans(QueryPlan dataPlan, PhoenixStatement statement, SelectStatement select, ColumnResolver resolver, List<? extends PDatum> targetColumns, ParallelIteratorFactory parallelIteratorFactory) throws SQLException {
        return getApplicablePlans(dataPlan, statement, targetColumns, parallelIteratorFactory, false);
    }
    
    private List<QueryPlan> getApplicablePlans(QueryPlan dataPlan, PhoenixStatement statement, List<? extends PDatum> targetColumns, ParallelIteratorFactory parallelIteratorFactory, boolean stopAtBestPlan) throws SQLException {
        if (!useIndexes) {
            return Collections.singletonList(dataPlan);
        }

        SelectStatement select = (SelectStatement) dataPlan.getStatement();
        if (!select.isUnion()
                && !select.isJoin()
                && select.getInnerSelectStatement() == null
                && (select.getWhere() == null || !select.getWhere().hasSubquery())) {
            return getApplicablePlansForSingleFlatQuery(dataPlan, statement, targetColumns, parallelIteratorFactory, stopAtBestPlan);
        }

        Map<TableRef, QueryPlan> dataPlans = null;
        // Find the optimal index plan for each join tables in a join query or a
        // non-correlated sub-query, then rewrite the query with found index tables.
        if (select.isJoin()
                || (select.getWhere() != null && select.getWhere().hasSubquery())) {
            ColumnResolver resolver = FromCompiler.getResolverForQuery(select, statement.getConnection());
            JoinCompiler.JoinTable join = JoinCompiler.compile(statement, select, resolver);
            Map<TableRef, TableRef> replacement = null;
            for (JoinCompiler.Table table : join.getAllTables()) {
                if (table.isSubselect())
                    continue;
                TableRef tableRef = table.getTableRef();
                SelectStatement stmt = table.getAsSubqueryForOptimization(tableRef.equals(dataPlan.getTableRef()));
                // Replace non-correlated sub-queries in WHERE clause with dummy values
                // so the filter conditions can be taken into account in optimization.
                if (stmt.getWhere() != null && stmt.getWhere().hasSubquery()) {
                    StatementContext context =
                            new StatementContext(statement, resolver, new Scan(), new SequenceManager(statement));;
                    ParseNode dummyWhere = GenSubqueryParamValuesRewriter.replaceWithDummyValues(stmt.getWhere(), context);
                    stmt = FACTORY.select(stmt, dummyWhere);
                }
                // TODO: It seems inefficient to be recompiling the statement again inside of this optimize call
                QueryPlan subDataPlan =
                        new QueryCompiler(
                                statement, stmt,
                                FromCompiler.getResolverForQuery(stmt, statement.getConnection()),
                                false, false, null)
                                .compile();
                QueryPlan subPlan = optimize(statement, subDataPlan);
                TableRef newTableRef = subPlan.getTableRef();
                if (!newTableRef.equals(tableRef)) {
                    if (replacement == null) {
                        replacement = new HashMap<TableRef, TableRef>();
                        dataPlans = new HashMap<TableRef, QueryPlan>();
                    }
                    replacement.put(tableRef, newTableRef);
                    dataPlans.put(newTableRef, subDataPlan);
                }
            }

            if (replacement != null) {
                select = rewriteQueryWithIndexReplacement(
                        statement.getConnection(), resolver, select, replacement);
            }
        }

        // Re-compile the plan with option "optimizeSubquery" turned on, so that enclosed
        // sub-queries can be optimized recursively.
        QueryCompiler compiler = new QueryCompiler(
                statement,
                select,
                FromCompiler.getResolverForQuery(select, statement.getConnection()),
                targetColumns,
                parallelIteratorFactory,
                dataPlan.getContext().getSequenceManager(),
                true,
                true,
                dataPlans);
        return Collections.singletonList(compiler.compile());
    }

    private List<QueryPlan> getApplicablePlansForSingleFlatQuery(QueryPlan dataPlan, PhoenixStatement statement, List<? extends PDatum> targetColumns, ParallelIteratorFactory parallelIteratorFactory, boolean stopAtBestPlan) throws SQLException {
        SelectStatement select = (SelectStatement)dataPlan.getStatement();
        // Exit early if we have a point lookup as we can't get better than that
        if (dataPlan.getContext().getScanRanges().isPointLookup() && stopAtBestPlan && dataPlan.isApplicable()) {
            return Collections.<QueryPlan> singletonList(dataPlan);
        }

        List<PTable>indexes = Lists.newArrayList(dataPlan.getTableRef().getTable().getIndexes());
        if (dataPlan.isApplicable() && (indexes.isEmpty() || dataPlan.isDegenerate() || dataPlan.getTableRef().hasDynamicCols() || select.getHint().hasHint(Hint.NO_INDEX))) {
            return Collections.<QueryPlan> singletonList(dataPlan);
        }
        
        // The targetColumns is set for UPSERT SELECT to ensure that the proper type conversion takes place.
        // For a SELECT, it is empty. In this case, we want to set the targetColumns to match the projection
        // from the dataPlan to ensure that the metadata for when an index is used matches the metadata for
        // when the data table is used.
        if (targetColumns.isEmpty()) {
            List<? extends ColumnProjector> projectors = dataPlan.getProjector().getColumnProjectors();
            List<PDatum> targetDatums = Lists.newArrayListWithExpectedSize(projectors.size());
            for (ColumnProjector projector : projectors) {
                targetDatums.add(projector.getExpression());
            }
            targetColumns = targetDatums;
        }
        
        SelectStatement translatedIndexSelect = IndexStatementRewriter.translate(select, FromCompiler.getResolver(dataPlan.getTableRef()));
        List<QueryPlan> plans = Lists.newArrayListWithExpectedSize(1 + indexes.size());
        plans.add(dataPlan);
        QueryPlan hintedPlan = getHintedQueryPlan(statement, translatedIndexSelect, indexes, targetColumns, parallelIteratorFactory, plans);
        if (hintedPlan != null) {
            if (stopAtBestPlan && hintedPlan.isApplicable()) {
                return Collections.singletonList(hintedPlan);
            }
            plans.add(0, hintedPlan);
        }
        
        for (PTable index : indexes) {
            QueryPlan plan = addPlan(statement, translatedIndexSelect, index, targetColumns, parallelIteratorFactory, dataPlan, false);
            if (plan != null) {
                // Query can't possibly return anything so just return this plan.
                if (plan.isDegenerate()) {
                    return Collections.singletonList(plan);
                }
                plans.add(plan);
            }
        }

        //Only pull out applicable plans, late filtering since dataplan is used to construct the plans
        List<QueryPlan> applicablePlans = Lists.newArrayListWithExpectedSize(plans.size());
        for(QueryPlan plan : plans) {
            if(plan.isApplicable()) {
                applicablePlans.add(plan);
            }
        }
        if(applicablePlans.isEmpty()) {
            //Currently this is the only case for non-applicable plans
            throw new RowValueConstructorOffsetNotCoercibleException("No table or index could be coerced to the PK as the offset. Or an uncovered index was attempted");
        }

        //OrderPlans
        return hintedPlan == null ? orderPlansBestToWorst(select, applicablePlans, stopAtBestPlan) : applicablePlans;
    }
    
    private QueryPlan getHintedQueryPlan(PhoenixStatement statement, SelectStatement select, List<PTable> indexes, List<? extends PDatum> targetColumns, ParallelIteratorFactory parallelIteratorFactory, List<QueryPlan> plans) throws SQLException {
        QueryPlan dataPlan = plans.get(0);
        String indexHint = select.getHint().getHint(Hint.INDEX);
        if (indexHint == null) {
            return null;
        }
        int startIndex = 0;
        String alias = dataPlan.getTableRef().getTableAlias();
        String prefix = HintNode.PREFIX + (alias == null ? dataPlan.getTableRef().getTable().getName().getString() : alias) + HintNode.SEPARATOR;
        while (startIndex < indexHint.length()) {
            startIndex = indexHint.indexOf(prefix, startIndex);
            if (startIndex < 0) {
                return null;
            }
            startIndex += prefix.length();
            boolean done = false; // true when SUFFIX found
            while (startIndex < indexHint.length() && !done) {
                int endIndex;
                int endIndex1 = indexHint.indexOf(HintNode.SEPARATOR, startIndex);
                int endIndex2 = indexHint.indexOf(HintNode.SUFFIX, startIndex);
                if (endIndex1 < 0 && endIndex2 < 0) { // Missing SUFFIX shouldn't happen
                    endIndex = indexHint.length();
                } else if (endIndex1 < 0) {
                    done = true;
                    endIndex = endIndex2;
                } else if (endIndex2 < 0) {
                    endIndex = endIndex1;
                } else {
                    endIndex = Math.min(endIndex1, endIndex2);
                    done = endIndex2 == endIndex;
                }
                String indexName = indexHint.substring(startIndex, endIndex);
                int indexPos = getIndexPosition(indexes, indexName);
                if (indexPos >= 0) {
                    // Hinted index is applicable, so return it's index
                    PTable index = indexes.get(indexPos);
                    indexes.remove(indexPos);
                    QueryPlan plan = addPlan(statement, select, index, targetColumns, parallelIteratorFactory, dataPlan, true);
                    if (plan != null) {
                        return plan;
                    }
                }
                startIndex = endIndex + 1;
            }
        }
        return null;
    }
    
    private static int getIndexPosition(List<PTable> indexes, String indexName) {
        for (int i = 0; i < indexes.size(); i++) {
            if (indexName.equals(indexes.get(i).getTableName().getString())) {
                return i;
            }
        }
        return -1;
    }
    
    private QueryPlan addPlan(PhoenixStatement statement, SelectStatement select, PTable index, List<? extends PDatum> targetColumns, ParallelIteratorFactory parallelIteratorFactory, QueryPlan dataPlan, boolean isHinted) throws SQLException {
        int nColumns = dataPlan.getProjector().getColumnCount();
        String tableAlias = dataPlan.getTableRef().getTableAlias();
		String alias = tableAlias==null ? null : '"' + tableAlias + '"'; // double quote in case it's case sensitive
        String schemaName = index.getParentSchemaName().getString();
        schemaName = schemaName.length() == 0 ? null :  '"' + schemaName + '"';

        String tableName = '"' + index.getTableName().getString() + '"';
        TableNode table = FACTORY.namedTable(alias, FACTORY.table(schemaName, tableName),select.getTableSamplingRate());
        SelectStatement indexSelect = FACTORY.select(select, table);
        ColumnResolver resolver = FromCompiler.getResolverForQuery(indexSelect, statement.getConnection());
        // We will or will not do tuple projection according to the data plan.
        boolean isProjected = dataPlan.getContext().getResolver().getTables().get(0).getTable().getType() == PTableType.PROJECTED;
        // Check index state of now potentially updated index table to make sure it's active
        TableRef indexTableRef = resolver.getTables().get(0);
        indexTableRef.setHinted(isHinted);
        Map<TableRef, QueryPlan> dataPlans = Collections.singletonMap(indexTableRef, dataPlan);
        PTable indexTable = indexTableRef.getTable();
        PIndexState indexState = indexTable.getIndexState();
        if (indexState == PIndexState.ACTIVE || indexState == PIndexState.PENDING_ACTIVE
                || (indexState == PIndexState.PENDING_DISABLE && isUnderPendingDisableThreshold(indexTableRef.getCurrentTime(), indexTable.getIndexDisableTimestamp()))) {
            try {
            	// translate nodes that match expressions that are indexed to the associated column parse node
                indexSelect = ParseNodeRewriter.rewrite(indexSelect, new  IndexExpressionParseNodeRewriter(index, null, statement.getConnection(), indexSelect.getUdfParseNodes()));
                QueryCompiler compiler = new QueryCompiler(statement, indexSelect, resolver, targetColumns, parallelIteratorFactory, dataPlan.getContext().getSequenceManager(), isProjected, true, dataPlans);

                QueryPlan plan = compiler.compile();

                indexTableRef = plan.getTableRef();
                indexTable = indexTableRef.getTable();
                indexState = indexTable.getIndexState();
                // Checking number of columns handles the wildcard cases correctly, as in that case the index
                // must contain all columns from the data table to be able to be used.
                if (indexState == PIndexState.ACTIVE || indexState == PIndexState.PENDING_ACTIVE
                        || (indexState == PIndexState.PENDING_DISABLE && isUnderPendingDisableThreshold(indexTableRef.getCurrentTime(), indexTable.getIndexDisableTimestamp()))) {
                    if (plan.getProjector().getColumnCount() == nColumns) {
                        return plan;
                    } else if (index.getIndexType() == IndexType.GLOBAL) {
                        String schemaNameStr = index.getSchemaName()==null?null:index.getSchemaName().getString();
                        String tableNameStr = index.getTableName()==null?null:index.getTableName().getString();
                        throw new ColumnNotFoundException(schemaNameStr, tableNameStr, null, "*");
                    }
                }
            } catch (ColumnNotFoundException e) {
                /* Means that a column is being used that's not in our index.
                 * Since we currently don't keep stats, we don't know the selectivity of the index.
                 * For now, if this is a hinted plan, we will try rewriting the query as a subquery;
                 * otherwise we just don't use this index (as opposed to trying to join back from
                 * the index table to the data table.
                 */
                SelectStatement dataSelect = (SelectStatement)dataPlan.getStatement();
                ParseNode where = dataSelect.getWhere();
                if (isHinted && where != null) {
                    StatementContext context = new StatementContext(statement, resolver);
                    WhereConditionRewriter whereRewriter = new WhereConditionRewriter(FromCompiler.getResolver(dataPlan.getTableRef()), context);
                    where = where.accept(whereRewriter);
                    if (where != null) {
                        PTable dataTable = dataPlan.getTableRef().getTable();
                        List<PColumn> pkColumns = dataTable.getPKColumns();
                        List<AliasedNode> aliasedNodes = Lists.<AliasedNode>newArrayListWithExpectedSize(pkColumns.size());
                        List<ParseNode> nodes = Lists.<ParseNode>newArrayListWithExpectedSize(pkColumns.size());
                        boolean isSalted = dataTable.getBucketNum() != null;
                        boolean isTenantSpecific = dataTable.isMultiTenant() && statement.getConnection().getTenantId() != null;
                        int posOffset = (isSalted ? 1 : 0) + (isTenantSpecific ? 1 : 0);
                        for (int i = posOffset; i < pkColumns.size(); i++) {
                            PColumn column = pkColumns.get(i);
                            String indexColName = IndexUtil.getIndexColumnName(column);
                            ParseNode indexColNode = new ColumnParseNode(null, '"' + indexColName + '"', indexColName);
                            PDataType indexColType = IndexUtil.getIndexColumnDataType(column);
                            PDataType dataColType = column.getDataType();
                            if (indexColType != dataColType) {
                                indexColNode = FACTORY.cast(indexColNode, dataColType, null, null);
                            }
                            aliasedNodes.add(FACTORY.aliasedNode(null, indexColNode));
                            nodes.add(new ColumnParseNode(null, '"' + column.getName().getString() + '"'));
                        }
                        SelectStatement innerSelect = FACTORY.select(indexSelect.getFrom(), indexSelect.getHint(), false, aliasedNodes, where, null, null, null, null, null, indexSelect.getBindCount(), false, indexSelect.hasSequence(), Collections.<SelectStatement>emptyList(), indexSelect.getUdfParseNodes());
                        ParseNode outerWhere = FACTORY.in(nodes.size() == 1 ? nodes.get(0) : FACTORY.rowValueConstructor(nodes), FACTORY.subquery(innerSelect, false), false, true);
                        ParseNode extractedCondition = whereRewriter.getExtractedCondition();
                        if (extractedCondition != null) {
                            outerWhere = FACTORY.and(Lists.newArrayList(outerWhere, extractedCondition));
                        }
                        HintNode hint = HintNode.combine(HintNode.subtract(indexSelect.getHint(), new Hint[] {Hint.INDEX, Hint.NO_CHILD_PARENT_JOIN_OPTIMIZATION}), FACTORY.hint("NO_INDEX"));
                        SelectStatement query = FACTORY.select(dataSelect, hint, outerWhere);
                        RewriteResult rewriteResult =
                                ParseNodeUtil.rewrite(query, statement.getConnection());
                        QueryPlan plan = new QueryCompiler(
                                statement,
                                rewriteResult.getRewrittenSelectStatement(),
                                rewriteResult.getColumnResolver(),
                                targetColumns,
                                parallelIteratorFactory,
                                dataPlan.getContext().getSequenceManager(),
                                isProjected,
                                true,
                                dataPlans).compile();
                        return plan;
                    }
                }
            }
            catch (RowValueConstructorOffsetNotCoercibleException e) {
                // Could not coerce the user provided RVC Offset so we do not have a plan to add.
                return null;
            }
        }
        return null;
    }

    // returns true if we can still use the index
    // retuns false if we've been in PENDING_DISABLE too long - index should be considered disabled
    private boolean isUnderPendingDisableThreshold(long currentTimestamp, long indexDisableTimestamp) {
        return currentTimestamp - indexDisableTimestamp <= indexPendingDisabledThreshold;
    }

    /**
     * Order the plans among all the possible ones from best to worst.
     * If option COST_BASED_OPTIMIZER_ENABLED is on and stats are available, we order the plans based on
     * their costs, otherwise we use the following simple algorithm:
     * 1) If the query is a point lookup (i.e. we have a set of exact row keys), choose that one immediately.
     * 2) If the query has an ORDER BY and a LIMIT, choose the plan that has all the ORDER BY expression
     * in the same order as the row key columns.
     * 3) If there are more than one plan that meets (1&2), choose the plan with:
     *    a) the most row key columns that may be used to form the start/stop scan key (i.e. bound slots).
     *    b) the plan that preserves ordering for a group by.
     *    c) the non local index table plan
     * @param plans the list of candidate plans
     * @return list of plans ordered from best to worst.
     */
    private List<QueryPlan> orderPlansBestToWorst(SelectStatement select, List<QueryPlan> plans, boolean stopAtBestPlan) {
        final QueryPlan dataPlan = plans.get(0);
        if (plans.size() == 1) {
            return plans;
        }

        if (this.costBased) {
            Collections.sort(plans, new Comparator<QueryPlan>() {
                @Override
                public int compare(QueryPlan plan1, QueryPlan plan2) {
                    return plan1.getCost().compareTo(plan2.getCost());
                }
            });
            // Return ordered list based on cost if stats are available; otherwise fall
            // back to static ordering.
            if (!plans.get(0).getCost().isUnknown()) {
                return stopAtBestPlan ? plans.subList(0, 1) : plans;
            }
        }

        /**
         * If we have a plan(s) that are just point lookups (i.e. fully qualified row
         * keys), then favor those first.
         */
        List<QueryPlan> candidates = Lists.newArrayListWithExpectedSize(plans.size());
        if (stopAtBestPlan) { // If we're stopping at the best plan, only consider point lookups if there are any
            for (QueryPlan plan : plans) {
                if (plan.getContext().getScanRanges().isPointLookup()) {
                    candidates.add(plan);
                }
            }
        } else {
            candidates.addAll(plans);
        }
        /**
         * If we have a plan(s) that removes the order by, choose from among these,
         * as this is typically the most expensive operation. Once we have stats, if
         * there's a limit on the query, we might choose a different plan. For example
         * if the limit was a very large number and the combination of applying other 
         * filters on the row key are estimated to choose fewer rows, we'd choose that
         * one.
         */
        List<QueryPlan> stillCandidates = plans;
        List<QueryPlan> bestCandidates = candidates;
        if (!candidates.isEmpty()) {
            stillCandidates = candidates;
            bestCandidates = Lists.<QueryPlan>newArrayListWithExpectedSize(candidates.size());
        }
        for (QueryPlan plan : stillCandidates) {
            // If ORDER BY optimized out (or not present at all)
            if (plan.getOrderBy().getOrderByExpressions().isEmpty()) {
                bestCandidates.add(plan);
            }
        }
        if (bestCandidates.isEmpty()) {
            bestCandidates.addAll(stillCandidates);
        }
        
        int nViewConstants = 0;
        PTable dataTable = dataPlan.getTableRef().getTable();
        if (dataTable.getType() == PTableType.VIEW) {
            for (PColumn column : dataTable.getColumns()) {
                if (column.getViewConstant() != null) {
                    nViewConstants++;
                }
            }
        }
        final int boundRanges = nViewConstants;
        final boolean useDataOverIndexHint = select.getHint().hasHint(Hint.USE_DATA_OVER_INDEX_TABLE);
        final int comparisonOfDataVersusIndexTable = useDataOverIndexHint ? -1 : 1;
        Collections.sort(bestCandidates, new Comparator<QueryPlan>() {

            @Override
            public int compare(QueryPlan plan1, QueryPlan plan2) {
                PTable table1 = plan1.getTableRef().getTable();
                PTable table2 = plan2.getTableRef().getTable();
                int boundCount1 = plan1.getContext().getScanRanges().getBoundPkColumnCount();
                int boundCount2 = plan2.getContext().getScanRanges().getBoundPkColumnCount();
                // For shared indexes (i.e. indexes on views and local indexes),
                // a) add back any view constants as these won't be in the index, and
                // b) ignore the viewIndexId which will be part of the row key columns.
                boundCount1 += table1.getViewIndexId() == null ? 0 : (boundRanges - 1);
                boundCount2 += table2.getViewIndexId() == null ? 0 : (boundRanges - 1);
                // Adjust for salting. Salting adds a bound range for each salt bucket.
                // (but the sum of buckets cover the entire table)
                boundCount1 -= plan1.getContext().getScanRanges().isSalted() ? 1 : 0;
                boundCount2 -= plan2.getContext().getScanRanges().isSalted() ? 1 : 0;
                int c = boundCount2 - boundCount1;
                if (c != 0) return c;
                if (plan1.getGroupBy() != null && plan2.getGroupBy() != null) {
                    if (plan1.getGroupBy().isOrderPreserving() != plan2.getGroupBy().isOrderPreserving()) {
                        return plan1.getGroupBy().isOrderPreserving() ? -1 : 1;
                    }
                }

                // Use the plan that has fewer "dataColumns" (columns that need to be merged in)
                c = plan1.getContext().getDataColumns().size() - plan2.getContext().getDataColumns().size();
                if (c != 0) return c;

                // Use smaller table (table with fewest kv columns)
                if (!useDataOverIndexHint || (table1.getType() == PTableType.INDEX && table2.getType() == PTableType.INDEX)) {
                    c = (table1.getColumns().size() - table1.getPKColumns().size()) - (table2.getColumns().size() - table2.getPKColumns().size());
                    if (c != 0) return c;
                }

                // If all things are equal, don't choose local index as it forces scan
                // on every region (unless there's no start/stop key)

                if (table1.getIndexType() == IndexType.LOCAL && table2.getIndexType() !=
                        IndexType.LOCAL) {
                    return plan1.getContext().getScanRanges().getRanges().isEmpty() ? -1 : 1;
                }
                if (table2.getIndexType() == IndexType.LOCAL && table1.getIndexType() !=
                        IndexType.LOCAL) {
                    return plan2.getContext().getScanRanges().getRanges().isEmpty() ? 1 : -1;
                }

                // All things being equal, just use the table based on the Hint.USE_DATA_OVER_INDEX_TABLE

                if (table1.getType() == PTableType.INDEX && table2.getType() != PTableType.INDEX) {
                    return -comparisonOfDataVersusIndexTable;
                }
                if (table2.getType() == PTableType.INDEX && table1.getType() != PTableType.INDEX) {
                    return comparisonOfDataVersusIndexTable;
                }
                return 0;
            }
            
        });

        return stopAtBestPlan ? bestCandidates.subList(0, 1) : bestCandidates;
    }

    
    private static class WhereConditionRewriter extends AndRewriterBooleanParseNodeVisitor {
        private final ColumnResolver dataResolver;
        private final ExpressionCompiler expressionCompiler;
        private List<ParseNode> extractedConditions;
        
        public WhereConditionRewriter(ColumnResolver dataResolver, StatementContext context) throws SQLException {
            super(FACTORY);
            this.dataResolver = dataResolver;
            this.expressionCompiler = new ExpressionCompiler(context);
            this.extractedConditions = Lists.<ParseNode> newArrayList();
        }
        
        public ParseNode getExtractedCondition() {
            if (this.extractedConditions.isEmpty())
                return null;
            
            if (this.extractedConditions.size() == 1)
                return this.extractedConditions.get(0);
            
            return FACTORY.and(this.extractedConditions);            
        }

        @Override
        protected ParseNode leaveBooleanNode(ParseNode node, List<ParseNode> l)
                throws SQLException {
            ParseNode translatedNode = IndexStatementRewriter.translate(node, dataResolver);
            expressionCompiler.reset();
            try {
                translatedNode.accept(expressionCompiler);
            } catch (ColumnNotFoundException e) {
                extractedConditions.add(node);
                return null;
            }
            
            return translatedNode;
        }
    }

    private static SelectStatement rewriteQueryWithIndexReplacement(
            final PhoenixConnection connection, final ColumnResolver resolver,
            final SelectStatement select, final Map<TableRef, TableRef> replacement) throws SQLException {
        TableNode from = select.getFrom();
        TableNode newFrom = from.accept(new QueryOptimizerTableNode(resolver, replacement));
        if (from == newFrom) {
            return select;
        }

        SelectStatement indexSelect = IndexStatementRewriter.translate(FACTORY.select(select, newFrom), resolver, replacement);
        for (TableRef indexTableRef : replacement.values()) {
            // replace expressions with corresponding matching columns for functional indexes
            indexSelect = ParseNodeRewriter.rewrite(indexSelect, new IndexExpressionParseNodeRewriter(indexTableRef.getTable(), indexTableRef.getTableAlias(), connection, indexSelect.getUdfParseNodes()));
        }

        return indexSelect;
    }
    private static class QueryOptimizerTableNode implements TableNodeVisitor<TableNode> {
        private final ColumnResolver resolver;
        private final Map<TableRef, TableRef> replacement;

        QueryOptimizerTableNode (ColumnResolver resolver, final Map<TableRef, TableRef> replacement){
            this.resolver = resolver;
            this.replacement = replacement;
        }

        private TableRef resolveTable(String alias, TableName name) throws SQLException {
            if (alias != null)
                return resolver.resolveTable(null, alias);

            return resolver.resolveTable(name.getSchemaName(), name.getTableName());
        }

        private TableName getReplacedTableName(TableRef tableRef) {
            String schemaName = tableRef.getTable().getSchemaName().getString();
            return TableName.create(schemaName.length() == 0 ? null : schemaName, tableRef.getTable().getTableName().getString());
        }

        @Override
        public TableNode visit(BindTableNode boundTableNode) throws SQLException {
            TableRef tableRef = resolveTable(boundTableNode.getAlias(), boundTableNode.getName());
            TableRef replaceRef = replacement.get(tableRef);
            if (replaceRef == null)
                return boundTableNode;

            String alias = boundTableNode.getAlias();
            return FACTORY.bindTable(alias == null ? null : '"' + alias + '"', getReplacedTableName(replaceRef));
        }

        @Override
        public TableNode visit(JoinTableNode joinNode) throws SQLException {
            TableNode lhs = joinNode.getLHS();
            TableNode rhs = joinNode.getRHS();
            TableNode lhsReplace = lhs.accept(this);
            TableNode rhsReplace = rhs.accept(this);
            if (lhs == lhsReplace && rhs == rhsReplace)
                return joinNode;

            return FACTORY.join(joinNode.getType(), lhsReplace, rhsReplace, joinNode.getOnNode(), joinNode.isSingleValueOnly());
        }

        @Override
        public TableNode visit(NamedTableNode namedTableNode)
                    throws SQLException {
            TableRef tableRef = resolveTable(namedTableNode.getAlias(), namedTableNode.getName());
            TableRef replaceRef = replacement.get(tableRef);
            if (replaceRef == null)
                return namedTableNode;

            String alias = namedTableNode.getAlias();
            return FACTORY.namedTable(alias == null ? null : '"' + alias + '"', getReplacedTableName(replaceRef), namedTableNode.getDynamicColumns(), namedTableNode.getTableSamplingRate());
        }

        @Override
        public TableNode visit(DerivedTableNode subselectNode)
                    throws SQLException {
            return subselectNode;
        }
    }
}
