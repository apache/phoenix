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
import java.util.List;

import org.apache.phoenix.compile.ColumnProjector;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.ExpressionCompiler;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.IndexStatementRewriter;
import org.apache.phoenix.compile.QueryCompiler;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.SequenceManager;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.StatementNormalizer;
import org.apache.phoenix.compile.SubqueryRewriter;
import org.apache.phoenix.iterate.ParallelIteratorFactory;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.AliasedNode;
import org.apache.phoenix.parse.AndParseNode;
import org.apache.phoenix.parse.BooleanParseNodeVisitor;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.IndexExpressionParseNodeRewriter;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.ParseNodeRewriter;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.TableNode;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.IndexUtil;

import com.google.common.collect.Lists;

public class QueryOptimizer {
    private static final ParseNodeFactory FACTORY = new ParseNodeFactory();

    private final QueryServices services;
    private final boolean useIndexes;

    public QueryOptimizer(QueryServices services) {
        this.services = services;
        this.useIndexes = this.services.getProps().getBoolean(QueryServices.USE_INDEXES_ATTRIB, QueryServicesOptions.DEFAULT_USE_INDEXES);
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
        List<QueryPlan>plans = getApplicablePlans(dataPlan, statement, targetColumns, parallelIteratorFactory, true);
        return plans.get(0);
    }
    
    public List<QueryPlan> getBestPlan(PhoenixStatement statement, SelectStatement select, ColumnResolver resolver, List<? extends PDatum> targetColumns, ParallelIteratorFactory parallelIteratorFactory) throws SQLException {
        return getApplicablePlans(statement, select, resolver, targetColumns, parallelIteratorFactory, true);
    }
    
    public List<QueryPlan> getApplicablePlans(PhoenixStatement statement, SelectStatement select, ColumnResolver resolver, List<? extends PDatum> targetColumns, ParallelIteratorFactory parallelIteratorFactory) throws SQLException {
        return getApplicablePlans(statement, select, resolver, targetColumns, parallelIteratorFactory, false);
    }
    
    private List<QueryPlan> getApplicablePlans(PhoenixStatement statement, SelectStatement select, ColumnResolver resolver, List<? extends PDatum> targetColumns, ParallelIteratorFactory parallelIteratorFactory, boolean stopAtBestPlan) throws SQLException {
        QueryCompiler compiler = new QueryCompiler(statement, select, resolver, targetColumns, parallelIteratorFactory, new SequenceManager(statement));
        QueryPlan dataPlan = compiler.compile();
        return getApplicablePlans(dataPlan, statement, targetColumns, parallelIteratorFactory, false);
    }
    
    private List<QueryPlan> getApplicablePlans(QueryPlan dataPlan, PhoenixStatement statement, List<? extends PDatum> targetColumns, ParallelIteratorFactory parallelIteratorFactory, boolean stopAtBestPlan) throws SQLException {
        SelectStatement select = (SelectStatement)dataPlan.getStatement();
        // Exit early if we have a point lookup as we can't get better than that
        if (!useIndexes 
                || (dataPlan.getContext().getScanRanges().isPointLookup() && stopAtBestPlan)) {
            return Collections.singletonList(dataPlan);
        }
        // For single query tuple projection, indexes are inherited from the original table to the projected
        // table; otherwise not. So we pass projected table here, which is enough to tell if this is from a
        // single query or a part of join query.
        List<PTable>indexes = Lists.newArrayList(dataPlan.getContext().getResolver().getTables().get(0).getTable().getIndexes());
        if (indexes.isEmpty() || dataPlan.isDegenerate() || dataPlan.getTableRef().hasDynamicCols() || select.getHint().hasHint(Hint.NO_INDEX)) {
            return Collections.singletonList(dataPlan);
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
            if (stopAtBestPlan) {
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
        
        return hintedPlan == null ? orderPlansBestToWorst(select, plans) : plans;
    }
    
    private static QueryPlan getHintedQueryPlan(PhoenixStatement statement, SelectStatement select, List<PTable> indexes, List<? extends PDatum> targetColumns, ParallelIteratorFactory parallelIteratorFactory, List<QueryPlan> plans) throws SQLException {
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
    
    private static QueryPlan addPlan(PhoenixStatement statement, SelectStatement select, PTable index, List<? extends PDatum> targetColumns, ParallelIteratorFactory parallelIteratorFactory, QueryPlan dataPlan, boolean isHinted) throws SQLException {
        int nColumns = dataPlan.getProjector().getColumnCount();
        String alias = '"' + dataPlan.getTableRef().getTableAlias() + '"'; // double quote in case it's case sensitive
        String schemaName = index.getParentSchemaName().getString();
        schemaName = schemaName.length() == 0 ? null :  '"' + schemaName + '"';

        String tableName = '"' + index.getTableName().getString() + '"';
        TableNode table = FACTORY.namedTable(alias, FACTORY.table(schemaName, tableName));
        SelectStatement indexSelect = FACTORY.select(select, table);
        ColumnResolver resolver = FromCompiler.getResolverForQuery(indexSelect, statement.getConnection());
        // We will or will not do tuple projection according to the data plan.
        boolean isProjected = dataPlan.getContext().getResolver().getTables().get(0).getTable().getType() == PTableType.PROJECTED;
        // Check index state of now potentially updated index table to make sure it's active
        if (PIndexState.ACTIVE.equals(resolver.getTables().get(0).getTable().getIndexState())) {
            try {
            	// translate nodes that match expressions that are indexed to the associated column parse node
                indexSelect = ParseNodeRewriter.rewrite(indexSelect, new  IndexExpressionParseNodeRewriter(index, statement.getConnection()));
                QueryCompiler compiler = new QueryCompiler(statement, indexSelect, resolver, targetColumns, parallelIteratorFactory, dataPlan.getContext().getSequenceManager(), isProjected);
                
                QueryPlan plan = compiler.compile();
                // If query doesn't have where clause and some of columns to project are missing
                // in the index then we need to get missing columns from main table for each row in
                // local index. It's like full scan of both local index and data table which is inefficient.
                // Then we don't use the index. If all the columns to project are present in the index 
                // then we can use the index even the query doesn't have where clause. 
                if (index.getIndexType() == IndexType.LOCAL && indexSelect.getWhere() == null
                        && !plan.getContext().getDataColumns().isEmpty()) {
                    return null;
                }
                // Checking number of columns handles the wildcard cases correctly, as in that case the index
                // must contain all columns from the data table to be able to be used.
                if (plan.getTableRef().getTable().getIndexState() == PIndexState.ACTIVE) {
                    if (plan.getProjector().getColumnCount() == nColumns) {
                        return plan;
                    } else if (index.getIndexType() == IndexType.GLOBAL) {
                        throw new ColumnNotFoundException("*");
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
                        SelectStatement innerSelect = FACTORY.select(indexSelect.getFrom(), indexSelect.getHint(), false, aliasedNodes, where, null, null, null, indexSelect.getLimit(), indexSelect.getBindCount(), false, indexSelect.hasSequence(), Collections.<SelectStatement>emptyList());
                        ParseNode outerWhere = FACTORY.in(nodes.size() == 1 ? nodes.get(0) : FACTORY.rowValueConstructor(nodes), FACTORY.subquery(innerSelect, false), false, true);
                        ParseNode extractedCondition = whereRewriter.getExtractedCondition();
                        if (extractedCondition != null) {
                            outerWhere = FACTORY.and(Lists.newArrayList(outerWhere, extractedCondition));
                        }
                        HintNode hint = HintNode.combine(HintNode.subtract(indexSelect.getHint(), new Hint[] {Hint.INDEX, Hint.NO_CHILD_PARENT_JOIN_OPTIMIZATION}), FACTORY.hint("NO_INDEX"));
                        SelectStatement query = FACTORY.select(dataSelect, hint, outerWhere);
                        ColumnResolver queryResolver = FromCompiler.getResolverForQuery(query, statement.getConnection());
                        query = SubqueryRewriter.transform(query, queryResolver, statement.getConnection());
                        queryResolver = FromCompiler.getResolverForQuery(query, statement.getConnection());
                        query = StatementNormalizer.normalize(query, queryResolver);
                        QueryPlan plan = new QueryCompiler(statement, query, queryResolver, targetColumns, parallelIteratorFactory, dataPlan.getContext().getSequenceManager(), isProjected).compile();
                        return plan;
                    }
                }
            }
        }
        return null;
    }
    
    /**
     * Order the plans among all the possible ones from best to worst.
     * Since we don't keep stats yet, we use the following simple algorithm:
     * 1) If the query is a point lookup (i.e. we have a set of exact row keys), choose among those.
     * 2) If the query has an ORDER BY and a LIMIT, choose the plan that has all the ORDER BY expression
     * in the same order as the row key columns.
     * 3) If there are more than one plan that meets (1&2), choose the plan with:
     *    a) the most row key columns that may be used to form the start/stop scan key.
     *    b) the plan that preserves ordering for a group by.
     *    c) the data table plan
     * @param plans the list of candidate plans
     * @return list of plans ordered from best to worst.
     */
    private List<QueryPlan> orderPlansBestToWorst(SelectStatement select, List<QueryPlan> plans) {
        final QueryPlan dataPlan = plans.get(0);
        if (plans.size() == 1) {
            return plans;
        }
        
        /**
         * If we have a plan(s) that are just point lookups (i.e. fully qualified row
         * keys), then favor those first.
         */
        List<QueryPlan> candidates = Lists.newArrayListWithExpectedSize(plans.size());
        for (QueryPlan plan : plans) {
            if (plan.getContext().getScanRanges().isPointLookup()) {
                candidates.add(plan);
            }
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
        final int comparisonOfDataVersusIndexTable = select.getHint().hasHint(Hint.USE_DATA_OVER_INDEX_TABLE) ? -1 : 1;
        Collections.sort(bestCandidates, new Comparator<QueryPlan>() {

            @Override
            public int compare(QueryPlan plan1, QueryPlan plan2) {
                PTable table1 = plan1.getTableRef().getTable();
                PTable table2 = plan2.getTableRef().getTable();
                // For shared indexes (i.e. indexes on views and local indexes),
                // a) add back any view constants as these won't be in the index, and
                // b) ignore the viewIndexId which will be part of the row key columns.
                int c = (plan2.getContext().getScanRanges().getRanges().size() + (table2.getViewIndexId() == null ? 0 : (boundRanges - 1))) - 
                        (plan1.getContext().getScanRanges().getRanges().size() + (table1.getViewIndexId() == null ? 0 : (boundRanges - 1)));
                if (c != 0) return c;
                if (plan1.getGroupBy()!=null && plan2.getGroupBy()!=null) {
                    if (plan1.getGroupBy().isOrderPreserving() != plan2.getGroupBy().isOrderPreserving()) {
                        return plan1.getGroupBy().isOrderPreserving() ? -1 : 1;
                    } 
                }
                // Use smaller table (table with fewest kv columns)
                c = (table1.getColumns().size() - table1.getPKColumns().size()) - (table2.getColumns().size() - table2.getPKColumns().size());
                if (c != 0) return c;
                
                // If all things are equal, don't choose local index as it forces scan
                // on every region (unless there's no start/stop key)
                if (table1.getIndexType() == IndexType.LOCAL) {
                    return plan1.getContext().getScanRanges().getRanges().isEmpty() ? -1 : 1;
                }
                if (table2.getIndexType() == IndexType.LOCAL) {
                    return plan2.getContext().getScanRanges().getRanges().isEmpty() ? 1 : -1;
                }

                // All things being equal, just use the table based on the Hint.USE_DATA_OVER_INDEX_TABLE
                if (table1.getType() == PTableType.INDEX) {
                    return comparisonOfDataVersusIndexTable;
                }
                if (table2.getType() == PTableType.INDEX) {
                    return -comparisonOfDataVersusIndexTable;
                }
                
                return 0;
            }
            
        });
        
        return bestCandidates;
    }

    
    private static class WhereConditionRewriter extends BooleanParseNodeVisitor<ParseNode> {
        private final ColumnResolver dataResolver;
        private final ExpressionCompiler expressionCompiler;
        private List<ParseNode> extractedConditions;
        
        public WhereConditionRewriter(ColumnResolver dataResolver, StatementContext context) throws SQLException {
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
        public List<ParseNode> newElementList(int size) {
            return Lists.<ParseNode> newArrayListWithExpectedSize(size);
        }

        @Override
        public void addElement(List<ParseNode> l, ParseNode element) {
            if (element != null) {
                l.add(element);
            }
        }

        @Override
        public boolean visitEnter(AndParseNode node) throws SQLException {
            return true;
        }

        @Override
        public ParseNode visitLeave(AndParseNode node, List<ParseNode> l)
                throws SQLException {
            if (l.equals(node.getChildren()))
                return node;

            if (l.isEmpty())
                return null;
            
            if (l.size() == 1)
                return l.get(0);
            
            return FACTORY.and(l);
        }

        @Override
        protected boolean enterBooleanNode(ParseNode node) throws SQLException {
            return false;
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

        @Override
        protected boolean enterNonBooleanNode(ParseNode node)
                throws SQLException {
            return false;
        }

        @Override
        protected ParseNode leaveNonBooleanNode(ParseNode node,
                List<ParseNode> l) throws SQLException {
            return node;
        }
    }
    
}
