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
package org.apache.phoenix.execute;

import static org.apache.phoenix.util.NumberUtil.add;
import static org.apache.phoenix.util.NumberUtil.getMin;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes
    .ExplainPlanAttributesBuilder;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.execute.visitor.QueryPlanVisitor;
import org.apache.phoenix.expression.OrderByExpression;
import org.apache.phoenix.iterate.ConcatResultIterator;
import org.apache.phoenix.iterate.DefaultParallelScanGrouper;
import org.apache.phoenix.iterate.LimitingResultIterator;
import org.apache.phoenix.iterate.MergeSortTopNResultIterator;
import org.apache.phoenix.iterate.OffsetResultIterator;
import org.apache.phoenix.iterate.ParallelScanGrouper;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.iterate.UnionResultIterators;
import org.apache.phoenix.jdbc.PhoenixStatement.Operation;
import org.apache.phoenix.optimize.Cost;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.TableRef;

import org.apache.phoenix.thirdparty.com.google.common.collect.Sets;


public class UnionPlan implements QueryPlan {
    private static final long DEFAULT_ESTIMATED_SIZE = 10 * 1024; // 10 K

    private final TableRef tableRef;
    private final FilterableStatement statement;
    private final ParameterMetaData paramMetaData;
    private final OrderBy orderBy;
    private final StatementContext parentContext;
    private final Integer limit;
    private final Integer offset;
    private final GroupBy groupBy;
    private final RowProjector projector;
    private final boolean isDegenerate;
    private final List<QueryPlan> plans;
    private UnionResultIterators iterators;
    private Long estimatedRows;
    private Long estimatedBytes;
    private Long estimateInfoTs;
    private boolean getEstimatesCalled;
    private boolean supportOrderByOptimize = false;
    private List<OrderBy> outputOrderBys = null;

    public UnionPlan(StatementContext context, FilterableStatement statement, TableRef table, RowProjector projector,
            Integer limit, Integer offset, OrderBy orderBy, GroupBy groupBy, List<QueryPlan> plans, ParameterMetaData paramMetaData) throws SQLException {
        this.parentContext = context;
        this.statement = statement;
        this.tableRef = table;
        this.projector = projector;
        this.limit = limit;
        this.orderBy = orderBy;
        this.groupBy = groupBy;
        this.plans = plans;
        this.offset= offset;
        this.paramMetaData = paramMetaData;
        boolean isDegen = true;
        for (QueryPlan plan : plans) {           
            if (plan.getContext().getScanRanges() != ScanRanges.NOTHING) {
                isDegen = false;
                break;
            }
        }
        this.isDegenerate = isDegen;
    }

    /**
     * If every subquery in {@link UnionPlan} is ordered, and {@link QueryPlan#getOutputOrderBys}
     * of each subquery are equal(absolute equality or the same column name is unnecessary, just
     * column types are compatible and columns count is same), then it just needs to perform a
     * simple merge on the subquery results to ensure the overall order of the union all, see
     * comments on {@link QueryCompiler#optimizeUnionOrderByIfPossible}.
     */
    private boolean checkIfSupportOrderByOptimize() {
        if (!this.orderBy.isEmpty()) {
            return false;
        }
        if (plans.isEmpty()) {
            return false;
        }
        OrderBy prevOrderBy = null;
        for (QueryPlan queryPlan : plans) {
            List<OrderBy> orderBys = queryPlan.getOutputOrderBys();
            if (orderBys.isEmpty() || orderBys.size() > 1) {
                return false;
            }
            OrderBy orderBy = orderBys.get(0);
            if (prevOrderBy != null && !OrderBy.equalsForOutputOrderBy(prevOrderBy, orderBy)) {
                return false;
            }
            prevOrderBy = orderBy;
        }
        return true;
    }

    public boolean isSupportOrderByOptimize() {
        return this.supportOrderByOptimize;
    }

    public void enableCheckSupportOrderByOptimize() {
        this.supportOrderByOptimize = checkIfSupportOrderByOptimize();
        this.outputOrderBys = null;
    }

    public void disableSupportOrderByOptimize() {
        if (!this.supportOrderByOptimize) {
            return;
        }
        this.outputOrderBys = null;
        this.supportOrderByOptimize = false;
    }

    @Override
    public boolean isDegenerate() {
        return isDegenerate;
    }

    @Override
    public List<KeyRange> getSplits() {
        if (iterators == null)
            return null;
        return iterators.getSplits();
    }

    @Override
    public List<List<Scan>> getScans() {
        if (iterators == null)
            return null;
        return iterators.getScans();
    }

    public List<QueryPlan> getSubPlans() {
        return plans;
    }

    @Override
    public GroupBy getGroupBy() {
        return groupBy;
    }

    @Override
    public OrderBy getOrderBy() {
        return orderBy;
    }

    @Override
    public TableRef getTableRef() {
        return tableRef;
    }

    @Override
    public Integer getLimit() {
        return limit;
    }

    @Override
    public Integer getOffset() {
        return offset;
    }

    @Override
    public RowProjector getProjector() {
        return projector;
    }
    
    @Override
    public ResultIterator iterator() throws SQLException {
        return iterator(DefaultParallelScanGrouper.getInstance());
    }

    @Override
    public ResultIterator iterator(ParallelScanGrouper scanGrouper) throws SQLException {
        return iterator(scanGrouper, null);
    }

    @Override
    public final ResultIterator iterator(ParallelScanGrouper scanGrouper, Scan scan) throws SQLException {
        this.iterators = new UnionResultIterators(plans, parentContext);
        ResultIterator scanner;      

        if (!orderBy.isEmpty()) { // TopN
            scanner = new MergeSortTopNResultIterator(iterators, limit, offset, orderBy.getOrderByExpressions());
        } else if (this.supportOrderByOptimize) {
            //Every subquery is ordered
            scanner = new MergeSortTopNResultIterator(
                    iterators, limit, offset, getOrderByExpressionsWhenSupportOrderByOptimize());
        } else {
            scanner = new ConcatResultIterator(iterators);
            if (offset != null) {
                scanner = new OffsetResultIterator(scanner, offset);
            }
            if (limit != null) {
                scanner = new LimitingResultIterator(scanner, limit);
            }          
        }
        return scanner;
    }

    @Override
    public ExplainPlan getExplainPlan() throws SQLException {
        List<String> steps = new ArrayList<String>();
        ExplainPlanAttributesBuilder builder = new ExplainPlanAttributesBuilder();
        String abstractExplainPlan = "UNION ALL OVER " + this.plans.size()
            + " QUERIES";
        builder.setAbstractExplainPlan(abstractExplainPlan);
        steps.add(abstractExplainPlan);
        ResultIterator iterator = iterator();
        iterator.explain(steps, builder);
        // Indent plans steps nested under union, except last client-side merge/concat step (if there is one)
        int offset = !orderBy.getOrderByExpressions().isEmpty() && limit != null ? 2 : limit != null ? 1 : 0;
        for (int i = 1 ; i < steps.size()-offset; i++) {
            steps.set(i, "    " + steps.get(i));
        }
        return new ExplainPlan(steps, builder.build());
    }


    @Override
    public long getEstimatedSize() {
        return DEFAULT_ESTIMATED_SIZE;
    }

    @Override
    public Cost getCost() {
        Cost cost = Cost.ZERO;
        for (QueryPlan plan : plans) {
            cost = cost.plus(plan.getCost());
        }
        return cost;
    }

    @Override
    public ParameterMetaData getParameterMetaData() {
        return paramMetaData;
    }

    @Override
    public FilterableStatement getStatement() {
        return statement;
    }

    @Override
    public StatementContext getContext() {
        return parentContext;
    }

    @Override
    public boolean isRowKeyOrdered() {
        return groupBy.isEmpty() ? orderBy.getOrderByExpressions().isEmpty() : groupBy.isOrderPreserving();
    }

    public List<QueryPlan> getPlans() {
        return this.plans;
    }

    @Override
    public boolean useRoundRobinIterator() throws SQLException {
        return false;
    }

    @Override
    public <T> T accept(QueryPlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
	public Operation getOperation() {
		return statement.getOperation();
	}

	@Override
	public Set<TableRef> getSourceRefs() {
		// TODO is this correct?
		Set<TableRef> sources = Sets.newHashSetWithExpectedSize(plans.size());
		for (QueryPlan plan : plans) {
			sources.addAll(plan.getSourceRefs());
		}
		return sources;
	}

    @Override
    public Long getEstimatedRowsToScan() throws SQLException {
        if (!getEstimatesCalled) {
            getEstimates();
        }
        return estimatedRows;
    }

    @Override
    public Long getEstimatedBytesToScan() throws SQLException {
        if (!getEstimatesCalled) {
            getEstimates();
        }
        return estimatedBytes;
    }

    @Override
    public Long getEstimateInfoTimestamp() throws SQLException {
        if (!getEstimatesCalled) {
            getEstimates();
        }
        return estimateInfoTs;
    }

    private void getEstimates() throws SQLException {
        getEstimatesCalled = true;
        for (QueryPlan plan : plans) {
            if (plan.getEstimatedBytesToScan() == null || plan.getEstimatedRowsToScan() == null
                    || plan.getEstimateInfoTimestamp() == null) {
                /*
                 * If any of the sub plans doesn't have the estimate info available, then we don't
                 * provide estimate for the overall plan
                 */
                estimatedBytes = null;
                estimatedRows = null;
                estimateInfoTs = null;
                break;
            } else {
                estimatedBytes = add(estimatedBytes, plan.getEstimatedBytesToScan());
                estimatedRows = add(estimatedRows, plan.getEstimatedRowsToScan());
                estimateInfoTs = getMin(estimateInfoTs, plan.getEstimateInfoTimestamp());
            }
        }
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(
            value = "EI_EXPOSE_REP",
            justification = "getOutputOrderBys designed to work this way.")
    @Override
    public List<OrderBy> getOutputOrderBys() {
        if (this.outputOrderBys != null) {
            return this.outputOrderBys;
        }
        return this.outputOrderBys = convertToOutputOrderBys();
    }

    private List<OrderBy> convertToOutputOrderBys() {
        assert this.groupBy == GroupBy.EMPTY_GROUP_BY;
        assert this.orderBy != OrderBy.FWD_ROW_KEY_ORDER_BY && this.orderBy != OrderBy.REV_ROW_KEY_ORDER_BY;
        if(!this.orderBy.isEmpty()) {
            return Collections.<OrderBy> singletonList(
                    OrderBy.convertCompiledOrderByToOutputOrderBy(this.orderBy));
        }
        if (this.supportOrderByOptimize) {
            assert this.plans.size() > 0;
            return this.plans.get(0).getOutputOrderBys();
        }
        return Collections.<OrderBy> emptyList();
    }

    private List<OrderByExpression> getOrderByExpressionsWhenSupportOrderByOptimize() {
        assert this.supportOrderByOptimize;
        assert this.plans.size() > 0;
        assert this.orderBy.isEmpty();
        List<OrderBy> outputOrderBys = this.plans.get(0).getOutputOrderBys();
        assert outputOrderBys != null && outputOrderBys.size() == 1;
        List<OrderByExpression> orderByExpressions = outputOrderBys.get(0).getOrderByExpressions();
        assert orderByExpressions != null && orderByExpressions.size() > 0;
        return orderByExpressions.stream().map(OrderByExpression::convertIfExpressionSortOrderDesc)
            .collect(Collectors.toList());
    }

    @Override
    public boolean isApplicable() {
        return true;
    }
}
