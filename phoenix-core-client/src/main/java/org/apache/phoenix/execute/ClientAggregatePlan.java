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

import static org.apache.phoenix.query.QueryConstants.UNGROUPED_AGG_ROW_KEY;
import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.AGGREGATORS;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.compile.ExplainPlanAttributes
    .ExplainPlanAttributesBuilder;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.execute.visitor.AvgRowWidthVisitor;
import org.apache.phoenix.execute.visitor.ByteCountVisitor;
import org.apache.phoenix.execute.visitor.QueryPlanVisitor;
import org.apache.phoenix.execute.visitor.RowCountVisitor;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.OrderByExpression;
import org.apache.phoenix.expression.aggregator.Aggregators;
import org.apache.phoenix.expression.aggregator.ClientAggregators;
import org.apache.phoenix.expression.aggregator.ServerAggregators;
import org.apache.phoenix.iterate.AggregatingResultIterator;
import org.apache.phoenix.iterate.BaseGroupedAggregatingResultIterator;
import org.apache.phoenix.iterate.ClientHashAggregatingResultIterator;
import org.apache.phoenix.iterate.DistinctAggregatingResultIterator;
import org.apache.phoenix.iterate.FilterAggregatingResultIterator;
import org.apache.phoenix.iterate.FilterResultIterator;
import org.apache.phoenix.iterate.GroupedAggregatingResultIterator;
import org.apache.phoenix.iterate.LimitingResultIterator;
import org.apache.phoenix.iterate.LookAheadResultIterator;
import org.apache.phoenix.iterate.OffsetResultIterator;
import org.apache.phoenix.iterate.OrderedAggregatingResultIterator;
import org.apache.phoenix.iterate.OrderedResultIterator;
import org.apache.phoenix.iterate.ParallelScanGrouper;
import org.apache.phoenix.iterate.PeekingResultIterator;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.iterate.SequenceResultIterator;
import org.apache.phoenix.iterate.UngroupedAggregatingResultIterator;
import org.apache.phoenix.optimize.Cost;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.tuple.MultiKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.CostUtil;
import org.apache.phoenix.util.ExpressionUtil;
import org.apache.phoenix.util.TupleUtil;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

public class ClientAggregatePlan extends ClientProcessingPlan {
    private final GroupBy groupBy;
    private final Expression having;
    private final ServerAggregators serverAggregators;
    private final ClientAggregators clientAggregators;
    private final boolean useHashAgg;
    private OrderBy actualOutputOrderBy;
    
    public ClientAggregatePlan(StatementContext context, FilterableStatement statement, TableRef table, RowProjector projector,
            Integer limit, Integer offset, Expression where, OrderBy orderBy, GroupBy groupBy, Expression having, QueryPlan delegate) {
        super(context, statement, table, projector, limit, offset, where, orderBy, delegate);
        this.groupBy = groupBy;
        this.having = having;
        this.clientAggregators = context.getAggregationManager().getAggregators();
        // We must deserialize rather than clone based off of client aggregators because
        // upon deserialization we create the server-side aggregators instead of the client-side
        // aggregators. We use the Configuration directly here to avoid the expense of creating
        // another one.
        this.serverAggregators = ServerAggregators.deserialize(context.getScan()
                        .getAttribute(AGGREGATORS), context.getConnection().getQueryServices().getConfiguration(), null);

        // Extract hash aggregate hint, if any.
        HintNode hints = statement.getHint();
        useHashAgg = hints != null && hints.hasHint(HintNode.Hint.HASH_AGGREGATE);
        this.actualOutputOrderBy = convertActualOutputOrderBy(orderBy, groupBy, context);
    }

    @Override
    public Cost getCost() {
        Double outputBytes = this.accept(new ByteCountVisitor());
        Double inputRows = this.getDelegate().accept(new RowCountVisitor());
        Double rowWidth = this.accept(new AvgRowWidthVisitor());
        if (inputRows == null || outputBytes == null || rowWidth == null) {
            return Cost.UNKNOWN;
        }
        double inputBytes = inputRows * rowWidth;
        double rowsBeforeHaving = RowCountVisitor.aggregate(
                RowCountVisitor.filter(
                        inputRows.doubleValue(),
                        RowCountVisitor.stripSkipScanFilter(
                                context.getScan().getFilter())),
                groupBy);
        double rowsAfterHaving = RowCountVisitor.filter(rowsBeforeHaving, having);
        double bytesBeforeHaving = rowWidth * rowsBeforeHaving;
        double bytesAfterHaving = rowWidth * rowsAfterHaving;

        int parallelLevel = CostUtil.estimateParallelLevel(
                false, context.getConnection().getQueryServices());
        Cost cost = CostUtil.estimateAggregateCost(
                inputBytes, bytesBeforeHaving, groupBy, parallelLevel);
        if (!orderBy.getOrderByExpressions().isEmpty()) {
            Cost orderByCost = CostUtil.estimateOrderByCost(
                    bytesAfterHaving, outputBytes, parallelLevel);
            cost = cost.plus(orderByCost);
        }
        return super.getCost().plus(cost);
    }

    @Override
    public ResultIterator iterator(ParallelScanGrouper scanGrouper, Scan scan) throws SQLException {
        ResultIterator iterator = delegate.iterator(scanGrouper, scan);
        if (where != null) {
            iterator = new FilterResultIterator(iterator, where);
        }
        
        AggregatingResultIterator aggResultIterator;
        if (groupBy.isEmpty()) {
            aggResultIterator = new ClientUngroupedAggregatingResultIterator(LookAheadResultIterator.wrap(iterator), serverAggregators);
            aggResultIterator = new UngroupedAggregatingResultIterator(LookAheadResultIterator.wrap(aggResultIterator), clientAggregators);
        } else {
            List<Expression> keyExpressions = groupBy.getKeyExpressions();
            if (groupBy.isOrderPreserving()) {
                aggResultIterator = new ClientGroupedAggregatingResultIterator(LookAheadResultIterator.wrap(iterator), serverAggregators, keyExpressions);
            } else {
                long thresholdBytes =
                        context.getConnection().getQueryServices().getProps().getLongBytes(
                            QueryServices.CLIENT_SPOOL_THRESHOLD_BYTES_ATTRIB,
                            QueryServicesOptions.DEFAULT_CLIENT_SPOOL_THRESHOLD_BYTES);
                boolean spoolingEnabled =
                        context.getConnection().getQueryServices().getProps().getBoolean(
                            QueryServices.CLIENT_ORDERBY_SPOOLING_ENABLED_ATTRIB,
                            QueryServicesOptions.DEFAULT_CLIENT_ORDERBY_SPOOLING_ENABLED);
                List<OrderByExpression> keyExpressionOrderBy = Lists.newArrayListWithExpectedSize(keyExpressions.size());
                for (Expression keyExpression : keyExpressions) {
                    /**
                     * Sort the result tuples by the GroupBy expressions.
                     * If some GroupBy expression is SortOrder.DESC, then sorted results on that expression are DESC, not ASC.
                     * for ClientAggregatePlan,the orderBy should not be OrderBy.REV_ROW_KEY_ORDER_BY, which is different from {@link AggregatePlan.OrderingResultIteratorFactory#newIterator}
                     **/
                    keyExpressionOrderBy.add(OrderByExpression.createByCheckIfOrderByReverse(keyExpression, false, true, false));
                }

                if (useHashAgg) {
                    // Pass in orderBy to apply any sort that has been optimized away
                    aggResultIterator = new ClientHashAggregatingResultIterator(context, iterator, serverAggregators, keyExpressions, orderBy);
                } else {
                    iterator =
                            new OrderedResultIterator(iterator, keyExpressionOrderBy,
                                    spoolingEnabled, thresholdBytes, null, null,
                                    projector.getEstimatedRowByteSize());
                    aggResultIterator = new ClientGroupedAggregatingResultIterator(LookAheadResultIterator.wrap(iterator), serverAggregators, keyExpressions);
                }
            }
            aggResultIterator = new GroupedAggregatingResultIterator(LookAheadResultIterator.wrap(aggResultIterator), clientAggregators);
        }

        if (having != null) {
            aggResultIterator = new FilterAggregatingResultIterator(aggResultIterator, having);
        }
        
        if (statement.isDistinct() && statement.isAggregate()) { // Dedup on client if select distinct and aggregation
            aggResultIterator = new DistinctAggregatingResultIterator(aggResultIterator, getProjector());
        }

        ResultIterator resultScanner = aggResultIterator;
        if (orderBy.getOrderByExpressions().isEmpty()) {
            if (offset != null) {
                resultScanner = new OffsetResultIterator(resultScanner, offset);
            }
            if (limit != null) {
                resultScanner = new LimitingResultIterator(resultScanner, limit);
            }
        } else {
            long thresholdBytes =
                    context.getConnection().getQueryServices().getProps().getLongBytes(
                        QueryServices.CLIENT_SPOOL_THRESHOLD_BYTES_ATTRIB,
                        QueryServicesOptions.DEFAULT_CLIENT_SPOOL_THRESHOLD_BYTES);
            boolean spoolingEnabled =
                    context.getConnection().getQueryServices().getProps().getBoolean(
                        QueryServices.CLIENT_ORDERBY_SPOOLING_ENABLED_ATTRIB,
                        QueryServicesOptions.DEFAULT_CLIENT_ORDERBY_SPOOLING_ENABLED);
            resultScanner =
                    new OrderedAggregatingResultIterator(aggResultIterator,
                            orderBy.getOrderByExpressions(), spoolingEnabled, thresholdBytes, limit,
                            offset);
        }
        if (context.getSequenceManager().getSequenceCount() > 0) {
            resultScanner = new SequenceResultIterator(resultScanner, context.getSequenceManager());
        }
        
        return resultScanner;
    }

    @Override
    public ExplainPlan getExplainPlan() throws SQLException {
        ExplainPlan explainPlan = delegate.getExplainPlan();
        List<String> planSteps = Lists.newArrayList(explainPlan.getPlanSteps());
        ExplainPlanAttributes explainPlanAttributes =
            explainPlan.getPlanStepsAsAttributes();
        ExplainPlanAttributesBuilder newBuilder =
            new ExplainPlanAttributesBuilder(explainPlanAttributes);
        if (where != null) {
            planSteps.add("CLIENT FILTER BY " + where.toString());
            newBuilder.setClientFilterBy(where.toString());
        }
        if (groupBy.isEmpty()) {
            planSteps.add("CLIENT AGGREGATE INTO SINGLE ROW");
            newBuilder.setClientAggregate("CLIENT AGGREGATE INTO SINGLE ROW");
        } else if (groupBy.isOrderPreserving()) {
            planSteps.add("CLIENT AGGREGATE INTO ORDERED DISTINCT ROWS BY "
                + groupBy.getExpressions().toString());
            newBuilder.setClientAggregate("CLIENT AGGREGATE INTO ORDERED DISTINCT ROWS BY "
                + groupBy.getExpressions().toString());
        } else if (useHashAgg) {
            planSteps.add("CLIENT HASH AGGREGATE INTO DISTINCT ROWS BY " + groupBy.getExpressions().toString());
            newBuilder.setClientAggregate("CLIENT HASH AGGREGATE INTO DISTINCT ROWS BY "
                + groupBy.getExpressions().toString());
            if (orderBy == OrderBy.FWD_ROW_KEY_ORDER_BY || orderBy == OrderBy.REV_ROW_KEY_ORDER_BY) {
                planSteps.add("CLIENT SORTED BY " + groupBy.getKeyExpressions().toString());
                newBuilder.setClientSortedBy(
                    groupBy.getKeyExpressions().toString());
            }
        } else {
            planSteps.add("CLIENT SORTED BY " + groupBy.getKeyExpressions().toString());
            planSteps.add("CLIENT AGGREGATE INTO DISTINCT ROWS BY " + groupBy.getExpressions().toString());
            newBuilder.setClientSortedBy(groupBy.getKeyExpressions().toString());
            newBuilder.setClientAggregate("CLIENT AGGREGATE INTO DISTINCT ROWS BY "
                + groupBy.getExpressions().toString());
        }
        if (having != null) {
            planSteps.add("CLIENT AFTER-AGGREGATION FILTER BY " + having.toString());
            newBuilder.setClientAfterAggregate("CLIENT AFTER-AGGREGATION FILTER BY "
                + having.toString());
        }
        if (statement.isDistinct() && statement.isAggregate()) {
            planSteps.add("CLIENT DISTINCT ON " + projector.toString());
            newBuilder.setClientDistinctFilter(projector.toString());
        }
        if (offset != null) {
            planSteps.add("CLIENT OFFSET " + offset);
            newBuilder.setClientOffset(offset);
        }
        if (orderBy.getOrderByExpressions().isEmpty()) {
            if (limit != null) {
                planSteps.add("CLIENT " + limit + " ROW LIMIT");
                newBuilder.setClientRowLimit(limit);
            }
        } else {
            planSteps.add("CLIENT" + (limit == null ? "" : " TOP " + limit + " ROW"  + (limit == 1 ? "" : "S"))  + " SORTED BY " + orderBy.getOrderByExpressions().toString());
            newBuilder.setClientRowLimit(limit);
            newBuilder.setClientSortedBy(
                orderBy.getOrderByExpressions().toString());
        }
        if (context.getSequenceManager().getSequenceCount() > 0) {
            int nSequences = context.getSequenceManager().getSequenceCount();
            planSteps.add("CLIENT RESERVE VALUES FROM " + nSequences + " SEQUENCE" + (nSequences == 1 ? "" : "S"));
            newBuilder.setClientSequenceCount(nSequences);
        }

        return new ExplainPlan(planSteps, newBuilder.build());
    }

    @Override
    public GroupBy getGroupBy() {
        return groupBy;
    }

    @Override
    public <T> T accept(QueryPlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public Expression getHaving() {
        return having;
    }

    private static class ClientGroupedAggregatingResultIterator extends BaseGroupedAggregatingResultIterator {
        private final List<Expression> groupByExpressions;

        public ClientGroupedAggregatingResultIterator(PeekingResultIterator iterator, Aggregators aggregators, List<Expression> groupByExpressions) {
            super(iterator, aggregators);
            this.groupByExpressions = groupByExpressions;
        }

        @Override
        protected ImmutableBytesWritable getGroupingKey(Tuple tuple,
                ImmutableBytesWritable ptr) throws SQLException {
            try {
                ImmutableBytesWritable key = TupleUtil.getConcatenatedValue(tuple, groupByExpressions);
                ptr.set(key.get(), key.getOffset(), key.getLength());
                return ptr;
            } catch (IOException e) {
                throw new SQLException(e);
            }
        }

        @Override
        protected Tuple wrapKeyValueAsResult(Cell keyValue) {
            return new MultiKeyValueTuple(Collections.<Cell> singletonList(keyValue));
        }

        @Override
        public String toString() {
            return "ClientGroupedAggregatingResultIterator [resultIterator=" 
                    + resultIterator + ", aggregators=" + aggregators + ", groupByExpressions="
                    + groupByExpressions + "]";
        }
    }

    private static class ClientUngroupedAggregatingResultIterator extends BaseGroupedAggregatingResultIterator {

        public ClientUngroupedAggregatingResultIterator(PeekingResultIterator iterator, Aggregators aggregators) {
            super(iterator, aggregators);
        }

        @Override
        protected ImmutableBytesWritable getGroupingKey(Tuple tuple,
                ImmutableBytesWritable ptr) throws SQLException {
            ptr.set(UNGROUPED_AGG_ROW_KEY);
            return ptr;
        }

        @Override
        protected Tuple wrapKeyValueAsResult(Cell keyValue)
                throws SQLException {
            return new MultiKeyValueTuple(Collections.<Cell> singletonList(keyValue));
        }

        @Override
        public String toString() {
            return "ClientUngroupedAggregatingResultIterator [resultIterator=" 
                    + resultIterator + ", aggregators=" + aggregators + "]";
        }
    }

    private OrderBy convertActualOutputOrderBy(OrderBy orderBy, GroupBy groupBy, StatementContext statementContext) {
        if(!orderBy.isEmpty()) {
            return OrderBy.convertCompiledOrderByToOutputOrderBy(orderBy);
        }

        if(this.useHashAgg &&
           !groupBy.isEmpty() &&
           !groupBy.isOrderPreserving() &&
           orderBy != OrderBy.FWD_ROW_KEY_ORDER_BY &&
           orderBy != OrderBy.REV_ROW_KEY_ORDER_BY) {
            return OrderBy.EMPTY_ORDER_BY;
        }

        return ExpressionUtil.convertGroupByToOrderBy(groupBy, orderBy == OrderBy.REV_ROW_KEY_ORDER_BY);
    }

    @Override
    public List<OrderBy> getOutputOrderBys() {
       return OrderBy.wrapForOutputOrderBys(this.actualOutputOrderBy);
    }
}
