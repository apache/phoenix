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


import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.cache.ServerCacheClient.ServerCache;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.coprocessor.GroupedAggregateRegionObserver;
import org.apache.phoenix.coprocessor.UngroupedAggregateRegionObserver;
import org.apache.phoenix.execute.visitor.AvgRowWidthVisitor;
import org.apache.phoenix.execute.visitor.ByteCountVisitor;
import org.apache.phoenix.execute.visitor.QueryPlanVisitor;
import org.apache.phoenix.execute.visitor.RowCountVisitor;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.OrderByExpression;
import org.apache.phoenix.expression.RowKeyExpression;
import org.apache.phoenix.expression.aggregator.Aggregators;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.iterate.AggregatingResultIterator;
import org.apache.phoenix.iterate.BaseResultIterators;
import org.apache.phoenix.iterate.ConcatResultIterator;
import org.apache.phoenix.iterate.DistinctAggregatingResultIterator;
import org.apache.phoenix.iterate.FilterAggregatingResultIterator;
import org.apache.phoenix.iterate.GroupedAggregatingResultIterator;
import org.apache.phoenix.iterate.LimitingResultIterator;
import org.apache.phoenix.iterate.MergeSortRowKeyResultIterator;
import org.apache.phoenix.iterate.OffsetResultIterator;
import org.apache.phoenix.iterate.OrderedAggregatingResultIterator;
import org.apache.phoenix.iterate.OrderedResultIterator;
import org.apache.phoenix.iterate.ParallelIteratorFactory;
import org.apache.phoenix.iterate.ParallelIterators;
import org.apache.phoenix.iterate.ParallelScanGrouper;
import org.apache.phoenix.iterate.PeekingResultIterator;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.iterate.RowKeyOrderedAggregateResultIterator;
import org.apache.phoenix.iterate.SequenceResultIterator;
import org.apache.phoenix.iterate.SerialIterators;
import org.apache.phoenix.iterate.SpoolingResultIterator;
import org.apache.phoenix.iterate.UngroupedAggregatingResultIterator;
import org.apache.phoenix.optimize.Cost;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.CostUtil;
import org.apache.phoenix.util.ExpressionUtil;
import org.apache.phoenix.util.ScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Query plan for aggregating queries
 *
 * 
 * @since 0.1
 */
public class AggregatePlan extends BaseQueryPlan {
    private final Aggregators aggregators;
    private final Expression having;
    private List<KeyRange> splits;
    private List<List<Scan>> scans;
    private static final Logger LOGGER = LoggerFactory.getLogger(AggregatePlan.class);
    private boolean isSerial;
    private OrderBy actualOutputOrderBy;

    public AggregatePlan(StatementContext context, FilterableStatement statement, TableRef table,
            RowProjector projector, Integer limit, Integer offset, OrderBy orderBy,
            ParallelIteratorFactory parallelIteratorFactory, GroupBy groupBy, Expression having, QueryPlan dataPlan) throws SQLException {
        this(context, statement, table, projector, limit, offset, orderBy, parallelIteratorFactory, groupBy, having,
                null, dataPlan);
    }

    private AggregatePlan(StatementContext context, FilterableStatement statement, TableRef table,
            RowProjector projector, Integer limit, Integer offset, OrderBy orderBy,
            ParallelIteratorFactory parallelIteratorFactory, GroupBy groupBy, Expression having,
            Expression dynamicFilter, QueryPlan dataPlan) throws SQLException {
        super(context, statement, table, projector, context.getBindManager().getParameterMetaData(), limit, offset,
                orderBy, groupBy, parallelIteratorFactory, dynamicFilter, dataPlan);
        this.having = having;
        this.aggregators = context.getAggregationManager().getAggregators();
        boolean hasSerialHint = statement.getHint().hasHint(HintNode.Hint.SERIAL);
        boolean canBeExecutedSerially = ScanUtil.canQueryBeExecutedSerially(table.getTable(), orderBy, context); 
        if (hasSerialHint && !canBeExecutedSerially) {
            LOGGER.warn("This query cannot be executed serially. Ignoring the hint");
        }
        this.isSerial = hasSerialHint && canBeExecutedSerially;
        this.actualOutputOrderBy = convertActualOutputOrderBy(orderBy, groupBy, context);
    }

    public Expression getHaving() {
        return having;
    }

    @Override
    public Cost getCost() {
        Double outputBytes = this.accept(new ByteCountVisitor());
        Double rowWidth = this.accept(new AvgRowWidthVisitor());
        Long inputRows = null;
        try {
            inputRows = getEstimatedRowsToScan();
        } catch (SQLException e) {
            // ignored.
        }
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
                true, context.getConnection().getQueryServices());
        Cost cost = new Cost(0, 0, inputBytes);
        Cost aggCost = CostUtil.estimateAggregateCost(
                inputBytes, bytesBeforeHaving, groupBy, parallelLevel);
        cost = cost.plus(aggCost);
        if (!orderBy.getOrderByExpressions().isEmpty()) {
            parallelLevel = CostUtil.estimateParallelLevel(
                    false, context.getConnection().getQueryServices());
            Cost orderByCost = CostUtil.estimateOrderByCost(
                    bytesAfterHaving, outputBytes, parallelLevel);
            cost = cost.plus(orderByCost);
        }
        return cost;
    }

    @Override
    public List<KeyRange> getSplits() {
        if (splits == null)
            return Collections.emptyList();
        else
            return splits;
    }

    @Override
    public List<List<Scan>> getScans() {
        if (scans == null)
            return Collections.emptyList();
        else
            return scans;
    }

    private static class OrderingResultIteratorFactory implements ParallelIteratorFactory {
        private final QueryServices services;
        private final OrderBy orderBy;
        
        public OrderingResultIteratorFactory(QueryServices services,OrderBy orderBy) {
            this.services = services;
            this.orderBy=orderBy;
        }
        @Override
        public PeekingResultIterator newIterator(StatementContext context, ResultIterator scanner, Scan scan, String tableName, QueryPlan plan) throws SQLException {
            /**
             * Sort the result tuples by the GroupBy expressions.
             * When orderByReverse is false,if some GroupBy expression is SortOrder.DESC, then sorted results on that expression are DESC, not ASC.
             * When orderByReverse is true,if some GroupBy expression is SortOrder.DESC, then sorted results on that expression are ASC, not DESC.
             */
            OrderByExpression orderByExpression =
                    OrderByExpression.createByCheckIfOrderByReverse(
                            RowKeyExpression.INSTANCE,
                            false,
                            true,
                            this.orderBy == OrderBy.REV_ROW_KEY_ORDER_BY);
            long threshold =
                    services.getProps().getLong(QueryServices.CLIENT_SPOOL_THRESHOLD_BYTES_ATTRIB,
                            QueryServicesOptions.DEFAULT_CLIENT_SPOOL_THRESHOLD_BYTES);
            boolean spoolingEnabled =
                    services.getProps().getBoolean(
                            QueryServices.CLIENT_ORDERBY_SPOOLING_ENABLED_ATTRIB,
                            QueryServicesOptions.DEFAULT_CLIENT_ORDERBY_SPOOLING_ENABLED);
            return new OrderedResultIterator(scanner,
                    Collections.<OrderByExpression> singletonList(orderByExpression),
                    spoolingEnabled, threshold);
        }
    }

    private static class WrappingResultIteratorFactory implements ParallelIteratorFactory {
        private final ParallelIteratorFactory innerFactory;
        private final ParallelIteratorFactory outerFactory;
        
        public WrappingResultIteratorFactory(ParallelIteratorFactory innerFactory, ParallelIteratorFactory outerFactory) {
            this.innerFactory = innerFactory;
            this.outerFactory = outerFactory;
        }
        @Override
        public PeekingResultIterator newIterator(StatementContext context, ResultIterator scanner, Scan scan, String tableName, QueryPlan plan) throws SQLException {
            PeekingResultIterator iterator = innerFactory.newIterator(context, scanner, scan, tableName, plan);
            return outerFactory.newIterator(context, iterator, scan, tableName, plan);
        }
    }

    private ParallelIteratorFactory wrapParallelIteratorFactory () {
        ParallelIteratorFactory innerFactory;
        QueryServices services = context.getConnection().getQueryServices();
        if (groupBy.isEmpty() || groupBy.isOrderPreserving()) {
            if (ScanUtil.isPacingScannersPossible(context)) {
                innerFactory = ParallelIteratorFactory.NOOP_FACTORY;
            } else {
                innerFactory = new SpoolingResultIterator.SpoolingResultIteratorFactory(services);
            }
        } else {
            innerFactory = new OrderingResultIteratorFactory(services,this.getOrderBy());
        }
        if (parallelIteratorFactory == null) {
            return innerFactory;
        }
        // wrap any existing parallelIteratorFactory
        return new WrappingResultIteratorFactory(innerFactory, parallelIteratorFactory);
    }
    
    @Override
    protected ResultIterator newIterator(ParallelScanGrouper scanGrouper, Scan scan, Map<ImmutableBytesPtr,ServerCache> caches) throws SQLException {
        if (groupBy.isEmpty()) {
            UngroupedAggregateRegionObserver.serializeIntoScan(scan);
        } else {
            // Set attribute with serialized expressions for coprocessor
            GroupedAggregateRegionObserver.serializeIntoScan(scan, groupBy.getScanAttribName(), groupBy.getKeyExpressions());
            if (limit != null && orderBy.getOrderByExpressions().isEmpty() && having == null
                    && (  (   statement.isDistinct() && ! statement.isAggregate() )
                            || ( ! statement.isDistinct() && (   context.getAggregationManager().isEmpty()
                                                              || BaseScannerRegionObserver.KEY_ORDERED_GROUP_BY_EXPRESSIONS.equals(groupBy.getScanAttribName()) ) ) ) ) {
                /*
                 * Optimization to early exit from the scan for a GROUP BY or DISTINCT with a LIMIT.
                 * We may exit early according to the LIMIT specified if the query has:
                 * 1) No ORDER BY clause (or the ORDER BY was optimized out). We cannot exit
                 *    early if there's an ORDER BY because the first group may be found last
                 *    in the scan.
                 * 2) No HAVING clause, since we execute the HAVING on the client side. The LIMIT
                 *    needs to be evaluated *after* the HAVING.
                 * 3) DISTINCT clause with no GROUP BY. We cannot exit early if there's a
                 *    GROUP BY, as the GROUP BY is processed on the client-side post aggregation
                 *    if a DISTNCT has a GROUP BY. Otherwise, since there are no aggregate
                 *    functions in a DISTINCT, we can exit early regardless of if the
                 *    groups are in row key order or unordered.
                 * 4) GROUP BY clause with no aggregate functions. This is in the same category
                 *    as (3). If we're using aggregate functions, we need to look at all the
                 *    rows, as otherwise we'd exit early with incorrect aggregate function
                 *    calculations.
                 * 5) GROUP BY clause along the pk axis, as the rows are processed in row key
                 *    order, so we can early exit, even when aggregate functions are used, as
                 *    the rows in the group are contiguous.
                 */
                scan.setAttribute(BaseScannerRegionObserver.GROUP_BY_LIMIT,
                        PInteger.INSTANCE.toBytes(limit + (offset == null ? 0 : offset)));
            }
        }
        BaseResultIterators iterators = isSerial
                ? new SerialIterators(this, null, null, wrapParallelIteratorFactory(), scanGrouper, scan, caches, dataPlan)
                : new ParallelIterators(this, null, wrapParallelIteratorFactory(), scan, false, caches, dataPlan);
        estimatedRows = iterators.getEstimatedRowCount();
        estimatedSize = iterators.getEstimatedByteCount();
        estimateInfoTimestamp = iterators.getEstimateInfoTimestamp();
        splits = iterators.getSplits();
        scans = iterators.getScans();

        AggregatingResultIterator aggResultIterator;
        // No need to merge sort for ungrouped aggregation
        if (groupBy.isEmpty() || groupBy.isUngroupedAggregate()) {
            aggResultIterator = new UngroupedAggregatingResultIterator(new ConcatResultIterator(iterators), aggregators);
        // If salted or local index we still need a merge sort as we'll potentially have multiple group by keys that aren't contiguous.
        } else if (groupBy.isOrderPreserving() && !(this.getTableRef().getTable().getBucketNum() != null || this.getTableRef().getTable().getIndexType() == IndexType.LOCAL)) {
            aggResultIterator = new RowKeyOrderedAggregateResultIterator(iterators, aggregators);
        } else {
            aggResultIterator = new GroupedAggregatingResultIterator(
                    new MergeSortRowKeyResultIterator(iterators, 0, this.getOrderBy() == OrderBy.REV_ROW_KEY_ORDER_BY),aggregators);
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
                resultScanner = new OffsetResultIterator(aggResultIterator, offset);
            }
            if (limit != null) {
                resultScanner = new LimitingResultIterator(resultScanner, limit);
            }
        } else {
            long thresholdBytes =
                    context.getConnection().getQueryServices().getProps().getLong(
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
    public boolean useRoundRobinIterator() throws SQLException {
        return false;
    }

    @Override
    public <T> T accept(QueryPlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    private static OrderBy convertActualOutputOrderBy(OrderBy orderBy, GroupBy groupBy, StatementContext statementContext) {
        if(!orderBy.isEmpty()) {
            return OrderBy.convertCompiledOrderByToOutputOrderBy(orderBy);
        }
        return ExpressionUtil.convertGroupByToOrderBy(groupBy, orderBy == OrderBy.REV_ROW_KEY_ORDER_BY);
    }

    @Override
    public List<OrderBy> getOutputOrderBys() {
       return OrderBy.wrapForOutputOrderBys(this.actualOutputOrderBy);
    }
}
