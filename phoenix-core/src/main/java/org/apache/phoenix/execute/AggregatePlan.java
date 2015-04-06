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

import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.coprocessor.GroupedAggregateRegionObserver;
import org.apache.phoenix.coprocessor.UngroupedAggregateRegionObserver;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.OrderByExpression;
import org.apache.phoenix.expression.RowKeyExpression;
import org.apache.phoenix.expression.aggregator.Aggregators;
import org.apache.phoenix.iterate.AggregatingResultIterator;
import org.apache.phoenix.iterate.ConcatResultIterator;
import org.apache.phoenix.iterate.DistinctAggregatingResultIterator;
import org.apache.phoenix.iterate.FilterAggregatingResultIterator;
import org.apache.phoenix.iterate.GroupedAggregatingResultIterator;
import org.apache.phoenix.iterate.LimitingResultIterator;
import org.apache.phoenix.iterate.MergeSortRowKeyResultIterator;
import org.apache.phoenix.iterate.OrderedAggregatingResultIterator;
import org.apache.phoenix.iterate.OrderedResultIterator;
import org.apache.phoenix.iterate.ParallelIteratorFactory;
import org.apache.phoenix.iterate.ParallelIterators;
import org.apache.phoenix.iterate.PeekingResultIterator;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.iterate.SequenceResultIterator;
import org.apache.phoenix.iterate.SpoolingResultIterator;
import org.apache.phoenix.iterate.UngroupedAggregatingResultIterator;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PInteger;



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

    public AggregatePlan(
            StatementContext context, FilterableStatement statement, TableRef table, RowProjector projector,
            Integer limit, OrderBy orderBy, ParallelIteratorFactory parallelIteratorFactory, GroupBy groupBy,
            Expression having) {
        super(context, statement, table, projector, context.getBindManager().getParameterMetaData(), limit, orderBy, groupBy, parallelIteratorFactory);
        this.having = having;
        this.aggregators = context.getAggregationManager().getAggregators();
    }

    public Expression getHaving() {
        return having;
    }
    
    @Override
    public List<KeyRange> getSplits() {
        return splits;
    }

    @Override
    public List<List<Scan>> getScans() {
        return scans;
    }

    private static class OrderingResultIteratorFactory implements ParallelIteratorFactory {
        private final QueryServices services;
        
        public OrderingResultIteratorFactory(QueryServices services) {
            this.services = services;
        }
        @Override
        public PeekingResultIterator newIterator(StatementContext context, ResultIterator scanner, Scan scan) throws SQLException {
            Expression expression = RowKeyExpression.INSTANCE;
            OrderByExpression orderByExpression = new OrderByExpression(expression, false, true);
            int threshold = services.getProps().getInt(QueryServices.SPOOL_THRESHOLD_BYTES_ATTRIB, QueryServicesOptions.DEFAULT_SPOOL_THRESHOLD_BYTES);
            return new OrderedResultIterator(scanner, Collections.<OrderByExpression>singletonList(orderByExpression), threshold);
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
        public PeekingResultIterator newIterator(StatementContext context, ResultIterator scanner, Scan scan) throws SQLException {
            PeekingResultIterator iterator = innerFactory.newIterator(context, scanner, scan);
            return outerFactory.newIterator(context, iterator, scan);
        }
    }

    private ParallelIteratorFactory wrapParallelIteratorFactory () {
        ParallelIteratorFactory innerFactory;
        QueryServices services = context.getConnection().getQueryServices();
        if (groupBy.isEmpty() || groupBy.isOrderPreserving()) {
            innerFactory = new SpoolingResultIterator.SpoolingResultIteratorFactory(services);
        } else {
            innerFactory = new OrderingResultIteratorFactory(services);
        }
        if (parallelIteratorFactory == null) {
            return innerFactory;
        }
        // wrap any existing parallelIteratorFactory
        return new WrappingResultIteratorFactory(innerFactory, parallelIteratorFactory);
    }
    
    @Override
    protected ResultIterator newIterator() throws SQLException {
        if (groupBy.isEmpty()) {
            UngroupedAggregateRegionObserver.serializeIntoScan(context.getScan());
        } else {
            // Set attribute with serialized expressions for coprocessor
            GroupedAggregateRegionObserver.serializeIntoScan(context.getScan(), groupBy.getScanAttribName(), groupBy.getKeyExpressions());
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
                context.getScan().setAttribute(BaseScannerRegionObserver.GROUP_BY_LIMIT, PInteger.INSTANCE.toBytes(limit));
            }
        }
        ParallelIterators parallelIterators = new ParallelIterators(this, null, wrapParallelIteratorFactory());
        splits = parallelIterators.getSplits();
        scans = parallelIterators.getScans();

        AggregatingResultIterator aggResultIterator;
        // No need to merge sort for ungrouped aggregation
        if (groupBy.isEmpty()) {
            aggResultIterator = new UngroupedAggregatingResultIterator(new ConcatResultIterator(parallelIterators), aggregators);
        } else {
            aggResultIterator = new GroupedAggregatingResultIterator(new MergeSortRowKeyResultIterator(parallelIterators), aggregators);
        }

        if (having != null) {
            aggResultIterator = new FilterAggregatingResultIterator(aggResultIterator, having);
        }
        
        if (statement.isDistinct() && statement.isAggregate()) { // Dedup on client if select distinct and aggregation
            aggResultIterator = new DistinctAggregatingResultIterator(aggResultIterator, getProjector());
        }

        ResultIterator resultScanner = aggResultIterator;
        if (orderBy.getOrderByExpressions().isEmpty()) {
            if (limit != null) {
                resultScanner = new LimitingResultIterator(aggResultIterator, limit);
            }
        } else {
            int thresholdBytes = context.getConnection().getQueryServices().getProps().getInt(
                    QueryServices.SPOOL_THRESHOLD_BYTES_ATTRIB, QueryServicesOptions.DEFAULT_SPOOL_THRESHOLD_BYTES);
            resultScanner = new OrderedAggregatingResultIterator(aggResultIterator, orderBy.getOrderByExpressions(), thresholdBytes, limit);
        }
        if (context.getSequenceManager().getSequenceCount() > 0) {
            resultScanner = new SequenceResultIterator(resultScanner, context.getSequenceManager());
        }
        return resultScanner;
    }
}
