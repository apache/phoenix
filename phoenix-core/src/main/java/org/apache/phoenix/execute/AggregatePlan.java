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

import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.StatementContext;
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
import org.apache.phoenix.iterate.ParallelIterators;
import org.apache.phoenix.iterate.ParallelIterators.ParallelIteratorFactory;
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



/**
 *
 * Query plan for aggregating queries
 *
 * 
 * @since 0.1
 */
public class AggregatePlan extends BasicQueryPlan {
    private final Aggregators aggregators;
    private final Expression having;
    private List<KeyRange> splits;

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

    private static class OrderingResultIteratorFactory implements ParallelIteratorFactory {
        private final QueryServices services;
        
        public OrderingResultIteratorFactory(QueryServices services) {
            this.services = services;
        }
        @Override
        public PeekingResultIterator newIterator(StatementContext context, ResultIterator scanner) throws SQLException {
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
        public PeekingResultIterator newIterator(StatementContext context, ResultIterator scanner) throws SQLException {
            PeekingResultIterator iterator = innerFactory.newIterator(context, scanner);
            return outerFactory.newIterator(context, iterator);
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
        }
        ParallelIterators parallelIterators = new ParallelIterators(context, tableRef, statement, projection, groupBy, null, wrapParallelIteratorFactory());
        splits = parallelIterators.getSplits();

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
            int thresholdBytes = getConnectionQueryServices(context.getConnection().getQueryServices()).getProps().getInt(
                    QueryServices.SPOOL_THRESHOLD_BYTES_ATTRIB, QueryServicesOptions.DEFAULT_SPOOL_THRESHOLD_BYTES);
            resultScanner = new OrderedAggregatingResultIterator(aggResultIterator, orderBy.getOrderByExpressions(), thresholdBytes, limit);
        }
        if (context.getSequenceManager().getSequenceCount() > 0) {
            resultScanner = new SequenceResultIterator(resultScanner, context.getSequenceManager());
        }
        return resultScanner;
    }
}
