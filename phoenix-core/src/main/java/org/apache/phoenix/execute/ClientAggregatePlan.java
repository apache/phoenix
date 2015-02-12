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

import static org.apache.phoenix.query.QueryConstants.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.OrderByExpression;
import org.apache.phoenix.expression.aggregator.Aggregators;
import org.apache.phoenix.expression.aggregator.ServerAggregators;
import org.apache.phoenix.iterate.AggregatingResultIterator;
import org.apache.phoenix.iterate.BaseGroupedAggregatingResultIterator;
import org.apache.phoenix.iterate.DistinctAggregatingResultIterator;
import org.apache.phoenix.iterate.FilterAggregatingResultIterator;
import org.apache.phoenix.iterate.FilterResultIterator;
import org.apache.phoenix.iterate.GroupedAggregatingResultIterator;
import org.apache.phoenix.iterate.LimitingResultIterator;
import org.apache.phoenix.iterate.LookAheadResultIterator;
import org.apache.phoenix.iterate.OrderedAggregatingResultIterator;
import org.apache.phoenix.iterate.OrderedResultIterator;
import org.apache.phoenix.iterate.PeekingResultIterator;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.iterate.SequenceResultIterator;
import org.apache.phoenix.iterate.UngroupedAggregatingResultIterator;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.tuple.MultiKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.TupleUtil;

import com.google.common.collect.Lists;

public class ClientAggregatePlan extends ClientProcessingPlan {
    private final GroupBy groupBy;
    private final Expression having;
    private final Aggregators serverAggregators;
    private final Aggregators clientAggregators;
    
    public ClientAggregatePlan(StatementContext context, FilterableStatement statement, TableRef table, RowProjector projector,
            Integer limit, Expression where, OrderBy orderBy, GroupBy groupBy, Expression having, QueryPlan delegate) {
        super(context, statement, table, projector, limit, where, orderBy, delegate);
        this.groupBy = groupBy;
        this.having = having;
        this.serverAggregators =
                ServerAggregators.deserialize(context.getScan()
                        .getAttribute(BaseScannerRegionObserver.AGGREGATORS), QueryServicesOptions.withDefaults().getConfiguration());
        this.clientAggregators = context.getAggregationManager().getAggregators();
    }

    @Override
    public ResultIterator iterator() throws SQLException {
        ResultIterator iterator = delegate.iterator();
        if (where != null) {
            iterator = new FilterResultIterator(iterator, where);
        }
        
        AggregatingResultIterator aggResultIterator;
        if (groupBy.isEmpty()) {
            aggResultIterator = new ClientUngroupedAggregatingResultIterator(LookAheadResultIterator.wrap(iterator), serverAggregators);
            aggResultIterator = new UngroupedAggregatingResultIterator(LookAheadResultIterator.wrap(aggResultIterator), clientAggregators);
        } else {
            if (!groupBy.isOrderPreserving()) {
                int thresholdBytes = context.getConnection().getQueryServices().getProps().getInt(
                        QueryServices.SPOOL_THRESHOLD_BYTES_ATTRIB, QueryServicesOptions.DEFAULT_SPOOL_THRESHOLD_BYTES);
                List<Expression> keyExpressions = groupBy.getKeyExpressions();
                List<OrderByExpression> keyExpressionOrderBy = Lists.newArrayListWithExpectedSize(keyExpressions.size());
                for (Expression keyExpression : keyExpressions) {
                    keyExpressionOrderBy.add(new OrderByExpression(keyExpression, false, true));
                }
                iterator = new OrderedResultIterator(iterator, keyExpressionOrderBy, thresholdBytes, limit, projector.getEstimatedRowByteSize());
            }
            aggResultIterator = new ClientGroupedAggregatingResultIterator(LookAheadResultIterator.wrap(iterator), serverAggregators, groupBy.getKeyExpressions());
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

    @Override
    public ExplainPlan getExplainPlan() throws SQLException {
        List<String> planSteps = Lists.newArrayList(delegate.getExplainPlan().getPlanSteps());
        if (where != null) {
            planSteps.add("CLIENT FILTER BY " + where.toString());
        }
        if (!groupBy.isEmpty()) {
            if (!groupBy.isOrderPreserving()) {
                planSteps.add("CLIENT SORTED BY " + groupBy.getKeyExpressions().toString());
            }
            planSteps.add("CLIENT AGGREGATE INTO DISTINCT ROWS BY " + groupBy.getExpressions().toString());
        } else {
            planSteps.add("CLIENT AGGREGATE INTO SINGLE ROW");            
        }
        if (having != null) {
            planSteps.add("CLIENT AFTER-AGGREGATION FILTER BY " + having.toString());
        }
        if (statement.isDistinct() && statement.isAggregate()) {
            planSteps.add("CLIENT DISTINCT ON " + projector.toString());
        }
        if (orderBy.getOrderByExpressions().isEmpty()) {
            if (limit != null) {
                planSteps.add("CLIENT " + limit + " ROW LIMIT");
            }
        } else {
            planSteps.add("CLIENT" + (limit == null ? "" : " TOP " + limit + " ROW"  + (limit == 1 ? "" : "S"))  + " SORTED BY " + orderBy.getOrderByExpressions().toString());
        }
        if (context.getSequenceManager().getSequenceCount() > 0) {
            int nSequences = context.getSequenceManager().getSequenceCount();
            planSteps.add("CLIENT RESERVE VALUES FROM " + nSequences + " SEQUENCE" + (nSequences == 1 ? "" : "S"));
        }
        
        return new ExplainPlan(planSteps);
    }

    @Override
    public GroupBy getGroupBy() {
        return groupBy;
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
        protected Tuple wrapKeyValueAsResult(KeyValue keyValue) {
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
        protected Tuple wrapKeyValueAsResult(KeyValue keyValue)
                throws SQLException {
            return new MultiKeyValueTuple(Collections.<Cell> singletonList(keyValue));
        }

        @Override
        public String toString() {
            return "ClientUngroupedAggregatingResultIterator [resultIterator=" 
                    + resultIterator + ", aggregators=" + aggregators + "]";
        }
    }
}
