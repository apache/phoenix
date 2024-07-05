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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.compile.ExplainPlanAttributes
    .ExplainPlanAttributesBuilder;
import org.apache.phoenix.compile.OrderPreservingTracker;
import org.apache.phoenix.compile.OrderPreservingTracker.Info;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.OrderPreservingTracker.Ordering;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.execute.visitor.QueryPlanVisitor;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.OrderByExpression;
import org.apache.phoenix.expression.ProjectedColumnExpression;
import org.apache.phoenix.iterate.DelegateResultIterator;
import org.apache.phoenix.iterate.FilterResultIterator;
import org.apache.phoenix.iterate.ParallelScanGrouper;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.tuple.Tuple;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

public class TupleProjectionPlan extends DelegateQueryPlan {
    private final TupleProjector tupleProjector;
    private final Expression postFilter;
    private final ColumnResolver columnResolver;
    private final List<OrderBy> actualOutputOrderBys;

    public TupleProjectionPlan(
            QueryPlan plan,
            TupleProjector tupleProjector,
            StatementContext statementContext,
            Expression postFilter) throws SQLException {
        super(plan);
        if (tupleProjector == null) {
            throw new IllegalArgumentException("tupleProjector is null");
        }
        this.tupleProjector = tupleProjector;
        this.postFilter = postFilter;
        if (statementContext != null) {
            this.columnResolver = statementContext.getResolver();
            this.actualOutputOrderBys = this.convertInputOrderBys(plan);
        } else {
            this.columnResolver = null;
            this.actualOutputOrderBys = Collections.<OrderBy>emptyList();
        }
    }

    /**
     * Map the expressions in the actualOutputOrderBys of targetQueryPlan to {@link ProjectedColumnExpression}.
     * @param targetQueryPlan
     * @return
     * @throws SQLException
     */
    private List<OrderBy> convertInputOrderBys(QueryPlan targetQueryPlan) throws SQLException {
        List<OrderBy> inputOrderBys = targetQueryPlan.getOutputOrderBys();
        if(inputOrderBys.isEmpty()) {
            return Collections.<OrderBy> emptyList();
        }
        Expression[] selectColumnExpressions = this.tupleProjector.getExpressions();
        Map<Expression,Integer> selectColumnExpressionToIndex =
                new HashMap<Expression, Integer>(selectColumnExpressions.length);
        int columnIndex = 0;
        for(Expression selectColumnExpression : selectColumnExpressions) {
            selectColumnExpressionToIndex.put(selectColumnExpression, columnIndex++);
        }
        List<OrderBy> newOrderBys = new ArrayList<OrderBy>(inputOrderBys.size());
        for(OrderBy inputOrderBy : inputOrderBys) {
            OrderBy newOrderBy = this.convertSingleInputOrderBy(
                    targetQueryPlan,
                    selectColumnExpressionToIndex,
                    selectColumnExpressions,
                    inputOrderBy);
            if(newOrderBy != OrderBy.EMPTY_ORDER_BY) {
                newOrderBys.add(newOrderBy);
            }
        }
        if(newOrderBys.isEmpty()) {
            return Collections.<OrderBy> emptyList();
        }
        return newOrderBys;
    }

    private OrderBy convertSingleInputOrderBy(
            QueryPlan targetQueryPlan,
            Map<Expression,Integer> selectColumnExpressionToIndex,
            Expression[] selectColumnExpressions,
            OrderBy inputOrderBy) throws SQLException {
        //Here we track targetQueryPlan's output so we use targetQueryPlan's StatementContext
        OrderPreservingTracker orderPreservingTracker = new OrderPreservingTracker(
                targetQueryPlan.getContext(),
                GroupBy.EMPTY_GROUP_BY,
                Ordering.UNORDERED,
                selectColumnExpressions.length,
                Collections.singletonList(inputOrderBy),
                null,
                null);
        for(Expression selectColumnExpression : selectColumnExpressions) {
            orderPreservingTracker.track(selectColumnExpression);
        }
        orderPreservingTracker.isOrderPreserving();
        List<Info> orderPreservingTrackInfos = orderPreservingTracker.getOrderPreservingTrackInfos();
        if(orderPreservingTrackInfos.isEmpty()) {
            return OrderBy.EMPTY_ORDER_BY;
        }
        List<OrderByExpression> newOrderByExpressions = new ArrayList<OrderByExpression>(orderPreservingTrackInfos.size());
        for(Info orderPreservingTrackInfo : orderPreservingTrackInfos) {
            Expression expression = orderPreservingTrackInfo.getExpression();
            Integer index = selectColumnExpressionToIndex.get(expression);
            assert index != null;
            ProjectedColumnExpression projectedValueColumnExpression = this.getProjectedValueColumnExpression(index);
            OrderByExpression newOrderByExpression = OrderByExpression.createByCheckIfOrderByReverse(
                    projectedValueColumnExpression,
                    orderPreservingTrackInfo.isNullsLast(),
                    orderPreservingTrackInfo.isAscending(),
                    false);
            newOrderByExpressions.add(newOrderByExpression);
        }
        return new OrderBy(newOrderByExpressions);
    }

    private ProjectedColumnExpression getProjectedValueColumnExpression(int columnIndex) throws SQLException {
        assert this.columnResolver != null;
        TableRef tableRef = this.columnResolver.getTables().get(0);
        ColumnRef columnRef = new ColumnRef(tableRef, columnIndex);
        return (ProjectedColumnExpression)columnRef.newColumnExpression();
    }

    @Override
    public ExplainPlan getExplainPlan() throws SQLException {
        ExplainPlan explainPlan = delegate.getExplainPlan();
        List<String> planSteps = Lists.newArrayList(explainPlan.getPlanSteps());
        ExplainPlanAttributes explainPlanAttributes =
            explainPlan.getPlanStepsAsAttributes();
        if (postFilter != null) {
            planSteps.add("CLIENT FILTER BY " + postFilter.toString());
            ExplainPlanAttributesBuilder newBuilder =
                new ExplainPlanAttributesBuilder(explainPlanAttributes);
            newBuilder.setClientFilterBy(postFilter.toString());
            explainPlanAttributes = newBuilder.build();
        }

        return new ExplainPlan(planSteps, explainPlanAttributes);
    }

    @Override
    public ResultIterator iterator(ParallelScanGrouper scanGrouper, Scan scan) throws SQLException {
        ResultIterator iterator = new DelegateResultIterator(delegate.iterator(scanGrouper, scan)) {
            
            @Override
            public Tuple next() throws SQLException {
                Tuple tuple = super.next();
                if (tuple == null)
                    return null;
                
                return tupleProjector.projectResults(tuple);
            }

            @Override
            public String toString() {
                return "TupleProjectionResultIterator [projector=" + tupleProjector + "]";
            }            
        };
        
        if (postFilter != null) {
            iterator = new FilterResultIterator(iterator, postFilter);
        }
        
        return iterator;
    }

    @Override
    public <T> T accept(QueryPlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public List<OrderBy> getOutputOrderBys() {
        return this.actualOutputOrderBys;
    }
}
