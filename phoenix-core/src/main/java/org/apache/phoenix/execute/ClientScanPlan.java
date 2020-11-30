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
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.compile.ExplainPlanAttributes
    .ExplainPlanAttributesBuilder;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.execute.visitor.ByteCountVisitor;
import org.apache.phoenix.execute.visitor.QueryPlanVisitor;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.iterate.FilterResultIterator;
import org.apache.phoenix.iterate.LimitingResultIterator;
import org.apache.phoenix.iterate.OffsetResultIterator;
import org.apache.phoenix.iterate.OrderedResultIterator;
import org.apache.phoenix.iterate.ParallelScanGrouper;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.iterate.SequenceResultIterator;
import org.apache.phoenix.optimize.Cost;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.CostUtil;
import org.apache.phoenix.util.ExpressionUtil;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

public class ClientScanPlan extends ClientProcessingPlan {

    private List<OrderBy> actualOutputOrderBys;

    public ClientScanPlan(StatementContext context, FilterableStatement statement, TableRef table,
            RowProjector projector, Integer limit, Integer offset, Expression where, OrderBy orderBy,
            QueryPlan delegate) {
        super(context, statement, table, projector, limit, offset, where, orderBy, delegate);
        this.actualOutputOrderBys = convertActualOutputOrderBy(orderBy, delegate, context);
    }

    @Override
    public Cost getCost() {
        Double inputBytes = this.getDelegate().accept(new ByteCountVisitor());
        Double outputBytes = this.accept(new ByteCountVisitor());

        if (inputBytes == null || outputBytes == null) {
            return Cost.UNKNOWN;
        }

        int parallelLevel = CostUtil.estimateParallelLevel(
                false, context.getConnection().getQueryServices());
        Cost cost = new Cost(0, 0, 0);
        if (!orderBy.getOrderByExpressions().isEmpty()) {
            Cost orderByCost =
                    CostUtil.estimateOrderByCost(inputBytes, outputBytes, parallelLevel);
            cost = cost.plus(orderByCost);
        }
        return super.getCost().plus(cost);
    }

    @Override
    public <T> T accept(QueryPlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public ResultIterator iterator(ParallelScanGrouper scanGrouper, Scan scan) throws SQLException {
        ResultIterator iterator = delegate.iterator(scanGrouper, scan);
        if (where != null) {
            iterator = new FilterResultIterator(iterator, where);
        }
        
        if (!orderBy.getOrderByExpressions().isEmpty()) { // TopN
            long thresholdBytes =
                    context.getConnection().getQueryServices().getProps().getLong(
                        QueryServices.CLIENT_SPOOL_THRESHOLD_BYTES_ATTRIB,
                        QueryServicesOptions.DEFAULT_CLIENT_SPOOL_THRESHOLD_BYTES);
            boolean spoolingEnabled =
                    context.getConnection().getQueryServices().getProps().getBoolean(
                        QueryServices.CLIENT_ORDERBY_SPOOLING_ENABLED_ATTRIB,
                        QueryServicesOptions.DEFAULT_CLIENT_ORDERBY_SPOOLING_ENABLED);
            iterator =
                    new OrderedResultIterator(iterator, orderBy.getOrderByExpressions(),
                            spoolingEnabled, thresholdBytes, limit, offset,
                            projector.getEstimatedRowByteSize());
        } else {
            if (offset != null) {
                iterator = new OffsetResultIterator(iterator, offset);
            }
            if (limit != null) {
                iterator = new LimitingResultIterator(iterator, limit);
            }
        }
        
        if (context.getSequenceManager().getSequenceCount() > 0) {
            iterator = new SequenceResultIterator(iterator, context.getSequenceManager());
        }
        
        return iterator;
    }

    @Override
    public ExplainPlan getExplainPlan() throws SQLException {
        ExplainPlan explainPlan = delegate.getExplainPlan();
        List<String> currentPlanSteps = explainPlan.getPlanSteps();
        ExplainPlanAttributes explainPlanAttributes =
            explainPlan.getPlanStepsAsAttributes();
        List<String> planSteps = Lists.newArrayList(currentPlanSteps);
        ExplainPlanAttributesBuilder newBuilder =
            new ExplainPlanAttributesBuilder(explainPlanAttributes);
        if (where != null) {
            planSteps.add("CLIENT FILTER BY " + where.toString());
            newBuilder.setClientFilterBy(where.toString());
        }
        if (!orderBy.getOrderByExpressions().isEmpty()) {
            if (offset != null) {
                planSteps.add("CLIENT OFFSET " + offset);
                newBuilder.setClientOffset(offset);
            }
            planSteps.add("CLIENT" + (limit == null ? "" : " TOP " + limit + " ROW" + (limit == 1 ? "" : "S"))
                    + " SORTED BY " + orderBy.getOrderByExpressions().toString());
            newBuilder.setClientRowLimit(limit);
            newBuilder.setClientSortedBy(
                orderBy.getOrderByExpressions().toString());
        } else {
            if (offset != null) {
                planSteps.add("CLIENT OFFSET " + offset);
                newBuilder.setClientOffset(offset);
            }
            if (limit != null) {
                planSteps.add("CLIENT " + limit + " ROW LIMIT");
                newBuilder.setClientRowLimit(limit);
            }
        }
        if (context.getSequenceManager().getSequenceCount() > 0) {
            int nSequences = context.getSequenceManager().getSequenceCount();
            planSteps.add("CLIENT RESERVE VALUES FROM " + nSequences + " SEQUENCE" + (nSequences == 1 ? "" : "S"));
            newBuilder.setClientSequenceCount(nSequences);
        }

        return new ExplainPlan(planSteps, newBuilder.build());
    }

    private static List<OrderBy> convertActualOutputOrderBy(
            OrderBy orderBy,
            QueryPlan targetQueryPlan,
            StatementContext statementContext) {

        if(!orderBy.isEmpty()) {
            return Collections.singletonList(OrderBy.convertCompiledOrderByToOutputOrderBy(orderBy));
        }

        assert orderBy != OrderBy.REV_ROW_KEY_ORDER_BY;
        return targetQueryPlan.getOutputOrderBys();
    }

    @Override
    public List<OrderBy> getOutputOrderBys() {
       return this.actualOutputOrderBys;
    }
}
