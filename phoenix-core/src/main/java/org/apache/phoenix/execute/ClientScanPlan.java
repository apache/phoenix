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
import java.util.List;

import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.iterate.FilterResultIterator;
import org.apache.phoenix.iterate.LimitingResultIterator;
import org.apache.phoenix.iterate.OrderedResultIterator;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.iterate.SequenceResultIterator;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.TableRef;

import com.google.common.collect.Lists;

public class ClientScanPlan extends ClientProcessingPlan {

    public ClientScanPlan(StatementContext context,
            FilterableStatement statement, TableRef table,
            RowProjector projector, Integer limit, Expression where,
            OrderBy orderBy, QueryPlan delegate) {
        super(context, statement, table, projector, limit, where, orderBy,
                delegate);
    }

    @Override
    public ResultIterator iterator() throws SQLException {
        ResultIterator iterator = delegate.iterator();
        if (where != null) {
            iterator = new FilterResultIterator(iterator, where);
        }
        
        if (!orderBy.getOrderByExpressions().isEmpty()) { // TopN
            int thresholdBytes = context.getConnection().getQueryServices().getProps().getInt(
                    QueryServices.SPOOL_THRESHOLD_BYTES_ATTRIB, QueryServicesOptions.DEFAULT_SPOOL_THRESHOLD_BYTES);
            iterator = new OrderedResultIterator(iterator, orderBy.getOrderByExpressions(), thresholdBytes, limit, projector.getEstimatedRowByteSize());
        } else if (limit != null) {
            iterator = new LimitingResultIterator(iterator, limit);
        }
        
        if (context.getSequenceManager().getSequenceCount() > 0) {
            iterator = new SequenceResultIterator(iterator, context.getSequenceManager());
        }
        
        return iterator;
    }

    @Override
    public ExplainPlan getExplainPlan() throws SQLException {
        List<String> planSteps = Lists.newArrayList(delegate.getExplainPlan().getPlanSteps());
        if (where != null) {
            planSteps.add("CLIENT FILTER BY " + where.toString());
        }
        if (!orderBy.getOrderByExpressions().isEmpty()) {
            planSteps.add("CLIENT" + (limit == null ? "" : " TOP " + limit + " ROW"  + (limit == 1 ? "" : "S"))  + " SORTED BY " + orderBy.getOrderByExpressions().toString());
        } else if (limit != null) {
            planSteps.add("CLIENT " + limit + " ROW LIMIT");
        }
        if (context.getSequenceManager().getSequenceCount() > 0) {
            int nSequences = context.getSequenceManager().getSequenceCount();
            planSteps.add("CLIENT RESERVE VALUES FROM " + nSequences + " SEQUENCE" + (nSequences == 1 ? "" : "S"));
        }
        
        return new ExplainPlan(planSteps);
    }

}
