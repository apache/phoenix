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
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.iterate.ConcatResultIterator;
import org.apache.phoenix.iterate.LimitingResultIterator;
import org.apache.phoenix.iterate.LookAheadResultIterator;
import org.apache.phoenix.iterate.OrderedResultIterator;
import org.apache.phoenix.iterate.ParallelIteratorFactory;
import org.apache.phoenix.iterate.PeekingResultIterator;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.iterate.SequenceResultIterator;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.TableRef;


public class UnionPlan extends BaseQueryPlan {
 //   private static final Logger logger = LoggerFactory.getLogger(UnionPlan.class);
    private List<KeyRange> splits = new ArrayList<KeyRange>();
    private List<List<Scan>> scans = new ArrayList<List<Scan>>();

    public UnionPlan(StatementContext context, FilterableStatement statement, TableRef table, RowProjector projector,
            Integer limit, OrderBy orderBy, ParallelIteratorFactory parallelIteratorFactory, GroupBy groupBy,
            List<QueryPlan> plans) {
        super(context, statement, table, projector, context.getBindManager().getParameterMetaData(), limit, orderBy, groupBy, parallelIteratorFactory, plans);
    }

    @Override
    public List<KeyRange> getSplits() {
        return splits;
    }

    @Override
    public List<List<Scan>> getScans() {
        return scans;
    }

    @Override
    public ResultIterator newIterator() throws SQLException {
        ResultIterator scanner;
        boolean isOrdered = !orderBy.getOrderByExpressions().isEmpty();

        List<ResultIterator> iterators = new ArrayList<ResultIterator>();
        List<PeekingResultIterator> pIterators = new ArrayList<PeekingResultIterator>();
        for (QueryPlan plan : this.getPlans()) {
            ResultIterator iterator = plan.iterator();
            PeekingResultIterator iter = LookAheadResultIterator.wrap(iterator);
            if (iter instanceof LookAheadResultIterator) {
                ((LookAheadResultIterator)iter).setIterator(iterator);
                ((LookAheadResultIterator)iter).setRowProjector(plan.getProjector());
                ((LookAheadResultIterator)iter).setUnionQuery(true);
            } 
            pIterators.add(iter);
        }
        scanner = new ConcatResultIterator(pIterators);

        if (isOrdered) { // TopN
            int thresholdBytes = context.getConnection().getQueryServices().getProps().getInt(
                QueryServices.SPOOL_THRESHOLD_BYTES_ATTRIB, QueryServicesOptions.DEFAULT_SPOOL_THRESHOLD_BYTES);
            scanner = new OrderedResultIterator(scanner, orderBy.getOrderByExpressions(), thresholdBytes, limit, this.getProjector().getEstimatedRowByteSize());
        } else if (limit != null) {
            scanner = new LimitingResultIterator(scanner, limit);
        }

        if (context.getSequenceManager().getSequenceCount() > 0) {
            scanner = new SequenceResultIterator(scanner, context.getSequenceManager());
        }
        return scanner;
    }

    @Override
    public ExplainPlan getExplainPlan() throws SQLException {
        List<String> steps = new ArrayList<String>();
        for (QueryPlan plan : this.getPlans()) {
            List<String> planSteps = plan.getExplainPlan().getPlanSteps();
            steps.addAll(planSteps);
        }
        if (!orderBy.getOrderByExpressions().isEmpty()) {
            steps.add("CLIENT" + (limit == null ? "" : " TOP " + limit + " ROW"  + (limit == 1 ? "" : "S"))  + " SORTED BY " + orderBy.getOrderByExpressions().toString());
        } else if (limit != null) {
            steps.add("CLIENT " + limit + " ROW LIMIT");
        }
        if (context.getSequenceManager().getSequenceCount() > 0) {
            int nSequences = context.getSequenceManager().getSequenceCount();
            steps.add("CLIENT RESERVE VALUES FROM " + nSequences + " SEQUENCE" + (nSequences == 1 ? "" : "S"));
        }
        return new ExplainPlan(steps);
    }
}

