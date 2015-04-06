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

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.iterate.ConcatResultIterator;
import org.apache.phoenix.iterate.LimitingResultIterator;
import org.apache.phoenix.iterate.MergeSortTopNResultIterator;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.iterate.UnionResultIterators;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.SQLCloseable;


public class UnionPlan implements QueryPlan {
    private static final long DEFAULT_ESTIMATED_SIZE = 10 * 1024; // 10 K

    private final TableRef tableRef;
    private final FilterableStatement statement;
    private final ParameterMetaData paramMetaData;
    private final OrderBy orderBy;
    private final StatementContext context;
    private final Integer limit;
    private final GroupBy groupBy;
    private final RowProjector projector;
    private final boolean isDegenerate;
    private final List<QueryPlan> plans;
    private UnionResultIterators iterators;

    public UnionPlan(StatementContext context, FilterableStatement statement, TableRef table, RowProjector projector,
            Integer limit, OrderBy orderBy, GroupBy groupBy, List<QueryPlan> plans, ParameterMetaData paramMetaData) throws SQLException {
        this.context = context;
        this.statement = statement;
        this.tableRef = table;
        this.projector = projector;
        this.limit = limit;
        this.orderBy = orderBy;
        this.groupBy = groupBy;
        this.plans = plans;
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
    public RowProjector getProjector() {
        return projector;
    }

    @Override
    public final ResultIterator iterator() throws SQLException {
        return iterator(Collections.<SQLCloseable>emptyList());
    }

    public final ResultIterator iterator(final List<? extends SQLCloseable> dependencies) throws SQLException {
        this.iterators = new UnionResultIterators(plans);
        ResultIterator scanner;      
        boolean isOrdered = !orderBy.getOrderByExpressions().isEmpty();

        if (isOrdered) { // TopN
            scanner = new MergeSortTopNResultIterator(iterators, limit, orderBy.getOrderByExpressions());
        } else {
            scanner = new ConcatResultIterator(iterators);
            if (limit != null) {
                scanner = new LimitingResultIterator(scanner, limit);
            }          
        }
        return scanner;
    }

    @Override
    public ExplainPlan getExplainPlan() throws SQLException {
        List<String> steps = new ArrayList<String>();
        steps.add("UNION ALL OVER " + this.plans.size() + " QUERIES");
        ResultIterator iterator = iterator();
        iterator.explain(steps);
        // Indent plans steps nested under union, except last client-side merge/concat step (if there is one)
        int offset = !orderBy.getOrderByExpressions().isEmpty() || limit != null ? 1 : 0;
        for (int i = 1 ; i < steps.size()-offset; i++) {
            steps.set(i, "    " + steps.get(i));
        }
        return new ExplainPlan(steps);
    }


    @Override
    public long getEstimatedSize() {
        return DEFAULT_ESTIMATED_SIZE;
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
        return context;
    }

    @Override
    public boolean isRowKeyOrdered() {
        return groupBy.isEmpty() ? orderBy.getOrderByExpressions().isEmpty() : groupBy.isOrderPreserving();
    }

    public List<QueryPlan> getPlans() {
        return this.plans;
    }
}

