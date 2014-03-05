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
import org.apache.phoenix.iterate.DelegateResultIterator;
import org.apache.phoenix.iterate.ParallelIterators.ParallelIteratorFactory;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.SQLCloseable;
import org.apache.phoenix.util.SQLCloseables;
import org.apache.phoenix.util.ScanUtil;

import com.google.common.collect.Lists;



/**
 *
 * Query plan that has no child plans
 *
 * 
 * @since 0.1
 */
public abstract class BasicQueryPlan implements QueryPlan {
    protected static final long DEFAULT_ESTIMATED_SIZE = 10 * 1024; // 10 K
    
    protected final TableRef tableRef;
    protected final StatementContext context;
    protected final FilterableStatement statement;
    protected final RowProjector projection;
    protected final ParameterMetaData paramMetaData;
    protected final Integer limit;
    protected final OrderBy orderBy;
    protected final GroupBy groupBy;
    protected final ParallelIteratorFactory parallelIteratorFactory;

    protected BasicQueryPlan(
            StatementContext context, FilterableStatement statement, TableRef table,
            RowProjector projection, ParameterMetaData paramMetaData, Integer limit, OrderBy orderBy,
            GroupBy groupBy, ParallelIteratorFactory parallelIteratorFactory) {
        this.context = context;
        this.statement = statement;
        this.tableRef = table;
        this.projection = projection;
        this.paramMetaData = paramMetaData;
        this.limit = limit;
        this.orderBy = orderBy;
        this.groupBy = groupBy;
        this.parallelIteratorFactory = parallelIteratorFactory;
    }

    @Override
    public boolean isDegenerate() {
        return context.getScanRanges() == ScanRanges.NOTHING;

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
        return projection;
    }

//    /**
//     * Sets up an id used to do round robin queue processing on the server
//     * @param scan
//     */
//    private void setProducer(Scan scan) {
//        byte[] producer = Bytes.toBytes(UUID.randomUUID().toString());
//        scan.setAttribute(HBaseServer.CALL_QUEUE_PRODUCER_ATTRIB_NAME, producer);
//    }
    
    @Override
    public final ResultIterator iterator() throws SQLException {
        return iterator(Collections.<SQLCloseable>emptyList());
    }

    public final ResultIterator iterator(final List<SQLCloseable> dependencies) throws SQLException {
        if (context.getScanRanges() == ScanRanges.NOTHING) {
            return ResultIterator.EMPTY_ITERATOR;
        }
        
        Scan scan = context.getScan();
        // Set producer on scan so HBase server does round robin processing
        //setProducer(scan);
        // Set the time range on the scan so we don't get back rows newer than when the statement was compiled
        // The time stamp comes from the server at compile time when the meta data
        // is resolved.
        // TODO: include time range in explain plan?
        PhoenixConnection connection = context.getConnection();
        Long scn = connection.getSCN();
        ScanUtil.setTimeRange(scan, scn == null ? context.getCurrentTime() : scn);
        ScanUtil.setTenantId(scan, connection.getTenantId() == null ? null : connection.getTenantId().getBytes());
        ResultIterator iterator = newIterator();
        return dependencies.isEmpty() ? 
                iterator : new DelegateResultIterator(iterator) {
            @Override
            public void close() throws SQLException {
                try {
                    super.close();
                } finally {
                    SQLCloseables.closeAll(dependencies);
                }
            }
        };
    }

    abstract protected ResultIterator newIterator() throws SQLException;
    
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
    public ExplainPlan getExplainPlan() throws SQLException {
        if (context.getScanRanges() == ScanRanges.NOTHING) {
            return new ExplainPlan(Collections.singletonList("DEGENERATE SCAN OVER " + tableRef.getTable().getName().getString()));
        }
        
        // Optimize here when getting explain plan, as queries don't get optimized until after compilation
        QueryPlan plan = context.getConnection().getQueryServices().getOptimizer().optimize(context.getStatement(), this);
        ResultIterator iterator = plan.iterator();
        List<String> planSteps = Lists.newArrayListWithExpectedSize(5);
        iterator.explain(planSteps);
        return new ExplainPlan(planSteps);
    }
}
