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
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.iterate.DefaultParallelScanGrouper;
import org.apache.phoenix.iterate.ParallelScanGrouper;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.jdbc.PhoenixStatement.Operation;
import org.apache.phoenix.optimize.Cost;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.TableRef;

public abstract class DelegateQueryPlan implements QueryPlan {
    protected final QueryPlan delegate;

    public DelegateQueryPlan(QueryPlan delegate) {
        this.delegate = delegate;
    }

    @Override
    public StatementContext getContext() {
        return delegate.getContext();
    }

    @Override
    public ParameterMetaData getParameterMetaData() {
        return delegate.getParameterMetaData();
    }

    @Override
    public long getEstimatedSize() {
        return delegate.getEstimatedSize();
    }

    @Override
    public Cost getCost() {
        return delegate.getCost();
    }

    @Override
    public TableRef getTableRef() {
        return delegate.getTableRef();
    }

    @Override
    public Set<TableRef> getSourceRefs() {
        return delegate.getSourceRefs();
    }

    @Override
    public RowProjector getProjector() {
        return delegate.getProjector();
    }

    @Override
    public Integer getLimit() {
        return delegate.getLimit();
    }

    @Override
    public OrderBy getOrderBy() {
        return delegate.getOrderBy();
    }

    @Override
    public GroupBy getGroupBy() {
        return delegate.getGroupBy();
    }

    @Override
    public List<KeyRange> getSplits() {
        return delegate.getSplits();
    }

    @Override
    public List<List<Scan>> getScans() {
        return delegate.getScans();
    }

    @Override
    public FilterableStatement getStatement() {
        return delegate.getStatement();
    }

    @Override
    public boolean isDegenerate() {
        return delegate.isDegenerate();
    }

    @Override
    public boolean isRowKeyOrdered() {
        return delegate.isRowKeyOrdered();
    }
    
    @Override
    public boolean useRoundRobinIterator() throws SQLException {
        return delegate.useRoundRobinIterator();
    }

	@Override
	public Operation getOperation() {
		return delegate.getOperation();
	}
	
	@Override
    public Integer getOffset() {
        return delegate.getOffset();
    }
     
     @Override
     public ResultIterator iterator() throws SQLException {
         return iterator(DefaultParallelScanGrouper.getInstance());
     }
 
     @Override
     public ResultIterator iterator(ParallelScanGrouper scanGrouper) throws SQLException {
         return iterator(scanGrouper, null);
     }

    public QueryPlan getDelegate() {
        return delegate;
    }

    @Override
    public Long getEstimatedRowsToScan() throws SQLException {
        return delegate.getEstimatedRowsToScan();
    }

    @Override
    public Long getEstimatedBytesToScan() throws SQLException {
        return delegate.getEstimatedBytesToScan();
    }

    @Override
    public Long getEstimateInfoTimestamp() throws SQLException {
        return delegate.getEstimateInfoTimestamp();
    }

    @Override
    public List<OrderBy> getOutputOrderBys() {
        return delegate.getOutputOrderBys();
    }

    @Override
    public boolean isApplicable() { return delegate.isApplicable(); }
}