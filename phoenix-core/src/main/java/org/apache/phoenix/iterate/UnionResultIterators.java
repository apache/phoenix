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
package org.apache.phoenix.iterate;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.monitoring.OverAllQueryMetrics;
import org.apache.phoenix.monitoring.ReadMetricQueue;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.util.ServerUtil;

import com.google.common.collect.Lists;


/**
 *
 * Create a union ResultIterators
 *
 * 
 */
public class UnionResultIterators implements ResultIterators {
    private final List<KeyRange> splits;
    private final List<List<Scan>> scans;
    private final List<PeekingResultIterator> iterators;
    private final List<ReadMetricQueue> readMetricsList;
    private final List<OverAllQueryMetrics> overAllQueryMetricsList;
    private boolean closed;
    private final StatementContext parentStmtCtx;
    public UnionResultIterators(List<QueryPlan> plans, StatementContext parentStmtCtx) throws SQLException {
        this.parentStmtCtx = parentStmtCtx;
        int nPlans = plans.size();
        iterators = Lists.newArrayListWithExpectedSize(nPlans);
        splits = Lists.newArrayListWithExpectedSize(nPlans * 30); 
        scans = Lists.newArrayListWithExpectedSize(nPlans * 10); 
        readMetricsList = Lists.newArrayListWithCapacity(nPlans);
        overAllQueryMetricsList = Lists.newArrayListWithCapacity(nPlans);
        for (QueryPlan plan : plans) {
            readMetricsList.add(plan.getContext().getReadMetricsQueue());
            overAllQueryMetricsList.add(plan.getContext().getOverallQueryMetrics());
            iterators.add(LookAheadResultIterator.wrap(plan.iterator()));
            splits.addAll(plan.getSplits()); 
            scans.addAll(plan.getScans());
        }
    }

    @Override
    public List<KeyRange> getSplits() {
        if (splits == null)
            return Collections.emptyList();
        else
            return splits;
    }

    @Override
    public void close() throws SQLException {
        if (!closed) {
            closed = true;
            SQLException toThrow = null;
            try {
                if (iterators != null) {
                    for (int index=0; index < iterators.size(); index++) {
                        PeekingResultIterator iterator = iterators.get(index);
                        try {
                            iterator.close();
                        } catch (Exception e) {
                            if (toThrow == null) {
                                toThrow = ServerUtil.parseServerException(e);
                            } else {
                                toThrow.setNextException(ServerUtil.parseServerException(e));
                            }
                        }
                    }
                }
            } catch (Exception e) {
                toThrow = ServerUtil.parseServerException(e);
            } finally {
                setMetricsInParentContext();
                if (toThrow != null) {
                    throw toThrow;
                }
            }
        }
    }
    
    private void setMetricsInParentContext() {
        ReadMetricQueue parentCtxReadMetrics = parentStmtCtx.getReadMetricsQueue();
        for (ReadMetricQueue readMetrics : readMetricsList) {
            parentCtxReadMetrics.combineReadMetrics(readMetrics);
        }
        OverAllQueryMetrics parentCtxQueryMetrics = parentStmtCtx.getOverallQueryMetrics();
        for (OverAllQueryMetrics metric : overAllQueryMetricsList) {
            parentCtxQueryMetrics.combine(metric);
        }
    }
    
    @Override
    public List<List<Scan>> getScans() {
        if (scans == null)
            return Collections.emptyList();
        else
            return scans;
    }

    @Override
    public int size() {
        return scans.size();
    }

    @Override
    public void explain(List<String> planSteps) {
        for (int index=0; index < iterators.size(); index++) {
            iterators.get(index).explain(planSteps);
        }
    }

    @Override 
    public List<PeekingResultIterator> getIterators() throws SQLException {    
        return iterators;
    }
}