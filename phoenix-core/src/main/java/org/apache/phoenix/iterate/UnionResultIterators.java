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
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.QueryPlan;
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
    private final List<QueryPlan> plans;

    public UnionResultIterators(List<QueryPlan> plans) throws SQLException {
        this.plans = plans;
        int nPlans = plans.size();
        iterators = Lists.newArrayListWithExpectedSize(nPlans);
        splits = Lists.newArrayListWithExpectedSize(nPlans * 30); 
        scans = Lists.newArrayListWithExpectedSize(nPlans * 10); 
        for (QueryPlan plan : this.plans) {
            iterators.add(LookAheadResultIterator.wrap(plan.iterator()));
            splits.addAll(plan.getSplits()); 
            scans.addAll(plan.getScans());
        }
    }

    @Override
    public List<KeyRange> getSplits() {
        return splits;
    }

    @Override
    public void close() throws SQLException {   
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
            if (toThrow != null) {
                throw toThrow;
            }
        }
    }

    @Override
    public List<List<Scan>> getScans() {
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