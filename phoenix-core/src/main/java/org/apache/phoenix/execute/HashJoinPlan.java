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
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.cache.ServerCacheClient.ServerCache;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.job.JobManager.JobCallable;
import org.apache.phoenix.join.HashCacheClient;
import org.apache.phoenix.join.HashJoinInfo;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.SQLCloseable;
import org.apache.phoenix.util.SQLCloseables;

import com.google.common.collect.Lists;

public class HashJoinPlan implements QueryPlan {
    private static final Log LOG = LogFactory.getLog(HashJoinPlan.class);
    
    private BasicQueryPlan plan;
    private HashJoinInfo joinInfo;
    private List<Expression>[] hashExpressions;
    private QueryPlan[] hashPlans;
    
    public HashJoinPlan(BasicQueryPlan plan, HashJoinInfo joinInfo,
            List<Expression>[] hashExpressions, QueryPlan[] hashPlans) {
        this.plan = plan;
        this.joinInfo = joinInfo;
        this.hashExpressions = hashExpressions;
        this.hashPlans = hashPlans;
    }

    @Override
    public Integer getLimit() {
        return plan.getLimit();
    }

    @Override
    public OrderBy getOrderBy() {
        return plan.getOrderBy();
    }

    @Override
    public RowProjector getProjector() {
        return plan.getProjector();
    }

    @Override
    public ResultIterator iterator() throws SQLException {
        ImmutableBytesPtr[] joinIds = joinInfo.getJoinIds();
        assert (joinIds.length == hashExpressions.length && joinIds.length == hashPlans.length);
        
        final HashCacheClient hashClient = new HashCacheClient(plan.getContext().getConnection());
        Scan scan = plan.getContext().getScan();
        final ScanRanges ranges = plan.getContext().getScanRanges();
        
        int count = joinIds.length;
        ConnectionQueryServices services = getContext().getConnection().getQueryServices();
        ExecutorService executor = services.getExecutor();
        List<Future<ServerCache>> futures = new ArrayList<Future<ServerCache>>(count);
        List<SQLCloseable> dependencies = new ArrayList<SQLCloseable>(count);
        final int maxServerCacheTimeToLive = services.getProps().getInt(QueryServices.MAX_SERVER_CACHE_TIME_TO_LIVE_MS_ATTRIB, QueryServicesOptions.DEFAULT_MAX_SERVER_CACHE_TIME_TO_LIVE_MS);
        final AtomicLong firstJobEndTime = new AtomicLong(0);
        SQLException firstException = null;
        for (int i = 0; i < count; i++) {
            final int index = i;
            futures.add(executor.submit(new JobCallable<ServerCache>() {

                @Override
                public ServerCache call() throws Exception {
                    QueryPlan hashPlan = hashPlans[index];
                    ServerCache cache = hashClient.addHashCache(ranges, hashPlan.iterator(), 
                            hashPlan.getEstimatedSize(), hashExpressions[index], plan.getTableRef());
                    long endTime = System.currentTimeMillis();
                    boolean isSet = firstJobEndTime.compareAndSet(0, endTime);
                    if (!isSet && (endTime - firstJobEndTime.get()) > maxServerCacheTimeToLive) {
                        LOG.warn("Hash plan [" + index + "] execution seems too slow. Earlier hash cache(s) might have expired on servers.");
                    }
                    return cache;
                }

                @Override
                public Object getJobId() {
                    return HashJoinPlan.this;
                }
            }));
        }
        for (int i = 0; i < count; i++) {
            try {
                ServerCache cache = futures.get(i).get();
                joinIds[i].set(cache.getId());
                dependencies.add(cache);
            } catch (InterruptedException e) {
                if (firstException == null) {
                    firstException = new SQLException("Hash plan [" + i + "] execution interrupted.", e);
                }
            } catch (ExecutionException e) {
                if (firstException == null) {
                    firstException = new SQLException("Encountered exception in hash plan [" + i + "] execution.", 
                        e.getCause());
                }
            }
        }
        if (firstException != null) {
            SQLCloseables.closeAllQuietly(dependencies);
            throw firstException;
        }
        
        HashJoinInfo.serializeHashJoinIntoScan(scan, joinInfo);
        
        return plan.iterator(dependencies);
    }
    
    @Override
    public long getEstimatedSize() {
        return plan.getEstimatedSize();
    }

    @Override
    public List<KeyRange> getSplits() {
        return plan.getSplits();
    }

    @Override
    public ExplainPlan getExplainPlan() throws SQLException {
        List<String> mainQuerySteps = plan.getExplainPlan().getPlanSteps();
        List<String> planSteps = Lists.newArrayList(mainQuerySteps);
        int count = hashPlans.length;
        planSteps.add("    PARALLEL EQUI-JOIN " + count + " HASH TABLES:");
        for (int i = 0; i < count; i++) {
            boolean earlyEvaluation = joinInfo.earlyEvaluation()[i];
            boolean skipMerge = joinInfo.getSchemas()[i].getFieldCount() == 0;
        	planSteps.add("    BUILD HASH TABLE " + i + (earlyEvaluation ? "" : "(DELAYED EVALUATION)") + (skipMerge ? " (SKIP MERGE)" : ""));
        	List<String> steps = hashPlans[i].getExplainPlan().getPlanSteps();
        	for (String step : steps) {
        		planSteps.add("        " + step);
        	}
        }
        if (joinInfo.getPostJoinFilterExpression() != null) {
        	planSteps.add("    AFTER-JOIN SERVER FILTER BY " + joinInfo.getPostJoinFilterExpression().toString());
        }
        
        return new ExplainPlan(planSteps);
    }

    @Override
    public ParameterMetaData getParameterMetaData() {
        return plan.getParameterMetaData();
    }

    @Override
    public StatementContext getContext() {
        return plan.getContext();
    }

    @Override
    public GroupBy getGroupBy() {
        return plan.getGroupBy();
    }

    @Override
    public TableRef getTableRef() {
        return plan.getTableRef();
    }

    @Override
    public FilterableStatement getStatement() {
        return plan.getStatement();
    }

    @Override
    public boolean isDegenerate() {
        // TODO can we determine this won't return anything?
        return false;
    }

}

