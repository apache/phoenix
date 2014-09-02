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
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.cache.ServerCacheClient.ServerCache;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.WhereCompiler;
import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.expression.ComparisonExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.InListExpression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.job.JobManager.JobCallable;
import org.apache.phoenix.join.HashCacheClient;
import org.apache.phoenix.join.HashJoinInfo;
import org.apache.phoenix.join.TupleProjector;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.SQLCloseable;
import org.apache.phoenix.util.SQLCloseables;

import com.google.common.collect.Lists;

public class HashJoinPlan implements QueryPlan {
    private static final Log LOG = LogFactory.getLog(HashJoinPlan.class);

    private final FilterableStatement statement;
    private final BaseQueryPlan plan;
    private final HashJoinInfo joinInfo;
    private final List<Expression>[] hashExpressions;
    private final Expression[] keyRangeLhsExpressions;
    private final Expression[] keyRangeRhsExpressions;
    private final QueryPlan[] hashPlans;
    private final TupleProjector[] clientProjectors;
    private final boolean[] hasFilters;
    private final boolean forceHashJoinRangeScan;
    private final boolean forceHashJoinSkipScan;

    public HashJoinPlan(FilterableStatement statement, 
            BaseQueryPlan plan, HashJoinInfo joinInfo,
            List<Expression>[] hashExpressions, Expression[] keyRangeLhsExpressions,
            Expression[] keyRangeRhsExpressions, QueryPlan[] hashPlans, 
            TupleProjector[] clientProjectors, boolean[] hasFilters) {
        this.statement = statement;
        this.plan = plan;
        this.joinInfo = joinInfo;
        this.hashExpressions = hashExpressions;
        this.keyRangeLhsExpressions = keyRangeLhsExpressions;
        this.keyRangeRhsExpressions = keyRangeRhsExpressions;
        this.hashPlans = hashPlans;
        this.clientProjectors = clientProjectors;
        this.hasFilters = hasFilters;
        this.forceHashJoinRangeScan = plan.getStatement().getHint().hasHint(Hint.RANGE_SCAN_HASH_JOIN);
        this.forceHashJoinSkipScan = plan.getStatement().getHint().hasHint(Hint.SKIP_SCAN_HASH_JOIN);
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
        List<Expression> keyRangeExpressions = new ArrayList<Expression>();
        @SuppressWarnings("unchecked")
        final List<ImmutableBytesWritable>[] keyRangeRhsValues = new List[count];  
        final int maxServerCacheTimeToLive = services.getProps().getInt(QueryServices.MAX_SERVER_CACHE_TIME_TO_LIVE_MS_ATTRIB, QueryServicesOptions.DEFAULT_MAX_SERVER_CACHE_TIME_TO_LIVE_MS);
        final AtomicLong firstJobEndTime = new AtomicLong(0);
        SQLException firstException = null;
        for (int i = 0; i < count; i++) {
            final int index = i;
            if (keyRangeRhsExpressions[index] != null) {
                keyRangeRhsValues[index] = new ArrayList<ImmutableBytesWritable>();
            }
            futures.add(executor.submit(new JobCallable<ServerCache>() {

                @Override
                public ServerCache call() throws Exception {
                    QueryPlan hashPlan = hashPlans[index];
                    ServerCache cache = hashClient.addHashCache(ranges, hashPlan.iterator(), 
                            clientProjectors[index], hashPlan.getEstimatedSize(), hashExpressions[index], plan.getTableRef(), keyRangeRhsExpressions[index], keyRangeRhsValues[index]);
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
                if (keyRangeRhsExpressions[i] != null) {
                    keyRangeExpressions.add(createKeyRangeExpression(keyRangeLhsExpressions[i], keyRangeRhsExpressions[i], keyRangeRhsValues[i], plan.getContext().getTempPtr(), hasFilters[i]));
                }
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
        if (!keyRangeExpressions.isEmpty()) {
            StatementContext context = plan.getContext();
            PTable table = context.getCurrentTable().getTable();
            ParseNode viewWhere = table.getViewStatement() == null ? null : new SQLParser(table.getViewStatement()).parseQuery().getWhere();
            context.setResolver(FromCompiler.getResolverForQuery((SelectStatement) (plan.getStatement()), plan.getContext().getConnection()));
            WhereCompiler.compile(plan.getContext(), plan.getStatement(), viewWhere, keyRangeExpressions, true);
        }

        return plan.iterator(dependencies);
    }

    private Expression createKeyRangeExpression(Expression lhsExpression,
            Expression rhsExpression, List<ImmutableBytesWritable> rhsValues, 
            ImmutableBytesWritable ptr, boolean hasFilters) throws SQLException {
        if (rhsValues.isEmpty())
            return LiteralExpression.newConstant(null, PDataType.BOOLEAN, true);
        
        PDataType type = rhsExpression.getDataType();
        if (!useInClause(hasFilters)) {
            ImmutableBytesWritable minValue = rhsValues.get(0);
            ImmutableBytesWritable maxValue = rhsValues.get(0);
            for (ImmutableBytesWritable value : rhsValues) {
                if (value.compareTo(minValue) < 0) {
                    minValue = value;
                }
                if (value.compareTo(maxValue) > 0) {
                    maxValue = value;
                }
            }
            
            if (minValue.equals(maxValue))
                return ComparisonExpression.create(CompareOp.EQUAL, Lists.newArrayList(lhsExpression, LiteralExpression.newConstant(type.toObject(minValue), type)), ptr);
            
            return AndExpression.create(Lists.newArrayList(
                    ComparisonExpression.create(CompareOp.GREATER_OR_EQUAL, Lists.newArrayList(lhsExpression, LiteralExpression.newConstant(type.toObject(minValue), type)), ptr), 
                    ComparisonExpression.create(CompareOp.LESS_OR_EQUAL, Lists.newArrayList(lhsExpression, LiteralExpression.newConstant(type.toObject(maxValue), type)), ptr)));
        }
        
        List<Expression> children = Lists.newArrayList(lhsExpression);
        for (ImmutableBytesWritable value : rhsValues) {
            children.add(LiteralExpression.newConstant(type.toObject(value), type));
        }
        
        return InListExpression.create(children, false, ptr);
    }
    
    private boolean useInClause(boolean hasFilters) {
        return this.forceHashJoinSkipScan || (!this.forceHashJoinRangeScan && hasFilters);
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
        String dynamicFilters = null;
        int filterCount = 0;
        for (int i = 0; i < count; i++) {
            if (keyRangeLhsExpressions[i] != null) {
                if (filterCount == 1) {
                    dynamicFilters = "(" + dynamicFilters + ")";
                }
                String filter = keyRangeLhsExpressions[i].toString() 
                        + (useInClause(hasFilters[i]) ? " IN " : " BETWEEN MIN/MAX OF ") 
                        + "(" + keyRangeRhsExpressions[i].toString() + ")";
                dynamicFilters = dynamicFilters == null ? filter : (dynamicFilters + " AND (" + filter + ")");
                filterCount++;
            }
        }
        if (dynamicFilters != null) {
            planSteps.add("    DYNAMIC SERVER FILTER BY " + dynamicFilters);
        }
        if (joinInfo.getPostJoinFilterExpression() != null) {
            planSteps.add("    AFTER-JOIN SERVER FILTER BY " + joinInfo.getPostJoinFilterExpression().toString());
        }
        if (joinInfo.getLimit() != null) {
            planSteps.add("    JOIN-SCANNER " + joinInfo.getLimit() + " ROW LIMIT");
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
        return statement;
    }

    @Override
    public boolean isDegenerate() {
        return false;
    }

}

