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

import static org.apache.phoenix.monitoring.TaskExecutionMetricsHolder.NO_OP_INSTANCE;
import static org.apache.phoenix.util.LogUtil.addCustomAnnotations;
import static org.apache.phoenix.util.NumberUtil.add;
import static org.apache.phoenix.util.NumberUtil.getMin;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.phoenix.thirdparty.com.google.common.base.Optional;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.cache.ServerCacheClient.ServerCache;
import org.apache.phoenix.compile.ColumnProjector;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.WhereCompiler;
import org.apache.phoenix.coprocessorclient.HashJoinCacheNotFoundException;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.visitor.AvgRowWidthVisitor;
import org.apache.phoenix.execute.visitor.QueryPlanVisitor;
import org.apache.phoenix.execute.visitor.RowCountVisitor;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.InListExpression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.RowValueConstructorExpression;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.iterate.*;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.job.JobManager.JobCallable;
import org.apache.phoenix.join.HashCacheClient;
import org.apache.phoenix.join.HashJoinInfo;
import org.apache.phoenix.monitoring.TaskExecutionMetricsHolder;
import org.apache.phoenix.optimize.Cost;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PArrayDataType;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.util.ClientUtil;
import org.apache.phoenix.util.CostUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.SQLCloseables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.thirdparty.com.google.common.collect.Sets;

public class HashJoinPlan extends DelegateQueryPlan {
    private static final Logger LOGGER = LoggerFactory.getLogger(HashJoinPlan.class);
    private static final Random RANDOM = new Random();

    private final SelectStatement statement;
    private final HashJoinInfo joinInfo;
    private final SubPlan[] subPlans;
    private final boolean recompileWhereClause;
    private final Set<TableRef> tableRefs;
    private final int maxServerCacheTimeToLive;
    private final long serverCacheLimit;
    private final Map<ImmutableBytesPtr,ServerCache> dependencies = Maps.newHashMap();
    private HashCacheClient hashClient;
    private AtomicLong firstJobEndTime;
    private List<Expression> keyRangeExpressions;
    private Long estimatedRows;
    private Long estimatedBytes;
    private Long estimateInfoTs;
    private boolean getEstimatesCalled;
    private boolean hasSubPlansWithPersistentCache;
    
    public static HashJoinPlan create(SelectStatement statement, 
            QueryPlan plan, HashJoinInfo joinInfo, SubPlan[] subPlans) throws SQLException {
        if (!(plan instanceof HashJoinPlan))
            return new HashJoinPlan(statement, plan, joinInfo, subPlans, joinInfo == null, Collections.<ImmutableBytesPtr,ServerCache>emptyMap());
        
        HashJoinPlan hashJoinPlan = (HashJoinPlan) plan;
        assert (hashJoinPlan.joinInfo == null && hashJoinPlan.delegate instanceof BaseQueryPlan);
        SubPlan[] mergedSubPlans = new SubPlan[hashJoinPlan.subPlans.length + subPlans.length];
        int i = 0;
        for (SubPlan subPlan : hashJoinPlan.subPlans) {
            mergedSubPlans[i++] = subPlan;
        }
        for (SubPlan subPlan : subPlans) {
            mergedSubPlans[i++] = subPlan;
        }
        return new HashJoinPlan(statement, hashJoinPlan.delegate, joinInfo, mergedSubPlans, true, hashJoinPlan.dependencies);
    }
    
    private HashJoinPlan(SelectStatement statement, 
            QueryPlan plan, HashJoinInfo joinInfo, SubPlan[] subPlans, boolean recompileWhereClause, Map<ImmutableBytesPtr,ServerCache> dependencies) throws SQLException {
        super(plan);
        this.dependencies.putAll(dependencies);
        this.statement = statement;
        this.joinInfo = joinInfo;
        this.subPlans = subPlans;
        this.recompileWhereClause = recompileWhereClause;
        this.tableRefs = Sets.newHashSetWithExpectedSize(subPlans.length + plan.getSourceRefs().size());
        this.tableRefs.addAll(plan.getSourceRefs());
        this.hasSubPlansWithPersistentCache = false;
        for (SubPlan subPlan : subPlans) {
            tableRefs.addAll(subPlan.getInnerPlan().getSourceRefs());
            if (subPlan instanceof HashSubPlan && ((HashSubPlan)subPlan).usePersistentCache) {
                this.hasSubPlansWithPersistentCache = true;
            }
        }
        QueryServices services = plan.getContext().getConnection().getQueryServices();
        this.maxServerCacheTimeToLive = services.getProps().getInt(
                QueryServices.MAX_SERVER_CACHE_TIME_TO_LIVE_MS_ATTRIB, QueryServicesOptions.DEFAULT_MAX_SERVER_CACHE_TIME_TO_LIVE_MS);
        this.serverCacheLimit = services.getProps().getLongBytes(
                QueryServices.MAX_SERVER_CACHE_SIZE_ATTRIB, QueryServicesOptions.DEFAULT_MAX_SERVER_CACHE_SIZE);
    }
    
    @Override
    public Set<TableRef> getSourceRefs() {
        return tableRefs;
    }
        
    @Override
    public ResultIterator iterator(ParallelScanGrouper scanGrouper, Scan scan) throws SQLException {
        if (scan == null) {
            scan = delegate.getContext().getScan();
        }
        
        int count = subPlans.length;
        PhoenixConnection connection = getContext().getConnection();
        ConnectionQueryServices services = connection.getQueryServices();
        ExecutorService executor = services.getExecutor();
        List<Future<ServerCache>> futures = Lists.newArrayListWithExpectedSize(count);
        if (joinInfo != null) {
            hashClient = hashClient != null ? 
                    hashClient 
                  : new HashCacheClient(delegate.getContext().getConnection());
            firstJobEndTime = new AtomicLong(0);
            keyRangeExpressions = new CopyOnWriteArrayList<Expression>();
        }
        
        for (int i = 0; i < count; i++) {
            final int index = i;
            futures.add(executor.submit(new JobCallable<ServerCache>() {

                @Override
                public ServerCache call() throws Exception {
                    ServerCache cache = subPlans[index].execute(HashJoinPlan.this);
                    return cache;
                }

                @Override
                public Object getJobId() {
                    return HashJoinPlan.this;
                }

                @Override
                public TaskExecutionMetricsHolder getTaskExecutionMetric() {
                    return NO_OP_INSTANCE;
                }
            }));
        }
        
        SQLException firstException = null;
        for (int i = 0; i < count; i++) {
            try {
                ServerCache result = futures.get(i).get();
                if (result != null) {
                    dependencies.put(new ImmutableBytesPtr(result.getId()),result);
                }
                subPlans[i].postProcess(result, this);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (firstException == null) {
                    firstException = new SQLExceptionInfo.Builder(SQLExceptionCode.INTERRUPTED_EXCEPTION).setRootCause(e).setMessage("Sub plan [" + i + "] execution interrupted.").build().buildException();
                }
            } catch (ExecutionException e) {
                if (firstException == null) {
                    firstException = new SQLException("Encountered exception in sub plan [" + i + "] execution.", 
                            e.getCause());
                }
            }
        }
        if (firstException != null) {
            SQLCloseables.closeAllQuietly(dependencies.values());
            throw firstException;
        }

        Expression postFilter = null;
        boolean hasKeyRangeExpressions = keyRangeExpressions != null && !keyRangeExpressions.isEmpty();
        if (recompileWhereClause || hasKeyRangeExpressions) {
            StatementContext context = delegate.getContext();
            // Since we are going to compile the WHERE conditions all over again, we will clear
            // the old filter, otherwise there would be conflicts and would cause PHOENIX-4692.
            context.getScan().setFilter(null);
            PTable table = context.getCurrentTable().getTable();
            ParseNode viewWhere = table.getViewStatement() == null ? null : new SQLParser(table.getViewStatement()).parseQuery().getWhere();
            context.setResolver(FromCompiler.getResolverForQuery((SelectStatement) (delegate.getStatement()), delegate.getContext().getConnection()));
            if (recompileWhereClause) {
                postFilter = WhereCompiler.compile(delegate.getContext(), delegate.getStatement(), viewWhere, null, Optional.<byte[]>absent());
            }
            if (hasKeyRangeExpressions) {
                WhereCompiler.compile(delegate.getContext(), delegate.getStatement(), viewWhere, keyRangeExpressions, null, Optional.<byte[]>absent());
            }
        }

        if (joinInfo != null) {
            HashJoinInfo.serializeHashJoinIntoScan(scan, joinInfo);
        }
        
        ResultIterator iterator = joinInfo == null ? delegate.iterator(scanGrouper, scan) : ((BaseQueryPlan) delegate).iterator(dependencies, scanGrouper, scan);
        if (statement.getInnerSelectStatement() != null && postFilter != null) {
            iterator = new FilterResultIterator(iterator, postFilter);
        }

        if (hasSubPlansWithPersistentCache) {
            return peekForPersistentCache(iterator, scanGrouper, scan);
        } else {
            return iterator;
        }
    }

    private ResultIterator peekForPersistentCache(ResultIterator iterator, ParallelScanGrouper scanGrouper, Scan scan) throws SQLException {
        // The persistent subquery is optimistic and assumes caches are present on region
        // servers. We verify that this is the case by peeking at one result. If there is
        // a cache missing exception, we retry the query with the persistent cache disabled
        // for that specific cache ID.
        PeekingResultIterator peeking = LookAheadResultIterator.wrap(iterator);
        try {
            peeking.peek();
        } catch (Exception e) {
            try {
                throw ClientUtil.parseServerException(e);
            } catch (HashJoinCacheNotFoundException e2) {
                Long cacheId = e2.getCacheId();
                if (delegate.getContext().getRetryingPersistentCache(cacheId)) {
                    throw e2;
                }
                delegate.getContext().setRetryingPersistentCache(cacheId);
                return iterator(scanGrouper, scan);
            }
        }
        return peeking;
    }

    private Expression createKeyRangeExpression(Expression lhsExpression,
            Expression rhsExpression, List<Expression> rhsValues, 
            ImmutableBytesWritable ptr, boolean rowKeyOrderOptimizable) throws SQLException {
        if (rhsValues.isEmpty())
            return LiteralExpression.newConstant(false, PBoolean.INSTANCE, Determinism.ALWAYS);        
        
        rhsValues.add(0, lhsExpression);
        
        return InListExpression.create(rhsValues, false, ptr, rowKeyOrderOptimizable);
    }

    @Override
    public ExplainPlan getExplainPlan() throws SQLException {
        // TODO : Support ExplainPlanAttributes for HashJoinPlan
        List<String> planSteps = Lists.newArrayList(delegate.getExplainPlan().getPlanSteps());
        int count = subPlans.length;
        for (int i = 0; i < count; i++) {
            planSteps.addAll(subPlans[i].getPreSteps(this));
        }
        for (int i = 0; i < count; i++) {
            planSteps.addAll(subPlans[i].getPostSteps(this));
        }
        
        if (joinInfo != null && joinInfo.getPostJoinFilterExpression() != null) {
            planSteps.add("    AFTER-JOIN SERVER FILTER BY " + joinInfo.getPostJoinFilterExpression().toString());
        }
        if (joinInfo != null && joinInfo.getLimit() != null) {
            planSteps.add("    JOIN-SCANNER " + joinInfo.getLimit() + " ROW LIMIT");
        }
        return new ExplainPlan(planSteps);
    }

    @Override
    public FilterableStatement getStatement() {
        return statement;
    }

    public HashJoinInfo getJoinInfo() {
        return joinInfo;
    }

    public SubPlan[] getSubPlans() {
        return subPlans;
    }

    @Override
    public <T> T accept(QueryPlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public Cost getCost() {
        try {
            Long r = delegate.getEstimatedRowsToScan();
            Double w = delegate.accept(new AvgRowWidthVisitor());
            if (r == null || w == null) {
                return Cost.UNKNOWN;
            }

            int parallelLevel = CostUtil.estimateParallelLevel(
                    true, getContext().getConnection().getQueryServices());

            double rowWidth = w;
            double rows = RowCountVisitor.filter(
                    r.doubleValue(),
                    RowCountVisitor.stripSkipScanFilter(
                            delegate.getContext().getScan().getFilter()));
            double bytes = rowWidth * rows;
            Cost cost = Cost.ZERO;
            double rhsByteSum = 0.0;
            for (int i = 0; i < subPlans.length; i++) {
                double lhsBytes = bytes;
                Double rhsRows = subPlans[i].getInnerPlan().accept(new RowCountVisitor());
                Double rhsWidth = subPlans[i].getInnerPlan().accept(new AvgRowWidthVisitor());
                if (rhsRows == null || rhsWidth == null) {
                    return Cost.UNKNOWN;
                }
                double rhsBytes = rhsWidth * rhsRows;
                rows = RowCountVisitor.join(rows, rhsRows, joinInfo.getJoinTypes()[i]);
                rowWidth = AvgRowWidthVisitor.join(rowWidth, rhsWidth, joinInfo.getJoinTypes()[i]);
                bytes = rowWidth * rows;
                cost = cost.plus(CostUtil.estimateHashJoinCost(
                        lhsBytes, rhsBytes, bytes, subPlans[i].hasKeyRangeExpression(), parallelLevel));
                rhsByteSum += rhsBytes;
            }

            if (rhsByteSum > serverCacheLimit) {
                return Cost.UNKNOWN;
            }

            // Calculate the cost of aggregation and ordering that is performed with the HashJoinPlan
            if (delegate instanceof AggregatePlan) {
                AggregatePlan aggPlan = (AggregatePlan) delegate;
                double rowsBeforeHaving = RowCountVisitor.aggregate(rows, aggPlan.getGroupBy());
                double rowsAfterHaving = RowCountVisitor.filter(rowsBeforeHaving, aggPlan.getHaving());
                double bytesBeforeHaving = rowWidth * rowsBeforeHaving;
                double bytesAfterHaving = rowWidth * rowsAfterHaving;
                Cost aggCost = CostUtil.estimateAggregateCost(
                        bytes, bytesBeforeHaving, aggPlan.getGroupBy(), parallelLevel);
                cost = cost.plus(aggCost);
                rows = rowsAfterHaving;
                bytes = bytesAfterHaving;
            }
            double outputRows = RowCountVisitor.limit(rows, delegate.getLimit());
            double outputBytes = rowWidth * outputRows;
            if (!delegate.getOrderBy().getOrderByExpressions().isEmpty()) {
                Cost orderByCost = CostUtil.estimateOrderByCost(
                        bytes, outputBytes, parallelLevel);
                cost = cost.plus(orderByCost);
            }

            // Calculate the cost of child nodes
            Cost lhsCost = new Cost(0, 0, r.doubleValue() * w);
            Cost rhsCost = Cost.ZERO;
            for (SubPlan subPlan : subPlans) {
                rhsCost = rhsCost.plus(subPlan.getInnerPlan().getCost());
            }
            return cost.plus(lhsCost).plus(rhsCost);
        } catch (SQLException e) {
        }
        return Cost.UNKNOWN;
    }

    public interface SubPlan {
        public ServerCache execute(HashJoinPlan parent) throws SQLException;
        public void postProcess(ServerCache result, HashJoinPlan parent) throws SQLException;
        public List<String> getPreSteps(HashJoinPlan parent) throws SQLException;
        public List<String> getPostSteps(HashJoinPlan parent) throws SQLException;
        public QueryPlan getInnerPlan();
        public boolean hasKeyRangeExpression();
    }
    
    public static class WhereClauseSubPlan implements SubPlan {
        private final QueryPlan plan;
        private final SelectStatement select;
        private final boolean expectSingleRow;
        
        public WhereClauseSubPlan(QueryPlan plan, SelectStatement select, boolean expectSingleRow) {
            this.plan = plan;
            this.select = select;
            this.expectSingleRow = expectSingleRow;
        }

        @Override
        public ServerCache execute(HashJoinPlan parent) throws SQLException {
            List<Object> values = Lists.<Object> newArrayList();
            ResultIterator iterator = plan.iterator();
            try {
                RowProjector projector = plan.getProjector();
                ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                int columnCount = projector.getColumnCount();
                int rowCount = 0;
                PDataType baseType = PVarbinary.INSTANCE;
                for (Tuple tuple = iterator.next(); tuple != null; tuple = iterator.next()) {
                    if (expectSingleRow && rowCount >= 1)
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.SINGLE_ROW_SUBQUERY_RETURNS_MULTIPLE_ROWS).build().buildException();

                    if (columnCount == 1) {
                        ColumnProjector columnProjector = projector.getColumnProjector(0);
                        baseType = columnProjector.getExpression().getDataType();
                        Object value = columnProjector.getValue(tuple, baseType, ptr);
                        values.add(value);
                    } else {
                        List<Expression> expressions = Lists.<Expression>newArrayListWithExpectedSize(columnCount);
                        for (int i = 0; i < columnCount; i++) {
                            ColumnProjector columnProjector = projector.getColumnProjector(i);
                            PDataType type = columnProjector.getExpression().getDataType();
                            Object value = columnProjector.getValue(tuple, type, ptr);
                            expressions.add(LiteralExpression.newConstant(value, type));
                        }
                        Expression expression = new RowValueConstructorExpression(expressions, true);
                        baseType = expression.getDataType();
                        expression.evaluate(null, ptr);
                        values.add(baseType.toObject(ptr));
                    }
                    rowCount++;
                }

                Object result = expectSingleRow ? (values.isEmpty() ? null : values.get(0)) : PArrayDataType.instantiatePhoenixArray(baseType, values.toArray());
                if (result != null) {
                    parent.getContext().setSubqueryResult(select, result);
                }
                return null;
            } finally {
                iterator.close();
            }
        }

        @Override
        public void postProcess(ServerCache result, HashJoinPlan parent) throws SQLException {
        }

        @Override
        public List<String> getPreSteps(HashJoinPlan parent) throws SQLException {
            List<String> steps = Lists.newArrayList();
            steps.add("    EXECUTE " + (expectSingleRow ? "SINGLE" : "MULTIPLE") + "-ROW SUBQUERY");
            for (String step : plan.getExplainPlan().getPlanSteps()) {
                steps.add("        " + step);
            }
            return steps;
        }

        @Override
        public List<String> getPostSteps(HashJoinPlan parent) throws SQLException {
            return Collections.<String>emptyList();
        }

        @Override
        public QueryPlan getInnerPlan() {
            return plan;
        }

        @Override
        public boolean hasKeyRangeExpression() {
            return false;
        }
    }
    
    public static class HashSubPlan implements SubPlan {        
        private final int index;
        private final QueryPlan plan;
        private final List<Expression> hashExpressions;
        private final boolean singleValueOnly;
        private final boolean usePersistentCache;
        private final Expression keyRangeLhsExpression;
        private final Expression keyRangeRhsExpression;
        private final MessageDigest digest;
        
        public HashSubPlan(int index, QueryPlan subPlan, 
                List<Expression> hashExpressions,
                boolean singleValueOnly,
                boolean usePersistentCache,
                Expression keyRangeLhsExpression, 
                Expression keyRangeRhsExpression) {
            this.index = index;
            this.plan = subPlan;
            this.hashExpressions = hashExpressions;
            this.singleValueOnly = singleValueOnly;
            this.usePersistentCache = usePersistentCache;
            this.keyRangeLhsExpression = keyRangeLhsExpression;
            this.keyRangeRhsExpression = keyRangeRhsExpression;
            try {
                this.digest = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public ServerCache execute(HashJoinPlan parent) throws SQLException {
            ScanRanges ranges = parent.delegate.getContext().getScanRanges();
            List<Expression> keyRangeRhsValues = null;
            if (keyRangeRhsExpression != null) {
                keyRangeRhsValues = Lists.<Expression>newArrayList();
            }
            ServerCache cache = null;
            if (hashExpressions != null) {
                ResultIterator iterator = plan.iterator();
                try {
                    final byte[] cacheId;
                    String queryString = plan.getStatement().toString().replaceAll("\\$[0-9]+", "\\$");
                    if (usePersistentCache) {
                        cacheId = Arrays.copyOfRange(digest.digest(
                            queryString.getBytes(StandardCharsets.UTF_8)), 0, 8);
                        boolean retrying = parent.delegate.getContext().getRetryingPersistentCache(Bytes.toLong(cacheId));
                        if (!retrying) {
                            try {
                                cache = parent.hashClient.createServerCache(cacheId, parent.delegate);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    } else {
                        cacheId = Bytes.toBytes(RANDOM.nextLong());
                    }
                    LOGGER.debug("Using cache ID " + Hex.encodeHexString(cacheId) +
                            " for " + queryString);
                    if (cache == null) {
                        LOGGER.debug("Making RPC to add cache " + Hex.encodeHexString(cacheId));
                        cache = parent.hashClient.addHashCache(ranges, cacheId, iterator,
                                plan.getEstimatedSize(), hashExpressions, singleValueOnly, usePersistentCache,
                                parent.delegate.getTableRef().getTable(), keyRangeRhsExpression,
                                keyRangeRhsValues);
                        long endTime = EnvironmentEdgeManager.currentTimeMillis();
                        boolean isSet = parent.firstJobEndTime.compareAndSet(0, endTime);
                        if (!isSet && (endTime
                                - parent.firstJobEndTime.get()) > parent.maxServerCacheTimeToLive) {
                            LOGGER.warn(addCustomAnnotations(
                                "Hash plan [" + index
                                        + "] execution seems too slow. Earlier" +
                                        " hash cache(s) might have expired on servers.",
                                parent.delegate.getContext().getConnection()));
                        }
                    }
                } finally {
                    iterator.close();
                }
            } else {
                assert (keyRangeRhsExpression != null);
                ResultIterator iterator = plan.iterator();
                try {
                    for (Tuple result = iterator.next(); result != null; result = iterator.next()) {
                        // Evaluate key expressions for hash join key range optimization.
                        keyRangeRhsValues.add(HashCacheClient.evaluateKeyExpression(
                            keyRangeRhsExpression, result, plan.getContext().getTempPtr()));
                    }
                } finally {
                    iterator.close();
                }
            }
            if (keyRangeRhsValues != null) {
                parent.keyRangeExpressions.add(parent.createKeyRangeExpression(keyRangeLhsExpression, keyRangeRhsExpression, keyRangeRhsValues, plan.getContext().getTempPtr(), plan.getContext().getCurrentTable().getTable().rowKeyOrderOptimizable()));
            }
            return cache;
        }

        @Override
        public void postProcess(ServerCache result, HashJoinPlan parent)
                throws SQLException {
            ServerCache cache = result;
            if (cache != null) {
                parent.joinInfo.getJoinIds()[index].set(cache.getId());
            }
        }

        @Override
        public List<String> getPreSteps(HashJoinPlan parent) throws SQLException {
            List<String> steps = Lists.newArrayList();
            boolean earlyEvaluation = parent.joinInfo.earlyEvaluation()[index];
            boolean skipMerge = parent.joinInfo.getSchemas()[index].getFieldCount() == 0;
            if (hashExpressions != null) {
                steps.add("    PARALLEL " + parent.joinInfo.getJoinTypes()[index].toString().toUpperCase()
                        + "-JOIN TABLE " + index + (earlyEvaluation ? "" : "(DELAYED EVALUATION)") + (skipMerge ? " (SKIP MERGE)" : ""));
            }
            else {
                steps.add("    SKIP-SCAN-JOIN TABLE " + index);
            }
            for (String step : plan.getExplainPlan().getPlanSteps()) {
                steps.add("        " + step);
            }
            return steps;
        }

        @Override
        public List<String> getPostSteps(HashJoinPlan parent) throws SQLException {
            if (keyRangeLhsExpression == null)
                return Collections.<String> emptyList();
            
            String step = "    DYNAMIC SERVER FILTER BY " + keyRangeLhsExpression.toString() 
                    + " IN (" + keyRangeRhsExpression.toString() + ")";
            return Collections.<String> singletonList(step);
        }


        @Override
        public QueryPlan getInnerPlan() {
            return plan;
        }

        @Override
        public boolean hasKeyRangeExpression() {
            return keyRangeLhsExpression != null;
        }
    }

    @Override
    public Long getEstimatedRowsToScan() throws SQLException {
        if (!getEstimatesCalled) {
            getEstimates();
        }
        return estimatedRows;
    }

    @Override
    public Long getEstimatedBytesToScan() throws SQLException {
        if (!getEstimatesCalled) {
            getEstimates();
        }
        return estimatedBytes;
    }

    @Override
    public Long getEstimateInfoTimestamp() throws SQLException {
        if (!getEstimatesCalled) {
            getEstimates();
        }
        return estimateInfoTs;
    }

    private void getEstimates() throws SQLException {
        getEstimatesCalled = true;
        for (SubPlan subPlan : subPlans) {
            if (subPlan.getInnerPlan().getEstimatedBytesToScan() == null
                    || subPlan.getInnerPlan().getEstimatedRowsToScan() == null
                    || subPlan.getInnerPlan().getEstimateInfoTimestamp() == null) {
                /*
                 * If any of the sub plans doesn't have the estimate info available, then we don't
                 * provide estimate for the overall plan
                 */
                estimatedBytes = null;
                estimatedRows = null;
                estimateInfoTs = null;
                break;
            } else {
                estimatedBytes =
                        add(estimatedBytes, subPlan.getInnerPlan().getEstimatedBytesToScan());
                estimatedRows = add(estimatedRows, subPlan.getInnerPlan().getEstimatedRowsToScan());
                estimateInfoTs =
                        getMin(estimateInfoTs, subPlan.getInnerPlan().getEstimateInfoTimestamp());
            }
        }
    }
}


