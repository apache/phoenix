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

import static org.apache.phoenix.util.LogUtil.addCustomAnnotations;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.cache.ServerCacheClient.ServerCache;
import org.apache.phoenix.compile.ColumnProjector;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.WhereCompiler;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.InListExpression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.RowValueConstructorExpression;
import org.apache.phoenix.iterate.FilterResultIterator;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.job.JobManager.JobCallable;
import org.apache.phoenix.join.HashCacheClient;
import org.apache.phoenix.join.HashJoinInfo;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.types.PArrayDataType;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.SQLCloseable;
import org.apache.phoenix.util.SQLCloseables;

import com.google.common.collect.Lists;

public class HashJoinPlan extends DelegateQueryPlan {
    private static final Log LOG = LogFactory.getLog(HashJoinPlan.class);

    private final SelectStatement statement;
    private final HashJoinInfo joinInfo;
    private final SubPlan[] subPlans;
    private final boolean recompileWhereClause;
    private List<SQLCloseable> dependencies;
    private HashCacheClient hashClient;
    private int maxServerCacheTimeToLive;
    private AtomicLong firstJobEndTime;
    private List<Expression> keyRangeExpressions;
    
    public static HashJoinPlan create(SelectStatement statement, 
            QueryPlan plan, HashJoinInfo joinInfo, SubPlan[] subPlans) {
        if (!(plan instanceof HashJoinPlan))
            return new HashJoinPlan(statement, plan, joinInfo, subPlans, joinInfo == null);
        
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
        return new HashJoinPlan(statement, hashJoinPlan.delegate, joinInfo, mergedSubPlans, true);
    }
    
    private HashJoinPlan(SelectStatement statement, 
            QueryPlan plan, HashJoinInfo joinInfo, SubPlan[] subPlans, boolean recompileWhereClause) {
        super(plan);
        this.statement = statement;
        this.joinInfo = joinInfo;
        this.subPlans = subPlans;
        this.recompileWhereClause = recompileWhereClause;
    }

    @Override
    public ResultIterator iterator() throws SQLException {
        int count = subPlans.length;
        PhoenixConnection connection = getContext().getConnection();
        ConnectionQueryServices services = connection.getQueryServices();
        ExecutorService executor = services.getExecutor();
        List<Future<Object>> futures = Lists.<Future<Object>>newArrayListWithExpectedSize(count);
        dependencies = Lists.newArrayList();
        if (joinInfo != null) {
            hashClient = new HashCacheClient(delegate.getContext().getConnection());
            maxServerCacheTimeToLive = services.getProps().getInt(QueryServices.MAX_SERVER_CACHE_TIME_TO_LIVE_MS_ATTRIB, QueryServicesOptions.DEFAULT_MAX_SERVER_CACHE_TIME_TO_LIVE_MS);
            firstJobEndTime = new AtomicLong(0);
            keyRangeExpressions = new CopyOnWriteArrayList<Expression>();
        }
        
        for (int i = 0; i < count; i++) {
            final int index = i;
            futures.add(executor.submit(new JobCallable<Object>() {

                @Override
                public Object call() throws Exception {
                    return subPlans[index].execute(HashJoinPlan.this);
                }

                @Override
                public Object getJobId() {
                    return HashJoinPlan.this;
                }
            }));
        }
        
        SQLException firstException = null;
        for (int i = 0; i < count; i++) {
            try {
                Object result = futures.get(i).get();
                subPlans[i].postProcess(result, this);
            } catch (InterruptedException e) {
                if (firstException == null) {
                    firstException = new SQLException("Sub plan [" + i + "] execution interrupted.", e);
                }
            } catch (ExecutionException e) {
                if (firstException == null) {
                    firstException = new SQLException("Encountered exception in sub plan [" + i + "] execution.", 
                            e.getCause());
                }
            }
        }
        if (firstException != null) {
            SQLCloseables.closeAllQuietly(dependencies);
            throw firstException;
        }
        
        Expression postFilter = null;
        boolean hasKeyRangeExpressions = keyRangeExpressions != null && !keyRangeExpressions.isEmpty();
        if (recompileWhereClause || hasKeyRangeExpressions) {
            StatementContext context = delegate.getContext();
            PTable table = context.getCurrentTable().getTable();
            ParseNode viewWhere = table.getViewStatement() == null ? null : new SQLParser(table.getViewStatement()).parseQuery().getWhere();
            context.setResolver(FromCompiler.getResolverForQuery((SelectStatement) (delegate.getStatement()), delegate.getContext().getConnection()));
            if (recompileWhereClause) {
                postFilter = WhereCompiler.compile(delegate.getContext(), delegate.getStatement(), viewWhere, null);
            }
            if (hasKeyRangeExpressions) {
                WhereCompiler.compile(delegate.getContext(), delegate.getStatement(), viewWhere, keyRangeExpressions, true, null);
            }
        }

        if (joinInfo != null) {
            Scan scan = delegate.getContext().getScan();
            HashJoinInfo.serializeHashJoinIntoScan(scan, joinInfo);
        }
        
        ResultIterator iterator = joinInfo == null ? delegate.iterator() : ((BaseQueryPlan) delegate).iterator(dependencies);
        if (statement.getInnerSelectStatement() != null && postFilter != null) {
            iterator = new FilterResultIterator(iterator, postFilter);
        }
        
        return iterator;
    }

    private Expression createKeyRangeExpression(Expression lhsExpression,
            Expression rhsExpression, List<Expression> rhsValues, 
            ImmutableBytesWritable ptr) throws SQLException {
        if (rhsValues.isEmpty())
            return LiteralExpression.newConstant(false, PBoolean.INSTANCE, Determinism.ALWAYS);        
        
        rhsValues.add(0, lhsExpression);
        
        return InListExpression.create(rhsValues, false, ptr);
    }

    @Override
    public ExplainPlan getExplainPlan() throws SQLException {
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

    protected interface SubPlan {
        public Object execute(HashJoinPlan parent) throws SQLException;
        public void postProcess(Object result, HashJoinPlan parent) throws SQLException;
        public List<String> getPreSteps(HashJoinPlan parent) throws SQLException;
        public List<String> getPostSteps(HashJoinPlan parent) throws SQLException;
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
        public Object execute(HashJoinPlan parent) throws SQLException {
            List<Object> values = Lists.<Object> newArrayList();
            ResultIterator iterator = plan.iterator();
            RowProjector projector = plan.getProjector();
            ImmutableBytesWritable ptr = new ImmutableBytesWritable();
            int columnCount = projector.getColumnCount();
            int rowCount = 0;
            PDataType baseType = null;
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
            parent.getContext().setSubqueryResult(select, result);
            return null;
        }

        @Override
        public void postProcess(Object result, HashJoinPlan parent) throws SQLException {
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
    }
    
    public static class HashSubPlan implements SubPlan {        
        private final int index;
        private final QueryPlan plan;
        private final List<Expression> hashExpressions;
        private final boolean singleValueOnly;
        private final Expression keyRangeLhsExpression;
        private final Expression keyRangeRhsExpression;
        
        public HashSubPlan(int index, QueryPlan subPlan, 
                List<Expression> hashExpressions,
                boolean singleValueOnly,
                Expression keyRangeLhsExpression, 
                Expression keyRangeRhsExpression) {
            this.index = index;
            this.plan = subPlan;
            this.hashExpressions = hashExpressions;
            this.singleValueOnly = singleValueOnly;
            this.keyRangeLhsExpression = keyRangeLhsExpression;
            this.keyRangeRhsExpression = keyRangeRhsExpression;
        }

        @Override
        public Object execute(HashJoinPlan parent) throws SQLException {
            ScanRanges ranges = parent.delegate.getContext().getScanRanges();
            List<Expression> keyRangeRhsValues = null;
            if (keyRangeRhsExpression != null) {
                keyRangeRhsValues = Lists.<Expression>newArrayList();
            }
            ServerCache cache = null;
            if (hashExpressions != null) {
                cache = parent.hashClient.addHashCache(ranges, plan.iterator(), 
                        plan.getEstimatedSize(), hashExpressions, singleValueOnly, parent.delegate.getTableRef(), keyRangeRhsExpression, keyRangeRhsValues);
                long endTime = System.currentTimeMillis();
                boolean isSet = parent.firstJobEndTime.compareAndSet(0, endTime);
                if (!isSet && (endTime - parent.firstJobEndTime.get()) > parent.maxServerCacheTimeToLive) {
                    LOG.warn(addCustomAnnotations("Hash plan [" + index + "] execution seems too slow. Earlier hash cache(s) might have expired on servers.", parent.delegate.getContext().getConnection()));
                }
            } else {
                assert(keyRangeRhsExpression != null);
                ResultIterator iterator = plan.iterator();
                for (Tuple result = iterator.next(); result != null; result = iterator.next()) {
                    // Evaluate key expressions for hash join key range optimization.
                    keyRangeRhsValues.add(HashCacheClient.evaluateKeyExpression(keyRangeRhsExpression, result, plan.getContext().getTempPtr()));
                }
            }
            if (keyRangeRhsValues != null) {
                parent.keyRangeExpressions.add(parent.createKeyRangeExpression(keyRangeLhsExpression, keyRangeRhsExpression, keyRangeRhsValues, plan.getContext().getTempPtr()));
            }
            return cache;
        }

        @Override
        public void postProcess(Object result, HashJoinPlan parent)
                throws SQLException {
            ServerCache cache = (ServerCache) result;
            if (cache != null) {
                parent.joinInfo.getJoinIds()[index].set(cache.getId());
                parent.dependencies.add(cache);
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
        
    }
}


