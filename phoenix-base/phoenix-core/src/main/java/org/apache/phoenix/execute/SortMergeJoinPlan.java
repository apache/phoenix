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

import static org.apache.phoenix.util.NumberUtil.add;
import static org.apache.phoenix.util.NumberUtil.getMin;

import java.io.IOException;
import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.compile.ExplainPlanAttributes
    .ExplainPlanAttributesBuilder;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.QueryCompiler;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.StatelessExpressionCompiler;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.exception.PhoenixIOException;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.TupleProjector.ProjectedValueTuple;
import org.apache.phoenix.execute.visitor.ByteCountVisitor;
import org.apache.phoenix.execute.visitor.QueryPlanVisitor;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.OrderByExpression;
import org.apache.phoenix.iterate.DefaultParallelScanGrouper;
import org.apache.phoenix.iterate.ParallelScanGrouper;
import org.apache.phoenix.iterate.PhoenixQueues;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.iterate.SizeAwareQueue;
import org.apache.phoenix.jdbc.PhoenixParameterMetaData;
import org.apache.phoenix.jdbc.PhoenixStatement.Operation;
import org.apache.phoenix.optimize.Cost;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.parse.OrderByNode;
import org.apache.phoenix.parse.JoinTableNode.JoinType;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.KeyValueSchema;
import org.apache.phoenix.schema.KeyValueSchema.KeyValueSchemaBuilder;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.ValueBitSet;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Sets;

public class SortMergeJoinPlan implements QueryPlan {
    private static final Logger LOGGER = LoggerFactory.getLogger(SortMergeJoinPlan.class);
    private static final byte[] EMPTY_PTR = new byte[0];
    
    private final StatementContext context;
    private final FilterableStatement statement;
    private final TableRef table;
    /**
     * In {@link QueryCompiler#compileJoinQuery},{@link JoinType#Right} is converted
     * to {@link JoinType#Left}.
     */
    private final JoinType joinType;
    private final QueryPlan lhsPlan;
    private final QueryPlan rhsPlan;
    private final List<Expression> lhsKeyExpressions;
    private final List<Expression> rhsKeyExpressions;
    private final KeyValueSchema joinedSchema;
    private final KeyValueSchema lhsSchema;
    private final KeyValueSchema rhsSchema;
    private final int rhsFieldPosition;
    private final boolean isSingleValueOnly;
    private final Set<TableRef> tableRefs;
    private final long thresholdBytes;
    private final boolean spoolingEnabled;
    private Long estimatedBytes;
    private Long estimatedRows;
    private Long estimateInfoTs;
    private boolean getEstimatesCalled;
    private List<OrderBy> actualOutputOrderBys;

    public SortMergeJoinPlan(
            StatementContext context,
            FilterableStatement statement,
            TableRef table,
            JoinType type,
            QueryPlan lhsPlan,
            QueryPlan rhsPlan,
            Pair<List<Expression>,List<Expression>> lhsAndRhsKeyExpressions,
            List<Expression> rhsKeyExpressions,
            PTable joinedTable,
            PTable lhsTable,
            PTable rhsTable,
            int rhsFieldPosition,
            boolean isSingleValueOnly,
            Pair<List<OrderByNode>,List<OrderByNode>> lhsAndRhsOrderByNodes) throws SQLException {
        if (type == JoinType.Right) throw new IllegalArgumentException("JoinType should not be " + type);
        this.context = context;
        this.statement = statement;
        this.table = table;
        this.joinType = type;
        this.lhsPlan = lhsPlan;
        this.rhsPlan = rhsPlan;
        this.lhsKeyExpressions = lhsAndRhsKeyExpressions.getFirst();
        this.rhsKeyExpressions = lhsAndRhsKeyExpressions.getSecond();
        this.joinedSchema = buildSchema(joinedTable);
        this.lhsSchema = buildSchema(lhsTable);
        this.rhsSchema = buildSchema(rhsTable);
        this.rhsFieldPosition = rhsFieldPosition;
        this.isSingleValueOnly = isSingleValueOnly;
        this.tableRefs = Sets.newHashSetWithExpectedSize(lhsPlan.getSourceRefs().size() + rhsPlan.getSourceRefs().size());
        this.tableRefs.addAll(lhsPlan.getSourceRefs());
        this.tableRefs.addAll(rhsPlan.getSourceRefs());
        this.thresholdBytes =
                context.getConnection().getQueryServices().getProps().getLong(
                    QueryServices.CLIENT_SPOOL_THRESHOLD_BYTES_ATTRIB,
                    QueryServicesOptions.DEFAULT_CLIENT_SPOOL_THRESHOLD_BYTES);
        this.spoolingEnabled =
                context.getConnection().getQueryServices().getProps().getBoolean(
                    QueryServices.CLIENT_JOIN_SPOOLING_ENABLED_ATTRIB,
                    QueryServicesOptions.DEFAULT_CLIENT_JOIN_SPOOLING_ENABLED);
        this.actualOutputOrderBys = convertActualOutputOrderBy(lhsAndRhsOrderByNodes.getFirst(), lhsAndRhsOrderByNodes.getSecond(), context);
    }

    @Override
    public Operation getOperation() {
        return statement.getOperation();
    }

    private static KeyValueSchema buildSchema(PTable table) {
        KeyValueSchemaBuilder builder = new KeyValueSchemaBuilder(0);
        if (table != null) {
            for (PColumn column : table.getColumns()) {
                if (!SchemaUtil.isPKColumn(column)) {
                    builder.addField(column);
                }
            }
        }
        return builder.build();
    }

    @Override
    public ResultIterator iterator(ParallelScanGrouper scanGrouper) throws SQLException {
        return iterator(scanGrouper, null);
    }

    @Override
    public ResultIterator iterator(ParallelScanGrouper scanGrouper, Scan scan) throws SQLException {        
        return joinType == JoinType.Semi || joinType == JoinType.Anti ?
                new SemiAntiJoinIterator(lhsPlan.iterator(scanGrouper), rhsPlan.iterator(scanGrouper)) :
                new BasicJoinIterator(lhsPlan.iterator(scanGrouper), rhsPlan.iterator(scanGrouper));
    }
    
    @Override
    public ResultIterator iterator() throws SQLException {        
        return iterator(DefaultParallelScanGrouper.getInstance());
    }

    @Override
    public ExplainPlan getExplainPlan() throws SQLException {
        List<String> steps = Lists.newArrayList();
        steps.add("SORT-MERGE-JOIN (" + joinType.toString().toUpperCase() + ") TABLES");
        ExplainPlan lhsExplainPlan = lhsPlan.getExplainPlan();
        List<String> lhsPlanSteps = lhsExplainPlan.getPlanSteps();
        ExplainPlanAttributes lhsPlanAttributes =
          lhsExplainPlan.getPlanStepsAsAttributes();
        ExplainPlanAttributesBuilder lhsPlanBuilder =
          new ExplainPlanAttributesBuilder(lhsPlanAttributes);
        lhsPlanBuilder.setAbstractExplainPlan("SORT-MERGE-JOIN ("
          + joinType.toString().toUpperCase() + ")");

        for (String step : lhsPlanSteps) {
            steps.add("    " + step);
        }
        steps.add("AND" + (rhsSchema.getFieldCount() == 0 ? " (SKIP MERGE)" : ""));

        ExplainPlan rhsExplainPlan = rhsPlan.getExplainPlan();
        List<String> rhsPlanSteps = rhsExplainPlan.getPlanSteps();
        ExplainPlanAttributes rhsPlanAttributes =
          rhsExplainPlan.getPlanStepsAsAttributes();
        ExplainPlanAttributesBuilder rhsPlanBuilder =
          new ExplainPlanAttributesBuilder(rhsPlanAttributes);

        lhsPlanBuilder.setRhsJoinQueryExplainPlan(rhsPlanBuilder.build());

        for (String step : rhsPlanSteps) {
            steps.add("    " + step);
        }
        return new ExplainPlan(steps, lhsPlanBuilder.build());
    }

    @Override
    public Cost getCost() {
        Double byteCount = this.accept(new ByteCountVisitor());

        if (byteCount == null) {
            return Cost.UNKNOWN;
        }

        Cost cost = new Cost(0, 0, byteCount);
        return cost.plus(lhsPlan.getCost()).plus(rhsPlan.getCost());
    }

    @Override
    public StatementContext getContext() {
        return context;
    }

    @Override
    public ParameterMetaData getParameterMetaData() {
        return context.getBindManager().getParameterMetaData();
    }

    @Override
    public long getEstimatedSize() {
        return lhsPlan.getEstimatedSize() + rhsPlan.getEstimatedSize();
    }

    @Override
    public TableRef getTableRef() {
        return table;
    }

    @Override
    public RowProjector getProjector() {
        return null;
    }

    @Override
    public Integer getLimit() {
        return null;
    }

    @Override
    public Integer getOffset() {
        return null;
    }

    @Override
    public OrderBy getOrderBy() {
        return null;
    }

    @Override
    public GroupBy getGroupBy() {
        return null;
    }

    @Override
    public List<KeyRange> getSplits() {
        return Collections.<KeyRange> emptyList();
    }

    @Override
    public List<List<Scan>> getScans() {
        return Collections.<List<Scan>> emptyList();
    }

    @Override
    public FilterableStatement getStatement() {
        return statement;
    }

    @Override
    public boolean isDegenerate() {
        return false;
    }

    @Override
    public boolean isRowKeyOrdered() {
        return false;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    private static SQLException closeIterators(ResultIterator lhsIterator, ResultIterator rhsIterator) {
        SQLException e = null;
        try {
            lhsIterator.close();
        } catch (Throwable e1) {
            e = e1 instanceof SQLException ? (SQLException)e1 : new SQLException(e1);
        }
        try {
            rhsIterator.close();
        } catch (Throwable e2) {
            SQLException e22 = e2 instanceof SQLException ? (SQLException)e2 : new SQLException(e2);
            if (e != null) {
                e.setNextException(e22);
            } else {
                e = e22;
            }
        }
        return e;
    }

    /**
     * close the futures and threadPoolExecutor,ignore exception.
     * @param threadPoolExecutor
     * @param futures
     */
    private static void clearThreadPoolExecutor(
            ExecutorService threadPoolExecutor,
            List<Future<Boolean>> futures) {
        for(Future<?> future : futures) {
            try {
                future.cancel(true);
            } catch(Throwable ignore) {
                LOGGER.error("cancel future error", ignore);
            }
        }

        try {
            threadPoolExecutor.shutdownNow();
        } catch(Throwable ignore) {
            LOGGER.error("shutdownNow threadPoolExecutor error", ignore);
        }
    }

    @VisibleForTesting
    public class BasicJoinIterator implements ResultIterator {
        private final ResultIterator lhsIterator;
        private final ResultIterator rhsIterator;
        private boolean initialized;
        private Tuple lhsTuple;
        private Tuple rhsTuple;
        private JoinKey lhsKey;
        private JoinKey rhsKey;
        private Tuple nextLhsTuple;
        private Tuple nextRhsTuple;
        private JoinKey nextLhsKey;
        private JoinKey nextRhsKey;
        private ValueBitSet destBitSet;
        private ValueBitSet lhsBitSet;
        private ValueBitSet rhsBitSet;
        private byte[] emptyProjectedValue;
        private SizeAwareQueue<Tuple> queue;
        private Iterator<Tuple> queueIterator;
        private boolean joinResultNullBecauseOneSideNull = false;

        public BasicJoinIterator(ResultIterator lhsIterator, ResultIterator rhsIterator) {
            this.lhsIterator = lhsIterator;
            this.rhsIterator = rhsIterator;
            this.initialized = false;
            this.lhsTuple = null;
            this.rhsTuple = null;
            this.lhsKey = new JoinKey(lhsKeyExpressions);
            this.rhsKey = new JoinKey(rhsKeyExpressions);
            this.nextLhsTuple = null;
            this.nextRhsTuple = null;
            this.nextLhsKey = new JoinKey(lhsKeyExpressions);
            this.nextRhsKey = new JoinKey(rhsKeyExpressions);
            this.destBitSet = ValueBitSet.newInstance(joinedSchema);
            this.lhsBitSet = ValueBitSet.newInstance(lhsSchema);
            this.rhsBitSet = ValueBitSet.newInstance(rhsSchema);
            lhsBitSet.clear();
            int len = lhsBitSet.getEstimatedLength();
            this.emptyProjectedValue = new byte[len];
            lhsBitSet.toBytes(emptyProjectedValue, 0);
            this.queue = PhoenixQueues.newTupleQueue(spoolingEnabled, thresholdBytes);
            this.queueIterator = null;
        }

        public boolean isJoinResultNullBecauseOneSideNull() {
            return this.joinResultNullBecauseOneSideNull;
        }

        public boolean isInitialized() {
            return this.initialized;
        }

        @Override
        public void close() throws SQLException {
            SQLException sqlException = closeIterators(lhsIterator, rhsIterator);
            try {
              queue.close();
            } catch (IOException t) {
              if (sqlException != null) {
                    sqlException.setNextException(
                        new SQLException("Also encountered exception while closing queue", t));
              } else {
                sqlException = new SQLException("Error while closing queue",t);
              }
            }
            if (sqlException != null) {
                LOGGER.error("BasicJoinIterator close error!", sqlException);
            }
        }

        @Override
        public Tuple next() throws SQLException {
            if (!initialized) {
                init();
            }

            if(this.joinResultNullBecauseOneSideNull) {
                return null;
            }

            Tuple next = null;
            while (next == null && !isEnd()) {
                if (queueIterator != null) {
                    if (queueIterator.hasNext()) {
                        next = join(lhsTuple, queueIterator.next());
                    } else {
                        boolean eq = nextLhsTuple != null && lhsKey.equals(nextLhsKey);
                        advance(true);
                        if (eq) {
                            queueIterator = queue.iterator();
                        } else {
                            queue.clear();
                            queueIterator = null;
                        }
                    }
                } else if (lhsTuple != null) {
                    if (rhsTuple != null) {
                        if (lhsKey.equals(rhsKey)) {
                            next = join(lhsTuple, rhsTuple);
                             if (nextLhsTuple != null && lhsKey.equals(nextLhsKey)) {
                                try {
                                    queue.add(rhsTuple);
                                } catch (IllegalStateException e) {
                                    throw new PhoenixIOException(e);
                                }
                                if (nextRhsTuple == null || !rhsKey.equals(nextRhsKey)) {
                                    queueIterator = queue.iterator();
                                    advance(true);
                                } else if (isSingleValueOnly) {
                                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.SINGLE_ROW_SUBQUERY_RETURNS_MULTIPLE_ROWS).build().buildException();
                                }
                            } else if (nextRhsTuple == null || !rhsKey.equals(nextRhsKey)) {
                                advance(true);
                            } else if (isSingleValueOnly) {
                                throw new SQLExceptionInfo.Builder(SQLExceptionCode.SINGLE_ROW_SUBQUERY_RETURNS_MULTIPLE_ROWS).build().buildException();                                
                            }
                            advance(false);
                        } else if (lhsKey.compareTo(rhsKey) < 0) {
                            if (joinType == JoinType.Full || joinType == JoinType.Left) {
                                next = join(lhsTuple, null);
                            }
                            advance(true);
                        } else {
                            if (joinType == JoinType.Full) {
                                next = join(null, rhsTuple);
                            }
                            advance(false);
                        }
                    } else { // left-join or full-join
                        next = join(lhsTuple, null);
                        advance(true);
                    }
                } else { // full-join
                    next = join(null, rhsTuple);
                    advance(false);
                }
            }

            return next;
        }

        @Override
        public void explain(List<String> planSteps) {
        }

        @Override
        public void explain(List<String> planSteps,
                ExplainPlanAttributesBuilder explainPlanAttributesBuilder) {
        }

        private void doInit(boolean lhs) throws SQLException {
            if(lhs) {
                nextLhsTuple = lhsIterator.next();
                if (nextLhsTuple != null) {
                    nextLhsKey.evaluate(nextLhsTuple);
                }
                advance(true);
            } else {
                nextRhsTuple = rhsIterator.next();
                if (nextRhsTuple != null) {
                    nextRhsKey.evaluate(nextRhsTuple);
                }
                advance(false);
            }
        }

        /**
         * Parallel init, when:
         * 1. {@link #lhsTuple} is null for inner join or left join.
         * 2. {@link #rhsTuple} is null for inner join.
         * we could conclude that the join result is null early, set {@link #joinResultNullBecauseOneSideNull} true.
         * @throws SQLException
         */
        private void init() throws SQLException {
            ExecutorService threadPoolExecutor = Executors.newFixedThreadPool(2);
            ExecutorCompletionService<Boolean> executorCompletionService =
                    new ExecutorCompletionService<Boolean>(threadPoolExecutor);
            List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>(2);
            futures.add(executorCompletionService.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    doInit(true);
                    return lhsTuple == null &&
                            ((joinType == JoinType.Inner) || (joinType == JoinType.Left));
                }
            }));

            futures.add(executorCompletionService.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    doInit(false);
                    return rhsTuple == null && joinType == JoinType.Inner;
                }
            }));

            try {
                Future<Boolean> future = executorCompletionService.take();
                if(future.get()) {
                    this.joinResultNullBecauseOneSideNull = true;
                    this.initialized = true;
                    return;
                }

                future = executorCompletionService.take();
                if(future.get()) {
                    this.joinResultNullBecauseOneSideNull = true;
                }
                initialized = true;
            } catch (Throwable throwable) {
                throw new SQLException("failed in init join iterators", throwable);
            } finally {
                clearThreadPoolExecutor(threadPoolExecutor, futures);
            }
        }

        private void advance(boolean lhs) throws SQLException {
            if (lhs) {
                lhsTuple = nextLhsTuple;
                lhsKey.set(nextLhsKey);
                if (lhsTuple != null) {
                    nextLhsTuple = lhsIterator.next();
                    if (nextLhsTuple != null) {
                        nextLhsKey.evaluate(nextLhsTuple);
                    } else {
                        nextLhsKey.clear();
                    }
                }
            } else {
                rhsTuple = nextRhsTuple;
                rhsKey.set(nextRhsKey);
                if (rhsTuple != null) {
                    nextRhsTuple = rhsIterator.next();
                    if (nextRhsTuple != null) {
                        nextRhsKey.evaluate(nextRhsTuple);
                    } else {
                        nextRhsKey.clear();
                    }
                }                    
            }
        }
        
        private boolean isEnd() {
            return (lhsTuple == null && (rhsTuple == null || joinType != JoinType.Full))
                    || (queueIterator == null && rhsTuple == null && joinType == JoinType.Inner);
        }        
        
        private Tuple join(Tuple lhs, Tuple rhs) throws SQLException {
            try {
                ProjectedValueTuple t = null;
                if (lhs == null) {
                    t = new ProjectedValueTuple(rhs, rhs.getValue(0).getTimestamp(), 
                            this.emptyProjectedValue, 0, this.emptyProjectedValue.length, 
                            this.emptyProjectedValue.length);
                } else if (lhs instanceof ProjectedValueTuple) {
                    t = (ProjectedValueTuple) lhs;
                } else {
                    ImmutableBytesWritable ptr = context.getTempPtr();
                    TupleProjector.decodeProjectedValue(lhs, ptr);
                    lhsBitSet.clear();
                    lhsBitSet.or(ptr);
                    int bitSetLen = lhsBitSet.getEstimatedLength();
                    t = new ProjectedValueTuple(lhs, lhs.getValue(0).getTimestamp(), 
                            ptr.get(), ptr.getOffset(), ptr.getLength(), bitSetLen);
                    
                }
                return rhsBitSet == ValueBitSet.EMPTY_VALUE_BITSET ?
                        t : TupleProjector.mergeProjectedValue(t, destBitSet,
                                rhs, rhsBitSet, rhsFieldPosition, true);
            } catch (IOException e) {
                throw new SQLException(e);
            }
        }
    }

    @VisibleForTesting
    public class SemiAntiJoinIterator implements ResultIterator {
        private final ResultIterator lhsIterator;
        private final ResultIterator rhsIterator;
        private final boolean isSemi;
        private boolean initialized;
        private Tuple lhsTuple;
        private Tuple rhsTuple;
        private JoinKey lhsKey;
        private JoinKey rhsKey;
        private boolean joinResultNullBecauseOneSideNull = false;

        public SemiAntiJoinIterator(ResultIterator lhsIterator, ResultIterator rhsIterator) {
            if (joinType != JoinType.Semi && joinType != JoinType.Anti) {
                throw new IllegalArgumentException("Type " + joinType + " is not allowed by " + SemiAntiJoinIterator.class.getName());
            }
            this.lhsIterator = lhsIterator;
            this.rhsIterator = rhsIterator;
            this.isSemi = joinType == JoinType.Semi;
            this.initialized = false;
            this.lhsTuple = null;
            this.rhsTuple = null;
            this.lhsKey = new JoinKey(lhsKeyExpressions);
            this.rhsKey = new JoinKey(rhsKeyExpressions);
        }

        public boolean isJoinResultNullBecauseOneSideNull() {
            return this.joinResultNullBecauseOneSideNull;
        }

        public boolean isInitialized() {
            return this.initialized;
        }

        @Override
        public void close() throws SQLException {
            SQLException sqlException = closeIterators(lhsIterator, rhsIterator);
            if (sqlException != null) {
                LOGGER.error("SemiAntiJoinIterator close error!", sqlException);
            }
        }

        /**
         * Parallel init, when:
         * 1. {@link #lhsTuple} is null.
         * 2. {@link #rhsTuple} is null for left semi join.
         * we could conclude that the join result is null early, set {@link #joinResultNullBecauseOneSideNull} true.
         * @throws SQLException
         */
        private void init() throws SQLException {
            ExecutorService threadPoolExecutor = Executors.newFixedThreadPool(2);
            ExecutorCompletionService<Boolean> executorCompletionService =
                    new ExecutorCompletionService<Boolean>(threadPoolExecutor);
            List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>(2);
            futures.add(executorCompletionService.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    advance(true);
                    return lhsTuple == null;
                }
            }));

            futures.add(executorCompletionService.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    advance(false);
                    return (rhsTuple == null && isSemi);
                }
            }));

            try {
                Future<Boolean> future = executorCompletionService.take();
                if(future.get()) {
                    this.joinResultNullBecauseOneSideNull = true;
                    this.initialized = true;
                    return;
                }

                future = executorCompletionService.take();
                if(future.get()) {
                    this.joinResultNullBecauseOneSideNull = true;
                }
                initialized = true;
            } catch (Throwable throwable) {
                throw new SQLException("failed in init join iterators", throwable);
            } finally {
                clearThreadPoolExecutor(threadPoolExecutor, futures);
            }
        }

        @Override
        public Tuple next() throws SQLException {
            if (!initialized) {
                init();
            }

            if(this.joinResultNullBecauseOneSideNull) {
                return null;
            }

            Tuple next = null;            
            while (next == null && !isEnd()) {
                if (rhsTuple != null) {
                    if (lhsKey.equals(rhsKey)) {
                        if (isSemi) {
                            next = lhsTuple;
                        }
                        advance(true);
                    } else if (lhsKey.compareTo(rhsKey) < 0) {
                        if (!isSemi) {
                            next = lhsTuple;
                        }
                        advance(true);
                    } else {
                        advance(false);
                    }
                } else {
                    if (!isSemi) {
                        next = lhsTuple;
                    }
                    advance(true);
                }
            }
            
            return next;
        }

        /**
         * Check if the {@link #next} could exit early when the {@link #lhsTuple}
         * or {@link #rhsTuple} is null.
         */
        @VisibleForTesting
        public boolean isEnd() {
            return (this.lhsTuple == null) ||
                   (this.rhsTuple == null && this.isSemi);
        }

        @Override
        public void explain(List<String> planSteps) {
        }

        @Override
        public void explain(List<String> planSteps,
                ExplainPlanAttributesBuilder explainPlanAttributesBuilder) {
        }

        private void advance(boolean lhs) throws SQLException {
            if (lhs) {
                lhsTuple = lhsIterator.next();
                if (lhsTuple != null) {
                    lhsKey.evaluate(lhsTuple);
                } else {
                    lhsKey.clear();
                }
            } else {
                rhsTuple = rhsIterator.next();
                if (rhsTuple != null) {
                    rhsKey.evaluate(rhsTuple);
                } else {
                    rhsKey.clear();
                }
            }
        }
    }
    
    private static class JoinKey implements Comparable<JoinKey> {
        private final List<Expression> expressions;
        private final List<ImmutableBytesWritable> keys;
        
        public JoinKey(List<Expression> expressions) {
            this.expressions = expressions;
            this.keys = Lists.newArrayListWithExpectedSize(expressions.size());
            for (int i = 0; i < expressions.size(); i++) {
                this.keys.add(new ImmutableBytesWritable(EMPTY_PTR));
            }
        }
        
        public void evaluate(Tuple tuple) {
            for (int i = 0; i < keys.size(); i++) {
                if (!expressions.get(i).evaluate(tuple, keys.get(i))) {
                    keys.get(i).set(EMPTY_PTR);
                }
            }
        }
        
        public void set(JoinKey other) {
            for (int i = 0; i < keys.size(); i++) {
                ImmutableBytesWritable key = other.keys.get(i);
                this.keys.get(i).set(key.get(), key.getOffset(), key.getLength());
            }            
        }
        
        public void clear() {
            for (int i = 0; i < keys.size(); i++) {
                this.keys.get(i).set(EMPTY_PTR);
            }            
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof JoinKey)) 
                return false;
            return this.compareTo((JoinKey) other) == 0;
        }
        
        @Override
        public int compareTo(JoinKey other) {
            for (int i = 0; i < keys.size(); i++) {
                int comp = this.keys.get(i).compareTo(other.keys.get(i));
                if (comp != 0) 
                    return comp; 
            }
            
            return 0;
        }
    }
    
    
    @Override
    public boolean useRoundRobinIterator() {
        return false;
    }

    @Override
    public <T> T accept(QueryPlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public Set<TableRef> getSourceRefs() {
        return tableRefs;
    }

    public QueryPlan getLhsPlan() {
        return lhsPlan;
    }

    public QueryPlan getRhsPlan() {
        return rhsPlan;
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
        if ((lhsPlan.getEstimatedBytesToScan() == null || rhsPlan.getEstimatedBytesToScan() == null)
                || (lhsPlan.getEstimatedRowsToScan() == null
                || rhsPlan.getEstimatedRowsToScan() == null)
                || (lhsPlan.getEstimateInfoTimestamp() == null
                || rhsPlan.getEstimateInfoTimestamp() == null)) {
            /*
             * If any of the sub plans doesn't have the estimate info available, then we don't
             * provide estimate for the overall plan
             */
            estimatedBytes = null;
            estimatedRows = null;
            estimateInfoTs = null;
        } else {
            estimatedBytes =
                    add(add(estimatedBytes, lhsPlan.getEstimatedBytesToScan()),
                            rhsPlan.getEstimatedBytesToScan());
            estimatedRows =
                    add(add(estimatedRows, lhsPlan.getEstimatedRowsToScan()),
                            rhsPlan.getEstimatedRowsToScan());
            estimateInfoTs =
                    getMin(lhsPlan.getEstimateInfoTimestamp(), rhsPlan.getEstimateInfoTimestamp());
        }
    }

    /**
     * We do not use {@link #lhsKeyExpressions} and {@link #rhsKeyExpressions} directly because {@link #lhsKeyExpressions} is compiled by the
     * {@link ColumnResolver} of lhs and {@link #rhsKeyExpressions} is compiled by the {@link ColumnResolver} of rhs, so we must recompile use
     * the {@link ColumnResolver} of joinProjectedTables.
     * @param lhsOrderByNodes
     * @param rhsOrderByNodes
     * @param statementContext
     * @return
     * @throws SQLException
     */
    private static List<OrderBy> convertActualOutputOrderBy(
            List<OrderByNode> lhsOrderByNodes,
            List<OrderByNode> rhsOrderByNodes,
            StatementContext statementContext) throws SQLException {

        List<OrderBy> orderBys = new ArrayList<OrderBy>(2);
        List<OrderByExpression> lhsOrderByExpressions =
                compileOrderByNodes(lhsOrderByNodes, statementContext);
        if(!lhsOrderByExpressions.isEmpty()) {
            orderBys.add(new OrderBy(lhsOrderByExpressions));
        }

        List<OrderByExpression> rhsOrderByExpressions =
                compileOrderByNodes(rhsOrderByNodes, statementContext);
        if(!rhsOrderByExpressions.isEmpty()) {
            orderBys.add(new OrderBy(rhsOrderByExpressions));
        }
        if(orderBys.isEmpty()) {
            return Collections.<OrderBy> emptyList();
        }
        return orderBys;
    }

    private static List<OrderByExpression> compileOrderByNodes(List<OrderByNode> orderByNodes, StatementContext statementContext) throws SQLException {
        /**
         * If there is TableNotFoundException or ColumnNotFoundException, it means that the orderByNodes is not referenced by other parts of the sql,
         * so could be ignored.
         */
        StatelessExpressionCompiler expressionCompiler = new StatelessExpressionCompiler(statementContext);
        List<OrderByExpression> orderByExpressions = new ArrayList<OrderByExpression>(orderByNodes.size());
        for(OrderByNode orderByNode : orderByNodes) {
            expressionCompiler.reset();
            Expression expression = null;
            try {
                expression = orderByNode.getNode().accept(expressionCompiler);
            } catch(TableNotFoundException exception) {
                return orderByExpressions;
            } catch(ColumnNotFoundException exception) {
                return orderByExpressions;
            } catch(ColumnFamilyNotFoundException exception) {
                return orderByExpressions;
            }
            assert expression != null;
            orderByExpressions.add(
                    OrderByExpression.createByCheckIfOrderByReverse(
                            expression,
                            orderByNode.isNullsLast(),
                            orderByNode.isAscending(),
                            false));
        }
        return orderByExpressions;
    }

    @Override
    public List<OrderBy> getOutputOrderBys() {
        return this.actualOutputOrderBys;
    }

    @Override
    public boolean isApplicable() {
        return true;
    }
}
