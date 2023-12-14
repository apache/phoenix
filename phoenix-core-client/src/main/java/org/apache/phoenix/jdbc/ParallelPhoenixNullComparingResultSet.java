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
package org.apache.phoenix.jdbc;

import static org.apache.phoenix.exception.SQLExceptionCode.CLASS_NOT_UNWRAPPABLE;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.monitoring.MetricType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * ResultSet suitable for truly immutable use cases that do not delete data and do not query data the does not exist.
 * Returns a non-nullvalue when possible. Checks result from both the underlying clusters for nulls(empty result). If
 * we get an empty result from one cluster but are unable get a result from the other cluster we
 * either return a null that we got or throw an error based on {@link #ERROR_ON_SINGLE_NULL_ATTRIB}
 * This gives some additional consistency for this specific case at the cost of some latency and behavior
 */
public class ParallelPhoenixNullComparingResultSet extends DelegateResultSet implements PhoenixMonitoredResultSet {
    // Keeping this separate from ParallelPhoenixResultSet to allow separate evolution of the
    // two classes' behavior, keeping PPRS as the default implementation

    public static final String ERROR_ON_SINGLE_NULL_ATTRIB =
            "phoenix.parallel.nullComparingRs.errorOnSingleNull";
    public static final String DEFAULT_ERROR_ON_SINGLE_NULL = "false";

    private static final Logger LOG =
            LoggerFactory.getLogger(ParallelPhoenixNullComparingResultSet.class);

    private final CompletableFuture<ResultSet> rs1, rs2;
    private final ParallelPhoenixContext context;
    private boolean errorOnSingleNull = true;

    /**
     * @param context
     * @param rs1 CompletableFuture<ResultSet> from the Active cluster
     * @param rs2 CompletableFuture<ResultSet> from the Standby cluster
     */
    public ParallelPhoenixNullComparingResultSet(ParallelPhoenixContext context,
            CompletableFuture<ResultSet> rs1, CompletableFuture<ResultSet> rs2) {
        super(null);
        this.rs1 = rs1;
        this.rs2 = rs2;
        this.context = context;
        this.errorOnSingleNull =
                Boolean.valueOf(context.getProperties().getProperty(ERROR_ON_SINGLE_NULL_ATTRIB,
                    DEFAULT_ERROR_ON_SINGLE_NULL));
    }

    @Override
    public boolean next() throws SQLException {
        context.checkOpen();
        // First call to next
        if (this.rs == null) {
            Function<ResultSet, Boolean> function = (T) -> {
                try {
                    return T.next();
                } catch (SQLException exception) {
                    throw new CompletionException(exception);
                }
            };
            CompletableFuture<Boolean> candidate1 =
                    ParallelPhoenixUtil.INSTANCE.getFutureAndChainOnContext(function, rs1,
                        context::chainOnConn1,
                        context.getParallelPhoenixMetrics().getActiveClusterOperationCount(),
                        context.getParallelPhoenixMetrics().getActiveClusterFailedOperationCount());
            CompletableFuture<Boolean> candidate2 =
                    ParallelPhoenixUtil.INSTANCE.getFutureAndChainOnContext(function, rs2,
                        context::chainOnConn2, context.getParallelPhoenixMetrics().getStandbyClusterOperationCount(),
                        context.getParallelPhoenixMetrics().getStandbyClusterFailedOperationCount());
            List<CompletableFuture<? extends Object>> candidates = new ArrayList<>();
            candidates.add(candidate1);
            candidates.add(candidate2);
            boolean notEmpty =
                    (boolean) ParallelPhoenixUtil.INSTANCE.getAnyOfNonExceptionally(candidates, context);
            CandidateResult<Boolean> candidateResult1 = new CandidateResult<>(candidate1, rs1, true);
            CandidateResult<Boolean> candidateResult2 = new CandidateResult<>(candidate2, rs2, false);
            try {
                if (notEmpty) {
                    // Non empty result. Bind to resultset that gave us non empty result
                    bindToNonEmptyCompletedResultSet(candidateResult1, candidateResult2);
                    return true;
                } else {
                    // We got an empty result. Wait for both the responses
                    Pair<CandidateResult<Boolean>, CandidateResult<Boolean>> candidateResultPair =
                            findFirstNonExceptionallyCompletedCandidateResult(candidateResult1,
                                candidateResult2);
                    boolean firstResult = candidateResultPair.getFirst().getCandidate().get();
                    // If first result is not empty
                    if (firstResult) {
                        this.rs = candidateResultPair.getFirst().getRs().get();
                        logIfTraceEnabled(candidateResultPair.getFirst());
                        incrementClusterUsedCount(candidateResultPair.getFirst());
                        return true;
                    }
                    // First result is empty, check the second
                    boolean secondResult;
                    try {
                        secondResult =
                                ParallelPhoenixUtil.INSTANCE.getFutureNoRetry(
                                    candidateResultPair.getSecond().getCandidate(), context);
                    } catch (Exception e) {
                        LOG.warn(
                            "Exception while trying to read from other cluster after getting empty result from "
                                    + "one cluster, errorOnSingleNull: " + errorOnSingleNull,
                            e);
                        // We can't get the secondResult, check property and error if set
                        if (errorOnSingleNull) {
                            context.setError();
                            throw new SQLExceptionInfo.Builder(
                                    SQLExceptionCode.HA_READ_FROM_CLUSTER_FAILED_ON_NULL)
                                            .setRootCause(e)
                                            .setHaGroupInfo(
                                                context.getHaGroup().getGroupInfo().toString())
                                            .build().buildException();
                        }
                        this.rs = candidateResultPair.getFirst().getRs().get();
                        logIfTraceEnabled(candidateResultPair.getFirst());
                        incrementClusterUsedCount(candidateResultPair.getFirst());
                        return false;
                    }
                    // TODO: track which rs came back first and is potentially faster. Bind accordingly
                    this.rs = candidateResultPair.getSecond().getRs().get();
                    logIfTraceEnabled(candidateResultPair.getSecond());
                    incrementClusterUsedCount(candidateResultPair.getSecond());
                    return secondResult;
                }
            } catch (InterruptedException | ExecutionException e) {
                // This should never happen
                LOG.error("Unexpected exception:", e);
                context.setError();
                throw new SQLException(e);
            }
        }
        return rs.next();
    }

    private Object runOnResultSets(Function<ResultSet, ?> function) throws SQLException {
        return ParallelPhoenixUtil.INSTANCE.runFutures(function, rs1, rs2, context, true);
    }

    /**
     * binds the delegate resultSet to the ResultSet from the candidate that completed without
     * exception and is not empty(returned true on the next() call)
     * @param candidateResult1
     * @param candidateResult2
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws SQLException
     */
    private void bindToNonEmptyCompletedResultSet(CandidateResult<Boolean> candidateResult1,
            CandidateResult<Boolean> candidateResult2)
            throws InterruptedException, ExecutionException, SQLException {
        CompletableFuture<Boolean> candidate1 = candidateResult1.getCandidate();
        CompletableFuture<Boolean> candidate2 = candidateResult2.getCandidate();
        if (candidate1.isDone() && !candidate1.isCompletedExceptionally()
                && candidate1.get()) {
            this.rs = candidateResult1.getRs().get();
            logIfTraceEnabled(candidateResult1);
            incrementClusterUsedCount(candidateResult1);
        } else if (candidate2.isDone() && !candidate2.isCompletedExceptionally()
                && candidate2.get()) {
            this.rs = candidateResult2.getRs().get();
            logIfTraceEnabled(candidateResult2);
            incrementClusterUsedCount(candidateResult2);
        } else {
            throw new SQLException(
                    "Unexpected exception, one of the RS should've completed successfully");
        }
    }

    /**
     * @param <T>
     * @param candidateResult1
     * @param candidateResult2
     * @return Pair of CandidateResult ordered by completion. First of the pair is guaranteed to be
     * completed non-exceptionally
     * @throws SQLException
     */
    private <T> Pair<CandidateResult<T>, CandidateResult<T>>
            findFirstNonExceptionallyCompletedCandidateResult(CandidateResult<T> candidateResult1,
                    CandidateResult<T> candidateResult2) throws SQLException {
        Pair<CandidateResult<T>, CandidateResult<T>> pair = new Pair<>();
        CompletableFuture<T> candidate1 = candidateResult1.getCandidate();
        CompletableFuture<T> candidate2 = candidateResult2.getCandidate();
        if (candidate1.isDone() && !candidate1.isCompletedExceptionally()) {
            pair.setFirst(candidateResult1);
            pair.setSecond(candidateResult2);
        } else if (candidate2.isDone() && !candidate2.isCompletedExceptionally()) {
            pair.setFirst(candidateResult2);
            pair.setSecond(candidateResult1);
        } else {
            throw new SQLException(
                    "Unexpected exception, one of the RS should've completed successfully");
        }
        return pair;
    }

    @Override
    public void close() throws SQLException {
        Function<ResultSet, Void> function = (T) -> {
            try {
                T.close();
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };
        runOnResultSets(function);
    }

    @VisibleForTesting
    ResultSet getResultSet() {
        return this.rs;
    }

    @Override
    public Map<String, Map<MetricType, Long>> getReadMetrics() {
        Map<String, Map<MetricType, Long>> metrics;
        if (rs != null) {
            metrics = ((PhoenixMonitoredResultSet) rs).getReadMetrics();
        } else {
            metrics = new HashMap<>();
        }
        context.decorateMetrics(metrics);
        return metrics;
    }

    @Override
    public Map<MetricType, Long> getOverAllRequestReadMetrics() {
        Map<MetricType, Long> metrics;
        if (rs != null) {
            metrics = ((PhoenixMonitoredResultSet) rs).getOverAllRequestReadMetrics();
        } else {
            metrics = context.getContextMetrics();
        }
        return metrics;
    }

    @Override
    public void resetMetrics() {
        if (rs != null) {
            ((PhoenixResultSet) rs).resetMetrics();
        }
        // reset our metrics
        context.resetMetrics();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return (T) this;
        }
        throw new SQLExceptionInfo.Builder(CLASS_NOT_UNWRAPPABLE).build().buildException();
    }

    private void logIfTraceEnabled(CandidateResult<Boolean> bindingCandidateResult)
            throws InterruptedException, ExecutionException {
        if (LOG.isTraceEnabled()) {
            boolean isNull = bindingCandidateResult.getCandidate().get();
            boolean belongsToActiveCluster = bindingCandidateResult.belongsToActiveCluster();
            LOG.trace(String.format(
                "ParallelPhoenixNullComparingResultSet binding to ResultSet"
                        + " with attributes: isEmpty:%s belongsToActiveCluster:%s",
                isNull, belongsToActiveCluster));
        }
    }

    private <T> void incrementClusterUsedCount(CandidateResult<T> candidateResult) {
        if (candidateResult.belongsToActiveCluster()) {
            context.getParallelPhoenixMetrics().getActiveClusterUsedCount().increment();
        } else {
            context.getParallelPhoenixMetrics().getStandbyClusterUsedCount().increment();
        }
    }

    private static class CandidateResult<T> {
        private final CompletableFuture<T> candidate;
        private final CompletableFuture<ResultSet> rs;
        private final boolean belongsToActiveCluster;

        CandidateResult(CompletableFuture<T> candidate, CompletableFuture<ResultSet> rs,
                boolean belongsToActiveCluster) {
            this.candidate = candidate;
            this.rs = rs;
            this.belongsToActiveCluster = belongsToActiveCluster;
        }

        public CompletableFuture<T> getCandidate() {
            return candidate;
        }

        public CompletableFuture<ResultSet> getRs() {
            return rs;
        }

        public boolean belongsToActiveCluster() {
            return belongsToActiveCluster;
        }
    }
}
