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

import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HA_PARALLEL_TASK_TIMEOUT_COUNTER;

import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableList;

import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.monitoring.Metric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class ParallelPhoenixUtil {

    // Timeout used for every operation on a ParallelPhoenixConnection. Defaults to
    // phoenix.query.timeoutMs. 0 means wait indefinitely
    public static final String PHOENIX_HA_PARALLEL_OPERATION_TIMEOUT_ATTRIB = "phoenix.ha.parallel.operation.timeout.ms";

    public static ParallelPhoenixUtil INSTANCE = new ParallelPhoenixUtil();

    private static final Logger LOGGER =
            LoggerFactory.getLogger(ParallelPhoenixUtil.class);
    private static final long DEFAULT_INTERNAL_OPERATION_TIMEOUT_MS = 1000;

    private ParallelPhoenixUtil(){
    }

    /**
     *
     * @param futures position in list indicates the cluster
     * @param context
     * @return
     * @throws SQLException
     */
    public Object getAnyOfNonExceptionally(List<CompletableFuture<? extends Object>> futures, ParallelPhoenixContext context) throws SQLException {
        long timeoutMs = context.getOperationTimeout();
        long endTime =
                (timeoutMs == 0) ? Long.MAX_VALUE
                        : EnvironmentEdgeManager.currentTime() + timeoutMs;
        long internalTimeoutMs =
                (timeoutMs == 0) ? DEFAULT_INTERNAL_OPERATION_TIMEOUT_MS
                        : Math.min(DEFAULT_INTERNAL_OPERATION_TIMEOUT_MS, timeoutMs);
        Object result = null;
        boolean changed = true;
        boolean timedout = false;
        List<CompletableFuture<?>> originalFutures = futures;
        CompletableFuture<Object> resultFuture = null;
        while (!futures.isEmpty() && result == null) {
            if (EnvironmentEdgeManager.currentTime() > endTime) {
                timedout = true;
                break;
            }
            /**
             * CompletableFuture.anyOf adds a completion handler to every future its passed so if we call it
             * too often we can quickly wind up with thousands of completion handlers which take a long time
             * to iterate through and notify. So only create it when the futures it covers have actually
             * changed.
             */
            if(changed) {
                resultFuture = CompletableFuture.anyOf(futures.toArray(new CompletableFuture[0]));
            }
            try {
                result = resultFuture.get(internalTimeoutMs, TimeUnit.MILLISECONDS);
                break;
            } catch (Exception e) {
                //remove the exceptionally completed results
                List<CompletableFuture<? extends Object>> filteredResults = futures.stream().filter(f -> !f.isCompletedExceptionally()).collect(Collectors.toList());
                if(filteredResults.equals(futures)) {
                    changed = false;
                } else {
                    futures = filteredResults;
                    changed = true;
                }
            }
        }

        //All of our futures failed
        if (futures.isEmpty()) {
            LOGGER.error("All Futures failed.");
            SQLException futuresException = null;
            int i = 0;
            for(CompletableFuture<?> failedFuture : originalFutures) {
                try {
                    failedFuture.get();
                } catch (Exception e) {
                    LOGGER.error("Future Exception. Cluster " + i + " HAGroup:" + context.getHaGroup(), e);
                    if (futuresException == null) {
                        futuresException = new SQLException("All futures failed. HAGroup:" + context.getHaGroup(), e);
                    } else {
                        futuresException.addSuppressed(e);
                    }
                }
            }
            context.setError();
            throw futuresException;
        }

        if (timedout) {
            GLOBAL_HA_PARALLEL_TASK_TIMEOUT_COUNTER.increment();

            if(futures.isEmpty()) {
                LOGGER.warn("Unexpected race between timeout and failure occurred.");
            } else {
                int i = 0;
                LOGGER.error("Parallel Phoenix Timeout occurred");
                for(CompletableFuture future : originalFutures) {
                    if(future.isCompletedExceptionally()) {
                        try {
                            future.get();
                        } catch (Exception e) {
                            LOGGER.info("For timeout cluster " + i + " completed exceptionally. HAGroup:" + context.getHaGroup(), e);
                        }
                    } else {
                        if(future.isDone()) {
                            LOGGER.info("For timeout cluster " + i + " finished post timeout prior to recording. HAGroup:" + context.getHaGroup());
                        } else {
                            LOGGER.info("For timeout cluster " + i + " still running. HAGroup:" + context.getHaGroup());
                        }
                    }
                    i++;
                }
            }
            context.setError();
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.OPERATION_TIMED_OUT)
                    .setMessage("Operation timedout. Operation timeout ms = " + timeoutMs).build()
                    .buildException();
        } else {
            //Debug
            int i = 0;
            for(CompletableFuture<?> failedFuture : originalFutures) {
                if(failedFuture.isCompletedExceptionally()) {
                    try {
                        failedFuture.get();
                    } catch (Exception e) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("Future Exception. Cluster " + i + "HAGroup:" + context.getHaGroup(), e);
                        }
                    }
                }
                i++;
            }
        }

        return result;
    }

    static class FutureResult<T> {
        FutureResult(T t, int index) {
            this.t = t;
            this.index = index;
        }

        private final T t;
        private final int index;

        T getResult() {
            return t;
        }

        int getIndex() {
            return index;
        }
    }


    public <T> T getFutureNoRetry(CompletableFuture<T> future, ParallelPhoenixContext context)
            throws InterruptedException, ExecutionException, TimeoutException {
        long operationTimeoutMs = context.getOperationTimeout();
        long timeout = (operationTimeoutMs > 0) ? operationTimeoutMs : Long.MAX_VALUE;
        return future.get(timeout, TimeUnit.MILLISECONDS);
    }


    <T, R> CompletableFuture<T> getFutureAndChainOnContextNoMetrics(Function<R, T> functionToApply,
                                                           CompletableFuture<R> future1,
                                                           Function<Supplier<T>,
                                                                   CompletableFuture<T>> chainOnConn) {
        return getFutureAndChainOnContext(functionToApply, future1, chainOnConn, null, null, false);
    }

    <T, R> CompletableFuture<T> getFutureAndChainOnContext(Function<R, T> functionToApply,
                                                           CompletableFuture<R> future1,
                                                           Function<Supplier<T>,
                                                                   CompletableFuture<T>> chainOnConn,
                                                           Metric operationCount,
                                                           Metric failureCount) {
        return getFutureAndChainOnContext(functionToApply, future1, chainOnConn, operationCount, failureCount, true);
    }

    private <T, R> CompletableFuture<T> getFutureAndChainOnContext(Function<R, T> functionToApply,
                                                           CompletableFuture<R> future1,
                                                           Function<Supplier<T>,CompletableFuture<T>> chainOnConn,
                                                           Metric operationCount,
                                                           Metric failureCount, boolean useMetrics) {

        return chainOnConn.apply(() -> {
            try {
                if(useMetrics) {
                    operationCount.increment();
                }
                return functionToApply.apply(future1.get());
            } catch (Exception e) {
                if(useMetrics) {
                    failureCount.increment();
                }
                throw new CompletionException(e);
            }
        });
    }

    <T, R> List<CompletableFuture<T>> applyFunctionToFutures(Function<R, T> function, CompletableFuture<R> future1,
                                                             CompletableFuture<R> future2, ParallelPhoenixContext context, boolean useMetrics) {

        CompletableFuture<T> result1 =
                getFutureAndChainOnContext(function, future1, context::chainOnConn1,
                        context.getParallelPhoenixMetrics().getActiveClusterOperationCount(),
                        context.getParallelPhoenixMetrics().getActiveClusterFailedOperationCount(), useMetrics);
        CompletableFuture<T> result2 =
                getFutureAndChainOnContext(function, future2, context::chainOnConn2,
                        context.getParallelPhoenixMetrics().getStandbyClusterOperationCount(),
                        context.getParallelPhoenixMetrics().getStandbyClusterFailedOperationCount(), useMetrics);

        return ImmutableList.of(result1,result2);
    }

    /**
     *
     * @param <T> Type of the future
     * @param futures list of futures to run, for 2 clusters 0 will be the active 1 will be the standby
     * @param context this parallel connections context
     * @param useMetrics
     * @return The first non-expectional result of the futures
     * @throws SQLException no non-exceptional future available
     */
    <T> Object runFutures(List<CompletableFuture<T>> futures, ParallelPhoenixContext context, boolean useMetrics) throws SQLException {

        List<CompletableFuture<? extends Object>> ranFutures = new ArrayList<>();
        for(int i = 0; i < futures.size(); i++) {
            CompletableFuture<T> future = futures.get(i);
            int finalI = i;
            CompletableFuture<FutureResult<T>> decoratedFuture = future.thenApply(t -> new FutureResult<>(t, finalI));
            ranFutures.add(decoratedFuture);
        }
        FutureResult<T> result = (FutureResult<T>) getAnyOfNonExceptionally(ranFutures, context);
        if(useMetrics) {
            context.getParallelPhoenixMetrics().get(PhoenixHAGroupMetrics.HAMetricType.HA_PARALLEL_USED_OPERATIONS, result.index).increment();
        }
        return result.t;
    }

    <T, R> Object runFutures(Function<R, T> function, CompletableFuture<R> future1,
                             CompletableFuture<R> future2, ParallelPhoenixContext context, boolean useMetrics) throws SQLException {
        List<CompletableFuture<T>> list = applyFunctionToFutures(function,future1,future2,context, useMetrics);
        return runFutures(list,context, useMetrics);
    }

    /**
     * Blocks
     * @throws SQLException if any of the futures fail with any exception
     */
    <T, R> PairOfSameType<Object> runOnFuturesGetAll(Function<R, T> function, CompletableFuture<R> future1,
                                                     CompletableFuture<R> future2, ParallelPhoenixContext context, boolean useMetrics) throws SQLException {

        CompletableFuture<T> result1,result2;
        if(useMetrics) {
            result1 = getFutureAndChainOnContext(function, future1, context::chainOnConn1,
                    context.getParallelPhoenixMetrics().getActiveClusterOperationCount(),
                    context.getParallelPhoenixMetrics().getActiveClusterFailedOperationCount());
            result2 = getFutureAndChainOnContext(function, future2, context::chainOnConn2,
                    context.getParallelPhoenixMetrics().getStandbyClusterOperationCount(),
                    context.getParallelPhoenixMetrics().getStandbyClusterFailedOperationCount());
        } else {
            result1 = getFutureAndChainOnContextNoMetrics(function, future1, context::chainOnConn1);
            result2 = getFutureAndChainOnContextNoMetrics(function, future2, context::chainOnConn2);
        }

        Object value1, value2;
        try {
            value1 = result1.get();
        } catch (Exception e) {
            throw new SQLException(e);
        }
        try {
            value2 = result2.get();
        } catch (Exception e) {
            throw new SQLException(e);
        }
        return new PairOfSameType<>(value1, value2);
    }
}
