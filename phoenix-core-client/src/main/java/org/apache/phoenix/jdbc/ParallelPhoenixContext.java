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

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.monitoring.MetricType;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.phoenix.jdbc.ParallelPhoenixUtil.PHOENIX_HA_PARALLEL_OPERATION_TIMEOUT_ATTRIB;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HA_PARALLEL_CONNECTION_CREATED_COUNTER;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HA_PARALLEL_CONNECTION_ERROR_COUNTER;
import static org.apache.phoenix.query.QueryServices.AUTO_COMMIT_ATTRIB;

/**
 * ParallelPhoenixContext holds the state of the execution of a parallel phoenix operation as well as metrics.
 */
public class ParallelPhoenixContext {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelPhoenixContext.class);

    public static String PARALLEL_PHOENIX_METRICS = "parallel_phoenix_metrics";

    private final ParallelPhoenixClusterContext cluster1Context;
    private final ParallelPhoenixClusterContext cluster2Context;

    //May need multiple properties in the future...
    //Depends on if we have phoenix.querytimeout and phoenix.second.querytimeout
    private final Properties properties;

    private final HighAvailabilityGroup haGroup;
    private final long operationTimeoutMs;

    private volatile boolean isClosed = false;
    private volatile boolean isErrored = false;

    private ParallelPhoenixMetrics parallelPhoenixMetrics;

    /**
     * @param properties
     * @param haGroup
     * @param executors          Executors to use for operations on connections. We use first executor in the
     *                           list for connection1 and second for connection2
     * @param executorCapacities Ordered list of executorCapacities corresponding to executors. Null is interpreted as
     *                           executors having capacity
     */
    ParallelPhoenixContext(Properties properties, HighAvailabilityGroup haGroup, List<PhoenixHAExecutorServiceProvider.PhoenixHAClusterExecutorServices> executors, List<Boolean> executorCapacities) {
        Preconditions.checkNotNull(executors);
        Preconditions.checkArgument(executors.size() >= 2, "Expected 2 executor pairs, one for each connection with a normal/close executor");
        GLOBAL_HA_PARALLEL_CONNECTION_CREATED_COUNTER.increment();
        this.properties = properties;
        this.haGroup = haGroup;

        this.parallelPhoenixMetrics = new ParallelPhoenixMetrics();
        this.operationTimeoutMs = getOperationTimeoutMs(properties);

        cluster1Context = new ParallelPhoenixClusterContext(1, executors.get(0).getExecutorService(), executors.get(0).getCloseExecutorService());
        cluster2Context = new ParallelPhoenixClusterContext(2, executors.get(1).getExecutorService(), executors.get(1).getCloseExecutorService());

        /**
         * Initializes ClusterContext's chainOnConnections according to capacity available in the threadpools.
         * If there is no capacity available we initialize the chain for that connection exceptionally
         * so any further operations on the chain also complete exceptionally
         */
        if (executorCapacities == null) {
            return;
        }
        Preconditions.checkArgument(executorCapacities.size() >= 2,
                "Expected 2 executorCapacities values for each threadpool");
        if (!executorCapacities.get(0)) {
            disableChainOnConn(cluster1Context, this.haGroup.getGroupInfo().getUrl1());
        }
        if (!executorCapacities.get(1)) {
            disableChainOnConn(cluster2Context, this.haGroup.getGroupInfo().getUrl2());
        }
    }

    public ParallelPhoenixMetrics getParallelPhoenixMetrics() {
        return parallelPhoenixMetrics;
    }

    private void disableChainOnConn(ParallelPhoenixClusterContext context, String url) {
        CompletableFuture chainOnConn = new CompletableFuture<>();
        chainOnConn.completeExceptionally(
                new SQLException("No capacity available for connection " + context.clusterIndex + " for cluster " + url));
        LOG.debug("No capacity available for connection " + context.clusterIndex + " for cluster {}", url);
        context.setChainOnConn(chainOnConn);
    }

    public Properties getProperties() {
        //FIXME should return immutable
        return properties;
    }

    public HighAvailabilityGroup getHaGroup() {
        return haGroup;
    }

    public boolean isAutoCommit() {
        return Boolean.valueOf((String) properties.getOrDefault(AUTO_COMMIT_ATTRIB, "false"));
    }

    /**
     * Chains an operation on the connection from the last chained operation. This is to ensure that
     * we operate on the underlying phoenix connection (and related objects) using a single thread
     * at any given time. Operations are supposed to be expressed in the form of Supplier. All async
     * operations on the underlying connection should be chained using this method
     *
     * @param Supplier <T>
     * @return CompletableFuture<T>
     */
    public <T> CompletableFuture<T> chainOnConn1(Supplier<T> s) {
        return chainOnConnClusterContext(s, cluster1Context);
    }

    public <T> void setConnection1Tail(CompletableFuture<T> future) {
        cluster1Context.setChainOnConn(future);
    }

    public <T> CompletableFuture<T> chainOnConn2(Supplier<T> s) {
        return chainOnConnClusterContext(s, cluster2Context);
    }

    public <T> void setConnection2Tail(CompletableFuture<T> future) {
        cluster2Context.setChainOnConn(future);
    }

    private <T> CompletableFuture<T> chainOnConnClusterContext(Supplier<T> s, ParallelPhoenixClusterContext context) {
        CompletableFuture<T> chainedFuture =
                context.getChainOnConn().thenApplyAsync((f) -> s.get(), context.getExecutorForCluster());
        context.setChainOnConn(chainedFuture);
        return chainedFuture;
    }

    public void close() {
        isClosed = true;
        if (isErrored) {
            GLOBAL_HA_PARALLEL_CONNECTION_ERROR_COUNTER.increment();
        }
    }

    public boolean isClosed() {
        return isClosed;
    }

    public void setError() {
        isErrored = true;
    }

    public void checkOpen() throws SQLException {
        if (isClosed) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CONNECTION_CLOSED)
                    .build()
                    .buildException();
        }
    }

    public Map<MetricType, Long> getContextMetrics() {
        return this.parallelPhoenixMetrics.getAllMetrics().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, t -> t.getValue().getValue()));
    }

    public void resetMetrics() {
        //We don't use ParallelPhoenixMetrics::reset() here as that will race with any remaining operations
        //Instead we generate new metrics, any updates won't be reflected in future reads
        parallelPhoenixMetrics = new ParallelPhoenixMetrics();
    }

    /**
     * Decorates metrics from PhoenixConnections table metrics with a virtual table of
     * PARALLEL_PHOENIX_METRICS name as well as all the context's metrics.
     *
     * @param initialMetrics Table Specific Metrics class to populate
     */
    public void decorateMetrics(Map<String, Map<MetricType, Long>> initialMetrics) {
        //decorate
        initialMetrics.put(PARALLEL_PHOENIX_METRICS, getContextMetrics());
    }

    public long getOperationTimeout() {
        return this.operationTimeoutMs;
    }

    CompletableFuture<?> getChainOnConn1() {
        return this.cluster1Context.getChainOnConn();
    }

    CompletableFuture<?> getChainOnConn2() {
        return this.cluster2Context.getChainOnConn();
    }

    ExecutorService getCloseConnection1ExecutorService() {return this.cluster1Context.getConnectionCloseExecutor(); }

    ExecutorService getCloseConnection2ExecutorService() {return this.cluster2Context.getConnectionCloseExecutor(); }

    private long getOperationTimeoutMs(Properties properties) {
        long operationTimeoutMs;
        if (properties.getProperty(PHOENIX_HA_PARALLEL_OPERATION_TIMEOUT_ATTRIB) != null) {
            operationTimeoutMs = Long.parseLong(
                    properties.getProperty(PHOENIX_HA_PARALLEL_OPERATION_TIMEOUT_ATTRIB));
        } else {
            operationTimeoutMs =
                    Long.parseLong(properties.getProperty(QueryServices.THREAD_TIMEOUT_MS_ATTRIB,
                            Long.toString(QueryServicesOptions.DEFAULT_THREAD_TIMEOUT_MS)));
        }
        Preconditions.checkArgument(operationTimeoutMs >= 0);
        return operationTimeoutMs;
    }

    private static class ParallelPhoenixClusterContext {
        private final int clusterIndex;
        private final ExecutorService executorForCluster;
        private final ExecutorService connectionCloseExecutor;

        private CompletableFuture chainOnConn = CompletableFuture.completedFuture(new Object());

        public ParallelPhoenixClusterContext(int clusterIndex, ExecutorService executorForCluster, ExecutorService connectionCloseExecutor) {
            this.clusterIndex = clusterIndex;
            this.executorForCluster = executorForCluster;
            this.connectionCloseExecutor = connectionCloseExecutor;
        }

        public ExecutorService getExecutorForCluster() {
            return executorForCluster;
        }

        public ExecutorService getConnectionCloseExecutor() { return connectionCloseExecutor; }

        public CompletableFuture getChainOnConn() {
            return chainOnConn;
        }

        public void setChainOnConn(CompletableFuture chainOnConn) {
            this.chainOnConn = chainOnConn;
        }
    }
}
