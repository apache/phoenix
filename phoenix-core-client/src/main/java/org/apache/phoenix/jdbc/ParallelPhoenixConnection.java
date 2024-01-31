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

import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.jdbc.ParallelPhoenixUtil.FutureResult;
import org.apache.phoenix.monitoring.MetricType;
import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.apache.phoenix.exception.SQLExceptionCode.CLASS_NOT_UNWRAPPABLE;

public class ParallelPhoenixConnection implements PhoenixMonitoredConnection {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelPhoenixConnection.class);

    private final ParallelPhoenixContext context;
    CompletableFuture<PhoenixConnection> futureConnection1;
    CompletableFuture<PhoenixConnection> futureConnection2;
    public ParallelPhoenixConnection(ParallelPhoenixContext context) throws SQLException {
        this.context = context;
        LOG.trace("First Url: {} Second Url: {}", context.getHaGroup().getGroupInfo().getJDBCUrl1(),
                context.getHaGroup().getGroupInfo().getJDBCUrl2());
        futureConnection1 = context.chainOnConn1(() -> getConnection(context.getHaGroup(),
                context.getHaGroup().getGroupInfo().getJDBCUrl1(),
                context.getProperties()));
        futureConnection2 = context.chainOnConn2(() -> getConnection(context.getHaGroup(),
                context.getHaGroup().getGroupInfo().getJDBCUrl2(),
                context.getProperties()));

        // Ensure one connection is successful before returning
        ParallelPhoenixUtil.INSTANCE.runFutures(Arrays.asList(futureConnection1, futureConnection2), context, false);
    }

    @VisibleForTesting
    ParallelPhoenixConnection(ParallelPhoenixContext context, CompletableFuture<PhoenixConnection> futureConnection1, CompletableFuture<PhoenixConnection> futureConnection2) throws SQLException {
        this.context = context;
        this.futureConnection1 = futureConnection1;
        this.futureConnection2 = futureConnection2;
        // Ensure one connection is successful before returning
        ParallelPhoenixUtil.INSTANCE.runFutures(Arrays.asList(futureConnection1, futureConnection2), context, false);
    }

    private static PhoenixConnection getConnection(HighAvailabilityGroup haGroup, String url, Properties properties) {
        try {
            return haGroup.connectToOneCluster(url, properties);
        } catch (SQLException exception) {
            if (LOG.isTraceEnabled()) {
                LOG.trace(String.format("Failed to get a connection for haGroup %s to %s", haGroup.toString(), url), exception);
            }
            throw new CompletionException(exception);
        }
    }

    public CompletableFuture<PhoenixConnection> getFutureConnection1() {
        return futureConnection1;
    }

    public CompletableFuture<PhoenixConnection> getFutureConnection2() {
        return futureConnection2;
    }

    @VisibleForTesting
    ParallelPhoenixContext getContext() {
        return this.context;
    }

    Object runOnConnections(Function<PhoenixConnection, ?> function, boolean useMetrics) throws SQLException {
        return ParallelPhoenixUtil.INSTANCE.runFutures(function, futureConnection1, futureConnection2, context, useMetrics);
    }

    PairOfSameType<Object> runOnConnectionsGetAll(Function<PhoenixConnection, ?> function, boolean useMetrics) throws SQLException {
        return ParallelPhoenixUtil.INSTANCE.runOnFuturesGetAll(function, futureConnection1, futureConnection2, context, useMetrics);
    }

    @Override
    public ParallelPhoenixStatement createStatement() throws SQLException {
        context.checkOpen();

        Function<PhoenixConnection, PhoenixMonitoredStatement> function = (T) -> {
            try {
                return (PhoenixStatement) T.createStatement();
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        List<CompletableFuture<PhoenixMonitoredStatement>> futures = ParallelPhoenixUtil.INSTANCE.applyFunctionToFutures(function, futureConnection1,
                futureConnection2, context, true);

        Preconditions.checkState(futures.size() == 2);
        CompletableFuture<PhoenixMonitoredStatement> statement1 = futures.get(0);
        CompletableFuture<PhoenixMonitoredStatement> statement2 = futures.get(1);

        //Ensure one statement is successful before returning
        ParallelPhoenixUtil.INSTANCE.runFutures(futures, context, true);

        return new ParallelPhoenixStatement(context, statement1, statement2);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        context.checkOpen();

        Function<PhoenixConnection, PhoenixMonitoredPreparedStatement> function = (T) -> {
            try {
                return (PhoenixMonitoredPreparedStatement) T.prepareStatement(sql);
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        List<CompletableFuture<PhoenixMonitoredPreparedStatement>> futures = ParallelPhoenixUtil.INSTANCE.applyFunctionToFutures(function, futureConnection1,
                futureConnection2, context, true);

        Preconditions.checkState(futures.size() == 2);
        CompletableFuture<PhoenixMonitoredPreparedStatement> statement1 = futures.get(0);
        CompletableFuture<PhoenixMonitoredPreparedStatement> statement2 = futures.get(1);

        //Ensure one statement is successful before returning
        ParallelPhoenixUtil.INSTANCE.runFutures(futures, context, true);

        return new ParallelPhoenixPreparedStatement(this.context, statement1, statement2);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return null;
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return null;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        Function<PhoenixConnection, Boolean> function = (T) -> {
            try {
                return T.getAutoCommit();
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        return (boolean) runOnConnections(function, true);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        Function<PhoenixConnection, Void> function = (T) -> {
            try {
                T.setAutoCommit(autoCommit);
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        runOnConnections(function, true);
    }

    @Override
    public void commit() throws SQLException {
        Function<PhoenixConnection, Void> function = (T) -> {
            try {
                T.commit();
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        runOnConnections(function, true);
    }

    @Override
    public void rollback() throws SQLException {

    }

    /**
     * Close the underlying connections. Returns after any one of the underlying connections have
     * closed successfully
     *
     * @throws SQLException if trying to close both the underlying connections encounters errors
     */
    @Override
    public void close() throws SQLException {
        context.close();
        SQLException closeExp = null;

        // We can have errors on the chain, we still need to close the underlying connections
        // irrespective. Do a close when we're at the end of the chain and wait for any 1 to be
        // successful. We need to track which future completed hence use FutureResult
        // Enqueue a close operation at the end of the chain in the common ForkJoin Pool
        java.util.List<java.util.concurrent.CompletableFuture<? extends java.lang.Object>> futures = new ArrayList<>();
        CompletableFuture<FutureResult<CompletableFuture<Boolean>>> closeFuture1=null,closeFuture2=null;

        // For connection close, we use the separate close executor pool to perform
        // close on both the connections. We ensure that a success is returned when
        // any one of the connection is closed successfully.


        try {
            Supplier<Boolean> closeSupplier1 = getCloseSupplier(futureConnection1);
            closeFuture1 =
                    futureConnection1.handle((obj, e) -> {
                        return CompletableFuture.supplyAsync(closeSupplier1, context.getCloseConnection1ExecutorService());
                    }).thenApply(t -> new FutureResult<>(t, 0));
            futures.add(closeFuture1);
        } catch (Exception e) {
            //Swallow close exceptions
            LOG.error("Unknow error happened preparing to close connection 1.",e);
        }
        try {
            Supplier<Boolean> closeSupplier2 = getCloseSupplier(futureConnection2);
            closeFuture2 =
                    futureConnection2.handle((obj, e) -> {
                        return CompletableFuture.supplyAsync(closeSupplier2, context.getCloseConnection2ExecutorService());
                    }).thenApply(t -> new FutureResult<>(t, 1));
            futures.add(closeFuture2);
        } catch (Exception e) {
            //Swallow close exceptions
            LOG.error("Unknow error happened preparing to close connection 2.",e);
        }



        FutureResult<CompletableFuture<Boolean>> result =
                (FutureResult<CompletableFuture<Boolean>>) ParallelPhoenixUtil.INSTANCE
                        .getAnyOfNonExceptionally(futures, context);

        try {
            ParallelPhoenixUtil.INSTANCE.getFutureNoRetry(result.getResult(), context);
            return;
        } catch (Exception e) {
            closeExp = new SQLException(e);
        }
        // The previous close encountered an exception try the other one
        CompletableFuture<FutureResult<CompletableFuture<Boolean>>> otherFuture =
                (result.getIndex() == 0) ? closeFuture2 : closeFuture1;
        if(otherFuture != null) {
            try {
                FutureResult<CompletableFuture<Boolean>> otherResult =
                        ParallelPhoenixUtil.INSTANCE.getFutureNoRetry(otherFuture, context);
                ParallelPhoenixUtil.INSTANCE.getFutureNoRetry(otherResult.getResult(), context);
            } catch (Exception e) {
                closeExp.addSuppressed(e);
                LOG.error("Failed closing both underlying connections within time limits", closeExp);
                throw closeExp;
            }
        }
    }

    private Supplier<Boolean> getCloseSupplier(CompletableFuture<PhoenixConnection> conn) {
        return () -> {
            try {
                getConnectionAndTryClose(conn);
            } catch (Exception exp) {
                throw new CompletionException(exp);
            }
            return true;
        };
    }

    private void getConnectionAndTryClose(CompletableFuture<PhoenixConnection> futureConn)
            throws SQLException {
        try {
            futureConn.get().close();
        } catch (InterruptedException | ExecutionException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return context.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        Function<PhoenixConnection, DatabaseMetaData> function = (T) -> {
            try {
                return T.getMetaData();
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };
        return (DatabaseMetaData) runOnConnections(function, true);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getCatalog() throws SQLException {
        Function<PhoenixConnection, String> function = (T) -> {
            try {
                return T.getCatalog();
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };
        return (String) runOnConnections(function, true);
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        Function<PhoenixConnection, Void> function = (T) -> {
            try {
                T.getCatalog();
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };
        runOnConnections(function, true);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        Function<PhoenixConnection, Integer> function = (T) -> {
            try {
                return T.getTransactionIsolation();
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };
        return (int) runOnConnections(function, true);
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        Function<PhoenixConnection, Void> function = (T) -> {
            try {
                T.setTransactionIsolation(level);
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };
        runOnConnections(function, true);
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {

        Function<PhoenixConnection, SQLWarning> function = (T) -> {
            try {
                return T.getWarnings();
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        try {
            PairOfSameType<Object> pair = runOnConnectionsGetAll(function, true);
            SQLWarning warning1 = (SQLWarning) pair.getFirst();
            SQLWarning warning2 = (SQLWarning) pair.getSecond();
            if (warning1 != null && warning2 != null) {
                SQLWarning warning = new SQLWarning("Warnings on multiple connections.");
                warning.setNextWarning(warning1);
                warning.setNextWarning(warning2);
                return warning;
            } else {
                return Stream.of(warning1, warning2).filter(Objects::nonNull).findFirst().orElse(null);
            }
        } catch (Exception e) {
            throw new SQLException(e);
        }

    }

    @Override
    public void clearWarnings() throws SQLException {
        Function<PhoenixConnection, Void> function = (T) -> {
            try {
                T.clearWarnings();
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };
        runOnConnectionsGetAll(function, true);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency) throws SQLException {
        return null;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency,
                                     int resultSetHoldability) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return null;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
            throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames)
            throws SQLException {
        return null;
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return context.getProperties() != null ? context.getProperties().getProperty(name) : null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return context.getProperties();
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getSchema() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return (T) this;
        }
        throw new SQLExceptionInfo.Builder(CLASS_NOT_UNWRAPPABLE).build().buildException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    @Override
    public Map<String, Map<MetricType, Long>> getMutationMetrics() {
        Map<String, Map<MetricType, Long>> metrics = new HashMap<>();
        try {
            //This can return an unmodifiable map so we create our own
            Map<String, Map<MetricType, Long>> winningMetrics = (Map<String, Map<MetricType, Long>>) runOnConnections(PhoenixConnection::getMutationMetrics, false);
            metrics.putAll(winningMetrics);
            context.decorateMetrics(metrics);
            return metrics;
        } catch (SQLException e) {
            LOG.error("Unexpected error while getting mutation metrics.", e);
            return Collections.emptyMap();
        }
    }

    @Override
    public Map<String, Map<MetricType, Long>> getReadMetrics() {
        Map<String, Map<MetricType, Long>> metrics = new HashMap<>();
        try {
            //This can return an unmodifiable map so we create our own
            Map<String, Map<MetricType, Long>> winningMetrics = (Map<String, Map<MetricType, Long>>) runOnConnections(PhoenixConnection::getReadMetrics, false);
            metrics.putAll(winningMetrics);
            context.decorateMetrics(metrics);
            return metrics;
        } catch (SQLException e) {
            LOG.error("Unexpected error while getting read metrics.", e);
            return Collections.emptyMap();
        }
    }

    @Override
    public boolean isRequestLevelMetricsEnabled() {
        //For initial offering assume this is true, may want to OR the 2 connections
        return true;
    }

    @Override
    public void clearMetrics() {
        Function<PhoenixConnection, Void> function = (T) -> {
            T.clearMetrics();
            return null;
        };
        try {
            runOnConnections(function, false);
        } catch (SQLException exception) {
            LOG.error("Unexpected exception while clearning metrics.", exception);
        }
        context.resetMetrics();
    }
}
