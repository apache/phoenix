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

import org.apache.phoenix.exception.FailoverSQLException;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.monitoring.MetricType;
import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * An implementation of JDBC connection which supports failover between two cluster in an HA group.
 * <p>
 * During its lifetime, a failover Phoenix connection could possibly connect to two HBase clusters
 * in an HA group mutually exclusively.  It wraps and delegates the logic to a PhoenixConnection
 * object.  At any given time, the wrapped connection should only talk to the ACTIVE HBase cluster
 * in the HA group.
 * <p>
 * A failover connection will behave according to the given failover policy upon cluster role
 * failover, especially when the current connected HBase cluster becomes STANDBY role from ACTIVE.
 * The default behavior (aka default failover policy) will simply close the current connection and
 * throw {@link org.apache.phoenix.exception.FailoverSQLException} exception to those clients who
 * still use this connection after closing.
 * <p>
 * This class is not thread safe.
 *
 * @see HighAvailabilityGroup
 * @see FailoverPolicy
 */
public class FailoverPhoenixConnection implements PhoenixMonitoredConnection {
    /**
     * Failover timeout interval after which failover operation will fail and clients can retry.
     */
    public static final String FAILOVER_TIMEOUT_MS_ATTR = "phoenix.ha.failover.timeout.ms";
    public static final long FAILOVER_TIMEOUT_MS_DEFAULT = 10_000;
    private static final Logger LOG = LoggerFactory.getLogger(FailoverPhoenixConnection.class);
    /**
     * Connection properties.
     */
    private final Properties properties;
    /**
     * High availability group.
     */
    private final HighAvailabilityGroup haGroup;
    /**
     * Failover policy, per connection.
     */
    private final FailoverPolicy policy;

    /**
     * True iff this connection has been closed by the client.
     */
    private boolean isClosed;
    /**
     * The wrapped PhoenixConnection object which could be re-assigned upon failover operation.
     */
    private PhoenixConnection connection;

    /**
     * Mutation metrics before failover to current connection.
     */
    private Map<String, Map<MetricType, Long>> previousMutationMetrics = new HashMap<>();
    /**
     * Read metrics before failover to current connection.
     */
    private Map<String, Map<MetricType, Long>> previousReadMetrics = new HashMap<>();

    public FailoverPhoenixConnection(HighAvailabilityGroup haGroup, Properties properties)
            throws SQLException {
        this.properties = properties;
        this.haGroup = haGroup;
        this.policy = FailoverPolicy.get(properties);
        this.isClosed = false;
        this.connection = haGroup.connectActive(properties);
    }

    /**
     * This is used for explicit failover request made by client.
     * <p>
     * It fails over to the current ACTIVE HBase cluster; if failover happens in between, this could
     * possibly target this same cluster again.
     * <p>
     *
     * @param conn      if not of FailoverPhoenixConnection type, throw illegal argument exception
     * @param timeoutMs timeout in milliseconds to failover to current active cluster
     * @throws SQLException if fails to failover
     */
    public static void failover(Connection conn, long timeoutMs) throws SQLException {
        Preconditions.checkNotNull(conn, "Connection to failover must not be null!");
        FailoverPhoenixConnection failoverConnection = conn.unwrap(FailoverPhoenixConnection.class);
        if (failoverConnection == null) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CLASS_NOT_UNWRAPPABLE)
                    .setMessage("Connection is not a valid FailoverPhoenixConnection object")
                    .build()
                    .buildException();
        }
        failoverConnection.failover(timeoutMs);
    }

    /**
     * Helper method to merge two metrics map into one.
     * <p>
     * Shallow copy the first one, and deep copy the second one.
     * An optimization is that, it will return the shallow directly if the deep is empty.
     */
    private static Map<String, Map<MetricType, Long>> mergeMetricMaps(
            Map<String, Map<MetricType, Long>> shallow, Map<String, Map<MetricType, Long>> deep) {
        if (deep.isEmpty()) {
            return shallow;
        }

        Map<String, Map<MetricType, Long>> metrics = new HashMap<>(shallow);
        deep.forEach((k, v) -> {
            metrics.putIfAbsent(k, new HashMap<>());
            Map<MetricType, Long> map = metrics.get(k);
            v.forEach((kk, vv) -> {
                Long value = map.getOrDefault(kk, 0L);
                map.put(kk, value + vv);
            });
        });
        return metrics;
    }

    /**
     * Failover this connection by switching underlying phoenix connection to the ACTIVE one.
     * <p>
     * If the current phoenix connection is already connecting to ACTIVE cluster, this is a no-op.
     *
     * @param timeoutMs timeout in ms waiting for a new connection to be established.
     * @throws SQLException if fails to failover
     */
    @VisibleForTesting
    void failover(long timeoutMs) throws SQLException {
        checkConnection();

        if (haGroup.isActive(connection)) {
            LOG.info("Connection {} is against ACTIVE cluster in HA group {}; skip failing over.",
                    connection.getURL(), haGroup.getGroupInfo().getName());
            return;
        }

        PhoenixConnection newConn = null;
        SQLException cause = null;
        final long startTime = EnvironmentEdgeManager.currentTimeMillis();
        while (newConn == null &&
                EnvironmentEdgeManager.currentTimeMillis() < startTime + timeoutMs) {
            try {
                newConn = haGroup.connectActive(properties);
            } catch (SQLException e) {
                cause = e;
                LOG.info("Got exception when trying to connect to active cluster.", e);
                try {
                    Thread.sleep(100); // TODO: be smart than this
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new SQLException("Got interrupted waiting for connection failover", e);
                }
            }
        }
        if (newConn == null) {
            throw new FailoverSQLException("Can not failover connection",
                    haGroup.getGroupInfo().toString(), cause);
        }

        final PhoenixConnection oldConn = connection;
        connection = newConn;
        if (oldConn != null) {
            // aggregate metrics
            previousMutationMetrics = oldConn.getMutationMetrics();
            previousReadMetrics = oldConn.getReadMetrics();
            oldConn.clearMetrics();

            // close old connection
            if (!oldConn.isClosed()) {
                // TODO:  what happens to in-flight edits/mutations?
                // Can we copy into the new connection we do not allow this failover?
                // MutationState state = oldConn.getMutationState();
                try {
                    oldConn.close(new SQLExceptionInfo
                            .Builder(SQLExceptionCode.HA_CLOSED_AFTER_FAILOVER)
                            .setMessage("Phoenix connection got closed due to failover")
                            .setHaGroupInfo(haGroup.getGroupInfo().toString())
                            .build()
                            .buildException());
                } catch (SQLException e) {
                    LOG.error("Failed to close old connection after failover: {}", e.getMessage());
                    LOG.info("Full stack when closing old connection after failover", e);
                }
            }
        }
        LOG.info("Connection {} failed over to {}", haGroup.getGroupInfo(), connection.getURL());
    }

    /**
     * Connection can not be null before any operation.
     * <p>
     * Here when connection is non-null, we do not need to check if the wrapped connection is open.
     * The reason is that each individual delegated call on the wrapped connection will internally
     * check open itself, see {@link PhoenixConnection#checkOpen()}.
     *
     * @throws SQLException if current wrapped phoenix connection is not valid state
     */
    private void checkConnection() throws SQLException {
        if (isClosed) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CONNECTION_CLOSED)
                    .setHaGroupInfo(haGroup.getGroupInfo().toString())
                    .build()
                    .buildException();
        }
        if (connection == null) {
            throw new SQLExceptionInfo
                    .Builder(SQLExceptionCode.CANNOT_ESTABLISH_CONNECTION)
                    .setMessage("Connection has not been established to ACTIVE HBase cluster")
                    .setHaGroupInfo(haGroup.getGroupInfo().toString())
                    .build()
                    .buildException();
        }
    }

    @Override
    public void close() throws SQLException {
        if (isClosed()) {
            return;
        }

        try {
            connection.close();
            connection.clearMetrics();
        } finally {
            previousMutationMetrics.clear();
            previousReadMetrics.clear();
            isClosed = true;
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    //// metrics for monitoring methods

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (!iface.isInstance(this)) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CLASS_NOT_UNWRAPPABLE)
                    .setMessage(getClass().getName() + " not unwrappable from " + iface.getName())
                    .build()
                    .buildException();
        }
        return (T) this;
    }

    @Override
    public Map<String, Map<MetricType, Long>> getMutationMetrics() {
        return mergeMetricMaps(connection.getMutationMetrics(), previousMutationMetrics);
    }

    @Override
    public Map<String, Map<MetricType, Long>> getReadMetrics() {
        return mergeMetricMaps(connection.getReadMetrics(), previousReadMetrics);
    }

    @Override
    public boolean isRequestLevelMetricsEnabled() {
        return connection != null && connection.isRequestLevelMetricsEnabled();
    }

    @Override
    public void clearMetrics() {
        previousMutationMetrics.clear();
        previousReadMetrics.clear();
        if (connection != null) {
            connection.clearMetrics();
        }
    }

    //// Wrapping phoenix connection operations

    /**
     * This is the utility method to help wrapping a method call to phoenix connection.
     *
     * @param s   the supplier which returns a value and may throw SQLException
     * @param <T> type of the returned object by the supplier
     * @return the object returned by the supplier if any
     * @throws SQLException exception when getting object from the supplier
     */
    @VisibleForTesting
    <T> T wrapActionDuringFailover(SupplierWithSQLException<T> s) throws SQLException {
        checkConnection();
        final long timeoutMs = Long.parseLong(properties.getProperty(FAILOVER_TIMEOUT_MS_ATTR,
                String.valueOf(FAILOVER_TIMEOUT_MS_DEFAULT)));
        int failoverCount = 0;
        while (true) {
            try {
                return s.get();
            } catch (SQLException e) {
                if (policy.shouldFailover(e, ++failoverCount)) {
                    failover(timeoutMs);
                } else {
                    throw new SQLException(
                            String.format("Error on operation with failover policy %s", policy), e);
                }
            }
        }
    }

    @VisibleForTesting
    void wrapActionDuringFailover(RunWithSQLException runnable) throws SQLException {
        wrapActionDuringFailover(() -> {
            runnable.run();
            return null;
        });
    }

    @Override
    public void commit() throws SQLException {
        wrapActionDuringFailover(() -> connection.commit());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return wrapActionDuringFailover(() -> connection.isWrapperFor(iface));
    }

    @Override
    public Statement createStatement() throws SQLException {
        return wrapActionDuringFailover(() -> connection.createStatement());
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return wrapActionDuringFailover(() -> connection.prepareStatement(sql));
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return wrapActionDuringFailover(() -> connection.prepareCall(sql));
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return wrapActionDuringFailover(() -> connection.nativeSQL(sql));
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return wrapActionDuringFailover(() -> connection.getAutoCommit());
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        wrapActionDuringFailover(() -> connection.setAutoCommit(autoCommit));
    }

    @Override
    public void rollback() throws SQLException {
        wrapActionDuringFailover(() -> connection.rollback());
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return wrapActionDuringFailover(() -> connection.getMetaData());
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return wrapActionDuringFailover(() -> connection.isReadOnly());
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        wrapActionDuringFailover(() -> connection.setReadOnly(readOnly));
    }

    @Override
    public String getCatalog() throws SQLException {
        return wrapActionDuringFailover(() -> connection.getCatalog());
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        wrapActionDuringFailover(() -> connection.setCatalog(catalog));
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        //noinspection MagicConstant
        return wrapActionDuringFailover(() -> connection.getTransactionIsolation());
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        wrapActionDuringFailover(() -> connection.setTransactionIsolation(level));
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return wrapActionDuringFailover(() -> connection.getWarnings());
    }

    @Override
    public void clearWarnings() throws SQLException {
        wrapActionDuringFailover(() -> connection.clearWarnings());
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return wrapActionDuringFailover(() -> connection
                .createStatement(resultSetType, resultSetConcurrency));
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency) throws SQLException {
        return wrapActionDuringFailover(() -> connection
                .prepareStatement(sql, resultSetType, resultSetConcurrency));
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return wrapActionDuringFailover(() -> connection
                .prepareCall(sql, resultSetType, resultSetConcurrency));
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return wrapActionDuringFailover(() -> connection.getTypeMap());
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        wrapActionDuringFailover(() -> connection.setTypeMap(map));
    }

    @Override
    public int getHoldability() throws SQLException {
        return wrapActionDuringFailover(() -> connection.getHoldability());
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        wrapActionDuringFailover(() -> connection.setHoldability(holdability));
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return wrapActionDuringFailover(() -> connection.setSavepoint());
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return wrapActionDuringFailover(() -> connection.setSavepoint(name));
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        wrapActionDuringFailover(() -> connection.rollback(savepoint));
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        wrapActionDuringFailover(() -> connection.releaseSavepoint(savepoint));
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency,
                                     int resultSetHoldability) throws SQLException {
        return wrapActionDuringFailover(() -> connection
                .createStatement(resultSetType, resultSetConcurrency, resultSetHoldability));
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return wrapActionDuringFailover(() -> connection
                .prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability));
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        return wrapActionDuringFailover(() -> connection
                .prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability));
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
            throws SQLException {
        return wrapActionDuringFailover(() -> connection.prepareStatement(sql, autoGeneratedKeys));
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return wrapActionDuringFailover(() -> connection.prepareStatement(sql, columnIndexes));
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames)
            throws SQLException {
        return wrapActionDuringFailover(() -> connection.prepareStatement(sql, columnNames));
    }

    @Override
    public Clob createClob() throws SQLException {
        return wrapActionDuringFailover(() -> connection.createClob());
    }

    @Override
    public Blob createBlob() throws SQLException {
        return wrapActionDuringFailover(() -> connection.createBlob());
    }

    @Override
    public NClob createNClob() throws SQLException {
        return wrapActionDuringFailover(() -> connection.createNClob());
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        checkConnection();
        return wrapActionDuringFailover(() -> connection.createSQLXML());
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return wrapActionDuringFailover(() -> connection.isValid(timeout));
    }

    @Override
    public void setClientInfo(String name, String value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return wrapActionDuringFailover(() -> connection.getClientInfo(name));
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return wrapActionDuringFailover(() -> connection.getClientInfo());
    }

    @Override
    public void setClientInfo(Properties properties) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return wrapActionDuringFailover(() -> connection.createArrayOf(typeName, elements));
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return wrapActionDuringFailover(() -> connection.createStruct(typeName, attributes));
    }

    @Override
    public String getSchema() throws SQLException {
        return wrapActionDuringFailover(() -> connection.getSchema());
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        wrapActionDuringFailover(() -> connection.setSchema(schema));
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        wrapActionDuringFailover(() -> connection.abort(executor));
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        wrapActionDuringFailover(() -> connection.setNetworkTimeout(executor, milliseconds));
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return wrapActionDuringFailover(() -> connection.getNetworkTimeout());
    }

    /**
     * @return the currently wrapped connection.
     */
    @VisibleForTesting
    PhoenixConnection getWrappedConnection() {
        return connection;
    }

    @VisibleForTesting
    @FunctionalInterface
    interface SupplierWithSQLException<T> {
        T get() throws SQLException;
    }

    @VisibleForTesting
    @FunctionalInterface
    interface RunWithSQLException {
        void run() throws SQLException;
    }
}
