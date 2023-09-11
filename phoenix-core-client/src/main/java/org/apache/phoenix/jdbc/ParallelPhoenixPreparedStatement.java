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

import org.apache.phoenix.jdbc.PhoenixStatement.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

public class ParallelPhoenixPreparedStatement implements PhoenixMonitoredPreparedStatement {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(ParallelPhoenixPreparedStatement.class);

    private final ParallelPhoenixContext context;

    private final CompletableFuture<PhoenixMonitoredPreparedStatement> statement1;

    private final CompletableFuture<PhoenixMonitoredPreparedStatement> statement2;

    public ParallelPhoenixPreparedStatement(ParallelPhoenixContext context, CompletableFuture<PhoenixMonitoredPreparedStatement> statement1, CompletableFuture<PhoenixMonitoredPreparedStatement> statement2) throws SQLException {
        this.context = context;
        this.statement1 = statement1;
        this.statement2 = statement2;

        //todo: make sure 1 statement is completed
    }

    public CompletableFuture<PhoenixMonitoredPreparedStatement> getStatement1() {
        return statement1;
    }

    public CompletableFuture<PhoenixMonitoredPreparedStatement> getStatement2() {
        return statement2;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        CompletableFuture<ResultSet> result1 =
                ParallelPhoenixUtil.INSTANCE.getFutureAndChainOnContext(statement -> {
                            try {
                                return statement.executeQuery();
                            } catch (SQLException exception) {
                                throw new CompletionException(exception);
                            }
                        }, statement1, context::chainOnConn1,
                        context.getParallelPhoenixMetrics().getActiveClusterOperationCount(),
                        context.getParallelPhoenixMetrics().getActiveClusterFailedOperationCount());
        CompletableFuture<ResultSet> result2 =
                ParallelPhoenixUtil.INSTANCE.getFutureAndChainOnContext(statement -> {
                            try {
                                return statement.executeQuery();
                            } catch (SQLException exception) {
                                throw new CompletionException(exception);
                            }
                        }, statement2, context::chainOnConn2,
                        context.getParallelPhoenixMetrics().getStandbyClusterOperationCount(),
                        context.getParallelPhoenixMetrics().getStandbyClusterFailedOperationCount());

        return ParallelPhoenixResultSetFactory.INSTANCE.getParallelResultSet(context, result1, result2);
    }

    Object runOnPreparedStatements(Function<PhoenixMonitoredPreparedStatement, ?> function) throws SQLException {
        return ParallelPhoenixUtil.INSTANCE.runFutures(function, statement1, statement2, context, true);
    }

    @Override
    public int executeUpdate() throws SQLException {
        //TODO handle disabling connetions in a client to have all or nothing commits
        Function<PhoenixMonitoredPreparedStatement, Integer> function = (T) -> {
            try {
                return T.executeUpdate();
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        return (int) runOnPreparedStatements(function);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        Function<PhoenixMonitoredPreparedStatement, Void> function = (T) -> {
            try {
                T.setNull(parameterIndex, sqlType);
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        runOnPreparedStatements(function);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        Function<PhoenixMonitoredPreparedStatement, Void> function = (T) -> {
            try {
                T.setBoolean(parameterIndex, x);
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        runOnPreparedStatements(function);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        Function<PhoenixMonitoredPreparedStatement, Void> function = (T) -> {
            try {
                T.setByte(parameterIndex, x);
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        runOnPreparedStatements(function);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        Function<PhoenixMonitoredPreparedStatement, Void> function = (T) -> {
            try {
                T.setShort(parameterIndex, x);
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        runOnPreparedStatements(function);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        Function<PhoenixMonitoredPreparedStatement, Void> function = (T) -> {
            try {
                T.setInt(parameterIndex, x);
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        runOnPreparedStatements(function);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        Function<PhoenixMonitoredPreparedStatement, Void> function = (T) -> {
            try {
                T.setLong(parameterIndex, x);
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        runOnPreparedStatements(function);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        Function<PhoenixMonitoredPreparedStatement, Void> function = (T) -> {
            try {
                T.setFloat(parameterIndex, x);
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        runOnPreparedStatements(function);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        Function<PhoenixMonitoredPreparedStatement, Void> function = (T) -> {
            try {
                T.setDouble(parameterIndex, x);
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        runOnPreparedStatements(function);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        Function<PhoenixMonitoredPreparedStatement, Void> function = (T) -> {
            try {
                T.setBigDecimal(parameterIndex, x);
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        runOnPreparedStatements(function);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        Function<PhoenixMonitoredPreparedStatement, Void> function = (T) -> {
            try {
                T.setString(parameterIndex, x);
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        runOnPreparedStatements(function);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        Function<PhoenixMonitoredPreparedStatement, Void> function = (T) -> {
            try {
                T.setBytes(parameterIndex, x);
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        runOnPreparedStatements(function);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        Function<PhoenixMonitoredPreparedStatement, Void> function = (T) -> {
            try {
                T.setDate(parameterIndex, x);
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        runOnPreparedStatements(function);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        Function<PhoenixMonitoredPreparedStatement, Void> function = (T) -> {
            try {
                T.setTime(parameterIndex, x);
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        runOnPreparedStatements(function);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        Function<PhoenixMonitoredPreparedStatement, Void> function = (T) -> {
            try {
                T.setTimestamp(parameterIndex, x);
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        runOnPreparedStatements(function);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        Function<PhoenixMonitoredPreparedStatement, Void> function = (T) -> {
            try {
                T.setAsciiStream(parameterIndex, x, length);
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        runOnPreparedStatements(function);
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        Function<PhoenixMonitoredPreparedStatement, Void> function = (T) -> {
            try {
                T.setUnicodeStream(parameterIndex, x, length);
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        runOnPreparedStatements(function);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        Function<PhoenixMonitoredPreparedStatement, Void> function = (T) -> {
            try {
                T.setBinaryStream(parameterIndex, x, length);
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        runOnPreparedStatements(function);
    }

    @Override
    public void clearParameters() throws SQLException {
        Function<PhoenixMonitoredPreparedStatement, Void> function = (T) -> {
            try {
                T.clearParameters();
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        runOnPreparedStatements(function);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        Function<PhoenixMonitoredPreparedStatement, Void> function = (T) -> {
            try {
                T.setObject(parameterIndex, x, targetSqlType);
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        runOnPreparedStatements(function);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        Function<PhoenixMonitoredPreparedStatement, Void> function = (T) -> {
            try {
                T.setObject(parameterIndex, x);
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        runOnPreparedStatements(function);
    }

    @Override
    public boolean execute() throws SQLException {
        Function<PhoenixMonitoredPreparedStatement, Boolean> function = (T) -> {
            try {
                return T.execute();
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        return (boolean) runOnPreparedStatements(function);
    }

    @Override
    public Operation getUpdateOperation() throws SQLException {
        Function<PhoenixMonitoredPreparedStatement, Operation> function = (T) -> {
            try {
                return T.getUpdateOperation();
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        return (Operation) runOnPreparedStatements(function);
    }

    @Override
    public void addBatch() throws SQLException {

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {

    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {

    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {

    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {

    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {

    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {

    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {

    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {

    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {

    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {

    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {

    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {

    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {

    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {

    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {

    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {

    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        Function<PhoenixMonitoredPreparedStatement, ResultSet> function = (T) -> {
            try {
                T.executeQuery(sql);
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        return (ResultSet) runOnPreparedStatements(function);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        Function<PhoenixMonitoredPreparedStatement, Integer> function = (T) -> {
            try {
                T.executeUpdate(sql);
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        return (int) runOnPreparedStatements(function);
    }

    @Override
    public void close() throws SQLException {
        Function<PhoenixMonitoredPreparedStatement, Void> function = (T) -> {
            try {
                T.close();
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        runOnPreparedStatements(function);
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {

    }

    @Override
    public int getMaxRows() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {

    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {

    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {

    }

    @Override
    public void cancel() throws SQLException {

    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public void setCursorName(String name) throws SQLException {

    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        // TODO: implement using the ParallelPhoenixResultSetFactory
        return null;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return 0;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return 0;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {

    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {

    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return 0;
    }

    @Override
    public void addBatch(String sql) throws SQLException {

    }

    @Override
    public void clearBatch() throws SQLException {

    }

    @Override
    public int[] executeBatch() throws SQLException {
        return new int[0];
    }

    @Override
    public Connection getConnection() throws SQLException {
        //TODO Inject ParallelPhoenixConnection
        return null;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return 0;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {

    }

    @Override
    public void closeOnCompletion() throws SQLException {

    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return (T) this;
        }
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

}
