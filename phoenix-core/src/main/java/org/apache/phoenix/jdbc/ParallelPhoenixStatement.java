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

import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.commons.lang3.ArrayUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

import org.apache.phoenix.jdbc.PhoenixStatement.Operation;

public class ParallelPhoenixStatement implements PhoenixMonitoredStatement {
    private final ParallelPhoenixContext context;
    private final CompletableFuture<PhoenixMonitoredStatement> statement1;

    CompletableFuture<PhoenixMonitoredStatement> getStatement1() {
        return statement1;
    }

    CompletableFuture<PhoenixMonitoredStatement> getStatement2() {
        return statement2;
    }

    private final CompletableFuture<PhoenixMonitoredStatement> statement2;

    public ParallelPhoenixStatement(ParallelPhoenixContext context, CompletableFuture<PhoenixMonitoredStatement> statement1, CompletableFuture<PhoenixMonitoredStatement> statement2) throws SQLException {
        this.context = context;
        this.statement1 = statement1;
        this.statement2 = statement2;
    }

    Object runOnStatements(Function<PhoenixMonitoredStatement,?> function) throws SQLException {
        return ParallelPhoenixUtil.INSTANCE.runFutures(function,statement1,statement2,context, true);
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {

        Function<PhoenixMonitoredStatement, ResultSet> function = (T) -> {
            try {
                return T.executeQuery(sql);
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        List<CompletableFuture<ResultSet>> futures = ParallelPhoenixUtil.INSTANCE.applyFunctionToFutures(function, statement1,
                statement2, context, true);

        Preconditions.checkState(futures.size() == 2);
        CompletableFuture<ResultSet> rs1 = futures.get(0);
        CompletableFuture<ResultSet> rs2 = futures.get(1);

        //Ensure one statement is successful before returning
        ParallelPhoenixUtil.INSTANCE.runFutures(futures, context, true);

        return ParallelPhoenixResultSetFactory.INSTANCE.getParallelResultSet(context, rs1, rs2);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        Function<PhoenixMonitoredStatement, Integer> function = (T) -> {
            try {
                return T.executeUpdate(sql);
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        return (int) runOnStatements(function);
    }

    @Override
    public void close() throws SQLException {
        Function<PhoenixMonitoredStatement, Void> function = (T) -> {
            try {
                T.close();
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        runOnStatements(function);
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        Function<PhoenixMonitoredStatement, Integer> function = (T) -> {
            try {
                return T.getMaxFieldSize();
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        return (int) runOnStatements(function);
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        Function<PhoenixMonitoredStatement, Void> function = (T) -> {
            try {
                T.setMaxFieldSize(max);
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        runOnStatements(function);
    }

    @Override
    public int getMaxRows() throws SQLException {
        Function<PhoenixMonitoredStatement, Integer> function = (T) -> {
            try {
                return T.getMaxRows();
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        return (int) runOnStatements(function);
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        Function<PhoenixMonitoredStatement, Void> function = (T) -> {
            try {
                T.setMaxRows(max);
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        runOnStatements(function);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        Function<PhoenixMonitoredStatement, Void> function = (T) -> {
            try {
                T.setEscapeProcessing(enable);
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        runOnStatements(function);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        Function<PhoenixMonitoredStatement, Integer> function = (T) -> {
            try {
                return T.getQueryTimeout();
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        return (int) runOnStatements(function);
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        Function<PhoenixMonitoredStatement, Void> function = (T) -> {
            try {
                T.setQueryTimeout(seconds);
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        runOnStatements(function);
    }

    @Override
    public void cancel() throws SQLException {
        Function<PhoenixMonitoredStatement, Void> function = (T) -> {
            try {
                T.cancel();
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        runOnStatements(function);
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        Function<PhoenixMonitoredStatement, SQLWarning> function = (T) -> {
            try {
                return T.getWarnings();
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        return (SQLWarning) runOnStatements(function);
    }

    @Override
    public void clearWarnings() throws SQLException {
        Function<PhoenixMonitoredStatement, Void> function = (T) -> {
            try {
                T.clearWarnings();
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        runOnStatements(function);
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        Function<PhoenixMonitoredStatement, Void> function = (T) -> {
            try {
                T.setCursorName(name);
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        runOnStatements(function);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        Function<PhoenixMonitoredStatement, Boolean> function = (T) -> {
            try {
                return T.execute(sql);
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        return (boolean) runOnStatements(function);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        Function<PhoenixMonitoredStatement, ResultSet> function = (T) -> {
            try {
                return T.getResultSet();
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        List<CompletableFuture<ResultSet>> futures = ParallelPhoenixUtil.INSTANCE.applyFunctionToFutures(function, statement1,
                statement2, context, true);

        Preconditions.checkState(futures.size() == 2);
        CompletableFuture<ResultSet> rs1 = futures.get(0);
        CompletableFuture<ResultSet> rs2 = futures.get(1);

        //Ensure one statement is successful before returning
        ParallelPhoenixUtil.INSTANCE.runFutures(futures, context, true);

        return ParallelPhoenixResultSetFactory.INSTANCE.getParallelResultSet(context, rs1, rs2);
    }

    @Override
    public int getUpdateCount() throws SQLException {
        Function<PhoenixMonitoredStatement, Integer> function = (T) -> {
            try {
                return T.getUpdateCount();
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        return (int) runOnStatements(function);
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        Function<PhoenixMonitoredStatement, Boolean> function = (T) -> {
            try {
                return T.getMoreResults();
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        return (boolean) runOnStatements(function);
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        Function<PhoenixMonitoredStatement, Void> function = (T) -> {
            try {
                T.setFetchDirection(direction);
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        runOnStatements(function);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        Function<PhoenixMonitoredStatement, Integer> function = (T) -> {
            try {
                return T.getUpdateCount();
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        return (int) runOnStatements(function);
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        Function<PhoenixMonitoredStatement, Integer> function = (T) -> {
            try {
                T.setFetchSize(rows);
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        runOnStatements(function);
    }

    @Override
    public int getFetchSize() throws SQLException {
        Function<PhoenixMonitoredStatement, Integer> function = (T) -> {
            try {
                return T.getFetchSize();
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        return (int) runOnStatements(function);
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        Function<PhoenixMonitoredStatement, Integer> function = (T) -> {
            try {
                return T.getResultSetConcurrency();
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        return (int) runOnStatements(function);
    }

    @Override
    public int getResultSetType() throws SQLException {
        Function<PhoenixMonitoredStatement, Integer> function = (T) -> {
            try {
                return T.getResultSetType();
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        return (int) runOnStatements(function);
    }

    @Override
    public Operation getUpdateOperation() throws SQLException {
        Function<PhoenixMonitoredStatement, Operation> function = (T) -> {
            try {
                return T.getUpdateOperation();
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        return (Operation) runOnStatements(function);
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        Function<PhoenixMonitoredStatement, Void> function = (T) -> {
            try {
                T.addBatch(sql);
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        runOnStatements(function);
    }

    @Override
    public void clearBatch() throws SQLException {
        Function<PhoenixMonitoredStatement, Void> function = (T) -> {
            try {
                T.clearBatch();
                return null;
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        runOnStatements(function);
    }

    @Override
    public int[] executeBatch() throws SQLException {
        Function<PhoenixMonitoredStatement, Integer[]> function = (T) -> {
            try {
                return ArrayUtils.toObject(T.executeBatch());
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        return ArrayUtils.toPrimitive((Integer[])runOnStatements(function));
    }

    @Override
    public Connection getConnection() throws SQLException {
        return null; //TODO: Push parallel context to this layer
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        Function<PhoenixMonitoredStatement, Boolean> function = (T) -> {
            try {
                return T.getMoreResults(current);
            } catch (SQLException exception) {
                throw new CompletionException(exception);
            }
        };

        return (boolean)runOnStatements(function);
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException();
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
        throw new SQLFeatureNotSupportedException();
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
        return context.isClosed();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isPoolable() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public long getLargeUpdateCount() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setLargeMaxRows(long max) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public long getLargeMaxRows() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return (T) this;
        }
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
