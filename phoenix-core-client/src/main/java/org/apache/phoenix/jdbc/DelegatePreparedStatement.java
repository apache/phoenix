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
import java.sql.PreparedStatement;
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

public class DelegatePreparedStatement implements PreparedStatement {

    protected PreparedStatement ps;
    
    public DelegatePreparedStatement(PreparedStatement ps) {
        this.ps = ps;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return ps.unwrap(iface);
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return ps.executeQuery(sql);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return ps.executeQuery();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return ps.isWrapperFor(iface);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return ps.executeUpdate(sql);
    }

    @Override
    public int executeUpdate() throws SQLException {
        return ps.executeUpdate();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        ps.setNull(parameterIndex, sqlType);
    }

    @Override
    public void close() throws SQLException {
        ps.close();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return ps.getMaxFieldSize();
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        ps.setBoolean(parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        ps.setByte(parameterIndex, x);
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        ps.setMaxFieldSize(max);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        ps.setShort(parameterIndex, x);
    }

    @Override
    public int getMaxRows() throws SQLException {
        return ps.getMaxRows();
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        ps.setInt(parameterIndex, x);
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        ps.setMaxRows(max);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        ps.setLong(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        ps.setFloat(parameterIndex, x);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        ps.setEscapeProcessing(enable);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        ps.setDouble(parameterIndex, x);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return ps.getQueryTimeout();
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        ps.setBigDecimal(parameterIndex, x);
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        ps.setQueryTimeout(seconds);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        ps.setString(parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        ps.setBytes(parameterIndex, x);
    }

    @Override
    public void cancel() throws SQLException {
        ps.cancel();
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        ps.setDate(parameterIndex, x);
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return ps.getWarnings();
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        ps.setTime(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        ps.setTimestamp(parameterIndex, x);
    }

    @Override
    public void clearWarnings() throws SQLException {
        ps.clearWarnings();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        ps.setCursorName(name);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        ps.setAsciiStream(parameterIndex, x, length);
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length)
            throws SQLException {
        ps.setUnicodeStream(parameterIndex, x, length);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return ps.execute(sql);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        ps.setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return ps.getResultSet();
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return ps.getUpdateCount();
    }

    @Override
    public void clearParameters() throws SQLException {
        ps.clearParameters();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return ps.getMoreResults();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        ps.setObject(parameterIndex, x, targetSqlType);
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        ps.setFetchDirection(direction);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        ps.setObject(parameterIndex, x);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ps.getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        ps.setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return ps.getFetchSize();
    }

    @Override
    public boolean execute() throws SQLException {
        return ps.execute();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return ps.getResultSetConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
        return ps.getResultSetType();
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        ps.addBatch(sql);
    }

    @Override
    public void addBatch() throws SQLException {
        ps.addBatch();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length)
            throws SQLException {
        ps.setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void clearBatch() throws SQLException {
        ps.clearBatch();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return ps.executeBatch();
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        ps.setRef(parameterIndex, x);
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        ps.setBlob(parameterIndex, x);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        ps.setClob(parameterIndex, x);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        ps.setArray(parameterIndex, x);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return ps.getConnection();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return ps.getMetaData();
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        ps.setDate(parameterIndex, x, cal);
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return ps.getMoreResults(current);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        ps.setTime(parameterIndex, x, cal);
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return ps.getGeneratedKeys();
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        ps.setTimestamp(parameterIndex, x, cal);
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return ps.executeUpdate(sql, autoGeneratedKeys);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        ps.setNull(parameterIndex, sqlType, typeName);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return ps.executeUpdate(sql, columnIndexes);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        ps.setURL(parameterIndex, x);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return ps.getParameterMetaData();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        ps.setRowId(parameterIndex, x);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return ps.executeUpdate(sql, columnNames);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        ps.setNString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length)
            throws SQLException {
        ps.setNCharacterStream(parameterIndex, value, length);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return ps.execute(sql, autoGeneratedKeys);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        ps.setNClob(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        ps.setClob(parameterIndex, reader, length);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return ps.execute(sql, columnIndexes);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length)
            throws SQLException {
        ps.setBlob(parameterIndex, inputStream, length);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        ps.setNClob(parameterIndex, reader, length);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return ps.execute(sql, columnNames);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        ps.setSQLXML(parameterIndex, xmlObject);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
            throws SQLException {
        ps.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ps.getResultSetHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return ps.isClosed();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        ps.setPoolable(poolable);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        ps.setAsciiStream(parameterIndex, x, length);
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return ps.isPoolable();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        ps.closeOnCompletion();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length)
            throws SQLException {
        ps.setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return ps.isCloseOnCompletion();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length)
            throws SQLException {
        ps.setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        ps.setAsciiStream(parameterIndex, x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        ps.setBinaryStream(parameterIndex, x);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        ps.setCharacterStream(parameterIndex, reader);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        ps.setNCharacterStream(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        ps.setClob(parameterIndex, reader);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        ps.setBlob(parameterIndex, inputStream);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        ps.setNClob(parameterIndex, reader);
    }

}
