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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

import org.apache.phoenix.compile.BindManager;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.StatementPlan;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.SQLCloseable;

/**
 * JDBC PreparedStatement implementation of Phoenix. Currently only the following methods (in addition to the ones
 * supported on {@link PhoenixStatement} are supported: - {@link #executeQuery()} - {@link #setInt(int, int)} -
 * {@link #setShort(int, short)} - {@link #setLong(int, long)} - {@link #setFloat(int, float)} -
 * {@link #setDouble(int, double)} - {@link #setBigDecimal(int, BigDecimal)} - {@link #setString(int, String)} -
 * {@link #setDate(int, Date)} - {@link #setDate(int, Date, Calendar)} - {@link #setTime(int, Time)} -
 * {@link #setTime(int, Time, Calendar)} - {@link #setTimestamp(int, Timestamp)} -
 * {@link #setTimestamp(int, Timestamp, Calendar)} - {@link #setNull(int, int)} - {@link #setNull(int, int, String)} -
 * {@link #setBytes(int, byte[])} - {@link #clearParameters()} - {@link #getMetaData()}
 * 
 * 
 * @since 0.1
 */
public class PhoenixPreparedStatement extends PhoenixStatement implements PreparedStatement, SQLCloseable {
    private final List<Object> parameters;
    private final ExecutableStatement statement;

    private final String query;

    public PhoenixPreparedStatement(PhoenixConnection connection, PhoenixStatementParser parser) throws SQLException,
            IOException {
        super(connection);
        this.statement = parser.nextStatement(new ExecutableNodeFactory());
        if (this.statement == null) { throw new EOFException(); }
        this.query = null; // TODO: add toString on SQLStatement
        this.parameters = Arrays.asList(new Object[statement.getBindCount()]);
        Collections.fill(parameters, BindManager.UNBOUND_PARAMETER);
    }

    public PhoenixPreparedStatement(PhoenixConnection connection, String query) throws SQLException {
        super(connection);
        this.query = query;
        this.statement = parseStatement(query);
        this.parameters = Arrays.asList(new Object[statement.getBindCount()]);
        Collections.fill(parameters, BindManager.UNBOUND_PARAMETER);
    }

    @Override
    public void addBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void clearParameters() throws SQLException {
        Collections.fill(parameters, BindManager.UNBOUND_PARAMETER);
    }

    @Override
    public List<Object> getParameters() {
        return parameters;
    }

    @Override
    public boolean execute() throws SQLException {
        throwIfUnboundParameters();
        return statement.execute();
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        throwIfUnboundParameters();
        return statement.executeQuery();
    }

    public QueryPlan optimizeQuery() throws SQLException {
        throwIfUnboundParameters();
        return (QueryPlan)statement.optimizePlan();
    }


    @Override
    public int executeUpdate() throws SQLException {
        throwIfUnboundParameters();
        return statement.executeUpdate();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        int paramCount = statement.getBindCount();
        List<Object> params = this.getParameters();
        BitSet unsetParams = new BitSet(statement.getBindCount());
        for (int i = 0; i < paramCount; i++) {
            if ( params.get(i) == BindManager.UNBOUND_PARAMETER) {
                unsetParams.set(i);
                params.set(i, null);
            }
        }
        try {
            return statement.getResultSetMetaData();
        } finally {
            int lastSetBit = 0;
            while ((lastSetBit = unsetParams.nextSetBit(lastSetBit)) != -1) {
                params.set(lastSetBit, BindManager.UNBOUND_PARAMETER);
                lastSetBit++;
            }
        }
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        int paramCount = statement.getBindCount();
        List<Object> params = this.getParameters();
        BitSet unsetParams = new BitSet(statement.getBindCount());
        for (int i = 0; i < paramCount; i++) {
            if ( params.get(i) == BindManager.UNBOUND_PARAMETER) {
                unsetParams.set(i);
                params.set(i, null);
            }
        }
        try {
            StatementPlan plan = statement.compilePlan();
            return plan.getParameterMetaData();
        } finally {
            int lastSetBit = 0;
            while ((lastSetBit = unsetParams.nextSetBit(lastSetBit)) != -1) {
                params.set(lastSetBit, BindManager.UNBOUND_PARAMETER);
                lastSetBit++;
            }
        }
    }

    @Override
    public String toString() {
        return query;
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        cal.setTime(x);
        parameters.set(parameterIndex - 1, new Date(cal.getTimeInMillis()));
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
//        parameters.set(parameterIndex - 1, BigDecimal.valueOf(x));
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
//        parameters.set(parameterIndex - 1, BigDecimal.valueOf(x));
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        parameters.set(parameterIndex - 1, null);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        parameters.set(parameterIndex - 1, null);
    }

    @Override
    public void setObject(int parameterIndex, Object o) throws SQLException {
        parameters.set(parameterIndex - 1, o);
    }

    @Override
    public void setObject(int parameterIndex, Object o, int targetSqlType) throws SQLException {
        PDataType targetType = PDataType.fromSqlType(targetSqlType);
        PDataType sourceType = PDataType.fromLiteral(o);
        o = targetType.toObject(o, sourceType);
        parameters.set(parameterIndex - 1, o);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x, targetSqlType);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        cal.setTime(x);
        parameters.set(parameterIndex - 1, new Time(cal.getTimeInMillis()));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        cal.setTime(x);
        parameters.set(parameterIndex - 1,  DateUtil.getTimestamp(cal.getTimeInMillis(), x.getNanos()));
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        parameters.set(parameterIndex - 1, x.toExternalForm()); // Just treat as String
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
}
