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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.phoenix.compile.ColumnProjector;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDataType;

/**
 * 
 * JDBC ResultSetMetaData implementation of Phoenix.
 * Currently only the following methods are supported:
 * - {@link #getColumnCount()}
 * - {@link #getColumnDisplaySize(int)}
 * - {@link #getColumnLabel(int)} displays alias name if present and column name otherwise
 * - {@link #getColumnName(int)} same as {@link #getColumnLabel(int)}
 * - {@link #isCaseSensitive(int)}
 * - {@link #getColumnType(int)}
 * - {@link #getColumnTypeName(int)}
 * - {@link #getTableName(int)}
 * - {@link #getSchemaName(int)} always returns empty string
 * - {@link #getCatalogName(int)} always returns empty string
 * - {@link #isNullable(int)}
 * - {@link #isSigned(int)}
 * - {@link #isAutoIncrement(int)} always false
 * - {@link #isCurrency(int)} always false
 * - {@link #isDefinitelyWritable(int)} always false
 * - {@link #isReadOnly(int)} always true
 * - {@link #isSearchable(int)} always true
 * 
 * 
 * @since 0.1
 */
public class PhoenixResultSetMetaData implements ResultSetMetaData {
    static final int DEFAULT_DISPLAY_WIDTH = 40;
    private final RowProjector rowProjector;
    private final PhoenixConnection connection;
    
    public PhoenixResultSetMetaData(PhoenixConnection connection, RowProjector projector) {
        this.connection = connection;
        this.rowProjector = projector;
    }
    
    @Override
    public String getCatalogName(int column) throws SQLException {
        return "";
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        PDataType type = rowProjector.getColumnProjector(column-1).getExpression().getDataType();
        return type == null ? null : type.getJavaClassName();
    }

    @Override
    public int getColumnCount() throws SQLException {
        return rowProjector.getColumnCount();
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        ColumnProjector projector = rowProjector.getColumnProjector(column-1);
        PDataType type = projector.getExpression().getDataType();
        if (type == null) {
            return QueryConstants.NULL_DISPLAY_TEXT.length();
        }
        if (type.isCoercibleTo(PDate.INSTANCE)) {
            return connection.getDatePattern().length();
        }
        if (projector.getExpression().getMaxLength() != null) {
            return projector.getExpression().getMaxLength();
        }
        return DEFAULT_DISPLAY_WIDTH;
    }
    
    @Override
    public String getColumnLabel(int column) throws SQLException {
        return rowProjector.getColumnProjector(column-1).getName();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        // TODO: will return alias if there is one
        return rowProjector.getColumnProjector(column-1).getName();
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        PDataType type = rowProjector.getColumnProjector(column-1).getExpression().getDataType();
        return type == null ? Types.NULL : type.getResultSetSqlType();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        PDataType type = rowProjector.getColumnProjector(column-1).getExpression().getDataType();
        return type == null ? null : type.getSqlTypeName();
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        Integer precision = rowProjector.getColumnProjector(column-1).getExpression().getMaxLength();
        return precision == null ? 0 : precision;
    }

    @Override
    public int getScale(int column) throws SQLException {
        Integer scale = rowProjector.getColumnProjector(column-1).getExpression().getScale();
        return scale == null ? 0 : scale;
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return ""; // TODO
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return rowProjector.getColumnProjector(column-1).getTableName();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return rowProjector.getColumnProjector(column-1).isCaseSensitive();
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return rowProjector.getColumnProjector(column-1).getExpression().isNullable() ? ResultSetMetaData.columnNullable : ResultSetMetaData.columnNoNulls;
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        PDataType type = rowProjector.getColumnProjector(column-1).getExpression().getDataType();
        if (type == null) {
            return false;
        }
        return type.isCoercibleTo(PDecimal.INSTANCE);
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (!iface.isInstance(this)) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CLASS_NOT_UNWRAPPABLE)
                .setMessage(this.getClass().getName() + " not unwrappable from " + iface.getName())
                .build().buildException();
        }
        return (T)this;
    }
    
}
