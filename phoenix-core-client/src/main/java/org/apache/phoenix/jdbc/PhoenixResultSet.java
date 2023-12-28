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

import static org.apache.phoenix.coprocessorclient.ScanRegionObserverConstants.DYN_COLS_METADATA_CELL_QUALIFIER;
import static org.apache.phoenix.query.QueryServices.WILDCARD_QUERY_DYNAMIC_COLS_ATTRIB;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_WILDCARD_QUERY_DYNAMIC_COLS_ATTRIB;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.Format;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.phoenix.monitoring.TableMetricsManager;
import org.apache.phoenix.thirdparty.com.google.common.primitives.Bytes;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.ColumnProjector;
import org.apache.phoenix.compile.ExpressionProjector;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.coprocessor.generated.DynamicColumnMetaDataProtos;
import org.apache.phoenix.coprocessor.generated.PTableProtos;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ProjectedColumnExpression;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.log.QueryLogInfo;
import org.apache.phoenix.log.QueryLogger;
import org.apache.phoenix.log.QueryStatus;
import org.apache.phoenix.monitoring.MetricType;
import org.apache.phoenix.monitoring.OverAllQueryMetrics;
import org.apache.phoenix.monitoring.ReadMetricQueue;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PFloat;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PSmallint;
import org.apache.phoenix.schema.types.PTime;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PTinyint;
import org.apache.phoenix.schema.types.PUnsignedDate;
import org.apache.phoenix.schema.types.PUnsignedTime;
import org.apache.phoenix.schema.types.PUnsignedTimestamp;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.SQLCloseable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.base.Throwables;
import org.apache.phoenix.thirdparty.com.google.common.base.Strings;
import org.apache.phoenix.util.SchemaUtil;

/**
 *
 * JDBC ResultSet implementation of Phoenix.
 * Currently only the following data types are supported:
 * - String
 * - Date
 * - Time
 * - Timestamp
 * - BigDecimal
 * - Double
 * - Float
 * - Int
 * - Short
 * - Long
 * - Binary
 * - Array - 1D
 * None of the update or delete methods are supported.
 * The ResultSet only supports the following options:
 * - ResultSet.FETCH_FORWARD
 * - ResultSet.CONCUR_READ_ONLY
 * - ResultSet.TYPE_FORWARD_ONLY
 * - ResultSet.CLOSE_CURSORS_AT_COMMIT
 *
 *
 * @since 0.1
 */
public class PhoenixResultSet implements PhoenixMonitoredResultSet, SQLCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixResultSet.class);

    private final static String STRING_FALSE = "0";
    private final static BigDecimal BIG_DECIMAL_FALSE = BigDecimal.valueOf(0);
    private final static Integer INTEGER_FALSE = Integer.valueOf(0);
    private final static Tuple BEFORE_FIRST = ResultTuple.EMPTY_TUPLE;

    private final ResultIterator scanner;
    private final RowProjector rowProjector;
    private final PhoenixStatement statement;
    private final StatementContext context;
    private final ReadMetricQueue readMetricsQueue;
    private final OverAllQueryMetrics overAllQueryMetrics;
    private final ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    private final boolean wildcardIncludesDynamicCols;
    private final List<PColumn> staticColumns;
    private final int startPositionForDynamicCols;
    private final boolean isApplyTimeZoneDisplacement;

    private RowProjector rowProjectorWithDynamicCols;
    private Tuple currentRow = BEFORE_FIRST;
    private boolean isClosed = false;
    private boolean wasNull = false;
    private boolean firstRecordRead = false;

    private QueryLogger queryLogger;

    private Long count = 0L;

    private Object exception;
    private long queryTime;
    private final Calendar localCalendar;

    public PhoenixResultSet(ResultIterator resultIterator, RowProjector rowProjector,
            StatementContext ctx) throws SQLException {
        this.rowProjector = rowProjector;
        this.scanner = resultIterator;
        this.context = ctx;
        this.statement = context.getStatement();
        statement.setLastResultSet(this);
        this.readMetricsQueue = context.getReadMetricsQueue();
        this.overAllQueryMetrics = context.getOverallQueryMetrics();
        this.queryLogger = context.getQueryLogger() != null ? context.getQueryLogger() : QueryLogger.NO_OP_INSTANCE;
        this.wildcardIncludesDynamicCols = this.context.getConnection().getQueryServices()
                .getConfiguration().getBoolean(WILDCARD_QUERY_DYNAMIC_COLS_ATTRIB,
                        DEFAULT_WILDCARD_QUERY_DYNAMIC_COLS_ATTRIB);
        if (this.wildcardIncludesDynamicCols) {
            Pair<List<PColumn>, Integer> res = getStaticColsAndStartingPosForDynCols();
            this.staticColumns = res.getFirst();
            this.startPositionForDynamicCols = res.getSecond();
        } else {
            this.staticColumns = null;
            this.startPositionForDynamicCols = 0;
        }
        this.isApplyTimeZoneDisplacement = statement.getConnection().isApplyTimeZoneDisplacement();
        this.localCalendar = statement.getLocalCalendar();
    }
    
    @Override
    public boolean absolute(int row) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void afterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void beforeFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void clearWarnings() throws SQLException {
    }

    @Override
    public void close() throws SQLException {
        if (isClosed) {
            return;
        }
        try {
            scanner.close();
        } finally {
            isClosed = true;
            statement.removeResultSet(this);
            overAllQueryMetrics.endQuery();
            overAllQueryMetrics.stopResultSetWatch();
            if (context.getCurrentTable() != null && context.getCurrentTable().getTable() != null
                    && !Strings.isNullOrEmpty(
                    context.getCurrentTable().getTable().getPhysicalName().getString())) {
                boolean isPointLookup = context.getScanRanges().isPointLookup();
                String tableName =
                        context.getCurrentTable().getTable().getPhysicalName().toString();
                updateTableLevelReadMetrics(tableName, isPointLookup);
            }
            if (!queryLogger.isSynced()) {
                if(this.exception==null){
                    queryLogger.log(QueryLogInfo.QUERY_STATUS_I,QueryStatus.COMPLETED.toString());
                }
                queryLogger.log(QueryLogInfo.NO_OF_RESULTS_ITERATED_I, count);
                if (queryLogger.isDebugEnabled()) {
                    queryLogger.log(QueryLogInfo.SCAN_METRICS_JSON_I,
                            readMetricsQueue.getScanMetricsHolderList().toString());
                    readMetricsQueue.getScanMetricsHolderList().clear();
                }
                // if not already synced , like closing before result set exhausted
                queryLogger.sync(getReadMetrics(), getOverAllRequestReadMetrics());
            }
        }
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        Integer index = getRowProjector().getColumnIndex(columnLabel);
        return index + 1;
    }

    @Override
    public boolean first() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
    	checkCursorState();
        // Get the value using the expected type instead of trying to coerce to VARCHAR.
        // We can't coerce using our formatter because we don't have enough context in PDataType.
        ColumnProjector projector = getRowProjector().getColumnProjector(columnIndex-1);
        Array value = (Array)projector.getValue(currentRow, projector.getExpression().getDataType(), ptr);
        wasNull = (value == null);
        return value;
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return getArray(findColumn(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    private void checkOpen() throws SQLException {
        if (isClosed) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.RESULTSET_CLOSED).build().buildException();
        }
    }

    private void checkCursorState() throws SQLException {
        checkOpen();
        if (currentRow == BEFORE_FIRST) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CURSOR_BEFORE_FIRST_ROW).build().buildException();
        }else if (currentRow == null) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CURSOR_PAST_LAST_ROW).build().buildException();
        }
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        checkCursorState();
        BigDecimal value = (BigDecimal)getRowProjector().getColumnProjector(columnIndex-1)
                .getValue(currentRow, PDecimal.INSTANCE, ptr);
        wasNull = (value == null);
        return value;
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        BigDecimal value = getBigDecimal(columnIndex);
        if (wasNull) {
            return null;
        }
        return value.setScale(scale);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel), scale);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        checkCursorState();
        ColumnProjector colProjector = getRowProjector().getColumnProjector(columnIndex-1);
        PDataType type = colProjector.getExpression().getDataType();
        Object value = colProjector.getValue(currentRow, type, ptr);
        wasNull = (value == null);
        if (wasNull) {
            return false;
        }
        if (type == PBoolean.INSTANCE) {
          return Boolean.TRUE.equals(value);
        } else if (type == PVarchar.INSTANCE) {
          return !STRING_FALSE.equals(value);
        } else if (type == PInteger.INSTANCE) {
          return !INTEGER_FALSE.equals(value);
        } else if (type == PDecimal.INSTANCE) {
          return !BIG_DECIMAL_FALSE.equals(value);
        } else {
          throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_CALL_METHOD_ON_TYPE)
              .setMessage("Method: getBoolean; Type:" + type).build().buildException();
        }
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        checkCursorState();
        byte[] value = (byte[])getRowProjector().getColumnProjector(columnIndex-1)
                .getValue(currentRow, PVarbinary.INSTANCE, ptr);
        wasNull = (value == null);
        return value;
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
//        throw new SQLFeatureNotSupportedException();
        checkCursorState();
        Byte value = (Byte)getRowProjector().getColumnProjector(columnIndex-1).getValue(currentRow,
            PTinyint.INSTANCE, ptr);
        wasNull = (value == null);
        if (value == null) {
            return 0;
        }
        return value;
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return getDate(columnIndex, localCalendar);
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        checkCursorState();
        Date value =
                (Date) getRowProjector().getColumnProjector(columnIndex - 1).getValue(currentRow,
                    PDate.INSTANCE, ptr);
        wasNull = (value == null);
        if (wasNull) {
            return null;
        }
        if (isApplyTimeZoneDisplacement) {
            return DateUtil.applyOutputDisplacement(value, cal.getTimeZone());
        } else {
            return value;
        }
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(findColumn(columnLabel), cal);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        checkCursorState();
        Double value = (Double)getRowProjector().getColumnProjector(columnIndex-1)
                .getValue(currentRow, PDouble.INSTANCE, ptr);
        wasNull = (value == null);
        if (wasNull) {
            return 0;
        }
        return value;
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public int getFetchSize() throws SQLException {
        return statement.getFetchSize();
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        checkCursorState();
        Float value = (Float)getRowProjector().getColumnProjector(columnIndex-1)
                .getValue(currentRow, PFloat.INSTANCE, ptr);
        wasNull = (value == null);
        if (wasNull) {
            return 0;
        }
        return value;
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        checkCursorState();
        Integer value = (Integer)getRowProjector().getColumnProjector(columnIndex-1)
                .getValue(currentRow, PInteger.INSTANCE, ptr);
        wasNull = (value == null);
        if (wasNull) {
            return 0;
        }
        return value;
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        checkCursorState();
        Long value = (Long)getRowProjector().getColumnProjector(columnIndex-1).getValue(currentRow,
            PLong.INSTANCE, ptr);
        wasNull = (value == null);
        if (wasNull) {
            return 0;
        }
        return value;
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new PhoenixResultSetMetaData(statement.getConnection(), getRowProjector());
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        checkCursorState();
        ColumnProjector projector = getRowProjector().getColumnProjector(columnIndex - 1);
        Object value = projector.getValue(currentRow, projector.getExpression().getDataType(), ptr);
        wasNull = (value == null);
        if (isApplyTimeZoneDisplacement) {
            PDataType type = projector.getExpression().getDataType();
            if (type == PDate.INSTANCE || type == PUnsignedDate.INSTANCE) {
                value =
                        DateUtil.applyOutputDisplacement((java.sql.Date) value,
                            localCalendar.getTimeZone());
            } else if (type == PTime.INSTANCE || type == PUnsignedTime.INSTANCE) {
                value =
                        DateUtil.applyOutputDisplacement((java.sql.Time) value,
                            localCalendar.getTimeZone());
            } else if (type == PTimestamp.INSTANCE || type == PUnsignedTimestamp.INSTANCE) {
                value =
                        DateUtil.applyOutputDisplacement((java.sql.Timestamp) value,
                            localCalendar.getTimeZone());
            }
        }
        return value;
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return getObject(columnIndex); // Just ignore map since we only support built-in types
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getObject(findColumn(columnLabel), map);
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        // TODO: support?
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        checkCursorState();
        Short value = (Short)getRowProjector().getColumnProjector(columnIndex-1)
                .getValue(currentRow, PSmallint.INSTANCE, ptr);
        wasNull = (value == null);
        if (wasNull) {
            return 0;
        }
        return value;
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public PhoenixStatement getStatement() throws SQLException {
        return statement;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        checkCursorState();
        // Get the value using the expected type instead of trying to coerce to VARCHAR.
        // We can't coerce using our formatter because we don't have enough context in PDataType.
        ColumnProjector projector = getRowProjector().getColumnProjector(columnIndex-1);
        PDataType type = projector.getExpression().getDataType();
        Object value = projector.getValue(currentRow,type, ptr);
        wasNull = (value == null);
        if (wasNull) {
            return null;
        }
        // Run Object through formatter to get String.
        // This provides a simple way of getting a reasonable string representation
        // for types like DATE and TIME
        Format formatter = statement.getFormatter(type);
        return formatter == null ? value.toString() : formatter.format(value);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return getTime(columnIndex, localCalendar);
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        checkCursorState();
        Time value = (Time)getRowProjector().getColumnProjector(columnIndex-1).getValue(currentRow,
            PTime.INSTANCE, ptr);
        wasNull = (value == null);
        if (wasNull) {
            return null;
        }
        if (isApplyTimeZoneDisplacement) {
            return DateUtil.applyOutputDisplacement(value, cal.getTimeZone());
        } else {
            return value;
        }
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(findColumn(columnLabel),cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return getTimestamp(columnIndex, localCalendar);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        checkCursorState();
        Timestamp value = (Timestamp)getRowProjector().getColumnProjector(columnIndex-1)
                .getValue(currentRow, PTimestamp.INSTANCE, ptr);
        wasNull = (value == null);
        if (wasNull) {
            return null;
        }
        if (isApplyTimeZoneDisplacement) {
            return DateUtil.applyOutputDisplacement(value, cal.getTimeZone());
        } else {
            return value;
        }
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(findColumn(columnLabel),cal);
    }

    @Override
    public int getType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        checkCursorState();
        String value = (String)getRowProjector().getColumnProjector(columnIndex-1)
                .getValue(currentRow, PVarchar.INSTANCE, ptr);
        wasNull = (value == null);
        if (wasNull) {
            return null;
        }
        try {
            return new URL(value);
        } catch (MalformedURLException e) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.MALFORMED_URL).setRootCause(e).build().buildException();
        }
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return getURL(findColumn(columnLabel));
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return currentRow == null;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return currentRow == BEFORE_FIRST;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    @Override
    public boolean isFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isLast() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean last() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Tuple getCurrentRow() {
        return currentRow;
    }

    @Override
    public boolean next() throws SQLException {
        checkOpen();
        try {
            if (!firstRecordRead) {
                firstRecordRead = true;
                overAllQueryMetrics.startResultSetWatch();
            }
            currentRow = scanner.next();
            if (currentRow != null) {
                count++;
                // Reset this projector with each row
                if (this.rowProjectorWithDynamicCols != null) {
                    this.rowProjectorWithDynamicCols = null;
                }
                processDynamicColumnsIfRequired();
            }
            rowProjector.reset();
            if (rowProjectorWithDynamicCols != null) {
                rowProjectorWithDynamicCols.reset();
            }
        } catch (RuntimeException | SQLException e) {
            // FIXME: Expression.evaluate does not throw SQLException
            // so this will unwrap throws from that.
            queryLogger.log(QueryLogInfo.QUERY_STATUS_I, QueryStatus.FAILED.toString());
            if (queryLogger.isDebugEnabled()) {
                queryLogger.log(QueryLogInfo.EXCEPTION_TRACE_I, Throwables.getStackTraceAsString(e));
            }
            this.exception = e;
            if (e.getCause() instanceof SQLException) {
                throw (SQLException) e.getCause();
            }
            throw e;
        } finally {
            // If an exception occurs during rs.next(), or if we're on the last row, update metrics
            if (this.exception != null || currentRow == null) {
                overAllQueryMetrics.endQuery();
                overAllQueryMetrics.stopResultSetWatch();
            }

            if (this.exception!=null) {
                queryLogger.log(QueryLogInfo.NO_OF_RESULTS_ITERATED_I, count);
                if (queryLogger.isDebugEnabled()) {
                    queryLogger.log(QueryLogInfo.SCAN_METRICS_JSON_I,
                            readMetricsQueue.getScanMetricsHolderList().toString());
                    readMetricsQueue.getScanMetricsHolderList().clear();
                }
                if (queryLogger != null) {
                    queryLogger.sync(getReadMetrics(), getOverAllRequestReadMetrics());
                }
            }
            if (currentRow == null) {
                overAllQueryMetrics.endQuery();
                overAllQueryMetrics.stopResultSetWatch();
            }
        }
        return currentRow != null;
    }

    private void updateTableLevelReadMetrics(String tableName, boolean isPointLookup) {
        Map<String, Map<MetricType, Long>> readMetrics = getReadMetrics();
        TableMetricsManager.pushMetricsFromConnInstanceMethod(readMetrics);
        Map<String, Map<MetricType, Long>> metricsFromOverallQuery = new HashMap<>();
        Map<MetricType, Long> overAllReadMetrics = getOverAllRequestReadMetrics();
        metricsFromOverallQuery.put(tableName, overAllReadMetrics);
        TableMetricsManager.pushMetricsFromConnInstanceMethod(metricsFromOverallQuery);
        if (readMetrics.get(tableName) != null) {
            Long scanBytes = readMetrics.get(tableName).get(MetricType.SCAN_BYTES);
            if (scanBytes == null) {
                scanBytes = 0L;
            }
            TableMetricsManager.updateHistogramMetricsForQueryScanBytes(
                    scanBytes, tableName, isPointLookup);
            Long timeSpentInRSNext = overAllReadMetrics.get(MetricType.RESULT_SET_TIME_MS);

            if (timeSpentInRSNext == null) {
                timeSpentInRSNext = 0l;
            }
            timeSpentInRSNext += queryTime;
            TableMetricsManager.updateHistogramMetricsForQueryLatency(tableName, timeSpentInRSNext, isPointLookup);

            TableMetricsManager.updateMetricsMethod(tableName, this.exception == null ?
                    MetricType.SELECT_AGGREGATE_SUCCESS_SQL_COUNTER :
                    MetricType.SELECT_AGGREGATE_FAILURE_SQL_COUNTER, 1);
        }
    }

    @Override
    public boolean previous() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean rowInserted() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != ResultSet.FETCH_FORWARD) {
            throw new SQLFeatureNotSupportedException();
        }
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        LOGGER.warn("Ignoring setFetchSize(" + rows + ")");
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean wasNull() throws SQLException {
        return wasNull;
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

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        if (type.equals(String.class)) {
            // Special case, the connection specific formatter is not available in the Type system
            return (T) getString(columnIndex);
        } else if (java.util.Date.class.isAssignableFrom(type)) {
            // The displacement handling code is in the specific getters
            if (java.sql.Timestamp.class.isAssignableFrom(type)) {
                return (T) getTimestamp(columnIndex);
            } else if (java.sql.Date.class.isAssignableFrom(type)) {
                return (T) getDate(columnIndex);
            } else if (java.sql.Time.class.isAssignableFrom(type)) {
                return (T) getTime(columnIndex);
            } else if (java.util.Date.class.equals(type)) {
                return (T) new java.util.Date(getDate(columnIndex).getTime());
            }
        }
        checkCursorState();
        ColumnProjector projector = getRowProjector().getColumnProjector(columnIndex - 1);

        Object value =
                projector.getValue(currentRow, projector.getExpression().getDataType(), ptr, type);

        wasNull = (value == null);
        return (T) value;
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(findColumn(columnLabel), type);
    }

    @VisibleForTesting
    public ResultIterator getUnderlyingIterator() {
        return scanner;
    }

    @Override
    public Map<String, Map<MetricType, Long>> getReadMetrics() {
        return readMetricsQueue.aggregate();
    }

    @Override
    public Map<MetricType, Long> getOverAllRequestReadMetrics() {
        return overAllQueryMetrics.publish();
    }

    @Override
    public void resetMetrics() {
        readMetricsQueue.clearMetrics();
        readMetricsQueue.getScanMetricsHolderList().clear();
        overAllQueryMetrics.reset();
    }
    
    public StatementContext getContext() {
        return context;
    }

    public void setQueryTime(long queryTime) {
        this.queryTime = queryTime;
    }

    /**
     * Return the row projector to use
     * @return the row projector including dynamic column projectors in case we are including
     * dynamic columns, otherwise the regular row projector containing static column projectors
     */
    private RowProjector getRowProjector() {
        if (this.rowProjectorWithDynamicCols != null) {
            return this.rowProjectorWithDynamicCols;
        }
        return this.rowProjector;
    }

    /**
     * Populate the static columns and the starting position for dynamic columns which we use when
     * merging column projectors of static and dynamic columns
     * @return Pair whose first part is the list of static column PColumns and the second part is
     * the starting position for dynamic columns
     */
    private Pair<List<PColumn>, Integer> getStaticColsAndStartingPosForDynCols(){
        List<PColumn> staticCols = new ArrayList<>();
        for (ColumnProjector cp : this.rowProjector.getColumnProjectors()) {
            Expression exp = cp.getExpression();
            if (exp instanceof ProjectedColumnExpression) {
                staticCols.addAll(((ProjectedColumnExpression) exp).getColumns());
                break;
            }
        }
        int startingPosForDynCols = 0;
        for (PColumn col : staticCols) {
            if (!SchemaUtil.isPKColumn(col)) {
                startingPosForDynCols++;
            }
        }
        return new Pair<>(staticCols, startingPosForDynCols);
    }

    /**
     * Process the dynamic column metadata for the current row and store the complete projector for
     * all static and dynamic columns for this row
     */
    private void processDynamicColumnsIfRequired() {
        if (!this.wildcardIncludesDynamicCols || this.currentRow == null ||
                !this.rowProjector.projectDynColsInWildcardQueries()) {
            return;
        }
        List<PColumn> dynCols = getDynColsListAndSeparateFromActualData();
        if (dynCols == null) {
            return;
        }

        RowProjector rowProjectorWithDynamicColumns = null;
        if (this.rowProjector.getColumnCount() > 0 &&
                dynCols.size() > 0) {
            rowProjectorWithDynamicColumns = mergeRowProjectorWithDynColProjectors(dynCols,
                            this.rowProjector.getColumnProjector(0).getTableName());
        }
        // Set the combined row projector
        if (rowProjectorWithDynamicColumns != null) {
            this.rowProjectorWithDynamicCols = rowProjectorWithDynamicColumns;
        }
    }

    /**
     * Separate the actual cell data from the serialized list of dynamic column PColumns and
     * return the deserialized list of dynamic column PColumns for the current row
     * @return Deserialized list of dynamic column PColumns or null if there are no dynamic columns
     */
    private List<PColumn> getDynColsListAndSeparateFromActualData() {
        Cell base = this.currentRow.getValue(0);
        final byte[] valueArray = CellUtil.cloneValue(base);
        // We inserted the known byte array before appending the serialized list of dynamic columns
        final byte[] anchor = Arrays.copyOf(DYN_COLS_METADATA_CELL_QUALIFIER,
                DYN_COLS_METADATA_CELL_QUALIFIER.length);
        // Reverse the arrays to find the last occurrence of the sub-array in the value array
        ArrayUtils.reverse(valueArray);
        ArrayUtils.reverse(anchor);
        final int pos = valueArray.length - Bytes.indexOf(valueArray, anchor);
        // There are no dynamic columns to process so return immediately
        if (pos >= valueArray.length) {
            return null;
        }
        ArrayUtils.reverse(valueArray);

        // Separate the serialized list of dynamic column PColumns from the actual cell data
        byte[] actualCellDataBytes = Arrays.copyOfRange(valueArray, 0,
                pos - DYN_COLS_METADATA_CELL_QUALIFIER.length);
        ImmutableBytesWritable actualCellData = new ImmutableBytesWritable(actualCellDataBytes);
        ImmutableBytesWritable key = new ImmutableBytesWritable();
        currentRow.getKey(key);
        // Store only the actual cell data as part of the current row
        this.currentRow = new TupleProjector.ProjectedValueTuple(key.get(), key.getOffset(),
                key.getLength(), base.getTimestamp(),
                actualCellData.get(), actualCellData.getOffset(), actualCellData.getLength(), 0);

        byte[] dynColsListBytes = Arrays.copyOfRange(valueArray, pos, valueArray.length);
        List<PColumn> dynCols = new ArrayList<>();
        try {
            List<PTableProtos.PColumn> dynColsProtos = DynamicColumnMetaDataProtos
                    .DynamicColumnMetaData.parseFrom(dynColsListBytes).getDynamicColumnsList();
            for (PTableProtos.PColumn colProto : dynColsProtos) {
                dynCols.add(PColumnImpl.createFromProto(colProto));
            }
        } catch (InvalidProtocolBufferException e) {
            return null;
        }
        return dynCols;
    }

    /**
     * Add the dynamic column projectors at the end of the current row's row projector
     * @param dynCols list of dynamic column PColumns for the current row
     * @param tableName table name
     * @return The combined row projector containing column projectors for both static and dynamic
     * columns
     */
    private RowProjector mergeRowProjectorWithDynColProjectors(List<PColumn> dynCols,
            String tableName) {
        List<ColumnProjector> allColumnProjectors =
                new ArrayList<>(this.rowProjector.getColumnProjectors());
        List<PColumn> allCols = new ArrayList<>();
        if (this.staticColumns != null) {
            allCols.addAll(this.staticColumns);
        }
        // Add dynamic columns to the end
        allCols.addAll(dynCols);

        int startingPos = this.startPositionForDynamicCols;
        // Get the ProjectedColumnExpressions for dynamic columns
        for (PColumn currentDynCol : dynCols) {
            // Note that we refer to all the existing static columns along with all dynamic columns
            // in each of the newly added dynamic column projectors.
            // This is required for correctly building the schema for each of the dynamic columns
            Expression exp = new ProjectedColumnExpression(currentDynCol, allCols,
                    startingPos++, currentDynCol.getName().getString());

            ColumnProjector dynColProj = new ExpressionProjector(
                    currentDynCol.getName().getString(), currentDynCol.getName().getString(), tableName, exp, false);
            allColumnProjectors.add(dynColProj);
        }

        return new RowProjector(allColumnProjectors, this.rowProjector.getEstimatedRowByteSize(),
                this.rowProjector.projectEveryRow(), this.rowProjector.hasUDFs(),
                this.rowProjector.projectEverything(),
                this.rowProjector.projectDynColsInWildcardQueries());
    }

}
