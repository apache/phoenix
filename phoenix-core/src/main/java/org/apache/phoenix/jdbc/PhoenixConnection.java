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

import static java.util.Collections.emptyMap;

import java.io.EOFException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Reader;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.text.Format;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.Executor;

import javax.annotation.Nullable;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.phoenix.call.CallRunner;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.expression.function.FunctionArgumentType;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.jdbc.PhoenixStatement.PhoenixStatementParser;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.DelegateConnectionQueryServices;
import org.apache.phoenix.query.MetaDataMutated;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PMetaData;
import org.apache.phoenix.schema.PMetaData.Pruner;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.types.PArrayDataType;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PTime;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PUnsignedDate;
import org.apache.phoenix.schema.types.PUnsignedTime;
import org.apache.phoenix.schema.types.PUnsignedTimestamp;
import org.apache.phoenix.trace.util.Tracing;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.JDBCUtil;
import org.apache.phoenix.util.NumberUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SQLCloseable;
import org.apache.phoenix.util.SQLCloseables;
import org.apache.htrace.Sampler;
import org.apache.htrace.TraceScope;

import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;


/**
 * 
 * JDBC Connection implementation of Phoenix.
 * Currently the following are supported:
 * - Statement
 * - PreparedStatement
 * The connection may only be used with the following options:
 * - ResultSet.TYPE_FORWARD_ONLY
 * - Connection.TRANSACTION_READ_COMMITTED
 * 
 * 
 * @since 0.1
 */
public class PhoenixConnection implements Connection, org.apache.phoenix.jdbc.Jdbc7Shim.Connection, MetaDataMutated  {
    private final String url;
    private final ConnectionQueryServices services;
    private final Properties info;
    private List<SQLCloseable> statements = new ArrayList<SQLCloseable>();
    private final Map<PDataType<?>, Format> formatters = new HashMap<>();
    private final MutationState mutationState;
    private final int mutateBatchSize;
    private final Long scn;
    private boolean isAutoCommit = false;
    private PMetaData metaData;
    private final PName tenantId;
    private final String datePattern;
    private final String timePattern;
    private final String timestampPattern;
    private TraceScope traceScope = null;
    
    private boolean isClosed = false;
    private Sampler<?> sampler;
    private boolean readOnly = false;
    private Map<String, String> customTracingAnnotations = emptyMap(); 
    private Consistency consistency = Consistency.STRONG;

    static {
        Tracing.addTraceMetricsSource();
    }

    private static Properties newPropsWithSCN(long scn, Properties props) {
        props = new Properties(props);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(scn));
        return props;
    }

    public PhoenixConnection(PhoenixConnection connection) throws SQLException {
        this(connection.getQueryServices(), connection.getURL(), connection.getClientInfo(), connection.getMetaDataCache());
        this.isAutoCommit = connection.isAutoCommit;
        this.sampler = connection.sampler;
    }
    
    public PhoenixConnection(PhoenixConnection connection, long scn) throws SQLException {
        this(connection.getQueryServices(), connection, scn);
        this.sampler = connection.sampler;
    }
    
    public PhoenixConnection(ConnectionQueryServices services, PhoenixConnection connection, long scn) throws SQLException {
        this(services, connection.getURL(), newPropsWithSCN(scn,connection.getClientInfo()), connection.getMetaDataCache());
        this.isAutoCommit = connection.isAutoCommit;
        this.sampler = connection.sampler;
    }
    
    public PhoenixConnection(ConnectionQueryServices services, String url, Properties info, PMetaData metaData) throws SQLException {
        this.url = url;
        // Copy so client cannot change
        this.info = info == null ? new Properties() : PropertiesUtil.deepCopy(info);
        final PName tenantId = JDBCUtil.getTenantId(url, info);
        if (this.info.isEmpty() && tenantId == null) {
            this.services = services;
        } else {
            // Create child services keyed by tenantId to track resource usage for
            // a tenantId for all connections on this JVM.
            if (tenantId != null) {
                services = services.getChildQueryServices(tenantId.getBytesPtr());
            }
            // TODO: we could avoid creating another wrapper if the only property
            // specified was for the tenant ID
            Map<String, String> existingProps = services.getProps().asMap();
            final Map<String, String> tmpAugmentedProps = Maps.newHashMapWithExpectedSize(existingProps.size() + info.size());
            tmpAugmentedProps.putAll(existingProps);
            boolean needsDelegate = false;
            for (Entry<Object, Object> entry : this.info.entrySet()) {
                String key = entry.getKey().toString();
                String value = entry.getValue().toString();
                String oldValue = tmpAugmentedProps.put(key, value);
                needsDelegate |= !Objects.equal(oldValue, value);
            }
            this.services = !needsDelegate ? services : new DelegateConnectionQueryServices(services) {
                final ReadOnlyProps augmentedProps = new ReadOnlyProps(tmpAugmentedProps);
    
                @Override
                public ReadOnlyProps getProps() {
                    return augmentedProps;
                }
            };
        }
        this.scn = JDBCUtil.getCurrentSCN(url, this.info);
        this.isAutoCommit = JDBCUtil.getAutoCommit(
                url, this.info,
                this.services.getProps().getBoolean(
                        QueryServices.AUTO_COMMIT_ATTRIB,
                        QueryServicesOptions.DEFAULT_AUTO_COMMIT));
        this.consistency = JDBCUtil.getConsistencyLevel(url, this.info, this.services.getProps()
                 .get(QueryServices.CONSISTENCY_ATTRIB,
                         QueryServicesOptions.DEFAULT_CONSISTENCY_LEVEL));
        this.tenantId = tenantId;
        this.mutateBatchSize = JDBCUtil.getMutateBatchSize(url, this.info, this.services.getProps());
        datePattern = this.services.getProps().get(QueryServices.DATE_FORMAT_ATTRIB, DateUtil.DEFAULT_DATE_FORMAT);
        timePattern = this.services.getProps().get(QueryServices.TIME_FORMAT_ATTRIB, DateUtil.DEFAULT_TIME_FORMAT);
        timestampPattern = this.services.getProps().get(QueryServices.TIMESTAMP_FORMAT_ATTRIB, DateUtil.DEFAULT_TIMESTAMP_FORMAT);
        String numberPattern = this.services.getProps().get(QueryServices.NUMBER_FORMAT_ATTRIB, NumberUtil.DEFAULT_NUMBER_FORMAT);
        int maxSize = this.services.getProps().getInt(QueryServices.MAX_MUTATION_SIZE_ATTRIB,QueryServicesOptions.DEFAULT_MAX_MUTATION_SIZE);
        Format dateFormat = DateUtil.getDateFormatter(datePattern);
        Format timeFormat = DateUtil.getDateFormatter(timePattern);
        Format timestampFormat = DateUtil.getDateFormatter(timestampPattern);
        formatters.put(PDate.INSTANCE, dateFormat);
        formatters.put(PTime.INSTANCE, timeFormat);
        formatters.put(PTimestamp.INSTANCE, timestampFormat);
        formatters.put(PUnsignedDate.INSTANCE, dateFormat);
        formatters.put(PUnsignedTime.INSTANCE, timeFormat);
        formatters.put(PUnsignedTimestamp.INSTANCE, timestampFormat);
        formatters.put(PDecimal.INSTANCE, FunctionArgumentType.NUMERIC.getFormatter(numberPattern));
        // We do not limit the metaData on a connection less than the global one,
        // as there's not much that will be cached here.
        this.metaData = metaData.pruneTables(new Pruner() {

            @Override
            public boolean prune(PTable table) {
                long maxTimestamp = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
                return (table.getType() != PTableType.SYSTEM && 
                        (  table.getTimeStamp() >= maxTimestamp || 
                         ! Objects.equal(tenantId, table.getTenantId())) );
            }
            
        });
        this.mutationState = new MutationState(maxSize, this);
        this.services.addConnection(this);

        // setup tracing, if its enabled
        this.sampler = Tracing.getConfiguredSampler(this);
        this.customTracingAnnotations = getImmutableCustomTracingAnnotations();
    }
    
    private ImmutableMap<String, String> getImmutableCustomTracingAnnotations() {
    	Builder<String, String> result = ImmutableMap.builder();
    	result.putAll(JDBCUtil.getAnnotations(url, info));
    	if (getSCN() != null) {
    		result.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, getSCN().toString());
    	}
    	if (getTenantId() != null) {
    		result.put(PhoenixRuntime.TENANT_ID_ATTRIB, getTenantId().getString());
    	}
    	return result.build();
    }

    public Sampler<?> getSampler() {
        return this.sampler;
    }
    
    public void setSampler(Sampler<?> sampler) throws SQLException {
        this.sampler = sampler;
    }

    public Map<String, String> getCustomTracingAnnotations() {
        return customTracingAnnotations;
    }

    public int executeStatements(Reader reader, List<Object> binds, PrintStream out) throws IOException, SQLException {
        int bindsOffset = 0;
        int nStatements = 0;
        PhoenixStatementParser parser = new PhoenixStatementParser(reader);
        try {
            while (true) {
                PhoenixPreparedStatement stmt = null;
                try {
                    stmt = new PhoenixPreparedStatement(this, parser);
                    ParameterMetaData paramMetaData = stmt.getParameterMetaData();
                    for (int i = 0; i < paramMetaData.getParameterCount(); i++) {
                        stmt.setObject(i+1, binds.get(bindsOffset+i));
                    }
                    long start = System.currentTimeMillis();
                    boolean isQuery = stmt.execute();
                    if (isQuery) {
                        ResultSet rs = stmt.getResultSet();
                        if (!rs.next()) {
                            if (out != null) {
                                out.println("no rows selected");
                            }
                        } else {
                            int columnCount = 0;
                            if (out != null) {
                                ResultSetMetaData md = rs.getMetaData();
                                columnCount = md.getColumnCount();
                                for (int i = 1; i <= columnCount; i++) {
                                    int displayWidth = md.getColumnDisplaySize(i);
                                    String label = md.getColumnLabel(i);
                                    if (md.isSigned(i)) {
                                        out.print(displayWidth < label.length() ? label.substring(0,displayWidth) : Strings.padStart(label, displayWidth, ' '));
                                        out.print(' ');
                                    } else {
                                        out.print(displayWidth < label.length() ? label.substring(0,displayWidth) : Strings.padEnd(md.getColumnLabel(i), displayWidth, ' '));
                                        out.print(' ');
                                    }
                                }
                                out.println();
                                for (int i = 1; i <= columnCount; i++) {
                                    int displayWidth = md.getColumnDisplaySize(i);
                                    out.print(Strings.padStart("", displayWidth,'-'));
                                    out.print(' ');
                                }
                                out.println();
                            }
                            do {
                                if (out != null) {
                                    ResultSetMetaData md = rs.getMetaData();
                                    for (int i = 1; i <= columnCount; i++) {
                                        int displayWidth = md.getColumnDisplaySize(i);
                                        String value = rs.getString(i);
                                        String valueString = value == null ? QueryConstants.NULL_DISPLAY_TEXT : value;
                                        if (md.isSigned(i)) {
                                            out.print(Strings.padStart(valueString, displayWidth, ' '));
                                        } else {
                                            out.print(Strings.padEnd(valueString, displayWidth, ' '));
                                        }
                                        out.print(' ');
                                    }
                                    out.println();
                                }
                            } while (rs.next());
                        }
                    } else if (out != null){
                        int updateCount = stmt.getUpdateCount();
                        if (updateCount >= 0) {
                            out.println((updateCount == 0 ? "no" : updateCount) + (updateCount == 1 ? " row " : " rows ") + stmt.getUpdateOperation().toString());
                        }
                    }
                    bindsOffset += paramMetaData.getParameterCount();
                    double elapsedDuration = ((System.currentTimeMillis() - start) / 1000.0);
                    out.println("Time: " + elapsedDuration + " sec(s)\n");
                    nStatements++;
                } finally {
                    if (stmt != null) {
                        stmt.close();
                    }
                }
            }
        } catch (EOFException e) {
        }
        return nStatements;
    }

    public @Nullable PName getTenantId() {
        return tenantId;
    }
    
    public Long getSCN() {
        return scn;
    }
    
    public int getMutateBatchSize() {
        return mutateBatchSize;
    }
    
    public PMetaData getMetaDataCache() {
        return metaData;
    }

    public MutationState getMutationState() {
        return mutationState;
    }
    
    public String getDatePattern() {
        return datePattern;
    }
    
    public Format getFormatter(PDataType type) {
        return formatters.get(type);
    }
    
    public String getURL() {
        return url;
    }
    
    public ConnectionQueryServices getQueryServices() {
        return services;
    }
    
    @Override
    public void clearWarnings() throws SQLException {
    }

    private void closeStatements() throws SQLException {
        List<SQLCloseable> statements = this.statements;
        // create new list to prevent close of statements
        // from modifying this list.
        this.statements = Lists.newArrayList();
        try {
            mutationState.rollback(this);
        } finally {
            try {
                SQLCloseables.closeAll(statements);
            } finally {
                statements.clear();
            }
        }
    }
    
    @Override
    public void close() throws SQLException {
        if (isClosed) {
            return;
        }
        try {
            try {
                if (traceScope != null) {
                    traceScope.close();
                }
                closeStatements();
            } finally {
                services.removeConnection(this);
            }
        } finally {
            isClosed = true;
        }
    }

    @Override
    public void commit() throws SQLException {
        CallRunner.run(new CallRunner.CallableThrowable<Void, SQLException>() {
            @Override
            public Void call() throws SQLException {
                mutationState.commit();
                return null;
            }
        }, Tracing.withTracing(this, "committing mutations"));
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    	PDataType arrayPrimitiveType = PDataType.fromSqlTypeName(typeName);
    	return PArrayDataType.instantiatePhoenixArray(arrayPrimitiveType, elements);
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob createClob() throws SQLException {
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
    public Statement createStatement() throws SQLException {
        PhoenixStatement statement = new PhoenixStatement(this);
        statements.add(statement);
        return statement;
    }

    /**
     * Back-door way to inject processing into walking through a result set
     * @param statementFactory
     * @return PhoenixStatement
     * @throws SQLException
     */
    public PhoenixStatement createStatement(PhoenixStatementFactory statementFactory) throws SQLException {
        PhoenixStatement statement = statementFactory.newStatement(this);
        statements.add(statement);
        return statement;
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw new SQLFeatureNotSupportedException();
        }
        return createStatement();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        if (resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
            throw new SQLFeatureNotSupportedException();
        }
        return createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return isAutoCommit;
    }

    public Consistency getConsistency() {
        return this.consistency;
    }

    @Override
    public String getCatalog() throws SQLException {
        return "";
    }

    @Override
    public Properties getClientInfo() throws SQLException { 
        // Defensive copy so client cannot change
        return new Properties(info);
    }

    @Override
    public String getClientInfo(String name) {
        return info.getProperty(name);
    }

    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return new PhoenixDatabaseMetaData(this);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_READ_COMMITTED;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return Collections.emptyMap();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return readOnly; 
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        // TODO: run query here or ping
        return !isClosed;
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        PhoenixPreparedStatement statement = new PhoenixPreparedStatement(this, sql);
        statements.add(statement);
        return statement;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw new SQLFeatureNotSupportedException();
        }
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        if (resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
            throw new SQLFeatureNotSupportedException();
        }
        return prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void rollback() throws SQLException {
        mutationState.rollback(this);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAutoCommit(boolean isAutoCommit) throws SQLException {
        this.isAutoCommit = isAutoCommit;
    }

    public void setConsistency(Consistency val) {
        this.consistency = val;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        this.readOnly=readOnly;
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
    public void setTransactionIsolation(int level) throws SQLException {
        if (level != Connection.TRANSACTION_READ_COMMITTED) {
            throw new SQLFeatureNotSupportedException();
        }
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
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

    @Override
    public void setSchema(String schema) throws SQLException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public String getSchema() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public PMetaData addTable(PTable table) throws SQLException {
        // TODO: since a connection is only used by one thread at a time,
        // we could modify this metadata in place since it's not shared.
        if (scn == null || scn > table.getTimeStamp()) {
            metaData = metaData.addTable(table);
        }
        //Cascade through to connectionQueryServices too
        getQueryServices().addTable(table);
        return metaData;
    }

    @Override
    public PMetaData addColumn(PName tenantId, String tableName, List<PColumn> columns, long tableTimeStamp, long tableSeqNum, boolean isImmutableRows, boolean isWalDisabled, boolean isMultitenant, boolean storeNulls)
            throws SQLException {
        metaData = metaData.addColumn(tenantId, tableName, columns, tableTimeStamp, tableSeqNum, isImmutableRows, isWalDisabled, isMultitenant, storeNulls);
        //Cascade through to connectionQueryServices too
        getQueryServices().addColumn(tenantId, tableName, columns, tableTimeStamp, tableSeqNum, isImmutableRows, isWalDisabled, isMultitenant, storeNulls);
        return metaData;
    }

    @Override
    public PMetaData removeTable(PName tenantId, String tableName, String parentTableName, long tableTimeStamp) throws SQLException {
        metaData = metaData.removeTable(tenantId, tableName, parentTableName, tableTimeStamp);
        //Cascade through to connectionQueryServices too
        getQueryServices().removeTable(tenantId, tableName, parentTableName, tableTimeStamp);
        return metaData;
    }

    @Override
    public PMetaData removeColumn(PName tenantId, String tableName, List<PColumn> columnsToRemove, long tableTimeStamp,
            long tableSeqNum) throws SQLException {
        metaData = metaData.removeColumn(tenantId, tableName, columnsToRemove, tableTimeStamp, tableSeqNum);
        //Cascade through to connectionQueryServices too
        getQueryServices().removeColumn(tenantId, tableName, columnsToRemove, tableTimeStamp, tableSeqNum);
        return metaData;
    }

    protected boolean removeStatement(PhoenixStatement statement) throws SQLException {
        return statements.remove(statement);
   }

    public KeyValueBuilder getKeyValueBuilder() {
        return this.services.getKeyValueBuilder();
    }

    public TraceScope getTraceScope() {
        return traceScope;
    }

    public void setTraceScope(TraceScope traceScope) {
        this.traceScope = traceScope;
    }
}
