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

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.emptyMap;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_OPEN_PHOENIX_CONNECTIONS;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_PHOENIX_CONNECTIONS_ATTEMPTED_COUNTER;

import java.io.EOFException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Reader;
import java.lang.ref.WeakReference;
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
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.htrace.Sampler;
import org.apache.htrace.TraceScope;
import org.apache.phoenix.call.CallRunner;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.CommitException;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.expression.function.FunctionArgumentType;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.iterate.DefaultTableResultIteratorFactory;
import org.apache.phoenix.iterate.ParallelIteratorFactory;
import org.apache.phoenix.iterate.TableResultIterator;
import org.apache.phoenix.iterate.TableResultIteratorFactory;
import org.apache.phoenix.jdbc.PhoenixStatement.PhoenixStatementParser;
import org.apache.phoenix.parse.PFunction;
import org.apache.phoenix.parse.PSchema;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.ConnectionQueryServices.Feature;
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
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.PTableRef;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.SchemaNotFoundException;
import org.apache.phoenix.schema.TableNotFoundException;
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
import org.apache.phoenix.util.SchemaUtil;
import org.apache.tephra.TransactionContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Lists;


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
public class PhoenixConnection implements Connection, MetaDataMutated, SQLCloseable {
    private final String url;
    private String schema;
    private final ConnectionQueryServices services;
    private final Properties info;
    private final Map<PDataType<?>, Format> formatters = new HashMap<>();
    private final int mutateBatchSize;
    private final long mutateBatchSizeBytes;
    private final Long scn;
    private MutationState mutationState;
    private List<SQLCloseable> statements = new ArrayList<SQLCloseable>();
    private boolean isAutoFlush = false;
    private boolean isAutoCommit = false;
    private PMetaData metaData;
    private final PName tenantId;
    private final String datePattern; 
    private final String timePattern;
    private final String timestampPattern;
    private int statementExecutionCounter;
    private TraceScope traceScope = null;
    private volatile boolean isClosed = false;
    private Sampler<?> sampler;
    private boolean readOnly = false;
    private Consistency consistency = Consistency.STRONG;
    private Map<String, String> customTracingAnnotations = emptyMap();
    private final boolean isRequestLevelMetricsEnabled;
    private final boolean isDescVarLengthRowKeyUpgrade;
    private ParallelIteratorFactory parallelIteratorFactory;
    private final LinkedBlockingQueue<WeakReference<TableResultIterator>> scannerQueue;
    private TableResultIteratorFactory tableResultIteratorFactory;
    private boolean isRunningUpgrade;
    
    static {
        Tracing.addTraceMetricsSource();
    }

    private static Properties newPropsWithSCN(long scn, Properties props) {
        props = new Properties(props);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(scn));
        return props;
    }

    public PhoenixConnection(PhoenixConnection connection, boolean isDescRowKeyOrderUpgrade, boolean isRunningUpgrade) throws SQLException {
        this(connection.getQueryServices(), connection.getURL(), connection.getClientInfo(), connection.metaData, connection.getMutationState(), isDescRowKeyOrderUpgrade, isRunningUpgrade);
        this.isAutoCommit = connection.isAutoCommit;
        this.isAutoFlush = connection.isAutoFlush;
        this.sampler = connection.sampler;
        this.statementExecutionCounter = connection.statementExecutionCounter;
    }

    public PhoenixConnection(PhoenixConnection connection) throws SQLException {
        this(connection, connection.isDescVarLengthRowKeyUpgrade(), connection.isRunningUpgrade());
    }
    
    public PhoenixConnection(PhoenixConnection connection, MutationState mutationState) throws SQLException {
        this(connection.getQueryServices(), connection.getURL(), connection.getClientInfo(), connection.getMetaDataCache(), mutationState, connection.isDescVarLengthRowKeyUpgrade(), connection.isRunningUpgrade());
    }
    
    public PhoenixConnection(PhoenixConnection connection, long scn) throws SQLException {
        this(connection.getQueryServices(), connection, scn);
    }
    
    public PhoenixConnection(ConnectionQueryServices services, PhoenixConnection connection, long scn) throws SQLException {
        this(services, connection.getURL(), newPropsWithSCN(scn,connection.getClientInfo()), connection.metaData, connection.getMutationState(), connection.isDescVarLengthRowKeyUpgrade(), connection.isRunningUpgrade());
        this.isAutoCommit = connection.isAutoCommit;
        this.isAutoFlush = connection.isAutoFlush;
        this.sampler = connection.sampler;
        this.statementExecutionCounter = connection.statementExecutionCounter;
    }
    
    public PhoenixConnection(ConnectionQueryServices services, String url, Properties info, PMetaData metaData) throws SQLException {
        this(services, url, info, metaData, null, false, false);
    }
    
    public PhoenixConnection(PhoenixConnection connection, ConnectionQueryServices services, Properties info) throws SQLException {
        this(services, connection.url, info, connection.metaData, null, connection.isDescVarLengthRowKeyUpgrade(), connection.isRunningUpgrade());
    }
    
    public PhoenixConnection(ConnectionQueryServices services, String url, Properties info, PMetaData metaData, MutationState mutationState, boolean isDescVarLengthRowKeyUpgrade, boolean isRunningUpgrade) throws SQLException {
        GLOBAL_PHOENIX_CONNECTIONS_ATTEMPTED_COUNTER.increment();
        this.url = url;
        this.isDescVarLengthRowKeyUpgrade = isDescVarLengthRowKeyUpgrade;
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
            ReadOnlyProps currentProps = services.getProps();
            final ReadOnlyProps augmentedProps = currentProps.addAll(filterKnownNonProperties(this.info));
            this.services = augmentedProps == currentProps ? services : new DelegateConnectionQueryServices(services) {
                @Override
                public ReadOnlyProps getProps() {
                    return augmentedProps;
                }
            };
        }
        
        Long scnParam = JDBCUtil.getCurrentSCN(url, this.info);
        checkScn(scnParam);
        this.scn = scnParam;
        this.isAutoFlush = this.services.getProps().getBoolean(QueryServices.TRANSACTIONS_ENABLED, QueryServicesOptions.DEFAULT_TRANSACTIONS_ENABLED)
                && this.services.getProps().getBoolean(QueryServices.AUTO_FLUSH_ATTRIB, QueryServicesOptions.DEFAULT_AUTO_FLUSH) ;
        this.isAutoCommit = JDBCUtil.getAutoCommit(
                url, this.info,
                this.services.getProps().getBoolean(
                        QueryServices.AUTO_COMMIT_ATTRIB,
                        QueryServicesOptions.DEFAULT_AUTO_COMMIT));
        this.consistency = JDBCUtil.getConsistencyLevel(url, this.info, this.services.getProps()
                 .get(QueryServices.CONSISTENCY_ATTRIB,
                         QueryServicesOptions.DEFAULT_CONSISTENCY_LEVEL));
        // currently we are not resolving schema set through property, so if schema doesn't exists ,connection will not fail
        // but queries may fail
        this.schema = JDBCUtil.getSchema(url, this.info,
                this.services.getProps().get(QueryServices.SCHEMA_ATTRIB, QueryServicesOptions.DEFAULT_SCHEMA));
        this.tenantId = tenantId;
        this.mutateBatchSize = JDBCUtil.getMutateBatchSize(url, this.info, this.services.getProps());
        this.mutateBatchSizeBytes = JDBCUtil.getMutateBatchSizeBytes(url, this.info, this.services.getProps());
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
        Pruner pruner = new Pruner() {

            @Override
            public boolean prune(PTable table) {
                long maxTimestamp = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
                return (table.getType() != PTableType.SYSTEM && 
                        (  table.getTimeStamp() >= maxTimestamp || 
                         (table.getTenantId()!=null && ! Objects.equal(tenantId, table.getTenantId()))));
            }
            
            @Override
            public boolean prune(PFunction function) {
                long maxTimestamp = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
                return ( function.getTimeStamp() >= maxTimestamp ||
                        (function.getTenantId()!=null && ! Objects.equal(tenantId, function.getTenantId())));
            }
        };
        this.isRequestLevelMetricsEnabled = JDBCUtil.isCollectingRequestLevelMetricsEnabled(url, info, this.services.getProps());
        this.mutationState = mutationState == null ? newMutationState(maxSize) : new MutationState(mutationState);
        this.metaData = metaData;
        this.metaData.pruneTables(pruner);
        this.metaData.pruneFunctions(pruner);
        this.services.addConnection(this);

        // setup tracing, if its enabled
        this.sampler = Tracing.getConfiguredSampler(this);
        this.customTracingAnnotations = getImmutableCustomTracingAnnotations();
        this.scannerQueue = new LinkedBlockingQueue<>();
        this.tableResultIteratorFactory = new DefaultTableResultIteratorFactory();
        this.isRunningUpgrade = isRunningUpgrade;
        GLOBAL_OPEN_PHOENIX_CONNECTIONS.increment();
    }
    
    private static void checkScn(Long scnParam) throws SQLException {
        if (scnParam != null && scnParam < 0) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_SCN).build().buildException();
        }
    }

    private static Properties filterKnownNonProperties(Properties info) {
        Properties prunedProperties = info;
        for (String property : PhoenixRuntime.CONNECTION_PROPERTIES) {
            if (info.containsKey(property)) {
                if (prunedProperties == info) {
                    prunedProperties = PropertiesUtil.deepCopy(info);
                }
                prunedProperties.remove(property);
            }
        }
        return prunedProperties;
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

    public long getMutateBatchSizeBytes(){
        return mutateBatchSizeBytes;
    }

    public PMetaData getMetaDataCache() {
        return metaData;
    }
    
    public PTable getTable(PTableKey key) throws TableNotFoundException {
    	return metaData.getTableRef(key).getTable();
    }
    
    public PTableRef getTableRef(PTableKey key) throws TableNotFoundException {
    	return metaData.getTableRef(key);
    }

    protected MutationState newMutationState(int maxSize) {
        return new MutationState(maxSize, this);
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
            mutationState.rollback();
        } catch (SQLException e) {
            // ignore any exceptions while rolling back
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
            clearMetrics();
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
            GLOBAL_OPEN_PHOENIX_CONNECTIONS.decrement();
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
        statementExecutionCounter = 0;
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

    public boolean getAutoFlush() {
        return isAutoFlush;
    }

    public void setAutoFlush(boolean autoFlush) throws SQLException {
        if (autoFlush && !this.services.getProps().getBoolean(QueryServices.TRANSACTIONS_ENABLED, QueryServicesOptions.DEFAULT_TRANSACTIONS_ENABLED)) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.TX_MUST_BE_ENABLED_TO_SET_AUTO_FLUSH)
            .build().buildException();
        }
        this.isAutoFlush = autoFlush;
    }

    public void flush() throws SQLException {
        mutationState.sendUncommitted();
    }
        
    public void setTransactionContext(TransactionContext txContext) throws SQLException {
        if (!this.services.getProps().getBoolean(QueryServices.TRANSACTIONS_ENABLED, QueryServicesOptions.DEFAULT_TRANSACTIONS_ENABLED)) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.TX_MUST_BE_ENABLED_TO_SET_TX_CONTEXT)
            .build().buildException();
        }
        this.mutationState.rollback();
        this.mutationState = new MutationState(this.mutationState.getMaxSize(), this, txContext);
        
        // Write data to HBase after each statement execution as the commit may not
        // come through Phoenix APIs.
        setAutoFlush(true);
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
        boolean transactionsEnabled = getQueryServices().getProps().getBoolean(QueryServices.TRANSACTIONS_ENABLED, 
                QueryServicesOptions.DEFAULT_TRANSACTIONS_ENABLED);
        return  transactionsEnabled ?
                Connection.TRANSACTION_REPEATABLE_READ : Connection.TRANSACTION_READ_COMMITTED;
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
        // Ignore autoGeneratedKeys, and just execute the statement.
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        // Ignore columnIndexes, and just execute the statement.
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        // Ignore columnNames, and just execute the statement.
        return prepareStatement(sql);
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
        CallRunner.run(new CallRunner.CallableThrowable<Void, SQLException>() {
            @Override
            public Void call() throws SQLException {
                mutationState.rollback();
                return null;
            }
        }, Tracing.withTracing(this, "rolling back"));
        statementExecutionCounter = 0;
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
        if (!this.getCatalog().equalsIgnoreCase(catalog)) {
            // allow noop calls to pass through.
            throw new SQLFeatureNotSupportedException();
        }
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
        boolean transactionsEnabled = getQueryServices().getProps().getBoolean(QueryServices.TRANSACTIONS_ENABLED, 
                QueryServicesOptions.DEFAULT_TRANSACTIONS_ENABLED);
        if (level == Connection.TRANSACTION_SERIALIZABLE) {
            throw new SQLFeatureNotSupportedException();
        }
        if (!transactionsEnabled && level == Connection.TRANSACTION_REPEATABLE_READ) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.TX_MUST_BE_ENABLED_TO_SET_ISOLATION_LEVEL)
            .build().buildException();
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
        this.schema = schema;

    }

    @Override
    public String getSchema() throws SQLException {
        return SchemaUtil.normalizeIdentifier(this.schema);
    }

    public PSchema getSchema(PTableKey key) throws SchemaNotFoundException {
        return metaData.getSchema(key);
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
    public void addTable(PTable table, long resolvedTime) throws SQLException {
        metaData.addTable(table, resolvedTime);
        //Cascade through to connectionQueryServices too
        getQueryServices().addTable(table, resolvedTime);
    }
    
    @Override
    public void updateResolvedTimestamp(PTable table, long resolvedTime) throws SQLException {
    	metaData.updateResolvedTimestamp(table, resolvedTime);
    	//Cascade through to connectionQueryServices too
        getQueryServices().updateResolvedTimestamp(table, resolvedTime);
    }
    
    @Override
    public void addFunction(PFunction function) throws SQLException {
        // TODO: since a connection is only used by one thread at a time,
        // we could modify this metadata in place since it's not shared.
        if (scn == null || scn > function.getTimeStamp()) {
            metaData.addFunction(function);
        }
        //Cascade through to connectionQueryServices too
        getQueryServices().addFunction(function);
    }

    @Override
    public void addSchema(PSchema schema) throws SQLException {
        metaData.addSchema(schema);
        // Cascade through to connectionQueryServices too
        getQueryServices().addSchema(schema);
    }

    @Override
    public void removeTable(PName tenantId, String tableName, String parentTableName, long tableTimeStamp) throws SQLException {
        metaData.removeTable(tenantId, tableName, parentTableName, tableTimeStamp);
        //Cascade through to connectionQueryServices too
        getQueryServices().removeTable(tenantId, tableName, parentTableName, tableTimeStamp);
    }

    @Override
    public void removeFunction(PName tenantId, String functionName, long tableTimeStamp) throws SQLException {
        metaData.removeFunction(tenantId, functionName, tableTimeStamp);
        //Cascade through to connectionQueryServices too
        getQueryServices().removeFunction(tenantId, functionName, tableTimeStamp);
    }

    @Override
    public void removeColumn(PName tenantId, String tableName, List<PColumn> columnsToRemove, long tableTimeStamp,
            long tableSeqNum, long resolvedTime) throws SQLException {
        metaData.removeColumn(tenantId, tableName, columnsToRemove, tableTimeStamp, tableSeqNum, resolvedTime);
        //Cascade through to connectionQueryServices too
        getQueryServices().removeColumn(tenantId, tableName, columnsToRemove, tableTimeStamp, tableSeqNum, resolvedTime);
    }

    protected boolean removeStatement(PhoenixStatement statement) throws SQLException {
        return statements.remove(statement);
   }

    public KeyValueBuilder getKeyValueBuilder() {
        return this.services.getKeyValueBuilder();
    }
    
    /**
     * Used to track executions of {@link Statement}s and {@link PreparedStatement}s that were created from this connection before
     * commit or rollback. 0-based. Used to associate partial save errors with SQL statements
     * invoked by users.
     * @see CommitException
     * @see #incrementStatementExecutionCounter()
     */
    public int getStatementExecutionCounter() {
		return statementExecutionCounter;
	}
    
    public void incrementStatementExecutionCounter() {
        statementExecutionCounter++;
    }

    public TraceScope getTraceScope() {
        return traceScope;
    }

    public void setTraceScope(TraceScope traceScope) {
        this.traceScope = traceScope;
    }
    
    public Map<String, Map<String, Long>> getMutationMetrics() {
        return mutationState.getMutationMetricQueue().aggregate();
    }
    
    public Map<String, Map<String, Long>> getReadMetrics() {
        return mutationState.getReadMetricQueue() != null ? mutationState.getReadMetricQueue().aggregate() : Collections.<String, Map<String, Long>>emptyMap();
    }
    
    public boolean isRequestLevelMetricsEnabled() {
        return isRequestLevelMetricsEnabled;
    }
    
    public void clearMetrics() {
        mutationState.getMutationMetricQueue().clearMetrics();
        if (mutationState.getReadMetricQueue() != null) {
            mutationState.getReadMetricQueue().clearMetrics();
        }
    }

    /**
     * Returns true if this connection is being used to upgrade the
     * data due to PHOENIX-2067 and false otherwise.
     * @return
     */
    public boolean isDescVarLengthRowKeyUpgrade() {
        return isDescVarLengthRowKeyUpgrade;
    }
    
    /**
     * Added for tests only. Do not use this elsewhere.
     */
    public ParallelIteratorFactory getIteratorFactory() {
        return parallelIteratorFactory;
    }
    
    /**
     * Added for testing purposes. Do not use this elsewhere.
     */
    public void setIteratorFactory(ParallelIteratorFactory parallelIteratorFactory) {
        this.parallelIteratorFactory = parallelIteratorFactory;
    }
    
    public void addIteratorForLeaseRenewal(@Nonnull TableResultIterator itr) {
        if (services.supportsFeature(Feature.RENEW_LEASE)) {
            checkNotNull(itr);
            scannerQueue.add(new WeakReference<TableResultIterator>(itr));
        }
    }

    public LinkedBlockingQueue<WeakReference<TableResultIterator>> getScanners() {
        return scannerQueue;
    }

    @VisibleForTesting
    @Nonnull
    public TableResultIteratorFactory getTableResultIteratorFactory() {
        return tableResultIteratorFactory;
    }

    @VisibleForTesting
    public void setTableResultIteratorFactory(TableResultIteratorFactory factory) {
        checkNotNull(factory);
        this.tableResultIteratorFactory = factory;
    }

    @Override
    public void removeSchema(PSchema schema, long schemaTimeStamp) {
        metaData.removeSchema(schema, schemaTimeStamp);
        // Cascade through to connectionQueryServices too
        getQueryServices().removeSchema(schema, schemaTimeStamp);

    }
    
    public boolean isRunningUpgrade() {
        return isRunningUpgrade;
    }
    
    public void setRunningUpgrade(boolean isRunningUpgrade) {
        this.isRunningUpgrade = isRunningUpgrade;
    }
    
}
