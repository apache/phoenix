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

import static org.apache.phoenix.monitoring.MetricType.OPEN_INTERNAL_PHOENIX_CONNECTIONS_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.OPEN_PHOENIX_CONNECTIONS_COUNTER;
import static org.apache.phoenix.query.QueryServices.QUERY_SERVICES_NAME;
import static org.apache.phoenix.thirdparty.com.google.common.base.Preconditions.checkArgument;
import static org.apache.phoenix.thirdparty.com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.emptyMap;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_OPEN_INTERNAL_PHOENIX_CONNECTIONS;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_OPEN_PHOENIX_CONNECTIONS;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.htrace.Sampler;
import org.apache.htrace.TraceScope;
import org.apache.phoenix.call.CallRunner;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol;
import org.apache.phoenix.exception.FailoverSQLException;
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
import org.apache.phoenix.log.ActivityLogInfo;
import org.apache.phoenix.log.ConnectionActivityLogger;
import org.apache.phoenix.log.LogLevel;
import org.apache.phoenix.monitoring.MetricType;
import org.apache.phoenix.monitoring.TableMetricsManager;
import org.apache.phoenix.monitoring.connectionqueryservice.ConnectionQueryServicesMetricsManager;
import org.apache.phoenix.parse.PFunction;
import org.apache.phoenix.parse.PSchema;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.DelegateConnectionQueryServices;
import org.apache.phoenix.query.MetaDataMutated;
import org.apache.phoenix.query.PropertyPolicyProvider;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.ConnectionProperty;
import org.apache.phoenix.schema.FunctionNotFoundException;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PMetaData;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.PTableRef;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.SchemaNotFoundException;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.types.PArrayDataType;
import org.apache.phoenix.schema.types.PBinary;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PTime;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PUnsignedDate;
import org.apache.phoenix.schema.types.PUnsignedTime;
import org.apache.phoenix.schema.types.PUnsignedTimestamp;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.base.Objects;
import org.apache.phoenix.thirdparty.com.google.common.base.Strings;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableMap.Builder;
import org.apache.phoenix.trace.util.Tracing;
import org.apache.phoenix.transaction.PhoenixTransactionContext;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.JDBCUtil;
import org.apache.phoenix.util.NumberUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SQLCloseable;
import org.apache.phoenix.util.SQLCloseables;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.VarBinaryFormatter;

/**
 * 
 * JDBC Connection implementation of Phoenix. Currently the following are
 * supported: - Statement - PreparedStatement The connection may only be used
 * with the following options: - ResultSet.TYPE_FORWARD_ONLY -
 * Connection.TRANSACTION_READ_COMMITTED
 * 
 * 
 * @since 0.1
 */
public class PhoenixConnection implements MetaDataMutated, SQLCloseable, PhoenixMonitoredConnection {
    private final String url;
    private String schema;
    private final ConnectionQueryServices services;
    private final Properties info;
    private final Map<PDataType<?>, Format> formatters = new HashMap<>();
    private final int mutateBatchSize;
    private final long mutateBatchSizeBytes;
    private final Long scn;
    private final boolean buildingIndex;
    private MutationState mutationState;
    private HashSet<PhoenixStatement> statements = new HashSet<>();
    private boolean isAutoFlush = false;
    private boolean isAutoCommit = false;
    private final PName tenantId;
    private final String dateFormatTimeZoneId;
    private final String datePattern;
    private final String timePattern;
    private final String timestampPattern;
    private int statementExecutionCounter;
    private TraceScope traceScope = null;
    private volatile boolean isClosed = false;
    private volatile boolean isClosing = false;
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
    private LogLevel logLevel;
    private LogLevel auditLogLevel;
    private Double logSamplingRate;
    private String sourceOfOperation;
    private volatile SQLException reasonForClose;
    private static final String[] CONNECTION_PROPERTIES;

    private final ConcurrentLinkedQueue<PhoenixConnection> childConnections =
        new ConcurrentLinkedQueue<>();

    //For now just the copy constructor paths will have this as true as I don't want to change the
    //public interfaces.
    private final boolean isInternalConnection;
    private boolean isApplyTimeZoneDisplacement;
    private final UUID uniqueID;
    private ConnectionActivityLogger connectionActivityLogger = ConnectionActivityLogger.NO_OP_LOGGER;

    static {
        Tracing.addTraceMetricsSource();
        CONNECTION_PROPERTIES = PhoenixRuntime.getConnectionProperties();
    }

    private static Properties newPropsWithSCN(long scn, Properties props) {
        props = new Properties(props);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(scn));
        return props;
    }

    public PhoenixConnection(PhoenixConnection connection,
            boolean isDescRowKeyOrderUpgrade, boolean isRunningUpgrade)
                    throws SQLException {
        this(connection.getQueryServices(), connection.getURL(), connection
                .getClientInfo(), connection
                .getMutationState(), isDescRowKeyOrderUpgrade,
                isRunningUpgrade, connection.buildingIndex, true);
        this.isAutoCommit = connection.isAutoCommit;
        this.isAutoFlush = connection.isAutoFlush;
        this.sampler = connection.sampler;
        this.statementExecutionCounter = connection.statementExecutionCounter;
    }

    public PhoenixConnection(PhoenixConnection connection) throws SQLException {
        this(connection, connection.isDescVarLengthRowKeyUpgrade(), connection
                .isRunningUpgrade());
    }

    public PhoenixConnection(PhoenixConnection connection,
            MutationState mutationState) throws SQLException {
        this(connection.getQueryServices(), connection.getURL(), connection
                .getClientInfo(), mutationState,
                connection.isDescVarLengthRowKeyUpgrade(), connection
                .isRunningUpgrade(), connection.buildingIndex, true);
    }

    public PhoenixConnection(PhoenixConnection connection, long scn)
            throws SQLException {
        this(connection, newPropsWithSCN(scn, connection.getClientInfo()));
    }

	public PhoenixConnection(PhoenixConnection connection, Properties props) throws SQLException {
        this(connection.getQueryServices(), connection.getURL(), props, connection
                .getMutationState(), connection.isDescVarLengthRowKeyUpgrade(),
                connection.isRunningUpgrade(), connection.buildingIndex, true);
        this.isAutoCommit = connection.isAutoCommit;
        this.isAutoFlush = connection.isAutoFlush;
        this.sampler = connection.sampler;
        this.statementExecutionCounter = connection.statementExecutionCounter;
    }

    public PhoenixConnection(ConnectionQueryServices services, String url,
            Properties info) throws SQLException {
        this(services, url, info, null, false, false, false, false);
    }

    public PhoenixConnection(PhoenixConnection connection,
            ConnectionQueryServices services, Properties info)
                    throws SQLException {
        this(services, connection.url, info, null,
                connection.isDescVarLengthRowKeyUpgrade(), connection
                .isRunningUpgrade(), connection.buildingIndex, true);
    }

    private PhoenixConnection(ConnectionQueryServices services, String url,
            Properties info, MutationState mutationState,
            boolean isDescVarLengthRowKeyUpgrade, boolean isRunningUpgrade,
            boolean buildingIndex, boolean isInternalConnection) throws SQLException {
            this.url = url;
            this.isDescVarLengthRowKeyUpgrade = isDescVarLengthRowKeyUpgrade;
            this.isInternalConnection = isInternalConnection;

            // Filter user provided properties based on property policy, if
            // provided and QueryServices.PROPERTY_POLICY_PROVIDER_ENABLED is true
            if (Boolean.valueOf(info.getProperty(QueryServices.PROPERTY_POLICY_PROVIDER_ENABLED,
                    String.valueOf(QueryServicesOptions.DEFAULT_PROPERTY_POLICY_PROVIDER_ENABLED)))) {
                PropertyPolicyProvider.getPropertyPolicy().evaluate(info);
            }

            // Copy so client cannot change
            this.info = PropertiesUtil.deepCopy(info);
            final PName tenantId = JDBCUtil.getTenantId(url, info);
            if (this.info.isEmpty() && tenantId == null) {
                this.services = services;
            } else {
                // Create child services keyed by tenantId to track resource usage
                // for
                // a tenantId for all connections on this JVM.
                if (tenantId != null) {
                    services = services.getChildQueryServices(tenantId
                            .getBytesPtr());
                }
                ReadOnlyProps currentProps = services.getProps();
                final ReadOnlyProps augmentedProps = currentProps
                        .addAll(filterKnownNonProperties(this.info));
                this.services = augmentedProps == currentProps ? services
                        : new DelegateConnectionQueryServices(services) {
                    @Override
                    public ReadOnlyProps getProps() {
                        return augmentedProps;
                    }
                };
            }

            Long scnParam = JDBCUtil.getCurrentSCN(url, this.info);
            checkScn(scnParam);
            Long buildIndexAtParam = JDBCUtil.getBuildIndexSCN(url, this.info);
            checkBuildIndexAt(buildIndexAtParam);
            checkScnAndBuildIndexAtEquality(scnParam, buildIndexAtParam);

            this.scn = scnParam != null ? scnParam : buildIndexAtParam;
            this.buildingIndex = buildingIndex || buildIndexAtParam != null;
            this.isAutoFlush = this.services.getProps().getBoolean(
                    QueryServices.TRANSACTIONS_ENABLED,
                    QueryServicesOptions.DEFAULT_TRANSACTIONS_ENABLED)
                    && this.services.getProps().getBoolean(
                    QueryServices.AUTO_FLUSH_ATTRIB,
                    QueryServicesOptions.DEFAULT_AUTO_FLUSH);
            this.isAutoCommit = JDBCUtil.getAutoCommit(
                    url,
                    this.info,
                    this.services.getProps().getBoolean(
                            QueryServices.AUTO_COMMIT_ATTRIB,
                            QueryServicesOptions.DEFAULT_AUTO_COMMIT));
            this.consistency = JDBCUtil.getConsistencyLevel(
                    url,
                    this.info,
                    this.services.getProps().get(QueryServices.CONSISTENCY_ATTRIB,
                            QueryServicesOptions.DEFAULT_CONSISTENCY_LEVEL));
            // currently we are not resolving schema set through property, so if
            // schema doesn't exists ,connection will not fail
            // but queries may fail
            this.schema = JDBCUtil.getSchema(
                    url,
                    this.info,
                    this.services.getProps().get(QueryServices.SCHEMA_ATTRIB,
                            QueryServicesOptions.DEFAULT_SCHEMA));
            this.tenantId = tenantId;
            this.mutateBatchSize = JDBCUtil.getMutateBatchSize(url, this.info,
                    this.services.getProps());
            this.mutateBatchSizeBytes = JDBCUtil.getMutateBatchSizeBytes(url,
                    this.info, this.services.getProps());
            datePattern = this.services.getProps().get(
                    QueryServices.DATE_FORMAT_ATTRIB, DateUtil.DEFAULT_DATE_FORMAT);
            timePattern = this.services.getProps().get(
                    QueryServices.TIME_FORMAT_ATTRIB, DateUtil.DEFAULT_TIME_FORMAT);
            timestampPattern = this.services.getProps().get(
                    QueryServices.TIMESTAMP_FORMAT_ATTRIB,
                    DateUtil.DEFAULT_TIMESTAMP_FORMAT);
            String numberPattern = this.services.getProps().get(
                    QueryServices.NUMBER_FORMAT_ATTRIB,
                    NumberUtil.DEFAULT_NUMBER_FORMAT);
            int maxSize = this.services.getProps().getInt(
                    QueryServices.MAX_MUTATION_SIZE_ATTRIB,
                    QueryServicesOptions.DEFAULT_MAX_MUTATION_SIZE);
            long maxSizeBytes = this.services.getProps().getLongBytes(
                    QueryServices.MAX_MUTATION_SIZE_BYTES_ATTRIB,
                    QueryServicesOptions.DEFAULT_MAX_MUTATION_SIZE_BYTES);
            this.isApplyTimeZoneDisplacement = this.services.getProps().getBoolean(
                QueryServices.APPLY_TIME_ZONE_DISPLACMENT_ATTRIB,
                QueryServicesOptions.DEFAULT_APPLY_TIME_ZONE_DISPLACMENT);
            this.dateFormatTimeZoneId = this.services.getProps().get(QueryServices.DATE_FORMAT_TIMEZONE_ATTRIB,
                    DateUtil.DEFAULT_TIME_ZONE_ID);
            Format dateFormat = DateUtil.getDateFormatter(datePattern, dateFormatTimeZoneId);
            Format timeFormat = DateUtil.getDateFormatter(timePattern, dateFormatTimeZoneId);
            Format timestampFormat = DateUtil.getDateFormatter(timestampPattern, dateFormatTimeZoneId);
            formatters.put(PDate.INSTANCE, dateFormat);
            formatters.put(PTime.INSTANCE, timeFormat);
            formatters.put(PTimestamp.INSTANCE, timestampFormat);
            formatters.put(PUnsignedDate.INSTANCE, dateFormat);
            formatters.put(PUnsignedTime.INSTANCE, timeFormat);
            formatters.put(PUnsignedTimestamp.INSTANCE, timestampFormat);
            formatters.put(PDecimal.INSTANCE,
                    FunctionArgumentType.NUMERIC.getFormatter(numberPattern));
            formatters.put(PVarbinary.INSTANCE, VarBinaryFormatter.INSTANCE);
            formatters.put(PBinary.INSTANCE, VarBinaryFormatter.INSTANCE);

            this.logLevel = LogLevel.valueOf(this.services.getProps().get(QueryServices.LOG_LEVEL,
                    QueryServicesOptions.DEFAULT_LOGGING_LEVEL));
            this.auditLogLevel = LogLevel.valueOf(this.services.getProps().get(QueryServices.AUDIT_LOG_LEVEL,
                    QueryServicesOptions.DEFAULT_AUDIT_LOGGING_LEVEL));
            this.isRequestLevelMetricsEnabled = JDBCUtil.isCollectingRequestLevelMetricsEnabled(url, info,
                    this.services.getProps());
            this.mutationState = mutationState == null ? newMutationState(maxSize,
                    maxSizeBytes) : new MutationState(mutationState, this);
            this.uniqueID = UUID.randomUUID();
            this.services.addConnection(this);

            // setup tracing, if its enabled
            this.sampler = Tracing.getConfiguredSampler(this);
            this.customTracingAnnotations = getImmutableCustomTracingAnnotations();
            this.scannerQueue = new LinkedBlockingQueue<>();
            this.tableResultIteratorFactory = new DefaultTableResultIteratorFactory();
            this.isRunningUpgrade = isRunningUpgrade;

            this.logSamplingRate = Double.parseDouble(this.services.getProps().get(QueryServices.LOG_SAMPLE_RATE,
                    QueryServicesOptions.DEFAULT_LOG_SAMPLE_RATE));
        String connectionQueryServiceName =
                this.services.getConfiguration().get(QUERY_SERVICES_NAME);
        if (isInternalConnection) {
            GLOBAL_OPEN_INTERNAL_PHOENIX_CONNECTIONS.increment();
            long currentInternalConnectionCount =
                    this.getQueryServices().getConnectionCount(isInternalConnection);
            ConnectionQueryServicesMetricsManager.updateMetrics(connectionQueryServiceName,
                    OPEN_INTERNAL_PHOENIX_CONNECTIONS_COUNTER, currentInternalConnectionCount);
            ConnectionQueryServicesMetricsManager
                .updateConnectionQueryServiceOpenInternalConnectionHistogram(
                        currentInternalConnectionCount, connectionQueryServiceName);
        } else {
            GLOBAL_OPEN_PHOENIX_CONNECTIONS.increment();
            long currentConnectionCount =
                    this.getQueryServices().getConnectionCount(isInternalConnection);
            ConnectionQueryServicesMetricsManager.updateMetrics(connectionQueryServiceName,
                    OPEN_PHOENIX_CONNECTIONS_COUNTER, currentConnectionCount);
            ConnectionQueryServicesMetricsManager
                .updateConnectionQueryServiceOpenConnectionHistogram(currentConnectionCount,
                        connectionQueryServiceName);
            }
        this.sourceOfOperation = this.services.getProps()
            .get(QueryServices.SOURCE_OPERATION_ATTRIB, null);
    }

    private static void checkScn(Long scnParam) throws SQLException {
        if (scnParam != null && scnParam < 0) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_SCN)
            .build().buildException();
        }
    }

    private static void checkBuildIndexAt(Long replayAtParam) throws SQLException {
        if (replayAtParam != null && replayAtParam < 0) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.INVALID_REPLAY_AT).build()
                    .buildException();
        }
    }

    private static void checkScnAndBuildIndexAtEquality(Long scnParam, Long replayAt)
            throws SQLException {
        if (scnParam != null && replayAt != null && !scnParam.equals(replayAt)) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.UNEQUAL_SCN_AND_BUILD_INDEX_AT).build()
                    .buildException();
        }
    }

    private static Properties filterKnownNonProperties(Properties info) {
        Properties prunedProperties = info;
        for (String property : CONNECTION_PROPERTIES) {
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
            result.put(PhoenixRuntime.TENANT_ID_ATTRIB, getTenantId()
                    .getString());
        }
        return result.build();
    }

    public boolean isInternalConnection() {
        return isInternalConnection;
    }

    /**
     * Add connection to the internal childConnections queue
     * This method is thread safe
     * @param connection
     */
    public void addChildConnection(PhoenixConnection connection) {
        childConnections.add(connection);
    }

    /**
     * Method to remove child connection from childConnections Queue
     *
     * @param connection
     */
    public void removeChildConnection(PhoenixConnection connection) {
        childConnections.remove(connection);
    }

    /**
     * Method to fetch child connections count from childConnections Queue
     *
     * @return int count
     */
    @VisibleForTesting
    public int getChildConnectionsCount() {
        return childConnections.size();
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

    public int executeStatements(Reader reader, List<Object> binds,
            PrintStream out) throws IOException, SQLException {
        int bindsOffset = 0;
        int nStatements = 0;
        PhoenixStatementParser parser = new PhoenixStatementParser(reader);
        try {
            while (true) {
                PhoenixPreparedStatement stmt = null;
                try {
                    stmt = new PhoenixPreparedStatement(this, parser);
                    ParameterMetaData paramMetaData = stmt
                            .getParameterMetaData();
                    for (int i = 0; i < paramMetaData.getParameterCount(); i++) {
                        stmt.setObject(i + 1, binds.get(bindsOffset + i));
                    }
                    long start = EnvironmentEdgeManager.currentTimeMillis();
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
                                    int displayWidth = md
                                            .getColumnDisplaySize(i);
                                    String label = md.getColumnLabel(i);
                                    if (md.isSigned(i)) {
                                        out.print(displayWidth < label.length() ? label
                                                .substring(0, displayWidth)
                                                : Strings.padStart(label,
                                                        displayWidth, ' '));
                                        out.print(' ');
                                    } else {
                                        out.print(displayWidth < label.length() ? label
                                                .substring(0, displayWidth)
                                                : Strings.padEnd(
                                                        md.getColumnLabel(i),
                                                        displayWidth, ' '));
                                        out.print(' ');
                                    }
                                }
                                out.println();
                                for (int i = 1; i <= columnCount; i++) {
                                    int displayWidth = md
                                            .getColumnDisplaySize(i);
                                    out.print(Strings.padStart("",
                                            displayWidth, '-'));
                                    out.print(' ');
                                }
                                out.println();
                            }
                            do {
                                if (out != null) {
                                    ResultSetMetaData md = rs.getMetaData();
                                    for (int i = 1; i <= columnCount; i++) {
                                        int displayWidth = md
                                                .getColumnDisplaySize(i);
                                        String value = rs.getString(i);
                                        String valueString = value == null ? QueryConstants.NULL_DISPLAY_TEXT
                                                : value;
                                        if (md.isSigned(i)) {
                                            out.print(Strings.padStart(
                                                    valueString, displayWidth,
                                                    ' '));
                                        } else {
                                            out.print(Strings.padEnd(
                                                    valueString, displayWidth,
                                                    ' '));
                                        }
                                        out.print(' ');
                                    }
                                    out.println();
                                }
                            } while (rs.next());
                        }
                    } else if (out != null) {
                        int updateCount = stmt.getUpdateCount();
                        if (updateCount >= 0) {
                            out.println((updateCount == 0 ? "no" : updateCount)
                                    + (updateCount == 1 ? " row " : " rows ")
                                    + stmt.getUpdateOperation().toString());
                        }
                    }
                    bindsOffset += paramMetaData.getParameterCount();
                    double elapsedDuration = ((EnvironmentEdgeManager.currentTimeMillis() - start) / 1000.0);
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

    public boolean isBuildingIndex() {
        return buildingIndex;
    }

    public int getMutateBatchSize() {
        return mutateBatchSize;
    }

    public long getMutateBatchSizeBytes() {
        return mutateBatchSizeBytes;
    }

    public PMetaData getMetaDataCache() {
        return getQueryServices().getMetaDataCache();
    }

    private boolean prune(PTable table) {
        long maxTimestamp = scn == null ? HConstants.LATEST_TIMESTAMP
                : scn;
        return (table.getType() != PTableType.SYSTEM && (table
                .getTimeStamp() >= maxTimestamp || (table.getTenantId() != null && !Objects
                .equal(tenantId, table.getTenantId()))));
    }

    /**
     * Similar to {@link #getTable(String, String, Long)} but returns the most recent
     * PTable
     */
    public PTable getTable(String tenantId, String fullTableName)
            throws SQLException {
        return getTable(tenantId, fullTableName, HConstants.LATEST_TIMESTAMP);
    }

    /**
     * Returns the PTable as of the timestamp provided. This method can be used to fetch tenant
     * specific PTable through a global connection. A null timestamp would result in the client side
     * metadata cache being used (ie. in case table metadata is already present it'll be returned).
     * To get the latest metadata use {@link #getTable(String, String)}
     * @param tenantId
     * @param fullTableName
     * @param timestamp
     * @return PTable
     * @throws SQLException
     * @throws NullPointerException if conn or fullTableName is null
     * @throws IllegalArgumentException if timestamp is negative
     */
    public PTable getTable(@Nullable String tenantId, String fullTableName,
            @Nullable Long timestamp) throws SQLException {
        checkNotNull(fullTableName);
        if (timestamp != null) {
            checkArgument(timestamp >= 0);
        }
        PTable table;
        PName pTenantId = (tenantId == null) ? null : PNameFactory.newName(tenantId);
        try {
            PTableRef tableref = getTableRef(new PTableKey(pTenantId, fullTableName));
            if (timestamp == null
                    || (tableref != null && tableref.getResolvedTimeStamp() == timestamp)) {
                table = tableref.getTable();
            } else {
                throw new TableNotFoundException(fullTableName);
            }
        } catch (TableNotFoundException e) {
            table = getTableNoCache(pTenantId, fullTableName, timestamp);
        }
        return table;
    }

    public PTable getTableNoCache(PName tenantId, String name, long timestamp) throws SQLException {
        String schemaName = SchemaUtil.getSchemaNameFromFullName(name);
        String tableName = SchemaUtil.getTableNameFromFullName(name);
        MetaDataProtocol.MetaDataMutationResult result =
                new MetaDataClient(this).updateCache(tenantId, schemaName, tableName, false,
                        timestamp);
        if (result.getMutationCode() != MetaDataProtocol.MutationCode.TABLE_ALREADY_EXISTS) {
            throw new TableNotFoundException(schemaName, tableName);
        }
        return result.getTable();
    }

    public PTable getTableNoCache(PName tenantId, String name) throws SQLException {
        String schemaName = SchemaUtil.getSchemaNameFromFullName(name);
        String tableName = SchemaUtil.getTableNameFromFullName(name);
        MetaDataProtocol.MetaDataMutationResult result =
                new MetaDataClient(this).updateCache(tenantId, schemaName,
                        tableName, true);
        if (result.getMutationCode() != MetaDataProtocol.MutationCode.TABLE_ALREADY_EXISTS) {
            throw new TableNotFoundException(schemaName, tableName);
        }
        return result.getTable();
    }
    @VisibleForTesting
    public PTable getTableNoCache(String name) throws SQLException {
        return getTableNoCache(getTenantId(), name);
    }

    /**
     * Returns the table if it is found in the client metadata cache. If the metadata of this
     * table has changed since it was put in the cache these changes will not necessarily be
     * reflected in the returned table. If the table is not found, makes a call to the server to
     * fetch the latest metadata of the table. This is different than how a table is resolved when
     * it is referenced from a query (a call is made to the server to fetch the latest metadata of
     * the table depending on the UPDATE_CACHE_FREQUENCY property)
     * See https://issues.apache.org/jira/browse/PHOENIX-4475
     * @param name requires a pre-normalized table name or a pre-normalized schema and table name
     * @return
     * @throws SQLException
     */
    public PTable getTable(String name) throws SQLException {
        return getTable(new PTableKey(getTenantId(), name));
    }

    public PTable getTable(PTableKey key) throws SQLException {
        PTable table;
        try {
            table = getTableRef(key).getTable();
        } catch (TableNotFoundException e) {
            table =  getTableNoCache(key.getName());
        }
        return table;
    }

    public PTableRef getTableRef(PTableKey key) throws TableNotFoundException {
        PTableRef tableRef = getQueryServices().getMetaDataCache().getTableRef(key);
        if (prune(tableRef.getTable())) {
            throw new TableNotFoundException(key.getName());
        }
        return  tableRef;
    }

    public boolean prune(PFunction function) {
        long maxTimestamp = scn == null ? HConstants.LATEST_TIMESTAMP
                : scn;
        return (function.getTimeStamp() >= maxTimestamp || (function
                .getTenantId() != null && !Objects.equal(tenantId,
                function.getTenantId())));
    }

    public PFunction getFunction(PTableKey key) throws FunctionNotFoundException {
        PFunction function = getQueryServices().getMetaDataCache().getFunction(key);
        return prune(function) ? null : function;
    }
    protected MutationState newMutationState(int maxSize, long maxSizeBytes) {
        return new MutationState(maxSize, maxSizeBytes, this);
    }

    public MutationState getMutationState() {
        return mutationState;
    }

    public String getDatePattern() {
        return datePattern;
    }

    public String getTimePattern() {
        return timePattern;
    }

    public String getTimestampPattern() {
        return timestampPattern;
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
        try {
            mutationState.rollback();
        } catch (SQLException e) {
            // ignore any exceptions while rolling back
        } finally {
            try {
                // create new set to prevent close of statements from modifying this collection.
                // TODO This could be optimized out by decoupling closing the stmt and removing it
                // from the connection.
                HashSet<? extends PhoenixStatement> statementsCopy = new HashSet<>(this.statements);
                SQLCloseables.closeAll(statementsCopy);
            } finally {
                statements.clear();
            }
        }
    }

    void checkOpen() throws SQLException {
        if (isClosed || isClosing) {
            throw reasonForClose != null
                ? reasonForClose
                : new SQLExceptionInfo.Builder(SQLExceptionCode.CONNECTION_CLOSED)
                    .build()

            .buildException();
        }
    }

    /**
     * Close the Phoenix connection and also store the reason for it getting closed.
     *
     * @param reasonForClose The reason for closing the phoenix connection to be set as state
     *                        in phoenix connection.
     * @throws SQLException if error happens when closing.
     * @see #close()
     */
    public void close(SQLException reasonForClose) throws SQLException {
        if (isClosed || isClosing) {
            return;
        }
        this.reasonForClose = reasonForClose;
        close();
    }

    // A connection can be closed by calling thread, or by the high availability (HA) framework.
    // Making this logic synchronized will enforce a connection is closed only once.
    //Does this need to be synchronized?
    @Override
    synchronized public void close() throws SQLException {
        if (isClosed || isClosing) {
            return;
        }

        String connectionQueryServiceName =
                this.services.getConfiguration().get(QUERY_SERVICES_NAME);
        try {
            isClosing = true;
            TableMetricsManager.pushMetricsFromConnInstanceMethod(getMutationMetrics());
            if(!(reasonForClose instanceof FailoverSQLException)) {
                // If the reason for close is because of failover, the metrics will be kept for
                // consolidation by the wrapper PhoenixFailoverConnection object.
                clearMetrics();
            }
            try {
                closeStatements();
                if (childConnections != null) {
                    SQLCloseables.closeAllQuietly(childConnections);
                }
                if (traceScope != null) {
                    traceScope.close();
                }
            } finally {
                services.removeConnection(this);
            }

        } finally {
            isClosing = false;
            isClosed = true;
            if (isInternalConnection()){
                GLOBAL_OPEN_INTERNAL_PHOENIX_CONNECTIONS.decrement();
                long currentInternalConnectionCount =
                        this.getQueryServices().getConnectionCount(isInternalConnection());
                ConnectionQueryServicesMetricsManager.updateMetrics(connectionQueryServiceName,
                        OPEN_INTERNAL_PHOENIX_CONNECTIONS_COUNTER, currentInternalConnectionCount);
                ConnectionQueryServicesMetricsManager
                        .updateConnectionQueryServiceOpenInternalConnectionHistogram(
                                currentInternalConnectionCount, connectionQueryServiceName);
            } else {
                GLOBAL_OPEN_PHOENIX_CONNECTIONS.decrement();
                long currentConnectionCount =
                        this.getQueryServices().getConnectionCount(isInternalConnection());
                ConnectionQueryServicesMetricsManager.updateMetrics(connectionQueryServiceName,
                                OPEN_PHOENIX_CONNECTIONS_COUNTER, currentConnectionCount);
                ConnectionQueryServicesMetricsManager
                        .updateConnectionQueryServiceOpenConnectionHistogram(
                                currentConnectionCount, connectionQueryServiceName);
            }
        }
    }

    @Override
    public void commit() throws SQLException {
        CallRunner.run(new CallRunner.CallableThrowable<Void, SQLException>() {
            @Override
            public Void call() throws SQLException {
                checkOpen();
                try {
                    mutationState.commit();
                } finally {
                    mutationState.resetExecuteMutationTimeMap();
                }
                return null;
            }
        }, Tracing.withTracing(this, "committing mutations"));
        statementExecutionCounter = 0;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements)
            throws SQLException {
        checkOpen();
        PDataType arrayPrimitiveType = PDataType.fromSqlTypeName(typeName);
        return PArrayDataType.instantiatePhoenixArray(arrayPrimitiveType,
                elements);
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
        checkOpen();
        PhoenixStatement statement = new PhoenixStatement(this);
        statements.add(statement);
        return statement;
    }

    /**
     * Back-door way to inject processing into walking through a result set
     *
     * @param statementFactory
     * @return PhoenixStatement
     * @throws SQLException
     */
    public PhoenixStatement createStatement(
            PhoenixStatementFactory statementFactory) throws SQLException {
        PhoenixStatement statement = statementFactory.newStatement(this);
        statements.add(statement);
        return statement;
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws SQLException {
        checkOpen();
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY
                || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw new SQLFeatureNotSupportedException();
        }
        return createStatement();
    }

    @Override
    public Statement createStatement(int resultSetType,
            int resultSetConcurrency, int resultSetHoldability)
                    throws SQLException {
        checkOpen();
        if (resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
            throw new SQLFeatureNotSupportedException();
        }
        return createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes)
            throws SQLException {
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
        if (autoFlush
                && !this.services.getProps().getBoolean(
                        QueryServices.TRANSACTIONS_ENABLED,
                        QueryServicesOptions.DEFAULT_TRANSACTIONS_ENABLED)) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.TX_MUST_BE_ENABLED_TO_SET_AUTO_FLUSH)
            .build().buildException();
        }
        this.isAutoFlush = autoFlush;
    }

    public void flush() throws SQLException {
        mutationState.sendUncommitted();
    }

    public void setTransactionContext(PhoenixTransactionContext txContext)
            throws SQLException {
        if (!this.services.getProps().getBoolean(
                QueryServices.TRANSACTIONS_ENABLED,
                QueryServicesOptions.DEFAULT_TRANSACTIONS_ENABLED)) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.TX_MUST_BE_ENABLED_TO_SET_TX_CONTEXT)
            .build().buildException();
        }
        this.mutationState.rollback();
        this.mutationState = new MutationState(this.mutationState.getMaxSize(),
                this.mutationState.getMaxSizeBytes(), this, txContext);

        // Write data to HBase after each statement execution as the commit may
        // not
        // come through Phoenix APIs.
        setAutoFlush(true);
    }

    public Consistency getConsistency() {
        return this.consistency;
    }

    @Override
    public String getCatalog() throws SQLException {
        return tenantId == null ? "" : tenantId.getString();
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
        checkOpen();
        return new PhoenixDatabaseMetaData(this);
    }

    public UUID getUniqueID() {
        return this.uniqueID;
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        boolean transactionsEnabled = getQueryServices().getProps().getBoolean(
                QueryServices.TRANSACTIONS_ENABLED,
                QueryServicesOptions.DEFAULT_TRANSACTIONS_ENABLED);
        return transactionsEnabled ? Connection.TRANSACTION_REPEATABLE_READ
                : Connection.TRANSACTION_READ_COMMITTED;
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

    public boolean isClosing() throws SQLException {
        return isClosing;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return readOnly;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        // TODO: run query here or ping
        return !isClosed && !isClosing;
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
    public CallableStatement prepareCall(String sql, int resultSetType,
            int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability)
                    throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkOpen();
        PhoenixPreparedStatement statement = new PhoenixPreparedStatement(this,
                sql);
        statements.add(statement);
        return statement;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
            throws SQLException {
        checkOpen();
        // Ignore autoGeneratedKeys, and just execute the statement.
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
            throws SQLException {
        checkOpen();
        // Ignore columnIndexes, and just execute the statement.
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames)
            throws SQLException {
        checkOpen();
        // Ignore columnNames, and just execute the statement.
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
            int resultSetConcurrency) throws SQLException {
        checkOpen();
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY
                || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw new SQLFeatureNotSupportedException();
        }
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability)
                    throws SQLException {
        checkOpen();
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
                checkOpen();
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
        checkOpen();
        this.isAutoCommit = isAutoCommit;
    }

    public void setConsistency(Consistency val) {
        this.consistency = val;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        checkOpen();
        if (!this.getCatalog().equalsIgnoreCase(catalog)) {
            // allow noop calls to pass through.
            throw new SQLFeatureNotSupportedException();
        }
        // TODO:
        // if (catalog == null) {
        // tenantId = null;
        // } else {
        // tenantId = PNameFactory.newName(catalog);
        // }
    }

    @Override
    public void setClientInfo(Properties properties)
            throws SQLClientInfoException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setClientInfo(String name, String value)
            throws SQLClientInfoException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        checkOpen();
        this.readOnly = readOnly;
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
        checkOpen();
        boolean transactionsEnabled = getQueryServices().getProps().getBoolean(
                QueryServices.TRANSACTIONS_ENABLED,
                QueryServicesOptions.DEFAULT_TRANSACTIONS_ENABLED);
        if (level == Connection.TRANSACTION_SERIALIZABLE) {
            throw new SQLFeatureNotSupportedException();
        }
        if (!transactionsEnabled
                && level == Connection.TRANSACTION_REPEATABLE_READ) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.TX_MUST_BE_ENABLED_TO_SET_ISOLATION_LEVEL)
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
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.CLASS_NOT_UNWRAPPABLE)
            .setMessage(
                    this.getClass().getName()
                    + " not unwrappable from "
                    + iface.getName()).build().buildException();
        }
        return (T) this;
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        checkOpen();
        this.schema = schema;
    }

    @Override
    public String getSchema() throws SQLException {
        return SchemaUtil.normalizeIdentifier(this.schema);
    }

    public PSchema getSchema(PTableKey key) throws SchemaNotFoundException {
        return getQueryServices().getMetaDataCache().getSchema(key);
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        checkOpen();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds)
            throws SQLException {
        checkOpen();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    private boolean useMetaDataCache(PTable table) {
        return table.getType() == PTableType.SYSTEM
                || table.getUpdateCacheFrequency() != 0
                || (Long) ConnectionProperty.UPDATE_CACHE_FREQUENCY.getValue(
                        getQueryServices().getProps().get(QueryServices.DEFAULT_UPDATE_CACHE_FREQUENCY_ATRRIB)) != 0;

    }
    @Override
    public void addTable(PTable table, long resolvedTime) throws SQLException {
        getQueryServices().addTable(table, resolvedTime);
    }

    @Override
    public void updateResolvedTimestamp(PTable table, long resolvedTime)
            throws SQLException {

        getQueryServices().updateResolvedTimestamp(table, resolvedTime);
    }

    @Override
    public void addFunction(PFunction function) throws SQLException {
        getQueryServices().addFunction(function);
    }

    @Override
    public void addSchema(PSchema schema) throws SQLException {
        getQueryServices().addSchema(schema);
    }

    @Override
    public void removeTable(PName tenantId, String tableName,
            String parentTableName, long tableTimeStamp) throws SQLException {
        getQueryServices().removeTable(tenantId, tableName, parentTableName,
                tableTimeStamp);
    }

    @Override
    public void removeFunction(PName tenantId, String functionName,
            long tableTimeStamp) throws SQLException {
        getQueryServices().removeFunction(tenantId, functionName,
                tableTimeStamp);
    }

    @Override
    public void removeColumn(PName tenantId, String tableName,
            List<PColumn> columnsToRemove, long tableTimeStamp,
            long tableSeqNum, long resolvedTime) throws SQLException {
        getQueryServices().removeColumn(tenantId, tableName, columnsToRemove,
                tableTimeStamp, tableSeqNum, resolvedTime);
    }

    protected boolean removeStatement(PhoenixStatement statement)
            throws SQLException {
        return statements.remove(statement);
    }

    public KeyValueBuilder getKeyValueBuilder() {
        return this.services.getKeyValueBuilder();
    }

    /**
     * Used to track executions of {@link Statement}s and
     * {@link PreparedStatement}s that were created from this connection before
     * commit or rollback. 0-based. Used to associate partial save errors with
     * SQL statements invoked by users.
     *
     * @see CommitException
     * @see #incrementStatementExecutionCounter()
     */
    public int getStatementExecutionCounter() {
        return statementExecutionCounter;
    }

    public void incrementStatementExecutionCounter() {
        statementExecutionCounter++;
        if (connectionActivityLogger.isLevelEnabled(ActivityLogInfo.OP_STMTS.getLogLevel())) {
            connectionActivityLogger.log(ActivityLogInfo.OP_STMTS, String.valueOf(statementExecutionCounter));
        }
    }

    public TraceScope getTraceScope() {
        return traceScope;
    }

    public void setTraceScope(TraceScope traceScope) {
        this.traceScope = traceScope;
    }

    @Override
    public Map<String, Map<MetricType, Long>> getMutationMetrics() {
        return mutationState.getMutationMetricQueue().aggregate();
    }

    @Override
    public Map<String, Map<MetricType, Long>> getReadMetrics() {
        return mutationState.getReadMetricQueue() != null ? mutationState
                .getReadMetricQueue().aggregate() : Collections
                .<String, Map<MetricType, Long>> emptyMap();
    }

    @Override
    public boolean isRequestLevelMetricsEnabled() {
        return isRequestLevelMetricsEnabled;
    }

    @Override
    public void clearMetrics() {
        mutationState.getMutationMetricQueue().clearMetrics();
        if (mutationState.getReadMetricQueue() != null) {
            mutationState.getReadMetricQueue().clearMetrics();
        }
    }

    /**
     * Returns true if this connection is being used to upgrade the data due to
     * PHOENIX-2067 and false otherwise.
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
    public void setIteratorFactory(
            ParallelIteratorFactory parallelIteratorFactory) {
        this.parallelIteratorFactory = parallelIteratorFactory;
    }

    public void addIteratorForLeaseRenewal(@Nonnull TableResultIterator itr) {
        if (services.isRenewingLeasesEnabled()) {
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

     /**
     * Added for testing purposes. Do not use this elsewhere.
     */
    @VisibleForTesting
    public void setIsClosing(boolean imitateIsClosing) {
        isClosing = imitateIsClosing;
    }

    @Override
    public void removeSchema(PSchema schema, long schemaTimeStamp) {
        getQueryServices().removeSchema(schema, schemaTimeStamp);

    }

    public boolean isRunningUpgrade() {
        return isRunningUpgrade;
    }

    public void setRunningUpgrade(boolean isRunningUpgrade) {
        this.isRunningUpgrade = isRunningUpgrade;
    }

    public LogLevel getLogLevel(){
        return this.logLevel;
    }

    public LogLevel getAuditLogLevel(){
        return this.auditLogLevel;
    }

    public Double getLogSamplingRate(){
        return this.logSamplingRate;
    }

    /**
     *
     * @return source of operation
     */
    public String getSourceOfOperation() {
        return sourceOfOperation;
    }

    public String getDateFormatTimeZoneId() {
        return dateFormatTimeZoneId;
    }

    public boolean isApplyTimeZoneDisplacement() {
        return isApplyTimeZoneDisplacement;
    }

    public String getActivityLog() {
        return getActivityLogger().getActivityLog();
    }

    public ConnectionActivityLogger getActivityLogger() {
        return this.connectionActivityLogger;
    }

    public void setActivityLogger(ConnectionActivityLogger connectionActivityLogger) {
        this.connectionActivityLogger = connectionActivityLogger;
    }
}
