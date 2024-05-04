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
package org.apache.phoenix.query;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.KEEP_DELETED_CELLS;
import static org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.MAX_VERSIONS;
import static org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.REPLICATION_SCOPE;
import static org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.TTL;
import static org.apache.phoenix.coprocessorclient.MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP;
import static org.apache.phoenix.coprocessorclient.MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_15_0;
import static org.apache.phoenix.coprocessorclient.MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_16_0;
import static org.apache.phoenix.coprocessorclient.MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_5_2_0;
import static org.apache.phoenix.coprocessorclient.MetaDataProtocol.PHOENIX_MAJOR_VERSION;
import static org.apache.phoenix.coprocessorclient.MetaDataProtocol.PHOENIX_MINOR_VERSION;
import static org.apache.phoenix.coprocessorclient.MetaDataProtocol.PHOENIX_PATCH_NUMBER;
import static org.apache.phoenix.coprocessorclient.MetaDataProtocol.getVersion;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ARRAY_SIZE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_DEF;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_FAMILY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_SIZE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DECIMAL_DIGITS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DEFAULT_COLUMN_FAMILY_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IS_ROW_TIMESTAMP;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IS_VIEW_REFERENCED;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.KEY_SEQ;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.NULLABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ORDINAL_POSITION;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PK_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SORT_ORDER;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_FUNCTION_NAME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_FUNCTION_HBASE_TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_MUTEX_FAMILY_NAME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_MUTEX_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_MUTEX_TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_SCHEMA_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_STATS_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_TASK_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TASK_TABLE_TTL;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TENANT_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TRANSACTIONAL;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TTL_FOR_MUTEX;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_CONSTANT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_INDEX_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_HBASE_TABLE_NAME;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HCONNECTIONS_COUNTER;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_QUERY_SERVICES_COUNTER;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HBASE_COUNTER_METADATA_INCONSISTENCY;
import static org.apache.phoenix.monitoring.MetricType.NUM_SYSTEM_TABLE_RPC_FAILURES;
import static org.apache.phoenix.monitoring.MetricType.NUM_SYSTEM_TABLE_RPC_SUCCESS;
import static org.apache.phoenix.monitoring.MetricType.TIME_SPENT_IN_SYSTEM_TABLE_RPC_CALLS;
import static org.apache.phoenix.query.QueryConstants.DEFAULT_COLUMN_FAMILY;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_DROP_METADATA;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_RENEW_LEASE_ENABLED;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_RENEW_LEASE_THREAD_POOL_SIZE;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_RENEW_LEASE_THRESHOLD_MILLISECONDS;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_RUN_RENEW_LEASE_FREQUENCY_INTERVAL_MILLISECONDS;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_TIMEOUT_DURING_UPGRADE_MS;
import static org.apache.phoenix.util.UpgradeUtil.addParentToChildLinks;
import static org.apache.phoenix.util.UpgradeUtil.addViewIndexToParentLinks;
import static org.apache.phoenix.util.UpgradeUtil.getSysTableSnapshotName;
import static org.apache.phoenix.util.UpgradeUtil.moveOrCopyChildLinks;
import static org.apache.phoenix.util.UpgradeUtil.syncTableAndIndexProperties;
import static org.apache.phoenix.util.UpgradeUtil.upgradeTo4_5_0;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.ref.WeakReference;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import javax.annotation.concurrent.GuardedBy;

import com.google.protobuf.RpcController;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.CoprocessorDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.ipc.controller.ServerToServerRpcController;
import org.apache.hadoop.hbase.ipc.controller.ServerSideRPCControllerFactory;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.phoenix.compile.MutationPlan;
import org.apache.phoenix.coprocessor.generated.ChildLinkMetaDataProtos.ChildLinkMetaDataService;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.AddColumnRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.ClearCacheRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.ClearCacheResponse;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.ClearTableFromCacheRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.ClearTableFromCacheResponse;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.CreateFunctionRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.CreateSchemaRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.CreateTableRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.DropColumnRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.DropFunctionRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.DropSchemaRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.DropTableRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.GetFunctionsRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.GetSchemaRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.GetTableRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.GetVersionRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.GetVersionResponse;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.MetaDataResponse;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.MetaDataService;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.UpdateIndexStateRequest;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol.MutationCode;
import org.apache.phoenix.coprocessorclient.SequenceRegionObserverConstants;
import org.apache.phoenix.exception.InvalidRegionSplitPolicyException;
import org.apache.phoenix.exception.PhoenixIOException;
import org.apache.phoenix.exception.RetriableUpgradeException;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.exception.UpgradeInProgressException;
import org.apache.phoenix.exception.UpgradeNotRequiredException;
import org.apache.phoenix.exception.UpgradeRequiredException;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.hbase.index.util.VersionUtil;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.iterate.TableResultIterator;
import org.apache.phoenix.iterate.TableResultIterator.RenewLeaseStatus;
import org.apache.phoenix.jdbc.ConnectionInfo;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.log.ConnectionLimiter;
import org.apache.phoenix.log.DefaultConnectionLimiter;
import org.apache.phoenix.log.LoggingConnectionLimiter;
import org.apache.phoenix.log.QueryLoggerDisruptor;
import org.apache.phoenix.monitoring.TableMetricsManager;
import org.apache.phoenix.parse.PFunction;
import org.apache.phoenix.parse.PSchema;
import org.apache.phoenix.protobuf.ProtobufUtil;
import org.apache.phoenix.schema.ColumnAlreadyExistsException;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.schema.ConnectionProperty;
import org.apache.phoenix.schema.EmptySequenceCacheException;
import org.apache.phoenix.schema.FunctionNotFoundException;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.NewerSchemaAlreadyExistsException;
import org.apache.phoenix.schema.NewerTableAlreadyExistsException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PMetaData;
import org.apache.phoenix.schema.PMetaDataImpl;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.PTableRef;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.ReadOnlyTableException;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.schema.Sequence;
import org.apache.phoenix.schema.SequenceAllocation;
import org.apache.phoenix.schema.SequenceKey;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableAlreadyExistsException;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.TableProperty;
import org.apache.phoenix.schema.stats.GuidePostsInfo;
import org.apache.phoenix.schema.stats.GuidePostsKey;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PTinyint;
import org.apache.phoenix.schema.types.PUnsignedTinyint;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.transaction.PhoenixTransactionClient;
import org.apache.phoenix.transaction.PhoenixTransactionContext;
import org.apache.phoenix.transaction.PhoenixTransactionProvider;
import org.apache.phoenix.transaction.TransactionFactory;
import org.apache.phoenix.transaction.TransactionFactory.Provider;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.ClientUtil;
import org.apache.phoenix.util.Closeables;
import org.apache.phoenix.util.ConfigUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.JDBCUtil;
import org.apache.phoenix.util.LogUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixContextExecutor;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PhoenixStopWatch;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;
import org.apache.phoenix.util.UpgradeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.base.Joiner;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.thirdparty.com.google.common.base.Strings;
import org.apache.phoenix.thirdparty.com.google.common.base.Throwables;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.phoenix.thirdparty.com.google.common.collect.Iterables;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.thirdparty.com.google.common.collect.Sets;

public class ConnectionQueryServicesImpl extends DelegateQueryServices implements ConnectionQueryServices {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ConnectionQueryServicesImpl.class);
    private static final int INITIAL_CHILD_SERVICES_CAPACITY = 100;
    private static final int DEFAULT_OUT_OF_ORDER_MUTATIONS_WAIT_TIME_MS = 1000;
    private static final String ALTER_TABLE_SET_PROPS =
        "ALTER TABLE %s SET %s=%s";
    protected final Configuration config;

    public ConnectionInfo getConnectionInfo() {
        return connectionInfo;
    }

    protected final ConnectionInfo connectionInfo;
    // Copy of config.getProps(), but read-only to prevent synchronization that we
    // don't need.
    private final ReadOnlyProps props;
    private final String userName;
    private final User user;
    private final ConcurrentHashMap<ImmutableBytesWritable,ConnectionQueryServices> childServices;
    private GuidePostsCacheWrapper tableStatsCache;

    // Cache the latest meta data here for future connections
    // writes guarded by "latestMetaDataLock"
    private volatile PMetaData latestMetaData;
    private final Object latestMetaDataLock = new Object();

    // Lowest HBase version on the cluster.
    private int lowestClusterHBaseVersion = Integer.MAX_VALUE;
    private boolean hasIndexWALCodec = true;

    @GuardedBy("connectionCountLock")
    private int connectionCount = 0;

    @GuardedBy("connectionCountLock")
    private int internalConnectionCount = 0;

    private final Object connectionCountLock = new Object();
    private final boolean returnSequenceValues ;

    private Connection connection;
    private volatile boolean initialized;
    private volatile int nSequenceSaltBuckets;

    // writes guarded by "this"
    private volatile boolean closed;

    private volatile SQLException initializationException;
    // setting this member variable guarded by "connectionCountLock"
    private volatile ConcurrentMap<SequenceKey,Sequence> sequenceMap = Maps.newConcurrentMap();
    private KeyValueBuilder kvBuilder;

    private final int renewLeaseTaskFrequency;
    private final int renewLeasePoolSize;
    private final int renewLeaseThreshold;
    // List of queues instead of a single queue to provide reduced contention via lock striping
    private final List<LinkedBlockingQueue<WeakReference<PhoenixConnection>>> connectionQueues;
    private ScheduledExecutorService renewLeaseExecutor;
    // Use TransactionFactory.Provider.values() here not TransactionFactory.Provider.available()
    // because the array will be indexed by ordinal.
    private PhoenixTransactionClient[] txClients = new PhoenixTransactionClient[Provider.values().length];
    /*
     * We can have multiple instances of ConnectionQueryServices. By making the thread factory
     * static, renew lease thread names will be unique across them.
     */
    private static final ThreadFactory renewLeaseThreadFactory = new RenewLeaseThreadFactory();
    private final boolean renewLeaseEnabled;
    private final boolean isAutoUpgradeEnabled;
    private final AtomicBoolean upgradeRequired = new AtomicBoolean(false);
    private final int maxConnectionsAllowed;
    private final int maxInternalConnectionsAllowed;
    private final boolean shouldThrottleNumConnections;
    public static final byte[] MUTEX_LOCKED = "MUTEX_LOCKED".getBytes(StandardCharsets.UTF_8);
    private ServerSideRPCControllerFactory serverSideRPCControllerFactory;
    private boolean localIndexUpgradeRequired;

    private final boolean enableConnectionActivityLogging;
    private final int loggingIntervalInMins;

    private final ConnectionLimiter connectionLimiter;

    private static interface FeatureSupported {
        boolean isSupported(ConnectionQueryServices services);
    }

    private final Map<Feature, FeatureSupported> featureMap = ImmutableMap.<Feature, FeatureSupported>of(
            Feature.LOCAL_INDEX, new FeatureSupported() {
                @Override
                public boolean isSupported(ConnectionQueryServices services) {
                    int hbaseVersion = services.getLowestClusterHBaseVersion();
                    return hbaseVersion < MetaDataProtocol.MIN_LOCAL_SI_VERSION_DISALLOW || hbaseVersion > MetaDataProtocol.MAX_LOCAL_SI_VERSION_DISALLOW;
                }
            },
            Feature.RENEW_LEASE, new FeatureSupported() {
                @Override
                public boolean isSupported(ConnectionQueryServices services) {
                    int hbaseVersion = services.getLowestClusterHBaseVersion();
                    return hbaseVersion >= MetaDataProtocol.MIN_RENEW_LEASE_VERSION;
                }
            });
    private QueryLoggerDisruptor queryDisruptor;

    private PMetaData newEmptyMetaData() {
        return new PMetaDataImpl(INITIAL_META_DATA_TABLE_CAPACITY,
                (Long) ConnectionProperty.UPDATE_CACHE_FREQUENCY.getValue(
                getProps().get(QueryServices.DEFAULT_UPDATE_CACHE_FREQUENCY_ATRRIB)),
                        getProps());
    }

    /**
     * Construct a ConnectionQueryServicesImpl that represents a connection to an HBase
     * cluster.
     * @param services base services from where we derive our default configuration
     * @param connectionInfo to provide connection information
     * @param info hbase configuration properties
     */
    public ConnectionQueryServicesImpl(QueryServices services, ConnectionInfo connectionInfo, Properties info) {
        super(services);
        Configuration config = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
        for (Entry<String,String> entry : services.getProps()) {
            config.set(entry.getKey(), entry.getValue());
        }
        if (info != null) {
            for (Object key : info.keySet()) {
                config.set((String) key, info.getProperty((String) key));
            }
        }
        for (Entry<String,String> entry : connectionInfo.asProps()) {
            config.set(entry.getKey(), entry.getValue());
        }
        if (connectionInfo.getPrincipal() != null) {
            config.set(QUERY_SERVICES_NAME, connectionInfo.getPrincipal());
        }
        LOGGER.info(String.format("CQS initialized with connection query service : %s",
                config.get(QUERY_SERVICES_NAME)));
        this.connectionInfo = connectionInfo;

        // Without making a copy of the configuration we cons up, we lose some of our properties
        // on the server side during testing.
        this.config = HBaseFactoryProvider.getConfigurationFactory().getConfiguration(config);
        //Set the rpcControllerFactory if it is a server side connnection.
        boolean isServerSideConnection = config.getBoolean(QueryUtil.IS_SERVER_CONNECTION, false);
        if (isServerSideConnection) {
            this.serverSideRPCControllerFactory = new ServerSideRPCControllerFactory(config);
        }
        // set replication required parameter
        ConfigUtil.setReplicationConfigIfAbsent(this.config);
        this.props = new ReadOnlyProps(this.config.iterator());
        this.userName = connectionInfo.getPrincipal();
        this.user = connectionInfo.getUser();
        this.latestMetaData = newEmptyMetaData();
        // TODO: should we track connection wide memory usage or just org-wide usage?
        // If connection-wide, create a MemoryManager here, otherwise just use the one from the delegate
        this.childServices = new ConcurrentHashMap<ImmutableBytesWritable,ConnectionQueryServices>(INITIAL_CHILD_SERVICES_CAPACITY);
        // find the HBase version and use that to determine the KeyValueBuilder that should be used
        String hbaseVersion = VersionInfo.getVersion();
        this.kvBuilder = KeyValueBuilder.get(hbaseVersion);
        this.returnSequenceValues = props.getBoolean(QueryServices.RETURN_SEQUENCE_VALUES_ATTRIB, QueryServicesOptions.DEFAULT_RETURN_SEQUENCE_VALUES);
        this.renewLeaseEnabled = config.getBoolean(RENEW_LEASE_ENABLED, DEFAULT_RENEW_LEASE_ENABLED);
        this.renewLeasePoolSize = config.getInt(RENEW_LEASE_THREAD_POOL_SIZE, DEFAULT_RENEW_LEASE_THREAD_POOL_SIZE);
        this.renewLeaseThreshold = config.getInt(RENEW_LEASE_THRESHOLD_MILLISECONDS, DEFAULT_RENEW_LEASE_THRESHOLD_MILLISECONDS);
        this.renewLeaseTaskFrequency = config.getInt(RUN_RENEW_LEASE_FREQUENCY_INTERVAL_MILLISECONDS, DEFAULT_RUN_RENEW_LEASE_FREQUENCY_INTERVAL_MILLISECONDS);
        List<LinkedBlockingQueue<WeakReference<PhoenixConnection>>> list = Lists.newArrayListWithCapacity(renewLeasePoolSize);
        for (int i = 0; i < renewLeasePoolSize; i++) {
            LinkedBlockingQueue<WeakReference<PhoenixConnection>> queue = new LinkedBlockingQueue<WeakReference<PhoenixConnection>>();
            list.add(queue);
        }
        connectionQueues = ImmutableList.copyOf(list);

        this.isAutoUpgradeEnabled = config.getBoolean(AUTO_UPGRADE_ENABLED, QueryServicesOptions.DEFAULT_AUTO_UPGRADE_ENABLED);
        this.maxConnectionsAllowed = config.getInt(QueryServices.CLIENT_CONNECTION_MAX_ALLOWED_CONNECTIONS,
            QueryServicesOptions.DEFAULT_CLIENT_CONNECTION_MAX_ALLOWED_CONNECTIONS);
        this.maxInternalConnectionsAllowed = config.getInt(QueryServices.INTERNAL_CONNECTION_MAX_ALLOWED_CONNECTIONS,
                QueryServicesOptions.DEFAULT_INTERNAL_CONNECTION_MAX_ALLOWED_CONNECTIONS);
        this.shouldThrottleNumConnections = (maxConnectionsAllowed > 0) || (maxInternalConnectionsAllowed > 0);
        this.enableConnectionActivityLogging =
                config.getBoolean(CONNECTION_ACTIVITY_LOGGING_ENABLED,
                        QueryServicesOptions.DEFAULT_CONNECTION_ACTIVITY_LOGGING_ENABLED);
        this.loggingIntervalInMins =
                config.getInt(CONNECTION_ACTIVITY_LOGGING_INTERVAL,
                        QueryServicesOptions.DEFAULT_CONNECTION_ACTIVITY_LOGGING_INTERVAL_IN_MINS);

        if (enableConnectionActivityLogging) {
            LoggingConnectionLimiter.Builder builder = new LoggingConnectionLimiter.Builder(shouldThrottleNumConnections);
            connectionLimiter = builder
                    .withLoggingIntervalInMins(loggingIntervalInMins)
                    .withLogging(true)
                    .withMaxAllowed(this.maxConnectionsAllowed)
                    .withMaxInternalAllowed(this.maxInternalConnectionsAllowed)
                    .build();
        } else {
            DefaultConnectionLimiter.Builder builder = new DefaultConnectionLimiter.Builder(shouldThrottleNumConnections);
            connectionLimiter = builder
                    .withMaxAllowed(this.maxConnectionsAllowed)
                    .withMaxInternalAllowed(this.maxInternalConnectionsAllowed)
                    .build();
        }


        if (!QueryUtil.isServerConnection(props)) {
            //Start queryDistruptor everytime as log level can be change at connection level as well, but we can avoid starting for server connections.
            try {
                this.queryDisruptor = new QueryLoggerDisruptor(this.config);
            } catch (SQLException e) {
                LOGGER.warn("Unable to initiate query logging service !!");
                e.printStackTrace();
            }
        }

    }

    private void openConnection() throws SQLException {
        try {
            this.connection = HBaseFactoryProvider.getHConnectionFactory().createConnection(this.config);
            GLOBAL_HCONNECTIONS_COUNTER.increment();
            LOGGER.info("HConnection established. Stacktrace for informational purposes: "
                    + connection + " " +  LogUtil.getCallerStackTrace());
        } catch (IOException e) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_ESTABLISH_CONNECTION)
            .setRootCause(e).build().buildException();
        }
        if (this.connection.isClosed()) { // TODO: why the heck doesn't this throw above?
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_ESTABLISH_CONNECTION).build().buildException();
        }
    }

    /**
     * Close the HBase connection and decrement the counter.
     * @throws IOException throws IOException
     */
    private void closeConnection() throws IOException {
        if (connection != null) {
            connection.close();
            LOGGER.info("{} HConnection closed. Stacktrace for informational"
                + " purposes: {}", connection, LogUtil.getCallerStackTrace());
        }
        GLOBAL_HCONNECTIONS_COUNTER.decrement();
    }

    @Override
    public Table getTable(byte[] tableName) throws SQLException {
        try {
            return HBaseFactoryProvider.getHTableFactory().getTable(tableName,
                connection, null);
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public Table getTableIfExists(byte[] tableName) throws SQLException {
        try (Admin admin = getAdmin()) {
            if (!AdminUtilWithFallback.tableExists(admin, TableName.valueOf(tableName))) {
                throw new TableNotFoundException(
                    SchemaUtil.getSchemaNameFromFullName(tableName),
                    SchemaUtil.getTableNameFromFullName(tableName));
            }
        } catch (IOException | InterruptedException e) {
            throw new SQLException(e);
        }
        return getTable(tableName);
    }

    @Override
    public TableDescriptor getTableDescriptor(byte[] tableName) throws SQLException {
        Table htable = getTable(tableName);
        try {
            return htable.getDescriptor();
        } catch (IOException e) {
            if (e instanceof org.apache.hadoop.hbase.TableNotFoundException
                    || e.getCause() instanceof org.apache.hadoop.hbase.TableNotFoundException) {
                byte[][] schemaAndTableName = new byte[2][];
                SchemaUtil.getVarChars(tableName, schemaAndTableName);
                throw new TableNotFoundException(Bytes.toString(schemaAndTableName[0]), Bytes.toString(schemaAndTableName[1]));
            }
            throw new RuntimeException(e);
        } finally {
            Closeables.closeQuietly(htable);
        }
    }

    @Override
    public ReadOnlyProps getProps() {
        return props;
    }

    /**
     * Closes all the connections it has in its connectionQueues.
     */
    @Override
    public void closeAllConnections(SQLExceptionInfo.Builder reasonBuilder) {
        for (LinkedBlockingQueue<WeakReference<PhoenixConnection>> queue : connectionQueues) {
            for (WeakReference<PhoenixConnection> connectionReference : queue) {
                PhoenixConnection connection = connectionReference.get();
                try {
                    if (connection != null && !connection.isClosed()) {
                        connection.close(reasonBuilder.build().buildException());
                    }
                } catch (SQLException e) {
                    LOGGER.warn("Exception while closing phoenix connection {}", connection, e);
                }
            }
        }
    }


    /**
     * Closes the underlying connection to zookeeper. The QueryServices
     * may not be used after that point. When a Connection is closed,
     * this is not called, since these instances are pooled by the
     * Driver. Instead, the Driver should call this if the QueryServices
     * is ever removed from the pool
     */
    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            GLOBAL_QUERY_SERVICES_COUNTER.decrement();
            try {
                if (this.queryDisruptor != null) {
                    this.queryDisruptor.close();
                }
            } catch (Exception e) {
                // Ignore
            }
            SQLException sqlE = null;
            try {
                // Attempt to return any unused sequences.
                if (connection != null) returnAllSequences(this.sequenceMap);
            } catch (SQLException e) {
                sqlE = e;
            } finally {
                try {
                    childServices.clear();
                    synchronized (latestMetaDataLock) {
                        latestMetaData = null;
                        latestMetaDataLock.notifyAll();
                    }
                    try {
                        // close the HBase connection
                        closeConnection();
                    } finally {
                        if (renewLeaseExecutor != null) {
                            renewLeaseExecutor.shutdownNow();
                        }
                        // shut down the tx client service if we created one to support transactions
                        for (PhoenixTransactionClient client : txClients) {
                            if (client != null) {
                                client.close();
                            }
                        }
                    }
                } catch (IOException e) {
                    if (sqlE == null) {
                        sqlE = ClientUtil.parseServerException(e);
                    } else {
                        sqlE.setNextException(ClientUtil.parseServerException(e));
                    }
                } finally {
                    try {
                        tableStatsCache.invalidateAll();
                        super.close();
                    } catch (SQLException e) {
                        if (sqlE == null) {
                            sqlE = e;
                        } else {
                            sqlE.setNextException(e);
                        }
                    } finally {
                        if (sqlE != null) { throw sqlE; }
                    }
                }
            }
        }
    }

    protected ConnectionQueryServices newChildQueryService() {
        return new ChildQueryServices(this);
    }

    /**
     * Get (and create if necessary) a child QueryService for a given tenantId.
     * The QueryService will be cached for the lifetime of the parent QueryService
     * @param tenantId the tenant ID
     * @return the child QueryService
     */
    @Override
    public ConnectionQueryServices getChildQueryServices(ImmutableBytesWritable tenantId) {
        ConnectionQueryServices childQueryService = childServices.get(tenantId);
        if (childQueryService == null) {
            childQueryService = newChildQueryService();
            ConnectionQueryServices prevQueryService = childServices.putIfAbsent(tenantId, childQueryService);
            return prevQueryService == null ? childQueryService : prevQueryService;
        }
        return childQueryService;
    }

    @Override
    public void clearTableRegionCache(TableName tableName) throws SQLException {
        ((ClusterConnection)connection).clearRegionCache(tableName);
    }

    public byte[] getNextRegionStartKey(HRegionLocation regionLocation, byte[] currentKey,
        HRegionLocation prevRegionLocation) {
        // in order to check the overlap/inconsistencies bad region info, we have to make sure
        // the current endKey always increasing(compare the previous endKey)

        // conditionOne = true if the currentKey does not belong to the region boundaries specified
        // by regionLocation i.e. if the currentKey is less than the region startKey or if the
        // currentKey is greater than or equal to the region endKey.

        // conditionTwo = true if the previous region endKey is either not same as current region
        // startKey or if the previous region endKey is greater than or equal to current region
        // endKey.
        boolean conditionOne =
            (Bytes.compareTo(regionLocation.getRegion().getStartKey(), currentKey) > 0
                || Bytes.compareTo(regionLocation.getRegion().getEndKey(), currentKey) <= 0)
                && !Bytes.equals(currentKey, HConstants.EMPTY_START_ROW)
                && !Bytes.equals(regionLocation.getRegion().getEndKey(), HConstants.EMPTY_END_ROW);
        boolean conditionTwo = prevRegionLocation != null && (
            Bytes.compareTo(regionLocation.getRegion().getStartKey(),
                prevRegionLocation.getRegion().getEndKey()) != 0 ||
                Bytes.compareTo(regionLocation.getRegion().getEndKey(),
                    prevRegionLocation.getRegion().getEndKey()) <= 0)
            && !Bytes.equals(prevRegionLocation.getRegion().getEndKey(), HConstants.EMPTY_START_ROW)
            && !Bytes.equals(regionLocation.getRegion().getEndKey(), HConstants.EMPTY_END_ROW);
        if (conditionOne || conditionTwo) {
            GLOBAL_HBASE_COUNTER_METADATA_INCONSISTENCY.increment();
            LOGGER.warn(
                "HBase region overlap/inconsistencies on {} , current key: {} , region startKey:"
                    + " {} , region endKey: {} , prev region startKey: {} , prev region endKey: {}",
                regionLocation,
                Bytes.toStringBinary(currentKey),
                Bytes.toStringBinary(regionLocation.getRegion().getStartKey()),
                Bytes.toStringBinary(regionLocation.getRegion().getEndKey()),
                prevRegionLocation == null ?
                    "null" : Bytes.toStringBinary(prevRegionLocation.getRegion().getStartKey()),
                prevRegionLocation == null ?
                    "null" : Bytes.toStringBinary(prevRegionLocation.getRegion().getEndKey()));
        }
        return regionLocation.getRegion().getEndKey();
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<HRegionLocation> getAllTableRegions(byte[] tableName) throws SQLException {
        int queryTimeout = this.getProps().getInt(QueryServices.THREAD_TIMEOUT_MS_ATTRIB,
                QueryServicesOptions.DEFAULT_THREAD_TIMEOUT_MS);
        return getTableRegions(tableName, HConstants.EMPTY_START_ROW,
                HConstants.EMPTY_END_ROW, queryTimeout);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<HRegionLocation> getAllTableRegions(byte[] tableName, int queryTimeout)
            throws SQLException {
        return getTableRegions(tableName, HConstants.EMPTY_START_ROW,
                HConstants.EMPTY_END_ROW, queryTimeout);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<HRegionLocation> getTableRegions(byte[] tableName, byte[] startRowKey,
                                                 byte[] endRowKey) throws SQLException{
        int queryTimeout = this.getProps().getInt(QueryServices.THREAD_TIMEOUT_MS_ATTRIB,
                QueryServicesOptions.DEFAULT_THREAD_TIMEOUT_MS);
        return getTableRegions(tableName, startRowKey, endRowKey, queryTimeout);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<HRegionLocation> getTableRegions(final byte[] tableName, final byte[] startRowKey,
                                                 final byte[] endRowKey, final int queryTimeout)
            throws SQLException {
        /*
         * Use HConnection.getRegionLocation as it uses the cache in HConnection, while getting
         * all region locations from the HTable doesn't.
         */
        int retryCount = 0;
        int maxRetryCount =
            config.getInt(PHOENIX_GET_REGIONS_RETRIES, DEFAULT_PHOENIX_GET_REGIONS_RETRIES);
        TableName table = TableName.valueOf(tableName);
        byte[] currentKey = null;
        final long startTime = EnvironmentEdgeManager.currentTimeMillis();
        final long maxQueryEndTime = startTime + queryTimeout;
        while (true) {
            try {
                // We could surface the package projected HConnectionImplementation.getNumberOfCachedRegionLocations
                // to get the sizing info we need, but this would require a new class in the same package and a cast
                // to this implementation class, so it's probably not worth it.
                List<HRegionLocation> locations = Lists.newArrayList();
                HRegionLocation prevRegionLocation = null;
                currentKey = startRowKey;
                do {
                    HRegionLocation regionLocation =
                        ((ClusterConnection) connection).getRegionLocation(table,
                            currentKey, false);
                    currentKey =
                        getNextRegionStartKey(regionLocation, currentKey, prevRegionLocation);
                    locations.add(regionLocation);
                    prevRegionLocation = regionLocation;
                    if (!Bytes.equals(endRowKey, HConstants.EMPTY_END_ROW)
                        && Bytes.compareTo(currentKey, endRowKey) >= 0) {
                        break;
                    }
                    throwErrorIfQueryTimedOut(startRowKey, endRowKey, maxQueryEndTime,
                            queryTimeout, table, retryCount, currentKey);
                } while (!Bytes.equals(currentKey, HConstants.EMPTY_END_ROW));
                return locations;
            } catch (org.apache.hadoop.hbase.TableNotFoundException e) {
                throw new TableNotFoundException(table.getNameAsString());
            } catch (IOException e) {
                LOGGER.error("Exception encountered in getAllTableRegions for "
                        + "table: {}, retryCount: {} , currentKey: {} , startRowKey: {} ,"
                        + " endRowKey: {}",
                    table.getNameAsString(),
                    retryCount,
                    Bytes.toStringBinary(currentKey),
                    Bytes.toStringBinary(startRowKey),
                    Bytes.toStringBinary(endRowKey),
                    e);
                if (retryCount++ < maxRetryCount) {
                    continue;
                }
                throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.GET_TABLE_REGIONS_FAIL).setRootCause(e).build()
                    .buildException();
            }
        }
    }

    /**
     * Throw Error if the metadata lookup takes longer than query timeout configured.
     *
     * @param startRowKey Start RowKey to begin the region metadata lookup from.
     * @param endRowKey End RowKey to end the region metadata lookup at.
     * @param maxQueryEndTime Max time to execute the metadata lookup.
     * @param queryTimeout Query timeout.
     * @param table Table Name.
     * @param retryCount Retry Count.
     * @param currentKey Current Key.
     * @throws SQLException Throw Error if the metadata lookup takes longer than query timeout.
     */
    private static void throwErrorIfQueryTimedOut(byte[] startRowKey, byte[] endRowKey,
                                                  long maxQueryEndTime,
                                                  int queryTimeout, TableName table, int retryCount,
                                                  byte[] currentKey) throws SQLException {
        long currentTime = EnvironmentEdgeManager.currentTimeMillis();
        if (currentTime >= maxQueryEndTime) {
            LOGGER.error("getTableRegions has exceeded query timeout {} ms."
                            + "Table: {}, retryCount: {} , currentKey: {} , "
                            + "startRowKey: {} , endRowKey: {}",
                    queryTimeout,
                    table.getNameAsString(),
                    retryCount,
                    Bytes.toStringBinary(currentKey),
                    Bytes.toStringBinary(startRowKey),
                    Bytes.toStringBinary(endRowKey)
            );
            final String message = "getTableRegions has exceeded query timeout " + queryTimeout
                    + "ms";
            IOException e = new IOException(message);
            throw new SQLTimeoutException(message,
                    SQLExceptionCode.OPERATION_TIMED_OUT.getSQLState(),
                    SQLExceptionCode.OPERATION_TIMED_OUT.getErrorCode(), e);
        }
    }

    public PMetaData getMetaDataCache() {
        return latestMetaData;
    }

    @Override
    public int getConnectionCount(boolean isInternal) {
        if (isInternal) {
            return connectionLimiter.getInternalConnectionCount();
        } else {
            return connectionLimiter.getConnectionCount();
        }
    }

    @Override
    public void addTable(PTable table, long resolvedTime) throws SQLException {
        synchronized (latestMetaDataLock) {
            try {
                throwConnectionClosedIfNullMetaData();
                // If existing table isn't older than new table, don't replace
                // If a client opens a connection at an earlier timestamp, this can happen
                PTableRef existingTableRef = latestMetaData.getTableRef(new PTableKey(
                        table.getTenantId(), table.getName().getString()));
                PTable existingTable = existingTableRef.getTable();
                if (existingTable.getTimeStamp() > table.getTimeStamp()) {
                    return;
                }
            } catch (TableNotFoundException e) {}
            latestMetaData.addTable(table, resolvedTime);
            latestMetaDataLock.notifyAll();
        }
    }
    @Override
    public void updateResolvedTimestamp(PTable table, long resolvedTime) throws SQLException {
        synchronized (latestMetaDataLock) {
            throwConnectionClosedIfNullMetaData();
            latestMetaData.updateResolvedTimestamp(table, resolvedTime);
            latestMetaDataLock.notifyAll();
        }
    }

    private static interface Mutator {
        void mutate(PMetaData metaData) throws SQLException;
    }

    /**
     * Ensures that metaData mutations are handled in the correct order
     */
    private PMetaData metaDataMutated(PName tenantId, String tableName, long tableSeqNum, Mutator mutator) throws SQLException {
        synchronized (latestMetaDataLock) {
            throwConnectionClosedIfNullMetaData();
            PMetaData metaData = latestMetaData;
            PTable table;
            long endTime = EnvironmentEdgeManager.currentTimeMillis() +
                DEFAULT_OUT_OF_ORDER_MUTATIONS_WAIT_TIME_MS;
            while (true) {
                try {
                    try {
                        table = metaData.getTableRef(new PTableKey(tenantId, tableName)).getTable();
                        /* If the table is at the prior sequence number, then we're good to go.
                         * We know if we've got this far, that the server validated the mutations,
                         * so we'd just need to wait until the other connection that mutated the same
                         * table is processed.
                         */
                        if (table.getSequenceNumber() + 1 == tableSeqNum) {
                            // TODO: assert that timeStamp is bigger that table timeStamp?
                            mutator.mutate(metaData);
                            break;
                        } else if (table.getSequenceNumber() >= tableSeqNum) {
                            LOGGER.warn("Attempt to cache older version of " + tableName +
                                    ": current= " + table.getSequenceNumber() +
                                    ", new=" + tableSeqNum);
                            break;
                        }
                    } catch (TableNotFoundException e) {
                    }
                    long waitTime = endTime - EnvironmentEdgeManager.currentTimeMillis();
                    // We waited long enough - just remove the table from the cache
                    // and the next time it's used it'll be pulled over from the server.
                    if (waitTime <= 0) {
                        LOGGER.warn("Unable to update meta data repo within " +
                                (DEFAULT_OUT_OF_ORDER_MUTATIONS_WAIT_TIME_MS/1000) +
                                " seconds for " + tableName);
                        // There will never be a parentTableName here, as that would only
                        // be non null for an index an we never add/remove columns from an index.
                        metaData.removeTable(tenantId, tableName, null, HConstants.LATEST_TIMESTAMP);
                        break;
                    }
                    latestMetaDataLock.wait(waitTime);
                } catch (InterruptedException e) {
                    // restore the interrupt status
                    Thread.currentThread().interrupt();
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.INTERRUPTED_EXCEPTION)
                    .setRootCause(e).build().buildException(); // FIXME
                }
            }
            latestMetaData = metaData;
            latestMetaDataLock.notifyAll();
            return metaData;
        }
    }

    @Override
    public void removeTable(PName tenantId, final String tableName, String parentTableName, long tableTimeStamp) throws SQLException {
        synchronized (latestMetaDataLock) {
            throwConnectionClosedIfNullMetaData();
            latestMetaData.removeTable(tenantId, tableName, parentTableName, tableTimeStamp);
            latestMetaDataLock.notifyAll();
        }
    }

    @Override
    public void removeColumn(final PName tenantId, final String tableName, final List<PColumn> columnsToRemove, final long tableTimeStamp, final long tableSeqNum, final long resolvedTime) throws SQLException {
        metaDataMutated(tenantId, tableName, tableSeqNum, new Mutator() {
            @Override
            public void mutate(PMetaData metaData) throws SQLException {
                try {
                    metaData.removeColumn(tenantId, tableName, columnsToRemove, tableTimeStamp, tableSeqNum, resolvedTime);
                } catch (TableNotFoundException e) {
                    // The DROP TABLE may have been processed first, so just ignore.
                }
            }
        });
    }

    /**
     * Check that the supplied connection properties are set to valid values.
     * @param info The properties to be validated.
     * @throws IllegalArgumentException when a property is not set to a valid value.
     */
    private void validateConnectionProperties(Properties info) {
        if (info.get(DEFAULT_UPDATE_CACHE_FREQUENCY_ATRRIB) != null) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Connection's " + DEFAULT_UPDATE_CACHE_FREQUENCY_ATRRIB + " set to " +
                        info.get(DEFAULT_UPDATE_CACHE_FREQUENCY_ATRRIB));
            }
            ConnectionProperty.UPDATE_CACHE_FREQUENCY.getValue(
                    info.getProperty(DEFAULT_UPDATE_CACHE_FREQUENCY_ATRRIB));
        }
    }

    @Override
    public PhoenixConnection connect(String url, Properties info) throws SQLException {
        checkClosed();
        throwConnectionClosedIfNullMetaData();
        validateConnectionProperties(info);

        return new PhoenixConnection(this, url, info);
    }

    private ColumnFamilyDescriptor generateColumnFamilyDescriptor(Pair<byte[],Map<String,Object>> family, PTableType tableType) throws SQLException {
        ColumnFamilyDescriptorBuilder columnDescBuilder = ColumnFamilyDescriptorBuilder.newBuilder(family.getFirst());
        if (tableType != PTableType.VIEW) {
            columnDescBuilder.setDataBlockEncoding(SchemaUtil.DEFAULT_DATA_BLOCK_ENCODING);
            for (Entry<String,Object> entry : family.getSecond().entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                setHColumnDescriptorValue(columnDescBuilder, key, value);
            }
        }
        return columnDescBuilder.build();
    }

    // Workaround HBASE-14737
    private static void setHColumnDescriptorValue(ColumnFamilyDescriptorBuilder columnDescBuilder, String key, Object value) {
        if (HConstants.VERSIONS.equals(key)) {
            columnDescBuilder.setMaxVersions(getMaxVersion(value));
        } else {
            columnDescBuilder.setValue(key, value == null ? null : value.toString());
        }
    }

    private static int getMaxVersion(Object value) {
        if (value == null) {
            return -1;  // HColumnDescriptor.UNINITIALIZED is private
        }
        if (value instanceof Number) {
            return ((Number)value).intValue();
        }
        String stringValue = value.toString();
        if (stringValue.isEmpty()) {
            return -1;
        }
        return Integer.parseInt(stringValue);
    }

    private void modifyColumnFamilyDescriptor(ColumnFamilyDescriptorBuilder hcd, Map<String,Object> props) throws SQLException {
        for (Entry<String, Object> entry : props.entrySet()) {
            String propName = entry.getKey();
            Object value = entry.getValue();
            setHColumnDescriptorValue(hcd, propName, value);
        }
    }

    private TableDescriptorBuilder generateTableDescriptor(byte[] physicalTableName,  byte[] parentPhysicalTableName, TableDescriptor existingDesc,
            PTableType tableType, Map<String, Object> tableProps, List<Pair<byte[], Map<String, Object>>> families,
            byte[][] splits, boolean isNamespaceMapped) throws SQLException {
        String defaultFamilyName = (String)tableProps.remove(PhoenixDatabaseMetaData.DEFAULT_COLUMN_FAMILY_NAME);
        TableDescriptorBuilder tableDescriptorBuilder = (existingDesc != null) ?TableDescriptorBuilder.newBuilder(existingDesc)
        : TableDescriptorBuilder.newBuilder(TableName.valueOf(physicalTableName));

        ColumnFamilyDescriptor dataTableColDescForIndexTablePropSyncing = null;
        boolean doNotAddGlobalIndexChecker = false;
        if (tableType == PTableType.INDEX || MetaDataUtil.isViewIndex(Bytes.toString(physicalTableName))) {
            byte[] defaultFamilyBytes =
                    defaultFamilyName == null ? QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES : Bytes.toBytes(defaultFamilyName);

            final TableDescriptor baseTableDesc;
            if (MetaDataUtil.isViewIndex(Bytes.toString(physicalTableName))) {
                // Handles indexes created on views for single-tenant tables and
                // global indexes created on views of multi-tenant tables
                baseTableDesc = this.getTableDescriptor(parentPhysicalTableName);
            } else if (existingDesc == null) {
                // Global/local index creation on top of a physical base table
                baseTableDesc = this.getTableDescriptor(SchemaUtil.getPhysicalTableName(
                        Bytes.toBytes((String) tableProps.get(PhoenixDatabaseMetaData.DATA_TABLE_NAME)), isNamespaceMapped)
                        .getName());
            } else {
                // In case this a local index created on a view of a multi-tenant table, the
                // PHYSICAL_DATA_TABLE_NAME points to the name of the view instead of the physical base table
                baseTableDesc = existingDesc;
            }
            dataTableColDescForIndexTablePropSyncing = baseTableDesc.getColumnFamily(defaultFamilyBytes);
            // It's possible that the table has specific column families and none of them are declared
            // to be the DEFAULT_COLUMN_FAMILY, so we choose the first column family for syncing properties
            if (dataTableColDescForIndexTablePropSyncing == null) {
                dataTableColDescForIndexTablePropSyncing = baseTableDesc.getColumnFamilies()[0];
            }
            if (baseTableDesc.hasCoprocessor(QueryConstants.INDEXER_CLASSNAME)) {
                // The base table still uses the old indexing
                doNotAddGlobalIndexChecker = true;
            }
        }
        // By default, do not automatically rebuild/catch up an index on a write failure
        // Add table-specific properties to the table descriptor
        for (Entry<String,Object> entry : tableProps.entrySet()) {
            String key = entry.getKey();
            if (!TableProperty.isPhoenixTableProperty(key)) {
                Object value = entry.getValue();
                tableDescriptorBuilder.setValue(key, value == null ? null : value.toString());
            }
        }

        Map<String, Object> syncedProps = MetaDataUtil.getSyncedProps(dataTableColDescForIndexTablePropSyncing);
        // Add column family-specific properties to the table descriptor
        for (Pair<byte[],Map<String,Object>> family : families) {
            // If family is only in phoenix description, add it. otherwise, modify its property accordingly.
            byte[] familyByte = family.getFirst();
            if (tableDescriptorBuilder.build().getColumnFamily(familyByte) == null) {
                if (tableType == PTableType.VIEW) {
                    String fullTableName = Bytes.toString(physicalTableName);
                    throw new ReadOnlyTableException(
                            "The HBase column families for a read-only table must already exist",
                            SchemaUtil.getSchemaNameFromFullName(fullTableName),
                            SchemaUtil.getTableNameFromFullName(fullTableName),
                            Bytes.toString(familyByte));
                }

                ColumnFamilyDescriptor columnDescriptor = generateColumnFamilyDescriptor(family, tableType);
                // Keep certain index column family properties in sync with the base table
                if ((tableType == PTableType.INDEX || MetaDataUtil.isViewIndex(Bytes.toString(physicalTableName))) &&
                        (syncedProps != null && !syncedProps.isEmpty())) {
                    ColumnFamilyDescriptorBuilder colFamDescBuilder = ColumnFamilyDescriptorBuilder.newBuilder(columnDescriptor);
                    modifyColumnFamilyDescriptor(colFamDescBuilder, syncedProps);
                    columnDescriptor = colFamDescBuilder.build();
                }
                tableDescriptorBuilder.setColumnFamily(columnDescriptor);
            } else {
                if (tableType != PTableType.VIEW) {
                    ColumnFamilyDescriptor columnDescriptor = tableDescriptorBuilder.build().getColumnFamily(familyByte);
                    if (columnDescriptor == null) {
                        throw new IllegalArgumentException("Unable to find column descriptor with family name " + Bytes.toString(family.getFirst()));
                    }
                    ColumnFamilyDescriptorBuilder columnDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(columnDescriptor);
                    modifyColumnFamilyDescriptor(columnDescriptorBuilder, family.getSecond());
                    tableDescriptorBuilder.modifyColumnFamily(columnDescriptorBuilder.build());
                }
            }
        }
        addCoprocessors(physicalTableName, tableDescriptorBuilder,
            tableType, tableProps, existingDesc, doNotAddGlobalIndexChecker);

        // PHOENIX-3072: Set index priority if this is a system table or index table
        if (tableType == PTableType.SYSTEM) {
            tableDescriptorBuilder.setValue(QueryConstants.PRIORITY,
                    String.valueOf(IndexUtil.getMetadataPriority(config)));
        } else if (tableType == PTableType.INDEX // Global, mutable index
                && !isLocalIndexTable(tableDescriptorBuilder.build().getColumnFamilyNames())
                && !Boolean.TRUE.equals(tableProps.get(PhoenixDatabaseMetaData.IMMUTABLE_ROWS))) {
            tableDescriptorBuilder.setValue(QueryConstants.PRIORITY,
                    String.valueOf(IndexUtil.getIndexPriority(config)));
        }
        return tableDescriptorBuilder;
    }

    private boolean isLocalIndexTable(Collection<byte[]> families) {
        // no easier way to know local index table?
        for (byte[] family: families) {
            if (Bytes.toString(family).startsWith(QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX)) {
                return true;
            }
        }
        return false;
    }


    private void addCoprocessors(byte[] tableName, TableDescriptorBuilder builder,
            PTableType tableType, Map<String, Object> tableProps, TableDescriptor existingDesc,
            boolean doNotAddGlobalIndexChecker) throws SQLException {
        // The phoenix jar must be available on HBase classpath
        int priority = props.getInt(QueryServices.COPROCESSOR_PRIORITY_ATTRIB, QueryServicesOptions.DEFAULT_COPROCESSOR_PRIORITY);
        try {
            TableDescriptor newDesc = builder.build();
            TransactionFactory.Provider provider = getTransactionProvider(tableProps);
            boolean isTransactional = (provider != null);

            boolean indexRegionObserverEnabled = config.getBoolean(
                    QueryServices.INDEX_REGION_OBSERVER_ENABLED_ATTRIB,
                    QueryServicesOptions.DEFAULT_INDEX_REGION_OBSERVER_ENABLED);
            boolean isViewIndex = TRUE_BYTES_AS_STRING
                    .equals(tableProps.get(MetaDataUtil.IS_VIEW_INDEX_TABLE_PROP_NAME));
            boolean isServerSideMaskingEnabled = config.getBoolean(
                    QueryServices.PHOENIX_TTL_SERVER_SIDE_MASKING_ENABLED,
                    QueryServicesOptions.DEFAULT_SERVER_SIDE_MASKING_ENABLED);

            boolean isViewBaseTransactional = false;
            if (!isTransactional && isViewIndex) {
                if (tableProps.containsKey(TRANSACTIONAL) &&
                        Boolean.TRUE.equals(tableProps.get(TRANSACTIONAL))) {
                    isViewBaseTransactional = true;
                }
            }

            if (!isTransactional && !isViewBaseTransactional
                    && (tableType == PTableType.INDEX || isViewIndex)) {
                if (!indexRegionObserverEnabled && newDesc.hasCoprocessor(QueryConstants.GLOBAL_INDEX_CHECKER_CLASSNAME)) {
                    builder.removeCoprocessor(QueryConstants.GLOBAL_INDEX_CHECKER_CLASSNAME);
                } else if (indexRegionObserverEnabled && !newDesc.hasCoprocessor(QueryConstants.GLOBAL_INDEX_CHECKER_CLASSNAME) &&
                        !isLocalIndexTable(newDesc.getColumnFamilyNames())) {
                    if (newDesc.hasCoprocessor(QueryConstants.INDEX_REGION_OBSERVER_CLASSNAME)) {
                        builder.removeCoprocessor(QueryConstants.INDEX_REGION_OBSERVER_CLASSNAME);
                    }
                    if (!doNotAddGlobalIndexChecker) {
                        builder.setCoprocessor(CoprocessorDescriptorBuilder
                                .newBuilder(QueryConstants.GLOBAL_INDEX_CHECKER_CLASSNAME)
                                .setPriority(priority - 1).build());
                    }
                }
            }

            if (!newDesc.hasCoprocessor(QueryConstants.SCAN_REGION_OBSERVER_CLASSNAME)) {
                builder.setCoprocessor(CoprocessorDescriptorBuilder.newBuilder(QueryConstants.SCAN_REGION_OBSERVER_CLASSNAME)
                        .setPriority(priority).build());
            }
            if (!newDesc.hasCoprocessor(QueryConstants.UNGROUPED_AGGREGATE_REGION_OBSERVER_CLASSNAME)) {
                builder.setCoprocessor(
                        CoprocessorDescriptorBuilder.newBuilder(QueryConstants.UNGROUPED_AGGREGATE_REGION_OBSERVER_CLASSNAME)
                                .setPriority(priority).build());
            }
            if (!newDesc.hasCoprocessor(QueryConstants.GROUPED_AGGREGATE_REGION_OBSERVER_CLASSNAME)) {
                builder.setCoprocessor(
                        CoprocessorDescriptorBuilder.newBuilder(QueryConstants.GROUPED_AGGREGATE_REGION_OBSERVER_CLASSNAME)
                                .setPriority(priority).build());
            }
            if (!newDesc.hasCoprocessor(QueryConstants.SERVER_CACHING_ENDPOINT_IMPL_CLASSNAME)) {
                builder.setCoprocessor(
                        CoprocessorDescriptorBuilder.newBuilder(QueryConstants.SERVER_CACHING_ENDPOINT_IMPL_CLASSNAME)
                                .setPriority(priority).build());
            }

            // TODO: better encapsulation for this
            // Since indexes can't have indexes, don't install our indexing coprocessor for indexes.
            // Also don't install on the SYSTEM.CATALOG and SYSTEM.STATS table because we use
            // all-or-none mutate class which break when this coprocessor is installed (PHOENIX-1318).
            if ((tableType != PTableType.INDEX && tableType != PTableType.VIEW && !isViewIndex)
                    && !SchemaUtil.isMetaTable(tableName)
                    && !SchemaUtil.isStatsTable(tableName)) {
                if (isTransactional) {
                    if (!newDesc.hasCoprocessor(QueryConstants.PHOENIX_TRANSACTIONAL_INDEXER_CLASSNAME)) {
                        builder.setCoprocessor(
                                CoprocessorDescriptorBuilder.newBuilder(QueryConstants.PHOENIX_TRANSACTIONAL_INDEXER_CLASSNAME)
                                        .setPriority(priority).build());
                    }
                    // For alter table, remove non transactional index coprocessor
                    if (newDesc.hasCoprocessor(QueryConstants.INDEXER_CLASSNAME)) {
                        builder.removeCoprocessor(QueryConstants.INDEXER_CLASSNAME);
                    }
                    if (newDesc.hasCoprocessor(QueryConstants.INDEX_REGION_OBSERVER_CLASSNAME)) {
                        builder.removeCoprocessor(QueryConstants.INDEX_REGION_OBSERVER_CLASSNAME);
                    }
                } else {
                    // If exception on alter table to transition back to non transactional
                    if (newDesc.hasCoprocessor(QueryConstants.PHOENIX_TRANSACTIONAL_INDEXER_CLASSNAME)) {
                        builder.removeCoprocessor(QueryConstants.PHOENIX_TRANSACTIONAL_INDEXER_CLASSNAME);
                    }
                    // we only want to mess with the indexing coprocs if we're on the original
                    // CREATE statement. Otherwise, if we're on an ALTER or CREATE TABLE
                    // IF NOT EXISTS of an existing table, we should leave them unaltered,
                    // because they should be upgraded or downgraded using the IndexUpgradeTool
                    if (!doesPhoenixTableAlreadyExist(existingDesc)) {
                        if (indexRegionObserverEnabled) {
                            if (newDesc.hasCoprocessor(QueryConstants.INDEXER_CLASSNAME)) {
                                builder.removeCoprocessor(QueryConstants.INDEXER_CLASSNAME);
                            }
                            if (!newDesc.hasCoprocessor(QueryConstants.INDEX_REGION_OBSERVER_CLASSNAME)) {
                                Map<String, String> opts = Maps.newHashMapWithExpectedSize(1);
                                opts.put(IndexUtil.CODEC_CLASS_NAME_KEY,  PhoenixIndexCodec.class.getName());
                                IndexUtil.enableIndexing(builder, IndexUtil.PHOENIX_INDEX_BUILDER_CLASSNAME,
                                        opts, priority, QueryConstants.INDEX_REGION_OBSERVER_CLASSNAME);
                            }
                        } else {
                            if (newDesc.hasCoprocessor(QueryConstants.INDEX_REGION_OBSERVER_CLASSNAME)) {
                                builder.removeCoprocessor(QueryConstants.INDEX_REGION_OBSERVER_CLASSNAME);
                            }
                            if (!newDesc.hasCoprocessor(QueryConstants.INDEXER_CLASSNAME)) {
                                Map<String, String> opts = Maps.newHashMapWithExpectedSize(1);
                                opts.put(IndexUtil.CODEC_CLASS_NAME_KEY, PhoenixIndexCodec.class.getName());
                                IndexUtil.enableIndexing(builder, IndexUtil.PHOENIX_INDEX_BUILDER_CLASSNAME,
                                        opts, priority, QueryConstants.INDEXER_CLASSNAME);
                            }
                        }
                    }
                }
            }

            if ((SchemaUtil.isStatsTable(tableName) || SchemaUtil.isMetaTable(tableName))
                    && !newDesc.hasCoprocessor(QueryConstants.MULTI_ROW_MUTATION_ENDPOINT_CLASSNAME)) {
                builder.setCoprocessor(
                        CoprocessorDescriptorBuilder
                                .newBuilder(QueryConstants.MULTI_ROW_MUTATION_ENDPOINT_CLASSNAME)
                                .setPriority(priority)
                                .setProperties(Collections.emptyMap())
                                .build());
            }

            Set<byte[]> familiesKeys = builder.build().getColumnFamilyNames();
            for (byte[] family: familiesKeys) {
                if (Bytes.toString(family).startsWith(QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX)) {
                    if (!newDesc.hasCoprocessor(QueryConstants.INDEX_HALF_STORE_FILE_READER_GENERATOR_CLASSNAME)) {
                        builder.setCoprocessor(
                                CoprocessorDescriptorBuilder
                                        .newBuilder(QueryConstants.INDEX_HALF_STORE_FILE_READER_GENERATOR_CLASSNAME)
                                        .setPriority(priority)
                                        .setProperties(Collections.emptyMap())
                                        .build());
                        break;
                    }
                }
            }

            // Setup split policy on Phoenix metadata table to ensure that the key values of a Phoenix table
            // stay on the same region.
            if (SchemaUtil.isMetaTable(tableName) || SchemaUtil.isFunctionTable(tableName)) {
                if (!newDesc.hasCoprocessor(QueryConstants.META_DATA_ENDPOINT_IMPL_CLASSNAME)) {
                    builder.setCoprocessor(
                            CoprocessorDescriptorBuilder
                                    .newBuilder(QueryConstants.META_DATA_ENDPOINT_IMPL_CLASSNAME)
                                    .setPriority(priority)
                                    .setProperties(Collections.emptyMap())
                                    .build());
                }
                if (SchemaUtil.isMetaTable(tableName) ) {
                    if (!newDesc.hasCoprocessor(QueryConstants.META_DATA_REGION_OBSERVER_CLASSNAME)) {
                        builder.setCoprocessor(
                                CoprocessorDescriptorBuilder
                                        .newBuilder(QueryConstants.META_DATA_REGION_OBSERVER_CLASSNAME)
                                        .setPriority(priority + 1)
                                        .setProperties(Collections.emptyMap())
                                        .build());
                    }
                }
            } else if (SchemaUtil.isSequenceTable(tableName)) {
                if (!newDesc.hasCoprocessor(QueryConstants.SEQUENCE_REGION_OBSERVER_CLASSNAME)) {
                    builder.setCoprocessor(
                            CoprocessorDescriptorBuilder
                                    .newBuilder(QueryConstants.SEQUENCE_REGION_OBSERVER_CLASSNAME)
                                    .setPriority(priority)
                                    .setProperties(Collections.emptyMap())
                                    .build());
                }
            } else if (SchemaUtil.isTaskTable(tableName)) {
                if (!newDesc.hasCoprocessor(QueryConstants.TASK_REGION_OBSERVER_CLASSNAME)) {
                    builder.setCoprocessor(
                            CoprocessorDescriptorBuilder
                                    .newBuilder(QueryConstants.TASK_REGION_OBSERVER_CLASSNAME)
                                    .setPriority(priority)
                                    .setProperties(Collections.emptyMap())
                                    .build());
                }
                if (!newDesc.hasCoprocessor(QueryConstants.TASK_META_DATA_ENDPOINT_CLASSNAME)) {
                    builder.setCoprocessor(
                            CoprocessorDescriptorBuilder
                                    .newBuilder(QueryConstants.TASK_META_DATA_ENDPOINT_CLASSNAME)
                                    .setPriority(priority)
                                    .setProperties(Collections.emptyMap())
                                    .build());
                }
            } else if (SchemaUtil.isChildLinkTable(tableName)) {
                if (!newDesc.hasCoprocessor(QueryConstants.CHILD_LINK_META_DATA_ENDPOINT_CLASSNAME)) {
                    builder.setCoprocessor(
                            CoprocessorDescriptorBuilder
                                    .newBuilder(QueryConstants.CHILD_LINK_META_DATA_ENDPOINT_CLASSNAME)
                                    .setPriority(priority)
                                    .setProperties(Collections.emptyMap())
                                    .build());
                }
            }

            if (isTransactional) {
                String coprocessorClassName = provider.getTransactionProvider().getCoprocessorClassName();
                if (!newDesc.hasCoprocessor(coprocessorClassName)) {
                    builder.setCoprocessor(
                            CoprocessorDescriptorBuilder
                                    .newBuilder(coprocessorClassName)
                                    .setPriority(priority - 10)
                                    .setProperties(Collections.emptyMap())
                                    .build());
                }
                String coprocessorGCClassName = provider.getTransactionProvider().getGCCoprocessorClassName();
                if (coprocessorGCClassName != null) {
                    if (!newDesc.hasCoprocessor(coprocessorGCClassName)) {
                        builder.setCoprocessor(
                                CoprocessorDescriptorBuilder
                                        .newBuilder(coprocessorGCClassName)
                                        .setPriority(priority - 10)
                                        .setProperties(Collections.emptyMap())
                                        .build());
                    }
                }
            } else {
                // Remove all potential transactional coprocessors
                for (TransactionFactory.Provider aprovider : TransactionFactory.Provider.available()) {
                    String coprocessorClassName = aprovider.getTransactionProvider().getCoprocessorClassName();
                    String coprocessorGCClassName = aprovider.getTransactionProvider().getGCCoprocessorClassName();
                    if (coprocessorClassName != null && newDesc.hasCoprocessor(coprocessorClassName)) {
                        builder.removeCoprocessor(coprocessorClassName);
                    }
                    if (coprocessorGCClassName != null && newDesc.hasCoprocessor(coprocessorGCClassName)) {
                        builder.removeCoprocessor(coprocessorGCClassName);
                    }
                }
            }

            // The priority for this co-processor should be set higher than the GlobalIndexChecker so that the read repair scans
            // are intercepted by the TTLAwareRegionObserver and only the rows that are not ttl-expired are returned.
            if (!SchemaUtil.isSystemTable(tableName)) {
                if (!newDesc.hasCoprocessor(QueryConstants.PHOENIX_TTL_REGION_OBSERVER_CLASSNAME) &&
                        isServerSideMaskingEnabled) {
                        builder.setCoprocessor(
                            CoprocessorDescriptorBuilder
                                    .newBuilder(QueryConstants.PHOENIX_TTL_REGION_OBSERVER_CLASSNAME)
                                    .setPriority(priority - 2)
                                    .setProperties(Collections.emptyMap())
                                    .build());
                }
            }
            if (Arrays.equals(tableName, SchemaUtil.getPhysicalName(SYSTEM_CATALOG_NAME_BYTES, props).getName())) {
                if (!newDesc.hasCoprocessor(QueryConstants.SYSTEM_CATALOG_REGION_OBSERVER_CLASSNAME)) {
                    builder.setCoprocessor(
                            CoprocessorDescriptorBuilder
                                    .newBuilder(QueryConstants.SYSTEM_CATALOG_REGION_OBSERVER_CLASSNAME)
                                    .setPriority(priority)
                                    .setProperties(Collections.emptyMap())
                                    .build());
                }
            }

        } catch (IOException e) {
            throw ClientUtil.parseServerException(e);
        }
    }

    private TransactionFactory.Provider getTransactionProvider(Map<String,Object> tableProps) {
        TransactionFactory.Provider provider = (TransactionFactory.Provider)TableProperty.TRANSACTION_PROVIDER.getValue(tableProps);
        return provider;
    }

    private boolean doesPhoenixTableAlreadyExist(TableDescriptor existingDesc) {
        //if the table descriptor already has Phoenix coprocs, we assume it's
        //already gone through a Phoenix create statement once
        if (existingDesc == null){
            return false;
        }
        boolean hasScanObserver = existingDesc.hasCoprocessor(QueryConstants.SCAN_REGION_OBSERVER_CLASSNAME);
        boolean hasUnAggObserver = existingDesc.hasCoprocessor(
                QueryConstants.UNGROUPED_AGGREGATE_REGION_OBSERVER_CLASSNAME);
        boolean hasGroupedObserver = existingDesc.hasCoprocessor(
            QueryConstants.GROUPED_AGGREGATE_REGION_OBSERVER_CLASSNAME);
        boolean hasIndexObserver = existingDesc.hasCoprocessor(QueryConstants.INDEXER_CLASSNAME)
            || existingDesc.hasCoprocessor(QueryConstants.INDEX_REGION_OBSERVER_CLASSNAME)
            || existingDesc.hasCoprocessor(QueryConstants.GLOBAL_INDEX_CHECKER_CLASSNAME);
        return hasScanObserver && hasUnAggObserver && hasGroupedObserver && hasIndexObserver;
    }

    private static interface RetriableOperation {
        boolean checkForCompletion() throws TimeoutException, IOException;
        String getOperationName();
    }

    private void pollForUpdatedTableDescriptor(final Admin admin, final TableDescriptor newTableDescriptor,
            final byte[] tableName) throws InterruptedException, TimeoutException {
        checkAndRetry(new RetriableOperation() {

            @Override
            public String getOperationName() {
                return "UpdateOrNewTableDescriptor";
            }

            @Override
            public boolean checkForCompletion() throws TimeoutException, IOException {
                TableDescriptor tableDesc = admin.getDescriptor(TableName.valueOf(tableName));
                return newTableDescriptor.equals(tableDesc);
            }
        });
    }

    private void checkAndRetry(RetriableOperation op) throws InterruptedException, TimeoutException {
        int maxRetries = ConnectionQueryServicesImpl.this.props.getInt(
                QueryServices.NUM_RETRIES_FOR_SCHEMA_UPDATE_CHECK,
                QueryServicesOptions.DEFAULT_RETRIES_FOR_SCHEMA_UPDATE_CHECK);
        long sleepInterval = ConnectionQueryServicesImpl.this.props
                .getLong(QueryServices.DELAY_FOR_SCHEMA_UPDATE_CHECK,
                        QueryServicesOptions.DEFAULT_DELAY_FOR_SCHEMA_UPDATE_CHECK);
        boolean success = false;
        int numTries = 1;
        PhoenixStopWatch watch = new PhoenixStopWatch();
        watch.start();
        do {
            try {
                success = op.checkForCompletion();
            } catch (Exception ex) {
                // If we encounter any exception on the first or last try, propagate the exception and fail.
                // Else, we swallow the exception and retry till we reach maxRetries.
                if (numTries == 1 || numTries == maxRetries) {
                    watch.stop();
                    TimeoutException toThrow = new TimeoutException("Operation " + op.getOperationName()
                            + " didn't complete because of exception. Time elapsed: " + watch.elapsedMillis());
                    toThrow.initCause(ex);
                    throw toThrow;
                }
            }
            numTries++;
            Thread.sleep(sleepInterval);
        } while (numTries < maxRetries && !success);

        watch.stop();

        if (!success) {
            throw new TimeoutException("Operation  " + op.getOperationName() + " didn't complete within "
                    + watch.elapsedMillis() + " ms "
                    + "after trying " + numTries + "times.");
        } else {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Operation "
                        + op.getOperationName()
                        + " completed within "
                        + watch.elapsedMillis()
                        + "ms "
                        + "after trying " + numTries +  " times." );
            }
        }
    }

    private boolean allowOnlineTableSchemaUpdate() {
        return props.getBoolean(
                QueryServices.ALLOW_ONLINE_TABLE_SCHEMA_UPDATE,
                QueryServicesOptions.DEFAULT_ALLOW_ONLINE_TABLE_SCHEMA_UPDATE);
    }

    /**
     * Ensure that the HBase namespace is created/exists already
     * @param schemaName Phoenix schema name for which we ensure existence of the HBase namespace
     * @return true if we created the HBase namespace because it didn't already exist
     * @throws SQLException If there is an exception creating the HBase namespace
     */
    boolean ensureNamespaceCreated(String schemaName) throws SQLException {
        SQLException sqlE = null;
        boolean createdNamespace = false;
        try (Admin admin = getAdmin()) {
            if (!ClientUtil.isHBaseNamespaceAvailable(admin, schemaName)) {
                NamespaceDescriptor namespaceDescriptor =
                    NamespaceDescriptor.create(schemaName).build();
                admin.createNamespace(namespaceDescriptor);
                createdNamespace = true;
            }
        } catch (IOException e) {
            sqlE = ClientUtil.parseServerException(e);
        } finally {
            if (sqlE != null) { throw sqlE; }
        }
        return createdNamespace;
    }

    /**
     *
     * @param physicalTableName
     * @param tableType
     * @param props
     * @param families
     * @param splits
     * @param modifyExistingMetaData
     * @param isNamespaceMapped
     * @param isDoNotUpgradePropSet
     * @return true if table was created and false if it already exists
     * @throws SQLException
     */

    private TableDescriptor ensureTableCreated(byte[] physicalTableName, byte[] parentPhysicalTableName, PTableType tableType, Map<String, Object> props,
            List<Pair<byte[], Map<String, Object>>> families, byte[][] splits, boolean modifyExistingMetaData,
            boolean isNamespaceMapped, boolean isDoNotUpgradePropSet) throws SQLException {
        SQLException sqlE = null;
        TableDescriptor existingDesc = null;
        boolean isMetaTable = SchemaUtil.isMetaTable(physicalTableName);
        boolean tableExist = true;
        try (Admin admin = getAdmin()) {
            final String quorum = ZKConfig.getZKQuorumServersString(config);
            final String znode = this.getProps().get(HConstants.ZOOKEEPER_ZNODE_PARENT);
            boolean createdNamespace = false;
            LOGGER.debug("Found quorum: " + quorum + ":" + znode);

            if (isMetaTable) {
                if (SchemaUtil.isNamespaceMappingEnabled(PTableType.SYSTEM, this.getProps())) {
                    try {
                        // SYSTEM namespace needs to be created via HBase APIs because "CREATE SCHEMA" statement tries to write
                        // its metadata in SYSTEM:CATALOG table. Without SYSTEM namespace, SYSTEM:CATALOG table cannot be created
                        createdNamespace = ensureNamespaceCreated(QueryConstants.SYSTEM_SCHEMA_NAME);
                    } catch (PhoenixIOException e) {
                        // We could either:
                        // 1) Not access the NS descriptor. The NS may or may not exist at this point
                        // 2) We could not create the NS
                        // Regardless of the case 1 or 2, if we eventually try to migrate SYSTEM tables to the SYSTEM
                        // namespace using the {@link ensureSystemTablesMigratedToSystemNamespace(ReadOnlyProps)} method,
                        // if the NS does not exist, we will error as expected, or
                        // if the NS does exist and tables are already mapped, the check will exit gracefully
                    }
                    if (AdminUtilWithFallback.tableExists(admin,
                        SchemaUtil.getPhysicalTableName(SYSTEM_CATALOG_NAME_BYTES, false))) {
                        // SYSTEM.CATALOG exists, so at this point, we have 3 cases:
                        // 1) If server-side namespace mapping is disabled, drop the SYSTEM namespace if it was created
                        //    above and throw Inconsistent namespace mapping exception
                        // 2) If server-side namespace mapping is enabled and SYSTEM.CATALOG needs to be upgraded,
                        //    upgrade SYSTEM.CATALOG and also migrate SYSTEM tables to the SYSTEM namespace
                        // 3. If server-side namespace mapping is enabled and SYSTEM.CATALOG doesn't need to be
                        //    upgraded, we still need to migrate SYSTEM tables to the SYSTEM namespace using the
                        //    {@link ensureSystemTablesMigratedToSystemNamespace(ReadOnlyProps)} method (as part of
                        //    {@link upgradeSystemTables(String, Properties)})
                        try {
                            checkClientServerCompatibility(SYSTEM_CATALOG_NAME_BYTES);
                        } catch (SQLException possibleCompatException) {
                            // Handles Case 1: Drop the SYSTEM namespace in case it was created above
                            if (createdNamespace && possibleCompatException.getErrorCode() ==
                                    SQLExceptionCode.INCONSISTENT_NAMESPACE_MAPPING_PROPERTIES.getErrorCode()) {
                                ensureNamespaceDropped(QueryConstants.SYSTEM_SCHEMA_NAME);
                            }
                            // rethrow the SQLException
                            throw possibleCompatException;
                        }
                        // Thrown so we can force an upgrade which will just migrate SYSTEM tables to the SYSTEM namespace
                        throw new UpgradeRequiredException(MIN_SYSTEM_TABLE_TIMESTAMP);
                    }
                } else if (AdminUtilWithFallback.tableExists(admin,
                    SchemaUtil.getPhysicalTableName(SYSTEM_CATALOG_NAME_BYTES, true))) {
                    // If SYSTEM:CATALOG exists, but client-side namespace mapping for SYSTEM tables is disabled, throw an exception
                    throw new SQLExceptionInfo.Builder(
                      SQLExceptionCode.INCONSISTENT_NAMESPACE_MAPPING_PROPERTIES)
                      .setMessage("Cannot initiate connection as "
                        + SchemaUtil.getPhysicalTableName(SYSTEM_CATALOG_NAME_BYTES, true)
                        + " is found but client does not have "
                        + IS_NAMESPACE_MAPPING_ENABLED + " enabled")
                      .build().buildException();
                }
                // If DoNotUpgrade config is set only check namespace mapping and
                // Client-server compatibility for system tables.
                if (isDoNotUpgradePropSet) {
                    try {
                        checkClientServerCompatibility(SchemaUtil.getPhysicalName(
                                SYSTEM_CATALOG_NAME_BYTES, this.getProps()).getName());
                    } catch (SQLException possibleCompatException) {
                        if (possibleCompatException.getCause()
                                instanceof org.apache.hadoop.hbase.TableNotFoundException) {
                            throw new UpgradeRequiredException(MIN_SYSTEM_TABLE_TIMESTAMP);
                        }
                        throw possibleCompatException;
                    }
                    return null;
                }
            }

            try {
                existingDesc = admin.getDescriptor(TableName.valueOf(physicalTableName));
            } catch (org.apache.hadoop.hbase.TableNotFoundException e) {
                tableExist = false;
                if (tableType == PTableType.VIEW) {
                    String fullTableName = Bytes.toString(physicalTableName);
                    throw new ReadOnlyTableException(
                            "An HBase table for a VIEW must already exist",
                            SchemaUtil.getSchemaNameFromFullName(fullTableName),
                            SchemaUtil.getTableNameFromFullName(fullTableName));
                }
            }

            TableDescriptorBuilder newDesc = generateTableDescriptor(physicalTableName, parentPhysicalTableName, existingDesc, tableType, props, families,
                    splits, isNamespaceMapped);

            if (!tableExist) {
                if (SchemaUtil.isSystemTable(physicalTableName) && !isUpgradeRequired() && (!isAutoUpgradeEnabled || isDoNotUpgradePropSet)) {
                    // Disallow creating the SYSTEM.CATALOG or SYSTEM:CATALOG HBase table
                    throw new UpgradeRequiredException();
                }
                if (newDesc.build().getValue(MetaDataUtil.IS_LOCAL_INDEX_TABLE_PROP_BYTES) != null && Boolean.TRUE.equals(
                        PBoolean.INSTANCE.toObject(newDesc.build().getValue(MetaDataUtil.IS_LOCAL_INDEX_TABLE_PROP_BYTES)))) {
                    newDesc.setRegionSplitPolicyClassName(QueryConstants.INDEX_REGION_SPLIT_POLICY_CLASSNAME);
                }
                try {
                    if (splits == null) {
                        admin.createTable(newDesc.build());
                    } else {
                        admin.createTable(newDesc.build(), splits);
                    }
                } catch (TableExistsException e) {
                    // We can ignore this, as it just means that another client beat us
                    // to creating the HBase metadata.
                    if (isMetaTable && !isUpgradeRequired()) {
                        checkClientServerCompatibility(SchemaUtil.getPhysicalName(SYSTEM_CATALOG_NAME_BYTES, this.getProps()).getName());
                    }
                    return null;
                }
                if (isMetaTable && !isUpgradeRequired()) {
                    try {
                        checkClientServerCompatibility(SchemaUtil.getPhysicalName(SYSTEM_CATALOG_NAME_BYTES,
                                this.getProps()).getName());
                    } catch (SQLException possibleCompatException) {
                        if (possibleCompatException.getErrorCode() ==
                                SQLExceptionCode.INCONSISTENT_NAMESPACE_MAPPING_PROPERTIES.getErrorCode()) {
                            try {
                                // In case we wrongly created SYSTEM.CATALOG or SYSTEM:CATALOG, we should drop it
                                admin.disableTable(TableName.valueOf(physicalTableName));
                                admin.deleteTable(TableName.valueOf(physicalTableName));
                            } catch (org.apache.hadoop.hbase.TableNotFoundException ignored) {
                                // Ignore this since it just means that another client with a similar set of
                                // incompatible configs and conditions beat us to dropping the SYSCAT HBase table
                            }
                            if (createdNamespace &&
                                    SchemaUtil.isNamespaceMappingEnabled(PTableType.SYSTEM, this.getProps())) {
                                // We should drop the SYSTEM namespace which we just created, since
                                // server-side namespace mapping is disabled
                                ensureNamespaceDropped(QueryConstants.SYSTEM_SCHEMA_NAME);
                            }
                        }
                        // rethrow the SQLException
                        throw possibleCompatException;
                    }

                }
                return null;
            } else {
                if (isMetaTable && !isUpgradeRequired()) {
                    checkClientServerCompatibility(SchemaUtil.getPhysicalName(SYSTEM_CATALOG_NAME_BYTES, this.getProps()).getName());
                } else {
                    for (Pair<byte[],Map<String,Object>> family: families) {
                        if ((Bytes.toString(family.getFirst())
                                .startsWith(QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX))) {
                            newDesc.setRegionSplitPolicyClassName(QueryConstants.INDEX_REGION_SPLIT_POLICY_CLASSNAME);
                            break;
                        }
                    }
                }

                if (!modifyExistingMetaData) {
                    return existingDesc; // Caller already knows that no metadata was changed
                }
                TransactionFactory.Provider provider = getTransactionProvider(props);
                boolean willBeTx = provider != null;
                // If mapping an existing table as transactional, set property so that existing
                // data is correctly read.
                if (willBeTx) {
                    if (!equalTxCoprocessor(provider, existingDesc, newDesc.build())) {
                        // Cannot switch between different providers
                        if (hasTxCoprocessor(existingDesc)) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_SWITCH_TXN_PROVIDERS)
                            .setSchemaName(SchemaUtil.getSchemaNameFromFullName(physicalTableName))
                            .setTableName(SchemaUtil.getTableNameFromFullName(physicalTableName)).build().buildException();
                        }
                        if (provider.getTransactionProvider().isUnsupported(PhoenixTransactionProvider.Feature.ALTER_NONTX_TO_TX)) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_ALTER_TABLE_FROM_NON_TXN_TO_TXNL)
                            .setMessage(provider.name())
                            .setSchemaName(SchemaUtil.getSchemaNameFromFullName(physicalTableName))
                            .setTableName(SchemaUtil.getTableNameFromFullName(physicalTableName)).build().buildException();
                        }
                        newDesc.setValue(PhoenixTransactionContext.READ_NON_TX_DATA, Boolean.TRUE.toString());
                    }
                } else {
                    // If we think we're creating a non transactional table when it's already
                    // transactional, don't allow.
                    if (hasTxCoprocessor(existingDesc)) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.TX_MAY_NOT_SWITCH_TO_NON_TX)
                        .setSchemaName(SchemaUtil.getSchemaNameFromFullName(physicalTableName))
                        .setTableName(SchemaUtil.getTableNameFromFullName(physicalTableName)).build().buildException();
                    }
                    newDesc.removeValue(Bytes.toBytes(PhoenixTransactionContext.READ_NON_TX_DATA));
                }
                TableDescriptor result = newDesc.build();
                if (existingDesc.equals(result)) {
                    return null; // Indicate that no metadata was changed
                }

                // Do not call modifyTable for SYSTEM tables
                if (tableType != PTableType.SYSTEM) {
                    modifyTable(physicalTableName, newDesc.build(), true);
                }
                return result;
            }

        } catch (IOException e) {
            sqlE = ClientUtil.parseServerException(e);
        } catch (InterruptedException e) {
            // restore the interrupt status
            Thread.currentThread().interrupt();
            sqlE = new SQLExceptionInfo.Builder(SQLExceptionCode.INTERRUPTED_EXCEPTION).setRootCause(e).build().buildException();
        } catch (TimeoutException e) {
            sqlE = new SQLExceptionInfo.Builder(SQLExceptionCode.OPERATION_TIMED_OUT).setRootCause(e.getCause() != null ? e.getCause() : e).build().buildException();
        } finally {
            if (sqlE != null) {
                throw sqlE;
            }
        }
        return null; // will never make it here
    }

    /**
     * If given TableDescriptorBuilder belongs to SYSTEM.TASK and if the table
     * still does not have split policy setup as SystemTaskSplitPolicy, set
     * it up and return true, else return false. This method is expected
     * to return true only if it updated split policy (which should happen
     * once during initial upgrade).
     *
     * @param tdBuilder table descriptor builder
     * @return return true if split policy of SYSTEM.TASK is updated to
     *     SystemTaskSplitPolicy.
     * @throws SQLException If SYSTEM.TASK already has custom split policy
     *     set up other than SystemTaskSplitPolicy
     */
    @VisibleForTesting
    public boolean updateAndConfirmSplitPolicyForTask(
            final TableDescriptorBuilder tdBuilder) throws SQLException {
        boolean isTaskTable = false;
        TableName sysTaskTable = SchemaUtil
            .getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_TASK_NAME,
                props);
        if (tdBuilder.build().getTableName().equals(sysTaskTable)) {
            isTaskTable = true;
        }
        if (isTaskTable) {
            final String actualSplitPolicy = tdBuilder.build()
                .getRegionSplitPolicyClassName();
            final String targetSplitPolicy = QueryConstants.SYSTEM_TASK_SPLIT_POLICY_CLASSNAME;
            if (!targetSplitPolicy.equals(actualSplitPolicy)) {
                if (StringUtils.isNotEmpty(actualSplitPolicy)) {
                    // Rare possibility. pre-4.16 create DDL query
                    // doesn't have any split policy setup for SYSTEM.TASK
                    throw new InvalidRegionSplitPolicyException(
                        QueryConstants.SYSTEM_SCHEMA_NAME, SYSTEM_TASK_TABLE,
                        ImmutableList.of("null", targetSplitPolicy),
                        actualSplitPolicy);
                }
                tdBuilder.setRegionSplitPolicyClassName(targetSplitPolicy);
                return true;
            }
        }
        return false;
    }

    private static boolean hasTxCoprocessor(TableDescriptor descriptor) {
        for (TransactionFactory.Provider provider : TransactionFactory.Provider.available()) {
            String coprocessorClassName = provider.getTransactionProvider().getCoprocessorClassName();
            if (coprocessorClassName != null && descriptor.hasCoprocessor(coprocessorClassName)) {
                return true;
            }
        }
        return false;
    }

    private static boolean equalTxCoprocessor(TransactionFactory.Provider provider, TableDescriptor existingDesc, TableDescriptor newDesc) {
        String coprocessorClassName = provider.getTransactionProvider().getCoprocessorClassName();
        return (coprocessorClassName != null && existingDesc.hasCoprocessor(coprocessorClassName) && newDesc.hasCoprocessor(coprocessorClassName));
}

    private void modifyTable(byte[] tableName, TableDescriptor newDesc, boolean shouldPoll) throws IOException,
    InterruptedException, TimeoutException, SQLException {
        TableName tn = TableName.valueOf(tableName);
        try (Admin admin = getAdmin()) {
            if (!allowOnlineTableSchemaUpdate()) {
                admin.disableTable(tn);
                admin.modifyTable(newDesc); // TODO: Update to TableDescriptor
                admin.enableTable(tn);
            } else {
                admin.modifyTable(newDesc); // TODO: Update to TableDescriptor
                if (shouldPoll) {
                    pollForUpdatedTableDescriptor(admin, newDesc, tableName);
                }
            }
        }
    }

    private static boolean hasIndexWALCodec(Long serverVersion) {
        if (serverVersion == null) {
            return true;
        }
        return MetaDataUtil.decodeHasIndexWALCodec(serverVersion);
    }


    private void checkClientServerCompatibility(byte[] metaTable) throws SQLException,
            AccessDeniedException {
        StringBuilder errorMessage = new StringBuilder();
        int minHBaseVersion = Integer.MAX_VALUE;
        boolean isTableNamespaceMappingEnabled = false;
        long systemCatalogTimestamp = Long.MAX_VALUE;
        long startTime = 0L;
        long systemCatalogRpcTime;
        Map<byte[], GetVersionResponse> results;
        Table ht = null;

        try {
            try {
                startTime = EnvironmentEdgeManager.currentTimeMillis();
                ht = this.getTable(metaTable);
                final byte[] tableKey = PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES;
                results =
                        ht.coprocessorService(MetaDataService.class, tableKey, tableKey,
                                new Batch.Call<MetaDataService, GetVersionResponse>() {
                                    @Override
                                    public GetVersionResponse call(MetaDataService instance)
                                            throws IOException {
                                        RpcController controller = getController();
                                        BlockingRpcCallback<GetVersionResponse> rpcCallback =
                                                new BlockingRpcCallback<>();
                                        GetVersionRequest.Builder builder =
                                                GetVersionRequest.newBuilder();
                                        builder.setClientVersion(
                                                VersionUtil.encodeVersion(PHOENIX_MAJOR_VERSION,
                                                        PHOENIX_MINOR_VERSION, PHOENIX_PATCH_NUMBER));
                                        instance.getVersion(controller, builder.build(), rpcCallback);
                                        checkForRemoteExceptions(controller);
                                        return rpcCallback.get();
                                    }
                                });
                TableMetricsManager.updateMetricsForSystemCatalogTableMethod(null, NUM_SYSTEM_TABLE_RPC_SUCCESS, 1);
            } catch (Throwable e) {
                TableMetricsManager.updateMetricsForSystemCatalogTableMethod(null, NUM_SYSTEM_TABLE_RPC_FAILURES, 1);
                throw ClientUtil.parseServerException(e);
            } finally {
                systemCatalogRpcTime = EnvironmentEdgeManager.currentTimeMillis() - startTime;
                TableMetricsManager.updateMetricsForSystemCatalogTableMethod(null,
                        TIME_SPENT_IN_SYSTEM_TABLE_RPC_CALLS, systemCatalogRpcTime);
            }
            for (Map.Entry<byte[],GetVersionResponse> result : results.entrySet()) {
                // This is the "phoenix.jar" is in-place, but server is out-of-sync with client case.
                GetVersionResponse versionResponse = result.getValue();
                long serverJarVersion = versionResponse.getVersion();
                isTableNamespaceMappingEnabled |= MetaDataUtil.decodeTableNamespaceMappingEnabled(serverJarVersion);

                MetaDataUtil.ClientServerCompatibility compatibility = MetaDataUtil.areClientAndServerCompatible(serverJarVersion);
                if (!compatibility.getIsCompatible()) {
                    if (compatibility.getErrorCode() == SQLExceptionCode.OUTDATED_JARS.getErrorCode()) {
                        errorMessage.append("Newer Phoenix clients can't communicate with older "
                                + "Phoenix servers. Client version: "
                                + MetaDataProtocol.CURRENT_CLIENT_VERSION
                                + "; Server version: "
                                + getServerVersion(serverJarVersion)
                                + " The following servers require an updated "
                                + QueryConstants.DEFAULT_COPROCESS_JAR_NAME
                                + " to be put in the classpath of HBase.");
                    } else if (compatibility.getErrorCode() == SQLExceptionCode.INCOMPATIBLE_CLIENT_SERVER_JAR.getErrorCode()) {
                        errorMessage.append("Major version of client is less than that of the server. Client version: "
                                + MetaDataProtocol.CURRENT_CLIENT_VERSION
                                + "; Server version: "
                                + getServerVersion(serverJarVersion));
                    }
                }
                hasIndexWALCodec = hasIndexWALCodec && hasIndexWALCodec(serverJarVersion);
                if (minHBaseVersion > MetaDataUtil.decodeHBaseVersion(serverJarVersion)) {
                    minHBaseVersion = MetaDataUtil.decodeHBaseVersion(serverJarVersion);
                }
                // In case this is the first time connecting to this cluster, the system catalog table does not have an
                // entry for itself yet, so we cannot get the timestamp and this will not be returned from the
                // GetVersionResponse message object
                if (versionResponse.hasSystemCatalogTimestamp()) {
                    systemCatalogTimestamp = systemCatalogTimestamp < versionResponse.getSystemCatalogTimestamp() ?
                      systemCatalogTimestamp: versionResponse.getSystemCatalogTimestamp();
                }

                if (compatibility.getErrorCode() != 0) {
                    if (compatibility.getErrorCode() == SQLExceptionCode.OUTDATED_JARS.getErrorCode()) {
                        errorMessage.setLength(errorMessage.length()-1);
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.OUTDATED_JARS).setMessage(errorMessage.toString()).build().buildException();
                    } else if (compatibility.getErrorCode() == SQLExceptionCode.INCOMPATIBLE_CLIENT_SERVER_JAR.getErrorCode()) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.INCOMPATIBLE_CLIENT_SERVER_JAR).setMessage(errorMessage.toString()).build().buildException();
                    }
                }
            }
            if (isTableNamespaceMappingEnabled != SchemaUtil.isNamespaceMappingEnabled(PTableType.TABLE,
                    getProps())) { throw new SQLExceptionInfo.Builder(
                            SQLExceptionCode.INCONSISTENT_NAMESPACE_MAPPING_PROPERTIES)
                    .setMessage(
                            "Ensure that config " + QueryServices.IS_NAMESPACE_MAPPING_ENABLED
                            + " is consistent on client and server.")
                            .build().buildException(); }
            lowestClusterHBaseVersion = minHBaseVersion;
        } finally {
            if (ht != null) {
                try {
                    ht.close();
                } catch (IOException e) {
                    LOGGER.warn("Could not close HTable", e);
                }
            }
        }

        if (systemCatalogTimestamp < MIN_SYSTEM_TABLE_TIMESTAMP) {
            throw new UpgradeRequiredException(systemCatalogTimestamp);
        }
    }

    private String getServerVersion(long serverJarVersion) {
        return (VersionUtil.decodeMajorVersion(MetaDataUtil.decodePhoenixVersion(serverJarVersion)) + "."
                + VersionUtil.decodeMinorVersion(MetaDataUtil.decodePhoenixVersion(serverJarVersion)) + "."
                + VersionUtil.decodePatchVersion(MetaDataUtil.decodePhoenixVersion(serverJarVersion)));
    }

    /**
     * Invoke the SYSTEM.CHILD_LINK metadata coprocessor endpoint
     * @param parentTableKey key corresponding to the parent of the view
     * @param callable used to invoke the coprocessor endpoint to write links from a parent to its child view
     * @return result of invoking the coprocessor endpoint
     * @throws SQLException
     */
    private MetaDataMutationResult childLinkMetaDataCoprocessorExec(byte[] parentTableKey,
            Batch.Call<ChildLinkMetaDataService, MetaDataResponse> callable) throws SQLException {
        try (Table htable = this.getTable(SchemaUtil.getPhysicalName(
                PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME_BYTES, this.getProps()).getName()))
        {
            final Map<byte[], MetaDataResponse> results =
                    htable.coprocessorService(ChildLinkMetaDataService.class, parentTableKey, parentTableKey, callable);
            assert(results.size() == 1);
            MetaDataResponse result = results.values().iterator().next();
            return MetaDataMutationResult.constructFromProto(result);
        } catch (IOException e) {
            throw ClientUtil.parseServerException(e);
        } catch (Throwable t) {
            throw new SQLException(t);
        }
    }

    @VisibleForTesting
    protected RpcController getController() {
        return getController(SYSTEM_CATALOG_HBASE_TABLE_NAME);
    }

        /**
         * If configured to use the server-server metadata handler pool for server side connections,
         * use the {@link org.apache.hadoop.hbase.ipc.controller.ServerToServerRpcController}
         * else use the ordinary handler pool {@link ServerRpcController}
         *
         * return the rpcController to use
         * @return
         */
    @VisibleForTesting
    protected RpcController getController(TableName systemTableName) {
        if (serverSideRPCControllerFactory != null) {
            ServerToServerRpcController controller = serverSideRPCControllerFactory.newController();
            controller.setPriority(systemTableName);
            return controller;
        } else {
            return new ServerRpcController();
        }
    }

    @VisibleForTesting
    public ConnectionLimiter getConnectionLimiter() {
        return connectionLimiter;
    }
    /**
     * helper function to return the exception from the RPC
     * @param controller
     * @throws IOException
     */

    private void checkForRemoteExceptions(RpcController controller) throws IOException {
        if (controller != null) {
            if (controller instanceof ServerRpcController) {
                if (((ServerRpcController)controller).getFailedOn() != null) {
                    throw ((ServerRpcController)controller).getFailedOn();
                }
            } else {
                if (((HBaseRpcController)controller).getFailed() != null) {
                    throw ((HBaseRpcController)controller).getFailed();
                }
            }
        }
    }

    /**
     * Invoke meta data coprocessor with one retry if the key was found to not be in the regions
     * (due to a table split)
     */
    private MetaDataMutationResult metaDataCoprocessorExec(String tableName, byte[] tableKey,
            Batch.Call<MetaDataService, MetaDataResponse> callable) throws SQLException {
        return metaDataCoprocessorExec(tableName, tableKey, callable, PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
    }

    /**
     * Invoke meta data coprocessor with one retry if the key was found to not be in the regions
     * (due to a table split)
     */
    private MetaDataMutationResult metaDataCoprocessorExec(String tableName, byte[] tableKey,
            Batch.Call<MetaDataService, MetaDataResponse> callable, byte[] systemTableName) throws SQLException {
        Map<byte[], MetaDataResponse> results;
        try {
            boolean success = false;
            boolean retried = false;
            long startTime = EnvironmentEdgeManager.currentTimeMillis();
            while (true) {
                if (retried) {
                    ((ClusterConnection) connection).relocateRegion(
                        SchemaUtil.getPhysicalName(systemTableName, this.getProps()), tableKey);
                }

                Table ht = this.getTable(SchemaUtil.getPhysicalName(systemTableName, this.getProps()).getName());
                try {
                    results = ht.coprocessorService(MetaDataService.class, tableKey, tableKey, callable);

                    assert(results.size() == 1);
                    MetaDataResponse result = results.values().iterator().next();
                    if (result.getReturnCode() == MetaDataProtos.MutationCode.TABLE_NOT_IN_REGION
                            || result.getReturnCode() == MetaDataProtos.MutationCode.FUNCTION_NOT_IN_REGION) {
                        if (retried) return MetaDataMutationResult.constructFromProto(result);
                        retried = true;
                        continue;
                    }
                    success = true;
                    return MetaDataMutationResult.constructFromProto(result);
                } finally {
                    long systemCatalogRpcTime = EnvironmentEdgeManager.currentTimeMillis() - startTime;
                    TableMetricsManager.updateMetricsForSystemCatalogTableMethod(tableName,
                            TIME_SPENT_IN_SYSTEM_TABLE_RPC_CALLS, systemCatalogRpcTime);
                    if (success) {
                        TableMetricsManager.updateMetricsForSystemCatalogTableMethod(tableName,
                                NUM_SYSTEM_TABLE_RPC_SUCCESS, 1);
                    } else {
                        TableMetricsManager.updateMetricsForSystemCatalogTableMethod(tableName,
                                NUM_SYSTEM_TABLE_RPC_FAILURES, 1);
                    }
                    Closeables.closeQuietly(ht);
                }
            }
        } catch (IOException e) {
            throw ClientUtil.parseServerException(e);
        } catch (Throwable t) {
            throw new SQLException(t);
        }
    }

    // Our property values are translated using toString, so we need to "string-ify" this.
    private static final String TRUE_BYTES_AS_STRING = Bytes.toString(PDataType.TRUE_BYTES);

    private void ensureViewIndexTableCreated(byte[] physicalTableName, byte[] parentPhysicalTableName, Map<String, Object> tableProps,
            List<Pair<byte[], Map<String, Object>>> families, byte[][] splits, long timestamp,
            boolean isNamespaceMapped) throws SQLException {
        byte[] physicalIndexName = MetaDataUtil.getViewIndexPhysicalName(physicalTableName);

        tableProps.put(MetaDataUtil.IS_VIEW_INDEX_TABLE_PROP_NAME, TRUE_BYTES_AS_STRING);
        TableDescriptor desc = ensureTableCreated(physicalIndexName, parentPhysicalTableName, PTableType.TABLE, tableProps, families, splits,
                true, isNamespaceMapped, false);
        if (desc != null) {
            if (!Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(desc.getValue(MetaDataUtil.IS_VIEW_INDEX_TABLE_PROP_BYTES)))) {
                String fullTableName = Bytes.toString(physicalIndexName);
                throw new TableAlreadyExistsException(
                  SchemaUtil.getSchemaNameFromFullName(fullTableName),
                  SchemaUtil.getTableNameFromFullName(fullTableName),
                  "Unable to create shared physical table for indexes on views.");
            }
        }
    }

    private boolean ensureViewIndexTableDropped(byte[] physicalTableName, long timestamp) throws SQLException {
        byte[] physicalIndexName = MetaDataUtil.getViewIndexPhysicalName(physicalTableName);
        boolean wasDeleted = false;
        try (Admin admin = getAdmin()) {
            try {
                TableName physicalIndexTableName = TableName.valueOf(physicalIndexName);
                TableDescriptor desc = admin.getDescriptor(physicalIndexTableName);
                if (Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(desc.getValue(MetaDataUtil.IS_VIEW_INDEX_TABLE_PROP_BYTES)))) {
                    final ReadOnlyProps props = this.getProps();
                    final boolean dropMetadata = props.getBoolean(DROP_METADATA_ATTRIB, DEFAULT_DROP_METADATA);
                    if (dropMetadata) {
                        admin.disableTable(physicalIndexTableName);
                        admin.deleteTable(physicalIndexTableName);
                        clearTableRegionCache(physicalIndexTableName);
                        wasDeleted = true;
                    } else {
                        this.tableStatsCache.invalidateAll(desc);
                    }
                }
            } catch (org.apache.hadoop.hbase.TableNotFoundException ignore) {
                // Ignore, as we may never have created a view index table
            }
        } catch (IOException e) {
            throw ClientUtil.parseServerException(e);
        }
        return wasDeleted;
    }

    private boolean ensureLocalIndexTableDropped(byte[] physicalTableName, long timestamp) throws SQLException {
        TableDescriptor desc = null;
        boolean wasDeleted = false;
        try (Admin admin = getAdmin()) {
            try {
                desc = admin.getDescriptor(TableName.valueOf(physicalTableName));
                for (byte[] fam : desc.getColumnFamilyNames()) {
                    this.tableStatsCache.invalidate(new GuidePostsKey(physicalTableName, fam));
                }
                final ReadOnlyProps props = this.getProps();
                final boolean dropMetadata = props.getBoolean(DROP_METADATA_ATTRIB, DEFAULT_DROP_METADATA);
                if (dropMetadata) {
                    List<String> columnFamiles = new ArrayList<>();
                    for (ColumnFamilyDescriptor cf : desc.getColumnFamilies()) {
                        if (cf.getNameAsString().startsWith(
                            QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX)) {
                            columnFamiles.add(cf.getNameAsString());
                        }
                    }
                    for (String cf: columnFamiles) {
                        admin.deleteColumnFamily(TableName.valueOf(physicalTableName), Bytes.toBytes(cf));
                    }
                    clearTableRegionCache(TableName.valueOf(physicalTableName));
                    wasDeleted = true;
                }
            } catch (org.apache.hadoop.hbase.TableNotFoundException ignore) {
                // Ignore, as we may never have created a view index table
            }
        } catch (IOException e) {
            throw ClientUtil.parseServerException(e);
        }
        return wasDeleted;
    }

    @Override
    public MetaDataMutationResult createTable(final List<Mutation> tableMetaData, final byte[] physicalTableName,
                                              PTableType tableType, Map<String, Object> tableProps,
                                              final List<Pair<byte[], Map<String, Object>>> families,
                                              byte[][] splits, boolean isNamespaceMapped,
                                              final boolean allocateIndexId, final boolean isDoNotUpgradePropSet,
                                              final PTable parentTable) throws SQLException {
        List<Mutation> childLinkMutations = MetaDataUtil.removeChildLinkMutations(tableMetaData);
        byte[][] rowKeyMetadata = new byte[3][];
        Mutation m = MetaDataUtil.getPutOnlyTableHeaderRow(tableMetaData);
        byte[] key = m.getRow();
        SchemaUtil.getVarChars(key, rowKeyMetadata);
        byte[] tenantIdBytes = rowKeyMetadata[PhoenixDatabaseMetaData.TENANT_ID_INDEX];
        byte[] schemaBytes = rowKeyMetadata[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
        byte[] tableBytes = rowKeyMetadata[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
        byte[] physicalTableNameBytes = physicalTableName != null ? physicalTableName :
            SchemaUtil.getPhysicalHBaseTableName(schemaBytes, tableBytes, isNamespaceMapped).getBytes();
        boolean localIndexTable = false;
        for (Pair<byte[], Map<String, Object>> family: families) {
            if (Bytes.toString(family.getFirst()).startsWith(QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX)) {
                localIndexTable = true;
                break;
            }
        }
        if ((tableType == PTableType.VIEW && physicalTableName != null) ||
                (tableType != PTableType.VIEW && (physicalTableName == null || localIndexTable))) {
            // For views this will ensure that metadata already exists
            // For tables and indexes, this will create the metadata if it doesn't already exist
            ensureTableCreated(physicalTableNameBytes, null, tableType, tableProps, families, splits, true,
                    isNamespaceMapped, isDoNotUpgradePropSet);
        }
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        if (tableType == PTableType.INDEX) { // Index on view
            // Physical index table created up front for multi tenant
            // TODO: if viewIndexId is Short.MIN_VALUE, then we don't need to attempt to create it
            if (physicalTableName != null) {
                if (!localIndexTable && !MetaDataUtil.isMultiTenant(m, kvBuilder, ptr)) {
                    // For view index, the physical table name is _IDX_+ logical table name format
                    ensureViewIndexTableCreated(tenantIdBytes.length == 0 ? null : PNameFactory.newName(tenantIdBytes),
                            physicalTableName, MetaDataUtil.getClientTimeStamp(m), isNamespaceMapped);
                }
            }
        } else if (tableType == PTableType.TABLE && MetaDataUtil.isMultiTenant(m, kvBuilder, ptr)) { // Create view index table up front for multi tenant tables
            ptr.set(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES);
            MetaDataUtil.getMutationValue(m, PhoenixDatabaseMetaData.DEFAULT_COLUMN_FAMILY_NAME_BYTES, kvBuilder, ptr);
            List<Pair<byte[],Map<String,Object>>> familiesPlusDefault = null;
            for (Pair<byte[],Map<String,Object>> family : families) {
                byte[] cf = family.getFirst();
                if (Bytes.compareTo(cf, 0, cf.length, ptr.get(), ptr.getOffset(),ptr.getLength()) == 0) {
                    familiesPlusDefault = families;
                    break;
                }
            }
            // Don't override if default family already present
            if (familiesPlusDefault == null) {
                byte[] defaultCF = ByteUtil.copyKeyBytesIfNecessary(ptr);
                // Only use splits if table is salted, otherwise it may not be applicable
                // Always add default column family, as we don't know in advance if we'll need it
                familiesPlusDefault = Lists.newArrayList(families);
                familiesPlusDefault.add(new Pair<byte[],Map<String,Object>>(defaultCF,Collections.<String,Object>emptyMap()));
            }
            ensureViewIndexTableCreated(
                physicalTableNameBytes, physicalTableNameBytes, tableProps, familiesPlusDefault,
                    MetaDataUtil.isSalted(m, kvBuilder, ptr) ? splits : null,
                MetaDataUtil.getClientTimeStamp(m), isNamespaceMapped);
        }

        // Avoid the client-server RPC if this is not a view creation
        if (!childLinkMutations.isEmpty()) {
            // Send mutations for parent-child links to SYSTEM.CHILD_LINK
            // We invoke this using rowKey available in the first element
            // of childLinkMutations.
            final byte[] rowKey = childLinkMutations.get(0).getRow();
            final RpcController controller = getController(PhoenixDatabaseMetaData.SYSTEM_LINK_HBASE_TABLE_NAME);
            final MetaDataMutationResult result =
                childLinkMetaDataCoprocessorExec(rowKey,
                    new ChildLinkMetaDataServiceCallBack(controller, childLinkMutations));

            switch (result.getMutationCode()) {
                case UNABLE_TO_CREATE_CHILD_LINK:
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.UNABLE_TO_CREATE_CHILD_LINK)
                            .setSchemaName(Bytes.toString(schemaBytes))
                            .setTableName(Bytes.toString(physicalTableNameBytes)).build().buildException();
                default:
                    break;
            }
        }

        // Send the remaining metadata mutations to SYSTEM.CATALOG
        byte[] tableKey = SchemaUtil.getTableKey(tenantIdBytes, schemaBytes, tableBytes);
        return metaDataCoprocessorExec(SchemaUtil.getPhysicalHBaseTableName(schemaBytes, tableBytes,
                SchemaUtil.isNamespaceMappingEnabled(PTableType.SYSTEM, this.props)).toString(),
                tableKey,
                new Batch.Call<MetaDataService, MetaDataResponse>() {
            @Override
            public MetaDataResponse call(MetaDataService instance) throws IOException {
                RpcController controller = getController();
                BlockingRpcCallback<MetaDataResponse> rpcCallback =
                        new BlockingRpcCallback<>();
                CreateTableRequest.Builder builder = CreateTableRequest.newBuilder();
                for (Mutation m : tableMetaData) {
                    MutationProto mp = ProtobufUtil.toProto(m);
                    builder.addTableMetadataMutations(mp.toByteString());
                }
                builder.setClientVersion(VersionUtil.encodeVersion(PHOENIX_MAJOR_VERSION, PHOENIX_MINOR_VERSION, PHOENIX_PATCH_NUMBER));
                        if (allocateIndexId) {
                            builder.setAllocateIndexId(allocateIndexId);
                        }
                if (parentTable!=null) {
                    builder.setParentTable(PTableImpl.toProto(parentTable));
                }
                CreateTableRequest build = builder.build();
                instance.createTable(controller, build, rpcCallback);
                checkForRemoteExceptions(controller);
                return rpcCallback.get();
            }
        });
    }

    @Override
    public MetaDataMutationResult getTable(final PName tenantId, final byte[] schemaBytes,
            final byte[] tableBytes, final long tableTimestamp, final long clientTimestamp) throws SQLException {
        final byte[] tenantIdBytes = tenantId == null ? ByteUtil.EMPTY_BYTE_ARRAY : tenantId.getBytes();
        byte[] tableKey = SchemaUtil.getTableKey(tenantIdBytes, schemaBytes, tableBytes);
        return metaDataCoprocessorExec( SchemaUtil.getPhysicalHBaseTableName(schemaBytes, tableBytes,
                SchemaUtil.isNamespaceMappingEnabled(PTableType.SYSTEM, this.props)).toString(),
                tableKey,
                new Batch.Call<MetaDataService, MetaDataResponse>() {
            @Override
            public MetaDataResponse call(MetaDataService instance) throws IOException {
                RpcController controller = getController();
                BlockingRpcCallback<MetaDataResponse> rpcCallback =
                        new BlockingRpcCallback<MetaDataResponse>();
                GetTableRequest.Builder builder = GetTableRequest.newBuilder();
                builder.setTenantId(ByteStringer.wrap(tenantIdBytes));
                builder.setSchemaName(ByteStringer.wrap(schemaBytes));
                builder.setTableName(ByteStringer.wrap(tableBytes));
                builder.setTableTimestamp(tableTimestamp);
                builder.setClientTimestamp(clientTimestamp);
                builder.setClientVersion(VersionUtil.encodeVersion(PHOENIX_MAJOR_VERSION, PHOENIX_MINOR_VERSION, PHOENIX_PATCH_NUMBER));
                instance.getTable(controller, builder.build(), rpcCallback);
                checkForRemoteExceptions(controller);
                return rpcCallback.get();
            }
        });
    }

    @Override
    public MetaDataMutationResult dropTable(final List<Mutation> tableMetaData, final PTableType tableType,
            final boolean cascade) throws SQLException {
        byte[][] rowKeyMetadata = new byte[3][];
        SchemaUtil.getVarChars(tableMetaData.get(0).getRow(), rowKeyMetadata);
        byte[] tenantIdBytes = rowKeyMetadata[PhoenixDatabaseMetaData.TENANT_ID_INDEX];
        byte[] schemaBytes = rowKeyMetadata[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
        byte[] tableBytes = rowKeyMetadata[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
        byte[] tableKey = SchemaUtil.getTableKey(tenantIdBytes == null ? ByteUtil.EMPTY_BYTE_ARRAY : tenantIdBytes, schemaBytes, tableBytes);
        final MetaDataMutationResult result =  metaDataCoprocessorExec(
                SchemaUtil.getPhysicalHBaseTableName(schemaBytes, tableBytes,
                    SchemaUtil.isNamespaceMappingEnabled(PTableType.SYSTEM, this.props)).toString(),
                tableKey,
                new Batch.Call<MetaDataService, MetaDataResponse>() {
            @Override
            public MetaDataResponse call(MetaDataService instance) throws IOException {
                RpcController controller = getController();
                BlockingRpcCallback<MetaDataResponse> rpcCallback =
                        new BlockingRpcCallback<MetaDataResponse>();
                DropTableRequest.Builder builder = DropTableRequest.newBuilder();
                for (Mutation m : tableMetaData) {
                    MutationProto mp = ProtobufUtil.toProto(m);
                    builder.addTableMetadataMutations(mp.toByteString());
                }
                builder.setTableType(tableType.getSerializedValue());
                builder.setCascade(cascade);
                builder.setClientVersion(VersionUtil.encodeVersion(PHOENIX_MAJOR_VERSION, PHOENIX_MINOR_VERSION, PHOENIX_PATCH_NUMBER));
                instance.dropTable(controller, builder.build(), rpcCallback);
                checkForRemoteExceptions(controller);
                return rpcCallback.get();
            }
        });

        final MutationCode code = result.getMutationCode();
        switch(code) {
        case TABLE_ALREADY_EXISTS:
            ReadOnlyProps props = this.getProps();
            boolean dropMetadata = props.getBoolean(DROP_METADATA_ATTRIB, DEFAULT_DROP_METADATA);
            PTable table = result.getTable();
            if (dropMetadata) {
                flushParentPhysicalTable(table);
                dropTables(result.getTableNamesToDelete());
            } else {
                invalidateTableStats(result.getTableNamesToDelete());
            }
            long timestamp = MetaDataUtil.getClientTimeStamp(tableMetaData);
            if (tableType == PTableType.TABLE) {
                byte[] physicalName = table.getPhysicalName().getBytes();
                ensureViewIndexTableDropped(physicalName, timestamp);
                ensureLocalIndexTableDropped(physicalName, timestamp);
                tableStatsCache.invalidateAll(table);
            }
            break;
        default:
            break;
        }
        return result;
    }

    /*
     * PHOENIX-2915 while dropping index, flush data table to avoid stale WAL edits of indexes 1. Flush parent table if
     * dropping view has indexes 2. Dropping table indexes 3. Dropping view indexes
     */
    private void flushParentPhysicalTable(PTable table) throws SQLException {
        byte[] parentPhysicalTableName = null;
        if (PTableType.VIEW == table.getType()) {
            if (!table.getIndexes().isEmpty()) {
                parentPhysicalTableName = table.getPhysicalName().getBytes();
            }
        } else if (PTableType.INDEX == table.getType()) {
            PTable parentTable = getTable(table.getTenantId(), table.getParentName().getString(), HConstants.LATEST_TIMESTAMP);
            parentPhysicalTableName = parentTable.getPhysicalName().getBytes();
        }
        if (parentPhysicalTableName != null) {
            try {
                flushTable(parentPhysicalTableName);
            } catch (PhoenixIOException ex) {
                if (ex.getCause() instanceof org.apache.hadoop.hbase.TableNotFoundException) {
                    LOGGER.info("Flushing physical parent table " + Bytes.toString(parentPhysicalTableName) + " of " + table.getName()
                            .getString() + " failed with : " + ex + " with cause: " + ex.getCause()
                            + " since the table has already been dropped");
                } else {
                    throw ex;
                }
            }
        }
    }

    @Override
    public MetaDataMutationResult dropFunction(final List<Mutation> functionData, final boolean ifExists) throws SQLException {
        byte[][] rowKeyMetadata = new byte[2][];
        byte[] key = functionData.get(0).getRow();
        SchemaUtil.getVarChars(key, rowKeyMetadata);
        byte[] tenantIdBytes = rowKeyMetadata[PhoenixDatabaseMetaData.TENANT_ID_INDEX];
        byte[] functionBytes = rowKeyMetadata[PhoenixDatabaseMetaData.FUNTION_NAME_INDEX];
        byte[] functionKey = SchemaUtil.getFunctionKey(tenantIdBytes, functionBytes);

        final MetaDataMutationResult result =  metaDataCoprocessorExec(null, functionKey,
                new Batch.Call<MetaDataService, MetaDataResponse>() {
            @Override
            public MetaDataResponse call(MetaDataService instance) throws IOException {
                RpcController controller = getController(SYSTEM_FUNCTION_HBASE_TABLE_NAME);
                BlockingRpcCallback<MetaDataResponse> rpcCallback =
                        new BlockingRpcCallback<MetaDataResponse>();
                DropFunctionRequest.Builder builder = DropFunctionRequest.newBuilder();
                for (Mutation m : functionData) {
                    MutationProto mp = ProtobufUtil.toProto(m);
                    builder.addTableMetadataMutations(mp.toByteString());
                }
                builder.setIfExists(ifExists);
                builder.setClientVersion(VersionUtil.encodeVersion(PHOENIX_MAJOR_VERSION, PHOENIX_MINOR_VERSION, PHOENIX_PATCH_NUMBER));
                instance.dropFunction(controller, builder.build(), rpcCallback);
                checkForRemoteExceptions(controller);
                return rpcCallback.get();
            }
        }, SYSTEM_FUNCTION_NAME_BYTES);
        return result;
    }

    private void invalidateTableStats(final List<byte[]> tableNamesToDelete) throws SQLException {
        if (tableNamesToDelete != null) {
            for (byte[] tableName : tableNamesToDelete) {
                TableName tn = TableName.valueOf(tableName);
                TableDescriptor htableDesc = this.getTableDescriptor(tableName);
                tableStatsCache.invalidateAll(htableDesc);
            }
        }
    }

    private void dropTable(byte[] tableNameToDelete) throws SQLException {
        dropTables(Collections.<byte[]>singletonList(tableNameToDelete));
    }

    private void dropTables(final List<byte[]> tableNamesToDelete) throws SQLException {
        SQLException sqlE = null;
        try (Admin admin = getAdmin()) {
            if (tableNamesToDelete != null) {
                for ( byte[] tableName : tableNamesToDelete ) {
                    try {
                        TableName tn = TableName.valueOf(tableName);
                        TableDescriptor htableDesc = this.getTableDescriptor(tableName);
                        admin.disableTable(tn);
                        admin.deleteTable(tn);
                        tableStatsCache.invalidateAll(htableDesc);
                        clearTableRegionCache(TableName.valueOf(tableName));
                    } catch (TableNotFoundException ignore) {
                    }
                }
            }

        } catch (IOException e) {
            sqlE = ClientUtil.parseServerException(e);
        } finally {
            if (sqlE != null) {
                throw sqlE;
            }
        }
    }

    private static Map<String,Object> createPropertiesMap(Map<Bytes,Bytes> htableProps) {
        Map<String,Object> props = Maps.newHashMapWithExpectedSize(htableProps.size());
        for (Map.Entry<Bytes,Bytes> entry : htableProps.entrySet()) {
            Bytes key = entry.getKey();
            Bytes value = entry.getValue();
            props.put(Bytes.toString(key.get(), key.getOffset(), key.getLength()),
                    Bytes.toString(value.get(), value.getOffset(), value.getLength()));
        }
        return props;
    }

    private void ensureViewIndexTableCreated(PName tenantId, byte[] physicalIndexTableName, long timestamp,
            boolean isNamespaceMapped) throws SQLException {
        String name = Bytes
                .toString(SchemaUtil.getParentTableNameFromIndexTable(physicalIndexTableName,
                        MetaDataUtil.VIEW_INDEX_TABLE_PREFIX))
                        .replace(QueryConstants.NAMESPACE_SEPARATOR, QueryConstants.NAME_SEPARATOR);
        PTable table = getTable(tenantId, name, timestamp);
        ensureViewIndexTableCreated(table, timestamp, isNamespaceMapped);
    }

    private PTable getTable(PName tenantId, String fullTableName, long timestamp) throws SQLException {
        PTable table;
        try {
            PMetaData metadata = latestMetaData;
            throwConnectionClosedIfNullMetaData();
            table = metadata.getTableRef(new PTableKey(tenantId, fullTableName)).getTable();
            if (table.getTimeStamp() >= timestamp) { // Table in cache is newer than client timestamp which shouldn't be
                // the case
                throw new TableNotFoundException(table.getSchemaName().getString(), table.getTableName().getString());
            }
        } catch (TableNotFoundException e) {
            byte[] schemaName = Bytes.toBytes(SchemaUtil.getSchemaNameFromFullName(fullTableName));
            byte[] tableName = Bytes.toBytes(SchemaUtil.getTableNameFromFullName(fullTableName));
            MetaDataMutationResult result = this.getTable(tenantId, schemaName, tableName, HConstants.LATEST_TIMESTAMP,
                    timestamp);
            table = result.getTable();
            if (table == null) { throw e; }
        }
        return table;
    }

    private void ensureViewIndexTableCreated(PTable table, long timestamp, boolean isNamespaceMapped)
            throws SQLException {
        byte[] physicalTableName = table.getPhysicalName().getBytes();
        TableDescriptor htableDesc = this.getTableDescriptor(physicalTableName);
        List<Pair<byte[],Map<String,Object>>> families = Lists.newArrayListWithExpectedSize(Math.max(1, table.getColumnFamilies().size() + 1));

        // Create all column families that the parent table has
        for (PColumnFamily family : table.getColumnFamilies()) {
            byte[] familyName = family.getName().getBytes();
            Map<String,Object> familyProps = createPropertiesMap(htableDesc.getColumnFamily(familyName).getValues());
            families.add(new Pair<>(familyName, familyProps));
        }
        // Always create default column family, because we don't know in advance if we'll
        // need it for an index with no covered columns.
        byte[] defaultFamilyName = table.getDefaultFamilyName() == null ?
          QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES : table.getDefaultFamilyName().getBytes();
        families.add(new Pair<>(defaultFamilyName, Collections.<String,Object>emptyMap()));

        byte[][] splits = null;
        if (table.getBucketNum() != null) {
            splits = SaltingUtil.getSalteByteSplitPoints(table.getBucketNum());
        }

        // Transfer over table values into tableProps
        // TODO: encapsulate better
        Map<String,Object> tableProps = createPropertiesMap(htableDesc.getValues());
        tableProps.put(PhoenixDatabaseMetaData.TRANSACTIONAL, table.isTransactional());
        tableProps.put(PhoenixDatabaseMetaData.IMMUTABLE_ROWS, table.isImmutableRows());

        // We got the properties of the physical base table but we need to create the view index table using logical name
        byte[] viewPhysicalTableName =
                MetaDataUtil.getNamespaceMappedName(table.getName(), isNamespaceMapped)
                .getBytes(StandardCharsets.UTF_8);
        ensureViewIndexTableCreated(viewPhysicalTableName, physicalTableName, tableProps, families, splits, timestamp, isNamespaceMapped);
    }

    @Override
    public MetaDataMutationResult addColumn(final List<Mutation> tableMetaData,
                                            PTable table,
                                            final PTable parentTable,
                                            final PTable transformingNewTable,
                                            Map<String, List<Pair<String, Object>>> stmtProperties,
                                            Set<String> colFamiliesForPColumnsToBeAdded,
                                            List<PColumn> columns) throws SQLException {
        List<Pair<byte[], Map<String, Object>>> families = new ArrayList<>(stmtProperties.size());
        Map<String, Object> tableProps = new HashMap<>();
        Set<TableDescriptor> tableDescriptors = Collections.emptySet();
        boolean nonTxToTx = false;

        Map<TableDescriptor, TableDescriptor> oldToNewTableDescriptors =
                separateAndValidateProperties(table, stmtProperties, colFamiliesForPColumnsToBeAdded, tableProps);
        Set<TableDescriptor> origTableDescriptors = new HashSet<>(oldToNewTableDescriptors.keySet());

        TableDescriptor baseTableOrigDesc = this.getTableDescriptor(table.getPhysicalName().getBytes());
        TableDescriptor tableDescriptor = oldToNewTableDescriptors.get(baseTableOrigDesc);

        if (tableDescriptor != null) {
            tableDescriptors = Sets.newHashSetWithExpectedSize(3 + table.getIndexes().size());
            nonTxToTx = Boolean.TRUE.equals(tableProps.get(PhoenixTransactionContext.READ_NON_TX_DATA));
            /*
             * If the table was transitioned from non transactional to transactional, we need
             * to also transition the index tables.
             */

            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableDescriptor);
            if (nonTxToTx) {
                updateDescriptorForTx(table, tableProps, tableDescriptorBuilder, Boolean.TRUE.toString(),
                        tableDescriptors, origTableDescriptors, oldToNewTableDescriptors);
                tableDescriptor = tableDescriptorBuilder.build();
                tableDescriptors.add(tableDescriptor);
            } else {
                tableDescriptors = new HashSet<>(oldToNewTableDescriptors.values());
            }
        }

        boolean success = false;
        boolean metaDataUpdated = !tableDescriptors.isEmpty();
        boolean pollingNeeded = !(!tableProps.isEmpty() && families.isEmpty() && colFamiliesForPColumnsToBeAdded.isEmpty());
        MetaDataMutationResult result = null;
        try {
            boolean modifyHTable = true;
            if (table.getType() == PTableType.VIEW) {
                boolean canViewsAddNewCF = props.getBoolean(QueryServices.ALLOW_VIEWS_ADD_NEW_CF_BASE_TABLE,
                        QueryServicesOptions.DEFAULT_ALLOW_VIEWS_ADD_NEW_CF_BASE_TABLE);
                // When adding a column to a view, base physical table should only be modified when new column families are being added.
                modifyHTable = canViewsAddNewCF && !existingColumnFamiliesForBaseTable(table.getPhysicalName()).containsAll(colFamiliesForPColumnsToBeAdded);
            }

            // Special case for call during drop table to ensure that the empty column family exists.
            // In this, case we only include the table header row, as until we add schemaBytes and tableBytes
            // as args to this function, we have no way of getting them in this case.
            // TODO: change to  if (tableMetaData.isEmpty()) once we pass through schemaBytes and tableBytes
            // Also, could be used to update table descriptor property values on ALTER TABLE t SET prop=xxx
            if ((tableMetaData.isEmpty()) || (tableMetaData.size() == 1 && tableMetaData.get(0).isEmpty())) {
                if (modifyHTable) {
                    sendHBaseMetaData(tableDescriptors, pollingNeeded);
                }
                return new MetaDataMutationResult(MutationCode.NO_OP, EnvironmentEdgeManager.currentTimeMillis(), table);
            }
            byte[][] rowKeyMetaData = new byte[3][];
            PTableType tableType = table.getType();

            Mutation m = tableMetaData.get(0);
            byte[] rowKey = m.getRow();
            SchemaUtil.getVarChars(rowKey, rowKeyMetaData);
            byte[] tenantIdBytes = rowKeyMetaData[PhoenixDatabaseMetaData.TENANT_ID_INDEX];
            byte[] schemaBytes = rowKeyMetaData[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
            byte[] tableBytes = rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
            byte[] tableKey = SchemaUtil.getTableKey(tenantIdBytes, schemaBytes, tableBytes);

            ImmutableBytesWritable ptr = new ImmutableBytesWritable();
            final boolean addingColumns = columns != null && columns.size() > 0;
            result =  metaDataCoprocessorExec(
                    SchemaUtil.getPhysicalHBaseTableName(schemaBytes, tableBytes,
                            SchemaUtil.isNamespaceMappingEnabled(PTableType.SYSTEM, this.props)).toString(),
                    tableKey,
                    new Batch.Call<MetaDataService, MetaDataResponse>() {
                @Override
                public MetaDataResponse call(MetaDataService instance) throws IOException {
                    RpcController controller = getController();
                    BlockingRpcCallback<MetaDataResponse> rpcCallback =
                            new BlockingRpcCallback<MetaDataResponse>();
                    AddColumnRequest.Builder builder = AddColumnRequest.newBuilder();
                    for (Mutation m : tableMetaData) {
                        MutationProto mp = ProtobufUtil.toProto(m);
                        builder.addTableMetadataMutations(mp.toByteString());
                    }
                    builder.setClientVersion(VersionUtil.encodeVersion(PHOENIX_MAJOR_VERSION, PHOENIX_MINOR_VERSION, PHOENIX_PATCH_NUMBER));
                    if (parentTable!=null)
                        builder.setParentTable(PTableImpl.toProto(parentTable));
                    if (transformingNewTable!=null) {
                        builder.setTransformingNewTable(PTableImpl.toProto(transformingNewTable));
                    }
                    builder.setAddingColumns(addingColumns);
                    instance.addColumn(controller, builder.build(), rpcCallback);
                    checkForRemoteExceptions(controller);
                    return rpcCallback.get();
                }
            });

            if (result.getMutationCode() == MutationCode.COLUMN_NOT_FOUND || result.getMutationCode() == MutationCode.TABLE_ALREADY_EXISTS) { // Success
                success = true;
                // Flush the table if transitioning DISABLE_WAL from TRUE to FALSE
                if (  MetaDataUtil.getMutationValue(m,PhoenixDatabaseMetaData.DISABLE_WAL_BYTES, kvBuilder, ptr)
                        && Boolean.FALSE.equals(PBoolean.INSTANCE.toObject(ptr))) {
                    flushTable(table.getPhysicalName().getBytes());
                }

                if (tableType == PTableType.TABLE) {
                    // If we're changing MULTI_TENANT to true or false, create or drop the view index table
                    if (MetaDataUtil.getMutationValue(m, PhoenixDatabaseMetaData.MULTI_TENANT_BYTES, kvBuilder, ptr)){
                        long timestamp = MetaDataUtil.getClientTimeStamp(m);
                        if (Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(ptr.get(), ptr.getOffset(), ptr.getLength()))) {
                            this.ensureViewIndexTableCreated(table, timestamp,table.isNamespaceMapped());
                        } else {
                            this.ensureViewIndexTableDropped(table.getPhysicalName().getBytes(), timestamp);
                        }
                    }
                }
            }

            if (modifyHTable && result.getMutationCode() != MutationCode.UNALLOWED_TABLE_MUTATION) {
                sendHBaseMetaData(tableDescriptors, pollingNeeded);
            }
        } finally {
            // If we weren't successful with our metadata update
            // and we've already pushed the HBase metadata changes to the server
            // and we've tried to go from non transactional to transactional
            // then we must undo the metadata change otherwise the table will
            // no longer function correctly.
            // Note that if this fails, we're in a corrupt state.
            if (!success && metaDataUpdated && nonTxToTx) {
                sendHBaseMetaData(origTableDescriptors, pollingNeeded);
            }
        }
        return result;
    }

    private void updateDescriptorForTx(PTable table, Map<String, Object> tableProps, TableDescriptorBuilder tableDescriptorBuilder,
            String txValue, Set<TableDescriptor> descriptorsToUpdate, Set<TableDescriptor> origDescriptors,
            Map<TableDescriptor, TableDescriptor> oldToNewTableDescriptors) throws SQLException {
        byte[] physicalTableName = table.getPhysicalName().getBytes();
        try (Admin admin = getAdmin()) {
            TableDescriptor baseDesc = admin.getDescriptor(TableName.valueOf(physicalTableName));
            boolean hasOldIndexing = baseDesc.hasCoprocessor(QueryConstants.INDEXER_CLASSNAME);
            setTransactional(physicalTableName, tableDescriptorBuilder, table.getType(), txValue, tableProps, hasOldIndexing);
            Map<String, Object> indexTableProps;
            if (txValue == null) {
                indexTableProps = Collections.emptyMap();
            } else {
                indexTableProps = Maps.newHashMapWithExpectedSize(1);
                indexTableProps.put(PhoenixTransactionContext.READ_NON_TX_DATA, Boolean.valueOf(txValue));
                indexTableProps.put(PhoenixDatabaseMetaData.TRANSACTION_PROVIDER, tableProps.get(PhoenixDatabaseMetaData.TRANSACTION_PROVIDER));
            }
            for (PTable index : table.getIndexes()) {
                TableDescriptor origIndexDesc = admin.getDescriptor(TableName.valueOf(index.getPhysicalName().getBytes()));
                TableDescriptor intermedIndexDesc = origIndexDesc;
                // If we already wished to make modifications to this index table descriptor previously, we use the updated
                // table descriptor to carry out further modifications
                // See {@link ConnectionQueryServicesImpl#separateAndValidateProperties(PTable, Map, Set, Map)}
                if (origDescriptors.contains(origIndexDesc)) {
                    intermedIndexDesc = oldToNewTableDescriptors.get(origIndexDesc);
                    // Remove any previous modification for this table descriptor because we will add
                    // the combined modification done in this method as well
                    descriptorsToUpdate.remove(intermedIndexDesc);
                } else {
                    origDescriptors.add(origIndexDesc);
                }
                TableDescriptorBuilder indexDescriptorBuilder = TableDescriptorBuilder.newBuilder(intermedIndexDesc);
                if (index.getColumnFamilies().isEmpty()) {
                    byte[] dataFamilyName = SchemaUtil.getEmptyColumnFamily(table);
                    byte[] indexFamilyName = SchemaUtil.getEmptyColumnFamily(index);
                    ColumnFamilyDescriptorBuilder indexColDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(indexDescriptorBuilder.build().getColumnFamily(indexFamilyName));
                    ColumnFamilyDescriptor tableColDescriptor = tableDescriptorBuilder.build().getColumnFamily(dataFamilyName);
                    indexColDescriptor.setMaxVersions(tableColDescriptor.getMaxVersions());
                    indexColDescriptor.setValue(Bytes.toBytes(PhoenixTransactionContext.PROPERTY_TTL),
                            tableColDescriptor.getValue(Bytes.toBytes(PhoenixTransactionContext.PROPERTY_TTL)));
                    indexDescriptorBuilder.removeColumnFamily(indexFamilyName);
                    indexDescriptorBuilder.setColumnFamily(indexColDescriptor.build());
                } else {
                    for (PColumnFamily family : index.getColumnFamilies()) {
                        byte[] familyName = family.getName().getBytes();
                        ColumnFamilyDescriptorBuilder indexColDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(indexDescriptorBuilder.build().getColumnFamily(familyName));
                        ColumnFamilyDescriptor tableColDescriptor = tableDescriptorBuilder.build().getColumnFamily(familyName);
                        indexColDescriptor.setMaxVersions(tableColDescriptor.getMaxVersions());
                        indexColDescriptor.setValue(Bytes.toBytes(PhoenixTransactionContext.PROPERTY_TTL),
                                tableColDescriptor.getValue(Bytes.toBytes(PhoenixTransactionContext.PROPERTY_TTL)));
                        indexDescriptorBuilder.removeColumnFamily(familyName);
                        indexDescriptorBuilder.setColumnFamily(indexColDescriptor.build());
                    }
                }
                setTransactional(index.getPhysicalName().getBytes(), indexDescriptorBuilder, index.getType(), txValue, indexTableProps, hasOldIndexing);
                descriptorsToUpdate.add(indexDescriptorBuilder.build());
            }
            try {
                TableDescriptor origIndexDesc = admin.getDescriptor(TableName.valueOf(MetaDataUtil.getViewIndexPhysicalName(physicalTableName)));
                TableDescriptor intermedIndexDesc = origIndexDesc;
                if (origDescriptors.contains(origIndexDesc)) {
                    intermedIndexDesc = oldToNewTableDescriptors.get(origIndexDesc);
                    descriptorsToUpdate.remove(intermedIndexDesc);
                } else {
                    origDescriptors.add(origIndexDesc);
                }
                TableDescriptorBuilder indexDescriptorBuilder = TableDescriptorBuilder.newBuilder(intermedIndexDesc);
                setSharedIndexMaxVersion(table, tableDescriptorBuilder.build(), indexDescriptorBuilder);
                setTransactional(MetaDataUtil.getViewIndexPhysicalName(physicalTableName), indexDescriptorBuilder, PTableType.INDEX, txValue, indexTableProps, hasOldIndexing);
                descriptorsToUpdate.add(indexDescriptorBuilder.build());
            } catch (org.apache.hadoop.hbase.TableNotFoundException ignore) {
                // Ignore, as we may never have created a view index table
            }
            try {
                TableDescriptor origIndexDesc = admin.getDescriptor(TableName.valueOf(MetaDataUtil.getLocalIndexPhysicalName(physicalTableName)));
                TableDescriptor intermedIndexDesc = origIndexDesc;
                if (origDescriptors.contains(origIndexDesc)) {
                    intermedIndexDesc = oldToNewTableDescriptors.get(origIndexDesc);
                    descriptorsToUpdate.remove(intermedIndexDesc);
                } else {
                    origDescriptors.add(origIndexDesc);
                }
                TableDescriptorBuilder indexDescriptorBuilder = TableDescriptorBuilder.newBuilder(intermedIndexDesc);
                setSharedIndexMaxVersion(table, tableDescriptorBuilder.build(), indexDescriptorBuilder);
                setTransactional(MetaDataUtil.getViewIndexPhysicalName(physicalTableName), indexDescriptorBuilder, PTableType.INDEX, txValue, indexTableProps, hasOldIndexing);
                descriptorsToUpdate.add(indexDescriptorBuilder.build());
            } catch (org.apache.hadoop.hbase.TableNotFoundException ignore) {
                // Ignore, as we may never have created a local index
            }
        } catch (IOException e) {
            throw ClientUtil.parseServerException(e);
        }
    }

    private void setSharedIndexMaxVersion(PTable table, TableDescriptor tableDescriptor,
            TableDescriptorBuilder indexDescriptorBuilder) {
        if (table.getColumnFamilies().isEmpty()) {
            byte[] familyName = SchemaUtil.getEmptyColumnFamily(table);
            ColumnFamilyDescriptorBuilder indexColDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(indexDescriptorBuilder.build().getColumnFamily(familyName));
            ColumnFamilyDescriptor tableColDescriptor = tableDescriptor.getColumnFamily(familyName);
            indexColDescriptorBuilder.setMaxVersions(tableColDescriptor.getMaxVersions());
            indexColDescriptorBuilder.setValue( Bytes.toBytes(PhoenixTransactionContext.PROPERTY_TTL),tableColDescriptor.getValue(Bytes.toBytes(PhoenixTransactionContext.PROPERTY_TTL)));
            indexDescriptorBuilder.removeColumnFamily(familyName);
            indexDescriptorBuilder.setColumnFamily(indexColDescriptorBuilder.build());
        } else {
            for (PColumnFamily family : table.getColumnFamilies()) {
                byte[] familyName = family.getName().getBytes();
                ColumnFamilyDescriptor indexColDescriptor = indexDescriptorBuilder.build().getColumnFamily(familyName);
                if (indexColDescriptor != null) {
                    ColumnFamilyDescriptor tableColDescriptor = tableDescriptor.getColumnFamily(familyName);
                    ColumnFamilyDescriptorBuilder indexColDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(indexColDescriptor);
                    indexColDescriptorBuilder.setMaxVersions(tableColDescriptor.getMaxVersions());
                    indexColDescriptorBuilder.setValue( Bytes.toBytes(PhoenixTransactionContext.PROPERTY_TTL),tableColDescriptor.getValue(Bytes.toBytes(PhoenixTransactionContext.PROPERTY_TTL)));
                    indexDescriptorBuilder.removeColumnFamily(familyName);
                    indexDescriptorBuilder.setColumnFamily(indexColDescriptorBuilder.build());
                }
            }
        }
    }

    private void sendHBaseMetaData(Set<TableDescriptor> tableDescriptors, boolean pollingNeeded) throws SQLException {
        SQLException sqlE = null;
        for (TableDescriptor descriptor : tableDescriptors) {
            try {
                modifyTable(descriptor.getTableName().getName(), descriptor, pollingNeeded);
            } catch (IOException e) {
                sqlE = ClientUtil.parseServerException(e);
            } catch (InterruptedException e) {
                // restore the interrupt status
                Thread.currentThread().interrupt();
                sqlE = new SQLExceptionInfo.Builder(SQLExceptionCode.INTERRUPTED_EXCEPTION).setRootCause(e).build().buildException();
            } catch (TimeoutException e) {
                sqlE = new SQLExceptionInfo.Builder(SQLExceptionCode.OPERATION_TIMED_OUT).setRootCause(e.getCause() != null ? e.getCause() : e).build().buildException();
            } finally {
                if (sqlE != null) {
                    throw sqlE;
                }
            }
        }
    }
    private void setTransactional(byte[] physicalTableName, TableDescriptorBuilder tableDescriptorBuilder, PTableType tableType, String txValue, Map<String, Object> tableProps, boolean hasOldIndexing) throws SQLException {
        if (txValue == null) {
            tableDescriptorBuilder.removeValue(Bytes.toBytes(PhoenixTransactionContext.READ_NON_TX_DATA));
        } else {
            tableDescriptorBuilder.setValue(PhoenixTransactionContext.READ_NON_TX_DATA, txValue);
        }
        this.addCoprocessors(physicalTableName, tableDescriptorBuilder, tableType, tableProps, null, hasOldIndexing);
    }

    private Map<TableDescriptor, TableDescriptor> separateAndValidateProperties(PTable table,
            Map<String, List<Pair<String, Object>>> properties, Set<String> colFamiliesForPColumnsToBeAdded,
             Map<String, Object> tableProps) throws SQLException {
        Map<String, Map<String, Object>> stmtFamiliesPropsMap = new HashMap<>(properties.size());
        Map<String,Object> commonFamilyProps = new HashMap<>();
        boolean addingColumns = colFamiliesForPColumnsToBeAdded != null && !colFamiliesForPColumnsToBeAdded.isEmpty();
        HashSet<String> existingColumnFamilies = existingColumnFamilies(table);
        Map<String, Map<String, Object>> allFamiliesProps = new HashMap<>(existingColumnFamilies.size());
        boolean isTransactional = table.isTransactional();
        boolean willBeTransactional = false;
        boolean isOrWillBeTransactional = isTransactional;
        Integer newTTL = null;
        Integer newReplicationScope = null;
        KeepDeletedCells newKeepDeletedCells = null;
        TransactionFactory.Provider txProvider = null;
        for (String family : properties.keySet()) {
            List<Pair<String, Object>> propsList = properties.get(family);
            if (propsList != null && propsList.size() > 0) {
                Map<String, Object> colFamilyPropsMap = new HashMap<>(propsList.size());
                for (Pair<String, Object> prop : propsList) {
                    String propName = prop.getFirst();
                    Object propValue = prop.getSecond();
                    if ((MetaDataUtil.isHTableProperty(propName) ||  TableProperty.isPhoenixTableProperty(propName)) && addingColumns) {
                        // setting HTable and PhoenixTable properties while adding a column is not allowed.
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_SET_TABLE_PROPERTY_ADD_COLUMN)
                        .setMessage("Property: " + propName)
                        .setSchemaName(table.getSchemaName().getString())
                        .setTableName(table.getTableName().getString())
                        .build()
                        .buildException();
                    }
                    if (MetaDataUtil.isHTableProperty(propName)) {
                        // Can't have a column family name for a property that's an HTableProperty
                        if (!family.equals(QueryConstants.ALL_FAMILY_PROPERTIES_KEY)) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.COLUMN_FAMILY_NOT_ALLOWED_TABLE_PROPERTY)
                            .setMessage("Column Family: " + family + ", Property: " + propName)
                            .setSchemaName(table.getSchemaName().getString())
                            .setTableName(table.getTableName().getString())
                            .build()
                            .buildException();
                        }
                        tableProps.put(propName, propValue);
                    } else {
                        if (TableProperty.isPhoenixTableProperty(propName)) {
                            TableProperty tableProp = TableProperty.valueOf(propName);
                            tableProp.validate(true, !family.equals(QueryConstants.ALL_FAMILY_PROPERTIES_KEY), table.getType());
                            if (propName.equals(TTL)) {
                                if (table.getType() == PTableType.INDEX) {
                                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_SET_OR_ALTER_PROPERTY_FOR_INDEX)
                                            .setMessage("Property: " + propName).build()
                                            .buildException();
                                }
                                newTTL = ((Number)propValue).intValue();
                                // Even though TTL is really a HColumnProperty we treat it specially.
                                // We enforce that all column families have the same TTL.
                                commonFamilyProps.put(propName, propValue);
                            } else if (propName.equals(PhoenixDatabaseMetaData.TRANSACTIONAL) && Boolean.TRUE.equals(propValue)) {
                                willBeTransactional = isOrWillBeTransactional = true;
                                tableProps.put(PhoenixTransactionContext.READ_NON_TX_DATA, propValue);
                            } else if (propName.equals(PhoenixDatabaseMetaData.TRANSACTION_PROVIDER) && propValue != null) {
                                willBeTransactional = isOrWillBeTransactional = true;
                                tableProps.put(PhoenixTransactionContext.READ_NON_TX_DATA, Boolean.TRUE);
                                txProvider = (Provider)TableProperty.TRANSACTION_PROVIDER.getValue(propValue);
                                tableProps.put(PhoenixDatabaseMetaData.TRANSACTION_PROVIDER, txProvider);
                            }
                        } else {
                            if (MetaDataUtil.isHColumnProperty(propName)) {
                                if (table.getType() == PTableType.INDEX && MetaDataUtil.propertyNotAllowedToBeOutOfSync(propName)) {
                                    // We disallow index tables from overriding TTL, KEEP_DELETED_CELLS and REPLICATION_SCOPE,
                                    // in order to avoid situations where indexes are not in sync with their data table
                                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_SET_OR_ALTER_PROPERTY_FOR_INDEX)
                                            .setMessage("Property: " + propName).build()
                                            .buildException();
                                }
                                if (family.equals(QueryConstants.ALL_FAMILY_PROPERTIES_KEY)) {
                                    if (propName.equals(KEEP_DELETED_CELLS)) {
                                        newKeepDeletedCells =
                                                Boolean.valueOf(propValue.toString()) ? KeepDeletedCells.TRUE : KeepDeletedCells.FALSE;
                                    }
                                    if (propName.equals(REPLICATION_SCOPE)) {
                                        newReplicationScope = ((Number)propValue).intValue();
                                    }
                                    commonFamilyProps.put(propName, propValue);
                                } else if (MetaDataUtil.propertyNotAllowedToBeOutOfSync(propName)) {
                                    // Don't allow specifying column families for TTL, KEEP_DELETED_CELLS and REPLICATION_SCOPE.
                                    // These properties can only be applied for all column families of a table and can't be column family specific.
                                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.COLUMN_FAMILY_NOT_ALLOWED_FOR_PROPERTY)
                                            .setMessage("Property: " + propName).build()
                                            .buildException();
                                } else {
                                    colFamilyPropsMap.put(propName, propValue);
                                }
                            } else {
                                // invalid property - neither of HTableProp, HColumnProp or PhoenixTableProp
                                // FIXME: This isn't getting triggered as currently a property gets evaluated
                                // as HTableProp if its neither HColumnProp or PhoenixTableProp.
                                throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_ALTER_PROPERTY)
                                .setMessage("Column Family: " + family + ", Property: " + propName)
                                .setSchemaName(table.getSchemaName().getString())
                                .setTableName(table.getTableName().getString())
                                .build()
                                .buildException();
                            }
                        }
                    }
                }
                if (isOrWillBeTransactional && newTTL != null) {
                    TransactionFactory.Provider isOrWillBeTransactionProvider = txProvider == null ? table.getTransactionProvider() : txProvider;
                    if (isOrWillBeTransactionProvider.getTransactionProvider().isUnsupported(PhoenixTransactionProvider.Feature.SET_TTL)) {
                        throw new SQLExceptionInfo.Builder(PhoenixTransactionProvider.Feature.SET_TTL.getCode())
                        .setMessage(isOrWillBeTransactionProvider.name())
                        .setSchemaName(table.getSchemaName().getString())
                        .setTableName(table.getTableName().getString())
                        .build()
                        .buildException();
                    }
                }
                if (!colFamilyPropsMap.isEmpty()) {
                    stmtFamiliesPropsMap.put(family, colFamilyPropsMap);
                }

            }
        }
        commonFamilyProps = Collections.unmodifiableMap(commonFamilyProps);
        boolean isAddingPkColOnly = colFamiliesForPColumnsToBeAdded.size() == 1 && colFamiliesForPColumnsToBeAdded.contains(null);
        if (!commonFamilyProps.isEmpty()) {
            if (!addingColumns) {
                // Add the common family props to all existing column families
                for (String existingColFamily : existingColumnFamilies) {
                    Map<String, Object> m = new HashMap<>(commonFamilyProps.size());
                    m.putAll(commonFamilyProps);
                    allFamiliesProps.put(existingColFamily, m);
                }
            } else {
                // Add the common family props to the column families of the columns being added
                for (String colFamily : colFamiliesForPColumnsToBeAdded) {
                    if (colFamily != null) {
                        // only set properties for key value columns
                        Map<String, Object> m = new HashMap<>(commonFamilyProps.size());
                        m.putAll(commonFamilyProps);
                        allFamiliesProps.put(colFamily, m);
                    } else if (isAddingPkColOnly) {
                        // Setting HColumnProperty for a pk column is invalid
                        // because it will be part of the row key and not a key value column family.
                        // However, if both pk cols as well as key value columns are getting added
                        // together, then its allowed. The above if block will make sure that we add properties
                        // only for the kv cols and not pk cols.
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.SET_UNSUPPORTED_PROP_ON_ALTER_TABLE)
                        .build().buildException();
                    }
                }
            }
        }

        // Now go through the column family properties specified in the statement
        // and merge them with the common family properties.
        for (String f : stmtFamiliesPropsMap.keySet()) {
            if (!addingColumns && !existingColumnFamilies.contains(f)) {
                String schemaNameStr = table.getSchemaName()==null?null:table.getSchemaName().getString();
                String tableNameStr = table.getTableName()==null?null:table.getTableName().getString();
                throw new ColumnFamilyNotFoundException(schemaNameStr, tableNameStr, f);
            }
            if (addingColumns && !colFamiliesForPColumnsToBeAdded.contains(f)) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_SET_PROPERTY_FOR_COLUMN_NOT_ADDED).build().buildException();
            }
            Map<String, Object> commonProps = allFamiliesProps.get(f);
            Map<String, Object> stmtProps = stmtFamiliesPropsMap.get(f);
            if (commonProps != null) {
                if (stmtProps != null) {
                    // merge common props with statement props for the family
                    commonProps.putAll(stmtProps);
                }
            } else {
                // if no common props were specified, then assign family specific props
                if (stmtProps != null) {
                    allFamiliesProps.put(f, stmtProps);
                }
            }
        }

        // case when there is a column family being added but there are no props
        // For ex - in DROP COLUMN when a new empty CF needs to be added since all
        // the columns of the existing empty CF are getting dropped. Or the case
        // when one is just adding a column for a column family like this:
        // ALTER TABLE ADD CF.COL
        for (String cf : colFamiliesForPColumnsToBeAdded) {
            if (cf != null && allFamiliesProps.get(cf) == null) {
                allFamiliesProps.put(cf, new HashMap<String, Object>());
            }
        }

        if (table.getColumnFamilies().isEmpty() && !addingColumns && !commonFamilyProps.isEmpty()) {
            allFamiliesProps.put(Bytes.toString(table.getDefaultFamilyName() == null ? QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES : table.getDefaultFamilyName().getBytes() ), commonFamilyProps);
        }

        // Views are not allowed to have any of these properties.
        if (table.getType() == PTableType.VIEW && (!stmtFamiliesPropsMap.isEmpty() || !commonFamilyProps.isEmpty() || !tableProps.isEmpty())) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.VIEW_WITH_PROPERTIES).build()
            .buildException();
        }

        TableDescriptorBuilder newTableDescriptorBuilder = null;
        TableDescriptor origTableDescriptor = null;
        // Store all old to new table descriptor mappings for the table as well as its global indexes
        Map<TableDescriptor, TableDescriptor> tableAndIndexDescriptorMappings = Collections.emptyMap();
        if (!allFamiliesProps.isEmpty() || !tableProps.isEmpty()) {
            tableAndIndexDescriptorMappings = Maps.newHashMapWithExpectedSize(3 + table.getIndexes().size());
            TableDescriptor existingTableDescriptor = origTableDescriptor = this.getTableDescriptor(table.getPhysicalName().getBytes());
            newTableDescriptorBuilder = TableDescriptorBuilder.newBuilder(existingTableDescriptor);
            if (!tableProps.isEmpty()) {
                // add all the table properties to the new table descriptor
                for (Entry<String, Object> entry : tableProps.entrySet()) {
                    newTableDescriptorBuilder.setValue(entry.getKey(), entry.getValue() != null ? entry.getValue().toString() : null);
                }
            }
            if (addingColumns) {
                // Make sure that TTL, KEEP_DELETED_CELLS and REPLICATION_SCOPE for the new column family to be added stays in sync
                // with the table's existing column families. Note that we use the new values for these properties in case we are
                // altering their values. We also propagate these altered values to existing column families and indexes on the table below
                setSyncedPropsForNewColumnFamilies(allFamiliesProps, table, newTableDescriptorBuilder, newTTL, newKeepDeletedCells, newReplicationScope);
            }
            if (newTTL != null || newKeepDeletedCells != null || newReplicationScope != null) {
                // Set properties to be kept in sync on all table column families of this table, even if they are not referenced here
                setSyncedPropsForUnreferencedColumnFamilies(this.getTableDescriptor(table.getPhysicalName().getBytes()),
                        allFamiliesProps, newTTL, newKeepDeletedCells, newReplicationScope);
            }

            Integer defaultTxMaxVersions = null;
            if (isOrWillBeTransactional) {
                // Calculate default for max versions
                Map<String, Object> emptyFamilyProps = allFamiliesProps.get(SchemaUtil.getEmptyColumnFamilyAsString(table));
                if (emptyFamilyProps != null) {
                    defaultTxMaxVersions = (Integer)emptyFamilyProps.get(MAX_VERSIONS);
                }
                if (defaultTxMaxVersions == null) {
                    if (isTransactional) {
                        defaultTxMaxVersions = newTableDescriptorBuilder.build()
                                .getColumnFamily(SchemaUtil.getEmptyColumnFamily(table)).getMaxVersions();
                    } else {
                        defaultTxMaxVersions =
                                this.getProps().getInt(
                                        QueryServices.MAX_VERSIONS_TRANSACTIONAL_ATTRIB,
                                        QueryServicesOptions.DEFAULT_MAX_VERSIONS_TRANSACTIONAL);
                    }
                }
                if (willBeTransactional) {
                    // Set VERSIONS for all column families when transitioning to transactional
                    for (PColumnFamily family : table.getColumnFamilies()) {
                        if (!allFamiliesProps.containsKey(family.getName().getString())) {
                            Map<String,Object> familyProps = Maps.newHashMapWithExpectedSize(1);
                            familyProps.put(MAX_VERSIONS, defaultTxMaxVersions);
                            allFamiliesProps.put(family.getName().getString(), familyProps);
                        }
                    }
                }
                // Set transaction context TTL property based on HBase property if we're
                // transitioning to become transactional or setting TTL on
                // an already transactional table.
                int ttl = getTTL(table, newTableDescriptorBuilder.build(), newTTL);
                if (ttl != ColumnFamilyDescriptorBuilder.DEFAULT_TTL) {
                    for (Map.Entry<String, Map<String, Object>> entry : allFamiliesProps.entrySet()) {
                        Map<String, Object> props = entry.getValue();
                        if (props == null) {
                            allFamiliesProps.put(entry.getKey(), new HashMap<>());
                            props = allFamiliesProps.get(entry.getKey());
                        } else {
                            props = new HashMap<>(props);
                        }
                        // Note: After PHOENIX-6627, is PhoenixTransactionContext.PROPERTY_TTL still useful?
                        props.put(PhoenixTransactionContext.PROPERTY_TTL, ttl);
                        // Remove HBase TTL if we're not transitioning an existing table to become transactional
                        // or if the existing transactional table wasn't originally non transactional.
                        if (!willBeTransactional && !Boolean.valueOf(newTableDescriptorBuilder.build().getValue(PhoenixTransactionContext.READ_NON_TX_DATA))) {
                            props.remove(TTL);
                        }
                        entry.setValue(props);
                    }
                }
            }
            for (Entry<String, Map<String, Object>> entry : allFamiliesProps.entrySet()) {
                Map<String,Object> familyProps = entry.getValue();
                if (isOrWillBeTransactional) {
                    if (!familyProps.containsKey(MAX_VERSIONS)) {
                        familyProps.put(MAX_VERSIONS, defaultTxMaxVersions);
                    }
                }
                byte[] cf = Bytes.toBytes(entry.getKey());
                ColumnFamilyDescriptor colDescriptor = newTableDescriptorBuilder.build().getColumnFamily(cf);
                if (colDescriptor == null) {
                    // new column family
                    colDescriptor = generateColumnFamilyDescriptor(new Pair<>(cf, familyProps), table.getType());
                    newTableDescriptorBuilder.setColumnFamily(colDescriptor);
                } else {
                    ColumnFamilyDescriptorBuilder colDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(colDescriptor);
                    modifyColumnFamilyDescriptor(colDescriptorBuilder, familyProps);
                    colDescriptor = colDescriptorBuilder.build();
                    newTableDescriptorBuilder.removeColumnFamily(cf);
                    newTableDescriptorBuilder.setColumnFamily(colDescriptor);
                }
                if (isOrWillBeTransactional) {
                    checkTransactionalVersionsValue(colDescriptor);
                }
            }
        }
        if (origTableDescriptor != null && newTableDescriptorBuilder != null) {
            // Add the table descriptor mapping for the base table
            tableAndIndexDescriptorMappings.put(origTableDescriptor, newTableDescriptorBuilder.build());
        }

        Map<String, Object> applyPropsToAllIndexColFams = getNewSyncedPropsMap(newTTL, newKeepDeletedCells, newReplicationScope);
        // Copy properties that need to be synced from the default column family of the base table to
        // the column families of each of its indexes (including indexes on this base table's views)
        // and store those table descriptor mappings as well
        setSyncedPropertiesForTableIndexes(table, tableAndIndexDescriptorMappings, applyPropsToAllIndexColFams);
        return tableAndIndexDescriptorMappings;
    }

    private void checkTransactionalVersionsValue(ColumnFamilyDescriptor colDescriptor) throws SQLException {
        int maxVersions = colDescriptor.getMaxVersions();
        if (maxVersions <= 1) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.TX_MAX_VERSIONS_MUST_BE_GREATER_THAN_ONE)
            .setFamilyName(colDescriptor.getNameAsString())
            .build().buildException();
        }
    }

    private HashSet<String> existingColumnFamiliesForBaseTable(PName baseTableName) throws TableNotFoundException {
        throwConnectionClosedIfNullMetaData();
        PTable table = latestMetaData.getTableRef(new PTableKey(null, baseTableName.getString())).getTable();
        return existingColumnFamilies(table);
    }

    public HashSet<String> existingColumnFamilies(PTable table) {
        List<PColumnFamily> cfs = table.getColumnFamilies();
        HashSet<String> cfNames = new HashSet<>(cfs.size());
        for (PColumnFamily cf : table.getColumnFamilies()) {
            cfNames.add(cf.getName().getString());
        }
        return cfNames;
    }

    public static KeepDeletedCells getKeepDeletedCells(PTable table, TableDescriptor tableDesc,
            KeepDeletedCells newKeepDeletedCells) throws SQLException {
        // If we're setting KEEP_DELETED_CELLS now, then use that value. Otherwise, use the empty column family value
        return (newKeepDeletedCells != null) ?
                newKeepDeletedCells :
                tableDesc.getColumnFamily(SchemaUtil.getEmptyColumnFamily(table)).getKeepDeletedCells();
    }

    public static int getReplicationScope(PTable table, TableDescriptor tableDesc,
            Integer newReplicationScope) throws SQLException {
        // If we're setting replication scope now, then use that value. Otherwise, use the empty column family value
        return (newReplicationScope != null) ?
                newReplicationScope :
                tableDesc.getColumnFamily(SchemaUtil.getEmptyColumnFamily(table)).getScope();
    }

    public static int getTTL(PTable table, TableDescriptor tableDesc, Integer newTTL) throws SQLException {
        // If we're setting TTL now, then use that value. Otherwise, use empty column family value
        return (newTTL != null) ?
                newTTL :
                tableDesc.getColumnFamily(SchemaUtil.getEmptyColumnFamily(table)).getTimeToLive();
    }

    /**
     * Keep the TTL, KEEP_DELETED_CELLS and REPLICATION_SCOPE properties of new column families
     * in sync with the existing column families. Note that we use the new values for these properties in case they
     * are passed from our alter table command, if not, we use the default column family's value for each property
     * See {@link MetaDataUtil#SYNCED_DATA_TABLE_AND_INDEX_COL_FAM_PROPERTIES}
     * @param allFamiliesProps Map of all column family properties
     * @param table original table
     * @param tableDescBuilder new table descriptor builder
     * @param newTTL new value of TTL
     * @param newKeepDeletedCells new value of KEEP_DELETED_CELLS
     * @param newReplicationScope new value of REPLICATION_SCOPE
     * @throws SQLException
     */
    private void setSyncedPropsForNewColumnFamilies(Map<String, Map<String, Object>> allFamiliesProps, PTable table,
            TableDescriptorBuilder tableDescBuilder, Integer newTTL, KeepDeletedCells newKeepDeletedCells,
            Integer newReplicationScope) throws SQLException {
        if (!allFamiliesProps.isEmpty()) {
            int ttl = getTTL(table, tableDescBuilder.build(), newTTL);
            int replicationScope = getReplicationScope(table, tableDescBuilder.build(), newReplicationScope);
            KeepDeletedCells keepDeletedCells = getKeepDeletedCells(table, tableDescBuilder.build(), newKeepDeletedCells);
            for (Map.Entry<String, Map<String, Object>> entry : allFamiliesProps.entrySet()) {
                Map<String, Object> props = entry.getValue();
                if (props == null) {
                    allFamiliesProps.put(entry.getKey(), new HashMap<>());
                    props = allFamiliesProps.get(entry.getKey());
                }
                props.put(TTL, ttl);
                props.put(KEEP_DELETED_CELLS, keepDeletedCells);
                props.put(REPLICATION_SCOPE, replicationScope);
            }
        }
    }

    private void setPropIfNotNull(Map<String, Object> propMap, String propName, Object propVal) {
        if (propName!= null && propVal != null) {
            propMap.put(propName, propVal);
        }
    }

    private Map<String, Object> getNewSyncedPropsMap(Integer newTTL, KeepDeletedCells newKeepDeletedCells, Integer newReplicationScope) {
        Map<String,Object> newSyncedProps = Maps.newHashMapWithExpectedSize(3);
        setPropIfNotNull(newSyncedProps, TTL, newTTL);
        setPropIfNotNull(newSyncedProps,KEEP_DELETED_CELLS, newKeepDeletedCells);
        setPropIfNotNull(newSyncedProps, REPLICATION_SCOPE, newReplicationScope);
        return newSyncedProps;
    }

    /**
     * Set the new values for properties that are to be kept in sync amongst those column families of the table which are
     * not referenced in the context of our alter table command, including the local index column family if it exists
     * See {@link MetaDataUtil#SYNCED_DATA_TABLE_AND_INDEX_COL_FAM_PROPERTIES}
     * @param tableDesc original table descriptor
     * @param allFamiliesProps Map of all column family properties
     * @param newTTL new value of TTL
     * @param newKeepDeletedCells new value of KEEP_DELETED_CELLS
     * @param newReplicationScope new value of REPLICATION_SCOPE
     * @return
     */
    private void setSyncedPropsForUnreferencedColumnFamilies(TableDescriptor tableDesc, Map<String, Map<String, Object>> allFamiliesProps,
            Integer newTTL, KeepDeletedCells newKeepDeletedCells, Integer newReplicationScope) {
        for (ColumnFamilyDescriptor family: tableDesc.getColumnFamilies()) {
            if (!allFamiliesProps.containsKey(family.getNameAsString())) {
                allFamiliesProps.put(family.getNameAsString(),
                        getNewSyncedPropsMap(newTTL, newKeepDeletedCells, newReplicationScope));
            }
        }
    }

    /**
     * Set properties to be kept in sync for global indexes of a table, as well as
     * the physical table corresponding to indexes created on views of a table
     * See {@link MetaDataUtil#SYNCED_DATA_TABLE_AND_INDEX_COL_FAM_PROPERTIES} and
     * @param table base table
     * @param tableAndIndexDescriptorMappings old to new table descriptor mappings
     * @param applyPropsToAllIndexesDefaultCF new properties to apply to all index column families
     * @throws SQLException
     */
    private void setSyncedPropertiesForTableIndexes(PTable table,
            Map<TableDescriptor, TableDescriptor> tableAndIndexDescriptorMappings,
            Map<String, Object> applyPropsToAllIndexesDefaultCF) throws SQLException {
        if (applyPropsToAllIndexesDefaultCF == null || applyPropsToAllIndexesDefaultCF.isEmpty()) {
            return;
        }

        for (PTable indexTable: table.getIndexes()) {
            if (indexTable.getIndexType() == PTable.IndexType.LOCAL) {
                // local index tables are already handled when we sync all column families of a base table
                continue;
            }
            TableDescriptor origIndexDescriptor = this.getTableDescriptor(indexTable.getPhysicalName().getBytes());
            TableDescriptorBuilder newIndexDescriptorBuilder = TableDescriptorBuilder.newBuilder(origIndexDescriptor);

            byte[] defaultIndexColFam = SchemaUtil.getEmptyColumnFamily(indexTable);
            ColumnFamilyDescriptorBuilder indexDefaultColDescriptorBuilder =
                    ColumnFamilyDescriptorBuilder.newBuilder(origIndexDescriptor.getColumnFamily(defaultIndexColFam));
            modifyColumnFamilyDescriptor(indexDefaultColDescriptorBuilder, applyPropsToAllIndexesDefaultCF);
            newIndexDescriptorBuilder.removeColumnFamily(defaultIndexColFam);
            newIndexDescriptorBuilder.setColumnFamily(indexDefaultColDescriptorBuilder.build());
            tableAndIndexDescriptorMappings.put(origIndexDescriptor, newIndexDescriptorBuilder.build());
        }
        // Also keep properties for the physical view index table in sync
        String viewIndexName = MetaDataUtil.getViewIndexPhysicalName(table.getName(), table.isNamespaceMapped());
        if (!Strings.isNullOrEmpty(viewIndexName)) {
            try {
                TableDescriptor origViewIndexTableDescriptor = this.getTableDescriptor(Bytes.toBytes(viewIndexName));
                TableDescriptorBuilder newViewIndexDescriptorBuilder =
                        TableDescriptorBuilder.newBuilder(origViewIndexTableDescriptor);
                for (ColumnFamilyDescriptor cfd: origViewIndexTableDescriptor.getColumnFamilies()) {
                    ColumnFamilyDescriptorBuilder newCfd =
                            ColumnFamilyDescriptorBuilder.newBuilder(cfd);
                    modifyColumnFamilyDescriptor(newCfd, applyPropsToAllIndexesDefaultCF);
                    newViewIndexDescriptorBuilder.removeColumnFamily(cfd.getName());
                    newViewIndexDescriptorBuilder.setColumnFamily(newCfd.build());
                }
                tableAndIndexDescriptorMappings.put(origViewIndexTableDescriptor, newViewIndexDescriptorBuilder.build());
            } catch (TableNotFoundException ignore) {
                // Ignore since this means that a view index table does not exist for this table
            }
        }
    }

    @Override
    public MetaDataMutationResult dropColumn(final List<Mutation> tableMetaData,
                                             final PTableType tableType,
                                             final PTable parentTable) throws SQLException {
        byte[][] rowKeyMetadata = new byte[3][];
        SchemaUtil.getVarChars(tableMetaData.get(0).getRow(), rowKeyMetadata);
        byte[] tenantIdBytes = rowKeyMetadata[PhoenixDatabaseMetaData.TENANT_ID_INDEX];
        byte[] schemaBytes = rowKeyMetadata[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
        byte[] tableBytes = rowKeyMetadata[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
        byte[] tableKey = SchemaUtil.getTableKey(tenantIdBytes, schemaBytes, tableBytes);
        MetaDataMutationResult result = metaDataCoprocessorExec(
                SchemaUtil.getPhysicalHBaseTableName(schemaBytes, tableBytes,
                        SchemaUtil.isNamespaceMappingEnabled(PTableType.SYSTEM, this.props)).toString(),
                tableKey,
                new Batch.Call<MetaDataService, MetaDataResponse>() {
            @Override
            public MetaDataResponse call(MetaDataService instance) throws IOException {
                RpcController controller = getController();
                BlockingRpcCallback<MetaDataResponse> rpcCallback =
                        new BlockingRpcCallback<MetaDataResponse>();
                DropColumnRequest.Builder builder = DropColumnRequest.newBuilder();
                for (Mutation m : tableMetaData) {
                    MutationProto mp = ProtobufUtil.toProto(m);
                    builder.addTableMetadataMutations(mp.toByteString());
                }
                builder.setClientVersion(VersionUtil.encodeVersion(PHOENIX_MAJOR_VERSION, PHOENIX_MINOR_VERSION, PHOENIX_PATCH_NUMBER));
                if (parentTable!=null)
                    builder.setParentTable(PTableImpl.toProto(parentTable));
                instance.dropColumn(controller, builder.build(), rpcCallback);
                checkForRemoteExceptions(controller);
                return rpcCallback.get();
            }
        });
        final MutationCode code = result.getMutationCode();
        switch(code) {
        case TABLE_ALREADY_EXISTS:
            final ReadOnlyProps props = this.getProps();
            final boolean dropMetadata = props.getBoolean(DROP_METADATA_ATTRIB, DEFAULT_DROP_METADATA);
            if (dropMetadata) {
                dropTables(result.getTableNamesToDelete());
            } else {
                invalidateTableStats(result.getTableNamesToDelete());
            }
            break;
        default:
            break;
        }
        return result;

    }

    private PhoenixConnection removeNotNullConstraint(PhoenixConnection oldMetaConnection, String schemaName, String tableName, long timestamp, String columnName) throws SQLException {
        Properties props = PropertiesUtil.deepCopy(oldMetaConnection.getClientInfo());
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(timestamp));
        // Cannot go through DriverManager or you end up in an infinite loop because it'll call init again
        PhoenixConnection metaConnection = new PhoenixConnection(oldMetaConnection, this, props);
        SQLException sqlE = null;
        try {
            String dml = "UPSERT INTO " + SYSTEM_CATALOG_NAME + " (" + PhoenixDatabaseMetaData.TENANT_ID + ","
                    + PhoenixDatabaseMetaData.TABLE_SCHEM + "," + PhoenixDatabaseMetaData.TABLE_NAME + ","
                    + PhoenixDatabaseMetaData.COLUMN_NAME + ","
                    + PhoenixDatabaseMetaData.NULLABLE + ") VALUES (null, ?, ?, ?, ?)";
            PreparedStatement stmt = metaConnection.prepareStatement(dml);
            stmt.setString(1, schemaName);
            stmt.setString(2, tableName);
            stmt.setString(3, columnName);
            stmt.setInt(4, ResultSetMetaData.columnNullable);
            stmt.executeUpdate();
            metaConnection.commit();
        } catch (NewerTableAlreadyExistsException e) {
            LOGGER.warn("Table already modified at this timestamp," +
                    " so assuming column already nullable: " + columnName);
        } catch (SQLException e) {
            LOGGER.warn("Add column failed due to:" + e);
            sqlE = e;
        } finally {
            try {
                oldMetaConnection.close();
            } catch (SQLException e) {
                if (sqlE != null) {
                    sqlE.setNextException(e);
                } else {
                    sqlE = e;
                }
            }
            if (sqlE != null) {
                throw sqlE;
            }
        }
        return metaConnection;
    }
    /**
     * This closes the passed connection.
     */
    private PhoenixConnection addColumn(PhoenixConnection oldMetaConnection, String tableName, long timestamp, String columns, boolean addIfNotExists) throws SQLException {
        Properties props = PropertiesUtil.deepCopy(oldMetaConnection.getClientInfo());
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(timestamp));
        // Cannot go through DriverManager or you end up in an infinite loop because it'll call init again
        PhoenixConnection metaConnection = new PhoenixConnection(oldMetaConnection, this, props);
        SQLException sqlE = null;
        try {
            metaConnection.createStatement().executeUpdate("ALTER TABLE " + tableName + " ADD " + (addIfNotExists ? " IF NOT EXISTS " : "") + columns );
        } catch (NewerTableAlreadyExistsException e) {
            LOGGER.warn("Table already modified at this timestamp," +
                    " so assuming add of these columns already done: " + columns);
        } catch (SQLException e) {
            LOGGER.warn("Add column failed due to:" + e);
            sqlE = e;
        } finally {
            try {
                oldMetaConnection.close();
            } catch (SQLException e) {
                if (sqlE != null) {
                    sqlE.setNextException(e);
                } else {
                    sqlE = e;
                }
            }
            if (sqlE != null) {
                throw sqlE;
            }
        }
        return metaConnection;
    }

    /**
     * Keeping this to use for further upgrades. This method closes the oldMetaConnection.
     */
    private PhoenixConnection addColumnsIfNotExists(PhoenixConnection oldMetaConnection,
            String tableName, long timestamp, String columns) throws SQLException {
        return addColumn(oldMetaConnection, tableName, timestamp, columns, true);
    }

    // Available for testing
    protected long getSystemTableVersion() {
        return MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP;
    }
    

    // Available for testing
    protected void setUpgradeRequired() {
        this.upgradeRequired.set(true);
    }
    
    // Available for testing
    protected boolean isInitialized() {
        return initialized;
    }
    
    // Available for testing
    protected void setInitialized(boolean isInitialized) {
        initialized = isInitialized;
    }

    // Available for testing
    protected String getSystemCatalogTableDDL() {
        return setSystemDDLProperties(QueryConstants.CREATE_TABLE_METADATA);
    }

    protected String getSystemSequenceTableDDL(int nSaltBuckets) {
        String schema = String.format(setSystemDDLProperties(QueryConstants.CREATE_SEQUENCE_METADATA));
        return Sequence.getCreateTableStatement(schema, nSaltBuckets);
    }

    // Available for testing
    protected String getFunctionTableDDL() {
        return setSystemDDLProperties(QueryConstants.CREATE_FUNCTION_METADATA);
    }

    // Available for testing
    protected String getLogTableDDL() {
        return setSystemLogDDLProperties(QueryConstants.CREATE_LOG_METADATA);
    }

    private String setSystemLogDDLProperties(String ddl) {
        return String.format(ddl, props.getInt(LOG_SALT_BUCKETS_ATTRIB, QueryServicesOptions.DEFAULT_LOG_SALT_BUCKETS));

    }

    // Available for testing
    protected String getChildLinkDDL() {
        return setSystemDDLProperties(QueryConstants.CREATE_CHILD_LINK_METADATA);
    }

    protected String getMutexDDL() {
        return setSystemDDLProperties(QueryConstants.CREATE_MUTEX_METADATA);
    }

    protected String getTaskDDL() {
        return setSystemDDLProperties(QueryConstants.CREATE_TASK_METADATA);
    }

    protected String getTransformDDL() {
        return setSystemDDLProperties(QueryConstants.CREATE_TRANSFORM_METADATA);
    }

    private String setSystemDDLProperties(String ddl) {
        return String.format(ddl,
          props.getInt(DEFAULT_SYSTEM_MAX_VERSIONS_ATTRIB, QueryServicesOptions.DEFAULT_SYSTEM_MAX_VERSIONS),
          props.getBoolean(DEFAULT_SYSTEM_KEEP_DELETED_CELLS_ATTRIB, QueryServicesOptions.DEFAULT_SYSTEM_KEEP_DELETED_CELLS));
    }

    @Override
    public void init(final String url, final Properties props) throws SQLException {
        try {
            PhoenixContextExecutor.call(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    if (isInitialized()) {
                        if (initializationException != null) {
                            // Throw previous initialization exception, as we won't resuse this instance
                            throw initializationException;
                        }
                        return null;
                    }
                    synchronized (ConnectionQueryServicesImpl.this) {
                        if (isInitialized()) {
                            if (initializationException != null) {
                                // Throw previous initialization exception, as we won't resuse this instance
                                throw initializationException;
                            }
                            return null;
                        }

                        checkClosed();
                        boolean hConnectionEstablished = false;
                        boolean success = false;
                        try {
                            GLOBAL_QUERY_SERVICES_COUNTER.increment();
                            LOGGER.info("An instance of ConnectionQueryServices was created.");
                            openConnection();
                            hConnectionEstablished = true;
                            tableStatsCache =
                                    (new GuidePostsCacheProvider()).getGuidePostsCache(
                                        props.getProperty(GUIDE_POSTS_CACHE_FACTORY_CLASS,
                                            QueryServicesOptions.DEFAULT_GUIDE_POSTS_CACHE_FACTORY_CLASS),
                                        ConnectionQueryServicesImpl.this, config);
                            String skipSystemExistenceCheck =
                                props.getProperty(SKIP_SYSTEM_TABLES_EXISTENCE_CHECK);
                            if (skipSystemExistenceCheck != null &&
                                Boolean.valueOf(skipSystemExistenceCheck)) {
                                initialized = true;
                                success = true;
                                return null;
                            }
                            nSequenceSaltBuckets = ConnectionQueryServicesImpl.this.props.getInt(
                                    QueryServices.SEQUENCE_SALT_BUCKETS_ATTRIB,
                                    QueryServicesOptions.DEFAULT_SEQUENCE_TABLE_SALT_BUCKETS);
                            boolean isDoNotUpgradePropSet = UpgradeUtil.isNoUpgradeSet(props);
                            Properties scnProps = PropertiesUtil.deepCopy(props);
                            scnProps.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
                                    Long.toString(getSystemTableVersion()));
                            scnProps.remove(PhoenixRuntime.TENANT_ID_ATTRIB);
                            String globalUrl = JDBCUtil.removeProperty(url, PhoenixRuntime.TENANT_ID_ATTRIB);
                            try (PhoenixConnection metaConnection = new PhoenixConnection(ConnectionQueryServicesImpl.this, globalUrl,
                                         scnProps)) {
                                try (Statement statement =
                                        metaConnection.createStatement()) {
                                    metaConnection.setRunningUpgrade(true);
                                    statement.executeUpdate(
                                        getSystemCatalogTableDDL());
                                } catch (NewerTableAlreadyExistsException ignore) {
                                    // Ignore, as this will happen if the SYSTEM.CATALOG already exists at this fixed
                                    // timestamp. A TableAlreadyExistsException is not thrown, since the table only exists
                                    // *after* this fixed timestamp.
                                } catch (TableAlreadyExistsException e) {
                                    long currentServerSideTableTimeStamp = e.getTable().getTimeStamp();
                                    if (currentServerSideTableTimeStamp < MIN_SYSTEM_TABLE_TIMESTAMP) {
                                        setUpgradeRequired();
                                    }
                                } catch (PhoenixIOException e) {
                                    boolean foundAccessDeniedException = false;
                                    // when running spark/map reduce jobs the ADE might be wrapped
                                    // in a RemoteException
                                    if (inspectIfAnyExceptionInChain(e, Collections
                                            .<Class<? extends Exception>> singletonList(AccessDeniedException.class))) {
                                        // Pass
                                        LOGGER.warn("Could not check for Phoenix SYSTEM tables," +
                                                " assuming they exist and are properly configured");
                                        checkClientServerCompatibility(SchemaUtil.getPhysicalName(SYSTEM_CATALOG_NAME_BYTES, getProps()).getName());
                                        success = true;
                                    } else if (inspectIfAnyExceptionInChain(e,
                                            Collections.<Class<? extends Exception>> singletonList(
                                                    NamespaceNotFoundException.class))) {
                                        // This exception is only possible if SYSTEM namespace mapping is enabled and SYSTEM namespace is missing
                                        // It implies that SYSTEM tables are not created and hence we shouldn't provide a connection
                                        AccessDeniedException ade = new AccessDeniedException("Insufficient permissions to create SYSTEM namespace and SYSTEM Tables");
                                        initializationException = ClientUtil.parseServerException(ade);
                                    } else {
                                        initializationException = e;
                                    }
                                    return null;
                                } catch (UpgradeRequiredException e) {
                                    // This will occur in 2 cases:
                                    // 1. when SYSTEM.CATALOG doesn't exists
                                    // 2. when SYSTEM.CATALOG exists, but client and
                                    // server-side namespace mapping is enabled so
                                    // we need to migrate SYSTEM tables to the SYSTEM namespace
                                    setUpgradeRequired();
                                }

                                if (!ConnectionQueryServicesImpl.this.upgradeRequired.get()) {
                                    if (!isDoNotUpgradePropSet) {
                                        createOtherSystemTables(metaConnection);
                                        // In case namespace mapping is enabled and system table to
                                        // system namespace mapping is also enabled, create an entry
                                        // for the SYSTEM namespace in the SYSCAT table, so that
                                        // GRANT/REVOKE commands can work with SYSTEM Namespace
                                        createSchemaIfNotExistsSystemNSMappingEnabled(metaConnection);
                                    }
                                } else if (isAutoUpgradeEnabled && !isDoNotUpgradePropSet) {
                                    // Upgrade is required and we are allowed to automatically upgrade
                                    upgradeSystemTables(url, props);
                                } else {
                                    // We expect the user to manually run the "EXECUTE UPGRADE" command first.
                                    LOGGER.error("Upgrade is required. Must run 'EXECUTE UPGRADE' "
                                            + "before any other command");
                                }
                            }
                            success = true;
                        } catch (RetriableUpgradeException e) {
                            // Set success to true and don't set the exception as an initializationException,
                            // because otherwise the client won't be able to retry establishing the connection.
                            success = true;
                            throw e;
                        } catch (Exception e) {
                            if (e instanceof SQLException) {
                                initializationException = (SQLException) e;
                            } else {
                                // wrap every other exception into a SQLException
                                initializationException = new SQLException(e);
                            }
                        } finally {
                            if (success) {
                                scheduleRenewLeaseTasks();
                            }
                            try {
                                if (!success && hConnectionEstablished) {
                                    closeConnection();
                                }
                            } catch (IOException e) {
                                SQLException ex = new SQLException(e);
                                if (initializationException != null) {
                                    initializationException.setNextException(ex);
                                } else {
                                    initializationException = ex;
                                }
                            } finally {
                                try {
                                    if (initializationException != null) {
                                        throw initializationException;
                                    }
                                } finally {
                                    setInitialized(true);
                                }
                            }
                        }
                    }
                    return null;
                }
            });
        } catch (Exception e) {
            Throwables.propagateIfInstanceOf(e, SQLException.class);
            Throwables.propagate(e);
        }
    }

    void createSysMutexTableIfNotExists(Admin admin) throws IOException {
        try {
            if (checkIfSysMutexExistsAndModifyTTLIfRequired(admin)) {
                return;
            }
            final TableName mutexTableName = SchemaUtil.getPhysicalTableName(
                    SYSTEM_MUTEX_NAME, props);
            TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(mutexTableName)
                    .setColumnFamily(ColumnFamilyDescriptorBuilder
                            .newBuilder(PhoenixDatabaseMetaData.SYSTEM_MUTEX_FAMILY_NAME_BYTES)
                            .setTimeToLive(TTL_FOR_MUTEX).build())
                    .build();
            admin.createTable(tableDesc);
        }
        catch (IOException e) {
            if (inspectIfAnyExceptionInChain(e, Arrays.<Class<? extends Exception>> asList(
                    AccessDeniedException.class, org.apache.hadoop.hbase.TableExistsException.class))) {
                // Ignore TableExistsException as another client might beat us during upgrade.
                // Ignore AccessDeniedException, as it may be possible underpriviliged user trying to use the connection
                // which doesn't required upgrade.
                LOGGER.debug("Ignoring exception while creating mutex table" +
                        " during connection initialization: "
                        + Throwables.getStackTraceAsString(e));
            } else {
                throw e;
            }
        }
    }

    /**
     * Check if the SYSTEM MUTEX table exists. If it does, ensure that its TTL is correct and if
     * not, modify its table descriptor
     * @param admin HBase admin
     * @return true if SYSTEM MUTEX exists already and false if it needs to be created
     * @throws IOException thrown if there is an error getting the table descriptor
     */
    @VisibleForTesting
    boolean checkIfSysMutexExistsAndModifyTTLIfRequired(Admin admin) throws IOException {
        TableDescriptor htd;
        try {
            htd = admin.getDescriptor(TableName.valueOf(SYSTEM_MUTEX_NAME));
        } catch (org.apache.hadoop.hbase.TableNotFoundException ignored) {
            try {
                // Try with the namespace mapping name
                htd = admin.getDescriptor(TableName.valueOf(SYSTEM_SCHEMA_NAME,
                        SYSTEM_MUTEX_TABLE_NAME));
            } catch (org.apache.hadoop.hbase.TableNotFoundException ignored2) {
                return false;
            }
        }

        // The SYSTEM MUTEX table already exists so check its TTL
        if (htd.getColumnFamily(SYSTEM_MUTEX_FAMILY_NAME_BYTES).getTimeToLive() != TTL_FOR_MUTEX) {
            LOGGER.debug("SYSTEM MUTEX already appears to exist, but has the wrong TTL. " +
                    "Will modify the TTL");
            ColumnFamilyDescriptor hColFamDesc = ColumnFamilyDescriptorBuilder
                    .newBuilder(htd.getColumnFamily(SYSTEM_MUTEX_FAMILY_NAME_BYTES))
                    .setTimeToLive(TTL_FOR_MUTEX)
                    .build();
            htd = TableDescriptorBuilder
                    .newBuilder(htd)
                    .modifyColumnFamily(hColFamDesc)
                    .build();
            admin.modifyTable(htd);
        } else {
            LOGGER.debug("SYSTEM MUTEX already appears to exist with the correct TTL, " +
                    "not creating it");
        }
        return true;
    }

    private boolean inspectIfAnyExceptionInChain(Throwable io, List<Class<? extends Exception>> ioList) {
        boolean exceptionToIgnore = false;
        for (Throwable t : Throwables.getCausalChain(io)) {
            for (Class<? extends Exception> exception : ioList) {
                exceptionToIgnore |= isExceptionInstanceOf(t, exception);
            }
            if (exceptionToIgnore) {
                break;
            }

        }
        return exceptionToIgnore;
    }

    private boolean isExceptionInstanceOf(Throwable io, Class<? extends Exception> exception) {
        return exception.isInstance(io) || (io instanceof RemoteException
                && (((RemoteException)io).getClassName().equals(exception.getName())));
    }

    List<TableName> getSystemTableNamesInDefaultNamespace(Admin admin) throws IOException {
        return Lists.newArrayList(admin.listTableNames(Pattern.compile(QueryConstants.SYSTEM_SCHEMA_NAME + "\\..*"))); // TODO: replace to pattern
    }

    private void createOtherSystemTables(PhoenixConnection metaConnection) throws SQLException, IOException {
        try {
            metaConnection.createStatement().execute(getSystemSequenceTableDDL(nSequenceSaltBuckets));
            // When creating the table above, DDL statements are
            // used. However, the CFD level properties are not set
            // via DDL commands, hence we are explicitly setting
            // few properties using the Admin API below.
            updateSystemSequenceWithCacheOnWriteProps(metaConnection);
        } catch (TableAlreadyExistsException e) {
            nSequenceSaltBuckets = getSaltBuckets(e);
        }
        try {
            metaConnection.createStatement().execute(QueryConstants.CREATE_STATS_TABLE_METADATA);
        } catch (TableAlreadyExistsException ignore) {}
        try {
            metaConnection.createStatement().execute(getFunctionTableDDL());
        } catch (TableAlreadyExistsException ignore) {}
        try {
            metaConnection.createStatement().execute(getLogTableDDL());
        } catch (TableAlreadyExistsException ignore) {}
        try {
            metaConnection.createStatement().executeUpdate(getChildLinkDDL());
        } catch (TableAlreadyExistsException ignore) {}
        try {
            metaConnection.createStatement().executeUpdate(getMutexDDL());
        } catch (TableAlreadyExistsException ignore) {}
        try {
            metaConnection.createStatement().executeUpdate(getTaskDDL());
        } catch (TableAlreadyExistsException ignore) {}
        try {
            metaConnection.createStatement().executeUpdate(getTransformDDL());
        } catch (TableAlreadyExistsException ignore) {}
    }

    /**
     * Create an entry for the SYSTEM namespace in the SYSCAT table in case namespace mapping is enabled and system table
     * to system namespace mapping is also enabled. If not enabled, this method returns immediately without doing anything
     * @param metaConnection
     * @throws SQLException
     */
    private void createSchemaIfNotExistsSystemNSMappingEnabled(PhoenixConnection metaConnection) throws SQLException {
        // HBase Namespace SYSTEM is assumed to be already created inside {@link ensureTableCreated(byte[], PTableType,
        // Map<String, Object>, List<Pair<byte[], Map<String, Object>>>, byte[][], boolean, boolean, boolean)}.
        // This statement will create an entry for the SYSTEM namespace in the SYSCAT table, so that GRANT/REVOKE
        // commands can work with SYSTEM Namespace. (See PHOENIX-4227 https://issues.apache.org/jira/browse/PHOENIX-4227)
        if (SchemaUtil.isNamespaceMappingEnabled(PTableType.SYSTEM,
          ConnectionQueryServicesImpl.this.getProps())) {
            try {
                metaConnection.createStatement().execute("CREATE SCHEMA IF NOT EXISTS "
                  + PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA);
            } catch (NewerSchemaAlreadyExistsException e) {
                // Older clients with appropriate perms may try getting a new connection
                // This results in NewerSchemaAlreadyExistsException, so we can safely ignore it here
            } catch (PhoenixIOException e) {
                if (!Iterables.isEmpty(Iterables.filter(Throwables.getCausalChain(e), AccessDeniedException.class))) {
                    // Ignore ADE
                } else {
                    throw e;
                }
            }
        }
    }

    /**
     * Upgrade the SYSCAT schema if required
     * @param metaConnection
     * @param currentServerSideTableTimeStamp
     * @return Phoenix connection object
     * @throws SQLException
     * @throws IOException
     * @throws TimeoutException
     * @throws InterruptedException
     */
    // Available for testing
    protected PhoenixConnection upgradeSystemCatalogIfRequired(PhoenixConnection metaConnection,
      long currentServerSideTableTimeStamp) throws SQLException, IOException, TimeoutException, InterruptedException {
        String columnsToAdd = "";
        // This will occur if we have an older SYSTEM.CATALOG and we need to update it to
        // include any new columns we've added.
        if (currentServerSideTableTimeStamp < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_3_0) {
            // We know that we always need to add the STORE_NULLS column for 4.3 release
            columnsToAdd = addColumn(columnsToAdd, PhoenixDatabaseMetaData.STORE_NULLS
              + " " + PBoolean.INSTANCE.getSqlTypeName());
            try (Admin admin = getAdmin()) {
                List<TableDescriptor> localIndexTables =
                        admin.listTableDescriptors(Pattern
                                .compile(MetaDataUtil.LOCAL_INDEX_TABLE_PREFIX + ".*"));
                for (TableDescriptor table : localIndexTables) {
                    if (table.getValue(MetaDataUtil.PARENT_TABLE_KEY) == null
                            && table.getValue(MetaDataUtil.IS_LOCAL_INDEX_TABLE_PROP_NAME) != null) {

                        table=TableDescriptorBuilder.newBuilder(table).setValue(Bytes.toBytes(MetaDataUtil.PARENT_TABLE_KEY),
                                Bytes.toBytes(MetaDataUtil.getLocalIndexUserTableName(table.getTableName().getNameAsString()))).build();
                        // Explicitly disable, modify and enable the table to ensure
                        // co-location of data and index regions. If we just modify the
                        // table descriptor when online schema change enabled may reopen
                        // the region in same region server instead of following data region.
                        admin.disableTable(table.getTableName());
                        admin.modifyTable(table);
                        admin.enableTable(table.getTableName());
                    }
                }
            }
        }

        // If the server side schema is before MIN_SYSTEM_TABLE_TIMESTAMP_4_1_0 then
        // we need to add INDEX_TYPE and INDEX_DISABLE_TIMESTAMP columns too.
        // TODO: Once https://issues.apache.org/jira/browse/PHOENIX-1614 is fixed,
        // we should just have a ALTER TABLE ADD IF NOT EXISTS statement with all
        // the column names that have been added to SYSTEM.CATALOG since 4.0.
        if (currentServerSideTableTimeStamp < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_1_0) {
            columnsToAdd = addColumn(columnsToAdd, PhoenixDatabaseMetaData.INDEX_TYPE + " "
              + PUnsignedTinyint.INSTANCE.getSqlTypeName() + ", "
              + PhoenixDatabaseMetaData.INDEX_DISABLE_TIMESTAMP + " "
              + PLong.INSTANCE.getSqlTypeName());
        }

        // If we have some new columns from 4.1-4.3 to add, add them now.
        if (!columnsToAdd.isEmpty()) {
            // Ugh..need to assign to another local variable to keep eclipse happy.
            PhoenixConnection newMetaConnection = addColumnsIfNotExists(metaConnection,
              PhoenixDatabaseMetaData.SYSTEM_CATALOG,
              MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_3_0, columnsToAdd);
            metaConnection = newMetaConnection;
        }

        if (currentServerSideTableTimeStamp < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_5_0) {
            columnsToAdd = PhoenixDatabaseMetaData.BASE_COLUMN_COUNT + " "
              + PInteger.INSTANCE.getSqlTypeName();
            try {
                metaConnection = addColumn(metaConnection,
                  PhoenixDatabaseMetaData.SYSTEM_CATALOG,
                  MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_5_0, columnsToAdd,
                  false);
                upgradeTo4_5_0(metaConnection);
            } catch (ColumnAlreadyExistsException ignored) {
                /*
                 * Upgrade to 4.5 is a slightly special case. We use the fact that the
                 * column BASE_COLUMN_COUNT is already part of the meta-data schema as the
                 * signal that the server side upgrade has finished or is in progress.
                 */
                LOGGER.debug("No need to run 4.5 upgrade");
            }
            Properties p = PropertiesUtil.deepCopy(metaConnection.getClientInfo());
            p.remove(PhoenixRuntime.CURRENT_SCN_ATTRIB);
            p.remove(PhoenixRuntime.TENANT_ID_ATTRIB);
            PhoenixConnection conn = new PhoenixConnection(
              ConnectionQueryServicesImpl.this, metaConnection.getURL(), p);
            try {
                List<String> tablesNeedingUpgrade = UpgradeUtil
                  .getPhysicalTablesWithDescRowKey(conn);
                if (!tablesNeedingUpgrade.isEmpty()) {
                    LOGGER.warn("The following tables require upgrade due to a bug " +
                            "causing the row key to be incorrect for descending columns " +
                            "and ascending BINARY columns (PHOENIX-2067 and PHOENIX-2120):\n"
                      + Joiner.on(' ').join(tablesNeedingUpgrade)
                      + "\nTo upgrade issue the \"bin/psql.py -u\" command.");
                }
                List<String> unsupportedTables = UpgradeUtil
                  .getPhysicalTablesWithDescVarbinaryRowKey(conn);
                if (!unsupportedTables.isEmpty()) {
                    LOGGER.warn("The following tables use an unsupported " +
                            "VARBINARY DESC construct and need to be changed:\n"
                      + Joiner.on(' ').join(unsupportedTables));
                }
            } catch (Exception ex) {
                LOGGER.error(
                  "Unable to determine tables requiring upgrade due to PHOENIX-2067",
                  ex);
            } finally {
                conn.close();
            }
        }
        // Add these columns one at a time so that if folks have run the upgrade code
        // already for a snapshot, we'll still enter this block (and do the parts we
        // haven't yet done).
        // Add each column with different timestamp else the code assumes that the
        // table is already modified at that timestamp resulting in not updating the
        // second column with same timestamp
        if (currentServerSideTableTimeStamp < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_6_0) {
            columnsToAdd = PhoenixDatabaseMetaData.IS_ROW_TIMESTAMP + " "
              + PBoolean.INSTANCE.getSqlTypeName();
            metaConnection = addColumnsIfNotExists(metaConnection,
              PhoenixDatabaseMetaData.SYSTEM_CATALOG,
              MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_6_0, columnsToAdd);
        }
        if (currentServerSideTableTimeStamp < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_7_0) {
            // Drop old stats table so that new stats table is created
            metaConnection = dropStatsTable(metaConnection,
              MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_7_0 - 4);
            metaConnection = addColumnsIfNotExists(
              metaConnection,
              PhoenixDatabaseMetaData.SYSTEM_CATALOG,
              MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_7_0 - 3,
              PhoenixDatabaseMetaData.TRANSACTIONAL + " "
                + PBoolean.INSTANCE.getSqlTypeName());
            metaConnection = addColumnsIfNotExists(
              metaConnection,
              PhoenixDatabaseMetaData.SYSTEM_CATALOG,
              MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_7_0 - 2,
              PhoenixDatabaseMetaData.UPDATE_CACHE_FREQUENCY + " "
                + PLong.INSTANCE.getSqlTypeName());
            metaConnection = setImmutableTableIndexesImmutable(metaConnection,
              MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_7_0 - 1);
            metaConnection = updateSystemCatalogTimestamp(metaConnection,
              MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_7_0);
            ConnectionQueryServicesImpl.this.removeTable(null,
              PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME, null,
              MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_7_0);
            clearCache();
        }

        if (currentServerSideTableTimeStamp < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_8_0) {
            metaConnection = addColumnsIfNotExists(
              metaConnection,
              PhoenixDatabaseMetaData.SYSTEM_CATALOG,
              MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_8_0 - 2,
              PhoenixDatabaseMetaData.IS_NAMESPACE_MAPPED + " "
                + PBoolean.INSTANCE.getSqlTypeName());
            metaConnection = addColumnsIfNotExists(
              metaConnection,
              PhoenixDatabaseMetaData.SYSTEM_CATALOG,
              MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_8_0 - 1,
              PhoenixDatabaseMetaData.AUTO_PARTITION_SEQ + " "
                + PVarchar.INSTANCE.getSqlTypeName());
            metaConnection = addColumnsIfNotExists(
              metaConnection,
              PhoenixDatabaseMetaData.SYSTEM_CATALOG,
              MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_8_0,
              PhoenixDatabaseMetaData.APPEND_ONLY_SCHEMA + " "
                + PBoolean.INSTANCE.getSqlTypeName());
            metaConnection = UpgradeUtil.disableViewIndexes(metaConnection);
            if (getProps().getBoolean(QueryServices.LOCAL_INDEX_CLIENT_UPGRADE_ATTRIB,
              QueryServicesOptions.DEFAULT_LOCAL_INDEX_CLIENT_UPGRADE)) {
                localIndexUpgradeRequired = true;
            }
            ConnectionQueryServicesImpl.this.removeTable(null,
              PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME, null,
              MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_8_0);
            clearCache();
        }
        if (currentServerSideTableTimeStamp < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_9_0) {
            metaConnection = addColumnsIfNotExists(
              metaConnection,
              PhoenixDatabaseMetaData.SYSTEM_CATALOG,
              MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_9_0,
              PhoenixDatabaseMetaData.GUIDE_POSTS_WIDTH + " "
                + PLong.INSTANCE.getSqlTypeName());
            ConnectionQueryServicesImpl.this.removeTable(null,
              PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME, null,
              MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_9_0);
            clearCache();
        }
        if (currentServerSideTableTimeStamp < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_10_0) {
            metaConnection = addColumnQualifierColumn(metaConnection, MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_10_0 - 3);
            metaConnection = addColumnsIfNotExists(
              metaConnection,
              PhoenixDatabaseMetaData.SYSTEM_CATALOG,
              MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_10_0 - 2,
              PhoenixDatabaseMetaData.IMMUTABLE_STORAGE_SCHEME + " "
                + PTinyint.INSTANCE.getSqlTypeName());
            metaConnection = addColumnsIfNotExists(
              metaConnection,
              PhoenixDatabaseMetaData.SYSTEM_CATALOG,
              MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_10_0 - 1,
              PhoenixDatabaseMetaData.ENCODING_SCHEME + " "
                + PTinyint.INSTANCE.getSqlTypeName());
            metaConnection = addColumnsIfNotExists(
              metaConnection,
              PhoenixDatabaseMetaData.SYSTEM_CATALOG,
              MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_10_0,
              PhoenixDatabaseMetaData.COLUMN_QUALIFIER_COUNTER + " "
                + PInteger.INSTANCE.getSqlTypeName());
            ConnectionQueryServicesImpl.this.removeTable(null,
              PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME, null,
              MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_10_0);
            clearCache();
        }
        if (currentServerSideTableTimeStamp < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_11_0) {
            metaConnection = addColumnsIfNotExists(
              metaConnection,
              PhoenixDatabaseMetaData.SYSTEM_CATALOG,
              MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_11_0,
              PhoenixDatabaseMetaData.USE_STATS_FOR_PARALLELIZATION + " "
                + PBoolean.INSTANCE.getSqlTypeName());
            addParentToChildLinks(metaConnection);
        }
        if (currentServerSideTableTimeStamp < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_14_0) {
            metaConnection = addColumnsIfNotExists(
              metaConnection,
              PhoenixDatabaseMetaData.SYSTEM_CATALOG,
              MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_14_0,
              PhoenixDatabaseMetaData.TRANSACTION_PROVIDER + " "
                + PTinyint.INSTANCE.getSqlTypeName());
            try (Statement altQry = metaConnection.createStatement()) {
                altQry.executeUpdate("ALTER TABLE "
                    + PhoenixDatabaseMetaData.SYSTEM_CATALOG + " SET "
                    + HConstants.VERSIONS + "= "
                    + props.getInt(DEFAULT_SYSTEM_MAX_VERSIONS_ATTRIB, QueryServicesOptions
                    .DEFAULT_SYSTEM_MAX_VERSIONS) + ",\n"
                    + ColumnFamilyDescriptorBuilder.KEEP_DELETED_CELLS + "="
                    + props.getBoolean(DEFAULT_SYSTEM_KEEP_DELETED_CELLS_ATTRIB,
                    QueryServicesOptions.DEFAULT_SYSTEM_KEEP_DELETED_CELLS));

                altQry.executeUpdate("ALTER TABLE "
                        + PhoenixDatabaseMetaData.SYSTEM_FUNCTION + " SET "
                        + TableDescriptorBuilder.SPLIT_POLICY + "='"
                        + QueryConstants.SYSTEM_FUNCTION_SPLIT_POLICY_CLASSNAME + "',\n"
                        + HConstants.VERSIONS + "= "
                        + props.getInt(DEFAULT_SYSTEM_MAX_VERSIONS_ATTRIB, QueryServicesOptions
                        .DEFAULT_SYSTEM_MAX_VERSIONS) + ",\n"
                        + ColumnFamilyDescriptorBuilder.KEEP_DELETED_CELLS + "="
                        + props.getBoolean(DEFAULT_SYSTEM_KEEP_DELETED_CELLS_ATTRIB,
                        QueryServicesOptions.DEFAULT_SYSTEM_KEEP_DELETED_CELLS));

                altQry.executeUpdate("ALTER TABLE "
                        + PhoenixDatabaseMetaData.SYSTEM_STATS_NAME + " SET "
                        + TableDescriptorBuilder.SPLIT_POLICY + "='"
                        + QueryConstants.SYSTEM_STATS_SPLIT_POLICY_CLASSNAME + "'");
            }
        }
        if (currentServerSideTableTimeStamp < MIN_SYSTEM_TABLE_TIMESTAMP_4_15_0) {
            addViewIndexToParentLinks(metaConnection);
            metaConnection = addColumnsIfNotExists(
                    metaConnection,
                    PhoenixDatabaseMetaData.SYSTEM_CATALOG,
                    MIN_SYSTEM_TABLE_TIMESTAMP_4_15_0,
                    PhoenixDatabaseMetaData.VIEW_INDEX_ID_DATA_TYPE + " "
                            + PInteger.INSTANCE.getSqlTypeName());
        }
        if (currentServerSideTableTimeStamp < MIN_SYSTEM_TABLE_TIMESTAMP_4_16_0) {
            metaConnection = addColumnsIfNotExists(
                metaConnection,
                PhoenixDatabaseMetaData.SYSTEM_CATALOG,
                MIN_SYSTEM_TABLE_TIMESTAMP_4_16_0 - 3,
                PhoenixDatabaseMetaData.PHOENIX_TTL + " "
                        + PInteger.INSTANCE.getSqlTypeName());
            metaConnection = addColumnsIfNotExists(
                metaConnection,
                PhoenixDatabaseMetaData.SYSTEM_CATALOG,
                MIN_SYSTEM_TABLE_TIMESTAMP_4_16_0 - 2,
                PhoenixDatabaseMetaData.PHOENIX_TTL_HWM + " "
                        + PInteger.INSTANCE.getSqlTypeName());
            metaConnection = addColumnsIfNotExists(metaConnection,
                PhoenixDatabaseMetaData.SYSTEM_CATALOG, MIN_SYSTEM_TABLE_TIMESTAMP_4_16_0 -1,
                PhoenixDatabaseMetaData.LAST_DDL_TIMESTAMP + " "
                    + PLong.INSTANCE.getSqlTypeName());
            metaConnection = addColumnsIfNotExists(metaConnection,
                PhoenixDatabaseMetaData.SYSTEM_CATALOG, MIN_SYSTEM_TABLE_TIMESTAMP_4_16_0,
                PhoenixDatabaseMetaData.CHANGE_DETECTION_ENABLED
                    + " " + PBoolean.INSTANCE.getSqlTypeName());
            UpgradeUtil.bootstrapLastDDLTimestampForTablesAndViews(metaConnection);

            boolean isNamespaceMapping =
                    SchemaUtil.isNamespaceMappingEnabled(null,  getConfiguration());
            String tableName = PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME;
            if (isNamespaceMapping) {
                tableName = tableName.replace(
                        QueryConstants.NAME_SEPARATOR,
                        QueryConstants.NAMESPACE_SEPARATOR);
            }
            byte[] tableBytes = StringUtil.toBytes(tableName);
            byte[] rowKey = SchemaUtil.getColumnKey(null,
                    QueryConstants.SYSTEM_SCHEMA_NAME,
                    SYSTEM_CATALOG_TABLE, VIEW_INDEX_ID,
                    PhoenixDatabaseMetaData.TABLE_FAMILY);
            if (UpgradeUtil.isUpdateViewIndexIdColumnDataTypeFromShortToLongNeeded
                    (metaConnection, rowKey, tableBytes)) {
                LOGGER.info("Updating VIEW_INDEX_ID data type to BIGINT.");
                UpgradeUtil.updateViewIndexIdColumnDataTypeFromShortToLong(
                        metaConnection, rowKey, tableBytes);
            } else {
                LOGGER.info("Updating VIEW_INDEX_ID data type is not needed.");
            }
            try (Admin admin = metaConnection.getQueryServices().getAdmin()) {
                TableDescriptorBuilder tdBuilder;
                TableName sysCatPhysicalTableName = SchemaUtil.getPhysicalTableName(
                    PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME, props);
                tdBuilder = TableDescriptorBuilder.newBuilder(
                    admin.getDescriptor(sysCatPhysicalTableName));
                if (!tdBuilder.build().hasCoprocessor(
                        QueryConstants.SYSTEM_CATALOG_REGION_OBSERVER_CLASSNAME)) {
                    int priority = props.getInt(
                        QueryServices.COPROCESSOR_PRIORITY_ATTRIB,
                        QueryServicesOptions.DEFAULT_COPROCESSOR_PRIORITY);
                    tdBuilder.setCoprocessor(
                        CoprocessorDescriptorBuilder
                            .newBuilder(QueryConstants.SYSTEM_CATALOG_REGION_OBSERVER_CLASSNAME)
                            .setPriority(priority)
                            .setProperties(Collections.emptyMap())
                            .build());
                    admin.modifyTable(tdBuilder.build());
                    pollForUpdatedTableDescriptor(admin, tdBuilder.build(),
                        sysCatPhysicalTableName.getName());
                }
            }
        }
        if (currentServerSideTableTimeStamp < MIN_SYSTEM_TABLE_TIMESTAMP_5_2_0) {
            metaConnection = addColumnsIfNotExists(metaConnection,
                    PhoenixDatabaseMetaData.SYSTEM_CATALOG, MIN_SYSTEM_TABLE_TIMESTAMP_5_2_0 - 4,
                    PhoenixDatabaseMetaData.PHYSICAL_TABLE_NAME
                            + " " + PVarchar.INSTANCE.getSqlTypeName());

            metaConnection = addColumnsIfNotExists(metaConnection, PhoenixDatabaseMetaData.SYSTEM_CATALOG,
                MIN_SYSTEM_TABLE_TIMESTAMP_5_2_0 - 3,
                    PhoenixDatabaseMetaData.SCHEMA_VERSION + " " + PVarchar.INSTANCE.getSqlTypeName());
            metaConnection = addColumnsIfNotExists(metaConnection, PhoenixDatabaseMetaData.SYSTEM_CATALOG,
                MIN_SYSTEM_TABLE_TIMESTAMP_5_2_0 - 2,
                PhoenixDatabaseMetaData.EXTERNAL_SCHEMA_ID + " " + PVarchar.INSTANCE.getSqlTypeName());
            metaConnection = addColumnsIfNotExists(metaConnection, PhoenixDatabaseMetaData.SYSTEM_CATALOG,
                MIN_SYSTEM_TABLE_TIMESTAMP_5_2_0 - 1,
                PhoenixDatabaseMetaData.STREAMING_TOPIC_NAME + " " + PVarchar.INSTANCE.getSqlTypeName());
            metaConnection = addColumnsIfNotExists(metaConnection, PhoenixDatabaseMetaData.SYSTEM_CATALOG,
                    MIN_SYSTEM_TABLE_TIMESTAMP_5_2_0,
                    PhoenixDatabaseMetaData.INDEX_WHERE + " " + PVarchar.INSTANCE.getSqlTypeName());
            UpgradeUtil.bootstrapLastDDLTimestampForIndexes(metaConnection);
        }
        return metaConnection;
    }

    /**
     * There is no other locking needed here since only one connection (on the same or different JVM) will be able to
     * acquire the upgrade mutex via {@link #acquireUpgradeMutex(long)} .
     */
    @Override
    public void upgradeSystemTables(final String url, final Properties props) throws SQLException {
        PhoenixConnection metaConnection = null;
        boolean success = false;
        final Map<String, String> systemTableToSnapshotMap = new HashMap<>();
        String sysCatalogTableName = null;
        SQLException toThrow = null;
        boolean acquiredMutexLock = false;
        boolean moveChildLinks = false;
        boolean syncAllTableAndIndexProps = false;
        try {
            if (!isUpgradeRequired()) {
                throw new UpgradeNotRequiredException();
            }
            Properties scnProps = PropertiesUtil.deepCopy(props);
            scnProps.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
                    Long.toString(MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP));
            scnProps.remove(PhoenixRuntime.TENANT_ID_ATTRIB);
            String globalUrl = JDBCUtil.removeProperty(url, PhoenixRuntime.TENANT_ID_ATTRIB);
            metaConnection = new PhoenixConnection(ConnectionQueryServicesImpl.this, globalUrl,
                    scnProps);
            metaConnection.setRunningUpgrade(true);
            // Always try to create SYSTEM.MUTEX table first since we need it to acquire the
            // upgrade mutex. Upgrade or migration is not possible without the upgrade mutex
            try (Admin admin = getAdmin()) {
                createSysMutexTableIfNotExists(admin);
            }
            UpgradeRequiredException caughtUpgradeRequiredException = null;
            TableAlreadyExistsException caughtTableAlreadyExistsException = null;
            try {
                metaConnection.createStatement().executeUpdate(getSystemCatalogTableDDL());
            } catch (NewerTableAlreadyExistsException ignore) {
                // Ignore, as this will happen if the SYSTEM.CATALOG already exists at this fixed
                // timestamp. A TableAlreadyExistsException is not thrown, since the table only exists
                // *after* this fixed timestamp.
            } catch (UpgradeRequiredException e) {
                // This is thrown while trying to create SYSTEM:CATALOG to indicate that we must
                // migrate SYSTEM tables to the SYSTEM namespace and/or upgrade SYSCAT if required
                caughtUpgradeRequiredException = e;
            } catch (TableAlreadyExistsException e) {
                caughtTableAlreadyExistsException = e;
            }

            if (caughtUpgradeRequiredException != null
                    || caughtTableAlreadyExistsException != null) {
                long currentServerSideTableTimeStamp;
                if (caughtUpgradeRequiredException != null) {
                    currentServerSideTableTimeStamp =
                            caughtUpgradeRequiredException.getSystemCatalogTimeStamp();
                } else {
                    currentServerSideTableTimeStamp =
                            caughtTableAlreadyExistsException.getTable().getTimeStamp();
                }
                acquiredMutexLock = acquireUpgradeMutex(
                    MetaDataProtocol.MIN_SYSTEM_TABLE_MIGRATION_TIMESTAMP);
                LOGGER.debug(
                    "Acquired lock in SYSMUTEX table for migrating SYSTEM tables to SYSTEM "
                    + "namespace and/or upgrading " + sysCatalogTableName);
                String snapshotName = getSysTableSnapshotName(currentServerSideTableTimeStamp,
                    SYSTEM_CATALOG_NAME);
                createSnapshot(snapshotName, SYSTEM_CATALOG_NAME);
                systemTableToSnapshotMap.put(SYSTEM_CATALOG_NAME, snapshotName);
                LOGGER.info("Created snapshot {} for {}", snapshotName, SYSTEM_CATALOG_NAME);

                if (caughtUpgradeRequiredException != null) {
                    if (SchemaUtil.isNamespaceMappingEnabled(
                            PTableType.SYSTEM, ConnectionQueryServicesImpl.this.getProps())) {
                        // If SYSTEM tables exist, they are migrated to HBase SYSTEM namespace
                        // If they don't exist or they're already migrated, this method will return
                        //immediately
                        ensureSystemTablesMigratedToSystemNamespace();
                        LOGGER.debug("Migrated SYSTEM tables to SYSTEM namespace");
                    }
                }

                metaConnection = upgradeSystemCatalogIfRequired(metaConnection, currentServerSideTableTimeStamp);
                if (currentServerSideTableTimeStamp < MIN_SYSTEM_TABLE_TIMESTAMP_4_15_0) {
                    moveChildLinks = true;
                    syncAllTableAndIndexProps = true;
                }
                if (currentServerSideTableTimeStamp < MIN_SYSTEM_TABLE_TIMESTAMP_4_16_0) {
                    //Combine view index id sequences for the same physical view index table
                    //to avoid collisions. See PHOENIX-5132 and PHOENIX-5138
                    try (PhoenixConnection conn = new PhoenixConnection(
                            ConnectionQueryServicesImpl.this, globalUrl,
                            props)) {
                        UpgradeUtil.mergeViewIndexIdSequences(metaConnection);
                    } catch (Exception mergeViewIndeIdException) {
                        LOGGER.warn("Merge view index id sequence failed! If possible, " +
                                "please run MergeViewIndexIdSequencesTool to avoid view index" +
                                "id collision. Error: " + mergeViewIndeIdException.getMessage());
                    }
                }
            }

            // pass systemTableToSnapshotMap to capture more system table to
            // snapshot entries
            metaConnection = upgradeOtherSystemTablesIfRequired(metaConnection,
                moveChildLinks, systemTableToSnapshotMap);

            // Once the system tables are upgraded the local index upgrade can be done
            if (localIndexUpgradeRequired) {
                LOGGER.info("Upgrading local indexes");
                metaConnection = UpgradeUtil.upgradeLocalIndexes(metaConnection);
            }

            // Synchronize necessary properties amongst all column families of a base table
            // and its indexes. See PHOENIX-3955
            if (syncAllTableAndIndexProps) {
                syncTableAndIndexProperties(metaConnection);
            }

            // In case namespace mapping is enabled and system table to system namespace mapping is also enabled,
            // create an entry for the SYSTEM namespace in the SYSCAT table, so that GRANT/REVOKE commands can work
            // with SYSTEM Namespace
            createSchemaIfNotExistsSystemNSMappingEnabled(metaConnection);

            clearUpgradeRequired();
            success = true;
        } catch (UpgradeInProgressException | UpgradeNotRequiredException e) {
            // don't set it as initializationException because otherwise client won't be able to retry
            throw e;
        } catch (Exception e) {
            if (e instanceof SQLException) {
                toThrow = (SQLException)e;
            } else {
                // wrap every other exception into a SQLException
                toThrow = new SQLException(e);
            }
        } finally {
            try {
                if (metaConnection != null) {
                    metaConnection.close();
                }
            } catch (SQLException e) {
                if (toThrow != null) {
                    toThrow.setNextException(e);
                } else {
                    toThrow = e;
                }
            } finally {
                if (!success) {
                    LOGGER.warn("Failed upgrading System tables. " +
                        "Snapshots for system tables created so far: {}",
                        systemTableToSnapshotMap);
                }
                if (acquiredMutexLock) {
                    try {
                        releaseUpgradeMutex();
                    } catch (IOException e) {
                        LOGGER.warn("Release of upgrade mutex failed ", e);
                    }
                }
                if (toThrow != null) {
                    throw toThrow;
                }
            }
        }
    }

    /**
     * Create or upgrade SYSTEM tables other than SYSTEM.CATALOG
     * @param metaConnection Phoenix connection
     * @param moveChildLinks true if we need to move child links from SYSTEM.CATALOG to
     *                       SYSTEM.CHILD_LINK
     * @param systemTableToSnapshotMap table to snapshot map which can be
     *     where new entries of system table to it's corresponding  created
     *     snapshot is added
     * @return Phoenix connection
     * @throws SQLException thrown by underlying upgrade system methods
     * @throws IOException thrown by underlying upgrade system methods
     */
    private PhoenixConnection upgradeOtherSystemTablesIfRequired(
            PhoenixConnection metaConnection, boolean moveChildLinks,
            Map<String, String> systemTableToSnapshotMap)
            throws SQLException, IOException {
        // if we are really going to perform upgrades of other system tables,
        // by this point we would have already taken mutex lock, hence
        // we can proceed with creation of snapshots and add table to
        // snapshot entries in systemTableToSnapshotMap
        metaConnection = upgradeSystemSequence(metaConnection,
            systemTableToSnapshotMap);
        metaConnection = upgradeSystemStats(metaConnection,
            systemTableToSnapshotMap);
        metaConnection = upgradeSystemTask(metaConnection,
            systemTableToSnapshotMap);
        metaConnection = upgradeSystemFunction(metaConnection);
        metaConnection = upgradeSystemTransform(metaConnection, systemTableToSnapshotMap);
        metaConnection = upgradeSystemLog(metaConnection, systemTableToSnapshotMap);
        metaConnection = upgradeSystemMutex(metaConnection);

        // As this is where the most time will be spent during an upgrade,
        // especially when there are large number of views.
        // Upgrade the SYSTEM.CHILD_LINK towards the end,
        // so that any failures here can be handled/continued out of band.
        metaConnection = upgradeSystemChildLink(metaConnection, moveChildLinks,
                systemTableToSnapshotMap);
        return metaConnection;
    }

    private PhoenixConnection upgradeSystemChildLink(
            PhoenixConnection metaConnection, boolean moveChildLinks,
            Map<String, String> systemTableToSnapshotMap) throws SQLException, IOException {
        try (Statement statement = metaConnection.createStatement()) {
            statement.executeUpdate(getChildLinkDDL());
        } catch (TableAlreadyExistsException e) {
            takeSnapshotOfSysTable(systemTableToSnapshotMap, e);
        }
        if (moveChildLinks) {
            // Increase the timeouts so that the scan queries during moveOrCopyChildLinks do not timeout on large syscat's
            Map<String, String> options = new HashMap<>();
            options.put(HConstants.HBASE_RPC_TIMEOUT_KEY, Integer.toString(DEFAULT_TIMEOUT_DURING_UPGRADE_MS));
            options.put(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, Integer.toString(DEFAULT_TIMEOUT_DURING_UPGRADE_MS));
            moveOrCopyChildLinks(metaConnection, options);
        }
        return metaConnection;
    }

    @VisibleForTesting
    public PhoenixConnection upgradeSystemSequence(
            PhoenixConnection metaConnection,
            Map<String, String> systemTableToSnapshotMap) throws SQLException, IOException {
        try (Statement statement = metaConnection.createStatement()) {
            String createSequenceTable = getSystemSequenceTableDDL(nSequenceSaltBuckets);
            statement.executeUpdate(createSequenceTable);
        } catch (NewerTableAlreadyExistsException e) {
            // Ignore, as this will happen if the SYSTEM.SEQUENCE already exists at this fixed
            // timestamp.
            // A TableAlreadyExistsException is not thrown, since the table only exists *after* this
            // fixed timestamp.
            nSequenceSaltBuckets = getSaltBuckets(e);
        } catch (TableAlreadyExistsException e) {
            // take snapshot first
            takeSnapshotOfSysTable(systemTableToSnapshotMap, e);

            // This will occur if we have an older SYSTEM.SEQUENCE and we need to update it to
            // include
            // any new columns we've added.
            long currentServerSideTableTimeStamp = e.getTable().getTimeStamp();
            if (currentServerSideTableTimeStamp <
                    MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_1_0) {
                // If the table time stamp is before 4.1.0 then we need to add below columns
                // to the SYSTEM.SEQUENCE table.
                String columnsToAdd = PhoenixDatabaseMetaData.MIN_VALUE + " "
                        + PLong.INSTANCE.getSqlTypeName() + ", "
                        + PhoenixDatabaseMetaData.MAX_VALUE + " "
                        + PLong.INSTANCE.getSqlTypeName() + ", "
                        + PhoenixDatabaseMetaData.CYCLE_FLAG + " "
                        + PBoolean.INSTANCE.getSqlTypeName() + ", "
                        + PhoenixDatabaseMetaData.LIMIT_REACHED_FLAG + " "
                        + PBoolean.INSTANCE.getSqlTypeName();
                addColumnsIfNotExists(metaConnection, PhoenixDatabaseMetaData.SYSTEM_CATALOG,
                        MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP, columnsToAdd);
            }
            // If the table timestamp is before 4.2.1 then run the upgrade script
            if (currentServerSideTableTimeStamp <
                    MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_2_1) {
                if (UpgradeUtil.upgradeSequenceTable(metaConnection, nSequenceSaltBuckets,
                        e.getTable())) {
                    metaConnection.removeTable(null,
                            PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_SCHEMA,
                            PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_TABLE,
                            MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP);
                    clearTableFromCache(ByteUtil.EMPTY_BYTE_ARRAY,
                            PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_SCHEMA_BYTES,
                            PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_TABLE_BYTES,
                            MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP);
                    clearTableRegionCache(TableName.valueOf(
                            PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME_BYTES));
                }
            } else {
                nSequenceSaltBuckets = getSaltBuckets(e);
            }

            updateSystemSequenceWithCacheOnWriteProps(metaConnection);
        }
        return metaConnection;
    }

    private void updateSystemSequenceWithCacheOnWriteProps(PhoenixConnection metaConnection) throws
        IOException, SQLException {

        try (Admin admin = getAdmin()) {
            TableDescriptor oldTD = admin.getDescriptor(
                SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME,
                    metaConnection.getQueryServices().getProps()));
            ColumnFamilyDescriptor oldCf = oldTD.getColumnFamily(
                QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES);

            // If the CacheOnWrite related properties are not set, lets set them.
            if (!oldCf.isCacheBloomsOnWrite() || !oldCf.isCacheDataOnWrite()
                || !oldCf.isCacheIndexesOnWrite()) {
                ColumnFamilyDescriptorBuilder newCFBuilder =
                    ColumnFamilyDescriptorBuilder.newBuilder(oldCf);
                newCFBuilder.setCacheBloomsOnWrite(true);
                newCFBuilder.setCacheDataOnWrite(true);
                newCFBuilder.setCacheIndexesOnWrite(true);

                TableDescriptorBuilder newTD = TableDescriptorBuilder.newBuilder(oldTD);
                newTD.modifyColumnFamily(newCFBuilder.build());
                admin.modifyTable(newTD.build());
            }
        }
    }

    private void takeSnapshotOfSysTable(
            Map<String, String> systemTableToSnapshotMap,
            TableAlreadyExistsException e) throws SQLException {
        long currentServerSideTableTimeStamp = e.getTable().getTimeStamp();
        String tableName = e.getTable().getPhysicalName().getString();
        String snapshotName = getSysTableSnapshotName(
            currentServerSideTableTimeStamp, tableName);
        // Snapshot qualifiers may only contain 'alphanumeric characters' and
        // digits, hence : cannot be part of snapshot name
        if (snapshotName.contains(QueryConstants.NAMESPACE_SEPARATOR)) {
            snapshotName = snapshotName.replace(
                QueryConstants.NAMESPACE_SEPARATOR,
                QueryConstants.NAME_SEPARATOR);
        }
        createSnapshot(snapshotName, tableName);
        systemTableToSnapshotMap.put(tableName, snapshotName);
        LOGGER.info("Snapshot {} created for table {}", snapshotName,
            tableName);
    }

    @VisibleForTesting
    public PhoenixConnection upgradeSystemStats(
            PhoenixConnection metaConnection,
            Map<String, String> systemTableToSnapshotMap) throws
        SQLException, org.apache.hadoop.hbase.TableNotFoundException, IOException {
        try (Statement statement = metaConnection.createStatement()) {
            statement.executeUpdate(QueryConstants.CREATE_STATS_TABLE_METADATA);
        } catch (NewerTableAlreadyExistsException ignored) {

        } catch (TableAlreadyExistsException e) {
            // take snapshot first
            takeSnapshotOfSysTable(systemTableToSnapshotMap, e);
            long currentServerSideTableTimeStamp = e.getTable().getTimeStamp();
            if (currentServerSideTableTimeStamp <
                    MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_3_0) {
                metaConnection = addColumnsIfNotExists(
                        metaConnection,
                        SYSTEM_STATS_NAME,
                        MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP,
                        PhoenixDatabaseMetaData.GUIDE_POSTS_ROW_COUNT + " "
                                + PLong.INSTANCE.getSqlTypeName());
            }
            if (currentServerSideTableTimeStamp <
                    MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_9_0) {
                // The COLUMN_FAMILY column should be nullable as we create a row in it without
                // any column family to mark when guideposts were last collected.
                metaConnection = removeNotNullConstraint(metaConnection,
                        SYSTEM_SCHEMA_NAME,
                        PhoenixDatabaseMetaData.SYSTEM_STATS_TABLE,
                        MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_9_0,
                        PhoenixDatabaseMetaData.COLUMN_FAMILY);
                ConnectionQueryServicesImpl.this.removeTable(null,
                        PhoenixDatabaseMetaData.SYSTEM_STATS_NAME, null,
                        MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_9_0);
                clearCache();
            }
            if (UpgradeUtil.tableHasKeepDeleted(
                metaConnection, PhoenixDatabaseMetaData.SYSTEM_STATS_NAME)) {
                try (Statement altStmt = metaConnection.createStatement()) {
                    altStmt.executeUpdate("ALTER TABLE "
                        + PhoenixDatabaseMetaData.SYSTEM_STATS_NAME + " SET "
                        + KEEP_DELETED_CELLS + "='" + KeepDeletedCells.FALSE + "'");
                }
            }
            if (UpgradeUtil.tableHasMaxVersions(
                metaConnection, PhoenixDatabaseMetaData.SYSTEM_STATS_NAME)) {
                try (Statement altStats = metaConnection.createStatement()) {
                    altStats.executeUpdate("ALTER TABLE "
                        + PhoenixDatabaseMetaData.SYSTEM_STATS_NAME + " SET "
                        + HConstants.VERSIONS + " = '1' ");
                }
            }
        }
        return metaConnection;
    }

    private PhoenixConnection upgradeSystemTask(
            PhoenixConnection metaConnection,
            Map<String, String> systemTableToSnapshotMap)
            throws SQLException, IOException {
        try (Statement statement = metaConnection.createStatement()) {
            statement.executeUpdate(getTaskDDL());
        } catch (NewerTableAlreadyExistsException ignored) {

        } catch (TableAlreadyExistsException e) {
            // take snapshot first
            takeSnapshotOfSysTable(systemTableToSnapshotMap, e);
            long currentServerSideTableTimeStamp = e.getTable().getTimeStamp();
            if (currentServerSideTableTimeStamp <=
                    MIN_SYSTEM_TABLE_TIMESTAMP_4_15_0) {
                String columnsToAdd =
                        PhoenixDatabaseMetaData.TASK_STATUS + " " +
                                PVarchar.INSTANCE.getSqlTypeName() + ", "
                                + PhoenixDatabaseMetaData.TASK_END_TS + " " +
                                PTimestamp.INSTANCE.getSqlTypeName() + ", "
                                + PhoenixDatabaseMetaData.TASK_PRIORITY + " " +
                                PUnsignedTinyint.INSTANCE.getSqlTypeName() + ", "
                                + PhoenixDatabaseMetaData.TASK_DATA + " " +
                                PVarchar.INSTANCE.getSqlTypeName();
                String taskTableFullName = SchemaUtil.getTableName(SYSTEM_CATALOG_SCHEMA,
                        SYSTEM_TASK_TABLE);
                metaConnection =
                        addColumnsIfNotExists(metaConnection, taskTableFullName,
                                MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP, columnsToAdd);
                String altQuery = String.format(ALTER_TABLE_SET_PROPS,
                    taskTableFullName, TTL, TASK_TABLE_TTL);
                try (PreparedStatement altQueryStmt = metaConnection.prepareStatement(altQuery)) {
                    altQueryStmt.executeUpdate();
                }
                clearCache();
            }
            // If SYSTEM.TASK does not have disabled regions split policy,
            // set it up here while upgrading it
            try (Admin admin = metaConnection.getQueryServices().getAdmin()) {
                TableDescriptor td;
                TableName tableName = SchemaUtil.getPhysicalTableName(
                    PhoenixDatabaseMetaData.SYSTEM_TASK_NAME, props);
                td = admin.getDescriptor(tableName);
                TableDescriptorBuilder tableDescriptorBuilder =
                    TableDescriptorBuilder.newBuilder(td);
                boolean isTableDescUpdated = false;
                if (updateAndConfirmSplitPolicyForTask(
                        tableDescriptorBuilder)) {
                    isTableDescUpdated = true;
                }
                if (!tableDescriptorBuilder.build().hasCoprocessor(
                    QueryConstants.TASK_META_DATA_ENDPOINT_CLASSNAME)) {
                    int priority = props.getInt(
                        QueryServices.COPROCESSOR_PRIORITY_ATTRIB,
                        QueryServicesOptions.DEFAULT_COPROCESSOR_PRIORITY);
                    tableDescriptorBuilder.setCoprocessor(
                            CoprocessorDescriptorBuilder
                                    .newBuilder(QueryConstants.TASK_META_DATA_ENDPOINT_CLASSNAME)
                                    .setPriority(priority)
                                    .setProperties(Collections.emptyMap())
                                    .build());
                    isTableDescUpdated=true;
                }
                if (isTableDescUpdated) {
                    admin.modifyTable(tableDescriptorBuilder.build());
                    pollForUpdatedTableDescriptor(admin,
                        tableDescriptorBuilder.build(), tableName.getName());
                }
            } catch (InterruptedException | TimeoutException ite) {
                throw new SQLException(PhoenixDatabaseMetaData.SYSTEM_TASK_NAME
                    + " Upgrade is not confirmed");
            }
        }
        return metaConnection;
    }

    private PhoenixConnection upgradeSystemTransform(
            PhoenixConnection metaConnection,
            Map<String, String> systemTableToSnapshotMap)
            throws SQLException {
        try (Statement statement = metaConnection.createStatement()) {
            statement.executeUpdate(getTransformDDL());
        } catch (TableAlreadyExistsException ignored) {

        }
        return metaConnection;
    }

    private PhoenixConnection upgradeSystemFunction(PhoenixConnection metaConnection)
    throws SQLException {
        try {
            metaConnection.createStatement().executeUpdate(getFunctionTableDDL());
        } catch (TableAlreadyExistsException ignored) {
            // Since we are not performing any action as part of upgrading
            // SYSTEM.FUNCTION, we don't need to take snapshot as of this
            // writing. However, if need arises to perform significant
            // update, we should take snapshot just like other system tables.
            // e.g usages of takeSnapshotOfSysTable()
        }
        return metaConnection;
    }

    @VisibleForTesting
    public PhoenixConnection upgradeSystemLog(PhoenixConnection metaConnection,
            Map<String, String> systemTableToSnapshotMap)
    throws SQLException, org.apache.hadoop.hbase.TableNotFoundException, IOException {
        try (Statement statement = metaConnection.createStatement()) {
            statement.executeUpdate(getLogTableDDL());
        } catch (NewerTableAlreadyExistsException ignored) {
        } catch (TableAlreadyExistsException e) {
            // take snapshot first
            takeSnapshotOfSysTable(systemTableToSnapshotMap, e);
            if (UpgradeUtil.tableHasKeepDeleted(
                metaConnection, PhoenixDatabaseMetaData.SYSTEM_LOG_NAME) ) {
                try (Statement altLogStmt = metaConnection.createStatement()) {
                    altLogStmt.executeUpdate("ALTER TABLE "
                        + PhoenixDatabaseMetaData.SYSTEM_LOG_NAME + " SET "
                        + KEEP_DELETED_CELLS + "='" + KeepDeletedCells.FALSE + "'");
                }
            }
            if (UpgradeUtil.tableHasMaxVersions(
                metaConnection, PhoenixDatabaseMetaData.SYSTEM_LOG_NAME)) {
                try (Statement altLogVer = metaConnection.createStatement()) {
                    altLogVer.executeUpdate("ALTER TABLE "
                        + PhoenixDatabaseMetaData.SYSTEM_LOG_NAME + " SET "
                        + HConstants.VERSIONS + "='1'");
                }
            }
        }
        return metaConnection;
    }

    private PhoenixConnection upgradeSystemMutex(PhoenixConnection metaConnection)
    throws SQLException {
        try {
            metaConnection.createStatement().executeUpdate(getMutexDDL());
        } catch (TableAlreadyExistsException ignored) {
            // Since we are not performing any action as part of upgrading
            // SYSTEM.MUTEX, we don't need to take snapshot as of this
            // writing. However, if need arises to perform significant
            // update, we should take snapshot just like other system tables.
            // e.g usages of takeSnapshotOfSysTable()
        }
        return metaConnection;
    }


    // Special method for adding the column qualifier column for 4.10. 
    private PhoenixConnection addColumnQualifierColumn(PhoenixConnection oldMetaConnection, Long timestamp) throws SQLException {
        Properties props = PropertiesUtil.deepCopy(oldMetaConnection.getClientInfo());
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(timestamp));
        // Cannot go through DriverManager or you end up in an infinite loop because it'll call init again
        PhoenixConnection metaConnection = new PhoenixConnection(oldMetaConnection, this, props);
        metaConnection.setAutoCommit(false);
        PTable sysCatalogPTable = metaConnection.getTable(SYSTEM_CATALOG_NAME);
        int numColumns = sysCatalogPTable.getColumns().size();
        try (PreparedStatement mutateTable = metaConnection.prepareStatement(MetaDataClient.MUTATE_TABLE)) {
            mutateTable.setString(1, null);
            mutateTable.setString(2, SYSTEM_CATALOG_SCHEMA);
            mutateTable.setString(3, SYSTEM_CATALOG_TABLE);
            mutateTable.setString(4, PTableType.SYSTEM.getSerializedValue());
            mutateTable.setLong(5, sysCatalogPTable.getSequenceNumber() + 1);
            mutateTable.setInt(6, numColumns + 1);
            mutateTable.execute();
        }
        List<Mutation> tableMetadata = new ArrayList<>(
                metaConnection.getMutationState().toMutations(metaConnection.getSCN()).next()
                        .getSecond());
        metaConnection.rollback();
        PColumn column = new PColumnImpl(PNameFactory.newName("COLUMN_QUALIFIER"),
                PNameFactory.newName(DEFAULT_COLUMN_FAMILY_NAME), PVarbinary.INSTANCE, null, null, true, numColumns,
                SortOrder.ASC, null, null, false, null, false, false,
                Bytes.toBytes("COLUMN_QUALIFIER"), timestamp);
        String upsertColumnMetadata = "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " +
                TENANT_ID + "," +
                TABLE_SCHEM + "," +
                TABLE_NAME + "," +
                COLUMN_NAME + "," +
                COLUMN_FAMILY + "," +
                DATA_TYPE + "," +
                NULLABLE + "," +
                COLUMN_SIZE + "," +
                DECIMAL_DIGITS + "," +
                ORDINAL_POSITION + "," +
                SORT_ORDER + "," +
                DATA_TABLE_NAME + "," +
                ARRAY_SIZE + "," +
                VIEW_CONSTANT + "," +
                IS_VIEW_REFERENCED + "," +
                PK_NAME + "," +
                KEY_SEQ + "," +
                COLUMN_DEF + "," +
                IS_ROW_TIMESTAMP +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement colUpsert = metaConnection.prepareStatement(upsertColumnMetadata)) {
            colUpsert.setString(1, null);
            colUpsert.setString(2, SYSTEM_CATALOG_SCHEMA);
            colUpsert.setString(3, SYSTEM_CATALOG_TABLE);
            colUpsert.setString(4, "COLUMN_QUALIFIER");
            colUpsert.setString(5, DEFAULT_COLUMN_FAMILY);
            colUpsert.setInt(6, column.getDataType().getSqlType());
            colUpsert.setInt(7, ResultSetMetaData.columnNullable);
            colUpsert.setNull(8, Types.INTEGER);
            colUpsert.setNull(9, Types.INTEGER);
            colUpsert.setInt(10, sysCatalogPTable.getBucketNum() != null ? numColumns : (numColumns + 1));
            colUpsert.setInt(11, SortOrder.ASC.getSystemValue());
            colUpsert.setString(12, null);
            colUpsert.setNull(13, Types.INTEGER);
            colUpsert.setBytes(14, null);
            colUpsert.setBoolean(15, false);
            colUpsert.setString(16, sysCatalogPTable.getPKName() == null ? null : sysCatalogPTable.getPKName().getString());
            colUpsert.setNull(17, Types.SMALLINT);
            colUpsert.setNull(18, Types.VARCHAR);
            colUpsert.setBoolean(19, false);
            colUpsert.execute();
        }
        tableMetadata.addAll(metaConnection.getMutationState().toMutations(metaConnection.getSCN()).next().getSecond());
        metaConnection.rollback();
        metaConnection.getQueryServices().addColumn(tableMetadata, sysCatalogPTable, null,null, Collections.<String,List<Pair<String,Object>>>emptyMap(), Collections.<String>emptySet(), Lists.newArrayList(column));
        metaConnection.removeTable(null, SYSTEM_CATALOG_NAME, null, timestamp);
        ConnectionQueryServicesImpl.this.removeTable(null,
                SYSTEM_CATALOG_NAME, null,
                timestamp);
        clearCache();
        return metaConnection;
    }

    private void deleteSnapshot(String snapshotName)
            throws SQLException, IOException {
        try (Admin admin = getAdmin()) {
            admin.deleteSnapshot(snapshotName);
            LOGGER.info("Snapshot {} is deleted", snapshotName);
        }
    }

    private void createSnapshot(String snapshotName, String tableName)
            throws SQLException {
        Admin admin = null;
        SQLException sqlE = null;
        try {
            admin = getAdmin();
            admin.snapshot(snapshotName, TableName.valueOf(tableName));
            LOGGER.info("Successfully created snapshot " + snapshotName + " for "
                    + tableName);
        } catch (SnapshotCreationException e) {
            if (e.getMessage().contains("doesn't exist")) {
                LOGGER.warn("Could not create snapshot {}, table is missing." + snapshotName, e);
            } else {
                sqlE = new SQLException(e);
            }
        } catch (Exception e) {
            sqlE = new SQLException(e);
        } finally {
            try {
                if (admin != null) {
                    admin.close();
                }
            } catch (Exception e) {
                SQLException adminCloseEx = new SQLException(e);
                if (sqlE == null) {
                    sqlE = adminCloseEx;
                } else {
                    sqlE.setNextException(adminCloseEx);
                }
            } finally {
                if (sqlE != null) {
                    throw sqlE;
                }
            }
        }
    }

    void ensureSystemTablesMigratedToSystemNamespace()
            throws SQLException, IOException, IllegalArgumentException, InterruptedException {
        if (!SchemaUtil.isNamespaceMappingEnabled(PTableType.SYSTEM, this.getProps())) { return; }

        Table metatable = null;
        try (Admin admin = getAdmin()) {
            List<TableName> tableNames = getSystemTableNamesInDefaultNamespace(admin);
            // No tables exist matching "SYSTEM\..*", they are all already in "SYSTEM:.*"
            if (tableNames.size() == 0) { return; }
            // Try to move any remaining tables matching "SYSTEM\..*" into "SYSTEM:"
            if (tableNames.size() > 9) {
                LOGGER.warn("Expected 9 system tables but found " + tableNames.size() + ":" + tableNames);
            }

            byte[] mappedSystemTable = SchemaUtil
                    .getPhysicalName(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES, this.getProps()).getName();
            metatable = getTable(mappedSystemTable);
            if (tableNames.contains(PhoenixDatabaseMetaData.SYSTEM_CATALOG_HBASE_TABLE_NAME)) {
                if (!AdminUtilWithFallback.tableExists(admin,
                    TableName.valueOf(mappedSystemTable))) {
                    LOGGER.info("Migrating SYSTEM.CATALOG table to SYSTEM namespace.");
                    // Actual migration of SYSCAT table
                    UpgradeUtil.mapTableToNamespace(admin, metatable,
                            PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME, this.getProps(), null, PTableType.SYSTEM,
                            null);
                    // Invalidate the client-side metadataCache
                    ConnectionQueryServicesImpl.this.removeTable(null,
                            PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME, null,
                            MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_1_0);
                }
                tableNames.remove(PhoenixDatabaseMetaData.SYSTEM_CATALOG_HBASE_TABLE_NAME);
            }
            for (TableName table : tableNames) {
                LOGGER.info(String.format("Migrating %s table to SYSTEM namespace.", table.getNameAsString()));
                UpgradeUtil.mapTableToNamespace(admin, metatable, table.getNameAsString(), this.getProps(), null, PTableType.SYSTEM,
                        null);
                ConnectionQueryServicesImpl.this.removeTable(null, table.getNameAsString(), null,
                        MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_1_0);
            }

            // Clear the server-side metadataCache when all tables are migrated so that the new PTable can be loaded with NS mapping
            clearCache();
        } finally {
            if (metatable != null) {
                metatable.close();
            }
        }
    }

    /**
     * Acquire distributed mutex of sorts to make sure only one JVM is able to run the upgrade code by
     * making use of HBase's checkAndPut api.
     *
     * @return true if client won the race, false otherwise
     * @throws SQLException
     */
    @VisibleForTesting
    public boolean acquireUpgradeMutex(long currentServerSideTableTimestamp)
            throws SQLException {
        Preconditions.checkArgument(currentServerSideTableTimestamp < MIN_SYSTEM_TABLE_TIMESTAMP);
        if (!writeMutexCell(null, PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA,
            PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE, null, null)) {
            throw new UpgradeInProgressException(getVersion(currentServerSideTableTimestamp),
                    getVersion(MIN_SYSTEM_TABLE_TIMESTAMP));
        }
        return true;
    }

    @Override
    public boolean writeMutexCell(String tenantId, String schemaName, String tableName,
            String columnName, String familyName) throws SQLException {
        try {
            byte[] rowKey = columnName != null
                ? SchemaUtil.getColumnKey(tenantId, schemaName, tableName,
                    columnName, familyName)
                : SchemaUtil.getTableKey(tenantId, schemaName, tableName);
            // at this point the system mutex table should have been created or
            // an exception thrown
            try (Table sysMutexTable = getSysMutexTable()) {
                byte[] family = PhoenixDatabaseMetaData.SYSTEM_MUTEX_FAMILY_NAME_BYTES;
                byte[] qualifier = PhoenixDatabaseMetaData.SYSTEM_MUTEX_COLUMN_NAME_BYTES;
                byte[] value = MUTEX_LOCKED;
                Put put = new Put(rowKey);
                put.addColumn(family, qualifier, value);
                CheckAndMutate checkAndMutate = CheckAndMutate.newBuilder(rowKey)
                        .ifNotExists(family, qualifier)
                        .build(put);
                boolean checkAndPut =
                        sysMutexTable.checkAndMutate(checkAndMutate).isSuccess();
                String processName = ManagementFactory.getRuntimeMXBean().getName();
                String msg =
                        " tenantId : " + tenantId + " schemaName : " + schemaName + " tableName : "
                                + tableName + " columnName : " + columnName + " familyName : "
                                + familyName;
                if (!checkAndPut) {
                    LOGGER.error(processName + " failed to acquire mutex for "+ msg);
                }
                else {
                    LOGGER.debug(processName + " acquired mutex for "+ msg);
                }
                return checkAndPut;
            }
        } catch (IOException e) {
            throw ClientUtil.parseServerException(e);
        }
    }

    @VisibleForTesting
    public void releaseUpgradeMutex() throws IOException, SQLException {
        deleteMutexCell(null, PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA,
            PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE, null, null);
    }

    @Override
    public void deleteMutexCell(String tenantId, String schemaName, String tableName,
            String columnName, String familyName) throws SQLException {
        try {
            byte[] rowKey = columnName != null
                ? SchemaUtil.getColumnKey(tenantId, schemaName, tableName,
                    columnName, familyName)
                : SchemaUtil.getTableKey(tenantId, schemaName, tableName);
            // at this point the system mutex table should have been created or
            // an exception thrown
            try (Table sysMutexTable = getSysMutexTable()) {
                byte[] family = PhoenixDatabaseMetaData.SYSTEM_MUTEX_FAMILY_NAME_BYTES;
                byte[] qualifier = PhoenixDatabaseMetaData.SYSTEM_MUTEX_COLUMN_NAME_BYTES;
                Delete delete = new Delete(rowKey);
                delete.addColumn(family, qualifier);
                sysMutexTable.delete(delete);
                String processName = ManagementFactory.getRuntimeMXBean().getName();
                String msg =
                        " tenantId : " + tenantId + " schemaName : " + schemaName + " tableName : "
                                + tableName + " columnName : " + columnName + " familyName : "
                                + familyName;
                LOGGER.debug(processName + " released mutex for "+ msg);
            }
        } catch (IOException e) {
            throw ClientUtil.parseServerException(e);
        }
    }

    @VisibleForTesting
    public Table getSysMutexTable() throws SQLException {
        String tableNameAsString = SYSTEM_MUTEX_NAME;
        Table table;
        try {
            table = getTableIfExists(Bytes.toBytes(tableNameAsString));
        } catch (TableNotFoundException e) {
            tableNameAsString = tableNameAsString.replace(
                QueryConstants.NAME_SEPARATOR,
                QueryConstants.NAMESPACE_SEPARATOR);
            // if SYSTEM.MUTEX does not exist, we don't need to check
            // for the existence of SYSTEM:MUTEX as it must exist, hence
            // we can call getTable() here instead of getTableIfExists()
            table = getTable(Bytes.toBytes(tableNameAsString));
        }
        return table;
    }

    private String addColumn(String columnsToAddSoFar, String columns) {
        if (columnsToAddSoFar == null || columnsToAddSoFar.isEmpty()) {
            return columns;
        } else {
            return columnsToAddSoFar + ", " + columns;
        }
    }

    /**
     * Set IMMUTABLE_ROWS to true for all index tables over immutable tables.
     * @param oldMetaConnection connection over which to run the upgrade
     * @param timestamp SCN at which to run the update
     * @throws SQLException
     */
    private PhoenixConnection setImmutableTableIndexesImmutable(PhoenixConnection oldMetaConnection, long timestamp) throws SQLException {
        SQLException sqlE = null;
        Properties props = PropertiesUtil.deepCopy(oldMetaConnection.getClientInfo());
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(timestamp));
        PhoenixConnection metaConnection = new PhoenixConnection(oldMetaConnection, this, props);
        boolean autoCommit = metaConnection.getAutoCommit();
        try {
            metaConnection.setAutoCommit(true);
            metaConnection.createStatement().execute(
                    "UPSERT INTO SYSTEM.CATALOG(TENANT_ID, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, COLUMN_FAMILY, IMMUTABLE_ROWS)\n" +
                            "SELECT A.TENANT_ID, A.TABLE_SCHEM,B.COLUMN_FAMILY,null,null,true\n" +
                            "FROM SYSTEM.CATALOG A JOIN SYSTEM.CATALOG B ON (\n" +
                            " A.TENANT_ID = B.TENANT_ID AND \n" +
                            " A.TABLE_SCHEM = B.TABLE_SCHEM AND\n" +
                            " A.TABLE_NAME = B.TABLE_NAME AND\n" +
                            " A.COLUMN_NAME = B.COLUMN_NAME AND\n" +
                            " B.LINK_TYPE = 1\n" +
                            ")\n" +
                            "WHERE A.COLUMN_FAMILY IS NULL AND\n" +
                            " B.COLUMN_FAMILY IS NOT NULL AND\n" +
                            " A.IMMUTABLE_ROWS = TRUE");
        } catch (SQLException e) {
            LOGGER.warn("exception during upgrading stats table:" + e);
            sqlE = e;
        } finally {
            try {
                metaConnection.setAutoCommit(autoCommit);
                oldMetaConnection.close();
            } catch (SQLException e) {
                if (sqlE != null) {
                    sqlE.setNextException(e);
                } else {
                    sqlE = e;
                }
            }
            if (sqlE != null) {
                throw sqlE;
            }
        }
        return metaConnection;
    }



    /**
     * Forces update of SYSTEM.CATALOG by setting column to existing value
     * @param oldMetaConnection
     * @param timestamp
     * @return
     * @throws SQLException
     */
    private PhoenixConnection updateSystemCatalogTimestamp(PhoenixConnection oldMetaConnection, long timestamp) throws SQLException {
        SQLException sqlE = null;
        Properties props = PropertiesUtil.deepCopy(oldMetaConnection.getClientInfo());
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(timestamp));
        PhoenixConnection metaConnection = new PhoenixConnection(oldMetaConnection, this, props);
        boolean autoCommit = metaConnection.getAutoCommit();
        try {
            metaConnection.setAutoCommit(true);
            metaConnection.createStatement().execute(
                    "UPSERT INTO SYSTEM.CATALOG(TENANT_ID, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, COLUMN_FAMILY, DISABLE_WAL)\n" +
                            "VALUES (NULL, '" + QueryConstants.SYSTEM_SCHEMA_NAME + "','" + PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE + "', NULL, NULL, FALSE)");
        } catch (SQLException e) {
            LOGGER.warn("exception during upgrading stats table:" + e);
            sqlE = e;
        } finally {
            try {
                metaConnection.setAutoCommit(autoCommit);
                oldMetaConnection.close();
            } catch (SQLException e) {
                if (sqlE != null) {
                    sqlE.setNextException(e);
                } else {
                    sqlE = e;
                }
            }
            if (sqlE != null) {
                throw sqlE;
            }
        }
        return metaConnection;
    }

    private PhoenixConnection dropStatsTable(PhoenixConnection oldMetaConnection, long timestamp)
            throws SQLException, IOException {
        Properties props = PropertiesUtil.deepCopy(oldMetaConnection.getClientInfo());
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(timestamp));
        PhoenixConnection metaConnection = new PhoenixConnection(oldMetaConnection, this, props);
        SQLException sqlE = null;
        boolean wasCommit = metaConnection.getAutoCommit();
        try {
            metaConnection.setAutoCommit(true);
            metaConnection.createStatement()
            .executeUpdate("DELETE FROM " + PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME + " WHERE "
                    + PhoenixDatabaseMetaData.TABLE_NAME + "='" + PhoenixDatabaseMetaData.SYSTEM_STATS_TABLE
                    + "' AND " + PhoenixDatabaseMetaData.TABLE_SCHEM + "='"
                    + SYSTEM_SCHEMA_NAME + "'");
        } catch (SQLException e) {
            LOGGER.warn("exception during upgrading stats table:" + e);
            sqlE = e;
        } finally {
            try {
                metaConnection.setAutoCommit(wasCommit);
                oldMetaConnection.close();
            } catch (SQLException e) {
                if (sqlE != null) {
                    sqlE.setNextException(e);
                } else {
                    sqlE = e;
                }
            }
            if (sqlE != null) {
                throw sqlE;
            }
        }
        return metaConnection;
    }

    private void scheduleRenewLeaseTasks() {
        if (isRenewingLeasesEnabled()) {
            renewLeaseExecutor =
                    Executors.newScheduledThreadPool(renewLeasePoolSize, renewLeaseThreadFactory);
            for (LinkedBlockingQueue<WeakReference<PhoenixConnection>> q : connectionQueues) {
                renewLeaseExecutor.scheduleAtFixedRate(new RenewLeaseTask(q), 0,
                    renewLeaseTaskFrequency, TimeUnit.MILLISECONDS);
            }
        }
    }

    private static class RenewLeaseThreadFactory implements ThreadFactory {
        private static final AtomicInteger threadNumber = new AtomicInteger(1);
        private static final String NAME_PREFIX = "PHOENIX-SCANNER-RENEW-LEASE-thread-";

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, NAME_PREFIX + threadNumber.getAndIncrement());
            t.setDaemon(true);
            return t;
        }
    }

    private static int getSaltBuckets(TableAlreadyExistsException e) {
        PTable table = e.getTable();
        Integer sequenceSaltBuckets = table == null ? null : table.getBucketNum();
        return sequenceSaltBuckets == null ? 0 : sequenceSaltBuckets;
    }

    @Override
    public MutationState updateData(MutationPlan plan) throws SQLException {
        MutationState state = plan.execute();
        plan.getContext().getConnection().commit();
        return state;
    }

    @Override
    public int getLowestClusterHBaseVersion() {
        return lowestClusterHBaseVersion;
    }

    @Override
    public boolean hasIndexWALCodec() {
        return hasIndexWALCodec;
    }

    /**
     * Clears the Phoenix meta data cache on each region server
     * @throws SQLException
     */
    @Override
    public long clearCache() throws SQLException {
        synchronized (latestMetaDataLock) {
            latestMetaData = newEmptyMetaData();
        }
        tableStatsCache.invalidateAll();
        long startTime = 0L;
        long systemCatalogRpcTime;
        Map<byte[], Long> results;
        try (Table htable =
                this.getTable(
                    SchemaUtil.getPhysicalName(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES,
                        this.getProps()).getName())) {
            try {
                startTime = EnvironmentEdgeManager.currentTimeMillis();
                results = htable.coprocessorService(MetaDataService.class, HConstants.EMPTY_START_ROW,
                                HConstants.EMPTY_END_ROW, new Batch.Call<MetaDataService, Long>() {
                                    @Override
                                    public Long call(MetaDataService instance) throws IOException {
                                        RpcController controller = getController();
                                        BlockingRpcCallback<ClearCacheResponse> rpcCallback =
                                                new BlockingRpcCallback<ClearCacheResponse>();
                                        ClearCacheRequest.Builder builder = ClearCacheRequest.newBuilder();
                                        builder.setClientVersion(
                                                VersionUtil.encodeVersion(PHOENIX_MAJOR_VERSION,
                                                        PHOENIX_MINOR_VERSION, PHOENIX_PATCH_NUMBER));
                                        instance.clearCache(controller, builder.build(), rpcCallback);
                                        checkForRemoteExceptions(controller);
                                        return rpcCallback.get().getUnfreedBytes();
                                    }
                                });
                TableMetricsManager.updateMetricsForSystemCatalogTableMethod(null,NUM_SYSTEM_TABLE_RPC_SUCCESS, 1);
            } catch(Throwable e) {
                TableMetricsManager.updateMetricsForSystemCatalogTableMethod(null, NUM_SYSTEM_TABLE_RPC_FAILURES, 1);
                throw ClientUtil.parseServerException(e);
            } finally {
                systemCatalogRpcTime = EnvironmentEdgeManager.currentTimeMillis() - startTime;
                TableMetricsManager.updateMetricsForSystemCatalogTableMethod(null,
                        TIME_SPENT_IN_SYSTEM_TABLE_RPC_CALLS, systemCatalogRpcTime);
            }

            long unfreedBytes = 0;
            for (Map.Entry<byte[], Long> result : results.entrySet()) {
                if (result.getValue() != null) {
                    unfreedBytes += result.getValue();
                }
            }
            return unfreedBytes;
        } catch (IOException e) {
            throw ClientUtil.parseServerException(e);
        } catch (Throwable e) {
            // wrap all other exceptions in a SQLException
            throw new SQLException(e);
        }
    }

    private void flushTable(byte[] tableName) throws SQLException {
        Admin admin = getAdmin();
        try {
            admin.flush(TableName.valueOf(tableName));
        } catch (IOException e) {
            throw new PhoenixIOException(e);
//        } catch (InterruptedException e) {
//            // restore the interrupt status
//            Thread.currentThread().interrupt();
//            throw new SQLExceptionInfo.Builder(SQLExceptionCode.INTERRUPTED_EXCEPTION).setRootCause(e).build()
//            .buildException();
        } finally {
            Closeables.closeQuietly(admin);
        }
    }

    @Override
    public Admin getAdmin() throws SQLException {
        try {
            return connection.getAdmin();
        } catch (IOException e) {
            throw new PhoenixIOException(e);
        }
    }

    @Override
    public MetaDataMutationResult updateIndexState(final List<Mutation> tableMetaData, String parentTableName) throws SQLException {
        byte[][] rowKeyMetadata = new byte[3][];
        SchemaUtil.getVarChars(tableMetaData.get(0).getRow(), rowKeyMetadata);
        byte[] tableKey =
                SchemaUtil.getTableKey(rowKeyMetadata[PhoenixDatabaseMetaData.TENANT_ID_INDEX],
                    rowKeyMetadata[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX],
                    rowKeyMetadata[PhoenixDatabaseMetaData.TABLE_NAME_INDEX]);
        byte[] schemaBytes = rowKeyMetadata[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
        byte[] tableBytes = rowKeyMetadata[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
        return metaDataCoprocessorExec(
                SchemaUtil.getPhysicalHBaseTableName(schemaBytes, tableBytes,
                        SchemaUtil.isNamespaceMappingEnabled(PTableType.SYSTEM, this.props)).toString(),
                tableKey,
                new Batch.Call<MetaDataService, MetaDataResponse>() {
            @Override
            public MetaDataResponse call(MetaDataService instance) throws IOException {
                RpcController controller = getController();
                BlockingRpcCallback<MetaDataResponse> rpcCallback =
                        new BlockingRpcCallback<MetaDataResponse>();
                UpdateIndexStateRequest.Builder builder = UpdateIndexStateRequest.newBuilder();
                for (Mutation m : tableMetaData) {
                    MutationProto mp = ProtobufUtil.toProto(m);
                    builder.addTableMetadataMutations(mp.toByteString());
                }
                builder.setClientVersion(VersionUtil.encodeVersion(PHOENIX_MAJOR_VERSION, PHOENIX_MINOR_VERSION, PHOENIX_PATCH_NUMBER));
                instance.updateIndexState(controller, builder.build(), rpcCallback);
                checkForRemoteExceptions(controller);
                return rpcCallback.get();
            }
        });
    }

    @Override
    public MetaDataMutationResult updateIndexState(final List<Mutation> tableMetaData, String parentTableName, Map<String, List<Pair<String,Object>>> stmtProperties,  PTable table) throws SQLException {
        if (stmtProperties == null) {
            return updateIndexState(tableMetaData,parentTableName);
        }

        Map<TableDescriptor, TableDescriptor> oldToNewTableDescriptors =
                separateAndValidateProperties(table, stmtProperties, new HashSet<>(), new HashMap<>());
        TableDescriptor origTableDescriptor = this.getTableDescriptor(table.getPhysicalName().getBytes());
        TableDescriptor newTableDescriptor = oldToNewTableDescriptors.remove(origTableDescriptor);
        Set<TableDescriptor> modifiedTableDescriptors = Collections.emptySet();
        if (newTableDescriptor != null) {
            modifiedTableDescriptors = Sets.newHashSetWithExpectedSize(3 + table.getIndexes().size());
            modifiedTableDescriptors.add(newTableDescriptor);
        }
        sendHBaseMetaData(modifiedTableDescriptors, true);
        return updateIndexState(tableMetaData, parentTableName);
    }

    @Override
    public long createSequence(String tenantId, String schemaName, String sequenceName,
            long startWith, long incrementBy, long cacheSize, long minValue, long maxValue,
            boolean cycle, long timestamp) throws SQLException {
        SequenceKey sequenceKey = new SequenceKey(tenantId, schemaName, sequenceName, nSequenceSaltBuckets);
        Sequence newSequences = new Sequence(sequenceKey);
        Sequence sequence = sequenceMap.putIfAbsent(sequenceKey, newSequences);
        if (sequence == null) {
            sequence = newSequences;
        }
        try {
            sequence.getLock().lock();
            // Now that we have the lock we need, create the sequence
            Append append = sequence.createSequence(startWith, incrementBy, cacheSize, timestamp, minValue, maxValue, cycle);
            Table htable = this.getTable(SchemaUtil
                    .getPhysicalName(PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME_BYTES, this.getProps()).getName());
            try {
                Result result = htable.append(append);
                return sequence.createSequence(result, minValue, maxValue, cycle);
            } catch (IOException e) {
                throw ClientUtil.parseServerException(e);
            } finally {
                Closeables.closeQuietly(htable);
            }
        } finally {
            sequence.getLock().unlock();
        }
    }

    @Override
    public long dropSequence(String tenantId, String schemaName, String sequenceName, long timestamp) throws SQLException {
        SequenceKey sequenceKey = new SequenceKey(tenantId, schemaName, sequenceName, nSequenceSaltBuckets);
        Sequence newSequences = new Sequence(sequenceKey);
        Sequence sequence = sequenceMap.putIfAbsent(sequenceKey, newSequences);
        if (sequence == null) {
            sequence = newSequences;
        }
        try {
            sequence.getLock().lock();
            // Now that we have the lock we need, create the sequence
            Append append = sequence.dropSequence(timestamp);
            Table htable = this.getTable(SchemaUtil
                    .getPhysicalName(PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME_BYTES, this.getProps()).getName());
            try {
                Result result = htable.append(append);
                return sequence.dropSequence(result);
            } catch (IOException e) {
                throw ClientUtil.parseServerException(e);
            } finally {
                Closeables.closeQuietly(htable);
            }
        } finally {
            sequence.getLock().unlock();
        }
    }

    /**
     * Gets the current sequence value
     * @throws SQLException if cached sequence cannot be found
     */
    @Override
    public long currentSequenceValue(SequenceKey sequenceKey, long timestamp) throws SQLException {
        Sequence sequence = sequenceMap.get(sequenceKey);
        if (sequence == null) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_CALL_CURRENT_BEFORE_NEXT_VALUE)
            .setSchemaName(sequenceKey.getSchemaName()).setTableName(sequenceKey.getSequenceName())
            .build().buildException();
        }
        sequence.getLock().lock();
        try {
            return sequence.currentValue(timestamp);
        } catch (EmptySequenceCacheException e) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_CALL_CURRENT_BEFORE_NEXT_VALUE)
            .setSchemaName(sequenceKey.getSchemaName()).setTableName(sequenceKey.getSequenceName())
            .build().buildException();
        } finally {
            sequence.getLock().unlock();
        }
    }

    /**
     * Verifies that sequences exist and reserves values for them if reserveValues is true
     */
    @Override
    public void validateSequences(List<SequenceAllocation> sequenceAllocations, long timestamp, long[] values, SQLException[] exceptions, Sequence.ValueOp action) throws SQLException {
        incrementSequenceValues(sequenceAllocations, timestamp, values, exceptions, action);
    }

    /**
     * Increment any of the set of sequences that need more values. These are the sequences
     * that are asking for the next value within a given statement. The returned sequences
     * are the ones that were not found because they were deleted by another client.
     * @param sequenceAllocations sorted list of sequence kyes
     * @param timestamp
     * @throws SQLException if any of the sequences cannot be found
     *
     */
    @Override
    public void incrementSequences(List<SequenceAllocation> sequenceAllocations, long timestamp, long[] values, SQLException[] exceptions) throws SQLException {
        incrementSequenceValues(sequenceAllocations, timestamp, values, exceptions, Sequence.ValueOp.INCREMENT_SEQUENCE);
    }

    private void incrementSequenceValues(List<SequenceAllocation> sequenceAllocations, long timestamp, long[] values, SQLException[] exceptions, Sequence.ValueOp op) throws SQLException {
        List<Sequence> sequences = Lists.newArrayListWithExpectedSize(sequenceAllocations.size());
        for (SequenceAllocation sequenceAllocation : sequenceAllocations) {
            SequenceKey key = sequenceAllocation.getSequenceKey();
            Sequence newSequences = new Sequence(key);
            Sequence sequence = getSequence(sequenceAllocation);
            if (sequence == null) {
                sequence = newSequences;
            }
            sequences.add(sequence);
        }
        try {
            for (Sequence sequence : sequences) {
                sequence.getLock().lock();
            }
            // Now that we have all the locks we need, increment the sequences
            List<Increment> incrementBatch = Lists.newArrayListWithExpectedSize(sequences.size());
            List<Sequence> toIncrementList = Lists.newArrayListWithExpectedSize(sequences.size());
            int[] indexes = new int[sequences.size()];
            for (int i = 0; i < sequences.size(); i++) {
                Sequence sequence = sequences.get(i);
                try {
                    values[i] = sequence.incrementValue(timestamp, op, sequenceAllocations.get(i).getNumAllocations());
                } catch (EmptySequenceCacheException e) {
                    indexes[toIncrementList.size()] = i;
                    toIncrementList.add(sequence);
                    Increment inc = sequence.newIncrement(timestamp, op, sequenceAllocations.get(i).getNumAllocations());
                    incrementBatch.add(inc);
                } catch (SQLException e) {
                    exceptions[i] = e;
                }
            }
            if (toIncrementList.isEmpty()) {
                return;
            }
            Table hTable = this.getTable(SchemaUtil.getPhysicalName(PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME_BYTES,this.getProps()).getName());
            Object[] resultObjects = new Object[incrementBatch.size()];
            SQLException sqlE = null;
            try {
                hTable.batch(incrementBatch, resultObjects);
            } catch (IOException e) {
                sqlE = ClientUtil.parseServerException(e);
            } catch (InterruptedException e) {
                // restore the interrupt status
                Thread.currentThread().interrupt();
                sqlE = new SQLExceptionInfo.Builder(SQLExceptionCode.INTERRUPTED_EXCEPTION)
                .setRootCause(e).build().buildException(); // FIXME ?
            } finally {
                try {
                    hTable.close();
                } catch (IOException e) {
                    if (sqlE == null) {
                        sqlE = ClientUtil.parseServerException(e);
                    } else {
                        sqlE.setNextException(ClientUtil.parseServerException(e));
                    }
                }
                if (sqlE != null) {
                    throw sqlE;
                }
            }
            for (int i=0;i<resultObjects.length;i++){
                Sequence sequence = toIncrementList.get(i);
                Result result = (Result)resultObjects[i];
                try {
                    long numToAllocate = Bytes.toLong(incrementBatch.get(i).getAttribute(SequenceRegionObserverConstants.NUM_TO_ALLOCATE));
                    values[indexes[i]] = sequence.incrementValue(result, op, numToAllocate);
                } catch (SQLException e) {
                    exceptions[indexes[i]] = e;
                }
            }
        } finally {
            for (Sequence sequence : sequences) {
                sequence.getLock().unlock();
            }
        }
    }

    /**
     * checks if sequenceAllocation's sequence there in sequenceMap, also returns Global Sequences
     * from Tenant sequenceAllocations
     * @param sequenceAllocation
     * @return
     */

    private Sequence getSequence(SequenceAllocation sequenceAllocation) {
        SequenceKey key = sequenceAllocation.getSequenceKey();
        if (key.getTenantId() == null) {
            return sequenceMap.putIfAbsent(key, new Sequence(key));
        } else {
            Sequence sequence = sequenceMap.get(key);
            if (sequence == null) {
                return sequenceMap.entrySet().stream()
                        .filter(entry -> compareSequenceKeysWithoutTenant(key, entry.getKey()))
                        .findFirst()
                        .map(Entry::getValue)
                        .orElse(null);
            } else {
                return sequence;
            }
        }
    }

    private boolean compareSequenceKeysWithoutTenant(SequenceKey keyToCompare, SequenceKey availableKey) {
        if (availableKey.getTenantId() != null) {
            return false;
        }
        boolean sameSchema = keyToCompare.getSchemaName() == null ? availableKey.getSchemaName() == null :
                keyToCompare.getSchemaName().equals(availableKey.getSchemaName());
        if (!sameSchema) {
            return false;
        }
        return keyToCompare.getSequenceName().equals(availableKey.getSequenceName());
    }

    @Override
    public void clearTableFromCache(final byte[] tenantId, final byte[] schemaName, final byte[] tableName,
            final long clientTS) throws SQLException {
        // clear the meta data cache for the table here
        boolean success = false;
        try {
            SQLException sqlE = null;
            long startTime = 0L;
            long systemCatalogRpcTime;
            Table htable = this.getTable(SchemaUtil
                    .getPhysicalName(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES, this.getProps()).getName());

            try {
                startTime = EnvironmentEdgeManager.currentTimeMillis();
                htable.coprocessorService(MetaDataService.class, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW,
                        new Batch.Call<MetaDataService, ClearTableFromCacheResponse>() {
                    @Override
                    public ClearTableFromCacheResponse call(MetaDataService instance) throws IOException {
                        RpcController controller = getController();
                        BlockingRpcCallback<ClearTableFromCacheResponse> rpcCallback = new BlockingRpcCallback<ClearTableFromCacheResponse>();
                        ClearTableFromCacheRequest.Builder builder = ClearTableFromCacheRequest.newBuilder();
                        builder.setTenantId(ByteStringer.wrap(tenantId));
                        builder.setTableName(ByteStringer.wrap(tableName));
                        builder.setSchemaName(ByteStringer.wrap(schemaName));
                        builder.setClientTimestamp(clientTS);
                        builder.setClientVersion(VersionUtil.encodeVersion(PHOENIX_MAJOR_VERSION, PHOENIX_MINOR_VERSION, PHOENIX_PATCH_NUMBER));
                        instance.clearTableFromCache(controller, builder.build(), rpcCallback);
                        checkForRemoteExceptions(controller);
                        return rpcCallback.get();
                    }
                });
                success = true;
            } catch (IOException e) {
                throw ClientUtil.parseServerException(e);
            } catch (Throwable e) {
                sqlE = new SQLException(e);
            } finally {
                try {
                    htable.close();
                    systemCatalogRpcTime = EnvironmentEdgeManager.currentTimeMillis() - startTime;
                    TableMetricsManager.updateMetricsForSystemCatalogTableMethod(Bytes.toString(tableName),
                            TIME_SPENT_IN_SYSTEM_TABLE_RPC_CALLS, systemCatalogRpcTime);
                    if (success) {
                        TableMetricsManager.updateMetricsForSystemCatalogTableMethod(Bytes.toString(tableName),
                                NUM_SYSTEM_TABLE_RPC_SUCCESS, 1);
                    } else {
                        TableMetricsManager.updateMetricsForSystemCatalogTableMethod(Bytes.toString(tableName),
                                NUM_SYSTEM_TABLE_RPC_FAILURES, 1);
                    }
                } catch (IOException e) {
                    if (sqlE == null) {
                        sqlE = ClientUtil.parseServerException(e);
                    } else {
                        sqlE.setNextException(ClientUtil.parseServerException(e));
                    }
                } finally {
                    if (sqlE != null) { throw sqlE; }
                }
            }
        } catch (Exception e) {
            throw new SQLException(ClientUtil.parseServerException(e));
        }
    }

    @Override
    public void returnSequences(List<SequenceKey> keys, long timestamp, SQLException[] exceptions) throws SQLException {
        List<Sequence> sequences = Lists.newArrayListWithExpectedSize(keys.size());
        for (SequenceKey key : keys) {
            Sequence newSequences = new Sequence(key);
            Sequence sequence = sequenceMap.putIfAbsent(key, newSequences);
            if (sequence == null) {
                sequence = newSequences;
            }
            sequences.add(sequence);
        }
        try {
            for (Sequence sequence : sequences) {
                sequence.getLock().lock();
            }
            // Now that we have all the locks we need, attempt to return the unused sequence values
            List<Append> mutations = Lists.newArrayListWithExpectedSize(sequences.size());
            List<Sequence> toReturnList = Lists.newArrayListWithExpectedSize(sequences.size());
            int[] indexes = new int[sequences.size()];
            for (int i = 0; i < sequences.size(); i++) {
                Sequence sequence = sequences.get(i);
                try {
                    Append append = sequence.newReturn(timestamp);
                    toReturnList.add(sequence);
                    mutations.add(append);
                } catch (EmptySequenceCacheException ignore) { // Nothing to return, so ignore
                }
            }
            if (toReturnList.isEmpty()) {
                return;
            }
            Table hTable = this.getTable(SchemaUtil
                    .getPhysicalName(PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME_BYTES, this.getProps()).getName());
            Object[] resultObjects = null;
            SQLException sqlE = null;
            try {
                hTable.batch(mutations, resultObjects);
            } catch (IOException e){
                sqlE = ClientUtil.parseServerException(e);
            } catch (InterruptedException e){
                // restore the interrupt status
                Thread.currentThread().interrupt();
                sqlE = new SQLExceptionInfo.Builder(SQLExceptionCode.INTERRUPTED_EXCEPTION)
                .setRootCause(e).build().buildException(); // FIXME ?
            } finally {
                try {
                    hTable.close();
                } catch (IOException e) {
                    if (sqlE == null) {
                        sqlE = ClientUtil.parseServerException(e);
                    } else {
                        sqlE.setNextException(ClientUtil.parseServerException(e));
                    }
                }
                if (sqlE != null) {
                    throw sqlE;
                }
            }
            for (int i=0;i<resultObjects.length;i++){
                Sequence sequence = toReturnList.get(i);
                Result result = (Result)resultObjects[i];
                try {
                    sequence.returnValue(result);
                } catch (SQLException e) {
                    exceptions[indexes[i]] = e;
                }
            }
        } finally {
            for (Sequence sequence : sequences) {
                sequence.getLock().unlock();
            }
        }
    }

    // Take no locks, as this only gets run when there are no open connections
    // so there's no danger of contention.
    private void returnAllSequences(ConcurrentMap<SequenceKey,Sequence> sequenceMap) throws SQLException {
        List<Append> mutations = Lists.newArrayListWithExpectedSize(sequenceMap.size());
        for (Sequence sequence : sequenceMap.values()) {
            mutations.addAll(sequence.newReturns());
        }
        if (mutations.isEmpty()) {
            return;
        }
        Table hTable = this.getTable(
                SchemaUtil.getPhysicalName(PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME_BYTES, this.getProps()).getName());
        SQLException sqlE = null;
        try {
            hTable.batch(mutations, null);
        } catch (IOException e) {
            sqlE = ClientUtil.parseServerException(e);
        } catch (InterruptedException e) {
            // restore the interrupt status
            Thread.currentThread().interrupt();
            sqlE = new SQLExceptionInfo.Builder(SQLExceptionCode.INTERRUPTED_EXCEPTION)
            .setRootCause(e).build().buildException(); // FIXME ?
        } finally {
            try {
                hTable.close();
            } catch (IOException e) {
                if (sqlE == null) {
                    sqlE = ClientUtil.parseServerException(e);
                } else {
                    sqlE.setNextException(ClientUtil.parseServerException(e));
                }
            }
            if (sqlE != null) {
                throw sqlE;
            }
        }
    }

    @Override
    public void addConnection(PhoenixConnection connection) throws SQLException {
        if (returnSequenceValues || shouldThrottleNumConnections) {
            synchronized (connectionCountLock) {
                connectionLimiter.acquireConnection(connection);
            }
        }
        // If lease renewal isn't enabled, these are never cleaned up. Tracking when renewals
        // aren't enabled also (presently) has no purpose.
        if (isRenewingLeasesEnabled()) {
          connectionQueues.get(getQueueIndex(connection)).add(new WeakReference<PhoenixConnection>(connection));
        }
    }

    @Override
    public void removeConnection(PhoenixConnection connection) throws SQLException {
        if (returnSequenceValues) {
            ConcurrentMap<SequenceKey,Sequence> formerSequenceMap = null;
            synchronized (connectionCountLock) {
                if (!connection.isInternalConnection()) {
                    if (connectionLimiter.isLastConnection()) {
                        if (!this.sequenceMap.isEmpty()) {
                            formerSequenceMap = this.sequenceMap;
                            this.sequenceMap = Maps.newConcurrentMap();
                        }
                    }
                }
            }
            // Since we're using the former sequenceMap, we can do this outside
            // the lock.
            if (formerSequenceMap != null) {
                // When there are no more connections, attempt to return any sequences
                returnAllSequences(formerSequenceMap);
            }
        }
        if (returnSequenceValues || connectionLimiter.isShouldThrottleNumConnections()) { //still need to decrement connection count
            synchronized (connectionCountLock) {
                connectionLimiter.returnConnection(connection);
            }
        }
    }

    private int getQueueIndex(PhoenixConnection conn) {
        return ThreadLocalRandom.current().nextInt(renewLeasePoolSize);
    }

    @Override
    public KeyValueBuilder getKeyValueBuilder() {
        return this.kvBuilder;
    }

    @Override
    public boolean supportsFeature(Feature feature) {
        FeatureSupported supported = featureMap.get(feature);
        if (supported == null) {
            return false;
        }
        return supported.isSupported(this);
    }

    @Override
    public String getUserName() {
        return userName;
    }
    
    @Override
    public User getUser() {
        return user;
    }

    @VisibleForTesting
    public void checkClosed() {
        if (closed) {
            throwConnectionClosedException();
        }
    }

    private void throwConnectionClosedIfNullMetaData() {
        if (latestMetaData == null) {
            throwConnectionClosedException();
        }
    }

    private void throwConnectionClosedException() {
        throw new IllegalStateException("Connection to the cluster is closed");
    }

    @Override
    public GuidePostsInfo getTableStats(GuidePostsKey key) throws SQLException {
        try {
            return tableStatsCache.get(key);
        } catch (ExecutionException e) {
            throw ClientUtil.parseServerException(e);
        }
    }

    @Override
    public int getSequenceSaltBuckets() {
        return nSequenceSaltBuckets;
    }

    @Override
    public void addFunction(PFunction function) throws SQLException {
        synchronized (latestMetaDataLock) {
            try {
                throwConnectionClosedIfNullMetaData();
                // If existing table isn't older than new table, don't replace
                // If a client opens a connection at an earlier timestamp, this can happen
                PFunction existingFunction = latestMetaData.getFunction(new PTableKey(function.getTenantId(), function.getFunctionName()));
                if (existingFunction.getTimeStamp() >= function.getTimeStamp()) {
                    return;
                }
            } catch (FunctionNotFoundException e) {}
            latestMetaData.addFunction(function);
            latestMetaDataLock.notifyAll();
        }
    }

    @Override
    public void removeFunction(PName tenantId, String function, long functionTimeStamp)
            throws SQLException {
        synchronized (latestMetaDataLock) {
            throwConnectionClosedIfNullMetaData();
            latestMetaData.removeFunction(tenantId, function, functionTimeStamp);
            latestMetaDataLock.notifyAll();
        }
    }

    @Override
    public MetaDataMutationResult getFunctions(PName tenantId, final List<Pair<byte[], Long>> functions,
            final long clientTimestamp) throws SQLException {
        final byte[] tenantIdBytes = tenantId == null ? ByteUtil.EMPTY_BYTE_ARRAY : tenantId.getBytes();
        return metaDataCoprocessorExec(null, tenantIdBytes,
                new Batch.Call<MetaDataService, MetaDataResponse>() {
            @Override
            public MetaDataResponse call(MetaDataService instance) throws IOException {
                RpcController controller = getController(SYSTEM_FUNCTION_HBASE_TABLE_NAME);
                BlockingRpcCallback<MetaDataResponse> rpcCallback =
                        new BlockingRpcCallback<MetaDataResponse>();
                GetFunctionsRequest.Builder builder = GetFunctionsRequest.newBuilder();
                builder.setTenantId(ByteStringer.wrap(tenantIdBytes));
                for (Pair<byte[], Long> function: functions) {
                    builder.addFunctionNames(ByteStringer.wrap(function.getFirst()));
                    builder.addFunctionTimestamps(function.getSecond().longValue());
                }
                builder.setClientTimestamp(clientTimestamp);
                builder.setClientVersion(VersionUtil.encodeVersion(PHOENIX_MAJOR_VERSION, PHOENIX_MINOR_VERSION, PHOENIX_PATCH_NUMBER));
                instance.getFunctions(controller, builder.build(), rpcCallback);
                checkForRemoteExceptions(controller);
                return rpcCallback.get();
            }
        }, SYSTEM_FUNCTION_NAME_BYTES);

    }

    @Override
    public MetaDataMutationResult getSchema(final String schemaName, final long clientTimestamp) throws SQLException {
        return metaDataCoprocessorExec(null, SchemaUtil.getSchemaKey(schemaName),
                new Batch.Call<MetaDataService, MetaDataResponse>() {
            @Override
            public MetaDataResponse call(MetaDataService instance) throws IOException {
                RpcController controller = getController();
                BlockingRpcCallback<MetaDataResponse> rpcCallback = new BlockingRpcCallback<MetaDataResponse>();
                GetSchemaRequest.Builder builder = GetSchemaRequest.newBuilder();
                builder.setSchemaName(schemaName);
                builder.setClientTimestamp(clientTimestamp);
                builder.setClientVersion(VersionUtil.encodeVersion(PHOENIX_MAJOR_VERSION, PHOENIX_MINOR_VERSION,
                        PHOENIX_PATCH_NUMBER));
                instance.getSchema(controller, builder.build(), rpcCallback);
                checkForRemoteExceptions(controller);
                return rpcCallback.get();
            }
        });

    }

    // TODO the mutations should be added to System functions table.
    @Override
    public MetaDataMutationResult createFunction(final List<Mutation> functionData,
            final PFunction function, final boolean temporary) throws SQLException {
        byte[][] rowKeyMetadata = new byte[2][];
        Mutation m = MetaDataUtil.getPutOnlyTableHeaderRow(functionData);
        byte[] key = m.getRow();
        SchemaUtil.getVarChars(key, rowKeyMetadata);
        byte[] tenantIdBytes = rowKeyMetadata[PhoenixDatabaseMetaData.TENANT_ID_INDEX];
        byte[] functionBytes = rowKeyMetadata[PhoenixDatabaseMetaData.FUNTION_NAME_INDEX];
        byte[] functionKey = SchemaUtil.getFunctionKey(tenantIdBytes, functionBytes);
        MetaDataMutationResult result = metaDataCoprocessorExec(null, functionKey,
                new Batch.Call<MetaDataService, MetaDataResponse>() {
            @Override
            public MetaDataResponse call(MetaDataService instance) throws IOException {
                RpcController controller = getController(SYSTEM_FUNCTION_HBASE_TABLE_NAME);
                BlockingRpcCallback<MetaDataResponse> rpcCallback =
                        new BlockingRpcCallback<MetaDataResponse>();
                CreateFunctionRequest.Builder builder = CreateFunctionRequest.newBuilder();
                for (Mutation m : functionData) {
                    MutationProto mp = ProtobufUtil.toProto(m);
                    builder.addTableMetadataMutations(mp.toByteString());
                }
                builder.setTemporary(temporary);
                builder.setReplace(function.isReplace());
                builder.setClientVersion(VersionUtil.encodeVersion(PHOENIX_MAJOR_VERSION, PHOENIX_MINOR_VERSION, PHOENIX_PATCH_NUMBER));
                instance.createFunction(controller, builder.build(), rpcCallback);
                checkForRemoteExceptions(controller);
                return rpcCallback.get();
            }
        }, SYSTEM_FUNCTION_NAME_BYTES);
        return result;
    }

    @VisibleForTesting
    static class RenewLeaseTask implements Runnable {

        private final LinkedBlockingQueue<WeakReference<PhoenixConnection>> connectionsQueue;
        private final Random random = new Random();
        private static final int MAX_WAIT_TIME = 1000;

        RenewLeaseTask(LinkedBlockingQueue<WeakReference<PhoenixConnection>> queue) {
            this.connectionsQueue = queue;
        }

        private void waitForRandomDuration() throws InterruptedException {
            new CountDownLatch(1).await(random.nextInt(MAX_WAIT_TIME), MILLISECONDS);
        }

        private static class InternalRenewLeaseTaskException extends Exception {
            public InternalRenewLeaseTaskException(String msg) {
                super(msg);
            }
        }

        @Override
        public void run() {
            try {
                int numConnections = connectionsQueue.size();
                boolean wait = true;
                // We keep adding items to the end of the queue. So to stop the loop, iterate only up to
                // whatever the current count is.
                while (numConnections > 0) {
                    if (wait) {
                        // wait for some random duration to prevent all threads from renewing lease at
                        // the same time.
                        waitForRandomDuration();
                        wait = false;
                    }
                    // It is guaranteed that this poll won't hang indefinitely because this is the
                    // only thread that removes items from the queue. Still adding a 1 ms timeout
                    // for sanity check.
                    WeakReference<PhoenixConnection> connRef =
                            connectionsQueue.poll(1, TimeUnit.MILLISECONDS);
                    if (connRef == null) {
                        throw new InternalRenewLeaseTaskException(
                                "Connection ref found to be null. This is a bug. Some other thread removed items from the connection queue.");
                    }
                    PhoenixConnection conn = connRef.get();
                    if (conn != null && !conn.isClosed()) {
                        LinkedBlockingQueue<WeakReference<TableResultIterator>> scannerQueue =
                                conn.getScanners();
                        // We keep adding items to the end of the queue. So to stop the loop,
                        // iterate only up to whatever the current count is.
                        int numScanners = scannerQueue.size();
                        int renewed = 0;
                        long start = EnvironmentEdgeManager.currentTimeMillis();
                        while (numScanners > 0) {
                            // It is guaranteed that this poll won't hang indefinitely because this is the
                            // only thread that removes items from the queue. Still adding a 1 ms timeout
                            // for sanity check.
                            WeakReference<TableResultIterator> ref =
                                    scannerQueue.poll(1, TimeUnit.MILLISECONDS);
                            if (ref == null) {
                                throw new InternalRenewLeaseTaskException(
                                        "TableResulIterator ref found to be null. This is a bug. Some other thread removed items from the scanner queue.");
                            }
                            TableResultIterator scanningItr = ref.get();
                            if (scanningItr != null) {
                                RenewLeaseStatus status = scanningItr.renewLease();
                                switch (status) {
                                case RENEWED:
                                    renewed++;
                                    // add it back at the tail
                                    scannerQueue.offer(new WeakReference<TableResultIterator>(
                                            scanningItr));
                                    LOGGER.info("Lease renewed for scanner: " + scanningItr);
                                    break;
                                // Scanner not initialized probably because next() hasn't been called on it yet. Enqueue it back to attempt lease renewal later.
                                case UNINITIALIZED:
                                // Threshold not yet reached. Re-enqueue to renew lease later.
                                case THRESHOLD_NOT_REACHED:
                                // Another scanner operation in progress. Re-enqueue to attempt renewing lease later.
                                case LOCK_NOT_ACQUIRED:
                                    // add it back at the tail
                                    scannerQueue.offer(new WeakReference<TableResultIterator>(
                                            scanningItr));
                                    break;
                                    // if lease wasn't renewed or scanner was closed, don't add the
                                    // scanner back to the queue.
                                case CLOSED:
                                case NOT_SUPPORTED:
                                    break;
                                }
                            }
                            numScanners--;
                        }
                        if (renewed > 0) {
                            LOGGER.info("Renewed leases for " + renewed + " scanner/s in "
                                    + (EnvironmentEdgeManager.currentTimeMillis() - start) + " ms ");
                        }
                        connectionsQueue.offer(connRef);
                    }
                    numConnections--;
                }
            } catch (InternalRenewLeaseTaskException e) {
                LOGGER.error("Exception thrown when renewing lease. Draining the queue of scanners ", e);
                // clear up the queue since the task is about to be unscheduled.
                connectionsQueue.clear();
                // throw an exception since we want the task execution to be suppressed because we just encountered an
                // exception that happened because of a bug.
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // restore the interrupt status
                LOGGER.error("Thread interrupted when renewing lease.", e);
            } catch (Exception e) {
                LOGGER.error("Exception thrown when renewing lease ", e);
                // don't drain the queue and swallow the exception in this case since we don't want the task
                // execution to be suppressed because renewing lease of a scanner failed.
            } catch (Throwable e) {
                LOGGER.error("Exception thrown when renewing lease. Draining the queue of scanners ", e);
                connectionsQueue.clear(); // clear up the queue since the task is about to be unscheduled.
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public long getRenewLeaseThresholdMilliSeconds() {
        return renewLeaseThreshold;
    }

    @Override
    public boolean isRenewingLeasesEnabled() {
        return supportsFeature(ConnectionQueryServices.Feature.RENEW_LEASE) && renewLeaseEnabled;
    }

    @Override
    public HRegionLocation getTableRegionLocation(byte[] tableName, byte[] row) throws SQLException {
        /*
         * Use HConnection.getRegionLocation as it uses the cache in HConnection, to get the region
         * to which specified row belongs to.
         */
        int retryCount = 0, maxRetryCount = 1;
        while (true) {
            TableName table = TableName.valueOf(tableName);
            try {
                return connection.getRegionLocator(table).getRegionLocation(row, false);
            } catch (org.apache.hadoop.hbase.TableNotFoundException e) {
                String fullName = Bytes.toString(tableName);
                throw new TableNotFoundException(SchemaUtil.getSchemaNameFromFullName(fullName), SchemaUtil.getTableNameFromFullName(fullName));
            } catch (IOException e) {
                LOGGER.error("Exception encountered in getTableRegionLocation for "
                        + "table: {}, retryCount: {}", table.getNameAsString(), retryCount, e);
                if (retryCount++ < maxRetryCount) { // One retry, in case split occurs while navigating
                    continue;
                }
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.GET_TABLE_REGIONS_FAIL)
                .setRootCause(e).build().buildException();
            }
        }
    }

    @Override
    public MetaDataMutationResult createSchema(final List<Mutation> schemaMutations, final String schemaName)
            throws SQLException {
        ensureNamespaceCreated(schemaName);
        Mutation m = MetaDataUtil.getPutOnlyTableHeaderRow(schemaMutations);
        byte[] key = m.getRow();
        MetaDataMutationResult result = metaDataCoprocessorExec(null, key,
                new Batch.Call<MetaDataService, MetaDataResponse>() {
            @Override
            public MetaDataResponse call(MetaDataService instance) throws IOException {
                RpcController controller = getController();
                BlockingRpcCallback<MetaDataResponse> rpcCallback = new BlockingRpcCallback<MetaDataResponse>();
                CreateSchemaRequest.Builder builder = CreateSchemaRequest.newBuilder();
                for (Mutation m : schemaMutations) {
                    MutationProto mp = ProtobufUtil.toProto(m);
                    builder.addTableMetadataMutations(mp.toByteString());
                }
                builder.setSchemaName(schemaName);
                builder.setClientVersion(VersionUtil.encodeVersion(PHOENIX_MAJOR_VERSION, PHOENIX_MINOR_VERSION,
                        PHOENIX_PATCH_NUMBER));
                instance.createSchema(controller, builder.build(), rpcCallback);
                checkForRemoteExceptions(controller);
                return rpcCallback.get();
            }
        });
        return result;
    }

    @Override
    public void addSchema(PSchema schema) throws SQLException {
        latestMetaData.addSchema(schema);
    }

    @Override
    public void removeSchema(PSchema schema, long schemaTimeStamp) {
        latestMetaData.removeSchema(schema, schemaTimeStamp);
    }

    @Override
    public MetaDataMutationResult dropSchema(final List<Mutation> schemaMetaData, final String schemaName)
            throws SQLException {
        final MetaDataMutationResult result = metaDataCoprocessorExec(null, SchemaUtil.getSchemaKey(schemaName),
                new Batch.Call<MetaDataService, MetaDataResponse>() {
            @Override
            public MetaDataResponse call(MetaDataService instance) throws IOException {
                RpcController controller = getController();
                BlockingRpcCallback<MetaDataResponse> rpcCallback = new BlockingRpcCallback<MetaDataResponse>();
                DropSchemaRequest.Builder builder = DropSchemaRequest.newBuilder();
                for (Mutation m : schemaMetaData) {
                    MutationProto mp = ProtobufUtil.toProto(m);
                    builder.addSchemaMetadataMutations(mp.toByteString());
                }
                builder.setSchemaName(schemaName);
                builder.setClientVersion(VersionUtil.encodeVersion(PHOENIX_MAJOR_VERSION, PHOENIX_MINOR_VERSION,
                        PHOENIX_PATCH_NUMBER));
                instance.dropSchema(controller, builder.build(), rpcCallback);
                checkForRemoteExceptions(controller);
                return rpcCallback.get();
            }
        });

        final MutationCode code = result.getMutationCode();
        switch (code) {
        case SCHEMA_ALREADY_EXISTS:
            ReadOnlyProps props = this.getProps();
            boolean dropMetadata = props.getBoolean(DROP_METADATA_ATTRIB, DEFAULT_DROP_METADATA);
            if (dropMetadata) {
                ensureNamespaceDropped(schemaName);
            }
            break;
        default:
            break;
        }
        return result;
    }

    private void ensureNamespaceDropped(String schemaName) throws SQLException {
        SQLException sqlE = null;
        try (Admin admin = getAdmin()) {
            final String quorum = ZKConfig.getZKQuorumServersString(config);
            final String znode = this.props.get(HConstants.ZOOKEEPER_ZNODE_PARENT);
            LOGGER.debug("Found quorum: " + quorum + ":" + znode);
            if (ClientUtil.isHBaseNamespaceAvailable(admin, schemaName)) {
                admin.deleteNamespace(schemaName);
            }
        } catch (IOException e) {
            sqlE = ClientUtil.parseServerException(e);
        } finally {
            if (sqlE != null) { throw sqlE; }
        }
    }

    /**
     * Manually adds {@link GuidePostsInfo} for a table to the client-side cache. Not a
     * {@link ConnectionQueryServices} method. Exposed for testing purposes.
     *
     * @param key Table name
     * @param info Stats instance
     */
    public void addTableStats(GuidePostsKey key, GuidePostsInfo info) {
        this.tableStatsCache.put(Objects.requireNonNull(key), Objects.requireNonNull(info));
    }

    @Override
    public void invalidateStats(GuidePostsKey key) {
        this.tableStatsCache.invalidate(Objects.requireNonNull(key));
    }

    @Override
    public boolean isUpgradeRequired() {
        return upgradeRequired.get();
    }

    @Override
    public void clearUpgradeRequired() {
        upgradeRequired.set(false);
    }

    @Override
    public Configuration getConfiguration() {
        return config;
    }

    @Override
    public QueryLoggerDisruptor getQueryDisruptor() {
        return this.queryDisruptor;
    }

    @Override
    public synchronized PhoenixTransactionClient initTransactionClient(Provider provider) throws SQLException {
        PhoenixTransactionClient client = txClients[provider.ordinal()];
        if (client == null) {
            client = txClients[provider.ordinal()] = provider.getTransactionProvider().getTransactionClient(config, connectionInfo);
        }
        return client;
    }

    @VisibleForTesting
    public List<LinkedBlockingQueue<WeakReference<PhoenixConnection>>> getCachedConnections() {
      return connectionQueues;
    }
}
