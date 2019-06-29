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
import static org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.MAX_VERSIONS;
import static org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.TTL;
import static org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.REPLICATION_SCOPE;
import static org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.KEEP_DELETED_CELLS;
import static org.apache.phoenix.coprocessor.MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP;
import static org.apache.phoenix.coprocessor.MetaDataProtocol.PHOENIX_MAJOR_VERSION;
import static org.apache.phoenix.coprocessor.MetaDataProtocol.PHOENIX_MINOR_VERSION;
import static org.apache.phoenix.coprocessor.MetaDataProtocol.PHOENIX_PATCH_NUMBER;
import static org.apache.phoenix.coprocessor.MetaDataProtocol.getVersion;
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
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_STATS_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_TASK_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TASK_TABLE_TTL;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TENANT_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_CONSTANT;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HCONNECTIONS_COUNTER;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_PHOENIX_CONNECTIONS_THROTTLED_COUNTER;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_QUERY_SERVICES_COUNTER;
import static org.apache.phoenix.query.QueryConstants.DEFAULT_COLUMN_FAMILY;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_DROP_METADATA;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_RENEW_LEASE_ENABLED;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_RENEW_LEASE_THREAD_POOL_SIZE;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_RENEW_LEASE_THRESHOLD_MILLISECONDS;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_RUN_RENEW_LEASE_FREQUENCY_INTERVAL_MILLISECONDS;
import static org.apache.phoenix.util.UpgradeUtil.addParentToChildLinks;
import static org.apache.phoenix.util.UpgradeUtil.addViewIndexToParentLinks;
import static org.apache.phoenix.util.UpgradeUtil.getSysCatalogSnapshotName;
import static org.apache.phoenix.util.UpgradeUtil.moveChildLinks;
import static org.apache.phoenix.util.UpgradeUtil.upgradeTo4_5_0;
import static org.apache.phoenix.util.UpgradeUtil.syncTableAndIndexProperties;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.ref.WeakReference;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
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
import java.util.TreeMap;
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

import com.google.common.base.Strings;
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
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.PhoenixRpcSchedulerFactory;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.IndexHalfStoreFileReaderGenerator;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.phoenix.compile.MutationPlan;
import org.apache.phoenix.coprocessor.GroupedAggregateRegionObserver;
import org.apache.phoenix.coprocessor.MetaDataEndpointImpl;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MutationCode;
import org.apache.phoenix.coprocessor.MetaDataRegionObserver;
import org.apache.phoenix.coprocessor.ScanRegionObserver;
import org.apache.phoenix.coprocessor.SequenceRegionObserver;
import org.apache.phoenix.coprocessor.ServerCachingEndpointImpl;
import org.apache.phoenix.coprocessor.TaskRegionObserver;
import org.apache.phoenix.coprocessor.UngroupedAggregateRegionObserver;
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
import org.apache.phoenix.exception.PhoenixIOException;
import org.apache.phoenix.exception.RetriableUpgradeException;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.exception.UpgradeInProgressException;
import org.apache.phoenix.exception.UpgradeNotRequiredException;
import org.apache.phoenix.exception.UpgradeRequiredException;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.hbase.index.IndexRegionSplitPolicy;
import org.apache.phoenix.hbase.index.Indexer;
import org.apache.phoenix.hbase.index.covered.NonTxIndexBuilder;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.hbase.index.util.VersionUtil;
import org.apache.phoenix.index.GlobalIndexChecker;
import org.apache.phoenix.index.PhoenixIndexBuilder;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.index.PhoenixTransactionalIndexer;
import org.apache.phoenix.iterate.TableResultIterator;
import org.apache.phoenix.iterate.TableResultIterator.RenewLeaseStatus;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver.ConnectionInfo;
import org.apache.phoenix.log.QueryLoggerDisruptor;
import org.apache.phoenix.parse.PFunction;
import org.apache.phoenix.parse.PSchema;
import org.apache.phoenix.protobuf.ProtobufUtil;
import org.apache.phoenix.schema.ColumnAlreadyExistsException;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
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
import org.apache.phoenix.schema.PSynchronizedMetaData;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.ReadOnlyTableException;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.schema.Sequence;
import org.apache.phoenix.schema.SequenceAllocation;
import org.apache.phoenix.schema.SequenceKey;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.SystemFunctionSplitPolicy;
import org.apache.phoenix.schema.SystemStatsSplitPolicy;
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
import org.apache.phoenix.util.Closeables;
import org.apache.phoenix.util.ConfigUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
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
import org.apache.phoenix.util.ServerUtil;
import org.apache.phoenix.util.UpgradeUtil;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class ConnectionQueryServicesImpl extends DelegateQueryServices implements ConnectionQueryServices {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionQueryServicesImpl.class);
    private static final int INITIAL_CHILD_SERVICES_CAPACITY = 100;
    private static final int DEFAULT_OUT_OF_ORDER_MUTATIONS_WAIT_TIME_MS = 1000;
    private static final int TTL_FOR_MUTEX = 15 * 60; // 15min
    private final GuidePostsCacheProvider
            GUIDE_POSTS_CACHE_PROVIDER = new GuidePostsCacheProvider();
    protected final Configuration config;
    protected final ConnectionInfo connectionInfo;
    // Copy of config.getProps(), but read-only to prevent synchronization that we
    // don't need.
    private final ReadOnlyProps props;
    private final String userName;
    private final User user;
    private final ConcurrentHashMap<ImmutableBytesWritable,ConnectionQueryServices> childServices;
    private final GuidePostsCacheWrapper tableStatsCache;

    // Cache the latest meta data here for future connections
    // writes guarded by "latestMetaDataLock"
    private volatile PMetaData latestMetaData;
    private final Object latestMetaDataLock = new Object();

    // Lowest HBase version on the cluster.
    private int lowestClusterHBaseVersion = Integer.MAX_VALUE;
    private boolean hasIndexWALCodec = true;

    @GuardedBy("connectionCountLock")
    private int connectionCount = 0;
    private final Object connectionCountLock = new Object();
    private final boolean returnSequenceValues ;

    private Connection connection;
    private ZKClientService txZKClientService;
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
    private PhoenixTransactionClient[] txClients = new PhoenixTransactionClient[TransactionFactory.Provider.values().length];;
    /*
     * We can have multiple instances of ConnectionQueryServices. By making the thread factory
     * static, renew lease thread names will be unique across them.
     */
    private static final ThreadFactory renewLeaseThreadFactory = new RenewLeaseThreadFactory();
    private final boolean renewLeaseEnabled;
    private final boolean isAutoUpgradeEnabled;
    private final AtomicBoolean upgradeRequired = new AtomicBoolean(false);
    private final int maxConnectionsAllowed;
    private final boolean shouldThrottleNumConnections;
    public static final byte[] MUTEX_LOCKED = "MUTEX_LOCKED".getBytes();

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
        return new PSynchronizedMetaData(new PMetaDataImpl(INITIAL_META_DATA_TABLE_CAPACITY, getProps()));
    }

    /**
     * Construct a ConnectionQueryServicesImpl that represents a connection to an HBase
     * cluster.
     * @param services base services from where we derive our default configuration
     * @param connectionInfo to provide connection information
     * @param info hbase configuration properties
     * @throws SQLException
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
        this.connectionInfo = connectionInfo;

        // Without making a copy of the configuration we cons up, we lose some of our properties
        // on the server side during testing.
        this.config = HBaseFactoryProvider.getConfigurationFactory().getConfiguration(config);
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

        // A little bit of a smell to leak `this` here, but should not be a problem
        this.tableStatsCache = GUIDE_POSTS_CACHE_PROVIDER.getGuidePostsCache(props.get(GUIDE_POSTS_CACHE_FACTORY_CLASS,
                QueryServicesOptions.DEFAULT_GUIDE_POSTS_CACHE_FACTORY_CLASS), this, config);

        this.isAutoUpgradeEnabled = config.getBoolean(AUTO_UPGRADE_ENABLED, QueryServicesOptions.DEFAULT_AUTO_UPGRADE_ENABLED);
        this.maxConnectionsAllowed = config.getInt(QueryServices.CLIENT_CONNECTION_MAX_ALLOWED_CONNECTIONS,
            QueryServicesOptions.DEFAULT_CLIENT_CONNECTION_MAX_ALLOWED_CONNECTIONS);
        this.shouldThrottleNumConnections = (maxConnectionsAllowed > 0);
        if (!QueryUtil.isServerConnection(props)) {
            //Start queryDistruptor everytime as log level can be change at connection level as well, but we can avoid starting for server connections.
            try {
                this.queryDisruptor = new QueryLoggerDisruptor(this.config);
            } catch (SQLException e) {
                LOGGER.warn("Unable to initiate qeuery logging service !!");
                e.printStackTrace();
            }
        }

    }

    private void openConnection() throws SQLException {
        try {
            this.connection = HBaseFactoryProvider.getHConnectionFactory().createConnection(this.config);
            GLOBAL_HCONNECTIONS_COUNTER.increment();
            LOGGER.info("HConnection established. Stacktrace for informational purposes: " + connection + " " +  LogUtil.getCallerStackTrace());
        } catch (IOException e) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_ESTABLISH_CONNECTION)
            .setRootCause(e).build().buildException();
        }
        if (this.connection.isClosed()) { // TODO: why the heck doesn't this throw above?
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_ESTABLISH_CONNECTION).build().buildException();
        }
    }

    @Override
    public Table getTable(byte[] tableName) throws SQLException {
        try {
            return HBaseFactoryProvider.getHTableFactory().getTable(tableName, connection, null);
        } catch (org.apache.hadoop.hbase.TableNotFoundException e) {
            throw new TableNotFoundException(SchemaUtil.getSchemaNameFromFullName(tableName), SchemaUtil.getTableNameFromFullName(tableName));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public TableDescriptor getTableDescriptor(byte[] tableName) throws SQLException {
        Table htable = getTable(tableName);
        try {
            return htable.getDescriptor();
        } catch (IOException e) {
            if(e instanceof org.apache.hadoop.hbase.TableNotFoundException
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
                        if (connection != null) connection.close();
                        GLOBAL_HCONNECTIONS_COUNTER.decrement();
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
                        sqlE = ServerUtil.parseServerException(e);
                    } else {
                        sqlE.setNextException(ServerUtil.parseServerException(e));
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

    @Override
    public List<HRegionLocation> getAllTableRegions(byte[] tableName) throws SQLException {
        /*
         * Use HConnection.getRegionLocation as it uses the cache in HConnection, while getting
         * all region locations from the HTable doesn't.
         */
        int retryCount = 0, maxRetryCount = 1;
        boolean reload =false;
        while (true) {
            try {
                // We could surface the package projected HConnectionImplementation.getNumberOfCachedRegionLocations
                // to get the sizing info we need, but this would require a new class in the same package and a cast
                // to this implementation class, so it's probably not worth it.
                List<HRegionLocation> locations = Lists.newArrayList();
                byte[] currentKey = HConstants.EMPTY_START_ROW;
                do {
                    HRegionLocation regionLocation = ((ClusterConnection)connection).getRegionLocation(
                            TableName.valueOf(tableName), currentKey, reload);
                    locations.add(regionLocation);
                    currentKey = regionLocation.getRegion().getEndKey();
                } while (!Bytes.equals(currentKey, HConstants.EMPTY_END_ROW));
                return locations;
            } catch (org.apache.hadoop.hbase.TableNotFoundException e) {
                String fullName = Bytes.toString(tableName);
                throw new TableNotFoundException(fullName);
            } catch (IOException e) {
                if (retryCount++ < maxRetryCount) { // One retry, in case split occurs while navigating
                    reload = true;
                    continue;
                }
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.GET_TABLE_REGIONS_FAIL)
                .setRootCause(e).build().buildException();
            }
        }
    }

    @Override
    public void addTable(PTable table, long resolvedTime) throws SQLException {
        synchronized (latestMetaDataLock) {
            try {
                throwConnectionClosedIfNullMetaData();
                // If existing table isn't older than new table, don't replace
                // If a client opens a connection at an earlier timestamp, this can happen
                PTable existingTable = latestMetaData.getTableRef(new PTableKey(table.getTenantId(), table.getName().getString())).getTable();
                if (existingTable.getTimeStamp() >= table.getTimeStamp()) {
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
            long endTime = System.currentTimeMillis() + DEFAULT_OUT_OF_ORDER_MUTATIONS_WAIT_TIME_MS;
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
                            LOGGER.warn("Attempt to cache older version of " + tableName + ": current= " + table.getSequenceNumber() + ", new=" + tableSeqNum);
                            break;
                        }
                    } catch (TableNotFoundException e) {
                    }
                    long waitTime = endTime - System.currentTimeMillis();
                    // We waited long enough - just remove the table from the cache
                    // and the next time it's used it'll be pulled over from the server.
                    if (waitTime <= 0) {
                        LOGGER.warn("Unable to update meta data repo within " + (DEFAULT_OUT_OF_ORDER_MUTATIONS_WAIT_TIME_MS/1000) + " seconds for " + tableName);
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


    @Override
    public PhoenixConnection connect(String url, Properties info) throws SQLException {
        checkClosed();
        PMetaData metadata = latestMetaData;
        throwConnectionClosedIfNullMetaData();
        metadata = metadata.clone();
        return new PhoenixConnection(this, url, info, metadata);
    }


    private ColumnFamilyDescriptor generateColumnFamilyDescriptor(Pair<byte[],Map<String,Object>> family, PTableType tableType) throws SQLException {
        ColumnFamilyDescriptorBuilder columnDescBuilder = ColumnFamilyDescriptorBuilder.newBuilder(family.getFirst());
        if (tableType != PTableType.VIEW) {
            columnDescBuilder.setDataBlockEncoding(SchemaUtil.DEFAULT_DATA_BLOCK_ENCODING);
            columnDescBuilder.setBloomFilterType(BloomType.NONE);
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

    private TableDescriptorBuilder generateTableDescriptor(byte[] physicalTableName, TableDescriptor existingDesc,
            PTableType tableType, Map<String, Object> tableProps, List<Pair<byte[], Map<String, Object>>> families,
            byte[][] splits, boolean isNamespaceMapped) throws SQLException {
        String defaultFamilyName = (String)tableProps.remove(PhoenixDatabaseMetaData.DEFAULT_COLUMN_FAMILY_NAME);
        TableDescriptorBuilder tableDescriptorBuilder = (existingDesc != null) ?TableDescriptorBuilder.newBuilder(existingDesc)
        : TableDescriptorBuilder.newBuilder(TableName.valueOf(physicalTableName));

        ColumnFamilyDescriptor dataTableColDescForIndexTablePropSyncing = null;
        if (tableType == PTableType.INDEX || MetaDataUtil.isViewIndex(Bytes.toString(physicalTableName))) {
            byte[] defaultFamilyBytes =
                    defaultFamilyName == null ? QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES : Bytes.toBytes(defaultFamilyName);

            final TableDescriptor baseTableDesc;
            if (MetaDataUtil.isViewIndex(Bytes.toString(physicalTableName))) {
                // Handles indexes created on views for single-tenant tables and
                // global indexes created on views of multi-tenant tables
                baseTableDesc = this.getTableDescriptor(Bytes.toBytes(MetaDataUtil.getViewIndexUserTableName(Bytes.toString(physicalTableName))));
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
        addCoprocessors(physicalTableName, tableDescriptorBuilder, tableType, tableProps);

        // PHOENIX-3072: Set index priority if this is a system table or index table
        if (tableType == PTableType.SYSTEM) {
            tableDescriptorBuilder.setValue(QueryConstants.PRIORITY,
                    String.valueOf(PhoenixRpcSchedulerFactory.getMetadataPriority(config)));
        } else if (tableType == PTableType.INDEX // Global, mutable index
                && !isLocalIndexTable(tableDescriptorBuilder.build().getColumnFamilyNames())
                && !Boolean.TRUE.equals(tableProps.get(PhoenixDatabaseMetaData.IMMUTABLE_ROWS))) {
            tableDescriptorBuilder.setValue(QueryConstants.PRIORITY,
                    String.valueOf(PhoenixRpcSchedulerFactory.getIndexPriority(config)));
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


    private void addCoprocessors(byte[] tableName, TableDescriptorBuilder builder, PTableType tableType, Map<String,Object> tableProps) throws SQLException {
        // The phoenix jar must be available on HBase classpath
        int priority = props.getInt(QueryServices.COPROCESSOR_PRIORITY_ATTRIB, QueryServicesOptions.DEFAULT_COPROCESSOR_PRIORITY);
        try {
            TableDescriptor newDesc = builder.build();
            TransactionFactory.Provider provider = getTransactionProvider(tableProps);
            boolean isTransactional = (provider != null);

            boolean indexRegionObserverEnabled = config.getBoolean(
                    QueryServices.INDEX_REGION_OBSERVER_ENABLED_ATTRIB,
                    QueryServicesOptions.DEFAULT_INDEX_REGION_OBSERVER_ENABLED);

            if (tableType == PTableType.INDEX && !isTransactional) {
                if (!indexRegionObserverEnabled && newDesc.hasCoprocessor(GlobalIndexChecker.class.getName())) {
                    builder.removeCoprocessor(GlobalIndexChecker.class.getName());
                } else if (indexRegionObserverEnabled && !newDesc.hasCoprocessor(GlobalIndexChecker.class.getName())) {
                    builder.addCoprocessor(GlobalIndexChecker.class.getName(), null, priority - 1, null);
                }
            }

            if(!newDesc.hasCoprocessor(ScanRegionObserver.class.getName())) {
                builder.addCoprocessor(ScanRegionObserver.class.getName(), null, priority, null);
            }
            if(!newDesc.hasCoprocessor(UngroupedAggregateRegionObserver.class.getName())) {
                builder.addCoprocessor(UngroupedAggregateRegionObserver.class.getName(), null, priority, null);
            }
            if(!newDesc.hasCoprocessor(GroupedAggregateRegionObserver.class.getName())) {
                builder.addCoprocessor(GroupedAggregateRegionObserver.class.getName(), null, priority, null);
            }
            if(!newDesc.hasCoprocessor(ServerCachingEndpointImpl.class.getName())) {
                builder.addCoprocessor(ServerCachingEndpointImpl.class.getName(), null, priority, null);
            }

            // TODO: better encapsulation for this
            // Since indexes can't have indexes, don't install our indexing coprocessor for indexes.
            // Also don't install on the SYSTEM.CATALOG and SYSTEM.STATS table because we use
            // all-or-none mutate class which break when this coprocessor is installed (PHOENIX-1318).
            if ((tableType != PTableType.INDEX && tableType != PTableType.VIEW)
                    && !SchemaUtil.isMetaTable(tableName)
                    && !SchemaUtil.isStatsTable(tableName)) {
                if (isTransactional) {
                    if (!newDesc.hasCoprocessor(PhoenixTransactionalIndexer.class.getName())) {
                        builder.addCoprocessor(PhoenixTransactionalIndexer.class.getName(), null, priority, null);
                    }
                    // For alter table, remove non transactional index coprocessor
                    if (newDesc.hasCoprocessor(Indexer.class.getName())) {
                        builder.removeCoprocessor(Indexer.class.getName());
                    }
                    if (newDesc.hasCoprocessor(IndexRegionObserver.class.getName())) {
                        builder.removeCoprocessor(IndexRegionObserver.class.getName());
                    }
                } else {
                    // If exception on alter table to transition back to non transactional
                    if (newDesc.hasCoprocessor(PhoenixTransactionalIndexer.class.getName())) {
                        builder.removeCoprocessor(PhoenixTransactionalIndexer.class.getName());
                    }
                    if (indexRegionObserverEnabled) {
                        if (newDesc.hasCoprocessor(Indexer.class.getName())) {
                            builder.removeCoprocessor(Indexer.class.getName());
                        }
                        if (!newDesc.hasCoprocessor(IndexRegionObserver.class.getName())) {
                            Map<String, String> opts = Maps.newHashMapWithExpectedSize(1);
                            opts.put(NonTxIndexBuilder.CODEC_CLASS_NAME_KEY, PhoenixIndexCodec.class.getName());
                            IndexRegionObserver.enableIndexing(builder, PhoenixIndexBuilder.class, opts, priority);
                        }
                    } else {
                        if (newDesc.hasCoprocessor(IndexRegionObserver.class.getName())) {
                            builder.removeCoprocessor(IndexRegionObserver.class.getName());
                        }
                        if (!newDesc.hasCoprocessor(Indexer.class.getName())) {
                            Map<String, String> opts = Maps.newHashMapWithExpectedSize(1);
                            opts.put(NonTxIndexBuilder.CODEC_CLASS_NAME_KEY, PhoenixIndexCodec.class.getName());
                            Indexer.enableIndexing(builder, PhoenixIndexBuilder.class, opts, priority);
                        }
                        if (newDesc.hasCoprocessor(IndexRegionObserver.class.getName())) {
                            builder.removeCoprocessor(IndexRegionObserver.class.getName());
                        }
                    }
                }
            }

            if ((SchemaUtil.isStatsTable(tableName) || SchemaUtil.isMetaTable(tableName))
                    && !newDesc.hasCoprocessor(MultiRowMutationEndpoint.class.getName())) {
                builder.addCoprocessor(MultiRowMutationEndpoint.class.getName(),
                        null, priority, null);
            }

            Set<byte[]> familiesKeys = builder.build().getColumnFamilyNames();
            for(byte[] family: familiesKeys) {
                if(Bytes.toString(family).startsWith(QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX)) {
                    if(!newDesc.hasCoprocessor(IndexHalfStoreFileReaderGenerator.class.getName())) {
                        builder.addCoprocessor(IndexHalfStoreFileReaderGenerator.class.getName(),
                            null, priority, null);
                        break;
                    }
                }
            }

            // Setup split policy on Phoenix metadata table to ensure that the key values of a Phoenix table
            // stay on the same region.
            if (SchemaUtil.isMetaTable(tableName) || SchemaUtil.isFunctionTable(tableName)) {
                if (!newDesc.hasCoprocessor(MetaDataEndpointImpl.class.getName())) {
                    builder.addCoprocessor(MetaDataEndpointImpl.class.getName(), null, priority, null);
                }
                if(SchemaUtil.isMetaTable(tableName) ) {
                    if (!newDesc.hasCoprocessor(MetaDataRegionObserver.class.getName())) {
                        builder.addCoprocessor(MetaDataRegionObserver.class.getName(), null, priority + 1, null);
                    }
                }
            } else if (SchemaUtil.isSequenceTable(tableName)) {
                if(!newDesc.hasCoprocessor(SequenceRegionObserver.class.getName())) {
                    builder.addCoprocessor(SequenceRegionObserver.class.getName(), null, priority, null);
                }
            } else if (SchemaUtil.isTaskTable(tableName)) {
                if(!newDesc.hasCoprocessor(TaskRegionObserver.class.getName())) {
                    builder.addCoprocessor(TaskRegionObserver.class.getName(), null, priority, null);
            }
        }

            if (isTransactional) {
                Class<? extends RegionObserver> coprocessorClass = provider.getTransactionProvider().getCoprocessor();
                if (!newDesc.hasCoprocessor(coprocessorClass.getName())) {
                    builder.addCoprocessor(coprocessorClass.getName(), null, priority - 10, null);
                }
                Class<? extends RegionObserver> coprocessorGCClass = provider.getTransactionProvider().getGCCoprocessor();
                if (coprocessorGCClass != null) {
                    if (!newDesc.hasCoprocessor(coprocessorGCClass.getName())) {
                        builder.addCoprocessor(coprocessorGCClass.getName(), null, priority - 10, null);
                    }
                }
            } else {
                // Remove all potential transactional coprocessors
                for (TransactionFactory.Provider aprovider : TransactionFactory.Provider.values()) {
                    Class<? extends RegionObserver> coprocessorClass = aprovider.getTransactionProvider().getCoprocessor();
                    Class<? extends RegionObserver> coprocessorGCClass = aprovider.getTransactionProvider().getGCCoprocessor();
                    if (coprocessorClass != null && newDesc.hasCoprocessor(coprocessorClass.getName())) {
                        builder.removeCoprocessor(coprocessorClass.getName());
                    }
                    if (coprocessorGCClass != null && newDesc.hasCoprocessor(coprocessorGCClass.getName())) {
                        builder.removeCoprocessor(coprocessorGCClass.getName());
                    }
                }
            }
        } catch (IOException e) {
            throw ServerUtil.parseServerException(e);
        }
    }

    private TransactionFactory.Provider getTransactionProvider(Map<String,Object> tableProps) {
        TransactionFactory.Provider provider = (TransactionFactory.Provider)TableProperty.TRANSACTION_PROVIDER.getValue(tableProps);
        return provider;
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
                    + (numTries > 1 ? ("after trying " + numTries + (numTries > 1 ? "times." : "time.")) : ""));
        } else {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Operation "
                        + op.getOperationName()
                        + " completed within "
                        + watch.elapsedMillis()
                        + "ms "
                        + (numTries > 1 ? ("after trying " + numTries + (numTries > 1 ? "times." : "time.")) : ""));
            }
        }
    }

    private boolean allowOnlineTableSchemaUpdate() {
        return props.getBoolean(
                QueryServices.ALLOW_ONLINE_TABLE_SCHEMA_UPDATE,
                QueryServicesOptions.DEFAULT_ALLOW_ONLINE_TABLE_SCHEMA_UPDATE);
    }

    void ensureNamespaceCreated(String schemaName) throws SQLException {
        SQLException sqlE = null;
        try (Admin admin = getAdmin()) {
            NamespaceDescriptor namespaceDescriptor = null;
            try {
                namespaceDescriptor = admin.getNamespaceDescriptor(schemaName);
            } catch (org.apache.hadoop.hbase.NamespaceNotFoundException e) {

            }
            if (namespaceDescriptor == null) {
                namespaceDescriptor = NamespaceDescriptor.create(schemaName).build();
                admin.createNamespace(namespaceDescriptor);
            }
            return;
        } catch (IOException e) {
            sqlE = ServerUtil.parseServerException(e);
        } finally {
            if (sqlE != null) { throw sqlE; }
        }
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
    private TableDescriptor ensureTableCreated(byte[] physicalTableName, PTableType tableType, Map<String, Object> props,
            List<Pair<byte[], Map<String, Object>>> families, byte[][] splits, boolean modifyExistingMetaData,
            boolean isNamespaceMapped, boolean isDoNotUpgradePropSet) throws SQLException {
        SQLException sqlE = null;
        TableDescriptor existingDesc = null;
        boolean isMetaTable = SchemaUtil.isMetaTable(physicalTableName);
        boolean tableExist = true;
        try (Admin admin = getAdmin()) {
            final String quorum = ZKConfig.getZKQuorumServersString(config);
            final String znode = this.getProps().get(HConstants.ZOOKEEPER_ZNODE_PARENT);
            LOGGER.debug("Found quorum: " + quorum + ":" + znode);

            if (isMetaTable) {
                if(SchemaUtil.isNamespaceMappingEnabled(PTableType.SYSTEM, this.getProps())) {
                    try {
                        // SYSTEM namespace needs to be created via HBase APIs because "CREATE SCHEMA" statement tries to write
                        // its metadata in SYSTEM:CATALOG table. Without SYSTEM namespace, SYSTEM:CATALOG table cannot be created
                        ensureNamespaceCreated(QueryConstants.SYSTEM_SCHEMA_NAME);
                    } catch (PhoenixIOException e) {
                        // We could either:
                        // 1) Not access the NS descriptor. The NS may or may not exist at this point
                        // 2) We could not create the NS
                        // Regardless of the case 1 or 2, if we eventually try to migrate SYSTEM tables to the SYSTEM
                        // namespace using the {@link ensureSystemTablesMigratedToSystemNamespace(ReadOnlyProps)} method,
                        // if the NS does not exist, we will error as expected, or
                        // if the NS does exist and tables are already mapped, the check will exit gracefully
                    }
                    if (admin.tableExists(SchemaUtil.getPhysicalTableName(SYSTEM_CATALOG_NAME_BYTES, false))) {
                        // SYSTEM.CATALOG exists, so at this point, we have 3 cases:
                        // 1) If server-side namespace mapping is disabled, throw Inconsistent namespace mapping exception
                        // 2) If server-side namespace mapping is enabled and SYSCAT needs to be upgraded, upgrade SYSCAT
                        //    and also migrate SYSTEM tables to the SYSTEM namespace
                        // 3. If server-side namespace mapping is enabled and SYSCAT doesn't need to be upgraded, we still
                        //    need to migrate SYSTEM tables to the SYSTEM namespace using the
                        //    {@link ensureSystemTablesMigratedToSystemNamespace(ReadOnlyProps)} method (as part of
                        //    {@link upgradeSystemTables(String, Properties)})
                        checkClientServerCompatibility(SYSTEM_CATALOG_NAME_BYTES);
                        // Thrown so we can force an upgrade which will just migrate SYSTEM tables to the SYSTEM namespace
                        throw new UpgradeRequiredException(MIN_SYSTEM_TABLE_TIMESTAMP);
                    }
                } else if (admin.tableExists(SchemaUtil.getPhysicalTableName(SYSTEM_CATALOG_NAME_BYTES, true))) {
                    // If SYSTEM:CATALOG exists, but client-side namespace mapping for SYSTEM tables is disabled, throw an exception
                    throw new SQLExceptionInfo.Builder(
                      SQLExceptionCode.INCONSISTENT_NAMESPACE_MAPPING_PROPERTIES)
                      .setMessage("Cannot initiate connection as "
                        + SchemaUtil.getPhysicalTableName(SYSTEM_CATALOG_NAME_BYTES, true)
                        + " is found but client does not have "
                        + IS_NAMESPACE_MAPPING_ENABLED + " enabled")
                      .build().buildException();
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

            TableDescriptorBuilder newDesc = generateTableDescriptor(physicalTableName, existingDesc, tableType, props, families,
                    splits, isNamespaceMapped);
            
            if (!tableExist) {
                if (SchemaUtil.isSystemTable(physicalTableName) && !isUpgradeRequired() && (!isAutoUpgradeEnabled || isDoNotUpgradePropSet)) {
                    // Disallow creating the SYSTEM.CATALOG or SYSTEM:CATALOG HBase table
                    throw new UpgradeRequiredException();
                }
                if (newDesc.build().getValue(MetaDataUtil.IS_LOCAL_INDEX_TABLE_PROP_BYTES) != null && Boolean.TRUE.equals(
                        PBoolean.INSTANCE.toObject(newDesc.build().getValue(MetaDataUtil.IS_LOCAL_INDEX_TABLE_PROP_BYTES)))) {
                    newDesc.setRegionSplitPolicyClassName(IndexRegionSplitPolicy.class.getName());
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
                    checkClientServerCompatibility(SchemaUtil.getPhysicalName(SYSTEM_CATALOG_NAME_BYTES, this.getProps()).getName());
                }
                return null;
            } else {
                if (isMetaTable && !isUpgradeRequired()) {
                    checkClientServerCompatibility(SchemaUtil.getPhysicalName(SYSTEM_CATALOG_NAME_BYTES, this.getProps()).getName());
                } else {
                    for(Pair<byte[],Map<String,Object>> family: families) {
                        if ((Bytes.toString(family.getFirst())
                                .startsWith(QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX))) {
                            newDesc.setRegionSplitPolicyClassName(IndexRegionSplitPolicy.class.getName());
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
            sqlE = ServerUtil.parseServerException(e);
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
    
    private static boolean hasTxCoprocessor(TableDescriptor descriptor) {
        for (TransactionFactory.Provider provider : TransactionFactory.Provider.values()) {
            Class<? extends RegionObserver> coprocessorClass = provider.getTransactionProvider().getCoprocessor();
            if (coprocessorClass != null && descriptor.hasCoprocessor(coprocessorClass.getName())) {
                return true;
            }
        }
        return false;
    }

    private static boolean equalTxCoprocessor(TransactionFactory.Provider provider, TableDescriptor existingDesc, TableDescriptor newDesc) {
        Class<? extends RegionObserver> coprocessorClass = provider.getTransactionProvider().getCoprocessor();
        return (coprocessorClass != null && existingDesc.hasCoprocessor(coprocessorClass.getName()) && newDesc.hasCoprocessor(coprocessorClass.getName()));
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

    private static boolean isCompatible(Long serverVersion) {
        if (serverVersion == null) {
            return false;
        }
        return MetaDataUtil.areClientAndServerCompatible(serverVersion);
    }

    private void checkClientServerCompatibility(byte[] metaTable) throws SQLException,
            AccessDeniedException {
        StringBuilder buf = new StringBuilder("Newer Phoenix clients can't communicate with older "
                + "Phoenix servers. The following servers require an updated "
                + QueryConstants.DEFAULT_COPROCESS_JAR_NAME
                + " to be put in the classpath of HBase: ");
        boolean isIncompatible = false;
        int minHBaseVersion = Integer.MAX_VALUE;
        boolean isTableNamespaceMappingEnabled = false;
        long systemCatalogTimestamp = Long.MAX_VALUE;
        Table ht = null;
        try {
            List<HRegionLocation> locations = this
                    .getAllTableRegions(metaTable);
            Set<HRegionLocation> serverMap = Sets.newHashSetWithExpectedSize(locations.size());
            TreeMap<byte[], HRegionLocation> regionMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
            List<byte[]> regionKeys = Lists.newArrayListWithExpectedSize(locations.size());
            for (HRegionLocation entry : locations) {
                if (!serverMap.contains(entry)) {
                    regionKeys.add(entry.getRegion().getStartKey());
                    regionMap.put(entry.getRegion().getRegionName(), entry);
                    serverMap.add(entry);
                }
            }

            ht = this.getTable(metaTable);
            final Map<byte[], GetVersionResponse> results =
                    ht.coprocessorService(MetaDataService.class, null, null, new Batch.Call<MetaDataService,GetVersionResponse>() {
                        @Override
                        public GetVersionResponse call(MetaDataService instance) throws IOException {
                            ServerRpcController controller = new ServerRpcController();
                            BlockingRpcCallback<GetVersionResponse> rpcCallback =
                                    new BlockingRpcCallback<>();
                            GetVersionRequest.Builder builder = GetVersionRequest.newBuilder();
                            builder.setClientVersion(VersionUtil.encodeVersion(PHOENIX_MAJOR_VERSION, PHOENIX_MINOR_VERSION, PHOENIX_PATCH_NUMBER));
                            instance.getVersion(controller, builder.build(), rpcCallback);
                            if(controller.getFailedOn() != null) {
                                throw controller.getFailedOn();
                            }
                            return rpcCallback.get();
                        }
                    });
            for (Map.Entry<byte[],GetVersionResponse> result : results.entrySet()) {
                // This is the "phoenix.jar" is in-place, but server is out-of-sync with client case.
                GetVersionResponse versionResponse = result.getValue();
                long serverJarVersion = versionResponse.getVersion();
                isTableNamespaceMappingEnabled |= MetaDataUtil.decodeTableNamespaceMappingEnabled(serverJarVersion);

                if (!isCompatible(serverJarVersion)) {
                    isIncompatible = true;
                    HRegionLocation name = regionMap.get(result.getKey());
                    buf.append(name);
                    buf.append(';');
                }
                hasIndexWALCodec &= hasIndexWALCodec(serverJarVersion);
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
            }
            if (isTableNamespaceMappingEnabled != SchemaUtil.isNamespaceMappingEnabled(PTableType.TABLE,
                    getProps())) { throw new SQLExceptionInfo.Builder(
                            SQLExceptionCode.INCONSISTENT_NAMESPACE_MAPPING_PROPERTIES)
                    .setMessage(
                            "Ensure that config " + QueryServices.IS_NAMESPACE_MAPPING_ENABLED
                            + " is consistent on client and server.")
                            .build().buildException(); }
            lowestClusterHBaseVersion = minHBaseVersion;
        } catch (SQLException | AccessDeniedException e) {
            throw e;
        } catch (Throwable t) {
            // This is the case if the "phoenix.jar" is not on the classpath of HBase on the region server
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.INCOMPATIBLE_CLIENT_SERVER_JAR).setRootCause(t)
            .setMessage("Ensure that " + QueryConstants.DEFAULT_COPROCESS_JAR_NAME + " is put on the classpath of HBase in every region server: " + t.getMessage())
            .build().buildException();
        } finally {
            if (ht != null) {
                try {
                    ht.close();
                } catch (IOException e) {
                    LOGGER.warn("Could not close HTable", e);
                }
            }
        }
        if (isIncompatible) {
            buf.setLength(buf.length()-1);
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.OUTDATED_JARS).setMessage(buf.toString()).build().buildException();
        }
        if (systemCatalogTimestamp < MIN_SYSTEM_TABLE_TIMESTAMP) {
            throw new UpgradeRequiredException(systemCatalogTimestamp);
        }
    }

    /**
     * Invoke meta data coprocessor with one retry if the key was found to not be in the regions
     * (due to a table split)
     */
    private MetaDataMutationResult metaDataCoprocessorExec(byte[] tableKey,
            Batch.Call<MetaDataService, MetaDataResponse> callable) throws SQLException {
        return metaDataCoprocessorExec(tableKey, callable, PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
    }

    /**
     * Invoke meta data coprocessor with one retry if the key was found to not be in the regions
     * (due to a table split)
     */
    private MetaDataMutationResult metaDataCoprocessorExec(byte[] tableKey,
            Batch.Call<MetaDataService, MetaDataResponse> callable, byte[] tableName) throws SQLException {

        try {
            boolean retried = false;
            while (true) {
                if (retried) {
                    ((ClusterConnection) connection).relocateRegion(
                        SchemaUtil.getPhysicalName(tableName, this.getProps()), tableKey);
                }

                Table ht = this.getTable(SchemaUtil.getPhysicalName(tableName, this.getProps()).getName());
                try {
                    final Map<byte[], MetaDataResponse> results =
                            ht.coprocessorService(MetaDataService.class, tableKey, tableKey, callable);

                    assert(results.size() == 1);
                    MetaDataResponse result = results.values().iterator().next();
                    if (result.getReturnCode() == MetaDataProtos.MutationCode.TABLE_NOT_IN_REGION
                            || result.getReturnCode() == MetaDataProtos.MutationCode.FUNCTION_NOT_IN_REGION) {
                        if (retried) return MetaDataMutationResult.constructFromProto(result);
                        retried = true;
                        continue;
                    }
                    return MetaDataMutationResult.constructFromProto(result);
                } finally {
                    Closeables.closeQuietly(ht);
                }
            }
        } catch (IOException e) {
            throw ServerUtil.parseServerException(e);
        } catch (Throwable t) {
            throw new SQLException(t);
        }
    }

    // Our property values are translated using toString, so we need to "string-ify" this.
    private static final String TRUE_BYTES_AS_STRING = Bytes.toString(PDataType.TRUE_BYTES);

    private void ensureViewIndexTableCreated(byte[] physicalTableName, Map<String, Object> tableProps,
            List<Pair<byte[], Map<String, Object>>> families, byte[][] splits, long timestamp,
            boolean isNamespaceMapped) throws SQLException {
        byte[] physicalIndexName = MetaDataUtil.getViewIndexPhysicalName(physicalTableName);

        tableProps.put(MetaDataUtil.IS_VIEW_INDEX_TABLE_PROP_NAME, TRUE_BYTES_AS_STRING);
        TableDescriptor desc = ensureTableCreated(physicalIndexName, PTableType.TABLE, tableProps, families, splits,
                false, isNamespaceMapped, false);
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
            throw ServerUtil.parseServerException(e);
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
                    List<String> columnFamiles = new ArrayList<String>();
                    for(ColumnFamilyDescriptor cf : desc.getColumnFamilies()) {
                        if(cf.getNameAsString().startsWith(QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX)) {
                            columnFamiles.add(cf.getNameAsString());
                        }
                    }
                    for(String cf: columnFamiles) {
                        admin.deleteColumnFamily(TableName.valueOf(physicalTableName), Bytes.toBytes(cf));
                    }
                    clearTableRegionCache(TableName.valueOf(physicalTableName));
                    wasDeleted = true;
                }
            } catch (org.apache.hadoop.hbase.TableNotFoundException ignore) {
                // Ignore, as we may never have created a view index table
            }
        } catch (IOException e) {
            throw ServerUtil.parseServerException(e);
        }
        return wasDeleted;
    }

    @Override
    public MetaDataMutationResult createTable(final List<Mutation> tableMetaData, final byte[] physicalTableName,
            PTableType tableType, Map<String, Object> tableProps,
            final List<Pair<byte[], Map<String, Object>>> families, byte[][] splits, boolean isNamespaceMapped,
      final boolean allocateIndexId, final boolean isDoNotUpgradePropSet) throws SQLException {
        byte[][] rowKeyMetadata = new byte[3][];
        Mutation m = MetaDataUtil.getPutOnlyTableHeaderRow(tableMetaData);
        byte[] key = m.getRow();
        SchemaUtil.getVarChars(key, rowKeyMetadata);
        byte[] tenantIdBytes = rowKeyMetadata[PhoenixDatabaseMetaData.TENANT_ID_INDEX];
        byte[] schemaBytes = rowKeyMetadata[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
        byte[] tableBytes = rowKeyMetadata[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
        byte[] tableName = physicalTableName != null ? physicalTableName :
            SchemaUtil.getPhysicalHBaseTableName(schemaBytes, tableBytes, isNamespaceMapped).getBytes();
        boolean localIndexTable = false;
        for(Pair<byte[], Map<String, Object>> family: families) {
            if(Bytes.toString(family.getFirst()).startsWith(QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX)) {
                localIndexTable = true;
                break;
            }
        }
        if ((tableType == PTableType.VIEW && physicalTableName != null) || (tableType != PTableType.VIEW && (physicalTableName == null || localIndexTable))) {
            // For views this will ensure that metadata already exists
            // For tables and indexes, this will create the metadata if it doesn't already exist
            ensureTableCreated(tableName, tableType, tableProps, families, splits, true, isNamespaceMapped, isDoNotUpgradePropSet);
        }
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        if (tableType == PTableType.INDEX) { // Index on view
            // Physical index table created up front for multi tenant
            // TODO: if viewIndexId is Short.MIN_VALUE, then we don't need to attempt to create it
            if (physicalTableName != null) {
                if (!localIndexTable && !MetaDataUtil.isMultiTenant(m, kvBuilder, ptr)) {
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
                tableName, tableProps, familiesPlusDefault, MetaDataUtil.isSalted(m, kvBuilder, ptr) ? splits : null,
                MetaDataUtil.getClientTimeStamp(m), isNamespaceMapped);
        }

        byte[] tableKey = SchemaUtil.getTableKey(tenantIdBytes, schemaBytes, tableBytes);
        MetaDataMutationResult result = metaDataCoprocessorExec(tableKey,
                new Batch.Call<MetaDataService, MetaDataResponse>() {
            @Override
            public MetaDataResponse call(MetaDataService instance) throws IOException {
                ServerRpcController controller = new ServerRpcController();
                BlockingRpcCallback<MetaDataResponse> rpcCallback =
                        new BlockingRpcCallback<MetaDataResponse>();
                CreateTableRequest.Builder builder = CreateTableRequest.newBuilder();
                for (Mutation m : tableMetaData) {
                    MutationProto mp = ProtobufUtil.toProto(m);
                    builder.addTableMetadataMutations(mp.toByteString());
                }
                builder.setClientVersion(VersionUtil.encodeVersion(PHOENIX_MAJOR_VERSION, PHOENIX_MINOR_VERSION, PHOENIX_PATCH_NUMBER));
                        if (allocateIndexId) {
                            builder.setAllocateIndexId(allocateIndexId);
                        }
                CreateTableRequest build = builder.build();
                instance.createTable(controller, build, rpcCallback);
                if(controller.getFailedOn() != null) {
                    throw controller.getFailedOn();
                }
                return rpcCallback.get();
            }
        });
        return result;
    }

    @Override
    public MetaDataMutationResult getTable(final PName tenantId, final byte[] schemaBytes,
            final byte[] tableBytes, final long tableTimestamp, final long clientTimestamp,
            final boolean skipAddingIndexes, final boolean skipAddingParentColumns,
            final PTable lockedAncestorTable) throws SQLException {
        final byte[] tenantIdBytes = tenantId == null ? ByteUtil.EMPTY_BYTE_ARRAY : tenantId.getBytes();
        byte[] tableKey = SchemaUtil.getTableKey(tenantIdBytes, schemaBytes, tableBytes);
        return metaDataCoprocessorExec(tableKey,
                new Batch.Call<MetaDataService, MetaDataResponse>() {
            @Override
            public MetaDataResponse call(MetaDataService instance) throws IOException {
                ServerRpcController controller = new ServerRpcController();
                BlockingRpcCallback<MetaDataResponse> rpcCallback =
                        new BlockingRpcCallback<MetaDataResponse>();
                GetTableRequest.Builder builder = GetTableRequest.newBuilder();
                builder.setTenantId(ByteStringer.wrap(tenantIdBytes));
                builder.setSchemaName(ByteStringer.wrap(schemaBytes));
                builder.setTableName(ByteStringer.wrap(tableBytes));
                builder.setTableTimestamp(tableTimestamp);
                builder.setClientTimestamp(clientTimestamp);
                builder.setClientVersion(VersionUtil.encodeVersion(PHOENIX_MAJOR_VERSION, PHOENIX_MINOR_VERSION, PHOENIX_PATCH_NUMBER));
                builder.setSkipAddingParentColumns(skipAddingParentColumns);
                builder.setSkipAddingIndexes(skipAddingIndexes);
                if (lockedAncestorTable!=null)
                    builder.setLockedAncestorTable(PTableImpl.toProto(lockedAncestorTable));
                instance.getTable(controller, builder.build(), rpcCallback);
                if(controller.getFailedOn() != null) {
                    throw controller.getFailedOn();
                }
                return rpcCallback.get();
            }
        });
    }

    @Override
    public MetaDataMutationResult dropTable(final List<Mutation> tableMetaData, final PTableType tableType,
            final boolean cascade, final boolean skipAddingParentColumns) throws SQLException {
        byte[][] rowKeyMetadata = new byte[3][];
        SchemaUtil.getVarChars(tableMetaData.get(0).getRow(), rowKeyMetadata);
        byte[] tenantIdBytes = rowKeyMetadata[PhoenixDatabaseMetaData.TENANT_ID_INDEX];
        byte[] schemaBytes = rowKeyMetadata[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
        byte[] tableBytes = rowKeyMetadata[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
        byte[] tableKey = SchemaUtil.getTableKey(tenantIdBytes == null ? ByteUtil.EMPTY_BYTE_ARRAY : tenantIdBytes, schemaBytes, tableBytes);
        final MetaDataMutationResult result =  metaDataCoprocessorExec(tableKey,
                new Batch.Call<MetaDataService, MetaDataResponse>() {
            @Override
            public MetaDataResponse call(MetaDataService instance) throws IOException {
                ServerRpcController controller = new ServerRpcController();
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
                builder.setSkipAddingParentColumns(skipAddingParentColumns);
                instance.dropTable(controller, builder.build(), rpcCallback);
                if(controller.getFailedOn() != null) {
                    throw controller.getFailedOn();
                }
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
            flushTable(parentPhysicalTableName);
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

        final MetaDataMutationResult result =  metaDataCoprocessorExec(functionKey,
                new Batch.Call<MetaDataService, MetaDataResponse>() {
            @Override
            public MetaDataResponse call(MetaDataService instance) throws IOException {
                ServerRpcController controller = new ServerRpcController();
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
                if(controller.getFailedOn() != null) {
                    throw controller.getFailedOn();
                }
                return rpcCallback.get();
            }
        }, PhoenixDatabaseMetaData.SYSTEM_FUNCTION_NAME_BYTES);
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
            if (tableNamesToDelete != null){
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
            sqlE = ServerUtil.parseServerException(e);
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
                    timestamp, false, false, null);
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
        ensureViewIndexTableCreated(physicalTableName, tableProps, families, splits, timestamp, isNamespaceMapped);
    }

    @Override
    public MetaDataMutationResult addColumn(final List<Mutation> tableMetaData, PTable table, Map<String, List<Pair<String,Object>>> stmtProperties, Set<String> colFamiliesForPColumnsToBeAdded, List<PColumn> columns) throws SQLException {
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
            if (modifyHTable) {
                sendHBaseMetaData(tableDescriptors, pollingNeeded);
            }

            // Special case for call during drop table to ensure that the empty column family exists.
            // In this, case we only include the table header row, as until we add schemaBytes and tableBytes
            // as args to this function, we have no way of getting them in this case.
            // TODO: change to  if (tableMetaData.isEmpty()) once we pass through schemaBytes and tableBytes
            // Also, could be used to update property values on ALTER TABLE t SET prop=xxx
            if ((tableMetaData.isEmpty()) || (tableMetaData.size() == 1 && tableMetaData.get(0).isEmpty())) {
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
            result =  metaDataCoprocessorExec(tableKey,
                    new Batch.Call<MetaDataService, MetaDataResponse>() {
                @Override
                public MetaDataResponse call(MetaDataService instance) throws IOException {
                    ServerRpcController controller = new ServerRpcController();
                    BlockingRpcCallback<MetaDataResponse> rpcCallback =
                            new BlockingRpcCallback<MetaDataResponse>();
                    AddColumnRequest.Builder builder = AddColumnRequest.newBuilder();
                    for (Mutation m : tableMetaData) {
                        MutationProto mp = ProtobufUtil.toProto(m);
                        builder.addTableMetadataMutations(mp.toByteString());
                    }
                    builder.setClientVersion(VersionUtil.encodeVersion(PHOENIX_MAJOR_VERSION, PHOENIX_MINOR_VERSION, PHOENIX_PATCH_NUMBER));
                    instance.addColumn(controller, builder.build(), rpcCallback);
                    if(controller.getFailedOn() != null) {
                        throw controller.getFailedOn();
                    }
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
            setTransactional(physicalTableName, tableDescriptorBuilder, table.getType(), txValue, tableProps);
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
                setTransactional(index.getPhysicalName().getBytes(), indexDescriptorBuilder, index.getType(), txValue, indexTableProps);
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
                setTransactional(MetaDataUtil.getViewIndexPhysicalName(physicalTableName), indexDescriptorBuilder, PTableType.INDEX, txValue, indexTableProps);
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
                setTransactional(MetaDataUtil.getViewIndexPhysicalName(physicalTableName), indexDescriptorBuilder, PTableType.INDEX, txValue, indexTableProps);
                descriptorsToUpdate.add(indexDescriptorBuilder.build());
            } catch (org.apache.hadoop.hbase.TableNotFoundException ignore) {
                // Ignore, as we may never have created a local index
            }
        } catch (IOException e) {
            throw ServerUtil.parseServerException(e);
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
            indexDescriptorBuilder.addColumnFamily(indexColDescriptorBuilder.build());
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
                    indexDescriptorBuilder.addColumnFamily(indexColDescriptorBuilder.build());
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
                sqlE = ServerUtil.parseServerException(e);
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
    private void setTransactional(byte[] physicalTableName, TableDescriptorBuilder tableDescriptorBuilder, PTableType tableType, String txValue, Map<String, Object> tableProps) throws SQLException {
        if (txValue == null) {
            tableDescriptorBuilder.removeValue(Bytes.toBytes(PhoenixTransactionContext.READ_NON_TX_DATA));
        } else {
            tableDescriptorBuilder.setValue(PhoenixTransactionContext.READ_NON_TX_DATA, txValue);
        }
        this.addCoprocessors(physicalTableName, tableDescriptorBuilder, tableType, tableProps);
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
                // Set Tephra's TTL property based on HBase property if we're
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

    private HashSet<String> existingColumnFamilies(PTable table) {
        List<PColumnFamily> cfs = table.getColumnFamilies();
        HashSet<String> cfNames = new HashSet<>(cfs.size());
        for (PColumnFamily cf : table.getColumnFamilies()) {
            cfNames.add(cf.getName().getString());
        }
        return cfNames;
    }

    private static KeepDeletedCells getKeepDeletedCells(PTable table, TableDescriptor tableDesc,
            KeepDeletedCells newKeepDeletedCells) throws SQLException {
        // If we're setting KEEP_DELETED_CELLS now, then use that value. Otherwise, use the empty column family value
        return (newKeepDeletedCells != null) ?
                newKeepDeletedCells :
                tableDesc.getColumnFamily(SchemaUtil.getEmptyColumnFamily(table)).getKeepDeletedCells();
    }

    private static int getReplicationScope(PTable table, TableDescriptor tableDesc,
            Integer newReplicationScope) throws SQLException {
        // If we're setting replication scope now, then use that value. Otherwise, use the empty column family value
        return (newReplicationScope != null) ?
                newReplicationScope :
                tableDesc.getColumnFamily(SchemaUtil.getEmptyColumnFamily(table)).getScope();
    }

    private static int getTTL(PTable table, TableDescriptor tableDesc, Integer newTTL) throws SQLException {
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
        String viewIndexName = MetaDataUtil.getViewIndexPhysicalName(table.getPhysicalName().getString());
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
    public MetaDataMutationResult dropColumn(final List<Mutation> tableMetaData, PTableType tableType) throws SQLException {
        byte[][] rowKeyMetadata = new byte[3][];
        SchemaUtil.getVarChars(tableMetaData.get(0).getRow(), rowKeyMetadata);
        byte[] tenantIdBytes = rowKeyMetadata[PhoenixDatabaseMetaData.TENANT_ID_INDEX];
        byte[] schemaBytes = rowKeyMetadata[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
        byte[] tableBytes = rowKeyMetadata[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
        byte[] tableKey = SchemaUtil.getTableKey(tenantIdBytes, schemaBytes, tableBytes);
        MetaDataMutationResult result = metaDataCoprocessorExec(tableKey,
                new Batch.Call<MetaDataService, MetaDataResponse>() {
            @Override
            public MetaDataResponse call(MetaDataService instance) throws IOException {
                ServerRpcController controller = new ServerRpcController();
                BlockingRpcCallback<MetaDataResponse> rpcCallback =
                        new BlockingRpcCallback<MetaDataResponse>();
                DropColumnRequest.Builder builder = DropColumnRequest.newBuilder();
                for (Mutation m : tableMetaData) {
                    MutationProto mp = ProtobufUtil.toProto(m);
                    builder.addTableMetadataMutations(mp.toByteString());
                }
                builder.setClientVersion(VersionUtil.encodeVersion(PHOENIX_MAJOR_VERSION, PHOENIX_MINOR_VERSION, PHOENIX_PATCH_NUMBER));
                instance.dropColumn(controller, builder.build(), rpcCallback);
                if(controller.getFailedOn() != null) {
                    throw controller.getFailedOn();
                }
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
            LOGGER.warn("Table already modified at this timestamp, so assuming column already nullable: " + columnName);
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
            LOGGER.warn("Table already modified at this timestamp, so assuming add of these columns already done: " + columns);
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
        return setSystemDDLProperties(QueryConstants.CREATE_MUTEX_METADTA);
    }

    protected String getTaskDDL() {
        return setSystemDDLProperties(QueryConstants.CREATE_TASK_METADATA);
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
                            boolean isDoNotUpgradePropSet = UpgradeUtil.isNoUpgradeSet(props);
                            Properties scnProps = PropertiesUtil.deepCopy(props);
                            scnProps.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
                                    Long.toString(getSystemTableVersion()));
                            scnProps.remove(PhoenixRuntime.TENANT_ID_ATTRIB);
                            String globalUrl = JDBCUtil.removeProperty(url, PhoenixRuntime.TENANT_ID_ATTRIB);
                            try (Admin hBaseAdmin = getAdmin();
                                 PhoenixConnection metaConnection = new PhoenixConnection(ConnectionQueryServicesImpl.this, globalUrl,
                                         scnProps, newEmptyMetaData())) {
                                try {
                                    metaConnection.setRunningUpgrade(true);
                                    metaConnection.createStatement().executeUpdate(getSystemCatalogTableDDL());
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
                                        LOGGER.warn("Could not check for Phoenix SYSTEM tables, assuming they exist and are properly configured");
                                        checkClientServerCompatibility(SchemaUtil.getPhysicalName(SYSTEM_CATALOG_NAME_BYTES, getProps()).getName());
                                        success = true;
                                    } else if (inspectIfAnyExceptionInChain(e,
                                            Collections.<Class<? extends Exception>> singletonList(
                                                    NamespaceNotFoundException.class))) {
                                        // This exception is only possible if SYSTEM namespace mapping is enabled and SYSTEM namespace is missing
                                        // It implies that SYSTEM tables are not created and hence we shouldn't provide a connection
                                        AccessDeniedException ade = new AccessDeniedException("Insufficient permissions to create SYSTEM namespace and SYSTEM Tables");
                                        initializationException = ServerUtil.parseServerException(ade);
                                    } else {
                                        initializationException = e;
                                    }
                                    return null;
                                } catch (UpgradeRequiredException e) {
                                    // This will occur in 3 cases:
                                    // 1. SYSTEM.CATALOG does not exist and we don't want to allow the user to create it i.e.
                                    //    !isAutoUpgradeEnabled or isDoNotUpgradePropSet is set
                                    // 2. SYSTEM.CATALOG exists and its timestamp < MIN_SYSTEM_TABLE_TIMESTAMP
                                    // 3. SYSTEM.CATALOG exists, but client and server-side namespace mapping is enabled so
                                    //    we need to migrate SYSTEM tables to the SYSTEM namespace
                                    setUpgradeRequired();
                                }

                                if (!ConnectionQueryServicesImpl.this.upgradeRequired.get()) {
                                    createOtherSystemTables(metaConnection, hBaseAdmin);
                                    // In case namespace mapping is enabled and system table to system namespace mapping is also enabled,
                                    // create an entry for the SYSTEM namespace in the SYSCAT table, so that GRANT/REVOKE commands can work
                                    // with SYSTEM Namespace
                                    createSchemaIfNotExistsSystemNSMappingEnabled(metaConnection);
                                } else if (isAutoUpgradeEnabled && !isDoNotUpgradePropSet) {
                                    // Upgrade is required and we are allowed to automatically upgrade
                                    upgradeSystemTables(url, props);
                                } else {
                                    // We expect the user to manually run the "EXECUTE UPGRADE" command first.
                                    LOGGER.error("Upgrade is required. Must run 'EXECUTE UPGRADE' "
                                            + "before any other command");
                                }
                            }
                            scheduleRenewLeaseTasks();
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
                            try {
                                if (!success && hConnectionEstablished) {
                                    connection.close();
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

    void createSysMutexTableIfNotExists(Admin admin) throws IOException, SQLException {
        try {
            if (admin.tableExists(TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_MUTEX_NAME))
                    || admin.tableExists(TableName.valueOf(
                        PhoenixDatabaseMetaData.SYSTEM_SCHEMA_NAME,
                        PhoenixDatabaseMetaData.SYSTEM_MUTEX_TABLE_NAME))) {
                LOGGER.debug("System mutex table already appears to exist, not creating it");
                return;
            }
            final TableName mutexTableName = SchemaUtil.getPhysicalTableName(
                    PhoenixDatabaseMetaData.SYSTEM_MUTEX_NAME, props);
            TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(mutexTableName)
                    .addColumnFamily(ColumnFamilyDescriptorBuilder
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
                LOGGER.debug("Ignoring exception while creating mutex table during connection initialization: "
                        + Throwables.getStackTraceAsString(e));
            } else {
                throw e;
            }
        }
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

    private void createOtherSystemTables(PhoenixConnection metaConnection, Admin hbaseAdmin) throws SQLException, IOException {
        try {

            nSequenceSaltBuckets = ConnectionQueryServicesImpl.this.props.getInt(
                    QueryServices.SEQUENCE_SALT_BUCKETS_ATTRIB,
                    QueryServicesOptions.DEFAULT_SEQUENCE_TABLE_SALT_BUCKETS);
            metaConnection.createStatement().execute(getSystemSequenceTableDDL(nSequenceSaltBuckets));
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
        } catch (TableAlreadyExistsException e) {}
        try {
            metaConnection.createStatement().executeUpdate(getMutexDDL());
        } catch (TableAlreadyExistsException e) {}
        try {
            metaConnection.createStatement().executeUpdate(getTaskDDL());
        } catch (TableAlreadyExistsException e) {}

        // Catch the IOException to log the error message and then bubble it up for the client to retry.
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
              ConnectionQueryServicesImpl.this, metaConnection.getURL(), p,
              metaConnection.getMetaDataCache());
            try {
                List<String> tablesNeedingUpgrade = UpgradeUtil
                  .getPhysicalTablesWithDescRowKey(conn);
                if (!tablesNeedingUpgrade.isEmpty()) {
                    LOGGER.warn("The following tables require upgrade due to a bug causing the row key to be incorrect for descending columns and ascending BINARY columns (PHOENIX-2067 and PHOENIX-2120):\n"
                      + Joiner.on(' ').join(tablesNeedingUpgrade)
                      + "\nTo upgrade issue the \"bin/psql.py -u\" command.");
                }
                List<String> unsupportedTables = UpgradeUtil
                  .getPhysicalTablesWithDescVarbinaryRowKey(conn);
                if (!unsupportedTables.isEmpty()) {
                    LOGGER.warn("The following tables use an unsupported VARBINARY DESC construct and need to be changed:\n"
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
        // Add these columns one at a time, each with different timestamps so that if folks
        // have
        // run the upgrade code already for a snapshot, we'll still enter this block (and do
        // the
        // parts we haven't yet done).
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
                metaConnection = UpgradeUtil.upgradeLocalIndexes(metaConnection);
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
            metaConnection.createStatement().executeUpdate("ALTER TABLE " + 
                    PhoenixDatabaseMetaData.SYSTEM_CATALOG + " SET " + 
                    HConstants.VERSIONS + "= " + props.getInt(DEFAULT_SYSTEM_MAX_VERSIONS_ATTRIB, QueryServicesOptions.DEFAULT_SYSTEM_MAX_VERSIONS) + ",\n" +
                    ColumnFamilyDescriptorBuilder.KEEP_DELETED_CELLS + "=" + props.getBoolean(DEFAULT_SYSTEM_KEEP_DELETED_CELLS_ATTRIB, QueryServicesOptions.DEFAULT_SYSTEM_KEEP_DELETED_CELLS)
                    );
            metaConnection.createStatement().executeUpdate("ALTER TABLE " + 
                PhoenixDatabaseMetaData.SYSTEM_FUNCTION + " SET " + 
                    TableDescriptorBuilder.SPLIT_POLICY + "='" + SystemFunctionSplitPolicy.class.getName() + "',\n" +
                    HConstants.VERSIONS + "= " + props.getInt(DEFAULT_SYSTEM_MAX_VERSIONS_ATTRIB, QueryServicesOptions.DEFAULT_SYSTEM_MAX_VERSIONS) + ",\n" +
                    ColumnFamilyDescriptorBuilder.KEEP_DELETED_CELLS + "=" + props.getBoolean(DEFAULT_SYSTEM_KEEP_DELETED_CELLS_ATTRIB, QueryServicesOptions.DEFAULT_SYSTEM_KEEP_DELETED_CELLS)
                    );
            metaConnection.createStatement().executeUpdate("ALTER TABLE " + 
                    PhoenixDatabaseMetaData.SYSTEM_STATS_NAME + " SET " + 
                    TableDescriptorBuilder.SPLIT_POLICY + "='" + SystemStatsSplitPolicy.class.getName() +"'"
                    );
        }
        if (currentServerSideTableTimeStamp < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_15_0) {
            addViewIndexToParentLinks(metaConnection);
            moveChildLinks(metaConnection);
        }
        if (currentServerSideTableTimeStamp < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_5_1_0) {
            metaConnection = addColumnsIfNotExists(
                    metaConnection,
                    PhoenixDatabaseMetaData.SYSTEM_CATALOG,
                    MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_5_1_0,
                    PhoenixDatabaseMetaData.VIEW_INDEX_ID_DATA_TYPE + " "
                            + PInteger.INSTANCE.getSqlTypeName());
        }
        return metaConnection;
    }

    /**
     * There is no other locking needed here since only one connection (on the same or different JVM) will be able to
     * acquire the upgrade mutex via {@link #acquireUpgradeMutex(long, byte[])}.
     */
    @Override
    public void upgradeSystemTables(final String url, final Properties props) throws SQLException {
        PhoenixConnection metaConnection = null;
        boolean success = false;
        String snapshotName = null;
        String sysCatalogTableName = null;
        SQLException toThrow = null;
        boolean acquiredMutexLock = false;
        boolean snapshotCreated = false;
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
                    scnProps, newEmptyMetaData());
            metaConnection.setRunningUpgrade(true);
            // Always try to create SYSTEM.MUTEX table since we need it to acquire the upgrade mutex.
            // Upgrade or migration is not possible without the upgrade mutex
            try (Admin admin = getAdmin()) {
                createSysMutexTableIfNotExists(admin);
            }
            try {
                metaConnection.createStatement().executeUpdate(getSystemCatalogTableDDL());
            } catch (NewerTableAlreadyExistsException ignore) {
                // Ignore, as this will happen if the SYSTEM.CATALOG already exists at this fixed
                // timestamp. A TableAlreadyExistsException is not thrown, since the table only exists
                // *after* this fixed timestamp.
            } catch (UpgradeRequiredException e) {
                // This is thrown while trying to create SYSTEM:CATALOG to indicate that we must migrate SYSTEM tables
                // to the SYSTEM namespace and/or upgrade SYSCAT if required
                sysCatalogTableName = SchemaUtil.getPhysicalName(SYSTEM_CATALOG_NAME_BYTES, this.getProps()).getNameAsString();
                if (SchemaUtil.isNamespaceMappingEnabled(PTableType.SYSTEM, ConnectionQueryServicesImpl.this.getProps())) {
                    // Try acquiring a lock in SYSMUTEX table before migrating the tables since it involves disabling the table.
                    if (acquiredMutexLock = acquireUpgradeMutex(MetaDataProtocol.MIN_SYSTEM_TABLE_MIGRATION_TIMESTAMP)) {
                        LOGGER.debug("Acquired lock in SYSMUTEX table for migrating SYSTEM tables to SYSTEM namespace "
                          + "and/or upgrading " + sysCatalogTableName);
                    }
                    // We will not reach here if we fail to acquire the lock, since it throws UpgradeInProgressException

                    // If SYSTEM tables exist, they are migrated to HBase SYSTEM namespace
                    // If they don't exist or they're already migrated, this method will return immediately
                    ensureSystemTablesMigratedToSystemNamespace();
                    LOGGER.debug("Migrated SYSTEM tables to SYSTEM namespace");
                    metaConnection = upgradeSystemCatalogIfRequired(metaConnection, e.getSystemCatalogTimeStamp());
                }
            } catch (TableAlreadyExistsException e) {
                long currentServerSideTableTimeStamp = e.getTable().getTimeStamp();
                sysCatalogTableName = e.getTable().getPhysicalName().getString();
                if (currentServerSideTableTimeStamp < MIN_SYSTEM_TABLE_TIMESTAMP) {
                    // Try acquiring a lock in SYSMUTEX table before upgrading SYSCAT. If we cannot acquire the lock,
                    // it means some old client is either migrating SYSTEM tables or trying to upgrade the schema of
                    // SYSCAT table and hence it should not be interrupted
                    if (acquiredMutexLock = acquireUpgradeMutex(currentServerSideTableTimeStamp)) {
                        LOGGER.debug("Acquired lock in SYSMUTEX table for upgrading " + sysCatalogTableName);
                        snapshotName = getSysCatalogSnapshotName(currentServerSideTableTimeStamp);
                        createSnapshot(snapshotName, sysCatalogTableName);
                        snapshotCreated = true;
                        LOGGER.debug("Created snapshot for SYSCAT");
                    }
                    // We will not reach here if we fail to acquire the lock, since it throws UpgradeInProgressException
                }
                metaConnection = upgradeSystemCatalogIfRequired(metaConnection, currentServerSideTableTimeStamp);
                // Synchronize necessary properties amongst all column families of a base table and its indexes
                // See PHOENIX-3955
                if (currentServerSideTableTimeStamp < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_15_0) {
                    syncTableAndIndexProperties(metaConnection, getAdmin());
                    //Combine view index id sequences for the same physical view index table
                    //to avoid collisions. See PHOENIX-5132 and PHOENIX-5138
                    UpgradeUtil.mergeViewIndexIdSequences(this, metaConnection);
                }
            }

            int nSaltBuckets = ConnectionQueryServicesImpl.this.props.getInt(
                    QueryServices.SEQUENCE_SALT_BUCKETS_ATTRIB,
                    QueryServicesOptions.DEFAULT_SEQUENCE_TABLE_SALT_BUCKETS);
            try {
                String createSequenceTable = getSystemSequenceTableDDL(nSaltBuckets);
                metaConnection.createStatement().executeUpdate(createSequenceTable);
                nSequenceSaltBuckets = nSaltBuckets;
            } catch (NewerTableAlreadyExistsException e) {
                // Ignore, as this will happen if the SYSTEM.SEQUENCE already exists at this fixed
                // timestamp.
                // A TableAlreadyExistsException is not thrown, since the table only exists *after* this
                // fixed timestamp.
                nSequenceSaltBuckets = getSaltBuckets(e);
            } catch (TableAlreadyExistsException e) {
                // This will occur if we have an older SYSTEM.SEQUENCE and we need to update it to
                // include
                // any new columns we've added.
                long currentServerSideTableTimeStamp = e.getTable().getTimeStamp();
                if (currentServerSideTableTimeStamp < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_1_0) {
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
                if (currentServerSideTableTimeStamp < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_2_1) {
                    if (UpgradeUtil.upgradeSequenceTable(metaConnection, nSaltBuckets, e.getTable())) {
                        metaConnection.removeTable(null,
                                PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_SCHEMA,
                                PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_TABLE,
                                MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP);
                        clearTableFromCache(ByteUtil.EMPTY_BYTE_ARRAY,
                                PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_SCHEMA_BYTES,
                                PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_TABLE_BYTES,
                                MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP);
                        clearTableRegionCache(TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME_BYTES));
                    }
                    nSequenceSaltBuckets = nSaltBuckets;
                } else {
                    nSequenceSaltBuckets = getSaltBuckets(e);
                }
            }
            try {
                metaConnection.createStatement().executeUpdate(
                        QueryConstants.CREATE_STATS_TABLE_METADATA);
            } catch (NewerTableAlreadyExistsException ignore) {

            } catch (TableAlreadyExistsException e) {
                long currentServerSideTableTimeStamp = e.getTable().getTimeStamp();
                if (currentServerSideTableTimeStamp < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_3_0) {
                    metaConnection = addColumnsIfNotExists(
                            metaConnection,
                            SYSTEM_STATS_NAME,
                            MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP,
                            PhoenixDatabaseMetaData.GUIDE_POSTS_ROW_COUNT + " "
                                    + PLong.INSTANCE.getSqlTypeName());
                }
                if (currentServerSideTableTimeStamp < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_9_0) {
                    // The COLUMN_FAMILY column should be nullable as we create a row in it without
                    // any column family to mark when guideposts were last collected.
                    metaConnection = removeNotNullConstraint(metaConnection,
                            PhoenixDatabaseMetaData.SYSTEM_SCHEMA_NAME,
                            PhoenixDatabaseMetaData.SYSTEM_STATS_TABLE,
                            MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_9_0,
                            PhoenixDatabaseMetaData.COLUMN_FAMILY);
                    ConnectionQueryServicesImpl.this.removeTable(null,
                            PhoenixDatabaseMetaData.SYSTEM_STATS_NAME, null,
                            MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_9_0);
                    clearCache();
                }
            }

            try {
                metaConnection.createStatement().executeUpdate(getTaskDDL());
            } catch (NewerTableAlreadyExistsException e) {

            } catch (TableAlreadyExistsException e) {
                long currentServerSideTableTimeStamp = e.getTable().getTimeStamp();
                if (currentServerSideTableTimeStamp <= MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_15_0) {
                    String
                            columnsToAdd =
                            PhoenixDatabaseMetaData.TASK_STATUS + " " + PVarchar.INSTANCE.getSqlTypeName() + ", "
                                    + PhoenixDatabaseMetaData.TASK_END_TS + " " + PTimestamp.INSTANCE.getSqlTypeName() + ", "
                                    + PhoenixDatabaseMetaData.TASK_PRIORITY + " " + PUnsignedTinyint.INSTANCE.getSqlTypeName() + ", "
                                    + PhoenixDatabaseMetaData.TASK_DATA + " " + PVarchar.INSTANCE.getSqlTypeName();
                    String taskTableFullName = SchemaUtil.getTableName(SYSTEM_CATALOG_SCHEMA, SYSTEM_TASK_TABLE);
                    metaConnection =
                            addColumnsIfNotExists(metaConnection, taskTableFullName,
                                    MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP, columnsToAdd);
                    metaConnection.createStatement().executeUpdate(
                            "ALTER TABLE " + taskTableFullName + " SET " + TTL + "=" + TASK_TABLE_TTL);
                    clearCache();
                }
            }

            try {
                metaConnection.createStatement().executeUpdate(getFunctionTableDDL());
            } catch (NewerTableAlreadyExistsException e) {} catch (TableAlreadyExistsException e) {}
            try {
                metaConnection.createStatement().executeUpdate(getLogTableDDL());
            } catch (NewerTableAlreadyExistsException e) {} catch (TableAlreadyExistsException e) {}
            try {
                metaConnection.createStatement().executeUpdate(getChildLinkDDL());
            } catch (NewerTableAlreadyExistsException e) {} catch (TableAlreadyExistsException e) {}
            try {
                metaConnection.createStatement().executeUpdate(getMutexDDL());
            } catch (NewerTableAlreadyExistsException e) {} catch (TableAlreadyExistsException e) {}

            // In case namespace mapping is enabled and system table to system namespace mapping is also enabled,
            // create an entry for the SYSTEM namespace in the SYSCAT table, so that GRANT/REVOKE commands can work
            // with SYSTEM Namespace
            createSchemaIfNotExistsSystemNSMappingEnabled(metaConnection);

            ConnectionQueryServicesImpl.this.upgradeRequired.set(false);
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
                try {
                    if (snapshotCreated) {
                        restoreFromSnapshot(sysCatalogTableName, snapshotName, success);
                    }
                } catch (SQLException e) {
                    if (toThrow != null) {
                        toThrow.setNextException(e);
                    } else {
                        toThrow = e;
                    }
                } finally {
                    if (acquiredMutexLock) {
                        try {
                            releaseUpgradeMutex();
                        } catch (IOException e) {
                            LOGGER.warn("Release of upgrade mutex failed ", e);
                        }
                    }
                }
                if (toThrow != null) { throw toThrow; }
            }
        }
    }

    // Special method for adding the column qualifier column for 4.10. 
    private PhoenixConnection addColumnQualifierColumn(PhoenixConnection oldMetaConnection, Long timestamp) throws SQLException {
        Properties props = PropertiesUtil.deepCopy(oldMetaConnection.getClientInfo());
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(timestamp));
        // Cannot go through DriverManager or you end up in an infinite loop because it'll call init again
        PhoenixConnection metaConnection = new PhoenixConnection(oldMetaConnection, this, props);
        PTable sysCatalogPTable = metaConnection.getTable(new PTableKey(null, PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME));
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
        List<Mutation> tableMetadata = new ArrayList<>();
        tableMetadata.addAll(metaConnection.getMutationState().toMutations(metaConnection.getSCN()).next().getSecond());
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
        metaConnection.getQueryServices().addColumn(tableMetadata, sysCatalogPTable, Collections.<String,List<Pair<String,Object>>>emptyMap(), Collections.<String>emptySet(), Lists.newArrayList(column));
        metaConnection.removeTable(null, SYSTEM_CATALOG_NAME, null, timestamp);
        ConnectionQueryServicesImpl.this.removeTable(null,
                SYSTEM_CATALOG_NAME, null,
                timestamp);
        clearCache();
        return metaConnection;
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

    private void restoreFromSnapshot(String tableName, String snapshotName,
            boolean success) throws SQLException {
        boolean snapshotRestored = false;
        boolean tableDisabled = false;
        if (!success && snapshotName != null) {
            SQLException sqlE = null;
            Admin admin = null;
            try {
                LOGGER.warn("Starting restore of " + tableName + " using snapshot "
                        + snapshotName + " because upgrade failed");
                admin = getAdmin();
                admin.disableTable(TableName.valueOf(tableName));
                tableDisabled = true;
                admin.restoreSnapshot(snapshotName);
                snapshotRestored = true;
                LOGGER.warn("Successfully restored " + tableName + " using snapshot "
                        + snapshotName);
            } catch (Exception e) {
                sqlE = new SQLException(e);
            } finally {
                if (admin != null && tableDisabled) {
                    try {
                        admin.enableTable(TableName.valueOf(tableName));
                        if (snapshotRestored) {
                            LOGGER.warn("Successfully restored and enabled " + tableName + " using snapshot "
                                    + snapshotName);
                        } else {
                            LOGGER.warn("Successfully enabled " + tableName + " after restoring using snapshot "
                                    + snapshotName + " failed. ");
                        }
                    } catch (Exception e1) {
                        SQLException enableTableEx = new SQLException(e1);
                        if (sqlE == null) {
                            sqlE = enableTableEx;
                        } else {
                            sqlE.setNextException(enableTableEx);
                        }
                        LOGGER.error("Failure in enabling "
                                + tableName
                                + (snapshotRestored ? " after successfully restoring using snapshot"
                                        + snapshotName
                                        : " after restoring using snapshot "
                                                + snapshotName + " failed. "));
                    } finally {
                        try {
                            admin.close();
                        } catch (Exception e2) {
                            SQLException adminCloseEx = new SQLException(e2);
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
            if (tableNames.size() > 8) {
                LOGGER.warn("Expected 8 system tables but found " + tableNames.size() + ":" + tableNames);
            }

            byte[] mappedSystemTable = SchemaUtil
                    .getPhysicalName(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES, this.getProps()).getName();
            metatable = getTable(mappedSystemTable);
            if (tableNames.contains(PhoenixDatabaseMetaData.SYSTEM_CATALOG_HBASE_TABLE_NAME)) {
                if (!admin.tableExists(TableName.valueOf(mappedSystemTable))) {
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
     * @throws IOException
     * @throws SQLException
     */
    @VisibleForTesting
    public boolean acquireUpgradeMutex(long currentServerSideTableTimestamp)
            throws IOException,
            SQLException {
        Preconditions.checkArgument(currentServerSideTableTimestamp < MIN_SYSTEM_TABLE_TIMESTAMP);
        byte[] sysMutexPhysicalTableNameBytes = getSysMutexPhysicalTableNameBytes();
        if(sysMutexPhysicalTableNameBytes == null) {
            throw new UpgradeInProgressException(getVersion(currentServerSideTableTimestamp),
                    getVersion(MIN_SYSTEM_TABLE_TIMESTAMP));
        }
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
            byte[] rowKey =
                    columnName != null
                            ? SchemaUtil.getColumnKey(tenantId, schemaName, tableName, columnName,
                                familyName)
                            : SchemaUtil.getTableKey(tenantId, schemaName, tableName);
            // at this point the system mutex table should have been created or
            // an exception thrown
            byte[] sysMutexPhysicalTableNameBytes = getSysMutexPhysicalTableNameBytes();
            try (Table sysMutexTable = getTable(sysMutexPhysicalTableNameBytes)) {
                byte[] family = PhoenixDatabaseMetaData.SYSTEM_MUTEX_FAMILY_NAME_BYTES;
                byte[] qualifier = PhoenixDatabaseMetaData.SYSTEM_MUTEX_COLUMN_NAME_BYTES;
                byte[] value = MUTEX_LOCKED;
                Put put = new Put(rowKey);
                put.addColumn(family, qualifier, value);
                boolean checkAndPut =
                        sysMutexTable.checkAndPut(rowKey, family, qualifier, null, put);
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
            throw ServerUtil.parseServerException(e);
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
            byte[] rowKey =
                    columnName != null
                            ? SchemaUtil.getColumnKey(tenantId, schemaName, tableName, columnName,
                                familyName)
                            : SchemaUtil.getTableKey(tenantId, schemaName, tableName);
            // at this point the system mutex table should have been created or
            // an exception thrown
            byte[] sysMutexPhysicalTableNameBytes = getSysMutexPhysicalTableNameBytes();
            try (Table sysMutexTable = getTable(sysMutexPhysicalTableNameBytes)) {
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
            throw ServerUtil.parseServerException(e);
        }
    }

    private byte[] getSysMutexPhysicalTableNameBytes() throws IOException, SQLException {
        byte[] sysMutexPhysicalTableNameBytes = null;
        try(Admin admin = getAdmin()) {
            if(admin.tableExists(PhoenixDatabaseMetaData.SYSTEM_MUTEX_HBASE_TABLE_NAME)) {
                sysMutexPhysicalTableNameBytes = PhoenixDatabaseMetaData.SYSTEM_MUTEX_NAME_BYTES;
            } else if (admin.tableExists(TableName.valueOf(
                    SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_MUTEX_NAME, props).getName()))) {
                sysMutexPhysicalTableNameBytes = SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_MUTEX_NAME, props).getName();
            }
        }
        return sysMutexPhysicalTableNameBytes;
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
     * @param metaConnection connection over which to run the upgrade
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
                    + PhoenixDatabaseMetaData.SYSTEM_SCHEMA_NAME + "'");
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
        try (Table htable =
                this.getTable(
                    SchemaUtil.getPhysicalName(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES,
                        this.getProps()).getName())) {
            final Map<byte[], Long> results =
                    htable.coprocessorService(MetaDataService.class, HConstants.EMPTY_START_ROW,
                        HConstants.EMPTY_END_ROW, new Batch.Call<MetaDataService, Long>() {
                            @Override
                            public Long call(MetaDataService instance) throws IOException {
                                ServerRpcController controller = new ServerRpcController();
                                BlockingRpcCallback<ClearCacheResponse> rpcCallback =
                                        new BlockingRpcCallback<ClearCacheResponse>();
                                ClearCacheRequest.Builder builder = ClearCacheRequest.newBuilder();
                                builder.setClientVersion(
                                    VersionUtil.encodeVersion(PHOENIX_MAJOR_VERSION,
                                        PHOENIX_MINOR_VERSION, PHOENIX_PATCH_NUMBER));
                                instance.clearCache(controller, builder.build(), rpcCallback);
                                if (controller.getFailedOn() != null) {
                                    throw controller.getFailedOn();
                                }
                                return rpcCallback.get().getUnfreedBytes();
                            }
                        });
            long unfreedBytes = 0;
            for (Map.Entry<byte[], Long> result : results.entrySet()) {
                if (result.getValue() != null) {
                    unfreedBytes += result.getValue();
                }
            }
            return unfreedBytes;
        } catch (IOException e) {
            throw ServerUtil.parseServerException(e);
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
        return metaDataCoprocessorExec(tableKey,
                new Batch.Call<MetaDataService, MetaDataResponse>() {
            @Override
            public MetaDataResponse call(MetaDataService instance) throws IOException {
                ServerRpcController controller = new ServerRpcController();
                BlockingRpcCallback<MetaDataResponse> rpcCallback =
                        new BlockingRpcCallback<MetaDataResponse>();
                UpdateIndexStateRequest.Builder builder = UpdateIndexStateRequest.newBuilder();
                for (Mutation m : tableMetaData) {
                    MutationProto mp = ProtobufUtil.toProto(m);
                    builder.addTableMetadataMutations(mp.toByteString());
                }
                builder.setClientVersion(VersionUtil.encodeVersion(PHOENIX_MAJOR_VERSION, PHOENIX_MINOR_VERSION, PHOENIX_PATCH_NUMBER));
                instance.updateIndexState(controller, builder.build(), rpcCallback);
                if(controller.getFailedOn() != null) {
                    throw controller.getFailedOn();
                }
                return rpcCallback.get();
            }
        });
    }

    @Override
    public MetaDataMutationResult updateIndexState(final List<Mutation> tableMetaData, String parentTableName, Map<String, List<Pair<String,Object>>> stmtProperties,  PTable table) throws SQLException {
        if(stmtProperties == null) {
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
                throw ServerUtil.parseServerException(e);
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
                throw ServerUtil.parseServerException(e);
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
     * @param sequenceKeys sorted list of sequence kyes
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
                sqlE = ServerUtil.parseServerException(e);
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
                        sqlE = ServerUtil.parseServerException(e);
                    } else {
                        sqlE.setNextException(ServerUtil.parseServerException(e));
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
                    long numToAllocate = Bytes.toLong(incrementBatch.get(i).getAttribute(SequenceRegionObserver.NUM_TO_ALLOCATE));
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

    @Override
    public void clearTableFromCache(final byte[] tenantId, final byte[] schemaName, final byte[] tableName,
            final long clientTS) throws SQLException {
        // clear the meta data cache for the table here
        try {
            SQLException sqlE = null;
            Table htable = this.getTable(SchemaUtil
                    .getPhysicalName(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES, this.getProps()).getName());

            try {
                htable.coprocessorService(MetaDataService.class, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW,
                        new Batch.Call<MetaDataService, ClearTableFromCacheResponse>() {
                    @Override
                    public ClearTableFromCacheResponse call(MetaDataService instance) throws IOException {
                        ServerRpcController controller = new ServerRpcController();
                        BlockingRpcCallback<ClearTableFromCacheResponse> rpcCallback = new BlockingRpcCallback<ClearTableFromCacheResponse>();
                        ClearTableFromCacheRequest.Builder builder = ClearTableFromCacheRequest.newBuilder();
                        builder.setTenantId(ByteStringer.wrap(tenantId));
                        builder.setTableName(ByteStringer.wrap(tableName));
                        builder.setSchemaName(ByteStringer.wrap(schemaName));
                        builder.setClientTimestamp(clientTS);
                        builder.setClientVersion(VersionUtil.encodeVersion(PHOENIX_MAJOR_VERSION, PHOENIX_MINOR_VERSION, PHOENIX_PATCH_NUMBER));
                        instance.clearTableFromCache(controller, builder.build(), rpcCallback);
                        if (controller.getFailedOn() != null) { throw controller.getFailedOn(); }
                        return rpcCallback.get();
                    }
                });
            } catch (IOException e) {
                throw ServerUtil.parseServerException(e);
            } catch (Throwable e) {
                sqlE = new SQLException(e);
            } finally {
                try {
                    htable.close();
                } catch (IOException e) {
                    if (sqlE == null) {
                        sqlE = ServerUtil.parseServerException(e);
                    } else {
                        sqlE.setNextException(ServerUtil.parseServerException(e));
                    }
                } finally {
                    if (sqlE != null) { throw sqlE; }
                }
            }
        } catch (Exception e) {
            throw new SQLException(ServerUtil.parseServerException(e));
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
                sqlE = ServerUtil.parseServerException(e);
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
                        sqlE = ServerUtil.parseServerException(e);
                    } else {
                        sqlE.setNextException(ServerUtil.parseServerException(e));
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
            sqlE = ServerUtil.parseServerException(e);
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
                    sqlE = ServerUtil.parseServerException(e);
                } else {
                    sqlE.setNextException(ServerUtil.parseServerException(e));
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
                if (shouldThrottleNumConnections && connectionCount + 1 > maxConnectionsAllowed){
                    GLOBAL_PHOENIX_CONNECTIONS_THROTTLED_COUNTER.increment();
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.NEW_CONNECTION_THROTTLED).
                        build().buildException();
                }
                connectionCount++;
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
                if (--connectionCount <= 0) {
                    if (!this.sequenceMap.isEmpty()) {
                        formerSequenceMap = this.sequenceMap;
                        this.sequenceMap = Maps.newConcurrentMap();
                    }
                }
                if (connectionCount < 0) {
                    connectionCount = 0;
                }
            }
            // Since we're using the former sequenceMap, we can do this outside
            // the lock.
            if (formerSequenceMap != null) {
                // When there are no more connections, attempt to return any sequences
                returnAllSequences(formerSequenceMap);
            }
        } else if (shouldThrottleNumConnections){ //still need to decrement connection count
            synchronized (connectionCountLock) {
                if (connectionCount > 0) {
                    --connectionCount;
                }
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

    private void checkClosed() {
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
            throw ServerUtil.parseServerException(e);
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
        return metaDataCoprocessorExec(tenantIdBytes,
                new Batch.Call<MetaDataService, MetaDataResponse>() {
            @Override
            public MetaDataResponse call(MetaDataService instance) throws IOException {
                ServerRpcController controller = new ServerRpcController();
                BlockingRpcCallback<MetaDataResponse> rpcCallback =
                        new BlockingRpcCallback<MetaDataResponse>();
                GetFunctionsRequest.Builder builder = GetFunctionsRequest.newBuilder();
                builder.setTenantId(ByteStringer.wrap(tenantIdBytes));
                for(Pair<byte[], Long> function: functions) {
                    builder.addFunctionNames(ByteStringer.wrap(function.getFirst()));
                    builder.addFunctionTimestamps(function.getSecond().longValue());
                }
                builder.setClientTimestamp(clientTimestamp);
                builder.setClientVersion(VersionUtil.encodeVersion(PHOENIX_MAJOR_VERSION, PHOENIX_MINOR_VERSION, PHOENIX_PATCH_NUMBER));
                instance.getFunctions(controller, builder.build(), rpcCallback);
                if(controller.getFailedOn() != null) {
                    throw controller.getFailedOn();
                }
                return rpcCallback.get();
            }
        }, PhoenixDatabaseMetaData.SYSTEM_FUNCTION_NAME_BYTES);

    }

    @Override
    public MetaDataMutationResult getSchema(final String schemaName, final long clientTimestamp) throws SQLException {
        return metaDataCoprocessorExec(SchemaUtil.getSchemaKey(schemaName),
                new Batch.Call<MetaDataService, MetaDataResponse>() {
            @Override
            public MetaDataResponse call(MetaDataService instance) throws IOException {
                ServerRpcController controller = new ServerRpcController();
                BlockingRpcCallback<MetaDataResponse> rpcCallback = new BlockingRpcCallback<MetaDataResponse>();
                GetSchemaRequest.Builder builder = GetSchemaRequest.newBuilder();
                builder.setSchemaName(schemaName);
                builder.setClientTimestamp(clientTimestamp);
                builder.setClientVersion(VersionUtil.encodeVersion(PHOENIX_MAJOR_VERSION, PHOENIX_MINOR_VERSION,
                        PHOENIX_PATCH_NUMBER));
                instance.getSchema(controller, builder.build(), rpcCallback);
                if (controller.getFailedOn() != null) { throw controller.getFailedOn(); }
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
        MetaDataMutationResult result = metaDataCoprocessorExec(functionKey,
                new Batch.Call<MetaDataService, MetaDataResponse>() {
            @Override
            public MetaDataResponse call(MetaDataService instance) throws IOException {
                ServerRpcController controller = new ServerRpcController();
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
                if(controller.getFailedOn() != null) {
                    throw controller.getFailedOn();
                }
                return rpcCallback.get();
            }
        }, PhoenixDatabaseMetaData.SYSTEM_FUNCTION_NAME_BYTES);
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
                        long start = System.currentTimeMillis();
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
                                    + (System.currentTimeMillis() - start) + " ms ");
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
        boolean reload =false;
        while (true) {
            try {
                return connection.getRegionLocator(TableName.valueOf(tableName)).getRegionLocation(row, reload);
            } catch (org.apache.hadoop.hbase.TableNotFoundException e) {
                String fullName = Bytes.toString(tableName);
                throw new TableNotFoundException(SchemaUtil.getSchemaNameFromFullName(fullName), SchemaUtil.getTableNameFromFullName(fullName));
            } catch (IOException e) {
                if (retryCount++ < maxRetryCount) { // One retry, in case split occurs while navigating
                    reload = true;
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
        MetaDataMutationResult result = metaDataCoprocessorExec(key,
                new Batch.Call<MetaDataService, MetaDataResponse>() {
            @Override
            public MetaDataResponse call(MetaDataService instance) throws IOException {
                ServerRpcController controller = new ServerRpcController();
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
                if (controller.getFailedOn() != null) { throw controller.getFailedOn(); }
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
        final MetaDataMutationResult result = metaDataCoprocessorExec(SchemaUtil.getSchemaKey(schemaName),
                new Batch.Call<MetaDataService, MetaDataResponse>() {
            @Override
            public MetaDataResponse call(MetaDataService instance) throws IOException {
                ServerRpcController controller = new ServerRpcController();
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
                if (controller.getFailedOn() != null) { throw controller.getFailedOn(); }
                return rpcCallback.get();
            }
        });

        final MutationCode code = result.getMutationCode();
        switch (code) {
        case SCHEMA_ALREADY_EXISTS:
            ReadOnlyProps props = this.getProps();
            boolean dropMetadata = props.getBoolean(DROP_METADATA_ATTRIB, DEFAULT_DROP_METADATA);
            if (dropMetadata) {
                ensureNamespaceDropped(schemaName, result.getMutationTime());
            }
            break;
        default:
            break;
        }
        return result;
    }

    private void ensureNamespaceDropped(String schemaName, long mutationTime) throws SQLException {
        SQLException sqlE = null;
        try (Admin admin = getAdmin()) {
            final String quorum = ZKConfig.getZKQuorumServersString(config);
            final String znode = this.props.get(HConstants.ZOOKEEPER_ZNODE_PARENT);
            LOGGER.debug("Found quorum: " + quorum + ":" + znode);
            boolean nameSpaceExists = true;
            try {
                admin.getNamespaceDescriptor(schemaName);
            } catch (org.apache.hadoop.hbase.NamespaceNotFoundException e) {
                nameSpaceExists = false;
            }
            if (nameSpaceExists) {
                admin.deleteNamespace(schemaName);
            }
        } catch (IOException e) {
            sqlE = ServerUtil.parseServerException(e);
        } finally {
            if (sqlE != null) { throw sqlE; }
        }
    }

    /**
     * Manually adds {@link GuidePostsInfo} for a table to the client-side cache. Not a
     * {@link ConnectionQueryServices} method. Exposed for testing purposes.
     *
     * @param tableName Table name
     * @param stats Stats instance
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
