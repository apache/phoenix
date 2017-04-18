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
import static org.apache.hadoop.hbase.HColumnDescriptor.TTL;
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
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;
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
import static org.apache.phoenix.util.UpgradeUtil.getSysCatalogSnapshotName;
import static org.apache.phoenix.util.UpgradeUtil.upgradeTo4_5_0;

import java.io.IOException;
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

import javax.annotation.concurrent.GuardedBy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.PhoenixRpcSchedulerFactory;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.regionserver.IndexHalfStoreFileReaderGenerator;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.phoenix.compile.MutationPlan;
import org.apache.phoenix.coprocessor.GroupedAggregateRegionObserver;
import org.apache.phoenix.coprocessor.MetaDataEndpointImpl;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MutationCode;
import org.apache.phoenix.coprocessor.MetaDataRegionObserver;
import org.apache.phoenix.coprocessor.PhoenixTransactionalProcessor;
import org.apache.phoenix.coprocessor.ScanRegionObserver;
import org.apache.phoenix.coprocessor.SequenceRegionObserver;
import org.apache.phoenix.coprocessor.ServerCachingEndpointImpl;
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
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.hbase.index.IndexRegionSplitPolicy;
import org.apache.phoenix.hbase.index.Indexer;
import org.apache.phoenix.hbase.index.covered.NonTxIndexBuilder;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.hbase.index.util.VersionUtil;
import org.apache.phoenix.index.PhoenixIndexBuilder;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.index.PhoenixTransactionalIndexer;
import org.apache.phoenix.iterate.TableResultIterator;
import org.apache.phoenix.iterate.TableResultIterator.RenewLeaseStatus;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver.ConnectionInfo;
import org.apache.phoenix.parse.PFunction;
import org.apache.phoenix.parse.PSchema;
import org.apache.phoenix.protobuf.ProtobufUtil;
import org.apache.phoenix.schema.ColumnAlreadyExistsException;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.schema.EmptySequenceCacheException;
import org.apache.phoenix.schema.FunctionNotFoundException;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.MetaDataSplitPolicy;
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
import org.apache.phoenix.schema.PTableKey;
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
import org.apache.phoenix.schema.types.PTinyint;
import org.apache.phoenix.schema.types.PUnsignedTinyint;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.Closeables;
import org.apache.phoenix.util.ConfigUtil;
import org.apache.phoenix.util.JDBCUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixContextExecutor;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PhoenixStopWatch;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ServerUtil;
import org.apache.phoenix.util.UpgradeUtil;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.TxConstants;
import org.apache.tephra.distributed.PooledClientProvider;
import org.apache.tephra.distributed.TransactionServiceClient;
import org.apache.tephra.zookeeper.TephraZKClientService;
import org.apache.twill.discovery.ZKDiscoveryService;
import org.apache.twill.zookeeper.RetryStrategies;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClientServices;
import org.apache.twill.zookeeper.ZKClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class ConnectionQueryServicesImpl extends DelegateQueryServices implements ConnectionQueryServices {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionQueryServicesImpl.class);
    private static final int INITIAL_CHILD_SERVICES_CAPACITY = 100;
    private static final int DEFAULT_OUT_OF_ORDER_MUTATIONS_WAIT_TIME_MS = 1000;
    private static final int TTL_FOR_MUTEX = 15 * 60; // 15min 
    protected final Configuration config;
    private final ConnectionInfo connectionInfo;
    // Copy of config.getProps(), but read-only to prevent synchronization that we
    // don't need.
    private final ReadOnlyProps props;
    private final String userName;
    private final ConcurrentHashMap<ImmutableBytesWritable,ConnectionQueryServices> childServices;
    private final GuidePostsCache tableStatsCache;

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

    private HConnection connection;
    private ZKClientService txZKClientService;
    private TransactionServiceClient txServiceClient;
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
    private final boolean renewLeaseEnabled;
    private final boolean isAutoUpgradeEnabled;
    private final AtomicBoolean upgradeRequired = new AtomicBoolean(false);
    private final int maxConnectionsAllowed;
    private final boolean shouldThrottleNumConnections;
    public static final byte[] UPGRADE_MUTEX = "UPGRADE_MUTEX".getBytes();
    public static final byte[] UPGRADE_MUTEX_LOCKED = "UPGRADE_MUTEX_LOCKED".getBytes();
    public static final byte[] UPGRADE_MUTEX_UNLOCKED = "UPGRADE_MUTEX_UNLOCKED".getBytes();

    private static interface FeatureSupported {
        boolean isSupported(ConnectionQueryServices services);
    }

    private final Map<Feature, FeatureSupported> featureMap = ImmutableMap.<Feature, FeatureSupported>of(
            Feature.LOCAL_INDEX, new FeatureSupported() {
                @Override
                public boolean isSupported(ConnectionQueryServices services) {
                    int hbaseVersion = services.getLowestClusterHBaseVersion();
                    return hbaseVersion < PhoenixDatabaseMetaData.MIN_LOCAL_SI_VERSION_DISALLOW || hbaseVersion > PhoenixDatabaseMetaData.MAX_LOCAL_SI_VERSION_DISALLOW;
                }
            },
            Feature.RENEW_LEASE, new FeatureSupported() {
                @Override
                public boolean isSupported(ConnectionQueryServices services) {
                    int hbaseVersion = services.getLowestClusterHBaseVersion();
                    return hbaseVersion >= PhoenixDatabaseMetaData.MIN_RENEW_LEASE_VERSION;
                }
            });
    
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
        this.tableStatsCache = new GuidePostsCache(this, config);
        this.isAutoUpgradeEnabled = config.getBoolean(AUTO_UPGRADE_ENABLED, QueryServicesOptions.DEFAULT_AUTO_UPGRADE_ENABLED);
        this.maxConnectionsAllowed = config.getInt(QueryServices.CLIENT_CONNECTION_MAX_ALLOWED_CONNECTIONS,
            QueryServicesOptions.DEFAULT_CLIENT_CONNECTION_MAX_ALLOWED_CONNECTIONS);
        this.shouldThrottleNumConnections = (maxConnectionsAllowed > 0);

    }

    @Override
    public TransactionSystemClient getTransactionSystemClient() {
        return txServiceClient;
    }

    private void initTxServiceClient() {
        String zkQuorumServersString = this.getProps().get(TxConstants.Service.CFG_DATA_TX_ZOOKEEPER_QUORUM);
        if (zkQuorumServersString==null) {
            zkQuorumServersString = connectionInfo.getZookeeperQuorum()+":"+connectionInfo.getPort();
        }

        int timeOut = props.getInt(HConstants.ZK_SESSION_TIMEOUT, HConstants.DEFAULT_ZK_SESSION_TIMEOUT);
        // Create instance of the tephra zookeeper client
        txZKClientService = ZKClientServices.delegate(
            ZKClients.reWatchOnExpire(
                ZKClients.retryOnFailure(
                     new TephraZKClientService(zkQuorumServersString, timeOut, null,
                             ArrayListMultimap.<String, byte[]>create()), 
                         RetryStrategies.exponentialDelay(500, 2000, TimeUnit.MILLISECONDS))
                     )
                );
        txZKClientService.startAndWait();
        ZKDiscoveryService zkDiscoveryService = new ZKDiscoveryService(txZKClientService);
        PooledClientProvider pooledClientProvider = new PooledClientProvider(
                config, zkDiscoveryService);
        this.txServiceClient = new TransactionServiceClient(config,pooledClientProvider);
    }

    private void openConnection() throws SQLException {
        try {
            boolean transactionsEnabled = props.getBoolean(
                    QueryServices.TRANSACTIONS_ENABLED,
                    QueryServicesOptions.DEFAULT_TRANSACTIONS_ENABLED);
            this.connection = HBaseFactoryProvider.getHConnectionFactory().createConnection(this.config);
            GLOBAL_HCONNECTIONS_COUNTER.increment();
            logger.info("HConnnection established. Details: " + connection + " " +  Throwables.getStackTraceAsString(new Exception()));
            // only initialize the tx service client if needed and if we succeeded in getting a connection
            // to HBase
            if (transactionsEnabled) {
                initTxServiceClient();
            }
        } catch (IOException e) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_ESTABLISH_CONNECTION)
            .setRootCause(e).build().buildException();
        }
        if (this.connection.isClosed()) { // TODO: why the heck doesn't this throw above?
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_ESTABLISH_CONNECTION).build().buildException();
        }
    }

    @Override
    public HTableInterface getTable(byte[] tableName) throws SQLException {
        try {
            return HBaseFactoryProvider.getHTableFactory().getTable(tableName, connection, null);
        } catch (org.apache.hadoop.hbase.TableNotFoundException e) {
            throw new TableNotFoundException(SchemaUtil.getSchemaNameFromFullName(tableName), SchemaUtil.getTableNameFromFullName(tableName));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public HTableDescriptor getTableDescriptor(byte[] tableName) throws SQLException {
        HTableInterface htable = getTable(tableName);
        try {
            return htable.getTableDescriptor();
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
                        if (this.txZKClientService != null) this.txZKClientService.stopAndWait();
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
    public void clearTableRegionCache(byte[] tableName) throws SQLException {
        connection.clearRegionCache(TableName.valueOf(tableName));
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
                    HRegionLocation regionLocation = connection.getRegionLocation(
                            TableName.valueOf(tableName), currentKey, reload);
                    locations.add(regionLocation);
                    currentKey = regionLocation.getRegionInfo().getEndKey();
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
                            logger.warn("Attempt to cache older version of " + tableName + ": current= " + table.getSequenceNumber() + ", new=" + tableSeqNum);
                            break;
                        }
                    } catch (TableNotFoundException e) {
                    }
                    long waitTime = endTime - System.currentTimeMillis();
                    // We waited long enough - just remove the table from the cache
                    // and the next time it's used it'll be pulled over from the server.
                    if (waitTime <= 0) {
                        logger.warn("Unable to update meta data repo within " + (DEFAULT_OUT_OF_ORDER_MUTATIONS_WAIT_TIME_MS/1000) + " seconds for " + tableName);
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


    private HColumnDescriptor generateColumnFamilyDescriptor(Pair<byte[],Map<String,Object>> family, PTableType tableType) throws SQLException {
        HColumnDescriptor columnDesc = new HColumnDescriptor(family.getFirst());
        if (tableType != PTableType.VIEW) {
            if(props.get(QueryServices.DEFAULT_KEEP_DELETED_CELLS_ATTRIB) != null){
                columnDesc.setKeepDeletedCells(props.getBoolean(
                        QueryServices.DEFAULT_KEEP_DELETED_CELLS_ATTRIB, QueryServicesOptions.DEFAULT_KEEP_DELETED_CELLS));
            }
            columnDesc.setDataBlockEncoding(SchemaUtil.DEFAULT_DATA_BLOCK_ENCODING);
            for (Entry<String,Object> entry : family.getSecond().entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                setHColumnDescriptorValue(columnDesc, key, value);
            }
        }
        return columnDesc;
    }

    // Workaround HBASE-14737
    private static void setHColumnDescriptorValue(HColumnDescriptor columnDesc, String key, Object value) {
        if (HConstants.VERSIONS.equals(key)) {
            columnDesc.setMaxVersions(getMaxVersion(value));
        } else {
            columnDesc.setValue(key, value == null ? null : value.toString());
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

    private void modifyColumnFamilyDescriptor(HColumnDescriptor hcd, Map<String,Object> props) throws SQLException {
        for (Entry<String, Object> entry : props.entrySet()) {
            String propName = entry.getKey();
            Object value = entry.getValue();
            setHColumnDescriptorValue(hcd, propName, value);
        }
    }

    private HTableDescriptor generateTableDescriptor(byte[] tableName, HTableDescriptor existingDesc,
            PTableType tableType, Map<String, Object> tableProps, List<Pair<byte[], Map<String, Object>>> families,
            byte[][] splits, boolean isNamespaceMapped) throws SQLException {
        String defaultFamilyName = (String)tableProps.remove(PhoenixDatabaseMetaData.DEFAULT_COLUMN_FAMILY_NAME);
        HTableDescriptor tableDescriptor = (existingDesc != null) ? new HTableDescriptor(existingDesc)
        : new HTableDescriptor(
                SchemaUtil.getPhysicalHBaseTableName(tableName, isNamespaceMapped, tableType).getBytes());
        for (Entry<String,Object> entry : tableProps.entrySet()) {
            String key = entry.getKey();
            if (!TableProperty.isPhoenixTableProperty(key)) {
                Object value = entry.getValue();
                tableDescriptor.setValue(key, value == null ? null : value.toString());
            }
        }
        if (families.isEmpty()) {
            if (tableType != PTableType.VIEW) {
                byte[] defaultFamilyByes = defaultFamilyName == null ? QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES : Bytes.toBytes(defaultFamilyName);
                // Add dummy column family so we have key values for tables that
                HColumnDescriptor columnDescriptor = generateColumnFamilyDescriptor(new Pair<byte[],Map<String,Object>>(defaultFamilyByes,Collections.<String,Object>emptyMap()), tableType);
                tableDescriptor.addFamily(columnDescriptor);
            }
        } else {
            for (Pair<byte[],Map<String,Object>> family : families) {
                // If family is only in phoenix description, add it. otherwise, modify its property accordingly.
                byte[] familyByte = family.getFirst();
                if (tableDescriptor.getFamily(familyByte) == null) {
                    if (tableType == PTableType.VIEW) {
                        String fullTableName = Bytes.toString(tableName);
                        throw new ReadOnlyTableException(
                                "The HBase column families for a read-only table must already exist",
                                SchemaUtil.getSchemaNameFromFullName(fullTableName),
                                SchemaUtil.getTableNameFromFullName(fullTableName),
                                Bytes.toString(familyByte));
                    }
                    HColumnDescriptor columnDescriptor = generateColumnFamilyDescriptor(family, tableType);
                    tableDescriptor.addFamily(columnDescriptor);
                } else {
                    if (tableType != PTableType.VIEW) {
                        HColumnDescriptor columnDescriptor = tableDescriptor.getFamily(familyByte);
                        if (columnDescriptor == null) {
                            throw new IllegalArgumentException("Unable to find column descriptor with family name " + Bytes.toString(family.getFirst()));
                        }
                        modifyColumnFamilyDescriptor(columnDescriptor, family.getSecond());
                    }
                }
            }
        }
        addCoprocessors(tableName, tableDescriptor, tableType, tableProps);

        // PHOENIX-3072: Set index priority if this is a system table or index table
        if (tableType == PTableType.SYSTEM) {
            tableDescriptor.setValue(QueryConstants.PRIORITY,
                    String.valueOf(PhoenixRpcSchedulerFactory.getMetadataPriority(config)));
        } else if (tableType == PTableType.INDEX // Global, mutable index
                && !isLocalIndexTable(tableDescriptor.getFamiliesKeys())
                && !Boolean.TRUE.equals(tableProps.get(PhoenixDatabaseMetaData.IMMUTABLE_ROWS))) {
            tableDescriptor.setValue(QueryConstants.PRIORITY,
                    String.valueOf(PhoenixRpcSchedulerFactory.getIndexPriority(config)));
        }
        return tableDescriptor;
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

    
    private void addCoprocessors(byte[] tableName, HTableDescriptor descriptor, PTableType tableType, Map<String,Object> tableProps) throws SQLException {
        // The phoenix jar must be available on HBase classpath
        int priority = props.getInt(QueryServices.COPROCESSOR_PRIORITY_ATTRIB, QueryServicesOptions.DEFAULT_COPROCESSOR_PRIORITY);
        try {
            if (!descriptor.hasCoprocessor(ScanRegionObserver.class.getName())) {
                descriptor.addCoprocessor(ScanRegionObserver.class.getName(), null, priority, null);
            }
            if (!descriptor.hasCoprocessor(UngroupedAggregateRegionObserver.class.getName())) {
                descriptor.addCoprocessor(UngroupedAggregateRegionObserver.class.getName(), null, priority, null);
            }
            if (!descriptor.hasCoprocessor(GroupedAggregateRegionObserver.class.getName())) {
                descriptor.addCoprocessor(GroupedAggregateRegionObserver.class.getName(), null, priority, null);
            }
            if (!descriptor.hasCoprocessor(ServerCachingEndpointImpl.class.getName())) {
                descriptor.addCoprocessor(ServerCachingEndpointImpl.class.getName(), null, priority, null);
            }
            boolean isTransactional =
                    Boolean.TRUE.equals(tableProps.get(TableProperty.TRANSACTIONAL.name())) ||
                    Boolean.TRUE.equals(tableProps.get(TxConstants.READ_NON_TX_DATA)); // For ALTER TABLE
            // TODO: better encapsulation for this
            // Since indexes can't have indexes, don't install our indexing coprocessor for indexes.
            // Also don't install on the SYSTEM.CATALOG and SYSTEM.STATS table because we use
            // all-or-none mutate class which break when this coprocessor is installed (PHOENIX-1318).
            if ((tableType != PTableType.INDEX && tableType != PTableType.VIEW)
                    && !SchemaUtil.isMetaTable(tableName)
                    && !SchemaUtil.isStatsTable(tableName)) {
                if (isTransactional) {
                    if (!descriptor.hasCoprocessor(PhoenixTransactionalIndexer.class.getName())) {
                        descriptor.addCoprocessor(PhoenixTransactionalIndexer.class.getName(), null, priority, null);
                    }
                    // For alter table, remove non transactional index coprocessor
                    if (descriptor.hasCoprocessor(Indexer.class.getName())) {
                        descriptor.removeCoprocessor(Indexer.class.getName());
                    }
                } else {
                    if (!descriptor.hasCoprocessor(Indexer.class.getName())) {
                        // If exception on alter table to transition back to non transactional
                        if (descriptor.hasCoprocessor(PhoenixTransactionalIndexer.class.getName())) {
                            descriptor.removeCoprocessor(PhoenixTransactionalIndexer.class.getName());
                        }
                        Map<String, String> opts = Maps.newHashMapWithExpectedSize(1);
                        opts.put(NonTxIndexBuilder.CODEC_CLASS_NAME_KEY, PhoenixIndexCodec.class.getName());
                        Indexer.enableIndexing(descriptor, PhoenixIndexBuilder.class, opts, priority);
                    }
                }
            }
            if (SchemaUtil.isStatsTable(tableName) && !descriptor.hasCoprocessor(MultiRowMutationEndpoint.class.getName())) {
                descriptor.addCoprocessor(MultiRowMutationEndpoint.class.getName(),
                        null, priority, null);
            }

            Set<byte[]> familiesKeys = descriptor.getFamiliesKeys();
            for(byte[] family: familiesKeys) {
                if(Bytes.toString(family).startsWith(QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX)) {
                    if (!descriptor.hasCoprocessor(IndexHalfStoreFileReaderGenerator.class.getName())) {
                        descriptor.addCoprocessor(IndexHalfStoreFileReaderGenerator.class.getName(),
                                null, priority, null);
                        break;
                    }
                }
            }

            // Setup split policy on Phoenix metadata table to ensure that the key values of a Phoenix table
            // stay on the same region.
            if (SchemaUtil.isMetaTable(tableName) || SchemaUtil.isFunctionTable(tableName)) {
                if (!descriptor.hasCoprocessor(MetaDataEndpointImpl.class.getName())) {
                    descriptor.addCoprocessor(MetaDataEndpointImpl.class.getName(), null, priority, null);
                }
                if(SchemaUtil.isMetaTable(tableName) ) {
                    if (!descriptor.hasCoprocessor(MetaDataRegionObserver.class.getName())) {
                        descriptor.addCoprocessor(MetaDataRegionObserver.class.getName(), null, priority + 1, null);
                    }
                }
            } else if (SchemaUtil.isSequenceTable(tableName)) {
                if (!descriptor.hasCoprocessor(SequenceRegionObserver.class.getName())) {
                    descriptor.addCoprocessor(SequenceRegionObserver.class.getName(), null, priority, null);
                }
            }

            if (isTransactional) {
                if (!descriptor.hasCoprocessor(PhoenixTransactionalProcessor.class.getName())) {
                    descriptor.addCoprocessor(PhoenixTransactionalProcessor.class.getName(), null, priority - 10, null);
                }
            } else {
                // If exception on alter table to transition back to non transactional
                if (descriptor.hasCoprocessor(PhoenixTransactionalProcessor.class.getName())) {
                    descriptor.removeCoprocessor(PhoenixTransactionalProcessor.class.getName());
                }
            }
        } catch (IOException e) {
            throw ServerUtil.parseServerException(e);
        }
    }

    private static interface RetriableOperation {
        boolean checkForCompletion() throws TimeoutException, IOException;
        String getOperationName();
    }

    private void pollForUpdatedTableDescriptor(final HBaseAdmin admin, final HTableDescriptor newTableDescriptor,
            final byte[] tableName) throws InterruptedException, TimeoutException {
        checkAndRetry(new RetriableOperation() {

            @Override
            public String getOperationName() {
                return "UpdateOrNewTableDescriptor";
            }

            @Override
            public boolean checkForCompletion() throws TimeoutException, IOException {
                HTableDescriptor tableDesc = admin.getTableDescriptor(tableName);
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
            if (logger.isDebugEnabled()) {
                logger.debug("Operation "
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

    private NamespaceDescriptor ensureNamespaceCreated(String schemaName) throws SQLException {
        SQLException sqlE = null;
        try (HBaseAdmin admin = getAdmin()) {
            NamespaceDescriptor namespaceDescriptor = null;
            try {
                namespaceDescriptor = admin.getNamespaceDescriptor(schemaName);
            } catch (org.apache.hadoop.hbase.NamespaceNotFoundException e) {

            }
            if (namespaceDescriptor == null) {
                namespaceDescriptor = NamespaceDescriptor.create(schemaName).build();
                admin.createNamespace(namespaceDescriptor);
            }
            return namespaceDescriptor;
        } catch (IOException e) {
            sqlE = ServerUtil.parseServerException(e);
        } finally {
            if (sqlE != null) { throw sqlE; }
        }
        return null; // will never make it here
    }

    /**
     *
     * @param tableName
     * @param splits
     * @param modifyExistingMetaData TODO
     * @return true if table was created and false if it already exists
     * @throws SQLException
     */
    private HTableDescriptor ensureTableCreated(byte[] tableName, PTableType tableType, Map<String, Object> props,
            List<Pair<byte[], Map<String, Object>>> families, byte[][] splits, boolean modifyExistingMetaData,
            boolean isNamespaceMapped) throws SQLException {
        SQLException sqlE = null;
        HTableDescriptor existingDesc = null;
        boolean isMetaTable = SchemaUtil.isMetaTable(tableName);
        byte[] physicalTable = SchemaUtil.getPhysicalHBaseTableName(tableName, isNamespaceMapped, tableType).getBytes();
        boolean tableExist = true;
        try (HBaseAdmin admin = getAdmin()) {
            final String quorum = ZKConfig.getZKQuorumServersString(config);
            final String znode = this.props.get(HConstants.ZOOKEEPER_ZNODE_PARENT);
            logger.debug("Found quorum: " + quorum + ":" + znode);
            try {
                existingDesc = admin.getTableDescriptor(physicalTable);
            } catch (org.apache.hadoop.hbase.TableNotFoundException e) {
                tableExist = false;
                if (tableType == PTableType.VIEW) {
                    String fullTableName = Bytes.toString(tableName);
                    throw new ReadOnlyTableException(
                            "An HBase table for a VIEW must already exist",
                            SchemaUtil.getSchemaNameFromFullName(fullTableName),
                            SchemaUtil.getTableNameFromFullName(fullTableName));
                }
            }

            HTableDescriptor newDesc = generateTableDescriptor(tableName, existingDesc, tableType, props, families,
                    splits, isNamespaceMapped);

            if (!tableExist) {
                if (newDesc.getValue(MetaDataUtil.IS_LOCAL_INDEX_TABLE_PROP_BYTES) != null && Boolean.TRUE.equals(
                        PBoolean.INSTANCE.toObject(newDesc.getValue(MetaDataUtil.IS_LOCAL_INDEX_TABLE_PROP_BYTES)))) {
                    newDesc.setValue(HTableDescriptor.SPLIT_POLICY, IndexRegionSplitPolicy.class.getName());
                }
                // Remove the splitPolicy attribute to prevent HBASE-12570
                if (isMetaTable) {
                    newDesc.remove(HTableDescriptor.SPLIT_POLICY);
                }
                try {
                    if (splits == null) {
                        admin.createTable(newDesc);
                    } else {
                        admin.createTable(newDesc, splits);
                    }
                } catch (TableExistsException e) {
                    // We can ignore this, as it just means that another client beat us
                    // to creating the HBase metadata.
                    return null;
                }
                if (isMetaTable) {
                    checkClientServerCompatibility(SchemaUtil.getPhysicalName(SYSTEM_CATALOG_NAME_BYTES, this.getProps()).getName());
                    /*
                     * Now we modify the table to add the split policy, since we know that the client and
                     * server and compatible. This works around HBASE-12570 which causes the cluster to be
                     * brought down.
                     */
                    newDesc.setValue(HTableDescriptor.SPLIT_POLICY, MetaDataSplitPolicy.class.getName());
                    modifyTable(physicalTable, newDesc, true);
                }
                return null;
            } else {
                if (isMetaTable) {
                    checkClientServerCompatibility(SchemaUtil.getPhysicalName(SYSTEM_CATALOG_NAME_BYTES, this.getProps()).getName());
                } else {
                    for(Pair<byte[],Map<String,Object>> family: families) {
                        if ((newDesc.getValue(HTableDescriptor.SPLIT_POLICY)==null || !newDesc.getValue(HTableDescriptor.SPLIT_POLICY).equals(
                                IndexRegionSplitPolicy.class.getName()))
                                && Bytes.toString(family.getFirst()).startsWith(
                                        QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX)) {
                            newDesc.setValue(HTableDescriptor.SPLIT_POLICY, IndexRegionSplitPolicy.class.getName());
                            break;
                        }
                    }
                }


                if (!modifyExistingMetaData) {
                    return existingDesc; // Caller already knows that no metadata was changed
                }
                boolean willBeTx = Boolean.TRUE.equals(props.get(TableProperty.TRANSACTIONAL.name()));
                // If mapping an existing table as transactional, set property so that existing
                // data is correctly read.
                if (willBeTx) {
                    newDesc.setValue(TxConstants.READ_NON_TX_DATA, Boolean.TRUE.toString());
                } else {
                    // If we think we're creating a non transactional table when it's already
                    // transactional, don't allow.
                    if (existingDesc.hasCoprocessor(PhoenixTransactionalProcessor.class.getName())) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.TX_MAY_NOT_SWITCH_TO_NON_TX)
                        .setSchemaName(SchemaUtil.getSchemaNameFromFullName(tableName))
                        .setTableName(SchemaUtil.getTableNameFromFullName(tableName)).build().buildException();
                    }
                    newDesc.remove(TxConstants.READ_NON_TX_DATA);
                }
                if (existingDesc.equals(newDesc)) {
                    return null; // Indicate that no metadata was changed
                }

                modifyTable(physicalTable, newDesc, true);
                return newDesc;
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

    private void modifyTable(byte[] tableName, HTableDescriptor newDesc, boolean shouldPoll) throws IOException,
    InterruptedException, TimeoutException, SQLException {
        try (HBaseAdmin admin = getAdmin()) {
            if (!allowOnlineTableSchemaUpdate()) {
                admin.disableTable(tableName);
                admin.modifyTable(tableName, newDesc);
                admin.enableTable(tableName);
            } else {
                admin.modifyTable(tableName, newDesc);
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

    private void checkClientServerCompatibility(byte[] metaTable) throws SQLException {
        StringBuilder buf = new StringBuilder("The following servers require an updated " + QueryConstants.DEFAULT_COPROCESS_PATH + " to be put in the classpath of HBase: ");
        boolean isIncompatible = false;
        int minHBaseVersion = Integer.MAX_VALUE;
        boolean isTableNamespaceMappingEnabled = false;
        HTableInterface ht = null;
        try {
            List<HRegionLocation> locations = this
                    .getAllTableRegions(metaTable);
            Set<HRegionLocation> serverMap = Sets.newHashSetWithExpectedSize(locations.size());
            TreeMap<byte[], HRegionLocation> regionMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
            List<byte[]> regionKeys = Lists.newArrayListWithExpectedSize(locations.size());
            for (HRegionLocation entry : locations) {
                if (!serverMap.contains(entry)) {
                    regionKeys.add(entry.getRegionInfo().getStartKey());
                    regionMap.put(entry.getRegionInfo().getRegionName(), entry);
                    serverMap.add(entry);
                }
            }

            ht = this.getTable(metaTable);
            final Map<byte[], Long> results =
                    ht.coprocessorService(MetaDataService.class, null, null, new Batch.Call<MetaDataService,Long>() {
                        @Override
                        public Long call(MetaDataService instance) throws IOException {
                            ServerRpcController controller = new ServerRpcController();
                            BlockingRpcCallback<GetVersionResponse> rpcCallback =
                                    new BlockingRpcCallback<GetVersionResponse>();
                            GetVersionRequest.Builder builder = GetVersionRequest.newBuilder();
                            builder.setClientVersion(VersionUtil.encodeVersion(PHOENIX_MAJOR_VERSION, PHOENIX_MINOR_VERSION, PHOENIX_PATCH_NUMBER));
                            instance.getVersion(controller, builder.build(), rpcCallback);
                            if(controller.getFailedOn() != null) {
                                throw controller.getFailedOn();
                            }
                            return rpcCallback.get().getVersion();
                        }
                    });
            for (Map.Entry<byte[],Long> result : results.entrySet()) {
                // This is the "phoenix.jar" is in-place, but server is out-of-sync with client case.
                long version = result.getValue();
                isTableNamespaceMappingEnabled |= MetaDataUtil.decodeTableNamespaceMappingEnabled(version);

                if (!isCompatible(result.getValue())) {
                    isIncompatible = true;
                    HRegionLocation name = regionMap.get(result.getKey());
                    buf.append(name);
                    buf.append(';');
                }
                hasIndexWALCodec &= hasIndexWALCodec(result.getValue());
                if (minHBaseVersion > MetaDataUtil.decodeHBaseVersion(result.getValue())) {
                    minHBaseVersion = MetaDataUtil.decodeHBaseVersion(result.getValue());
                }
            }
            if (isTableNamespaceMappingEnabled != SchemaUtil.isNamespaceMappingEnabled(PTableType.TABLE,
                    getProps())) { throw new SQLExceptionInfo.Builder(
                            SQLExceptionCode.INCONSISTENET_NAMESPACE_MAPPING_PROPERTIES)
                    .setMessage(
                            "Ensure that config " + QueryServices.IS_NAMESPACE_MAPPING_ENABLED
                            + " is consitent on client and server.")
                            .build().buildException(); }
            lowestClusterHBaseVersion = minHBaseVersion;
        } catch (SQLException e) {
            throw e;
        } catch (Throwable t) {
            // This is the case if the "phoenix.jar" is not on the classpath of HBase on the region server
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.INCOMPATIBLE_CLIENT_SERVER_JAR).setRootCause(t)
            .setMessage("Ensure that " + QueryConstants.DEFAULT_COPROCESS_PATH + " is put on the classpath of HBase in every region server: " + t.getMessage())
            .build().buildException();
        } finally {
            if (ht != null) {
                try {
                    ht.close();
                } catch (IOException e) {
                    logger.warn("Could not close HTable", e);
                }
            }
        }
        if (isIncompatible) {
            buf.setLength(buf.length()-1);
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.OUTDATED_JARS).setMessage(buf.toString()).build().buildException();
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
                    connection.relocateRegion(SchemaUtil.getPhysicalName(tableName, this.getProps()), tableKey);
                }

                HTableInterface ht = this.getTable(SchemaUtil.getPhysicalName(tableName, this.getProps()).getName());
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
        HTableDescriptor desc = ensureTableCreated(physicalIndexName, PTableType.TABLE, tableProps, families, splits,
                false, isNamespaceMapped);
        if (desc != null) {
            if (!Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(desc.getValue(MetaDataUtil.IS_VIEW_INDEX_TABLE_PROP_BYTES)))) {
                String fullTableName = Bytes.toString(physicalIndexName);
                throw new TableAlreadyExistsException(
                        "Unable to create shared physical table for indexes on views.",
                        SchemaUtil.getSchemaNameFromFullName(fullTableName),
                        SchemaUtil.getTableNameFromFullName(fullTableName));
            }
        }
    }

    private boolean ensureViewIndexTableDropped(byte[] physicalTableName, long timestamp) throws SQLException {
        byte[] physicalIndexName = MetaDataUtil.getViewIndexPhysicalName(physicalTableName);
        boolean wasDeleted = false;
        try (HBaseAdmin admin = getAdmin()) {
            try {
                HTableDescriptor desc = admin.getTableDescriptor(physicalIndexName);
                if (Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(desc.getValue(MetaDataUtil.IS_VIEW_INDEX_TABLE_PROP_BYTES)))) {
                    final ReadOnlyProps props = this.getProps();
                    final boolean dropMetadata = props.getBoolean(DROP_METADATA_ATTRIB, DEFAULT_DROP_METADATA);
                    if (dropMetadata) {
                        admin.disableTable(physicalIndexName);
                        admin.deleteTable(physicalIndexName);
                        clearTableRegionCache(physicalIndexName);
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
        HTableDescriptor desc = null;
        boolean wasDeleted = false;
        try (HBaseAdmin admin = getAdmin()) {
            try {
                desc = admin.getTableDescriptor(physicalTableName);
                for (byte[] fam : desc.getFamiliesKeys()) {
                    this.tableStatsCache.invalidate(new GuidePostsKey(physicalTableName, fam));
                }
                final ReadOnlyProps props = this.getProps();
                final boolean dropMetadata = props.getBoolean(DROP_METADATA_ATTRIB, DEFAULT_DROP_METADATA);
                if (dropMetadata) {
                    List<String> columnFamiles = new ArrayList<String>();
                    for(HColumnDescriptor cf : desc.getColumnFamilies()) {
                        if(cf.getNameAsString().startsWith(QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX)) {
                            columnFamiles.add(cf.getNameAsString());
                        }
                    }
                    for(String cf: columnFamiles) {
                        admin.deleteColumn(physicalTableName, cf);
                    }
                    clearTableRegionCache(physicalTableName);
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
            final List<Pair<byte[], Map<String, Object>>> families, byte[][] splits, boolean isNamespaceMapped, final boolean allocateIndexId)
                    throws SQLException {
        byte[][] rowKeyMetadata = new byte[3][];
        Mutation m = MetaDataUtil.getPutOnlyTableHeaderRow(tableMetaData);
        byte[] key = m.getRow();
        SchemaUtil.getVarChars(key, rowKeyMetadata);
        byte[] tenantIdBytes = rowKeyMetadata[PhoenixDatabaseMetaData.TENANT_ID_INDEX];
        byte[] schemaBytes = rowKeyMetadata[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
        byte[] tableBytes = rowKeyMetadata[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
        byte[] tableName = physicalTableName != null ? physicalTableName : SchemaUtil.getTableNameAsBytes(schemaBytes, tableBytes);
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
            ensureTableCreated(tableName, tableType, tableProps, families, splits, true, isNamespaceMapped);
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
                    SchemaUtil.getPhysicalHBaseTableName(tableName, isNamespaceMapped, tableType).getBytes(),
                    tableProps, familiesPlusDefault, MetaDataUtil.isSalted(m, kvBuilder, ptr) ? splits : null,
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
    public MetaDataMutationResult getTable(final PName tenantId, final byte[] schemaBytes, final byte[] tableBytes,
            final long tableTimestamp, final long clientTimestamp) throws SQLException {
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
            final boolean cascade) throws SQLException {
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

    private void invalidateTableStats(final List<byte[]> tableNamesToDelete) {
        if (tableNamesToDelete != null) {
            for (byte[] tableName : tableNamesToDelete) {
                tableStatsCache.invalidateAll(tableName);
            }
        }
    }

    private void dropTable(byte[] tableNameToDelete) throws SQLException {
        dropTables(Collections.<byte[]>singletonList(tableNameToDelete));
    }
    
    private void dropTables(final List<byte[]> tableNamesToDelete) throws SQLException {
        SQLException sqlE = null;
        try (HBaseAdmin admin = getAdmin()) {
            if (tableNamesToDelete != null){
                for ( byte[] tableName : tableNamesToDelete ) {
                    try {
                        HTableDescriptor htableDesc = this.getTableDescriptor(tableName);
                        admin.disableTable(tableName);
                        admin.deleteTable(tableName);
                        tableStatsCache.invalidateAll(htableDesc);
                        clearTableRegionCache(tableName);
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

    private static Map<String,Object> createPropertiesMap(Map<ImmutableBytesWritable,ImmutableBytesWritable> htableProps) {
        Map<String,Object> props = Maps.newHashMapWithExpectedSize(htableProps.size());
        for (Map.Entry<ImmutableBytesWritable,ImmutableBytesWritable> entry : htableProps.entrySet()) {
            ImmutableBytesWritable key = entry.getKey();
            ImmutableBytesWritable value = entry.getValue();
            props.put(Bytes.toString(key.get(), key.getOffset(), key.getLength()), Bytes.toString(value.get(), value.getOffset(), value.getLength()));
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
        HTableDescriptor htableDesc = this.getTableDescriptor(physicalTableName);
        Map<String,Object> tableProps = createPropertiesMap(htableDesc.getValues());
        List<Pair<byte[],Map<String,Object>>> families = Lists.newArrayListWithExpectedSize(Math.max(1, table.getColumnFamilies().size()+1));
        if (families.isEmpty()) {
            byte[] familyName = SchemaUtil.getEmptyColumnFamily(table);
            Map<String,Object> familyProps = createPropertiesMap(htableDesc.getFamily(familyName).getValues());
            families.add(new Pair<byte[],Map<String,Object>>(familyName, familyProps));
        } else {
            for (PColumnFamily family : table.getColumnFamilies()) {
                byte[] familyName = family.getName().getBytes();
                Map<String,Object> familyProps = createPropertiesMap(htableDesc.getFamily(familyName).getValues());
                families.add(new Pair<byte[],Map<String,Object>>(familyName, familyProps));
            }
            // Always create default column family, because we don't know in advance if we'll
            // need it for an index with no covered columns.
            families.add(new Pair<byte[],Map<String,Object>>(table.getDefaultFamilyName().getBytes(), Collections.<String,Object>emptyMap()));
        }
        byte[][] splits = null;
        if (table.getBucketNum() != null) {
            splits = SaltingUtil.getSalteByteSplitPoints(table.getBucketNum());
        }

        // Transfer over table values into tableProps
        // TODO: encapsulate better
        tableProps.put(PhoenixDatabaseMetaData.TRANSACTIONAL, table.isTransactional());
        tableProps.put(PhoenixDatabaseMetaData.IMMUTABLE_ROWS, table.isImmutableRows());
        ensureViewIndexTableCreated(physicalTableName, tableProps, families, splits, timestamp, isNamespaceMapped);
    }

    @Override
    public MetaDataMutationResult addColumn(final List<Mutation> tableMetaData, PTable table, Map<String, List<Pair<String,Object>>> stmtProperties, Set<String> colFamiliesForPColumnsToBeAdded, List<PColumn> columns) throws SQLException {
        List<Pair<byte[], Map<String, Object>>> families = new ArrayList<>(stmtProperties.size());
        Map<String, Object> tableProps = new HashMap<String, Object>();
        Set<HTableDescriptor> tableDescriptors = Collections.emptySet();
        Set<HTableDescriptor> origTableDescriptors = Collections.emptySet();
        boolean nonTxToTx = false;
        Pair<HTableDescriptor,HTableDescriptor> tableDescriptorPair = separateAndValidateProperties(table, stmtProperties, colFamiliesForPColumnsToBeAdded, families, tableProps);
        HTableDescriptor tableDescriptor = tableDescriptorPair.getSecond();
        HTableDescriptor origTableDescriptor = tableDescriptorPair.getFirst();
        if (tableDescriptor != null) {
            tableDescriptors = Sets.newHashSetWithExpectedSize(3 + table.getIndexes().size());
            origTableDescriptors = Sets.newHashSetWithExpectedSize(3 + table.getIndexes().size());
            tableDescriptors.add(tableDescriptor);
            origTableDescriptors.add(origTableDescriptor);
            nonTxToTx = Boolean.TRUE.equals(tableProps.get(TxConstants.READ_NON_TX_DATA));
            /*
             * If the table was transitioned from non transactional to transactional, we need
             * to also transition the index tables.
             */
            if (nonTxToTx) {
                updateDescriptorForTx(table, tableProps, tableDescriptor, Boolean.TRUE.toString(), tableDescriptors, origTableDescriptors);
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
                return new MetaDataMutationResult(MutationCode.NO_OP, System.currentTimeMillis(), table);
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
    private void updateDescriptorForTx(PTable table, Map<String, Object> tableProps, HTableDescriptor tableDescriptor,
            String txValue, Set<HTableDescriptor> descriptorsToUpdate, Set<HTableDescriptor> origDescriptors) throws SQLException {
        byte[] physicalTableName = table.getPhysicalName().getBytes();
        try (HBaseAdmin admin = getAdmin()) {
            setTransactional(tableDescriptor, table.getType(), txValue, tableProps);
            Map<String, Object> indexTableProps;
            if (txValue == null) {
                indexTableProps = Collections.<String,Object>emptyMap();
            } else {
                indexTableProps = Maps.newHashMapWithExpectedSize(1);
                indexTableProps.put(TxConstants.READ_NON_TX_DATA, Boolean.valueOf(txValue));
            }
            for (PTable index : table.getIndexes()) {
                HTableDescriptor indexDescriptor = admin.getTableDescriptor(index.getPhysicalName().getBytes());
                origDescriptors.add(indexDescriptor);
                indexDescriptor = new HTableDescriptor(indexDescriptor);
                descriptorsToUpdate.add(indexDescriptor);
                if (index.getColumnFamilies().isEmpty()) {
                    byte[] dataFamilyName = SchemaUtil.getEmptyColumnFamily(table);
                    byte[] indexFamilyName = SchemaUtil.getEmptyColumnFamily(index);
                    HColumnDescriptor indexColDescriptor = indexDescriptor.getFamily(indexFamilyName);
                    HColumnDescriptor tableColDescriptor = tableDescriptor.getFamily(dataFamilyName);
                    indexColDescriptor.setMaxVersions(tableColDescriptor.getMaxVersions());
                    indexColDescriptor.setValue(TxConstants.PROPERTY_TTL, tableColDescriptor.getValue(TxConstants.PROPERTY_TTL));
                } else {
                    for (PColumnFamily family : index.getColumnFamilies()) {
                        byte[] familyName = family.getName().getBytes();
                        indexDescriptor.getFamily(familyName).setMaxVersions(tableDescriptor.getFamily(familyName).getMaxVersions());
                        HColumnDescriptor indexColDescriptor = indexDescriptor.getFamily(familyName);
                        HColumnDescriptor tableColDescriptor = tableDescriptor.getFamily(familyName);
                        indexColDescriptor.setMaxVersions(tableColDescriptor.getMaxVersions());
                        indexColDescriptor.setValue(TxConstants.PROPERTY_TTL, tableColDescriptor.getValue(TxConstants.PROPERTY_TTL));
                    }
                }
                setTransactional(indexDescriptor, index.getType(), txValue, indexTableProps);
            }
            try {
                HTableDescriptor indexDescriptor = admin.getTableDescriptor(MetaDataUtil.getViewIndexPhysicalName(physicalTableName));
                origDescriptors.add(indexDescriptor);
                indexDescriptor = new HTableDescriptor(indexDescriptor);
                descriptorsToUpdate.add(indexDescriptor);
                setSharedIndexMaxVersion(table, tableDescriptor, indexDescriptor);
                setTransactional(indexDescriptor, PTableType.INDEX, txValue, indexTableProps);
            } catch (org.apache.hadoop.hbase.TableNotFoundException ignore) {
                // Ignore, as we may never have created a view index table
            }
            try {
                HTableDescriptor indexDescriptor = admin.getTableDescriptor(MetaDataUtil.getLocalIndexPhysicalName(physicalTableName));
                origDescriptors.add(indexDescriptor);
                indexDescriptor = new HTableDescriptor(indexDescriptor);
                descriptorsToUpdate.add(indexDescriptor);
                setSharedIndexMaxVersion(table, tableDescriptor, indexDescriptor);
                setTransactional(indexDescriptor, PTableType.INDEX, txValue, indexTableProps);
            } catch (org.apache.hadoop.hbase.TableNotFoundException ignore) {
                // Ignore, as we may never have created a view index table
            }
        } catch (IOException e) {
            throw ServerUtil.parseServerException(e);
        }
    }
    private void setSharedIndexMaxVersion(PTable table, HTableDescriptor tableDescriptor,
            HTableDescriptor indexDescriptor) {
        if (table.getColumnFamilies().isEmpty()) {
            byte[] familyName = SchemaUtil.getEmptyColumnFamily(table);
            HColumnDescriptor indexColDescriptor = indexDescriptor.getFamily(familyName);
            HColumnDescriptor tableColDescriptor = tableDescriptor.getFamily(familyName);
            indexColDescriptor.setMaxVersions(tableColDescriptor.getMaxVersions());
            indexColDescriptor.setValue(TxConstants.PROPERTY_TTL, tableColDescriptor.getValue(TxConstants.PROPERTY_TTL));
        } else {
            for (PColumnFamily family : table.getColumnFamilies()) {
                byte[] familyName = family.getName().getBytes();
                HColumnDescriptor indexColDescriptor = indexDescriptor.getFamily(familyName);
                if (indexColDescriptor != null) {
                    HColumnDescriptor tableColDescriptor = tableDescriptor.getFamily(familyName);
                    indexColDescriptor.setMaxVersions(tableColDescriptor.getMaxVersions());
                    indexColDescriptor.setValue(TxConstants.PROPERTY_TTL, tableColDescriptor.getValue(TxConstants.PROPERTY_TTL));
                }
            }
        }
    }

    private void sendHBaseMetaData(Set<HTableDescriptor> tableDescriptors, boolean pollingNeeded) throws SQLException {
        SQLException sqlE = null;
        for (HTableDescriptor descriptor : tableDescriptors) {
            try {
                modifyTable(descriptor.getName(), descriptor, pollingNeeded);
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
    private void setTransactional(HTableDescriptor tableDescriptor, PTableType tableType, String txValue, Map<String, Object> tableProps) throws SQLException {
        if (txValue == null) {
            tableDescriptor.remove(TxConstants.READ_NON_TX_DATA);
        } else {
            tableDescriptor.setValue(TxConstants.READ_NON_TX_DATA, txValue);
        }
        this.addCoprocessors(tableDescriptor.getName(), tableDescriptor, tableType, tableProps);
    }

    private Pair<HTableDescriptor,HTableDescriptor> separateAndValidateProperties(PTable table, Map<String, List<Pair<String, Object>>> properties, Set<String> colFamiliesForPColumnsToBeAdded, List<Pair<byte[], Map<String, Object>>> families, Map<String, Object> tableProps) throws SQLException {
        Map<String, Map<String, Object>> stmtFamiliesPropsMap = new HashMap<>(properties.size());
        Map<String,Object> commonFamilyProps = new HashMap<>();
        boolean addingColumns = colFamiliesForPColumnsToBeAdded != null && !colFamiliesForPColumnsToBeAdded.isEmpty();
        HashSet<String> existingColumnFamilies = existingColumnFamilies(table);
        Map<String, Map<String, Object>> allFamiliesProps = new HashMap<>(existingColumnFamilies.size());
        boolean isTransactional = table.isTransactional();
        boolean willBeTransactional = false;
        boolean isOrWillBeTransactional = isTransactional;
        Integer newTTL = null;
        for (String family : properties.keySet()) {
            List<Pair<String, Object>> propsList = properties.get(family);
            if (propsList != null && propsList.size() > 0) {
                Map<String, Object> colFamilyPropsMap = new HashMap<String, Object>(propsList.size());
                for (Pair<String, Object> prop : propsList) {
                    String propName = prop.getFirst();
                    Object propValue = prop.getSecond();
                    if ((MetaDataUtil.isHTableProperty(propName) ||  TableProperty.isPhoenixTableProperty(propName)) && addingColumns) {
                        // setting HTable and PhoenixTable properties while adding a column is not allowed.
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_SET_TABLE_PROPERTY_ADD_COLUMN)
                        .setMessage("Property: " + propName).build()
                        .buildException();
                    }
                    if (MetaDataUtil.isHTableProperty(propName)) {
                        // Can't have a column family name for a property that's an HTableProperty
                        if (!family.equals(QueryConstants.ALL_FAMILY_PROPERTIES_KEY)) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.COLUMN_FAMILY_NOT_ALLOWED_TABLE_PROPERTY)
                            .setMessage("Column Family: " + family + ", Property: " + propName).build()
                            .buildException();
                        }
                        tableProps.put(propName, propValue);
                    } else {
                        if (TableProperty.isPhoenixTableProperty(propName)) {
                            TableProperty.valueOf(propName).validate(true, !family.equals(QueryConstants.ALL_FAMILY_PROPERTIES_KEY), table.getType());
                            if (propName.equals(TTL)) {
                                newTTL = ((Number)prop.getSecond()).intValue();
                                // Even though TTL is really a HColumnProperty we treat it specially.
                                // We enforce that all column families have the same TTL.
                                commonFamilyProps.put(propName, prop.getSecond());
                            } else if (propName.equals(PhoenixDatabaseMetaData.TRANSACTIONAL) && Boolean.TRUE.equals(propValue)) {
                                willBeTransactional = isOrWillBeTransactional = true;
                                tableProps.put(TxConstants.READ_NON_TX_DATA, propValue);
                            }
                        } else {
                            if (MetaDataUtil.isHColumnProperty(propName)) {
                                if (family.equals(QueryConstants.ALL_FAMILY_PROPERTIES_KEY)) {
                                    commonFamilyProps.put(propName, propValue);
                                } else {
                                    colFamilyPropsMap.put(propName, propValue);
                                }
                            } else {
                                // invalid property - neither of HTableProp, HColumnProp or PhoenixTableProp
                                // FIXME: This isn't getting triggered as currently a property gets evaluated
                                // as HTableProp if its neither HColumnProp or PhoenixTableProp.
                                throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_ALTER_PROPERTY)
                                .setMessage("Column Family: " + family + ", Property: " + propName).build()
                                .buildException();
                            }
                        }
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
                    Map<String, Object> m = new HashMap<String, Object>(commonFamilyProps.size());
                    m.putAll(commonFamilyProps);
                    allFamiliesProps.put(existingColFamily, m);
                }
            } else {
                // Add the common family props to the column families of the columns being added
                for (String colFamily : colFamiliesForPColumnsToBeAdded) {
                    if (colFamily != null) {
                        // only set properties for key value columns
                        Map<String, Object> m = new HashMap<String, Object>(commonFamilyProps.size());
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
                throw new ColumnFamilyNotFoundException(f);
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

        HTableDescriptor newTableDescriptor = null;
        HTableDescriptor origTableDescriptor = null;
        if (!allFamiliesProps.isEmpty() || !tableProps.isEmpty()) {
            byte[] tableNameBytes = Bytes.toBytes(table.getPhysicalName().getString());
            HTableDescriptor existingTableDescriptor = origTableDescriptor = getTableDescriptor(tableNameBytes);
            newTableDescriptor = new HTableDescriptor(existingTableDescriptor);
            if (!tableProps.isEmpty()) {
                // add all the table properties to the existing table descriptor
                for (Entry<String, Object> entry : tableProps.entrySet()) {
                    newTableDescriptor.setValue(entry.getKey(), entry.getValue() != null ? entry.getValue().toString() : null);
                }
            }
            if (addingColumns) {
                // Make sure that all the CFs of the table have the same TTL as the empty CF.
                setTTLForNewCFs(allFamiliesProps, table, newTableDescriptor, newTTL);
            }
            // Set TTL on all table column families, even if they're not referenced here
            if (newTTL != null) {
                for (PColumnFamily family : table.getColumnFamilies()) {
                    if (!allFamiliesProps.containsKey(family.getName().getString())) {
                        Map<String,Object> familyProps = Maps.newHashMapWithExpectedSize(1);
                        familyProps.put(TTL, newTTL);
                        allFamiliesProps.put(family.getName().getString(), familyProps);
                    }
                }
            }
            Integer defaultTxMaxVersions = null;
            if (isOrWillBeTransactional) {
                // Calculate default for max versions
                Map<String, Object> emptyFamilyProps = allFamiliesProps.get(SchemaUtil.getEmptyColumnFamilyAsString(table));
                if (emptyFamilyProps != null) {
                    defaultTxMaxVersions = (Integer)emptyFamilyProps.get(HConstants.VERSIONS);
                }
                if (defaultTxMaxVersions == null) {
                    if (isTransactional) {
                        defaultTxMaxVersions = newTableDescriptor.getFamily(SchemaUtil.getEmptyColumnFamily(table)).getMaxVersions();
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
                            familyProps.put(HConstants.VERSIONS, defaultTxMaxVersions);
                            allFamiliesProps.put(family.getName().getString(), familyProps);
                        }
                    }
                }
            }
            // Set Tephra's TTL property based on HBase property if we're
            // transitioning to become transactional or setting TTL on
            // an already transactional table.
            if (isOrWillBeTransactional) {
                int ttl = getTTL(table, newTableDescriptor, newTTL);
                if (ttl != HColumnDescriptor.DEFAULT_TTL) {
                    for (Map.Entry<String, Map<String, Object>> entry : allFamiliesProps.entrySet()) {
                        Map<String, Object> props = entry.getValue();
                        if (props == null) {
                            props = new HashMap<String, Object>();
                        }
                        props.put(TxConstants.PROPERTY_TTL, ttl);
                        // Remove HBase TTL if we're not transitioning an existing table to become transactional
                        // or if the existing transactional table wasn't originally non transactional.
                        if (!willBeTransactional && !Boolean.valueOf(newTableDescriptor.getValue(TxConstants.READ_NON_TX_DATA))) {
                            props.remove(TTL);
                        }
                    }
                }
            }
            for (Entry<String, Map<String, Object>> entry : allFamiliesProps.entrySet()) {
                Map<String,Object> familyProps = entry.getValue();
                if (isOrWillBeTransactional) {
                    if (!familyProps.containsKey(HConstants.VERSIONS)) {
                        familyProps.put(HConstants.VERSIONS, defaultTxMaxVersions);
                    }
                }
                byte[] cf = Bytes.toBytes(entry.getKey());
                HColumnDescriptor colDescriptor = newTableDescriptor.getFamily(cf);
                if (colDescriptor == null) {
                    // new column family
                    colDescriptor = generateColumnFamilyDescriptor(new Pair<>(cf, familyProps), table.getType());
                    newTableDescriptor.addFamily(colDescriptor);
                } else {
                    modifyColumnFamilyDescriptor(colDescriptor, familyProps);
                }
                if (isOrWillBeTransactional) {
                    checkTransactionalVersionsValue(colDescriptor);
                }
            }
        }
        return new Pair<>(origTableDescriptor, newTableDescriptor);
    }

    private void checkTransactionalVersionsValue(HColumnDescriptor colDescriptor) throws SQLException {
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

    private static int getTTL(PTable table, HTableDescriptor tableDesc, Integer newTTL) throws SQLException {
        // If we're setting TTL now, then use that value. Otherwise, use empty column family value
        int ttl = newTTL != null ? newTTL
                : tableDesc.getFamily(SchemaUtil.getEmptyColumnFamily(table)).getTimeToLive();
        return ttl;
    }

    private static void setTTLForNewCFs(Map<String, Map<String, Object>> familyProps, PTable table,
            HTableDescriptor tableDesc, Integer newTTL) throws SQLException {
        if (!familyProps.isEmpty()) {
            int ttl = getTTL(table, tableDesc, newTTL);
            for (Map.Entry<String, Map<String, Object>> entry : familyProps.entrySet()) {
                Map<String, Object> props = entry.getValue();
                if (props == null) {
                    props = new HashMap<String, Object>();
                }
                props.put(TTL, ttl);
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
            logger.warn("Table already modified at this timestamp, so assuming column already nullable: " + columnName);
        } catch (SQLException e) {
            logger.warn("Add column failed due to:" + e);
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
            logger.warn("Table already modified at this timestamp, so assuming add of these columns already done: " + columns);
        } catch (SQLException e) {
            logger.warn("Add column failed due to:" + e);
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

    @Override
    public void init(final String url, final Properties props) throws SQLException {
        try {
            PhoenixContextExecutor.call(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    if (initialized) {
                        if (initializationException != null) {
                            // Throw previous initialization exception, as we won't resuse this instance
                            throw initializationException;
                        }
                        return null;
                    }
                    synchronized (ConnectionQueryServicesImpl.this) {
                        if (initialized) {
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
                            logger.info("An instance of ConnectionQueryServices was created: " + Throwables.getStackTraceAsString(new Exception()));
                            openConnection();
                            hConnectionEstablished = true;
                            boolean isDoNotUpgradePropSet = UpgradeUtil.isNoUpgradeSet(props);
                            try (HBaseAdmin admin = getAdmin()) {
                                boolean mappedSystemCatalogExists = admin
                                        .tableExists(SchemaUtil.getPhysicalTableName(SYSTEM_CATALOG_NAME_BYTES, true));
                                if (SchemaUtil.isNamespaceMappingEnabled(PTableType.SYSTEM,
                                        ConnectionQueryServicesImpl.this.getProps())) {
                                    if (admin.tableExists(SYSTEM_CATALOG_NAME_BYTES)) {
                                        //check if the server is already updated and have namespace config properly set. 
                                        checkClientServerCompatibility(SYSTEM_CATALOG_NAME_BYTES);
                                    }
                                    ensureSystemTablesUpgraded(ConnectionQueryServicesImpl.this.getProps());
                                } else if (mappedSystemCatalogExists) { throw new SQLExceptionInfo.Builder(
                                        SQLExceptionCode.INCONSISTENET_NAMESPACE_MAPPING_PROPERTIES)
                                .setMessage("Cannot initiate connection as "
                                        + SchemaUtil.getPhysicalTableName(
                                                SYSTEM_CATALOG_NAME_BYTES, true)
                                                + " is found but client does not have "
                                                + IS_NAMESPACE_MAPPING_ENABLED + " enabled")
                                                .build().buildException(); }
                                createSysMutexTable(admin);
                            }
                            Properties scnProps = PropertiesUtil.deepCopy(props);
                            scnProps.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
                                    Long.toString(MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP));
                            scnProps.remove(PhoenixRuntime.TENANT_ID_ATTRIB);
                            String globalUrl = JDBCUtil.removeProperty(url, PhoenixRuntime.TENANT_ID_ATTRIB);
                            try (PhoenixConnection metaConnection = new PhoenixConnection(ConnectionQueryServicesImpl.this, globalUrl,
                                    scnProps, newEmptyMetaData())) {
                                try {
                                    metaConnection.createStatement().executeUpdate(QueryConstants.CREATE_TABLE_METADATA);
                                } catch (NewerTableAlreadyExistsException ignore) {
                                    // Ignore, as this will happen if the SYSTEM.CATALOG already exists at this fixed
                                    // timestamp. A TableAlreadyExistsException is not thrown, since the table only exists
                                    // *after* this fixed timestamp.
                                } catch (TableAlreadyExistsException e) {
                                    long currentServerSideTableTimeStamp = e.getTable().getTimeStamp();
                                    if (currentServerSideTableTimeStamp < MIN_SYSTEM_TABLE_TIMESTAMP) {
                                        ConnectionQueryServicesImpl.this.upgradeRequired.set(true);
                                    }
                                }
                                if (!ConnectionQueryServicesImpl.this.upgradeRequired.get()) {
                                    createOtherSystemTables(metaConnection);
                                } else if (isAutoUpgradeEnabled && !isDoNotUpgradePropSet) {
                                    upgradeSystemTables(url, props);
                                }
                            }
                            scheduleRenewLeaseTasks();
                            success = true;
                        } catch (RetriableUpgradeException e) {
                            // Don't set it as initializationException because otherwise the clien't won't be able
                            // to retry establishing connection.
                            throw e;
                        } catch (Exception e) {
                            if (e instanceof SQLException) {
                                initializationException = (SQLException)e;
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
                                    if (initializationException != null) { throw initializationException; }
                                } finally {
                                    initialized = true;
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
    
    private void createSysMutexTable(HBaseAdmin admin) throws IOException, SQLException {
        try {
            HTableDescriptor tableDesc = new HTableDescriptor(
                    TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_MUTEX_NAME_BYTES));
            HColumnDescriptor columnDesc = new HColumnDescriptor(
                    PhoenixDatabaseMetaData.SYSTEM_MUTEX_FAMILY_NAME_BYTES);
            columnDesc.setTimeToLive(TTL_FOR_MUTEX); // Let mutex expire after some time
            tableDesc.addFamily(columnDesc);
            admin.createTable(tableDesc);
            try (HTableInterface sysMutexTable = getTable(PhoenixDatabaseMetaData.SYSTEM_MUTEX_NAME_BYTES)) {
                byte[] mutexRowKey = SchemaUtil.getTableKey(null, PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA,
                        PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE);
                Put put = new Put(mutexRowKey);
                put.add(PhoenixDatabaseMetaData.SYSTEM_MUTEX_FAMILY_NAME_BYTES, UPGRADE_MUTEX, UPGRADE_MUTEX_UNLOCKED);
                sysMutexTable.put(put);
            }
        } catch (TableExistsException e) {
            // Ignore
        }
    }

    private void createOtherSystemTables(PhoenixConnection metaConnection) throws SQLException {
        try {
            metaConnection.createStatement().execute(QueryConstants.CREATE_SEQUENCE_METADATA);
        } catch (TableAlreadyExistsException e) {
            nSequenceSaltBuckets = getSaltBuckets(e);
        }
        try {
            metaConnection.createStatement().execute(QueryConstants.CREATE_STATS_TABLE_METADATA);
        } catch (TableAlreadyExistsException ignore) {}
        try {
            metaConnection.createStatement().execute(QueryConstants.CREATE_FUNCTION_METADATA);
        } catch (TableAlreadyExistsException ignore) {}
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
        byte[] mutexRowKey = SchemaUtil.getTableKey(null, PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA,
                PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE);
        boolean snapshotCreated = false;
        try {
            if (!ConnectionQueryServicesImpl.this.upgradeRequired.get()) {
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
            try {
                metaConnection.createStatement().executeUpdate(QueryConstants.CREATE_TABLE_METADATA);
            } catch (NewerTableAlreadyExistsException ignore) {
                // Ignore, as this will happen if the SYSTEM.CATALOG already exists at this fixed
                // timestamp. A TableAlreadyExistsException is not thrown, since the table only exists
                // *after* this fixed timestamp.
            } catch (TableAlreadyExistsException e) {
                long currentServerSideTableTimeStamp = e.getTable().getTimeStamp();
                sysCatalogTableName = e.getTable().getPhysicalName().getString();
                if (currentServerSideTableTimeStamp < MIN_SYSTEM_TABLE_TIMESTAMP
                        && (acquiredMutexLock = acquireUpgradeMutex(currentServerSideTableTimeStamp, mutexRowKey))) {
                    snapshotName = getSysCatalogSnapshotName(currentServerSideTableTimeStamp);
                    createSnapshot(snapshotName, sysCatalogTableName);
                    snapshotCreated = true;
                }
                String columnsToAdd = "";
                // This will occur if we have an older SYSTEM.CATALOG and we need to update it to
                // include any new columns we've added.
                if (currentServerSideTableTimeStamp < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_3_0) {
                    // We know that we always need to add the STORE_NULLS column for 4.3 release
                    columnsToAdd = addColumn(columnsToAdd, PhoenixDatabaseMetaData.STORE_NULLS
                            + " " + PBoolean.INSTANCE.getSqlTypeName());
                    try (HBaseAdmin admin = getAdmin()) {
                        HTableDescriptor[] localIndexTables = admin
                                .listTables(MetaDataUtil.LOCAL_INDEX_TABLE_PREFIX + ".*");
                        for (HTableDescriptor table : localIndexTables) {
                            if (table.getValue(MetaDataUtil.PARENT_TABLE_KEY) == null
                                    && table.getValue(MetaDataUtil.IS_LOCAL_INDEX_TABLE_PROP_NAME) != null) {
                                table.setValue(MetaDataUtil.PARENT_TABLE_KEY,
                                        MetaDataUtil.getLocalIndexUserTableName(table.getNameAsString()));
                                // Explicitly disable, modify and enable the table to ensure
                                // co-location of data and index regions. If we just modify the
                                // table descriptor when online schema change enabled may reopen 
                                // the region in same region server instead of following data region.
                                admin.disableTable(table.getTableName());
                                admin.modifyTable(table.getTableName(), table);
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
                        logger.debug("No need to run 4.5 upgrade");
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
                            logger.warn("The following tables require upgrade due to a bug causing the row key to be incorrect for descending columns and ascending BINARY columns (PHOENIX-2067 and PHOENIX-2120):\n"
                                    + Joiner.on(' ').join(tablesNeedingUpgrade)
                                    + "\nTo upgrade issue the \"bin/psql.py -u\" command.");
                        }
                        List<String> unsupportedTables = UpgradeUtil
                                .getPhysicalTablesWithDescVarbinaryRowKey(conn);
                        if (!unsupportedTables.isEmpty()) {
                            logger.warn("The following tables use an unsupported VARBINARY DESC construct and need to be changed:\n"
                                    + Joiner.on(' ').join(unsupportedTables));
                        }
                    } catch (Exception ex) {
                        logger.error(
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
            }


            int nSaltBuckets = ConnectionQueryServicesImpl.this.props.getInt(
                    QueryServices.SEQUENCE_SALT_BUCKETS_ATTRIB,
                    QueryServicesOptions.DEFAULT_SEQUENCE_TABLE_SALT_BUCKETS);
            try {
                String createSequenceTable = Sequence.getCreateTableStatement(nSaltBuckets);
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
                        clearTableRegionCache(PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME_BYTES);
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
                metaConnection.createStatement().executeUpdate(QueryConstants.CREATE_FUNCTION_METADATA);
            } catch (NewerTableAlreadyExistsException e) {} catch (TableAlreadyExistsException e) {}
            if (SchemaUtil.isNamespaceMappingEnabled(PTableType.SYSTEM,
                    ConnectionQueryServicesImpl.this.getProps())) {
                try {
                    metaConnection.createStatement().executeUpdate(
                            "CREATE SCHEMA IF NOT EXISTS "
                                    + PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA);
                } catch (NewerSchemaAlreadyExistsException e) {}
            }
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
                        releaseUpgradeMutex(mutexRowKey);
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
                Bytes.toBytes("COLUMN_QUALIFIER"));
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
        HBaseAdmin admin = null;
        SQLException sqlE = null;
        try {
            admin = getAdmin();
            admin.snapshot(snapshotName, tableName);
            logger.info("Successfully created snapshot " + snapshotName + " for "
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
            HBaseAdmin admin = null;
            try {
                logger.warn("Starting restore of " + tableName + " using snapshot "
                        + snapshotName + " because upgrade failed");
                admin = getAdmin();
                admin.disableTable(tableName);
                tableDisabled = true;
                admin.restoreSnapshot(snapshotName);
                snapshotRestored = true;
                logger.warn("Successfully restored " + tableName + " using snapshot "
                        + snapshotName);
            } catch (Exception e) {
                sqlE = new SQLException(e);
            } finally {
                if (admin != null && tableDisabled) {
                    try {
                        admin.enableTable(tableName);
                        if (snapshotRestored) {
                            logger.warn("Successfully restored and enabled " + tableName + " using snapshot "
                                    + snapshotName);
                        } else {
                            logger.warn("Successfully enabled " + tableName + " after restoring using snapshot "
                                    + snapshotName + " failed. ");
                        }
                    } catch (Exception e1) {
                        SQLException enableTableEx = new SQLException(e1);
                        if (sqlE == null) {
                            sqlE = enableTableEx;
                        } else {
                            sqlE.setNextException(enableTableEx);
                        }
                        logger.error("Failure in enabling "
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
    
    private void ensureSystemTablesUpgraded(ReadOnlyProps props)
            throws SQLException, IOException, IllegalArgumentException, InterruptedException {
        if (!SchemaUtil.isNamespaceMappingEnabled(PTableType.SYSTEM, props)) { return; }
        HTableInterface metatable = null;
        try (HBaseAdmin admin = getAdmin()) {
            ensureNamespaceCreated(QueryConstants.SYSTEM_SCHEMA_NAME);
            
            List<TableName> tableNames = Lists.newArrayList(admin.listTableNames(QueryConstants.SYSTEM_SCHEMA_NAME + "\\..*"));
            if (tableNames.size() == 0) { return; }
            if (tableNames.size() > 5) {
                logger.warn("Expected 5 system tables but found " + tableNames.size() + ":" + tableNames);
            }
            byte[] mappedSystemTable = SchemaUtil
                    .getPhysicalName(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES, props).getName();
            metatable = getTable(mappedSystemTable);
            if (tableNames.contains(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME)) {
                if (!admin.tableExists(mappedSystemTable)) {
                    UpgradeUtil.mapTableToNamespace(admin, metatable,
                            PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME, props, null, PTableType.SYSTEM,
                            null);
                    ConnectionQueryServicesImpl.this.removeTable(null,
                            PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME, null,
                            MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_1_0);
                }
                tableNames.remove(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME);
            }
            tableNames.remove(PhoenixDatabaseMetaData.SYSTEM_MUTEX_NAME);
            for (TableName table : tableNames) {
                UpgradeUtil.mapTableToNamespace(admin, metatable, table.getNameAsString(), props, null, PTableType.SYSTEM,
                        null);
                ConnectionQueryServicesImpl.this.removeTable(null, table.getNameAsString(), null,
                        MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_1_0);
            }
            if (!tableNames.isEmpty()) {
                clearCache();
            }
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
    public boolean acquireUpgradeMutex(long currentServerSideTableTimestamp, byte[] rowToLock) throws IOException,
            SQLException {
        Preconditions.checkArgument(currentServerSideTableTimestamp < MIN_SYSTEM_TABLE_TIMESTAMP);
        try (HTableInterface sysMutexTable = getTable(PhoenixDatabaseMetaData.SYSTEM_MUTEX_NAME_BYTES)) {
            byte[] family = PhoenixDatabaseMetaData.SYSTEM_MUTEX_FAMILY_NAME_BYTES;
            byte[] qualifier = UPGRADE_MUTEX;
            byte[] oldValue = UPGRADE_MUTEX_UNLOCKED;
            byte[] newValue = UPGRADE_MUTEX_LOCKED;
            Put put = new Put(rowToLock);
            put.addColumn(family, qualifier, newValue);
            boolean acquired =  sysMutexTable.checkAndPut(rowToLock, family, qualifier, oldValue, put);
            if (!acquired) {
                /*
                 * Because of TTL on the SYSTEM_MUTEX_FAMILY, it is very much possible that the cell
                 * has gone away. So we need to retry with an old value of null. Note there is a small
                 * race condition here that between the two checkAndPut calls, it is possible that another
                 * request would have set the value back to UPGRADE_MUTEX_UNLOCKED. In that scenario this
                 * following checkAndPut would still return false even though the lock was available.
                 */
                acquired =  sysMutexTable.checkAndPut(rowToLock, family, qualifier, null, put);
                if (!acquired) {
                    throw new UpgradeInProgressException(getVersion(currentServerSideTableTimestamp),
                        getVersion(MIN_SYSTEM_TABLE_TIMESTAMP));
                }
            }
            return true;
        }
    }
    
    @VisibleForTesting
    public boolean releaseUpgradeMutex(byte[] mutexRowKey) {
        boolean released = false;
        try (HTableInterface sysMutexTable = getTable(PhoenixDatabaseMetaData.SYSTEM_MUTEX_NAME_BYTES)) {
            byte[] family = PhoenixDatabaseMetaData.SYSTEM_MUTEX_FAMILY_NAME_BYTES;
            byte[] qualifier = UPGRADE_MUTEX;
            byte[] expectedValue = UPGRADE_MUTEX_LOCKED;
            byte[] newValue = UPGRADE_MUTEX_UNLOCKED;
            Put put = new Put(mutexRowKey);
            put.addColumn(family, qualifier, newValue);
            released = sysMutexTable.checkAndPut(mutexRowKey, family, qualifier, expectedValue, put);
        } catch (Exception e) {
            logger.warn("Release of upgrade mutex failed", e);
        }
        return released;
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
            logger.warn("exception during upgrading stats table:" + e);
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
            logger.warn("exception during upgrading stats table:" + e);
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
            logger.warn("exception during upgrading stats table:" + e);
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
            ThreadFactory threadFactory =
                    new ThreadFactoryBuilder().setDaemon(true)
                    .setNameFormat("PHOENIX-SCANNER-RENEW-LEASE" + "-thread-%s").build();
            renewLeaseExecutor =
                    Executors.newScheduledThreadPool(renewLeasePoolSize, threadFactory);
            for (LinkedBlockingQueue<WeakReference<PhoenixConnection>> q : connectionQueues) {
                renewLeaseExecutor.scheduleAtFixedRate(new RenewLeaseTask(q), 0,
                        renewLeaseTaskFrequency, TimeUnit.MILLISECONDS);
            }
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
        try {
            SQLException sqlE = null;
            HTableInterface htable = this.getTable(SchemaUtil
                    .getPhysicalName(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES, this.getProps()).getName());
            try {
                tableStatsCache.invalidateAll();
                final Map<byte[], Long> results =
                        htable.coprocessorService(MetaDataService.class, HConstants.EMPTY_START_ROW,
                                HConstants.EMPTY_END_ROW, new Batch.Call<MetaDataService, Long>() {
                            @Override
                            public Long call(MetaDataService instance) throws IOException {
                                ServerRpcController controller = new ServerRpcController();
                                BlockingRpcCallback<ClearCacheResponse> rpcCallback =
                                        new BlockingRpcCallback<ClearCacheResponse>();
                                ClearCacheRequest.Builder builder = ClearCacheRequest.newBuilder();
                                builder.setClientVersion(VersionUtil.encodeVersion(PHOENIX_MAJOR_VERSION, PHOENIX_MINOR_VERSION, PHOENIX_PATCH_NUMBER));
                                instance.clearCache(controller, builder.build(), rpcCallback);
                                if(controller.getFailedOn() != null) {
                                    throw controller.getFailedOn();
                                }
                                return rpcCallback.get().getUnfreedBytes();
                            }
                        });
                long unfreedBytes = 0;
                for (Map.Entry<byte[],Long> result : results.entrySet()) {
                    if (result.getValue() != null) {
                        unfreedBytes += result.getValue();
                    }
                }
                return unfreedBytes;
            } catch (IOException e) {
                throw ServerUtil.parseServerException(e);
            } catch (Throwable e) {
                sqlE = new SQLException(e);
            } finally {
                try {
                    tableStatsCache.invalidateAll();
                    htable.close();
                } catch (IOException e) {
                    if (sqlE == null) {
                        sqlE = ServerUtil.parseServerException(e);
                    } else {
                        sqlE.setNextException(ServerUtil.parseServerException(e));
                    }
                } finally {
                    if (sqlE != null) {
                        throw sqlE;
                    }
                }
            }
        } catch (Exception e) {
            throw new SQLException(ServerUtil.parseServerException(e));
        }
        return 0;
    }

    private void flushTable(byte[] tableName) throws SQLException {
        HBaseAdmin admin = getAdmin();
        try {
            admin.flush(tableName);
        } catch (IOException e) {
            throw new PhoenixIOException(e);
        } catch (InterruptedException e) {
            // restore the interrupt status
            Thread.currentThread().interrupt();
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.INTERRUPTED_EXCEPTION).setRootCause(e).build()
            .buildException();
        } finally {
            Closeables.closeQuietly(admin);
        }
    }

    @Override
    public HBaseAdmin getAdmin() throws SQLException {
        try {
            return new HBaseAdmin(connection);
        } catch (IOException e) {
            throw new PhoenixIOException(e);
        }
    }

    @Override
    public MetaDataMutationResult updateIndexState(final List<Mutation> tableMetaData, String parentTableName) throws SQLException {
        byte[][] rowKeyMetadata = new byte[3][];
        SchemaUtil.getVarChars(tableMetaData.get(0).getRow(), rowKeyMetadata);
        byte[] tableKey = SchemaUtil.getTableKey(ByteUtil.EMPTY_BYTE_ARRAY, rowKeyMetadata[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX], rowKeyMetadata[PhoenixDatabaseMetaData.TABLE_NAME_INDEX]);
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
            HTableInterface htable = this.getTable(SchemaUtil
                    .getPhysicalName(PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME_BYTES, this.getProps()).getName());
            htable.setAutoFlush(true);
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
            HTableInterface htable = this.getTable(SchemaUtil
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

    @SuppressWarnings("deprecation")
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
            HTableInterface hTable = this.getTable(SchemaUtil.getPhysicalName(PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME_BYTES,this.getProps()).getName());
            Object[] resultObjects = null;
            SQLException sqlE = null;
            try {
                resultObjects= hTable.batch(incrementBatch);
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
            HTableInterface htable = this.getTable(SchemaUtil
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

    @SuppressWarnings("deprecation")
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
            HTableInterface hTable = this.getTable(SchemaUtil
                    .getPhysicalName(PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME_BYTES, this.getProps()).getName());
            Object[] resultObjects = null;
            SQLException sqlE = null;
            try {
                resultObjects= hTable.batch(mutations);
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
    @SuppressWarnings("deprecation")
    private void returnAllSequences(ConcurrentMap<SequenceKey,Sequence> sequenceMap) throws SQLException {
        List<Append> mutations = Lists.newArrayListWithExpectedSize(sequenceMap.size());
        for (Sequence sequence : sequenceMap.values()) {
            mutations.addAll(sequence.newReturns());
        }
        if (mutations.isEmpty()) {
            return;
        }
        HTableInterface hTable = this.getTable(
                SchemaUtil.getPhysicalName(PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME_BYTES, this.getProps()).getName());
        SQLException sqlE = null;
        try {
            hTable.batch(mutations);
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
        connectionQueues.get(getQueueIndex(connection)).add(new WeakReference<PhoenixConnection>(connection));
    }

    @Override
    public void removeConnection(PhoenixConnection connection) throws SQLException {
        if (returnSequenceValues) {
            ConcurrentMap<SequenceKey,Sequence> formerSequenceMap = null;
            synchronized (connectionCountLock) {
                if (--connectionCount == 0) {
                    if (!this.sequenceMap.isEmpty()) {
                        formerSequenceMap = this.sequenceMap;
                        this.sequenceMap = Maps.newConcurrentMap();
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
                                    logger.info("Lease renewed for scanner: " + scanningItr);
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
                            logger.info("Renewed leases for " + renewed + " scanner/s in "
                                    + (System.currentTimeMillis() - start) + " ms ");
                        }
                        connectionsQueue.offer(connRef);
                    }
                    numConnections--;
                }
            } catch (InternalRenewLeaseTaskException e) {
                logger.error("Exception thrown when renewing lease. Draining the queue of scanners ", e);
                // clear up the queue since the task is about to be unscheduled.
                connectionsQueue.clear();
                // throw an exception since we want the task execution to be suppressed because we just encountered an
                // exception that happened because of a bug.
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // restore the interrupt status
                logger.error("Thread interrupted when renewing lease.", e);
            } catch (Exception e) {
                logger.error("Exception thrown when renewing lease ", e);
                // don't drain the queue and swallow the exception in this case since we don't want the task
                // execution to be suppressed because renewing lease of a scanner failed.
            } catch (Throwable e) {
                logger.error("Exception thrown when renewing lease. Draining the queue of scanners ", e);
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
                return connection.getRegionLocation(TableName.valueOf(tableName), row, reload);
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
        try (HBaseAdmin admin = getAdmin()) {
            final String quorum = ZKConfig.getZKQuorumServersString(config);
            final String znode = this.props.get(HConstants.ZOOKEEPER_ZNODE_PARENT);
            logger.debug("Found quorum: " + quorum + ":" + znode);
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
}
