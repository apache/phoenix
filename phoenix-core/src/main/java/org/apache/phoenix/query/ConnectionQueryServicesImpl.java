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

import static org.apache.hadoop.hbase.HColumnDescriptor.TTL;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_DROP_METADATA;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.GuardedBy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.regionserver.IndexHalfStoreFileReaderGenerator;
import org.apache.hadoop.hbase.regionserver.LocalIndexSplitter;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.hadoop.security.UserGroupInformation;
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
import org.apache.phoenix.coprocessor.UngroupedAggregateRegionObserver;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.AddColumnRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.ClearCacheRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.ClearCacheResponse;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.ClearTableFromCacheRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.ClearTableFromCacheResponse;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.CreateTableRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.DropColumnRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.DropTableRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.GetTableRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.GetVersionRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.GetVersionResponse;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.MetaDataResponse;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.MetaDataService;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.UpdateIndexStateRequest;
import org.apache.phoenix.exception.PhoenixIOException;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.hbase.index.IndexRegionSplitPolicy;
import org.apache.phoenix.hbase.index.Indexer;
import org.apache.phoenix.hbase.index.covered.CoveredColumnsIndexBuilder;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.index.PhoenixIndexBuilder;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver.ConnectionInfo;
import org.apache.phoenix.protobuf.ProtobufUtil;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.schema.EmptySequenceCacheException;
import org.apache.phoenix.schema.MetaDataSplitPolicy;
import org.apache.phoenix.schema.NewerTableAlreadyExistsException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PMetaData;
import org.apache.phoenix.schema.PMetaDataImpl;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.ReadOnlyTableException;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.schema.Sequence;
import org.apache.phoenix.schema.SequenceKey;
import org.apache.phoenix.schema.TableAlreadyExistsException;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.TableProperty;
import org.apache.phoenix.schema.stats.PTableStats;
import org.apache.phoenix.schema.stats.StatisticsUtil;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PUnsignedTinyint;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.Closeables;
import org.apache.phoenix.util.ConfigUtil;
import org.apache.phoenix.util.JDBCUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixContextExecutor;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ServerUtil;
import org.apache.phoenix.util.UpgradeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.HBaseZeroCopyByteString;


public class ConnectionQueryServicesImpl extends DelegateQueryServices implements ConnectionQueryServices {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionQueryServicesImpl.class);
    private static final int INITIAL_CHILD_SERVICES_CAPACITY = 100;
    private static final int DEFAULT_OUT_OF_ORDER_MUTATIONS_WAIT_TIME_MS = 1000;
    // Max number of cached table stats for view or shared index physical tables
    private static final int MAX_TABLE_STATS_CACHE_ENTRIES = 512;
    protected final Configuration config;
    // Copy of config.getProps(), but read-only to prevent synchronization that we
    // don't need.
    private final ReadOnlyProps props;
    private final String userName;
    private final ConcurrentHashMap<ImmutableBytesWritable,ConnectionQueryServices> childServices;
    private final Cache<ImmutableBytesPtr, PTableStats> tableStatsCache;

    // Cache the latest meta data here for future connections
    // writes guarded by "latestMetaDataLock"
    private volatile PMetaData latestMetaData;
    private final Object latestMetaDataLock = new Object();

    // Lowest HBase version on the cluster.
    private int lowestClusterHBaseVersion = Integer.MAX_VALUE;
    private boolean hasInvalidIndexConfiguration = false;

    @GuardedBy("connectionCountLock")
    private int connectionCount = 0;
    private final Object connectionCountLock = new Object();

    private HConnection connection;
    private volatile boolean initialized;
    private volatile int nSequenceSaltBuckets;

    // writes guarded by "this"
    private volatile boolean closed;

    private volatile SQLException initializationException;
    // setting this member variable guarded by "connectionCountLock"
    private volatile ConcurrentMap<SequenceKey,Sequence> sequenceMap = Maps.newConcurrentMap();
    private KeyValueBuilder kvBuilder;

    private static interface FeatureSupported {
        boolean isSupported(ConnectionQueryServices services);
    }
    
    private final Map<Feature, FeatureSupported> featureMap = ImmutableMap.<Feature, FeatureSupported>of(
            Feature.LOCAL_INDEX, new FeatureSupported(){
                @Override
                public boolean isSupported(ConnectionQueryServices services) {
                    int hbaseVersion = services.getLowestClusterHBaseVersion();
                    return hbaseVersion < PhoenixDatabaseMetaData.MIN_LOCAL_SI_VERSION_DISALLOW || hbaseVersion > PhoenixDatabaseMetaData.MAX_LOCAL_SI_VERSION_DISALLOW;
                }
            });
    
    private PMetaData newEmptyMetaData() {
        long maxSizeBytes = props.getLong(QueryServices.MAX_CLIENT_METADATA_CACHE_SIZE_ATTRIB,
                QueryServicesOptions.DEFAULT_MAX_CLIENT_METADATA_CACHE_SIZE);
        return new PMetaDataImpl(INITIAL_META_DATA_TABLE_CAPACITY, maxSizeBytes);
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
        long halfStatsUpdateFreq = config.getLong(
                QueryServices.STATS_UPDATE_FREQ_MS_ATTRIB,
                QueryServicesOptions.DEFAULT_STATS_UPDATE_FREQ_MS) / 2;
        tableStatsCache = CacheBuilder.newBuilder()
                .maximumSize(MAX_TABLE_STATS_CACHE_ENTRIES)
                .expireAfterWrite(halfStatsUpdateFreq, TimeUnit.MILLISECONDS)
                .build();
    }

    private void openConnection() throws SQLException {
        try {
            // check if we need to authenticate with kerberos
            String clientKeytab = this.getProps().get(HBASE_CLIENT_KEYTAB);
            String clientPrincipal = this.getProps().get(HBASE_CLIENT_PRINCIPAL);
            if (clientKeytab != null && clientPrincipal != null) {
                logger.info("Trying to connect to a secure cluster with keytab:" + clientKeytab);
                UserGroupInformation.setConfiguration(config);
                User.login(config, HBASE_CLIENT_KEYTAB, HBASE_CLIENT_PRINCIPAL, null);
                logger.info("Successfull login to secure cluster!!");
            }
            this.connection = HBaseFactoryProvider.getHConnectionFactory().createConnection(this.config);
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
            byte[][] schemaAndTableName = new byte[2][];
            SchemaUtil.getVarChars(tableName, schemaAndTableName);
            throw new TableNotFoundException(Bytes.toString(schemaAndTableName[0]), Bytes.toString(schemaAndTableName[1]));
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
            if(e instanceof org.apache.hadoop.hbase.TableNotFoundException ||
                e.getCause() instanceof org.apache.hadoop.hbase.TableNotFoundException) {
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
                    if (connection != null) connection.close();
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
    public PMetaData addTable(PTable table) throws SQLException {
        synchronized (latestMetaDataLock) {
            try {
                throwConnectionClosedIfNullMetaData();
                // If existing table isn't older than new table, don't replace
                // If a client opens a connection at an earlier timestamp, this can happen
                PTable existingTable = latestMetaData.getTable(new PTableKey(table.getTenantId(), table.getName().getString()));
                if (existingTable.getTimeStamp() >= table.getTimeStamp()) {
                    return latestMetaData;
                }
            } catch (TableNotFoundException e) {}
            latestMetaData = latestMetaData.addTable(table);
            latestMetaDataLock.notifyAll();
            return latestMetaData;
        }
    }

    private static interface Mutator {
        PMetaData mutate(PMetaData metaData) throws SQLException;
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
                        table = metaData.getTable(new PTableKey(tenantId, tableName));
                        /* If the table is at the prior sequence number, then we're good to go.
                         * We know if we've got this far, that the server validated the mutations,
                         * so we'd just need to wait until the other connection that mutated the same
                         * table is processed.
                         */
                        if (table.getSequenceNumber() + 1 == tableSeqNum) {
                            // TODO: assert that timeStamp is bigger that table timeStamp?
                            metaData = mutator.mutate(metaData);
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
                        metaData = metaData.removeTable(tenantId, tableName, null, HConstants.LATEST_TIMESTAMP);
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
    public PMetaData addColumn(final PName tenantId, final String tableName, final List<PColumn> columns, final long tableTimeStamp, final long tableSeqNum, final boolean isImmutableRows, final boolean isWalDisabled, final boolean isMultitenant, final boolean storeNulls) throws SQLException {
        return metaDataMutated(tenantId, tableName, tableSeqNum, new Mutator() {
            @Override
            public PMetaData mutate(PMetaData metaData) throws SQLException {
                try {
                    return metaData.addColumn(tenantId, tableName, columns, tableTimeStamp, tableSeqNum, isImmutableRows, isWalDisabled, isMultitenant, storeNulls);
                } catch (TableNotFoundException e) {
                    // The DROP TABLE may have been processed first, so just ignore.
                    return metaData;
                }
            }
        });
     }

    @Override
    public PMetaData removeTable(PName tenantId, final String tableName, String parentTableName, long tableTimeStamp) throws SQLException {
        synchronized (latestMetaDataLock) {
            throwConnectionClosedIfNullMetaData();
            latestMetaData = latestMetaData.removeTable(tenantId, tableName, parentTableName, tableTimeStamp);
            latestMetaDataLock.notifyAll();
            return latestMetaData;
        }
    }

    @Override
    public PMetaData removeColumn(final PName tenantId, final String tableName, final List<PColumn> columnsToRemove, final long tableTimeStamp, final long tableSeqNum) throws SQLException {
        return metaDataMutated(tenantId, tableName, tableSeqNum, new Mutator() {
            @Override
            public PMetaData mutate(PMetaData metaData) throws SQLException {
                try {
                    return metaData.removeColumn(tenantId, tableName, columnsToRemove, tableTimeStamp, tableSeqNum);
                } catch (TableNotFoundException e) {
                    // The DROP TABLE may have been processed first, so just ignore.
                    return metaData;
                }
            }
        });
    }


    @Override
    public PhoenixConnection connect(String url, Properties info) throws SQLException {
        checkClosed();
        PMetaData metadata = latestMetaData;
        if (metadata == null) {
            throwConnectionClosedException();
        }
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
                columnDesc.setValue(key, value == null ? null : value.toString());
            }
        }
        return columnDesc;
    }

    private void modifyColumnFamilyDescriptor(HColumnDescriptor hcd, Pair<byte[], Map<String,Object>> family) throws SQLException {
        if (Bytes.equals(hcd.getName(), family.getFirst())) {
            modifyColumnFamilyDescriptor(hcd, family.getSecond());
        } else {
            throw new IllegalArgumentException("Column family names don't match. Column descriptor family name: " + hcd.getNameAsString() + ", Family name: " + Bytes.toString(family.getFirst()));
        }
    }
    
    private void modifyColumnFamilyDescriptor(HColumnDescriptor hcd, Map<String,Object> props) throws SQLException {
        for (Entry<String, Object> entry : props.entrySet()) {
            String propName = entry.getKey();
            Object value = entry.getValue();
            hcd.setValue(propName, value == null ? null : value.toString());
        }
    }
    
    private HTableDescriptor generateTableDescriptor(byte[] tableName, HTableDescriptor existingDesc, PTableType tableType, Map<String,Object> tableProps, List<Pair<byte[],Map<String,Object>>> families, byte[][] splits) throws SQLException {
        String defaultFamilyName = (String)tableProps.remove(PhoenixDatabaseMetaData.DEFAULT_COLUMN_FAMILY_NAME);
        HTableDescriptor tableDescriptor = (existingDesc != null) ? new HTableDescriptor(existingDesc) :
          new HTableDescriptor(TableName.valueOf(tableName));
        for (Entry<String,Object> entry : tableProps.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            tableDescriptor.setValue(key, value == null ? null : value.toString());
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
                        modifyColumnFamilyDescriptor(tableDescriptor.getFamily(familyByte), family);
                    }
                }
            }
        }
        addCoprocessors(tableName, tableDescriptor, tableType);
        return tableDescriptor;
    }

    private void addCoprocessors(byte[] tableName, HTableDescriptor descriptor, PTableType tableType) throws SQLException {
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
            // TODO: better encapsulation for this
            // Since indexes can't have indexes, don't install our indexing coprocessor for indexes.
            // Also don't install on the SYSTEM.CATALOG and SYSTEM.STATS table because we use
            // all-or-none mutate class which break when this coprocessor is installed (PHOENIX-1318).
            if ((tableType != PTableType.INDEX && tableType != PTableType.VIEW)
                    && !SchemaUtil.isMetaTable(tableName)
                    && !SchemaUtil.isStatsTable(tableName)
                    && !descriptor.hasCoprocessor(Indexer.class.getName())) {
                Map<String, String> opts = Maps.newHashMapWithExpectedSize(1);
                opts.put(CoveredColumnsIndexBuilder.CODEC_CLASS_NAME_KEY, PhoenixIndexCodec.class.getName());
                Indexer.enableIndexing(descriptor, PhoenixIndexBuilder.class, opts, priority);
            }
            if (SchemaUtil.isStatsTable(tableName) && !descriptor.hasCoprocessor(MultiRowMutationEndpoint.class.getName())) {
                descriptor.addCoprocessor(MultiRowMutationEndpoint.class.getName(),
                        null, priority, null);
            }

            if (descriptor.getValue(MetaDataUtil.IS_LOCAL_INDEX_TABLE_PROP_BYTES) != null
                    && Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(descriptor
                            .getValue(MetaDataUtil.IS_LOCAL_INDEX_TABLE_PROP_BYTES)))) {
                if (!descriptor.hasCoprocessor(IndexHalfStoreFileReaderGenerator.class.getName())) {
                    descriptor.addCoprocessor(IndexHalfStoreFileReaderGenerator.class.getName(),
                        null, priority, null);
                }
            } else {
                if (!descriptor.hasCoprocessor(LocalIndexSplitter.class.getName())
                        && !SchemaUtil.isMetaTable(tableName)
                        && !SchemaUtil.isSequenceTable(tableName)) {
                    descriptor.addCoprocessor(LocalIndexSplitter.class.getName(), null, priority, null);
                }
            }

            // Setup split policy on Phoenix metadata table to ensure that the key values of a Phoenix table
            // stay on the same region.
            if (SchemaUtil.isMetaTable(tableName)) {
                if (!descriptor.hasCoprocessor(MetaDataEndpointImpl.class.getName())) {
                    descriptor.addCoprocessor(MetaDataEndpointImpl.class.getName(), null, priority, null);
                }
                if (!descriptor.hasCoprocessor(MetaDataRegionObserver.class.getName())) {
                    descriptor.addCoprocessor(MetaDataRegionObserver.class.getName(), null, priority + 1, null);
                }
            } else if (SchemaUtil.isSequenceTable(tableName)) {
                if (!descriptor.hasCoprocessor(SequenceRegionObserver.class.getName())) {
                    descriptor.addCoprocessor(SequenceRegionObserver.class.getName(), null, priority, null);
                }
            }
        } catch (IOException e) {
            throw ServerUtil.parseServerException(e);
        }
    }

    private static interface RetriableOperation {
        boolean checkForCompletion() throws TimeoutException, IOException;
        String getOperatioName();
    }

    private void pollForUpdatedTableDescriptor(final HBaseAdmin admin, final HTableDescriptor newTableDescriptor,
            final byte[] tableName) throws InterruptedException, TimeoutException {
        checkAndRetry(new RetriableOperation() {

            @Override
            public String getOperatioName() {
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
        Stopwatch watch = new Stopwatch();
        watch.start();
        do {
            try {
                success = op.checkForCompletion();
            } catch (Exception ex) {
                // If we encounter any exception on the first or last try, propagate the exception and fail.
                // Else, we swallow the exception and retry till we reach maxRetries.
                if (numTries == 1 || numTries == maxRetries) {
                    watch.stop();
                    TimeoutException toThrow = new TimeoutException("Operation " + op.getOperatioName()
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
            throw new TimeoutException("Operation  " + op.getOperatioName() + " didn't complete within "
                    + watch.elapsedMillis() + " ms "
                    + (numTries > 1 ? ("after trying " + numTries + (numTries > 1 ? "times." : "time.")) : ""));
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Operation "
                        + op.getOperatioName()
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

    /**
     *
     * @param tableName
     * @param splits
     * @param modifyExistingMetaData TODO
     * @return true if table was created and false if it already exists
     * @throws SQLException
     */
    private HTableDescriptor ensureTableCreated(byte[] tableName, PTableType tableType , Map<String,Object> props, List<Pair<byte[],Map<String,Object>>> families, byte[][] splits, boolean modifyExistingMetaData) throws SQLException {
        HBaseAdmin admin = null;
        SQLException sqlE = null;
        HTableDescriptor existingDesc = null;
        boolean isMetaTable = SchemaUtil.isMetaTable(tableName);
        boolean tableExist = true;
        try {
            logger.info("Found quorum: " + ZKConfig.getZKQuorumServersString(config));
            admin = new HBaseAdmin(config);
            try {
                existingDesc = admin.getTableDescriptor(tableName);
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

            HTableDescriptor newDesc = generateTableDescriptor(tableName, existingDesc, tableType , props, families, splits);

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
                    checkClientServerCompatibility();
                    /*
                     * Now we modify the table to add the split policy, since we know that the client and
                     * server and compatible. This works around HBASE-12570 which causes the cluster to be
                     * brought down.
                     */
                    newDesc.setValue(HTableDescriptor.SPLIT_POLICY, MetaDataSplitPolicy.class.getName());
                    if (allowOnlineTableSchemaUpdate()) {
                        // No need to wait/poll for this update
                        admin.modifyTable(tableName, newDesc);
                    } else {
                        admin.disableTable(tableName);
                        admin.modifyTable(tableName, newDesc);
                        admin.enableTable(tableName);
                    }
                }
                return null;
            } else {
                if (isMetaTable) {
                    checkClientServerCompatibility();
                }

                if (!modifyExistingMetaData || existingDesc.equals(newDesc)) {
                    return existingDesc;
                }

                modifyTable(tableName, newDesc, true);
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
            try {
                if (admin != null) {
                    admin.close();
                }
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
        return null; // will never make it here
    }

    private void modifyTable(byte[] tableName, HTableDescriptor newDesc, boolean shouldPoll) throws IOException,
    InterruptedException, TimeoutException {
    	try (HBaseAdmin admin = new HBaseAdmin(config)) {
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

    private static boolean isInvalidMutableIndexConfig(Long serverVersion) {
        if (serverVersion == null) {
            return false;
        }
        return !MetaDataUtil.decodeMutableIndexConfiguredProperly(serverVersion);
    }

    private static boolean isCompatible(Long serverVersion) {
        if (serverVersion == null) {
            return false;
        }
        return MetaDataUtil.areClientAndServerCompatible(serverVersion);
    }

    private void checkClientServerCompatibility() throws SQLException {
        StringBuilder buf = new StringBuilder("The following servers require an updated " + QueryConstants.DEFAULT_COPROCESS_PATH + " to be put in the classpath of HBase: ");
        boolean isIncompatible = false;
        int minHBaseVersion = Integer.MAX_VALUE;
        try {
            List<HRegionLocation> locations = this.getAllTableRegions(SYSTEM_CATALOG_NAME_BYTES);
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

            HTableInterface ht = this.getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            final Map<byte[], Long> results =
                    ht.coprocessorService(MetaDataService.class, null, null, new Batch.Call<MetaDataService,Long>() {
                        @Override
                        public Long call(MetaDataService instance) throws IOException {
                            ServerRpcController controller = new ServerRpcController();
                            BlockingRpcCallback<GetVersionResponse> rpcCallback =
                                    new BlockingRpcCallback<GetVersionResponse>();
                            GetVersionRequest.Builder builder = GetVersionRequest.newBuilder();

                            instance.getVersion(controller, builder.build(), rpcCallback);
                            if(controller.getFailedOn() != null) {
                                throw controller.getFailedOn();
                            }
                            return rpcCallback.get().getVersion();
                        }
                    });
            for (Map.Entry<byte[],Long> result : results.entrySet()) {
                // This is the "phoenix.jar" is in-place, but server is out-of-sync with client case.
                if (!isCompatible(result.getValue())) {
                    isIncompatible = true;
                    HRegionLocation name = regionMap.get(result.getKey());
                    buf.append(name);
                    buf.append(';');
                }
                hasInvalidIndexConfiguration |= isInvalidMutableIndexConfig(result.getValue());
                if (minHBaseVersion > MetaDataUtil.decodeHBaseVersion(result.getValue())) {
                    minHBaseVersion = MetaDataUtil.decodeHBaseVersion(result.getValue());
                }
            }
            lowestClusterHBaseVersion = minHBaseVersion;
        } catch (SQLException e) {
            throw e;
        } catch (Throwable t) {
            // This is the case if the "phoenix.jar" is not on the classpath of HBase on the region server
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.INCOMPATIBLE_CLIENT_SERVER_JAR).setRootCause(t)
                .setMessage("Ensure that " + QueryConstants.DEFAULT_COPROCESS_PATH + " is put on the classpath of HBase in every region server: " + t.getMessage())
                .build().buildException();
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
        try {
            boolean retried = false;
            while (true) {
                if (retried) {
                    connection.relocateRegion(
                        TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES),
                        tableKey);
                }

                HTableInterface ht = this.getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
                try {
                    final Map<byte[], MetaDataResponse> results =
                            ht.coprocessorService(MetaDataService.class, tableKey, tableKey, callable);

                    assert(results.size() == 1);
                    MetaDataResponse result = results.values().iterator().next();
                    if (result.getReturnCode() == MetaDataProtos.MutationCode.TABLE_NOT_IN_REGION) {
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

    private void ensureViewIndexTableCreated(byte[] physicalTableName, Map<String,Object> tableProps, List<Pair<byte[],Map<String,Object>>> families, byte[][] splits, long timestamp) throws SQLException {
        Long maxFileSize = (Long)tableProps.get(HTableDescriptor.MAX_FILESIZE);
        if (maxFileSize == null) {
            maxFileSize = this.config.getLong(HConstants.HREGION_MAX_FILESIZE, HConstants.DEFAULT_MAX_FILE_SIZE);
        }
        byte[] physicalIndexName = MetaDataUtil.getViewIndexPhysicalName(physicalTableName);

        int indexMaxFileSizePerc;
        // Get percentage to use from table props first and then fallback to config
        Integer indexMaxFileSizePercProp = (Integer)tableProps.remove(QueryServices.INDEX_MAX_FILESIZE_PERC_ATTRIB);
        if (indexMaxFileSizePercProp == null) {
            indexMaxFileSizePerc = config.getInt(QueryServices.INDEX_MAX_FILESIZE_PERC_ATTRIB, QueryServicesOptions.DEFAULT_INDEX_MAX_FILESIZE_PERC);
        } else {
            indexMaxFileSizePerc = indexMaxFileSizePercProp;
        }
        long indexMaxFileSize = maxFileSize * indexMaxFileSizePerc / 100;
        tableProps.put(HTableDescriptor.MAX_FILESIZE, indexMaxFileSize);
        tableProps.put(MetaDataUtil.IS_VIEW_INDEX_TABLE_PROP_NAME, TRUE_BYTES_AS_STRING);
        HTableDescriptor desc = ensureTableCreated(physicalIndexName, PTableType.TABLE, tableProps, families, splits, false);
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

    private void ensureLocalIndexTableCreated(byte[] physicalTableName, Map<String,Object> tableProps, List<Pair<byte[],Map<String,Object>>> families, byte[][] splits, long timestamp) throws SQLException {
        PTable table;
        String parentTableName = Bytes.toString(physicalTableName, MetaDataUtil.LOCAL_INDEX_TABLE_PREFIX_BYTES.length,
            physicalTableName.length - MetaDataUtil.LOCAL_INDEX_TABLE_PREFIX_BYTES.length);
        try {
            synchronized (latestMetaDataLock) {
                throwConnectionClosedIfNullMetaData();
                table = latestMetaData.getTable(new PTableKey(PName.EMPTY_NAME, parentTableName));
                latestMetaDataLock.notifyAll();
            }
            if (table.getTimeStamp() >= timestamp) { // Table in cache is newer than client timestamp which shouldn't be the case
                throw new TableNotFoundException(table.getSchemaName().getString(), table.getTableName().getString());
            }
        } catch (TableNotFoundException e) {
            byte[] schemaName = Bytes.toBytes(SchemaUtil.getSchemaNameFromFullName(parentTableName));
            byte[] tableName = Bytes.toBytes(SchemaUtil.getTableNameFromFullName(parentTableName));
            MetaDataMutationResult result = this.getTable(null, schemaName, tableName, HConstants.LATEST_TIMESTAMP, timestamp);
            table = result.getTable();
            if (table == null) {
                throw e;
            }
        }
        ensureLocalIndexTableCreated(physicalTableName, tableProps, families, splits);
    }

    private void ensureLocalIndexTableCreated(byte[] physicalTableName, Map<String, Object> tableProps, List<Pair<byte[], Map<String, Object>>> families, byte[][] splits) throws SQLException, TableAlreadyExistsException {
        
        // If we're not allowing local indexes or the hbase version is too low,
        // don't create the local index table
        if (   !this.getProps().getBoolean(QueryServices.ALLOW_LOCAL_INDEX_ATTRIB, QueryServicesOptions.DEFAULT_ALLOW_LOCAL_INDEX) 
            || !this.supportsFeature(Feature.LOCAL_INDEX)) {
                    return;
        }
        
        tableProps.put(MetaDataUtil.IS_LOCAL_INDEX_TABLE_PROP_NAME, TRUE_BYTES_AS_STRING);
        HTableDescriptor desc = ensureTableCreated(physicalTableName, PTableType.TABLE, tableProps, families, splits, true);
        if (desc != null) {
            if (!Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(desc.getValue(MetaDataUtil.IS_LOCAL_INDEX_TABLE_PROP_BYTES)))) {
                String fullTableName = Bytes.toString(physicalTableName);
                throw new TableAlreadyExistsException(
                        "Unable to create shared physical table for local indexes.",
                        SchemaUtil.getSchemaNameFromFullName(fullTableName),
                        SchemaUtil.getTableNameFromFullName(fullTableName));
            }
        }
    }

    private boolean ensureViewIndexTableDropped(byte[] physicalTableName, long timestamp) throws SQLException {
        byte[] physicalIndexName = MetaDataUtil.getViewIndexPhysicalName(physicalTableName);
        HTableDescriptor desc = null;
        HBaseAdmin admin = null;
        boolean wasDeleted = false;
        try {
            admin = new HBaseAdmin(config);
            try {
                desc = admin.getTableDescriptor(physicalIndexName);
                if (Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(desc.getValue(MetaDataUtil.IS_VIEW_INDEX_TABLE_PROP_BYTES)))) {
                    this.tableStatsCache.invalidate(new ImmutableBytesPtr(physicalIndexName));
                    final ReadOnlyProps props = this.getProps();
                    final boolean dropMetadata = props.getBoolean(DROP_METADATA_ATTRIB, DEFAULT_DROP_METADATA);
                    if (dropMetadata) {
                        admin.disableTable(physicalIndexName);
                        admin.deleteTable(physicalIndexName);
                        clearTableRegionCache(physicalIndexName);
                        wasDeleted = true;
                    }
                }
            } catch (org.apache.hadoop.hbase.TableNotFoundException ignore) {
                // Ignore, as we may never have created a view index table
            }
        } catch (IOException e) {
            throw ServerUtil.parseServerException(e);
        } finally {
            try {
                if (admin != null) admin.close();
            } catch (IOException e) {
                logger.warn("",e);
            }
        }
        return wasDeleted;
    }

    private boolean ensureLocalIndexTableDropped(byte[] physicalTableName, long timestamp) throws SQLException {
        byte[] physicalIndexName = MetaDataUtil.getLocalIndexPhysicalName(physicalTableName);
        HTableDescriptor desc = null;
        HBaseAdmin admin = null;
        boolean wasDeleted = false;
        try {
            admin = new HBaseAdmin(config);
            try {
                desc = admin.getTableDescriptor(physicalIndexName);
                if (Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(desc.getValue(MetaDataUtil.IS_LOCAL_INDEX_TABLE_PROP_BYTES)))) {
                    this.tableStatsCache.invalidate(new ImmutableBytesPtr(physicalIndexName));
                    final ReadOnlyProps props = this.getProps();
                    final boolean dropMetadata = props.getBoolean(DROP_METADATA_ATTRIB, DEFAULT_DROP_METADATA);
                    if (dropMetadata) {
                        admin.disableTable(physicalIndexName);
                        admin.deleteTable(physicalIndexName);
                        clearTableRegionCache(physicalIndexName);
                        wasDeleted = true;
                    }
                }
            } catch (org.apache.hadoop.hbase.TableNotFoundException ignore) {
                // Ignore, as we may never have created a view index table
            }
        } catch (IOException e) {
            throw ServerUtil.parseServerException(e);
        } finally {
            try {
                if (admin != null) admin.close();
            } catch (IOException e) {
                logger.warn("",e);
            }
        }
        return wasDeleted;
    }

    @Override
    public MetaDataMutationResult createTable(final List<Mutation> tableMetaData, byte[] physicalTableName, PTableType tableType,
            Map<String,Object> tableProps, final List<Pair<byte[],Map<String,Object>>> families, byte[][] splits) throws SQLException {
        byte[][] rowKeyMetadata = new byte[3][];
        Mutation m = MetaDataUtil.getPutOnlyTableHeaderRow(tableMetaData);
        byte[] key = m.getRow();
        SchemaUtil.getVarChars(key, rowKeyMetadata);
        byte[] tenantIdBytes = rowKeyMetadata[PhoenixDatabaseMetaData.TENANT_ID_INDEX];
        byte[] schemaBytes = rowKeyMetadata[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
        byte[] tableBytes = rowKeyMetadata[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
        byte[] tableName = physicalTableName != null ? physicalTableName : SchemaUtil.getTableNameAsBytes(schemaBytes, tableBytes);
        boolean localIndexTable = Boolean.TRUE.equals(tableProps.remove(MetaDataUtil.IS_LOCAL_INDEX_TABLE_PROP_NAME));

        if ((tableType == PTableType.VIEW && physicalTableName != null) || (tableType != PTableType.VIEW && physicalTableName == null)) {
            // For views this will ensure that metadata already exists
            // For tables and indexes, this will create the metadata if it doesn't already exist
            ensureTableCreated(tableName, tableType, tableProps, families, splits, true);
        }
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        if (tableType == PTableType.INDEX) { // Index on view
            // Physical index table created up front for multi tenant
            // TODO: if viewIndexId is Short.MIN_VALUE, then we don't need to attempt to create it
            if (physicalTableName != null) {
                if (localIndexTable) {
                    ensureLocalIndexTableCreated(tableName, tableProps, families, splits, MetaDataUtil.getClientTimeStamp(m));
                } else if (!MetaDataUtil.isMultiTenant(m, kvBuilder, ptr)) {
                    ensureViewIndexTableCreated(tenantIdBytes.length == 0 ? null : PNameFactory.newName(tenantIdBytes), physicalTableName, MetaDataUtil.getClientTimeStamp(m));
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
            ensureViewIndexTableCreated(tableName, tableProps, familiesPlusDefault, MetaDataUtil.isSalted(m, kvBuilder, ptr) ? splits : null, MetaDataUtil.getClientTimeStamp(m));
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
                        instance.createTable(controller, builder.build(), rpcCallback);
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
                    builder.setTenantId(HBaseZeroCopyByteString.wrap(tenantIdBytes));
                    builder.setSchemaName(HBaseZeroCopyByteString.wrap(schemaBytes));
                    builder.setTableName(HBaseZeroCopyByteString.wrap(tableBytes));
                    builder.setTableTimestamp(tableTimestamp);
                    builder.setClientTimestamp(clientTimestamp);

                   instance.getTable(controller, builder.build(), rpcCallback);
                   if(controller.getFailedOn() != null) {
                       throw controller.getFailedOn();
                   }
                   return rpcCallback.get();
                }
            });
    }

    @Override
    public MetaDataMutationResult dropTable(final List<Mutation> tableMetaData, final PTableType tableType, final boolean cascade) throws SQLException {
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
            if (dropMetadata) {
                dropTables(result.getTableNamesToDelete());
            }
            invalidateTables(result.getTableNamesToDelete());
            if (tableType == PTableType.TABLE) {
                byte[] physicalName = SchemaUtil.getTableNameAsBytes(schemaBytes, tableBytes);
                long timestamp = MetaDataUtil.getClientTimeStamp(tableMetaData);
                ensureViewIndexTableDropped(physicalName, timestamp);
                ensureLocalIndexTableDropped(physicalName, timestamp);
                tableStatsCache.invalidate(new ImmutableBytesPtr(physicalName));
            }
            break;
        default:
            break;
        }
          return result;
    }

    private void invalidateTables(final List<byte[]> tableNamesToDelete) {
        if (tableNamesToDelete != null) {
            for ( byte[] tableName : tableNamesToDelete ) {
                tableStatsCache.invalidate(new ImmutableBytesPtr(tableName));
            }
        }
    }

    private void dropTables(final List<byte[]> tableNamesToDelete) throws SQLException {
        HBaseAdmin admin = null;
        SQLException sqlE = null;
        try{
            admin = new HBaseAdmin(config);
            if (tableNamesToDelete != null){
                for ( byte[] tableName : tableNamesToDelete ) {
                    if ( admin.tableExists(tableName) ) {
                        admin.disableTable(tableName);
                        admin.deleteTable(tableName);
                        clearTableRegionCache(tableName);
                    }
                }
            }

        } catch (IOException e) {
            sqlE = ServerUtil.parseServerException(e);
        } finally {
            try {
                if (admin != null) {
                    admin.close();
                }
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

    private void ensureViewIndexTableCreated(PName tenantId, byte[] physicalIndexTableName, long timestamp) throws SQLException {
        PTable table;
        String name = Bytes.toString(
                physicalIndexTableName,
                MetaDataUtil.VIEW_INDEX_TABLE_PREFIX_BYTES.length,
                physicalIndexTableName.length-MetaDataUtil.VIEW_INDEX_TABLE_PREFIX_BYTES.length);
        try {
            PMetaData metadata = latestMetaData;
            if (metadata == null) {
                throwConnectionClosedException();
            }
            table = metadata.getTable(new PTableKey(tenantId, name));
            if (table.getTimeStamp() >= timestamp) { // Table in cache is newer than client timestamp which shouldn't be the case
                throw new TableNotFoundException(table.getSchemaName().getString(), table.getTableName().getString());
            }
        } catch (TableNotFoundException e) {
            byte[] schemaName = Bytes.toBytes(SchemaUtil.getSchemaNameFromFullName(name));
            byte[] tableName = Bytes.toBytes(SchemaUtil.getTableNameFromFullName(name));
            MetaDataMutationResult result = this.getTable(null, schemaName, tableName, HConstants.LATEST_TIMESTAMP, timestamp);
            table = result.getTable();
            if (table == null) {
                throw e;
            }
        }
        ensureViewIndexTableCreated(table, timestamp);
    }

    private void ensureViewIndexTableCreated(PTable table, long timestamp) throws SQLException {
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

        ensureViewIndexTableCreated(physicalTableName, tableProps, families, splits, timestamp);
    }
    
    @Override
    public MetaDataMutationResult addColumn(final List<Mutation> tableMetaData, PTable table, Map<String, List<Pair<String,Object>>> stmtProperties, Set<String> colFamiliesForPColumnsToBeAdded) throws SQLException {
        List<Pair<byte[], Map<String, Object>>> families = new ArrayList<>(stmtProperties.size());
        Map<String, Object> tableProps = new HashMap<String, Object>();
        HTableDescriptor tableDescriptor = separateAndValidateProperties(table, stmtProperties, colFamiliesForPColumnsToBeAdded, families, tableProps);
        SQLException sqlE = null;
        if (tableDescriptor != null) {
            try {
                boolean pollingNotNeeded = (!tableProps.isEmpty() && families.isEmpty() && colFamiliesForPColumnsToBeAdded.isEmpty());
                modifyTable(table.getPhysicalName().getBytes(), tableDescriptor, !pollingNotNeeded);
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
        MetaDataMutationResult result =  metaDataCoprocessorExec(tableKey,
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

                    instance.addColumn(controller, builder.build(), rpcCallback);
                    if(controller.getFailedOn() != null) {
                        throw controller.getFailedOn();
                    }
                    return rpcCallback.get();
                }
            });

        if (result.getMutationCode() == MutationCode.COLUMN_NOT_FOUND) { // Success
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
                        this.ensureViewIndexTableCreated(table, timestamp);
                    } else {
                        this.ensureViewIndexTableDropped(table.getPhysicalName().getBytes(), timestamp);
                    }
                }
            }
        }
        return result;
    }
    
    private HTableDescriptor separateAndValidateProperties(PTable table, Map<String, List<Pair<String, Object>>> properties, Set<String> colFamiliesForPColumnsToBeAdded, List<Pair<byte[], Map<String, Object>>> families, Map<String, Object> tableProps) throws SQLException {
        Map<String, Map<String, Object>> stmtFamiliesPropsMap = new HashMap<>(properties.size());
        Map<String,Object> commonFamilyProps = new HashMap<>();
        boolean addingColumns = colFamiliesForPColumnsToBeAdded != null && colFamiliesForPColumnsToBeAdded.size() > 0;
        HashSet<String> existingColumnFamilies = existingColumnFamilies(table);
        Map<String, Map<String, Object>> allFamiliesProps = new HashMap<>(existingColumnFamilies.size());
        for (String family : properties.keySet()) {
            List<Pair<String, Object>> propsList = properties.get(family);
            if (propsList != null && propsList.size() > 0) {
                Map<String, Object> colFamilyPropsMap = new HashMap<String, Object>(propsList.size());
                for (Pair<String, Object> prop : propsList) {
                    String propName = prop.getFirst();
                    Object propValue = prop.getSecond();
                    if ((isHTableProperty(propName) ||  TableProperty.isPhoenixTableProperty(propName)) && addingColumns) {
                        // setting HTable and PhoenixTable properties while adding a column is not allowed.
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_SET_TABLE_PROPERTY_ADD_COLUMN)
                        .setMessage("Property: " + propName).build()
                        .buildException();
                    }
                    if (isHTableProperty(propName)) {
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
                                // Even though TTL is really a HColumnProperty we treat it specially.
                                // We enforce that all column families have the same TTL.
                                commonFamilyProps.put(propName, prop.getSecond());
                            }
                        } else {
                            if (isHColumnProperty(propName)) {
                                if (family.equals(QueryConstants.ALL_FAMILY_PROPERTIES_KEY)) {
                                    commonFamilyProps.put(propName, prop.getSecond());
                                } else {
                                    colFamilyPropsMap.put(propName, prop.getSecond());
                                }
                            } else {
                                // invalid property - neither of HTableProp, HColumnProp or PhoenixTableProp
                                // FIXME: This isn't getting triggered as currently a property gets evaluated 
                                // as HTableProp if its neither HColumnProp or PhoenixTableProp.
                                throw new SQLExceptionInfo.Builder(SQLExceptionCode.SET_UNSUPPORTED_PROP_ON_ALTER_TABLE)
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
        if (!allFamiliesProps.isEmpty() || !tableProps.isEmpty()) {
            byte[] tableNameBytes = Bytes.toBytes(table.getPhysicalName().getString());
            HTableDescriptor existingTableDescriptor = getTableDescriptor(tableNameBytes);
            newTableDescriptor = new HTableDescriptor(existingTableDescriptor);
            if (!tableProps.isEmpty()) {
                // add all the table properties to the existing table descriptor
                for (Entry<String, Object> entry : tableProps.entrySet()) {
                    newTableDescriptor.setValue(entry.getKey(), entry.getValue() != null ? entry.getValue().toString() : null);
                }
            }
            if (addingColumns) {
                // Make sure that all the CFs of the table have the same TTL as the empty CF. 
                setTTLToEmptyCFTTL(allFamiliesProps, table, newTableDescriptor);
            }
            for (Entry<String, Map<String, Object>> entry : allFamiliesProps.entrySet()) {
                byte[] cf = entry.getKey().getBytes();
                HColumnDescriptor colDescriptor = newTableDescriptor.getFamily(cf);
                if (colDescriptor == null) {
                    // new column family
                    colDescriptor = generateColumnFamilyDescriptor(new Pair<>(cf, entry.getValue()), table.getType());
                    newTableDescriptor.addFamily(colDescriptor);
                } else {
                    modifyColumnFamilyDescriptor(colDescriptor, entry.getValue());
                }
            }
        }
        return newTableDescriptor;
    }
    
    private boolean isHColumnProperty(String propName) {
        return HColumnDescriptor.getDefaultValues().containsKey(propName);
    }

    private boolean isHTableProperty(String propName) {
        return !isHColumnProperty(propName) && !TableProperty.isPhoenixTableProperty(propName);
    } 
    
    private HashSet<String> existingColumnFamilies(PTable table) {
        List<PColumnFamily> cfs = table.getColumnFamilies();
        HashSet<String> cfNames = new HashSet<>(cfs.size());
        for (PColumnFamily cf : table.getColumnFamilies()) {
            cfNames.add(cf.getName().getString());
        }
        return cfNames;
    }

    private int getTTLForEmptyCf(byte[] emptyCf, byte[] tableNameBytes, HTableDescriptor tableDescriptor) throws SQLException {
        if (tableDescriptor == null) {
            tableDescriptor = getTableDescriptor(tableNameBytes);
        }
        return tableDescriptor.getFamily(emptyCf).getTimeToLive();
    }
    
    private void setTTLToEmptyCFTTL(Map<String, Map<String, Object>> familyProps, PTable table, 
            HTableDescriptor tableDesc) throws SQLException {
        if (!familyProps.isEmpty()) {
            int emptyCFTTL = getTTLForEmptyCf(SchemaUtil.getEmptyColumnFamily(table), table.getPhysicalName().getBytes(), tableDesc);
            for (String family : familyProps.keySet()) {
                Map<String, Object> props = familyProps.get(family) != null ? familyProps.get(family) : new HashMap<String, Object>();
                props.put(TTL, emptyCFTTL);
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
            }
            invalidateTables(result.getTableNamesToDelete());
            break;
        default:
            break;
        }
        return result;

    }

    /** 
     * Keeping this to use for further upgrades. This method closes the oldMetaConnection.
     */
    private PhoenixConnection addColumnsIfNotExists(PhoenixConnection oldMetaConnection,
        String tableName, long timestamp, String columns) throws SQLException {

        Properties props = new Properties(oldMetaConnection.getClientInfo());
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(timestamp));
        // Cannot go through DriverManager or you end up in an infinite loop because it'll call init again
        PhoenixConnection metaConnection = new PhoenixConnection(this, oldMetaConnection.getURL(), props, oldMetaConnection.getMetaDataCache());
        SQLException sqlE = null;
        try {
            metaConnection.createStatement().executeUpdate("ALTER TABLE " + tableName + " ADD IF NOT EXISTS " + columns );
        } catch (SQLException e) {
            logger.warn("addColumnsIfNotExists failed due to:" + e);
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
                        PhoenixConnection metaConnection = null;
                        try {
                            openConnection();
                            Properties scnProps = PropertiesUtil.deepCopy(props);
                            scnProps.setProperty(
                                    PhoenixRuntime.CURRENT_SCN_ATTRIB,
                                    Long.toString(MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP));
                            scnProps.remove(PhoenixRuntime.TENANT_ID_ATTRIB);
                            String globalUrl = JDBCUtil.removeProperty(url, PhoenixRuntime.TENANT_ID_ATTRIB);
                            metaConnection = new PhoenixConnection(
                                    ConnectionQueryServicesImpl.this, globalUrl, scnProps, newEmptyMetaData());
                            try {
                                metaConnection.createStatement().executeUpdate(QueryConstants.CREATE_TABLE_METADATA);
                            } catch (NewerTableAlreadyExistsException ignore) {
                                // Ignore, as this will happen if the SYSTEM.CATALOG already exists at this fixed timestamp.
                                // A TableAlreadyExistsException is not thrown, since the table only exists *after* this fixed timestamp.
                            } catch (TableAlreadyExistsException e) {
                                // This will occur if we have an older SYSTEM.CATALOG and we need to update it to include
                                // any new columns we've added.
                                long currentServerSideTableTimeStamp = e.getTable().getTimeStamp();

                                String columnsToAdd = "";
                                if(currentServerSideTableTimeStamp < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_3_0) {
                                    // We know that we always need to add the STORE_NULLS column for 4.3 release
                                    columnsToAdd = PhoenixDatabaseMetaData.STORE_NULLS + " " + PBoolean.INSTANCE.getSqlTypeName();
                                    HBaseAdmin admin = null;
                                    try {
                                        admin = getAdmin();
                                        HTableDescriptor[] localIndexTables = admin.listTables(MetaDataUtil.LOCAL_INDEX_TABLE_PREFIX+".*");
                                        for (HTableDescriptor table : localIndexTables) {
                                            if (table.getValue(MetaDataUtil.PARENT_TABLE_KEY) == null
                                                    && table.getValue(MetaDataUtil.IS_LOCAL_INDEX_TABLE_PROP_NAME) != null) {
                                                table.setValue(MetaDataUtil.PARENT_TABLE_KEY,
                                                    MetaDataUtil.getUserTableName(table
                                                        .getNameAsString()));
                                                // Explicitly disable, modify and enable the table to ensure co-location of data
                                                // and index regions. If we just modify the table descriptor when online schema
                                                // change enabled may reopen the region in same region server instead of following data region.
                                                admin.disableTable(table.getTableName());
                                                admin.modifyTable(table.getTableName(), table);
                                                admin.enableTable(table.getTableName());
                                            }
                                        }
                                    } finally {
                                        if (admin != null) admin.close();
                                    }
                                }

                                // If the server side schema is at before MIN_SYSTEM_TABLE_TIMESTAMP_4_1_0 then 
                                // we need to add INDEX_TYPE and INDEX_DISABLE_TIMESTAMP columns too.
                                // TODO: Once https://issues.apache.org/jira/browse/PHOENIX-1614 is fixed,
                                // we should just have a ALTER TABLE ADD IF NOT EXISTS statement with all
                                // the column names that have been added to SYSTEM.CATALOG since 4.0.
                                if (currentServerSideTableTimeStamp < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_1_0) {
                                    columnsToAdd += ", " + PhoenixDatabaseMetaData.INDEX_TYPE + " " + PUnsignedTinyint.INSTANCE.getSqlTypeName()
                                            + ", " + PhoenixDatabaseMetaData.INDEX_DISABLE_TIMESTAMP + " " + PLong.INSTANCE.getSqlTypeName();
                                }

                                // Ugh..need to assign to another local variable to keep eclipse happy.
                                PhoenixConnection newMetaConnection = addColumnsIfNotExists(metaConnection,
                                        PhoenixDatabaseMetaData.SYSTEM_CATALOG,
                                        MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP, columnsToAdd);
                                metaConnection = newMetaConnection;
                            }
                            int nSaltBuckets = ConnectionQueryServicesImpl.this.props.getInt(QueryServices.SEQUENCE_SALT_BUCKETS_ATTRIB,
                                    QueryServicesOptions.DEFAULT_SEQUENCE_TABLE_SALT_BUCKETS);
                            try {
                                String createSequenceTable = Sequence.getCreateTableStatement(nSaltBuckets);
                                metaConnection.createStatement().executeUpdate(createSequenceTable);
                                nSequenceSaltBuckets = nSaltBuckets;
                            } catch (NewerTableAlreadyExistsException e) {
                                // Ignore, as this will happen if the SYSTEM.SEQUENCE already exists at this fixed timestamp.
                                // A TableAlreadyExistsException is not thrown, since the table only exists *after* this fixed timestamp.
                                nSequenceSaltBuckets = getSaltBuckets(e);
                            } catch (TableAlreadyExistsException e) {
                                // This will occur if we have an older SYSTEM.SEQUENCE and we need to update it to include
                                // any new columns we've added.
                                long currentServerSideTableTimeStamp = e.getTable().getTimeStamp();
                                if (currentServerSideTableTimeStamp < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_1_0) {
                                    // If the table time stamp is before 4.1.0 then we need to add below columns
                                    // to the SYSTEM.SEQUENCE table.
                                    String columnsToAdd = PhoenixDatabaseMetaData.MIN_VALUE + " " + PLong.INSTANCE.getSqlTypeName() 
                                            + ", " + PhoenixDatabaseMetaData.MAX_VALUE + " " + PLong.INSTANCE.getSqlTypeName()
                                            + ", " + PhoenixDatabaseMetaData.CYCLE_FLAG + " " + PBoolean.INSTANCE.getSqlTypeName()
                                            + ", " + PhoenixDatabaseMetaData.LIMIT_REACHED_FLAG + " " + PBoolean.INSTANCE.getSqlTypeName();
                                    addColumnsIfNotExists(metaConnection, PhoenixDatabaseMetaData.SYSTEM_CATALOG,
                                            MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP, columnsToAdd);
                                }
                                // If the table timestamp is before 4.2.1 then run the upgrade script
                                if (currentServerSideTableTimeStamp < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_2_1) {
                                    if (UpgradeUtil.upgradeSequenceTable(metaConnection, nSaltBuckets, e.getTable())) {
                                        metaConnection.removeTable(null,
                                                PhoenixDatabaseMetaData.SEQUENCE_SCHEMA_NAME,
                                                PhoenixDatabaseMetaData.SEQUENCE_TABLE_NAME,
                                                MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP);
                                        clearTableFromCache(ByteUtil.EMPTY_BYTE_ARRAY,
                                                PhoenixDatabaseMetaData.SEQUENCE_SCHEMA_NAME_BYTES,
                                                PhoenixDatabaseMetaData.SEQUENCE_TABLE_NAME_BYTES,
                                                MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP);
                                        clearTableRegionCache(PhoenixDatabaseMetaData.SEQUENCE_FULLNAME_BYTES);
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
                            } catch(TableAlreadyExistsException ignore) {
                                metaConnection = addColumnsIfNotExists(
                                        metaConnection,
                                        PhoenixDatabaseMetaData.SYSTEM_STATS_NAME,
                                        MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP,
                                        PhoenixDatabaseMetaData.GUIDE_POSTS_ROW_COUNT + " "
                                                + PLong.INSTANCE.getSqlTypeName());
                            }
                        } catch (Exception e) {
                            if (e instanceof SQLException) {
                                initializationException = (SQLException)e;
                            } else {
                                // wrap every other exception into a SQLException
                                initializationException = new SQLException(e);
                            }
                        } finally {
                            try {
                                if (metaConnection != null) metaConnection.close();
                            } catch (SQLException e) {
                                if (initializationException != null) {
                                    initializationException.setNextException(e);
                                } else {
                                    initializationException = e;
                                }
                            } finally {
                                try {
                                    if (initializationException != null) {
                                        throw initializationException;
                                    }
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
            throw Throwables.propagate(e);
        }
    }
    
    private static int getSaltBuckets(TableAlreadyExistsException e) {
        PTable table = e.getTable();
        Integer sequenceSaltBuckets = table == null ? null : table.getBucketNum();
        return sequenceSaltBuckets == null ? 0 : sequenceSaltBuckets;
    }
    
    @Override
    public MutationState updateData(MutationPlan plan) throws SQLException {
        return plan.execute();
    }

    @Override
    public int getLowestClusterHBaseVersion() {
        return lowestClusterHBaseVersion;
    }

    @Override
    public boolean hasInvalidIndexConfiguration() {
        return hasInvalidIndexConfiguration;
    }

    /**
     * Clears the Phoenix meta data cache on each region server
     * @throws SQLException
     */
    @Override
    public void clearCache() throws SQLException {
        try {
            SQLException sqlE = null;
            HTableInterface htable = this.getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            try {
                htable.coprocessorService(MetaDataService.class, HConstants.EMPTY_START_ROW,
                        HConstants.EMPTY_END_ROW, new Batch.Call<MetaDataService, ClearCacheResponse>() {
                    @Override
                    public ClearCacheResponse call(MetaDataService instance) throws IOException {
                        ServerRpcController controller = new ServerRpcController();
                        BlockingRpcCallback<ClearCacheResponse> rpcCallback =
                                new BlockingRpcCallback<ClearCacheResponse>();
                        ClearCacheRequest.Builder builder = ClearCacheRequest.newBuilder();
                        instance.clearCache(controller, builder.build(), rpcCallback);
                        if(controller.getFailedOn() != null) {
                            throw controller.getFailedOn();
                        }
                        return rpcCallback.get();
                    }
                  });
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
            return new HBaseAdmin(config);
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
            HTableInterface htable =
                    this.getTable(PhoenixDatabaseMetaData.SEQUENCE_FULLNAME_BYTES);
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
            HTableInterface htable = this.getTable(PhoenixDatabaseMetaData.SEQUENCE_FULLNAME_BYTES);
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
    public void validateSequences(List<SequenceKey> sequenceKeys, long timestamp, long[] values, SQLException[] exceptions, Sequence.ValueOp action) throws SQLException {
        incrementSequenceValues(sequenceKeys, timestamp, values, exceptions, action);
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
    public void incrementSequences(List<SequenceKey> sequenceKeys, long timestamp, long[] values, SQLException[] exceptions) throws SQLException {
        incrementSequenceValues(sequenceKeys, timestamp, values, exceptions, Sequence.ValueOp.INCREMENT_SEQUENCE);
    }

    @SuppressWarnings("deprecation")
    private void incrementSequenceValues(List<SequenceKey> keys, long timestamp, long[] values, SQLException[] exceptions, Sequence.ValueOp op) throws SQLException {
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
            // Now that we have all the locks we need, increment the sequences
            List<Increment> incrementBatch = Lists.newArrayListWithExpectedSize(sequences.size());
            List<Sequence> toIncrementList = Lists.newArrayListWithExpectedSize(sequences.size());
            int[] indexes = new int[sequences.size()];
            for (int i = 0; i < sequences.size(); i++) {
                Sequence sequence = sequences.get(i);
                try {
                    values[i] = sequence.incrementValue(timestamp, op);
                } catch (EmptySequenceCacheException e) {
                    indexes[toIncrementList.size()] = i;
                    toIncrementList.add(sequence);
                    Increment inc = sequence.newIncrement(timestamp, op);
                    incrementBatch.add(inc);
                } catch (SQLException e) {
                    exceptions[i] = e;
                }
            }
            if (toIncrementList.isEmpty()) {
                return;
            }
            HTableInterface hTable = this.getTable(PhoenixDatabaseMetaData.SEQUENCE_FULLNAME_BYTES);
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
                    values[indexes[i]] = sequence.incrementValue(result, op);
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
            HTableInterface htable = this.getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            try {
                htable.coprocessorService(MetaDataService.class, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW,
                        new Batch.Call<MetaDataService, ClearTableFromCacheResponse>() {
                            @Override
                            public ClearTableFromCacheResponse call(MetaDataService instance) throws IOException {
                                ServerRpcController controller = new ServerRpcController();
                                BlockingRpcCallback<ClearTableFromCacheResponse> rpcCallback = new BlockingRpcCallback<ClearTableFromCacheResponse>();
                                ClearTableFromCacheRequest.Builder builder = ClearTableFromCacheRequest.newBuilder();
                                builder.setTenantId(HBaseZeroCopyByteString.wrap(tenantId));
                                builder.setTableName(HBaseZeroCopyByteString.wrap(tableName));
                                builder.setSchemaName(HBaseZeroCopyByteString.wrap(schemaName));
                                builder.setClientTimestamp(clientTS);
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
                    if (tenantId.length == 0) tableStatsCache.invalidate(new ImmutableBytesPtr(SchemaUtil.getTableNameAsBytes(schemaName, tableName)));
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
            HTableInterface hTable = this.getTable(PhoenixDatabaseMetaData.SEQUENCE_FULLNAME_BYTES);
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
        HTableInterface hTable = this.getTable(PhoenixDatabaseMetaData.SEQUENCE_FULLNAME_BYTES);
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
        synchronized (connectionCountLock) {
            connectionCount++;
        }
    }

    @Override
    public void removeConnection(PhoenixConnection connection) throws SQLException {
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
    public PTableStats getTableStats(final byte[] physicalName, final long clientTimeStamp) throws SQLException {
        try {
            return tableStatsCache.get(new ImmutableBytesPtr(physicalName), new Callable<PTableStats>() {
                @Override
                public PTableStats call() throws Exception {
                    /*
                     *  The shared view index case is tricky, because we don't have
                     *  table metadata for it, only an HBase table. We do have stats,
                     *  though, so we'll query them directly here and cache them so
                     *  we don't keep querying for them.
                     */
                    HTableInterface statsHTable = ConnectionQueryServicesImpl.this.getTable(PhoenixDatabaseMetaData.SYSTEM_STATS_NAME_BYTES);
                    try {
                        return StatisticsUtil.readStatistics(statsHTable, physicalName, clientTimeStamp);
                    } catch (IOException e) {
                        logger.warn("Unable to read from stats table", e);
                        // Just cache empty stats. We'll try again after some time anyway.
                        return PTableStats.EMPTY_STATS;
                    } finally {
                        try {
                            statsHTable.close();
                        } catch (IOException e) {
                            // Log, but continue. We have our stats anyway now.
                            logger.warn("Unable to close stats table", e);
                        }
                    }
                }

            });
        } catch (ExecutionException e) {
            throw ServerUtil.parseServerException(e);
        }
    }

    @Override
    public int getSequenceSaltBuckets() {
        return nSequenceSaltBuckets;
    }
}
