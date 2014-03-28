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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_COUNT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_FAMILY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_SIZE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TABLE_NAME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DECIMAL_DIGITS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DEFAULT_COLUMN_FAMILY_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IMMUTABLE_ROWS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_STATE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.KEY_SEQ;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LINK_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.NULLABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ORDINAL_POSITION;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PK_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SALT_BUCKETS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SORT_ORDER;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_CAT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SEQ_NUM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_TYPE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TENANT_ID;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_DROP_METADATA;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
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
import org.apache.phoenix.coprocessor.ScanRegionObserver;
import org.apache.phoenix.coprocessor.SequenceRegionObserver;
import org.apache.phoenix.coprocessor.ServerCachingEndpointImpl;
import org.apache.phoenix.coprocessor.UngroupedAggregateRegionObserver;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.AddColumnRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.ClearCacheRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.ClearCacheResponse;
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
import org.apache.phoenix.hbase.index.Indexer;
import org.apache.phoenix.hbase.index.covered.CoveredColumnsIndexBuilder;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.index.PhoenixIndexBuilder;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver.ConnectionInfo;
import org.apache.phoenix.protobuf.ProtobufUtil;
import org.apache.phoenix.schema.EmptySequenceCacheException;
import org.apache.phoenix.schema.MetaDataSplitPolicy;
import org.apache.phoenix.schema.NewerTableAlreadyExistsException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PMetaData;
import org.apache.phoenix.schema.PMetaDataImpl;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.LinkType;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.ReadOnlyTableException;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.schema.Sequence;
import org.apache.phoenix.schema.SequenceKey;
import org.apache.phoenix.schema.TableAlreadyExistsException;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.ConfigUtil;
import org.apache.phoenix.util.JDBCUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.google.protobuf.HBaseZeroCopyByteString;

public class ConnectionQueryServicesImpl extends DelegateQueryServices implements ConnectionQueryServices {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionQueryServicesImpl.class);
    private static final int INITIAL_CHILD_SERVICES_CAPACITY = 100;
    private static final int DEFAULT_OUT_OF_ORDER_MUTATIONS_WAIT_TIME_MS = 1000;
    private static final String OLD_DEFAULT_COLUMN_FAMILY = "_0";
    private static final byte[] OLD_DEFAULT_COLUMN_FAMILY_BYTES = Bytes.toBytes(OLD_DEFAULT_COLUMN_FAMILY);
    private static final String OLD_SYSTEM_SCHEMA_NAME = "SYSTEM";
    private static final String OLD_SYSTEM_TABLE_NAME = "TABLE";
    private static final byte[] OLD_SYSTEM_TABLE_NAME_BYTES = SchemaUtil.getTableNameAsBytes("SYSTEM", "TABLE");
    // Don't use SYSTEM as the schema name, otherwise we'll treat it as a system table
    private static final String OLD_SYSTEM_TABLE_AS_VIEW_NAME = SchemaUtil.getTableName("META", "\"TABLE\"");
    
    public static final String UPGRADE_TO_3_0 = "UpgradeTo30";
    public static final String UPGRADE_TO_2_2 = "UpgradeTo22";
    
    protected final Configuration config;
    // Copy of config.getProps(), but read-only to prevent synchronization that we
    // don't need.
    private final ReadOnlyProps props;
    private final ConcurrentHashMap<ImmutableBytesWritable,ConnectionQueryServices> childServices;
    private final StatsManager statsManager;
    // Cache the latest meta data here for future connections
    private volatile PMetaData latestMetaData;
    private final Object latestMetaDataLock = new Object();
    // Lowest HBase version on the cluster.
    private int lowestClusterHBaseVersion = Integer.MAX_VALUE;
    private boolean hasInvalidIndexConfiguration = false;
    private int connectionCount = 0;
    private WhiteList upgradeWhiteList = null;

    private HConnection connection;
    private volatile boolean initialized;
    private volatile boolean closed;
    private volatile SQLException initializationException;
    private ConcurrentMap<SequenceKey,Sequence> sequenceMap = Maps.newConcurrentMap();
    private KeyValueBuilder kvBuilder;

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
     * @throws SQLException
     */
    public ConnectionQueryServicesImpl(QueryServices services, ConnectionInfo connectionInfo) {
        super(services);
        Configuration config = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
        for (Entry<String,String> entry : services.getProps()) {
            config.set(entry.getKey(), entry.getValue());
        }
        for (Entry<String,String> entry : connectionInfo.asProps()) {
            config.set(entry.getKey(), entry.getValue());
        }
        // Without making a copy of the configuration we cons up, we lose some of our properties
        // on the server side during testing.
        this.config = HBaseConfiguration.create(config);
        // set replication required parameter
        ConfigUtil.setReplicationConfigIfAbsent(this.config);
        this.props = new ReadOnlyProps(this.config.iterator());
        this.latestMetaData = newEmptyMetaData();
        // TODO: should we track connection wide memory usage or just org-wide usage?
        // If connection-wide, create a MemoryManager here, otherwise just use the one from the delegate
        this.childServices = new ConcurrentHashMap<ImmutableBytesWritable,ConnectionQueryServices>(INITIAL_CHILD_SERVICES_CAPACITY);
        int statsUpdateFrequencyMs = this.getProps().getInt(QueryServices.STATS_UPDATE_FREQ_MS_ATTRIB, QueryServicesOptions.DEFAULT_STATS_UPDATE_FREQ_MS);
        int maxStatsAgeMs = this.getProps().getInt(QueryServices.MAX_STATS_AGE_MS_ATTRIB, QueryServicesOptions.DEFAULT_MAX_STATS_AGE_MS);
        this.statsManager = new StatsManagerImpl(this, statsUpdateFrequencyMs, maxStatsAgeMs);
        
        // find the HBase version and use that to determine the KeyValueBuilder that should be used
        String hbaseVersion = VersionInfo.getVersion();
        this.kvBuilder = KeyValueBuilder.get(hbaseVersion);
    }
    
    private void openConnection() throws SQLException {
        try {
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
    public StatsManager getStatsManager() {
        return this.statsManager;
    }
    
    @Override
    public HTableInterface getTable(byte[] tableName) throws SQLException {
        try {
            return HBaseFactoryProvider.getHTableFactory().getTable(tableName, connection, getExecutor());
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
                    // Clear any client-side caches.  
                    statsManager.clearStats();
                } catch (SQLException e) {
                    if (sqlE == null) {
                        sqlE = e;
                    } else {
                        sqlE.setNextException(e);
                    }
                } finally {
                    try {
                        childServices.clear();
                        latestMetaData = null;
                        if (connection != null) connection.close();
                    } catch (IOException e) {
                        if (sqlE == null) {
                            sqlE = ServerUtil.parseServerException(e);
                        } else {
                            sqlE.setNextException(ServerUtil.parseServerException(e));
                        }
                    } finally {
                        try {
                            super.close();
                        } catch (SQLException e) {
                            if (sqlE == null) {
                                sqlE = e;
                            } else {
                                sqlE.setNextException(e);
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

    @SuppressWarnings("deprecation")
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
                  @SuppressWarnings("deprecation")
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
        try {
            // If existing table isn't older than new table, don't replace
            // If a client opens a connection at an earlier timestamp, this can happen
            PTable existingTable = latestMetaData.getTable(new PTableKey(table.getTenantId(), table.getName().getString()));
            if (existingTable.getTimeStamp() >= table.getTimeStamp()) {
                return latestMetaData;
            }
        } catch (TableNotFoundException e) {
        }
        synchronized(latestMetaDataLock) {
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
     * @param tenantId TODO
     */
    private PMetaData metaDataMutated(PName tenantId, String tableName, long tableSeqNum, Mutator mutator) throws SQLException {
        synchronized(latestMetaDataLock) {
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
                        metaData = metaData.removeTable(tenantId, tableName);
                        break;
                    }
                    latestMetaDataLock.wait(waitTime);
                } catch (InterruptedException e) {
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
    public PMetaData addColumn(final PName tenantId, final String tableName, final List<PColumn> columns, final long tableTimeStamp, final long tableSeqNum, final boolean isImmutableRows) throws SQLException {
        return metaDataMutated(tenantId, tableName, tableSeqNum, new Mutator() {
            @Override
            public PMetaData mutate(PMetaData metaData) throws SQLException {
                try {
                    return metaData.addColumn(tenantId, tableName, columns, tableTimeStamp, tableSeqNum, isImmutableRows);
                } catch (TableNotFoundException e) {
                    // The DROP TABLE may have been processed first, so just ignore.
                    return metaData;
                }
            }
        });
     }

    @Override
    public PMetaData removeTable(PName tenantId, final String tableName) throws SQLException {
        synchronized(latestMetaDataLock) {
            latestMetaData = latestMetaData.removeTable(tenantId, tableName);
            latestMetaDataLock.notifyAll();
            return latestMetaData;
        }
    }

    @Override
    public PMetaData removeColumn(final PName tenantId, final String tableName, final String familyName, final String columnName, final long tableTimeStamp, final long tableSeqNum) throws SQLException {
        return metaDataMutated(tenantId, tableName, tableSeqNum, new Mutator() {
            @Override
            public PMetaData mutate(PMetaData metaData) throws SQLException {
                try {
                    return metaData.removeColumn(tenantId, tableName, familyName, columnName, tableTimeStamp, tableSeqNum);
                } catch (TableNotFoundException e) {
                    // The DROP TABLE may have been processed first, so just ignore.
                    return metaData;
                }
            }
        });
    }


    @Override
    public PhoenixConnection connect(String url, Properties info) throws SQLException {
        return new PhoenixConnection(this, url, info, latestMetaData);
    }


    private HColumnDescriptor generateColumnFamilyDescriptor(Pair<byte[],Map<String,Object>> family, PTableType tableType) throws SQLException {
        HColumnDescriptor columnDesc = new HColumnDescriptor(family.getFirst());
        if (tableType != PTableType.VIEW) {
            columnDesc.setKeepDeletedCells(true);
            columnDesc.setDataBlockEncoding(SchemaUtil.DEFAULT_DATA_BLOCK_ENCODING);
            for (Entry<String,Object> entry : family.getSecond().entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                columnDesc.setValue(key, value == null ? null : value.toString());
            }
        }
        return columnDesc;
    }

    private void modifyColumnFamilyDescriptor(HColumnDescriptor hcd, Pair<byte[],Map<String,Object>> family) throws SQLException {
      for (Entry<String, Object> entry : family.getSecond().entrySet()) {
        String key = entry.getKey();
        Object value = entry.getValue();
        hcd.setValue(key, value == null ? null : value.toString());
      }
    }
    
    private static final String OLD_PACKAGE = "com.salesforce.";
    private static final String NEW_PACKAGE = "org.apache.";
    private static final String OLD_INDEXER_CLASS_NAME = "com.salesforce.hbase.index.Indexer";
    
    private HTableDescriptor generateTableDescriptor(byte[] tableName, HTableDescriptor existingDesc, PTableType tableType, Map<String,Object> tableProps, List<Pair<byte[],Map<String,Object>>> families, byte[][] splits) throws SQLException {
        String defaultFamilyName = (String)tableProps.remove(PhoenixDatabaseMetaData.DEFAULT_COLUMN_FAMILY_NAME);                
        HTableDescriptor descriptor = (existingDesc != null) ? new HTableDescriptor(existingDesc) : 
          new HTableDescriptor(TableName.valueOf(tableName));
        for (Entry<String,Object> entry : tableProps.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            descriptor.setValue(key, value == null ? null : value.toString());
        }
        if (families.isEmpty()) {
            if (tableType != PTableType.VIEW) {
                byte[] defaultFamilyByes = defaultFamilyName == null ? QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES : Bytes.toBytes(defaultFamilyName);
                // Add dummy column family so we have key values for tables that 
                HColumnDescriptor columnDescriptor = generateColumnFamilyDescriptor(new Pair<byte[],Map<String,Object>>(defaultFamilyByes,Collections.<String,Object>emptyMap()), tableType);
                descriptor.addFamily(columnDescriptor);
            }
        } else {
            for (Pair<byte[],Map<String,Object>> family : families) {
                // If family is only in phoenix description, add it. otherwise, modify its property accordingly.
                byte[] familyByte = family.getFirst();
                if (descriptor.getFamily(familyByte) == null) {
                    if (tableType == PTableType.VIEW) {
                        String fullTableName = Bytes.toString(tableName);
                        throw new ReadOnlyTableException(
                                "The HBase column families for a read-only table must already exist",
                                SchemaUtil.getSchemaNameFromFullName(fullTableName),
                                SchemaUtil.getTableNameFromFullName(fullTableName),
                                Bytes.toString(familyByte));
                    }
                    HColumnDescriptor columnDescriptor = generateColumnFamilyDescriptor(family, tableType);
                    descriptor.addFamily(columnDescriptor);
                } else {
                    if (tableType != PTableType.VIEW) {
                        modifyColumnFamilyDescriptor(descriptor.getFamily(familyByte), family);
                    }
                }
            }
        }
        addCoprocessors(tableName, descriptor, tableType);
        return descriptor;
    }

    private void addCoprocessors(byte[] tableName, HTableDescriptor descriptor, PTableType tableType) throws SQLException {
        // The phoenix jar must be available on HBase classpath
        try {
            // Hack to detect that we're installing view on old SYSTEM.TABLE for conversion.
            // In this case, we do not want to uninstall the old coprocessors.
            boolean isOldSysTable = Bytes.compareTo(OLD_SYSTEM_TABLE_NAME_BYTES,tableName) == 0;
            if (!descriptor.hasCoprocessor(ScanRegionObserver.class.getName())) {
                if (!isOldSysTable) {
                    descriptor.removeCoprocessor(ScanRegionObserver.class.getName().replace(NEW_PACKAGE, OLD_PACKAGE));
                }
                descriptor.addCoprocessor(ScanRegionObserver.class.getName(), null, 1, null);
            }
            if (!descriptor.hasCoprocessor(UngroupedAggregateRegionObserver.class.getName())) {
                if (!isOldSysTable) {
                    descriptor.removeCoprocessor(UngroupedAggregateRegionObserver.class.getName().replace(NEW_PACKAGE, OLD_PACKAGE));
                }
                descriptor.addCoprocessor(UngroupedAggregateRegionObserver.class.getName(), null, 1, null);
            }
            if (!descriptor.hasCoprocessor(GroupedAggregateRegionObserver.class.getName())) {
                if (!isOldSysTable) {
                    descriptor.removeCoprocessor(GroupedAggregateRegionObserver.class.getName().replace(NEW_PACKAGE, OLD_PACKAGE));
                }
                descriptor.addCoprocessor(GroupedAggregateRegionObserver.class.getName(), null, 1, null);
            }
            if (!descriptor.hasCoprocessor(ServerCachingEndpointImpl.class.getName())) {
                if (!isOldSysTable) {
                    descriptor.removeCoprocessor(ServerCachingEndpointImpl.class.getName().replace(NEW_PACKAGE, OLD_PACKAGE));
                }
                descriptor.addCoprocessor(ServerCachingEndpointImpl.class.getName(), null, 1, null);
            }
            
            // Always try to remove, as this coprocessor doesn't exist anymore
            descriptor.removeCoprocessor("com.salesforce.phoenix.join.HashJoiningRegionObserver");
            // Remove indexing coprocessor if on VIEW or INDEX, as we may have added this by mistake in 2.x versions
            if (tableType == PTableType.INDEX || tableType == PTableType.VIEW) {
                if (!isOldSysTable) {
                    descriptor.removeCoprocessor(Indexer.class.getName());
                    descriptor.removeCoprocessor(OLD_INDEXER_CLASS_NAME);
                }
            }
            // TODO: better encapsulation for this
            // Since indexes can't have indexes, don't install our indexing coprocessor for indexes. Also,
            // don't install on the metadata table until we fix the TODO there.
            if ((tableType != PTableType.INDEX && tableType != PTableType.VIEW) && !SchemaUtil.isMetaTable(tableName) && !descriptor.hasCoprocessor(Indexer.class.getName())) {
                if (!isOldSysTable) {
                    descriptor.removeCoprocessor(OLD_INDEXER_CLASS_NAME);
                }
                Map<String, String> opts = Maps.newHashMapWithExpectedSize(1);
                opts.put(CoveredColumnsIndexBuilder.CODEC_CLASS_NAME_KEY, PhoenixIndexCodec.class.getName());
                Indexer.enableIndexing(descriptor, PhoenixIndexBuilder.class, opts);
            }
            
            // Setup split policy on Phoenix metadata table to ensure that the key values of a Phoenix table
            // stay on the same region.
            if (SchemaUtil.isMetaTable(tableName)) {
                descriptor.setValue(ConnectionQueryServicesImpl.UPGRADE_TO_3_0, Boolean.TRUE.toString());
                if (!descriptor.hasCoprocessor(MetaDataEndpointImpl.class.getName())) {
                    descriptor.removeCoprocessor(MetaDataEndpointImpl.class.getName().replace(NEW_PACKAGE, OLD_PACKAGE));
                    descriptor.addCoprocessor(MetaDataEndpointImpl.class.getName(), null, 1, null);
                }
                if (!descriptor.hasCoprocessor(MetaDataRegionObserver.class.getName())) {
                    descriptor.removeCoprocessor(MetaDataRegionObserver.class.getName().replace(NEW_PACKAGE, OLD_PACKAGE));
                    descriptor.addCoprocessor(MetaDataRegionObserver.class.getName(), null, 2, null);
                }
            } else if (SchemaUtil.isSequenceTable(tableName)) {
                if (!descriptor.hasCoprocessor(SequenceRegionObserver.class.getName())) {
                    descriptor.addCoprocessor(SequenceRegionObserver.class.getName(), null, 1, null);
                }
            }
        } catch (IOException e) {
            throw ServerUtil.parseServerException(e);
        }
    }

    private void ensureFamilyCreated(byte[] tableName, PTableType tableType , Pair<byte[],Map<String,Object>> family) throws SQLException {
        HBaseAdmin admin = null;
        SQLException sqlE = null;
        try {
            admin = new HBaseAdmin(config);
            try {
                HTableDescriptor existingDesc = admin.getTableDescriptor(tableName);
                HColumnDescriptor oldDescriptor = existingDesc.getFamily(family.getFirst());
                HColumnDescriptor columnDescriptor = null;

                if (oldDescriptor == null) {
                    if (tableType == PTableType.VIEW) {
                        String fullTableName = Bytes.toString(tableName);
                        throw new ReadOnlyTableException(
                                "The HBase column families for a VIEW must already exist",
                                SchemaUtil.getSchemaNameFromFullName(fullTableName),
                                SchemaUtil.getTableNameFromFullName(fullTableName),
                                Bytes.toString(family.getFirst()));
                    }
                    columnDescriptor = generateColumnFamilyDescriptor(family, tableType );
                } else {
                    columnDescriptor = new HColumnDescriptor(oldDescriptor);
                    // Don't attempt to make any metadata changes for a VIEW
                    if (tableType == PTableType.VIEW) {
                        return;
                    }
                    modifyColumnFamilyDescriptor(columnDescriptor, family);
                }
                
                if (columnDescriptor.equals(oldDescriptor)) {
                    // Table already has family and it's the same.
                    return;
                }
                admin.disableTable(tableName);
                if (oldDescriptor == null) {
                    admin.addColumn(tableName, columnDescriptor);
                } else {
                    admin.modifyColumn(tableName, columnDescriptor);
                }
                admin.enableTable(tableName);
            } catch (org.apache.hadoop.hbase.TableNotFoundException e) {
                sqlE = new SQLExceptionInfo.Builder(SQLExceptionCode.TABLE_UNDEFINED).setRootCause(e).build().buildException();
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
    
    /**
     * 
     * @param tableName
     * @param splits
     * @param modifyExistingMetaData TODO
     * @param familyNames
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
                /*
                 * Remove the splitPolicy attribute due to an HBase bug (see below)
                 */
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
                     * server and compatible. This works around a nasty, known HBase bug where if a split
                     * policy class cannot be found on the server, the HBase table is left in a horrible
                     * "ghost" state where it can't be used and can't be deleted without bouncing the master. 
                     */
                    newDesc.setValue(HTableDescriptor.SPLIT_POLICY, MetaDataSplitPolicy.class.getName());
                    admin.disableTable(tableName);
                    admin.modifyTable(tableName, newDesc);
                    admin.enableTable(tableName);

                    upgradeWhiteList = upgradeCoprocessorsTo3_0(admin, false);
                }
                return null;
            } else {
                if (!modifyExistingMetaData || existingDesc.equals(newDesc)) {
                    // Table is already created. Note that the presplits are ignored in this case
                    if (isMetaTable) {
                        checkClientServerCompatibility();
                    }
                    return existingDesc;
                }

                if (isMetaTable) {
                    checkClientServerCompatibility();
                    // To allow clients to run the upgrade again, for example if more tables were added to white list
                    if (!Boolean.TRUE.toString().equals(existingDesc.getValue(ConnectionQueryServicesImpl.UPGRADE_TO_3_0))) {
                        upgradeWhiteList = upgradeCoprocessorsTo3_0(admin, true);
                    }
                }
                         
                // TODO: Take advantage of online schema change ability by setting "hbase.online.schema.update.enable" to true
                admin.disableTable(tableName);
                admin.modifyTable(tableName, newDesc);
                admin.enableTable(tableName);
                
                return newDesc;
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
        return null; // will never make it here
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
    @SuppressWarnings("deprecation")
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
            if (!Boolean.TRUE.equals(PDataType.BOOLEAN.toObject(desc.getValue(MetaDataUtil.IS_VIEW_INDEX_TABLE_PROP_BYTES)))) {
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
        HTableDescriptor desc = null;
        HBaseAdmin admin = null;
        boolean wasDeleted = false;
        try {
            admin = new HBaseAdmin(config);
            try {
                desc = admin.getTableDescriptor(physicalIndexName);
                if (Boolean.TRUE.equals(PDataType.BOOLEAN.toObject(desc.getValue(MetaDataUtil.IS_VIEW_INDEX_TABLE_PROP_BYTES)))) {
                    final ReadOnlyProps props = this.getProps();
                    final boolean dropMetadata = props.getBoolean(DROP_METADATA_ATTRIB, DEFAULT_DROP_METADATA);
                    if (dropMetadata) {
                        admin.disableTable(physicalIndexName);
                        admin.deleteTable(physicalIndexName);
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
        if ((tableType == PTableType.VIEW && physicalTableName != null) || (tableType != PTableType.VIEW && physicalTableName == null)) {
            // For views this will ensure that metadata already exists
            ensureTableCreated(tableName, tableType, tableProps, families, splits, true);
        }
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        if (tableType == PTableType.INDEX && physicalTableName != null) { // Index on view
            // Physical index table created up front for multi tenant
            // TODO: if viewIndexId is Short.MIN_VALUE, then we don't need to attempt to create it
            if (!MetaDataUtil.isMultiTenant(m, kvBuilder, ptr)) {
                ensureViewIndexTableCreated(tenantIdBytes.length == 0 ? null : PNameFactory.newName(tenantIdBytes), physicalTableName, MetaDataUtil.getClientTimeStamp(m));
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
    public MetaDataMutationResult dropTable(final List<Mutation> tableMetaData, final PTableType tableType) throws SQLException {
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
            if (tableType == PTableType.TABLE) {
                byte[] physicalTableName = SchemaUtil.getTableNameAsBytes(schemaBytes, tableBytes);
                long timestamp = MetaDataUtil.getClientTimeStamp(tableMetaData);
                ensureViewIndexTableDropped(physicalTableName, timestamp);
            }
            break;
        default:
            break;
        }
          return result;
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
            table = latestMetaData.getTable(new PTableKey(tenantId, name));
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
    public MetaDataMutationResult addColumn(final List<Mutation> tableMetaData, List<Pair<byte[],Map<String,Object>>> families, PTable table) throws SQLException {
        byte[][] rowKeyMetaData = new byte[3][];
        PTableType tableType = table.getType();

        Mutation m = tableMetaData.get(0);
        byte[] rowKey = m.getRow();
        SchemaUtil.getVarChars(rowKey, rowKeyMetaData);
        byte[] tenantIdBytes = rowKeyMetaData[PhoenixDatabaseMetaData.TENANT_ID_INDEX];
        byte[] schemaBytes = rowKeyMetaData[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
        byte[] tableBytes = rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
        byte[] tableKey = SchemaUtil.getTableKey(tenantIdBytes, schemaBytes, tableBytes);
        for ( Pair<byte[],Map<String,Object>>  family : families) {
            ensureFamilyCreated(table.getPhysicalName().getBytes(), tableType, family);
        }
        // Special case for call during drop table to ensure that the empty column family exists.
        // In this, case we only include the table header row, as until we add schemaBytes and tableBytes
        // as args to this function, we have no way of getting them in this case.
        // TODO: change to  if (tableMetaData.isEmpty()) once we pass through schemaBytes and tableBytes
        // Also, could be used to update property values on ALTER TABLE t SET prop=xxx
        if (tableMetaData.size() == 1 && tableMetaData.get(0).isEmpty()) {
            return null;
        }
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
               && Boolean.FALSE.equals(PDataType.BOOLEAN.toObject(ptr))) {
                flushTable(table.getPhysicalName().getBytes());
            }
            
            if (tableType == PTableType.TABLE) {
                // If we're changing MULTI_TENANT to true or false, create or drop the view index table
                if (MetaDataUtil.getMutationValue(m, PhoenixDatabaseMetaData.MULTI_TENANT_BYTES, kvBuilder, ptr)){
                    long timestamp = MetaDataUtil.getClientTimeStamp(m);
                    if (Boolean.TRUE.equals(PDataType.BOOLEAN.toObject(ptr.get(), ptr.getOffset(), ptr.getLength()))) {
                        this.ensureViewIndexTableCreated(table, timestamp);
                    } else {
                        this.ensureViewIndexTableDropped(table.getPhysicalName().getBytes(), timestamp);
                    }
                }
            }
        }
        return result;
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
            break;
        default:
            break;
        }
        return result;
       
    }

    // Keeping this to use for further upgrades
    protected PhoenixConnection addColumnsIfNotExists(PhoenixConnection oldMetaConnection, long timestamp, String columns) throws SQLException {
        Properties props = new Properties(oldMetaConnection.getClientInfo());
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(timestamp));
        // Cannot go through DriverManager or you end up in an infinite loop because it'll call init again
        PhoenixConnection metaConnection = new PhoenixConnection(this, oldMetaConnection.getURL(), props, oldMetaConnection.getMetaDataCache());
        SQLException sqlE = null;
        try {
            metaConnection.createStatement().executeUpdate("ALTER TABLE " + PhoenixDatabaseMetaData.SYSTEM_CATALOG + " ADD IF NOT EXISTS " + columns );
        } catch (SQLException e) {
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
    public void init(String url, Properties props) throws SQLException {
        if (initialized) {
            if (initializationException != null) {
                // Throw previous initialization exception, as we won't resuse this instance
                throw initializationException;
            }
            return;
        }
        synchronized (this) {
            if (initialized) {
                if (initializationException != null) {
                    // Throw previous initialization exception, as we won't resuse this instance
                    throw initializationException;
                }
                return;
            }
            if (closed) {
                throw new SQLException("The connection to the cluster has been closed.");
            }
                
            SQLException sqlE = null;
            PhoenixConnection metaConnection = null;
            try {
                openConnection();
                Properties scnProps = PropertiesUtil.deepCopy(props);
                scnProps.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP));
                scnProps.remove(PhoenixRuntime.TENANT_ID_ATTRIB);
                metaConnection = new PhoenixConnection(this, url, scnProps, newEmptyMetaData());
                try {
                    metaConnection.createStatement().executeUpdate(QueryConstants.CREATE_TABLE_METADATA);
                } catch (NewerTableAlreadyExistsException ignore) {
                    // Ignore, as this will happen if the SYSTEM.TABLE already exists at this fixed timestamp.
                    // A TableAlreadyExistsException is not thrown, since the table only exists *after* this fixed timestamp.
                }
                try {
                    metaConnection.createStatement().executeUpdate(QueryConstants.CREATE_SEQUENCE_METADATA);
                } catch (NewerTableAlreadyExistsException ignore) {
                    // Ignore, as this will happen if the SYSTEM.SEQUENCE already exists at this fixed timestamp.
                    // A TableAlreadyExistsException is not thrown, since the table only exists *after* this fixed timestamp.
                }
                upgradeMetaDataTo3_0(url, props);
            } catch (SQLException e) {
                sqlE = e;
            } finally {
                try {
                    if (metaConnection != null) metaConnection.close();
                } catch (SQLException e) {
                    if (sqlE != null) {
                        sqlE.setNextException(e);
                    } else {
                        sqlE = e;
                    }
                } finally {
                    try {
                        if (sqlE != null) {
                            initializationException = sqlE;
                            throw sqlE;
                        }
                    } finally {
                        initialized = true;
                    }
                }
            }
        }
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
    protected void clearCache() throws SQLException {
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
    public long createSequence(String tenantId, String schemaName, String sequenceName, long startWith, long incrementBy, long cacheSize, long timestamp) 
            throws SQLException {
        SequenceKey sequenceKey = new SequenceKey(tenantId, schemaName, sequenceName);
        Sequence newSequences = new Sequence(sequenceKey);
        Sequence sequence = sequenceMap.putIfAbsent(sequenceKey, newSequences);
        if (sequence == null) {
            sequence = newSequences;
        }
        try {
            sequence.getLock().lock();
            // Now that we have the lock we need, create the sequence
            Append append = sequence.createSequence(startWith, incrementBy, cacheSize, timestamp);
            HTableInterface htable = this.getTable(PhoenixDatabaseMetaData.SEQUENCE_TABLE_NAME_BYTES);
            try {
                Result result = htable.append(append);
                return sequence.createSequence(result);
            } catch (IOException e) {
                throw ServerUtil.parseServerException(e);
            }
        } finally {
            sequence.getLock().unlock();
        }
    }

    @Override
    public long dropSequence(String tenantId, String schemaName, String sequenceName, long timestamp) throws SQLException {
        SequenceKey sequenceKey = new SequenceKey(tenantId, schemaName, sequenceName);
        Sequence newSequences = new Sequence(sequenceKey);
        Sequence sequence = sequenceMap.putIfAbsent(sequenceKey, newSequences);
        if (sequence == null) {
            sequence = newSequences;
        }
        try {
            sequence.getLock().lock();
            // Now that we have the lock we need, create the sequence
            Append append = sequence.dropSequence(timestamp);
            HTableInterface htable = this.getTable(PhoenixDatabaseMetaData.SEQUENCE_TABLE_NAME_BYTES);
            try {
                Result result = htable.append(append);
                return sequence.dropSequence(result);
            } catch (IOException e) {
                throw ServerUtil.parseServerException(e);
            }
        } finally {
            sequence.getLock().unlock();
        }
    }

    /**
     * Gets the current sequence value
     * @param tenantId
     * @param sequence
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
    public void validateSequences(List<SequenceKey> sequenceKeys, long timestamp, long[] values, SQLException[] exceptions, Sequence.Action action) throws SQLException {
        incrementSequenceValues(sequenceKeys, timestamp, values, exceptions, 0, action);
    }
    
    /**
     * Increment any of the set of sequences that need more values. These are the sequences
     * that are asking for the next value within a given statement. The returned sequences
     * are the ones that were not found because they were deleted by another client. 
     * @param tenantId
     * @param sequenceKeys sorted list of sequence kyes
     * @param batchSize
     * @param timestamp
     * @throws SQLException if any of the sequences cannot be found
     * 
     */
    @Override
    public void incrementSequences(List<SequenceKey> sequenceKeys, long timestamp, long[] values, SQLException[] exceptions) throws SQLException {
        incrementSequenceValues(sequenceKeys, timestamp, values, exceptions, 1, Sequence.Action.RESERVE);
    }

    @SuppressWarnings("deprecation")
    private void incrementSequenceValues(List<SequenceKey> keys, long timestamp, long[] values, SQLException[] exceptions, int factor, Sequence.Action action) throws SQLException {
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
                    values[i] = sequence.incrementValue(timestamp, factor, action);
                } catch (EmptySequenceCacheException e) {
                    indexes[toIncrementList.size()] = i;
                    toIncrementList.add(sequence);
                    Increment inc = sequence.newIncrement(timestamp, action);
                    incrementBatch.add(inc);
                }
            }
            if (toIncrementList.isEmpty()) {
                return;
            }
            HTableInterface hTable = this.getTable(PhoenixDatabaseMetaData.SEQUENCE_TABLE_NAME_BYTES);
            Object[] resultObjects = null;
            SQLException sqlE = null;
            try {
                resultObjects= hTable.batch(incrementBatch);
            } catch (IOException e){
                sqlE = ServerUtil.parseServerException(e);
            } catch (InterruptedException e){
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
                    values[indexes[i]] = sequence.incrementValue(result, factor);
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
            HTableInterface hTable = this.getTable(PhoenixDatabaseMetaData.SEQUENCE_TABLE_NAME_BYTES);
            Object[] resultObjects = null;
            SQLException sqlE = null;
            try {
                resultObjects= hTable.batch(mutations);
            } catch (IOException e){
                sqlE = ServerUtil.parseServerException(e);
            } catch (InterruptedException e){
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
        HTableInterface hTable = this.getTable(PhoenixDatabaseMetaData.SEQUENCE_TABLE_NAME_BYTES);
        SQLException sqlE = null;
        try {
            hTable.batch(mutations);
        } catch (IOException e){
            sqlE = ServerUtil.parseServerException(e);
        } catch (InterruptedException e){
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
    public synchronized void addConnection(PhoenixConnection connection) throws SQLException {
        connectionCount++;
    }

    @Override
    public void removeConnection(PhoenixConnection connection) throws SQLException {
        ConcurrentMap<SequenceKey,Sequence> formerSequenceMap = null;
        synchronized(this) {
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
        // TODO: Keep map of Feature -> min HBase version
        // For now, only Feature is REVERSE_SCAN and it's not supported in any version yet
        return false;
    }
    
    private static final class WhiteList implements Iterable<String>{
        private final Set<String> matchList;
        private static final String MATCH_EVERYTHING = "*";
        private static final String DELIMITER = ",";
        
        public WhiteList() {
            matchList = Collections.emptySet();
        }
        
        public WhiteList(Set<String> matchList) {
            this.matchList = matchList == null ? null : ImmutableSet.copyOf(matchList);
        }
        
        public WhiteList(String tableNames) {
            Set<String> matchList = Sets.newHashSet();
            if (tableNames == null) {
                this.matchList = ImmutableSet.of();
            } else if (MATCH_EVERYTHING.equals(tableNames)) {
                this.matchList = null;
            } else {
                matchList = Sets.newHashSet();
                StringTokenizer tokenizer = new StringTokenizer(tableNames,DELIMITER);
                while (tokenizer.hasMoreTokens()) {
                    matchList.add(tokenizer.nextToken().trim());
                }
                this.matchList = ImmutableSet.copyOf(matchList);
            }
        }
        
        public boolean alwaysMatches() {
            return matchList == null;
        }
        
        public boolean neverMatches() {
            return matchList != null && matchList.isEmpty();
        }
        
        public boolean matches(byte[] fullTableName) {
            if (matchList == null) {
                return true;
            }
            return matchList.contains(Bytes.toString(fullTableName));
        }

        @Override
        public Iterator<String> iterator() {
            if (matchList == null) {
                return Iterators.emptyIterator();
            }
            return matchList.iterator();
        }
        
        @Override
        public String toString() {
            return alwaysMatches() ? "ALL" : neverMatches() ? "NONE" : matchList.toString();
        }

        public boolean matches(String tableName) {
            return matchList == null ? true : matchList.contains(tableName);
        }
    }
    
    private static void addViewForSystemTable(Connection conn) throws SQLException {
        conn.createStatement().execute("CREATE VIEW IF NOT EXISTS " + OLD_SYSTEM_TABLE_AS_VIEW_NAME + "(\n" +
                "TABLE_SCHEM VARCHAR NULL,\n" + 
                "TABLE_NAME VARCHAR NOT NULL,\n" + 
                "COLUMN_NAME VARCHAR NULL,\n" + 
                "TABLE_CAT VARCHAR NULL,\n" + 
                "TABLE_TYPE CHAR(1),\n" + 
                "TABLE_SEQ_NUM BIGINT,\n" +
                "PK_NAME VARCHAR,\n" + 
                "COLUMN_COUNT INTEGER,\n" + 
                "SALT_BUCKETS INTEGER,\n" + 
                "DATA_TABLE_NAME VARCHAR,\n" + 
                "INDEX_STATE CHAR(1),\n" + 
                "IMMUTABLE_ROWS BOOLEAN,\n" + 
                "VIEW_STATEMENT VARCHAR,\n" + 
                "DEFAULT_COLUMN_FAMILY VARCHAR,\n" + 
                "DISABLE_WAL BOOLEAN,\n" + 
                "MULTI_TENANT BOOLEAN,\n" + 
                "VIEW_TYPE UNSIGNED_TINYINT,\n" + 
                "VIEW_INDEX_ID SMALLINT,\n" + 
                "DATA_TYPE INTEGER,\n" + 
                "COLUMN_SIZE INTEGER,\n" + 
                "DECIMAL_DIGITS INTEGER,\n" + 
                "NULLABLE INTEGER,\n" + 
                "ORDINAL_POSITION INTEGER,\n" + 
                "COLUMN_MODIFIER INTEGER,\n" + 
                "CONSTRAINT pk PRIMARY KEY (TABLE_SCHEM,TABLE_NAME,COLUMN_NAME, TABLE_CAT))\n" +
        "AS SELECT * FROM SYSTEM.\"TABLE\" DEFAULT_COLUMN_FAMILY='_0'");
    }
    private static void addWhereClauseForUpgrade3_0(WhiteList whiteList, StringBuilder buf) {
        if (whiteList.neverMatches()) {
            buf.append(" WHERE FALSE\n");
        } else {
            // Filter out system tables
            // Filter in tables in white list and indexes for tables in white list
            buf.append(" WHERE ");
            if (whiteList.alwaysMatches()) {
                buf.append("NOT (");
                buf.append(TABLE_SCHEM +  " = '" + OLD_SYSTEM_SCHEMA_NAME + "'" + " AND ");
                buf.append(TABLE_NAME + " = '" + OLD_SYSTEM_TABLE_NAME + "')\n");
            } else {
                for (String fullName : whiteList) {
                    buf.append(" (");
                    String schemaName = SchemaUtil.getSchemaNameFromFullName(fullName);
                    buf.append((TABLE_SCHEM + (schemaName.length()==0 ? " IS NULL " : (" = '" + schemaName + "'")) + " AND "));
                    String tableName = SchemaUtil.getTableNameFromFullName(fullName);
                    buf.append(TABLE_NAME + " = '" + tableName + "' )\nOR");
                }
                buf.setLength(buf.length()-"OR".length()); // Remove last OR
            }
        }
    }
    
    private static final Integer DEFAULT_PRECISION = PDataType.MAX_PRECISION;
    private static final Integer DEFAULT_SCALE = PDataType.DEFAULT_SCALE;
    
    private void upgradeMetaDataTo3_0(String url, Properties props) throws SQLException {
        if (upgradeWhiteList == null) {
            return;
        }
        if (upgradeWhiteList.neverMatches()) {
            if (logger.isInfoEnabled()) {
                logger.info("No table meta data needs to be upgraded to Apache Phoenix 3.0");
            }
        } else {
            if (logger.isInfoEnabled()) {
                logger.info("Upgrading table meta data to Apache Phoenix 3.0 based on the following white list: " + upgradeWhiteList);
            }
            boolean createdView = false;
            SQLException sqlE = null;
            // If we go through DriverManager, another ConnectionQueryServices is created
            Connection conn = new PhoenixConnection(this, url, props, this.latestMetaData);
            try {
                PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + SYSTEM_CATALOG_NAME + "(\n" +
                        TENANT_ID + "," +
                        TABLE_SCHEM + "," +
                        TABLE_NAME + "," +
                        COLUMN_NAME + "," +
                        COLUMN_FAMILY + "," +
                        TABLE_SEQ_NUM + "," +
                        TABLE_TYPE + "," +
                        DATA_TYPE + "," +
                        PK_NAME + "," +
                        COLUMN_COUNT + "," +
                        SALT_BUCKETS + "," +
                        DATA_TABLE_NAME + "," +
                        INDEX_STATE + "," +
                        IMMUTABLE_ROWS + "," +
                        DEFAULT_COLUMN_FAMILY_NAME + "," +
                        COLUMN_SIZE + "," +
                        DECIMAL_DIGITS + "," +
                        NULLABLE + "," +
                        ORDINAL_POSITION + "," +
                        SORT_ORDER + "," +
                        KEY_SEQ + "," +
                        LINK_TYPE + ")\n" + 
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
                StringBuilder buf = new StringBuilder("SELECT * FROM " + OLD_SYSTEM_TABLE_AS_VIEW_NAME + "\n");
                addWhereClauseForUpgrade3_0(upgradeWhiteList, buf);
                buf.append("ORDER BY " + TABLE_SCHEM + "," + TABLE_NAME + "," + COLUMN_NAME + "," + TABLE_CAT + "," + ORDINAL_POSITION);
                addViewForSystemTable(conn);
                createdView = true;
                ResultSet rs = conn.createStatement().executeQuery(buf.toString());
                String lastSchemaName = null;
                String lastTableName = null;
                short nextKeySeq = 1;
                String tablePkName = null;
                boolean isSalted = false;
                while (rs.next()) {
                    String pkName = null;
                    String defaultColumnFamily = null;
                    String schemaName = rs.getString(TABLE_SCHEM);
                    String tableName = rs.getString(TABLE_NAME);
                    if (!Objects.equal(schemaName, lastSchemaName) || !Objects.equal(tableName, lastTableName)) {
                        if (lastTableName != null) {
                            if (logger.isInfoEnabled()) {
                                logger.info("Committing upgrades of table meta data for: " + SchemaUtil.getTableName(lastSchemaName, lastTableName));
                            }
                            conn.commit();
                        }
                        lastSchemaName = schemaName;
                        lastTableName = tableName;
                        nextKeySeq = 1; // reset
                        defaultColumnFamily = "_0"; // old default column family
                        isSalted = rs.getInt(SALT_BUCKETS) > 0;
                        pkName = tablePkName = rs.getString(PK_NAME);
                        if (logger.isInfoEnabled()) {
                            logger.info("Upgrading table meta data for: " + SchemaUtil.getTableName(schemaName, tableName));
                        }
                    }
                    String columnName = rs.getString(COLUMN_NAME);
                    String familyName = rs.getString(TABLE_CAT); // old cf name
                    Short keySeq = null;
                    Byte linkType = null;
                    if (columnName != null && familyName == null) { // pk column
                        // TODO: remember columnName when keySeq == 1 so that we
                        // can update NULLABLE is this is the sole PK column
                        keySeq = nextKeySeq++;
                        pkName = tablePkName;
                    }
                    if (columnName == null && familyName != null) { // link to index
                        linkType = LinkType.INDEX_TABLE.getSerializedValue();
                    }
                    /* 
                     * Convert from old to new format:
                     * - sql date types to unsigend date types
                     * - don't persist maxLength and scale for non decimal numbers
                     * - don't persist default precision for decimal
                     */
                    Integer ordinalPosition = (Integer)rs.getObject(ORDINAL_POSITION);
                    // ORDINAL_POSITION is not taking into account salting so we need
                    // to subtract one
                    if (ordinalPosition != null && isSalted) {
                        ordinalPosition--;
                    }
                    Integer maxLength = (Integer)rs.getObject(COLUMN_SIZE);
                    Integer scale = (Integer)rs.getObject(DECIMAL_DIGITS);
                    Integer dataTypeNum = (Integer)rs.getObject(DATA_TYPE);
                    if (dataTypeNum != null) {
                        PDataType dataType = PDataType.fromTypeId(dataTypeNum);
                        switch (dataType) {
                            case DECIMAL:
                                if (DEFAULT_PRECISION.equals(maxLength) && DEFAULT_SCALE.equals(scale)) {
                                    maxLength = null;
                                    scale = null;
                                }
                                break;
                            case DATE:
                                dataTypeNum = PDataType.UNSIGNED_DATE.getSqlType();
                                break;
                            case TIME:
                                dataTypeNum = PDataType.UNSIGNED_TIME.getSqlType();
                                break;
                            case TIMESTAMP:
                                dataTypeNum = PDataType.UNSIGNED_TIMESTAMP.getSqlType();
                                break;
                            case BINARY:
                                // From way-back-when, we introduced VARBINARY after BINARY
                                rs.getInt(COLUMN_SIZE);
                                if (rs.wasNull()) {
                                    dataTypeNum = PDataType.VARBINARY.getSqlType();
                                }
                                break;
                            default:
                                // Don't store precision and scale for int, long, etc, as
                                // there's no value (we actually lose information and need
                                // more special cases b/c we don't know that it wasn't set).
                                if (dataType.isCoercibleTo(PDataType.LONG)) {
                                    maxLength = null;
                                    scale = null;
                                }
                                break;
                        }
                    }
                   /* 
                     * Upsert meta .data for one table
                     * Use setObject, as primitive value may often be null
                     */
                    stmt.setString(1,null);
                    stmt.setString(2,schemaName);
                    stmt.setString(3,tableName);
                    stmt.setString(4,columnName);
                    stmt.setString(5,familyName);
                    stmt.setObject(6, rs.getObject(TABLE_SEQ_NUM));
                    stmt.setString(7, rs.getString(TABLE_TYPE));
                    stmt.setObject(8, dataTypeNum);
                    stmt.setString(9, pkName);
                    stmt.setObject(10, rs.getObject(COLUMN_COUNT));
                    stmt.setObject(11, rs.getObject(SALT_BUCKETS));
                    stmt.setString(12, rs.getString(DATA_TABLE_NAME));
                    stmt.setString(13, rs.getString(INDEX_STATE));
                    stmt.setObject(14, rs.getObject(IMMUTABLE_ROWS));
                    stmt.setString(15, defaultColumnFamily); // previous DEFAULT_COLUMN_FAMILY_NAME
                    stmt.setObject(16, maxLength);
                    stmt.setObject(17, scale);
                    stmt.setObject(18, rs.getObject(NULLABLE));
                    stmt.setObject(19, ordinalPosition);
                    stmt.setObject(20, rs.getObject("COLUMN_MODIFIER")); // old column name
                    stmt.setObject(21, keySeq); // new column for PK position
                    stmt.setObject(22, linkType); // link to index
                    stmt.execute();
                }
                if (lastTableName != null) {
                    if (logger.isInfoEnabled()) {
                        logger.info("Committing upgrades of table meta data for: " + SchemaUtil.getTableName(lastSchemaName, lastTableName));
                    }
                    conn.commit();
                }
            } catch (TableNotFoundException e) {
                if (logger.isInfoEnabled()) {
                    logger.info("Unable to upgrade metadata.",e);
                }
            } catch (SQLException e) {
                sqlE = e;
            } finally {
                try {
                    if (createdView) {
                        Long scn = JDBCUtil.getCurrentSCN(url, props);
                        if (scn == null) {
                            conn.createStatement().execute("DROP VIEW IF EXISTS " + OLD_SYSTEM_TABLE_AS_VIEW_NAME);                        
                        } else {
                            Properties newProps = new Properties(props);
                            newProps.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(scn + 1));
                            // If we go through DriverManager, another ConnectionQueryServices is created
                            Connection newConn = new PhoenixConnection(this, url, newProps, this.latestMetaData);
                            try {
                                newConn.createStatement().execute("DROP VIEW IF EXISTS " + OLD_SYSTEM_TABLE_AS_VIEW_NAME);                        
                            } finally {
                                try { // Don't propagate a close exception
                                    newConn.close();
                                } catch (SQLException e) {
                                    logger.warn("Unable to close connection",e);
                                }
                            }
                        }
                    }
                } catch (SQLException e) {
                    if (sqlE == null) { // Chain together sql exceptions
                        sqlE = e;
                    } else {
                        sqlE.setNextException(e);
                    }
                } finally {
                    try { // Don't propagate a close exception
                        conn.close();
                    } catch (SQLException e) {
                        logger.warn("Unable to close connection",e);
                    } finally {
                        if (sqlE != null) { // Throw if any sql exceptions
                            throw sqlE;
                        }
                    }
                }
            }
        }
    }

     /**
     * Walk through all existing tables and install new coprocessors
     * @param admin
     * @param systemCatalogDesc 
     * @throws IOException
     * @throws SQLException
     */
    @SuppressWarnings("deprecation")
    private WhiteList upgradeCoprocessorsTo3_0(HBaseAdmin admin, boolean forceUpgrade) throws IOException, SQLException {
        String files = config.get(QueryServices.AUTO_UPGRADE_WHITELIST_ATTRIB);
        WhiteList coprocUpgradeWhiteList = new WhiteList(files);
        if (coprocUpgradeWhiteList.neverMatches()) {
            if (logger.isInfoEnabled()) {
                logger.info("No coprocessors will be upgraded from Salesforce Phoenix 2.x to Apache Phoenix 3.0");
            }
            return coprocUpgradeWhiteList;
        } else {
            Set<String> upgradedTables = Sets.newHashSet();
            if (logger.isInfoEnabled()) {
                logger.info("Upgrading coprocessors from Salesforce Phoenix 2.x to Apache Phoenix 3.0 based on the following white list: " + coprocUpgradeWhiteList);
            }
            /* Use regular HBase scan instead of query because the jar on the server may
             * not be compatible (we don't know yet) and this is our one chance to do
             * the conversion automatically.
             */
            Scan scan = new Scan();
            scan.addColumn(OLD_DEFAULT_COLUMN_FAMILY_BYTES, TABLE_TYPE_BYTES);
            scan.addColumn(OLD_DEFAULT_COLUMN_FAMILY_BYTES, DATA_TABLE_NAME_BYTES);
            SingleColumnValueFilter filter = new SingleColumnValueFilter(OLD_DEFAULT_COLUMN_FAMILY_BYTES, TABLE_TYPE_BYTES, CompareOp.GREATER_OR_EQUAL, PDataType.CHAR.toBytes("a"));
            filter.setFilterIfMissing(true);
            // Add filter so that we only get the table row and not the column rows
            scan.setFilter(filter);
            HTableInterface table = null;
            try {
                table = getTable(OLD_SYSTEM_TABLE_NAME_BYTES);
                ResultScanner scanner = table.getScanner(scan);
                Result result = null;
                while ((result = scanner.next()) != null) {
                    byte[] rowKey = result.getRow();
                    byte[][] rowKeyMetaData = new byte[2][];
                    SchemaUtil.getVarChars(rowKey, rowKeyMetaData);
                    byte[] schemaBytes = rowKeyMetaData[0];
                    byte[] tableBytes = rowKeyMetaData[1];
                    byte[] tableName = SchemaUtil.getTableNameAsBytes(schemaBytes, tableBytes);
                    Cell tableTypeKv = result.getColumnLatestCell(OLD_DEFAULT_COLUMN_FAMILY_BYTES, TABLE_TYPE_BYTES);
                    PTableType tableType = PTableType.fromSerializedValue(tableTypeKv.getValueArray()[tableTypeKv.getValueOffset()]);
                    Cell dataNameKv = result.getColumnLatestCell(OLD_DEFAULT_COLUMN_FAMILY_BYTES, DATA_TABLE_NAME_BYTES);
                    // Update coprocessors if table is on white list or it's data table is on the white list
                    if (tableType != PTableType.SYSTEM
                            && (coprocUpgradeWhiteList.matches(tableName) || (dataNameKv != null
                            && coprocUpgradeWhiteList.matches(SchemaUtil.getTableName(schemaBytes, dataNameKv.getValue()))))) {
                        HTableDescriptor existingDesc = admin.getTableDescriptor(tableName);
                        HTableDescriptor newDesc = new HTableDescriptor(existingDesc);
                        addCoprocessors(tableName, newDesc, tableType);
                        String fullTableName = Bytes.toString(tableName);
                        upgradedTables.add(fullTableName);

                        if (!existingDesc.equals(newDesc)) {
                            if (logger.isInfoEnabled()) {
                                logger.info("Upgrading coprocessors for: " + SchemaUtil.getTableName(schemaBytes, tableBytes));
                            }
                            admin.disableTable(tableName);
                            admin.modifyTable(tableName, newDesc);
                            admin.enableTable(tableName);
                        }
                    }
                }
                return new WhiteList(upgradedTables);
            } catch (TableNotFoundException ignore) {
                return new WhiteList(); // No SYSTEM.TABLE, so nothing to do
            } finally {
                if (table != null) Closeables.closeQuietly(table);
            }
        }
    }
}
