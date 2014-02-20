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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TYPE_TABLE_NAME_BYTES;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_DROP_METADATA;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.phoenix.client.KeyValueBuilder;
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
import org.apache.phoenix.exception.PhoenixIOException;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.hbase.index.Indexer;
import org.apache.phoenix.hbase.index.covered.CoveredColumnsIndexBuilder;
import org.apache.phoenix.index.PhoenixIndexBuilder;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver.ConnectionInfo;
import org.apache.phoenix.schema.EmptySequenceCacheException;
import org.apache.phoenix.schema.MetaDataSplitPolicy;
import org.apache.phoenix.schema.NewerTableAlreadyExistsException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PMetaData;
import org.apache.phoenix.schema.PMetaDataImpl;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.ReadOnlyTableException;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.schema.Sequence;
import org.apache.phoenix.schema.SequenceKey;
import org.apache.phoenix.schema.TableAlreadyExistsException;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.JDBCUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;

public class ConnectionQueryServicesImpl extends DelegateQueryServices implements ConnectionQueryServices {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionQueryServicesImpl.class);
    private static final int INITIAL_CHILD_SERVICES_CAPACITY = 100;
    private static final int DEFAULT_OUT_OF_ORDER_MUTATIONS_WAIT_TIME_MS = 1000;
    protected final Configuration config;
    // Copy of config.getProps(), but read-only to prevent synchronization that we
    // don't need.
    private final ReadOnlyProps props;
    private final HConnection connection;
    private final StatsManager statsManager;
    private final ConcurrentHashMap<ImmutableBytesWritable,ConnectionQueryServices> childServices;
    // Cache the latest meta data here for future connections
    private volatile PMetaData latestMetaData = PMetaDataImpl.EMPTY_META_DATA;
    private final Object latestMetaDataLock = new Object();
    // Lowest HBase version on the cluster.
    private int lowestClusterHBaseVersion = Integer.MAX_VALUE;
    private boolean hasInvalidIndexConfiguration = false;
    private int connectionCount = 0;
    
    private ConcurrentMap<SequenceKey,Sequence> sequenceMap = Maps.newConcurrentMap();
    private KeyValueBuilder kvBuilder;

    /**
     * Construct a ConnectionQueryServicesImpl that represents a connection to an HBase
     * cluster.
     * @param services base services from where we derive our default configuration
     * @param connectionInfo to provide connection information
     * @throws SQLException
     */
    public ConnectionQueryServicesImpl(QueryServices services, ConnectionInfo connectionInfo) throws SQLException {
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
        this.props = new ReadOnlyProps(this.config.iterator());
        try {
            this.connection = HBaseFactoryProvider.getHConnectionFactory().createConnection(this.config);
        } catch (ZooKeeperConnectionException e) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_ESTABLISH_CONNECTION)
                .setRootCause(e).build().buildException();
        }
        if (this.connection.isClosed()) { // TODO: why the heck doesn't this throw above?
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_ESTABLISH_CONNECTION).build().buildException();
        }
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
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public HTableDescriptor getTableDescriptor(byte[] tableName) throws SQLException {
        HTableInterface htable = getTable(tableName);
        try {
            return htable.getTableDescriptor();
        } catch (IOException e) {
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
        SQLException sqlE = null;
        try {
            // Clear Phoenix metadata cache before closing HConnection
            clearCache();
        } catch (SQLException e) {
            sqlE = e;
        } finally {
            try {
                connection.close();
            } catch (IOException e) {
                if (sqlE == null) {
                    sqlE = ServerUtil.parseServerException(e);
                } else {
                    sqlE.setNextException(ServerUtil.parseServerException(e));
                }
                throw sqlE;
            } finally {
                super.close();
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
        connection.clearRegionCache(tableName);
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
                  HRegionLocation regionLocation = connection.getRegionLocation(tableName, currentKey, reload);
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
            PTable existingTable = latestMetaData.getTable(table.getName().getString());
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
     */
    private PMetaData metaDataMutated(String tableName, long tableSeqNum, Mutator mutator) throws SQLException {
        synchronized(latestMetaDataLock) {
            PMetaData metaData = latestMetaData;
            PTable table;
            long endTime = System.currentTimeMillis() + DEFAULT_OUT_OF_ORDER_MUTATIONS_WAIT_TIME_MS;
            while (true) {
                try {
                    try {
                        table = metaData.getTable(tableName);
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
                        metaData = metaData.removeTable(tableName);
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
    public PMetaData addColumn(final String tableName, final List<PColumn> columns, final long tableTimeStamp, final long tableSeqNum, final boolean isImmutableRows) throws SQLException {
        return metaDataMutated(tableName, tableSeqNum, new Mutator() {
            @Override
            public PMetaData mutate(PMetaData metaData) throws SQLException {
                try {
                    return metaData.addColumn(tableName, columns, tableTimeStamp, tableSeqNum, isImmutableRows);
                } catch (TableNotFoundException e) {
                    // The DROP TABLE may have been processed first, so just ignore.
                    return metaData;
                }
            }
        });
     }

    @Override
    public PMetaData removeTable(final String tableName) throws SQLException {
        synchronized(latestMetaDataLock) {
            latestMetaData = latestMetaData.removeTable(tableName);
            latestMetaDataLock.notifyAll();
            return latestMetaData;
        }
    }

    @Override
    public PMetaData removeColumn(final String tableName, final String familyName, final String columnName, final long tableTimeStamp, final long tableSeqNum) throws SQLException {
        return metaDataMutated(tableName, tableSeqNum, new Mutator() {
            @Override
            public PMetaData mutate(PMetaData metaData) throws SQLException {
                try {
                    return metaData.removeColumn(tableName, familyName, columnName, tableTimeStamp, tableSeqNum);
                } catch (TableNotFoundException e) {
                    // The DROP TABLE may have been processed first, so just ignore.
                    return metaData;
                }
            }
        });
    }


    @Override
    public PhoenixConnection connect(String url, Properties info) throws SQLException {
        Long scn = JDBCUtil.getCurrentSCN(url, info);
        PMetaData metaData = scn == null ? latestMetaData : PMetaDataImpl.pruneNewerTables(scn, latestMetaData);
        return new PhoenixConnection(this, url, info, metaData);
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
    
    private HTableDescriptor generateTableDescriptor(byte[] tableName, HTableDescriptor existingDesc, PTableType tableType, Map<String,Object> tableProps, List<Pair<byte[],Map<String,Object>>> families, byte[][] splits) throws SQLException {
        String defaultFamilyName = (String)tableProps.remove(PhoenixDatabaseMetaData.DEFAULT_COLUMN_FAMILY_NAME);                
        HTableDescriptor descriptor = (existingDesc != null) ? new HTableDescriptor(existingDesc) : new HTableDescriptor(tableName);
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
        // The phoenix jar must be available on HBase classpath
        try {
            if (!descriptor.hasCoprocessor(ScanRegionObserver.class.getName())) {
                descriptor.addCoprocessor(ScanRegionObserver.class.getName(), null, 1, null);
            }
            if (!descriptor.hasCoprocessor(UngroupedAggregateRegionObserver.class.getName())) {
                descriptor.addCoprocessor(UngroupedAggregateRegionObserver.class.getName(), null, 1, null);
            }
            if (!descriptor.hasCoprocessor(GroupedAggregateRegionObserver.class.getName())) {
                descriptor.addCoprocessor(GroupedAggregateRegionObserver.class.getName(), null, 1, null);
            }
            if (!descriptor.hasCoprocessor(ServerCachingEndpointImpl.class.getName())) {
                descriptor.addCoprocessor(ServerCachingEndpointImpl.class.getName(), null, 1, null);
            }
            // TODO: better encapsulation for this
            // Since indexes can't have indexes, don't install our indexing coprocessor for indexes. Also,
            // don't install on the metadata table until we fix the TODO there.
            if (tableType != PTableType.INDEX && !descriptor.hasCoprocessor(Indexer.class.getName())
                  && !SchemaUtil.isMetaTable(tableName) && !SchemaUtil.isSequenceTable(tableName)) {
                Map<String, String> opts = Maps.newHashMapWithExpectedSize(1);
                opts.put(CoveredColumnsIndexBuilder.CODEC_CLASS_NAME_KEY, PhoenixIndexCodec.class.getName());
                Indexer.enableIndexing(descriptor, PhoenixIndexBuilder.class, opts);
            }
            
            // Setup split policy on Phoenix metadata table to ensure that the key values of a Phoenix table
            // stay on the same region.
            if (SchemaUtil.isMetaTable(tableName)) {
                descriptor.setValue(SchemaUtil.UPGRADE_TO_2_0, Boolean.TRUE.toString());
                descriptor.setValue(SchemaUtil.UPGRADE_TO_2_1, Boolean.TRUE.toString());
                if (!descriptor.hasCoprocessor(MetaDataEndpointImpl.class.getName())) {
                    descriptor.addCoprocessor(MetaDataEndpointImpl.class.getName(), null, 1, null);
                }
                if (!descriptor.hasCoprocessor(MetaDataRegionObserver.class.getName())) {
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
        return descriptor;
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
                }
                
                // We'll do this alter at the end of the upgrade
                // Just let the table metadata be updated for 3.0 here, as otherwise
                // we have a potential race condition
                // Update metadata of table
                // TODO: Take advantage of online schema change ability by setting "hbase.online.schema.update.enable" to true
                admin.disableTable(tableName);
                // TODO: What if not all existing column families are present?
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
            List<HRegionLocation> locations = this.getAllTableRegions(TYPE_TABLE_NAME_BYTES);
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
            final TreeMap<byte[],Long> results = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
            connection.processExecs(MetaDataProtocol.class, regionKeys,
                    PhoenixDatabaseMetaData.TYPE_TABLE_NAME_BYTES, this.getDelegate().getExecutor(), new Batch.Call<MetaDataProtocol,Long>() {
                        @Override
                        public Long call(MetaDataProtocol instance) throws IOException {
                            return instance.getVersion();
                        }
                    }, 
                    new Batch.Callback<Long>(){
                        @Override
                        public void update(byte[] region, byte[] row, Long value) {
                          results.put(region, value);
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
    private MetaDataMutationResult metaDataCoprocessorExec(byte[] tableKey, Batch.Call<MetaDataProtocol, MetaDataMutationResult> callable) throws SQLException {
        try {
            boolean retried = false;
            while (true) {
                HRegionLocation regionLocation = retried ? connection.relocateRegion(PhoenixDatabaseMetaData.TYPE_TABLE_NAME_BYTES, tableKey) : connection.locateRegion(PhoenixDatabaseMetaData.TYPE_TABLE_NAME_BYTES, tableKey);
                List<byte[]> regionKeys = Collections.singletonList(regionLocation.getRegionInfo().getStartKey());
                final Map<byte[],MetaDataMutationResult> results = Maps.newHashMapWithExpectedSize(1);
                connection.processExecs(MetaDataProtocol.class, regionKeys,
                        PhoenixDatabaseMetaData.TYPE_TABLE_NAME_BYTES, this.getDelegate().getExecutor(), callable, 
                        new Batch.Callback<MetaDataMutationResult>(){
                            @Override
                            public void update(byte[] region, byte[] row, MetaDataMutationResult value) {
                              results.put(region, value);
                            }
                        });
                assert(results.size() == 1);
                MetaDataMutationResult result = results.values().iterator().next();
                if (result.getMutationCode() == MutationCode.TABLE_NOT_IN_REGION) {
                    if (retried) return result;
                    retried = true;
                    continue;
                }
                return result;
            }
        } catch (IOException e) {
            throw ServerUtil.parseServerException(e);
        } catch (Throwable t) {
            throw new SQLException(t);
        }
    }

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
        tableProps.put(MetaDataUtil.IS_VIEW_INDEX_TABLE_PROP_NAME, PDataType.TRUE_BYTES);
        // Only use splits if table is salted, otherwise it may not be applicable
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

        if (tableType == PTableType.INDEX && physicalTableName != null) { // Index on view
            // Physical index table created up front for multi tenant
            // TODO: if viewIndexId is Short.MIN_VALUE, then we don't need to attempt to create it
            if (!MetaDataUtil.isMultiTenant(m)) {
                ensureViewIndexTableCreated(physicalTableName, MetaDataUtil.getClientTimeStamp(m));
            }
        } else if (tableType == PTableType.TABLE && MetaDataUtil.isMultiTenant(m)) { // Create view index table up front for multi tenant tables
            ensureViewIndexTableCreated(tableName, tableProps, families, MetaDataUtil.isSalted(m) ? splits : null, MetaDataUtil.getClientTimeStamp(m));
        }
        
        byte[] tableKey = SchemaUtil.getTableKey(tenantIdBytes, schemaBytes, tableBytes);
        MetaDataMutationResult result = metaDataCoprocessorExec(tableKey,
            new Batch.Call<MetaDataProtocol, MetaDataMutationResult>() {
                @Override
                public MetaDataMutationResult call(MetaDataProtocol instance) throws IOException {
                  return instance.createTable(tableMetaData);
                }
            });
        return result;
    }

    @Override
    public MetaDataMutationResult getTable(byte[] tenantId, final byte[] schemaBytes, final byte[] tableBytes,
            final long tableTimestamp, final long clientTimestamp) throws SQLException {
        final byte[] nonNullTenantId = tenantId == null ? ByteUtil.EMPTY_BYTE_ARRAY : tenantId;
        byte[] tableKey = SchemaUtil.getTableKey(nonNullTenantId, schemaBytes, tableBytes);
        return metaDataCoprocessorExec(tableKey,
                new Batch.Call<MetaDataProtocol, MetaDataMutationResult>() {
                    @Override
                    public MetaDataMutationResult call(MetaDataProtocol instance) throws IOException {
                      return instance.getTable(nonNullTenantId, schemaBytes, tableBytes, tableTimestamp, clientTimestamp);
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
                new Batch.Call<MetaDataProtocol, MetaDataMutationResult>() {
                    @Override
                    public MetaDataMutationResult call(MetaDataProtocol instance) throws IOException {
                      return instance.dropTable(tableMetaData, tableType.getSerializedValue());
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
    
    private void ensureViewIndexTableCreated(byte[] physicalIndexTableName, long timestamp) throws SQLException {
        PTable table;
        String name = Bytes.toString(
                physicalIndexTableName, 
                MetaDataUtil.VIEW_INDEX_TABLE_PREFIX_BYTES.length,
                physicalIndexTableName.length-MetaDataUtil.VIEW_INDEX_TABLE_PREFIX_BYTES.length);
        try {
            table = latestMetaData.getTable(name);
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
        List<Pair<byte[],Map<String,Object>>> families = Lists.newArrayListWithExpectedSize(Math.max(1, table.getColumnFamilies().size()));
        if (families.isEmpty()) {
            byte[] familyName = SchemaUtil.getEmptyColumnFamily(table);
            Map<String,Object> familyProps = createPropertiesMap(htableDesc.getFamily(familyName).getValues());
            families.add(new Pair<byte[],Map<String,Object>>(familyName, familyProps));
        } else {
            for (PColumnFamily family : table.getColumnFamilies()) {
                byte[] familyName = SchemaUtil.getEmptyColumnFamily(table);
                Map<String,Object> familyProps = createPropertiesMap(htableDesc.getFamily(familyName).getValues());
                families.add(new Pair<byte[],Map<String,Object>>(family.getName().getBytes(), familyProps));
            }
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
        MetaDataMutationResult result =  metaDataCoprocessorExec(tableKey,
            new Batch.Call<MetaDataProtocol, MetaDataMutationResult>() {
                @Override
                public MetaDataMutationResult call(MetaDataProtocol instance) throws IOException {
                    return instance.addColumn(tableMetaData);
                }
            });

        if (result.getMutationCode() == MutationCode.COLUMN_NOT_FOUND) { // Success
            // Flush the table if transitioning DISABLE_WAL from TRUE to FALSE
            if (Boolean.FALSE.equals(PDataType.BOOLEAN.toObject(
                    MetaDataUtil.getMutationKVByteValue(m,PhoenixDatabaseMetaData.DISABLE_WAL_BYTES)))) {
                flushTable(table.getPhysicalName().getBytes());
            }
            
            if (tableType == PTableType.TABLE) {
                // If we're changing MULTI_TENANT to true or false, create or drop the view index table
                KeyValue kv = MetaDataUtil.getMutationKeyValue(m, PhoenixDatabaseMetaData.MULTI_TENANT_BYTES);
                if (kv != null) {
                    long timestamp = MetaDataUtil.getClientTimeStamp(m);
                    if (Boolean.TRUE.equals(PDataType.BOOLEAN.toObject(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength()))) {
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
            new Batch.Call<MetaDataProtocol, MetaDataMutationResult>() {
                @Override
                public MetaDataMutationResult call(MetaDataProtocol instance) throws IOException {
                    return instance.dropColumn(tableMetaData);
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
        PhoenixConnection metaConnection = new PhoenixConnection(this, oldMetaConnection.getURL(), props, oldMetaConnection.getPMetaData());
        SQLException sqlE = null;
        try {
            metaConnection.createStatement().executeUpdate("ALTER TABLE " + PhoenixDatabaseMetaData.TYPE_SCHEMA_AND_TABLE + " ADD IF NOT EXISTS " + columns );
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
        props = new Properties(props);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP));
        PhoenixConnection metaConnection = new PhoenixConnection(this, url, props, PMetaDataImpl.EMPTY_META_DATA);
        SQLException sqlE = null;
        try {
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
        } catch (SQLException e) {
            sqlE = e;
        } finally {
            try {
                metaConnection.close();
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
            HTableInterface htable = this.getTable(PhoenixDatabaseMetaData.TYPE_TABLE_NAME_BYTES);
            try {
                htable.coprocessorExec(MetaDataProtocol.class, HConstants.EMPTY_START_ROW,
                        HConstants.EMPTY_END_ROW, new Batch.Call<MetaDataProtocol, Void>() {
                    @Override
                    public Void call(MetaDataProtocol instance) throws IOException {
                      instance.clearCache();
                      return null;
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
                new Batch.Call<MetaDataProtocol, MetaDataMutationResult>() {
                    @Override
                    public MetaDataMutationResult call(MetaDataProtocol instance) throws IOException {
                      return instance.updateIndexState(tableMetaData);
                    }
                });
    }

    @Override
    public long createSequence(String tenantId, String schemaName, String sequenceName, long startWith, long incrementBy, int cacheSize, long timestamp) 
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
     * @return
     * @throws SQLException if cached sequence cannot be found 
     */
    @Override
    public long getSequenceValue(SequenceKey sequenceKey, long timestamp) throws SQLException {
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
     * Verifies that sequences exist and reserves values for them.
     * @param tenantId
     * @param sequences
     * @param timestamp
     * @throws SQLException if any sequence does not exist
     */
    @Override
    public void reserveSequenceValues(List<SequenceKey> sequenceKeys, long timestamp, long[] values, SQLException[] exceptions) throws SQLException {
        incrementSequenceValues(sequenceKeys, timestamp, values, exceptions, 0);
    }
    
    /**
     * Increment any of the set of sequences that need more values. These are the sequences
     * that are asking for the next value within a given statement. The returned sequences
     * are the ones that were not found because they were deleted by another client. 
     * @param tenantId
     * @param sequenceKeys sorted list of sequence kyes
     * @param batchSize
     * @param timestamp
     * @return
     * @throws SQLException if any of the sequences cannot be found
     * 
     * PSequences -> Sequence
     * PSequenceKey -> SequenceKey
     */
    @Override
    public void incrementSequenceValues(List<SequenceKey> sequenceKeys, long timestamp, long[] values, SQLException[] exceptions) throws SQLException {
        incrementSequenceValues(sequenceKeys, timestamp, values, exceptions, 1);
    }

    private void incrementSequenceValues(List<SequenceKey> keys, long timestamp, long[] values, SQLException[] exceptions, int factor) throws SQLException {
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
                    values[i] = sequence.incrementValue(timestamp, factor);
                } catch (EmptySequenceCacheException e) {
                    indexes[toIncrementList.size()] = i;
                    toIncrementList.add(sequence);
                    Increment inc = sequence.newIncrement(timestamp);
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

    @Override
    public void returnSequenceValues(List<SequenceKey> keys, long timestamp, SQLException[] exceptions) throws SQLException {
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
    private void returnAllSequenceValues(ConcurrentMap<SequenceKey,Sequence> sequenceMap) throws SQLException {
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
            returnAllSequenceValues(formerSequenceMap);
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
}
