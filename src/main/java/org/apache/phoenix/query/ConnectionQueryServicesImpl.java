/*
 * Copyright 2010 The Apache Software Foundation
 *
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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_COUNT_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TABLE_NAME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_TYPE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TYPE_TABLE_NAME_BYTES;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_DROP_METADATA;
import static org.apache.phoenix.util.SchemaUtil.getVarChars;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hbase.index.Indexer;
import org.apache.hbase.index.covered.CoveredColumnsIndexBuilder;
import org.apache.phoenix.compile.MutationPlan;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.coprocessor.GroupedAggregateRegionObserver;
import org.apache.phoenix.coprocessor.MetaDataEndpointImpl;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MutationCode;
import org.apache.phoenix.coprocessor.MetaDataRegionObserver;
import org.apache.phoenix.coprocessor.ScanRegionObserver;
import org.apache.phoenix.coprocessor.ServerCachingEndpointImpl;
import org.apache.phoenix.coprocessor.UngroupedAggregateRegionObserver;
import org.apache.phoenix.exception.PhoenixIOException;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.index.PhoenixIndexBuilder;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver.ConnectionInfo;
import org.apache.phoenix.join.HashJoiningRegionObserver;
import org.apache.phoenix.schema.MetaDataSplitPolicy;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PMetaData;
import org.apache.phoenix.schema.PMetaDataImpl;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.ReadOnlyTableException;
import org.apache.phoenix.schema.TableAlreadyExistsException;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.IndexUtil;
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
            this.connection = HConnectionManager.createConnection(this.config);
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
    }

    @Override
    public StatsManager getStatsManager() {
        return this.statsManager;
    }
    
    @Override
    public HTableInterface getTable(byte[] tableName) throws SQLException {
        try {
            return HBaseFactoryProvider.getHTableFactory().getTable(tableName, connection, getExecutor());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public HTableDescriptor getTableDescriptor(byte[] tableName) throws SQLException {
        try {
            return getTable(tableName).getTableDescriptor();
        } catch (IOException e) {
            throw new RuntimeException(e);
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
                        .setRootCause(e).build().buildException();
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

    private static boolean hasNewerTables(long scn, PMetaData metaData) {
        for (PTable table : metaData.getTables().values()) {
            if (table.getTimeStamp() >= scn) {
                return true;
            }
        }
        return false;
    }
    
    private static PMetaData pruneNewerTables(long scn, PMetaData metaData) {
        if (!hasNewerTables(scn, metaData)) {
            return metaData;
        }
        Map<String,PTable> newTables = Maps.newHashMap(metaData.getTables());
        Iterator<Map.Entry<String, PTable>> iterator = newTables.entrySet().iterator();
        boolean wasModified = false;
        while (iterator.hasNext()) {
            if (iterator.next().getValue().getTimeStamp() >= scn) {
                iterator.remove();
                wasModified = true;
            }
        }
        if (wasModified) {
            return new PMetaDataImpl(newTables);
        }
        return metaData;
    }
    
    @Override
    public PhoenixConnection connect(String url, Properties info) throws SQLException {
        Long scn = JDBCUtil.getCurrentSCN(url, info);
        PMetaData metaData = scn == null ? latestMetaData : pruneNewerTables(scn, latestMetaData);
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
      hcd.setKeepDeletedCells(true);
    }
    
    private static final String OLD_PACKAGE = "com.salesforce.";
    private static final String NEW_PACKAGE = "org.apache.";
    
    private HTableDescriptor generateTableDescriptor(byte[] tableName, HTableDescriptor existingDesc, PTableType tableType, Map<String,Object> tableProps, List<Pair<byte[],Map<String,Object>>> families, byte[][] splits) throws SQLException {
        HTableDescriptor descriptor = (existingDesc != null) ? new HTableDescriptor(existingDesc) : new HTableDescriptor(tableName);
        for (Entry<String,Object> entry : tableProps.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            descriptor.setValue(key, value == null ? null : value.toString());
        }
        if (families.isEmpty()) {
            if (tableType != PTableType.VIEW) {
                // Add dummy column family so we have key values for tables that 
                HColumnDescriptor columnDescriptor = generateColumnFamilyDescriptor(new Pair<byte[],Map<String,Object>>(QueryConstants.EMPTY_COLUMN_BYTES,Collections.<String,Object>emptyMap()), tableType);
                descriptor.addFamily(columnDescriptor);
            }
        } else {
            for (Pair<byte[],Map<String,Object>> family : families) {
                // If family is only in phoenix description, add it. otherwise, modify its property accordingly.
                byte[] familyByte = family.getFirst();
                if (descriptor.getFamily(familyByte) == null) {
                    if (tableType == PTableType.VIEW) {
                        throw new ReadOnlyTableException("The HBase column families for a VIEW must already exist(" + Bytes.toStringBinary(familyByte) + ")");
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
                descriptor.removeCoprocessor(ScanRegionObserver.class.getName().replace(NEW_PACKAGE, OLD_PACKAGE));
                descriptor.addCoprocessor(ScanRegionObserver.class.getName(), null, 1, null);
            }
            if (!descriptor.hasCoprocessor(UngroupedAggregateRegionObserver.class.getName())) {
                descriptor.removeCoprocessor(UngroupedAggregateRegionObserver.class.getName().replace(NEW_PACKAGE, OLD_PACKAGE));
                descriptor.addCoprocessor(UngroupedAggregateRegionObserver.class.getName(), null, 1, null);
            }
            if (!descriptor.hasCoprocessor(GroupedAggregateRegionObserver.class.getName())) {
                descriptor.removeCoprocessor(GroupedAggregateRegionObserver.class.getName().replace(NEW_PACKAGE, OLD_PACKAGE));
                descriptor.addCoprocessor(GroupedAggregateRegionObserver.class.getName(), null, 1, null);
            }
            if (!descriptor.hasCoprocessor(HashJoiningRegionObserver.class.getName())) {
                descriptor.removeCoprocessor(HashJoiningRegionObserver.class.getName().replace(NEW_PACKAGE, OLD_PACKAGE));
                descriptor.addCoprocessor(HashJoiningRegionObserver.class.getName(), null, 1, null);
            }
            if (!descriptor.hasCoprocessor(ServerCachingEndpointImpl.class.getName())) {
                descriptor.removeCoprocessor(ServerCachingEndpointImpl.class.getName().replace(NEW_PACKAGE, OLD_PACKAGE));
                descriptor.addCoprocessor(ServerCachingEndpointImpl.class.getName(), null, 1, null);
            }
            // TODO: better encapsulation for this
            // Since indexes can't have indexes, don't install our indexing coprocessor for indexes. Also,
            // don't install on the metadata table until we fix the TODO there.
            if (tableType != PTableType.INDEX && !descriptor.hasCoprocessor(Indexer.class.getName())
                  && !SchemaUtil.isMetaTable(tableName)) {
                Map<String, String> opts = Maps.newHashMapWithExpectedSize(1);
                opts.put(CoveredColumnsIndexBuilder.CODEC_CLASS_NAME_KEY, PhoenixIndexCodec.class.getName());
                Indexer.enableIndexing(descriptor, PhoenixIndexBuilder.class, opts);
            }
            
            // Setup split policy on Phoenix metadata table to ensure that the key values of a Phoenix table
            // stay on the same region.
            if (SchemaUtil.isMetaTable(tableName)) {
                descriptor.setValue(SchemaUtil.UPGRADE_TO_2_0, Boolean.TRUE.toString());
                descriptor.setValue(SchemaUtil.UPGRADE_TO_2_1, Boolean.TRUE.toString());
                descriptor.setValue(SchemaUtil.UPGRADE_TO_2_2, Boolean.TRUE.toString());
                if (!descriptor.hasCoprocessor(MetaDataEndpointImpl.class.getName())) {
                    descriptor.removeCoprocessor(MetaDataEndpointImpl.class.getName().replace(NEW_PACKAGE, OLD_PACKAGE));
                    descriptor.addCoprocessor(MetaDataEndpointImpl.class.getName(), null, 1, null);
                }
                if (!descriptor.hasCoprocessor(MetaDataRegionObserver.class.getName())) {
                    descriptor.removeCoprocessor(MetaDataRegionObserver.class.getName().replace(NEW_PACKAGE, OLD_PACKAGE));
                    descriptor.addCoprocessor(MetaDataRegionObserver.class.getName(), null, 2, null);
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
                        throw new ReadOnlyTableException("The HBase column families for a read-only table must already exist(" + Bytes.toStringBinary(family.getFirst()) + ")");
                    }
                    columnDescriptor = generateColumnFamilyDescriptor(family, tableType );
                } else {
                    columnDescriptor = new HColumnDescriptor(oldDescriptor);
                    if (tableType != PTableType.VIEW) {
                        modifyColumnFamilyDescriptor(columnDescriptor, family);
                    }
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
     * @param familyNames
     * @param splits
     * @return true if table was created and false if it already exists
     * @throws SQLException
     */
    private boolean ensureTableCreated(byte[] tableName, PTableType tableType , Map<String,Object> props, List<Pair<byte[],Map<String,Object>>> families, byte[][] splits) throws SQLException {
        HBaseAdmin admin = null;
        SQLException sqlE = null;
        HTableDescriptor existingDesc = null;
        boolean isMetaTable = SchemaUtil.isMetaTable(tableName);
        boolean tableExist = true;
        try {
            admin = new HBaseAdmin(config);
            try {
                existingDesc = admin.getTableDescriptor(tableName);
            } catch (org.apache.hadoop.hbase.TableNotFoundException e) {
                tableExist = false;
                if (tableType == PTableType.VIEW) {
                    throw new ReadOnlyTableException("An HBase table for a VIEW must already exist(" + Bytes.toString(tableName) + ")");
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
                    return false;
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
                return true;
            } else {
                if (existingDesc.equals(newDesc)) {
                    // Table is already created. Note that the presplits are ignored in this case
                    if (isMetaTable) {
                        checkClientServerCompatibility();
                    }
                    return false;
                }

                boolean updateTo2_0 = false;
                boolean updateTo1_2 = false;
                boolean updateTo2_1 = false;
                boolean updateTo2_2 = false;
                if (isMetaTable) {
                    /*
                     *  FIXME: remove this once everyone has been upgraded to v 0.94.4+
                     *  This hack checks to see if we've got "phoenix.jar" specified as
                     *  the jar file path. We need to set this to null in this case due
                     *  to a change in behavior of HBase.
                     */
                    String value = existingDesc.getValue("coprocessor$1");
                    updateTo1_2 = (value != null && value.startsWith("phoenix.jar"));
                    if (!updateTo1_2) {
                        checkClientServerCompatibility();
                    }
                    
                    updateTo2_0 = existingDesc.getValue(SchemaUtil.UPGRADE_TO_2_0) == null;
                    updateTo2_1 = existingDesc.getValue(SchemaUtil.UPGRADE_TO_2_1) == null;
                    updateTo2_2 = existingDesc.getValue(SchemaUtil.UPGRADE_TO_2_2) == null;
                }
                
                // We'll do this alter at the end of the upgrade
                if (!updateTo2_1 && !updateTo2_0) {
                    // Update metadata of table
                    // TODO: Take advantage of online schema change ability by setting "hbase.online.schema.update.enable" to true
                    admin.disableTable(tableName);
                    // TODO: What if not all existing column families are present?
                    admin.modifyTable(tableName, newDesc);
                    admin.enableTable(tableName);
                }
                /*
                 *  FIXME: remove this once everyone has been upgraded to v 0.94.4+
                 * We've detected that the SYSTEM.TABLE needs to be upgraded, so let's
                 * query and update all tables here.
                 */
                if (updateTo1_2) {
                    upgradeTablesFrom0_94_2to0_94_4(admin);
                    // Do the compatibility check here, now that the jar path has been corrected.
                    // This will work with the new and the old jar, so do the compatibility check now.
                    checkClientServerCompatibility();
                }
                if (updateTo2_1 && !updateTo2_0) {
                    upgradeTablesFrom2_0to2_1(admin, newDesc);
                }
                if (updateTo2_2) {
                    upgradeTo2_2(admin);
                }
                return false;
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
        return true; // will never make it here
    }

    /**
     * FIXME: Temporary code to convert tables to 0.94.4 format (i.e. no jar specified
     * in coprocessor definition). This is necessary because of a change in
     * HBase behavior between 0.94.3 and 0.94.4. Once everyone has been upgraded
     * this code can be removed.
     * @throws SQLException
     */
    private void upgradeTablesFrom0_94_2to0_94_4(HBaseAdmin admin) throws IOException {
        if (logger.isInfoEnabled()) {
            logger.info("Upgrading tables from HBase 0.94.2 to 0.94.4+");
        }
        /* Use regular HBase scan instead of query because the jar on the server may
         * not be compatible (we don't know yet) and this is our one chance to do
         * the conversion automatically.
         */
        Scan scan = new Scan();
        scan.addColumn(TABLE_FAMILY_BYTES, COLUMN_COUNT_BYTES);
        // Add filter so that we only get the table row and not the column rows
        scan.setFilter(new SingleColumnValueFilter(TABLE_FAMILY_BYTES, COLUMN_COUNT_BYTES, CompareOp.GREATER_OR_EQUAL, PDataType.INTEGER.toBytes(0)));
        HTableInterface table = HBaseFactoryProvider.getHTableFactory().getTable(TYPE_TABLE_NAME_BYTES, connection, getExecutor());
        ResultScanner scanner = table.getScanner(scan);
        Result result = null;
        while ((result = scanner.next()) != null) {
            byte[] rowKey = result.getRow();
            byte[][] rowKeyMetaData = new byte[2][];
            getVarChars(rowKey, rowKeyMetaData);
            byte[] schemaBytes = rowKeyMetaData[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
            byte[] tableBytes = rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
            byte[] tableName = SchemaUtil.getTableNameAsBytes(schemaBytes, tableBytes);
            if (!SchemaUtil.isMetaTable(tableName)) {
                try {
                    HTableDescriptor existingDesc = admin.getTableDescriptor(tableName);
                    existingDesc.removeCoprocessor(ScanRegionObserver.class.getName());
                    existingDesc.removeCoprocessor(UngroupedAggregateRegionObserver.class.getName());
                    existingDesc.removeCoprocessor(GroupedAggregateRegionObserver.class.getName());
                    existingDesc.removeCoprocessor(HashJoiningRegionObserver.class.getName());
                    existingDesc.addCoprocessor(ScanRegionObserver.class.getName(), null, 1, null);
                    existingDesc.addCoprocessor(UngroupedAggregateRegionObserver.class.getName(), null, 1, null);
                    existingDesc.addCoprocessor(GroupedAggregateRegionObserver.class.getName(), null, 1, null);
                    existingDesc.addCoprocessor(HashJoiningRegionObserver.class.getName(), null, 1, null);
                    boolean wasEnabled = admin.isTableEnabled(tableName);
                    if (wasEnabled) {
                        admin.disableTable(tableName);
                    }
                    admin.modifyTable(tableName, existingDesc);
                    if (wasEnabled) {
                        admin.enableTable(tableName);
                    }
                } catch (org.apache.hadoop.hbase.TableNotFoundException e) {
                    logger.error("Unable to convert " + Bytes.toString(tableName), e);
                } catch (IOException e) {
                    logger.error("Unable to convert " + Bytes.toString(tableName), e);
                }
            }
        }
    }

    /**
     * Walk through all existing tables and install new coprocessors
     * @param admin
     * @throws IOException
     * @throws SQLException
     */
    private void upgradeTo2_2(HBaseAdmin admin) throws IOException, SQLException {
        if (logger.isInfoEnabled()) {
            logger.info("Upgrading tables from Phoenix 2.x to Apache Phoenix 2.2.3");
        }
        /* Use regular HBase scan instead of query because the jar on the server may
         * not be compatible (we don't know yet) and this is our one chance to do
         * the conversion automatically.
         */
        Scan scan = new Scan();
        scan.addColumn(TABLE_FAMILY_BYTES, TABLE_TYPE_BYTES);
        scan.addColumn(TABLE_FAMILY_BYTES, DATA_TABLE_NAME_BYTES);
        SingleColumnValueFilter filter = new SingleColumnValueFilter(TABLE_FAMILY_BYTES, TABLE_TYPE_BYTES, CompareOp.GREATER_OR_EQUAL, PDataType.CHAR.toBytes("a"));
        filter.setFilterIfMissing(true);
        // Add filter so that we only get the table row and not the column rows
        scan.setFilter(filter);
        HTableInterface table = HBaseFactoryProvider.getHTableFactory().getTable(TYPE_TABLE_NAME_BYTES, connection, getExecutor());
        ResultScanner scanner = table.getScanner(scan);
        Result result = null;
        while ((result = scanner.next()) != null) {
            byte[] rowKey = result.getRow();
            byte[][] rowKeyMetaData = new byte[2][];
            getVarChars(rowKey, rowKeyMetaData);
            byte[] schemaBytes = rowKeyMetaData[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
            byte[] tableBytes = rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
            byte[] tableName = SchemaUtil.getTableNameAsBytes(schemaBytes, tableBytes);
            if (!SchemaUtil.isMetaTable(tableName)) {
                HTableDescriptor existingDesc = admin.getTableDescriptor(tableName);
                HTableDescriptor newDesc = generateTableDescriptor(tableName, existingDesc, PTableType.VIEW, Collections.<String,Object>emptyMap(), Collections.<Pair<byte[],Map<String,Object>>>emptyList(), null);
                admin.disableTable(tableName);
                admin.modifyTable(tableName, newDesc);
                admin.enableTable(tableName);
            }
        }
    }
        
    /**
     * FIXME: Temporary code to convert tables from 2.0 to 2.1 by:
     * 1) adding the new coprocessors for mutable secondary indexing
     * 2) add a ":" prefix to any index column names that are for data table pk columns
     * @param newDesc 
     * @throws IOException
     */
    private void upgradeTablesFrom2_0to2_1(HBaseAdmin admin, HTableDescriptor newDesc) throws IOException {
        if (logger.isInfoEnabled()) {
            logger.info("Upgrading tables from Phoenix 2.0 to Phoenix 2.1");
        }
        /* Use regular HBase scan instead of query because the jar on the server may
         * not be compatible (we don't know yet) and this is our one chance to do
         * the conversion automatically.
         */
        Scan scan = new Scan();
        scan.addColumn(TABLE_FAMILY_BYTES, TABLE_TYPE_BYTES);
        scan.addColumn(TABLE_FAMILY_BYTES, DATA_TABLE_NAME_BYTES);
        SingleColumnValueFilter filter = new SingleColumnValueFilter(TABLE_FAMILY_BYTES, TABLE_TYPE_BYTES, CompareOp.GREATER_OR_EQUAL, PDataType.CHAR.toBytes("a"));
        filter.setFilterIfMissing(true);
        // Add filter so that we only get the table row and not the column rows
        scan.setFilter(filter);
        HTableInterface table = HBaseFactoryProvider.getHTableFactory().getTable(TYPE_TABLE_NAME_BYTES, connection, getExecutor());
        ResultScanner scanner = table.getScanner(scan);
        Result result = null;
        List<byte[][]> indexesToUpdate = Lists.newArrayList();
        while ((result = scanner.next()) != null) {
            boolean wasModified = false;
            byte[] rowKey = result.getRow();
            byte[][] rowKeyMetaData = new byte[2][];
            getVarChars(rowKey, rowKeyMetaData);
            byte[] schemaBytes = rowKeyMetaData[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
            byte[] tableBytes = rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
            byte[] tableName = SchemaUtil.getTableNameAsBytes(schemaBytes, tableBytes);
            KeyValue kv = result.getColumnLatest(TABLE_FAMILY_BYTES, TABLE_TYPE_BYTES);
            PTableType tableType = PTableType.fromSerializedValue(kv.getBuffer()[kv.getValueOffset()]);
            if (!SchemaUtil.isMetaTable(tableName)) {
                try {
                    HTableDescriptor existingDesc = admin.getTableDescriptor(tableName);
                    if (!existingDesc.hasCoprocessor(ServerCachingEndpointImpl.class.getName())) {
                        existingDesc.addCoprocessor(ServerCachingEndpointImpl.class.getName(), null, 1, null);
                        wasModified = true;
                    }
                    if (tableType == PTableType.INDEX) {
                        byte[] dataTableName = result.getColumnLatest(TABLE_FAMILY_BYTES, DATA_TABLE_NAME_BYTES).getValue();
                        if (logger.isInfoEnabled()) {
                            logger.info("Detected index " + SchemaUtil.getTableName(schemaBytes, dataTableName) + " that needs to be upgraded" );
                        }
                        indexesToUpdate.add(new byte[][] {schemaBytes, tableBytes, dataTableName});
                    } else if (!existingDesc.hasCoprocessor(Indexer.class.getName())) {
                        Map<String, String> opts = Maps.newHashMapWithExpectedSize(1);
                        opts.put(CoveredColumnsIndexBuilder.CODEC_CLASS_NAME_KEY, PhoenixIndexCodec.class.getName());
                        Indexer.enableIndexing(existingDesc, PhoenixIndexBuilder.class, opts);
                        wasModified = true;
                    }
                    if (wasModified) {
                        boolean wasEnabled = admin.isTableEnabled(tableName);
                        if (wasEnabled) {
                            admin.disableTable(tableName);
                        }
                        admin.modifyTable(tableName, existingDesc);
                        if (wasEnabled) {
                            admin.enableTable(tableName);
                        }
                    }
                } catch (org.apache.hadoop.hbase.TableNotFoundException e) {
                    logger.error("Unable to convert " + Bytes.toString(tableName), e);
                } catch (IOException e) {
                    logger.error("Unable to convert " + Bytes.toString(tableName), e);
                }
            }
        }
        
        List<Mutation> mutations = Lists.newArrayList();
        for (byte[][] indexInfo : indexesToUpdate) {
            scan = new Scan();
            scan.addFamily(TABLE_FAMILY_BYTES);
            byte[] startRow = ByteUtil.concat(indexInfo[0],QueryConstants.SEPARATOR_BYTE_ARRAY, indexInfo[2], QueryConstants.SEPARATOR_BYTE_ARRAY);
            byte[] stopRow = ByteUtil.nextKey(startRow);
            scan.setStartRow(startRow);
            scan.setStopRow(stopRow);
            scan.setFilter(new FirstKeyOnlyFilter());
            scanner = table.getScanner(scan);
            List<KeyRange> indexRowsToUpdate = Lists.newArrayList();
            while ((result = scanner.next()) != null) {
                byte[] rowKey = result.getRow();
                byte[][] rowKeyMetaData = new byte[4][];
                getVarChars(rowKey, rowKeyMetaData);
                byte[] columnBytes = rowKeyMetaData[PhoenixDatabaseMetaData.COLUMN_NAME_INDEX];
                byte[] familyBytes = rowKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX];
                if (familyBytes == null || familyBytes.length == 0) {
                    byte[] indexRowKey = ByteUtil.concat(indexInfo[0],QueryConstants.SEPARATOR_BYTE_ARRAY, indexInfo[1], QueryConstants.SEPARATOR_BYTE_ARRAY, columnBytes);
                    KeyRange keyRange = PDataType.VARBINARY.getKeyRange(indexRowKey, true, indexRowKey, true);
                    indexRowsToUpdate.add(keyRange);
                }
            }
            ScanRanges ranges = ScanRanges.create(Collections.singletonList(indexRowsToUpdate), SchemaUtil.VAR_BINARY_SCHEMA);
            Scan indexScan = new Scan();
            scan.addFamily(TABLE_FAMILY_BYTES);
            indexScan.setFilter(ranges.getSkipScanFilter());
            ResultScanner indexScanner = table.getScanner(indexScan);
            while ((result = indexScanner.next()) != null) {
                byte[] rowKey = result.getRow();
                byte[][] rowKeyMetaData = new byte[3][];
                getVarChars(rowKey, rowKeyMetaData);
                byte[] schemaBytes = rowKeyMetaData[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
                byte[] tableBytes = rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
                byte[] columnBytes = rowKeyMetaData[PhoenixDatabaseMetaData.COLUMN_NAME_INDEX];
                Delete del = new Delete(rowKey);
                // All this to add a ':' to the beginning of the column name...
                byte[] newRowKey = ByteUtil.concat(schemaBytes,QueryConstants.SEPARATOR_BYTE_ARRAY, tableBytes, QueryConstants.SEPARATOR_BYTE_ARRAY, IndexUtil.INDEX_COLUMN_NAME_SEP_BYTES, columnBytes);
                Put put = new Put(newRowKey);
                for (KeyValue kv : result.raw()) {
                    byte[] cf = kv.getFamily();
                    byte[] cq= kv.getQualifier();
                    long ts = kv.getTimestamp();
                    del.deleteColumn(cf, cq, ts);
                    put.add(cf,cq,ts,kv.getValue());
                }
                mutations.add(del);
                mutations.add(put);
            }
        }
        if (!mutations.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("Performing batched mutation of " + mutations.size() + " index column rows to update" );
            }
            try {
                table.batch(mutations);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
        
        // Finally, at the end, modify the system table
        // which will include the flag that'll prevent this
        // update from occurring again
        admin.disableTable(TYPE_TABLE_NAME_BYTES);
        admin.modifyTable(TYPE_TABLE_NAME_BYTES, newDesc);
        admin.enableTable(TYPE_TABLE_NAME_BYTES);
        if (logger.isInfoEnabled()) {
            logger.info("Upgrade to 2.1.0 completed successfully" );
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

    @Override
    public MetaDataMutationResult createTable(final List<Mutation> tableMetaData, PTableType tableType, Map<String,Object> tableProps,
            final List<Pair<byte[],Map<String,Object>>> families, byte[][] splits) throws SQLException {
        byte[][] rowKeyMetadata = new byte[2][];
        Mutation m = tableMetaData.get(0);
        byte[] key = m.getRow();
        SchemaUtil.getVarChars(key, rowKeyMetadata);
        byte[] schemaBytes = rowKeyMetadata[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
        byte[] tableBytes = rowKeyMetadata[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
        byte[] tableName = SchemaUtil.getTableNameAsBytes(schemaBytes, tableBytes);
        ensureTableCreated(tableName, tableType, tableProps, families, splits);

        byte[] tableKey = SchemaUtil.getTableKey(schemaBytes, tableBytes);
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
    public MetaDataMutationResult getTable(final byte[] schemaBytes, final byte[] tableBytes,
            final long tableTimestamp, final long clientTimestamp) throws SQLException {
        byte[] tableKey = SchemaUtil.getTableKey(schemaBytes, tableBytes);
        return metaDataCoprocessorExec(tableKey,
                new Batch.Call<MetaDataProtocol, MetaDataMutationResult>() {
                    @Override
                    public MetaDataMutationResult call(MetaDataProtocol instance) throws IOException {
                      return instance.getTable(schemaBytes, tableBytes, tableTimestamp, clientTimestamp);
                    }
                });
    }

    @Override
    public MetaDataMutationResult dropTable(final List<Mutation> tableMetaData, final PTableType tableType) throws SQLException {
        byte[][] rowKeyMetadata = new byte[2][];
        SchemaUtil.getVarChars(tableMetaData.get(0).getRow(), rowKeyMetadata);
        byte[] tableKey = SchemaUtil.getTableKey(rowKeyMetadata[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX], rowKeyMetadata[PhoenixDatabaseMetaData.TABLE_NAME_INDEX]);
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

    @Override
    public MetaDataMutationResult addColumn(final List<Mutation> tableMetaData, PTableType tableType, List<Pair<byte[],Map<String,Object>>> families) throws SQLException {
        byte[][] rowKeyMetaData = new byte[2][];
        byte[] rowKey = tableMetaData.get(0).getRow();
        SchemaUtil.getVarChars(rowKey, rowKeyMetaData);
        byte[] schemaBytes = rowKeyMetaData[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
        byte[] tableBytes = rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
        byte[] tableKey = SchemaUtil.getTableKey(schemaBytes, tableBytes);
        byte[] tableName = SchemaUtil.getTableNameAsBytes(schemaBytes, tableBytes);
        for ( Pair<byte[],Map<String,Object>>  family : families) {
            
            ensureFamilyCreated(tableName, tableType, family);
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
        return result;
    }

    @Override
    public MetaDataMutationResult dropColumn(final List<Mutation> tableMetaData, PTableType tableType) throws SQLException {
        byte[][] rowKeyMetadata = new byte[2][];
        SchemaUtil.getVarChars(tableMetaData.get(0).getRow(), rowKeyMetadata);
        byte[] schemaBytes = rowKeyMetadata[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
        byte[] tableBytes = rowKeyMetadata[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
        byte[] tableKey = SchemaUtil.getTableKey(schemaBytes, tableBytes);
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

    @Override
    public void init(String url, Properties props) throws SQLException {
        props = new Properties(props);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP));
        PhoenixConnection metaConnection = new PhoenixConnection(this, url, props, PMetaDataImpl.EMPTY_META_DATA);
        SQLException sqlE = null;
        try {
            metaConnection.createStatement().executeUpdate(QueryConstants.CREATE_METADATA);
        } catch (TableAlreadyExistsException e) {
            SchemaUtil.updateSystemTableTo2(metaConnection, e.getTable());
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
        byte[][] rowKeyMetadata = new byte[2][];
        SchemaUtil.getVarChars(tableMetaData.get(0).getRow(), rowKeyMetadata);
        byte[] tableKey = SchemaUtil.getTableKey(rowKeyMetadata[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX], rowKeyMetadata[PhoenixDatabaseMetaData.TABLE_NAME_INDEX]);
        return metaDataCoprocessorExec(tableKey,
                new Batch.Call<MetaDataProtocol, MetaDataMutationResult>() {
                    @Override
                    public MetaDataMutationResult call(MetaDataProtocol instance) throws IOException {
                      return instance.updateIndexState(tableMetaData);
                    }
                });
    }
}
