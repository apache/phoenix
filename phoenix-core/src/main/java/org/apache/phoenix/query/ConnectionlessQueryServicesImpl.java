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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_STATE_BYTES;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.MutationPlan;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MutationCode;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.hbase.index.util.GenericKeyValueBuilder;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver.ConnectionInfo;
import org.apache.phoenix.parse.PFunction;
import org.apache.phoenix.parse.PSchema;
import org.apache.phoenix.schema.FunctionNotFoundException;
import org.apache.phoenix.schema.NewerTableAlreadyExistsException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PMetaData;
import org.apache.phoenix.schema.PMetaDataImpl;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.SchemaNotFoundException;
import org.apache.phoenix.schema.Sequence;
import org.apache.phoenix.schema.SequenceAllocation;
import org.apache.phoenix.schema.SequenceAlreadyExistsException;
import org.apache.phoenix.schema.SequenceInfo;
import org.apache.phoenix.schema.SequenceKey;
import org.apache.phoenix.schema.SequenceNotFoundException;
import org.apache.phoenix.schema.TableAlreadyExistsException;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.stats.GuidePostsInfo;
import org.apache.phoenix.schema.stats.GuidePostsKey;
import org.apache.phoenix.util.JDBCUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.SequenceUtil;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.inmemory.InMemoryTxSystemClient;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 *
 * Implementation of ConnectionQueryServices used in testing where no connection to
 * an hbase cluster is necessary.
 * 
 * 
 * @since 0.1
 */
public class ConnectionlessQueryServicesImpl extends DelegateQueryServices implements ConnectionQueryServices  {
    private static ServerName SERVER_NAME = ServerName.parseServerName(HConstants.LOCALHOST + Addressing.HOSTNAME_PORT_SEPARATOR + HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT);
    
    private PMetaData metaData;
    private final Map<SequenceKey, SequenceInfo> sequenceMap = Maps.newHashMap();
    private final String userName;
    private final TransactionSystemClient txSystemClient;
    private KeyValueBuilder kvBuilder;
    private volatile boolean initialized;
    private volatile SQLException initializationException;
    private final Map<String, List<HRegionLocation>> tableSplits = Maps.newHashMap();
    private final GuidePostsCache guidePostsCache;
    private final Configuration config;
    
    public ConnectionlessQueryServicesImpl(QueryServices services, ConnectionInfo connInfo, Properties info) {
        super(services);
        userName = connInfo.getPrincipal();
        metaData = newEmptyMetaData();
        
        // Use KeyValueBuilder that builds real KeyValues, as our test utils require this
        this.kvBuilder = GenericKeyValueBuilder.INSTANCE;
        Configuration config = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
        for (Entry<String,String> entry : services.getProps()) {
            config.set(entry.getKey(), entry.getValue());
        }
        if (info != null) {
            for (Object key : info.keySet()) {
                config.set((String) key, info.getProperty((String) key));
            }
        }
        for (Entry<String,String> entry : connInfo.asProps()) {
            config.set(entry.getKey(), entry.getValue());
        }

        // Without making a copy of the configuration we cons up, we lose some of our properties
        // on the server side during testing.
        this.config = HBaseFactoryProvider.getConfigurationFactory().getConfiguration(config);
        TransactionManager txnManager = new TransactionManager(config);
        this.txSystemClient = new InMemoryTxSystemClient(txnManager);
        this.guidePostsCache = new GuidePostsCache(this, config);
    }

    private PMetaData newEmptyMetaData() {
        return new PMetaDataImpl(INITIAL_META_DATA_TABLE_CAPACITY, getProps());
    }

    @Override
    public ConnectionQueryServices getChildQueryServices(ImmutableBytesWritable childId) {
        return this; // Just reuse the same query services
    }

    @Override
    public HTableInterface getTable(byte[] tableName) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<HRegionLocation> getAllTableRegions(byte[] tableName) throws SQLException {
        List<HRegionLocation> regions = tableSplits.get(Bytes.toString(tableName));
        if (regions != null) {
            return regions;
        }
        return Collections.singletonList(new HRegionLocation(
            new HRegionInfo(TableName.valueOf(tableName), HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW),
            SERVER_NAME, -1));
    }

    @Override
    public void addTable(PTable table, long resolvedTime) throws SQLException {
        metaData.addTable(table, resolvedTime);
    }
    
    @Override
    public void updateResolvedTimestamp(PTable table, long resolvedTimestamp) throws SQLException {
        metaData.updateResolvedTimestamp(table, resolvedTimestamp);
    }

    @Override
    public void removeTable(PName tenantId, String tableName, String parentTableName, long tableTimeStamp)
            throws SQLException {
        metaData.removeTable(tenantId, tableName, parentTableName, tableTimeStamp);
    }

    @Override
    public void removeColumn(PName tenantId, String tableName, List<PColumn> columnsToRemove, long tableTimeStamp,
            long tableSeqNum, long resolvedTime) throws SQLException {
        metaData.removeColumn(tenantId, tableName, columnsToRemove, tableTimeStamp, tableSeqNum, resolvedTime);
    }

    
    @Override
    public PhoenixConnection connect(String url, Properties info) throws SQLException {
        return new PhoenixConnection(this, url, info, metaData.clone());
    }

    @Override
    public MetaDataMutationResult getTable(PName tenantId, byte[] schemaBytes, byte[] tableBytes, long tableTimestamp, long clientTimestamp) throws SQLException {
        // Return result that will cause client to use it's own metadata instead of needing
        // to get anything from the server (since we don't have a connection)
        try {
            String fullTableName = SchemaUtil.getTableName(schemaBytes, tableBytes);
            PTable table = metaData.getTableRef(new PTableKey(tenantId, fullTableName)).getTable();
            return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS, 0, table, true);
        } catch (TableNotFoundException e) {
            return new MetaDataMutationResult(MutationCode.TABLE_NOT_FOUND, 0, null);
        }
    }

    private static byte[] getTableName(List<Mutation> tableMetaData, byte[] physicalTableName) {
        if (physicalTableName != null) {
            return physicalTableName;
        }
        byte[][] rowKeyMetadata = new byte[3][];
        Mutation m = MetaDataUtil.getTableHeaderRow(tableMetaData);
        byte[] key = m.getRow();
        SchemaUtil.getVarChars(key, rowKeyMetadata);
        byte[] schemaBytes = rowKeyMetadata[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
        byte[] tableBytes = rowKeyMetadata[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
        return SchemaUtil.getTableNameAsBytes(schemaBytes, tableBytes);
    }
    
    private static List<HRegionLocation> generateRegionLocations(byte[] physicalName, byte[][] splits) {
        byte[] startKey = HConstants.EMPTY_START_ROW;
        List<HRegionLocation> regions = Lists.newArrayListWithExpectedSize(splits.length);
        for (byte[] split : splits) {
            regions.add(new HRegionLocation(
                    new HRegionInfo(TableName.valueOf(physicalName), startKey, split),
                    SERVER_NAME, -1));
            startKey = split;
        }
        regions.add(new HRegionLocation(
                new HRegionInfo(TableName.valueOf(physicalName), startKey, HConstants.EMPTY_END_ROW),
                SERVER_NAME, -1));
        return regions;
    }
    
    @Override
    public MetaDataMutationResult createTable(List<Mutation> tableMetaData, byte[] physicalName, PTableType tableType,
            Map<String, Object> tableProps, List<Pair<byte[], Map<String, Object>>> families, byte[][] splits,
            boolean isNamespaceMapped, boolean allocateIndexId) throws SQLException {
        if (splits != null) {
            byte[] tableName = getTableName(tableMetaData, physicalName);
            tableSplits.put(Bytes.toString(tableName), generateRegionLocations(tableName, splits));
        }
        if (!allocateIndexId) {
            return new MetaDataMutationResult(MutationCode.TABLE_NOT_FOUND, 0, null);
        } else {
            return new MetaDataMutationResult(MutationCode.TABLE_NOT_FOUND, 0, null, Short.MIN_VALUE);
        }
    }

    @Override
    public MetaDataMutationResult dropTable(List<Mutation> tableMetadata, PTableType tableType, boolean cascade) throws SQLException {
        byte[] tableName = getTableName(tableMetadata, null);
        tableSplits.remove(Bytes.toString(tableName));
        return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS, 0, null);
    }

    @Override
    public MetaDataMutationResult addColumn(List<Mutation> tableMetaData, PTable table, Map<String, List<Pair<String,Object>>> properties, Set<String> colFamiliesForPColumnsToBeAdded, List<PColumn> columnsToBeAdded) throws SQLException {
        List<PColumn> columns = Lists.newArrayList(table.getColumns());
        columns.addAll(columnsToBeAdded);
        return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS, 0, PTableImpl.makePTable(table, columns));
    }

    @Override
    public MetaDataMutationResult dropColumn(List<Mutation> tableMetadata, PTableType tableType) throws SQLException {
        return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS, 0, null);
    }
    
    @Override
    public void clearTableFromCache(byte[] tenantId, byte[] schemaName, byte[] tableName, long clientTS)
            throws SQLException {}
    // TODO: share this with ConnectionQueryServicesImpl
    @Override
    public void init(String url, Properties props) throws SQLException {
        if (initialized) {
            if (initializationException != null) {
                throw initializationException;
            }
            return;
        }
        synchronized (this) {
            if (initialized) {
                if (initializationException != null) {
                    throw initializationException;
                }
                return;
            }
            SQLException sqlE = null;
            PhoenixConnection metaConnection = null;
            try {
                Properties scnProps = PropertiesUtil.deepCopy(props);
                scnProps.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP));
                scnProps.remove(PhoenixRuntime.TENANT_ID_ATTRIB);
                String globalUrl = JDBCUtil.removeProperty(url, PhoenixRuntime.TENANT_ID_ATTRIB);
                metaConnection = new PhoenixConnection(this, globalUrl, scnProps, newEmptyMetaData());
                try {
                    metaConnection.createStatement().executeUpdate(QueryConstants.CREATE_TABLE_METADATA);
                } catch (TableAlreadyExistsException ignore) {
                    // Ignore, as this will happen if the SYSTEM.TABLE already exists at this fixed timestamp.
                    // A TableAlreadyExistsException is not thrown, since the table only exists *after* this fixed timestamp.
                }
                try {
                    int nSaltBuckets = getSequenceSaltBuckets();
                    String createTableStatement = Sequence.getCreateTableStatement(nSaltBuckets);
                   metaConnection.createStatement().executeUpdate(createTableStatement);
                } catch (NewerTableAlreadyExistsException ignore) {
                    // Ignore, as this will happen if the SYSTEM.SEQUENCE already exists at this fixed timestamp.
                    // A TableAlreadyExistsException is not thrown, since the table only exists *after* this fixed timestamp.
                }
                try {
                    metaConnection.createStatement().executeUpdate(QueryConstants.CREATE_STATS_TABLE_METADATA);
                } catch (NewerTableAlreadyExistsException ignore) {
                    // Ignore, as this will happen if the SYSTEM.SEQUENCE already exists at this fixed
                    // timestamp.
                    // A TableAlreadyExistsException is not thrown, since the table only exists *after* this
                    // fixed timestamp.
                }
                
                try {
                   metaConnection.createStatement().executeUpdate(QueryConstants.CREATE_FUNCTION_METADATA);
                } catch (NewerTableAlreadyExistsException ignore) {
                }
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
        return new MutationState(0, plan.getContext().getConnection());
    }

    @Override
    public int getLowestClusterHBaseVersion() {
        return Integer.MAX_VALUE; // Allow everything for connectionless
    }

    @Override
    public HBaseAdmin getAdmin() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public MetaDataMutationResult updateIndexState(List<Mutation> tableMetadata, String parentTableName) throws SQLException {
        byte[][] rowKeyMetadata = new byte[3][];
        SchemaUtil.getVarChars(tableMetadata.get(0).getRow(), rowKeyMetadata);
        Mutation m = MetaDataUtil.getTableHeaderRow(tableMetadata);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        if (!MetaDataUtil.getMutationValue(m, INDEX_STATE_BYTES, kvBuilder, ptr)) {
            throw new IllegalStateException();
        }
        PIndexState newState =  PIndexState.fromSerializedValue(ptr.get()[ptr.getOffset()]);
        byte[] tenantIdBytes = rowKeyMetadata[PhoenixDatabaseMetaData.TENANT_ID_INDEX];
        String schemaName = Bytes.toString(rowKeyMetadata[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX]);
        String indexName = Bytes.toString(rowKeyMetadata[PhoenixDatabaseMetaData.TABLE_NAME_INDEX]);
        String indexTableName = SchemaUtil.getTableName(schemaName, indexName);
        PName tenantId = tenantIdBytes.length == 0 ? null : PNameFactory.newName(tenantIdBytes);
        PTable index = metaData.getTableRef(new PTableKey(tenantId, indexTableName)).getTable();
        index = PTableImpl.makePTable(index,newState == PIndexState.USABLE ? PIndexState.ACTIVE : newState == PIndexState.UNUSABLE ? PIndexState.INACTIVE : newState);
        return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS, 0, index);
    }

    @Override
    public HTableDescriptor getTableDescriptor(byte[] tableName) throws SQLException {
        return null;
    }

    @Override
    public void clearTableRegionCache(byte[] tableName) throws SQLException {
    }

    @Override
    public boolean hasIndexWALCodec() {
        return true;
    }

    @Override
    public long createSequence(String tenantId, String schemaName, String sequenceName,
            long startWith, long incrementBy, long cacheSize, long minValue, long maxValue,
            boolean cycle, long timestamp) throws SQLException {
        SequenceKey key = new SequenceKey(tenantId, schemaName, sequenceName, getSequenceSaltBuckets());
        if (sequenceMap.get(key) != null) {
            throw new SequenceAlreadyExistsException(schemaName, sequenceName);
        }
        sequenceMap.put(key, new SequenceInfo(startWith, incrementBy, minValue, maxValue, 1l, cycle)) ;
        return timestamp;
    }

    @Override
    public long dropSequence(String tenantId, String schemaName, String sequenceName, long timestamp) throws SQLException {
        SequenceKey key = new SequenceKey(tenantId, schemaName, sequenceName, getSequenceSaltBuckets());
        if (sequenceMap.remove(key) == null) {
            throw new SequenceNotFoundException(schemaName, sequenceName);
        }
        return timestamp;
    }

    @Override
    public void validateSequences(List<SequenceAllocation> sequenceAllocations, long timestamp,
            long[] values, SQLException[] exceptions, Sequence.ValueOp action) throws SQLException {
        int i = 0;
        for (SequenceAllocation sequenceAllocation : sequenceAllocations) {
            SequenceInfo info = sequenceMap.get(sequenceAllocation.getSequenceKey());
            if (info == null) {
                exceptions[i] = new SequenceNotFoundException(sequenceAllocation.getSequenceKey().getSchemaName(), sequenceAllocation.getSequenceKey().getSequenceName());
            } else {
                values[i] = info.sequenceValue;          
            }
            i++;
        }
    }

    @Override
    public void incrementSequences(List<SequenceAllocation> sequenceAllocations, long timestamp,
            long[] values, SQLException[] exceptions) throws SQLException {
        int i = 0;
		for (SequenceAllocation sequenceAllocation : sequenceAllocations) {
		    SequenceKey key = sequenceAllocation.getSequenceKey();
			SequenceInfo info = sequenceMap.get(key);
			if (info == null) {
				exceptions[i] = new SequenceNotFoundException(
						key.getSchemaName(), key.getSequenceName());
			} else {
				boolean increaseSeq = info.incrementBy > 0;
				if (info.limitReached) {
					SQLExceptionCode code = increaseSeq ? SQLExceptionCode.SEQUENCE_VAL_REACHED_MAX_VALUE
							: SQLExceptionCode.SEQUENCE_VAL_REACHED_MIN_VALUE;
					exceptions[i] = new SQLExceptionInfo.Builder(code).build().buildException();
				} else {
					values[i] = info.sequenceValue;
					info.sequenceValue += info.incrementBy * info.cacheSize;
					info.limitReached = SequenceUtil.checkIfLimitReached(info);
					if (info.limitReached && info.cycle) {
						info.sequenceValue = increaseSeq ? info.minValue : info.maxValue;
						info.limitReached = false;
					}
				}
			}
			i++;
		}
        i = 0;
        for (SQLException e : exceptions) {
            if (e != null) {
                sequenceMap.remove(sequenceAllocations.get(i).getSequenceKey());
            }
            i++;
        }
    }

    @Override
    public long currentSequenceValue(SequenceKey sequenceKey, long timestamp) throws SQLException {
        SequenceInfo info = sequenceMap.get(sequenceKey);
        if (info == null) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_CALL_CURRENT_BEFORE_NEXT_VALUE)
            .setSchemaName(sequenceKey.getSchemaName()).setTableName(sequenceKey.getSequenceName())
            .build().buildException();
        }
        return info.sequenceValue;
    }

    @Override
    public void returnSequences(List<SequenceKey> sequenceKeys, long timestamp, SQLException[] exceptions)
            throws SQLException {
    }

    @Override
    public void addConnection(PhoenixConnection connection) throws SQLException {
    }

    @Override
    public void removeConnection(PhoenixConnection connection) throws SQLException {
    }

    @Override
    public KeyValueBuilder getKeyValueBuilder() {
        return this.kvBuilder;
    }

    @Override
    public boolean supportsFeature(Feature feature) {
        return true;
    }

    @Override
    public String getUserName() {
        return userName;
    }

    @Override
    public GuidePostsInfo getTableStats(GuidePostsKey key) {
        GuidePostsInfo info = guidePostsCache.getCache().getIfPresent(key);
        if (null == info) {
          return GuidePostsInfo.NO_GUIDEPOST;
        }
        return info;
    }

    @Override
    public long clearCache() throws SQLException {
        return 0;
    }

    @Override
    public int getSequenceSaltBuckets() {
        return getProps().getInt(QueryServices.SEQUENCE_SALT_BUCKETS_ATTRIB,
                QueryServicesOptions.DEFAULT_SEQUENCE_TABLE_SALT_BUCKETS);
    }

    @Override
    public TransactionSystemClient getTransactionSystemClient() {
        return txSystemClient;
    }
 
    public MetaDataMutationResult createFunction(List<Mutation> functionData, PFunction function, boolean temporary)
            throws SQLException {
        return new MetaDataMutationResult(MutationCode.FUNCTION_NOT_FOUND, 0l, null);
    }

    @Override
    public void addFunction(PFunction function) throws SQLException {
        this.metaData.addFunction(function);
    }

    @Override
    public void removeFunction(PName tenantId, String function, long functionTimeStamp)
            throws SQLException {
        this.metaData.removeFunction(tenantId, function, functionTimeStamp);
    }

    @Override
    public MetaDataMutationResult getFunctions(PName tenantId,
            List<Pair<byte[], Long>> functionNameAndTimeStampPairs, long clientTimestamp)
            throws SQLException {
        List<PFunction> functions = new ArrayList<PFunction>(functionNameAndTimeStampPairs.size());
        for(Pair<byte[], Long> functionInfo: functionNameAndTimeStampPairs) {
            try {
                PFunction function2 = metaData.getFunction(new PTableKey(tenantId, Bytes.toString(functionInfo.getFirst())));
                functions.add(function2);
            } catch (FunctionNotFoundException e) {
                return new MetaDataMutationResult(MutationCode.FUNCTION_NOT_FOUND, 0, null);
            }
        }
        if(functions.isEmpty()) {
            return null;
        }
        return new MetaDataMutationResult(MutationCode.FUNCTION_ALREADY_EXISTS, 0, functions, true);
    }

    @Override
    public MetaDataMutationResult dropFunction(List<Mutation> tableMetadata, boolean ifExists)
            throws SQLException {
        return new MetaDataMutationResult(MutationCode.FUNCTION_ALREADY_EXISTS, 0, null);
    }

    @Override
    public long getRenewLeaseThresholdMilliSeconds() {
        return 0;
    }

    @Override
    public boolean isRenewingLeasesEnabled() {
        return false;
    }

    public HRegionLocation getTableRegionLocation(byte[] tableName, byte[] row) throws SQLException {
       List<HRegionLocation> regions = tableSplits.get(Bytes.toString(tableName));
       if (regions != null) {
               for(HRegionLocation region: regions) {
                       if (Bytes.compareTo(region.getRegionInfo().getStartKey(), row) <= 0
                                       && Bytes.compareTo(region.getRegionInfo().getEndKey(), row) > 0) {
                           return region;
                       }
               }
       }
       return new HRegionLocation(
                       new HRegionInfo(TableName.valueOf(tableName), HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW),
                       SERVER_NAME, -1);
    }

    @Override
    public MetaDataMutationResult createSchema(List<Mutation> schemaMutations, String schemaName) {
        return new MetaDataMutationResult(MutationCode.SCHEMA_NOT_FOUND, 0l, null);
    }

    @Override
    public void addSchema(PSchema schema) throws SQLException {
        this.metaData.addSchema(schema);
    }

    @Override
    public MetaDataMutationResult getSchema(String schemaName, long clientTimestamp) throws SQLException {
        try {
            PSchema schema = metaData.getSchema(new PTableKey(null, schemaName));
            new MetaDataMutationResult(MutationCode.SCHEMA_ALREADY_EXISTS, schema, 0);
        } catch (SchemaNotFoundException e) {}
        return new MetaDataMutationResult(MutationCode.SCHEMA_NOT_FOUND, 0, null);
    }

    @Override
    public void removeSchema(PSchema schema, long schemaTimeStamp) {
        metaData.removeSchema(schema, schemaTimeStamp);
    }

    @Override
    public MetaDataMutationResult dropSchema(List<Mutation> schemaMetaData, String schemaName) {
        return new MetaDataMutationResult(MutationCode.SCHEMA_ALREADY_EXISTS, 0, null);
    }

    /**
     * Manually adds {@link GuidePostsInfo} for a table to the client-side cache. Not a
     * {@link ConnectionQueryServices} method. Exposed for testing purposes.
     *
     * @param tableName Table name
     * @param stats Stats instance
     */
    public void addTableStats(GuidePostsKey key, GuidePostsInfo info) {
        this.guidePostsCache.put(Objects.requireNonNull(key), info);
    }

    @Override
    public void invalidateStats(GuidePostsKey key) {
        this.guidePostsCache.invalidate(Objects.requireNonNull(key));
    }

    @Override
    public void upgradeSystemTables(String url, Properties props) throws SQLException {}

    @Override
    public boolean isUpgradeRequired() {
        return false;
    }

    @Override
    public Configuration getConfiguration() {
        return config;
    }
}
