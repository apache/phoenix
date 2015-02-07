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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

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
import org.apache.phoenix.schema.Sequence;
import org.apache.phoenix.schema.SequenceAlreadyExistsException;
import org.apache.phoenix.schema.SequenceInfo;
import org.apache.phoenix.schema.SequenceKey;
import org.apache.phoenix.schema.SequenceNotFoundException;
import org.apache.phoenix.schema.TableAlreadyExistsException;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.stats.PTableStats;
import org.apache.phoenix.util.JDBCUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.SequenceUtil;

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
    private KeyValueBuilder kvBuilder;
    private volatile boolean initialized;
    private volatile SQLException initializationException;
    private final Map<String, List<HRegionLocation>> tableSplits = Maps.newHashMap();
    
    public ConnectionlessQueryServicesImpl(QueryServices queryServices, ConnectionInfo connInfo) {
        super(queryServices);
        userName = connInfo.getPrincipal();
        metaData = newEmptyMetaData();
        // Use KeyValueBuilder that builds real KeyValues, as our test utils require this
        this.kvBuilder = GenericKeyValueBuilder.INSTANCE;
    }

    private PMetaData newEmptyMetaData() {
        long maxSizeBytes = getProps().getLong(QueryServices.MAX_CLIENT_METADATA_CACHE_SIZE_ATTRIB,
                QueryServicesOptions.DEFAULT_MAX_CLIENT_METADATA_CACHE_SIZE);
        return new PMetaDataImpl(INITIAL_META_DATA_TABLE_CAPACITY, maxSizeBytes);
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
    public PMetaData addTable(PTable table) throws SQLException {
        return metaData = metaData.addTable(table);
    }

    @Override
    public PMetaData addColumn(PName tenantId, String tableName, List<PColumn> columns, long tableTimeStamp,
            long tableSeqNum, boolean isImmutableRows, boolean isWalDisabled, boolean isMultitenant, boolean storeNulls) throws SQLException {
        return metaData = metaData.addColumn(tenantId, tableName, columns, tableTimeStamp, tableSeqNum, isImmutableRows, isWalDisabled, isMultitenant, storeNulls);
    }

    @Override
    public PMetaData removeTable(PName tenantId, String tableName, String parentTableName, long tableTimeStamp)
            throws SQLException {
        return metaData = metaData.removeTable(tenantId, tableName, parentTableName, tableTimeStamp);
    }

    @Override
    public PMetaData removeColumn(PName tenantId, String tableName, List<PColumn> columnsToRemove, long tableTimeStamp,
            long tableSeqNum) throws SQLException {
        return metaData = metaData.removeColumn(tenantId, tableName, columnsToRemove, tableTimeStamp, tableSeqNum);
    }

    
    @Override
    public PhoenixConnection connect(String url, Properties info) throws SQLException {
        return new PhoenixConnection(this, url, info, metaData);
    }

    @Override
    public MetaDataMutationResult getTable(PName tenantId, byte[] schemaBytes, byte[] tableBytes, long tableTimestamp, long clientTimestamp) throws SQLException {
        // Return result that will cause client to use it's own metadata instead of needing
        // to get anything from the server (since we don't have a connection)
        try {
            String fullTableName = SchemaUtil.getTableName(schemaBytes, tableBytes);
            PTable table = metaData.getTable(new PTableKey(tenantId, fullTableName));
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
    public MetaDataMutationResult createTable(List<Mutation> tableMetaData, byte[] physicalName, PTableType tableType, Map<String,Object> tableProps, List<Pair<byte[],Map<String,Object>>> families, byte[][] splits) throws SQLException {
        if (splits != null) {
            byte[] tableName = getTableName(tableMetaData, physicalName);
            tableSplits.put(Bytes.toString(tableName), generateRegionLocations(tableName, splits));
        }
        return new MetaDataMutationResult(MutationCode.TABLE_NOT_FOUND, 0, null);
    }

    @Override
    public MetaDataMutationResult dropTable(List<Mutation> tableMetadata, PTableType tableType, boolean cascade) throws SQLException {
        byte[] tableName = getTableName(tableMetadata, null);
        tableSplits.remove(Bytes.toString(tableName));
        return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS, 0, null);
    }

    @Override
    public MetaDataMutationResult addColumn(List<Mutation> tableMetaData, PTable table, Map<String, List<Pair<String,Object>>> properties, Set<String> colFamiliesForPColumnsToBeAdded) throws SQLException {
        return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS, 0, null);
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
        return new MutationState(0, plan.getConnection());
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
        PTable index = metaData.getTable(new PTableKey(tenantId, indexTableName));
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
    public boolean hasInvalidIndexConfiguration() {
        return false;
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
    public void validateSequences(List<SequenceKey> sequenceKeys, long timestamp, long[] values,
            SQLException[] exceptions, Sequence.ValueOp action) throws SQLException {
        int i = 0;
        for (SequenceKey key : sequenceKeys) {
            SequenceInfo info = sequenceMap.get(key);
            if (info == null) {
                exceptions[i] = new SequenceNotFoundException(key.getSchemaName(), key.getSequenceName());
            } else {
                values[i] = info.sequenceValue;          
            }
            i++;
        }
    }

    @Override
    public void incrementSequences(List<SequenceKey> sequenceKeys, long timestamp, long[] values,
            SQLException[] exceptions) throws SQLException {
        int i = 0;
		for (SequenceKey key : sequenceKeys) {
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
                sequenceMap.remove(sequenceKeys.get(i));
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
    public PTableStats getTableStats(byte[] physicalName, long clientTimeStamp) {
        return PTableStats.EMPTY_STATS;
    }

    @Override
    public void clearCache() throws SQLException {
    }

    @Override
    public int getSequenceSaltBuckets() {
        return getProps().getInt(QueryServices.SEQUENCE_SALT_BUCKETS_ATTRIB,
                QueryServicesOptions.DEFAULT_SEQUENCE_TABLE_SALT_BUCKETS);
    }
 
}
