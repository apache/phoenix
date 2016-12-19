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

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.MutationPlan;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.parse.PFunction;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.Sequence;
import org.apache.phoenix.schema.SequenceAllocation;
import org.apache.phoenix.schema.SequenceKey;
import org.apache.phoenix.schema.stats.GuidePostsInfo;
import org.apache.phoenix.schema.stats.GuidePostsKey;
import org.apache.tephra.TransactionSystemClient;


public interface ConnectionQueryServices extends QueryServices, MetaDataMutated {
    public static final int INITIAL_META_DATA_TABLE_CAPACITY = 100;

    /**
     * Get (and create if necessary) a child QueryService for a given tenantId.
     * The QueryService will be cached for the lifetime of the parent QueryService
     * @param tenantId the organization ID
     * @return the child QueryService
     */
    public ConnectionQueryServices getChildQueryServices(ImmutableBytesWritable tenantId);

    /**
     * Get an HTableInterface by the given name. It is the callers
     * responsibility to close the returned HTableInterface.
     * @param tableName the name of the HTable
     * @return the HTableInterface
     * @throws SQLException 
     */
    public HTableInterface getTable(byte[] tableName) throws SQLException;

    public HTableDescriptor getTableDescriptor(byte[] tableName) throws SQLException;

    public HRegionLocation getTableRegionLocation(byte[] tableName, byte[] row) throws SQLException;
    public List<HRegionLocation> getAllTableRegions(byte[] tableName) throws SQLException;

    public PhoenixConnection connect(String url, Properties info) throws SQLException;

    public MetaDataMutationResult getTable(PName tenantId, byte[] schemaName, byte[] tableName, long tableTimestamp, long clientTimetamp) throws SQLException;
    public MetaDataMutationResult getFunctions(PName tenantId, List<Pair<byte[], Long>> functionNameAndTimeStampPairs, long clientTimestamp) throws SQLException;

    public MetaDataMutationResult createTable(List<Mutation> tableMetaData, byte[] tableName, PTableType tableType,
            Map<String, Object> tableProps, List<Pair<byte[], Map<String, Object>>> families, byte[][] splits,
            boolean isNamespaceMapped, boolean allocateIndexId) throws SQLException;
    public MetaDataMutationResult dropTable(List<Mutation> tableMetadata, PTableType tableType, boolean cascade) throws SQLException;
    public MetaDataMutationResult dropFunction(List<Mutation> tableMetadata, boolean ifExists) throws SQLException;
    public MetaDataMutationResult addColumn(List<Mutation> tableMetaData, PTable table, Map<String, List<Pair<String,Object>>> properties, Set<String> colFamiliesForPColumnsToBeAdded, List<PColumn> columns) throws SQLException;
    public MetaDataMutationResult dropColumn(List<Mutation> tableMetadata, PTableType tableType) throws SQLException;
    public MetaDataMutationResult updateIndexState(List<Mutation> tableMetadata, String parentTableName) throws SQLException;
    public MutationState updateData(MutationPlan plan) throws SQLException;

    public void init(String url, Properties props) throws SQLException;

    public int getLowestClusterHBaseVersion();
    public HBaseAdmin getAdmin() throws SQLException;

    void clearTableRegionCache(byte[] tableName) throws SQLException;

    boolean hasIndexWALCodec();
    
    long createSequence(String tenantId, String schemaName, String sequenceName, long startWith, long incrementBy, long cacheSize, long minValue, long maxValue, boolean cycle, long timestamp) throws SQLException;
    long dropSequence(String tenantId, String schemaName, String sequenceName, long timestamp) throws SQLException;
    void validateSequences(List<SequenceAllocation> sequenceAllocations, long timestamp, long[] values, SQLException[] exceptions, Sequence.ValueOp action) throws SQLException;
    void incrementSequences(List<SequenceAllocation> sequenceAllocation, long timestamp, long[] values, SQLException[] exceptions) throws SQLException;
    long currentSequenceValue(SequenceKey sequenceKey, long timestamp) throws SQLException;
    void returnSequences(List<SequenceKey> sequenceKeys, long timestamp, SQLException[] exceptions) throws SQLException;

    MetaDataMutationResult createFunction(List<Mutation> functionData, PFunction function, boolean temporary) throws SQLException;
    void addConnection(PhoenixConnection connection) throws SQLException;
    void removeConnection(PhoenixConnection connection) throws SQLException;

    /**
     * @return the {@link KeyValueBuilder} that is valid for the locally installed version of HBase.
     */
    public KeyValueBuilder getKeyValueBuilder();
    
    public enum Feature {LOCAL_INDEX, RENEW_LEASE};
    public boolean supportsFeature(Feature feature);
    
    public String getUserName();
    public void clearTableFromCache(final byte[] tenantId, final byte[] schemaName, final byte[] tableName, long clientTS) throws SQLException;

    public GuidePostsInfo getTableStats(GuidePostsKey key) throws SQLException;
    /**
     * Removes cache {@link GuidePostsInfo} for the table with the given name. If no cached guideposts are present, this does nothing.
     *
     * @param tableName The table to remove stats for
     */
    void invalidateStats(GuidePostsKey key);
    
    
    public long clearCache() throws SQLException;
    public int getSequenceSaltBuckets();

    TransactionSystemClient getTransactionSystemClient();
    public long getRenewLeaseThresholdMilliSeconds();
    public boolean isRenewingLeasesEnabled();

    public MetaDataMutationResult createSchema(List<Mutation> schemaMutations, String schemaName) throws SQLException;

    MetaDataMutationResult getSchema(String schemaName, long clientTimestamp) throws SQLException;

    public MetaDataMutationResult dropSchema(List<Mutation> schemaMetaData, String schemaName) throws SQLException;

    boolean isUpgradeRequired();
    void upgradeSystemTables(String url, Properties props) throws SQLException;
    
    public Configuration getConfiguration();
}