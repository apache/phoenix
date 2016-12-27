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
import org.apache.phoenix.parse.PSchema;
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


public class DelegateConnectionQueryServices extends DelegateQueryServices implements ConnectionQueryServices {

    public DelegateConnectionQueryServices(ConnectionQueryServices delegate) {
        super(delegate);
    }
    
    @Override
    protected ConnectionQueryServices getDelegate() {
        return (ConnectionQueryServices)super.getDelegate();
    }
    
    @Override
    public ConnectionQueryServices getChildQueryServices(ImmutableBytesWritable tenantId) {
        return getDelegate().getChildQueryServices(tenantId);
    }

    @Override
    public HTableInterface getTable(byte[] tableName) throws SQLException {
        return getDelegate().getTable(tableName);
    }

    @Override
    public List<HRegionLocation> getAllTableRegions(byte[] tableName) throws SQLException {
        return getDelegate().getAllTableRegions(tableName);
    }

    @Override
    public void addTable(PTable table, long resolvedTime) throws SQLException {
        getDelegate().addTable(table, resolvedTime);
    }
    
    @Override
    public void updateResolvedTimestamp(PTable table, long resolvedTimestamp) throws SQLException {
        getDelegate().updateResolvedTimestamp(table, resolvedTimestamp);
    }

    @Override
    public void removeTable(PName tenantId, String tableName, String parentTableName, long tableTimeStamp)
            throws SQLException {
        getDelegate().removeTable(tenantId, tableName, parentTableName, tableTimeStamp);
    }

    @Override
    public void removeColumn(PName tenantId, String tableName, List<PColumn> columnsToRemove, long tableTimeStamp,
            long tableSeqNum, long resolvedTime) throws SQLException {
        getDelegate().removeColumn(tenantId, tableName, columnsToRemove, tableTimeStamp, tableSeqNum, resolvedTime);
    }

    @Override
    public PhoenixConnection connect(String url, Properties info) throws SQLException {
        return getDelegate().connect(url, info);
    }

    @Override
    public MetaDataMutationResult getTable(PName tenantId, byte[] schemaBytes, byte[] tableBytes, long tableTimestamp, long clientTimestamp) throws SQLException {
        return getDelegate().getTable(tenantId, schemaBytes, tableBytes, tableTimestamp, clientTimestamp);
    }

    @Override
    public MetaDataMutationResult createTable(List<Mutation> tableMetaData, byte[] physicalName, PTableType tableType,
            Map<String, Object> tableProps, List<Pair<byte[], Map<String, Object>>> families, byte[][] splits,
            boolean isNamespaceMapped, boolean allocateIndexId) throws SQLException {
        return getDelegate().createTable(tableMetaData, physicalName, tableType, tableProps, families, splits,
                isNamespaceMapped, allocateIndexId);
    }

    @Override
    public MetaDataMutationResult dropTable(List<Mutation> tabeMetaData, PTableType tableType, boolean cascade) throws SQLException {
        return getDelegate().dropTable(tabeMetaData, tableType, cascade);
    }

    @Override
    public MetaDataMutationResult addColumn(List<Mutation> tableMetaData, PTable table, Map<String, List<Pair<String,Object>>> properties, Set<String> colFamiliesForPColumnsToBeAdded, List<PColumn> columns) throws SQLException {
        return getDelegate().addColumn(tableMetaData, table, properties, colFamiliesForPColumnsToBeAdded, columns);
    }


    @Override
    public MetaDataMutationResult dropColumn(List<Mutation> tabeMetaData, PTableType tableType) throws SQLException {
        return getDelegate().dropColumn(tabeMetaData, tableType);
    }

    @Override
    public MetaDataMutationResult updateIndexState(List<Mutation> tableMetadata, String parentTableName) throws SQLException {
        return getDelegate().updateIndexState(tableMetadata, parentTableName);
    }
    
    @Override
    public void init(String url, Properties props) throws SQLException {
        getDelegate().init(url, props);
    }

    @Override
    public MutationState updateData(MutationPlan plan) throws SQLException {
        return getDelegate().updateData(plan);
    }

    @Override
    public int getLowestClusterHBaseVersion() {
        return getDelegate().getLowestClusterHBaseVersion();
    }

    @Override
    public HBaseAdmin getAdmin() throws SQLException {
        return getDelegate().getAdmin();
    }

    @Override
    public HTableDescriptor getTableDescriptor(byte[] tableName) throws SQLException {
        return getDelegate().getTableDescriptor(tableName);
    }

    @Override
    public void clearTableRegionCache(byte[] tableName) throws SQLException {
        getDelegate().clearTableRegionCache(tableName);
    }

    @Override
    public boolean hasIndexWALCodec() {
        return getDelegate().hasIndexWALCodec();
    }

    @Override
    public long createSequence(String tenantId, String schemaName, String sequenceName,
            long startWith, long incrementBy, long cacheSize, long minValue, long maxValue,
            boolean cycle, long timestamp) throws SQLException {
        return getDelegate().createSequence(tenantId, schemaName, sequenceName, startWith,
            incrementBy, cacheSize, minValue, maxValue, cycle, timestamp);
    }

    @Override
    public long dropSequence(String tenantId, String schemaName, String sequenceName, long timestamp)
            throws SQLException {
        return getDelegate().dropSequence(tenantId, schemaName, sequenceName, timestamp);
    }

    @Override
    public void validateSequences(List<SequenceAllocation> sequenceAllocations, long timestamp,
            long[] values, SQLException[] exceptions, Sequence.ValueOp action) throws SQLException {
        getDelegate().validateSequences(sequenceAllocations, timestamp, values, exceptions, action);
    }

    @Override
    public void incrementSequences(List<SequenceAllocation> sequenceAllocations, long timestamp,
            long[] values, SQLException[] exceptions) throws SQLException {
        getDelegate().incrementSequences(sequenceAllocations, timestamp, values, exceptions);
    }

    @Override
    public long currentSequenceValue(SequenceKey sequenceKey, long timestamp) throws SQLException {
        return getDelegate().currentSequenceValue(sequenceKey, timestamp);
    }

    @Override
    public void returnSequences(List<SequenceKey> sequenceKeys, long timestamp, SQLException[] exceptions)
            throws SQLException {
        getDelegate().returnSequences(sequenceKeys, timestamp, exceptions);
    }

    @Override
    public void addConnection(PhoenixConnection connection) throws SQLException {
        getDelegate().addConnection(connection);
    }

    @Override
    public void removeConnection(PhoenixConnection connection) throws SQLException {
        getDelegate().removeConnection(connection);
    }

    @Override
    public KeyValueBuilder getKeyValueBuilder() {
        return getDelegate().getKeyValueBuilder();
    }

    @Override
    public boolean supportsFeature(Feature feature) {
        return getDelegate().supportsFeature(feature);
    }

    @Override
    public String getUserName() {
        return getDelegate().getUserName();
    }
    
    @Override
    public void clearTableFromCache(byte[] tenantId, byte[] schemaName, byte[] tableName, long clientTS)
            throws SQLException {
        getDelegate().clearTableFromCache(tenantId, schemaName, tableName, clientTS);
    }

    @Override
    public GuidePostsInfo getTableStats(GuidePostsKey key) throws SQLException {
        return getDelegate().getTableStats(key);
    }


    @Override
    public long clearCache() throws SQLException {
        return getDelegate().clearCache();
    }

    @Override
    public int getSequenceSaltBuckets() {
        return getDelegate().getSequenceSaltBuckets();
    }

    @Override
    public TransactionSystemClient getTransactionSystemClient() {
        return getDelegate().getTransactionSystemClient();
    }

    @Override
    public MetaDataMutationResult createFunction(List<Mutation> functionData, PFunction function, boolean temporary)
            throws SQLException {
        return getDelegate().createFunction(functionData, function, temporary);
    }

    @Override
    public void addFunction(PFunction function) throws SQLException {
        getDelegate().addFunction(function);
    }

    @Override
    public void removeFunction(PName tenantId, String function, long functionTimeStamp)
            throws SQLException {
        getDelegate().removeFunction(tenantId, function, functionTimeStamp);
    }

    @Override
    public MetaDataMutationResult getFunctions(PName tenantId,
            List<Pair<byte[], Long>> functionNameAndTimeStampPairs, long clientTimestamp)
            throws SQLException {
        return getDelegate().getFunctions(tenantId, functionNameAndTimeStampPairs, clientTimestamp);
    }

    @Override
    public MetaDataMutationResult dropFunction(List<Mutation> tableMetadata, boolean ifExists)
            throws SQLException {
        return getDelegate().dropFunction(tableMetadata, ifExists);
    }

    @Override
    public long getRenewLeaseThresholdMilliSeconds() {
        return getDelegate().getRenewLeaseThresholdMilliSeconds();
    }

    @Override
    public boolean isRenewingLeasesEnabled() {
        return getDelegate().isRenewingLeasesEnabled();
    }

    @Override
    public HRegionLocation getTableRegionLocation(byte[] tableName, byte[] row)
            throws SQLException {
        return getDelegate().getTableRegionLocation(tableName, row);
    }

    @Override
    public void addSchema(PSchema schema) throws SQLException {
        getDelegate().addSchema(schema);
    }

    @Override
    public MetaDataMutationResult createSchema(List<Mutation> schemaMutations, String schemaName) throws SQLException {
        return getDelegate().createSchema(schemaMutations, schemaName);
    }

    @Override
    public MetaDataMutationResult getSchema(String schemaName, long clientTimestamp) throws SQLException {
        return getDelegate().getSchema(schemaName, clientTimestamp);
    }

    @Override
    public void removeSchema(PSchema schema, long schemaTimeStamp) {
        getDelegate().removeSchema(schema, schemaTimeStamp);
    }

    @Override
    public MetaDataMutationResult dropSchema(List<Mutation> schemaMetaData, String schemaName) throws SQLException {
        return getDelegate().dropSchema(schemaMetaData, schemaName);
    }

    @Override
    public void invalidateStats(GuidePostsKey key) {
        getDelegate().invalidateStats(key);
    }

    @Override
    public void upgradeSystemTables(String url, Properties props) throws SQLException {
        getDelegate().upgradeSystemTables(url, props);
    }

    @Override
    public boolean isUpgradeRequired() {
        return getDelegate().isUpgradeRequired();
    }

    @Override
    public Configuration getConfiguration() {
        return getDelegate().getConfiguration();
    }
}