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
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PMetaData;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.Sequence;
import org.apache.phoenix.schema.SequenceKey;


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
    public StatsManager getStatsManager() {
        return getDelegate().getStatsManager();
    }

    @Override
    public List<HRegionLocation> getAllTableRegions(byte[] tableName) throws SQLException {
        return getDelegate().getAllTableRegions(tableName);
    }

    @Override
    public PMetaData addTable(PTable table) throws SQLException {
        return getDelegate().addTable(table);
    }

    @Override
    public PMetaData addColumn(PName tenantId, String tableName, List<PColumn> columns, long tableTimeStamp,
            long tableSeqNum, boolean isImmutableRows) throws SQLException {
        return getDelegate().addColumn(tenantId, tableName, columns, tableTimeStamp, tableSeqNum, isImmutableRows);
    }

    @Override
    public PMetaData removeTable(PName tenantId, String tableName)
            throws SQLException {
        return getDelegate().removeTable(tenantId, tableName);
    }

    @Override
    public PMetaData removeColumn(PName tenantId, String tableName, String familyName, String columnName,
            long tableTimeStamp, long tableSeqNum) throws SQLException {
        return getDelegate().removeColumn(tenantId, tableName, familyName, columnName, tableTimeStamp, tableSeqNum);
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
    public MetaDataMutationResult createTable(List<Mutation> tableMetaData, byte[] physicalName,
            PTableType tableType, Map<String, Object> tableProps, List<Pair<byte[], Map<String, Object>>> families, byte[][] splits)
            throws SQLException {
        return getDelegate().createTable(tableMetaData, physicalName, tableType, tableProps, families, splits);
    }

    @Override
    public MetaDataMutationResult dropTable(List<Mutation> tabeMetaData, PTableType tableType) throws SQLException {
        return getDelegate().dropTable(tabeMetaData, tableType);
    }

    @Override
    public MetaDataMutationResult addColumn(List<Mutation> tabeMetaData, List<Pair<byte[],Map<String,Object>>> families, PTable table) throws SQLException {
        return getDelegate().addColumn(tabeMetaData, families, table);
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
    public boolean hasInvalidIndexConfiguration() {
        return getDelegate().hasInvalidIndexConfiguration();
    }

    @Override
    public long createSequence(String tenantId, String schemaName, String sequenceName, long startWith,
            long incrementBy, long cacheSize, long timestamp) throws SQLException {
        return getDelegate().createSequence(tenantId, schemaName, sequenceName, startWith, incrementBy, cacheSize, timestamp);
    }

    @Override
    public long dropSequence(String tenantId, String schemaName, String sequenceName, long timestamp)
            throws SQLException {
        return getDelegate().dropSequence(tenantId, schemaName, sequenceName, timestamp);
    }

    @Override
    public void validateSequences(List<SequenceKey> sequenceKeys, long timestamp, long[] values,
            SQLException[] exceptions, Sequence.Action action) throws SQLException {
        getDelegate().validateSequences(sequenceKeys, timestamp, values, exceptions, action);
    }

    @Override
    public void incrementSequences(List<SequenceKey> sequenceKeys, long timestamp, long[] values,
            SQLException[] exceptions) throws SQLException {
        getDelegate().incrementSequences(sequenceKeys, timestamp, values, exceptions);
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
}