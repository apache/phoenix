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
package org.apache.phoenix.schema;

import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.jdbc.PhoenixConnection;

public class DelegateTable implements PTable {
    @Override
    public long getTimeStamp() {
        return delegate.getTimeStamp();
    }

    @Override
    public long getIndexDisableTimestamp() {
        return delegate.getIndexDisableTimestamp();
    }

    @Override
    public long getSequenceNumber() {
        return delegate.getSequenceNumber();
    }

    @Override
    public PName getName() {
        return delegate.getName();
    }

    @Override
    public PName getSchemaName() {
        return delegate.getSchemaName();
    }

    @Override
    public PName getTableName() {
        return delegate.getTableName();
    }

    @Override
    public PName getTenantId() {
        return delegate.getTenantId();
    }

    @Override
    public PTableType getType() {
        return delegate.getType();
    }

    @Override
    public PName getPKName() {
        return delegate.getPKName();
    }

    @Override
    public List<PColumn> getPKColumns() {
        return delegate.getPKColumns();
    }

    @Override
    public List<PColumn> getColumns() {
        return delegate.getColumns();
    }

    @Override
    public List<PColumnFamily> getColumnFamilies() {
        return delegate.getColumnFamilies();
    }

    @Override
    public PColumnFamily getColumnFamily(byte[] family) throws ColumnFamilyNotFoundException {
        return delegate.getColumnFamily(family);
    }

    @Override
    public PColumnFamily getColumnFamily(String family) throws ColumnFamilyNotFoundException {
        return delegate.getColumnFamily(family);
    }

    @Override
    public PColumn getColumnForColumnName(String name) throws ColumnNotFoundException, AmbiguousColumnException {
        return delegate.getColumnForColumnName(name);
    }

    @Override
    public PColumn getPKColumn(String name) throws ColumnNotFoundException {
        return delegate.getPKColumn(name);
    }

    @Override
    public PRow newRow(KeyValueBuilder builder, long ts, ImmutableBytesWritable key, boolean hasOnDupKey, byte[]... values) {
        return delegate.newRow(builder, ts, key, hasOnDupKey, values);
    }

    @Override
    public PRow newRow(KeyValueBuilder builder, ImmutableBytesWritable key, boolean hasOnDupKey, byte[]... values) {
        return delegate.newRow(builder, key, hasOnDupKey, values);
    }

    @Override
    public int newKey(ImmutableBytesWritable key, byte[][] values) {
        return delegate.newKey(key, values);
    }

    @Override
    public RowKeySchema getRowKeySchema() {
        return delegate.getRowKeySchema();
    }

    @Override
    public Integer getBucketNum() {
        return delegate.getBucketNum();
    }

    @Override
    public List<PTable> getIndexes() {
        return delegate.getIndexes();
    }

    @Override
    public PIndexState getIndexState() {
        return delegate.getIndexState();
    }

    @Override
    public PName getParentName() {
        return delegate.getParentName();
    }

    @Override
    public PName getParentTableName() {
        return delegate.getParentTableName();
    }

    @Override
    public List<PName> getPhysicalNames() {
        return delegate.getPhysicalNames();
    }

    @Override
    public PName getPhysicalName() {
        return delegate.getPhysicalName();
    }

    @Override
    public boolean isImmutableRows() {
        return delegate.isImmutableRows();
    }

    @Override
    public boolean getIndexMaintainers(ImmutableBytesWritable ptr, PhoenixConnection connection) {
        return delegate.getIndexMaintainers(ptr, connection);
    }

    @Override
    public IndexMaintainer getIndexMaintainer(PTable dataTable, PhoenixConnection connection) {
        return delegate.getIndexMaintainer(dataTable, connection);
    }

    @Override
    public PName getDefaultFamilyName() {
        return delegate.getDefaultFamilyName();
    }

    @Override
    public boolean isWALDisabled() {
        return delegate.isWALDisabled();
    }

    @Override
    public boolean isMultiTenant() {
        return delegate.isMultiTenant();
    }

    @Override
    public boolean getStoreNulls() {
        return delegate.getStoreNulls();
    }

    @Override
    public ViewType getViewType() {
        return delegate.getViewType();
    }

    @Override
    public String getViewStatement() {
        return delegate.getViewStatement();
    }

    @Override
    public Short getViewIndexId() {
        return delegate.getViewIndexId();
    }

    @Override
    public PTableKey getKey() {
        return delegate.getKey();
    }

    @Override
    public int getEstimatedSize() {
        return delegate.getEstimatedSize();
    }

    @Override
    public IndexType getIndexType() {
        return delegate.getIndexType();
    }

    private final PTable delegate;

    public DelegateTable(PTable delegate) {
        this.delegate = delegate;
    }

    @Override
    public PName getParentSchemaName() {
        return delegate.getParentSchemaName();
    }

    @Override
    public boolean isTransactional() {
        return delegate.isTransactional();
    }

    @Override
    public int getBaseColumnCount() {
        return delegate.getBaseColumnCount();
    }

    @Override
    public boolean rowKeyOrderOptimizable() {
        return delegate.rowKeyOrderOptimizable();
    }

    @Override
    public int getRowTimestampColPos() {
        return delegate.getRowTimestampColPos();
    }
    
    @Override
    public String toString() {
        return delegate.toString();
    }

    @Override
    public long getUpdateCacheFrequency() {
        return delegate.getUpdateCacheFrequency();
    }

    @Override
    public boolean isNamespaceMapped() {
        return delegate.isNamespaceMapped();
    }

    @Override
    public String getAutoPartitionSeqName() {
        return delegate.getAutoPartitionSeqName();
    }
    
    @Override
    public boolean isAppendOnlySchema() {
        return delegate.isAppendOnlySchema();
    }
    
    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return delegate.equals(obj);
    }
    
    @Override
    public ImmutableStorageScheme getImmutableStorageScheme() {
        return delegate.getImmutableStorageScheme();
    }

    @Override
    public PColumn getColumnForColumnQualifier(byte[] cf, byte[] cq) throws ColumnNotFoundException, AmbiguousColumnException {
        return delegate.getColumnForColumnQualifier(cf, cq);
    }

    @Override
    public EncodedCQCounter getEncodedCQCounter() {
        return delegate.getEncodedCQCounter();
    }

    @Override
    public QualifierEncodingScheme getEncodingScheme() {
        return delegate.getEncodingScheme();
    }
}
