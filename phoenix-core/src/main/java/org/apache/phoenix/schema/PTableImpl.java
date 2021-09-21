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

import static org.apache.phoenix.coprocessor.ScanRegionObserver.DYNAMIC_COLUMN_METADATA_STORED_FOR_MUTATION;
import static org.apache.phoenix.hbase.index.util.KeyValueBuilder.addQuietly;
import static org.apache.phoenix.hbase.index.util.KeyValueBuilder.deleteQuietly;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.APPEND_ONLY_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.AUTO_PARTITION_SEQ;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_ENCODED_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DISABLE_WAL;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ENCODING_SCHEME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IMMUTABLE_ROWS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DEFAULT_COLUMN_FAMILY_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IMMUTABLE_STORAGE_SCHEME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MULTI_TENANT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SALT_BUCKETS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TRANSACTIONAL;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TRANSACTION_PROVIDER;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.UPDATE_CACHE_FREQUENCY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.USE_STATS_FOR_PARALLELIZATION;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_COLUMN_ENCODED_BYTES;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_IMMUTABLE_STORAGE_SCHEME;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_MULTI_TENANT;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_SALT_BUCKETS;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_TRANSACTIONAL;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_TRANSACTION_PROVIDER;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_UPDATE_CACHE_FREQUENCY;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_USE_STATS_FOR_PARALLELIZATION;
import static org.apache.phoenix.schema.SaltingUtil.SALTING_COLUMN;
import static org.apache.phoenix.schema.TableProperty.DEFAULT_COLUMN_FAMILY;
import static org.apache.phoenix.schema.types.PDataType.TRUE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PHOENIX_TTL_NOT_DEFINED;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MIN_PHOENIX_TTL_HWM;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nonnull;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.ExpressionCompiler;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.coprocessor.generated.DynamicColumnMetaDataProtos;
import org.apache.phoenix.coprocessor.generated.PTableProtos;
import org.apache.phoenix.exception.DataExceedsCapacityException;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.SingleCellConstructorExpression;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.protobuf.ProtobufUtil;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.RowKeySchema.RowKeySchemaBuilder;
import org.apache.phoenix.schema.types.PBinary;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PFloat;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.transaction.TransactionFactory;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.SizedUtil;
import org.apache.phoenix.util.TrustedByteArrayOutputStream;

import org.apache.phoenix.thirdparty.com.google.common.base.Objects;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableSortedMap;
import org.apache.phoenix.thirdparty.com.google.common.collect.ListMultimap;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;


/**
 *
 * Base class for PTable implementors.  Provides abstraction for
 * storing data in a single column (ColumnLayout.SINGLE) or in
 * multiple columns (ColumnLayout.MULTI).
 *
 * @since 0.1
 */
public class PTableImpl implements PTable {
    private static final Integer NO_SALTING = -1;
    private static final int VIEW_MODIFIED_UPDATE_CACHE_FREQUENCY_BIT_SET_POS = 0;
    private static final int VIEW_MODIFIED_USE_STATS_FOR_PARALLELIZATION_BIT_SET_POS = 1;
    private static final int VIEW_MODIFIED_PHOENIX_TTL_BIT_SET_POS = 2;

    private IndexMaintainer indexMaintainer;
    private ImmutableBytesWritable indexMaintainersPtr;

    private final PTableKey key;
    private final PName name;
    private final PName schemaName;
    private final PName tableName;
    private final PName tenantId;
    private final PTableType type;
    private final PIndexState state;
    private final long sequenceNumber;
    private final long timeStamp;
    private final long indexDisableTimestamp;
    // Have MultiMap for String->PColumn (may need family qualifier)
    private final List<PColumn> pkColumns;
    private final List<PColumn> allColumns;
    // columns that were inherited from a parent table but that were dropped in the view
    private final List<PColumn> excludedColumns;
    private final List<PColumnFamily> families;
    private final Map<byte[], PColumnFamily> familyByBytes;
    private final Map<String, PColumnFamily> familyByString;
    private final ListMultimap<String, PColumn> columnsByName;
    private final Map<KVColumnFamilyQualifier, PColumn> kvColumnsByQualifiers;
    private final PName pkName;
    private final Integer bucketNum;
    private final RowKeySchema rowKeySchema;
    // Indexes associated with this table.
    private final List<PTable> indexes;
    // Data table name that the index is created on.
    private final PName parentName;
    private final PName parentSchemaName;
    private final PName parentTableName;
    private final List<PName> physicalNames;
    private final boolean isImmutableRows;
    private final PName defaultFamilyName;
    private final String viewStatement;
    private final boolean disableWAL;
    private final boolean multiTenant;
    private final boolean storeNulls;
    private final TransactionFactory.Provider transactionProvider;
    private final ViewType viewType;
    private final PDataType viewIndexIdType;
    private final Long viewIndexId;
    private final int estimatedSize;
    private final IndexType indexType;
    private final int baseColumnCount;
    private final boolean rowKeyOrderOptimizable; // TODO: remove when required that tables have been upgrade for PHOENIX-2067
    private final boolean hasColumnsRequiringUpgrade; // TODO: remove when required that tables have been upgrade for PHOENIX-2067
    private final int rowTimestampColPos;
    private final long updateCacheFrequency;
    private final boolean isNamespaceMapped;
    private final String autoPartitionSeqName;
    private final boolean isAppendOnlySchema;
    private final ImmutableStorageScheme immutableStorageScheme;
    private final QualifierEncodingScheme qualifierEncodingScheme;
    private final EncodedCQCounter encodedCQCounter;
    private final Boolean useStatsForParallelization;
    private final long phoenixTTL;
    private final long phoenixTTLHighWaterMark;
    private final BitSet viewModifiedPropSet;
    private final Long lastDDLTimestamp;
    private final boolean isChangeDetectionEnabled;
    private Map<String, String> propertyValues;

    public static class Builder {
        private PTableKey key;
        private PName name;
        private PName schemaName = PName.EMPTY_NAME;
        private PName tableName = PName.EMPTY_NAME;
        private PName tenantId;
        private PTableType type;
        private PIndexState state;
        private long sequenceNumber;
        private long timeStamp;
        private long indexDisableTimestamp;
        private List<PColumn> pkColumns;
        private List<PColumn> allColumns;
        private List<PColumn> excludedColumns;
        private List<PColumnFamily> families;
        private Map<byte[], PColumnFamily> familyByBytes;
        private Map<String, PColumnFamily> familyByString;
        private ListMultimap<String, PColumn> columnsByName;
        private Map<KVColumnFamilyQualifier, PColumn> kvColumnsByQualifiers;
        private PName pkName;
        private Integer bucketNum;
        private RowKeySchema rowKeySchema;
        private List<PTable> indexes;
        private PName parentName;
        private PName parentSchemaName;
        private PName parentTableName;
        private List<PName> physicalNames;
        private boolean isImmutableRows;
        private IndexMaintainer indexMaintainer;
        private ImmutableBytesWritable indexMaintainersPtr;
        private PName defaultFamilyName;
        private String viewStatement;
        private boolean disableWAL;
        private boolean multiTenant;
        private boolean storeNulls;
        private TransactionFactory.Provider transactionProvider;
        private ViewType viewType;
        private PDataType viewIndexIdType;
        private Long viewIndexId;
        private int estimatedSize;
        private IndexType indexType;
        private int baseColumnCount;
        private boolean rowKeyOrderOptimizable;
        private boolean hasColumnsRequiringUpgrade;
        private int rowTimestampColPos;
        private long updateCacheFrequency;
        private boolean isNamespaceMapped;
        private String autoPartitionSeqName;
        private boolean isAppendOnlySchema;
        private ImmutableStorageScheme immutableStorageScheme;
        private QualifierEncodingScheme qualifierEncodingScheme;
        private EncodedCQCounter encodedCQCounter;
        private Boolean useStatsForParallelization;
        private long phoenixTTL;
        private long phoenixTTLHighWaterMark;
        private Long lastDDLTimestamp;
        private boolean isChangeDetectionEnabled = false;
        private Map<String, String> propertyValues = new HashMap<>();

        // Used to denote which properties a view has explicitly modified
        private BitSet viewModifiedPropSet = new BitSet(3);
        // Optionally set columns for the builder, but not for the actual PTable
        private Collection<PColumn> columns;

        public Builder setKey(PTableKey key) {
            this.key = key;
            return this;
        }

        public Builder setName(PName name) {
            this.name = name;
            return this;
        }

        public Builder setSchemaName(PName schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        public Builder setTableName(PName tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder setTenantId(PName tenantId) {
            this.tenantId = tenantId;
            return this;
        }

        public Builder setType(PTableType type) {
            this.type = type;
            return this;
        }

        public Builder setState(PIndexState state) {
            this.state = state;
            return this;
        }

        public Builder setSequenceNumber(long sequenceNumber) {
            this.sequenceNumber = sequenceNumber;
            return this;
        }

        public Builder setTimeStamp(long timeStamp) {
            this.timeStamp = timeStamp;
            return this;
        }

        public Builder setIndexDisableTimestamp(long indexDisableTimestamp) {
            this.indexDisableTimestamp = indexDisableTimestamp;
            return this;
        }

        public Builder setPkColumns(List<PColumn> pkColumns) {
            this.pkColumns = pkColumns;
            return this;
        }

        public Builder setAllColumns(List<PColumn> allColumns) {
            this.allColumns = allColumns;
            return this;
        }

        public Builder setExcludedColumns(List<PColumn> excludedColumns) {
            this.excludedColumns = excludedColumns;
            return this;
        }

        public Builder setFamilyAttributes(List<PColumnFamily> families) {
            this.familyByBytes = Maps.newHashMapWithExpectedSize(families.size());
            this.familyByString = Maps.newHashMapWithExpectedSize(families.size());
            for (PColumnFamily family : families) {
                familyByBytes.put(family.getName().getBytes(), family);
                familyByString.put(family.getName().getString(), family);
            }
            this.families = families;
            return this;
        }

        public Builder setFamilies(List<PColumnFamily> families) {
            this.families = families;
            return this;
        }

        public Builder setFamilyByBytes(Map<byte[], PColumnFamily> familyByBytes) {
            this.familyByBytes = familyByBytes;
            return this;
        }

        public Builder setFamilyByString(Map<String, PColumnFamily> familyByString) {
            this.familyByString = familyByString;
            return this;
        }

        public Builder setColumnsByName(ListMultimap<String, PColumn> columnsByName) {
            this.columnsByName = columnsByName;
            return this;
        }

        public Builder setKvColumnsByQualifiers(Map<KVColumnFamilyQualifier, PColumn> kvColumnsByQualifiers) {
            this.kvColumnsByQualifiers = kvColumnsByQualifiers;
            return this;
        }

        public Builder setPkName(PName pkName) {
            this.pkName = pkName;
            return this;
        }

        public Builder setBucketNum(Integer bucketNum) {
            if(bucketNum!=null) {
                propertyValues.put(SALT_BUCKETS, String.valueOf(bucketNum));
            }
            this.bucketNum = bucketNum;
            return this;
        }

        public Builder setRowKeySchema(RowKeySchema rowKeySchema) {
            this.rowKeySchema = rowKeySchema;
            return this;
        }

        public Builder setIndexes(List<PTable> indexes) {
            this.indexes = indexes;
            return this;
        }

        public Builder setParentName(PName parentName) {
            this.parentName = parentName;
            return this;
        }

        public Builder setParentSchemaName(PName parentSchemaName) {
            this.parentSchemaName = parentSchemaName;
            return this;
        }

        public Builder setParentTableName(PName parentTableName) {
            this.parentTableName = parentTableName;
            return this;
        }

        public Builder setPhysicalNames(List<PName> physicalNames) {
            this.physicalNames = physicalNames;
            return this;
        }

        public Builder setImmutableRows(boolean immutableRows) {
            propertyValues.put(IMMUTABLE_ROWS, String.valueOf(immutableRows));
            isImmutableRows = immutableRows;
            return this;
        }

        public Builder setIndexMaintainer(IndexMaintainer indexMaintainer) {
            this.indexMaintainer = indexMaintainer;
            return this;
        }

        public Builder setIndexMaintainersPtr(ImmutableBytesWritable indexMaintainersPtr) {
            this.indexMaintainersPtr = indexMaintainersPtr;
            return this;
        }

        public Builder setDefaultFamilyName(PName defaultFamilyName) {
            if (defaultFamilyName != null){
                propertyValues.put(DEFAULT_COLUMN_FAMILY_NAME, defaultFamilyName.getString());
            }
            this.defaultFamilyName = defaultFamilyName;
            return this;
        }

        public Builder setViewStatement(String viewStatement) {
            this.viewStatement = viewStatement;
            return this;
        }

        public Builder setDisableWAL(boolean disableWAL) {
            propertyValues.put(DISABLE_WAL, String.valueOf(disableWAL));
            this.disableWAL = disableWAL;
            return this;
        }

        public Builder setMultiTenant(boolean multiTenant) {
            propertyValues.put(MULTI_TENANT, String.valueOf(multiTenant));
            this.multiTenant = multiTenant;
            return this;
        }

        public Builder setStoreNulls(boolean storeNulls) {
            this.storeNulls = storeNulls;
            return this;
        }

        public Builder setTransactionProvider(TransactionFactory.Provider transactionProvider) {
            if(transactionProvider != null) {
                propertyValues.put(TRANSACTION_PROVIDER, String.valueOf(transactionProvider));
            }
            this.transactionProvider = transactionProvider;
            return this;
        }

        public Builder setViewType(ViewType viewType) {
            this.viewType = viewType;
            return this;
        }

        public Builder setViewIndexIdType(PDataType viewIndexIdType) {
            this.viewIndexIdType = viewIndexIdType;
            return this;
        }

        public Builder setViewIndexId(Long viewIndexId) {
            this.viewIndexId = viewIndexId;
            return this;
        }

        public Builder setEstimatedSize(int estimatedSize) {
            this.estimatedSize = estimatedSize;
            return this;
        }

        public Builder setIndexType(IndexType indexType) {
            this.indexType = indexType;
            return this;
        }

        public Builder setBaseColumnCount(int baseColumnCount) {
            this.baseColumnCount = baseColumnCount;
            return this;
        }

        public Builder setRowKeyOrderOptimizable(boolean rowKeyOrderOptimizable) {
            this.rowKeyOrderOptimizable = rowKeyOrderOptimizable;
            return this;
        }

        public Builder setHasColumnsRequiringUpgrade(boolean hasColumnsRequiringUpgrade) {
            this.hasColumnsRequiringUpgrade = hasColumnsRequiringUpgrade;
            return this;
        }

        public Builder setRowTimestampColPos(int rowTimestampColPos) {
            this.rowTimestampColPos = rowTimestampColPos;
            return this;
        }

        public Builder setUpdateCacheFrequency(long updateCacheFrequency) {
            propertyValues.put(UPDATE_CACHE_FREQUENCY, String.valueOf(updateCacheFrequency));
            this.updateCacheFrequency = updateCacheFrequency;
            return this;
        }

        public Builder setNamespaceMapped(boolean namespaceMapped) {
            isNamespaceMapped = namespaceMapped;
            return this;
        }

        public Builder setAutoPartitionSeqName(String autoPartitionSeqName) {
            propertyValues.put(AUTO_PARTITION_SEQ, autoPartitionSeqName);
            this.autoPartitionSeqName = autoPartitionSeqName;
            return this;
        }

        public Builder setAppendOnlySchema(boolean appendOnlySchema) {
            propertyValues.put(APPEND_ONLY_SCHEMA, String.valueOf(appendOnlySchema));
            isAppendOnlySchema = appendOnlySchema;
            return this;
        }

        public Builder setImmutableStorageScheme(ImmutableStorageScheme immutableStorageScheme) {
            propertyValues.put(IMMUTABLE_STORAGE_SCHEME, immutableStorageScheme.toString());
            this.immutableStorageScheme = immutableStorageScheme;
            return this;
        }

        public Builder setQualifierEncodingScheme(QualifierEncodingScheme qualifierEncodingScheme) {
            propertyValues.put(ENCODING_SCHEME, qualifierEncodingScheme.toString());
            this.qualifierEncodingScheme = qualifierEncodingScheme;
            return this;
        }

        public Builder setEncodedCQCounter(EncodedCQCounter encodedCQCounter) {
            this.encodedCQCounter = encodedCQCounter;
            return this;
        }

        public Builder setUseStatsForParallelization(Boolean useStatsForParallelization) {
            if(useStatsForParallelization!=null) {
                propertyValues.put(USE_STATS_FOR_PARALLELIZATION, String.valueOf(useStatsForParallelization));
            }
            this.useStatsForParallelization = useStatsForParallelization;
            return this;
        }

        public Builder setViewModifiedUpdateCacheFrequency(boolean modified) {
            this.viewModifiedPropSet.set(VIEW_MODIFIED_UPDATE_CACHE_FREQUENCY_BIT_SET_POS,
                    modified);
            return this;
        }

        public Builder setViewModifiedUseStatsForParallelization(boolean modified) {
            this.viewModifiedPropSet.set(VIEW_MODIFIED_USE_STATS_FOR_PARALLELIZATION_BIT_SET_POS,
                    modified);
            return this;
        }

        public Builder setPhoenixTTL(long phoenixTTL) {
            this.phoenixTTL = phoenixTTL;
            return this;
        }

        public Builder setPhoenixTTLHighWaterMark(long phoenixTTLHighWaterMark) {
            this.phoenixTTLHighWaterMark = phoenixTTLHighWaterMark;
            return this;
        }

        public Builder setViewModifiedPhoenixTTL(boolean modified) {
            this.viewModifiedPropSet.set(VIEW_MODIFIED_PHOENIX_TTL_BIT_SET_POS,
                    modified);
            return this;
        }

        /**
         * Note: When set in the builder, we must call {@link Builder#initDerivedAttributes()}
         * before building the PTable in order to correctly populate other attributes of the PTable
         * @param columns PColumns to be set in the builder
         * @return PTableImpl.Builder object
         */
        public Builder setColumns(Collection<PColumn> columns) {
            this.columns = columns;
            return this;
        }

        public Builder setLastDDLTimestamp(Long lastDDLTimestamp) {
            this.lastDDLTimestamp = lastDDLTimestamp;
            return this;
        }

        public Builder setIsChangeDetectionEnabled(Boolean isChangeDetectionEnabled) {
            if (isChangeDetectionEnabled != null) {
                this.isChangeDetectionEnabled = isChangeDetectionEnabled;
            }
            return this;
        }

        /**
         * Populate derivable attributes of the PTable
         * @return PTableImpl.Builder object
         * @throws SQLException
         */
        private Builder initDerivedAttributes() throws SQLException {
            checkTenantId(this.tenantId);
            Preconditions.checkNotNull(this.schemaName);
            Preconditions.checkNotNull(this.tableName);
            Preconditions.checkNotNull(this.columns);
            Preconditions.checkNotNull(this.indexes);
            Preconditions.checkNotNull(this.physicalNames);
            //hasColumnsRequiringUpgrade and rowKeyOrderOptimizable are booleans and can never be
            // null, so no need to check them

            PName fullName = PNameFactory.newName(SchemaUtil.getTableName(
                    this.schemaName.getString(), this.tableName.getString()));
            int estimatedSize = SizedUtil.OBJECT_SIZE * 2 + 23 * SizedUtil.POINTER_SIZE +
                    4 * SizedUtil.INT_SIZE + 2 * SizedUtil.LONG_SIZE + 2 * SizedUtil.INT_OBJECT_SIZE +
                    PNameFactory.getEstimatedSize(this.tenantId) +
                    PNameFactory.getEstimatedSize(this.schemaName) +
                    PNameFactory.getEstimatedSize(this.tableName) +
                    PNameFactory.getEstimatedSize(this.pkName) +
                    PNameFactory.getEstimatedSize(this.parentTableName) +
                    PNameFactory.getEstimatedSize(this.defaultFamilyName);
            int numPKColumns = 0;
            List<PColumn> pkColumns;
            PColumn[] allColumns;
            if (this.bucketNum != null) {
                // Add salt column to allColumns and pkColumns, but don't add to
                // columnsByName, since it should not be addressable via name.
                allColumns = new PColumn[this.columns.size()+1];
                allColumns[SALTING_COLUMN.getPosition()] = SALTING_COLUMN;
                pkColumns = Lists.newArrayListWithExpectedSize(this.columns.size()+1);
                ++numPKColumns;
            } else {
                allColumns = new PColumn[this.columns.size()];
                pkColumns = Lists.newArrayListWithExpectedSize(this.columns.size());
            }
            // Must do this as with the new method of storing diffs, we just care about
            // ordinal position relative order and not the true ordinal value itself.
            List<PColumn> sortedColumns = Lists.newArrayList(this.columns);
            Collections.sort(sortedColumns, new Comparator<PColumn>() {
                @Override
                public int compare(PColumn o1, PColumn o2) {
                    return Integer.compare(o1.getPosition(), o2.getPosition());
                }
            });

            int position = 0;
            if (this.bucketNum != null) {
                position = 1;
            }
            ListMultimap<String, PColumn> populateColumnsByName =
                    ArrayListMultimap.create(this.columns.size(), 1);
            Map<KVColumnFamilyQualifier, PColumn> populateKvColumnsByQualifiers =
                    Maps.newHashMapWithExpectedSize(this.columns.size());
            for (PColumn column : sortedColumns) {
                allColumns[position] = column;
                position++;
                PName familyName = column.getFamilyName();
                if (familyName == null) {
                    ++numPKColumns;
                }
                String columnName = column.getName().getString();
                if (populateColumnsByName.put(columnName, column)) {
                    int count = 0;
                    for (PColumn dupColumn : populateColumnsByName.get(columnName)) {
                        if (Objects.equal(familyName, dupColumn.getFamilyName())) {
                            count++;
                            if (count > 1) {
                                throw new ColumnAlreadyExistsException(this.schemaName.getString(),
                                        fullName.getString(), columnName);
                            }
                        }
                    }
                }
                byte[] cq = column.getColumnQualifierBytes();
                String cf = column.getFamilyName() != null ?
                        column.getFamilyName().getString() : null;
                if (cf != null && cq != null) {
                    KVColumnFamilyQualifier info = new KVColumnFamilyQualifier(cf, cq);
                    if (populateKvColumnsByQualifiers.get(info) != null) {
                        throw new ColumnAlreadyExistsException(this.schemaName.getString(),
                                fullName.getString(), columnName);
                    }
                    populateKvColumnsByQualifiers.put(info, column);
                }
            }
            estimatedSize += SizedUtil.sizeOfMap(allColumns.length, SizedUtil.POINTER_SIZE,
                    SizedUtil.sizeOfArrayList(1)); // for multi-map
            estimatedSize += SizedUtil.sizeOfMap(numPKColumns) +
                    SizedUtil.sizeOfMap(allColumns.length);

            RowKeySchemaBuilder builder = new RowKeySchemaBuilder(numPKColumns);
            // Two pass so that column order in column families matches overall column order
            // and to ensure that column family order is constant
            int maxExpectedSize = allColumns.length - numPKColumns;
            // Maintain iteration order so that column families are ordered as they are listed
            Map<PName, List<PColumn>> familyMap = Maps.newLinkedHashMap();
            PColumn rowTimestampCol = null;
            boolean hasColsRequiringUpgrade = false;
            for (PColumn column : allColumns) {
                PName familyName = column.getFamilyName();
                if (familyName == null) {
                    hasColsRequiringUpgrade |=
                            (column.getSortOrder() == SortOrder.DESC
                                    && (!column.getDataType().isFixedWidth()
                                    || column.getDataType() == PChar.INSTANCE
                                    || column.getDataType() == PFloat.INSTANCE
                                    || column.getDataType() == PDouble.INSTANCE
                                    || column.getDataType() == PBinary.INSTANCE) )
                                    || (column.getSortOrder() == SortOrder.ASC
                                        && column.getDataType() == PBinary.INSTANCE
                                        && column.getMaxLength() != null
                                        && column.getMaxLength() > 1);
                    pkColumns.add(column);
                    if (column.isRowTimestamp()) {
                        rowTimestampCol = column;
                    }
                }
                if (familyName == null) {
                    estimatedSize += column.getEstimatedSize(); // PK columns
                    builder.addField(column, column.isNullable(), column.getSortOrder());
                } else {
                    List<PColumn> columnsInFamily = familyMap.get(familyName);
                    if (columnsInFamily == null) {
                        columnsInFamily = Lists.newArrayListWithExpectedSize(maxExpectedSize);
                        familyMap.put(familyName, columnsInFamily);
                    }
                    columnsInFamily.add(column);
                }
            }
            int rowTimestampColPos;
            if (rowTimestampCol != null) {
                rowTimestampColPos = pkColumns.indexOf(rowTimestampCol);
            } else {
                rowTimestampColPos = -1;
            }

            Iterator<Map.Entry<PName,List<PColumn>>> iterator = familyMap.entrySet().iterator();
            PColumnFamily[] families = new PColumnFamily[familyMap.size()];
            ImmutableMap.Builder<String, PColumnFamily> familyByString = ImmutableMap.builder();
            ImmutableSortedMap.Builder<byte[], PColumnFamily> familyByBytes = ImmutableSortedMap
                    .orderedBy(Bytes.BYTES_COMPARATOR);
            for (int i = 0; i < families.length; i++) {
                Map.Entry<PName,List<PColumn>> entry = iterator.next();
                PColumnFamily family = new PColumnFamilyImpl(entry.getKey(), entry.getValue());
                families[i] = family;
                familyByString.put(family.getName().getString(), family);
                familyByBytes.put(family.getName().getBytes(), family);
                estimatedSize += family.getEstimatedSize();
            }
            estimatedSize += SizedUtil.sizeOfArrayList(families.length);
            estimatedSize += SizedUtil.sizeOfMap(families.length) * 2;
            for (PTable index : this.indexes) {
                estimatedSize += index.getEstimatedSize();
            }

            estimatedSize += PNameFactory.getEstimatedSize(this.parentName);
            for (PName physicalName : this.physicalNames) {
                estimatedSize += physicalName.getEstimatedSize();
            }
            // Populate the derived fields and return the builder
            return this.setName(fullName)
                    .setKey(new PTableKey(this.tenantId, fullName.getString()))
                    .setParentName(this.parentTableName == null ? null :
                            PNameFactory.newName(SchemaUtil.getTableName(
                                    this.parentSchemaName != null ?
                                            this.parentSchemaName.getString() : null,
                                    this.parentTableName.getString())))
                    .setColumnsByName(populateColumnsByName)
                    .setKvColumnsByQualifiers(populateKvColumnsByQualifiers)
                    .setAllColumns(ImmutableList.copyOf(allColumns))
                    .setHasColumnsRequiringUpgrade(hasColsRequiringUpgrade
                            | this.hasColumnsRequiringUpgrade)
                    .setPkColumns(ImmutableList.copyOf(pkColumns))
                    .setRowTimestampColPos(rowTimestampColPos)
                    // after hasDescVarLengthColumns is calculated
                    .setRowKeySchema(builder.rowKeyOrderOptimizable(
                            this.rowKeyOrderOptimizable || !this.hasColumnsRequiringUpgrade)
                            .build())
                    .setFamilies(ImmutableList.copyOf(families))
                    .setFamilyByBytes(familyByBytes.build())
                    .setFamilyByString(familyByString.build())
                    .setEstimatedSize(estimatedSize + this.rowKeySchema.getEstimatedSize());
        }

        public PTableImpl build() throws SQLException {
            // Note that we call initDerivedAttributes to populate derivable attributes if
            // this.columns is set in the PTableImpl.Builder object
            return (this.columns == null) ? new PTableImpl(this) :
                    new PTableImpl(this.initDerivedAttributes());
        }

    }

    @VisibleForTesting
    PTableImpl() {
        this(new PTableImpl.Builder()
                .setIndexes(Collections.emptyList())
                .setPhysicalNames(Collections.emptyList())
                .setRowKeySchema(RowKeySchema.EMPTY_SCHEMA));
    }

    // Private constructor used by the builder
    private PTableImpl(Builder builder) {
        this.key = builder.key;
        this.name = builder.name;
        this.schemaName = builder.schemaName;
        this.tableName = builder.tableName;
        this.tenantId = builder.tenantId;
        this.type = builder.type;
        this.state = builder.state;
        this.sequenceNumber = builder.sequenceNumber;
        this.timeStamp = builder.timeStamp;
        this.indexDisableTimestamp = builder.indexDisableTimestamp;
        this.pkColumns = builder.pkColumns;
        this.allColumns = builder.allColumns;
        this.excludedColumns = builder.excludedColumns;
        this.families = builder.families;
        this.familyByBytes = builder.familyByBytes;
        this.familyByString = builder.familyByString;
        this.columnsByName = builder.columnsByName;
        this.kvColumnsByQualifiers = builder.kvColumnsByQualifiers;
        this.pkName = builder.pkName;
        this.bucketNum = builder.bucketNum;
        this.rowKeySchema = builder.rowKeySchema;
        this.indexes = builder.indexes;
        this.parentName = builder.parentName;
        this.parentSchemaName = builder.parentSchemaName;
        this.parentTableName = builder.parentTableName;
        this.physicalNames = builder.physicalNames;
        this.isImmutableRows = builder.isImmutableRows;
        this.indexMaintainer = builder.indexMaintainer;
        this.indexMaintainersPtr = builder.indexMaintainersPtr;
        this.defaultFamilyName = builder.defaultFamilyName;
        this.viewStatement = builder.viewStatement;
        this.disableWAL = builder.disableWAL;
        this.multiTenant = builder.multiTenant;
        this.storeNulls = builder.storeNulls;
        this.transactionProvider = builder.transactionProvider;
        this.viewType = builder.viewType;
        this.viewIndexIdType = builder.viewIndexIdType;
        this.viewIndexId = builder.viewIndexId;
        this.estimatedSize = builder.estimatedSize;
        this.indexType = builder.indexType;
        this.baseColumnCount = builder.baseColumnCount;
        this.rowKeyOrderOptimizable = builder.rowKeyOrderOptimizable;
        this.hasColumnsRequiringUpgrade = builder.hasColumnsRequiringUpgrade;
        this.rowTimestampColPos = builder.rowTimestampColPos;
        this.updateCacheFrequency = builder.updateCacheFrequency;
        this.isNamespaceMapped = builder.isNamespaceMapped;
        this.autoPartitionSeqName = builder.autoPartitionSeqName;
        this.isAppendOnlySchema = builder.isAppendOnlySchema;
        this.immutableStorageScheme = builder.immutableStorageScheme;
        this.qualifierEncodingScheme = builder.qualifierEncodingScheme;
        this.encodedCQCounter = builder.encodedCQCounter;
        this.useStatsForParallelization = builder.useStatsForParallelization;
        this.phoenixTTL = builder.phoenixTTL;
        this.phoenixTTLHighWaterMark = builder.phoenixTTLHighWaterMark;
        this.viewModifiedPropSet = builder.viewModifiedPropSet;
        this.propertyValues = builder.propertyValues;
        this.lastDDLTimestamp = builder.lastDDLTimestamp;
        this.isChangeDetectionEnabled = builder.isChangeDetectionEnabled;
    }

    // When cloning table, ignore the salt column as it will be added back in the constructor
    public static List<PColumn> getColumnsToClone(PTable table) {
        return table == null ? Collections.<PColumn> emptyList() :
                (table.getBucketNum() == null ? table.getColumns() :
                        table.getColumns().subList(1, table.getColumns().size()));
    }

    /**
     * Get a PTableImpl.Builder from an existing PTable and set the builder columns
     * @param table Original PTable
     * @param columns Columns to set in the builder for the new PTable to be constructed
     * @return PTable builder object based on an existing PTable
     */
    public static PTableImpl.Builder builderWithColumns(PTable table, Collection<PColumn> columns) {
        return builderFromExisting(table).setColumns(columns);
    }

    /**
     * Get a PTableImpl.Builder from an existing PTable
     * @param table Original PTable
     */
    private static PTableImpl.Builder builderFromExisting(PTable table) {
        return new PTableImpl.Builder()
                .setType(table.getType())
                .setState(table.getIndexState())
                .setTimeStamp(table.getTimeStamp())
                .setIndexDisableTimestamp(table.getIndexDisableTimestamp())
                .setSequenceNumber(table.getSequenceNumber())
                .setImmutableRows(table.isImmutableRows())
                .setViewStatement(table.getViewStatement())
                .setDisableWAL(table.isWALDisabled())
                .setMultiTenant(table.isMultiTenant())
                .setStoreNulls(table.getStoreNulls())
                .setViewType(table.getViewType())
                .setViewIndexIdType(table.getviewIndexIdType())
                .setViewIndexId(table.getViewIndexId())
                .setIndexType(table.getIndexType())
                .setTransactionProvider(table.getTransactionProvider())
                .setUpdateCacheFrequency(table.getUpdateCacheFrequency())
                .setNamespaceMapped(table.isNamespaceMapped())
                .setAutoPartitionSeqName(table.getAutoPartitionSeqName())
                .setAppendOnlySchema(table.isAppendOnlySchema())
                .setImmutableStorageScheme(table.getImmutableStorageScheme() == null ?
                        ImmutableStorageScheme.ONE_CELL_PER_COLUMN : table.getImmutableStorageScheme())
                .setQualifierEncodingScheme(table.getEncodingScheme() == null ?
                        QualifierEncodingScheme.NON_ENCODED_QUALIFIERS : table.getEncodingScheme())
                .setBaseColumnCount(table.getBaseColumnCount())
                .setEncodedCQCounter(table.getEncodedCQCounter())
                .setUseStatsForParallelization(table.useStatsForParallelization())
                .setExcludedColumns(table.getExcludedColumns() == null ?
                        ImmutableList.of() : ImmutableList.copyOf(table.getExcludedColumns()))
                .setTenantId(table.getTenantId())
                .setSchemaName(table.getSchemaName())
                .setTableName(table.getTableName())
                .setPkName(table.getPKName())
                .setDefaultFamilyName(table.getDefaultFamilyName())
                .setRowKeyOrderOptimizable(table.rowKeyOrderOptimizable())
                .setBucketNum(table.getBucketNum())
                .setIndexes(table.getIndexes() == null ?
                        Collections.emptyList() : table.getIndexes())
                .setParentSchemaName(table.getParentSchemaName())
                .setParentTableName(table.getParentTableName())
                .setPhysicalNames(table.getPhysicalNames() == null ?
                        ImmutableList.of() : ImmutableList.copyOf(table.getPhysicalNames()))
                .setViewModifiedUseStatsForParallelization(table
                        .hasViewModifiedUseStatsForParallelization())
                .setViewModifiedUpdateCacheFrequency(table.hasViewModifiedUpdateCacheFrequency())
                .setViewModifiedPhoenixTTL(table.hasViewModifiedPhoenixTTL())
                .setPhoenixTTL(table.getPhoenixTTL())
                .setPhoenixTTLHighWaterMark(table.getPhoenixTTLHighWaterMark())
                .setLastDDLTimestamp(table.getLastDDLTimestamp())
                .setIsChangeDetectionEnabled(table.isChangeDetectionEnabled());
    }

    @Override
    public long getUpdateCacheFrequency() {
        return updateCacheFrequency;
    }
    
    @Override
    public boolean isMultiTenant() {
        return multiTenant;
    }

    @Override
    public boolean getStoreNulls() {
        return storeNulls;
    }

    @Override
    public ViewType getViewType() {
        return viewType;
    }

    @Override
    public int getEstimatedSize() {
        return estimatedSize;
    }

    public static void checkTenantId(PName tenantId) {
        // tenantId should be null or not empty
        Preconditions.checkArgument(tenantId == null || tenantId.getBytes().length > 0);
    }

    @Override
    public boolean isImmutableRows() {
        return isImmutableRows;
    }

    @Override
    public String toString() {
        return name.getString();
    }

    @Override
    public List<PColumn> getPKColumns() {
        return pkColumns;
    }

    @Override
    public final PName getName() {
        return name;
    }

    @Override
    public final PName getSchemaName() {
        return schemaName;
    }

    @Override
    public final PName getTableName() {
        return tableName;
    }

    @Override
    public final PTableType getType() {
        return type;
    }

    @Override
    public final List<PColumnFamily> getColumnFamilies() {
        return families;
    }

    @Override
    public int newKey(ImmutableBytesWritable key, byte[][] values) {
        List<PColumn> columns = getPKColumns();
        int nValues = values.length;
        while (nValues > 0 && (values[nValues-1] == null || values[nValues-1].length == 0)) {
            nValues--;
        }
        for (PColumn column : columns) {
            if (column.getExpressionStr() != null) {
                nValues++;
            }
        }
        int i = 0;
        TrustedByteArrayOutputStream os = new TrustedByteArrayOutputStream(SchemaUtil.estimateKeyLength(this));
        try {
            Integer bucketNum = this.getBucketNum();
            if (bucketNum != null) {
                // Write place holder for salt byte
                i++;
                os.write(QueryConstants.SEPARATOR_BYTE_ARRAY);
            }
            int nColumns = columns.size();
            PDataType type = null;
            SortOrder sortOrder = null;
            boolean wasNull = false;

            while (i < nValues && i < nColumns) {
                // Separate variable length column values in key with zero byte
                if (type != null && !type.isFixedWidth()) {
                    os.write(SchemaUtil.getSeparatorByte(rowKeyOrderOptimizable(), wasNull, sortOrder));
                }
                PColumn column = columns.get(i);
                sortOrder = column.getSortOrder();
                type = column.getDataType();
                // This will throw if the value is null and the type doesn't allow null
                byte[] byteValue = values[i++];
                if (byteValue == null) {
                    if (column.getExpressionStr() != null) {
                        try {
                            String url = PhoenixRuntime.JDBC_PROTOCOL
                                    + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR
                                    + PhoenixRuntime.CONNECTIONLESS;
                            PhoenixConnection conn = DriverManager.getConnection(url)
                                    .unwrap(PhoenixConnection.class);
                            StatementContext context =
                                    new StatementContext(new PhoenixStatement(conn));

                            ExpressionCompiler compiler = new ExpressionCompiler(context);
                            ParseNode defaultParseNode =
                                    new SQLParser(column.getExpressionStr()).parseExpression();
                            Expression defaultExpression = defaultParseNode.accept(compiler);
                            defaultExpression.evaluate(null, key);
                            column.getDataType().coerceBytes(key, null,
                                    defaultExpression.getDataType(),
                                    defaultExpression.getMaxLength(), defaultExpression.getScale(),
                                    defaultExpression.getSortOrder(),
                                    column.getMaxLength(), column.getScale(),
                                    column.getSortOrder());
                            byteValue = ByteUtil.copyKeyBytesIfNecessary(key);
                        } catch (SQLException e) { // should not be possible
                            throw new ConstraintViolationException(name.getString() + "."
                                    + column.getName().getString()
                                    + " failed to compile default value expression of "
                                    + column.getExpressionStr());
                        }
                    }
                    else {
                        byteValue = ByteUtil.EMPTY_BYTE_ARRAY;
                    }
                }
                wasNull = byteValue.length == 0;
                // An empty byte array return value means null. Do this,
                // since a type may have muliple representations of null.
                // For example, VARCHAR treats both null and an empty string
                // as null. This way we don't need to leak that part of the
                // implementation outside of PDataType by checking the value
                // here.
                if (byteValue.length == 0 && !column.isNullable()) {
                    throw new ConstraintViolationException(name.getString() + "." + column.getName().getString() + " may not be null");
                }
                Integer    maxLength = column.getMaxLength();
                Integer scale = column.getScale();
                key.set(byteValue);
                if (!type.isSizeCompatible(key, null, type, sortOrder, null, null, maxLength, scale)) {
                    throw new DataExceedsCapacityException(column.getDataType(), maxLength,
                            column.getScale(), column.getName().getString());
                }
                key.set(byteValue);
                type.pad(key, maxLength, sortOrder);
                byteValue = ByteUtil.copyKeyBytesIfNecessary(key);
                os.write(byteValue, 0, byteValue.length);
            }
            // Need trailing byte for DESC columns
            if (type != null && !type.isFixedWidth() && SchemaUtil.getSeparatorByte(rowKeyOrderOptimizable(), wasNull, sortOrder) == QueryConstants.DESC_SEPARATOR_BYTE) {
                os.write(QueryConstants.DESC_SEPARATOR_BYTE);
            }
            // If some non null pk values aren't set, then throw
            if (i < nColumns) {
                PColumn column = columns.get(i);
                if (column.getDataType().isFixedWidth() || !column.isNullable()) {
                    throw new ConstraintViolationException(name.getString() + "." + column.getName().getString() + " may not be null");
                }
            }
            if (nValues == 0) {
                throw new ConstraintViolationException("Primary key may not be null ("+ name.getString() + ")");
            }
            byte[] buf = os.getBuffer();
            int size = os.size();
            if (bucketNum != null) {
                buf[0] = SaltingUtil.getSaltingByte(buf, 1, size-1, bucketNum);
            }
            key.set(buf,0,size);
            return i;
        } finally {
            try {
                os.close();
            } catch (IOException e) {
                throw new RuntimeException(e); // Impossible
            }
        }
    }

    private PRow newRow(KeyValueBuilder builder, long ts, ImmutableBytesWritable key, int i, boolean hasOnDupKey, byte[]... values) {
        PRow row = new PRowImpl(builder, key, ts, getBucketNum(), hasOnDupKey);
        if (i < values.length) {
            for (PColumnFamily family : getColumnFamilies()) {
                for (PColumn column : family.getColumns()) {
                    row.setValue(column, values[i++]);
                    if (i == values.length)
                        return row;
                }
            }
        }
        return row;
    }

    @Override
    public PRow newRow(KeyValueBuilder builder, long ts, ImmutableBytesWritable key,
            boolean hasOnDupKey, byte[]... values) {
        return newRow(builder, ts, key, 0, hasOnDupKey, values);
    }

    @Override
    public PRow newRow(KeyValueBuilder builder, ImmutableBytesWritable key, boolean hasOnDupKey, byte[]... values) {
        return newRow(builder, HConstants.LATEST_TIMESTAMP, key, hasOnDupKey, values);
    }

    @Override
    public PColumn getColumnForColumnName(String name) throws ColumnNotFoundException, AmbiguousColumnException {
        List<PColumn> columns = columnsByName.get(name);
        int size = columns.size();
        if (size == 0) {
            String schemaNameStr = schemaName==null?null:schemaName.getString();
            String tableNameStr = tableName==null?null:tableName.getString();
            throw new ColumnNotFoundException(schemaNameStr, tableNameStr, null, name);

        }
        if (size > 1) {
            for (PColumn column : columns) {
                if (column.getFamilyName() == null || QueryConstants.DEFAULT_COLUMN_FAMILY.equals(column.getFamilyName().getString())) {
                    // Allow ambiguity with PK column or column in the default column family,
                    // since a PK column cannot be prefixed and a user would not know how to
                    // prefix a column in the default column family.
                    return column;
                }
            }
            throw new AmbiguousColumnException(name);
        }
        return columns.get(0);
    }
    
    @Override
    public PColumn getColumnForColumnQualifier(byte[] cf, byte[] cq) throws ColumnNotFoundException, AmbiguousColumnException {
        Preconditions.checkNotNull(cq);
        if (!EncodedColumnsUtil.usesEncodedColumnNames(this) || cf == null) {
            String columnName = (String)PVarchar.INSTANCE.toObject(cq);
            return getColumnForColumnName(columnName);
        } else {
            String family = (String)PVarchar.INSTANCE.toObject(cf);
            PColumn col = kvColumnsByQualifiers.get(new KVColumnFamilyQualifier(family, cq));
            if (col == null) {
                String schemaNameStr = schemaName==null?null:schemaName.getString();
                String tableNameStr = tableName==null?null:tableName.getString();
                throw new ColumnNotFoundException(schemaNameStr, tableNameStr, null,
                    "No column found for column qualifier " + qualifierEncodingScheme.decode(cq));
            }
            return col;
        }
    }

    /**
     *
     * PRow implementation for ColumnLayout.MULTI mode which stores column
     * values across multiple hbase columns.
     *
     *
     * @since 0.1
     */
    private class PRowImpl implements PRow {
        private final byte[] key;
        private final ImmutableBytesWritable keyPtr;
        // default to the generic builder, and only override when we know on the client
        private final KeyValueBuilder kvBuilder;

        private Mutation setValues;
        private Delete unsetValues;
        private Mutation deleteRow;
        private final long ts;
        private final boolean hasOnDupKey;
        // map from column name to value 
        private Map<PColumn, byte[]> columnToValueMap;
        // Map from the column family name to the list of dynamic columns in that column family.
        // If there are no dynamic columns in a column family, the key for that column family
        // will not exist in the map, rather than the corresponding value being an empty list.
        private Map<String, List<PColumn>> colFamToDynamicColumnsMapping;

        PRowImpl(KeyValueBuilder kvBuilder, ImmutableBytesWritable key, long ts, Integer bucketNum, boolean hasOnDupKey) {
            this.kvBuilder = kvBuilder;
            this.ts = ts;
            this.hasOnDupKey = hasOnDupKey;
            if (bucketNum != null) {
                this.key = SaltingUtil.getSaltedKey(key, bucketNum);
                this.keyPtr = new ImmutableBytesPtr(this.key);
            } else {
                this.keyPtr =  new ImmutableBytesPtr(key);
                this.key = ByteUtil.copyKeyBytesIfNecessary(key);
            }
            this.columnToValueMap = Maps.newHashMapWithExpectedSize(1);
            this.colFamToDynamicColumnsMapping = Maps.newHashMapWithExpectedSize(1);
            newMutations();
        }

        private void newMutations() {
            Mutation put = this.hasOnDupKey ? new Increment(this.key) : new Put(this.key);
            Delete delete = new Delete(this.key);
            if (isWALDisabled()) {
                put.setDurability(Durability.SKIP_WAL);
                delete.setDurability(Durability.SKIP_WAL);
            }
            this.setValues = put;
            this.unsetValues = delete;
       }

        @Override
        public List<Mutation> toRowMutations() {
            List<Mutation> mutations = new ArrayList<Mutation>(3);
            if (deleteRow != null) {
                // Include only deleteRow mutation if present because it takes precedence over all others
                mutations.add(deleteRow);
            } else {
                // store all columns for a given column family in a single cell instead of one column per cell in order to improve write performance
                if (immutableStorageScheme != null && immutableStorageScheme != ImmutableStorageScheme.ONE_CELL_PER_COLUMN) {
                    Put put = new Put(this.key);
                    if (isWALDisabled()) {
                        put.setDurability(Durability.SKIP_WAL);
                    }
                    // the setValues Put contains one cell per column, we need to convert it to a Put that contains a cell with all columns for a given column family
                    for (PColumnFamily family : families) {
                        byte[] columnFamily = family.getName().getBytes();
                        Collection<PColumn> columns = family.getColumns();
                        int maxEncodedColumnQualifier = Integer.MIN_VALUE;
                        for (PColumn column : columns) {
                            int qualifier = qualifierEncodingScheme.decode(column.getColumnQualifierBytes());
                            maxEncodedColumnQualifier = Math.max(maxEncodedColumnQualifier, qualifier);
                        }
                        Expression[] colValues = EncodedColumnsUtil.createColumnExpressionArray(maxEncodedColumnQualifier);
                        for (PColumn column : columns) {
                            if (columnToValueMap.containsKey(column)) {
                                int colIndex = qualifierEncodingScheme.decode(column.getColumnQualifierBytes())-QueryConstants.ENCODED_CQ_COUNTER_INITIAL_VALUE+1;
                                colValues[colIndex] = new LiteralExpression(columnToValueMap.get(column));
                            }
                        }
                        
                        List<Expression> children = Arrays.asList(colValues);
                        // we use SingleCellConstructorExpression to serialize all the columns into a single byte[]
                        SingleCellConstructorExpression singleCellConstructorExpression = new SingleCellConstructorExpression(immutableStorageScheme, children);
                        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                        singleCellConstructorExpression.evaluate(null, ptr);
                        ImmutableBytesPtr colFamilyPtr = new ImmutableBytesPtr(columnFamily);
                        addQuietly(put, kvBuilder.buildPut(keyPtr,
                            colFamilyPtr, QueryConstants.SINGLE_KEYVALUE_COLUMN_QUALIFIER_BYTES_PTR, ts, ptr));
                    }
                    // Preserve the attributes of the original mutation
                    Map<String, byte[]> attrsMap = setValues.getAttributesMap();
                    setValues = put;
                    for (String attrKey : attrsMap.keySet()) {
                        setValues.setAttribute(attrKey, attrsMap.get(attrKey));
                    }
                }
                // Because we cannot enforce a not null constraint on a KV column (since we don't know if the row exists when
                // we upsert it), so instead add a KV that is always empty. This allows us to imitate SQL semantics given the
                // way HBase works.
                Pair<byte[], byte[]> emptyKvInfo = EncodedColumnsUtil.getEmptyKeyValueInfo(PTableImpl.this);
                addQuietly(setValues, kvBuilder.buildPut(keyPtr,
                    SchemaUtil.getEmptyColumnFamilyPtr(PTableImpl.this),
                    new ImmutableBytesPtr(emptyKvInfo.getFirst()), ts,
                    new ImmutableBytesPtr(emptyKvInfo.getSecond())));
                mutations.add(setValues);
                if (!unsetValues.isEmpty()) {
                    mutations.add(unsetValues);
                }
            }
            return mutations;
        }

        private void removeIfPresent(Mutation m, byte[] family, byte[] qualifier) {
            Map<byte[],List<Cell>> familyMap = m.getFamilyCellMap();
            List<Cell> kvs = familyMap.get(family);
            if (kvs != null) {
                Iterator<Cell> iterator = kvs.iterator();
                while (iterator.hasNext()) {
                    Cell kv = iterator.next();
                    if (Bytes.compareTo(kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength(),
                          qualifier, 0, qualifier.length) == 0) {
                        iterator.remove();
                        break;
                    }
                }
            }
        }

        @Override
        public void setValue(PColumn column, byte[] byteValue) {
            deleteRow = null;
            byte[] family = column.getFamilyName().getBytes();
            byte[] qualifier = column.getColumnQualifierBytes();
            ImmutableBytesPtr qualifierPtr = new ImmutableBytesPtr(qualifier);
            PDataType<?> type = column.getDataType();
            // Check null, since some types have no byte representation for null
            if (byteValue == null) {
                byteValue = ByteUtil.EMPTY_BYTE_ARRAY;
            }
            boolean isNull = type.isNull(byteValue);
            if (isNull && !column.isNullable()) {
                throw new ConstraintViolationException(name.getString() + "." + column.getName().getString() + 
                        " may not be null");
            } else if (isNull && PTableImpl.this.isImmutableRows() && column.getExpressionStr() == null) {
                // Store nulls for immutable tables otherwise default value would be used
                removeIfPresent(setValues, family, qualifier);
                removeIfPresent(unsetValues, family, qualifier);
            } else if (isNull && !getStoreNulls() && !this.hasOnDupKey && column.getExpressionStr() == null) {
                // Cannot use column delete marker when row has ON DUPLICATE KEY clause
                // because we cannot change a Delete mutation to a Put mutation in the
                // case of updates occurring due to the execution of the clause.
                removeIfPresent(setValues, family, qualifier);
                deleteQuietly(unsetValues, kvBuilder, kvBuilder.buildDeleteColumns(keyPtr, column
                            .getFamilyName().getBytesPtr(), qualifierPtr, ts));
            } else {
                ImmutableBytesWritable ptr = new ImmutableBytesWritable(byteValue);
                Integer    maxLength = column.getMaxLength();
                Integer scale = column.getScale();
                SortOrder sortOrder = column.getSortOrder();
                if (!type.isSizeCompatible(ptr, null, type, sortOrder, null, null, maxLength, scale)) {
                    throw new DataExceedsCapacityException(column.getDataType(), maxLength,
                            column.getScale(), column.getName().getString());
                }
                ptr.set(byteValue);
                type.pad(ptr, maxLength, sortOrder);
                removeIfPresent(unsetValues, family, qualifier);
                // store all columns for a given column family in a single cell instead of one column per cell in order to improve write performance
                // we don't need to do anything with unsetValues as it is only used when storeNulls is false, storeNulls is always true when storeColsInSingleCell is true
                if (immutableStorageScheme == ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS) {
                    columnToValueMap.put(column, ptr.get());
                }
                else {
                    removeIfPresent(unsetValues, family, qualifier);
                    addQuietly(setValues, kvBuilder.buildPut(keyPtr,
                        column.getFamilyName().getBytesPtr(), qualifierPtr,
                        ts, ptr));
                }
                String fam = Bytes.toString(family);
                if (column.isDynamic()) {
                    if (!this.colFamToDynamicColumnsMapping.containsKey(fam)) {
                        this.colFamToDynamicColumnsMapping.put(fam, new ArrayList<>());
                    }
                    this.colFamToDynamicColumnsMapping.get(fam).add(column);
                }
            }
        }

        /**
         * Add attributes to the Put mutations indicating that we need to add shadow cells to Puts
         * to store dynamic column metadata. See
         * {@link org.apache.phoenix.coprocessor.ScanRegionObserver#preBatchMutate(ObserverContext,
         * MiniBatchOperationInProgress)}
         */
        public boolean setAttributesForDynamicColumnsIfReqd() {
            if (this.colFamToDynamicColumnsMapping == null ||
                    this.colFamToDynamicColumnsMapping.isEmpty()) {
                return false;
            }
            boolean attrsForDynColsSet = false;
            for (Entry<String, List<PColumn>> colFamToDynColsList :
                    this.colFamToDynamicColumnsMapping.entrySet()) {
                DynamicColumnMetaDataProtos.DynamicColumnMetaData.Builder builder =
                        DynamicColumnMetaDataProtos.DynamicColumnMetaData.newBuilder();
                for (PColumn dynCol : colFamToDynColsList.getValue()) {
                    builder.addDynamicColumns(PColumnImpl.toProto(dynCol));
                }
                if (builder.getDynamicColumnsCount() != 0) {
                    // The attribute key is the column family name and the value is the
                    // serialized list of dynamic columns
                    setValues.setAttribute(colFamToDynColsList.getKey(),
                            builder.build().toByteArray());
                    attrsForDynColsSet = true;
                }
            }
            return attrsForDynColsSet;
        }

        @Override public void setAttributeToProcessDynamicColumnsMetadata() {
            setValues.setAttribute(DYNAMIC_COLUMN_METADATA_STORED_FOR_MUTATION, TRUE_BYTES);
        }

        @Override
        public void delete() {
            newMutations();
            Delete delete = new Delete(key);
            if (families.isEmpty()) {
                delete.addFamily(SchemaUtil.getEmptyColumnFamily(PTableImpl.this), ts);
            } else {
                for (PColumnFamily colFamily : families) {
                    delete.addFamily(colFamily.getName().getBytes(), ts);
                }
            }
            deleteRow = delete;
            if (isWALDisabled()) {
                deleteRow.setDurability(Durability.SKIP_WAL);
            }
        }
        
    }

    @Override
    public PColumnFamily getColumnFamily(String familyName) throws ColumnFamilyNotFoundException {
        PColumnFamily family = familyByString.get(familyName);
        if (family == null) {
            String schemaNameStr = schemaName==null?null:schemaName.getString();
            String tableNameStr = tableName==null?null:tableName.getString();
            throw new ColumnFamilyNotFoundException(schemaNameStr, tableNameStr, familyName);
        }
        return family;
    }

    @Override
    public PColumnFamily getColumnFamily(byte[] familyBytes) throws ColumnFamilyNotFoundException {
        PColumnFamily family = familyByBytes.get(familyBytes);
        if (family == null) {
            String familyName = Bytes.toString(familyBytes);
            String schemaNameStr = schemaName==null?null:schemaName.getString();
            String tableNameStr = tableName==null?null:tableName.getString();
            throw new ColumnFamilyNotFoundException(schemaNameStr, tableNameStr, familyName);
        }
        return family;
    }

    @Override
    public List<PColumn> getColumns() {
        return allColumns;
    }
    
    @Override
    public List<PColumn> getExcludedColumns() {
        return excludedColumns;
    }

    @Override
    public long getSequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public long getTimeStamp() {
        return timeStamp;
    }

    @Override
    public long getIndexDisableTimestamp() {
        return indexDisableTimestamp;
    }
    
    @Override
    public PColumn getPKColumn(String name) throws ColumnNotFoundException {
        List<PColumn> columns = columnsByName.get(name);
        int size = columns.size();
        if (size == 0) {
            String schemaNameStr = schemaName==null?null:schemaName.getString();
            String tableNameStr = tableName==null?null:tableName.getString();
            throw new ColumnNotFoundException(schemaNameStr, tableNameStr, null, name);
        }
        if (size > 1) {
            do {
                PColumn column = columns.get(--size);
                if (column.getFamilyName() == null) {
                    return column;
                }
            } while (size > 0);
            String schemaNameStr = schemaName==null?null:schemaName.getString();
            String tableNameStr = tableName==null?null:tableName.getString();
            throw new ColumnNotFoundException(schemaNameStr, tableNameStr, null, name);
        }
        return columns.get(0);
    }

    @Override
    public PName getPKName() {
        return pkName;
    }

    @Override
    public RowKeySchema getRowKeySchema() {
        return rowKeySchema;
    }

    @Override
    public Integer getBucketNum() {
        return bucketNum;
    }

    @Override
    public List<PTable> getIndexes() {
        return indexes;
    }

    @Override
    public PIndexState getIndexState() {
        return state;
    }

    @Override
    public PName getParentTableName() {
        // a view on a table will not have a parent name but will have a physical table name (which is the parent)
        return (type!=PTableType.VIEW || parentName!=null) ? parentTableName : 
            PNameFactory.newName(SchemaUtil.getTableNameFromFullName(getPhysicalName().getBytes()));
    }

    @Override
    public PName getParentName() {
        // a view on a table will not have a parent name but will have a physical table name (which is the parent)
        return (type!=PTableType.VIEW || parentName!=null) ? parentName : getPhysicalName();
    }

    @Override
    public synchronized IndexMaintainer getIndexMaintainer(PTable dataTable, PhoenixConnection connection) {
        if (indexMaintainer == null) {
            indexMaintainer = IndexMaintainer.create(dataTable, this, connection);
        }
        return indexMaintainer;
    }

    @Override
    public synchronized boolean getIndexMaintainers(ImmutableBytesWritable ptr, PhoenixConnection connection) {
        if (indexMaintainersPtr == null || indexMaintainersPtr.getLength()==0) {
            indexMaintainersPtr = new ImmutableBytesWritable();
            if (indexes.isEmpty()) {
                indexMaintainersPtr.set(ByteUtil.EMPTY_BYTE_ARRAY);
            } else {
                IndexMaintainer.serialize(this, indexMaintainersPtr, connection);
            }
        }
        ptr.set(indexMaintainersPtr.get(), indexMaintainersPtr.getOffset(), indexMaintainersPtr.getLength());
        return indexMaintainersPtr.getLength() > 0;
    }

    @Override
    public PName getPhysicalName() {
        if (physicalNames.isEmpty()) {
            return SchemaUtil.getPhysicalHBaseTableName(schemaName, tableName, isNamespaceMapped);
        } else {
            return PNameFactory.newName(physicalNames.get(0).getBytes());
        }
    }

    @Override
    public List<PName> getPhysicalNames() {
        return !physicalNames.isEmpty() ? physicalNames : Lists.newArrayList(getPhysicalName());
    }

    @Override
    public PName getDefaultFamilyName() {
        return defaultFamilyName;
    }

    @Override
    public String getViewStatement() {
        return viewStatement;
    }

    @Override
    public boolean isWALDisabled() {
        return disableWAL;
    }

    @Override
    public Long getViewIndexId() {
        return viewIndexId;
    }

    @Override
    public PDataType getviewIndexIdType() {
        return viewIndexIdType;
    }

    @Override
    public PName getTenantId() {
        return tenantId;
    }

    @Override
    public IndexType getIndexType() {
        return indexType;
    }
    
    /**
     * Construct a PTable instance from ProtoBuffered PTable instance
     * @param table
     */
    public static PTable createFromProto(PTableProtos.PTable table) {
        if (table==null)
            return null;
        PName tenantId = null;
        if(table.hasTenantId()){
            tenantId = PNameFactory.newName(table.getTenantId().toByteArray());
        }
        PName schemaName = PNameFactory.newName(table.getSchemaNameBytes().toByteArray());
        PName tableName = PNameFactory.newName(table.getTableNameBytes().toByteArray());
        PTableType tableType = PTableType.values()[table.getTableType().ordinal()];
        PIndexState indexState = null;
        if (table.hasIndexState()) {
            indexState = PIndexState.fromSerializedValue(table.getIndexState());
        }
        Long viewIndexId = null;
        if (table.hasViewIndexId()) {
            viewIndexId = table.getViewIndexId();
        }
        PDataType viewIndexIdType = table.hasViewIndexIdType()
                ? PDataType.fromTypeId(table.getViewIndexIdType())
                : MetaDataUtil.getLegacyViewIndexIdDataType();
        IndexType indexType = IndexType.getDefault();
        if(table.hasIndexType()){
            indexType = IndexType.fromSerializedValue(table.getIndexType().toByteArray()[0]);
        }
        long sequenceNumber = table.getSequenceNumber();
        long timeStamp = table.getTimeStamp();
        long indexDisableTimestamp = table.getIndexDisableTimestamp();
        PName pkName = null;
        if (table.hasPkNameBytes()) {
            pkName = PNameFactory.newName(table.getPkNameBytes().toByteArray());
        }
        int bucketNum = table.getBucketNum();
        List<PColumn> columns = Lists.newArrayListWithExpectedSize(table.getColumnsCount());
        for (PTableProtos.PColumn curPColumnProto : table.getColumnsList()) {
            columns.add(PColumnImpl.createFromProto(curPColumnProto));
        }
        List<PTable> indexes = Lists.newArrayListWithExpectedSize(table.getIndexesCount());
        for (PTableProtos.PTable curPTableProto : table.getIndexesList()) {
            indexes.add(createFromProto(curPTableProto));
        }

        boolean isImmutableRows = table.getIsImmutableRows();
        PName parentSchemaName = null;
        PName parentTableName = null;
        if (table.hasParentNameBytes()) {
            parentSchemaName = PNameFactory.newName(SchemaUtil.getSchemaNameFromFullName((table.getParentNameBytes().toByteArray())));
            parentTableName = PNameFactory.newName(SchemaUtil.getTableNameFromFullName(table.getParentNameBytes().toByteArray()));
        }
        PName defaultFamilyName = null;
        if (table.hasDefaultFamilyName()) {
            defaultFamilyName = PNameFactory.newName(table.getDefaultFamilyName().toByteArray());
        }
        boolean disableWAL = table.getDisableWAL();
        boolean multiTenant = table.getMultiTenant();
        boolean storeNulls = table.getStoreNulls();
        TransactionFactory.Provider transactionProvider = null;
        if (table.hasTransactionProvider()) {
            transactionProvider = TransactionFactory.Provider.fromCode(table.getTransactionProvider());
        } else if (table.hasTransactional()) {
            // For backward compatibility prior to transactionProvider field
            transactionProvider = TransactionFactory.Provider.TEPHRA;
        }
        ViewType viewType = null;
        String viewStatement = null;
        if (tableType == PTableType.VIEW) {
            viewType = ViewType.fromSerializedValue(table.getViewType().toByteArray()[0]);
        }
        if(table.hasViewStatement()){
            viewStatement = (String) PVarchar.INSTANCE.toObject(table.getViewStatement().toByteArray());
        }
        List<PName> physicalNames = Lists.newArrayListWithExpectedSize(table.getPhysicalNamesCount());
        for(int i = 0; i < table.getPhysicalNamesCount(); i++) {
            physicalNames.add(PNameFactory.newName(table.getPhysicalNames(i).toByteArray()));
        }
        int baseColumnCount = -1;
        if (table.hasBaseColumnCount()) {
            baseColumnCount = table.getBaseColumnCount();
        }

        boolean rowKeyOrderOptimizable = false;
        if (table.hasRowKeyOrderOptimizable()) {
            rowKeyOrderOptimizable = table.getRowKeyOrderOptimizable();
        }
        long updateCacheFrequency = 0;
        if (table.hasUpdateCacheFrequency()) {
            updateCacheFrequency = table.getUpdateCacheFrequency();
        }
        boolean isNamespaceMapped=false;
        if (table.hasIsNamespaceMapped()) {
            isNamespaceMapped = table.getIsNamespaceMapped();
        }
        String autoPartitionSeqName = null;
        if (table.hasAutoParititonSeqName()) {
            autoPartitionSeqName = table.getAutoParititonSeqName();
        }
        boolean isAppendOnlySchema = false;
        if (table.hasIsAppendOnlySchema()) {
            isAppendOnlySchema = table.getIsAppendOnlySchema();
        }
        // For backward compatibility. Clients older than 4.10 will always have non-encoded immutable tables.
        ImmutableStorageScheme storageScheme = ImmutableStorageScheme.ONE_CELL_PER_COLUMN;
        if (table.hasStorageScheme()) {
            storageScheme = ImmutableStorageScheme.fromSerializedValue(table.getStorageScheme().toByteArray()[0]);
        }
        // For backward compatibility. Clients older than 4.10 will always have non-encoded qualifiers.
        QualifierEncodingScheme qualifierEncodingScheme = QualifierEncodingScheme.NON_ENCODED_QUALIFIERS;
        if (table.hasEncodingScheme()) {
            qualifierEncodingScheme = QualifierEncodingScheme.fromSerializedValue(table.getEncodingScheme().toByteArray()[0]);
        }
        EncodedCQCounter encodedColumnQualifierCounter;
        if ((!EncodedColumnsUtil.usesEncodedColumnNames(qualifierEncodingScheme) || tableType == PTableType.VIEW)) {
            encodedColumnQualifierCounter = PTable.EncodedCQCounter.NULL_COUNTER;
        }
        else {
            encodedColumnQualifierCounter = new EncodedCQCounter();
            if (table.getEncodedCQCountersList() != null) {
                for (org.apache.phoenix.coprocessor.generated.PTableProtos.EncodedCQCounter cqCounterFromProto : table.getEncodedCQCountersList()) {
                    encodedColumnQualifierCounter.setValue(cqCounterFromProto.getColFamily(), cqCounterFromProto.getCounter());
                }
            }
        }
        Boolean useStatsForParallelization = null;
        if (table.hasUseStatsForParallelization()) {
            useStatsForParallelization = table.getUseStatsForParallelization();
        }
        long phoenixTTL = PHOENIX_TTL_NOT_DEFINED;
        if (table.hasPhoenixTTL()) {
            phoenixTTL = table.getPhoenixTTL();
        }
        long phoenixTTLHighWaterMark = MIN_PHOENIX_TTL_HWM;
        if (table.hasPhoenixTTLHighWaterMark()) {
            phoenixTTLHighWaterMark = table.getPhoenixTTLHighWaterMark();
        }

        // for older clients just use the value of the properties that are set on the view
        boolean viewModifiedUpdateCacheFrequency = true;
        boolean viewModifiedUseStatsForParallelization = true;
        boolean viewModifiedPhoenixTTL = true;
        if (table.hasViewModifiedUpdateCacheFrequency()) {
            viewModifiedUpdateCacheFrequency = table.getViewModifiedUpdateCacheFrequency();
        }
        if (table.hasViewModifiedUseStatsForParallelization()) {
            viewModifiedUseStatsForParallelization = table.getViewModifiedUseStatsForParallelization();
        }
        if (table.hasViewModifiedPhoenixTTL()) {
            viewModifiedPhoenixTTL = table.getViewModifiedPhoenixTTL();
        }
        Long lastDDLTimestamp = null;
        if (table.hasLastDDLTimestamp()) {
            lastDDLTimestamp = table.getLastDDLTimestamp();
        }
        boolean isChangeDetectionEnabled = false;
        if (table.hasChangeDetectionEnabled()) {
            isChangeDetectionEnabled = table.getChangeDetectionEnabled();
        }
        try {
            return new PTableImpl.Builder()
                    .setType(tableType)
                    .setState(indexState)
                    .setTimeStamp(timeStamp)
                    .setIndexDisableTimestamp(indexDisableTimestamp)
                    .setSequenceNumber(sequenceNumber)
                    .setImmutableRows(isImmutableRows)
                    .setViewStatement(viewStatement)
                    .setDisableWAL(disableWAL)
                    .setMultiTenant(multiTenant)
                    .setStoreNulls(storeNulls)
                    .setViewType(viewType)
                    .setViewIndexIdType(viewIndexIdType)
                    .setViewIndexId(viewIndexId)
                    .setIndexType(indexType)
                    .setTransactionProvider(transactionProvider)
                    .setUpdateCacheFrequency(updateCacheFrequency)
                    .setNamespaceMapped(isNamespaceMapped)
                    .setAutoPartitionSeqName(autoPartitionSeqName)
                    .setAppendOnlySchema(isAppendOnlySchema)
                    // null check for backward compatibility and sanity. If any of the two below is null,
                    // then it means the table is a non-encoded table.
                    .setImmutableStorageScheme(storageScheme == null ?
                            ImmutableStorageScheme.ONE_CELL_PER_COLUMN : storageScheme)
                    .setQualifierEncodingScheme(qualifierEncodingScheme == null ?
                            QualifierEncodingScheme.NON_ENCODED_QUALIFIERS : qualifierEncodingScheme)
                    .setBaseColumnCount(baseColumnCount)
                    .setEncodedCQCounter(encodedColumnQualifierCounter)
                    .setUseStatsForParallelization(useStatsForParallelization)
                    .setPhoenixTTL(phoenixTTL)
                    .setPhoenixTTLHighWaterMark(phoenixTTLHighWaterMark)
                    .setExcludedColumns(ImmutableList.of())
                    .setTenantId(tenantId)
                    .setSchemaName(schemaName)
                    .setTableName(tableName)
                    .setPkName(pkName)
                    .setDefaultFamilyName(defaultFamilyName)
                    .setRowKeyOrderOptimizable(rowKeyOrderOptimizable)
                    .setBucketNum((bucketNum == NO_SALTING) ? null : bucketNum)
                    .setIndexes(indexes == null ? Collections.emptyList() : indexes)
                    .setParentSchemaName(parentSchemaName)
                    .setParentTableName(parentTableName)
                    .setPhysicalNames(physicalNames == null ?
                            ImmutableList.of() : ImmutableList.copyOf(physicalNames))
                    .setColumns(columns)
                    .setViewModifiedUpdateCacheFrequency(viewModifiedUpdateCacheFrequency)
                    .setViewModifiedUseStatsForParallelization(viewModifiedUseStatsForParallelization)
                    .setViewModifiedPhoenixTTL(viewModifiedPhoenixTTL)
                    .setLastDDLTimestamp(lastDDLTimestamp)
                    .setIsChangeDetectionEnabled(isChangeDetectionEnabled)
                    .build();
        } catch (SQLException e) {
            throw new RuntimeException(e); // Impossible
        }
    }

    public static PTableProtos.PTable toProto(PTable table) {
      PTableProtos.PTable.Builder builder = PTableProtos.PTable.newBuilder();
      if(table.getTenantId() != null){
        builder.setTenantId(ByteStringer.wrap(table.getTenantId().getBytes()));
      }
      builder.setSchemaNameBytes(ByteStringer.wrap(table.getSchemaName().getBytes()));
      builder.setTableNameBytes(ByteStringer.wrap(table.getTableName().getBytes()));
      builder.setTableType(ProtobufUtil.toPTableTypeProto(table.getType()));
      if (table.getType() == PTableType.INDEX) {
        if(table.getIndexState() != null) {
          builder.setIndexState(table.getIndexState().getSerializedValue());
        }
        if(table.getViewIndexId() != null) {
          builder.setViewIndexId(table.getViewIndexId());
          builder.setViewIndexIdType(table.getviewIndexIdType().getSqlType());
		}
        if(table.getIndexType() != null) {
            builder.setIndexType(ByteStringer.wrap(new byte[]{table.getIndexType().getSerializedValue()}));
        }
      }
      builder.setSequenceNumber(table.getSequenceNumber());
      builder.setTimeStamp(table.getTimeStamp());
      PName tmp = table.getPKName();
      if (tmp != null) {
        builder.setPkNameBytes(ByteStringer.wrap(tmp.getBytes()));
      }
      Integer bucketNum = table.getBucketNum();
      int offset = 0;
      if(bucketNum == null){
        builder.setBucketNum(NO_SALTING);
      } else {
        offset = 1;
        builder.setBucketNum(bucketNum);
      }
      List<PColumn> columns = table.getColumns();
      int columnSize = columns.size();
      for (int i = offset; i < columnSize; i++) {
          PColumn column = columns.get(i);
          builder.addColumns(PColumnImpl.toProto(column));
      }
      List<PTable> indexes = table.getIndexes();
      for (PTable curIndex : indexes) {
        builder.addIndexes(toProto(curIndex));
      }
      builder.setIsImmutableRows(table.isImmutableRows());
      // TODO remove this field in 5.0 release
      if (table.getParentName() != null) {
          builder.setDataTableNameBytes(ByteStringer.wrap(table.getParentTableName().getBytes()));
      }
      if (table.getParentName() !=null) {
          builder.setParentNameBytes(ByteStringer.wrap(table.getParentName().getBytes()));
      }
      if (table.getDefaultFamilyName()!= null) {
        builder.setDefaultFamilyName(ByteStringer.wrap(table.getDefaultFamilyName().getBytes()));
      }
      builder.setDisableWAL(table.isWALDisabled());
      builder.setMultiTenant(table.isMultiTenant());
      builder.setStoreNulls(table.getStoreNulls());
      if (table.getTransactionProvider() != null) {
          builder.setTransactionProvider(table.getTransactionProvider().getCode());
      }
      if(table.getType() == PTableType.VIEW){
        builder.setViewType(ByteStringer.wrap(new byte[]{table.getViewType().getSerializedValue()}));
      }
      if(table.getViewStatement()!=null){
        builder.setViewStatement(ByteStringer.wrap(PVarchar.INSTANCE.toBytes(table.getViewStatement())));
      }
      for (int i = 0; i < table.getPhysicalNames().size(); i++) {
        builder.addPhysicalNames(ByteStringer.wrap(table.getPhysicalNames().get(i).getBytes()));
      }
      builder.setBaseColumnCount(table.getBaseColumnCount());
      builder.setRowKeyOrderOptimizable(table.rowKeyOrderOptimizable());
      builder.setUpdateCacheFrequency(table.getUpdateCacheFrequency());
      builder.setIndexDisableTimestamp(table.getIndexDisableTimestamp());
      builder.setIsNamespaceMapped(table.isNamespaceMapped());
      if (table.getAutoPartitionSeqName() != null) {
          builder.setAutoParititonSeqName(table.getAutoPartitionSeqName());
      }
      builder.setIsAppendOnlySchema(table.isAppendOnlySchema());
      if (table.getImmutableStorageScheme() != null) {
          builder.setStorageScheme(ByteStringer.wrap(new byte[]{table.getImmutableStorageScheme().getSerializedMetadataValue()}));
      }
      if (table.getEncodedCQCounter() != null) {
          Map<String, Integer> values = table.getEncodedCQCounter().values();
          for (Entry<String, Integer> cqCounter : values.entrySet()) {
              org.apache.phoenix.coprocessor.generated.PTableProtos.EncodedCQCounter.Builder cqBuilder = org.apache.phoenix.coprocessor.generated.PTableProtos.EncodedCQCounter.newBuilder();
              cqBuilder.setColFamily(cqCounter.getKey());
              cqBuilder.setCounter(cqCounter.getValue());
              builder.addEncodedCQCounters(cqBuilder.build());
          }
      }
      if (table.getEncodingScheme() != null) {
          builder.setEncodingScheme(ByteStringer.wrap(new byte[]{table.getEncodingScheme().getSerializedMetadataValue()}));
      }
      if (table.useStatsForParallelization() != null) {
          builder.setUseStatsForParallelization(table.useStatsForParallelization());
      }
      builder.setPhoenixTTL(table.getPhoenixTTL());
      builder.setPhoenixTTLHighWaterMark(table.getPhoenixTTLHighWaterMark());
      builder.setViewModifiedUpdateCacheFrequency(table.hasViewModifiedUpdateCacheFrequency());
      builder.setViewModifiedUseStatsForParallelization(table.hasViewModifiedUseStatsForParallelization());
      builder.setViewModifiedPhoenixTTL(table.hasViewModifiedPhoenixTTL());
      if (table.getLastDDLTimestamp() != null) {
          builder.setLastDDLTimestamp(table.getLastDDLTimestamp());
      }
      builder.setChangeDetectionEnabled(table.isChangeDetectionEnabled());
      return builder.build();
    }

    @Override
    public PTableKey getKey() {
        return key;
    }

    @Override
    public PName getParentSchemaName() {
        // a view on a table will not have a parent name but will have a physical table name (which is the parent)
        return (type!=PTableType.VIEW || parentName!=null) ? parentSchemaName : 
            PNameFactory.newName(SchemaUtil.getSchemaNameFromFullName(getPhysicalName().getBytes()));
    }

    @Override
    public TransactionFactory.Provider getTransactionProvider() {
        return transactionProvider;
    }
    
    @Override
    public final boolean isTransactional() {
        return transactionProvider != null;
    }

    @Override
    public int getBaseColumnCount() {
        return baseColumnCount;
    }

    @Override
    public boolean rowKeyOrderOptimizable() {
        return rowKeyOrderOptimizable || !hasColumnsRequiringUpgrade;
    }

    @Override
    public int getRowTimestampColPos() {
        return rowTimestampColPos;
    }

    @Override
    public boolean isNamespaceMapped() {
        return isNamespaceMapped;
    }
    
    @Override
    public String getAutoPartitionSeqName() {
        return autoPartitionSeqName;
    }
    
    @Override
    public boolean isAppendOnlySchema() {
        return isAppendOnlySchema;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((key == null) ? 0 : key.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (! (obj instanceof PTable)) return false;
        PTable other = (PTable) obj;
        if (key == null) {
            if (other.getKey() != null) return false;
        } else if (!key.equals(other.getKey())) return false;
        return true;
    }
    
    @Override
    public ImmutableStorageScheme getImmutableStorageScheme() {
        return immutableStorageScheme;
    }
    
    @Override
    public EncodedCQCounter getEncodedCQCounter() {
        return encodedCQCounter;
    }

    @Override
    public QualifierEncodingScheme getEncodingScheme() {
        return qualifierEncodingScheme;
    }
    
    @Override
    public Boolean useStatsForParallelization() {
        return useStatsForParallelization;
    }

    @Override
    public long getPhoenixTTL() {
        return phoenixTTL;
    }

    @Override
    public long getPhoenixTTLHighWaterMark() {
        return phoenixTTLHighWaterMark;
    }

    @Override public boolean hasViewModifiedUpdateCacheFrequency() {
        return viewModifiedPropSet.get(VIEW_MODIFIED_UPDATE_CACHE_FREQUENCY_BIT_SET_POS);
    }

    @Override public boolean hasViewModifiedUseStatsForParallelization() {
        return viewModifiedPropSet.get(VIEW_MODIFIED_USE_STATS_FOR_PARALLELIZATION_BIT_SET_POS);
    }

    @Override public boolean hasViewModifiedPhoenixTTL() {
        return viewModifiedPropSet.get(VIEW_MODIFIED_PHOENIX_TTL_BIT_SET_POS);
    }

    @Override
    public Long getLastDDLTimestamp() {
        return lastDDLTimestamp;
    }

    @Override
    public boolean isChangeDetectionEnabled() {
        return isChangeDetectionEnabled;
    }

    private static final class KVColumnFamilyQualifier {
        @Nonnull
        private final String colFamilyName;
        @Nonnull
        private final byte[] colQualifier;

        public KVColumnFamilyQualifier(String colFamilyName, byte[] colQualifier) {
            Preconditions.checkArgument(colFamilyName != null && colQualifier != null,
                "None of the arguments, column family name or column qualifier can be null");
            this.colFamilyName = colFamilyName;
            this.colQualifier = colQualifier;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + colFamilyName.hashCode();
            result = prime * result + Arrays.hashCode(colQualifier);
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            KVColumnFamilyQualifier other = (KVColumnFamilyQualifier) obj;
            if (!colFamilyName.equals(other.colFamilyName)) return false;
            if (!Arrays.equals(colQualifier, other.colQualifier)) return false;
            return true;
        }

    }

    @Override
    public Map<String, String> getPropertyValues() {
        return Collections.unmodifiableMap(propertyValues);
    }

    @Override
    public Map<String, String> getDefaultPropertyValues() {
        Map<String, String> map = new HashMap<>();
        map.put(DISABLE_WAL, String.valueOf(DEFAULT_DISABLE_WAL));
        map.put(IMMUTABLE_ROWS, String.valueOf(DEFAULT_IMMUTABLE_ROWS));
        map.put(TRANSACTION_PROVIDER, DEFAULT_TRANSACTION_PROVIDER);
        map.put(IMMUTABLE_STORAGE_SCHEME, DEFAULT_IMMUTABLE_STORAGE_SCHEME);
        map.put(COLUMN_ENCODED_BYTES, String.valueOf(DEFAULT_COLUMN_ENCODED_BYTES));
        map.put(UPDATE_CACHE_FREQUENCY, String.valueOf(DEFAULT_UPDATE_CACHE_FREQUENCY));
        map.put(USE_STATS_FOR_PARALLELIZATION, String.valueOf(DEFAULT_USE_STATS_FOR_PARALLELIZATION));
        map.put(TRANSACTIONAL, String.valueOf(DEFAULT_TRANSACTIONAL));
        map.put(MULTI_TENANT, String.valueOf(DEFAULT_MULTI_TENANT));
        map.put(SALT_BUCKETS, String.valueOf(DEFAULT_SALT_BUCKETS));
        map.put(DEFAULT_COLUMN_FAMILY_NAME, String.valueOf(DEFAULT_COLUMN_FAMILY));
        return Collections.unmodifiableMap(map);
    }

}
