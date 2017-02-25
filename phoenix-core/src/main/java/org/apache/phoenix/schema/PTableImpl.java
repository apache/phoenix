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

import static org.apache.phoenix.hbase.index.util.KeyValueBuilder.addQuietly;
import static org.apache.phoenix.hbase.index.util.KeyValueBuilder.deleteQuietly;
import static org.apache.phoenix.schema.SaltingUtil.SALTING_COLUMN;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nonnull;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.ExpressionCompiler;
import org.apache.phoenix.compile.StatementContext;
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
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.SizedUtil;
import org.apache.phoenix.util.TrustedByteArrayOutputStream;
import org.apache.tephra.TxConstants;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;


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
    // Have MultiMap for String->PColumn (may need family qualifier)
    private List<PColumn> pkColumns;
    private List<PColumn> allColumns;
    private List<PColumnFamily> families;
    private Map<byte[], PColumnFamily> familyByBytes;
    private Map<String, PColumnFamily> familyByString;
    private ListMultimap<String, PColumn> columnsByName;
    private Map<KVColumnFamilyQualifier, PColumn> kvColumnsByQualifiers;
    private PName pkName;
    private Integer bucketNum;
    private RowKeySchema rowKeySchema;
    // Indexes associated with this table.
    private List<PTable> indexes;
    // Data table name that the index is created on.
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
    private boolean isTransactional;
    private ViewType viewType;
    private Short viewIndexId;
    private int estimatedSize;
    private IndexType indexType;
    private int baseColumnCount;
    private boolean rowKeyOrderOptimizable; // TODO: remove when required that tables have been upgrade for PHOENIX-2067
    private boolean hasColumnsRequiringUpgrade; // TODO: remove when required that tables have been upgrade for PHOENIX-2067
    private int rowTimestampColPos;
    private long updateCacheFrequency;
    private boolean isNamespaceMapped;
    private String autoPartitionSeqName;
    private boolean isAppendOnlySchema;
    private ImmutableStorageScheme immutableStorageScheme;
    private QualifierEncodingScheme qualifierEncodingScheme;
    private EncodedCQCounter encodedCQCounter;

    public PTableImpl() {
        this.indexes = Collections.emptyList();
        this.physicalNames = Collections.emptyList();
        this.rowKeySchema = RowKeySchema.EMPTY_SCHEMA;
    }
    
    // Constructor used at table creation time
    public PTableImpl(PName tenantId, String schemaName, String tableName, long timestamp, List<PColumnFamily> families, boolean isNamespaceMapped) {
        Preconditions.checkArgument(tenantId==null || tenantId.getBytes().length > 0); // tenantId should be null or not empty
        this.tenantId = tenantId;
        this.name = PNameFactory.newName(SchemaUtil.getTableName(schemaName, tableName));
        this.key = new PTableKey(tenantId, name.getString());
        this.schemaName = PNameFactory.newName(schemaName);
        this.tableName = PNameFactory.newName(tableName);
        this.type = PTableType.VIEW;
        this.viewType = ViewType.MAPPED;
        this.timeStamp = timestamp;
        this.pkColumns = this.allColumns = Collections.emptyList();
        this.rowKeySchema = RowKeySchema.EMPTY_SCHEMA;
        this.indexes = Collections.emptyList();
        this.familyByBytes = Maps.newHashMapWithExpectedSize(families.size());
        this.familyByString = Maps.newHashMapWithExpectedSize(families.size());
        for (PColumnFamily family : families) {
            familyByBytes.put(family.getName().getBytes(), family);
            familyByString.put(family.getName().getString(), family);
        }
        this.families = families;
        this.physicalNames = Collections.emptyList();
        this.isNamespaceMapped = isNamespaceMapped;
    }
    
    public PTableImpl(PName tenantId, String schemaName, String tableName, long timestamp, List<PColumnFamily> families, boolean isNamespaceMapped, ImmutableStorageScheme storageScheme, QualifierEncodingScheme encodingScheme) { // For base table of mapped VIEW
        Preconditions.checkArgument(tenantId==null || tenantId.getBytes().length > 0); // tenantId should be null or not empty
        this.tenantId = tenantId;
        this.name = PNameFactory.newName(SchemaUtil.getTableName(schemaName, tableName));
        this.key = new PTableKey(tenantId, name.getString());
        this.schemaName = PNameFactory.newName(schemaName);
        this.tableName = PNameFactory.newName(tableName);
        this.type = PTableType.VIEW;
        this.viewType = ViewType.MAPPED;
        this.timeStamp = timestamp;
        this.pkColumns = this.allColumns = Collections.emptyList();
        this.rowKeySchema = RowKeySchema.EMPTY_SCHEMA;
        this.indexes = Collections.emptyList();
        this.familyByBytes = Maps.newHashMapWithExpectedSize(families.size());
        this.familyByString = Maps.newHashMapWithExpectedSize(families.size());
        for (PColumnFamily family : families) {
            familyByBytes.put(family.getName().getBytes(), family);
            familyByString.put(family.getName().getString(), family);
        }
        this.families = families;
        this.physicalNames = Collections.emptyList();
        this.isNamespaceMapped = isNamespaceMapped;
        this.immutableStorageScheme = storageScheme;
        this.qualifierEncodingScheme = encodingScheme;
    }
    
    // For indexes stored in shared physical tables
    public PTableImpl(PName tenantId, PName schemaName, PName tableName, long timestamp, List<PColumnFamily> families, 
            List<PColumn> columns, List<PName> physicalNames, Short viewIndexId, boolean multiTenant, boolean isNamespaceMpped, ImmutableStorageScheme storageScheme, QualifierEncodingScheme qualifierEncodingScheme, 
            EncodedCQCounter encodedCQCounter) throws SQLException {
        this.pkColumns = this.allColumns = Collections.emptyList();
        this.rowKeySchema = RowKeySchema.EMPTY_SCHEMA;
        this.indexes = Collections.emptyList();
        this.familyByBytes = Maps.newHashMapWithExpectedSize(families.size());
        this.familyByString = Maps.newHashMapWithExpectedSize(families.size());
        for (PColumnFamily family : families) {
            familyByBytes.put(family.getName().getBytes(), family);
            familyByString.put(family.getName().getString(), family);
        }
        this.families = families;
        init(tenantId, this.schemaName, this.tableName, PTableType.INDEX, state, timeStamp, sequenceNumber, pkName, bucketNum, columns,
            this.schemaName, parentTableName, indexes, isImmutableRows, physicalNames, defaultFamilyName,
            null, disableWAL, multiTenant, storeNulls, viewType, viewIndexId, indexType, baseColumnCount, rowKeyOrderOptimizable,
            isTransactional, updateCacheFrequency, indexDisableTimestamp, isNamespaceMpped, null, false, storageScheme, qualifierEncodingScheme, encodedCQCounter);
    }

    public PTableImpl(long timeStamp) { // For delete marker
        this(timeStamp, false);
    }

    public PTableImpl(long timeStamp, boolean isIndex) { // For index delete marker
        if (isIndex) {
            this.type = PTableType.INDEX;
            this.state = PIndexState.INACTIVE;
        } else {
            this.type = PTableType.TABLE;
        }
        this.timeStamp = timeStamp;
        this.pkColumns = this.allColumns = Collections.emptyList();
        this.families = Collections.emptyList();
        this.familyByBytes = Collections.emptyMap();
        this.familyByString = Collections.emptyMap();
        this.rowKeySchema = RowKeySchema.EMPTY_SCHEMA;
        this.indexes = Collections.emptyList();
        this.physicalNames = Collections.emptyList();;
    }

    // When cloning table, ignore the salt column as it will be added back in the constructor
    public static List<PColumn> getColumnsToClone(PTable table) {
        return table.getBucketNum() == null ? table.getColumns() : table.getColumns().subList(1, table.getColumns().size());
    }

    public static PTableImpl makePTable(PTable table, long timeStamp, List<PTable> indexes) throws SQLException {
        return makePTable(table, timeStamp, indexes, table.getParentSchemaName(), table.getViewStatement());
    }

    public static PTable makePTable(PTable index, PName indexName, String viewStatement, long updateCacheFrequency, PName tenantId) throws SQLException {
        return Objects.equal(viewStatement, index.getViewStatement()) ? index : makePTable(index, indexName, index.getTimeStamp(), Lists.newArrayList(index.getPhysicalName()), index.getIndexes(), viewStatement, updateCacheFrequency, tenantId);
    }
    
    public static PTableImpl makePTable(PTable table, PName tableName, long timeStamp, List<PName> physicalNames, List<PTable> indexes, String viewStatement, long updateCacheFrequency, PName tenantId) throws SQLException {
            return new PTableImpl(
                    tenantId, table.getSchemaName(), tableName, table.getType(), table.getIndexState(), timeStamp,
                    table.getSequenceNumber(), table.getPKName(), table.getBucketNum(), getColumnsToClone(table), table.getParentSchemaName(), table.getParentTableName(),
                    indexes, table.isImmutableRows(), physicalNames, table.getDefaultFamilyName(), viewStatement,
                    table.isWALDisabled(), table.isMultiTenant(), table.getStoreNulls(), table.getViewType(), table.getViewIndexId(), table.getIndexType(),
                    table.getBaseColumnCount(), table.rowKeyOrderOptimizable(), table.isTransactional(), updateCacheFrequency,
                    table.getIndexDisableTimestamp(), table.isNamespaceMapped(), table.getAutoPartitionSeqName(), table.isAppendOnlySchema(), table.getImmutableStorageScheme(), table.getEncodingScheme(), table.getEncodedCQCounter());
        }

    public static PTableImpl makePTable(PTable table, long timeStamp, List<PTable> indexes, PName parentSchemaName, String viewStatement) throws SQLException {
        return new PTableImpl(
                table.getTenantId(), table.getSchemaName(), table.getTableName(), table.getType(), table.getIndexState(), timeStamp,
                table.getSequenceNumber(), table.getPKName(), table.getBucketNum(), getColumnsToClone(table), parentSchemaName, table.getParentTableName(),
                indexes, table.isImmutableRows(), table.getPhysicalNames(), table.getDefaultFamilyName(), viewStatement,
                table.isWALDisabled(), table.isMultiTenant(), table.getStoreNulls(), table.getViewType(), table.getViewIndexId(), table.getIndexType(),
                table.getBaseColumnCount(), table.rowKeyOrderOptimizable(), table.isTransactional(), table.getUpdateCacheFrequency(),
                table.getIndexDisableTimestamp(), table.isNamespaceMapped(), table.getAutoPartitionSeqName(), table.isAppendOnlySchema(), table.getImmutableStorageScheme(), table.getEncodingScheme(), table.getEncodedCQCounter());
    }

    public static PTableImpl makePTable(PTable table, Collection<PColumn> columns) throws SQLException {
        return new PTableImpl(
                table.getTenantId(), table.getSchemaName(), table.getTableName(), table.getType(), table.getIndexState(), table.getTimeStamp(),
                table.getSequenceNumber(), table.getPKName(), table.getBucketNum(), columns, table.getParentSchemaName(), table.getParentTableName(),
                table.getIndexes(), table.isImmutableRows(), table.getPhysicalNames(), table.getDefaultFamilyName(), table.getViewStatement(),
                table.isWALDisabled(), table.isMultiTenant(), table.getStoreNulls(), table.getViewType(), table.getViewIndexId(), table.getIndexType(),
                table.getBaseColumnCount(), table.rowKeyOrderOptimizable(), table.isTransactional(), table.getUpdateCacheFrequency(),
                table.getIndexDisableTimestamp(), table.isNamespaceMapped(), table.getAutoPartitionSeqName(), table.isAppendOnlySchema(), table.getImmutableStorageScheme(), table.getEncodingScheme(), table.getEncodedCQCounter());
    }
    
    public static PTableImpl makePTable(PTable table, Collection<PColumn> columns, PName defaultFamily) throws SQLException {
        return new PTableImpl(
                table.getTenantId(), table.getSchemaName(), table.getTableName(), table.getType(), table.getIndexState(), table.getTimeStamp(),
                table.getSequenceNumber(), table.getPKName(), table.getBucketNum(), columns, table.getParentSchemaName(), table.getParentTableName(),
                table.getIndexes(), table.isImmutableRows(), table.getPhysicalNames(), defaultFamily, table.getViewStatement(),
                table.isWALDisabled(), table.isMultiTenant(), table.getStoreNulls(), table.getViewType(), table.getViewIndexId(), table.getIndexType(),
                table.getBaseColumnCount(), table.rowKeyOrderOptimizable(), table.isTransactional(), table.getUpdateCacheFrequency(),
                table.getIndexDisableTimestamp(), table.isNamespaceMapped(), table.getAutoPartitionSeqName(), table.isAppendOnlySchema(), table.getImmutableStorageScheme(), table.getEncodingScheme(), table.getEncodedCQCounter());
    }

    public static PTableImpl makePTable(PTable table, long timeStamp, long sequenceNumber, Collection<PColumn> columns) throws SQLException {
        return new PTableImpl(
                table.getTenantId(), table.getSchemaName(), table.getTableName(), table.getType(), table.getIndexState(), timeStamp,
                sequenceNumber, table.getPKName(), table.getBucketNum(), columns, table.getParentSchemaName(), table.getParentTableName(), table.getIndexes(),
                table.isImmutableRows(), table.getPhysicalNames(), table.getDefaultFamilyName(), table.getViewStatement(), table.isWALDisabled(),
                table.isMultiTenant(), table.getStoreNulls(), table.getViewType(), table.getViewIndexId(), table.getIndexType(),
                table.getBaseColumnCount(), table.rowKeyOrderOptimizable(), table.isTransactional(), table.getUpdateCacheFrequency(), table.getIndexDisableTimestamp(), 
                table.isNamespaceMapped(), table.getAutoPartitionSeqName(), table.isAppendOnlySchema(), table.getImmutableStorageScheme(), table.getEncodingScheme(), table.getEncodedCQCounter());
    }

    public static PTableImpl makePTable(PTable table, long timeStamp, long sequenceNumber, Collection<PColumn> columns, boolean isImmutableRows) throws SQLException {
        return new PTableImpl(
                table.getTenantId(), table.getSchemaName(), table.getTableName(), table.getType(), table.getIndexState(), timeStamp,
                sequenceNumber, table.getPKName(), table.getBucketNum(), columns, table.getParentSchemaName(), table.getParentTableName(),
                table.getIndexes(), isImmutableRows, table.getPhysicalNames(), table.getDefaultFamilyName(), table.getViewStatement(),
                table.isWALDisabled(), table.isMultiTenant(), table.getStoreNulls(), table.getViewType(), table.getViewIndexId(),
                table.getIndexType(), table.getBaseColumnCount(), table.rowKeyOrderOptimizable(), table.isTransactional(),
                table.getUpdateCacheFrequency(), table.getIndexDisableTimestamp(), table.isNamespaceMapped(), table.getAutoPartitionSeqName(), table.isAppendOnlySchema(), table.getImmutableStorageScheme(), table.getEncodingScheme(), table.getEncodedCQCounter());
    }
    
    public static PTableImpl makePTable(PTable table, long timeStamp, long sequenceNumber, Collection<PColumn> columns, boolean isImmutableRows, boolean isWalDisabled,
            boolean isMultitenant, boolean storeNulls, boolean isTransactional, long updateCacheFrequency, boolean isNamespaceMapped) throws SQLException {
        return new PTableImpl(
                table.getTenantId(), table.getSchemaName(), table.getTableName(), table.getType(), table.getIndexState(), timeStamp,
                sequenceNumber, table.getPKName(), table.getBucketNum(), columns, table.getParentSchemaName(), table.getParentTableName(),
                table.getIndexes(), isImmutableRows, table.getPhysicalNames(), table.getDefaultFamilyName(), table.getViewStatement(),
                isWalDisabled, isMultitenant, storeNulls, table.getViewType(), table.getViewIndexId(), table.getIndexType(),
                table.getBaseColumnCount(), table.rowKeyOrderOptimizable(), isTransactional, updateCacheFrequency, table.getIndexDisableTimestamp(), 
                isNamespaceMapped, table.getAutoPartitionSeqName(), table.isAppendOnlySchema(), table.getImmutableStorageScheme(), table.getEncodingScheme(), table.getEncodedCQCounter());
    }
    
    public static PTableImpl makePTable(PTable table, PIndexState state) throws SQLException {
        return new PTableImpl(
                table.getTenantId(), table.getSchemaName(), table.getTableName(), table.getType(), state, table.getTimeStamp(),
                table.getSequenceNumber(), table.getPKName(), table.getBucketNum(), getColumnsToClone(table),
                table.getParentSchemaName(), table.getParentTableName(), table.getIndexes(),
                table.isImmutableRows(), table.getPhysicalNames(), table.getDefaultFamilyName(), table.getViewStatement(),
                table.isWALDisabled(), table.isMultiTenant(), table.getStoreNulls(), table.getViewType(), table.getViewIndexId(), table.getIndexType(),
                table.getBaseColumnCount(), table.rowKeyOrderOptimizable(), table.isTransactional(), table.getUpdateCacheFrequency(),
                table.getIndexDisableTimestamp(), table.isNamespaceMapped(), table.getAutoPartitionSeqName(), table.isAppendOnlySchema(), table.getImmutableStorageScheme(), table.getEncodingScheme(), table.getEncodedCQCounter());
    }

    public static PTableImpl makePTable(PTable table, boolean rowKeyOrderOptimizable) throws SQLException {
        return new PTableImpl(
                table.getTenantId(), table.getSchemaName(), table.getTableName(), table.getType(), table.getIndexState(), table.getTimeStamp(),
                table.getSequenceNumber(), table.getPKName(), table.getBucketNum(), getColumnsToClone(table),
                table.getParentSchemaName(), table.getParentTableName(), table.getIndexes(),
                table.isImmutableRows(), table.getPhysicalNames(), table.getDefaultFamilyName(), table.getViewStatement(),
                table.isWALDisabled(), table.isMultiTenant(), table.getStoreNulls(), table.getViewType(), table.getViewIndexId(), table.getIndexType(),
                table.getBaseColumnCount(), rowKeyOrderOptimizable, table.isTransactional(), table.getUpdateCacheFrequency(), table.getIndexDisableTimestamp(), table.isNamespaceMapped(), 
                table.getAutoPartitionSeqName(), table.isAppendOnlySchema(), table.getImmutableStorageScheme(), table.getEncodingScheme(), table.getEncodedCQCounter());
    }

    public static PTableImpl makePTable(PTable table) throws SQLException {
        return new PTableImpl(
                table.getTenantId(), table.getSchemaName(), table.getTableName(), table.getType(), table.getIndexState(), table.getTimeStamp(),
                table.getSequenceNumber(), table.getPKName(), table.getBucketNum(), getColumnsToClone(table),
                table.getParentSchemaName(), table.getParentTableName(), table.getIndexes(),
                table.isImmutableRows(), table.getPhysicalNames(), table.getDefaultFamilyName(), table.getViewStatement(),
                table.isWALDisabled(), table.isMultiTenant(), table.getStoreNulls(), table.getViewType(), table.getViewIndexId(), table.getIndexType(),
                table.getBaseColumnCount(), table.rowKeyOrderOptimizable(), table.isTransactional(), table.getUpdateCacheFrequency(), table.getIndexDisableTimestamp(), 
                table.isNamespaceMapped(), table.getAutoPartitionSeqName(), table.isAppendOnlySchema(), table.getImmutableStorageScheme(), table.getEncodingScheme(), table.getEncodedCQCounter());
    }

    public static PTableImpl makePTable(PName tenantId, PName schemaName, PName tableName, PTableType type,
            PIndexState state, long timeStamp, long sequenceNumber, PName pkName, Integer bucketNum,
            Collection<PColumn> columns, PName dataSchemaName, PName dataTableName, List<PTable> indexes,
            boolean isImmutableRows, List<PName> physicalNames, PName defaultFamilyName, String viewExpression,
            boolean disableWAL, boolean multiTenant, boolean storeNulls, ViewType viewType, Short viewIndexId,
            IndexType indexType, boolean rowKeyOrderOptimizable, boolean isTransactional, long updateCacheFrequency,
            long indexDisableTimestamp, boolean isNamespaceMapped, String autoPartitionSeqName, boolean isAppendOnlySchema, ImmutableStorageScheme storageScheme, QualifierEncodingScheme qualifierEncodingScheme, EncodedCQCounter encodedCQCounter) throws SQLException {
        return new PTableImpl(tenantId, schemaName, tableName, type, state, timeStamp, sequenceNumber, pkName, bucketNum, columns, dataSchemaName,
                dataTableName, indexes, isImmutableRows, physicalNames, defaultFamilyName,
                viewExpression, disableWAL, multiTenant, storeNulls, viewType, viewIndexId,
                indexType, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, rowKeyOrderOptimizable, isTransactional,
                updateCacheFrequency,indexDisableTimestamp, isNamespaceMapped, autoPartitionSeqName, isAppendOnlySchema, storageScheme, qualifierEncodingScheme, encodedCQCounter);
    }

    public static PTableImpl makePTable(PName tenantId, PName schemaName, PName tableName, PTableType type,
            PIndexState state, long timeStamp, long sequenceNumber, PName pkName, Integer bucketNum,
            Collection<PColumn> columns, PName dataSchemaName, PName dataTableName, List<PTable> indexes,
            boolean isImmutableRows, List<PName> physicalNames, PName defaultFamilyName, String viewExpression,
            boolean disableWAL, boolean multiTenant, boolean storeNulls, ViewType viewType, Short viewIndexId,
            IndexType indexType, boolean rowKeyOrderOptimizable, boolean isTransactional, long updateCacheFrequency,
            int baseColumnCount, long indexDisableTimestamp, boolean isNamespaceMapped,
            String autoPartitionSeqName, boolean isAppendOnlySchema, ImmutableStorageScheme storageScheme, QualifierEncodingScheme qualifierEncodingScheme, EncodedCQCounter encodedCQCounter)
            throws SQLException {
        return new PTableImpl(tenantId, schemaName, tableName, type, state, timeStamp, sequenceNumber, pkName,
                bucketNum, columns, dataSchemaName, dataTableName, indexes, isImmutableRows, physicalNames,
                defaultFamilyName, viewExpression, disableWAL, multiTenant, storeNulls, viewType, viewIndexId,
                indexType, baseColumnCount, rowKeyOrderOptimizable, isTransactional, updateCacheFrequency, 
                indexDisableTimestamp, isNamespaceMapped, autoPartitionSeqName, isAppendOnlySchema, storageScheme, qualifierEncodingScheme, encodedCQCounter);
    }

    private PTableImpl(PName tenantId, PName schemaName, PName tableName, PTableType type, PIndexState state,
            long timeStamp, long sequenceNumber, PName pkName, Integer bucketNum, Collection<PColumn> columns,
            PName parentSchemaName, PName parentTableName, List<PTable> indexes, boolean isImmutableRows,
            List<PName> physicalNames, PName defaultFamilyName, String viewExpression, boolean disableWAL, boolean multiTenant,
            boolean storeNulls, ViewType viewType, Short viewIndexId, IndexType indexType,
            int baseColumnCount, boolean rowKeyOrderOptimizable, boolean isTransactional, long updateCacheFrequency,
            long indexDisableTimestamp, boolean isNamespaceMapped, String autoPartitionSeqName, boolean isAppendOnlySchema, ImmutableStorageScheme storageScheme, 
            QualifierEncodingScheme qualifierEncodingScheme, EncodedCQCounter encodedCQCounter) throws SQLException {
        init(tenantId, schemaName, tableName, type, state, timeStamp, sequenceNumber, pkName, bucketNum, columns,
                parentSchemaName, parentTableName, indexes, isImmutableRows, physicalNames, defaultFamilyName,
                viewExpression, disableWAL, multiTenant, storeNulls, viewType, viewIndexId, indexType, baseColumnCount, rowKeyOrderOptimizable,
                isTransactional, updateCacheFrequency, indexDisableTimestamp, isNamespaceMapped, autoPartitionSeqName, isAppendOnlySchema, storageScheme, 
                qualifierEncodingScheme, encodedCQCounter);
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

    private void init(PName tenantId, PName schemaName, PName tableName, PTableType type, PIndexState state, long timeStamp, long sequenceNumber,
            PName pkName, Integer bucketNum, Collection<PColumn> columns, PName parentSchemaName, PName parentTableName,
            List<PTable> indexes, boolean isImmutableRows, List<PName> physicalNames, PName defaultFamilyName, String viewExpression, boolean disableWAL,
            boolean multiTenant, boolean storeNulls, ViewType viewType, Short viewIndexId,
            IndexType indexType , int baseColumnCount, boolean rowKeyOrderOptimizable, boolean isTransactional, long updateCacheFrequency, long indexDisableTimestamp, 
            boolean isNamespaceMapped, String autoPartitionSeqName, boolean isAppendOnlySchema, ImmutableStorageScheme storageScheme, QualifierEncodingScheme qualifierEncodingScheme, 
            EncodedCQCounter encodedCQCounter) throws SQLException {
        Preconditions.checkNotNull(schemaName);
        Preconditions.checkArgument(tenantId==null || tenantId.getBytes().length > 0); // tenantId should be null or not empty
        int estimatedSize = SizedUtil.OBJECT_SIZE * 2 + 23 * SizedUtil.POINTER_SIZE + 4 * SizedUtil.INT_SIZE + 2 * SizedUtil.LONG_SIZE + 2 * SizedUtil.INT_OBJECT_SIZE +
              PNameFactory.getEstimatedSize(tenantId) +
              PNameFactory.getEstimatedSize(schemaName) +
              PNameFactory.getEstimatedSize(tableName) +
              PNameFactory.getEstimatedSize(pkName) +
              PNameFactory.getEstimatedSize(parentTableName) +
              PNameFactory.getEstimatedSize(defaultFamilyName);
        this.tenantId = tenantId;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.name = PNameFactory.newName(SchemaUtil.getTableName(schemaName.getString(), tableName.getString()));
        this.key = new PTableKey(tenantId, name.getString());
        this.type = type;
        this.state = state;
        this.timeStamp = timeStamp;
        this.indexDisableTimestamp = indexDisableTimestamp;
        this.sequenceNumber = sequenceNumber;
        this.pkName = pkName;
        this.isImmutableRows = isImmutableRows;
        this.defaultFamilyName = defaultFamilyName;
        this.viewStatement = viewExpression;
        this.disableWAL = disableWAL;
        this.multiTenant = multiTenant;
        this.storeNulls = storeNulls;
        this.viewType = viewType;
        this.viewIndexId = viewIndexId;
        this.indexType = indexType;
        this.isTransactional = isTransactional;
        this.rowKeyOrderOptimizable = rowKeyOrderOptimizable;
        this.updateCacheFrequency = updateCacheFrequency;
        this.isNamespaceMapped = isNamespaceMapped;
        this.autoPartitionSeqName = autoPartitionSeqName;
        this.isAppendOnlySchema = isAppendOnlySchema;
        this.immutableStorageScheme = storageScheme;
        this.qualifierEncodingScheme = qualifierEncodingScheme;
        List<PColumn> pkColumns;
        PColumn[] allColumns;
        
        this.columnsByName = ArrayListMultimap.create(columns.size(), 1);
        this.kvColumnsByQualifiers = Maps.newHashMapWithExpectedSize(columns.size());
        int numPKColumns = 0;
        if (bucketNum != null) {
            // Add salt column to allColumns and pkColumns, but don't add to
            // columnsByName, since it should not be addressable via name.
            allColumns = new PColumn[columns.size()+1];
            allColumns[SALTING_COLUMN.getPosition()] = SALTING_COLUMN;
            pkColumns = Lists.newArrayListWithExpectedSize(columns.size()+1);
            ++numPKColumns;
        } else {
            allColumns = new PColumn[columns.size()];
            pkColumns = Lists.newArrayListWithExpectedSize(columns.size());
        }
        for (PColumn column : columns) {
            allColumns[column.getPosition()] = column;
            PName familyName = column.getFamilyName();
            if (familyName == null) {
                ++numPKColumns;
            }
            String columnName = column.getName().getString();
            if (columnsByName.put(columnName, column)) {
                int count = 0;
                for (PColumn dupColumn : columnsByName.get(columnName)) {
                    if (Objects.equal(familyName, dupColumn.getFamilyName())) {
                        count++;
                        if (count > 1) {
                            throw new ColumnAlreadyExistsException(schemaName.getString(), name.getString(), columnName);
                        }
                    }
                }
            }
            byte[] cq = column.getColumnQualifierBytes();
            String cf = column.getFamilyName() != null ? column.getFamilyName().getString() : null;
            if (cf != null && cq != null) {
                KVColumnFamilyQualifier info = new KVColumnFamilyQualifier(cf, cq);
                if (kvColumnsByQualifiers.get(info) != null) {
                    throw new ColumnAlreadyExistsException(schemaName.getString(),
                            name.getString(), columnName);
                }
                kvColumnsByQualifiers.put(info, column);
            }
        }
        estimatedSize += SizedUtil.sizeOfMap(allColumns.length, SizedUtil.POINTER_SIZE, SizedUtil.sizeOfArrayList(1)); // for multi-map

        this.bucketNum = bucketNum;
        this.allColumns = ImmutableList.copyOf(allColumns);
        estimatedSize += SizedUtil.sizeOfMap(numPKColumns) + SizedUtil.sizeOfMap(allColumns.length);

        RowKeySchemaBuilder builder = new RowKeySchemaBuilder(numPKColumns);
        // Two pass so that column order in column families matches overall column order
        // and to ensure that column family order is constant
        int maxExpectedSize = allColumns.length - numPKColumns;
        // Maintain iteration order so that column families are ordered as they are listed
        Map<PName, List<PColumn>> familyMap = Maps.newLinkedHashMap();
        PColumn rowTimestampCol = null;
        for (PColumn column : allColumns) {
            PName familyName = column.getFamilyName();
            if (familyName == null) {
                hasColumnsRequiringUpgrade |= 
                        ( column.getSortOrder() == SortOrder.DESC 
                            && (!column.getDataType().isFixedWidth() 
                                || column.getDataType() == PChar.INSTANCE 
                                || column.getDataType() == PFloat.INSTANCE 
                                || column.getDataType() == PDouble.INSTANCE 
                                || column.getDataType() == PBinary.INSTANCE) )
                        || (column.getSortOrder() == SortOrder.ASC && column.getDataType() == PBinary.INSTANCE && column.getMaxLength() != null && column.getMaxLength() > 1);
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
        this.pkColumns = ImmutableList.copyOf(pkColumns);
        if (rowTimestampCol != null) {
            this.rowTimestampColPos = this.pkColumns.indexOf(rowTimestampCol);
        } else {
            this.rowTimestampColPos = -1;
        }
        
        builder.rowKeyOrderOptimizable(this.rowKeyOrderOptimizable()); // after hasDescVarLengthColumns is calculated
        this.rowKeySchema = builder.build();
        estimatedSize += rowKeySchema.getEstimatedSize();
        Iterator<Map.Entry<PName,List<PColumn>>> iterator = familyMap.entrySet().iterator();
        PColumnFamily[] families = new PColumnFamily[familyMap.size()];
        ImmutableMap.Builder<String, PColumnFamily> familyByString = ImmutableMap.builder();
        ImmutableSortedMap.Builder<byte[], PColumnFamily> familyByBytes = ImmutableSortedMap
                .orderedBy(Bytes.BYTES_COMPARATOR);
        for (int i = 0; i < families.length; i++) {
            Map.Entry<PName,List<PColumn>> entry = iterator.next();
            PColumnFamily family = new PColumnFamilyImpl(entry.getKey(), entry.getValue());//, qualifierEncodingScheme);
            families[i] = family;
            familyByString.put(family.getName().getString(), family);
            familyByBytes.put(family.getName().getBytes(), family);
            estimatedSize += family.getEstimatedSize();
        }
        this.families = ImmutableList.copyOf(families);
        this.familyByBytes = familyByBytes.build();
        this.familyByString = familyByString.build();
        estimatedSize += SizedUtil.sizeOfArrayList(families.length);
        estimatedSize += SizedUtil.sizeOfMap(families.length) * 2;
        this.indexes = indexes == null ? Collections.<PTable>emptyList() : indexes;
        for (PTable index : this.indexes) {
            estimatedSize += index.getEstimatedSize();
        }

        this.parentSchemaName = parentSchemaName;
        this.parentTableName = parentTableName;
        this.parentName = parentTableName == null ? null : PNameFactory.newName(SchemaUtil.getTableName(
            parentSchemaName!=null ? parentSchemaName.getString() : null, parentTableName.getString()));
        estimatedSize += PNameFactory.getEstimatedSize(this.parentName);

        this.physicalNames = physicalNames == null ? ImmutableList.<PName>of() : ImmutableList.copyOf(physicalNames);
        for (PName name : this.physicalNames) {
            estimatedSize += name.getEstimatedSize();
        }
        this.estimatedSize = estimatedSize;
        this.baseColumnCount = baseColumnCount;
        this.encodedCQCounter = encodedCQCounter;
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
                Integer	maxLength = column.getMaxLength();
                Integer scale = column.getScale();
                key.set(byteValue);
                if (!type.isSizeCompatible(key, null, type, sortOrder, null, null, maxLength, scale)) {
                    throw new DataExceedsCapacityException(name.getString() + "." + column.getName().getString() + " may not exceed " + maxLength + " (" + SchemaUtil.toString(type, byteValue) + ")");
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
            throw new ColumnNotFoundException(name);
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
                throw new ColumnNotFoundException("No column found for column qualifier " + qualifierEncodingScheme.decode(cq));
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

        public PRowImpl(KeyValueBuilder kvBuilder, ImmutableBytesWritable key, long ts, Integer bucketNum, boolean hasOnDupKey) {
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
                if (immutableStorageScheme != ImmutableStorageScheme.ONE_CELL_PER_COLUMN) {
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
                        addQuietly(put, kvBuilder, kvBuilder.buildPut(keyPtr,
                            colFamilyPtr, QueryConstants.SINGLE_KEYVALUE_COLUMN_QUALIFIER_BYTES_PTR, ts, ptr));
                    }
                    setValues = put;
                }
                // Because we cannot enforce a not null constraint on a KV column (since we don't know if the row exists when
                // we upsert it), so instead add a KV that is always empty. This allows us to imitate SQL semantics given the
                // way HBase works.
                Pair<byte[], byte[]> emptyKvInfo = EncodedColumnsUtil.getEmptyKeyValueInfo(PTableImpl.this);
                addQuietly(setValues, kvBuilder, kvBuilder.buildPut(keyPtr,
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
                Integer	maxLength = column.getMaxLength();
                Integer scale = column.getScale();
                SortOrder sortOrder = column.getSortOrder();
                if (!type.isSizeCompatible(ptr, null, type, sortOrder, null, null, maxLength, scale)) {
                    throw new DataExceedsCapacityException(name.getString() + "." + column.getName().getString() + 
                            " may not exceed " + maxLength + " (" + SchemaUtil.toString(type, byteValue) + ")");
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
                    addQuietly(setValues, kvBuilder, kvBuilder.buildPut(keyPtr,
                        column.getFamilyName().getBytesPtr(), qualifierPtr,
                        ts, ptr));
                }
            }
        }

        @SuppressWarnings("deprecation")
        @Override
        public void delete() {
            newMutations();
            // we're using the Tephra column family delete marker here to prevent the translation 
            // of deletes to puts by the Tephra's TransactionProcessor
            if (PTableImpl.this.isTransactional()) {
                Put put = new Put(key);
                if (families.isEmpty()) {
                    put.add(SchemaUtil.getEmptyColumnFamily(PTableImpl.this), TxConstants.FAMILY_DELETE_QUALIFIER, ts,
                            HConstants.EMPTY_BYTE_ARRAY);
                } else {
                    for (PColumnFamily colFamily : families) {
                        put.add(colFamily.getName().getBytes(), TxConstants.FAMILY_DELETE_QUALIFIER, ts,
                                HConstants.EMPTY_BYTE_ARRAY);
                    }
                }
                deleteRow = put;                
            } else {
                Delete delete = new Delete(key);
                for (PColumnFamily colFamily : families) {
                	delete.deleteFamily(colFamily.getName().getBytes(), ts);
                }
                deleteRow = delete;
            }
            if (isWALDisabled()) {
                deleteRow.setDurability(Durability.SKIP_WAL);
            }
        }
        
    }

    @Override
    public PColumnFamily getColumnFamily(String familyName) throws ColumnFamilyNotFoundException {
        PColumnFamily family = familyByString.get(familyName);
        if (family == null) {
            throw new ColumnFamilyNotFoundException(familyName);
        }
        return family;
    }

    @Override
    public PColumnFamily getColumnFamily(byte[] familyBytes) throws ColumnFamilyNotFoundException {
        PColumnFamily family = familyByBytes.get(familyBytes);
        if (family == null) {
            String familyName = Bytes.toString(familyBytes);
            throw new ColumnFamilyNotFoundException(familyName);
        }
        return family;
    }

    @Override
    public List<PColumn> getColumns() {
        return allColumns;
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
            throw new ColumnNotFoundException(name);
        }
        if (size > 1) {
            do {
                PColumn column = columns.get(--size);
                if (column.getFamilyName() == null) {
                    return column;
                }
            } while (size > 0);
            throw new ColumnNotFoundException(name);
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
        return SchemaUtil.getPhysicalHBaseTableName(physicalNames.isEmpty() ? getName() : physicalNames.get(0),
                isNamespaceMapped, type);
    }

    @Override
    public List<PName> getPhysicalNames() {
        return physicalNames;
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
    public Short getViewIndexId() {
        return viewIndexId;
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
        Short viewIndexId = null;
        if(table.hasViewIndexId()){
            viewIndexId = (short)table.getViewIndexId();
        }
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
        boolean isTransactional = table.getTransactional();
        ViewType viewType = null;
        String viewStatement = null;
        List<PName> physicalNames = Collections.emptyList();
        if (tableType == PTableType.VIEW) {
            viewType = ViewType.fromSerializedValue(table.getViewType().toByteArray()[0]);
        }
        if(table.hasViewStatement()){
            viewStatement = (String) PVarchar.INSTANCE.toObject(table.getViewStatement().toByteArray());
        }
        if (tableType == PTableType.VIEW || viewIndexId != null) {
            physicalNames = Lists.newArrayListWithExpectedSize(table.getPhysicalNamesCount());
            for(int i = 0; i < table.getPhysicalNamesCount(); i++) {
                physicalNames.add(PNameFactory.newName(table.getPhysicalNames(i).toByteArray()));
            }
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
        String autoParititonSeqName = null;
        if (table.hasAutoParititonSeqName()) {
            autoParititonSeqName = table.getAutoParititonSeqName();
        }
        boolean isAppendOnlySchema = false;
        if (table.hasIsAppendOnlySchema()) {
            isAppendOnlySchema = table.getIsAppendOnlySchema();
        }
        ImmutableStorageScheme storageScheme = null;
        if (table.hasStorageScheme()) {
            storageScheme = ImmutableStorageScheme.fromSerializedValue(table.getStorageScheme().toByteArray()[0]);
        }
        QualifierEncodingScheme qualifierEncodingScheme = null;
        if (table.hasEncodingScheme()) {
            qualifierEncodingScheme = QualifierEncodingScheme.fromSerializedValue(table.getEncodingScheme().toByteArray()[0]);
        }
        EncodedCQCounter encodedColumnQualifierCounter = null;
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

        try {
            PTableImpl result = new PTableImpl();
            result.init(tenantId, schemaName, tableName, tableType, indexState, timeStamp, sequenceNumber, pkName,
                (bucketNum == NO_SALTING) ? null : bucketNum, columns, parentSchemaName, parentTableName, indexes,
                        isImmutableRows, physicalNames, defaultFamilyName, viewStatement, disableWAL,
                        multiTenant, storeNulls, viewType, viewIndexId, indexType, baseColumnCount, rowKeyOrderOptimizable,
                        isTransactional, updateCacheFrequency, indexDisableTimestamp, isNamespaceMapped, autoParititonSeqName, 
                        isAppendOnlySchema, storageScheme, qualifierEncodingScheme, encodedColumnQualifierCounter);
            return result;
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
      builder.setTransactional(table.isTransactional());
      if(table.getType() == PTableType.VIEW){
        builder.setViewType(ByteStringer.wrap(new byte[]{table.getViewType().getSerializedValue()}));
      }
      if(table.getViewStatement()!=null){
        builder.setViewStatement(ByteStringer.wrap(PVarchar.INSTANCE.toBytes(table.getViewStatement())));
      }
      if(table.getType() == PTableType.VIEW || table.getViewIndexId() != null){
        for (int i = 0; i < table.getPhysicalNames().size(); i++) {
          builder.addPhysicalNames(ByteStringer.wrap(table.getPhysicalNames().get(i).getBytes()));
        }
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
    public boolean isTransactional() {
        return isTransactional;
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
}
