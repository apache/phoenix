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
import static org.apache.phoenix.query.QueryConstants.SEPARATOR_BYTE;
import static org.apache.phoenix.schema.SaltingUtil.SALTING_COLUMN;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.generated.PGuidePostsProtos;
import org.apache.phoenix.coprocessor.generated.PGuidePostsProtos.PGuidePosts;
import org.apache.phoenix.coprocessor.generated.PTableProtos;
import org.apache.phoenix.exception.DataExceedsCapacityException;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.protobuf.ProtobufUtil;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.RowKeySchema.RowKeySchemaBuilder;
import org.apache.phoenix.schema.stats.GuidePostsInfo;
import org.apache.phoenix.schema.stats.PTableStats;
import org.apache.phoenix.schema.stats.PTableStatsImpl;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.SizedUtil;
import org.apache.phoenix.util.StringUtil;
import org.apache.phoenix.util.TrustedByteArrayOutputStream;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.HBaseZeroCopyByteString;
import com.sun.istack.NotNull;

/**
 *
 * Base class for PTable implementors.  Provides abstraction for
 * storing data in a single column (ColumnLayout.SINGLE) or in
 * multiple columns (ColumnLayout.MULTI).
 *
 *
 * @since 0.1
 */
public class PTableImpl implements PTable {
    private static final Integer NO_SALTING = -1;

    private PTableKey key;
    private PName name;
    private PName schemaName;
    private PName tableName;
    private PName tenantId;
    private PTableType type;
    private PIndexState state;
    private long sequenceNumber;
    private long timeStamp;
    // Have MultiMap for String->PColumn (may need family qualifier)
    private List<PColumn> pkColumns;
    private List<PColumn> allColumns;
    private List<PColumnFamily> families;
    private Map<byte[], PColumnFamily> familyByBytes;
    private Map<String, PColumnFamily> familyByString;
    private ListMultimap<String,PColumn> columnsByName;
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
    private ViewType viewType;
    private Short viewIndexId;
    private int estimatedSize;
    private IndexType indexType;
    private PTableStats tableStats = PTableStats.EMPTY_STATS;

    public PTableImpl() {
        this.indexes = Collections.emptyList();
        this.physicalNames = Collections.emptyList();
    }

    public PTableImpl(PName tenantId, String schemaName, String tableName, long timestamp, List<PColumnFamily> families) { // For base table of mapped VIEW
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
        return makePTable(table, timeStamp, indexes, table.getSchemaName(), table.getViewStatement());
    }

    public static PTable makePTable(PTable table, String viewStatement) throws SQLException {
        return Objects.equal(viewStatement, table.getViewStatement()) ? table : makePTable(table, table.getTimeStamp(), table.getIndexes(), table.getSchemaName(), viewStatement);
    }

    public static PTableImpl makePTable(PTable table, long timeStamp, List<PTable> indexes, PName parentSchemaName, String viewStatement) throws SQLException {
        return new PTableImpl(
                table.getTenantId(), table.getSchemaName(), table.getTableName(), table.getType(), table.getIndexState(), timeStamp,
                table.getSequenceNumber(), table.getPKName(), table.getBucketNum(), getColumnsToClone(table), parentSchemaName, table.getParentTableName(),
                indexes, table.isImmutableRows(), table.getPhysicalNames(), table.getDefaultFamilyName(), viewStatement,
                table.isWALDisabled(), table.isMultiTenant(), table.getStoreNulls(), table.getViewType(), table.getViewIndexId(), table.getIndexType(), table.getTableStats());
    }

    public static PTableImpl makePTable(PTable table, List<PColumn> columns) throws SQLException {
        return new PTableImpl(
                table.getTenantId(), table.getSchemaName(), table.getTableName(), table.getType(), table.getIndexState(), table.getTimeStamp(),
                table.getSequenceNumber(), table.getPKName(), table.getBucketNum(), columns, table.getParentSchemaName(), table.getParentTableName(),
                table.getIndexes(), table.isImmutableRows(), table.getPhysicalNames(), table.getDefaultFamilyName(), table.getViewStatement(),
                table.isWALDisabled(), table.isMultiTenant(), table.getStoreNulls(), table.getViewType(), table.getViewIndexId(), table.getIndexType(), table.getTableStats());
    }

    public static PTableImpl makePTable(PTable table, long timeStamp, long sequenceNumber, List<PColumn> columns) throws SQLException {
        return new PTableImpl(
                table.getTenantId(), table.getSchemaName(), table.getTableName(), table.getType(), table.getIndexState(), timeStamp,
                sequenceNumber, table.getPKName(), table.getBucketNum(), columns, table.getParentSchemaName(), table.getParentTableName(), table.getIndexes(),
                table.isImmutableRows(), table.getPhysicalNames(), table.getDefaultFamilyName(), table.getViewStatement(), table.isWALDisabled(),
                table.isMultiTenant(), table.getStoreNulls(), table.getViewType(), table.getViewIndexId(), table.getIndexType(), table.getTableStats());
    }

    public static PTableImpl makePTable(PTable table, long timeStamp, long sequenceNumber, List<PColumn> columns, boolean isImmutableRows) throws SQLException {
        return new PTableImpl(
                table.getTenantId(), table.getSchemaName(), table.getTableName(), table.getType(), table.getIndexState(), timeStamp,
                sequenceNumber, table.getPKName(), table.getBucketNum(), columns, table.getParentSchemaName(), table.getParentTableName(),
                table.getIndexes(), isImmutableRows, table.getPhysicalNames(), table.getDefaultFamilyName(), table.getViewStatement(),
                table.isWALDisabled(), table.isMultiTenant(), table.getStoreNulls(), table.getViewType(), table.getViewIndexId(), table.getIndexType(), table.getTableStats());
    }
    
    public static PTableImpl makePTable(PTable table, long timeStamp, long sequenceNumber, List<PColumn> columns, boolean isImmutableRows, boolean isWalDisabled, boolean isMultitenant, boolean storeNulls) throws SQLException {
        return new PTableImpl(
                table.getTenantId(), table.getSchemaName(), table.getTableName(), table.getType(), table.getIndexState(), timeStamp,
                sequenceNumber, table.getPKName(), table.getBucketNum(), columns, table.getParentSchemaName(), table.getParentTableName(),
                table.getIndexes(), isImmutableRows, table.getPhysicalNames(), table.getDefaultFamilyName(), table.getViewStatement(),
                isWalDisabled, isMultitenant, storeNulls, table.getViewType(), table.getViewIndexId(), table.getIndexType(), table.getTableStats());
    }
    
    public static PTableImpl makePTable(PTable table, PIndexState state) throws SQLException {
        return new PTableImpl(
                table.getTenantId(), table.getSchemaName(), table.getTableName(), table.getType(), state, table.getTimeStamp(),
                table.getSequenceNumber(), table.getPKName(), table.getBucketNum(), getColumnsToClone(table),
                table.getParentSchemaName(), table.getParentTableName(), table.getIndexes(),
                table.isImmutableRows(), table.getPhysicalNames(), table.getDefaultFamilyName(), table.getViewStatement(),
                table.isWALDisabled(), table.isMultiTenant(), table.getStoreNulls(), table.getViewType(), table.getViewIndexId(), table.getIndexType(), table.getTableStats());
    }

    public static PTableImpl makePTable(PTable table, PTableStats stats) throws SQLException {
        return new PTableImpl(
                table.getTenantId(), table.getSchemaName(), table.getTableName(), table.getType(), table.getIndexState(), table.getTimeStamp(),
                table.getSequenceNumber(), table.getPKName(), table.getBucketNum(), getColumnsToClone(table),
                table.getParentSchemaName(), table.getParentTableName(), table.getIndexes(),
                table.isImmutableRows(), table.getPhysicalNames(), table.getDefaultFamilyName(), table.getViewStatement(),
                table.isWALDisabled(), table.isMultiTenant(), table.getStoreNulls(), table.getViewType(), table.getViewIndexId(), table.getIndexType(), stats);
    }

    public static PTableImpl makePTable(PName tenantId, PName schemaName, PName tableName, PTableType type, PIndexState state, long timeStamp, long sequenceNumber,
            PName pkName, Integer bucketNum, List<PColumn> columns, PName dataSchemaName, PName dataTableName, List<PTable> indexes,
            boolean isImmutableRows, List<PName> physicalNames, PName defaultFamilyName, String viewExpression, boolean disableWAL, boolean multiTenant,
            boolean storeNulls, ViewType viewType, Short viewIndexId, IndexType indexType) throws SQLException {
        return new PTableImpl(tenantId, schemaName, tableName, type, state, timeStamp, sequenceNumber, pkName, bucketNum, columns, dataSchemaName,
                dataTableName, indexes, isImmutableRows, physicalNames, defaultFamilyName,
                viewExpression, disableWAL, multiTenant, storeNulls, viewType, viewIndexId,
                indexType, PTableStats.EMPTY_STATS);
    }

    public static PTableImpl makePTable(PName tenantId, PName schemaName, PName tableName, PTableType type,
            PIndexState state, long timeStamp, long sequenceNumber, PName pkName, Integer bucketNum,
            List<PColumn> columns, PName dataSchemaName, PName dataTableName, List<PTable> indexes,
            boolean isImmutableRows, List<PName> physicalNames, PName defaultFamilyName, String viewExpression,
            boolean disableWAL, boolean multiTenant, boolean storeNulls, ViewType viewType, Short viewIndexId, IndexType indexType, @NotNull PTableStats stats)
            throws SQLException {
        return new PTableImpl(tenantId, schemaName, tableName, type, state, timeStamp, sequenceNumber, pkName,
                bucketNum, columns, dataSchemaName, dataTableName, indexes, isImmutableRows, physicalNames,
                defaultFamilyName, viewExpression, disableWAL, multiTenant, storeNulls, viewType, viewIndexId, indexType, stats);
    }

    private PTableImpl(PName tenantId, PName schemaName, PName tableName, PTableType type, PIndexState state,
            long timeStamp, long sequenceNumber, PName pkName, Integer bucketNum, List<PColumn> columns,
            PName parentSchemaName, PName parentTableName, List<PTable> indexes, boolean isImmutableRows,
            List<PName> physicalNames, PName defaultFamilyName, String viewExpression, boolean disableWAL, boolean multiTenant,
            boolean storeNulls, ViewType viewType, Short viewIndexId, IndexType indexType,
            PTableStats stats) throws SQLException {
        init(tenantId, schemaName, tableName, type, state, timeStamp, sequenceNumber, pkName, bucketNum, columns,
                stats, schemaName, parentTableName, indexes, isImmutableRows, physicalNames, defaultFamilyName,
                viewExpression, disableWAL, multiTenant, storeNulls, viewType, viewIndexId, indexType);
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
            PName pkName, Integer bucketNum, List<PColumn> columns, PTableStats stats, PName parentSchemaName, PName parentTableName,
            List<PTable> indexes, boolean isImmutableRows, List<PName> physicalNames, PName defaultFamilyName, String viewExpression, boolean disableWAL,
            boolean multiTenant, boolean storeNulls, ViewType viewType, Short viewIndexId,
            IndexType indexType ) throws SQLException {
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
        this.tableStats = stats;
        List<PColumn> pkColumns;
        PColumn[] allColumns;
        
        this.columnsByName = ArrayListMultimap.create(columns.size(), 1);
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
        for (int i = 0; i < columns.size(); i++) {
            PColumn column = columns.get(i);
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
                            throw new ColumnAlreadyExistsException(null, name.getString(), columnName);
                        }
                    }
                }
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
        for (PColumn column : allColumns) {
            PName familyName = column.getFamilyName();
            if (familyName == null) {
            	 pkColumns.add(column);
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
        this.rowKeySchema = builder.build();
        estimatedSize += rowKeySchema.getEstimatedSize();
        Iterator<Map.Entry<PName,List<PColumn>>> iterator = familyMap.entrySet().iterator();
        PColumnFamily[] families = new PColumnFamily[familyMap.size()];
        estimatedSize += this.tableStats.getEstimatedSize();
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
                parentSchemaName.getString(), parentTableName.getString()));
        estimatedSize += PNameFactory.getEstimatedSize(this.parentName);

        this.physicalNames = physicalNames == null ? ImmutableList.<PName>of() : ImmutableList.copyOf(physicalNames);
        for (PName name : this.physicalNames) {
            estimatedSize += name.getEstimatedSize();
        }

        this.estimatedSize = estimatedSize;
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
        int nValues = values.length;
        while (nValues > 0 && (values[nValues-1] == null || values[nValues-1].length == 0)) {
            nValues--;
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
            List<PColumn> columns = getPKColumns();
            int nColumns = columns.size();
            PDataType type = null;
            while (i < nValues && i < nColumns) {
                // Separate variable length column values in key with zero byte
                if (type != null && !type.isFixedWidth()) {
                    os.write(SEPARATOR_BYTE);
                }
                PColumn column = columns.get(i);
                type = column.getDataType();
                // This will throw if the value is null and the type doesn't allow null
                byte[] byteValue = values[i++];
                if (byteValue == null) {
                    byteValue = ByteUtil.EMPTY_BYTE_ARRAY;
                }
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
                if (maxLength != null && type.isFixedWidth() && byteValue.length <= maxLength) {
                    byteValue = StringUtil.padChar(byteValue, maxLength);
                } else if (maxLength != null && byteValue.length > maxLength) {
                    throw new DataExceedsCapacityException(name.getString() + "." + column.getName().getString() + " may not exceed " + maxLength + " bytes (" + SchemaUtil.toString(type, byteValue) + ")");
                }
                os.write(byteValue, 0, byteValue.length);
            }
            // If some non null pk values aren't set, then throw
            if (i < nColumns) {
                PColumn column = columns.get(i);
                type = column.getDataType();
                if (type.isFixedWidth() || !column.isNullable()) {
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

    private PRow newRow(KeyValueBuilder builder, long ts, ImmutableBytesWritable key, int i, byte[]... values) {
        PRow row = new PRowImpl(builder, key, ts, getBucketNum());
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
            byte[]... values) {
        return newRow(builder, ts, key, 0, values);
    }

    @Override
    public PRow newRow(KeyValueBuilder builder, ImmutableBytesWritable key, byte[]... values) {
        return newRow(builder, HConstants.LATEST_TIMESTAMP, key, values);
    }

    @Override
    public PColumn getColumn(String name) throws ColumnNotFoundException, AmbiguousColumnException {
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

        private Put setValues;
        private Delete unsetValues;
        private Delete deleteRow;
        private final long ts;

        public PRowImpl(KeyValueBuilder kvBuilder, ImmutableBytesWritable key, long ts, Integer bucketNum) {
            this.kvBuilder = kvBuilder;
            this.ts = ts;
            if (bucketNum != null) {
                this.key = SaltingUtil.getSaltedKey(key, bucketNum);
                this.keyPtr = new ImmutableBytesPtr(this.key);
            } else {
                this.keyPtr =  new ImmutableBytesPtr(key);
                this.key = ByteUtil.copyKeyBytesIfNecessary(key);
            }

            newMutations();
        }

        @SuppressWarnings("deprecation")
        private void newMutations() {
            this.setValues = new Put(this.key);
            this.unsetValues = new Delete(this.key);
            this.setValues.setWriteToWAL(!isWALDisabled());
            this.unsetValues.setWriteToWAL(!isWALDisabled());
       }

        @Override
        public List<Mutation> toRowMutations() {
            List<Mutation> mutations = new ArrayList<Mutation>(3);
            if (deleteRow != null) {
                // Include only deleteRow mutation if present because it takes precedence over all others
                mutations.add(deleteRow);
            } else {
                // Because we cannot enforce a not null constraint on a KV column (since we don't know if the row exists when
                // we upsert it), se instead add a KV that is always emtpy. This allows us to imitate SQL semantics given the
                // way HBase works.
                addQuietly(setValues, kvBuilder, kvBuilder.buildPut(keyPtr,
                    SchemaUtil.getEmptyColumnFamilyPtr(PTableImpl.this),
                    QueryConstants.EMPTY_COLUMN_BYTES_PTR, ts, ByteUtil.EMPTY_BYTE_ARRAY_PTR));
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
                    }
                }
            }
        }

        @Override
        public void setValue(PColumn column, Object value) {
            byte[] byteValue = value == null ? ByteUtil.EMPTY_BYTE_ARRAY : column.getDataType().toBytes(value);
            setValue(column, byteValue);
        }

        @Override
        public void setValue(PColumn column, byte[] byteValue) {
            deleteRow = null;
            byte[] family = column.getFamilyName().getBytes();
            byte[] qualifier = column.getName().getBytes();
            PDataType type = column.getDataType();
            // Check null, since some types have no byte representation for null
            boolean isNull = type.isNull(byteValue);
            if (isNull && !getStoreNulls()) {
                if (!column.isNullable()) {
                    throw new ConstraintViolationException(name.getString() + "." + column.getName().getString() + " may not be null");
                }
                removeIfPresent(setValues, family, qualifier);
                deleteQuietly(unsetValues, kvBuilder, kvBuilder.buildDeleteColumns(keyPtr, column
                            .getFamilyName().getBytesPtr(), column.getName().getBytesPtr(), ts));
            } else {
                ImmutableBytesWritable ptr = new ImmutableBytesWritable(byteValue == null ?
                        HConstants.EMPTY_BYTE_ARRAY : byteValue);
                Integer	maxLength = column.getMaxLength();
            	if (!isNull && type.isFixedWidth() && maxLength != null) {
    				if (ptr.getLength() <= maxLength) {
                        type.pad(ptr, maxLength);
                    } else if (ptr.getLength() > maxLength) {
                        throw new DataExceedsCapacityException(name.getString() + "." + column.getName().getString() + " may not exceed " + maxLength + " bytes (" + type.toObject(byteValue) + ")");
                    }
            	}
                removeIfPresent(unsetValues, family, qualifier);
                addQuietly(setValues, kvBuilder, kvBuilder.buildPut(keyPtr,
                        column.getFamilyName().getBytesPtr(), column.getName().getBytesPtr(),
                        ts, ptr));
            }
        }

        @Override
        public void delete() {
            newMutations();
            // FIXME: the version of the Delete constructor without the lock args was introduced
            // in 0.94.4, thus if we try to use it here we can no longer use the 0.94.2 version
            // of the client.
            Delete delete = new Delete(key,ts);
            deleteRow = delete;
            // No need to write to the WAL for indexes
            if (PTableImpl.this.getType() == PTableType.INDEX) {
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
        return parentTableName;
    }

    @Override
    public PName getParentName() {
        return parentName;
    }

    @Override
    public synchronized IndexMaintainer getIndexMaintainer(PTable dataTable, PhoenixConnection connection) {
        if (indexMaintainer == null) {
            indexMaintainer = IndexMaintainer.create(dataTable, this, connection);
        }
        return indexMaintainer;
    }

    @Override
    public synchronized void getIndexMaintainers(ImmutableBytesWritable ptr, PhoenixConnection connection) {
        if (indexMaintainersPtr == null) {
            indexMaintainersPtr = new ImmutableBytesWritable();
            if (indexes.isEmpty()) {
                indexMaintainersPtr.set(ByteUtil.EMPTY_BYTE_ARRAY);
            } else {
                IndexMaintainer.serialize(this, indexMaintainersPtr, connection);
            }
        }
        ptr.set(indexMaintainersPtr.get(), indexMaintainersPtr.getOffset(), indexMaintainersPtr.getLength());
    }

    @Override
    public PName getPhysicalName() {
        return physicalNames.isEmpty() ? getName() : physicalNames.get(0);
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
      SortedMap<byte[], GuidePostsInfo> tableGuidePosts = new TreeMap<byte[], GuidePostsInfo>(Bytes.BYTES_COMPARATOR);
        for (PTableProtos.PTableStats pTableStatsProto : table.getGuidePostsList()) {
            List<byte[]> value = Lists.newArrayListWithExpectedSize(pTableStatsProto.getValuesCount());
            for (int j = 0; j < pTableStatsProto.getValuesCount(); j++) {
                value.add(pTableStatsProto.getValues(j).toByteArray());
            }
            // No op
            pTableStatsProto.getGuidePostsByteCount();
            value = Lists.newArrayListWithExpectedSize(pTableStatsProto.getValuesCount());
            PGuidePosts pGuidePosts = pTableStatsProto.getPGuidePosts();
            for(int j = 0; j < pGuidePosts.getGuidePostsCount(); j++) {
                value.add(pGuidePosts.getGuidePosts(j).toByteArray());
            }
            long guidePostsByteCount = pGuidePosts.getByteCount();
            long rowCount = pGuidePosts.getRowCount();
            GuidePostsInfo info = new GuidePostsInfo(guidePostsByteCount, value, rowCount);
            tableGuidePosts.put(pTableStatsProto.getKey().toByteArray(), info);
      }
      PTableStats stats = new PTableStatsImpl(tableGuidePosts, table.getStatsTimeStamp());

      PName dataTableName = null;
      if (table.hasDataTableNameBytes()) {
        dataTableName = PNameFactory.newName(table.getDataTableNameBytes().toByteArray());
      }
      PName defaultFamilyName = null;
      if (table.hasDefaultFamilyName()) {
        defaultFamilyName = PNameFactory.newName(table.getDefaultFamilyName().toByteArray());
      }
      boolean disableWAL = table.getDisableWAL();
      boolean multiTenant = table.getMultiTenant();
      boolean storeNulls = table.getStoreNulls();
      ViewType viewType = null;
      String viewStatement = null;
      List<PName> physicalNames = Collections.emptyList();
      if (tableType == PTableType.VIEW) {
        viewType = ViewType.fromSerializedValue(table.getViewType().toByteArray()[0]);
        if(table.hasViewStatement()){
          viewStatement = (String) PVarchar.INSTANCE.toObject(table.getViewStatement().toByteArray());
        }
      }
      if (tableType == PTableType.VIEW || viewIndexId != null) {
        physicalNames = Lists.newArrayListWithExpectedSize(table.getPhysicalNamesCount());
        for(int i = 0; i < table.getPhysicalNamesCount(); i++){
          physicalNames.add(PNameFactory.newName(table.getPhysicalNames(i).toByteArray()));
        }
      }

      try {
        PTableImpl result = new PTableImpl();
        result.init(tenantId, schemaName, tableName, tableType, indexState, timeStamp, sequenceNumber, pkName,
          (bucketNum == NO_SALTING) ? null : bucketNum, columns, stats, schemaName,dataTableName, indexes,
              isImmutableRows, physicalNames, defaultFamilyName, viewStatement, disableWAL,
                multiTenant, storeNulls, viewType, viewIndexId, indexType);
        return result;
      } catch (SQLException e) {
        throw new RuntimeException(e); // Impossible
      }
    }

    public static PTableProtos.PTable toProto(PTable table) {
      PTableProtos.PTable.Builder builder = PTableProtos.PTable.newBuilder();
      if(table.getTenantId() != null){
        builder.setTenantId(HBaseZeroCopyByteString.wrap(table.getTenantId().getBytes()));
      }
      builder.setSchemaNameBytes(HBaseZeroCopyByteString.wrap(table.getSchemaName().getBytes()));
      builder.setTableNameBytes(HBaseZeroCopyByteString.wrap(table.getTableName().getBytes()));
      builder.setTableType(ProtobufUtil.toPTableTypeProto(table.getType()));
      if (table.getType() == PTableType.INDEX) {
    	if(table.getIndexState() != null) {
    	  builder.setIndexState(table.getIndexState().getSerializedValue());
    	}
    	if(table.getViewIndexId() != null) {
    	  builder.setViewIndexId(table.getViewIndexId());
    	}
    	if(table.getIndexType() != null) {
    	    builder.setIndexType(HBaseZeroCopyByteString.wrap(new byte[]{table.getIndexType().getSerializedValue()}));
    	}
      }
      builder.setSequenceNumber(table.getSequenceNumber());
      builder.setTimeStamp(table.getTimeStamp());
      PName tmp = table.getPKName();
      if (tmp != null) {
        builder.setPkNameBytes(HBaseZeroCopyByteString.wrap(tmp.getBytes()));
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

      for (Map.Entry<byte[], GuidePostsInfo> entry : table.getTableStats().getGuidePosts().entrySet()) {
         PTableProtos.PTableStats.Builder statsBuilder = PTableProtos.PTableStats.newBuilder();
         statsBuilder.setKey(HBaseZeroCopyByteString.wrap(entry.getKey()));
         for (byte[] stat : entry.getValue().getGuidePosts()) {
             statsBuilder.addValues(HBaseZeroCopyByteString.wrap(stat));
         }
         statsBuilder.setGuidePostsByteCount(entry.getValue().getByteCount());
         PGuidePostsProtos.PGuidePosts.Builder guidePstsBuilder = PGuidePostsProtos.PGuidePosts.newBuilder();
         for (byte[] stat : entry.getValue().getGuidePosts()) {
             guidePstsBuilder.addGuidePosts(HBaseZeroCopyByteString.wrap(stat));
         }
         guidePstsBuilder.setByteCount(entry.getValue().getByteCount());
         guidePstsBuilder.setRowCount(entry.getValue().getRowCount());
         statsBuilder.setPGuidePosts(guidePstsBuilder);
         builder.addGuidePosts(statsBuilder.build());
      }
      builder.setStatsTimeStamp(table.getTableStats().getTimestamp());

      if (table.getParentName() != null) {
        builder.setDataTableNameBytes(HBaseZeroCopyByteString.wrap(table.getParentTableName().getBytes()));
      }
      if (table.getDefaultFamilyName()!= null) {
        builder.setDefaultFamilyName(HBaseZeroCopyByteString.wrap(table.getDefaultFamilyName().getBytes()));
      }
      builder.setDisableWAL(table.isWALDisabled());
      builder.setMultiTenant(table.isMultiTenant());
      builder.setStoreNulls(table.getStoreNulls());
      if(table.getType() == PTableType.VIEW){
        builder.setViewType(HBaseZeroCopyByteString.wrap(new byte[]{table.getViewType().getSerializedValue()}));
        builder.setViewStatement(HBaseZeroCopyByteString.wrap(PVarchar.INSTANCE.toBytes(table.getViewStatement())));
      }
      if(table.getType() == PTableType.VIEW || table.getViewIndexId() != null){
        for (int i = 0; i < table.getPhysicalNames().size(); i++) {
          builder.addPhysicalNames(HBaseZeroCopyByteString.wrap(table.getPhysicalNames().get(i).getBytes()));
        }
      }

      return builder.build();
    }

    @Override
    public PTableKey getKey() {
        return key;
    }

    @Override
    public PTableStats getTableStats() {
        return tableStats;
    }

    @Override
    public PName getParentSchemaName() {
        return parentSchemaName;
    }

}
