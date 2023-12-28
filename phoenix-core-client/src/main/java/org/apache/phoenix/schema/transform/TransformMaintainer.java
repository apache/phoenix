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
package org.apache.phoenix.schema.transform;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.thirdparty.com.google.common.collect.Sets;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.coprocessor.generated.ServerCachingProtos;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;

import org.apache.phoenix.hbase.index.ValueGetter;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.SaltingUtil;

import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TrustedByteArrayOutputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class TransformMaintainer extends IndexMaintainer {
    private boolean isMultiTenant;
    // expressions that are not present in the row key of the old table, the expression can also refer to a regular column
    private List<Expression> newTableExpressions;
    private Set<ColumnReference> newTableColumns;

    private List<PDataType> newTableColumnTypes;
    private int newTableColumnCount;
    private byte[] newTableName;
    private int nNewTableSaltBuckets;
    private byte[] oldTableEmptyKeyValueCF;
    private ImmutableBytesPtr emptyKeyValueCFPtr;
    private int nOldTableCFs;
    private boolean newTableWALDisabled;
    private boolean newTableImmutableRows;
    private Set<ColumnReference> allColumns;

    // Transient state
    private final boolean isOldTableSalted;
    private final RowKeySchema oldTableRowKeySchema;

    private int estimatedNewTableRowKeyBytes;
    private ColumnReference newTableEmptyKeyValueRef;
    private ColumnReference oldTableEmptyKeyValueRef;
    private boolean newTableRowKeyOrderOptimizable;

    private PTable.QualifierEncodingScheme newTableEncodingScheme;
    private PTable.ImmutableStorageScheme newTableImmutableStorageScheme;
    private PTable.QualifierEncodingScheme oldTableEncodingScheme;
    private PTable.ImmutableStorageScheme oldTableImmutableStorageScheme;
    /*
     * The first part of the pair is column family name
     * and second part is the column name. The reason we need to track this state is because for certain storage schemes
     * like ImmutableStorageScheme#SINGLE_CELL_ARRAY_WITH_OFFSETS, the column for which we need to generate an new
     * table put/delete is different from the old columns in the phoenix schema.
     */
    private Set<Pair<String, String>> newTableColumnsInfo;
    /*
     * Map of covered columns where a key is column reference for a column in the data table
     * and value is column reference for corresponding column in the new table.
     */
    private Map<ColumnReference, ColumnReference> coveredColumnsMap;

    private String logicalNewTableName;

    public static TransformMaintainer create(PTable oldTable, PTable newTable, PhoenixConnection connection) {
        if (oldTable.getType() == PTableType.INDEX) {
            throw new IllegalArgumentException();
        }
        TransformMaintainer maintainer = new TransformMaintainer(oldTable, newTable, connection);
        return maintainer;
    }

    private TransformMaintainer(RowKeySchema oldRowKeySchema, boolean isOldTableSalted) {
        super(oldRowKeySchema, isOldTableSalted);
        this.oldTableRowKeySchema = oldRowKeySchema;
        this.isOldTableSalted = isOldTableSalted;
    }

    public Set<ColumnReference> getAllColumns() {
        return allColumns;
    }

    public Set<ColumnReference> getCoveredColumns() {
        return coveredColumnsMap.keySet();
    }
    
    private TransformMaintainer(final PTable oldTable, final PTable newTable, PhoenixConnection connection) {
        this(oldTable.getRowKeySchema(), oldTable.getBucketNum() != null);
        this.newTableRowKeyOrderOptimizable = newTable.rowKeyOrderOptimizable();
        this.isMultiTenant = oldTable.isMultiTenant();

        this.newTableEncodingScheme = newTable.getEncodingScheme() == null ? PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS : newTable.getEncodingScheme();
        this.newTableImmutableStorageScheme = newTable.getImmutableStorageScheme() == null ? PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN : newTable.getImmutableStorageScheme();
        this.oldTableEncodingScheme = oldTable.getEncodingScheme() == null ? PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS : oldTable.getEncodingScheme();
        this.oldTableImmutableStorageScheme = oldTable.getImmutableStorageScheme() == null ? PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN : oldTable.getImmutableStorageScheme();

        this.newTableName = newTable.getPhysicalName().getBytes();
        boolean newTableWALDisabled = newTable.isWALDisabled();
        int nNewTableColumns = newTable.getColumns().size();
        int nNewTablePKColumns = newTable.getPKColumns().size();

        List<PColumn> oldTablePKColumns = oldTable.getPKColumns();

        this.newTableColumnCount = oldTablePKColumns.size();

        this.newTableColumnTypes = Lists.newArrayListWithExpectedSize(nNewTablePKColumns);
        this.newTableExpressions = Lists.newArrayListWithExpectedSize(nNewTableColumns);
        this.coveredColumnsMap = Maps.newHashMapWithExpectedSize(nNewTableColumns - nNewTablePKColumns);
        this.nNewTableSaltBuckets = newTable.getBucketNum() == null ? 0 : newTable.getBucketNum();
        this.oldTableEmptyKeyValueCF = SchemaUtil.getEmptyColumnFamily(oldTable);
        this.emptyKeyValueCFPtr = SchemaUtil.getEmptyColumnFamilyPtr(newTable);
        this.nOldTableCFs = oldTable.getColumnFamilies().size();
        this.newTableWALDisabled = newTableWALDisabled;
        this.newTableImmutableRows = newTable.isImmutableRows();
        this.newTableColumnsInfo = Sets.newHashSetWithExpectedSize(nNewTableColumns - nNewTablePKColumns);

        for (int i = 0; i < newTable.getColumnFamilies().size(); i++) {
            PColumnFamily family = newTable.getColumnFamilies().get(i);
            for (PColumn newColumn : family.getColumns()) {
                PColumn oldColumn = getColumnOrNull(oldTable, newColumn.getName().getString(), newColumn.getFamilyName().getString());
                // This can happen during deletion where we don't need covered columns
                if (oldColumn != null) {
                    byte[] oldColumnCq = oldColumn.getColumnQualifierBytes();
                    byte[] newColumnCq = newColumn.getColumnQualifierBytes();
                    this.coveredColumnsMap.put(new ColumnReference(oldColumn.getFamilyName().getBytes(), oldColumnCq),
                            new ColumnReference(newColumn.getFamilyName().getBytes(), newColumnCq));
                }
            }
        }
        this.logicalNewTableName = newTable.getName().getString();
        initCachedState();
    }

    public static PColumn getColumnOrNull(PTable table, String columnName, String familyName) {
        PColumnFamily family;
        try {
            family = table.getColumnFamily(familyName);
        } catch (ColumnFamilyNotFoundException e) {
            return null;
        }
        try {
            return family.getPColumnForColumnName(columnName);
        } catch (ColumnNotFoundException e) {
            return null;
        }
    }

    public Set<ColumnReference> getAllColumnsForDataTable() {
        Set<ColumnReference> result = Sets.newLinkedHashSetWithExpectedSize(newTableExpressions.size() + coveredColumnsMap.size());
        result.addAll(newTableColumns);
        for (ColumnReference colRef : coveredColumnsMap.keySet()) {
            if (oldTableImmutableStorageScheme == PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN) {
                result.add(colRef);
            } else {
                result.add(new ColumnReference(colRef.getFamily(), QueryConstants.SINGLE_KEYVALUE_COLUMN_QUALIFIER_BYTES));
            }
        }
        return result;
    }

    /*
     * Build the old table row key
     */
    public byte[] buildDataRowKey(ImmutableBytesWritable indexRowKeyPtr, byte[][] viewConstants) {
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        TrustedByteArrayOutputStream stream = new TrustedByteArrayOutputStream(estimatedNewTableRowKeyBytes);
        DataOutput output = new DataOutputStream(stream);

        try {
            int dataPosOffset = 0;
            int maxRowKeyOffset = indexRowKeyPtr.getLength();

            oldTableRowKeySchema.iterator(indexRowKeyPtr, ptr, 0);
            // The oldTableRowKeySchema includes the salt byte field,
            while (oldTableRowKeySchema.next(ptr, dataPosOffset, maxRowKeyOffset) != null) {
                output.write(ptr.get(), ptr.getOffset(), ptr.getLength());
                if (!oldTableRowKeySchema.getField(dataPosOffset).getDataType().isFixedWidth()) {
                    output.writeByte(SchemaUtil.getSeparatorByte(oldTableRowKeySchema.rowKeyOrderOptimizable(), ptr.getLength()==0
                            , oldTableRowKeySchema.getField(dataPosOffset)));
                }
                dataPosOffset++;
            }

            byte[] oldTableRowKey = stream.getBuffer();
            return oldTableRowKey;
        } catch (IOException e) {
            throw new RuntimeException(e); // Impossible
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e); // Impossible
            }
        }
    }

    /**
     * Init calculated state reading/creating
     */
    private void initCachedState() {
        this.allColumns = Sets.newLinkedHashSetWithExpectedSize(newTableExpressions.size() + coveredColumnsMap.size());

        byte[] newTableEmptyKvQualifier = EncodedColumnsUtil.getEmptyKeyValueInfo(newTableEncodingScheme).getFirst();
        byte[] oldTableEmptyKvQualifier = EncodedColumnsUtil.getEmptyKeyValueInfo(oldTableEncodingScheme).getFirst();
        newTableEmptyKeyValueRef = new ColumnReference(oldTableEmptyKeyValueCF, newTableEmptyKvQualifier);
        oldTableEmptyKeyValueRef = new ColumnReference(oldTableEmptyKeyValueCF, oldTableEmptyKvQualifier);
        this.newTableColumns = Sets.newLinkedHashSetWithExpectedSize(this.newTableColumnCount);

        for (ColumnReference colRef : coveredColumnsMap.keySet()) {
            if (newTableImmutableStorageScheme == PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN) {
                newTableColumns.add(colRef);
            } else {
                newTableColumns.add(new ColumnReference(colRef.getFamily(), QueryConstants.SINGLE_KEYVALUE_COLUMN_QUALIFIER_BYTES));
            }
        }
    }

    /**
     * For client-side to serialize TransformMaintainer for a given table
     *
     * @param oldTable old table
     * @param ptr      bytes pointer to hold returned serialized value
     * @param newTable new table to serialize
     */
    public static void serialize(PTable oldTable, ImmutableBytesWritable ptr,
                                 PTable newTable, PhoenixConnection connection) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(stream);
        try {
            // Encode data table salting
            WritableUtils.writeVInt(output, oldTable.getBucketNum() == null ? 1 : -1);
            // Write out data row key schema once, since it's the same
            oldTable.getRowKeySchema().write(output);
            org.apache.phoenix.coprocessor.generated.ServerCachingProtos.TransformMaintainer proto =
                    TransformMaintainer.toProto(newTable.getTransformMaintainer(oldTable, connection));
            byte[] protoBytes = proto.toByteArray();
            WritableUtils.writeVInt(output, protoBytes.length);
            output.write(protoBytes);

        } catch (IOException e) {
            throw new RuntimeException(e); // Impossible
        }
        ptr.set(stream.toByteArray(), 0, stream.size());
    }

    @Override
    public Iterator<ColumnReference> iterator() {
        return newTableColumns.iterator();
    }

    public static ServerCachingProtos.TransformMaintainer toProto(TransformMaintainer maintainer) throws IOException {
        ServerCachingProtos.TransformMaintainer.Builder builder = ServerCachingProtos.TransformMaintainer.newBuilder();
        builder.setSaltBuckets(maintainer.nNewTableSaltBuckets);
        builder.setIsMultiTenant(maintainer.isMultiTenant);

        for (ColumnReference colRef : maintainer.newTableColumns) {
            ServerCachingProtos.ColumnReference.Builder cRefBuilder = ServerCachingProtos.ColumnReference.newBuilder();
            cRefBuilder.setFamily(ByteStringer.wrap(colRef.getFamily()));
            cRefBuilder.setQualifier(ByteStringer.wrap(colRef.getQualifier()));
            builder.addNewTableColumns(cRefBuilder.build());
        }

        for (Map.Entry<ColumnReference, ColumnReference> e : maintainer.coveredColumnsMap.entrySet()) {
            ServerCachingProtos.ColumnReference.Builder cRefBuilder = ServerCachingProtos.ColumnReference.newBuilder();
            ColumnReference dataTableColRef = e.getKey();
            cRefBuilder.setFamily(ByteStringer.wrap(dataTableColRef.getFamily()));
            cRefBuilder.setQualifier(ByteStringer.wrap(dataTableColRef.getQualifier()));
            builder.addOldTableColRefForCoveredColumns(cRefBuilder.build());
            ColumnReference newTableColRef = e.getValue();
            cRefBuilder = ServerCachingProtos.ColumnReference.newBuilder();
            cRefBuilder.setFamily(ByteStringer.wrap(newTableColRef.getFamily()));
            cRefBuilder.setQualifier(ByteStringer.wrap(newTableColRef.getQualifier()));
            builder.addNewTableColRefForCoveredColumns(cRefBuilder.build());
        }

        builder.setNewTableColumnCount(maintainer.newTableColumnCount);
        builder.setNewTableName(ByteStringer.wrap(maintainer.newTableName));
        builder.setNewTableRowKeyOrderOptimizable(maintainer.newTableRowKeyOrderOptimizable);
        builder.setOldTableEmptyKeyValueColFamily(ByteStringer.wrap(maintainer.oldTableEmptyKeyValueCF));
        ServerCachingProtos.ImmutableBytesWritable.Builder ibwBuilder = ServerCachingProtos.ImmutableBytesWritable.newBuilder();
        ibwBuilder.setByteArray(ByteStringer.wrap(maintainer.emptyKeyValueCFPtr.get()));
        ibwBuilder.setLength(maintainer.emptyKeyValueCFPtr.getLength());
        ibwBuilder.setOffset(maintainer.emptyKeyValueCFPtr.getOffset());
        builder.setEmptyKeyValueColFamily(ibwBuilder.build());
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            DataOutput output = new DataOutputStream(stream);
            for (Expression expression : maintainer.newTableExpressions) {
                WritableUtils.writeVInt(output, ExpressionType.valueOf(expression).ordinal());
                expression.write(output);
            }
            builder.setNewTableExpressions(ByteStringer.wrap(stream.toByteArray()));
        }

        builder.setNumDataTableColFamilies(maintainer.nOldTableCFs);
        builder.setNewTableWalDisabled(maintainer.newTableWALDisabled);
        builder.setNewTableRowKeyByteSize(maintainer.estimatedNewTableRowKeyBytes);
        builder.setNewTableImmutable(maintainer.newTableImmutableRows);
        for (Pair<String, String> p : maintainer.newTableColumnsInfo) {
            ServerCachingProtos.ColumnInfo.Builder ciBuilder = ServerCachingProtos.ColumnInfo.newBuilder();
            if (p.getFirst() != null) {
                ciBuilder.setFamilyName(p.getFirst());
            }
            ciBuilder.setColumnName(p.getSecond());
            builder.addNewTableColumnInfo(ciBuilder.build());
        }
        builder.setNewTableEncodingScheme(maintainer.newTableEncodingScheme.getSerializedMetadataValue());
        builder.setNewTableImmutableStorageScheme(maintainer.newTableImmutableStorageScheme.getSerializedMetadataValue());
        builder.setLogicalNewTableName(maintainer.logicalNewTableName);
        builder.setOldTableEncodingScheme(maintainer.oldTableEncodingScheme.getSerializedMetadataValue());
        builder.setOldTableImmutableStorageScheme(maintainer.oldTableImmutableStorageScheme.getSerializedMetadataValue());
        return builder.build();
    }

    public static TransformMaintainer fromProto(ServerCachingProtos.TransformMaintainer proto, RowKeySchema dataTableRowKeySchema, boolean isDataTableSalted) throws IOException {
        TransformMaintainer maintainer = new TransformMaintainer(dataTableRowKeySchema, isDataTableSalted);
        maintainer.nNewTableSaltBuckets = proto.getSaltBuckets();
        maintainer.isMultiTenant = proto.getIsMultiTenant();
        List<ServerCachingProtos.ColumnReference> newTableColList = proto.getNewTableColumnsList();
        maintainer.newTableColumns = new HashSet<ColumnReference>(newTableColList.size());
        for (ServerCachingProtos.ColumnReference colRefFromProto : newTableColList) {
            maintainer.newTableColumns.add(new ColumnReference(colRefFromProto.getFamily().toByteArray(), colRefFromProto.getQualifier().toByteArray()));
        }

        maintainer.newTableName = proto.getNewTableName().toByteArray();
        if (proto.getNewTableColumnCount() != -1) {
            maintainer.newTableColumnCount = proto.getNewTableColumnCount();
        }

        maintainer.newTableRowKeyOrderOptimizable = proto.getNewTableRowKeyOrderOptimizable();
        maintainer.oldTableEmptyKeyValueCF = proto.getOldTableEmptyKeyValueColFamily().toByteArray();
        ServerCachingProtos.ImmutableBytesWritable emptyKeyValueColFamily = proto.getEmptyKeyValueColFamily();
        maintainer.emptyKeyValueCFPtr = new ImmutableBytesPtr(emptyKeyValueColFamily.getByteArray().toByteArray(), emptyKeyValueColFamily.getOffset(), emptyKeyValueColFamily.getLength());

        maintainer.nOldTableCFs = proto.getNumDataTableColFamilies();
        maintainer.newTableWALDisabled = proto.getNewTableWalDisabled();
        maintainer.estimatedNewTableRowKeyBytes = proto.getNewTableRowKeyByteSize();
        maintainer.newTableImmutableRows = proto.getNewTableImmutable();
        List<ServerCachingProtos.ColumnInfo> newTblColumnInfoList = proto.getNewTableColumnInfoList();
        maintainer.newTableColumnsInfo = Sets.newHashSet();
        for (ServerCachingProtos.ColumnInfo info : newTblColumnInfoList) {
            maintainer.newTableColumnsInfo.add(new Pair<>(info.getFamilyName(), info.getColumnName()));
        }
        maintainer.newTableExpressions = new ArrayList<>();
        try (ByteArrayInputStream stream = new ByteArrayInputStream(proto.getNewTableExpressions().toByteArray())) {
            DataInput input = new DataInputStream(stream);
            while (stream.available() > 0) {
                int expressionOrdinal = WritableUtils.readVInt(input);
                Expression expression = ExpressionType.values()[expressionOrdinal].newInstance();
                expression.readFields(input);
                maintainer.newTableExpressions.add(expression);
            }
        }
        // proto doesn't support single byte so need an explicit cast here
        maintainer.newTableEncodingScheme = PTable.QualifierEncodingScheme.fromSerializedValue((byte) proto.getNewTableEncodingScheme());
        maintainer.newTableImmutableStorageScheme = PTable.ImmutableStorageScheme.fromSerializedValue((byte) proto.getNewTableImmutableStorageScheme());
        maintainer.oldTableEncodingScheme = PTable.QualifierEncodingScheme.fromSerializedValue((byte) proto.getOldTableEncodingScheme());
        maintainer.oldTableImmutableStorageScheme = PTable.ImmutableStorageScheme.fromSerializedValue((byte) proto.getOldTableImmutableStorageScheme());

        List<ServerCachingProtos.ColumnReference> oldTableColRefsForCoveredColumnsList = proto.getOldTableColRefForCoveredColumnsList();
        List<ServerCachingProtos.ColumnReference> newTableColRefsForCoveredColumnsList = proto.getNewTableColRefForCoveredColumnsList();
        maintainer.coveredColumnsMap = Maps.newHashMapWithExpectedSize(oldTableColRefsForCoveredColumnsList.size());
        Iterator<ServerCachingProtos.ColumnReference> newTableColRefItr = newTableColRefsForCoveredColumnsList.iterator();
        for (ServerCachingProtos.ColumnReference colRefFromProto : oldTableColRefsForCoveredColumnsList) {
            ColumnReference oldTableColRef = new ColumnReference(colRefFromProto.getFamily().toByteArray(), colRefFromProto.getQualifier().toByteArray());
            ColumnReference newTableColRef;
            ServerCachingProtos.ColumnReference fromProto = newTableColRefItr.next();
            newTableColRef = new ColumnReference(fromProto.getFamily().toByteArray(), fromProto.getQualifier().toByteArray());
            maintainer.coveredColumnsMap.put(oldTableColRef, newTableColRef);
        }
        maintainer.logicalNewTableName = proto.getLogicalNewTableName();
        maintainer.initCachedState();
        return maintainer;
    }


    public static List<IndexMaintainer> deserialize(byte[] buf) {
        return deserialize(buf, 0, buf.length);
    }

    private static List<IndexMaintainer> deserialize(byte[] buf, int offset, int length) {
        List<IndexMaintainer> maintainers = Collections.emptyList();
        if (length > 0) {
            ByteArrayInputStream stream = new ByteArrayInputStream(buf, offset, length);
            DataInput input = new DataInputStream(stream);
            try {
                int size = WritableUtils.readVInt(input);
                boolean isDataTableSalted = size < 0;
                size = Math.abs(size);
                RowKeySchema rowKeySchema = new RowKeySchema();
                rowKeySchema.readFields(input);
                maintainers = Lists.newArrayListWithExpectedSize(size);
                for (int i = 0; i < size; i++) {
                    int protoSize = WritableUtils.readVInt(input);
                    byte[] b = new byte[protoSize];
                    input.readFully(b);
                    ServerCachingProtos.TransformMaintainer proto = ServerCachingProtos.TransformMaintainer.parseFrom(b);
                    maintainers.add(TransformMaintainer.fromProto(proto, rowKeySchema, isDataTableSalted));
                }
            } catch (IOException e) {
                throw new RuntimeException(e); // Impossible
            }
        }
        return maintainers;
    }

    // Return new table's name
    public byte[] getIndexTableName() {
        return newTableName;
    }

    // Builds new table's rowkey using the old table's rowkey.
    // This method will change when we support rowkey related transforms
    public byte[] buildRowKey(ValueGetter valueGetter, ImmutableBytesWritable rowKeyPtr, byte[] regionStartKey, byte[] regionEndKey, long ts)  {
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        boolean isNewTableSalted = nNewTableSaltBuckets > 0;

        try (TrustedByteArrayOutputStream stream = new TrustedByteArrayOutputStream(estimatedNewTableRowKeyBytes)){
            DataOutput output = new DataOutputStream(stream);

            if (isNewTableSalted) {
                output.write(0); // will be set at end to new table salt byte
            }
            // The oldTableRowKeySchema includes the salt byte field,
            // so we must adjust for that here.
            int dataPosOffset = isOldTableSalted ? 1 : 0 ;
            //BitSet viewConstantColumnBitSet = this.rowKeyMetaData.getViewConstantColumnBitSet();
            // Skip data table salt byte
            int maxRowKeyOffset = rowKeyPtr.getOffset() + rowKeyPtr.getLength();
            oldTableRowKeySchema.iterator(rowKeyPtr, ptr, dataPosOffset);

            // Write new table row key
            int trailingVariableWidthColumnNum = 0;
            while (oldTableRowKeySchema.next(ptr, dataPosOffset, maxRowKeyOffset) != null) {
                output.write(ptr.get(), ptr.getOffset(), ptr.getLength());
                if (!oldTableRowKeySchema.getField(dataPosOffset).getDataType().isFixedWidth()) {
                    output.writeByte(SchemaUtil.getSeparatorByte(newTableRowKeyOrderOptimizable, ptr.getLength()==0
                            , oldTableRowKeySchema.getField(dataPosOffset)));
                    trailingVariableWidthColumnNum++;
                } else {
                    trailingVariableWidthColumnNum = 0;
                }

                dataPosOffset++;
            }

            byte[] newTableRowKey = stream.getBuffer();
            // Remove trailing nulls
            int length = stream.size();
            // The existing code does not eliminate the separator if the data type is not nullable. It not clear why.
            // The actual bug is in the calculation of maxTrailingNulls with view indexes. So, in order not to impact some other cases, we should keep minLength check here.
            while (trailingVariableWidthColumnNum > 0 && length > 0 && newTableRowKey[length-1] == QueryConstants.SEPARATOR_BYTE) {
                length--;
                trailingVariableWidthColumnNum--;
            }

            if (isNewTableSalted) {
                // Set salt byte
                byte saltByte = SaltingUtil.getSaltingByte(newTableRowKey, SaltingUtil.NUM_SALTING_BYTES, length-SaltingUtil.NUM_SALTING_BYTES, nNewTableSaltBuckets);
                newTableRowKey[0] = saltByte;
            }
            return newTableRowKey.length == length ? newTableRowKey : Arrays.copyOf(newTableRowKey, length);
        } catch (IOException e) {
            throw new RuntimeException(e); // Impossible
        }
    }

    public Put buildUpdateMutation(KeyValueBuilder kvBuilder, ValueGetter valueGetter, ImmutableBytesWritable oldRowKeyPtr,
                                   long ts, byte[] regionStartKey, byte[] regionEndKey, boolean verified) throws IOException {
        byte[] newRowKey = this.buildRowKey(valueGetter, oldRowKeyPtr, regionStartKey, regionEndKey, ts);
        return buildUpdateMutation(kvBuilder, valueGetter, oldRowKeyPtr, ts, regionStartKey, regionEndKey,
                newRowKey, this.getEmptyKeyValueFamily(), coveredColumnsMap,
                newTableEmptyKeyValueRef, newTableWALDisabled, oldTableImmutableStorageScheme, newTableImmutableStorageScheme,
                newTableEncodingScheme, oldTableEncodingScheme, verified);
    }

    public ImmutableBytesPtr getEmptyKeyValueFamily() {
        return emptyKeyValueCFPtr;
    }

    public byte[] getEmptyKeyValueQualifier() {
        return newTableEmptyKeyValueRef.getQualifier();
    }

    public byte[] getDataEmptyKeyValueCF() {
        return oldTableEmptyKeyValueCF;
    }

    public byte[] getEmptyKeyValueQualifierForDataTable() {
        return oldTableEmptyKeyValueRef.getQualifier();
    }
}