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
package org.apache.phoenix.index;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.hbase.index.ValueGetter;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.ValueSchema;
import org.apache.phoenix.schema.ValueSchema.Field;
import org.apache.phoenix.util.BitSet;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TrustedByteArrayOutputStream;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * 
 * Class that builds index row key from data row key and current state of
 * row and caches any covered columns. Client-side serializes into byte array using 
 * @link #serialize(PTable, ImmutableBytesWritable)}
 * and transmits to server-side through either the 
 * {@link org.apache.phoenix.index.PhoenixIndexCodec#INDEX_MD}
 * Mutation attribute or as a separate RPC call using 
 * {@link org.apache.phoenix.cache.ServerCacheClient})
 *
 * 
 * @since 2.1.0
 */
public class IndexMaintainer implements Writable, Iterable<ColumnReference> {
    
    public static IndexMaintainer create(PTable dataTable, PTable index) {
        if (dataTable.getType() == PTableType.INDEX || index.getType() != PTableType.INDEX || !dataTable.getIndexes().contains(index)) {
            throw new IllegalArgumentException();
        }
        IndexMaintainer maintainer = new IndexMaintainer(dataTable, index);
        int indexPosOffset = (index.getBucketNum() == null ? 0 : 1) + (maintainer.isMultiTenant ? 1 : 0) + (maintainer.viewIndexId == null ? 0 : 1);
        RowKeyMetaData rowKeyMetaData = maintainer.getRowKeyMetaData();
        int indexColByteSize = 0;
        for (int i = indexPosOffset; i < index.getPKColumns().size(); i++) {
            PColumn indexColumn = index.getPKColumns().get(i);
            int indexPos = i - indexPosOffset;
            PColumn column = IndexUtil.getDataColumn(dataTable, indexColumn.getName().getString());
            boolean isPKColumn = SchemaUtil.isPKColumn(column);
            if (isPKColumn) {
                int dataPkPos = dataTable.getPKColumns().indexOf(column) - (dataTable.getBucketNum() == null ? 0 : 1) - (maintainer.isMultiTenant ? 1 : 0);
                rowKeyMetaData.setIndexPkPosition(dataPkPos, indexPos);
            } else {
                indexColByteSize += column.getDataType().isFixedWidth() ? SchemaUtil.getFixedByteSize(column) : ValueSchema.ESTIMATED_VARIABLE_LENGTH_SIZE;
                maintainer.getIndexedColumnTypes().add(column.getDataType());
                maintainer.getIndexedColumns().add(new ColumnReference(column.getFamilyName().getBytes(), column.getName().getBytes()));
            }
            if (indexColumn.getSortOrder() == SortOrder.DESC) {
                rowKeyMetaData.getDescIndexColumnBitSet().set(indexPos);
            }
        }
        for (int i = 0; i < index.getColumnFamilies().size(); i++) {
            PColumnFamily family = index.getColumnFamilies().get(i);
            for (PColumn indexColumn : family.getColumns()) {
                PColumn column = IndexUtil.getDataColumn(dataTable, indexColumn.getName().getString());
                maintainer.getCoverededColumns().add(new ColumnReference(column.getFamilyName().getBytes(), column.getName().getBytes()));
            }
        }
        maintainer.estimatedIndexRowKeyBytes = maintainer.estimateIndexRowKeyByteSize(indexColByteSize);
        maintainer.initCachedState();
        return maintainer;
    }
    
    public static Iterator<PTable> nonDisabledIndexIterator(Iterator<PTable> indexes) {
        return Iterators.filter(indexes, new Predicate<PTable>() {
            @Override
            public boolean apply(PTable index) {
                return !PIndexState.DISABLE.equals(index.getIndexState());
            }
        });
    }
    
    /**
     * For client-side to serialize all IndexMaintainers for a given table
     * @param dataTable data table
     * @param ptr bytes pointer to hold returned serialized value
     */
    public static void serialize(PTable dataTable, ImmutableBytesWritable ptr) {
        Iterator<PTable> indexes = nonDisabledIndexIterator(dataTable.getIndexes().iterator());
        if (dataTable.isImmutableRows() || !indexes.hasNext()) {
            ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
            return;
        }
        int nIndexes = 0;
        int estimatedSize = dataTable.getRowKeySchema().getEstimatedByteSize() + 2;
        while (indexes.hasNext()) {
            nIndexes++;
            PTable index = indexes.next();
            estimatedSize += index.getIndexMaintainer(dataTable).getEstimatedByteSize();
        }
        TrustedByteArrayOutputStream stream = new TrustedByteArrayOutputStream(estimatedSize + 1);
        DataOutput output = new DataOutputStream(stream);
        try {
            // Encode data table salting in sign of number of indexes
            WritableUtils.writeVInt(output, nIndexes * (dataTable.getBucketNum() == null ? 1 : -1));
            // Write out data row key schema once, since it's the same for all index maintainers
            dataTable.getRowKeySchema().write(output);
            indexes = nonDisabledIndexIterator(dataTable.getIndexes().iterator());
            while (indexes.hasNext()) {
                    indexes.next().getIndexMaintainer(dataTable).write(output);
            }
        } catch (IOException e) {
            throw new RuntimeException(e); // Impossible
        }
        ptr.set(stream.getBuffer(), 0, stream.size());
    }
    
    public static List<IndexMaintainer> deserialize(ImmutableBytesWritable metaDataPtr,
            KeyValueBuilder builder) {
        return deserialize(metaDataPtr.get(), metaDataPtr.getOffset(), metaDataPtr.getLength());
    }
    
    public static List<IndexMaintainer> deserialize(byte[] buf) {
        return deserialize(buf, 0, buf.length);
    }

    public static List<IndexMaintainer> deserialize(byte[] buf, int offset, int length) {
        ByteArrayInputStream stream = new ByteArrayInputStream(buf, offset, length);
        DataInput input = new DataInputStream(stream);
        List<IndexMaintainer> maintainers = Collections.emptyList();
        try {
            int size = WritableUtils.readVInt(input);
            boolean isDataTableSalted = size < 0;
            size = Math.abs(size);
            RowKeySchema rowKeySchema = new RowKeySchema();
            rowKeySchema.readFields(input);
            maintainers = Lists.newArrayListWithExpectedSize(size);
            for (int i = 0; i < size; i++) {
                IndexMaintainer maintainer = new IndexMaintainer(rowKeySchema, isDataTableSalted);
                maintainer.readFields(input);
                maintainers.add(maintainer);
            }
        } catch (IOException e) {
            throw new RuntimeException(e); // Impossible
        }
        return maintainers;
    }

    private byte[] viewIndexId;
    private boolean isMultiTenant;
    private Set<ColumnReference> indexedColumns;
    private Set<ColumnReference> coveredColumns;
    private Set<ColumnReference> allColumns;
    private List<PDataType> indexedColumnTypes;
    private RowKeyMetaData rowKeyMetaData;
    private byte[] indexTableName;
    private int nIndexSaltBuckets;
    private byte[] dataEmptyKeyValueCF;
    private ImmutableBytesPtr emptyKeyValueCFPtr;
    private int nDataCFs;
    private boolean indexWALDisabled;

    // Transient state
    private final boolean isDataTableSalted;
    private final RowKeySchema dataRowKeySchema;
    
    private List<ImmutableBytesPtr> indexQualifiers;
    private int estimatedIndexRowKeyBytes;
    private int[] dataPkPosition;
    private int maxTrailingNulls;
    private ColumnReference dataEmptyKeyValueRef;
    
    private IndexMaintainer(RowKeySchema dataRowKeySchema, boolean isDataTableSalted) {
        this.dataRowKeySchema = dataRowKeySchema;
        this.isDataTableSalted = isDataTableSalted;
    }

    private IndexMaintainer(PTable dataTable, PTable index) {
        this(dataTable.getRowKeySchema(), dataTable.getBucketNum() != null);
        this.isMultiTenant = dataTable.isMultiTenant();
        this.viewIndexId = index.getViewIndexId() == null ? null : MetaDataUtil.getViewIndexIdDataType().toBytes(index.getViewIndexId());

        RowKeySchema dataRowKeySchema = dataTable.getRowKeySchema();
        boolean isDataTableSalted = dataTable.getBucketNum() != null;
        byte[] indexTableName = index.getPhysicalName().getBytes();
        Integer nIndexSaltBuckets = index.getBucketNum();
        boolean indexWALDisabled = index.isWALDisabled();
        int indexPosOffset = (index.getBucketNum() == null ? 0 : 1) + (this.isMultiTenant ? 1 : 0) + (this.viewIndexId == null ? 0 : 1);
        int nIndexColumns = index.getColumns().size() - indexPosOffset;
        int nIndexPKColumns = index.getPKColumns().size() - indexPosOffset;
        this.rowKeyMetaData = newRowKeyMetaData(nIndexPKColumns);
        BitSet bitSet = this.rowKeyMetaData.getViewConstantColumnBitSet();

        int dataPosOffset = (isDataTableSalted ? 1 : 0) + (this.isMultiTenant ? 1 : 0);
        int nDataPKColumns = dataRowKeySchema.getFieldCount() - dataPosOffset;
        // For indexes on views, we need to remember which data columns are "constants"
        // These are the values in a VIEW where clause. For these, we don't put them in the
        // index, as they're the same for every row in the index.
        if (dataTable.getType() == PTableType.VIEW) {
            List<PColumn>dataPKColumns = dataTable.getPKColumns();
            for (int i = dataPosOffset; i < dataPKColumns.size(); i++) {
                PColumn dataPKColumn = dataPKColumns.get(i);
                if (dataPKColumn.getViewConstant() != null) {
                    bitSet.set(i);
                    nDataPKColumns--;
                }
            }
        }
        this.indexTableName = indexTableName;
        this.indexedColumns = Sets.newLinkedHashSetWithExpectedSize(nIndexPKColumns-nDataPKColumns);
        this.indexedColumnTypes = Lists.<PDataType>newArrayListWithExpectedSize(nIndexPKColumns-nDataPKColumns);
        this.coveredColumns = Sets.newLinkedHashSetWithExpectedSize(nIndexColumns-nIndexPKColumns);
        this.allColumns = Sets.newLinkedHashSetWithExpectedSize(nDataPKColumns + nIndexColumns);
        this.allColumns.addAll(indexedColumns);
        this.allColumns.addAll(coveredColumns);
        this.nIndexSaltBuckets  = nIndexSaltBuckets == null ? 0 : nIndexSaltBuckets;
        this.dataEmptyKeyValueCF = SchemaUtil.getEmptyColumnFamily(dataTable);
        this.emptyKeyValueCFPtr = SchemaUtil.getEmptyColumnFamilyPtr(index);
        this.nDataCFs = dataTable.getColumnFamilies().size();
        this.indexWALDisabled = indexWALDisabled;
    }

    public byte[] buildRowKey(ValueGetter valueGetter, ImmutableBytesWritable rowKeyPtr)  {
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        TrustedByteArrayOutputStream stream = new TrustedByteArrayOutputStream(estimatedIndexRowKeyBytes);
        DataOutput output = new DataOutputStream(stream);
        try {
            if (nIndexSaltBuckets > 0) {
                output.write(0); // will be set at end to index salt byte
            }
            // The dataRowKeySchema includes the salt byte field,
            // so we must adjust for that here.
            int dataPosOffset = isDataTableSalted ? 1 : 0 ;
            int nIndexedColumns = getIndexPkColumnCount();
            int[][] dataRowKeyLocator = new int[2][nIndexedColumns];
            // Skip data table salt byte
            int maxRowKeyOffset = rowKeyPtr.getOffset() + rowKeyPtr.getLength();
            dataRowKeySchema.iterator(rowKeyPtr, ptr, dataPosOffset);
            if (isMultiTenant) {
                dataRowKeySchema.next(ptr, dataPosOffset, maxRowKeyOffset);
                output.write(ptr.get(), ptr.getOffset(), ptr.getLength());
                if (!dataRowKeySchema.getField(dataPosOffset).getDataType().isFixedWidth()) {
                    output.writeByte(QueryConstants.SEPARATOR_BYTE);
                }
                dataPosOffset++;
            }
            if (viewIndexId != null) {
                output.write(viewIndexId);
            }
            
            BitSet viewConstantColumnBitSet = this.rowKeyMetaData.getViewConstantColumnBitSet();
            // Write index row key
            for (int i = dataPosOffset; i < dataRowKeySchema.getFieldCount(); i++) {
                Boolean hasValue=dataRowKeySchema.next(ptr, i, maxRowKeyOffset);
                // Ignore view constants from the data table, as these
                // don't need to appear in the index (as they're the
                // same for all rows in this index)
                if (!viewConstantColumnBitSet.get(i)) {
                    int pos = rowKeyMetaData.getIndexPkPosition(i-dataPosOffset);
                    if (Boolean.TRUE.equals(hasValue)) {
                        dataRowKeyLocator[0][pos] = ptr.getOffset();
                        dataRowKeyLocator[1][pos] = ptr.getLength();
                    } else {
                        dataRowKeyLocator[0][pos] = 0;
                        dataRowKeyLocator[1][pos] = 0;
                    }
                }
            }
            BitSet descIndexColumnBitSet = rowKeyMetaData.getDescIndexColumnBitSet();
            int j = 0;
            Iterator<ColumnReference> iterator = indexedColumns.iterator();
            for (int i = 0; i < nIndexedColumns; i++) {
                PDataType dataColumnType;
                boolean isNullable = true;
                boolean isDataColumnInverted = false;
                SortOrder dataSortOrder = SortOrder.getDefault();
                if (dataPkPosition[i] == -1) {
                    dataColumnType = indexedColumnTypes.get(j);
                    ImmutableBytesPtr value = valueGetter.getLatestValue(iterator.next());
                    if (value == null) {
                        ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
                    } else {
                        ptr.set(value.copyBytesIfNecessary());
                    }
                    j++;
               } else {
                    Field field = dataRowKeySchema.getField(dataPkPosition[i]);
                    dataColumnType = field.getDataType();
                    ptr.set(rowKeyPtr.get(), dataRowKeyLocator[0][i], dataRowKeyLocator[1][i]);
                    dataSortOrder = field.getSortOrder();
                    isDataColumnInverted = dataSortOrder != SortOrder.ASC;
                    isNullable = field.isNullable();
                }
                PDataType indexColumnType = IndexUtil.getIndexColumnDataType(isNullable, dataColumnType);
                boolean isBytesComparable = dataColumnType.isBytesComparableWith(indexColumnType) ;
                if (isBytesComparable && isDataColumnInverted == descIndexColumnBitSet.get(i)) {
                    output.write(ptr.get(), ptr.getOffset(), ptr.getLength());
                } else {
                    if (!isBytesComparable)  {
                        indexColumnType.coerceBytes(ptr, dataColumnType, dataSortOrder, SortOrder.getDefault());
                    }
                    if (descIndexColumnBitSet.get(i) != isDataColumnInverted) {
                        writeInverted(ptr.get(), ptr.getOffset(), ptr.getLength(), output);
                    } else {
                        output.write(ptr.get(), ptr.getOffset(), ptr.getLength());
                    }
                }
                if (!indexColumnType.isFixedWidth()) {
                    output.writeByte(QueryConstants.SEPARATOR_BYTE);
                }
            }
            int length = stream.size();
            int minLength = length - maxTrailingNulls;
            byte[] indexRowKey = stream.getBuffer();
            // Remove trailing nulls
            while (length > minLength && indexRowKey[length-1] == QueryConstants.SEPARATOR_BYTE) {
                length--;
            }
            if (nIndexSaltBuckets > 0) {
                // Set salt byte
                byte saltByte = SaltingUtil.getSaltingByte(indexRowKey, SaltingUtil.NUM_SALTING_BYTES, length-SaltingUtil.NUM_SALTING_BYTES, nIndexSaltBuckets);
                indexRowKey[0] = saltByte;
            }
            return indexRowKey.length == length ? indexRowKey : Arrays.copyOf(indexRowKey, length);
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

    public Put buildUpdateMutation(KeyValueBuilder kvBuilder, ValueGetter valueGetter, ImmutableBytesWritable dataRowKeyPtr, long ts) throws IOException {
        Put put = null;
        // New row being inserted: add the empty key value
        if (valueGetter.getLatestValue(dataEmptyKeyValueRef) == null) {
            byte[] indexRowKey = this.buildRowKey(valueGetter, dataRowKeyPtr);
            put = new Put(indexRowKey);
            // add the keyvalue for the empty row
            put.add(kvBuilder.buildPut(new ImmutableBytesPtr(indexRowKey),
                this.getEmptyKeyValueFamily(), QueryConstants.EMPTY_COLUMN_BYTES_PTR, ts,
                ByteUtil.EMPTY_BYTE_ARRAY_PTR));
            put.setDurability(!indexWALDisabled ? Durability.USE_DEFAULT : Durability.SKIP_WAL);
        }
        int i = 0;
        for (ColumnReference ref : this.getCoverededColumns()) {
            ImmutableBytesPtr cq = this.indexQualifiers.get(i++);
            ImmutableBytesPtr value = valueGetter.getLatestValue(ref);
            byte[] indexRowKey = this.buildRowKey(valueGetter, dataRowKeyPtr);
            ImmutableBytesPtr rowKey = new ImmutableBytesPtr(indexRowKey);
            if (value != null) {
                if (put == null) {
                    put = new Put(indexRowKey);
                    put.setDurability(!indexWALDisabled ? Durability.USE_DEFAULT : Durability.SKIP_WAL);
                }
                //this is a little bit of extra work for installations that are running <0.94.14, but that should be rare and is a short-term set of wrappers - it shouldn't kill GC
                put.add(kvBuilder.buildPut(rowKey, ref.getFamilyWritable(), cq, ts, value));
            }
        }
        return put;
    }

    public Put buildUpdateMutation(KeyValueBuilder kvBuilder, ValueGetter valueGetter, ImmutableBytesWritable dataRowKeyPtr) throws IOException {
        return buildUpdateMutation(kvBuilder, valueGetter, dataRowKeyPtr, HConstants.LATEST_TIMESTAMP);
    }
    
    public Delete buildDeleteMutation(KeyValueBuilder kvBuilder, ValueGetter valueGetter, ImmutableBytesWritable dataRowKeyPtr, Collection<KeyValue> pendingUpdates) throws IOException {
        return buildDeleteMutation(kvBuilder, valueGetter, dataRowKeyPtr, pendingUpdates, HConstants.LATEST_TIMESTAMP);
    }
    
    public boolean isRowDeleted(Collection<KeyValue> pendingUpdates) {
        int nDeleteCF = 0;
        for (KeyValue kv : pendingUpdates) {
            if (kv.getTypeByte() == KeyValue.Type.DeleteFamily.getCode()) {
                nDeleteCF++;
                boolean isEmptyCF = Bytes.compareTo(kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength(), 
                  dataEmptyKeyValueCF, 0, dataEmptyKeyValueCF.length) == 0;
                // This is what a delete looks like on the client side for immutable indexing...
                if (isEmptyCF) {
                    return true;
                }
            }
        }
        // This is what a delete looks like on the server side for mutable indexing...
        return nDeleteCF == this.nDataCFs;
    }
    
    private boolean hasIndexedColumnChanged(ValueGetter oldState, Collection<KeyValue> pendingUpdates) throws IOException {
        if (pendingUpdates.isEmpty()) {
            return false;
        }
        Map<ColumnReference,KeyValue> newState = Maps.newHashMapWithExpectedSize(pendingUpdates.size()); 
        for (KeyValue kv : pendingUpdates) {
            newState.put(new ColumnReference(CellUtil.cloneFamily(kv), CellUtil.cloneQualifier(kv)), kv);
        }
        for (ColumnReference ref : indexedColumns) {
            KeyValue newValue = newState.get(ref);
            if (newValue != null) { // Indexed column was potentially changed
                ImmutableBytesPtr oldValue = oldState.getLatestValue(ref);
                // If there was no old value or the old value is different than the new value, the index row needs to be deleted
                if (oldValue == null || 
                        Bytes.compareTo(oldValue.get(), oldValue.getOffset(), oldValue.getLength(), 
                          newValue.getValueArray(), newValue.getValueOffset(), newValue.getValueLength()) != 0){
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Used for immutable indexes that only index PK column values. In that case, we can handle a data row deletion,
     * since we can build the corresponding index row key.
     */
    public Delete buildDeleteMutation(KeyValueBuilder kvBuilder, ImmutableBytesWritable dataRowKeyPtr, long ts) throws IOException {
        return buildDeleteMutation(kvBuilder, null, dataRowKeyPtr, Collections.<KeyValue>emptyList(), ts);
    }
    
    @SuppressWarnings("deprecation")
    public Delete buildDeleteMutation(KeyValueBuilder kvBuilder, ValueGetter oldState, ImmutableBytesWritable dataRowKeyPtr, Collection<KeyValue> pendingUpdates, long ts) throws IOException {
        byte[] indexRowKey = this.buildRowKey(oldState, dataRowKeyPtr);
        // Delete the entire row if any of the indexed columns changed
        if (oldState == null || isRowDeleted(pendingUpdates) || hasIndexedColumnChanged(oldState, pendingUpdates)) { // Deleting the entire row
            Delete delete = new Delete(indexRowKey, ts);
            delete.setDurability(!indexWALDisabled ? Durability.USE_DEFAULT : Durability.SKIP_WAL);
            return delete;
        }
        Delete delete = null;
        // Delete columns for missing key values
        for (KeyValue kv : pendingUpdates) {
            if (kv.getTypeByte() != KeyValue.Type.Put.getCode()) {
                ColumnReference ref = new ColumnReference(kv.getFamily(), kv.getQualifier());
                if (coveredColumns.contains(ref)) {
                    if (delete == null) {
                        delete = new Delete(indexRowKey);                    
                        delete.setDurability(!indexWALDisabled ? Durability.USE_DEFAULT : Durability.SKIP_WAL);
                    }
                    delete.deleteColumns(ref.getFamily(), IndexUtil.getIndexColumnName(ref.getFamily(), ref.getQualifier()), ts);
                }
            }
        }
        return delete;
  }

    public byte[] getIndexTableName() {
        return indexTableName;
    }
    
    public Set<ColumnReference> getCoverededColumns() {
        return coveredColumns;
    }

    public Set<ColumnReference> getIndexedColumns() {
        return indexedColumns;
    }

    public Set<ColumnReference> getAllColumns() {
        return allColumns;
    }
    
    private ImmutableBytesPtr getEmptyKeyValueFamily() {
        // Since the metadata of an index table will never change,
        // we can infer this based on the family of the first covered column
        // If if there are no covered columns, we know it's our default name
        return emptyKeyValueCFPtr;
    }

    private RowKeyMetaData getRowKeyMetaData() {
        return rowKeyMetaData;
    }
    
    private List<PDataType> getIndexedColumnTypes() {
        return indexedColumnTypes;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        int encodedIndexSaltBucketsAndMultiTenant = WritableUtils.readVInt(input);
        isMultiTenant = encodedIndexSaltBucketsAndMultiTenant < 0;
        nIndexSaltBuckets = Math.abs(encodedIndexSaltBucketsAndMultiTenant) - 1;
        int encodedIndexedColumnsAndViewId = WritableUtils.readVInt(input);
        boolean hasViewIndexId = encodedIndexedColumnsAndViewId < 0;
        if (hasViewIndexId) {
            // Fixed length
            viewIndexId = new byte[MetaDataUtil.getViewIndexIdDataType().getByteSize()];
            input.readFully(viewIndexId);
        }
        int nIndexedColumns = Math.abs(encodedIndexedColumnsAndViewId) - 1;
        indexedColumns = Sets.newLinkedHashSetWithExpectedSize(nIndexedColumns);
        for (int i = 0; i < nIndexedColumns; i++) {
            byte[] cf = Bytes.readByteArray(input);
            byte[] cq = Bytes.readByteArray(input);
            indexedColumns.add(new ColumnReference(cf,cq));
        }
        indexedColumnTypes = Lists.newArrayListWithExpectedSize(nIndexedColumns);
        for (int i = 0; i < nIndexedColumns; i++) {
            PDataType type = PDataType.values()[WritableUtils.readVInt(input)];
            indexedColumnTypes.add(type);
        }
        int nCoveredColumns = WritableUtils.readVInt(input);
        coveredColumns = Sets.newLinkedHashSetWithExpectedSize(nCoveredColumns);
        for (int i = 0; i < nCoveredColumns; i++) {
            byte[] cf = Bytes.readByteArray(input);
            byte[] cq = Bytes.readByteArray(input);
            coveredColumns.add(new ColumnReference(cf,cq));
        }
        indexTableName = Bytes.readByteArray(input);
        dataEmptyKeyValueCF = Bytes.readByteArray(input);
        emptyKeyValueCFPtr = new ImmutableBytesPtr(Bytes.readByteArray(input));
        
        rowKeyMetaData = newRowKeyMetaData();
        rowKeyMetaData.readFields(input);
        int nDataCFs = WritableUtils.readVInt(input);
        // Encode indexWALDisabled in nDataCFs
        indexWALDisabled = nDataCFs < 0;
        this.nDataCFs = Math.abs(nDataCFs) - 1;
        this.estimatedIndexRowKeyBytes = WritableUtils.readVInt(input);
        
        initCachedState();
    }
    
    @Override
    public void write(DataOutput output) throws IOException {
        // Encode nIndexSaltBuckets and isMultiTenant together
        WritableUtils.writeVInt(output, (nIndexSaltBuckets + 1) * (isMultiTenant ? -1 : 1));
        // Encode indexedColumns.size() and whether or not there's a viewIndexId
        WritableUtils.writeVInt(output, (indexedColumns.size() + 1) * (viewIndexId != null ? -1 : 1));
        if (viewIndexId != null) {
            output.write(viewIndexId);
        }
        for (ColumnReference ref : indexedColumns) {
            Bytes.writeByteArray(output, ref.getFamily());
            Bytes.writeByteArray(output, ref.getQualifier());
        }
        for (int i = 0; i < indexedColumnTypes.size(); i++) {
            PDataType type = indexedColumnTypes.get(i);
            WritableUtils.writeVInt(output, type.ordinal());
        }
        WritableUtils.writeVInt(output, coveredColumns.size());
        for (ColumnReference ref : coveredColumns) {
            Bytes.writeByteArray(output, ref.getFamily());
            Bytes.writeByteArray(output, ref.getQualifier());
        }
        Bytes.writeByteArray(output, indexTableName);
        Bytes.writeByteArray(output, dataEmptyKeyValueCF);
        WritableUtils.writeVInt(output,emptyKeyValueCFPtr.getLength());
        output.write(emptyKeyValueCFPtr.get(),emptyKeyValueCFPtr.getOffset(), emptyKeyValueCFPtr.getLength());
        
        rowKeyMetaData.write(output);
        // Encode indexWALDisabled in nDataCFs
        WritableUtils.writeVInt(output, (nDataCFs + 1) * (indexWALDisabled ? -1 : 1));
        WritableUtils.writeVInt(output, estimatedIndexRowKeyBytes);
    }

    public int getEstimatedByteSize() {
        int size = WritableUtils.getVIntSize(nIndexSaltBuckets);
        size += WritableUtils.getVIntSize(estimatedIndexRowKeyBytes);
        size += WritableUtils.getVIntSize(indexedColumns.size());
        size += viewIndexId == null ? 0 : viewIndexId.length;
        for (ColumnReference ref : indexedColumns) {
            size += WritableUtils.getVIntSize(ref.getFamily().length);
            size += ref.getFamily().length;
            size += WritableUtils.getVIntSize(ref.getQualifier().length);
            size += ref.getQualifier().length;
        }
        size += indexedColumnTypes.size();
        size += WritableUtils.getVIntSize(coveredColumns.size());
        for (ColumnReference ref : coveredColumns) {
            size += WritableUtils.getVIntSize(ref.getFamily().length);
            size += ref.getFamily().length;
            size += WritableUtils.getVIntSize(ref.getQualifier().length);
            size += ref.getQualifier().length;
        }
        size += indexTableName.length + WritableUtils.getVIntSize(indexTableName.length);
        size += rowKeyMetaData.getByteSize();
        size += dataEmptyKeyValueCF.length + WritableUtils.getVIntSize(dataEmptyKeyValueCF.length);
        size += emptyKeyValueCFPtr.getLength() + WritableUtils.getVIntSize(emptyKeyValueCFPtr.getLength());
        size += WritableUtils.getVIntSize(nDataCFs+1);
        return size;
    }
    
    private int estimateIndexRowKeyByteSize(int indexColByteSize) {
        int estimatedIndexRowKeyBytes = indexColByteSize + dataRowKeySchema.getEstimatedValueLength() + (nIndexSaltBuckets == 0 || this.isDataTableSalted ? 0 : SaltingUtil.NUM_SALTING_BYTES);
        return estimatedIndexRowKeyBytes;
   }
    
    /**
     * Init calculated state reading/creating
     */
    private void initCachedState() {
        dataEmptyKeyValueRef =
                new ColumnReference(emptyKeyValueCFPtr.copyBytesIfNecessary(),
                        QueryConstants.EMPTY_COLUMN_BYTES);

        indexQualifiers = Lists.newArrayListWithExpectedSize(this.coveredColumns.size());
        for (ColumnReference ref : coveredColumns) {
            indexQualifiers.add(new ImmutableBytesPtr(IndexUtil.getIndexColumnName(
                ref.getFamily(), ref.getQualifier())));
        }

        this.allColumns = Sets.newLinkedHashSetWithExpectedSize(indexedColumns.size() + coveredColumns.size());
        allColumns.addAll(indexedColumns);
        allColumns.addAll(coveredColumns);
        
        int dataPkOffset = (isDataTableSalted ? 1 : 0) + (isMultiTenant ? 1 : 0);
        int nIndexPkColumns = getIndexPkColumnCount();
        dataPkPosition = new int[nIndexPkColumns];
        Arrays.fill(dataPkPosition, -1);
        BitSet viewConstantColumnBitSet = rowKeyMetaData.getViewConstantColumnBitSet();
        for (int i = dataPkOffset; i < dataRowKeySchema.getFieldCount(); i++) {
            if (!viewConstantColumnBitSet.get(i)) {
                int dataPkPosition = rowKeyMetaData.getIndexPkPosition(i-dataPkOffset);
                this.dataPkPosition[dataPkPosition] = i;
            }
        }
        
        // Calculate the max number of trailing nulls that we should get rid of after building the index row key.
        // We only get rid of nulls for variable length types, so we have to be careful to consider the type of the
        // index table, not the data type of the data table
        int indexedColumnTypesPos = indexedColumnTypes.size()-1;
        int indexPkPos = nIndexPkColumns-1;
        while (indexPkPos >= 0) {
            int dataPkPos = dataPkPosition[indexPkPos];
            boolean isDataNullable;
            PDataType dataType;
            if (dataPkPos == -1) {
                isDataNullable = true;
                dataType = indexedColumnTypes.get(indexedColumnTypesPos--);
            } else {
                Field dataField = dataRowKeySchema.getField(dataPkPos);
                dataType = dataField.getDataType();
                isDataNullable = dataField.isNullable();
            }
            PDataType indexDataType = IndexUtil.getIndexColumnDataType(isDataNullable, dataType);
            if (indexDataType.isFixedWidth()) {
                break;
            }
            indexPkPos--;
        }
        maxTrailingNulls = nIndexPkColumns-indexPkPos-1;
    }

    private int getIndexPkColumnCount() {
        return dataRowKeySchema.getFieldCount() + indexedColumns.size() - (isDataTableSalted ? 1 : 0) - (isMultiTenant ? 1 : 0) - (viewIndexId == null ? 0 : 1);
    }
    
    private RowKeyMetaData newRowKeyMetaData() {
        return getIndexPkColumnCount() < 0xFF ? new ByteSizeRowKeyMetaData() : new IntSizedRowKeyMetaData();
    }

    private RowKeyMetaData newRowKeyMetaData(int capacity) {
        return capacity < 0xFF ? new ByteSizeRowKeyMetaData(capacity) : new IntSizedRowKeyMetaData(capacity);
    }

    private static void writeInverted(byte[] buf, int offset, int length, DataOutput output) throws IOException {
        for (int i = offset; i < offset + length; i++) {
            byte b = SortOrder.invert(buf[i]);
            output.write(b);
        }
    }
    
    private abstract class RowKeyMetaData implements Writable {
        private BitSet descIndexColumnBitSet;
        private BitSet viewConstantColumnBitSet;
        
        private RowKeyMetaData() {
        }
        
        private RowKeyMetaData(int nIndexedColumns) {
            descIndexColumnBitSet = BitSet.withCapacity(nIndexedColumns);
            viewConstantColumnBitSet = BitSet.withCapacity(dataRowKeySchema.getMaxFields()); // Size based on number of data PK columns
      }
        
        protected int getByteSize() {
            return BitSet.getByteSize(getIndexPkColumnCount()) * 3 + BitSet.getByteSize(dataRowKeySchema.getMaxFields());
        }
        
        protected abstract int getIndexPkPosition(int dataPkPosition);
        protected abstract int setIndexPkPosition(int dataPkPosition, int indexPkPosition);
        
        @Override
        public void readFields(DataInput input) throws IOException {
            int length = getIndexPkColumnCount();
            descIndexColumnBitSet = BitSet.read(input, length);
            int vclength = dataRowKeySchema.getMaxFields();
            viewConstantColumnBitSet = BitSet.read(input, vclength);
        }
        
        @Override
        public void write(DataOutput output) throws IOException {
            int length = getIndexPkColumnCount();
            BitSet.write(output, descIndexColumnBitSet, length);
            int vclength = dataRowKeySchema.getMaxFields();
            BitSet.write(output, viewConstantColumnBitSet, vclength);
        }

        private BitSet getDescIndexColumnBitSet() {
            return descIndexColumnBitSet;
        }

        private BitSet getViewConstantColumnBitSet() {
            return viewConstantColumnBitSet;
        }
    }
    
    private static int BYTE_OFFSET = 127;
    
    private class ByteSizeRowKeyMetaData extends RowKeyMetaData {
        private byte[] indexPkPosition;
        
        private ByteSizeRowKeyMetaData() {
        }

        private ByteSizeRowKeyMetaData(int nIndexedColumns) {
            super(nIndexedColumns);
            this.indexPkPosition = new byte[nIndexedColumns];
        }
        
        @Override
        protected int getIndexPkPosition(int dataPkPosition) {
            // Use offset for byte so that we can get full range of 0 - 255
            // We use -128 as marker for a non row key index column,
            // that's why our offset if 127 instead of 128
            return this.indexPkPosition[dataPkPosition] + BYTE_OFFSET;
        }

        @Override
        protected int setIndexPkPosition(int dataPkPosition, int indexPkPosition) {
            return this.indexPkPosition[dataPkPosition] = (byte)(indexPkPosition - BYTE_OFFSET);
        }

        @Override
        public void write(DataOutput output) throws IOException {
            super.write(output);
            output.write(indexPkPosition);
        }

        @Override
        protected int getByteSize() {
            return super.getByteSize() + indexPkPosition.length;
        }

        @Override
        public void readFields(DataInput input) throws IOException {
            super.readFields(input);
            this.indexPkPosition = new byte[getIndexPkColumnCount()];
            input.readFully(indexPkPosition);
        }
    }
    
    private class IntSizedRowKeyMetaData extends RowKeyMetaData {
        private int[] indexPkPosition;
        
        private IntSizedRowKeyMetaData() {
        }

        private IntSizedRowKeyMetaData(int nIndexedColumns) {
            super(nIndexedColumns);
            this.indexPkPosition = new int[nIndexedColumns];
        }
        
        @Override
        protected int getIndexPkPosition(int dataPkPosition) {
            return this.indexPkPosition[dataPkPosition];
        }

        @Override
        protected int setIndexPkPosition(int dataPkPosition, int indexPkPosition) {
            return this.indexPkPosition[dataPkPosition] = indexPkPosition;
        }
        
        @Override
        public void write(DataOutput output) throws IOException {
            super.write(output);
            for (int i = 0; i < indexPkPosition.length; i++) {
                output.writeInt(indexPkPosition[i]);
            }
        }

        @Override
        protected int getByteSize() {
            return super.getByteSize() + indexPkPosition.length * Bytes.SIZEOF_INT;
        }

        @Override
        public void readFields(DataInput input) throws IOException {
            super.readFields(input);
            this.indexPkPosition = new int[getIndexPkColumnCount()];
            for (int i = 0; i < indexPkPosition.length; i++) {
                indexPkPosition[i] = input.readInt();
            }
        }
    }

    @Override
    public Iterator<ColumnReference> iterator() {
        return allColumns.iterator();
    }
}
