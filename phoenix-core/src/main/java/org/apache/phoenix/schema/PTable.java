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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.phoenix.query.QueryConstants.ENCODED_CQ_COUNTER_INITIAL_VALUE;
import static org.apache.phoenix.util.EncodedColumnsUtil.isReservedColumnQualifier;

import java.io.DataOutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nullable;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.types.PArrayDataType;
import org.apache.phoenix.schema.types.PArrayDataTypeDecoder;
import org.apache.phoenix.schema.types.PArrayDataTypeEncoder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.util.TrustedByteArrayOutputStream;

import com.google.common.annotations.VisibleForTesting;


/**
 * Definition of a Phoenix table
 *
 *
 * @since 0.1
 */
public interface PTable extends PMetaDataEntity {
    public static final long INITIAL_SEQ_NUM = 0;
    public static final String IS_IMMUTABLE_ROWS_PROP_NAME = "IMMUTABLE_ROWS";
    public static final boolean DEFAULT_DISABLE_WAL = false;

    public enum ViewType {
        MAPPED((byte)1),
        READ_ONLY((byte)2),
        UPDATABLE((byte)3);

        private final byte[] byteValue;
        private final byte serializedValue;

        ViewType(byte serializedValue) {
            this.serializedValue = serializedValue;
            this.byteValue = Bytes.toBytes(this.name());
        }

        public byte[] getBytes() {
            return byteValue;
        }

        public boolean isReadOnly() {
            return this != UPDATABLE;
        }

        public byte getSerializedValue() {
            return this.serializedValue;
        }

        public static ViewType fromSerializedValue(byte serializedValue) {
            if (serializedValue < 1 || serializedValue > ViewType.values().length) {
                throw new IllegalArgumentException("Invalid ViewType " + serializedValue);
            }
            return ViewType.values()[serializedValue-1];
        }

        public ViewType combine(ViewType otherType) {
            if (otherType == null) {
                return this;
            }
            if (this == UPDATABLE && otherType == UPDATABLE) {
                return UPDATABLE;
            }
            return READ_ONLY;
        }
    }

    public enum IndexType {
        GLOBAL((byte)1),
        LOCAL((byte)2);

        private final byte[] byteValue;
        private final byte serializedValue;

        IndexType(byte serializedValue) {
            this.serializedValue = serializedValue;
            this.byteValue = Bytes.toBytes(this.name());
        }

        public byte[] getBytes() {
            return byteValue;
        }

        public byte getSerializedValue() {
            return this.serializedValue;
        }

        public static IndexType getDefault() {
            return GLOBAL;
        }

        public static IndexType fromToken(String token) {
            return IndexType.valueOf(token.trim().toUpperCase());
        }

        public static IndexType fromSerializedValue(byte serializedValue) {
            if (serializedValue < 1 || serializedValue > IndexType.values().length) {
                throw new IllegalArgumentException("Invalid IndexType " + serializedValue);
            }
            return IndexType.values()[serializedValue-1];
        }
    }

    public enum LinkType {
        /**
         * Link from a table to its index table
         */
        INDEX_TABLE((byte)1),
        /**
         * Link from a view to its physical table
         */
        PHYSICAL_TABLE((byte)2),
        /**
         * Link from a view to its parent table
         */
        PARENT_TABLE((byte)3);
        
        private final byte[] byteValue;
        private final byte serializedValue;

        LinkType(byte serializedValue) {
            this.serializedValue = serializedValue;
            this.byteValue = Bytes.toBytes(this.name());
        }

        public byte[] getBytes() {
            return byteValue;
        }

        public byte getSerializedValue() {
            return this.serializedValue;
        }

        public static LinkType fromSerializedValue(byte serializedValue) {
            if (serializedValue < 1 || serializedValue > LinkType.values().length) {
                return null;
            }
            return LinkType.values()[serializedValue-1];
        }
    }
    
    public enum ImmutableStorageScheme implements ColumnValueEncoderDecoderSupplier {
        ONE_CELL_PER_COLUMN((byte)1) {
            @Override
            public ColumnValueEncoder getEncoder(int numElements) {
                throw new UnsupportedOperationException();
            }
            
            @Override
            public ColumnValueDecoder getDecoder() {
                throw new UnsupportedOperationException();
            }
        },
        // stores a single cell per column family that contains all serialized column values
        SINGLE_CELL_ARRAY_WITH_OFFSETS((byte)2) {
            @Override
            public ColumnValueEncoder getEncoder(int numElements) {
                PDataType type = PVarbinary.INSTANCE;
                int estimatedSize = PArrayDataType.estimateSize(numElements, type);
                TrustedByteArrayOutputStream byteStream = new TrustedByteArrayOutputStream(estimatedSize);
                DataOutputStream oStream = new DataOutputStream(byteStream);
                return new PArrayDataTypeEncoder(byteStream, oStream, numElements, type, SortOrder.ASC, false, PArrayDataType.IMMUTABLE_SERIALIZATION_VERSION);
            }
            
            @Override
            public ColumnValueDecoder getDecoder() {
                return new PArrayDataTypeDecoder();
            }
        };

        private final byte serializedValue;
        
        private ImmutableStorageScheme(byte serializedValue) {
            this.serializedValue = serializedValue;
        }

        public byte getSerializedMetadataValue() {
            return this.serializedValue;
        }

        public static ImmutableStorageScheme fromSerializedValue(byte serializedValue) {
            if (serializedValue < 1 || serializedValue > ImmutableStorageScheme.values().length) {
                return null;
            }
            return ImmutableStorageScheme.values()[serializedValue-1];
        }

    }
    
    interface ColumnValueEncoderDecoderSupplier {
        ColumnValueEncoder getEncoder(int numElements);
        ColumnValueDecoder getDecoder();
    }
    
    public enum QualifierEncodingScheme implements QualifierEncoderDecoder {
        NON_ENCODED_QUALIFIERS((byte)0, null) {
            @Override
            public byte[] encode(int value) {
                throw new UnsupportedOperationException();
            }

            @Override
            public int decode(byte[] bytes) {
                throw new UnsupportedOperationException();
            }

            @Override
            public int decode(byte[] bytes, int offset, int length) {
                throw new UnsupportedOperationException();
            }
            
            @Override
            public String toString() {
                return name();
            }
        },
        ONE_BYTE_QUALIFIERS((byte)1, 255) {
            private final int c = Math.abs(Byte.MIN_VALUE);
            
            @Override
            public byte[] encode(int value) {
                if (isReservedColumnQualifier(value)) {
                    return FOUR_BYTE_QUALIFIERS.encode(value);
                }
                if (value < 0 || value > maxQualifier) {
                    throw new QualifierOutOfRangeException(0, maxQualifier);
                }
                return new byte[]{(byte)(value - c)};
            }

            @Override
            public int decode(byte[] bytes) {
                if (bytes.length == 4) {
                    return getReservedQualifier(bytes);
                }
                if (bytes.length != 1) {
                    throw new InvalidQualifierBytesException(1, bytes.length);
                }
                return bytes[0] + c;
            }

            @Override
            public int decode(byte[] bytes, int offset, int length) {
                if (length == 4) {
                    return getReservedQualifier(bytes, offset, length);
                }
                if (length != 1) {
                    throw new InvalidQualifierBytesException(1, length);
                }
                return bytes[offset] + c;
            }
            
            @Override
            public String toString() {
                return name();
            }
        },
        TWO_BYTE_QUALIFIERS((byte)2, 65535) {
            private final int c = Math.abs(Short.MIN_VALUE);
            
            @Override
            public byte[] encode(int value) {
                if (isReservedColumnQualifier(value)) {
                    return FOUR_BYTE_QUALIFIERS.encode(value);
                }
                if (value < 0 || value > maxQualifier) {
                    throw new QualifierOutOfRangeException(0, maxQualifier);
                }
                return Bytes.toBytes((short)(value - c));
            }

            @Override
            public int decode(byte[] bytes) {
                if (bytes.length == 4) {
                    return getReservedQualifier(bytes);
                }
                if (bytes.length != 2) {
                    throw new InvalidQualifierBytesException(2, bytes.length);
                }
                return Bytes.toShort(bytes) + c;
            }

            @Override
            public int decode(byte[] bytes, int offset, int length) {
                if (length == 4) {
                    return getReservedQualifier(bytes, offset, length);
                }
                if (length != 2) {
                    throw new InvalidQualifierBytesException(2, length);
                }
                return Bytes.toShort(bytes, offset, length) + c;
            }
            
            @Override
            public String toString() {
                return name();
            }
        },
        THREE_BYTE_QUALIFIERS((byte)3, 16777215) {
            @Override
            public byte[] encode(int value) {
                if (isReservedColumnQualifier(value)) {
                    return FOUR_BYTE_QUALIFIERS.encode(value);
                }
                if (value < 0 || value > maxQualifier) {
                    throw new QualifierOutOfRangeException(0, maxQualifier);
                }
                byte[] arr = Bytes.toBytes(value);
                return new byte[]{arr[1], arr[2], arr[3]};
            }

            @Override
            public int decode(byte[] bytes) {
                if (bytes.length == 4) {
                    return getReservedQualifier(bytes);
                }
                if (bytes.length != 3) {
                    throw new InvalidQualifierBytesException(2, bytes.length);
                }
                byte[] toReturn = new byte[4];
                toReturn[1] = bytes[0];
                toReturn[2] = bytes[1];
                toReturn[3] = bytes[2];
                return Bytes.toInt(toReturn);
            }

            @Override
            public int decode(byte[] bytes, int offset, int length) {
                if (length == 4) {
                    return getReservedQualifier(bytes, offset, length);
                }
                if (length != 3) {
                    throw new InvalidQualifierBytesException(3, length);
                }
                byte[] toReturn = new byte[4];
                toReturn[1] = bytes[offset];
                toReturn[2] = bytes[offset + 1];
                toReturn[3] = bytes[offset + 2];
                return Bytes.toInt(toReturn);
            }
            
            @Override
            public String toString() {
                return name();
            }
        },
        FOUR_BYTE_QUALIFIERS((byte)4, Integer.MAX_VALUE) {
            @Override
            public byte[] encode(int value) {
                if (value < 0) {
                    throw new QualifierOutOfRangeException(0, maxQualifier);
                }
                return Bytes.toBytes(value);
            }

            @Override
            public int decode(byte[] bytes) {
                if (bytes.length != 4) {
                    throw new InvalidQualifierBytesException(4, bytes.length);
                }
                return Bytes.toInt(bytes);
            }

            @Override
            public int decode(byte[] bytes, int offset, int length) {
                if (length != 4) {
                    throw new InvalidQualifierBytesException(4, length);
                }
                return Bytes.toInt(bytes, offset, length);
            }
            
            @Override
            public String toString() {
                return name();
            }
        };
        
        final byte metadataValue;
        final Integer maxQualifier;
        
        public byte getSerializedMetadataValue() {
            return this.metadataValue;
        }

        public static QualifierEncodingScheme fromSerializedValue(byte serializedValue) {
            if (serializedValue < 0 || serializedValue >= QualifierEncodingScheme.values().length) {
                return null;
            }
            return QualifierEncodingScheme.values()[serializedValue];
        }
        
        @Override
        public Integer getMaxQualifier() {
            return maxQualifier;
        }

        private QualifierEncodingScheme(byte serializedMetadataValue, Integer maxQualifier) {
            this.metadataValue = serializedMetadataValue;
            this.maxQualifier = maxQualifier;
        }
        
        @VisibleForTesting
        public static class QualifierOutOfRangeException extends RuntimeException {
            public QualifierOutOfRangeException(int minQualifier, int maxQualifier) {
                super("Qualifier out of range (" + minQualifier + ", " + maxQualifier + ")"); 
            }
        }
        
        @VisibleForTesting
        public static class InvalidQualifierBytesException extends RuntimeException {
            public InvalidQualifierBytesException(int expectedLength, int actualLength) {
                super("Invalid number of qualifier bytes. Expected length: " + expectedLength + ". Actual: " + actualLength);
            }
        }

        /**
         * We generate our column qualifiers in the reserved range 0-10 using the FOUR_BYTE_QUALIFIERS
         * encoding. When adding Cells corresponding to the reserved qualifiers to the
         * EncodedColumnQualifierCells list, we need to make sure that we use the FOUR_BYTE_QUALIFIERS
         * scheme to decode the correct int value.
         */
        private static int getReservedQualifier(byte[] bytes) {
            checkArgument(bytes.length == 4);
            int number = FOUR_BYTE_QUALIFIERS.decode(bytes);
            if (!isReservedColumnQualifier(number)) {
                throw new InvalidQualifierBytesException(4, bytes.length);
            }
            return number;
        }

        /**
         * We generate our column qualifiers in the reserved range 0-10 using the FOUR_BYTE_QUALIFIERS
         * encoding. When adding Cells corresponding to the reserved qualifiers to the
         * EncodedColumnQualifierCells list, we need to make sure that we use the FOUR_BYTE_QUALIFIERS
         * scheme to decode the correct int value.
         */
        private static int getReservedQualifier(byte[] bytes, int offset, int length) {
            checkArgument(length == 4);
            int number = FOUR_BYTE_QUALIFIERS.decode(bytes, offset, length);
            if (!isReservedColumnQualifier(number)) {
                throw new InvalidQualifierBytesException(4, length);
            }
            return number;
        }
    }
    
    interface QualifierEncoderDecoder {
        byte[] encode(int value);
        int decode(byte[] bytes);
        int decode(byte[] bytes, int offset, int length);
        Integer getMaxQualifier();
    }

    long getTimeStamp();
    long getSequenceNumber();
    long getIndexDisableTimestamp();
    /**
     * @return table name
     */
    PName getName();
    PName getSchemaName();
    PName getTableName();
    PName getTenantId();

    /**
     * @return the table type
     */
    PTableType getType();

    PName getPKName();

    /**
     * Get the PK columns ordered by position.
     * @return a list of the PK columns
     */
    List<PColumn> getPKColumns();

    /**
     * Get all columns ordered by position.
     * @return a list of all columns
     */
    List<PColumn> getColumns();

    /**
     * @return A list of the column families of this table
     *  ordered by position.
     */
    List<PColumnFamily> getColumnFamilies();

    /**
     * Get the column family with the given name
     * @param family the column family name
     * @return the PColumnFamily with the given name
     * @throws ColumnFamilyNotFoundException if the column family cannot be found
     */
    PColumnFamily getColumnFamily(byte[] family) throws ColumnFamilyNotFoundException;

    PColumnFamily getColumnFamily(String family) throws ColumnFamilyNotFoundException;

    /**
     * Get the column with the given string name.
     * @param name the column name
     * @return the PColumn with the given name
     * @throws ColumnNotFoundException if no column with the given name
     * can be found
     * @throws AmbiguousColumnException if multiple columns are found with the given name
     */
    PColumn getColumnForColumnName(String name) throws ColumnNotFoundException, AmbiguousColumnException;
    
    /**
     * Get the column with the given column qualifier.
     * @param column qualifier bytes
     * @return the PColumn with the given column qualifier
     * @throws ColumnNotFoundException if no column with the given column qualifier can be found
     * @throws AmbiguousColumnException if multiple columns are found with the given column qualifier
     */
    PColumn getColumnForColumnQualifier(byte[] cf, byte[] cq) throws ColumnNotFoundException, AmbiguousColumnException; 
    
    /**
     * Get the PK column with the given name.
     * @param name the column name
     * @return the PColumn with the given name
     * @throws ColumnNotFoundException if no PK column with the given name
     * can be found
     * @throws ColumnNotFoundException
     */
    PColumn getPKColumn(String name) throws ColumnNotFoundException;

    /**
     * Creates a new row at the specified timestamp using the key
     * for the PK values (from {@link #newKey(ImmutableBytesWritable, byte[][])}
     * and the optional key values specified using values.
     * @param ts the timestamp that the key value will have when committed
     * @param key the row key of the key value
     * @param hasOnDupKey true if row has an ON DUPLICATE KEY clause and false otherwise.
     * @param values the optional key values
     * @return the new row. Use {@link org.apache.phoenix.schema.PRow#toRowMutations()} to
     * generate the Row to send to the HBase server.
     * @throws ConstraintViolationException if row data violates schema
     * constraint
     */
    PRow newRow(KeyValueBuilder builder, long ts, ImmutableBytesWritable key, boolean hasOnDupKey, byte[]... values);

    /**
     * Creates a new row for the PK values (from {@link #newKey(ImmutableBytesWritable, byte[][])}
     * and the optional key values specified using values. The timestamp of the key value
     * will be set by the HBase server.
     * @param key the row key of the key value
     * @param hasOnDupKey true if row has an ON DUPLICATE KEY clause and false otherwise.
     * @param values the optional key values
     * @return the new row. Use {@link org.apache.phoenix.schema.PRow#toRowMutations()} to
     * generate the row to send to the HBase server.
     * @throws ConstraintViolationException if row data violates schema
     * constraint
     */
    PRow newRow(KeyValueBuilder builder, ImmutableBytesWritable key, boolean hasOnDupKey, byte[]... values);

    /**
     * Formulates a row key using the values provided. The values must be in
     * the same order as {@link #getPKColumns()}.
     * @param key bytes pointer that will be filled in with the row key
     * @param values the PK column values
     * @return the number of values that were used from values to set
     * the row key
     */
    int newKey(ImmutableBytesWritable key, byte[][] values);

    RowKeySchema getRowKeySchema();

    /**
     * Return the number of buckets used by this table for salting. If the table does
     * not use salting, returns null.
     * @return number of buckets used by this table for salting, or null if salting is not used.
     */
    Integer getBucketNum();

    /**
     * Return the list of indexes defined on this table.
     * @return the list of indexes.
     */
    List<PTable> getIndexes();

    /**
     * For a table of index type, return the state of the table.
     * @return the state of the index.
     */
    PIndexState getIndexState();

    /**
     * @return the full name of the parent view for a view or data table for an index table 
     * or null if this is not a view or index table. Also returns null for a view of a data table 
     * (use @getPhysicalName for this case) 
     */
    PName getParentName();
    /**
     * @return the table name of the parent view for a view or data table for an index table 
     * or null if this is not a view or index table. Also returns null for a view of a data table 
     * (use @getPhysicalTableName for this case) 
     */
    PName getParentTableName();
    /**
     * @return the schema name of the parent view for a view or data table for an index table 
     * or null if this is not a view or index table. Also returns null for view of a data table 
     * (use @getPhysicalSchemaName for this case) 
     */
    PName getParentSchemaName();

    /**
     * For a view, return the name of table in Phoenix that physically stores data.
     * Currently a single name, but when views are allowed over multiple tables, will become multi-valued.
     * @return the name of the physical table storing the data.
     */
    public List<PName> getPhysicalNames();

    /**
     * For a view, return the name of table in HBase that physically stores data.
     * @return the name of the physical HBase table storing the data.
     */
    PName getPhysicalName();
    boolean isImmutableRows();

    boolean getIndexMaintainers(ImmutableBytesWritable ptr, PhoenixConnection connection);
    IndexMaintainer getIndexMaintainer(PTable dataTable, PhoenixConnection connection);
    PName getDefaultFamilyName();

    boolean isWALDisabled();
    boolean isMultiTenant();
    boolean getStoreNulls();
    boolean isTransactional();

    ViewType getViewType();
    String getViewStatement();
    Short getViewIndexId();
    PTableKey getKey();

    IndexType getIndexType();
    int getBaseColumnCount();

    /**
     * Determines whether or not we may optimize out an ORDER BY or do a GROUP BY
     * in-place when the optimizer tells us it's possible. This is due to PHOENIX-2067
     * and only applicable for tables using DESC primary key column(s) which have
     * not been upgraded.
     * @return true if optimizations row key order optimizations are possible
     */
    boolean rowKeyOrderOptimizable();
    
    /**
     * @return Position of the column with {@link PColumn#isRowTimestamp()} as true. 
     * -1 if there is no such column.
     */
    int getRowTimestampColPos();
    long getUpdateCacheFrequency();
    boolean isNamespaceMapped();
    
    /**
     * @return The sequence name used to get the unique identifier for views
     * that are automatically partitioned.
     */
    String getAutoPartitionSeqName();
    
    /**
     * @return true if the you can only add (and never delete) columns to the table,
     * you are also not allowed to delete the table  
     */
    boolean isAppendOnlySchema();
    ImmutableStorageScheme getImmutableStorageScheme();
    QualifierEncodingScheme getEncodingScheme();
    EncodedCQCounter getEncodedCQCounter();
    
    /**
     * Class to help track encoded column qualifier counters per column family.
     */
    public class EncodedCQCounter {
        
        private final Map<String, Integer> familyCounters = new HashMap<>();
        
        /**
         * Copy constructor
         * @param counterToCopy
         * @return copy of the passed counter
         */
        public static EncodedCQCounter copy(EncodedCQCounter counterToCopy) {
            EncodedCQCounter cqCounter = new EncodedCQCounter();
            for (Entry<String, Integer> e : counterToCopy.values().entrySet()) {
                cqCounter.setValue(e.getKey(), e.getValue());
            }
            return cqCounter;
        }
        
        public static final EncodedCQCounter NULL_COUNTER = new EncodedCQCounter() {

            @Override
            public Integer getNextQualifier(String columnFamily) {
                return null;
            }

            @Override
            public void setValue(String columnFamily, Integer value) {
            }

            @Override
            public boolean increment(String columnFamily) {
                return false;
            }

            @Override
            public Map<String, Integer> values() {
                return Collections.emptyMap();
            }

        };
        
        /**
         * Get the next qualifier to be used for the column family.
         * This method also ends up initializing the counter if the
         * column family already doesn't have one.
         */
        @Nullable
        public Integer getNextQualifier(String columnFamily) {
            Integer counter = familyCounters.get(columnFamily);
            if (counter == null) {
                counter = ENCODED_CQ_COUNTER_INITIAL_VALUE;
                familyCounters.put(columnFamily, counter);
            }
            return counter;
        }
        
        public void setValue(String columnFamily, Integer value) {
            familyCounters.put(columnFamily, value);
        }
        
        /**
         * 
         * @param columnFamily
         * @return true if the counter was incremented, false otherwise.
         */
        public boolean increment(String columnFamily) {
            if (columnFamily == null) {
                return false;
            }
            Integer counter = familyCounters.get(columnFamily);
            if (counter == null) {
                counter = ENCODED_CQ_COUNTER_INITIAL_VALUE;
            }
            counter++;
            familyCounters.put(columnFamily, counter);
            return true;
        }
        
        public Map<String, Integer> values()  {
            return Collections.unmodifiableMap(familyCounters);
        }
        
    }

}
