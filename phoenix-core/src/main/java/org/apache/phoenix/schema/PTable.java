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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.stats.PTableStats;


/**
 * Definition of a Phoenix table
 *
 *
 * @since 0.1
 */
public interface PTable {
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

    long getTimeStamp();
    long getSequenceNumber();
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
    PColumn getColumn(String name) throws ColumnNotFoundException, AmbiguousColumnException;
    
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
     * @param values the optional key values
     * @return the new row. Use {@link org.apache.phoenix.schema.PRow#toRowMutations()} to
     * generate the Row to send to the HBase server.
     * @throws ConstraintViolationException if row data violates schema
     * constraint
     */
    PRow newRow(KeyValueBuilder builder, long ts, ImmutableBytesWritable key, byte[]... values);

    /**
     * Creates a new row for the PK values (from {@link #newKey(ImmutableBytesWritable, byte[][])}
     * and the optional key values specified using values. The timestamp of the key value
     * will be set by the HBase server.
     * @param key the row key of the key value
     * @param values the optional key values
     * @return the new row. Use {@link org.apache.phoenix.schema.PRow#toRowMutations()} to
     * generate the row to send to the HBase server.
     * @throws ConstraintViolationException if row data violates schema
     * constraint
     */
    PRow newRow(KeyValueBuilder builder, ImmutableBytesWritable key, byte[]... values);

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
     * Gets the full name of the data table for an index table.
     * @return the name of the data table that this index is on
     *  or null if not an index.
     */
    PName getParentName();
    /**
     * Gets the table name of the data table for an index table.
     * @return the table name of the data table that this index is
     * on or null if not an index.
     */
    PName getParentTableName();
    /**
     * Gets the schema name of the data table for an index table.
     * @return the schema name of the data table that this index is
     * on or null if not an index.
     */
    PName getParentSchemaName();

    /**
     * For a view, return the name of table in Phoenix that physically stores data.
     * Currently a single name, but when views are allowed over multiple tables, will become multi-valued.
     * @return the name of the physical table storing the data.
     */
    public List<PName> getPhysicalNames();

    PName getPhysicalName();
    boolean isImmutableRows();

    void getIndexMaintainers(ImmutableBytesWritable ptr, PhoenixConnection connection);
    IndexMaintainer getIndexMaintainer(PTable dataTable, PhoenixConnection connection);
    PName getDefaultFamilyName();

    boolean isWALDisabled();
    boolean isMultiTenant();
    boolean getStoreNulls();

    ViewType getViewType();
    String getViewStatement();
    Short getViewIndexId();
    PTableKey getKey();

    int getEstimatedSize();
    IndexType getIndexType();
    PTableStats getTableStats();
}
