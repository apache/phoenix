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
package org.apache.phoenix.client;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;

import com.google.common.primitives.Longs;

/**
 * A {@link KeyValueBuilder} that builds {@link ClientKeyValue}, eliminating the extra byte copies inherent in the
 * standard {@link KeyValue} implementation.
 * <p>
 * This {@link KeyValueBuilder} is only supported in HBase 0.94.14+ (
 * {@link PhoenixDatabaseMetaData#CLIENT_KEY_VALUE_BUILDER_THRESHOLD}), with the addition of HBASE-9834.
 */
public class ClientKeyValueBuilder extends KeyValueBuilder {

    public static final KeyValueBuilder INSTANCE = new ClientKeyValueBuilder();

    private ClientKeyValueBuilder() {
        // private ctor for singleton
    }

    @Override
    public KeyValue buildPut(ImmutableBytesWritable row, ImmutableBytesWritable family,
            ImmutableBytesWritable qualifier, long ts, ImmutableBytesWritable value) {
        return new ClientKeyValue(row, family, qualifier, ts, Type.Put, value);
    }

    @Override
    public KeyValue buildDeleteFamily(ImmutableBytesWritable row, ImmutableBytesWritable family,
            ImmutableBytesWritable qualifier, long ts) {
        return new ClientKeyValue(row, family, qualifier, ts, Type.DeleteFamily, null);
    }

    @Override
    public KeyValue buildDeleteColumns(ImmutableBytesWritable row, ImmutableBytesWritable family,
            ImmutableBytesWritable qualifier, long ts) {
        return new ClientKeyValue(row, family, qualifier, ts, Type.DeleteColumn, null);
    }

    @Override
    public KeyValue buildDeleteColumn(ImmutableBytesWritable row, ImmutableBytesWritable family,
            ImmutableBytesWritable qualifier, long ts) {
        return new ClientKeyValue(row, family, qualifier, ts, Type.Delete, null);
    }

    @Override
    public int compareQualifier(KeyValue kv, byte[] key, int offset, int length) {
        byte[] qual = kv.getQualifier();
        return Bytes.compareTo(qual, 0, qual.length, key, offset, length);
    }

    @Override
    public void getValueAsPtr(KeyValue kv, ImmutableBytesWritable ptr) {
        ClientKeyValue ckv = (ClientKeyValue)kv;
        ImmutableBytesWritable value = ckv.getRawValue();
        ptr.set(value.get(), value.getOffset(), value.getLength());
    }

    /**
     * 
     * Singleton ClientKeyValue version of KVComparator that doesn't assume we
     * have a backing buffer for the key value
     *
     */
    private static class ClientKeyValueComparator extends KVComparator {
        private ClientKeyValueComparator() { // Singleton
        }
        
        public int compareTypes(final KeyValue left, final KeyValue right) {
            return left.getType() - right.getType();
        }

        public int compareMemstoreTimestamps(final KeyValue left, final KeyValue right) {
            // Descending order
            return Longs.compare(right.getMemstoreTS(), left.getMemstoreTS());
        }

        @Override
        public int compareTimestamps(final KeyValue left, final KeyValue right) {
            // Descending order
            return Longs.compare(right.getTimestamp(), left.getTimestamp());
        }


        @Override
        public int compare(final KeyValue left, final KeyValue right) {
            int c = compareRows(left, right);
            if (c != 0) return c;
            c = Bytes.compareTo(left.getFamily(), right.getFamily());
            if (c != 0) return c;
            c = Bytes.compareTo(left.getQualifier(), right.getQualifier());
            c = compareTimestamps(left, right);
            if (c != 0) return c;
            c = compareTypes(left, right);
            if (c != 0) return c;            
            
            return compareMemstoreTimestamps(left, right);
        }

        /**
         * @param left
         * @param right
         * @return Result comparing rows.
         */
        @Override
        public int compareRows(final KeyValue left, final KeyValue right) {
            return Bytes.compareTo(left.getRow(), right.getRow());
        }

        /**
         * @param left
         * @param lrowlength Length of left row.
         * @param right
         * @param rrowlength Length of right row.
         * @return Result comparing rows.
         */
        @Override
        public int compareRows(final KeyValue left, final short lrowlength,
            final KeyValue right, final short rrowlength) {
            return Bytes.compareTo(left.getRow(), 0, lrowlength, right.getRow(), 0, rrowlength);
        }

        /**
         * @param left
         * @param row - row key (arbitrary byte array)
         * @return RawComparator
         */
        @Override
        public int compareRows(final KeyValue left, final byte [] row) {
            return Bytes.compareTo(left.getRow(), row);
        }

        @Override
        public int compareColumns(final KeyValue left, 
                final byte [] right, final int roffset, final int rlength, final int rfamilyLength) {
          // Compare family portion first.
          byte[] lcf = left.getFamily();
          int diff = Bytes.compareTo(lcf, 0, lcf.length,
            right, roffset, rfamilyLength);
          if (diff != 0) {
            return diff;
          }
          // Compare qualifier portion
          byte[] lcq = left.getQualifier();
          return Bytes.compareTo(lcq, 0, lcq.length,
            right, roffset + rfamilyLength, rlength - rfamilyLength);

        }

        /**
         * Compares the row and column of two keyvalues for equality
         * @param left
         * @param right
         * @return True if same row and column.
         */
        @Override
        public boolean matchingRowColumn(final KeyValue left, final KeyValue right) {
            if (compareRows(left, right) != 0) {
                return false;
            }
            if (Bytes.compareTo(left.getFamily(), right.getFamily()) != 0) {
                return false;
            }
            if (Bytes.compareTo(left.getQualifier(), right.getQualifier()) != 0) {
                return false;
            }
            return true;
        }

        /**
         * @param left
         * @param right
         * @return True if rows match.
         */
        @Override
        public boolean matchingRows(final KeyValue left, final byte [] right) {
          return Bytes.equals(left.getRow(), right);
        }

        /**
         * Compares the row of two keyvalues for equality
         * @param left
         * @param right
         * @return True if rows match.
         */
        @Override
        public boolean matchingRows(final KeyValue left, final KeyValue right) {
            return compareRows(left, right) == 0;
        }

        /**
         * @param left
         * @param lrowlength
         * @param right
         * @param rrowlength
         * @return True if rows match.
         */
        @Override
        public boolean matchingRows(final KeyValue left, final short lrowlength,
            final KeyValue right, final short rrowlength) {
            return compareRows(left, lrowlength, right, rrowlength) == 0;
        }

        @Override
        protected Object clone() throws CloneNotSupportedException {
          return this; // Makes no sense to clone this
        }

        /**
         * @return Comparator that ignores timestamps; useful counting versions.
         */
        @Override
        public KVComparator getComparatorIgnoringTimestamps() {
            return IGNORE_TIMESTAMP_COMPARATOR;
        }

        /**
         * @return Comparator that ignores key type; useful checking deletes
         */
        @Override
        public KVComparator getComparatorIgnoringType() {
            return IGNORE_TYPE_COMPARATOR;
        }
    }
    
    /**
     * 
     * Singleton ClientKeyValue version of KVComparator that ignores differences in the Type
     *
     */
    private static class IgnoreTypeClientKeyValueComparator extends ClientKeyValueComparator {
        private IgnoreTypeClientKeyValueComparator() { // Singleton
        }
        
        @Override
        public int compareTypes(final KeyValue left, final KeyValue right) {
            return 0;
        }
        
        @Override
        public KVComparator getComparatorIgnoringTimestamps() {
            return IGNORE_TIMESTAMP_AND_TYPE_COMPARATOR;
        }
    }


    /**
     * 
     * Singleton ClientKeyValue version of KVComparator that ignores differences in the Timestamp
     *
     */
    private static class IgnoreTimestampClientKeyValueComparator extends ClientKeyValueComparator {
        private IgnoreTimestampClientKeyValueComparator() { // Singleton
        }
        
        @Override
        public int compareMemstoreTimestamps(final KeyValue left, final KeyValue right) {
            return 0;
        }

        @Override
        public int compareTimestamps(final KeyValue left, final KeyValue right) {
            return 0;
        }
        
        @Override
        public KVComparator getComparatorIgnoringType() {
            return IGNORE_TYPE_COMPARATOR;
        }
    }
    
    /**
     * 
     * Singleton ClientKeyValue version of KVComparator that ignores differences in the Timestamp and Type
     *
     */
    private static final class IgnoreTimestampAndTypeClientKeyValueComparator extends IgnoreTimestampClientKeyValueComparator {
        private IgnoreTimestampAndTypeClientKeyValueComparator() { // Singleton
        }
        
        @Override
        public int compareTypes(final KeyValue left, final KeyValue right) {
            return 0;
        }

        @Override
        public KVComparator getComparatorIgnoringTimestamps() {
            return this;
        }

        @Override
        public KVComparator getComparatorIgnoringType() {
            return this;
        }
    }

    private static final KVComparator COMPARATOR = new ClientKeyValueComparator();
    private static final KVComparator IGNORE_TYPE_COMPARATOR = new IgnoreTypeClientKeyValueComparator();
    private static final KVComparator IGNORE_TIMESTAMP_COMPARATOR = new IgnoreTimestampClientKeyValueComparator();
    private static final KVComparator IGNORE_TIMESTAMP_AND_TYPE_COMPARATOR = new IgnoreTimestampAndTypeClientKeyValueComparator();
    
    @Override
    public KVComparator getKeyValueComparator() {
        return COMPARATOR;
    }

    @Override
    public int compareRow(KeyValue kv, byte[] rrow, int roffset, int rlength) {
        byte[] lrow = kv.getRow();
        return Bytes.compareTo(lrow, 0, lrow.length, rrow, roffset, rlength);
    }

    @Override
    public int compareFamily(KeyValue kv, byte[] rbuf, int roffset, int rlength) {
        byte[] lcf = kv.getFamily();
        return Bytes.compareTo(lcf, 0, lcf.length, rbuf, roffset, rlength);
    }
}