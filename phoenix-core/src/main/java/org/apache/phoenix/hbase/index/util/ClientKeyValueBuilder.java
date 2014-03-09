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
package org.apache.phoenix.hbase.index.util;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;

/**
 * A {@link KeyValueBuilder} that builds {@link ClientKeyValue}, eliminating the extra byte copies inherent in the
 * standard {@link KeyValue} implementation.
 * <p>
 * This {@link KeyValueBuilder} is only supported in HBase 0.94.14+ with the addition of HBASE-9834.
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
    public int compareQualifier(Cell kv, byte[] key, int offset, int length) {
        return Bytes.compareTo(kv.getQualifierArray(), kv.getQualifierOffset(), 
          kv.getQualifierLength(), key, offset, length);
    }

    @Override
    public void getValueAsPtr(Cell kv, ImmutableBytesWritable ptr) {
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
        
        public int compareTypes(final Cell left, final Cell right) {
            return left.getTypeByte() - right.getTypeByte();
        }

        public int compareMemstoreTimestamps(final Cell left, final Cell right) {
            // Descending order
            return Longs.compare(right.getMvccVersion(), left.getMvccVersion());
        }

        public int compareTimestamps(final long ltimestamp, final long rtimestamp) {
          if (ltimestamp < rtimestamp) {
            return 1;
          } else if (ltimestamp > rtimestamp) {
            return -1;
          }
          return 0;
        }
        
        @Override
        public int compareTimestamps(final KeyValue left, final KeyValue right) {
            // Descending order
            return Longs.compare(right.getTimestamp(), left.getTimestamp());
        }


        @Override
        public int compare(final Cell left, final Cell right) {
            int c = compareRows(left.getRowArray(), left.getRowOffset(), left.getRowLength(),
              right.getRowArray(), right.getRowOffset(), right.getRowLength());
            if (c != 0) return c;
            c = Bytes.compareTo(left.getFamilyArray(), left.getFamilyOffset(), left.getFamilyLength(),
              right.getFamilyArray(), right.getFamilyOffset(), right.getFamilyLength());
            if (c != 0) return c;
            c = Bytes.compareTo(left.getQualifierArray(), left.getQualifierOffset(),
              left.getQualifierLength(), right.getQualifierArray(), right.getQualifierOffset(),
              right.getQualifierLength());
            c = compareTimestamps(left.getTimestamp(), right.getTimestamp());
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
            return Bytes.compareTo(left.getRowArray(), left.getRowOffset(), left.getRowLength(),
              right.getRowArray(), right.getRowOffset(), right.getRowLength());
        }

        /**
         * @param left
         * @param lrowlength Length of left row.
         * @param right
         * @param rrowlength Length of right row.
         * @return Result comparing rows.
         */
        @Override
        public int compareRows(byte [] left, int loffset, int llength,
            byte [] right, int roffset, int rlength) {
            return Bytes.compareTo(left, loffset, llength, 
              right, roffset, rlength);
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
            if (Bytes.compareTo(left.getFamilyArray(), left.getFamilyOffset(), 
              left.getFamilyLength(), right.getFamilyArray(), right.getFamilyOffset(), 
              right.getFamilyLength()) != 0) {
                return false;
            }
            if (Bytes.compareTo(left.getQualifierArray(), left.getQualifierOffset(),
              left.getQualifierLength(), right.getQualifierArray(), right.getQualifierOffset(), 
              right.getQualifierLength()) != 0) {
                return false;
            }
            return true;
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

        @Override
        protected Object clone() throws CloneNotSupportedException {
          return this; // Makes no sense to clone this
        }
    }

    private static final KVComparator COMPARATOR = new ClientKeyValueComparator();
    
    @Override
    public KVComparator getKeyValueComparator() {
        return COMPARATOR;
    }

    @Override
    public int compareRow(Cell kv, byte[] rrow, int roffset, int rlength) {
        return Bytes.compareTo(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), 
          rrow, roffset, rlength);
    }

    @Override
    public int compareFamily(Cell kv, byte[] rbuf, int roffset, int rlength) {
        return Bytes.compareTo(kv.getFamilyArray(), kv.getFamilyOffset(), 
          kv.getFamilyLength(), rbuf, roffset, rlength);
    }

    /**
     * Copy the mutations to that they have a real KeyValue as the memstore
     * requires this.
     */
    @Override
    public List<Mutation> cloneIfNecessary(List<Mutation> mutations) {
        boolean cloneNecessary = false;
        for (Mutation mutation : mutations) {
            if (mutation instanceof Put) {
                cloneNecessary = true;
                break;
            }
        }
        if (!cloneNecessary) {
            return mutations;
        }
        List<Mutation> newMutations = Lists.newArrayListWithExpectedSize(mutations.size());
        for (Mutation mutation : mutations) {
            if (mutation instanceof Put) {
                Put put = (Put)mutation;
                Put newPut = new Put(put.getRow(),put.getTimeStamp());
                Map<byte[],List<Cell>> newFamilyMap = newPut.getFamilyCellMap();
                for (Map.Entry<byte[], List<Cell>> entry : put.getFamilyCellMap().entrySet()) {
                    List<Cell> values = entry.getValue();
                    List<Cell> newValues = Lists.newArrayListWithExpectedSize(values.size());
                    for (Cell value : values) {
                        newValues.add(new KeyValue(value.getRowArray(), value.getRowOffset(),
                          value.getRowLength(), value.getFamilyArray(), value.getFamilyOffset(),
                          value.getFamilyLength(), value.getQualifierArray(), value.getQualifierOffset(),
                          value.getQualifierLength(),value.getTimestamp(), Type.codeToType(value.getTypeByte()),
                          value.getValueArray(), value.getValueOffset(), value.getValueLength()));
                    }
                    newFamilyMap.put(entry.getKey(), newValues);
                }
                newMutations.add(newPut);
            } else {
                newMutations.add(mutation);
            }
        }
        return newMutations;
    }
}