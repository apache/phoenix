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
package org.apache.phoenix.util;

import static java.util.Objects.requireNonNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.CellBuilder;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.execute.MutationState.MultiRowMutationState;
import org.apache.phoenix.execute.MutationState.RowMutationState;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.tuple.Tuple;

/**
 * 
 * Utilities for KeyValue. Where there's duplication with KeyValue methods,
 * these avoid creating new objects when not necessary (primary preventing
 * byte array copying).
 *
 * 
 * @since 0.1
 */
public class PhoenixKeyValueUtil {
    private PhoenixKeyValueUtil() {
    }

    /**
     * Use {@link CellBuilder} instead.
     */
    @Deprecated
    public static Cell newKeyValue(byte[] key, byte[] cf, byte[] cq, long ts, byte[] value, int valueOffset, int valueLength) {
        return CellBuilderFactory.create(CellBuilderType.DEEP_COPY).setRow(key).setFamily(cf)
                .setQualifier(cq).setTimestamp(ts).setType(Type.Put)
                .setValue(value, valueOffset, valueLength).build();
    }

    /**
     * Use {@link CellBuilder} instead.
     */
    @Deprecated
    public static Cell newKeyValue(ImmutableBytesWritable key, byte[] cf, byte[] cq, long ts, byte[] value, int valueOffset, int valueLength) {
        return CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
                .setRow(key.get(), key.getOffset(), key.getLength()).setFamily(cf).setQualifier(cq)
                .setTimestamp(ts).setType(Type.Put).setValue(value, valueOffset, valueLength)
                .build();
    }

    /**
     * Use {@link CellBuilder} instead.
     */
    @Deprecated
    public static Cell newKeyValue(byte[] key, int keyOffset, int keyLength, byte[] cf, byte[] cq, long ts, byte[] value, int valueOffset, int valueLength) {
        return CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
                .setRow(key, keyOffset, keyLength).setFamily(cf).setQualifier(cq).setTimestamp(ts)
                .setType(Type.Put).setValue(value, valueOffset, valueLength).build();
    }

    /**
     * Use {@link CellBuilder} instead.
     */
    @Deprecated
    public static Cell newKeyValue(byte[] key, int keyOffset, int keyLength, byte[] cf, 
        int cfOffset, int cfLength, byte[] cq, int cqOffset, int cqLength, long ts, byte[] value, 
        int valueOffset, int valueLength,Type type) {
        return CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
                .setRow(key, keyOffset, keyLength).setFamily(cf, cfOffset, cfLength)
                .setQualifier(cq, cqOffset, cqLength).setTimestamp(ts)
                .setValue(value, valueOffset, valueLength).setType(type).build();
    }

    public static CellBuilder copyCell(Cell c) {
      return CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
              .setRow(c.getRowArray(), c.getRowOffset(), c.getRowLength())
              .setFamily(c.getFamilyArray(), c.getFamilyOffset(), c.getFamilyLength())
              .setQualifier(c.getQualifierArray(),c.getQualifierOffset(), c.getQualifierLength())
              .setTimestamp(c.getTimestamp())
              .setType(c.getType())
              .setValue(c.getValueArray(), c.getValueOffset(), c.getValueLength());
    }

    /**
     * Copies a Cell, but does not include the sequence ID.
     */
    public static Cell newKeyValue(Cell c) {
        return copyCell(c).build();
    }

    public static Cell copyCellWithTimestamp(Cell c, long timestamp) {
      return copyCell(c).setTimestamp(timestamp).build();
        
    }

    /**
     * Use {@link CellBuilder} instead.
     */
    @Deprecated
  	public static Cell newKeyValue(byte[] key, byte[] cf, byte[] cq, long ts, byte[] value) {
  		  return newKeyValue(key, cf, cq, ts, value, 0, value.length);
  	}

    /**
     * Binary search for latest column value without allocating memory in the process
     * @param kvBuilder TODO
     * @param kvs
     * @param family
     * @param qualifier
     */
    public static Cell getColumnLatest(KeyValueBuilder kvBuilder, List<Cell>kvs, byte[] family, byte[] qualifier) {
        if (kvs.size() == 0) {
        	return null;
        }
        assert CellUtil.matchingRows(kvs.get(0), kvs.get(kvs.size()-1));

        Comparator<Cell> comp = new SearchComparator(kvBuilder, family, qualifier);
        int pos = Collections.binarySearch(kvs, null, comp);
        if (pos < 0 || pos == kvs.size()) {
          return null; // doesn't exist
        }
    
        return kvs.get(pos);
    }


    /**
     * Binary search for latest column value without allocating memory in the process
     * @param kvBuilder TODO
     * @param kvs
     * @param family
     * @param qualifier
     */
    public static Cell getColumnLatest(KeyValueBuilder kvBuilder, Cell[] kvs, byte[] family, byte[] qualifier) {
        if (kvs.length == 0) {
            return null;
        }
        assert CellUtil.matchingRows(kvs[0], kvs[kvs.length-1]);

        Comparator<Cell> comp = new SearchComparator(kvBuilder, family, qualifier);
        int pos = Arrays.binarySearch(kvs, null, comp);
        if (pos < 0 || pos == kvs.length) {
          return null; // doesn't exist
        }
    
        return kvs[pos];
    }

    /*
     * Special comparator, *only* works for binary search.
     *
     * We make the following assumption:
     * 1. All KVs compared have the same row key
     * 2. For each (rowkey, family, qualifier) there is at most one version
     * 3. Current JDKs only uses the search term on the right side
     *
     * #1 allows us to avoid row key comparisons altogether.
     * #2 allows for exact matches
     * #3 lets us save instanceof checks, and allows to inline the search term in the comparator
     */
	private static class SearchComparator implements Comparator<Cell> {
	  private final KeyValueBuilder kvBuilder;
    private final byte[] family;
    private final byte[] qualifier;

    public SearchComparator(KeyValueBuilder kvBuilder, byte[] f, byte[] q) {
      this.kvBuilder = kvBuilder;
      family = f;
      qualifier = q;
    }

    @Override
    public int compare(final Cell l, final Cell ignored) {
			assert ignored == null;
			// family
			int val = kvBuilder.compareFamily(l, family, 0, family.length);
			if (val != 0) {
				return val;
			}
			// qualifier
			return kvBuilder.compareQualifier(l, qualifier, 0, qualifier.length);
		}
	}

	/**
	 * Calculate the size a mutation will likely take when stored in HBase
	 * @param m The Mutation
	 * @return the disk size of the passed mutation
	 */
    public static long calculateMutationDiskSize(Mutation m) {
        long size = 0;
        for (Entry<byte [], List<Cell>> entry : m.getFamilyCellMap().entrySet()) {
            for (Cell c : entry.getValue()) {
                size += org.apache.hadoop.hbase.KeyValueUtil.length(c);
            }
        }
        return size;
    }

    /**
     * Estimates the storage size of a row
     * @param mutations map from table to row to RowMutationState
     * @return estimated row size
     */
    public static long getEstimatedRowMutationSize(
            Map<TableRef, MultiRowMutationState> tableMutationMap) {
        long size = 0;
        // iterate over table
        for (Entry<TableRef, MultiRowMutationState> tableEntry : tableMutationMap.entrySet()) {
            // iterate over rows
            for (Entry<ImmutableBytesPtr, RowMutationState> rowEntry : tableEntry.getValue().entrySet()) {
                size += calculateRowMutationSize(rowEntry);
            }
        }
        return size;
    }

//    public static Cell copyCell(Cell c) {
//        if (c instanceof KeyValue) {
//            try {
//                return ((KeyValue) c).clone();
//            } catch (CloneNotSupportedException e) {
//                throw new RuntimeException(e);
//            }
//        } else if (c instanceof IndexedCell) {
//            return new IndexedCell((IndexedCell) c);
//        }
//        throw new IllegalArgumentException("Cannot copy Cell implementation: " + c.getClass());
//    }
//
    public static Cell maybeCopyCell(Cell c) {
        // Same as KeyValueUtil, but HBase has deprecated this method. Avoid depending on something
        // that will likely be removed at some point in time.
        if (c == null) return null;
        return c;
//        if (c instanceof IndexedCell) {
//            return c;
//        }
//        if (c instanceof KeyValue) {
//            return (KeyValue) c;
//        }
//        return newKeyValue(c);
    }

    private static long calculateRowMutationSize(Entry<ImmutableBytesPtr, RowMutationState> rowEntry) {
        int rowLength = rowEntry.getKey().getLength();
        long colValuesLength = rowEntry.getValue().calculateEstimatedSize();
        return (rowLength + colValuesLength);
    }

    public static Mutation setTimestamp(Mutation m, long timestamp) throws IOException {
        if (m instanceof Put) {
            return setTimestamp((Put)m, timestamp);
        }
        if (m instanceof Delete) {
            return setTimestamp((Delete)m, timestamp);
        }
        throw new IllegalArgumentException("Cannot handle Mutation: " + m.getClass());
    }

    public static Put setTimestamp(Put p, long timestamp) throws IOException {
        Put copy = new Put(p.getRow());
        for (CellScanner cs = p.cellScanner(); cs.advance();) {
            Cell c = cs.current();
            copy.add(copyCellWithTimestamp(c, timestamp));
        }
        return copy;
    }

    public static Delete setTimestamp(Delete d, long timestamp) throws IOException {
        Delete copy = new Delete(d);
        for (CellScanner cs = d.cellScanner(); cs.advance();) {
            Cell c = cs.current();
            copy.add(copyCellWithTimestamp(c, timestamp));
        }
        return copy;
    }

    public static Type getTypeFromByte(byte b) {
        for (Type type : Type.values()) {
            if (type.getCode() == b) {
                return type;
            }
        }
        throw new IllegalArgumentException("Could not find Type for id: " + b + " " +
                    Bytes.toStringBinary(new byte[] {b}));
    }

    /**
     * Returns the total serialized size as used by {@link #serialize(DataOutputStream, Cell)).
     */
    public static int getSerializedSize(Cell c) {
        return getSerializedDataSize(c) +
                // Timestamp
                Bytes.SIZEOF_LONG +
                // Type
                Bytes.SIZEOF_BYTE;
    }

    public static long getSerializedSize(List<Cell> cells) {
        // The number of cells
        long size = Bytes.SIZEOF_INT;
        for (Cell c : cells) {
            // The size of each cell
            size += PhoenixKeyValueUtil.getSerializedSize(c);
        }
        return size;
    }

    /**
     * Returns the size of the key and value portions of the data. Separated from
     * {@link #getSerializedSize(Cell)) to enable fewer calls to the InputStream in
     * {@link #deserialize(DataInputStream)). You likely do not want this method.
     */
    static int getSerializedDataSize(Cell c) {
        requireNonNull(c, "Cell was null");
        return Bytes.SIZEOF_SHORT + c.getRowLength() +
            Bytes.SIZEOF_BYTE + c.getFamilyLength() +
            Bytes.SIZEOF_INT + c.getQualifierLength() +
            Bytes.SIZEOF_INT + c.getValueLength();
    }

    public static void serializeCell(DataOutput os, Cell c) throws IOException {
        requireNonNull(os, "Output stream was null");
        requireNonNull(c, "Cell was null");
        os.writeShort(c.getRowLength());
        os.write(c.getRowArray(), c.getRowOffset(), c.getRowLength());
        os.write(c.getFamilyLength());
        os.write(c.getFamilyArray(), c.getFamilyOffset(), c.getFamilyLength());
        os.writeInt(c.getQualifierLength());
        os.write(c.getQualifierArray(), c.getQualifierOffset(), c.getQualifierLength());
        os.writeInt(c.getValueLength());
        os.write(c.getValueArray(), c.getValueOffset(), c.getValueLength());
        os.writeLong(c.getTimestamp());
        os.write(c.getType().getCode());
    }

    public static Cell deserializeCell(DataInput is) throws IOException {
        requireNonNull(is, "Input stream was null");
        CellBuilder cb = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY);

        short rowLength = is.readShort();
        byte[] row = new byte[rowLength];
        is.readFully(row);
        cb.setRow(row);

        byte famLength = is.readByte();
        byte[] family = new byte[famLength];
        is.readFully(family);
        cb.setFamily(family);
        
        int qLength = is.readInt();
        byte[] qualifier = new byte[qLength];
        is.readFully(qualifier);
        cb.setQualifier(qualifier);

        int vLength = is.readInt();
        byte[] value = new byte[vLength];
        is.readFully(value);
        cb.setValue(value);

        cb.setTimestamp(is.readLong());
        cb.setType(getTypeFromByte(is.readByte()));

        return cb.build();
    }

    public static int getSerializedResultSize(Tuple t) {
        requireNonNull(t, "Tuple was null");
        // We store the number of cells we're writing.
        int size = Bytes.SIZEOF_INT;
        // Add in the size of the cells we're serializing
        for (int i = 0; i < t.size(); i++) {
            size += getSerializedSize(t.getValue(i));
        }
        return size;
    }

    public static void serializeResult(DataOutput os, Tuple t) throws IOException {
        requireNonNull(t, "Tuple was null");
        os.writeInt(t.size());
        for (int i = 0; i < t.size(); i++) {
          serializeCell(os, t.getValue(i));
        }
    }

    public static Result deserializeResult(DataInput is) throws IOException {
        requireNonNull(is, "Input stream was null");
        int numSerializedCells = is.readInt();
        List<Cell> cells = new ArrayList<>(numSerializedCells);
        for (int i = 0; i < numSerializedCells; i++) {
            cells.add(deserializeCell(is));
        }
        return Result.create(cells);
    }
}
