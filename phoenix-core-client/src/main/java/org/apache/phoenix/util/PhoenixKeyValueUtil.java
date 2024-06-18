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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.ByteBufferExtendedCell;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.com.google.common.base.Function;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.compat.hbase.CompatUtil;
import org.apache.phoenix.execute.MutationState.MultiRowMutationState;
import org.apache.phoenix.execute.MutationState.RowMutationState;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.schema.TableRef;

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

    public static Cell newKeyValue(byte[] key, byte[] cf, byte[] cq, long ts, byte[] value, int valueOffset, int valueLength) {
        return CellBuilderFactory.create(CellBuilderType.DEEP_COPY).setRow(key).setFamily(cf)
                .setQualifier(cq).setTimestamp(ts).setType(Type.Put)
                .setValue(value, valueOffset, valueLength).build();
    }

    public static Cell newKeyValue(ImmutableBytesWritable key, byte[] cf, byte[] cq, long ts, byte[] value, int valueOffset, int valueLength) {
        return CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
                .setRow(key.get(), key.getOffset(), key.getLength()).setFamily(cf).setQualifier(cq)
                .setTimestamp(ts).setType(Type.Put).setValue(value, valueOffset, valueLength)
                .build();
    }

    public static Cell newKeyValue(byte[] key, int keyOffset, int keyLength, byte[] cf, byte[] cq, long ts, byte[] value, int valueOffset, int valueLength) {
        return CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
                .setRow(key, keyOffset, keyLength).setFamily(cf).setQualifier(cq).setTimestamp(ts)
                .setType(Type.Put).setValue(value, valueOffset, valueLength).build();
    }
    
    public static Cell newKeyValue(byte[] key, int keyOffset, int keyLength, byte[] cf, 
        int cfOffset, int cfLength, byte[] cq, int cqOffset, int cqLength, long ts, byte[] value, 
        int valueOffset, int valueLength,Type type) {
        return CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
                .setRow(key, keyOffset, keyLength).setFamily(cf, cfOffset, cfLength)
                .setQualifier(cq, cqOffset, cqLength).setTimestamp(ts)
                .setValue(value, valueOffset, valueLength).setType(type).build();
    }
    
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
                size += c.getSerializedSize();
            }
        }
        return size;
    }

    /**
     * Estimates the storage size of a row
     * @param tableMutationMap map from table to row to RowMutationState
     * @return estimated row size
     */
    public static long getEstimatedRowMutationSize(
            Map<TableRef, MultiRowMutationState> tableMutationMap) {
        long size = 0;
        // iterate over table
        for (Entry<TableRef, MultiRowMutationState> tableEntry : tableMutationMap.entrySet()) {
            size += calculateMultiRowMutationSize(tableEntry.getValue());
        }
        return size;
    }

    public static long getEstimatedRowMutationSizeWithBatch(Map<TableRef, List<MultiRowMutationState>> tableMutationMap) {
        long size = 0;
        // iterate over table
        for (Entry<TableRef, List<MultiRowMutationState>> tableEntry : tableMutationMap.entrySet()) {
            for (MultiRowMutationState batch : tableEntry.getValue()) {
                size += calculateMultiRowMutationSize(batch);
            }
        }
        return size;
    }

    /**
     * If c is not a KeyValue, cast it to KeyValue and return it.
     * If c is a KeyValue, just return it
     *
     * @param c cell
     * @return either c case to ExtendedCell, or its copy as a KeyValue
     */
    public static KeyValue ensureKeyValue(Cell c) {
        // Same as KeyValueUtil, but HBase has deprecated this method. Avoid depending on something
        // that will likely be removed at some point in time.
        if (c == null) return null;
        // TODO Do we really want to return only KeyValues, or would it be enough to
        // copy ByteBufferExtendedCells to heap ?
        // i.e can we avoid copying on-heap cells like BufferedDataBlockEncoder.OnheapDecodedCell ?
        if (c instanceof KeyValue) {
            return (KeyValue) c;
        } else {
            return KeyValueUtil.copyToNewKeyValue(c);
        }
    }

    /**
     * Replace non-KeyValue cells with their KeyValue copies
     *
     * This ensures that all Cells are copied on-heap, but will do
     * extra work for any non-KeyValue on-heap cells
     *
     * @param cells modified, its elements are replaced
     * @return the modified input cells object, for convenience
     */
    public static List<KeyValue> ensureKeyValues(List<Cell> cells) {
        List<KeyValue> lazyList = Lists.transform(cells, new Function<Cell, KeyValue>() {
            @Override
            public KeyValue apply(Cell arg0) {
                return ensureKeyValue(arg0);
            }
        });
        return new ArrayList<>(lazyList);
    }

    public static KeyValue maybeCopyCell(Cell c) {
        // Same as KeyValueUtil, but HBase has deprecated this method. Avoid depending on something
        // that will likely be removed at some point in time.
        if (c == null) {
            return null;
        }
        // TODO Do we really want to return only KeyValues, or would it be enough to
        // copy ByteBufferExtendedCells to heap ?
        // i.e can we avoid copying on-heap cells like BufferedDataBlockEncoder.OnheapDecodedCell ?
        if (c instanceof KeyValue) {
            return (KeyValue) c;
        }
        return KeyValueUtil.copyToNewKeyValue(c);
    }

    /**
     * Copy all Off-Heap cells to KeyValues
     * The input list is modified.
     *
     * @param cells is modified in place
     * @return the modified list (optional, input list is modified in place)
     */
    public static List<Cell> maybeCopyCellList(List<Cell> cells) {
        ListIterator<Cell> cellsIt = cells.listIterator();
        while (cellsIt.hasNext()) {
            Cell c = cellsIt.next();
            //FIXME this does not catch all off-heap cells
            if (c instanceof ByteBufferExtendedCell) {
                cellsIt.set(KeyValueUtil.copyToNewKeyValue(c));
            }
        }
        return cells;
    }

    private static long calculateMultiRowMutationSize(MultiRowMutationState mutations) {
        long size = 0;
        // iterate over rows
        for (Entry<ImmutableBytesPtr, RowMutationState> rowEntry : mutations.entrySet()) {
            size += calculateRowMutationSize(rowEntry);
        }
        return size;
    }

    private static long calculateRowMutationSize(Entry<ImmutableBytesPtr, RowMutationState> rowEntry) {
        int rowLength = rowEntry.getKey().getLength();
        long colValuesLength = rowEntry.getValue().calculateEstimatedSize();
        return (rowLength + colValuesLength);
    }

    public static void setTimestamp(Mutation m, long timestamp) {
        byte[] tsBytes = Bytes.toBytes(timestamp);
        for (List<Cell> family : m.getFamilyCellMap().values()) {
            // TODO Do we really need to copy everything to the HEAP here ?
            List<KeyValue> familyKVs = ensureKeyValues(family);
            for (KeyValue kv : familyKVs) {
                int tsOffset = kv.getTimestampOffset();
                System.arraycopy(tsBytes, 0, kv.getBuffer(), tsOffset, Bytes.SIZEOF_LONG);
            }
        }
    }
}
