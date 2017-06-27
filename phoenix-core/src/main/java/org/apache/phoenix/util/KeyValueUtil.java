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

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.execute.MutationState.RowMutationState;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PArrayDataTypeEncoder;

/**
 * 
 * Utilities for KeyValue. Where there's duplication with KeyValue methods,
 * these avoid creating new objects when not necessary (primary preventing
 * byte array copying).
 *
 * 
 * @since 0.1
 */
public class KeyValueUtil {
    private KeyValueUtil() {
    }

    public static KeyValue newKeyValue(byte[] key, byte[] cf, byte[] cq, long ts, byte[] value, int valueOffset, int valueLength) {
        return new KeyValue(key, 0, key.length,
                cf, 0, cf.length,
                cq, 0, cq.length,
                ts, Type.Put,
                value, valueOffset, valueLength);
    }

    public static KeyValue newKeyValue(ImmutableBytesWritable key, byte[] cf, byte[] cq, long ts, byte[] value, int valueOffset, int valueLength) {
        return new KeyValue(key.get(), key.getOffset(), key.getLength(),
                cf, 0, cf.length,
                cq, 0, cq.length,
                ts, Type.Put,
                value, valueOffset, valueLength);
    }

    public static KeyValue newKeyValue(byte[] key, int keyOffset, int keyLength, byte[] cf, byte[] cq, long ts, byte[] value, int valueOffset, int valueLength) {
        return new KeyValue(key, keyOffset, keyLength,
                cf, 0, cf.length,
                cq, 0, cq.length,
                ts, Type.Put,
                value, valueOffset, valueLength);
    }
    
    public static KeyValue newKeyValue(byte[] key, int keyOffset, int keyLength, byte[] cf, 
        int cfOffset, int cfLength, byte[] cq, int cqOffset, int cqLength, long ts, byte[] value, 
        int valueOffset, int valueLength) {
        return new KeyValue(key, keyOffset, keyLength,
                cf, cfOffset, cfLength,
                cq, cqOffset, cqLength,
                ts, Type.Put,
                value, valueOffset, valueLength);
    }
    
	public static KeyValue newKeyValue(byte[] key, byte[] cf, byte[] cq, long ts, byte[] value) {
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
        assert CellUtil.matchingRow(kvs.get(0), kvs.get(kvs.size()-1));

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
        assert CellUtil.matchingRow(kvs[0], kvs[kvs.length-1]);

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
    public static long
            getEstimatedRowSize(Map<TableRef, Map<ImmutableBytesPtr, RowMutationState>> mutations) {
        long size = 0;
        // iterate over tables
        for (Entry<TableRef, Map<ImmutableBytesPtr, RowMutationState>> tableEntry : mutations
                .entrySet()) {
            PTable table = tableEntry.getKey().getTable();
            // iterate over rows
            for (Entry<ImmutableBytesPtr, RowMutationState> rowEntry : tableEntry.getValue()
                    .entrySet()) {
                int rowLength = rowEntry.getKey().getLength();
                Map<PColumn, byte[]> colValueMap = rowEntry.getValue().getColumnValues();
                switch (table.getImmutableStorageScheme()) {
                case ONE_CELL_PER_COLUMN:
                    // iterate over columns
                    for (Entry<PColumn, byte[]> colValueEntry : colValueMap.entrySet()) {
                        PColumn pColumn = colValueEntry.getKey();
                        size +=
                                KeyValue.getKeyValueDataStructureSize(rowLength,
                                    pColumn.getFamilyName().getBytes().length,
                                    pColumn.getColumnQualifierBytes().length,
                                    colValueEntry.getValue().length);
                    }
                    break;
                case SINGLE_CELL_ARRAY_WITH_OFFSETS:
                    // we store all the column values in a single key value that contains all the
                    // column values followed by an offset array
                    size +=
                            PArrayDataTypeEncoder.getEstimatedByteSize(table, rowLength,
                                colValueMap);
                    break;
                }
                // count the empty key value
                Pair<byte[], byte[]> emptyKeyValueInfo =
                        EncodedColumnsUtil.getEmptyKeyValueInfo(table);
                size +=
                        KeyValue.getKeyValueDataStructureSize(rowLength,
                            SchemaUtil.getEmptyColumnFamilyPtr(table).getLength(),
                            emptyKeyValueInfo.getFirst().length,
                            emptyKeyValueInfo.getSecond().length);
            }
        }
        return size;
    }
}
