/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hbase.index.covered.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.collect.Lists;
import org.apache.phoenix.hbase.index.covered.IndexUpdate;
import org.apache.phoenix.hbase.index.covered.TableState;
import org.apache.phoenix.hbase.index.scanner.Scanner;
import org.apache.phoenix.index.BaseIndexCodec;

/**
 *
 */
public class CoveredColumnIndexCodec extends BaseIndexCodec {

  private static final byte[] EMPTY_BYTES = new byte[0];
  public static final byte[] INDEX_ROW_COLUMN_FAMILY = Bytes.toBytes("INDEXED_COLUMNS");

  private List<ColumnGroup> groups;

  /**
   * @param groups to initialize the codec with
   * @return an instance that is initialized with the given {@link ColumnGroup}s, for testing
   *         purposes
   */
  public static CoveredColumnIndexCodec getCodecForTesting(List<ColumnGroup> groups) {
    CoveredColumnIndexCodec codec = new CoveredColumnIndexCodec();
    codec.groups = Lists.newArrayList(groups);
    return codec;
  }

  @Override
  public void initialize(RegionCoprocessorEnvironment env) {
    groups = CoveredColumnIndexSpecifierBuilder.getColumns(env.getConfiguration());
  }

  @Override
  public Iterable<IndexUpdate> getIndexUpserts(TableState state) {
    List<IndexUpdate> updates = new ArrayList<IndexUpdate>();
    for (ColumnGroup group : groups) {
      IndexUpdate update = getIndexUpdateForGroup(group, state);
      updates.add(update);
    }
    return updates;
  }

  /**
   * @param group
   * @param state
   * @return the update that should be made to the table
   */
  private IndexUpdate getIndexUpdateForGroup(ColumnGroup group, TableState state) {
    List<CoveredColumn> refs = group.getColumns();
    try {
      Pair<Scanner, IndexUpdate> stateInfo = state.getIndexedColumnsTableState(refs);
      Scanner kvs = stateInfo.getFirst();
      Pair<Integer, List<ColumnEntry>> columns =
          getNextEntries(refs, kvs, state.getCurrentRowKey());
      // make sure we close the scanner
      kvs.close();
      if (columns.getFirst().intValue() == 0) {
        return stateInfo.getSecond();
      }
      // have all the column entries, so just turn it into a Delete for the row
      // convert the entries to the needed values
      byte[] rowKey =
          composeRowKey(state.getCurrentRowKey(), columns.getFirst(), columns.getSecond());
      Put p = new Put(rowKey, state.getCurrentTimestamp());
      // add the columns to the put
      addColumnsToPut(p, columns.getSecond());

      // update the index info
      IndexUpdate update = stateInfo.getSecond();
      update.setTable(Bytes.toBytes(group.getTable()));
      update.setUpdate(p);
      return update;
    } catch (IOException e) {
      throw new RuntimeException("Unexpected exception when getting state for columns: " + refs);
    }
  }

  private static void addColumnsToPut(Put indexInsert, List<ColumnEntry> columns) {
    // add each of the corresponding families to the put
    int count = 0;
    for (ColumnEntry column : columns) {
      indexInsert.add(INDEX_ROW_COLUMN_FAMILY,
        ArrayUtils.addAll(Bytes.toBytes(count++), toIndexQualifier(column.ref)), null);
    }
  }

  private static byte[] toIndexQualifier(CoveredColumn column) {
    return ArrayUtils.addAll(Bytes.toBytes(column.familyString + CoveredColumn.SEPARATOR),
      column.getQualifier());
  }

  @Override
  public Iterable<IndexUpdate> getIndexDeletes(TableState state) {
    List<IndexUpdate> deletes = new ArrayList<IndexUpdate>();
    for (ColumnGroup group : groups) {
      deletes.add(getDeleteForGroup(group, state));
    }
    return deletes;
  }


  /**
   * Get all the deletes necessary for a group of columns - logically, the cleanup the index table
   * for a given index.
   * @param group index information
   * @return the cleanup for the given index, or <tt>null</tt> if no cleanup is necessary
   */
  private IndexUpdate getDeleteForGroup(ColumnGroup group, TableState state) {
    List<CoveredColumn> refs = group.getColumns();
    try {
      Pair<Scanner, IndexUpdate> kvs = state.getIndexedColumnsTableState(refs);
      Pair<Integer, List<ColumnEntry>> columns =
          getNextEntries(refs, kvs.getFirst(), state.getCurrentRowKey());
      // make sure we close the scanner reference
      kvs.getFirst().close();
      // no change, just return the passed update
      if (columns.getFirst() == 0) {
        return kvs.getSecond();
      }
      // have all the column entries, so just turn it into a Delete for the row
      // convert the entries to the needed values
      byte[] rowKey =
          composeRowKey(state.getCurrentRowKey(), columns.getFirst(), columns.getSecond());
      Delete d = new Delete(rowKey);
      d.setTimestamp(state.getCurrentTimestamp());
      IndexUpdate update = kvs.getSecond();
      update.setUpdate(d);
      update.setTable(Bytes.toBytes(group.getTable()));
      return update;
    } catch (IOException e) {
      throw new RuntimeException("Unexpected exception when getting state for columns: " + refs);
    }
  }

  /**
   * Get the next batch of primary table values for the given columns
   * @param refs columns to match against
   * @param kvs
   * @param currentRow
   * @return the total length of all values found and the entries to add for the index
   */
  private Pair<Integer, List<ColumnEntry>> getNextEntries(List<CoveredColumn> refs, Scanner kvs,
      byte[] currentRow) throws IOException {
    int totalValueLength = 0;
    List<ColumnEntry> entries = new ArrayList<ColumnEntry>(refs.size());

    // pull out the latest state for each column reference, in order
    for (CoveredColumn ref : refs) {
      KeyValue first = ref.getFirstKeyValueForRow(currentRow);
      if (!kvs.seek(first)) {
        // no more keys, so add a null value
        entries.add(new ColumnEntry(null, ref));
        continue;
      }
      // there is a next value - we only care about the current value, so we can just snag that
      Cell next = kvs.next();
      if (ref.matchesFamily(next.getFamily()) && ref.matchesQualifier(next.getQualifier())) {
        byte[] v = next.getValue();
        totalValueLength += v.length;
        entries.add(new ColumnEntry(v, ref));
      } else {
        // this first one didn't match at all, so we have to put in a null entry
        entries.add(new ColumnEntry(null, ref));
        continue;
      }
      // here's where is gets a little tricky - we either need to decide if we should continue
      // adding entries (matches all qualifiers) or if we are done (matches a single qualifier)
      if (!ref.allColumns()) {
        continue;
      }
      // matches all columns, so we need to iterate until we hit the next column with the same
      // family as the current key
      byte[] lastQual = next.getQualifier();
      byte[] nextQual = null;
      while ((next = kvs.next()) != null) {
        // different family, done with this column
        if (!ref.matchesFamily(next.getFamily())) {
          break;
        }
        nextQual = next.getQualifier();
        // we are still on the same qualifier - skip it, since we already added a column for it
        if (Arrays.equals(lastQual, nextQual)) {
          continue;
        }
        // this must match the qualifier since its an all-qualifiers specifier, so we add it
        byte[] v = next.getValue();
        totalValueLength += v.length;
        entries.add(new ColumnEntry(v, ref));
        // update the last qualifier to check against
        lastQual = nextQual;
      }
    }
    return new Pair<Integer, List<ColumnEntry>>(totalValueLength, entries);
  }

  static class ColumnEntry {
    byte[] value = EMPTY_BYTES;
    CoveredColumn ref;

    public ColumnEntry(byte[] value, CoveredColumn ref) {
      this.value = value == null ? EMPTY_BYTES : value;
      this.ref = ref;
    }
  }

  /**
   * Compose the final index row key.
   * <p>
   * This is faster than adding each value independently as we can just build a single a array and
   * copy everything over once.
   * @param pk primary key of the original row
   * @param length total number of bytes of all the values that should be added
   * @param values to use when building the key
   */
  static byte[] composeRowKey(byte[] pk, int length, List<ColumnEntry> values) {
    // now build up expected row key, each of the values, in order, followed by the PK and then some
    // info about lengths so we can deserialize each value
    byte[] output = new byte[length + pk.length];
    int pos = 0;
    int[] lengths = new int[values.size()];
    int i = 0;
    for (ColumnEntry entry : values) {
      byte[] v = entry.value;
      // skip doing the copy attempt, if we don't need to
      if (v.length != 0) {
        System.arraycopy(v, 0, output, pos, v.length);
        pos += v.length;
      }
      lengths[i++] = v.length;
    }

    // add the primary key to the end of the row key
    System.arraycopy(pk, 0, output, pos, pk.length);

    // add the lengths as suffixes so we can deserialize the elements again
    for (int l : lengths) {
      output = ArrayUtils.addAll(output, Bytes.toBytes(l));
    }

    // and the last integer is the number of values
    return ArrayUtils.addAll(output, Bytes.toBytes(values.size()));
  }

  /**
   * Essentially a short-cut from building a {@link Put}.
   * @param pk row key
   * @param timestamp timestamp of all the keyvalues
   * @param values expected value--column pair
   * @return a keyvalues that the index contains for a given row at a timestamp with the given value
   *         -- column pairs.
   */
  public static List<KeyValue> getIndexKeyValueForTesting(byte[] pk, long timestamp,
      List<Pair<byte[], CoveredColumn>> values) {
  
    int length = 0;
    List<ColumnEntry> expected = new ArrayList<ColumnEntry>(values.size());
    for (Pair<byte[], CoveredColumn> value : values) {
      ColumnEntry entry = new ColumnEntry(value.getFirst(), value.getSecond());
      length += value.getFirst().length;
      expected.add(entry);
    }
  
    byte[] rowKey = CoveredColumnIndexCodec.composeRowKey(pk, length, expected);
    Put p = new Put(rowKey, timestamp);
    CoveredColumnIndexCodec.addColumnsToPut(p, expected);
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    for (Entry<byte[], List<KeyValue>> entry : p.getFamilyMap().entrySet()) {
      kvs.addAll(entry.getValue());
    }
  
    return kvs;
  }

  public static List<byte[]> getValues(byte[] bytes) {
    // get the total number of keys in the bytes
    int keyCount = CoveredColumnIndexCodec.getPreviousInteger(bytes, bytes.length);
    List<byte[]> keys = new ArrayList<byte[]>(keyCount);
    int[] lengths = new int[keyCount];
    int lengthPos = keyCount - 1;
    int pos = bytes.length - Bytes.SIZEOF_INT;
    // figure out the length of each key
    for (int i = 0; i < keyCount; i++) {
      lengths[lengthPos--] = CoveredColumnIndexCodec.getPreviousInteger(bytes, pos);
      pos -= Bytes.SIZEOF_INT;
    }

    int current = 0;
    for (int length : lengths) {
      byte[] key = Arrays.copyOfRange(bytes, current, current + length);
      keys.add(key);
      current += length;
    }

    return keys;
  }

  /**
   * Read an integer from the preceding {@value Bytes#SIZEOF_INT} bytes
   * @param bytes array to read from
   * @param start start point, backwards from which to read. For example, if specifying "25", we
   *          would try to read an integer from 21 -> 25
   * @return an integer from the proceeding {@value Bytes#SIZEOF_INT} bytes, if it exists.
   */
  private static int getPreviousInteger(byte[] bytes, int start) {
    return Bytes.toInt(bytes, start - Bytes.SIZEOF_INT);
  }

  /**
   * Check to see if an row key just contains a list of null values.
   * @param bytes row key to examine
   * @return <tt>true</tt> if all the values are zero-length, <tt>false</tt> otherwise
   */
  public static boolean checkRowKeyForAllNulls(byte[] bytes) {
    int keyCount = CoveredColumnIndexCodec.getPreviousInteger(bytes, bytes.length);
    int pos = bytes.length - Bytes.SIZEOF_INT;
    for (int i = 0; i < keyCount; i++) {
      int next = CoveredColumnIndexCodec.getPreviousInteger(bytes, pos);
      if (next > 0) {
        return false;
      }
      pos -= Bytes.SIZEOF_INT;
    }

    return true;
  }

  @Override
  public boolean isEnabled(Mutation m) {
    // this could be a bit smarter, looking at the groups for the mutation, but we leave it at this
    // simple check for the moment.
    return groups.size() > 0;
  }
}