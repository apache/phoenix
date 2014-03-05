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
package org.apache.phoenix.hbase.index.covered.example;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;
import org.apache.phoenix.hbase.index.covered.IndexCodec;
import org.apache.phoenix.hbase.index.covered.IndexUpdate;
import org.apache.phoenix.hbase.index.covered.LocalTableState;
import org.apache.phoenix.hbase.index.covered.data.LocalHBaseState;
import org.apache.phoenix.hbase.index.covered.example.ColumnGroup;
import org.apache.phoenix.hbase.index.covered.example.CoveredColumn;
import org.apache.phoenix.hbase.index.covered.example.CoveredColumnIndexCodec;
import org.apache.phoenix.hbase.index.covered.example.CoveredColumnIndexCodec.ColumnEntry;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;

public class TestCoveredColumnIndexCodec {
  private static final byte[] PK = new byte[] { 'a' };
  private static final String FAMILY_STRING = "family";
  private static final byte[] FAMILY = Bytes.toBytes(FAMILY_STRING);
  private static final byte[] QUAL = Bytes.toBytes("qual");
  private static final CoveredColumn COLUMN_REF = new CoveredColumn(FAMILY_STRING, QUAL);
  private static final byte[] EMPTY_INDEX_KEY = CoveredColumnIndexCodec.composeRowKey(PK, 0,
    Arrays.asList(toColumnEntry(new byte[0])));
  private static final byte[] BLANK_INDEX_KEY = CoveredColumnIndexCodec.composeRowKey(PK, 0,
    Collections.<ColumnEntry> emptyList());

  private static ColumnEntry toColumnEntry(byte[] bytes) {
    return new ColumnEntry(bytes, COLUMN_REF);
  }

  /**
   * Convert between an index and a bunch of values
   * @throws Exception
   */
  @Test
  public void toFromIndexKey() throws Exception {
    // start with empty values
    byte[] indexKey = BLANK_INDEX_KEY;
    List<byte[]> stored = CoveredColumnIndexCodec.getValues(indexKey);
    assertEquals("Found some stored values in an index row key that wasn't created with values!",
      0, stored.size());

    // a single, empty value
    indexKey = EMPTY_INDEX_KEY;
    stored = CoveredColumnIndexCodec.getValues(indexKey);
    assertEquals("Found some stored values in an index row key that wasn't created with values!",
      1, stored.size());
    assertEquals("Found a non-zero length value: " + Bytes.toString(stored.get(0)), 0,
      stored.get(0).length);

    // try with a couple values, some different lengths
    byte[] v1 = new byte[] { 'a' };
    byte[] v2 = new byte[] { 'b' };
    byte[] v3 = Bytes.toBytes("v3");
    int len = v1.length + v2.length + v3.length;
    indexKey =
        CoveredColumnIndexCodec.composeRowKey(PK, len,
          Arrays.asList(toColumnEntry(v1), toColumnEntry(v2), toColumnEntry(v3)));
    stored = CoveredColumnIndexCodec.getValues(indexKey);
    assertEquals("Didn't find expected number of values in index key!", 3, stored.size());
    assertTrue("First index keys don't match!", Bytes.equals(v1, stored.get(0)));
    assertTrue("Second index keys don't match!", Bytes.equals(v2, stored.get(1)));
    assertTrue("Third index keys don't match!", Bytes.equals(v3, stored.get(2)));
  }

  /**
   * Ensure that we correctly can determine when a row key is empty (no values).
   */
  @Test
  public void testCheckRowKeyForAllNulls() {
    byte[] pk = new byte[] { 'a', 'b', 'z' };
    // check positive cases first
    byte[] result = EMPTY_INDEX_KEY;
    assertTrue("Didn't correctly read single element as being null in row key",
      CoveredColumnIndexCodec.checkRowKeyForAllNulls(result));
    result =
        CoveredColumnIndexCodec.composeRowKey(pk, 0,
          Lists.newArrayList(toColumnEntry(new byte[0]), toColumnEntry(new byte[0])));
    assertTrue("Didn't correctly read two elements as being null in row key",
      CoveredColumnIndexCodec.checkRowKeyForAllNulls(result));

    // check cases where it isn't null
    result =
        CoveredColumnIndexCodec.composeRowKey(pk, 2,
          Arrays.asList(toColumnEntry(new byte[] { 1, 2 })));
    assertFalse("Found a null key, when it wasn't!",
      CoveredColumnIndexCodec.checkRowKeyForAllNulls(result));
    result =
        CoveredColumnIndexCodec.composeRowKey(pk, 2,
          Arrays.asList(toColumnEntry(new byte[] { 1, 2 }), toColumnEntry(new byte[0])));
    assertFalse("Found a null key, when it wasn't!",
      CoveredColumnIndexCodec.checkRowKeyForAllNulls(result));
  }

  private static class SimpleTableState implements LocalHBaseState {

    private Result r;

    public SimpleTableState(Result r) {
      this.r = r;
    }

    @Override
    public Result getCurrentRowState(Mutation m, Collection<? extends ColumnReference> toCover)
        throws IOException {
      return r;
    }

  }

  /**
   * Test that we get back the correct index updates for a given column group
   * @throws Exception on failure
   */
  @Test
  public void testGeneratedIndexUpdates() throws Exception {
    ColumnGroup group = new ColumnGroup("test-column-group");
    group.add(COLUMN_REF);

    final Result emptyState = Result.create(Collections.<Cell> emptyList());
    
    // setup the state we expect for the codec
    RegionCoprocessorEnvironment env = Mockito.mock(RegionCoprocessorEnvironment.class);
    Configuration conf = new Configuration(false);
    Mockito.when(env.getConfiguration()).thenReturn(conf);
    LocalHBaseState table = new SimpleTableState(emptyState);

    // make a new codec on those kvs
    CoveredColumnIndexCodec codec =
        CoveredColumnIndexCodec.getCodecForTesting(Arrays.asList(group));

    // start with a basic put that has some keyvalues
    Put p = new Put(PK);
    // setup the kvs to add
    List<Cell> kvs = new ArrayList<Cell>();
    byte[] v1 = Bytes.toBytes("v1");
    KeyValue kv = new KeyValue(PK, FAMILY, QUAL, 1, v1);
    kvs.add(kv);
    p.add(kv);
    byte[] v2 = Bytes.toBytes("v2");
    kv = new KeyValue(PK, Bytes.toBytes("family2"), QUAL, 1, v2);
    kvs.add(kv);
    p.add(kv);

    // check the codec for deletes it should send
    LocalTableState state = new LocalTableState(env, table, p);
    Iterable<IndexUpdate> updates = codec.getIndexDeletes(state);
    assertFalse("Found index updates without any existing kvs in table!", updates.iterator().next()
        .isValid());

    // get the updates with the pending update
    state.setCurrentTimestamp(1);
    state.addPendingUpdates(KeyValueUtil.ensureKeyValues(kvs));
    updates = codec.getIndexUpserts(state);
    assertTrue("Didn't find index updates for pending primary table update!", updates.iterator()
        .hasNext());
    for (IndexUpdate update : updates) {
      assertTrue("Update marked as invalid, but should be a pending index write!", update.isValid());
      Put m = (Put) update.getUpdate();
      // should just be the single update for the column reference
      byte[] expected =
          CoveredColumnIndexCodec.composeRowKey(PK, v1.length, Arrays.asList(toColumnEntry(v1)));
      assertArrayEquals("Didn't get expected index value", expected, m.getRow());
    }

    // then apply a delete
    Delete d = new Delete(PK, 2);
    // need to set the timestamp here, as would actually happen on the server, unlike what happens
    // with puts, where the get the constructor specified timestamp for unspecified methods.
    d.deleteFamily(FAMILY, 2);
    // setup the next batch of 'current state', basically just ripping out the current state from
    // the last round
    table = new SimpleTableState(Result.create(kvs));
    state = new LocalTableState(env, table, d);
    state.setCurrentTimestamp(2);
    // check the cleanup of the current table, after the puts (mocking a 'next' update)
    updates = codec.getIndexDeletes(state);
    for (IndexUpdate update : updates) {
      assertTrue("Didn't have any index cleanup, even though there is current state",
        update.isValid());
      Delete m = (Delete) update.getUpdate();
      // should just be the single update for the column reference
      byte[] expected =
          CoveredColumnIndexCodec.composeRowKey(PK, v1.length, Arrays.asList(toColumnEntry(v1)));
      assertArrayEquals("Didn't get expected index value", expected, m.getRow());
    }
    ensureNoUpdatesWhenCoveredByDelete(env, codec, kvs, d);

    // now with the delete of the columns
    d = new Delete(PK, 2);
    d.deleteColumns(FAMILY, QUAL, 2);
    ensureNoUpdatesWhenCoveredByDelete(env, codec, kvs, d);

    // this delete needs to match timestamps exactly, by contract, to have any effect
    d = new Delete(PK, 1);
    d.deleteColumn(FAMILY, QUAL, 1);
    ensureNoUpdatesWhenCoveredByDelete(env, codec, kvs, d);
  }

  private void ensureNoUpdatesWhenCoveredByDelete(RegionCoprocessorEnvironment env, IndexCodec codec, List<Cell> currentState,
      Delete d) throws IOException {
    LocalHBaseState table = new SimpleTableState(Result.create(currentState));
    LocalTableState state = new LocalTableState(env, table, d);
    state.setCurrentTimestamp(d.getTimeStamp());
    // now we shouldn't see anything when getting the index update
    state.addPendingUpdates(KeyValueUtil.ensureKeyValues(d.getFamilyCellMap().get(FAMILY)));
    Iterable<IndexUpdate> updates = codec.getIndexUpserts(state);
    for (IndexUpdate update : updates) {
      assertFalse("Had some index updates, though it should have been covered by the delete",
        update.isValid());
    }
  }
}