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
package org.apache.phoenix.hbase.index.covered.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.junit.Test;

/**
 * Test filter to ensure that it correctly handles KVs of different types correctly
 */
public class TestApplyAndFilterDeletesFilter {

  private static final Set<ImmutableBytesPtr> EMPTY_SET = Collections
      .<ImmutableBytesPtr> emptySet();
  private byte[] row = Bytes.toBytes("row");
  private byte[] family = Bytes.toBytes("family");
  private byte[] qualifier = Bytes.toBytes("qualifier");
  private byte[] value = Bytes.toBytes("value");
  private long ts = 10;

  @Test
  public void testDeletesAreNotReturned() {
    KeyValue kv = createKvForType(Type.Delete);
    ApplyAndFilterDeletesFilter filter = new ApplyAndFilterDeletesFilter(EMPTY_SET);
    assertEquals("Didn't skip point delete!", ReturnCode.SKIP, filter.filterKeyValue(kv));

    filter.reset();
    kv = createKvForType(Type.DeleteColumn);
    assertEquals("Didn't skip from column delete!", ReturnCode.SKIP, filter.filterKeyValue(kv));

    filter.reset();
    kv = createKvForType(Type.DeleteFamily);
    assertEquals("Didn't skip from family delete!", ReturnCode.SKIP, filter.filterKeyValue(kv));
  }

  /**
   * Hinting with this filter is a little convoluted as we binary search the list of families to
   * attempt to find the right one to seek.
   */
  @Test
  public void testHintCorrectlyToNextFamily() {
    // start with doing a family delete, so we will seek to the next column
    KeyValue kv = createKvForType(Type.DeleteFamily);
    ApplyAndFilterDeletesFilter filter = new ApplyAndFilterDeletesFilter(EMPTY_SET);
    assertEquals(ReturnCode.SKIP, filter.filterKeyValue(kv));
    KeyValue next = createKvForType(Type.Put);
    // make sure the hint is our attempt at the end key, because we have no more families to seek
    assertEquals("Didn't get a hint from a family delete", ReturnCode.SEEK_NEXT_USING_HINT,
      filter.filterKeyValue(next));
    assertEquals("Didn't get END_KEY with no families to match", KeyValue.LOWESTKEY,
      filter.getNextCellHint(next));

    // check for a family that comes before our family, so we always seek to the end as well
    filter = new ApplyAndFilterDeletesFilter(asSet(Bytes.toBytes("afamily")));
    assertEquals(ReturnCode.SKIP, filter.filterKeyValue(kv));
    // make sure the hint is our attempt at the end key, because we have no more families to seek
    assertEquals("Didn't get a hint from a family delete", ReturnCode.SEEK_NEXT_USING_HINT,
      filter.filterKeyValue(next));
    assertEquals("Didn't get END_KEY with no families to match", KeyValue.LOWESTKEY,
      filter.getNextCellHint(next));

    // check that we seek to the correct family that comes after our family
    byte[] laterFamily = Bytes.toBytes("zfamily");
    filter = new ApplyAndFilterDeletesFilter(asSet(laterFamily));
    assertEquals(ReturnCode.SKIP, filter.filterKeyValue(kv));
    @SuppressWarnings("deprecation")
    KeyValue expected = KeyValue.createFirstOnRow(kv.getRow(), laterFamily, new byte[0]);
    assertEquals("Didn't get a hint from a family delete", ReturnCode.SEEK_NEXT_USING_HINT,
      filter.filterKeyValue(next));
    assertEquals("Didn't get correct next key with a next family", expected,
      filter.getNextCellHint(next));
  }

  /**
   * Point deletes should only cover the exact entry they are tied to. Earlier puts should always
   * show up.
   */
  @Test
  public void testCoveringPointDelete() {
    // start with doing a family delete, so we will seek to the next column
    KeyValue kv = createKvForType(Type.Delete);
    ApplyAndFilterDeletesFilter filter = new ApplyAndFilterDeletesFilter(EMPTY_SET);
    filter.filterKeyValue(kv);
    KeyValue put = createKvForType(Type.Put);
    assertEquals("Didn't filter out put with same timestamp!", ReturnCode.SKIP,
      filter.filterKeyValue(put));
    // we should filter out the exact same put again, which could occur with the kvs all kept in the
    // same memstore
    assertEquals("Didn't filter out put with same timestamp on second call!", ReturnCode.SKIP,
      filter.filterKeyValue(put));

    // ensure then that we don't filter out a put with an earlier timestamp (though everything else
    // matches)
    put = createKvForType(Type.Put, ts - 1);
    assertEquals("Didn't accept put that has an earlier ts than the covering delete!",
      ReturnCode.INCLUDE, filter.filterKeyValue(put));
  }

  private KeyValue createKvForType(Type t) {
    return createKvForType(t, this.ts);
  }

  private KeyValue createKvForType(Type t, long timestamp) {
    return new KeyValue(row, family, qualifier, timestamp, t, value);
  }

  /**
   * Test that when we do a column delete at a given timestamp that we delete the entire column.
   * @throws Exception
   */
  @Test
  public void testCoverForDeleteColumn() throws Exception {
    ApplyAndFilterDeletesFilter filter = new ApplyAndFilterDeletesFilter(EMPTY_SET);
    KeyValue dc = createKvForType(Type.DeleteColumn, 11);
    KeyValue put = createKvForType(Type.Put, 10);
    assertEquals("Didn't filter out delete column.", ReturnCode.SKIP, filter.filterKeyValue(dc));
    assertEquals("Didn't get a seek hint for the deleted column", ReturnCode.SEEK_NEXT_USING_HINT,
      filter.filterKeyValue(put));
    // seek past the given put
    Cell seek = filter.getNextCellHint(put);
    assertTrue("Seeked key wasn't past the expected put - didn't skip the column",
      KeyValue.COMPARATOR.compare(seek, put) > 0);
  }

  /**
   * DeleteFamily markers should delete everything from that timestamp backwards, but not hide
   * anything forwards
   */
  @Test
  public void testDeleteFamilyCorrectlyCoversColumns() {
    ApplyAndFilterDeletesFilter filter = new ApplyAndFilterDeletesFilter(EMPTY_SET);
    KeyValue df = createKvForType(Type.DeleteFamily, 11);
    KeyValue put = createKvForType(Type.Put, 12);

    assertEquals("Didn't filter out delete family", ReturnCode.SKIP, filter.filterKeyValue(df));
    assertEquals("Filtered out put with newer TS than delete family", ReturnCode.INCLUDE,
      filter.filterKeyValue(put));

    // older kv shouldn't be visible
    put = createKvForType(Type.Put, 10);
    assertEquals("Didn't filter out older put, covered by DeleteFamily marker",
      ReturnCode.SEEK_NEXT_USING_HINT, filter.filterKeyValue(put));

    // next seek should be past the families
    assertEquals(KeyValue.LOWESTKEY, filter.getNextCellHint(put));
  }

  /**
   * Test that we don't cover other columns when we have a delete column.
   */
  @Test
  public void testDeleteColumnCorrectlyCoversColumns() {
    ApplyAndFilterDeletesFilter filter = new ApplyAndFilterDeletesFilter(EMPTY_SET);
    KeyValue d = createKvForType(Type.DeleteColumn, 12);
    byte[] qual2 = Bytes.add(qualifier, Bytes.toBytes("-other"));
    KeyValue put = new KeyValue(row, family, qual2, 11, Type.Put, value);

    assertEquals("Didn't filter out delete column", ReturnCode.SKIP, filter.filterKeyValue(d));
    // different column put should still be visible
    assertEquals("Filtered out put with different column than the delete", ReturnCode.INCLUDE,
      filter.filterKeyValue(put));

    // set a delete family, but in the past
    d = createKvForType(Type.DeleteFamily, 10);
    assertEquals("Didn't filter out delete column", ReturnCode.SKIP, filter.filterKeyValue(d));
    // add back in the original delete column
    d = createKvForType(Type.DeleteColumn, 11);
    assertEquals("Didn't filter out delete column", ReturnCode.SKIP, filter.filterKeyValue(d));
    // onto a different family, so that must be visible too
    assertEquals("Filtered out put with different column than the delete", ReturnCode.INCLUDE,
      filter.filterKeyValue(put));
  }

  private static Set<ImmutableBytesPtr> asSet(byte[]... strings) {
    Set<ImmutableBytesPtr> set = new HashSet<ImmutableBytesPtr>();
    for (byte[] s : strings) {
      set.add(new ImmutableBytesPtr(s));
    }
    return set;
  }
}
