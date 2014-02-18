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
package org.apache.phoenix.hbase.index.covered.update;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;

import org.apache.phoenix.hbase.index.covered.update.IndexUpdateManager;

public class TestIndexUpdateManager {

  private static final byte[] row = Bytes.toBytes("row");
  private static final String TABLE_NAME = "table";
  private static final byte[] table = Bytes.toBytes(TABLE_NAME);

  @Test
  public void testMutationComparator() throws Exception {
    IndexUpdateManager manager = new IndexUpdateManager();
    Comparator<Mutation> comparator = manager.COMPARATOR;
    Put p = new Put(row, 10);
    // lexigraphically earlier should sort earlier
    Put p1 = new Put(Bytes.toBytes("ro"), 10);
    assertTrue("lexigraphically later sorting first, should be earlier first.",
      comparator.compare(p, p1) > 0);
    p1 = new Put(Bytes.toBytes("row1"), 10);
    assertTrue("lexigraphically later sorting first, should be earlier first.",
      comparator.compare(p1, p) > 0);

    // larger ts sorts before smaller, for the same row
    p1 = new Put(row, 11);
    assertTrue("Smaller timestamp sorting first, should be larger first.",
      comparator.compare(p, p1) > 0);
    // still true, even for deletes
    Delete d = new Delete(row, 11);
    assertTrue("Smaller timestamp sorting first, should be larger first.",
      comparator.compare(p, d) > 0);

    // for the same row, t1, the delete should sort earlier
    d = new Delete(row, 10);
    assertTrue("Delete doesn't sort before put, for the same row and ts",
      comparator.compare(p, d) > 0);

    // but for different rows, we still respect the row sorting.
    d = new Delete(Bytes.toBytes("row1"), 10);
    assertTrue("Delete doesn't sort before put, for the same row and ts",
      comparator.compare(p, d) < 0);
  }

  /**
   * When making updates we need to cancel out {@link Delete} and {@link Put}s for the same row.
   * @throws Exception on failure
   */
  @Test
  public void testCancelingUpdates() throws Exception {
    IndexUpdateManager manager = new IndexUpdateManager();

    long ts1 = 10, ts2 = 11;
    // at different timestamps, so both should be retained
    Delete d = new Delete(row, ts1);
    Put p = new Put(row, ts2);
    manager.addIndexUpdate(table, d);
    manager.addIndexUpdate(table, p);
    List<Mutation> pending = new ArrayList<Mutation>();
    pending.add(p);
    pending.add(d);
    validate(manager, pending);

    // add a delete that should cancel out the put, leading to only one delete remaining
    Delete d2 = new Delete(row, ts2);
    manager.addIndexUpdate(table, d2);
    pending.add(d);
    validate(manager, pending);

    // double-deletes of the same row only retain the existing one, which was already canceled out
    // above
    Delete d3 = new Delete(row, ts2);
    manager.addIndexUpdate(table, d3);
    pending.add(d);
    validate(manager, pending);

    // if there is just a put and a delete at the same ts, no pending updates should be returned
    manager = new IndexUpdateManager();
    manager.addIndexUpdate(table, d2);
    manager.addIndexUpdate(table, p);
    validate(manager, Collections.<Mutation> emptyList());

    // different row insertions can be tricky too, if you don't get the base cases right
    manager = new IndexUpdateManager();
    manager.addIndexUpdate(table, p);
    // this row definitely sorts after the current row
    byte[] row1 = Bytes.toBytes("row1");
    Put p1 = new Put(row1, ts1);
    manager.addIndexUpdate(table, p1);
    // this delete should completely cover the given put and both should be removed
    Delete d4 = new Delete(row1, ts1);
    manager.addIndexUpdate(table, d4);
    pending.clear();
    pending.add(p);
    validate(manager, pending);
  }

  private void validate(IndexUpdateManager manager, List<Mutation> pending) {
    for (Pair<Mutation, byte[]> entry : manager.toMap()) {
      assertEquals("Table name didn't match for stored entry!", table, entry.getSecond());
      Mutation m = pending.remove(0);
      // test with == to match the exact entries, Mutation.equals just checks the row
      assertTrue(
        "Didn't get the expected mutation! Expected: " + m + ", but got: " + entry.getFirst(),
        m == entry.getFirst());
    }
    assertTrue("Missing pending updates: " + pending, pending.isEmpty());
  }
}