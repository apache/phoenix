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
package org.apache.phoenix.hbase.index.covered.data;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class TestIndexMemStore {

  private static final byte[] row = Bytes.toBytes("row");
  private static final byte[] family = Bytes.toBytes("family");
  private static final byte[] qual = Bytes.toBytes("qual");
  private static final byte[] val = Bytes.toBytes("val");
  private static final byte[] val2 = Bytes.toBytes("val2");

  @Test
  public void testCorrectOverwritting() throws Exception {
    IndexMemStore store = new IndexMemStore(IndexMemStore.COMPARATOR);
    long ts = 10;
    KeyValue kv = new KeyValue(row, family, qual, ts, Type.Put, val);
    kv.setSequenceId(2);
    KeyValue kv2 = new KeyValue(row, family, qual, ts, Type.Put, val2);
    kv2.setSequenceId(0);
    store.add(kv, true);
    // adding the exact same kv shouldn't change anything stored if not overwritting
    store.add(kv2, false);
    KeyValueScanner scanner = store.getScanner();
    KeyValue first = KeyValue.createFirstOnRow(row);
    scanner.seek(first);
    assertTrue("Overwrote kv when specifically not!", kv == scanner.next());
    scanner.close();

    // now when we overwrite, we should get the newer one
    store.add(kv2, true);
    scanner = store.getScanner();
    scanner.seek(first);
    assertTrue("Didn't overwrite kv when specifically requested!", kv2 == scanner.next());
    scanner.close();
  }

  /**
   * We don't expect custom KeyValue creation, so we can't get into weird situations, where a
   * {@link Type#DeleteFamily} has a column qualifier specified.
   * @throws Exception
   */
  @Test
  public void testExpectedOrdering() throws Exception {
    IndexMemStore store = new IndexMemStore();
    KeyValue kv = new KeyValue(row, family, qual, 12, Type.Put, val);
    store.add(kv, true);
    KeyValue kv2 = new KeyValue(row, family, qual, 10, Type.Put, val2);
    store.add(kv2, true);
    KeyValue df = new KeyValue(row, family, null, 11, Type.DeleteFamily, null);
    store.add(df, true);
    KeyValue dc = new KeyValue(row, family, qual, 11, Type.DeleteColumn, null);
    store.add(dc, true);
    KeyValue d = new KeyValue(row, family, qual, 12, Type.Delete, null);
    store.add(d, true);

    // null qualifiers should always sort before the non-null cases
    KeyValueScanner scanner = store.getScanner();
    KeyValue first = KeyValue.createFirstOnRow(row);
    assertTrue("Didn't have any data in the scanner", scanner.seek(first));
    assertTrue("Didn't get delete family first (no qualifier == sort first)", df == scanner.next());
    assertTrue("Didn't get point delete before corresponding put", d == scanner.next());
    assertTrue("Didn't get larger ts Put", kv == scanner.next());
    assertTrue("Didn't get delete column before corresponding put(delete sorts first)",
      dc == scanner.next());
    assertTrue("Didn't get smaller ts Put", kv2 == scanner.next());
    assertNull("Have more data in the scanner", scanner.next());
  }
}