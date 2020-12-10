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

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.covered.filter.NewerTimestampFilter;
import org.junit.Test;

public class TestNewerTimestampFilter {
  byte[] row = new byte[] { 'a' };
  byte[] fam = Bytes.toBytes("family");
  byte[] qual = new byte[] { 'b' };
  byte[] val = Bytes.toBytes("val");

  @Test
  public void testOnlyAllowsOlderTimestamps() {
    long ts = 100;
    NewerTimestampFilter filter = new NewerTimestampFilter(ts);

    KeyValue kv = new KeyValue(row, fam, qual, ts, val);
    assertEquals("Didn't accept kv with matching ts", ReturnCode.INCLUDE, filter.filterKeyValue(kv));

    kv = new KeyValue(row, fam, qual, ts + 1, val);
    assertEquals("Didn't skip kv with greater ts", ReturnCode.SKIP, filter.filterKeyValue(kv));

    kv = new KeyValue(row, fam, qual, ts - 1, val);
    assertEquals("Didn't accept kv with lower ts", ReturnCode.INCLUDE, filter.filterKeyValue(kv));
  }
}