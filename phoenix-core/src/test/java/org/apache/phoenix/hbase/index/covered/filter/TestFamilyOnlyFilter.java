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
import org.apache.phoenix.hbase.index.covered.filter.FamilyOnlyFilter;
import org.junit.Test;

/**
 * Test that the family only filter only allows a single family through
 */
public class TestFamilyOnlyFilter {

  byte[] row = new byte[] { 'a' };
  byte[] qual = new byte[] { 'b' };
  byte[] val = Bytes.toBytes("val");

  @Test
  public void testPassesFirstFamily() {
    byte[] fam = Bytes.toBytes("fam");
    byte[] fam2 = Bytes.toBytes("fam2");

    FamilyOnlyFilter filter = new FamilyOnlyFilter(fam);

    KeyValue kv = new KeyValue(row, fam, qual, 10, val);
    ReturnCode code = filter.filterKeyValue(kv);
    assertEquals("Didn't pass matching family!", ReturnCode.INCLUDE, code);

    kv = new KeyValue(row, fam2, qual, 10, val);
    code = filter.filterKeyValue(kv);
    assertEquals("Didn't filter out non-matching family!", ReturnCode.SKIP, code);
  }

  @Test
  public void testPassesTargetFamilyAsNonFirstFamily() {
    byte[] fam = Bytes.toBytes("fam");
    byte[] fam2 = Bytes.toBytes("fam2");
    byte[] fam3 = Bytes.toBytes("way_after_family");

    FamilyOnlyFilter filter = new FamilyOnlyFilter(fam2);

    KeyValue kv = new KeyValue(row, fam, qual, 10, val);

    ReturnCode code = filter.filterKeyValue(kv);
    assertEquals("Didn't filter out non-matching family!", ReturnCode.SKIP, code);

    kv = new KeyValue(row, fam2, qual, 10, val);
    code = filter.filterKeyValue(kv);
    assertEquals("Didn't pass matching family", ReturnCode.INCLUDE, code);

    kv = new KeyValue(row, fam3, qual, 10, val);
    code = filter.filterKeyValue(kv);
    assertEquals("Didn't filter out non-matching family!", ReturnCode.SKIP, code);
  }

  @Test
  public void testResetFilter() {
    byte[] fam = Bytes.toBytes("fam");
    byte[] fam2 = Bytes.toBytes("fam2");
    byte[] fam3 = Bytes.toBytes("way_after_family");

    FamilyOnlyFilter filter = new FamilyOnlyFilter(fam2);

    KeyValue kv = new KeyValue(row, fam, qual, 10, val);

    ReturnCode code = filter.filterKeyValue(kv);
    assertEquals("Didn't filter out non-matching family!", ReturnCode.SKIP, code);

    KeyValue accept = new KeyValue(row, fam2, qual, 10, val);
    code = filter.filterKeyValue(accept);
    assertEquals("Didn't pass matching family", ReturnCode.INCLUDE, code);

    kv = new KeyValue(row, fam3, qual, 10, val);
    code = filter.filterKeyValue(kv);
    assertEquals("Didn't filter out non-matching family!", ReturnCode.SKIP, code);

    // we shouldn't match the family again - everything after a switched family should be ignored
    code = filter.filterKeyValue(accept);
    assertEquals("Should have skipped a 'matching' family if it arrives out of order",
      ReturnCode.SKIP, code);

    // reset the filter and we should accept it again
    filter.reset();
    code = filter.filterKeyValue(accept);
    assertEquals("Didn't pass matching family after reset", ReturnCode.INCLUDE, code);
  }
}
