/*
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
package org.apache.phoenix.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.phoenix.query.KeyRange;
import org.junit.Before;
import org.junit.Test;

public class PhoenixNoOpPerRangeRecordReaderTest {

  private PhoenixNoOpPerRangeRecordReader reader;

  @Before
  public void setup() {
    reader = new PhoenixNoOpPerRangeRecordReader();
  }

  @Test
  public void testNextKeyValueDefaultsToOneRangeWhenNotInitialized() {
    assertTrue("First call should return true", reader.nextKeyValue());
    assertFalse("Second call should return false", reader.nextKeyValue());
    assertFalse("Third call should still return false", reader.nextKeyValue());
  }

  @Test
  public void testNextKeyValueReturnsOncePerRangeForSingleRangeSplit() {
    reader.initialize(splitWithRanges(range("a", "b")), null);

    assertTrue("First call should return true", reader.nextKeyValue());
    assertFalse("Second call should return false", reader.nextKeyValue());
  }

  @Test
  public void testNextKeyValueReturnsOncePerRangeForCoalescedSplit() {
    reader.initialize(splitWithRanges(range("a", "b"), range("b", "c"), range("c", "d")), null);

    assertTrue("Call 1 should return true", reader.nextKeyValue());
    assertTrue("Call 2 should return true", reader.nextKeyValue());
    assertTrue("Call 3 should return true", reader.nextKeyValue());
    assertFalse("Call 4 should return false", reader.nextKeyValue());
  }

  @Test
  public void testGetCurrentKeyReturnsNullWritable() {
    NullWritable key = reader.getCurrentKey();
    assertNotNull(key);
    assertEquals(NullWritable.get(), key);
  }

  @Test
  public void testGetCurrentValueReturnsNullDBWritable() {
    DBWritable value = reader.getCurrentValue();
    assertNotNull(value);
  }

  @Test
  public void testProgressReflectsRangeConsumption() {
    reader.initialize(
      splitWithRanges(range("a", "b"), range("b", "c"), range("c", "d"), range("d", "e")), null);

    assertEquals("0/4 ranges consumed", 0.0f, reader.getProgress(), 0.0001f);
    reader.nextKeyValue();
    assertEquals("1/4 ranges consumed", 0.25f, reader.getProgress(), 0.0001f);
    reader.nextKeyValue();
    assertEquals("2/4 ranges consumed", 0.50f, reader.getProgress(), 0.0001f);
    reader.nextKeyValue();
    assertEquals("3/4 ranges consumed", 0.75f, reader.getProgress(), 0.0001f);
    reader.nextKeyValue();
    assertEquals("4/4 ranges consumed", 1.0f, reader.getProgress(), 0.0001f);
  }

  @Test
  public void testProgressDefaultsBeforeInitialize() {
    assertEquals("Progress should be 0.0 before consuming record", 0.0f, reader.getProgress(),
      0.0f);
    reader.nextKeyValue();
    assertEquals("Progress should be 1.0 after consuming record", 1.0f, reader.getProgress(), 0.0f);
  }

  @Test
  public void testInitializeIsTolerantOfNonPhoenixSplit() {
    reader.initialize(mock(InputSplit.class), null);
    assertTrue("Should still emit one record by default", reader.nextKeyValue());
    assertFalse(reader.nextKeyValue());
  }

  @Test
  public void testInitializeWithEmptyKeyRangesFallsBackToSingleRecord() {
    PhoenixInputSplit split = mock(PhoenixInputSplit.class);
    when(split.getKeyRanges()).thenReturn(Collections.emptyList());
    reader.initialize(split, null);

    assertTrue("Should still emit one record by default", reader.nextKeyValue());
    assertFalse(reader.nextKeyValue());
  }

  @Test
  public void testCloseDoesNotThrow() {
    reader.close();
  }

  private PhoenixInputSplit splitWithRanges(KeyRange... ranges) {
    PhoenixInputSplit split = mock(PhoenixInputSplit.class);
    List<KeyRange> rangeList = new ArrayList<>(ranges.length);
    Collections.addAll(rangeList, ranges);
    when(split.getKeyRanges()).thenReturn(rangeList);
    return split;
  }

  private KeyRange range(String start, String stop) {
    return KeyRange.getKeyRange(Bytes.toBytes(start), Bytes.toBytes(stop));
  }
}
