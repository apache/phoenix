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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.junit.Before;
import org.junit.Test;

public class PhoenixNoOpSingleRecordReaderTest {

  private PhoenixNoOpSingleRecordReader reader;

  @Before
  public void setup() {
    reader = new PhoenixNoOpSingleRecordReader();
  }

  @Test
  public void testNextKeyValueReturnsTrueExactlyOnce() {
    assertTrue("First call should return true", reader.nextKeyValue());
    assertFalse("Second call should return false", reader.nextKeyValue());
    assertFalse("Third call should still return false", reader.nextKeyValue());
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
  public void testProgressReflectsRecordConsumption() {
    assertEquals("Progress should be 0.0 before consuming record", 0.0f, reader.getProgress(),
      0.0f);
    reader.nextKeyValue();
    assertEquals("Progress should be 1.0 after consuming record", 1.0f, reader.getProgress(), 0.0f);
  }

  @Test
  public void testInitializeAndCloseDoNotThrow() {
    reader.initialize(null, null);
    reader.close();
  }
}
