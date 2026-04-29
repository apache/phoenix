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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.KeyRange;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PhoenixInputSplitTest {

  /**
   * Helper method to create a Scan with given row boundaries.
   */
  private Scan createScan(byte[] startRow, byte[] stopRow) {
    Scan scan = new Scan();
    scan.withStartRow(startRow, true);
    scan.withStopRow(stopRow, false);
    return scan;
  }

  /**
   * Helper method to serialize and deserialize a PhoenixInputSplit.
   */
  private PhoenixInputSplit serializeAndDeserialize(PhoenixInputSplit split) throws IOException {
    // Serialize
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(baos);
    split.write(out);

    // Deserialize
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInput in = new DataInputStream(bais);
    PhoenixInputSplit deserialized = new PhoenixInputSplit();
    deserialized.readFields(in);

    return deserialized;
  }

  @Test
  public void testStandardConstructorWithSingleScan() {
    List<Scan> scans = new ArrayList<>();
    scans.add(createScan(Bytes.toBytes("a"), Bytes.toBytes("d")));

    PhoenixInputSplit split = new PhoenixInputSplit(scans);

    assertFalse("Should not be coalesced with single scan", split.isCoalesced());
    assertEquals("Should have 1 keyRange", 1, split.getKeyRanges().size());
    assertNotNull("KeyRange should be initialized", split.getKeyRange());
    assertTrue("KeyRange should span scan boundaries",
      Bytes.equals(Bytes.toBytes("a"), split.getKeyRange().getLowerRange()));
    assertTrue("KeyRange should span scan boundaries",
      Bytes.equals(Bytes.toBytes("d"), split.getKeyRange().getUpperRange()));
  }

  @Test(expected = NullPointerException.class)
  public void testStandardConstructorWithNullScans() {
    new PhoenixInputSplit(null);
  }

  @Test(expected = IllegalStateException.class)
  public void testStandardConstructorWithEmptyScans() {
    new PhoenixInputSplit(Collections.emptyList());
  }

  @Test
  public void testCoalescingConstructorWithMultipleRegions()
    throws IOException, InterruptedException {
    List<Scan> scans = new ArrayList<>();
    scans.add(createScan(Bytes.toBytes("a"), Bytes.toBytes("d")));
    scans.add(createScan(Bytes.toBytes("d"), Bytes.toBytes("g")));
    scans.add(createScan(Bytes.toBytes("g"), Bytes.toBytes("j")));

    long splitSize = 3072;
    String regionLocation = "server1:16020";

    // KeyRanges are now automatically derived from scans
    PhoenixInputSplit split = new PhoenixInputSplit(scans, splitSize, regionLocation);

    assertTrue("Should be coalesced with multiple regions", split.isCoalesced());
    assertEquals("Should have 3 keyRanges (derived from scans)", 3, split.getKeyRanges().size());
    assertEquals("Split size should match", splitSize, split.getLength());
    assertArrayEquals("Region location should match", new String[] { regionLocation },
      split.getLocations());

    // Verify keyRanges were derived correctly from scans
    List<KeyRange> keyRanges = split.getKeyRanges();
    assertTrue("First keyRange should match first scan",
      Bytes.equals(Bytes.toBytes("a"), keyRanges.get(0).getLowerRange()));
    assertTrue("First keyRange should match first scan",
      Bytes.equals(Bytes.toBytes("d"), keyRanges.get(0).getUpperRange()));
    assertTrue("Second keyRange should match second scan",
      Bytes.equals(Bytes.toBytes("d"), keyRanges.get(1).getLowerRange()));
    assertTrue("Second keyRange should match second scan",
      Bytes.equals(Bytes.toBytes("g"), keyRanges.get(1).getUpperRange()));
    assertTrue("Third keyRange should match third scan",
      Bytes.equals(Bytes.toBytes("g"), keyRanges.get(2).getLowerRange()));
    assertTrue("Third keyRange should match third scan",
      Bytes.equals(Bytes.toBytes("j"), keyRanges.get(2).getUpperRange()));
  }

  @Test
  public void testSerializationWithSingleRegion() throws IOException, InterruptedException {
    List<Scan> scans = new ArrayList<>();
    scans.add(createScan(Bytes.toBytes("a"), Bytes.toBytes("d")));

    PhoenixInputSplit original = new PhoenixInputSplit(scans, 1024, "server1:16020");

    PhoenixInputSplit deserialized = serializeAndDeserialize(original);

    assertFalse("Should not be coalesced after deserialization", deserialized.isCoalesced());
    assertEquals("Should have 1 keyRange", 1, deserialized.getKeyRanges().size());
    assertEquals("Split size should match", original.getLength(), deserialized.getLength());
    assertArrayEquals("Region location should match", original.getLocations(),
      deserialized.getLocations());
    assertTrue("KeyRange should match", Bytes.equals(original.getKeyRange().getLowerRange(),
      deserialized.getKeyRange().getLowerRange()));
    assertTrue("KeyRange should match", Bytes.equals(original.getKeyRange().getUpperRange(),
      deserialized.getKeyRange().getUpperRange()));
  }

  @Test
  public void testSerializationWithCoalescedSplit() throws IOException, InterruptedException {
    List<Scan> scans = new ArrayList<>();
    scans.add(createScan(Bytes.toBytes("a"), Bytes.toBytes("d")));
    scans.add(createScan(Bytes.toBytes("d"), Bytes.toBytes("g")));
    scans.add(createScan(Bytes.toBytes("g"), Bytes.toBytes("j")));

    // KeyRanges are now automatically derived from scans
    PhoenixInputSplit original = new PhoenixInputSplit(scans, 3072, "server1:16020");

    PhoenixInputSplit deserialized = serializeAndDeserialize(original);

    assertTrue("Should be coalesced after deserialization", deserialized.isCoalesced());
    assertEquals("Should have 3 keyRanges", 3, deserialized.getKeyRanges().size());
    assertEquals("Split size should match", original.getLength(), deserialized.getLength());
    assertArrayEquals("Region location should match", original.getLocations(),
      deserialized.getLocations());

    // Verify all keyRanges are preserved (derived from scans)
    List<KeyRange> originalKeyRanges = original.getKeyRanges();
    List<KeyRange> deserializedKeyRanges = deserialized.getKeyRanges();
    for (int i = 0; i < originalKeyRanges.size(); i++) {
      assertTrue("KeyRange " + i + " should match", Bytes.equals(
        originalKeyRanges.get(i).getLowerRange(), deserializedKeyRanges.get(i).getLowerRange()));
      assertTrue("KeyRange " + i + " should match", Bytes.equals(
        originalKeyRanges.get(i).getUpperRange(), deserializedKeyRanges.get(i).getUpperRange()));
    }
  }

  @Test
  public void testGetLocations() throws IOException, InterruptedException {
    List<Scan> scans =
      Collections.singletonList(createScan(Bytes.toBytes("a"), Bytes.toBytes("d")));

    // Test with null regionLocation
    PhoenixInputSplit split1 = new PhoenixInputSplit(scans, 1024, null);
    assertArrayEquals("Should return empty array for null location", new String[] {},
      split1.getLocations());

    // Test with valid regionLocation
    PhoenixInputSplit split2 = new PhoenixInputSplit(scans, 1024, "server1:16020");
    assertArrayEquals("Should return array with server location", new String[] { "server1:16020" },
      split2.getLocations());
  }
}
