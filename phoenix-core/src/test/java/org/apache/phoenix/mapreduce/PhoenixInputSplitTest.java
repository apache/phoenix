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

import static org.junit.Assert.*;

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

/**
 * Unit tests for PhoenixInputSplit. Tests standard constructor, coalescing constructor,
 * serialization, and getter methods.
 */
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
   * Helper method to create a KeyRange with given boundaries.
   */
  private KeyRange createKeyRange(byte[] start, byte[] end) {
    return KeyRange.getKeyRange(start, true, end, false);
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

  // ============================================================================
  // Standard Constructor Tests
  // ============================================================================

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

  @Test
  public void testStandardConstructorWithMultipleScans() {
    List<Scan> scans = new ArrayList<>();
    scans.add(createScan(Bytes.toBytes("a"), Bytes.toBytes("d")));
    scans.add(createScan(Bytes.toBytes("d"), Bytes.toBytes("g")));
    scans.add(createScan(Bytes.toBytes("g"), Bytes.toBytes("j")));

    PhoenixInputSplit split = new PhoenixInputSplit(scans, 1024, "server1:16020");

    assertFalse("Should not be coalesced (keyRanges not provided)", split.isCoalesced());
    assertEquals("Should have 1 keyRange", 1, split.getKeyRanges().size());
    assertNotNull("KeyRange should be initialized", split.getKeyRange());
    // KeyRange should span from first scan start to last scan stop
    assertTrue("KeyRange should span from first to last scan",
      Bytes.equals(Bytes.toBytes("a"), split.getKeyRange().getLowerRange()));
    assertTrue("KeyRange should span from first to last scan",
      Bytes.equals(Bytes.toBytes("j"), split.getKeyRange().getUpperRange()));
  }

  @Test(expected = NullPointerException.class)
  public void testStandardConstructorWithNullScans() {
    new PhoenixInputSplit(null);
  }

  @Test(expected = IllegalStateException.class)
  public void testStandardConstructorWithEmptyScans() {
    new PhoenixInputSplit(Collections.emptyList());
  }

  // ============================================================================
  // Coalescing Constructor Tests
  // ============================================================================

  @Test
  public void testCoalescingConstructorWithMultipleRegions()
    throws IOException, InterruptedException {
    List<Scan> scans = new ArrayList<>();
    scans.add(createScan(Bytes.toBytes("a"), Bytes.toBytes("d")));
    scans.add(createScan(Bytes.toBytes("d"), Bytes.toBytes("g")));
    scans.add(createScan(Bytes.toBytes("g"), Bytes.toBytes("j")));

    List<KeyRange> keyRanges = new ArrayList<>();
    keyRanges.add(createKeyRange(Bytes.toBytes("a"), Bytes.toBytes("d")));
    keyRanges.add(createKeyRange(Bytes.toBytes("d"), Bytes.toBytes("g")));
    keyRanges.add(createKeyRange(Bytes.toBytes("g"), Bytes.toBytes("j")));

    long splitSize = 3072;
    String regionLocation = "server1:16020";

    PhoenixInputSplit split = new PhoenixInputSplit(scans, keyRanges, splitSize, regionLocation);

    assertTrue("Should be coalesced with multiple regions", split.isCoalesced());
    assertEquals("Should have 3 keyRanges", 3, split.getKeyRanges().size());
    assertEquals("Split size should match", splitSize, split.getLength());
    assertArrayEquals("Region location should match", new String[] { regionLocation },
      split.getLocations());
  }

  @Test(expected = NullPointerException.class)
  public void testCoalescingConstructorWithNullKeyRanges() {
    List<Scan> scans = new ArrayList<>();
    scans.add(createScan(Bytes.toBytes("a"), Bytes.toBytes("d")));

    new PhoenixInputSplit(scans, null, 1024, "server1:16020");
  }

  @Test(expected = IllegalStateException.class)
  public void testCoalescingConstructorWithEmptyKeyRanges() {
    List<Scan> scans = new ArrayList<>();
    scans.add(createScan(Bytes.toBytes("a"), Bytes.toBytes("d")));

    new PhoenixInputSplit(scans, Collections.emptyList(), 1024, "server1:16020");
  }

  @Test
  public void testCoalescingConstructorValidatesSizeMismatch() {
    List<Scan> scans = new ArrayList<>();
    scans.add(createScan(Bytes.toBytes("a"), Bytes.toBytes("d")));
    scans.add(createScan(Bytes.toBytes("d"), Bytes.toBytes("g")));
    scans.add(createScan(Bytes.toBytes("g"), Bytes.toBytes("j")));

    List<KeyRange> keyRanges = new ArrayList<>();
    keyRanges.add(createKeyRange(Bytes.toBytes("a"), Bytes.toBytes("d")));
    keyRanges.add(createKeyRange(Bytes.toBytes("d"), Bytes.toBytes("g")));
    // Only 2 keyRanges for 3 scans

    try {
      new PhoenixInputSplit(scans, keyRanges, 1024, "server1:16020");
      fail("Should have thrown IllegalStateException for size mismatch");
    } catch (IllegalStateException e) {
      assertTrue("Error message should mention size mismatch",
        e.getMessage().contains("Number of scans must match number of keyRanges"));
    }
  }

  // ============================================================================
  // Serialization Tests
  // ============================================================================

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

    List<KeyRange> keyRanges = new ArrayList<>();
    keyRanges.add(createKeyRange(Bytes.toBytes("a"), Bytes.toBytes("d")));
    keyRanges.add(createKeyRange(Bytes.toBytes("d"), Bytes.toBytes("g")));
    keyRanges.add(createKeyRange(Bytes.toBytes("g"), Bytes.toBytes("j")));

    PhoenixInputSplit original = new PhoenixInputSplit(scans, keyRanges, 3072, "server1:16020");

    PhoenixInputSplit deserialized = serializeAndDeserialize(original);

    assertTrue("Should be coalesced after deserialization", deserialized.isCoalesced());
    assertEquals("Should have 3 keyRanges", 3, deserialized.getKeyRanges().size());
    assertEquals("Split size should match", original.getLength(), deserialized.getLength());
    assertArrayEquals("Region location should match", original.getLocations(),
      deserialized.getLocations());

    // Verify all keyRanges are preserved
    List<KeyRange> deserializedKeyRanges = deserialized.getKeyRanges();
    for (int i = 0; i < keyRanges.size(); i++) {
      assertTrue("KeyRange " + i + " should match", Bytes.equals(keyRanges.get(i).getLowerRange(),
        deserializedKeyRanges.get(i).getLowerRange()));
      assertTrue("KeyRange " + i + " should match", Bytes.equals(keyRanges.get(i).getUpperRange(),
        deserializedKeyRanges.get(i).getUpperRange()));
    }
  }

  // ============================================================================
  // Getter Method Tests
  // ============================================================================

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
