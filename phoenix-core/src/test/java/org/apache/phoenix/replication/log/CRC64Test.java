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
package org.apache.phoenix.replication.log;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.nio.charset.StandardCharsets;
import org.junit.Test;

public class CRC64Test {

  /**
   * Test CRC64 calculation against a known value. The value 0x995dc9bbdf1939faL is the expected
   * CRC64-ECMA result for the ASCII string "123456789".
   */
  @Test
  public void testKnownValue() {
    CRC64 crc = new CRC64();
    byte[] input = "123456789".getBytes(StandardCharsets.UTF_8);
    long expectedCRC = 0x995dc9bbdf1939faL; // Standard CRC64-ECMA value for "123456789"
    crc.update(input);
    long actualCRC = crc.getValue();
    assertEquals("CRC64 for '123456789' does not match known value", expectedCRC, actualCRC);
  }

  /**
   * Test CRC64 calculation for an empty input byte array. The expected result for an empty input
   * with initial -1 and final XOR -1 is 0.
   */
  @Test
  public void testEmptyInput() {
    CRC64 crc = new CRC64();
    byte[] emptyInput = new byte[0];
    long expectedCRC = 0L; // ~(-1) = 0
    crc.update(emptyInput);
    long actualCRC = crc.getValue();
    assertEquals("CRC64 for empty input should be 0", expectedCRC, actualCRC);
  }

  /**
   * Test the reset() method to ensure the CRC state is correctly reset.
   */
  @Test
  public void testReset() {
    CRC64 crc = new CRC64();
    byte[] input1 = "TestData1".getBytes(StandardCharsets.UTF_8);
    byte[] input2 = "TestData2".getBytes(StandardCharsets.UTF_8);
    // Calculate CRC for first input
    crc.update(input1);
    long crc1 = crc.getValue();
    // Reset and calculate CRC for second input
    crc.reset();
    crc.update(input2);
    long crc2 = crc.getValue();
    // Ensure the two CRC values are different (unless inputs are identical)
    assertNotEquals("CRCs should be different after reset", crc1, crc2);
    // Recalculate CRC for first input after reset to ensure it matches the original
    crc.reset();
    crc.update(input1);
    long crc1Again = crc.getValue();
    assertEquals("CRC after reset should match original CRC", crc1, crc1Again);
  }

}
