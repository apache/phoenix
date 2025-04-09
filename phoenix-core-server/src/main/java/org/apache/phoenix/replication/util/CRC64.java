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

package org.apache.phoenix.replication.util;

/**
 * Computes CRC64 checksums. Uses the same polynomial as Hadoop's implementation for
 * potential compatibility if needed, although the specific use here is internal.
 */
public class CRC64 {
  private static final long POLY = 0xC96C5795D7870F42L; // ECMA-182

  private static final int TABLE_LENGTH = 256;
  private static final long[] TABLE = new long[TABLE_LENGTH];

  static {
    /* Initialize a table constructed from POLY */
    for (int n = 0; n < TABLE_LENGTH; ++n) {
      long crc = n;
      for (int i = 0; i < 8; ++i) {
        if ((crc & 1) == 1) {
          crc = (crc >>> 1) ^ POLY;
        } else {
          crc >>>= 1;
        }
      }
      TABLE[n] = crc;
    }
  }

  private long value = -1; // Initial CRC value is -1

  /**
   * Resets the CRC calculation to the initial value.
   */
  public void reset() {
    value = -1;
  }

  /**
   * Updates the CRC value with a specified portion of a byte array.
   * @param input the byte array to update the CRC value with
   * @param off the start offset of the data
   * @param len the number of bytes to use for the update
   */
  public void update(byte[] input, int off, int len) {
     long crc = value; // Use local variable for performance within the loop
     for (int i = off; i < off + len; i++) {
        // Update CRC calculation using the lookup table
        crc = TABLE[(input[i] ^ (int) crc) & 0xFF] ^ (crc >>> 8);
     }
     value = crc;
  }

  /**
   * Updates the CRC value with an entire byte array.
   * @param input the byte array to update the CRC value with
   */
  public void update(byte[] input) {
    update(input, 0, input.length);
  }

  /**
   * Returns the current CRC value. Note that the final checksum requires inverting the bits.
   * @return the current CRC value (before final inversion)
   */
  public long getValue() {
     // Return the compliment (~ operator) of 'value' to match HDFS CRC64 standard behavior
    return ~value;
  }

}
