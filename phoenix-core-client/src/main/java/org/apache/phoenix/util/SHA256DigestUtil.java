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
package org.apache.phoenix.util;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.util.Bytes;
import org.bouncycastle.crypto.digests.SHA256Digest;

/**
 * Utility class for SHA-256 digest state serialization and deserialization. Used by
 * PhoenixSyncTableTool for cross-region hash continuation.
 */
public class SHA256DigestUtil {

  /**
   * Maximum allowed size for encoded SHA-256 digest state. SHA-256 state is ~96 bytes, we allow up
   * to 128 bytes as buffer.
   */
  public static final int MAX_SHA256_DIGEST_STATE_SIZE = 128;

  private SHA256DigestUtil() {
    // Utility class, no instantiation
  }

  /**
   * Encodes a SHA256Digest state to a byte array with length prefix for validation. Format: [4-byte
   * integer length][encoded digest state bytes]
   * @param digest The digest whose state should be encoded
   * @return Byte array containing integer length prefix + encoded state
   */
  public static byte[] encodeDigestState(SHA256Digest digest) {
    byte[] encoded = digest.getEncodedState();
    ByteBuffer buffer = ByteBuffer.allocate(Bytes.SIZEOF_INT + encoded.length);
    buffer.putInt(encoded.length);
    buffer.put(encoded);
    return buffer.array();
  }

  /**
   * Decodes a SHA256Digest state from a byte array.
   * @param encodedState Byte array containing 4-byte integer length prefix + encoded state
   * @return SHA256Digest restored to the saved state
   * @throws IOException if state is invalid, corrupted, or security checks fail
   */
  public static SHA256Digest decodeDigestState(byte[] encodedState) throws IOException {
    if (encodedState == null) {
      throw new IllegalArgumentException("Invalid encoded digest state: encodedState is null");
    }

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(encodedState));
    int stateLength = dis.readInt();

    // Prevent malicious large allocations
    if (stateLength > MAX_SHA256_DIGEST_STATE_SIZE) {
      throw new IllegalArgumentException(
        String.format("Invalid SHA256 state length: %d, expected <= %d", stateLength,
          MAX_SHA256_DIGEST_STATE_SIZE));
    }

    byte[] state = new byte[stateLength];
    dis.readFully(state);
    return new SHA256Digest(state);
  }

  /**
   * Decodes a digest state and finalizes it to produce the SHA-256 checksum.
   * @param encodedState Serialized digest state (format: [4-byte length][state bytes])
   * @return 32-byte SHA-256 hash
   * @throws IOException if state decoding fails
   */
  public static byte[] finalizeDigestToChecksum(byte[] encodedState) throws IOException {
    SHA256Digest digest = decodeDigestState(encodedState);
    return finalizeDigestToChecksum(digest);
  }

  /**
   * Finalizes a SHA256Digest to produce the final checksum.
   * @param digest The digest to finalize
   * @return 32-byte SHA-256 hash
   */
  public static byte[] finalizeDigestToChecksum(SHA256Digest digest) {
    byte[] hash = new byte[digest.getDigestSize()];
    digest.doFinal(hash, 0);
    return hash;
  }
}
