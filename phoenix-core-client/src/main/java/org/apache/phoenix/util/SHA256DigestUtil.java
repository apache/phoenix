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

import java.io.IOException;
import org.bouncycastle.crypto.digests.SHA256Digest;

/**
 * Utility class for SHA-256 digest state serialization and deserialization. We are not using jdk
 * bundled SHA, since their digest can't be serialized/deserialized which is needed for
 * PhoenixSyncTableTool for cross-region hash continuation.
 */
public class SHA256DigestUtil {

  /**
   * Encodes a SHA256Digest state to a byte array.
   * @param digest The digest whose state should be encoded
   * @return Byte array containing the raw BouncyCastle encoded state
   */
  public static byte[] encodeDigestState(SHA256Digest digest) {
    return digest.getEncodedState();
  }

  /**
   * Decodes a SHA256Digest state from a byte array.
   * @param encodedState Byte array containing BouncyCastle encoded digest state
   * @return SHA256Digest restored to the saved state
   * @throws IOException if state is invalid, corrupted
   */
  public static SHA256Digest decodeDigestState(byte[] encodedState) throws IOException {
    if (encodedState == null || encodedState.length == 0) {
      throw new IllegalArgumentException(
        "Invalid encoded digest state: encodedState is null or empty");
    }
    return new SHA256Digest(encodedState);
  }

  /**
   * Decodes a digest state and finalizes it to produce the SHA-256 checksum.
   * @param encodedState Serialized BouncyCastle digest state
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
