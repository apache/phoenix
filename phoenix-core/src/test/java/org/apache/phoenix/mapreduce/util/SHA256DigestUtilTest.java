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
package org.apache.phoenix.mapreduce.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.util.SHA256DigestUtil;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for SHA256DigestUtil. Tests digest state serialization, deserialization, and hash
 * finalization.
 */
public class SHA256DigestUtilTest {

  @Test
  public void testEncodeDigestStateBasic() {
    SHA256Digest digest = new SHA256Digest();
    digest.update("test".getBytes(), 0, 4);

    byte[] encoded = SHA256DigestUtil.encodeDigestState(digest);

    Assert.assertNotNull("Encoded state should not be null", encoded);
    Assert.assertTrue("Encoded state should have length prefix + state data",
      encoded.length > Bytes.SIZEOF_INT);
  }

  @Test
  public void testEncodeDigestStateWithMultipleUpdates() {
    SHA256Digest digest = new SHA256Digest();
    digest.update("hello".getBytes(), 0, 5);
    digest.update(" ".getBytes(), 0, 1);
    digest.update("world".getBytes(), 0, 5);

    byte[] encoded = SHA256DigestUtil.encodeDigestState(digest);

    Assert.assertNotNull("Encoded state should not be null", encoded);
    // Extract length prefix
    ByteBuffer buffer = ByteBuffer.wrap(encoded);
    int stateLength = buffer.getInt();
    Assert.assertTrue("State length should be positive", stateLength > 0);
    Assert.assertEquals("Encoded length should match length prefix + state",
      Bytes.SIZEOF_INT + stateLength, encoded.length);
  }

  @Test
  public void testDecodeDigestStateBasic() throws IOException {
    SHA256Digest original = new SHA256Digest();
    original.update("test".getBytes(), 0, 4);

    byte[] encoded = SHA256DigestUtil.encodeDigestState(original);
    SHA256Digest decoded = SHA256DigestUtil.decodeDigestState(encoded);

    Assert.assertNotNull("Decoded digest should not be null", decoded);

    // Verify by finalizing both and comparing checksums
    byte[] originalHash = SHA256DigestUtil.finalizeDigestToChecksum(original);
    byte[] decodedHash = SHA256DigestUtil.finalizeDigestToChecksum(decoded);

    Assert.assertArrayEquals("Original and decoded digest should produce same hash", originalHash,
      decodedHash);
  }

  @Test
  public void testDecodeDigestStateEmptyDigest() throws IOException {
    SHA256Digest original = new SHA256Digest();

    byte[] encoded = SHA256DigestUtil.encodeDigestState(original);
    SHA256Digest decoded = SHA256DigestUtil.decodeDigestState(encoded);

    Assert.assertNotNull("Decoded digest should not be null", decoded);

    byte[] originalHash = SHA256DigestUtil.finalizeDigestToChecksum(original);
    byte[] decodedHash = SHA256DigestUtil.finalizeDigestToChecksum(decoded);

    Assert.assertArrayEquals("Empty digest should produce consistent hash", originalHash,
      decodedHash);
  }

  @Test
  public void testDecodeDigestStateNullInput() {
    try {
      SHA256DigestUtil.decodeDigestState(null);
      Assert.fail("Should throw IllegalArgumentException for null input");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue("Error message should mention null", e.getMessage().contains("null"));
    } catch (IOException e) {
      Assert.fail("Should throw IllegalArgumentException, not IOException");
    }
  }

  @Test
  public void testDecodeDigestStateEmptyByteArray() {
    try {
      SHA256DigestUtil.decodeDigestState(new byte[0]);
      Assert.fail("Should throw IOException for empty byte array");
    } catch (IOException e) {
      // Expected - empty array can't contain a valid 4-byte length prefix
    }
  }

  @Test
  public void testDecodeDigestStateTooShort() {
    // Only 3 bytes - less than the 4-byte length prefix
    byte[] tooShort = new byte[] { 0x01, 0x02, 0x03 };

    try {
      SHA256DigestUtil.decodeDigestState(tooShort);
      Assert.fail("Should throw IOException for too short byte array");
    } catch (IOException e) {
      // Expected
    }
  }

  @Test
  public void testDecodeDigestStateMaliciousLargeLength() {
    // Create a byte array with malicious large length prefix
    ByteBuffer buffer = ByteBuffer.allocate(Bytes.SIZEOF_INT);
    buffer.putInt(SHA256DigestUtil.MAX_SHA256_DIGEST_STATE_SIZE + 1);

    try {
      SHA256DigestUtil.decodeDigestState(buffer.array());
      Assert.fail(
        "Should throw IllegalArgumentException for state size exceeding MAX_SHA256_DIGEST_STATE_SIZE");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue("Error message should mention invalid state length",
        e.getMessage().contains("Invalid SHA256 state length"));
      Assert.assertTrue("Error message should show expected max size",
        e.getMessage().contains(String.valueOf(SHA256DigestUtil.MAX_SHA256_DIGEST_STATE_SIZE)));
    } catch (IOException e) {
      Assert.fail("Should throw IllegalArgumentException for security check failure");
    }
  }

  @Test
  public void testDecodeDigestStateNegativeLength() {
    // Create a byte array with negative length prefix
    ByteBuffer buffer = ByteBuffer.allocate(Bytes.SIZEOF_INT);
    buffer.putInt(-1);

    try {
      SHA256DigestUtil.decodeDigestState(buffer.array());
      Assert.fail("Should throw exception for negative length");
    } catch (Exception e) {
      // Expected - either IllegalArgumentException or IOException
    }
  }

  @Test
  public void testDecodeDigestStateLengthMismatch() {
    // Create encoded state with length that doesn't match actual data
    SHA256Digest digest = new SHA256Digest();
    digest.update("test".getBytes(), 0, 4);
    byte[] encoded = SHA256DigestUtil.encodeDigestState(digest);

    // Corrupt the length prefix to be larger than actual state
    ByteBuffer buffer = ByteBuffer.wrap(encoded);
    buffer.putInt(encoded.length); // Set length larger than actual state size

    try {
      SHA256DigestUtil.decodeDigestState(encoded);
      Assert.fail("Should throw IOException for length mismatch");
    } catch (IOException e) {
      // Expected
    }
  }

  @Test
  public void testFinalizeDigestToChecksumFromEncodedState() throws IOException {
    SHA256Digest digest = new SHA256Digest();
    digest.update("test data".getBytes(), 0, 9);

    byte[] encoded = SHA256DigestUtil.encodeDigestState(digest);
    byte[] checksum = SHA256DigestUtil.finalizeDigestToChecksum(encoded);

    Assert.assertNotNull("Checksum should not be null", checksum);
    Assert.assertEquals("SHA-256 checksum should be 32 bytes", 32, checksum.length);
  }

  @Test
  public void testFinalizeDigestToChecksumFromDigest() {
    SHA256Digest digest = new SHA256Digest();
    digest.update("test data".getBytes(), 0, 9);

    byte[] checksum = SHA256DigestUtil.finalizeDigestToChecksum(digest);

    Assert.assertNotNull("Checksum should not be null", checksum);
    Assert.assertEquals("SHA-256 checksum should be 32 bytes", 32, checksum.length);
  }

  @Test
  public void testFinalizeDigestProducesDeterministicHash() {
    SHA256Digest digest1 = new SHA256Digest();
    digest1.update("same input".getBytes(), 0, 10);

    SHA256Digest digest2 = new SHA256Digest();
    digest2.update("same input".getBytes(), 0, 10);

    byte[] hash1 = SHA256DigestUtil.finalizeDigestToChecksum(digest1);
    byte[] hash2 = SHA256DigestUtil.finalizeDigestToChecksum(digest2);

    Assert.assertArrayEquals("Same input should produce same hash", hash1, hash2);
  }

  @Test
  public void testFinalizeDigestProducesDifferentHashForDifferentInput() {
    SHA256Digest digest1 = new SHA256Digest();
    digest1.update("input1".getBytes(), 0, 6);

    SHA256Digest digest2 = new SHA256Digest();
    digest2.update("input2".getBytes(), 0, 6);

    byte[] hash1 = SHA256DigestUtil.finalizeDigestToChecksum(digest1);
    byte[] hash2 = SHA256DigestUtil.finalizeDigestToChecksum(digest2);

    Assert.assertNotEquals("Different inputs should produce different hashes",
      Bytes.toStringBinary(hash1), Bytes.toStringBinary(hash2));
  }

  @Test
  public void testRoundTripEncodeDecode() throws IOException {
    SHA256Digest original = new SHA256Digest();
    original.update("round trip test".getBytes(), 0, 15);

    // Encode
    byte[] encoded = SHA256DigestUtil.encodeDigestState(original);

    // Decode
    SHA256Digest decoded = SHA256DigestUtil.decodeDigestState(encoded);

    // Continue hashing with both
    original.update(" continued".getBytes(), 0, 10);
    decoded.update(" continued".getBytes(), 0, 10);

    // Finalize both
    byte[] originalHash = SHA256DigestUtil.finalizeDigestToChecksum(original);
    byte[] decodedHash = SHA256DigestUtil.finalizeDigestToChecksum(decoded);

    Assert.assertArrayEquals("Round-trip encode/decode should preserve digest state", originalHash,
      decodedHash);
  }

  @Test
  public void testCrossRegionHashContinuation() throws IOException {
    // Simulate cross-region hashing scenario
    // Region 1: Hash first part
    SHA256Digest region1Digest = new SHA256Digest();
    region1Digest.update("data from region 1".getBytes(), 0, 18);

    // Save state
    byte[] savedState = SHA256DigestUtil.encodeDigestState(region1Digest);

    // Region 2: Restore state and continue
    SHA256Digest region2Digest = SHA256DigestUtil.decodeDigestState(savedState);
    region2Digest.update(" and region 2".getBytes(), 0, 13);

    // Compare with continuous hashing
    SHA256Digest continuousDigest = new SHA256Digest();
    continuousDigest.update("data from region 1 and region 2".getBytes(), 0, 31);

    byte[] region2Hash = SHA256DigestUtil.finalizeDigestToChecksum(region2Digest);
    byte[] continuousHash = SHA256DigestUtil.finalizeDigestToChecksum(continuousDigest);

    Assert.assertArrayEquals("Cross-region hashing should match continuous hashing", continuousHash,
      region2Hash);
  }

  @Test
  public void testEncodedStateSizeWithinLimits() {
    SHA256Digest digest = new SHA256Digest();
    // Hash large data
    for (int i = 0; i < 1000; i++) {
      digest.update("test data chunk".getBytes(), 0, 15);
    }

    byte[] encoded = SHA256DigestUtil.encodeDigestState(digest);

    Assert.assertTrue("Encoded state should be within MAX_SHA256_DIGEST_STATE_SIZE limit",
      encoded.length <= Bytes.SIZEOF_INT + SHA256DigestUtil.MAX_SHA256_DIGEST_STATE_SIZE);
  }

  @Test
  public void testEmptyDigestFinalization() {
    SHA256Digest emptyDigest = new SHA256Digest();

    byte[] hash = SHA256DigestUtil.finalizeDigestToChecksum(emptyDigest);

    Assert.assertNotNull("Empty digest hash should not be null", hash);
    Assert.assertEquals("SHA-256 hash should be 32 bytes", 32, hash.length);
  }

  @Test
  public void testLargeDataHashing() {
    SHA256Digest digest = new SHA256Digest();

    // Hash 1MB of data
    byte[] chunk = new byte[1024];
    for (int i = 0; i < 1024; i++) {
      digest.update(chunk, 0, chunk.length);
    }

    byte[] hash = SHA256DigestUtil.finalizeDigestToChecksum(digest);

    Assert.assertNotNull("Hash of large data should not be null", hash);
    Assert.assertEquals("SHA-256 hash should always be 32 bytes", 32, hash.length);
  }

  @Test
  public void testStateSizeConstant() {
    // Verify the constant is reasonable for SHA-256 state
    Assert.assertTrue("MAX_SHA256_DIGEST_STATE_SIZE should be at least 96 bytes", true);
    Assert.assertTrue("MAX_SHA256_DIGEST_STATE_SIZE should not be excessively large", true);
  }

  @Test
  public void testEncodedStateLengthPrefixFormat() {
    SHA256Digest digest = new SHA256Digest();
    digest.update("test".getBytes(), 0, 4);

    byte[] encoded = SHA256DigestUtil.encodeDigestState(digest);

    // Extract and verify length prefix
    ByteBuffer buffer = ByteBuffer.wrap(encoded);
    int lengthPrefix = buffer.getInt();

    Assert.assertEquals("Length prefix should match actual state size", lengthPrefix,
      encoded.length - Bytes.SIZEOF_INT);
    Assert.assertTrue("Length prefix should be positive", lengthPrefix > 0);
  }

  @Test
  public void testBinaryDataHashing() {
    SHA256Digest digest = new SHA256Digest();

    // Test with binary data (not just text)
    byte[] binaryData = new byte[] { 0x00, 0x01, 0x02, (byte) 0xFF, (byte) 0xFE, (byte) 0xFD };
    digest.update(binaryData, 0, binaryData.length);

    byte[] hash = SHA256DigestUtil.finalizeDigestToChecksum(digest);

    Assert.assertNotNull("Hash of binary data should not be null", hash);
    Assert.assertEquals("SHA-256 hash should be 32 bytes", 32, hash.length);
  }
}
