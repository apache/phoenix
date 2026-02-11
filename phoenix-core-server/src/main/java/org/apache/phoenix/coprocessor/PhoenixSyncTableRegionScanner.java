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
package org.apache.phoenix.coprocessor;

import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.*;
import static org.apache.phoenix.query.QueryConstants.AGG_TIMESTAMP;
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN_FAMILY;
import static org.apache.phoenix.schema.types.PDataType.FALSE_BYTES;
import static org.apache.phoenix.schema.types.PDataType.TRUE_BYTES;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.util.PhoenixKeyValueUtil;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Server-side coprocessor that performs chunk formation and SHA-256 hashing for
 * PhoenixSyncTableTool.
 * <p>
 * Accumulates rows into chunks (based on size/row limits) and computes a hash of all row data
 * (keys, column families, qualifiers, timestamps, cell types, values).
 * <p>
 * Source mode (forceFullRange=false): Returns complete chunks bounded by region boundaries. Sets
 * hasMoreRowsInRegion=false when region is exhausted.
 * <p>
 * Target mode (forceFullRange=true): Returns partial chunks with serialized digest state when
 * region boundary is reached, allowing cross-region hash continuation.
 * <p>
 * Returns chunk metadata cells: END_KEY, HASH (or digest state), ROW_COUNT, IS_PARTIAL_CHUNK,
 * HAS_MORE_ROWS_IN_REGION.
 */
public class PhoenixSyncTableRegionScanner extends BaseRegionScanner {

  private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixSyncTableRegionScanner.class);
  private static final byte[] CHUNK_METADATA_FAMILY = SINGLE_COLUMN_FAMILY;
  private static final int MAX_SHA256_DIGEST_STATE_SIZE = 128;
  private final Region region;
  private final Scan scan;
  private final RegionCoprocessorEnvironment env;
  private final UngroupedAggregateRegionObserver ungroupedAggregateRegionObserver;

  private final byte[] mapperRegionEndKey;
  private final long chunkSizeBytes;
  private final boolean forceFullRange;
  private byte[] chunkStartKey = null;
  private byte[] chunkEndKey = null;
  private long currentChunkSize = 0L;
  private long currentChunkRowCount = 0L;
  private SHA256Digest digest;
  private boolean hasMoreRowsInRegion = true;
  private boolean isUsingContinuedDigest; // If target chunk was partial, and we are continuing to
                                          // update digest before calculating checksum

  /**
   * @param innerScanner                     The underlying region scanner
   * @param region                           The region being scanned
   * @param scan                             The scan request
   * @param env                              The coprocessor environment
   * @param ungroupedAggregateRegionObserver Parent observer for region state checks
   */
  @VisibleForTesting
  public PhoenixSyncTableRegionScanner(final RegionScanner innerScanner, final Region region,
    final Scan scan, final RegionCoprocessorEnvironment env,
    final UngroupedAggregateRegionObserver ungroupedAggregateRegionObserver) {
    super(innerScanner);
    this.region = region;
    this.scan = scan;
    this.env = env;
    this.ungroupedAggregateRegionObserver = ungroupedAggregateRegionObserver;

    byte[] mapperEndAttr =
      scan.getAttribute(BaseScannerRegionObserverConstants.SYNC_TABLE_MAPPER_REGION_END_KEY);
    byte[] chunkSizeAttr =
      scan.getAttribute(BaseScannerRegionObserverConstants.SYNC_TABLE_CHUNK_SIZE_BYTES);
    this.mapperRegionEndKey = mapperEndAttr != null ? mapperEndAttr : new byte[0];
    this.chunkSizeBytes = chunkSizeAttr != null
      ? Bytes.toLong(chunkSizeAttr)
      : DEFAULT_PHOENIX_SYNC_TABLE_CHUNK_SIZE_BYTES;
    byte[] forceFullRangeAttr =
      scan.getAttribute(BaseScannerRegionObserverConstants.SYNC_TABLE_FORCE_FULL_RANGE);
    this.forceFullRange = (forceFullRangeAttr != null && Bytes.toBoolean(forceFullRangeAttr));
    // Check if we should continue from a previous digest state (cross-region continuation)
    byte[] continuedDigestStateAttr =
      scan.getAttribute(BaseScannerRegionObserverConstants.SYNC_TABLE_CONTINUED_DIGEST_STATE);
    if (continuedDigestStateAttr != null) {
      try {
        this.digest = decodeDigestState(continuedDigestStateAttr);
        this.isUsingContinuedDigest = true;
      } catch (IOException e) {
        throw new RuntimeException("Failed to restore continued digest state", e);
      }
    } else {
      this.digest = new SHA256Digest();
      this.isUsingContinuedDigest = false;
    }
  }

  /**
   * Accumulates rows into a chunk and returns chunk metadata cells.
   * @param results Output list to populate with chunk metadata cells
   * @return true if more chunks available, false if scanning complete
   */
  @Override
  public boolean next(List<Cell> results) throws IOException {
    region.startRegionOperation();
    try {
      resetChunkState();
      RegionScanner localScanner = delegate;
      synchronized (localScanner) {
        List<Cell> rowCells = new ArrayList<>();
        while (hasMoreRowsInRegion) {
          // Check region state INSIDE loop for long-running scans
          ungroupedAggregateRegionObserver.checkForRegionClosingOrSplitting();
          rowCells.clear();
          hasMoreRowsInRegion = localScanner.nextRaw(rowCells);
          if (rowCells.isEmpty()) {
            break;
          }

          byte[] rowKey = CellUtil.cloneRow(rowCells.get(0));
          long rowSize = calculateRowSize(rowCells);
          addRowToChunk(rowKey, rowCells, rowSize);
          if (!forceFullRange && willExceedChunkLimits(rowSize)) {
            break;
          }
        }
      }
      if (chunkStartKey == null) {
        // LOGGER.error(
        // "Exception during chunk scanning in region {} table {} at chunk startKey: {}, endkey:
        // {})",
        // region.getRegionInfo().getRegionNameAsString(),
        // region.getRegionInfo().getTable().getNameAsString(),
        // chunkStartKey != null ? Bytes.toStringBinary(chunkStartKey) : "null",
        // chunkEndKey != null ? Bytes.toStringBinary(chunkEndKey) : "null");
        // throw new RuntimeException("Intentional error throw");
        return false;
      }

      boolean isPartialChunk = forceFullRange && !hasMoreRowsInRegion
        && Bytes.compareTo(chunkEndKey, mapperRegionEndKey) < 0;
      buildChunkMetadataResult(results, isPartialChunk);
      LOGGER.info("Chunk metadata being sent with startKey {}, endKey {}, forceFullRange {}",
        chunkStartKey, chunkEndKey, forceFullRange);
      return hasMoreRowsInRegion;

    } catch (Throwable t) {
      LOGGER.error(
        "Exception during chunk scanning in region {} table {} at chunk startKey: {}, endkey: {})",
        region.getRegionInfo().getRegionNameAsString(),
        region.getRegionInfo().getTable().getNameAsString(),
        chunkStartKey != null ? Bytes.toStringBinary(chunkStartKey) : "null",
        chunkEndKey != null ? Bytes.toStringBinary(chunkEndKey) : "null", t);
      throw t;
    } finally {
      region.closeRegionOperation();
    }
  }

  @Override
  public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
    return next(result);
  }

  /**
   * Resets chunk state for a new chunk. Note: If this scanner was initialized with continued digest
   * state, the first call to this method will NOT reset the digest, allowing us to continue hashing
   * from the previous region's state.
   */
  private void resetChunkState() {
    chunkStartKey = null;
    chunkEndKey = null;
    currentChunkSize = 0;
    currentChunkRowCount = 0;
    if (!isUsingContinuedDigest) {
      digest.reset();
    }
    isUsingContinuedDigest = false;
  }

  private long calculateRowSize(List<Cell> cells) {
    long size = 0;
    for (Cell cell : cells) {
      size += PrivateCellUtil.estimatedSerializedSizeOf(cell);
    }
    return size;
  }

  private boolean willExceedChunkLimits(long rowSize) {
    return currentChunkSize + rowSize > chunkSizeBytes;
  }

  /**
   * Adds a row to the current chunk and updates digest
   */
  private void addRowToChunk(byte[] rowKey, List<Cell> cells, long rowSize) {
    // Set chunk start key on first row
    if (chunkStartKey == null) {
      chunkStartKey = rowKey;
    }
    chunkEndKey = rowKey;
    currentChunkSize += rowSize;
    currentChunkRowCount++;
    updateDigestWithRow(rowKey, cells);
  }

  /**
   * Updates the SHA-256 digest with data from a row. Hash includes: row key + cell family + cell
   * qualifier + cell timestamp + cell type + cell value. This ensures that any difference in the
   * data will result in different hashes. Optimized to avoid cloning - reads directly from cell's
   * backing arrays (zero-copy).
   */
  private void updateDigestWithRow(byte[] rowKey, List<Cell> cells) {
    digest.update(rowKey, 0, rowKey.length);
    for (Cell cell : cells) {
      digest.update(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
      digest.update(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
      long ts = cell.getTimestamp();
      digest.update((byte) (ts >>> 56));
      digest.update((byte) (ts >>> 48));
      digest.update((byte) (ts >>> 40));
      digest.update((byte) (ts >>> 32));
      digest.update((byte) (ts >>> 24));
      digest.update((byte) (ts >>> 16));
      digest.update((byte) (ts >>> 8));
      digest.update((byte) (ts));

      digest.update(cell.getType().getCode());
      digest.update(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
    }
  }

  /**
   * Encodes a SHA256Digest state to a byte array with length prefix for validation. This
   * production-grade implementation adds security checks for critical deployment: - Length prefix
   * for validation and extensibility - Prevents malicious large allocations - Enables detection of
   * corrupted serialization
   * @param digest The digest whose state should be encoded
   * @return Byte array containing 4-byte length prefix + encoded state
   * @throws IOException if encoding fails
   */
  private byte[] encodeDigestState(SHA256Digest digest) throws IOException {
    byte[] encoded = digest.getEncodedState();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    dos.writeInt(encoded.length);
    dos.write(encoded);
    dos.flush();
    return bos.toByteArray();
  }

  /**
   * Decodes a SHA256Digest state from a byte array.
   * @param encodedState Byte array containing 4-byte length prefix + encoded state
   * @return SHA256Digest restored to the saved state
   * @throws IOException if state is invalid, corrupted, or security checks fail
   */
  private SHA256Digest decodeDigestState(byte[] encodedState) throws IOException {
    if (encodedState == null) {
      String regionName = region.getRegionInfo().getRegionNameAsString();
      String tableName = region.getRegionInfo().getTable().getNameAsString();
      throw new IllegalArgumentException(
        String.format("Invalid encoded digest state in region %s table %s: encodedState is null",
          regionName, tableName));
    }

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(encodedState));
    int stateLength = dis.readInt();
    // Prevent malicious large allocations, hash digest can never go beyond ~96 bytes, giving some
    // buffer upto 128 Bytes
    if (stateLength > MAX_SHA256_DIGEST_STATE_SIZE) {
      String regionName = region.getRegionInfo().getRegionNameAsString();
      String tableName = region.getRegionInfo().getTable().getNameAsString();
      throw new IllegalArgumentException(
        String.format("Invalid SHA256 state length in region %s table %s: %d expected <= %d",
          regionName, tableName, stateLength, MAX_SHA256_DIGEST_STATE_SIZE));
    }
    byte[] state = new byte[stateLength];
    dis.readFully(state);
    return new SHA256Digest(state);
  }

  /**
   * Builds chunk metadata result cells and adds them to the results list. Returns a single
   * "row"[rowkey=chunkStartKey] with multiple cells containing chunk metadata[chunkEndKey,
   * hash/digest, rowCount, hasMoreRowsInRegion, isPartialChunk]. For complete chunks: includes
   * final SHA-256 hash (32 bytes) For partial chunks: includes serialized MessageDigest state for
   * continuation
   * @param results        Output list to populate with chunk metadata cells
   * @param isPartialChunk true if this is a partial chunk (region boundary reached before
   *                       completion)
   */
  private void buildChunkMetadataResult(List<Cell> results, boolean isPartialChunk)
    throws IOException {
    byte[] resultRowKey = this.chunkStartKey;
    results.add(PhoenixKeyValueUtil.newKeyValue(resultRowKey, CHUNK_METADATA_FAMILY,
      BaseScannerRegionObserverConstants.SYNC_TABLE_END_KEY_QUALIFIER, AGG_TIMESTAMP, chunkEndKey));
    results.add(PhoenixKeyValueUtil.newKeyValue(resultRowKey, CHUNK_METADATA_FAMILY,
      BaseScannerRegionObserverConstants.SYNC_TABLE_ROW_COUNT_QUALIFIER, AGG_TIMESTAMP,
      Bytes.toBytes(currentChunkRowCount)));
    results.add(PhoenixKeyValueUtil.newKeyValue(resultRowKey, CHUNK_METADATA_FAMILY,
      BaseScannerRegionObserverConstants.SYNC_TABLE_HAS_MORE_ROWS_IN_REGION_QUALIFIER,
      AGG_TIMESTAMP, Bytes.toBytes(hasMoreRowsInRegion)));
    if (isPartialChunk) {
      // Partial chunk digest
      SHA256Digest cloned = new SHA256Digest(digest);
      byte[] digestState = encodeDigestState(cloned);
      results.add(PhoenixKeyValueUtil.newKeyValue(resultRowKey, CHUNK_METADATA_FAMILY,
        BaseScannerRegionObserverConstants.SYNC_TABLE_IS_PARTIAL_CHUNK_QUALIFIER, AGG_TIMESTAMP,
        TRUE_BYTES));
      results.add(PhoenixKeyValueUtil.newKeyValue(resultRowKey, CHUNK_METADATA_FAMILY,
        BaseScannerRegionObserverConstants.SYNC_TABLE_HASH_QUALIFIER, AGG_TIMESTAMP, digestState));
    } else {
      // Complete chunk - finalize and return hash
      byte[] hash = new byte[digest.getDigestSize()];
      digest.doFinal(hash, 0);
      results.add(PhoenixKeyValueUtil.newKeyValue(resultRowKey, CHUNK_METADATA_FAMILY,
        BaseScannerRegionObserverConstants.SYNC_TABLE_HASH_QUALIFIER, AGG_TIMESTAMP, hash));
      results.add(PhoenixKeyValueUtil.newKeyValue(resultRowKey, CHUNK_METADATA_FAMILY,
        BaseScannerRegionObserverConstants.SYNC_TABLE_IS_PARTIAL_CHUNK_QUALIFIER, AGG_TIMESTAMP,
        FALSE_BYTES));
    }
  }

  @Override
  public void close() throws IOException {
    try {
      super.close();
    } catch (Exception e) {
      LOGGER.error("Error closing PhoenixSyncTableRegionScanner", e);
    }
  }

  // Getters for testing
  @VisibleForTesting
  public long getChunkSizeBytes() {
    return chunkSizeBytes;
  }
}
