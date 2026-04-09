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

import static org.apache.phoenix.mapreduce.PhoenixSyncTableTool.DEFAULT_PHOENIX_SYNC_TABLE_CHUNK_SIZE_BYTES;
import static org.apache.phoenix.query.QueryConstants.AGG_TIMESTAMP;
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN_FAMILY;
import static org.apache.phoenix.schema.types.PDataType.FALSE_BYTES;
import static org.apache.phoenix.schema.types.PDataType.TRUE_BYTES;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.PhoenixScannerContext;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.util.PhoenixKeyValueUtil;
import org.apache.phoenix.util.SHA256DigestUtil;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Server-side coprocessor that performs chunk formation and SHA-256 hashing for
 * PhoenixSyncTableTool.
 * <p>
 * Accumulates rows into chunks (based on size limits) and computes a hash of all row data (keys,
 * column families, qualifiers, timestamps, cell types, values). In case of paging timeout, return
 * whatever is accumulated in chunk. If nothing is accumulated return dummy row either with prev
 * result rowKey or max possible key < currentRowKey
 * <p>
 * Source scan (isTargetClusterScan=false): Returns complete chunks(if paging dint timeout) bounded
 * by region boundaries. Sets hasMoreRows=false when region is exhausted.
 * <p>
 * Target scan (isTargetClusterScan=true): Returns partial chunks with serialized digest state when
 * region boundary is reached, allowing cross-region hash continuation.
 * <p>
 * Returns chunk metadata cells: START_KEY, END_KEY, HASH (or digest state), ROW_COUNT,
 * IS_PARTIAL_CHUNK
 */
public class PhoenixSyncTableRegionScanner extends BaseRegionScanner {

  private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixSyncTableRegionScanner.class);
  private static final byte[] CHUNK_METADATA_FAMILY = SINGLE_COLUMN_FAMILY;
  private final Region region;
  private final Scan scan;
  private final RegionCoprocessorEnvironment env;
  private final UngroupedAggregateRegionObserver ungroupedAggregateRegionObserver;
  private final long chunkSizeBytes;
  private boolean isTargetClusterScan = false;
  private boolean hasMoreRows = true;
  private final long pageSizeMs;
  private Chunk currentChunk;

  /**
   * Creates a PhoenixSyncTableRegionScanner for chunk-based hashing.
   * @param innerScanner                     The underlying region scanner
   * @param region                           The region being scanned
   * @param scan                             The scan request
   * @param env                              The coprocessor environment
   * @param ungroupedAggregateRegionObserver Parent observer for region state checks
   * @param pageSizeMs                       Paging timeout in milliseconds
   */
  public PhoenixSyncTableRegionScanner(final RegionScanner innerScanner, final Region region,
    final Scan scan, final RegionCoprocessorEnvironment env,
    final UngroupedAggregateRegionObserver ungroupedAggregateRegionObserver, long pageSizeMs) {
    super(innerScanner);
    this.region = region;
    this.scan = scan;
    this.env = env;
    this.ungroupedAggregateRegionObserver = ungroupedAggregateRegionObserver;
    byte[] chunkSizeAttr =
      scan.getAttribute(BaseScannerRegionObserverConstants.SYNC_TABLE_CHUNK_SIZE_BYTES);
    if (chunkSizeAttr == null) { // Since we don't set chunk size scan attr for target cluster scan
      this.isTargetClusterScan = true;
    }
    this.chunkSizeBytes = chunkSizeAttr != null
      ? Bytes.toLong(chunkSizeAttr)
      : DEFAULT_PHOENIX_SYNC_TABLE_CHUNK_SIZE_BYTES;
    this.pageSizeMs = pageSizeMs;
  }

  @Override
  public boolean next(List<Cell> results) throws IOException {
    return next(results, null);
  }

  /**
   * Accumulates rows into a chunk and returns chunk metadata cells. Supports server-side paging via
   * {@link PhoenixScannerContext} following the same pattern as
   * {@link GroupedAggregateRegionObserver} and {@link UncoveredIndexRegionScanner}.
   * @param results        Output list to populate with chunk metadata cells
   * @param scannerContext Phoenix scanner context for paging timeout detection
   * @return true if more chunks available, false if scanning complete
   */
  @Override
  public boolean next(List<Cell> results, ScannerContext scannerContext) throws IOException {
    region.startRegionOperation();
    try {
      currentChunk = createNewChunk();
      RegionScanner localScanner = delegate;
      synchronized (localScanner) {
        List<Cell> rowCells = new ArrayList<>();
        while (hasMoreRows) {
          ungroupedAggregateRegionObserver.checkForRegionClosingOrSplitting();
          rowCells.clear();
          hasMoreRows = (scannerContext == null)
            ? localScanner.nextRaw(rowCells)
            : localScanner.nextRaw(rowCells, scannerContext);

          if (rowCells.isEmpty()) {
            break;
          }

          currentChunk.addRow(rowCells);
          if (!isTargetClusterScan && currentChunk.exceedsSize(chunkSizeBytes)) {
            break;
          }

          if (
            hasMoreRows && (PhoenixScannerContext.isReturnImmediately(scannerContext)
              || PhoenixScannerContext.isTimedOut(scannerContext, pageSizeMs))
          ) {
            LOGGER.info("Paging timeout after {} rows ({} bytes) in region {}, chunk [{}:{}]",
              currentChunk.getRowCount(), currentChunk.getSize(),
              region.getRegionInfo().getRegionNameAsString(),
              Bytes.toStringBinary(currentChunk.getStartKey()),
              Bytes.toStringBinary(currentChunk.getEndKey()));
            PhoenixScannerContext.setReturnImmediately(scannerContext);
            break;
          }
        }
      }

      if (currentChunk.isEmpty()) {
        return false;
      }
      buildChunkMetadataResult(results, currentChunk, isTargetClusterScan);
      return hasMoreRows;
    } catch (Throwable t) {
      LOGGER.error(
        "Exception during chunk scanning in region {} table {} at chunk startKey: {}, endKey: {})",
        region.getRegionInfo().getRegionNameAsString(),
        region.getRegionInfo().getTable().getNameAsString(),
        currentChunk != null && !currentChunk.isEmpty()
          ? Bytes.toStringBinary(currentChunk.getStartKey())
          : "null",
        currentChunk != null && !currentChunk.isEmpty()
          ? Bytes.toStringBinary(currentChunk.getEndKey())
          : "null",
        t);
      throw t;
    } finally {
      region.closeRegionOperation();
    }
  }

  /**
   * Creates a new chunk, with continued digest for first chunk if applicable. Only the very first
   * chunk (when previousResultRowKey is null) can have a continued digest. All subsequent chunks
   * get a fresh digest.
   */
  private Chunk createNewChunk() {
    byte[] continuedDigestStateAttr =
      scan.getAttribute(BaseScannerRegionObserverConstants.SYNC_TABLE_CONTINUED_DIGEST_STATE);
    if (continuedDigestStateAttr != null) {
      try {
        SHA256Digest continuedDigest = SHA256DigestUtil.decodeDigestState(continuedDigestStateAttr);
        return new Chunk(continuedDigest);
      } catch (IOException e) {
        throw new IllegalStateException("Failed to restore continued digest state", e);
      }
    }
    return new Chunk(null);
  }

  /**
   * Builds chunk metadata result cells and adds them to the results list. Returns a single
   * "row"[rowKey=chunkEndKey] with multiple cells containing chunk metadata[chunkStartKey,
   * hash/digest, rowCount, isPartialChunk]. For complete chunks: includes final SHA-256 hash (32
   * bytes) For partial chunks: includes serialized MessageDigest state for continuation
   * @param results             Output list to populate with chunk metadata cells
   * @param chunk               The chunk to build metadata for
   * @param isTargetClusterScan Whether this is a target cluster scan (partial chunk)
   */
  static void buildChunkMetadataResult(List<Cell> results, Chunk chunk,
    boolean isTargetClusterScan) {
    byte[] resultRowKey = chunk.getEndKey();
    // If we are scanning target cluster, we always return partial chunk digest
    // and compute final checksum in Mapper itself when scanner.next() returns null
    boolean isPartialChunk = isTargetClusterScan;
    results.add(PhoenixKeyValueUtil.newKeyValue(resultRowKey, CHUNK_METADATA_FAMILY,
      BaseScannerRegionObserverConstants.SYNC_TABLE_START_KEY_QUALIFIER, AGG_TIMESTAMP,
      chunk.getStartKey()));
    results.add(PhoenixKeyValueUtil.newKeyValue(resultRowKey, CHUNK_METADATA_FAMILY,
      BaseScannerRegionObserverConstants.SYNC_TABLE_ROW_COUNT_QUALIFIER, AGG_TIMESTAMP,
      Bytes.toBytes(chunk.getRowCount())));
    if (isPartialChunk) {
      // Partial chunk - return serialized digest state
      byte[] digestState = chunk.getDigestState();
      results.add(PhoenixKeyValueUtil.newKeyValue(resultRowKey, CHUNK_METADATA_FAMILY,
        BaseScannerRegionObserverConstants.SYNC_TABLE_IS_PARTIAL_CHUNK_QUALIFIER, AGG_TIMESTAMP,
        TRUE_BYTES));
      results.add(PhoenixKeyValueUtil.newKeyValue(resultRowKey, CHUNK_METADATA_FAMILY,
        BaseScannerRegionObserverConstants.SYNC_TABLE_HASH_QUALIFIER, AGG_TIMESTAMP, digestState));
    } else {
      // Complete chunk - finalize and return hash
      byte[] hash = chunk.finalizeHash();
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

  /**
   * Encapsulates a chunk of rows being accumulated for sync verification. Manages chunk boundaries,
   * size tracking, row counting, and SHA-256 digest computation.
   */
  static class Chunk {
    private byte[] startKey;
    private byte[] endKey;
    private long size;
    private long rowCount;
    private final SHA256Digest digest;

    /**
     * Creates a new chunk with optional continued digest state.
     * @param continuedDigest If not null, continues hashing from previous chunk's state (used for
     *                        target scans spanning multiple regions)
     */
    Chunk(SHA256Digest continuedDigest) {
      this.digest = continuedDigest != null ? continuedDigest : new SHA256Digest();
      this.size = 0;
      this.rowCount = 0;
    }

    /**
     * Adds a row to this chunk, updating size, count, and digest.
     */
    void addRow(List<Cell> cells) {
      byte[] rowKey = CellUtil.cloneRow(cells.get(0));
      if (startKey == null) {
        startKey = rowKey;
      }
      endKey = rowKey;
      size += calculateRowSize(cells);
      rowCount++;
      updateDigest(rowKey, cells);
    }

    private long calculateRowSize(List<Cell> cells) {
      long rowSize = 0;
      for (Cell cell : cells) {
        rowSize += cell.getSerializedSize();
      }
      return rowSize;
    }

    /**
     * Updates the SHA-256 digest with data from a row. Hash includes: row key + cell family + cell
     * qualifier + cell timestamp + cell type + cell value. This ensures that any difference in the
     * data will result in different hashes.
     */
    private void updateDigest(byte[] rowKey, List<Cell> cells) {
      digest.update(rowKey, 0, rowKey.length);
      byte[] timestampBuffer = new byte[8];
      for (Cell cell : cells) {
        digest.update(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
        digest.update(cell.getQualifierArray(), cell.getQualifierOffset(),
          cell.getQualifierLength());
        long ts = cell.getTimestamp();
        Bytes.putLong(timestampBuffer, 0, ts);
        digest.update(timestampBuffer, 0, 8);
        digest.update(cell.getType().getCode());
        digest.update(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
      }
    }

    /**
     * Finalizes the digest and returns the SHA-256 checksum. For complete chunks only.
     */
    byte[] finalizeHash() {
      return SHA256DigestUtil.finalizeDigestToChecksum(digest);
    }

    /**
     * Returns the serialized digest state for cross-region continuation. For partial chunks only
     */
    byte[] getDigestState() {
      return SHA256DigestUtil.encodeDigestState(digest);
    }

    boolean isEmpty() {
      return startKey == null;
    }

    boolean exceedsSize(long limit) {
      return size > limit;
    }

    byte[] getStartKey() {
      return startKey;
    }

    byte[] getEndKey() {
      return endKey;
    }

    long getSize() {
      return size;
    }

    long getRowCount() {
      return rowCount;
    }
  }
}
