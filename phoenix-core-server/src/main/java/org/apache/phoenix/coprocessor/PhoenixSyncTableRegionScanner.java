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

import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.DEFAULT_PHOENIX_SYNC_TABLE_CHUNK_SIZE_BYTES;
import static org.apache.phoenix.query.QueryConstants.AGG_TIMESTAMP;
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN_FAMILY;
import static org.apache.phoenix.schema.types.PDataType.FALSE_BYTES;
import static org.apache.phoenix.schema.types.PDataType.TRUE_BYTES;
import static org.apache.phoenix.util.ScanUtil.getDummyResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.PhoenixScannerContext;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PhoenixKeyValueUtil;
import org.apache.phoenix.util.SHA256DigestUtil;
import org.apache.phoenix.util.ScanUtil;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Server-side coprocessor that performs chunk formation and SHA-256 hashing for
 * PhoenixSyncTableTool.
 * <p>
 * Accumulates rows into chunks (based on size limits) and computes a hash of all row data (keys,
 * column families, qualifiers, timestamps, cell types, values).
 * <p>
 * Source scan (isTargetScan=false): Returns complete chunks bounded by region boundaries. Sets
 * hasMoreRows=false when region is exhausted.
 * <p>
 * Target scan (isTargetScan=true): Returns partial chunks with serialized digest state when region
 * boundary is reached, allowing cross-region hash continuation.
 * <p>
 * Returns chunk metadata cells: END_KEY, HASH (or digest state), ROW_COUNT, IS_PARTIAL_CHUNK
 */
public class PhoenixSyncTableRegionScanner extends BaseRegionScanner {

  private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixSyncTableRegionScanner.class);
  private static final byte[] CHUNK_METADATA_FAMILY = SINGLE_COLUMN_FAMILY;
  private final Region region;
  private final Scan scan;
  private final RegionCoprocessorEnvironment env;
  private final UngroupedAggregateRegionObserver ungroupedAggregateRegionObserver;
  private final long chunkSizeBytes;
  private boolean isTargetScan = false;
  private byte[] chunkStartKey = null;
  private byte[] chunkEndKey = null;
  private long currentChunkSize = 0L;
  private long currentChunkRowCount = 0L;
  // We are not using jdk bundled SHA, since their digest can't be serialized/deserialized
  // which is needed for passing around partial chunk
  private final SHA256Digest digest;
  private boolean hasMoreRows = true;
  // If target chunk was partial, and we are continuing to
  // update digest before calculating checksum
  private boolean isUsingContinuedDigest;
  private byte[] previousResultRowKey = null;
  private final byte[] initStartRowKey;
  private final boolean includeInitStartRowKey;
  private final long pageSizeMs;

  /**
   * Creates a PhoenixSyncTableRegionScanner for chunk-based hashing.
   * @param innerScanner                     The underlying region scanner
   * @param region                           The region being scanned
   * @param scan                             The scan request
   * @param env                              The coprocessor environment
   * @param ungroupedAggregateRegionObserver Parent observer for region state checks
   * @throws IllegalStateException if digest state restoration fails
   */
  @VisibleForTesting
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
      this.isTargetScan = true;
    }
    this.chunkSizeBytes = chunkSizeAttr != null
      ? Bytes.toLong(chunkSizeAttr)
      : DEFAULT_PHOENIX_SYNC_TABLE_CHUNK_SIZE_BYTES;

    // Check if we should continue from a previous digest state (cross-region continuation)
    byte[] continuedDigestStateAttr =
      scan.getAttribute(BaseScannerRegionObserverConstants.SYNC_TABLE_CONTINUED_DIGEST_STATE);
    if (continuedDigestStateAttr != null) {
      try {
        this.digest = SHA256DigestUtil.decodeDigestState(continuedDigestStateAttr);
        this.isUsingContinuedDigest = true;
      } catch (IOException e) {
        throw new IllegalStateException("Failed to restore continued digest state", e);
      }
    } else {
      this.digest = new SHA256Digest();
      this.isUsingContinuedDigest = false;
    }
    this.initStartRowKey = scan.getStartRow();
    this.includeInitStartRowKey = scan.includeStartRow();
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
      resetChunkState();
      RegionScanner localScanner = delegate;
      synchronized (localScanner) {
        List<Cell> rowCells = new ArrayList<>();
        while (hasMoreRows) {
          ungroupedAggregateRegionObserver.checkForRegionClosingOrSplitting();
          rowCells.clear();
          hasMoreRows = (scannerContext == null)
            ? localScanner.nextRaw(rowCells)
            : localScanner.nextRaw(rowCells, scannerContext);

          if (!rowCells.isEmpty() && ScanUtil.isDummy(rowCells)) {
            if (chunkStartKey == null) {
              updateDummyWithPrevRowKey(results, initStartRowKey, includeInitStartRowKey, scan);
              return true;
            } else {
              break;
            }
          }

          if (rowCells.isEmpty()) {
            break;
          }

          byte[] rowKey = CellUtil.cloneRow(rowCells.get(0));
          long rowSize = calculateRowSize(rowCells);
          addRowToChunk(rowKey, rowCells, rowSize);
          if (!isTargetScan && willExceedChunkLimits(rowSize)) {
            break;
          }
          if (
            hasMoreRows && (PhoenixScannerContext.isReturnImmediately(scannerContext)
              || PhoenixScannerContext.isTimedOut(scannerContext, pageSizeMs))
          ) {
            LOGGER.info("Paging timeout after {} rows ({} bytes) in region {}, chunk [{}:{}]",
              currentChunkRowCount, currentChunkSize,
              region.getRegionInfo().getRegionNameAsString(), Bytes.toStringBinary(chunkStartKey),
              Bytes.toStringBinary(chunkEndKey));
            PhoenixScannerContext.setReturnImmediately(scannerContext);
            break;
          }
        }
      }
      if (chunkStartKey == null) {
        return false;
      }

      buildChunkMetadataResult(results, isTargetScan);
      previousResultRowKey = chunkEndKey;
      return hasMoreRows;
    } catch (Throwable t) {
      LOGGER.error(
        "Exception during chunk scanning in region {} table {} at chunk startKey: {}, endKey: {})",
        region.getRegionInfo().getRegionNameAsString(),
        region.getRegionInfo().getTable().getNameAsString(),
        chunkStartKey != null ? Bytes.toStringBinary(chunkStartKey) : "null",
        chunkEndKey != null ? Bytes.toStringBinary(chunkEndKey) : "null", t);
      throw t;
    } finally {
      region.closeRegionOperation();
    }
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
   * data will result in different hashes.
   */
  private void updateDigestWithRow(byte[] rowKey, List<Cell> cells) {
    digest.update(rowKey, 0, rowKey.length);
    byte[] timestampBuffer = new byte[8];
    for (Cell cell : cells) {
      digest.update(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
      digest.update(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
      long ts = cell.getTimestamp();
      Bytes.putLong(timestampBuffer, 0, ts);
      digest.update(timestampBuffer, 0, 8);
      digest.update(cell.getType().getCode());
      digest.update(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
    }
  }

  /**
   * Builds chunk metadata result cells and adds them to the results list. Returns a single
   * "row"[rowKey=chunkEndKey] with multiple cells containing chunk metadata[chunkStartKey,
   * hash/digest, rowCount, isPartialChunk]. For complete chunks: includes final SHA-256 hash (32
   * bytes) For partial chunks: includes serialized MessageDigest state for continuation
   * @param results        Output list to populate with chunk metadata cells
   * @param isPartialChunk true if this is a partial chunk (region boundary reached before
   *                       completion)
   */
  private void buildChunkMetadataResult(List<Cell> results, boolean isPartialChunk)
    throws IOException {
    byte[] resultRowKey = this.chunkEndKey;
    results.add(PhoenixKeyValueUtil.newKeyValue(resultRowKey, CHUNK_METADATA_FAMILY,
      BaseScannerRegionObserverConstants.SYNC_TABLE_START_KEY_QUALIFIER, AGG_TIMESTAMP,
      chunkStartKey));
    results.add(PhoenixKeyValueUtil.newKeyValue(resultRowKey, CHUNK_METADATA_FAMILY,
      BaseScannerRegionObserverConstants.SYNC_TABLE_ROW_COUNT_QUALIFIER, AGG_TIMESTAMP,
      Bytes.toBytes(currentChunkRowCount)));
    if (isPartialChunk) {
      // Partial chunk digest
      byte[] digestState = SHA256DigestUtil.encodeDigestState(digest);
      results.add(PhoenixKeyValueUtil.newKeyValue(resultRowKey, CHUNK_METADATA_FAMILY,
        BaseScannerRegionObserverConstants.SYNC_TABLE_IS_PARTIAL_CHUNK_QUALIFIER, AGG_TIMESTAMP,
        TRUE_BYTES));
      results.add(PhoenixKeyValueUtil.newKeyValue(resultRowKey, CHUNK_METADATA_FAMILY,
        BaseScannerRegionObserverConstants.SYNC_TABLE_HASH_QUALIFIER, AGG_TIMESTAMP, digestState));
    } else {
      // Complete chunk - finalize and return hash
      byte[] hash = SHA256DigestUtil.finalizeDigestToChecksum(digest);
      results.add(PhoenixKeyValueUtil.newKeyValue(resultRowKey, CHUNK_METADATA_FAMILY,
        BaseScannerRegionObserverConstants.SYNC_TABLE_HASH_QUALIFIER, AGG_TIMESTAMP, hash));
      results.add(PhoenixKeyValueUtil.newKeyValue(resultRowKey, CHUNK_METADATA_FAMILY,
        BaseScannerRegionObserverConstants.SYNC_TABLE_IS_PARTIAL_CHUNK_QUALIFIER, AGG_TIMESTAMP,
        FALSE_BYTES));
    }
  }

  /**
   * Add dummy cell to the result list based on either the previous rowKey returned to the client or
   * the start rowKey and start rowKey include params.
   * @param result                 result to add the dummy cell to.
   * @param initStartRowKey        scan start rowKey.
   * @param includeInitStartRowKey scan start rowKey included.
   * @param scan                   scan object.
   */
  private void updateDummyWithPrevRowKey(List<Cell> result, byte[] initStartRowKey,
    boolean includeInitStartRowKey, Scan scan) {
    result.clear();
    if (previousResultRowKey != null) {
      getDummyResult(previousResultRowKey, result);
    } else {
      if (includeInitStartRowKey && initStartRowKey.length > 0) {
        byte[] prevKey;
        // In order to generate largest possible rowkey that is less than
        // initStartRowKey, we need to check size of the region name that can be
        // used by hbase client for meta lookup, in case meta cache is expired at client.
        // Once we know regionLookupInMetaLen, use it to generate largest possible
        // rowkey that is lower than initStartRowKey by using
        // ByteUtil#previousKeyWithLength function, which appends "\\xFF" bytes to
        // prev rowey upto the length provided. e.g. for the given key
        // "\\x01\\xC1\\x06", the previous key with length 5 would be
        // "\\x01\\xC1\\x05\\xFF\\xFF" by padding 2 bytes "\\xFF".
        // The length of the largest scan start rowkey should not exceed
        // HConstants#MAX_ROW_LENGTH.
        int regionLookupInMetaLen =
          RegionInfo.createRegionName(region.getTableDescriptor().getTableName(), new byte[1],
            HConstants.NINES, false).length;
        if (
          Bytes.compareTo(initStartRowKey, initStartRowKey.length - 1, 1, ByteUtil.ZERO_BYTE, 0, 1)
              == 0
        ) {
          // If initStartRowKey has last byte as "\\x00", we can discard the last
          // byte and send the key as dummy rowKey.
          prevKey = new byte[initStartRowKey.length - 1];
          System.arraycopy(initStartRowKey, 0, prevKey, 0, prevKey.length);
        } else
          if (initStartRowKey.length < (HConstants.MAX_ROW_LENGTH - 1 - regionLookupInMetaLen)) {
            prevKey =
              ByteUtil.previousKeyWithLength(
                ByteUtil.concat(initStartRowKey,
                  new byte[HConstants.MAX_ROW_LENGTH - initStartRowKey.length - 1
                    - regionLookupInMetaLen]),
                HConstants.MAX_ROW_LENGTH - 1 - regionLookupInMetaLen);
          } else {
            prevKey = initStartRowKey;
          }
        getDummyResult(prevKey, result);
      } else {
        getDummyResult(initStartRowKey, result);
      }
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
}
