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

import static org.apache.phoenix.schema.types.PDataType.TRUE_BYTES;

import java.io.IOException;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Mapper that acts as a driver for synchronizing table between source and target clusters. The
 * actual work of chunking and hashing is done server-side by the coprocessor. This mapper fetches
 * chunk hashes from both clusters, compares them and write to checkpoint table.
 */
public class PhoenixSyncTableMapper
  extends Mapper<NullWritable, DBInputFormat.NullDBWritable, NullWritable, NullWritable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixSyncTableMapper.class);

  public enum SyncCounters {
    CHUNKS_VERIFIED,
    CHUNKS_MISMATCHED,
    SOURCE_ROWS_PROCESSED,
    TARGET_ROWS_PROCESSED,
  }

  private String tableName;
  private String targetZkQuorum;
  private Long fromTime;
  private Long toTime;
  private boolean isDryRun;
  private long chunkSizeBytes;
  private Configuration conf;
  private Connection sourceConnection;
  private Connection targetConnection;
  private Connection globalConnection;
  private PTable pTable;
  private byte[] physicalTableName;
  private byte[] mapperRegionStart;
  private byte[] mapperRegionEnd;
  private PhoenixSyncTableOutputRepository syncTableOutputRepository;
  private Timestamp mapperStartTime;

  @Override
  protected void setup(Context context) throws InterruptedException {
    try {
      super.setup(context);
      mapperStartTime = new Timestamp(System.currentTimeMillis());
      this.conf = context.getConfiguration();
      tableName = PhoenixConfigurationUtil.getPhoenixSyncTableName(conf);
      targetZkQuorum = PhoenixConfigurationUtil.getPhoenixSyncTableTargetZkQuorum(conf);
      fromTime = PhoenixConfigurationUtil.getPhoenixSyncTableFromTime(conf);
      toTime = PhoenixConfigurationUtil.getPhoenixSyncTableToTime(conf);
      isDryRun = PhoenixConfigurationUtil.getPhoenixSyncTableDryRun(conf);
      chunkSizeBytes = PhoenixConfigurationUtil.getPhoenixSyncTableChunkSizeBytes(conf);
      extractRegionBoundariesFromSplit(context);
      sourceConnection = ConnectionUtil.getInputConnection(conf);
      pTable = sourceConnection.unwrap(PhoenixConnection.class).getTable(tableName);
      physicalTableName = pTable.getPhysicalName().getBytes();
      connectToTargetCluster();
      globalConnection = createGlobalConnection(conf);
      syncTableOutputRepository = new PhoenixSyncTableOutputRepository(globalConnection);
    } catch (SQLException | IOException e) {
      tryClosingResources();
      throw new RuntimeException(String.format("Failed to setup PhoenixSyncTableMapper for table: %s", tableName),
        e);
    }
  }

  /**
   * Extracts mapper region boundaries from the PhoenixInputSplit
   */
  private void extractRegionBoundariesFromSplit(Context context) {
    PhoenixInputSplit split = (PhoenixInputSplit) context.getInputSplit();
    KeyRange keyRange = split.getKeyRange();
    if (keyRange == null) {
      throw new IllegalStateException(String.format(
        "PhoenixInputSplit has no KeyRange for table: %s . Cannot determine region boundaries for sync operation.",
        tableName));
    }
    mapperRegionStart = keyRange.getLowerRange();
    mapperRegionEnd = keyRange.getUpperRange();
  }

  /**
   * Connects to the target cluster using the target ZK quorum, port, znode, krb principal
   */
  private void connectToTargetCluster() throws SQLException, IOException {
    Configuration targetConf =
      PhoenixMapReduceUtil.createConfigurationForZkQuorum(conf, targetZkQuorum);
    targetConnection = ConnectionUtil.getInputConnection(targetConf);
  }

  /**
   * Creates a global (non-tenant) connection for the checkpoint table.
   */
  private Connection createGlobalConnection(Configuration conf) throws SQLException {
    Configuration globalConf = new Configuration(conf);
    globalConf.unset(PhoenixConfigurationUtil.MAPREDUCE_TENANT_ID);
    globalConf.unset(PhoenixRuntime.CURRENT_SCN_ATTRIB);
    return ConnectionUtil.getInputConnection(globalConf);
  }

  /**
   * Processes a mapper region by comparing chunks between source and target clusters. Gets already
   * processed chunks from checkpoint table, resumes from checkpointed progress and records final
   * status for chunks & mapper (VERIFIED/MISMATCHED).
   */
  @Override
  protected void map(NullWritable key, DBInputFormat.NullDBWritable value, Context context)
    throws IOException, InterruptedException {
    context.getCounter(PhoenixJobCounters.INPUT_RECORDS).increment(1);
    try {
      List<PhoenixSyncTableOutputRow> processedChunks =
        syncTableOutputRepository.getProcessedChunks(tableName, targetZkQuorum, fromTime, toTime,
          mapperRegionStart, mapperRegionEnd);
      List<Pair<byte[], byte[]>> unprocessedRanges =
        calculateUnprocessedRanges(mapperRegionStart, mapperRegionEnd, processedChunks);
      for (Pair<byte[], byte[]> range : unprocessedRanges) {
        processChunkRange(range.getFirst(), range.getSecond(), context);
      }

      long mismatchedChunk = context.getCounter(SyncCounters.CHUNKS_MISMATCHED).getValue();
      long verifiedChunk = context.getCounter(SyncCounters.CHUNKS_VERIFIED).getValue();
      long sourceRowsProcessed = context.getCounter(SyncCounters.SOURCE_ROWS_PROCESSED).getValue();
      long targetRowsProcessed = context.getCounter(SyncCounters.TARGET_ROWS_PROCESSED).getValue();
      Timestamp mapperEndTime = new Timestamp(System.currentTimeMillis());
      Map<String, Long> mapperCounters = new LinkedHashMap<>();
      mapperCounters.put(SyncCounters.CHUNKS_VERIFIED.name(), verifiedChunk);
      mapperCounters.put(SyncCounters.CHUNKS_MISMATCHED.name(), mismatchedChunk);
      mapperCounters.put(SyncCounters.SOURCE_ROWS_PROCESSED.name(), sourceRowsProcessed);
      mapperCounters.put(SyncCounters.TARGET_ROWS_PROCESSED.name(), targetRowsProcessed);
      String counters = formatCounters(mapperCounters);

      if (mismatchedChunk == 0) {
        context.getCounter(PhoenixJobCounters.OUTPUT_RECORDS).increment(1);
        syncTableOutputRepository.checkpointSyncTableResult(tableName, targetZkQuorum,
          PhoenixSyncTableOutputRow.Type.MAPPER_REGION, fromTime, toTime, isDryRun,
          mapperRegionStart, mapperRegionEnd, PhoenixSyncTableOutputRow.Status.VERIFIED,
          mapperStartTime, mapperEndTime, counters);
        LOGGER.info(
          "PhoenixSyncTable mapper completed with verified: {} verifiedChunk chunks, {} mismatchedChunk chunks",
          verifiedChunk, mismatchedChunk);
      } else {
        context.getCounter(PhoenixJobCounters.FAILED_RECORDS).increment(1);
        LOGGER.warn(
          "PhoenixSyncTable mapper completed with mismatch: {} verifiedChunk chunks, {} mismatchedChunk chunks",
          verifiedChunk, mismatchedChunk);
        syncTableOutputRepository.checkpointSyncTableResult(tableName, targetZkQuorum,
          PhoenixSyncTableOutputRow.Type.MAPPER_REGION, fromTime, toTime, isDryRun,
          mapperRegionStart, mapperRegionEnd, PhoenixSyncTableOutputRow.Status.MISMATCHED,
          mapperStartTime, mapperEndTime, counters);
      }
    } catch (SQLException e) {
      tryClosingResources();
      throw new RuntimeException("Error processing PhoenixSyncTableMapper", e);
    }
  }

  /**
   * Processes a chunk range by comparing source and target cluster data.
   * Source chunking: Breaks data into size-based chunks within given mapper region boundary.
   * Target chunking: Follows source chunk boundaries exactly. Source chunk boundary might
   * be split across multiple target region, if so corpoc signals for partial chunk with partial
   * digest. Once entire Source chunk is covered by target scanner, we calculate resulting checksum
   * from combined digest.
   * @param rangeStart Range start key
   * @param rangeEnd   Range end key
   * @param context    Mapper context for progress and counters
   * @throws IOException  if scan fails
   * @throws SQLException if database operations fail
   */
  private void processChunkRange(byte[] rangeStart, byte[] rangeEnd, Context context)
      throws IOException, SQLException {
    boolean isStartKeyInclusive = true;
    try (ChunkScannerContext sourceScanner = createChunkScanner(sourceConnection, rangeStart,
        rangeEnd, null, isStartKeyInclusive, false, false)) {
      while (true) {
        // We only try to get one chunked metadata row returned at a time until no more chunk returned(i.e null)
        ChunkInfo sourceChunk = sourceScanner.getNextChunk();
        if (sourceChunk == null) {
          break;
        }
        sourceChunk.executionStartTime = new Timestamp(System.currentTimeMillis());
        ChunkInfo targetChunk =
            getTargetChunkWithSourceBoundary(targetConnection, sourceChunk.startKey,
                sourceChunk.endKey);

        context.getCounter(SyncCounters.SOURCE_ROWS_PROCESSED).increment(sourceChunk.rowCount);
        context.getCounter(SyncCounters.TARGET_ROWS_PROCESSED).increment(targetChunk.rowCount);
        boolean matched = MessageDigest.isEqual(sourceChunk.hash, targetChunk.hash);
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Chunk comparison {}, {}: source={} rows, target={} rows, matched={}",
              Bytes.toStringBinary(sourceChunk.startKey), Bytes.toStringBinary(sourceChunk.endKey),
              sourceChunk.rowCount, targetChunk.rowCount, matched);
        }
        sourceChunk.executionEndTime = new Timestamp(System.currentTimeMillis());
        Map<String, Long> mapperCounters = new LinkedHashMap<>();
        mapperCounters.put(SyncCounters.SOURCE_ROWS_PROCESSED.name(), sourceChunk.rowCount);
        mapperCounters.put(SyncCounters.TARGET_ROWS_PROCESSED.name(), targetChunk.rowCount);
        String counters = formatCounters(mapperCounters);
        if (matched) {
          handleVerifiedChunk(sourceChunk, context, counters);
        } else {
          handleMismatchedChunk(sourceChunk, context, counters);
        }
        context.progress();
      }
    }
    LOGGER.info("Completed sync table processing of Mapper region boundary {}, {}",
        Bytes.toStringBinary(rangeStart), Bytes.toStringBinary(rangeEnd));
  }

  /**
   * Scans target across multiple regions and returns a single combined ChunkInfo.
   * Handles partial chunks by passing digest state to next scanner via scan attributes, enabling cross-region
   * digest continuation. Since we are scanning rows based on source chunk boundary, it could be
   * distributed across multiple target regions. We keep on creating scanner across target region
   * until entire source chunk boundary is processed or chunk is null
   * @param conn     Target connection
   * @param startKey Source chunk start key
   * @param endKey   Source chunk end key
   * @return Single ChunkInfo with final hash from all target regions
   */
  private ChunkInfo getTargetChunkWithSourceBoundary(Connection conn, byte[] startKey,
    byte[] endKey) throws IOException, SQLException {
    ChunkInfo combinedTargetChunk = new ChunkInfo();
    combinedTargetChunk.startKey = startKey;
    combinedTargetChunk.endKey = endKey;
    combinedTargetChunk.hash = new byte[0];
    combinedTargetChunk.rowCount = 0;
    combinedTargetChunk.isPartial = false;
    byte[] currentStartKey = startKey;
    byte[] continuedDigestState = null;
    boolean isStartKeyInclusive = true;
    while (true) {
      // We are creating a new scanner for every target region chunk.
      // This chunk could be partial or full depending on whether the source region boundary is part of one or multiple target region.
      // For every target region scanned, we want to have one row processed and returned back
      // immediately(that's why we set scan.setLimit(1)), since output from one region partial chunk
      // scanner is input to next region scanner.
      try (ChunkScannerContext scanner = createChunkScanner(conn, currentStartKey, endKey,
        continuedDigestState, isStartKeyInclusive, true, true)) {
        ChunkInfo chunk = scanner.getNextChunk();
        // In a happy path where source and target rows are matching, target chunk would never be null.
        // If chunk returned null, this would mean it couldn't find last source rows in target,
        // since we only return isPartial=true until target chunk end key < source chunk endKey.
        // Hash would still be digest if chunk returned is null and not a checksum, so would never match(which is expected).
        // We could convert the digest to checksum but since it won't match anyhow, we don't need to.
        if (chunk == null) {
          break;
        }
        combinedTargetChunk.rowCount += chunk.rowCount;
        // Updating it with either digest(when isPartial) or checksum(when all rows chunked)
        combinedTargetChunk.hash = chunk.hash;
        if (chunk.isPartial) {
          continuedDigestState = chunk.hash;
          currentStartKey = chunk.endKey;
          isStartKeyInclusive = false;
        } else {
          break;
        }
      }
    }
    return combinedTargetChunk;
  }

  /**
   * Creates a reusable scanner context for fetching chunks from a range.
   * @param conn                 Connection to cluster (source or target)
   * @param startKey             Range start key (inclusive)
   * @param endKey               Range end key (exclusive)
   * @param continuedDigestState If not null, coprocessor will continue hashing from this state (for
   *                             cross-region continuation on target)
   * @param isStartKeyInclusive  Whether StartKey Inclusive
   * @param isEndKeyInclusive    Whether EndKey Inclusive
   * @return ChunkScannerContext
   * @throws IOException  scanner creation fails
   * @throws SQLException hTable connection fails
   */
  private ChunkScannerContext createChunkScanner(Connection conn, byte[] startKey, byte[] endKey,
    byte[] continuedDigestState, boolean isStartKeyInclusive,
    boolean isEndKeyInclusive, boolean isTargetScan) throws IOException, SQLException {
    // Not using try-with-resources since ChunkScannerContext owns the table lifecycle
    Table hTable =
      conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(physicalTableName);
    Scan scan = createChunkScan(startKey, endKey, isStartKeyInclusive, isEndKeyInclusive, isTargetScan);
    scan.setAttribute(BaseScannerRegionObserverConstants.SYNC_TABLE_CHUNK_FORMATION, TRUE_BYTES);
    scan.setAttribute(BaseScannerRegionObserverConstants.SKIP_REGION_BOUNDARY_CHECK, TRUE_BYTES);
    scan.setAttribute(BaseScannerRegionObserverConstants.UNGROUPED_AGG, TRUE_BYTES);
    if (continuedDigestState != null && continuedDigestState.length > 0) {
      scan.setAttribute(BaseScannerRegionObserverConstants.SYNC_TABLE_CONTINUED_DIGEST_STATE,
        continuedDigestState);
    }

    if (!isTargetScan) {
      scan.setAttribute(BaseScannerRegionObserverConstants.SYNC_TABLE_CHUNK_SIZE_BYTES,
        Bytes.toBytes(chunkSizeBytes));
    }
    ResultScanner scanner = hTable.getScanner(scan);
    return new ChunkScannerContext(hTable, scanner);
  }

  /**
   * Parses chunk information from the coprocessor result. The PhoenixSyncTableRegionScanner returns
   * cells with chunk metadata including SHA-256 hash (for complete chunks) or MessageDigest state
   * (for partial chunks).
   */
  private ChunkInfo parseChunkInfo(Result result) {
    List<Cell> cells = Arrays.asList(result.rawCells());
    Cell endKeyCell =
      MetaDataUtil.getCell(cells, BaseScannerRegionObserverConstants.SYNC_TABLE_END_KEY_QUALIFIER);
    Cell rowCountCell = MetaDataUtil.getCell(cells,
      BaseScannerRegionObserverConstants.SYNC_TABLE_ROW_COUNT_QUALIFIER);
    Cell isPartialChunkCell = MetaDataUtil.getCell(cells,
      BaseScannerRegionObserverConstants.SYNC_TABLE_IS_PARTIAL_CHUNK_QUALIFIER);
    Cell hashCell =
      MetaDataUtil.getCell(cells, BaseScannerRegionObserverConstants.SYNC_TABLE_HASH_QUALIFIER);

    if (endKeyCell == null || rowCountCell == null || isPartialChunkCell == null
        || hashCell == null) {
      throw new RuntimeException("Missing required chunk metadata cells.");
    }

    ChunkInfo info = new ChunkInfo();
    info.startKey = result.getRow();
    info.endKey = CellUtil.cloneValue(endKeyCell);
    info.rowCount = Bytes.toLong(rowCountCell.getValueArray(), rowCountCell.getValueOffset(),
      rowCountCell.getValueLength());
    info.isPartial = isPartialChunkCell.getValueArray()[isPartialChunkCell.getValueOffset()] != 0;
    info.hash = CellUtil.cloneValue(hashCell);
    return info;
  }

  /**
   * Formats counters as a comma-separated key=value string. Example:
   * "CHUNKS_VERIFIED=10,CHUNKS_MISMATCHED=2,SOURCE_ROWS_PROCESSED=5678..."
   * @param counters Map of counter names to values
   * @return Formatted string or null if counters is null/empty
   */
  private String formatCounters(Map<String, Long> counters) {
    if (counters == null || counters.isEmpty()) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Map.Entry<String, Long> entry : counters.entrySet()) {
      if (!first) {
        sb.append(",");
      }
      sb.append(entry.getKey()).append("=").append(entry.getValue());
      first = false;
    }
    return sb.toString();
  }

  /***
   *
   */
  private void handleVerifiedChunk(ChunkInfo sourceChunk, Context context, String counters) throws SQLException {
    syncTableOutputRepository.checkpointSyncTableResult(tableName, targetZkQuorum,
      PhoenixSyncTableOutputRow.Type.CHUNK, fromTime, toTime, isDryRun, sourceChunk.startKey,
        sourceChunk.endKey, PhoenixSyncTableOutputRow.Status.VERIFIED, sourceChunk.executionStartTime,
      sourceChunk.executionEndTime, counters);
    context.getCounter(SyncCounters.CHUNKS_VERIFIED).increment(1);
  }

  /***
   *
   */
  private void handleMismatchedChunk(ChunkInfo sourceChunk, Context context, String counters) throws SQLException {
    LOGGER.warn("Chunk mismatch detected for table: {}, with startKey: {}, endKey {}",
        tableName, Bytes.toStringBinary(sourceChunk.startKey),
        Bytes.toStringBinary(sourceChunk.endKey));
    syncTableOutputRepository.checkpointSyncTableResult(tableName, targetZkQuorum,
        PhoenixSyncTableOutputRow.Type.CHUNK, fromTime, toTime, isDryRun, sourceChunk.startKey,
        sourceChunk.endKey, PhoenixSyncTableOutputRow.Status.MISMATCHED,
        sourceChunk.executionStartTime, sourceChunk.executionEndTime, counters);
    context.getCounter(SyncCounters.CHUNKS_MISMATCHED).increment(1);
  }

  /**
   * Creates a Hbase raw scan for a chunk range to capture all cell versions and delete markers.
   */
  private Scan createChunkScan(byte[] startKey, byte[] endKey, boolean isStartKeyInclusive,
    boolean isEndKeyInclusive, boolean isTargetScan) throws IOException {
    Scan scan = new Scan();
    scan.withStartRow(startKey, isStartKeyInclusive);
    scan.withStopRow(endKey, isEndKeyInclusive);
    scan.setRaw(true);
    scan.readAllVersions();
    scan.setCacheBlocks(false);
    scan.setTimeRange(fromTime, toTime);
    if (isTargetScan) {
      scan.setLimit(1);
      scan.setCaching(1);
    }
    return scan;
  }

  /**
   * Calculates unprocessed chunk ranges (gaps) within a mapper region. Given a list of processed
   * chunks, returns the ranges that haven't been processed yet.
   * @param mapperRegionStart Start of mapper region
   * @param mapperRegionEnd   End of mapper region
   * @param processedChunks   List of already-processed chunks from getProcessedChunks()
   * @return List of (startKey, endKey) pairs representing unprocessed ranges
   */
  @VisibleForTesting
  public List<Pair<byte[], byte[]>> calculateUnprocessedRanges(byte[] mapperRegionStart,
    byte[] mapperRegionEnd, List<PhoenixSyncTableOutputRow> processedChunks) {
    List<Pair<byte[], byte[]>> gaps = new ArrayList<>();
    if (processedChunks == null || processedChunks.isEmpty()) {
      gaps.add(new Pair<>(mapperRegionStart, mapperRegionEnd));
      return gaps;
    }

    byte[] currentPos = mapperRegionStart;
    for (PhoenixSyncTableOutputRow chunk : processedChunks) {
      byte[] chunkStart = chunk.getStartRowKey();
      byte[] chunkEnd = chunk.getEndRowKey();
      // Clip chunk boundaries to mapper region boundaries
      // If Mapper region boundary is [20,85) and already processed chunked rows boundaries are
      // [10,30] and [70,80]
      // When we start mapper, it would identify already processed ranges as [10,30] and [70,80]
      // and return gaps as [30,70) and [80,85).
      byte[] effectiveStart =
        Bytes.compareTo(chunkStart, mapperRegionStart) > 0 ? chunkStart : mapperRegionStart;
      byte[] effectiveEnd =
        Bytes.compareTo(chunkEnd, mapperRegionEnd) < 0 ? chunkEnd : mapperRegionEnd;
      // Check for gap before this chunk's effective start
      if (Bytes.compareTo(currentPos, effectiveStart) < 0) {
        gaps.add(new Pair<>(currentPos, effectiveStart));
      }
      currentPos = effectiveEnd;
    }
    if (Bytes.compareTo(currentPos, mapperRegionEnd) < 0) {
      gaps.add(new Pair<>(currentPos, mapperRegionEnd));
    }
    return gaps;
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    tryClosingResources();
    super.cleanup(context);
  }

  private void tryClosingResources() {
    if (sourceConnection != null) {
      try {
        sourceConnection.close();
      } catch (SQLException e) {
        LOGGER.error("Error while closing source connection in PhoenixSyncTableMapper", e);
      }
    }
    if (targetConnection != null) {
      try {
        targetConnection.close();
      } catch (SQLException e) {
        LOGGER.error("Error while closing target connection in PhoenixSyncTableMapper", e);
      }
    }
    if (globalConnection != null) {
      try {
        globalConnection.close();
      } catch (SQLException e) {
        LOGGER.error("Error while closing output connection in PhoenixSyncTableMapper", e);
      }
    }
  }

  /**
   * Hold chunk metadata returned from coprocessor
   */
  private static class ChunkInfo {
    byte[] startKey;
    byte[] endKey;
    byte[] hash;
    long rowCount;
    boolean isPartial;
    boolean hasMoreRowsInRegion;
    Timestamp executionStartTime;
    Timestamp executionEndTime;

    @Override
    public String toString() {
      return String.format("Chunk[start=%s, end=%s, rows=%d, partial=%s, hasMoreRowsInRegion=%s]",
        Bytes.toStringBinary(startKey), Bytes.toStringBinary(endKey), rowCount, isPartial,
        hasMoreRowsInRegion);
    }
  }

  /**
   * Holds a ResultScanner and Table reference for reuse across multiple chunks.
   */
  private class ChunkScannerContext implements AutoCloseable {
    private final Table table;
    private final ResultScanner scanner;

    ChunkScannerContext(Table table, ResultScanner scanner) {
      this.table = table;
      this.scanner = scanner;
    }

    /**
     * Fetches the next chunk from the scanner. Each call retrieves one chunk's metadata from the
     * server-side coprocessor.
     * @return ChunkInfo or null if no more chunks available for region
     * @throws IOException if scan fails
     */
    ChunkInfo getNextChunk() throws IOException {
      Result result = scanner.next();
      if (result == null || result.isEmpty()) {
        return null;
      }
      return parseChunkInfo(result);
    }

    @Override
    public void close() throws IOException {
      try {
        if (scanner != null) {
          scanner.close();
        }
      } finally {
        if (table != null) {
          table.close();
        }
      }
    }
  }
}
