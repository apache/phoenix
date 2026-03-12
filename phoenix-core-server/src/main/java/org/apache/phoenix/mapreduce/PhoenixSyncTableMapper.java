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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
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
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SHA256DigestUtil;
import org.apache.phoenix.util.ScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Mapper that acts as a driver for validating table data between source and target clusters. The
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
      throw new RuntimeException(
        String.format("Failed to setup PhoenixSyncTableMapper for table: %s", tableName), e);
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
   * processed chunks from checkpoint table, resumes from check pointed progress and records final
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
      boolean isStartKeyInclusive = shouldStartKeyBeInclusive(mapperRegionStart, processedChunks);
      for (Pair<byte[], byte[]> range : unprocessedRanges) {
        processMapperRanges(range.getFirst(), range.getSecond(), isStartKeyInclusive, context);
        isStartKeyInclusive = false;
      }

      long mismatchedChunk = context.getCounter(SyncCounters.CHUNKS_MISMATCHED).getValue();
      long verifiedChunk = context.getCounter(SyncCounters.CHUNKS_VERIFIED).getValue();
      long sourceRowsProcessed = context.getCounter(SyncCounters.SOURCE_ROWS_PROCESSED).getValue();
      long targetRowsProcessed = context.getCounter(SyncCounters.TARGET_ROWS_PROCESSED).getValue();
      Timestamp mapperEndTime = new Timestamp(System.currentTimeMillis());
      String counters = formatMapperCounters(verifiedChunk, mismatchedChunk, sourceRowsProcessed,
        targetRowsProcessed);

      if (sourceRowsProcessed > 0) {
        if (mismatchedChunk == 0) {
          context.getCounter(PhoenixJobCounters.OUTPUT_RECORDS).increment(1);
          syncTableOutputRepository.checkpointSyncTableResult(tableName, targetZkQuorum,
            PhoenixSyncTableOutputRow.Type.MAPPER_REGION, fromTime, toTime, isDryRun,
            mapperRegionStart, mapperRegionEnd, PhoenixSyncTableOutputRow.Status.VERIFIED,
            mapperStartTime, mapperEndTime, counters);
          LOGGER.info(
            "PhoenixSyncTable mapper completed with verified: {} verified chunks, {} mismatched chunks",
            verifiedChunk, mismatchedChunk);
        } else {
          context.getCounter(PhoenixJobCounters.FAILED_RECORDS).increment(1);
          LOGGER.warn(
            "PhoenixSyncTable mapper completed with mismatch: {} verified chunks, {} mismatched chunks",
            verifiedChunk, mismatchedChunk);
          syncTableOutputRepository.checkpointSyncTableResult(tableName, targetZkQuorum,
            PhoenixSyncTableOutputRow.Type.MAPPER_REGION, fromTime, toTime, isDryRun,
            mapperRegionStart, mapperRegionEnd, PhoenixSyncTableOutputRow.Status.MISMATCHED,
            mapperStartTime, mapperEndTime, counters);
        }
      } else {
        LOGGER.info(
          "No rows pending to process. All mapper region boundaries are covered for startKey:{}, endKey: {}",
          mapperRegionStart, mapperRegionEnd);
      }
    } catch (SQLException e) {
      tryClosingResources();
      throw new RuntimeException("Error processing PhoenixSyncTableMapper", e);
    }
  }

  /**
   * Processes a chunk range by comparing source and target cluster data. Source chunking: Breaks
   * data into size-based chunks within given mapper region boundary. Target chunking: Follows
   * source chunk boundaries. Source chunk boundary might be split across multiple target region, if
   * so corpoc signals for partial chunk with partial digest. Once entire Source chunk is covered by
   * target scanner, we calculate resulting checksum from combined digest.
   * @param rangeStart                Range start key
   * @param rangeEnd                  Range end key
   * @param isSourceStartKeyInclusive Whether startKey be inclusive for source chunking
   * @param context                   Mapper context for progress and counters
   * @throws IOException  if scan fails
   * @throws SQLException if database operations fail
   */
  private void processMapperRanges(byte[] rangeStart, byte[] rangeEnd,
    boolean isSourceStartKeyInclusive, Context context) throws IOException, SQLException {
    // To handle scenario of target having extra keys compared to source keys:
    // For every source chunk, we track whether its first chunk of Region or whether its lastChunk
    // of region
    // For every source chunk, we issue scan on target with
    // - FirstChunkOfRegion : target scan start boundary would be rangeStart
    // - LastChunkOfRegion : target scan end boundary would be rangeEnd
    // - notFirstChunkOfRegion: target scan start boundary would be previous source chunk endKey
    // - notLastChunkOfRegion: target scan end boundary would be current source chunk endKey
    // Lets understand with an example.
    // Source region boundary is [c,n) and source chunk returns [c1,d] , here `c` key is not present
    // in source
    // It could be the case that target has `c` present, so we issue scan on target chunk with
    // startKey as `c` and not `c1` i.e [c,d]
    // Similarly, if two consecutive source chunk returns its boundary as [e,g] and [h,j]
    // When target is scanning for [h,j], it would issue scan with (g,j] to ensure we cover any
    // extra key which is not in source but present in target
    //
    // Now eventually when chunking will reach for last source chunk on this region boundary, we
    // again pass rangeEnd(with Exclusive) as target chunk boundary.
    // Lets say, for above region boundary example second last and last sourceChunk returns [j,k]
    // and [l,m]. Target chunk would issue scan for last chunk (k,n)
    boolean isLastChunkOfRegion = false;
    // We only want target startKey to be inclusive if source startKey is inclusive as well
    // Source start key won't be inclusive if start of region boundary is already processed as chunk
    // and check pointed
    // Refer to shouldStartKeyBeInclusive() method to understand more about when source start key
    // would be exclusive
    boolean isTargetStartKeyInclusive = isSourceStartKeyInclusive;
    try (ChunkScannerContext sourceScanner = createChunkScanner(sourceConnection, rangeStart,
      rangeEnd, null, isSourceStartKeyInclusive, false, false)) {
      ChunkInfo previousSourceChunk = null;
      ChunkInfo sourceChunk = sourceScanner.getNextChunk();
      while (sourceChunk != null) {
        sourceChunk.executionStartTime = new Timestamp(System.currentTimeMillis());
        // Peek ahead to see if this is the last chunk
        ChunkInfo nextSourceChunk = sourceScanner.getNextChunk();
        if (nextSourceChunk == null) {
          isLastChunkOfRegion = true;
        }
        ChunkInfo targetChunk = getTargetChunkWithSourceBoundary(targetConnection,
          previousSourceChunk == null ? rangeStart : previousSourceChunk.endKey,
          isLastChunkOfRegion ? rangeEnd : sourceChunk.endKey, isTargetStartKeyInclusive,
          !isLastChunkOfRegion);

        context.getCounter(SyncCounters.SOURCE_ROWS_PROCESSED).increment(sourceChunk.rowCount);
        context.getCounter(SyncCounters.TARGET_ROWS_PROCESSED).increment(targetChunk.rowCount);
        boolean matched = MessageDigest.isEqual(sourceChunk.hash, targetChunk.hash);
        if (LOGGER.isDebugEnabled()) {
          byte[] targetStartKey = targetChunk.startKey;
          byte[] targetEndKey = targetChunk.endKey;
          LOGGER.info(
            "isSourceStartKeyInclusive: {}, isTargetStartKeyInclusive: {},"
              + "isTargetEndKeyInclusive: {}, isFirstChunkOfRegion: {}, isLastChunkOfRegion: {}."
              + "Chunk comparison source {}, {}. Key range passed to target chunk: {}, {}."
              + "target chunk returned {}, {}: source={} rows, target={} rows, matched={}",
            isSourceStartKeyInclusive, isTargetStartKeyInclusive, !isLastChunkOfRegion,
            previousSourceChunk == null, isLastChunkOfRegion,
            Bytes.toStringBinary(sourceChunk.startKey), Bytes.toStringBinary(sourceChunk.endKey),
            Bytes.toStringBinary(targetStartKey), Bytes.toStringBinary(targetEndKey),
            Bytes.toStringBinary(targetChunk.startKey), Bytes.toStringBinary(targetChunk.endKey),
            sourceChunk.rowCount, targetChunk.rowCount, matched);
        }
        sourceChunk.executionEndTime = new Timestamp(System.currentTimeMillis());
        String counters = formatChunkCounters(sourceChunk.rowCount, targetChunk.rowCount);
        if (matched) {
          handleVerifiedChunk(sourceChunk, context, counters);
        } else {
          handleMismatchedChunk(sourceChunk, context, counters);
        }
        previousSourceChunk = sourceChunk;
        sourceChunk = nextSourceChunk;
        // After first chunk, our target chunk boundary would be previousSourceChunk.endKey,
        // so start key should not be inclusive
        isTargetStartKeyInclusive = false;
        context.progress();
      }
    }
    LOGGER.info("Completed sync table processing of Mapper region boundary {}, {}",
      Bytes.toStringBinary(rangeStart), Bytes.toStringBinary(rangeEnd));
  }

  /**
   * Scans target across multiple regions and returns a single combined ChunkInfo. Handles partial
   * chunks by passing digest state to next scanner via scan attributes, enabling cross-region
   * digest continuation. Since we are scanning rows based on source chunk boundary, it could be
   * distributed across multiple target regions. We keep on creating scanner across target region
   * until entire source chunk boundary is processed or chunk is null
   * @param conn     Target connection
   * @param startKey Source chunk start key
   * @param endKey   Source chunk end key
   * @return Single ChunkInfo with final hash from all target regions
   */
  private ChunkInfo getTargetChunkWithSourceBoundary(Connection conn, byte[] startKey,
    byte[] endKey, boolean isTargetStartKeyInclusive, boolean isTargetEndKeyInclusive)
    throws IOException, SQLException {
    ChunkInfo combinedTargetChunk = new ChunkInfo();
    combinedTargetChunk.startKey = null;
    combinedTargetChunk.endKey = null;
    combinedTargetChunk.hash = null;
    combinedTargetChunk.rowCount = 0;
    byte[] currentStartKey = startKey;
    byte[] continuedDigestState = null;
    ChunkInfo chunk;
    while (true) {
      // Each iteration scans one target region. The coprocessor processes all rows in
      // that region within the scan range. For target boundary, the chunk is always
      // marked partial and the digest state is passed to the next
      // scanner for cross-region hash continuation.
      try (ChunkScannerContext scanner = createChunkScanner(conn, currentStartKey, endKey,
        continuedDigestState, isTargetStartKeyInclusive, isTargetEndKeyInclusive, true)) {
        chunk = scanner.getNextChunk();
        // chunk == null means no more rows in the target range.
        // We must finalize the digest to produce a proper checksum for comparison.
        if (chunk == null) {
          if (continuedDigestState != null) {
            combinedTargetChunk.hash =
              SHA256DigestUtil.finalizeDigestToChecksum(continuedDigestState);
          }
          break;
        }
        if (combinedTargetChunk.startKey == null) {
          combinedTargetChunk.startKey = chunk.startKey;
        }
        combinedTargetChunk.endKey = chunk.endKey;
        combinedTargetChunk.rowCount += chunk.rowCount;
        continuedDigestState = chunk.hash;
        currentStartKey = chunk.endKey;
        isTargetStartKeyInclusive = false;
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
   * @throws IOException  scanner creation fails
   * @throws SQLException hTable connection fails
   */
  private ChunkScannerContext createChunkScanner(Connection conn, byte[] startKey, byte[] endKey,
    byte[] continuedDigestState, boolean isStartKeyInclusive, boolean isEndKeyInclusive,
    boolean isTargetScan) throws IOException, SQLException {
    // Not using try-with-resources since ChunkScannerContext owns the table lifecycle
    Table hTable =
      conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(physicalTableName);
    Scan scan =
      createChunkScan(startKey, endKey, isStartKeyInclusive, isEndKeyInclusive, isTargetScan);
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
    long syncTableRpcTimeoutMs = conf.getLong(HConstants.HBASE_RPC_TIMEOUT_KEY,
      QueryServicesOptions.DEFAULT_SYNC_TABLE_RPC_TIMEOUT);
    scan.setAttribute(BaseScannerRegionObserverConstants.SERVER_PAGE_SIZE_MS,
      Bytes.toBytes(syncTableRpcTimeoutMs / 2));
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
    Cell startKeyCell = MetaDataUtil.getCell(cells,
      BaseScannerRegionObserverConstants.SYNC_TABLE_START_KEY_QUALIFIER);
    Cell rowCountCell = MetaDataUtil.getCell(cells,
      BaseScannerRegionObserverConstants.SYNC_TABLE_ROW_COUNT_QUALIFIER);
    Cell isPartialChunkCell = MetaDataUtil.getCell(cells,
      BaseScannerRegionObserverConstants.SYNC_TABLE_IS_PARTIAL_CHUNK_QUALIFIER);
    Cell hashCell =
      MetaDataUtil.getCell(cells, BaseScannerRegionObserverConstants.SYNC_TABLE_HASH_QUALIFIER);

    if (
      startKeyCell == null || rowCountCell == null || isPartialChunkCell == null || hashCell == null
    ) {
      throw new RuntimeException("Missing required chunk metadata cells.");
    }

    ChunkInfo info = new ChunkInfo();
    info.startKey = CellUtil.cloneValue(startKeyCell);
    info.endKey = result.getRow();
    info.rowCount = Bytes.toLong(rowCountCell.getValueArray(), rowCountCell.getValueOffset(),
      rowCountCell.getValueLength());
    info.isPartial = isPartialChunkCell.getValueArray()[isPartialChunkCell.getValueOffset()] != 0;
    info.hash = CellUtil.cloneValue(hashCell);
    return info;
  }

  /**
   * Formats chunk counters as a comma-separated string.
   * @param sourceRows Source rows processed
   * @param targetRows Target rows processed
   * @return Formatted string: "SOURCE_ROWS_PROCESSED=123,TARGET_ROWS_PROCESSED=456"
   */
  private String formatChunkCounters(long sourceRows, long targetRows) {
    return String.format("%s=%d,%s=%d", SyncCounters.SOURCE_ROWS_PROCESSED.name(), sourceRows,
      SyncCounters.TARGET_ROWS_PROCESSED.name(), targetRows);
  }

  /**
   * Formats mapper counters as a comma-separated string.
   * @param chunksVerified   Chunks verified count
   * @param chunksMismatched Chunks mismatched count
   * @param sourceRows       Source rows processed
   * @param targetRows       Target rows processed
   * @return Formatted string with all mapper counters
   */
  private String formatMapperCounters(long chunksVerified, long chunksMismatched, long sourceRows,
    long targetRows) {
    return String.format("%s=%d,%s=%d,%s=%d,%s=%d", SyncCounters.CHUNKS_VERIFIED.name(),
      chunksVerified, SyncCounters.CHUNKS_MISMATCHED.name(), chunksMismatched,
      SyncCounters.SOURCE_ROWS_PROCESSED.name(), sourceRows,
      SyncCounters.TARGET_ROWS_PROCESSED.name(), targetRows);
  }

  private void handleVerifiedChunk(ChunkInfo sourceChunk, Context context, String counters)
    throws SQLException {
    syncTableOutputRepository.checkpointSyncTableResult(tableName, targetZkQuorum,
      PhoenixSyncTableOutputRow.Type.CHUNK, fromTime, toTime, isDryRun, sourceChunk.startKey,
      sourceChunk.endKey, PhoenixSyncTableOutputRow.Status.VERIFIED, sourceChunk.executionStartTime,
      sourceChunk.executionEndTime, counters);
    context.getCounter(SyncCounters.CHUNKS_VERIFIED).increment(1);
  }

  private void handleMismatchedChunk(ChunkInfo sourceChunk, Context context, String counters)
    throws SQLException {
    LOGGER.warn("Chunk mismatch detected for table: {}, with startKey: {}, endKey {}", tableName,
      Bytes.toStringBinary(sourceChunk.startKey), Bytes.toStringBinary(sourceChunk.endKey));
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
   * Calculates unprocessed gaps within a mapper region. Since a mapper region is divided into
   * multiple chunks and we store mapper region boundary and its chunked boundary. Once we have all
   * the processedChunks which falls in this Mapper region boundary, we look for holes/gaps in
   * mapper region boundary which haven't been processed as chunks. Given a list of processed
   * chunks, returns the ranges that haven't been processed yet. This will be useful on retries if
   * Region boundary has changed and we some chunks in the new region boundary has been processed
   * which can be skipped
   * @param mapperRegionStart Start of mapper region
   * @param mapperRegionEnd   End of mapper region
   * @param processedChunks   List of already-processed chunks from getProcessedChunks()
   * @return List of (startKey, endKey) pairs representing unprocessed ranges
   */
  @VisibleForTesting
  public List<Pair<byte[], byte[]>> calculateUnprocessedRanges(byte[] mapperRegionStart,
    byte[] mapperRegionEnd, List<PhoenixSyncTableOutputRow> processedChunks) {
    List<Pair<byte[], byte[]>> gaps = new ArrayList<>();
    // If processedChunks is null or empty, the entire mapper region needs processing
    if (processedChunks == null || processedChunks.isEmpty()) {
      gaps.add(new Pair<>(mapperRegionStart, mapperRegionEnd));
      return gaps;
    }

    // Since chunk keys are always inclusive(start/endKey) it would never be null/empty.
    // But Mapper region boundary can be empty i.e [] for start/end region of table.
    // We would be doing byte comparison as part of identifying gaps and empty bytes
    // needs to be considered as special case as comparison won't work on them.
    boolean isStartRegionOfTable = mapperRegionStart == null || mapperRegionStart.length == 0;
    boolean isEndRegionOfTable = mapperRegionEnd == null || mapperRegionEnd.length == 0;

    // Track our scanning position through the mapper region as we iterate through chunks
    byte[] scanPos = mapperRegionStart;

    // With entire Mapper region boundary, we iterate over each chunk and if any gap/hole identified
    // in Mapper region range which is not covered by processed chunk, we add it to gaps list.
    // Since chunks are sorted and non-overlapping, only first/last chunks
    // need boundary clipping. All middle chunks are guaranteed to be within region boundaries.
    for (int i = 0; i < processedChunks.size(); i++) {
      PhoenixSyncTableOutputRow chunk = processedChunks.get(i);
      byte[] chunkStart = chunk.getStartRowKey();
      byte[] chunkEnd = chunk.getEndRowKey();
      boolean initialChunk = i == 0;
      boolean lastChunk = i == processedChunks.size() - 1;

      // Determine effective start boundary for this chunk
      // Only the first chunk might start before mapperRegionStart and need clipping
      byte[] effectiveStart;
      if (initialChunk && !isStartRegionOfTable) {
        // initialChunk chunk, clip boundary outside of Mapper region.
        // Example: Mapper region [20, 85), first chunk [10, 30]
        // effectiveStart = max[10, 20] = 20
        // ---[20---MapperRegion---------------85)
        // [10---chunk1---30]-------
        effectiveStart =
          Bytes.compareTo(chunkStart, mapperRegionStart) > 0 ? chunkStart : mapperRegionStart;
      } else {
        // isFirstRegionOfTable -> Mapper region [,80) effectiveStart = chunkStart
        // Not an initial chunks: chunk start guaranteed to be within region boundaries, no clipping
        // needed
        effectiveStart = chunkStart;
      }

      // Determine effective end boundary for this chunk
      // Only the last chunk might extend beyond mapperRegionEnd and need clipping
      byte[] effectiveEnd;
      if (lastChunk && !isEndRegionOfTable) {
        // last Chunk, clip boundary outside of Mapper region.
        // Example: Mapper region [20, 85), last chunk [70, 90]
        // effectiveEnd = min(90, 85) = 85
        // ---[20---MapperRegion---------------85)
        // ------------------------------[70---chunk1---90]-------
        effectiveEnd = Bytes.compareTo(chunkEnd, mapperRegionEnd) < 0 ? chunkEnd : mapperRegionEnd;
      } else {
        // isLastRegionOfTable -> Mapper region [80,) effectiveEnd = chunkEnd
        // Not last chunk: chunk end is guaranteed to be within region boundaries, no clipping
        // needed
        effectiveEnd = chunkEnd;
      }

      // Check for gap BEFORE this chunk
      // If there's space between our current position and where this chunk starts, that's a gap
      // that needs processing
      // Example: scanPos=30 (processed till this key), effectiveStart=70 (chunk start key)
      // Gap detected: [30, 70) needs processing
      if (Bytes.compareTo(scanPos, effectiveStart) < 0) {
        gaps.add(new Pair<>(scanPos, effectiveStart));
      }
      // We've now "processed" up to this key
      scanPos = effectiveEnd;
    }

    // Since Mapper region end is exclusive, we want to add any remaining key boundary as gaps
    // except when scanPos == mapperRegionEnd (i.e end of Mapper region boundary got covered by
    // chunk)
    if (isEndRegionOfTable || Bytes.compareTo(scanPos, mapperRegionEnd) < 0) {
      gaps.add(new Pair<>(scanPos, mapperRegionEnd));
    }
    return gaps;
  }

  /***
   * Checking if start key should be inclusive, this is specific to scenario when there are
   * processed chunks within this Mapper region boundary.
   */
  boolean shouldStartKeyBeInclusive(byte[] mapperRegionStart,
    List<PhoenixSyncTableOutputRow> processedChunks) {
    // Only with processed chunk like below we need to
    // have first unprocessedRanges startKeyInclusive = true.
    // [---MapperRegion---------------)
    // -----[--chunk1--] [--chunk2--]
    //
    // Otherwise with processed chunk like below, we don't want startKeyInclusive = true
    // for any of unprocessedRange
    // ---[---MapperRegion---------------)
    // [--chunk1--] [--chunk2--]
    // OR
    // [---MapperRegion---------------)
    // [--chunk1--] [--chunk2--]
    if (
      mapperRegionStart == null || mapperRegionStart.length == 0 || processedChunks == null
        || processedChunks.isEmpty()
    ) {
      return true;
    }
    return Bytes.compareTo(processedChunks.get(0).getStartRowKey(), mapperRegionStart) > 0;
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
    Timestamp executionStartTime;
    Timestamp executionEndTime;

    @Override
    public String toString() {
      return String.format("Chunk[start=%s, end=%s, rows=%d, partial=%s]",
        Bytes.toStringBinary(startKey), Bytes.toStringBinary(endKey), rowCount, isPartial);
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
      while (true) {
        Result result = scanner.next();
        if (result == null || result.isEmpty()) {
          return null;
        }
        // Skip dummy results from PagingFilter and continue scanning
        if (ScanUtil.isDummy(result)) {
          LOGGER.info("Skipping dummy paging result at row {}, continuing scan",
            Bytes.toStringBinary(result.getRow()));
          continue;
        }
        return parseChunkInfo(result);
      }
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
