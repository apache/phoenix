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
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.SHA256DigestUtil;
import org.apache.phoenix.util.ScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mapper that acts as a driver for validating table data between source and target clusters. The
 * actual work of chunking and hashing is done server-side by the coprocessor. This mapper fetches
 * chunk hashes from both clusters, compares them and write to checkpoint table.
 */
public class PhoenixSyncTableMapper
  extends Mapper<NullWritable, DBInputFormat.NullDBWritable, NullWritable, NullWritable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixSyncTableMapper.class);

  public enum SyncCounters {
    MAPPERS_VERIFIED,
    MAPPERS_MISMATCHED,
    MAPPERS_REPAIRED,
    MAPPERS_UNREPAIRABLE,
    MAPPERS_REPAIR_FAILED,
    CHUNKS_VERIFIED,
    CHUNKS_MISMATCHED,
    CHUNKS_REPAIRED,
    CHUNKS_UNREPAIRABLE,
    CHUNKS_REPAIR_FAILED,
    CHECKPOINT_WRITE_FAILED,
    SOURCE_ROWS_PROCESSED,
    TARGET_ROWS_PROCESSED,
    ROWS_MISSING_ON_TARGET,
    ROWS_EXTRA_ON_TARGET,
    ROWS_CANNOT_REPAIR,
    CELLS_MISSING_ON_TARGET,
    CELLS_EXTRA_ON_TARGET,
    CELLS_DIFFERENT_ON_TARGET
  }

  private String tableName;
  private String targetZkQuorum;
  private Long fromTime;
  private Long toTime;
  private String tenantId;
  private boolean isDryRun;
  private long chunkSizeBytes;
  private boolean isRawScan;
  private boolean isReadAllVersions;
  private int repairBatchSize;
  private Configuration conf;
  private Connection sourceConnection;
  private Connection targetConnection;
  private Connection globalConnection;
  private PTable pTable;
  private byte[] physicalTableName;
  private List<KeyRange> regionKeyRanges;
  private PhoenixSyncTableOutputRepository syncTableOutputRepository;

  @Override
  protected void setup(Context context) throws InterruptedException {
    try {
      super.setup(context);
      this.conf = context.getConfiguration();
      tableName = PhoenixSyncTableTool.getPhoenixSyncTableName(conf);
      targetZkQuorum = PhoenixSyncTableTool.getPhoenixSyncTableTargetZkQuorum(conf);
      fromTime = PhoenixSyncTableTool.getPhoenixSyncTableFromTime(conf);
      toTime = PhoenixSyncTableTool.getPhoenixSyncTableToTime(conf);
      tenantId = PhoenixConfigurationUtil.getTenantId(conf);
      isDryRun = PhoenixSyncTableTool.getPhoenixSyncTableDryRun(conf);
      chunkSizeBytes = PhoenixSyncTableTool.getPhoenixSyncTableChunkSizeBytes(conf);
      isRawScan = PhoenixSyncTableTool.getPhoenixSyncTableRawScan(conf);
      isReadAllVersions = PhoenixSyncTableTool.getPhoenixSyncTableReadAllVersions(conf);
      repairBatchSize = PhoenixSyncTableTool.getPhoenixSyncTableRepairBatchSize(conf);
      extractRegionBoundariesFromSplit(context);
      sourceConnection = ConnectionUtil.getInputConnection(conf);
      pTable = sourceConnection.unwrap(PhoenixConnection.class).getTable(tableName);
      physicalTableName = pTable.getPhysicalName().getBytes();
      connectToTargetCluster();
      globalConnection = createGlobalConnection(conf);
      syncTableOutputRepository = new PhoenixSyncTableOutputRepository(globalConnection);
    } catch (Exception e) {
      tryClosingResources();
      throw new RuntimeException(
        String.format("Failed to setup PhoenixSyncTableMapper for table: %s", tableName), e);
    }
  }

  /**
   * Extracts region key ranges from the PhoenixInputSplit. Handles both single-region splits and
   * coalesced splits with multiple regions.
   */
  private void extractRegionBoundariesFromSplit(Context context) {
    PhoenixInputSplit split = (PhoenixInputSplit) context.getInputSplit();
    regionKeyRanges = split.getKeyRanges();

    if (regionKeyRanges == null || regionKeyRanges.isEmpty()) {
      throw new IllegalStateException(String.format(
        "PhoenixInputSplit has no KeyRanges for table: %s. Cannot determine region boundaries for sync operation.",
        tableName));
    }

    if (split.isCoalesced()) {
      LOGGER.info("Mapper processing coalesced split with {} regions for table {}",
        regionKeyRanges.size(), tableName);
    } else {
      LOGGER.info("Mapper processing single region split for table {}", tableName);
    }
  }

  /**
   * Connects to the target cluster using the target ZK quorum, port, znode, krb principal
   */
  private void connectToTargetCluster() throws SQLException, IOException {
    Configuration targetConf = HBaseConfiguration.createClusterConf(conf, targetZkQuorum);
    targetConnection = ConnectionUtil.getInputConnection(targetConf);
  }

  /**
   * Creates a global (non-tenant) connection for the checkpoint table.
   */
  private Connection createGlobalConnection(Configuration conf) throws SQLException {
    Configuration globalConf = new Configuration(conf);
    globalConf.unset(PhoenixConfigurationUtil.MAPREDUCE_TENANT_ID);
    return ConnectionUtil.getInputConnection(globalConf);
  }

  /**
   * Processes mapper region(s) by comparing chunks between source and target clusters. For
   * coalesced splits, processes each region sequentially. Gets already processed chunks from
   * checkpoint table, resumes from check pointed progress and records final status for chunks &
   * mapper (VERIFIED/MISMATCHED).
   */
  @Override
  protected void map(NullWritable key, DBInputFormat.NullDBWritable value, Context context)
    throws IOException, InterruptedException {
    context.getCounter(PhoenixJobCounters.INPUT_RECORDS).increment(1);
    try {
      // Process each region in the split (one or multiple for coalesced splits)
      for (KeyRange keyRange : regionKeyRanges) {
        byte[] regionStart = keyRange.getLowerRange();
        byte[] regionEnd = keyRange.getUpperRange();
        LOGGER.info("Processing region [{}, {}) from split for table {}",
          Bytes.toStringBinary(regionStart), Bytes.toStringBinary(regionEnd), tableName);
        processRegion(regionStart, regionEnd, context);
      }
    } catch (SQLException | IOException e) {
      tryClosingResources();
      throw new RuntimeException("Error processing PhoenixSyncTableMapper", e);
    }
  }

  /**
   * Processes a single region within a split (could be part of a coalesced split).
   * @param regionStart Start key of the region
   * @param regionEnd   End key of the region
   * @param context     Mapper context
   */
  private void processRegion(byte[] regionStart, byte[] regionEnd, Context context)
    throws SQLException, IOException, InterruptedException {

    Timestamp regionStartTime = new Timestamp(System.currentTimeMillis());

    // Get processed chunks for this specific region
    List<PhoenixSyncTableCheckpointOutputRow> processedChunks =
      syncTableOutputRepository.getProcessedChunks(tableName, targetZkQuorum, fromTime, toTime,
        tenantId, regionStart, regionEnd, isDryRun);

    // Calculate unprocessed ranges within this region
    List<KeyRange> unprocessedRanges =
      calculateUnprocessedRanges(regionStart, regionEnd, processedChunks);

    // Track counters before processing this region
    long verifiedBefore = context.getCounter(SyncCounters.CHUNKS_VERIFIED).getValue();
    long mismatchedBefore = context.getCounter(SyncCounters.CHUNKS_MISMATCHED).getValue();
    long unrepairableBefore = context.getCounter(SyncCounters.CHUNKS_UNREPAIRABLE).getValue();
    long repairFailedBefore = context.getCounter(SyncCounters.CHUNKS_REPAIR_FAILED).getValue();
    long sourceRowsBefore = context.getCounter(SyncCounters.SOURCE_ROWS_PROCESSED).getValue();
    long targetRowsBefore = context.getCounter(SyncCounters.TARGET_ROWS_PROCESSED).getValue();

    // Process all unprocessed ranges in this region
    boolean isStartKeyInclusive = shouldStartKeyBeInclusive(regionStart, processedChunks);
    for (KeyRange range : unprocessedRanges) {
      processMapperRanges(range.getLowerRange(), range.getUpperRange(), isStartKeyInclusive,
        context);
      isStartKeyInclusive = false;
    }

    // Calculate counters for this region only
    long verifiedChunks =
      context.getCounter(SyncCounters.CHUNKS_VERIFIED).getValue() - verifiedBefore;
    long mismatchedChunks =
      context.getCounter(SyncCounters.CHUNKS_MISMATCHED).getValue() - mismatchedBefore;
    long unrepairableChunks =
      context.getCounter(SyncCounters.CHUNKS_UNREPAIRABLE).getValue() - unrepairableBefore;
    long repairFailedChunks =
      context.getCounter(SyncCounters.CHUNKS_REPAIR_FAILED).getValue() - repairFailedBefore;
    long sourceRowsProcessed =
      context.getCounter(SyncCounters.SOURCE_ROWS_PROCESSED).getValue() - sourceRowsBefore;
    long targetRowsProcessed =
      context.getCounter(SyncCounters.TARGET_ROWS_PROCESSED).getValue() - targetRowsBefore;

    Timestamp regionEndTime = new Timestamp(System.currentTimeMillis());
    String counters = PhoenixSyncTableCheckpointOutputRow.CounterFormatter
      .formatMapper(verifiedChunks, mismatchedChunks, sourceRowsProcessed, targetRowsProcessed);
    if (sourceRowsProcessed > 0) {
      recordRegionCompletion(regionStart, regionEnd, regionStartTime, regionEndTime, verifiedChunks,
        mismatchedChunks, unrepairableChunks, repairFailedChunks, counters, context);
    } else {
      LOGGER.info(
        "No rows pending to process. All region boundaries are covered for startKey:{}, endKey: {}",
        Bytes.toStringBinary(regionStart), Bytes.toStringBinary(regionEnd));
    }
  }

  /**
   * Records region completion by updating counters, recording checkpoint, and logging result.
   * Consolidates all region completion logic to eliminate duplication.
   * @param regionStart        Region start key
   * @param regionEnd          Region end key
   * @param regionStartTime    Region processing start time
   * @param regionEndTime      Region processing end time
   * @param verifiedChunks     Number of verified chunks
   * @param mismatchedChunks   Number of mismatched chunks
   * @param unrepairableChunks Number of chunks where any row landed in ROWS_CANNOT_REPAIR;
   *                           if > 0 (and no repair-failed chunks) the region rolls up to
   *                           UNREPAIRABLE, signalling operator intervention is needed
   * @param repairFailedChunks Number of chunks whose repair threw an IOException; if > 0 the
   *                           region rolls up to REPAIR_FAILED (highest precedence)
   * @param counters           Formatted counter string
   * @param context            Mapper context
   */
  private void recordRegionCompletion(byte[] regionStart, byte[] regionEnd,
    Timestamp regionStartTime, Timestamp regionEndTime, long verifiedChunks, long mismatchedChunks,
    long unrepairableChunks, long repairFailedChunks, String counters, Context context)
    throws SQLException {

    // Region rolls up its child chunks' outcomes into one of five statuses, in precedence
    // order (most-severe wins):
    //   REPAIR_FAILED — at least one chunk threw during merge-scan or flush.
    //   UNREPAIRABLE  — repair completed but at least one chunk has rows that cannot be
    //                   repaired (target tombstones shadow source Puts, or target row is
    //                   entirely tombstones). Operator action (typically major compaction
    //                   on target) needed before a re-run can converge.
    //   MISMATCHED    — drift was detected but repair was not attempted (dry-run mode).
    //   REPAIRED      — drift was detected and every chunk's repair fully succeeded.
    //   VERIFIED      — every chunk matched; no drift in this region.
    // The resume filter on re-invocation skips VERIFIED and REPAIRED — UNREPAIRABLE,
    // MISMATCHED, and REPAIR_FAILED chunks are re-entered as gaps and re-attempted.
    PhoenixSyncTableCheckpointOutputRow.Status status;
    SyncCounters mapperCounter;
    if (mismatchedChunks == 0) {
      status = PhoenixSyncTableCheckpointOutputRow.Status.VERIFIED;
      mapperCounter = SyncCounters.MAPPERS_VERIFIED;
    } else if (isDryRun) {
      status = PhoenixSyncTableCheckpointOutputRow.Status.MISMATCHED;
      mapperCounter = SyncCounters.MAPPERS_MISMATCHED;
    } else if (repairFailedChunks > 0) {
      status = PhoenixSyncTableCheckpointOutputRow.Status.REPAIR_FAILED;
      mapperCounter = SyncCounters.MAPPERS_REPAIR_FAILED;
    } else if (unrepairableChunks > 0) {
      status = PhoenixSyncTableCheckpointOutputRow.Status.UNREPAIRABLE;
      mapperCounter = SyncCounters.MAPPERS_UNREPAIRABLE;
    } else {
      status = PhoenixSyncTableCheckpointOutputRow.Status.REPAIRED;
      mapperCounter = SyncCounters.MAPPERS_REPAIRED;
    }

    context.getCounter(mapperCounter).increment(1);

    recordRegionCheckpoint(regionStart, regionEnd, status, regionStartTime, regionEndTime,
      counters);

    String logMessage = String.format(
      "PhoenixSyncTable region [%s, %s) completed with %s: %d verified, %d mismatched, "
        + "%d unrepairable, %d repair-failed",
      Bytes.toStringBinary(regionStart), Bytes.toStringBinary(regionEnd),
      status.name().toLowerCase(), verifiedChunks, mismatchedChunks, unrepairableChunks,
      repairFailedChunks);

    if (
      status == PhoenixSyncTableCheckpointOutputRow.Status.MISMATCHED
        || status == PhoenixSyncTableCheckpointOutputRow.Status.UNREPAIRABLE
        || status == PhoenixSyncTableCheckpointOutputRow.Status.REPAIR_FAILED
    ) {
      LOGGER.warn(logMessage);
    } else {
      LOGGER.info(logMessage);
    }
  }

  /**
   * Records a region checkpoint to the checkpoint table.
   * @param regionStart     Region start key
   * @param regionEnd       Region end key
   * @param status          Status (VERIFIED or MISMATCHED)
   * @param regionStartTime Region processing start time
   * @param regionEndTime   Region processing end time
   * @param counters        Formatted counter string
   */
  private void recordRegionCheckpoint(byte[] regionStart, byte[] regionEnd,
    PhoenixSyncTableCheckpointOutputRow.Status status, Timestamp regionStartTime,
    Timestamp regionEndTime, String counters) throws SQLException {

    syncTableOutputRepository
      .checkpointSyncTableResult(new PhoenixSyncTableCheckpointOutputRow.Builder()
        .setTableName(tableName).setTargetCluster(targetZkQuorum)
        .setType(PhoenixSyncTableCheckpointOutputRow.Type.REGION).setFromTime(fromTime)
        .setToTime(toTime).setTenantId(tenantId).setIsDryRun(isDryRun).setStartRowKey(regionStart)
        .setEndRowKey(regionEnd).setStatus(status).setExecutionStartTime(regionStartTime)
        .setExecutionEndTime(regionEndTime).setCounters(counters).build());
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
        // Target scan boundary: covers extra-on-target rows that fall before the first
        // source chunk, between consecutive source chunks, or after the last. Both verify
        // and repair use the same range so repair sees the same cells the verifier hashed.
        byte[] targetStart =
          previousSourceChunk == null ? rangeStart : previousSourceChunk.endKey;
        byte[] targetEnd = isLastChunkOfRegion ? rangeEnd : sourceChunk.endKey;
        boolean targetEndInclusive = !isLastChunkOfRegion;
        ChunkInfo targetChunk = getTargetChunkWithSourceBoundary(targetConnection, targetStart,
          targetEnd, isTargetStartKeyInclusive, targetEndInclusive);
        context.getCounter(SyncCounters.SOURCE_ROWS_PROCESSED).increment(sourceChunk.rowCount);
        context.getCounter(SyncCounters.TARGET_ROWS_PROCESSED).increment(targetChunk.rowCount);
        boolean matched = MessageDigest.isEqual(sourceChunk.hash, targetChunk.hash);
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
            "isSourceStartKeyInclusive: {}, isTargetStartKeyInclusive: {},"
              + "isTargetEndKeyInclusive: {}, isFirstChunkOfRegion: {}, isLastChunkOfRegion: {}."
              + "Chunk comparison source {}, {}. Key range passed to target chunk: {}, {}."
              + "target chunk returned {}, {}: source={} rows, target={} rows, matched={}",
            isSourceStartKeyInclusive, isTargetStartKeyInclusive, targetEndInclusive,
            previousSourceChunk == null, isLastChunkOfRegion,
            Bytes.toStringBinary(sourceChunk.startKey), Bytes.toStringBinary(sourceChunk.endKey),
            Bytes.toStringBinary(targetStart), Bytes.toStringBinary(targetEnd),
            Bytes.toStringBinary(targetChunk.startKey), Bytes.toStringBinary(targetChunk.endKey),
            sourceChunk.rowCount, targetChunk.rowCount, matched);
        }
        sourceChunk.executionEndTime = new Timestamp(System.currentTimeMillis());
        String counters = PhoenixSyncTableCheckpointOutputRow.CounterFormatter
          .formatChunk(sourceChunk.rowCount, targetChunk.rowCount, 0L, 0L, 0L, 0L, 0L, 0L);
        if (matched) {
          handleVerifiedChunk(sourceChunk, context, counters);
        } else {
          handleMismatchedChunk(sourceChunk, context, counters);
          if (!isDryRun) {
            repairChunk(sourceChunk.startKey, sourceChunk.endKey, targetStart, targetEnd,
              isTargetStartKeyInclusive, targetEndInclusive, sourceChunk.rowCount,
              targetChunk.rowCount, sourceChunk.executionStartTime, context);
          }
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
    PhoenixConnection phoenixConn = conn.unwrap(PhoenixConnection.class);
    Table hTable = phoenixConn.getQueryServices().getTable(physicalTableName);
    Scan scan =
      createChunkScan(startKey, endKey, isStartKeyInclusive, isEndKeyInclusive, isTargetScan);
    scan.setAttribute(BaseScannerRegionObserverConstants.SYNC_TABLE_CHUNK_FORMATION, TRUE_BYTES);
    scan.setAttribute(BaseScannerRegionObserverConstants.SKIP_REGION_BOUNDARY_CHECK, TRUE_BYTES);
    scan.setAttribute(BaseScannerRegionObserverConstants.UNGROUPED_AGG, TRUE_BYTES);
    ScanUtil.setScanAttributesForPhoenixTTL(scan, pTable, phoenixConn);
    // Force strict TTL for sync tool regardless of table setting. Even when the table
    // uses non-strict TTL, the sync tool should only compare live rows.
    scan.setAttribute(BaseScannerRegionObserverConstants.IS_STRICT_TTL, TRUE_BYTES);
    if (continuedDigestState != null && continuedDigestState.length > 0) {
      scan.setAttribute(BaseScannerRegionObserverConstants.SYNC_TABLE_CONTINUED_DIGEST_STATE,
        continuedDigestState);
    }

    if (!isTargetScan) {
      scan.setAttribute(BaseScannerRegionObserverConstants.SYNC_TABLE_CHUNK_SIZE_BYTES,
        Bytes.toBytes(chunkSizeBytes));
    }
    ScanUtil.setScanAttributeForPaging(scan, phoenixConn);
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

  private void handleVerifiedChunk(ChunkInfo sourceChunk, Context context, String counters)
    throws SQLException {
    recordChunkCheckpoint(sourceChunk, PhoenixSyncTableCheckpointOutputRow.Status.VERIFIED,
      counters);
    context.getCounter(SyncCounters.CHUNKS_VERIFIED).increment(1);
  }

  private void handleMismatchedChunk(ChunkInfo sourceChunk, Context context, String counters)
    throws SQLException {
    LOGGER.warn("Chunk mismatch detected for table: {}, with startKey: {}, endKey {}", tableName,
      Bytes.toStringBinary(sourceChunk.startKey), Bytes.toStringBinary(sourceChunk.endKey));
    recordChunkCheckpoint(sourceChunk, PhoenixSyncTableCheckpointOutputRow.Status.MISMATCHED,
      counters);
    context.getCounter(SyncCounters.CHUNKS_MISMATCHED).increment(1);
  }

  /**
   * Records a chunk checkpoint to the checkpoint table.
   * @param sourceChunk Chunk information
   * @param status      Status (VERIFIED or MISMATCHED)
   * @param counters    Formatted counter string
   */
  private void recordChunkCheckpoint(ChunkInfo sourceChunk,
    PhoenixSyncTableCheckpointOutputRow.Status status, String counters) throws SQLException {

    syncTableOutputRepository.checkpointSyncTableResult(
      new PhoenixSyncTableCheckpointOutputRow.Builder().setTableName(tableName)
        .setTargetCluster(targetZkQuorum).setType(PhoenixSyncTableCheckpointOutputRow.Type.CHUNK)
        .setFromTime(fromTime).setToTime(toTime).setTenantId(tenantId).setIsDryRun(isDryRun)
        .setStartRowKey(sourceChunk.startKey).setEndRowKey(sourceChunk.endKey).setStatus(status)
        .setExecutionStartTime(sourceChunk.executionStartTime)
        .setExecutionEndTime(sourceChunk.executionEndTime).setCounters(counters).build());
  }

  /**
   * Builds the common Scan shape used by verification, repair, and tombstone loading: key
   * range, inclusivity, time window, cache-blocks, plus raw-scan and all-versions semantics
   * controlled by the user's {@code --raw-scan} / {@code --read-all-versions} flags.
   *
   * Callers that need to force raw-scan or all-versions on a specific call (e.g., the
   * tombstone loader, which must surface tombstones regardless of user flags) pass
   * {@code forceRaw=true} or {@code forceAllVersions=true} to override.
   *
   * Callers layer on their own caching, limits, and coprocessor attributes. Keeping the
   * base shared guarantees that the cells visited by repair are exactly the cells the
   * verifier hashed (when both pass the same force flags).
   */
  private Scan createBaseScan(byte[] startKey, byte[] endKey, boolean isStartKeyInclusive,
    boolean isEndKeyInclusive, boolean forceRaw, boolean forceAllVersions) throws IOException {
    Scan scan = new Scan();
    scan.withStartRow(startKey, isStartKeyInclusive);
    scan.withStopRow(endKey, isEndKeyInclusive);
    scan.setRaw(forceRaw || isRawScan);
    if (forceAllVersions || isReadAllVersions) {
      scan.readAllVersions();
    }
    scan.setCacheBlocks(false);
    scan.setTimeRange(fromTime, toTime);
    return scan;
  }

  /**
   * Creates an HBase scan for a chunk range. Can be configured to use raw scan mode and read all
   * cell versions based on command-line options.
   */
  private Scan createChunkScan(byte[] startKey, byte[] endKey, boolean isStartKeyInclusive,
    boolean isEndKeyInclusive, boolean isTargetScan) throws IOException {
    Scan scan =
      createBaseScan(startKey, endKey, isStartKeyInclusive, isEndKeyInclusive, false, false);
    // Set limit and caching to 1 for sequential partial digest retrieval from target.
    // Enables digest continuation: each target chunk's digest feeds into the next until scanning
    // completes
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
   * @return List of KeyRange representing unprocessed ranges
   */
  List<KeyRange> calculateUnprocessedRanges(byte[] mapperRegionStart, byte[] mapperRegionEnd,
    List<PhoenixSyncTableCheckpointOutputRow> processedChunks) {
    List<KeyRange> gaps = new ArrayList<>();
    // If processedChunks is null or empty, the entire mapper region needs processing
    if (processedChunks == null || processedChunks.isEmpty()) {
      gaps.add(KeyRange.getKeyRange(mapperRegionStart, mapperRegionEnd));
      return gaps;
    }

    // Since chunk keys are always inclusive(start/endKey) it would never be null/empty.
    // But Mapper region boundary are exclusive and can be empty i.e [] for start/end region of
    // table.
    // We would be doing byte comparison of chunk with region boundary as part of identifying gaps
    // and empty bytes(specially end of region) needs to be considered as special case as comparison
    // won't work on them.
    boolean isEndRegionOfTable = mapperRegionEnd == null || mapperRegionEnd.length == 0;

    // Track our scanning position through the mapper region as we iterate through chunks
    byte[] scanPos = mapperRegionStart;

    // With Mapper region boundary, we iterate over each chunk and if any gap/hole identified
    // in Mapper region range which is not covered by processed chunk, we add it to gaps list.
    for (PhoenixSyncTableCheckpointOutputRow chunk : processedChunks) {
      byte[] chunkStart = chunk.getStartRowKey();
      byte[] chunkEnd = chunk.getEndRowKey();

      // Clip start: max(chunkStart, mapperRegionStart)
      // Clip any chunk boundary which is outside of mapperRegionStart
      // Technically, clipping will only be applied for first chunk i.e
      // -----[---Mapper---]
      // --[chunk]
      byte[] effectiveStart =
        Bytes.compareTo(chunkStart, mapperRegionStart) > 0 ? chunkStart : mapperRegionStart;

      // Clip end: min(chunkEnd, mapperRegionEnd) : Skip clipping for end-of-table since []
      // semantically means infinity
      // Clip any chunk boundary which is outside of mapperRegionEnd
      // Technically, clipping will only be applied for last chunk i.e
      // ---[---Mapper---]
      // --------------[chunk]
      byte[] effectiveEnd = chunkEnd;
      if (!isEndRegionOfTable && Bytes.compareTo(chunkEnd, mapperRegionEnd) > 0) {
        effectiveEnd = mapperRegionEnd;
      }

      // If scanPos is behind effectiveStart, there's an unprocessed gap:
      // --[---Mapper Region------------------------------)
      // ----[--chunk--]-----------[--chunk2--------]---[--chunk3]
      // --------------|scanPos----|effectiveStart
      // --------------|<---gap--->| // Add this gap
      // Start and end key of gap would be considered exclusive while processing this range
      if (Bytes.compareTo(scanPos, effectiveStart) < 0) {
        gaps.add(KeyRange.getKeyRange(scanPos, effectiveStart));
      }

      // Advance scanPos past this processed chunk to look for the next gap:
      // --[---Mapper Region------------------------------)
      // ----[--chunk--]-----------[--chunk2--------]---[--chunk3]
      // -------------------------------------------|scanPos
      scanPos = effectiveEnd;
    }

    // Since Mapper region end is exclusive, we want to add any remaining key boundary as gaps
    // except when scanPos == mapperRegionEnd (i.e end of Mapper region boundary got covered by
    // chunk)
    if (isEndRegionOfTable || Bytes.compareTo(scanPos, mapperRegionEnd) < 0) {
      gaps.add(KeyRange.getKeyRange(scanPos, mapperRegionEnd));
    }
    return gaps;
  }

  /***
   * Checking if start key should be inclusive, this is specific to scenario when there are
   * processed chunks within this Mapper region boundary.
   */
  boolean shouldStartKeyBeInclusive(byte[] mapperRegionStart,
    List<PhoenixSyncTableCheckpointOutputRow> processedChunks) {
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

  /**
   * Builds a row-level HBase scan for repair. Differs from {@link #createChunkScan} in that it
   * does NOT set {@code SYNC_TABLE_CHUNK_FORMATION} or {@code SYNC_TABLE_CHUNK_SIZE_BYTES}, so
   * the scanner returns actual {@link Result} rows rather than coprocessor chunk metadata.
   * Shares the {@link #createBaseScan} core (time range, raw-scan, all-versions, inclusivity)
   * with verification so the cells visited here are the same cells that produced the chunk
   * hash. Adds bulk caching plus Phoenix TTL / {@code IS_STRICT_TTL} attributes.
   */
  private Scan createRepairScan(byte[] startKey, byte[] endKey, boolean isStartKeyInclusive,
    boolean isEndKeyInclusive, PhoenixConnection phoenixConn) throws IOException, SQLException {
    Scan scan =
      createBaseScan(startKey, endKey, isStartKeyInclusive, isEndKeyInclusive, false, false);
    scan.setCaching(1000);
    ScanUtil.setScanAttributesForPhoenixTTL(scan, pTable, phoenixConn);
    scan.setAttribute(BaseScannerRegionObserverConstants.IS_STRICT_TTL, TRUE_BYTES);
    return scan;
  }

  /**
   * Loads the target row's tombstone index for shadow detection. Issues a single-row scan
   * against target with raw=true and all-versions forced (regardless of user flags), so the
   * tombstone subtypes that would otherwise be filtered out are surfaced. Live cells in the
   * response are ignored — they were already visible to the repair scan and are handled by
   * the merge logic. (HBase 2.x exposes {@code setRaw} only on Scan, not on Get — so a
   * one-row scan stands in for what would otherwise be a raw Get.)
   *
   * Time range: the base scan applies {@code [fromTime, toTime]}, but shadow detection
   * needs tombstones at ts >= fromTime regardless of the upper bound. A DeleteColumn /
   * DeleteFamily at ts > toTime can still shadow a Put we mirror at ts in window during
   * application reads (HBase tombstones don't respect the verifier's sync window). Lower
   * bound stays at fromTime since tombstones below the window can't shadow anything we'd
   * write inside the window.
   */
  private TargetRowTombstones loadTargetRowTombstones(byte[] rowKey, Table targetHTable)
    throws IOException {
    Scan scan = createBaseScan(rowKey, rowKey, true, true, true, true);
    scan.setTimeRange(fromTime, Long.MAX_VALUE);
    scan.setCaching(1);
    scan.setLimit(1);
    TargetRowTombstones tombstones = new TargetRowTombstones();
    try (ResultScanner scanner = targetHTable.getScanner(scan)) {
      Result raw = scanner.next();
      if (raw != null) {
        for (Cell cell : raw.rawCells()) {
          // Record both tombstones (for shadow detection) and Puts (for hidden-version
          // discovery during cmp > 0 tombstoning of target-only cells).
          tombstones.record(cell);
        }
      }
    }
    return tombstones;
  }

  /**
   * Lazily-built per-row Put and Delete mutations plus per-row unrepairable-drift state.
   * Each mutation field is created on first use so a row that needs only Puts produces no
   * Delete (and vice versa); a row that needs no mutation at all produces neither.
   *
   * Tombstone-index state is lazy: {@link #tombstones} is loaded only on the first
   * shadow check via {@link #tombstones(Table)} (one raw scan per row at most).
   * {@link #anyCellUnrepairable} accumulates whether the row carries any drift that
   * repair could not act on — either a source-side Put was shadow-suppressed by an
   * existing target tombstone, or a target-only tombstone exists that source lacks
   * (HBase has no API to remove tombstones). The caller reads it after the merge to
   * decide whether to bump {@link SyncCounters#ROWS_CANNOT_REPAIR}.
   *
   * After construction, callers append produced mutations to the pending batches via
   * {@link #flush(List, List)}.
   */
  private final class RowRepairState {
    private final byte[] rowKey;
    Put put;
    Delete delete;
    TargetRowTombstones tombstones;
    boolean anyCellUnrepairable;

    RowRepairState(byte[] rowKey) {
      this.rowKey = rowKey;
    }

    Put put() {
      if (put == null) {
        put = new Put(rowKey);
      }
      return put;
    }

    Delete delete() {
      if (delete == null) {
        delete = new Delete(rowKey);
      }
      return delete;
    }

    /**
     * Lazily loads the target row's tombstone index on first call (one raw single-row scan
     * via {@link #loadTargetRowTombstones}); cached thereafter for reuse across the row's
     * subsequent shadow checks.
     */
    TargetRowTombstones targetRowTombstones(Table targetHTable) throws IOException {
      if (tombstones == null) {
        tombstones = loadTargetRowTombstones(rowKey, targetHTable);
      }
      return tombstones;
    }

    void flush(List<Put> pendingPuts, List<Delete> pendingDeletes) {
      if (put != null) {
        pendingPuts.add(put);
      }
      if (delete != null) {
        pendingDeletes.add(delete);
      }
    }
  }

  /**
   * Cell-level drift counts produced by {@link #diffCellsForRow}. Populated only for rows
   * present on both clusters; whole-row drift is signaled by the caller directly at the
   * {@code cmp != 0} branches in {@link #repairChunk}. Three counters partition the cell
   * differences into disjoint buckets — source-only, target-only-live, same-coord-diff-value.
   */
  private static final class CellDriftCounts {
    static final CellDriftCounts NONE = new CellDriftCounts(0, 0, 0);

    final int missing;
    final int extra;
    final int different;

    CellDriftCounts(int missing, int extra, int different) {
      this.missing = missing;
      this.extra = extra;
      this.different = different;
    }
  }

  /**
   * Per-row index of target's tombstones AND Puts in {@code [fromTime, MAX_VALUE]}, built
   * lazily from a single raw single-row scan with all-versions enabled. Used in two roles:
   *
   * <ol>
   *   <li><b>Shadow detection</b> ({@link #wouldShadow}): would a source Put we're about
   *       to mirror be suppressed by an existing target tombstone?</li>
   *   <li><b>Hidden-version discovery</b> ({@link #targetPutTimestampsBetween}): what
   *       max-versions-filtered target Puts sit between source's max ts at a column and
   *       target's visible Put? Used in the {@code cmp > 0} tombstoning path to issue
   *       point Deletes for hidden versions that would otherwise surface on read after
   *       the visible Put is shadowed.</li>
   * </ol>
   *
   * HBase has four tombstone subtypes, each with distinct shadow semantics:
   *   Delete                — shadows a Put at {@code (cf, q, ts == T)} exactly
   *   DeleteColumn          — shadows Puts at {@code (cf, q, ts <= T)}
   *   DeleteFamily          — shadows Puts at {@code (cf, *, ts <= T)}
   *   DeleteFamilyVersion   — shadows Puts at {@code (cf, *, ts == T)}
   * {@link #wouldShadow(Cell)} consults all four indices and returns true on any match.
   */
  private static final class TargetRowTombstones {
    private final Map<ColumnKey, Set<Long>> deletePointTs = new HashMap<>();
    private final Map<ColumnKey, Long> deleteColumnUpperBound = new HashMap<>();
    private final Map<ByteBuffer, Long> deleteFamilyUpperBound = new HashMap<>();
    private final Map<ByteBuffer, Set<Long>> deleteFamilyVersionTs = new HashMap<>();
    /** Per-column ts-ordered set of target's Put timestamps. */
    private final Map<ColumnKey, NavigableMap<Long, Boolean>> targetPutTs = new HashMap<>();

    void record(Cell cell) {
      if (CellUtil.isDelete(cell)) {
        recordTombstone(cell);
      } else {
        targetPutTs.computeIfAbsent(columnKey(cell), k -> new TreeMap<>())
          .put(cell.getTimestamp(), Boolean.TRUE);
      }
    }

    private void recordTombstone(Cell tombstone) {
      long ts = tombstone.getTimestamp();
      ByteBuffer family = ByteBuffer.wrap(CellUtil.cloneFamily(tombstone));
      switch (tombstone.getType()) {
        case Delete:
          deletePointTs.computeIfAbsent(columnKey(tombstone), k -> new HashSet<>()).add(ts);
          break;
        case DeleteColumn:
          deleteColumnUpperBound.merge(columnKey(tombstone), ts, Math::max);
          break;
        case DeleteFamily:
          deleteFamilyUpperBound.merge(family, ts, Math::max);
          break;
        case DeleteFamilyVersion:
          deleteFamilyVersionTs.computeIfAbsent(family, k -> new HashSet<>()).add(ts);
          break;
        default:
          // Caller filters via CellUtil.isDelete; non-tombstone cells should never reach here.
      }
    }

    /** Returns true if any tombstone in this index would shadow a Put at the cell's coords. */
    boolean wouldShadow(Cell sourcePut) {
      long ts = sourcePut.getTimestamp();
      ByteBuffer family = ByteBuffer.wrap(CellUtil.cloneFamily(sourcePut));
      ColumnKey column = columnKey(sourcePut);

      Set<Long> pointTs = deletePointTs.get(column);
      if (pointTs != null && pointTs.contains(ts)) {
        return true;
      }
      Long deleteColTs = deleteColumnUpperBound.get(column);
      if (deleteColTs != null && ts <= deleteColTs) {
        return true;
      }
      Long deleteFamTs = deleteFamilyUpperBound.get(family);
      if (deleteFamTs != null && ts <= deleteFamTs) {
        return true;
      }
      Set<Long> dfvTs = deleteFamilyVersionTs.get(family);
      return dfvTs != null && dfvTs.contains(ts);
    }

    /**
     * Returns target's Put timestamps at {@code (cf, q)} that are strictly greater than
     * {@code lowerExclusive} and strictly less than {@code upperExclusive}. Used to find
     * hidden (max-versions-filtered) target versions sitting between source's max ts and
     * target's visible ts so they can be point-Deleted.
     */
    Set<Long> targetPutTimestampsBetween(byte[] family, byte[] qualifier, long lowerExclusive,
      long upperExclusive) {
      NavigableMap<Long, Boolean> tss = targetPutTs.get(new ColumnKey(family, qualifier));
      if (tss == null) {
        return Collections.emptySet();
      }
      return tss.subMap(lowerExclusive, false, upperExclusive, false).keySet();
    }

    private static ColumnKey columnKey(Cell cell) {
      return new ColumnKey(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell));
    }
  }

  /** Composite (family, qualifier) key with byte-array equality semantics. */
  private static final class ColumnKey {
    private final byte[] family;
    private final byte[] qualifier;

    ColumnKey(byte[] family, byte[] qualifier) {
      this.family = family;
      this.qualifier = qualifier;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof ColumnKey)) {
        return false;
      }
      ColumnKey other = (ColumnKey) o;
      return Bytes.equals(family, other.family) && Bytes.equals(qualifier, other.qualifier);
    }

    @Override
    public int hashCode() {
      return Bytes.hashCode(family) * 31 + Bytes.hashCode(qualifier);
    }
  }

  /**
   * Result of {@link #diffCellsForRow}. Carries the cell-level drift counts plus a flag
   * indicating whether any source-side mirror was suppressed because target tombstones
   * would shadow it — making the row partially or fully unrepairable.
   */
  private static final class RowDiffOutcome {
    static final RowDiffOutcome NONE = new RowDiffOutcome(CellDriftCounts.NONE, false);

    final CellDriftCounts cells;
    final boolean rowCannotRepair;

    RowDiffOutcome(CellDriftCounts cells, boolean rowCannotRepair) {
      this.cells = cells;
      this.rowCannotRepair = rowCannotRepair;
    }
  }

  /**
   * Per-chunk aggregate of all six drift counters: three row-level (whole rows missing /
   * extra on target, plus rows that cannot be repaired because target's row is entirely
   * tombstones — HBase has no API to remove tombstones, only major compaction does) plus
   * three cell-level (cells missing / extra / different on rows present on both clusters).
   * Owns the bookkeeping that was previously scattered across {@link #repairChunk} — local
   * accumulators, MapReduce job-counter increments, the
   * {@link PhoenixSyncTableCheckpointOutputRow.CounterFormatter#formatChunk} call, and the
   * end-of-chunk log line. Adding a new drift signal means touching this class and the one
   * place in the merge loop that produces it; everything else (commit to job context,
   * checkpoint COUNTERS string, log) flows through these methods.
   */
  private static final class DriftCounters {
    long rowsMissingOnTarget;
    long rowsExtraOnTarget;
    long rowsCannotRepair;
    long cellsMissingOnTarget;
    long cellsExtraOnTarget;
    long cellsDifferentOnTarget;

    void addCellDrift(CellDriftCounts cellDrift) {
      cellsMissingOnTarget += cellDrift.missing;
      cellsExtraOnTarget += cellDrift.extra;
      cellsDifferentOnTarget += cellDrift.different;
    }

    /** Increments the job's MapReduce counters with this chunk's drift totals. */
    void commitTo(Context context) {
      context.getCounter(SyncCounters.ROWS_MISSING_ON_TARGET).increment(rowsMissingOnTarget);
      context.getCounter(SyncCounters.ROWS_EXTRA_ON_TARGET).increment(rowsExtraOnTarget);
      context.getCounter(SyncCounters.ROWS_CANNOT_REPAIR).increment(rowsCannotRepair);
      context.getCounter(SyncCounters.CELLS_MISSING_ON_TARGET).increment(cellsMissingOnTarget);
      context.getCounter(SyncCounters.CELLS_EXTRA_ON_TARGET).increment(cellsExtraOnTarget);
      context.getCounter(SyncCounters.CELLS_DIFFERENT_ON_TARGET).increment(cellsDifferentOnTarget);
    }

    /** Formats the chunk's COUNTERS string for persistence in the checkpoint table. */
    String formatChunkCounters(long verifySourceRows, long verifyTargetRows) {
      return PhoenixSyncTableCheckpointOutputRow.CounterFormatter.formatChunk(verifySourceRows,
        verifyTargetRows, rowsMissingOnTarget, rowsExtraOnTarget, rowsCannotRepair,
        cellsMissingOnTarget, cellsExtraOnTarget, cellsDifferentOnTarget);
    }

    /** Compact end-of-chunk log line summarizing all six drift signals. */
    String toLogString() {
      return String.format(
        "rowsMissingOnTarget=%d, rowsExtraOnTarget=%d, rowsCannotRepair=%d, "
          + "cellsMissingOnTarget=%d, cellsExtraOnTarget=%d, cellsDifferentOnTarget=%d",
        rowsMissingOnTarget, rowsExtraOnTarget, rowsCannotRepair, cellsMissingOnTarget,
        cellsExtraOnTarget, cellsDifferentOnTarget);
    }
  }

  /**
   * Routes a source cell to the right mutation kind. Put cells go to a {@link Put}; tombstone
   * cells go to a {@link Delete} via {@link Delete#add(Cell)} which preserves the tombstone's
   * exact subtype (Delete / DeleteColumn / DeleteFamily / DeleteFamilyVersion). Required under
   * {@code --raw-scan}: {@link Put#add(Cell)} rejects non-Put cells.
   */
  private void mirrorSourceCell(Cell cell, RowRepairState rowState) throws IOException {
    if (CellUtil.isDelete(cell)) {
      rowState.delete().add(cell);
    } else {
      rowState.put().add(cell);
    }
  }

  /**
   * Mirrors a source cell onto target only if no existing target tombstone would shadow
   * the resulting Put. Lazily loads the target row's tombstone index on first Put cell
   * (one raw single-row scan per row at most) via {@link RowRepairState#tombstones}. If
   * the cell would be shadowed, the mirror is suppressed, {@code rowState.anyCellUnrepairable}
   * is set, and the caller is expected to record {@link SyncCounters#ROWS_CANNOT_REPAIR}.
   *
   * Shadow detection only applies to Put cells. Tombstone source cells (under
   * {@code --raw-scan}) bypass the check and always mirror via {@link #mirrorSourceCell}:
   * {@link TargetRowTombstones#wouldShadow} is defined for live cells (does an existing target
   * tombstone hide a new Put on read), and is not meaningful for delete-marker cells —
   * mirroring a tombstone over an existing tombstone is a benign duplicate, not a suppression.
   *
   * @return {@code true} if the cell was mirrored, {@code false} if suppressed by shadowing.
   */
  private boolean mirrorSourceCellUnlessShadowed(Cell cell, Table targetHTable,
      RowRepairState rowState) throws IOException {
    if (!CellUtil.isDelete(cell) && rowState.targetRowTombstones(targetHTable).wouldShadow(cell)) {
      rowState.anyCellUnrepairable = true;
      return false;
    }
    mirrorSourceCell(cell, rowState);
    return true;
  }

  /**
   * Tombstones a target-only cell to make target's read view at this column match source's.
   * Skips cells that are themselves already tombstones: HBase has no API to remove a
   * tombstone cell — tombstones can only be reaped by major compaction once they age past
   * the keep-deleted-cells window. Issuing another Delete at the same coordinates writes a
   * duplicate marker, does not change the row's effective state, and only adds compaction
   * load. Combined with the absence of a source-side counterpart to mirror, the right
   * action is to leave the existing tombstone untouched.
   *
   * Tombstone subtype depends on what source has at this {@code (cf, q)}:
   * <ul>
   *   <li><b>Source has no cell at this column</b> — target's read at this column should
   *       be empty. Use {@link Delete#addColumns(byte[], byte[], long)} (DeleteColumn,
   *       scope {@code ts <= T}) so even max-versions-hidden older target versions are
   *       shadowed. A point Delete would only shadow this exact ts, surfacing the next
   *       hidden version on read.</li>
   *   <li><b>Source's max ts at this column is >= target's ts</b> — point-Delete only
   *       target's exact ts. Source's equal-or-higher ts mirror will surface; no hidden
   *       version can surface above it.</li>
   *   <li><b>Source's max ts at this column is &lt; target's ts</b> — point-Delete target's
   *       ts, AND point-Delete every max-versions-hidden Put on target with ts in
   *       {@code (sourceMaxTs, targetTs)}. Otherwise after target's visible cell is
   *       shadowed, the next hidden version surfaces above source's mirror — silent
   *       divergence. Hidden versions are discovered via the row's tombstone+Put index
   *       loaded from a single raw all-versions scan.</li>
   * </ul>
   *
   * Pre-build {@code sourceMaxTsByColumn} once per row from the source cell array.
   *
   * @return true if the cell was a live cell that contributed a tombstone marker, false if
   *         the cell was already a tombstone and was skipped.
   */
  private boolean tombstoneTargetCell(Cell cell, Table targetHTable, RowRepairState rowState,
    Map<ColumnKey, Long> sourceMaxTsByColumn) throws IOException {
    if (CellUtil.isDelete(cell)) {
      return false;
    }
    byte[] family = CellUtil.cloneFamily(cell);
    byte[] qualifier = CellUtil.cloneQualifier(cell);
    long ts = cell.getTimestamp();
    Long sourceMaxTs = sourceMaxTsByColumn.get(new ColumnKey(family, qualifier));
    if (sourceMaxTs == null) {
      // Source has no cell at this column; shadow ts <= T so hidden older versions
      // don't surface on read.
      rowState.delete().addColumns(family, qualifier, ts);
    } else if (sourceMaxTs >= ts) {
      // Source's mirror is at >= target's ts; point-Delete only target's exact ts.
      rowState.delete().addColumn(family, qualifier, ts);
    } else {
      // Source's max ts at this column < target's ts. Point-Delete target's visible cell,
      // AND point-Delete every hidden target Put in (sourceMaxTs, ts) so they don't
      // surface above source's mirror after the visible cell is shadowed.
      rowState.delete().addColumn(family, qualifier, ts);
      Set<Long> hiddenTs = rowState.targetRowTombstones(targetHTable)
        .targetPutTimestampsBetween(family, qualifier, sourceMaxTs, ts);
      for (Long hidden : hiddenTs) {
        rowState.delete().addColumn(family, qualifier, hidden);
      }
    }
    return true;
  }

  /**
   * Outcome of {@link #mirrorWholeRow}. {@code FULLY_MIRRORED} = every source cell mirrored
   * onto target with no shadowing. {@code PARTIALLY_MIRRORED} = at least one cell mirrored,
   * at least one suppressed by an existing target tombstone (caller bumps both
   * ROWS_MISSING_ON_TARGET and ROWS_CANNOT_REPAIR). {@code FULLY_SHADOWED} = every source
   * cell suppressed; the row is unrepairable until target's tombstones are reaped (caller
   * bumps only ROWS_CANNOT_REPAIR).
   */
  private enum WholeRowMirrorOutcome {
    FULLY_MIRRORED, PARTIALLY_MIRRORED, FULLY_SHADOWED
  }

  /**
   * Mirrors every source cell of a row that is missing on target. Source cells route by
   * type: live cells to a Put, tombstone cells (under {@code --raw-scan}) to a Delete via
   * {@link Delete#add(Cell)}. Each cell is shadow-checked against existing target
   * tombstones (lazy raw single-row scan on first cell) — even though target's filtered scan
   * returned no cells for this row, target may carry tombstones that would shadow our Put.
   */
  private WholeRowMirrorOutcome mirrorWholeRow(Result sourceResult, Table targetHTable,
    List<Put> pendingPuts, List<Delete> pendingDeletes) throws IOException {
    RowRepairState rowState = new RowRepairState(sourceResult.getRow());
    int mirrored = 0;
    for (Cell cell : sourceResult.rawCells()) {
      if (mirrorSourceCellUnlessShadowed(cell, targetHTable, rowState)) {
        mirrored++;
      }
    }
    rowState.flush(pendingPuts, pendingDeletes);
    if (mirrored == 0) {
      return WholeRowMirrorOutcome.FULLY_SHADOWED;
    }
    return rowState.anyCellUnrepairable ? WholeRowMirrorOutcome.PARTIALLY_MIRRORED
      : WholeRowMirrorOutcome.FULLY_MIRRORED;
  }

  /**
   * Tombstones every live cell of a row that is extra on target. Existing tombstones on the
   * target row are skipped — HBase cannot remove tombstone cells; only major compaction
   * reaps them.
   *
   * @return the number of live cells that contributed a tombstone marker. {@code 0} means
   *         the row was already entirely tombstones — repair could not act on it, and the
   *         caller should record this as {@link SyncCounters#ROWS_CANNOT_REPAIR}.
   */
  private int tombstoneWholeRow(Result targetResult, Table targetHTable,
    List<Put> pendingPuts, List<Delete> pendingDeletes) throws IOException {
    RowRepairState rowState = new RowRepairState(targetResult.getRow());
    // Source has no row at all here, so source has no cell at any column — every target
    // cell hits the "shadow ts <= T" path inside tombstoneTargetCell.
    Map<ColumnKey, Long> sourceMaxTsByColumn = Collections.emptyMap();
    int liveCellsTombstoned = 0;
    for (Cell cell : targetResult.rawCells()) {
      if (tombstoneTargetCell(cell, targetHTable, rowState, sourceMaxTsByColumn)) {
        liveCellsTombstoned++;
      }
    }
    rowState.flush(pendingPuts, pendingDeletes);
    return liveCellsTombstoned;
  }

  /**
   * Diffs cells of two rows present on both clusters in lock-step using {@link CellComparator}
   * order and appends the resulting {@link Put}/{@link Delete} mutations (if any) to the
   * pending lists. Returns a {@link RowDiffOutcome} carrying both the cell-level drift
   * counts and a flag indicating whether any source-side mirror was suppressed by target
   * tombstone shadowing — letting the caller decide whether to bump
   * {@link SyncCounters#ROWS_CANNOT_REPAIR}.
   *
   * Branches:
   *   same coords + matching value         → no drift, no signal
   *   same coords + different value        → different++; mirror source cell (shadow-checked)
   *   source-only cell at unique coords    → missing++;   mirror source cell (shadow-checked)
   *   target-only live cell at unique coords → extra++;   tombstone target cell
   *   target-only tombstone cell           → skip (HBase cannot remove tombstones)
   *
   * Cells whose mirror is suppressed by shadowing do NOT bump the cell counter — the
   * cell wasn't written to target, so it isn't repaired drift. The row-level shadow
   * signal is surfaced via {@link RowDiffOutcome#rowCannotRepair}.
   */
  private RowDiffOutcome diffCellsForRow(Result sourceResult, Result targetResult,
    Table targetHTable, List<Put> pendingPuts, List<Delete> pendingDeletes) throws IOException {
    Cell[] sourceCells = sourceResult.rawCells();
    Cell[] targetCells = targetResult.rawCells();
    CellComparator comparator = CellComparator.getInstance();

    // Always use the lazy raw single-row fetch path for shadow detection. Even under
    // --raw-scan, the merge-scan's targetCells came from a time-range-filtered scan and
    // would not surface tombstones at ts > toTime — and those out-of-window tombstones
    // can still shadow Puts we mirror inside the window during application reads. The
    // lazy fetch in {@link RowRepairState#tombstones} loads tombstones at ts >= fromTime
    // regardless of upper bound, catching that case correctly.
    RowRepairState rowState = new RowRepairState(sourceResult.getRow());

    // Pre-compute the set of (cf, q) coordinates source has any cell at. tombstoneTargetCell
    // uses this to choose between point-Delete (when source has the column at some ts —
    // mirrored cell will surface on read) and DeleteColumn (when source has nothing at the
    // column — must shadow ts <= T to keep max-versions-hidden older versions invisible.
    // The map records source's max ts at each column. When tombstoning a target cell at a
    // column source has at lower ts, hidden target Puts in (sourceMax, targetTs) need
    // their own point Deletes too — otherwise after the visible cell is shadowed, a hidden
    // version surfaces above source's mirror.
    Map<ColumnKey, Long> sourceMaxTsByColumn = new HashMap<>();
    for (Cell sourceCell : sourceCells) {
      if (!CellUtil.isDelete(sourceCell)) {
        ColumnKey key = new ColumnKey(CellUtil.cloneFamily(sourceCell),
          CellUtil.cloneQualifier(sourceCell));
        sourceMaxTsByColumn.merge(key, sourceCell.getTimestamp(), Math::max);
      }
    }

    int cellMissing = 0;
    int cellExtra = 0;
    int cellDifferent = 0;

    int sourceIdx = 0;
    int targetIdx = 0;
    while (sourceIdx < sourceCells.length && targetIdx < targetCells.length) {
      int cmp = comparator.compare(sourceCells[sourceIdx], targetCells[targetIdx]);
      if (cmp == 0) {
        // Same coordinates; CellComparator does not compare values, check separately.
        if (!CellUtil.matchingValue(sourceCells[sourceIdx], targetCells[targetIdx])) {
          if (mirrorSourceCellUnlessShadowed(sourceCells[sourceIdx], targetHTable, rowState)) {
            cellDifferent++;
          }
        }
        sourceIdx++;
        targetIdx++;
      } else if (cmp < 0) {
        if (mirrorSourceCellUnlessShadowed(sourceCells[sourceIdx], targetHTable, rowState)) {
          cellMissing++;
        }
        sourceIdx++;
      } else {
        // Target-only cell. Live cell → tombstone it (cellExtra++). Tombstone cell →
        // can't act on it (HBase has no API to remove tombstones), so the row carries
        // unrepairable drift and re-verify will mismatch.
        if (tombstoneTargetCell(targetCells[targetIdx++], targetHTable, rowState,
          sourceMaxTsByColumn)) {
          cellExtra++;
        } else {
          rowState.anyCellUnrepairable = true;
        }
      }
    }
    while (sourceIdx < sourceCells.length) {
      if (mirrorSourceCellUnlessShadowed(sourceCells[sourceIdx], targetHTable, rowState)) {
        cellMissing++;
      }
      sourceIdx++;
    }
    while (targetIdx < targetCells.length) {
      if (tombstoneTargetCell(targetCells[targetIdx++], targetHTable, rowState,
        sourceMaxTsByColumn)) {
        cellExtra++;
      } else {
        rowState.anyCellUnrepairable = true;
      }
    }

    if (cellMissing == 0 && cellExtra == 0 && cellDifferent == 0 && !rowState.anyCellUnrepairable) {
      return RowDiffOutcome.NONE;
    }
    rowState.flush(pendingPuts, pendingDeletes);
    return new RowDiffOutcome(new CellDriftCounts(cellMissing, cellExtra, cellDifferent),
      rowState.anyCellUnrepairable);
  }

  /**
   * Flushes the accumulated Put and Delete batches to the target HTable as a single mixed
   * RPC and clears both lists. Called every {@code repairBatchSize} rows and once more at
   * the end of a chunk.
   *
   * Issuing both Puts and Deletes via {@link Table#batch} (one network round-trip) instead
   * of separate {@code put()} + {@code delete()} calls (two round-trips) eliminates the
   * inter-RPC failure window where a JVM/regionserver crash between the two would leave
   * target with Puts applied but matching Deletes not yet tombstoned. Server-side, batch
   * mutations are still applied per-row sequentially, so a regionserver crash mid-batch
   * can still leave partial application — but the mid-flush gap on the client side is
   * gone.
   *
   * {@link InterruptedException} from {@code batch} is converted to {@link IOException}
   * so the existing per-chunk catch path treats interruption like any other transient
   * write failure (chunk → REPAIR_FAILED, retry on next invocation). The interrupt flag is
   * restored so an outer cancellation still observes the interrupted state.
   */
  private void flushRepairMutations(Table targetHTable, List<Put> puts, List<Delete> deletes)
    throws IOException {
    if (puts.isEmpty() && deletes.isEmpty()) {
      return;
    }
    List<Row> mutations = new ArrayList<>(puts.size() + deletes.size());
    mutations.addAll(puts);
    mutations.addAll(deletes);
    Object[] results = new Object[mutations.size()];
    try {
      targetHTable.batch(mutations, results);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while flushing repair mutations", e);
    }
    puts.clear();
    deletes.clear();
  }

  /**
   * Writes a chunk-level checkpoint row and bumps the matching outcome counter, as a single
   * "this attempt is recorded" unit. The outcome counter is bumped only after a successful
   * checkpoint write, so on-disk audit and in-memory counters stay in sync.
   *
   * If the checkpoint write throws {@link SQLException}, the failure is logged and the
   * {@link SyncCounters#CHECKPOINT_WRITE_FAILED} counter is bumped, but the exception is
   * NOT propagated. Reasons:
   * <ul>
   *   <li>Target's data was already mutated during the merge — failing the mapper task
   *       wouldn't roll that back, and would trigger a MapReduce retry that re-verifies
   *       against already-mutated target state (audit trail loss).</li>
   *   <li>Other chunks in this mapper still deserve a chance to be processed.</li>
   *   <li>The {@code CHECKPOINT_WRITE_FAILED} counter surfaces the audit-row gap to
   *       operators and drives a non-zero exit at job end.</li>
   * </ul>
   */
  private void writeChunkCheckpoint(PhoenixSyncTableCheckpointOutputRow row,
    SyncCounters outcomeCounter, Context context) {
    try {
      syncTableOutputRepository.checkpointSyncTableResult(row);
      context.getCounter(outcomeCounter).increment(1);
    } catch (SQLException e) {
      LOGGER.error(
        "Failed to write {} checkpoint for chunk source=[{}, {}] on table {}: target data "
          + "was mutated during the merge, but no checkpoint row will exist for this chunk. "
          + "CHECKPOINT_WRITE_FAILED counter is incremented; mapper continues.",
        row.getStatus(), Bytes.toStringBinary(row.getStartRowKey()),
        Bytes.toStringBinary(row.getEndRowKey()), tableName, e);
      context.getCounter(SyncCounters.CHECKPOINT_WRITE_FAILED).increment(1);
    }
  }

  /**
   * Performs row-level repair for a mismatched chunk by merge-scanning source and target
   * cluster data and applying targeted mutations to target. The two scan ranges may differ:
   * the verifier reads target over a wider range than source (covers extra-on-target rows
   * that fall between consecutive source chunks); repair must mirror the same boundaries so
   * those extras are visible here as {@code cmp > 0} rows and get deleted.
   *
   * Merge-scan contract: both scanners return rows in ascending key order (HBase guarantee).
   *   cmp == 0 (same row): compare cells; repair if different.
   *   cmp <  0 (source-only): Put all source cells.
   *   cmp >  0 (target-only): Delete target cells within [fromTime, toTime].
   *
   * Cells outside [fromTime, toTime] are never read (scan time range), so never mutated.
   *
   * Only called when isDryRun == false.
   *
   * @param sourceStart           Source chunk start key (also the checkpoint PK) — inclusive
   * @param sourceEnd             Source chunk end key (also the checkpoint PK) — inclusive
   * @param targetStart           Target scan start (matches verifier-side boundary)
   * @param targetEnd             Target scan end (matches verifier-side boundary)
   * @param targetStartInclusive  Inclusivity of target scan start — matches verify side
   * @param targetEndInclusive    Inclusivity of target scan end — matches verify side
   * @param verifyStartTime       When the verify pass began for this chunk; reused as the
   *                              REPAIRED row's START_TIME so the row spans the full
   *                              verify+repair lifecycle that overwrites the MISMATCHED row.
   */
  private void repairChunk(byte[] sourceStart, byte[] sourceEnd, byte[] targetStart,
    byte[] targetEnd, boolean targetStartInclusive, boolean targetEndInclusive,
    long verifySourceRows, long verifyTargetRows, Timestamp verifyStartTime, Context context)
    throws IOException, SQLException {
    DriftCounters driftCounters = new DriftCounters();

    LOGGER.info(
      "Starting repair for chunk source=[{}, {}] target=[{}{}, {}{} on table {}",
      Bytes.toStringBinary(sourceStart), Bytes.toStringBinary(sourceEnd),
      targetStartInclusive ? "[" : "(", Bytes.toStringBinary(targetStart),
      Bytes.toStringBinary(targetEnd), targetEndInclusive ? "]" : ")", tableName);

    PhoenixConnection sourcePhoenixConn = sourceConnection.unwrap(PhoenixConnection.class);
    PhoenixConnection targetPhoenixConn = targetConnection.unwrap(PhoenixConnection.class);

    Scan sourceScan = createRepairScan(sourceStart, sourceEnd, true, true, sourcePhoenixConn);
    Scan targetScan = createRepairScan(targetStart, targetEnd, targetStartInclusive,
      targetEndInclusive, targetPhoenixConn);

    List<Put> pendingPuts = new ArrayList<>();
    List<Delete> pendingDeletes = new ArrayList<>();

    try (Table sourceHTable = sourcePhoenixConn.getQueryServices().getTable(physicalTableName);
      Table targetHTable = targetPhoenixConn.getQueryServices().getTable(physicalTableName);
      ResultScanner sourceScanner = sourceHTable.getScanner(sourceScan);
      ResultScanner targetScanner = targetHTable.getScanner(targetScan)) {

      Result sourceResult = sourceScanner.next();
      Result targetResult = targetScanner.next();

      while (sourceResult != null || targetResult != null) {
        int cmp;
        if (sourceResult == null) {
          cmp = 1;
        } else if (targetResult == null) {
          cmp = -1;
        } else {
          cmp = Bytes.compareTo(sourceResult.getRow(), targetResult.getRow());
        }

        // Drift signals are bumped at the branch that semantically caused them: row-level
        // signals at the cmp != 0 branches, cell-level signals at the cmp == 0 branch.
        // ROWS_CANNOT_REPAIR is bumped whenever any source mirror is suppressed by an
        // existing target tombstone (or the cmp > 0 row was already entirely tombstones).
        if (cmp == 0) {
          // Same row key on both clusters — diff at cell level and repair only if cells differ.
          RowDiffOutcome outcome = diffCellsForRow(sourceResult, targetResult, targetHTable,
            pendingPuts, pendingDeletes);
          driftCounters.addCellDrift(outcome.cells);
          if (outcome.rowCannotRepair) {
            driftCounters.rowsCannotRepair++;
          }
          sourceResult = sourceScanner.next();
          targetResult = targetScanner.next();
        } else if (cmp < 0) {
          // Source-only row — mirror it onto target. Even though target's filtered scan
          // returned no row at this key, target may carry tombstones that would shadow
          // some or all of the Puts.
          WholeRowMirrorOutcome outcome =
            mirrorWholeRow(sourceResult, targetHTable, pendingPuts, pendingDeletes);
          if (outcome != WholeRowMirrorOutcome.FULLY_SHADOWED) {
            driftCounters.rowsMissingOnTarget++;
          }
          if (outcome != WholeRowMirrorOutcome.FULLY_MIRRORED) {
            driftCounters.rowsCannotRepair++;
          }
          sourceResult = sourceScanner.next();
        } else {
          // Target-only row — tombstone its live cells. If the row is already entirely
          // tombstones, repair has nothing to do (HBase cannot remove tombstones; only
          // major compaction reaps them) — record as ROWS_CANNOT_REPAIR so operators can
          // see the unrepairable drift volume.
          int liveCellsTombstoned =
            tombstoneWholeRow(targetResult, targetHTable, pendingPuts, pendingDeletes);
          if (liveCellsTombstoned == 0) {
            driftCounters.rowsCannotRepair++;
          } else {
            driftCounters.rowsExtraOnTarget++;
          }
          targetResult = targetScanner.next();
        }

        if (pendingPuts.size() + pendingDeletes.size() >= repairBatchSize) {
          flushRepairMutations(targetHTable, pendingPuts, pendingDeletes);
        }
        context.progress();
      }
      flushRepairMutations(targetHTable, pendingPuts, pendingDeletes);
    } catch (IOException e) {
      // Per-chunk fault isolation. Mark this chunk REPAIR_FAILED, increment the counter,
      // and return so the mapper continues with the next chunk. Phase 2's STATUS filter
      // (VERIFIED, REPAIRED) excludes REPAIR_FAILED, so a re-run will re-attempt this chunk
      // as an unprocessed gap.
      LOGGER.error("Repair failed for chunk source=[{}, {}] on table {}: {}",
        Bytes.toStringBinary(sourceStart), Bytes.toStringBinary(sourceEnd), tableName,
        e.getMessage(), e);

      Timestamp failedAt = new Timestamp(System.currentTimeMillis());
      // Capture partial progress in the COUNTERS column for triage.
      String failedCounters =
        driftCounters.formatChunkCounters(verifySourceRows, verifyTargetRows);
      writeChunkCheckpoint(new PhoenixSyncTableCheckpointOutputRow.Builder()
        .setTableName(tableName).setTargetCluster(targetZkQuorum)
        .setType(PhoenixSyncTableCheckpointOutputRow.Type.CHUNK).setFromTime(fromTime)
        .setToTime(toTime).setTenantId(tenantId).setIsDryRun(isDryRun)
        .setStartRowKey(sourceStart).setEndRowKey(sourceEnd)
        .setStatus(PhoenixSyncTableCheckpointOutputRow.Status.REPAIR_FAILED)
        .setExecutionStartTime(verifyStartTime).setExecutionEndTime(failedAt)
        .setCounters(failedCounters).build(), SyncCounters.CHUNKS_REPAIR_FAILED, context);
      return;
    }

    driftCounters.commitTo(context);
    // Chunk transitions to UNREPAIRABLE if any row landed in ROWS_CANNOT_REPAIR — operator
    // intervention (typically major compaction on target to reap shadowing tombstones) is
    // needed before re-running the tool. Otherwise REPAIRED.
    boolean unrepairable = driftCounters.rowsCannotRepair > 0;
    PhoenixSyncTableCheckpointOutputRow.Status status = unrepairable
      ? PhoenixSyncTableCheckpointOutputRow.Status.UNREPAIRABLE
      : PhoenixSyncTableCheckpointOutputRow.Status.REPAIRED;

    Timestamp repairEndTime = new Timestamp(System.currentTimeMillis());
    String repairCounters =
      driftCounters.formatChunkCounters(verifySourceRows, verifyTargetRows);

    // Write checkpoint first; outcome counter is bumped inside writeChunkCheckpoint only
    // on a successful write so the audit row and the counter stay consistent.
    writeChunkCheckpoint(new PhoenixSyncTableCheckpointOutputRow.Builder()
      .setTableName(tableName).setTargetCluster(targetZkQuorum)
      .setType(PhoenixSyncTableCheckpointOutputRow.Type.CHUNK).setFromTime(fromTime)
      .setToTime(toTime).setTenantId(tenantId).setIsDryRun(isDryRun).setStartRowKey(sourceStart)
      .setEndRowKey(sourceEnd).setStatus(status)
      .setExecutionStartTime(verifyStartTime).setExecutionEndTime(repairEndTime)
      .setCounters(repairCounters).build(),
      unrepairable ? SyncCounters.CHUNKS_UNREPAIRABLE : SyncCounters.CHUNKS_REPAIRED, context);

    LOGGER.info("Completed repair for chunk source=[{}, {}] with status={}: {}",
      Bytes.toStringBinary(sourceStart), Bytes.toStringBinary(sourceEnd), status,
      driftCounters.toLogString());
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
