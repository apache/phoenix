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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.PhoenixSyncTableChunkRepairer.ChunkRepairRequest;
import org.apache.phoenix.mapreduce.PhoenixSyncTableChunkRepairer.ChunkRepairResult;
import org.apache.phoenix.mapreduce.PhoenixSyncTableChunkRepairer.DriftCounters;
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
    ROWS_DIFFERENT_ON_TARGET,
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
  private Configuration conf;
  private Connection sourceConnection;
  private Connection targetConnection;
  private Connection globalConnection;
  private PTable pTable;
  private byte[] physicalTableName;
  private List<KeyRange> regionKeyRanges;
  private int currentRangeIndex;
  private PhoenixSyncTableOutputRepository syncTableOutputRepository;
  private PhoenixSyncTableChunkRepairer chunkRepairer;

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
      int repairBatchSize = PhoenixSyncTableTool.getPhoenixSyncTableRepairBatchSize(conf);
      extractRegionBoundariesFromSplit(context);
      sourceConnection = ConnectionUtil.getInputConnection(conf);
      pTable = sourceConnection.unwrap(PhoenixConnection.class).getTable(tableName);
      physicalTableName = pTable.getPhysicalName().getBytes();
      connectToTargetCluster();
      globalConnection = createGlobalConnection(conf);
      syncTableOutputRepository = new PhoenixSyncTableOutputRepository(globalConnection);
      chunkRepairer = new PhoenixSyncTableChunkRepairer(sourceConnection, targetConnection, pTable,
        physicalTableName, tableName, fromTime, toTime, isRawScan, isReadAllVersions,
        repairBatchSize);
    } catch (Exception e) {
      tryClosingResources();
      throw new RuntimeException(
        String.format("Failed to setup PhoenixSyncTableMapper for table: %s", tableName), e);
    }
  }

  /**
   * Extracts region key ranges from the PhoenixInputSplit. Handles both single-region splits and
   * coalesced splits with multiple regions. The mapper processes one region per {@code map()} call,
   * driven by {@link PhoenixNoOpPerRangeRecordReader} which emits one record per region so YARN
   * gets per-region progress visibility.
   */
  private void extractRegionBoundariesFromSplit(Context context) {
    PhoenixInputSplit split = (PhoenixInputSplit) context.getInputSplit();
    regionKeyRanges = split.getKeyRanges();
    currentRangeIndex = 0;

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
   * Processes one mapper region per call by comparing chunks between source and target clusters.
   * The {@link PhoenixNoOpPerRangeRecordReader} emits one record per region in the split, so for a
   * coalesced split with N regions this method runs N times - giving YARN per-region progress
   * visibility instead of jumping from 0% to 100% only when the whole split completes. Gets already
   * processed chunks from checkpoint table, resumes from check pointed progress and records final
   * status for chunks & mapper (VERIFIED/MISMATCHED).
   */
  @Override
  protected void map(NullWritable key, DBInputFormat.NullDBWritable value, Context context)
    throws IOException, InterruptedException {
    LOGGER.info("Mapper being called");
    context.getCounter(PhoenixJobCounters.INPUT_RECORDS).increment(1);
    if (currentRangeIndex >= regionKeyRanges.size()) {
      throw new IllegalStateException(
        String.format("map() called %d times but split for table %s only has %d regions",
          currentRangeIndex + 1, tableName, regionKeyRanges.size()));
    }
    try {
      KeyRange keyRange = regionKeyRanges.get(currentRangeIndex++);
      byte[] regionStart = keyRange.getLowerRange();
      byte[] regionEnd = keyRange.getUpperRange();
      LOGGER.info("Processing region {}/{} [{}, {}) from split for table {}", currentRangeIndex,
        regionKeyRanges.size(), Bytes.toStringBinary(regionStart), Bytes.toStringBinary(regionEnd),
        tableName);
      processRegion(regionStart, regionEnd, context);
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
    long rowsMissingBefore = context.getCounter(SyncCounters.ROWS_MISSING_ON_TARGET).getValue();
    long rowsExtraBefore = context.getCounter(SyncCounters.ROWS_EXTRA_ON_TARGET).getValue();
    long rowsDifferentBefore = context.getCounter(SyncCounters.ROWS_DIFFERENT_ON_TARGET).getValue();
    long rowsCannotRepairBefore = context.getCounter(SyncCounters.ROWS_CANNOT_REPAIR).getValue();
    long cellsMissingBefore = context.getCounter(SyncCounters.CELLS_MISSING_ON_TARGET).getValue();
    long cellsExtraBefore = context.getCounter(SyncCounters.CELLS_EXTRA_ON_TARGET).getValue();
    long cellsDifferentBefore =
      context.getCounter(SyncCounters.CELLS_DIFFERENT_ON_TARGET).getValue();

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
    long rowsMissingOnTarget =
      context.getCounter(SyncCounters.ROWS_MISSING_ON_TARGET).getValue() - rowsMissingBefore;
    long rowsExtraOnTarget =
      context.getCounter(SyncCounters.ROWS_EXTRA_ON_TARGET).getValue() - rowsExtraBefore;
    long rowsDifferentOnTarget =
      context.getCounter(SyncCounters.ROWS_DIFFERENT_ON_TARGET).getValue() - rowsDifferentBefore;
    long rowsCannotRepair =
      context.getCounter(SyncCounters.ROWS_CANNOT_REPAIR).getValue() - rowsCannotRepairBefore;
    long cellsMissingOnTarget =
      context.getCounter(SyncCounters.CELLS_MISSING_ON_TARGET).getValue() - cellsMissingBefore;
    long cellsExtraOnTarget =
      context.getCounter(SyncCounters.CELLS_EXTRA_ON_TARGET).getValue() - cellsExtraBefore;
    long cellsDifferentOnTarget =
      context.getCounter(SyncCounters.CELLS_DIFFERENT_ON_TARGET).getValue() - cellsDifferentBefore;

    Timestamp regionEndTime = new Timestamp(System.currentTimeMillis());
    String counters = PhoenixSyncTableCheckpointOutputRow.CounterFormatter.formatMapper(
      verifiedChunks, mismatchedChunks, sourceRowsProcessed, targetRowsProcessed,
      rowsMissingOnTarget, rowsExtraOnTarget, rowsDifferentOnTarget, rowsCannotRepair,
      cellsMissingOnTarget, cellsExtraOnTarget, cellsDifferentOnTarget);
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
   * @param unrepairableChunks Number of chunks where any row landed in ROWS_CANNOT_REPAIR; if > 0
   *                           (and no repair-failed chunks) the region rolls up to UNREPAIRABLE,
   *                           signalling operator intervention is needed
   * @param repairFailedChunks Number of chunks whose repair threw an IOException; if > 0 the region
   *                           rolls up to REPAIR_FAILED (highest precedence)
   * @param counters           Formatted counter string
   * @param context            Mapper context
   */
  private void recordRegionCompletion(byte[] regionStart, byte[] regionEnd,
    Timestamp regionStartTime, Timestamp regionEndTime, long verifiedChunks, long mismatchedChunks,
    long unrepairableChunks, long repairFailedChunks, String counters, Context context)
    throws SQLException {

    // Region rolls up its child chunks' outcomes into one of five statuses, in precedence
    // order (most-severe wins):
    // REPAIR_FAILED — at least one chunk threw during merge-scan or flush.
    // UNREPAIRABLE — repair completed but at least one chunk has rows that cannot be
    // repaired (target tombstones shadow source Puts, or target row is
    // entirely tombstones). Operator action (typically major compaction
    // on target) needed before a re-run can converge.
    // MISMATCHED — drift was detected but repair was not attempted (dry-run mode).
    // REPAIRED — drift was detected and every chunk's repair fully succeeded.
    // VERIFIED — every chunk matched; no drift in this region.
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
        byte[] targetStart = previousSourceChunk == null ? rangeStart : previousSourceChunk.endKey;
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
        if (matched) {
          String counters = PhoenixSyncTableCheckpointOutputRow.CounterFormatter
            .formatChunk(sourceChunk.rowCount, targetChunk.rowCount, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
          handleVerifiedChunk(sourceChunk, context, counters);
        } else {
          ChunkRepairRequest request =
            new ChunkRepairRequest(sourceChunk.startKey, sourceChunk.endKey, targetStart, targetEnd,
              isTargetStartKeyInclusive, targetEndInclusive, sourceChunk.rowCount,
              targetChunk.rowCount, sourceChunk.executionStartTime, isDryRun);
          ChunkRepairResult result = chunkRepairer.repair(request, context::progress);
          if (isDryRun) {
            // Dry-run: write CHUNK/MISMATCHED with real row-level drift in COUNTERS so the
            // checkpoint audit row matches the job counters. No CHUNK/REPAIRED row and no
            // target mutations.
            DriftCounters drift = result.drift;
            context.getCounter(SyncCounters.ROWS_MISSING_ON_TARGET)
              .increment(drift.rowsMissingOnTarget);
            context.getCounter(SyncCounters.ROWS_EXTRA_ON_TARGET)
              .increment(drift.rowsExtraOnTarget);
            context.getCounter(SyncCounters.ROWS_DIFFERENT_ON_TARGET)
              .increment(drift.rowsDifferentOnTarget);
            String counters = PhoenixSyncTableCheckpointOutputRow.CounterFormatter.formatChunk(
              sourceChunk.rowCount, targetChunk.rowCount, drift.rowsMissingOnTarget,
              drift.rowsExtraOnTarget, drift.rowsDifferentOnTarget, 0L, 0L, 0L, 0L);
            handleMismatchedChunk(sourceChunk, context, counters);
          } else {
            recordRepairOutcome(sourceChunk, request, result, context);
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
   * Creates an HBase scan for a chunk range. Honors the user's {@code --raw-scan} and
   * {@code --read-all-versions} flags. For target-side scans, sets caching/limit to 1 to enable
   * sequential partial-digest retrieval — each target chunk's digest feeds into the next until
   * scanning completes.
   */
  private Scan createChunkScan(byte[] startKey, byte[] endKey, boolean isStartKeyInclusive,
    boolean isEndKeyInclusive, boolean isTargetScan) throws IOException {
    Scan scan = new Scan();
    scan.withStartRow(startKey, isStartKeyInclusive);
    scan.withStopRow(endKey, isEndKeyInclusive);
    scan.setRaw(isRawScan);
    if (isReadAllVersions) {
      scan.readAllVersions();
    }
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
   * Translates a {@link ChunkRepairResult} into MapReduce side effects: bumps the cell/row drift
   * counters, builds the chunk-level checkpoint row (REPAIRED / UNREPAIRABLE / REPAIR_FAILED), and
   * writes it via {@link #writeChunkCheckpoint} so the outcome counter is bumped only on a
   * successful checkpoint write (audit row and counter stay consistent).
   * <p>
   * {@code CHUNKS_MISMATCHED} is bumped here too: it tracks every chunk where source and target
   * hashes differed — the drift-detected signal — regardless of whether repair ran. Without this,
   * repair-mode {@link #recordRegionCompletion} would see {@code mismatchedChunks
   * == 0} for fully-repaired regions and roll them up as VERIFIED instead of REPAIRED.
   */
  private void recordRepairOutcome(ChunkInfo sourceChunk, ChunkRepairRequest request,
    ChunkRepairResult result, Context context) {
    context.getCounter(SyncCounters.CHUNKS_MISMATCHED).increment(1);
    DriftCounters drift = result.drift;
    context.getCounter(SyncCounters.ROWS_MISSING_ON_TARGET).increment(drift.rowsMissingOnTarget);
    context.getCounter(SyncCounters.ROWS_EXTRA_ON_TARGET).increment(drift.rowsExtraOnTarget);
    context.getCounter(SyncCounters.ROWS_CANNOT_REPAIR).increment(drift.rowsCannotRepair);
    context.getCounter(SyncCounters.CELLS_MISSING_ON_TARGET).increment(drift.cellsMissingOnTarget);
    context.getCounter(SyncCounters.CELLS_EXTRA_ON_TARGET).increment(drift.cellsExtraOnTarget);
    context.getCounter(SyncCounters.CELLS_DIFFERENT_ON_TARGET)
      .increment(drift.cellsDifferentOnTarget);

    String counters = PhoenixSyncTableCheckpointOutputRow.CounterFormatter.formatChunk(
      request.verifySourceRows, request.verifyTargetRows, drift.rowsMissingOnTarget,
      drift.rowsExtraOnTarget, drift.rowsDifferentOnTarget, drift.rowsCannotRepair,
      drift.cellsMissingOnTarget, drift.cellsExtraOnTarget, drift.cellsDifferentOnTarget);

    PhoenixSyncTableCheckpointOutputRow.Status status;
    SyncCounters outcomeCounter;
    switch (result.status) {
      case REPAIRED:
        status = PhoenixSyncTableCheckpointOutputRow.Status.REPAIRED;
        outcomeCounter = SyncCounters.CHUNKS_REPAIRED;
        break;
      case UNREPAIRABLE:
        status = PhoenixSyncTableCheckpointOutputRow.Status.UNREPAIRABLE;
        outcomeCounter = SyncCounters.CHUNKS_UNREPAIRABLE;
        break;
      case REPAIR_FAILED:
        status = PhoenixSyncTableCheckpointOutputRow.Status.REPAIR_FAILED;
        outcomeCounter = SyncCounters.CHUNKS_REPAIR_FAILED;
        break;
      default:
        throw new IllegalStateException("Unexpected repair status: " + result.status);
    }

    writeChunkCheckpoint(new PhoenixSyncTableCheckpointOutputRow.Builder().setTableName(tableName)
      .setTargetCluster(targetZkQuorum).setType(PhoenixSyncTableCheckpointOutputRow.Type.CHUNK)
      .setFromTime(fromTime).setToTime(toTime).setTenantId(tenantId).setIsDryRun(isDryRun)
      .setStartRowKey(sourceChunk.startKey).setEndRowKey(sourceChunk.endKey).setStatus(status)
      .setExecutionStartTime(request.verifyStartTime).setExecutionEndTime(result.endTime)
      .setCounters(counters).build(), outcomeCounter, context);
  }

  /**
   * Writes a chunk-level checkpoint row and bumps the matching outcome counter. The outcome counter
   * is bumped only after a successful checkpoint write, so on-disk audit and in-memory counters
   * stay in sync.
   * <p>
   * If the checkpoint write throws {@link SQLException}, the failure is logged and the
   * {@link SyncCounters#CHECKPOINT_WRITE_FAILED} counter is bumped, but the exception is NOT
   * propagated. Reasons:
   * <ul>
   * <li>Target's data was already mutated during the merge — failing the mapper task wouldn't roll
   * that back, and would trigger a MapReduce retry that re-verifies against already-mutated target
   * state (audit trail loss).</li>
   * <li>Other chunks in this mapper still deserve a chance to be processed.</li>
   * <li>The {@code CHECKPOINT_WRITE_FAILED} counter surfaces the audit-row gap to operators and
   * drives a non-zero exit at job end.</li>
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
