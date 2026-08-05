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
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Progressable;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.ScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performs row-level repair for a mismatched chunk by merge-scanning source and target cluster data
 * and applying targeted mutations to target.
 * <p>
 * The two scan ranges may differ: the verifier reads target over a wider range than source (covers
 * extra-on-target rows that fall between consecutive source chunks); repair must mirror the same
 * boundaries so those extras are visible here as {@code cmp > 0} rows and get deleted.
 * <p>
 * Merge-scan contract: both scanners return rows in ascending key order (HBase guarantee).
 * <ul>
 * <li>{@code cmp == 0} (same row): compare cells; repair only differing cells.</li>
 * <li>{@code cmp <  0} (source-only): mirror all source cells onto target.</li>
 * <li>{@code cmp >  0} (target-only): tombstone target cells within
 * {@code [fromTime, toTime]}.</li>
 * </ul>
 * Cells outside {@code [fromTime, toTime]} are never read (scan time range), so never mutated.
 * <p>
 * Tombstone semantics: HBase has four tombstone subtypes ({@code Delete}, {@code DeleteColumn},
 * {@code DeleteFamily}, {@code DeleteFamilyVersion}). Source Puts we mirror onto target may be
 * silently shadowed by an existing target tombstone; in that case the mirror is suppressed and the
 * row carries unrepairable drift (operator must major-compact target to reap shadowing tombstones
 * before a re-run can converge). See {@link TargetRowRecord}.
 */
public final class PhoenixSyncTableChunkRepairer {

  private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixSyncTableChunkRepairer.class);

  private final Connection sourceConnection;
  private final Connection targetConnection;
  private final PTable pTable;
  private final byte[] physicalTableName;
  private final long fromTime;
  private final long toTime;
  private final boolean isRawScan;
  private final boolean isReadAllVersions;
  private final int repairBatchSize;
  private final String tableName;

  public PhoenixSyncTableChunkRepairer(Connection sourceConnection, Connection targetConnection,
    PTable pTable, byte[] physicalTableName, String tableName, long fromTime, long toTime,
    boolean isRawScan, boolean isReadAllVersions, int repairBatchSize) {
    this.sourceConnection = sourceConnection;
    this.targetConnection = targetConnection;
    this.pTable = pTable;
    this.physicalTableName = physicalTableName;
    this.tableName = tableName;
    this.fromTime = fromTime;
    this.toTime = toTime;
    this.isRawScan = isRawScan;
    this.isReadAllVersions = isReadAllVersions;
    this.repairBatchSize = repairBatchSize;
  }

  /**
   * Repairs one mismatched chunk. Returns a {@link ChunkRepairResult} carrying the terminal status
   * and accumulated {@link DriftCounters}; never throws on per-chunk scan/flush failure (returns
   * {@link ChunkRepairResult.Status#REPAIR_FAILED}). The only declared {@link SQLException}
   * surfaces from {@link Connection#unwrap}, which indicates a misconfigured connection rather than
   * a per-chunk fault.
   */
  public ChunkRepairResult repair(ChunkRepairRequest req, Progressable progress)
    throws SQLException {
    DriftCounters drift = new DriftCounters();

    LOGGER.info("Starting repair for chunk source=[{}, {}] target={}{}, {}{} on table {}",
      Bytes.toStringBinary(req.sourceStart), Bytes.toStringBinary(req.sourceEnd),
      req.targetStartInclusive ? "[" : "(", Bytes.toStringBinary(req.targetStart),
      Bytes.toStringBinary(req.targetEnd), req.targetEndInclusive ? "]" : ")", tableName);

    PhoenixConnection sourcePhoenixConn = sourceConnection.unwrap(PhoenixConnection.class);
    PhoenixConnection targetPhoenixConn = targetConnection.unwrap(PhoenixConnection.class);

    Scan sourceScan;
    Scan targetScan;
    try {
      sourceScan = createRepairScan(req.sourceStart, req.sourceEnd, true, true, sourcePhoenixConn);
      targetScan = createRepairScan(req.targetStart, req.targetEnd, req.targetStartInclusive,
        req.targetEndInclusive, targetPhoenixConn);
    } catch (IOException e) {
      LOGGER.error("Repair failed to build scans for chunk source=[{}, {}] on table {}: {}",
        Bytes.toStringBinary(req.sourceStart), Bytes.toStringBinary(req.sourceEnd), tableName,
        e.getMessage(), e);
      return ChunkRepairResult.failed(drift, e);
    }

    try (Table sourceHTable = sourcePhoenixConn.getQueryServices().getTable(physicalTableName);
      Table targetHTable = targetPhoenixConn.getQueryServices().getTable(physicalTableName);
      ResultScanner sourceScanner = sourceHTable.getScanner(sourceScan);
      ResultScanner targetScanner = targetHTable.getScanner(targetScan)) {
      if (req.dryRun) {
        walkAndCountDrift(sourceScanner, targetScanner, drift, progress);
      } else {
        repairDiffRows(sourceScanner, targetScanner, targetHTable, drift, progress);
      }
    } catch (IOException e) {
      // Per-chunk fault isolation. The mapper marks this chunk REPAIR_FAILED and continues
      // with the next chunk
      LOGGER.error("Repair failed for chunk source=[{}, {}] on table {}: {}",
        Bytes.toStringBinary(req.sourceStart), Bytes.toStringBinary(req.sourceEnd), tableName,
        e.getMessage(), e);
      return ChunkRepairResult.failed(drift, e);
    }

    ChunkRepairResult result = ChunkRepairResult.completed(drift);
    LOGGER.info("Completed repair for chunk source=[{}, {}] with status={}: {}",
      Bytes.toStringBinary(req.sourceStart), Bytes.toStringBinary(req.sourceEnd), result.status,
      drift.toLogString());
    return result;
  }

  /**
   * Dry-run merge-walk: bumps the three row-level drift counters and logs each diverged row; never
   * touches target. {@code rowsDifferentOnTarget} flags rows present on both sides whose contents
   * differ — verifier-only signal, not produced in repair mode (which reports cell granularity
   * instead).
   */
  private void walkAndCountDrift(ResultScanner sourceScanner, ResultScanner targetScanner,
    DriftCounters drift, Progressable progress) throws IOException {
    Result sourceResult = sourceScanner.next();
    Result targetResult = targetScanner.next();

    while (sourceResult != null || targetResult != null) {
      int cmp = compareRowKeys(sourceResult, targetResult);
      if (cmp == 0) {
        if (!rowsEqual(sourceResult, targetResult)) {
          drift.rowsDifferentOnTarget++;
          LOGGER.warn("Row different on target for table {} row={}", tableName,
            Bytes.toStringBinary(sourceResult.getRow()));
        }
        sourceResult = sourceScanner.next();
        targetResult = targetScanner.next();
      } else if (cmp < 0) {
        drift.rowsMissingOnTarget++;
        LOGGER.warn("Row missing on target for table {} row={}", tableName,
          Bytes.toStringBinary(sourceResult.getRow()));
        sourceResult = sourceScanner.next();
      } else {
        drift.rowsExtraOnTarget++;
        LOGGER.warn("Row extra on target for table {} row={}", tableName,
          Bytes.toStringBinary(targetResult.getRow()));
        targetResult = targetScanner.next();
      }
      if (progress != null) {
        progress.progress();
      }
    }
  }

  /**
   * Repair-mode merge-walk: resolves drift by emitting mutations into pending batches, flushing
   * each time the batch reaches {@link #repairBatchSize}, and finally draining the tail. Per
   * branch:
   * <ul>
   * <li>{@code cmp == 0} — diff cells; record cell-level drift and any row-unrepairable flag.</li>
   * <li>{@code cmp <  0} — mirror the source row onto target; bump {@code rowsMissing} unless the
   * whole row was shadowed, and {@code rowsCannotRepair} unless every cell was mirrored.</li>
   * <li>{@code cmp >  0} — tombstone the extra row on target; bump {@code rowsExtra} when at least
   * one live cell was tombstoned, else {@code rowsCannotRepair} (row was already all
   * tombstones).</li>
   * </ul>
   */
  private void repairDiffRows(ResultScanner sourceScanner, ResultScanner targetScanner,
    Table targetHTable, DriftCounters drift, Progressable progress) throws IOException {
    List<Put> pendingPuts = new ArrayList<>();
    List<Delete> pendingDeletes = new ArrayList<>();
    Result sourceResult = sourceScanner.next();
    Result targetResult = targetScanner.next();

    while (sourceResult != null || targetResult != null) {
      int cmp = compareRowKeys(sourceResult, targetResult);
      if (cmp == 0) {
        RowDriftInfo rowDriftInfo = generateMutationForDiffCells(sourceResult, targetResult,
          targetHTable, pendingPuts, pendingDeletes);
        drift.addCellDrift(rowDriftInfo.cells);
        if (rowDriftInfo.rowCannotRepair) {
          drift.rowsCannotRepair++;
        }
        if (rowDriftInfo != RowDriftInfo.NONE) {
          LOGGER.warn(
            "Row mismatch on table {} row={}: cell drift missing={}, extra={}, different={}, "
              + "rowCannotRepair={}",
            tableName, Bytes.toStringBinary(sourceResult.getRow()), rowDriftInfo.cells.missing,
            rowDriftInfo.cells.extra, rowDriftInfo.cells.different, rowDriftInfo.rowCannotRepair);
        }
        sourceResult = sourceScanner.next();
        targetResult = targetScanner.next();
      } else if (cmp < 0) {
        byte[] missingRowKey = sourceResult.getRow();
        RowMirrorStatus outcome =
          mirrorWholeRow(sourceResult, targetHTable, pendingPuts, pendingDeletes);
        if (outcome != RowMirrorStatus.FULLY_SHADOWED) {
          drift.rowsMissingOnTarget++;
        }
        if (outcome != RowMirrorStatus.FULLY_MIRRORED) {
          drift.rowsCannotRepair++;
        }
        LOGGER.warn("Row missing on target for table {} row={}: mirrorOutcome={}", tableName,
          Bytes.toStringBinary(missingRowKey), outcome);
        sourceResult = sourceScanner.next();
      } else {
        byte[] extraRowKey = targetResult.getRow();
        int liveCellsTombstoned =
          tombstoneWholeRow(targetResult, targetHTable, pendingPuts, pendingDeletes);
        if (liveCellsTombstoned == 0) {
          drift.rowsCannotRepair++;
        } else {
          drift.rowsExtraOnTarget++;
        }
        LOGGER.warn("Row extra on target for table {} row={}: liveCellsTombstoned={}", tableName,
          Bytes.toStringBinary(extraRowKey), liveCellsTombstoned);
        targetResult = targetScanner.next();
      }

      if (pendingPuts.size() + pendingDeletes.size() >= repairBatchSize) {
        flushRepairMutations(targetHTable, pendingPuts, pendingDeletes);
      }
      if (progress != null) {
        progress.progress();
      }
    }
    flushRepairMutations(targetHTable, pendingPuts, pendingDeletes);
  }

  /**
   * Compares the row keys of two scanner results; treats a null result as past-end so a
   * {@code null/non-null} pair sorts the non-null side first.
   */
  private static int compareRowKeys(Result sourceResult, Result targetResult) {
    if (sourceResult == null) {
      return 1;
    }
    if (targetResult == null) {
      return -1;
    }
    return Bytes.compareTo(sourceResult.getRow(), targetResult.getRow());
  }

  /**
   * Whole-row content equality check used by dry-run row-level diffing. Delegates to
   * {@link Result#compareResults(Result, Result, boolean)} which throws on any cell-level mismatch
   * (family, qualifier, timestamp, type, value); we map the throw to {@code false} so the cmp==0
   * path can flag the row without producing repair mutations.
   */
  private boolean rowsEqual(Result src, Result tgt) {
    try {
      Result.compareResults(src, tgt, false);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Mirrors every source cell of a row that is missing on target. Each cell is shadow-checked
   * against target's per-row record (see {@link TargetRowRecord}).
   */
  private RowMirrorStatus mirrorWholeRow(Result sourceResult, Table targetHTable,
    List<Put> pendingPuts, List<Delete> pendingDeletes) throws IOException {
    RowRepairBuffer rowRepairBuffer = new RowRepairBuffer(sourceResult.getRow());
    int mirrored = 0;
    for (Cell cell : sourceResult.rawCells()) {
      if (mirrorSourceCellUnlessShadowed(cell, targetHTable, rowRepairBuffer)) {
        mirrored++;
      }
    }
    rowRepairBuffer.flush(pendingPuts, pendingDeletes);
    if (mirrored == 0) {
      return RowMirrorStatus.FULLY_SHADOWED;
    }
    return rowRepairBuffer.anyCellUnrepairable
      ? RowMirrorStatus.PARTIALLY_MIRRORED
      : RowMirrorStatus.FULLY_MIRRORED;
  }

  /**
   * Tombstones every live cell of a row that is extra on target. Skips cells that are themselves
   * already tombstones (see {@link #tombstoneTargetCell}).
   * @return the number of live cells that contributed a tombstone marker. {@code 0} means the row
   *         was already entirely tombstones; the caller records this as {@code ROWS_CANNOT_REPAIR}.
   */
  private int tombstoneWholeRow(Result targetResult, Table targetHTable, List<Put> pendingPuts,
    List<Delete> pendingDeletes) throws IOException {
    RowRepairBuffer rowRepairBuffer = new RowRepairBuffer(targetResult.getRow());
    // Empty source map drives every target cell into tombstoneTargetCell's "no source column"
    // branch (DeleteColumn at ts <= T).
    Map<ColumnKey, Long> sourceMaxTsByColumn = Collections.emptyMap();
    int liveCellsTombstoned = 0;
    for (Cell cell : targetResult.rawCells()) {
      if (tombstoneTargetCell(cell, targetHTable, rowRepairBuffer, sourceMaxTsByColumn)) {
        liveCellsTombstoned++;
      }
    }
    rowRepairBuffer.flush(pendingPuts, pendingDeletes);
    return liveCellsTombstoned;
  }

  /**
   * Diffs cells of two rows present on both clusters in lock-step using {@link CellComparator}
   * order and emits {@link Put}/{@link Delete} mutations.
   * <p>
   * Branches:
   * <ul>
   * <li>same coords + matching value → no drift</li>
   * <li>same coords + different value → different++; mirror source cell (shadow-checked)</li>
   * <li>source-only cell → missing++; mirror source cell (shadow-checked)</li>
   * <li>target-only live cell → extra++; tombstone target cell</li>
   * <li>target-only tombstone cell → skip; row carries unrepairable drift</li>
   * </ul>
   * Mirrors suppressed by shadowing do NOT bump the cell counter (nothing was written); the
   * row-level signal flows through {@link RowDriftInfo#rowCannotRepair}.
   */
  private RowDriftInfo generateMutationForDiffCells(Result sourceResult, Result targetResult,
    Table targetHTable, List<Put> pendingPuts, List<Delete> pendingDeletes) throws IOException {
    Cell[] sourceCells = sourceResult.rawCells();
    Cell[] targetCells = targetResult.rawCells();
    CellComparator comparator = CellComparator.getInstance();
    RowRepairBuffer rowRepairBuffer = new RowRepairBuffer(sourceResult.getRow());

    // Per-column max source PUT timestamp; consumed by tombstoneTargetCell to pick the
    // delete shape for a target-extra cell — see its javadoc for the three cases.
    // Math::max collapses source's multi-version cells into a single Long per column so
    // the comparison against target's ts is a scalar check.
    //
    // Example: source has Put(NAME)@300 and Put(NAME)@200 → sourceMaxTsByColumn[NAME]=300.
    Map<ColumnKey, Long> sourceMaxTsByColumn = new HashMap<>();
    for (Cell sourceCell : sourceCells) {
      if (!CellUtil.isDelete(sourceCell)) {
        sourceMaxTsByColumn.merge(ColumnKey.of(sourceCell), sourceCell.getTimestamp(), Math::max);
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
        // Same coordinates, compare values.
        if (!CellUtil.matchingValue(sourceCells[sourceIdx], targetCells[targetIdx])) {
          if (
            mirrorSourceCellUnlessShadowed(sourceCells[sourceIdx], targetHTable, rowRepairBuffer)
          ) {
            cellDifferent++;
          }
        }
        sourceIdx++;
        targetIdx++;
      } else if (cmp < 0) {
        // Missing on target
        if (mirrorSourceCellUnlessShadowed(sourceCells[sourceIdx], targetHTable, rowRepairBuffer)) {
          cellMissing++;
        }
        sourceIdx++;
      } else {
        // extra on target
        if (
          tombstoneTargetCell(targetCells[targetIdx++], targetHTable, rowRepairBuffer,
            sourceMaxTsByColumn)
        ) {
          cellExtra++;
        } else {
          rowRepairBuffer.anyCellUnrepairable = true;
        }
      }
    }
    while (sourceIdx < sourceCells.length) {
      if (mirrorSourceCellUnlessShadowed(sourceCells[sourceIdx], targetHTable, rowRepairBuffer)) {
        cellMissing++;
      }
      sourceIdx++;
    }
    while (targetIdx < targetCells.length) {
      if (
        tombstoneTargetCell(targetCells[targetIdx++], targetHTable, rowRepairBuffer,
          sourceMaxTsByColumn)
      ) {
        cellExtra++;
      } else {
        rowRepairBuffer.anyCellUnrepairable = true;
      }
    }

    if (
      cellMissing == 0 && cellExtra == 0 && cellDifferent == 0
        && !rowRepairBuffer.anyCellUnrepairable
    ) {
      return RowDriftInfo.NONE;
    }
    rowRepairBuffer.flush(pendingPuts, pendingDeletes);
    return new RowDriftInfo(new CellDriftCounts(cellMissing, cellExtra, cellDifferent),
      rowRepairBuffer.anyCellUnrepairable);
  }

  /**
   * Routes a source cell to the right mutation kind. Tombstone cells go through
   * {@link Delete#add(Cell)} (preserves the exact tombstone subtype); under {@code --raw-scan} this
   * matters because {@link Put#add(Cell)} rejects non-Put cells.
   */
  private void mirrorSourceCell(Cell cell, RowRepairBuffer rowRepairBuffer) throws IOException {
    if (CellUtil.isDelete(cell)) {
      rowRepairBuffer.delete().add(cell);
    } else {
      rowRepairBuffer.put().add(cell);
    }
  }

  /**
   * Mirrors a source cell onto target unless an existing target tombstone would shadow it. Shadow
   * detection runs only if source has Put cells; tombstoned source cells always mirror.
   * @return {@code true} if mirrored, {@code false} if suppressed (caller marks the row
   *         unrepairable).
   */
  private boolean mirrorSourceCellUnlessShadowed(Cell cell, Table targetHTable,
    RowRepairBuffer rowRepairBuffer) throws IOException {
    // Source Puts can be shadowed by an existing target tombstone, the Put lands on
    // disk but stays invisible to reads, so writing it is wasted work and the row stays
    // diverged. e.g. src Put(name, T=200) vs tgt DeleteColumn(name, T=300) covering
    // ts<=300. Skip the write and flag the row unrepairable; operator must major-compact
    // target to reap the shadow. Source tombstones can't be shadowed, hence skip the check.
    if (
      !CellUtil.isDelete(cell) && rowRepairBuffer.targetRowRecord(targetHTable).wouldShadow(cell)
    ) {
      rowRepairBuffer.anyCellUnrepairable = true;
      return false;
    }
    mirrorSourceCell(cell, rowRepairBuffer);
    return true;
  }

  /**
   * Tombstones a target-only cell to make target's read view at this column match source's. Skips
   * cells that are themselves already tombstones.
   * <p>
   * Called only when source has no cell at this target cell's exact {@code (cf, q, ts)}. If source
   * does have a cell at the same {@code (cf, q, ts)}, the caller takes the mirroring path instead
   * <p>
   * Tombstone subtype depends on what source has at this {@code (cf, q)}. Examples assume
   * {@code MAX_VERSIONS=3} and show only the relevant column.
   * <p>
   * <b>Case 1 — Source has no cell at this column:</b>
   *
   * <pre>
   *   source row: (no NAME)
   *   target row: Put(NAME, "carol")@900 visible
   *               Put(NAME, "bob")  @600 hidden
   *   action    : DeleteColumn(NAME)@900   (covers ts <= 900, wipes "bob" too)
   *   result    : target reads no NAME — matches source.
   * </pre>
   * <p>
   * <b>Case 2 — {@code sourceMaxTs >= targetTs}:</b>
   *
   * <pre>
   *   source row: Put(NAME, "alice")@500       (sourceMaxTs = 500)
   *   target row: Put(NAME, "old",  )@200      (input cell; source has nothing at @200)
   *   action    : point-Delete(NAME)@200
   *   result    : "old"@200 is shadowed;
   *              Put(NAME, "alice")@500 would already have been mirrored
   * </pre>
   * <p>
   * <b>Case 3 — {@code sourceMaxTs < targetTs}:</b>
   *
   * <pre>
   *   source row: Put(NAME, "alice")@300       (sourceMaxTs = 300)
   *   target row: Put(NAME, "carol")@900 visible
   *               Put(NAME, "bob")  @600 hidden
   *               Put(NAME, "alice")@300 hidden
   *   action    : point-Delete(NAME)@900 + point-Delete(NAME)@600
   *               (without the second, "bob"@600 surfaces above source's mirror)
   *   result    : target's "alice"@300 is the highest live version — matches source.
   * </pre>
   *
   * @return true if the cell was a live cell that contributed a tombstone marker, false if the cell
   *         was already a tombstone and was skipped.
   */
  private boolean tombstoneTargetCell(Cell cell, Table targetHTable,
    RowRepairBuffer rowRepairBuffer, Map<ColumnKey, Long> sourceMaxTsByColumn) throws IOException {
    if (CellUtil.isDelete(cell)) {
      return false;
    }
    byte[] family = CellUtil.cloneFamily(cell);
    byte[] qualifier = CellUtil.cloneQualifier(cell);
    long ts = cell.getTimestamp();
    Long sourceMaxTs = sourceMaxTsByColumn.get(new ColumnKey(family, qualifier));
    if (sourceMaxTs == null) {
      rowRepairBuffer.delete().addColumns(family, qualifier, ts);
    } else if (sourceMaxTs >= ts) {
      rowRepairBuffer.delete().addColumn(family, qualifier, ts);
    } else {
      rowRepairBuffer.delete().addColumn(family, qualifier, ts);
      Set<Long> hiddenTs = rowRepairBuffer.targetRowRecord(targetHTable)
        .targetPutTimestampsBetween(family, qualifier, sourceMaxTs, ts);
      for (Long hidden : hiddenTs) {
        rowRepairBuffer.delete().addColumn(family, qualifier, hidden);
      }
    }
    return true;
  }

  /**
   * Builds a row-level HBase scan for repair. Honors the user's {@code --raw-scan} and
   * {@code --read-all-versions} flags; adds bulk caching plus Phoenix TTL / {@code IS_STRICT_TTL}
   * attributes so the cells visited here are the same cells the verifier hashed.
   */
  private Scan createRepairScan(byte[] startKey, byte[] endKey, boolean isStartKeyInclusive,
    boolean isEndKeyInclusive, PhoenixConnection phoenixConn) throws IOException, SQLException {
    Scan scan = new Scan();
    scan.withStartRow(startKey, isStartKeyInclusive);
    scan.withStopRow(endKey, isEndKeyInclusive);
    scan.setRaw(isRawScan);
    if (isReadAllVersions) {
      scan.readAllVersions();
    }
    scan.setCacheBlocks(false);
    scan.setTimeRange(fromTime, toTime);
    scan.setCaching(1000);
    ScanUtil.setScanAttributesForPhoenixTTL(scan, pTable, phoenixConn);
    scan.setAttribute(BaseScannerRegionObserverConstants.IS_STRICT_TTL, TRUE_BYTES);
    return scan;
  }

  /**
   * Flushes the accumulated Put and Delete batches to target as a single mixed RPC via
   * {@link Table#batch}. The mixed batch (rather than separate {@code put()} + {@code delete()}
   * calls) closes the inter-RPC window where a JVM/regionserver crash between the two could leave
   * target with Puts applied but matching Deletes missing.
   * <p>
   * {@link Table#batch} does NOT throw for partial failures — per-mutation failures (e.g.
   * {@code NotServingRegionException} from a region split mid-batch, {@code WrongRegionException}
   * from a merge) land in the {@code results} array as {@link Throwable} entries. We surface the
   * first such failure as {@link IOException} so the caller treats this chunk as
   * {@code REPAIR_FAILED} rather than silently marking it {@code REPAIRED}; on re-run the resume
   * filter excludes {@code REPAIR_FAILED} and the chunk re-enters as an unprocessed gap.
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
    int failureCount = 0;
    int firstFailureIdx = -1;
    for (int i = 0; i < results.length; i++) {
      if (results[i] instanceof Throwable) {
        failureCount++;
        if (firstFailureIdx < 0) {
          firstFailureIdx = i;
        }
      }
    }
    if (failureCount > 0) {
      Throwable firstFailure = (Throwable) results[firstFailureIdx];
      Row failedRow = mutations.get(firstFailureIdx);
      throw new IOException(
        String.format("Repair batch had %d/%d mutation failure(s); first failure on row %s: %s",
          failureCount, results.length, Bytes.toStringBinary(failedRow.getRow()),
          firstFailure.getMessage()),
        firstFailure);
    }
    puts.clear();
    deletes.clear();
  }

  /**
   * Inputs to a chunk repair attempt. Source range is the chunk boundary; target range may be wider
   * so the repair scan sees the same cells (including extra-on-target rows between consecutive
   * source chunks) that the verifier hashed.
   * <p>
   * {@link #verifySourceRows} / {@link #verifyTargetRows} are the row counts the verifier recorded;
   * threaded into the COUNTERS column on the resulting checkpoint row. {@link #verifyStartTime} is
   * the timestamp captured when verification began for this chunk; reused as EXECUTION_START_TIME
   * on the REPAIRED/UNREPAIRABLE/REPAIR_FAILED checkpoint row so the row spans the full
   * verify+repair lifecycle that overwrites the MISMATCHED row.
   */
  public static final class ChunkRepairRequest {
    public final byte[] sourceStart;
    public final byte[] sourceEnd;
    public final byte[] targetStart;
    public final byte[] targetEnd;
    public final boolean targetStartInclusive;
    public final boolean targetEndInclusive;
    public final long verifySourceRows;
    public final long verifyTargetRows;
    public final Timestamp verifyStartTime;
    public final boolean dryRun;

    public ChunkRepairRequest(byte[] sourceStart, byte[] sourceEnd, byte[] targetStart,
      byte[] targetEnd, boolean targetStartInclusive, boolean targetEndInclusive,
      long verifySourceRows, long verifyTargetRows, Timestamp verifyStartTime, boolean dryRun) {
      this.sourceStart = sourceStart;
      this.sourceEnd = sourceEnd;
      this.targetStart = targetStart;
      this.targetEnd = targetEnd;
      this.targetStartInclusive = targetStartInclusive;
      this.targetEndInclusive = targetEndInclusive;
      this.verifySourceRows = verifySourceRows;
      this.verifyTargetRows = verifyTargetRows;
      this.verifyStartTime = verifyStartTime;
      this.dryRun = dryRun;
    }
  }

  /**
   * Outcome of a chunk repair attempt. Carries the terminal status, accumulated drift counters,
   * end-of-attempt timestamp, and the failure exception when status is
   * {@link Status#REPAIR_FAILED}. Status precedence (most-severe wins):
   * {@link Status#REPAIR_FAILED} &gt; {@link Status#UNREPAIRABLE} &gt; {@link Status#REPAIRED}.
   */
  public static final class ChunkRepairResult {

    public enum Status {
      REPAIRED,
      UNREPAIRABLE,
      REPAIR_FAILED
    }

    public final Status status;
    public final DriftCounters drift;
    public final Timestamp endTime;
    public final IOException failure;

    private ChunkRepairResult(Status status, DriftCounters drift, Timestamp endTime,
      IOException failure) {
      this.status = status;
      this.drift = drift;
      this.endTime = endTime;
      this.failure = failure;
    }

    static ChunkRepairResult completed(DriftCounters drift) {
      Status status = drift.rowsCannotRepair > 0 ? Status.UNREPAIRABLE : Status.REPAIRED;
      return new ChunkRepairResult(status, drift, new Timestamp(System.currentTimeMillis()), null);
    }

    static ChunkRepairResult failed(DriftCounters drift, IOException failure) {
      return new ChunkRepairResult(Status.REPAIR_FAILED, drift,
        new Timestamp(System.currentTimeMillis()), failure);
    }
  }

  /**
   * Per-chunk aggregate of six drift counters — three row-level ({@code rowsMissingOnTarget},
   * {@code rowsExtraOnTarget}, {@code rowsCannotRepair}) and three cell-level
   * ({@code cellsMissing/Extra/DifferentOnTarget}). Pure accumulator; the caller maps fields onto
   * MapReduce job counters and the checkpoint COUNTERS string.
   */
  public static final class DriftCounters {
    public long rowsMissingOnTarget;
    public long rowsExtraOnTarget;
    public long rowsDifferentOnTarget;
    public long rowsCannotRepair;
    public long cellsMissingOnTarget;
    public long cellsExtraOnTarget;
    public long cellsDifferentOnTarget;

    void addCellDrift(CellDriftCounts cellDrift) {
      cellsMissingOnTarget += cellDrift.missing;
      cellsExtraOnTarget += cellDrift.extra;
      cellsDifferentOnTarget += cellDrift.different;
    }

    /** Compact end-of-chunk log line summarizing all drift signals. */
    public String toLogString() {
      return String.format(
        "rowsMissingOnTarget=%d, rowsExtraOnTarget=%d, rowsDifferentOnTarget=%d, "
          + "rowsCannotRepair=%d, cellsMissingOnTarget=%d, cellsExtraOnTarget=%d, "
          + "cellsDifferentOnTarget=%d",
        rowsMissingOnTarget, rowsExtraOnTarget, rowsDifferentOnTarget, rowsCannotRepair,
        cellsMissingOnTarget, cellsExtraOnTarget, cellsDifferentOnTarget);
    }
  }

  /**
   * Per-row snapshot of target's tombstones and Puts. Two queries: {@link #wouldShadow} (shadow
   * detection) and {@link #targetPutTimestampsBetween} (hidden-version discovery). For examples of
   * how callers use these, see the doc on {@link RowRepairBuffer#targetRowRecord}; for scan shape
   * and time-range rationale, see {@link #load}.
   * <p>
   * HBase has four tombstone subtypes; each is recorded into its own map because shadow scope
   * differs:
   *
   * <pre>
   *   Delete               shadows Put at (cf, q, ts == T) exactly
   *   DeleteColumn         shadows Puts at (cf, q, ts &lt;= T)
   *   DeleteFamily         shadows Puts at (cf, *, ts &lt;= T)
   *   DeleteFamilyVersion  shadows Puts at (cf, *, ts == T)
   * </pre>
   */
  static final class TargetRowRecord {
    private final Map<ColumnKey, Set<Long>> deletePointTs = new HashMap<>();
    private final Map<ColumnKey, Long> deleteColumnUpperBound = new HashMap<>();
    private final Map<ByteBuffer, Long> deleteFamilyUpperBound = new HashMap<>();
    private final Map<ByteBuffer, Set<Long>> deleteFamilyVersionTs = new HashMap<>();
    /** Per-column ts-ordered set of target's Put timestamps. */
    private final Map<ColumnKey, NavigableMap<Long, Boolean>> targetPutTs = new HashMap<>();

    /**
     * Builds a {@link TargetRowRecord} from a single-row HBase scan.
     * <p>
     * <b>raw=true + all-versions</b> are forced regardless of user flags so tombstones and
     * max-versions-filtered older Puts (the two things this record exists to capture) are surfaced.
     * <p>
     * <b>Time range {@code [fromTime, MAX_VALUE]}</b>:
     * <ul>
     * <li>Lower bound = {@code fromTime}: cells below the verify window can't affect repair inside
     * the window.</li>
     * <li>Upper bound = {@code MAX_VALUE} (NOT {@code toTime}): a tombstone at {@code ts >= toTime}
     * can still shadow a Put we mirror at {@code ts} in window during application reads, so we must
     * see it. e.g. window {@code [0, 600)}, tgt has DeleteColumn@900, src wants Put@500 — without
     * the wide upper bound we'd miss the 900 tombstone and write a doomed mirror.</li>
     * </ul>
     */
    static TargetRowRecord load(byte[] rowKey, Table targetHTable, long fromTime)
      throws IOException {
      Scan scan = new Scan();
      scan.withStartRow(rowKey, true);
      scan.withStopRow(rowKey, true);
      scan.setRaw(true);
      scan.readAllVersions();
      scan.setCacheBlocks(false);
      scan.setTimeRange(fromTime, Long.MAX_VALUE);
      scan.setCaching(1);
      scan.setLimit(1);
      TargetRowRecord rowRecord = new TargetRowRecord();
      try (ResultScanner scanner = targetHTable.getScanner(scan)) {
        Result raw = scanner.next();
        if (raw != null) {
          for (Cell cell : raw.rawCells()) {
            rowRecord.record(cell);
          }
        }
      }
      return rowRecord;
    }

    void record(Cell cell) {
      if (CellUtil.isDelete(cell)) {
        recordTombstone(cell);
      } else {
        targetPutTs.computeIfAbsent(ColumnKey.of(cell), k -> new TreeMap<>())
          .put(cell.getTimestamp(), Boolean.TRUE);
      }
    }

    /**
     * Records one tombstone into its per-subtype map for {@link #wouldShadow} to query.
     * {@code <=ts} delete subtypes ({@code DeleteColumn}, {@code DeleteFamily}) collapse to the max
     * ts; exact-ts subtypes ({@code Delete}, {@code DeleteFamilyVersion}) accumulate into a set.
     */
    private void recordTombstone(Cell tombstone) {
      long ts = tombstone.getTimestamp();
      ByteBuffer family = ByteBuffer.wrap(CellUtil.cloneFamily(tombstone));
      switch (tombstone.getType()) {
        case Delete:
          deletePointTs.computeIfAbsent(ColumnKey.of(tombstone), k -> new HashSet<>()).add(ts);
          break;
        case DeleteColumn:
          deleteColumnUpperBound.merge(ColumnKey.of(tombstone), ts, Math::max);
          break;
        case DeleteFamily:
          deleteFamilyUpperBound.merge(family, ts, Math::max);
          break;
        case DeleteFamilyVersion:
          deleteFamilyVersionTs.computeIfAbsent(family, k -> new HashSet<>()).add(ts);
          break;
        default:
          // Unreachable: caller filters via CellUtil.isDelete.
      }
    }

    /** Returns true if any tombstone recorded here would shadow a Put at the cell's coords. */
    boolean wouldShadow(Cell sourcePut) {
      long ts = sourcePut.getTimestamp();
      ByteBuffer family = ByteBuffer.wrap(CellUtil.cloneFamily(sourcePut));
      ColumnKey column = ColumnKey.of(sourcePut);

      // Delete: shadows Put at exactly (cf, q, ts == T).
      Set<Long> pointTs = deletePointTs.get(column);
      if (pointTs != null && pointTs.contains(ts)) {
        return true;
      }
      // DeleteColumn: shadows every Put at (cf, q) with ts <= T.
      Long deleteColTs = deleteColumnUpperBound.get(column);
      if (deleteColTs != null && ts <= deleteColTs) {
        return true;
      }
      // DeleteFamily: shadows every Put across all qualifiers in cf with ts <= T.
      Long deleteFamTs = deleteFamilyUpperBound.get(family);
      if (deleteFamTs != null && ts <= deleteFamTs) {
        return true;
      }
      // DeleteFamilyVersion: shadows Puts across all qualifiers in cf at exactly ts == T.
      Set<Long> dfvTs = deleteFamilyVersionTs.get(family);
      return dfvTs != null && dfvTs.contains(ts);
    }

    /**
     * Returns target's Put timestamps at {@code (cf, q)} that are strictly greater than
     * {@code lowerExclusive} and strictly less than {@code upperExclusive}. Used to find hidden
     * (max-versions-filtered) target versions sitting between source's max ts and target's visible
     * ts so they can be point-Deleted.
     */
    Set<Long> targetPutTimestampsBetween(byte[] family, byte[] qualifier, long lowerExclusive,
      long upperExclusive) {
      NavigableMap<Long, Boolean> putTimestamps = targetPutTs.get(new ColumnKey(family, qualifier));
      if (putTimestamps == null) {
        return Collections.emptySet();
      }
      return putTimestamps.subMap(lowerExclusive, false, upperExclusive, false).keySet();
    }
  }

  /** Composite (family, qualifier) key with byte-array equality semantics. */
  static final class ColumnKey {
    private final byte[] family;
    private final byte[] qualifier;

    ColumnKey(byte[] family, byte[] qualifier) {
      this.family = family;
      this.qualifier = qualifier;
    }

    static ColumnKey of(Cell cell) {
      return new ColumnKey(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell));
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
   * Per-row scratch buffer: lazily-built {@link Put}/{@link Delete} mutations, lazily-loaded
   * {@link TargetRowRecord}, and an unrepairable-drift flag the caller reads after the merge.
   */
  final class RowRepairBuffer {
    private final byte[] rowKey;
    Put put;
    Delete delete;
    TargetRowRecord targetRowRecord;
    boolean anyCellUnrepairable;

    RowRepairBuffer(byte[] rowKey) {
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
     * Returns the cached {@link TargetRowRecord} for this row, loading on first call via
     * {@link TargetRowRecord#load} (one raw all-versions scan, time range
     * {@code [fromTime, MAX_VALUE]}). Cache scope is the buffer's lifetime — i.e. the current row —
     * so repeated cell-level lookups within the row pay one round-trip total.
     * <p>
     * Two consumers:
     * <p>
     * <b>Shadow detection</b> — {@link #mirrorSourceCellUnlessShadowed} asks
     * {@link TargetRowRecord#wouldShadow} before mirroring a source Put, to skip writes that
     * target's existing tombstones would render invisible.
     *
     * <pre>
     *   target row state: DeleteColumn(NAME)@T=900   (covers ts &lt;= 900)
     *   source row state: Put(NAME, "alice")@T=500
     *   wouldShadow(srcPut@500) → true
     *   ⇒ skip mirror, mark row unrepairable; operator must major-compact target
     * </pre>
     * <p>
     * <b>Hidden-version discovery</b> — {@link #tombstoneTargetCell} asks
     * {@link TargetRowRecord#targetPutTimestampsBetween} for max-versions-filtered Puts sitting
     * between source's max ts and target's visible ts, so each can be point-Deleted before they
     * surface above source's mirror.
     *
     * <pre>
     *   target row state (MAX_VERSIONS=3):
     *     Put(NAME, "carol")@T=900   visible
     *     Put(NAME, "bob")  @T=600   hidden
     *     Put(NAME, "alice")@T=300   hidden
     *   source row state:
     *     Put(NAME, "alice")@T=300   (sourceMaxTs=300)
     *   targetPutTimestampsBetween(NAME, 300, 900) → {600}
     *     point-Delete T=900 (visible) and T=600 (hidden) so T=300 surfaces
     * </pre>
     */
    TargetRowRecord targetRowRecord(Table targetHTable) throws IOException {
      if (targetRowRecord == null) {
        targetRowRecord = TargetRowRecord.load(rowKey, targetHTable, fromTime);
      }
      return targetRowRecord;
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
   * Cell-level drift counts produced by per-row diff. Three counters partition the cell differences
   * into disjoint buckets — source-only, target-only-live, same-coord-diff-value.
   */
  static final class CellDriftCounts {
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

  /** Per-row drift summary: cell-level drift counts plus a row-unrepairable flag. */
  static final class RowDriftInfo {
    static final RowDriftInfo NONE = new RowDriftInfo(CellDriftCounts.NONE, false);

    final CellDriftCounts cells;
    final boolean rowCannotRepair;

    RowDriftInfo(CellDriftCounts cells, boolean rowCannotRepair) {
      this.cells = cells;
      this.rowCannotRepair = rowCannotRepair;
    }
  }

  /** Terminal classification of a per-row mirror attempt onto target. */
  enum RowMirrorStatus {
    /** All source cells mirrored in row. */
    FULLY_MIRRORED,
    /** Some mirrored, some suppressed by target tombstones. */
    PARTIALLY_MIRRORED,
    /** Every source cell suppressed by target tombstones. */
    FULLY_SHADOWED
  }
}
