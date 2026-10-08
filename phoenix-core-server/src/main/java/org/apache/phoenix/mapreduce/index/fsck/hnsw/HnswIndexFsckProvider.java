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
package org.apache.phoenix.mapreduce.index.fsck.hnsw;

import io.github.jbellis.jvector.graph.NodesIterator;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.disk.feature.FeatureId;
import io.github.jbellis.jvector.graph.similarity.ScoreFunction;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.phoenix.coprocessor.IndexToolVerificationResult.PhaseResult;
import org.apache.phoenix.hbase.index.hnsw.HnswSegment;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.vector.HnswIndexManager;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.mapreduce.index.IndexVerificationOutputRepository;
import org.apache.phoenix.mapreduce.index.IndexVerificationOutputRow;
import org.apache.phoenix.mapreduce.index.fsck.Finding;
import org.apache.phoenix.mapreduce.index.fsck.GlobalIndexFsckProvider;
import org.apache.phoenix.mapreduce.index.fsck.IndexFsckContext;
import org.apache.phoenix.mapreduce.index.fsck.IndexFsckTool;
import org.apache.phoenix.mapreduce.index.fsck.RepairAction;
import org.apache.phoenix.mapreduce.index.fsck.RepairPlan;
import org.apache.phoenix.mapreduce.index.fsck.Report;
import org.apache.phoenix.mapreduce.index.fsck.Retry;
import org.apache.phoenix.mapreduce.index.fsck.Severity;
import org.apache.phoenix.mapreduce.index.fsck.VerifyFindings;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.EnvironmentEdgeManager;

/**
 * FSCK provider for HNSW vector indexes.
 * <p>
 * Unlike traditional global indexes where each base row maps to an individual index row, HNSW
 * indexes partition vector graphs into segment rows per HBase region (base segments and associated
 * deltas) combined with server-side memstore graphs.
 * <p>
 * Operational modes:
 * <ul>
 * <li>{@code verify}: Executes a MapReduce job ({@link HnswVerifyMapper}) comparing region rows
 * against open segments and recording discrepancies in {@link IndexTool} verification tables.</li>
 * <li>{@code fsck}: Supports two diagnostic scopes:
 * <ul>
 * <li>{@code CATALOG}: Validates index segment metadata without decoding graph payloads, detecting
 * stray or malformed rows, unresolved MOB references, region key coverage gaps, and superseded
 * segments.</li>
 * <li>{@code SEGMENTS}: Validates segment payload integrity, binary framing, graph structure,
 * ordinal mappings, graph reachability, and sample recall against brute-force vector search.</li>
 * </ul>
 * </li>
 * <li>{@code repair}: Cleans up orphan rows, removes corrupt or superseded segment entries, and
 * triggers server-side segment rebuilds for affected regions without directly mutating index graph
 * structures.</li>
 * </ul>
 */
public class HnswIndexFsckProvider extends GlobalIndexFsckProvider {
  public static final String SCOPE_CATALOG = "CATALOG";
  public static final String SCOPE_SEGMENTS = "SEGMENTS";

  public static final String ACTIVE_UNBUILT = "ACTIVE_UNBUILT";
  public static final String STRAY_ROW = "STRAY_ROW";
  public static final String SEGMENT_MALFORMED = "SEGMENT_MALFORMED";
  public static final String PAYLOAD_UNRESOLVED = "PAYLOAD_UNRESOLVED";
  public static final String RANGE_UNCOVERED = "RANGE_UNCOVERED";
  public static final String SEGMENT_SUPERSEDED = "SEGMENT_SUPERSEDED";
  public static final String SEGMENT_CORRUPT = "SEGMENT_CORRUPT";
  public static final String SEGMENT_INVALID = "SEGMENT_INVALID";
  public static final String GRAPH_UNREACHABLE = "GRAPH_UNREACHABLE";
  public static final String RECALL = "RECALL";
  public static final String ROWS_VERIFIED = "ROWS_VERIFIED";
  public static final String ROWS_BACKLOG = "ROWS_BACKLOG";
  public static final String SEGMENTS_UNREADABLE = "SEGMENTS_UNREADABLE";
  public static final String REPAIR_REFUSED = "REPAIR_REFUSED";

  public static final String DELETE_STRAY_ROWS = "DELETE_STRAY_ROWS";
  public static final String DELETE_CORRUPT_SEGMENTS = "DELETE_CORRUPT_SEGMENTS";
  public static final String REBUILD_REGIONS = "REBUILD_REGIONS";
  public static final String RETIRE_SEGMENTS = "RETIRE_SEGMENTS";

  /** Number of nearest neighbors evaluated during sample recall checks. */
  static final int RECALL_K = 10;
  /** Number of query vectors sampled per segment for recall validation. */
  static final int RECALL_QUERIES = 10;
  /** Minimum acceptable recall threshold before generating a warning finding. */
  static final double RECALL_WARN = 0.9;
  /** Maximum allowable fraction of unreachable graph nodes before generating a warning finding. */
  static final double UNREACHABLE_WARN = 0.05;

  private static final VectorTypeSupport VTS =
    VectorizationProvider.getInstance().getVectorTypeSupport();

  @Override
  public Report verify(IndexFsckContext context) throws Exception {
    rejectTimeRange(context);
    Report report = new Report(IndexFsckTool.CMD_VERIFY, context);
    checkTable(context, report);
    if (canVerifyRows(context, report)) {
      report.addFindings(verifyRows(context).findings);
    }
    return report;
  }

  @Override
  public Report fsck(IndexFsckContext context) throws Exception {
    Report report = super.fsck(context);
    try (HnswIndexReader reader = reader(context)) {
      report.addFindings(check(context, reader).findings);
    }
    return report;
  }

  @Override
  public Report inspect(IndexFsckContext context, String command, List<String> args)
    throws Exception {
    Report report = new Report(IndexFsckTool.CMD_INSPECT + " " + command, context);
    try (HnswIndexReader reader = reader(context)) {
      new HnswIndexInspector(context, reader).inspect(command, args, report);
    }
    return report;
  }

  /**
   * Pre-validation to ensure repair is not executed while the index is actively building or
   * underlying data regions are splitting/merging.
   */
  @Override
  public Report repair(IndexFsckContext context) throws Exception {
    rejectTimeRange(context);
    String refusal = refusal(context);
    if (refusal != null) {
      Report report = new Report(IndexFsckTool.CMD_REPAIR, context);
      report.setRepairPlan(new RepairPlan(!context.isConfirm()));
      report.addFinding(new Finding(Severity.ERROR, SCOPE_CATALOG, REPAIR_REFUSED, refusal));
      return report;
    }
    return super.repair(context);
  }

  /** Verification is disallowed while an index remains in BUILDING state. */
  @Override
  protected boolean canVerifyRows(IndexFsckContext context, Report report) {
    if (!super.canVerifyRows(context, report)) {
      return false;
    }
    if (context.getIndexTable().getIndexState() == PIndexState.BUILDING) {
      report.addFinding(new Finding(Severity.WARN, VerifyFindings.SCOPE, ROWS_NOT_VERIFIED,
        "Rows are not verified while the index is BUILDING; build it with IndexTool"));
      return false;
    }
    return true;
  }

  @Override
  protected List<Finding> planRepair(IndexFsckContext context, RepairPlan plan) throws Exception {
    Report checks = new Report(IndexFsckTool.CMD_REPAIR, context);
    checkTable(context, checks);
    try (HnswIndexReader reader = reader(context)) {
      Checks found = check(context, reader);
      checks.addFindings(found.findings);
      if (!found.stray.isEmpty()) {
        plan.add(DELETE_STRAY_ROWS,
          "Delete " + found.stray.size() + " index rows that are not segment rows");
      }
      if (!found.corrupt.isEmpty()) {
        plan.add(DELETE_CORRUPT_SEGMENTS,
          "Delete " + found.corrupt.size() + " corrupt segments and rebuild their regions");
      }
      boolean rebuild = !found.corrupt.isEmpty() || found.rebuildAll || !found.uncovered.isEmpty();
      if (canVerifyRows(context, checks)) {
        VerifyRun run = verifyRows(context);
        checks.addFindings(run.findings);
        rebuild |= run.hasFailures();
      }
      if (rebuild) {
        plan.add(REBUILD_REGIONS, "Rebuild the regions whose segments are missing, corrupt, or "
          + "hold rows that differ from the data table");
      }
      if (!found.superseded.isEmpty()) {
        plan.add(RETIRE_SEGMENTS,
          "Delete " + found.superseded.size() + " segments wholly covered by newer segments");
      }
    }
    return new ArrayList<>(checks.getFindings());
  }

  /**
   * Executes a repair iteration: purges stray rows, deletes corrupt segments, rebuilds affected
   * regions, and purges superseded segments.
   */
  @Override
  protected List<Finding> repairRound(IndexFsckContext context, RepairPlan plan, int round)
    throws Exception {
    Report checks = new Report(IndexFsckTool.CMD_REPAIR, context);
    checkTable(context, checks);
    try (HnswIndexReader reader = reader(context)) {
      HnswIndexContext hnsw = reader.getContext();
      Checks found = check(context, reader);
      if (!found.stray.isEmpty()) {
        deleteStrayRows(reader, action(plan, DELETE_STRAY_ROWS,
          "Delete index rows that are not " + "segment rows", round));
      }
      List<byte[][]> ranges = new ArrayList<>(found.uncovered);
      if (!found.corrupt.isEmpty()) {
        ranges.addAll(deleteCorruptSegments(context, reader, found.corrupt, action(plan,
          DELETE_CORRUPT_SEGMENTS, "Delete corrupt segments and rebuild their regions", round)));
      }
      List<byte[]> keys = new ArrayList<>();
      if (canVerifyRows(context, checks)) {
        keys.addAll(verifyRows(context).failedRows(hnsw));
      }
      if (found.rebuildAll || !ranges.isEmpty() || !keys.isEmpty()) {
        RepairAction rebuild = action(plan, REBUILD_REGIONS, "Rebuild the regions whose segments "
          + "are missing, corrupt, or hold rows that differ from the data table", round);
        Set<String> rebuilt = new LinkedHashSet<>();
        boolean all = found.rebuildAll;
        Retry.run("Rebuild HNSW regions", () -> IndexTool.buildHnswRegions(hnsw.getConnection(),
          hnsw.getDataTable(), hnsw.getIndexTable(), region -> {
            boolean selected = all || overlapsAny(region, ranges) || holdsAny(region, keys);
            if (selected) {
              rebuilt.add(region.getEncodedName());
            }
            return selected;
          }));
        rebuild.putDetail("regions", new ArrayList<>(rebuilt));
        rebuild.markExecuted();
      }
      retireSuperseded(reader, plan, round);
      checks.addFindings(check(context, reader).findings);
    }
    if (canVerifyRows(context, checks)) {
      checks.addFindings(verifyRows(context).findings);
    }
    return new ArrayList<>(checks.getFindings());
  }

  private static RepairAction action(RepairPlan plan, String name, String description, int round) {
    RepairAction action = plan.add(name, description);
    action.putDetail("round", round);
    return action;
  }

  private static void deleteStrayRows(HnswIndexReader reader, RepairAction action)
    throws Exception {
    // Re-evaluate current rows to avoid deleting concurrently registered segments
    List<String> deleted = new ArrayList<>();
    try (Table table = indexTable(reader)) {
      for (Result stray : reader.listStrayRows()) {
        Retry.run("Delete stray row", () -> table.delete(new Delete(stray.getRow())));
        deleted.add(Bytes.toStringBinary(stray.getRow()));
      }
    }
    action.putDetail("deletedRows", deleted);
    action.markExecuted();
  }

  /** Deletes confirmed corrupt segments and returns their key boundaries for targeted rebuild. */
  private List<byte[][]> deleteCorruptSegments(IndexFsckContext context, HnswIndexReader reader,
    List<HnswSegment.Descriptor> corrupt, RepairAction action) throws Exception {
    List<byte[][]> ranges = new ArrayList<>();
    List<String> deleted = new ArrayList<>();
    try (Table table = indexTable(reader)) {
      for (HnswSegment.Descriptor d : corrupt) {
        List<Finding> recheck = new ArrayList<>();
        checkSegment(context, reader, d, recheck);
        if (recheck.stream().noneMatch(HnswIndexFsckProvider::isCorruption)) {
          continue;
        }
        Retry.run("Delete corrupt segment", () -> table.delete(new Delete(d.rowKey)));
        deleted.add(Bytes.toStringBinary(d.rowKey));
        ranges.add(new byte[][] { d.startKey, d.endKey });
      }
    }
    action.putDetail("deletedSegments", deleted);
    action.markExecuted();
    return ranges;
  }

  private static void retireSuperseded(HnswIndexReader reader, RepairPlan plan, int round)
    throws Exception {
    List<HnswSegment.Descriptor> retire = HnswIndexManager.segmentsToRetire(reader.listSegments(),
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
    if (retire.isEmpty()) {
      return;
    }
    RepairAction action =
      action(plan, RETIRE_SEGMENTS, "Delete segments wholly covered by newer segments", round);
    List<String> deleted = new ArrayList<>();
    try (Table table = indexTable(reader)) {
      for (HnswSegment.Descriptor d : retire) {
        Retry.run("Retire segment", () -> table.delete(new Delete(d.rowKey)));
        deleted.add(Bytes.toStringBinary(d.rowKey));
      }
    }
    action.putDetail("deletedSegments", deleted);
    action.markExecuted();
  }

  private static Table indexTable(HnswIndexReader reader) throws SQLException {
    HnswIndexContext hnsw = reader.getContext();
    return hnsw.getConnection().getQueryServices()
      .getTable(hnsw.getIndexTable().getPhysicalName().getBytes());
  }

  private static boolean overlapsAny(RegionInfo region, List<byte[][]> ranges) {
    byte[] start = region.getStartKey();
    byte[] end = region.getEndKey();
    for (byte[][] range : ranges) {
      if (
        (end.length == 0 || Bytes.compareTo(range[0], end) < 0)
          && (range[1].length == 0 || Bytes.compareTo(start, range[1]) < 0)
      ) {
        return true;
      }
    }
    return false;
  }

  private static boolean holdsAny(RegionInfo region, List<byte[]> keys) {
    for (byte[] key : keys) {
      if (region.containsRow(key)) {
        return true;
      }
    }
    return false;
  }

  // HNSW verification evaluates current segment state against current data table rows without
  // point-in-time windowing
  private static void rejectTimeRange(IndexFsckContext context) {
    if (context.getStartTime() != null || context.getEndTime() != null) {
      throw new IllegalArgumentException(
        "HNSW indexes are verified as of now; -st and -et do " + "not apply");
    }
  }

  private static String refusal(IndexFsckContext context) throws Exception {
    PTable index = context.getIndexTable();
    if (index.getIndexState() == PIndexState.BUILDING) {
      return "The index is BUILDING; repair it when IndexTool finishes building it";
    }
    PhoenixConnection pconn = context.getConnection().unwrap(PhoenixConnection.class);
    try (Admin admin = pconn.getQueryServices().getAdmin()) {
      ClusterMetrics metrics =
        admin.getClusterMetrics(EnumSet.of(ClusterMetrics.Option.REGIONS_IN_TRANSITION));
      for (RegionState state : metrics.getRegionStatesInTransition()) {
        if (
          state.getRegion().getTable()
            .equals(org.apache.hadoop.hbase.TableName
              .valueOf(context.getDataTable().getPhysicalName().getBytes()))
            && (state.isSplitting() || state.isSplittingNew() || state.isMerging()
              || state.isMergingNew())
        ) {
          return "Region " + state.getRegion().getEncodedName() + " of the data table is "
            + state.getState() + "; repair the index when the split or merge finishes";
        }
      }
    }
    return null;
  }

  private static HnswIndexReader reader(IndexFsckContext context) throws IOException, SQLException {
    return new HnswIndexReader(
      new HnswIndexContext(context.getConnection().unwrap(PhoenixConnection.class),
        context.getDataTable(), context.getIndexTable()));
  }

  /** Aggregated results and metrics from an HNSW verification execution. */
  static final class VerifyRun {
    final long scanMaxTs;
    final List<Finding> findings = new ArrayList<>();
    private final long failures;

    VerifyRun(long scanMaxTs, Counters counters) {
      this.scanMaxTs = scanMaxTs;
      PhaseResult data = new PhaseResult();
      data.setMissingIndexRowCount(get(counters, HnswVerifyMapper.Counters.MISSING));
      data.setInvalidIndexRowCount(get(counters, HnswVerifyMapper.Counters.INVALID));
      PhaseResult segments = new PhaseResult();
      segments.setExtraVerifiedIndexRowCount(get(counters, HnswVerifyMapper.Counters.ORPHAN));
      segments.setExpiredIndexRowCount(get(counters, HnswVerifyMapper.Counters.EXPIRED));
      Map<String, Object> details = new LinkedHashMap<>();
      details.put("rows", get(counters, HnswVerifyMapper.Counters.ROWS));
      details.put("valid", get(counters, HnswVerifyMapper.Counters.VALID));
      details.put("scanMaxTs", scanMaxTs);
      findings.add(new Finding(
        Severity.INFO, VerifyFindings.SCOPE, ROWS_VERIFIED, details.get("valid") + " of "
          + details.get("rows") + " data rows with a vector are held " + "by the segments",
        details));
      findings.addAll(VerifyFindings.fromPhase(data, "data"));
      findings.addAll(VerifyFindings.fromPhase(segments, "index"));
      long backlog = get(counters, HnswVerifyMapper.Counters.BACKLOG);
      if (backlog > 0) {
        findings.add(new Finding(Severity.INFO, VerifyFindings.SCOPE, ROWS_BACKLOG,
          backlog + " rows changed since their segments are held in memory until the next flush",
          Collections.singletonMap("count", backlog)));
      }
      long unreadable = get(counters, HnswVerifyMapper.Counters.UNREADABLE);
      if (unreadable > 0) {
        findings.add(new Finding(
          Severity.ERROR, VerifyFindings.SCOPE, SEGMENTS_UNREADABLE, "The rows of " + unreadable
            + " regions are not verified, as their segments cannot be " + "read; run fsck",
          Collections.singletonMap("regions", unreadable)));
      }
      this.failures = data.getMissingIndexRowCount() + data.getInvalidIndexRowCount()
        + segments.getExtraVerifiedIndexRowCount();
    }

    boolean hasFailures() {
      return failures > 0;
    }

    /** Retrieves data row keys of verification discrepancies from {@code PHOENIX_INDEX_TOOL}. */
    List<byte[]> failedRows(HnswIndexContext hnsw) throws Exception {
      List<byte[]> keys = new ArrayList<>();
      if (failures == 0) {
        return keys;
      }
      byte[] index = hnsw.getIndexTable().getPhysicalName().getBytes();
      try (IndexVerificationOutputRepository output =
        new IndexVerificationOutputRepository(index, hnsw.getConnection())) {
        for (IndexVerificationOutputRow row : output.getOutputRows(scanMaxTs, index)) {
          keys.add(row.getDataTableRowKey());
        }
      }
      return keys;
    }

    private static long get(Counters counters, HnswVerifyMapper.Counters counter) {
      return counters.findCounter(counter).getValue();
    }
  }

  /** Launches the verification MapReduce job across all regions of the data table. */
  VerifyRun verifyRows(IndexFsckContext context) throws Exception {
    return Retry.call("HNSW verification", () -> {
      long scanMaxTs = EnvironmentEdgeManager.currentTimeMillis();
      Configuration conf = HBaseConfiguration.create(context.getConfiguration());
      conf.set(HnswVerifyMapper.INDEX_NAME, context.getIndexTableName());
      conf.set(HnswVerifyMapper.DATA_TABLE_NAME, context.getDataTableName());
      conf.setLong(HnswVerifyMapper.SCAN_MAX_TS, scanMaxTs);
      Job job = Job.getInstance(conf, "HnswVerify-" + context.getIndexTableName());
      job.setJarByClass(HnswVerifyMapper.class);
      try (HnswIndexReader reader = reader(context)) {
        TableMapReduceUtil.initTableMapperJob(reader.getContext().getDataPhysicalName(),
          reader.vectorScan(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW),
          HnswVerifyMapper.class, NullWritable.class, NullWritable.class, job);
      }
      job.setNumReduceTasks(0);
      job.setOutputFormatClass(NullOutputFormat.class);
      if (!job.waitForCompletion(false)) {
        throw new IOException("HNSW verification job " + job.getJobID() + " failed");
      }
      return new VerifyRun(scanMaxTs, job.getCounters());
    });
  }

  /** Diagnostic findings and planned repair targets across CATALOG and SEGMENTS scopes. */
  static final class Checks {
    final List<Finding> findings = new ArrayList<>();
    final List<byte[]> stray = new ArrayList<>();
    final List<HnswSegment.Descriptor> corrupt = new ArrayList<>();
    final List<byte[][]> uncovered = new ArrayList<>();
    final List<HnswSegment.Descriptor> superseded = new ArrayList<>();
    boolean rebuildAll;
  }

  Checks check(IndexFsckContext context, HnswIndexReader reader) throws Exception {
    Checks checks = new Checks();
    PTable index = context.getIndexTable();
    boolean active = index.getIndexState() == PIndexState.ACTIVE;
    List<HnswSegment.Descriptor> segments = reader.listSegments();

    if (active && segments.isEmpty() && reader.hasVectors()) {
      checks.rebuildAll = true;
      checks.findings.add(new Finding(Severity.ERROR, SCOPE_CATALOG, ACTIVE_UNBUILT,
        "The index is ACTIVE but has no segments though the table holds vectors; repair "
          + "rebuilds it"));
    }

    for (Result row : reader.listStrayRows()) {
      checks.stray.add(row.getRow());
      boolean segmentCells = false;
      for (byte[] qualifier : Arrays.asList(HnswSegment.END_KEY_QUALIFIER,
        HnswSegment.COUNT_QUALIFIER, HnswSegment.PAYLOAD_QUALIFIER, HnswSegment.BASE_TIME_QUALIFIER,
        HnswSegment.TOMBSTONES_QUALIFIER)) {
        segmentCells |= row.containsColumn(reader.getContext().getFamily(), qualifier);
      }
      Map<String, Object> details = new LinkedHashMap<>();
      details.put("row", Bytes.toStringBinary(row.getRow()));
      checks.findings.add(segmentCells
        ? new Finding(Severity.ERROR, SCOPE_CATALOG, SEGMENT_MALFORMED,
          "Index row " + Bytes.toStringBinary(row.getRow()) + " has segment cells but is not a "
            + "start key and time with well-formed E and N cells",
          details)
        : new Finding(Severity.ERROR, SCOPE_CATALOG, STRAY_ROW,
          "Index row " + Bytes.toStringBinary(row.getRow()) + " is not a segment row", details));
    }

    if (active) {
      for (RegionInfo region : reader.listRegions()) {
        if (!HnswSegment.covered(region.getStartKey(), region.getEndKey(), segments)) {
          checks.uncovered.add(new byte[][] { region.getStartKey(), region.getEndKey() });
          Map<String, Object> details = new LinkedHashMap<>();
          details.put("region", region.getEncodedName());
          checks.findings.add(new Finding(Severity.WARN, SCOPE_CATALOG, RANGE_UNCOVERED,
            "Region " + region.getEncodedName() + " has key ranges no segment covers; it "
              + "rebuilds when it opens",
            details));
        }
      }
    }

    checks.superseded.addAll(HnswIndexManager.segmentsToRetire(segments, HConstants.EMPTY_START_ROW,
      HConstants.EMPTY_END_ROW));
    for (HnswSegment.Descriptor d : checks.superseded) {
      checks.findings.add(new Finding(Severity.WARN, SCOPE_CATALOG, SEGMENT_SUPERSEDED,
        "Segment " + Bytes.toStringBinary(d.rowKey) + " is wholly covered by newer segments "
          + "but was not retired",
        segmentDetails(d)));
    }

    for (HnswSegment.Descriptor d : segments) {
      List<Finding> findings = new ArrayList<>();
      checkSegment(context, reader, d, findings);
      if (findings.stream().anyMatch(HnswIndexFsckProvider::isCorruption)) {
        checks.corrupt.add(d);
      }
      checks.findings.addAll(findings);
    }
    return checks;
  }

  private static boolean isCorruption(Finding finding) {
    return finding.getRule().equals(SEGMENT_CORRUPT) || finding.getRule().equals(SEGMENT_INVALID)
      || finding.getRule().equals(PAYLOAD_UNRESOLVED);
  }

  static Map<String, Object> segmentDetails(HnswSegment.Descriptor d) {
    Map<String, Object> details = new LinkedHashMap<>();
    details.put("segment", Bytes.toStringBinary(d.rowKey));
    details.put("time", d.time);
    if (d.isDelta()) {
      details.put("baseTime", d.baseTime);
    }
    return details;
  }

  /** Validates segment payload decoding, structural integrity, and graph connectivity/recall. */
  void checkSegment(IndexFsckContext context, HnswIndexReader reader, HnswSegment.Descriptor d,
    List<Finding> findings) throws Exception {
    HnswSegment segment;
    try {
      segment = reader.open(d);
    } catch (IOException | RuntimeException e) {
      byte[] payload;
      try {
        payload = reader.readPayload(d);
      } catch (IOException readFailure) {
        payload = null;
      }
      Map<String, Object> details = segmentDetails(d);
      details.put("error", String.valueOf(e.getMessage()));
      findings.add(payload != null && payload.length == 0
        ? new Finding(Severity.ERROR, SCOPE_SEGMENTS, PAYLOAD_UNRESOLVED,
          "The payload of segment " + Bytes.toStringBinary(d.rowKey) + " references a MOB "
            + "file that does not exist",
          details)
        : new Finding(Severity.ERROR, SCOPE_SEGMENTS, SEGMENT_CORRUPT,
          "Segment " + Bytes.toStringBinary(d.rowKey) + " cannot be decoded: " + e.getMessage(),
          details));
      return;
    }
    try {
      List<String> problems = new ArrayList<>();
      try {
        checkStructure(reader.getContext(), d, segment, problems);
      } catch (RuntimeException e) {
        problems.add("the graph cannot be traversed: " + e);
      }
      if (!problems.isEmpty()) {
        Map<String, Object> details = segmentDetails(d);
        details.put("problems", problems);
        findings.add(new Finding(Severity.ERROR, SCOPE_SEGMENTS, SEGMENT_INVALID,
          "Segment " + Bytes.toStringBinary(d.rowKey) + " is invalid: " + problems.get(0)
            + (problems.size() > 1 ? " and " + (problems.size() - 1) + " more" : ""),
          details));
        return;
      }
      checkQuality(reader, d, segment, findings);
    } finally {
      segment.close();
    }
  }

  /** Validates internal segment structure including row key ordering, bounds, and graph layout. */
  static void checkStructure(HnswIndexContext hnsw, HnswSegment.Descriptor d, HnswSegment segment,
    List<String> problems) throws IOException {
    int size = segment.size();
    if (d.count != size) {
      problems.add("N records " + d.count + " rows but the mapping holds " + size);
    }
    for (int i = 1; i < size; i++) {
      if (Bytes.compareTo(segment.getKey(i - 1), segment.getKey(i)) >= 0) {
        problems.add("the mapping's row keys are not strictly ascending at ordinal " + i);
        break;
      }
    }
    List<byte[]> keys = new ArrayList<>(Arrays.asList(segment.getTombstones()));
    if (size > 0) {
      keys.add(segment.getKey(0));
      keys.add(segment.getKey(size - 1));
    }
    for (byte[] key : keys) {
      if (
        Bytes.compareTo(key, d.startKey) < 0
          || (d.endKey.length > 0 && Bytes.compareTo(key, d.endKey) >= 0)
      ) {
        problems.add("row key " + Bytes.toStringBinary(key) + " lies outside the segment's range");
        break;
      }
    }
    OnDiskGraphIndex graph = segment.graph();
    if (graph == null) {
      return;
    }
    if (graph.getDimension() != hnsw.getVectorIndex().getDimension()) {
      problems.add("the graph has dimension " + graph.getDimension() + " where the index has "
        + hnsw.getVectorIndex().getDimension());
    }
    if (graph.size(0) != size || graph.getIdUpperBound() != size) {
      problems.add("the graph holds " + graph.size(0) + " nodes with ids below "
        + graph.getIdUpperBound() + " for " + size + " mapped rows");
      return;
    }
    try (OnDiskGraphIndex.View view = graph.getView()) {
      OnDiskGraphIndex.NodeAtLevel entry = view.entryNode();
      if (
        entry == null || entry.node < 0 || entry.node >= size || entry.level != graph.getMaxLevel()
      ) {
        problems.add(
          "the entry node " + entry + " is not a node of the top level " + graph.getMaxLevel());
      }
      for (int level = 0; level <= graph.getMaxLevel() && problems.isEmpty(); level++) {
        for (NodesIterator nodes = graph.getNodes(level); nodes.hasNext();) {
          int node = nodes.nextInt();
          int degree = 0;
          for (NodesIterator it = view.getNeighborsIterator(level, node); it.hasNext();) {
            int neighbor = it.nextInt();
            degree++;
            if (neighbor < 0 || neighbor >= size || !view.contains(level, neighbor)) {
              problems.add("node " + node + " at level " + level + " has neighbor " + neighbor
                + ", which is not a node of the level");
            } else if (neighbor == node) {
              problems.add("node " + node + " at level " + level + " is its own neighbor");
            }
          }
          if (degree > graph.getDegree(level)) {
            problems.add("node " + node + " at level " + level + " has " + degree
              + " neighbors, more than the level's maximum of " + graph.getDegree(level));
          }
          if (problems.size() > 10) {
            return;
          }
        }
      }
      if (graph.getFeatureSet().contains(FeatureId.INLINE_VECTORS)) {
        for (int node = 0; node < size; node++) {
          VectorFloat<?> v = view.getVector(node);
          for (int i = 0; i < v.length(); i++) {
            if (!Float.isFinite(v.get(i))) {
              problems.add("the vector of node " + node + " is not finite");
              return;
            }
          }
        }
      }
    }
  }

  /**
   * Computes graph quality metrics including node reachability and approximate nearest neighbor
   * recall.
   */
  private void checkQuality(HnswIndexReader reader, HnswSegment.Descriptor d, HnswSegment segment,
    List<Finding> findings) throws IOException {
    OnDiskGraphIndex graph = segment.graph();
    if (graph == null) {
      return;
    }
    int size = segment.size();
    int unreachable = size - reachable(graph).cardinality();
    if (unreachable > 0) {
      Map<String, Object> details = segmentDetails(d);
      details.put("unreachable", unreachable);
      details.put("nodes", size);
      findings
        .add(
          new Finding(unreachable > UNREACHABLE_WARN * size ? Severity.WARN : Severity.INFO,
            SCOPE_SEGMENTS, GRAPH_UNREACHABLE, unreachable + " of " + size + " nodes of segment "
              + Bytes.toStringBinary(d.rowKey) + " cannot be reached from the entry node",
            details));
    }
    if (size <= RECALL_K) {
      return;
    }
    List<byte[]> sample = new ArrayList<>();
    for (int i = 0; i < RECALL_QUERIES; i++) {
      sample.add(segment.getKey((int) ((long) i * size / RECALL_QUERIES)));
    }
    Map<ImmutableBytesPtr, float[]> queries = reader.getVectors(sample);
    if (queries.isEmpty()) {
      return;
    }
    VectorSimilarityFunction similarity = reader.getContext().getSimilarity();
    double recall = 0;
    for (float[] query : queries.values()) {
      recall += recall(segment, VTS.createFloatVector(query), similarity);
    }
    recall /= queries.size();
    Map<String, Object> details = segmentDetails(d);
    details.put("recall", recall);
    details.put("queries", queries.size());
    details.put("k", RECALL_K);
    findings.add(new Finding(recall < RECALL_WARN ? Severity.WARN : Severity.INFO, SCOPE_SEGMENTS,
      RECALL, String.format("Segment %s finds %.3f of the %d nearest neighbors brute force finds",
        Bytes.toStringBinary(d.rowKey), recall, RECALL_K),
      details));
  }

  /** Traverses all hierarchy levels from entry node to compute graph reachability. */
  static BitSet reachable(OnDiskGraphIndex graph) throws IOException {
    BitSet seen = new BitSet(graph.size(0));
    try (OnDiskGraphIndex.View view = graph.getView()) {
      ArrayDeque<int[]> queue = new ArrayDeque<>();
      OnDiskGraphIndex.NodeAtLevel entry = view.entryNode();
      seen.set(entry.node);
      queue.add(new int[] { entry.node, entry.level });
      while (!queue.isEmpty()) {
        int[] next = queue.poll();
        for (int level = next[1]; level >= 0; level--) {
          for (NodesIterator it = view.getNeighborsIterator(level, next[0]); it.hasNext();) {
            int neighbor = it.nextInt();
            if (!seen.get(neighbor)) {
              seen.set(neighbor);
              queue.add(new int[] { neighbor, level });
            }
          }
        }
      }
    }
    return seen;
  }

  /**
   * Computes approximate search recall against exact brute-force top-k neighbors.
   */
  static double recall(HnswSegment segment, VectorFloat<?> query,
    VectorSimilarityFunction similarity) throws IOException {
    SearchResult result =
      segment.search(query, RECALL_K, QueryServicesOptions.DEFAULT_HNSW_EF_SEARCH, Bits.ALL);
    Set<Integer> found = new HashSet<>();
    for (SearchResult.NodeScore ns : result.getNodes()) {
      found.add(ns.node);
    }
    int hits = 0;
    for (double[] node : bruteForce(segment, query, similarity, RECALL_K, Bits.ALL)) {
      hits += found.contains((int) node[0]) ? 1 : 0;
    }
    return (double) hits / RECALL_K;
  }

  /**
   * Computes exact top-k nearest neighbors via exhaustive linear scan for recall benchmarking.
   */
  static List<double[]> bruteForce(HnswSegment segment, VectorFloat<?> query,
    VectorSimilarityFunction similarity, int k, Bits accept) throws IOException {
    OnDiskGraphIndex graph = segment.graph();
    PriorityQueue<double[]> best = new PriorityQueue<>(k + 1, (a, b) -> Double.compare(a[1], b[1]));
    try (OnDiskGraphIndex.View view = graph.getView()) {
      boolean exact = graph.getFeatureSet().contains(FeatureId.INLINE_VECTORS);
      ScoreFunction.ExactScoreFunction reranker =
        exact ? null : view.rerankerFor(query, similarity);
      for (int node = 0; node < segment.size(); node++) {
        if (!accept.get(node)) {
          continue;
        }
        float score =
          exact ? similarity.compare(query, view.getVector(node)) : reranker.similarityTo(node);
        best.add(new double[] { node, score });
        if (best.size() > k) {
          best.poll();
        }
      }
    }
    List<double[]> nodes = new ArrayList<>(best.size());
    while (!best.isEmpty()) {
      nodes.add(0, best.poll());
    }
    return nodes;
  }
}
