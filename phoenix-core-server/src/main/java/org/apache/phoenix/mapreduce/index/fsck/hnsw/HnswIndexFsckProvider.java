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
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.phoenix.coprocessor.IndexToolVerificationResult;
import org.apache.phoenix.coprocessor.IndexToolVerificationResult.PhaseResult;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.hbase.index.hnsw.HnswSegment;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.vector.HnswIndexManager;
import org.apache.phoenix.index.vector.HnswRegionRequests;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository;
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
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;

/**
 * The fsck provider for HNSW vector indexes.
 * <p>
 * A global index has one index row for each data row. An HNSW index has segments instead. Each data
 * region writes a stack of a base segment and delta segments, and each segment holds a graph of
 * rows of the region. The segments are cells of the hidden family {@code V#} of the data table, in
 * the anchor rows of the regions. Each region holds and retires its own segments. The region server
 * also holds the rows that changed since the last flush in an in-memory graph. Thus the checks
 * count a row that changed since the replay start as backlog, not as an error.
 * <p>
 * The commands:
 * <ul>
 * <li>{@code verify}: Runs a MapReduce job of {@link HnswVerifyMapper} that compares the data rows
 * of each region with the current segments of the region. The job writes the errors and the counts
 * to the {@link IndexTool} output and result tables.</li>
 * <li>{@code fsck}: Adds the findings of two scopes to the global index checks:
 * <ul>
 * <li>{@code CATALOG}: Checks the segment cells and does not decode the payloads. It finds stray
 * and malformed cells, and an ACTIVE index with no segments. It also finds regions that no base
 * segment covers, stacks that another region wrote, and superseded segments.</li>
 * <li>{@code SEGMENTS}: Opens and checks each segment. It finds payloads that refer to a missing
 * MOB file, payloads that do not decode, and faults in the mapping and the graph structure. It also
 * finds nodes that a search cannot reach, and low recall against a brute force search.</li>
 * </ul>
 * </li>
 * <li>{@code repair}: Never writes {@code V#}. It asks the regions to purge the cells of index keys
 * that are not HNSW indexes of the table. It also asks a region to rebuild when its segments are
 * missing, corrupt, malformed or superseded. The same applies to segments of another definition or
 * of another region, and to rows that fail verification. After the cutover of a rebuild, the region
 * retires the other segments of the index that it holds, and deletes the malformed cells. Only the
 * region servers write segments.</li>
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
  public static final String STACK_UNTRUSTED = "STACK_UNTRUSTED";
  public static final String SEGMENT_SUPERSEDED = "SEGMENT_SUPERSEDED";
  public static final String SEGMENT_CORRUPT = "SEGMENT_CORRUPT";
  public static final String SEGMENT_UNCHECKED = "SEGMENT_UNCHECKED";
  public static final String SEGMENT_INVALID = "SEGMENT_INVALID";
  public static final String GRAPH_UNREACHABLE = "GRAPH_UNREACHABLE";
  public static final String RECALL = "RECALL";
  public static final String ROWS_VERIFIED = "ROWS_VERIFIED";
  public static final String ROWS_BACKLOG = "ROWS_BACKLOG";
  public static final String ROWS_UNCOVERED = "ROWS_UNCOVERED";
  public static final String SEGMENTS_UNREADABLE = "SEGMENTS_UNREADABLE";
  public static final String SEGMENTS_UNCHECKED = "SEGMENTS_UNCHECKED";
  public static final String REPAIR_REFUSED = "REPAIR_REFUSED";

  public static final String PURGE_STRAY_CELLS = "PURGE_STRAY_CELLS";
  public static final String REBUILD_REGIONS = "REBUILD_REGIONS";

  private static final String REBUILD_DESCRIPTION = "Rebuild the regions whose segments are "
    + "missing, corrupt, malformed, superseded, of another definition or of another region, or "
    + "hold rows that differ from the data table";

  /** The number of nearest neighbors that a recall check compares. */
  static final int RECALL_K = 10;
  /** The number of rows of a segment whose vectors the recall check uses as queries. */
  static final int RECALL_QUERIES = 10;
  /** A mean recall less than this value gives a warning. */
  static final double RECALL_WARN = 0.9;
  /** A fraction of unreachable nodes more than this value gives a warning. */
  static final double UNREACHABLE_WARN = 0.05;
  /** The maximum time that a repair waits for the rebuilt regions to retire their old segments. */
  static final long RETIREMENT_WAIT_MS = 60_000;

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
   * Refuses to repair while the index is BUILDING, or while a data region splits or merges. The
   * IndexTool build owns a BUILDING index. A split or a merge changes the region boundaries that
   * the checks and the rebuilds use.
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

  /** Does not verify rows while the index is BUILDING, because the build is not complete. */
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
        plan.add(PURGE_STRAY_CELLS, "Purge the cells of " + found.stray.size()
          + " index keys that are not HNSW indexes of the table");
      }
      boolean rebuild = found.rebuildAll || !found.rebuild.isEmpty();
      if (canVerifyRows(context, checks)) {
        VerifyRun run = verifyRows(context);
        checks.addFindings(run.findings);
        rebuild |= run.hasFailures();
      }
      if (rebuild) {
        plan.add(REBUILD_REGIONS, REBUILD_DESCRIPTION);
      }
    }
    return new ArrayList<>(checks.getFindings());
  }

  /**
   * Runs one repair round. The round checks the index again, asks the regions to purge the cells of
   * stray index keys, and rebuilds the regions that need it. After the cutover of a rebuild, the
   * region retires the other segments of the index that it holds, and deletes the malformed cells.
   * The round returns the findings of the checks after these changes, so that the caller can decide
   * on another round.
   */
  @Override
  protected List<Finding> repairRound(IndexFsckContext context, RepairPlan plan, int round)
    throws Exception {
    context.refreshIndexTable();
    Report checks = new Report(IndexFsckTool.CMD_REPAIR, context);
    checkTable(context, checks);
    try (HnswIndexReader reader = reader(context)) {
      HnswIndexContext hnsw = reader.getContext();
      Checks found = check(context, reader);
      if (!found.stray.isEmpty()) {
        purgeStrayCells(reader, found.stray, action(plan, PURGE_STRAY_CELLS,
          "Purge the cells of index keys that are not HNSW indexes of the table", round));
      }
      // When every region rebuilds, the round does not verify rows to find the failed regions
      Set<String> failed = new HashSet<>();
      if (!found.rebuildAll && canVerifyRows(context, checks)) {
        failed.addAll(verifyRows(context).failedRegions(hnsw));
      }
      if (found.rebuildAll || !found.rebuild.isEmpty() || !failed.isEmpty()) {
        RepairAction rebuild = action(plan, REBUILD_REGIONS, REBUILD_DESCRIPTION, round);
        Set<String> rebuilt = new LinkedHashSet<>();
        boolean all = found.rebuildAll;
        // The round lists the segments once, so a retry does not rebuild a region that an earlier
        // attempt rebuilt
        List<HnswSegment.Descriptor> before = reader.listSegments();
        rebuild.logExecuting();
        Retry.run("Rebuild HNSW regions", () -> IndexTool.buildHnswRegions(hnsw.getConnection(),
          hnsw.getDataTable(), hnsw.getIndexTable(), region -> {
            boolean selected = all || found.rebuild.contains(region.getEncodedName())
              || failed.contains(region.getEncodedName());
            if (selected) {
              rebuilt.add(region.getEncodedName());
            }
            return selected;
          }, region -> IndexTool.rebuiltSince(before, region)));
        rebuild.putDetail("regions", new ArrayList<>(rebuilt));
        awaitRetirement(reader, rebuilt);
        rebuild.markExecuted();
      }
      checks.addFindings(check(context, reader).findings);
    }
    if (canVerifyRows(context, checks)) {
      checks.addFindings(verifyRows(context).findings);
    }
    return new ArrayList<>(checks.getFindings());
  }

  /**
   * Waits until each rebuilt region retires the segments that its new base supersedes and deletes
   * the malformed cells of the index. A region writes its new base, cuts over to it, and then
   * deletes the malformed cells and retires the old segments. The build ends when the region writes
   * the new base, so the build does not wait for the retirement. A region without an earlier base
   * has no segment to retire, so the method also waits for the malformed cells. After
   * {@link #RETIREMENT_WAIT_MS}, the method returns, and the checks report the cells that remain.
   */
  static void awaitRetirement(HnswIndexReader reader, Set<String> regions) throws Exception {
    byte[] definition = reader.getContext().getDefinition();
    long deadline = EnvironmentEdgeManager.currentTimeMillis() + RETIREMENT_WAIT_MS;
    while (true) {
      HnswIndexReader.Catalog catalog = reader.readCatalog();
      List<RegionInfo> listed = reader.listRegions();
      boolean retired = true;
      for (RegionInfo region : listed) {
        if (regions.contains(region.getEncodedName())) {
          byte[] start = region.getStartKey();
          byte[] end = region.getEndKey();
          retired &=
            HnswIndexManager.segmentsToRetire(HnswIndexReader.held(catalog.segments, start, end),
              start, end, definition).isEmpty();
        }
      }
      for (byte[][] malformed : catalog.malformed) {
        RegionInfo region = HnswIndexReader.regionOf(listed, malformed[0]);
        retired &= region == null || !regions.contains(region.getEncodedName());
      }
      if (retired || EnvironmentEdgeManager.currentTimeMillis() > deadline) {
        return;
      }
      Thread.sleep(1000);
    }
  }

  private static RepairAction action(RepairPlan plan, String name, String description, int round) {
    RepairAction action = plan.add(name, description);
    action.putDetail("round", round);
    return action;
  }

  /**
   * Asks each region that holds cells of a stray index key to purge the cells of that key. A purge
   * is idempotent. A region refuses to purge a key that names an HNSW index of its table. An
   * example is an index that a user creates again after the check. The method then skips the key
   * and reports it.
   */
  private static void purgeStrayCells(HnswIndexReader reader, Map<String, Set<String>> stray,
    RepairAction action) throws Exception {
    action.putDetail("indexKeys", new ArrayList<>(stray.keySet()));
    action.logExecuting();
    HnswIndexContext hnsw = reader.getContext();
    List<RegionInfo> regions = reader.listRegions();
    List<String> skipped = new ArrayList<>();
    long purged = 0;
    try (Table data = hnsw.getConnection().getQueryServices()
      .getTable(hnsw.getDataTable().getPhysicalName().getBytes())) {
      for (Map.Entry<String, Set<String>> e : stray.entrySet()) {
        List<RegionInfo> holders = new ArrayList<>();
        for (RegionInfo region : regions) {
          if (e.getValue().contains(region.getEncodedName())) {
            holders.add(region);
          }
        }
        long[] cells = new long[1];
        boolean[] refused = new boolean[1];
        Retry.run("Purge stray HNSW cells", () -> {
          cells[0] = 0;
          refused[0] = false;
          HnswRegionRequests.send(data, holders, BaseScannerRegionObserverConstants.HNSW_PURGE,
            region -> Bytes.toBytes(e.getKey()), (region, value, failure) -> {
              if (failure != null) {
                if (!Retry.isUnrecoverable(failure)) {
                  throw failure;
                }
                refused[0] = true;
              } else if (value != null) {
                cells[0] += Bytes.toLong(value);
              }
            });
        });
        purged += cells[0];
        if (refused[0]) {
          skipped.add(e.getKey());
        }
      }
    }
    action.putDetail("purgedCells", purged);
    if (!skipped.isEmpty()) {
      action.putDetail("skippedIndexKeys", skipped);
    }
    action.markExecuted();
  }

  // The tools compare the current segments with the current data rows. The segments cannot show
  // the index at an earlier time, so a time range does not apply
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

  /** The findings and the failure count of one verification job, from the job counters. */
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
      long uncovered = get(counters, HnswVerifyMapper.Counters.UNCOVERED);
      if (uncovered > 0) {
        // As in RANGE_UNCOVERED, the regions read these rows without the index until their builds
        findings.add(new Finding(Severity.WARN, VerifyFindings.SCOPE, ROWS_UNCOVERED,
          uncovered + " rows are in regions that their segments do not cover; the regions read "
            + "them without the index until their builds",
          Collections.singletonMap("count", uncovered)));
      }
      long unreadable = get(counters, HnswVerifyMapper.Counters.UNREADABLE);
      if (unreadable > 0) {
        findings.add(new Finding(
          Severity.ERROR, VerifyFindings.SCOPE, SEGMENTS_UNREADABLE, "The rows of " + unreadable
            + " regions are not verified, as their segments cannot be " + "read; run fsck",
          Collections.singletonMap("regions", unreadable)));
      }
      long unchecked = get(counters, HnswVerifyMapper.Counters.UNCHECKED);
      if (unchecked > 0) {
        // As in SEGMENT_UNCHECKED, a size guard is not an error that repair can act on
        findings.add(new Finding(Severity.WARN, VerifyFindings.SCOPE, SEGMENTS_UNCHECKED,
          "The rows of " + unchecked + " regions are not verified, as their segments are too "
            + "large for the heap of the verification tasks",
          Collections.singletonMap("regions", unchecked)));
      }
      this.failures = data.getMissingIndexRowCount() + data.getInvalidIndexRowCount()
        + segments.getExtraVerifiedIndexRowCount();
    }

    boolean hasFailures() {
      return failures > 0;
    }

    /**
     * Returns the encoded names of the regions with verification failures, from the row that each
     * map task writes to {@code PHOENIX_INDEX_TOOL_RESULT}. The memory use grows with the region
     * count, not with the failure count.
     */
    Set<String> failedRegions(HnswIndexContext hnsw) throws Exception {
      Set<String> regions = new HashSet<>();
      if (failures == 0) {
        return regions;
      }
      // The row key is: scan max timestamp | index table | encoded region name | start | stop
      byte[] separator = IndexVerificationResultRepository.ROW_KEY_SEPARATOR_BYTE;
      byte[] prefix = Bytes.add(Bytes.toBytes(Long.toString(scanMaxTs)), separator,
        Bytes.add(hnsw.getIndexTable().getPhysicalName().getBytes(), separator));
      Scan scan = new Scan().withStartRow(prefix)
        .withStopRow(ByteUtil.calculateTheClosestNextRowKeyForPrefix(prefix));
      try (
        Table results = hnsw.getConnection().getQueryServices()
          .getTable(IndexVerificationResultRepository.getResultTableNameBytes());
        ResultScanner scanner = results.getScanner(scan)) {
        for (Result row : scanner) {
          IndexToolVerificationResult result = new IndexToolVerificationResult(scanMaxTs);
          for (Cell cell : row.rawCells()) {
            result.update(cell);
          }
          if (
            result.getBeforeRebuildMissingIndexRowCount()
              + result.getBeforeRebuildInvalidIndexRowCount()
              + result.getBeforeRepairExtraVerifiedIndexRowCount() > 0
          ) {
            // An encoded region name holds only hex digits, so the next separator ends it
            byte[] key = row.getRow();
            int end = prefix.length;
            while (end < key.length && key[end] != separator[0]) {
              end++;
            }
            regions.add(Bytes.toString(key, prefix.length, end - prefix.length));
          }
        }
      }
      return regions;
    }

    private static long get(Counters counters, HnswVerifyMapper.Counters counter) {
      return counters.findCounter(counter).getValue();
    }
  }

  /**
   * Runs the verification job on all regions of the data table, with one map task for each region.
   * A retry runs a new job with a new scan max timestamp.
   */
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
      if (!awaitJob(job)) {
        throw new IOException("HNSW verification job " + job.getJobID() + " failed");
      }
      return new VerifyRun(scanMaxTs, job.getCounters());
    });
  }

  /**
   * Submits the job and waits for it. If the wait fails, for example on an error from a poll of the
   * job status, the method kills the job. Thus a retry does not run a second job beside it. If the
   * kill fails, the job can still run, so the method throws an exception that prevents a retry.
   */
  static boolean awaitJob(Job job) throws Exception {
    try {
      return job.waitForCompletion(false);
    } catch (Exception e) {
      // An interrupt must not stop the kill, so clear it and set it again after the kill
      boolean interrupted = Thread.interrupted();
      try {
        if (!job.isComplete()) {
          job.killJob();
        }
      } catch (IllegalStateException notSubmitted) {
        // The job did not start, so no job runs
      } catch (IOException killFailure) {
        e.addSuppressed(killFailure);
        throw new DoNotRetryIOException(
          "HNSW verification job " + job.getJobID() + " may still run; it was not killed", e);
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
      throw e;
    }
  }

  /**
   * The findings of one check, and the repair targets that it found. {@code rebuildAll} is true
   * when an ACTIVE index has no segments but the data table holds vectors.
   */
  static final class Checks {
    final List<Finding> findings = new ArrayList<>();
    // The index keys that name no HNSW index of the table, with the regions that hold their cells
    final Map<String, Set<String>> stray = new TreeMap<>();
    // The encoded names of the regions to rebuild
    final Set<String> rebuild = new TreeSet<>();
    boolean rebuildAll;
  }

  Checks check(IndexFsckContext context, HnswIndexReader reader) throws Exception {
    Checks checks = new Checks();
    PTable index = context.getIndexTable();
    boolean active = index.getIndexState() == PIndexState.ACTIVE;
    byte[] definition = reader.getContext().getDefinition();
    HnswIndexReader.Catalog catalog = reader.readCatalog();
    List<HnswSegment.Descriptor> segments = reader.fenced(catalog.segments);
    List<RegionInfo> regions = reader.listRegions();

    if (active && segments.isEmpty() && reader.hasVectors()) {
      checks.rebuildAll = true;
      checks.findings.add(new Finding(Severity.ERROR, SCOPE_CATALOG, ACTIVE_UNBUILT,
        "The index is ACTIVE but has no segments though the table holds vectors; repair "
          + "rebuilds it"));
    }

    for (Map.Entry<String, List<byte[]>> e : catalog.strayKeys.entrySet()) {
      Set<String> holders = new TreeSet<>();
      Set<String> rows = new LinkedHashSet<>();
      for (byte[] row : e.getValue()) {
        rows.add(Bytes.toStringBinary(row));
        RegionInfo region = HnswIndexReader.regionOf(regions, row);
        if (region != null) {
          holders.add(region.getEncodedName());
        }
      }
      checks.stray.put(e.getKey(), holders);
      Map<String, Object> details = new LinkedHashMap<>();
      details.put("indexKey", e.getKey());
      details.put("cells", e.getValue().size());
      details.put("rows", new ArrayList<>(rows));
      details.put("regions", new ArrayList<>(holders));
      checks.findings.add(new Finding(
        Severity.ERROR, SCOPE_CATALOG, STRAY_ROW, e.getValue().size() + " cells of index key "
          + e.getKey() + " are not cells of an HNSW " + "index of the table; repair purges them",
        details));
    }
    for (byte[][] cell : catalog.keyless) {
      Map<String, Object> details = new LinkedHashMap<>();
      details.put("row", Bytes.toStringBinary(cell[0]));
      details.put("qualifier", Bytes.toStringBinary(cell[1]));
      checks.findings.add(new Finding(Severity.ERROR, SCOPE_CATALOG, STRAY_ROW,
        "Cell " + Bytes.toStringBinary(cell[1]) + " of row " + Bytes.toStringBinary(cell[0])
          + " names no index that repair can purge; repair does not delete it",
        details));
    }
    for (HnswSegment.Descriptor d : catalog.segments) {
      if (!Bytes.equals(d.definition, definition)) {
        rebuild(checks, regions, d.anchor);
        checks.findings.add(new Finding(
          Severity.ERROR, SCOPE_CATALOG, STRAY_ROW, "Segment " + Bytes.toStringBinary(d.identity)
            + " has another definition of the index; " + "the rebuild of its region retires it",
          segmentDetails(d)));
      }
    }
    for (byte[][] malformed : catalog.malformed) {
      rebuild(checks, regions, malformed[0]);
      Map<String, Object> details = new LinkedHashMap<>();
      details.put("row", Bytes.toStringBinary(malformed[0]));
      details.put("cells", Bytes.toStringBinary(malformed[1]));
      checks.findings.add(new Finding(Severity.ERROR, SCOPE_CATALOG, SEGMENT_MALFORMED,
        "Row " + Bytes.toStringBinary(malformed[0]) + " has segment cells "
          + Bytes.toStringBinary(malformed[1]) + " of the index that are not a well-formed "
          + "segment in its anchor row; the rebuild of its region deletes them",
        details));
    }

    for (RegionInfo region : regions) {
      byte[] start = region.getStartKey();
      byte[] end = region.getEndKey();
      List<HnswSegment.Descriptor> held = HnswIndexReader.held(segments, start, end);
      Map<String, Object> details = new LinkedHashMap<>();
      details.put("region", region.getEncodedName());
      if (active && !HnswIndexReader.covered(start, end, held)) {
        checks.rebuild.add(region.getEncodedName());
        checks.findings.add(new Finding(Severity.WARN, SCOPE_CATALOG, RANGE_UNCOVERED,
          "Region " + region.getEncodedName() + " has key ranges that its segments do not cover; "
            + "it reads them without the index until its build",
          details));
      }
      for (HnswSegment.Descriptor d : HnswIndexManager.currentSegments(held, start, end)) {
        if (!Bytes.equals(d.writer, region.getEncodedNameAsBytes())) {
          checks.rebuild.add(region.getEncodedName());
          Map<String, Object> writer = new LinkedHashMap<>(details);
          writer.put("writer", Bytes.toStringBinary(d.writer));
          checks.findings.add(new Finding(Severity.WARN, SCOPE_CATALOG, STACK_UNTRUSTED,
            "Region " + region.getEncodedName() + " searches segments that region "
              + Bytes.toStringBinary(d.writer) + " wrote; it rebuilds them",
            writer));
          break;
        }
      }
      for (HnswSegment.Descriptor d : HnswIndexManager.segmentsToRetire(held, start, end,
        definition)) {
        checks.rebuild.add(region.getEncodedName());
        checks.findings
          .add(
            new Finding(Severity.WARN, SCOPE_CATALOG, SEGMENT_SUPERSEDED,
              "Segment " + Bytes.toStringBinary(d.identity)
                + " is wholly covered by newer segments " + "but was not retired",
              segmentDetails(d)));
      }
    }

    for (HnswSegment.Descriptor d : segments) {
      List<Finding> findings = new ArrayList<>();
      checkSegment(context, reader, d, findings);
      if (findings.stream().anyMatch(HnswIndexFsckProvider::isCorruption)) {
        rebuild(checks, regions, d.anchor);
      }
      checks.findings.addAll(findings);
    }
    return checks;
  }

  // Adds the region that holds the row to the regions to rebuild
  private static void rebuild(Checks checks, List<RegionInfo> regions, byte[] row) {
    RegionInfo region = HnswIndexReader.regionOf(regions, row);
    if (region != null) {
      checks.rebuild.add(region.getEncodedName());
    }
  }

  private static boolean isCorruption(Finding finding) {
    return finding.getRule().equals(SEGMENT_CORRUPT) || finding.getRule().equals(SEGMENT_INVALID)
      || finding.getRule().equals(PAYLOAD_UNRESOLVED);
  }

  static Map<String, Object> segmentDetails(HnswSegment.Descriptor d) {
    Map<String, Object> details = new LinkedHashMap<>();
    details.put("segment", Bytes.toStringBinary(d.identity));
    details.put("anchor", Bytes.toStringBinary(d.anchor));
    details.put("time", d.time);
    if (d.isDelta()) {
      details.put("baseTime", d.baseTime);
    }
    return details;
  }

  /**
   * Checks one segment. The segment must open and decode, and its mapping and graph must be valid.
   * Only a valid segment gets the reachability and recall checks.
   */
  void checkSegment(IndexFsckContext context, HnswIndexReader reader, HnswSegment.Descriptor d,
    List<Finding> findings) throws Exception {
    HnswSegment segment = openForCheck(reader, d, findings);
    if (segment == null) {
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
          "Segment " + Bytes.toStringBinary(d.identity) + " is invalid: " + problems.get(0)
            + (problems.size() > 1 ? " and " + (problems.size() - 1) + " more" : ""),
          details));
        return;
      }
      checkQuality(reader, d, segment, findings);
    } finally {
      segment.close();
    }
  }

  /**
   * Opens a segment for a check. Returns null if the segment cannot be checked, and records a
   * finding where one applies. A segment that a server retired after the listing gets no finding. A
   * payload too large for this JVM gets a warning that repair does not act on. A payload that
   * refers to a missing MOB file is an error. A segment is corrupt only when its payload, if it has
   * one, reads and a second open still fails. A read failure is retried and then thrown, because it
   * does not show corruption.
   */
  private static HnswSegment openForCheck(HnswIndexReader reader, HnswSegment.Descriptor d,
    List<Finding> findings) throws Exception {
    Exception failure;
    try {
      return reader.open(d);
    } catch (HnswSegment.NotFoundException missing) {
      return missingCell(reader, d, findings);
    } catch (HnswIndexReader.PayloadTooLargeException e) {
      Map<String, Object> details = segmentDetails(d);
      details.put("error", e.getMessage());
      findings.add(new Finding(Severity.WARN, SCOPE_SEGMENTS, SEGMENT_UNCHECKED,
        "Segment " + Bytes.toStringBinary(d.identity) + " is not checked: " + e.getMessage(),
        details));
      return null;
    } catch (IOException | RuntimeException e) {
      failure = e;
    }
    if (Retry.isInterrupt(failure)) {
      // An interrupt does not show that the segment is corrupt
      Thread.currentThread().interrupt();
      throw failure;
    }
    byte[] payload = Retry.call("Read HNSW segment payload", () -> reader.readPayload(d));
    if (payload != null && payload.length == 0) {
      Map<String, Object> details = segmentDetails(d);
      details.put("error", String.valueOf(failure.getMessage()));
      findings.add(new Finding(
        Severity.ERROR, SCOPE_SEGMENTS, PAYLOAD_UNRESOLVED, "The payload of segment "
          + Bytes.toStringBinary(d.identity) + " references a MOB " + "file that does not exist",
        details));
      return null;
    }
    // The payload reads now, or the segment has none, so a failure of the second open is a failure
    // to decode the segment
    try {
      return reader.open(d);
    } catch (HnswSegment.NotFoundException missing) {
      return missingCell(reader, d, findings);
    } catch (IOException | RuntimeException e) {
      if (Retry.isInterrupt(e)) {
        Thread.currentThread().interrupt();
        throw e;
      }
      failure = e;
    }
    Map<String, Object> details = segmentDetails(d);
    details.put("error", String.valueOf(failure.getMessage()));
    findings.add(new Finding(Severity.ERROR, SCOPE_SEGMENTS, SEGMENT_CORRUPT,
      "Segment " + Bytes.toStringBinary(d.identity) + " cannot be decoded: " + failure.getMessage(),
      details));
    return null;
  }

  /**
   * Handles a segment without its payload cell, or a delta without its count cell. If a new listing
   * still returns the segment, no server retired it. The cell is then missing, and the method
   * records PAYLOAD_UNRESOLVED. The region leaves such a segment out and rebuilds. Returns null.
   */
  private static HnswSegment missingCell(HnswIndexReader reader, HnswSegment.Descriptor d,
    List<Finding> findings) throws IOException {
    for (HnswSegment.Descriptor listed : reader.listSegments()) {
      if (Bytes.equals(listed.identity, d.identity) && Bytes.equals(listed.anchor, d.anchor)) {
        findings.add(new Finding(Severity.ERROR, SCOPE_SEGMENTS, PAYLOAD_UNRESOLVED,
          "Segment " + Bytes.toStringBinary(d.identity) + " has no payload cell",
          segmentDetails(d)));
        break;
      }
    }
    return null;
  }

  /**
   * Adds a problem for each structural fault of a segment. The count must match the mapping. The
   * row keys must be strictly ascending. The row keys and the tombstones must be in the segment
   * range. The graph must have the index dimension and one node for each mapped row. It must also
   * have a top level entry node, valid neighbors within the degree limit, and finite inline
   * vectors. The neighbor check stops after more than 10 problems.
   */
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
   * Records the graph quality of a valid segment. The check finds the nodes that a search cannot
   * reach from the entry node. It also measures the recall of searches for a sample of rows of the
   * segment. The queries are the current data table vectors of the sample rows. Many unreachable
   * nodes or a low recall give a warning.
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
        .add(new Finding(unreachable > UNREACHABLE_WARN * size ? Severity.WARN : Severity.INFO,
          SCOPE_SEGMENTS, GRAPH_UNREACHABLE, unreachable + " of " + size + " nodes of segment "
            + Bytes.toStringBinary(d.identity) + " cannot be reached from the entry node",
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
        Bytes.toStringBinary(d.identity), recall, RECALL_K),
      details));
  }

  /**
   * Returns the nodes that a breadth first walk from the entry node reaches. The walk follows a
   * node at the level where it found the node and at all lower levels.
   */
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
   * Returns the fraction of the brute force nearest neighbors of the query that a graph search of
   * the segment finds.
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
   * Returns the {@code k} accepted nodes with the best scores, best first, from a linear scan of
   * the segment. Each result is an ordinal and a score. A segment with full vectors gives exact
   * scores. A quantized segment gives scores from its NVQ vectors.
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
