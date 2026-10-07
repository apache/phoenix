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
package org.apache.phoenix.mapreduce.index.fsck;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.phoenix.coprocessor.IndexToolVerificationResult;
import org.apache.phoenix.coprocessor.IndexToolVerificationResult.PhaseResult;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository;
import org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;

/** Converts IndexTool verification counters and stored verification results into findings. */
public final class VerifyFindings {
  public static final String SCOPE = "ROWS";

  public static final String MISSING = "ROW_MISSING";
  public static final String INVALID = "ROW_INVALID";
  public static final String ORPHAN_VERIFIED = "ROW_ORPHAN_VERIFIED";
  public static final String ORPHAN_UNVERIFIED = "ROW_ORPHAN_UNVERIFIED";
  public static final String UNVERIFIED = "ROW_UNVERIFIED";
  public static final String OLD_DESIGN = "ROW_OLD_DESIGN";
  public static final String UNKNOWN = "ROW_UNKNOWN";
  public static final String BEYOND_LOOKBACK = "ROW_BEYOND_MAX_LOOKBACK";
  public static final String EXPIRED = "ROW_EXPIRED";
  public static final String LAST_VERIFY = "LAST_VERIFY";

  private VerifyFindings() {
  }

  /**
   * Converts the IndexTool job counters of one verification phase into findings. If
   * {@code afterRepair} is true, the method uses the after phase of a repair job ({@code -v AFTER}
   * or {@code -v BOTH}). If it is false, the method uses the before phase of a verify only job
   * ({@code -v ONLY}). The {@code fromIndex} flag tells whether the job verified from the index
   * table or from the data table.
   */
  public static List<Finding> fromCounters(Counters counters, boolean fromIndex,
    boolean afterRepair) {
    PhaseResult phase = new PhaseResult();
    if (afterRepair) {
      phase.setValidIndexRowCount(
        get(counters, PhoenixIndexToolJobCounters.AFTER_REBUILD_VALID_INDEX_ROW_COUNT));
      phase.setMissingIndexRowCount(
        get(counters, PhoenixIndexToolJobCounters.AFTER_REBUILD_MISSING_INDEX_ROW_COUNT));
      phase.setInvalidIndexRowCount(
        get(counters, PhoenixIndexToolJobCounters.AFTER_REBUILD_INVALID_INDEX_ROW_COUNT));
      phase.setExpiredIndexRowCount(
        get(counters, PhoenixIndexToolJobCounters.AFTER_REBUILD_EXPIRED_INDEX_ROW_COUNT));
      phase.setBeyondMaxLookBackMissingIndexRowCount(get(counters,
        PhoenixIndexToolJobCounters.AFTER_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT));
      phase.setBeyondMaxLookBackInvalidIndexRowCount(get(counters,
        PhoenixIndexToolJobCounters.AFTER_REBUILD_BEYOND_MAXLOOKBACK_INVALID_INDEX_ROW_COUNT));
      phase.setExtraVerifiedIndexRowCount(
        get(counters, PhoenixIndexToolJobCounters.AFTER_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT));
      phase.setExtraUnverifiedIndexRowCount(
        get(counters, PhoenixIndexToolJobCounters.AFTER_REPAIR_EXTRA_UNVERIFIED_INDEX_ROW_COUNT));
    } else {
      phase.setValidIndexRowCount(
        get(counters, PhoenixIndexToolJobCounters.BEFORE_REBUILD_VALID_INDEX_ROW_COUNT));
      phase.setMissingIndexRowCount(
        get(counters, PhoenixIndexToolJobCounters.BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT));
      phase.setInvalidIndexRowCount(
        get(counters, PhoenixIndexToolJobCounters.BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT));
      phase.setExpiredIndexRowCount(
        get(counters, PhoenixIndexToolJobCounters.BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT));
      phase.setBeyondMaxLookBackMissingIndexRowCount(get(counters,
        PhoenixIndexToolJobCounters.BEFORE_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT));
      phase.setBeyondMaxLookBackInvalidIndexRowCount(get(counters,
        PhoenixIndexToolJobCounters.BEFORE_REBUILD_BEYOND_MAXLOOKBACK_INVALID_INDEX_ROW_COUNT));
      phase.setUnverifiedIndexRowCount(
        get(counters, PhoenixIndexToolJobCounters.BEFORE_REBUILD_UNVERIFIED_INDEX_ROW_COUNT));
      phase.setOldIndexRowCount(
        get(counters, PhoenixIndexToolJobCounters.BEFORE_REBUILD_OLD_INDEX_ROW_COUNT));
      phase.setUnknownIndexRowCount(
        get(counters, PhoenixIndexToolJobCounters.BEFORE_REBUILD_UNKNOWN_INDEX_ROW_COUNT));
      phase.setExtraVerifiedIndexRowCount(
        get(counters, PhoenixIndexToolJobCounters.BEFORE_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT));
      phase.setExtraUnverifiedIndexRowCount(
        get(counters, PhoenixIndexToolJobCounters.BEFORE_REPAIR_EXTRA_UNVERIFIED_INDEX_ROW_COUNT));
    }
    return fromPhase(phase, fromIndex ? "index" : "data");
  }

  /**
   * Returns the number of verified orphan index rows that an IndexTool job with {@code -do} found
   * before repair, and so deleted.
   */
  public static long deletedOrphans(Counters counters) {
    return get(counters, PhoenixIndexToolJobCounters.BEFORE_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT);
  }

  /**
   * Converts the row counts of one verification phase into findings, one for each count that is not
   * zero. The source is the table that the phase verified from, data or index.
   */
  public static List<Finding> fromPhase(PhaseResult phase, String source) {
    List<Finding> findings = new ArrayList<>();
    add(findings, source, phase.getMissingIndexRowCount(), Severity.ERROR, MISSING,
      "index rows missing");
    add(findings, source, phase.getInvalidIndexRowCount(), Severity.ERROR, INVALID,
      "index rows that differ from their data rows");
    add(findings, source, phase.getExtraVerifiedIndexRowCount(), Severity.ERROR, ORPHAN_VERIFIED,
      "verified orphan index rows, which no data row expects");
    add(findings, source, phase.getExtraUnverifiedIndexRowCount(), Severity.INFO, ORPHAN_UNVERIFIED,
      "unverified orphan index rows, left to read repair");
    add(findings, source, phase.getUnverifiedIndexRowCount(), Severity.INFO, UNVERIFIED,
      "unverified index rows, left to read repair");
    add(findings, source, phase.getOldIndexRowCount(), Severity.INFO, OLD_DESIGN,
      "index rows of the old design");
    add(findings, source, phase.getUnknownIndexRowCount(), Severity.INFO, UNKNOWN,
      "index rows with no empty column");
    add(findings, source,
      phase.getBeyondMaxLookBackMissingIndexRowCount()
        + phase.getBeyondMaxLookBackInvalidIndexRowCount(),
      Severity.INFO, BEYOND_LOOKBACK,
      "missing or invalid index rows beyond the max lookback age, which cannot be repaired");
    add(findings, source, phase.getExpiredIndexRowCount(), Severity.INFO, EXPIRED,
      "expired index rows");
    return findings;
  }

  /**
   * Summarizes the stored results of earlier verifications of the index in
   * {@code PHOENIX_INDEX_TOOL_RESULT}. Returns null if the index has no stored result, or if the
   * result table does not exist.
   */
  public static Finding lastVerification(Connection conn, PTable index)
    throws SQLException, IOException {
    try (
      Table table = conn.unwrap(PhoenixConnection.class).getQueryServices()
        .getTable(IndexVerificationResultRepository.getResultTableNameBytes());
      ResultScanner scanner = table.getScanner(new Scan())) {
      return lastVerification(scanner, index.getPhysicalName().getBytes());
    } catch (org.apache.hadoop.hbase.TableNotFoundException e) {
      return null;
    }
  }

  /**
   * Summarizes the latest run from the data table and the latest run from the index table. A run
   * from the data table finds missing and invalid rows, and a run from the index table finds orphan
   * rows. Because each source finds different rows, the latest run alone can miss what the first
   * pass of a verification found. A result row names the region that verified it. For a run from
   * the index table, this region belongs to the index table.
   * <p>
   * A run that verified after repair ({@code -v AFTER} or {@code -v BOTH}) writes the after phase
   * columns, and its after phase shows the rows that remain. The summary uses the before phase only
   * for a verify only run.
   */
  static Finding lastVerification(Iterable<Result> results, byte[] indexName) {
    byte[] indexRegionPrefix = Bytes.add(indexName, new byte[] { (byte) HConstants.DELIMITER });
    Map<String, TreeMap<Long, IndexToolVerificationResult>> runs = new LinkedHashMap<>();
    Set<IndexToolVerificationResult> repaired = Collections.newSetFromMap(new IdentityHashMap<>());
    for (Result result : results) {
      byte[][] parts = ByteUtil.splitArrayBySeparator(result.getRow(),
        IndexVerificationResultRepository.ROW_KEY_SEPARATOR_BYTE[0]);
      if (parts.length < 3 || !Bytes.equals(parts[1], indexName)) {
        continue;
      }
      String source = Bytes.startsWith(parts[2], indexRegionPrefix) ? "index" : "data";
      long ts = Long.parseLong(Bytes.toString(parts[0]));
      IndexToolVerificationResult run = runs.computeIfAbsent(source, s -> new TreeMap<>())
        .computeIfAbsent(ts, t -> new IndexToolVerificationResult(t));
      for (Cell cell : result.rawCells()) {
        run.update(cell);
        if (
          CellUtil.matchingQualifier(cell,
            IndexVerificationResultRepository.AFTER_REBUILD_VALID_INDEX_ROW_COUNT_BYTES)
        ) {
          repaired.add(run);
        }
      }
    }
    if (runs.isEmpty()) {
      return null;
    }
    long failures = 0;
    Map<String, Object> details = new LinkedHashMap<>();
    for (String source : Arrays.asList("data", "index")) {
      TreeMap<Long, IndexToolVerificationResult> sourceRuns = runs.get(source);
      if (sourceRuns == null) {
        continue;
      }
      long ts = sourceRuns.lastKey();
      IndexToolVerificationResult last = sourceRuns.get(ts);
      PhaseResult before = last.getBefore();
      PhaseResult after = last.getAfter();
      PhaseResult remaining = repaired.contains(last) ? after : before;
      failures += remaining.getMissingIndexRowCount() + remaining.getInvalidIndexRowCount()
        + remaining.getExtraVerifiedIndexRowCount();
      Map<String, Object> run = new LinkedHashMap<>();
      run.put("scanMaxTs", ts);
      run.put("ageMs", EnvironmentEdgeManager.currentTimeMillis() - ts);
      run.put("before", counts(before));
      run.put("after", counts(after));
      details.put(source, run);
    }
    return new Finding(failures > 0 ? Severity.WARN : Severity.INFO, SCOPE, LAST_VERIFY,
      failures > 0
        ? "The last verification found " + failures + " missing, invalid, or orphan rows"
        : "The last verification found no missing, invalid, or orphan rows",
      details);
  }

  private static Map<String, Long> counts(PhaseResult phase) {
    Map<String, Long> counts = new LinkedHashMap<>();
    counts.put("valid", phase.getValidIndexRowCount());
    counts.put("missing", phase.getMissingIndexRowCount());
    counts.put("invalid", phase.getInvalidIndexRowCount());
    counts.put("orphanVerified", phase.getExtraVerifiedIndexRowCount());
    counts.put("orphanUnverified", phase.getExtraUnverifiedIndexRowCount());
    return counts;
  }

  private static void add(List<Finding> findings, String source, long count, Severity severity,
    String rule, String what) {
    if (count > 0) {
      Map<String, Object> details = new LinkedHashMap<>();
      details.put("count", count);
      details.put("source", source);
      findings.add(new Finding(severity, SCOPE, rule,
        count + " " + what + ", verifying from the " + source + " table", details));
    }
  }

  private static long get(Counters counters, PhoenixIndexToolJobCounters counter) {
    return counters.findCounter(counter).getValue();
  }
}
