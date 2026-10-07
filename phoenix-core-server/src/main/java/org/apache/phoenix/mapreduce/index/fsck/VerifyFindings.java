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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.Cell;
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

/** Translates IndexTool verification counters and historical results into structured findings. */
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
   * Extracts verification findings from IndexTool job counters, inspecting either pre-repair
   * ({@code -v ONLY}) or post-repair ({@code -v AFTER}) phase results.
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

  /** Returns the count of deleted orphan index rows from IndexTool job counters. */
  public static long deletedOrphans(Counters counters) {
    return get(counters, PhoenixIndexToolJobCounters.BEFORE_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT);
  }

  /** Generates findings for a verification phase from the specified source table. */
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
   * Retrieves and summarizes historical verification results from
   * {@code PHOENIX_INDEX_TOOL_RESULT}, returning null if no previous runs exist.
   */
  public static Finding lastVerification(Connection conn, PTable index)
    throws SQLException, IOException {
    byte[] indexName = index.getPhysicalName().getBytes();
    Map<Long, IndexToolVerificationResult> runs = new LinkedHashMap<>();
    try (
      Table table = conn.unwrap(PhoenixConnection.class).getQueryServices()
        .getTable(IndexVerificationResultRepository.getResultTableNameBytes());
      ResultScanner scanner = table.getScanner(new Scan())) {
      for (Result result : scanner) {
        byte[][] parts = ByteUtil.splitArrayBySeparator(result.getRow(),
          IndexVerificationResultRepository.ROW_KEY_SEPARATOR_BYTE[0]);
        if (parts.length < 2 || !Bytes.equals(parts[1], indexName)) {
          continue;
        }
        long ts = Long.parseLong(Bytes.toString(parts[0]));
        IndexToolVerificationResult run =
          runs.computeIfAbsent(ts, t -> new IndexToolVerificationResult(t));
        for (Cell cell : result.rawCells()) {
          run.update(cell);
        }
      }
    } catch (org.apache.hadoop.hbase.TableNotFoundException e) {
      return null;
    }
    if (runs.isEmpty()) {
      return null;
    }
    long ts = runs.keySet().stream().max(Long::compare).get();
    IndexToolVerificationResult run = runs.get(ts);
    PhaseResult before = run.getBefore();
    PhaseResult after = run.getAfter();
    long failures = 0;
    for (PhaseResult phase : Arrays.asList(before, after)) {
      failures += phase.getMissingIndexRowCount() + phase.getInvalidIndexRowCount()
        + phase.getExtraVerifiedIndexRowCount();
    }
    Map<String, Object> details = new LinkedHashMap<>();
    details.put("scanMaxTs", ts);
    details.put("ageMs", EnvironmentEdgeManager.currentTimeMillis() - ts);
    details.put("before", counts(before));
    details.put("after", counts(after));
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
