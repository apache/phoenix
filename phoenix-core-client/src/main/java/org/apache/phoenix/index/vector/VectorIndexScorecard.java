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
package org.apache.phoenix.index.vector;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;

/**
 * Vector index scorecard reconciliation and drift detection.
 * <p>
 * Manages posting list population and reassignment counters in {@code SYSTEM.VECTOR_CENTROID}.
 * Periodic reconciliation synchronizes inline maintenance statistics with exact index row counts.
 */
public final class VectorIndexScorecard {

  /** Maximum acceptable coefficient of variation for posting list sizes before flagging drift. */
  static final double SIZE_CV_THRESHOLD = ClusterSkewMetrics.SEVERE_SKEW_CV_THRESHOLD;
  /** Maximum acceptable fraction of empty posting lists before flagging drift. */
  static final double EMPTY_CENTROID_FRACTION_THRESHOLD = 0.25;
  /** Maximum acceptable reassignment rate per indexed vector between reconciliations. */
  static final double REASSIGN_RATE_THRESHOLD = 0.20;

  private VectorIndexScorecard() {
  }

  /** Encapsulates drift evaluation results and statistical indicators for a generation. */
  public static final class Assessment {
    private final boolean drifted;
    private final String reason;
    private final double skewRatio;
    private final double sizeCv;
    private final double emptyCentroidFraction;
    private final double reassignmentRate;
    private final long population;

    Assessment(boolean drifted, String reason, double skewRatio, double sizeCv,
      double emptyCentroidFraction, double reassignmentRate, long population) {
      this.drifted = drifted;
      this.reason = reason;
      this.skewRatio = skewRatio;
      this.sizeCv = sizeCv;
      this.emptyCentroidFraction = emptyCentroidFraction;
      this.reassignmentRate = reassignmentRate;
      this.population = population;
    }

    /** Returns true if any drift threshold was exceeded. */
    public boolean isDrifted() {
      return drifted;
    }

    /** Returns formatted summary of exceeded drift thresholds, or null if within limits. */
    public String getReason() {
      return reason;
    }

    /** Ratio of maximum posting list size to median list size. */
    public double getSkewRatio() {
      return skewRatio;
    }

    public double getSizeCv() {
      return sizeCv;
    }

    public double getEmptyCentroidFraction() {
      return emptyCentroidFraction;
    }

    public double getReassignmentRate() {
      return reassignmentRate;
    }

    public long getPopulation() {
      return population;
    }

    @Override
    public String toString() {
      return "Assessment{drifted=" + drifted + ", reason=" + reason + ", skewRatio=" + skewRatio
        + ", sizeCv=" + sizeCv + ", emptyCentroidFraction=" + emptyCentroidFraction
        + ", reassignmentRate=" + reassignmentRate + ", population=" + population + "}";
    }
  }

  /**
   * Test hook invoked after reconciliation reads the scorecard and before it writes corrections.
   */
  interface ReconcileHook {
    void loaded(String indexName, long generation) throws Exception;
  }

  private static volatile ReconcileHook reconcileHookForTesting;

  /** Injects test hook for interleaving scorecard writes with reconciliation. */
  static void setReconcileHookForTesting(ReconcileHook hook) {
    reconcileHookForTesting = hook;
  }

  /**
   * Counts live index rows grouped by centroid ID across the index table.
   * @param conn  connection without tenant scoping to aggregate across all tenants
   * @param index vector index table
   * @return map of centroid ID to live posting count
   */
  public static Map<Integer, Long> countPostings(Connection conn, PTable index)
    throws SQLException {
    String indexName = index.getName().getString();
    String centroidColumn = "\"" + MetaDataUtil.VECTOR_CENTROID_ID_COLUMN_NAME + "\"";
    Map<Integer, Long> populations = new HashMap<>();
    try (Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT " + centroidColumn + ", COUNT(*) FROM "
        + SchemaUtil.getEscapedFullTableName(indexName) + " GROUP BY " + centroidColumn)) {
      while (rs.next()) {
        populations.put(rs.getInt(1), rs.getLong(2));
      }
    }
    return populations;
  }

  /**
   * Reconciles scorecard statistics against physical index table row counts. Resets inline
   * reassignment counters and updates last reconciliation timestamp. Both are applied as
   * corrections relative to the counters read, so concurrent RegionServer flushes are kept.
   * @param conn connection without tenant scoping to aggregate across all tenants
   * @return reconciled scorecard rows with pre-reset reassignment metrics
   */
  public static List<ScorecardRow> reconcile(PhoenixConnection conn, PTable index, long generation)
    throws SQLException {
    String indexName = index.getName().getString();
    Map<Integer, Long> populations = countPostings(conn, index);
    List<ScorecardRow> before = CentroidManager.loadScorecard(conn, indexName, generation);
    List<ScorecardRow> reconciled = new ArrayList<>(before.size());
    // Corrections are relative to the values read, so RegionServer flushes committed after the
    // read are preserved rather than overwritten
    List<ScorecardRow> corrections = new ArrayList<>(before.size());
    for (ScorecardRow row : before) {
      long population = populations.getOrDefault(row.getCentroidId(), 0L);
      reconciled.add(new ScorecardRow(row.getCentroidId(), population, row.getReassignCount()));
      corrections.add(new ScorecardRow(row.getCentroidId(), population - row.getClusterSize(),
        -row.getReassignCount()));
    }
    ReconcileHook hook = reconcileHookForTesting;
    if (hook != null) {
      try {
        hook.loaded(indexName, generation);
      } catch (Exception e) {
        throw new SQLException(e);
      }
    }
    CentroidManager.adjustScorecard(conn, indexName, generation, corrections);
    CentroidManager.persistGenerationSummary(conn, indexName, generation, new GenerationSummary(
      null, null, null, null, null, EnvironmentEdgeManager.currentTimeMillis()));
    return reconciled;
  }

  /**
   * Evaluates scorecard metrics against configured drift thresholds (skew ratio, size CV, empty
   * fraction, and reassignment rate). Bypassed when total population is below minimum.
   */
  public static Assessment assess(List<ScorecardRow> rows, ReadOnlyProps props) {
    int k = rows.size();
    if (k == 0) {
      return new Assessment(false, null, 0, 0, 0, 0, 0);
    }
    long[] sizes = new long[k];
    long population = 0;
    long reassigned = 0;
    int empty = 0;
    for (int i = 0; i < k; i++) {
      ScorecardRow row = rows.get(i);
      sizes[i] = row.getClusterSize();
      population += sizes[i];
      reassigned += row.getReassignCount();
      if (sizes[i] == 0) {
        empty++;
      }
    }
    Arrays.sort(sizes);
    long max = sizes[k - 1];
    double median = k % 2 == 1 ? sizes[k / 2] : (sizes[k / 2 - 1] + sizes[k / 2]) / 2.0;
    // Skew relative to median cluster size.
    // Non-zero maximum with zero median indicates unbounded skew
    double skewRatio = median > 0 ? max / median : max > 0 ? Double.POSITIVE_INFINITY : 0;
    double mean = (double) population / k;
    double sumSquares = 0;
    for (long size : sizes) {
      sumSquares += (size - mean) * (size - mean);
    }
    double sizeCv = mean > 0 ? Math.sqrt(sumSquares / k) / mean : 0;
    double emptyFraction = (double) empty / k;
    double reassignmentRate = population > 0 ? (double) reassigned / population : 0;

    long minPopulation = props.getLong(QueryServices.VECTOR_DRIFT_MIN_POPULATION_ATTRIB,
      QueryServicesOptions.DEFAULT_VECTOR_DRIFT_MIN_POPULATION);
    if (population < minPopulation) {
      return new Assessment(false, null, skewRatio, sizeCv, emptyFraction, reassignmentRate,
        population);
    }
    double skewThreshold =
      Double.parseDouble(props.get(QueryServices.VECTOR_DRIFT_SKEW_RATIO_THRESHOLD_ATTRIB,
        Double.toString(QueryServicesOptions.DEFAULT_VECTOR_DRIFT_SKEW_RATIO_THRESHOLD)));
    List<String> reasons = new ArrayList<>(4);
    addIfExceeded(reasons, "SKEW_RATIO_EXCEEDED", skewRatio, skewThreshold);
    addIfExceeded(reasons, "SIZE_CV_EXCEEDED", sizeCv, SIZE_CV_THRESHOLD);
    addIfExceeded(reasons, "EMPTY_CENTROID_FRACTION_EXCEEDED", emptyFraction,
      EMPTY_CENTROID_FRACTION_THRESHOLD);
    addIfExceeded(reasons, "REASSIGN_RATE_EXCEEDED", reassignmentRate, REASSIGN_RATE_THRESHOLD);
    return new Assessment(!reasons.isEmpty(), reasons.isEmpty() ? null : String.join("; ", reasons),
      skewRatio, sizeCv, emptyFraction, reassignmentRate, population);
  }

  private static void addIfExceeded(List<String> reasons, String name, double value,
    double threshold) {
    if (value > threshold) {
      reasons.add(name + ": " + (Double.isInfinite(value) ? "unbounded" : format(value)) + " > "
        + format(threshold));
    }
  }

  private static String format(double value) {
    return String.format("%.2f", value);
  }
}
