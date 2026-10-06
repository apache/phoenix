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
 * Reconciles the vector index scorecard and finds centroid drift.
 * <p>
 * The scorecard in {@code SYSTEM.VECTOR_CENTROID} keeps a population counter and a reassignment
 * counter for each posting list. Region servers change these counters inline during index
 * maintenance, so they can become approximate. A periodic reconcile corrects each population
 * counter to the exact index row count and subtracts the reassignment counts that it read. It
 * applies both as deltas, so increments that a region server flushes after the read stay in the
 * counters.
 */
public final class VectorIndexScorecard {

  /** Largest coefficient of variation of posting list sizes that is not drift. */
  static final double SIZE_CV_THRESHOLD = ClusterSkewMetrics.SEVERE_SKEW_CV_THRESHOLD;
  /** Largest fraction of empty posting lists that is not drift. */
  static final double EMPTY_CENTROID_FRACTION_THRESHOLD = 0.25;
  /** Largest number of reassignments per indexed vector between reconciles that is not drift. */
  static final double REASSIGN_RATE_THRESHOLD = 0.20;

  private VectorIndexScorecard() {
  }

  /** The drift decision and the metrics that it uses, for one generation. */
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

    /**
     * Returns true if the population is not less than the drift minimum and the metrics exceed one
     * or more drift thresholds.
     */
    public boolean isDrifted() {
      return drifted;
    }

    /**
     * Returns the exceeded drift thresholds and their values, separated by semicolons, or null if
     * the assessment found no drift.
     */
    public String getReason() {
      return reason;
    }

    /** Returns the size of the largest posting list divided by the median size. */
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
   * Test hook. The reconcile calls it after it reads the scorecard and before it writes the
   * corrections.
   */
  interface ReconcileHook {
    void loaded(String indexName, long generation) throws Exception;
  }

  private static volatile ReconcileHook reconcileHookForTesting;

  /** Sets the test hook that lets a test put scorecard writes between the read and the write. */
  static void setReconcileHookForTesting(ReconcileHook hook) {
    reconcileHookForTesting = hook;
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
    String centroidColumn = "\"" + MetaDataUtil.VECTOR_CENTROID_ID_COLUMN_NAME + "\"";
    Map<Integer, Long> populations = new HashMap<>();
    try (Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT " + centroidColumn + ", COUNT(*) FROM "
        + SchemaUtil.getEscapedFullTableName(indexName) + " GROUP BY " + centroidColumn)) {
      while (rs.next()) {
        populations.put(rs.getInt(1), rs.getLong(2));
      }
    }
    List<ScorecardRow> before = CentroidManager.loadScorecard(conn, indexName, generation);
    List<ScorecardRow> reconciled = new ArrayList<>(before.size());
    // Each correction is a delta from the values read. Thus a region server flush that commits
    // after the read stays in the counters and the reconcile does not overwrite it.
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
   * Compares the scorecard of one generation with the drift thresholds. The skew ratio threshold
   * comes from configuration. The size CV, empty fraction and reassignment rate thresholds are
   * fixed. If the total population is less than the configured minimum, the result is never
   * drifted, but it still contains the computed metrics.
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
    // Skew is the largest posting list relative to the median posting list.
    // If the median is zero and the largest list is not empty, the skew has no limit.
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
