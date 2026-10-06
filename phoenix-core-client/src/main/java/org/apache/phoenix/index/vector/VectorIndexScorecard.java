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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CENTROID_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CLUSTER_SIZE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.GENERATION_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LAST_SCORECARD_UPDATE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.REASSIGN_COUNT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SCORECARD_FENCE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_VECTOR_CENTROID_NAME;

import java.sql.Connection;
import java.sql.PreparedStatement;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reconciles the vector index scorecard and finds centroid drift.
 * <p>
 * The scorecard in {@code SYSTEM.VECTOR_CENTROID} keeps a population counter and a reassignment
 * counter for each posting list. Region servers change these counters inline during index
 * maintenance, so they can become approximate. A recount sets each population counter to the index
 * row count and subtracts the reassignment counts that it read.
 * <p>
 * A region server buffers its deltas before it flushes them. The recount count can contain a write
 * whose delta is still in a buffer. Thus the recount also sets a time fence on each centroid. A
 * population delta applies only if its oldest write is not older than the fence. The fence is the
 * server time after the count, plus the clock skew bound and the completion budget. Thus no write
 * in the count is newer than the fence, and no delta counts two times.
 * <p>
 * The recount loses each write that commits after the count and before the fence. A lost addition
 * gives an undercount, and a lost removal gives an overcount. The next recount repairs both errors.
 */
public final class VectorIndexScorecard {

  private static final Logger LOGGER = LoggerFactory.getLogger(VectorIndexScorecard.class);

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
   * Test hook. The reconcile calls it after it reads the scorecard and before it writes the counts.
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
   * Counts the live index rows of each centroid ID in the full index table.
   * @param conn  a connection without a tenant, so that the count includes all tenants
   * @param index the vector index table
   * @return a map from the centroid ID to the count of live postings
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
   * Makes the scorecard of a generation agree with the actual index row counts. The method also
   * resets the inline reassignment counters and records the time of the reconciliation. It writes
   * each population as an absolute count with a time fence. A population delta that a region server
   * flushes later applies only if it is not older than the fence. It subtracts the reassignment
   * counts that it read, so concurrent reassignment increments stay in the scorecard. Every
   * scorecard recount uses this method.
   * @param conn       a connection without a tenant, so that the counts include all tenants
   * @param index      the vector index table
   * @param generation the centroid generation of the scorecard
   * @return the reconciled scorecard rows, with the reassignment counts from before the reset
   */
  public static List<ScorecardRow> reconcile(PhoenixConnection conn, PTable index, long generation)
    throws SQLException {
    String indexName = index.getName().getString();
    // Count at the latest time. A read at an earlier time can miss cells that a flush or a
    // compaction removes.
    Map<Integer, Long> populations = countPostings(conn, index);
    // Take the server time after the count. The client clock is not related to the region servers.
    long serverTime = readServerTime(conn, indexName, generation);
    long started = EnvironmentEdgeManager.currentTimeMillis();
    ReadOnlyProps props = conn.getQueryServices().getProps();
    long fence = fence(serverTime, props);
    List<ScorecardRow> before = CentroidManager.loadScorecard(conn, indexName, generation);
    List<ScorecardRow> reconciled = new ArrayList<>(before.size());
    for (ScorecardRow row : before) {
      long population = populations.getOrDefault(row.getCentroidId(), 0L);
      reconciled.add(new ScorecardRow(row.getCentroidId(), population, row.getReassignCount()));
    }
    ReconcileHook hook = reconcileHookForTesting;
    if (hook != null) {
      try {
        hook.loaded(indexName, generation);
      } catch (Exception e) {
        throw new SQLException(e);
      }
    }
    writeCounts(conn, indexName, generation, reconciled, fence);
    long elapsed = EnvironmentEdgeManager.currentTimeMillis() - started;
    long budget = completionBudget(props);
    if (elapsed > budget) {
      LOGGER.warn(
        "Scorecard recount of {} generation {} took {} ms to write its counts, more than the"
          + " budget of {} ms. Deltas that flushed in this time are lost until the next recount.",
        indexName, generation, elapsed, budget);
    }
    CentroidManager.persistGenerationSummary(conn, indexName, generation, new GenerationSummary(
      null, null, null, null, null, EnvironmentEdgeManager.currentTimeMillis()));
    return reconciled;
  }

  /**
   * Returns the recount fence for a server time. The fence adds the clock skew bound between region
   * servers and the completion budget of the recount. Thus a write with a batch time at or after
   * the fence commits after the count, and its delta flushes after the recount writes its counts.
   */
  static long fence(long serverTime, ReadOnlyProps props) {
    return addNonNegative(serverTime,
      props.getLong(QueryServices.VECTOR_SCORECARD_CLOCK_SKEW_MS_ATTRIB,
        QueryServicesOptions.DEFAULT_VECTOR_SCORECARD_CLOCK_SKEW_MS),
      completionBudget(props));
  }

  private static long completionBudget(ReadOnlyProps props) {
    return Math.max(0, props.getLong(QueryServices.VECTOR_SCORECARD_COMPLETION_BUDGET_MS_ATTRIB,
      QueryServicesOptions.DEFAULT_VECTOR_SCORECARD_COMPLETION_BUDGET_MS));
  }

  /**
   * Returns the sum of a time and some durations. A negative value counts as zero. The result is
   * {@link Long#MAX_VALUE} on overflow.
   */
  static long addNonNegative(long time, long... durations) {
    long result = Math.max(0, time);
    for (long duration : durations) {
      long value = Math.max(0, duration);
      if (value > Long.MAX_VALUE - result) {
        return Long.MAX_VALUE;
      }
      result += value;
    }
    return result;
  }

  /**
   * Writes the summary row of a generation and returns the time that the server gave the write. The
   * write does not change a value, and it never creates the row. Thus a recount of a deleted
   * generation fails and does not bring back its summary row.
   */
  private static long readServerTime(Connection conn, String indexName, long generation)
    throws SQLException {
    try (PreparedStatement ps =
      conn.prepareStatement("UPSERT INTO " + SYSTEM_VECTOR_CENTROID_NAME + " (" + INDEX_NAME + ", "
        + GENERATION_ID + ", " + CENTROID_ID + ") VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE_ONLY "
        + LAST_SCORECARD_UPDATE + " = " + LAST_SCORECARD_UPDATE)) {
      ps.setString(1, indexName);
      ps.setLong(2, generation);
      ps.setInt(3, CentroidManager.SENTINEL_CENTROID_ID);
      ps.executeUpdate();
    }
    conn.commit();
    Long serverTime = CentroidManager.summaryWriteTime(conn, indexName, generation);
    if (serverTime == null) {
      throw new SQLException("The summary row of " + indexName + " generation " + generation
        + " does not exist, so the generation is deleted");
    }
    return serverTime;
  }

  /**
   * Writes the absolute population and the reassignment reset of each centroid, and commits. A
   * centroid takes the new count and fence only if its fence is older. Thus the fence only moves
   * forward, and a late recount cannot replace the count of a later recount. The update never
   * creates a row.
   */
  private static void writeCounts(Connection conn, String indexName, long generation,
    List<ScorecardRow> counts, long fence) throws SQLException {
    String older = "COALESCE(" + SCORECARD_FENCE + ", 0) < ?";
    String sql = "UPSERT INTO " + SYSTEM_VECTOR_CENTROID_NAME + " (" + INDEX_NAME + ", "
      + GENERATION_ID + ", " + CENTROID_ID + ") VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE_ONLY "
      + CLUSTER_SIZE + " = CASE WHEN " + older + " THEN ? ELSE " + CLUSTER_SIZE + " END, "
      + SCORECARD_FENCE + " = CASE WHEN " + older + " THEN ? ELSE " + SCORECARD_FENCE + " END, "
      + REASSIGN_COUNT + " = COALESCE(" + REASSIGN_COUNT + ", 0) - ?";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (ScorecardRow row : counts) {
        ps.setString(1, indexName);
        ps.setLong(2, generation);
        ps.setInt(3, row.getCentroidId());
        ps.setLong(4, fence);
        ps.setLong(5, row.getClusterSize());
        ps.setLong(6, fence);
        ps.setLong(7, fence);
        ps.setLong(8, row.getReassignCount());
        ps.executeUpdate();
      }
    }
    conn.commit();
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
