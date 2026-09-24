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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.phoenix.hbase.index.metrics.MetricsIndexerSourceFactory;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.SchemaUtil;

/** Manages scorecard reconciliation and drift evaluation for vector indexes. */
public class VectorIndexScorecard {

  /** Default non-configurable drift thresholds. */
  static final double SIZE_CV_THRESHOLD = ClusterSkewMetrics.SEVERE_SKEW_CV_THRESHOLD;
  static final double EMPTY_CENTROID_FRACTION_THRESHOLD = 0.25;
  static final double REASSIGN_RATE_THRESHOLD = 0.20;

  /** Holds the results of evaluating an index generation against configured drift thresholds. */
  public static class DriftEvaluationResult {
    private final boolean shouldRebuild;
    private final String triggerReason;
    private final double skewRatio;
    private final double sizeCv;
    private final double emptyCentroidFraction;
    private final double reassignmentRate;
    private final long totalClusterSize;
    private final long totalReassignCount;
    private final int centroidCount;

    public DriftEvaluationResult(boolean shouldRebuild, String triggerReason, double skewRatio,
      double sizeCv, double emptyCentroidFraction, double reassignmentRate, long totalClusterSize,
      long totalReassignCount, int centroidCount) {
      this.shouldRebuild = shouldRebuild;
      this.triggerReason = triggerReason;
      this.skewRatio = skewRatio;
      this.sizeCv = sizeCv;
      this.emptyCentroidFraction = emptyCentroidFraction;
      this.reassignmentRate = reassignmentRate;
      this.totalClusterSize = totalClusterSize;
      this.totalReassignCount = totalReassignCount;
      this.centroidCount = centroidCount;
    }

    public boolean shouldRebuild() {
      return shouldRebuild;
    }

    public String getTriggerReason() {
      return triggerReason;
    }

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

    public long getTotalClusterSize() {
      return totalClusterSize;
    }

    public long getTotalReassignCount() {
      return totalReassignCount;
    }

    public int getCentroidCount() {
      return centroidCount;
    }

    @Override
    public String toString() {
      return "DriftEvaluationResult{" + "shouldRebuild=" + shouldRebuild + ", triggerReason='"
        + triggerReason + '\'' + ", skewRatio=" + skewRatio + ", sizeCv=" + sizeCv
        + ", emptyCentroidFraction=" + emptyCentroidFraction + ", reassignmentRate="
        + reassignmentRate + ", totalClusterSize=" + totalClusterSize + ", totalReassignCount="
        + totalReassignCount + ", centroidCount=" + centroidCount + '}';
    }
  }

  /**
   * Reconciles physical index row counts with scorecard cluster sizes, resets reassignment
   * counters, and updates the scorecard timestamp.
   * @param conn       Phoenix connection
   * @param indexName  index table name
   * @param generation centroid generation identifier
   * @return reconciled scorecard rows with pre-reset reassignment counts for drift evaluation
   * @throws SQLException if a database access error occurs
   */
  public static List<ScorecardRow> reconcile(Connection conn, String indexName, long generation)
    throws SQLException {
    if (conn == null) {
      throw new IllegalArgumentException("connection must not be null");
    }
    if (indexName == null || indexName.trim().isEmpty()) {
      throw new IllegalArgumentException("indexName must not be null or empty");
    }
    if (generation < 0) {
      throw new IllegalArgumentException("generation must be non-negative");
    }

    long startTime = System.currentTimeMillis();
    String normalizedIndexName = SchemaUtil.normalizeFullTableName(indexName);
    String escapedIndexTable = SchemaUtil.getEscapedFullTableName(normalizedIndexName);

    String centroidColName =
      IndexUtil.getIndexColumnName(null, PhoenixDatabaseMetaData.CENTROID_ID);
    String countSql = "SELECT \"" + centroidColName + "\", COUNT(*) FROM " + escapedIndexTable
      + " GROUP BY \"" + centroidColName + "\"";

    Map<Integer, Long> physicalCounts = new HashMap<>();
    try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(countSql)) {
      while (rs.next()) {
        int centroidId = rs.getInt(1);
        long count = rs.getLong(2);
        physicalCounts.put(centroidId, count);
      }
    }

    // Reconcile across the union of physical centroids and existing scorecard entries,
    // defaulting unpopulated centroids to zero.
    List<ScorecardRow> existingScorecard =
      CentroidManager.loadScorecard(conn, normalizedIndexName, generation);

    Set<Integer> centroidIds = new TreeSet<>(physicalCounts.keySet());
    for (ScorecardRow row : existingScorecard) {
      centroidIds.add(row.getCentroidId());
    }

    Map<Integer, Long> existingReassignCounts = new HashMap<>();
    for (ScorecardRow row : existingScorecard) {
      long reassign = row.getReassignCount() != null ? row.getReassignCount() : 0L;
      existingReassignCounts.put(row.getCentroidId(), reassign);
    }

    long now = System.currentTimeMillis();
    List<ScorecardRow> preResetRows = new ArrayList<>(centroidIds.size());
    List<ScorecardRow> persistRows = new ArrayList<>(centroidIds.size());
    for (int centroidId : centroidIds) {
      long clusterSize = physicalCounts.getOrDefault(centroidId, 0L);
      long preResetReassign = existingReassignCounts.getOrDefault(centroidId, 0L);
      preResetRows.add(new ScorecardRow(normalizedIndexName, generation, centroidId, clusterSize,
        preResetReassign, now));
      persistRows
        .add(new ScorecardRow(normalizedIndexName, generation, centroidId, clusterSize, 0L, now));
    }

    CentroidManager.persistScorecard(conn, persistRows);
    CentroidManager.persistGenerationSummary(conn, normalizedIndexName, generation, null, null,
      null, null, now);

    long duration = System.currentTimeMillis() - startTime;
    MetricsIndexerSourceFactory.getInstance().getMetricsVectorIndexSource()
      .updateVectorScorecardReconcileTime(normalizedIndexName, duration);

    return preResetRows;
  }

  /**
   * Evaluates active generation scorecard rows against configured drift thresholds.
   * @param conn       Phoenix connection
   * @param indexName  index table name
   * @param generation centroid generation identifier
   * @param config     Phoenix/Hadoop configuration
   * @return evaluation result indicating whether rebuild should occur and trigger reason
   * @throws SQLException if a database access error occurs
   */
  public static DriftEvaluationResult evaluate(Connection conn, String indexName, long generation,
    Configuration config) throws SQLException {
    if (conn == null) {
      throw new IllegalArgumentException("connection must not be null");
    }
    if (indexName == null || indexName.trim().isEmpty()) {
      throw new IllegalArgumentException("indexName must not be null or empty");
    }
    if (generation < 0) {
      throw new IllegalArgumentException("generation must be non-negative");
    }

    Configuration conf = config != null ? config : HBaseConfiguration.create();
    String normalizedIndexName = SchemaUtil.normalizeFullTableName(indexName);
    List<ScorecardRow> rows = CentroidManager.loadScorecard(conn, normalizedIndexName, generation);
    DriftEvaluationResult result = evaluateRows(rows, conf);

    long now = System.currentTimeMillis();
    CentroidManager.persistGenerationSummary(conn, normalizedIndexName, generation, null, null,
      result.getTriggerReason(), null, now);

    return result;
  }

  /** Evaluates scorecard rows against configured drift and skew thresholds. */
  public static DriftEvaluationResult evaluateRows(List<ScorecardRow> rows, Configuration conf) {
    if (rows == null || rows.isEmpty()) {
      return new DriftEvaluationResult(false, null, 0.0, 0.0, 0.0, 0.0, 0L, 0L, 0);
    }

    long minClusterSize = conf.getLong(QueryServices.VECTOR_INDEX_DRIFT_MIN_CLUSTER_SIZE_ATTRIB,
      QueryServicesOptions.DEFAULT_VECTOR_INDEX_DRIFT_MIN_CLUSTER_SIZE);
    double skewRatioThreshold =
      conf.getDouble(QueryServices.VECTOR_INDEX_DRIFT_SKEW_RATIO_THRESHOLD_ATTRIB,
        QueryServicesOptions.DEFAULT_VECTOR_INDEX_DRIFT_SKEW_RATIO_THRESHOLD);
    double sizeCvThreshold = SIZE_CV_THRESHOLD;
    double emptyCentroidFractionThreshold = EMPTY_CENTROID_FRACTION_THRESHOLD;
    double reassignRateThreshold = REASSIGN_RATE_THRESHOLD;

    int k = rows.size();
    long totalClusterSize = 0L;
    long totalReassignCount = 0L;
    long maxClusterSize = 0L;
    int emptyCount = 0;

    for (ScorecardRow row : rows) {
      long size = row.getClusterSize() != null ? row.getClusterSize() : 0L;
      long reassign = row.getReassignCount() != null ? row.getReassignCount() : 0L;
      totalClusterSize += size;
      totalReassignCount += reassign;
      if (size > maxClusterSize) {
        maxClusterSize = size;
      }
      if (size == 0) {
        emptyCount++;
      }
    }

    double avgSize = k > 0 ? (double) totalClusterSize / k : 0.0;
    double skewRatio = avgSize > 0.0 ? (double) maxClusterSize / avgSize : 0.0;

    double sumSqDiff = 0.0;
    for (ScorecardRow row : rows) {
      long size = row.getClusterSize() != null ? row.getClusterSize() : 0L;
      double diff = size - avgSize;
      sumSqDiff += diff * diff;
    }
    double stdDevPop = k > 0 ? Math.sqrt(sumSqDiff / k) : 0.0;
    double sizeCv = avgSize > 0.0 ? stdDevPop / avgSize : 0.0;

    double emptyCentroidFraction = k > 0 ? (double) emptyCount / k : 0.0;
    double reassignmentRate =
      totalClusterSize > 0 ? (double) totalReassignCount / totalClusterSize : 0.0;

    // Skip drift evaluation if total rows are below minimum cluster size threshold
    if (totalClusterSize < minClusterSize) {
      return new DriftEvaluationResult(false, null, skewRatio, sizeCv, emptyCentroidFraction,
        reassignmentRate, totalClusterSize, totalReassignCount, k);
    }

    boolean shouldRebuild = false;
    StringBuilder reasonBuilder = new StringBuilder();

    if (skewRatio > skewRatioThreshold) {
      shouldRebuild = true;
      reasonBuilder.append("SKEW_RATIO_EXCEEDED: ")
        .append(String.format("%.2f > %.2f", skewRatio, skewRatioThreshold));
    }
    if (sizeCv > sizeCvThreshold) {
      if (shouldRebuild) {
        reasonBuilder.append("; ");
      }
      shouldRebuild = true;
      reasonBuilder.append("SIZE_CV_EXCEEDED: ")
        .append(String.format("%.2f > %.2f", sizeCv, sizeCvThreshold));
    }
    if (emptyCentroidFraction > emptyCentroidFractionThreshold) {
      if (shouldRebuild) {
        reasonBuilder.append("; ");
      }
      shouldRebuild = true;
      reasonBuilder.append("EMPTY_CENTROID_FRACTION_EXCEEDED: ").append(
        String.format("%.2f > %.2f", emptyCentroidFraction, emptyCentroidFractionThreshold));
    }
    if (reassignmentRate > reassignRateThreshold) {
      if (shouldRebuild) {
        reasonBuilder.append("; ");
      }
      shouldRebuild = true;
      reasonBuilder.append("REASSIGN_RATE_EXCEEDED: ")
        .append(String.format("%.2f > %.2f", reassignmentRate, reassignRateThreshold));
    }

    String triggerReason = shouldRebuild ? reasonBuilder.toString() : null;
    return new DriftEvaluationResult(shouldRebuild, triggerReason, skewRatio, sizeCv,
      emptyCentroidFraction, reassignmentRate, totalClusterSize, totalReassignCount, k);
  }
}
