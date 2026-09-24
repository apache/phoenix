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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.phoenix.index.vector.ScorecardAccumulator.CentroidKey;
import org.apache.phoenix.index.vector.VectorIndexScorecard.DriftEvaluationResult;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryServices;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class VectorIndexScorecardTest {

  @Test
  public void testBalancedIndexDoesNotTriggerRebuild() {
    Configuration conf = HBaseConfiguration.create();
    List<ScorecardRow> rows = new ArrayList<>();
    // 4 centroids, each with 500 rows (total 2000 >= 1000 threshold), 0 reassignments
    for (int i = 0; i < 4; i++) {
      rows.add(new ScorecardRow("TEST_IDX", 1L, i, 500L, 0L, System.currentTimeMillis()));
    }

    DriftEvaluationResult result = VectorIndexScorecard.evaluateRows(rows, conf);
    assertFalse("Balanced index must not trigger rebuild", result.shouldRebuild());
    assertNull("Trigger reason should be null when thresholds are not exceeded",
      result.getTriggerReason());
    assertEquals(1.0, result.getSkewRatio(), 0.001);
    assertEquals(0.0, result.getSizeCv(), 0.001);
    assertEquals(0.0, result.getEmptyCentroidFraction(), 0.001);
    assertEquals(0.0, result.getReassignmentRate(), 0.001);
    assertEquals(2000L, result.getTotalClusterSize());
  }

  @Test
  public void testSkewRatioTrigger() {
    Configuration conf = HBaseConfiguration.create();
    List<ScorecardRow> rows = new ArrayList<>();
    // 10 centroids: 9 with 100 rows, 1 with 9100 rows.
    // Total = 10000 >= 1000, avg = 1000, max = 9100, skew ratio = 9.1 > 4.0
    for (int i = 0; i < 9; i++) {
      rows.add(new ScorecardRow("TEST_IDX", 1L, i, 100L, 0L, System.currentTimeMillis()));
    }
    rows.add(new ScorecardRow("TEST_IDX", 1L, 9, 9100L, 0L, System.currentTimeMillis()));

    DriftEvaluationResult result = VectorIndexScorecard.evaluateRows(rows, conf);
    assertTrue("Skewed index must trigger rebuild", result.shouldRebuild());
    assertNotNull(result.getTriggerReason());
    assertTrue("Reason should mention skew ratio",
      result.getTriggerReason().contains("SKEW_RATIO_EXCEEDED"));
    assertEquals(9.1, result.getSkewRatio(), 0.01);
  }

  @Test
  public void testSizeCvTrigger() {
    Configuration conf = HBaseConfiguration.create();
    List<ScorecardRow> rows = new ArrayList<>();
    // 4 centroids with sizes [200, 200, 200, 2400]
    // Total = 3000 >= 1000, avg = 750, CV = ~1.27 > 1.0
    rows.add(new ScorecardRow("TEST_IDX", 1L, 0, 200L, 0L, System.currentTimeMillis()));
    rows.add(new ScorecardRow("TEST_IDX", 1L, 1, 200L, 0L, System.currentTimeMillis()));
    rows.add(new ScorecardRow("TEST_IDX", 1L, 2, 200L, 0L, System.currentTimeMillis()));
    rows.add(new ScorecardRow("TEST_IDX", 1L, 3, 2400L, 0L, System.currentTimeMillis()));

    DriftEvaluationResult result = VectorIndexScorecard.evaluateRows(rows, conf);
    assertTrue("High CV index must trigger rebuild", result.shouldRebuild());
    assertNotNull(result.getTriggerReason());
    assertTrue("Reason should mention size CV",
      result.getTriggerReason().contains("SIZE_CV_EXCEEDED"));
    assertTrue(result.getSizeCv() > 1.0);
  }

  @Test
  public void testEmptyCentroidFractionTrigger() {
    Configuration conf = HBaseConfiguration.create();
    List<ScorecardRow> rows = new ArrayList<>();
    // 8 centroids, 3 empty, 5 with 400 rows
    // Total = 2000 >= 1000, empty fraction = 3/8 = 0.375 > 0.25
    for (int i = 0; i < 3; i++) {
      rows.add(new ScorecardRow("TEST_IDX", 1L, i, 0L, 0L, System.currentTimeMillis()));
    }
    for (int i = 3; i < 8; i++) {
      rows.add(new ScorecardRow("TEST_IDX", 1L, i, 400L, 0L, System.currentTimeMillis()));
    }

    DriftEvaluationResult result = VectorIndexScorecard.evaluateRows(rows, conf);
    assertTrue("High empty centroid fraction must trigger rebuild", result.shouldRebuild());
    assertNotNull(result.getTriggerReason());
    assertTrue("Reason should mention empty centroid fraction",
      result.getTriggerReason().contains("EMPTY_CENTROID_FRACTION_EXCEEDED"));
    assertEquals(0.375, result.getEmptyCentroidFraction(), 0.001);
  }

  @Test
  public void testReassignmentRateTrigger() {
    Configuration conf = HBaseConfiguration.create();
    List<ScorecardRow> rows = new ArrayList<>();
    // 4 centroids with 500 rows each (total 2000 >= 1000), total reassignments = 600
    // Reassignment rate = 600 / 2000 = 0.30 > 0.20
    for (int i = 0; i < 4; i++) {
      rows.add(new ScorecardRow("TEST_IDX", 1L, i, 500L, 150L, System.currentTimeMillis()));
    }

    DriftEvaluationResult result = VectorIndexScorecard.evaluateRows(rows, conf);
    assertTrue("High reassignment rate must trigger rebuild", result.shouldRebuild());
    assertNotNull(result.getTriggerReason());
    assertTrue("Reason should mention reassignment rate",
      result.getTriggerReason().contains("REASSIGN_RATE_EXCEEDED"));
    assertEquals(0.30, result.getReassignmentRate(), 0.001);
  }

  @Test
  public void testSmallIndexSuppression() {
    Configuration conf = HBaseConfiguration.create();
    List<ScorecardRow> rows = new ArrayList<>();
    // Heavily skewed, but total rows = 500 < min.cluster.size (1000)
    rows.add(new ScorecardRow("TEST_IDX", 1L, 0, 500L, 200L, System.currentTimeMillis()));
    rows.add(new ScorecardRow("TEST_IDX", 1L, 1, 0L, 0L, System.currentTimeMillis()));
    rows.add(new ScorecardRow("TEST_IDX", 1L, 2, 0L, 0L, System.currentTimeMillis()));
    rows.add(new ScorecardRow("TEST_IDX", 1L, 3, 0L, 0L, System.currentTimeMillis()));

    DriftEvaluationResult result = VectorIndexScorecard.evaluateRows(rows, conf);
    assertFalse("Small index below min.cluster.size must suppress rebuild", result.shouldRebuild());
    assertNull(result.getTriggerReason());
    assertEquals(500L, result.getTotalClusterSize());
  }

  @Test
  public void testEmptyScorecardList() {
    Configuration conf = HBaseConfiguration.create();
    DriftEvaluationResult result = VectorIndexScorecard.evaluateRows(Collections.emptyList(), conf);
    assertFalse(result.shouldRebuild());
    assertNull(result.getTriggerReason());
    assertEquals(0L, result.getTotalClusterSize());
    assertEquals(0, result.getCentroidCount());
  }

  @Test
  public void testCustomConfigThresholds() {
    Configuration conf = HBaseConfiguration.create();
    conf.setDouble(QueryServices.VECTOR_INDEX_DRIFT_SKEW_RATIO_THRESHOLD_ATTRIB, 1.2);
    conf.setLong(QueryServices.VECTOR_INDEX_DRIFT_MIN_CLUSTER_SIZE_ATTRIB, 100L);

    List<ScorecardRow> rows = new ArrayList<>();
    // 2 centroids: 80 and 120 rows. Total = 200 >= 100.
    // Avg = 100, max = 120. Skew ratio = 1.20 <= 1.2 (no trigger).
    rows.add(new ScorecardRow("TEST_IDX", 1L, 0, 80L, 0L, System.currentTimeMillis()));
    rows.add(new ScorecardRow("TEST_IDX", 1L, 1, 120L, 0L, System.currentTimeMillis()));

    DriftEvaluationResult result = VectorIndexScorecard.evaluateRows(rows, conf);
    assertFalse(result.shouldRebuild());

    // Now lower threshold to 1.1: 1.20 > 1.1 -> triggers rebuild
    conf.setDouble(QueryServices.VECTOR_INDEX_DRIFT_SKEW_RATIO_THRESHOLD_ATTRIB, 1.1);
    result = VectorIndexScorecard.evaluateRows(rows, conf);
    assertTrue(result.shouldRebuild());
    assertTrue(result.getTriggerReason().contains("SKEW_RATIO_EXCEEDED"));
  }

  @Test
  public void testScorecardAccumulatorBufferingAndFlushing() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    // Disable background flusher
    conf.setLong(QueryServices.VECTOR_INDEX_SCORECARD_FLUSH_INTERVAL_MS_ATTRIB, 0L);
    ScorecardAccumulator accumulator = new ScorecardAccumulator(conf);

    accumulator.accumulate("MY_INDEX", 1L, 0, 5L, 1L);
    accumulator.accumulate("MY_INDEX", 1L, 0, 3L, 2L);
    accumulator.accumulate("MY_INDEX", 1L, 1, 10L, 0L);
    accumulator.accumulate("MY_INDEX", 1L, 1, -4L, 0L);

    assertEquals(8L, accumulator.getClusterSizeDelta("MY_INDEX", 1L, 0));
    assertEquals(3L, accumulator.getReassignCountDelta("MY_INDEX", 1L, 0));
    assertEquals("a delete cancels part of an insert before it is ever written", 6L,
      accumulator.getClusterSizeDelta("MY_INDEX", 1L, 1));
    assertEquals(0L, accumulator.getReassignCountDelta("MY_INDEX", 1L, 1));

    Connection mockConn = mock(Connection.class);
    PreparedStatement mockPs = mock(PreparedStatement.class);
    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    when(mockConn.prepareStatement(anyString())).thenReturn(mockPs);

    accumulator.flush(mockConn);

    // Verify atomic delta accumulation SQL syntax using ON DUPLICATE KEY UPDATE.
    verify(mockConn).prepareStatement(sqlCaptor.capture());
    String sql = sqlCaptor.getValue();
    assertTrue(sql, sql.contains("ON DUPLICATE KEY UPDATE"));
    assertTrue(sql, sql.contains(PhoenixDatabaseMetaData.CLUSTER_SIZE + " = "
      + PhoenixDatabaseMetaData.CLUSTER_SIZE + " + ?"));
    assertTrue(sql, sql.contains(PhoenixDatabaseMetaData.REASSIGN_COUNT + " = "
      + PhoenixDatabaseMetaData.REASSIGN_COUNT + " + ?"));

    // One statement per buffered centroid, under a single commit.
    verify(mockPs, times(2)).executeUpdate();
    verify(mockConn, times(1)).commit();
    // Verify initial values and increment parameters match accumulated deltas.
    verify(mockPs).setLong(4, 8L);
    verify(mockPs).setLong(6, 8L);
    verify(mockPs).setLong(5, 3L);
    verify(mockPs).setLong(7, 3L);
    verify(mockPs).setLong(4, 6L);
    verify(mockPs).setLong(6, 6L);

    // After flush, deltas should be cleared
    assertEquals(0L, accumulator.getClusterSizeDelta("MY_INDEX", 1L, 0));
    assertEquals(0L, accumulator.getReassignCountDelta("MY_INDEX", 1L, 0));
    assertEquals(0L, accumulator.getClusterSizeDelta("MY_INDEX", 1L, 1));
    assertEquals(0L, accumulator.getReassignCountDelta("MY_INDEX", 1L, 1));

    accumulator.close();
  }

  @Test
  public void testScorecardAccumulatorFlushFailureRestoresDeltas() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setLong(QueryServices.VECTOR_INDEX_SCORECARD_FLUSH_INTERVAL_MS_ATTRIB, 0L);
    ScorecardAccumulator accumulator = new ScorecardAccumulator(conf);

    accumulator.accumulate("MY_INDEX", 1L, 0, 10L, 4L);

    Connection mockConn = mock(Connection.class);
    PreparedStatement mockPs = mock(PreparedStatement.class);
    when(mockConn.prepareStatement(anyString())).thenReturn(mockPs);
    doThrow(new SQLException("Simulated write failure")).when(mockPs).executeUpdate();

    try {
      accumulator.flush(mockConn);
      assertTrue("Expected SQLException", false);
    } catch (SQLException expected) {
      // Expected
    }

    // Deltas should be restored for retry on next flush
    assertEquals(10L, accumulator.getClusterSizeDelta("MY_INDEX", 1L, 0));
    assertEquals(4L, accumulator.getReassignCountDelta("MY_INDEX", 1L, 0));

    accumulator.close();
  }

  @Test
  public void testCentroidKeyEquality() {
    CentroidKey k1 = new CentroidKey("idx", 1L, 2);
    CentroidKey k2 = new CentroidKey("IDX", 1L, 2);
    CentroidKey k3 = new CentroidKey("idx", 2L, 2);
    CentroidKey k4 = new CentroidKey("idx", 1L, 3);

    assertEquals(k1, k2);
    assertEquals(k1.hashCode(), k2.hashCode());
    assertFalse(k1.equals(k3));
    assertFalse(k1.equals(k4));
    assertFalse(k1.equals("different type"));
  }
}
