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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.phoenix.index.vector.VectorIndexScorecard.Assessment;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Test;

public class VectorIndexScorecardTest {

  private static List<ScorecardRow> rows(long... sizes) {
    List<ScorecardRow> rows = new ArrayList<>();
    for (int i = 0; i < sizes.length; i++) {
      rows.add(new ScorecardRow(i, sizes[i], 0));
    }
    return rows;
  }

  private static ReadOnlyProps conf() {
    return conf(new HashMap<>());
  }

  private static ReadOnlyProps conf(Map<String, String> props) {
    props.put(QueryServices.VECTOR_DRIFT_MIN_POPULATION_ATTRIB, "1000");
    return new ReadOnlyProps(props);
  }

  @Test
  public void testBalancedIndexIsNotDrifted() {
    Assessment assessment = VectorIndexScorecard.assess(rows(500, 500, 500, 500), conf());
    assertFalse(assessment.isDrifted());
    assertNull(assessment.getReason());
    assertEquals(1.0, assessment.getSkewRatio(), 1e-9);
    assertEquals(0.0, assessment.getSizeCv(), 1e-9);
    assertEquals(0.0, assessment.getEmptyCentroidFraction(), 1e-9);
    assertEquals(2000, assessment.getPopulation());
  }

  @Test
  public void testSkewRatioIsTakenAgainstTheMedian() {
    // Nine lists have size 100 and one list has size 9100. The median is 100 and the mean is 1000.
    long[] sizes = new long[10];
    java.util.Arrays.fill(sizes, 100);
    sizes[9] = 9100;
    Assessment assessment = VectorIndexScorecard.assess(rows(sizes), conf());
    assertEquals(91.0, assessment.getSkewRatio(), 1e-9);
    assertTrue(assessment.isDrifted());
    assertTrue(assessment.getReason(), assessment.getReason().contains("SKEW_RATIO_EXCEEDED"));
  }

  /**
   * A zero median list size with a nonzero maximum size gives an unbounded skew ratio.
   */
  @Test
  public void testZeroMedianIsUnboundedSkew() {
    Assessment assessment = VectorIndexScorecard.assess(rows(0, 0, 0, 1000), conf());
    assertTrue(Double.isInfinite(assessment.getSkewRatio()));
    assertTrue(assessment.isDrifted());
    assertTrue(assessment.getReason(),
      assessment.getReason().contains("SKEW_RATIO_EXCEEDED: unbounded"));
  }

  @Test
  public void testEmptyIndexHasNoSkew() {
    Assessment assessment = VectorIndexScorecard.assess(rows(0, 0, 0, 0), conf());
    assertEquals(0.0, assessment.getSkewRatio(), 1e-9);
    assertFalse(assessment.isDrifted());
  }

  @Test
  public void testSmallIndexIsNotAssessed() {
    Assessment assessment = VectorIndexScorecard.assess(rows(0, 0, 0, 999), conf());
    assertTrue(Double.isInfinite(assessment.getSkewRatio()));
    assertFalse("below the minimum population", assessment.isDrifted());
  }

  /**
   * The skew ratio and the empty list fraction are exactly at their thresholds. Each check uses a
   * strict comparison, thus only the coefficient of variation shows drift.
   */
  @Test
  public void testSizeCoefficientOfVariation() {
    Assessment assessment =
      VectorIndexScorecard.assess(rows(0, 0, 1000, 2000, 2000, 2000, 2000, 8000), conf());
    assertEquals(4.0, assessment.getSkewRatio(), 1e-9);
    assertEquals(0.25, assessment.getEmptyCentroidFraction(), 1e-9);
    assertEquals(1.1145, assessment.getSizeCv(), 1e-4);
    assertTrue(assessment.isDrifted());
    assertEquals("SIZE_CV_EXCEEDED: 1.11 > 1.00", assessment.getReason());
  }

  @Test
  public void testEmptyCentroidFraction() {
    // Half of the posting lists are empty. The median list size is 500.
    Assessment assessment = VectorIndexScorecard.assess(rows(0, 0, 1000, 1000), conf());
    assertEquals(0.5, assessment.getEmptyCentroidFraction(), 1e-9);
    assertTrue(assessment.getReason(),
      assessment.getReason().contains("EMPTY_CENTROID_FRACTION_EXCEEDED"));
  }

  @Test
  public void testReassignmentRate() {
    List<ScorecardRow> rows = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      rows.add(new ScorecardRow(i, 500, 150));
    }
    Assessment assessment = VectorIndexScorecard.assess(rows, conf());
    assertEquals(0.3, assessment.getReassignmentRate(), 1e-9);
    assertEquals("REASSIGN_RATE_EXCEEDED: 0.30 > 0.20", assessment.getReason());
  }

  @Test
  public void testSkewRatioThresholdIsConfigurable() {
    Map<String, String> props = new HashMap<>();
    props.put(QueryServices.VECTOR_DRIFT_SKEW_RATIO_THRESHOLD_ATTRIB, "100");
    ReadOnlyProps conf = conf(props);
    long[] sizes = new long[10];
    java.util.Arrays.fill(sizes, 100);
    sizes[9] = 9100;
    Assessment assessment = VectorIndexScorecard.assess(rows(sizes), conf);
    assertFalse(assessment.getReason(), assessment.getReason().contains("SKEW_RATIO_EXCEEDED"));
  }

  @Test
  public void testNoCentroids() {
    assertFalse(VectorIndexScorecard.assess(new ArrayList<>(), conf()).isDrifted());
  }

  /**
   * Verifies that the recount fence adds the clock skew bound and the completion budget. A negative
   * setting counts as zero, and the fence does not overflow.
   */
  @Test
  public void testFenceAddsSkewAndCompletionBudget() {
    assertEquals(
      1000L + QueryServicesOptions.DEFAULT_VECTOR_SCORECARD_CLOCK_SKEW_MS
        + QueryServicesOptions.DEFAULT_VECTOR_SCORECARD_COMPLETION_BUDGET_MS,
      VectorIndexScorecard.fence(1000L, ReadOnlyProps.EMPTY_PROPS));
    Map<String, String> props = new HashMap<>();
    props.put(QueryServices.VECTOR_SCORECARD_CLOCK_SKEW_MS_ATTRIB, "100");
    props.put(QueryServices.VECTOR_SCORECARD_COMPLETION_BUDGET_MS_ATTRIB, "20");
    assertEquals(1120L, VectorIndexScorecard.fence(1000L, new ReadOnlyProps(props)));
    props.put(QueryServices.VECTOR_SCORECARD_CLOCK_SKEW_MS_ATTRIB, "-100");
    assertEquals(1020L, VectorIndexScorecard.fence(1000L, new ReadOnlyProps(props)));
    props.put(QueryServices.VECTOR_SCORECARD_CLOCK_SKEW_MS_ATTRIB, String.valueOf(Long.MAX_VALUE));
    assertEquals(Long.MAX_VALUE, VectorIndexScorecard.fence(1000L, new ReadOnlyProps(props)));
  }

  /**
   * Verifies that a recount writes the absolute count of each centroid with a fence from the server
   * time after the count. A centroid with no postings gets a count of zero. The reassignment reset
   * subtracts the counts that the recount read. The write applies only to a centroid with an older
   * fence, so the fence only moves forward.
   */
  @Test
  public void testReconcileWritesAbsoluteCountsUnderFence() throws Exception {
    Map<String, String> settings = new HashMap<>();
    settings.put(QueryServices.VECTOR_SCORECARD_CLOCK_SKEW_MS_ATTRIB, "100");
    settings.put(QueryServices.VECTOR_SCORECARD_COMPLETION_BUDGET_MS_ATTRIB, "20");
    ConnectionQueryServices services = mock(ConnectionQueryServices.class);
    when(services.getProps()).thenReturn(new ReadOnlyProps(settings));
    PhoenixConnection conn = mock(PhoenixConnection.class);
    when(conn.getQueryServices()).thenReturn(services);
    PTable index = mock(PTable.class);
    PName name = mock(PName.class);
    when(name.getString()).thenReturn("IDX_RC");
    when(index.getName()).thenReturn(name);

    // The index holds 5 rows under centroid 0 and 1 row under centroid 2
    Statement count = mock(Statement.class);
    when(conn.createStatement()).thenReturn(count);
    ResultSet postings = mock(ResultSet.class);
    when(count.executeQuery(anyString())).thenReturn(postings);
    when(postings.next()).thenReturn(true, true, false);
    when(postings.getInt(1)).thenReturn(0, 2);
    when(postings.getLong(2)).thenReturn(5L, 1L);

    // The server writes the summary row at time 5000
    PreparedStatement time = mock(PreparedStatement.class);
    ResultSet timeResult = mock(ResultSet.class);
    when(time.executeQuery()).thenReturn(timeResult);
    when(timeResult.next()).thenReturn(true);
    when(timeResult.getDate(1)).thenReturn(new Date(5000L));

    // The stored scorecard has wrong sizes and some reassignments
    PreparedStatement load = mock(PreparedStatement.class);
    ResultSet stored = mock(ResultSet.class);
    when(load.executeQuery()).thenReturn(stored);
    when(stored.next()).thenReturn(true, true, true, false);
    when(stored.getInt(1)).thenReturn(0, 1, 2);
    when(stored.getLong(2)).thenReturn(9L, 3L, 0L);
    when(stored.getLong(3)).thenReturn(4L, 0L, 1L);

    PreparedStatement counts = mock(PreparedStatement.class);
    PreparedStatement other = mock(PreparedStatement.class);
    List<String> countSql = new ArrayList<>();
    List<String> otherSql = new ArrayList<>();
    when(conn.prepareStatement(anyString())).thenAnswer(invocation -> {
      String sql = invocation.getArgument(0);
      if (sql.startsWith("SELECT PHOENIX_ROW_TIMESTAMP()")) {
        return time;
      } else if (sql.startsWith("SELECT")) {
        return load;
      } else if (sql.contains("SCORECARD_FENCE")) {
        countSql.add(sql);
        return counts;
      }
      otherSql.add(sql);
      return other;
    });

    List<ScorecardRow> reconciled = VectorIndexScorecard.reconcile(conn, index, 7L);

    assertEquals(3, reconciled.size());
    assertEquals(5, reconciled.get(0).getClusterSize());
    assertEquals(4, reconciled.get(0).getReassignCount());
    assertEquals(0, reconciled.get(1).getClusterSize());
    assertEquals(1, reconciled.get(2).getClusterSize());
    assertEquals(1, countSql.size());
    String sql = countSql.get(0);
    assertTrue(sql, sql.contains("CLUSTER_SIZE = CASE WHEN COALESCE(SCORECARD_FENCE, 0) < ?"));
    assertTrue(sql, sql.contains("SCORECARD_FENCE = CASE WHEN COALESCE(SCORECARD_FENCE, 0) < ?"));
    assertTrue(sql, sql.contains("REASSIGN_COUNT = COALESCE(REASSIGN_COUNT, 0) - ?"));
    // The fence is the server time plus the skew bound and the completion budget
    verify(counts, times(3)).setLong(4, 5120L);
    verify(counts, times(3)).setLong(6, 5120L);
    verify(counts, times(3)).setLong(7, 5120L);
    verify(counts).setLong(5, 5L);
    verify(counts).setLong(5, 0L);
    verify(counts).setLong(5, 1L);
    verify(counts).setLong(8, 4L);
    verify(counts).setLong(8, 0L);
    verify(counts).setLong(8, 1L);
    verify(counts, times(3)).executeUpdate();
    // The write that gets the server time does not create the summary row
    assertTrue(otherSql.toString(),
      otherSql.get(0).endsWith("UPDATE_ONLY LAST_SCORECARD_UPDATE = LAST_SCORECARD_UPDATE"));
  }

  /**
   * Verifies that a recount of a generation with no summary row fails. The recount does not write
   * counts for a deleted generation.
   */
  @Test
  public void testReconcileFailsForDeletedGeneration() throws Exception {
    ConnectionQueryServices services = mock(ConnectionQueryServices.class);
    when(services.getProps()).thenReturn(ReadOnlyProps.EMPTY_PROPS);
    PhoenixConnection conn = mock(PhoenixConnection.class);
    when(conn.getQueryServices()).thenReturn(services);
    PTable index = mock(PTable.class);
    PName name = mock(PName.class);
    when(name.getString()).thenReturn("IDX_GONE");
    when(index.getName()).thenReturn(name);
    Statement count = mock(Statement.class);
    when(conn.createStatement()).thenReturn(count);
    ResultSet postings = mock(ResultSet.class);
    when(count.executeQuery(anyString())).thenReturn(postings);
    when(postings.next()).thenReturn(false);

    // The summary row does not exist, so the time query returns no row
    PreparedStatement time = mock(PreparedStatement.class);
    ResultSet timeResult = mock(ResultSet.class);
    when(time.executeQuery()).thenReturn(timeResult);
    when(timeResult.next()).thenReturn(false);
    PreparedStatement counts = mock(PreparedStatement.class);
    PreparedStatement other = mock(PreparedStatement.class);
    when(conn.prepareStatement(anyString())).thenAnswer(invocation -> {
      String sql = invocation.getArgument(0);
      if (sql.startsWith("SELECT PHOENIX_ROW_TIMESTAMP()")) {
        return time;
      }
      return sql.contains("SCORECARD_FENCE") ? counts : other;
    });

    try {
      VectorIndexScorecard.reconcile(conn, index, 7L);
      fail("A recount of a deleted generation must fail");
    } catch (SQLException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("does not exist"));
    }
    verify(counts, never()).executeUpdate();
  }
}
