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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.phoenix.index.vector.VectorIndexScorecard.Assessment;
import org.apache.phoenix.query.QueryServices;
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
}
