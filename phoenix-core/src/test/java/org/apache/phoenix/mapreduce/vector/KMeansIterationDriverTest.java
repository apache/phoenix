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
package org.apache.phoenix.mapreduce.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.phoenix.mapreduce.vector.KMeansIterationDriver.WorstFitCandidate;
import org.junit.Test;

public class KMeansIterationDriverTest {

  @Test
  public void testSelectDistinctReseedVectorsAssignsDistinctVectorsToMultipleEmptyClusters() {
    List<WorstFitCandidate> candidates = Arrays.asList(
      new WorstFitCandidate(new float[] { 1.0f, 1.0f }, 5.0),
      new WorstFitCandidate(new float[] { 2.0f, 2.0f }, 15.0),
      new WorstFitCandidate(new float[] { 3.0f, 3.0f }, 10.0));

    // Three empty clusters, three distinct mapper-local worst-fit candidates available: each
    // empty cluster must be re-seeded with a different vector, ordered most-severe first.
    List<float[]> reseedVectors =
      KMeansIterationDriver.selectDistinctReseedVectors(candidates, 3, "L2");

    assertEquals(3, reseedVectors.size());
    assertEquals(2.0f, reseedVectors.get(0)[0], 1e-6f);
    assertEquals(3.0f, reseedVectors.get(1)[0], 1e-6f);
    assertEquals(1.0f, reseedVectors.get(2)[0], 1e-6f);
    assertNotEquals(reseedVectors.get(0)[0], reseedVectors.get(1)[0]);
    assertNotEquals(reseedVectors.get(1)[0], reseedVectors.get(2)[0]);
  }

  @Test
  public void testSelectDistinctReseedVectorsExhaustsGracefullyWhenFewerCandidatesThanEmptyClusters() {
    List<WorstFitCandidate> candidates =
      Collections.singletonList(new WorstFitCandidate(new float[] { 1.0f, 1.0f }, 5.0));

    // Two empty clusters but only one candidate available: only one re-seed vector is produced,
    // leaving the caller to fall back for the remaining empty cluster instead of duplicating.
    List<float[]> reseedVectors =
      KMeansIterationDriver.selectDistinctReseedVectors(candidates, 2, "L2");

    assertEquals(1, reseedVectors.size());
  }

  @Test
  public void testSelectDistinctReseedVectorsNormalizesForCosineMetric() {
    List<WorstFitCandidate> candidates =
      Collections.singletonList(new WorstFitCandidate(new float[] { 3.0f, 4.0f }, 5.0));

    List<float[]> reseedVectors =
      KMeansIterationDriver.selectDistinctReseedVectors(candidates, 1, "COSINE");

    float[] v = reseedVectors.get(0);
    double norm = Math.sqrt((double) v[0] * v[0] + (double) v[1] * v[1]);
    assertEquals(1.0, norm, 1e-6);
  }
}
