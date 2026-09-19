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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.phoenix.expression.function.VectorDistanceUtil;
import org.apache.phoenix.optimize.DistanceMetric;
import org.junit.Test;

public class KMeansTrainerTest {

  private static final double EPSILON = 1e-5;

  /** Returns 2D vectors with Gaussian noise around each given center. */
  private static List<float[]> generate2DGaussianClusters(float[][] centers, int pointsPerCluster,
    double stdDev, Random rng) {
    List<float[]> dataset = new ArrayList<>(centers.length * pointsPerCluster);
    for (float[] center : centers) {
      for (int i = 0; i < pointsPerCluster; i++) {
        float x = (float) (center[0] + rng.nextGaussian() * stdDev);
        float y = (float) (center[1] + rng.nextGaussian() * stdDev);
        dataset.add(new float[] { x, y });
      }
    }
    return dataset;
  }

  /** Verifies that training finds the centers of well-separated Gaussian clusters. */
  @Test
  public void testClusterRecovery() {
    float[][] trueCenters =
      new float[][] { { 10.0f, 0.0f }, { 0.0f, 10.0f }, { -10.0f, 0.0f }, { 0.0f, -10.0f } };
    Random rng = new Random(42);
    List<float[]> dataset = generate2DGaussianClusters(trueCenters, 100, 0.5, rng);

    KMeansConfig config = KMeansConfig.newBuilder().randomSeed(42L).build();
    KMeansResult result = KMeansTrainer.train(dataset, 4, config);

    assertEquals(4, result.getEffectiveK());
    boolean[] matched = new boolean[4];
    for (float[] centroid : result.getCentroids()) {
      int matchIdx = -1;
      for (int c = 0; c < 4; c++) {
        double dist =
          Math.sqrt(VectorDistanceUtil.scalarL2DistanceSquared(centroid, trueCenters[c], 2));
        if (dist < 2.0 && !matched[c]) {
          matchIdx = c;
          break;
        }
      }
      assertTrue("Centroid " + Arrays.toString(centroid) + " did not match any remaining center",
        matchIdx >= 0);
      matched[matchIdx] = true;
    }
    for (int c = 0; c < 4; c++) {
      assertTrue("Center " + c + " was not matched", matched[c]);
    }
  }

  /**
   * Verifies that training with k-means++ seeds converges within 50 iterations for 10 different
   * random seeds.
   */
  @Test
  public void testKMeansPlusPlusInitializationQuality() {
    float[][] trueCenters =
      new float[][] { { 10.0f, 0.0f }, { 0.0f, 10.0f }, { -10.0f, 0.0f }, { 0.0f, -10.0f } };
    Random rng = new Random(100);
    List<float[]> dataset = generate2DGaussianClusters(trueCenters, 100, 0.5, rng);

    for (int run = 0; run < 10; run++) {
      KMeansConfig config =
        KMeansConfig.newBuilder().randomSeed(1000L + run * 17).maxIterations(50).build();
      KMeansResult result = KMeansTrainer.train(dataset, 4, config);
      assertTrue("Run " + run + " did not converge in <= 50 iterations (took "
        + result.getIterations() + ")", result.isConverged());
      assertTrue(result.getIterations() <= 50);
    }
  }

  /** Verifies that a single cluster converges to the mean of the full dataset. */
  @Test
  public void testSingleClusterDegenerateCase() {
    Random rng = new Random(42);
    List<float[]> dataset = new ArrayList<>(200);
    double sumX = 0.0;
    double sumY = 0.0;
    for (int i = 0; i < 200; i++) {
      float x = (float) (rng.nextGaussian() * 3.0 + 7.0);
      float y = (float) (rng.nextGaussian() * 3.0 - 5.0);
      sumX += x;
      sumY += y;
      dataset.add(new float[] { x, y });
    }

    KMeansResult result = KMeansTrainer.train(dataset, 1);
    assertEquals(1, result.getEffectiveK());
    assertEquals(1, result.getRequestedK());
    float[] centroid = result.getCentroids().get(0);
    assertEquals(sumX / 200.0, centroid[0], 0.01);
    assertEquals(sumY / 200.0, centroid[1], 0.01);
  }

  /**
   * Verifies that the skew metrics show a skewed clustering, and that the split heuristic after
   * training decreases the coefficient of variation of the cluster sizes.
   */
  @Test
  public void testSkewMetricComputationAndSplitHeuristic() {
    Random rng = new Random(42);
    List<float[]> dataset = new ArrayList<>(1000);
    // One large cluster and three small clusters make the dataset skewed
    for (int i = 0; i < 900; i++) {
      dataset.add(new float[] { (float) (20.0 + rng.nextGaussian() * 0.5),
        (float) (20.0 + rng.nextGaussian() * 0.5) });
    }
    for (int i = 0; i < 33; i++) {
      dataset.add(new float[] { (float) (-20.0 + rng.nextGaussian() * 0.5),
        (float) (20.0 + rng.nextGaussian() * 0.5) });
    }
    for (int i = 0; i < 33; i++) {
      dataset.add(new float[] { (float) (-20.0 + rng.nextGaussian() * 0.5),
        (float) (-20.0 + rng.nextGaussian() * 0.5) });
    }
    for (int i = 0; i < 34; i++) {
      dataset.add(new float[] { (float) (20.0 + rng.nextGaussian() * 0.5),
        (float) (-20.0 + rng.nextGaussian() * 0.5) });
    }

    KMeansConfig config = KMeansConfig.newBuilder().randomSeed(42L).enableSplitHeuristic(true)
      .splitThresholdMultiplier(2.0).build();

    KMeansResult result = KMeansTrainer.train(dataset, 4, config);

    assertNotNull(result.getPreSplitSkewMetrics());
    double preSplitCv = result.getPreSplitSkewMetrics().getCoefficientOfVariation();
    assertTrue("Pre-split CV must be > 0.5, got " + preSplitCv, preSplitCv > 0.5);

    assertTrue("Split heuristic should have triggered", result.hasSplit());
    assertTrue("Effective k must be >= requested k",
      result.getEffectiveK() >= result.getRequestedK());

    assertNotNull(result.getPostSplitSkewMetrics());
    double postSplitCv = result.getPostSplitSkewMetrics().getCoefficientOfVariation();
    assertTrue(
      "Post-split CV (" + postSplitCv + ") should be lower than pre-split CV (" + preSplitCv + ")",
      postSplitCv < preSplitCv);
  }

  /**
   * Verifies that no cluster stays empty after the split of a cluster of duplicate points. Both
   * halves of the split are at the same distance from each duplicate, so the tie can leave one half
   * empty. The rebalance after the split must reseed that half.
   */
  @Test
  public void testSplitRebalanceReseedsPhantomEmptyCentroid() {
    List<float[]> dataset = new ArrayList<>(210);
    for (int i = 0; i < 200; i++) {
      dataset.add(new float[] { 5.0f, 5.0f });
    }
    for (int i = 0; i < 10; i++) {
      dataset.add(new float[] { -5.0f, -5.0f });
    }

    KMeansConfig config = KMeansConfig.newBuilder().randomSeed(42L).enableSplitHeuristic(true)
      .splitThresholdMultiplier(1.1).splitRebalanceIterations(1).build();

    KMeansResult result = KMeansTrainer.train(dataset, 2, config);

    assertTrue("Split heuristic should have triggered", result.hasSplit());
    assertEquals(3, result.getEffectiveK());

    int[] clusterSizes = result.getClusterSizes();
    int total = 0;
    for (int size : clusterSizes) {
      assertTrue(
        "No split cluster should be permanently empty, got sizes " + Arrays.toString(clusterSizes),
        size > 0);
      total += size;
    }
    assertEquals(210, total);
  }

  /**
   * Verifies that COSINE training uses spherical k-means and gives unit-length centroids that match
   * the cluster directions.
   */
  @Test
  public void testCosineMetricTraining() {
    float[][] directions = new float[][] { { 1.0f, 0.0f, 0.0f }, { 0.0f, 1.0f, 0.0f },
      { 0.0f, 0.0f, 1.0f }, { (float) (-1.0 / Math.sqrt(3.0)), (float) (-1.0 / Math.sqrt(3.0)),
        (float) (-1.0 / Math.sqrt(3.0)) } };

    Random rng = new Random(42);
    List<float[]> dataset = new ArrayList<>(400);
    for (float[] dir : directions) {
      for (int i = 0; i < 100; i++) {
        float[] v = new float[] { (float) (dir[0] + (rng.nextFloat() - 0.5f) * 0.05f),
          (float) (dir[1] + (rng.nextFloat() - 0.5f) * 0.05f),
          (float) (dir[2] + (rng.nextFloat() - 0.5f) * 0.05f) };
        dataset.add(KMeansTrainer.l2Normalize(v));
      }
    }

    KMeansConfig config =
      KMeansConfig.newBuilder().distanceMetric(DistanceMetric.COSINE).randomSeed(42L).build();

    KMeansResult result = KMeansTrainer.train(dataset, 4, config);
    assertEquals(4, result.getEffectiveK());

    boolean[] matched = new boolean[4];
    for (float[] centroid : result.getCentroids()) {
      double normSq = 0.0;
      for (float val : centroid) {
        normSq += (double) val * val;
      }
      double norm = Math.sqrt(normSq);
      assertEquals("Centroid must be unit-normalized", 1.0, norm, 1e-5);

      int matchIdx = -1;
      for (int d = 0; d < 4; d++) {
        double cosDist = VectorDistanceUtil.scalarCosineDistance(centroid, directions[d], 3);
        if (cosDist < 0.05 && !matched[d]) {
          matchIdx = d;
          break;
        }
      }
      assertTrue("Centroid did not match any directional cluster with cosine dist < 0.05",
        matchIdx >= 0);
      matched[matchIdx] = true;
    }
    for (int d = 0; d < 4; d++) {
      assertTrue("Direction " + d + " was not recovered", matched[d]);
    }
  }

  /**
   * Verifies that INNER_PRODUCT training uses the arithmetic mean as the centroid and does not
   * normalize it.
   */
  @Test
  public void testInnerProductMetricTraining() {
    Random rng = new Random(42);
    List<float[]> dataset = new ArrayList<>(200);
    for (int i = 0; i < 100; i++) {
      dataset.add(new float[] { (float) (10.0 + rng.nextGaussian() * 0.5),
        (float) (10.0 + rng.nextGaussian() * 0.5) });
    }
    for (int i = 0; i < 100; i++) {
      dataset.add(new float[] { (float) (-10.0 + rng.nextGaussian() * 0.5),
        (float) (-10.0 + rng.nextGaussian() * 0.5) });
    }

    KMeansConfig config = KMeansConfig.newBuilder().distanceMetric(DistanceMetric.INNER_PRODUCT)
      .randomSeed(42L).build();

    KMeansResult result = KMeansTrainer.train(dataset, 2, config);
    assertEquals(2, result.getEffectiveK());

    for (float[] centroid : result.getCentroids()) {
      double norm =
        Math.sqrt((double) centroid[0] * centroid[0] + (double) centroid[1] * centroid[1]);
      // The arithmetic mean keeps the magnitude of the vectors
      assertTrue("Centroids must not be unit-normalized for INNER_PRODUCT", norm > 5.0);
    }

    boolean hasPositive = false;
    boolean hasNegative = false;
    for (float[] centroid : result.getCentroids()) {
      if (centroid[0] > 5.0 && centroid[1] > 5.0) {
        hasPositive = true;
      }
      if (centroid[0] < -5.0 && centroid[1] < -5.0) {
        hasNegative = true;
      }
    }
    assertTrue("Positive cluster center not recovered", hasPositive);
    assertTrue("Negative cluster center not recovered", hasNegative);
  }

  /**
   * Verifies the assignment distance for each metric. INNER_PRODUCT uses L2 distance, so a vector
   * goes to the nearest centroid and not to the centroid with the largest inner product.
   */
  @Test
  public void testAssignmentDistanceByMetric() {
    float[] v = new float[] { 10f, 0f };
    float[] near = new float[] { 9f, 0f };
    float[] large = new float[] { 100f, 0f };
    assertTrue(KMeansTrainer.assignmentDistance(DistanceMetric.INNER_PRODUCT, v, near)
        < KMeansTrainer.assignmentDistance(DistanceMetric.INNER_PRODUCT, v, large));
    assertEquals(1.0, KMeansTrainer.assignmentDistance(DistanceMetric.L2, v, near), EPSILON);
    assertEquals(VectorDistanceUtil.scalarCosineDistance(v, new float[] { 0f, 1f }, 2),
      KMeansTrainer.assignmentDistance(DistanceMetric.COSINE, v, new float[] { 0f, 1f }), EPSILON);
  }

  /**
   * Verifies that training converges by the relative change in distortion, and that distortion does
   * not increase from one iteration to the next.
   */
  @Test
  public void testConvergenceByRelativeDistortion() {
    float[][] trueCenters =
      new float[][] { { 10.0f, 0.0f }, { 0.0f, 10.0f }, { -10.0f, 0.0f }, { 0.0f, -10.0f } };
    Random rng = new Random(42);
    List<float[]> dataset = generate2DGaussianClusters(trueCenters, 100, 0.5, rng);

    KMeansConfig config =
      KMeansConfig.newBuilder().randomSeed(42L).convergenceThreshold(1e-4).build();

    KMeansResult result = KMeansTrainer.train(dataset, 4, config);
    assertTrue(result.isConverged());
    assertTrue(result.getFinalDistortion() > 0.0);
    assertFalse(Double.isInfinite(result.getFinalDistortion()));
    assertFalse(Double.isNaN(result.getFinalDistortion()));

    List<Double> history = result.getDistortionHistory();
    assertTrue(history.size() >= 2);
    for (int i = 1; i < history.size(); i++) {
      assertTrue("Distortion should not increase at iteration " + i + ": prev=" + history.get(i - 1)
        + ", curr=" + history.get(i), history.get(i) <= history.get(i - 1) + EPSILON);
    }
  }

  /**
   * Verifies that training rejects a null or empty vector list, a cluster count out of range, and
   * vectors of different dimensions.
   */
  @Test
  public void testValidationAndEdgeCases() {
    List<float[]> validVectors = Arrays.asList(new float[] { 1f, 2f }, new float[] { 3f, 4f });

    try {
      KMeansTrainer.train((List<float[]>) null, 2);
      fail("Expected IllegalArgumentException on null vector list");
    } catch (IllegalArgumentException expected) {
    }

    try {
      KMeansTrainer.train(Collections.<float[]> emptyList(), 2);
      fail("Expected IllegalArgumentException on empty vector list");
    } catch (IllegalArgumentException expected) {
    }

    try {
      KMeansTrainer.train(validVectors, 0);
      fail("Expected IllegalArgumentException on k <= 0");
    } catch (IllegalArgumentException expected) {
    }

    try {
      KMeansTrainer.train(validVectors, -1);
      fail("Expected IllegalArgumentException on k < 0");
    } catch (IllegalArgumentException expected) {
    }

    try {
      KMeansTrainer.train(validVectors, 3);
      fail("Expected IllegalArgumentException on k > N");
    } catch (IllegalArgumentException expected) {
    }

    List<float[]> mismatched = Arrays.asList(new float[] { 1f, 2f }, new float[] { 3f, 4f, 5f });
    try {
      KMeansTrainer.train(mismatched, 2);
      fail("Expected IllegalArgumentException on mismatched dimensions");
    } catch (IllegalArgumentException expected) {
    }
  }

  /** Verifies that each centroid is one of the input points when k equals the sample size. */
  @Test
  public void testKEqualsN() {
    List<float[]> dataset = new ArrayList<>(10);
    for (int i = 0; i < 10; i++) {
      dataset.add(new float[] { (float) (i * 2.0), (float) (i * 3.0) });
    }

    KMeansConfig config = KMeansConfig.newBuilder().randomSeed(42L).build();
    KMeansResult result = KMeansTrainer.train(dataset, 10, config);

    assertEquals(10, result.getEffectiveK());
    boolean[] matched = new boolean[10];
    for (float[] c : result.getCentroids()) {
      int matchIdx = -1;
      for (int i = 0; i < 10; i++) {
        float[] orig = dataset.get(i);
        if (Math.abs(c[0] - orig[0]) < 1e-4 && Math.abs(c[1] - orig[1]) < 1e-4 && !matched[i]) {
          matchIdx = i;
          break;
        }
      }
      assertTrue("Centroid must match an original point: " + Arrays.toString(c), matchIdx >= 0);
      matched[matchIdx] = true;
    }
  }

  /** Verifies that ClusterSkewMetrics serializes to bytes and back without change. */
  @Test
  public void testClusterSkewMetricsSerialization() {
    int[] sizes = new int[] { 5, 10, 20, 50, 100, 250, 500 };
    ClusterSkewMetrics m1 = ClusterSkewMetrics.compute(sizes);

    byte[] bytes = m1.toBytes();
    assertNotNull(bytes);
    assertTrue(bytes.length > 0);

    ClusterSkewMetrics m2 = ClusterSkewMetrics.fromBytes(bytes);
    assertEquals(m1, m2);
    assertEquals(m1.getMin(), m2.getMin());
    assertEquals(m1.getMax(), m2.getMax());
    assertEquals(m1.getMean(), m2.getMean(), EPSILON);
    assertEquals(m1.getP95(), m2.getP95(), EPSILON);
    assertEquals(m1.getStdDev(), m2.getStdDev(), EPSILON);
    assertEquals(m1.getCoefficientOfVariation(), m2.getCoefficientOfVariation(), EPSILON);
    assertEquals(m1.getTotalVectors(), m2.getTotalVectors());
    assertEquals(m1.getK(), m2.getK());
    assertArrayEquals(m1.getClusterSizes(), m2.getClusterSizes());
  }

  /**
   * Verifies that reservoirSample returns the correct sample size and selects each element with
   * equal probability.
   */
  @Test
  public void testReservoirSampling() {
    List<float[]> streamSmall = new ArrayList<>(5);
    for (int i = 0; i < 5; i++) {
      streamSmall.add(new float[] { (float) i });
    }
    List<float[]> sample1 =
      KMeansTrainer.reservoirSample(streamSmall.iterator(), 10, new Random(42));
    assertEquals(5, sample1.size());

    List<float[]> streamLarge = new ArrayList<>(20);
    for (int i = 0; i < 20; i++) {
      streamLarge.add(new float[] { (float) i });
    }
    List<float[]> sample2 =
      KMeansTrainer.reservoirSample(streamLarge.iterator(), 10, new Random(42));
    assertEquals(10, sample2.size());

    int n = 10;
    int k = 3;
    int runs = 10000;
    int[] counts = new int[n];
    Random rng = new Random(12345);

    for (int r = 0; r < runs; r++) {
      List<float[]> items = new ArrayList<>(n);
      for (int i = 0; i < n; i++) {
        items.add(new float[] { (float) i });
      }
      List<float[]> sample = KMeansTrainer.reservoirSample(items.iterator(), k, rng);
      for (float[] v : sample) {
        counts[(int) v[0]]++;
      }
    }

    // Each count has a binomial distribution with mean 3000 and a standard deviation of about 46.
    // The bounds are approximately 6.5 standard deviations from the mean.
    for (int i = 0; i < n; i++) {
      assertTrue("Element " + i + " count (" + counts[i] + ") outside expected range [2700, 3300]",
        counts[i] >= 2700 && counts[i] <= 3300);
    }
  }

  /** Verifies that reservoirSample ignores null elements. */
  @Test
  public void testReservoirSamplingSkipsNulls() {
    List<float[]> stream = new ArrayList<>(20);
    int expectedNulls = 7;
    for (int i = 0; i < 20; i++) {
      if (i % 3 == 0 && expectedNulls > 0) {
        stream.add(null);
        expectedNulls--;
      } else {
        stream.add(new float[] { (float) i, (float) (i * 2) });
      }
    }

    List<float[]> sample = KMeansTrainer.reservoirSample(stream.iterator(), 10, new Random(42));
    assertEquals(10, sample.size());
    for (float[] v : sample) {
      assertNotNull("Sample must not contain null vectors", v);
    }
  }

  /** Verifies that two training runs with the same random seed give the same centroids. */
  @Test
  public void testDeterministicReproducibility() {
    float[][] trueCenters =
      new float[][] { { 10.0f, 0.0f }, { 0.0f, 10.0f }, { -10.0f, 0.0f }, { 0.0f, -10.0f } };
    Random rng = new Random(42);
    List<float[]> dataset = generate2DGaussianClusters(trueCenters, 100, 0.5, rng);

    KMeansConfig config = KMeansConfig.newBuilder().randomSeed(99999L).build();

    KMeansResult r1 = KMeansTrainer.train(dataset, 4, config);
    KMeansResult r2 = KMeansTrainer.train(dataset, 4, config);

    assertEquals(r1.getEffectiveK(), r2.getEffectiveK());
    assertEquals(r1.getIterations(), r2.getIterations());
    for (int c = 0; c < r1.getEffectiveK(); c++) {
      assertArrayEquals("Centroid " + c + " must match across runs with same seed",
        r1.getCentroids().get(c), r2.getCentroids().get(c), 0.0f);
    }
  }

  /** Verifies that training finds each of the four cluster centers in 128 dimensions. */
  @Test(timeout = 10000)
  public void testHighDimensionalTraining() {
    int dim = 128;
    int k = 4;
    int pointsPerCluster = 125;
    Random rng = new Random(42);

    float[][] centers = new float[k][dim];
    for (int c = 0; c < k; c++) {
      for (int d = 0; d < dim; d++) {
        centers[c][d] = (c == (d % k)) ? 20.0f : 0.0f;
      }
    }

    List<float[]> dataset = new ArrayList<>(k * pointsPerCluster);
    for (int c = 0; c < k; c++) {
      for (int i = 0; i < pointsPerCluster; i++) {
        float[] v = new float[dim];
        for (int d = 0; d < dim; d++) {
          v[d] = (float) (centers[c][d] + rng.nextGaussian() * 0.5);
        }
        dataset.add(v);
      }
    }

    KMeansConfig config = KMeansConfig.newBuilder().randomSeed(42L).maxIterations(50).build();
    KMeansResult result = KMeansTrainer.train(dataset, k, config);

    assertEquals(k, result.getEffectiveK());

    boolean[] matched = new boolean[k];
    for (float[] centroid : result.getCentroids()) {
      int matchIdx = -1;
      for (int c = 0; c < k; c++) {
        double dist =
          Math.sqrt(VectorDistanceUtil.scalarL2DistanceSquared(centroid, centers[c], dim));
        if (dist < 5.0 && !matched[c]) {
          matchIdx = c;
          break;
        }
      }
      assertTrue("Centroid did not match any true high-dimensional center", matchIdx >= 0);
      matched[matchIdx] = true;
    }
    for (int c = 0; c < k; c++) {
      assertTrue("Center " + c + " was not matched in high-dimensional test", matched[c]);
    }
  }
}
