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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import org.apache.phoenix.expression.function.VectorDistanceUtil;
import org.apache.phoenix.optimize.DistanceMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-memory k-means trainer for IVF index centroids.
 * <p>
 * Provides k-means++ initialization, metric aware assignment and centroid updates (standard Lloyd
 * updates for L2 and inner product; spherical k-means for cosine), empty cluster reseeding,
 * relative distortion convergence, and post-training cluster splitting to balance posting lists.
 * Assignment distance calculation is centralized in {@link #assignmentDistance} to guarantee
 * consistent routing across training, index maintenance, and query probing.
 */
public final class KMeansTrainer {

  private static final Logger LOGGER = LoggerFactory.getLogger(KMeansTrainer.class);

  private KMeansTrainer() {
  }

  /**
   * Computes vector assignment distance according to the specified distance metric. Cosine distance
   * is used for COSINE indexes; squared Euclidean distance is used for L2 and INNER_PRODUCT
   * indexes.
   */
  public static double assignmentDistance(DistanceMetric metric, float[] a, float[] b) {
    if (metric == DistanceMetric.COSINE) {
      return VectorDistanceUtil.scalarCosineDistance(a, b, a.length);
    }
    return VectorDistanceUtil.scalarL2DistanceSquared(a, b, a.length);
  }

  /** Collects a uniform sample of vectors using reservoir sampling, skipping null entries. */
  public static List<float[]> reservoirSample(Iterator<float[]> iterator, int sampleSize,
    Random random) {
    if (sampleSize <= 0) {
      throw new IllegalArgumentException("sampleSize must be > 0: " + sampleSize);
    }
    List<float[]> reservoir = new ArrayList<>(sampleSize);
    long count = 0;
    while (iterator.hasNext()) {
      float[] v = iterator.next();
      if (v == null) {
        continue;
      }
      if (count < sampleSize) {
        reservoir.add(v);
      } else {
        long j = (long) (random.nextDouble() * (count + 1));
        if (j < sampleSize) {
          reservoir.set((int) j, v);
        }
      }
      count++;
    }
    return reservoir;
  }

  /**
   * Returns an L2-normalized vector copy, or a clone of the original vector if its norm is zero.
   */
  public static float[] l2Normalize(float[] v) {
    double sumSq = 0.0;
    for (float val : v) {
      sumSq += (double) val * val;
    }
    double norm = Math.sqrt(sumSq);
    if (norm == 0.0 || Double.isNaN(norm)) {
      return v.clone();
    }
    float[] out = new float[v.length];
    for (int i = 0; i < v.length; i++) {
      out[i] = (float) (v[i] / norm);
    }
    return out;
  }

  /**
   * Selects initial centroid positions using k-means++ probabilistic seeding based on assignment
   * distance.
   */
  static List<float[]> initializeKMeansPlusPlus(List<float[]> vectors, int k, DistanceMetric metric,
    Random random) {
    int n = vectors.size();
    List<float[]> centroids = new ArrayList<>(k);
    centroids.add(seed(vectors.get(random.nextInt(n)), metric));
    double[] weight = new double[n];
    Arrays.fill(weight, Double.MAX_VALUE);
    for (int c = 1; c < k; c++) {
      float[] last = centroids.get(c - 1);
      double total = 0.0;
      for (int i = 0; i < n; i++) {
        double d = assignmentDistance(metric, vectors.get(i), last);
        // Square cosine distance to scale selection probability
        double w = metric == DistanceMetric.COSINE ? d * d : d;
        if (w < weight[i]) {
          weight[i] = w;
        }
        total += weight[i];
      }
      int selected = n - 1;
      if (total > 1e-12) {
        double r = random.nextDouble() * total;
        double accum = 0.0;
        for (int i = 0; i < n; i++) {
          accum += weight[i];
          if (accum >= r) {
            selected = i;
            break;
          }
        }
      } else {
        selected = random.nextInt(n);
      }
      centroids.add(seed(vectors.get(selected), metric));
    }
    return centroids;
  }

  /** Trains centroid clusters across the provided vector sample. */
  public static KMeansResult train(List<float[]> vectors, int k, KMeansConfig config) {
    if (vectors == null || vectors.isEmpty()) {
      throw new IllegalArgumentException("vectors list must not be null or empty");
    }
    if (k <= 0) {
      throw new IllegalArgumentException("k must be > 0: " + k);
    }
    if (k > vectors.size()) {
      throw new IllegalArgumentException(
        "k (" + k + ") cannot exceed number of vectors (" + vectors.size() + ")");
    }
    int n = vectors.size();
    int dim = vectors.get(0).length;
    for (int i = 0; i < n; i++) {
      if (vectors.get(i) == null || vectors.get(i).length != dim) {
        throw new IllegalArgumentException(
          "Vector at index " + i + " is null or does not have" + " the expected dimension " + dim);
      }
    }
    DistanceMetric metric = config.getDistanceMetric();
    Random random =
      config.getRandomSeed() != null ? new Random(config.getRandomSeed()) : new Random();

    List<float[]> centroids = initializeKMeansPlusPlus(vectors, k, metric, random);
    Lloyd lloyd = new Lloyd(vectors, metric, random);
    List<Double> distortionHistory = new ArrayList<>();
    double prevDistortion = Double.MAX_VALUE;
    boolean converged = false;
    int iteration;
    for (iteration = 1; iteration <= config.getMaxIterations(); iteration++) {
      double distortion = lloyd.iterate(centroids);
      distortionHistory.add(distortion);
      LOGGER.debug("K-means iteration {}: total distortion = {}", iteration, distortion);
      if (
        distortion == 0.0 || (iteration > 1 && prevDistortion > 0.0
          && Math.abs(prevDistortion - distortion) / prevDistortion
              < config.getConvergenceThreshold())
      ) {
        converged = true;
        break;
      }
      prevDistortion = distortion;
    }
    int iterations = Math.min(iteration, config.getMaxIterations());
    LOGGER.info("K-means finished after {} iterations, converged={}, distortion={}", iterations,
      converged, lloyd.distortion);

    ClusterSkewMetrics preSplitSkew = ClusterSkewMetrics.compute(lloyd.clusterSizes);
    ClusterSkewMetrics postSplitSkew = null;
    if (config.isEnableSplitHeuristic() && n >= 2 * k) {
      List<float[]> split = splitOverloaded(vectors, centroids, lloyd,
        config.getSplitThresholdMultiplier() * n / k, metric, random);
      if (split.size() > centroids.size()) {
        centroids = split;
        for (int pass = 0; pass < Math.max(1, config.getSplitRebalanceIterations()); pass++) {
          lloyd.iterate(centroids);
        }
        postSplitSkew = ClusterSkewMetrics.compute(lloyd.clusterSizes);
        LOGGER.info("Split heuristic raised centroid count from {} to {}; CV {} -> {}", k,
          centroids.size(), preSplitSkew.getCoefficientOfVariation(),
          postSplitSkew.getCoefficientOfVariation());
      }
    }
    ClusterSkewMetrics finalSkew = postSplitSkew != null ? postSplitSkew : preSplitSkew;
    if (finalSkew.isSevereSkew()) {
      LOGGER.warn("Severe cluster skew after training: CV = {}",
        finalSkew.getCoefficientOfVariation());
    }
    return new KMeansResult(centroids, k, iterations, converged, lloyd.distortion,
      lloyd.clusterSizes, lloyd.assignments, finalSkew, preSplitSkew, postSplitSkew, dim, metric,
      distortionHistory);
  }

  public static KMeansResult train(List<float[]> vectors, int k) {
    return train(vectors, k, KMeansConfig.defaultConfig());
  }

  /**
   * Splits clusters exceeding the membership threshold into pairs of sub-centroids using k-means++.
   */
  private static List<float[]> splitOverloaded(List<float[]> vectors, List<float[]> centroids,
    Lloyd lloyd, double threshold, DistanceMetric metric, Random random) {
    List<float[]> result = new ArrayList<>(centroids.size());
    boolean split = false;
    for (int c = 0; c < centroids.size(); c++) {
      if (lloyd.clusterSizes[c] > threshold) {
        List<float[]> members = new ArrayList<>(lloyd.clusterSizes[c]);
        for (int i = 0; i < vectors.size(); i++) {
          if (lloyd.assignments[i] == c) {
            members.add(vectors.get(i));
          }
        }
        result.addAll(initializeKMeansPlusPlus(members, 2, metric, random));
        split = true;
      } else {
        result.add(centroids.get(c));
      }
    }
    return split ? result : centroids;
  }

  private static float[] seed(float[] v, DistanceMetric metric) {
    return metric == DistanceMetric.COSINE ? l2Normalize(v) : v.clone();
  }

  /**
   * Manages state and execution for Lloyd iteration passes, including nearest centroid assignment,
   * empty cluster reseeding, and centroid position updates.
   */
  private static final class Lloyd {
    private final List<float[]> vectors;
    private final DistanceMetric metric;
    private final Random random;
    private final int[] assignments;
    private final double[] distances;
    private int[] clusterSizes;
    private double distortion;

    Lloyd(List<float[]> vectors, DistanceMetric metric, Random random) {
      this.vectors = vectors;
      this.metric = metric;
      this.random = random;
      this.assignments = new int[vectors.size()];
      this.distances = new double[vectors.size()];
    }

    /** Executes an assignment and centroid update pass, returning total distortion. */
    double iterate(List<float[]> centroids) {
      int k = centroids.size();
      int n = vectors.size();
      int dim = vectors.get(0).length;
      clusterSizes = new int[k];
      distortion = 0.0;
      for (int i = 0; i < n; i++) {
        float[] v = vectors.get(i);
        int best = 0;
        double bestDist = Double.MAX_VALUE;
        for (int c = 0; c < k; c++) {
          double d = assignmentDistance(metric, v, centroids.get(c));
          if (d < bestDist) {
            bestDist = d;
            best = c;
          }
        }
        assignments[i] = best;
        distances[i] = bestDist;
        clusterSizes[best]++;
        distortion += bestDist;
      }
      for (int c = 0; c < k; c++) {
        if (clusterSizes[c] == 0) {
          reseed(c);
        }
      }
      double[][] sums = new double[k][dim];
      for (int i = 0; i < n; i++) {
        float[] v = vectors.get(i);
        double[] s = sums[assignments[i]];
        for (int d = 0; d < dim; d++) {
          s[d] += v[d];
        }
      }
      for (int c = 0; c < k; c++) {
        if (clusterSizes[c] == 0) {
          continue;
        }
        float[] updated = new float[dim];
        for (int d = 0; d < dim; d++) {
          updated[d] = (float) (sums[c][d] / clusterSizes[c]);
        }
        centroids.set(c, metric == DistanceMetric.COSINE ? l2Normalize(updated) : updated);
      }
      return distortion;
    }

    /**
     * Reseeds an empty cluster by reassigning the vector exhibiting maximum distortion from a
     * cluster with surplus membership.
     */
    private void reseed(int c) {
      int worst = -1;
      double maxDist = -1.0;
      for (int i = 0; i < assignments.length; i++) {
        if (clusterSizes[assignments[i]] > 1 && distances[i] > maxDist) {
          maxDist = distances[i];
          worst = i;
        }
      }
      int idx = worst != -1 ? worst : random.nextInt(assignments.length);
      LOGGER.debug("Cluster {} is empty; re-seeding with vector {}", c, idx);
      clusterSizes[assignments[idx]]--;
      assignments[idx] = c;
      distances[idx] = 0.0;
      clusterSizes[c]++;
    }
  }
}
