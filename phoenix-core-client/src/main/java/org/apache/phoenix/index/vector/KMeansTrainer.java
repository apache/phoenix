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
 * Training starts from k-means++ seeds and runs Lloyd iterations. Under L2 and INNER_PRODUCT, each
 * centroid moves to the mean of its members. Under COSINE, training uses spherical k-means and
 * scales each centroid to unit length. Training reseeds empty clusters, and stops when the relative
 * change in distortion is below the threshold. After training, it can split overloaded clusters to
 * balance the posting lists. Training, index maintenance and query probes all use
 * {@link #assignmentDistance}, so they route a vector to the same centroid.
 */
public final class KMeansTrainer {

  private static final Logger LOGGER = LoggerFactory.getLogger(KMeansTrainer.class);

  private KMeansTrainer() {
  }

  /**
   * Returns the distance that assigns a vector to a centroid. COSINE indexes use cosine distance.
   * L2 and INNER_PRODUCT indexes use squared Euclidean distance.
   */
  public static double assignmentDistance(DistanceMetric metric, float[] a, float[] b) {
    if (metric == DistanceMetric.COSINE) {
      return VectorDistanceUtil.scalarCosineDistance(a, b, a.length);
    }
    return VectorDistanceUtil.scalarL2DistanceSquared(a, b, a.length);
  }

  /**
   * Returns a uniform sample of at most {@code sampleSize} vectors, by reservoir sampling. The
   * sample skips null entries. {@code sampleSize} must be positive.
   */
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
   * Returns a copy of the vector scaled to unit length. If the norm is zero or NaN, it returns an
   * unchanged copy.
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
   * Selects {@code k} initial centroids by k-means++ seeding with the assignment distance. Under
   * COSINE, each seed has unit length.
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
        // The L2 distance is already squared. Square the cosine distance, because k-means++
        // selects a seed with probability in proportion to the squared distance.
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

  /**
   * Trains {@code k} centroids on the vector sample, then splits overloaded clusters if the
   * configuration enables the split. All vectors must be non-null and have the same dimension.
   * {@code k} must be positive and not more than the sample size.
   */
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
   * Replaces each cluster with more members than {@code threshold} by two sub-centroids, which
   * k-means++ seeds from its members. Returns {@code centroids} if no cluster is over the
   * threshold.
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
   * State of the Lloyd iterations: the cluster of each vector, its distance and the cluster sizes.
   * Each pass assigns vectors, reseeds empty clusters and updates the centroids.
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

    /**
     * Runs one pass. It assigns each vector to its nearest centroid, reseeds empty clusters, and
     * moves each centroid to the mean of its members. Returns the total distortion of the
     * assignment.
     */
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
     * Fills an empty cluster with the farthest vector of a cluster that has more than one member.
     * If no such vector exists, it uses a random vector. The update step then puts the centroid on
     * that vector.
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
