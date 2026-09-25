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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.function.VectorDistanceUtil;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.types.PVectorDouble;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client-side k-means clustering trainer for vector centroid calculation. Supports k-means++
 * initialization, metric-aware assignment and centroid updates (L2, spherical k-means for COSINE,
 * and INNER_PRODUCT), empty cluster re-seeding, relative distortion convergence checking,
 * post-training split heuristic for cluster skew mitigation, and base table reservoir sampling.
 */
public final class KMeansTrainer {

  private static final Logger LOGGER = LoggerFactory.getLogger(KMeansTrainer.class);
  private static final long MEMORY_WARNING_THRESHOLD_BYTES = 256L * 1024L * 1024L;
  private static final AtomicInteger LAST_NULL_COUNT = new AtomicInteger(0);

  private KMeansTrainer() {
  }

  /**
   * Samples up to {@code sampleSize} non-null vector rows from the base table using reservoir
   * sampling (Algorithm R).
   */
  public static List<float[]> sampleVectors(Connection conn, String tableName, String vectorCol,
    int sampleSize) throws SQLException {
    if (conn == null) {
      throw new IllegalArgumentException("Connection must not be null");
    }
    if (tableName == null || tableName.trim().isEmpty()) {
      throw new IllegalArgumentException("tableName must not be null or empty");
    }
    if (vectorCol == null || vectorCol.trim().isEmpty()) {
      throw new IllegalArgumentException("vectorCol must not be null or empty");
    }
    if (sampleSize <= 0) {
      throw new IllegalArgumentException("sampleSize must be > 0: " + sampleSize);
    }
    String sql = "SELECT " + vectorCol + " FROM " + tableName;
    try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
      return reservoirSample(rs, 1, sampleSize, new Random());
    }
  }

  /**
   * Reservoir-samples up to {@code sampleSize} non-null vectors from the specified table and
   * column.
   */
  public static List<float[]> sampleVectors(PhoenixConnection conn, String tableName,
    String vectorCol, int sampleSize) throws SQLException {
    return sampleVectors((Connection) conn, tableName, vectorCol, sampleSize);
  }

  /**
   * Reservoir-samples up to {@code sampleSize} non-null vectors from {@code rs} using Algorithm R.
   * Null vectors are skipped and the skipped count is tracked.
   */
  public static List<float[]> reservoirSample(ResultSet rs, int colIndex, int sampleSize,
    Random random) throws SQLException {
    if (rs == null) {
      throw new IllegalArgumentException("ResultSet must not be null");
    }
    if (sampleSize <= 0) {
      throw new IllegalArgumentException("sampleSize must be > 0: " + sampleSize);
    }
    Random rng = (random != null) ? random : new Random();
    List<float[]> reservoir = new ArrayList<>(sampleSize);
    int count = 0;
    int nullsSkipped = 0;

    boolean isDoubleVector = false;
    try {
      String typeName = rs.getMetaData().getColumnTypeName(colIndex);
      if (typeName != null && typeName.toUpperCase(Locale.ROOT).contains("DOUBLE")) {
        isDoubleVector = true;
      }
    } catch (Exception ignored) {
    }

    while (rs.next()) {
      byte[] bytes = rs.getBytes(colIndex);
      if (bytes == null || rs.wasNull()) {
        nullsSkipped++;
        continue;
      }
      float[] v;
      if (isDoubleVector) {
        double[] doubles = PVectorDouble.readElements(bytes, 0, bytes.length);
        v = new float[doubles.length];
        for (int d = 0; d < doubles.length; d++) {
          v[d] = (float) doubles[d];
        }
      } else {
        v = PVectorFloat.readElements(bytes, 0, bytes.length);
      }
      if (count < sampleSize) {
        reservoir.add(v);
      } else {
        int j = rng.nextInt(count + 1);
        if (j < sampleSize) {
          reservoir.set(j, v);
        }
      }
      count++;
    }

    LAST_NULL_COUNT.set(nullsSkipped);
    LOGGER.debug(
      "Reservoir sampling from ResultSet completed: sampled {} vectors from {} non-nulls, skipped {} nulls",
      reservoir.size(), count, nullsSkipped);
    return reservoir;
  }

  /**
   * Reservoir-samples up to {@code sampleSize} non-null vectors from an iterator using Algorithm R.
   */
  public static List<float[]> reservoirSample(Iterator<float[]> iterator, int sampleSize,
    Random random) {
    if (iterator == null) {
      throw new IllegalArgumentException("Iterator must not be null");
    }
    if (sampleSize <= 0) {
      throw new IllegalArgumentException("sampleSize must be > 0: " + sampleSize);
    }
    Random rng = (random != null) ? random : new Random();
    List<float[]> reservoir = new ArrayList<>(sampleSize);
    int count = 0;
    int nullsSkipped = 0;

    while (iterator.hasNext()) {
      float[] v = iterator.next();
      if (v == null) {
        nullsSkipped++;
        continue;
      }
      if (count < sampleSize) {
        reservoir.add(v);
      } else {
        int j = rng.nextInt(count + 1);
        if (j < sampleSize) {
          reservoir.set(j, v);
        }
      }
      count++;
    }

    LAST_NULL_COUNT.set(nullsSkipped);
    LOGGER.debug(
      "Reservoir sampling from Iterator completed: sampled {} vectors from {} non-nulls, skipped {} nulls",
      reservoir.size(), count, nullsSkipped);
    return reservoir;
  }

  /**
   * Returns the number of null vectors skipped during the most recent reservoir sampling execution.
   */
  public static int getLastNullCount() {
    return LAST_NULL_COUNT.get();
  }

  /**
   * Alias for {@link #getLastNullCount()}.
   */
  public static int getLastNullsSkipped() {
    return LAST_NULL_COUNT.get();
  }

  /**
   * Normalizes a vector to unit length (L2 norm = 1.0). If norm is zero or NaN, returns a clone.
   */
  public static float[] l2Normalize(float[] v) {
    if (v == null) {
      return null;
    }
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
   * Computes the assignment distance between vector {@code a} and centroid {@code b} using the
   * configured distance metric. For L2 and INNER_PRODUCT, returns squared Euclidean distance. For
   * COSINE, returns cosine distance.
   */
  private static double computeAssignmentDistance(float[] a, float[] b, int dim, String metric) {
    if ("COSINE".equals(metric)) {
      return VectorDistanceUtil.scalarCosineDistance(a, b, dim);
    }
    return VectorDistanceUtil.scalarL2DistanceSquared(a, b, dim);
  }

  /**
   * Initializes {@code k} centroids using k-means++ initialization with metric-aware D² weighting.
   */
  public static List<float[]> initializeKMeansPlusPlus(List<float[]> vectors, int k, String metric,
    Random random) {
    int n = vectors.size();
    int dim = vectors.get(0).length;
    List<float[]> centroids = new ArrayList<>(k);

    // Select the initial centroid uniformly at random, then sample remaining centroids with
    // probability proportional to squared distance from the nearest existing centroid.
    int firstIdx = random.nextInt(n);
    float[] firstCentroid = vectors.get(firstIdx).clone();
    if ("COSINE".equals(metric)) {
      firstCentroid = l2Normalize(firstCentroid);
    }
    centroids.add(firstCentroid);

    if (k == 1) {
      return centroids;
    }

    double[] minDistanceSq = new double[n];
    for (int i = 0; i < n; i++) {
      double dist = computeAssignmentDistance(vectors.get(i), firstCentroid, dim, metric);
      minDistanceSq[i] = "COSINE".equals(metric) ? (dist * dist) : dist;
    }

    for (int c = 1; c < k; c++) {
      double totalWeight = 0.0;
      for (int i = 0; i < n; i++) {
        totalWeight += minDistanceSq[i];
      }

      int selectedIdx = -1;
      if (totalWeight > 1e-12) {
        double r = random.nextDouble() * totalWeight;
        double accum = 0.0;
        for (int i = 0; i < n; i++) {
          accum += minDistanceSq[i];
          if (accum >= r) {
            selectedIdx = i;
            break;
          }
        }
        if (selectedIdx == -1) {
          selectedIdx = n - 1;
        }
      } else {
        selectedIdx = random.nextInt(n);
      }

      float[] nextCentroid = vectors.get(selectedIdx).clone();
      if ("COSINE".equals(metric)) {
        nextCentroid = l2Normalize(nextCentroid);
      }
      centroids.add(nextCentroid);

      if (c < k - 1) {
        for (int i = 0; i < n; i++) {
          double dist = computeAssignmentDistance(vectors.get(i), nextCentroid, dim, metric);
          double distSq = "COSINE".equals(metric) ? (dist * dist) : dist;
          if (distSq < minDistanceSq[i]) {
            minDistanceSq[i] = distSq;
          }
        }
      }
    }

    return centroids;
  }

  /**
   * Trains k-means centroids on the given input vectors with the specified configuration.
   * @param vectors non-null, non-empty list of float vectors of identical dimension
   * @param k       number of centroids to initialize; must satisfy {@code 1 <= k <= vectors.size()}
   * @param config  training configuration parameters
   * @return {@link KMeansResult} containing centroids and detailed diagnostics
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
    if (config == null) {
      config = KMeansConfig.defaultConfig();
    }

    int n = vectors.size();
    float[] v0 = vectors.get(0);
    if (v0 == null) {
      throw new IllegalArgumentException("Vector at index 0 must not be null");
    }
    int dim = v0.length;
    if (dim <= 0) {
      throw new IllegalArgumentException("Vector dimension must be > 0: " + dim);
    }
    for (int i = 1; i < n; i++) {
      float[] v = vectors.get(i);
      if (v == null) {
        throw new IllegalArgumentException("Vector at index " + i + " must not be null");
      }
      if (v.length != dim) {
        throw new IllegalArgumentException(
          "Mismatched vector dimension at index " + i + ": expected " + dim + ", got " + v.length);
      }
    }

    String metric = config.getDistanceMetric().trim().toUpperCase(Locale.ROOT);
    if (!"L2".equals(metric) && !"COSINE".equals(metric) && !"INNER_PRODUCT".equals(metric)) {
      throw new IllegalArgumentException("Unknown distance metric: " + config.getDistanceMetric());
    }

    long memoryBytes = (long) n * dim * Bytes.SIZEOF_FLOAT;
    if (memoryBytes > MEMORY_WARNING_THRESHOLD_BYTES) {
      LOGGER.warn("Training dataset size ({} vectors, dimension {}) is {} bytes (> 256 MB). "
        + "Consider using async index creation.", n, dim, memoryBytes);
    }

    Random random =
      config.getRandomSeed() != null ? new Random(config.getRandomSeed()) : new Random();

    List<float[]> centroids = initializeKMeansPlusPlus(vectors, k, metric, random);

    int[] assignments = new int[n];
    int[] clusterSizes = new int[k];
    double[] pointDistances = new double[n];
    // Pre-allocated coordinate accumulators reused across iterations to bound allocation overhead
    double[][] partialSums = new double[k][dim];
    List<Double> distortionHistory = new ArrayList<>();

    double prevDistortion = Double.MAX_VALUE;
    double currDistortion = 0.0;
    boolean converged = false;
    int iteration = 0;

    for (iteration = 1; iteration <= config.getMaxIterations(); iteration++) {
      currDistortion = 0.0;
      Arrays.fill(clusterSizes, 0);
      for (int c = 0; c < k; c++) {
        Arrays.fill(partialSums[c], 0.0);
      }

      // Assign vectors to their nearest centroid and accumulate coordinate partial sums
      for (int i = 0; i < n; i++) {
        float[] v = vectors.get(i);
        double bestDist = Double.MAX_VALUE;
        int bestC = 0;
        for (int c = 0; c < k; c++) {
          double d = computeAssignmentDistance(v, centroids.get(c), dim, metric);
          if (d < bestDist) {
            bestDist = d;
            bestC = c;
          }
        }
        assignments[i] = bestC;
        clusterSizes[bestC]++;
        pointDistances[i] = bestDist;
        currDistortion += bestDist;
        double[] sums = partialSums[bestC];
        for (int d = 0; d < dim; d++) {
          sums[d] += v[d];
        }
      }

      distortionHistory.add(currDistortion);
      LOGGER.info("K-Means iteration {}: total distortion = {}", iteration, currDistortion);

      // Re-seed empty clusters using the vector exhibiting maximum residual distortion
      for (int c = 0; c < k; c++) {
        if (clusterSizes[c] == 0) {
          int worstIdx = -1;
          double maxDist = -1.0;
          for (int i = 0; i < n; i++) {
            int assignedClust = assignments[i];
            if (clusterSizes[assignedClust] > 1 && pointDistances[i] > maxDist) {
              maxDist = pointDistances[i];
              worstIdx = i;
            }
          }
          if (worstIdx != -1) {
            LOGGER.warn(
              "Cluster {} is empty after assignment step. Re-seeding with worst-fit vector at index {} (distance = {})",
              c, worstIdx, maxDist);
            float[] newCentroid = vectors.get(worstIdx).clone();
            if ("COSINE".equals(metric)) {
              newCentroid = l2Normalize(newCentroid);
            }
            centroids.set(c, newCentroid);
            int prevClust = assignments[worstIdx];
            clusterSizes[prevClust]--;
            float[] v = vectors.get(worstIdx);
            for (int d = 0; d < dim; d++) {
              partialSums[prevClust][d] -= v[d];
              partialSums[c][d] = v[d];
            }
            assignments[worstIdx] = c;
            clusterSizes[c] = 1;
            pointDistances[worstIdx] = 0.0;
          } else {
            int randIdx = random.nextInt(n);
            LOGGER.warn(
              "Cluster {} is empty after assignment step. Re-seeding with random vector at index {}",
              c, randIdx);
            float[] newCentroid = vectors.get(randIdx).clone();
            if ("COSINE".equals(metric)) {
              newCentroid = l2Normalize(newCentroid);
            }
            centroids.set(c, newCentroid);
            float[] v = vectors.get(randIdx);
            for (int d = 0; d < dim; d++) {
              partialSums[c][d] = v[d];
            }
            clusterSizes[c] = 1;
          }
        }
      }

      // Recompute centroids from accumulated sums and evaluate convergence
      for (int c = 0; c < k; c++) {
        if (clusterSizes[c] > 0) {
          float[] updated = new float[dim];
          double[] sums = partialSums[c];
          double count = clusterSizes[c];
          for (int d = 0; d < dim; d++) {
            updated[d] = (float) (sums[d] / count);
          }
          if ("COSINE".equals(metric)) {
            updated = l2Normalize(updated);
          }
          centroids.set(c, updated);
        }
      }

      if (iteration > 1) {
        if (prevDistortion > 0.0) {
          double relChange = Math.abs(prevDistortion - currDistortion) / prevDistortion;
          if (relChange < config.getConvergenceThreshold()) {
            converged = true;
            LOGGER.info("K-Means converged at iteration {} with relative distortion change {}",
              iteration, relChange);
            break;
          }
        } else if (currDistortion == 0.0) {
          converged = true;
          LOGGER.info("K-Means converged at iteration {} with 0 distortion", iteration);
          break;
        }
      } else if (currDistortion == 0.0) {
        converged = true;
        LOGGER.info("K-Means converged at iteration 1 with 0 distortion");
        break;
      }

      prevDistortion = currDistortion;
    }

    int finalIterations = Math.min(iteration, config.getMaxIterations());

    // Mitigate partition skew by splitting clusters exceeding the size threshold, followed by
    // rebalancing
    ClusterSkewMetrics preSplitSkew = ClusterSkewMetrics.compute(clusterSizes);
    ClusterSkewMetrics postSplitSkew = null;
    boolean hasSplit = false;

    if (config.isEnableSplitHeuristic() && n >= 2 * k) {
      if (preSplitSkew.isSevereSkew()) {
        LOGGER.warn("Severe cluster skew detected before split pass: CV = {} > 1.0",
          preSplitSkew.getCoefficientOfVariation());
      }

      double threshold = config.getSplitThresholdMultiplier() * ((double) n / k);
      Set<Integer> overloaded = new HashSet<>();
      for (int c = 0; c < k; c++) {
        if (clusterSizes[c] > threshold) {
          overloaded.add(c);
        }
      }

      if (!overloaded.isEmpty()) {
        hasSplit = true;
        List<float[]> newCentroidList = new ArrayList<>();
        for (int c = 0; c < k; c++) {
          if (overloaded.contains(c)) {
            List<float[]> clusterVecs = new ArrayList<>(clusterSizes[c]);
            for (int i = 0; i < n; i++) {
              if (assignments[i] == c) {
                clusterVecs.add(vectors.get(i));
              }
            }
            List<float[]> subCentroids = initializeKMeansPlusPlus(clusterVecs, 2, metric, random);
            newCentroidList.add(subCentroids.get(0));
            newCentroidList.add(subCentroids.get(1));
          } else {
            newCentroidList.add(centroids.get(c));
          }
        }

        centroids = newCentroidList;
        int effectiveK = centroids.size();
        clusterSizes = new int[effectiveK];
        double[][] splitSums = new double[effectiveK][dim];

        int rebalancePasses = Math.max(1, config.getSplitRebalanceIterations());
        for (int pass = 0; pass < rebalancePasses; pass++) {
          Arrays.fill(clusterSizes, 0);
          for (int c = 0; c < effectiveK; c++) {
            Arrays.fill(splitSums[c], 0.0);
          }
          currDistortion = 0.0;
          for (int i = 0; i < n; i++) {
            float[] v = vectors.get(i);
            double bestDist = Double.MAX_VALUE;
            int bestC = 0;
            for (int c = 0; c < effectiveK; c++) {
              double d = computeAssignmentDistance(v, centroids.get(c), dim, metric);
              if (d < bestDist) {
                bestDist = d;
                bestC = c;
              }
            }
            assignments[i] = bestC;
            clusterSizes[bestC]++;
            currDistortion += bestDist;
            double[] sums = splitSums[bestC];
            for (int d = 0; d < dim; d++) {
              sums[d] += v[d];
            }
          }
          for (int c = 0; c < effectiveK; c++) {
            if (clusterSizes[c] > 0) {
              float[] updated = new float[dim];
              double count = clusterSizes[c];
              double[] sums = splitSums[c];
              for (int d = 0; d < dim; d++) {
                updated[d] = (float) (sums[d] / count);
              }
              if ("COSINE".equals(metric)) {
                updated = l2Normalize(updated);
              }
              centroids.set(c, updated);
            }
          }
        }

        postSplitSkew = ClusterSkewMetrics.compute(clusterSizes);
        double cvReduction =
          preSplitSkew.getCoefficientOfVariation() - postSplitSkew.getCoefficientOfVariation();
        LOGGER.info(
          "Cluster skew reduction achieved: pre-split CV = {}, post-split CV = {}, reduction = {}",
          preSplitSkew.getCoefficientOfVariation(), postSplitSkew.getCoefficientOfVariation(),
          cvReduction);
      }
    }

    ClusterSkewMetrics finalSkew = (postSplitSkew != null) ? postSplitSkew : preSplitSkew;

    return new KMeansResult(centroids, k, finalIterations, converged, currDistortion, clusterSizes,
      assignments, finalSkew, preSplitSkew, postSplitSkew, hasSplit, dim, metric,
      distortionHistory);
  }

  public static KMeansResult train(List<float[]> vectors, int k) {
    return train(vectors, k, KMeansConfig.defaultConfig());
  }

  public static KMeansResult train(List<float[]> vectors, int k, boolean enableSplitHeuristic) {
    return train(vectors, k,
      KMeansConfig.newBuilder().enableSplitHeuristic(enableSplitHeuristic).build());
  }

  public static KMeansResult train(float[][] vectors, int k) {
    if (vectors == null) {
      throw new IllegalArgumentException("vectors array must not be null");
    }
    return train(Arrays.asList(vectors), k, KMeansConfig.defaultConfig());
  }

  public static KMeansResult train(float[][] vectors, int k, KMeansConfig config) {
    if (vectors == null) {
      throw new IllegalArgumentException("vectors array must not be null");
    }
    return train(Arrays.asList(vectors), k, config);
  }

  public static KMeansResult train(float[][] vectors, int k, boolean enableSplitHeuristic) {
    if (vectors == null) {
      throw new IllegalArgumentException("vectors array must not be null");
    }
    return train(Arrays.asList(vectors), k, enableSplitHeuristic);
  }

  public static KMeansResult trainPacked(List<byte[]> packedVectors, int dim, int k) {
    return trainPacked(packedVectors, dim, k, KMeansConfig.defaultConfig());
  }

  public static KMeansResult trainPacked(List<byte[]> packedVectors, int dim, int k,
    KMeansConfig config) {
    if (packedVectors == null || packedVectors.isEmpty()) {
      throw new IllegalArgumentException("packedVectors must not be null or empty");
    }
    if (dim <= 0) {
      throw new IllegalArgumentException("dim must be > 0: " + dim);
    }
    int expectedBytes = dim * Bytes.SIZEOF_FLOAT;
    List<float[]> vectors = new ArrayList<>(packedVectors.size());
    for (int i = 0; i < packedVectors.size(); i++) {
      byte[] b = packedVectors.get(i);
      if (b == null || b.length != expectedBytes) {
        throw new IllegalArgumentException("Invalid packed vector length at index " + i
          + ": expected " + expectedBytes + " bytes, got " + (b == null ? "null" : b.length));
      }
      vectors.add(PVectorFloat.readElements(b, 0, b.length));
    }
    return train(vectors, k, config);
  }

  public static ClusterSkewMetrics computeSkewMetrics(int[] clusterSizes) {
    return ClusterSkewMetrics.compute(clusterSizes);
  }
}
