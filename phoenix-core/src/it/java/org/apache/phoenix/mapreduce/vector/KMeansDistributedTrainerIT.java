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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.end2end.BaseOwnClusterIT;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.index.vector.KMeansConfig;
import org.apache.phoenix.index.vector.KMeansResult;
import org.apache.phoenix.index.vector.KMeansTrainer;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Integration tests for the distributed MapReduce K-Means training pipeline. */
@Category(NeedsOwnMiniClusterTest.class)
public class KMeansDistributedTrainerIT extends BaseOwnClusterIT {

  private static Connection conn;

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    setUpTestDriver(ReadOnlyProps.EMPTY_PROPS, ReadOnlyProps.EMPTY_PROPS);
    conn = DriverManager.getConnection(getUrl());
  }

  private static void insertVectors(Connection conn, String tableName, List<float[]> vectors)
    throws SQLException {
    String sql = "UPSERT INTO " + tableName + " VALUES (?, ?)";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int i = 0; i < vectors.size(); i++) {
        float[] v = vectors.get(i);
        ps.setInt(1, i);
        Float[] boxed = new Float[v.length];
        for (int d = 0; d < v.length; d++) {
          boxed[d] = v[d];
        }
        Array arr = conn.createArrayOf("FLOAT", boxed);
        ps.setArray(2, arr);
        ps.addBatch();
        if ((i + 1) % 500 == 0) {
          ps.executeBatch();
          conn.commit();
        }
      }
      ps.executeBatch();
      conn.commit();
    }
  }

  private static double l2Distance(float[] a, float[] b) {
    double sum = 0.0;
    for (int i = 0; i < a.length; i++) {
      double diff = a[i] - b[i];
      sum += diff * diff;
    }
    return Math.sqrt(sum);
  }

  @Test
  public void testDistributedClusterRecovery() throws Exception {
    String tableName = generateUniqueName();
    conn.createStatement().execute(
      "CREATE TABLE " + tableName + " (ID INTEGER NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4))");

    float[][] centers = new float[][] { { 10.0f, 0.0f, 0.0f, 0.0f }, { 0.0f, 10.0f, 0.0f, 0.0f },
      { -10.0f, 0.0f, 0.0f, 0.0f }, { 0.0f, -10.0f, 0.0f, 0.0f } };

    Random rng = new Random(42);
    List<float[]> vectors = new ArrayList<>(2000);
    for (int c = 0; c < 4; c++) {
      for (int i = 0; i < 500; i++) {
        float[] v = new float[4];
        for (int d = 0; d < 4; d++) {
          v[d] = centers[c][d] + (float) (rng.nextGaussian() * 0.5);
        }
        vectors.add(v);
      }
    }
    insertVectors(conn, tableName, vectors);

    Configuration conf = new Configuration(getUtility().getConfiguration());
    KMeansConfig config =
      KMeansConfig.newBuilder().maxIterations(50).randomSeed(42L).sampleSize(2000).build();

    KMeansResult result = KMeansTool.trainDistributed(conf, tableName, "V", 4, 4, config);
    assertNotNull(result);
    assertTrue(result.getEffectiveK() >= 4);

    Set<Integer> matchedCenters = new HashSet<>();
    for (float[] centroid : result.getCentroids()) {
      int bestCenter = -1;
      double minD = Double.MAX_VALUE;
      for (int c = 0; c < 4; c++) {
        double d = l2Distance(centroid, centers[c]);
        if (d < minD) {
          minD = d;
          bestCenter = c;
        }
      }
      if (minD < 2.0) {
        matchedCenters.add(bestCenter);
      }
    }
    assertEquals("All 4 clusters must be recovered by distributed trainer", 4,
      matchedCenters.size());

    // Validate that distributed clustering output matches the client-side trainer.
    KMeansResult clientResult = KMeansTrainer.train(vectors, 4, config);
    assertEquals(4, clientResult.getEffectiveK());
  }

  @Test
  public void testSamplingCorrectness() throws Exception {
    String tableName = generateUniqueName();
    conn.createStatement().execute(
      "CREATE TABLE " + tableName + " (ID INTEGER NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4))");

    Random rng = new Random(100);
    int n = 10000;
    double[] popSum = new double[4];
    List<float[]> vectors = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      float[] v = new float[4];
      for (int d = 0; d < 4; d++) {
        v[d] = rng.nextFloat() * 10.0f;
        popSum[d] += v[d];
      }
      vectors.add(v);
    }
    insertVectors(conn, tableName, vectors);

    double[] popMean = new double[4];
    for (int d = 0; d < 4; d++) {
      popMean[d] = popSum[d] / n;
    }

    Configuration conf = new Configuration(getUtility().getConfiguration());
    PhoenixConfigurationUtil.setKMeansTableName(conf, tableName);
    PhoenixConfigurationUtil.setKMeansVectorColumn(conf, "V");
    PhoenixConfigurationUtil.setKMeansDimension(conf, 4);
    PhoenixConfigurationUtil.setKMeansSampleSize(conf, 500);
    PhoenixConfigurationUtil.setKMeansRandomSeed(conf, 12345L);

    Path sampleDir = new Path("/tmp/sample_correctness_" + UUID.randomUUID().toString());
    Job job = KMeansDistributedSampler.createSamplingJob(conf, sampleDir);
    assertTrue(job.waitForCompletion(true));

    List<float[]> sampled = KMeansDistributedSampler.readSampledVectors(conf, sampleDir);
    assertEquals(500, sampled.size());

    double[] sampleSum = new double[4];
    for (float[] v : sampled) {
      for (int d = 0; d < 4; d++) {
        sampleSum[d] += v[d];
      }
    }
    for (int d = 0; d < 4; d++) {
      double sampleMean = sampleSum[d] / sampled.size();
      // Verify sample mean is within statistical bounds of the population mean (within ~2 standard
      // deviations)
      assertTrue("Sample mean for dim " + d + " (" + sampleMean + ") is too far from population ("
        + popMean[d] + ")", Math.abs(sampleMean - popMean[d]) < 1.0);
    }
  }

  @Test
  public void testConvergenceParity() throws Exception {
    String tableName = generateUniqueName();
    conn.createStatement().execute(
      "CREATE TABLE " + tableName + " (ID INTEGER NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4))");

    float[][] centers = new float[][] { { 5.0f, 0.0f, 0.0f, 0.0f }, { 0.0f, 5.0f, 0.0f, 0.0f },
      { -5.0f, 0.0f, 0.0f, 0.0f }, { 0.0f, -5.0f, 0.0f, 0.0f } };

    Random rng = new Random(77);
    List<float[]> vectors = new ArrayList<>(1000);
    for (int c = 0; c < 4; c++) {
      for (int i = 0; i < 250; i++) {
        float[] v = new float[4];
        for (int d = 0; d < 4; d++) {
          v[d] = centers[c][d] + (float) (rng.nextGaussian() * 0.4);
        }
        vectors.add(v);
      }
    }
    insertVectors(conn, tableName, vectors);

    Configuration conf = new Configuration(getUtility().getConfiguration());
    KMeansConfig config =
      KMeansConfig.newBuilder().maxIterations(50).randomSeed(999L).sampleSize(1000).build();

    KMeansResult mrResult = KMeansTool.trainDistributed(conf, tableName, "V", 4, 4, config);
    KMeansResult clientResult = KMeansTrainer.train(vectors, 4, config);

    int iterDiff = Math.abs(mrResult.getIterations() - clientResult.getIterations());
    assertTrue("Iteration difference should be within ±5, got MR: " + mrResult.getIterations()
      + ", client: " + clientResult.getIterations(), iterDiff <= 5);

    double relDistDiff = Math.abs(mrResult.getFinalDistortion() - clientResult.getFinalDistortion())
      / clientResult.getFinalDistortion();
    assertTrue("Distortion relative difference should be within 15%, got " + relDistDiff,
      relDistDiff < 0.15);
  }

  @Test
  public void testCosineMetricDistributedTraining() throws Exception {
    String tableName = generateUniqueName();
    conn.createStatement().execute(
      "CREATE TABLE " + tableName + " (ID INTEGER NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4))");

    float[][] basis = new float[][] { { 1.0f, 0.0f, 0.0f, 0.0f }, { 0.0f, 1.0f, 0.0f, 0.0f },
      { 0.0f, 0.0f, 1.0f, 0.0f }, { 0.0f, 0.0f, 0.0f, 1.0f } };

    Random rng = new Random(55);
    List<float[]> vectors = new ArrayList<>(1000);
    for (int c = 0; c < 4; c++) {
      for (int i = 0; i < 250; i++) {
        float[] v = new float[4];
        for (int d = 0; d < 4; d++) {
          v[d] = basis[c][d] + (float) (rng.nextGaussian() * 0.05);
        }
        v = KMeansTrainer.l2Normalize(v);
        vectors.add(v);
      }
    }
    insertVectors(conn, tableName, vectors);

    Configuration conf = new Configuration(getUtility().getConfiguration());
    KMeansConfig config = KMeansConfig.newBuilder().distanceMetric("COSINE").maxIterations(50)
      .randomSeed(42L).sampleSize(1000).build();

    KMeansResult result = KMeansTool.trainDistributed(conf, tableName, "V", 4, 4, config);
    assertEquals(4, result.getEffectiveK());

    for (float[] centroid : result.getCentroids()) {
      double norm = 0.0;
      for (float val : centroid) {
        norm += val * val;
      }
      norm = Math.sqrt(norm);
      assertEquals("Centroid must be unit-normalized for COSINE metric", 1.0, norm, 1e-4);

      double minCosineDist = Double.MAX_VALUE;
      for (int c = 0; c < 4; c++) {
        double dist = KMeansIterationDriver.computeDistance(centroid, basis[c], 4, "COSINE");
        if (dist < minCosineDist) {
          minCosineDist = dist;
        }
      }
      assertTrue("Centroid cosine distance to nearest cluster must be < 0.05, got " + minCosineDist,
        minCosineDist < 0.05);
    }
  }

  @Test
  public void testLargeKScalability() throws Exception {
    String tableName = generateUniqueName();
    conn.createStatement().execute(
      "CREATE TABLE " + tableName + " (ID INTEGER NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 8))");

    Random rng = new Random(88);
    List<float[]> vectors = new ArrayList<>(1000);
    for (int i = 0; i < 1000; i++) {
      float[] v = new float[8];
      for (int d = 0; d < 8; d++) {
        v[d] = rng.nextFloat() * 50.0f;
      }
      vectors.add(v);
    }
    insertVectors(conn, tableName, vectors);

    Configuration conf = new Configuration(getUtility().getConfiguration());
    // Set parallelInitThreshold below k to exercise the scalable k-means|| initialization path
    KMeansConfig config = KMeansConfig.newBuilder().maxIterations(10).parallelInitThreshold(32)
      .sampleSize(1000).randomSeed(42L).build();

    KMeansResult result = KMeansTool.trainDistributed(conf, tableName, "V", 8, 64, config);
    assertNotNull(result);
    assertTrue("Should produce at least 64 centroids", result.getEffectiveK() >= 64);
  }

  @Test
  public void testFaultTolerance() throws Exception {
    String tableName = generateUniqueName();
    conn.createStatement().execute(
      "CREATE TABLE " + tableName + " (ID INTEGER NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4))");

    float[][] centers = new float[][] { { 10.0f, 0.0f, 0.0f, 0.0f }, { 0.0f, 10.0f, 0.0f, 0.0f },
      { -10.0f, 0.0f, 0.0f, 0.0f }, { 0.0f, -10.0f, 0.0f, 0.0f } };

    Random rng = new Random(123);
    List<float[]> vectors = new ArrayList<>(400);
    for (int c = 0; c < 4; c++) {
      for (int i = 0; i < 100; i++) {
        float[] v = new float[4];
        for (int d = 0; d < 4; d++) {
          v[d] = centers[c][d] + (float) (rng.nextGaussian() * 0.5);
        }
        vectors.add(v);
      }
    }
    insertVectors(conn, tableName, vectors);

    AtomicBoolean hookFired = new AtomicBoolean(false);
    KMeansIterationDriver.setIterationFailureHook(() -> {
      hookFired.set(true);
      throw new RuntimeException("Simulated mid-iteration mapper/reducer failure");
    });

    Configuration conf = new Configuration(getUtility().getConfiguration());
    KMeansConfig config =
      KMeansConfig.newBuilder().maxIterations(10).randomSeed(42L).sampleSize(400).build();

    KMeansResult result = KMeansTool.trainDistributed(conf, tableName, "V", 4, 4, config);
    assertTrue("Failure hook must have fired during iteration", hookFired.get());
    assertNotNull(result);
    assertTrue("Training should succeed after retry", result.getEffectiveK() >= 4);
  }

  @Test
  public void testWorkConservingRestart() throws Exception {
    String tableName = generateUniqueName();
    conn.createStatement().execute(
      "CREATE TABLE " + tableName + " (ID INTEGER NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4))");

    float[][] centers = new float[][] { { 10.0f, 0.0f, 0.0f, 0.0f }, { 0.0f, 10.0f, 0.0f, 0.0f },
      { -10.0f, 0.0f, 0.0f, 0.0f }, { 0.0f, -10.0f, 0.0f, 0.0f } };

    Random rng = new Random(123);
    List<float[]> vectors = new ArrayList<>(1000);
    for (int c = 0; c < 4; c++) {
      for (int i = 0; i < 250; i++) {
        float[] v = new float[4];
        for (int d = 0; d < 4; d++) {
          v[d] = centers[c][d] + (float) (rng.nextGaussian() * 2.0);
        }
        vectors.add(v);
      }
    }
    insertVectors(conn, tableName, vectors);

    Path workDir = new Path("/tmp/kmeans_restart_" + UUID.randomUUID().toString());
    Configuration conf = new Configuration(getUtility().getConfiguration());
    PhoenixConfigurationUtil.setKMeansHdfsWorkDir(conf, workDir.toString());

    // Execute an initial partial run to persist intermediate iteration checkpoints prior to
    // convergence.
    KMeansConfig config1 =
      KMeansConfig.newBuilder().maxIterations(3).randomSeed(123L).sampleSize(1000).build();

    KMeansResult result1 = KMeansTool.trainDistributed(conf, tableName, "V", 4, 4, config1);
    assertNotNull(result1);
    assertEquals("First run should have run exactly 3 iterations", 3, result1.getIterations());

    org.apache.hadoop.fs.FileSystem fs = workDir.getFileSystem(conf);
    assertTrue("iter_3 checkpoint must exist", fs.exists(new Path(workDir, "centroids/iter_3")));

    // Resume execution from the checkpointed working directory and run to convergence.
    KMeansConfig config2 =
      KMeansConfig.newBuilder().maxIterations(50).randomSeed(123L).sampleSize(1000).build();

    KMeansResult result2 = KMeansTool.trainDistributed(conf, tableName, "V", 4, 4, config2);
    assertNotNull(result2);
    assertTrue("Second run should have resumed and continued past iteration 3",
      result2.getIterations() >= 4);
    assertTrue("Should produce at least 4 centroids", result2.getEffectiveK() >= 4);

    Set<Integer> matchedCenters = new HashSet<>();
    for (float[] centroid : result2.getCentroids()) {
      int bestCenter = -1;
      double minD = Double.MAX_VALUE;
      for (int c = 0; c < 4; c++) {
        double d = l2Distance(centroid, centers[c]);
        if (d < minD) {
          minD = d;
          bestCenter = c;
        }
      }
      if (minD < 3.0) {
        matchedCenters.add(bestCenter);
      }
    }
    assertEquals("All 4 clusters must be recovered on resume", 4, matchedCenters.size());
  }

  @Test
  public void testCompletedTrainingReturnsImmediately() throws Exception {
    String tableName = generateUniqueName();
    conn.createStatement().execute(
      "CREATE TABLE " + tableName + " (ID INTEGER NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4))");

    float[][] centers = new float[][] { { 10.0f, 0.0f, 0.0f, 0.0f }, { 0.0f, 10.0f, 0.0f, 0.0f },
      { -10.0f, 0.0f, 0.0f, 0.0f }, { 0.0f, -10.0f, 0.0f, 0.0f } };

    Random rng = new Random(42);
    List<float[]> vectors = new ArrayList<>(1000);
    for (int c = 0; c < 4; c++) {
      for (int i = 0; i < 250; i++) {
        float[] v = new float[4];
        for (int d = 0; d < 4; d++) {
          v[d] = centers[c][d] + (float) (rng.nextGaussian() * 0.5);
        }
        vectors.add(v);
      }
    }
    insertVectors(conn, tableName, vectors);

    Path workDir = new Path("/tmp/kmeans_complete_" + UUID.randomUUID().toString());
    Configuration conf = new Configuration(getUtility().getConfiguration());
    PhoenixConfigurationUtil.setKMeansHdfsWorkDir(conf, workDir.toString());

    KMeansConfig config =
      KMeansConfig.newBuilder().maxIterations(50).randomSeed(42L).sampleSize(1000).build();

    KMeansResult result1 = KMeansTool.trainDistributed(conf, tableName, "V", 4, 4, config);
    assertNotNull(result1);
    assertTrue(result1.isConverged());

    // Verify completion marker persistence
    org.apache.hadoop.fs.FileSystem fs = workDir.getFileSystem(conf);
    assertTrue("_complete marker must exist", fs.exists(new Path(workDir, "_complete")));
    assertTrue("final_centroids.seq must exist",
      fs.exists(new Path(workDir, "final_centroids.seq")));

    // Verify that re-executing against an existing completed directory returns cached results
    // idempotently
    KMeansResult result2 = KMeansTool.trainDistributed(conf, tableName, "V", 4, 4, config);
    assertNotNull(result2);
    assertEquals("Iteration count should match first run", result1.getIterations(),
      result2.getIterations());
    assertEquals("Effective K should match first run", result1.getEffectiveK(),
      result2.getEffectiveK());

    assertEquals(result1.size(), result2.size());
    for (int i = 0; i < result1.size(); i++) {
      float[] c1 = result1.get(i);
      float[] c2 = result2.get(i);
      assertEquals("Centroid dimension must match", c1.length, c2.length);
      for (int d = 0; d < c1.length; d++) {
        assertEquals("Centroid component must match", c1[d], c2[d], 1e-5f);
      }
    }
  }
}
