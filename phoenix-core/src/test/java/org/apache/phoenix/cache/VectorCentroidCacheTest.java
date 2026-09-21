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
package org.apache.phoenix.cache;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.phoenix.index.vector.CentroidManager;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class VectorCentroidCacheTest {

  @Before
  public void setUp() {
    VectorCentroidCache.resetInstance();
    CentroidManager.clearThreadLocalConnection();
    CentroidManager.setDefaultConnection(null);
  }

  @After
  public void tearDown() {
    VectorCentroidCache.resetInstance();
    CentroidManager.clearThreadLocalConnection();
    CentroidManager.setDefaultConnection(null);
  }

  /**
   * Tests nearest centroid lookup on 3 centroids at (0,0), (10,0), (0,10) with query (9.5, 0.5)
   * under L2 metric. The closest centroid is (10,0), which corresponds to centroid ID 1.
   */
  @Test
  public void testNearestCentroid() {
    VectorCentroidCache cache = new VectorCentroidCache("TEST_IDX");
    List<float[]> centroids = Arrays.asList(new float[] { 0.0f, 0.0f }, // ID 0
      new float[] { 10.0f, 0.0f }, // ID 1
      new float[] { 0.0f, 10.0f } // ID 2
    );
    cache.putFloatCentroids("TEST_IDX", 1L, centroids);

    float[] queryFloats = new float[] { 9.5f, 0.5f };
    byte[] queryBytes = PVectorFloat.INSTANCE.toBytes(queryFloats);

    // Test with float[]
    int nearestIdFloat = cache.findNearestCentroid(queryFloats, "L2");
    assertEquals(1, nearestIdFloat);

    // Test with byte[]
    int nearestIdByte = cache.findNearestCentroid(queryBytes, "L2");
    assertEquals(1, nearestIdByte);

    // Test with explicit index name and generation
    assertEquals(1, cache.findNearestCentroid("TEST_IDX", 1L, queryBytes, "L2"));
    assertEquals(1, cache.findNearestCentroid("TEST_IDX", queryBytes, "L2"));

    // Verify stored packed bytes match
    VectorCentroidCache.CachedCentroids cached = cache.getCentroids("TEST_IDX", 1L);
    assertEquals(3, cached.getCentroidCount());
    assertEquals(2, cached.getDimension());
    assertArrayEquals(centroids.get(1), cached.getFloatCentroid(1), 0.0f);
  }

  /**
   * Tests top-N nearest centroids search with probeCount=2 for query (9.5, 0.5). The two nearest
   * centroids in distance order are (10,0) (ID 1) and (0,0) (ID 0).
   */
  @Test
  public void testTopNNearestCentroids() {
    VectorCentroidCache cache = new VectorCentroidCache("TEST_IDX");
    List<float[]> centroids = Arrays.asList(new float[] { 0.0f, 0.0f }, new float[] { 10.0f, 0.0f },
      new float[] { 0.0f, 10.0f });
    cache.putFloatCentroids("TEST_IDX", 1L, centroids);

    float[] queryFloats = new float[] { 9.5f, 0.5f };
    byte[] queryBytes = PVectorFloat.INSTANCE.toBytes(queryFloats);

    List<Integer> top2 = cache.findNearestCentroids(queryBytes, "L2", 2);
    assertEquals(Arrays.asList(1, 0), top2);

    List<Integer> top2Floats = cache.findNearestCentroids(queryFloats, "L2", 2);
    assertEquals(Arrays.asList(1, 0), top2Floats);

    List<Integer> top1 = cache.findNearestCentroids(queryBytes, "L2", 1);
    assertEquals(Collections.singletonList(1), top1);

    List<Integer> top3 = cache.findNearestCentroids(queryBytes, "L2", 3);
    assertEquals(Arrays.asList(1, 0, 2), top3);

    List<Integer> topAll = cache.findNearestCentroids(queryBytes, "L2", 10);
    assertEquals(Arrays.asList(1, 0, 2), topAll);

    List<Integer> topZero = cache.findNearestCentroids(queryBytes, "L2", 0);
    assertTrue(topZero.isEmpty());
  }

  /**
   * Tests cache invalidation when the generation advances. Generation 1: ID 0=(0,0), ID 1=(10,0),
   * ID 2=(0,10) -> query (9.5, 0.5) is nearest ID 1. Generation 2: ID 0=(10,0), ID 1=(0,0), ID
   * 2=(0,10) -> query (9.5, 0.5) is nearest ID 0.
   */
  @Test
  public void testCacheInvalidation() {
    VectorCentroidCache cache = new VectorCentroidCache("TEST_IDX");

    List<float[]> gen1Centroids = Arrays.asList(new float[] { 0.0f, 0.0f },
      new float[] { 10.0f, 0.0f }, new float[] { 0.0f, 10.0f });
    cache.putFloatCentroids("TEST_IDX", 1L, gen1Centroids);

    float[] query = new float[] { 9.5f, 0.5f };
    assertEquals(1, cache.findNearestCentroid(query, "L2"));

    // Advance generation and register updated centroid positions
    List<float[]> gen2Centroids = Arrays.asList(new float[] { 10.0f, 0.0f },
      new float[] { 0.0f, 0.0f }, new float[] { 0.0f, 10.0f });
    cache.advanceGeneration("TEST_IDX", 2L);
    cache.putFloatCentroids("TEST_IDX", 2L, gen2Centroids);

    assertEquals(0, cache.findNearestCentroid(query, "L2"));
    assertEquals(0, cache.findNearestCentroid("TEST_IDX", 2L, query, "L2"));
  }

  /**
   * Tests cache invalidation triggered via explicit generation advancement.
   */
  @Test
  public void testCacheInvalidationWithGenerationAdvance() throws Exception {
    VectorCentroidCache cache = new VectorCentroidCache("TEST_IDX");

    List<float[]> gen1 = Arrays.asList(new float[] { 0.0f, 0.0f }, new float[] { 10.0f, 0.0f });
    cache.putFloatCentroids("TEST_IDX", 1L, gen1);

    float[] query = new float[] { 9.5f, 0.5f };
    assertEquals(1, cache.findNearestCentroid("TEST_IDX", 1L, query, "L2"));

    List<float[]> gen2 = Arrays.asList(new float[] { 10.0f, 0.0f }, new float[] { 0.0f, 0.0f });
    cache.advanceGeneration("TEST_IDX", 2L);
    cache.putFloatCentroids("TEST_IDX", 2L, gen2);

    assertEquals(0, cache.findNearestCentroid("TEST_IDX", 2L, query, "L2"));
  }

  /**
   * Tests LRU eviction and cache bounding with configurable max size.
   */
  @Test
  public void testLruEvictionAndMetrics() {
    Configuration conf = HBaseConfiguration.create();
    conf.setLong(VectorCentroidCache.VECTOR_CENTROID_CACHE_MAX_SIZE_ATTRIB, 2L);

    VectorCentroidCache cache = new VectorCentroidCache(conf);

    List<float[]> c1 = Collections.singletonList(new float[] { 1.0f, 1.0f });
    List<float[]> c2 = Collections.singletonList(new float[] { 2.0f, 2.0f });
    List<float[]> c3 = Collections.singletonList(new float[] { 3.0f, 3.0f });

    cache.putFloatCentroids("IDX_A", 1L, c1);
    cache.putFloatCentroids("IDX_B", 1L, c2);
    assertEquals(2, cache.getCacheSize());

    assertNotNull(cache.getCentroids("IDX_A", 1L));

    cache.putFloatCentroids("IDX_C", 1L, c3);

    assertEquals(2, cache.getCacheSize());
    assertTrue(cache.getEvictionCount() >= 1);

    long initialHits = cache.getHitCount();
    cache.getCentroids("IDX_A", 1L);
    assertEquals(initialHits + 1, cache.getHitCount());
    assertTrue(cache.getHitRate() > 0.0);
  }

  /**
   * Tests distance metrics: L2, COSINE, and INNER_PRODUCT.
   */
  @Test
  public void testDistanceMetrics() {
    VectorCentroidCache cache = new VectorCentroidCache("METRIC_IDX");

    List<float[]> cosineCentroids =
      Arrays.asList(new float[] { 1.0f, 0.0f }, new float[] { 0.0f, 1.0f });
    cache.putFloatCentroids("METRIC_IDX", 1L, cosineCentroids);

    float[] queryCosine = new float[] { 0.95f, 0.05f };
    assertEquals(0, cache.findNearestCentroid(queryCosine, "COSINE"));

    float[] queryCosine2 = new float[] { 0.05f, 0.95f };
    assertEquals(1, cache.findNearestCentroid(queryCosine2, "COSINE"));

    List<float[]> ipCentroids =
      Arrays.asList(new float[] { 10.0f, 0.0f }, new float[] { 1.0f, 0.0f });
    cache.putFloatCentroids("METRIC_IDX", 2L, ipCentroids);

    float[] queryIp = new float[] { 1.0f, 0.0f };
    assertEquals(0, cache.findNearestCentroid("METRIC_IDX", 2L, queryIp, "INNER_PRODUCT"));

    // Top-2 nearest centroids using cosine distance
    List<float[]> cosineCentroids3 = Arrays.asList(new float[] { 1.0f, 0.0f }, // ID 0: 0 deg
      new float[] { 0.707f, 0.707f }, // ID 1: 45 deg
      new float[] { 0.0f, 1.0f } // ID 2: 90 deg
    );
    cache.putFloatCentroids("METRIC_COS_TOPN", 1L, cosineCentroids3);
    assertEquals(Arrays.asList(0, 1),
      cache.findNearestCentroids("METRIC_COS_TOPN", 1L, queryCosine, "COSINE", 2));

    // Top-2 nearest centroids using inner product
    List<float[]> ipCentroids3 = Arrays.asList(new float[] { 10.0f, 0.0f }, // ID 0: dot product 10
      new float[] { 5.0f, 0.0f }, // ID 1: dot product 5
      new float[] { 1.0f, 0.0f } // ID 2: dot product 1
    );
    cache.putFloatCentroids("METRIC_IP_TOPN", 1L, ipCentroids3);
    assertEquals(Arrays.asList(0, 1),
      cache.findNearestCentroids("METRIC_IP_TOPN", 1L, queryIp, "INNER_PRODUCT", 2));
  }

  /**
   * Tests isolation between multiple index tables cached simultaneously.
   */
  @Test
  public void testMultiIndexIsolation() {
    VectorCentroidCache cache = new VectorCentroidCache();

    List<float[]> idx1Centroids =
      Arrays.asList(new float[] { 1.0f, 0.0f }, new float[] { 0.0f, 1.0f });
    List<float[]> idx2Centroids =
      Arrays.asList(new float[] { 100.0f, 0.0f, 0.0f }, new float[] { 0.0f, 100.0f, 0.0f });

    cache.putFloatCentroids("INDEX_ONE", 1L, idx1Centroids);
    cache.putFloatCentroids("INDEX_TWO", 1L, idx2Centroids);

    assertEquals(0, cache.findNearestCentroid("INDEX_ONE", new float[] { 0.9f, 0.1f }, "L2"));
    assertEquals(1,
      cache.findNearestCentroid("INDEX_TWO", new float[] { 0.0f, 90.0f, 0.0f }, "L2"));

    cache.invalidate("INDEX_ONE");
    assertEquals(1, cache.getCacheSize());
    assertNotNull(cache.getCentroids("INDEX_TWO", 1L));
  }

  /**
   * Tests concurrent multi-threaded lookups for thread safety.
   */
  @Test
  public void testConcurrentLookups() throws Exception {
    final VectorCentroidCache cache = new VectorCentroidCache("CONCURRENT_IDX");
    List<float[]> centroids = Arrays.asList(new float[] { 0.0f, 0.0f }, new float[] { 10.0f, 0.0f },
      new float[] { 0.0f, 10.0f });
    cache.putFloatCentroids("CONCURRENT_IDX", 1L, centroids);

    int numThreads = 8;
    int iterationsPerThread = 500;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    final CountDownLatch latch = new CountDownLatch(1);
    List<Future<Boolean>> futures = new ArrayList<>();

    for (int t = 0; t < numThreads; t++) {
      futures.add(executor.submit(() -> {
        latch.await();
        float[] q = new float[] { 9.5f, 0.5f };
        for (int i = 0; i < iterationsPerThread; i++) {
          int nearest = cache.findNearestCentroid(q, "L2");
          if (nearest != 1) {
            return false;
          }
          List<Integer> top2 = cache.findNearestCentroids(q, "L2", 2);
          if (top2.size() != 2 || top2.get(0) != 1 || top2.get(1) != 0) {
            return false;
          }
        }
        return true;
      }));
    }

    latch.countDown();
    for (Future<Boolean> f : futures) {
      assertTrue(f.get(10, TimeUnit.SECONDS));
    }
    executor.shutdown();
  }

  /**
   * Tests error handling for nulls, dimension mismatch, and invalid vector lengths.
   */
  @Test
  public void testValidationAndEdgeCases() {
    VectorCentroidCache cache = new VectorCentroidCache("VALIDATION_IDX");
    cache.putFloatCentroids("VALIDATION_IDX", 1L,
      Collections.singletonList(new float[] { 1.0f, 2.0f }));

    try {
      cache.findNearestCentroid((float[]) null, "L2");
      fail("Should reject null query vector");
    } catch (IllegalArgumentException expected) {
    }

    try {
      cache.findNearestCentroid(new float[] { 1.0f, 2.0f, 3.0f }, "L2");
      fail("Should reject query with dimension mismatch");
    } catch (IllegalArgumentException expected) {
    }

    try {
      cache.findNearestCentroid(new byte[] { 1, 2, 3 }, "L2");
      fail("Should reject byte vector not multiple of 4");
    } catch (IllegalArgumentException expected) {
    }

    VectorCentroidCache emptyCache = new VectorCentroidCache("EMPTY_IDX");
    try {
      emptyCache.findNearestCentroid(new float[] { 1.0f }, "L2");
      fail("Should fail when index has no centroids");
    } catch (IllegalStateException expected) {
    }
  }

  /**
   * Create a centroid cache with 4096 centroids at random positions. For 100 random query vectors,
   * compute the nearest centroid via brute-force and via the hierarchical lookup. Assert the
   * results match in at least 95% of cases.
   */
  @Test
  public void testHierarchicalCorrectness() {
    int centroidCount = 4096;
    int dimension = 4;
    int queryCount = 100;
    Random rng = new Random(12345L);

    List<float[]> centroids = new ArrayList<>(centroidCount);
    for (int i = 0; i < centroidCount; i++) {
      float[] v = new float[dimension];
      for (int d = 0; d < dimension; d++) {
        v[d] = rng.nextFloat();
      }
      centroids.add(v);
    }

    VectorCentroidCache cache = new VectorCentroidCache("HIER_IDX");
    cache.putFloatCentroids("HIER_IDX", 1L, centroids);

    VectorCentroidCache.CachedCentroids cachedCentroids = cache.getCentroids("HIER_IDX", 1L);
    assertTrue("Cache should have built hierarchical index for 4096 centroids",
      cachedCentroids.hasHierarchicalIndex());
    assertEquals(64, cachedCentroids.getHierarchicalIndex().getNumBuckets());

    int matches = 0;
    for (int q = 0; q < queryCount; q++) {
      float[] query = new float[dimension];
      for (int d = 0; d < dimension; d++) {
        query[d] = rng.nextFloat();
      }

      int bfNearest = cache.findNearestCentroidBruteForce(query, "L2");
      int hierNearest = cache.findNearestCentroid(query, "L2");

      if (bfNearest == hierNearest) {
        matches++;
      }
    }

    double matchRate = (double) matches / queryCount;
    assertTrue("Hierarchical lookup must match brute-force in at least 95% of cases, got: "
      + matches + "/" + queryCount + " (" + (matchRate * 100) + "%)", matchRate >= 0.95);
  }

  /** Hierarchical lookup evaluates fewer than 25% of total centroids per query. */
  @Test
  public void testHierarchicalPerformance() {
    int centroidCount = 4096;
    int dimension = 4;
    int numCalls = 50;
    Random rng = new Random(42L);

    List<float[]> centroids = new ArrayList<>(centroidCount);
    for (int i = 0; i < centroidCount; i++) {
      float[] v = new float[dimension];
      for (int d = 0; d < dimension; d++) {
        v[d] = rng.nextFloat();
      }
      centroids.add(v);
    }

    VectorCentroidCache cache = new VectorCentroidCache("PERF_IDX");
    cache.putFloatCentroids("PERF_IDX", 1L, centroids);
    VectorCentroidCache.CachedCentroids cached = cache.getCentroids("PERF_IDX", 1L);

    for (int i = 0; i < numCalls; i++) {
      float[] q = new float[dimension];
      for (int d = 0; d < dimension; d++) {
        q[d] = rng.nextFloat();
      }
      cache.findNearestCentroidHierarchical(q, "L2");
      int evals = cached.getLastDistanceEvaluationCount();
      assertTrue(
        "Hierarchical lookup must evaluate fewer than 25% of 4096 centroids, got: " + evals,
        evals < 0.25 * centroidCount);
    }
  }

  /**
   * Evaluates top-N nearest centroids search recall and accuracy using hierarchical lookup against
   * brute-force baseline across random queries.
   */
  @Test
  public void testHierarchicalTopNProbing() {
    int centroidCount = 4096;
    int dimension = 4;
    int queryCount = 100;
    Random rng = new Random(99L);

    List<float[]> centroids = new ArrayList<>(centroidCount);
    for (int i = 0; i < centroidCount; i++) {
      float[] v = new float[dimension];
      for (int d = 0; d < dimension; d++) {
        v[d] = rng.nextFloat();
      }
      centroids.add(v);
    }

    VectorCentroidCache cache = new VectorCentroidCache("TOPN_IDX");
    cache.putFloatCentroids("TOPN_IDX", 1L, centroids);

    int totalMatches = 0;
    int top1Matches = 0;

    for (int q = 0; q < queryCount; q++) {
      float[] query = new float[dimension];
      for (int d = 0; d < dimension; d++) {
        query[d] = rng.nextFloat();
      }

      List<Integer> hier5 = cache.findNearestCentroidsHierarchical(query, "L2", 5);
      List<Integer> bf5 = cache.findNearestCentroidsBruteForce(query, "L2", 5);

      assertEquals(5, hier5.size());
      assertEquals(5, bf5.size());

      if (hier5.get(0).equals(bf5.get(0))) {
        top1Matches++;
      }

      // Verify candidates are sorted by ascending distance
      for (int i = 0; i < hier5.size() - 1; i++) {
        float d1 = computeL2(query, centroids.get(hier5.get(i)));
        float d2 = computeL2(query, centroids.get(hier5.get(i + 1)));
        assertTrue("Results must be sorted by ascending distance: " + d1 + " <= " + d2,
          d1 <= d2 + 1e-6f);
      }

      // Calculate recall against brute force nearest centroids
      Set<Integer> hierSet = new HashSet<>(hier5);
      hierSet.retainAll(bf5);
      totalMatches += hierSet.size();
    }

    double avgRecall = (double) totalMatches / (queryCount * 5);
    assertTrue("Average set recall must be >= 0.95, got: " + avgRecall, avgRecall >= 0.95);

    double top1Rate = (double) top1Matches / queryCount;
    assertTrue("Top-1 match rate must be >= 0.95, got: " + top1Rate, top1Rate >= 0.95);
  }

  private static float computeL2(float[] a, float[] b) {
    float sum = 0.0f;
    for (int i = 0; i < a.length; i++) {
      float diff = a[i] - b[i];
      sum += diff * diff;
    }
    return sum;
  }

  /**
   * Tests that hierarchical index is only built when centroid count exceeds bruteforceLimit.
   */
  @Test
  public void testBruteforceLimitThreshold() {
    Configuration conf = HBaseConfiguration.create();
    conf.setInt(VectorCentroidCache.VECTOR_CENTROID_BRUTEFORCE_LIMIT_ATTRIB, 10);
    VectorCentroidCache cache = new VectorCentroidCache(conf);

    List<float[]> c5 = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      c5.add(new float[] { (float) i, (float) i });
    }
    cache.putFloatCentroids("SMALL_IDX", 1L, c5);
    assertFalse(cache.getCentroids("SMALL_IDX", 1L).hasHierarchicalIndex());

    List<float[]> c15 = new ArrayList<>();
    for (int i = 0; i < 15; i++) {
      c15.add(new float[] { (float) i, (float) i });
    }
    cache.putFloatCentroids("LARGE_IDX", 1L, c15);
    assertTrue(cache.getCentroids("LARGE_IDX", 1L).hasHierarchicalIndex());
  }

}
