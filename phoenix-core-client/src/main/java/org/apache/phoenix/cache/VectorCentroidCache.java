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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.function.VectorDistanceUtil;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.vector.CentroidManager;
import org.apache.phoenix.index.vector.KMeansTrainer;
import org.apache.phoenix.optimize.DistanceMetric;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.cache.Cache;
import org.apache.phoenix.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.phoenix.thirdparty.com.google.common.cache.CacheStats;
import org.apache.phoenix.thirdparty.com.google.common.cache.RemovalListener;

/**
 * Caches centroid vectors in memory as packed byte arrays and pre-decoded native float arrays,
 * enabling high-performance nearest-neighbor search and probe selection during index maintenance
 * and query planning.
 */
public class VectorCentroidCache {

  private static final Logger LOG = LoggerFactory.getLogger(VectorCentroidCache.class);

  public static final String VECTOR_CENTROID_CACHE_MAX_SIZE_ATTRIB =
    QueryServices.VECTOR_CENTROID_CACHE_MAX_SIZE_ATTRIB;
  public static final long DEFAULT_VECTOR_CENTROID_CACHE_MAX_SIZE =
    QueryServicesOptions.DEFAULT_VECTOR_CENTROID_CACHE_MAX_SIZE;
  public static final String VECTOR_CENTROID_BRUTEFORCE_LIMIT_ATTRIB =
    QueryServices.VECTOR_CENTROID_BRUTEFORCE_LIMIT_ATTRIB;
  public static final int DEFAULT_VECTOR_CENTROID_BRUTEFORCE_LIMIT =
    QueryServicesOptions.DEFAULT_VECTOR_CENTROID_BRUTEFORCE_LIMIT;
  public static final String VECTOR_CENTROID_PROBE_BUCKETS_ATTRIB =
    QueryServices.VECTOR_CENTROID_PROBE_BUCKETS_ATTRIB;
  public static final int DEFAULT_VECTOR_CENTROID_PROBE_BUCKETS =
    QueryServicesOptions.DEFAULT_VECTOR_CENTROID_PROBE_BUCKETS;

  private static volatile VectorCentroidCache defaultInstance;

  /**
   * Retained cluster configuration fallback for singleton access when no explicit
   * {@link Configuration} is provided. Preserved across {@link #resetInstance()}.
   */
  private static volatile Configuration suppliedConf;

  private volatile Configuration conf;
  private final Cache<CacheKey, CachedCentroids> cache;
  private final ConcurrentMap<String, Long> activeGenerations = new ConcurrentHashMap<>();
  private final int bruteforceLimit;
  private final int probeBuckets;
  private volatile String defaultIndexName;

  /** Composite cache key combining normalized index name and generation ID. */
  public static final class CacheKey {
    private final String indexName;
    private final long generation;

    public CacheKey(String indexName, long generation) {
      this.indexName = SchemaUtil
        .normalizeFullTableName(Objects.requireNonNull(indexName, "indexName must not be null"));
      this.generation = generation;
    }

    public String getIndexName() {
      return indexName;
    }

    public long getGeneration() {
      return generation;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof CacheKey)) {
        return false;
      }
      CacheKey cacheKey = (CacheKey) o;
      return generation == cacheKey.generation && indexName.equals(cacheKey.indexName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(indexName, generation);
    }

    @Override
    public String toString() {
      return "CacheKey{" + indexName + ", gen=" + generation + "}";
    }
  }

  /** Pair of centroid ID and distance for sorting / top-N priority queue. */
  public static final class CentroidDistance implements Comparable<CentroidDistance> {
    private final int id;
    private final double distance;

    public CentroidDistance(int id, double distance) {
      this.id = id;
      this.distance = distance;
    }

    public int getId() {
      return id;
    }

    public double getDistance() {
      return distance;
    }

    @Override
    public int compareTo(CentroidDistance o) {
      int cmp = Double.compare(this.distance, o.distance);
      if (cmp != 0) {
        return cmp;
      }
      return Integer.compare(this.id, o.id);
    }
  }

  /** Holds the cached centroids for a specific index and generation. */
  public static final class CachedCentroids {
    private final String indexName;
    private final long generation;
    private final byte[][] packedCentroids;
    private final float[][] floatCentroids;
    private final int dimension;
    private final int centroidCount;
    private final long loadedTimestamp;
    private final int bruteforceLimit;
    private final int probeBuckets;
    private final HierarchicalIndex hierarchicalIndex;
    private final Cache<ImmutableBytesPtr, Integer> centroidAssignmentCache =
      CacheBuilder.newBuilder().maximumSize(10000).build();
    private volatile int lastDistanceEvaluationCount;

    public int getLastDistanceEvaluationCount() {
      return lastDistanceEvaluationCount;
    }

    public CachedCentroids(String indexName, long generation, List<byte[]> byteCentroids,
      int bruteforceLimit, int probeBuckets) {
      this.indexName = SchemaUtil
        .normalizeFullTableName(Objects.requireNonNull(indexName, "indexName must not be null"));
      this.generation = generation;
      this.loadedTimestamp = System.currentTimeMillis();
      this.bruteforceLimit = bruteforceLimit;
      this.probeBuckets = probeBuckets > 0 ? probeBuckets : DEFAULT_VECTOR_CENTROID_PROBE_BUCKETS;

      if (byteCentroids == null || byteCentroids.isEmpty()) {
        this.packedCentroids = new byte[0][];
        this.floatCentroids = new float[0][];
        this.dimension = 0;
        this.centroidCount = 0;
        this.hierarchicalIndex = null;
        return;
      }

      this.centroidCount = byteCentroids.size();
      this.packedCentroids = new byte[centroidCount][];
      this.floatCentroids = new float[centroidCount][];

      int expectedDim = -1;
      for (int i = 0; i < centroidCount; i++) {
        byte[] bytes = byteCentroids.get(i);
        if (bytes == null) {
          throw new IllegalArgumentException("Centroid at index " + i + " must not be null");
        }
        if (bytes.length % Bytes.SIZEOF_FLOAT != 0) {
          throw new IllegalArgumentException("Centroid bytes length at index " + i
            + " must be a multiple of " + Bytes.SIZEOF_FLOAT + ", got: " + bytes.length);
        }
        int dim = bytes.length / Bytes.SIZEOF_FLOAT;
        if (expectedDim == -1) {
          expectedDim = dim;
        } else if (dim != expectedDim) {
          throw new IllegalArgumentException("Dimension mismatch across centroids: expected "
            + expectedDim + ", got " + dim + " at index " + i);
        }

        this.packedCentroids[i] = bytes.clone();
        this.floatCentroids[i] =
          PVectorFloat.decodeCentroid(this.packedCentroids[i], 0, bytes.length, SortOrder.ASC);
      }
      this.dimension = expectedDim;

      if (this.centroidCount > this.bruteforceLimit) {
        this.hierarchicalIndex =
          HierarchicalIndex.build(this.floatCentroids, this.dimension, this.probeBuckets);
      } else {
        this.hierarchicalIndex = null;
      }
    }

    public CachedCentroids(String indexName, long generation, List<byte[]> byteCentroids) {
      this(indexName, generation, byteCentroids, DEFAULT_VECTOR_CENTROID_BRUTEFORCE_LIMIT,
        DEFAULT_VECTOR_CENTROID_PROBE_BUCKETS);
    }

    public CachedCentroids(String indexName, long generation, byte[][] byteCentroids) {
      this(indexName, generation,
        byteCentroids == null ? Collections.emptyList() : Arrays.asList(byteCentroids),
        DEFAULT_VECTOR_CENTROID_BRUTEFORCE_LIMIT, DEFAULT_VECTOR_CENTROID_PROBE_BUCKETS);
    }

    public CachedCentroids(String indexName, long generation, byte[][] byteCentroids,
      int bruteforceLimit, int probeBuckets) {
      this(indexName, generation,
        byteCentroids == null ? Collections.emptyList() : Arrays.asList(byteCentroids),
        bruteforceLimit, probeBuckets);
    }

    public CachedCentroids(String indexName, long generation, float[][] floatCentroidsArray) {
      this(indexName, generation,
        floatCentroidsArray == null ? Collections.emptyList() : Arrays.asList(floatCentroidsArray),
        true, DEFAULT_VECTOR_CENTROID_BRUTEFORCE_LIMIT, DEFAULT_VECTOR_CENTROID_PROBE_BUCKETS);
    }

    public CachedCentroids(String indexName, long generation, float[][] floatCentroidsArray,
      int bruteforceLimit, int probeBuckets) {
      this(indexName, generation,
        floatCentroidsArray == null ? Collections.emptyList() : Arrays.asList(floatCentroidsArray),
        true, bruteforceLimit, probeBuckets);
    }

    private CachedCentroids(String indexName, long generation, List<float[]> floatCentroidsList,
      boolean isFloatList, int bruteforceLimit, int probeBuckets) {
      this.indexName = SchemaUtil
        .normalizeFullTableName(Objects.requireNonNull(indexName, "indexName must not be null"));
      this.generation = generation;
      this.loadedTimestamp = System.currentTimeMillis();
      this.bruteforceLimit = bruteforceLimit;
      this.probeBuckets = probeBuckets > 0 ? probeBuckets : DEFAULT_VECTOR_CENTROID_PROBE_BUCKETS;

      if (floatCentroidsList == null || floatCentroidsList.isEmpty()) {
        this.packedCentroids = new byte[0][];
        this.floatCentroids = new float[0][];
        this.dimension = 0;
        this.centroidCount = 0;
        this.hierarchicalIndex = null;
        return;
      }

      this.centroidCount = floatCentroidsList.size();
      this.packedCentroids = new byte[centroidCount][];
      this.floatCentroids = new float[centroidCount][];

      int expectedDim = -1;
      for (int i = 0; i < centroidCount; i++) {
        float[] vector = floatCentroidsList.get(i);
        if (vector == null) {
          throw new IllegalArgumentException("Centroid vector at index " + i + " must not be null");
        }
        if (expectedDim == -1) {
          expectedDim = vector.length;
        } else if (vector.length != expectedDim) {
          throw new IllegalArgumentException("Dimension mismatch across centroids: expected "
            + expectedDim + ", got " + vector.length + " at index " + i);
        }

        this.floatCentroids[i] = vector.clone();
        this.packedCentroids[i] = PVectorFloat.INSTANCE.toBytes(this.floatCentroids[i]);
      }
      this.dimension = expectedDim;

      if (this.centroidCount > this.bruteforceLimit) {
        this.hierarchicalIndex =
          HierarchicalIndex.build(this.floatCentroids, this.dimension, this.probeBuckets);
      } else {
        this.hierarchicalIndex = null;
      }
    }

    public static CachedCentroids fromFloatVectors(String indexName, long generation,
      List<float[]> floatCentroidsList) {
      return new CachedCentroids(indexName, generation, floatCentroidsList, true,
        DEFAULT_VECTOR_CENTROID_BRUTEFORCE_LIMIT, DEFAULT_VECTOR_CENTROID_PROBE_BUCKETS);
    }

    public static CachedCentroids fromFloatVectors(String indexName, long generation,
      List<float[]> floatCentroidsList, int bruteforceLimit, int probeBuckets) {
      return new CachedCentroids(indexName, generation, floatCentroidsList, true, bruteforceLimit,
        probeBuckets);
    }

    public String getIndexName() {
      return indexName;
    }

    public long getGeneration() {
      return generation;
    }

    public int getDimension() {
      return dimension;
    }

    public int getCentroidCount() {
      return centroidCount;
    }

    public long getLoadedTimestamp() {
      return loadedTimestamp;
    }

    public int getBruteforceLimit() {
      return bruteforceLimit;
    }

    public int getProbeBuckets() {
      return probeBuckets;
    }

    public boolean hasHierarchicalIndex() {
      return hierarchicalIndex != null;
    }

    public HierarchicalIndex getHierarchicalIndex() {
      return hierarchicalIndex;
    }

    public int getNumBuckets() {
      return hierarchicalIndex != null ? hierarchicalIndex.getNumBuckets() : 0;
    }

    public byte[] getPackedCentroid(int centroidId) {
      if (centroidId < 0 || centroidId >= centroidCount) {
        throw new IndexOutOfBoundsException("Centroid ID out of range: " + centroidId);
      }
      return packedCentroids[centroidId].clone();
    }

    public float[] getFloatCentroid(int centroidId) {
      if (centroidId < 0 || centroidId >= centroidCount) {
        throw new IndexOutOfBoundsException("Centroid ID out of range: " + centroidId);
      }
      return floatCentroids[centroidId].clone();
    }

    public byte[][] getPackedCentroids() {
      byte[][] copy = new byte[centroidCount][];
      for (int i = 0; i < centroidCount; i++) {
        copy[i] = packedCentroids[i].clone();
      }
      return copy;
    }

    public float[][] getFloatCentroids() {
      float[][] copy = new float[centroidCount][];
      for (int i = 0; i < centroidCount; i++) {
        copy[i] = floatCentroids[i].clone();
      }
      return copy;
    }

    static double computeDistance(float[] query, float[] centroid, int dim, DistanceMetric metric) {
      if (metric == DistanceMetric.COSINE) {
        return VectorDistanceUtil.scalarCosineDistance(query, centroid, dim);
      } else if (metric == DistanceMetric.INNER_PRODUCT) {
        return VectorDistanceUtil.scalarInnerProductDistance(query, centroid, dim);
      } else {
        return VectorDistanceUtil.scalarL2DistanceSquared(query, centroid, dim);
      }
    }

    private float[] decodeQueryVector(byte[] queryVector, int offset, int length) {
      if (queryVector == null) {
        throw new IllegalArgumentException("queryVector must not be null");
      }
      if (length % Bytes.SIZEOF_FLOAT != 0) {
        throw new IllegalArgumentException(
          "Query vector length must be a multiple of " + Bytes.SIZEOF_FLOAT + ", got: " + length);
      }
      int qDim = length / Bytes.SIZEOF_FLOAT;
      if (centroidCount > 0 && qDim != dimension) {
        throw new IllegalArgumentException(
          "Dimension mismatch: expected " + dimension + ", got " + qDim);
      }
      return PVectorFloat.readElements(queryVector, offset, length);
    }

    private float[] decodeQueryVector(byte[] queryVector) {
      return decodeQueryVector(queryVector, 0, queryVector != null ? queryVector.length : 0);
    }

    /**
     * Finds the nearest centroid ID for the given query vector. Uses the hierarchical index if
     * present (> bruteforceLimit), otherwise brute force.
     */
    public int findNearestCentroid(float[] queryVector, String metric) {
      if (hierarchicalIndex != null) {
        return findNearestCentroidHierarchical(queryVector, metric);
      }
      return findNearestCentroidBruteForce(queryVector, metric);
    }

    /** Finds the nearest centroid ID for the given packed byte query vector slice. */
    public int findNearestCentroid(byte[] queryVector, int offset, int length, String metric) {
      if (queryVector == null) {
        throw new IllegalArgumentException("queryVector must not be null");
      }
      ImmutableBytesPtr key = new ImmutableBytesPtr(queryVector, offset, length);
      Integer cachedCentroid = centroidAssignmentCache.getIfPresent(key);
      if (cachedCentroid != null) {
        return cachedCentroid;
      }
      float[] decoded = decodeQueryVector(queryVector, offset, length);
      int centroidId = findNearestCentroid(decoded, metric);
      byte[] copy = new byte[length];
      System.arraycopy(queryVector, offset, copy, 0, length);
      centroidAssignmentCache.put(new ImmutableBytesPtr(copy), centroidId);
      return centroidId;
    }

    /** Finds the nearest centroid ID for the given packed byte query vector. */
    public int findNearestCentroid(byte[] queryVector, String metric) {
      if (queryVector == null) {
        throw new IllegalArgumentException("queryVector must not be null");
      }
      return findNearestCentroid(queryVector, 0, queryVector.length, metric);
    }

    /** Finds the nearest centroid ID using brute force linear scan. */
    public int findNearestCentroidBruteForce(float[] queryVector, String metric) {
      if (queryVector == null) {
        throw new IllegalArgumentException("queryVector must not be null");
      }
      if (centroidCount == 0) {
        throw new IllegalStateException("No centroids available in cache for index: " + indexName);
      }
      if (queryVector.length != dimension) {
        throw new IllegalArgumentException(
          "Dimension mismatch: expected " + dimension + ", got " + queryVector.length);
      }

      DistanceMetric metricEnum = DistanceMetric.fromString(metric);
      if (metricEnum == null) {
        metricEnum = DistanceMetric.L2;
      }

      int bestId = -1;
      double minDistance = Double.MAX_VALUE;
      for (int i = 0; i < centroidCount; i++) {
        double dist = computeDistance(queryVector, floatCentroids[i], dimension, metricEnum);
        if (bestId == -1 || dist < minDistance) {
          minDistance = dist;
          bestId = i;
        }
      }
      return bestId;
    }

    /** Finds the nearest centroid ID using brute force linear scan for packed byte vector. */
    public int findNearestCentroidBruteForce(byte[] queryVector, String metric) {
      return findNearestCentroidBruteForce(decodeQueryVector(queryVector), metric);
    }

    /** Finds the nearest centroid ID using hierarchical lookup with configured probe_buckets. */
    public int findNearestCentroidHierarchical(float[] queryVector, String metric) {
      return findNearestCentroidHierarchical(queryVector, metric, this.probeBuckets);
    }

    /** Finds the nearest centroid ID using hierarchical lookup with custom probe_buckets. */
    public int findNearestCentroidHierarchical(float[] queryVector, String metric,
      int probeBucketsToUse) {
      if (queryVector == null) {
        throw new IllegalArgumentException("queryVector must not be null");
      }
      if (centroidCount == 0) {
        throw new IllegalStateException("No centroids available in cache for index: " + indexName);
      }
      if (queryVector.length != dimension) {
        throw new IllegalArgumentException(
          "Dimension mismatch: expected " + dimension + ", got " + queryVector.length);
      }
      HierarchicalIndex hIndex = this.hierarchicalIndex;
      if (hIndex == null) {
        hIndex = HierarchicalIndex.build(this.floatCentroids, this.dimension, probeBucketsToUse);
      }
      if (hIndex == null) {
        return findNearestCentroidBruteForce(queryVector, metric);
      }
      int res = hIndex.findNearestCentroid(queryVector, metric, floatCentroids, probeBucketsToUse);
      this.lastDistanceEvaluationCount = hIndex.getLastEvaluationsCount();
      return res != -1 ? res : findNearestCentroidBruteForce(queryVector, metric);
    }

    /** Finds the nearest centroid ID using hierarchical lookup for packed byte vector. */
    public int findNearestCentroidHierarchical(byte[] queryVector, String metric) {
      return findNearestCentroidHierarchical(decodeQueryVector(queryVector), metric);
    }

    /** Finds the nearest centroid ID using hierarchical lookup for packed byte vector. */
    public int findNearestCentroidHierarchical(byte[] queryVector, String metric,
      int probeBucketsToUse) {
      return findNearestCentroidHierarchical(decodeQueryVector(queryVector), metric,
        probeBucketsToUse);
    }

    /** Finds the top-N closest centroid IDs for the query vector, sorted by distance ascending. */
    public List<Integer> findNearestCentroids(float[] queryVector, String metric, int probeCount) {
      if (hierarchicalIndex != null) {
        return findNearestCentroidsHierarchical(queryVector, metric, probeCount);
      }
      return findNearestCentroidsBruteForce(queryVector, metric, probeCount);
    }

    /** Finds the top-N closest centroid IDs for the given packed byte query vector. */
    public List<Integer> findNearestCentroids(byte[] queryVector, String metric, int probeCount) {
      return findNearestCentroids(decodeQueryVector(queryVector), metric, probeCount);
    }

    /** Finds the top-N closest centroid IDs using brute force linear scan. */
    public List<Integer> findNearestCentroidsBruteForce(float[] queryVector, String metric,
      int probeCount) {
      if (queryVector == null) {
        throw new IllegalArgumentException("queryVector must not be null");
      }
      if (probeCount <= 0 || centroidCount == 0) {
        return Collections.emptyList();
      }
      if (queryVector.length != dimension) {
        throw new IllegalArgumentException(
          "Dimension mismatch: expected " + dimension + ", got " + queryVector.length);
      }

      DistanceMetric metricEnum = DistanceMetric.fromString(metric);
      if (metricEnum == null) {
        metricEnum = DistanceMetric.L2;
      }

      int effectiveProbeCount = Math.min(probeCount, centroidCount);
      PriorityQueue<CentroidDistance> maxHeap =
        new PriorityQueue<>(effectiveProbeCount, (a, b) -> b.compareTo(a));

      for (int i = 0; i < centroidCount; i++) {
        double dist = computeDistance(queryVector, floatCentroids[i], dimension, metricEnum);
        CentroidDistance cd = new CentroidDistance(i, dist);
        if (maxHeap.size() < effectiveProbeCount) {
          maxHeap.offer(cd);
        } else if (cd.compareTo(maxHeap.peek()) < 0) {
          maxHeap.poll();
          maxHeap.offer(cd);
        }
      }

      List<CentroidDistance> topList = new ArrayList<>(maxHeap);
      Collections.sort(topList);
      List<Integer> result = new ArrayList<>(topList.size());
      for (CentroidDistance cd : topList) {
        result.add(cd.getId());
      }
      return result;
    }

    /** Finds the top-N closest centroid IDs using brute force scan for packed byte vector. */
    public List<Integer> findNearestCentroidsBruteForce(byte[] queryVector, String metric,
      int probeCount) {
      return findNearestCentroidsBruteForce(decodeQueryVector(queryVector), metric, probeCount);
    }

    /**
     * Finds the top-N closest centroid IDs using hierarchical lookup with configured probe_buckets.
     */
    public List<Integer> findNearestCentroidsHierarchical(float[] queryVector, String metric,
      int probeCount) {
      return findNearestCentroidsHierarchical(queryVector, metric, probeCount, this.probeBuckets);
    }

    /** Finds the top-N closest centroid IDs using hierarchical lookup with custom probe_buckets. */
    public List<Integer> findNearestCentroidsHierarchical(float[] queryVector, String metric,
      int probeCount, int probeBucketsToUse) {
      if (queryVector == null) {
        throw new IllegalArgumentException("queryVector must not be null");
      }
      if (probeCount <= 0 || centroidCount == 0) {
        return Collections.emptyList();
      }
      if (queryVector.length != dimension) {
        throw new IllegalArgumentException(
          "Dimension mismatch: expected " + dimension + ", got " + queryVector.length);
      }
      HierarchicalIndex hIndex = this.hierarchicalIndex;
      if (hIndex == null) {
        hIndex = HierarchicalIndex.build(this.floatCentroids, this.dimension, probeBucketsToUse);
      }
      if (hIndex == null) {
        return findNearestCentroidsBruteForce(queryVector, metric, probeCount);
      }
      List<Integer> res = hIndex.findNearestCentroids(queryVector, metric, floatCentroids,
        probeCount, probeBucketsToUse);
      this.lastDistanceEvaluationCount = hIndex.getLastEvaluationsCount();
      return res;
    }

    /** Finds top-N closest centroid IDs using hierarchical lookup for packed byte vector. */
    public List<Integer> findNearestCentroidsHierarchical(byte[] queryVector, String metric,
      int probeCount) {
      return findNearestCentroidsHierarchical(decodeQueryVector(queryVector), metric, probeCount);
    }

    /** Finds top-N closest centroid IDs using hierarchical lookup for packed byte vector. */
    public List<Integer> findNearestCentroidsHierarchical(byte[] queryVector, String metric,
      int probeCount, int probeBucketsToUse) {
      return findNearestCentroidsHierarchical(decodeQueryVector(queryVector), metric, probeCount,
        probeBucketsToUse);
    }
  }

  /**
   * Hierarchical coarse quantizer index over cached centroids for large centroid counts (>
   * bruteforce limit). Partitions centroids into √L buckets using a single round of k-means,
   * reducing nearest-neighbor centroid search from O(L) to O(√L) amortized.
   */
  public static final class HierarchicalIndex {
    private final int numBuckets;
    private final float[][] bucketCentroids;
    private final int[][] bucketMembers;
    private final int dimension;
    private final int probeBuckets;
    private volatile int lastEvaluationsCount;

    public int getLastEvaluationsCount() {
      return lastEvaluationsCount;
    }

    public HierarchicalIndex(int numBuckets, float[][] bucketCentroids, int[][] bucketMembers,
      int dimension, int probeBuckets) {
      this.numBuckets = numBuckets;
      this.bucketCentroids = bucketCentroids;
      this.bucketMembers = bucketMembers;
      this.dimension = dimension;
      this.probeBuckets = Math.max(1, probeBuckets);
    }

    public int getNumBuckets() {
      return numBuckets;
    }

    public float[][] getBucketCentroids() {
      float[][] copy = new float[numBuckets][];
      for (int i = 0; i < numBuckets; i++) {
        copy[i] = bucketCentroids[i].clone();
      }
      return copy;
    }

    public int[][] getBucketMembers() {
      int[][] copy = new int[numBuckets][];
      for (int i = 0; i < numBuckets; i++) {
        copy[i] = bucketMembers[i].clone();
      }
      return copy;
    }

    public int getProbeBuckets() {
      return probeBuckets;
    }

    public static HierarchicalIndex build(float[][] floatCentroids, int dimension,
      int probeBuckets) {
      return build(floatCentroids, dimension, probeBuckets, 42L);
    }

    public static HierarchicalIndex build(float[][] floatCentroids, int dimension, int probeBuckets,
      Long randomSeed) {
      int L = floatCentroids.length;
      if (L == 0 || dimension == 0) {
        return null;
      }
      int K = (int) Math.round(Math.sqrt(L));
      if (K < 1) {
        K = 1;
      }
      if (K > L) {
        K = L;
      }

      Random random = randomSeed != null ? new Random(randomSeed) : new Random();

      // Partition centroids into sqrt(L) coarse buckets using k-means++ initialization
      // followed by Lloyd refinement iterations to optimize bucket centroids.
      List<float[]> vectorList = Arrays.asList(floatCentroids);
      List<float[]> initialCenters =
        KMeansTrainer.initializeKMeansPlusPlus(vectorList, K, "L2", random);
      float[][] bucketCentroids = new float[K][dimension];
      for (int i = 0; i < K; i++) {
        bucketCentroids[i] = initialCenters.get(i).clone();
      }

      final int HIERARCHICAL_KMEANS_ITERATIONS = 5;
      int[] assignments = new int[L];
      int[] clusterSizes = new int[K];
      double[][] clusterSums = new double[K][dimension];

      for (int iter = 0; iter < HIERARCHICAL_KMEANS_ITERATIONS; iter++) {
        Arrays.fill(clusterSizes, 0);
        for (int c = 0; c < K; c++) {
          Arrays.fill(clusterSums[c], 0.0);
        }
        for (int i = 0; i < L; i++) {
          float[] v = floatCentroids[i];
          int bestC = 0;
          double bestDist = Double.MAX_VALUE;
          for (int c = 0; c < K; c++) {
            double d = VectorDistanceUtil.scalarL2DistanceSquared(v, bucketCentroids[c], dimension);
            if (d < bestDist) {
              bestDist = d;
              bestC = c;
            }
          }
          assignments[i] = bestC;
          clusterSizes[bestC]++;
          for (int d = 0; d < dimension; d++) {
            clusterSums[bestC][d] += v[d];
          }
        }

        boolean anyUpdated = false;
        for (int c = 0; c < K; c++) {
          if (clusterSizes[c] > 0) {
            float[] updated = new float[dimension];
            double count = clusterSizes[c];
            for (int d = 0; d < dimension; d++) {
              updated[d] = (float) (clusterSums[c][d] / count);
            }
            bucketCentroids[c] = updated;
            anyUpdated = true;
          }
        }
        if (!anyUpdated) {
          break;
        }
      }

      // Assign centroids to their final nearest bucket and construct bucket inverted lists
      Arrays.fill(clusterSizes, 0);
      for (int i = 0; i < L; i++) {
        float[] v = floatCentroids[i];
        int bestC = 0;
        double bestDist = Double.MAX_VALUE;
        for (int c = 0; c < K; c++) {
          double d = VectorDistanceUtil.scalarL2DistanceSquared(v, bucketCentroids[c], dimension);
          if (d < bestDist) {
            bestDist = d;
            bestC = c;
          }
        }
        assignments[i] = bestC;
        clusterSizes[bestC]++;
      }

      int[][] bucketMembers = new int[K][];
      int[] fillPointers = new int[K];
      for (int c = 0; c < K; c++) {
        bucketMembers[c] = new int[clusterSizes[c]];
      }
      for (int i = 0; i < L; i++) {
        int c = assignments[i];
        bucketMembers[c][fillPointers[c]++] = i;
      }

      return new HierarchicalIndex(K, bucketCentroids, bucketMembers, dimension, probeBuckets);
    }

    public int findNearestCentroid(float[] queryVector, String metric, float[][] floatCentroids,
      int probeBucketsToUse) {
      DistanceMetric metricEnum = DistanceMetric.fromString(metric);
      if (metricEnum == null) {
        metricEnum = DistanceMetric.L2;
      }
      int effectiveProbeBuckets =
        Math.min(probeBucketsToUse > 0 ? probeBucketsToUse : this.probeBuckets, numBuckets);

      PriorityQueue<CentroidDistance> topBuckets =
        new PriorityQueue<>(effectiveProbeBuckets, (a, b) -> b.compareTo(a));

      for (int b = 0; b < numBuckets; b++) {
        double dist =
          CachedCentroids.computeDistance(queryVector, bucketCentroids[b], dimension, metricEnum);
        CentroidDistance cd = new CentroidDistance(b, dist);
        if (topBuckets.size() < effectiveProbeBuckets) {
          topBuckets.offer(cd);
        } else if (cd.compareTo(topBuckets.peek()) < 0) {
          topBuckets.poll();
          topBuckets.offer(cd);
        }
      }

      int bestId = -1;
      double minDistance = Double.MAX_VALUE;
      int evals = numBuckets;
      for (CentroidDistance cd : topBuckets) {
        int b = cd.getId();
        int[] members = bucketMembers[b];
        if (members == null) {
          continue;
        }
        evals += members.length;
        for (int i = 0; i < members.length; i++) {
          int cId = members[i];
          double dist = CachedCentroids.computeDistance(queryVector, floatCentroids[cId], dimension,
            metricEnum);
          if (bestId == -1 || dist < minDistance) {
            minDistance = dist;
            bestId = cId;
          }
        }
      }
      this.lastEvaluationsCount = evals;
      return bestId;
    }

    public List<Integer> findNearestCentroids(float[] queryVector, String metric,
      float[][] floatCentroids, int probeCount, int probeBucketsToUse) {
      if (probeCount <= 0 || floatCentroids.length == 0) {
        return Collections.emptyList();
      }
      DistanceMetric metricEnum = DistanceMetric.fromString(metric);
      if (metricEnum == null) {
        metricEnum = DistanceMetric.L2;
      }
      int effectiveProbeCount = Math.min(probeCount, floatCentroids.length);
      int pBuckets = probeBucketsToUse > 0 ? probeBucketsToUse : this.probeBuckets;

      CentroidDistance[] bucketDists = new CentroidDistance[numBuckets];
      for (int b = 0; b < numBuckets; b++) {
        double dist =
          CachedCentroids.computeDistance(queryVector, bucketCentroids[b], dimension, metricEnum);
        bucketDists[b] = new CentroidDistance(b, dist);
      }
      Arrays.sort(bucketDists);

      int candidateCount = 0;
      int probedBucketCount = 0;
      for (int i = 0; i < numBuckets; i++) {
        int b = bucketDists[i].getId();
        candidateCount += bucketMembers[b].length;
        probedBucketCount++;
        if (probedBucketCount >= pBuckets && candidateCount >= effectiveProbeCount) {
          break;
        }
      }

      PriorityQueue<CentroidDistance> maxHeap =
        new PriorityQueue<>(effectiveProbeCount, (a, b) -> b.compareTo(a));

      int evalsTopN = numBuckets;
      for (int i = 0; i < probedBucketCount; i++) {
        int b = bucketDists[i].getId();
        int[] members = bucketMembers[b];
        evalsTopN += members.length;
        for (int j = 0; j < members.length; j++) {
          int cId = members[j];
          double dist = CachedCentroids.computeDistance(queryVector, floatCentroids[cId], dimension,
            metricEnum);
          CentroidDistance cd = new CentroidDistance(cId, dist);
          if (maxHeap.size() < effectiveProbeCount) {
            maxHeap.offer(cd);
          } else if (cd.compareTo(maxHeap.peek()) < 0) {
            maxHeap.poll();
            maxHeap.offer(cd);
          }
        }
      }
      this.lastEvaluationsCount = evalsTopN;

      List<CentroidDistance> topList = new ArrayList<>(maxHeap);
      Collections.sort(topList);
      List<Integer> result = new ArrayList<>(topList.size());
      for (CentroidDistance cd : topList) {
        result.add(cd.getId());
      }
      return result;
    }
  }

  public VectorCentroidCache() {
    this(HBaseConfiguration.create(), null);
  }

  public VectorCentroidCache(Configuration conf) {
    this(conf, null);
  }

  public VectorCentroidCache(String defaultIndexName) {
    this(null, defaultIndexName);
  }

  public VectorCentroidCache(Configuration conf, String defaultIndexName) {
    this.conf = conf != null ? conf : HBaseConfiguration.create();
    this.defaultIndexName = defaultIndexName;

    long maxSize = this.conf.getLong(VECTOR_CENTROID_CACHE_MAX_SIZE_ATTRIB,
      DEFAULT_VECTOR_CENTROID_CACHE_MAX_SIZE);
    this.bruteforceLimit = this.conf.getInt(VECTOR_CENTROID_BRUTEFORCE_LIMIT_ATTRIB,
      DEFAULT_VECTOR_CENTROID_BRUTEFORCE_LIMIT);
    this.probeBuckets =
      this.conf.getInt(VECTOR_CENTROID_PROBE_BUCKETS_ATTRIB, DEFAULT_VECTOR_CENTROID_PROBE_BUCKETS);

    this.cache = CacheBuilder.newBuilder().maximumSize(maxSize).recordStats()
      .removalListener((RemovalListener<CacheKey, CachedCentroids>) notification -> {
        if (notification.getKey() != null) {
          LOG.debug("Evicted centroid cache for {} gen {} due to {}",
            notification.getKey().getIndexName(), notification.getKey().getGeneration(),
            notification.getCause());
        }
      }).build();
  }

  /** Returns the singleton instance of VectorCentroidCache for the given configuration. */
  public static VectorCentroidCache getInstance(Configuration conf) {
    if (conf != null) {
      suppliedConf = conf;
    }
    VectorCentroidCache result = defaultInstance;
    if (result == null) {
      synchronized (VectorCentroidCache.class) {
        result = defaultInstance;
        if (result == null) {
          defaultInstance = result = new VectorCentroidCache(conf);
        }
      }
    }
    if (conf != null && result.conf != conf) {
      result.conf = conf;
    }
    return result;
  }

  /**
   * Returns the singleton instance, preferring the last explicitly configured cluster configuration
   * or falling back to a default {@link HBaseConfiguration}.
   */
  public static VectorCentroidCache getInstance() {
    Configuration conf = suppliedConf;
    return getInstance(conf != null ? conf : HBaseConfiguration.create());
  }

  public static synchronized void resetInstance() {
    defaultInstance = null;
  }

  public static synchronized void setDefaultInstance(VectorCentroidCache instance) {
    defaultInstance = instance;
  }

  public Configuration getConfiguration() {
    return conf;
  }

  public void setConfiguration(Configuration conf) {
    if (conf != null) {
      suppliedConf = conf;
    }
    this.conf = conf;
  }

  public String getDefaultIndexName() {
    return defaultIndexName;
  }

  public void setDefaultIndexName(String defaultIndexName) {
    this.defaultIndexName = defaultIndexName;
  }

  public int getBruteforceLimit() {
    return bruteforceLimit;
  }

  public int getProbeBuckets() {
    return probeBuckets;
  }

  public CachedCentroids putCentroids(String indexName, long generation, List<byte[]> centroids) {
    String normalized = SchemaUtil.normalizeFullTableName(indexName);
    CachedCentroids cached =
      new CachedCentroids(normalized, generation, centroids, bruteforceLimit, probeBuckets);
    cache.put(new CacheKey(normalized, generation), cached);
    activeGenerations.put(normalized, generation);
    if (defaultIndexName == null) {
      defaultIndexName = normalized;
    }
    return cached;
  }

  public CachedCentroids putCentroids(String indexName, long generation, byte[][] centroids) {
    return putCentroids(indexName, generation,
      centroids == null ? Collections.emptyList() : Arrays.asList(centroids));
  }

  public CachedCentroids putCentroids(String indexName, long generation, float[][] centroids) {
    String normalized = SchemaUtil.normalizeFullTableName(indexName);
    CachedCentroids cached =
      new CachedCentroids(normalized, generation, centroids, bruteforceLimit, probeBuckets);
    cache.put(new CacheKey(normalized, generation), cached);
    activeGenerations.put(normalized, generation);
    if (defaultIndexName == null) {
      defaultIndexName = normalized;
    }
    return cached;
  }

  public CachedCentroids putCentroidsFromFloatList(String indexName, long generation,
    List<float[]> centroids) {
    String normalized = SchemaUtil.normalizeFullTableName(indexName);
    CachedCentroids cached = CachedCentroids.fromFloatVectors(normalized, generation, centroids,
      bruteforceLimit, probeBuckets);
    cache.put(new CacheKey(normalized, generation), cached);
    activeGenerations.put(normalized, generation);
    if (defaultIndexName == null) {
      defaultIndexName = normalized;
    }
    return cached;
  }

  public CachedCentroids putFloatCentroids(String indexName, long generation,
    List<float[]> centroids) {
    return putCentroidsFromFloatList(indexName, generation, centroids);
  }

  public CachedCentroids putFloatCentroids(long generation, List<float[]> centroids) {
    String index = resolveDefaultIndexName(true);
    return putCentroidsFromFloatList(index, generation, centroids);
  }

  public CachedCentroids putFloatCentroids(List<float[]> centroids) {
    return putFloatCentroids(1L, centroids);
  }

  public CachedCentroids loadCentroids(String indexName, long generation, Connection conn)
    throws SQLException {
    String normalized = SchemaUtil.normalizeFullTableName(indexName);
    if (conn == null) {
      throw new IllegalStateException("No Connection available to load centroids from "
        + "SYSTEM.VECTOR_CENTROID for index: " + normalized);
    }
    List<byte[]> centroids = CentroidManager.loadCentroids(conn, normalized, generation);
    CachedCentroids cached =
      new CachedCentroids(normalized, generation, centroids, bruteforceLimit, probeBuckets);
    cache.put(new CacheKey(normalized, generation), cached);
    activeGenerations.put(normalized, generation);
    if (defaultIndexName == null) {
      defaultIndexName = normalized;
    }
    return cached;
  }

  /** Concurrency locks coordinating on-demand centroid loading during index write mutations. */
  private final ConcurrentMap<CacheKey, Object> writeLoadLocks = new ConcurrentHashMap<>();

  /**
   * Retrieves centroids for index write assignment for the specified centroid generation, loading
   * and caching them from {@code SYSTEM.VECTOR_CENTROID} on a cache miss.
   * @param indexName  logical index table name
   * @param generation centroid generation recorded in the system catalog
   * @return cached centroid representation for vector assignment
   * @throws IllegalStateException if generation is null or centroids cannot be loaded
   */
  public CachedCentroids getCentroidsForWrite(String indexName, Long generation) {
    String normalized = SchemaUtil.normalizeFullTableName(indexName);
    if (generation == null) {
      throw new IllegalStateException("No centroid generation recorded for index " + normalized
        + "; its centroids have not been trained");
    }
    CacheKey key = new CacheKey(normalized, generation);
    CachedCentroids cached = cache.getIfPresent(key);
    if (cached != null) {
      return cached;
    }
    Object lock = writeLoadLocks.computeIfAbsent(key, k -> new Object());
    try {
      synchronized (lock) {
        cached = cache.getIfPresent(key);
        if (cached != null) {
          return cached;
        }
        Configuration configuration = this.conf != null
          ? this.conf
          : (suppliedConf != null ? suppliedConf : HBaseConfiguration.create());
        try (Connection conn = QueryUtil.getConnectionOnServer(configuration)) {
          cached = loadCentroids(normalized, generation, conn);
        } catch (SQLException e) {
          throw new IllegalStateException("Could not load centroids for index " + normalized
            + " generation " + generation + " from SYSTEM.VECTOR_CENTROID", e);
        }
        if (cached == null || cached.getCentroidCount() == 0) {
          throw new IllegalStateException("No centroids recorded for index " + normalized
            + " generation " + generation + "; the index cannot be maintained");
        }
        return cached;
      }
    } finally {
      writeLoadLocks.remove(key, lock);
    }
  }

  public CachedCentroids peekCentroids(String indexName, long generation) {
    String normalized = SchemaUtil.normalizeFullTableName(indexName);
    CacheKey key = new CacheKey(normalized, generation);
    return cache.getIfPresent(key);
  }

  public CachedCentroids peekCentroids(String indexName) {
    String normalized = SchemaUtil.normalizeFullTableName(indexName);
    Long activeGen = activeGenerations.get(normalized);
    if (activeGen != null) {
      CachedCentroids cached = cache.getIfPresent(new CacheKey(normalized, activeGen));
      if (cached != null) {
        return cached;
      }
    }
    for (Map.Entry<CacheKey, CachedCentroids> entry : cache.asMap().entrySet()) {
      if (entry.getKey().getIndexName().equals(normalized)) {
        return entry.getValue();
      }
    }
    return null;
  }

  public CachedCentroids loadCentroids(String indexName, Connection conn) throws SQLException {
    if (conn == null) {
      throw new IllegalStateException("No Connection available to load centroids for " + indexName);
    }
    String normalized = SchemaUtil.normalizeFullTableName(indexName);
    long gen = 1L;
    long catalogGen = CentroidManager.getGeneration(conn, normalized);
    if (catalogGen > 0) {
      gen = catalogGen;
    }
    return loadCentroids(normalized, gen, conn);
  }

  public CachedCentroids getCentroids(String indexName, Connection conn) {
    if (conn != null) {
      String normalized = SchemaUtil.normalizeFullTableName(indexName);
      Long activeGen = activeGenerations.get(normalized);
      if (activeGen != null) {
        CacheKey key = new CacheKey(normalized, activeGen);
        CachedCentroids cached = cache.getIfPresent(key);
        if (cached != null) {
          return cached;
        }
        try {
          return loadCentroids(normalized, activeGen, conn);
        } catch (SQLException e) {
          LOG.debug("Could not load centroids for {} gen {} via connection: {}", normalized,
            activeGen, e.getMessage());
        }
      }
      try {
        return loadCentroids(normalized, conn);
      } catch (SQLException e) {
        LOG.debug("Could not load centroids for {} via connection: {}", normalized, e.getMessage());
      }
    }
    return getCentroids(indexName);
  }

  /**
   * Returns the centroids for the given index at exactly the given generation, using the connection
   * to load them on a cache miss.
   */
  public CachedCentroids getCentroids(String indexName, long generation, Connection conn) {
    String normalized = SchemaUtil.normalizeFullTableName(indexName);
    CacheKey key = new CacheKey(normalized, generation);
    CachedCentroids cached = cache.getIfPresent(key);
    if (cached != null) {
      activeGenerations.put(normalized, generation);
      return cached;
    }
    if (conn != null) {
      try {
        return loadCentroids(normalized, generation, conn);
      } catch (SQLException e) {
        LOG.debug("Could not load centroids for {} gen {} via connection: {}", normalized,
          generation, e.getMessage());
      }
    }
    return getCentroids(indexName, generation);
  }

  public CachedCentroids getCentroids(String indexName, long generation) {
    String normalized = SchemaUtil.normalizeFullTableName(indexName);
    CacheKey key = new CacheKey(normalized, generation);
    CachedCentroids cached = cache.getIfPresent(key);
    if (cached != null) {
      return cached;
    }
    throw new IllegalStateException("No centroids in cache for index " + normalized + " generation "
      + generation + "; centroids must be loaded before use");
  }

  public CachedCentroids getCentroids(String indexName) {
    String normalized = SchemaUtil.normalizeFullTableName(indexName);

    Long activeGen = activeGenerations.get(normalized);
    if (activeGen != null) {
      CacheKey key = new CacheKey(normalized, activeGen);
      CachedCentroids cached = cache.getIfPresent(key);
      if (cached != null) {
        return cached;
      }
    }

    for (Map.Entry<CacheKey, CachedCentroids> entry : cache.asMap().entrySet()) {
      if (entry.getKey().getIndexName().equals(normalized)) {
        return entry.getValue();
      }
    }

    throw new IllegalStateException("No centroids found in cache for index: " + normalized);
  }

  public CachedCentroids getCentroids() {
    return getCentroids(resolveDefaultIndexName(false));
  }

  /**
   * Advances the generation for an index, evicting any stale cached entry under previous
   * generations.
   */
  public void advanceGeneration(String indexName, long newGeneration) {
    String normalized = SchemaUtil.normalizeFullTableName(indexName);
    Long priorGen = activeGenerations.put(normalized, newGeneration);
    if (priorGen != null && priorGen != newGeneration) {
      cache.invalidate(new CacheKey(normalized, priorGen));
    }
  }

  /** Evicts cached centroids for the given index and generation. */
  public void invalidate(String indexName, long generation) {
    String normalized = SchemaUtil.normalizeFullTableName(indexName);
    cache.invalidate(new CacheKey(normalized, generation));
    activeGenerations.remove(normalized, generation);
  }

  /** Evicts all cached entries for the given index. */
  public void invalidate(String indexName) {
    String normalized = SchemaUtil.normalizeFullTableName(indexName);
    activeGenerations.remove(normalized);
    for (CacheKey key : cache.asMap().keySet()) {
      if (key.getIndexName().equals(normalized)) {
        cache.invalidate(key);
      }
    }
  }

  /** Clears the entire centroid cache. */
  public void invalidateAll() {
    cache.invalidateAll();
    activeGenerations.clear();
  }

  public int findNearestCentroid(String indexName, long generation, byte[] queryVector,
    String metric) {
    return getCentroids(indexName, generation).findNearestCentroid(queryVector, metric);
  }

  public int findNearestCentroid(String indexName, long generation, byte[] queryVector, int offset,
    int length, String metric) {
    return getCentroids(indexName, generation).findNearestCentroid(queryVector, offset, length,
      metric);
  }

  public int findNearestCentroid(String indexName, long generation, float[] queryVector,
    String metric) {
    return getCentroids(indexName, generation).findNearestCentroid(queryVector, metric);
  }

  public int findNearestCentroid(String indexName, byte[] queryVector, String metric) {
    return getCentroids(indexName).findNearestCentroid(queryVector, metric);
  }

  public int findNearestCentroid(String indexName, byte[] queryVector, int offset, int length,
    String metric) {
    return getCentroids(indexName).findNearestCentroid(queryVector, offset, length, metric);
  }

  public int findNearestCentroid(String indexName, float[] queryVector, String metric) {
    return getCentroids(indexName).findNearestCentroid(queryVector, metric);
  }

  public int findNearestCentroid(byte[] queryVector, String metric) {
    return getCentroids().findNearestCentroid(queryVector, metric);
  }

  public int findNearestCentroid(byte[] queryVector, int offset, int length, String metric) {
    return getCentroids().findNearestCentroid(queryVector, offset, length, metric);
  }

  public int findNearestCentroid(float[] queryVector, String metric) {
    return getCentroids().findNearestCentroid(queryVector, metric);
  }

  public List<Integer> findNearestCentroids(String indexName, long generation, byte[] queryVector,
    String metric, int probeCount) {
    return getCentroids(indexName, generation).findNearestCentroids(queryVector, metric,
      probeCount);
  }

  public List<Integer> findNearestCentroids(String indexName, long generation, float[] queryVector,
    String metric, int probeCount) {
    return getCentroids(indexName, generation).findNearestCentroids(queryVector, metric,
      probeCount);
  }

  public List<Integer> findNearestCentroids(String indexName, byte[] queryVector, String metric,
    int probeCount) {
    return getCentroids(indexName).findNearestCentroids(queryVector, metric, probeCount);
  }

  public List<Integer> findNearestCentroids(String indexName, float[] queryVector, String metric,
    int probeCount) {
    return getCentroids(indexName).findNearestCentroids(queryVector, metric, probeCount);
  }

  public List<Integer> findNearestCentroids(byte[] queryVector, String metric, int probeCount) {
    return getCentroids().findNearestCentroids(queryVector, metric, probeCount);
  }

  public List<Integer> findNearestCentroids(float[] queryVector, String metric, int probeCount) {
    return getCentroids().findNearestCentroids(queryVector, metric, probeCount);
  }

  public int findNearestCentroidBruteForce(String indexName, long generation, byte[] queryVector,
    String metric) {
    return getCentroids(indexName, generation).findNearestCentroidBruteForce(queryVector, metric);
  }

  public int findNearestCentroidBruteForce(String indexName, long generation, float[] queryVector,
    String metric) {
    return getCentroids(indexName, generation).findNearestCentroidBruteForce(queryVector, metric);
  }

  public int findNearestCentroidBruteForce(String indexName, byte[] queryVector, String metric) {
    return getCentroids(indexName).findNearestCentroidBruteForce(queryVector, metric);
  }

  public int findNearestCentroidBruteForce(String indexName, float[] queryVector, String metric) {
    return getCentroids(indexName).findNearestCentroidBruteForce(queryVector, metric);
  }

  public int findNearestCentroidBruteForce(byte[] queryVector, String metric) {
    return getCentroids().findNearestCentroidBruteForce(queryVector, metric);
  }

  public int findNearestCentroidBruteForce(float[] queryVector, String metric) {
    return getCentroids().findNearestCentroidBruteForce(queryVector, metric);
  }

  public int findNearestCentroidHierarchical(String indexName, long generation, byte[] queryVector,
    String metric) {
    return getCentroids(indexName, generation).findNearestCentroidHierarchical(queryVector, metric);
  }

  public int findNearestCentroidHierarchical(String indexName, long generation, float[] queryVector,
    String metric) {
    return getCentroids(indexName, generation).findNearestCentroidHierarchical(queryVector, metric);
  }

  public int findNearestCentroidHierarchical(String indexName, byte[] queryVector, String metric) {
    return getCentroids(indexName).findNearestCentroidHierarchical(queryVector, metric);
  }

  public int findNearestCentroidHierarchical(String indexName, float[] queryVector, String metric) {
    return getCentroids(indexName).findNearestCentroidHierarchical(queryVector, metric);
  }

  public int findNearestCentroidHierarchical(byte[] queryVector, String metric) {
    return getCentroids().findNearestCentroidHierarchical(queryVector, metric);
  }

  public int findNearestCentroidHierarchical(float[] queryVector, String metric) {
    return getCentroids().findNearestCentroidHierarchical(queryVector, metric);
  }

  public List<Integer> findNearestCentroidsBruteForce(String indexName, long generation,
    byte[] queryVector, String metric, int probeCount) {
    return getCentroids(indexName, generation).findNearestCentroidsBruteForce(queryVector, metric,
      probeCount);
  }

  public List<Integer> findNearestCentroidsBruteForce(String indexName, long generation,
    float[] queryVector, String metric, int probeCount) {
    return getCentroids(indexName, generation).findNearestCentroidsBruteForce(queryVector, metric,
      probeCount);
  }

  public List<Integer> findNearestCentroidsBruteForce(String indexName, byte[] queryVector,
    String metric, int probeCount) {
    return getCentroids(indexName).findNearestCentroidsBruteForce(queryVector, metric, probeCount);
  }

  public List<Integer> findNearestCentroidsBruteForce(String indexName, float[] queryVector,
    String metric, int probeCount) {
    return getCentroids(indexName).findNearestCentroidsBruteForce(queryVector, metric, probeCount);
  }

  public List<Integer> findNearestCentroidsBruteForce(byte[] queryVector, String metric,
    int probeCount) {
    return getCentroids().findNearestCentroidsBruteForce(queryVector, metric, probeCount);
  }

  public List<Integer> findNearestCentroidsBruteForce(float[] queryVector, String metric,
    int probeCount) {
    return getCentroids().findNearestCentroidsBruteForce(queryVector, metric, probeCount);
  }

  public List<Integer> findNearestCentroidsHierarchical(String indexName, long generation,
    byte[] queryVector, String metric, int probeCount) {
    return getCentroids(indexName, generation).findNearestCentroidsHierarchical(queryVector, metric,
      probeCount);
  }

  public List<Integer> findNearestCentroidsHierarchical(String indexName, long generation,
    float[] queryVector, String metric, int probeCount) {
    return getCentroids(indexName, generation).findNearestCentroidsHierarchical(queryVector, metric,
      probeCount);
  }

  public List<Integer> findNearestCentroidsHierarchical(String indexName, byte[] queryVector,
    String metric, int probeCount) {
    return getCentroids(indexName).findNearestCentroidsHierarchical(queryVector, metric,
      probeCount);
  }

  public List<Integer> findNearestCentroidsHierarchical(String indexName, float[] queryVector,
    String metric, int probeCount) {
    return getCentroids(indexName).findNearestCentroidsHierarchical(queryVector, metric,
      probeCount);
  }

  public List<Integer> findNearestCentroidsHierarchical(byte[] queryVector, String metric,
    int probeCount) {
    return getCentroids().findNearestCentroidsHierarchical(queryVector, metric, probeCount);
  }

  public List<Integer> findNearestCentroidsHierarchical(float[] queryVector, String metric,
    int probeCount) {
    return getCentroids().findNearestCentroidsHierarchical(queryVector, metric, probeCount);
  }

  public long getCacheSize() {
    return cache.size();
  }

  public long getHitCount() {
    return cache.stats().hitCount();
  }

  public long getMissCount() {
    return cache.stats().missCount();
  }

  public double getHitRate() {
    return cache.stats().hitRate();
  }

  public long getEvictionCount() {
    return cache.stats().evictionCount();
  }

  public CacheStats getStats() {
    return cache.stats();
  }

  private String resolveDefaultIndexName(boolean generateIfNone) {
    if (defaultIndexName != null) {
      return defaultIndexName;
    }
    if (activeGenerations.size() == 1) {
      return activeGenerations.keySet().iterator().next();
    }
    if (cache.asMap().size() == 1) {
      return cache.asMap().keySet().iterator().next().getIndexName();
    }
    if (generateIfNone) {
      String generated = "DEFAULT_VECTOR_INDEX";
      defaultIndexName = generated;
      return generated;
    }
    throw new IllegalStateException(
      "Index name must be specified when multiple or no indexes are cached");
  }
}
