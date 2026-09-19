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
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.index.vector.CentroidManager;
import org.apache.phoenix.index.vector.KMeansTrainer;
import org.apache.phoenix.optimize.DistanceMetric;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVectorDouble;
import org.apache.phoenix.schema.types.PVectorFloat;

import org.apache.phoenix.thirdparty.com.google.common.cache.Cache;
import org.apache.phoenix.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.phoenix.thirdparty.com.google.common.cache.CacheStats;

/**
 * Process-wide LRU cache of IVF centroid models keyed by index name and generation ID. Centroid
 * models are immutable per generation. Missing entries are loaded on demand from
 * {@code SYSTEM.VECTOR_CENTROID} with concurrent misses for the same key coalesced into a single
 * load.
 */
public final class VectorCentroidCache {

  private static volatile VectorCentroidCache instance;

  private final Cache<Key, CachedCentroids> cache;

  VectorCentroidCache(long maxEntries) {
    this.cache = CacheBuilder.newBuilder().maximumSize(maxEntries).recordStats().build();
  }

  /** Returns the singleton cache instance initialized from configuration. */
  public static VectorCentroidCache getInstance(Configuration conf) {
    VectorCentroidCache result = instance;
    if (result == null) {
      synchronized (VectorCentroidCache.class) {
        result = instance;
        if (result == null) {
          instance = result = new VectorCentroidCache(
            conf.getLong(QueryServices.VECTOR_CENTROID_CACHE_MAX_SIZE_ATTRIB,
              QueryServicesOptions.DEFAULT_VECTOR_CENTROID_CACHE_MAX_SIZE));
        }
      }
    }
    return result;
  }

  /**
   * Retrieves centroids for the specified index and generation, loading on cache miss.
   * @throws SQLException if centroids are missing or unreadable; empty loads are not cached
   */
  public CachedCentroids get(Connection conn, String indexName, long generation,
    DistanceMetric metric) throws SQLException {
    Key key = new Key(indexName, generation);
    try {
      return cache.get(key, () -> load(conn, key, metric));
    } catch (ExecutionException e) {
      if (e.getCause() instanceof SQLException) {
        throw (SQLException) e.getCause();
      }
      throw new SQLException(e.getCause());
    }
  }

  /** Returns cached centroids for the specified key if present, without loading on miss. */
  public CachedCentroids getIfPresent(String indexName, long generation) {
    return cache.getIfPresent(new Key(indexName, generation));
  }

  /** Installs a trained centroid model directly into the cache. */
  public void put(String indexName, long generation, CachedCentroids centroids) {
    cache.put(new Key(indexName, generation), centroids);
  }

  /** Invalidates all cached generations for the given index. */
  public void invalidate(String indexName) {
    cache.asMap().keySet().removeIf(k -> k.indexName.equals(indexName));
  }

  public long size() {
    return cache.size();
  }

  public CacheStats stats() {
    return cache.stats();
  }

  private static CachedCentroids load(Connection conn, Key key, DistanceMetric metric)
    throws SQLException {
    List<float[]> centroids = CentroidManager.loadCentroids(conn, key.indexName, key.generation);
    if (centroids.isEmpty()) {
      throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS).setMessage(
        "No centroids recorded for vector index " + key.indexName + " generation " + key.generation)
        .build().buildException();
    }
    return new CachedCentroids(centroids, metric);
  }

  private static final class Key {
    private final String indexName;
    private final long generation;

    Key(String indexName, long generation) {
      this.indexName = Objects.requireNonNull(indexName, "indexName");
      this.generation = generation;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Key)) {
        return false;
      }
      Key other = (Key) o;
      return generation == other.generation && indexName.equals(other.indexName);
    }

    @Override
    public int hashCode() {
      return 31 * indexName.hashCode() + Long.hashCode(generation);
    }
  }

  /**
   * Immutable centroid model supporting exact nearest-centroid assignment and multi-probe neighbor
   * selection using {@link KMeansTrainer#assignmentDistance}.
   */
  public static final class CachedCentroids {
    private final float[][] centroids;
    private final int dimension;
    private final DistanceMetric metric;

    public CachedCentroids(List<float[]> centroids, DistanceMetric metric) {
      if (centroids.isEmpty()) {
        throw new IllegalArgumentException("centroids must not be empty");
      }
      this.centroids = centroids.toArray(new float[0][]);
      this.dimension = this.centroids[0].length;
      this.metric = Objects.requireNonNull(metric, "metric");
    }

    public int size() {
      return centroids.length;
    }

    public int getDimension() {
      return dimension;
    }

    public DistanceMetric getMetric() {
      return metric;
    }

    /** Returns the centroid vector at the specified index. */
    public float[] getCentroid(int id) {
      return centroids[id];
    }

    /** Returns the ID of the centroid nearest to the given vector. */
    public int assign(float[] vector) {
      checkDimension(vector.length);
      int best = 0;
      double bestDist = Double.MAX_VALUE;
      for (int c = 0; c < centroids.length; c++) {
        double d = KMeansTrainer.assignmentDistance(metric, vector, centroids[c]);
        if (d < bestDist) {
          bestDist = d;
          best = c;
        }
      }
      return best;
    }

    /**
     * Returns the ID of the centroid nearest to a packed vector value.
     */
    public int assign(byte[] buf, int offset, int length, PDataType<?> vectorType) {
      return assign(decode(buf, offset, length, vectorType));
    }

    /**
     * Returns the IDs of the {@code n} centroids nearest to {@code vector}, in ascending distance
     * order.
     */
    public int[] nearest(float[] vector, int n) {
      checkDimension(vector.length);
      int count = Math.min(n, centroids.length);
      double[] dist = new double[centroids.length];
      PriorityQueue<Integer> farthestFirst =
        new PriorityQueue<>(count + 1, (a, b) -> Double.compare(dist[b], dist[a]));
      for (int c = 0; c < centroids.length; c++) {
        dist[c] = KMeansTrainer.assignmentDistance(metric, vector, centroids[c]);
        farthestFirst.add(c);
        if (farthestFirst.size() > count) {
          farthestFirst.poll();
        }
      }
      int[] result = new int[count];
      for (int i = count - 1; i >= 0; i--) {
        result[i] = farthestFirst.poll();
      }
      return result;
    }

    /** Decodes packed vector bytes into a float array. */
    public static float[] decode(byte[] buf, int offset, int length, PDataType<?> vectorType) {
      if (vectorType == PVectorDouble.INSTANCE) {
        double[] d = PVectorDouble.readElements(buf, offset, length);
        float[] f = new float[d.length];
        for (int i = 0; i < d.length; i++) {
          f[i] = (float) d[i];
        }
        return f;
      }
      return PVectorFloat.readElements(buf, offset, length);
    }

    private void checkDimension(int length) {
      if (length != dimension) {
        throw new IllegalArgumentException(
          "Vector dimension " + length + " does not match centroid dimension " + dimension);
      }
    }
  }
}
