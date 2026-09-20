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
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.optimize.DistanceMetric;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVectorDouble;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.apache.phoenix.util.QueryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.cache.Cache;
import org.apache.phoenix.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.phoenix.thirdparty.com.google.common.cache.CacheStats;

/**
 * Process-wide LRU cache of IVF centroid models, keyed by index name and generation ID. The model
 * of a generation does not change, so the cache does not refresh an entry. A byte limit on the
 * centroid vectors controls the size of the cache. A miss loads the model from
 * {@code SYSTEM.VECTOR_CENTROID}, and concurrent misses for the same key share one load.
 */
public final class VectorCentroidCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(VectorCentroidCache.class);

  private static volatile VectorCentroidCache instance;
  /** The server configuration that index maintenance uses to load centroids on a cache miss. */
  private static volatile Configuration serverConf;

  private final Cache<Key, CachedCentroids> cache;
  private final long maxBytes;

  VectorCentroidCache(long maxBytes) {
    this.maxBytes = maxBytes;
    // Use one segment, so that the byte limit applies to the whole cache. Guava divides a weight
    // limit equally among segments, and at insert it evicts a model heavier than one share.
    this.cache = CacheBuilder.newBuilder().maximumWeight(maxBytes)
      .weigher((Key key, CachedCentroids centroids) -> centroids.getWeight()).concurrencyLevel(1)
      .recordStats().build();
  }

  /**
   * Returns the process-wide cache. The first call sets the byte limit from {@code conf}, and later
   * calls ignore {@code conf}.
   */
  public static VectorCentroidCache getInstance(Configuration conf) {
    VectorCentroidCache result = instance;
    if (result == null) {
      synchronized (VectorCentroidCache.class) {
        result = instance;
        if (result == null) {
          instance = result = new VectorCentroidCache(
            conf.getLong(QueryServices.VECTOR_CENTROID_CACHE_MAX_BYTES_ATTRIB,
              QueryServicesOptions.DEFAULT_VECTOR_CENTROID_CACHE_MAX_BYTES));
        }
      }
    }
    return result;
  }

  /**
   * Returns the centroids of the index generation, and loads them on a cache miss.
   * @throws SQLException if no centroids are recorded for the generation, or if the read fails. The
   *                      cache does not keep a failed load, so a later call tries again.
   */
  public CachedCentroids get(Connection conn, String indexName, long generation,
    DistanceMetric metric) throws SQLException {
    Key key = new Key(indexName, generation);
    try {
      return cache.get(key, () -> checkWeight(key, load(conn, key, metric)));
    } catch (ExecutionException e) {
      if (e.getCause() instanceof SQLException) {
        throw (SQLException) e.getCause();
      }
      throw new SQLException(e.getCause());
    }
  }

  /**
   * Sets the server configuration that {@link #getForWrite} uses to load centroids on a cache miss
   * without a caller connection. Only the first call sets the value.
   */
  public static void setServerConfiguration(Configuration conf) {
    if (serverConf == null) {
      serverConf = conf;
    }
  }

  /**
   * Returns the centroids that vector index maintenance uses for writes. A cache miss loads them
   * through {@code conn}, or through an internal server connection if {@code conn} is null.
   * @throws SQLException if no centroids are recorded for the generation, if the load fails, or if
   *                      {@code conn} is null and no server configuration is set
   */
  public static CachedCentroids getForWrite(Connection conn, String indexName, long generation,
    DistanceMetric metric) throws SQLException {
    VectorCentroidCache cache = instance;
    CachedCentroids centroids = cache == null ? null : cache.getIfPresent(indexName, generation);
    if (centroids != null) {
      return centroids;
    }
    if (conn != null) {
      return getInstance(conn.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration())
        .get(conn, indexName, generation, metric);
    }
    Configuration conf = serverConf;
    if (conf == null) {
      throw new SQLException("Centroids of vector index " + indexName + " generation " + generation
        + " are not loaded and no server connection is available");
    }
    try (Connection serverConn = QueryUtil.getConnectionOnServer(conf)) {
      return getInstance(conf).get(serverConn, indexName, generation, metric);
    }
  }

  /** Returns the cached centroids of the index generation, or null on a miss. It does not load. */
  public CachedCentroids getIfPresent(String indexName, long generation) {
    return cache.getIfPresent(new Key(indexName, generation));
  }

  /** Puts a trained model into the cache, so that the process does not load it again. */
  public void put(String indexName, long generation, CachedCentroids centroids) {
    Key key = new Key(indexName, generation);
    cache.put(key, checkWeight(key, centroids));
  }

  /** Removes all cached generations of the index. */
  public void invalidate(String indexName) {
    cache.asMap().keySet().removeIf(k -> k.indexName.equals(indexName));
  }

  public long size() {
    return cache.size();
  }

  public CacheStats stats() {
    return cache.stats();
  }

  /**
   * Logs a warning if the model is heavier than the whole cache. The cache evicts such a model at
   * insert, so each use loads it again.
   */
  private CachedCentroids checkWeight(Key key, CachedCentroids centroids) {
    if (centroids.getWeight() > maxBytes) {
      LOGGER.warn(
        "Centroids of vector index {} generation {} take {} bytes, more than {} = {}, so the"
          + " cache cannot retain them and queries reload them on each use",
        key.indexName, key.generation, centroids.getWeight(),
        QueryServices.VECTOR_CENTROID_CACHE_MAX_BYTES_ATTRIB, maxBytes);
    }
    return centroids;
  }

  private static CachedCentroids load(Connection conn, Key key, DistanceMetric metric)
    throws SQLException {
    List<float[]> centroids = CentroidManager.loadCentroids(conn, key.indexName, key.generation);
    if (centroids.isEmpty()) {
      // A generation without recorded centroids shows that the caller has stale index metadata
      throw new SQLExceptionInfo.Builder(SQLExceptionCode.STALE_METADATA_CACHE_EXCEPTION)
        .setMessage("No centroids recorded for vector index " + key.indexName + " generation "
          + key.generation)
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
   * Immutable centroid model of one generation. It assigns a vector to its nearest centroid, and it
   * selects the nearest centroids that a query probes. It uses
   * {@link KMeansTrainer#assignmentDistance}, so training, writes and probes route a vector the
   * same way.
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

    /** Returns the size of the centroid vectors in bytes, at most Integer.MAX_VALUE. */
    int getWeight() {
      return (int) Math.min(Integer.MAX_VALUE, (long) centroids.length * dimension * Float.BYTES);
    }

    public DistanceMetric getMetric() {
      return metric;
    }

    /** Returns the centroid vector that has the specified centroid ID. */
    public float[] getCentroid(int id) {
      return centroids[id];
    }

    /**
     * Returns the ID of the centroid nearest to the vector. Equal distances resolve to the lowest
     * centroid ID.
     */
    public int assign(float[] vector) {
      checkDimension(vector.length);
      vector = saturate(vector);
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

    /** Returns the ID of the centroid nearest to a packed FLOAT or DOUBLE vector value. */
    public int assign(byte[] buf, int offset, int length, PDataType<?> vectorType) {
      return assign(decode(buf, offset, length, vectorType));
    }

    /**
     * Returns the IDs of the {@code n} centroids nearest to {@code vector}, in ascending distance
     * order. Equal distances sort by ascending ID, so the first ID is the ID that {@link #assign}
     * returns.
     */
    public int[] nearest(float[] vector, int n) {
      checkDimension(vector.length);
      vector = saturate(vector);
      int count = Math.min(n, centroids.length);
      double[] dist = new double[centroids.length];
      PriorityQueue<Integer> farthestFirst = new PriorityQueue<>(count + 1, (a, b) -> {
        int cmp = Double.compare(dist[b], dist[a]);
        return cmp != 0 ? cmp : Integer.compare(b, a);
      });
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

    /**
     * Decodes packed vector bytes into a float array. A DOUBLE element beyond float range becomes
     * infinite.
     */
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

    /**
     * Replaces each infinite component with the largest finite float of the same sign. A finite
     * DOUBLE element beyond float range narrows to an infinite component. Under COSINE, the vector
     * then routes by its direction. Under L2 and INNER_PRODUCT, the saturated component dominates
     * every distance, so the distances tie. {@link #assign} and {@link #nearest} resolve the tie to
     * the lowest centroid ID, so writes, probes and inspection route the vector the same way.
     */
    private static float[] saturate(float[] vector) {
      float[] out = vector;
      for (int i = 0; i < vector.length; i++) {
        if (Float.isInfinite(vector[i])) {
          if (out == vector) {
            out = vector.clone();
          }
          out[i] = Math.copySign(Float.MAX_VALUE, vector[i]);
        }
      }
      return out;
    }

    private void checkDimension(int length) {
      if (length != dimension) {
        throw new IllegalArgumentException(
          "Vector dimension " + length + " does not match centroid dimension " + dimension);
      }
    }
  }
}
