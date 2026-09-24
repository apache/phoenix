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

import java.io.Closeable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.hbase.index.metrics.MetricsIndexerSourceFactory;
import org.apache.phoenix.hbase.index.metrics.MetricsVectorIndexSource;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Buffers per-centroid scorecard deltas in RegionServer memory and flushes them on a background
 * schedule governed by {@code phoenix.vector.index.scorecard.flush.interval.ms}.
 */
public class ScorecardAccumulator implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(ScorecardAccumulator.class);

  private static volatile ScorecardAccumulator defaultInstance;

  private final Configuration conf;
  private volatile Connection connection;
  private final ConcurrentMap<CentroidKey, DeltaCounters> deltas = new ConcurrentHashMap<>();
  private final ScheduledExecutorService scheduler;
  private final long flushIntervalMs;

  /** Composite key identifying a specific centroid within an index generation. */
  public static final class CentroidKey {
    private final String indexName;
    private final long generationId;
    private final int centroidId;

    public CentroidKey(String indexName, long generationId, int centroidId) {
      this.indexName = SchemaUtil
        .normalizeFullTableName(Objects.requireNonNull(indexName, "indexName must not be null"));
      this.generationId = generationId;
      this.centroidId = centroidId;
    }

    public String getIndexName() {
      return indexName;
    }

    public long getGenerationId() {
      return generationId;
    }

    public int getCentroidId() {
      return centroidId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof CentroidKey)) {
        return false;
      }
      CentroidKey other = (CentroidKey) o;
      return generationId == other.generationId && centroidId == other.centroidId
        && Objects.equals(indexName, other.indexName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(indexName, generationId, centroidId);
    }

    @Override
    public String toString() {
      return indexName + ":" + generationId + ":" + centroidId;
    }
  }

  /** Holds atomic deltas for cluster size and reassignment count. */
  public static final class DeltaCounters {
    final AtomicLong clusterSizeDelta = new AtomicLong(0);
    final AtomicLong reassignCountDelta = new AtomicLong(0);

    public DeltaCounters() {
    }

    public DeltaCounters(long clusterSizeDelta, long reassignCountDelta) {
      this.clusterSizeDelta.set(clusterSizeDelta);
      this.reassignCountDelta.set(reassignCountDelta);
    }

    public long getClusterSizeDelta() {
      return clusterSizeDelta.get();
    }

    public long getReassignCountDelta() {
      return reassignCountDelta.get();
    }
  }

  public ScorecardAccumulator(Configuration conf) {
    // Retain null configuration if unprovided to prevent connecting to a default ZooKeeper quorum.
    this.conf = conf;
    this.flushIntervalMs = conf == null
      ? 0L
      : conf.getLong(QueryServices.VECTOR_INDEX_SCORECARD_FLUSH_INTERVAL_MS_ATTRIB,
        QueryServicesOptions.DEFAULT_VECTOR_INDEX_SCORECARD_FLUSH_INTERVAL_MS);

    if (flushIntervalMs > 0) {
      this.scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
        .setNameFormat("vector-scorecard-flush-%d").setDaemon(true).build());
      this.scheduler.scheduleWithFixedDelay(() -> {
        try {
          flush();
        } catch (Throwable t) {
          LOG.warn("Background scorecard flush failed: {}", t.getMessage(), t);
        }
      }, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
    } else {
      this.scheduler = null;
    }
  }

  public static ScorecardAccumulator getInstance(Configuration conf) {
    if (defaultInstance == null) {
      synchronized (ScorecardAccumulator.class) {
        if (defaultInstance == null) {
          defaultInstance = new ScorecardAccumulator(conf);
        }
      }
    }
    return defaultInstance;
  }

  /**
   * Returns the singleton accumulator instance. If unconfigured, the instance buffers deltas
   * without scheduling background flushes.
   */
  public static ScorecardAccumulator getInstance() {
    return getInstance(null);
  }

  public Connection getConnection() {
    return connection;
  }

  public void setConnection(Connection connection) {
    this.connection = connection;
  }

  /**
   * Accumulates deltas for the specified centroid.
   */
  public void accumulate(String indexName, long generationId, int centroidId, long clusterSizeDelta,
    long reassignCountDelta) {
    if (clusterSizeDelta == 0 && reassignCountDelta == 0) {
      return;
    }
    CentroidKey key = new CentroidKey(indexName, generationId, centroidId);
    DeltaCounters counters = deltas.computeIfAbsent(key, k -> new DeltaCounters());
    counters.clusterSizeDelta.addAndGet(clusterSizeDelta);
    counters.reassignCountDelta.addAndGet(reassignCountDelta);
  }

  public long getClusterSizeDelta(String indexName, long generationId, int centroidId) {
    CentroidKey key = new CentroidKey(indexName, generationId, centroidId);
    DeltaCounters counters = deltas.get(key);
    return counters != null ? counters.clusterSizeDelta.get() : 0L;
  }

  public long getReassignCountDelta(String indexName, long generationId, int centroidId) {
    CentroidKey key = new CentroidKey(indexName, generationId, centroidId);
    DeltaCounters counters = deltas.get(key);
    return counters != null ? counters.reassignCountDelta.get() : 0L;
  }

  public void clear() {
    deltas.clear();
  }

  protected Connection resolveConnection() throws SQLException {
    if (this.connection != null) {
      return this.connection;
    }
    Connection threadConn = CentroidManager.getThreadLocalConnection();
    if (threadConn != null) {
      return threadConn;
    }
    Connection defConn = CentroidManager.getDefaultConnection();
    if (defConn != null) {
      return defConn;
    }
    if (this.conf != null) {
      try {
        // Cache server connection across flush invocations to avoid connection churn.
        Connection serverConn = QueryUtil.getConnectionOnServer(this.conf);
        this.connection = serverConn;
        return serverConn;
      } catch (Exception e) {
        LOG.debug("Could not obtain server connection via QueryUtil: {}", e.getMessage());
      }
    }
    return null;
  }

  /**
   * Flushes all buffered deltas using the resolved connection.
   */
  public synchronized void flush() throws SQLException {
    Connection conn = resolveConnection();
    if (conn == null) {
      LOG.warn("No connection available to flush scorecard accumulator");
      return;
    }
    flush(conn);
  }

  /**
   * Flushes all buffered deltas using the provided connection.
   */
  public synchronized void flush(Connection conn) throws SQLException {
    if (conn == null) {
      throw new IllegalArgumentException("Connection must not be null");
    }
    if (deltas.isEmpty()) {
      return;
    }

    Map<CentroidKey, DeltaCounters> drained = new HashMap<>();
    for (Iterator<Map.Entry<CentroidKey, DeltaCounters>> it = deltas.entrySet().iterator(); it
      .hasNext();) {
      Map.Entry<CentroidKey, DeltaCounters> entry = it.next();
      DeltaCounters counters = entry.getValue();
      long sizeDelta = counters.clusterSizeDelta.getAndSet(0);
      long reassignDelta = counters.reassignCountDelta.getAndSet(0);
      if (sizeDelta != 0 || reassignDelta != 0) {
        drained.put(entry.getKey(), new DeltaCounters(sizeDelta, reassignDelta));
      } else {
        it.remove();
      }
    }

    if (drained.isEmpty()) {
      return;
    }

    String upsertSql = "UPSERT INTO " + PhoenixDatabaseMetaData.SYSTEM_VECTOR_CENTROID_NAME + " ("
      + PhoenixDatabaseMetaData.INDEX_NAME + ", " + PhoenixDatabaseMetaData.GENERATION_ID + ", "
      + PhoenixDatabaseMetaData.CENTROID_ID + ", " + PhoenixDatabaseMetaData.CLUSTER_SIZE + ", "
      + PhoenixDatabaseMetaData.REASSIGN_COUNT + ") VALUES (?, ?, ?, ?, ?) "
      + "ON DUPLICATE KEY UPDATE " + PhoenixDatabaseMetaData.CLUSTER_SIZE + " = "
      + PhoenixDatabaseMetaData.CLUSTER_SIZE + " + ?, " + PhoenixDatabaseMetaData.REASSIGN_COUNT
      + " = " + PhoenixDatabaseMetaData.REASSIGN_COUNT + " + ?";

    long startTime = System.currentTimeMillis();
    try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
      for (Map.Entry<CentroidKey, DeltaCounters> entry : drained.entrySet()) {
        CentroidKey key = entry.getKey();
        DeltaCounters delta = entry.getValue();
        ps.setString(1, key.getIndexName());
        ps.setLong(2, key.getGenerationId());
        ps.setInt(3, key.getCentroidId());
        ps.setLong(4, delta.clusterSizeDelta.get());
        ps.setLong(5, delta.reassignCountDelta.get());
        ps.setLong(6, delta.clusterSizeDelta.get());
        ps.setLong(7, delta.reassignCountDelta.get());
        ps.executeUpdate();
      }
      conn.commit();

      // Record flush duration once per distinct index flushed.
      long duration = System.currentTimeMillis() - startTime;
      MetricsVectorIndexSource metrics =
        MetricsIndexerSourceFactory.getInstance().getMetricsVectorIndexSource();
      Set<String> flushedIndexes = new HashSet<>();
      for (CentroidKey key : drained.keySet()) {
        if (flushedIndexes.add(key.getIndexName())) {
          metrics.updateVectorScorecardFlushTime(key.getIndexName(), duration);
        }
      }
    } catch (SQLException e) {
      // Re-accumulate drained deltas on failure
      for (Map.Entry<CentroidKey, DeltaCounters> entry : drained.entrySet()) {
        CentroidKey key = entry.getKey();
        DeltaCounters delta = entry.getValue();
        accumulate(key.getIndexName(), key.getGenerationId(), key.getCentroidId(),
          delta.clusterSizeDelta.get(), delta.reassignCountDelta.get());
      }
      throw e;
    }
  }

  @Override
  public void close() {
    shutdown();
  }

  public void shutdown() {
    if (scheduler != null && !scheduler.isShutdown()) {
      scheduler.shutdown();
      try {
        if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
          scheduler.shutdownNow();
        }
      } catch (InterruptedException e) {
        scheduler.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
  }
}
