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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CENTROID_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CENTROID_VECTOR;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.GENERATION_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_VECTOR_CENTROID_NAME;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.metrics.MetricsIndexerSourceFactory;
import org.apache.phoenix.hbase.index.metrics.MetricsVectorIndexSource;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.QueryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * RegionServer-level accumulator for inline vector index scorecard delta tracking.
 * <p>
 * Evaluates pre- and post-mutation row states to maintain accurate posting list populations and
 * centroid reassignment frequencies during write operations. Scorecard deltas are buffered in
 * memory and flushed periodically to {@code SYSTEM.VECTOR_CENTROID} via atomic increment upserts.
 */
public final class ScorecardAccumulator {

  private static final Logger LOGGER = LoggerFactory.getLogger(ScorecardAccumulator.class);

  private static volatile ScorecardAccumulator instance;

  private final Configuration conf;
  private final ConcurrentHashMap<Key, long[]> deltas = new ConcurrentHashMap<>();

  private ScorecardAccumulator(Configuration conf) {
    this.conf = conf;
    long interval = conf.getLong(QueryServices.VECTOR_SCORECARD_FLUSH_INTERVAL_MS_ATTRIB,
      QueryServicesOptions.DEFAULT_VECTOR_SCORECARD_FLUSH_INTERVAL_MS);
    if (interval > 0) {
      ScheduledExecutorService flusher = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("vector-scorecard-flush").setDaemon(true).build());
      flusher.scheduleWithFixedDelay(this::flushQuietly, interval, interval, TimeUnit.MILLISECONDS);
    }
  }

  /** Returns the singleton region server accumulator instance, initializing if necessary. */
  public static ScorecardAccumulator getInstance(Configuration conf) {
    ScorecardAccumulator result = instance;
    if (result == null) {
      synchronized (ScorecardAccumulator.class) {
        result = instance;
        if (result == null) {
          instance = result = new ScorecardAccumulator(conf);
        }
      }
    }
    return result;
  }

  /** Returns the singleton region server accumulator instance, or null if uninitialized. */
  public static ScorecardAccumulator getInstance() {
    return instance;
  }

  /**
   * Derives scorecard deltas from index mutations for a single primary data row.
   * @param maintainer index maintainer for the target vector index
   * @param current    prior data row image, or null if row is new
   * @param mutations  generated index mutations for the update
   * @param pending    map collecting pending centroid deltas
   */
  public static void collect(IndexMaintainer maintainer, Put current,
    Collection<Mutation> mutations, Map<Key, long[]> pending) {
    byte[] written = null;
    byte[] deleted = null;
    for (Mutation mutation : mutations) {
      if (mutation instanceof Put) {
        written = mutation.getRow();
      }
    }
    for (Mutation mutation : mutations) {
      // Distinguish prior row key deletion from covered column updates on the new row key
      if (mutation instanceof Delete && !Bytes.equals(mutation.getRow(), written)) {
        deleted = mutation.getRow();
      }
    }
    String indexName = maintainer.getLogicalIndexName();
    long generation = maintainer.getCentroidGeneration();
    if (written != null) {
      int to = maintainer.getCentroidId(written);
      if (deleted != null) {
        add(pending, new Key(indexName, generation, maintainer.getCentroidId(deleted)), -1, 0);
        add(pending, new Key(indexName, generation, to), 1, 1);
      } else if (current == null || !maintainer.hasIndexRow(current)) {
        add(pending, new Key(indexName, generation, to), 1, 0);
      }
    } else if (deleted != null) {
      add(pending, new Key(indexName, generation, maintainer.getCentroidId(deleted)), -1, 0);
    }
  }

  private static void add(Map<Key, long[]> pending, Key key, long size, long reassigned) {
    long[] delta = pending.computeIfAbsent(key, k -> new long[2]);
    delta[0] += size;
    delta[1] += reassigned;
  }

  /** Aggregates batch scorecard deltas and updates metrics counters upon commit. */
  public void accumulate(Map<Key, long[]> batchDeltas) {
    MetricsVectorIndexSource metrics =
      MetricsIndexerSourceFactory.getInstance().getMetricsVectorIndexSource();
    for (Map.Entry<Key, long[]> entry : batchDeltas.entrySet()) {
      long[] delta = entry.getValue();
      if (delta[0] == 0 && delta[1] == 0) {
        continue;
      }
      merge(entry.getKey(), delta.clone());
      if (delta[0] > 0) {
        metrics.incrementVectorCentroidAssignments(entry.getKey().indexName, delta[0]);
      }
      if (delta[1] > 0) {
        metrics.incrementVectorCentroidReassignments(entry.getKey().indexName, delta[1]);
      }
    }
  }

  private void merge(Key key, long[] delta) {
    deltas.merge(key, delta, (a, b) -> new long[] { a[0] + b[0], a[1] + b[1] });
  }

  private void flushQuietly() {
    try {
      flush();
    } catch (Throwable t) {
      LOGGER.warn("Vector index scorecard flush failed; deltas are retained for the next flush", t);
    }
  }

  /**
   * Flushes buffered deltas to {@code SYSTEM.VECTOR_CENTROID} using atomic counter increments, one
   * commit per index generation. Deltas for dropped indexes or retired generations are discarded.
   * Deltas of a generation whose commit did not complete are returned to the buffer and retried by
   * the next flush, since reassignment counts, unlike populations, cannot be recovered by
   * reconciliation. A commit that fails after partially applying may therefore be counted twice.
   */
  public synchronized void flush() throws SQLException {
    if (deltas.isEmpty()) {
      return;
    }
    Map<Key, Map<Key, long[]>> byGeneration = new HashMap<>();
    for (Key key : deltas.keySet()) {
      long[] delta = deltas.remove(key);
      if (delta != null) {
        byGeneration.computeIfAbsent(key.generationKey(), k -> new HashMap<>()).put(key, delta);
      }
    }
    List<String> indexNames =
      byGeneration.keySet().stream().map(k -> k.indexName).distinct().collect(Collectors.toList());
    long start = EnvironmentEdgeManager.currentTimeMillis();
    try (Connection conn = QueryUtil.getConnectionOnServer(conf)) {
      Iterator<Map.Entry<Key, Map<Key, long[]>>> pending = byGeneration.entrySet().iterator();
      while (pending.hasNext()) {
        Map.Entry<Key, Map<Key, long[]>> entry = pending.next();
        Key generation = entry.getKey();
        if (isLive(conn, generation)) {
          List<ScorecardRow> rows = new ArrayList<>(entry.getValue().size());
          for (Map.Entry<Key, long[]> delta : entry.getValue().entrySet()) {
            rows.add(new ScorecardRow(delta.getKey().centroidId, delta.getValue()[0],
              delta.getValue()[1]));
          }
          CentroidManager.adjustScorecard(conn, generation.indexName, generation.generation, rows);
        }
        pending.remove();
      }
    } finally {
      for (Map<Key, long[]> unflushed : byGeneration.values()) {
        unflushed.forEach(this::merge);
      }
    }
    long elapsed = EnvironmentEdgeManager.currentTimeMillis() - start;
    MetricsVectorIndexSource metrics =
      MetricsIndexerSourceFactory.getInstance().getMetricsVectorIndexSource();
    indexNames.forEach(name -> metrics.updateVectorScorecardFlushTime(name, elapsed));
  }

  /**
   * Verifies whether centroid metadata for the specified generation remains active. A failed read
   * propagates, so that a catalog outage retains deltas rather than treating them as retired.
   */
  private static boolean isLive(Connection conn, Key generationKey) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement("SELECT 1 FROM " + SYSTEM_VECTOR_CENTROID_NAME
      + " WHERE " + INDEX_NAME + " = ? AND " + GENERATION_ID + " = ? AND " + CENTROID_ID
      + " >= 0 AND " + CENTROID_VECTOR + " IS NOT NULL LIMIT 1")) {
      ps.setString(1, generationKey.indexName);
      ps.setLong(2, generationKey.generation);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  /** Composite key identifying a specific centroid within a generation and index. */
  public static final class Key {
    private final String indexName;
    private final long generation;
    private final int centroidId;

    public Key(String indexName, long generation, int centroidId) {
      this.indexName = Objects.requireNonNull(indexName, "indexName");
      this.generation = generation;
      this.centroidId = centroidId;
    }

    Key generationKey() {
      return new Key(indexName, generation, 0);
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
      return generation == other.generation && centroidId == other.centroidId
        && indexName.equals(other.indexName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(indexName, generation, centroidId);
    }

    @Override
    public String toString() {
      return indexName + ":" + generation + ":" + centroidId;
    }
  }
}
