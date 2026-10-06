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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
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

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Collects inline scorecard deltas for vector indexes in a region server.
 * <p>
 * Each write compares the old and new index rows of a data row. From this comparison, the write
 * gets the change to the population of each posting list and the number of reassignments. The
 * accumulator keeps these deltas in memory. A background thread flushes them periodically to
 * {@code SYSTEM.VECTOR_CENTROID} as atomic increments. There is one instance in each process.
 * <p>
 * Each buffered entry also keeps the batch time of its oldest write. A scorecard recount sets a
 * time fence on each centroid. The flush applies a population delta only if its oldest write is not
 * older than the fence. Thus a delta that waits in the buffer during a recount does not count two
 * times. A merge keeps the older time, so a merged delta never counts two times. The fence can drop
 * a delta that the recount did not count. The next recount repairs this error.
 */
public final class ScorecardAccumulator {

  private static final Logger LOGGER = LoggerFactory.getLogger(ScorecardAccumulator.class);

  private static volatile ScorecardAccumulator instance;

  private final Configuration conf;
  /** Buffered entries of population delta, reassignment delta, and oldest write time. */
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

  /** Returns the accumulator of this process, and creates it on the first call. */
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

  /** Returns the accumulator of this process, or null if no caller created it yet. */
  public static ScorecardAccumulator getInstance() {
    return instance;
  }

  /**
   * Adds the scorecard deltas of the index mutations for one data row to a batch. A new index row
   * adds one to its posting list. A removed index row subtracts one. A move of the row to a
   * different posting list also counts one reassignment. A covered column update on the same index
   * row does not change the scorecard.
   * @param maintainer index maintainer of the vector index
   * @param current    old image of the data row, or null if the row is new
   * @param vector     indexed vector of the old data row image, or null
   * @param mutations  index mutations for this update of the data row
   * @param pending    the deltas of the batch, which this call changes
   */
  public static void collect(IndexMaintainer maintainer, Put current, ImmutableBytesWritable vector,
    Collection<Mutation> mutations, Map<Key, long[]> pending) {
    byte[] written = null;
    byte[] deleted = null;
    for (Mutation mutation : mutations) {
      if (mutation instanceof Put) {
        written = mutation.getRow();
      }
    }
    for (Mutation mutation : mutations) {
      // Only a Delete of a different row key removes an old index row. A Delete of the new row
      // key is part of a covered column update.
      if (mutation instanceof Delete && !Bytes.equals(mutation.getRow(), written)) {
        deleted = mutation.getRow();
      }
    }
    String indexName = CentroidManager.getCentroidIndexName(maintainer.getLogicalIndexName());
    long generation = maintainer.getCentroidGeneration();
    if (written != null) {
      int to = maintainer.getCentroidId(written);
      if (deleted != null) {
        add(pending, new Key(indexName, generation, maintainer.getCentroidId(deleted)), -1, 0);
        add(pending, new Key(indexName, generation, to), 1, 1);
      } else if (current == null || !maintainer.shouldPrepareIndexMutations(current, vector)) {
        add(pending, new Key(indexName, generation, to), 1, 0);
      }
    } else if (deleted != null) {
      add(pending, new Key(indexName, generation, maintainer.getCentroidId(deleted)), -1, 0);
    }
  }

  /**
   * Adds a population delta and a reassignment delta for a centroid. If the delta adds a vector to
   * the posting list, it also counts one assignment to the centroid. In a batch, the deltas of each
   * kind for one centroid add together. Thus a population delta of +1 and a population delta of -1
   * cancel. The assignment count only increases, so it does not cancel.
   */
  private static void add(Map<Key, long[]> pending, Key key, long size, long reassigned) {
    long[] delta = pending.computeIfAbsent(key, k -> new long[3]);
    delta[0] += size;
    delta[1] += reassigned;
    if (size > 0) {
      delta[2] += size;
    }
  }

  /**
   * Adds the deltas of a committed batch to the process buffer and updates the metrics.
   * @param batchDeltas    the deltas of the batch
   * @param batchTimestamp the server time of the batch, which is also the time of its index rows
   */
  public void accumulate(Map<Key, long[]> batchDeltas, long batchTimestamp) {
    MetricsVectorIndexSource metrics =
      MetricsIndexerSourceFactory.getInstance().getMetricsVectorIndexSource();
    for (Map.Entry<Key, long[]> entry : batchDeltas.entrySet()) {
      long[] delta = entry.getValue();
      if (delta[0] != 0 || delta[1] != 0) {
        merge(entry.getKey(), new long[] { delta[0], delta[1], batchTimestamp });
      }
      if (delta[2] > 0) {
        metrics.incrementVectorCentroidAssignments(entry.getKey().indexName, delta[2]);
      }
      if (delta[1] > 0) {
        metrics.incrementVectorCentroidReassignments(entry.getKey().indexName, delta[1]);
      }
    }
  }

  private void merge(Key key, long[] delta) {
    deltas.merge(key, delta, ScorecardAccumulator::combine);
  }

  /** Adds two buffered entries and keeps the older write time. */
  static long[] combine(long[] a, long[] b) {
    return new long[] { a[0] + b[0], a[1] + b[1], Math.min(a[2], b[2]) };
  }

  /** Returns a copy of the buffered entry of a centroid, or null if there is none. */
  @VisibleForTesting
  long[] buffered(Key key) {
    long[] entry = deltas.get(key);
    return entry == null ? null : entry.clone();
  }

  private void flushQuietly() {
    try {
      flush();
    } catch (Throwable t) {
      LOGGER.warn("Vector index scorecard flush failed; deltas are retained for the next flush", t);
    }
  }

  /**
   * Flushes the buffered deltas to {@code SYSTEM.VECTOR_CENTROID} as atomic counter increments.
   * Each index generation has its own commit. The update does not create rows, so the deltas of a
   * dropped index or a deleted generation have no effect, unless the flush races with the delete.
   * If the commit of a generation does not complete, its deltas go back to the buffer for the next
   * flush. This is necessary because a recount can recover populations from index row counts, but
   * it cannot recover reassignment counts. Thus a commit that fails after it applies some
   * increments can count those increments two times.
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
    // The flush time of each index, for all of its generations
    Map<String, Long> flushTimes = new HashMap<>();
    try (Connection conn = QueryUtil.getConnectionOnServer(conf)) {
      Iterator<Map.Entry<Key, Map<Key, long[]>>> pending = byGeneration.entrySet().iterator();
      while (pending.hasNext()) {
        Map.Entry<Key, Map<Key, long[]>> entry = pending.next();
        Key generation = entry.getKey();
        long start = EnvironmentEdgeManager.currentTimeMillis();
        flush(conn, generation, entry.getValue());
        flushTimes.merge(generation.indexName, EnvironmentEdgeManager.currentTimeMillis() - start,
          Long::sum);
        pending.remove();
      }
    } finally {
      for (Map<Key, long[]> unflushed : byGeneration.values()) {
        unflushed.forEach(this::merge);
      }
      MetricsVectorIndexSource metrics =
        MetricsIndexerSourceFactory.getInstance().getMetricsVectorIndexSource();
      flushTimes.forEach(metrics::updateVectorScorecardFlushTime);
    }
  }

  /**
   * Commits the deltas of one generation. The update does not create rows. Thus the deltas of a
   * generation that is deleted before the commit have no effect. An update that races with the
   * delete can write a partial row, and the next debris delete removes it.
   */
  static void flush(Connection conn, Key generation, Map<Key, long[]> deltas) throws SQLException {
    List<ScorecardRow> rows = new ArrayList<>(deltas.size());
    for (Map.Entry<Key, long[]> delta : deltas.entrySet()) {
      long[] entry = delta.getValue();
      rows.add(new ScorecardRow(delta.getKey().centroidId, entry[0], entry[1], entry[2]));
    }
    CentroidManager.adjustScorecard(conn, generation.indexName, generation.generation, rows);
  }

  /** Identifies one centroid of one generation of one index. */
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
