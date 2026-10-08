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
package org.apache.phoenix.hbase.index.vector;

import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.hnsw.HnswOffheapAllocator;
import org.apache.phoenix.hbase.index.hnsw.HnswSegment;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Manages an HNSW vector index for a single HBase region.
 * <p>
 * Index state combines an immutable on disk {@link HnswSegment} with an in-memory graph that
 * buffers incremental row mutations. When rows are modified or deleted, corresponding segment
 * entries are masked during search. Background flush tasks periodically rebuild the immutable
 * segment from base table scans and replace the active segment.
 * <p>
 * Unflushed mutations are recovered during region initialization by replaying mutations that
 * occurred after the current segment's creation timestamp.
 */
public final class HnswIndexManager implements VectorIndexManager {
  private static final Logger LOG = LoggerFactory.getLogger(HnswIndexManager.class);
  private static final VectorTypeSupport VTS =
    VectorizationProvider.getInstance().getVectorTypeSupport();
  /** Number of accumulated mutations before triggering a segment flush. */
  public static final int FLUSH_THRESHOLD = 10_000;
  /** Maximum number of delta segments permitted before forcing a full rebuild. */
  public static final int MAX_DELTAS = 4;
  /** Maximum ratio of accumulated delta changes to base segment size before forcing a rebuild. */
  public static final double REBUILD_RATIO = 0.25;
  /** Delay before retrying a failed rebuild or flush. */
  public static final long RETRY_DELAY_MS = 60_000;
  /** Lookback window applied during mutation replay to account for concurrent writes. */
  public static final long REPLAY_MARGIN_MS = 60_000;
  // The replay margin in effect; tests shorten it rather than wait it out
  private static volatile long replayMarginMs = REPLAY_MARGIN_MS;
  private static final float NEIGHBOR_OVERFLOW = 1.2f;
  // Limit concurrent rebuild operations per RegionServer to bound memory usage
  private static final ExecutorService REBUILDS = Executors.newSingleThreadExecutor(r -> {
    Thread t = new Thread(r, "hnsw-rebuild");
    t.setDaemon(true);
    return t;
  });
  private static final ScheduledExecutorService RETRIES =
    Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "hnsw-rebuild-retry");
      t.setDaemon(true);
      return t;
    });

  private final RegionCoprocessorEnvironment env;
  private final Region region;
  private final String indexName;
  private final byte[] startKey;
  private final byte[] endKey;
  private final PTable.VectorIndex vectorIndex;
  private final IndexMaintainer maintainer;
  private final TableName indexTable;
  private final byte[] family;
  private final VectorSimilarityFunction similarity;
  private final HnswOffheapAllocator allocator;

  /**
   * Readable segment source with its mutation mask. A segment may cover rows outside this region (a
   * split parent or merge input); searches are clipped to the region's key range.
   */
  private static final class Source {
    final HnswSegment segment;
    final HnswSegment.Descriptor descriptor;
    // Total mutated rows represented by this segment
    final int changes;
    // Segment row ordinals invalidated by subsequent updates or deletions
    final Set<Integer> mask = ConcurrentHashMap.newKeySet();

    Source(HnswSegment segment, HnswSegment.Descriptor descriptor) {
      this.segment = segment;
      this.descriptor = descriptor;
      this.changes = segment.size() + segment.getTombstones().length;
    }
  }

  /** Immutable snapshot of active segment sources and in-memory graph state used for queries. */
  private static final class State {
    final List<Source> sources;
    final MutableGraph mutable;
    // Active flush state: in-flight graph being serialized and keys mutated during flush
    final MutableGraph flushing;
    final Set<ImmutableBytesPtr> changedSinceSwap;
    final long swapTime;

    State(List<Source> sources, MutableGraph mutable, MutableGraph flushing,
      Set<ImmutableBytesPtr> changedSinceSwap, long swapTime) {
      this.sources = sources;
      this.mutable = mutable;
      this.flushing = flushing;
      this.changedSinceSwap = changedSinceSwap;
      this.swapTime = swapTime;
    }
  }

  private volatile State state;
  private boolean rebuilding;
  // Target base segment timestamp for an in-progress delta flush, or null for a full rebuild
  private Long deltaBase;
  private boolean closed;

  /**
   * Opens or recovers an HNSW index manager for the specified region and index.
   * @param env       region coprocessor environment
   * @param indexName logical index name
   * @return initialized manager instance
   * @throws IOException if the index or base table cannot be accessed
   */
  static HnswIndexManager open(RegionCoprocessorEnvironment env, String indexName)
    throws IOException {
    try (PhoenixConnection conn =
      QueryUtil.getConnectionOnServer(env.getConfiguration()).unwrap(PhoenixConnection.class)) {
      PTable index = conn.getTableNoCache(indexName);
      PTable data = conn.getTableNoCache(index.getParentName().getString());
      return new HnswIndexManager(env, index, index.getIndexMaintainer(data, conn),
        index.getIndexState() == PIndexState.ACTIVE);
    } catch (SQLException e) {
      throw new IOException("Cannot open HNSW index " + indexName, e);
    }
  }

  private HnswIndexManager(RegionCoprocessorEnvironment env, PTable index,
    IndexMaintainer maintainer, boolean active) throws IOException {
    this.env = env;
    this.region = env.getRegion();
    this.indexName = index.getName().getString();
    this.startKey = region.getRegionInfo().getStartKey();
    this.endKey = region.getRegionInfo().getEndKey();
    this.vectorIndex = index.getVectorIndex();
    this.maintainer = maintainer;
    this.indexTable = TableName.valueOf(index.getPhysicalName().getBytes());
    this.family = SchemaUtil.getEmptyColumnFamily(index);
    this.similarity = HnswSegment.similarityFunction(vectorIndex.getDistanceMetric());
    this.allocator = HnswOffheapAllocator.get(env.getConfiguration());

    List<HnswSegment.Descriptor> current;
    List<Source> sources;
    do {
      current = listCurrentSegments();
      sources = openSources(current, Collections.emptyMap());
    } while (sources == null);
    applyStackMasks(sources);
    this.state = new State(sources, new MutableGraph(), null, null, 0);
    if (!current.isEmpty()) {
      replay(replayStartTime(current));
    }
    LOG.info("Opened HNSW index {} for region {} with {} segments", indexName,
      region.getRegionInfo().getEncodedName(), sources.size());
    // Rebuild immediately if the region lacks an exact base segment stack
    boolean exact = exactBase(current, startKey, endKey) != null;
    if (!exact && (active || !current.isEmpty())) {
      flush(true);
    }
  }

  private static final class StackId {
    final ImmutableBytesPtr startKey;
    final long baseTime;

    StackId(byte[] startKey, long baseTime) {
      this.startKey = new ImmutableBytesPtr(startKey);
      this.baseTime = baseTime;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof StackId)) {
        return false;
      }
      StackId other = (StackId) o;
      return baseTime == other.baseTime && startKey.equals(other.startKey);
    }

    @Override
    public int hashCode() {
      return 31 * startKey.hashCode() + Long.hashCode(baseTime);
    }
  }

  private static StackId stackId(HnswSegment.Descriptor d) {
    return new StackId(d.startKey, d.isDelta() ? d.baseTime : d.time);
  }

  /**
   * Applies invalidation masks across segment stacks so that newer deltas and tombstones shadow
   * older entries in the stack.
   */
  private static void applyStackMasks(List<Source> sources) {
    Map<StackId, List<Source>> stacks = new LinkedHashMap<>();
    for (Source source : sources) {
      stacks.computeIfAbsent(stackId(source.descriptor), k -> new ArrayList<>()).add(source);
    }
    for (List<Source> stack : stacks.values()) {
      Set<ImmutableBytesPtr> newerKeys = new HashSet<>();
      for (Source source : stack) {
        for (ImmutableBytesPtr key : newerKeys) {
          int ordinal = source.segment.ordinalOf(key);
          if (ordinal >= 0) {
            source.mask.add(ordinal);
          }
        }
        if (source.descriptor.isDelta()) {
          for (int i = 0; i < source.segment.size(); i++) {
            newerKeys.add(new ImmutableBytesPtr(source.segment.getKey(i)));
          }
          for (byte[] tombstone : source.segment.getTombstones()) {
            newerKeys.add(new ImmutableBytesPtr(tombstone));
          }
        }
      }
    }
  }

  /** Returns the lookback margin applied during mutation replay. */
  public static long getReplayMarginMs() {
    return replayMarginMs;
  }

  /**
   * Sets the lookback margin applied during mutation replay; {@link #REPLAY_MARGIN_MS} by default.
   */
  @VisibleForTesting
  public static void setReplayMarginMs(long marginMs) {
    replayMarginMs = marginMs;
  }

  /**
   * Determines the replay start timestamp across active stacks, accounting for the replay lookback
   * margin.
   */
  public static long replayStartTime(List<HnswSegment.Descriptor> current) {
    Map<StackId, Long> newestPerStack = new HashMap<>();
    for (HnswSegment.Descriptor d : current) {
      StackId id = stackId(d);
      newestPerStack.merge(id, d.time, Math::max);
    }
    long oldestNewest = Long.MAX_VALUE;
    for (long t : newestPerStack.values()) {
      oldestNewest = Math.min(oldestNewest, t);
    }
    return Math.max(0, oldestNewest - replayMarginMs);
  }

  /**
   * Finds active segments covering this region, retaining base segments not superseded by newer
   * bases alongside their associated deltas.
   */
  public static List<HnswSegment.Descriptor> currentSegments(List<HnswSegment.Descriptor> all,
    byte[] startKey, byte[] endKey) {
    List<HnswSegment.Descriptor> overlappingBases = new ArrayList<>();
    List<HnswSegment.Descriptor> overlappingDeltas = new ArrayList<>();
    for (HnswSegment.Descriptor d : all) {
      if (d.overlaps(startKey, endKey)) {
        if (d.isDelta()) {
          overlappingDeltas.add(d);
        } else {
          overlappingBases.add(d);
        }
      }
    }
    List<HnswSegment.Descriptor> currentBases = new ArrayList<>();
    for (HnswSegment.Descriptor b : overlappingBases) {
      List<HnswSegment.Descriptor> newerBases = new ArrayList<>();
      for (HnswSegment.Descriptor n : overlappingBases) {
        if (n.time > b.time) {
          newerBases.add(n);
        }
      }
      byte[] from = Bytes.compareTo(b.startKey, startKey) > 0 ? b.startKey : startKey;
      byte[] to =
        endKey.length == 0 || (b.endKey.length > 0 && Bytes.compareTo(b.endKey, endKey) < 0)
          ? b.endKey
          : endKey;
      if (!HnswSegment.covered(from, to, newerBases)) {
        currentBases.add(b);
      }
    }
    List<HnswSegment.Descriptor> current = new ArrayList<>(currentBases);
    for (HnswSegment.Descriptor d : overlappingDeltas) {
      for (HnswSegment.Descriptor b : currentBases) {
        if (d.baseTime == b.time && Bytes.equals(d.startKey, b.startKey)) {
          current.add(d);
          break;
        }
      }
    }
    current.sort((a, b) -> Long.compare(b.time, a.time));
    return current;
  }

  private List<HnswSegment.Descriptor> listCurrentSegments() throws IOException {
    try (Table table = env.getConnection().getTable(indexTable)) {
      return currentSegments(HnswSegment.list(table, family), startKey, endKey);
    }
  }

  /**
   * Returns sources for the given segments in order, newest first, reusing those already open and
   * opening the rest. Returns null if a segment row was retired after it was listed, so the caller
   * lists again.
   */
  private List<Source> openSources(List<HnswSegment.Descriptor> current,
    Map<ImmutableBytesPtr, Source> open) throws IOException {
    List<Source> sources = new ArrayList<>(current.size());
    List<Source> opened = new ArrayList<>();
    try {
      for (HnswSegment.Descriptor d : current) {
        Source source = open.get(new ImmutableBytesPtr(d.rowKey));
        if (source == null && (d.count > 0 || d.isDelta())) {
          source = new Source(HnswSegment.open(env.getConnection(), indexTable, family, d.rowKey,
            allocator, vectorIndex.getDistanceMetric()), d);
          opened.add(source);
        }
        if (source != null) {
          sources.add(source);
        }
      }
      return sources;
    } catch (HnswSegment.NotFoundException e) {
      for (Source source : opened) {
        source.segment.close();
      }
      return null;
    }
  }

  /**
   * Replaces a source whose segment row was retired with the newer segments now covering this
   * region. A split parent is retired once every daughter has written its own segment, which can
   * precede a daughter's cutover to that segment. Returns false if the segment is still listed, so
   * its payload is missing for some other reason.
   */
  private boolean replaceRetired(List<Source> searched, Source retired) throws IOException {
    Map<ImmutableBytesPtr, Source> open = new HashMap<>();
    for (Source source : searched) {
      open.put(new ImmutableBytesPtr(source.descriptor.rowKey), source);
    }
    List<Source> sources;
    do {
      List<HnswSegment.Descriptor> current = listCurrentSegments();
      for (HnswSegment.Descriptor d : current) {
        if (Bytes.equals(d.rowKey, retired.descriptor.rowKey)) {
          return false;
        }
      }
      sources = openSources(current, open);
    } while (sources == null);
    List<Source> close = new ArrayList<>();
    synchronized (this) {
      State s = state;
      if (closed || s.sources != searched) {
        // A cutover or another replacement installed new sources; the caller searches those
        for (Source source : sources) {
          if (!searched.contains(source)) {
            close.add(source);
          }
        }
      } else {
        // Buffered mutations are newer than any segment and shadow the newly opened ones
        Set<ImmutableBytesPtr> buffered = new HashSet<>(s.mutable.live().keySet());
        buffered.addAll(s.mutable.deleted());
        if (s.flushing != null) {
          buffered.addAll(s.flushing.live().keySet());
          buffered.addAll(s.flushing.deleted());
        }
        for (Source source : sources) {
          if (!searched.contains(source)) {
            for (ImmutableBytesPtr key : buffered) {
              int ordinal = source.segment.ordinalOf(key);
              if (ordinal >= 0) {
                source.mask.add(ordinal);
              }
            }
          }
        }
        applyStackMasks(sources);
        for (Source source : searched) {
          if (!sources.contains(source)) {
            close.add(source);
          }
        }
        state = new State(sources, s.mutable, s.flushing, s.changedSinceSwap, s.swapTime);
        LOG.info("Replaced retired HNSW segment {} of index {} for region {} with {} segments",
          retired.descriptor, indexName, region.getRegionInfo().getEncodedName(), sources.size());
      }
    }
    for (Source source : close) {
      source.segment.close();
    }
    return true;
  }

  @Override
  public void onMutation(Put currentDataRowState, Put nextDataRowState) {
    float[] next = nextDataRowState == null
      ? null
      : maintainer.getVectorAsFloats(new IndexUtil.SimpleValueGetter(nextDataRowState),
        HConstants.LATEST_TIMESTAMP);
    float[] current = currentDataRowState == null
      ? null
      : maintainer.getVectorAsFloats(new IndexUtil.SimpleValueGetter(currentDataRowState),
        HConstants.LATEST_TIMESTAMP);
    if (Arrays.equals(next, current)) {
      return;
    }
    Put row = nextDataRowState != null ? nextDataRowState : currentDataRowState;
    apply(new ImmutableBytesPtr(row.getRow()), next);
  }

  // Applies an upsert or delete mutation to the in-memory graph and updates segment masks
  private synchronized void apply(ImmutableBytesPtr key, float[] vector) {
    State s = state;
    for (Source source : s.sources) {
      int ordinal = source.segment.ordinalOf(key);
      if (ordinal >= 0) {
        source.mask.add(ordinal);
      }
    }
    if (s.changedSinceSwap != null) {
      s.changedSinceSwap.add(key);
    }
    if (vector != null) {
      s.mutable.upsert(key, vector);
    } else {
      s.mutable.delete(key);
    }
    if (s.mutable.changes() >= FLUSH_THRESHOLD) {
      flush(false);
    }
  }

  // Replays row mutations that occurred at or after the given timestamp
  private void replay(long since) throws IOException {
    Set<ImmutableBytesPtr> changed = new HashSet<>();
    Scan scan = new Scan().setRaw(true).setFilter(new KeyOnlyFilter());
    scan.setTimeRange(Math.max(0, since), HConstants.LATEST_TIMESTAMP);
    try (RegionScanner scanner = region.getScanner(scan)) {
      List<Cell> cells = new ArrayList<>();
      boolean more;
      do {
        more = scanner.next(cells);
        if (!cells.isEmpty()) {
          changed.add(new ImmutableBytesPtr(CellUtil.cloneRow(cells.get(0))));
          cells.clear();
        }
      } while (more);
    }
    for (ImmutableBytesPtr key : changed) {
      apply(key, vectorOf(region.get(new Get(key.copyBytesIfNecessary()))));
    }
    LOG.info("Replayed {} rows changed since {} into HNSW index {} for region {}", changed.size(),
      since, indexName, region.getRegionInfo().getEncodedName());
  }

  private float[] vectorOf(Result row) {
    if (row.isEmpty()) {
      return null;
    }
    Put put = new Put(row.getRow());
    for (Cell cell : row.rawCells()) {
      try {
        put.add(cell);
      } catch (IOException e) {
        throw new IllegalStateException(e); // Impossible, the cells are from this row
      }
    }
    return maintainer.getVectorAsFloats(new IndexUtil.SimpleValueGetter(put),
      HConstants.LATEST_TIMESTAMP);
  }

  /** Triggers an immediate segment flush. */
  public void flush() {
    flush(false);
  }

  /** Triggers an immediate full segment rebuild. */
  public void rebuild() {
    flush(true);
  }

  // Swaps active mutable state and submits a background segment rebuild task
  private synchronized void flush(boolean force) {
    State s = state;
    // Secondary replicas serve queries read-only and do not participate in segment rebuilds
    if (closed || rebuilding || region.getRegionInfo().getReplicaId() != 0) {
      return;
    }
    if (s.flushing == null) {
      if (!force && s.mutable.isEmpty() && !hasMasks(s)) {
        return;
      }
      deltaBase = force ? null : deltaBase(s.sources, s.mutable.changes(), startKey, endKey);
      state = new State(s.sources, new MutableGraph(), s.mutable, ConcurrentHashMap.newKeySet(),
        EnvironmentEdgeManager.currentTimeMillis());
    } else if (force) {
      deltaBase = null;
    }
    rebuilding = true;
    REBUILDS.submit(this::rebuildSegment);
  }

  // Retries a previously failed flush or rebuild operation
  private synchronized void retryFlush() {
    if (closed || rebuilding || state.flushing == null) {
      return;
    }
    rebuilding = true;
    REBUILDS.submit(this::rebuildSegment);
  }

  private static boolean hasMasks(State s) {
    for (Source source : s.sources) {
      if (!source.mask.isEmpty()) {
        return true;
      }
    }
    return false;
  }

  private void rebuildSegment() {
    Long base = null;
    try {
      State s;
      synchronized (this) {
        if (closed) {
          return;
        }
        s = state;
        base = deltaBase;
      }
      if (base != null) {
        writeDelta(s, base);
      } else {
        writeFull(s);
      }
    } catch (Throwable t) {
      LOG.error("HNSW index {} {} failed for region {}; will retry", indexName,
        base != null ? "delta flush" : "rebuild", region.getRegionInfo().getEncodedName(), t);
      RETRIES.schedule(this::retryFlush, RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
    } finally {
      synchronized (this) {
        rebuilding = false;
      }
    }
  }

  // Performs a full region scan to build and install a new base segment
  private void writeFull(State s) throws IOException {
    Map<ImmutableBytesPtr, float[]> vectors = scanRegion();
    s.flushing.applyTo(vectors);
    List<VectorFloat<?>> values = new ArrayList<>(vectors.size());
    byte[][] keys = new byte[vectors.size()][];
    int i = 0;
    for (Map.Entry<ImmutableBytesPtr, float[]> e : vectors.entrySet()) {
      keys[i++] = e.getKey().copyBytesIfNecessary();
      values.add(VTS.createFloatVector(e.getValue()));
    }
    byte[] payload = keys.length > 0
      ? HnswSegment.build(vectorIndex,
        new ListRandomAccessVectorValues(values, vectorIndex.getDimension()), keys)
      : null;
    HnswSegment.Descriptor written;
    try (Table table = env.getConnection().getTable(indexTable)) {
      written =
        HnswSegment.write(table, family, startKey, endKey, s.swapTime, payload, keys.length);
    }
    HnswSegment segment = payload != null
      ? HnswSegment.open(env.getConnection(), indexTable, family, written.rowKey, allocator,
        vectorIndex.getDistanceMetric())
      : null;
    if (cutover(segment, written)) {
      retireSupersededSegments();
    }
    LOG.info("Rebuilt HNSW index {} for region {} with {} vectors", indexName,
      region.getRegionInfo().getEncodedName(), keys.length);
  }

  // Flushes buffered mutations as a delta segment stacked on an existing base segment
  private void writeDelta(State s, long baseTime) throws IOException {
    Map<ImmutableBytesPtr, float[]> live = s.flushing.live();
    List<VectorFloat<?>> values = new ArrayList<>(live.size());
    byte[][] keys = new byte[live.size()][];
    int i = 0;
    for (Map.Entry<ImmutableBytesPtr, float[]> e : live.entrySet()) {
      keys[i++] = e.getKey().copyBytesIfNecessary();
      values.add(VTS.createFloatVector(e.getValue()));
    }
    byte[] payload = keys.length > 0
      ? HnswSegment.build(vectorIndex,
        new ListRandomAccessVectorValues(values, vectorIndex.getDimension()), keys)
      : null;

    Set<ImmutableBytesPtr> deleted = s.flushing.deleted();
    byte[][] deletedKeys = new byte[deleted.size()][];
    int d = 0;
    for (ImmutableBytesPtr del : deleted) {
      deletedKeys[d++] = del.copyBytesIfNecessary();
    }
    HnswSegment.Descriptor written;
    try (Table table = env.getConnection().getTable(indexTable)) {
      written = HnswSegment.write(table, family, startKey, endKey, s.swapTime, payload, keys.length,
        baseTime, deletedKeys);
    }
    HnswSegment segment = HnswSegment.open(env.getConnection(), indexTable, family, written.rowKey,
      allocator, vectorIndex.getDistanceMetric());
    if (cutover(segment, written)) {
      LOG.info(
        "Wrote HNSW delta for index {} region {} stacked on base {} with {} vectors and {} tombstones",
        indexName, region.getRegionInfo().getEncodedName(), baseTime, keys.length,
        deletedKeys.length);
    }
  }

  // The full-precision vectors of every row in the region
  private Map<ImmutableBytesPtr, float[]> scanRegion() throws IOException {
    Map<ImmutableBytesPtr, float[]> vectors = new LinkedHashMap<>();
    Scan scan = new Scan();
    for (ColumnReference ref : maintainer.getAllColumnsForDataTable()) {
      scan.addColumn(ref.getFamily(), ref.getQualifier());
    }
    try (RegionScanner scanner = region.getScanner(scan)) {
      List<Cell> cells = new ArrayList<>();
      boolean more;
      do {
        more = scanner.next(cells);
        if (!cells.isEmpty()) {
          float[] vector = vectorOf(Result.create(cells));
          if (vector != null) {
            vectors.put(new ImmutableBytesPtr(CellUtil.cloneRow(cells.get(0))), vector);
          }
          cells.clear();
        }
      } while (more);
    }
    return vectors;
  }

  // Installs the newly written segment into active state and releases flushed mutations
  private synchronized boolean cutover(HnswSegment segment, HnswSegment.Descriptor written) {
    State s = state;
    boolean isDelta = written.isDelta();
    if (closed) {
      if (segment != null) {
        segment.close();
      }
      return false;
    }
    List<Source> sources = new ArrayList<>(isDelta ? s.sources.size() + 1 : 1);
    if (segment != null) {
      Source source = new Source(segment, written);
      for (ImmutableBytesPtr key : s.changedSinceSwap) {
        int ordinal = segment.ordinalOf(key);
        if (ordinal >= 0) {
          source.mask.add(ordinal);
        }
      }
      sources.add(source);
    }
    if (isDelta) {
      sources.addAll(s.sources);
    } else {
      for (Source old : s.sources) {
        old.segment.close();
      }
    }
    state = new State(sources, s.mutable, null, null, 0);
    return true;
  }

  /**
   * Returns the base segment timestamp to stack a delta upon, or null if a full rebuild is required
   * due to threshold limits or topology changes.
   */
  static Long deltaBase(List<Source> sources, int flushChanges, byte[] startKey, byte[] endKey) {
    List<HnswSegment.Descriptor> stack = new ArrayList<>(sources.size());
    int changes = flushChanges;
    for (Source source : sources) {
      stack.add(source.descriptor);
      if (source.descriptor.isDelta()) {
        changes += source.changes;
      }
    }
    HnswSegment.Descriptor base = exactBase(stack, startKey, endKey);
    return base != null && stack.size() <= MAX_DELTAS && changes < REBUILD_RATIO * base.count
      ? base.time
      : null;
  }

  /**
   * Returns the base descriptor if the given segments form a single coherent stack exactly spanning
   * the specified key range, or null otherwise.
   */
  static HnswSegment.Descriptor exactBase(List<HnswSegment.Descriptor> segments, byte[] startKey,
    byte[] endKey) {
    HnswSegment.Descriptor base = null;
    for (HnswSegment.Descriptor d : segments) {
      if (!d.covers(startKey, endKey) || (!d.isDelta() && base != null)) {
        return null;
      }
      if (!d.isDelta()) {
        base = d;
      }
    }
    for (HnswSegment.Descriptor d : segments) {
      if (base == null || (d != base && d.baseTime != base.time)) {
        return null;
      }
    }
    return base;
  }

  /**
   * Deletes segment rows fully covered by newer segments. Delta segments are retired alongside
   * their base segment, and orphaned deltas are removed.
   */
  private void retireSupersededSegments() throws IOException {
    try (Table table = env.getConnection().getTable(indexTable)) {
      List<HnswSegment.Descriptor> all = HnswSegment.list(table, family);
      List<HnswSegment.Descriptor> toRetire = segmentsToRetire(all, startKey, endKey);
      if (!toRetire.isEmpty()) {
        List<Delete> deletes = new ArrayList<>(toRetire.size());
        for (HnswSegment.Descriptor d : toRetire) {
          deletes.add(new Delete(d.rowKey));
        }
        table.delete(deletes);
      }
    }
  }

  public static List<HnswSegment.Descriptor> segmentsToRetire(List<HnswSegment.Descriptor> all,
    byte[] startKey, byte[] endKey) {
    List<HnswSegment.Descriptor> bases = new ArrayList<>();
    List<HnswSegment.Descriptor> deltas = new ArrayList<>();
    Set<StackId> listedBases = new HashSet<>();

    for (HnswSegment.Descriptor d : all) {
      if (d.isDelta()) {
        deltas.add(d);
      } else {
        bases.add(d);
        listedBases.add(new StackId(d.startKey, d.time));
      }
    }

    Set<StackId> retiredBases = new HashSet<>();
    List<HnswSegment.Descriptor> toRetire = new ArrayList<>();

    for (HnswSegment.Descriptor b : bases) {
      if (b.overlaps(startKey, endKey)) {
        List<HnswSegment.Descriptor> newerBases = new ArrayList<>();
        for (HnswSegment.Descriptor n : bases) {
          if (n.time > b.time) {
            newerBases.add(n);
          }
        }
        if (b.coveredBy(newerBases)) {
          toRetire.add(b);
          retiredBases.add(new StackId(b.startKey, b.time));
        }
      }
    }

    for (HnswSegment.Descriptor d : deltas) {
      if (d.overlaps(startKey, endKey)) {
        StackId baseId = new StackId(d.startKey, d.baseTime);
        if (retiredBases.contains(baseId) || !listedBases.contains(baseId)) {
          toRetire.add(d);
        }
      }
    }

    return toRetire;
  }

  /**
   * Finds approximate nearest neighbors for the query vector within this region.
   * @param query    query vector values
   * @param topK     maximum number of neighbor row keys to return
   * @param efSearch size of the dynamic candidate list evaluated during search
   * @return list of matching data table row keys ordered by similarity
   * @throws IOException if search execution fails
   */
  public List<byte[]> search(float[] query, int topK, int efSearch) throws IOException {
    return search(query, topK, efSearch, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW,
      null);
  }

  /**
   * Executes approximate nearest neighbor search for the query vector constrained to primary keys
   * in {@code [startRow, stopRow)} matching {@code keyFilter}. Key predicates are evaluated
   * dynamically during graph traversal, retaining filtered out vertices as routing nodes to
   * preserve graph connectivity. If the total indexed row count within the key range does not
   * exceed {@code topK}, all qualifying keys are returned directly, bypassing graph traversal.
   * @param query     query vector values
   * @param topK      maximum candidate row keys to return
   * @param efSearch  size of the dynamic candidate list evaluated during traversal
   * @param startRow  inclusive lower bound of the scan key range, or empty for region start
   * @param stopRow   exclusive upper bound of the scan key range, or empty for region end
   * @param keyFilter optional row key predicate evaluated during traversal, or null
   * @return candidate primary keys matching the range and filter criteria
   * @throws IOException if graph index search fails
   */
  public List<byte[]> search(float[] query, int topK, int efSearch, byte[] startRow, byte[] stopRow,
    Predicate<byte[]> keyFilter) throws IOException {
    State s = state;
    byte[] from = Bytes.compareTo(startRow, startKey) > 0 ? startRow : startKey;
    byte[] to = minStop(stopRow, endKey);
    Predicate<byte[]> accept =
      key -> inRange(key, from, to) && (keyFilter == null || keyFilter.test(key));
    if (count(s, from, to) <= topK) {
      return all(s, from, to, accept, keyFilter);
    }
    VectorFloat<?> q = VTS.createFloatVector(query);
    // Search index sources in reverse chronological order: active mutable graph, in-flight flush
    // snapshot (omitting keys modified post-swap), and immutable disk segments (omitting masked
    // ordinals)
    Map<ImmutableBytesPtr, Float> scores = new HashMap<>();
    s.mutable.search(q, topK, efSearch, scores, null, accept);
    if (s.flushing != null) {
      s.flushing.search(q, topK, efSearch, scores, s.changedSinceSwap, accept);
    }
    for (Source source : s.sources) {
      HnswSegment segment = source.segment;
      int low = segment.ceiling(from);
      int high = to.length == 0 ? segment.size() : segment.ceiling(to);
      if (low >= high) {
        continue;
      }
      SearchResult result;
      try {
        result = segment.search(q, topK, efSearch,
          ordinal -> ordinal >= low && ordinal < high && !source.mask.contains(ordinal)
            && (keyFilter == null || keyFilter.test(segment.getKey(ordinal))));
      } catch (HnswSegment.NotFoundException e) {
        // An evicted segment reloads its payload; if its row was retired, search its replacements
        if (!replaceRetired(s.sources, source)) {
          throw e;
        }
        return search(query, topK, efSearch, startRow, stopRow, keyFilter);
      }
      for (SearchResult.NodeScore ns : result.getNodes()) {
        scores.putIfAbsent(new ImmutableBytesPtr(segment.getKey(ns.node)), ns.score);
      }
    }
    List<Map.Entry<ImmutableBytesPtr, Float>> ranked = new ArrayList<>(scores.entrySet());
    ranked.sort((a, b) -> Float.compare(b.getValue(), a.getValue()));
    List<byte[]> keys = new ArrayList<>(Math.min(topK, ranked.size()));
    for (int i = 0; i < ranked.size() && i < topK; i++) {
      keys.add(ranked.get(i).getKey().copyBytesIfNecessary());
    }
    return keys;
  }

  /**
   * Estimates an upper bound on indexed row keys spanning {@code [startRow, stopRow)} within this
   * region. Masked segment ordinals are included in segment range spans for efficiency, so the
   * returned estimate may exceed the true live key count.
   */
  public int count(byte[] startRow, byte[] stopRow) {
    return count(state, Bytes.compareTo(startRow, startKey) > 0 ? startRow : startKey,
      minStop(stopRow, endKey));
  }

  private static int count(State s, byte[] from, byte[] to) {
    long count = s.mutable.count(from, to);
    if (s.flushing != null) {
      count += s.flushing.count(from, to);
    }
    for (Source source : s.sources) {
      int high = to.length == 0 ? source.segment.size() : source.segment.ceiling(to);
      count += Math.max(0, high - source.segment.ceiling(from));
    }
    return (int) Math.min(Integer.MAX_VALUE, count);
  }

  // Collects all qualifying live keys across active memory and immutable segments in reverse
  // chronological order
  private static List<byte[]> all(State s, byte[] from, byte[] to, Predicate<byte[]> accept,
    Predicate<byte[]> keyFilter) {
    Set<ImmutableBytesPtr> keys = new LinkedHashSet<>();
    s.mutable.collect(keys, null, accept);
    if (s.flushing != null) {
      s.flushing.collect(keys, s.changedSinceSwap, accept);
    }
    for (Source source : s.sources) {
      HnswSegment segment = source.segment;
      int high = to.length == 0 ? segment.size() : segment.ceiling(to);
      for (int ordinal = segment.ceiling(from); ordinal < high; ordinal++) {
        byte[] key = segment.getKey(ordinal);
        if (!source.mask.contains(ordinal) && (keyFilter == null || keyFilter.test(key))) {
          keys.add(new ImmutableBytesPtr(key));
        }
      }
    }
    List<byte[]> list = new ArrayList<>(keys.size());
    for (ImmutableBytesPtr key : keys) {
      list.add(key.copyBytesIfNecessary());
    }
    return list;
  }

  // Resolves the tighter of two exclusive upper bounds, treating empty byte arrays as unbounded
  private static byte[] minStop(byte[] a, byte[] b) {
    if (a.length == 0) {
      return b;
    }
    return b.length == 0 || Bytes.compareTo(a, b) < 0 ? a : b;
  }

  private static boolean inRange(byte[] key, byte[] from, byte[] to) {
    return Bytes.compareTo(key, from) >= 0 && (to.length == 0 || Bytes.compareTo(key, to) < 0);
  }

  @Override
  public void close() {
    List<Source> sources;
    synchronized (this) {
      closed = true;
      sources = state.sources;
    }
    for (Source source : sources) {
      source.segment.close();
    }
  }

  /** In-memory HNSW graph supporting concurrent vector insertions and soft deletions. */
  final class MutableGraph {
    private final Map<ImmutableBytesPtr, float[]> live = new ConcurrentHashMap<>();
    private final Set<ImmutableBytesPtr> deleted = ConcurrentHashMap.newKeySet();
    private final Map<ImmutableBytesPtr, Integer> ordinals = new ConcurrentHashMap<>();
    private final Map<Integer, ImmutableBytesPtr> keys = new ConcurrentHashMap<>();
    private final Map<Integer, VectorFloat<?>> vectors = new ConcurrentHashMap<>();
    private volatile GraphIndexBuilder builder;
    private volatile int next;

    private final RandomAccessVectorValues ravv = new RandomAccessVectorValues() {
      @Override
      public int size() {
        return next;
      }

      @Override
      public int dimension() {
        return vectorIndex.getDimension();
      }

      @Override
      public VectorFloat<?> getVector(int ordinal) {
        return vectors.get(ordinal);
      }

      @Override
      public boolean isValueShared() {
        return false;
      }

      @Override
      public RandomAccessVectorValues copy() {
        return this;
      }
    };

    MutableGraph() {
      reset();
    }

    Map<ImmutableBytesPtr, float[]> live() {
      return live;
    }

    Set<ImmutableBytesPtr> deleted() {
      return deleted;
    }

    // Recreates the graph index builder to reset entry points and graph state
    private void reset() {
      vectors.clear();
      keys.clear();
      next = 0;
      if (vectorIndex != null && similarity != null) {
        builder = new GraphIndexBuilder(ravv, similarity, vectorIndex.getHnswM(),
          vectorIndex.getHnswEfConstruction(), NEIGHBOR_OVERFLOW,
          vectorIndex.getHnswAlpha().floatValue(), true, false);
      }
    }

    // Total count of buffered additions and deletions
    int changes() {
      return live.size() + deleted.size();
    }

    boolean isEmpty() {
      return live.isEmpty() && deleted.isEmpty();
    }

    void upsert(ImmutableBytesPtr key, float[] vector) {
      remove(key);
      deleted.remove(key);
      live.put(key, vector);
      int ordinal = next++;
      VectorFloat<?> v = VTS.createFloatVector(vector);
      vectors.put(ordinal, v);
      keys.put(ordinal, key);
      ordinals.put(key, ordinal);
      if (builder != null) {
        builder.addGraphNode(ordinal, v);
      }
    }

    void delete(ImmutableBytesPtr key) {
      remove(key);
      live.remove(key);
      deleted.add(key);
      if (ordinals.isEmpty()) {
        reset();
      }
    }

    private void remove(ImmutableBytesPtr key) {
      Integer old = ordinals.remove(key);
      if (old != null) {
        keys.remove(old);
        if (builder != null) {
          builder.markNodeDeleted(old);
        }
      }
    }

    // Searches the in-memory graph using JVector GraphSearcher, collecting candidate similarity
    // scores for passing keys
    void search(VectorFloat<?> query, int topK, int efSearch, Map<ImmutableBytesPtr, Float> scores,
      Set<ImmutableBytesPtr> excluded, Predicate<byte[]> accept) {
      if (ordinals.isEmpty() || builder == null) {
        return;
      }
      Bits bits = ordinal -> {
        ImmutableBytesPtr key = keys.get(ordinal);
        return key != null && (excluded == null || !excluded.contains(key))
          && accept.test(key.copyBytesIfNecessary());
      };
      SearchResult result = GraphSearcher.search(query, topK, Math.max(efSearch, topK), ravv,
        similarity, builder.getGraph(), bits);
      for (SearchResult.NodeScore ns : result.getNodes()) {
        ImmutableBytesPtr key = keys.get(ns.node);
        if (key != null) {
          scores.putIfAbsent(key, ns.score);
        }
      }
    }

    // Returns the count of live in-memory keys falling within [from, to)
    int count(byte[] from, byte[] to) {
      int count = 0;
      for (ImmutableBytesPtr key : live.keySet()) {
        if (inRange(key.copyBytesIfNecessary(), from, to)) {
          count++;
        }
      }
      return count;
    }

    // Collects non-excluded live keys matching the acceptance predicate into the destination set
    void collect(Set<ImmutableBytesPtr> into, Set<ImmutableBytesPtr> excluded,
      Predicate<byte[]> accept) {
      for (ImmutableBytesPtr key : live.keySet()) {
        if (
          (excluded == null || !excluded.contains(key)) && accept.test(key.copyBytesIfNecessary())
        ) {
          into.add(key);
        }
      }
    }

    // Overlays buffered additions and deletions onto the scanned vector dataset
    void applyTo(Map<ImmutableBytesPtr, float[]> vectors) {
      for (ImmutableBytesPtr key : deleted) {
        vectors.remove(key);
      }
      vectors.putAll(live);
    }
  }
}
