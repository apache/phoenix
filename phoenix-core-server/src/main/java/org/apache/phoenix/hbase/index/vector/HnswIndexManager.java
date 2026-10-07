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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
  /** Maximum number of buffered mutations before triggering a segment flush. */
  public static final int FLUSH_THRESHOLD = 10_000;
  /** Delay before retrying a failed segment rebuild. */
  public static final long RETRY_DELAY_MS = 60_000;
  /** Lookback window applied during mutation replay to account for concurrent writes. */
  static final long REPLAY_MARGIN_MS = 60_000;
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

  /** Readable segment source with its mutation mask and boundary scope. */
  private static final class Source {
    final HnswSegment segment;
    // Segment row ordinals invalidated by updates or deletions after segment construction
    final Set<Integer> mask = ConcurrentHashMap.newKeySet();
    // Indicates whether the segment covers rows outside this region (e.g., split parent or merge
    // input)
    final boolean wider;

    Source(HnswSegment segment, boolean wider) {
      this.segment = segment;
      this.wider = wider;
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
    long oldest = Long.MAX_VALUE;
    for (HnswSegment.Descriptor d : current) {
      oldest = Math.min(oldest, d.time);
    }
    this.state = new State(sources, new MutableGraph(), null, null, 0);
    if (!current.isEmpty()) {
      replay(oldest - REPLAY_MARGIN_MS);
    }
    LOG.info("Opened HNSW index {} for region {} with {} segments", indexName,
      region.getRegionInfo().getEncodedName(), sources.size());
    // Rebuild immediately if the region does not have an exact covering segment (e.g., after
    // splits, merges, or initial index building), while serving queries from overlapping segments
    boolean exact = current.size() == 1 && current.get(0).covers(startKey, endKey);
    if (!exact && (active || !current.isEmpty())) {
      flush(true);
    }
  }

  /**
   * Discovers active segments covering this region by selecting overlapping segment entries whose
   * span within the region has not been superseded by newer segments.
   * @param all candidate segment descriptors
   * @return list of active segments for this region
   */
  private List<HnswSegment.Descriptor> currentSegments(List<HnswSegment.Descriptor> all) {
    List<HnswSegment.Descriptor> overlapping = new ArrayList<>();
    for (HnswSegment.Descriptor d : all) {
      if (d.overlaps(startKey, endKey)) {
        overlapping.add(d);
      }
    }
    List<HnswSegment.Descriptor> current = new ArrayList<>();
    for (HnswSegment.Descriptor d : overlapping) {
      List<HnswSegment.Descriptor> newer = new ArrayList<>();
      for (HnswSegment.Descriptor n : overlapping) {
        if (n.time > d.time) {
          newer.add(n);
        }
      }
      byte[] from = Bytes.compareTo(d.startKey, startKey) > 0 ? d.startKey : startKey;
      byte[] to =
        endKey.length == 0 || (d.endKey.length > 0 && Bytes.compareTo(d.endKey, endKey) < 0)
          ? d.endKey
          : endKey;
      if (!HnswSegment.covered(from, to, newer)) {
        current.add(d);
      }
    }
    return current;
  }

  private List<HnswSegment.Descriptor> listCurrentSegments() throws IOException {
    try (Table table = env.getConnection().getTable(indexTable)) {
      return currentSegments(HnswSegment.list(table, family));
    }
  }

  /**
   * Returns sources for the given segments in order, reusing those already open and opening the
   * rest. Returns null if a segment row was retired after it was listed, so the caller lists again.
   */
  private List<Source> openSources(List<HnswSegment.Descriptor> current,
    Map<ImmutableBytesPtr, Source> open) throws IOException {
    List<Source> sources = new ArrayList<>(current.size());
    List<Source> opened = new ArrayList<>();
    try {
      for (HnswSegment.Descriptor d : current) {
        Source source = open.get(new ImmutableBytesPtr(d.rowKey));
        if (source == null && d.count > 0) {
          source = new Source(HnswSegment.open(env.getConnection(), indexTable, family, d.rowKey,
            allocator, vectorIndex.getDistanceMetric()), !d.covers(startKey, endKey));
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
      open.put(new ImmutableBytesPtr(source.segment.getRowKey()), source);
    }
    List<Source> sources;
    do {
      List<HnswSegment.Descriptor> current = listCurrentSegments();
      for (HnswSegment.Descriptor d : current) {
        if (Bytes.equals(d.rowKey, retired.segment.getRowKey())) {
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
        Set<ImmutableBytesPtr> buffered = new HashSet<>(s.mutable.live.keySet());
        buffered.addAll(s.mutable.deleted);
        if (s.flushing != null) {
          buffered.addAll(s.flushing.live.keySet());
          buffered.addAll(s.flushing.deleted);
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
        for (Source source : searched) {
          if (!sources.contains(source)) {
            close.add(source);
          }
        }
        state = new State(sources, s.mutable, s.flushing, s.changedSinceSwap, s.swapTime);
        LOG.info("Replaced retired HNSW segment {} of index {} for region {} with {} segments",
          Bytes.toStringBinary(retired.segment.getRowKey()), indexName,
          region.getRegionInfo().getEncodedName(), sources.size());
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

  /** Triggers an immediate segment rebuild. */
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
      state = new State(s.sources, new MutableGraph(), s.mutable, ConcurrentHashMap.newKeySet(),
        EnvironmentEdgeManager.currentTimeMillis());
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
    try {
      State s;
      synchronized (this) {
        if (closed) {
          return;
        }
        s = state;
      }
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
      if (cutover(segment)) {
        retireSupersededSegments();
      }
      LOG.info("Rebuilt HNSW index {} for region {} with {} vectors", indexName,
        region.getRegionInfo().getEncodedName(), keys.length);
    } catch (Throwable t) {
      LOG.error("HNSW index {} rebuild failed for region {}; will retry", indexName,
        region.getRegionInfo().getEncodedName(), t);
      RETRIES.schedule(() -> flush(true), RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
    } finally {
      synchronized (this) {
        rebuilding = false;
      }
    }
  }

  // Scans the region to retrieve current vector values for all data table rows
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

  // Atomically installs the newly built segment and releases completed flush state
  private synchronized boolean cutover(HnswSegment segment) {
    State s = state;
    if (closed) {
      if (segment != null) {
        segment.close();
      }
      return false;
    }
    List<Source> sources = new ArrayList<>(1);
    if (segment != null) {
      Source source = new Source(segment, false);
      for (ImmutableBytesPtr key : s.changedSinceSwap) {
        int ordinal = segment.ordinalOf(key);
        if (ordinal >= 0) {
          source.mask.add(ordinal);
        }
      }
      sources.add(source);
    }
    state = new State(sources, s.mutable, null, null, 0);
    for (Source old : s.sources) {
      old.segment.close();
    }
    return true;
  }

  /**
   * Purges segment records intersecting this region whose entire key range is fully covered by
   * newer segments. Predecessor segments (such as split parents) are retained until all daughter
   * regions complete their respective rebuilds.
   */
  private void retireSupersededSegments() throws IOException {
    try (Table table = env.getConnection().getTable(indexTable)) {
      List<HnswSegment.Descriptor> all = HnswSegment.list(table, family);
      for (HnswSegment.Descriptor d : all) {
        List<HnswSegment.Descriptor> newer = new ArrayList<>();
        for (HnswSegment.Descriptor n : all) {
          if (n.time > d.time) {
            newer.add(n);
          }
        }
        if (d.overlaps(startKey, endKey) && d.coveredBy(newer)) {
          table.delete(new Delete(d.rowKey));
        }
      }
    }
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
    State s = state;
    VectorFloat<?> q = VTS.createFloatVector(query);
    // Evaluate sources in reverse chronological order: active mutable graph, in-flight flush
    // graph (excluding keys modified since swap), and segment sources (filtering masked ordinals
    // and keys outside the region boundary)
    Map<ImmutableBytesPtr, Float> scores = new HashMap<>();
    s.mutable.search(q, topK, efSearch, scores, null);
    if (s.flushing != null) {
      s.flushing.search(q, topK, efSearch, scores, s.changedSinceSwap);
    }
    for (Source source : s.sources) {
      HnswSegment segment = source.segment;
      SearchResult result;
      try {
        result = segment.search(q, topK, efSearch, ordinal -> !source.mask.contains(ordinal)
          && (!source.wider || region.getRegionInfo().containsRow(segment.getKey(ordinal))));
      } catch (HnswSegment.NotFoundException e) {
        // An evicted segment reloads its payload; if its row was retired, search its replacements
        if (!replaceRetired(s.sources, source)) {
          throw e;
        }
        return search(query, topK, efSearch);
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
  private final class MutableGraph {
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

    // Reinitializes the graph builder to reset graph structure and entry points
    private void reset() {
      vectors.clear();
      keys.clear();
      next = 0;
      builder = new GraphIndexBuilder(ravv, similarity, vectorIndex.getHnswM(),
        vectorIndex.getHnswEfConstruction(), NEIGHBOR_OVERFLOW,
        vectorIndex.getHnswAlpha().floatValue(), true, false);
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
      builder.addGraphNode(ordinal, v);
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
        builder.markNodeDeleted(old);
      }
    }

    // Searches the in-memory graph and records scores for eligible row keys
    void search(VectorFloat<?> query, int topK, int efSearch, Map<ImmutableBytesPtr, Float> scores,
      Set<ImmutableBytesPtr> excluded) {
      if (ordinals.isEmpty()) {
        return;
      }
      SearchResult result = GraphSearcher.search(query, topK, Math.max(efSearch, topK), ravv,
        similarity, builder.getGraph(), Bits.ALL);
      for (SearchResult.NodeScore ns : result.getNodes()) {
        ImmutableBytesPtr key = keys.get(ns.node);
        if (key != null && (excluded == null || !excluded.contains(key))) {
          scores.putIfAbsent(key, ns.score);
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
