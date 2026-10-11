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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.metrics.ServerSideScanMetrics;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.DelegateRegionScanner;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.hbase.index.ValueGetter;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.hnsw.HnswSegment;
import org.apache.phoenix.hbase.index.hnsw.SegmentCells;
import org.apache.phoenix.hbase.index.metrics.HnswMetricValues;
import org.apache.phoenix.hbase.index.metrics.MetricsVectorIndexSource.HnswMetric;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.LiteralTTLExpression;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.apache.phoenix.util.ScanUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

/**
 * Unit tests for {@link HnswCandidateProbe}, which limits an HNSW search scan to candidate rows. A
 * mock region holds the rows in memory, and the in-memory graph of a real manager indexes them. The
 * mock region also holds an empty base segment that covers the region, unless a test removes it.
 */
public class HnswCandidateProbeTest {
  private static final int DIM = 8;
  private static final PTable.VectorIndex VI = new PTable.VectorIndex.Builder().setAlgorithm("HNSW")
    .setDistanceMetric("L2").setDimension(DIM).setHnswM(16).setHnswEfConstruction(100)
    .setHnswAlpha(1.2).setQuantizationType("NONE").build();
  private static final byte[] FAMILY = QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES;
  private static final byte[] V = Bytes.toBytes("V");
  private static final ColumnReference V_REF = new ColumnReference(FAMILY, V);
  private static final String INDEX = "I";
  private static final int CANDIDATES = 64;
  private static final int TARGET = 10;

  // The vector of each row of the graph
  private final Map<ImmutableBytesPtr, float[]> vectors = new HashMap<>();
  // The rows that a scan of the region returns. A graph row that the scan masks is not in this map
  private final NavigableMap<byte[], float[]> rows = new TreeMap<>(Bytes.BYTES_COMPARATOR);
  // The predicate for the rows that pass the client filter
  private volatile Predicate<byte[]> passes = key -> true;
  // The number of candidate keys of each probe scan
  private final List<Integer> probes = new CopyOnWriteArrayList<>();
  private final List<HnswIndexManager> managers = new ArrayList<>();
  // While set, this latch stops the full rebuild of a test. The teardown releases it
  private volatile CountDownLatch rebuildGate;
  private final Region region = mock(Region.class);
  private final RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);
  // The V# cells of the region
  private final SegmentCells cells = new SegmentCells();

  @Before
  public void setUp() throws IOException {
    RegionInfo info = RegionInfoBuilder.newBuilder(TableName.valueOf("T")).build();
    Configuration conf = HBaseConfiguration.create();
    when(region.getRegionInfo()).thenReturn(info);
    when(region.getTableDescriptor())
      .thenReturn(TableDescriptorBuilder.newBuilder(TableName.valueOf("T"))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build());
    cells.stub(region);
    // A scan of V# reads the segment cells. A probe scan returns the rows of its candidate ranges
    // that the region holds and that pass the client filter. Other scans, such as the scans of a
    // rebuild, find no rows
    when(region.getScanner(any(Scan.class))).thenAnswer(inv -> {
      Scan scan = inv.getArgument(0);
      if (SegmentCells.isSegmentScan(scan)) {
        return cells.scanner(scan, info);
      }
      MultiRowRangeFilter candidates = candidates(scan.getFilter());
      List<byte[]> found = new ArrayList<>();
      // The number of candidate rows that the client filter removes
      int[] filtered = new int[1];
      if (candidates != null) {
        probes.add(candidates.getRowRanges().size());
        for (RowRange range : candidates.getRowRanges()) {
          if (rows.containsKey(range.getStartRow())) {
            if (passes.test(range.getStartRow())) {
              found.add(range.getStartRow());
            } else {
              filtered[0]++;
            }
          }
        }
      }
      Iterator<byte[]> it = found.iterator();
      RegionScanner scanner = mock(RegionScanner.class);
      Answer<Boolean> next = inv2 -> {
        if (it.hasNext()) {
          byte[] key = it.next();
          List<Cell> cells = inv2.getArgument(0);
          cells.add(new KeyValue(key, FAMILY, V, encode(rows.get(key))));
        }
        return it.hasNext();
      };
      when(scanner.next(anyList())).thenAnswer(next);
      // As a region scanner does, the scanner counts each row that it reads in a context that
      // tracks metrics. It also counts each row that the filter removes
      when(scanner.next(anyList(), any(ScannerContext.class))).thenAnswer(inv2 -> {
        ScannerContext context = inv2.getArgument(1);
        if (context.isTrackingMetrics()) {
          ServerSideScanMetrics metrics = context.getMetrics();
          metrics.addToCounter(ServerSideScanMetrics.COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME,
            filtered[0] + (it.hasNext() ? 1 : 0));
          metrics.addToCounter(ServerSideScanMetrics.COUNT_OF_ROWS_FILTERED_KEY_METRIC_NAME,
            filtered[0]);
          filtered[0] = 0;
        }
        return next.answer(inv2);
      });
      return scanner;
    });
    when(region.get(any(Get.class))).thenAnswer(inv -> {
      Get get = inv.getArgument(0);
      return SegmentCells.isSegmentGet(get) ? cells.get(get) : Result.create(new ArrayList<>());
    });
    when(env.getRegion()).thenReturn(region);
    when(env.getRegionInfo()).thenReturn(info);
    when(env.getConfiguration()).thenReturn(conf);
    // An empty base segment of the region covers the region, so that searches restrict the scans
    cells.put(
      new HnswSegment.Descriptor(Bytes.toBytes(INDEX), HConstants.EMPTY_START_ROW, 1,
        HConstants.EMPTY_END_ROW, 0, null, 1,
        HnswSegment.definitionDigest(HnswIndexManager.definition(index()))),
      null, info.getEncodedNameAsBytes());
  }

  @After
  public void tearDown() {
    CountDownLatch gate = rebuildGate;
    if (gate != null) {
      gate.countDown();
    }
    for (HnswIndexManager manager : managers) {
      manager.close();
    }
  }

  // Returns the candidate ranges of a probe scan, or null for a scan that is not a probe
  private static MultiRowRangeFilter candidates(Filter filter) {
    if (filter instanceof MultiRowRangeFilter) {
      return (MultiRowRangeFilter) filter;
    }
    if (filter instanceof FilterList) {
      for (Filter f : ((FilterList) filter).getFilters()) {
        if (f instanceof MultiRowRangeFilter) {
          return (MultiRowRangeFilter) f;
        }
      }
    }
    return null;
  }

  private static byte[] encode(float[] v) {
    byte[] bytes = new byte[v.length * Bytes.SIZEOF_FLOAT];
    for (int i = 0; i < v.length; i++) {
      Bytes.putFloat(bytes, i * Bytes.SIZEOF_FLOAT, v[i]);
    }
    return bytes;
  }

  private static float[] decode(ImmutableBytesWritable ptr) {
    float[] v = new float[ptr.getLength() / Bytes.SIZEOF_FLOAT];
    for (int i = 0; i < v.length; i++) {
      v[i] = Bytes.toFloat(ptr.get(), ptr.getOffset() + i * Bytes.SIZEOF_FLOAT);
    }
    return v;
  }

  private static float[] vector(Random random) {
    float[] v = new float[DIM];
    for (int d = 0; d < DIM; d++) {
      v[d] = (float) random.nextGaussian();
    }
    return v;
  }

  // Returns the HNSW index I on the vector column V
  private static PTable index() {
    PColumn column = mock(PColumn.class);
    when(column.getFamilyName()).thenReturn(PNameFactory.newName("0"));
    doReturn(PVectorFloat.INSTANCE).when(column).getDataType();
    when(column.getExpressionStr()).thenReturn("\"0\".\"V\"");
    PTable index = mock(PTable.class);
    when(index.getName()).thenReturn(PNameFactory.newName(INDEX));
    when(index.getVectorIndex()).thenReturn(VI);
    when(index.getColumns()).thenReturn(Collections.singletonList(column));
    when(index.getColumnFamilies()).thenReturn(Collections.emptyList());
    return index;
  }

  private HnswIndexManager open(HnswIndexManager.Catalog catalog, IndexMaintainer maintainer)
    throws IOException {
    HnswIndexManager manager =
      new HnswIndexManager(env, index(), maintainer, false, false, catalog, () -> Long.MAX_VALUE);
    managers.add(manager);
    return manager;
  }

  private HnswIndexManager open() throws IOException {
    return open(scan -> true, maintainer());
  }

  private static IndexMaintainer maintainer() {
    IndexMaintainer maintainer = mock(IndexMaintainer.class);
    when(maintainer.getVectorAsFloats(any(ValueGetter.class), anyLong(), anyBoolean()))
      .thenAnswer(inv -> {
        ImmutableBytesWritable ptr =
          ((ValueGetter) inv.getArgument(0)).getLatestValue(V_REF, HConstants.LATEST_TIMESTAMP);
        return ptr == null ? null : decode(ptr);
      });
    when(maintainer.getAllColumnsForDataTable()).thenReturn(Collections.singleton(V_REF));
    return maintainer;
  }

  // Adds the given number of rows to the graph and to the region
  private void load(HnswIndexManager manager, int count) {
    load(manager, 0, count);
  }

  // Adds the given number of rows to the graph and to the region, from the given row number
  private void load(HnswIndexManager manager, int first, int count) {
    Random random = new Random(7 + first);
    for (int i = first; i < first + count; i++) {
      byte[] key = Bytes.toBytes(String.format("r%06d", i));
      float[] v = vector(random);
      vectors.put(new ImmutableBytesPtr(key), v);
      rows.put(key, v);
      manager.onMutation(null, new Put(key).addColumn(FAMILY, V, encode(v)), v);
    }
  }

  // Returns the rows of the graph in order of distance to the query, nearest first
  private List<byte[]> ranked(float[] query) {
    return vectors.entrySet().stream()
      .sorted(Comparator.comparingDouble(e -> distance(query, e.getValue())))
      .map(e -> e.getKey().copyBytesIfNecessary()).collect(Collectors.toList());
  }

  private static double distance(float[] a, float[] b) {
    double sum = 0;
    for (int i = 0; i < a.length; i++) {
      sum += (a[i] - b[i]) * (double) (a[i] - b[i]);
    }
    return sum;
  }

  private static Set<ImmutableBytesPtr> keys(Filter filter) {
    Set<ImmutableBytesPtr> keys = new HashSet<>();
    for (RowRange range : ((MultiRowRangeFilter) filter).getRowRanges()) {
      keys.add(new ImmutableBytesPtr(range.getStartRow()));
    }
    return keys;
  }

  private static Set<ImmutableBytesPtr> keys(List<byte[]> list) {
    Set<ImmutableBytesPtr> keys = new HashSet<>();
    for (byte[] key : list) {
      keys.add(new ImmutableBytesPtr(key));
    }
    return keys;
  }

  private static byte[] search(int candidates, int target, float[] query) {
    byte[] search = new byte[Bytes.SIZEOF_INT * (2 + query.length)];
    Bytes.putInt(search, 0, candidates);
    Bytes.putInt(search, Bytes.SIZEOF_INT, target);
    for (int i = 0; i < query.length; i++) {
      Bytes.putFloat(search, Bytes.SIZEOF_INT * (2 + i), query[i]);
    }
    return search;
  }

  // Returns a search attribute with a distance bound after the query vector
  private static byte[] search(int candidates, int target, float[] query, float radius) {
    byte[] search = Bytes.add(search(candidates, target, query), new byte[Bytes.SIZEOF_FLOAT]);
    Bytes.putFloat(search, Bytes.SIZEOF_INT * (2 + query.length), radius);
    return search;
  }

  /**
   * The client filter holds a distance bound at the fifth nearest row, so only four rows pass. If
   * the search knows the bound, its farthest candidate is beyond the bound, so it stops after one
   * probe and returns the four rows. Without the bound, the search widens until it reads the range
   * without restriction.
   */
  @Test
  public void testDistanceBoundStopsWidening() throws Exception {
    HnswIndexManager manager = open();
    load(manager, 3000);
    float[] query = vector(new Random(41));
    List<byte[]> ranked = ranked(query);
    double bound = distance(query, vectors.get(new ImmutableBytesPtr(ranked.get(4))));
    passes = key -> distance(query, vectors.get(new ImmutableBytesPtr(key))) < bound;
    Scan scan = new Scan().setFilter(new FirstKeyOnlyFilter());
    assertNull(HnswCandidateProbe.candidateFilter(env, manager, scan, query, CANDIDATES, TARGET,
      Double.NaN));
    assertTrue("probes " + probes, probes.size() > 1);
    probes.clear();
    Filter filter = HnswCandidateProbe.candidateFilter(env, manager, scan, query, CANDIDATES,
      TARGET, Math.sqrt(bound));
    assertEquals(keys(ranked.subList(0, 4)), keys(filter));
    assertEquals(Collections.singletonList(CANDIDATES), probes);
  }

  /**
   * The search stops only if its farthest candidate is beyond the bound by more than RADIUS_MARGIN.
   * Inside the margin, the search asks for more candidates, and the next farthest candidate is
   * beyond the margin. A bound beyond the farthest candidate does not stop a search that needs more
   * rows.
   */
  @Test
  public void testDistanceBoundMargin() throws Exception {
    HnswIndexManager manager = open();
    load(manager, 3000);
    float[] query = vector(new Random(43));
    double farthest = manager.candidates(query, CANDIDATES, HConstants.EMPTY_START_ROW,
      HConstants.EMPTY_END_ROW, null).farthest;
    assertTrue(farthest > 0);
    // The target is more than the candidate count, so only the bound can stop the first probe
    int target = 2 * CANDIDATES;
    Scan scan = new Scan().setFilter(new FirstKeyOnlyFilter());
    double outside = farthest / (1 + 2 * HnswCandidateProbe.RADIUS_MARGIN);
    double inside = farthest / (1 + HnswCandidateProbe.RADIUS_MARGIN / 2);
    for (double bound : new double[] { outside, inside, 2 * farthest }) {
      probes.clear();
      passes = key -> Math.sqrt(distance(query, vectors.get(new ImmutableBytesPtr(key)))) < bound;
      HnswCandidateProbe.candidateFilter(env, manager, scan, query, CANDIDATES, target, bound);
      assertEquals("probes " + probes + " for bound " + bound, bound == outside ? 1 : 2,
        probes.size());
      assertEquals(CANDIDATES, (int) probes.get(0));
    }
  }

  /**
   * The distance of a JVector similarity score has the units of the Phoenix distance function of
   * the metric: Euclidean distance for L2, and cosine distance for COSINE.
   */
  @Test
  public void testScoreDistance() {
    VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();
    Random random = new Random(47);
    for (int i = 0; i < 100; i++) {
      float[] a = vector(random);
      float[] b = vector(random);
      double dot = 0;
      double na = 0;
      double nb = 0;
      for (int d = 0; d < DIM; d++) {
        dot += a[d] * (double) b[d];
        na += a[d] * (double) a[d];
        nb += b[d] * (double) b[d];
      }
      float euclidean = VectorSimilarityFunction.EUCLIDEAN.compare(vts.createFloatVector(a),
        vts.createFloatVector(b));
      assertEquals(Math.sqrt(distance(a, b)),
        HnswSegment.distance(VectorSimilarityFunction.EUCLIDEAN, euclidean),
        1e-3 * Math.sqrt(distance(a, b)));
      float cosine =
        VectorSimilarityFunction.COSINE.compare(vts.createFloatVector(a), vts.createFloatVector(b));
      assertEquals(1 - dot / Math.sqrt(na * nb),
        HnswSegment.distance(VectorSimilarityFunction.COSINE, cosine), 1e-5);
    }
    assertEquals(0.0, HnswSegment.distance(VectorSimilarityFunction.EUCLIDEAN, 1f), 0.0);
  }

  /**
   * The lower estimate of a distance is not more than the exact distance, also for distances that
   * the float score cannot resolve. For a normal distance, the estimate is near the exact distance.
   */
  @Test
  public void testNearestDistanceIsLowerEstimate() {
    VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();
    Random random = new Random(53);
    for (int i = 0; i < 100; i++) {
      float[] a = vector(random);
      for (double step : new double[] { 1e-5, 1e-4, 3e-4, 1e-3, 1e-2, 0.5 }) {
        float[] b = a.clone();
        b[i % DIM] += (float) step;
        b[(i + 1) % DIM] -= (float) step;
        double dot = 0;
        double na = 0;
        double nb = 0;
        for (int d = 0; d < DIM; d++) {
          dot += a[d] * (double) b[d];
          na += a[d] * (double) a[d];
          nb += b[d] * (double) b[d];
        }
        double euclidean = Math.sqrt(distance(a, b));
        double cosine = 1 - dot / Math.sqrt(na * nb);
        double l2Estimate = HnswSegment.nearestDistance(VectorSimilarityFunction.EUCLIDEAN,
          VectorSimilarityFunction.EUCLIDEAN.compare(vts.createFloatVector(a),
            vts.createFloatVector(b)));
        double cosineEstimate = HnswSegment.nearestDistance(VectorSimilarityFunction.COSINE,
          VectorSimilarityFunction.COSINE.compare(vts.createFloatVector(a),
            vts.createFloatVector(b)));
        assertTrue(l2Estimate + " > " + euclidean, l2Estimate <= euclidean);
        assertTrue(cosineEstimate + " > " + cosine, cosineEstimate <= Math.max(0, cosine));
        if (step == 0.5) {
          assertEquals(euclidean, l2Estimate, 1e-3 * euclidean);
          assertEquals(cosine, cosineEstimate, 1e-3 * cosine + 1e-4);
        }
      }
    }
  }

  /**
   * On a table with a TTL, the graph keeps expired rows until a rebuild, and the scan masks them. A
   * search without a filter probes its candidates, so expired rows near the query do not take the
   * places of live rows. The range is large enough that a first probe with no live row widens and
   * does not read the full range.
   */
  @Test
  public void testUnfilteredSearchSkipsExpiredCandidates() throws Exception {
    HnswIndexManager manager = open();
    load(manager, 8000);
    float[] query = vector(new Random(11));
    List<byte[]> ranked = ranked(query);
    for (byte[] key : ranked.subList(0, 80)) {
      rows.remove(key);
    }
    // The client sets the TTL of the table on the scan
    Scan scan = new Scan();
    scan.setAttribute(BaseScannerRegionObserverConstants.TTL,
      new LiteralTTLExpression(86400).serialize());
    Filter filter =
      HnswCandidateProbe.candidateFilter(env, manager, scan, query, CANDIDATES, TARGET, Double.NaN);
    Set<ImmutableBytesPtr> live = new HashSet<>();
    for (ImmutableBytesPtr key : keys(filter)) {
      if (rows.containsKey(key.copyBytesIfNecessary())) {
        live.add(key);
      }
    }
    assertTrue("live candidates " + live.size(), live.size() >= TARGET);
    // The nearest live rows are among them
    assertTrue(live.containsAll(keys(ranked.subList(80, 80 + TARGET))));

    // Without a TTL, every candidate is a row of the scan, so the search does not probe
    probes.clear();
    assertEquals(CANDIDATES, keys(HnswCandidateProbe.candidateFilter(env, manager, new Scan(),
      query, CANDIDATES, TARGET, Double.NaN)).size());
    assertTrue(probes.isEmpty());
    // If the client does not set the TTL, for example for a view, the scan still masks rows with
    // the TTL of the table descriptor
    when(region.getTableDescriptor())
      .thenReturn(TableDescriptorBuilder.newBuilder(TableName.valueOf("T")).setColumnFamily(
        ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).setTimeToLive(86400).build()).build());
    filter = HnswCandidateProbe.candidateFilter(env, manager, new Scan(), query, CANDIDATES, TARGET,
      Double.NaN);
    assertTrue(probes.size() > 0);
    assertTrue(keys(filter).containsAll(keys(ranked.subList(80, 80 + TARGET))));
    // A client with Phoenix TTL off sets TTL_EXPRESSION_DEFINED_IN_TABLE_DESCRIPTOR on each scan.
    // Without a family TTL, no row expires, so the search does not probe
    when(region.getTableDescriptor())
      .thenReturn(TableDescriptorBuilder.newBuilder(TableName.valueOf("T"))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build());
    probes.clear();
    scan = new Scan();
    scan.setAttribute(BaseScannerRegionObserverConstants.TTL,
      LiteralTTLExpression.TTL_EXPRESSION_DEFINED_IN_TABLE_DESCRIPTOR.serialize());
    assertEquals(CANDIDATES, keys(
      HnswCandidateProbe.candidateFilter(env, manager, scan, query, CANDIDATES, TARGET, Double.NaN))
        .size());
    assertTrue(probes.isEmpty());
    // The hidden family V# has no TTL. It is the first family when the data families sort after
    // it, and the TTL of the table is on the data families. Thus the search probes
    when(region.getTableDescriptor())
      .thenReturn(TableDescriptorBuilder.newBuilder(TableName.valueOf("T"))
        .setColumnFamily(
          ColumnFamilyDescriptorBuilder.of(QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY_BYTES))
        .setColumnFamily(
          ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("X")).setTimeToLive(86400).build())
        .build());
    filter =
      HnswCandidateProbe.candidateFilter(env, manager, scan, query, CANDIDATES, TARGET, Double.NaN);
    assertTrue(probes.size() > 0);
    assertTrue(keys(filter).containsAll(keys(ranked.subList(80, 80 + TARGET))));
  }

  /**
   * A region whose bases do not cover its range reads its range without candidate restriction, with
   * or without a filter. An example is the upper daughter of a split. The graph does not hold every
   * row of the range, so the search does not probe. The first base restricts the scans again.
   */
  @Test
  public void testUncoveredRegionReadsRange() throws Exception {
    cells.clear();
    HnswIndexManager manager = open();
    load(manager, 100);
    assertFalse(manager.covered());
    float[] query = vector(new Random(13));
    Scan filtered = new Scan().setFilter(new FirstKeyOnlyFilter());
    assertNull(HnswCandidateProbe.candidateFilter(env, manager, new Scan(), query, CANDIDATES,
      TARGET, Double.NaN));
    assertNull(HnswCandidateProbe.candidateFilter(env, manager, filtered, query, CANDIDATES, TARGET,
      Double.NaN));
    assertTrue(probes.isEmpty());
    manager.rebuild(Long.MAX_VALUE);
    for (int i = 0; i < 3000 && !manager.covered(); i++) {
      Thread.sleep(10);
    }
    assertTrue(manager.covered());
    assertNotNull(HnswCandidateProbe.candidateFilter(env, manager, new Scan(), query, CANDIDATES,
      TARGET, Double.NaN));
    assertNotNull(HnswCandidateProbe.candidateFilter(env, manager, filtered, query, CANDIDATES,
      TARGET, Double.NaN));
  }

  /**
   * If the candidates cover all indexed rows of the scan range, the scan reads only those rows,
   * with or without a filter. Thus the scan does not read rows that have no vector. A search with a
   * filter probes each indexed row one time. It returns the rows that pass, also when fewer rows
   * than the target pass.
   */
  @Test
  public void testCandidatesCoveringRangeRestrictToIndexedRows() throws Exception {
    HnswIndexManager manager = open();
    load(manager, CANDIDATES);
    float[] query = vector(new Random(13));
    assertEquals(keys(new ArrayList<>(rows.keySet())), keys(HnswCandidateProbe.candidateFilter(env,
      manager, new Scan(), query, CANDIDATES, TARGET, Double.NaN)));
    assertTrue(probes.isEmpty());
    Set<ImmutableBytesPtr> passing = keys(ranked(query).subList(20, 20 + TARGET / 2));
    passes = key -> passing.contains(new ImmutableBytesPtr(key));
    assertEquals(passing, keys(HnswCandidateProbe.candidateFilter(env, manager,
      new Scan().setFilter(new FirstKeyOnlyFilter()), query, CANDIDATES, TARGET, Double.NaN)));
    assertEquals(Collections.singletonList(CANDIDATES), probes);
  }

  /**
   * If no candidate passes a probe, the search multiplies its candidate count by GROWTH. This range
   * is large, so a probe with one passing candidate keeps the restriction. Thus the search also
   * keeps it and does not read every row. In this test, the second probe finds rows that pass.
   */
  @Test
  public void testEmptyProbeWidensGeometrically() throws Exception {
    HnswIndexManager manager = open();
    load(manager, 8000);
    float[] query = vector(new Random(17));
    Set<ImmutableBytesPtr> passing = keys(ranked(query).subList(100, 200));
    passes = key -> passing.contains(new ImmutableBytesPtr(key));
    Filter filter = HnswCandidateProbe.candidateFilter(env, manager,
      new Scan().setFilter(new FirstKeyOnlyFilter()), query, CANDIDATES, TARGET, Double.NaN);
    assertTrue(filter instanceof MultiRowRangeFilter);
    Set<ImmutableBytesPtr> found = keys(filter);
    assertTrue("found " + found.size(), found.size() >= TARGET);
    assertTrue(passing.containsAll(found));
    // The second search asks for GROWTH times the candidates, and probes only the new candidates
    assertEquals(2, probes.size());
    assertEquals(CANDIDATES, (int) probes.get(0));
    assertTrue("second probe " + probes.get(1),
      probes.get(1) >= CANDIDATES * (HnswCandidateProbe.GROWTH - 1)
        && probes.get(1) <= CANDIDATES * HnswCandidateProbe.GROWTH);
  }

  /**
   * In this test, no candidate passes the first probe. One passing candidate would make the search
   * read the range without restriction. Zero passing candidates show a pass rate that is not higher
   * than the rate of one passing candidate. Thus the search reads the range without restriction and
   * does not probe again.
   */
  @Test
  public void testEmptyProbeReadsRangeAsOnePassWould() throws Exception {
    HnswIndexManager manager = open();
    load(manager, 3000);
    float[] query = vector(new Random(17));
    Set<ImmutableBytesPtr> passing = keys(ranked(query).subList(100, 200));
    passes = key -> passing.contains(new ImmutableBytesPtr(key));
    assertNull(HnswCandidateProbe.candidateFilter(env, manager,
      new Scan().setFilter(new FirstKeyOnlyFilter()), query, CANDIDATES, TARGET, Double.NaN));
    assertEquals(Collections.singletonList(CANDIDATES), probes);
  }

  /**
   * The probe takes the limit of its widening from the state that each search reads. In this test,
   * a swap and new rows replace the state during the first search, so the search runs again on the
   * new state. The first state holds fewer rows than the candidate count. A limit from that state
   * would stop the probe after one search, with no row that passes. Instead, the probe must ask for
   * more candidates. A cutover that gives coverage to an uncovered region during a probe is the
   * same case, because the uncovered state holds only the rows of its in-memory graphs.
   */
  @Test
  public void testWideningBoundComesFromSearchedState() throws Exception {
    // The full rebuild swaps the graphs. Then its catalog check waits on the gate
    CountDownLatch swapped = new CountDownLatch(1);
    CountDownLatch gate = new CountDownLatch(1);
    rebuildGate = gate;
    HnswIndexManager manager = open(scan -> {
      swapped.countDown();
      try {
        gate.await(60, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return false;
    }, maintainer());
    load(manager, 50);
    assertEquals(50, manager.count(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW));
    float[] query = vector(new Random(37));
    Set<ImmutableBytesPtr> passing = new HashSet<>();
    // The first key test of the first search starts a full rebuild, waits for its swap, and adds
    // 8000 rows to the new mutable graph. Only rows far from the query pass the filter
    KeyFilterHook.hook = () -> {
      manager.rebuild(Long.MAX_VALUE);
      try {
        assertTrue("swap", swapped.await(60, TimeUnit.SECONDS));
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      load(manager, 50, 8000);
      passing.addAll(keys(ranked(query).subList(150, 250)));
    };
    passes = key -> passing.contains(new ImmutableBytesPtr(key));
    try {
      Filter filter = HnswCandidateProbe.candidateFilter(env, manager,
        new Scan().setFilter(new KeyFilterHook()), query, CANDIDATES, TARGET, Double.NaN);
      assertNull("hook did not run", KeyFilterHook.hook);
      assertTrue(String.valueOf(filter), filter instanceof MultiRowRangeFilter);
      Set<ImmutableBytesPtr> found = keys(filter);
      assertTrue("found " + found.size(), found.size() >= TARGET);
      assertTrue(passing.containsAll(found));
      assertEquals(2, probes.size());
    } finally {
      KeyFilterHook.hook = null;
    }
  }

  /**
   * A SkipScanFilter that matches every row key. Its first key test runs {@link #hook}, so that a
   * test can change the manager during the first graph search. The probe copies the client filter
   * through protobuf, which calls {@link #parseFrom(byte[])}.
   */
  public static final class KeyFilterHook extends SkipScanFilter {
    static volatile Runnable hook;

    @Override
    public boolean hasIntersect(byte[] lowerInclusiveKey, byte[] upperExclusiveKey) {
      Runnable run = hook;
      if (run != null) {
        hook = null;
        run.run();
      }
      return true;
    }

    @Override
    public byte[] toByteArray() {
      return new byte[0];
    }

    public static KeyFilterHook parseFrom(byte[] bytes) {
      return new KeyFilterHook();
    }
  }

  /**
   * If the pass rate shows that the search needs a large share of the indexed rows, the search
   * immediately reads the range without restriction. It does not seek to that many candidates
   * first.
   */
  @Test
  public void testLowPassRateReadsRange() throws Exception {
    HnswIndexManager manager = open();
    load(manager, 3000);
    float[] query = vector(new Random(19));
    byte[] only = ranked(query).get(5);
    passes = key -> Bytes.equals(key, only);
    assertNull(HnswCandidateProbe.candidateFilter(env, manager,
      new Scan().setFilter(new FirstKeyOnlyFilter()), query, CANDIDATES, TARGET, Double.NaN));
    assertEquals(Collections.singletonList(CANDIDATES), probes);
  }

  // The search metrics that testSearchMetrics checks, in this order
  private static final HnswMetric[] SEARCH_METRICS =
    { HnswMetric.SEARCHES, HnswMetric.SEARCH_TIME, HnswMetric.UNRESTRICTED_UNCOVERED,
      HnswMetric.UNRESTRICTED_LIMIT, HnswMetric.PROBE_ROUNDS, HnswMetric.PROBE_ROWS_READ };

  private static long[] searchMetrics() {
    long[] values = new long[SEARCH_METRICS.length];
    for (int i = 0; i < values.length; i++) {
      values[i] = HnswMetricValues.value(SEARCH_METRICS[i], INDEX);
    }
    return values;
  }

  // Runs a search, and checks the changes of the search metrics of the index
  private static void assertSearchMetrics(Runnable search, long... changes) {
    long[] before = searchMetrics();
    search.run();
    long[] after = searchMetrics();
    for (int i = 0; i < changes.length; i++) {
      assertEquals(SEARCH_METRICS[i].metricName(), changes[i], after[i] - before[i]);
    }
  }

  private void assertFilter(HnswIndexManager manager, Scan scan, float[] query, int candidates,
    boolean restricted) {
    try {
      Filter filter = HnswCandidateProbe.candidateFilter(env, manager, scan, query, candidates,
        TARGET, Double.NaN);
      assertEquals(restricted, filter != null);
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  /**
   * Each HNSW scan counts one search and its time. A scan without candidate restriction counts
   * exactly one reason: the region is uncovered, or a candidate limit applies. A restricted scan
   * counts no reason. Each probe round counts the candidate rows that it reads.
   */
  @Test
  public void testSearchMetrics() throws Exception {
    HnswIndexManager manager = open();
    load(manager, 3000);
    float[] query = vector(new Random(29));
    Scan filtered = new Scan().setFilter(new FirstKeyOnlyFilter());
    long visited = HnswMetricValues.value(HnswMetric.NODES_VISITED, INDEX);
    assertSearchMetrics(() -> assertFilter(manager, new Scan(), query, CANDIDATES, true), 1, 1, 0,
      0, 0, 0);
    assertTrue(HnswMetricValues.value(HnswMetric.NODES_VISITED, INDEX) > visited);
    assertSearchMetrics(() -> assertFilter(manager, filtered, query, CANDIDATES, true), 1, 1, 0, 0,
      1, CANDIDATES);
    // The client asks for more candidates than the maximum
    assertSearchMetrics(
      () -> assertFilter(manager, new Scan(), query, HnswCandidateProbe.MAX_CANDIDATES + 1, false),
      1, 1, 0, 1, 0, 0);
    // Few candidates pass, so the widening needs too many candidates
    byte[] only = ranked(query).get(5);
    passes = key -> Bytes.equals(key, only);
    assertSearchMetrics(() -> assertFilter(manager, filtered, query, CANDIDATES, false), 1, 1, 0, 1,
      1, CANDIDATES);
    passes = key -> true;

    // A region without coverage
    manager.close();
    cells.clear();
    HnswIndexManager uncovered = open();
    load(uncovered, 100);
    assertFalse(uncovered.covered());
    assertSearchMetrics(() -> assertFilter(uncovered, filtered, query, CANDIDATES, false), 1, 1, 1,
      0, 0, 0);
    assertSearchMetrics(() -> assertFilter(uncovered, new Scan(), query, CANDIDATES, false), 1, 1,
      1, 0, 0, 0);
  }

  /**
   * The client sets the candidate count. A count over the maximum reads the range without
   * restriction. The search does not keep a candidate range for each of that many rows.
   */
  @Test
  public void testCandidateCountIsCapped() throws Exception {
    // The flush threshold queues a full rebuild, which swaps out the graph. The catalog check of
    // the rebuild waits on the gate, so the search still reads the swapped graph
    CountDownLatch gate = new CountDownLatch(1);
    rebuildGate = gate;
    HnswIndexManager manager = open(scan -> {
      try {
        gate.await(60, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return false;
    }, maintainer());
    int count = HnswCandidateProbe.MAX_CANDIDATES + 10;
    load(manager, count);
    assertEquals(count, manager.count(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW));
    float[] query = vector(new Random(23));
    assertNull(HnswCandidateProbe.candidateFilter(env, manager, new Scan(), query,
      HnswCandidateProbe.MAX_CANDIDATES + 1, TARGET, Double.NaN));
    assertEquals(HnswCandidateProbe.MAX_CANDIDATES, keys(HnswCandidateProbe.candidateFilter(env,
      manager, new Scan(), query, HnswCandidateProbe.MAX_CANDIDATES, TARGET, Double.NaN)).size());
  }

  /**
   * A search that finds no candidate ends the scan with a filter. TTLRegionScanner does not take
   * this filter out of the scan as a row limit.
   */
  @Test
  public void testNoCandidatesFilterSurvivesTtlMasking() throws Exception {
    Filter none = HnswCandidateProbe.rows(Collections.emptyList());
    assertTrue(none.filterAllRemaining());
    Scan scan = new Scan().setFilter(none);
    assertNull(ScanUtil.removePageFilter(scan));
    assertSame(none, scan.getFilter());
    // The observer adds the candidate filter to the client filter
    scan = new Scan().setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL,
      new FirstKeyOnlyFilter(), HnswCandidateProbe.rows(Collections.emptyList())));
    assertNull(ScanUtil.removePageFilter(scan));
    assertTrue(scan.getFilter().filterAllRemaining());
  }

  /**
   * A missing or malformed search attribute fails the scan with a DoNotRetryIOException. A negative
   * distance bound is malformed.
   */
  @Test
  public void testMalformedSearchAttribute() throws Exception {
    HnswIndexManager manager = open();
    load(manager, 100);
    float[] query = vector(new Random(29));
    for (byte[] search : new byte[][] { null, new byte[3], search(CANDIDATES, TARGET, new float[1]),
      search(0, TARGET, query), search(CANDIDATES, -1, query),
      search(CANDIDATES, TARGET, query, -1f),
      Bytes.add(search(CANDIDATES, TARGET, query), new byte[1]) }) {
      Scan scan = new Scan();
      scan.setAttribute(BaseScannerRegionObserverConstants.HNSW_SEARCH_QUERY, search);
      try {
        HnswCandidateProbe.candidateFilter(env, manager, scan);
        fail("expected a malformed search to fail");
      } catch (DoNotRetryIOException expected) {
      }
    }
    Scan scan = new Scan();
    scan.setAttribute(BaseScannerRegionObserverConstants.HNSW_SEARCH_QUERY,
      search(CANDIDATES, TARGET, query));
    assertEquals(CANDIDATES, keys(HnswCandidateProbe.candidateFilter(env, manager, scan)).size());
    // A search without a filter returns its candidates, so a distance bound does not change them
    scan.setAttribute(BaseScannerRegionObserverConstants.HNSW_SEARCH_QUERY,
      search(CANDIDATES, TARGET, query, 0.5f));
    assertEquals(CANDIDATES, keys(HnswCandidateProbe.candidateFilter(env, manager, scan)).size());
  }

  /**
   * An unchecked exception from the HNSW search of a scan, for example from the client filter,
   * fails the scan with an IOException. If the unchecked exception left the hook, HBase would abort
   * the region server.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testScanHookFailsWithIOException() throws Exception {
    HnswIndexManager manager = open();
    load(manager, 1000);
    IndexRegionObserver observer = new IndexRegionObserver();
    Field field = IndexRegionObserver.class.getDeclaredField("vectorIndexManagers");
    field.setAccessible(true);
    ((Map<String, VectorIndexManager>) field.get(observer)).put(INDEX, manager);
    ObserverContext<RegionCoprocessorEnvironment> c = mock(ObserverContext.class);
    when(c.getEnvironment()).thenReturn(env);

    Scan scan = new Scan();
    scan.setAttribute(BaseScannerRegionObserverConstants.HNSW_SEARCH_INDEX, Bytes.toBytes(INDEX));
    try {
      observer.preScannerOpen(c, scan);
      fail("expected a search without parameters to fail");
    } catch (DoNotRetryIOException expected) {
    }

    scan = new Scan().setFilter(new FirstKeyOnlyFilter());
    scan.setAttribute(BaseScannerRegionObserverConstants.HNSW_SEARCH_INDEX, Bytes.toBytes(INDEX));
    scan.setAttribute(BaseScannerRegionObserverConstants.HNSW_SEARCH_QUERY,
      search(CANDIDATES, TARGET, vector(new Random(31))));
    passes = key -> {
      throw new ArithmeticException("/ by zero");
    };
    try {
      observer.preScannerOpen(c, scan);
      fail("expected the probe to fail");
    } catch (IOException e) {
      assertTrue(e.toString(), e.getCause() instanceof ArithmeticException);
    }
  }

  /**
   * The scan metrics of the client include the rows that the probe of a search reads. The probe
   * runs before HBase makes the scanner context of the RPC. Thus the scan carries the counts to the
   * scanner. The scanner adds them once, to the first context that tracks metrics. A client value
   * of the carrier attribute does not reach the metrics. A search that does not probe, or a client
   * that does not track scan metrics, gets the scanner without a wrapper.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testProbeRowsReachScanMetrics() throws Exception {
    HnswIndexManager manager = open();
    load(manager, 3000);
    IndexRegionObserver observer = new IndexRegionObserver();
    Field field = IndexRegionObserver.class.getDeclaredField("vectorIndexManagers");
    field.setAccessible(true);
    ((Map<String, VectorIndexManager>) field.get(observer)).put(INDEX, manager);
    ObserverContext<RegionCoprocessorEnvironment> c = mock(ObserverContext.class);
    when(c.getEnvironment()).thenReturn(env);
    float[] query = vector(new Random(37));
    // About half of the candidates pass the client filter
    passes = key -> key[key.length - 1] % 2 == 0;
    byte[] forged = Bytes.add(Bytes.toBytes(1_000_000L), Bytes.toBytes(1_000_000L));

    Scan scan = new Scan().setFilter(new FirstKeyOnlyFilter());
    scan.setAttribute(BaseScannerRegionObserverConstants.HNSW_SEARCH_INDEX, Bytes.toBytes(INDEX));
    scan.setAttribute(BaseScannerRegionObserverConstants.HNSW_SEARCH_QUERY,
      search(CANDIDATES, TARGET, query));
    scan.setAttribute(HnswProbeScanMetrics.ATTRIBUTE, forged);
    scan.setScanMetricsEnabled(true);
    long read = HnswMetricValues.value(HnswMetric.PROBE_ROWS_READ, INDEX);
    observer.preScannerOpen(c, scan);
    read = HnswMetricValues.value(HnswMetric.PROBE_ROWS_READ, INDEX) - read;
    assertTrue(read >= CANDIDATES);
    byte[] counts = scan.getAttribute(HnswProbeScanMetrics.ATTRIBUTE);
    assertNotNull(counts);
    // Each candidate is a row of the region, so the probe scans each candidate that it reads
    long scanned = Bytes.toLong(counts, 0);
    long filtered = Bytes.toLong(counts, Bytes.SIZEOF_LONG);
    assertEquals(read, scanned);
    assertTrue(filtered > 0 && filtered < scanned);

    // The wrapper is a Phoenix scanner, so a later observer adds no paging and TTL scanners
    RegionScanner s = mock(RegionScanner.class);
    RegionScanner wrapped = HnswProbeScanMetrics.wrap(env, scan, s);
    assertNotSame(s, wrapped);
    assertTrue(wrapped instanceof DelegateRegionScanner);
    assertNull(scan.getAttribute(HnswProbeScanMetrics.ATTRIBUTE));
    // A context that does not track metrics gets nothing. The first context that tracks metrics
    // gets the counts, and a later context gets nothing
    ScannerContext untracked = ScannerContext.newBuilder().build();
    wrapped.nextRaw(new ArrayList<>(), untracked);
    for (int rpc = 0; rpc < 2; rpc++) {
      ScannerContext context = ScannerContext.newBuilder().setTrackMetrics(true).build();
      wrapped.nextRaw(new ArrayList<>(), context);
      wrapped.next(new ArrayList<>(), context);
      assertEquals(rpc == 0 ? scanned : 0, context.getMetrics().countOfRowsScanned.get());
      assertEquals(rpc == 0 ? filtered : 0, context.getMetrics().countOfRowsFiltered.get());
    }

    // A client that does not track scan metrics gets no counts, and the forged value is removed
    scan = new Scan().setFilter(new FirstKeyOnlyFilter());
    scan.setAttribute(BaseScannerRegionObserverConstants.HNSW_SEARCH_INDEX, Bytes.toBytes(INDEX));
    scan.setAttribute(BaseScannerRegionObserverConstants.HNSW_SEARCH_QUERY,
      search(CANDIDATES, TARGET, query));
    scan.setAttribute(HnswProbeScanMetrics.ATTRIBUTE, forged);
    observer.preScannerOpen(c, scan);
    assertNull(scan.getAttribute(HnswProbeScanMetrics.ATTRIBUTE));
    assertSame(s, HnswProbeScanMetrics.wrap(env, scan, s));

    // No search removes the value of a scan that is not a search, so the wrapper ignores it
    scan = new Scan();
    scan.setAttribute(HnswProbeScanMetrics.ATTRIBUTE, forged);
    scan.setScanMetricsEnabled(true);
    assertSame(s, HnswProbeScanMetrics.wrap(env, scan, s));
    assertNull(scan.getAttribute(HnswProbeScanMetrics.ATTRIBUTE));

    // A search without a filter on a table without a TTL does not probe
    scan = new Scan();
    scan.setAttribute(BaseScannerRegionObserverConstants.HNSW_SEARCH_INDEX, Bytes.toBytes(INDEX));
    scan.setAttribute(BaseScannerRegionObserverConstants.HNSW_SEARCH_QUERY,
      search(CANDIDATES, TARGET, query));
    scan.setScanMetricsEnabled(true);
    observer.preScannerOpen(c, scan);
    assertNull(scan.getAttribute(HnswProbeScanMetrics.ATTRIBUTE));
    assertSame(s, HnswProbeScanMetrics.wrap(env, scan, s));
  }

  /**
   * A view inherits each index of its table as {@code <view>#<index>}. Writes and scans through the
   * view use the manager of the index, so the region keeps only one graph for the index.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testViewIndexNameUsesManagerOfIndex() throws Exception {
    HnswIndexManager manager = open();
    IndexRegionObserver observer = new IndexRegionObserver();
    Field field = IndexRegionObserver.class.getDeclaredField("vectorIndexManagers");
    field.setAccessible(true);
    ((Map<String, VectorIndexManager>) field.get(observer)).put(INDEX, manager);
    assertSame(manager, observer.getVectorIndexManager(INDEX));
    assertSame(manager,
      observer.getVectorIndexManager("V" + QueryConstants.CHILD_VIEW_INDEX_NAME_SEPARATOR + INDEX));
  }

  /**
   * A row change that fails to apply to the graph does not fail the batch. The batch applies the
   * change while it holds its row locks. The region rebuilds its graph from its rows instead. If a
   * change fails while a full rebuild runs, another rebuild follows, because the running rebuild
   * can have read the row before the change.
   */
  @Test
  public void testFailedGraphUpdateRebuilds() throws Exception {
    IndexMaintainer maintainer = maintainer();
    byte[] bad = Bytes.toBytes("bad");
    when(maintainer.getVectorAsFloats(any(ValueGetter.class), anyLong(), anyBoolean()))
      .thenAnswer(inv -> {
        ValueGetter getter = inv.getArgument(0);
        if (Bytes.equals(bad, getter.getRowKey())) {
          throw new IllegalStateException("cannot evaluate");
        }
        ImmutableBytesWritable ptr = getter.getLatestValue(V_REF, HConstants.LATEST_TIMESTAMP);
        return ptr == null ? null : decode(ptr);
      });
    // The first rebuild waits for the gate and then fails. The second rebuild finds that the index
    // is stale, so the manager closes
    CountDownLatch gate = new CountDownLatch(1);
    rebuildGate = gate;
    CountDownLatch first = new CountDownLatch(1);
    CountDownLatch second = new CountDownLatch(1);
    AtomicInteger rebuilds = new AtomicInteger();
    HnswIndexManager manager = open(scan -> {
      if (rebuilds.incrementAndGet() > 1) {
        second.countDown();
        return false;
      }
      first.countDown();
      try {
        gate.await(60, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      throw new IOException("rebuild fails");
    }, maintainer);
    load(manager, 10);
    Put change = new Put(bad).addColumn(FAMILY, V, encode(new float[DIM]));
    manager.onMutation(change, change, new float[DIM]);
    assertTrue("rebuild requested", first.await(30, TimeUnit.SECONDS));
    manager.onMutation(change, change, new float[DIM]);
    gate.countDown();
    // A failed rebuild retries after RETRY_DELAY_MS. The pending rebuild that follows it starts
    // immediately
    assertTrue("rebuild after the running rebuild", second.await(30, TimeUnit.SECONDS));
  }
}
