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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.hbase.index.ValueGetter;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.hnsw.HnswSegment;
import org.apache.phoenix.hbase.index.hnsw.SegmentCells;
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

/**
 * Unit tests for {@link HnswCandidateProbe}, which limits an HNSW search scan to candidate rows.
 * The mutable graph of a real manager holds the rows. A mock region supplies the table descriptor.
 * It also supplies an empty base segment that covers the region, unless a test removes it.
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
    when(env.getRegion()).thenReturn(region);
    when(env.getRegionInfo()).thenReturn(info);
    when(env.getConfiguration()).thenReturn(conf);
    cells.stubOnly(region);
    // An empty base segment of the region covers the region, so that searches restrict the scans
    cells.put(
      new HnswSegment.Descriptor(Bytes.toBytes(INDEX), HConstants.EMPTY_START_ROW, 1,
        HConstants.EMPTY_END_ROW, 0, 1,
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

  // Adds the given number of rows to the graph
  private void load(HnswIndexManager manager, int count) {
    Random random = new Random(7);
    for (int i = 0; i < count; i++) {
      byte[] key = Bytes.toBytes(String.format("r%06d", i));
      float[] v = vector(random);
      manager.onMutation(null, new Put(key).addColumn(FAMILY, V, encode(v)), v);
    }
  }

  private static Set<ImmutableBytesPtr> keys(Filter filter) {
    Set<ImmutableBytesPtr> keys = new HashSet<>();
    for (RowRange range : ((MultiRowRangeFilter) filter).getRowRanges()) {
      keys.add(new ImmutableBytesPtr(range.getStartRow()));
    }
    return keys;
  }

  private static byte[] search(int candidates, float[] query) {
    byte[] search = new byte[Bytes.SIZEOF_INT * (1 + query.length)];
    Bytes.putInt(search, 0, candidates);
    for (int i = 0; i < query.length; i++) {
      Bytes.putFloat(search, Bytes.SIZEOF_INT * (1 + i), query[i]);
    }
    return search;
  }

  /**
   * On a table with a TTL, the graph holds expired rows until a rebuild, and the scan masks them.
   * Thus the scan reads its range without restriction. Otherwise, expired rows near the query can
   * take the places of live rows among the candidates. The TTL comes from the scan, or else from
   * the table descriptor.
   */
  @Test
  public void testTtlScanReadsRange() throws Exception {
    HnswIndexManager manager = open();
    load(manager, 3000);
    float[] query = vector(new Random(11));
    // The client sets the TTL of the table on the scan
    Scan scan = new Scan();
    scan.setAttribute(BaseScannerRegionObserverConstants.TTL,
      new LiteralTTLExpression(86400).serialize());
    assertNull(HnswCandidateProbe.candidateFilter(env, manager, scan, query, CANDIDATES));

    // Without a TTL, every candidate is a row of the scan
    assertEquals(CANDIDATES,
      keys(HnswCandidateProbe.candidateFilter(env, manager, new Scan(), query, CANDIDATES)).size());
    // If the client does not set the TTL, for example for a view, the scan still masks rows with
    // the TTL of the table descriptor
    when(region.getTableDescriptor())
      .thenReturn(TableDescriptorBuilder.newBuilder(TableName.valueOf("T")).setColumnFamily(
        ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).setTimeToLive(86400).build()).build());
    assertNull(HnswCandidateProbe.candidateFilter(env, manager, new Scan(), query, CANDIDATES));
    // A client with Phoenix TTL off sets TTL_EXPRESSION_DEFINED_IN_TABLE_DESCRIPTOR on each scan.
    // Without a family TTL, no row expires, so the scan reads only the candidates
    when(region.getTableDescriptor())
      .thenReturn(TableDescriptorBuilder.newBuilder(TableName.valueOf("T"))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build());
    scan = new Scan();
    scan.setAttribute(BaseScannerRegionObserverConstants.TTL,
      LiteralTTLExpression.TTL_EXPRESSION_DEFINED_IN_TABLE_DESCRIPTOR.serialize());
    assertEquals(CANDIDATES,
      keys(HnswCandidateProbe.candidateFilter(env, manager, scan, query, CANDIDATES)).size());
    // The hidden family V# has no TTL. It is the first family when the data families sort after
    // it, and the TTL of the table is on the data families
    when(region.getTableDescriptor())
      .thenReturn(TableDescriptorBuilder.newBuilder(TableName.valueOf("T"))
        .setColumnFamily(
          ColumnFamilyDescriptorBuilder.of(QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY_BYTES))
        .setColumnFamily(
          ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("X")).setTimeToLive(86400).build())
        .build());
    assertNull(HnswCandidateProbe.candidateFilter(env, manager, scan, query, CANDIDATES));
  }

  /**
   * A region whose bases do not cover its range, for example the upper daughter of a split, reads
   * its range without candidate restriction. The graph does not hold every row of the range. The
   * first base restricts the scans again.
   */
  @Test
  public void testUncoveredRegionReadsRange() throws Exception {
    cells.clear();
    HnswIndexManager manager = open();
    load(manager, 100);
    assertFalse(manager.covered());
    float[] query = vector(new Random(13));
    assertNull(HnswCandidateProbe.candidateFilter(env, manager, new Scan(), query, CANDIDATES));
    manager.rebuild(Long.MAX_VALUE);
    for (int i = 0; i < 3000 && !manager.covered(); i++) {
      Thread.sleep(10);
    }
    assertTrue(manager.covered());
    assertNotNull(HnswCandidateProbe.candidateFilter(env, manager, new Scan(), query, CANDIDATES));
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
    load(manager, HnswCandidateProbe.MAX_CANDIDATES + 10);
    float[] query = vector(new Random(23));
    assertNull(HnswCandidateProbe.candidateFilter(env, manager, new Scan(), query,
      HnswCandidateProbe.MAX_CANDIDATES + 1));
    assertEquals(HnswCandidateProbe.MAX_CANDIDATES, keys(HnswCandidateProbe.candidateFilter(env,
      manager, new Scan(), query, HnswCandidateProbe.MAX_CANDIDATES)).size());
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

  /** A missing or malformed search attribute fails the scan with a DoNotRetryIOException. */
  @Test
  public void testMalformedSearchAttribute() throws Exception {
    HnswIndexManager manager = open();
    load(manager, 100);
    float[] query = vector(new Random(29));
    for (byte[] search : new byte[][] { null, new byte[3], search(CANDIDATES, new float[1]),
      search(0, query) }) {
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
      search(CANDIDATES, query));
    assertEquals(CANDIDATES, keys(HnswCandidateProbe.candidateFilter(env, manager, scan)).size());
  }

  /**
   * An unchecked exception from the HNSW search of a scan, for example from the table descriptor,
   * fails the scan. HBase would abort the region server if the exception left the hook.
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

    scan = new Scan();
    scan.setAttribute(BaseScannerRegionObserverConstants.HNSW_SEARCH_INDEX, Bytes.toBytes(INDEX));
    scan.setAttribute(BaseScannerRegionObserverConstants.HNSW_SEARCH_QUERY,
      search(CANDIDATES, vector(new Random(31))));
    when(region.getTableDescriptor()).thenThrow(new IllegalStateException("no descriptor"));
    try {
      observer.preScannerOpen(c, scan);
      fail("expected the search to fail");
    } catch (IOException e) {
      assertTrue(e.toString(), e.getCause() instanceof IllegalStateException);
    }
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
