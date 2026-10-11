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
package org.apache.phoenix.mapreduce.index.fsck.hnsw;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.jbellis.jvector.disk.ByteBufferReader;
import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.hbase.index.hnsw.HnswOffheapAllocator;
import org.apache.phoenix.hbase.index.hnsw.HnswSegment;
import org.apache.phoenix.hbase.index.hnsw.HnswSegmentStore;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository;
import org.apache.phoenix.mapreduce.index.fsck.Finding;
import org.apache.phoenix.mapreduce.index.fsck.IndexFsckContext;
import org.apache.phoenix.mapreduce.index.fsck.Report;
import org.apache.phoenix.mapreduce.index.fsck.Severity;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.junit.Test;

/**
 * Unit tests for the helpers of the HNSW verify, fsck, repair and inspect tools, with mocked tables
 * and segments that the test builds in memory.
 */
public class HnswIndexFsckProviderTest {
  private static final VectorTypeSupport VTS =
    VectorizationProvider.getInstance().getVectorTypeSupport();
  private static final int DIM = 8;
  private static final int COUNT = 200;

  private static OnDiskGraphIndex load(byte[] payload) {
    int graphLength = payload.length - 8 - Bytes.toInt(payload, payload.length - 8);
    ByteBuffer graph = ByteBuffer.wrap(payload, 0, graphLength).slice();
    return OnDiskGraphIndex.load(() -> new ByteBufferReader(graph.duplicate()));
  }

  private static int indexOf(byte[] haystack, byte[] needle) {
    for (int i = 0; i + needle.length <= haystack.length; i++) {
      if (Bytes.equals(haystack, i, needle.length, needle, 0, needle.length)) {
        return i;
      }
    }
    return -1;
  }

  /**
   * The reachability check finds the nodes that the edges of the graph connect to the entry node.
   * When the test removes the level-0 edges, only the nodes of the upper levels can stay reachable.
   */
  @Test
  public void testReachable() throws Exception {
    Random random = new Random(3);
    List<VectorFloat<?>> vectors = new ArrayList<>();
    byte[][] keys = new byte[COUNT][];
    for (int i = 0; i < COUNT; i++) {
      float[] v = new float[DIM];
      for (int d = 0; d < DIM; d++) {
        v[d] = (float) random.nextGaussian();
      }
      vectors.add(VTS.createFloatVector(v));
      keys[i] = Bytes.toBytes(String.format("k%04d", i));
    }
    PTable.VectorIndex vi = new PTable.VectorIndex.Builder().setAlgorithm("HNSW")
      .setDistanceMetric("L2").setDimension(DIM).setHnswM(8).setHnswEfConstruction(64)
      .setHnswAlpha(1.2).setQuantizationType("NONE").build();
    byte[] payload = HnswSegment.build(vi, new ListRandomAccessVectorValues(vectors, DIM), keys);
    OnDiskGraphIndex graph = load(payload);
    // A parallel graph build can leave a small number of nodes without inbound edges
    int built = HnswIndexFsckProvider.reachable(graph).cardinality();
    assertTrue(built + " of " + COUNT, built >= COUNT * 0.95);

    // A level-0 node record is [ordinal][vector][degree][neighbors]. Set each degree to 0
    byte[] first = Bytes.toBytes(0);
    for (int d = 0; d < DIM; d++) {
      first = Bytes.add(first, Bytes.toBytes(vectors.get(0).get(d)));
    }
    int base = indexOf(payload, first);
    assertTrue(base >= 0);
    int record = Bytes.SIZEOF_INT * (2 + DIM + graph.getDegree(0));
    for (int node = 0; node < COUNT; node++) {
      int offset = base + node * record;
      assertEquals(node, Bytes.toInt(payload, offset));
      Bytes.putInt(payload, offset + Bytes.SIZEOF_INT * (1 + DIM), 0);
    }
    int upper = 0;
    for (int level = 1; level <= graph.getMaxLevel(); level++) {
      upper = Math.max(upper, graph.size(level));
    }
    int reachable = HnswIndexFsckProvider.reachable(load(payload)).cardinality();
    assertTrue(reachable + " of " + upper, reachable <= upper && reachable < built);
  }

  @Test
  public void testParseVector() {
    assertArrayEquals(new float[] { 1f, -2.5f, 3f },
      HnswIndexInspector.parseVector(" [1, -2.5,3] "), 0f);
    assertArrayEquals(new float[] { 1f, 2f }, HnswIndexInspector.parseVector("[1 2]"), 0f);
    try {
      HnswIndexInspector.parseVector("1,2");
      fail();
    } catch (IllegalArgumentException expected) {
    }
  }

  @Test
  public void testArgs() {
    HnswIndexInspector.Args args =
      new HnswIndexInspector.Args(Arrays.asList("a", "k=5", "nodes", "b=1", "time=12"));
    assertEquals(Arrays.asList("a", "b=1"), args.positional);
    assertEquals(5, args.getInt("k", 10));
    assertEquals(10, args.getInt("ef", 10));
    assertEquals(Long.valueOf(12), args.getLong("time"));
    assertTrue(args.has("nodes"));
  }

  private static final byte[] KEY = Bytes.toBytes("IDX");
  private static final byte[] DEFINITION = HnswSegment.definitionDigest("definition");
  private static final HnswSegment.Descriptor SEGMENT = new HnswSegment.Descriptor(KEY,
    Bytes.toBytes("m"), 100L, HConstants.EMPTY_END_ROW, 10, null, 100L, DEFINITION);

  private static HnswSegment.NotFoundException notFound(HnswSegment.Descriptor d) throws Exception {
    Constructor<HnswSegment.NotFoundException> notFound =
      HnswSegment.NotFoundException.class.getDeclaredConstructor(byte[].class, Object.class);
    notFound.setAccessible(true);
    return notFound.newInstance(d.identity, "table T");
  }

  private static List<Finding> checkSegment(HnswIndexReader reader) throws Exception {
    List<Finding> findings = new ArrayList<>();
    new HnswIndexFsckProvider().checkSegment(null, reader, SEGMENT, findings);
    return findings;
  }

  /** A payload that is too large for the heap of the tool gets a warning, not a corrupt finding. */
  @Test
  public void testOversizedSegmentIsNotCorrupt() throws Exception {
    HnswIndexReader reader = mock(HnswIndexReader.class);
    when(reader.open(SEGMENT))
      .thenThrow(new HnswIndexReader.PayloadTooLargeException("payload too large"));
    when(reader.readPayload(SEGMENT)).thenReturn(new byte[100]);
    List<Finding> findings = checkSegment(reader);
    assertEquals(findings.toString(), 1, findings.size());
    assertEquals(HnswIndexFsckProvider.SEGMENT_UNCHECKED, findings.get(0).getRule());
    assertEquals(Severity.WARN, findings.get(0).getSeverity());
  }

  /**
   * A read failure does not show that a payload is corrupt. Thus the check fails, and it records no
   * finding.
   */
  @Test
  public void testUnreadableSegmentIsNotCorrupt() throws Exception {
    HnswIndexReader reader = mock(HnswIndexReader.class);
    when(reader.open(SEGMENT)).thenThrow(new IOException("read timed out"));
    when(reader.readPayload(SEGMENT)).thenThrow(new DoNotRetryIOException("read failed"));
    List<Finding> findings = new ArrayList<>();
    try {
      new HnswIndexFsckProvider().checkSegment(null, reader, SEGMENT, findings);
      fail();
    } catch (DoNotRetryIOException expected) {
      assertTrue(findings.toString(), findings.isEmpty());
    }
  }

  /** A payload that the tool can read but cannot decode is corrupt. */
  @Test
  public void testUndecodableSegmentIsCorrupt() throws Exception {
    HnswIndexReader reader = mock(HnswIndexReader.class);
    when(reader.open(SEGMENT)).thenThrow(new RuntimeException("bad graph"));
    when(reader.readPayload(SEGMENT)).thenReturn(new byte[100]);
    List<Finding> findings = checkSegment(reader);
    assertEquals(findings.toString(), 1, findings.size());
    assertEquals(HnswIndexFsckProvider.SEGMENT_CORRUPT, findings.get(0).getRule());
  }

  /** A payload that refers to a MOB file that does not exist is unresolved. */
  @Test
  public void testUnresolvedPayload() throws Exception {
    HnswIndexReader reader = mock(HnswIndexReader.class);
    when(reader.open(SEGMENT)).thenThrow(new IOException("no MOB file"));
    when(reader.readPayload(SEGMENT)).thenReturn(new byte[0]);
    List<Finding> findings = checkSegment(reader);
    assertEquals(findings.toString(), 1, findings.size());
    assertEquals(HnswIndexFsckProvider.PAYLOAD_UNRESOLVED, findings.get(0).getRule());
  }

  /** A segment that the server retires during the check gets no finding. */
  @Test
  public void testRetiredSegmentIsSkipped() throws Exception {
    HnswIndexReader reader = mock(HnswIndexReader.class);
    when(reader.open(SEGMENT)).thenThrow(new IOException("retired")).thenThrow(notFound(SEGMENT));
    when(reader.readPayload(SEGMENT)).thenReturn(null);
    assertTrue(checkSegment(reader).isEmpty());
  }

  /**
   * If a new listing still returns a segment without its payload cell, no server retired the
   * segment. Its payload is missing, so the region leaves it out and rebuilds.
   */
  @Test
  public void testMissingPayloadCellIsUnresolved() throws Exception {
    HnswIndexReader reader = mock(HnswIndexReader.class);
    when(reader.open(SEGMENT)).thenThrow(notFound(SEGMENT));
    HnswSegment.Descriptor listed = new HnswSegment.Descriptor(KEY, Bytes.toBytes("m"), 100L,
      HConstants.EMPTY_END_ROW, 10, null, 100L, DEFINITION);
    when(reader.listSegments()).thenReturn(Collections.singletonList(listed));
    List<Finding> findings = checkSegment(reader);
    assertEquals(findings.toString(), 1, findings.size());
    assertEquals(HnswIndexFsckProvider.PAYLOAD_UNRESOLVED, findings.get(0).getRule());
    assertEquals(Bytes.toStringBinary(SEGMENT.identity),
      findings.get(0).getDetails().get("segment"));
  }

  /**
   * The check opens a segment without a payload again after a failed open. A delta that holds only
   * tombstones is such a segment. A temporary failure of the first open is not corruption.
   */
  @Test
  public void testSegmentWithoutPayloadIsOpenedAgain() throws Exception {
    HnswSegment.Descriptor delta = new HnswSegment.Descriptor(KEY, Bytes.toBytes("m"), 200L,
      HConstants.EMPTY_END_ROW, 0, 100L, 200L, DEFINITION);
    // The delta records no deleted rows, so it has no tombstones cell
    HnswSegmentStore store = mock(HnswSegmentStore.class);
    when(store.readTombstones(delta)).thenReturn(null);
    HnswSegment segment = HnswSegment.open(store, delta,
      HnswOffheapAllocator.getForClient(new Configuration()), "COSINE");

    HnswIndexReader reader = mock(HnswIndexReader.class);
    when(reader.open(delta)).thenThrow(new IOException("read timed out")).thenReturn(segment);
    when(reader.readPayload(delta)).thenReturn(null);
    // The server did not retire the segment, so a listing still holds it
    when(reader.listSegments()).thenReturn(Collections.singletonList(delta));
    List<Finding> findings = new ArrayList<>();
    new HnswIndexFsckProvider().checkSegment(null, reader, delta, findings);
    assertTrue(findings.toString(), findings.isEmpty());
  }

  /**
   * The checks classify the cells of {@code V#} and name the regions that repair acts on. Each
   * region uses only the segments of the index definition that it holds. The lower region has a
   * trusted stack and a superseded base, and holds malformed cells. The middle region searches a
   * wider stack that another region wrote, as the lower daughter of a split does. The upper region
   * holds only a segment of another definition, so it is not covered. Repair purges the cells of a
   * dropped index key. It cannot repair a cell that names no index key.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testCheckTargetsRegions() throws Exception {
    byte[] m = Bytes.toBytes("m");
    byte[] t = Bytes.toBytes("t");
    TableName table = TableName.valueOf("T");
    RegionInfo lower = RegionInfoBuilder.newBuilder(table).setEndKey(m).build();
    RegionInfo middle = RegionInfoBuilder.newBuilder(table).setStartKey(m).setEndKey(t).build();
    RegionInfo upper = RegionInfoBuilder.newBuilder(table).setStartKey(t).build();
    HnswSegment.Descriptor base = written(new HnswSegment.Descriptor(KEY,
      HConstants.EMPTY_START_ROW, 100L, m, 10, null, 100L, DEFINITION), lower.getEncodedName());
    HnswSegment.Descriptor old = written(new HnswSegment.Descriptor(KEY, HConstants.EMPTY_START_ROW,
      50L, m, 10, null, 50L, DEFINITION), lower.getEncodedName());
    HnswSegment.Descriptor parent = written(
      new HnswSegment.Descriptor(KEY, m, 80L, HConstants.EMPTY_END_ROW, 10, null, 80L, DEFINITION),
      "parent");
    HnswSegment.Descriptor stale =
      written(new HnswSegment.Descriptor(KEY, t, 90L, HConstants.EMPTY_END_ROW, 10, null, 90L,
        HnswSegment.definitionDigest("stale")), upper.getEncodedName());

    HnswIndexReader.Catalog catalog = new HnswIndexReader.Catalog();
    catalog.segments.addAll(Arrays.asList(base, old, parent, stale));
    catalog.malformed.add(new byte[][] { Bytes.toBytes("a"), Bytes.toBytes("IDX\0junk") });
    catalog.strayKeys.put("GONE", Collections.singletonList(Bytes.toBytes("n")));
    catalog.keyless.add(new byte[][] { Bytes.toBytes("u"), Bytes.toBytes("junk") });
    HnswIndexContext hnsw = mock(HnswIndexContext.class);
    when(hnsw.getDefinition()).thenReturn(DEFINITION);
    HnswIndexReader reader = mock(HnswIndexReader.class);
    when(reader.getContext()).thenReturn(hnsw);
    when(reader.readCatalog()).thenReturn(catalog);
    when(reader.listRegions()).thenReturn(Arrays.asList(lower, middle, upper));
    when(reader.fenced(any())).thenAnswer(i -> {
      List<HnswSegment.Descriptor> fenced = new ArrayList<>();
      for (HnswSegment.Descriptor d : (List<HnswSegment.Descriptor>) i.getArgument(0)) {
        if (Bytes.equals(d.definition, DEFINITION)) {
          fenced.add(d);
        }
      }
      return fenced;
    });
    // The server retires the segments during the check, so the payload checks find nothing
    when(reader.open(any())).thenThrow(notFound(base));
    PTable index = mock(PTable.class);
    when(index.getIndexState()).thenReturn(PIndexState.ACTIVE);
    IndexFsckContext context = mock(IndexFsckContext.class);
    when(context.getIndexTable()).thenReturn(index);

    HnswIndexFsckProvider.Checks checks = new HnswIndexFsckProvider().check(context, reader);
    assertFalse(checks.rebuildAll);
    Map<String, Object> stray = new TreeMap<>();
    for (Map.Entry<String, Set<String>> e : checks.stray.entrySet()) {
      stray.put(e.getKey(), new ArrayList<>(e.getValue()));
    }
    assertEquals(Collections.singletonMap("GONE", Arrays.asList(middle.getEncodedName())), stray);
    assertEquals(
      new TreeSet<>(
        Arrays.asList(lower.getEncodedName(), middle.getEncodedName(), upper.getEncodedName())),
      checks.rebuild);
    Map<String, Integer> rules = new TreeMap<>();
    for (Finding finding : checks.findings) {
      rules.merge(finding.getRule(), 1, Integer::sum);
    }
    Map<String, Integer> expected = new TreeMap<>();
    expected.put(HnswIndexFsckProvider.STRAY_ROW, 3);
    expected.put(HnswIndexFsckProvider.SEGMENT_MALFORMED, 1);
    expected.put(HnswIndexFsckProvider.RANGE_UNCOVERED, 1);
    expected.put(HnswIndexFsckProvider.STACK_UNTRUSTED, 1);
    expected.put(HnswIndexFsckProvider.SEGMENT_SUPERSEDED, 1);
    assertEquals(checks.findings.toString(), expected, rules);
    for (Finding finding : checks.findings) {
      if (finding.getRule().equals(HnswIndexFsckProvider.SEGMENT_SUPERSEDED)) {
        assertEquals(Bytes.toStringBinary(old.identity), finding.getDetails().get("segment"));
      } else if (finding.getRule().equals(HnswIndexFsckProvider.STACK_UNTRUSTED)) {
        assertEquals(middle.getEncodedName(), finding.getDetails().get("region"));
        assertEquals("parent", finding.getDetails().get("writer"));
      } else if (finding.getRule().equals(HnswIndexFsckProvider.RANGE_UNCOVERED)) {
        assertEquals(upper.getEncodedName(), finding.getDetails().get("region"));
      }
    }
  }

  // Returns the descriptor of a segment as a listing returns it, with the region that wrote it
  private static HnswSegment.Descriptor written(HnswSegment.Descriptor d, String writer)
    throws Exception {
    byte[] family = QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY_BYTES;
    List<Cell> cells = new ArrayList<>(Arrays.asList(
      new KeyValue(d.anchor, family, HnswSegment.qualifier(d.identity, HnswSegment.END_KEY_TAG),
        d.endKey),
      new KeyValue(d.anchor, family, HnswSegment.qualifier(d.identity, HnswSegment.COUNT_TAG),
        Bytes.toBytes(d.count)),
      new KeyValue(d.anchor, family, HnswSegment.qualifier(d.identity, HnswSegment.REPLAY_FROM_TAG),
        Bytes.toBytes(d.replayFrom)),
      new KeyValue(d.anchor, family, HnswSegment.qualifier(d.identity, HnswSegment.DEFINITION_TAG),
        d.definition),
      new KeyValue(d.anchor, family, HnswSegment.qualifier(d.identity, HnswSegment.WRITER_TAG),
        Bytes.toBytes(writer))));
    cells.sort(CellComparator.getInstance());
    ResultScanner scanner = mock(ResultScanner.class);
    when(scanner.iterator()).thenReturn(Collections.singletonList(Result.create(cells)).iterator());
    Table table = mock(Table.class);
    when(table.getScanner(any(Scan.class))).thenReturn(scanner);
    return new HnswSegmentStore.TableStore(table).list(d.indexKey).get(0);
  }

  // Returns a mock reader that gives the catalog and the regions, and that applies the definition
  // fence
  @SuppressWarnings("unchecked")
  private static HnswIndexReader reader(HnswIndexReader.Catalog catalog, List<RegionInfo> regions)
    throws Exception {
    HnswIndexContext hnsw = mock(HnswIndexContext.class);
    when(hnsw.getDefinition()).thenReturn(DEFINITION);
    HnswIndexReader reader = mock(HnswIndexReader.class);
    when(reader.getContext()).thenReturn(hnsw);
    when(reader.readCatalog()).thenReturn(catalog);
    when(reader.listRegions()).thenReturn(regions);
    when(reader.fenced(any())).thenAnswer(i -> {
      List<HnswSegment.Descriptor> fenced = new ArrayList<>();
      for (HnswSegment.Descriptor d : (List<HnswSegment.Descriptor>) i.getArgument(0)) {
        if (Bytes.equals(d.definition, DEFINITION)) {
          fenced.add(d);
        }
      }
      return fenced;
    });
    return reader;
  }

  /**
   * A region without an anchor row holds no rows, and IndexTool counts it as built. Thus the checks
   * do not report it as uncovered, and repair does not select it.
   */
  @Test
  public void testRegionWithoutAnchorIsCovered() throws Exception {
    byte[] zero = new byte[] { 0 };
    TableName table = TableName.valueOf("T");
    RegionInfo empty = RegionInfoBuilder.newBuilder(table).setEndKey(zero).build();
    RegionInfo rest = RegionInfoBuilder.newBuilder(table).setStartKey(zero).build();
    assertNull(HnswSegment.anchor(empty.getStartKey(), empty.getEndKey()));
    HnswSegment.Descriptor base = written(new HnswSegment.Descriptor(KEY, zero, 100L,
      HConstants.EMPTY_END_ROW, 10, null, 100L, DEFINITION), rest.getEncodedName());
    HnswIndexReader.Catalog catalog = new HnswIndexReader.Catalog();
    catalog.segments.add(base);
    HnswIndexReader reader = reader(catalog, Arrays.asList(empty, rest));
    // The server retires the segment during the check, so the payload check finds nothing
    when(reader.open(any())).thenThrow(notFound(base));
    PTable index = mock(PTable.class);
    when(index.getIndexState()).thenReturn(PIndexState.ACTIVE);
    IndexFsckContext context = mock(IndexFsckContext.class);
    when(context.getIndexTable()).thenReturn(index);

    HnswIndexFsckProvider.Checks checks = new HnswIndexFsckProvider().check(context, reader);
    assertTrue(checks.findings.toString(), checks.findings.isEmpty());
    assertTrue(checks.rebuild.toString(), checks.rebuild.isEmpty());
  }

  /**
   * The inspect coverage command reports a region without an anchor row as covered. Its retirement
   * candidates are the segments that the region retires after its next full rebuild. These come
   * from the segments of all definitions that the region holds.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testCoverage() throws Exception {
    byte[] zero = new byte[] { 0 };
    TableName table = TableName.valueOf("T");
    RegionInfo empty = RegionInfoBuilder.newBuilder(table).setEndKey(zero).build();
    RegionInfo rest = RegionInfoBuilder.newBuilder(table).setStartKey(zero).build();
    HnswSegment.Descriptor base = written(new HnswSegment.Descriptor(KEY, zero, 100L,
      HConstants.EMPTY_END_ROW, 10, null, 100L, DEFINITION), rest.getEncodedName());
    HnswSegment.Descriptor stale =
      written(new HnswSegment.Descriptor(KEY, zero, 50L, HConstants.EMPTY_END_ROW, 10, null, 50L,
        HnswSegment.definitionDigest("stale")), rest.getEncodedName());
    HnswIndexReader reader = reader(new HnswIndexReader.Catalog(), Arrays.asList(empty, rest));
    when(reader.listSegments()).thenReturn(Arrays.asList(stale, base));
    IndexFsckContext context = mock(IndexFsckContext.class);
    Report report = new Report("inspect coverage", context);
    new HnswIndexInspector(context, reader).inspect(HnswIndexInspector.COVERAGE,
      Collections.emptyList(), report);

    List<Map<String, Object>> regions =
      (List<Map<String, Object>>) report.getInspection().get("regions");
    assertEquals(2, regions.size());
    assertEquals(regions.toString(), true, regions.get(0).get("covered"));
    assertEquals(regions.toString(), true, regions.get(1).get("covered"));
    assertEquals(regions.toString(), true, regions.get(1).get("exact"));
    assertEquals(Collections.singletonList(Bytes.toStringBinary(stale.identity)),
      report.getInspection().get("retirable"));
  }

  /**
   * A rebuilt region without an earlier base has no segment to retire. Repair still waits until the
   * region deletes the malformed cells of the index, so that its last check does not report them.
   */
  @Test
  public void testAwaitRetirementWaitsForMalformedCells() throws Exception {
    RegionInfo region = RegionInfoBuilder.newBuilder(TableName.valueOf("T")).build();
    HnswIndexReader.Catalog malformed = new HnswIndexReader.Catalog();
    malformed.malformed.add(new byte[][] { Bytes.toBytes("a"), Bytes.toBytes("IDX\0junk") });
    HnswIndexReader reader = reader(malformed, Collections.singletonList(region));
    when(reader.readCatalog()).thenReturn(malformed, new HnswIndexReader.Catalog());
    HnswIndexFsckProvider.awaitRetirement(reader, Collections.singleton(region.getEncodedName()));
    verify(reader, times(2)).readCatalog();
  }

  /**
   * A failed wait for the verification job kills the job, so that a retry does not run a second job
   * at the same time.
   */
  @Test
  public void testAwaitJobKillsRunningJob() throws Exception {
    Job job = mock(Job.class);
    IOException poll = new IOException("status poll failed");
    when(job.waitForCompletion(false)).thenThrow(poll);
    when(job.isComplete()).thenReturn(false);
    try {
      HnswIndexFsckProvider.awaitJob(job);
      fail();
    } catch (IOException e) {
      assertSame(poll, e);
    }
    verify(job).killJob();

    // A job that the kill did not stop can still run, so a retry is not permitted
    doThrow(new IOException("kill failed")).when(job).killJob();
    try {
      HnswIndexFsckProvider.awaitJob(job);
      fail();
    } catch (DoNotRetryIOException e) {
      assertSame(poll, e.getCause());
    }
  }

  /** The tool does not kill a job that did not start. */
  @Test
  public void testAwaitJobNotSubmitted() throws Exception {
    Job job = mock(Job.class);
    IOException submit = new IOException("submit failed");
    when(job.waitForCompletion(false)).thenThrow(submit);
    when(job.isComplete()).thenThrow(new IllegalStateException("Job in state DEFINE"));
    try {
      HnswIndexFsckProvider.awaitJob(job);
      fail();
    } catch (IOException e) {
      assertSame(submit, e);
    }
    verify(job, never()).killJob();
  }

  private static Result resultRow(byte[] key, long missing, long invalid, long orphan) {
    List<Cell> cells = new ArrayList<>();
    byte[] family = IndexVerificationResultRepository.RESULT_TABLE_COLUMN_FAMILY;
    cells.add(new KeyValue(key, family,
      IndexVerificationResultRepository.BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT_BYTES,
      Bytes.toBytes(Long.toString(missing))));
    cells.add(new KeyValue(key, family,
      IndexVerificationResultRepository.BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_BYTES,
      Bytes.toBytes(Long.toString(invalid))));
    cells.add(new KeyValue(key, family,
      IndexVerificationResultRepository.BEFORE_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT_BYTES,
      Bytes.toBytes(Long.toString(orphan))));
    cells.sort(CellComparator.getInstance());
    return Result.create(cells);
  }

  /**
   * Repair finds the regions with failures in the verification result row of each region. This
   * works for all region keys, also for keys that hold the separator of the row key.
   */
  @Test
  public void testFailedRegions() throws Exception {
    Counters counters = new Counters();
    counters.findCounter(HnswVerifyMapper.Counters.MISSING).setValue(1);
    counters.findCounter(HnswVerifyMapper.Counters.ORPHAN).setValue(1);
    HnswIndexFsckProvider.VerifyRun run = new HnswIndexFsckProvider.VerifyRun(123, counters);
    assertTrue(run.hasFailures());

    String failedMissing = "0123456789abcdef0123456789abcdef";
    String failedOrphan = "fedcba9876543210fedcba9876543210";
    String clean = "00000000000000000000000000000000";
    List<Result> rows = Arrays.asList(
      resultRow(Bytes.toBytes("123|IDX|" + failedMissing + "|doc500|chunk9|doc9|x"), 1, 0, 0),
      resultRow(Bytes.toBytes("123|IDX|" + clean + "|a|b"), 0, 0, 0),
      resultRow(Bytes.toBytes("123|IDX|" + failedOrphan + "||"), 0, 0, 1));
    ResultScanner scanner = mock(ResultScanner.class);
    when(scanner.iterator()).thenReturn(rows.iterator());
    Table results = mock(Table.class);
    when(results.getScanner(any(Scan.class))).thenReturn(scanner);
    ConnectionQueryServices services = mock(ConnectionQueryServices.class);
    when(services.getTable(IndexVerificationResultRepository.getResultTableNameBytes()))
      .thenReturn(results);
    PhoenixConnection conn = mock(PhoenixConnection.class);
    when(conn.getQueryServices()).thenReturn(services);
    PTable index = mock(PTable.class);
    when(index.getPhysicalName()).thenReturn(PNameFactory.newName("IDX"));
    HnswIndexContext hnsw = mock(HnswIndexContext.class);
    when(hnsw.getIndexTable()).thenReturn(index);
    when(hnsw.getConnection()).thenReturn(conn);
    assertEquals(new HashSet<>(Arrays.asList(failedMissing, failedOrphan)),
      run.failedRegions(hnsw));
  }

  /**
   * Verification skips a region whose segment is too large for the heap of a verification task.
   * This is a warning, as in fsck, and not an error that repair must correct.
   */
  @Test
  public void testUncheckedRegionsAreWarnings() {
    Counters counters = new Counters();
    counters.findCounter(HnswVerifyMapper.Counters.UNCHECKED).setValue(2);
    HnswIndexFsckProvider.VerifyRun run = new HnswIndexFsckProvider.VerifyRun(123, counters);
    assertFalse(run.hasFailures());
    Finding unchecked = null;
    for (Finding finding : run.findings) {
      assertNotEquals(finding.toString(), Severity.ERROR, finding.getSeverity());
      if (finding.getRule().equals(HnswIndexFsckProvider.SEGMENTS_UNCHECKED)) {
        unchecked = finding;
      }
    }
    assertEquals(Severity.WARN, unchecked.getSeverity());
    assertEquals(2L, unchecked.getDetails().get("regions"));
  }

  /**
   * The rows of a region that its segments do not cover are not missing, because the region reads
   * them without the index. As in fsck, they give a warning, and verification has no failure that
   * repair acts on.
   */
  @Test
  public void testUncoveredRowsAreWarnings() {
    Counters counters = new Counters();
    counters.findCounter(HnswVerifyMapper.Counters.ROWS).setValue(100);
    counters.findCounter(HnswVerifyMapper.Counters.VALID).setValue(75);
    counters.findCounter(HnswVerifyMapper.Counters.UNCOVERED).setValue(25);
    HnswIndexFsckProvider.VerifyRun run = new HnswIndexFsckProvider.VerifyRun(123, counters);
    assertFalse(run.hasFailures());
    Finding uncovered = null;
    for (Finding finding : run.findings) {
      assertNotEquals(finding.toString(), Severity.ERROR, finding.getSeverity());
      if (finding.getRule().equals(HnswIndexFsckProvider.ROWS_UNCOVERED)) {
        uncovered = finding;
      }
    }
    assertEquals(Severity.WARN, uncovered.getSeverity());
    assertEquals(25L, uncovered.getDetails().get("count"));
  }

  /** Export file names stay short for long start keys, and are different for different segments. */
  @Test
  public void testExportFileName() {
    byte[] start = new byte[300];
    Arrays.fill(start, (byte) 'k');
    HnswSegment.Descriptor a = new HnswSegment.Descriptor(KEY, start.clone(), 7L,
      HConstants.EMPTY_END_ROW, 1, null, 7L, DEFINITION);
    start[299] = 'j';
    HnswSegment.Descriptor b = new HnswSegment.Descriptor(KEY, start.clone(), 7L,
      HConstants.EMPTY_END_ROW, 1, null, 7L, DEFINITION);
    String name = HnswIndexInspector.fileName(a);
    assertTrue(name, name.length() < 64 && name.startsWith("7-"));
    assertEquals(name, HnswIndexInspector.fileName(a));
    assertNotEquals(name, HnswIndexInspector.fileName(b));
  }

  /** A row with a cell timestamp at or after the replay start counts as changed. */
  @Test
  public void testChangedSince() {
    byte[] row = Bytes.toBytes("r");
    List<Cell> cells = new ArrayList<>(
      Arrays.asList(new KeyValue(row, Bytes.toBytes("0"), Bytes.toBytes("A"), 5, Bytes.toBytes(1)),
        new KeyValue(row, Bytes.toBytes("0"), Bytes.toBytes("V"), 9, Bytes.toBytes(1))));
    Result result = Result.create(cells);
    assertTrue(HnswVerifyMapper.changedSince(result, 9));
    assertFalse(HnswVerifyMapper.changedSince(result, 10));
  }
}
