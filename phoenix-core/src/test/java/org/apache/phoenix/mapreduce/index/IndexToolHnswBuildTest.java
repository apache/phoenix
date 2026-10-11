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
package org.apache.phoenix.mapreduce.index;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol.MutationCode;
import org.apache.phoenix.hbase.index.hnsw.HnswSegment;
import org.apache.phoenix.hbase.index.vector.HnswIndexManager;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.apache.phoenix.util.EnvironmentEdge;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

/**
 * Tests when {@link IndexTool#buildHnswRegions} counts a region as built, and when the build fails.
 * The bar is the oldest base segment time that completes a region, and
 * {@link IndexTool#buildHnswIndex} sets it. Mocked tables simulate the region servers: a build scan
 * of a region writes a base segment to the {@code V#} family of the data table.
 */
public class IndexToolHnswBuildTest {
  private static final byte[] FAMILY = QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY_BYTES;
  private static final byte[] DATA = Bytes.toBytes("T");
  private static final byte[] INDEX = Bytes.toBytes("IDX");

  private final List<HRegionLocation> locations = new ArrayList<>();
  // The segments in the V# family of the data table
  private final List<Result> segments = Collections.synchronizedList(new ArrayList<>());
  private final List<String> triggered = Collections.synchronizedList(new ArrayList<>());
  // The attribute values of the build scans
  private final List<byte[]> requests = Collections.synchronizedList(new ArrayList<>());
  // The time at which the simulated servers accept a build request. A rebuild for the request
  // starts later, and writes a base segment one tick newer
  private final AtomicLong serverTime = new AtomicLong(5_000);
  // When false, the simulated rebuilds do not finish
  private volatile boolean serversBuild = true;
  // The answers of the data table to a build scan and to a scan of V#
  private volatile Answer<ResultScanner> build;
  private volatile Answer<ResultScanner> list;
  private byte[] definition;
  private ConnectionQueryServices services;
  private PhoenixConnection conn;
  private PTable dataTable;
  private PTable indexTable;

  @Before
  public void setUp() throws Exception {
    dataTable = mock(PTable.class);
    when(dataTable.getPhysicalName()).thenReturn(PNameFactory.newName(DATA));
    PColumn column = mock(PColumn.class);
    when(column.getFamilyName()).thenReturn(PNameFactory.newName("0"));
    doReturn(PVectorFloat.INSTANCE).when(column).getDataType();
    when(column.getExpressionStr()).thenReturn("\"0\".\"V\"");
    indexTable = mock(PTable.class);
    when(indexTable.getName()).thenReturn(PNameFactory.newName(INDEX));
    when(indexTable.getPhysicalName()).thenReturn(PNameFactory.newName(DATA));
    when(indexTable.getColumnFamilies()).thenReturn(Collections.emptyList());
    when(indexTable.getIndexType()).thenReturn(IndexType.VECTOR_LOCAL);
    when(indexTable.getVectorIndex()).thenReturn(new PTable.VectorIndex.Builder()
      .setAlgorithm("HNSW").setDistanceMetric("L2").setDimension(2).setHnswM(16)
      .setHnswEfConstruction(100).setHnswAlpha(1.2).setQuantizationType("NONE").build());
    when(indexTable.getColumns()).thenReturn(Collections.singletonList(column));
    definition = HnswSegment.definitionDigest(HnswIndexManager.definition(indexTable));

    build = invocation -> {
      Scan scan = invocation.getArgument(0);
      triggered.add(Bytes.toStringBinary(scan.getStartRow()));
      requests.add(scan.getAttribute(BaseScannerRegionObserverConstants.HNSW_BUILD));
      if (serversBuild) {
        segments.add(segment(scan.getStartRow(), scan.getStopRow(), serverTime.get() + 1));
      }
      return scanner(Collections.singletonList(accepted(serverTime.get())));
    };
    list = invocation -> scanner(new ArrayList<>(segments));
    Table data = mock(Table.class);
    when(data.getScanner(any(Scan.class))).thenAnswer(invocation -> {
      Scan scan = invocation.getArgument(0);
      return scan.getAttribute(BaseScannerRegionObserverConstants.HNSW_BUILD) != null
        ? build.answer(invocation)
        : list.answer(invocation);
    });
    services = mock(ConnectionQueryServices.class);
    when(services.getTable(DATA)).thenReturn(data);
    when(services.getAllTableRegions(DATA)).thenReturn(locations);
    conn = mock(PhoenixConnection.class);
    when(conn.getQueryServices()).thenReturn(services);
  }

  @After
  public void tearDown() {
    EnvironmentEdgeManager.reset();
  }

  private static ResultScanner scanner(List<Result> results) throws IOException {
    Iterator<Result> it = results.iterator();
    ResultScanner scanner = mock(ResultScanner.class);
    when(scanner.iterator()).thenReturn(it);
    when(scanner.next()).thenAnswer(invocation -> it.hasNext() ? it.next() : null);
    return scanner;
  }

  // The one cell that a build scan returns: the time at which the region accepted the request
  private static Result accepted(long time) {
    return Result.create(Collections.singletonList(new KeyValue(HConstants.EMPTY_START_ROW,
      HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY, time, Bytes.toBytes(time))));
  }

  // Returns the cells of a base segment of the index with the given definition, as a listing scan
  // of V# returns them
  private static Result segment(byte[] start, byte[] end, long time, byte[] definition) {
    return segment(start, end, time, null, definition);
  }

  // Returns the cells of a segment as a listing scan of V# returns them. A base time makes the
  // segment a delta
  private static Result segment(byte[] start, byte[] end, long time, Long baseTime,
    byte[] definition) {
    HnswSegment.Descriptor d =
      new HnswSegment.Descriptor(INDEX, start, time, end, 1, baseTime, time, definition);
    List<Cell> cells = new ArrayList<>();
    if (baseTime != null) {
      cells.add(
        new KeyValue(d.anchor, FAMILY, HnswSegment.qualifier(d.identity, HnswSegment.BASE_TIME_TAG),
          time, Bytes.toBytes(baseTime)));
    }
    cells.add(new KeyValue(d.anchor, FAMILY,
      HnswSegment.qualifier(d.identity, HnswSegment.END_KEY_TAG), time, end));
    cells.add(new KeyValue(d.anchor, FAMILY,
      HnswSegment.qualifier(d.identity, HnswSegment.COUNT_TAG), time, Bytes.toBytes(1)));
    cells.add(new KeyValue(d.anchor, FAMILY,
      HnswSegment.qualifier(d.identity, HnswSegment.REPLAY_FROM_TAG), time, Bytes.toBytes(time)));
    cells.add(new KeyValue(d.anchor, FAMILY,
      HnswSegment.qualifier(d.identity, HnswSegment.DEFINITION_TAG), time, definition));
    cells.add(new KeyValue(d.anchor, FAMILY,
      HnswSegment.qualifier(d.identity, HnswSegment.WRITER_TAG), time, Bytes.toBytes("writer")));
    return Result.create(cells);
  }

  private Result segment(byte[] start, byte[] end, long time) {
    return segment(start, end, time, definition);
  }

  private RegionInfo region(String start, String end) {
    RegionInfo region = RegionInfoBuilder.newBuilder(TableName.valueOf(DATA))
      .setStartKey(Bytes.toBytes(start)).setEndKey(Bytes.toBytes(end)).build();
    locations.add(new HRegionLocation(region, ServerName.valueOf("rs", 1, 1)));
    return region;
  }

  /**
   * A rerun, or a retry of repair, counts a region as built when its base segment is at least as
   * new as the bar. This is also true when the segment is older than the client clock. The build
   * does not trigger the region again.
   */
  @Test(timeout = 30_000)
  public void testBuiltSinceBarIsKept() throws Exception {
    region("", "m");
    region("m", "");
    segments.add(segment(Bytes.toBytes(""), Bytes.toBytes("m"), 1_000));
    segments.add(segment(Bytes.toBytes("m"), HConstants.EMPTY_END_ROW, 1_200));
    IndexTool.buildHnswRegions(conn, dataTable, indexTable, r -> 1_000);
    assertTrue(triggered.toString(), triggered.isEmpty());
  }

  /**
   * IndexTool uses the last DDL time of the index as the bar. The server sets this time when the
   * index enters its current state. A rerun keeps the regions that an earlier run built, also when
   * their bases are older than the client clock. Then the rerun makes the index ACTIVE.
   */
  @Test(timeout = 30_000)
  public void testBuildIndexKeepsBuiltRegions() throws Exception {
    region("", "m");
    region("m", "");
    segments.add(segment(Bytes.toBytes(""), Bytes.toBytes("m"), 1_000));
    segments.add(segment(Bytes.toBytes("m"), HConstants.EMPTY_END_ROW, 1_200));
    EnvironmentEdgeManager.injectEdge(new EnvironmentEdge() {
      @Override
      public long currentTime() {
        return 10_000;
      }
    });
    PTable current = mock(PTable.class);
    when(current.getLastDDLTimestamp()).thenReturn(1_000L);
    when(conn.getTableNoCache(Bytes.toString(INDEX))).thenReturn(current);
    when(services.updateIndexState(any(), isNull()))
      .thenReturn(new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS, 0, null));
    IndexTool.buildHnswIndex(conn, dataTable, indexTable);
    assertTrue(triggered.toString(), triggered.isEmpty());
    ArgumentCaptor<List<Mutation>> update = ArgumentCaptor.forClass(List.class);
    verify(services).updateIndexState(update.capture(), isNull());
    assertArrayEquals(PIndexState.ACTIVE.getSerializedBytes(),
      CellUtil.cloneValue(((Put) update.getValue().get(0))
        .get(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES, PhoenixDatabaseMetaData.INDEX_STATE_BYTES)
        .get(0)));
  }

  /**
   * A region can split after the last listing of the build and before the index is ACTIVE. A
   * daughter that opens while the index is BUILDING does not rebuild on its own. Thus the build
   * lists the regions again after the index is ACTIVE, and builds both daughters.
   */
  @Test(timeout = 30_000)
  public void testRegionSplitBeforeActiveIsBuilt() throws Exception {
    region("", "m");
    region("m", "");
    segments.add(segment(Bytes.toBytes(""), Bytes.toBytes("m"), 1_000));
    segments.add(segment(Bytes.toBytes("m"), HConstants.EMPTY_END_ROW, 1_200));
    PTable current = mock(PTable.class);
    when(current.getLastDDLTimestamp()).thenReturn(1_000L);
    when(conn.getTableNoCache(Bytes.toString(INDEX))).thenReturn(current);
    when(services.updateIndexState(any(), isNull())).thenAnswer(invocation -> {
      // The second region splits before the index is ACTIVE
      locations.remove(1);
      region("m", "t");
      region("t", "");
      return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS, 0, null);
    });
    IndexTool.buildHnswIndex(conn, dataTable, indexTable);
    assertEquals(Arrays.asList("m", "t"), triggered);
  }

  /**
   * A region server with a clock behind the clock that set the bar writes bases older than the bar.
   * A base that the server builds after it accepts the request also completes its region. This is
   * true for a region with a stale base and for a region with no base. Thus the build triggers each
   * region one time.
   */
  @Test(timeout = 30_000)
  public void testServerClockBehindBar() throws Exception {
    region("", "m");
    region("m", "");
    segments.add(segment(Bytes.toBytes(""), Bytes.toBytes("m"), 500));
    serverTime.set(800);
    IndexTool.buildHnswRegions(conn, dataTable, indexTable, r -> 1_000);
    assertEquals(Arrays.asList("", "m"), triggered);
  }

  /**
   * A delta newer than the bar is not a rebuild. A base older than the bar is also not a rebuild.
   * The build triggers only the stale region, and waits for its new base.
   */
  @Test(timeout = 30_000)
  public void testDeltaIsNotRebuild() throws Exception {
    region("", "m");
    region("m", "");
    segments.add(segment(Bytes.toBytes(""), Bytes.toBytes("m"), 900));
    segments.add(segment(Bytes.toBytes(""), Bytes.toBytes("m"), 2_000, 900L, definition));
    segments.add(segment(Bytes.toBytes("m"), HConstants.EMPTY_END_ROW, 1_500));
    IndexTool.buildHnswRegions(conn, dataTable, indexTable, r -> 1_000);
    assertEquals(Arrays.asList(""), triggered);
  }

  /**
   * A build that takes longer than the stall limit does not fail while regions continue to
   * complete. At each poll, the client clock moves forward by three quarters of the limit.
   */
  @Test(timeout = 60_000)
  public void testSlowBuildWithProgressSucceeds() throws Exception {
    region("", "f");
    region("f", "m");
    region("m", "");
    AtomicInteger polls = new AtomicInteger();
    EnvironmentEdgeManager.injectEdge(new EnvironmentEdge() {
      @Override
      public long currentTime() {
        return polls.incrementAndGet() * (IndexTool.HNSW_BUILD_STALL_MS * 3 / 4);
      }
    });
    // Each poll finds one more completed region, independent of the build scans
    build = i -> scanner(Collections.emptyList());
    List<RegionInfo> pending = new ArrayList<>();
    for (HRegionLocation location : locations) {
      pending.add(location.getRegion());
    }
    list = invocation -> {
      RegionInfo next = pending.remove(0);
      segments.add(segment(next.getStartKey(), next.getEndKey(), Long.MAX_VALUE / 2));
      return scanner(new ArrayList<>(segments));
    };
    IndexTool.buildHnswRegions(conn, dataTable, indexTable, r -> 1_000);
    assertTrue(pending.isEmpty());
    assertTrue(
      polls.get() * (IndexTool.HNSW_BUILD_STALL_MS * 3 / 4) > 2 * IndexTool.HNSW_BUILD_STALL_MS);
  }

  /** A build fails when no region completes within the stall limit. */
  @Test(timeout = 30_000)
  public void testStalledBuildFails() throws Exception {
    region("", "");
    AtomicInteger polls = new AtomicInteger();
    EnvironmentEdgeManager.injectEdge(new EnvironmentEdge() {
      @Override
      public long currentTime() {
        return polls.incrementAndGet() * (IndexTool.HNSW_BUILD_STALL_MS + 1);
      }
    });
    // The rebuilds on the servers stop or fail, so no segment appears
    serversBuild = false;
    try {
      IndexTool.buildHnswRegions(conn, dataTable, indexTable, r -> 1_000);
      fail();
    } catch (DoNotRetryIOException expected) {
      assertTrue(expected.getMessage(), expected.getMessage().contains("stalled"));
    }
  }

  /**
   * A rebuild swaps when it starts. Thus a rebuild that runs when the region accepts the request
   * swapped before the request, and its base does not complete the region. The build waits for the
   * base of the rebuild that the request queued, which appears at the third poll. The build does
   * not trigger the region again. The first request holds Long.MAX_VALUE, because no earlier
   * request has an accepted time.
   */
  @Test(timeout = 30_000)
  public void testRebuildRunningAtRequestDoesNotCount() throws Exception {
    region("", "");
    AtomicInteger polls = new AtomicInteger();
    list = invocation -> {
      if (polls.incrementAndGet() == 3) {
        segments
          .add(segment(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, serverTime.get() + 1));
      }
      return scanner(new ArrayList<>(segments));
    };
    build = invocation -> {
      requests.add(((Scan) invocation.getArgument(0))
        .getAttribute(BaseScannerRegionObserverConstants.HNSW_BUILD));
      // The rebuild that runs at the time of the request ends now
      segments
        .add(segment(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, serverTime.get() - 1));
      return scanner(Collections.singletonList(accepted(serverTime.get())));
    };
    IndexTool.buildHnswRegions(conn, dataTable, indexTable, r -> 1_000);
    assertEquals(3, polls.get());
    assertEquals(1, requests.size());
    byte[] request = requests.get(0);
    assertEquals(Long.MAX_VALUE, Bytes.toLong(request));
    assertEquals(Bytes.toString(INDEX),
      Bytes.toString(request, Bytes.SIZEOF_LONG, request.length - Bytes.SIZEOF_LONG));
  }

  /**
   * A base of another index definition, for example of an index that was dropped and created again
   * with the same name, does not complete its region. A region without an anchor row holds no rows,
   * so it is built without a segment.
   */
  @Test(timeout = 30_000)
  public void testSegmentOfAnotherDefinitionDoesNotCount() throws Exception {
    region("", "\0");
    region("\0", "m");
    region("m", "");
    byte[] other = HnswSegment.definitionDigest("other");
    segments.add(segment(Bytes.toBytes("\0"), Bytes.toBytes("m"), 1_200, other));
    segments.add(segment(Bytes.toBytes("m"), HConstants.EMPTY_END_ROW, 1_200));
    IndexTool.buildHnswRegions(conn, dataTable, indexTable, r -> 1_000);
    assertEquals(Collections.singletonList("\\x00"), triggered);
  }
}
