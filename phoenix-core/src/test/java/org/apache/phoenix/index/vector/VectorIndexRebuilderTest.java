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

import static org.apache.phoenix.index.vector.VectorIndexRebuilder.cacheLagMs;
import static org.apache.phoenix.index.vector.VectorIndexRebuilder.childHeaderKey;
import static org.apache.phoenix.index.vector.VectorIndexRebuilder.childLinkRange;
import static org.apache.phoenix.index.vector.VectorIndexRebuilder.childLinkScan;
import static org.apache.phoenix.index.vector.VectorIndexRebuilder.isDue;
import static org.apache.phoenix.index.vector.VectorIndexRebuilder.metadataWaitMs;
import static org.apache.phoenix.index.vector.VectorIndexRebuilder.retireAfter;
import static org.apache.phoenix.index.vector.VectorIndexRebuilder.sleepAndCheckClaim;
import static org.apache.phoenix.index.vector.VectorIndexRebuilder.viewCacheLag;
import static org.apache.phoenix.index.vector.VectorIndexRebuilder.viewFrequency;
import static org.apache.phoenix.index.vector.VectorIndexRebuilder.viewName;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.OptionalLong;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol.MutationCode;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.vector.VectorIndexRebuilder.ViewCacheLag;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.JacksonUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class VectorIndexRebuilderTest {

  private static final long INTERVAL = 86400000L;
  private static final long NOW = 1_000_000_000_000L;
  private static final String INDEX = "S.IDX";

  @Test
  public void testNeverReconciledIsDue() {
    assertTrue(isDue(null, INTERVAL, NOW));
  }

  @Test
  public void testNotDueWithinInterval() {
    assertFalse(isDue(NOW, INTERVAL, NOW));
    assertFalse(isDue(NOW - INTERVAL + 1, INTERVAL, NOW));
  }

  @Test
  public void testDueAtInterval() {
    assertTrue(isDue(NOW - INTERVAL, INTERVAL, NOW));
    assertTrue(isDue(NOW - 10 * INTERVAL, INTERVAL, NOW));
  }

  @Test
  public void testZeroIntervalIsAlwaysDue() {
    assertTrue(isDue(NOW, 0, NOW));
  }

  /** Verifies that a future update time, for example from clock skew, does not make a task due. */
  @Test
  public void testFutureUpdateIsNotDue() {
    assertFalse(isDue(NOW + INTERVAL, INTERVAL, NOW));
    assertFalse(isDue(NOW + 1, 0, NOW));
  }

  /** Verifies that the frequency ALWAYS adds nothing to the population wait. */
  @Test
  public void testMetadataWaitWithoutCacheFrequency() {
    assertEquals(0, cacheLagMs(0, 0, 0));
    assertEquals(5000, metadataWaitMs(5000, cacheLagMs(0, 0, 0)));
    assertEquals(0, metadataWaitMs(0, 0));
  }

  /** Verifies that the lag is the largest frequency of the data table, its views, and the index. */
  @Test
  public void testCacheLagIsLargestFrequency() {
    assertEquals(60000, cacheLagMs(0, 60000, 60000));
    assertEquals(30000, cacheLagMs(0, 10000, 30000));
    assertEquals(30000, cacheLagMs(0, 30000, 10000));
    assertEquals(600000, cacheLagMs(0, 15000, 15000, 600000, 1000));
    assertEquals(65000, metadataWaitMs(5000, 60000));
  }

  /**
   * Verifies that a table with the default frequency uses the default of the connection, and that a
   * frequency set on the table has priority.
   */
  @Test
  public void testCacheLagUsesDefaultFrequency() {
    assertEquals(20000, cacheLagMs(20000, 0, 0));
    assertEquals(20000, cacheLagMs(20000, 0, 5000));
    assertEquals(5000, cacheLagMs(20000, 5000, 5000));
    assertEquals(20000, cacheLagMs(20000, 5000, 5000, 0));
  }

  /** Verifies that the frequency NEVER and an overflow give an unbounded wait. */
  @Test
  public void testMetadataWaitIsUnboundedForNever() {
    assertEquals(Long.MAX_VALUE, cacheLagMs(0, Long.MAX_VALUE, 0));
    assertEquals(Long.MAX_VALUE, cacheLagMs(0, 0, Long.MAX_VALUE));
    assertEquals(Long.MAX_VALUE, cacheLagMs(Long.MAX_VALUE, 0, 1000));
    assertEquals(Long.MAX_VALUE, cacheLagMs(0, 1000, 1000, Long.MAX_VALUE));
    assertEquals(Long.MAX_VALUE, metadataWaitMs(1000, Long.MAX_VALUE));
    assertEquals(Long.MAX_VALUE, metadataWaitMs(10, Long.MAX_VALUE - 5));
    assertEquals(Long.MAX_VALUE - 5, metadataWaitMs(0, Long.MAX_VALUE - 5));
  }

  /**
   * Verifies that the retire time is the promotion time plus the metadata wait plus the clock skew
   * bound. An overflow gives the largest time, and negative values count as zero.
   */
  @Test
  public void testRetireAfterAddsWaitAndClockSkew() {
    assertEquals(1000 + 65000 + 30000, retireAfter(1000, 65000, 30000));
    assertEquals(1000, retireAfter(1000, 0, 0));
    assertEquals(1000, retireAfter(1000, -1, -1));
    assertEquals(Long.MAX_VALUE, retireAfter(1000, Long.MAX_VALUE, 30000));
    assertEquals(Long.MAX_VALUE, retireAfter(1000, Long.MAX_VALUE - 1000, 1));
    assertEquals(Long.MAX_VALUE, retireAfter(Long.MAX_VALUE - 10, 5, 5));
  }

  /** Verifies that negative values count as zero. */
  @Test
  public void testMetadataWaitIgnoresNegativeValues() {
    assertEquals(1000, cacheLagMs(0, 1000, -5));
    assertEquals(0, cacheLagMs(-1, -1, -1));
    assertEquals(1000, metadataWaitMs(-1, 1000));
    assertEquals(0, metadataWaitMs(-1, -1));
  }

  /**
   * Verifies the frequency of view header rows. A header without the frequency, with an empty
   * frequency, or with the frequency zero uses the default. A row that is not a view header does
   * not count.
   */
  @Test
  public void testViewCacheLagFromHeaderRows() {
    Result view = header(PTableType.VIEW, 4000L);
    Result viewWithDefault = header(PTableType.VIEW, null);
    Result localIndex = header(PTableType.INDEX, 90000L);
    Result dropped = Result.create(new Cell[0]);
    assertEquals(0, viewCacheLagMs(new Result[0], 20000));
    assertEquals(4000, viewCacheLagMs(new Result[] { view, localIndex, dropped }, 0));
    assertEquals(20000, viewCacheLagMs(new Result[] { view, viewWithDefault }, 20000));
    assertEquals(4000, viewCacheLagMs(new Result[] { view, viewWithDefault }, 1000));
    assertEquals(20000, viewCacheLagMs(new Result[] { header(PTableType.VIEW, 0L) }, 20000));
    List<Cell> emptyCells = new ArrayList<>(header(PTableType.VIEW, null).listCells());
    emptyCells.add(new KeyValue(Bytes.toBytes("V"), PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
      PhoenixDatabaseMetaData.UPDATE_CACHE_FREQUENCY_BYTES, HConstants.EMPTY_BYTE_ARRAY));
    emptyCells.sort(CellComparator.getInstance());
    assertEquals(20000, viewCacheLagMs(new Result[] { Result.create(emptyCells) }, 20000));
    assertEquals(Long.MAX_VALUE,
      viewCacheLagMs(new Result[] { view, header(PTableType.VIEW, Long.MAX_VALUE) }, 0));
  }

  private static Result header(PTableType type, Long frequency) {
    byte[] row = Bytes.toBytes("V");
    List<Cell> cells = new ArrayList<>();
    cells.add(new KeyValue(row, PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
      PhoenixDatabaseMetaData.TABLE_TYPE_BYTES, Bytes.toBytes(type.getSerializedValue())));
    if (frequency != null) {
      cells.add(new KeyValue(row, PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
        PhoenixDatabaseMetaData.UPDATE_CACHE_FREQUENCY_BYTES, PLong.INSTANCE.toBytes(frequency)));
    }
    cells.sort(CellComparator.getInstance());
    return Result.create(cells);
  }

  /** Returns the largest effective frequency of view header rows. */
  private static long viewCacheLagMs(Result[] headers, long defaultFrequency) {
    long lag = 0;
    for (Result header : headers) {
      OptionalLong frequency = viewFrequency(header);
      if (frequency.isPresent()) {
        lag = Math.max(lag, cacheLagMs(defaultFrequency, frequency.getAsLong()));
      }
    }
    return lag;
  }

  private static final byte[] ROOT = SchemaUtil.getTableKey(null, "S", "T");

  /**
   * A catalog in memory for the view walk. It counts the reads, and it keeps the batch sizes of the
   * reads.
   */
  private static final class FakeCatalog {
    private final NavigableSet<byte[]> links = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    private final Map<ImmutableBytesPtr, Result> headers = new HashMap<>();
    private final Map<ImmutableBytesPtr, OptionalLong> resolved = new HashMap<>();
    private final List<Integer> linkBatches = new ArrayList<>();
    private final List<Integer> headerBatches = new ArrayList<>();
    private final List<byte[]> resolvedViews = new ArrayList<>();

    /** Adds a child link and returns the header row key of the child. */
    byte[] link(byte[] parent, String childTenant, String childName) {
      links.add(linkRow(parent, childTenant, childName));
      return SchemaUtil.getTableKey(childTenant, SchemaUtil.getSchemaNameFromFullName(childName),
        SchemaUtil.getTableNameFromFullName(childName));
    }

    /** Adds a view with a child link and a header row, and returns its header row key. */
    byte[] view(byte[] parent, String childTenant, String childName, Long frequency) {
      byte[] child = link(parent, childTenant, childName);
      headers.put(new ImmutableBytesPtr(child), header(PTableType.VIEW, frequency));
      return child;
    }

    void resolvesTo(byte[] view, OptionalLong frequency) {
      resolved.put(new ImmutableBytesPtr(view), frequency);
    }

    List<byte[]> readLinks(List<byte[]> parents) {
      linkBatches.add(parents.size());
      List<byte[]> rows = new ArrayList<>();
      for (byte[] parent : parents) {
        RowRange range = childLinkRange(parent);
        for (byte[] row : links.subSet(range.getStartRow(), true, range.getStopRow(), false)) {
          assertTrue(range.contains(row));
          rows.add(row);
        }
      }
      return rows;
    }

    Result[] readHeaders(List<byte[]> keys) {
      headerBatches.add(keys.size());
      Result[] results = new Result[keys.size()];
      for (int i = 0; i < keys.size(); i++) {
        results[i] =
          headers.getOrDefault(new ImmutableBytesPtr(keys.get(i)), Result.create(new Cell[0]));
      }
      return results;
    }

    OptionalLong resolve(byte[] view) {
      resolvedViews.add(view);
      return resolved.getOrDefault(new ImmutableBytesPtr(view), OptionalLong.empty());
    }

    ViewCacheLag walk(long defaultFrequency, int parentBatch, int headerBatch) throws Exception {
      return viewCacheLag(this::readLinks, this::readHeaders, this::resolve, ROOT, defaultFrequency,
        parentBatch, headerBatch);
    }

    ViewCacheLag walk() throws Exception {
      return walk(0, 1000, 1000);
    }
  }

  private static byte[] linkRow(byte[] parent, String childTenant, String childName) {
    return ByteUtil.concat(parent, QueryConstants.SEPARATOR_BYTE_ARRAY,
      childTenant == null ? ByteUtil.EMPTY_BYTE_ARRAY : Bytes.toBytes(childTenant),
      QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes(childName));
  }

  /** Verifies that a table without views reads its child links once and no header rows. */
  @Test
  public void testViewWalkWithoutViews() throws Exception {
    FakeCatalog catalog = new FakeCatalog();
    // A view of a table whose name extends the name of the data table does not count
    catalog.view(SchemaUtil.getTableKey(null, "S", "TX"), null, "S.OV", Long.MAX_VALUE);
    ViewCacheLag lag = catalog.walk();
    assertEquals(0, lag.lagMs);
    assertNull(lag.view);
    assertEquals(Collections.singletonList(1), catalog.linkBatches);
    assertTrue(catalog.headerBatches.isEmpty());
  }

  /**
   * Verifies that the walk reads one batch of links on each level, and that it finds global views,
   * tenant views, views of views and tenant views of tenant views.
   */
  @Test
  public void testViewWalkFindsAllLevelsAndTenants() throws Exception {
    FakeCatalog catalog = new FakeCatalog();
    byte[] view = catalog.view(ROOT, null, "S.V", 2000L);
    byte[] tenantView = catalog.view(ROOT, "T1", "S.TV", 8000L);
    catalog.view(view, null, "S.VV", 4000L);
    catalog.view(view, "T2", "TV2", 1000L);
    byte[] tenantViewOfView = catalog.view(tenantView, "T1", "S.TVV", 16000L);
    catalog.view(SchemaUtil.getTableKey(null, "S", "TX"), null, "S.OV", Long.MAX_VALUE);
    ViewCacheLag lag = catalog.walk();
    assertEquals(16000, lag.lagMs);
    assertArrayEquals(tenantViewOfView, lag.view);
    assertEquals("T1:S.TVV", viewName(lag.view));
    assertEquals("S.V", viewName(view));
    assertEquals(Arrays.asList(1, 2, 3), catalog.linkBatches);
    assertEquals(Arrays.asList(2, 3), catalog.headerBatches);
    assertTrue(catalog.resolvedViews.isEmpty());
  }

  /**
   * Verifies that a view whose header says NEVER, but whose exact frequency is finite, counts with
   * the exact frequency. The walk then goes on to the next level.
   */
  @Test
  public void testViewWalkResolvesStaleNever() throws Exception {
    FakeCatalog catalog = new FakeCatalog();
    byte[] stale = catalog.view(ROOT, "T1", "S.V", Long.MAX_VALUE);
    catalog.resolvesTo(stale, OptionalLong.of(3000));
    catalog.view(stale, "T1", "S.VV", 5000L);
    ViewCacheLag lag = catalog.walk();
    assertEquals(5000, lag.lagMs);
    assertEquals(1, catalog.resolvedViews.size());
    assertArrayEquals(stale, catalog.resolvedViews.get(0));
    // An exact frequency of zero uses the default
    catalog.resolvesTo(stale, OptionalLong.of(0));
    assertEquals(20000, catalog.walk(20000, 1000, 1000).lagMs);
  }

  /** Verifies that an exact NEVER stops the walk before it reads the next level. */
  @Test
  public void testViewWalkStopsAtExactNever() throws Exception {
    FakeCatalog catalog = new FakeCatalog();
    catalog.view(ROOT, null, "S.A", 1000L);
    byte[] never = catalog.view(ROOT, "T1", "S.V", Long.MAX_VALUE);
    catalog.resolvesTo(never, OptionalLong.of(Long.MAX_VALUE));
    catalog.view(never, "T1", "S.VV", 5000L);
    ViewCacheLag lag = catalog.walk();
    assertEquals(Long.MAX_VALUE, lag.lagMs);
    assertArrayEquals(never, lag.view);
    assertEquals(Collections.singletonList(1), catalog.linkBatches);
  }

  /** Verifies that a view whose header says NEVER, and that no longer resolves, counts as NEVER. */
  @Test
  public void testViewWalkKeepsNeverOfUnresolvedView() throws Exception {
    FakeCatalog catalog = new FakeCatalog();
    byte[] dropped = catalog.view(ROOT, null, "S.V", Long.MAX_VALUE);
    ViewCacheLag lag = catalog.walk();
    assertEquals(Long.MAX_VALUE, lag.lagMs);
    assertArrayEquals(dropped, lag.view);
    // A frequency of zero with the default NEVER also needs the exact frequency
    FakeCatalog defaults = new FakeCatalog();
    byte[] view = defaults.view(ROOT, null, "S.V", 0L);
    defaults.resolvesTo(view, OptionalLong.of(7000));
    assertEquals(7000, defaults.walk(Long.MAX_VALUE, 1000, 1000).lagMs);
  }

  /** Verifies that the walk follows the links of a child whose header row is gone. */
  @Test
  public void testViewWalkFollowsOrphanLinks() throws Exception {
    FakeCatalog catalog = new FakeCatalog();
    byte[] orphan = catalog.link(ROOT, null, "S.O");
    byte[] grandchild = catalog.view(orphan, null, "S.C", 6000L);
    ViewCacheLag lag = catalog.walk();
    assertEquals(6000, lag.lagMs);
    assertArrayEquals(grandchild, lag.view);
  }

  /** Verifies that a cycle of links ends the walk. */
  @Test
  public void testViewWalkStopsOnCycle() throws Exception {
    FakeCatalog catalog = new FakeCatalog();
    byte[] a = catalog.view(ROOT, null, "S.A", 1000L);
    byte[] b = catalog.view(a, null, "S.B", 2000L);
    catalog.link(b, null, "S.A");
    catalog.link(b, null, "S.T");
    assertEquals(2000, catalog.walk().lagMs);
    assertEquals(Arrays.asList(1, 1, 1), catalog.linkBatches);
  }

  /** Verifies the batches of link scans and header reads at the batch boundaries. */
  @Test
  public void testViewWalkBatches() throws Exception {
    FakeCatalog catalog = new FakeCatalog();
    for (int i = 0; i < 2500; i++) {
      catalog.view(ROOT, i % 2 == 0 ? null : "T" + i, "S.V" + i, i == 2400 ? 7000L : 1000L);
    }
    assertEquals(7000, catalog.walk().lagMs);
    assertEquals(Arrays.asList(1, 1000, 1000, 500), catalog.linkBatches);
    assertEquals(Arrays.asList(1000, 1000, 500), catalog.headerBatches);
    FakeCatalog small = new FakeCatalog();
    byte[] view = small.view(ROOT, null, "S.V", 1000L);
    small.view(ROOT, "T1", "S.W", 3000L);
    small.view(view, null, "S.VV", 2000L);
    assertEquals(3000, small.walk(0, 1, 1).lagMs);
    assertEquals(Arrays.asList(1, 1, 1, 1), small.linkBatches);
    assertEquals(Arrays.asList(1, 1, 1), small.headerBatches);
  }

  /** Verifies the header row key of the child of a child link row. */
  @Test
  public void testChildHeaderKey() {
    byte[] tenantParent = SchemaUtil.getTableKey("T1", "S", "TV");
    assertArrayEquals(SchemaUtil.getTableKey(null, "S", "V"),
      childHeaderKey(linkRow(ROOT, null, "S.V")));
    assertArrayEquals(SchemaUtil.getTableKey("T1", "S", "V"),
      childHeaderKey(linkRow(ROOT, "T1", "S.V")));
    assertArrayEquals(SchemaUtil.getTableKey("T1", null, "V"),
      childHeaderKey(linkRow(tenantParent, "T1", "V")));
    assertNull(childHeaderKey(ROOT));
  }

  /**
   * Verifies that the link range of S.T holds the links of S.T only. It does not hold the header
   * row of S.T, or the links of S.TX. A scan of a batch takes its bounds from the sorted ranges.
   */
  @Test
  public void testChildLinkRange() {
    RowRange range = childLinkRange(ROOT);
    assertTrue(range.contains(linkRow(ROOT, null, "S.V")));
    assertTrue(range.contains(linkRow(ROOT, "T1", "V")));
    assertFalse(range.contains(ROOT));
    assertFalse(range.contains(linkRow(SchemaUtil.getTableKey(null, "S", "TX"), null, "S.V")));
    assertFalse(range.contains(linkRow(SchemaUtil.getTableKey("T1", "S", "T"), null, "S.V")));

    byte[] first = SchemaUtil.getTableKey(null, "S", "A");
    byte[] last = SchemaUtil.getTableKey(null, "S", "Z");
    Scan scan = childLinkScan(Arrays.asList(last, ROOT, first));
    assertArrayEquals(childLinkRange(first).getStartRow(), scan.getStartRow());
    assertArrayEquals(childLinkRange(last).getStopRow(), scan.getStopRow());
    assertFalse(scan.isRaw());
    assertEquals(1000, scan.getCaching());
    FilterList filters = (FilterList) scan.getFilter();
    assertEquals(FilterList.Operator.MUST_PASS_ALL, filters.getOperator());
    assertEquals(2, filters.getFilters().size());
  }

  /** Verifies that the wait sleeps in steps and checks the claim after each step. */
  @Test
  public void testWaitChecksClaimAfterEachStep() throws Exception {
    AtomicInteger checks = new AtomicInteger();
    sleepAndCheckClaim(25, 10, checks::incrementAndGet, INDEX);
    assertEquals(3, checks.get());
    sleepAndCheckClaim(0, 10, checks::incrementAndGet, INDEX);
    assertEquals(3, checks.get());
  }

  /** Verifies that a lost claim stops the wait with the exception of the claim check. */
  @Test
  public void testWaitStopsOnLostClaim() {
    AtomicInteger checks = new AtomicInteger();
    SQLException lost = new SQLException("lost");
    try {
      sleepAndCheckClaim(1000, 1, () -> {
        checks.incrementAndGet();
        throw lost;
      }, INDEX);
      fail("The wait did not stop");
    } catch (SQLException e) {
      assertSame(lost, e);
    }
    assertEquals(1, checks.get());
  }

  /** Verifies that an interrupt stops the wait and sets the interrupt status again. */
  @Test
  public void testWaitStopsOnInterrupt() {
    AtomicInteger checks = new AtomicInteger();
    // The logger setup of the class fails on an interrupted thread, so the test loads the class
    // before the interrupt
    assertEquals(0, metadataWaitMs(0, 0));
    Thread.currentThread().interrupt();
    try {
      sleepAndCheckClaim(60000, 10, checks::incrementAndGet, INDEX);
      fail("The wait did not stop");
    } catch (SQLException e) {
      assertEquals(SQLExceptionCode.INTERRUPTED_EXCEPTION.getErrorCode(), e.getErrorCode());
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
    assertEquals(0, checks.get());
  }

  @Test
  public void testRebuildTaskData() throws Exception {
    JsonNode data = JacksonUtil.getObjectReader()
      .readTree(VectorIndexRebuilder.rebuildTaskData(false, "SKEW_RATIO_EXCEEDED: \"x\""));
    assertFalse(data.path("manual").asBoolean(true));
    assertEquals("SKEW_RATIO_EXCEEDED: 'x'", data.path("reason").asText());
  }

  /** Verifies that an index disabled during the rebuild stays disabled with no state update. */
  @Test
  public void testActivateLeavesIndexDisabledDuringRebuild() throws Exception {
    PhoenixConnection conn = connection(PIndexState.DISABLE, MutationCode.TABLE_ALREADY_EXISTS);
    VectorIndexRebuilder.activate(conn, INDEX);
    verify(conn.getQueryServices(), never()).updateIndexState(any(), any());
  }

  /**
   * Verifies that activation does not fail if the endpoint refuses the state change. The endpoint
   * refuses the change after a concurrent disable, and the rebuild result stays.
   */
  @Test
  public void testActivateToleratesRefusedTransition() throws Exception {
    PhoenixConnection conn =
      connection(PIndexState.BUILDING, MutationCode.UNALLOWED_TABLE_MUTATION);
    VectorIndexRebuilder.activate(conn, INDEX);
    verify(conn.getQueryServices()).updateIndexState(any(), isNull());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testActivatePromotesBuildingIndex() throws Exception {
    PhoenixConnection conn = connection(PIndexState.BUILDING, MutationCode.TABLE_ALREADY_EXISTS);
    VectorIndexRebuilder.activate(conn, INDEX);
    ArgumentCaptor<List<Mutation>> metadata = ArgumentCaptor.forClass(List.class);
    verify(conn.getQueryServices()).updateIndexState(metadata.capture(), isNull());
    Cell state = ((Put) metadata.getValue().get(0))
      .get(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES, PhoenixDatabaseMetaData.INDEX_STATE_BYTES)
      .get(0);
    assertEquals(PIndexState.ACTIVE,
      PIndexState.fromSerializedValue(CellUtil.cloneValue(state)[0]));
  }

  /**
   * Returns a mock connection that reads the index in {@code state} and returns {@code code} for
   * each index state update.
   */
  private static PhoenixConnection connection(PIndexState state, MutationCode code)
    throws Exception {
    PTable index = mock(PTable.class);
    when(index.getIndexState()).thenReturn(state);
    ConnectionQueryServices services = mock(ConnectionQueryServices.class);
    when(services.updateIndexState(any(), any()))
      .thenReturn(new MetaDataMutationResult(code, 0L, null));
    PhoenixConnection conn = mock(PhoenixConnection.class);
    when(conn.getTableNoCache(INDEX)).thenReturn(index);
    when(conn.getQueryServices()).thenReturn(services);
    return conn;
  }
}
