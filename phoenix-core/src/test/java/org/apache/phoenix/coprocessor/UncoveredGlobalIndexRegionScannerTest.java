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
package org.apache.phoenix.coprocessor;

import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.EMPTY_COLUMN_FAMILY_NAME;
import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.EMPTY_COLUMN_QUALIFIER_NAME;
import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.PHYSICAL_DATA_TABLE_NAME;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.BaseTerminalExpression;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.ServerUtil.ConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class UncoveredGlobalIndexRegionScannerTest {
  private final RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);
  private final Connection connection = mock(Connection.class);
  private final Table table = mock(Table.class);
  private final RegionLocator locator = mock(RegionLocator.class);
  private final DelegateRegionScanner original = mock(DelegateRegionScanner.class);
  private final RegionScanner replacement = mock(RegionScanner.class);
  private final IndexMaintainer maintainer = mock(IndexMaintainer.class);
  private final Scan scan = new Scan().withStartRow(Bytes.toBytes("start"));
  private final Region region = mock(Region.class);
  private final Set<String> invalidRows = new HashSet<>();
  private static final long TS = 1000L;
  private static final long NOW = 10_000L;
  private static final long PAGE_SIZE_MS = 60_000L;

  @Before
  public void setUp() throws Exception {
    ConnectionFactory.shutdown();
    when(env.getConfiguration()).thenReturn(new Configuration(false));
    when(env.createConnection(any(Configuration.class))).thenReturn(connection);
    when(connection.getTable(any(TableName.class))).thenReturn(table);
    when(connection.getRegionLocator(any(TableName.class))).thenReturn(locator);
    when(table.getName()).thenReturn(TableName.valueOf("DATA"));
    when(original.getNewRegionScanner(any(Scan.class))).thenReturn(replacement);
    scan.setAttribute(PHYSICAL_DATA_TABLE_NAME, Bytes.toBytes("DATA"));
    scan.setFilter(new PageFilter(1));
  }

  @After
  public void tearDown() {
    ConnectionFactory.shutdown();
    EnvironmentEdgeManager.reset();
  }

  @Test
  public void testTableOpenFailureClosesReplacement() throws Exception {
    IOException failure = new IOException("table open failed");
    when(connection.getTable(any(TableName.class))).thenThrow(failure);
    assertConstructionFails(original, failure);
    verify(original).close();
    verify(replacement).close();
    verify(table, never()).close();
  }

  @Test
  public void testFailureAfterTableOpenClosesReplacementAndTable() throws Exception {
    // The constructor does not look up the data table regions. The empty column lookup is its
    // last step after the table opens.
    RuntimeException failure = new IllegalStateException("empty column lookup failed");
    when(maintainer.getDataEmptyKeyValueCF()).thenThrow(failure);
    assertConstructionFails(original, failure);
    verify(replacement).close();
    verify(table).close();
  }

  @Test
  public void testCleanupFailuresPreserveInitializationFailure() throws Exception {
    RuntimeException failure = new IllegalStateException("empty column lookup failed");
    IOException scannerCloseFailure = new IOException("scanner close failed");
    IOException tableCloseFailure = new IOException("table close failed");
    when(maintainer.getDataEmptyKeyValueCF()).thenThrow(failure);
    doThrow(scannerCloseFailure).when(replacement).close();
    doThrow(tableCloseFailure).when(table).close();
    assertConstructionFails(original, failure);
    assertEquals(2, failure.getSuppressed().length);
    assertSame(scannerCloseFailure, failure.getSuppressed()[0]);
    assertSame(tableCloseFailure, failure.getSuppressed()[1]);
    verify(table).close();
  }

  @Test
  public void testSuperclassFailureDoesNotReopenScanner() throws Exception {
    RuntimeException failure = new IllegalArgumentException("configuration failed");
    when(env.getConfiguration()).thenThrow(failure);
    try {
      construct(original);
      fail("Expected superclass initialization to fail");
    } catch (RuntimeException actual) {
      assertSame(failure, actual);
    }
    verify(original, never()).getNewRegionScanner(any(Scan.class));
    verify(original, never()).close();
  }

  @Test
  public void testFailureWithoutPageFilterLeavesOriginalOwnedByCaller() throws Exception {
    scan.setFilter(null);
    RuntimeException failure = new IllegalStateException("empty column lookup failed");
    when(maintainer.getDataEmptyKeyValueCF()).thenThrow(failure);
    assertConstructionFails(original, failure);
    verify(original, never()).close();
    verify(original, never()).getNewRegionScanner(any(Scan.class));
    verify(table).close();
  }

  @Test
  public void testUnsupportedScannerIsNotRetriedAndLeftOwnedByCaller() throws Exception {
    RegionScanner unsupported = mock(RegionScanner.class);
    try {
      construct(unsupported);
      fail("Expected incompatible scanner to fail");
    } catch (DoNotRetryIOException failure) {
      assertTrue(failure.getCause() instanceof ClassCastException);
    }
    verify(unsupported, never()).close();
  }

  @Test
  public void testSkippedRowsAreConsumedInOneCall() throws Exception {
    scan.setFilter(null);
    Map<String, Result> data = dataRows(TS, "i1", "i3", "i4", "i5");
    invalidRows.add("i3");
    RowFilter filter = new RowFilter("i4");
    UncoveredGlobalIndexRegionScanner scanner =
      newScanner(indexScanner("i1", "i2", "i3", "i4", "i5"), data, filter, false);
    List<Cell> result = new ArrayList<>();
    // The first call skips a rejected row, a row without a data row and an invalid row.
    assertTrue(scanner.next(result));
    assertEquals("i4", rowOf(result));
    assertEquals(Arrays.asList("i1", "i4"), filter.evaluated);
    result.clear();
    // A rejected row at the end of the page gives an empty result.
    assertTrue(scanner.next(result));
    assertTrue(result.isEmpty());
    assertFalse(scanner.next(result));
    assertTrue(result.isEmpty());
    verify(region, never()).delete(any(Delete.class));
  }

  @Test
  public void testPagingStopReturnsDummyAtLastSkippedRow() throws Exception {
    scan.setFilter(null);
    ManualEnvironmentEdge clock = new ManualEnvironmentEdge();
    clock.setValue(NOW);
    EnvironmentEdgeManager.injectEdge(clock);
    RowFilter filter = new RowFilter();
    filter.clock = clock;
    UncoveredGlobalIndexRegionScanner scanner =
      newScanner(indexScanner("i1", "i2", "i3"), dataRows(TS, "i1", "i2", "i3"), filter, false);
    List<Cell> result = new ArrayList<>();
    assertTrue(scanner.next(result));
    assertTrue(ScanUtil.isDummy(result));
    assertEquals("i1", rowOf(result));
    result.clear();
    assertTrue(scanner.next(result));
    assertTrue(ScanUtil.isDummy(result));
    assertEquals("i2", rowOf(result));
    result.clear();
    assertTrue(scanner.next(result));
    assertTrue(result.isEmpty());
    assertEquals(Arrays.asList("i1", "i2", "i3"), filter.evaluated);
  }

  @Test
  public void testDistinctScanReturnsEachSkippedRow() throws Exception {
    scan.setFilter(null);
    RowFilter filter = new RowFilter("i2");
    UncoveredGlobalIndexRegionScanner scanner =
      newScanner(indexScanner("i1", "i2"), dataRows(TS, "i1", "i2"), filter, true);
    List<Cell> result = new ArrayList<>();
    // An empty result lets the caller reset its distinct filter after each skipped row.
    assertTrue(scanner.next(result));
    assertTrue(result.isEmpty());
    assertTrue(scanner.next(result));
    assertEquals("i2", rowOf(result));
  }

  @Test
  public void testRepairRunsBeforeReject() throws Exception {
    scan.setFilter(null);
    ManualEnvironmentEdge clock = new ManualEnvironmentEdge();
    clock.setValue(NOW);
    EnvironmentEdgeManager.injectEdge(clock);
    RowFilter filter = new RowFilter();
    // The data row is newer than the index row, so verification repairs the index row.
    UncoveredGlobalIndexRegionScanner scanner =
      newScanner(indexScanner("i1"), dataRows(TS + 1, "i1"), filter, false);
    List<Cell> result = new ArrayList<>();
    assertTrue(scanner.next(result));
    assertTrue(result.isEmpty());
    assertEquals(Collections.singletonList("i1"), filter.evaluated);
    verify(region, times(1)).batchMutate(any(Mutation[].class));
    verify(region, never()).delete(any(Delete.class));
  }

  @Test
  public void testRejectedRowsDoNotCountTowardPageLimit() throws Exception {
    // The scan keeps its PageFilter of one row, so the scanner takes the limit.
    RowFilter filter = new RowFilter("i2");
    stubIndexRows(replacement, "i1", "i2", "i3");
    UncoveredGlobalIndexRegionScanner scanner =
      newScanner(original, dataRows(TS, "i1", "i2", "i3"), filter, false);
    List<Cell> result = new ArrayList<>();
    assertTrue(scanner.next(result));
    assertTrue(result.isEmpty());
    assertFalse(scanner.next(result));
    assertEquals("i2", rowOf(result));
  }

  private UncoveredGlobalIndexRegionScanner newScanner(RegionScanner indexScanner,
    Map<String, Result> data, RowFilter filter, boolean isDistinct) throws IOException {
    scan.setAttribute(EMPTY_COLUMN_FAMILY_NAME, QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES);
    scan.setAttribute(EMPTY_COLUMN_QUALIFIER_NAME, QueryConstants.EMPTY_COLUMN_BYTES);
    when(maintainer.isUncovered()).thenReturn(true);
    when(maintainer.buildDataRowKey(any(ImmutableBytesWritable.class), any()))
      .thenAnswer(invocation -> ((ImmutableBytesWritable) invocation.getArgument(0)).copyBytes());
    when(maintainer.checkIndexRow(any(byte[].class), any(Put.class)))
      .thenAnswer(invocation -> !invalidRows.contains(Bytes.toString(invocation.getArgument(0))));
    UncoveredGlobalIndexRegionScanner scanner =
      new UncoveredGlobalIndexRegionScanner(indexScanner, region, scan, env, new Scan(), null,
        maintainer, null, new ImmutableBytesWritable(), PAGE_SIZE_MS, -1, isDistinct) {
        @Override
        protected void scanDataTableRows(long startTime) {
          for (byte[] dataRowKey : indexToDataRowKeyMap.values()) {
            Result dataRow = data.get(Bytes.toString(dataRowKey));
            if (dataRow != null) {
              dataRows.put(new ImmutableBytesPtr(dataRowKey), dataRow);
            }
          }
          state = State.READY;
        }
      };
    scanner.setDataRowFilter(new UncoveredIndexDataRowFilter(filter));
    return scanner;
  }

  private RegionScanner indexScanner(String... keys) throws IOException {
    RegionScanner indexScanner = mock(RegionScanner.class);
    stubIndexRows(indexScanner, keys);
    return indexScanner;
  }

  private static void stubIndexRows(RegionScanner indexScanner, String... keys) throws IOException {
    Iterator<String> rows = Arrays.asList(keys).iterator();
    when(indexScanner.nextRaw(anyList())).thenAnswer(invocation -> {
      if (rows.hasNext()) {
        List<Cell> row = invocation.getArgument(0);
        row.add(new KeyValue(Bytes.toBytes(rows.next()), QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
          QueryConstants.EMPTY_COLUMN_BYTES, TS, QueryConstants.VERIFIED_BYTES));
      }
      return rows.hasNext();
    });
  }

  private static Map<String, Result> dataRows(long timestamp, String... keys) {
    Map<String, Result> rows = new HashMap<>();
    for (String key : keys) {
      rows.put(key,
        Result.create(Collections.singletonList(new KeyValue(Bytes.toBytes(key),
          QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES, timestamp,
          QueryConstants.EMPTY_COLUMN_VALUE_BYTES))));
    }
    return rows;
  }

  private static String rowOf(List<Cell> result) {
    assertFalse(result.isEmpty());
    byte[] row = CellUtil.cloneRow(result.get(0));
    for (Cell cell : result) {
      assertArrayEquals(row, CellUtil.cloneRow(cell));
    }
    return Bytes.toString(row);
  }

  /**
   * A data row filter that accepts only the given row keys. It records each evaluation. It can also
   * move the clock past the page time on each evaluation.
   */
  private static final class RowFilter extends BaseTerminalExpression {
    private final Set<String> accepted;
    private final List<String> evaluated = new ArrayList<>();
    private ManualEnvironmentEdge clock;

    private RowFilter(String... accepted) {
      this.accepted = new HashSet<>(Arrays.asList(accepted));
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
      tuple.getKey(ptr);
      String row = Bytes.toString(ptr.copyBytes());
      evaluated.add(row);
      if (clock != null) {
        clock.incrementValue(PAGE_SIZE_MS + 1);
      }
      ptr.set(accepted.contains(row) ? PDataType.TRUE_BYTES : PDataType.FALSE_BYTES);
      return true;
    }

    @Override
    public PDataType getDataType() {
      return PBoolean.INSTANCE;
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
      return visitor.defaultReturn(this, Collections.emptyList());
    }
  }

  private void assertConstructionFails(RegionScanner scanner, Exception expected) throws Exception {
    try {
      construct(scanner);
      fail("Expected scanner initialization to fail");
    } catch (IOException | RuntimeException actual) {
      assertSame(expected, actual);
    }
  }

  private void construct(RegionScanner scanner) throws IOException {
    new UncoveredGlobalIndexRegionScanner(scanner, mock(Region.class), scan, env, new Scan(), null,
      maintainer, null, new ImmutableBytesWritable(), 60_000, -1, false);
  }
}
