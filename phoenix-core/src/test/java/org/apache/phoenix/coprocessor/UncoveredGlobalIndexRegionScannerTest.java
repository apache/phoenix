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

import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.PHYSICAL_DATA_TABLE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.index.IndexMaintainer;
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
  public void testRegionLookupFailureClosesReplacementAndTable() throws Exception {
    IOException failure = new IOException("region lookup failed");
    when(locator.getEndKeys()).thenThrow(failure);
    assertConstructionFails(original, failure);
    verify(replacement).close();
    verify(table).close();
  }

  @Test
  public void testCleanupFailuresPreserveInitializationFailure() throws Exception {
    IOException failure = new IOException("region lookup failed");
    IOException scannerCloseFailure = new IOException("scanner close failed");
    IOException tableCloseFailure = new IOException("table close failed");
    when(locator.getEndKeys()).thenThrow(failure);
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
    IOException failure = new IOException("region lookup failed");
    when(locator.getEndKeys()).thenThrow(failure);
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

  private void assertConstructionFails(RegionScanner scanner, IOException expected)
    throws Exception {
    try {
      construct(scanner);
      fail("Expected scanner initialization to fail");
    } catch (IOException actual) {
      assertSame(expected, actual);
    }
  }

  private void construct(RegionScanner scanner) throws IOException {
    new UncoveredGlobalIndexRegionScanner(scanner, mock(Region.class), scan, env, new Scan(), null,
      maintainer, null, new ImmutableBytesWritable(), 60_000, -1, false);
  }
}
