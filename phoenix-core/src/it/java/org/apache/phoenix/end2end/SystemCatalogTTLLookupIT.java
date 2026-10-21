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
package org.apache.phoenix.end2end;

import static org.apache.phoenix.query.BaseTest.generateUniqueName;
import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;
import static org.apache.phoenix.schema.LiteralTTLExpression.TTL_EXPRESSION_NOT_DEFINED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Verifies that resolving TTL from the view/table/index hierarchy while building a PTable reads
 * SYSTEM.CATALOG from the local region instead of issuing a client scan to it. Such a client scan
 * targets the region whose operation the outer getTable holds, and stalls the region's close until
 * the client scanner timeout.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class SystemCatalogTTLLookupIT {
  private static final TableName SYSTEM_CATALOG =
    TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME);
  private static final long MAX_CLOSE_MILLIS = 10000;
  private static final long WAIT_MILLIS = 120000;

  private static HBaseTestingUtility utility;
  private static String url;

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    setUpConfigForMiniCluster(conf);
    // Restore the default client retries and scanner timeout (the test config lowers the former and
    // raises the latter) so that a client scan of a closing region retries until the scanner
    // timeout, as it does in production.
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
      HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    conf.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,
      HConstants.DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD);
    utility = new HBaseTestingUtility(conf);
    utility.startMiniCluster();
    // Same test driver URL as the server side connections (e.g. for index creation) build.
    url = QueryUtil.getConnectionUrl(new Properties(), utility.getConfiguration())
      + PhoenixRuntime.PHOENIX_TEST_DRIVER_URL_PARAM;
    DriverManager.registerDriver(new PhoenixTestDriver());
  }

  @AfterClass
  public static synchronized void tearDown() throws Exception {
    if (utility != null) {
      utility.shutdownMiniCluster();
    }
  }

  /**
   * Counts client scans (RPC path only) of SYSTEM.CATALOG starting at any watched row key, and
   * optionally runs a hook on the first one.
   */
  public static class ScanCountingObserver implements RegionCoprocessor, RegionObserver {
    static final List<byte[]> WATCHED_KEYS = new CopyOnWriteArrayList<>();
    static final AtomicInteger SCANS = new AtomicInteger();
    static final AtomicReference<Runnable> ON_FIRST_SCAN = new AtomicReference<>();

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan)
      throws IOException {
      for (byte[] key : WATCHED_KEYS) {
        if (Bytes.equals(key, scan.getStartRow())) {
          SCANS.incrementAndGet();
          Runnable hook = ON_FIRST_SCAN.getAndSet(null);
          if (hook != null) {
            hook.run();
          }
        }
      }
    }
  }

  @Test
  public void testTableWithIndexesDoesNotScanSystemCatalogViaRpc() throws Exception {
    String tableName = generateUniqueName();
    String indexName1 = generateUniqueName();
    String indexName2 = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(url)) {
      createTableWithTTL(conn, tableName);
      conn.createStatement()
        .execute("CREATE INDEX " + indexName1 + " ON " + tableName + " (V1) INCLUDE (V2)");
      conn.createStatement().execute("CREATE INDEX " + indexName2 + " ON " + tableName + " (V2)");
    }
    watchTableKeys(tableName);

    try (Connection conn = DriverManager.getConnection(url)) {
      PhoenixConnection pconn = clearCachesAndResetCount(conn);
      // Building the table also builds each of its indexes, which resolve TTL from the table.
      PTable table = pconn.getTableNoCache(tableName);

      assertNotEquals(TTL_EXPRESSION_NOT_DEFINED, table.getTTLExpression());
      assertEquals(2, table.getIndexes().size());
      for (PTable index : table.getIndexes()) {
        assertEquals(table.getTTLExpression(), index.getTTLExpression());
      }
      assertNoSystemCatalogClientScans();
    }
  }

  @Test
  public void testViewHierarchyDoesNotScanSystemCatalogViaRpc() throws Exception {
    String tableName = generateUniqueName();
    String parentViewName = generateUniqueName();
    String childViewName = generateUniqueName();
    String viewIndexName = generateUniqueName();
    createViewHierarchy(tableName, parentViewName, childViewName);
    try (Connection conn = DriverManager.getConnection(url)) {
      conn.createStatement()
        .execute("CREATE INDEX " + viewIndexName + " ON " + childViewName + " (V1)");
    }
    watchTableKeys(tableName, parentViewName, childViewName);

    try (Connection conn = DriverManager.getConnection(url)) {
      PhoenixConnection pconn = clearCachesAndResetCount(conn);
      PTable table = pconn.getTableNoCache(tableName);
      // Building the view also builds its index, which resolves TTL from the view hierarchy.
      PTable childView = pconn.getTableNoCache(childViewName);

      assertNotEquals(TTL_EXPRESSION_NOT_DEFINED, table.getTTLExpression());
      assertEquals(table.getTTLExpression(), childView.getTTLExpression());
      assertEquals(1, childView.getIndexes().size());
      assertEquals(table.getTTLExpression(), childView.getIndexes().get(0).getTTLExpression());
      assertNoSystemCatalogClientScans();
    }
  }

  /**
   * Closes SYSTEM.CATALOG while a getTable is resolving TTL. If the TTL lookup issues a client
   * scan, the close is started from that scan's preScannerOpen hook, so the scan then finds the
   * region closing and retries for the scanner timeout while the outer getTable holds the region
   * operation. Otherwise the region is closed once the getTable completes. Either way the close
   * must not stall.
   */
  @Test
  public void testSystemCatalogCloseIsNotStalledByTTLLookup() throws Exception {
    String tableName = generateUniqueName();
    String parentViewName = generateUniqueName();
    String childViewName = generateUniqueName();
    createViewHierarchy(tableName, parentViewName, childViewName);
    watchTableKeys(tableName, parentViewName, childViewName);

    HRegion sysCatRegion = utility.getHBaseCluster().getRegions(SYSTEM_CATALOG).get(0);
    byte[] regionName = sysCatRegion.getRegionInfo().getRegionName();
    ExecutorService executor = Executors.newFixedThreadPool(2);
    AtomicReference<Future<Long>> close = new AtomicReference<>();
    AtomicBoolean closeStarted = new AtomicBoolean();
    AtomicBoolean closingSeen = new AtomicBoolean();
    try (Admin admin = utility.getAdmin()) {
      Throwable failure = null;
      try {
        ScanCountingObserver.ON_FIRST_SCAN.set(() -> {
          close.set(executor.submit(() -> timedUnassign(admin, regionName)));
          closingSeen.set(waitUntilClosing(sysCatRegion));
          closeStarted.set(true);
        });
        Future<?> lookup = executor.submit(() -> {
          try (Connection conn = DriverManager.getConnection(url)) {
            clearCachesAndResetCount(conn).getTableNoCache(childViewName);
          }
          return null;
        });
        // Wait for either the hook to start the close, or the lookup to complete without it.
        long deadline = System.currentTimeMillis() + WAIT_MILLIS;
        while (!closeStarted.get() && !lookup.isDone()) {
          assertTrue("TTL lookup did not complete", System.currentTimeMillis() < deadline);
          Thread.sleep(10);
        }
        ScanCountingObserver.ON_FIRST_SCAN.set(null);
        if (closeStarted.get()) {
          assertTrue("SYSTEM.CATALOG did not start closing", closingSeen.get());
        } else {
          lookup.get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
          close.set(executor.submit(() -> timedUnassign(admin, regionName)));
        }
        long closeMillis = close.get().get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
        assertTrue("SYSTEM.CATALOG close took " + closeMillis + " ms",
          closeMillis < MAX_CLOSE_MILLIS);
      } catch (Throwable t) {
        failure = t;
        throw t;
      } finally {
        ScanCountingObserver.ON_FIRST_SCAN.set(null);
        executor.shutdownNow();
        executor.awaitTermination(WAIT_MILLIS, TimeUnit.MILLISECONDS);
        restoreSystemCatalog(admin, regionName, close.get(), failure);
      }
    }
  }

  /**
   * Brings SYSTEM.CATALOG back for the other tests if it was closed, without masking the failure of
   * the test, if any.
   */
  private static void restoreSystemCatalog(Admin admin, byte[] regionName, Future<Long> close,
    Throwable failure) throws Exception {
    if (close == null) {
      return;
    }
    try {
      try {
        close.get(WAIT_MILLIS, TimeUnit.MILLISECONDS);
      } catch (ExecutionException | TimeoutException e) {
        // The close failed or is still running; assign below only if the region is offline.
      }
      if (utility.getHBaseCluster().getRegions(SYSTEM_CATALOG).isEmpty()) {
        admin.assign(regionName);
      }
      utility.waitUntilAllRegionsAssigned(SYSTEM_CATALOG);
    } catch (Exception e) {
      if (failure == null) {
        throw e;
      }
      failure.addSuppressed(e);
    }
  }

  private static long timedUnassign(Admin admin, byte[] regionName) throws IOException {
    long start = System.currentTimeMillis();
    admin.unassign(regionName);
    return System.currentTimeMillis() - start;
  }

  private static boolean waitUntilClosing(HRegion region) {
    long deadline = System.currentTimeMillis() + MAX_CLOSE_MILLIS;
    while (!region.isClosing() && !region.isClosed()) {
      if (System.currentTimeMillis() >= deadline) {
        return false;
      }
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    return true;
  }

  private static void createTableWithTTL(Connection conn, String tableName) throws Exception {
    conn.createStatement().execute("CREATE TABLE " + tableName
      + " (ID VARCHAR NOT NULL PRIMARY KEY, V1 VARCHAR, V2 VARCHAR) TTL=86400");
    TestUtil.addCoprocessor(conn, PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME,
      ScanCountingObserver.class);
  }

  private static void createViewHierarchy(String tableName, String parentViewName,
    String childViewName) throws Exception {
    try (Connection conn = DriverManager.getConnection(url)) {
      createTableWithTTL(conn, tableName);
      conn.createStatement()
        .execute("CREATE VIEW " + parentViewName + " AS SELECT * FROM " + tableName);
      conn.createStatement()
        .execute("CREATE VIEW " + childViewName + " AS SELECT * FROM " + parentViewName);
    }
  }

  private static void watchTableKeys(String... names) {
    ScanCountingObserver.WATCHED_KEYS.clear();
    for (String name : names) {
      ScanCountingObserver.WATCHED_KEYS.add(SchemaUtil.getTableKey(null, null, name));
    }
  }

  private static PhoenixConnection clearCachesAndResetCount(Connection conn) throws Exception {
    PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
    // Clear the client and server metadata caches so that getTable rebuilds the PTables.
    pconn.getQueryServices().clearCache();
    ScanCountingObserver.SCANS.set(0);
    return pconn;
  }

  private static void assertNoSystemCatalogClientScans() throws IOException {
    assertEquals("TTL lookup must not issue client scans to SYSTEM.CATALOG", 0,
      ScanCountingObserver.SCANS.get());
    // Positive control: a client scan of a watched key is counted.
    try (Table sysCat = utility.getConnection().getTable(SYSTEM_CATALOG); ResultScanner scanner =
      sysCat.getScanner(new Scan().withStartRow(ScanCountingObserver.WATCHED_KEYS.get(0)))) {
      scanner.next();
    }
    assertEquals(1, ScanCountingObserver.SCANS.get());
  }
}
