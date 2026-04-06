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

import static org.apache.phoenix.end2end.IndexToolIT.verifyIndexTable;
import static org.apache.phoenix.hbase.index.metrics.MetricsIndexCDCConsumerSource.CDC_BATCH_COUNT;
import static org.apache.phoenix.hbase.index.metrics.MetricsIndexCDCConsumerSource.CDC_BATCH_PROCESS_TIME;
import static org.apache.phoenix.hbase.index.metrics.MetricsIndexCDCConsumerSource.CDC_MUTATION_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.phoenix.hbase.index.metrics.MetricsIndexCDCConsumerSourceImpl;
import org.apache.phoenix.hbase.index.metrics.MetricsIndexerSourceFactory;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public abstract class ConcurrentMutationsExtendedIndexIT extends ParallelStatsDisabledIT {

  private static final Logger LOGGER =
    LoggerFactory.getLogger(ConcurrentMutationsExtendedIndexIT.class);

  private final boolean uncovered;
  private final boolean eventual;

  protected ConcurrentMutationsExtendedIndexIT(boolean uncovered, boolean eventual) {
    this.uncovered = uncovered;
    this.eventual = eventual;
  }

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    ConcurrentMutationsExtendedIT.doSetup();
  }

  // This test is heavy and it might exhaust jenkins resources
  @Test(timeout = 1800000)
  public void testConcurrentUpsertsWithTableSplits() throws Exception {
    Map<String, Long> metricsBefore = getCdcMetricValues();
    int nThreads = 12;
    final int batchSize = 100;
    final int nRows = 777;
    final int nIndexValues = 23;
    final int nSplits = 3;
    final String schemaName = "SchemaN_01-" + generateUniqueName();
    final String tableName = "Table.Name-00_001_" + generateUniqueName();
    final String indexName1 = "IndexTable.Name-00_001_" + generateUniqueName();
    final String indexName2 = "IndexTable.Name-00_002_" + generateUniqueName();
    final String indexName3 = "IndexTable.Name-00_003_" + generateUniqueName();
    final String indexName4 = "IndexTable.Name-00_004_" + generateUniqueName();
    final String indexName5 = "IndexTable.Name-00_005_" + generateUniqueName();
    Connection conn = DriverManager.getConnection(getUrl());
    conn.createStatement().execute("CREATE TABLE " + "\"" + schemaName + "\".\"" + tableName + "\""
      + "(k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, v1 INTEGER, v2 INTEGER, v3 INTEGER, v4 INTEGER,"
      + "CONSTRAINT pk PRIMARY KEY (k1,k2)) \"phoenix.index.cdc.mutations.compress.enabled\"=true");
    conn.createStatement()
      .execute("CREATE " + (uncovered ? "UNCOVERED " : " ") + "INDEX " + "\"" + indexName1 + "\""
        + " ON " + "\"" + schemaName + "\".\"" + tableName + "\"" + "(v1)"
        + (uncovered ? "" : " INCLUDE(v2, v3)")
        + (eventual ? " CONSISTENCY = EVENTUAL" : " CONSISTENCY = STRONG"));
    conn.createStatement()
      .execute("CREATE " + (uncovered ? "UNCOVERED " : " ") + "INDEX " + "\"" + indexName2 + "\""
        + " ON " + "\"" + schemaName + "\".\"" + tableName + "\"" + "(v2)"
        + (uncovered ? "" : " INCLUDE(v1, v4)") + (eventual ? " CONSISTENCY = EVENTUAL" : ""));
    conn.createStatement()
      .execute("CREATE " + (uncovered ? "UNCOVERED " : " ") + "INDEX " + "\"" + indexName3 + "\""
        + " ON " + "\"" + schemaName + "\".\"" + tableName + "\"" + "(v3)"
        + (uncovered ? "" : " INCLUDE(v1, v2)")
        + (eventual ? " CONSISTENCY = EVENTUAL" : " CONSISTENCY = STRONG"));
    conn.createStatement()
      .execute("CREATE " + (uncovered ? "UNCOVERED " : " ") + "INDEX " + "\"" + indexName4 + "\""
        + " ON " + "\"" + schemaName + "\".\"" + tableName + "\"" + "(v4)"
        + (uncovered ? "" : " INCLUDE(v2, v3)") + (eventual ? " CONSISTENCY = EVENTUAL" : ""));
    conn.createStatement()
      .execute("CREATE " + (uncovered ? "UNCOVERED " : " ") + "INDEX " + "\"" + indexName5 + "\""
        + " ON " + "\"" + schemaName + "\".\"" + tableName + "\"" + "(v1, v2)"
        + (uncovered ? "" : " INCLUDE(v3, v4)")
        + (eventual ? " CONSISTENCY = EVENTUAL" : " CONSISTENCY = STRONG"));

    final CountDownLatch doneSignal = new CountDownLatch(nThreads);
    Runnable[] runnables = new Runnable[nThreads];
    Thread.sleep(3000);
    long startTime = EnvironmentEdgeManager.currentTimeMillis();

    ConcurrentMutationsExtendedIT.buildRunnables(nThreads, runnables,
      "\"" + schemaName + "\".\"" + tableName + "\"", nRows, nIndexValues, batchSize, doneSignal,
      10000);
    Thread splitThread = new Thread(() -> TestUtil.splitTable(getUrl(),
      schemaName + "." + tableName, nSplits, eventual ? 8000 : 2000));
    splitThread.start();
    for (int i = 0; i < nThreads; i++) {
      if (i >= (nThreads - 4)) {
        Thread.sleep(12000);
      }
      Thread t = new Thread(runnables[i]);
      t.start();
    }
    assertTrue("Ran out of time", doneSignal.await(350, TimeUnit.SECONDS));
    splitThread.join(10000);
    LOGGER.info(
      "Total upsert time in ms : " + (EnvironmentEdgeManager.currentTimeMillis() - startTime));
    List<String> allIndexes =
      new ArrayList<>(Arrays.asList(indexName1, indexName2, indexName3, indexName4, indexName5));
    verifyRandomIndexes(allIndexes, schemaName, tableName, conn, nRows);
    assertCdcMetrics(metricsBefore);
  }

  private void verifyRandomIndexes(List<String> allIndexes, String schemaName, String tableName,
    Connection conn, int nRows) throws Exception {
    Collections.shuffle(allIndexes, ThreadLocalRandom.current());
    if (!eventual) {
      verifyIndexes(allIndexes, schemaName, tableName, conn, nRows, true);
      return;
    }
    boolean isComplete;
    int tries = 0;
    do {
      Thread.sleep(10000);
      try {
        verifyIndexes(allIndexes, schemaName, tableName, conn, nRows, false);
        isComplete = true;
      } catch (AssertionError e) {
        isComplete = false;
        LOGGER.error("Index verification failed. Tries: {}", tries, e);
      } finally {
        dumpTableRows(conn, "SYSTEM.CDC_STREAM");
        dumpTableRows(conn, "SYSTEM.IDX_CDC_TRACKER");
      }
      tries++;
    } while (!isComplete);
    LOGGER.info("Index verification complete. Tries: {}", tries);
  }

  private static void verifyIndexes(List<String> allIndexes, String schemaName, String tableName,
    Connection conn, int nRows, boolean dumpTables) throws Exception {
    LOGGER.info("Randomly selected indexes to verify: {}, {}", allIndexes.get(0),
      allIndexes.get(1));
    long actualRowCount = verifyIndexTable("\"" + schemaName + "\".\"" + tableName + "\"",
      "\"" + schemaName + "\".\"" + allIndexes.get(0) + "\"", conn, dumpTables);
    assertEquals(nRows, actualRowCount);
    actualRowCount = verifyIndexTable("\"" + schemaName + "\".\"" + tableName + "\"",
      "\"" + schemaName + "\".\"" + allIndexes.get(1) + "\"", conn, dumpTables);
    assertEquals(nRows, actualRowCount);
  }

  // This test is heavy and it might exhaust jenkins resources
  @Test(timeout = 1800000)
  @Ignore("too aggressive for jenkins builds")
  public void testConcurrentUpsertsWithTableSplitsMerges() throws Exception {
    Assume.assumeFalse(uncovered);
    Map<String, Long> metricsBefore = getCdcMetricValues();
    int nThreads = 13;
    final int batchSize = 100;
    final int nRows = 1451;
    final int nIndexValues = 28;
    final int nSplits = 3;
    final String schemaName = "SchemaN_01-" + generateUniqueName();
    final String tableName = "Table.Name-00_001_" + generateUniqueName();
    final String indexName1 = "IndexTable.Name-00_001_" + generateUniqueName();
    final String indexName2 = "IndexTable.Name-00_002_" + generateUniqueName();
    final String indexName3 = "IndexTable.Name-00_003_" + generateUniqueName();
    final String indexName4 = "IndexTable.Name-00_004_" + generateUniqueName();
    final String indexName5 = "IndexTable.Name-00_005_" + generateUniqueName();
    final String indexName6 = "IndexTable.Name-00_006_" + generateUniqueName();
    Connection conn = DriverManager.getConnection(getUrl());
    conn.createStatement().execute("CREATE TABLE " + "\"" + schemaName + "\".\"" + tableName + "\""
      + "(k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, v1 INTEGER, v2 INTEGER, v3 INTEGER, v4 INTEGER,"
      + "CONSTRAINT pk PRIMARY KEY (k1,k2)) \"phoenix.index.cdc.mutations.compress.enabled\"=true");
    conn.createStatement()
      .execute("CREATE INDEX " + "\"" + indexName1 + "\"" + " ON " + "\"" + schemaName + "\".\""
        + tableName + "\"" + "(v1)" + " INCLUDE(v2, v3)"
        + (eventual ? " CONSISTENCY = EVENTUAL" : ""));
    conn.createStatement()
      .execute("CREATE UNCOVERED INDEX " + "\"" + indexName2 + "\"" + " ON " + "\"" + schemaName
        + "\".\"" + tableName + "\"" + "(v2)" + (eventual ? " CONSISTENCY = EVENTUAL" : ""));
    conn.createStatement()
      .execute("CREATE INDEX " + "\"" + indexName3 + "\"" + " ON " + "\"" + schemaName + "\".\""
        + tableName + "\"" + "(v3)" + " INCLUDE(v1, v2)"
        + (eventual ? " CONSISTENCY = EVENTUAL" : ""));
    conn.createStatement()
      .execute("CREATE UNCOVERED INDEX " + "\"" + indexName4 + "\"" + " ON " + "\"" + schemaName
        + "\".\"" + tableName + "\"" + "(v4)" + (eventual ? " CONSISTENCY = EVENTUAL" : ""));
    conn.createStatement()
      .execute("CREATE INDEX " + "\"" + indexName5 + "\"" + " ON " + "\"" + schemaName + "\".\""
        + tableName + "\"" + "(v1, v2)" + " INCLUDE(v3, v4)"
        + (eventual ? " CONSISTENCY = EVENTUAL" : ""));
    conn.createStatement()
      .execute("CREATE UNCOVERED INDEX " + "\"" + indexName6 + "\"" + " ON " + "\"" + schemaName
        + "\".\"" + tableName + "\"" + "(v3, v1)" + (eventual ? " CONSISTENCY = EVENTUAL" : ""));

    final CountDownLatch doneSignal = new CountDownLatch(nThreads);
    Runnable[] runnables = new Runnable[nThreads];
    Thread.sleep(3000);
    long startTime = EnvironmentEdgeManager.currentTimeMillis();

    ConcurrentMutationsExtendedIT.buildRunnables(nThreads, runnables,
      "\"" + schemaName + "\".\"" + tableName + "\"", nRows, nIndexValues, batchSize, doneSignal,
      10000);
    Thread splitThread = new Thread(() -> TestUtil.splitTable(getUrl(),
      schemaName + "." + tableName, nSplits, eventual ? 3500 : 2000));
    splitThread.start();
    for (int i = 0; i < (nThreads - 5); i++) {
      if (i >= (nThreads - 8)) {
        Thread.sleep(9000);
      }
      Thread t = new Thread(runnables[i]);
      t.start();
    }
    splitThread.join(10000);
    Thread mergeThread =
      new Thread(() -> TestUtil.mergeTableRegions(getUrl(), schemaName + "." + tableName, 4, 1000));
    for (int i = nThreads - 5; i < nThreads; i++) {
      Thread t = new Thread(runnables[i]);
      t.start();
      if (i == nThreads - 3) {
        mergeThread.start();
      }
    }
    mergeThread.join(10000);
    assertTrue("Ran out of time", doneSignal.await(1500, TimeUnit.SECONDS));
    LOGGER.info(
      "Total upsert time in ms : " + (EnvironmentEdgeManager.currentTimeMillis() - startTime));
    List<String> allIndexes = new ArrayList<>(
      Arrays.asList(indexName1, indexName2, indexName3, indexName4, indexName5, indexName6));
    verifyRandomIndexes(allIndexes, schemaName, tableName, conn, nRows);
    assertCdcMetrics(metricsBefore);
  }

  @Test(timeout = 1800000)
  public void testConcurrentMutationsWithNonIndexedColumnUpdates() throws Exception {
    Assume.assumeFalse(uncovered);
    Map<String, Long> metricsBefore = getCdcMetricValues();
    final int nThreads = 3;
    final int batchSize = 100;
    final int nRows = 500;
    final int nIndexValues = 23;
    final String tableName = generateUniqueName();
    final String coveredIndexName = generateUniqueName();
    final String uncoveredIndexName = generateUniqueName();
    Connection conn = DriverManager.getConnection(getUrl());

    conn.createStatement()
      .execute("CREATE TABLE " + tableName + "(k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, "
        + "v1 INTEGER, v2 INTEGER, v3 INTEGER, v4 INTEGER, v5 INTEGER, v6 INTEGER, v7 INTEGER, "
        + "CONSTRAINT pk PRIMARY KEY (k1,k2))");

    conn.createStatement().execute("CREATE INDEX " + coveredIndexName + " ON " + tableName
      + "(v1) INCLUDE(v2)" + (eventual ? " CONSISTENCY = EVENTUAL" : ""));

    conn.createStatement().execute("CREATE UNCOVERED INDEX " + uncoveredIndexName + " ON "
      + tableName + "(v3)" + (eventual ? " CONSISTENCY = EVENTUAL" : ""));

    runMutationPhase(tableName, nThreads, 15000, batchSize, nRows, nIndexValues, false);
    Thread.sleep(5000);

    runMutationPhase(tableName, nThreads, 13334, batchSize, nRows, nIndexValues, true);
    Thread.sleep(5000);

    runMutationPhase(tableName, nThreads, 15000, batchSize, nRows, nIndexValues, false);
    Thread.sleep(5000);

    runMutationPhase(tableName, nThreads, 15000, batchSize, nRows, nIndexValues, true);
    Thread.sleep(20000);

    if (eventual) {
      boolean verified = false;
      for (int attempt = 0; attempt < 40 && !verified; attempt++) {
        Thread.sleep(10000);
        try {
          long coveredCount = verifyIndexTable(tableName, coveredIndexName, conn, false);
          assertEquals(nRows, coveredCount);
          long uncoveredCount = verifyIndexTable(tableName, uncoveredIndexName, conn, false);
          assertEquals(nRows, uncoveredCount);
          verified = true;
        } catch (AssertionError e) {
          LOGGER.info("Verification attempt {} failed: {}", attempt, e.getMessage());
          dumpTableRows(conn, "SYSTEM.CDC_STREAM");
          dumpTableRows(conn, "SYSTEM.IDX_CDC_TRACKER");
        }
      }
      assertTrue("Index verification failed after retries", verified);
    } else {
      long coveredCount = verifyIndexTable(tableName, coveredIndexName, conn);
      assertEquals(nRows, coveredCount);
      long uncoveredCount = verifyIndexTable(tableName, uncoveredIndexName, conn);
      assertEquals(nRows, uncoveredCount);
    }
    assertCdcMetrics(metricsBefore);
  }

  private void runMutationPhase(String tableName, int nThreads, int mutationsPerThread,
    int batchSize, int nRows, int nIndexValues, boolean includeIndexedColumns) throws Exception {
    CountDownLatch doneSignal = new CountDownLatch(nThreads);
    for (int t = 0; t < nThreads; t++) {
      new Thread(() -> {
        try {
          Connection c = DriverManager.getConnection(getUrl());
          ThreadLocalRandom rand = ThreadLocalRandom.current();
          for (int j = 0; j < mutationsPerThread; j++) {
            if (includeIndexedColumns) {
              c.createStatement()
                .execute("UPSERT INTO " + tableName + " VALUES (" + (j % nRows) + ", 0, "
                  + randVal(rand, nIndexValues) + ", " + randVal(rand) + ", "
                  + randVal(rand, nIndexValues) + ", " + randVal(rand) + ", " + randVal(rand) + ", "
                  + randVal(rand) + ", " + randVal(rand) + ")");
            } else {
              c.createStatement()
                .execute("UPSERT INTO " + tableName + " (k1, k2, v4, v5, v6, v7) VALUES ("
                  + (j % nRows) + ", 0, " + randVal(rand) + ", " + randVal(rand) + ", "
                  + randVal(rand) + ", " + randVal(rand) + ")");
            }
            if ((j % batchSize) == 0) {
              c.commit();
            }
          }
          c.commit();
        } catch (SQLException e) {
          LOGGER.warn("Mutation phase exception: {}", e.getMessage());
        } finally {
          doneSignal.countDown();
        }
      }).start();
    }
    assertTrue("Mutation phase timed out", doneSignal.await(350, TimeUnit.SECONDS));
  }

  private static String randVal(ThreadLocalRandom rand) {
    return rand.nextBoolean() ? "null" : Integer.toString(rand.nextInt());
  }

  private static String randVal(ThreadLocalRandom rand, int bound) {
    return rand.nextBoolean() ? "null" : Integer.toString(rand.nextInt() % bound);
  }

  private Map<String, Long> getCdcMetricValues() {
    MetricsIndexCDCConsumerSourceImpl source =
      (MetricsIndexCDCConsumerSourceImpl) MetricsIndexerSourceFactory.getInstance()
        .getIndexCDCConsumerSource();
    Map<String, Long> values = new HashMap<>();
    values.put(CDC_BATCH_COUNT, source.getMetricsRegistry().getCounter(CDC_BATCH_COUNT, 0).value());
    values.put(CDC_MUTATION_COUNT,
      source.getMetricsRegistry().getCounter(CDC_MUTATION_COUNT, 0).value());
    values.put(CDC_BATCH_PROCESS_TIME,
      source.getMetricsRegistry().getHistogram(CDC_BATCH_PROCESS_TIME).getCount());
    return values;
  }

  private void assertCdcMetrics(Map<String, Long> before) {
    Map<String, Long> after = getCdcMetricValues();
    long batchDelta = after.get(CDC_BATCH_COUNT) - before.get(CDC_BATCH_COUNT);
    long mutationDelta = after.get(CDC_MUTATION_COUNT) - before.get(CDC_MUTATION_COUNT);
    long histogramDelta = after.get(CDC_BATCH_PROCESS_TIME) - before.get(CDC_BATCH_PROCESS_TIME);
    LOGGER.info(
      "CDC metrics — before: {}, after: {}, delta — batchCount: {}, "
        + "mutationCount: {}, batchProcessTime samples: {}",
      before, after, batchDelta, mutationDelta, histogramDelta);
    if (eventual) {
      assertTrue("Expected cdcBatchCount to increase by at least 1, got " + batchDelta,
        batchDelta >= 1);
      assertTrue("Expected cdcMutationCount to increase by at least 1000, got " + mutationDelta,
        mutationDelta >= 1000);
      assertTrue("Expected cdcBatchProcessTime histogram samples to increase by at least 1, got "
        + histogramDelta, histogramDelta >= 1);
    } else {
      assertEquals("Expected no CDC batch processing for synchronous indexes", 0, batchDelta);
      assertEquals("Expected no CDC mutations for synchronous indexes", 0, mutationDelta);
      assertEquals("Expected no CDC batch process time samples for synchronous indexes", 0,
        histogramDelta);
    }
  }

  private void dumpTableRows(Connection conn, String tableName) throws SQLException {
    LOGGER.info("Dumping {} table:", tableName);
    try (ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName)) {
      ResultSetMetaData metaData = rs.getMetaData();
      int columnCount = metaData.getColumnCount();
      StringBuilder header = new StringBuilder();
      for (int i = 1; i <= columnCount; i++) {
        if (i > 1) {
          header.append("\t|\t");
        }
        header.append(metaData.getColumnName(i));
      }
      LOGGER.info("{}", header);
      LOGGER.info("_____________________________________________________________________________");
      while (rs.next()) {
        StringBuilder row = new StringBuilder();
        for (int i = 1; i <= columnCount; i++) {
          if (i > 1) {
            row.append("\t|\t");
          }
          row.append(rs.getString(i));
        }
        LOGGER.info("{}", row);
      }
    }
  }

}
