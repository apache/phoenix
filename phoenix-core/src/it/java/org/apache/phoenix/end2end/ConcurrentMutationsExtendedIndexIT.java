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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assume;
import org.junit.BeforeClass;
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

  @Test(timeout = 2000000)
  public void testConcurrentUpsertsWithTableSplits() throws Exception {
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
      + "CONSTRAINT pk PRIMARY KEY (k1,k2))");
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

  @Test(timeout = 5000000)
  public void testConcurrentUpsertsWithTableSplitsMerges() throws Exception {
    Assume.assumeFalse(uncovered);
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
      + "CONSTRAINT pk PRIMARY KEY (k1,k2))");
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
