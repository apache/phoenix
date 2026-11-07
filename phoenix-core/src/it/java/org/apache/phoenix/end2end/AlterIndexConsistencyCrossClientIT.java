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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.types.IndexConsistency;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests that ALTER INDEX ... CONSISTENCY reaches the metadata cache of every client. Each client
 * uses its own connection query services, so each client has its own metadata cache.
 */
@Category(ParallelStatsDisabledTest.class)
public class AlterIndexConsistencyCrossClientIT extends ParallelStatsDisabledIT {

  private static final Logger LOGGER =
    LoggerFactory.getLogger(AlterIndexConsistencyCrossClientIT.class);

  private static final IndexConsistency S = IndexConsistency.STRONG;
  private static final IndexConsistency E = IndexConsistency.EVENTUAL;

  private static final long INDEX_CATCH_UP_TIMEOUT_MS = 120000;

  private static Connection newClient(Properties props, String principal) throws Exception {
    return DriverManager.getConnection(QueryUtil.getConnectionUrl(props, config, principal), props);
  }

  private static void upsert(Connection conn, String table, int id) throws Exception {
    conn.createStatement()
      .execute("UPSERT INTO " + table + " VALUES (" + id + ", 'n" + id + "', 'c" + id + "')");
    conn.commit();
  }

  private static void pointLookup(Connection conn, String table) throws Exception {
    try (ResultSet rs =
      conn.createStatement().executeQuery("SELECT * FROM " + table + " WHERE id = 1")) {
      while (rs.next()) {
      }
    }
  }

  private static void fullScan(Connection conn, String table) throws Exception {
    try (ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + table)) {
      while (rs.next()) {
      }
    }
  }

  private static long count(Connection conn, String sql) throws Exception {
    try (ResultSet rs = conn.createStatement().executeQuery(sql)) {
      rs.next();
      return rs.getLong(1);
    }
  }

  /** Returns the consistency that SYSTEM.CATALOG records for the index. */
  private static IndexConsistency catalogConsistency(Connection conn, String schema, String index)
    throws Exception {
    try (ResultSet rs = conn.createStatement()
      .executeQuery("SELECT INDEX_CONSISTENCY FROM SYSTEM.CATALOG WHERE TENANT_ID IS NULL"
        + " AND TABLE_SCHEM = '" + schema + "' AND TABLE_NAME = '" + index + "'"
        + " AND COLUMN_NAME IS NULL AND COLUMN_FAMILY IS NULL")) {
      if (!rs.next()) {
        fail("No header row for " + index);
      }
      String code = rs.getString(1);
      return code == null ? S : IndexConsistency.fromCode(code);
    }
  }

  /**
   * Returns the consistency of the index in the cached parent, and the consistency of the index
   * maintainer that the client sends with its writes. A null consistency means STRONG.
   */
  private static String clientView(Connection conn, String fullTable, String index)
    throws Exception {
    PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
    PTable parent = pconn.getTableRef(new PTableKey(pconn.getTenantId(), fullTable)).getTable();
    for (PTable idx : parent.getIndexes()) {
      if (idx.getTableName().getString().equals(index)) {
        IndexConsistency cached = idx.getIndexConsistency();
        IndexConsistency maintained = idx.getIndexMaintainer(parent, pconn).getIndexConsistency();
        return (cached == null ? S : cached) + "/" + (maintained == null ? S : maintained);
      }
    }
    return "MISSING/MISSING";
  }

  private static void check(List<String> failures, String step, String client, String phase,
    Connection conn, String fullTable, String index, IndexConsistency expected) throws Exception {
    String view = clientView(conn, fullTable, index);
    String expectedView = expected + "/" + expected;
    if (!view.equals(expectedView)) {
      failures
        .add(step + " " + client + " " + phase + ": expected " + expectedView + " got " + view);
    }
  }

  /**
   * The first client runs the DDL. The second client has the parent in its cache. After each ALTER,
   * the first client, the second client and a new client each write and do a point lookup, and then
   * do a full scan. After each operation, each client must have the new consistency in the cached
   * index and in the index maintainer.
   */
  private void runRoundTrip(String initialSuffix, IndexConsistency initial,
    IndexConsistency[] steps) throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String schema = generateUniqueName();
    String table = generateUniqueName();
    String index = generateUniqueName();
    String fullTable = schema + "." + table;
    String tag = generateUniqueName();
    List<String> failures = new ArrayList<>();
    try (Connection conn1 = newClient(props, tag + "c1");
      Connection conn2 = newClient(props, tag + "c2")) {
      conn1.createStatement().execute(
        "CREATE TABLE " + fullTable + " (id INTEGER PRIMARY KEY, name VARCHAR, city VARCHAR)");
      conn1.createStatement().execute(
        "CREATE INDEX " + index + " ON " + fullTable + " (name) INCLUDE (city)" + initialSuffix);
      int id = 1;
      upsert(conn1, fullTable, id++);
      upsert(conn2, fullTable, id++);
      fullScan(conn2, fullTable);
      check(failures, "CREATE " + initial, "conn1", "initial", conn1, fullTable, index, initial);
      check(failures, "CREATE " + initial, "conn2", "initial", conn2, fullTable, index, initial);

      IndexConsistency prev = initial;
      for (int s = 0; s < steps.length; s++) {
        IndexConsistency target = steps[s];
        String step = "step" + (s + 1) + " " + prev + "->" + target;
        conn1.createStatement()
          .execute("ALTER INDEX " + index + " ON " + fullTable + " CONSISTENCY=" + target);
        assertEquals("SYSTEM.CATALOG after " + step, target,
          catalogConsistency(conn1, schema, index));

        try (Connection fresh = newClient(props, tag + "fresh" + s)) {
          Connection[] conns = { conn1, conn2, fresh };
          String[] names = { "conn1", "conn2", "fresh" };
          for (int c = 0; c < conns.length; c++) {
            upsert(conns[c], fullTable, id++);
            pointLookup(conns[c], fullTable);
            check(failures, step, names[c], "write+point", conns[c], fullTable, index, target);
          }
          for (int c = 0; c < conns.length; c++) {
            fullScan(conns[c], fullTable);
            check(failures, step, names[c], "select*", conns[c], fullTable, index, target);
          }
        }
        prev = target;
      }
    }
    if (!failures.isEmpty()) {
      fail(failures.size() + " stale client view(s):\n  " + String.join("\n  ", failures));
    }
  }

  /** Returns the LAST_DDL_TIMESTAMP that SYSTEM.CATALOG records for the table or index. */
  private static Long lastDDLTimestamp(Connection conn, String schema, String name)
    throws Exception {
    try (ResultSet rs = conn.createStatement()
      .executeQuery("SELECT LAST_DDL_TIMESTAMP FROM SYSTEM.CATALOG WHERE TENANT_ID IS NULL"
        + " AND TABLE_SCHEM = '" + schema + "' AND TABLE_NAME = '" + name + "'"
        + " AND COLUMN_NAME IS NULL AND COLUMN_FAMILY IS NULL")) {
      if (!rs.next()) {
        return null;
      }
      long ts = rs.getLong(1);
      return rs.wasNull() ? null : ts;
    }
  }

  /**
   * ALTER from STRONG to EVENTUAL must create the CDC index before it writes EVENTUAL. Otherwise a
   * writer that sees EVENTUAL can skip index maintenance before CDC records its writes.
   */
  @Test
  public void testCDCIndexCreatedBeforeEventual() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String schema = generateUniqueName();
    String table = generateUniqueName();
    String index = generateUniqueName();
    String fullTable = schema + "." + table;
    String cdcIndex = CDCUtil.getCDCIndexName("CDC_" + table);
    try (Connection conn = newClient(props, generateUniqueName())) {
      conn.createStatement().execute(
        "CREATE TABLE " + fullTable + " (id INTEGER PRIMARY KEY, name VARCHAR, city VARCHAR)");
      conn.createStatement()
        .execute("CREATE INDEX " + index + " ON " + fullTable + " (name) INCLUDE (city)");
      assertEquals("CDC index before the ALTER", null, lastDDLTimestamp(conn, schema, cdcIndex));
      conn.createStatement()
        .execute("ALTER INDEX " + index + " ON " + fullTable + " CONSISTENCY=EVENTUAL");
      Long cdcCreated = lastDDLTimestamp(conn, schema, cdcIndex);
      Long switched = lastDDLTimestamp(conn, schema, index);
      assertNotNull("CDC index after the ALTER", cdcCreated);
      assertNotNull("index LAST_DDL_TIMESTAMP after the ALTER", switched);
      assertTrue("CDC index created at " + cdcCreated + ", after the switch at " + switched,
        cdcCreated <= switched);
    }
  }

  @Test
  public void testRoundTripFromEventual() throws Exception {
    runRoundTrip(" CONSISTENCY=EVENTUAL", E, new IndexConsistency[] { S, E, S, E });
  }

  @Test
  public void testRoundTripFromStrong() throws Exception {
    runRoundTrip("", S, new IndexConsistency[] { E, S, E, S });
  }

  /**
   * Waits until the index has one row for each data row. The eventual index consumer writes the
   * index rows some time after the data rows.
   */
  private static void waitForIndexRows(Connection conn, String fullTable, String fullIndex,
    String phase) throws Exception {
    long deadline = EnvironmentEdgeManager.currentTimeMillis() + INDEX_CATCH_UP_TIMEOUT_MS;
    long dataRows;
    long indexRows;
    while (true) {
      dataRows = count(conn, "SELECT /*+ NO_INDEX */ COUNT(*) FROM " + fullTable);
      indexRows = count(conn, "SELECT COUNT(*) FROM " + fullIndex);
      if (dataRows == indexRows || EnvironmentEdgeManager.currentTimeMillis() > deadline) {
        break;
      }
      Thread.sleep(1000);
    }
    LOGGER.info("{}: dataRows={} indexRows={}", phase, dataRows, indexRows);
    assertEquals(phase + ": index rows", dataRows, indexRows);
  }

  /**
   * A second client writes before and after each ALTER. The index must get a row for each data row
   * in each phase.
   */
  @Test
  public void testIndexRowsCompleteAcrossRoundTrip() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String schema = generateUniqueName();
    String table = generateUniqueName();
    String index = generateUniqueName();
    String fullTable = schema + "." + table;
    String fullIndex = schema + "." + index;
    String tag = generateUniqueName();
    try (Connection conn1 = newClient(props, tag + "c1");
      Connection conn2 = newClient(props, tag + "w2")) {
      conn1.createStatement().execute(
        "CREATE TABLE " + fullTable + " (id INTEGER PRIMARY KEY, name VARCHAR, city VARCHAR)");
      conn1.createStatement().execute("CREATE INDEX " + index + " ON " + fullTable
        + " (name) INCLUDE (city) CONSISTENCY=EVENTUAL");
      int id = 1;
      for (int i = 0; i < 5; i++) {
        upsert(conn2, fullTable, id++);
      }
      waitForIndexRows(conn1, fullTable, fullIndex, "CREATE EVENTUAL");
      for (IndexConsistency target : new IndexConsistency[] { S, E, S }) {
        conn1.createStatement()
          .execute("ALTER INDEX " + index + " ON " + fullTable + " CONSISTENCY=" + target);
        for (int i = 0; i < 5; i++) {
          upsert(conn2, fullTable, id++);
          pointLookup(conn2, fullTable);
        }
        waitForIndexRows(conn1, fullTable, fullIndex, "ALTER " + target);
      }
    }
  }
}
