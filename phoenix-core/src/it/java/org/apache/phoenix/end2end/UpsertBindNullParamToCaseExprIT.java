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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Properties;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class UpsertBindNullParamToCaseExprIT extends BaseTest {

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    setUpTestDriver(new ReadOnlyProps(new HashMap<>()));
  }

  @AfterClass
  public static synchronized void freeResources() throws Exception {
    BaseTest.freeResourcesIfBeyondThreshold();
  }

  @Test
  public void testBindNullUpsertSelect() throws Exception {
    try (Connection conn = newConnection()) {
      String tableName = createChunkTable(conn);
      String upsert_stmt = "UPSERT INTO " + tableName + " (row_id, chunk) VALUES (?, ?)";
      runTestBindForNull(tableName, conn, upsert_stmt, 1, null, null);
      runTestBindForNull(tableName, conn, upsert_stmt, 1, "value", "value");
    }
  }

  @Test
  public void testBindNullUpsertSelectWithCaseIsNotNull() throws Exception {
    try (Connection conn = newConnection()) {
      String tableName = createChunkTable(conn);
      String upsert_stmt = "UPSERT INTO " + tableName
        + " SELECT :1, CASE WHEN chunk IS NOT NULL THEN chunk ELSE :2 END FROM " + tableName
        + " WHERE row_id = :1";
      upsertNullRow(tableName, conn, 1);
      runTestBindForNull(tableName, conn, upsert_stmt, 1, null, null);
      runTestBindForNull(tableName, conn, upsert_stmt, 1, "value", "value");
      upsertNullRow(tableName, conn, 2);
      runTestBindForNull(tableName, conn, upsert_stmt, 2, "value", "value");
      runTestBindForNull(tableName, conn, upsert_stmt, 2, null, "value");
    }
  }

  @Test
  public void testBindNullUpsertSelectWithCaseIsNull() throws Exception {
    try (Connection conn = newConnection()) {
      String tableName = createChunkTable(conn);
      String upsert_stmt = "UPSERT INTO " + tableName
        + " SELECT :1, CASE WHEN :2 IS NULL THEN 'default' ELSE chunk END FROM " + tableName
        + " WHERE row_id = :1";
      upsertNullRow(tableName, conn, 1);
      runTestBindForNull(tableName, conn, upsert_stmt, 1, null, "default");
      runTestBindForNull(tableName, conn, upsert_stmt, 1, "value", "default");
      runTestBindForNull(tableName, conn, upsert_stmt, 1, null, "default");
    }
  }

  @Test
  public void testBindNullUpsertSelectWithCaseIsNull2() throws Exception {
    try (Connection conn = newConnection()) {
      String tableName = createChunkTable(conn);
      String upsert_stmt =
        "UPSERT INTO " + tableName + " SELECT :1, CASE WHEN :2 IS NULL THEN NULL ELSE :2 END FROM "
          + tableName + " WHERE row_id = :1";
      upsertNullRow(tableName, conn, 1);
      runTestBindForNull(tableName, conn, upsert_stmt, 1, null, null);
      runTestBindForNull(tableName, conn, upsert_stmt, 1, "value", "value");
      runTestBindForNull(tableName, conn, upsert_stmt, 1, null, null);
    }
  }

  @Test
  public void testBindNullOnDuplicateKeyIsNotNull1() throws Exception {
    try (Connection conn = newConnection()) {
      String tableName = createChunkTable(conn);
      String upsert_stmt = "UPSERT INTO " + tableName + " (row_id, chunk) VALUES (:1, :2)\n"
        + "ON DUPLICATE KEY UPDATE\n"
        + "    chunk = CASE WHEN chunk IS NOT NULL THEN chunk ELSE :2 END";
      runTestBindForNull(tableName, conn, upsert_stmt, 1, null, null);
      runTestBindForNull(tableName, conn, upsert_stmt, 1, "value", "value");
      runTestBindForNull(tableName, conn, upsert_stmt, 1, null, "value");
      runTestBindForNull(tableName, conn, upsert_stmt, 1, "newval", "value");
      runTestBindForNull(tableName, conn, upsert_stmt, 2, "value", "value");
      runTestBindForNull(tableName, conn, upsert_stmt, 2, null, "value");
    }
  }

  @Test
  public void testBindNullOnDuplicateKeyIsNotNull2() throws Exception {
    try (Connection conn = newConnection()) {
      String tableName = createChunkTable(conn);
      String upsert_stmt = "UPSERT INTO " + tableName + " (row_id, chunk) VALUES (:1, :2)\n"
        + "ON DUPLICATE KEY UPDATE\n"
        + "    chunk = CASE WHEN :2 IS NOT NULL THEN :2 ELSE chunk END";
      runTestBindForNull(tableName, conn, upsert_stmt, 1, null, null);
      runTestBindForNull(tableName, conn, upsert_stmt, 1, null, null);
      runTestBindForNull(tableName, conn, upsert_stmt, 2, "value", "value");
      runTestBindForNull(tableName, conn, upsert_stmt, 2, null, "value");
    }
  }

  @Test
  public void testBindNullOnDuplicateKeyIsNull() throws Exception {
    try (Connection conn = newConnection()) {
      String tableName = createChunkTable(conn);
      String upsert_stmt = "UPSERT INTO " + tableName + " (row_id, chunk) VALUES (:1, :2)\n"
        + "ON DUPLICATE KEY UPDATE\n" + "    chunk = CASE WHEN :2 IS NULL THEN NULL ELSE :2 END";
      runTestBindForNull(tableName, conn, upsert_stmt, 1, null, null);
      runTestBindForNull(tableName, conn, upsert_stmt, 1, "value", "value");
      runTestBindForNull(tableName, conn, upsert_stmt, 1, null, null);
      runTestBindForNull(tableName, conn, upsert_stmt, 1, null, null);
    }
  }

  private static Connection newConnection() throws SQLException {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    // Uncomment these only while debugging.
    // props.put(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB, Long.toString(Long.MAX_VALUE));
    // props.put("hbase.client.scanner.timeout.period", "6000000");
    // props.put("phoenix.query.timeoutMs", "6000000");
    // props.put("zookeeper.session.timeout", "6000000");
    // props.put("hbase.rpc.timeout", "6000000");
    Connection conn = DriverManager.getConnection(getUrl(), props);
    conn.setAutoCommit(true);
    return conn;
  }

  private static String createChunkTable(Connection conn) throws Exception {
    String tableName = generateUniqueName();
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE " + tableName + " (\n" + "    row_id INTEGER NOT NULL,\n"
        + "    chunk VARCHAR,\n" + "    CONSTRAINT PK PRIMARY KEY (row_id)\n" + ")");
    }
    return tableName;
  }

  private static void upsertNullRow(String tableName, Connection conn, int rowId) throws Exception {
    try (PreparedStatement stmt =
      conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?, NULL)")) {
      stmt.setInt(1, rowId);
      stmt.execute();
    }
  }

  private static void runTestBindForNull(String tableName, Connection conn, String upsert_stmt,
    int rowId, String chunkVal, String expectedChunk) throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(upsert_stmt)) {
      stmt.setInt(1, rowId);
      if (chunkVal == null) {
        stmt.setNull(2, java.sql.Types.VARCHAR);
      } else {
        stmt.setString(2, chunkVal);
      }
      stmt.execute();
    }
    String select_stmt = "SELECT * FROM " + tableName + " WHERE row_id = " + rowId;
    try (Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(select_stmt)) {
        assertTrue(rs.next());
        assertEquals(rowId, rs.getInt(1));
        if (expectedChunk == null) {
          assertNull(rs.getBytes(2));
        } else {
          assertEquals(expectedChunk, rs.getString(2));
        }
      }
    }
  }

  @Test
  public void testBindWithComplexCasePHOENIX_7615() throws Exception {
    String tableName = generateUniqueName();
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.setAutoCommit(true);
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName + " (\n" + "    row_id CHAR(15) NOT NULL,\n"
          + "    chunk_id INTEGER NOT NULL,\n" + "    total_chunks INTEGER,\n"
          + "    hash VARCHAR,\n" + "    chunk VARBINARY,\n"
          + "    CONSTRAINT PK PRIMARY KEY (row_id, chunk_id)\n" + ")");
      }
      String upsert_stmt =
        "UPSERT INTO " + tableName + " (row_id, chunk_id, total_chunks, hash, chunk)\n"
          + "VALUES (:1, :2, :3, :4, :5)\n" + "ON DUPLICATE KEY UPDATE\n"
          + "chunk = CASE WHEN (hash IS NULL AND :4 IS NOT NULL OR hash IS NOT NULL and "
          + ":4 IS NULL OR hash != :4) THEN :5 ELSE chunk END";
      String select_stmt = "SELECT * from " + tableName;
      String val1 = "def";
      upsertRow(conn, upsert_stmt, val1, val1.getBytes());
      assertRow(conn, select_stmt, val1.getBytes());
      String val2 = "def";
      upsertRow(conn, upsert_stmt, val2, val2.getBytes());
      assertRow(conn, select_stmt, val2.getBytes());
      upsertRow(conn, upsert_stmt, null, null);
      assertRow(conn, select_stmt, null);
    }
  }

  private static void assertRow(Connection conn, String select_stmt, byte[] val)
    throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(select_stmt)) {
        assertTrue(rs.next());
        if (val == null) {
          assertNull(rs.getBytes("chunk"));
        } else {
          assertTrue(Bytes.compareTo(rs.getBytes("chunk"), val) == 0);
        }
      }
    }
  }

  private static void upsertRow(Connection conn, String upsert_stmt, String hash, byte[] val)
    throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(upsert_stmt)) {
      stmt.setString(1, "R1");
      stmt.setInt(2, 1);
      stmt.setInt(3, 1);
      if (hash == null) {
        stmt.setNull(4, java.sql.Types.VARCHAR);
      } else {
        stmt.setString(4, hash);
      }
      if (val == null) {
        stmt.setNull(5, java.sql.Types.VARBINARY);
      } else {
        stmt.setBytes(5, val);
      }
      stmt.execute();
    }
  }
}
