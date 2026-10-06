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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import org.apache.phoenix.util.QueryUtil;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Integration tests for server side BSON projection together with a server top-N, and together with
 * server parsed functions of different types.
 */
@Category(ParallelStatsDisabledTest.class)
public class BsonServerProjectionIT extends ParallelStatsDisabledIT {

  private static String createTable(Connection conn) throws Exception {
    return createTable(conn, "");
  }

  private static String createTable(Connection conn, String options) throws Exception {
    String table = generateUniqueName();
    conn.createStatement().execute(
      "CREATE TABLE " + table + " (ID VARCHAR PRIMARY KEY, N INTEGER, DOC BSON) " + options);
    try (
      PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + table + " VALUES (?, ?, ?)")) {
      for (int i = 0; i < 5; i++) {
        BsonDocument doc = new BsonDocument("name", new BsonString("name" + i));
        doc.put("rank", new BsonInt32(10 - i));
        doc.put("tag", new BsonDocument("k", new BsonString("tag" + i)));
        ps.setString(1, "id" + i);
        ps.setInt(2, i);
        ps.setObject(3, doc);
        ps.executeUpdate();
      }
    }
    conn.commit();
    return table;
  }

  private static List<String> rows(Connection conn, String sql) throws Exception {
    List<String> out = new ArrayList<>();
    try (ResultSet rs = conn.createStatement().executeQuery(sql)) {
      int n = rs.getMetaData().getColumnCount();
      while (rs.next()) {
        StringBuilder row = new StringBuilder();
        for (int i = 1; i <= n; i++) {
          row.append(i > 1 ? "," : "").append(rs.getString(i));
        }
        out.add(row.toString());
      }
    }
    return out;
  }

  /** Tests server side BSON projection together with a server top-N (ORDER BY with LIMIT). */
  @Test
  public void testProjectionWithServerTopN() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String table = createTable(conn);
      String sql =
        "SELECT ID, BSON_VALUE(DOC, 'name', 'VARCHAR') FROM " + table + " ORDER BY N DESC LIMIT 2";
      assertTrue(QueryUtil.getExplainPlan(conn.createStatement().executeQuery("EXPLAIN " + sql))
        .contains("SERVER TOP 2"));
      List<String> expected = new ArrayList<>();
      expected.add("id4,name4");
      expected.add("id3,name3");
      assertEquals(expected, rows(conn, sql));
    }
  }

  /** Tests the same query on a table without column qualifier encoding. */
  @Test
  public void testProjectionWithServerTopNWithoutColumnEncoding() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String table = createTable(conn, "COLUMN_ENCODED_BYTES=0");
      String sql =
        "SELECT ID, BSON_VALUE(DOC, 'name', 'VARCHAR') FROM " + table + " ORDER BY N DESC LIMIT 2";
      List<String> expected = new ArrayList<>();
      expected.add("id4,name4");
      expected.add("id3,name3");
      assertEquals(expected, rows(conn, sql));
    }
  }

  /**
   * Verifies an ORDER BY on the same BSON document column as a server parsed projection. The server
   * must keep the document cell for the sort.
   */
  @Test
  public void testOrderByReadsProjectedDocument() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String table = createTable(conn);
      String sql = "SELECT ID, BSON_VALUE(DOC, 'name', 'VARCHAR') FROM " + table
        + " ORDER BY BSON_VALUE(DOC, 'rank', 'INTEGER') LIMIT 2";
      List<String> expected = new ArrayList<>();
      expected.add("id4,name4");
      expected.add("id3,name3");
      assertEquals(expected, rows(conn, sql));
    }
  }

  /**
   * Verifies the decode order of server parsed functions of different types in one projection. The
   * query lists them in an order different from the server evaluation order (array element, JSON
   * value, BSON value, JSON query). A client layout that does not match the server layout reads
   * wrong values.
   */
  @Test
  public void testMixedFunctionProjection() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String table = generateUniqueName();
      conn.createStatement().execute(
        "CREATE TABLE " + table + " (ID VARCHAR PRIMARY KEY, A INTEGER ARRAY, J JSON, DOC BSON)");
      BsonDocument doc = new BsonDocument("name", new BsonString("bson-name"));
      doc.put("rank", new BsonInt32(8));
      try (PreparedStatement ps =
        conn.prepareStatement("UPSERT INTO " + table + " VALUES (?, ?, ?, ?)")) {
        ps.setString(1, "id1");
        ps.setArray(2, conn.createArrayOf("INTEGER", new Integer[] { 7, 9 }));
        ps.setString(3, "{\"k\": \"json-value\", \"obj\": {\"a\": 1}}");
        ps.setObject(4, doc);
        ps.executeUpdate();
      }
      conn.commit();
      String sql = "SELECT ID, JSON_QUERY(J, '$.obj'), BSON_VALUE(DOC, 'name', 'VARCHAR'),"
        + " JSON_VALUE(J, '$.k'), A[2], BSON_VALUE(DOC, 'rank', 'INTEGER') FROM " + table;
      String plan = QueryUtil.getExplainPlan(conn.createStatement().executeQuery("EXPLAIN " + sql));
      assertTrue(plan, plan.contains("SERVER ARRAY PROJECTION 1"));
      assertTrue(plan, plan.contains("SERVER JSON PROJECTION 2"));
      assertTrue(plan, plan.contains("SERVER BSON PROJECTION 2"));
      try (ResultSet rs = conn.createStatement().executeQuery(sql)) {
        assertTrue(rs.next());
        assertEquals("id1", rs.getString(1));
        assertEquals("{\"a\":1}", rs.getString(2).replaceAll("\\s", ""));
        assertEquals("bson-name", rs.getString(3));
        assertEquals("json-value", rs.getString(4));
        assertEquals(9, rs.getInt(5));
        assertEquals(8, rs.getInt(6));
        assertFalse(rs.next());
      }
    }
  }
}
