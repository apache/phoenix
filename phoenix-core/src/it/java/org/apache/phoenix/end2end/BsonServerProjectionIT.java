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
 * Integration tests for server side BSON projection with top-N ordering and multi-function
 * evaluation.
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

  /** Tests server side BSON projection combined with TOP-N row ordering and limit. */
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

  /** Tests server side BSON projection with non-encoded column storage scheme. */
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

  /** Verifies ORDER BY expressions referencing the projected BSON document column. */
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

  /** Verifies correct decoding order when multiple distinct BSON functions are projected. */
  @Test
  public void testMixedFunctionProjection() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String table = createTable(conn);
      List<String> rows = rows(conn,
        "SELECT ID, BSON_VALUE(DOC, 'rank', 'INTEGER'), BSON_VALUE(DOC, 'name', 'VARCHAR'),"
          + " BSON_VALUE(DOC, 'tag.k', 'VARCHAR') FROM " + table + " WHERE ID = 'id2'");
      assertEquals(1, rows.size());
      assertEquals("id2,8,name2,tag2", rows.get(0));
      assertFalse(rows.get(0).contains("null"));
    }
  }
}
