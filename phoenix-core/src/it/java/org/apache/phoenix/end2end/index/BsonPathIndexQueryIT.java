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
package org.apache.phoenix.end2end.index;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.util.PropertiesUtil;
import org.bson.BsonDocument;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class BsonPathIndexQueryIT extends ParallelStatsDisabledIT {

  private String tbl;
  private String idx;

  private void setupSchema(Connection conn) throws Exception {
    tbl = generateUniqueName();
    idx = generateUniqueName();
    conn.createStatement().execute(
        "CREATE TABLE " + tbl + " (PK VARCHAR PRIMARY KEY, DOC BSON)");
    conn.createStatement().execute(
        "CREATE INDEX " + idx + " ON " + tbl + "(BSON_VALUE(DOC, '$.name', 'VARCHAR'))");
    try (PreparedStatement ps = conn.prepareStatement(
        "UPSERT INTO " + tbl + " VALUES (?, ?)")) {
      ps.setString(1, "k1"); ps.setObject(2, BsonDocument.parse("{\"name\":\"alice\"}")); ps.execute();
      ps.setString(1, "k2"); ps.setObject(2, BsonDocument.parse("{\"name\":\"bob\"}"));   ps.execute();
      ps.setString(1, "k3"); ps.setObject(2, BsonDocument.parse("{\"name\":\"carol\"}")); ps.execute();
      ps.setString(1, "k4"); ps.setObject(2, BsonDocument.parse("{\"other\":\"x\"}"));    ps.execute();
    }
    conn.commit();
  }

  private static String explain(Connection conn, String sql) throws Exception {
    try (ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + sql)) {
      StringBuilder sb = new StringBuilder();
      while (rs.next()) sb.append(rs.getString(1)).append('\n');
      return sb.toString();
    }
  }

  @Test
  public void canonicalEqualityHitsIndex() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      setupSchema(conn);
      long before = org.apache.phoenix.monitoring.BsonPathMetrics.getRewriteHits();
      String sql = "SELECT PK FROM " + tbl
          + " WHERE BSON_VALUE(DOC, '$.name', 'VARCHAR') = 'alice'";
      String plan = explain(conn, sql);
      assertTrue("expected index in plan: " + plan, plan.contains(idx));
      long after = org.apache.phoenix.monitoring.BsonPathMetrics.getRewriteHits();
      assertTrue("expected rewrite hit counter to increase", after > before);
    }
  }

  @Test
  public void barePathEqualityHitsIndex() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      setupSchema(conn);
      String sql = "SELECT PK FROM " + tbl
          + " WHERE BSON_VALUE(DOC, 'name', 'VARCHAR') = 'bob'";
      String plan = explain(conn, sql);
      assertTrue("expected index in plan (bare path): " + plan, plan.contains(idx));
      try (ResultSet rs = conn.createStatement().executeQuery(sql)) {
        assertTrue(rs.next());
        assertEquals("k2", rs.getString(1));
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void inHitsIndex() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      setupSchema(conn);
      String sql = "SELECT PK FROM " + tbl
          + " WHERE BSON_VALUE(DOC, 'name', 'VARCHAR') IN ('alice','carol')";
      String plan = explain(conn, sql);
      assertTrue("expected index in plan (IN): " + plan, plan.contains(idx));
    }
  }

  @Test
  public void rangeHitsIndex() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      setupSchema(conn);
      String sql = "SELECT PK FROM " + tbl
          + " WHERE BSON_VALUE(DOC, '$.name', 'VARCHAR') BETWEEN 'b' AND 'm'";
      String plan = explain(conn, sql);
      assertTrue("expected index in plan (BETWEEN): " + plan, plan.contains(idx));
    }
  }

  @Test
  public void wrappedLhsStillReturnsCorrectResults() throws Exception {
    // Phoenix's IndexStatementRewriter substitutes the indexed expression inside the
    // surrounding function (UPPER) and runs a SERVER FILTER on the index — that's fine and
    // produces correct results. We just verify the predicate returns the right row.
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      setupSchema(conn);
      String sql = "SELECT PK FROM " + tbl
          + " WHERE UPPER(BSON_VALUE(DOC, '$.name', 'VARCHAR')) = 'ALICE'";
      try (ResultSet rs = conn.createStatement().executeQuery(sql)) {
        assertTrue(rs.next());
        assertEquals("k1", rs.getString(1));
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void rewriteFlagOffFallsBackToFullScan() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    props.setProperty("phoenix.index.bson.rewrite.enabled", "false");
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      setupSchema(conn);
      String sql = "SELECT PK FROM " + tbl
          + " WHERE BSON_VALUE(DOC, 'name', 'VARCHAR') = 'alice'";
      String plan = explain(conn, sql);
      assertFalse("rewrite-disabled plan should not use index: " + plan, plan.contains(idx));
    }
  }
}
