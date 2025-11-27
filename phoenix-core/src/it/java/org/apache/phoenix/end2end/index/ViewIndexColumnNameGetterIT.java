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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Integration tests for PHOENIX-6644: Column name based ResultSet getter issue with view indexes.
 * When a view has a constant column (e.g., WHERE v1 = 'a') and an index is used, the query
 * optimizer rewrites the query replacing the column reference with a literal. This test ensures
 * that ResultSet.getString(columnName) works correctly for both the original column name and the
 * rewritten literal.
 */
@Category(ParallelStatsDisabledTest.class)
public class ViewIndexColumnNameGetterIT extends ParallelStatsDisabledIT {

  /**
   * Tests basic view index with view constant column retrieval by name. This is the core scenario
   * reported in PHOENIX-6644.
   */
  @Test
  public void testViewIndexColumnNameGetter() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String tableName = generateUniqueName();
      String viewName = generateUniqueName();
      String indexName = generateUniqueName();

      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (id INTEGER NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR, v3 VARCHAR)");

      conn.createStatement()
        .execute("CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName + " WHERE v1 = 'a'");

      conn.createStatement()
        .execute("CREATE INDEX " + indexName + " ON " + viewName + " (v2) INCLUDE (v3)");

      conn.createStatement().execute("UPSERT INTO " + viewName + " VALUES (1, 'a', 'ab', 'abc')");
      conn.commit();

      ResultSet rs =
        conn.createStatement().executeQuery("SELECT v1, v3 FROM " + viewName + " WHERE v2 = 'ab'");

      assertTrue(rs.next());

      // Test position-based getter (this always worked)
      assertEquals("a", rs.getString(1));
      assertEquals("abc", rs.getString(2));

      // Test column name-based getter (this is the PHOENIX-6644 fix)
      assertEquals("a", rs.getString("v1"));
      assertEquals("abc", rs.getString("v3"));

      // Test qualified column names
      assertEquals("a", rs.getString(viewName + ".v1"));
      assertEquals("abc", rs.getString(viewName + ".v3"));

      assertFalse(rs.next());
    }
  }

  /**
   * Tests multiple view constant columns in the same query.
   */
  @Test
  public void testMultipleViewConstantColumns() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String tableName = generateUniqueName();
      String viewName = generateUniqueName();
      String indexName = generateUniqueName();

      conn.createStatement()
        .execute("CREATE TABLE " + tableName
          + " (id INTEGER NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR, "
          + "v3 VARCHAR, v4 VARCHAR)");

      conn.createStatement().execute("CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName
        + " WHERE v1 = 'a' AND v2 = 'b'");

      conn.createStatement()
        .execute("CREATE INDEX " + indexName + " ON " + viewName + " (v3) INCLUDE (v4)");

      conn.createStatement().execute("UPSERT INTO " + viewName + " VALUES (1, 'a', 'b', 'c', 'd')");
      conn.commit();

      ResultSet rs = conn.createStatement()
        .executeQuery("SELECT v1, v2, v4 FROM " + viewName + " WHERE v3 = 'c'");

      assertTrue(rs.next());

      // Both view constants should be retrievable by name
      assertEquals("a", rs.getString("v1"));
      assertEquals("b", rs.getString("v2"));
      assertEquals("d", rs.getString("v4"));

      assertFalse(rs.next());
    }
  }

  /**
   * Tests view index with SELECT * to ensure all columns are properly mapped.
   */
  @Test
  public void testViewIndexWithSelectStar() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String tableName = generateUniqueName();
      String viewName = generateUniqueName();
      String indexName = generateUniqueName();

      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (id INTEGER NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR, v3 VARCHAR)");

      conn.createStatement().execute(
        "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName + " WHERE v1 = 'xyz'");

      conn.createStatement()
        .execute("CREATE INDEX " + indexName + " ON " + viewName + " (v2) INCLUDE (v3)");

      conn.createStatement()
        .execute("UPSERT INTO " + viewName + " VALUES (1, 'xyz', 'indexed', 'included')");
      conn.commit();

      ResultSet rs =
        conn.createStatement().executeQuery("SELECT * FROM " + viewName + " WHERE v2 = 'indexed'");

      assertTrue(rs.next());

      // All columns should be retrievable by name
      assertEquals(1, rs.getInt("id"));
      assertEquals("xyz", rs.getString("v1"));
      assertEquals("indexed", rs.getString("v2"));
      assertEquals("included", rs.getString("v3"));

      assertFalse(rs.next());
    }
  }

  /**
   * Tests that column name lookup works with PreparedStatement and repeated executions.
   */
  @Test
  public void testPreparedStatementWithViewIndex() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String tableName = generateUniqueName();
      String viewName = generateUniqueName();
      String indexName = generateUniqueName();

      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (id INTEGER NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR, v3 VARCHAR)");

      conn.createStatement().execute(
        "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName + " WHERE v1 = 'constant'");

      conn.createStatement()
        .execute("CREATE INDEX " + indexName + " ON " + viewName + " (v2) INCLUDE (v3)");

      conn.createStatement()
        .execute("UPSERT INTO " + viewName + " VALUES (1, 'constant', 'value1', 'data1')");
      conn.createStatement()
        .execute("UPSERT INTO " + viewName + " VALUES (2, 'constant', 'value2', 'data2')");
      conn.commit();

      // Use PreparedStatement with parameter
      String query = "SELECT v1, v3 FROM " + viewName + " WHERE v2 = ?";
      try (java.sql.PreparedStatement pstmt = conn.prepareStatement(query)) {
        // First execution
        pstmt.setString(1, "value1");
        ResultSet rs = pstmt.executeQuery();
        assertTrue(rs.next());
        assertEquals("constant", rs.getString("v1"));
        assertEquals("data1", rs.getString("v3"));
        assertFalse(rs.next());

        // Second execution with different parameter
        pstmt.setString(1, "value2");
        rs = pstmt.executeQuery();
        assertTrue(rs.next());
        assertEquals("constant", rs.getString("v1"));
        assertEquals("data2", rs.getString("v3"));
        assertFalse(rs.next());
      }
    }
  }

  /**
   * Tests view index used in a subquery.
   */
  @Test
  public void testSubqueryWithViewIndex() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String tableName = generateUniqueName();
      String viewName = generateUniqueName();
      String indexName = generateUniqueName();

      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (id INTEGER NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR, v3 VARCHAR)");

      conn.createStatement().execute("CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName
        + " WHERE v1 = 'subquery_test'");

      conn.createStatement()
        .execute("CREATE INDEX " + indexName + " ON " + viewName + " (v2) INCLUDE (v3)");

      conn.createStatement()
        .execute("UPSERT INTO " + viewName + " VALUES (1, 'subquery_test', 'indexed', 'result')");
      conn.commit();

      // Query using subquery with view index
      String subquery = "SELECT v1, v3 FROM " + viewName + " WHERE v2 = 'indexed'";
      ResultSet rs = conn.createStatement()
        .executeQuery("SELECT * FROM (" + subquery + ") sub WHERE sub.v1 = 'subquery_test'");

      assertTrue(rs.next());

      // Column names from subquery should be accessible
      assertEquals("subquery_test", rs.getString("v1"));
      assertEquals("result", rs.getString("v3"));

      assertFalse(rs.next());
    }
  }

  /**
   * Tests that the fix doesn't break regular index queries (without views).
   */
  @Test
  public void testRegularIndexNotAffected() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String tableName = generateUniqueName();
      String indexName = generateUniqueName();

      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (id INTEGER NOT NULL PRIMARY KEY, col1 VARCHAR, col2 VARCHAR)");

      // Create regular index (not on a view)
      conn.createStatement()
        .execute("CREATE INDEX " + indexName + " ON " + tableName + " (col1) INCLUDE (col2)");

      conn.createStatement()
        .execute("UPSERT INTO " + tableName + " VALUES (1, 'value1', 'value2')");
      conn.commit();

      // Query using the regular index
      ResultSet rs = conn.createStatement()
        .executeQuery("SELECT col1, col2 FROM " + tableName + " WHERE col1 = 'value1'");

      assertTrue(rs.next());

      // Regular index should still work fine
      assertEquals("value1", rs.getString("col1"));
      assertEquals("value2", rs.getString("col2"));

      assertFalse(rs.next());
    }
  }
}
