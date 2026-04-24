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
import java.sql.SQLException;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class RegexpLikeFunctionIT extends ParallelStatsDisabledIT {

  private String tableName;
  private int id;

  @Before
  public void setup() throws Exception {
    tableName = generateUniqueName();
    Connection conn = DriverManager.getConnection(getUrl());
    conn.createStatement()
      .execute("CREATE TABLE " + tableName + " (ID VARCHAR NOT NULL PRIMARY KEY, VAL VARCHAR)");
    insertRow(conn, "Hello World");
    insertRow(conn, "hello world");
    insertRow(conn, "Report123");
    insertRow(conn, "Test456");
    insertRow(conn, "line1\nline2");
    insertRow(conn, null);
    conn.commit();
    conn.close();
  }

  private void insertRow(Connection conn, String val) throws SQLException {
    PreparedStatement stmt =
      conn.prepareStatement("UPSERT INTO " + tableName + " (ID, VAL) VALUES (?, ?)");
    stmt.setString(1, "id" + id);
    stmt.setString(2, val);
    stmt.executeUpdate();
    id++;
  }

  @Test
  public void testBasicMatch() throws Exception {
    Connection conn = DriverManager.getConnection(getUrl());
    // Match rows starting with 'Hello' — full match requires .* at end
    ResultSet rs = conn.createStatement().executeQuery(
      "SELECT ID FROM " + tableName + " WHERE REGEXP_LIKE(VAL, 'Hello.*') ORDER BY ID");
    assertTrue(rs.next());
    assertEquals("id0", rs.getString(1));
    assertFalse(rs.next());
    conn.close();
  }

  @Test
  public void testNoMatch() throws Exception {
    Connection conn = DriverManager.getConnection(getUrl());
    ResultSet rs = conn.createStatement()
      .executeQuery("SELECT ID FROM " + tableName + " WHERE REGEXP_LIKE(VAL, '^ZZZZZ')");
    assertFalse(rs.next());
    conn.close();
  }

  @Test
  public void testCaseInsensitiveFlag() throws Exception {
    Connection conn = DriverManager.getConnection(getUrl());
    // With 'i' flag, should match both 'Hello World' and 'hello world'
    ResultSet rs = conn.createStatement().executeQuery(
      "SELECT ID FROM " + tableName + " WHERE REGEXP_LIKE(VAL, 'hello.*', 'i') ORDER BY ID");
    assertTrue(rs.next());
    assertEquals("id0", rs.getString(1));
    assertTrue(rs.next());
    assertEquals("id1", rs.getString(1));
    assertFalse(rs.next());
    conn.close();
  }

  @Test
  public void testCaseSensitiveDefault() throws Exception {
    Connection conn = DriverManager.getConnection(getUrl());
    // Without flag, case-sensitive: only lowercase 'hello' matches
    ResultSet rs = conn.createStatement().executeQuery(
      "SELECT ID FROM " + tableName + " WHERE REGEXP_LIKE(VAL, 'hello.*') ORDER BY ID");
    assertTrue(rs.next());
    assertEquals("id1", rs.getString(1));
    assertFalse(rs.next());
    conn.close();
  }

  @Test
  public void testCaseSensitiveFlagOverride() throws Exception {
    Connection conn = DriverManager.getConnection(getUrl());
    // 'ic' means last one wins: 'c' overrides 'i', so case-sensitive
    ResultSet rs = conn.createStatement().executeQuery(
      "SELECT ID FROM " + tableName + " WHERE REGEXP_LIKE(VAL, 'hello.*', 'ic') ORDER BY ID");
    assertTrue(rs.next());
    assertEquals("id1", rs.getString(1));
    assertFalse(rs.next());
    conn.close();
  }

  @Test
  public void testDigitPattern() throws Exception {
    Connection conn = DriverManager.getConnection(getUrl());
    // Match rows that are entirely digits
    ResultSet rs = conn.createStatement().executeQuery(
      "SELECT ID FROM " + tableName + " WHERE REGEXP_LIKE(VAL, '.*\\d+.*') ORDER BY ID");
    assertTrue(rs.next());
    assertEquals("id2", rs.getString(1)); // Report123
    assertTrue(rs.next());
    assertEquals("id3", rs.getString(1)); // Test456
    // id4 has newline — '.' does not match newline by default, so it won't match
    assertFalse(rs.next());
    conn.close();
  }

  @Test
  public void testInSelectList() throws Exception {
    Connection conn = DriverManager.getConnection(getUrl());
    // Use REGEXP_LIKE in SELECT list — should return boolean (full match)
    ResultSet rs = conn.createStatement()
      .executeQuery("SELECT ID, REGEXP_LIKE(VAL, 'Report.*') FROM " + tableName + " ORDER BY ID");
    assertTrue(rs.next());
    assertEquals("id0", rs.getString(1));
    assertFalse(rs.getBoolean(2)); // Hello World
    assertTrue(rs.next());
    assertEquals("id1", rs.getString(1));
    assertFalse(rs.getBoolean(2)); // hello world
    assertTrue(rs.next());
    assertEquals("id2", rs.getString(1));
    assertTrue(rs.getBoolean(2)); // Report123
    assertTrue(rs.next());
    assertEquals("id3", rs.getString(1));
    assertFalse(rs.getBoolean(2)); // Test456
    assertTrue(rs.next());
    assertEquals("id4", rs.getString(1));
    assertFalse(rs.getBoolean(2)); // line1\nline2
    assertTrue(rs.next());
    assertEquals("id5", rs.getString(1));
    // NULL val — REGEXP_LIKE returns false for null
    assertFalse(rs.getBoolean(2));
    assertFalse(rs.next());
    conn.close();
  }

  @Test
  public void testNullSourceReturnsNull() throws Exception {
    Connection conn = DriverManager.getConnection(getUrl());
    // NULL source should not match; use 's' flag so '.' matches newline too
    ResultSet rs = conn.createStatement().executeQuery(
      "SELECT ID FROM " + tableName + " WHERE REGEXP_LIKE(VAL, '.*', 's') ORDER BY ID");
    // id5 has NULL val, should not appear in results
    assertTrue(rs.next());
    assertEquals("id0", rs.getString(1));
    assertTrue(rs.next());
    assertEquals("id1", rs.getString(1));
    assertTrue(rs.next());
    assertEquals("id2", rs.getString(1));
    assertTrue(rs.next());
    assertEquals("id3", rs.getString(1));
    assertTrue(rs.next());
    assertEquals("id4", rs.getString(1));
    assertFalse(rs.next());
    conn.close();
  }

  @Test
  public void testFullMatchPattern() throws Exception {
    Connection conn = DriverManager.getConnection(getUrl());
    // REGEXP_LIKE does full match (like Java's matches()), not partial
    // 'Report' alone should NOT match 'Report123' because it's not a full match
    ResultSet rs = conn.createStatement()
      .executeQuery("SELECT ID FROM " + tableName + " WHERE REGEXP_LIKE(VAL, 'Report')");
    assertFalse(rs.next());

    // 'Report.*' should match 'Report123'
    rs = conn.createStatement()
      .executeQuery("SELECT ID FROM " + tableName + " WHERE REGEXP_LIKE(VAL, 'Report.*')");
    assertTrue(rs.next());
    assertEquals("id2", rs.getString(1));
    assertFalse(rs.next());
    conn.close();
  }

  @Test
  public void testMultilineFlag() throws Exception {
    Connection conn = DriverManager.getConnection(getUrl());
    // Without 'm' flag, ^ only matches start of string
    // 'line2' is on the second line of id4's value
    // '^line2$' should NOT match without multiline
    ResultSet rs = conn.createStatement()
      .executeQuery("SELECT ID FROM " + tableName + " WHERE REGEXP_LIKE(VAL, '^line2$')");
    assertFalse(rs.next());

    // With 'm' flag, ^ and $ match line boundaries
    // But REGEXP_LIKE is a full match, so we need '.*line2.*' with DOTALL
    // or use multiline with a pattern that matches the whole string
    rs = conn.createStatement().executeQuery(
      "SELECT ID FROM " + tableName + " WHERE REGEXP_LIKE(VAL, '(?s).*^line2$.*', 'm')");
    assertTrue(rs.next());
    assertEquals("id4", rs.getString(1));
    assertFalse(rs.next());
    conn.close();
  }

  @Test
  public void testDotallFlag() throws Exception {
    Connection conn = DriverManager.getConnection(getUrl());
    // Without 's' flag, '.' does not match newline
    // 'line1.line2' should NOT match because there's a \n between them
    ResultSet rs = conn.createStatement()
      .executeQuery("SELECT ID FROM " + tableName + " WHERE REGEXP_LIKE(VAL, 'line1.line2')");
    assertFalse(rs.next());

    // With 's' flag (dotall), '.' matches newline
    rs = conn.createStatement()
      .executeQuery("SELECT ID FROM " + tableName + " WHERE REGEXP_LIKE(VAL, 'line1.line2', 's')");
    assertTrue(rs.next());
    assertEquals("id4", rs.getString(1));
    assertFalse(rs.next());
    conn.close();
  }

  @Test
  public void testNotRegexpLike() throws Exception {
    Connection conn = DriverManager.getConnection(getUrl());
    // NOT REGEXP_LIKE — negate the function
    // '.*\\d+.*' with 's' flag matches any string containing digits (including newlines)
    ResultSet rs = conn.createStatement().executeQuery(
      "SELECT ID FROM " + tableName + " WHERE NOT REGEXP_LIKE(VAL, '.*\\d+.*', 's') ORDER BY ID");
    assertTrue(rs.next());
    assertEquals("id0", rs.getString(1)); // Hello World
    assertTrue(rs.next());
    assertEquals("id1", rs.getString(1)); // hello world
    assertFalse(rs.next());
    conn.close();
  }

  @Test
  public void testCaseSensitiveThenInsensitive_LastWins() throws Exception {
    Connection conn = DriverManager.getConnection(getUrl());
    // 'ci' — 'i' comes last, so case-insensitive: matches both Hello and hello
    ResultSet rs = conn.createStatement().executeQuery(
      "SELECT ID FROM " + tableName + " WHERE REGEXP_LIKE(VAL, 'hello.*', 'ci') ORDER BY ID");
    assertTrue(rs.next());
    assertEquals("id0", rs.getString(1)); // Hello World
    assertTrue(rs.next());
    assertEquals("id1", rs.getString(1)); // hello world
    assertFalse(rs.next());
    conn.close();
  }

  @Test
  public void testCombinedCaseInsensitiveAndDotall() throws Exception {
    Connection conn = DriverManager.getConnection(getUrl());
    // 'is' — case-insensitive + dotall: '.' matches newline
    // Pattern 'LINE1.LINE2' with 'is' should match 'line1\nline2'
    ResultSet rs = conn.createStatement().executeQuery(
      "SELECT ID FROM " + tableName + " WHERE REGEXP_LIKE(VAL, 'LINE1.LINE2', 'is') ORDER BY ID");
    assertTrue(rs.next());
    assertEquals("id4", rs.getString(1));
    assertFalse(rs.next());
    conn.close();
  }

  @Test
  public void testCombinedMultilineAndDotall() throws Exception {
    Connection conn = DriverManager.getConnection(getUrl());
    // 'ms' — multiline + dotall combined
    // With dotall, '.*' matches newlines; with multiline, ^ and $ match line boundaries
    ResultSet rs = conn.createStatement().executeQuery(
      "SELECT ID FROM " + tableName + " WHERE REGEXP_LIKE(VAL, '.*line2.*', 'ms') ORDER BY ID");
    assertTrue(rs.next());
    assertEquals("id4", rs.getString(1));
    assertFalse(rs.next());
    conn.close();
  }

  @Test
  public void testInvalidFlagThrowsError() throws Exception {
    Connection conn = DriverManager.getConnection(getUrl());
    try {
      conn.createStatement()
        .executeQuery("SELECT ID FROM " + tableName + " WHERE REGEXP_LIKE(VAL, 'Hello.*', 'x')");
      assertFalse("Expected exception for invalid flag", true);
    } catch (Exception e) {
      // Expected: invalid match_parameter character 'x'
      assertTrue(e.getMessage().contains("Invalid match_parameter character")
        || e.getCause().getMessage().contains("Invalid match_parameter character"));
    }
    conn.close();
  }

  @Test
  public void testWithPreparedStatement() throws Exception {
    Connection conn = DriverManager.getConnection(getUrl());
    // Test REGEXP_LIKE with a parameterized pattern via PreparedStatement
    String sql = "SELECT ID FROM " + tableName + " WHERE REGEXP_LIKE(VAL, ?) ORDER BY ID";
    PreparedStatement stmt = conn.prepareStatement(sql);
    stmt.setString(1, "Hello.*");
    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertEquals("id0", rs.getString(1));
    assertFalse(rs.next());

    // Test with a different pattern
    stmt.setString(1, ".*\\d+.*");
    rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertEquals("id2", rs.getString(1)); // Report123
    assertTrue(rs.next());
    assertEquals("id3", rs.getString(1)); // Test456
    assertFalse(rs.next());

    stmt.close();
    conn.close();
  }

  // ---- Tests for dynamic (evaluate-time) pattern compilation ----
  // These tests exercise the code path where this.pattern is null after init()
  // because the pattern expression is not stateless (fails isStateless() check).
  // In these cases, the pattern is compiled per-row during evaluate().

  /**
   * Helper to create and populate a table with columns needed for dynamic pattern tests.
   */
  private String createDynamicPatternTable() throws Exception {
    String dynTable = generateUniqueName();
    Connection conn = DriverManager.getConnection(getUrl());
    conn.createStatement()
      .execute("CREATE TABLE " + dynTable + " (ID VARCHAR NOT NULL PRIMARY KEY, VAL VARCHAR,"
        + " PATTERN_COL VARCHAR, CATEGORY VARCHAR, PREFIX_COL VARCHAR)");
    PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + dynTable
      + " (ID, VAL, PATTERN_COL, CATEGORY, PREFIX_COL) VALUES (?, ?, ?, ?, ?)");
    // id0: Hello World, pattern "Hello.*", greeting, prefix "Hello"
    upsertDynRow(stmt, "id0", "Hello World", "Hello.*", "greeting", "Hello");
    // id1: hello world, pattern "hello.*", greeting, prefix "hello"
    upsertDynRow(stmt, "id1", "hello world", "hello.*", "greeting", "hello");
    // id2: Report123, pattern ".*\\d+.*", code, prefix "Report"
    upsertDynRow(stmt, "id2", "Report123", ".*\\d+.*", "code", "Report");
    // id3: Test456, pattern "Test.*", code, prefix "Test"
    upsertDynRow(stmt, "id3", "Test456", "Test.*", "code", "Test");
    // id4: line1\nline2, pattern "NOMATCH", other, prefix "line1"
    upsertDynRow(stmt, "id4", "line1\nline2", "NOMATCH", "other", "line1");
    // id5: null val, null pattern, null category, null prefix
    upsertDynRow(stmt, "id5", null, null, null, null);
    conn.commit();
    conn.close();
    return dynTable;
  }

  private void upsertDynRow(PreparedStatement stmt, String id, String val, String patternCol,
    String category, String prefixCol) throws SQLException {
    stmt.setString(1, id);
    stmt.setString(2, val);
    stmt.setString(3, patternCol);
    stmt.setString(4, category);
    stmt.setString(5, prefixCol);
    stmt.executeUpdate();
  }

  @Test
  public void testDynamicPatternFromColumn() throws Exception {
    // Category: not stateless — pattern is a column reference.
    // PATTERN_COL is not a literal constant, so init() cannot pre-compile.
    // Each row supplies its own regex via PATTERN_COL, compiled at evaluate time.
    String dynTable = createDynamicPatternTable();
    Connection conn = DriverManager.getConnection(getUrl());
    ResultSet rs = conn.createStatement().executeQuery(
      "SELECT ID FROM " + dynTable + " WHERE REGEXP_LIKE(VAL, PATTERN_COL) ORDER BY ID");
    assertTrue(rs.next());
    assertEquals("id0", rs.getString(1)); // "Hello World" matches "Hello.*"
    assertTrue(rs.next());
    assertEquals("id1", rs.getString(1)); // "hello world" matches "hello.*"
    assertTrue(rs.next());
    assertEquals("id2", rs.getString(1)); // "Report123" matches ".*\\d+.*"
    assertTrue(rs.next());
    assertEquals("id3", rs.getString(1)); // "Test456" matches "Test.*"
    // id4: "line1\nline2" does NOT match "NOMATCH"
    // id5: null val and null pattern — excluded
    assertFalse(rs.next());
    conn.close();
  }

  @Test
  public void testDynamicPatternFromCaseExpression() throws Exception {
    // Category: not stateless — CASE expression depends on CATEGORY column.
    // The CASE references a column, so the entire expression is not stateless.
    // Pattern is determined per-row based on the CATEGORY value.
    String dynTable = createDynamicPatternTable();
    Connection conn = DriverManager.getConnection(getUrl());
    ResultSet rs = conn.createStatement()
      .executeQuery("SELECT ID FROM " + dynTable + " WHERE REGEXP_LIKE(VAL,"
        + " CASE WHEN CATEGORY = 'greeting' THEN 'Hello.*'"
        + "      WHEN CATEGORY = 'code' THEN '.*\\d+.*'" + "      ELSE 'NOMATCH' END) ORDER BY ID");
    assertTrue(rs.next());
    assertEquals("id0", rs.getString(1)); // greeting → "Hello.*" matches "Hello World"
    // id1: greeting → "Hello.*" does NOT match "hello world" (case-sensitive)
    assertTrue(rs.next());
    assertEquals("id2", rs.getString(1)); // code → ".*\\d+.*" matches "Report123"
    assertTrue(rs.next());
    assertEquals("id3", rs.getString(1)); // code → ".*\\d+.*" matches "Test456"
    // id4: other → "NOMATCH" does NOT match "line1\nline2"
    // id5: null category → ELSE "NOMATCH", null val → excluded
    assertFalse(rs.next());
    conn.close();
  }

  @Test
  public void testDynamicPatternFromColumnExpression() throws Exception {
    // Category: not stateless — expression involves PREFIX_COL column.
    // PREFIX_COL || '.*' is a concatenation that includes a column reference,
    // so the expression is not stateless. Pattern is built per-row at evaluate time.
    String dynTable = createDynamicPatternTable();
    Connection conn = DriverManager.getConnection(getUrl());
    ResultSet rs = conn.createStatement().executeQuery(
      "SELECT ID FROM " + dynTable + " WHERE REGEXP_LIKE(VAL, PREFIX_COL || '.*') ORDER BY ID");
    assertTrue(rs.next());
    assertEquals("id0", rs.getString(1)); // "Hello" || ".*" → "Hello.*" matches "Hello World"
    assertTrue(rs.next());
    assertEquals("id1", rs.getString(1)); // "hello" || ".*" → "hello.*" matches "hello world"
    assertTrue(rs.next());
    assertEquals("id2", rs.getString(1)); // "Report" || ".*" → "Report.*" matches "Report123"
    assertTrue(rs.next());
    assertEquals("id3", rs.getString(1)); // "Test" || ".*" → "Test.*" matches "Test456"
    // id4: "line1" || ".*" → "line1.*" does NOT match "line1\nline2" (dot doesn't match \n)
    // id5: null prefix → null pattern → excluded
    assertFalse(rs.next());
    conn.close();
  }
}
