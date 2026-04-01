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
}
