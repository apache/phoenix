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
 */
package org.apache.phoenix.end2end.index.dyncol;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class DynamicColumnIndexWriteIT extends ParallelStatsDisabledIT {

  private Connection conn() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    return DriverManager.getConnection(getUrl(), props);
  }

  private long countIndexRows(Connection c, String index) throws SQLException {
    String sql = "SELECT /*+ NO_INDEX */ COUNT(*) FROM " + index;
    try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
      rs.next();
      return rs.getLong(1);
    }
  }

  private String createBaseTable(Connection conn, String table) throws SQLException {
    try (Statement s = conn.createStatement()) {
      s.execute(
          "CREATE TABLE " + table + " (pk VARCHAR PRIMARY KEY, regular VARCHAR) "
              + "COLUMN_ENCODED_BYTES=0");
    }
    return table;
  }

  private static void exec(Connection c, String sql) throws SQLException {
    try (Statement s = c.createStatement()) {
      s.execute(sql);
    }
  }

  private static void upsert(Connection c, String sql) throws SQLException {
    try (PreparedStatement ps = c.prepareStatement(sql)) {
      ps.executeUpdate();
    }
  }

  @Test
  public void upsertWithColumnPopulatesIndex() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection c = conn()) {
      createBaseTable(c, table);
      exec(c, "CREATE INDEX " + index + " ON " + table + " (extra VARCHAR DYNAMIC)");

      upsert(c, "UPSERT INTO " + table + " (pk, extra) VALUES ('row1', 'hello')");
      upsert(c, "UPSERT INTO " + table + " (pk, extra) VALUES ('row2', 'world')");
      c.commit();

      assertEquals(2L, countIndexRows(c, index));
    }
  }

  @Test
  public void upsertWithoutColumnSkipsIndex() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection c = conn()) {
      createBaseTable(c, table);
      exec(c, "CREATE INDEX " + index + " ON " + table + " (extra VARCHAR DYNAMIC)");

      upsert(c, "UPSERT INTO " + table + " (pk, regular) VALUES ('row1', 'r1')");
      upsert(c, "UPSERT INTO " + table + " (pk, regular) VALUES ('row2', 'r2')");
      c.commit();

      assertEquals("rows omitting the indexed column must be sparse-skipped",
          0L, countIndexRows(c, index));
    }
  }

  @Test
  public void mixedSparseAndPopulatedRows() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection c = conn()) {
      createBaseTable(c, table);
      exec(c, "CREATE INDEX " + index + " ON " + table + " (extra VARCHAR DYNAMIC)");

      upsert(c, "UPSERT INTO " + table + " (pk, extra) VALUES ('a', '1')");
      upsert(c, "UPSERT INTO " + table + " (pk, extra) VALUES ('b', '2')");
      upsert(c, "UPSERT INTO " + table + " (pk, regular) VALUES ('c', 'r')");
      upsert(c, "UPSERT INTO " + table + " (pk, extra) VALUES ('d', '3')");
      upsert(c, "UPSERT INTO " + table + " (pk, regular) VALUES ('e', 'r')");
      c.commit();

      assertEquals(3L, countIndexRows(c, index));
    }
  }

  @Test
  public void upsertWithInlineMatchingTypeSucceeds() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection c = conn()) {
      exec(c, "CREATE TABLE " + table + " (pk VARCHAR PRIMARY KEY, regular VARCHAR) "
          + "COLUMN_ENCODED_BYTES=0");
      exec(c, "CREATE INDEX " + index + " ON " + table + " (extra VARCHAR DYNAMIC)");

      // Inline (extra VARCHAR) matches the registered virtual VARCHAR — must succeed.
      upsert(c, "UPSERT INTO " + table + " (pk, extra VARCHAR) VALUES ('row1', 'hello')");
      c.commit();

      assertEquals(1L, countIndexRows(c, index));
    }
  }

  @Test
  public void rejectsConflictingTypeAtUpsert() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection c = conn()) {
      createBaseTable(c, table);
      exec(c, "CREATE INDEX " + index + " ON " + table + " (extra VARCHAR DYNAMIC)");

      try (PreparedStatement ps = c.prepareStatement(
          "UPSERT INTO " + table + " (pk, extra INTEGER) VALUES ('row1', 42)")) {
        ps.executeUpdate();
        fail("expected PHOENIX_DYNAMIC_TYPE_CONFLICT");
      } catch (SQLException ex) {
        assertEquals(
            SQLExceptionCode.PHOENIX_DYNAMIC_TYPE_CONFLICT.getErrorCode(),
            ex.getErrorCode());
      }
    }
  }

  @Test
  public void sparseSkipDistinguishesAbsentFromExplicit() throws Exception {
    // Sparse-skip must fire only when the cell is genuinely absent. The previous
    // implementation also fired on ptr.getLength()==0, which conflated absent
    // cells with explicit length-0 values. Phoenix's PDataType treats empty
    // VARCHAR as null at the type level, so a present-empty VARCHAR is not
    // observable here; this test pins the behavior we DO want — a row that
    // upserts a non-empty value gets indexed, a row that omits the column does
    // not.
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection c = conn()) {
      c.createStatement().execute(
          "CREATE TABLE " + table + " (pk VARCHAR PRIMARY KEY, regular VARCHAR) "
              + "COLUMN_ENCODED_BYTES=0");
      c.createStatement().execute(
          "CREATE INDEX " + index + " ON " + table + " (extra VARCHAR DYNAMIC)");

      try (PreparedStatement ps = c.prepareStatement(
          "UPSERT INTO " + table + " (pk, extra) VALUES ('row1', 'x')")) {
        ps.executeUpdate();
      }
      try (PreparedStatement ps = c.prepareStatement(
          "UPSERT INTO " + table + " (pk, regular) VALUES ('row2', 'r2')")) {
        ps.executeUpdate();
      }
      c.commit();

      assertEquals(1L, countIndexRows(c, index));

      try (PreparedStatement ps = c.prepareStatement(
          "SELECT pk FROM " + table + " WHERE extra = 'x'");
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("row1", rs.getString(1));
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void deleteRowRemovesIndexEntry() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection c = conn()) {
      createBaseTable(c, table);
      exec(c, "CREATE INDEX " + index + " ON " + table + " (extra VARCHAR DYNAMIC)");
      upsert(c, "UPSERT INTO " + table + " (pk, extra) VALUES ('a', '1')");
      upsert(c, "UPSERT INTO " + table + " (pk, extra) VALUES ('b', '2')");
      c.commit();
      assertEquals(2L, countIndexRows(c, index));

      exec(c, "DELETE FROM " + table + " WHERE pk='a'");
      c.commit();
      assertEquals(1L, countIndexRows(c, index));
    }
  }

  @Test
  public void updateOfExtraReplacesIndexEntry() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection c = conn()) {
      createBaseTable(c, table);
      exec(c, "CREATE INDEX " + index + " ON " + table + " (extra VARCHAR DYNAMIC)");
      upsert(c, "UPSERT INTO " + table + " (pk, extra) VALUES ('a', 'v1')");
      c.commit();

      upsert(c, "UPSERT INTO " + table + " (pk, extra) VALUES ('a', 'v2')");
      c.commit();

      try (Statement s = c.createStatement();
          ResultSet rs = s.executeQuery(
              "SELECT pk FROM " + table + " WHERE extra='v2'")) {
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertFalse(rs.next());
      }

      try (Statement s = c.createStatement();
          ResultSet rs = s.executeQuery(
              "SELECT pk FROM " + table + " WHERE extra='v1'")) {
        assertFalse("old index entry must be gone after update", rs.next());
      }
    }
  }

  @Test
  public void selectStarWithIndexHintExcludesVirtualColumn() throws Exception {
    // SELECT * must never project virtual columns, even when the optimizer
    // routes the query through an index that covers the virtual column.
    // This pins behavior for the projectAllIndexColumns path.
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection c = conn()) {
      exec(c, "CREATE TABLE " + table + " (pk VARCHAR PRIMARY KEY, regular VARCHAR) "
          + "COLUMN_ENCODED_BYTES=0");
      exec(c, "CREATE INDEX " + index + " ON " + table + " (extra VARCHAR DYNAMIC) "
          + "INCLUDE (regular)");
      upsert(c,
          "UPSERT INTO " + table + " (pk, extra, regular) VALUES ('row1', 'x', 'r1')");
      c.commit();

      // Force the query through the index so projectAllIndexColumns is exercised
      // (the wildcard-rewrite path: SELECT * on a base table, optimizer/hint
      // routes the rewritten WildcardParseNode through the INDEX type table).
      try (PreparedStatement ps = c.prepareStatement(
          "SELECT /*+ INDEX(" + table + " " + index + ") */ * FROM " + table
              + " WHERE extra = 'x'");
          ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        ResultSetMetaData md = rs.getMetaData();
        // EXTRA is virtual and must be absent. PK + REGULAR remain.
        assertEquals(2, md.getColumnCount());
        for (int i = 1; i <= md.getColumnCount(); i++) {
          assertFalse("EXTRA must not appear in SELECT *",
              "EXTRA".equalsIgnoreCase(md.getColumnName(i)));
        }
        assertFalse(rs.next());
      }
    }
  }
}
