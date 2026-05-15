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
import java.sql.ResultSet;
import java.sql.SQLException;
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
    ResultSet rs = c.createStatement().executeQuery(
        "SELECT COUNT(*) FROM " + index);
    rs.next();
    return rs.getLong(1);
  }

  private String createBaseTable(Connection conn, String table) throws SQLException {
    conn.createStatement().execute(
        "CREATE TABLE " + table + " (pk VARCHAR PRIMARY KEY, regular VARCHAR) "
            + "COLUMN_ENCODED_BYTES=0");
    return table;
  }

  @Test
  public void upsertWithColumnPopulatesIndex() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection c = conn()) {
      createBaseTable(c, table);
      c.createStatement().execute(
          "CREATE INDEX " + index + " ON " + table + " (extra VARCHAR DYNAMIC)");

      c.prepareStatement(
          "UPSERT INTO " + table + " (pk, extra) VALUES ('row1', 'hello')").executeUpdate();
      c.prepareStatement(
          "UPSERT INTO " + table + " (pk, extra) VALUES ('row2', 'world')").executeUpdate();
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
      c.createStatement().execute(
          "CREATE INDEX " + index + " ON " + table + " (extra VARCHAR DYNAMIC)");

      c.prepareStatement(
          "UPSERT INTO " + table + " (pk, regular) VALUES ('row1', 'r1')").executeUpdate();
      c.prepareStatement(
          "UPSERT INTO " + table + " (pk, regular) VALUES ('row2', 'r2')").executeUpdate();
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
      c.createStatement().execute(
          "CREATE INDEX " + index + " ON " + table + " (extra VARCHAR DYNAMIC)");

      c.prepareStatement(
          "UPSERT INTO " + table + " (pk, extra) VALUES ('a', '1')").executeUpdate();
      c.prepareStatement(
          "UPSERT INTO " + table + " (pk, extra) VALUES ('b', '2')").executeUpdate();
      c.prepareStatement(
          "UPSERT INTO " + table + " (pk, regular) VALUES ('c', 'r')").executeUpdate();
      c.prepareStatement(
          "UPSERT INTO " + table + " (pk, extra) VALUES ('d', '3')").executeUpdate();
      c.prepareStatement(
          "UPSERT INTO " + table + " (pk, regular) VALUES ('e', 'r')").executeUpdate();
      c.commit();

      assertEquals(3L, countIndexRows(c, index));
    }
  }

  @Test
  public void rejectsConflictingTypeAtUpsert() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection c = conn()) {
      createBaseTable(c, table);
      c.createStatement().execute(
          "CREATE INDEX " + index + " ON " + table + " (extra VARCHAR DYNAMIC)");

      try {
        c.prepareStatement(
            "UPSERT INTO " + table + " (pk, extra INTEGER) VALUES ('row1', 42)").executeUpdate();
        fail("expected PHOENIX_DYNAMIC_TYPE_CONFLICT");
      } catch (SQLException ex) {
        assertEquals(
            SQLExceptionCode.PHOENIX_DYNAMIC_TYPE_CONFLICT.getErrorCode(),
            ex.getErrorCode());
      }
    }
  }

  @Test
  public void deleteRowRemovesIndexEntry() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection c = conn()) {
      createBaseTable(c, table);
      c.createStatement().execute(
          "CREATE INDEX " + index + " ON " + table + " (extra VARCHAR DYNAMIC)");
      c.prepareStatement(
          "UPSERT INTO " + table + " (pk, extra) VALUES ('a', '1')").executeUpdate();
      c.prepareStatement(
          "UPSERT INTO " + table + " (pk, extra) VALUES ('b', '2')").executeUpdate();
      c.commit();
      assertEquals(2L, countIndexRows(c, index));

      c.createStatement().execute("DELETE FROM " + table + " WHERE pk='a'");
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
      c.createStatement().execute(
          "CREATE INDEX " + index + " ON " + table + " (extra VARCHAR DYNAMIC)");
      c.prepareStatement(
          "UPSERT INTO " + table + " (pk, extra) VALUES ('a', 'v1')").executeUpdate();
      c.commit();

      c.prepareStatement(
          "UPSERT INTO " + table + " (pk, extra) VALUES ('a', 'v2')").executeUpdate();
      c.commit();

      ResultSet rs = c.createStatement().executeQuery(
          "SELECT pk FROM " + table + " WHERE extra='v2'");
      assertTrue(rs.next());
      assertEquals("a", rs.getString(1));
      assertFalse(rs.next());

      rs = c.createStatement().executeQuery(
          "SELECT pk FROM " + table + " WHERE extra='v1'");
      assertFalse("old index entry must be gone after update", rs.next());
    }
  }
}
