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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class DynamicColumnIndexCreationIT extends ParallelStatsDisabledIT {

  private Connection conn() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    return DriverManager.getConnection(getUrl(), props);
  }

  private String createBaseTable(Connection conn, String table) throws SQLException {
    conn.createStatement().execute(
        "CREATE TABLE " + table + " (pk VARCHAR PRIMARY KEY, regular VARCHAR) "
            + "COLUMN_ENCODED_BYTES=0");
    return table;
  }

  @Test
  public void createsIndexAndPromotesColumn() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection c = conn()) {
      createBaseTable(c, table);
      c.createStatement().execute(
          "CREATE INDEX " + index + " ON " + table + " (extra VARCHAR DYNAMIC)");

      PhoenixConnection pc = c.unwrap(PhoenixConnection.class);
      PTable t = pc.getTable(SchemaUtil.normalizeIdentifier(table));

      PColumn extra = null;
      for (PColumn col : t.getColumns()) {
        if (col.getName().getString().equals("EXTRA")) {
          extra = col;
          break;
        }
      }
      assertNotNull("EXTRA must be promoted into base table catalog", extra);
      assertTrue("promoted column must be virtual", extra.isVirtual());
      assertFalse("promoted column must NOT be marked dynamic-runtime", extra.isDynamic());
    }
  }

  @Test
  public void selectStarSkipsPromotedColumn() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection c = conn()) {
      createBaseTable(c, table);
      c.createStatement().execute(
          "CREATE INDEX " + index + " ON " + table + " (extra VARCHAR DYNAMIC)");
      ResultSet rs = c.createStatement().executeQuery("SELECT * FROM " + table);
      ResultSetMetaData md = rs.getMetaData();
      assertEquals(2, md.getColumnCount());
      assertEquals("PK", md.getColumnName(1));
      assertEquals("REGULAR", md.getColumnName(2));
    }
  }

  @Test
  public void explicitReferenceResolvesPromotedColumn() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection c = conn()) {
      createBaseTable(c, table);
      c.createStatement().execute(
          "CREATE INDEX " + index + " ON " + table + " (extra VARCHAR DYNAMIC)");
      // No rows yet — but must compile without "column not found".
      c.prepareStatement("SELECT extra FROM " + table).executeQuery().close();
      c.prepareStatement("SELECT extra FROM " + table + " WHERE extra='x'")
          .executeQuery().close();
    }
  }

  @Test
  public void rejectsDynamicWithoutType() throws Exception {
    String table = generateUniqueName();
    try (Connection c = conn()) {
      createBaseTable(c, table);
      try {
        c.createStatement().execute(
            "CREATE INDEX " + generateUniqueName() + " ON " + table + " (extra DYNAMIC)");
        fail("expected SQLException");
      } catch (SQLException ex) {
        // Either the grammar threw, or our DYNAMIC_INDEX_REQUIRES_TYPE fires.
        // Either way we just want a SQLException, not a successful CREATE.
      }
    }
  }

  @Test
  public void rejectsConflictWithRegularColumn() throws Exception {
    String table = generateUniqueName();
    try (Connection c = conn()) {
      c.createStatement().execute(
          "CREATE TABLE " + table + " (pk VARCHAR PRIMARY KEY, extra VARCHAR) "
              + "COLUMN_ENCODED_BYTES=0");
      try {
        c.createStatement().execute(
            "CREATE INDEX " + generateUniqueName() + " ON " + table + " (extra VARCHAR DYNAMIC)");
        fail("expected SQLException");
      } catch (SQLException ex) {
        assertEquals(
            SQLExceptionCode.DYNAMIC_INDEX_NAME_CONFLICTS_WITH_REGULAR_COLUMN.getErrorCode(),
            ex.getErrorCode());
      }
    }
  }

  @Test
  public void rejectsOnEncodedTable() throws Exception {
    String table = generateUniqueName();
    try (Connection c = conn()) {
      c.createStatement().execute(
          "CREATE TABLE " + table + " (pk VARCHAR PRIMARY KEY, regular VARCHAR) "
              + "COLUMN_ENCODED_BYTES=2");
      try {
        c.createStatement().execute(
            "CREATE INDEX " + generateUniqueName() + " ON " + table + " (extra VARCHAR DYNAMIC)");
        fail("expected SQLException");
      } catch (SQLException ex) {
        assertEquals(
            SQLExceptionCode.DYNAMIC_INDEX_NOT_ALLOWED_ON_ENCODED_TABLE.getErrorCode(),
            ex.getErrorCode());
      }
    }
  }

  @Test
  public void failedCreateIndexUnpromotesColumn() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection c = conn()) {
      createBaseTable(c, table);
      // First CREATE INDEX succeeds.
      c.createStatement().execute(
          "CREATE INDEX " + index + " ON " + table + " (extra VARCHAR DYNAMIC)");
      // Second CREATE with the SAME index name must fail (duplicate).
      try {
        c.createStatement().execute(
            "CREATE INDEX " + index + " ON " + table + " (other VARCHAR DYNAMIC)");
        fail("expected SQLException for duplicate index name");
      } catch (SQLException ex) {
        // Expected — index name conflict.
      }
      // Verify "other" was un-promoted (rolled back).
      PhoenixConnection pc = c.unwrap(PhoenixConnection.class);
      PTable t = pc.getTable(SchemaUtil.normalizeIdentifier(table));
      boolean otherFound = false;
      for (PColumn col : t.getColumns()) {
        if (col.getName().getString().equals("OTHER")) { otherFound = true; break; }
      }
      assertFalse("'other' must be unpromoted on failed CREATE INDEX", otherFound);
      // Verify "extra" (from successful first call) is still there.
      boolean extraFound = false;
      for (PColumn col : t.getColumns()) {
        if (col.getName().getString().equals("EXTRA")) { extraFound = true; break; }
      }
      assertTrue("'extra' from first CREATE INDEX must persist", extraFound);
    }
  }

  @Test
  public void multipleDynamicColumnsInOneIndex() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection c = conn()) {
      createBaseTable(c, table);
      c.createStatement().execute(
          "CREATE INDEX " + index + " ON " + table + " (a VARCHAR DYNAMIC, b BIGINT DYNAMIC)");
      PhoenixConnection pc = c.unwrap(PhoenixConnection.class);
      PTable t = pc.getTable(SchemaUtil.normalizeIdentifier(table));
      int virtualCount = 0;
      for (PColumn col : t.getColumns()) {
        if (col.isVirtual()) virtualCount++;
      }
      assertEquals(2, virtualCount);
    }
  }
}
