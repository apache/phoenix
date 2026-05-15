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

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.monitoring.DynamicColumnIndexMetrics;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertNotNull;

@Category(ParallelStatsDisabledTest.class)
public class DynamicColumnIndexDropIT extends ParallelStatsDisabledIT {

  @Before
  public void resetMetrics() {
    DynamicColumnIndexMetrics.resetForTesting();
  }

  private Connection conn() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    return DriverManager.getConnection(getUrl(), props);
  }

  private boolean hasColumn(Connection c, String table, String col) throws Exception {
    PhoenixConnection pc = c.unwrap(PhoenixConnection.class);
    // clearCache() forces a fresh metadata fetch — the same connection
    // that ran DROP INDEX has the parent's cached PTable invalidated via
    // LastDDLTimestamp, but our hasColumn lookup races the invalidation
    // in the test JVM. Production code observes the invalidation through
    // MetaDataClient's normal refresh path.
    pc.getQueryServices().clearCache();
    PTable t = pc.getTable(SchemaUtil.normalizeIdentifier(table));
    try {
      PColumn pc2 = t.getColumnForColumnName(SchemaUtil.normalizeIdentifier(col));
      return pc2 != null;
    } catch (ColumnNotFoundException e) {
      return false;
    }
  }

  @Test
  public void singleIndexDropUnpromotesColumn() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection c = conn()) {
      c.createStatement().execute(
        "CREATE TABLE " + table + " (pk VARCHAR PRIMARY KEY, regular VARCHAR) "
          + "COLUMN_ENCODED_BYTES=0");
      c.createStatement().execute(
        "CREATE INDEX " + index + " ON " + table + " (extra VARCHAR DYNAMIC)");
      assertTrue(hasColumn(c, table, "extra"));
      assertEquals(1, DynamicColumnIndexMetrics.getPromotions());

      c.createStatement().execute("DROP INDEX " + index + " ON " + table);
      assertFalse("virtual column must be removed", hasColumn(c, table, "extra"));
      assertEquals(1, DynamicColumnIndexMetrics.getUnpromotions());
    }
  }

  @Test
  public void twoIndexesDropOnePersists() throws Exception {
    String table = generateUniqueName();
    String i1 = generateUniqueName();
    String i2 = generateUniqueName();
    try (Connection c = conn()) {
      c.createStatement().execute(
        "CREATE TABLE " + table + " (pk VARCHAR PRIMARY KEY, regular VARCHAR) "
          + "COLUMN_ENCODED_BYTES=0");
      c.createStatement().execute(
        "CREATE INDEX " + i1 + " ON " + table + " (extra VARCHAR DYNAMIC)");
      c.createStatement().execute(
        "CREATE INDEX " + i2 + " ON " + table + " (extra VARCHAR DYNAMIC, regular)");
      // Phase 1's name-conflict guard rejects re-promotion if extra exists,
      // so the second index reuses the existing virtual column.
      assertEquals("only one promotion should have happened", 1,
        DynamicColumnIndexMetrics.getPromotions());

      c.createStatement().execute("DROP INDEX " + i1 + " ON " + table);
      assertTrue("extra still referenced by " + i2 + ", must persist",
        hasColumn(c, table, "extra"));
      assertEquals(0, DynamicColumnIndexMetrics.getUnpromotions());

      c.createStatement().execute("DROP INDEX " + i2 + " ON " + table);
      assertFalse("now orphaned, must be unpromoted",
        hasColumn(c, table, "extra"));
      assertEquals(1, DynamicColumnIndexMetrics.getUnpromotions());
    }
  }

  @Test
  public void concurrentCreateAfterDropPromotesFresh() throws Exception {
    // Simulate the drop-then-create race using two connections sequenced
    // explicitly. Conn A creates an index (promotes EXTRA virtual), drops
    // the index (un-promotes EXTRA). Conn B — which had a stale view of the
    // table — then creates a new index with the same column. After we clear
    // B's metadata cache (production observes invalidation via
    // LastDDLTimestamp), B must promote a fresh virtual column rather than
    // silently reusing the dropped one.
    String table = generateUniqueName();
    String i1 = generateUniqueName();
    String i2 = generateUniqueName();
    try (Connection c1 = conn(); Connection c2 = conn()) {
      c1.createStatement().execute(
        "CREATE TABLE " + table + " (pk VARCHAR PRIMARY KEY, regular VARCHAR) "
          + "COLUMN_ENCODED_BYTES=0");
      c1.createStatement().execute(
        "CREATE INDEX " + i1 + " ON " + table + " (extra VARCHAR DYNAMIC)");
      // Prime c2's cache with the post-create view.
      c2.unwrap(PhoenixConnection.class).getQueryServices().clearCache();
      assertTrue(hasColumn(c2, table, "extra"));

      c1.createStatement().execute("DROP INDEX " + i1 + " ON " + table);
      assertFalse(hasColumn(c1, table, "extra"));

      // Do NOT clear c2's cache — this is the race window: c2 still believes
      // EXTRA exists as a virtual column (silent-skip branch on the type-match
      // path). The promote logic must re-validate against the latest catalog
      // state and either promote fresh or error out — it must not silently
      // build i2 over a phantom column.
      c2.createStatement().execute(
        "CREATE INDEX " + i2 + " ON " + table + " (extra VARCHAR DYNAMIC)");
      assertTrue(hasColumn(c2, table, "extra"));

      // The new column must be a fresh promotion — virtual on the parent.
      PhoenixConnection pc = c2.unwrap(PhoenixConnection.class);
      pc.getQueryServices().clearCache();
      PTable t = pc.getTable(SchemaUtil.normalizeIdentifier(table));
      PColumn extra = null;
      for (PColumn col : t.getColumns()) {
        if (col.getName().getString().equals("EXTRA")) {
          extra = col;
          break;
        }
      }
      assertNotNull("EXTRA must exist after fresh promotion", extra);
      assertTrue("EXTRA must be virtual after fresh promotion", extra.isVirtual());
    }
  }

  @Test
  public void droppingNonDynamicIndexDoesNotTouchVirtual() throws Exception {
    String table = generateUniqueName();
    String i1 = generateUniqueName();
    String i2 = generateUniqueName();
    try (Connection c = conn()) {
      c.createStatement().execute(
        "CREATE TABLE " + table + " (pk VARCHAR PRIMARY KEY, regular VARCHAR) "
          + "COLUMN_ENCODED_BYTES=0");
      c.createStatement().execute(
        "CREATE INDEX " + i1 + " ON " + table + " (extra VARCHAR DYNAMIC)");
      c.createStatement().execute(
        "CREATE INDEX " + i2 + " ON " + table + " (regular)");
      c.createStatement().execute("DROP INDEX " + i2 + " ON " + table);
      assertTrue(hasColumn(c, table, "extra"));
      assertEquals(0, DynamicColumnIndexMetrics.getUnpromotions());
    }
  }
}
