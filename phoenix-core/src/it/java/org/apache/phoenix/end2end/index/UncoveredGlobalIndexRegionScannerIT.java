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

import static org.apache.phoenix.end2end.index.GlobalIndexCheckerIT.assertExplainPlan;
import static org.apache.phoenix.end2end.index.GlobalIndexCheckerIT.assertExplainPlanWithLimit;
import static org.apache.phoenix.query.explain.ExplainPlanTestUtil.assertPlan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.exception.PhoenixParserException;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.optimize.OptimizerReasons;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.EnvironmentEdge;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class UncoveredGlobalIndexRegionScannerIT extends BaseTest {
  private static final long AGE_THRESHOLD_MS = 60_000;
  // Keep stale entries live in HBase and in Phoenix while testing the independent repair threshold.
  private static final int TTL_SECONDS = 3600;
  private static final int ROW_COUNT = 20;
  private static final long UPDATED_TIME_BASE = 1_700_000_000_000L;
  private final boolean uncovered;
  private final boolean salted;

  public UncoveredGlobalIndexRegionScannerIT(boolean uncovered, boolean salted) {
    this.uncovered = uncovered;
    this.salted = salted;
  }

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
    props.put(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB, Long.toString(0));
    setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
  }

  @After
  public void unsetFailForTesting() throws Exception {
    boolean refCountLeaked = isAnyStoreRefCountLeaked();
    assertFalse("refCount leaked", refCountLeaked);
  }

  @Parameterized.Parameters(name = "uncovered={0},salted={1}")
  public static synchronized Collection<Boolean[]> data() {
    return Arrays.asList(
      new Boolean[][] { { false, false }, { false, true }, { true, false }, { true, true } });
  }

  private void populateTable(String tableName) throws Exception {
    Connection conn = DriverManager.getConnection(getUrl());
    conn.createStatement()
      .execute("create table " + tableName
        + " (id varchar(10) not null primary key, val1 varchar(10), val2 varchar(10),"
        + " val3 varchar(10))" + (salted ? " SALT_BUCKETS=4" : ""));
    conn.createStatement()
      .execute("upsert into " + tableName + " values ('a', 'ab', 'abc', 'abcd')");
    conn.commit();
    conn.createStatement()
      .execute("upsert into " + tableName + " values ('b', 'bc', 'bcd', 'bcde')");
    conn.commit();
    conn.close();
  }

  @Test
  public void testDDL() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String dataTableName = generateUniqueName();
      String indexTableName = generateUniqueName();
      conn.createStatement()
        .execute("create table " + dataTableName
          + " (id varchar(10) not null primary key, val1 varchar(10), val2 varchar(10),"
          + " val3 varchar(10))" + (salted ? " SALT_BUCKETS=4" : ""));
      if (uncovered) {
        // The INCLUDE clause should not be allowed
        try {
          conn.createStatement().execute("CREATE UNCOVERED INDEX " + indexTableName + " on "
            + dataTableName + " (val1) INCLUDE (val2)");
          Assert.fail();
        } catch (PhoenixParserException e) {
          // Expected
        }
        // The LOCAL keyword should not be allowed with UNCOVERED
        try {
          conn.createStatement()
            .execute("CREATE UNCOVERED LOCAL INDEX " + indexTableName + " on " + dataTableName);
          Assert.fail();
        } catch (PhoenixParserException e) {
          // Expected
        }
      } else {
        // The INCLUDE clause should be allowed
        conn.createStatement().execute(
          "CREATE INDEX " + indexTableName + " on " + dataTableName + " (val1) INCLUDE (val2)");
      }
    }
  }

  @Test
  public void testDDLWithPhoenixRowTimestamp() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String dataTableName = generateUniqueName();
      conn.createStatement().execute("create table " + dataTableName
        + " (id varchar(10) not null primary key)" + (salted ? " SALT_BUCKETS=4" : ""));
      if (uncovered) {
        conn.createStatement().execute("CREATE UNCOVERED INDEX IDX_" + dataTableName + " on "
          + dataTableName + " (PHOENIX_ROW_TIMESTAMP())");
      } else {
        conn.createStatement().execute("CREATE INDEX IDX_" + dataTableName + " on " + dataTableName
          + " (PHOENIX_ROW_TIMESTAMP())");
        conn.createStatement().execute("CREATE LOCAL INDEX IDX_LOCAL_" + dataTableName + " on "
          + dataTableName + " (PHOENIX_ROW_TIMESTAMP())");
      }
    }
  }

  @Test
  public void testUncoveredQueryWithPhoenixRowTimestamp() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String dataTableName = generateUniqueName();
      String indexTableName = generateUniqueName();
      Timestamp initial = new Timestamp(EnvironmentEdgeManager.currentTimeMillis() - 1);
      conn.createStatement()
        .execute("create table " + dataTableName
          + " (id varchar(10) not null primary key, val1 varchar(10), val2 varchar(10), "
          + " val3 varchar(10))" + (salted ? " SALT_BUCKETS=4" : ""));
      conn.createStatement()
        .execute("upsert into " + dataTableName + " values ('a', 'ab', 'abc', 'abcd')");
      conn.commit();
      Timestamp before = new Timestamp(EnvironmentEdgeManager.currentTimeMillis());
      // Sleep 1ms to get a different row timestamps
      Thread.sleep(1);
      conn.createStatement()
        .execute("upsert into " + dataTableName + " values ('b', 'bc', 'bcd', 'bcde')");
      conn.commit();
      Timestamp after = new Timestamp(EnvironmentEdgeManager.currentTimeMillis() + 1);
      conn.createStatement().execute("CREATE " + (uncovered ? "UNCOVERED " : " ") + "INDEX "
        + indexTableName + " on " + dataTableName + " (val1, PHOENIX_ROW_TIMESTAMP()) ");

      String timeZoneID = Calendar.getInstance().getTimeZone().getID();
      // Write a query to get the val2 = 'bc' with a time range query
      String query =
        "SELECT" + (uncovered ? " " : "/*+ INDEX(" + dataTableName + " " + indexTableName + ")*/ ")
          + "val1, val2, PHOENIX_ROW_TIMESTAMP(), val3 from " + dataTableName
          + " WHERE val1 = 'bc' AND " + "PHOENIX_ROW_TIMESTAMP() > TO_DATE('" + before.toString()
          + "','yyyy-MM-dd HH:mm:ss.SSS', '" + timeZoneID + "') AND "
          + "PHOENIX_ROW_TIMESTAMP() < TO_DATE('" + after + "','yyyy-MM-dd HH:mm:ss.SSS', '"
          + timeZoneID + "')";
      // Verify that we will read from the index table
      assertIndexPlan(conn, query, dataTableName, indexTableName, "PHOENIX_ROW_TIMESTAMP()");
      ResultSet rs = conn.createStatement().executeQuery(query);
      assertTrue(rs.next());
      assertEquals("bc", rs.getString(1));
      assertEquals("bcd", rs.getString(2));
      assertTrue(rs.getTimestamp(3).after(before));
      assertTrue(rs.getTimestamp(3).before(after));
      assertEquals("bcde", rs.getString(4));
      assertFalse(rs.next());
      // Count the number of index rows
      rs = conn.createStatement().executeQuery("SELECT COUNT(*) from " + indexTableName);
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
      // Add one more row with val2 ='bc' and check this does not change the result of the previous
      // query
      // Sleep 1ms to get a different row timestamps
      Thread.sleep(1);
      conn.createStatement()
        .execute("upsert into " + dataTableName + " values ('c', 'bc', 'ccc', 'cccc')");
      conn.commit();
      assertIndexPlan(conn, query, dataTableName, indexTableName, "PHOENIX_ROW_TIMESTAMP()");
      rs = conn.createStatement().executeQuery(query);
      assertTrue(rs.next());
      assertEquals("bc", rs.getString(1));
      assertEquals("bcd", rs.getString(2));
      assertTrue(rs.getTimestamp(3).after(before));
      assertTrue(rs.getTimestamp(3).before(after));
      assertEquals("bcde", rs.getString(4));
      assertFalse(rs.next());
      // Write a time range query to get the last row with val2 ='bc'
      query =
        "SELECT" + (uncovered ? " " : "/*+ INDEX(" + dataTableName + " " + indexTableName + ")*/ ")
          + "val1, val2, PHOENIX_ROW_TIMESTAMP(), val3 from " + dataTableName
          + " WHERE val1 = 'bc' AND " + "PHOENIX_ROW_TIMESTAMP() > TO_DATE('" + after
          + "','yyyy-MM-dd HH:mm:ss.SSS', '" + timeZoneID + "')";
      // Verify that we will read from the index table
      assertIndexPlan(conn, query, dataTableName, indexTableName, "PHOENIX_ROW_TIMESTAMP()");
      rs = conn.createStatement().executeQuery(query);
      assertTrue(rs.next());
      assertEquals("bc", rs.getString(1));
      assertEquals("ccc", rs.getString(2));
      assertTrue(rs.getTimestamp(3).after(after));
      assertEquals("cccc", rs.getString(4));
      assertFalse(rs.next());
      // Verify that we can execute the same query without using the index
      String noIndexQuery = "SELECT /*+ NO_INDEX */ val1, val2, PHOENIX_ROW_TIMESTAMP() from "
        + dataTableName + " WHERE val1 = 'bc' AND " + "PHOENIX_ROW_TIMESTAMP() > TO_DATE('" + after
        + "','yyyy-MM-dd HH:mm:ss.SSS', '" + timeZoneID + "')";
      // Verify that we will read from the data table
      assertPlan(conn, noIndexQuery).scanType("FULL SCAN").table(dataTableName);
      rs = conn.createStatement().executeQuery(noIndexQuery);
      assertTrue(rs.next());
      assertEquals("bc", rs.getString(1));
      assertEquals("ccc", rs.getString(2));
      assertTrue(rs.getTimestamp(3).after(after));
      after = rs.getTimestamp(3);
      assertFalse(rs.next());
      // Sleep 1ms to get a different row timestamps
      Thread.sleep(1);
      conn.createStatement()
        .execute("upsert into " + dataTableName + " values ('d', 'de', 'def', 'defg')");
      conn.commit();

      query =
        "SELECT" + (uncovered ? " " : "/*+ INDEX(" + dataTableName + " " + indexTableName + ")*/ ")
          + " val1, val2, PHOENIX_ROW_TIMESTAMP()  from " + dataTableName + " WHERE val1 = 'de'";
      // Verify that we will read from the index table
      assertIndexPlan(conn, query, dataTableName, indexTableName, "PHOENIX_ROW_TIMESTAMP()");
      rs = conn.createStatement().executeQuery(query);
      assertTrue(rs.next());
      assertEquals("de", rs.getString(1));
      assertEquals("def", rs.getString(2));
      assertTrue(rs.getTimestamp(3).after(after));
      assertFalse(rs.next());
      conn.createStatement().execute("DROP INDEX " + indexTableName + " on " + dataTableName);
      conn.commit();
      // Add a new index where the index row key starts with PHOENIX_ROW_TIMESTAMP()
      indexTableName = generateUniqueName();
      conn.createStatement().execute("CREATE " + (uncovered ? "UNCOVERED " : " ") + "INDEX "
        + indexTableName + " on " + dataTableName + " (PHOENIX_ROW_TIMESTAMP()) ");
      conn.commit();
      // Add one more row
      // Sleep 1ms to get a different row timestamps
      Thread.sleep(1);
      conn.createStatement()
        .execute("upsert into " + dataTableName + " values ('e', 'ae', 'efg', 'efgh')");
      conn.commit();
      // Write a query to get all the rows in the order of their timestamps
      query =
        "SELECT" + (uncovered ? " " : "/*+ INDEX(" + dataTableName + " " + indexTableName + ")*/ ")
          + " id, val1, val2, PHOENIX_ROW_TIMESTAMP() from " + dataTableName + " WHERE "
          + "PHOENIX_ROW_TIMESTAMP() > TO_DATE('" + initial + "','yyyy-MM-dd HH:mm:ss.SSS', '"
          + timeZoneID + "')";
      // Verify that we will read from the index table
      assertIndexPlan(conn, query, dataTableName, indexTableName, "PHOENIX_ROW_TIMESTAMP()");
      rs = conn.createStatement().executeQuery(query);
      assertTrue(rs.next());
      assertEquals("a", rs.getString(1));
      assertEquals("ab", rs.getString(2));
      assertEquals("abc", rs.getString(3));
      assertTrue(rs.next());
      assertEquals("b", rs.getString(1));
      assertEquals("bc", rs.getString(2));
      assertEquals("bcd", rs.getString(3));
      assertTrue(rs.next());
      assertEquals("c", rs.getString(1));
      assertEquals("bc", rs.getString(2));
      assertEquals("ccc", rs.getString(3));
      assertTrue(rs.next());
      assertEquals("d", rs.getString(1));
      assertEquals("de", rs.getString(2));
      assertEquals("def", rs.getString(3));
      assertTrue(rs.next());
      assertEquals("e", rs.getString(1));
      assertEquals("ae", rs.getString(2));
      assertEquals("efg", rs.getString(3));
      assertFalse(rs.next());

      // Sleep 1ms to get a different row timestamps
      Thread.sleep(1);
      conn.createStatement()
        .execute("upsert into " + dataTableName + " values ('a', 'ab', 'abc', 'abcd')");
      conn.commit();
      rs = conn.createStatement().executeQuery(query);
      assertTrue(rs.next());
      assertEquals("b", rs.getString(1));
      assertEquals("bc", rs.getString(2));
      assertEquals("bcd", rs.getString(3));
      assertTrue(rs.next());
      assertEquals("c", rs.getString(1));
      assertEquals("bc", rs.getString(2));
      assertEquals("ccc", rs.getString(3));
      assertTrue(rs.next());
      assertEquals("d", rs.getString(1));
      assertEquals("de", rs.getString(2));
      assertEquals("def", rs.getString(3));
      assertTrue(rs.next());
      assertEquals("e", rs.getString(1));
      assertEquals("ae", rs.getString(2));
      assertEquals("efg", rs.getString(3));
      assertTrue(rs.next());
      assertEquals("a", rs.getString(1));
      assertEquals("ab", rs.getString(2));
      assertEquals("abc", rs.getString(3));
      assertFalse(rs.next());
    }
  }

  @Test
  public void testUncoveredQueryWithPhoenixRowTimestampAndAllPkCols() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String dataTableName = generateUniqueName();
      String indexTableName = generateUniqueName();
      Timestamp initial = new Timestamp(EnvironmentEdgeManager.currentTimeMillis() - 1);
      conn.createStatement().execute(
        "create table " + dataTableName + " (id varchar(10), val1 varchar(10), val2 varchar(10), "
          + " val3 varchar(10) constraint pk primary key(id, val1, val2, val3))"
          + (salted ? " SALT_BUCKETS=4" : ""));
      conn.createStatement()
        .execute("upsert into " + dataTableName + " values ('a', 'ab', 'abc', 'abcd')");
      conn.commit();
      Timestamp before = new Timestamp(EnvironmentEdgeManager.currentTimeMillis());
      // Sleep 1ms to get a different row timestamps
      Thread.sleep(1);
      conn.createStatement()
        .execute("upsert into " + dataTableName + " values ('b', 'bc', 'bcd', 'bcde')");
      conn.commit();
      Timestamp after = new Timestamp(EnvironmentEdgeManager.currentTimeMillis() + 1);
      conn.createStatement().execute("CREATE " + (uncovered ? "UNCOVERED " : " ") + "INDEX "
        + indexTableName + " on " + dataTableName + " (val1, PHOENIX_ROW_TIMESTAMP()) ");

      String timeZoneID = Calendar.getInstance().getTimeZone().getID();
      // Write a query to get the val2 = 'bc' with a time range query
      String query =
        "SELECT" + (uncovered ? " " : "/*+ INDEX(" + dataTableName + " " + indexTableName + ")*/ ")
          + "val1, val2, PHOENIX_ROW_TIMESTAMP() from " + dataTableName + " WHERE val1 = 'bc' AND "
          + "PHOENIX_ROW_TIMESTAMP() > TO_DATE('" + before.toString()
          + "','yyyy-MM-dd HH:mm:ss.SSS', '" + timeZoneID + "') AND "
          + "PHOENIX_ROW_TIMESTAMP() < TO_DATE('" + after + "','yyyy-MM-dd HH:mm:ss.SSS', '"
          + timeZoneID + "')";
      // Verify that we will read from the index table
      assertIndexPlan(conn, query, dataTableName, indexTableName, "PHOENIX_ROW_TIMESTAMP()");
      ResultSet rs = conn.createStatement().executeQuery(query);
      assertTrue(rs.next());
      assertEquals("bc", rs.getString(1));
      assertEquals("bcd", rs.getString(2));
      assertTrue(rs.getTimestamp(3).after(before));
      assertTrue(rs.getTimestamp(3).before(after));
      assertFalse(rs.next());
      // Count the number of index rows
      rs = conn.createStatement().executeQuery("SELECT COUNT(*) from " + indexTableName);
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
      // Add one more row with val2 ='bc' and check this does not change the result of the previous
      // query
      // Sleep 1ms to get a different row timestamps
      Thread.sleep(1);
      conn.createStatement()
        .execute("upsert into " + dataTableName + " values ('c', 'bc', 'ccc', 'cccc')");
      conn.commit();
      assertIndexPlan(conn, query, dataTableName, indexTableName, "PHOENIX_ROW_TIMESTAMP()");
      rs = conn.createStatement().executeQuery(query);
      assertTrue(rs.next());
      assertEquals("bc", rs.getString(1));
      assertEquals("bcd", rs.getString(2));
      assertTrue(rs.getTimestamp(3).after(before));
      assertTrue(rs.getTimestamp(3).before(after));
      assertFalse(rs.next());
      // Write a time range query to get the last row with val2 ='bc'
      query =
        "SELECT" + (uncovered ? " " : "/*+ INDEX(" + dataTableName + " " + indexTableName + ")*/ ")
          + "val1, val2, PHOENIX_ROW_TIMESTAMP() from " + dataTableName + " WHERE val1 = 'bc' AND "
          + "PHOENIX_ROW_TIMESTAMP() > TO_DATE('" + after + "','yyyy-MM-dd HH:mm:ss.SSS', '"
          + timeZoneID + "')";
      // Verify that we will read from the index table
      assertIndexPlan(conn, query, dataTableName, indexTableName, "PHOENIX_ROW_TIMESTAMP()");
      rs = conn.createStatement().executeQuery(query);
      assertTrue(rs.next());
      assertEquals("bc", rs.getString(1));
      assertEquals("ccc", rs.getString(2));
      assertTrue(rs.getTimestamp(3).after(after));
      assertFalse(rs.next());
      // Verify that we can execute the same query without using the index
      String noIndexQuery = "SELECT /*+ NO_INDEX */ val1, val2, PHOENIX_ROW_TIMESTAMP() from "
        + dataTableName + " WHERE val1 = 'bc' AND " + "PHOENIX_ROW_TIMESTAMP() > TO_DATE('" + after
        + "','yyyy-MM-dd HH:mm:ss.SSS', '" + timeZoneID + "')";
      // Verify that we will read from the data table
      assertPlan(conn, noIndexQuery).scanType(salted ? "RANGE SCAN" : "FULL SCAN")
        .table(dataTableName);
      rs = conn.createStatement().executeQuery(noIndexQuery);
      assertTrue(rs.next());
      assertEquals("bc", rs.getString(1));
      assertEquals("ccc", rs.getString(2));
      assertTrue(rs.getTimestamp(3).after(after));
      after = rs.getTimestamp(3);
      assertFalse(rs.next());
      // Sleep 1ms to get a different row timestamps
      Thread.sleep(1);
      conn.createStatement()
        .execute("upsert into " + dataTableName + " values ('d', 'de', 'def', 'defg')");
      conn.commit();

      query =
        "SELECT" + (uncovered ? " " : "/*+ INDEX(" + dataTableName + " " + indexTableName + ")*/ ")
          + " val1, val2, PHOENIX_ROW_TIMESTAMP()  from " + dataTableName + " WHERE val1 = 'de'";
      // Verify that we will read from the index table
      assertIndexPlan(conn, query, dataTableName, indexTableName, "PHOENIX_ROW_TIMESTAMP()");
      rs = conn.createStatement().executeQuery(query);
      assertTrue(rs.next());
      assertEquals("de", rs.getString(1));
      assertEquals("def", rs.getString(2));
      assertTrue(rs.getTimestamp(3).after(after));
      assertFalse(rs.next());
      conn.createStatement().execute("DROP INDEX " + indexTableName + " on " + dataTableName);
      conn.commit();
      // Add a new index where the index row key starts with PHOENIX_ROW_TIMESTAMP()
      indexTableName = generateUniqueName();
      conn.createStatement().execute("CREATE " + (uncovered ? "UNCOVERED " : " ") + "INDEX "
        + indexTableName + " on " + dataTableName + " (PHOENIX_ROW_TIMESTAMP()) ");
      conn.commit();
      // Add one more row
      // Sleep 1ms to get a different row timestamps
      Thread.sleep(1);
      conn.createStatement()
        .execute("upsert into " + dataTableName + " values ('e', 'ae', 'efg', 'efgh')");
      conn.commit();
      // Write a query to get all the rows in the order of their timestamps
      query =
        "SELECT" + (uncovered ? " " : "/*+ INDEX(" + dataTableName + " " + indexTableName + ")*/ ")
          + " id, val1, val2, PHOENIX_ROW_TIMESTAMP() from " + dataTableName + " WHERE "
          + "PHOENIX_ROW_TIMESTAMP() > TO_DATE('" + initial + "','yyyy-MM-dd HH:mm:ss.SSS', '"
          + timeZoneID + "')";
      // Verify that we will read from the index table
      assertIndexPlan(conn, query, dataTableName, indexTableName, "PHOENIX_ROW_TIMESTAMP()");
      rs = conn.createStatement().executeQuery(query);
      assertTrue(rs.next());
      assertEquals("a", rs.getString(1));
      assertEquals("ab", rs.getString(2));
      assertEquals("abc", rs.getString(3));
      assertTrue(rs.next());
      assertEquals("b", rs.getString(1));
      assertEquals("bc", rs.getString(2));
      assertEquals("bcd", rs.getString(3));
      assertTrue(rs.next());
      assertEquals("c", rs.getString(1));
      assertEquals("bc", rs.getString(2));
      assertEquals("ccc", rs.getString(3));
      assertTrue(rs.next());
      assertEquals("d", rs.getString(1));
      assertEquals("de", rs.getString(2));
      assertEquals("def", rs.getString(3));
      assertTrue(rs.next());
      assertEquals("e", rs.getString(1));
      assertEquals("ae", rs.getString(2));
      assertEquals("efg", rs.getString(3));
      assertFalse(rs.next());

      // Sleep 1ms to get a different row timestamps
      Thread.sleep(1);
      conn.createStatement()
        .execute("upsert into " + dataTableName + " values ('a', 'ab', 'abc', 'abcd')");
      conn.commit();
      rs = conn.createStatement().executeQuery(query);
      assertTrue(rs.next());
      assertEquals("b", rs.getString(1));
      assertEquals("bc", rs.getString(2));
      assertEquals("bcd", rs.getString(3));
      assertTrue(rs.next());
      assertEquals("c", rs.getString(1));
      assertEquals("bc", rs.getString(2));
      assertEquals("ccc", rs.getString(3));
      assertTrue(rs.next());
      assertEquals("d", rs.getString(1));
      assertEquals("de", rs.getString(2));
      assertEquals("def", rs.getString(3));
      assertTrue(rs.next());
      assertEquals("e", rs.getString(1));
      assertEquals("ae", rs.getString(2));
      assertEquals("efg", rs.getString(3));
      assertTrue(rs.next());
      assertEquals("a", rs.getString(1));
      assertEquals("ab", rs.getString(2));
      assertEquals("abc", rs.getString(3));
      assertFalse(rs.next());
    }
  }

  private void assertIndexTableNotSelected(Connection conn, String dataTableName,
    String indexTableName, String sql) throws Exception {
    try {
      assertExplainPlan(conn, sql, dataTableName, indexTableName);
      throw new RuntimeException("The index table should not be selected without an index hint");
    } catch (AssertionError error) {
      // expected
    }
  }

  /**
   * Asserts the query is served by the index, disclosing the optimizer's selection rule (and, for a
   * functional index, the separate {@code matches <expr>}).
   */
  private void assertIndexPlan(Connection conn, String sql, String dataTableName,
    String indexTableName, String functionalExpr) throws SQLException {
    String rule = sql.contains("/*+ INDEX(")
      ? OptimizerReasons.RULE_HINT
      : OptimizerReasons.RULE_MORE_BOUND_PK_COLUMNS;
    if (functionalExpr == null) {
      assertExplainPlan(conn, sql, dataTableName, indexTableName, rule);
    } else {
      assertExplainPlan(conn, sql, dataTableName, indexTableName, rule, functionalExpr);
    }
  }

  @Test
  public void testUncoveredQuery() throws Exception {
    String dataTableName = generateUniqueName();
    populateTable(dataTableName); // with two rows ('a', 'ab', 'abc', 'abcd') and ('b', 'bc', 'bcd',
                                  // 'bcde')
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String indexTableName = generateUniqueName();
      conn.createStatement()
        .execute("CREATE " + (uncovered ? "UNCOVERED " : " ") + "INDEX " + indexTableName + " on "
          + dataTableName + " (val1) " + (uncovered ? "" : "INCLUDE (val2)"));
      String selectSql;
      int limit = 10;
      if (!uncovered) {
        // Verify that without an index hint, a covered index table is not selected
        assertIndexTableNotSelected(conn, dataTableName, indexTableName, "SELECT val3 from "
          + dataTableName + " WHERE val1 = 'bc' AND (val2 = 'bcd' OR val3 ='bcde') LIMIT 10");
        // Verify that with index hint, we will read from the index table
        // even though val3 is not included by the index table
        selectSql = "SELECT /*+ INDEX(" + dataTableName + " " + indexTableName
          + ")*/ val2, val3 from " + dataTableName
          + " WHERE val1 = 'bc' AND (val2 = 'bcd' OR val3 ='bcde') LIMIT " + limit;
        assertExplainPlanWithLimit(conn, selectSql, dataTableName, indexTableName, limit);
      } else {
        // Verify that an index hint is not necessary for an uncovered index
        selectSql = "SELECT  val2, val3 from " + dataTableName
          + " WHERE val1 = 'bc' AND (val2 = 'bcd' OR val3 ='bcde') LIMIT " + limit;
        assertExplainPlanWithLimit(conn, selectSql, dataTableName, indexTableName, limit);
      }

      ResultSet rs = conn.createStatement().executeQuery(selectSql);
      assertTrue(rs.next());
      assertEquals("bcd", rs.getString(1));
      assertEquals("bcde", rs.getString(2));
      assertFalse(rs.next());
      conn.createStatement().execute("DROP INDEX " + indexTableName + " on " + dataTableName);
      conn.commit();
      // Create an index does not include any columns
      indexTableName = generateUniqueName();
      conn.createStatement().execute("CREATE " + (uncovered ? "UNCOVERED " : " ") + "INDEX "
        + indexTableName + " on " + dataTableName + " (val1)");
      conn.commit();

      if (!uncovered) {
        // Verify that without hint, a covered index table is not selected
        assertIndexTableNotSelected(conn, dataTableName, indexTableName, "SELECT id from "
          + dataTableName + " WHERE val1 = 'bc' AND (val2 = 'bcd' OR val3 ='bcde')");
        selectSql = "SELECT /*+ INDEX(" + dataTableName + " " + indexTableName + ")*/ id from "
          + dataTableName + " WHERE val1 = 'bc' AND (val2 = 'bcd' OR val3 ='bcde')";
      } else {
        selectSql = "SELECT id from " + dataTableName
          + " WHERE val1 = 'bc' AND (val2 = 'bcd' OR val3 ='bcde')";
      }
      // Verify that we will read from the index table
      assertIndexPlan(conn, selectSql, dataTableName, indexTableName, null);
      rs = conn.createStatement().executeQuery(selectSql);
      assertTrue(rs.next());
      assertEquals("b", rs.getString(1));
      assertFalse(rs.next());

      // Add another row and run a group by query where the uncovered index should be used
      conn.createStatement().execute("upsert into " + dataTableName
        + " (id, val1, val2, val3) values ('c', 'ab','cde', 'cdef')");
      conn.commit();
      if (!uncovered) {
        // Verify that without an index hint, an uncovered index table is not selected
        assertIndexTableNotSelected(conn, dataTableName, indexTableName,
          "SELECT count(val3) from " + dataTableName + " where val1 > '0' GROUP BY val1");
        selectSql = "SELECT /*+ INDEX(" + dataTableName + " " + indexTableName
          + ")*/ count(val3) from " + dataTableName + " where val1 > '0' GROUP BY val1";
      } else {
        selectSql = "SELECT count(val3) from " + dataTableName + " where val1 > '0' GROUP BY val1";
      }
      // Verify that we will read from the index table
      assertIndexPlan(conn, selectSql, dataTableName, indexTableName, null);
      rs = conn.createStatement().executeQuery(selectSql);
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      assertFalse(rs.next());
      if (!uncovered) {
        selectSql = "SELECT /*+ INDEX(" + dataTableName + " " + indexTableName
          + ")*/ count(val3) from " + dataTableName + " where val1 > '0'";
      } else {
        selectSql = "SELECT count(val3) from " + dataTableName + " where val1 > '0'";
      }
      // Verify that we will read from the index table
      assertIndexPlan(conn, selectSql, dataTableName, indexTableName, null);
      rs = conn.createStatement().executeQuery(selectSql);
      assertTrue(rs.next());
      assertEquals(3, rs.getInt(1));
      // Run an order by query where the uncovered index should be used
      if (!uncovered) {
        // Verify that without hint, the index table is not selected
        assertIndexTableNotSelected(conn, dataTableName, indexTableName,
          "SELECT val3 from " + dataTableName + " where val1 > '0' ORDER BY val1");
        selectSql = "SELECT /*+ INDEX(" + dataTableName + " " + indexTableName + ")*/ val3 from "
          + dataTableName + " where val1 > '0' ORDER BY val1";
      } else {
        selectSql = "SELECT val3 from " + dataTableName + " where val1 > '0' ORDER BY val1";
      }
      // Verify that we will read from the index table
      assertIndexPlan(conn, selectSql, dataTableName, indexTableName, null);
      rs = conn.createStatement().executeQuery(selectSql);
      assertTrue(rs.next());
      assertEquals("abcd", rs.getString(1));
      assertTrue(rs.next());
      assertEquals("cdef", rs.getString(1));
      assertTrue(rs.next());
      assertEquals("bcde", rs.getString(1));
      assertFalse(rs.next());
    }
  }

  @Test
  public void testPartialIndexUpdate() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String dataTableName = generateUniqueName();
      conn.createStatement()
        .execute("create table " + dataTableName + " (id varchar not null primary key, "
          + "val1 varchar, val2 varchar, val3 varchar, val4 varchar)"
          + (salted ? " SALT_BUCKETS=4" : ""));
      conn.createStatement()
        .execute("upsert into " + dataTableName + " values ('b', 'bc', 'bcd', 'bcde', 'bcdef')");
      conn.commit();
      String indexTableName = generateUniqueName();
      conn.createStatement()
        .execute("CREATE " + (uncovered ? "UNCOVERED " : " ") + "INDEX " + indexTableName + " on "
          + dataTableName + " (val1, val2) " + (uncovered ? "" : "INCLUDE (val3)"));
      conn.createStatement()
        .execute("upsert into " + dataTableName + " (id, val2) values ('b', 'bcdd')");
      conn.commit();
      String selectSql;
      if (!uncovered) {
        // Verify that without an index hint, a covered index table is not selected
        assertIndexTableNotSelected(conn, dataTableName, indexTableName,
          "SELECT val4 from " + dataTableName + " WHERE val1 = 'bc' AND val2 = 'bcdd'");
        // Verify that with index hint, we will read from the index table even though val4
        // is not included by the index table
        selectSql = "SELECT /*+ INDEX(" + dataTableName + " " + indexTableName + ")*/ val4 from "
          + dataTableName + " WHERE val1 = 'bc' AND val2 = 'bcdd'";
        assertIndexPlan(conn, selectSql, dataTableName, indexTableName, null);
      } else {
        // Verify that an index hint is not necessary for an uncovered index
        selectSql = "SELECT  val4 from " + dataTableName + " WHERE val1 = 'bc' AND val2 = 'bcdd'";
        assertIndexPlan(conn, selectSql, dataTableName, indexTableName, null);
      }

      ResultSet rs = conn.createStatement().executeQuery(selectSql);
      assertTrue(rs.next());
      assertEquals("bcdef", rs.getString(1));
      assertFalse(rs.next());
    }
  }

  @Test
  public void testSkipScanFilter() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String dataTableName = generateUniqueName();
      conn.createStatement()
        .execute("CREATE TABLE " + dataTableName
          + "(k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, v1 INTEGER, " + "v2 INTEGER, v3 INTEGER "
          + "CONSTRAINT pk PRIMARY KEY (k1,k2)) " + " COLUMN_ENCODED_BYTES = 0, VERSIONS=1"
          + (salted ? ", SALT_BUCKETS=4" : ""));
      TestUtil.addCoprocessor(conn, dataTableName, ScanFilterRegionObserver.class);
      ScanFilterRegionObserver.resetCount();
      String indexTableName = generateUniqueName();
      conn.createStatement().execute("CREATE " + (uncovered ? "UNCOVERED " : " ") + "INDEX "
        + indexTableName + " ON " + dataTableName + "(v1)" + (uncovered ? "" : "include (v2)"));
      final int nIndexValues = 97;
      final Random RAND = new Random(7);
      final int batchSize = 100;
      for (int i = 0; i < 10000; i++) {
        conn.createStatement().execute("UPSERT INTO " + dataTableName + " VALUES (" + i + ", 1, "
          + (RAND.nextInt() % nIndexValues) + ", " + RAND.nextInt() + ", 1)");
        if ((i % batchSize) == 0) {
          conn.commit();
        }
      }
      conn.commit();
      String selectSql =
        "SELECT" + (uncovered ? " " : "/*+ INDEX(" + dataTableName + " " + indexTableName + ")*/ ")
          + "SUM(v3) from " + dataTableName + " GROUP BY v1";

      ResultSet rs = conn.createStatement().executeQuery(selectSql);
      int sum = 0;
      while (rs.next()) {
        sum += rs.getInt(1);
      }
      assertEquals(10000, sum);
      // UncoveredGlobalIndexRegionScanner uses the skip scan filter to retrieve data table
      // rows. Verify that the skip scan filter is used
      assertEquals(10000, ScanFilterRegionObserver.count.get());
    }
  }

  @Test
  public void testCount() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String dataTableName = generateUniqueName();
      conn.createStatement().execute(
        "CREATE TABLE " + dataTableName + "(k1 BIGINT NOT NULL, k2 BIGINT NOT NULL, v1 INTEGER, "
          + "v2 INTEGER, v3 BIGINT " + "CONSTRAINT pk PRIMARY KEY (k1,k2)) "
          + " VERSIONS=1, IMMUTABLE_ROWS=TRUE" + (salted ? ", SALT_BUCKETS=4" : ""));
      String indexTableName = generateUniqueName();
      conn.createStatement().execute("CREATE " + (uncovered ? "UNCOVERED " : " ") + "INDEX "
        + indexTableName + " ON " + dataTableName + "(v1)");
      final int nIndexValues = 9;
      final Random RAND = new Random(7);
      final int batchSize = 1000;
      for (int i = 0; i < 100000; i++) {
        conn.createStatement().execute("UPSERT INTO " + dataTableName + " VALUES (" + i + ", 1, "
          + (RAND.nextInt() % nIndexValues) + ", " + RAND.nextInt() + ", " + RAND.nextInt() + ")");
        if ((i % batchSize) == 0) {
          conn.commit();
        }
      }
      conn.commit();
      String selectSql =
        "SELECT /*+ NO INDEX */  Count(v3) from " + dataTableName + " where v1 = 5";
      ResultSet rs = conn.createStatement().executeQuery(selectSql);
      assertTrue(rs.next());
      long count = rs.getLong(1);
      selectSql =
        "SELECT" + (uncovered ? " " : "/*+ INDEX(" + dataTableName + " " + indexTableName + ")*/ ")
          + "Count(v3) from " + dataTableName + " where v1 = 5";
      // Verify that we will read from the index table
      assertIndexPlan(conn, selectSql, dataTableName, indexTableName, null);
      rs = conn.createStatement().executeQuery(selectSql);
      assertTrue(rs.next());
      assertEquals(count, rs.getInt(1));
    }
  }

  @Test
  public void testFailDataTableRowUpdate() throws Exception {
    String dataTableName = generateUniqueName();
    populateTable(dataTableName); // with two rows ('a', 'ab', 'abc', 'abcd') and ('b', 'bc', 'bcd',
                                  // 'bcde')
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String indexTableName = generateUniqueName();
      conn.createStatement().execute("CREATE " + (uncovered ? "UNCOVERED " : " ") + "INDEX "
        + indexTableName + " on " + dataTableName + " (val1)");
      // Configure IndexRegionObserver to fail the data table update
      // and check that this does not impact the correctness
      IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
      conn.createStatement()
        .execute("upsert into " + dataTableName + " (id, val2) values ('a', 'abcc')");
      try {
        conn.commit();
        fail();
      } catch (Exception e) {
        // this is expected
      }
      IndexRegionObserver.setFailDataTableUpdatesForTesting(false);

      conn.createStatement()
        .execute("upsert into " + dataTableName + " (id, val3) values ('a', 'abcdd')");
      conn.commit();
      String selectSql =
        "SELECT" + (uncovered ? " " : "/*+ INDEX(" + dataTableName + " " + indexTableName + ")*/ ")
          + "val2, val3 from " + dataTableName + " WHERE val1  = 'ab'";
      // Verify that we will read from the first index table
      assertIndexPlan(conn, selectSql, dataTableName, indexTableName, null);
      ResultSet rs = conn.createStatement().executeQuery(selectSql);
      assertTrue(rs.next());
      assertEquals("abc", rs.getString(1));
      assertEquals("abcdd", rs.getString(2));
      assertFalse(rs.next());
    }
  }

  @Test
  public void testFailPostIndexDeleteUpdate() throws Exception {
    String dataTableName = generateUniqueName();
    populateTable(dataTableName); // with two rows ('a', 'ab', 'abc', 'abcd') and ('b', 'bc', 'bcd',
                                  // 'bcde')
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String indexTableName = generateUniqueName();
      conn.createStatement().execute("CREATE " + (uncovered ? "UNCOVERED " : " ") + "INDEX "
        + indexTableName + " on " + dataTableName + " (val1)");
      String selectSql = "SELECT id from " + dataTableName + " WHERE val1  = 'ab'";

      // Verify that we will read from the index table
      assertIndexPlan(conn, selectSql, dataTableName, indexTableName, null);
      ResultSet rs = conn.createStatement().executeQuery(selectSql);
      assertTrue(rs.next());
      assertEquals("a", rs.getString(1));
      assertFalse(rs.next());
      // Configure IndexRegionObserver to fail the last write phase (i.e., the post index update
      // phase) where
      // index rows are deleted and check that this does not impact the correctness
      IndexRegionObserver.setFailPostIndexUpdatesForTesting(true);
      String dml = "DELETE from " + dataTableName + " WHERE id  = 'a'";
      assertEquals(1, conn.createStatement().executeUpdate(dml));
      conn.commit();
      // The index rows are actually not deleted yet because IndexRegionObserver failed delete
      // operation.
      dml = "DELETE from " + dataTableName + " WHERE val1  = 'ab'";
      // This DML will scan the Index table and detect invalid index rows. This will trigger read
      // repair which
      // result in deleting these rows since the corresponding data table rows are deleted already.
      // So,
      // the number of rows to be deleted by the "DELETE" DML will be zero since the rows deleted by
      // read repair
      // will not be visible to the DML
      assertEquals(0, conn.createStatement().executeUpdate(dml));
      rs = conn.createStatement().executeQuery(selectSql);
      assertFalse(rs.next());

      rs = conn.createStatement().executeQuery("SELECT count(*) from " + indexTableName);
      assertTrue(rs.next());
      assertEquals(1, rs.getLong(1));
      IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
    }
  }

  @Test
  public void testPointLookup() throws Exception {
    if (uncovered || salted) {
      return;
    }
    String schemaName = generateUniqueName();
    String dataTableName = generateUniqueName();
    String fullDataTableName = SchemaUtil.getTableName(schemaName, dataTableName);
    populateTable(fullDataTableName);
    String indexName = generateUniqueName();
    String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
    try (Connection conn = DriverManager.getConnection(getUrl());
      Statement stmt = conn.createStatement()) {
      stmt.execute(
        "create index " + indexName + " on " + fullDataTableName + " (val2) include (val1)");
      // Index hint is incorrect as full index name with schema is used
      String sql = "SELECT /*+ INDEX(" + fullDataTableName + " " + fullIndexName
        + ")*/ val2, val3 from " + fullDataTableName + " WHERE id = 'a'";
      assertPlan(conn, sql).scanType("POINT LOOKUP ON 1 KEY").table(fullDataTableName);
      ResultSet rs = stmt.executeQuery(sql);
      assertTrue(rs.next());
      // No explicit index hint and being point lookup no index will be used
      sql = "SELECT val2, val3 from " + fullDataTableName + " WHERE id = 'a'";
      assertPlan(conn, sql).scanType("POINT LOOKUP ON 1 KEY").table(fullDataTableName);
      rs = stmt.executeQuery(sql);
      assertTrue(rs.next());
      // Index hint with point lookup over data table, still index should be used
      sql = "SELECT /*+ INDEX(" + fullDataTableName + " " + indexName + ")*/ val2, val3 from "
        + fullDataTableName + " WHERE id = 'a'";
      assertPlan(conn, sql).scanType("FULL SCAN").table(fullIndexName);
      rs = stmt.executeQuery(sql);
      assertTrue(rs.next());
    }
  }

  /** PHOENIX-8016: an old index entry must be aged independently of its recent base row. */
  @Test
  public void testOldStaleEntriesAreDeletedWhileDataRowsAreRecent() throws Exception {
    assumeTrue(uncovered);
    verifyStaleEntryRepair(true);
  }

  @Test
  public void testYoungStaleEntriesAreNotDeleted() throws Exception {
    assumeTrue(uncovered);
    verifyStaleEntryRepair(false);
  }

  private void verifyStaleEntryRepair(boolean oldEntries) throws Exception {
    Map<Configuration, String> originalThresholds = new LinkedHashMap<>();
    Configuration clusterConfig = getUtility().getConfiguration();
    originalThresholds.put(clusterConfig,
      clusterConfig.getRaw(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB));
    getUtility().getHBaseCluster().getRegionServerThreads().forEach(thread -> {
      Configuration config = thread.getRegionServer().getConfiguration();
      originalThresholds.putIfAbsent(config,
        config.getRaw(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB));
    });
    EnvironmentEdge originalClock = EnvironmentEdgeManager.getDelegate();
    ManualEnvironmentEdge clock = new ManualEnvironmentEdge();
    try {
      // Keep the class-wide zero threshold for existing tests; use a positive age only here.
      for (Configuration config : originalThresholds.keySet()) {
        config.setLong(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB,
          AGE_THRESHOLD_MS);
        assertEquals(AGE_THRESHOLD_MS,
          config.getLong(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB, -1));
      }
      try (Connection conn = DriverManager.getConnection(getUrl())) {
        Fixture fixture = createFixture(conn, clock, oldEntries ? AGE_THRESHOLD_MS * 2 : 1000);
        // No LIMIT: isolate cleanup from premature scan termination in PHOENIX-8015.
        String query = fixture.indexQuery("");
        assertPlan(conn, query).table(fixture.indexTable).scanType("FULL SCAN");
        assertRows(conn, query, ROW_COUNT);
        if (oldEntries) {
          try (Table index =
            getUtility().getConnection().getTable(TableName.valueOf(fixture.indexTable))) {
            for (Result stale : fixture.staleRows) {
              assertTrue("Old stale index entry survived read repair",
                index.get(new Get(stale.getRow())).isEmpty());
              assertStaleEntryDeleteTimestamp(index, stale);
            }
          }
          assertEquals("Current index entries must survive cleanup", ROW_COUNT,
            readPhysicalRows(fixture.indexTable).size());
        } else {
          assertStaleEntriesPresent(fixture);
        }
        assertRows(conn, "SELECT /*+ NO_INDEX */ ID, PAYLOAD, UPDATED_TIME FROM "
          + fixture.dataTable + " ORDER BY UPDATED_TIME ASC", ROW_COUNT);
      }
    } finally {
      EnvironmentEdgeManager.injectEdge(originalClock);
      originalThresholds.forEach((config, value) -> {
        if (value == null) {
          config.unset(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB);
        } else {
          config.set(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB, value);
        }
      });
    }
  }

  private Fixture createFixture(Connection conn, ManualEnvironmentEdge clock, long updateDelayMs)
    throws Exception {
    String dataTable = generateUniqueName();
    String indexTable = generateUniqueName();
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE " + dataTable
        + " (ID VARCHAR NOT NULL PRIMARY KEY, UPDATED_TIME TIMESTAMP, PAYLOAD VARCHAR)" + " TTL="
        + TTL_SECONDS + ", IS_STRICT_TTL=true" + (salted ? ", SALT_BUCKETS=4" : ""));
      stmt.execute("CREATE UNCOVERED INDEX " + indexTable + " ON " + dataTable + " (UPDATED_TIME)"
        + (salted ? " SALT_BUCKETS=4" : ""));
    }
    clock.setValue(EnvironmentEdgeManager.currentTimeMillis() + 1);
    EnvironmentEdgeManager.injectEdge(clock);
    long insertTime = clock.currentTime();
    upsertRows(conn, dataTable, false);
    List<Result> staleRows = readPhysicalRows(indexTable);
    assertEquals("Initial NULL-keyed index entries", ROW_COUNT, staleRows.size());
    for (Result stale : staleRows) {
      assertEquals(insertTime, stale.rawCells()[0].getTimestamp());
    }

    clock.incrementValue(updateDelayMs);
    // Supply the indexed column explicitly to take the write-optimized uncovered-index path.
    upsertRows(conn, dataTable, true);
    Fixture fixture = new Fixture(dataTable, indexTable, staleRows);
    assertEquals("Both stale and current index entries must physically exist", ROW_COUNT * 2,
      readPhysicalRows(indexTable).size());
    assertStaleEntriesPresent(fixture);
    for (Result data : readPhysicalRows(dataTable)) {
      assertEquals("Base rows must have the recent update timestamp", clock.currentTime(),
        data.rawCells()[0].getTimestamp());
    }
    return fixture;
  }

  private void upsertRows(Connection conn, String dataTable, boolean nonNullTimestamp)
    throws Exception {
    try (PreparedStatement stmt = conn.prepareStatement(
      "UPSERT INTO " + dataTable + " (ID, UPDATED_TIME, PAYLOAD) VALUES (?, ?, ?)")) {
      for (int i = 0; i < ROW_COUNT; i++) {
        stmt.setString(1, "tenant-" + i);
        if (nonNullTimestamp) {
          stmt.setTimestamp(2, new Timestamp(UPDATED_TIME_BASE + i * 1000L));
        } else {
          stmt.setNull(2, Types.TIMESTAMP);
        }
        stmt.setString(3, "payload-" + i);
        stmt.executeUpdate();
      }
      conn.commit();
    }
  }

  private void assertStaleEntryDeleteTimestamp(Table index, Result stale) throws Exception {
    // Current index entries have different keys. Inspect the tombstone to verify that cleanup
    // cannot delete newer versions of this same key by using the recent data-row timestamp.
    Scan scan = new Scan().withStartRow(stale.getRow()).withStopRow(stale.getRow(), true);
    scan.setRaw(true);
    scan.readAllVersions();
    boolean foundDelete = false;
    try (ResultScanner scanner = index.getScanner(scan)) {
      for (Result result : scanner) {
        for (Cell cell : result.rawCells()) {
          if (CellUtil.isDeleteFamily(cell)) {
            foundDelete = true;
            assertEquals("Stale-entry deletion must use the index timestamp",
              stale.rawCells()[0].getTimestamp(), cell.getTimestamp());
          }
        }
      }
    }
    assertTrue("Stale-entry cleanup must leave a family delete marker", foundDelete);
  }

  private List<Result> readPhysicalRows(String tableName) throws Exception {
    // No Phoenix scan attributes: inspect live HBase entries without uncovered verification.
    List<Result> rows = new ArrayList<>();
    try (Table table = getUtility().getConnection().getTable(TableName.valueOf(tableName));
      ResultScanner scanner = table.getScanner(new Scan())) {
      for (Result result : scanner) {
        rows.add(result);
      }
    }
    return rows;
  }

  private void assertStaleEntriesPresent(Fixture fixture) throws Exception {
    try (
      Table index = getUtility().getConnection().getTable(TableName.valueOf(fixture.indexTable))) {
      for (Result stale : fixture.staleRows) {
        Result current = index.get(new Get(stale.getRow()));
        assertFalse("Fixture lost a stale index entry", current.isEmpty());
        assertEquals("Stale entry timestamp must not advance with the data row",
          stale.rawCells()[0].getTimestamp(), current.rawCells()[0].getTimestamp());
      }
    }
  }

  private void assertRows(Connection conn, String query, int expectedRows) throws Exception {
    try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
      int count = 0;
      while (rs.next()) {
        assertEquals("tenant-" + count, rs.getString(1));
        assertEquals("payload-" + count, rs.getString(2));
        assertEquals(new Timestamp(UPDATED_TIME_BASE + count * 1000L), rs.getTimestamp(3));
        count++;
      }
      assertEquals("Valid rows returned by: " + query, expectedRows, count);
    }
  }

  private static class Fixture {
    final String dataTable;
    final String indexTable;
    final List<Result> staleRows;

    Fixture(String dataTable, String indexTable, List<Result> staleRows) {
      this.dataTable = dataTable;
      this.indexTable = indexTable;
      this.staleRows = staleRows;
    }

    String indexQuery(String predicate) {
      return "SELECT /*+ INDEX(" + dataTable + " " + indexTable
        + ") */ ID, PAYLOAD, UPDATED_TIME FROM " + dataTable + predicate
        + " ORDER BY UPDATED_TIME ASC";
    }
  }

  public static class ScanFilterRegionObserver extends SimpleRegionObserver {
    public static final AtomicInteger count = new AtomicInteger(0);

    public static void resetCount() {
      count.set(0);
    }

    @Override
    public void preScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Scan scan) {
      if (scan.getFilter() instanceof SkipScanFilter) {
        List<List<KeyRange>> slots = ((SkipScanFilter) scan.getFilter()).getSlots();
        for (List<KeyRange> ranges : slots) {
          count.addAndGet(ranges.size());
        }
      }
    }
  }
}
