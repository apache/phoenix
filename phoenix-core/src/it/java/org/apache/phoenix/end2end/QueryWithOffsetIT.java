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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@Category(ParallelStatsDisabledTest.class)
@RunWith(Parameterized.class)
public class QueryWithOffsetIT extends ParallelStatsDisabledIT {

  private static final String[] STRINGS = { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k",
    "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z" };
  private final boolean isSalted;
  private final String preSplit;
  private String ddl;
  private String tableName;

  public QueryWithOffsetIT(String preSplit) {
    this.isSalted = preSplit.startsWith(" SALT_BUCKETS");
    this.preSplit = preSplit;
  }

  @Before
  public void initTest() {
    tableName = "T_" + generateUniqueName();
    ddl = "CREATE TABLE " + tableName + " (t_id VARCHAR NOT NULL,\n" + "k1 INTEGER NOT NULL,\n"
      + "k2 INTEGER NOT NULL,\n" + "C3.k3 INTEGER,\n" + "C2.v1 VARCHAR,\n"
      + "CONSTRAINT pk PRIMARY KEY (t_id, k1, k2)) " + preSplit;
  }

  @Parameters(name = "preSplit = {0}")
  public static synchronized Collection<String> data() {
    return Arrays.asList(new String[] { " SPLIT ON ('e','i','o')", " SALT_BUCKETS=10" });
  }

  @Test
  public void testLimitOffset() throws SQLException {
    Connection conn;
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    conn = DriverManager.getConnection(getUrl(), props);
    createTestTable(getUrl(), ddl);
    initTableValues(conn);
    int limit = 10;
    int offset = 10;
    updateStatistics(conn);
    ResultSet rs;
    rs = conn.createStatement().executeQuery(
      "SELECT t_id from " + tableName + " order by t_id limit " + limit + " offset " + offset);
    int i = 0;
    while (i < limit) {
      assertTrue(rs.next());
      assertEquals("Expected string didn't match for i = " + i, STRINGS[offset + i],
        rs.getString(1));
      i++;
    }

    limit = 35;
    rs = conn.createStatement()
      .executeQuery("SELECT t_id from " + tableName + " union all SELECT t_id from " + tableName
        + " offset " + offset + " FETCH FIRST " + limit + " rows only");
    i = 0;
    while (i++ < STRINGS.length - offset) {
      assertTrue(rs.next());
      assertEquals(STRINGS[offset + i - 1], rs.getString(1));
    }
    i = 0;
    while (i++ < limit - STRINGS.length - offset) {
      assertTrue(rs.next());
      assertEquals(STRINGS[i - 1], rs.getString(1));
    }
    limit = 1;
    offset = 1;
    rs = conn.createStatement().executeQuery(
      "SELECT k2 from " + tableName + " order by k2 desc limit " + limit + " offset " + offset);
    assertTrue(rs.next());
    assertEquals(25, rs.getInt(1));
    assertFalse(rs.next());
    conn.close();
  }

  @Test
  public void testOffsetSerialQueryExecutedOnServer() throws SQLException {
    Connection conn;
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    conn = DriverManager.getConnection(getUrl(), props);
    int offset = 10;
    createTestTable(getUrl(), ddl);
    initTableValues(conn);
    updateStatistics(conn);
    String query = "SELECT t_id from " + tableName + " offset " + offset;
    ExplainPlan plan = conn.prepareStatement(query).unwrap(PhoenixPreparedStatement.class)
      .optimizeQuery().getExplainPlan();
    ExplainPlanAttributes explainPlanAttributes = plan.getPlanStepsAsAttributes();
    assertEquals("FULL SCAN ", explainPlanAttributes.getExplainScanType());
    assertEquals(tableName, explainPlanAttributes.getTableName());
    assertEquals("SERVER FILTER BY EMPTY COLUMN ONLY",
      explainPlanAttributes.getServerWhereFilter());
    if (!isSalted) {
      assertEquals("SERIAL 1-WAY", explainPlanAttributes.getIteratorTypeAndScanSize());
      assertEquals(offset, explainPlanAttributes.getServerOffset().intValue());
    } else {
      assertEquals("PARALLEL 10-WAY", explainPlanAttributes.getIteratorTypeAndScanSize());
      assertEquals("CLIENT MERGE SORT", explainPlanAttributes.getClientSortAlgo());
      assertEquals(offset, explainPlanAttributes.getClientOffset().intValue());
    }

    ResultSet rs = conn.createStatement().executeQuery(query);
    int i = 0;
    while (i++ < STRINGS.length - offset) {
      assertTrue(rs.next());
      assertEquals(STRINGS[offset + i - 1], rs.getString(1));
    }
    query = "SELECT t_id from " + tableName + " ORDER BY v1 offset " + offset;
    plan = conn.prepareStatement(query).unwrap(PhoenixPreparedStatement.class).optimizeQuery()
      .getExplainPlan();
    explainPlanAttributes = plan.getPlanStepsAsAttributes();
    assertEquals("FULL SCAN ", explainPlanAttributes.getExplainScanType());
    assertEquals(tableName, explainPlanAttributes.getTableName());
    assertEquals("[C2.V1]", explainPlanAttributes.getServerSortedBy());
    assertEquals("CLIENT MERGE SORT", explainPlanAttributes.getClientSortAlgo());
    assertEquals(offset, explainPlanAttributes.getClientOffset().intValue());
    if (!isSalted) {
      // When Parallel stats is actually disabled, it is PARALLEL 4-WAY
      // CLIENT PARALLEL 4-WAY FULL SCAN OVER T_N000001
      // SERVER SORTED BY [C2.V1] CLIENT MERGE SORT CLIENT OFFSET 10
      // When enabled, it is 5-WAY
      assertEquals("PARALLEL 4-WAY", explainPlanAttributes.getIteratorTypeAndScanSize());
    } else {
      assertEquals("PARALLEL 10-WAY", explainPlanAttributes.getIteratorTypeAndScanSize());
    }
    conn.close();
  }

  @Test
  public void testOffsetWithoutLimit() throws SQLException {
    Connection conn;
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    conn = DriverManager.getConnection(getUrl(), props);
    int offset = 10;
    createTestTable(getUrl(), ddl);
    initTableValues(conn);
    updateStatistics(conn);
    ResultSet rs;
    int i = 0;
    rs = conn.createStatement()
      .executeQuery("SELECT t_id from " + tableName + " order by t_id offset " + offset + " row");
    while (i++ < STRINGS.length - offset) {
      assertTrue(rs.next());
      assertEquals(STRINGS[offset + i - 1], rs.getString(1));
    }
    rs = conn.createStatement().executeQuery("SELECT k3, count(*) from " + tableName
      + " group by k3 order by k3 desc offset " + offset + " row");

    i = 0;
    while (i++ < STRINGS.length - offset) {
      assertTrue(rs.next());
      assertEquals(STRINGS.length - offset - i + 2, rs.getInt(1));
    }

    rs = conn.createStatement().executeQuery("SELECT t_id from " + tableName
      + " union all SELECT t_id from " + tableName + " offset " + offset + " rows");
    i = 0;
    while (i++ < STRINGS.length - offset) {
      assertTrue(rs.next());
      assertEquals(STRINGS[offset + i - 1], rs.getString(1));
    }
    i = 0;
    while (i++ < STRINGS.length) {
      assertTrue(rs.next());
      assertEquals(STRINGS[i - 1], rs.getString(1));
    }
    conn.close();
  }

  @Test
  public void testMetaDataWithOffset() throws SQLException {
    Connection conn;
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    conn = DriverManager.getConnection(getUrl(), props);
    createTestTable(getUrl(), ddl);
    initTableValues(conn);
    updateStatistics(conn);
    PreparedStatement stmt = conn.prepareStatement("SELECT * from " + tableName + " offset ?");
    ParameterMetaData pmd = stmt.getParameterMetaData();
    assertEquals(1, pmd.getParameterCount());
    assertEquals(Types.INTEGER, pmd.getParameterType(1));
    stmt.setInt(1, 10);
    ResultSet rs = stmt.executeQuery();
    ResultSetMetaData md = rs.getMetaData();
    assertEquals(5, md.getColumnCount());
  }

  /**
   * Test for PHOENIX-7524: Query with WHERE clause that filters all rows + OFFSET
   *
   * Scenario: WHERE clause filters out all rows, then OFFSET tries to skip rows
   * Expected: Query should return empty result set
   */
  @Test
  public void testOffsetWithWhereClauseFilteringAllRows() throws SQLException {
    String testTableName = generateUniqueName();
    Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));

    conn.createStatement().execute(
      "CREATE TABLE " + testTableName + " (id INTEGER NOT NULL PRIMARY KEY, name VARCHAR)");

    for (int i = 1; i <= 10; i++) {
      conn.createStatement().executeUpdate(
        "UPSERT INTO " + testTableName + " VALUES (" + i + ", 'name" + i + "')");
    }
    conn.commit();

    // WHERE clause that filters ALL rows (no row has id > 100)
    String query = "SELECT * FROM " + testTableName + " WHERE id > 100 LIMIT 5 OFFSET 1";
    ResultSet rs = conn.createStatement().executeQuery(query);

    // Should return no rows without throwing exception
    assertFalse("Expected no rows when WHERE filters all rows", rs.next());
    conn.close();
  }

  /**
   * Test for PHOENIX-7524: Empty table with OFFSET
   *
   * Scenario: Table exists but has no rows
   * Expected: Query should return empty result set
   */
  @Test
  public void testOffsetOnEmptyTable() throws SQLException {
    String testTableName = generateUniqueName();
    Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));

    conn.createStatement().execute(
      "CREATE TABLE " + testTableName + " (id INTEGER NOT NULL PRIMARY KEY, val VARCHAR)");
    // Don't insert any rows - table is empty
    conn.commit();

    // Query empty table with OFFSET
    String query = "SELECT * FROM " + testTableName + " LIMIT 10 OFFSET 5";
    ResultSet rs = conn.createStatement().executeQuery(query);

    assertFalse("Expected no rows from empty table", rs.next());
    conn.close();
  }

  /**
   * Test for PHOENIX-7524: OFFSET with LIKE pattern matching nothing
   *
   * Scenario: LIKE pattern that doesn't match any rows
   * Expected: Query should return empty result set
   */
  @Test
  public void testOffsetWithLikePatternMatchingNothing() throws SQLException {
    String testTableName = generateUniqueName();
    Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));

    conn.createStatement().execute(
      "CREATE TABLE " + testTableName + " (name VARCHAR NOT NULL PRIMARY KEY, score INTEGER)");

    conn.createStatement().executeUpdate("UPSERT INTO " + testTableName + " VALUES ('test1', 100)");
    conn.createStatement().executeUpdate("UPSERT INTO " + testTableName + " VALUES ('test2', 200)");
    conn.createStatement().executeUpdate("UPSERT INTO " + testTableName + " VALUES ('test3', 300)");
    conn.commit();

    // LIKE pattern that doesn't match
    String query = "SELECT * FROM " + testTableName +
                   " WHERE name LIKE 'prod%' LIMIT 10 OFFSET 2";
    ResultSet rs = conn.createStatement().executeQuery(query);

    assertFalse("Expected no rows with LIKE pattern matching nothing", rs.next());
    conn.close();
  }

  /**
   * Test for PHOENIX-7524: OFFSET on table with splits but empty regions
   *
   * Scenario: Pre-split table with no data in certain regions
   * Expected: Query should return empty result set
   */
  @Test
  public void testOffsetOnSplitTableWithEmptyRegions() throws SQLException {
    String testTableName = generateUniqueName();
    Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));

    // Create pre-split table
    conn.createStatement().execute(
      "CREATE TABLE " + testTableName +
      " (pk VARCHAR NOT NULL PRIMARY KEY, data INTEGER) SPLIT ON ('m', 'z')");

    // Insert data only in first region (before 'm')
    conn.createStatement().executeUpdate("UPSERT INTO " + testTableName + " VALUES ('a', 1)");
    conn.createStatement().executeUpdate("UPSERT INTO " + testTableName + " VALUES ('b', 2)");
    conn.commit();

    // Query range 'n' to 'y' (in middle/last region with no data)
    String query = "SELECT * FROM " + testTableName +
                   " WHERE pk >= 'n' AND pk < 'y' LIMIT 5 OFFSET 1";
    ResultSet rs = conn.createStatement().executeQuery(query);

    assertFalse("Expected no rows in empty region", rs.next());
    conn.close();
  }

  /**
   * Test for PHOENIX-7524: OFFSET exceeds rows returned by WHERE clause
   *
   * Scenario: WHERE clause returns SOME rows (e.g., 5 rows), but OFFSET exceeds them (e.g., 10)
   * Expected: Query should return empty result set
   */
  @Test
  public void testOffsetExceedsRowsReturnedByWhereClause() throws SQLException {
    String testTableName = generateUniqueName();
    Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));

    conn.createStatement().execute(
      "CREATE TABLE " + testTableName + " (id INTEGER NOT NULL PRIMARY KEY, category VARCHAR, val INTEGER)");

    for (int i = 1; i <= 20; i++) {
      conn.createStatement().executeUpdate(
        "UPSERT INTO " + testTableName + " VALUES (" + i + ", 'cat" + (i % 3) + "', " + (i * 100) + ")");
    }
    conn.commit();

    // WHERE clause returns 7 rows (id <= 20 where id % 3 == 1: rows 1,4,7,10,13,16,19)
    // But OFFSET is 10, which exceeds the 7 rows available
    String query = "SELECT * FROM " + testTableName + 
                   " WHERE category = 'cat1' LIMIT 5 OFFSET 10";
    ResultSet rs = conn.createStatement().executeQuery(query);

    // Should return no rows without throwing exception
    assertFalse("Expected no rows when OFFSET exceeds filtered result count", rs.next());

    conn.close();
  }

  private void initTableValues(Connection conn) throws SQLException {
    for (int i = 0; i < 26; i++) {
      conn.createStatement().execute("UPSERT INTO " + tableName + " values('" + STRINGS[i] + "',"
        + i + "," + (i + 1) + "," + (i + 2) + ",'" + STRINGS[25 - i] + "')");
    }
    conn.commit();
  }

  private void updateStatistics(Connection conn) throws SQLException {
    String query = "UPDATE STATISTICS " + tableName + " SET \""
      + QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB + "\"=" + Long.toString(500);
    conn.createStatement().execute(query);
  }

}
