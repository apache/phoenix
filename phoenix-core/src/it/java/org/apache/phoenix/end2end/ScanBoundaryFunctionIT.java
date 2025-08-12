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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class ScanBoundaryFunctionIT extends ParallelStatsDisabledIT {

  private String tableName;
  private String fullTableName;

  @Before
  public void setUp() throws Exception {
    tableName = generateUniqueName();
    fullTableName = "\"" + tableName + "\"";

    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      // Create table with VARCHAR primary key
      String createTableSql = "CREATE TABLE " + fullTableName + " ("
        + "PK VARCHAR NOT NULL PRIMARY KEY, " + "COL1 VARCHAR, " + "COL2 INTEGER" + ")";
      conn.createStatement().execute(createTableSql);

      // Insert 30 rows with predictable VARCHAR keys
      String upsertSql = "UPSERT INTO " + fullTableName + " (PK, COL1, COL2) VALUES (?, ?, ?)";
      try (PreparedStatement stmt = conn.prepareStatement(upsertSql)) {
        for (int i = 1; i <= 30; i++) {
          String pk = String.format("KEY_%03d", i); // KEY_001, KEY_002, ..., KEY_030
          stmt.setString(1, pk);
          stmt.setString(2, "Value_" + i);
          stmt.setInt(3, i * 10);
          stmt.executeUpdate();
        }
      }
      conn.commit();
    }
  }

  @Test
  public void testScanStartKeyOnly() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      // Test with start key - should get all rows from KEY_010 onwards
      String sql = "SELECT PK FROM " + fullTableName + " WHERE SCAN_START_KEY() = ?";
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        stmt.setBytes(1, Bytes.toBytes("KEY_010"));

        // Verify scan boundaries are set correctly
        PhoenixPreparedStatement pstmt = stmt.unwrap(PhoenixPreparedStatement.class);
        QueryPlan plan = pstmt.optimizeQuery(sql);
        Scan scan = plan.getContext().getScan();
        assertArrayEquals("SCAN_START_KEY should set scan start row to 'KEY_010'",
          Bytes.toBytes("KEY_010"), scan.getStartRow());
        assertEquals("SCAN_END_KEY not specified, so stop row should be empty", 0,
          scan.getStopRow().length);

        // Verify results
        try (ResultSet rs = stmt.executeQuery()) {
          List<String> results = new ArrayList<>();
          while (rs.next()) {
            results.add(rs.getString(1));
          }

          // Should get KEY_010 through KEY_030 (21 rows)
          assertEquals("Should return 21 rows from KEY_010 to KEY_030 inclusive", 21,
            results.size());
          assertEquals("First result should be KEY_010 (inclusive start boundary)", "KEY_010",
            results.get(0));
          assertEquals("Last result should be KEY_030 (no end boundary specified)", "KEY_030",
            results.get(results.size() - 1));
        }
      }
    }
  }

  @Test
  public void testScanEndKeyOnly() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      // Test with end key - should get all rows before KEY_020 (exclusive)
      String sql = "SELECT PK FROM " + fullTableName + " WHERE SCAN_END_KEY() = ?";
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        stmt.setBytes(1, Bytes.toBytes("KEY_020"));

        // Verify scan boundaries are set correctly
        PhoenixPreparedStatement pstmt = stmt.unwrap(PhoenixPreparedStatement.class);
        QueryPlan plan = pstmt.optimizeQuery(sql);
        Scan scan = plan.getContext().getScan();
        assertEquals("SCAN_START_KEY not specified, so start row should be empty", 0,
          scan.getStartRow().length);
        assertArrayEquals("SCAN_END_KEY should set scan stop row to 'KEY_020'",
          Bytes.toBytes("KEY_020"), scan.getStopRow());

        // Verify results
        try (ResultSet rs = stmt.executeQuery()) {
          List<String> results = new ArrayList<>();
          while (rs.next()) {
            results.add(rs.getString(1));
          }

          // Should get KEY_001 through KEY_019 (19 rows)
          assertEquals("Should return 19 rows from beginning to KEY_019 (exclusive end at KEY_020)",
            19, results.size());
          assertEquals("First result should be KEY_001 (no start boundary specified)", "KEY_001",
            results.get(0));
          assertEquals("Last result should be KEY_019 (exclusive end boundary at KEY_020)",
            "KEY_019", results.get(results.size() - 1));
        }
      }
    }
  }

  @Test
  public void testScanBothBoundaries() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      // Test with both start and end keys - should get rows from KEY_010 to KEY_020 (exclusive)
      String sql =
        "SELECT PK FROM " + fullTableName + " WHERE SCAN_START_KEY() = ? AND SCAN_END_KEY() = ?";
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        stmt.setBytes(1, Bytes.toBytes("KEY_010"));
        stmt.setBytes(2, Bytes.toBytes("KEY_020"));

        // Verify scan boundaries are set correctly
        PhoenixPreparedStatement pstmt = stmt.unwrap(PhoenixPreparedStatement.class);
        QueryPlan plan = pstmt.optimizeQuery(sql);
        Scan scan = plan.getContext().getScan();
        assertArrayEquals("SCAN_START_KEY should set scan start row to 'KEY_010'",
          Bytes.toBytes("KEY_010"), scan.getStartRow());
        assertArrayEquals("SCAN_END_KEY should set scan stop row to 'KEY_020'",
          Bytes.toBytes("KEY_020"), scan.getStopRow());

        // Verify results
        try (ResultSet rs = stmt.executeQuery()) {
          List<String> results = new ArrayList<>();
          while (rs.next()) {
            results.add(rs.getString(1));
          }

          // Should get KEY_010 through KEY_019 (10 rows)
          assertEquals("Should return 10 rows from KEY_010 (inclusive) to KEY_020 (exclusive)", 10,
            results.size());
          assertEquals("First result should be KEY_010 (inclusive start boundary)", "KEY_010",
            results.get(0));
          assertEquals("Last result should be KEY_019 (exclusive end boundary at KEY_020)",
            "KEY_019", results.get(results.size() - 1));
        }
      }
    }
  }

  @Test
  public void testScanBothBoundaries2() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      // Test with null start and non-null end key
      String sql =
        "SELECT PK FROM " + fullTableName + " WHERE SCAN_START_KEY() = ? AND SCAN_END_KEY() = ?";
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        stmt.setBytes(1, null);
        stmt.setBytes(2, Bytes.toBytes("KEY_020"));

        // Verify scan boundaries are set correctly
        PhoenixPreparedStatement pstmt = stmt.unwrap(PhoenixPreparedStatement.class);
        QueryPlan plan = pstmt.optimizeQuery(sql);
        Scan scan = plan.getContext().getScan();
        assertEquals("SCAN_START_KEY not specified, so start row should be empty", 0,
          scan.getStartRow().length);
        assertArrayEquals("SCAN_END_KEY should set scan stop row to 'KEY_020'",
          Bytes.toBytes("KEY_020"), scan.getStopRow());

        // Verify results
        try (ResultSet rs = stmt.executeQuery()) {
          List<String> results = new ArrayList<>();
          while (rs.next()) {
            results.add(rs.getString(1));
          }

          // Should get KEY_001 through KEY_019 (19 rows)
          assertEquals("Should return 19 rows from KEY_001 (inclusive) to KEY_020 (exclusive)", 19,
            results.size());
          assertEquals("First result should be KEY_001 (inclusive start boundary)", "KEY_001",
            results.get(0));
          assertEquals("Last result should be KEY_019 (exclusive end boundary at KEY_020)",
            "KEY_019", results.get(results.size() - 1));
        }
      }
    }
  }

  @Test
  public void testScanBothBoundaries3() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      // Test with non-null start and null end key
      String sql =
        "SELECT PK FROM " + fullTableName + " WHERE SCAN_START_KEY() = ? AND SCAN_END_KEY() = ?";
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        stmt.setBytes(1, Bytes.toBytes("KEY_020"));
        stmt.setBytes(2, null);

        // Verify scan boundaries are set correctly
        PhoenixPreparedStatement pstmt = stmt.unwrap(PhoenixPreparedStatement.class);
        QueryPlan plan = pstmt.optimizeQuery(sql);
        Scan scan = plan.getContext().getScan();
        assertArrayEquals("SCAN_START_KEY should set scan start row to 'KEY_020'",
          Bytes.toBytes("KEY_020"), scan.getStartRow());
        assertEquals("SCAN_END_KEY set to null, so stop row should be empty", 0,
          scan.getStopRow().length);

        // Verify results
        try (ResultSet rs = stmt.executeQuery()) {
          List<String> results = new ArrayList<>();
          while (rs.next()) {
            results.add(rs.getString(1));
          }

          // Should get KEY_020 through KEY_030 (11 rows)
          assertEquals("Should return 11 rows from KEY_020 (inclusive) to empty (exclusive)", 11,
            results.size());
          assertEquals("First result should be KEY_020 (inclusive start boundary)", "KEY_020",
            results.get(0));
          assertEquals("Last result should be KEY_030", "KEY_030", results.get(results.size() - 1));
        }
      }
    }
  }

  @Test
  public void testScanBoundariesWithAdditionalFilter() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      // Test with scan boundaries and additional filter
      String sql = "SELECT PK, COL2 FROM " + fullTableName
        + " WHERE SCAN_START_KEY() = ? AND SCAN_END_KEY() = ? AND COL2 > ? " + "ORDER BY PK, COL2";
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        stmt.setBytes(1, Bytes.toBytes("KEY_010"));
        stmt.setBytes(2, Bytes.toBytes("KEY_020"));
        stmt.setInt(3, 150); // Only rows with COL2 > 150

        // Verify scan boundaries are set correctly
        PhoenixPreparedStatement pstmt = stmt.unwrap(PhoenixPreparedStatement.class);
        QueryPlan plan = pstmt.optimizeQuery(sql);
        Scan scan = plan.getContext().getScan();
        assertArrayEquals("SCAN_START_KEY should set scan start row to 'KEY_010'",
          Bytes.toBytes("KEY_010"), scan.getStartRow());
        assertArrayEquals("SCAN_END_KEY should set scan stop row to 'KEY_020'",
          Bytes.toBytes("KEY_020"), scan.getStopRow());

        // Verify results
        try (ResultSet rs = stmt.executeQuery()) {
          List<String> results = new ArrayList<>();
          List<Integer> col2Values = new ArrayList<>();
          while (rs.next()) {
            results.add(rs.getString(1));
            col2Values.add(rs.getInt(2));
          }

          // Should get KEY_016 through KEY_019 (COL2 values: 160, 170, 180, 190)
          assertEquals(
            "Should return 4 rows (KEY_016-KEY_019) that satisfy scan boundaries and COL2 > 150", 4,
            results.size());
          assertEquals("First result should be KEY_016 (first row in range with COL2 > 150)",
            "KEY_016", results.get(0));
          assertEquals("Last result should be KEY_019 (last row in range with COL2 > 150)",
            "KEY_019", results.get(results.size() - 1));

          // Verify all COL2 values are > 150
          for (int i = 0; i < col2Values.size(); i++) {
            Integer value = col2Values.get(i);
            assertTrue("COL2 value for " + results.get(i) + " should be > 150, but was: " + value,
              value > 150);
          }
        }
      }
    }
  }

  @Test
  public void testScanBoundariesWithLiterals() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      // Test with literal values instead of bind parameters
      String sql = "SELECT PK FROM " + fullTableName
        + " WHERE SCAN_START_KEY() = 'KEY_005' AND SCAN_END_KEY() = 'KEY_015'";
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        // Verify scan boundaries are set correctly
        PhoenixPreparedStatement pstmt = stmt.unwrap(PhoenixPreparedStatement.class);
        QueryPlan plan = pstmt.optimizeQuery(sql);
        Scan scan = plan.getContext().getScan();
        assertArrayEquals("SCAN_START_KEY literal should set scan start row to 'KEY_005'",
          Bytes.toBytes("KEY_005"), scan.getStartRow());
        assertArrayEquals("SCAN_END_KEY literal should set scan stop row to 'KEY_015'",
          Bytes.toBytes("KEY_015"), scan.getStopRow());

        // Verify results
        try (ResultSet rs = stmt.executeQuery()) {
          List<String> results = new ArrayList<>();
          while (rs.next()) {
            results.add(rs.getString(1));
          }

          // Should get KEY_005 through KEY_014 (10 rows)
          assertEquals(
            "Should return 10 rows from KEY_005 (inclusive) to KEY_015 (exclusive) using literals",
            10, results.size());
          assertEquals("First result should be KEY_005 (inclusive start boundary from literal)",
            "KEY_005", results.get(0));
          assertEquals(
            "Last result should be KEY_014 (exclusive end boundary at KEY_015 from literal)",
            "KEY_014", results.get(results.size() - 1));
        }
      }
    }
  }

  @Test
  public void testScanBoundariesWithColumnFilter() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      // Test with scan boundaries and column-based filter
      String sql = "SELECT PK, COL1 FROM " + fullTableName
        + " WHERE SCAN_START_KEY() = ? AND COL1 LIKE 'Value_1%'";
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        stmt.setBytes(1, Bytes.toBytes("KEY_010"));

        // Verify scan boundaries are set correctly
        PhoenixPreparedStatement pstmt = stmt.unwrap(PhoenixPreparedStatement.class);
        QueryPlan plan = pstmt.optimizeQuery(sql);
        Scan scan = plan.getContext().getScan();
        assertArrayEquals("SCAN_START_KEY should set scan start row to 'KEY_010'",
          Bytes.toBytes("KEY_010"), scan.getStartRow());
        assertEquals("SCAN_END_KEY not specified, so stop row should be empty", 0,
          scan.getStopRow().length);

        // Verify results
        try (ResultSet rs = stmt.executeQuery()) {
          List<String> results = new ArrayList<>();
          List<String> col1Values = new ArrayList<>();
          while (rs.next()) {
            results.add(rs.getString(1));
            col1Values.add(rs.getString(2));
          }

          // Should get KEY_010 through KEY_019 (rows with COL1 like 'Value_1%')
          assertEquals("Should return 10 rows from KEY_010 onwards that match COL1 LIKE 'Value_1%'",
            10, results.size());
          assertEquals("First result should be KEY_010 (start boundary with matching COL1)",
            "KEY_010", results.get(0));
          assertEquals("Last result should be KEY_019 (last row with COL1 starting with 'Value_1')",
            "KEY_019", results.get(results.size() - 1));

          // Verify all COL1 values match the pattern
          for (int i = 0; i < col1Values.size(); i++) {
            String value = col1Values.get(i);
            assertTrue("COL1 value for " + results.get(i)
              + " should start with 'Value_1', but was: " + value, value.startsWith("Value_1"));
          }
        }
      }
    }
  }

  @Test
  public void testScanBoundariesEmptyResult() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      // Test with scan boundaries that should return no results
      String sql = "SELECT PK FROM " + fullTableName
        + " WHERE SCAN_START_KEY() = ? AND SCAN_END_KEY() = ? ORDER BY PK";
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        stmt.setBytes(1, Bytes.toBytes("KEY_020"));
        stmt.setBytes(2, Bytes.toBytes("KEY_015")); // End before start - should be empty

        // Verify scan boundaries are set correctly
        PhoenixPreparedStatement pstmt = stmt.unwrap(PhoenixPreparedStatement.class);
        QueryPlan plan = pstmt.optimizeQuery(sql);
        Scan scan = plan.getContext().getScan();
        assertArrayEquals("SCAN_START_KEY should set scan start row to 'KEY_020'",
          Bytes.toBytes("KEY_020"), scan.getStartRow());
        assertArrayEquals("SCAN_END_KEY should set scan stop row to 'KEY_015'",
          Bytes.toBytes("KEY_015"), scan.getStopRow());

        // Verify results
        try (ResultSet rs = stmt.executeQuery()) {
          List<String> results = new ArrayList<>();
          while (rs.next()) {
            results.add(rs.getString(1));
          }

          // Should get no results
          assertEquals("Should return 0 rows when start key (KEY_020) > end key (KEY_015)", 0,
            results.size());
        }
      }
    }
  }

  @Test
  public void testScanBoundariesWithComplexFilter() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      // Test with scan boundaries and complex filter conditions
      String sql = "SELECT PK, COL1, COL2 FROM " + fullTableName
        + " WHERE SCAN_START_KEY() = ? AND SCAN_END_KEY() = ? "
        + " AND (COL2 BETWEEN ? AND ? OR COL1 = ?) ORDER BY PK";
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        stmt.setBytes(1, Bytes.toBytes("KEY_005"));
        stmt.setBytes(2, Bytes.toBytes("KEY_025"));
        stmt.setInt(3, 100);
        stmt.setInt(4, 120);
        stmt.setString(5, "Value_15");

        // Verify scan boundaries are set correctly
        PhoenixPreparedStatement pstmt = stmt.unwrap(PhoenixPreparedStatement.class);
        QueryPlan plan = pstmt.optimizeQuery(sql);
        Scan scan = plan.getContext().getScan();
        assertArrayEquals("SCAN_START_KEY should set scan start row to 'KEY_005'",
          Bytes.toBytes("KEY_005"), scan.getStartRow());
        assertArrayEquals("SCAN_END_KEY should set scan stop row to 'KEY_025'",
          Bytes.toBytes("KEY_025"), scan.getStopRow());

        // Verify results
        try (ResultSet rs = stmt.executeQuery()) {
          List<String> results = new ArrayList<>();
          while (rs.next()) {
            String pk = rs.getString(1);
            String col1 = rs.getString(2);
            int col2 = rs.getInt(3);
            results.add(pk);

            // Verify the filter condition is satisfied
            assertTrue(
              "Filter condition not satisfied for " + pk + " (COL2=" + col2 + ", COL1=" + col1
                + "). Expected: (COL2 BETWEEN 100 AND 120) OR COL1='Value_15'",
              (col2 >= 100 && col2 <= 120) || col1.equals("Value_15"));
          }

          // Should get KEY_010, KEY_011, KEY_012, KEY_015 (4 rows) in order
          assertEquals(
            "Should return 4 rows that satisfy complex filter within scan boundaries KEY_005 to KEY_025",
            4, results.size());
          assertEquals("First result should be KEY_010 (COL2=100, within BETWEEN range)", "KEY_010",
            results.get(0));
          assertEquals("Second result should be KEY_011 (COL2=110, within BETWEEN range)",
            "KEY_011", results.get(1));
          assertEquals("Third result should be KEY_012 (COL2=120, within BETWEEN range)", "KEY_012",
            results.get(2));
          assertEquals("Fourth result should be KEY_015 (COL1='Value_15', matches OR condition)",
            "KEY_015", results.get(3));
        }
      }
    }
  }

  @Test
  public void testScanBoundariesFail() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      // Test with scan boundaries in OR condition - this should fail
      String sql =
        "SELECT PK FROM " + fullTableName + " WHERE SCAN_START_KEY() = ? OR SCAN_END_KEY() = ?";
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        stmt.setBytes(1, Bytes.toBytes("KEY_010"));
        stmt.setBytes(2, Bytes.toBytes("KEY_020"));

        try {
          PhoenixPreparedStatement pstmt = stmt.unwrap(PhoenixPreparedStatement.class);
          try (ResultSet rs = stmt.executeQuery()) {
            rs.next();
            throw new AssertionError("Should not reach here");
          }
        } catch (Exception e) {
          assertTrue("ScanStartKeyFunction should not be instantiated",
            e.getMessage().contains("java.lang.InstantiationException: "
              + "org.apache.phoenix.expression.function.ScanStartKeyFunction"));
        }
      }

      sql = "SELECT PK FROM " + fullTableName + " WHERE SCAN_END_KEY() <= ?";
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        stmt.setBytes(1, Bytes.toBytes("KEY_010"));
        try {
          PhoenixPreparedStatement pstmt = stmt.unwrap(PhoenixPreparedStatement.class);
          try (ResultSet rs = stmt.executeQuery()) {
            rs.next();
            throw new AssertionError("Should not reach here");
          }
        } catch (Exception e) {
          assertTrue("ScanEndKeyFunction should not be instantiated",
            e.getMessage().contains("java.lang.InstantiationException: "
              + "org.apache.phoenix.expression.function.ScanEndKeyFunction"));
        }
      }
    }
  }

  @Test
  public void testScanBoundariesMixedWithOrConditionShouldWork() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      // Test scan boundaries combined with other OR conditions - this should work
      // The scan boundaries should be applied, and the OR condition should be evaluated as a filter
      String sql = "SELECT PK, COL1, COL2 FROM " + fullTableName
        + " WHERE SCAN_START_KEY() = ? AND SCAN_END_KEY() = ? AND (COL1 = ? OR COL2 = ?)";
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        stmt.setBytes(1, Bytes.toBytes("KEY_010"));
        stmt.setBytes(2, Bytes.toBytes("KEY_020"));
        stmt.setString(3, "Value_12"); // Should match KEY_012
        stmt.setInt(4, 150); // Should match KEY_015 (but KEY_015 is outside our scan range)

        // Verify scan boundaries are set correctly
        PhoenixPreparedStatement pstmt = stmt.unwrap(PhoenixPreparedStatement.class);
        QueryPlan plan = pstmt.optimizeQuery(sql);
        Scan scan = plan.getContext().getScan();
        assertArrayEquals(
          "SCAN_START_KEY should set scan start row to 'KEY_010' even with OR in additional filters",
          Bytes.toBytes("KEY_010"), scan.getStartRow());
        assertArrayEquals(
          "SCAN_END_KEY should set scan stop row to 'KEY_020' even with OR in additional filters",
          Bytes.toBytes("KEY_020"), scan.getStopRow());

        // Verify results
        try (ResultSet rs = stmt.executeQuery()) {
          List<String> results = new ArrayList<>();
          while (rs.next()) {
            String pk = rs.getString(1);
            String col1 = rs.getString(2);
            int col2 = rs.getInt(3);
            results.add(pk);

            // Verify the filter condition is satisfied within the scan range
            assertTrue(
              "Filter condition not satisfied for " + pk + " (COL1=" + col1 + ", COL2=" + col2
                + "). Expected: COL1='Value_12' OR COL2=150",
              col1.equals("Value_12") || col2 == 150);
          }

          // Should get KEY_012 (COL1='Value_12') and KEY_015 (COL2=150) in order
          assertEquals("Should return 2 rows", 2, results.size());
          assertEquals("First result should be KEY_012 which has COL1='Value_12'", "KEY_012",
            results.get(0));
          assertEquals("Second result should be KEY_015 which has COL2=150", "KEY_015",
            results.get(1));
        }
      }
    }
  }

  @Test
  public void testInvertedScanBoundaries() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      // Test scan boundaries combined with other OR conditions - this should work
      // The scan boundaries should be applied, and the OR condition should be evaluated as a filter
      String sql = "SELECT PK, COL1, COL2 FROM " + fullTableName
        + " WHERE SCAN_START_KEY() = ? AND SCAN_END_KEY() = ? AND (COL1 = ? OR COL2 = ?)";
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        stmt.setBytes(1, Bytes.toBytes("KEY_020"));
        stmt.setBytes(2, Bytes.toBytes("KEY_010"));
        stmt.setString(3, "Value_12");
        stmt.setInt(4, 150);

        // Verify scan boundaries are set correctly
        PhoenixPreparedStatement pstmt = stmt.unwrap(PhoenixPreparedStatement.class);
        QueryPlan plan = pstmt.optimizeQuery(sql);
        Scan scan = plan.getContext().getScan();
        assertArrayEquals(
          "SCAN_START_KEY should set scan start row to 'KEY_020' even with OR in additional"
            + " filters",
          Bytes.toBytes("KEY_020"), scan.getStartRow());
        assertArrayEquals(
          "SCAN_END_KEY should set scan stop row to 'KEY_010' even with OR in additional "
            + "filters",
          Bytes.toBytes("KEY_010"), scan.getStopRow());

        try (ResultSet rs = stmt.executeQuery()) {
          assertFalse("No rows should be found", rs.next());
        }
      }
    }
  }

}
