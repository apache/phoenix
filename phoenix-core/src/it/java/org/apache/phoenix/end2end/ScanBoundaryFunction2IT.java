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
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class ScanBoundaryFunction2IT extends ParallelStatsDisabledIT {

  private String tableName;
  private String fullTableName;

  @Before
  public void setUp() throws Exception {
    tableName = generateUniqueName();
    fullTableName = "\"" + tableName + "\"";

    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      // Create table with composite primary key: PK1 (INTEGER) and PK2 (VARCHAR)
      String createTableSql =
        "CREATE TABLE " + fullTableName + " (" + "PK1 INTEGER NOT NULL, " + "PK2 VARCHAR NOT NULL, "
          + "COL1 VARCHAR, " + "COL2 INTEGER, " + "CONSTRAINT PK PRIMARY KEY (PK1, PK2)" + ")";
      conn.createStatement().execute(createTableSql);

      // Insert 30 rows with predictable composite keys
      String upsertSql =
        "UPSERT INTO " + fullTableName + " (PK1, PK2, COL1, COL2) VALUES (?, ?, ?, ?)";
      try (PreparedStatement stmt = conn.prepareStatement(upsertSql)) {
        for (int i = 1; i <= 30; i++) {
          // PK1: cycle through 1, 2, 3 (10 rows each)
          // PK2: KEY_001, KEY_002, ..., KEY_030
          int pk1 = ((i - 1) % 3) + 1; // 1, 2, 3, 1, 2, 3, ...
          String pk2 = String.format("KEY_%03d", i);
          stmt.setInt(1, pk1);
          stmt.setString(2, pk2);
          stmt.setString(3, "Value_" + i);
          stmt.setInt(4, i * 10);
          stmt.executeUpdate();
        }
      }
      conn.commit();
    }
  }

  @Test
  public void testScanStartKeyOnlyWithCompositePK() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      // Test with start key using composite PK encoded bytes
      // This should get all rows from the specified composite key onwards
      String sql = "SELECT PK1, PK2 FROM " + fullTableName + " WHERE SCAN_START_KEY() = ?";
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        // Create composite key bytes for PK1=2, PK2='KEY_010'
        // This is complex as it involves Phoenix's row key encoding for composite keys
        // For simplicity, we'll use a known encoded key from our test data
        byte[] compositeKeyBytes = createCompositeKeyBytes(2, "KEY_010");
        stmt.setBytes(1, compositeKeyBytes);

        // Verify scan boundaries are set correctly
        PhoenixPreparedStatement pstmt = stmt.unwrap(PhoenixPreparedStatement.class);
        QueryPlan plan = pstmt.optimizeQuery(sql);
        Scan scan = plan.getContext().getScan();
        assertArrayEquals("SCAN_START_KEY should set scan start row to composite key",
          compositeKeyBytes, scan.getStartRow());
        assertEquals("SCAN_END_KEY not specified, so stop row should be empty", 0,
          scan.getStopRow().length);

        // Verify results
        try (ResultSet rs = stmt.executeQuery()) {
          List<String> results = new ArrayList<>();
          while (rs.next()) {
            results.add(rs.getInt(1) + ":" + rs.getString(2));
          }

          assertEquals(17, results.size());
          assertEquals("2:KEY_011", results.get(0));
          assertEquals("3:KEY_030", results.get(16));
        }
      }
    }
  }

  @Test
  public void testScanEndKeyOnlyWithCompositePK() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      // Test with end key using composite PK
      String sql = "SELECT PK1, PK2 FROM " + fullTableName + " WHERE SCAN_END_KEY() = ?";
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        byte[] compositeKeyBytes = createCompositeKeyBytes(2, "KEY_020");
        stmt.setBytes(1, compositeKeyBytes);

        // Verify scan boundaries are set correctly
        PhoenixPreparedStatement pstmt = stmt.unwrap(PhoenixPreparedStatement.class);
        QueryPlan plan = pstmt.optimizeQuery(sql);
        Scan scan = plan.getContext().getScan();
        assertEquals("SCAN_START_KEY not specified, so start row should be empty", 0,
          scan.getStartRow().length);
        assertArrayEquals("SCAN_END_KEY should set scan stop row to composite key",
          compositeKeyBytes, scan.getStopRow());

        // Verify results
        try (ResultSet rs = stmt.executeQuery()) {
          List<String> results = new ArrayList<>();
          while (rs.next()) {
            results.add(rs.getInt(1) + ":" + rs.getString(2));
          }

          assertEquals(16, results.size());
          assertEquals("1:KEY_001", results.get(0));
          assertEquals("2:KEY_017", results.get(15));
        }
      }
    }
  }

  @Test
  public void testScanBothBoundariesWithCompositePK() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      // Test with both start and end keys using composite PK
      String sql = "SELECT PK1, PK2 FROM " + fullTableName
        + " WHERE SCAN_START_KEY() = ? AND SCAN_END_KEY() = ?";
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        byte[] startKeyBytes = createCompositeKeyBytes(1, "KEY_015");
        byte[] endKeyBytes = createCompositeKeyBytes(3, "KEY_005");
        stmt.setBytes(1, startKeyBytes);
        stmt.setBytes(2, endKeyBytes);

        // Verify scan boundaries are set correctly
        PhoenixPreparedStatement pstmt = stmt.unwrap(PhoenixPreparedStatement.class);
        QueryPlan plan = pstmt.optimizeQuery(sql);
        Scan scan = plan.getContext().getScan();
        assertArrayEquals("SCAN_START_KEY should set scan start row to first composite key",
          startKeyBytes, scan.getStartRow());
        assertArrayEquals("SCAN_END_KEY should set scan stop row to second composite key",
          endKeyBytes, scan.getStopRow());

        // Verify results
        try (ResultSet rs = stmt.executeQuery()) {
          List<String> results = new ArrayList<>();
          while (rs.next()) {
            results.add(rs.getInt(1) + ":" + rs.getString(2));
          }

          assertEquals(16, results.size());
          assertEquals("1:KEY_016", results.get(0));
          assertEquals("3:KEY_003", results.get(15));
        }
      }
    }
  }

  @Test
  public void testScanEndWithPkFilters1() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String sql = "SELECT PK1, PK2, COL2 FROM " + fullTableName
        + " WHERE SCAN_END_KEY() = ? AND (PK1 = ? AND PK2 > ?)";
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        byte[] startKeyBytes = createCompositeKeyBytes(1, "KEY_001");
        byte[] endKeyBytes = createCompositeKeyBytes(3, "KEY_030");
        stmt.setBytes(1, endKeyBytes);
        stmt.setInt(2, 2); // Only rows with PK1 = 2
        stmt.setString(3, "KEY_005");

        // Verify scan boundaries are set correctly
        PhoenixPreparedStatement pstmt = stmt.unwrap(PhoenixPreparedStatement.class);
        QueryPlan plan = pstmt.optimizeQuery(sql);
        Scan scan = plan.getContext().getScan();
        byte[] combinedKeyBytes = createCompositeKeyBytes(2, "KEY_005");
        byte[] expectedScanStartKeyBytes = new byte[combinedKeyBytes.length + 1];
        System.arraycopy(combinedKeyBytes, 0, expectedScanStartKeyBytes, 0,
          combinedKeyBytes.length);
        expectedScanStartKeyBytes[expectedScanStartKeyBytes.length - 1] = (byte) 1;
        assertArrayEquals("SCAN_START_KEY should set scan start row as PK1 value",
          expectedScanStartKeyBytes, scan.getStartRow());
        assertArrayEquals("SCAN_END_KEY should set scan stop row with composite PK " + "filter",
          PInteger.INSTANCE.toBytes(3), scan.getStopRow());

        // Verify results
        try (ResultSet rs = stmt.executeQuery()) {
          List<String> results = new ArrayList<>();
          while (rs.next()) {
            int pk1 = rs.getInt(1);
            String pk2 = rs.getString(2);
            results.add(pk1 + ":" + pk2);

            // Verify all results have PK1 = 2
            assertEquals("All results should have PK1 = 2", 2, pk1);
          }

          assertEquals(8, results.size());
          assertEquals("2:KEY_008", results.get(0));
          assertEquals("2:KEY_029", results.get(7));
        }
      }
    }
  }

  @Test
  public void testScanEndWithPkFilters2() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String sql =
        "SELECT PK1, PK2, COL2 FROM " + fullTableName + " WHERE SCAN_END_KEY() = ? AND (PK1 > ?)";
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        byte[] endKeyBytes = createCompositeKeyBytes(3, "KEY_025");
        stmt.setBytes(1, endKeyBytes);
        stmt.setInt(2, 2);

        // Verify scan boundaries are set correctly
        PhoenixPreparedStatement pstmt = stmt.unwrap(PhoenixPreparedStatement.class);
        QueryPlan plan = pstmt.optimizeQuery(sql);
        Scan scan = plan.getContext().getScan();
        assertArrayEquals("SCAN_START_KEY should set scan start row as PK1 value",
          PInteger.INSTANCE.toBytes(3), scan.getStartRow());
        assertArrayEquals("SCAN_END_KEY should set scan stop row with SCAN_END_KEY", endKeyBytes,
          scan.getStopRow());

        try (ResultSet rs = stmt.executeQuery()) {
          List<String> results = new ArrayList<>();
          while (rs.next()) {
            int pk1 = rs.getInt(1);
            String pk2 = rs.getString(2);
            results.add(pk1 + ":" + pk2);
          }

          assertEquals(8, results.size());
          assertEquals("3:KEY_003", results.get(0));
          assertEquals("3:KEY_024", results.get(7));
        }
      }
    }
  }

  @Test
  public void testScanBoundariesFailWithCompositePK() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      // Test that OR conditions with scan boundaries fail on composite PK table
      String sql = "SELECT PK1, PK2 FROM " + fullTableName + " WHERE SCAN_START_KEY() != ?";
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        stmt.setBytes(1, createCompositeKeyBytes(1, "KEY_010"));

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
    }
  }

  /**
   * Helper method to create composite key bytes for testing.
   */
  private byte[] createCompositeKeyBytes(int pk1, String pk2) {
    byte[] pk1Bytes = PInteger.INSTANCE.toBytes(pk1);
    byte[] pk2Bytes = PVarchar.INSTANCE.toBytes(pk2);

    byte[] result = new byte[pk1Bytes.length + pk2Bytes.length];
    System.arraycopy(pk1Bytes, 0, result, 0, pk1Bytes.length);
    System.arraycopy(pk2Bytes, 0, result, pk1Bytes.length, pk2Bytes.length);

    return result;
  }
}
