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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(NeedsOwnMiniClusterTest.class)
public class WhereOptimizerForArrayAnyNullablePKIT extends WhereOptimizerForArrayAnyITBase {

  private static final BigDecimal PK3_VAL = new BigDecimal("100.5");

  /**
   * Configuration for PK4 column type and sort order.
   */
  private enum Pk4Config {
    /** Fixed-width CHAR(1) NOT NULL */
    FIXED_WIDTH_CHAR("CHAR(1) NOT NULL", ""),
    /** Nullable VARCHAR with ASC sort order (default) */
    NULLABLE_ASC("VARCHAR NOT NULL", ""),
    /** Nullable VARCHAR with DESC sort order */
    NULLABLE_DESC("VARCHAR NOT NULL", " DESC");

    private final String dataType;
    private final String sortOrder;

    Pk4Config(String dataType, String sortOrder) {
      this.dataType = dataType;
      this.sortOrder = sortOrder;
    }

    public String getDataType() {
      return dataType;
    }

    public String getSortOrder() {
      return sortOrder;
    }

    @Override
    public String toString() {
      return name() + "[" + dataType + (sortOrder.isEmpty() ? "" : sortOrder) + "]";
    }
  }

  /**
   * Creates a table with 5 PK columns (last one nullable) and inserts test data.
   * Schema: PK1 VARCHAR, PK2 VARCHAR, PK3 DECIMAL, PK4 (configurable), PK5 DECIMAL (nullable)
   *         COL1 VARCHAR, COL2 VARCHAR, COL3 VARCHAR
   * Inserts 5 rows:
   *   Row 1: (A, B, 100.5, X, NULL, val1, val2, val3)
   *   Row 2: (A, B, 100.5, Y, NULL, val4, val5, val6)
   *   Row 3: (A, B, 100.5, X, 1.0, val7, val8, val9)
   *   Row 4: (A, B, 100.5, Z, NULL, val10, val11, val12)
   *   Row 5: (C, B, 100.5, X, NULL, val13, val14, val15)
   * @param pk4Config configuration for PK4 column type and sort order
   * @param pk5Desc if true, PK5 will have DESC sort order
   * @return the generated table name
   */
  private String createTableAndInsertTestDataForNullablePKTests(Pk4Config pk4Config, boolean pk5Desc) throws Exception {
    String tableName = generateUniqueName();
    String pk5SortOrder = pk5Desc ? " DESC" : "";
    String ddl = "CREATE TABLE " + tableName + " ("
      + "PK1 VARCHAR NOT NULL, "
      + "PK2 VARCHAR NOT NULL, "
      + "PK3 DECIMAL NOT NULL, "
      + "PK4 " + pk4Config.getDataType() + ", "
      + "PK5 DECIMAL, "
      + "COL1 VARCHAR, "
      + "COL2 VARCHAR, "
      + "COL3 VARCHAR, "
      + "CONSTRAINT pk PRIMARY KEY (PK1, PK2, PK3, PK4" + pk4Config.getSortOrder() + ", PK5" + pk5SortOrder + ")"
      + ")";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (java.sql.Statement stmt = conn.createStatement()) {
        stmt.execute(ddl);
        conn.commit();
      }
    }

    String upsertStmt = "UPSERT INTO " + tableName
      + " (PK1, PK2, PK3, PK4, PK5, COL1, COL2, COL3) "
      + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (PreparedStatement stmt = conn.prepareStatement(upsertStmt)) {
        // Row 1: PK5 is NULL
        stmt.setString(1, "A");
        stmt.setString(2, "B");
        stmt.setBigDecimal(3, PK3_VAL);
        stmt.setString(4, "X");
        stmt.setNull(5, Types.DECIMAL);
        stmt.setString(6, "val1");
        stmt.setString(7, "val2");
        stmt.setString(8, "val3");
        stmt.executeUpdate();

        // Row 2: PK5 is NULL, different PK4
        stmt.setString(1, "A");
        stmt.setString(2, "B");
        stmt.setBigDecimal(3, PK3_VAL);
        stmt.setString(4, "Y");
        stmt.setNull(5, Types.DECIMAL);
        stmt.setString(6, "val4");
        stmt.setString(7, "val5");
        stmt.setString(8, "val6");
        stmt.executeUpdate();

        // Row 3: PK5 is NOT NULL
        stmt.setString(1, "A");
        stmt.setString(2, "B");
        stmt.setBigDecimal(3, PK3_VAL);
        stmt.setString(4, "X");
        stmt.setBigDecimal(5, new BigDecimal("1.0"));
        stmt.setString(6, "val7");
        stmt.setString(7, "val8");
        stmt.setString(8, "val9");
        stmt.executeUpdate();

        // Row 4: PK5 is NULL, different PK4
        stmt.setString(1, "A");
        stmt.setString(2, "B");
        stmt.setBigDecimal(3, PK3_VAL);
        stmt.setString(4, "Z");
        stmt.setNull(5, Types.DECIMAL);
        stmt.setString(6, "val10");
        stmt.setString(7, "val11");
        stmt.setString(8, "val12");
        stmt.executeUpdate();

        // Row 5: Different PK1
        stmt.setString(1, "C");
        stmt.setString(2, "B");
        stmt.setBigDecimal(3, PK3_VAL);
        stmt.setString(4, "X");
        stmt.setNull(5, Types.DECIMAL);
        stmt.setString(6, "val13");
        stmt.setString(7, "val14");
        stmt.setString(8, "val15");
        stmt.executeUpdate();

        conn.commit();
      }
    }
    return tableName;
  }

  private void assertQueryUsesIndex(PreparedStatement stmt, String indexName) throws SQLException {
    QueryPlan queryPlan = stmt.unwrap(PhoenixPreparedStatement.class).optimizeQuery();
    ExplainPlan explain = queryPlan.getExplainPlan();
    ExplainPlanAttributes planAttributes = explain.getPlanStepsAsAttributes();
    String tableName = planAttributes.getTableName();
    System.out.println("Explain plan: " + explain.toString());
    assertTrue("Expected query to use index " + indexName + " but used table " + tableName,
      tableName != null && tableName.contains(indexName));
  }

  /**
   * Parameterized test for single point lookup with nullable PK.
   * Tests all combinations of:
   * - PK4 config: FIXED_WIDTH_CHAR, NULLABLE_ASC, NULLABLE_DESC
   * - PK5 sort order: ASC, DESC
   */
  @Test
  public void testSinglePointLookupWithNullablePK() throws Exception {
    for (Pk4Config pk4Config : Pk4Config.values()) {
      for (boolean pk5Desc : new boolean[] { false, true }) {
        String configDesc = "PK4=" + pk4Config + ", PK5 DESC=" + pk5Desc;
        try {
          doTestSinglePointLookupWithNullablePK(pk4Config, pk5Desc);
        } catch (AssertionError e) {
          throw new AssertionError("Failed for configuration: " + configDesc, e);
        } catch (Exception e) {
          throw new Exception("Failed for configuration: " + configDesc, e);
        }
      }
    }
  }

  private void doTestSinglePointLookupWithNullablePK(Pk4Config pk4Config, boolean pk5Desc) throws Exception {
    String tableName = createTableAndInsertTestDataForNullablePKTests(pk4Config, pk5Desc);

    // Query with = for PK4 and IS NULL for PK5
    // IS NULL on trailing nullable PK column generates POINT LOOKUP because:
    // - Trailing nulls are stripped when storing, so key for NULL matches stored key
    // - The generated lookup key is exactly what's stored for rows with trailing NULL
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String selectSql = "SELECT COL1, COL2, PK4, COL3, PK5 FROM " + tableName
        + " WHERE PK1 = ? AND PK2 = ? AND PK3 = ? "
        + "AND PK4 = ? AND PK5 IS NULL";
      try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
        stmt.setString(1, "A");
        stmt.setString(2, "B");
        stmt.setBigDecimal(3, PK3_VAL);
        stmt.setString(4, "X");
        try (java.sql.ResultSet rs = stmt.executeQuery()) {
          // Should return 1 row: PK4='X' with PK5 IS NULL
          assertTrue(rs.next());
          assertEquals("X", rs.getString("PK4"));
          assertEquals("val1", rs.getString("COL1"));
          assertEquals("val2", rs.getString("COL2"));
          assertEquals("val3", rs.getString("COL3"));
          assertNull(rs.getBigDecimal("PK5"));

          // No more rows
          assertFalse(rs.next());
        }
        // IS NULL on trailing nullable PK column generates single POINT LOOKUP
        assertPointLookupsAreGenerated(stmt, 1);

        stmt.setString(4, "Y");
        try (java.sql.ResultSet rs = stmt.executeQuery()) {
          // Should return 1 row: PK4='Y' with PK5 IS NULL
          assertTrue(rs.next());
          assertEquals("Y", rs.getString("PK4"));
          assertEquals("val4", rs.getString("COL1"));
          assertEquals("val5", rs.getString("COL2"));
          assertEquals("val6", rs.getString("COL3"));
          assertNull(rs.getBigDecimal("PK5"));
        }
        // IS NULL on trailing nullable PK column generates single POINT LOOKUP
        assertPointLookupsAreGenerated(stmt, 1);
      }
    }
  }

  /**
   * Parameterized test for multi-point lookups with nullable PK.
   * Tests all combinations of:
   * - PK4 config: FIXED_WIDTH_CHAR, NULLABLE_ASC, NULLABLE_DESC
   * - PK5 sort order: ASC, DESC
   */
  @Test
  public void testMultiPointLookupsWithNullablePK() throws Exception {
    for (Pk4Config pk4Config : Pk4Config.values()) {
      for (boolean pk5Desc : new boolean[] { false, true }) {
        String configDesc = "PK4=" + pk4Config + ", PK5 DESC=" + pk5Desc;
        try {
          doTestMultiPointLookupsWithNullablePK(pk4Config, pk5Desc);
        } catch (AssertionError e) {
          throw new AssertionError("Failed for configuration: " + configDesc, e);
        } catch (Exception e) {
          throw new Exception("Failed for configuration: " + configDesc, e);
        }
      }
    }
  }

  private void doTestMultiPointLookupsWithNullablePK(Pk4Config pk4Config, boolean pk5Desc) throws Exception {
    String tableName = createTableAndInsertTestDataForNullablePKTests(pk4Config, pk5Desc);

    // Query with =ANY(?) for PK4 and IS NULL for PK5
    // IS NULL on trailing nullable PK column generates POINT LOOKUPS because:
    // - Trailing nulls are stripped when storing, so key for NULL matches stored key
    // - The generated lookup key is exactly what's stored for rows with trailing NULL
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String selectSql = "SELECT COL1, COL2, PK4, COL3, PK5 FROM " + tableName
        + " WHERE PK1 = ? AND PK2 = ? AND PK3 = ? AND PK4 = ANY(?) AND PK5 IS NULL";
      Array pk4Arr = conn.createArrayOf("VARCHAR", new String[] { "X", "Y" });
      try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
        stmt.setString(1, "A");
        stmt.setString(2, "B");
        stmt.setBigDecimal(3, PK3_VAL);
        stmt.setArray(4, pk4Arr);
        try (java.sql.ResultSet rs = stmt.executeQuery()) {
          // Should return 2 rows: PK4='X' and PK4='Y' with PK5 IS NULL
          assertTrue(rs.next());
          String pk4Val1 = rs.getString("PK4");
          assertTrue("Y".equals(pk4Val1) || "X".equals(pk4Val1));
          assertNull(rs.getBytes("PK5"));

          assertTrue(rs.next());
          String pk4Val2 = rs.getString("PK4");
          assertTrue("X".equals(pk4Val2) || "Y".equals(pk4Val2));
          assertNull(rs.getBigDecimal("PK5"));

          // No more rows
          assertFalse(rs.next());
          assertNotEquals(pk4Val1, pk4Val2);
        }
        // IS NULL on trailing nullable PK column generates POINT LOOKUPS (2 keys in array)
        assertPointLookupsAreGenerated(stmt, 2);
      }
    }
  }

  /**
   * Parameterized test for query with index after adding nullable PK column.
   * Tests all combinations of:
   * - PK3 sort order: ASC, DESC
   * - PK4 sort order: ASC, DESC
   */
  @Test
  public void testQueryWithIndexAfterAddingNullablePKColumn() throws Exception {
    for (boolean pk3Desc : new boolean[] { false, true }) {
      for (boolean pk4Desc : new boolean[] { false, true }) {
        String configDesc = "PK3 DESC=" + pk3Desc + ", PK4 DESC=" + pk4Desc;
        try {
          doTestQueryWithIndexAfterAddingNullablePKColumn(pk3Desc, pk4Desc);
        } catch (AssertionError e) {
          throw new AssertionError("Failed for configuration: " + configDesc, e);
        } catch (Exception e) {
          throw new Exception("Failed for configuration: " + configDesc, e);
        }
      }
    }
  }

  private void doTestQueryWithIndexAfterAddingNullablePKColumn(boolean pk3Desc, boolean pk4Desc) throws Exception {
    String tableName = generateUniqueName();
    String indexName = "IDX_" + generateUniqueName();

    String pk3SortOrder = pk3Desc ? " DESC" : "";
    String pk4SortOrder = pk4Desc ? " DESC" : "";

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      // Step 1: Create table with one nullable PK column (PK3) at the end
      String createTableDdl = "CREATE TABLE " + tableName + " ("
        + "PK1 VARCHAR NOT NULL, "
        + "PK2 VARCHAR NOT NULL, "
        + "PK3 VARCHAR, "  // Nullable PK column at end
        + "COL1 VARCHAR, "
        + "COL2 VARCHAR, "
        + "CONSTRAINT pk PRIMARY KEY (PK1, PK2, PK3" + pk3SortOrder + ")"
        + ")";
      conn.createStatement().execute(createTableDdl);
      conn.commit();

      // Step 2: Create a covered global index on the data table
      String createIndexDdl = "CREATE INDEX " + indexName + " ON " + tableName
        + " (COL1) INCLUDE (COL2)";
      conn.createStatement().execute(createIndexDdl);
      conn.commit();

      // Step 3: Insert initial data (before ALTER TABLE)
      // Row 1: PK3 is NULL
      String upsertSql = "UPSERT INTO " + tableName
        + " (PK1, PK2, PK3, COL1, COL2) VALUES (?, ?, ?, ?, ?)";
      try (PreparedStatement stmt = conn.prepareStatement(upsertSql)) {
        stmt.setString(1, "A");
        stmt.setString(2, "B");
        stmt.setNull(3, Types.VARCHAR);
        stmt.setString(4, "indexed_val1");
        stmt.setString(5, "col2_val1");
        stmt.executeUpdate();

        // Row 2: PK3 has a value
        stmt.setString(1, "A");
        stmt.setString(2, "B");
        stmt.setString(3, "pk3_val1");
        stmt.setString(4, "indexed_val2");
        stmt.setString(5, "col2_val2");
        stmt.executeUpdate();

        // Row 3: Different PK prefix
        stmt.setString(1, "C");
        stmt.setString(2, "D");
        stmt.setNull(3, Types.VARCHAR);
        stmt.setString(4, "indexed_val3");
        stmt.setString(5, "col2_val3");
        stmt.executeUpdate();
      }
      conn.commit();

      // Step 4: Add a new nullable PK column (PK4) via ALTER TABLE with configured sort order
      String alterTableDdl = "ALTER TABLE " + tableName + " ADD PK4 VARCHAR PRIMARY KEY " + pk4SortOrder;
      conn.createStatement().execute(alterTableDdl);
      conn.commit();

      // Step 5: Insert more data with same PK prefix but different PK4 values
      upsertSql = "UPSERT INTO " + tableName
        + " (PK1, PK2, PK3, PK4, COL1, COL2) VALUES (?, ?, ?, ?, ?, ?)";
      try (PreparedStatement stmt = conn.prepareStatement(upsertSql)) {
        // Row 4: Same prefix as Row 1 (A, B, NULL) but PK4 = 'X'
        stmt.setString(1, "A");
        stmt.setString(2, "B");
        stmt.setNull(3, Types.VARCHAR);
        stmt.setString(4, "X");
        stmt.setString(5, "indexed_val4");
        stmt.setString(6, "col2_val4");
        stmt.executeUpdate();

        // Row 5: Same prefix as Row 1 (A, B, NULL) but PK4 = 'Y'
        stmt.setString(1, "A");
        stmt.setString(2, "B");
        stmt.setNull(3, Types.VARCHAR);
        stmt.setString(4, "Y");
        stmt.setString(5, "indexed_val5");
        stmt.setString(6, "col2_val5");
        stmt.executeUpdate();

        // Row 6: Same prefix as Row 2 (A, B, pk3_val1) but PK4 = 'Y'
        stmt.setString(1, "A");
        stmt.setString(2, "B");
        stmt.setString(3, "pk3_val1");
        stmt.setString(4, "Y");
        stmt.setString(5, "indexed_val6");
        stmt.setString(6, "col2_val6");
        stmt.executeUpdate();

        // Row 7: Same prefix as Row 2 (A, B, NULL) but PK4 = 'Z'
        stmt.setString(1, "A");
        stmt.setString(2, "B");
        stmt.setNull(3, Types.VARCHAR);
        stmt.setString(4, "Z");
        stmt.setString(5, "indexed_val7");
        stmt.setString(6, "col2_val7");
        stmt.executeUpdate();
      }
      conn.commit();

      String selectSql = "SELECT /*+ INDEX(" + tableName + " " + indexName + ") */ "
        + "PK1, PK2, PK3, PK4, COL1, COL2 FROM " + tableName
        + " WHERE PK1 = ? AND PK2 = ? AND PK3 IS NULL AND (PK4 IS NULL OR PK4 = ANY(?)) AND COL1 = ANY(?)";
      Array pk4Arr = conn.createArrayOf("VARCHAR", new String[] { "Z", "Y" });
      Array col1Arr = conn.createArrayOf("VARCHAR", new String[] { "indexed_val5", "indexed_val1", "indexed_val7", "indexed_val4" });
      try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
        stmt.setString(1, "A");
        stmt.setString(2, "B");
        stmt.setArray(3, pk4Arr);
        stmt.setArray(4, col1Arr);
        try (java.sql.ResultSet rs = stmt.executeQuery()) {
          assertTrue(rs.next());
          String pk4Val = rs.getString("PK4");
          assertNull(pk4Val);

          assertTrue(rs.next());
          pk4Val = rs.getString("PK4");
          assertTrue("Y".equals(pk4Val));

          assertTrue(rs.next());
          pk4Val = rs.getString("PK4");
          assertTrue("Z".equals(pk4Val));

          // No more rows
          assertFalse(rs.next());
        }
        // Should generate point lookups for the three PK4 values
        assertPointLookupsAreGenerated(stmt, 12);
        // Assert that the query uses the index table
        assertQueryUsesIndex(stmt, indexName);
      }
    }
  }

  /**
   * Configuration for VIEW_PK1 column type and sort order.
   */
  private enum ViewPk1Config {
    /** Fixed-width CHAR(1) */
    FIXED_WIDTH_CHAR("CHAR(1) NOT NULL", ""),
    /** Nullable VARCHAR with ASC sort order (default) */
    NULLABLE_ASC("VARCHAR", ""),
    /** Nullable VARCHAR with DESC sort order */
    NULLABLE_DESC("VARCHAR", " DESC");

    private final String dataType;
    private final String sortOrder;

    ViewPk1Config(String dataType, String sortOrder) {
      this.dataType = dataType;
      this.sortOrder = sortOrder;
    }

    public String getDataType() {
      return dataType;
    }

    public String getSortOrder() {
      return sortOrder;
    }

    @Override
    public String toString() {
      return name() + "[" + dataType + (sortOrder.isEmpty() ? "" : sortOrder) + "]";
    }
  }

  /**
   * Configuration for VIEW_PK2 column sort order (always nullable VARCHAR).
   */
  private enum ViewPk2Config {
    /** Nullable VARCHAR with ASC sort order (default) */
    NULLABLE_ASC(""),
    /** Nullable VARCHAR with DESC sort order */
    NULLABLE_DESC(" DESC");

    private final String sortOrder;

    ViewPk2Config(String sortOrder) {
      this.sortOrder = sortOrder;
    }

    public String getSortOrder() {
      return sortOrder;
    }

    @Override
    public String toString() {
      return name() + "[VARCHAR" + (sortOrder.isEmpty() ? "" : sortOrder) + "]";
    }
  }

  /**
   * Parameterized test for multi-point lookups on view with nullable PK columns.
   * Tests all combinations of:
   * - VIEW_PK1 config: FIXED_WIDTH_CHAR, NULLABLE_ASC, NULLABLE_DESC
   * - VIEW_PK2 config: NULLABLE_ASC, NULLABLE_DESC
   */
  @Test
  public void testMultiPointLookupsOnViewWithNullablePKColumns() throws Exception {
    for (ViewPk1Config viewPk1Config : ViewPk1Config.values()) {
      for (ViewPk2Config viewPk2Config : ViewPk2Config.values()) {
        String configDesc = "VIEW_PK1=" + viewPk1Config + ", VIEW_PK2=" + viewPk2Config;
        try {
          doTestMultiPointLookupsOnViewWithNullablePKColumns(viewPk1Config, viewPk2Config);
        } catch (AssertionError e) {
          throw new AssertionError("Failed for configuration: " + configDesc, e);
        } catch (Exception e) {
          throw new Exception("Failed for configuration: " + configDesc, e);
        }
      }
    }
  }

  private void doTestMultiPointLookupsOnViewWithNullablePKColumns(
      ViewPk1Config viewPk1Config, ViewPk2Config viewPk2Config) throws Exception {
    String tableName = generateUniqueName();
    String viewName = "VW_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      // Step 1: Create parent table with fixed-width NOT NULL last PK
      // Using CHAR (fixed-width) for PK2 to allow view to add PK columns
      String createTableDdl = "CREATE TABLE " + tableName + " ("
        + "PK1 VARCHAR NOT NULL, "
        + "PK2 CHAR(10) NOT NULL, "  // Fixed-width NOT NULL - allows view to add PKs
        + "COL1 VARCHAR, "
        + "COL2 VARCHAR, "
        + "CONSTRAINT pk PRIMARY KEY (PK1, PK2)"
        + ")";
      conn.createStatement().execute(createTableDdl);
      conn.commit();

      // Step 2: Create view that adds two nullable PK columns with configured types/sort orders
      String createViewDdl = "CREATE VIEW " + viewName + " ("
        + "VIEW_PK1 " + viewPk1Config.getDataType() + ", "
        + "VIEW_PK2 VARCHAR, "
        + "VIEW_COL1 VARCHAR, "
        + "CONSTRAINT view_pk PRIMARY KEY (VIEW_PK1" + viewPk1Config.getSortOrder() 
        + ", VIEW_PK2" + viewPk2Config.getSortOrder() + ")"
        + ") AS SELECT * FROM " + tableName;
      conn.createStatement().execute(createViewDdl);
      conn.commit();

      // Step 3: Insert data through the view with various combinations
      String upsertSql = "UPSERT INTO " + viewName
        + " (PK1, PK2, VIEW_PK1, VIEW_PK2, COL1, COL2, VIEW_COL1) VALUES (?, ?, ?, ?, ?, ?, ?)";
      try (PreparedStatement stmt = conn.prepareStatement(upsertSql)) {
        // Row 1: VIEW_PK1 = 'X', VIEW_PK2 is NULL
        stmt.setString(1, "A");
        stmt.setString(2, "BASE1");
        stmt.setString(3, "X");
        stmt.setNull(4, Types.VARCHAR);
        stmt.setString(5, "col1_val2");
        stmt.setString(6, "col2_val2");
        stmt.setString(7, "view_col1_val2");
        stmt.executeUpdate();

        // Row 2: VIEW_PK1 = 'X', VIEW_PK2 = 'P'
        stmt.setString(1, "A");
        stmt.setString(2, "BASE1");
        stmt.setString(3, "X");
        stmt.setString(4, "P");
        stmt.setString(5, "col1_val3");
        stmt.setString(6, "col2_val3");
        stmt.setString(7, "view_col1_val3");
        stmt.executeUpdate();

        // Row 3: VIEW_PK1 = 'X', VIEW_PK2 = 'Q'
        stmt.setString(1, "A");
        stmt.setString(2, "BASE1");
        stmt.setString(3, "X");
        stmt.setString(4, "Q");
        stmt.setString(5, "col1_val4");
        stmt.setString(6, "col2_val4");
        stmt.setString(7, "view_col1_val4");
        stmt.executeUpdate();

        // Row 4: VIEW_PK1 = 'Y', VIEW_PK2 is NULL
        stmt.setString(1, "A");
        stmt.setString(2, "BASE1");
        stmt.setString(3, "Y");
        stmt.setNull(4, Types.VARCHAR);
        stmt.setString(5, "col1_val5");
        stmt.setString(6, "col2_val5");
        stmt.setString(7, "view_col1_val5");
        stmt.executeUpdate();

        // Row 5: VIEW_PK1 = 'Y', VIEW_PK2 = 'Q'
        stmt.setString(1, "A");
        stmt.setString(2, "BASE1");
        stmt.setString(3, "Y");
        stmt.setString(4, "Q");
        stmt.setString(5, "col1_val6");
        stmt.setString(6, "col2_val6");
        stmt.setString(7, "view_col1_val6");
        stmt.executeUpdate();

        // Row 6: Different base PK prefix
        stmt.setString(1, "B");
        stmt.setString(2, "BASE2");
        stmt.setString(3, "X");
        stmt.setString(4, "P");
        stmt.setString(5, "col1_val7");
        stmt.setString(6, "col2_val7");
        stmt.setString(7, "view_col1_val7");
        stmt.executeUpdate();
      }
      conn.commit();

      String selectSql = "SELECT PK1, PK2, VIEW_PK1, VIEW_PK2, COL1, VIEW_COL1 FROM " + viewName
        + " WHERE PK1 = ? AND PK2 = ? AND VIEW_PK1 = ANY(?) AND (VIEW_PK2 IS NULL OR VIEW_PK2 = ANY(?))";
      Array viewPk1Arr = conn.createArrayOf("VARCHAR", new String[] { "X", "Y" });
      Array viewPk2Arr = conn.createArrayOf("VARCHAR", new String[] { "P", "Q" });
      try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
        stmt.setString(1, "A");
        stmt.setString(2, "BASE1");
        stmt.setArray(3, viewPk1Arr);
        stmt.setArray(4, viewPk2Arr);
        try (java.sql.ResultSet rs = stmt.executeQuery()) {
          int rowCount = 0;
          while (rs.next()) {
            rowCount++;
            String pk2 = rs.getString("PK2");
            String viewPk1 = rs.getString("VIEW_PK1");
            String viewPk2 = rs.getString("VIEW_PK2");
            assertEquals(pk2, "BASE1");
            // Verify VIEW_PK1 is either X or Y
            assertTrue("X".equals(viewPk1) || "Y".equals(viewPk1));
            // Verify VIEW_PK2 is NULL, P, or Q
            assertTrue(viewPk2 == null || "P".equals(viewPk2) || "Q".equals(viewPk2));
          }
          // Expected rows: 
          // (A, BASE1, X, NULL), (A, BASE1, X, P), (A, BASE1, X, Q),
          // (A, BASE1, Y, NULL), (A, BASE1, Y, Q)
          assertEquals(5, rowCount);
        }
        // Assert point lookups are generated
        // VIEW_PK1 has 2 values (X, Y), VIEW_PK2 has 3 values (NULL, P, Q)
        // Total combinations: 2 * 3 = 6 point lookups
        assertPointLookupsAreGenerated(stmt, 6);
      }
    }
  }

  @Test
  public void testPointLookupWithIsNullCheckOnAllPKColumns() throws Exception {
    for (boolean descSortOrder : new boolean[] { false, true }) {
      for (boolean salted : new boolean[] { false, true }) {
        String configDesc = "DESC=" + descSortOrder + ", SALTED=" + salted;
        try {
          doTestPointLookupWithIsNullCheckOnAllPKColumns(descSortOrder, salted);
        } catch (AssertionError e) {
          throw new AssertionError("Failed for configuration: " + configDesc, e);
        } catch (Exception e) {
          throw new Exception("Failed for configuration: " + configDesc, e);
        }
      }
    }
  }

  private void doTestPointLookupWithIsNullCheckOnAllPKColumns(boolean descSortOrder, boolean salted)
      throws Exception {
    String tableName = generateUniqueName();
    String sortOrder = descSortOrder ? " DESC" : "";

    // Create table with all nullable PK columns
    String ddl = "CREATE TABLE " + tableName + " ("
      + "PK1 VARCHAR, "
      + "PK2 VARCHAR, "
      + "PK3 DECIMAL, "
      + "COL1 VARCHAR, "
      + "COL2 VARCHAR, "
      + "CONSTRAINT pk PRIMARY KEY (PK1 " + sortOrder + ", PK2 " + sortOrder + ", PK3 " + sortOrder + ")"
      + ")" + (salted ? " SALT_BUCKETS=3" : "");
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (java.sql.Statement stmt = conn.createStatement()) {
        stmt.execute(ddl);
        conn.commit();
      }
    }

    // Insert test data with various null combinations
    String upsertSql =
      "UPSERT INTO " + tableName + " (PK1, PK2, PK3, COL1, COL2) VALUES (?, ?, ?, ?, ?)";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (PreparedStatement stmt = conn.prepareStatement(upsertSql)) {
        // Row 1: PK1 has value, PK2 and PK3 are NULL
        stmt.setString(1, "A");
        stmt.setNull(2, Types.VARCHAR);
        stmt.setNull(3, Types.DECIMAL);
        stmt.setString(4, "val3");
        stmt.setString(5, "val4");
        stmt.executeUpdate();

        // Row 2: PK1 and PK2 have values, PK3 is NULL
        stmt.setString(1, "A");
        stmt.setString(2, "B");
        stmt.setNull(3, Types.DECIMAL);
        stmt.setString(4, "val5");
        stmt.setString(5, "val6");
        stmt.executeUpdate();

        // Row 3: All PKs have values (no nulls)
        stmt.setString(1, "A");
        stmt.setString(2, "B");
        stmt.setBigDecimal(3, new BigDecimal("100.5"));
        stmt.setString(4, "val7");
        stmt.setString(5, "val8");
        stmt.executeUpdate();

        // Row 4: PK1 is NULL, PK2 has value, PK3 is NULL
        stmt.setNull(1, Types.CHAR);
        stmt.setString(2, "C");
        stmt.setNull(3, Types.DECIMAL);
        stmt.setString(4, "val9");
        stmt.setString(5, "val10");
        stmt.executeUpdate();

        conn.commit();
      }
    }

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      // Query 1: All PKs are NULL - should match no rows
      String selectSql = "SELECT COL1, COL2 FROM " + tableName
        + " WHERE PK1 IS NULL AND PK2 IS NULL AND PK3 IS NULL";
      try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
        try (java.sql.ResultSet rs = stmt.executeQuery()) {
          assertFalse(rs.next());
        }
        String scanType = stmt.unwrap(PhoenixPreparedStatement.class).optimizeQuery()
          .getExplainPlan().getPlanStepsAsAttributes().getExplainScanType();
        assertNull(scanType);
      }
    }
  }
}
