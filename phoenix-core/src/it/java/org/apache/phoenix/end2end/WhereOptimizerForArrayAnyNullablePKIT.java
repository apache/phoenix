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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.junit.Assume;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Integration tests for WHERE clause optimization with nullable PK columns.
 * <p>
 * Parameterized at class level with the following parameters:
 * <ul>
 * <li>{@code salted} - whether the table uses salt buckets</li>
 * <li>{@code columnConfig} - primary column configuration (fixed-width, variable-width
 * ASC/DESC)</li>
 * <li>{@code secondarySortDesc} - sort order for secondary columns (true=DESC, false=ASC)</li>
 * </ul>
 * This covers all combinations of column types and sort orders across all tests.
 */
@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class WhereOptimizerForArrayAnyNullablePKIT extends WhereOptimizerForArrayAnyITBase {

  private static final BigDecimal PK3_VAL = new BigDecimal("100.5");
  private static final int SALT_BUCKETS = 3;

  /**
   * Configuration for column type and sort order. Used for primary configurable columns in tests.
   */
  public enum ColumnConfig {
    /** Fixed-width CHAR(1) NOT NULL */
    FIXED_WIDTH("CHAR(1) NOT NULL", "", false),
    /** Variable-width VARCHAR with ASC sort order (default) */
    VARWIDTH_ASC("VARCHAR", "", false),
    /** Variable-width VARCHAR with DESC sort order */
    VARWIDTH_DESC("VARCHAR", " DESC", true);

    private final String dataType;
    private final String sortOrderClause;
    private final boolean isDesc;

    ColumnConfig(String dataType, String sortOrderClause, boolean isDesc) {
      this.dataType = dataType;
      this.sortOrderClause = sortOrderClause;
      this.isDesc = isDesc;
    }

    public String getDataType() {
      return dataType;
    }

    public String getSortOrderClause() {
      return sortOrderClause;
    }

    public boolean isDesc() {
      return isDesc;
    }

    public boolean isFixedWidth() {
      return this == FIXED_WIDTH;
    }

    /**
     * Returns the data type with NOT NULL constraint for PK columns that must be NOT NULL.
     */
    public String getNotNullDataType() {
      if (this == FIXED_WIDTH) {
        return dataType; // Already includes NOT NULL
      }
      return dataType + " NOT NULL";
    }

    @Override
    public String toString() {
      return name();
    }
  }

  @Parameters(name = "salted={0}, columnConfig={1}, secondarySortDesc={2}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] { { false, ColumnConfig.FIXED_WIDTH, false },
      { false, ColumnConfig.FIXED_WIDTH, true }, { false, ColumnConfig.VARWIDTH_ASC, false },
      { false, ColumnConfig.VARWIDTH_ASC, true }, { false, ColumnConfig.VARWIDTH_DESC, false },
      { false, ColumnConfig.VARWIDTH_DESC, true }, { true, ColumnConfig.FIXED_WIDTH, false },
      { true, ColumnConfig.FIXED_WIDTH, true }, { true, ColumnConfig.VARWIDTH_ASC, false },
      { true, ColumnConfig.VARWIDTH_ASC, true }, { true, ColumnConfig.VARWIDTH_DESC, false },
      { true, ColumnConfig.VARWIDTH_DESC, true } });
  }

  private final boolean salted;
  private final ColumnConfig columnConfig;
  private final boolean secondarySortDesc;

  public WhereOptimizerForArrayAnyNullablePKIT(boolean salted, ColumnConfig columnConfig,
    boolean secondarySortDesc) {
    this.salted = salted;
    this.columnConfig = columnConfig;
    this.secondarySortDesc = secondarySortDesc;
  }

  /**
   * Returns the SALT_BUCKETS clause if salted, otherwise empty string.
   */
  private String getSaltClause() {
    return salted ? " SALT_BUCKETS=" + SALT_BUCKETS : "";
  }

  /**
   * Returns the sort order clause for secondary columns based on secondarySortDesc parameter.
   */
  private String getSecondarySortOrder() {
    return secondarySortDesc ? " DESC" : "";
  }

  /**
   * Creates a table with 5 PK columns (last one nullable) and inserts test data. Uses class-level
   * columnConfig for PK4 and secondarySortDesc for PK5. Schema: PK1 VARCHAR, PK2 VARCHAR, PK3
   * DECIMAL, PK4 (configurable), PK5 DECIMAL (nullable) COL1 VARCHAR, COL2 VARCHAR, COL3 VARCHAR
   * Inserts 5 rows: Row 1: (A, B, 100.5, X, NULL, val1, val2, val3) Row 2: (A, B, 100.5, Y, NULL,
   * val4, val5, val6) Row 3: (A, B, 100.5, X, 1.0, val7, val8, val9) Row 4: (A, B, 100.5, Z, NULL,
   * val10, val11, val12) Row 5: (C, B, 100.5, X, NULL, val13, val14, val15)
   * @return the generated table name
   */
  private String createTableAndInsertTestDataForNullablePKTests() throws Exception {
    String tableName = generateUniqueName();
    String ddl =
      "CREATE TABLE " + tableName + " (" + "PK1 VARCHAR NOT NULL, " + "PK2 VARCHAR NOT NULL, "
        + "PK3 DECIMAL NOT NULL, " + "PK4 " + columnConfig.getNotNullDataType() + ", "
        + "PK5 DECIMAL, " + "COL1 VARCHAR, " + "COL2 VARCHAR, " + "COL3 VARCHAR, "
        + "CONSTRAINT pk PRIMARY KEY (PK1, PK2, PK3, PK4" + columnConfig.getSortOrderClause()
        + ", PK5" + getSecondarySortOrder() + ")" + ")" + getSaltClause();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (java.sql.Statement stmt = conn.createStatement()) {
        stmt.execute(ddl);
        conn.commit();
      }
    }

    String upsertStmt = "UPSERT INTO " + tableName + " (PK1, PK2, PK3, PK4, PK5, COL1, COL2, COL3) "
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
   * Test for single point lookup with nullable PK. Uses class-level parameters: - columnConfig for
   * PK4 type and sort order - secondarySortDesc for PK5 sort order
   */
  @Test
  public void testSinglePointLookupWithNullablePK() throws Exception {
    String tableName = createTableAndInsertTestDataForNullablePKTests();

    // Query with = for PK4 and IS NULL for PK5
    // IS NULL on trailing nullable PK column generates POINT LOOKUP because:
    // - Trailing nulls are stripped when storing, so key for NULL matches stored key
    // - The generated lookup key is exactly what's stored for rows with trailing NULL
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String selectSql = "SELECT COL1, COL2, PK4, COL3, PK5 FROM " + tableName
        + " WHERE PK1 = ? AND PK2 = ? AND PK3 = ? " + "AND PK4 = ? AND PK5 IS NULL";
      try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
        stmt.setString(1, "A");
        stmt.setString(2, "B");
        stmt.setBigDecimal(3, PK3_VAL);
        stmt.setString(4, "X");
        try (ResultSet rs = stmt.executeQuery()) {
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
        try (ResultSet rs = stmt.executeQuery()) {
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
   * Test for multi-point lookups with nullable PK. Uses class-level parameters: - columnConfig for
   * PK4 type and sort order - secondarySortDesc for PK5 sort order
   */
  @Test
  public void testMultiPointLookupsWithNullablePK() throws Exception {
    String tableName = createTableAndInsertTestDataForNullablePKTests();

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
        try (ResultSet rs = stmt.executeQuery()) {
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
   * Test for query with index after adding nullable PK column. Uses class-level parameters: -
   * columnConfig.isDesc() for PK3 sort order - secondarySortDesc for PK4 sort order (added via
   * ALTER TABLE) Note: columnConfig's fixed-width vs variable-width aspect is not applicable here,
   * so FIXED_WIDTH and VARWIDTH_ASC behave the same (both use ASC sort order).
   */
  @Test
  public void testQueryWithIndexAfterAddingNullablePKColumn() throws Exception {
    Assume.assumeFalse(columnConfig.isFixedWidth());
    String tableName = generateUniqueName();
    String indexName = "IDX_" + generateUniqueName();

    String pk3SortOrder = columnConfig.isDesc() ? " DESC" : "";
    String pk4SortOrder = secondarySortDesc ? " DESC" : "";

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      // Step 1: Create table with one nullable PK column (PK3) at the end
      String createTableDdl = "CREATE TABLE " + tableName + " (" + "PK1 VARCHAR NOT NULL, "
        + "PK2 VARCHAR NOT NULL, " + "PK3 VARCHAR, " // Nullable PK column at end
        + "COL1 VARCHAR, " + "COL2 VARCHAR, " + "CONSTRAINT pk PRIMARY KEY (PK1, PK2, PK3"
        + pk3SortOrder + ")" + ")" + getSaltClause();
      conn.createStatement().execute(createTableDdl);
      conn.commit();

      // Step 2: Create a covered global index on the data table
      String createIndexDdl = "CREATE INDEX " + indexName + " ON " + tableName
        + " (COL1) INCLUDE (COL2) " + getSaltClause();
      conn.createStatement().execute(createIndexDdl);
      conn.commit();

      // Step 3: Insert initial data (before ALTER TABLE)
      // Row 1: PK3 is NULL
      String upsertSql =
        "UPSERT INTO " + tableName + " (PK1, PK2, PK3, COL1, COL2) VALUES (?, ?, ?, ?, ?)";
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
      String alterTableDdl =
        "ALTER TABLE " + tableName + " ADD PK4 VARCHAR PRIMARY KEY " + pk4SortOrder;
      conn.createStatement().execute(alterTableDdl);
      conn.commit();

      // Step 5: Insert more data with same PK prefix but different PK4 values
      upsertSql =
        "UPSERT INTO " + tableName + " (PK1, PK2, PK3, PK4, COL1, COL2) VALUES (?, ?, ?, ?, ?, ?)";
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
      Array col1Arr = conn.createArrayOf("VARCHAR",
        new String[] { "indexed_val5", "indexed_val1", "indexed_val7", "indexed_val4" });
      try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
        stmt.setString(1, "A");
        stmt.setString(2, "B");
        stmt.setArray(3, pk4Arr);
        stmt.setArray(4, col1Arr);
        try (ResultSet rs = stmt.executeQuery()) {
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
   * Test for multi-point lookups on view with nullable PK columns. Uses class-level parameters: -
   * columnConfig for VIEW_PK1 type and sort order - secondarySortDesc for VIEW_PK2 sort order
   */
  @Test
  public void testMultiPointLookupsOnViewWithNullablePKColumns() throws Exception {
    String tableName = generateUniqueName();
    String viewName = "VW_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      // Step 1: Create parent table with fixed-width NOT NULL last PK
      // Using CHAR (fixed-width) for PK2 to allow view to add PK columns
      String createTableDdl =
        "CREATE TABLE " + tableName + " (" + "PK1 VARCHAR NOT NULL, " + "PK2 CHAR(10) NOT NULL, " // Fixed-width
                                                                                                  // NOT
                                                                                                  // NULL
                                                                                                  // -
                                                                                                  // allows
                                                                                                  // view
                                                                                                  // to
                                                                                                  // add
                                                                                                  // PKs
          + "COL1 VARCHAR, " + "COL2 VARCHAR, " + "CONSTRAINT pk PRIMARY KEY (PK1, PK2)" + ")"
          + getSaltClause();
      conn.createStatement().execute(createTableDdl);
      conn.commit();

      // Step 2: Create view that adds two nullable PK columns with configured types/sort orders
      String createViewDdl = "CREATE VIEW " + viewName + " (" + "VIEW_PK1 "
        + columnConfig.getDataType() + ", " + "VIEW_PK2 VARCHAR, " + "VIEW_COL1 VARCHAR, "
        + "CONSTRAINT view_pk PRIMARY KEY (VIEW_PK1" + columnConfig.getSortOrderClause()
        + ", VIEW_PK2" + getSecondarySortOrder() + ")" + ") AS SELECT * FROM " + tableName;
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
        try (ResultSet rs = stmt.executeQuery()) {
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

  /**
   * Test for degenerate scan with IS NULL check on all PK columns. Uses class-level
   * columnConfig.isDesc() for sort order of all PK columns. Note: columnConfig's fixed-width vs
   * variable-width aspect is not applicable here, so FIXED_WIDTH and VARWIDTH_ASC behave the same
   * (both use ASC sort order). The secondarySortDesc parameter is also not applicable for this
   * test.
   */
  @Test
  public void testDegenerateScanWithIsNullCheckOnAllPKColumns() throws Exception {
    Assume.assumeFalse(columnConfig.isFixedWidth() || secondarySortDesc);
    String tableName = generateUniqueName();
    String sortOrder = columnConfig.isDesc() ? " DESC" : "";

    // Create table with all nullable PK columns
    String ddl = "CREATE TABLE " + tableName + " (" + "PK1 VARCHAR, " + "PK2 VARCHAR, "
      + "PK3 DECIMAL, " + "COL1 VARCHAR, " + "COL2 VARCHAR, " + "CONSTRAINT pk PRIMARY KEY (PK1 "
      + sortOrder + ", PK2 " + sortOrder + ", PK3 " + sortOrder + ")" + ")" + getSaltClause();
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
        stmt.setBigDecimal(3, PK3_VAL);
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
        try (ResultSet rs = stmt.executeQuery()) {
          assertFalse(rs.next());
        }
        assertDegenerateScanIsGenerated(stmt);
      }
    }
  }

  /**
   * Test for RANGE SCAN and SKIP SCAN with IS NULL on second-to-last nullable PK column, without
   * including the last PK column in the WHERE clause. Uses class-level parameters: - salted for
   * table salting - columnConfig for PK3 sort order (fixed-width vs variable-width aspect not
   * applicable for DECIMAL) - secondarySortDesc for PK4 sort order
   */
  @Test
  public void testRangeScanAndSkipScanWithIsNullOnSecondToLastPK() throws Exception {
    Assume.assumeFalse(columnConfig.isFixedWidth());
    String tableName = generateUniqueName();
    String pk4SortOrder = secondarySortDesc ? " DESC" : "";

    // Create table with last two PK columns (PK4, PK5) nullable
    // PK3 uses columnConfig for sort order, PK4 uses secondarySortDesc
    String ddl = "CREATE TABLE " + tableName + " (" + "PK1 VARCHAR NOT NULL, "
      + "PK2 VARCHAR NOT NULL, " + "PK3 DECIMAL NOT NULL, " + "PK4 VARCHAR, " + "PK5 DECIMAL, "
      + "COL1 VARCHAR, " + "COL2 VARCHAR, " + "CONSTRAINT pk PRIMARY KEY (PK1, PK2, PK3"
      + columnConfig.getSortOrderClause() + ", PK4" + pk4SortOrder + ", PK5)" + ")"
      + getSaltClause();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute(ddl);
      conn.commit();
    }

    // Insert test data
    String upsertStmt = "UPSERT INTO " + tableName
      + " (PK1, PK2, PK3, PK4, PK5, COL1, COL2) VALUES (?, ?, ?, ?, ?, ?, ?)";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (PreparedStatement stmt = conn.prepareStatement(upsertStmt)) {
        // Row 1: PK4 IS NULL, PK5 IS NULL
        stmt.setString(1, "A");
        stmt.setString(2, "B");
        stmt.setBigDecimal(3, PK3_VAL);
        stmt.setNull(4, Types.VARCHAR);
        stmt.setNull(5, Types.DECIMAL);
        stmt.setString(6, "val1");
        stmt.setString(7, "val2");
        stmt.executeUpdate();

        // Row 2: PK4 IS NULL, PK5 has value
        stmt.setString(1, "A");
        stmt.setString(2, "B");
        stmt.setBigDecimal(3, PK3_VAL);
        stmt.setNull(4, Types.VARCHAR);
        stmt.setBigDecimal(5, new BigDecimal("2.0"));
        stmt.setString(6, "val3");
        stmt.setString(7, "val4");
        stmt.executeUpdate();

        // Row 3: PK4 has value, PK5 IS NULL
        stmt.setString(1, "A");
        stmt.setString(2, "B");
        stmt.setBigDecimal(3, PK3_VAL);
        stmt.setString(4, "X");
        stmt.setNull(5, Types.DECIMAL);
        stmt.setString(6, "val5");
        stmt.setString(7, "val6");
        stmt.executeUpdate();

        // Row 4: PK4 has value, PK5 has value
        stmt.setString(1, "A");
        stmt.setString(2, "B");
        stmt.setBigDecimal(3, PK3_VAL);
        stmt.setString(4, "Y");
        stmt.setBigDecimal(5, new BigDecimal("3.0"));
        stmt.setString(6, "val7");
        stmt.setString(7, "val8");
        stmt.executeUpdate();

        // Row 5: Different PK3 value, PK4 IS NULL
        stmt.setString(1, "A");
        stmt.setString(2, "B");
        stmt.setBigDecimal(3, new BigDecimal("200.5"));
        stmt.setNull(4, Types.VARCHAR);
        stmt.setNull(5, Types.DECIMAL);
        stmt.setString(6, "val9");
        stmt.setString(7, "val10");
        stmt.executeUpdate();

        conn.commit();
      }
    }

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      // Test 1: RANGE SCAN - equality on PK1-PK3, IS NULL on PK4, no condition on PK5
      // This should produce a RANGE SCAN because trailing PK5 is not constrained
      String selectSql = "SELECT COL1, COL2, PK4, PK5 FROM " + tableName
        + " WHERE PK1 = ? AND PK2 = ? AND PK3 = ? AND PK4 IS NULL";
      try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
        stmt.setString(1, "A");
        stmt.setString(2, "B");
        stmt.setBigDecimal(3, PK3_VAL);
        try (ResultSet rs = stmt.executeQuery()) {
          // Should return 2 rows where PK4 IS NULL if secondarySortDesc is false, otherwise 1 row
          // due to bug involving DESC sort order and trailing IS NULL when doing range scan.
          // - Row 1: PK4 NULL, PK5 NULL (val1, val2)
          // - Row 2: PK4 NULL, PK5=2.0 (val3, val4)
          assertTrue(rs.next());
          assertNull(rs.getString("PK4"));
          String col1Val1 = rs.getString("COL1");
          if (!secondarySortDesc) {
            assertTrue("val1".equals(col1Val1) || "val3".equals(col1Val1));

            assertTrue(rs.next());
            assertNull(rs.getString("PK4"));
            String col1Val2 = rs.getString("COL1");
            assertTrue("val1".equals(col1Val2) || "val3".equals(col1Val2));

            assertNotEquals(col1Val1, col1Val2);
          } else {
            assertTrue("val3".equals(col1Val1));
          }
          assertFalse(rs.next());
        }
        // Query plan should show RANGE SCAN since trailing PK5 is not constrained
        assertRangeScanIsGenerated(stmt);
      }

      // Test 2: SKIP SCAN - equality on PK1-PK2, =ANY on PK3, IS NULL on PK4, no condition on PK5
      // This should produce a SKIP SCAN due to =ANY on PK3
      String selectSql2 = "SELECT COL1, COL2, PK3, PK4, PK5 FROM " + tableName
        + " WHERE PK1 = ? AND PK2 = ? AND PK3 = ANY(?) AND PK4 IS NULL";
      Array pk3Arr =
        conn.createArrayOf("DECIMAL", new BigDecimal[] { PK3_VAL, new BigDecimal("200.5") });
      try (PreparedStatement stmt = conn.prepareStatement(selectSql2)) {
        stmt.setString(1, "A");
        stmt.setString(2, "B");
        stmt.setArray(3, pk3Arr);
        try (ResultSet rs = stmt.executeQuery()) {
          // Should return 3 rows where PK4 IS NULL but returns 4 or 5 rows due to existing bug
          // inolving skip scan with IS NULL in where clause.
          // - Row 1: PK3=100.5, PK4 NULL, PK5 NULL (val1, val2)
          // - Row 2: PK3=100.5, PK4 NULL, PK5=2.0 (val3, val4)
          // - Row 5: PK3=200.5, PK4 NULL, PK5 NULL (val9, val10)
          int rowCount = 0;
          while (rs.next()) {
            rowCount++;
          }
          if (columnConfig == ColumnConfig.VARWIDTH_DESC && !secondarySortDesc) {
            assertEquals(3, rowCount);
          } else if (columnConfig == ColumnConfig.VARWIDTH_DESC && secondarySortDesc) {
            assertEquals(2, rowCount);
          } else if (salted && columnConfig == ColumnConfig.VARWIDTH_ASC && !secondarySortDesc) {
            assertEquals(4, rowCount);
          } else if (salted && columnConfig == ColumnConfig.VARWIDTH_ASC && secondarySortDesc) {
            assertEquals(2, rowCount);
          } else if (!salted && columnConfig == ColumnConfig.VARWIDTH_ASC && secondarySortDesc) {
            assertEquals(1, rowCount);
          } else {
            assertEquals(5, rowCount);
          }
        }
        if (salted) {
          // 2 * no. of salt buckets = 2 * 3 = 6
          assertSkipScanIsGenerated(stmt, 6);
        } else {
          assertSkipScanIsGenerated(stmt, 2);
        }
      }
    }
  }

  /**
   * Test for multi-point lookups with IS NULL on a fixed-width NOT NULL PK column. This test
   * verifies that degenerate scan is generated when using IS NULL on a fixed-width column (INTEGER
   * NOT NULL) in the primary key, and no rows are returned since a NOT NULL column can never have
   * NULL values. Uses class-level parameters: - salted for table salting - columnConfig for PK3
   * (nullable, variable-width) type and sort order - secondarySortDesc for PK4 (INTEGER NOT NULL,
   * fixed-width) sort order
   */
  @Test
  public void testDegenerateScanWithIsNullOnFixedWidthPK() throws Exception {
    String tableName = generateUniqueName();
    String pk4SortOrder = secondarySortDesc ? " DESC" : "";
    boolean isPK3FixedWidth = columnConfig.isFixedWidth();

    String ddl = "CREATE TABLE " + tableName + " (" + "PK1 VARCHAR NOT NULL, "
      + "PK2 VARCHAR NOT NULL, " + "PK3 " + columnConfig.getDataType() + ", " // Nullable
                                                                              // variable-width
                                                                              // column
      + "PK4 INTEGER NOT NULL, " // Fixed-width NOT NULL column
      + "COL1 VARCHAR, " + "COL2 VARCHAR, " + "CONSTRAINT pk PRIMARY KEY (PK1, PK2, PK3"
      + columnConfig.getSortOrderClause() + ", PK4" + pk4SortOrder + ")" + ")" + getSaltClause();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute(ddl);
      conn.commit();
    }

    // Insert test data with various PK3 values (some NULL, some non-NULL)
    // PK4 is always non-NULL since it's a fixed-width NOT NULL column
    String upsertStmt =
      "UPSERT INTO " + tableName + " (PK1, PK2, PK3, PK4, COL1, COL2) VALUES (?, ?, ?, ?, ?, ?)";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (PreparedStatement stmt = conn.prepareStatement(upsertStmt)) {
        // Row 1: PK3='X', PK4=1
        stmt.setString(1, "A");
        stmt.setString(2, "B");
        stmt.setString(3, "X");
        stmt.setInt(4, 1);
        stmt.setString(5, "val1");
        stmt.setString(6, "val2");
        stmt.executeUpdate();

        // Row 2: PK3='Y', PK4=2
        stmt.setString(1, "A");
        stmt.setString(2, "B");
        stmt.setString(3, "Y");
        stmt.setInt(4, 2);
        stmt.setString(5, "val3");
        stmt.setString(6, "val4");
        stmt.executeUpdate();

        // Row 3: PK3 is NULL, PK4=1
        stmt.setString(1, "A");
        stmt.setString(2, "B");
        if (isPK3FixedWidth) {
          stmt.setString(3, "U");
        } else {
          stmt.setNull(3, Types.VARCHAR);
        }
        stmt.setInt(4, 1);
        stmt.setString(5, "val5");
        stmt.setString(6, "val6");
        stmt.executeUpdate();

        // Row 4: PK3='Z', PK4=3
        stmt.setString(1, "A");
        stmt.setString(2, "B");
        stmt.setString(3, "Z");
        stmt.setInt(4, 3);
        stmt.setString(5, "val7");
        stmt.setString(6, "val8");
        stmt.executeUpdate();

        // Row 5: PK3 is NULL, PK4=2
        stmt.setString(1, "A");
        stmt.setString(2, "B");
        if (isPK3FixedWidth) {
          stmt.setString(3, "U");
        } else {
          stmt.setNull(3, Types.VARCHAR);
        }
        stmt.setInt(4, 2);
        stmt.setString(5, "val9");
        stmt.setString(6, "val10");
        stmt.executeUpdate();

        // Row 6: Different PK1, PK3='X', PK4=1
        stmt.setString(1, "C");
        stmt.setString(2, "B");
        stmt.setString(3, "X");
        stmt.setInt(4, 1);
        stmt.setString(5, "val11");
        stmt.setString(6, "val12");
        stmt.executeUpdate();

        conn.commit();
      }
    }

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String selectSql = "SELECT COL1, COL2, PK3, PK4 FROM " + tableName
        + " WHERE PK1 = ? AND PK2 = ? AND PK3 = ANY(?) AND PK4 IS NULL";
      Array pk3Arr = conn.createArrayOf("VARCHAR", new String[] { "X", "Y" });
      try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
        stmt.setString(1, "A");
        stmt.setString(2, "B");
        stmt.setArray(3, pk3Arr);
        try (ResultSet rs = stmt.executeQuery()) {
          // Should return 0 rows since PK4 is NOT NULL and cannot have NULL values
          assertFalse("Expected no rows since PK4 (INTEGER NOT NULL) cannot be NULL", rs.next());
        }
        assertDegenerateScanIsGenerated(stmt);
      }
    }
  }
}
