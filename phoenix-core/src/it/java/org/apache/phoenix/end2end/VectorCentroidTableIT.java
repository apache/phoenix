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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CENTROID_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CENTROID_VECTOR;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.GENERATION_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_VECTOR_CENTROID_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_VECTOR_CENTROID_TABLE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests SYSTEM.VECTOR_CENTROID schema, primary key semantics, and multi-generation lifecycle.
 */
@Category(ParallelStatsDisabledTest.class)
public class VectorCentroidTableIT extends ParallelStatsDisabledIT {

  /** Verifies table existence, column order, types, and nullability via JDBC metadata. */
  @Test
  public void testTableExistenceAndSchema() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement(); ResultSet rs =
        stmt.executeQuery("SELECT * FROM " + SYSTEM_VECTOR_CENTROID_NAME + " WHERE 1=0")) {
        ResultSetMetaData rsmd = rs.getMetaData();
        assertEquals(4, rsmd.getColumnCount());

        assertEquals(INDEX_NAME, rsmd.getColumnName(1));
        assertEquals(Types.VARCHAR, rsmd.getColumnType(1));
        assertEquals(ResultSetMetaData.columnNoNulls, rsmd.isNullable(1));

        assertEquals(GENERATION_ID, rsmd.getColumnName(2));
        assertEquals(Types.BIGINT, rsmd.getColumnType(2));
        assertEquals(ResultSetMetaData.columnNoNulls, rsmd.isNullable(2));

        assertEquals(CENTROID_ID, rsmd.getColumnName(3));
        assertEquals(Types.INTEGER, rsmd.getColumnType(3));
        assertEquals(ResultSetMetaData.columnNoNulls, rsmd.isNullable(3));

        assertEquals(CENTROID_VECTOR, rsmd.getColumnName(4));
        assertEquals(Types.VARBINARY, rsmd.getColumnType(4));
        assertEquals(ResultSetMetaData.columnNullable, rsmd.isNullable(4));
      }
    }
  }

  /** Verifies primary key upsert replacement and isolation across centroid generations. */
  @Test
  public void testPrimaryKeyConstraintAndUpsert() throws Exception {
    String indexName = "TEST_VECTOR_IDX_" + generateUniqueName();
    byte[] vector1 = new byte[] { 1, 2, 3, 4 };
    byte[] vector2 = new byte[] { 5, 6, 7, 8 };
    byte[] vector3 = new byte[] { 9, 10, 11, 12 };

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String upsertSql = "UPSERT INTO " + SYSTEM_VECTOR_CENTROID_NAME + " (" + INDEX_NAME + ", "
        + GENERATION_ID + ", " + CENTROID_ID + ", " + CENTROID_VECTOR + ") VALUES (?, ?, ?, ?)";

      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, indexName);
        ps.setLong(2, 1L);
        ps.setInt(3, 0);
        ps.setBytes(4, vector1);
        ps.executeUpdate();
      }
      conn.commit();

      // Upserting with an existing (INDEX_NAME, GENERATION_ID, CENTROID_ID) PK overwrites the
      // centroid vector
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, indexName);
        ps.setLong(2, 1L);
        ps.setInt(3, 0);
        ps.setBytes(4, vector2);
        ps.executeUpdate();
      }
      conn.commit();

      String countSql =
        "SELECT COUNT(*) FROM " + SYSTEM_VECTOR_CENTROID_NAME + " WHERE " + INDEX_NAME + " = ?";
      try (PreparedStatement ps = conn.prepareStatement(countSql)) {
        ps.setString(1, indexName);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals("Expected single row after overwriting PK", 1, rs.getInt(1));
        }
      }

      String selectSql = "SELECT " + CENTROID_VECTOR + " FROM " + SYSTEM_VECTOR_CENTROID_NAME
        + " WHERE " + INDEX_NAME + " = ? AND " + GENERATION_ID + " = ? AND " + CENTROID_ID + " = ?";
      try (PreparedStatement ps = conn.prepareStatement(selectSql)) {
        ps.setString(1, indexName);
        ps.setLong(2, 1L);
        ps.setInt(3, 0);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertArrayEquals(vector2, rs.getBytes(1));
        }
      }

      // Centroids belonging to distinct generations coexist under the same index
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, indexName);
        ps.setLong(2, 2L);
        ps.setInt(3, 0);
        ps.setBytes(4, vector3);
        ps.executeUpdate();
      }
      conn.commit();

      try (PreparedStatement ps = conn.prepareStatement(countSql)) {
        ps.setString(1, indexName);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals("Expected two rows for distinct generations", 2, rs.getInt(1));
        }
      }
    }
  }

  /** Verifies PTable schema definition of primary key columns and centroid payload. */
  @Test
  public void testPTableSchema() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable table = pconn.getTableNoCache(SYSTEM_VECTOR_CENTROID_NAME);
      assertNotNull("SYSTEM.VECTOR_CENTROID PTable must exist", table);

      List<PColumn> pkColumns = table.getPKColumns();
      assertEquals(3, pkColumns.size());
      assertEquals(INDEX_NAME, pkColumns.get(0).getName().getString());
      assertEquals(PVarchar.INSTANCE, pkColumns.get(0).getDataType());
      assertFalse(pkColumns.get(0).isNullable());
      assertEquals(GENERATION_ID, pkColumns.get(1).getName().getString());
      assertEquals(PLong.INSTANCE, pkColumns.get(1).getDataType());
      assertFalse(pkColumns.get(1).isNullable());
      assertEquals(CENTROID_ID, pkColumns.get(2).getName().getString());
      assertEquals(PInteger.INSTANCE, pkColumns.get(2).getDataType());
      assertFalse(pkColumns.get(2).isNullable());

      PColumn centroidVectorCol = table.getColumnForColumnName(CENTROID_VECTOR);
      assertNotNull(centroidVectorCol);
      assertEquals(PVarbinary.INSTANCE, centroidVectorCol.getDataType());
      assertTrue(centroidVectorCol.isNullable());
    }
  }

  /** Verifies column metadata reported by JDBC DatabaseMetaData. */
  @Test
  public void testDatabaseMetaDataColumns() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      DatabaseMetaData dbmd = conn.getMetaData();
      try (ResultSet rs =
        dbmd.getColumns(null, SYSTEM_CATALOG_SCHEMA, SYSTEM_VECTOR_CENTROID_TABLE, null)) {
        assertTrue("Expected INDEX_NAME column", rs.next());
        assertEquals(INDEX_NAME, rs.getString(COLUMN_NAME));
        assertEquals(Types.VARCHAR, rs.getInt(DATA_TYPE));

        assertTrue("Expected GENERATION_ID column", rs.next());
        assertEquals(GENERATION_ID, rs.getString(COLUMN_NAME));
        assertEquals(Types.BIGINT, rs.getInt(DATA_TYPE));

        assertTrue("Expected CENTROID_ID column", rs.next());
        assertEquals(CENTROID_ID, rs.getString(COLUMN_NAME));
        assertEquals(Types.INTEGER, rs.getInt(DATA_TYPE));

        assertTrue("Expected CENTROID_VECTOR column", rs.next());
        assertEquals(CENTROID_VECTOR, rs.getString(COLUMN_NAME));
        assertEquals(Types.VARBINARY, rs.getInt(DATA_TYPE));

        assertFalse("No additional columns expected", rs.next());
      }
    }
  }
}
