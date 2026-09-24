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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.types.PVectorDouble;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class VectorColumnIT extends ParallelStatsDisabledIT {

  /** Reset shared vector state after each test. */
  @After
  public void resetVectorState() {
    VectorIndexTestUtil.resetSharedVectorState();
  }

  @Test
  public void testVectorDimensionPersistenceRoundTrip() throws Exception {
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (pk INTEGER PRIMARY KEY, v VECTOR(FLOAT, 128))");
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable pTable = pconn.getTable(new PTableKey(null, tableName));
      assertNotNull(pTable);
      PColumn pColumn = pTable.getColumnForColumnName("V");
      assertNotNull(pColumn);
      assertEquals(Integer.valueOf(128), pColumn.getMaxLength());
      assertEquals(PVectorFloat.INSTANCE, pColumn.getDataType());

      // Verify from a fresh connection reading directly from system catalog
      try (Connection conn2 = DriverManager.getConnection(getUrl())) {
        PhoenixConnection pconn2 = conn2.unwrap(PhoenixConnection.class);
        PTable pTableFresh = pconn2.getTableNoCache(tableName);
        assertNotNull(pTableFresh);
        PColumn pColumnFresh = pTableFresh.getColumnForColumnName("V");
        assertNotNull(pColumnFresh);
        assertEquals(Integer.valueOf(128), pColumnFresh.getMaxLength());
        assertEquals(PVectorFloat.INSTANCE, pColumnFresh.getDataType());
      }

      // Drop and recreate with dimension 256
      conn.createStatement().execute("DROP TABLE " + tableName);
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (pk INTEGER PRIMARY KEY, v VECTOR(FLOAT, 256))");
      pTable = pconn.getTable(new PTableKey(null, tableName));
      assertNotNull(pTable);
      pColumn = pTable.getColumnForColumnName("V");
      assertNotNull(pColumn);
      assertEquals(Integer.valueOf(256), pColumn.getMaxLength());
      assertEquals(PVectorFloat.INSTANCE, pColumn.getDataType());

      // Verify fresh connection for 256
      try (Connection conn3 = DriverManager.getConnection(getUrl())) {
        PhoenixConnection pconn3 = conn3.unwrap(PhoenixConnection.class);
        PTable pTableFresh256 = pconn3.getTableNoCache(tableName);
        assertNotNull(pTableFresh256);
        PColumn pColumnFresh256 = pTableFresh256.getColumnForColumnName("V");
        assertNotNull(pColumnFresh256);
        assertEquals(Integer.valueOf(256), pColumnFresh256.getMaxLength());
        assertEquals(PVectorFloat.INSTANCE, pColumnFresh256.getDataType());
      }
    }
  }

  @Test
  public void testJdbcMetadataReporting() throws Exception {
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (pk INTEGER PRIMARY KEY, v VECTOR(FLOAT, 128))");

      DatabaseMetaData dbmd = conn.getMetaData();
      try (ResultSet rs = dbmd.getColumns(null, "", tableName, "V")) {
        assertTrue(rs.next());
        assertEquals("V", rs.getString("COLUMN_NAME"));
        assertEquals(PVectorFloat.INSTANCE.getSqlType(), rs.getInt("DATA_TYPE"));
        assertEquals("VECTOR(FLOAT)", rs.getString("TYPE_NAME"));
        assertEquals(128, rs.getInt("COLUMN_SIZE"));
      }

      try (ResultSet rs = conn.createStatement().executeQuery("SELECT v FROM " + tableName)) {
        ResultSetMetaData rsmd = rs.getMetaData();
        String colTypeName = rsmd.getColumnTypeName(1);
        assertNotNull(colTypeName);
        assertTrue("Column type name should contain VECTOR: " + colTypeName,
          colTypeName.contains("VECTOR"));
      }
    }
  }

  @Test
  public void testVectorDoubleDimensionPersistence() throws Exception {
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (pk INTEGER PRIMARY KEY, v VECTOR(DOUBLE, 64))");
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable pTable = pconn.getTable(new PTableKey(null, tableName));
      assertNotNull(pTable);
      PColumn pColumn = pTable.getColumnForColumnName("V");
      assertNotNull(pColumn);
      assertEquals(Integer.valueOf(64), pColumn.getMaxLength());
      assertEquals(PVectorDouble.INSTANCE, pColumn.getDataType());

      DatabaseMetaData dbmd = conn.getMetaData();
      try (ResultSet rs = dbmd.getColumns(null, "", tableName, "V")) {
        assertTrue(rs.next());
        assertEquals("VECTOR(DOUBLE)", rs.getString("TYPE_NAME"));
        assertEquals(64, rs.getInt("COLUMN_SIZE"));
        assertEquals(PVectorDouble.INSTANCE.getSqlType(), rs.getInt("DATA_TYPE"));
      }
    }
  }

  @Test
  public void testMultipleVectorColumnsPersistence() throws Exception {
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (pk INTEGER PRIMARY KEY, vf VECTOR(FLOAT, 512), vd VECTOR(DOUBLE, 1536))");
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable pTable = pconn.getTable(new PTableKey(null, tableName));
      assertNotNull(pTable);

      PColumn colF = pTable.getColumnForColumnName("VF");
      assertNotNull(colF);
      assertEquals(Integer.valueOf(512), colF.getMaxLength());
      assertEquals(PVectorFloat.INSTANCE, colF.getDataType());

      PColumn colD = pTable.getColumnForColumnName("VD");
      assertNotNull(colD);
      assertEquals(Integer.valueOf(1536), colD.getMaxLength());
      assertEquals(PVectorDouble.INSTANCE, colD.getDataType());
    }
  }

  @Test
  public void testJdbcArrayBindingRoundTrip() throws Exception {
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (pk INTEGER PRIMARY KEY, v VECTOR(FLOAT, 3))");

      String upsertSql = "UPSERT INTO " + tableName + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setInt(1, 1);
        Array array = conn.createArrayOf("FLOAT", new Float[] { 1.0f, 2.0f, 3.0f });
        ps.setArray(2, array);
        ps.executeUpdate();
      }
      conn.commit();

      try (ResultSet rs =
        conn.createStatement().executeQuery("SELECT v FROM " + tableName + " WHERE pk = 1")) {
        assertTrue(rs.next());
        float[] retrieved = (float[]) rs.getObject(1);
        assertNotNull(retrieved);
        assertArrayEquals(new float[] { 1.0f, 2.0f, 3.0f }, retrieved, 0.00001f);

        Array retrievedArray = rs.getArray(1);
        assertNotNull(retrievedArray);
        Object arrayObj = retrievedArray.getArray();
        if (arrayObj instanceof Float[]) {
          assertArrayEquals(new Float[] { 1.0f, 2.0f, 3.0f }, (Float[]) arrayObj);
        } else if (arrayObj instanceof float[]) {
          assertArrayEquals(new float[] { 1.0f, 2.0f, 3.0f }, (float[]) arrayObj, 0.00001f);
        }

        String str = rs.getString(1);
        assertNotNull(str);
        assertTrue("Expected string representation containing numbers: " + str,
          str.contains("1.0") && str.contains("2.0") && str.contains("3.0"));
      }
    }
  }

  @Test
  public void testDimensionMismatchRejection() throws Exception {
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (pk INTEGER PRIMARY KEY, v VECTOR(FLOAT, 3))");

      String upsertSql = "UPSERT INTO " + tableName + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setInt(1, 1);
        Array array = conn.createArrayOf("FLOAT", new Float[] { 1.0f, 2.0f, 3.0f, 4.0f });
        ps.setArray(2, array);
        try {
          ps.executeUpdate();
          fail("Should have thrown exception on dimension mismatch");
        } catch (SQLException e) {
          assertEquals(SQLExceptionCode.CONSTRAINT_VIOLATION.getErrorCode(), e.getErrorCode());
        }
      }
    }
  }

  @Test
  public void testArrayConstructorSyntax() throws Exception {
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (pk INTEGER PRIMARY KEY, v VECTOR(FLOAT, 3))");

      conn.createStatement()
        .execute("UPSERT INTO " + tableName + " VALUES (1, ARRAY[1.0, 2.0, 3.0])");
      conn.commit();

      try (ResultSet rs =
        conn.createStatement().executeQuery("SELECT v FROM " + tableName + " WHERE pk = 1")) {
        assertTrue(rs.next());
        float[] retrieved = (float[]) rs.getObject(1);
        assertNotNull(retrieved);
        assertArrayEquals(new float[] { 1.0f, 2.0f, 3.0f }, retrieved, 0.00001f);
      }
    }
  }

  @Test
  public void testJsonStringLiteralBinding() throws Exception {
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (pk INTEGER PRIMARY KEY, v VECTOR(FLOAT, 3))");

      conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (1, '[4.0, 5.0, 6.0]')");
      conn.commit();

      try (ResultSet rs =
        conn.createStatement().executeQuery("SELECT v FROM " + tableName + " WHERE pk = 1")) {
        assertTrue(rs.next());
        float[] retrieved = (float[]) rs.getObject(1);
        assertNotNull(retrieved);
        assertArrayEquals(new float[] { 4.0f, 5.0f, 6.0f }, retrieved, 0.00001f);
      }
    }
  }

  @Test
  public void testVectorDoubleArrayBindingRoundTrip() throws Exception {
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (pk INTEGER PRIMARY KEY, v VECTOR(DOUBLE, 3))");

      String upsertSql = "UPSERT INTO " + tableName + " VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setInt(1, 1);
        Array array = conn.createArrayOf("DOUBLE", new Double[] { 1.1, 2.2, 3.3 });
        ps.setArray(2, array);
        ps.executeUpdate();
      }
      conn.commit();

      try (ResultSet rs =
        conn.createStatement().executeQuery("SELECT v FROM " + tableName + " WHERE pk = 1")) {
        assertTrue(rs.next());
        double[] retrieved = (double[]) rs.getObject(1);
        assertNotNull(retrieved);
        assertArrayEquals(new double[] { 1.1, 2.2, 3.3 }, retrieved, 0.000001);
      }
    }
  }

  @Test
  public void testAlterTableAddVectorColumn() throws Exception {
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (pk INTEGER PRIMARY KEY, name VARCHAR)");
      conn.createStatement().execute("ALTER TABLE " + tableName + " ADD v VECTOR(FLOAT, 256)");

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable pTable = pconn.getTable(new PTableKey(null, tableName));
      PColumn pColumn = pTable.getColumnForColumnName("V");
      assertNotNull(pColumn);
      assertEquals(PVectorFloat.INSTANCE, pColumn.getDataType());
      assertEquals(Integer.valueOf(256), pColumn.getMaxLength());

      DatabaseMetaData dbmd = conn.getMetaData();
      try (ResultSet rs = dbmd.getColumns(null, "", tableName, "V")) {
        assertTrue(rs.next());
        assertEquals("VECTOR(FLOAT)", rs.getString("TYPE_NAME"));
        assertEquals(256, rs.getInt("COLUMN_SIZE"));
      }

      String upsertSql = "UPSERT INTO " + tableName + " VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setInt(1, 1);
        ps.setString(2, "test");
        Array array = conn.createArrayOf("FLOAT", new Float[] { 1.0f, 2.0f, 3.0f });
        Float[] vec = new Float[256];
        for (int i = 0; i < 256; i++) {
          vec[i] = (float) i;
        }
        ps.setArray(3, conn.createArrayOf("FLOAT", vec));
        ps.executeUpdate();
      }
      conn.commit();

      try (ResultSet rs =
        conn.createStatement().executeQuery("SELECT v FROM " + tableName + " WHERE pk = 1")) {
        assertTrue(rs.next());
        float[] retrieved = (float[]) rs.getObject(1);
        assertNotNull(retrieved);
        assertEquals(256, retrieved.length);
        assertEquals(0.0f, retrieved[0], 0.00001f);
        assertEquals(255.0f, retrieved[255], 0.00001f);
      }
    }
  }

  @Test
  public void testAlterTableAddVectorDoubleColumn() throws Exception {
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (pk INTEGER PRIMARY KEY, name VARCHAR)");

      conn.createStatement().execute("ALTER TABLE " + tableName + " ADD v VECTOR(DOUBLE, 64)");

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable pTable = pconn.getTable(new PTableKey(null, tableName));
      PColumn pColumn = pTable.getColumnForColumnName("V");
      assertNotNull(pColumn);
      assertEquals(PVectorDouble.INSTANCE, pColumn.getDataType());
      assertEquals(Integer.valueOf(64), pColumn.getMaxLength());
    }
  }

  @Test
  public void testVectorColumnAsPrimaryKeyRejected() throws Exception {
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try {
        conn.createStatement()
          .execute("CREATE TABLE " + tableName + " (v VECTOR(FLOAT, 128) PRIMARY KEY)");
        fail("Should have rejected vector column as primary key");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.INVALID_PRIMARY_KEY_CONSTRAINT.getErrorCode(),
          e.getErrorCode());
      }
    }
  }
}
