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
import java.util.Arrays;
import java.util.List;
import org.apache.phoenix.index.vector.CentroidManager;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class VectorCentroidTableIT extends ParallelStatsDisabledIT {

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

        assertEquals(CENTROID_ID, rsmd.getColumnName(2));
        assertEquals(Types.INTEGER, rsmd.getColumnType(2));
        assertEquals(ResultSetMetaData.columnNoNulls, rsmd.isNullable(2));

        assertEquals(CENTROID_VECTOR, rsmd.getColumnName(3));
        assertEquals(Types.VARBINARY, rsmd.getColumnType(3));
        assertEquals(ResultSetMetaData.columnNullable, rsmd.isNullable(3));

        assertEquals(GENERATION_ID, rsmd.getColumnName(4));
        assertEquals(Types.BIGINT, rsmd.getColumnType(4));
        assertEquals(ResultSetMetaData.columnNullable, rsmd.isNullable(4));
      }
    }
  }

  @Test
  public void testPrimaryKeyConstraintAndUpsert() throws Exception {
    String indexName = "TEST_VECTOR_IDX_" + generateUniqueName();
    byte[] vector1 = new byte[] { 1, 2, 3, 4 };
    byte[] vector2 = new byte[] { 5, 6, 7, 8 };
    byte[] vector3 = new byte[] { 9, 10, 11, 12 };

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String upsertSql = "UPSERT INTO " + SYSTEM_VECTOR_CENTROID_NAME + " (" + INDEX_NAME + ", "
        + CENTROID_ID + ", " + CENTROID_VECTOR + ", " + GENERATION_ID + ") VALUES (?, ?, ?, ?)";

      // Upsert an initial centroid row for the index.
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, indexName);
        ps.setInt(2, 0);
        ps.setBytes(3, vector1);
        ps.setLong(4, 1L);
        ps.executeUpdate();
      }
      conn.commit();

      // Upserting with the same primary key (INDEX_NAME, CENTROID_ID) updates the record in place.
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, indexName);
        ps.setInt(2, 0);
        ps.setBytes(3, vector2);
        ps.setLong(4, 2L);
        ps.executeUpdate();
      }
      conn.commit();

      // Verify that the record was updated rather than duplicated.
      String countSql =
        "SELECT COUNT(*) FROM " + SYSTEM_VECTOR_CENTROID_NAME + " WHERE " + INDEX_NAME + " = ?";
      try (PreparedStatement ps = conn.prepareStatement(countSql)) {
        ps.setString(1, indexName);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals("Expected single row after overwriting PK", 1, rs.getInt(1));
        }
      }

      // Verify updated centroid vector and generation.
      String selectSql = "SELECT " + CENTROID_VECTOR + ", " + GENERATION_ID + " FROM "
        + SYSTEM_VECTOR_CENTROID_NAME + " WHERE " + INDEX_NAME + " = ? AND " + CENTROID_ID + " = ?";
      try (PreparedStatement ps = conn.prepareStatement(selectSql)) {
        ps.setString(1, indexName);
        ps.setInt(2, 0);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertArrayEquals(vector2, rs.getBytes(1));
          assertEquals(2L, rs.getLong(2));
        }
      }

      // Upserting with a distinct centroid ID creates a separate entry for the index.
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, indexName);
        ps.setInt(2, 1);
        ps.setBytes(3, vector3);
        ps.setLong(4, 2L);
        ps.executeUpdate();
      }
      conn.commit();

      // Verify the index now contains two distinct centroid records.
      try (PreparedStatement ps = conn.prepareStatement(countSql)) {
        ps.setString(1, indexName);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals("Expected two centroid rows for distinct centroid IDs", 2, rs.getInt(1));
        }
      }
    }
  }

  @Test
  public void testPTableSchema() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable table = pconn.getTableNoCache(SYSTEM_VECTOR_CENTROID_NAME);
      assertNotNull("SYSTEM.VECTOR_CENTROID PTable must exist", table);

      // Validate primary key column schema
      List<PColumn> pkColumns = table.getPKColumns();
      assertEquals(2, pkColumns.size());
      assertEquals(INDEX_NAME, pkColumns.get(0).getName().getString());
      assertEquals(PVarchar.INSTANCE, pkColumns.get(0).getDataType());
      assertFalse(pkColumns.get(0).isNullable());
      assertEquals(CENTROID_ID, pkColumns.get(1).getName().getString());
      assertEquals(PInteger.INSTANCE, pkColumns.get(1).getDataType());
      assertFalse(pkColumns.get(1).isNullable());

      // Validate non-primary key column schema
      PColumn centroidVectorCol = table.getColumnForColumnName(CENTROID_VECTOR);
      assertNotNull(centroidVectorCol);
      assertEquals(PVarbinary.INSTANCE, centroidVectorCol.getDataType());
      assertTrue(centroidVectorCol.isNullable());

      PColumn generationIdCol = table.getColumnForColumnName(GENERATION_ID);
      assertNotNull(generationIdCol);
      assertEquals(PLong.INSTANCE, generationIdCol.getDataType());
      assertTrue(generationIdCol.isNullable());
    }
  }

  @Test
  public void testDatabaseMetaDataColumns() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      DatabaseMetaData dbmd = conn.getMetaData();
      try (ResultSet rs =
        dbmd.getColumns(null, SYSTEM_CATALOG_SCHEMA, SYSTEM_VECTOR_CENTROID_TABLE, null)) {
        assertTrue("Expected INDEX_NAME column", rs.next());
        assertEquals(INDEX_NAME, rs.getString(COLUMN_NAME));
        assertEquals(Types.VARCHAR, rs.getInt(DATA_TYPE));

        assertTrue("Expected CENTROID_ID column", rs.next());
        assertEquals(CENTROID_ID, rs.getString(COLUMN_NAME));
        assertEquals(Types.INTEGER, rs.getInt(DATA_TYPE));

        assertTrue("Expected CENTROID_VECTOR column", rs.next());
        assertEquals(CENTROID_VECTOR, rs.getString(COLUMN_NAME));
        assertEquals(Types.VARBINARY, rs.getInt(DATA_TYPE));

        assertTrue("Expected GENERATION_ID column", rs.next());
        assertEquals(GENERATION_ID, rs.getString(COLUMN_NAME));
        assertEquals(Types.BIGINT, rs.getInt(DATA_TYPE));

        assertFalse("No additional columns expected", rs.next());
      }
    }
  }

  @Test
  public void testPersistAndLoadRoundTrip() throws Exception {
    String indexName = "TEST_VECTOR_IDX_" + generateUniqueName();
    byte[] v0 = new byte[] { 1, 2, 3, 4 };
    byte[] v1 = new byte[] { 5, 6, 7, 8 };
    byte[] v2 = new byte[] { 9, 10, 11, 12 };
    List<byte[]> centroids = Arrays.asList(v0, v1, v2);

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      CentroidManager manager = new CentroidManager(conn);
      manager.persistCentroids(indexName, 1L, centroids);

      List<byte[]> loaded = manager.loadCentroids(indexName, 1L);
      assertNotNull(loaded);
      assertEquals(3, loaded.size());
      assertArrayEquals(v0, loaded.get(0));
      assertArrayEquals(v1, loaded.get(1));
      assertArrayEquals(v2, loaded.get(2));

      List<byte[]> loadedStatic = CentroidManager.loadCentroids(conn, indexName, 1L);
      assertEquals(3, loadedStatic.size());
      assertArrayEquals(v0, loadedStatic.get(0));
      assertArrayEquals(v1, loadedStatic.get(1));
      assertArrayEquals(v2, loadedStatic.get(2));
    }
  }

  @Test
  public void testGenerationIsolation() throws Exception {
    String indexName = "TEST_VECTOR_IDX_" + generateUniqueName();
    byte[] v0 = new byte[] { 10, 20, 30, 40 };
    byte[] v1 = new byte[] { 50, 60, 70, 80 };
    List<byte[]> centroids = Arrays.asList(v0, v1);

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      CentroidManager manager = new CentroidManager(conn);
      manager.persistCentroids(indexName, 1L, centroids);

      List<byte[]> wrongGen = manager.loadCentroids(indexName, 2L);
      assertNotNull(wrongGen);
      assertTrue("Wrong generation must return no centroids", wrongGen.isEmpty());

      List<byte[]> correctGen = manager.loadCentroids(indexName, 1L);
      assertEquals(2, correctGen.size());
    }
  }

  @Test
  public void testDeleteGeneration() throws Exception {
    String indexName = "TEST_VECTOR_IDX_" + generateUniqueName();
    String indexName2 = "TEST_VECTOR_IDX_" + generateUniqueName();
    byte[] v0 = new byte[] { 1, 1, 1, 1 };
    byte[] v1 = new byte[] { 2, 2, 2, 2 };
    List<byte[]> gen1Centroids = Arrays.asList(v0);
    List<byte[]> gen2Centroids = Arrays.asList(v1);

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      CentroidManager manager = new CentroidManager(conn);
      // Verify that deleting a generation removes only the targeted generation and isolates other
      // index records.
      manager.persistCentroids(indexName, 1L, gen1Centroids);
      manager.persistCentroids(indexName2, 2L, gen2Centroids);

      assertEquals(1, manager.loadCentroids(indexName, 1L).size());
      assertEquals(1, manager.loadCentroids(indexName2, 2L).size());

      manager.deleteGeneration(indexName, 1L);

      assertTrue(manager.loadCentroids(indexName, 1L).isEmpty());
      assertEquals(1, manager.loadCentroids(indexName2, 2L).size());
      assertArrayEquals(v1, manager.loadCentroids(indexName2, 2L).get(0));
    }
  }

  @Test
  public void testIncrementGeneration() throws Exception {
    String indexName = "TEST_VECTOR_IDX_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      CentroidManager manager = new CentroidManager(conn);

      long gen1 = manager.incrementGeneration(indexName);
      long gen2 = manager.incrementGeneration(indexName);
      assertTrue("Returned generations must be monotonically increasing", gen2 > gen1);
      assertEquals(gen1 + 1, gen2);
      assertEquals(gen2, manager.getGeneration(indexName));

      // Verify generation lifecycle operations on a catalog-defined vector index.
      String dataTable = "DATA_" + generateUniqueName();
      String vectorIdx = "IDX_" + generateUniqueName();
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + dataTable + " (ID VARCHAR PRIMARY KEY, V VECTOR(FLOAT, 4))");
        stmt.execute("CREATE VECTOR INDEX " + vectorIdx + " ON " + dataTable + " (V) "
          + "WITH (algorithm = 'IVF', metric = 'L2', dimension = 4, lists = 2, sample_size = 10)");
      }

      long idxGen1 = manager.incrementGeneration(vectorIdx);
      long idxGen2 = manager.incrementGeneration(vectorIdx);
      assertTrue("Generations must be monotonically increasing", idxGen2 > idxGen1);
      assertEquals(idxGen1 + 1, idxGen2);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexPTable = pconn.getTableNoCache(vectorIdx);
      assertNotNull(indexPTable);
      assertEquals(Long.valueOf(idxGen2), indexPTable.getVectorCentroidGeneration());
    }
  }
}
