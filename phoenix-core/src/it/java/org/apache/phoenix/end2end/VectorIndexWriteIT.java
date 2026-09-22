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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.index.IndexTestUtil;
import org.apache.phoenix.expression.SingleCellColumnExpression;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Write pipeline integration tests for vector indexes. */
@Category(ParallelStatsDisabledTest.class)
@RunWith(Parameterized.class)
public class VectorIndexWriteIT extends ParallelStatsDisabledIT {

  private final ImmutableStorageScheme storageScheme;

  public VectorIndexWriteIT(ImmutableStorageScheme storageScheme) {
    this.storageScheme = storageScheme;
  }

  @Parameterized.Parameters(name = "VectorIndexWriteIT_storageScheme={0}")
  public static synchronized Collection<ImmutableStorageScheme> data() {
    return Arrays.asList(ImmutableStorageScheme.ONE_CELL_PER_COLUMN,
      ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS);
  }

  /**
   * Replaces a row by primary key across the configured storage schemes.
   * <p>
   * On immutable tables using single cell storage, deletes precede upserts so that index
   * maintenance observes row replacement rather than appended cell versions.
   */
  private void replaceRow(Connection conn, String tableName, String id, Float[] vector,
    String label) throws SQLException {
    if (storageScheme == ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS) {
      try (PreparedStatement ps =
        conn.prepareStatement("DELETE FROM " + tableName + " WHERE ID = ?")) {
        ps.setString(1, id);
        ps.executeUpdate();
      }
      conn.commit();
    }
    try (PreparedStatement ps =
      conn.prepareStatement("UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)")) {
      ps.setString(1, id);
      if (vector == null) {
        ps.setNull(2, java.sql.Types.ARRAY);
      } else {
        ps.setArray(2, conn.createArrayOf("FLOAT", vector));
      }
      ps.setString(3, label);
      ps.executeUpdate();
    }
    conn.commit();
  }

  private String getTableDdlProps() {
    if (storageScheme == ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS) {
      return " IMMUTABLE_ROWS=true, IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2";
    }
    return " IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN, COLUMN_ENCODED_BYTES=0";
  }

  private void setupTableAndKnownCentroids(Connection conn, String tableName, String indexName)
    throws Exception {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE " + tableName
        + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), LABEL VARCHAR)"
        + getTableDdlProps());
      stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
        + "INCLUDE (LABEL) WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
    }

    List<float[]> knownCentroids = Arrays.asList(new float[] { 0.0f, 0.0f, 0.0f, 1.0f }, // ID 0
      new float[] { 0.0f, 0.0f, 1.0f, 0.0f }, // ID 1
      new float[] { 1.0f, 0.0f, 0.0f, 0.0f }, // ID 2
      new float[] { 0.0f, 1.0f, 0.0f, 0.0f } // ID 3
    );
    VectorIndexTestUtil.activateWithKnownCentroids(conn, tableName, indexName, knownCentroids, 1L);
  }

  @Test
  public void testVectorInsertGeneratesIndexRow() throws Exception {
    String tableName = "T_VEC_INS_" + generateUniqueName();
    String indexName = "IDX_VEC_INS_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupTableAndKnownCentroids(conn, tableName, indexName);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);

      Float[] vectorFloats = new Float[] { 1.0f, 0.0f, 0.0f, 0.0f };
      // Upsert a row with vector [1,0,0,0] (nearest centroid is ID 2)
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "row_1");
        ps.setArray(2, conn.createArrayOf("FLOAT", vectorFloats));
        ps.setString(3, "lbl_1");
        ps.executeUpdate();
      }
      conn.commit();

      // Verify raw HBase index table contains exactly 1 row with centroid ID 2
      List<byte[]> rowKeys = VectorIndexTestUtil.getHBaseRowKeys(pconn, indexTable);
      assertEquals("Expected exactly 1 index row", 1, rowKeys.size());
      assertEquals("Centroid prefix must be 2", 2,
        VectorIndexTestUtil.extractCentroidId(rowKeys.get(0), false));

      // Also assert raw index row's 0:V cell bytes equal PVectorFloat.INSTANCE.toBytes(vector)
      byte[] physicalIndexName = indexTable.getPhysicalName().getBytes();
      PColumn vectorCol = indexTable.getColumnForColumnName("0:V");
      byte[] vecCF = vectorCol.getFamilyName().getBytes();
      byte[] vecCQ = vectorCol.getColumnQualifierBytes();
      try (Table hIndexTable = pconn.getQueryServices().getTable(physicalIndexName)) {
        Result r = hIndexTable.get(new Get(rowKeys.get(0)));
        byte[] actualVecBytes;
        if (storageScheme == ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS) {
          SingleCellColumnExpression colExpr =
            new SingleCellColumnExpression(vectorCol, vectorCol.getName().getString(),
              indexTable.getEncodingScheme(), indexTable.getImmutableStorageScheme());
          ImmutableBytesPtr ptr = new ImmutableBytesPtr();
          assertTrue("Single-cell expression must evaluate to a non-null vector",
            colExpr.evaluate(new ResultTuple(r), ptr));
          actualVecBytes = ptr.copyBytesIfNecessary();
        } else {
          actualVecBytes = r.getValue(vecCF, vecCQ);
        }
        assertNotNull("Vector column 0:V must be present in index table", actualVecBytes);
        byte[] expectedVecBytes = PVectorFloat.INSTANCE.toBytes(vectorFloats);
        assertTrue("Stored vector payload must match expected bytes",
          Bytes.equals(expectedVecBytes, actualVecBytes));
      }

      // Verify via SQL scan
      String selectSql = "SELECT \":CENTROID_ID\", \":ID\", \"0:LABEL\" FROM " + indexName;
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(selectSql)) {
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("row_1", rs.getString(2));
        assertEquals("lbl_1", rs.getString(3));
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void testVectorUpdateWithCentroidChange() throws Exception {
    String tableName = "T_VEC_UPD_" + generateUniqueName();
    String indexName = "IDX_VEC_UPD_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupTableAndKnownCentroids(conn, tableName, indexName);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);

      // Upsert initial row: vector [1,0,0,0] -> centroid ID 2
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "row_1");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        ps.setString(3, "lbl_1");
        ps.executeUpdate();
      }
      conn.commit();

      List<byte[]> rowKeysBefore = VectorIndexTestUtil.getHBaseRowKeys(pconn, indexTable);
      assertEquals(1, rowKeysBefore.size());
      assertEquals(2, VectorIndexTestUtil.extractCentroidId(rowKeysBefore.get(0), false));

      // Update vector to [0,0,0,1] -> nearest centroid ID 0
      replaceRow(conn, tableName, "row_1", new Float[] { 0.0f, 0.0f, 0.0f, 1.0f }, "lbl_updated");

      // Verify old row (centroid 2) is deleted and new row (centroid 0) exists
      List<byte[]> rowKeysAfter = VectorIndexTestUtil.getHBaseRowKeys(pconn, indexTable);
      assertEquals("Expected exactly 1 index row after vector update", 1, rowKeysAfter.size());
      assertEquals("Centroid prefix must be updated to 0", 0,
        VectorIndexTestUtil.extractCentroidId(rowKeysAfter.get(0), false));

      // Verify via SQL scan
      String selectSql = "SELECT \":CENTROID_ID\", \":ID\", \"0:LABEL\" FROM " + indexName;
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(selectSql)) {
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        assertEquals("row_1", rs.getString(2));
        assertEquals("lbl_updated", rs.getString(3));
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void testVectorCoveredColumnUpdateWithoutVectorChange() throws Exception {
    String tableName = "T_VEC_COV_" + generateUniqueName();
    String indexName = "IDX_VEC_COV_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupTableAndKnownCentroids(conn, tableName, indexName);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);

      // Upsert initial row with vector [1,0,0,0] -> centroid ID 2
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "row_1");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        ps.setString(3, "initial_label");
        ps.executeUpdate();
      }
      conn.commit();

      List<byte[]> rowKeysBefore = VectorIndexTestUtil.getHBaseRowKeys(pconn, indexTable);
      assertEquals(1, rowKeysBefore.size());
      assertEquals(2, VectorIndexTestUtil.extractCentroidId(rowKeysBefore.get(0), false));

      // Update ONLY covered column (same vector, new label)
      replaceRow(conn, tableName, "row_1", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }, "updated_label");

      // Verify row key (and centroid prefix) is unchanged
      List<byte[]> rowKeysAfter = VectorIndexTestUtil.getHBaseRowKeys(pconn, indexTable);
      assertEquals("Expected exactly 1 index row", 1, rowKeysAfter.size());
      assertEquals("Centroid prefix must still be 2", 2,
        VectorIndexTestUtil.extractCentroidId(rowKeysAfter.get(0), false));

      // Verify covered column value was updated in place
      String selectSql = "SELECT \":CENTROID_ID\", \":ID\", \"0:LABEL\" FROM " + indexName;
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(selectSql)) {
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("row_1", rs.getString(2));
        assertEquals("updated_label", rs.getString(3));
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void testVectorDeleteRemovesIndexRow() throws Exception {
    String tableName = "T_VEC_DEL_" + generateUniqueName();
    String indexName = "IDX_VEC_DEL_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupTableAndKnownCentroids(conn, tableName, indexName);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);

      // Upsert a row with vector [1,0,0,0] -> centroid ID 2
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "row_1");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        ps.setString(3, "lbl_1");
        ps.executeUpdate();
      }
      conn.commit();

      List<byte[]> rowKeysBefore = VectorIndexTestUtil.getHBaseRowKeys(pconn, indexTable);
      assertEquals(1, rowKeysBefore.size());

      // Delete the base table row
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("DELETE FROM " + tableName + " WHERE ID = 'row_1'");
      }
      conn.commit();

      // Verify index table is completely empty
      List<byte[]> rowKeysAfter = VectorIndexTestUtil.getHBaseRowKeys(pconn, indexTable);
      assertEquals("Expected 0 index rows after delete", 0, rowKeysAfter.size());

      // Verify via SQL scan
      String selectSql = "SELECT COUNT(*) FROM " + indexName;
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(selectSql)) {
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
      }
    }
  }

  @Test
  public void testNullVectorExcludedFromIndex() throws Exception {
    String tableName = "T_VEC_NULL_" + generateUniqueName();
    String indexName = "IDX_VEC_NULL_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupTableAndKnownCentroids(conn, tableName, indexName);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);

      // Upsert a row with a null vector.
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "row_null");
        ps.setNull(2, java.sql.Types.ARRAY);
        ps.setString(3, "null_label");
        ps.executeUpdate();
      }
      conn.commit();

      // Verify index table has 0 rows (null vector excluded from index)
      List<byte[]> rowKeys = VectorIndexTestUtil.getHBaseRowKeys(pconn, indexTable);
      assertEquals("Null vector must not produce an index row", 0, rowKeys.size());

      // Verify base table has the row
      try (Statement stmt = conn.createStatement(); ResultSet rs =
        stmt.executeQuery("SELECT ID, LABEL FROM " + tableName + " WHERE ID = 'row_null'")) {
        assertTrue("Base table must contain the null vector row", rs.next());
        assertEquals("row_null", rs.getString(1));
        assertEquals("null_label", rs.getString(2));
      }

      // Update the row from null to a populated vector.
      replaceRow(conn, tableName, "row_null", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f },
        "now_has_vector");

      List<byte[]> rowKeysAfterRealVec = VectorIndexTestUtil.getHBaseRowKeys(pconn, indexTable);
      assertEquals("Index row must exist after transition from NULL to real vector", 1,
        rowKeysAfterRealVec.size());
      assertEquals("Centroid prefix must be 2", 2,
        VectorIndexTestUtil.extractCentroidId(rowKeysAfterRealVec.get(0), false));

      // Update back to null and verify removal from the index.
      replaceRow(conn, tableName, "row_null", null, "back_to_null");

      List<byte[]> rowKeysAfterNullAgain = VectorIndexTestUtil.getHBaseRowKeys(pconn, indexTable);
      assertEquals("Index row must be removed after transition back to NULL", 0,
        rowKeysAfterNullAgain.size());
    }
  }

  @Test
  public void testVectorUnchangedUpdateMaintainsIndexRowAndCoveredColumns() throws Exception {
    String tableName = "T_VEC_ZUPD_" + generateUniqueName();
    String indexName = "IDX_VEC_ZUPD_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupTableAndKnownCentroids(conn, tableName, indexName);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);

      // Insert initial row
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "row_alloc_u");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        ps.setString(3, "lbl_initial");
        ps.executeUpdate();
      }
      conn.commit();

      // Verify initial index row
      List<byte[]> rowKeysBefore = VectorIndexTestUtil.getHBaseRowKeys(pconn, indexTable);
      assertEquals(1, rowKeysBefore.size());
      assertEquals(2, VectorIndexTestUtil.extractCentroidId(rowKeysBefore.get(0), false));

      // Partial update modifying only the covered non-vector column.
      if (storageScheme == ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS) {
        replaceRow(conn, tableName, "row_alloc_u", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f },
          "lbl_updated");
      } else {
        try (Statement stmt = conn.createStatement()) {
          stmt.execute(
            "UPSERT INTO " + tableName + " (ID, LABEL) VALUES ('row_alloc_u', 'lbl_updated')");
        }
        conn.commit();
      }

      // Verify row key is maintained in place (not deleted or recreated)
      List<byte[]> rowKeysAfterPartial = VectorIndexTestUtil.getHBaseRowKeys(pconn, indexTable);
      assertEquals("Expected exactly 1 index row after partial update", 1,
        rowKeysAfterPartial.size());
      assertEquals(2, VectorIndexTestUtil.extractCentroidId(rowKeysAfterPartial.get(0), false));

      // Ensure unchanged vector columns omitted from partial update Puts are preserved rather
      // than treated as null updates emitting column deletions.
      PColumn indexVecCol = indexTable.getColumnForColumnName("0:V");
      try (
        Table hTable = pconn.getQueryServices().getTable(indexTable.getPhysicalName().getBytes())) {
        Result r = hTable.get(new Get(rowKeysAfterPartial.get(0)));
        byte[] storedVec;
        if (storageScheme == ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS) {
          SingleCellColumnExpression colExpr =
            new SingleCellColumnExpression(indexVecCol, indexVecCol.getName().getString(),
              indexTable.getEncodingScheme(), indexTable.getImmutableStorageScheme());
          ImmutableBytesPtr ptr = new ImmutableBytesPtr();
          assertTrue(colExpr.evaluate(new ResultTuple(r), ptr));
          storedVec = ptr.copyBytesIfNecessary();
        } else {
          storedVec = r.getValue(indexVecCol.getFamilyName().getBytes(),
            indexVecCol.getColumnQualifierBytes());
        }
        assertNotNull("index row must still carry the vector after a covered-only update",
          storedVec);
        assertArrayEquals(PVectorFloat.INSTANCE.toBytes(new float[] { 1.0f, 0.0f, 0.0f, 0.0f }),
          storedVec);
      }

      // Full update with identical vector values.
      replaceRow(conn, tableName, "row_alloc_u", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f },
        "lbl_updated_again");

      // Verify row in index table is maintained with centroid prefix 2
      List<byte[]> rowKeys = VectorIndexTestUtil.getHBaseRowKeys(pconn, indexTable);
      assertEquals("Expected exactly 1 index row after full unchanged update", 1, rowKeys.size());
      assertEquals(2, VectorIndexTestUtil.extractCentroidId(rowKeys.get(0), false));

      // Verify covered column was updated in place
      String selectSql = "SELECT \":CENTROID_ID\", \":ID\", \"0:LABEL\" FROM " + indexName;
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(selectSql)) {
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("row_alloc_u", rs.getString(2));
        assertEquals("lbl_updated_again", rs.getString(3));
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void testUnchangedVectorCoveredUpdateOnSingleCellIndex() throws Exception {
    String tableName = "T_VEC_SC_" + generateUniqueName();
    String indexName = "IDX_VEC_SC_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), LABEL VARCHAR)");
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
          + "INCLUDE (LABEL) WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100, "
          + "IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2)");
      }

      List<float[]> knownCentroids = Arrays.asList(new float[] { 0.0f, 0.0f, 0.0f, 1.0f }, // ID 0
        new float[] { 0.0f, 0.0f, 1.0f, 0.0f }, // ID 1
        new float[] { 1.0f, 0.0f, 0.0f, 0.0f }, // ID 2
        new float[] { 0.0f, 1.0f, 0.0f, 0.0f } // ID 3
      );
      VectorIndexTestUtil.activateWithKnownCentroids(conn, tableName, indexName, knownCentroids,
        1L);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable dataTable = pconn.getTableNoCache(tableName);
      PTable indexTable = pconn.getTableNoCache(indexName);
      assertEquals(ImmutableStorageScheme.ONE_CELL_PER_COLUMN,
        dataTable.getImmutableStorageScheme());
      assertEquals(ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS,
        indexTable.getImmutableStorageScheme());

      float[] initialVector = new float[] { 1.0f, 0.0f, 0.0f, 0.0f };
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "row_sc_1");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        ps.setString(3, "lbl_initial");
        ps.executeUpdate();
      }
      conn.commit();

      // Verify initial index row exists with centroid 2
      List<byte[]> rowKeysBefore = VectorIndexTestUtil.getHBaseRowKeys(pconn, indexTable);
      assertEquals(1, rowKeysBefore.size());
      assertEquals(2, VectorIndexTestUtil.extractCentroidId(rowKeysBefore.get(0), false));

      // Update only the covered non-vector column (unchanged vector)
      try (Statement stmt = conn.createStatement()) {
        stmt
          .execute("UPSERT INTO " + tableName + " (ID, LABEL) VALUES ('row_sc_1', 'lbl_updated')");
      }
      conn.commit();

      // Verify index row key is maintained in place
      List<byte[]> rowKeysAfter = VectorIndexTestUtil.getHBaseRowKeys(pconn, indexTable);
      assertEquals("Expected exactly 1 index row after covered-only update", 1,
        rowKeysAfter.size());
      assertEquals(2, VectorIndexTestUtil.extractCentroidId(rowKeysAfter.get(0), false));

      // Verify the single cell array retains the vector column after covered column update
      PColumn indexVecCol = indexTable.getColumnForColumnName("0:V");
      try (
        Table hTable = pconn.getQueryServices().getTable(indexTable.getPhysicalName().getBytes())) {
        Result r = hTable.get(new Get(rowKeysAfter.get(0)));
        byte[] singleCellBytes = r.getValue(indexVecCol.getFamilyName().getBytes(),
          QueryConstants.SINGLE_KEYVALUE_COLUMN_QUALIFIER_BYTES);
        assertNotNull("Index row must have the single-cell array in HBase", singleCellBytes);

        SingleCellColumnExpression colExpr =
          new SingleCellColumnExpression(indexVecCol, indexVecCol.getName().getString(),
            indexTable.getEncodingScheme(), indexTable.getImmutableStorageScheme());
        ImmutableBytesPtr ptr = new ImmutableBytesPtr();
        assertTrue("Single-cell expression must evaluate to a non-null vector",
          colExpr.evaluate(new ResultTuple(r), ptr));
        assertArrayEquals(
          "Index row must still carry the original vector after covered-only update",
          PVectorFloat.INSTANCE.toBytes(initialVector), ptr.copyBytesIfNecessary());
      }

      // Assert top-k query over the index returns the row
      String querySql =
        "SELECT ID, LABEL FROM " + tableName + " ORDER BY L2_DISTANCE(V, ?) LIMIT 1";
      try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + querySql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", new Float[] { 0.9f, 0.0f, 0.0f, 0.0f }));
        try (ResultSet rs = ps.executeQuery()) {
          String plan = QueryUtil.getExplainPlan(rs);
          assertTrue("EXPLAIN plan must reference index " + indexName + ": " + plan,
            plan.contains(indexName));
        }
      }

      try (PreparedStatement ps = conn.prepareStatement(querySql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", new Float[] { 0.9f, 0.0f, 0.0f, 0.0f }));
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue("Top-k query must return the row", rs.next());
          assertEquals("row_sc_1", rs.getString(1));
          assertEquals("lbl_updated", rs.getString(2));
          assertFalse(rs.next());
        }
      }
    }
  }

  @Test
  public void testVectorCoveredColumnTranscoding() throws Exception {
    String tableName = "T_VEC_COV_TR_" + generateUniqueName();
    String indexName = "IDX_VEC_COV_TR_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), COV_V VECTOR(FLOAT, 3))"
          + getTableDdlProps());
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
          + "INCLUDE (COV_V) WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }

      List<float[]> knownCentroids = Arrays.asList(new float[] { 0.0f, 0.0f, 0.0f, 1.0f }, // ID 0
        new float[] { 0.0f, 0.0f, 1.0f, 0.0f }, // ID 1
        new float[] { 1.0f, 0.0f, 0.0f, 0.0f }, // ID 2
        new float[] { 0.0f, 1.0f, 0.0f, 0.0f } // ID 3
      );
      VectorIndexTestUtil.activateWithKnownCentroids(conn, tableName, indexName, knownCentroids,
        1L);

      // Upsert a row with both indexed vector V and covered vector COV_V
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, COV_V) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "trans_row_1");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f })); // Nearest:
                                                                                             // ID 2
        ps.setArray(3, conn.createArrayOf("FLOAT", new Float[] { 10.5f, -2.25f, 3.125f }));
        ps.executeUpdate();
      }
      conn.commit();

      // Verify index table has the covered vector transcoded correctly
      String selectSql = "SELECT \":CENTROID_ID\", \":ID\", \"0:COV_V\" FROM " + indexName;
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(selectSql)) {
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("trans_row_1", rs.getString(2));
        Object obj = rs.getObject(3);
        assertNotNull(obj);
        assertTrue("Expected float[] from getObject on VECTOR(FLOAT) column",
          obj instanceof float[]);
        float[] actualFloats = (float[]) obj;
        assertEquals(3, actualFloats.length);
        assertEquals(10.5f, actualFloats[0], 1e-6f);
        assertEquals(-2.25f, actualFloats[1], 1e-6f);
        assertEquals(3.125f, actualFloats[2], 1e-6f);
        assertFalse(rs.next());
      }

      // Update covered vector while main vector is unchanged
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "trans_row_1");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        ps.setArray(3, conn.createArrayOf("FLOAT", new Float[] { 9.0f, 8.0f, 7.0f }));
        ps.executeUpdate();
      }
      conn.commit();

      // Verify covered vector was updated in the index row
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(selectSql)) {
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("trans_row_1", rs.getString(2));
        Object obj = rs.getObject(3);
        assertNotNull(obj);
        assertTrue("Expected float[] from getObject on VECTOR(FLOAT) column",
          obj instanceof float[]);
        float[] actualFloats = (float[]) obj;
        assertEquals(3, actualFloats.length);
        assertEquals(9.0f, actualFloats[0], 1e-6f);
        assertEquals(8.0f, actualFloats[1], 1e-6f);
        assertEquals(7.0f, actualFloats[2], 1e-6f);
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void testMixedVectorCoveredColumnTranscoding() throws Exception {
    String tableName = "T_VEC_MIX_TR_" + generateUniqueName();
    String indexName = "IDX_VEC_MIX_TR_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), COV_D VECTOR(DOUBLE, 3))"
          + getTableDdlProps());
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
          + "INCLUDE (COV_D) WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }

      List<float[]> knownCentroids = Arrays.asList(new float[] { 0.0f, 0.0f, 0.0f, 1.0f }, // ID 0
        new float[] { 0.0f, 0.0f, 1.0f, 0.0f }, // ID 1
        new float[] { 1.0f, 0.0f, 0.0f, 0.0f }, // ID 2
        new float[] { 0.0f, 1.0f, 0.0f, 0.0f } // ID 3
      );
      VectorIndexTestUtil.activateWithKnownCentroids(conn, tableName, indexName, knownCentroids,
        1L);

      // Upsert a row with indexed VECTOR(FLOAT, 4) and covered VECTOR(DOUBLE, 3)
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, COV_D) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "mix_row_1");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 0.0f, 0.0f, 1.0f, 0.0f })); // Nearest:
                                                                                             // ID 1
        ps.setArray(3, conn.createArrayOf("DOUBLE",
          new Double[] { 1.12345678901234, -2.98765432109876, 3.14159265358979 }));
        ps.executeUpdate();
      }
      conn.commit();

      // Verify index table has the covered double vector transcoded correctly with 8-byte element
      // stride
      String selectSql = "SELECT \":CENTROID_ID\", \":ID\", \"0:COV_D\" FROM " + indexName;
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(selectSql)) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("mix_row_1", rs.getString(2));
        Object obj = rs.getObject(3);
        assertNotNull(obj);
        assertTrue("Expected double[] from getObject on VECTOR(DOUBLE) column",
          obj instanceof double[]);
        double[] actualDoubles = (double[]) obj;
        assertEquals(3, actualDoubles.length);
        assertEquals(1.12345678901234, actualDoubles[0], 1e-12);
        assertEquals(-2.98765432109876, actualDoubles[1], 1e-12);
        assertEquals(3.14159265358979, actualDoubles[2], 1e-12);
        assertFalse(rs.next());
      }

      // Update covered double vector while main float vector is unchanged
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "mix_row_1");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 0.0f, 0.0f, 1.0f, 0.0f }));
        ps.setArray(3, conn.createArrayOf("DOUBLE",
          new Double[] { 9.87654321012345, 8.76543210987654, 7.65432109876543 }));
        ps.executeUpdate();
      }
      conn.commit();

      // Verify covered double vector was updated in the index row
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(selectSql)) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("mix_row_1", rs.getString(2));
        Object obj = rs.getObject(3);
        assertNotNull(obj);
        assertTrue("Expected double[] from getObject on VECTOR(DOUBLE) column",
          obj instanceof double[]);
        double[] actualDoubles = (double[]) obj;
        assertEquals(3, actualDoubles.length);
        assertEquals(9.87654321012345, actualDoubles[0], 1e-12);
        assertEquals(8.76543210987654, actualDoubles[1], 1e-12);
        assertEquals(7.65432109876543, actualDoubles[2], 1e-12);
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void testVerifiedMarkerPresentAfterCommit() throws Exception {
    String tableName = "T_VEC_VER_" + generateUniqueName();
    String indexName = "IDX_VEC_VER_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupTableAndKnownCentroids(conn, tableName, indexName);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);
      byte[] physicalIndexName = indexTable.getPhysicalName().getBytes();

      // Upsert a row with vector [1,0,0,0] (nearest centroid is ID 2)
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "row_v1");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        ps.setString(3, "lbl_v1");
        ps.executeUpdate();
      }
      conn.commit();

      byte[] emptyCF = SchemaUtil.getEmptyColumnFamily(indexTable);
      byte[] emptyCQ = EncodedColumnsUtil.getEmptyKeyValueInfo(indexTable).getFirst();

      // Scan index table raw via HBase Table API (bypassing GlobalIndexChecker)
      try (Table hTable = pconn.getQueryServices().getTable(physicalIndexName);
        ResultScanner scanner = hTable.getScanner(new Scan())) {
        Result result = scanner.next();
        assertNotNull("Expected index row in HBase table", result);

        byte[] emptyColVal = result.getValue(emptyCF, emptyCQ);
        assertNotNull("Empty column marker cell must be present on index row", emptyColVal);
        assertTrue("Empty column marker must be VERIFIED_BYTES",
          Bytes.equals(QueryConstants.VERIFIED_BYTES, emptyColVal));

        assertNull("Expected exactly 1 index row", scanner.next());
      }

      // Also verify via IndexTestUtil
      IndexTestUtil.assertRowsForEmptyColValue(conn, indexName, QueryConstants.VERIFIED_BYTES);
    }
  }

  @Test
  public void testReadRepairWithCentroidReassignment() throws Exception {
    String tableName = "T_VEC_RR_" + generateUniqueName();
    String indexName = "IDX_VEC_RR_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupTableAndKnownCentroids(conn, tableName, indexName);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);
      byte[] physicalIndexName = indexTable.getPhysicalName().getBytes();

      // Upsert a row in data table with vector [1,0,0,0] -> nearest centroid is ID 2
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "repair_row_1");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        ps.setString(3, "correct_label");
        ps.executeUpdate();
      }
      conn.commit();

      byte[] centroid0RowKey =
        ByteUtil.concat(PInteger.INSTANCE.toBytes(0), Bytes.toBytes("repair_row_1"));
      byte[] centroid2RowKey =
        ByteUtil.concat(PInteger.INSTANCE.toBytes(2), Bytes.toBytes("repair_row_1"));

      byte[] emptyCF = SchemaUtil.getEmptyColumnFamily(indexTable);
      byte[] emptyCQ = EncodedColumnsUtil.getEmptyKeyValueInfo(indexTable).getFirst();
      PColumn labelCol = indexTable.getColumnForColumnName("0:LABEL");
      byte[] labelCF = labelCol.getFamilyName().getBytes();
      byte[] labelCQ = labelCol.getColumnQualifierBytes();

      // Verify that after commit, Centroid 2 row exists and is verified
      try (Table hIndexTable = pconn.getQueryServices().getTable(physicalIndexName)) {
        Result r2 = hIndexTable.get(new Get(centroid2RowKey));
        assertFalse("Centroid 2 row should exist after commit", r2.isEmpty());
        assertTrue(Bytes.equals(QueryConstants.VERIFIED_BYTES, r2.getValue(emptyCF, emptyCQ)));

        // Simulate an unverified index row resulting from a failed write by deleting the current
        // index
        // row and writing an unverified entry under centroid 0. An explicit timestamp ensures the
        // unverified row is newer than the tombstone so that read repair is not masked.
        long deleteTs = EnvironmentEdgeManager.currentTimeMillis();
        hIndexTable.delete(new Delete(centroid2RowKey, deleteTs));

        long staleTs = deleteTs + 1;
        Put stalePut = new Put(centroid0RowKey);
        stalePut.addColumn(emptyCF, emptyCQ, staleTs, QueryConstants.UNVERIFIED_BYTES);
        if (storageScheme != ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS) {
          stalePut.addColumn(labelCF, labelCQ, staleTs, Bytes.toBytes("stale_label"));
        }
        hIndexTable.put(stalePut);

        // Verify pre-repair HBase state
        Result r0Before = hIndexTable.get(new Get(centroid0RowKey));
        assertFalse("Centroid 0 row must exist before repair", r0Before.isEmpty());
        assertTrue("Centroid 0 row must be unverified",
          Bytes.equals(QueryConstants.UNVERIFIED_BYTES, r0Before.getValue(emptyCF, emptyCQ)));

        Result r2Before = hIndexTable.get(new Get(centroid2RowKey));
        assertTrue("Centroid 2 row must not exist before repair", r2Before.isEmpty());
      }

      // Read through the index: this must trigger read repair in GlobalIndexChecker
      String selectSql = "SELECT \":CENTROID_ID\", \":ID\", \"0:LABEL\" FROM " + indexName;
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(selectSql)) {
        assertTrue("Query through index should return the repaired row", rs.next());
        assertEquals("Repaired centroid ID must be 2", 2, rs.getInt(1));
        assertEquals("repair_row_1", rs.getString(2));
        assertEquals("correct_label", rs.getString(3));
        assertFalse("Only one row should be returned", rs.next());
      }

      // Verify post-repair HBase state
      try (Table hIndexTable = pconn.getQueryServices().getTable(physicalIndexName)) {
        Result r0After = hIndexTable.get(new Get(centroid0RowKey));
        assertTrue("Stale Centroid 0 row must be deleted after read repair", r0After.isEmpty());

        Result r2After = hIndexTable.get(new Get(centroid2RowKey));
        assertFalse("Repaired Centroid 2 row must exist after read repair", r2After.isEmpty());
        assertTrue("Repaired Centroid 2 row must have VERIFIED marker",
          Bytes.equals(QueryConstants.VERIFIED_BYTES, r2After.getValue(emptyCF, emptyCQ)));
        String repairedLabel;
        if (storageScheme == ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS) {
          SingleCellColumnExpression labelExpr =
            new SingleCellColumnExpression(labelCol, labelCol.getName().getString(),
              indexTable.getEncodingScheme(), indexTable.getImmutableStorageScheme());
          ImmutableBytesPtr ptr = new ImmutableBytesPtr();
          assertTrue(labelExpr.evaluate(new ResultTuple(r2After), ptr));
          repairedLabel = Bytes.toString(ptr.copyBytesIfNecessary());
        } else {
          repairedLabel = Bytes.toString(r2After.getValue(labelCF, labelCQ));
        }
        assertEquals("Repaired row must have correct label", "correct_label", repairedLabel);
      }
    }
  }
}
