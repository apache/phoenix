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

import static org.apache.phoenix.end2end.VectorIndexTestUtil.KNOWN_CENTROIDS;
import static org.apache.phoenix.end2end.VectorIndexTestUtil.assertIndexVerifies;
import static org.apache.phoenix.end2end.VectorIndexTestUtil.extractCentroidId;
import static org.apache.phoenix.end2end.VectorIndexTestUtil.findNearestCentroid;
import static org.apache.phoenix.end2end.VectorIndexTestUtil.getHBaseRowKeys;
import static org.apache.phoenix.end2end.VectorIndexTestUtil.loadRandomVectors;
import static org.apache.phoenix.end2end.VectorIndexTestUtil.recordKnownCentroids;
import static org.apache.phoenix.end2end.VectorIndexTestUtil.setupTableAndKnownCentroids;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hbase.HConstants;
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
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVectorDouble;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Integration tests for write path vector index maintenance, covering centroid key assignment,
 * dynamic partition reassignment, NULL vector filtering, covered column preservation, verification
 * markers, read repair, and immutable client side maintenance.
 */
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

  private boolean isSingleCell() {
    return storageScheme == ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS;
  }

  /**
   * Initializes the base table and associated vector index for the parameterized storage scheme.
   */
  private void setup(Connection conn, String tableName, String indexName) throws Exception {
    setupTableAndKnownCentroids(conn, tableName, indexName, "FLOAT",
      isSingleCell()
        ? "IMMUTABLE_ROWS=true, IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS,"
          + " COLUMN_ENCODED_BYTES=2"
        : "IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN, COLUMN_ENCODED_BYTES=0");
  }

  /**
   * Replaces a data row. For immutable single cell tables, deletes the prior row before upserting
   * to ensure complete index row replacement without prior row reads.
   */
  private void replaceRow(Connection conn, String tableName, String id, Float[] vector,
    String label) throws SQLException {
    if (isSingleCell()) {
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
      ps.setArray(2, conn.createArrayOf("FLOAT", vector));
      ps.setString(3, label);
      ps.executeUpdate();
    }
    conn.commit();
  }

  /**
   * Extracts the serialized vector bytes from an index row across one cell per column and single
   * cell storage schemes.
   */
  private byte[] getIndexedVector(PhoenixConnection pconn, PTable indexTable, byte[] rowKey)
    throws Exception {
    PColumn vectorCol = indexTable.getColumnForColumnName("0:V");
    try (
      Table hTable = pconn.getQueryServices().getTable(indexTable.getPhysicalName().getBytes())) {
      Result r = hTable.get(new Get(rowKey));
      if (indexTable.getImmutableStorageScheme() == ImmutableStorageScheme.ONE_CELL_PER_COLUMN) {
        return r.getValue(vectorCol.getFamilyName().getBytes(),
          vectorCol.getColumnQualifierBytes());
      }
      ImmutableBytesPtr ptr = new ImmutableBytesPtr();
      SingleCellColumnExpression expr =
        new SingleCellColumnExpression(vectorCol, vectorCol.getName().getString(),
          indexTable.getEncodingScheme(), indexTable.getImmutableStorageScheme());
      return expr.evaluate(new ResultTuple(r), ptr) && ptr.getLength() > 0
        ? ptr.copyBytesIfNecessary()
        : null;
    }
  }

  @Test
  public void testVectorInsertGeneratesIndexRow() throws Exception {
    String tableName = "T_VEC_INS_" + generateUniqueName();
    String indexName = "IDX_VEC_INS_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setup(conn, tableName, indexName);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);

      // Upsert vector assigned to centroid partition 2
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "row_1");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        ps.setString(3, "lbl_1");
        ps.executeUpdate();
      }
      conn.commit();

      // Verify physical index table row key structure and centroid partition
      List<byte[]> rowKeys = getHBaseRowKeys(pconn, indexTable);
      assertEquals("Expected exactly 1 index row", 1, rowKeys.size());
      assertEquals("Centroid prefix must be 2", 2, extractCentroidId(rowKeys.get(0)));
      assertEquals(storageScheme, indexTable.getImmutableStorageScheme());
      assertArrayEquals(PVectorFloat.INSTANCE.toBytes(new float[] { 1.0f, 0.0f, 0.0f, 0.0f }),
        getIndexedVector(pconn, indexTable, rowKeys.get(0)));

      // Query index table via SQL
      String selectSql = "SELECT \"_CENTROID_ID\", \":ID\", \"0:LABEL\" FROM " + indexName;
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
      setup(conn, tableName, indexName);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);

      // Initial insert assigned to centroid 2
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "row_1");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        ps.setString(3, "lbl_1");
        ps.executeUpdate();
      }
      conn.commit();

      List<byte[]> rowKeysBefore = getHBaseRowKeys(pconn, indexTable);
      assertEquals(1, rowKeysBefore.size());
      assertEquals(2, extractCentroidId(rowKeysBefore.get(0)));

      // Update vector to [0,0,0,1] -> nearest centroid ID 0
      replaceRow(conn, tableName, "row_1", new Float[] { 0.0f, 0.0f, 0.0f, 1.0f }, "lbl_updated");

      // Assert old index key deletion and new index key creation across partitions
      List<byte[]> rowKeysAfter = getHBaseRowKeys(pconn, indexTable);
      assertEquals("Expected exactly 1 index row after vector update", 1, rowKeysAfter.size());
      assertEquals("Centroid prefix must be updated to 0", 0,
        extractCentroidId(rowKeysAfter.get(0)));

      // Query index table via SQL
      String selectSql = "SELECT \"_CENTROID_ID\", \":ID\", \"0:LABEL\" FROM " + indexName;
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
      setup(conn, tableName, indexName);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);

      // Initial insert assigned to centroid 2
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "row_1");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        ps.setString(3, "initial_label");
        ps.executeUpdate();
      }
      conn.commit();

      List<byte[]> rowKeysBefore = getHBaseRowKeys(pconn, indexTable);
      assertEquals(1, rowKeysBefore.size());
      assertEquals(2, extractCentroidId(rowKeysBefore.get(0)));

      // Update ONLY covered column (same vector, new label)
      replaceRow(conn, tableName, "row_1", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }, "updated_label");

      // Assert index row key and centroid partition remain unchanged
      List<byte[]> rowKeysAfter = getHBaseRowKeys(pconn, indexTable);
      assertEquals("Expected exactly 1 index row", 1, rowKeysAfter.size());
      assertEquals("Centroid prefix must still be 2", 2, extractCentroidId(rowKeysAfter.get(0)));

      // Verify covered column update
      String selectSql = "SELECT \"_CENTROID_ID\", \":ID\", \"0:LABEL\" FROM " + indexName;
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
      setup(conn, tableName, indexName);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);

      // Upsert row assigned to centroid 2
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "row_1");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        ps.setString(3, "lbl_1");
        ps.executeUpdate();
      }
      conn.commit();

      List<byte[]> rowKeysBefore = getHBaseRowKeys(pconn, indexTable);
      assertEquals(1, rowKeysBefore.size());

      // Delete base table row
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("DELETE FROM " + tableName + " WHERE ID = 'row_1'");
      }
      conn.commit();

      // Assert physical index row deletion
      List<byte[]> rowKeysAfter = getHBaseRowKeys(pconn, indexTable);
      assertEquals("Expected 0 index rows after delete", 0, rowKeysAfter.size());

      // Query index table via SQL
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
      setup(conn, tableName, indexName);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);

      // Upsert row with NULL vector value
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "row_null");
        ps.setNull(2, java.sql.Types.ARRAY);
        ps.setString(3, "null_label");
        ps.executeUpdate();
      }
      conn.commit();

      // Assert NULL vectors are excluded from physical index
      List<byte[]> rowKeys = getHBaseRowKeys(pconn, indexTable);
      assertEquals("Null vector must not produce an index row", 0, rowKeys.size());

      // Confirm presence in base table
      try (Statement stmt = conn.createStatement(); ResultSet rs =
        stmt.executeQuery("SELECT ID, LABEL FROM " + tableName + " WHERE ID = 'row_null'")) {
        assertTrue("Base table must contain the null vector row", rs.next());
        assertEquals("row_null", rs.getString(1));
        assertEquals("null_label", rs.getString(2));
      }
    }
  }

  @Test
  public void testVectorUnchangedUpdateMaintainsIndexRowAndCoveredColumns() throws Exception {
    assumeFalse(isSingleCell());
    String tableName = "T_VEC_ZUPD_" + generateUniqueName();
    String indexName = "IDX_VEC_ZUPD_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupTableAndKnownCentroids(conn, tableName, indexName);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);

      // Initial row insertion
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "row_alloc_u");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        ps.setString(3, "lbl_initial");
        ps.executeUpdate();
      }
      conn.commit();

      // Verify initial index row
      List<byte[]> rowKeysBefore = getHBaseRowKeys(pconn, indexTable);
      assertEquals(1, rowKeysBefore.size());
      assertEquals(2, extractCentroidId(rowKeysBefore.get(0)));

      // Partial update on covered non-vector column
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
          "UPSERT INTO " + tableName + " (ID, LABEL) VALUES ('row_alloc_u', 'lbl_updated')");
      }
      conn.commit();

      // Assert key preservation without partition deletion
      List<byte[]> rowKeysAfterPartial = getHBaseRowKeys(pconn, indexTable);
      assertEquals("Expected exactly 1 index row after partial update", 1,
        rowKeysAfterPartial.size());
      assertEquals(2, extractCentroidId(rowKeysAfterPartial.get(0)));

      // Assert preservation of unchanged vector column data during partial updates
      PColumn indexVecCol = indexTable.getColumnForColumnName("0:V");
      try (
        Table hTable = pconn.getQueryServices().getTable(indexTable.getPhysicalName().getBytes())) {
        Result r = hTable.get(new Get(rowKeysAfterPartial.get(0)));
        byte[] storedVec =
          r.getValue(indexVecCol.getFamilyName().getBytes(), indexVecCol.getColumnQualifierBytes());
        assertNotNull("index row must still carry the vector after a covered-only update",
          storedVec);
        assertArrayEquals(PVectorFloat.INSTANCE.toBytes(new float[] { 1.0f, 0.0f, 0.0f, 0.0f }),
          storedVec);
      }

      // Full update retaining existing vector value
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "row_alloc_u");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        ps.setString(3, "lbl_updated_again");
        ps.executeUpdate();
      }
      conn.commit();

      // Assert index key retention under existing centroid partition
      List<byte[]> rowKeys = getHBaseRowKeys(pconn, indexTable);
      assertEquals("Expected exactly 1 index row after full unchanged update", 1, rowKeys.size());
      assertEquals(2, extractCentroidId(rowKeys.get(0)));

      // Verify in-place covered column modification
      String selectSql = "SELECT \"_CENTROID_ID\", \":ID\", \"0:LABEL\" FROM " + indexName;
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(selectSql)) {
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("row_alloc_u", rs.getString(2));
        assertEquals("lbl_updated_again", rs.getString(3));
        assertFalse(rs.next());
      }
    }
  }

  /**
   * Verifies that single cell vector index rows preserve indexed vectors when updating covered
   * columns on a multi-cell base table.
   */
  @Test
  public void testUnchangedVectorCoveredUpdateOnSingleCellIndex() throws Exception {
    assumeFalse(isSingleCell());
    String tableName = "T_VEC_SC_" + generateUniqueName();
    String indexName = "IDX_VEC_SC_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), LABEL VARCHAR)");
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
          + "INCLUDE (LABEL) WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100,"
          + " IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2)");
      }
      VectorIndexTestUtil.activateWithKnownCentroids(conn, tableName, indexName,
        VectorIndexTestUtil.KNOWN_CENTROIDS);
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);
      assertEquals(ImmutableStorageScheme.ONE_CELL_PER_COLUMN,
        pconn.getTableNoCache(tableName).getImmutableStorageScheme());
      assertEquals(ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS,
        indexTable.getImmutableStorageScheme());

      replaceRow(conn, tableName, "row_sc_1", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f },
        "lbl_initial");
      try (Statement stmt = conn.createStatement()) {
        stmt
          .execute("UPSERT INTO " + tableName + " (ID, LABEL) VALUES ('row_sc_1', 'lbl_updated')");
      }
      conn.commit();

      List<byte[]> rowKeys = getHBaseRowKeys(pconn, indexTable);
      assertEquals(1, rowKeys.size());
      assertEquals(2, extractCentroidId(rowKeys.get(0)));
      assertArrayEquals(PVectorFloat.INSTANCE.toBytes(new float[] { 1.0f, 0.0f, 0.0f, 0.0f }),
        getIndexedVector(pconn, indexTable, rowKeys.get(0)));
      String querySql =
        "SELECT ID, LABEL FROM " + tableName + " ORDER BY L2_DISTANCE(V, ?) LIMIT 1";
      try (PreparedStatement ps = conn.prepareStatement(querySql)) {
        ps.setArray(1, conn.createArrayOf("FLOAT", new Float[] { 0.9f, 0.0f, 0.0f, 0.0f }));
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals("row_sc_1", rs.getString(1));
          assertEquals("lbl_updated", rs.getString(2));
          assertFalse(rs.next());
        }
      }
    }
  }

  @Test
  public void testVerifiedMarkerPresentAfterCommit() throws Exception {
    String tableName = "T_VEC_VER_" + generateUniqueName();
    String indexName = "IDX_VEC_VER_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setup(conn, tableName, indexName);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);
      byte[] physicalIndexName = indexTable.getPhysicalName().getBytes();

      // Upsert row assigned to centroid 2
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

      // Direct HBase table scan bypassing GlobalIndexChecker coprocessor
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

      // Verify row status via IndexTestUtil
      IndexTestUtil.assertRowsForEmptyColValue(conn, indexName, QueryConstants.VERIFIED_BYTES);
    }
  }

  /**
   * Validates read repair on unverified index rows, ensuring stale rows from failed writes are
   * ignored and rebuilt under current centroid partitions.
   */
  @Test
  public void testReadRepairWithCentroidReassignment() throws Exception {
    assumeFalse(isSingleCell());
    String tableName = "T_VEC_RR_" + generateUniqueName();
    String indexName = "IDX_VEC_RR_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupTableAndKnownCentroids(conn, tableName, indexName);
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);

      // Insert base table row while index is disabled to simulate index desynchronization
      IndexUtil.updateIndexState(pconn, indexName, PIndexState.DISABLE, 0L);
      pconn.removeTable(pconn.getTenantId(), tableName, null, HConstants.LATEST_TIMESTAMP);
      try (PreparedStatement ps =
        conn.prepareStatement("UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)")) {
        ps.setString(1, "repair_row_1");
        // Target centroid 2
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        ps.setString(3, "correct_label");
        ps.executeUpdate();
      }
      conn.commit();
      IndexUtil.updateIndexState(pconn, indexName, PIndexState.BUILDING, 0L);
      IndexUtil.updateIndexState(pconn, indexName, PIndexState.ACTIVE, 0L);
      pconn.removeTable(pconn.getTenantId(), indexName, null, HConstants.LATEST_TIMESTAMP);
      pconn.removeTable(pconn.getTenantId(), tableName, null, HConstants.LATEST_TIMESTAMP);

      PTable indexTable = pconn.getTableNoCache(indexName);
      byte[] physicalIndexName = indexTable.getPhysicalName().getBytes();
      byte[] centroid0RowKey =
        ByteUtil.concat(PInteger.INSTANCE.toBytes(0), Bytes.toBytes("repair_row_1"));
      byte[] centroid2RowKey =
        ByteUtil.concat(PInteger.INSTANCE.toBytes(2), Bytes.toBytes("repair_row_1"));
      byte[] emptyCF = SchemaUtil.getEmptyColumnFamily(indexTable);
      byte[] emptyCQ = EncodedColumnsUtil.getEmptyKeyValueInfo(indexTable).getFirst();
      PColumn labelCol = indexTable.getColumnForColumnName("0:LABEL");
      byte[] labelCF = labelCol.getFamilyName().getBytes();
      byte[] labelCQ = labelCol.getColumnQualifierBytes();

      // Inject synthetic unverified row under centroid 0 simulating partial write failure
      try (Table hIndexTable = pconn.getQueryServices().getTable(physicalIndexName)) {
        assertTrue(getHBaseRowKeys(pconn, indexTable).isEmpty());
        Put stalePut = new Put(centroid0RowKey);
        stalePut.addColumn(emptyCF, emptyCQ, QueryConstants.UNVERIFIED_BYTES);
        stalePut.addColumn(labelCF, labelCQ, Bytes.toBytes("stale_label"));
        hIndexTable.put(stalePut);
      }

      // Trigger read repair during index scan
      String selectSql = "SELECT \"_CENTROID_ID\", \":ID\", \"0:LABEL\" FROM " + indexName;
      try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(selectSql)) {
        assertTrue("Query through index should return the repaired row", rs.next());
        assertEquals("Repaired centroid ID must be 2", 2, rs.getInt(1));
        assertEquals("repair_row_1", rs.getString(2));
        assertEquals("correct_label", rs.getString(3));
        assertFalse("Only one row should be returned", rs.next());
      }

      try (Table hIndexTable = pconn.getQueryServices().getTable(physicalIndexName)) {
        Result r2 = hIndexTable.get(new Get(centroid2RowKey));
        assertFalse("Repaired centroid 2 row must exist after read repair", r2.isEmpty());
        assertArrayEquals(QueryConstants.VERIFIED_BYTES, r2.getValue(emptyCF, emptyCQ));
        assertEquals("correct_label", Bytes.toString(r2.getValue(labelCF, labelCQ)));
        // Stale unverified entries remain unserved until TTL expiration
        Result r0 = hIndexTable.get(new Get(centroid0RowKey));
        assertTrue(r0.isEmpty()
          || Bytes.equals(QueryConstants.UNVERIFIED_BYTES, r0.getValue(emptyCF, emptyCQ)));
      }
    }
  }

  /** Preserves covered vector columns of differing element widths in index rows. */
  @Test
  public void testCoveredDoubleVectorColumn() throws Exception {
    assumeFalse(isSingleCell());
    String tableName = "T_VEC_COVD_" + generateUniqueName();
    String indexName = "IDX_VEC_COVD_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName + " (ID VARCHAR NOT NULL PRIMARY KEY, "
          + "V VECTOR(FLOAT, 4), LABEL VARCHAR, COV_D VECTOR(DOUBLE, 3))");
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
          + "INCLUDE (LABEL, COV_D) WITH (algorithm = 'IVF', metric = 'L2', lists = 4, "
          + "sample_size = 100)");
      }
      recordKnownCentroids(conn, indexName, KNOWN_CENTROIDS);
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      IndexUtil.updateIndexState(pconn, indexName, PIndexState.ACTIVE, 0L);
      pconn.removeTable(pconn.getTenantId(), indexName, null, HConstants.LATEST_TIMESTAMP);
      pconn.removeTable(pconn.getTenantId(), tableName, null, HConstants.LATEST_TIMESTAMP);

      String upsert = "UPSERT INTO " + tableName + " (ID, V, COV_D) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsert)) {
        ps.setString(1, "r1");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        ps.setArray(3, conn.createArrayOf("DOUBLE", new Double[] { 1.5, -2.25, 3.125 }));
        ps.executeUpdate();
      }
      conn.commit();
      // Update covered vector column
      try (PreparedStatement ps =
        conn.prepareStatement("UPSERT INTO " + tableName + " (ID, COV_D) VALUES (?, ?)")) {
        ps.setString(1, "r1");
        ps.setArray(2, conn.createArrayOf("DOUBLE", new Double[] { 4.5, 5.5, -6.5 }));
        ps.executeUpdate();
      }
      conn.commit();

      PTable indexTable = pconn.getTableNoCache(indexName);
      List<byte[]> rowKeys = getHBaseRowKeys(pconn, indexTable);
      assertEquals(1, rowKeys.size());
      assertEquals(2, extractCentroidId(rowKeys.get(0)));
      PColumn covCol = indexTable.getColumnForColumnName("0:COV_D");
      PColumn vecCol = indexTable.getColumnForColumnName("0:V");
      try (
        Table hTable = pconn.getQueryServices().getTable(indexTable.getPhysicalName().getBytes())) {
        Result r = hTable.get(new Get(rowKeys.get(0)));
        assertArrayEquals(PVectorDouble.INSTANCE.toBytes(new double[] { 4.5, 5.5, -6.5 }),
          r.getValue(covCol.getFamilyName().getBytes(), covCol.getColumnQualifierBytes()));
        assertArrayEquals(PVectorFloat.INSTANCE.toBytes(new float[] { 1.0f, 0.0f, 0.0f, 0.0f }),
          r.getValue(vecCol.getFamilyName().getBytes(), vecCol.getColumnQualifierBytes()));
      }
    }
  }

  /** Validates double precision vector index maintenance across mutations and boundary updates. */
  @Test
  public void testDoubleVectorIndexMaintenance() throws Exception {
    assumeFalse(isSingleCell());
    String tableName = "T_VEC_DBL_" + generateUniqueName();
    String indexName = "IDX_VEC_DBL_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupTableAndKnownCentroids(conn, tableName, indexName, "DOUBLE", "");
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);
      String upsert = "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsert)) {
        ps.setString(1, "d1");
        ps.setArray(2, conn.createArrayOf("DOUBLE", new Double[] { 0.9, 0.1, 0.0, 0.0 }));
        ps.setString(3, "a");
        ps.executeUpdate();
      }
      conn.commit();
      List<byte[]> rowKeys = getHBaseRowKeys(pconn, indexTable);
      assertEquals(1, rowKeys.size());
      assertEquals(2, extractCentroidId(rowKeys.get(0)));

      try (PreparedStatement ps = conn.prepareStatement(upsert)) {
        ps.setString(1, "d1");
        ps.setArray(2, conn.createArrayOf("DOUBLE", new Double[] { 0.0, 0.1, 0.0, 0.9 }));
        ps.setString(3, "b");
        ps.executeUpdate();
      }
      conn.commit();
      rowKeys = getHBaseRowKeys(pconn, indexTable);
      assertEquals(1, rowKeys.size());
      assertEquals(0, extractCentroidId(rowKeys.get(0)));

      conn.createStatement().execute("DELETE FROM " + tableName + " WHERE ID = 'd1'");
      conn.commit();
      assertEquals(0, getHBaseRowKeys(pconn, indexTable).size());
    }
  }

  /** Validates client side immutable index generation parity against server side builds. */
  @Test
  public void testImmutableTableClientMaintenanceMatchesServerBuild() throws Exception {
    assumeFalse(isSingleCell());
    String tableName = "T_VEC_IMM_" + generateUniqueName();
    String indexName = "IDX_VEC_IMM_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupTableAndKnownCentroids(conn, tableName, indexName, "FLOAT", "IMMUTABLE_ROWS = true");
      float[][] vectors = loadRandomVectors(conn, tableName, null, null, 50, 3);
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable pIndex = pconn.getTableNoCache(indexName);
      List<byte[]> rowKeys = getHBaseRowKeys(pconn, pIndex);
      assertEquals(50, rowKeys.size());
      for (byte[] rowKey : rowKeys) {
        String id = Bytes.toString(rowKey, Bytes.SIZEOF_INT, rowKey.length - Bytes.SIZEOF_INT);
        int rowIdx = Integer.parseInt(id.replace("row_", ""));
        assertEquals(findNearestCentroid(vectors[rowIdx], KNOWN_CENTROIDS),
          extractCentroidId(rowKey));
      }
      assertIndexVerifies(tableName, indexName, 50);
    }
  }
}
