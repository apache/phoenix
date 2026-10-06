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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
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
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVectorDouble;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Integration tests for vector index maintenance on the write path. The tests cover the centroid
 * prefix of the row key, a change of centroid after an update, NULL vectors, covered columns, the
 * verified marker, read repair, and client-side maintenance for immutable tables. The class runs
 * for each immutable storage scheme.
 */
@Category(ParallelStatsDisabledTest.class)
public class VectorIndexWriteIT extends ParallelStatsDisabledIT {

  @Test
  public void testVectorInsertGeneratesIndexRow() throws Exception {
    String tableName = "T_VEC_INS_" + generateUniqueName();
    String indexName = "IDX_VEC_INS_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupTableAndKnownCentroids(conn, tableName, indexName);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);

      // Upsert a vector whose nearest centroid is 2
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "row_1");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        ps.setString(3, "lbl_1");
        ps.executeUpdate();
      }
      conn.commit();

      // Check the row key of the physical index row and its centroid ID prefix
      List<byte[]> rowKeys = getHBaseRowKeys(pconn, indexTable);
      assertEquals("Expected exactly 1 index row", 1, rowKeys.size());
      assertEquals("Centroid prefix must be 2", 2, extractCentroidId(rowKeys.get(0)));

      // Query the index table with SQL
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
      setupTableAndKnownCentroids(conn, tableName, indexName);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);

      // The nearest centroid of the first insert is 2
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

      // Update vector across centroid boundary to centroid 0
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "row_1");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 0.0f, 0.0f, 0.0f, 1.0f }));
        ps.setString(3, "lbl_updated");
        ps.executeUpdate();
      }
      conn.commit();

      // The update deletes the old index row and writes a new row under centroid 0
      List<byte[]> rowKeysAfter = getHBaseRowKeys(pconn, indexTable);
      assertEquals("Expected exactly 1 index row after vector update", 1, rowKeysAfter.size());
      assertEquals("Centroid prefix must be updated to 0", 0,
        extractCentroidId(rowKeysAfter.get(0)));

      // Query the index table with SQL
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
      setupTableAndKnownCentroids(conn, tableName, indexName);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);

      // The nearest centroid of the first insert is 2
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

      // Update non-vector covered column in place
      try (PreparedStatement ps =
        conn.prepareStatement("UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)")) {
        ps.setString(1, "row_1");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        ps.setString(3, "updated_label");
        ps.executeUpdate();
      }
      conn.commit();

      // The index row key and its centroid do not change
      List<byte[]> rowKeysAfter = getHBaseRowKeys(pconn, indexTable);
      assertEquals("Expected exactly 1 index row", 1, rowKeysAfter.size());
      assertEquals("Centroid prefix must still be 2", 2, extractCentroidId(rowKeysAfter.get(0)));

      // Check that the index row has the new covered column value
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
      setupTableAndKnownCentroids(conn, tableName, indexName);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);

      // Upsert a row whose nearest centroid is 2
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

      // Delete the data table row
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("DELETE FROM " + tableName + " WHERE ID = 'row_1'");
      }
      conn.commit();

      // The delete removes the physical index row
      List<byte[]> rowKeysAfter = getHBaseRowKeys(pconn, indexTable);
      assertEquals("Expected 0 index rows after delete", 0, rowKeysAfter.size());

      // Query the index table with SQL
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

      // Upsert a row with a NULL vector
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "row_null");
        ps.setNull(2, java.sql.Types.ARRAY);
        ps.setString(3, "null_label");
        ps.executeUpdate();
      }
      conn.commit();

      // A NULL vector gives no physical index row
      List<byte[]> rowKeys = getHBaseRowKeys(pconn, indexTable);
      assertEquals("Null vector must not produce an index row", 0, rowKeys.size());

      // The data table has the row
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
    String tableName = "T_VEC_ZUPD_" + generateUniqueName();
    String indexName = "IDX_VEC_ZUPD_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupTableAndKnownCentroids(conn, tableName, indexName);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);

      // Insert the first version of the row
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "row_alloc_u");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        ps.setString(3, "lbl_initial");
        ps.executeUpdate();
      }
      conn.commit();

      // Check the first index row
      List<byte[]> rowKeysBefore = getHBaseRowKeys(pconn, indexTable);
      assertEquals(1, rowKeysBefore.size());
      assertEquals(2, extractCentroidId(rowKeysBefore.get(0)));

      // Partially update only the covered LABEL column
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
          "UPSERT INTO " + tableName + " (ID, LABEL) VALUES ('row_alloc_u', 'lbl_updated')");
      }
      conn.commit();

      // The index row keeps its key under centroid 2
      List<byte[]> rowKeysAfterPartial = getHBaseRowKeys(pconn, indexTable);
      assertEquals("Expected exactly 1 index row after partial update", 1,
        rowKeysAfterPartial.size());
      assertEquals(2, extractCentroidId(rowKeysAfterPartial.get(0)));

      // The partial update keeps the vector value in the index row
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

      // Fully update the row with the same vector value
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, "row_alloc_u");
        ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { 1.0f, 0.0f, 0.0f, 0.0f }));
        ps.setString(3, "lbl_updated_again");
        ps.executeUpdate();
      }
      conn.commit();

      // The index row keeps its key under centroid 2
      List<byte[]> rowKeys = getHBaseRowKeys(pconn, indexTable);
      assertEquals("Expected exactly 1 index row after full unchanged update", 1, rowKeys.size());
      assertEquals(2, extractCentroidId(rowKeys.get(0)));

      // Check that the covered column changed in the same index row
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

  @Test
  public void testVerifiedMarkerPresentAfterCommit() throws Exception {
    String tableName = "T_VEC_VER_" + generateUniqueName();
    String indexName = "IDX_VEC_VER_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupTableAndKnownCentroids(conn, tableName, indexName);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);
      byte[] physicalIndexName = indexTable.getPhysicalName().getBytes();

      // Upsert a row whose nearest centroid is 2
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

      // Scan the HBase table directly, so GlobalIndexChecker does not filter or repair rows
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

      // Check the verified marker of all index rows with IndexTestUtil
      IndexTestUtil.assertRowsForEmptyColValue(conn, indexName, QueryConstants.VERIFIED_BYTES);
    }
  }

  /**
   * Tests read repair of an unverified index row under the wrong centroid. The query must skip the
   * stale row and return the row that repair writes under the correct centroid.
   */
  @Test
  public void testReadRepairWithCentroidReassignment() throws Exception {
    String tableName = "T_VEC_RR_" + generateUniqueName();
    String indexName = "IDX_VEC_RR_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupTableAndKnownCentroids(conn, tableName, indexName);
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);

      // Write the data row while the index is disabled, so the index has no row for it
      IndexUtil.updateIndexState(pconn, indexName, PIndexState.DISABLE, 0L);
      pconn.removeTable(pconn.getTenantId(), tableName, null, HConstants.LATEST_TIMESTAMP);
      try (PreparedStatement ps =
        conn.prepareStatement("UPSERT INTO " + tableName + " (ID, V, LABEL) VALUES (?, ?, ?)")) {
        ps.setString(1, "repair_row_1");
        // The nearest centroid is 2
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

      // Write an unverified row under centroid 0, as a failed index write can leave it
      try (Table hIndexTable = pconn.getQueryServices().getTable(physicalIndexName)) {
        assertTrue(getHBaseRowKeys(pconn, indexTable).isEmpty());
        Put stalePut = new Put(centroid0RowKey);
        stalePut.addColumn(emptyCF, emptyCQ, QueryConstants.UNVERIFIED_BYTES);
        stalePut.addColumn(labelCF, labelCQ, Bytes.toBytes("stale_label"));
        hIndexTable.put(stalePut);
      }

      // A query on the index starts read repair
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
        // Repair can delete the stale row or leave it unverified; a query does not return it
        Result r0 = hIndexTable.get(new Get(centroid0RowKey));
        assertTrue(r0.isEmpty()
          || Bytes.equals(QueryConstants.UNVERIFIED_BYTES, r0.getValue(emptyCF, emptyCQ)));
      }
    }
  }

  /**
   * Tests that an index row keeps a covered DOUBLE vector next to the indexed FLOAT vector. The
   * test also partially updates the covered vector.
   */
  @Test
  public void testCoveredDoubleVectorColumn() throws Exception {
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
      // Update only the covered vector column
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

  /** Tests index maintenance of a DOUBLE vector across insert, centroid change, and delete. */
  @Test
  public void testDoubleVectorIndexMaintenance() throws Exception {
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

  /**
   * Tests that client-side maintenance of an index on an immutable table writes the same index rows
   * as a server-side build.
   */
  @Test
  public void testImmutableTableClientMaintenanceMatchesServerBuild() throws Exception {
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
