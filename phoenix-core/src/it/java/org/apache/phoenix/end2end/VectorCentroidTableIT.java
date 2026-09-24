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
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CLUSTER_SIZE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.GENERATION_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.KEY_SEQ;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LAST_REBUILD_TIME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LAST_SCORECARD_UPDATE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.REASSIGN_COUNT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.REBUILD_STATE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.REBUILD_STATE_ACTIVE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.REBUILD_STATE_BUILDING;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.REBUILD_STATE_RETIRED;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SKEW_METRICS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_VECTOR_CENTROID_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_VECTOR_CENTROID_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TRIGGER_REASON;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TRIGGER_REASON_CREATE_INDEX;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.phoenix.index.vector.CentroidManager;
import org.apache.phoenix.index.vector.ClusterSkewMetrics;
import org.apache.phoenix.index.vector.GenerationSummary;
import org.apache.phoenix.index.vector.KMeansConfig;
import org.apache.phoenix.index.vector.KMeansResult;
import org.apache.phoenix.index.vector.KMeansTrainer;
import org.apache.phoenix.index.vector.ScorecardRow;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class VectorCentroidTableIT extends ParallelStatsDisabledIT {

  private final List<String> createdIndexNames = new ArrayList<>();

  private String uniqueIndex(String prefix) {
    String name = prefix + generateUniqueName();
    createdIndexNames.add(name);
    return name;
  }

  @After
  public void cleanUpVectorState() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      VectorIndexTestUtil.deleteCentroidRows(conn, createdIndexNames);
    } finally {
      createdIndexNames.clear();
      VectorIndexTestUtil.resetSharedVectorState();
    }
  }

  @Test
  public void testTableExistenceAndSchema() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement(); ResultSet rs =
        stmt.executeQuery("SELECT * FROM " + SYSTEM_VECTOR_CENTROID_NAME + " WHERE 1=0")) {
        ResultSetMetaData rsmd = rs.getMetaData();
        assertEquals(11, rsmd.getColumnCount());

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

        assertEquals(CLUSTER_SIZE, rsmd.getColumnName(5));
        assertEquals(Types.BIGINT, rsmd.getColumnType(5));
        assertEquals(ResultSetMetaData.columnNullable, rsmd.isNullable(5));

        assertEquals(REASSIGN_COUNT, rsmd.getColumnName(6));
        assertEquals(Types.BIGINT, rsmd.getColumnType(6));
        assertEquals(ResultSetMetaData.columnNullable, rsmd.isNullable(6));

        assertEquals(SKEW_METRICS, rsmd.getColumnName(7));
        assertEquals(Types.VARBINARY, rsmd.getColumnType(7));
        assertEquals(ResultSetMetaData.columnNullable, rsmd.isNullable(7));

        assertEquals(REBUILD_STATE, rsmd.getColumnName(8));
        assertEquals(Types.CHAR, rsmd.getColumnType(8));
        assertEquals(ResultSetMetaData.columnNullable, rsmd.isNullable(8));

        assertEquals(TRIGGER_REASON, rsmd.getColumnName(9));
        assertEquals(Types.VARCHAR, rsmd.getColumnType(9));
        assertEquals(ResultSetMetaData.columnNullable, rsmd.isNullable(9));

        assertEquals(LAST_REBUILD_TIME, rsmd.getColumnName(10));
        assertEquals(Types.BIGINT, rsmd.getColumnType(10));
        assertEquals(ResultSetMetaData.columnNullable, rsmd.isNullable(10));

        assertEquals(LAST_SCORECARD_UPDATE, rsmd.getColumnName(11));
        assertEquals(Types.BIGINT, rsmd.getColumnType(11));
        assertEquals(ResultSetMetaData.columnNullable, rsmd.isNullable(11));
      }
    }
  }

  @Test
  public void testPrimaryKeyShape() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      DatabaseMetaData dbmd = conn.getMetaData();
      java.util.Map<String, Short> pkColToSeq = new java.util.HashMap<>();
      try (ResultSet rs =
        dbmd.getPrimaryKeys(null, SYSTEM_CATALOG_SCHEMA, SYSTEM_VECTOR_CENTROID_TABLE)) {
        while (rs.next()) {
          pkColToSeq.put(rs.getString(COLUMN_NAME), rs.getShort(KEY_SEQ));
        }
      }
      assertEquals(3, pkColToSeq.size());
      assertEquals(Short.valueOf((short) 1), pkColToSeq.get(INDEX_NAME));
      assertEquals(Short.valueOf((short) 2), pkColToSeq.get(GENERATION_ID));
      assertEquals(Short.valueOf((short) 3), pkColToSeq.get(CENTROID_ID));
    }
  }

  @Test
  public void testPrimaryKeyConstraintAndUpsert() throws Exception {
    String indexName = uniqueIndex("TEST_VECTOR_IDX_");
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

      // Upsert second row with the SAME (INDEX_NAME, GENERATION_ID, CENTROID_ID) PK
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, indexName);
        ps.setLong(2, 1L);
        ps.setInt(3, 0);
        ps.setBytes(4, vector2);
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

      // Assert updated values
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

      // Upsert a second generation for the same index and centroid_id (generation=2)
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        ps.setString(1, indexName);
        ps.setLong(2, 2L);
        ps.setInt(3, 0);
        ps.setBytes(4, vector3);
        ps.executeUpdate();
      }
      conn.commit();

      // Assert count is now 2 for this index (generations coexist)
      try (PreparedStatement ps = conn.prepareStatement(countSql)) {
        ps.setString(1, indexName);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals("Expected two rows for distinct generations", 2, rs.getInt(1));
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

      // Validate non-primary key column schema
      PColumn centroidVectorCol = table.getColumnForColumnName(CENTROID_VECTOR);
      assertNotNull(centroidVectorCol);
      assertEquals(PVarbinary.INSTANCE, centroidVectorCol.getDataType());
      assertTrue(centroidVectorCol.isNullable());

      PColumn clusterSizeCol = table.getColumnForColumnName(CLUSTER_SIZE);
      assertNotNull(clusterSizeCol);
      assertEquals(PLong.INSTANCE, clusterSizeCol.getDataType());
      assertTrue(clusterSizeCol.isNullable());

      PColumn reassignCountCol = table.getColumnForColumnName(REASSIGN_COUNT);
      assertNotNull(reassignCountCol);
      assertEquals(PLong.INSTANCE, reassignCountCol.getDataType());
      assertTrue(reassignCountCol.isNullable());

      PColumn skewMetricsCol = table.getColumnForColumnName(SKEW_METRICS);
      assertNotNull(skewMetricsCol);
      assertEquals(PVarbinary.INSTANCE, skewMetricsCol.getDataType());
      assertTrue(skewMetricsCol.isNullable());

      PColumn rebuildStateCol = table.getColumnForColumnName(REBUILD_STATE);
      assertNotNull(rebuildStateCol);
      assertEquals(PChar.INSTANCE, rebuildStateCol.getDataType());
      assertTrue(rebuildStateCol.isNullable());

      PColumn triggerReasonCol = table.getColumnForColumnName(TRIGGER_REASON);
      assertNotNull(triggerReasonCol);
      assertEquals(PVarchar.INSTANCE, triggerReasonCol.getDataType());
      assertTrue(triggerReasonCol.isNullable());

      PColumn lastRebuildTimeCol = table.getColumnForColumnName(LAST_REBUILD_TIME);
      assertNotNull(lastRebuildTimeCol);
      assertEquals(PLong.INSTANCE, lastRebuildTimeCol.getDataType());
      assertTrue(lastRebuildTimeCol.isNullable());

      PColumn lastScorecardUpdateCol = table.getColumnForColumnName(LAST_SCORECARD_UPDATE);
      assertNotNull(lastScorecardUpdateCol);
      assertEquals(PLong.INSTANCE, lastScorecardUpdateCol.getDataType());
      assertTrue(lastScorecardUpdateCol.isNullable());
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

        assertTrue("Expected GENERATION_ID column", rs.next());
        assertEquals(GENERATION_ID, rs.getString(COLUMN_NAME));
        assertEquals(Types.BIGINT, rs.getInt(DATA_TYPE));

        assertTrue("Expected CENTROID_ID column", rs.next());
        assertEquals(CENTROID_ID, rs.getString(COLUMN_NAME));
        assertEquals(Types.INTEGER, rs.getInt(DATA_TYPE));

        assertTrue("Expected CENTROID_VECTOR column", rs.next());
        assertEquals(CENTROID_VECTOR, rs.getString(COLUMN_NAME));
        assertEquals(Types.VARBINARY, rs.getInt(DATA_TYPE));

        assertTrue("Expected CLUSTER_SIZE column", rs.next());
        assertEquals(CLUSTER_SIZE, rs.getString(COLUMN_NAME));
        assertEquals(Types.BIGINT, rs.getInt(DATA_TYPE));

        assertTrue("Expected REASSIGN_COUNT column", rs.next());
        assertEquals(REASSIGN_COUNT, rs.getString(COLUMN_NAME));
        assertEquals(Types.BIGINT, rs.getInt(DATA_TYPE));

        assertTrue("Expected SKEW_METRICS column", rs.next());
        assertEquals(SKEW_METRICS, rs.getString(COLUMN_NAME));
        assertEquals(Types.VARBINARY, rs.getInt(DATA_TYPE));

        assertTrue("Expected REBUILD_STATE column", rs.next());
        assertEquals(REBUILD_STATE, rs.getString(COLUMN_NAME));
        assertEquals(Types.CHAR, rs.getInt(DATA_TYPE));

        assertTrue("Expected TRIGGER_REASON column", rs.next());
        assertEquals(TRIGGER_REASON, rs.getString(COLUMN_NAME));
        assertEquals(Types.VARCHAR, rs.getInt(DATA_TYPE));

        assertTrue("Expected LAST_REBUILD_TIME column", rs.next());
        assertEquals(LAST_REBUILD_TIME, rs.getString(COLUMN_NAME));
        assertEquals(Types.BIGINT, rs.getInt(DATA_TYPE));

        assertTrue("Expected LAST_SCORECARD_UPDATE column", rs.next());
        assertEquals(LAST_SCORECARD_UPDATE, rs.getString(COLUMN_NAME));
        assertEquals(Types.BIGINT, rs.getInt(DATA_TYPE));

        assertFalse("No additional columns expected", rs.next());
      }
    }
  }

  @Test
  public void testGenerationsCoexist() throws Exception {
    String indexName = uniqueIndex("TEST_VECTOR_IDX_");
    List<byte[]> gen1 =
      Arrays.asList(new byte[] { 1 }, new byte[] { 2 }, new byte[] { 3 }, new byte[] { 4 });
    List<byte[]> gen2 =
      Arrays.asList(new byte[] { 5 }, new byte[] { 6 }, new byte[] { 7 }, new byte[] { 8 });

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      CentroidManager manager = new CentroidManager(conn);
      manager.persistCentroids(indexName, 1L, gen1);
      manager.persistCentroids(indexName, 2L, gen2);

      List<byte[]> loaded1 = manager.loadCentroids(indexName, 1L);
      assertEquals(4, loaded1.size());
      for (int i = 0; i < 4; i++) {
        assertArrayEquals(gen1.get(i), loaded1.get(i));
      }

      List<byte[]> loaded2 = manager.loadCentroids(indexName, 2L);
      assertEquals(4, loaded2.size());
      for (int i = 0; i < 4; i++) {
        assertArrayEquals(gen2.get(i), loaded2.get(i));
      }
    }
  }

  @Test
  public void testSentinelRowExcludedFromCentroidLoads() throws Exception {
    String indexName = uniqueIndex("TEST_VECTOR_IDX_");
    List<byte[]> centroids =
      Arrays.asList(new byte[] { 10 }, new byte[] { 20 }, new byte[] { 30 }, new byte[] { 40 });

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      CentroidManager manager = new CentroidManager(conn);
      manager.persistCentroids(indexName, 1L, centroids);

      manager.persistGenerationSummary(new GenerationSummary.Builder().setIndexName(indexName)
        .setGenerationId(1L).setRebuildState("A").setTriggerReason("TEST").build());

      List<byte[]> loaded = manager.loadCentroids(indexName, 1L);
      assertEquals(4, loaded.size());
      for (int i = 0; i < 4; i++) {
        assertArrayEquals(centroids.get(i), loaded.get(i));
      }
    }
  }

  /**
   * Verifies that the sentinel row ({@code CENTROID_ID = -1}) orders ahead of centroid rows in
   * physical row key order.
   */
  @Test
  public void testSentinelRowSortsBeforeCentroidsInKeyOrder() throws Exception {
    String indexName = uniqueIndex("TEST_VECTOR_IDX_");
    List<byte[]> centroids =
      Arrays.asList(new byte[] { 1 }, new byte[] { 2 }, new byte[] { 3 }, new byte[] { 4 });

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      CentroidManager manager = new CentroidManager(conn);
      manager.persistCentroids(indexName, 1L, centroids);
      manager.persistGenerationSummary(new GenerationSummary.Builder().setIndexName(indexName)
        .setGenerationId(1L).setRebuildState("A").setTriggerReason("TEST").build());

      String selectSql = "SELECT " + CENTROID_ID + " FROM " + SYSTEM_VECTOR_CENTROID_NAME
        + " WHERE " + INDEX_NAME + " = ? AND " + GENERATION_ID + " = ?";

      try (PreparedStatement ps = conn.prepareStatement(selectSql)) {
        ps.setString(1, indexName);
        ps.setLong(2, 1L);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals("Sentinel row (-1) must precede centroid 0 in key order", -1, rs.getInt(1));
          for (int i = 0; i < 4; i++) {
            assertTrue(rs.next());
            assertEquals(i, rs.getInt(1));
          }
          assertFalse(rs.next());
        }
      }
    }
  }

  /**
   * Verifies loading centroids excludes the sentinel row and orders results by {@code CENTROID_ID}.
   */
  @Test
  public void testCentroidLoadSkipsSentinelByScanBoundary() throws Exception {
    String indexName = uniqueIndex("TEST_VECTOR_IDX_");
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String sql = "SELECT " + CENTROID_VECTOR + " FROM " + SYSTEM_VECTOR_CENTROID_NAME + " WHERE "
        + INDEX_NAME + " = ? AND " + GENERATION_ID + " = ? AND " + CENTROID_ID + " >= 0 ORDER BY "
        + CENTROID_ID + " ASC";
      String plan = explain(conn, sql, indexName, 1L);
      assertTrue(plan, plan.contains("RANGE SCAN OVER " + SYSTEM_VECTOR_CENTROID_NAME + " ['"
        + indexName + "',1,0] - ['" + indexName + "',1,*]"));
      assertFalse("sentinel exclusion must not become a per-row filter", plan.contains("FILTER"));
      assertFalse("the key order already satisfies ORDER BY CENTROID_ID",
        plan.contains("SORTED BY"));
    }
  }

  /**
   * Verifies that retiring a generation deletes all rows for that generation.
   */
  @Test
  public void testDeleteGenerationIsAPrefixRangeDelete() throws Exception {
    String indexName = uniqueIndex("TEST_VECTOR_IDX_");
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String sql = "DELETE FROM " + SYSTEM_VECTOR_CENTROID_NAME + " WHERE " + INDEX_NAME
        + " = ? AND " + GENERATION_ID + " = ?";
      String plan = explain(conn, sql, indexName, 2L);
      assertTrue(plan, plan
        .contains("RANGE SCAN OVER " + SYSTEM_VECTOR_CENTROID_NAME + " ['" + indexName + "',2]"));
    }
  }

  /**
   * Verifies listing distinct generation IDs for an index.
   */
  @Test
  public void testListGenerationsSeeksBetweenGenerations() throws Exception {
    String indexName = uniqueIndex("TEST_VECTOR_IDX_");
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String sql = "SELECT DISTINCT " + GENERATION_ID + " FROM " + SYSTEM_VECTOR_CENTROID_NAME
        + " WHERE " + INDEX_NAME + " = ? ORDER BY " + GENERATION_ID + " ASC";
      String plan = explain(conn, sql, indexName);
      assertTrue(plan, plan.contains("DISTINCT PREFIX FILTER OVER [" + GENERATION_ID + "]"));
      assertFalse("generations are already ordered by the row key", plan.contains("SORTED BY"));
    }
  }

  private static String explain(Connection conn, String sql, Object... binds) throws Exception {
    StringBuilder plan = new StringBuilder();
    try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + sql)) {
      for (int i = 0; i < binds.length; i++) {
        ps.setObject(i + 1, binds[i]);
      }
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          plan.append(rs.getString(1)).append('\n');
        }
      }
    }
    return plan.toString();
  }

  @Test
  public void testScorecardAndGenerationSummaryRoundTrip() throws Exception {
    String indexName = uniqueIndex("TEST_VECTOR_IDX_");

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      CentroidManager manager = new CentroidManager(conn);

      manager.persistCentroids(indexName, 1L, Arrays.asList(new byte[] { 1 }, new byte[] { 2 }));

      ScorecardRow sc0 = new ScorecardRow(indexName, 1L, 0, 50L, 2L, 1000L);
      ScorecardRow sc1 = new ScorecardRow(indexName, 1L, 1, 60L, 1L, 1000L);
      manager.persistScorecardRow(sc0);
      manager.persistScorecardRow(sc1);

      List<ScorecardRow> scorecard = manager.loadScorecard(indexName, 1L);
      assertEquals(2, scorecard.size());
      assertEquals(sc0, scorecard.get(0));
      assertEquals(sc1, scorecard.get(1));

      int[] sizes = new int[] { 50, 60 };
      ClusterSkewMetrics skew = ClusterSkewMetrics.compute(sizes);
      GenerationSummary summary =
        new GenerationSummary.Builder().setIndexName(indexName).setGenerationId(1L)
          .setSkewMetrics(skew).setRebuildState("A").setTriggerReason("DRIFT_CHECK")
          .setLastRebuildTime(2000L).setLastScorecardUpdate(3000L).build();
      manager.persistGenerationSummary(summary);

      GenerationSummary loadedSummary = manager.loadGenerationSummary(indexName, 1L);
      assertNotNull(loadedSummary);
      assertEquals(summary, loadedSummary);

      manager.persistCentroids(indexName, 2L, Arrays.asList(new byte[] { 3 }));
      List<Long> gens = manager.listGenerations(indexName);
      assertEquals(Arrays.asList(1L, 2L), gens);
    }
  }

  /**
   * Verifies partial updates to sentinel and scorecard rows preserve unmentioned columns.
   */
  @Test
  public void testPartialWritesPreserveUnwrittenColumns() throws Exception {
    String indexName = uniqueIndex("TEST_VECTOR_IDX_");
    byte[] centroidVector = new byte[] { 7, 7, 7, 7 };

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      CentroidManager manager = new CentroidManager(conn);
      manager.persistCentroids(indexName, 1L, Arrays.asList(centroidVector));

      // Write reconciliation metrics
      manager.persistScorecardRow(new ScorecardRow(indexName, 1L, 0, 500L, 0L, 1000L));
      // Write counter updates
      manager.persistScorecardRow(indexName, 1L, 0, 512L, 6L, null);

      List<ScorecardRow> scorecard = manager.loadScorecard(indexName, 1L);
      assertEquals(1, scorecard.size());
      assertEquals(Long.valueOf(512L), scorecard.get(0).getClusterSize());
      assertEquals(Long.valueOf(6L), scorecard.get(0).getReassignCount());
      assertEquals("reconciliation timestamp must survive a counter-only flush",
        Long.valueOf(1000L), scorecard.get(0).getLastScorecardUpdate());
      assertArrayEquals("the centroid vector shares the row and must be untouched", centroidVector,
        manager.loadCentroids(indexName, 1L).get(0));

      ClusterSkewMetrics skew = ClusterSkewMetrics.compute(new int[] { 500, 12 });
      manager.persistGenerationSummary(indexName, 1L, skew, REBUILD_STATE_BUILDING, "DRIFT_SKEW",
        null, null);
      // Activate generation lifecycle columns
      manager.persistGenerationSummary(indexName, 1L, null, REBUILD_STATE_ACTIVE, null, 4000L,
        null);

      GenerationSummary summary = manager.loadGenerationSummary(indexName, 1L);
      assertNotNull(summary);
      assertEquals(REBUILD_STATE_ACTIVE, summary.getRebuildState());
      assertEquals(Long.valueOf(4000L), summary.getLastRebuildTime());
      assertEquals("the trigger reason must survive activation", "DRIFT_SKEW",
        summary.getTriggerReason());
      assertEquals("training metrics must survive activation", skew, summary.getSkewMetrics());
    }
  }

  /**
   * Verifies that persisting centroids updates skew metrics and rebuild state without setting
   * {@code LAST_REBUILD_TIME}.
   */
  @Test
  public void testPersistCentroidsRecordsTrainingSummary() throws Exception {
    String indexName = uniqueIndex("TEST_VECTOR_IDX_");

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      CentroidManager manager = new CentroidManager(conn);
      List<float[]> vectors = Arrays.asList(new float[] { 0f, 0f }, new float[] { 0.1f, 0.1f },
        new float[] { 9f, 9f }, new float[] { 9.1f, 9.1f });
      KMeansResult result =
        KMeansTrainer.train(vectors, 2, KMeansConfig.builder().maxIterations(10).build());
      assertNotNull("training must produce skew metrics to record", result.getSkewMetrics());

      CentroidManager.persistCentroids(conn, indexName, 1L, result, TRIGGER_REASON_CREATE_INDEX);

      GenerationSummary summary = manager.loadGenerationSummary(indexName, 1L);
      assertNotNull(summary);
      assertEquals(result.getSkewMetrics(), summary.getSkewMetrics());
      assertEquals(REBUILD_STATE_ACTIVE, summary.getRebuildState());
      assertEquals(TRIGGER_REASON_CREATE_INDEX, summary.getTriggerReason());
      assertNull("index creation is not a completed rebuild", summary.getLastRebuildTime());
      assertEquals(2, manager.loadCentroids(indexName, 1L).size());
    }
  }

  /**
   * Verifies that invalid or corrupted {@code SKEW_METRICS} payloads surface as decode failures
   * rather than returning null.
   */
  @Test
  public void testUndecodableSkewMetricsAreDistinguishableFromAbsent() throws Exception {
    String undecodableIndex = uniqueIndex("TEST_VECTOR_IDX_BADSKEW_");
    String absentIndex = uniqueIndex("TEST_VECTOR_IDX_NOSKEW_");

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      CentroidManager manager = new CentroidManager(conn);

      byte[] valid = ClusterSkewMetrics.compute(new int[] { 10, 90 }).toBytes();
      byte[] truncated = Arrays.copyOf(valid, valid.length / 2);
      try (PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + SYSTEM_VECTOR_CENTROID_NAME
        + " (" + INDEX_NAME + ", " + GENERATION_ID + ", " + CENTROID_ID + ", " + SKEW_METRICS + ", "
        + REBUILD_STATE + ") VALUES (?, 1, -1, ?, ?)")) {
        ps.setString(1, undecodableIndex);
        ps.setBytes(2, truncated);
        ps.setString(3, REBUILD_STATE_ACTIVE);
        ps.executeUpdate();
      }
      conn.commit();

      // The other generation gets a sentinel row with every column but SKEW_METRICS.
      manager.persistGenerationSummary(absentIndex, 1L, null, REBUILD_STATE_ACTIVE, "CREATE_INDEX",
        null, 1234L);

      GenerationSummary undecodable = manager.loadGenerationSummary(undecodableIndex, 1L);
      assertNotNull(undecodable);
      assertNull(undecodable.getSkewMetrics());
      assertNotNull("a truncated blob must surface as a decode failure",
        undecodable.getSkewMetricsDecodeError());
      assertEquals(REBUILD_STATE_ACTIVE, undecodable.getRebuildState());

      GenerationSummary absent = manager.loadGenerationSummary(absentIndex, 1L);
      assertNotNull(absent);
      assertNull(absent.getSkewMetrics());
      assertNull("no metrics recorded is not a decode failure", absent.getSkewMetricsDecodeError());
    }
  }

  @Test
  public void testPersistScorecardBatch() throws Exception {
    String indexName = uniqueIndex("TEST_VECTOR_IDX_");

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      CentroidManager manager = new CentroidManager(conn);
      List<ScorecardRow> rows = new ArrayList<>();
      for (int i = 0; i < 16; i++) {
        rows.add(new ScorecardRow(indexName, 1L, i, 10L * i, (long) i, 7000L));
      }
      manager.persistScorecard(rows);
      // Ensure sentinel row is excluded from centroids
      manager.persistGenerationSummary(new GenerationSummary.Builder().setIndexName(indexName)
        .setGenerationId(1L).setRebuildState(REBUILD_STATE_ACTIVE).build());

      List<ScorecardRow> loaded = manager.loadScorecard(indexName, 1L);
      assertEquals(16, loaded.size());
      for (int i = 0; i < 16; i++) {
        assertEquals("rows must come back in CENTROID_ID order", rows.get(i), loaded.get(i));
      }
    }
  }

  @Test
  public void testPersistAndLoadRoundTrip() throws Exception {
    String indexName = uniqueIndex("TEST_VECTOR_IDX_");
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
    String indexName = uniqueIndex("TEST_VECTOR_IDX_");
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
    String indexName = uniqueIndex("TEST_VECTOR_IDX_");
    String otherIndexName = "TEST_VECTOR_IDX_" + generateUniqueName();
    List<byte[]> gen1 =
      Arrays.asList(new byte[] { 1 }, new byte[] { 2 }, new byte[] { 3 }, new byte[] { 4 });
    List<byte[]> gen2 =
      Arrays.asList(new byte[] { 5 }, new byte[] { 6 }, new byte[] { 7 }, new byte[] { 8 });
    byte[] otherVector = new byte[] { 9 };

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      CentroidManager manager = new CentroidManager(conn);
      manager.persistCentroids(indexName, 1L, gen1);
      manager.persistCentroids(indexName, 2L, gen2);
      manager.persistCentroids(otherIndexName, 1L, Arrays.asList(otherVector));
      manager.persistGenerationSummary(new GenerationSummary.Builder().setIndexName(indexName)
        .setGenerationId(1L).setRebuildState(REBUILD_STATE_RETIRED).build());
      manager.persistScorecardRow(new ScorecardRow(indexName, 1L, 0, 100L, 5L, 1000L));

      manager.deleteGeneration(indexName, 1L);

      // Verify retired generation rows are removed
      assertTrue(manager.loadCentroids(indexName, 1L).isEmpty());
      assertTrue(manager.loadScorecard(indexName, 1L).isEmpty());
      assertNull(manager.loadGenerationSummary(indexName, 1L));
      assertEquals(Arrays.asList(2L), manager.listGenerations(indexName));

      // Verify other generations and indexes remain unaffected
      List<byte[]> survivors = manager.loadCentroids(indexName, 2L);
      assertEquals(4, survivors.size());
      for (int i = 0; i < 4; i++) {
        assertArrayEquals(gen2.get(i), survivors.get(i));
      }
      assertEquals(1, manager.loadCentroids(otherIndexName, 1L).size());
      assertArrayEquals(otherVector, manager.loadCentroids(otherIndexName, 1L).get(0));
    }
  }

  @Test
  public void testIncrementGeneration() throws Exception {
    String indexName = uniqueIndex("TEST_VECTOR_IDX_");

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
