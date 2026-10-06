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
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.apache.phoenix.util.QueryUtil;
import org.bson.BinaryVector;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Integration tests for BSON vector extraction via BSON_VECTOR_VALUE, functional vector indexing,
 * query optimization, and server-side projection.
 */
@Category(ParallelStatsDisabledTest.class)
public class VectorBsonIndexIT extends ParallelStatsDisabledIT {

  private static final int DIM = 8;

  private static BsonBinary bsonVector(float[] v) {
    return new BsonBinary(BinaryVector.floatVector(v));
  }

  /** Generates a test vector with the specified leading component. */
  private static float[] vecX(float x) {
    float[] v = new float[DIM];
    v[0] = x;
    return v;
  }

  private static BsonDocument embeddingDoc(float[] v, String category) {
    BsonDocument doc = new BsonDocument("embedding", bsonVector(v));
    doc.put("category", new BsonString(category));
    return doc;
  }

  private static void upsertDoc(Connection conn, String table, String id, BsonDocument doc)
    throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + table + " VALUES (?, ?)")) {
      ps.setString(1, id);
      ps.setObject(2, doc);
      ps.executeUpdate();
    }
    conn.commit();
  }

  private static int countRows(Connection conn, String table) throws SQLException {
    try (Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
      assertTrue(rs.next());
      return rs.getInt(1);
    }
  }

  private static Float[] boxed(float[] v) {
    Float[] b = new Float[v.length];
    for (int i = 0; i < v.length; i++) {
      b[i] = v[i];
    }
    return b;
  }

  private static List<String> runSearch(Connection conn, String sql, float[] q)
    throws SQLException {
    List<String> ids = new ArrayList<>();
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setArray(1, conn.createArrayOf("FLOAT", boxed(q)));
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          ids.add(rs.getString(1));
        }
      }
    }
    return ids;
  }

  private static String explain(Connection conn, String sql, float[] q) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement("EXPLAIN " + sql)) {
      ps.setArray(1, conn.createArrayOf("FLOAT", boxed(q)));
      return QueryUtil.getExplainPlan(ps.executeQuery());
    }
  }

  /** Test centroids partitioned along the first dimension. */
  private static List<float[]> fourCentroids() {
    List<float[]> centroids = new ArrayList<>();
    for (int c = 0; c < 4; c++) {
      centroids.add(vecX(c * 10.0f));
    }
    return centroids;
  }

  /** Maps indexed row IDs to their assigned centroid IDs. */
  private static Map<String, Integer> indexCentroidById(PhoenixConnection pconn, String indexName)
    throws Exception {
    PTable indexTable = pconn.getTableNoCache(indexName);
    Map<String, Integer> byId = new HashMap<>();
    for (byte[] rk : VectorIndexTestUtil.getHBaseRowKeys(pconn, indexTable)) {
      String id =
        (String) PVarchar.INSTANCE.toObject(rk, Bytes.SIZEOF_INT, rk.length - Bytes.SIZEOF_INT);
      byId.put(id, VectorIndexTestUtil.extractCentroidId(rk));
    }
    return byId;
  }

  private static void createIndexedTable(Connection conn, String table, String index)
    throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE " + table + " (ID VARCHAR NOT NULL PRIMARY KEY, DOC BSON)");
      stmt.execute(
        "CREATE VECTOR INDEX " + index + " ON " + table + " (BSON_VECTOR_VALUE(doc, 'embedding', "
          + DIM + ")) WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
    }
  }

  @Test
  public void testBsonFunctionalVectorIndexWriteLifecycle() throws Exception {
    String tableName = "T_BSON_FUNC_" + generateUniqueName();
    String indexName = "IDX_BSON_FUNC_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createIndexedTable(conn, tableName, indexName);
      List<float[]> centroids = fourCentroids();
      VectorIndexTestUtil.activateWithKnownCentroids(conn, tableName, indexName, centroids);
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);

      // Functional vector indexes store the computed vector directly in the index row
      PTable dataTable = pconn.getTableNoCache(tableName);
      PTable indexTable = pconn.getTableNoCache(indexName);
      IndexMaintainer maintainer = indexTable.getIndexMaintainer(dataTable, pconn);
      assertNotNull(maintainer.getFunctionalVectorColumn());
      String colTable = "T_COL_VEC_" + generateUniqueName();
      String colIndex = "IDX_COL_VEC_" + generateUniqueName();
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + colTable
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, " + DIM + "))");
        stmt.execute("CREATE VECTOR INDEX " + colIndex + " ON " + colTable
          + " (V) WITH (algorithm = 'IVF', metric = 'L2', lists = 4, sample_size = 100)");
      }
      assertNull(pconn.getTableNoCache(colIndex)
        .getIndexMaintainer(pconn.getTableNoCache(colTable), pconn).getFunctionalVectorColumn());

      // Seed test rows spanning centroids as well as null and missing vector cases
      Map<String, float[]> vectors = new LinkedHashMap<>();
      for (int i = 0; i < 40; i++) {
        float[] v = vecX(i);
        vectors.put("id_" + i, v);
        upsertDoc(conn, tableName, "id_" + i, embeddingDoc(v, "c" + (i % 3)));
      }
      for (int i = 0; i < 5; i++) {
        upsertDoc(conn, tableName, "noemb_" + i,
          new BsonDocument("category", new BsonString("none")));
      }
      upsertDoc(conn, tableName, "nullemb", new BsonDocument("embedding", BsonNull.VALUE));
      assertEquals(46, countRows(conn, tableName));
      assertEquals(40, countRows(conn, indexName));

      Map<String, Integer> byId = indexCentroidById(pconn, indexName);
      assertEquals(40, byId.size());
      for (Map.Entry<String, float[]> e : vectors.entrySet()) {
        int expected = VectorIndexTestUtil.nearestCentroid(e.getValue(), centroids, "L2");
        assertEquals("centroid of " + e.getKey(), Integer.valueOf(expected), byId.get(e.getKey()));
      }

      // Verify persisted vector encoding matches big endian PVectorFloat layout
      ColumnReference vecRef = maintainer.getFunctionalVectorColumn();
      byte[] rowKeyOf7 = null;
      for (byte[] rk : VectorIndexTestUtil.getHBaseRowKeys(pconn, indexTable)) {
        String id =
          (String) PVarchar.INSTANCE.toObject(rk, Bytes.SIZEOF_INT, rk.length - Bytes.SIZEOF_INT);
        if ("id_7".equals(id)) {
          rowKeyOf7 = rk;
        }
      }
      assertNotNull(rowKeyOf7);
      try (
        Table hTable = pconn.getQueryServices().getTable(indexTable.getPhysicalName().getBytes())) {
        Result r = hTable.get(new Get(rowKeyOf7));
        assertArrayEquals(PVectorFloat.INSTANCE.toBytes(vecX(7)),
          r.getValue(vecRef.getFamily(), vecRef.getQualifier()));
      }

      // Non-vector field mutation preserves existing index row key
      upsertDoc(conn, tableName, "id_7", embeddingDoc(vecX(7), "renamed"));
      assertEquals(40, countRows(conn, indexName));
      assertEquals(byId.get("id_7"), indexCentroidById(pconn, indexName).get("id_7"));

      // Vector value mutation relocates index entry to the newly assigned centroid
      assertEquals(Integer.valueOf(1), byId.get("id_7"));
      upsertDoc(conn, tableName, "id_7", embeddingDoc(vecX(35.5f), "renamed"));
      vectors.put("id_7", vecX(35.5f));
      assertEquals(40, countRows(conn, indexName));
      assertEquals(Integer.valueOf(3), indexCentroidById(pconn, indexName).get("id_7"));

      // Vector deletion removes the corresponding index entry
      upsertDoc(conn, tableName, "id_8", new BsonDocument("category", new BsonString("gone")));
      vectors.remove("id_8");
      assertEquals(46, countRows(conn, tableName));
      assertEquals(39, countRows(conn, indexName));
      assertFalse(indexCentroidById(pconn, indexName).containsKey("id_8"));

      // Malformed vector payload rejects write operation
      BsonBinary[] malformed = new BsonBinary[] { bsonVector(new float[DIM + 1]),
        new BsonBinary(BinaryVector.int8Vector(new byte[DIM])),
        new BsonBinary(BsonBinarySubType.BINARY, bsonVector(vecX(1)).getData()) };
      for (BsonBinary bad : malformed) {
        try {
          upsertDoc(conn, tableName, "bad", new BsonDocument("embedding", bad));
          fail("Malformed embedding " + bad + " must be rejected at write time");
        } catch (SQLException expected) {
          conn.rollback();
        }
      }
      assertEquals(46, countRows(conn, tableName));
      assertEquals(39, countRows(conn, indexName));

      float[] q = vecX(35.3f);
      String sql = "SELECT ID FROM " + tableName + " ORDER BY L2_DISTANCE(BSON_VECTOR_VALUE(doc, "
        + "'embedding', " + DIM + "), ?) LIMIT 3";
      assertTrue(explain(conn, sql, q).contains(indexName));
      assertEquals(VectorIndexTestUtil.bruteForceTopK(vectors, q, "L2", 3),
        runSearch(conn, sql, q));
    }
  }

  /**
   * Verifies that preexisting unindexed rows with malformed vectors remain deletable and updateable
   * without failing maintenance validation.
   */
  @Test
  public void testMalformedCurrentVectorDoesNotBlockDeleteOrCorrection() throws Exception {
    String tableName = "T_BSON_BAD_" + generateUniqueName();
    String indexName = "IDX_BSON_BAD_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      // Untrained index on empty table omits maintenance operations
      createIndexedTable(conn, tableName, indexName);
      BsonDocument bad =
        new BsonDocument("embedding", new BsonBinary(BinaryVector.int8Vector(new byte[DIM])));
      upsertDoc(conn, tableName, "fix", bad);
      upsertDoc(conn, tableName, "del", bad);
      upsertDoc(conn, tableName, "keep", bad);
      VectorIndexTestUtil.activateWithKnownCentroids(conn, tableName, indexName, fourCentroids());
      assertEquals(0, countRows(conn, indexName));

      upsertDoc(conn, tableName, "fix", embeddingDoc(vecX(21f), "fixed"));
      conn.createStatement().execute("DELETE FROM " + tableName + " WHERE ID = 'del'");
      conn.commit();
      try {
        upsertDoc(conn, tableName, "keep", bad);
        fail("Writing a malformed vector must fail");
      } catch (SQLException expected) {
        conn.rollback();
      }
      assertEquals(2, countRows(conn, tableName));
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      Map<String, Integer> byId = indexCentroidById(pconn, indexName);
      assertEquals(1, byId.size());
      assertEquals(Integer.valueOf(2), byId.get("fix"));
    }
  }

  @Test
  public void testBsonFunctionalVectorIndexSearch() throws Exception {
    String tableName = "T_BSON_QUERY_" + generateUniqueName();
    String indexName = "IDX_BSON_QUERY_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createIndexedTable(conn, tableName, indexName);
      VectorIndexTestUtil.activateWithKnownCentroids(conn, tableName, indexName, fourCentroids());

      // Populate boundary vectors across centroid thresholds
      Map<String, float[]> vectors = new LinkedHashMap<>();
      float[] xs = { 1f, 4f, 6f, 9f, 11f, 14f, 16f, 19f, 21f, 24f, 26f, 29f };
      for (int i = 0; i < xs.length; i++) {
        float[] v = vecX(xs[i]);
        v[1] = 0.1f * i;
        vectors.put("r" + i, v);
        upsertDoc(conn, tableName, "r" + i, embeddingDoc(v, "c" + (i % 2)));
      }

      String sql = "SELECT ID FROM " + tableName + " ORDER BY L2_DISTANCE(BSON_VECTOR_VALUE(doc, "
        + "'embedding', " + DIM + "), ?) LIMIT 3";
      for (float qx : new float[] { 5f, 15f, 25f, 0f }) {
        float[] q = vecX(qx);
        String plan = explain(conn, sql, q);
        assertTrue("Plan must use the BSON functional vector index: " + plan,
          plan.contains(indexName) && plan.contains("CLIENT PROBING"));
        assertEquals("query x=" + qx, VectorIndexTestUtil.bruteForceTopK(vectors, q, "L2", 3),
          runSearch(conn, sql, q));
      }

      // Validate commutative distance operand parsing and plan selection
      String flipped = "SELECT ID FROM " + tableName
        + " ORDER BY L2_DISTANCE(?, BSON_VECTOR_VALUE(doc, 'embedding', " + DIM + ")) LIMIT 3";
      float[] q = vecX(15f);
      assertTrue(explain(conn, flipped, q).contains(indexName));
      assertEquals(VectorIndexTestUtil.bruteForceTopK(vectors, q, "L2", 3),
        runSearch(conn, flipped, q));
    }
  }

  /**
   * Ensures covered vector columns with differing dimensions do not collide with indexed vectors.
   */
  @Test
  public void testBsonVectorIndexWithCoveredVectorColumnOfAnotherDimension() throws Exception {
    String tableName = "T_BSON_COV_" + generateUniqueName();
    String indexName = "IDX_BSON_COV_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, DOC BSON, V4 VECTOR(FLOAT, 4))");
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName
          + " (BSON_VECTOR_VALUE(doc, 'embedding', " + DIM + ")) INCLUDE (V4)"
          + " WITH (algorithm = 'IVF', metric = 'L2', lists = 2, sample_size = 100)");
      }
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable indexTable = pconn.getTableNoCache(indexName);
      assertEquals(Integer.valueOf(DIM), indexTable.getVectorDimension());
      assertNotNull(indexTable.getIndexMaintainer(pconn.getTableNoCache(tableName), pconn)
        .getFunctionalVectorColumn());

      VectorIndexTestUtil.activateWithKnownCentroids(conn, tableName, indexName,
        Arrays.asList(vecX(0f), vecX(10f)));
      try (PreparedStatement ps =
        conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?, ?, ?)")) {
        ps.setString(1, "r1");
        ps.setObject(2, new BsonDocument("embedding", bsonVector(vecX(11f))));
        ps.setArray(3, conn.createArrayOf("FLOAT", new Float[] { 1f, 2f, 3f, 4f }));
        ps.executeUpdate();
      }
      conn.commit();
      assertEquals(1, countRows(conn, indexName));
      assertEquals(Integer.valueOf(1), indexCentroidById(pconn, indexName).get("r1"));
    }
  }

  /** Verifies index rejection when query path, dimension, or target column mismatches. */
  @Test
  public void testBsonVectorIndexNotUsedForDifferentExpression() throws Exception {
    String tableName = "T_BSON_OTHER_" + generateUniqueName();
    String indexName = "IDX_BSON_OTHER_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, DOC BSON, V VECTOR(FLOAT, " + DIM + "))");
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName
          + " (BSON_VECTOR_VALUE(doc, 'embedding', " + DIM + ")) INCLUDE (DOC, V)"
          + " WITH (algorithm = 'IVF', metric = 'L2', lists = 2, sample_size = 100)");
      }
      VectorIndexTestUtil.activateWithKnownCentroids(conn, tableName, indexName,
        Arrays.asList(vecX(0f), vecX(10f)));

      String[] ids = { "A1", "A2", "A3", "B1", "B2", "B3" };
      float[] embX = { 2f, 3f, 4f, 6f, 7f, 8f };
      float[] otherX = { 100f, 101f, 102f, 0f, 1f, 2f };
      Map<String, float[]> otherRows = new LinkedHashMap<>();
      try (PreparedStatement ps =
        conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?, ?, ?)")) {
        for (int i = 0; i < ids.length; i++) {
          BsonDocument doc = new BsonDocument("embedding", bsonVector(vecX(embX[i])));
          doc.put("other", bsonVector(vecX(otherX[i])));
          ps.setString(1, ids[i]);
          ps.setObject(2, doc);
          ps.setArray(3, conn.createArrayOf("FLOAT", boxed(vecX(otherX[i]))));
          ps.executeUpdate();
          otherRows.put(ids[i], vecX(otherX[i]));
        }
        conn.commit();
      }

      float[] q = vecX(0f);
      List<String> expected = VectorIndexTestUtil.bruteForceTopK(otherRows, q, "L2", 2);
      String[] sqls = {
        "SELECT ID FROM " + tableName + " ORDER BY L2_DISTANCE(BSON_VECTOR_VALUE(doc, 'other', "
          + DIM + "), ?) LIMIT 2",
        "SELECT ID FROM " + tableName + " ORDER BY L2_DISTANCE(V, ?) LIMIT 2" };
      for (String sql : sqls) {
        String plan = explain(conn, sql, q);
        assertFalse("Index on 'embedding' must not serve: " + sql + "\n" + plan,
          plan.contains(indexName));
        assertEquals(sql, expected, runSearch(conn, sql, q));
      }

      String dimSql =
        "SELECT ID FROM " + tableName + " ORDER BY L2_DISTANCE(BSON_VECTOR_VALUE(doc, 'embedding', "
          + (DIM + 1) + "), ?) LIMIT 2";
      assertFalse(explain(conn, dimSql, new float[DIM + 1]).contains(indexName));

      Map<String, float[]> embRows = new LinkedHashMap<>();
      for (int i = 0; i < ids.length; i++) {
        embRows.put(ids[i], vecX(embX[i]));
      }
      String embSql = "SELECT ID FROM " + tableName
        + " ORDER BY L2_DISTANCE(BSON_VECTOR_VALUE(doc, 'embedding', " + DIM + "), ?) LIMIT 2";
      assertTrue(explain(conn, embSql, q).contains(indexName));
      assertEquals(VectorIndexTestUtil.bruteForceTopK(embRows, q, "L2", 2),
        runSearch(conn, embSql, q));
    }
  }

  /**
   * Tests exact nearest neighbor search over document vectors with server-side projections.
   */
  @Test
  public void testExactSearchOverDocumentVectors() throws Exception {
    String tableName = "T_BSON_EXACT_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (ID VARCHAR NOT NULL PRIMARY KEY, DOC BSON)");
      Map<String, float[]> vectors = new LinkedHashMap<>();
      for (int i = 0; i < 20; i++) {
        float[] v = vecX((i * 7) % 20);
        v[1] = i;
        vectors.put("r" + i, v);
        upsertDoc(conn, tableName, "r" + i, embeddingDoc(v, "n" + i));
      }
      float[] q = vecX(9.5f);
      q[1] = 3f;
      List<String> expected = VectorIndexTestUtil.bruteForceTopK(vectors, q, "L2", 5);
      String distance = "L2_DISTANCE(BSON_VECTOR_VALUE(doc, 'embedding', " + DIM + "), ?)";
      String[] sqls = { "SELECT ID FROM " + tableName + " ORDER BY " + distance + " LIMIT 5",
        "SELECT ID, " + distance + " D FROM " + tableName + " ORDER BY D LIMIT 5",
        "SELECT ID, BSON_VALUE(DOC, 'category', 'VARCHAR') FROM " + tableName + " ORDER BY "
          + distance + " LIMIT 5" };
      for (String sql : sqls) {
        assertEquals(sql, expected, runSearch(conn, sql, q));
      }
      try (PreparedStatement ps = conn.prepareStatement(sqls[2])) {
        ps.setArray(1, conn.createArrayOf("FLOAT", boxed(q)));
        try (ResultSet rs = ps.executeQuery()) {
          for (String id : expected) {
            assertTrue(rs.next());
            assertEquals(id, rs.getString(1));
            assertEquals("n" + id.substring(1), rs.getString(2));
          }
        }
      }
    }
  }

  @Test
  public void testServerSideBsonVectorProjection() throws Exception {
    String tableName = "T_BSON_PROJ_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (ID VARCHAR NOT NULL PRIMARY KEY, DOC BSON)");
      float[] v1 = new float[] { 1.5f, -2.5f, 3.0e-3f };
      float[] v2 = new float[] { 4.0f, 5.0f, 6.0f };
      BsonDocument data1 = new BsonDocument("vec", bsonVector(v1));
      data1.put("name", new BsonString("one"));
      BsonDocument data2 = new BsonDocument("vec", bsonVector(v2));
      data2.put("name", new BsonString("two"));
      upsertDoc(conn, tableName, "row1", new BsonDocument("data", data1));
      upsertDoc(conn, tableName, "row2", new BsonDocument("data", data2));
      upsertDoc(conn, tableName, "row3",
        new BsonDocument("data", new BsonDocument("other", new BsonString("hello"))));

      // Verify pushdown of vector extraction to region server
      String projSql =
        "SELECT ID, BSON_VECTOR_VALUE(doc, 'data.vec', 3) FROM " + tableName + " ORDER BY ID";
      try (ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + projSql)) {
        String plan = QueryUtil.getExplainPlan(rs);
        assertTrue(plan, plan.contains("SERVER BSON PROJECTION 1"));
        assertTrue(plan, plan.contains("BSON_VECTOR_VALUE(DOC, 'data.vec', 3)"));
      }
      try (ResultSet rs = conn.createStatement().executeQuery(projSql)) {
        assertTrue(rs.next());
        assertEquals("row1", rs.getString(1));
        assertArrayEquals(v1, (float[]) rs.getObject(2), 1e-6f);
        assertTrue(rs.next());
        assertEquals("row2", rs.getString(1));
        assertArrayEquals(v2, (float[]) rs.getObject(2), 1e-6f);
        assertTrue(rs.next());
        assertEquals("row3", rs.getString(1));
        assertNull(rs.getObject(2));
        assertTrue(rs.wasNull());
        assertFalse(rs.next());
      }

      // Projection ordering: fixed-width vector column preceding variable-width document fields
      String mixedSql = "SELECT ID, BSON_VECTOR_VALUE(doc, 'data.vec', 3),"
        + " BSON_VALUE(doc, 'data.name', 'VARCHAR') FROM " + tableName + " WHERE ID = 'row2'";
      try (ResultSet rs = conn.createStatement().executeQuery(mixedSql)) {
        assertTrue(rs.next());
        assertArrayEquals(v2, (float[]) rs.getObject(2), 1e-6f);
        assertEquals("two", rs.getString(3));
        assertFalse(rs.next());
      }

      String distSql = "SELECT L2_DISTANCE(BSON_VECTOR_VALUE(doc, 'data.vec', 3), ARRAY[1.5, -2.5,"
        + " 0.003]) FROM " + tableName + " WHERE ID = 'row1'";
      try (ResultSet rs = conn.createStatement().executeQuery(distSql)) {
        assertTrue(rs.next());
        assertEquals(0.0, rs.getDouble(1), 1e-6);
        assertFalse(rs.next());
      }

      // Full document projection delegates vector extraction to client side evaluation
      String fullDocSql = "SELECT doc, BSON_VECTOR_VALUE(doc, 'data.vec', 3) FROM " + tableName
        + " WHERE ID = 'row1'";
      try (ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + fullDocSql)) {
        String plan = QueryUtil.getExplainPlan(rs);
        assertFalse(plan, plan.contains("SERVER BSON PROJECTION"));
      }
      try (ResultSet rs = conn.createStatement().executeQuery(fullDocSql)) {
        assertTrue(rs.next());
        BsonDocument returnedDoc = (BsonDocument) rs.getObject(1);
        assertArrayEquals(v1,
          returnedDoc.getDocument("data").getBinary("vec").asVector().asFloat32Vector().getData(),
          0f);
        assertArrayEquals(v1, (float[]) rs.getObject(2), 1e-6f);
        assertFalse(rs.next());
      }

      // Region server evaluation failure on vector dimension mismatch
      String badDim = "SELECT BSON_VECTOR_VALUE(doc, 'data.vec', 2) FROM " + tableName;
      try (ResultSet rs = conn.createStatement().executeQuery(badDim)) {
        while (rs.next()) {
          rs.getObject(1);
        }
        fail("Expected evaluation exception on dimension mismatch");
      } catch (SQLException e) {
        assertTrue(e.getMessage(), e.getMessage().contains("dimension mismatch"));
      }
    }
  }
}
