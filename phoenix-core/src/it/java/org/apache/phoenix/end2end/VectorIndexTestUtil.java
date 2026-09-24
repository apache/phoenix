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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.cache.VectorCentroidCache;
import org.apache.phoenix.index.vector.CentroidManager;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.IndexUtil;

/** Shared test utilities and fixture builders for vector index integration tests. */
class VectorIndexTestUtil {

  /**
   * Clears shared vector state between tests, including default index name and centroid-manager
   * connections.
   */
  static void resetSharedVectorState() {
    VectorCentroidCache.getInstance().setDefaultIndexName(null);
    CentroidManager.clearThreadLocalConnection();
    CentroidManager.setDefaultConnection(null);
  }

  /** Removes {@code SYSTEM.VECTOR_CENTROID} rows for the specified indexes. */
  static void deleteCentroidRows(Connection conn, Collection<String> indexNames)
    throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement("DELETE FROM "
      + PhoenixDatabaseMetaData.SYSTEM_VECTOR_CENTROID_NAME + " WHERE INDEX_NAME = ?")) {
      for (String indexName : indexNames) {
        ps.setString(1, indexName);
        ps.executeUpdate();
      }
    }
    conn.commit();
  }

  static void activateWithKnownCentroids(Connection conn, String tableName, String indexName,
    List<float[]> centroids, long generation) throws SQLException {
    CentroidManager.persistCentroidsFromFloatList(conn, indexName, generation, centroids);
    CentroidManager.setGenerationAndLists(conn, indexName, generation, centroids.size());
    PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
    IndexUtil.updateIndexState(pconn, indexName, PIndexState.ACTIVE, 0L);
    pconn.removeTable(pconn.getTenantId(), indexName, null, HConstants.LATEST_TIMESTAMP);
    pconn.removeTable(pconn.getTenantId(), tableName, null, HConstants.LATEST_TIMESTAMP);
    VectorCentroidCache.getInstance().putFloatCentroids(indexName, generation, centroids);
  }

  static List<byte[]> getHBaseRowKeys(PhoenixConnection pconn, PTable table) throws Exception {
    List<byte[]> rowKeys = new ArrayList<>();
    byte[] physicalName = table.getPhysicalName().getBytes();
    try (Table hTable = pconn.getQueryServices().getTable(physicalName);
      ResultScanner scanner = hTable.getScanner(new Scan())) {
      for (Result r : scanner) {
        rowKeys.add(r.getRow());
      }
    }
    return rowKeys;
  }

  static int extractCentroidId(byte[] rowKey, boolean salted) {
    int offset = salted ? 1 : 0;
    return (Integer) PInteger.INSTANCE.toObject(rowKey, offset, Bytes.SIZEOF_INT, PInteger.INSTANCE,
      SortOrder.getDefault());
  }

  static int extractCentroidId(byte[] rowKey, boolean salted, int tenantOffset) {
    int offset = (salted ? 1 : 0) + tenantOffset;
    return (Integer) PInteger.INSTANCE.toObject(rowKey, offset, Bytes.SIZEOF_INT, PInteger.INSTANCE,
      SortOrder.getDefault());
  }

  static double dist(String metric, float[] a, float[] b) {
    double dot = 0, normA = 0, normB = 0, sumSq = 0;
    for (int i = 0; i < a.length; i++) {
      double diff = a[i] - b[i];
      sumSq += diff * diff;
      dot += a[i] * b[i];
      normA += a[i] * a[i];
      normB += b[i] * b[i];
    }
    switch (metric.toUpperCase()) {
      case "L2":
      case "L2_DISTANCE":
        return Math.sqrt(sumSq);
      case "L2_SQUARED":
      case "L2_DISTANCE_SQUARED":
        return sumSq;
      case "COSINE":
      case "COSINE_DISTANCE":
        double denom = Math.sqrt(normA) * Math.sqrt(normB);
        return (denom == 0) ? 1.0 : 1.0 - dot / denom;
      case "INNER_PRODUCT":
        return -dot;
      default:
        throw new IllegalArgumentException("Unknown metric: " + metric);
    }
  }

  static int nearestCentroid(float[] v, List<float[]> centroids, String metric) {
    int bestId = -1;
    double bestDist = Double.MAX_VALUE;
    for (int c = 0; c < centroids.size(); c++) {
      double d = dist(metric, v, centroids.get(c));
      if (d < bestDist) {
        bestDist = d;
        bestId = c;
      }
    }
    return bestId;
  }

  static List<String> bruteForceTopK(Map<String, float[]> rows, float[] q, String metric, int k) {
    List<Map.Entry<String, Double>> scored = new ArrayList<>();
    for (Map.Entry<String, float[]> entry : rows.entrySet()) {
      double d = dist(metric, q, entry.getValue());
      scored.add(new AbstractMap.SimpleEntry<>(entry.getKey(), d));
    }
    scored.sort((e1, e2) -> {
      int cmp = Double.compare(e1.getValue(), e2.getValue());
      if (cmp != 0) {
        return cmp;
      }
      return e1.getKey().compareTo(e2.getKey());
    });
    List<String> topK = new ArrayList<>();
    for (int i = 0; i < Math.min(k, scored.size()); i++) {
      topK.add(scored.get(i).getKey());
    }
    return topK;
  }

  static class ClusteredFixture {
    final String tableName;
    final String indexName;
    final List<float[]> centroids;
    final Map<String, float[]> rows;
    final Map<String, String> categories;
    final Map<String, String> descriptions;
    final Map<String, float[]> v2Rows;

    ClusteredFixture(String tableName, String indexName, List<float[]> centroids,
      Map<String, float[]> rows, Map<String, String> categories, Map<String, String> descriptions,
      Map<String, float[]> v2Rows) {
      this.tableName = tableName;
      this.indexName = indexName;
      this.centroids = centroids;
      this.rows = rows;
      this.categories = categories;
      this.descriptions = descriptions;
      this.v2Rows = v2Rows;
    }
  }

  static class HandPlacedFixture {
    final String tableName;
    final String indexName;
    final List<float[]> centroids;
    final Map<String, float[]> rows;
    final float[] queryVector;

    HandPlacedFixture(String tableName, String indexName, List<float[]> centroids,
      Map<String, float[]> rows, float[] queryVector) {
      this.tableName = tableName;
      this.indexName = indexName;
      this.centroids = centroids;
      this.rows = rows;
      this.queryVector = queryVector;
    }
  }

  static ClusteredFixture buildClusteredL2Fixture(Connection conn, String tableName,
    String indexName, boolean includeCategory, boolean includeExtraCols) throws Exception {
    int lists = 4;
    int dim = 4;
    int rowsPerCluster = 20;
    List<float[]> centroids = new ArrayList<>();
    for (int k = 0; k < lists; k++) {
      centroids.add(new float[] { k * 50.0f, 0.0f, 0.0f, 0.0f });
    }

    try (Statement stmt = conn.createStatement()) {
      if (includeExtraCols) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), CATEGORY VARCHAR, "
          + "DESCRIPTION VARCHAR, V2 VECTOR(FLOAT, 3))");
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
          + "WITH (algorithm = 'IVF', metric = 'L2', lists = " + lists + ", sample_size = 100)");
      } else if (includeCategory) {
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), CATEGORY VARCHAR)");
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
          + "INCLUDE (CATEGORY) WITH (algorithm = 'IVF', metric = 'L2', lists = " + lists
          + ", sample_size = 100)");
      } else {
        stmt.execute(
          "CREATE TABLE " + tableName + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4))");
        stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
          + "WITH (algorithm = 'IVF', metric = 'L2', lists = " + lists + ", sample_size = 100)");
      }
    }

    activateWithKnownCentroids(conn, tableName, indexName, centroids, 1L);

    Random rng = new Random(42);
    Map<String, float[]> rows = new LinkedHashMap<>();
    Map<String, String> categories = new LinkedHashMap<>();
    Map<String, String> descriptions = new LinkedHashMap<>();
    Map<String, float[]> v2Rows = new LinkedHashMap<>();

    String upsertSql;
    if (includeExtraCols) {
      upsertSql =
        "UPSERT INTO " + tableName + " (ID, V, CATEGORY, DESCRIPTION, V2) VALUES (?, ?, ?, ?, ?)";
    } else if (includeCategory) {
      upsertSql = "UPSERT INTO " + tableName + " (ID, V, CATEGORY) VALUES (?, ?, ?)";
    } else {
      upsertSql = "UPSERT INTO " + tableName + " (ID, V) VALUES (?, ?)";
    }

    try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
      int count = 0;
      for (int k = 0; k < lists; k++) {
        float[] center = centroids.get(k);
        for (int i = 0; i < rowsPerCluster; i++) {
          String rowId = String.format("k%d_r%02d", k, i);
          float[] vec = new float[] { center[0] + (float) (rng.nextGaussian() * 0.5),
            center[1] + (float) (rng.nextGaussian() * 0.5),
            center[2] + (float) (rng.nextGaussian() * 0.5),
            center[3] + (float) (rng.nextGaussian() * 0.5) };
          rows.put(rowId, vec);
          ps.setString(1, rowId);
          Float[] boxedVec = new Float[] { vec[0], vec[1], vec[2], vec[3] };
          ps.setArray(2, conn.createArrayOf("FLOAT", boxedVec));

          if (includeCategory || includeExtraCols) {
            String cat = (i % 2 == 0) ? "science" : "art";
            categories.put(rowId, cat);
            ps.setString(3, cat);
          }
          if (includeExtraCols) {
            String desc = "desc_" + rowId;
            descriptions.put(rowId, desc);
            ps.setString(4, desc);
            float[] v2 = new float[] { (float) i, 0.0f, 1.0f };
            v2Rows.put(rowId, v2);
            ps.setArray(5, conn.createArrayOf("FLOAT", new Float[] { v2[0], v2[1], v2[2] }));
          }
          ps.executeUpdate();
          count++;
          if (count % 100 == 0) {
            conn.commit();
          }
        }
      }
      conn.commit();
    }

    return new ClusteredFixture(tableName, indexName, centroids, rows, categories, descriptions,
      v2Rows);
  }

  static HandPlacedFixture buildProbeFixture(Connection conn, String tableName, String indexName,
    Integer saltBuckets, boolean multiTenant) throws Exception {
    List<float[]> centroids = Arrays.asList(new float[] { 0.0f, 0.0f, 0.0f, 0.0f },
      new float[] { 10.0f, 0.0f, 0.0f, 0.0f });

    try (Statement stmt = conn.createStatement()) {
      if (multiTenant) {
        String options = "MULTI_TENANT=true"
          + (saltBuckets != null && saltBuckets > 0 ? ", SALT_BUCKETS=" + saltBuckets : "");
        stmt.execute("CREATE TABLE " + tableName
          + " (TENANT_ID VARCHAR NOT NULL, ID VARCHAR NOT NULL, V VECTOR(FLOAT, 4) "
          + "CONSTRAINT PK PRIMARY KEY (TENANT_ID, ID)) " + options);
      } else {
        String options =
          (saltBuckets != null && saltBuckets > 0) ? " SALT_BUCKETS=" + saltBuckets : "";
        stmt.execute("CREATE TABLE " + tableName
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4))" + options);
      }
      stmt.execute("CREATE VECTOR INDEX " + indexName + " ON " + tableName + " (V) "
        + "WITH (algorithm = 'IVF', metric = 'L2', lists = 2, sample_size = 100)");
    }

    activateWithKnownCentroids(conn, tableName, indexName, centroids, 1L);

    Map<String, float[]> rows = new LinkedHashMap<>();
    rows.put("A1", new float[] { 4.0f, 0.0f, 0.0f, 0.0f });
    rows.put("A2", new float[] { 3.0f, 0.0f, 0.0f, 0.0f });
    rows.put("A3", new float[] { 2.0f, 0.0f, 0.0f, 0.0f });
    rows.put("B1", new float[] { 6.0f, 0.0f, 0.0f, 0.0f });
    rows.put("B2", new float[] { 7.0f, 0.0f, 0.0f, 0.0f });
    rows.put("B3", new float[] { 8.0f, 0.0f, 0.0f, 0.0f });

    if (!multiTenant) {
      String upsertSql = "UPSERT INTO " + tableName + " (ID, V) VALUES (?, ?)";
      try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
        for (Map.Entry<String, float[]> entry : rows.entrySet()) {
          ps.setString(1, entry.getKey());
          float[] v = entry.getValue();
          ps.setArray(2, conn.createArrayOf("FLOAT", new Float[] { v[0], v[1], v[2], v[3] }));
          ps.executeUpdate();
        }
        conn.commit();
      }
    }

    float[] queryVec = new float[] { 4.9f, 0.0f, 0.0f, 0.0f };
    return new HandPlacedFixture(tableName, indexName, centroids, rows, queryVec);
  }
}
