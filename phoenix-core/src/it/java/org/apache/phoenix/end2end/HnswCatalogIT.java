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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_VECTOR_CENTROID_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
import java.util.List;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.schema.VectorIndexType;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Integration tests for HNSW vector index DDL validation, schema definition, and catalog
 * persistence.
 */
@Category(ParallelStatsDisabledTest.class)
public class HnswCatalogIT extends ParallelStatsDisabledIT {

  private static List<String> pkColumnNames(PTable table) {
    List<String> names = new ArrayList<>();
    for (PColumn col : table.getPKColumns()) {
      names.add(col.getName().getString());
    }
    return names;
  }

  /**
   * Tests that HNSW index tables retain data table primary key columns without prepending a
   * centroid ID column.
   */
  @Test
  public void testIndexRowKeySchema() throws Exception {
    String table = generateUniqueName();
    String hnsw = generateUniqueName();
    String ivf = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl());
      Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE " + table + " (TENANT_ID VARCHAR NOT NULL, ID1 VARCHAR NOT NULL, "
        + "ID2 BIGINT NOT NULL, V VECTOR(FLOAT, 4), C1 VARCHAR, "
        + "CONSTRAINT PK PRIMARY KEY (TENANT_ID, ID1, ID2)) MULTI_TENANT=true, SALT_BUCKETS=2");
      stmt.execute("CREATE VECTOR INDEX " + hnsw + " ON " + table
        + " (V) WITH (algorithm='HNSW', metric='COSINE')");
      stmt.execute("CREATE VECTOR INDEX " + ivf + " ON " + table
        + " (V) INCLUDE (C1) WITH (algorithm='IVF', metric='L2', lists=2, sample_size=10)");

      List<String> expected = new ArrayList<>();
      expected.add(SaltingUtil.SALTING_COLUMN_NAME);
      for (String c : new String[] { "TENANT_ID", "ID1", "ID2" }) {
        expected.add(IndexUtil.getIndexColumnName(null, c));
      }
      assertEquals(expected, pkColumnNames(PhoenixRuntime.getTableNoCache(conn, hnsw)));
      assertTrue(pkColumnNames(PhoenixRuntime.getTableNoCache(conn, ivf))
        .contains(MetaDataUtil.VECTOR_CENTROID_ID_COLUMN_NAME));
    }
  }

  /**
   * Tests that HNSW indexes are created in BUILDING state and do not initiate centroid training.
   */
  @Test
  public void testInitialBuildState() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl());
      Statement stmt = conn.createStatement()) {
      stmt.execute(
        "CREATE TABLE " + table + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4))");
      stmt.execute("UPSERT INTO " + table + " VALUES ('r1', ARRAY[1.0, 0.0, 0.0, 0.0])");
      stmt.execute("UPSERT INTO " + table + " VALUES ('r2', ARRAY[0.0, 1.0, 0.0, 0.0])");
      conn.commit();
      stmt.execute("CREATE VECTOR INDEX " + index + " ON " + table
        + " (V) WITH (algorithm='HNSW', metric='COSINE')");

      assertEquals(PIndexState.BUILDING,
        PhoenixRuntime.getTableNoCache(conn, index).getIndexState());
      try (PreparedStatement ps = conn.prepareStatement(
        "SELECT COUNT(*) FROM " + SYSTEM_VECTOR_CENTROID_NAME + " WHERE " + INDEX_NAME + " = ?")) {
        ps.setString(1, index);
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(0L, rs.getLong(1));
        }
      }
    }
  }

  @Test
  public void testParameterValidation() throws Exception {
    String table = generateUniqueName();
    String doubleTable = generateUniqueName();
    // Test cases for invalid WITH clause parameter combinations
    Object[][] cases = { { "M=2", SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS },
      { "M=100", SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS },
      { "M='sixteen'", SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS },
      { "ef_construction=8", SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS },
      { "ef_construction=1000", SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS },
      { "alpha=0.5", SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS },
      { "alpha=3.0", SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS },
      { "alpha='wide'", SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS },
      { "quantization='INT4'", SQLExceptionCode.UNSUPPORTED_VECTOR_QUANTIZATION_TYPE },
      { "quantization='PQ'", SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS },
      { "quantization='PQ', pq_segments=5",
        SQLExceptionCode.VECTOR_QUANTIZATION_DIMENSION_MISMATCH },
      { "quantization='PQ', pq_segments=300", SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS },
      { "quantization='SQ8', pq_segments=2", SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS },
      { "pq_segments=2", SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS },
      { "lists=16", SQLExceptionCode.VECTOR_ALGORITHM_PARAM_MISMATCH },
      { "CONSISTENCY=EVENTUAL", SQLExceptionCode.HNSW_EVENTUAL_CONSISTENCY_NOT_SUPPORTED },
      { "algorithm='IVF', metric='L2', lists=2, sample_size=10, M=16",
        SQLExceptionCode.VECTOR_ALGORITHM_PARAM_MISMATCH },
      { "algorithm='DISKANN', metric='L2'", SQLExceptionCode.UNSUPPORTED_VECTOR_INDEX_ALGORITHM },
      { "algorithm='HNSW', metric='MANHATTAN'",
        SQLExceptionCode.UNSUPPORTED_VECTOR_DISTANCE_METRIC } };
    try (Connection conn = DriverManager.getConnection(getUrl());
      Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE " + table
        + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 12), C1 VARCHAR)");
      stmt.execute(
        "CREATE TABLE " + doubleTable + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(DOUBLE, 12))");
      for (Object[] c : cases) {
        String with = (String) c[0];
        if (!with.startsWith("algorithm")) {
          with = "algorithm='HNSW', metric='COSINE', " + with;
        }
        assertRejected(stmt, "CREATE VECTOR INDEX " + generateUniqueName() + " ON " + table
          + " (V) WITH (" + with + ")", (SQLExceptionCode) c[1]);
      }
      assertRejected(stmt,
        "CREATE VECTOR INDEX " + generateUniqueName() + " ON " + table
          + " (V) INCLUDE (C1) WITH (algorithm='HNSW', metric='COSINE')",
        SQLExceptionCode.HNSW_INCLUDE_NOT_SUPPORTED);
      assertRejected(stmt,
        "CREATE VECTOR INDEX " + generateUniqueName() + " ON " + doubleTable
          + " (V) WITH (algorithm='HNSW', metric='COSINE', quantization='SQ8')",
        SQLExceptionCode.UNSUPPORTED_VECTOR_QUANTIZATION_TYPE);
      String rowTimestamp = generateUniqueName();
      stmt.execute("CREATE TABLE " + rowTimestamp + " (ID VARCHAR NOT NULL, TS DATE NOT NULL,"
        + " V VECTOR(FLOAT, 4) CONSTRAINT PK PRIMARY KEY (ID, TS ROW_TIMESTAMP))"
        + " IMMUTABLE_ROWS=true");
      assertRejected(stmt,
        "CREATE VECTOR INDEX " + generateUniqueName() + " ON " + rowTimestamp
          + " (V) WITH (algorithm='HNSW', metric='COSINE')",
        SQLExceptionCode.HNSW_ROW_TIMESTAMP_NOT_SUPPORTED);
      stmt.execute("CREATE VECTOR INDEX " + generateUniqueName() + " ON " + rowTimestamp
        + " (V) WITH (algorithm='IVF', metric='L2', lists=2, sample_size=10)");
    }
  }

  /** Tests that modifying an existing HNSW index to use eventual consistency is rejected. */
  @Test
  public void testAlterToEventualConsistencyRejected() throws Exception {
    String table = generateUniqueName();
    String hnsw = generateUniqueName();
    String ivf = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl());
      Statement stmt = conn.createStatement()) {
      stmt.execute(
        "CREATE TABLE " + table + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4))");
      stmt.execute("CREATE VECTOR INDEX " + hnsw + " ON " + table
        + " (V) WITH (algorithm='HNSW', metric='COSINE')");
      stmt.execute("CREATE VECTOR INDEX " + ivf + " ON " + table
        + " (V) WITH (algorithm='IVF', metric='L2', lists=2, sample_size=10)");
      assertRejected(stmt, "ALTER INDEX " + hnsw + " ON " + table + " CONSISTENCY=EVENTUAL",
        SQLExceptionCode.HNSW_EVENTUAL_CONSISTENCY_NOT_SUPPORTED);
      stmt.execute("ALTER INDEX " + ivf + " ON " + table + " CONSISTENCY=EVENTUAL");
    }
  }

  private static void assertRejected(Statement stmt, String ddl, SQLExceptionCode expected) {
    try {
      stmt.execute(ddl);
      fail("Expected " + expected + " for: " + ddl);
    } catch (SQLException e) {
      assertEquals(ddl, expected.getErrorCode(), e.getErrorCode());
    }
  }

  /**
   * Tests persistence and normalization of explicit and default HNSW parameters in SYSTEM.CATALOG.
   */
  @Test
  public void testCatalogPersistence() throws Exception {
    String table = generateUniqueName();
    String explicit = generateUniqueName();
    String defaulted = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl());
      Statement stmt = conn.createStatement()) {
      stmt.execute(
        "CREATE TABLE " + table + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 8))");
      stmt.execute("CREATE VECTOR INDEX " + explicit + " ON " + table
        + " (V) WITH (algorithm='hnsw', metric='cosine', M=32, ef_construction=64, alpha=1.5, "
        + "quantization='pq', pq_segments=2)");
      stmt.execute("CREATE VECTOR INDEX " + defaulted + " ON " + table
        + " (V) WITH (algorithm='HNSW', metric='L2')");

      PTable explicitIndex = PhoenixRuntime.getTableNoCache(conn, explicit);
      assertEquals(VectorIndexType.HNSW, explicitIndex.getVectorIndexType());
      assertNull(explicitIndex.getVectorCentroidGeneration());
      PTable.VectorIndex vi = explicitIndex.getVectorIndex();
      assertEquals("HNSW", vi.getAlgorithm());
      assertEquals("COSINE", vi.getDistanceMetric());
      assertEquals(Integer.valueOf(8), vi.getDimension());
      assertEquals(Integer.valueOf(32), vi.getHnswM());
      assertEquals(Integer.valueOf(64), vi.getHnswEfConstruction());
      assertEquals(Double.valueOf(1.5), vi.getHnswAlpha());
      assertEquals("PQ", vi.getQuantizationType());
      assertEquals(Integer.valueOf(2), vi.getPqSegments());
      assertNull(vi.getIvfLists());

      vi = PhoenixRuntime.getTableNoCache(conn, defaulted).getVectorIndex();
      assertEquals("L2", vi.getDistanceMetric());
      assertEquals(Integer.valueOf(16), vi.getHnswM());
      assertEquals(Integer.valueOf(100), vi.getHnswEfConstruction());
      assertEquals(Double.valueOf(1.2), vi.getHnswAlpha());
      assertEquals("NONE", vi.getQuantizationType());
      assertNull(vi.getPqSegments());
    }
  }

  /** Tests that column families on HNSW index tables are configured with MOB storage enabled. */
  @Test
  public void testIndexTableMobConfiguration() throws Exception {
    String table = generateUniqueName();
    String hnsw = generateUniqueName();
    String ivf = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl());
      Statement stmt = conn.createStatement()) {
      stmt.execute(
        "CREATE TABLE " + table + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4))");
      stmt.execute("CREATE VECTOR INDEX " + hnsw + " ON " + table
        + " (V) WITH (algorithm='HNSW', metric='COSINE')");
      stmt.execute("CREATE VECTOR INDEX " + ivf + " ON " + table
        + " (V) WITH (algorithm='IVF', metric='L2', lists=2, sample_size=10)");
      try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
        for (ColumnFamilyDescriptor cf : admin.getDescriptor(TableName.valueOf(hnsw))
          .getColumnFamilies()) {
          assertTrue(cf.isMobEnabled());
          assertEquals(1024, cf.getMobThreshold());
        }
        for (ColumnFamilyDescriptor cf : admin.getDescriptor(TableName.valueOf(ivf))
          .getColumnFamilies()) {
          assertFalse(cf.isMobEnabled());
        }
      }
    }
  }
}
