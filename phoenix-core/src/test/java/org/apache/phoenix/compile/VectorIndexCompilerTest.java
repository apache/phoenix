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
package org.apache.phoenix.compile;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests the compile-time validation of vector index DDL, mainly in {@link CreateIndexCompiler}.
 */
public class VectorIndexCompilerTest extends BaseConnectionlessQueryTest {

  @BeforeClass
  public static void setupTables() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class)) {
      conn.createStatement()
        .execute("CREATE TABLE T_VEC (ID VARCHAR NOT NULL PRIMARY KEY, V1 VECTOR(FLOAT, 128), "
          + "V2 VECTOR(FLOAT, 128), S VARCHAR)");
    }
  }

  @Test
  public void testInvalidDistanceMetricRejection() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class)) {
      PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
      String ddl = "CREATE VECTOR INDEX idx ON T_VEC (V1) WITH (algorithm='IVF', metric='HAMMING', "
        + "lists=16, sample_size=500)";
      try {
        stmt.compileMutation(ddl);
        fail("Expected compilation error for unsupported metric HAMMING");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.UNSUPPORTED_VECTOR_DISTANCE_METRIC.getErrorCode(),
          e.getErrorCode());
        assertEquals(SQLExceptionCode.UNSUPPORTED_VECTOR_DISTANCE_METRIC.getSQLState(),
          e.getSQLState());
      }
    }
  }

  @Test
  public void testMultipleVectorColumnsRejection() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class)) {
      PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
      String ddl = "CREATE VECTOR INDEX idx ON T_VEC (V1, V2) WITH (algorithm='IVF', metric='L2', "
        + "lists=16, sample_size=500)";
      try {
        stmt.compileMutation(ddl);
        fail("Expected compilation error for multiple vector columns in index key constraint");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS.getErrorCode(), e.getErrorCode());
        assertEquals(SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS.getSQLState(), e.getSQLState());
      }
    }
  }

  @Test
  public void testMultipleColumnsMixedRejection() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class)) {
      PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
      String ddl = "CREATE VECTOR INDEX idx ON T_VEC (V1, S) WITH (algorithm='IVF', metric='L2', "
        + "lists=16, sample_size=500)";
      try {
        stmt.compileMutation(ddl);
        fail("Expected compilation error for multiple key expressions in vector index");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS.getErrorCode(), e.getErrorCode());
        assertEquals(SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS.getSQLState(), e.getSQLState());
      }
    }
  }

  @Test
  public void testMissingRequiredIvfListsRejection() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class)) {
      PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
      String ddl =
        "CREATE VECTOR INDEX idx ON T_VEC (V1) WITH (algorithm='IVF', metric='L2', sample_size=500)";
      try {
        stmt.compileMutation(ddl);
        fail("Expected compilation error for missing lists IVF parameter");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS.getErrorCode(), e.getErrorCode());
        assertEquals(SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS.getSQLState(), e.getSQLState());
      }
    }
  }

  @Test
  public void testMissingRequiredIvfSampleSizeRejection() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class)) {
      PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
      String ddl =
        "CREATE VECTOR INDEX idx ON T_VEC (V1) WITH (algorithm='IVF', metric='L2', lists=16)";
      try {
        stmt.compileMutation(ddl);
        fail("Expected compilation error for missing sample_size IVF parameter");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS.getErrorCode(), e.getErrorCode());
        assertEquals(SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS.getSQLState(), e.getSQLState());
      }
    }
  }

  @Test
  public void testMissingBothRequiredIvfParamsRejection() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class)) {
      PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
      String ddl = "CREATE VECTOR INDEX idx ON T_VEC (V1) WITH (algorithm='IVF', metric='L2')";
      try {
        stmt.compileMutation(ddl);
        fail("Expected compilation error for missing required IVF parameters");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS.getErrorCode(), e.getErrorCode());
        assertEquals(SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS.getSQLState(), e.getSQLState());
      }
    }
  }

  @Test
  public void testInvalidIvfZeroListsRejection() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class)) {
      PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
      String ddl = "CREATE VECTOR INDEX idx ON T_VEC (V1) WITH (algorithm='IVF', metric='L2', "
        + "lists=0, sample_size=500)";
      try {
        stmt.compileMutation(ddl);
        fail("Expected compilation error for lists <= 0");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS.getErrorCode(), e.getErrorCode());
        assertEquals(SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS.getSQLState(), e.getSQLState());
      }
    }
  }

  @Test
  public void testInvalidIvfSampleSizeLessThanListsRejection() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class)) {
      PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
      String ddl = "CREATE VECTOR INDEX idx ON T_VEC (V1) WITH (algorithm='IVF', metric='L2', "
        + "lists=32, sample_size=16)";
      try {
        stmt.compileMutation(ddl);
        fail("Expected compilation error for sample_size < lists");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS.getErrorCode(), e.getErrorCode());
        assertEquals(SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS.getSQLState(), e.getSQLState());
      }
    }
  }

  @Test
  public void testInvalidAlgorithmRejection() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class)) {
      PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
      String ddl = "CREATE VECTOR INDEX idx ON T_VEC (V1) WITH (algorithm='UNKNOWN', metric='L2', "
        + "lists=16, sample_size=500)";
      try {
        stmt.compileMutation(ddl);
        fail("Expected compilation error for unknown algorithm");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.UNSUPPORTED_VECTOR_INDEX_ALGORITHM.getErrorCode(),
          e.getErrorCode());
        assertEquals(SQLExceptionCode.UNSUPPORTED_VECTOR_INDEX_ALGORITHM.getSQLState(),
          e.getSQLState());
      }
    }
  }

  @Test
  public void testNonVectorColumnRejection() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class)) {
      PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
      String ddl = "CREATE VECTOR INDEX idx ON T_VEC (S) WITH (algorithm='IVF', metric='L2', "
        + "lists=16, sample_size=500)";
      try {
        stmt.compileMutation(ddl);
        fail("Expected compilation error for non-vector column S");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.VECTOR_INDEX_ON_NON_VECTOR_TYPE.getErrorCode(),
          e.getErrorCode());
        assertEquals(SQLExceptionCode.VECTOR_INDEX_ON_NON_VECTOR_TYPE.getSQLState(),
          e.getSQLState());
      }
    }
  }

  /**
   * Positive control for the rejection tests. Each supported metric compiles to a CREATE INDEX plan
   * on the data table. The option values use mixed case and extra whitespace. The DDL also has an
   * explicit ASC key, a sample size equal to the list count, and STRONG consistency.
   */
  @Test
  public void testValidVectorIndexCompilation() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class)) {
      PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
      for (String metric : new String[] { "l2", " Cosine ", "INNER_PRODUCT" }) {
        String ddl =
          "CREATE VECTOR INDEX idx ON T_VEC (V1 ASC) INCLUDE (S) WITH (algorithm=' ivf ', "
            + "metric='" + metric + "', lists=16, sample_size=16, CONSISTENCY=STRONG)";
        MutationPlan plan = stmt.compileMutation(ddl);
        assertEquals(ddl, "T_VEC", plan.getTargetRef().getTable().getTableName().getString());
        assertEquals(ddl, Collections.singletonList("CREATE INDEX"),
          plan.getExplainPlan().getPlanSteps());
      }
    }
  }

  /**
   * Index maintenance always writes the indexed vector in ascending byte order. A DESC vector key
   * would describe index data that maintenance never writes, so the compiler must reject it.
   */
  @Test
  public void testDescVectorKeyRejection() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class)) {
      PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
      String ddl = "CREATE VECTOR INDEX idx ON T_VEC (V1 DESC) WITH (algorithm='IVF', metric='L2', "
        + "lists=16, sample_size=500)";
      SQLException rejection = null;
      try {
        stmt.compileMutation(ddl);
      } catch (SQLException e) {
        rejection = e;
      }
      assertNotNull("Expected compilation error for a DESC vector index key", rejection);
      assertEquals(SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS.getErrorCode(),
        rejection.getErrorCode());
      assertEquals(SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS.getSQLState(),
        rejection.getSQLState());
    }
  }

  /**
   * The eventually consistent index consumer does not maintain vector indexes. Thus CREATE and
   * ALTER must both reject eventual consistency for a vector index.
   */
  @Test
  public void testEventualConsistencyRejection() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class)) {
      PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
      try {
        stmt.compileMutation("CREATE VECTOR INDEX idx ON T_VEC (V1) WITH (algorithm='IVF', "
          + "metric='L2', lists=16, sample_size=500, CONSISTENCY=EVENTUAL)");
        fail("Expected compilation error for an eventually consistent vector index");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS.getErrorCode(), e.getErrorCode());
        assertEquals(SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS.getSQLState(), e.getSQLState());
      }

      conn.createStatement()
        .execute("CREATE TABLE T_VEC_EC (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4))");
      conn.createStatement().execute("CREATE VECTOR INDEX I_VEC_EC ON T_VEC_EC (V) WITH "
        + "(algorithm='IVF', metric='L2', lists=2, sample_size=4) ASYNC");
      try {
        conn.createStatement().execute("ALTER INDEX I_VEC_EC ON T_VEC_EC CONSISTENCY=EVENTUAL");
        fail("Expected ALTER INDEX to refuse eventual consistency for a vector index");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS.getErrorCode(), e.getErrorCode());
        assertEquals(SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS.getSQLState(), e.getSQLState());
      }
    }
  }

  @Test
  public void testDdlExecutionInvalidDistanceMetricRejection() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class)) {
      try (Statement stmt = conn.createStatement()) {
        stmt
          .execute("CREATE VECTOR INDEX idx ON T_VEC (V1) WITH (algorithm='IVF', metric='HAMMING', "
            + "lists=16, sample_size=500)");
        fail("Expected compilation error for unsupported metric HAMMING during execute()");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.UNSUPPORTED_VECTOR_DISTANCE_METRIC.getErrorCode(),
          e.getErrorCode());
        assertEquals(SQLExceptionCode.UNSUPPORTED_VECTOR_DISTANCE_METRIC.getSQLState(),
          e.getSQLState());
      }
    }
  }

  @Test
  public void testDdlExecutionMultipleVectorColumnsRejection() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class)) {
      try (Statement stmt = conn.createStatement()) {
        stmt
          .execute("CREATE VECTOR INDEX idx ON T_VEC (V1, V2) WITH (algorithm='IVF', metric='L2', "
            + "lists=16, sample_size=500)");
        fail("Expected compilation error for multiple vector columns during execute()");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS.getErrorCode(), e.getErrorCode());
        assertEquals(SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS.getSQLState(), e.getSQLState());
      }
    }
  }

  @Test
  public void testDdlExecutionMissingRequiredParametersRejection() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class)) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE VECTOR INDEX idx ON T_VEC (V1) WITH (algorithm='IVF', metric='L2')");
        fail("Expected compilation error for missing required IVF parameters during execute()");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS.getErrorCode(), e.getErrorCode());
        assertEquals(SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS.getSQLState(), e.getSQLState());
      }
    }
  }

  @Test
  public void testVectorIndexOnViewRejection() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class)) {
      conn.createStatement().execute("CREATE VIEW V_VEC AS SELECT * FROM T_VEC WHERE S = 'a'");
      PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
      String ddl = "CREATE VECTOR INDEX idx ON V_VEC (V1) WITH (algorithm='IVF', metric='L2', "
        + "lists=16, sample_size=500)";
      try {
        stmt.compileMutation(ddl);
        fail("Expected compilation error for vector index on a view");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS.getErrorCode(), e.getErrorCode());
        assertEquals(SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS.getSQLState(), e.getSQLState());
      }
    }
  }

  @Test
  public void testDdlExecutionVectorIndexOnViewRejection() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class)) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE VECTOR INDEX idx ON V_VEC (V1) WITH (algorithm='IVF', metric='L2', "
          + "lists=16, sample_size=500)");
        fail("Expected compilation error for vector index on a view during execute()");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS.getErrorCode(), e.getErrorCode());
        assertEquals(SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS.getSQLState(), e.getSQLState());
      }
    }
  }

  @Test
  public void testDdlExecutionNonVectorColumnRejection() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class)) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE VECTOR INDEX idx ON T_VEC (S) WITH (algorithm='IVF', metric='L2', "
          + "lists=16, sample_size=500)");
        fail("Expected compilation error for non-vector column during execute()");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.VECTOR_INDEX_ON_NON_VECTOR_TYPE.getErrorCode(),
          e.getErrorCode());
        assertEquals(SQLExceptionCode.VECTOR_INDEX_ON_NON_VECTOR_TYPE.getSQLState(),
          e.getSQLState());
      }
    }
  }

  private static final String SPLIT_OPTIONS =
    " WITH (algorithm='IVF', metric='L2', lists=8, sample_size=100";

  private static List<HRegionLocation> regions(PhoenixConnection conn, String index)
    throws SQLException {
    return conn.getQueryServices().getAllTableRegions(Bytes.toBytes(index), 60000);
  }

  private static void assertVectorParamsRejected(SQLException e, String messagePart) {
    assertEquals(e.getMessage(), SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS.getErrorCode(),
      e.getErrorCode());
    assertEquals(e.getMessage(), SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS.getSQLState(),
      e.getSQLState());
    assertTrue(e.getMessage(), e.getMessage().contains(messagePart));
  }

  /**
   * SPLIT ON values are centroid IDs. An unsalted index key starts with the centroid ID, so each
   * split key starts with the INTEGER encoding of its ID. A split at 4 puts every row of list 3 in
   * one region and every row of list 4 in the next region.
   */
  @Test
  public void testSplitOnCentroidIdsMakesRegionsAtListBoundaries() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class)) {
      conn.createStatement()
        .execute("CREATE TABLE T_VEC_SPLIT (ID INTEGER NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4))");
      conn.createStatement().execute("CREATE VECTOR INDEX I_VEC_SPLIT ON T_VEC_SPLIT (V)"
        + SPLIT_OPTIONS + ") ASYNC SPLIT ON (2, 4, 6)");

      PTable index = conn.getTable("I_VEC_SPLIT");
      PColumn first = index.getPKColumns().get(0);
      assertEquals(MetaDataUtil.VECTOR_CENTROID_ID_COLUMN_NAME, first.getName().getString());
      assertEquals(PInteger.INSTANCE, first.getDataType());

      List<HRegionLocation> regions = regions(conn, "I_VEC_SPLIT");
      assertEquals(4, regions.size());
      int[] ids = { 2, 4, 6 };
      for (int i = 0; i < ids.length; i++) {
        byte[] boundary = regions.get(i + 1).getRegion().getStartKey();
        assertTrue(Bytes.equals(boundary, regions.get(i).getRegion().getEndKey()));
        // The highest row of the list below the split sorts before the boundary
        byte[] lastBelow = ByteUtil.concat(PInteger.INSTANCE.toBytes(ids[i] - 1),
          PInteger.INSTANCE.toBytes(Integer.MAX_VALUE));
        assertTrue(Bytes.compareTo(lastBelow, boundary) < 0);
        // The lowest row of the list at the split sorts at or after the boundary
        byte[] firstAt = ByteUtil.concat(PInteger.INSTANCE.toBytes(ids[i]),
          PInteger.INSTANCE.toBytes(Integer.MIN_VALUE));
        assertTrue(Bytes.compareTo(firstAt, boundary) >= 0);
      }
    }
  }

  /**
   * SPLIT ON is valid for an unsalted index. On a salted data table, SALT_BUCKETS=0 makes the index
   * unsalted. An explicit salt without SPLIT ON stays valid.
   */
  @Test
  public void testSplitOnAcceptedLayouts() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class)) {
      conn.createStatement().execute("CREATE VECTOR INDEX I_VEC_SPLIT_PLAIN ON T_VEC (V1)"
        + SPLIT_OPTIONS + ") ASYNC SPLIT ON (4)");
      assertEquals(2, regions(conn, "I_VEC_SPLIT_PLAIN").size());

      conn.createStatement().execute("CREATE TABLE T_VEC_SPLIT_SALT (ID VARCHAR NOT NULL "
        + "PRIMARY KEY, V VECTOR(FLOAT, 4)) SALT_BUCKETS=4");
      conn.createStatement()
        .execute("CREATE VECTOR INDEX I_VEC_SPLIT_UNSALTED ON T_VEC_SPLIT_SALT (V)" + SPLIT_OPTIONS
          + ", SALT_BUCKETS=0) ASYNC SPLIT ON (4)");
      assertEquals(null, conn.getTable("I_VEC_SPLIT_UNSALTED").getBucketNum());
      assertEquals(2, regions(conn, "I_VEC_SPLIT_UNSALTED").size());

      conn.createStatement().execute("CREATE VECTOR INDEX I_VEC_SALTED ON T_VEC (V1)"
        + SPLIT_OPTIONS + ", SALT_BUCKETS=4) ASYNC");
      assertEquals(Integer.valueOf(4), conn.getTable("I_VEC_SALTED").getBucketNum());
    }
  }

  /**
   * A SPLIT ON value must be a distinct INTEGER centroid ID of at least 1. IDs start at 0, so a
   * split at 0 or below makes a region that never gets rows.
   */
  @Test
  public void testSplitOnInvalidCentroidIdRejection() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class)) {
      PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
      String[][] cases =
        { { "1.5", "must be INTEGER" }, { "'a'", "must be INTEGER" }, { "NULL", "must be INTEGER" },
          { "3000000000", "must be from 1 to" }, { "0", "must be from 1 to" },
          { "-1", "must be from 1 to" }, { "2, 4, 2", "more than once" } };
      for (String[] c : cases) {
        String ddl =
          "CREATE VECTOR INDEX idx ON T_VEC (V1)" + SPLIT_OPTIONS + ") SPLIT ON (" + c[0] + ")";
        try {
          stmt.compileMutation(ddl);
          fail("Expected compilation error for " + ddl);
        } catch (SQLException e) {
          assertVectorParamsRejected(e, c[1]);
        }
      }
      // The highest INTEGER is a valid centroid ID
      stmt.compileMutation("CREATE VECTOR INDEX idx ON T_VEC (V1)" + SPLIT_OPTIONS
        + ") SPLIT ON (1, " + Integer.MAX_VALUE + ")");
    }
  }

  /**
   * SPLIT ON is rejected if a salt byte or a tenant ID comes before the centroid ID in the index
   * row key. A vector index on a view is always rejected.
   */
  @Test
  public void testSplitOnRejectedLayouts() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class)) {
      conn.createStatement().execute("CREATE TABLE T_VEC_REJ_SALT (ID VARCHAR NOT NULL "
        + "PRIMARY KEY, V VECTOR(FLOAT, 4)) SALT_BUCKETS=4");
      conn.createStatement()
        .execute("CREATE TABLE T_VEC_REJ_MT (TENANT_ID VARCHAR NOT NULL, ID VARCHAR NOT NULL, "
          + "V VECTOR(FLOAT, 4) CONSTRAINT PK PRIMARY KEY (TENANT_ID, ID)) MULTI_TENANT=true");
      conn.createStatement()
        .execute("CREATE VIEW V_VEC_REJ AS SELECT * FROM T_VEC_REJ_SALT WHERE ID > 'a'");
      String[][] cases =
        { { "I_REJ_INHERITED ON T_VEC_REJ_SALT (V)" + SPLIT_OPTIONS + ")", "SALT_BUCKETS=0" },
          { "I_REJ_EXPLICIT ON T_VEC (V1)" + SPLIT_OPTIONS + ", SALT_BUCKETS=4)",
            "SALT_BUCKETS=0" },
          { "I_REJ_MT ON T_VEC_REJ_MT (V)" + SPLIT_OPTIONS + ")", "multi-tenant" },
          { "I_REJ_VIEW ON V_VEC_REJ (V)" + SPLIT_OPTIONS + ")",
            "Vector indexes are not supported on views" } };
      for (String[] c : cases) {
        String ddl = "CREATE VECTOR INDEX " + c[0] + " ASYNC SPLIT ON (4)";
        try {
          conn.createStatement().execute(ddl);
          fail("Expected an error for " + ddl);
        } catch (SQLException e) {
          assertVectorParamsRejected(e, c[1]);
        }
      }
    }
  }
}
