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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.phoenix.coprocessor.MetaDataEndpointImpl;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.execute.HnswScanPlan;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.CreateIndexStatement;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.LiteralTTLExpression;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.VectorIndexType;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.CostUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
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

  /** Compiles a CREATE VECTOR INDEX statement and returns its error, or null if it compiles. */
  private static SQLException compileError(PhoenixStatement stmt, String ddl) {
    try {
      stmt.compileMutation(ddl);
      return null;
    } catch (SQLException e) {
      return e;
    }
  }

  private static void assertError(SQLExceptionCode expected, SQLException e, String ddl) {
    assertNotNull("Expected " + expected + " for: " + ddl, e);
    assertEquals(ddl, expected.getErrorCode(), e.getErrorCode());
  }

  /**
   * JVector DOT_PRODUCT gives correct results only for unit vectors. Thus CREATE VECTOR INDEX
   * rejects INNER_PRODUCT for HNSW and accepts L2 and COSINE. IVF still supports INNER_PRODUCT.
   */
  @Test
  public void testHnswInnerProductRejection() throws Exception {
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))
        .unwrap(PhoenixConnection.class)) {
      PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
      String ddl =
        "CREATE VECTOR INDEX idx ON T_VEC (V1) WITH (algorithm='HNSW', metric='inner_product')";
      assertError(SQLExceptionCode.UNSUPPORTED_VECTOR_DISTANCE_METRIC, compileError(stmt, ddl),
        ddl);
      for (String metric : new String[] { "L2", "COSINE" }) {
        ddl =
          "CREATE VECTOR INDEX idx ON T_VEC (V1) WITH (algorithm='HNSW', metric='" + metric + "')";
        assertNull(ddl, compileError(stmt, ddl));
      }
    }
  }

  /**
   * A LISTS or SAMPLE_SIZE value that is not a number is still an option that the user set. HNSW
   * rejects it as an IVF option. IVF rejects it because it is not an integer.
   */
  @Test
  public void testNonNumericIvfOptions() throws Exception {
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))
        .unwrap(PhoenixConnection.class)) {
      PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
      for (String option : new String[] { "lists='16x'", "sample_size='many'" }) {
        String ddl = "CREATE VECTOR INDEX idx ON T_VEC (V1) WITH (algorithm='HNSW', metric='L2', "
          + option + ")";
        assertError(SQLExceptionCode.VECTOR_ALGORITHM_PARAM_MISMATCH, compileError(stmt, ddl), ddl);
      }
      String ddl = "CREATE VECTOR INDEX idx ON T_VEC (V1) WITH (algorithm='IVF', metric='L2', "
        + "lists='16x', sample_size=500)";
      SQLException e = compileError(stmt, ddl);
      assertError(SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS, e, ddl);
      assertTrue(e.getMessage(), e.getMessage().contains("LISTS must be an integer"));
    }
  }

  /**
   * PQ_SEGMENTS is valid only with PQ quantization, and PQ quantization requires PQ_SEGMENTS. Each
   * error message names the missing part.
   */
  @Test
  public void testHnswPqSegmentsWithoutPq() throws Exception {
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))
        .unwrap(PhoenixConnection.class)) {
      PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
      String ddl = "CREATE VECTOR INDEX idx ON T_VEC (V1) WITH (algorithm='HNSW', metric='L2', "
        + "quantization='SQ8', pq_segments=2)";
      SQLException e = compileError(stmt, ddl);
      assertError(SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS, e, ddl);
      assertTrue(e.getMessage(), e.getMessage().contains("valid only when QUANTIZATION is 'PQ'"));
      ddl = "CREATE VECTOR INDEX idx ON T_VEC (V1) WITH (algorithm='HNSW', metric='L2', "
        + "quantization='PQ')";
      e = compileError(stmt, ddl);
      assertError(SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS, e, ddl);
      assertTrue(e.getMessage(), e.getMessage().contains("must be specified"));
    }
  }

  /** Every codec builds the graph from float32 vectors, so every codec accepts DOUBLE vectors. */
  @Test
  public void testHnswCodecsAcceptDoubleVectors() throws Exception {
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))
        .unwrap(PhoenixConnection.class)) {
      conn.createStatement()
        .execute("CREATE TABLE T_VEC_DBL (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(DOUBLE, 16))");
      PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
      for (String codec : new String[] { "quantization='NONE'", "quantization='SQ8'",
        "quantization='PQ', pq_segments=4" }) {
        String ddl = "CREATE VECTOR INDEX idx ON T_VEC_DBL (V) WITH (algorithm='HNSW', "
          + "metric='L2', " + codec + ")";
        assertNull(ddl, compileError(stmt, ddl));
      }
    }
  }

  /**
   * The server checks the HNSW parameters of the catalog metadata with the client rules. Thus a
   * missing or out of range value never reaches a region.
   */
  @Test
  public void testValidateHnswIndex() throws Exception {
    CreateIndexCompiler.validateHnswIndex(hnsw().build());
    CreateIndexCompiler.validateHnswIndex(hnsw().setDistanceMetric("L2").build());
    CreateIndexCompiler
      .validateHnswIndex(hnsw().setQuantizationType("PQ").setPqSegments(4).build());
    Object[][] cases = {
      { hnsw().setDistanceMetric("INNER_PRODUCT"),
        SQLExceptionCode.UNSUPPORTED_VECTOR_DISTANCE_METRIC },
      { hnsw().setDistanceMetric("cosine"), SQLExceptionCode.UNSUPPORTED_VECTOR_DISTANCE_METRIC },
      { hnsw().setDistanceMetric(null), SQLExceptionCode.UNSUPPORTED_VECTOR_DISTANCE_METRIC },
      { hnsw().setDimension(null), SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS },
      { hnsw().setHnswM(null), SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS },
      { hnsw().setHnswM(65), SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS },
      { hnsw().setHnswEfConstruction(null), SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS },
      { hnsw().setHnswEfConstruction(8), SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS },
      { hnsw().setHnswAlpha(null), SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS },
      { hnsw().setHnswAlpha(Double.NaN), SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS },
      { hnsw().setQuantizationType(null), SQLExceptionCode.UNSUPPORTED_VECTOR_QUANTIZATION_TYPE },
      { hnsw().setQuantizationType("sq8"), SQLExceptionCode.UNSUPPORTED_VECTOR_QUANTIZATION_TYPE },
      { hnsw().setQuantizationType("PQ"), SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS },
      { hnsw().setPqSegments(4), SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS },
      { hnsw().setQuantizationType("PQ").setPqSegments(0),
        SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS },
      { hnsw().setQuantizationType("PQ").setPqSegments(5),
        SQLExceptionCode.VECTOR_QUANTIZATION_DIMENSION_MISMATCH } };
    for (Object[] c : cases) {
      PTable.VectorIndex vi = ((PTable.VectorIndex.Builder) c[0]).build();
      try {
        CreateIndexCompiler.validateHnswIndex(vi);
        fail("Expected " + c[1]);
      } catch (SQLException e) {
        assertEquals(e.getMessage(), ((SQLExceptionCode) c[1]).getErrorCode(), e.getErrorCode());
      }
    }
  }

  private static PTable.VectorIndex.Builder hnsw() {
    return new PTable.VectorIndex.Builder().setAlgorithm("HNSW").setDistanceMetric("COSINE")
      .setDimension(16).setHnswM(16).setHnswEfConstruction(100).setHnswAlpha(1.2)
      .setQuantizationType("NONE");
  }

  /**
   * Tests the cost of an HNSW plan. Each region scan of the plan reads its own candidates. Thus the
   * cost counts the candidates of each scan, but not more than the rows of the scanned range.
   * Without statistics, the cost is unknown. Thus the cost-based optimizer does not prefer the plan
   * over a plan with a known cost.
   */
  @Test
  public void testHnswPlanCost() throws Exception {
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))
        .unwrap(PhoenixConnection.class)) {
      conn.createStatement()
        .execute("CREATE TABLE T_VEC_COST (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 2))");
      conn.createStatement().execute("CREATE VECTOR INDEX I_VEC_COST ON T_VEC_COST (V) "
        + "WITH (algorithm='HNSW', metric='COSINE')");
      // A new HNSW index stays in BUILDING state until IndexTool builds its segments
      conn.createStatement().execute("ALTER INDEX I_VEC_COST ON T_VEC_COST ACTIVE");
      String query = "SELECT /*+ INDEX(T_VEC_COST I_VEC_COST) */ ID FROM T_VEC_COST%s "
        + "ORDER BY COSINE_DISTANCE(V, ?) LIMIT 10";
      for (String where : new String[] { "", " WHERE ID IN ('a', 'b')" }) {
        PhoenixPreparedStatement ps =
          conn.prepareStatement(String.format(query, where)).unwrap(PhoenixPreparedStatement.class);
        ps.setArray(1, conn.createArrayOf("FLOAT", new Float[] { 1f, 0f }));
        QueryPlan plan = ps.optimizeQuery();
        assertTrue(plan.getClass().getName(), plan instanceof HnswScanPlan);
        if (where.isEmpty()) {
          assertTrue(plan.getCost().isUnknown());
        } else {
          // The cost is at most the rows of the scanned range. Two point lookups read two rows,
          // which is fewer than the candidate count
          assertEquals(CostUtil.estimateLookupCost(2, plan.getTableRef().getTable()),
            plan.getCost());
        }
      }
    }
  }

  /**
   * An HNSW index is a VECTOR_LOCAL index with the shape of a local index. It is not salted when
   * its data table is salted. CREATE VECTOR INDEX rejects an explicit SALT_BUCKETS, as for a local
   * index. The physical table of the index is the data table, and its default family is V#. An IVF
   * index still follows the salting of the data table.
   */
  @Test
  public void testHnswIndexIsNotSalted() throws Exception {
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))
        .unwrap(PhoenixConnection.class)) {
      conn.createStatement().execute("CREATE TABLE T_VEC_SALT (ID VARCHAR NOT NULL PRIMARY KEY, "
        + "V VECTOR(FLOAT, 4), C VARCHAR) SALT_BUCKETS=4");
      String ddl = "CREATE VECTOR INDEX I_VEC_SALT_X ON T_VEC_SALT (V) "
        + "WITH (algorithm='HNSW', metric='L2', SALT_BUCKETS=4)";
      try {
        conn.createStatement().execute(ddl);
        fail("Expected " + ddl + " to be rejected");
      } catch (SQLException e) {
        assertError(SQLExceptionCode.CANNOT_SALT_LOCAL_INDEX, e, ddl);
      }
      conn.createStatement().execute("CREATE VECTOR INDEX I_VEC_SALT ON T_VEC_SALT (V) "
        + "WITH (algorithm='HNSW', metric='L2')");
      PTable hnsw = conn.getTable("I_VEC_SALT");
      assertNull(hnsw.getBucketNum());
      assertEquals("T_VEC_SALT", hnsw.getPhysicalName().getString());
      assertEquals(QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY,
        hnsw.getDefaultFamilyName().getString());
      assertNull(hnsw.getViewIndexId());
      conn.createStatement().execute("CREATE VECTOR INDEX I_VEC_SALT_IVF ON T_VEC_SALT (V) "
        + "WITH (algorithm='IVF', metric='L2', lists=2, sample_size=4) ASYNC");
      assertEquals(Integer.valueOf(4), conn.getTable("I_VEC_SALT_IVF").getBucketNum());
    }
  }

  /**
   * An HNSW index does not inherit the TTL of its table. A conditional TTL on a column that the
   * index does not hold must not block CREATE VECTOR INDEX. It must also not block a later ALTER
   * TABLE SET TTL. The client caches the index without a TTL.
   */
  @Test
  public void testHnswIndexIgnoresConditionalTtl() throws Exception {
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))
        .unwrap(PhoenixConnection.class)) {
      conn.createStatement().execute("CREATE TABLE T_VEC_CTTL (ID VARCHAR NOT NULL PRIMARY KEY, "
        + "V VECTOR(FLOAT, 4), C VARCHAR, D VARCHAR) TTL = 'C = ''x'''");
      // An IVF index is a covered index, so it must hold the columns of the conditional TTL
      try {
        conn.createStatement().execute("CREATE VECTOR INDEX I_VEC_CTTL_IVF ON T_VEC_CTTL (V) "
          + "WITH (algorithm='IVF', metric='L2', lists=2, sample_size=4) ASYNC");
        fail("Expected the IVF index to be rejected");
      } catch (SQLException e) {
        assertTrue(e.getMessage(), e.getMessage().contains("refers columns not in"));
      }
      conn.createStatement().execute("CREATE VECTOR INDEX I_VEC_CTTL ON T_VEC_CTTL (V) "
        + "WITH (algorithm='HNSW', metric='L2')");
      assertEquals(LiteralTTLExpression.TTL_EXPRESSION_NOT_DEFINED,
        conn.getTable("I_VEC_CTTL").getTTLExpression());
      // The TTL check on ALTER resolves the columns of a vector index through its data table
      conn.createStatement().execute("ALTER TABLE T_VEC_CTTL SET TTL = 'D = ''y'''");
    }
  }

  /**
   * The catalog values are the enum names from the validated vector index, and the name match uses
   * Locale.ROOT. Thus a JVM default locale that maps 'i' to a dotted capital I does not change the
   * stored algorithm or metric.
   */
  @Test
  public void testCatalogValuesIgnoreDefaultLocale() throws Exception {
    Locale defaultLocale = Locale.getDefault();
    Locale.setDefault(new Locale("tr", "TR"));
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))
        .unwrap(PhoenixConnection.class)) {
      conn.createStatement()
        .execute("CREATE TABLE T_VEC_TR (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4))");
      conn.createStatement().execute("CREATE VECTOR INDEX I_VEC_TR ON T_VEC_TR (V) WITH "
        + "(algorithm='ivf', metric='cosine', lists=2, sample_size=4) ASYNC");
      PTable index = conn.getTable("I_VEC_TR");
      assertEquals("IVF", index.getVectorIndexAlgorithm());
      assertEquals(VectorIndexType.IVF, index.getVectorIndexType());
      assertEquals("COSINE", index.getVectorDistanceMetric());
      assertEquals(Integer.valueOf(2), index.getVectorIvfLists());
      assertEquals(Integer.valueOf(4), index.getVectorIvfSampleSize());
    } finally {
      Locale.setDefault(defaultLocale);
    }
  }

  /**
   * A region server finds an HNSW index and its data table by full name. It splits a full name at
   * the first separator. It also removes a view prefix from an index name, up to the '#' character.
   * CREATE VECTOR INDEX rejects HNSW names that do not split back into the same parts.
   */
  @Test
  public void testHnswRejectsNamesThatDoNotResolve() throws Exception {
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))
        .unwrap(PhoenixConnection.class)) {
      conn.createStatement()
        .execute("CREATE TABLE \"emb.v1\" (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4))");
      conn.createStatement().execute(
        "CREATE TABLE S_HNSW.\"t.1\" (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4))");
      for (String ddl : new String[] {
        "CREATE VECTOR INDEX I_EMB ON \"emb.v1\" (V) WITH (algorithm='HNSW', metric='L2')",
        "CREATE VECTOR INDEX \"i.x\" ON T_VEC (V1) WITH (algorithm='HNSW', metric='L2')",
        "CREATE VECTOR INDEX \"i#x\" ON T_VEC (V1) WITH (algorithm='HNSW', metric='L2')" }) {
        try {
          conn.createStatement().execute(ddl);
          fail("Expected " + ddl + " to be rejected");
        } catch (SQLException e) {
          assertEquals(ddl, SQLExceptionCode.INVALID_VECTOR_INDEX_PARAMS.getErrorCode(),
            e.getErrorCode());
        }
      }
      // A table name in a schema can contain '.'
      conn.createStatement().execute(
        "CREATE VECTOR INDEX I_T1 ON S_HNSW.\"t.1\" (V) WITH (algorithm='HNSW', metric='L2')");
      assertNotNull(conn.getTable("S_HNSW.I_T1"));
    }
  }

  /**
   * The algorithm selects the vector index type once, when the statement takes its options. HNSW
   * gives VECTOR_LOCAL. IVF, an unknown algorithm and a missing algorithm stay VECTOR_GLOBAL.
   * VECTOR_LOCAL is never a LOCAL index.
   */
  @Test
  public void testIndexTypeFromAlgorithm() throws Exception {
    String[][] cases = { { "algorithm='HNSW', metric='L2'", "VECTOR_LOCAL" },
      { "algorithm=' hnsw ', metric='L2'", "VECTOR_LOCAL" },
      { "algorithm='IVF', metric='L2', lists=2, sample_size=4", "VECTOR_GLOBAL" },
      { "algorithm='DISKANN', metric='L2'", "VECTOR_GLOBAL" }, { "metric='L2'", "VECTOR_GLOBAL" } };
    for (String[] c : cases) {
      CreateIndexStatement create = (CreateIndexStatement) new SQLParser(
        "CREATE VECTOR INDEX idx ON T_VEC (V1) WITH (" + c[0] + ")").parseStatement();
      assertEquals(c[0], IndexType.valueOf(c[1]), create.getIndexType());
    }
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))
        .unwrap(PhoenixConnection.class)) {
      conn.createStatement()
        .execute("CREATE TABLE T_VEC_TYPE (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4))");
      conn.createStatement().execute("CREATE VECTOR INDEX I_VEC_TYPE_HNSW ON T_VEC_TYPE (V) "
        + "WITH (algorithm='HNSW', metric='L2')");
      conn.createStatement().execute("CREATE VECTOR INDEX I_VEC_TYPE_IVF ON T_VEC_TYPE (V) "
        + "WITH (algorithm='IVF', metric='L2', lists=2, sample_size=4) ASYNC");
      PTable hnsw = conn.getTable("I_VEC_TYPE_HNSW");
      assertEquals(IndexType.VECTOR_LOCAL, hnsw.getIndexType());
      assertTrue(hnsw.isVectorIndex());
      assertEquals(VectorIndexType.HNSW, hnsw.getVectorIndexType());
      PTable ivf = conn.getTable("I_VEC_TYPE_IVF");
      assertEquals(IndexType.VECTOR_GLOBAL, ivf.getIndexType());
      assertEquals(VectorIndexType.IVF, ivf.getVectorIndexType());
      assertEquals("I_VEC_TYPE_IVF", ivf.getPhysicalName().getString());
    }
  }

  /**
   * The server accepts vector metadata only when the algorithm matches the index type. HNSW needs
   * VECTOR_LOCAL, and IVF needs VECTOR_GLOBAL.
   */
  @Test
  public void testServerPairsAlgorithmWithIndexType() throws Exception {
    Method validate = MetaDataEndpointImpl.class.getDeclaredMethod("validateVectorIndexMetadata",
      List.class, PTableType.class, IndexType.class);
    validate.setAccessible(true);
    List<Mutation> hnsw = header("HNSW", "L2", 16, null, null, 16, 100, 1.2, "NONE");
    List<Mutation> ivf = header("IVF", "L2", 16, 2, 4, null, null, null, null);
    validate.invoke(null, hnsw, PTableType.INDEX, IndexType.VECTOR_LOCAL);
    validate.invoke(null, ivf, PTableType.INDEX, IndexType.VECTOR_GLOBAL);
    Object[][] mismatches = { { hnsw, IndexType.VECTOR_GLOBAL }, { ivf, IndexType.VECTOR_LOCAL },
      { hnsw, IndexType.LOCAL }, { ivf, IndexType.GLOBAL } };
    for (Object[] m : mismatches) {
      try {
        validate.invoke(null, m[0], PTableType.INDEX, m[1]);
        fail("Expected a rejection for " + m[1]);
      } catch (InvocationTargetException e) {
        assertTrue(String.valueOf(e.getCause()), e.getCause() instanceof SQLException);
      }
    }
  }

  // Returns the table header row of a vector index with the specified catalog values. A null value
  // adds no cell.
  private static List<Mutation> header(String algorithm, String metric, Integer dimension,
    Integer lists, Integer sampleSize, Integer m, Integer efConstruction, Double alpha,
    String quantization) {
    Put put = new Put(SchemaUtil.getTableKey(null, "S", "I"));
    addValue(put, PhoenixDatabaseMetaData.VECTOR_INDEX_ALGORITHM_BYTES,
      PVarchar.INSTANCE.toBytes(algorithm));
    addValue(put, PhoenixDatabaseMetaData.VECTOR_DISTANCE_METRIC_BYTES,
      PVarchar.INSTANCE.toBytes(metric));
    addValue(put, PhoenixDatabaseMetaData.VECTOR_DIMENSION_BYTES,
      dimension == null ? null : PInteger.INSTANCE.toBytes(dimension));
    addValue(put, PhoenixDatabaseMetaData.VECTOR_IVF_LISTS_BYTES,
      lists == null ? null : PInteger.INSTANCE.toBytes(lists));
    addValue(put, PhoenixDatabaseMetaData.VECTOR_IVF_SAMPLE_SIZE_BYTES,
      sampleSize == null ? null : PInteger.INSTANCE.toBytes(sampleSize));
    addValue(put, PhoenixDatabaseMetaData.VECTOR_HNSW_M_BYTES,
      m == null ? null : PInteger.INSTANCE.toBytes(m));
    addValue(put, PhoenixDatabaseMetaData.VECTOR_HNSW_EF_CONSTRUCTION_BYTES,
      efConstruction == null ? null : PInteger.INSTANCE.toBytes(efConstruction));
    addValue(put, PhoenixDatabaseMetaData.VECTOR_HNSW_ALPHA_BYTES,
      alpha == null ? null : PDouble.INSTANCE.toBytes(alpha));
    addValue(put, PhoenixDatabaseMetaData.VECTOR_QUANTIZATION_TYPE_BYTES,
      quantization == null ? null : PVarchar.INSTANCE.toBytes(quantization));
    return Collections.<Mutation> singletonList(put);
  }

  private static void addValue(Put put, byte[] qualifier, byte[] value) {
    if (value != null) {
      put.addColumn(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES, qualifier, value);
    }
  }

  /**
   * The hidden family name of VECTOR_LOCAL indexes is reserved, as the local index prefix is.
   * CREATE TABLE and ALTER TABLE ADD reject a column family that contains it. CREATE TABLE also
   * rejects a DEFAULT_COLUMN_FAMILY that contains it.
   */
  @Test
  public void testHiddenFamilyNameRejection() throws Exception {
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))
        .unwrap(PhoenixConnection.class)) {
      conn.createStatement()
        .execute("CREATE TABLE T_VEC_RESERVED_ALTER (ID VARCHAR NOT NULL PRIMARY KEY, C VARCHAR)");
      for (String family : new String[] { "\"V#\"", "\"AV#\"", "\"V#0\"" }) {
        String[] ddls = {
          "CREATE TABLE T_VEC_RESERVED (ID VARCHAR NOT NULL PRIMARY KEY, " + family + ".C VARCHAR)",
          "CREATE TABLE T_VEC_RESERVED (ID VARCHAR NOT NULL PRIMARY KEY, C VARCHAR) "
            + "DEFAULT_COLUMN_FAMILY='" + family.replace("\"", "") + "'",
          "ALTER TABLE T_VEC_RESERVED_ALTER ADD " + family + ".X INTEGER" };
        for (String ddl : ddls) {
          try {
            conn.createStatement().execute(ddl);
            fail("Expected " + ddl + " to be rejected");
          } catch (SQLException e) {
            assertError(SQLExceptionCode.UNALLOWED_COLUMN_FAMILY, e, ddl);
          }
        }
      }
      conn.createStatement().execute("ALTER TABLE T_VEC_RESERVED_ALTER ADD \"V\".X INTEGER");
    }
  }

  /**
   * An HNSW index shares the HBase table of its data table. Like a local index, it accepts no HBase
   * table properties, because they would change the data table. An IVF index has its own index
   * table, which accepts them.
   */
  @Test
  public void testHnswRejectsHBaseTableProperties() throws Exception {
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))
        .unwrap(PhoenixConnection.class)) {
      conn.createStatement().execute(
        "CREATE TABLE T_VEC_HTP (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), C VARCHAR)");
      for (String property : new String[] { "DURABILITY='SKIP_WAL'", "MAX_FILESIZE=1000000",
        "REGION_REPLICATION=2", "COMPACTION_ENABLED=false" }) {
        String ddl = "CREATE VECTOR INDEX I_VEC_HTP ON T_VEC_HTP (V) "
          + "WITH (algorithm='HNSW', metric='L2', " + property + ")";
        try {
          conn.createStatement().execute(ddl);
          fail("Expected " + ddl + " to be rejected");
        } catch (SQLException e) {
          assertError(SQLExceptionCode.VIEW_WITH_PROPERTIES, e, ddl);
        }
      }
      conn.createStatement().execute(
        "CREATE VECTOR INDEX I_VEC_HTP ON T_VEC_HTP (V) WITH (algorithm='HNSW', metric='L2')");
      conn.createStatement().execute("CREATE VECTOR INDEX I_VEC_HTP_IVF ON T_VEC_HTP (V) "
        + "WITH (algorithm='IVF', metric='L2', lists=2, sample_size=4, DURABILITY='SKIP_WAL') ASYNC");
    }
  }
}
