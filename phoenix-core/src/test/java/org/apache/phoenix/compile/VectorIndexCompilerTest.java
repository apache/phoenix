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
import static org.junit.Assert.fail;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Compiler tests for vector indexes. Validates compile-time constraint checking in
 * {@link CreateIndexCompiler} including metric validation, algorithm validation, IVF parameter
 * bounds, vector expression count, and dimension resolution.
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

  @Test
  public void testDimensionMismatchRejection() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class)) {
      PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
      String ddl = "CREATE VECTOR INDEX idx ON T_VEC (V1) WITH (algorithm='IVF', metric='L2', "
        + "lists=16, sample_size=500, dimension=256)";
      try {
        stmt.compileMutation(ddl);
        fail("Expected compilation error for dimension mismatch");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.VECTOR_INDEX_DIMENSION_MISMATCH.getErrorCode(),
          e.getErrorCode());
        assertEquals(SQLExceptionCode.VECTOR_INDEX_DIMENSION_MISMATCH.getSQLState(),
          e.getSQLState());
      }
    }
  }

  @Test
  public void testValidVectorIndexCompilation() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class)) {
      PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
      String ddl = "CREATE VECTOR INDEX idx ON T_VEC (V1) INCLUDE (S) WITH (algorithm='IVF', "
        + "metric='COSINE', lists=16, sample_size=500)";
      MutationPlan plan = stmt.compileMutation(ddl);
      assertNotNull("Plan must not be null for valid vector index", plan);
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
}
