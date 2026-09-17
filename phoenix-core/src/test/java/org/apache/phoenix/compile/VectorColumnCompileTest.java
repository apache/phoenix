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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.junit.BeforeClass;
import org.junit.Test;

/** Tests the compilation and the write-time validation of vector column values. */
public class VectorColumnCompileTest extends BaseConnectionlessQueryTest {

  private static final int DIM = 1024;

  @BeforeClass
  public static void setupTables() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE T_VEC_COMPILE (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, "
          + DIM + "), VD VECTOR(DOUBLE, 3), A FLOAT ARRAY, S VARCHAR)");
    }
  }

  /**
   * No conversion from a vector to an array or to VARCHAR exists. Thus such casts and assignments
   * must fail at compile time, not on each row at execution time.
   */
  @Test
  public void testVectorToArrayOrVarcharFailsAtCompileTime() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
      for (String sql : new String[] { "SELECT CAST(V AS FLOAT ARRAY) FROM T_VEC_COMPILE",
        "SELECT CAST(VD AS DOUBLE ARRAY) FROM T_VEC_COMPILE",
        "SELECT CAST(V AS VARCHAR) FROM T_VEC_COMPILE" }) {
        try {
          stmt.compileQuery(sql);
          fail("Expected type mismatch compiling " + sql);
        } catch (SQLException e) {
          assertEquals(sql, SQLExceptionCode.TYPE_MISMATCH.getErrorCode(), e.getErrorCode());
        }
      }
      for (String sql : new String[] {
        "UPSERT INTO T_VEC_COMPILE (ID, A) SELECT ID, V FROM T_VEC_COMPILE",
        "UPSERT INTO T_VEC_COMPILE (ID, S) SELECT ID, V FROM T_VEC_COMPILE" }) {
        try {
          stmt.compileMutation(sql);
          fail("Expected type mismatch compiling " + sql);
        } catch (SQLException e) {
          assertEquals(sql, SQLExceptionCode.TYPE_MISMATCH.getErrorCode(), e.getErrorCode());
        }
      }
      // The conversion from an array to a vector stays valid.
      stmt.compileMutation("UPSERT INTO T_VEC_COMPILE (ID, V) SELECT ID, A FROM T_VEC_COMPILE");
    }
  }

  /** The size of a vector is the dimension times the element width in bytes, not the dimension. */
  @Test
  public void testEstimatedRowSizeCountsVectorBytes() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
      for (String sql : new String[] { "SELECT V FROM T_VEC_COMPILE",
        "SELECT * FROM T_VEC_COMPILE" }) {
        int estimate = stmt.optimizeQuery(sql).getProjector().getEstimatedRowByteSize();
        assertTrue(sql + " estimated " + estimate, estimate >= DIM * Float.BYTES);
      }
    }
  }

  /** A NaN or infinite element fails the write, from a bound value or from an array coercion. */
  @Test
  public void testNonFiniteVectorElementsRejectedOnUpsert() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (PreparedStatement ps =
        conn.prepareStatement("UPSERT INTO T_VEC_COMPILE (ID, VD) VALUES ('a', ?)")) {
        ps.setArray(1, conn.createArrayOf("DOUBLE", new Double[] { 1.0, Double.NaN, 0.0 }));
        assertConstraintViolation(ps::execute);
        ps.setArray(1, conn.createArrayOf("DOUBLE", new Double[] { 1.0, 2.0, 3.0 }));
        ps.execute();
      }
      // 1e39 is a finite DOUBLE, but it overflows FLOAT to infinity.
      String values = "0, 1e39" + String.join("", Collections.nCopies(DIM - 2, ", 0"));
      assertConstraintViolation(() -> conn.createStatement()
        .execute("UPSERT INTO T_VEC_COMPILE (ID, V) VALUES ('b', ARRAY[" + values + "])"));
      conn.createStatement().execute("UPSERT INTO T_VEC_COMPILE (ID, V) VALUES ('b', ARRAY["
        + values.replace("1e39", "1e38") + "])");
    }
  }

  private interface SqlAction {
    void run() throws SQLException;
  }

  private static void assertConstraintViolation(SqlAction action) {
    try {
      action.run();
      fail("Expected a constraint violation for a non-finite vector element");
    } catch (SQLException e) {
      assertEquals(SQLExceptionCode.CONSTRAINT_VIOLATION.getErrorCode(), e.getErrorCode());
      assertTrue(e.getMessage(), e.getMessage().contains("must be finite"));
    }
  }
}
