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
package org.apache.phoenix.query.explain;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * End-to-end tests for {@code EXPLAIN (FORMAT JSON) <stmt>}. Exercises the
 * {@code Statement.executeQuery} ResultSet path and confirms the single emitted row carries a
 * pretty-printed JSON document that matches the in process {@code ExplainPlanAttributes} tree for
 * the same query.
 */
public class ExplainJsonOutputTest extends BaseConnectionlessQueryTest {

  private static final String QUERY = "SELECT a_string, b_string FROM atable"
    + " WHERE organization_id = '00D000000000001' AND entity_id = '00E00000000001'"
    + " AND x_integer = 2 AND a_integer < 5";

  private static ExplainOracle oracle;

  @BeforeClass
  public static synchronized void setUpOracle() throws Exception {
    oracle = new ExplainOracle();
  }

  private static Properties defaultProps() {
    return PropertiesUtil.deepCopy(TEST_PROPERTIES);
  }

  /**
   * Read the single VARCHAR cell of an {@code EXPLAIN (FORMAT JSON)} result set, asserting that
   * exactly one row is returned.
   */
  private static String readSingleRow(Connection conn, String explainSql) throws Exception {
    try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(explainSql)) {
      assertTrue("expected at least one row", rs.next());
      String cell = rs.getString(1);
      assertFalse("expected exactly one row", rs.next());
      return cell;
    }
  }

  @Test
  public void testFormatJsonMatchesInProcessAttributes() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), defaultProps())) {
      String json = readSingleRow(conn, "EXPLAIN (FORMAT JSON) " + QUERY);
      // The emitted document is pretty-printed.
      assertTrue("expected pretty-printed JSON with newlines", json.contains("\n"));
      assertTrue("expected two-space indentation", json.contains("\n  \""));
      // The e2e ResultSet path must match the in process attributes path after normalization.
      JsonNode actual = oracle.mapper().readTree(json);
      new ExplainJsonNormalizer().normalize(actual);
      ExplainPlan plan = ExplainPlanTestUtil.getExplainPlan(conn, QUERY);
      JsonNode expected = oracle.serializeNormalized(plan.getPlanStepsAsAttributes());
      assertEquals(expected, actual);
    }
  }

  @Test
  public void testFormatJsonWithRegions() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), defaultProps())) {
      String json = readSingleRow(conn, "EXPLAIN (REGIONS, FORMAT JSON) " + QUERY);
      assertTrue("expected pretty-printed JSON with newlines", json.contains("\n"));
      // This case confirms the (REGIONS, FORMAT JSON) combination produces a single well formed
      // JSON row.
      JsonNode actual = oracle.mapper().readTree(json);
      new ExplainJsonNormalizer().normalize(actual);
      ExplainPlan plan = ExplainPlanTestUtil.getExplainPlan(conn, QUERY);
      JsonNode expected = oracle.serializeNormalized(plan.getPlanStepsAsAttributes());
      assertEquals(expected, actual);
    }
  }
}
