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
package org.apache.phoenix.end2end.tpcds;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.util.PropertiesUtil;

/**
 * Common plumbing for the TPC-DS derived ITs. The suite is parameterized over the two schemas
 * materialized by {@link TPCDSLikeFixture}.
 * <p>
 * Adapted query constants are written against {@link TPCDSLikeFixture#SCHEMA}.
 */
public abstract class TPCDSLikeBaseIT extends ParallelStatsDisabledIT {

  protected final String schema;
  protected final boolean noIndex;

  protected TPCDSLikeBaseIT(String label, String schema, boolean noIndex) {
    this.schema = schema;
    this.noIndex = noIndex;
  }

  protected static Collection<Object[]> indexParameters() {
    return Arrays.asList(new Object[][] { { "NO_INDEX", TPCDSLikeFixture.SCHEMA, true },
      { "GLOBAL_INDEX", TPCDSLikeFixture.SCHEMA_INDEXED, false } });
  }

  protected static void loadFixture() throws SQLException {
    try (Connection conn = newConnection()) {
      TPCDSLikeFixture.load(conn);
    }
  }

  protected static Connection newConnection() throws SQLException {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    return DriverManager.getConnection(getUrl(), props);
  }

  /** Rewrite the canonical {@code TPCDS.} table prefix to the parameter's schema. */
  protected String sql(String base) {
    if (TPCDSLikeFixture.SCHEMA.equals(schema)) {
      return base;
    }
    return base.replace(TPCDSLikeFixture.SCHEMA + ".", schema + ".");
  }

  private static final String[] NO_INDEXES = new String[0];

  /** Assert a query's full result. */
  protected void check(String label, String baseSql, String[][] expected, String... markers)
    throws SQLException {
    check(label, baseSql, expected, markers, NO_INDEXES);
  }

  /**
   * As {@link #check(String, String, String[][], String...)}, but additionally asserts that the
   * plan scans each covering index in {@code indexNames}.
   */
  protected void check(String label, String baseSql, String[][] expected, String[] markers,
    String[] indexNames) throws SQLException {
    String resolved = sql(baseSql);
    try (Connection conn = newConnection()) {
      boolean captured = TPCDSLikeAssertions.assertResult(conn, label, resolved, expected);
      if (captured) {
        return;
      }
      if (markers.length > 0) {
        TPCDSLikeAssertions.assertPlanContains(conn, resolved, markers);
      }
      if (!noIndex && indexNames.length > 0) {
        String[] qualified = new String[indexNames.length];
        for (int i = 0; i < indexNames.length; i++) {
          qualified[i] = schema + "." + indexNames[i];
        }
        TPCDSLikeAssertions.assertPlanContains(conn, resolved, qualified);
      }
    }
  }
}
