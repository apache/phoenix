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

import static org.apache.phoenix.end2end.HnswIndexIT.vector;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.execute.HnswScanPlan;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Integration tests verifying query planning and parallel scan chunking behavior for HNSW index
 * scans on tables with guidepost statistics enabled.
 */
@Category(ParallelStatsEnabledTest.class)
public class HnswParallelScanIT extends ParallelStatsEnabledIT {

  private static int scanCount(QueryPlan plan) {
    int count = 0;
    for (List<Scan> scans : plan.getScans()) {
      count += scans.size();
    }
    return count;
  }

  private static Float[] box(float[] v) {
    Float[] boxed = new Float[v.length];
    for (int i = 0; i < v.length; i++) {
      boxed[i] = v[i];
    }
    return boxed;
  }

  /**
   * Verify HNSW execution plans allocate exactly one scan per region to maintain cohesive graph
   * searches, contrasting with exact scan plans which split along guideposts, while preserving
   * target recall under filtered search.
   */
  @Test
  public void testOneScanPerRegion() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, HnswFilteredSearchIT.Row> rows =
        HnswFilteredSearchIT.createAndBuild(conn, table, index, 2000);
      conn.createStatement().execute(
        "ALTER TABLE " + table + " SET " + PhoenixDatabaseMetaData.GUIDE_POSTS_WIDTH + "=1000");
      conn.createStatement().execute("UPDATE STATISTICS " + table);
      Set<String> passing = HnswFilteredSearchIT.select(rows, id -> rows.get(id).category < 500);
      Random random = new Random(29);
      int hits = 0;
      int queries = 10;
      for (int i = 0; i < queries; i++) {
        float[] q = vector(random);
        for (String hint : new String[] { "", "/*+ NO_INDEX */" }) {
          List<String> found = new ArrayList<>();
          QueryPlan plan;
          try (PreparedStatement ps = conn.prepareStatement("SELECT " + hint + " ID FROM " + table
            + " WHERE C < 500 ORDER BY COSINE_DISTANCE(V, ?) LIMIT 10")) {
            ps.setArray(1, conn.createArrayOf("FLOAT", box(q)));
            try (ResultSet rs = ps.executeQuery()) {
              while (rs.next()) {
                found.add(rs.getString(1));
              }
              plan = rs.unwrap(PhoenixResultSet.class).getStatement().getQueryPlan();
            }
          }
          if (hint.isEmpty()) {
            assertTrue(plan instanceof HnswScanPlan);
            assertEquals("one scan per region", 2, scanCount(plan));
            List<String> expected = HnswFilteredSearchIT.topK(rows, passing, q, 10);
            hits += expected.stream().filter(found::contains).count();
          } else {
            assertTrue("guideposts split the exact plan: " + scanCount(plan), scanCount(plan) > 2);
          }
        }
      }
      double recall = hits / (double) (queries * 10);
      assertTrue("recall " + recall, recall >= 0.9);
    }
  }
}
