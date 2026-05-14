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
package org.apache.phoenix.end2end.index;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.TreeSet;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.util.PropertiesUtil;
import org.bson.BsonDocument;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class BsonPathIndexConsistencyIT extends ParallelStatsDisabledIT {

  private static final long SEED = 0xC0FFEEL;

  @Test
  public void resultsMatchWithAndWithoutIndex() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String tbl = generateUniqueName();
    String idx = generateUniqueName();
    Random rng = new Random(SEED);

    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement().execute(
          "CREATE TABLE " + tbl + " (PK VARCHAR PRIMARY KEY, DOC BSON)");
      // Insert 200 rows; ~20% are missing the indexed path.
      try (PreparedStatement ps = conn.prepareStatement(
          "UPSERT INTO " + tbl + " VALUES (?, ?)")) {
        for (int i = 0; i < 200; i++) {
          String name = "n" + (rng.nextInt(40));
          BsonDocument d = (rng.nextDouble() < 0.2)
              ? BsonDocument.parse("{\"other\":\"x\"}")
              : BsonDocument.parse("{\"name\":\"" + name + "\"}");
          ps.setString(1, "k" + i);
          ps.setObject(2, d);
          ps.execute();
        }
      }
      conn.commit();

      conn.createStatement().execute(
          "CREATE INDEX " + idx + " ON " + tbl + "(BSON_VALUE(DOC, '$.name', 'VARCHAR'))");

      List<String> queries = sampleQueries(tbl, rng, 100);

      // 1) Run all queries with index enabled.
      List<List<String>> indexed = runAll(conn, queries);

      // 2) Disable index, run again.
      conn.createStatement().execute("ALTER INDEX " + idx + " ON " + tbl + " DISABLE");
      List<List<String>> baseline = runAll(conn, queries);

      assertEquals("query count", indexed.size(), baseline.size());
      for (int i = 0; i < indexed.size(); i++) {
        assertEquals("mismatch on query: " + queries.get(i),
            new TreeSet<>(baseline.get(i)), new TreeSet<>(indexed.get(i)));
      }
    }
  }

  private static List<String> sampleQueries(String tbl, Random rng, int n) {
    List<String> qs = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      String pathForm = rng.nextBoolean() ? "$.name" : "name";
      int kind = rng.nextInt(4);
      switch (kind) {
        case 0:
          qs.add("SELECT PK FROM " + tbl + " WHERE BSON_VALUE(DOC, '" + pathForm
              + "', 'VARCHAR') = 'n" + rng.nextInt(40) + "'");
          break;
        case 1:
          qs.add("SELECT PK FROM " + tbl + " WHERE BSON_VALUE(DOC, '" + pathForm
              + "', 'VARCHAR') IN ('n" + rng.nextInt(40) + "', 'n" + rng.nextInt(40) + "')");
          break;
        case 2:
          qs.add("SELECT PK FROM " + tbl + " WHERE BSON_VALUE(DOC, '" + pathForm
              + "', 'VARCHAR') > 'n" + rng.nextInt(40) + "'");
          break;
        case 3:
          qs.add("SELECT PK FROM " + tbl + " WHERE BSON_VALUE(DOC, '" + pathForm
              + "', 'VARCHAR') BETWEEN 'n0' AND 'n" + rng.nextInt(40) + "'");
          break;
      }
    }
    return qs;
  }

  private static List<List<String>> runAll(Connection conn, List<String> queries) throws Exception {
    List<List<String>> out = new ArrayList<>();
    for (String q : queries) {
      List<String> rows = new ArrayList<>();
      try (ResultSet rs = conn.createStatement().executeQuery(q)) {
        while (rs.next()) rows.add(rs.getString(1));
      }
      Collections.sort(rows);
      out.add(rows);
    }
    return out;
  }
}
