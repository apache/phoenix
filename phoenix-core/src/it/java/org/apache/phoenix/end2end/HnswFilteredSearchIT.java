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

import static org.apache.phoenix.end2end.HnswIndexIT.buildIndex;
import static org.apache.phoenix.end2end.HnswIndexIT.cosine;
import static org.apache.phoenix.end2end.HnswIndexIT.manager;
import static org.apache.phoenix.end2end.HnswIndexIT.regions;
import static org.apache.phoenix.end2end.HnswIndexIT.vector;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.hbase.index.vector.HnswIndexManager;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.util.ScanUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Integration tests for the server side of a filtered HNSW nearest neighbor search. Each test sets
 * the HNSW search attributes on an HBase scan. It sends the scan to the regions directly, without
 * the Phoenix query planner. The tests use filters with different pass rates, key ranges and TTL
 * masks.
 */
@Category(ParallelStatsDisabledTest.class)
public class HnswFilteredSearchIT extends ParallelStatsDisabledIT {
  private static final int COUNT = 2000;
  private static final int K = 10;
  private static final int CANDIDATES = 64;
  private static final int QUERIES = 10;

  private static String sharedTable;
  private static String sharedIndex;
  private static Map<String, Row> sharedRows;

  static final class Row {
    final int category;
    final float[] vector;

    Row(int category, float[] vector) {
      this.category = category;
      this.vector = vector;
    }
  }

  /**
   * Creates a data table with two regions and the given number of rows, and builds an HNSW index on
   * it. The row IDs alternate between the two regions. Column C holds the values 0 to 999 in equal
   * numbers. Thus {@code C < n} passes about n of each 1000 rows.
   */
  static Map<String, Row> createAndBuild(Connection conn, String table, String index, int count)
    throws Exception {
    conn.createStatement().execute("CREATE TABLE " + table
      + " (ID VARCHAR NOT NULL PRIMARY KEY, C INTEGER, V VECTOR(FLOAT, 16)) SPLIT ON ('m')");
    Map<String, Row> rows = new HashMap<>();
    Random random = new Random(42);
    try (
      PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + table + " VALUES (?, ?, ?)")) {
      for (int i = 0; i < count; i++) {
        String id = (i % 2 == 0 ? "a" : "z") + i;
        Row row = new Row((i * 7919) % 1000, vector(random));
        ps.setString(1, id);
        ps.setInt(2, row.category);
        Float[] boxed = new Float[row.vector.length];
        for (int d = 0; d < boxed.length; d++) {
          boxed[d] = row.vector[d];
        }
        ps.setArray(3, conn.createArrayOf("FLOAT", boxed));
        ps.executeUpdate();
        rows.put(id, row);
      }
    }
    conn.commit();
    conn.createStatement().execute("CREATE VECTOR INDEX " + index + " ON " + table
      + " (V) WITH (algorithm='HNSW', metric='COSINE') ASYNC");
    buildIndex(table, index);
    return rows;
  }

  // Creates the shared table and index one time. The tests that do not change rows share them
  private static synchronized Map<String, Row> shared(Connection conn) throws Exception {
    if (sharedRows == null) {
      sharedTable = generateUniqueName();
      sharedIndex = generateUniqueName();
      sharedRows = createAndBuild(conn, sharedTable, sharedIndex, COUNT);
    }
    return sharedRows;
  }

  /**
   * Sends an HBase scan with the HNSW search attributes and the filter of the given WHERE clause.
   * Returns the row keys that the scan returns. Each region search starts with CANDIDATES
   * candidates and has a target of K rows.
   */
  private static List<String> search(Connection conn, String table, String index, String where,
    float[] query, byte[] start, byte[] stop) throws Exception {
    PhoenixPreparedStatement ps =
      conn.prepareStatement("SELECT /*+ NO_INDEX */ ID FROM " + table + where)
        .unwrap(PhoenixPreparedStatement.class);
    QueryPlan plan = ps.optimizeQuery();
    Scan scan = new Scan(plan.getContext().getScan());
    // Set the scan attributes of a Phoenix client, which include the Phoenix TTL attributes
    ScanUtil.setScanAttributesForClient(scan, plan.getTableRef().getTable(), plan.getContext());
    if (start != null) {
      scan.withStartRow(start);
    }
    if (stop != null) {
      scan.withStopRow(stop);
    }
    byte[] request = new byte[Bytes.SIZEOF_INT * (2 + query.length)];
    Bytes.putInt(request, 0, CANDIDATES);
    Bytes.putInt(request, Bytes.SIZEOF_INT, K);
    for (int i = 0; i < query.length; i++) {
      Bytes.putFloat(request, Bytes.SIZEOF_INT * (2 + i), query[i]);
    }
    scan.setAttribute(BaseScannerRegionObserverConstants.HNSW_SEARCH_INDEX, Bytes.toBytes(index));
    scan.setAttribute(BaseScannerRegionObserverConstants.HNSW_SEARCH_QUERY, request);
    List<String> ids = new ArrayList<>();
    try (Table t = getUtility().getConnection().getTable(TableName.valueOf(table));
      ResultScanner scanner = t.getScanner(scan)) {
      for (Result result : scanner) {
        if (!ScanUtil.isDummy(result)) {
          ids.add(Bytes.toString(result.getRow()));
        }
      }
    }
    return ids;
  }

  static Set<String> select(Map<String, Row> rows, Predicate<String> predicate) {
    return rows.keySet().stream().filter(predicate).collect(Collectors.toSet());
  }

  private static List<String> topK(Map<String, Row> rows, Set<String> ids, float[] query) {
    return topK(rows, ids, query, K);
  }

  static List<String> topK(Map<String, Row> rows, Set<String> ids, float[] query, int k) {
    return ids.stream()
      .sorted(Comparator.comparingDouble(id -> -cosine(query, rows.get(id).vector))).limit(k)
      .collect(Collectors.toList());
  }

  private static int hits(List<String> expected, List<String> found) {
    Set<String> set = new HashSet<>(found);
    return (int) expected.stream().filter(set::contains).count();
  }

  /**
   * Asserts that each result row passes the filter, and that no row occurs two times. Each region
   * must return K rows. If fewer than K rows of a region pass, the region must return all of them.
   */
  private static void assertFilled(Set<String> passing, List<String> found) {
    assertTrue("rows outside the filter: " + found, passing.containsAll(found));
    assertEquals("duplicate rows: " + found, found.size(), new HashSet<>(found).size());
    for (String prefix : new String[] { "a", "z" }) {
      long expected = Math.min(K, passing.stream().filter(id -> id.startsWith(prefix)).count());
      long actual = found.stream().filter(id -> id.startsWith(prefix)).count();
      assertTrue("region " + prefix + " returned " + actual + " of " + expected,
        actual >= expected);
    }
  }

  /**
   * A filter on a non-key column makes the search probe its candidates. The filters pass 50%, 5%
   * and 0.1% of the rows. The search asks for more candidates until enough rows pass. The recall is
   * at least 0.9. If fewer than K rows pass, the search returns all of them.
   */
  @Test
  public void testNonKeyFilter() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, Row> rows = shared(conn);
      for (int bound : new int[] { 500, 50, 1 }) {
        Set<String> passing = select(rows, id -> rows.get(id).category < bound);
        Random random = new Random(bound);
        int hits = 0;
        for (int q = 0; q < QUERIES; q++) {
          float[] query = vector(random);
          List<String> found =
            search(conn, sharedTable, sharedIndex, " WHERE C < " + bound, query, null, null);
          assertFilled(passing, found);
          if (bound == 500) {
            // At a high pass rate, the first candidates of each region give enough rows. Thus the
            // search does not ask for more candidates
            assertTrue(found.size() <= 2 * CANDIDATES);
          }
          hits += hits(topK(rows, passing, query), found);
          if (passing.size() < K) {
            assertEquals(passing, new HashSet<>(found));
          }
        }
        double recall = hits / (double) (QUERIES * Math.min(K, passing.size()));
        assertTrue("C < " + bound + " recall " + recall, recall >= 0.9);
      }
    }
  }

  /**
   * The graph search applies the key range of the scan and the keys of a SkipScanFilter to each
   * node. The test uses a wide range, a narrow range with fewer rows than CANDIDATES, and an IN
   * list with keys in both regions.
   */
  @Test
  public void testKeyRange() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, Row> rows = shared(conn);
      Set<String> wide = select(rows, id -> id.compareTo("a2") >= 0 && id.compareTo("a5") < 0);
      Random random = new Random(11);
      int hits = 0;
      for (int q = 0; q < QUERIES; q++) {
        float[] query = vector(random);
        List<String> found = search(conn, sharedTable, sharedIndex,
          " WHERE ID >= 'a2' AND ID < 'a5'", query, null, null);
        assertTrue("rows outside the range: " + found, wide.containsAll(found));
        assertTrue(found.size() >= K);
        // The scan reads only the candidates of the region, not every row of the range
        assertTrue(found.size() <= CANDIDATES);
        hits += hits(topK(rows, wide, query), found);
      }
      double recall = hits / (double) (QUERIES * K);
      assertTrue("range recall " + recall, recall >= 0.9);

      Set<String> narrow = select(rows, id -> id.compareTo("a10") >= 0 && id.compareTo("a102") < 0);
      assertTrue(narrow.size() > K && narrow.size() <= CANDIDATES);
      assertEquals(narrow, new HashSet<>(search(conn, sharedTable, sharedIndex,
        " WHERE ID >= 'a10' AND ID < 'a102'", vector(random), null, null)));

      Set<String> in = new HashSet<>(Arrays.asList("a2", "a40", "a1998", "z1", "z3", "z555"));
      assertEquals(in, new HashSet<>(search(conn, sharedTable, sharedIndex,
        " WHERE ID IN ('a2', 'a40', 'a1998', 'z1', 'z3', 'z555')", vector(random), null, null)));
    }
  }

  /**
   * Phoenix does not see a raw HBase delete, so the graph keeps the deleted rows. The probe removes
   * these candidates, and other candidates take their places. Each region still returns K rows.
   */
  @Test
  public void testStaleCandidates() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, Row> rows = createAndBuild(conn, table, index, 400);
      Set<String> passing = select(rows, id -> rows.get(id).category < 500);
      float[] query = vector(new Random(17));
      List<String> deleted = topK(rows, passing, query);
      try (Table t = getUtility().getConnection().getTable(TableName.valueOf(table))) {
        for (String id : deleted) {
          t.delete(new Delete(Bytes.toBytes(id)));
        }
      }
      passing.removeAll(deleted);
      List<String> found = search(conn, table, index, " WHERE C < 500", query, null, null);
      for (String id : deleted) {
        assertFalse(id + " was deleted", found.contains(id));
      }
      assertFilled(passing, found);
      assertTrue(hits(topK(rows, passing, query), found) >= 8);
    }
  }

  /**
   * A conditional Phoenix TTL masks the nearest rows that pass the filter. The probe removes these
   * candidates, and the search asks for more candidates until enough visible rows pass.
   */
  @Test
  public void testTtlMaskedCandidates() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      // A search reads its range without restriction when it needs a tenth of the rows of the
      // region. With 7000 rows in each region, a search can ask for more candidates before it
      // reaches that limit
      int perRegion = 7000;
      Map<String, Row> rows = createAndBuild(conn, table, index, 2 * perRegion);
      Set<String> passing = select(rows, id -> rows.get(id).category < 500);
      float[] query = vector(new Random(23));
      List<String> masked = topK(rows, passing, query, 60);
      conn.createStatement().execute("ALTER TABLE " + table + " SET TTL = 'ID IN ("
        + masked.stream().map(id -> "''" + id + "''").collect(Collectors.joining(", ")) + ")'");
      passing.removeAll(masked);
      // The masked rows are the nearest rows that pass the filter. Thus the nearest candidates of
      // each region hold fewer than K visible rows that pass. A search that returns K rows of each
      // region, as assertFilled requires, must ask for more candidates
      for (String prefix : new String[] { "a", "z" }) {
        List<String> nearest =
          topK(rows, select(rows, id -> id.startsWith(prefix)), query, CANDIDATES);
        long visible = nearest.stream().filter(passing::contains).count();
        assertTrue(
          "region " + prefix + " has " + visible + " visible rows in its nearest " + CANDIDATES,
          visible < K);
      }
      List<String> found = search(conn, table, index, " WHERE C < 500", query, null, null);
      // The search scan has no NON_AGGREGATE_QUERY attribute, so only the probe masks expired rows.
      // A read without restriction masks no rows, and returns about half of the rows of a region.
      // The check below uses this to find such a read
      for (String prefix : new String[] { "a", "z" }) {
        long actual = found.stream().filter(id -> id.startsWith(prefix)).count();
        assertTrue("region " + prefix + " read without restriction: " + actual,
          actual < perRegion / 10);
      }
      for (String id : masked) {
        assertFalse(id + " is masked by TTL", found.contains(id));
      }
      assertFilled(passing, found);
      assertTrue(hits(topK(rows, passing, query), found) >= 8);
    }
  }

  /**
   * Two scans divide one region at a key. Each scan searches only its own key range, returns at
   * most CANDIDATES rows, and has a recall of at least 0.9.
   */
  @Test
  public void testSplitScans() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, Row> rows = shared(conn);
      byte[] mid = Bytes.toBytes("a5");
      Set<String> low = select(rows,
        id -> id.startsWith("a") && id.compareTo("a5") < 0 && rows.get(id).category < 500);
      Set<String> high = select(rows,
        id -> id.startsWith("a") && id.compareTo("a5") >= 0 && rows.get(id).category < 500);
      Random random = new Random(13);
      int hits = 0;
      for (int q = 0; q < QUERIES; q++) {
        float[] query = vector(random);
        List<String> first =
          search(conn, sharedTable, sharedIndex, " WHERE C < 500", query, null, mid);
        List<String> second =
          search(conn, sharedTable, sharedIndex, " WHERE C < 500", query, mid, Bytes.toBytes("m"));
        assertTrue("rows outside the first half: " + first, low.containsAll(first));
        assertTrue("rows outside the second half: " + second, high.containsAll(second));
        assertTrue(first.size() >= K && second.size() >= K);
        // Each scan reads only its candidates, not every row of its range that passes
        assertTrue(first.size() <= CANDIDATES && second.size() <= CANDIDATES);
        hits += hits(topK(rows, low, query), first) + hits(topK(rows, high, query), second);
      }
      double recall = hits / (double) (2 * QUERIES * K);
      assertTrue("split scan recall " + recall, recall >= 0.9);
    }
  }

  /**
   * Tests the search and count methods of HnswIndexManager. A search returns only keys in its
   * range. If a range holds no more indexed keys than the candidate count, the search returns all
   * of them without a graph search. The graph search applies a key predicate to each node.
   */
  @Test
  public void testManagerKeyPredicate() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      Map<String, Row> rows = shared(conn);
      HRegion region = regions(sharedTable).get(0);
      HnswIndexManager manager = manager(region, sharedIndex);
      float[] query = vector(new Random(19));

      Set<String> wide = select(rows, id -> id.compareTo("a2") >= 0 && id.compareTo("a5") < 0);
      List<byte[]> found = manager.search(query, K, Bytes.toBytes("a2"), Bytes.toBytes("a5"), null);
      assertEquals(K, found.size());
      for (byte[] key : found) {
        assertTrue(wide.contains(Bytes.toString(key)));
      }
      assertEquals(wide.size(), manager.count(Bytes.toBytes("a2"), Bytes.toBytes("a5")));

      Set<String> narrow = select(rows, id -> id.compareTo("a10") >= 0 && id.compareTo("a102") < 0);
      Set<String> all = new HashSet<>();
      for (byte[] key : manager.search(query, CANDIDATES, Bytes.toBytes("a10"),
        Bytes.toBytes("a102"), null)) {
        all.add(Bytes.toString(key));
      }
      assertEquals(narrow, all);

      // The graph search applies the key predicate to each node
      found = manager.search(query, K, new byte[0], new byte[0], key -> key[key.length - 1] == '0');
      assertEquals(K, found.size());
      for (byte[] key : found) {
        assertTrue(Bytes.toString(key).endsWith("0"));
      }
      assertEquals(COUNT / 2, manager.count(new byte[0], new byte[0]));
    }
  }
}
