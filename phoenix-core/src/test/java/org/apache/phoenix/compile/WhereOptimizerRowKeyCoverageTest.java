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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiPredicate;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Test;

/**
 * Checks that the scan a WHERE clause compiles to admits every row the predicate matches, judged on
 * real row-key bytes the way HBase applies it: the start/stop rows, then the {@link SkipScanFilter}
 * fed the in-range rows in key order. One key column takes values that are byte prefixes of each
 * other ('1', '10', '100'), which order differently in the row key for ASC and DESC variable-length
 * columns, so any step that compares per-column bytes without the column's separator shows up as a
 * dropped row. That column is the leading key in one set of cases and the trailing one in another.
 */
public class WhereOptimizerRowKeyCoverageTest extends BaseConnectionlessQueryTest {

  private static final List<String> PREFIX_VALUES =
    Arrays.asList("1", "10", "100", "11", "2", "20", "23", "230", "3");
  private static final List<String> AB = Arrays.asList("a", "b");

  /** Predicates over a grid of k1 in {@link #PREFIX_VALUES} and k2 in {'a', 'b'}. */
  private static final Object[][] LEADING_CASES =
    { { "k1 IN ('1', '10', '2', '23')", in("1", "10", "2", "23") },
      { "k1 = '1' OR k1 = '100'", in("1", "100") },
      { "k1 > '1'", (BiPredicate<String, String>) (a, b) -> a.compareTo("1") > 0 },
      { "k1 >= '10'", (BiPredicate<String, String>) (a, b) -> a.compareTo("10") >= 0 },
      { "k1 < '2'", (BiPredicate<String, String>) (a, b) -> a.compareTo("2") < 0 },
      { "k1 <= '23'", (BiPredicate<String, String>) (a, b) -> a.compareTo("23") <= 0 },
      { "k1 BETWEEN '1' AND '2'",
        (BiPredicate<String, String>) (a, b) -> a.compareTo("1") >= 0 && a.compareTo("2") <= 0 },
      { "k1 > '1' AND k1 < '23'",
        (BiPredicate<String, String>) (a, b) -> a.compareTo("1") > 0 && a.compareTo("23") < 0 },
      { "k1 < '10' OR k1 > '23'",
        (BiPredicate<String, String>) (a, b) -> a.compareTo("10") < 0 || a.compareTo("23") > 0 },
      { "k1 = '2' OR k1 > '23'",
        (BiPredicate<String, String>) (a, b) -> a.equals("2") || a.compareTo("23") > 0 },
      { "k1 IN ('1', '10') OR k1 BETWEEN '2' AND '23'",
        (BiPredicate<String,
          String>) (a, b) -> a.equals("1") || a.equals("10")
            || (a.compareTo("2") >= 0 && a.compareTo("23") <= 0) },
      { "k1 LIKE '1%'", (BiPredicate<String, String>) (a, b) -> a.startsWith("1") },
      { "k1 IN ('1', '10', '2', '23') AND k2 = 'b'",
        (BiPredicate<String,
          String>) (a, b) -> in("1", "10", "2", "23").test(a, b) && b.equals("b") },
      { "(k1, k2) IN (('1', 'a'), ('10', 'b'), ('2', 'b'))",
        (BiPredicate<String,
          String>) (a, b) -> (a.equals("1") && b.equals("a")) || (a.equals("10") && b.equals("b"))
            || (a.equals("2") && b.equals("b")) },
      { "(k1, k2) > ('1', 'a')",
        (BiPredicate<String,
          String>) (a, b) -> a.compareTo("1") > 0 || (a.equals("1") && b.compareTo("a") > 0) },
      { "k1 <= '1' AND k1 <= '10'",
        (BiPredicate<String, String>) (a, b) -> a.compareTo("1") <= 0 && a.compareTo("10") <= 0 },
      { "k1 >= '1' AND k1 >= '10'",
        (BiPredicate<String, String>) (a, b) -> a.compareTo("1") >= 0 && a.compareTo("10") >= 0 },
      { "k1 > '1' AND k1 < '100'",
        (BiPredicate<String, String>) (a, b) -> a.compareTo("1") > 0 && a.compareTo("100") < 0 },
      { "k1 >= '10' AND k1 <= '2'",
        (BiPredicate<String, String>) (a, b) -> a.compareTo("10") >= 0 && a.compareTo("2") <= 0 },
      { "k1 < '10' OR k1 = '100'",
        (BiPredicate<String, String>) (a, b) -> a.compareTo("10") < 0 || a.equals("100") },
      { "k1 >= '1' AND k1 <= '10' OR k1 = '100'",
        (BiPredicate<String,
          String>) (a, b) -> (a.compareTo("1") >= 0 && a.compareTo("10") <= 0) || a.equals("100") },
      { "k1 = '10' OR k1 BETWEEN '1' AND '100'",
        (BiPredicate<String,
          String>) (a, b) -> a.equals("10") || (a.compareTo("1") >= 0 && a.compareTo("100") <= 0) },
      { "k1 < '2' OR k1 = '2'", (BiPredicate<String, String>) (a, b) -> a.compareTo("2") <= 0 },
      { "k1 > '2' OR k1 = '2'", (BiPredicate<String, String>) (a, b) -> a.compareTo("2") >= 0 },
      { "k1 > '23' OR k1 < '20'",
        (BiPredicate<String, String>) (a, b) -> a.compareTo("23") > 0 || a.compareTo("20") < 0 },
      { "k1 >= '230' OR k1 <= '2'",
        (BiPredicate<String, String>) (a, b) -> a.compareTo("230") >= 0 || a.compareTo("2") <= 0 },
      { "k1 = '1' AND k1 < '10'", in("1") }, { "k1 = '2' AND k1 < '23'", in("2") },
      { "k1 IN ('1', '2', '3') AND k1 < '23'", in("1", "2") },
      { "k1 = '10' AND k1 > '1'", in("10") }, { "k1 >= '1' AND k1 < '10'", in("1") },
      { "k1 > '1' AND k1 <= '10'", in("10") }, { "k1 >= '2' AND k1 < '23'", in("2", "20") },
      { "k1 BETWEEN '1' AND '10' AND k1 >= '10'", in("10") },
      { "(k1, k2) <= ('23', 'a')", (BiPredicate<String,
        String>) (a, b) -> a.compareTo("23") < 0 || (a.equals("23") && b.compareTo("a") <= 0) }, };

  /**
   * Predicates over a grid of k1 in {'a', 'b'} and k2 in {@link #PREFIX_VALUES}, so the scan's
   * compound slot spans both columns and ends on the prefix-valued one.
   */
  private static final Object[][] TRAILING_CASES = {
    { "k1 = 'a' AND k2 IN ('1', '10', '2', '23')",
      (BiPredicate<String,
        String>) (a, b) -> a.equals("a") && Arrays.asList("1", "10", "2", "23").contains(b) },
    { "k1 = 'a' AND (k2 = '2' OR k2 > '23')",
      (BiPredicate<String,
        String>) (a, b) -> a.equals("a") && (b.equals("2") || b.compareTo("23") > 0) },
    { "k1 = 'a' AND (k2 IN ('1', '10') OR k2 BETWEEN '2' AND '23')",
      (BiPredicate<String, String>) (a, b) -> a.equals("a")
        && (b.equals("1") || b.equals("10") || (b.compareTo("2") >= 0 && b.compareTo("23") <= 0)) },
    { "k1 = 'a' AND k2 >= '1' AND k2 < '10'",
      (BiPredicate<String, String>) (a, b) -> a.equals("a") && b.equals("1") },
    { "k1 = 'a' AND k2 > '1'",
      (BiPredicate<String, String>) (a, b) -> a.equals("a") && b.compareTo("1") > 0 },
    { "k1 IN ('a', 'b') AND (k2 < '10' OR k2 = '100')",
      (BiPredicate<String, String>) (a, b) -> b.compareTo("10") < 0 || b.equals("100") },
    { "(k1, k2) IN (('a', '1'), ('a', '10'), ('b', '2'))",
      (BiPredicate<String,
        String>) (a, b) -> (a.equals("a") && (b.equals("1") || b.equals("10")))
          || (a.equals("b") && b.equals("2")) },
    { "(k1 = 'a' AND k2 <= '2') OR (k1 = 'b' AND k2 >= '230')",
      (BiPredicate<String,
        String>) (a, b) -> (a.equals("a") && b.compareTo("2") <= 0)
          || (a.equals("b") && b.compareTo("230") >= 0) },
    { "(k1, k2) > ('a', '2')", (BiPredicate<String,
      String>) (a, b) -> a.compareTo("a") > 0 || (a.equals("a") && b.compareTo("2") > 0) }, };

  @Test
  public void testAscLeadingVarcharScanCoversMatchingRows() throws Exception {
    assertScansCoverMatchingRows(SortOrder.ASC, SortOrder.ASC, PREFIX_VALUES, AB, LEADING_CASES);
  }

  @Test
  public void testDescLeadingVarcharScanCoversMatchingRows() throws Exception {
    assertScansCoverMatchingRows(SortOrder.DESC, SortOrder.ASC, PREFIX_VALUES, AB, LEADING_CASES);
  }

  @Test
  public void testAscTrailingVarcharScanCoversMatchingRows() throws Exception {
    assertScansCoverMatchingRows(SortOrder.ASC, SortOrder.ASC, AB, PREFIX_VALUES, TRAILING_CASES);
  }

  @Test
  public void testDescTrailingVarcharScanCoversMatchingRows() throws Exception {
    assertScansCoverMatchingRows(SortOrder.ASC, SortOrder.DESC, AB, PREFIX_VALUES, TRAILING_CASES);
  }

  private static BiPredicate<String, String> in(String... values) {
    List<String> list = Arrays.asList(values);
    return (a, b) -> list.contains(a);
  }

  private static final class Row {
    final String k1;
    final String k2;
    final Cell cell;

    Row(String k1, String k2, Cell cell) {
      this.k1 = k1;
      this.k2 = k2;
      this.cell = cell;
    }

    byte[] key() {
      return CellUtil.cloneRow(cell);
    }

    @Override
    public String toString() {
      return "(" + k1 + ", " + k2 + ")";
    }
  }

  @SuppressWarnings("unchecked")
  private static void assertScansCoverMatchingRows(SortOrder k1Order, SortOrder k2Order,
    List<String> k1Values, List<String> k2Values, Object[][] cases) throws Exception {
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (k1 VARCHAR NOT NULL, k2 VARCHAR NOT NULL,"
          + " v VARCHAR CONSTRAINT pk PRIMARY KEY (k1 " + k1Order + ", k2 " + k2Order + "))");
      List<Row> rows = encodeRows(conn, tableName, k1Values, k2Values);
      List<String> failures = new ArrayList<>();
      for (Object[] c : cases) {
        String where = (String) c[0];
        BiPredicate<String, String> matches = (BiPredicate<String, String>) c[1];
        List<Row> admitted = admittedRows(conn, tableName, where, rows);
        for (Row row : rows) {
          if (matches.test(row.k1, row.k2) && !admitted.contains(row)) {
            failures.add(where + " drops " + row);
          }
        }
      }
      if (!failures.isEmpty()) {
        fail("k1 " + k1Order + ", k2 " + k2Order + ": scans exclude matching rows: " + failures);
      }
    }
  }

  /** Every (k1, k2) in the grid with its real row key, sorted in row-key order. */
  private static List<Row> encodeRows(Connection conn, String tableName, List<String> k1Values,
    List<String> k2Values) throws SQLException {
    List<Row> rows = new ArrayList<>();
    PreparedStatement upsert =
      conn.prepareStatement("UPSERT INTO " + tableName + " (k1, k2, v) VALUES (?, ?, 'x')");
    for (String k1 : k1Values) {
      for (String k2 : k2Values) {
        upsert.setString(1, k1);
        upsert.setString(2, k2);
        upsert.execute();
        Iterator<Pair<byte[], List<Cell>>> it = PhoenixRuntime.getUncommittedDataIterator(conn);
        rows.add(new Row(k1, k2, it.next().getSecond().get(0)));
        conn.rollback();
      }
    }
    rows.sort((x, y) -> Bytes.compareTo(x.key(), y.key()));
    return rows;
  }

  /** The rows HBase would return for the compiled scan, before any residual filter. */
  private static List<Row> admittedRows(Connection conn, String tableName, String where,
    List<Row> rows) throws SQLException {
    PhoenixPreparedStatement stmt = new PhoenixPreparedStatement(
      conn.unwrap(PhoenixConnection.class), "SELECT * FROM " + tableName + " WHERE " + where);
    StatementContext context = stmt.compileQuery().getContext();
    List<Row> admitted = new ArrayList<>();
    if (context.getScanRanges().isDegenerate()) {
      return admitted;
    }
    Scan scan = context.getScan();
    byte[] start = scan.getStartRow();
    byte[] stop = scan.getStopRow();
    SkipScanFilter skipScan = findSkipScanFilter(scan.getFilter());
    // Rows below the last seek hint are never shown to the filter, as in a real scan.
    byte[] seekTo = null;
    for (Row row : rows) {
      byte[] key = row.key();
      boolean inRange = (start.length == 0 || Bytes.compareTo(key, start) >= 0)
        && (stop.length == 0 || Bytes.compareTo(key, stop) < 0);
      if (!inRange || (seekTo != null && Bytes.compareTo(key, seekTo) < 0)) {
        continue;
      }
      if (skipScan != null) {
        if (skipScan.filterAllRemaining()) {
          break;
        }
        ReturnCode code = skipScan.filterCell(row.cell);
        if (code == ReturnCode.SEEK_NEXT_USING_HINT) {
          seekTo = CellUtil.cloneRow(skipScan.getNextCellHint(row.cell));
        }
        if (code != ReturnCode.INCLUDE && code != ReturnCode.INCLUDE_AND_NEXT_COL) {
          continue;
        }
      }
      admitted.add(row);
    }
    assertTrue("scan for '" + where + "' admits no rows; grid or predicate is wrong",
      !admitted.isEmpty());
    return admitted;
  }

  private static SkipScanFilter findSkipScanFilter(Filter filter) {
    if (filter instanceof SkipScanFilter) {
      return (SkipScanFilter) filter;
    }
    if (filter instanceof FilterList) {
      for (Filter f : ((FilterList) filter).getFilters()) {
        SkipScanFilter found = findSkipScanFilter(f);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
  }
}
