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

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
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
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.util.ByteUtil;
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

  /**
   * The RVC IN list ends on a DESC column with prefix-related values. The scan can put that column
   * in its own slot, which loses the pairing between k1 and k2. The residual filter must then stay
   * and remove the unpaired rows.
   */
  @Test
  public void testDescTrailingRvcInReturnsOnlyPairedRows() throws Exception {
    for (String pk : new String[] { "k1, k2 DESC, k3", "k1 DESC, k2 DESC, k3" }) {
      for (String options : new String[] { "", " SALT_BUCKETS=4" }) {
        assertScanReturnsExactly(pk, options, "(k1, k2) IN (('a', '1'), ('b', '10'))", false,
          (a, b, c) -> (a.equals("a") && b.equals("1")) || (a.equals("b") && b.equals("10")));
      }
    }
  }

  /**
   * Prefix-related ranges on a DESC column must form one sorted, disjoint slot. Region pruning and
   * the salted stop row read the slot in that order, so overlapping ranges drop rows.
   */
  @Test
  public void testDescPrefixRangesFormDisjointSlot() throws Exception {
    for (String options : new String[] { "", " SALT_BUCKETS=4" }) {
      StatementContext context = assertScanReturnsExactly("k1 DESC, k2, k3", options,
        "k1 = '10' OR k1 BETWEEN '1' AND '100'", true,
        (a, b, c) -> in("1", "10", "100").test(a, b));
      List<List<KeyRange>> slots = context.getScanRanges().getRanges();
      assertEquals("k1 slot " + slots, 1, slots.get(slots.size() - 1).size());
    }
  }

  /**
   * Each OR branch is a DESC range whose lower value is a prefix of its upper value, so its raw
   * lower bound is above its raw upper bound. When there are too many branches, they collapse to
   * one bounding range. That range must keep the rows of every branch. The first query exceeds the
   * cartesian bound of the key space list. The second query exceeds the bound of the scan slot. An
   * IS NULL branch gives the bounding range a null lower bound, and the range must then keep the
   * null rows too. V1 loses rows of crossed DESC ranges in salted scans, so only V2 runs those
   * salted cases.
   */
  @Test
  public void testDescPrefixRangesBoundingHullKeepsRows() throws Exception {
    List<String> k1Values = new ArrayList<>(AB);
    List<String> branches = new ArrayList<>();
    List<String> fValues = new ArrayList<>();
    for (int i = 0; i < 260; i++) {
      k1Values.add(String.format("c%03d", i));
      branches.add(String.format("k2 BETWEEN '2' AND '2%03d'", i));
    }
    for (int i = 0; i < 300; i++) {
      fValues.add(String.format("'f%03d'", i));
    }
    // The k2 list is the larger side of the AND, so it is the side that collapses.
    branches.add("k2 BETWEEN '2' AND '2260'");
    branches.add("k2 BETWEEN '2' AND '2261'");
    branches.add("k2 BETWEEN '2' AND '2262'");
    String k1In = "k1 IN ('" + String.join("', '", k1Values) + "')";
    String crossed = String.join(" OR ", branches);
    String[] crossedOptions = new String[] { "" };
    StringBuilder or = new StringBuilder();
    for (int i = 0; i <= 50000; i++) {
      or.append(i == 0 ? "" : " OR ").append(String.format("(k1 > '2' AND k1 <= '2%05d')", i));
    }
    for (String options : crossedOptions) {
      for (String pk : new String[] { "k1, k2 DESC, k3", "k1 DESC, k2 DESC, k3" }) {
        assertScanReturnsExactly(pk, options, k1In + " AND (" + crossed + ")", false,
          (a, b, c) -> AB.contains(a) && b.compareTo("2") >= 0 && b.compareTo("2262") <= 0);
      }
      assertScanReturnsExactly("k1 DESC, k2, k3", options, or.toString(), false,
        (a, b, c) -> a.compareTo("2") > 0 && a.compareTo("250000") <= 0);
    }
    String fIn = String.join(", ", fValues);
    // The flag of each case is true when V1 also returns the right rows in a salted scan.
    Object[][] cases = {
      { k1In + " AND (k2 IS NULL OR k2 IN (" + fIn + "))", true,
        (RowPredicate) (a, b, c) -> b == null },
      { k1In + " AND (k2 IS NULL OR k2 IN (" + fIn + ") OR k2 = '2')", true,
        (RowPredicate) (a, b, c) -> b == null || b.equals("2") },
      { k1In + " AND (k2 IS NULL OR k2 IN ('1', '10', '100', '2', '23', '230', " + fIn + "))", true,
        (RowPredicate) (a, b, c) -> b == null
          || in("1", "10", "100", "2", "23", "230").test(b, c) },
      { k1In + " AND (k2 IS NULL OR k2 <= '2' OR k2 IN (" + fIn + "))", true,
        (RowPredicate) (a, b, c) -> b == null || b.compareTo("2") <= 0 },
      { k1In + " AND (k2 IS NULL OR " + crossed + ")", false, (RowPredicate) (a, b, c) -> b == null
        || (b.compareTo("2") >= 0 && b.compareTo("2262") <= 0) }, };
    List<String> grid = new ArrayList<>(PREFIX_VALUES);
    grid.addAll(AB);
    List<String> k2Values = new ArrayList<>(grid);
    k2Values.add(null);
    for (String pk : new String[] { "k1, k2 DESC, k3", "k1 DESC, k2 DESC, k3" }) {
      for (String options : new String[] { "", " SALT_BUCKETS=4" }) {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
          String tableName = generateUniqueName();
          conn.createStatement()
            .execute("CREATE TABLE " + tableName + " (k1 VARCHAR NOT NULL, k2 VARCHAR,"
              + " k3 VARCHAR NOT NULL, v VARCHAR CONSTRAINT pk PRIMARY KEY (" + pk + "))"
              + options);
          List<Row> rows = new ArrayList<>();
          PreparedStatement upsert = conn.prepareStatement(
            "UPSERT INTO " + tableName + " (k1, k2, k3, v) VALUES (?, ?, ?, 'x')");
          for (String k1 : grid) {
            for (String k2 : k2Values) {
              for (String k3 : Arrays.asList("y", "z")) {
                upsert.setString(1, k1);
                upsert.setString(2, k2);
                upsert.setString(3, k3);
                upsert.execute();
                Iterator<Pair<byte[], List<Cell>>> it =
                  PhoenixRuntime.getUncommittedDataIterator(conn);
                rows.add(new Row(k1, k2, k3, it.next().getSecond().get(0)));
                conn.rollback();
              }
            }
          }
          rows.sort((x, y) -> Bytes.compareTo(x.key(), y.key()));
          for (int i = 0; i < cases.length; i++) {
            if (!(Boolean) cases[i][1] && !options.isEmpty() && !isV2Optimizer()) {
              continue;
            }
            RowPredicate matches = (RowPredicate) cases[i][2];
            Set<String> expected = new TreeSet<>();
            Set<String> returned = new TreeSet<>();
            for (Row row : rows) {
              if (AB.contains(row.k1) && matches.test(row.k1, row.k2, row.k3)) {
                expected.add(row.toString());
              }
            }
            for (Row row : returnedRows(compile(conn, tableName, (String) cases[i][0]), rows)) {
              returned.add(row.toString());
            }
            assertEquals("[" + pk + options + "] IS NULL case " + i, expected, returned);
          }
        }
      }
    }
  }

  /**
   * A DESC range whose lower value is a prefix of its upper value has a raw lower bound above its
   * raw upper bound ('23' to '2' is CDCC to CD). The range must stay a valid branch of the OR.
   * These tests use unsalted tables only. The salted check serializes the skip-scan filter, and
   * KeyRange serialization loses the inverted flag of such ranges. V1 has the same issue.
   */
  @Test
  public void testDescPrefixRangeInOrKeepsRows() throws Exception {
    assertScanReturnsExactly("k1 DESC, k2, k3", "", "(k1 > '2' AND k1 <= '23') OR k1 = '3'", false,
      (a, b, c) -> between(a, "2", "23") || a.equals("3"));
    assertScanReturnsExactly("k1 DESC, k2, k3", "",
      "(k1 > '2' AND k1 <= '23') OR (k1 > '1' AND k1 <= '100')", false,
      (a, b, c) -> between(a, "2", "23") || between(a, "1", "100"));
  }

  /**
   * An IS NULL branch on a DESC column must not stop the merge of the other ranges in its slot.
   * Overlapping ranges in the slot give a stop row that excludes the row (a, '1').
   */
  @Test
  public void testDescPrefixRangesMergeWithIsNull() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String tableName = generateUniqueName();
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (k1 VARCHAR NOT NULL, k2 VARCHAR,"
          + " k3 VARCHAR NOT NULL, v VARCHAR CONSTRAINT pk PRIMARY KEY (k1, k2 DESC, k3))");
      StatementContext context = compile(conn, tableName,
        "k1 = 'a' AND (k2 IS NULL OR k2 = '10' OR k2 BETWEEN '1' AND '100')");
      List<List<KeyRange>> slots = context.getScanRanges().getRanges();
      assertEquals("k2 slot " + slots, 2, slots.get(1).size());
      byte[] key = ByteUtil.concat(Bytes.toBytes("a"), new byte[] { 0, (byte) 0xCE, (byte) 0xFF },
        Bytes.toBytes("y"));
      assertTrue("stop row excludes (a, 1, y)",
        Bytes.compareTo(context.getScan().getStopRow(), key) > 0);
    }
  }

  /**
   * An AND of prefix-related bounds on a DESC column must intersect in row key order. In raw bytes
   * '2' (CD) is inside k1 > '23' (below CDCC), but in the row key '2' sorts after '23'. The cases
   * run on a leading DESC key and on a trailing DESC key.
   */
  @Test
  public void testDescPrefixRangesIntersect() throws Exception {
    Object[][] cases = { { "k1 > '23' AND k1 = '2'", in() }, { "k1 = '2' AND k1 < '23'", in("2") },
      { "k1 >= '230' AND k1 = '23'", in() }, { "k1 = '23' AND k1 < '230'", in("23") },
      { "k1 > '1' AND k1 = '100'", in("100") }, { "k1 < '10' AND k1 = '100'", in() },
      { "k1 > '2' AND k1 < '230'", in("20", "23") }, { "k1 >= '2' AND k1 < '23'", in("2", "20") },
      { "k1 > '23' AND k1 <= '3'", in("230", "3") },
      { "k1 < '23' AND k1 > '1'", in("10", "100", "11", "2", "20") },
      { "k1 IN ('2', '23', '230') AND k1 > '23'", in("230") }, };
    List<String> failures = new ArrayList<>();
    for (Object[] c : cases) {
      @SuppressWarnings("unchecked")
      BiPredicate<String, String> matches = (BiPredicate<String, String>) c[1];
      String where = (String) c[0];
      try {
        assertScanReturnsExactly("k1 DESC, k2, k3", "", where, false,
          (a, b, k3) -> matches.test(a, b));
      } catch (AssertionError e) {
        failures.add(e.getMessage());
      }
      try {
        assertScanReturnsExactly("k1, k2 DESC, k3", "", "k1 = 'a' AND " + where.replace("k1", "k2"),
          false, (a, b, k3) -> a.equals("a") && matches.test(b, a));
      } catch (AssertionError e) {
        failures.add(e.getMessage());
      }
    }
    assertTrue(String.join("\n", failures), failures.isEmpty());
  }

  /**
   * One OR branch ends on a DESC column with a value that is a byte prefix of the other branch's
   * value ('1' and '10'). The salted scan must still start at the rows of the longer value.
   */
  @Test
  public void testDescShorterBranchInCompoundKeepsRows() throws Exception {
    for (String pk : new String[] { "k1 DESC, k2 DESC, k3", "k1 DESC, k2 DESC, k3 DESC" }) {
      for (String options : new String[] { "", " SALT_BUCKETS=4" }) {
        assertScanReturnsExactly(pk, options, "k1 = '1' OR (k1 = '10' AND k2 = 'a')", false,
          (a, b, c) -> a.equals("1") || (a.equals("10") && b.equals("a")));
        assertScanReturnsExactly(pk, options, "k1 = '1' OR (k1 = '100' AND k2 IN ('a', 'b'))",
          false, (a, b, c) -> a.equals("1") || (a.equals("100") && AB.contains(b)));
        assertScanReturnsExactly(pk, options, "k1 = '10' OR (k1 = '1' AND k2 = 'a')", false,
          (a, b, c) -> a.equals("10") || (a.equals("1") && b.equals("a")));
        assertScanReturnsExactly(pk, options,
          "(k1 = '1' AND k2 = '2') OR (k1 = '1' AND k2 = '23' AND k3 = 'y')", false,
          (a, b, c) -> a.equals("1") && (b.equals("2") || (b.equals("23") && c.equals("y"))));
      }
    }
  }

  /**
   * The shorter OR branch ends on a DESC column after a leading IN list. The unsalted scan must
   * also keep the rows of the longer value, and its skip-scan hints must stay in key order.
   */
  @Test
  public void testDescShorterBranchAfterInListKeepsRows() throws Exception {
    for (String options : new String[] { "", " SALT_BUCKETS=4" }) {
      assertScanReturnsExactly("k1 DESC, k2 DESC, k3 DESC", options,
        "k1 IN ('a', 'b') AND (k2 = '1' OR (k2 = '10' AND k3 = 'y'))", false,
        (a, b, c) -> AB.contains(a) && (b.equals("1") || (b.equals("10") && c.equals("y"))));
    }
  }

  /**
   * A leading OR of points is ANDed with an OR on later key columns. The scan must keep only the
   * rows of the points. On a DESC key, a range over the points also holds rows of longer values. An
   * OR of a point branch and a range branch on k1 must also keep the rows of both branches.
   */
  @Test
  public void testLeadingPointsWithTrailingOrKeepRows() throws Exception {
    for (String pk : new String[] { "k1, k2, k3", "k1 DESC, k2, k3", "k1 DESC, k2 DESC, k3",
      "k1 DESC, k2 DESC, k3 DESC", "k1, k2 DESC, k3 DESC" }) {
      for (String options : new String[] { "", " SALT_BUCKETS=4" }) {
        assertScanReturnsExactly(pk, options, "(k1 = '1' OR k1 = '2') AND (k2 = '10' OR k3 = 'y')",
          false, (a, b, c) -> in("1", "2").test(a, b) && (b.equals("10") || c.equals("y")));
        assertScanReturnsExactly(pk, options, "k1 IN ('1', '2') AND (k2 = 'a' OR k3 = 'y')", false,
          (a, b, c) -> in("1", "2").test(a, b) && (b.equals("a") || c.equals("y")));
        assertScanReturnsExactly(pk, options, "(k1 = '1' AND k2 = '10') OR (k1 > '2' AND k1 < '3')",
          false, (a, b, c) -> (a.equals("1") && b.equals("10"))
            || (a.compareTo("2") > 0 && a.compareTo("3") < 0));
        assertScanReturnsExactly(pk, options, "(k1 = '1' AND k3 = 'y') OR (k1 > '2' AND k1 < '3')",
          false, (a, b, c) -> (a.equals("1") && c.equals("y"))
            || (a.compareTo("2") > 0 && a.compareTo("3") < 0));
        assertScanReturnsExactly(pk, options,
          "(k1 = '1' AND k3 = 'y') OR (k1 > '20' AND k1 <= '3')", false,
          (a, b, c) -> (a.equals("1") && c.equals("y"))
            || (a.compareTo("20") > 0 && a.compareTo("3") <= 0));
        assertScanReturnsExactly(pk, options, "(k1 > '1' AND k1 < '2') OR (k1 = '3' AND k2 = '1')",
          false, (a, b, c) -> (a.compareTo("1") > 0 && a.compareTo("2") < 0)
            || (a.equals("3") && b.equals("1")));
      }
    }
  }

  /**
   * A point and a range on k2 share the last slot. The slot must cover only k2. A wider slot
   * compares the trailing key columns too, so the point drops its rows.
   */
  @Test
  public void testTrailingPointAndRangeKeepRows() throws Exception {
    for (String pk : new String[] { "k1, k2, k3", "k1 DESC, k2, k3", "k1, k2 DESC, k3",
      "k1, k2, k3 DESC" }) {
      for (String options : new String[] { "", " SALT_BUCKETS=4" }) {
        assertScanReturnsExactly(pk, options, "k1 = 'a' AND (k2 = '2' OR k2 > '23')", false,
          (a, b, c) -> a.equals("a") && (b.equals("2") || b.compareTo("23") > 0));
        assertScanReturnsExactly(pk, options, "k1 IN ('a', 'b') AND (k2 = '2' OR k2 > '23')", false,
          (a, b, c) -> in("a", "b").test(a, b) && (b.equals("2") || b.compareTo("23") > 0));
      }
    }
  }

  /**
   * The IS NULL branch is a point in a slot that also has a range, and k3 follows k2. The scan must
   * return the k2 IS NULL rows and the range rows, and the skip-scan filter must not seek backward.
   */
  @Test
  public void testDescPrefixRangesWithIsNullReturnRows() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String tableName = generateUniqueName();
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (k1 VARCHAR NOT NULL, k2 VARCHAR,"
          + " k3 VARCHAR NOT NULL, v VARCHAR CONSTRAINT pk PRIMARY KEY (k1, k2 DESC, k3))");
      List<String> k2Values = new ArrayList<>(PREFIX_VALUES);
      k2Values.addAll(AB);
      k2Values.add(null);
      List<Row> rows = new ArrayList<>();
      PreparedStatement upsert = conn
        .prepareStatement("UPSERT INTO " + tableName + " (k1, k2, k3, v) VALUES (?, ?, ?, 'x')");
      for (String k1 : AB) {
        for (String k2 : k2Values) {
          for (String k3 : Arrays.asList("y", "z")) {
            upsert.setString(1, k1);
            upsert.setString(2, k2);
            upsert.setString(3, k3);
            upsert.execute();
            Iterator<Pair<byte[], List<Cell>>> it = PhoenixRuntime.getUncommittedDataIterator(conn);
            rows.add(new Row(k1, k2, k3, it.next().getSecond().get(0)));
            conn.rollback();
          }
        }
      }
      rows.sort((x, y) -> Bytes.compareTo(x.key(), y.key()));
      String where = "k1 = 'a' AND (k2 IS NULL OR k2 = '10' OR k2 BETWEEN '1' AND '100')";
      StatementContext context = compile(conn, tableName, where);
      Set<String> expected = new TreeSet<>();
      Set<String> returned = new TreeSet<>();
      for (Row row : rows) {
        if (row.k1.equals("a") && (row.k2 == null || in("1", "10", "100").test(row.k2, null))) {
          expected.add(row.toString());
        }
      }
      for (Row row : returnedRows(context, rows)) {
        returned.add(row.toString());
      }
      assertEquals(where, expected, returned);
    }
  }

  /**
   * A point and a range share the slot of a key column that is not the last. The slot must not span
   * the later key columns, because the point then matches no row.
   */
  @Test
  public void testPointAndRangeInMiddleSlotReturnRows() throws Exception {
    for (String pk : new String[] { "k1, k2, k3", "k1, k2 DESC, k3" }) {
      assertScanReturnsExactly(pk, "", "k1 = 'a' AND (k2 = '2' OR k2 BETWEEN '1' AND '100')", false,
        (a, b, c) -> a.equals("a") && in("1", "10", "100", "2").test(b, c));
      assertScanReturnsExactly(pk, "", "k1 IN ('a', 'b') AND (k2 = '3' OR k2 BETWEEN '1' AND '2')",
        false, (a, b, c) -> in("a", "b").test(a, c)
          && (b.equals("3") || (b.compareTo("1") >= 0 && b.compareTo("2") <= 0)));
    }
  }

  /**
   * An exclusive range on a DESC k2 after a point on a DESC k1 must keep its rows. The compound
   * bound of k1 and k2 must not lose the k2 values that start with the lower value, such as '10'.
   */
  @Test
  public void testDescExclusiveRangeAfterDescPointKeepsRows() throws Exception {
    Object[][] cases = {
      { "k1 = '1' AND k2 > '1' AND k2 < '2'",
        (RowPredicate) (a, b, c) -> a.equals("1") && b.compareTo("1") > 0 && b.compareTo("2") < 0 },
      { "k1 = '1' AND k2 > '10' AND k2 < '11'",
        (RowPredicate) (a, b, c) -> a.equals("1") && b.compareTo("10") > 0
          && b.compareTo("11") < 0 },
      { "k1 = '1' AND k2 > '2' AND k2 < '3'",
        (RowPredicate) (a, b, c) -> a.equals("1") && b.compareTo("2") > 0 && b.compareTo("3") < 0 },
      { "k1 = '10' AND k2 > '1' AND k2 < '2' AND k3 = 'y'", (RowPredicate) (a, b,
        c) -> a.equals("10") && b.compareTo("1") > 0 && b.compareTo("2") < 0 && c.equals("y") }, };
    for (String pk : new String[] { "k1 DESC, k2 DESC, k3", "k1 DESC, k2 DESC, k3 DESC",
      "k1, k2 DESC, k3" }) {
      for (String options : new String[] { "", " SALT_BUCKETS=4" }) {
        for (Object[] c : cases) {
          assertScanReturnsExactly(pk, options, (String) c[0], false, (RowPredicate) c[1]);
        }
      }
    }
  }

  /**
   * A range on a fixed-width k2 with no lower bound in row key order must keep its rows. A DESC k2
   * with k2 > x and an ASC k2 with k2 < x have no such bound. The start row must not put the bytes
   * of k3 at the position of k2.
   */
  @Test
  public void testFixedWidthOpenLowerBeforeKeyKeepsRows() throws Exception {
    Object[][] cases = {
      { "k1 = '1' AND k2 > 1 AND k3 = 'y'",
        (BiPredicate<BigDecimal,
          String>) (b, c) -> b.compareTo(BigDecimal.ONE) > 0 && c.equals("y") },
      { "k1 = '1' AND k2 >= 2 AND k3 = 'y'",
        (BiPredicate<BigDecimal,
          String>) (b, c) -> b.compareTo(BigDecimal.valueOf(2)) >= 0 && c.equals("y") },
      { "k1 = '1' AND k2 > 0 AND k3 = 'y'",
        (BiPredicate<BigDecimal, String>) (b, c) -> b.signum() > 0 && c.equals("y") },
      { "k1 = '1' AND k2 < 2 AND k3 = 'y'",
        (BiPredicate<BigDecimal,
          String>) (b, c) -> b.compareTo(BigDecimal.valueOf(2)) < 0 && c.equals("y") },
      { "k1 = '1' AND k2 <= 2 AND k3 = 'y'",
        (BiPredicate<BigDecimal,
          String>) (b, c) -> b.compareTo(BigDecimal.valueOf(2)) <= 0 && c.equals("y") },
      { "k1 = '1' AND k2 < 2",
        (BiPredicate<BigDecimal, String>) (b, c) -> b.compareTo(BigDecimal.valueOf(2)) < 0 }, };
    List<String> failures = new ArrayList<>();
    for (String type : new String[] { "DOUBLE", "INTEGER", "BIGINT", "UNSIGNED_INT" }) {
      // The extreme values have key bytes that start with 0x00 or are above 'y' (0x79).
      List<BigDecimal> k2Values = new ArrayList<>();
      for (String v : type.equals("DOUBLE")
        ? new String[] { "-1E308", "-1.5", "0", "1", "1.5", "2", "10", "2000000000" }
        : type.equals("INTEGER")
          ? new String[] { "-2147483648", "-1", "0", "1", "2", "10", "2000000000" }
        : type.equals("BIGINT")
          ? new String[] { "-9223372036854775808", "-1", "0", "1", "2", "10",
            "9000000000000000000" }
        : new String[] { "0", "1", "2", "3", "10", "2000000000" }) {
        k2Values.add(new BigDecimal(v));
      }
      for (String pk : new String[] { "k1, k2, k3", "k1, k2 DESC, k3", "k1 DESC, k2, k3",
        "k1 DESC, k2 DESC, k3" }) {
        for (String options : new String[] { "", " SALT_BUCKETS=4" }) {
          try (Connection conn = DriverManager.getConnection(getUrl())) {
            String tableName = generateUniqueName();
            conn.createStatement()
              .execute("CREATE TABLE " + tableName + " (k1 VARCHAR NOT NULL, k2 " + type
                + " NOT NULL, k3 VARCHAR NOT NULL, v VARCHAR CONSTRAINT pk PRIMARY KEY (" + pk
                + "))" + options);
            List<Row> rows = new ArrayList<>();
            PreparedStatement upsert = conn.prepareStatement(
              "UPSERT INTO " + tableName + " (k1, k2, k3, v) VALUES (?, ?, ?, 'x')");
            for (String k1 : Arrays.asList("1", "10", "2")) {
              for (BigDecimal k2 : k2Values) {
                for (String k3 : Arrays.asList("y", "z")) {
                  upsert.setString(1, k1);
                  upsert.setBigDecimal(2, k2);
                  upsert.setString(3, k3);
                  upsert.execute();
                  Iterator<Pair<byte[], List<Cell>>> it =
                    PhoenixRuntime.getUncommittedDataIterator(conn);
                  rows.add(new Row(k1, k2.toString(), k3, it.next().getSecond().get(0)));
                  conn.rollback();
                }
              }
            }
            rows.sort((x, y) -> Bytes.compareTo(x.key(), y.key()));
            for (Object[] c : cases) {
              String where = (String) c[0];
              @SuppressWarnings("unchecked")
              BiPredicate<BigDecimal, String> matches = (BiPredicate<BigDecimal, String>) c[1];
              Set<String> expected = new TreeSet<>();
              Set<String> returned = new TreeSet<>();
              for (Row row : rows) {
                if (row.k1.equals("1") && matches.test(new BigDecimal(row.k2), row.k3)) {
                  expected.add(row.toString());
                }
              }
              try {
                for (Row row : returnedRows(compile(conn, tableName, where), rows)) {
                  returned.add(row.toString());
                }
              } catch (Exception e) {
                returned.add(e.toString());
              }
              if (!expected.equals(returned)) {
                failures.add("[" + type + " " + pk + options + "] " + where + " expected "
                  + expected + " returned " + returned);
              }
            }
          }
        }
      }
    }
    assertTrue(String.join("\n", failures), failures.isEmpty());
  }

  /**
   * A point or an inclusive upper bound on a fixed-width or DECIMAL k2 must keep its rows when k3
   * follows k2. The slot of k2 must cover only k2.
   */
  @Test
  public void testFixedWidthTrailingPointAndRangeKeepRows() throws Exception {
    Object[][] cases = {
      { "k1 = 'a' AND (k2 = 2 OR k2 > 5)",
        (BiPredicate<String, BigDecimal>) (a, b) -> a.equals("a")
          && (b.compareTo(BigDecimal.valueOf(2)) == 0 || b.compareTo(BigDecimal.valueOf(5)) > 0) },
      { "k1 IN ('a', 'b') AND k2 <= 5",
        (BiPredicate<String, BigDecimal>) (a, b) -> b.compareTo(BigDecimal.valueOf(5)) <= 0 },
      { "k1 IN ('a', 'b') AND (k2 < 2 OR k2 = 5)",
        (BiPredicate<String, BigDecimal>) (a, b) -> b.compareTo(BigDecimal.valueOf(2)) < 0
          || b.compareTo(BigDecimal.valueOf(5)) == 0 }, };
    List<String> failures = new ArrayList<>();
    for (String type : new String[] { "INTEGER", "DECIMAL" }) {
      List<BigDecimal> k2Values = new ArrayList<>();
      for (String v : type.equals("INTEGER")
        ? new String[] { "-1", "0", "2", "3", "5", "6", "10" }
        : new String[] { "-1.5", "0", "2", "2.5", "5", "5.01", "10" }) {
        k2Values.add(new BigDecimal(v));
      }
      for (String pk : new String[] { "k1, k2, k3", "k1, k2 DESC, k3" }) {
        for (String options : new String[] { "", " SALT_BUCKETS=4" }) {
          try (Connection conn = DriverManager.getConnection(getUrl())) {
            String tableName = generateUniqueName();
            conn.createStatement()
              .execute("CREATE TABLE " + tableName + " (k1 VARCHAR NOT NULL, k2 " + type
                + " NOT NULL, k3 VARCHAR NOT NULL, v VARCHAR CONSTRAINT pk PRIMARY KEY (" + pk
                + "))" + options);
            List<Row> rows = new ArrayList<>();
            PreparedStatement upsert = conn.prepareStatement(
              "UPSERT INTO " + tableName + " (k1, k2, k3, v) VALUES (?, ?, ?, 'x')");
            for (String k1 : AB) {
              for (BigDecimal k2 : k2Values) {
                for (String k3 : Arrays.asList("y", "z")) {
                  upsert.setString(1, k1);
                  upsert.setBigDecimal(2, k2);
                  upsert.setString(3, k3);
                  upsert.execute();
                  Iterator<Pair<byte[], List<Cell>>> it =
                    PhoenixRuntime.getUncommittedDataIterator(conn);
                  rows.add(new Row(k1, k2.toPlainString(), k3, it.next().getSecond().get(0)));
                  conn.rollback();
                }
              }
            }
            rows.sort((x, y) -> Bytes.compareTo(x.key(), y.key()));
            for (Object[] c : cases) {
              String where = (String) c[0];
              @SuppressWarnings("unchecked")
              BiPredicate<String, BigDecimal> matches = (BiPredicate<String, BigDecimal>) c[1];
              Set<String> expected = new TreeSet<>();
              Set<String> returned = new TreeSet<>();
              for (Row row : rows) {
                if (matches.test(row.k1, new BigDecimal(row.k2))) {
                  expected.add(row.toString());
                }
              }
              try {
                for (Row row : returnedRows(compile(conn, tableName, where), rows)) {
                  returned.add(row.toString());
                }
              } catch (Exception e) {
                returned.add(e.toString());
              }
              if (!expected.equals(returned)) {
                failures.add("[" + type + " " + pk + options + "] " + where + " expected "
                  + expected + " returned " + returned);
              }
            }
          }
        }
      }
    }
    assertTrue(String.join("\n", failures), failures.isEmpty());
  }

  /** True when {@code lower < value <= upper}. */
  private static boolean between(String value, String lower, String upper) {
    return value.compareTo(lower) > 0 && value.compareTo(upper) <= 0;
  }

  /** The prefix guard applies only to DESC variable-length columns, so ASC ranges still merge. */
  @Test
  public void testAscPrefixRangesMerge() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String tableName = generateUniqueName();
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (k1 VARCHAR NOT NULL, k2 VARCHAR NOT NULL, v VARCHAR CONSTRAINT pk PRIMARY KEY (k1, k2))");
      ScanRanges scanRanges = compile(conn, tableName, "k1 LIKE '1%' OR k1 = '10'").getScanRanges();
      assertEquals(1, scanRanges.getRanges().size());
      assertEquals(1, scanRanges.getRanges().get(0).size());
    }
  }

  /**
   * The row key does not keep trailing null key columns. A skip scan does not match such a short
   * row key unless each later slot holds only IS NULL. An OR can give a slot IS NULL together with
   * other ranges, or no condition. The scan must keep the rows with trailing nulls. The cases run
   * on a nullable last key column, on a four-column key, and on a nullable middle key column. V1
   * loses rows in some cases, so only V2 runs those cases.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testTrailingNullInOrAfterOpenMiddleKeepsRows() throws Exception {
    RowPredicate bNull = (a, b, c) -> a.equals("b") && c == null;
    RowPredicate aOne = (a, b, c) -> a.equals("a") && "1".equals(c);
    // The flag of each case is true when V1 also returns the right rows.
    Object[][] trailing = {
      { "(k1 = 'b' AND k3 IS NULL) OR (k1 = 'a' AND k3 = '1')", true,
        (RowPredicate) (a, b, c) -> bNull.test(a, b, c) || aOne.test(a, b, c) },
      { "(k1 IN ('a', 'b') AND k3 IS NULL) OR (k1 = 'a' AND k3 = '1')", true,
        (RowPredicate) (a, b, c) -> (!a.equals("c") && c == null) || aOne.test(a, b, c) },
      { "(k1 = 'a' AND k3 = '1') OR (k1 = 'b' AND k3 IS NULL)", true,
        (RowPredicate) (a, b, c) -> bNull.test(a, b, c) || aOne.test(a, b, c) },
      { "(k1 = 'b' AND k3 IS NULL) OR (k1 = 'b' AND k3 = '1')", true,
        (RowPredicate) (a, b, c) -> a.equals("b") && (c == null || c.equals("1")) },
      { "(k1 = 'b' AND k3 IS NULL) OR (k1 = 'a' AND k3 IS NULL)", true,
        (RowPredicate) (a, b, c) -> !a.equals("c") && c == null },
      { "(k1 = 'b' AND k3 IS NULL) OR (k1 = 'a' AND k3 IS NOT NULL)", true,
        (RowPredicate) (a, b, c) -> bNull.test(a, b, c) || (a.equals("a") && c != null) },
      { "(k1 = 'b' AND k3 IS NOT NULL) OR (k1 = 'a' AND k3 = '1')", true,
        (RowPredicate) (a, b, c) -> (a.equals("b") && c != null) || aOne.test(a, b, c) },
      { "(k1 = 'b' AND k3 IS NULL) OR (k1 = 'a' AND k3 > '1')", true,
        (RowPredicate) (a, b, c) -> bNull.test(a, b, c)
          || (a.equals("a") && c != null && c.compareTo("1") > 0) },
      { "(k1 = 'b' AND k3 IS NULL) OR (k1 = 'a' AND k3 < '10')", true,
        (RowPredicate) (a, b, c) -> bNull.test(a, b, c)
          || (a.equals("a") && c != null && c.compareTo("10") < 0) },
      { "(k1 = 'b' AND (k3 IS NULL OR k3 >= '10')) OR (k1 = 'a' AND k3 = '1')", true,
        (RowPredicate) (a, b, c) -> (a.equals("b") && (c == null || c.compareTo("10") >= 0))
          || aOne.test(a, b, c) },
      { "(k1 = 'b' AND k3 IS NULL) OR (k1 = 'a' AND k3 IN ('1', 'y'))", true,
        (RowPredicate) (a, b, c) -> bNull.test(a, b, c)
          || (a.equals("a") && ("1".equals(c) || "y".equals(c))) },
      { "(k1 = 'b' AND k3 IS NULL) OR (k1 = 'a' AND k2 = '1' AND k3 = '1')", true,
        (RowPredicate) (a, b, c) -> bNull.test(a, b, c) || (aOne.test(a, b, c) && "1".equals(b)) },
      { "(k1 = 'b' AND k2 = '2' AND k3 IS NULL) OR (k1 = 'a' AND k3 = '1')", true,
        (RowPredicate) (a, b, c) -> (bNull.test(a, b, c) && "2".equals(b)) || aOne.test(a, b, c) },
      { "(k1 = 'b' AND k2 > '1' AND k3 IS NULL) OR (k1 = 'a' AND k3 = '1')", true,
        (RowPredicate) (a, b, c) -> (bNull.test(a, b, c) && b != null && b.compareTo("1") > 0)
          || aOne.test(a, b, c) },
      { "(k1 >= 'b' AND k3 IS NULL) OR (k1 = 'a' AND k3 = '1')", true,
        (RowPredicate) (a, b, c) -> (!a.equals("a") && c == null) || aOne.test(a, b, c) },
      { "(k1 = 'b' AND k3 IS NULL) OR (k1 = 'a' AND k2 = '1')", true,
        (RowPredicate) (a, b, c) -> bNull.test(a, b, c) || (a.equals("a") && "1".equals(b)) },
      { "(k1 = 'b' AND k3 = '1') OR (k1 = 'a' AND k2 = '1')", true,
        (RowPredicate) (a, b, c) -> (a.equals("b") && "1".equals(c))
          || (a.equals("a") && "1".equals(b)) },
      { "(k1 = 'b' AND k3 = '1') OR k1 = 'a'", true,
        (RowPredicate) (a, b, c) -> (a.equals("b") && "1".equals(c)) || a.equals("a") },
      { "(k1 = 'b' AND k2 = '1' AND k3 IS NULL) OR (k1 = 'a' AND k2 = '1' AND k3 = '1')", true,
        (RowPredicate) (a, b, c) -> "1".equals(b) && (bNull.test(a, b, c) || aOne.test(a, b, c)) },
      { "k3 IS NULL OR (k1 = 'a' AND k3 = '1')", true,
        (RowPredicate) (a, b, c) -> c == null || aOne.test(a, b, c) },
      { "k1 IN ('a', 'b') AND (k3 IS NULL OR k3 = '1')", false,
        (RowPredicate) (a, b, c) -> !a.equals("c") && (c == null || c.equals("1")) },
      { "k1 = 'a' AND k2 IN ('1', '2') AND (k3 IS NULL OR k3 = '1')", true,
        (RowPredicate) (a, b, c) -> a.equals("a") && b != null && (c == null || c.equals("1")) }, };
    Object[][] middle = {
      { "(k1 = 'b' AND k2 IS NULL) OR (k1 = 'a' AND k2 = '1')", true,
        (RowPredicate) (a, b, c) -> (a.equals("b") && b == null)
          || (a.equals("a") && "1".equals(b)) },
      { "(k1 = 'b' AND k2 IS NULL) OR (k1 = 'a' AND k3 = '1')", true,
        (RowPredicate) (a, b, c) -> (a.equals("b") && b == null) || aOne.test(a, b, c) },
      { "(k1 = 'b' AND k2 IS NULL AND k3 IS NULL) OR (k1 = 'a' AND k3 = '1')", true,
        (RowPredicate) (a, b, c) -> (bNull.test(a, b, c) && b == null) || aOne.test(a, b, c) },
      { "(k1 = 'b' AND k2 IS NULL AND k3 = '1') OR (k1 = 'a' AND k2 = '2')", true,
        (RowPredicate) (a, b, c) -> (a.equals("b") && b == null && "1".equals(c))
          || (a.equals("a") && "2".equals(b)) },
      { "(k1 = 'b' AND k2 IS NOT NULL) OR (k1 = 'a' AND k2 IS NULL)", true,
        (RowPredicate) (a, b, c) -> (a.equals("b") && b != null) || (a.equals("a") && b == null) },
      { "(k1 = 'b' AND k2 IS NULL) OR k1 = 'a'", true,
        (RowPredicate) (a, b, c) -> (a.equals("b") && b == null) || a.equals("a") },
      { "k1 IN ('a', 'b') AND (k2 IS NULL OR k2 = '1')", false,
        (RowPredicate) (a, b, c) -> !a.equals("c") && (b == null || b.equals("1")) }, };
    List<String> lastValues = Arrays.asList(null, "1", "10", "y");
    List<String> failures = new ArrayList<>();
    // Each shape gives the key columns, the middle and last values, and the cases. In the
    // four-column key, the cases name k3 as k2 and k4 as k3, and k2 is 'x' in each row.
    Object[][] shapes = {
      { "k1 VARCHAR NOT NULL, k2 VARCHAR NOT NULL, k3 VARCHAR", Arrays.asList("1", "2"), lastValues,
        trailing },
      { "k1 VARCHAR NOT NULL, k2 VARCHAR NOT NULL, k3 VARCHAR NOT NULL, k4 VARCHAR",
        Arrays.asList("1", "2"), lastValues, trailing },
      { "k1 VARCHAR NOT NULL, k2 VARCHAR, k3 VARCHAR", Arrays.asList(null, "1", "2"), lastValues,
        trailing },
      { "k1 VARCHAR NOT NULL, k2 VARCHAR, k3 VARCHAR", Arrays.asList(null, "1", "2"), lastValues,
        middle },
      { "k1 VARCHAR NOT NULL, k2 VARCHAR, k3 VARCHAR NOT NULL", Arrays.asList(null, "1", "2"),
        Arrays.asList("1", "10", "y"), middle }, };
    for (Object[] shape : shapes) {
      String columns = (String) shape[0];
      boolean fourKeys = columns.contains("k4");
      for (int orders = 0; orders < 8; orders++) {
        String o1 = (orders & 4) != 0 ? " DESC" : "";
        String o2 = (orders & 2) != 0 ? " DESC" : "";
        String o3 = (orders & 1) != 0 ? " DESC" : "";
        String pk = fourKeys
          ? "k1" + o1 + ", k2, k3" + o2 + ", k4" + o3
          : "k1" + o1 + ", k2" + o2 + ", k3" + o3;
        for (String options : new String[] { "", " SALT_BUCKETS=4" }) {
          try (Connection conn = DriverManager.getConnection(getUrl())) {
            String tableName = generateUniqueName();
            conn.createStatement().execute("CREATE TABLE " + tableName + " (" + columns
              + ", v VARCHAR CONSTRAINT pk PRIMARY KEY (" + pk + "))" + options);
            List<Row> rows = new ArrayList<>();
            PreparedStatement upsert = conn.prepareStatement(fourKeys
              ? "UPSERT INTO " + tableName + " (k1, k2, k3, k4, v) VALUES (?, 'x', ?, ?, 'x')"
              : "UPSERT INTO " + tableName + " (k1, k2, k3, v) VALUES (?, ?, ?, 'x')");
            for (String k1 : Arrays.asList("a", "b", "c")) {
              for (String k2 : (List<String>) shape[1]) {
                for (String k3 : (List<String>) shape[2]) {
                  upsert.setString(1, k1);
                  upsert.setString(2, k2);
                  upsert.setString(3, k3);
                  upsert.execute();
                  Iterator<Pair<byte[], List<Cell>>> it =
                    PhoenixRuntime.getUncommittedDataIterator(conn);
                  rows.add(new Row(k1, k2, k3, it.next().getSecond().get(0)));
                  conn.rollback();
                }
              }
            }
            rows.sort((x, y) -> Bytes.compareTo(x.key(), y.key()));
            for (Object[] c : (Object[][]) shape[3]) {
              if (!(Boolean) c[1] && !isV2Optimizer()) {
                continue;
              }
              String where = (String) c[0];
              if (fourKeys) {
                where = where.replace("k3", "k4").replace("k2", "k3");
              }
              RowPredicate matches = (RowPredicate) c[2];
              Set<String> expected = new TreeSet<>();
              Set<String> returned = new TreeSet<>();
              for (Row row : rows) {
                if (matches.test(row.k1, row.k2, row.k3)) {
                  expected.add(row.toString());
                }
              }
              StatementContext context = compile(conn, tableName, where);
              for (Row row : returnedRows(context, rows)) {
                returned.add(row.toString());
              }
              if (!expected.equals(returned)) {
                failures.add("[" + columns + "; " + pk + options + "] " + where + " expected "
                  + expected + " returned " + returned);
              }
            }
          }
        }
      }
    }
    assertTrue(String.join("\n", failures), failures.isEmpty());
  }

  /**
   * A row with trailing null key columns has a short row key. A range on a key column must keep
   * such rows when the row key can end after that column. The scan start row must not add a
   * separator after the last bound column. A skip-scan slot must not span the later nullable
   * columns. The expected rows come from the same query with each key column in an expression. The
   * expression stops key extraction, so the residual filter checks the full predicate. V1 and V2
   * both lose rows in some IN cases on a DESC k2 key, so those cases skip that key order.
   */
  @Test
  public void testTrailingNullAfterRangeKeepsRows() throws Exception {
    // The flag of each case is true when the case also runs on a DESC k2 key.
    Object[][] fourKeys = {
      { "(k1 = 'b' AND k2 >= '1' AND k3 IS NOT NULL) OR (k1 = 'a' AND k2 = '1' AND k4 = '1')",
        true },
      { "k1 IN ('a', 'b') AND k2 >= '1'", false }, { "k1 = 'b' AND k2 >= '1'", true },
      { "k1 >= 'b'", true }, { "k1 = 'b' AND k2 = '1' AND k3 >= '1'", true },
      { "k1 IN ('a', 'b') AND k2 = '1' AND k3 >= '1'", false },
      { "k1 = 'b' AND k2 = '1' AND k3 = '1' AND k4 >= '1'", true }, };
    Object[][] threeKeys =
      { { "k1 >= 'c'", true }, { "k1 > 'b'", true }, { "k1 BETWEEN 'b' AND 'c'", true },
        { "k1 = 'b' AND k2 >= '1'", true }, { "k1 IN ('a', 'c') AND k2 >= '1'", false },
        { "(k1 = 'b' AND k2 >= '1' AND k3 IS NULL) OR (k1 = 'a' AND k2 >= '1' AND k3 = '1')",
          true }, };
    List<String> nullable = Arrays.asList(null, "1", "y");
    List<String> failures = new ArrayList<>();
    // Each shape gives the key columns, the values of each key column, and the cases.
    Object[][] shapes = {
      { "k1 VARCHAR NOT NULL, k2 VARCHAR, k3 VARCHAR, k4 VARCHAR",
        Arrays.asList(Arrays.asList("a", "b"), Arrays.asList(null, "1", "2"), nullable, nullable),
        fourKeys },
      { "k1 VARCHAR NOT NULL, k2 VARCHAR NOT NULL, k3 VARCHAR, k4 VARCHAR",
        Arrays.asList(Arrays.asList("a", "b"), Arrays.asList("1", "2"), nullable, nullable),
        fourKeys },
      { "k1 VARCHAR NOT NULL, k2 VARCHAR, k3 VARCHAR",
        Arrays.asList(Arrays.asList("a", "b", "c", "d"), Arrays.asList(null, "1", "2"), nullable),
        threeKeys }, };
    for (Object[] shape : shapes) {
      String columns = (String) shape[0];
      @SuppressWarnings("unchecked")
      List<List<String>> values = (List<List<String>>) shape[1];
      int n = values.size();
      for (int orders = 0; orders < (1 << n); orders++) {
        StringBuilder pk = new StringBuilder();
        StringBuilder names = new StringBuilder();
        StringBuilder binds = new StringBuilder();
        for (int i = 0; i < n; i++) {
          boolean desc = (orders & (1 << (n - 1 - i))) != 0;
          pk.append(i > 0 ? ", " : "").append("k").append(i + 1).append(desc ? " DESC" : "");
          names.append("k").append(i + 1).append(", ");
          binds.append("?, ");
        }
        for (String options : new String[] { "", " SALT_BUCKETS=4" }) {
          try (Connection conn = DriverManager.getConnection(getUrl())) {
            String tableName = generateUniqueName();
            conn.createStatement().execute("CREATE TABLE " + tableName + " (" + columns
              + ", v VARCHAR CONSTRAINT pk PRIMARY KEY (" + pk + "))" + options);
            PreparedStatement upsert = conn.prepareStatement(
              "UPSERT INTO " + tableName + " (" + names + "v) VALUES (" + binds + "'x')");
            List<Row> rows = new ArrayList<>();
            int[] index = new int[n];
            int i;
            do {
              String[] row = new String[n];
              for (int d = 0; d < n; d++) {
                row[d] = values.get(d).get(index[d]);
                upsert.setString(d + 1, row[d]);
              }
              upsert.execute();
              Iterator<Pair<byte[], List<Cell>>> it =
                PhoenixRuntime.getUncommittedDataIterator(conn);
              // Row keeps three key values. The third value holds the later key columns.
              String rest = Arrays.toString(Arrays.copyOfRange(row, 2, n));
              rows.add(new Row(row[0], row[1], rest, it.next().getSecond().get(0)));
              conn.rollback();
              for (i = n - 1; i >= 0 && ++index[i] == values.get(i).size(); i--) {
                index[i] = 0;
              }
            } while (i >= 0);
            rows.sort((x, y) -> Bytes.compareTo(x.key(), y.key()));
            for (Object[] c : (Object[][]) shape[2]) {
              if (!(Boolean) c[1] && pk.indexOf("k2 DESC") >= 0) {
                continue;
              }
              String where = (String) c[0];
              Set<String> expected = new TreeSet<>();
              Set<String> returned = new TreeSet<>();
              String oracle = where.replaceAll("\\bk(\\d)\\b", "(k$1 || '')");
              for (Row row : returnedRows(compile(conn, tableName, oracle), rows)) {
                expected.add(row.toString());
              }
              try {
                for (Row row : returnedRows(compile(conn, tableName, where), rows)) {
                  returned.add(row.toString());
                }
              } catch (RuntimeException e) {
                returned.add(e.toString());
              }
              if (expected.isEmpty() || !expected.equals(returned)) {
                failures.add("[" + columns + "; " + pk + options + "] " + where + " expected "
                  + expected + " returned " + returned);
              }
            }
          }
        }
      }
    }
    assertTrue(String.join("\n", failures), failures.isEmpty());
  }

  private static BiPredicate<String, String> in(String... values) {
    List<String> list = Arrays.asList(values);
    return (a, b) -> list.contains(a);
  }

  private static final class Row {
    final String k1;
    final String k2;
    final String k3;
    final Cell cell;

    Row(String k1, String k2, Cell cell) {
      this(k1, k2, null, cell);
    }

    Row(String k1, String k2, String k3, Cell cell) {
      this.k1 = k1;
      this.k2 = k2;
      this.k3 = k3;
      this.cell = cell;
    }

    byte[] key() {
      return CellUtil.cloneRow(cell);
    }

    @Override
    public String toString() {
      return "(" + k1 + ", " + k2 + (k3 == null ? "" : ", " + k3) + ")";
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
    admitted =
      scanRows(scan.getStartRow(), scan.getStopRow(), findSkipScanFilter(scan.getFilter()), rows);
    assertTrue("scan for '" + where + "' admits no rows; grid or predicate is wrong",
      !admitted.isEmpty());
    return admitted;
  }

  /** The rows between {@code start} and {@code stop} that the skip-scan filter includes. */
  private static List<Row> scanRows(byte[] start, byte[] stop, SkipScanFilter skipScan,
    List<Row> rows) {
    List<Row> admitted = new ArrayList<>();
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
    return admitted;
  }

  /** A predicate over a row of a table with the key columns k1, k2 and k3. */
  @FunctionalInterface
  private interface RowPredicate {
    boolean test(String k1, String k2, String k3);
  }

  /**
   * Asserts that the scan for {@code where} returns exactly the rows that {@code matches} accepts.
   * The grid has k1 and k2 in {@link #PREFIX_VALUES} or {'a', 'b'}, and k3 in {'y', 'z'}. When
   * {@code checkRegions} is true, the scan must also keep the region of each matching row.
   */
  private static StatementContext assertScanReturnsExactly(String pk, String options, String where,
    boolean checkRegions, RowPredicate matches) throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String tableName = generateUniqueName();
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (k1 VARCHAR NOT NULL, k2 VARCHAR NOT NULL,"
          + " k3 VARCHAR NOT NULL, v VARCHAR CONSTRAINT pk PRIMARY KEY (" + pk + "))" + options);
      List<String> grid = new ArrayList<>(PREFIX_VALUES);
      grid.addAll(AB);
      List<Row> rows = new ArrayList<>();
      PreparedStatement upsert = conn
        .prepareStatement("UPSERT INTO " + tableName + " (k1, k2, k3, v) VALUES (?, ?, ?, 'x')");
      for (String k1 : grid) {
        for (String k2 : grid) {
          for (String k3 : Arrays.asList("y", "z")) {
            upsert.setString(1, k1);
            upsert.setString(2, k2);
            upsert.setString(3, k3);
            upsert.execute();
            Iterator<Pair<byte[], List<Cell>>> it = PhoenixRuntime.getUncommittedDataIterator(conn);
            rows.add(new Row(k1, k2, k3, it.next().getSecond().get(0)));
            conn.rollback();
          }
        }
      }
      rows.sort((x, y) -> Bytes.compareTo(x.key(), y.key()));
      StatementContext context = compile(conn, tableName, where);
      Set<String> expected = new TreeSet<>();
      Set<String> returned = new TreeSet<>();
      for (Row row : rows) {
        if (matches.test(row.k1, row.k2, row.k3)) {
          expected.add(row.toString());
        }
      }
      for (Row row : returnedRows(context, rows)) {
        returned.add(row.toString());
      }
      assertEquals("[" + pk + options + "] " + where, expected, returned);
      for (Row row : rows) {
        if (checkRegions && expected.contains(row.toString())) {
          byte[] key = row.key();
          assertTrue("[" + pk + options + "] " + where + " prunes the region of " + row, context
            .getScanRanges().intersectRegion(key, ByteUtil.concat(key, new byte[] { 0 }), false));
        }
      }
      return context;
    }
  }

  private static StatementContext compile(Connection conn, String tableName, String where)
    throws SQLException {
    PhoenixPreparedStatement stmt = new PhoenixPreparedStatement(
      conn.unwrap(PhoenixConnection.class), "SELECT * FROM " + tableName + " WHERE " + where);
    return stmt.compileQuery().getContext();
  }

  /**
   * The rows the compiled scan returns after the residual filters. A salted scan runs once per
   * bucket, as the parallel scans do, each with its own copy of the skip-scan filter.
   */
  private static List<Row> returnedRows(StatementContext context, List<Row> rows) throws Exception {
    ScanRanges scanRanges = context.getScanRanges();
    List<Row> admitted = new ArrayList<>();
    if (scanRanges.isDegenerate()) {
      return admitted;
    }
    Scan scan = context.getScan();
    List<Filter> residual = new ArrayList<>();
    SkipScanFilter skipScan = splitFilters(scan.getFilter(), residual);
    if (scanRanges.isPointLookup()) {
      Set<String> keys = new HashSet<>();
      scanRanges.getPointLookupKeyIterator()
        .forEachRemaining(k -> keys.add(Bytes.toStringBinary(k.getLowerRange())));
      for (Row row : rows) {
        if (keys.contains(Bytes.toStringBinary(row.key()))) {
          admitted.add(row);
        }
      }
    } else if (scanRanges.isSalted()) {
      for (int bucket = 0; bucket < 256; bucket++) {
        byte b = (byte) bucket;
        List<Row> bucketRows = new ArrayList<>();
        for (Row row : rows) {
          if (row.key()[0] == b) {
            bucketRows.add(row);
          }
        }
        if (!bucketRows.isEmpty()) {
          admitted.addAll(scanRows(withBucket(scan.getStartRow(), b, b),
            withBucket(scan.getStopRow(), b, (byte) (bucket + 1)),
            skipScan == null ? null : SkipScanFilter.parseFrom(skipScan.toByteArray()),
            bucketRows));
        }
      }
    } else {
      admitted = scanRows(scan.getStartRow(), scan.getStopRow(), skipScan, rows);
    }
    List<Row> returned = new ArrayList<>();
    for (Row row : admitted) {
      if (passes(residual, row.cell)) {
        returned.add(row);
      }
    }
    return returned;
  }

  /** The scan boundary moved into {@code bucket}, or the bucket edge when it is unbound. */
  private static byte[] withBucket(byte[] key, byte bucket, byte edge) {
    if (key.length == 0) {
      return new byte[] { edge };
    }
    byte[] copy = key.clone();
    copy[0] = bucket;
    return copy;
  }

  private static boolean passes(List<Filter> filters, Cell cell) throws Exception {
    for (Filter f : filters) {
      f.reset();
      ReturnCode code = f.filterCell(cell);
      if (
        (code != ReturnCode.INCLUDE && code != ReturnCode.INCLUDE_AND_NEXT_COL) || f.filterRow()
      ) {
        return false;
      }
    }
    return true;
  }

  /** Returns the {@link SkipScanFilter} and adds every other filter to {@code residual}. */
  private static SkipScanFilter splitFilters(Filter filter, List<Filter> residual) {
    if (filter == null) {
      return null;
    }
    if (filter instanceof SkipScanFilter) {
      return (SkipScanFilter) filter;
    }
    if (filter instanceof FilterList) {
      SkipScanFilter found = null;
      for (Filter f : ((FilterList) filter).getFilters()) {
        SkipScanFilter s = splitFilters(f, residual);
        if (s != null) {
          found = s;
        }
      }
      return found;
    }
    residual.add(filter);
    return null;
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
