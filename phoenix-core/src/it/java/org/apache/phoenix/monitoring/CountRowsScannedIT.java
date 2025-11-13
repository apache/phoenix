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
package org.apache.phoenix.monitoring;

import static org.apache.phoenix.query.QueryServices.USE_BLOOMFILTER_FOR_MULTIKEY_POINTLOOKUP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

@Category(NeedsOwnMiniClusterTest.class)
public class CountRowsScannedIT extends BaseTest {

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> props = Maps.newHashMapWithExpectedSize(2);
    props.put(QueryServices.COLLECT_REQUEST_LEVEL_METRICS, "true");
    // force many rpc calls
    props.put(QueryServices.SCAN_CACHE_SIZE_ATTRIB, "10");
    setUpTestDriver(new ReadOnlyProps(props));
  }

  @Test
  public void testSinglePrimaryKey() throws Exception {
    Connection conn = DriverManager.getConnection(getUrl());
    String tableName = generateUniqueName();
    PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
    stmt.execute(
      "CREATE TABLE " + tableName + " (A UNSIGNED_LONG NOT NULL PRIMARY KEY, Z UNSIGNED_LONG)");
    for (int i = 1; i <= 100; i++) {
      String sql = String.format("UPSERT INTO %s VALUES (%d, %d)", tableName, i, i);
      stmt.execute(sql);
    }
    conn.commit();

    // both columns, but primary key 3 to 100, 98 rows
    long count1 =
      countRowsScannedFromSql(stmt, "SELECT A,Z FROM " + tableName + " WHERE A >= 3 AND Z >= 7");
    assertEquals(98, count1);

    // primary key, 3 to 100, 98 rows
    long count2 = countRowsScannedFromSql(stmt, "SELECT A,Z FROM " + tableName + " WHERE A >= 3");
    assertEquals(98, count2);

    // non-primary key, all rows
    long count3 = countRowsScannedFromSql(stmt, "SELECT A,Z FROM " + tableName + " WHERE Z >= 7");
    assertEquals(100, count3);

    // primary key with limit, the first row
    long count4 =
      countRowsScannedFromSql(stmt, "SELECT A,Z FROM " + tableName + " WHERE A >= 3 limit 1");
    assertEquals(1, count4);

    // non-primary key with limit, find the first Z >= 7, 1 to 7, 7 rows
    long count5 =
      countRowsScannedFromSql(stmt, "SELECT A,Z FROM " + tableName + " WHERE Z >= 7 limit 1");
    assertEquals(7, count5);

    // primary key with order by primary and limit
    long count6 = countRowsScannedFromSql(stmt,
      "SELECT A,Z FROM " + tableName + " WHERE A >= 3 ORDER BY A limit 1");
    assertEquals(1, count6);

    // primary key with order by non-primary and limit
    long count7 = countRowsScannedFromSql(stmt,
      "SELECT A,Z FROM " + tableName + " WHERE A >= 3 ORDER BY Z limit 1");
    assertEquals(98, count7);

    // select non-primary key with order by primary limit
    long count8 = countRowsScannedFromSql(stmt,
      "SELECT A,Z FROM " + tableName + " WHERE Z >= 7 ORDER BY A limit 1");
    assertEquals(7, count8);

    // select non-primary key with order by primary limit desc
    // scan from the last, 1 row
    long count9 = countRowsScannedFromSql(stmt,
      "SELECT A,Z FROM " + tableName + " WHERE Z >= 7 ORDER BY A desc limit 1");
    assertEquals(1, count9);

    // select non-primary key with order by primary limit desc
    // scan from the last, 1 row
    long count10 = countRowsScannedFromSql(stmt,
      "SELECT A,Z FROM " + tableName + " WHERE Z >= 7 AND Z <= 60 ORDER BY A desc limit 1");
    assertEquals(41, count10);

    // select non-primary key with order by primary limit
    long count11 = countRowsScannedFromSql(stmt,
      "SELECT A,Z FROM " + tableName + " WHERE Z >= 7 ORDER BY Z limit 1");
    assertEquals(100, count11);

    // skip scan
    long count12 =
      countRowsScannedFromSql(stmt, "SELECT A,Z FROM " + tableName + " WHERE A in (20, 45, 68, 3)");
    assertEquals(7, count12); // 2n - 1 where n is number of keys

    // Query again but this time enable bloom filters for multi key point lookup
    String ddl = String.format("alter table %s set \"%s\" = true", tableName,
      USE_BLOOMFILTER_FOR_MULTIKEY_POINTLOOKUP);
    conn.createStatement().execute(ddl);
    long count13 =
      countRowsScannedFromSql(stmt, "SELECT A,Z FROM " + tableName + " WHERE A in (20, 45, 68, 3)");
    assertEquals(4, count13);
  }

  @Test
  public void testMultiPrimaryKeys() throws Exception {
    Connection conn = DriverManager.getConnection(getUrl());
    String tableName = generateUniqueName();
    PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
    stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " + tableName
      + " (A UNSIGNED_LONG NOT NULL, B UNSIGNED_LONG NOT NULL, "
      + " Z UNSIGNED_LONG, CONSTRAINT pk PRIMARY KEY (A, B))");
    for (int i = 1; i <= 100; i++) {
      String sql =
        String.format("UPSERT INTO %s VALUES (%d, %d, %d)", tableName, (i % 5) + 1, i, i);
      stmt.execute(sql);
    }
    conn.commit();

    // pk1 and pk2, only needed rows are scanned
    long count1 =
      countRowsScannedFromSql(stmt, "SELECT A,B,Z FROM " + tableName + " WHERE A >= 2 AND B >= 3");
    assertEquals(79, count1);

    // pk2, all rows
    long count2 = countRowsScannedFromSql(stmt, "SELECT A,B,Z FROM " + tableName + " WHERE B >= 3");
    assertEquals(100, count2);

    // non-pk, all rows
    long count3 = countRowsScannedFromSql(stmt, "SELECT A,B,Z FROM " + tableName + " WHERE Z >= 7");
    assertEquals(100, count3);

    // non group aggregate, pk2 only, all rows
    long count4 =
      countRowsScannedFromSql(stmt, "SELECT SUM(A) FROM " + tableName + " WHERE B >= 3");
    assertEquals(100, count4);

    // pk1 and pk2, group by
    long count5 = countRowsScannedFromSql(stmt,
      "SELECT B, SUM(A), SUM(Z) FROM " + tableName + " WHERE A >= 2 AND B >= 3 GROUP BY B");
    assertEquals(79, count5);

    // pk1 and pk2, group by, ordered
    long count6 = countRowsScannedFromSql(stmt, "SELECT B, SUM(A), SUM(Z) FROM " + tableName
      + " WHERE A >= 2 AND B >= 3 GROUP BY B ORDER BY B DESC");
    assertEquals(79, count6);
  }

  @Test
  public void testQueryWithDeleteMarkers() throws Exception {
    Connection conn = DriverManager.getConnection(getUrl());
    String tableName = generateUniqueName();
    PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
    stmt.execute(
      "CREATE TABLE " + tableName + " (A UNSIGNED_LONG NOT NULL PRIMARY KEY, Z UNSIGNED_LONG)");
    for (int i = 1; i <= 100; i++) {
      String sql = String.format("UPSERT INTO %s VALUES (%d, %d)", tableName, i, i);
      stmt.execute(sql);
    }
    conn.commit();
    String selectQuery = "SELECT A,Z FROM " + tableName + " LIMIT 1";
    for (int i = 10; i <= 100; i = i + 10) {
      stmt.execute("DELETE FROM " + tableName + " WHERE A < " + i);
      conn.commit();
      long count = countRowsScannedFromSql(stmt, selectQuery);
      assertEquals(i, count);
    }
  }

  @Test
  public void testQueryIndex() throws Exception {
    Connection conn = DriverManager.getConnection(getUrl());
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();
    PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
    stmt.execute(
      "CREATE TABLE " + tableName + " (A UNSIGNED_LONG NOT NULL PRIMARY KEY, Z UNSIGNED_LONG)");
    stmt.execute("CREATE INDEX " + indexName + " ON " + tableName + "(Z) INCLUDE (A)");
    for (int i = 1; i <= 100; i++) {
      String sql = String.format("UPSERT INTO %s VALUES (%d, %d)", tableName, i, i);
      stmt.execute(sql);
    }
    conn.commit();
    String selectQuery = "SELECT A FROM " + tableName + " WHERE Z > 49 AND Z < 71";
    long count = countRowsScannedFromSql(stmt, selectQuery);
    assertEquals(21, count);
    Assert.assertEquals(indexName,
      stmt.getQueryPlan().getTableRef().getTable().getTableName().toString());
  }

  @Test
  public void testQueryUncoveredIndex() throws Exception {
    Connection conn = DriverManager.getConnection(getUrl());
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();
    PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
    stmt.execute(
      "CREATE TABLE " + tableName + " (A UNSIGNED_LONG NOT NULL PRIMARY KEY, Z UNSIGNED_LONG)");
    stmt.execute("CREATE UNCOVERED INDEX " + indexName + " ON " + tableName + "(Z)");
    for (int i = 1; i <= 100; i++) {
      String sql = String.format("UPSERT INTO %s VALUES (%d, %d)", tableName, i, i);
      stmt.execute(sql);
    }
    conn.commit();
    String selectQuery = "SELECT A FROM " + tableName + " WHERE Z > 34 AND Z < 63";
    long count = countRowsScannedFromSql(stmt, selectQuery);
    assertEquals(28, count);
    Assert.assertEquals(indexName,
      stmt.getQueryPlan().getTableRef().getTable().getTableName().toString());
  }

  @Test
  public void testJoin() throws Exception {
    Connection conn = DriverManager.getConnection(getUrl());
    String tableName1 = generateUniqueName();
    String tableName2 = generateUniqueName();
    PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
    stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " + tableName1
      + " (A UNSIGNED_LONG NOT NULL, B UNSIGNED_LONG NOT NULL, "
      + " Z UNSIGNED_LONG, CONSTRAINT pk PRIMARY KEY (A, B))");
    stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " + tableName2
      + " (A UNSIGNED_LONG NOT NULL, B UNSIGNED_LONG NOT NULL, "
      + " Z UNSIGNED_LONG, CONSTRAINT pk PRIMARY KEY (A, B))");
    // table1.B = table2.A
    for (int i = 1; i <= 100; i++) {
      // table1.B in [51, 150], table2.A in [1, 100]
      String sql1 = String.format("UPSERT INTO %s VALUES (%d, %d, %d)", tableName1, i, i + 50, i);
      stmt.execute(sql1);
      String sql2 = String.format("UPSERT INTO %s VALUES (%d, %d, %d)", tableName2, i, i, i);
      stmt.execute(sql2);
    }

    conn.commit();

    // table1
    long count1 = countRowsScannedFromSql(stmt, "SELECT * FROM " + tableName1 + " WHERE A >= 40");
    assertEquals(61, count1);

    // table2, all rows
    long count2 = countRowsScannedFromSql(stmt, "SELECT * FROM " + tableName2 + " WHERE B >= 20");
    assertEquals(100, count2);

    // join
    String sqlJoin = "SELECT X.K, X.VX, Y.VY FROM ( SELECT B AS K, A AS VX FROM " + tableName1
      + " WHERE A >= 40) X JOIN (SELECT A AS K, B AS VY FROM " + tableName2
      + " WHERE B >= 20) Y ON X.K=Y.K";
    long count3 = countRowsScannedFromSql(stmt, sqlJoin);
    assertEquals(161, count3);
  }

  @Test
  public void testUnionAll() throws Exception {
    Connection conn = DriverManager.getConnection(getUrl());
    String tableName1 = generateUniqueName();
    String tableName2 = generateUniqueName();
    PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
    stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " + tableName1
      + " (A UNSIGNED_LONG NOT NULL, Z UNSIGNED_LONG, CONSTRAINT pk PRIMARY KEY (A))");
    stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " + tableName2
      + " (B UNSIGNED_LONG NOT NULL, Z UNSIGNED_LONG, CONSTRAINT pk PRIMARY KEY (B))");
    for (int i = 1; i <= 100; i++) {
      String sql1 = String.format("UPSERT INTO %s VALUES (%d, %d)", tableName1, i, i);
      stmt.execute(sql1);
      String sql2 = String.format("UPSERT INTO %s VALUES (%d, %d)", tableName2, i, i);
      stmt.execute(sql2);
    }

    conn.commit();

    // table1
    long count1 =
      countRowsScannedFromSql(stmt, "SELECT A, Z FROM " + tableName1 + " WHERE A >= 40");
    assertEquals(61, count1);

    // table2, all rows
    long count2 =
      countRowsScannedFromSql(stmt, "SELECT B, Z FROM " + tableName2 + " WHERE B >= 20");
    assertEquals(81, count2);

    // union all
    String sqlUnionAll = "SELECT SUM(Z) FROM ( SELECT Z FROM " + tableName1
      + " WHERE A >= 40 UNION ALL SELECT Z FROM " + tableName2 + " WHERE B >= 20)";
    long count3 = countRowsScannedFromSql(stmt, sqlUnionAll);
    assertEquals(142, count3);

    // union all then group by
    String sqlUnionAllGroupBy = "SELECT K, SUM(Z) FROM ( SELECT A AS K, Z FROM " + tableName1
      + " WHERE A >= 40 UNION ALL SELECT B AS K, Z FROM " + tableName2
      + " WHERE B >= 20) GROUP BY K";
    long count4 = countRowsScannedFromSql(stmt, sqlUnionAllGroupBy);
    assertEquals(142, count4);
  }

  @Test
  public void testLimitOffsetWithoutSplit() throws Exception {
    final String tablename = generateUniqueName();
    final String[] STRINGS = { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n",
      "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z" };
    String ddl = "CREATE TABLE " + tablename + " (t_id VARCHAR NOT NULL,\n"
      + "k1 INTEGER NOT NULL,\n" + "k2 INTEGER NOT NULL,\n" + "C3.k3 INTEGER,\n"
      + "C2.v1 VARCHAR,\n" + "CONSTRAINT pk PRIMARY KEY (t_id, k1, k2))";
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createTestTable(getUrl(), ddl);
      for (int i = 0; i < 26; i++) {
        conn.createStatement().execute("UPSERT INTO " + tablename + " values('" + STRINGS[i] + "',"
          + i + "," + (i + 1) + "," + (i + 2) + ",'" + STRINGS[25 - i] + "')");
      }
      conn.commit();
      int limit = 12;
      int offset = 5;
      ResultSet rs;
      rs = conn.createStatement().executeQuery(
        "SELECT t_id from " + tablename + " order by t_id limit " + limit + " offset " + offset);
      int i = 0;
      while (i < limit) {
        assertTrue(rs.next());
        assertEquals("Expected string didn't match for i = " + i, STRINGS[offset + i],
          rs.getString(1));
        i++;
      }
      assertEquals(limit + offset, getRowsScanned(rs));
    }
  }

  private long countRowsScannedFromSql(Statement stmt, String sql) throws SQLException {
    ResultSet rs = stmt.executeQuery(sql);
    while (rs.next()) {
      // loop to the end
    }
    return getRowsScanned(rs);
  }

  private long getRowsScanned(ResultSet rs) throws SQLException {
    if (!(rs instanceof PhoenixResultSet)) {
      return -1;
    }
    Map<String, Map<MetricType, Long>> metrics = PhoenixRuntime.getRequestReadMetricInfo(rs);

    long sum = 0;
    boolean valid = false;
    for (Map.Entry<String, Map<MetricType, Long>> entry : metrics.entrySet()) {
      Long val = entry.getValue().get(MetricType.COUNT_ROWS_SCANNED);
      if (val != null) {
        sum += val.longValue();
        valid = true;
      }
    }
    if (valid) {
      return sum;
    } else {
      return -1;
    }
  }
}
