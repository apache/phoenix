/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.end2end;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class DescOrderKeysIT extends ParallelStatsDisabledIT {

  @Test
  public void testVarCharDescOrderPKs() throws Exception {
    final String tableName = generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(true);
      final Statement stmt = conn.createStatement();

      stmt.execute("CREATE TABLE " + tableName +
        " (COL1 VARCHAR, COL2 VARCHAR CONSTRAINT PK PRIMARY KEY (COL1 DESC, COL2)) ");

      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2) VALUES ('h1uniq1', 'val1')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2) VALUES ('41efh', 'val2')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2) VALUES ('c49ghd', 'val3')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2) VALUES ('4232jfjg', 'val4')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2) VALUES ('zsw4tg', 'val5')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2) VALUES ('93hgwef', 'val6')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2) VALUES ('3jfytw', 'val7')");

      final String sql = "select COL1, COL2 from " + tableName;

      ResultSet rs = stmt.executeQuery(sql);
      Assert.assertTrue(rs.next());

      QueryPlan plan = stmt.unwrap(PhoenixStatement.class).getQueryPlan();
      Assert.assertEquals("Expected a single scan ", 1, plan.getScans().size());
      Assert.assertEquals("Expected a single scan ", 1, plan.getScans().get(0).size());

      Assert.assertEquals("zsw4tg", rs.getString(1));
      Assert.assertEquals("val5", rs.getString(2));
      Assert.assertTrue(rs.next());

      Assert.assertEquals("h1uniq1", rs.getString(1));
      Assert.assertEquals("val1", rs.getString(2));
      Assert.assertTrue(rs.next());

      Assert.assertEquals("c49ghd", rs.getString(1));
      Assert.assertEquals("val3", rs.getString(2));
      Assert.assertTrue(rs.next());

      Assert.assertEquals("93hgwef", rs.getString(1));
      Assert.assertEquals("val6", rs.getString(2));
      Assert.assertTrue(rs.next());

      Assert.assertEquals("4232jfjg", rs.getString(1));
      Assert.assertEquals("val4", rs.getString(2));
      Assert.assertTrue(rs.next());

      Assert.assertEquals("41efh", rs.getString(1));
      Assert.assertEquals("val2", rs.getString(2));
      Assert.assertTrue(rs.next());

      Assert.assertEquals("3jfytw", rs.getString(1));
      Assert.assertEquals("val7", rs.getString(2));
      Assert.assertFalse(rs.next());
    }
  }

  @Test
  public void testVarCharDescOrderMultiplePKs() throws Exception {
    final String tableName = generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(true);
      final Statement stmt = conn.createStatement();

      stmt.execute("CREATE TABLE " + tableName +
        " (COL1 VARCHAR, COL2 VARCHAR, COL3 VARCHAR CONSTRAINT " +
        "PK PRIMARY KEY (COL1 DESC, COL2 DESC, COL3)) ");

      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2, COL3) VALUES " +
        "('h1uniq1', 'key1', 'val1')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2, COL3) VALUES " +
        "('41efh', 'key2', 'val2')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2, COL3) VALUES " +
        "('c49ghd', 'key3', 'val3')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2, COL3) VALUES " +
        "('zsw4tg', 'key5', 'val5')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2, COL3) VALUES " +
        "('zsw4tg', 'key4', 'val4')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2, COL3) VALUES " +
        "('h1uniq1', 'key6', 'val6')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2, COL3) VALUES " +
        "('93hgwef', 'key7', 'val7')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2, COL3) VALUES " +
        "('3jfytw', 'key8', 'val8')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2, COL3) VALUES " +
        "('4232jfjg', 'key9', 'val9')");

      final String sql = "select COL1, COL2, COL3 from " + tableName;

      ResultSet rs = stmt.executeQuery(sql);
      Assert.assertTrue(rs.next());

      QueryPlan plan = stmt.unwrap(PhoenixStatement.class).getQueryPlan();
      Assert.assertEquals("Expected a single scan ", 1, plan.getScans().size());
      Assert.assertEquals("Expected a single scan ", 1, plan.getScans().get(0).size());

      Assert.assertEquals("zsw4tg", rs.getString(1));
      Assert.assertEquals("key5", rs.getString(2));
      Assert.assertEquals("val5", rs.getString(3));
      Assert.assertTrue(rs.next());

      Assert.assertEquals("zsw4tg", rs.getString(1));
      Assert.assertEquals("key4", rs.getString(2));
      Assert.assertEquals("val4", rs.getString(3));
      Assert.assertTrue(rs.next());

      Assert.assertEquals("h1uniq1", rs.getString(1));
      Assert.assertEquals("key6", rs.getString(2));
      Assert.assertEquals("val6", rs.getString(3));
      Assert.assertTrue(rs.next());

      Assert.assertEquals("h1uniq1", rs.getString(1));
      Assert.assertEquals("key1", rs.getString(2));
      Assert.assertEquals("val1", rs.getString(3));
      Assert.assertTrue(rs.next());

      Assert.assertEquals("c49ghd", rs.getString(1));
      Assert.assertEquals("key3", rs.getString(2));
      Assert.assertEquals("val3", rs.getString(3));
      Assert.assertTrue(rs.next());

      Assert.assertEquals("93hgwef", rs.getString(1));
      Assert.assertEquals("key7", rs.getString(2));
      Assert.assertEquals("val7", rs.getString(3));
      Assert.assertTrue(rs.next());

      Assert.assertEquals("4232jfjg", rs.getString(1));
      Assert.assertEquals("key9", rs.getString(2));
      Assert.assertEquals("val9", rs.getString(3));
      Assert.assertTrue(rs.next());

      Assert.assertEquals("41efh", rs.getString(1));
      Assert.assertEquals("key2", rs.getString(2));
      Assert.assertEquals("val2", rs.getString(3));
      Assert.assertTrue(rs.next());

      Assert.assertEquals("3jfytw", rs.getString(1));
      Assert.assertEquals("key8", rs.getString(2));
      Assert.assertEquals("val8", rs.getString(3));
      Assert.assertFalse(rs.next());
    }
  }

  @Test
  public void testIntDescOrderMultiplePKs() throws Exception {
    final String tableName = generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(true);
      final Statement stmt = conn.createStatement();

      stmt.execute("CREATE TABLE " + tableName +
        " (COL1 INTEGER NOT NULL, COL2 INTEGER NOT NULL, COL3 VARCHAR NOT NULL CONSTRAINT " +
        "PK PRIMARY KEY (COL1 DESC, COL2 DESC, COL3)) ");

      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2, COL3) VALUES " +
        "(1234, 3957, 'val1')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2, COL3) VALUES " +
        "(2453, 234, 'val2')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2, COL3) VALUES " +
        "(3463, 345561, 'val3')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2, COL3) VALUES " +
        "(34534, 345657, 'val4')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2, COL3) VALUES " +
        "(2453, 92374, 'val5')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2, COL3) VALUES " +
        "(9375, 11037, 'val6')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2, COL3) VALUES " +
        "(9375, 455, 'val7')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2, COL3) VALUES " +
        "(9375, 7712, 'val8')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2, COL3) VALUES " +
        "(1234, 3956, 'val9')");

      final String sql = "select COL1, COL2, COL3 from " + tableName;

      ResultSet rs = stmt.executeQuery(sql);
      Assert.assertTrue(rs.next());

      QueryPlan plan = stmt.unwrap(PhoenixStatement.class).getQueryPlan();
      Assert.assertEquals("Expected a single scan ", 1, plan.getScans().size());
      Assert.assertEquals("Expected a single scan ", 1, plan.getScans().get(0).size());

      Assert.assertEquals(34534, rs.getInt(1));
      Assert.assertEquals(345657, rs.getInt(2));
      Assert.assertEquals("val4", rs.getString(3));
      Assert.assertTrue(rs.next());

      Assert.assertEquals(9375, rs.getInt(1));
      Assert.assertEquals(11037, rs.getInt(2));
      Assert.assertEquals("val6", rs.getString(3));
      Assert.assertTrue(rs.next());

      Assert.assertEquals(9375, rs.getInt(1));
      Assert.assertEquals(7712, rs.getInt(2));
      Assert.assertEquals("val8", rs.getString(3));
      Assert.assertTrue(rs.next());

      Assert.assertEquals(9375, rs.getInt(1));
      Assert.assertEquals(455, rs.getInt(2));
      Assert.assertEquals("val7", rs.getString(3));
      Assert.assertTrue(rs.next());

      Assert.assertEquals(3463, rs.getInt(1));
      Assert.assertEquals(345561, rs.getInt(2));
      Assert.assertEquals("val3", rs.getString(3));
      Assert.assertTrue(rs.next());

      Assert.assertEquals(2453, rs.getInt(1));
      Assert.assertEquals(92374, rs.getInt(2));
      Assert.assertEquals("val5", rs.getString(3));
      Assert.assertTrue(rs.next());

      Assert.assertEquals(2453, rs.getInt(1));
      Assert.assertEquals(234, rs.getInt(2));
      Assert.assertEquals("val2", rs.getString(3));
      Assert.assertTrue(rs.next());

      Assert.assertEquals(1234, rs.getInt(1));
      Assert.assertEquals(3957, rs.getInt(2));
      Assert.assertEquals("val1", rs.getString(3));
      Assert.assertTrue(rs.next());

      Assert.assertEquals(1234, rs.getInt(1));
      Assert.assertEquals(3956, rs.getInt(2));
      Assert.assertEquals("val9", rs.getString(3));
      Assert.assertFalse(rs.next());
    }
  }

  @Test
  public void testDoubleDescOrderMultiplePKs() throws Exception {
    final String tableName = generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(true);
      final Statement stmt = conn.createStatement();

      stmt.execute("CREATE TABLE " + tableName +
        " (COL1 DOUBLE NOT NULL, COL2 DOUBLE NOT NULL, COL3 VARCHAR NOT NULL CONSTRAINT " +
        "PK PRIMARY KEY (COL1 DESC, COL2 DESC, COL3)) ");

      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2, COL3) VALUES " +
        "(1234.39, 3957.124, 'val1')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2, COL3) VALUES " +
        "(2453.97, 234.112, 'val2')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2, COL3) VALUES " +
        "(3463.384, 345561.124, 'val3')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2, COL3) VALUES " +
        "(34534.9191, 345657.913, 'val4')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2, COL3) VALUES " +
        "(2453.89, 92374.11, 'val5')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2, COL3) VALUES " +
        "(9375.23, 11037.729, 'val6')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2, COL3) VALUES " +
        "(9375.23, 11037.8, 'val7')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2, COL3) VALUES " +
        "(9375.23, 11037.72888, 'val8')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2, COL3) VALUES " +
        "(1234.39, 3957.123999, 'val9')");

      final String sql = "select COL1, COL2, COL3 from " + tableName;

      ResultSet rs = stmt.executeQuery(sql);
      Assert.assertTrue(rs.next());

      QueryPlan plan = stmt.unwrap(PhoenixStatement.class).getQueryPlan();
      Assert.assertEquals("Expected a single scan ", 1, plan.getScans().size());
      Assert.assertEquals("Expected a single scan ", 1, plan.getScans().get(0).size());

      Assert.assertEquals(34534.9191, rs.getDouble(1), 0.0);
      Assert.assertEquals(345657.913, rs.getDouble(2), 0.0);
      Assert.assertEquals("val4", rs.getString(3));
      Assert.assertTrue(rs.next());

      Assert.assertEquals(9375.23, rs.getDouble(1), 0.0);
      Assert.assertEquals(11037.8, rs.getDouble(2), 0.0);
      Assert.assertEquals("val7", rs.getString(3));
      Assert.assertTrue(rs.next());

      Assert.assertEquals(9375.23, rs.getDouble(1), 0.0);
      Assert.assertEquals(11037.729, rs.getDouble(2), 0.0);
      Assert.assertEquals("val6", rs.getString(3));
      Assert.assertTrue(rs.next());

      Assert.assertEquals(9375.23, rs.getDouble(1), 0.0);
      Assert.assertEquals(11037.72888, rs.getDouble(2), 0.0);
      Assert.assertEquals("val8", rs.getString(3));
      Assert.assertTrue(rs.next());

      Assert.assertEquals(3463.384, rs.getDouble(1), 0.0);
      Assert.assertEquals(345561.124, rs.getDouble(2), 0.0);
      Assert.assertEquals("val3", rs.getString(3));
      Assert.assertTrue(rs.next());

      Assert.assertEquals(2453.97, rs.getDouble(1), 0.0);
      Assert.assertEquals(234.112, rs.getDouble(2), 0.0);
      Assert.assertEquals("val2", rs.getString(3));
      Assert.assertTrue(rs.next());

      Assert.assertEquals(2453.89, rs.getDouble(1), 0.0);
      Assert.assertEquals(92374.11, rs.getDouble(2), 0.0);
      Assert.assertEquals("val5", rs.getString(3));
      Assert.assertTrue(rs.next());

      Assert.assertEquals(1234.39, rs.getDouble(1), 0.0);
      Assert.assertEquals(3957.124, rs.getDouble(2), 0.0);
      Assert.assertEquals("val1", rs.getString(3));
      Assert.assertTrue(rs.next());

      Assert.assertEquals(1234.39, rs.getDouble(1), 0.0);
      Assert.assertEquals(3957.123999, rs.getDouble(2), 0.0);
      Assert.assertEquals("val9", rs.getString(3));
      Assert.assertFalse(rs.next());
    }
  }

  @Test
  public void testDecimalDescOrderMultiplePKs() throws Exception {
    final String tableName = generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(true);
      final Statement stmt = conn.createStatement();

      stmt.execute("CREATE TABLE " + tableName +
        " (COL1 DECIMAL NOT NULL, COL2 DECIMAL NOT NULL, COL3 VARCHAR NOT NULL CONSTRAINT " +
        "PK PRIMARY KEY (COL1 DESC, COL2 DESC, COL3)) ");

      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2, COL3) VALUES " +
        "(1234.39, 3957.124, 'val1')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2, COL3) VALUES " +
        "(2453.97, 234.112, 'val2')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2, COL3) VALUES " +
        "(3463.384, 345561.124, 'val3')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2, COL3) VALUES " +
        "(34534.9191, 345657.913, 'val4')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2, COL3) VALUES " +
        "(2453.89, 92374.11, 'val5')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2, COL3) VALUES " +
        "(9375.23, 11037.729, 'val6')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2, COL3) VALUES " +
        "(9375.23, 11037.8, 'val7')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2, COL3) VALUES " +
        "(9375.23, 11037.72888, 'val8')");
      stmt.execute("UPSERT INTO " + tableName + " (COL1, COL2, COL3) VALUES " +
        "(1234.39, 3957.123999, 'val9')");

      final String sql = "select COL1, COL2, COL3 from " + tableName;

      ResultSet rs = stmt.executeQuery(sql);
      Assert.assertTrue(rs.next());

      QueryPlan plan = stmt.unwrap(PhoenixStatement.class).getQueryPlan();
      Assert.assertEquals("Expected a single scan ", 1, plan.getScans().size());
      Assert.assertEquals("Expected a single scan ", 1, plan.getScans().get(0).size());

      Assert.assertEquals(BigDecimal.valueOf(34534.9191), rs.getBigDecimal(1));
      Assert.assertEquals(BigDecimal.valueOf(345657.913), rs.getBigDecimal(2));
      Assert.assertEquals("val4", rs.getString(3));
      Assert.assertTrue(rs.next());

      Assert.assertEquals(BigDecimal.valueOf(9375.23), rs.getBigDecimal(1));
      Assert.assertEquals(BigDecimal.valueOf(11037.8), rs.getBigDecimal(2));
      Assert.assertEquals("val7", rs.getString(3));
      Assert.assertTrue(rs.next());

      Assert.assertEquals(BigDecimal.valueOf(9375.23), rs.getBigDecimal(1));
      Assert.assertEquals(BigDecimal.valueOf(11037.729), rs.getBigDecimal(2));
      Assert.assertEquals("val6", rs.getString(3));
      Assert.assertTrue(rs.next());

      Assert.assertEquals(BigDecimal.valueOf(9375.23), rs.getBigDecimal(1));
      Assert.assertEquals(BigDecimal.valueOf(11037.72888), rs.getBigDecimal(2));
      Assert.assertEquals("val8", rs.getString(3));
      Assert.assertTrue(rs.next());

      Assert.assertEquals(BigDecimal.valueOf(3463.384), rs.getBigDecimal(1));
      Assert.assertEquals(BigDecimal.valueOf(345561.124), rs.getBigDecimal(2));
      Assert.assertEquals("val3", rs.getString(3));
      Assert.assertTrue(rs.next());

      Assert.assertEquals(BigDecimal.valueOf(2453.97), rs.getBigDecimal(1));
      Assert.assertEquals(BigDecimal.valueOf(234.112), rs.getBigDecimal(2));
      Assert.assertEquals("val2", rs.getString(3));
      Assert.assertTrue(rs.next());

      Assert.assertEquals(BigDecimal.valueOf(2453.89), rs.getBigDecimal(1));
      Assert.assertEquals(BigDecimal.valueOf(92374.11), rs.getBigDecimal(2));
      Assert.assertEquals("val5", rs.getString(3));
      Assert.assertTrue(rs.next());

      Assert.assertEquals(BigDecimal.valueOf(1234.39), rs.getBigDecimal(1));
      Assert.assertEquals(BigDecimal.valueOf(3957.124), rs.getBigDecimal(2));
      Assert.assertEquals("val1", rs.getString(3));
      Assert.assertTrue(rs.next());

      Assert.assertEquals(BigDecimal.valueOf(1234.39), rs.getBigDecimal(1));
      Assert.assertEquals(BigDecimal.valueOf(3957.123999), rs.getBigDecimal(2));
      Assert.assertEquals("val9", rs.getString(3));
      Assert.assertFalse(rs.next());
    }
  }

  @Test
  public void testViewWithIntDescPkColumn() throws Exception {
    final String tableName = generateUniqueName();
    final String view01 = "v01_" + tableName;
    final String view02 = "v02_" + tableName;

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      final Statement stmt = conn.createStatement();

      stmt.execute("CREATE TABLE " + tableName
              + " (COL1 VARCHAR(10) NOT NULL, COL2 CHAR(5) NOT NULL, COL3 VARCHAR,"
              + " COL4 VARCHAR CONSTRAINT pk PRIMARY KEY(COL1, COL2))");
      stmt.execute("CREATE VIEW " + view01
              + " (VCOL1 INTEGER NOT NULL, COL5 VARCHAR CONSTRAINT pk PRIMARY KEY(VCOL1 DESC))"
              + " AS SELECT * FROM " + tableName + " WHERE COL1 = 'col1'");
      stmt.execute("CREATE VIEW " + view02
              + " (VCOL2 CHAR(10) NOT NULL, COL6 VARCHAR CONSTRAINT pk PRIMARY KEY(VCOL2))"
              + " AS SELECT * FROM " + view01 + " WHERE VCOL1 = 1");

      stmt.execute("UPSERT INTO " + view02
              + " (col2, vcol2, col5, col6) values ('0001', 'vcol2_01', 'col5_01', 'col6_01')");
      stmt.execute("UPSERT INTO " + view02
              + " (col2, vcol2, col5, col6) values ('0002', 'vcol2_02', 'col5_02', 'col6_02')");
      stmt.execute("UPSERT INTO " + view02
              + " (col2, vcol2, col5, col6) values ('0003', 'vcol2_03', 'col5_03', 'col6_03')");
      stmt.execute("UPSERT INTO " + view01 + " (col2, vcol1, col3, col4, col5) values "
              + "('0004', 2, 'col3_04', 'col4_04', 'col5_04')");
      stmt.execute("UPSERT INTO " + view01 + " (col2, vcol1, col3, col4, col5) values "
              + "('0005', -2, 'col3_05', 'col4_05', 'col5_05')");
      stmt.execute("UPSERT INTO " + view01 + " (col2, vcol1, col3, col4, col5) values "
              + "('0006', -1, 'col3_06', 'col4_06', 'col5_06')");
      stmt.execute("UPSERT INTO " + view01 + " (col2, vcol1, col3, col4, col5) values "
              + "('0007', 1, 'col3_07', 'col4_07', 'col5_07')");
      conn.commit();

      ResultSet rs = stmt.executeQuery("SELECT COL1, COL2, VCOL1 FROM " + view01);
      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0001", rs.getString(2));
      Assert.assertEquals(1, rs.getInt(3));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0002", rs.getString(2));
      Assert.assertEquals(1, rs.getInt(3));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0003", rs.getString(2));
      Assert.assertEquals(1, rs.getInt(3));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0004", rs.getString(2));
      Assert.assertEquals(2, rs.getInt(3));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0005", rs.getString(2));
      Assert.assertEquals(-2, rs.getInt(3));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0006", rs.getString(2));
      Assert.assertEquals(-1, rs.getInt(3));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0007", rs.getString(2));
      Assert.assertEquals(1, rs.getInt(3));

      Assert.assertFalse(rs.next());

      rs = stmt.executeQuery("SELECT COL2, VCOL1, VCOL2, COL5, COL6 FROM " + view02);
      Assert.assertTrue(rs.next());

      Assert.assertEquals("0001", rs.getString(1));
      Assert.assertEquals(1, rs.getInt(2));
      Assert.assertEquals("vcol2_01", rs.getString(3));
      Assert.assertEquals("col5_01", rs.getString(4));
      Assert.assertEquals("col6_01", rs.getString(5));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("0002", rs.getString(1));
      Assert.assertEquals(1, rs.getInt(2));
      Assert.assertEquals("vcol2_02", rs.getString(3));
      Assert.assertEquals("col5_02", rs.getString(4));
      Assert.assertEquals("col6_02", rs.getString(5));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("0003", rs.getString(1));
      Assert.assertEquals(1, rs.getInt(2));
      Assert.assertEquals("vcol2_03", rs.getString(3));
      Assert.assertEquals("col5_03", rs.getString(4));
      Assert.assertEquals("col6_03", rs.getString(5));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("0007", rs.getString(1));
      Assert.assertEquals(1, rs.getInt(2));
      Assert.assertNull(rs.getString(3));
      Assert.assertEquals("col5_07", rs.getString(4));
      Assert.assertNull(rs.getString(5));

      Assert.assertFalse(rs.next());
    }
  }

  @Test
  public void testViewWithCharDescPkColumn() throws Exception {
    final String tableName = generateUniqueName();
    final String view01 = "v01_" + tableName;
    final String view02 = "v02_" + tableName;

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      final Statement stmt = conn.createStatement();

      stmt.execute("CREATE TABLE " + tableName
              + " (COL1 VARCHAR(10) NOT NULL, COL2 CHAR(5) NOT NULL, COL3 VARCHAR,"
              + " COL4 VARCHAR CONSTRAINT pk PRIMARY KEY(COL1, COL2))");
      stmt.execute("CREATE VIEW " + view01
              + " (VCOL1 CHAR(8) NOT NULL, COL5 VARCHAR CONSTRAINT pk PRIMARY KEY(VCOL1 DESC))"
              + " AS SELECT * FROM " + tableName + " WHERE COL1 = 'col1'");
      stmt.execute("CREATE VIEW " + view02
              + " (VCOL2 CHAR(10) NOT NULL, COL6 VARCHAR CONSTRAINT pk PRIMARY KEY(VCOL2))"
              + " AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'vcol1'");

      stmt.execute("UPSERT INTO " + view02
              + " (col2, vcol2, col5, col6) values ('0001', 'vcol2_01', 'col5_01', 'col6_01')");
      stmt.execute("UPSERT INTO " + view02
              + " (col2, vcol2, col5, col6) values ('0002', 'vcol2_02', 'col5_02', 'col6_02')");
      stmt.execute("UPSERT INTO " + view02
              + " (col2, vcol2, col5, col6) values ('0003', 'vcol2_03', 'col5_03', 'col6_03')");
      stmt.execute("UPSERT INTO " + view01 + " (col2, vcol1, col3, col4, col5) values "
              + "('0004', 'vcol2', 'col3_04', 'col4_04', 'col5_04')");
      stmt.execute("UPSERT INTO " + view01 + " (col2, vcol1, col3, col4, col5) values "
              + "('0005', 'vcol-2', 'col3_05', 'col4_05', 'col5_05')");
      stmt.execute("UPSERT INTO " + view01 + " (col2, vcol1, col3, col4, col5) values "
              + "('0006', 'vcol-1', 'col3_06', 'col4_06', 'col5_06')");
      stmt.execute("UPSERT INTO " + view01 + " (col2, vcol1, col3, col4, col5) values "
              + "('0007', 'vcol1', 'col3_07', 'col4_07', 'col5_07')");
      conn.commit();

      ResultSet rs = stmt.executeQuery("SELECT COL1, COL2, VCOL1 FROM " + view01);
      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0001", rs.getString(2));
      Assert.assertEquals("vcol1", rs.getString(3));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0002", rs.getString(2));
      Assert.assertEquals("vcol1", rs.getString(3));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0003", rs.getString(2));
      Assert.assertEquals("vcol1", rs.getString(3));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0004", rs.getString(2));
      Assert.assertEquals("vcol2", rs.getString(3));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0005", rs.getString(2));
      Assert.assertEquals("vcol-2", rs.getString(3));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0006", rs.getString(2));
      Assert.assertEquals("vcol-1", rs.getString(3));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0007", rs.getString(2));
      Assert.assertEquals("vcol1", rs.getString(3));

      Assert.assertFalse(rs.next());

      rs = stmt.executeQuery("SELECT COL2, VCOL1, VCOL2, COL5, COL6 FROM " + view02);
      Assert.assertTrue(rs.next());

      Assert.assertEquals("0001", rs.getString(1));
      Assert.assertEquals("vcol1", rs.getString(2));
      Assert.assertEquals("vcol2_01", rs.getString(3));
      Assert.assertEquals("col5_01", rs.getString(4));
      Assert.assertEquals("col6_01", rs.getString(5));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("0002", rs.getString(1));
      Assert.assertEquals("vcol1", rs.getString(2));
      Assert.assertEquals("vcol2_02", rs.getString(3));
      Assert.assertEquals("col5_02", rs.getString(4));
      Assert.assertEquals("col6_02", rs.getString(5));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("0003", rs.getString(1));
      Assert.assertEquals("vcol1", rs.getString(2));
      Assert.assertEquals("vcol2_03", rs.getString(3));
      Assert.assertEquals("col5_03", rs.getString(4));
      Assert.assertEquals("col6_03", rs.getString(5));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("0007", rs.getString(1));
      Assert.assertEquals("vcol1", rs.getString(2));
      Assert.assertNull(rs.getString(3));
      Assert.assertEquals("col5_07", rs.getString(4));
      Assert.assertNull(rs.getString(5));

      Assert.assertFalse(rs.next());

      rs = stmt.executeQuery("SELECT COL1, COL2, VCOL1 FROM " + view01 + " ORDER BY "
              + "VCOL1 DESC, COL2 ASC");

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0004", rs.getString(2));
      Assert.assertEquals("vcol2", rs.getString(3));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0001", rs.getString(2));
      Assert.assertEquals("vcol1", rs.getString(3));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0002", rs.getString(2));
      Assert.assertEquals("vcol1", rs.getString(3));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0003", rs.getString(2));
      Assert.assertEquals("vcol1", rs.getString(3));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0007", rs.getString(2));
      Assert.assertEquals("vcol1", rs.getString(3));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0005", rs.getString(2));
      Assert.assertEquals("vcol-2", rs.getString(3));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0006", rs.getString(2));
      Assert.assertEquals("vcol-1", rs.getString(3));

      Assert.assertFalse(rs.next());

      rs = stmt.executeQuery("SELECT COL2, VCOL1, VCOL2, COL5, COL6 FROM " + view02
              + " ORDER BY VCOL2 DESC, VCOL1 DESC");

      Assert.assertTrue(rs.next());

      Assert.assertEquals("0007", rs.getString(1));
      Assert.assertEquals("vcol1", rs.getString(2));
      Assert.assertNull(rs.getString(3));
      Assert.assertEquals("col5_07", rs.getString(4));
      Assert.assertNull(rs.getString(5));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("0003", rs.getString(1));
      Assert.assertEquals("vcol1", rs.getString(2));
      Assert.assertEquals("vcol2_03", rs.getString(3));
      Assert.assertEquals("col5_03", rs.getString(4));
      Assert.assertEquals("col6_03", rs.getString(5));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("0002", rs.getString(1));
      Assert.assertEquals("vcol1", rs.getString(2));
      Assert.assertEquals("vcol2_02", rs.getString(3));
      Assert.assertEquals("col5_02", rs.getString(4));
      Assert.assertEquals("col6_02", rs.getString(5));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("0001", rs.getString(1));
      Assert.assertEquals("vcol1", rs.getString(2));
      Assert.assertEquals("vcol2_01", rs.getString(3));
      Assert.assertEquals("col5_01", rs.getString(4));
      Assert.assertEquals("col6_01", rs.getString(5));

      Assert.assertFalse(rs.next());
    }
  }

  @Test
  public void testViewWithDoubleDescPkColumn() throws Exception {
    final String tableName = generateUniqueName();
    final String view01 = "v01_" + tableName;
    final String view02 = "v02_" + tableName;

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      final Statement stmt = conn.createStatement();

      stmt.execute("CREATE TABLE " + tableName
              + " (COL1 VARCHAR(10) NOT NULL, COL2 CHAR(5) NOT NULL, COL3 VARCHAR,"
              + " COL4 VARCHAR CONSTRAINT pk PRIMARY KEY(COL1, COL2))");
      stmt.execute("CREATE VIEW " + view01
              + " (VCOL1 DOUBLE NOT NULL, COL5 VARCHAR CONSTRAINT pk PRIMARY KEY(VCOL1 DESC))"
              + " AS SELECT * FROM " + tableName + " WHERE COL1 = 'col1'");
      stmt.execute("CREATE VIEW " + view02
              + " (VCOL2 CHAR(10) NOT NULL, COL6 VARCHAR CONSTRAINT pk PRIMARY KEY(VCOL2))"
              + " AS SELECT * FROM " + view01 + " WHERE VCOL1 = 234.75");

      stmt.execute("UPSERT INTO " + view02
              + " (col2, vcol2, col5, col6) values ('0001', 'vcol2_01', 'col5_01', 'col6_01')");
      stmt.execute("UPSERT INTO " + view02
              + " (col2, vcol2, col5, col6) values ('0002', 'vcol2_02', 'col5_02', 'col6_02')");
      stmt.execute("UPSERT INTO " + view02
              + " (col2, vcol2, col5, col6) values ('0003', 'vcol2_03', 'col5_03', 'col6_03')");
      stmt.execute("UPSERT INTO " + view01 + " (col2, vcol1, col3, col4, col5) values "
              + "('0004', 236.49, 'col3_04', 'col4_04', 'col5_04')");
      stmt.execute("UPSERT INTO " + view01 + " (col2, vcol1, col3, col4, col5) values "
              + "('0005', 17.053, 'col3_05', 'col4_05', 'col5_05')");
      stmt.execute("UPSERT INTO " + view01 + " (col2, vcol1, col3, col4, col5) values "
              + "('0006', 98.8452, 'col3_06', 'col4_06', 'col5_06')");
      stmt.execute("UPSERT INTO " + view01 + " (col2, vcol1, col3, col4, col5) values "
              + "('0007', 234.75, 'col3_07', 'col4_07', 'col5_07')");
      conn.commit();

      ResultSet rs = stmt.executeQuery("SELECT COL1, COL2, VCOL1 FROM " + view01);
      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0001", rs.getString(2));
      Assert.assertEquals(234.75, rs.getDouble(3), 0);

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0002", rs.getString(2));
      Assert.assertEquals(234.75, rs.getDouble(3), 0);

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0003", rs.getString(2));
      Assert.assertEquals(234.75, rs.getDouble(3), 0);

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0004", rs.getString(2));
      Assert.assertEquals(236.49, rs.getDouble(3), 0);

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0005", rs.getString(2));
      Assert.assertEquals(17.053, rs.getDouble(3), 0);

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0006", rs.getString(2));
      Assert.assertEquals(98.8452, rs.getDouble(3), 0);

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0007", rs.getString(2));
      Assert.assertEquals(234.75, rs.getDouble(3), 0);

      Assert.assertFalse(rs.next());

      rs = stmt.executeQuery("SELECT COL2, VCOL1, VCOL2, COL5, COL6 FROM " + view02);
      Assert.assertTrue(rs.next());

      Assert.assertEquals("0001", rs.getString(1));
      Assert.assertEquals(234.75, rs.getDouble(2), 0);
      Assert.assertEquals("vcol2_01", rs.getString(3));
      Assert.assertEquals("col5_01", rs.getString(4));
      Assert.assertEquals("col6_01", rs.getString(5));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("0002", rs.getString(1));
      Assert.assertEquals(234.75, rs.getDouble(2), 0);
      Assert.assertEquals("vcol2_02", rs.getString(3));
      Assert.assertEquals("col5_02", rs.getString(4));
      Assert.assertEquals("col6_02", rs.getString(5));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("0003", rs.getString(1));
      Assert.assertEquals(234.75, rs.getDouble(2), 0);
      Assert.assertEquals("vcol2_03", rs.getString(3));
      Assert.assertEquals("col5_03", rs.getString(4));
      Assert.assertEquals("col6_03", rs.getString(5));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("0007", rs.getString(1));
      Assert.assertEquals(234.75, rs.getDouble(2), 0);
      Assert.assertNull(rs.getString(3));
      Assert.assertEquals("col5_07", rs.getString(4));
      Assert.assertNull(rs.getString(5));

      Assert.assertFalse(rs.next());

      rs = stmt.executeQuery("SELECT COL1, COL2, VCOL1 FROM " + view01 + " ORDER BY "
              + "VCOL1 DESC, COL2 ASC");

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0004", rs.getString(2));
      Assert.assertEquals(236.49, rs.getDouble(3), 0);

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0001", rs.getString(2));
      Assert.assertEquals(234.75, rs.getDouble(3), 0);

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0002", rs.getString(2));
      Assert.assertEquals(234.75, rs.getDouble(3), 0);

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0003", rs.getString(2));
      Assert.assertEquals(234.75, rs.getDouble(3), 0);

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0007", rs.getString(2));
      Assert.assertEquals(234.75, rs.getDouble(3), 0);

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0006", rs.getString(2));
      Assert.assertEquals(98.8452, rs.getDouble(3), 0);

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0005", rs.getString(2));
      Assert.assertEquals(17.053, rs.getDouble(3), 0);

      Assert.assertFalse(rs.next());

      rs = stmt.executeQuery("SELECT COL2, VCOL1, VCOL2, COL5, COL6 FROM " + view02
              + " ORDER BY VCOL2 DESC, VCOL1 DESC");

      Assert.assertTrue(rs.next());

      Assert.assertEquals("0007", rs.getString(1));
      Assert.assertEquals(234.75, rs.getDouble(2), 0);
      Assert.assertNull(rs.getString(3));
      Assert.assertEquals("col5_07", rs.getString(4));
      Assert.assertNull(rs.getString(5));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("0003", rs.getString(1));
      Assert.assertEquals(234.75, rs.getDouble(2), 0);
      Assert.assertEquals("vcol2_03", rs.getString(3));
      Assert.assertEquals("col5_03", rs.getString(4));
      Assert.assertEquals("col6_03", rs.getString(5));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("0002", rs.getString(1));
      Assert.assertEquals(234.75, rs.getDouble(2), 0);
      Assert.assertEquals("vcol2_02", rs.getString(3));
      Assert.assertEquals("col5_02", rs.getString(4));
      Assert.assertEquals("col6_02", rs.getString(5));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("0001", rs.getString(1));
      Assert.assertEquals(234.75, rs.getDouble(2), 0);
      Assert.assertEquals("vcol2_01", rs.getString(3));
      Assert.assertEquals("col5_01", rs.getString(4));
      Assert.assertEquals("col6_01", rs.getString(5));

      Assert.assertFalse(rs.next());
    }
  }

  @Test
  public void testViewWithCharDescPkColumnAndViewIndex() throws Exception {
    final String tableName = generateUniqueName();
    final String view01 = "v01_" + tableName;
    final String view02 = "v02_" + tableName;
    final String index_view01 = "idx_v01_" + tableName;
    final String index_view02 = "idx_v02_" + tableName;

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      final Statement stmt = conn.createStatement();

      stmt.execute("CREATE TABLE " + tableName
              + " (COL1 CHAR(10) NOT NULL, COL2 CHAR(5) NOT NULL, COL3 VARCHAR,"
              + " COL4 VARCHAR CONSTRAINT pk PRIMARY KEY(COL1 ASC, COL2 DESC))");
      stmt.execute("CREATE VIEW " + view01
              + " (VCOL1 CHAR(8), COL5 VARCHAR) AS SELECT * FROM " + tableName
              + " WHERE COL1 = 'col1'");
      stmt.execute("CREATE VIEW " + view02 + " (VCOL2 CHAR(10), COL6 VARCHAR)"
              + " AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'vcol1'");
      stmt.execute("CREATE INDEX " + index_view01 + " ON " + view01 + " (COL5) INCLUDE "
              + "(COL1, COL2, COL3)");
      stmt.execute("CREATE INDEX " + index_view02 + " ON " + view02 + " (COL6) INCLUDE "
              + "(COL1, COL2, COL3)");

      stmt.execute("UPSERT INTO " + view02
              + " (col2, vcol2, col5, col6) values ('0001', 'vcol2_01', 'col5_01', 'col6_01')");
      stmt.execute("UPSERT INTO " + view02
              + " (col2, vcol2, col5, col6) values ('0002', 'vcol2_02', 'col5_02', 'col6_02')");
      stmt.execute("UPSERT INTO " + view02
              + " (col2, vcol2, col5, col6) values ('0003', 'vcol2_03', 'col5_03', 'col6_03')");
      stmt.execute("UPSERT INTO " + view01 + " (col2, vcol1, col3, col4, col5) values "
              + "('0004', 'vcol2', 'col3_04', 'col4_04', 'col5_04')");
      stmt.execute("UPSERT INTO " + view01 + " (col2, vcol1, col3, col4, col5) values "
              + "('0005', 'vcol-2', 'col3_05', 'col4_05', 'col5_05')");
      stmt.execute("UPSERT INTO " + view01 + " (col2, vcol1, col3, col4, col5) values "
              + "('0006', 'vcol-1', 'col3_06', 'col4_06', 'col5_06')");
      stmt.execute("UPSERT INTO " + view01 + " (col2, vcol1, col3, col4, col5) values "
              + "('0007', 'vcol1', 'col3_07', 'col4_07', 'col5_07')");
      stmt.execute("UPSERT INTO " + view02
              + " (col2, vcol2, col5, col6) values ('0008', 'vcol2_08', 'col5_08', 'col6_02')");
      conn.commit();

      ResultSet rs = stmt.executeQuery("SELECT COL2, VCOL1, VCOL2, COL5, COL6 FROM " + view02);

      Assert.assertTrue(rs.next());

      Assert.assertEquals("0008", rs.getString(1));
      Assert.assertEquals("vcol1", rs.getString(2));
      Assert.assertEquals("vcol2_08", rs.getString(3));
      Assert.assertEquals("col5_08", rs.getString(4));
      Assert.assertEquals("col6_02", rs.getString(5));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("0007", rs.getString(1));
      Assert.assertEquals("vcol1", rs.getString(2));
      Assert.assertNull(rs.getString(3));
      Assert.assertEquals("col5_07", rs.getString(4));
      Assert.assertNull(rs.getString(5));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("0003", rs.getString(1));
      Assert.assertEquals("vcol1", rs.getString(2));
      Assert.assertEquals("vcol2_03", rs.getString(3));
      Assert.assertEquals("col5_03", rs.getString(4));
      Assert.assertEquals("col6_03", rs.getString(5));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("0002", rs.getString(1));
      Assert.assertEquals("vcol1", rs.getString(2));
      Assert.assertEquals("vcol2_02", rs.getString(3));
      Assert.assertEquals("col5_02", rs.getString(4));
      Assert.assertEquals("col6_02", rs.getString(5));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("0001", rs.getString(1));
      Assert.assertEquals("vcol1", rs.getString(2));
      Assert.assertEquals("vcol2_01", rs.getString(3));
      Assert.assertEquals("col5_01", rs.getString(4));
      Assert.assertEquals("col6_01", rs.getString(5));

      Assert.assertFalse(rs.next());

      rs = stmt.executeQuery("SELECT * FROM " + view02);

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0008", rs.getString(2));
      Assert.assertNull(rs.getString(3));
      Assert.assertNull(rs.getString(4));
      Assert.assertEquals("vcol1", rs.getString(5));
      Assert.assertEquals("col5_08", rs.getString(6));
      Assert.assertEquals("vcol2_08", rs.getString(7));
      Assert.assertEquals("col6_02", rs.getString(8));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0007", rs.getString(2));
      Assert.assertEquals("col3_07", rs.getString(3));
      Assert.assertEquals("col4_07", rs.getString(4));
      Assert.assertEquals("vcol1", rs.getString(5));
      Assert.assertEquals("col5_07", rs.getString(6));
      Assert.assertNull(rs.getString(7));
      Assert.assertNull(rs.getString(8));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0003", rs.getString(2));
      Assert.assertNull(rs.getString(3));
      Assert.assertNull(rs.getString(4));
      Assert.assertEquals("vcol1", rs.getString(5));
      Assert.assertEquals("col5_03", rs.getString(6));
      Assert.assertEquals("vcol2_03", rs.getString(7));
      Assert.assertEquals("col6_03", rs.getString(8));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0002", rs.getString(2));
      Assert.assertNull(rs.getString(3));
      Assert.assertNull(rs.getString(4));
      Assert.assertEquals("vcol1", rs.getString(5));
      Assert.assertEquals("col5_02", rs.getString(6));
      Assert.assertEquals("vcol2_02", rs.getString(7));
      Assert.assertEquals("col6_02", rs.getString(8));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0001", rs.getString(2));
      Assert.assertNull(rs.getString(3));
      Assert.assertNull(rs.getString(4));
      Assert.assertEquals("vcol1", rs.getString(5));
      Assert.assertEquals("col5_01", rs.getString(6));
      Assert.assertEquals("vcol2_01", rs.getString(7));
      Assert.assertEquals("col6_01", rs.getString(8));

      Assert.assertFalse(rs.next());

      rs = stmt.executeQuery(
              "SELECT * FROM " + view02 + " WHERE COL6 = 'col6_02'");

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0008", rs.getString(2));
      Assert.assertNull(rs.getString(3));
      Assert.assertNull(rs.getString(4));
      Assert.assertEquals("vcol1", rs.getString(5));
      Assert.assertEquals("col5_08", rs.getString(6));
      Assert.assertEquals("vcol2_08", rs.getString(7));
      Assert.assertEquals("col6_02", rs.getString(8));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0002", rs.getString(2));
      Assert.assertNull(rs.getString(3));
      Assert.assertNull(rs.getString(4));
      Assert.assertEquals("vcol1", rs.getString(5));
      Assert.assertEquals("col5_02", rs.getString(6));
      Assert.assertEquals("vcol2_02", rs.getString(7));
      Assert.assertEquals("col6_02", rs.getString(8));

      Assert.assertFalse(rs.next());

      rs = stmt.executeQuery(
              "SELECT COL1, COL2, VCOL1, COL6 FROM " + view02 + " WHERE COL6 = 'col6_02'");

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0008", rs.getString(2));
      Assert.assertEquals("vcol1", rs.getString(3));
      Assert.assertEquals("col6_02", rs.getString(4));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("col1", rs.getString(1));
      Assert.assertEquals("0002", rs.getString(2));
      Assert.assertEquals("vcol1", rs.getString(3));
      Assert.assertEquals("col6_02", rs.getString(4));

      Assert.assertFalse(rs.next());
    }
  }

  @Test
  public void testViewWithTimestampDescPkColumnAndViewIndex() throws Exception {
    final String tableName = generateUniqueName();
    final String view01 = "v01_" + tableName;
    final String view02 = "v02_" + tableName;
    final String index_view01 = "idx_v01_" + tableName;
    final String index_view02 = "idx_v02_" + tableName;

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      final Statement stmt = conn.createStatement();

      stmt.execute("CREATE TABLE " + tableName
              + " (COL1 TIMESTAMP NOT NULL, COL2 CHAR(5) NOT NULL, COL3 VARCHAR,"
              + " COL4 VARCHAR CONSTRAINT pk PRIMARY KEY(COL1 ASC, COL2 DESC))");
      stmt.execute("CREATE VIEW " + view01
              + " (VCOL1 CHAR(8), COL5 VARCHAR) AS SELECT * FROM " + tableName
              + " WHERE COL1 = TO_TIMESTAMP('2023-01-20 00:10:00')");
      stmt.execute("CREATE VIEW " + view02 + " (VCOL2 CHAR(10), COL6 VARCHAR)"
              + " AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'vcol1'");
      stmt.execute("CREATE INDEX " + index_view01 + " ON " + view01 + " (COL5) INCLUDE "
              + "(COL1, COL2, COL3)");
      stmt.execute("CREATE INDEX " + index_view02 + " ON " + view02 + " (COL6) INCLUDE "
              + "(COL1, COL2, COL3)");

      stmt.execute("UPSERT INTO " + view02
              + " (col2, vcol2, col5, col6) values ('0001', 'vcol2_01', 'col5_01', 'col6_01')");
      stmt.execute("UPSERT INTO " + view02
              + " (col2, vcol2, col5, col6) values ('0002', 'vcol2_02', 'col5_02', 'col6_02')");
      stmt.execute("UPSERT INTO " + view02
              + " (col2, vcol2, col5, col6) values ('0003', 'vcol2_03', 'col5_03', 'col6_03')");
      stmt.execute("UPSERT INTO " + view01 + " (col2, vcol1, col3, col4, col5) values "
              + "('0004', 'vcol2', 'col3_04', 'col4_04', 'col5_04')");
      stmt.execute("UPSERT INTO " + view01 + " (col2, vcol1, col3, col4, col5) values "
              + "('0005', 'vcol-2', 'col3_05', 'col4_05', 'col5_05')");
      stmt.execute("UPSERT INTO " + view01 + " (col2, vcol1, col3, col4, col5) values "
              + "('0006', 'vcol-1', 'col3_06', 'col4_06', 'col5_06')");
      stmt.execute("UPSERT INTO " + view01 + " (col2, vcol1, col3, col4, col5) values "
              + "('0007', 'vcol1', 'col3_07', 'col4_07', 'col5_07')");
      stmt.execute("UPSERT INTO " + view02
              + " (col2, vcol2, col5, col6) values ('0008', 'vcol2_08', 'col5_08', 'col6_02')");
      conn.commit();

      ResultSet rs = stmt.executeQuery("SELECT COL2, VCOL1, VCOL2, COL5, COL6 FROM " + view02);

      Assert.assertTrue(rs.next());

      Assert.assertEquals("0008", rs.getString(1));
      Assert.assertEquals("vcol1", rs.getString(2));
      Assert.assertEquals("vcol2_08", rs.getString(3));
      Assert.assertEquals("col5_08", rs.getString(4));
      Assert.assertEquals("col6_02", rs.getString(5));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("0007", rs.getString(1));
      Assert.assertEquals("vcol1", rs.getString(2));
      Assert.assertNull(rs.getString(3));
      Assert.assertEquals("col5_07", rs.getString(4));
      Assert.assertNull(rs.getString(5));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("0003", rs.getString(1));
      Assert.assertEquals("vcol1", rs.getString(2));
      Assert.assertEquals("vcol2_03", rs.getString(3));
      Assert.assertEquals("col5_03", rs.getString(4));
      Assert.assertEquals("col6_03", rs.getString(5));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("0002", rs.getString(1));
      Assert.assertEquals("vcol1", rs.getString(2));
      Assert.assertEquals("vcol2_02", rs.getString(3));
      Assert.assertEquals("col5_02", rs.getString(4));
      Assert.assertEquals("col6_02", rs.getString(5));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("0001", rs.getString(1));
      Assert.assertEquals("vcol1", rs.getString(2));
      Assert.assertEquals("vcol2_01", rs.getString(3));
      Assert.assertEquals("col5_01", rs.getString(4));
      Assert.assertEquals("col6_01", rs.getString(5));

      Assert.assertFalse(rs.next());

      rs = stmt.executeQuery("SELECT * FROM " + view02);

      Assert.assertTrue(rs.next());

      Assert.assertEquals("2023-01-20T00:10:00Z", rs.getTimestamp(1).toInstant().toString());
      Assert.assertEquals("0008", rs.getString(2));
      Assert.assertNull(rs.getString(3));
      Assert.assertNull(rs.getString(4));
      Assert.assertEquals("vcol1", rs.getString(5));
      Assert.assertEquals("col5_08", rs.getString(6));
      Assert.assertEquals("vcol2_08", rs.getString(7));
      Assert.assertEquals("col6_02", rs.getString(8));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("2023-01-20T00:10:00Z", rs.getTimestamp(1).toInstant().toString());
      Assert.assertEquals("0007", rs.getString(2));
      Assert.assertEquals("col3_07", rs.getString(3));
      Assert.assertEquals("col4_07", rs.getString(4));
      Assert.assertEquals("vcol1", rs.getString(5));
      Assert.assertEquals("col5_07", rs.getString(6));
      Assert.assertNull(rs.getString(7));
      Assert.assertNull(rs.getString(8));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("2023-01-20T00:10:00Z", rs.getTimestamp(1).toInstant().toString());
      Assert.assertEquals("0003", rs.getString(2));
      Assert.assertNull(rs.getString(3));
      Assert.assertNull(rs.getString(4));
      Assert.assertEquals("vcol1", rs.getString(5));
      Assert.assertEquals("col5_03", rs.getString(6));
      Assert.assertEquals("vcol2_03", rs.getString(7));
      Assert.assertEquals("col6_03", rs.getString(8));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("2023-01-20T00:10:00Z", rs.getTimestamp(1).toInstant().toString());
      Assert.assertEquals("0002", rs.getString(2));
      Assert.assertNull(rs.getString(3));
      Assert.assertNull(rs.getString(4));
      Assert.assertEquals("vcol1", rs.getString(5));
      Assert.assertEquals("col5_02", rs.getString(6));
      Assert.assertEquals("vcol2_02", rs.getString(7));
      Assert.assertEquals("col6_02", rs.getString(8));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("2023-01-20T00:10:00Z", rs.getTimestamp(1).toInstant().toString());
      Assert.assertEquals("0001", rs.getString(2));
      Assert.assertNull(rs.getString(3));
      Assert.assertNull(rs.getString(4));
      Assert.assertEquals("vcol1", rs.getString(5));
      Assert.assertEquals("col5_01", rs.getString(6));
      Assert.assertEquals("vcol2_01", rs.getString(7));
      Assert.assertEquals("col6_01", rs.getString(8));

      Assert.assertFalse(rs.next());

      rs = stmt.executeQuery(
              "SELECT * FROM " + view02 + " WHERE COL6 = 'col6_02'");

      Assert.assertTrue(rs.next());

      Assert.assertEquals("2023-01-20T00:10:00Z", rs.getTimestamp(1).toInstant().toString());
      Assert.assertEquals("0008", rs.getString(2));
      Assert.assertNull(rs.getString(3));
      Assert.assertNull(rs.getString(4));
      Assert.assertEquals("vcol1", rs.getString(5));
      Assert.assertEquals("col5_08", rs.getString(6));
      Assert.assertEquals("vcol2_08", rs.getString(7));
      Assert.assertEquals("col6_02", rs.getString(8));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("2023-01-20T00:10:00Z", rs.getTimestamp(1).toInstant().toString());
      Assert.assertEquals("0002", rs.getString(2));
      Assert.assertNull(rs.getString(3));
      Assert.assertNull(rs.getString(4));
      Assert.assertEquals("vcol1", rs.getString(5));
      Assert.assertEquals("col5_02", rs.getString(6));
      Assert.assertEquals("vcol2_02", rs.getString(7));
      Assert.assertEquals("col6_02", rs.getString(8));

      Assert.assertFalse(rs.next());

      rs = stmt.executeQuery(
              "SELECT COL1, COL2, VCOL1, COL6 FROM " + view02 + " WHERE COL6 = 'col6_02'");

      Assert.assertTrue(rs.next());

      Assert.assertEquals("2023-01-20T00:10:00Z", rs.getTimestamp(1).toInstant().toString());
      Assert.assertEquals("0008", rs.getString(2));
      Assert.assertEquals("vcol1", rs.getString(3));
      Assert.assertEquals("col6_02", rs.getString(4));

      Assert.assertTrue(rs.next());

      Assert.assertEquals("2023-01-20T00:10:00Z", rs.getTimestamp(1).toInstant().toString());
      Assert.assertEquals("0002", rs.getString(2));
      Assert.assertEquals("vcol1", rs.getString(3));
      Assert.assertEquals("col6_02", rs.getString(4));

      Assert.assertFalse(rs.next());
    }
  }

}
