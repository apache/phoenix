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


}
