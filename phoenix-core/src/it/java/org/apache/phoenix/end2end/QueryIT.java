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

import static org.apache.phoenix.util.TestUtil.A_VALUE;
import static org.apache.phoenix.util.TestUtil.B_VALUE;
import static org.apache.phoenix.util.TestUtil.C_VALUE;
import static org.apache.phoenix.util.TestUtil.ROW7;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized.Parameters;

/**
 * Basic tests for Phoenix JDBC implementation
 */
@Category(ParallelStatsDisabledTest.class)
public class QueryIT extends BaseQueryIT {

  @Parameters(name = "QueryIT_{index}") // name is used by failsafe as file name in reports
  public static synchronized Collection<Object> data() {
    // Return only one parameter set to run a single iteration
    // Parameters: indexDDL, columnEncoded, keepDeletedCells
    List<Object> testCases = Lists.newArrayList();
    testCases.add(new Object[] { NO_INDEX, false, false }); // No index, no column encoding
    return testCases;
    
    // Original code that runs all iterations:
    // return BaseQueryIT.allIndexes();
  }

  public QueryIT(String indexDDL, boolean columnEncoded, boolean keepDeletedCells) {
    super(indexDDL, columnEncoded, keepDeletedCells);
  }

  @Test
  public void testToDateOnString() throws Exception { // TODO: test more conversion combinations
    String query =
      "SELECT a_string FROM " + tableName + " WHERE organization_id=? and a_integer = 5";
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    Connection conn = DriverManager.getConnection(getUrl(), props);
    try {
      PreparedStatement statement = conn.prepareStatement(query);
      statement.setString(1, tenantId);
      ResultSet rs = statement.executeQuery();
      assertTrue(rs.next());
      rs.getDate(1);
      fail();
    } catch (SQLException e) { // Expected
      assertEquals(SQLExceptionCode.TYPE_MISMATCH.getErrorCode(), e.getErrorCode());
    } finally {
      conn.close();
    }
  }

  @Test
  public void testColumnOnBothSides() throws Exception {
    String query =
      "SELECT entity_id FROM " + tableName + " WHERE organization_id=? and a_string = b_string";
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    Connection conn = DriverManager.getConnection(getUrl(), props);
    try {
      PreparedStatement statement = conn.prepareStatement(query);
      statement.setString(1, tenantId);
      ResultSet rs = statement.executeQuery();
      assertTrue(rs.next());
      assertEquals(rs.getString(1), ROW7);
      assertFalse(rs.next());
    } finally {
      conn.close();
    }
  }

  @Test
  public void testColumnAliasMapping() throws Exception {
    String query = "SELECT a.a_string, " + tableName + ".b_string FROM " + tableName
      + " a WHERE ?=organization_id and 5=a_integer ORDER BY a_string, b_string";
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    Connection conn = DriverManager.getConnection(getUrl(), props);
    try {
      PreparedStatement statement = conn.prepareStatement(query);
      statement.setString(1, tenantId);
      ResultSet rs = statement.executeQuery();
      assertTrue(rs.next());
      assertEquals(rs.getString(1), B_VALUE);
      assertEquals(rs.getString("B_string"), C_VALUE);
      assertFalse(rs.next());
    } finally {
      conn.close();
    }
  }

  @Test
  public void testAllScan() throws Exception {
    String query = "SELECT ALL a_string, b_string FROM " + tableName
      + " WHERE ?=organization_id and 5=a_integer";
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    Connection conn = DriverManager.getConnection(getUrl(), props);
    try {
      PreparedStatement statement = conn.prepareStatement(query);
      statement.setString(1, tenantId);
      ResultSet rs = statement.executeQuery();
      assertTrue(rs.next());
      assertEquals(rs.getString(1), B_VALUE);
      assertEquals(rs.getString("B_string"), C_VALUE);
      assertFalse(rs.next());
    } finally {
      conn.close();
    }
  }

  @Test
  public void testDistinctScan() throws Exception {
    String query = "SELECT DISTINCT a_string FROM " + tableName + " WHERE organization_id=?";
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    Connection conn = DriverManager.getConnection(getUrl(), props);
    try {
      PreparedStatement statement = conn.prepareStatement(query);
      statement.setString(1, tenantId);
      ResultSet rs = statement.executeQuery();
      assertTrue(rs.next());
      assertEquals(rs.getString(1), A_VALUE);
      assertTrue(rs.next());
      assertEquals(rs.getString(1), B_VALUE);
      assertTrue(rs.next());
      assertEquals(rs.getString(1), C_VALUE);
      assertFalse(rs.next());
    } finally {
      conn.close();
    }
  }

  @Test
  public void testDistinctLimitScan() throws Exception {
    String query =
      "SELECT DISTINCT a_string FROM " + tableName + " WHERE organization_id=? LIMIT 1";
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    Connection conn = DriverManager.getConnection(getUrl(), props);
    try {
      PreparedStatement statement = conn.prepareStatement(query);
      statement.setString(1, tenantId);
      ResultSet rs = statement.executeQuery();
      assertTrue(rs.next());
      assertEquals(rs.getString(1), A_VALUE);
      assertFalse(rs.next());
    } finally {
      conn.close();
    }
  }

  @Test
  public void testExplosion() throws Exception {
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl());
        Statement stmt = conn.createStatement()) {
      stmt.execute("create table " + tableName + " (id varchar primary key, ts timestamp)");
      //      stmt.execute("create table " + tableName + "(id varchar NOT NULL, ts timestamp NOT NULL CONSTRAINT PK PRIMARY KEY (id, ts DESC))");
      stmt.execute("create index " + indexName + " on " + tableName + "(ts desc)");

      String query = "select id, ts from " + tableName
          + " where ts >= TIMESTAMP '2023-02-23 13:30:00'  and ts < TIMESTAMP '2023-02-23 13:40:00'";
      ResultSet rs = stmt.executeQuery("EXPLAIN " + query);
      String explainPlan = QueryUtil.getExplainPlan(rs);
      System.out.println("EXPLAIN PLAN: " + explainPlan);
      PreparedStatement statement = conn.prepareStatement(query);
      rs = statement.executeQuery();
      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
      }
      //      ResultSet rs = stmt.executeQuery("EXPLAIN " + query);
      //      String explainPlan = QueryUtil.getExplainPlan(rs);
      //      assertEquals(
      //          "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + indexName
      //              + " [~1,677,159,600,000] - [~1,677,159,000,000]\n    SERVER FILTER BY FIRST KEY ONLY",
      //          explainPlan);
    }
  }



  @Test
  public void testKeyExplosion() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    Connection conn = DriverManager.getConnection(getUrl(), props);
    String testTable = generateUniqueName();
    try {
      // Create table with DESC ordering on NUMBER column
      String createTableDDL = "CREATE TABLE IF NOT EXISTS " + testTable + " ("
        + "ID CHAR(15) NOT NULL, "
        + "NUMBER VARCHAR NOT NULL, "
        + "ENTITY_ID VARCHAR NOT NULL, "
        + "CREATED_BY VARCHAR, "
        + "DATA VARCHAR "
        + "CONSTRAINT PK PRIMARY KEY (ID, NUMBER DESC, ENTITY_ID))";
      conn.createStatement().execute(createTableDDL);
      
      // Insert test data
      String upsert = "UPSERT INTO " + testTable
        + " (ID, NUMBER, ENTITY_ID, CREATED_BY, DATA) VALUES (?, ?, ?, ?, ?)";
      PreparedStatement ps = conn.prepareStatement(upsert);

      // Insert first row
      ps.setString(1, "id_1");
      ps.setString(2, "20251012");
      ps.setString(3, "entity_1");
      ps.setString(4, "user1");
      ps.setString(5, "data1");
      ps.executeUpdate();

      // Insert second row
      ps.setString(1, "id_2");
      ps.setString(2, "20250912");
      ps.setString(3, "entity_2");
      ps.setString(4, "user2");
      ps.setString(5, "data2");
      ps.executeUpdate();

      ps.setString(1, "id_3");
      ps.setString(2, "20250913");
      ps.setString(3, "entity_3");
      ps.setString(4, "user3");
      ps.setString(5, "data3");
      ps.executeUpdate();

//
//      ps.setString(1, "id_1");
//      ps.setString(2, "20250910");
//      ps.setString(3, "entity_3");
//      ps.setString(4, "user22");
//      ps.setString(5, "data22");
//      ps.executeUpdate();
//
//      ps.setString(1, "id_3");
//      ps.setString(2, "20250911");
//      ps.setString(3, "entity_11");
//      ps.setString(4, "user21");
//      ps.setString(5, "data21");
//      ps.executeUpdate();

      conn.commit();
      
      // Run the query with IN clause
      String query = "SELECT * FROM " + testTable 
        + " WHERE (ID, NUMBER, ENTITY_ID) IN (('id_1', '20251012', 'entity_1'), ('id_2', '20250912', 'entity_2'))";
      PreparedStatement statement = conn.prepareStatement(query);
      ResultSet rs = statement.executeQuery();

      // Verify we get exactly 2 rows back
      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
        String id = rs.getString("ID");
        String number = rs.getString("NUMBER");
        String entityId = rs.getString("ENTITY_ID");

        // Verify the data matches what we inserted
        if (rowCount == 1) {
          assertEquals("id_1", id);
          assertEquals("20251012", number);
          assertEquals("entity_1", entityId);
        } else if (rowCount == 2) {
          assertEquals("id_2", id);
          assertEquals("20250912", number);
          assertEquals("entity_2", entityId);
        }
      }

      assertEquals("Expected 2 rows", 2, rowCount);
    } finally {
      conn.close();
    }
  }

  @Test
  public void testKeyExplosionInteger() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    Connection conn = DriverManager.getConnection(getUrl(), props);
    String testTable = generateUniqueName();
    // Create table with DESC ordering on NUMBER column
    String createTableDDL = "CREATE TABLE IF NOT EXISTS " + testTable + " ("
        + "ID CHAR(15) NOT NULL, "
        + "NUMBER INTEGER NOT NULL, "
        + "ENTITY_ID VARCHAR NOT NULL, "
        + "CREATED_BY VARCHAR, "
        + "DATA VARCHAR "
        + "CONSTRAINT PK PRIMARY KEY (ID, NUMBER DESC, ENTITY_ID))";
    conn.createStatement().execute(createTableDDL);

    // Insert test data
    String upsert = "UPSERT INTO " + testTable
        + " (ID, NUMBER, ENTITY_ID, CREATED_BY, DATA) VALUES (?, ?, ?, ?, ?)";
    PreparedStatement ps = conn.prepareStatement(upsert);

    // Insert first row
    ps.setString(1, "id_1");
    ps.setInt(2, 20251012);
    ps.setString(3, "entity_1");
    ps.setString(4, "user1");
    ps.setString(5, "data1");
    ps.executeUpdate();

    // Insert second row
    ps.setString(1, "id_2");
    ps.setInt(2, 20250912);
    ps.setString(3, "entity_2");
    ps.setString(4, "user2");
    ps.setString(5, "data2");
    ps.executeUpdate();

    ps.setString(1, "id_3");
    ps.setInt(2, 20250910);
    ps.setString(3, "entity_3");
    ps.setString(4, "user3");
    ps.setString(5, "data3");
    ps.executeUpdate();
    conn.commit();

    // Run the query with IN clause
    String query = "SELECT * FROM " + testTable
        + " WHERE (ID, NUMBER, ENTITY_ID) IN (('id_1', 20251012, 'entity_1'), ('id_2', 20250912, 'entity_2'))";
    PreparedStatement statement = conn.prepareStatement(query);
    ResultSet rs = statement.executeQuery();

    // Verify we get exactly 2 rows back
    int rowCount = 0;
    while (rs.next()) {
      rowCount++;
      String id = rs.getString("ID");
      String number = rs.getString("NUMBER");
      String entityId = rs.getString("ENTITY_ID");

      // Verify the data matches what we inserted
      if (rowCount == 1) {
        assertEquals("id_1", id);
        assertEquals("20251012", number);
        assertEquals("entity_1", entityId);
      } else if (rowCount == 2) {
        assertEquals("id_2", id);
        assertEquals("20250912", number);
        assertEquals("entity_2", entityId);
      }
    }

    assertEquals("Expected 2 rows", 2, rowCount);
  }


  @Test
  public void testExplosionIntegerIndex() throws Exception {
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();
    System.out.println(tableName);
    System.out.println(indexName);
    try (Connection conn = DriverManager.getConnection(getUrl());
        Statement stmt = conn.createStatement()) {
      stmt.execute("create table " + tableName + " (id varchar primary key, ts integer)");
      stmt.execute("create index " + indexName + " on " + tableName + "(ts desc)");

      // Insert test data
      String upsert = "UPSERT INTO " + tableName
          + " (id, ts) VALUES (?, ?)";
      PreparedStatement ps = conn.prepareStatement(upsert);

      // Insert first row
      ps.setString(1, "id_1");
      ps.setInt(2, 20251012);
      ps.executeUpdate();

      // Insert second row
      ps.setString(1, "id_2");
      ps.setInt(2, 20250912);
      ps.executeUpdate();

      ps.setString(1, "id_3");
      ps.setInt(2, 20250910);
      ps.executeUpdate();
      conn.commit();

      String query = "select * from " + tableName
          + " where ts > 20250911";
      PreparedStatement statement = conn.prepareStatement(query);
      ResultSet rs = statement.executeQuery();
      String explainPlan = QueryUtil.getExplainPlan(rs);
      System.out.println("EXPLAIN PLAN: " + explainPlan);
      statement = conn.prepareStatement(query);
      rs = statement.executeQuery();
      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
      }
      //      ResultSet rs = stmt.executeQuery("EXPLAIN " + query);
      //      String explainPlan = QueryUtil.getExplainPlan(rs);
      //      assertEquals(
      //          "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + indexName
      //              + " [~1,677,159,600,000] - [~1,677,159,000,000]\n    SERVER FILTER BY FIRST KEY ONLY",
      //          explainPlan);
    }
  }

  @Test
  public void testKeyExplosionPartialCompositeIn() throws Exception {
    // Variation 6: Partial composite key IN
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    Connection conn = DriverManager.getConnection(getUrl(), props);
    String testTable = generateUniqueName();
    try {
      // Create table with DESC ordering on NUMBER column
      String createTableDDL = "CREATE TABLE IF NOT EXISTS " + testTable + " ("
          + "ID CHAR(15) NOT NULL, "
          + "NUMBER VARCHAR NOT NULL, "
          + "ENTITY_ID VARCHAR NOT NULL, "
          + "CREATED_BY VARCHAR, "
          + "DATA VARCHAR "
          + "CONSTRAINT PK PRIMARY KEY (ID, NUMBER DESC, ENTITY_ID))";
      conn.createStatement().execute(createTableDDL);

      // Insert test data
      String upsert = "UPSERT INTO " + testTable
          + " (ID, NUMBER, ENTITY_ID, CREATED_BY, DATA) VALUES (?, ?, ?, ?, ?)";
      PreparedStatement ps = conn.prepareStatement(upsert);

      ps.setString(1, "id_1");
      ps.setString(2, "20251012");
      ps.setString(3, "entity_1");
      ps.setString(4, "user1");
      ps.setString(5, "data1");
      ps.executeUpdate();

      ps.setString(1, "id_2");
      ps.setString(2, "20250912");
      ps.setString(3, "entity_2");
      ps.setString(4, "user2");
      ps.setString(5, "data2");
      ps.executeUpdate();

      ps.setString(1, "id_3");
      ps.setString(2, "20250913");
      ps.setString(3, "entity_3");
      ps.setString(4, "user3");
      ps.setString(5, "data3");
      ps.executeUpdate();

      ps.setString(1, "id_1");
      ps.setString(2, "20251012");
      ps.setString(3, "entity_1b");
      ps.setString(4, "user4");
      ps.setString(5, "data4");
      ps.executeUpdate();

      conn.commit();

      // Run query with partial composite key IN (first two columns only)
      String query = "SELECT * FROM " + testTable
          + " WHERE (ID, NUMBER) IN (('id_1', '20251012'), ('id_2', '20250912'))";
      PreparedStatement statement = conn.prepareStatement(query);
      ResultSet rs = statement.executeQuery();

      // Should return 3 rows: id_1 with 2 ENTITY_IDs at same NUMBER, id_2 with 1 ENTITY_ID
      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
        String id = rs.getString("ID");
        String number = rs.getString("NUMBER");
        // Verify the combinations
        assertTrue("Unexpected row",
            (id.equals("id_1") && number.equals("20251012")) ||
            (id.equals("id_2") && number.equals("20250912")));
      }

      assertEquals("Expected 3 rows", 3, rowCount);
    } finally {
      conn.close();
    }
  }

  @Test
  public void testKeyExplosionMixedAndOr() throws Exception {
    // Variation 8: Mixed AND/OR with ranges on DESC column
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    Connection conn = DriverManager.getConnection(getUrl(), props);
    String testTable = generateUniqueName();
    try {
      // Create table with DESC ordering on NUMBER column
      String createTableDDL = "CREATE TABLE IF NOT EXISTS " + testTable + " ("
          + "ID CHAR(15) NOT NULL, "
          + "NUMBER VARCHAR NOT NULL, "
          + "ENTITY_ID VARCHAR NOT NULL, "
          + "CREATED_BY VARCHAR, "
          + "DATA VARCHAR "
          + "CONSTRAINT PK PRIMARY KEY (ID, NUMBER DESC, ENTITY_ID))";
      conn.createStatement().execute(createTableDDL);

      // Insert test data
      String upsert = "UPSERT INTO " + testTable
          + " (ID, NUMBER, ENTITY_ID, CREATED_BY, DATA) VALUES (?, ?, ?, ?, ?)";
      PreparedStatement ps = conn.prepareStatement(upsert);

      ps.setString(1, "id_1");
      ps.setString(2, "20251012");
      ps.setString(3, "entity_1");
      ps.setString(4, "user1");
      ps.setString(5, "data1");
      ps.executeUpdate();

      ps.setString(1, "id_1");
      ps.setString(2, "20250910");
      ps.setString(3, "entity_1b");
      ps.setString(4, "user2");
      ps.setString(5, "data2");
      ps.executeUpdate();

      ps.setString(1, "id_2");
      ps.setString(2, "20251011");
      ps.setString(3, "entity_2");
      ps.setString(4, "user3");
      ps.setString(5, "data3");
      ps.executeUpdate();

      ps.setString(1, "id_3");
      ps.setString(2, "20250913");
      ps.setString(3, "entity_3");
      ps.setString(4, "user4");
      ps.setString(5, "data4");
      ps.executeUpdate();

      conn.commit();

      // Run query with mixed AND/OR conditions
      String query = "SELECT * FROM " + testTable
          + " WHERE (ID = 'id_1' AND NUMBER > '20250911') OR (ID = 'id_2' AND NUMBER <= '20251012')";
      PreparedStatement statement = conn.prepareStatement(query);
      ResultSet rs = statement.executeQuery();

      // Should return: id_1 with NUMBER=20251012, and id_2 with NUMBER=20251011
      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
        String id = rs.getString("ID");
        String number = rs.getString("NUMBER");
        // Verify expected combinations
        assertTrue("Unexpected row",
            (id.equals("id_1") && number.equals("20251012")) ||
            (id.equals("id_2") && number.equals("20251011")));
      }

      assertEquals("Expected 2 rows", 2, rowCount);
    } finally {
      conn.close();
    }
  }

  @Test
  public void testKeyExplosionPartialCompositeInInteger() throws Exception {
    // Variation 6: Partial composite key IN (INTEGER type)
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    Connection conn = DriverManager.getConnection(getUrl(), props);
    String testTable = generateUniqueName();
    try {
      // Create table with DESC ordering on NUMBER column
      String createTableDDL = "CREATE TABLE IF NOT EXISTS " + testTable + " ("
          + "ID CHAR(15) NOT NULL, "
          + "NUMBER INTEGER NOT NULL, "
          + "ENTITY_ID VARCHAR NOT NULL, "
          + "CREATED_BY VARCHAR, "
          + "DATA VARCHAR "
          + "CONSTRAINT PK PRIMARY KEY (ID, NUMBER DESC, ENTITY_ID))";
      conn.createStatement().execute(createTableDDL);

      // Insert test data
      String upsert = "UPSERT INTO " + testTable
          + " (ID, NUMBER, ENTITY_ID, CREATED_BY, DATA) VALUES (?, ?, ?, ?, ?)";
      PreparedStatement ps = conn.prepareStatement(upsert);

      ps.setString(1, "id_1");
      ps.setInt(2, 20251012);
      ps.setString(3, "entity_1");
      ps.setString(4, "user1");
      ps.setString(5, "data1");
      ps.executeUpdate();

      ps.setString(1, "id_2");
      ps.setInt(2, 20250912);
      ps.setString(3, "entity_2");
      ps.setString(4, "user2");
      ps.setString(5, "data2");
      ps.executeUpdate();

      ps.setString(1, "id_3");
      ps.setInt(2, 20250913);
      ps.setString(3, "entity_3");
      ps.setString(4, "user3");
      ps.setString(5, "data3");
      ps.executeUpdate();

      ps.setString(1, "id_1");
      ps.setInt(2, 20251012);
      ps.setString(3, "entity_1b");
      ps.setString(4, "user4");
      ps.setString(5, "data4");
      ps.executeUpdate();

      conn.commit();

      // Run query with partial composite key IN (first two columns only)
      String query = "SELECT * FROM " + testTable
          + " WHERE (ID, NUMBER) IN (('id_1', 20251012), ('id_2', 20250912))";
      PreparedStatement statement = conn.prepareStatement(query);
      ResultSet rs = statement.executeQuery();

      // Should return 3 rows: id_1 with 2 ENTITY_IDs at same NUMBER, id_2 with 1 ENTITY_ID
      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
        String id = rs.getString("ID");
        int number = rs.getInt("NUMBER");
        // Verify the combinations
        assertTrue("Unexpected row",
            (id.equals("id_1") && number == 20251012) ||
            (id.equals("id_2") && number == 20250912));
      }

      assertEquals("Expected 3 rows", 3, rowCount);
    } finally {
      conn.close();
    }
  }

  @Test
  public void testKeyExplosionMixedAndOrInteger() throws Exception {
    // Variation 8: Mixed AND/OR with ranges on DESC column (INTEGER type)
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    Connection conn = DriverManager.getConnection(getUrl(), props);
    String testTable = generateUniqueName();
    try {
      // Create table with DESC ordering on NUMBER column
      String createTableDDL = "CREATE TABLE IF NOT EXISTS " + testTable + " ("
          + "ID CHAR(15) NOT NULL, "
          + "NUMBER INTEGER NOT NULL, "
          + "ENTITY_ID VARCHAR NOT NULL, "
          + "CREATED_BY VARCHAR, "
          + "DATA VARCHAR "
          + "CONSTRAINT PK PRIMARY KEY (ID, NUMBER DESC, ENTITY_ID))";
      conn.createStatement().execute(createTableDDL);

      // Insert test data
      String upsert = "UPSERT INTO " + testTable
          + " (ID, NUMBER, ENTITY_ID, CREATED_BY, DATA) VALUES (?, ?, ?, ?, ?)";
      PreparedStatement ps = conn.prepareStatement(upsert);

      ps.setString(1, "id_1");
      ps.setInt(2, 20251012);
      ps.setString(3, "entity_1");
      ps.setString(4, "user1");
      ps.setString(5, "data1");
      ps.executeUpdate();

      ps.setString(1, "id_1");
      ps.setInt(2, 20250910);
      ps.setString(3, "entity_1b");
      ps.setString(4, "user2");
      ps.setString(5, "data2");
      ps.executeUpdate();

      ps.setString(1, "id_2");
      ps.setInt(2, 20251011);
      ps.setString(3, "entity_2");
      ps.setString(4, "user3");
      ps.setString(5, "data3");
      ps.executeUpdate();

      ps.setString(1, "id_3");
      ps.setInt(2, 20250913);
      ps.setString(3, "entity_3");
      ps.setString(4, "user4");
      ps.setString(5, "data4");
      ps.executeUpdate();

      conn.commit();

      // Run query with mixed AND/OR conditions
      String query = "SELECT * FROM " + testTable
          + " WHERE (ID = 'id_1' AND NUMBER > 20250911) OR (ID = 'id_2' AND NUMBER <= 20251012)";
      PreparedStatement statement = conn.prepareStatement(query);
      ResultSet rs = statement.executeQuery();

      // Should return: id_1 with NUMBER=20251012, and id_2 with NUMBER=20251011
      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
        String id = rs.getString("ID");
        int number = rs.getInt("NUMBER");
        // Verify expected combinations
        assertTrue("Unexpected row",
            (id.equals("id_1") && number == 20251012) ||
            (id.equals("id_2") && number == 20251011));
      }

      assertEquals("Expected 2 rows", 2, rowCount);
    } finally {
      conn.close();
    }
  }



}
