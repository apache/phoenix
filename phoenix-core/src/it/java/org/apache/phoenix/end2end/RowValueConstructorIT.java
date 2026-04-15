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

import static org.apache.phoenix.query.QueryConstants.MILLIS_IN_DAY;
import static org.apache.phoenix.util.TestUtil.ENTITYHISTID1;
import static org.apache.phoenix.util.TestUtil.ENTITYHISTID3;
import static org.apache.phoenix.util.TestUtil.ENTITYHISTID7;
import static org.apache.phoenix.util.TestUtil.ENTITYHISTIDS;
import static org.apache.phoenix.util.TestUtil.PARENTID1;
import static org.apache.phoenix.util.TestUtil.PARENTID3;
import static org.apache.phoenix.util.TestUtil.PARENTID7;
import static org.apache.phoenix.util.TestUtil.PARENTIDS;
import static org.apache.phoenix.util.TestUtil.ROW1;
import static org.apache.phoenix.util.TestUtil.ROW2;
import static org.apache.phoenix.util.TestUtil.ROW3;
import static org.apache.phoenix.util.TestUtil.ROW4;
import static org.apache.phoenix.util.TestUtil.ROW5;
import static org.apache.phoenix.util.TestUtil.ROW6;
import static org.apache.phoenix.util.TestUtil.ROW7;
import static org.apache.phoenix.util.TestUtil.ROW8;
import static org.apache.phoenix.util.TestUtil.ROW9;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.phoenix.thirdparty.com.google.common.base.Joiner;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

@Category(ParallelStatsDisabledTest.class)
public class RowValueConstructorIT extends ParallelStatsDisabledIT {

  @Test
  public void testRowValueConstructorInWhereWithEqualsExpression() throws Exception {
    String tenantId = getOrganizationId();
    String tableName =
      initATableValues(null, tenantId, getDefaultSplits(tenantId), null, null, getUrl(), null);
    String query = "SELECT a_integer, x_integer FROM " + tableName
      + " WHERE ?=organization_id  AND (a_integer, x_integer) = (7, 5)";
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    Connection conn = DriverManager.getConnection(getUrl(), props);
    try {
      PreparedStatement statement = conn.prepareStatement(query);
      statement.setString(1, tenantId);
      ResultSet rs = statement.executeQuery();
      int count = 0;
      while (rs.next()) {
        assertTrue(rs.getInt(1) == 7);
        assertTrue(rs.getInt(2) == 5);
        count++;
      }
      assertTrue(count == 1);
    } finally {
      conn.close();
    }
  }

  @Test
  public void testRowValueConstructorInWhereWithGreaterThanExpression() throws Exception {
    String tenantId = getOrganizationId();
    String tableName =
      initATableValues(null, tenantId, getDefaultSplits(tenantId), null, null, getUrl(), null);
    String query = "SELECT a_integer, x_integer FROM " + tableName
      + " WHERE ?=organization_id  AND (a_integer, x_integer) >= (4, 4)";
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    Connection conn = DriverManager.getConnection(getUrl(), props);
    try {
      PreparedStatement statement = conn.prepareStatement(query);
      statement.setString(1, tenantId);
      ResultSet rs = statement.executeQuery();
      int count = 0;
      while (rs.next()) {
        assertTrue(rs.getInt(1) >= 4);
        assertTrue(rs.getInt(1) == 4 ? rs.getInt(2) >= 4 : rs.getInt(2) >= 0);
        count++;
      }
      // we have 6 values for a_integer present in the atable where a >= 4. x_integer is null for
      // a_integer = 4. So the query should have returned 5 rows.
      assertEquals(5, count);
    } finally {
      conn.close();
    }
  }

  @Test
  public void testRowValueConstructorInWhereWithUnEqualNumberArgs() throws Exception {
    String tenantId = getOrganizationId();
    String tableName =
      initATableValues(null, tenantId, getDefaultSplits(tenantId), null, null, getUrl(), null);
    String query = "SELECT a_integer, x_integer FROM " + tableName
      + " WHERE ?=organization_id  AND (a_integer, x_integer, y_integer) >= (7, 5)";
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    Connection conn = DriverManager.getConnection(getUrl(), props);
    try {
      PreparedStatement statement = conn.prepareStatement(query);
      statement.setString(1, tenantId);
      ResultSet rs = statement.executeQuery();
      int count = 0;
      while (rs.next()) {
        assertTrue(rs.getInt(1) >= 7);
        assertTrue(rs.getInt(1) == 7 ? rs.getInt(2) >= 5 : rs.getInt(2) >= 0);
        count++;
      }
      // we have key values (7,5) (8,4) and (9,3) present in aTable. So the query should return the
      // 3 records.
      assertTrue(count == 3);
    } finally {
      conn.close();
    }
  }

  @Test
  public void testBindVarsInRowValueConstructor() throws Exception {
    String tenantId = getOrganizationId();
    String tableName =
      initATableValues(null, tenantId, getDefaultSplits(tenantId), null, null, getUrl(), null);
    String query = "SELECT a_integer, x_integer FROM " + tableName
      + " WHERE ?=organization_id  AND (a_integer, x_integer) = (?, ?)";
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    Connection conn = DriverManager.getConnection(getUrl(), props);
    try {
      PreparedStatement statement = conn.prepareStatement(query);
      statement.setString(1, tenantId);
      statement.setInt(2, 7);
      statement.setInt(3, 5);
      ResultSet rs = statement.executeQuery();
      int count = 0;
      while (rs.next()) {
        assertTrue(rs.getInt(1) == 7);
        assertTrue(rs.getInt(2) == 5);
        count++;
      }
      assertTrue(count == 1);
    } finally {
      conn.close();
    }
  }

  @Test
  public void testRowValueConstructorOnLHSAndLiteralExpressionOnRHS() throws Exception {
    String tenantId = getOrganizationId();
    String tableName =
      initATableValues(null, tenantId, getDefaultSplits(tenantId), null, null, getUrl(), null);
    String query = "SELECT a_integer, x_integer FROM " + tableName
      + " WHERE ?=organization_id  AND (a_integer, x_integer) >= 7";
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    Connection conn = DriverManager.getConnection(getUrl(), props);
    try {
      PreparedStatement statement = conn.prepareStatement(query);
      statement.setString(1, tenantId);
      ResultSet rs = statement.executeQuery();
      int count = 0;
      while (rs.next()) {
        count++;
      }
      // we have key values (7,5) (8,4) and (9,3) present in aTable. So the query should return the
      // 3 records.
      assertTrue(count == 3);
    } finally {
      conn.close();
    }
  }

  @Test
  public void testRowValueConstructorOnRHSLiteralExpressionOnLHS() throws Exception {
    String tenantId = getOrganizationId();
    String tableName =
      initATableValues(null, tenantId, getDefaultSplits(tenantId), null, null, getUrl(), null);
    String query = "SELECT a_integer, x_integer FROM " + tableName
      + " WHERE ?=organization_id  AND 7 <= (a_integer, x_integer)";
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    Connection conn = DriverManager.getConnection(getUrl(), props);
    try {
      PreparedStatement statement = conn.prepareStatement(query);
      statement.setString(1, tenantId);
      ResultSet rs = statement.executeQuery();
      int count = 0;
      while (rs.next()) {
        count++;
      }
      // we have key values (7,5) (8,4) and (9,3) present in aTable. So the query should return the
      // 3 records.
      assertTrue(count == 3);
    } finally {
      conn.close();
    }
  }

  @Test
  public void testRowValueConstructorOnLHSBuiltInFunctionOperatingOnIntegerLiteralRHS()
    throws Exception {
    String tenantId = getOrganizationId();
    String tableName =
      initATableValues(null, tenantId, getDefaultSplits(tenantId), null, null, getUrl(), null);
    String query = "SELECT a_integer, x_integer FROM " + tableName
      + " WHERE ?=organization_id  AND (a_integer, x_integer) >= to_number('7')";
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    Connection conn = DriverManager.getConnection(getUrl(), props);
    try {
      PreparedStatement statement = conn.prepareStatement(query);
      statement.setString(1, tenantId);
      ResultSet rs = statement.executeQuery();
      int count = 0;
      while (rs.next()) {
        count++;
      }
      // we have key values (7,5) (8,4) and (9,3) present in aTable. So the query should return the
      // 3 records.
      assertEquals(3, count);
    } finally {
      conn.close();
    }
  }

  @Test
  public void testRowValueConstructorOnRHSWithBuiltInFunctionOperatingOnIntegerLiteralOnLHS()
    throws Exception {
    String tenantId = getOrganizationId();
    String tableName =
      initATableValues(null, tenantId, getDefaultSplits(tenantId), null, null, getUrl(), null);
    String query = "SELECT a_integer, x_integer FROM " + tableName
      + " WHERE ?=organization_id  AND to_number('7') <= (a_integer, x_integer)";
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    Connection conn = DriverManager.getConnection(getUrl(), props);
    try {
      PreparedStatement statement = conn.prepareStatement(query);
      statement.setString(1, tenantId);
      ResultSet rs = statement.executeQuery();
      int count = 0;
      while (rs.next()) {
        count++;
      }
      // we have key values (7,5) (8,4) and (9,3) present in aTable. So the query should return the
      // 3 records.
      assertEquals(3, count);
    } finally {
      conn.close();
    }
  }

  @Test
  public void testRowValueConstructorOnLHSWithBuiltInFunctionOperatingOnColumnRefOnRHS()
    throws Exception {
    String tenantId = getOrganizationId();
    String tableName =
      initATableValues(null, tenantId, getDefaultSplits(tenantId), null, null, getUrl(), null);
    String upsertQuery =
      "UPSERT INTO " + tableName + " (organization_id, entity_id, a_string) values (?, ?, ?)";
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    Connection conn = DriverManager.getConnection(getUrl(), props);
    conn.setAutoCommit(true);
    try {
      PreparedStatement statement = conn.prepareStatement(upsertQuery);
      statement.setString(1, tenantId);
      statement.setString(2, ROW1);
      statement.setString(3, "1");
      statement.executeUpdate();
      statement.setString(1, tenantId);
      statement.setString(2, ROW2);
      statement.setString(3, "1");
      statement.executeUpdate();
      statement.setString(1, tenantId);
      statement.setString(2, ROW3);
      statement.setString(3, "1");
      statement.executeUpdate();
      statement.setString(1, tenantId);
      statement.setString(2, ROW4);
      statement.setString(3, "1");
      statement.executeUpdate();
      statement.setString(1, tenantId);
      statement.setString(2, ROW5);
      statement.setString(3, "1");
      statement.executeUpdate();
      statement.setString(1, tenantId);
      statement.setString(2, ROW6);
      statement.setString(3, "1");
      statement.executeUpdate();
      statement.setString(1, tenantId);
      statement.setString(2, ROW7);
      statement.setString(3, "7");
      statement.executeUpdate();
      statement.setString(1, tenantId);
      statement.setString(2, ROW8);
      statement.setString(3, "7");
      statement.executeUpdate();
      statement.setString(1, tenantId);
      statement.setString(2, ROW9);
      statement.setString(3, "7");
      statement.executeUpdate();
      conn.commit();

      statement = conn.prepareStatement("select a_string from " + tableName
        + " where organization_id = ? and (6, x_integer) <= to_number(a_string)");
      statement.setString(1, tenantId);
      ResultSet rs = statement.executeQuery();
      int count = 0;
      while (rs.next()) {
        count++;
      }
      // we have key values (7,5) (8,4) and (9,3) present in aTable. So the query should return the
      // 3 records.
      assertTrue(count == 3);
    } finally {
      conn.close();
    }
  }

  @Test
  public void testRowValueConstructorOnRHSWithBuiltInFunctionOperatingOnColumnRefOnLHS()
    throws Exception {
    String tenantId = getOrganizationId();
    String tableName =
      initATableValues(null, tenantId, getDefaultSplits(tenantId), null, null, getUrl(), null);
    String upsertQuery =
      "UPSERT INTO " + tableName + "(organization_id, entity_id, a_string) values (?, ?, ?)";
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    Connection conn = DriverManager.getConnection(getUrl(), props);
    conn.setAutoCommit(true);
    try {
      PreparedStatement statement = conn.prepareStatement(upsertQuery);
      statement.setString(1, tenantId);
      statement.setString(2, ROW1);
      statement.setString(3, "1");
      statement.executeUpdate();
      statement.setString(1, tenantId);
      statement.setString(2, ROW2);
      statement.setString(3, "1");
      statement.executeUpdate();
      statement.setString(1, tenantId);
      statement.setString(2, ROW3);
      statement.setString(3, "1");
      statement.executeUpdate();
      statement.setString(1, tenantId);
      statement.setString(2, ROW4);
      statement.setString(3, "1");
      statement.executeUpdate();
      statement.setString(1, tenantId);
      statement.setString(2, ROW5);
      statement.setString(3, "1");
      statement.executeUpdate();
      statement.setString(1, tenantId);
      statement.setString(2, ROW6);
      statement.setString(3, "1");
      statement.executeUpdate();
      statement.setString(1, tenantId);
      statement.setString(2, ROW7);
      statement.setString(3, "7");
      statement.executeUpdate();
      statement.setString(1, tenantId);
      statement.setString(2, ROW8);
      statement.setString(3, "7");
      statement.executeUpdate();
      statement.setString(1, tenantId);
      statement.setString(2, ROW9);
      statement.setString(3, "7");
      statement.executeUpdate();
      conn.commit();

      statement = conn.prepareStatement("select a_string from " + tableName
        + " where organization_id = ? and to_number(a_string) >= (6, 6)");
      statement.setString(1, tenantId);
      ResultSet rs = statement.executeQuery();
      int count = 0;
      while (rs.next()) {
        count++;
      }
      // we have key values (7,5) (8,4) and (9,3) present in aTable. So the query should return the
      // 3 records.
      assertEquals(3, count);
    } finally {
      conn.close();
    }
  }

  @Test
  public void testQueryMoreWithInListRowValueConstructor() throws Exception {
    String tenantId = getOrganizationId();
    Date date = new Date(System.currentTimeMillis());
    String tableName =
      initEntityHistoryTableValues(null, tenantId, getDefaultSplits(tenantId), date, null);
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    Connection conn = DriverManager.getConnection(getUrl(), props);

    PreparedStatement statement = conn.prepareStatement("select parent_id from " + tableName
      + " WHERE (organization_id, parent_id, created_date, entity_history_id) IN ((?, ?, ?, ?),(?,?,?,?))");
    statement.setString(1, tenantId);
    statement.setString(2, PARENTID3);
    statement.setDate(3, date);
    statement.setString(4, ENTITYHISTID3);
    statement.setString(5, tenantId);
    statement.setString(6, PARENTID7);
    statement.setDate(7, date);
    statement.setString(8, ENTITYHISTID7);
    ResultSet rs = statement.executeQuery();

    assertTrue(rs.next());
    assertEquals(PARENTID3, rs.getString(1));
    assertTrue(rs.next());
    assertEquals(PARENTID7, rs.getString(1));
    assertFalse(rs.next());
  }

  @Test
  public void testQueryMoreFunctionalityUsingAllPKColsInRowValueConstructor() throws Exception {
    _testQueryMoreFunctionalityUsingAllPkColsInRowValueConstructor(false);
  }

  @Test
  public void testQueryMoreFunctionalityUsingAllPKColsInRowValueConstructor_Salted()
    throws Exception {
    _testQueryMoreFunctionalityUsingAllPkColsInRowValueConstructor(true);
  }

  private void _testQueryMoreFunctionalityUsingAllPkColsInRowValueConstructor(boolean salted)
    throws Exception, SQLException {
    String tenantId = getOrganizationId();
    Date date = new Date(System.currentTimeMillis());
    String tableName;
    if (salted) {
      tableName = initSaltedEntityHistoryTableValues(null, tenantId, null, date, null);
    } else {
      tableName =
        initEntityHistoryTableValues(null, tenantId, getDefaultSplits(tenantId), date, null);
    }
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    Connection conn = DriverManager.getConnection(getUrl(), props);

    String startingOrgId = tenantId;
    String startingParentId = PARENTID1;
    Date startingDate = date;
    String startingEntityHistId = ENTITYHISTID1;
    PreparedStatement statement = null;
    statement = conn.prepareStatement(
      "select organization_id, parent_id, created_date, entity_history_id, old_value, new_value from "
        + tableName
        + " WHERE (organization_id, parent_id, created_date, entity_history_id) > (?, ?, ?, ?) ORDER BY organization_id, parent_id, created_date, entity_history_id LIMIT 3 ");
    statement.setString(1, startingOrgId);
    statement.setString(2, startingParentId);
    statement.setDate(3, startingDate);
    statement.setString(4, startingEntityHistId);
    ResultSet rs = statement.executeQuery();

    int count = 0;
    int i = 1;
    // this loop should work on rows 2, 3, 4.
    while (rs.next()) {
      assertTrue(rs.getString(2).equals(PARENTIDS.get(i)));
      assertTrue(rs.getString(4).equals(ENTITYHISTIDS.get(i)));
      count++;
      i++;
      if (count == 3) {
        startingOrgId = rs.getString(1);
        startingParentId = rs.getString(2);
        startingDate = rs.getDate(3);
        startingEntityHistId = rs.getString(4);
      }
    }

    assertTrue("Number of rows returned: ", count == 3);
    // We will now use the row 4's pk values for bind variables.
    statement.setString(1, startingOrgId);
    statement.setString(2, startingParentId);
    statement.setDate(3, startingDate);
    statement.setString(4, startingEntityHistId);
    rs = statement.executeQuery();
    // this loop now should work on rows 5, 6, 7.
    while (rs.next()) {
      assertTrue(rs.getString(2).equals(PARENTIDS.get(i)));
      assertTrue(rs.getString(4).equals(ENTITYHISTIDS.get(i)));
      i++;
      count++;
    }
    assertTrue("Number of rows returned: ", count == 6);
  }

  @Test
  public void testQueryMoreWithSubsetofPKColsInRowValueConstructor() throws Exception {
    _testQueryMoreWithSubsetofPKColsInRowValueConstructor(false);
  }

  @Test
  public void testQueryMoreWithSubsetofPKColsInRowValueConstructor_salted() throws Exception {
    _testQueryMoreWithSubsetofPKColsInRowValueConstructor(true);
  }

  /**
   * Entity History table has primary keys defined in the order PRIMARY KEY (organization_id,
   * parent_id, created_date, entity_history_id). This test uses (organization_id, parent_id,
   * entity_history_id) in RVC and checks if the query more functionality still works.
   */
  private void _testQueryMoreWithSubsetofPKColsInRowValueConstructor(boolean salted)
    throws Exception {
    String tenantId = getOrganizationId();
    Date date = new Date(System.currentTimeMillis());
    String tableName;
    if (salted) {
      tableName = initSaltedEntityHistoryTableValues(null, tenantId, null, date, null);
    } else {
      tableName =
        initEntityHistoryTableValues(null, tenantId, getDefaultSplits(tenantId), date, null);
    }
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    Connection conn = DriverManager.getConnection(getUrl(), props);

    // initial values of pk.
    String startingOrgId = tenantId;
    String startingParentId = PARENTID1;

    String startingEntityHistId = ENTITYHISTID1;

    PreparedStatement statement = null;
    statement = conn.prepareStatement(
      "select organization_id, parent_id, created_date, entity_history_id, old_value, new_value from "
        + tableName
        + " WHERE (organization_id, parent_id, entity_history_id) > (?, ?, ?) ORDER BY organization_id, parent_id, entity_history_id LIMIT 3 ");
    statement.setString(1, startingOrgId);
    statement.setString(2, startingParentId);
    statement.setString(3, startingEntityHistId);
    ResultSet rs = statement.executeQuery();
    int count = 0;
    // this loop should work on rows 2, 3, 4.
    int i = 1;
    while (rs.next()) {
      assertTrue(rs.getString(2).equals(PARENTIDS.get(i)));
      assertTrue(rs.getString(4).equals(ENTITYHISTIDS.get(i)));
      i++;
      count++;
      if (count == 3) {
        startingOrgId = rs.getString(1);
        startingParentId = rs.getString(2);
        startingEntityHistId = rs.getString(4);
      }
    }
    assertTrue("Number of rows returned: " + count, count == 3);
    // We will now use the row 4's pk values for bind variables.
    statement.setString(1, startingOrgId);
    statement.setString(2, startingParentId);
    statement.setString(3, startingEntityHistId);
    rs = statement.executeQuery();
    // this loop now should work on rows 5, 6, 7.
    while (rs.next()) {
      assertTrue(rs.getString(2).equals(PARENTIDS.get(i)));
      assertTrue(rs.getString(4).equals(ENTITYHISTIDS.get(i)));
      i++;
      count++;
    }
    assertTrue("Number of rows returned: " + count, count == 6);
  }

  @Test
  public void testQueryMoreWithLeadingPKColSkippedInRowValueConstructor() throws Exception {
    _testQueryMoreWithLeadingPKColSkippedInRowValueConstructor(false);
  }

  @Test
  public void testQueryMoreWithLeadingPKColSkippedInRowValueConstructor_salted() throws Exception {
    _testQueryMoreWithLeadingPKColSkippedInRowValueConstructor(true);
  }

  /**
   * Entity History table has primary keys defined in the order PRIMARY KEY (organization_id,
   * parent_id, created_date, entity_history_id). This test skips the leading column organization_id
   * and uses (parent_id, created_date, entity_history_id) in RVC. In such a case Phoenix won't be
   * able to optimize the hbase scan. However, the query more functionality should still work.
   */
  private void _testQueryMoreWithLeadingPKColSkippedInRowValueConstructor(boolean salted)
    throws Exception {
    String tenantId = getOrganizationId();
    Date date = new Date(System.currentTimeMillis());
    String tableName;
    if (salted) {
      tableName = initSaltedEntityHistoryTableValues(null, tenantId, null, date, null);
    } else {
      tableName =
        initEntityHistoryTableValues(null, tenantId, getDefaultSplits(tenantId), date, null);
    }
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    Connection conn = DriverManager.getConnection(getUrl(), props);
    String startingParentId = PARENTID1;
    Date startingDate = date;
    String startingEntityHistId = ENTITYHISTID1;
    PreparedStatement statement = null;
    statement = conn.prepareStatement(
      "select organization_id, parent_id, created_date, entity_history_id, old_value, new_value from "
        + tableName
        + " WHERE (parent_id, created_date, entity_history_id) > (?, ?, ?) ORDER BY parent_id, created_date, entity_history_id LIMIT 3 ");
    statement.setString(1, startingParentId);
    statement.setDate(2, startingDate);
    statement.setString(3, startingEntityHistId);
    ResultSet rs = statement.executeQuery();
    int count = 0;
    // this loop should work on rows 2, 3, 4.
    int i = 1;
    while (rs.next()) {
      assertTrue(rs.getString(2).equals(PARENTIDS.get(i)));
      assertTrue(rs.getString(4).equals(ENTITYHISTIDS.get(i)));
      i++;
      count++;
      if (count == 3) {
        startingParentId = rs.getString(2);
        startingDate = rs.getDate(3);
        startingEntityHistId = rs.getString(4);
      }
    }
    assertTrue("Number of rows returned: " + count, count == 3);
    // We will now use the row 4's pk values for bind variables.
    statement.setString(1, startingParentId);
    statement.setDate(2, startingDate);
    statement.setString(3, startingEntityHistId);
    rs = statement.executeQuery();
    // this loop now should work on rows 5, 6, 7.
    while (rs.next()) {
      assertTrue(rs.getString(2).equals(PARENTIDS.get(i)));
      assertTrue(rs.getString(4).equals(ENTITYHISTIDS.get(i)));
      i++;
      count++;
    }
    assertTrue("Number of rows returned: " + count, count == 6);
  }

  @Test
  public void testRVCWithNonLeadingPkColsOfTypesIntegerAndString() throws Exception {
    String tenantId = getOrganizationId();
    String tableName =
      initATableValues(null, tenantId, getDefaultSplits(tenantId), null, null, getUrl(), null);
    String query = "SELECT a_integer, a_string FROM " + tableName
      + " WHERE ?=organization_id  AND (a_integer, a_string) <= (5, 'a')";
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    Connection conn = DriverManager.getConnection(getUrl(), props);
    try {
      PreparedStatement statement = conn.prepareStatement(query);
      statement.setString(1, tenantId);
      ResultSet rs = statement.executeQuery();
      int count = 0;
      // we have 4 rows present in a table with (a_integer, a_string) <= (5, 'a'). All have a_string
      // set to "a".
      while (rs.next()) {
        assertTrue(rs.getInt(1) <= 5);
        assertTrue(rs.getString(2).compareTo("a") == 0);
        count++;
      }
      assertTrue(count == 4);
    } finally {
      conn.close();
    }
  }

  @Test
  public void testRVCWithNonLeadingPkColsOfTypesTimeStampAndString() throws Exception {
    String tenantId = getOrganizationId();
    String tableName =
      initATableValues(null, tenantId, getDefaultSplits(tenantId), null, null, getUrl(), null);
    String updateStmt = "upsert into " + tableName + "(" + "    ORGANIZATION_ID, "
      + "    ENTITY_ID, " + "    A_TIMESTAMP) " + "VALUES (?, ?, ?)";
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    Connection upsertConn = DriverManager.getConnection(getUrl(), props);
    upsertConn.setAutoCommit(true);
    PreparedStatement stmt = upsertConn.prepareStatement(updateStmt);
    stmt.setString(1, tenantId);
    stmt.setString(2, ROW4);
    Timestamp tsValue = new Timestamp(System.nanoTime());
    stmt.setTimestamp(3, tsValue);
    stmt.execute();

    String query = "SELECT a_timestamp, a_string FROM " + tableName
      + " WHERE ?=organization_id  AND (a_timestamp, a_string) = (?, 'a')";
    props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    Connection conn = DriverManager.getConnection(getUrl(), props);
    try {
      PreparedStatement statement = conn.prepareStatement(query);
      statement.setString(1, tenantId);
      statement.setTimestamp(2, tsValue);
      ResultSet rs = statement.executeQuery();
      int count = 0;
      while (rs.next()) {
        assertTrue(rs.getTimestamp(1).equals(tsValue));
        assertTrue(rs.getString(2).compareTo("a") == 0);
        count++;
      }
      assertTrue(count == 1);
    } finally {
      conn.close();
    }
  }

  @Test
  public void testNestedRVCBasic() throws Exception {
    String tenantId = getOrganizationId();
    String tableName =
      initATableValues(null, tenantId, getDefaultSplits(tenantId), null, null, getUrl(), null);
    // all the three queries should return the same rows.
    String[] queries = {
      "SELECT organization_id, entity_id, a_string FROM " + tableName
        + " WHERE ((organization_id, entity_id), a_string) >= ((?, ?), ?)",
      "SELECT organization_id, entity_id, a_string FROM " + tableName
        + " WHERE (organization_id, entity_id, a_string) >= (?, ?, ?)",
      "SELECT organization_id, entity_id, a_string FROM " + tableName
        + " WHERE (organization_id, (entity_id, a_string)) >= (?, (?, ?))" };
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    Connection conn = DriverManager.getConnection(getUrl(), props);
    PreparedStatement statement = null;
    try {
      try {
        for (int i = 0; i <= 2; i++) {
          statement = conn.prepareStatement(queries[i]);
          statement.setString(1, tenantId);
          statement.setString(2, ROW1);
          statement.setString(3, "a");
          ResultSet rs = statement.executeQuery();
          int count = 0;
          while (rs.next()) {
            count++;
          }
          assertEquals(9, count);
        }
      } finally {
        if (statement != null) {
          statement.close();
        }
      }
    } finally {
      conn.close();
    }
  }

  @Test
  public void testRVCWithInListClausePossibleNullValues() throws Exception {
    String tenantId = getOrganizationId();
    String tableName =
      initATableValues(null, tenantId, getDefaultSplits(tenantId), null, null, getUrl(), null);
    // we have a row present in aTable where x_integer = 5 and y_integer = NULL which gets
    // translated to 0 when retriving from HBase.
    String query = "SELECT x_integer, y_integer FROM " + tableName
      + " WHERE ? = organization_id AND (x_integer, y_integer) IN ((5))";
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    Connection conn = DriverManager.getConnection(getUrl(), props);
    try {
      PreparedStatement statement = conn.prepareStatement(query);
      statement.setString(1, tenantId);
      ResultSet rs = statement.executeQuery();
      assertTrue(rs.next());
      assertEquals(5, rs.getInt(1));
      assertEquals(0, rs.getInt(2));
    } finally {
      conn.close();
    }
  }

  @Test
  public void testRVCWithInListClauseUsingSubsetOfPKColsInOrder() throws Exception {
    String tenantId = getOrganizationId();
    String tableName =
      initATableValues(null, tenantId, getDefaultSplits(tenantId), null, null, getUrl(), null);
    // Though we have a row present in aTable where organization_id = tenantId and x_integer = 5,
    // we'd also need to have an entity_id that is null (which we don't have).
    String query = "SELECT organization_id, entity_id FROM " + tableName
      + " WHERE (organization_id, entity_id) IN (('" + tenantId + "')) AND x_integer = 5";
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    Connection conn = null;
    PreparedStatement statement = null;
    try {
      try {
        conn = DriverManager.getConnection(getUrl(), props);
        statement = conn.prepareStatement(query);
        ResultSet rs = statement.executeQuery();
        assertFalse(rs.next());
      } finally {
        if (statement != null) {
          statement.close();
        }
      }
    } finally {
      if (conn != null) {
        conn.close();
      }
    }
  }

  @Test
  public void testRVCWithCeilAndFloorNeededForDecimal() throws Exception {
    String tenantId = getOrganizationId();
    String tableName =
      initATableValues(null, tenantId, getDefaultSplits(tenantId), null, null, getUrl(), null);
    String query = "SELECT a_integer, x_integer FROM " + tableName
      + " WHERE ?=organization_id  AND (a_integer, x_integer) < (8.6, 4.5) AND (a_integer, x_integer) > (6.8, 4)";
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    Connection conn = DriverManager.getConnection(getUrl(), props);
    try {
      PreparedStatement statement = conn.prepareStatement(query);
      statement.setString(1, tenantId);
      ResultSet rs = statement.executeQuery();
      int count = 0;
      while (rs.next()) {
        count++;
        assertEquals(7, rs.getInt(1));
        assertEquals(5, rs.getInt(2));
      }
      assertEquals(1, count);
    } finally {
      conn.close();
    }
  }

  @Test
  public void testRVCWithCeilAndFloorNeededForTimestamp() throws Exception {
    String tenantId = getOrganizationId();
    Date dateUpserted = DateUtil.parseDate("2012-01-01 14:25:28");
    dateUpserted = new Date(dateUpserted.getTime() + 660); // this makes the dateUpserted equivalent
                                                           // to 2012-01-01 14:25:28.660
    String tableName = initATableValues(null, tenantId, getDefaultSplits(tenantId), dateUpserted,
      null, getUrl(), null);
    String query = "SELECT a_integer, a_date FROM " + tableName
      + " WHERE ?=organization_id  AND (a_integer, a_date) <= (9, ?) AND (a_integer, a_date) >= (6, ?)";
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    Connection conn = DriverManager.getConnection(getUrl(), props);
    try {
      PreparedStatement statement = conn.prepareStatement(query);
      statement.setString(1, tenantId);
      Timestamp timestampWithNanos =
        DateUtil.getTimestamp(dateUpserted.getTime() + 2 * MILLIS_IN_DAY, 300);
      timestampWithNanos.setNanos(0);
      statement.setTimestamp(2, timestampWithNanos);
      statement.setTimestamp(3, timestampWithNanos);
      ResultSet rs = statement.executeQuery();
      int count = 0;
      while (rs.next()) {
        count++;
      }
      /*
       * We have following rows with values for (a_integer, a_date) present: (7, date), (8, date +
       * TestUtil.MILLIS_IN_DAY), (9, date 2 * TestUtil.MILLIS_IN_DAY) among others present. Above
       * query should return 3 rows since the CEIL operation on timestamp with non-zero nanoseconds
       * will bump up the milliseconds. That is (a_integer, a_date) < (6, CEIL(timestampWithNanos)).
       */
      assertEquals(3, count);
    } finally {
      conn.close();
    }
  }

  @Test
  public void testRVCWithMultiCompKeysForIn() throws Exception {
    String tableName = generateUniqueName();
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    Connection conn = DriverManager.getConnection(getUrl(), props);
    conn.createStatement().execute("CREATE TABLE " + tableName
      + " (pk1 varchar, pk2 varchar, constraint pk primary key (pk1,pk2))");
    conn.close();

    conn = DriverManager.getConnection(getUrl(), props);
    conn.setAutoCommit(true);
    conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a','a')");
    conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('b','b')");
    conn.close();

    conn = DriverManager.getConnection(getUrl(), props);
    ResultSet rs = conn.createStatement()
      .executeQuery("SELECT * FROM " + tableName + " WHERE (pk1,pk2) IN (('a','a'),('b','b'))");
    assertTrue(rs.next());
    assertEquals("a", rs.getString(1));
    assertEquals("a", rs.getString(2));
    assertTrue(rs.next());
    assertEquals("b", rs.getString(1));
    assertEquals("b", rs.getString(2));
    assertFalse(rs.next());
  }

  private Connection nextConnection(String url) throws SQLException {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    return DriverManager.getConnection(url, props);
  }

  // Table type - multi-tenant. Salted - No. Query against - tenant specific view. Connection used
  // for running IN list query - tenant specific.
  @Test
  public void testInListOfRVC1() throws Exception {
    String tenantId = "ABC";
    String tableName = generateUniqueName();
    String viewName = generateUniqueName();
    String tenantSpecificUrl = getUrl() + ";" + PhoenixRuntime.TENANT_ID_ATTRIB + '=' + tenantId;
    String baseTableDDL = "CREATE TABLE " + tableName
      + " (tenantId varchar(5) NOT NULL, pk2 varchar(5) NOT NULL, pk3 INTEGER NOT NULL, c1 INTEGER constraint pk primary key (tenantId,pk2,pk3)) MULTI_TENANT=true";
    createTestTable(getUrl(), baseTableDDL, null, null);
    String tenantTableDDL = "CREATE VIEW " + viewName + " (tenant_col VARCHAR) AS SELECT *\n"
      + "                FROM " + tableName;
    createTestTable(tenantSpecificUrl, tenantTableDDL, null, null);

    Connection conn = nextConnection(tenantSpecificUrl);
    conn.createStatement()
      .executeUpdate("upsert into " + viewName + " (pk2, pk3, c1) values ('helo1', 1, 1)");
    conn.createStatement()
      .executeUpdate("upsert into " + viewName + " (pk2, pk3, c1) values ('helo2', 2, 2)");
    conn.createStatement()
      .executeUpdate("upsert into " + viewName + " (pk2, pk3, c1) values ('helo3', 3, 3)");
    conn.createStatement()
      .executeUpdate("upsert into " + viewName + " (pk2, pk3, c1) values ('helo4', 4, 4)");
    conn.createStatement()
      .executeUpdate("upsert into " + viewName + " (pk2, pk3, c1) values ('helo5', 5, 5)");
    conn.commit();
    conn.close();

    conn = nextConnection(tenantSpecificUrl);
    // order by needed on the query to make the order of rows returned deterministic.
    PreparedStatement stmt = conn.prepareStatement(
      "select pk2, pk3 from " + viewName + " WHERE (pk2, pk3) IN ((?, ?), (?, ?)) ORDER BY pk2");
    stmt.setString(1, "helo3");
    stmt.setInt(2, 3);
    stmt.setString(3, "helo5");
    stmt.setInt(4, 5);

    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertEquals("helo3", rs.getString(1));
    assertEquals(3, rs.getInt(2));
    assertTrue(rs.next());
    assertEquals("helo5", rs.getString(1));
    assertEquals(5, rs.getInt(2));
    conn.close();
  }

  // Table type - multi-tenant. Salted - No. Query against - base table. Connection used for running
  // IN list query - global.
  @Test
  public void testInListOfRVC2() throws Exception {
    String tenantId = "ABC";
    String tableName = generateUniqueName();
    String viewName = generateUniqueName();
    String tenantSpecificUrl = getUrl() + ";" + PhoenixRuntime.TENANT_ID_ATTRIB + '=' + tenantId;
    String baseTableDDL = "CREATE TABLE " + tableName
      + " (tenantId varchar(5) NOT NULL, pk2 varchar(5) NOT NULL, pk3 INTEGER NOT NULL, c1 INTEGER constraint pk primary key (tenantId,pk2,pk3)) MULTI_TENANT=true";
    createTestTable(getUrl(), baseTableDDL, null, null);
    String tenantTableDDL = "CREATE VIEW " + viewName + " (tenant_col VARCHAR) AS SELECT *\n"
      + "                FROM " + tableName;
    createTestTable(tenantSpecificUrl, tenantTableDDL, null, null);

    Connection conn = nextConnection(tenantSpecificUrl);
    conn.createStatement()
      .executeUpdate("upsert into " + viewName + " (pk2, pk3, c1) values ('helo1', 1, 1)");
    conn.createStatement()
      .executeUpdate("upsert into " + viewName + " (pk2, pk3, c1) values ('helo2', 2, 2)");
    conn.createStatement()
      .executeUpdate("upsert into " + viewName + " (pk2, pk3, c1) values ('helo3', 3, 3)");
    conn.createStatement()
      .executeUpdate("upsert into " + viewName + " (pk2, pk3, c1) values ('helo4', 4, 4)");
    conn.createStatement()
      .executeUpdate("upsert into " + viewName + " (pk2, pk3, c1) values ('helo5', 5, 5)");
    conn.commit();
    conn.close();

    conn = nextConnection(getUrl());
    // order by needed on the query to make the order of rows returned deterministic.
    PreparedStatement stmt = conn.prepareStatement("select pk2, pk3 from " + tableName
      + " WHERE (tenantId, pk2, pk3) IN ((?, ?, ?), (?, ?, ?)) ORDER BY pk2");
    stmt.setString(1, tenantId);
    stmt.setString(2, "helo3");
    stmt.setInt(3, 3);
    stmt.setString(4, tenantId);
    stmt.setString(5, "helo5");
    stmt.setInt(6, 5);

    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertEquals("helo3", rs.getString(1));
    assertEquals(3, rs.getInt(2));
    assertTrue(rs.next());
    assertEquals("helo5", rs.getString(1));
    assertEquals(5, rs.getInt(2));
    conn.close();
  }

  // Table type - non multi-tenant. Salted - No. Query against - Table. Connection used for running
  // IN list query - global.
  @Test
  public void testInListOfRVC3() throws Exception {
    String tenantId = "ABC";
    String tableName = generateUniqueName();
    String tableDDL = "CREATE TABLE " + tableName
      + " (tenantId varchar(5) NOT NULL, pk2 varchar(5) NOT NULL, pk3 INTEGER NOT NULL, c1 INTEGER constraint pk primary key (tenantId,pk2,pk3))";
    createTestTable(getUrl(), tableDDL, null, null);

    Connection conn = nextConnection(getUrl());
    conn.createStatement().executeUpdate(
      "upsert into " + tableName + " (tenantId, pk2, pk3, c1) values ('ABC', 'helo1', 1, 1)");
    conn.createStatement().executeUpdate(
      "upsert into " + tableName + " (tenantId, pk2, pk3, c1) values ('ABC', 'helo2', 2, 2)");
    conn.createStatement().executeUpdate(
      "upsert into " + tableName + " (tenantId, pk2, pk3, c1) values ('ABC', 'helo3', 3, 3)");
    conn.createStatement().executeUpdate(
      "upsert into " + tableName + " (tenantId, pk2, pk3, c1) values ('ABC', 'helo4', 4, 4)");
    conn.createStatement().executeUpdate(
      "upsert into " + tableName + " (tenantId, pk2, pk3, c1) values ('ABC', 'helo5', 5, 5)");
    conn.commit();
    conn.close();

    conn = nextConnection(getUrl());
    // order by needed on the query to make the order of rows returned deterministic.
    PreparedStatement stmt = conn.prepareStatement("select pk2, pk3 from " + tableName
      + " WHERE (tenantId, pk2, pk3) IN ((?, ?, ?), (?, ?, ?)) ORDER BY pk2");
    stmt.setString(1, tenantId);
    stmt.setString(2, "helo3");
    stmt.setInt(3, 3);
    stmt.setString(4, tenantId);
    stmt.setString(5, "helo5");
    stmt.setInt(6, 5);

    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertEquals("helo3", rs.getString(1));
    assertEquals(3, rs.getInt(2));
    assertTrue(rs.next());
    assertEquals("helo5", rs.getString(1));
    assertEquals(5, rs.getInt(2));
    conn.close();
  }

  // Table type - multi-tenant. Salted - Yes. Query against - base table. Connection used for
  // running IN list query - global.
  @Test
  public void testInListOfRVC4() throws Exception {
    String tenantId = "ABC";
    String tableName = generateUniqueName();
    String viewName = generateUniqueName();
    String tenantSpecificUrl = getUrl() + ";" + PhoenixRuntime.TENANT_ID_ATTRIB + '=' + tenantId;
    String baseTableDDL = "CREATE TABLE " + tableName
      + " (tenantId varchar(5) NOT NULL, pk2 varchar(5) NOT NULL, pk3 INTEGER NOT NULL, c1 INTEGER constraint pk primary key (tenantId,pk2,pk3)) SALT_BUCKETS=4, MULTI_TENANT=true";
    createTestTable(getUrl(), baseTableDDL, null, null);
    String tenantTableDDL = "CREATE VIEW " + viewName + " (tenant_col VARCHAR) AS SELECT *\n"
      + "                FROM " + tableName;
    createTestTable(tenantSpecificUrl, tenantTableDDL, null, null);

    Connection conn = nextConnection(tenantSpecificUrl);
    conn.createStatement()
      .executeUpdate("upsert into " + viewName + " (pk2, pk3, c1) values ('helo1', 1, 1)");
    conn.createStatement()
      .executeUpdate("upsert into " + viewName + " (pk2, pk3, c1) values ('helo2', 2, 2)");
    conn.createStatement()
      .executeUpdate("upsert into " + viewName + " (pk2, pk3, c1) values ('helo3', 3, 3)");
    conn.createStatement()
      .executeUpdate("upsert into " + viewName + " (pk2, pk3, c1) values ('helo4', 4, 4)");
    conn.createStatement()
      .executeUpdate("upsert into " + viewName + " (pk2, pk3, c1) values ('helo5', 5, 5)");
    conn.commit();
    conn.close();

    conn = nextConnection(getUrl());
    // order by needed on the query to make the order of rows returned deterministic.
    PreparedStatement stmt = conn.prepareStatement("select pk2, pk3 from " + tableName
      + " WHERE (tenantId, pk2, pk3) IN ((?, ?, ?), (?, ?, ?)) ORDER BY pk2");
    stmt.setString(1, tenantId);
    stmt.setString(2, "helo3");
    stmt.setInt(3, 3);
    stmt.setString(4, tenantId);
    stmt.setString(5, "helo5");
    stmt.setInt(6, 5);

    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertEquals("helo3", rs.getString(1));
    assertEquals(3, rs.getInt(2));
    assertTrue(rs.next());
    assertEquals("helo5", rs.getString(1));
    assertEquals(5, rs.getInt(2));
    conn.close();
  }

  // Table type - non multi-tenant. Salted - Yes. Query against - regular table. Connection used for
  // running IN list query - global.
  @Test
  public void testInListOfRVC5() throws Exception {
    String tenantId = "ABC";
    String tableName = generateUniqueName();
    String tableDDL = "CREATE TABLE " + tableName
      + " (tenantId varchar(5) NOT NULL, pk2 varchar(5) NOT NULL, pk3 INTEGER NOT NULL, c1 INTEGER constraint pk primary key (tenantId,pk2,pk3)) SALT_BUCKETS=4";
    createTestTable(getUrl(), tableDDL, null, null);

    Connection conn = nextConnection(getUrl());
    conn.createStatement().executeUpdate(
      "upsert into " + tableName + " (tenantId, pk2, pk3, c1) values ('ABC', 'helo1', 1, 1)");
    conn.createStatement().executeUpdate(
      "upsert into " + tableName + " (tenantId, pk2, pk3, c1) values ('ABC', 'helo2', 2, 2)");
    conn.createStatement().executeUpdate(
      "upsert into " + tableName + " (tenantId, pk2, pk3, c1) values ('ABC', 'helo3', 3, 3)");
    conn.createStatement().executeUpdate(
      "upsert into " + tableName + " (tenantId, pk2, pk3, c1) values ('ABC', 'helo4', 4, 4)");
    conn.createStatement().executeUpdate(
      "upsert into " + tableName + " (tenantId, pk2, pk3, c1) values ('ABC', 'helo5', 5, 5)");
    conn.commit();
    conn.close();

    conn = nextConnection(getUrl());
    // order by needed on the query to make the order of rows returned deterministic.
    PreparedStatement stmt = conn.prepareStatement("select pk2, pk3 from " + tableName
      + " WHERE (tenantId, pk2, pk3) IN ((?, ?, ?), (?, ?, ?)) ORDER BY pk2");
    stmt.setString(1, tenantId);
    stmt.setString(2, "helo3");
    stmt.setInt(3, 3);
    stmt.setString(4, tenantId);
    stmt.setString(5, "helo5");
    stmt.setInt(6, 5);

    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertEquals("helo3", rs.getString(1));
    assertEquals(3, rs.getInt(2));
    assertTrue(rs.next());
    assertEquals("helo5", rs.getString(1));
    assertEquals(5, rs.getInt(2));
    conn.close();
  }

  @Test
  public void testInListOfRVCColumnValuesSmallerLengthThanSchema() throws Exception {
    String tenantId = "ABC";
    String tableName = generateUniqueName();
    String tableDDL = "CREATE TABLE " + tableName
      + " (tenantId char(15) NOT NULL, pk2 char(15) NOT NULL, pk3 INTEGER NOT NULL, c1 INTEGER constraint pk primary key (tenantId,pk2,pk3))";
    createTestTable(getUrl(), tableDDL, null, null);

    Connection conn = nextConnection(getUrl());
    conn.createStatement().executeUpdate(
      "upsert into " + tableName + " (tenantId, pk2, pk3, c1) values ('ABC', 'hel1', 1, 1)");
    conn.createStatement().executeUpdate(
      "upsert into " + tableName + " (tenantId, pk2, pk3, c1) values ('ABC', 'hel2', 2, 2)");
    conn.createStatement().executeUpdate(
      "upsert into " + tableName + " (tenantId, pk2, pk3, c1) values ('ABC', 'hel3', 3, 3)");
    conn.createStatement().executeUpdate(
      "upsert into " + tableName + " (tenantId, pk2, pk3, c1) values ('ABC', 'hel4', 4, 4)");
    conn.createStatement().executeUpdate(
      "upsert into " + tableName + " (tenantId, pk2, pk3, c1) values ('ABC', 'hel5', 5, 5)");
    conn.commit();
    conn.close();

    conn = nextConnection(getUrl());
    // order by needed on the query to make the order of rows returned deterministic.
    PreparedStatement stmt = conn.prepareStatement("select pk2, pk3 from " + tableName
      + " WHERE (tenantId, pk2, pk3) IN ((?, ?, ?), (?, ?, ?)) ORDER BY PK2");
    stmt.setString(1, tenantId);
    stmt.setString(2, "hel3");
    stmt.setInt(3, 3);
    stmt.setString(4, tenantId);
    stmt.setString(5, "hel5");
    stmt.setInt(6, 5);

    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertEquals("hel3", rs.getString(1));
    assertEquals(3, rs.getInt(2));
    assertTrue(rs.next());
    assertEquals("hel5", rs.getString(1));
    assertEquals(5, rs.getInt(2));
    conn.close();
  }

  @Test
  public void testRVCWithColumnValuesOfSmallerLengthThanSchema() throws Exception {
    testRVCWithComparisonOps(true);
  }

  @Test
  public void testRVCWithColumnValuesEqualToLengthInSchema() throws Exception {
    testRVCWithComparisonOps(false);
  }

  private void testRVCWithComparisonOps(boolean columnValueLengthSmaller) throws Exception {
    String tenantId = "ABC";
    String tableName = generateUniqueName();
    String tableDDLFormat = "CREATE TABLE " + tableName
      + " (tenantId char(%s) NOT NULL, pk2 char(%s) NOT NULL, pk3 INTEGER NOT NULL, c1 INTEGER constraint pk primary key (tenantId,pk2,pk3))";
    String tableDDL;
    if (columnValueLengthSmaller) {
      tableDDL = String.format(tableDDLFormat, 15, 15);
    } else {
      tableDDL = String.format(tableDDLFormat, 3, 5);
    }
    createTestTable(getUrl(), tableDDL, null, null);

    Connection conn = nextConnection(getUrl());
    conn.createStatement().executeUpdate(
      "upsert into " + tableName + " (tenantId, pk2, pk3, c1) values ('ABC', 'helo1', 1, 1)");
    conn.createStatement().executeUpdate(
      "upsert into " + tableName + " (tenantId, pk2, pk3, c1) values ('ABC', 'helo2', 2, 2)");
    conn.createStatement().executeUpdate(
      "upsert into " + tableName + " (tenantId, pk2, pk3, c1) values ('ABC', 'helo3', 3, 3)");
    conn.createStatement().executeUpdate(
      "upsert into " + tableName + " (tenantId, pk2, pk3, c1) values ('ABC', 'helo4', 4, 4)");
    conn.createStatement().executeUpdate(
      "upsert into " + tableName + " (tenantId, pk2, pk3, c1) values ('ABC', 'helo5', 5, 5)");
    conn.commit();
    conn.close();

    conn = nextConnection(getUrl());

    // >
    PreparedStatement stmt = conn.prepareStatement(
      "select pk2, pk3 from " + tableName + " WHERE (tenantId, pk2, pk3) > (?, ?, ?)");
    stmt.setString(1, tenantId);
    stmt.setString(2, "helo3");
    stmt.setInt(3, 3);

    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertEquals("helo4", rs.getString(1));
    assertEquals(4, rs.getInt(2));
    assertTrue(rs.next());
    assertEquals("helo5", rs.getString(1));
    assertEquals(5, rs.getInt(2));
    assertFalse(rs.next());

    // >=
    stmt = conn.prepareStatement(
      "select pk2, pk3 from " + tableName + " WHERE (tenantId, pk2, pk3) >= (?, ?, ?)");
    stmt.setString(1, tenantId);
    stmt.setString(2, "helo4");
    stmt.setInt(3, 4);

    rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertEquals("helo4", rs.getString(1));
    assertEquals(4, rs.getInt(2));
    assertTrue(rs.next());
    assertEquals("helo5", rs.getString(1));
    assertEquals(5, rs.getInt(2));
    assertFalse(rs.next());

    // <
    stmt = conn.prepareStatement(
      "select pk2, pk3 from " + tableName + " WHERE (tenantId, pk2, pk3) < (?, ?, ?)");
    stmt.setString(1, tenantId);
    stmt.setString(2, "helo2");
    stmt.setInt(3, 2);

    rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertEquals("helo1", rs.getString(1));
    assertEquals(1, rs.getInt(2));
    assertFalse(rs.next());

    // <=
    stmt = conn.prepareStatement(
      "select pk2, pk3 from " + tableName + " WHERE (tenantId, pk2, pk3) <= (?, ?, ?)");
    stmt.setString(1, tenantId);
    stmt.setString(2, "helo2");
    stmt.setInt(3, 2);
    rs = stmt.executeQuery();

    assertTrue(rs.next());
    assertEquals("helo1", rs.getString(1));
    assertEquals(1, rs.getInt(2));
    assertTrue(rs.next());
    assertEquals("helo2", rs.getString(1));
    assertEquals(2, rs.getInt(2));
    assertFalse(rs.next());

    // =
    stmt = conn.prepareStatement(
      "select pk2, pk3 from " + tableName + " WHERE (tenantId, pk2, pk3) = (?, ?, ?)");
    stmt.setString(1, tenantId);
    stmt.setString(2, "helo4");
    stmt.setInt(3, 4);

    rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertEquals("helo4", rs.getString(1));
    assertEquals(4, rs.getInt(2));
    assertFalse(rs.next());
  }

  @Test
  public void testForceSkipScan() throws Exception {
    String tempTableWithCompositePK = generateUniqueName();
    Connection conn = nextConnection(getUrl());
    try {
      conn.createStatement()
        .execute("CREATE TABLE " + tempTableWithCompositePK + "   (col0 INTEGER NOT NULL, "
          + "    col1 INTEGER NOT NULL, " + "    col2 INTEGER NOT NULL, " + "    col3 INTEGER "
          + "   CONSTRAINT pk PRIMARY KEY (col0, col1, col2)) " + "   SALT_BUCKETS=4");
      conn.close();

      conn = nextConnection(getUrl());
      PreparedStatement upsertStmt = conn.prepareStatement("upsert into " + tempTableWithCompositePK
        + "(col0, col1, col2, col3) " + "values (?, ?, ?, ?)");
      for (int i = 0; i < 3; i++) {
        upsertStmt.setInt(1, i + 1);
        upsertStmt.setInt(2, i + 2);
        upsertStmt.setInt(3, i + 3);
        upsertStmt.setInt(4, i + 5);
        upsertStmt.execute();
      }
      conn.commit();
      conn.close();

      conn = nextConnection(getUrl());
      String query = "SELECT * FROM " + tempTableWithCompositePK
        + " WHERE (col0, col1) in ((2, 3), (3, 4), (4, 5))";
      PreparedStatement statement = conn.prepareStatement(query);
      ResultSet rs = statement.executeQuery();
      assertTrue(rs.next());
      assertEquals(rs.getInt(1), 2);
      assertEquals(rs.getInt(2), 3);
      assertEquals(rs.getInt(3), 4);
      assertEquals(rs.getInt(4), 6);
      assertTrue(rs.next());
      assertEquals(rs.getInt(1), 3);
      assertEquals(rs.getInt(2), 4);
      assertEquals(rs.getInt(3), 5);
      assertEquals(rs.getInt(4), 7);

      assertFalse(rs.next());

      ExplainPlan plan = conn.prepareStatement(query).unwrap(PhoenixPreparedStatement.class)
        .optimizeQuery().getExplainPlan();
      ExplainPlanAttributes explainPlanAttributes = plan.getPlanStepsAsAttributes();
      assertEquals("PARALLEL 4-WAY", explainPlanAttributes.getIteratorTypeAndScanSize());
      assertEquals("SKIP SCAN ON 12 KEYS ", explainPlanAttributes.getExplainScanType());
      assertEquals(tempTableWithCompositePK, explainPlanAttributes.getTableName());
      assertEquals(" [X'00',2] - [X'03',4]", explainPlanAttributes.getKeyRanges());
      assertEquals("CLIENT MERGE SORT", explainPlanAttributes.getClientSortAlgo());
    } finally {
      conn.close();
    }
  }

  // query against non-multitenant table. Salted - yes
  @Test
  public void testComparisonAgainstRVCCombinedWithOrAnd_1() throws Exception {
    String tableDDL =
      "CREATE TABLE RVC1 (tenantId char(15) NOT NULL, pk2 char(15) NOT NULL, pk3 INTEGER NOT NULL, c1 INTEGER constraint pk primary key (tenantId,pk2,pk3)) SALT_BUCKETS = 4";
    createTestTable(getUrl(), tableDDL, null, null);

    Connection conn = nextConnection(getUrl());
    conn.createStatement()
      .executeUpdate("upsert into RVC1 (tenantId, pk2, pk3, c1) values ('ABC', 'helo1', 1, 1)");
    conn.createStatement()
      .executeUpdate("upsert into RVC1 (tenantId, pk2, pk3, c1) values ('ABC', 'helo2', 2, 2)");
    conn.createStatement()
      .executeUpdate("upsert into RVC1 (tenantId, pk2, pk3, c1) values ('DEF', 'helo3', 3, 3)");
    conn.commit();
    conn.close();

    conn = nextConnection(getUrl());
    PreparedStatement stmt = conn.prepareStatement(
      "select pk2, pk3 from RVC1 WHERE (tenantId = ? OR tenantId = ?) AND (tenantId, pk2, pk3) > (?, ?, ?) LIMIT 100");
    stmt.setString(1, "ABC");
    stmt.setString(2, "DEF");

    // give back all rows after row 1 - ABC|helo1|1
    stmt.setString(3, "ABC");
    stmt.setString(4, "helo1");
    stmt.setInt(5, 1);

    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertEquals("helo2", rs.getString(1));
    assertEquals(2, rs.getInt(2));
    assertTrue(rs.next());
    assertEquals("helo3", rs.getString(1));
    assertEquals(3, rs.getInt(2));
    assertFalse(rs.next());

    stmt = conn.prepareStatement(
      "select pk2, pk3 from RVC1 WHERE tenantId = ? AND (tenantId, pk2, pk3) BETWEEN (?, ?, ?) AND (?, ?, ?) LIMIT 100");
    stmt.setString(1, "ABC");
    stmt.setString(2, "ABC");
    stmt.setString(3, "helo2");
    stmt.setInt(4, 2);
    stmt.setString(5, "DEF");
    stmt.setString(6, "helo3");
    stmt.setInt(7, 3);

    rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertEquals("helo2", rs.getString(1));
    assertEquals(2, rs.getInt(2));
    assertFalse(rs.next());
  }

  // query against tenant specific view. Salted base table.
  @Test
  public void testComparisonAgainstRVCCombinedWithOrAnd_2() throws Exception {
    String tenantId = "ABC";
    String tenantSpecificUrl = getUrl() + ";" + PhoenixRuntime.TENANT_ID_ATTRIB + '=' + tenantId;
    String baseTableDDL =
      "CREATE TABLE RVC2 (tenant_id char(15) NOT NULL, pk2 char(15) NOT NULL, pk3 INTEGER NOT NULL, c1 INTEGER constraint pk primary key (tenant_id,pk2,pk3)) MULTI_TENANT=true, SALT_BUCKETS = 4";
    createTestTable(getUrl(), baseTableDDL, null, null);
    String tenantTableDDL = "CREATE VIEW t_view AS SELECT * FROM RVC2";
    createTestTable(tenantSpecificUrl, tenantTableDDL, null, null);

    Connection conn = nextConnection(tenantSpecificUrl);
    conn.createStatement()
      .executeUpdate("upsert into t_view (pk2, pk3, c1) values ('helo1', 1, 1)");
    conn.createStatement()
      .executeUpdate("upsert into t_view (pk2, pk3, c1) values ('helo2', 2, 2)");
    conn.createStatement()
      .executeUpdate("upsert into t_view (pk2, pk3, c1) values ('helo3', 3, 3)");
    conn.commit();
    conn.close();

    conn = nextConnection(tenantSpecificUrl);
    PreparedStatement stmt = conn.prepareStatement(
      "select pk2, pk3 from t_view WHERE (pk2 = ? OR pk2 = ?) AND (pk2, pk3) > (?, ?) LIMIT 100");
    stmt.setString(1, "helo1");
    stmt.setString(2, "helo3");

    // return rows after helo1|1
    stmt.setString(3, "helo1");
    stmt.setInt(4, 1);

    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertEquals("helo3", rs.getString(1));
    assertEquals(3, rs.getInt(2));
    assertFalse(rs.next());
    conn.close();
  }

  @Test
  public void testRVCWithRowKeyNotLeading() throws Exception {
    String ddl =
      "CREATE TABLE sorttest4 (rownum BIGINT primary key, name varchar(16), age integer)";
    Connection conn = nextConnection(getUrl());
    conn.createStatement().execute(ddl);
    conn.close();
    conn = nextConnection(getUrl());
    String dml = "UPSERT INTO sorttest4 (rownum, name, age) values (?, ?, ?)";
    PreparedStatement stmt = conn.prepareStatement(dml);
    stmt.setInt(1, 1);
    stmt.setString(2, "A");
    stmt.setInt(3, 1);
    stmt.executeUpdate();
    stmt.setInt(1, 2);
    stmt.setString(2, "B");
    stmt.setInt(3, 2);
    stmt.executeUpdate();
    conn.commit();
    conn.close();
    // the below query should only return one record -> (1, "A", 1)
    String query = "SELECT rownum, name, age FROM sorttest4 where (age, rownum) < (2, 2)";
    conn = nextConnection(getUrl());
    ResultSet rs = conn.createStatement().executeQuery(query);
    int numRecords = 0;
    while (rs.next()) {
      assertEquals(1, rs.getInt(1));
      assertEquals("A", rs.getString(2));
      assertEquals(1, rs.getInt(3));
      numRecords++;
    }
    assertEquals(1, numRecords);
  }

  @Test
  public void testRVCInView() throws Exception {
    Connection conn = nextConnection(getUrl());
    conn.createStatement().execute("CREATE TABLE TEST_TABLE.TEST1 (\n" + "PK1 CHAR(3) NOT NULL, \n"
      + "PK2 CHAR(3) NOT NULL,\n" + "DATA1 CHAR(10)\n" + "CONSTRAINT PK PRIMARY KEY (PK1, PK2))");
    conn.close();
    conn = nextConnection(getUrl());
    conn.createStatement()
      .execute("CREATE VIEW TEST_TABLE.FOO AS SELECT * FROM TEST_TABLE.TEST1 WHERE PK1 = 'FOO'");
    conn.close();
    conn = nextConnection(getUrl());
    conn.createStatement().execute("UPSERT INTO TEST_TABLE.TEST1 VALUES('FOO','001','SOMEDATA')");
    conn.createStatement().execute("UPSERT INTO TEST_TABLE.TEST1 VALUES('FOO','002','SOMEDATA')");
    conn.createStatement().execute("UPSERT INTO TEST_TABLE.TEST1 VALUES('FOO','003','SOMEDATA')");
    conn.createStatement().execute("UPSERT INTO TEST_TABLE.TEST1 VALUES('FOO','004','SOMEDATA')");
    conn.createStatement().execute("UPSERT INTO TEST_TABLE.TEST1 VALUES('FOO','005','SOMEDATA')");
    conn.commit();
    conn.close();

    conn = nextConnection(getUrl());
    ResultSet rs = conn.createStatement().executeQuery(
      "SELECT * FROM TEST_TABLE.FOO WHERE PK2 < '004' AND (PK1,PK2) > ('FOO','002') LIMIT 2");
    assertTrue(rs.next());
    assertEquals("003", rs.getString("PK2"));
    assertFalse(rs.next());
    conn.close();
  }

  @Test
  public void testCountDistinct1() throws Exception {
    Connection conn = nextConnection(getUrl());
    String tableName = generateUniqueName();
    String ddl =
      "CREATE TABLE " + tableName + " (region_name VARCHAR PRIMARY KEY, a INTEGER, b INTEGER)";
    conn.createStatement().execute(ddl);

    conn = nextConnection(getUrl());
    PreparedStatement stmt =
      conn.prepareStatement("UPSERT INTO  " + tableName + " (region_name, a, b) VALUES('a', 6,3)");
    stmt.execute();
    stmt =
      conn.prepareStatement("UPSERT INTO  " + tableName + " (region_name, a, b) VALUES('b', 2,4)");
    stmt.execute();
    stmt =
      conn.prepareStatement("UPSERT INTO  " + tableName + " (region_name, a, b) VALUES('c', 6,3)");
    stmt.execute();
    conn.commit();

    conn = nextConnection(getUrl());
    ResultSet rs;
    rs = conn.createStatement().executeQuery("SELECT COUNT(DISTINCT (a,b)) from " + tableName);
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    conn.close();
  }

  @Test
  public void testCountDistinct2() throws Exception {
    Connection conn = nextConnection(getUrl());
    String tableName = generateUniqueName();
    String ddl =
      "CREATE TABLE  " + tableName + "  (region_name VARCHAR PRIMARY KEY, a VARCHAR, b VARCHAR)";
    conn.createStatement().execute(ddl);
    conn.commit();

    conn = nextConnection(getUrl());
    PreparedStatement stmt = conn.prepareStatement(
      "UPSERT INTO  " + tableName + " (region_name, a, b) VALUES('a', 'fooo','abc')");
    stmt.execute();
    stmt = conn.prepareStatement(
      "UPSERT INTO  " + tableName + " (region_name, a, b) VALUES('b', 'off','bac')");
    stmt.execute();
    stmt = conn.prepareStatement(
      "UPSERT INTO  " + tableName + " (region_name, a, b) VALUES('c', 'fooo', 'abc')");
    stmt.execute();
    conn.commit();

    conn = nextConnection(getUrl());
    ResultSet rs;
    rs = conn.createStatement().executeQuery("SELECT COUNT(DISTINCT (a,b)) from  " + tableName);
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
  }

  @Test
  public void testCountDistinct3() throws Exception {
    Connection conn = nextConnection(getUrl());
    String tableName = "testCountDistinct3rvc";
    String ddl =
      "CREATE TABLE  " + tableName + "  (region_name VARCHAR PRIMARY KEY, a Boolean, b Boolean)";
    conn.createStatement().execute(ddl);

    conn = nextConnection(getUrl());
    PreparedStatement stmt = conn.prepareStatement(
      "UPSERT INTO  " + tableName + " (region_name, a, b) VALUES('a', true, true)");
    stmt.execute();
    stmt = conn.prepareStatement(
      "UPSERT INTO  " + tableName + " (region_name, a, b) VALUES('b', true, False)");
    stmt.execute();
    stmt = conn.prepareStatement(
      "UPSERT INTO  " + tableName + " (region_name, a, b) VALUES('c', true, true)");
    stmt.execute();
    stmt = conn.prepareStatement(
      "UPSERT INTO  " + tableName + " (region_name, a, b) VALUES('d', true, false)");
    stmt.execute();
    conn.commit();

    conn = nextConnection(getUrl());
    ResultSet rs;
    rs = conn.createStatement().executeQuery("SELECT COUNT(DISTINCT (a,b)) from  " + tableName);
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
  }

  @Test
  public void testCountDistinct4() throws Exception {
    Connection conn = nextConnection(getUrl());
    String tableName = generateUniqueName();
    String ddl =
      "CREATE TABLE  " + tableName + "  (region_name VARCHAR PRIMARY KEY, a VARCHAR, b VARCHAR)";
    conn.createStatement().execute(ddl);

    conn = nextConnection(getUrl());
    PreparedStatement stmt = conn.prepareStatement(
      "UPSERT INTO  " + tableName + " (region_name, a, b) VALUES('a', 'fooo','abc')");
    stmt.execute();
    stmt = conn.prepareStatement(
      "UPSERT INTO  " + tableName + " (region_name, a, b) VALUES('b', 'off','bac')");
    stmt.execute();
    stmt = conn.prepareStatement(
      "UPSERT INTO  " + tableName + " (region_name, a, b) VALUES('c', 'foo', 'abc')");
    stmt.execute();
    conn.commit();

    conn = nextConnection(getUrl());
    ResultSet rs;
    rs = conn.createStatement().executeQuery("SELECT COUNT(DISTINCT (a,b)) from  " + tableName);
    assertTrue(rs.next());
    assertEquals(3, rs.getInt(1));
  }

  @Test
  public void testRVCRequiringExtractNodeClear() throws Exception {
    Connection conn = nextConnection(getUrl());
    String tableName = generateUniqueName();
    String ddl = "CREATE TABLE  " + tableName
      + "  (k1 VARCHAR, k2 VARCHAR, k3 VARCHAR, k4 VARCHAR, CONSTRAINT pk PRIMARY KEY (k1,k2,k3,k4))";
    conn.createStatement().execute(ddl);

    conn = nextConnection(getUrl());
    PreparedStatement stmt =
      conn.prepareStatement("UPSERT INTO  " + tableName + " VALUES('a','b','c','d')");
    stmt.execute();
    stmt = conn.prepareStatement("UPSERT INTO  " + tableName + " VALUES('b', 'b', 'c', 'e')");
    stmt.execute();
    stmt = conn.prepareStatement("UPSERT INTO  " + tableName + " VALUES('c', 'b','c','f')");
    stmt.execute();
    conn.commit();

    TestUtil.dumpTable(conn, TableName.valueOf(tableName));

    conn = nextConnection(getUrl());
    ResultSet rs;
    rs = conn.createStatement().executeQuery("SELECT k1 from  " + tableName
      + " WHERE k1 IN ('a','c') AND (k2,k3) IN (('b','c'),('f','g')) AND k4 > 'c'");
    assertTrue(rs.next());
    assertEquals("a", rs.getString(1));
    assertTrue(rs.next());
    assertEquals("c", rs.getString(1));
    assertFalse(rs.next());
  }

  @Test
  public void testRVCRequiringExtractNodeVarBinary() throws Exception {
    Connection conn = nextConnection(getUrl());
    String tableName = generateUniqueName();
    String ddl = "CREATE TABLE  " + tableName
      + "  (k1 VARBINARY_ENCODED, k2 VARBINARY_ENCODED, k3 VARBINARY_ENCODED,"
      + " k4 VARBINARY_ENCODED, CONSTRAINT pk PRIMARY KEY (k1,k2,k3,k4))";
    conn.createStatement().execute(ddl);

    conn = nextConnection(getUrl());
    PreparedStatement stmt =
      conn.prepareStatement("UPSERT INTO  " + tableName + " VALUES(?,?,?,?)");
    stmt.setBytes(1, Bytes.toBytes('a'));
    stmt.setBytes(2, Bytes.toBytes('b'));
    stmt.setBytes(3, Bytes.toBytes('c'));
    stmt.setBytes(4, Bytes.toBytes('d'));
    stmt.execute();

    stmt = conn.prepareStatement("UPSERT INTO  " + tableName + " VALUES(?,?,?,?)");
    stmt.setBytes(1, Bytes.toBytes('b'));
    stmt.setBytes(2, Bytes.toBytes('b'));
    stmt.setBytes(3, Bytes.toBytes('c'));
    stmt.setBytes(4, Bytes.toBytes('e'));
    stmt.execute();

    stmt = conn.prepareStatement("UPSERT INTO  " + tableName + " VALUES(?,?,?,?)");
    stmt.setBytes(1, Bytes.toBytes('c'));
    stmt.setBytes(2, Bytes.toBytes('b'));
    stmt.setBytes(3, Bytes.toBytes('c'));
    stmt.setBytes(4, Bytes.toBytes('f'));
    stmt.execute();

    conn.commit();

    conn = nextConnection(getUrl());

    ResultSet rs;
    stmt = conn.prepareStatement("SELECT k1 from  " + tableName
      + " WHERE k1 IN (?,?) AND (k2,k3) IN ((?,?),(?,?)) AND k4 > ?");
    stmt.setBytes(1, Bytes.toBytes('a'));
    stmt.setBytes(2, Bytes.toBytes('c'));
    stmt.setBytes(3, Bytes.toBytes('b'));
    stmt.setBytes(4, Bytes.toBytes('c'));
    stmt.setBytes(5, Bytes.toBytes('f'));
    stmt.setBytes(6, Bytes.toBytes('g'));
    stmt.setBytes(7, Bytes.toBytes('c'));

    rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertArrayEquals(Bytes.toBytes('a'), rs.getBytes(1));
    assertTrue(rs.next());
    assertArrayEquals(Bytes.toBytes('c'), rs.getBytes(1));
    assertFalse(rs.next());

    stmt = conn.prepareStatement(
      "SELECT k1, k2, k3 from  " + tableName + " WHERE ((k1, k2), k3) >= ((?, ?), ?)");
    stmt.setBytes(1, Bytes.toBytes('b'));
    stmt.setBytes(2, Bytes.toBytes('b'));
    stmt.setBytes(3, Bytes.toBytes('b'));

    rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertArrayEquals(Bytes.toBytes('b'), rs.getBytes(1));
    assertTrue(rs.next());
    assertArrayEquals(Bytes.toBytes('c'), rs.getBytes(1));
    assertFalse(rs.next());

    stmt = conn.prepareStatement(
      "SELECT k1, k2, k3, k4 from  " + tableName + " WHERE (k1, k2, k3) >= (?, ?, ?)");
    stmt.setBytes(1, Bytes.toBytes('b'));
    stmt.setBytes(2, Bytes.toBytes('b'));
    stmt.setBytes(3, Bytes.toBytes('b'));

    rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertArrayEquals(Bytes.toBytes('b'), rs.getBytes(1));
    assertTrue(rs.next());
    assertArrayEquals(Bytes.toBytes('c'), rs.getBytes(1));
    assertFalse(rs.next());

    stmt = conn.prepareStatement(
      "SELECT k1, k2, k3 from  " + tableName + " WHERE (k1, (k2, k3)) >= (?, (?, ?))");
    stmt.setBytes(1, Bytes.toBytes('b'));
    stmt.setBytes(2, Bytes.toBytes('b'));
    stmt.setBytes(3, Bytes.toBytes('b'));

    rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertArrayEquals(Bytes.toBytes('b'), rs.getBytes(1));
    assertTrue(rs.next());
    assertArrayEquals(Bytes.toBytes('c'), rs.getBytes(1));
    assertFalse(rs.next());
  }

  @Test
  public void testRVCRequiringExtractNodeVarBinary2() throws Exception {
    Connection conn = nextConnection(getUrl());
    String tableName = generateUniqueName();
    String ddl = "CREATE TABLE  " + tableName
      + "  (k1 VARBINARY_ENCODED, k2 VARBINARY_ENCODED, k3 VARBINARY_ENCODED,"
      + " k4 VARBINARY_ENCODED CONSTRAINT pk PRIMARY KEY (k1,k2,k3))";
    conn.createStatement().execute(ddl);

    byte[] b1 = new byte[] { 34, 58, 38, 0, 40, -13, 34, 0 };
    byte[] b2 = new byte[] { 35, 58, 38, 0, 40, -13, 34, 0 };
    byte[] b3 = new byte[] { 36, 58, 38, 0, 40, -13, 34, 0 };
    byte[] b4 = new byte[] { 37, 58, 38, 0, 40, -13, 34, 0 };
    byte[] b5 = new byte[] { 38, 58, 38, 0, 40, -13, 34, 0 };
    byte[] b6 = new byte[] { 39, 58, 38, 0, 40, -13, 34, 0 };

    conn = nextConnection(getUrl());
    PreparedStatement stmt =
      conn.prepareStatement("UPSERT INTO  " + tableName + " VALUES(?,?,?,?)");
    stmt.setBytes(1, b1);
    stmt.setBytes(2, b2);
    stmt.setBytes(3, b3);
    stmt.setBytes(4, b4);
    stmt.execute();

    stmt = conn.prepareStatement("UPSERT INTO  " + tableName + " VALUES(?,?,?,?)");
    stmt.setBytes(1, b2);
    stmt.setBytes(2, b2);
    stmt.setBytes(3, b3);
    stmt.setBytes(4, b5);
    stmt.execute();

    stmt = conn.prepareStatement("UPSERT INTO  " + tableName + " VALUES(?,?,?,?)");
    stmt.setBytes(1, b3);
    stmt.setBytes(2, b2);
    stmt.setBytes(3, b3);
    stmt.setBytes(4, b6);
    stmt.execute();

    conn.commit();

    conn = nextConnection(getUrl());

    stmt = conn.prepareStatement(
      "SELECT k1, k2, k4 from  " + tableName + " WHERE ((k1, k2), k4) >= ((?, ?), ?)");
    stmt.setBytes(1, b2);
    stmt.setBytes(2, b2);
    stmt.setBytes(3, b3);

    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertArrayEquals(b2, rs.getBytes(1));
    assertArrayEquals(b2, rs.getBytes(2));
    assertArrayEquals(b5, rs.getBytes(3));
    assertTrue(rs.next());
    assertArrayEquals(b3, rs.getBytes(1));
    assertArrayEquals(b2, rs.getBytes(2));
    assertArrayEquals(b6, rs.getBytes(3));
    assertFalse(rs.next());

    stmt = conn.prepareStatement(
      "SELECT k1, k2, k4 from  " + tableName + " WHERE (k1, k2, k4) >= (?, ?, ?)");
    stmt.setBytes(1, b2);
    stmt.setBytes(2, b2);
    stmt.setBytes(3, b3);

    rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertArrayEquals(b2, rs.getBytes(1));
    assertArrayEquals(b2, rs.getBytes(2));
    assertArrayEquals(b5, rs.getBytes(3));
    assertTrue(rs.next());
    assertArrayEquals(b3, rs.getBytes(1));
    assertArrayEquals(b2, rs.getBytes(2));
    assertArrayEquals(b6, rs.getBytes(3));
    assertFalse(rs.next());

    stmt = conn.prepareStatement(
      "SELECT k1, k2, k4 from  " + tableName + " WHERE (k1, (k2, k4)) >= (?, (?, ?))");
    stmt.setBytes(1, b2);
    stmt.setBytes(2, b2);
    stmt.setBytes(3, b4);

    rs = stmt.executeQuery();
    assertTrue(rs.next());
    assertArrayEquals(b2, rs.getBytes(1));
    assertArrayEquals(b2, rs.getBytes(2));
    assertArrayEquals(b5, rs.getBytes(3));
    assertTrue(rs.next());
    assertArrayEquals(b3, rs.getBytes(1));
    assertArrayEquals(b2, rs.getBytes(2));
    assertArrayEquals(b6, rs.getBytes(3));
    assertFalse(rs.next());
  }

  @Test
  public void testRVCRequiringNoSkipScan() throws Exception {
    Connection conn = nextConnection(getUrl());
    String tableName = generateUniqueName();
    String ddl = "CREATE TABLE  " + tableName
      + "  (k1 VARCHAR, k2 VARCHAR, k3 VARCHAR, k4 VARCHAR, CONSTRAINT pk PRIMARY KEY (k1,k2,k3,k4))";
    conn.createStatement().execute(ddl);

    conn = nextConnection(getUrl());
    PreparedStatement stmt =
      conn.prepareStatement("UPSERT INTO  " + tableName + " VALUES('','','a')");
    stmt.execute();
    stmt = conn.prepareStatement("UPSERT INTO  " + tableName + " VALUES('', '', 'a', 'a')");
    stmt.execute();
    stmt = conn.prepareStatement("UPSERT INTO  " + tableName + " VALUES('', '','b')");
    stmt.execute();
    stmt = conn.prepareStatement("UPSERT INTO  " + tableName + " VALUES('', '','b','a')");
    stmt.execute();
    stmt = conn.prepareStatement("UPSERT INTO  " + tableName + " VALUES('a', '','c')");
    stmt.execute();
    stmt = conn.prepareStatement("UPSERT INTO  " + tableName + " VALUES('a', '','c', 'a')");
    stmt.execute();
    conn.commit();

    conn = nextConnection(getUrl());
    ResultSet rs;
    rs = conn.createStatement().executeQuery("SELECT k1,k3,k4 from  " + tableName
      + " WHERE (k1,k2,k3) IN (('','','a'),('','','b'),('a','','c')) AND k4 is not null");
    assertTrue(rs.next());
    assertEquals(null, rs.getString(1));
    assertEquals("a", rs.getString(2));
    assertEquals("a", rs.getString(3));

    assertTrue(rs.next());
    assertEquals(null, rs.getString(1));
    assertEquals("b", rs.getString(2));
    assertEquals("a", rs.getString(3));

    assertTrue(rs.next());
    assertEquals("a", rs.getString(1));
    assertEquals("c", rs.getString(2));
    assertEquals("a", rs.getString(3));

    assertFalse(rs.next());
  }

  /**
   * PHOENIX-2327 Table's pks are (pk1, pk2, ... , pkn), n >= 3 Index's pks are (pk2, ... , pkn,
   * pk1), n >= 3 RVC is (pk2, ... , pkn, pk1), n >= 3 Expalin select * from " + tableName + " where
   * (pk2, ... , pkn, pk1) > ('201', ..., 'n01', '101') and pk[2-n] = '[2-n]03' You will Get
   * "DEGENERATE SCAN OVER TABLE_NAME"
   * @throws java.lang.Exception
   */
  @Test
  public void testRVCLastPkIsTable1stPkIndex() throws Exception {
    Connection conn = nextConnection(getUrl());
    String tableName = generateUniqueName();
    String ddl = "CREATE TABLE " + tableName + " (k1 VARCHAR, k2 VARCHAR, k3 VARCHAR, k4 VARCHAR,"
      + " CONSTRAINT pk PRIMARY KEY (k1,k2,k3,k4))";
    conn.createStatement().execute(ddl);

    conn = nextConnection(getUrl());
    ddl = "CREATE INDEX  " + tableName + "_idx" + " ON " + tableName + " (k2, k3, k4, k1)";
    conn.createStatement().execute(ddl);

    conn = nextConnection(getUrl());
    String upsert = "UPSERT INTO " + tableName + " VALUES(?, ?, ?, ?)";
    PreparedStatement stmt = conn.prepareStatement(upsert);
    for (int i = 0; i < 5; i++) {
      stmt.setString(1, "10" + i);
      stmt.setString(2, "20" + i);
      stmt.setString(3, "30" + i);
      stmt.setString(4, "40" + i);
      stmt.execute();
    }
    conn.commit();

    conn = nextConnection(getUrl());
    String query = "SELECT k1, k2, k3, k4 FROM " + tableName + " WHERE k2 = '203'";
    ResultSet rs = conn.createStatement().executeQuery(query);
    assertTrue(rs.next());
    assertEquals("103", rs.getString(1));
    assertEquals("203", rs.getString(2));
    assertEquals("303", rs.getString(3));
    assertEquals("403", rs.getString(4));

    conn = nextConnection(getUrl());
    query = "SELECT k1, k2, k3, k4 FROM " + tableName
      + " WHERE (k2, k3, k4, k1) > ('201', '301', '401', '101')" + " AND k2 = '203'";
    rs = conn.createStatement().executeQuery(query);
    assertTrue(rs.next());
    assertEquals("103", rs.getString(1));
    assertEquals("203", rs.getString(2));
    assertEquals("303", rs.getString(3));
    assertEquals("403", rs.getString(4));
  }

  @Test
  public void testMultiTenantRVC() throws Exception {
    Connection conn = nextConnection(getUrl());
    String tableName = "mtRVC";
    String ddl = "CREATE TABLE " + tableName + " (\n" + "    pk1 VARCHAR NOT NULL,\n"
      + "    pk2 DECIMAL NOT NULL,\n" + "    v1 VARCHAR\n" + "    CONSTRAINT PK PRIMARY KEY \n"
      + "    (\n" + "        pk1,\n" + "        pk2\n" + "    )\n"
      + ") MULTI_TENANT=true,IMMUTABLE_ROWS=true";
    conn.createStatement().execute(ddl);

    conn = nextConnection(getUrl());
    ddl = "CREATE INDEX  " + tableName + "_idx" + " ON " + tableName + " (v1)";
    conn.createStatement().execute(ddl);

    conn = nextConnection(getUrl());
    String upsert = "UPSERT INTO " + tableName + " VALUES(?, ?, ?)";
    PreparedStatement stmt = conn.prepareStatement(upsert);
    stmt.setString(1, "a");
    stmt.setInt(2, 1);
    stmt.setString(3, "value");
    stmt.execute();
    stmt.setString(1, "a");
    stmt.setInt(2, 2);
    stmt.setString(3, "value");
    stmt.execute();
    conn.commit();

    conn = nextConnection(getUrl());
    String query = "SELECT pk1, pk2, v1 FROM " + tableName + " WHERE pk1 = 'a' AND\n"
      + "(pk1, pk2) > ('a', 1)\n" + "ORDER BY PK1, PK2\n" + "LIMIT 2";
    ResultSet rs = conn.createStatement().executeQuery(query);
    assertTrue(rs.next());
    assertEquals("a", rs.getString(1));
    assertTrue(new BigDecimal("2").compareTo(rs.getBigDecimal(2)) == 0);
    assertEquals("value", rs.getString(3));
    assertFalse(rs.next());
  }

  @Test
  /**
   * Verifies that a query with a RVC expression lexographically higher than all values in an OR
   * clause causes query works see PHOENIX-4734
   */
  public void testRVCWithAndClause() throws Exception {
    final int numItemsInClause = 5;
    Properties tenantProps = new Properties();
    tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, "tenant1");
    String fullTableName = SchemaUtil.getTableName("S", "T_" + generateUniqueName());
    String fullViewName = SchemaUtil.getTableName("S", "V_" + generateUniqueName());
    try (Connection tenantConn = DriverManager.getConnection(getUrl(), tenantProps)) {
      tenantConn.setAutoCommit(false);
      createBaseTableAndTenantView(tenantConn, fullTableName, fullViewName);
      loadDataIntoTenantView(tenantConn, fullViewName);
      List<String> objectIdsList =
        selectObjectIdsForInClause(tenantConn, fullViewName, numItemsInClause);
      StringBuilder querySb = generateQueryToTest(numItemsInClause, fullViewName);
      PreparedStatement ps = tenantConn.prepareStatement(querySb.toString());
      int numbBindVarsSet = 0;
      String objectId = null;
      for (int i = 0; i < numItemsInClause; i++) {
        objectId = objectIdsList.get(i);
        ps.setString((i + 1), objectId);
        numbBindVarsSet++;
      }
      assertEquals(numItemsInClause, numbBindVarsSet);
      assertEquals("v1000", objectId);
      ps.setString(numItemsInClause + 1, "z00");
      ps.setString(numItemsInClause + 2, "v1000"); // This value must match or be
                                                   // lexographically higher than the highest
                                                   // value in the IN clause
      // Query should execute and return 0 results
      ResultSet rs = ps.executeQuery();
      assertFalse("Query should return no results as IN clause and RVC clause are disjoint sets",
        rs.next());
    }
  }

  @Test
  public void testTrailingSeparator() throws Exception {
    Connection conn = null;
    try {
      conn = DriverManager.getConnection(getUrl());
      conn.createStatement()
        .execute("CREATE TABLE test2961 (\n" + "ACCOUNT_ID VARCHAR NOT NULL,\n"
          + "BUCKET_ID VARCHAR NOT NULL,\n" + "OBJECT_ID VARCHAR NOT NULL,\n"
          + "OBJECT_VERSION VARCHAR NOT NULL,\n" + "LOC VARCHAR,\n"
          + "CONSTRAINT PK PRIMARY KEY (ACCOUNT_ID, BUCKET_ID, OBJECT_ID, OBJECT_VERSION DESC))");

      String sql = "SELECT  OBJ.ACCOUNT_ID from  test2961 as OBJ where "
        + "(OBJ.ACCOUNT_ID, OBJ.BUCKET_ID, OBJ.OBJECT_ID, OBJ.OBJECT_VERSION) IN "
        + "((?,?,?,?),(?,?,?,?))";

      PhoenixPreparedStatement statement =
        conn.prepareStatement(sql).unwrap(PhoenixPreparedStatement.class);
      statement.setString(1, new String(new char[] { (char) 3 }));
      statement.setString(2, new String(new char[] { (char) 55 }));
      statement.setString(3, new String(new char[] { (char) 39 }));
      statement.setString(4, new String(new char[] { (char) 0 }));
      statement.setString(5, new String(new char[] { (char) 83 }));
      statement.setString(6, new String(new char[] { (char) 15 }));
      statement.setString(7, new String(new char[] { (char) 55 }));
      statement.setString(8, new String(new char[] { (char) 147 }));
      statement.optimizeQuery(sql);
    } finally {
      conn.close();
    }
  }

  @Test
  public void testRVCConjunction() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {

      String tableName = generateUniqueName();
      String ddl = String.format(
        "create table %s(a varchar(10) not null, b varchar(10) not null, c varchar(10) not null constraint pk primary key(a, b, c))",
        tableName);
      String upsert = String.format("upsert into %s values(?, ?, ?)", tableName);

      try (Statement statement = conn.createStatement()) {
        statement.execute(ddl);
      }

      try (PreparedStatement statement = conn.prepareStatement(upsert)) {
        statement.setString(1, "abc");
        statement.setString(2, "def");
        statement.setString(3, "RRSQ_IMKKL");
        statement.executeUpdate();
        statement.setString(3, "RRS_ZYTDT");
        statement.executeUpdate();
      }
      conn.commit();

      String conjunctionSelect = String.format(
        "select A, B, C from %s where (A, B, C) > ('abc', 'def', 'RRSQ_IMKKL') AND C like 'RRS\\\\_%%'",
        tableName);
      try (Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery(conjunctionSelect)) {
        assertTrue(rs.next());
        assertEquals("abc", rs.getString(1));
        assertEquals("def", rs.getString(2));
        assertEquals("RRS_ZYTDT", rs.getString(3));
        PhoenixStatement phoenixStatement = statement.unwrap(PhoenixStatement.class);

        byte[] lowerBound = phoenixStatement.getQueryPlan().getScans().get(0).get(0).getStartRow();
        byte[] expected = new byte[] { 'a', 'b', 'c', 0, 'd', 'e', 'f', 0, 'R', 'R', 'S', '_' };
        assertArrayEquals(expected, lowerBound);
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void testRVCWithUpperBoundOnIndex() throws Exception {
    Connection conn = nextConnection(getUrl());
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();

    String ddl = "CREATE TABLE " + tableName + " (" + "pk VARCHAR NOT NULL, "
      + "sk BIGINT NOT NULL, " + "category VARCHAR, " + "score DOUBLE, " + "data VARCHAR, "
      + "CONSTRAINT table_pk PRIMARY KEY (pk, sk))";
    conn.createStatement().execute(ddl);

    conn = nextConnection(getUrl());
    conn.createStatement()
      .execute("CREATE INDEX " + indexName + " ON " + tableName + " (category, score)");

    conn = nextConnection(getUrl());
    PreparedStatement stmt = conn.prepareStatement(
      "UPSERT INTO " + tableName + " (pk, sk, category, score, data) VALUES (?, ?, ?, ?, ?)");
    for (int i = 0; i < 20000; i++) {
      stmt.setString(1, "pk_" + (i % 100));
      stmt.setLong(2, i);
      stmt.setString(3, "category_" + (i % 10));
      stmt.setDouble(4, i);
      stmt.setString(5, "data_" + i);
      stmt.execute();
    }
    conn.commit();

    conn = nextConnection(getUrl());
    // category_0 rows have scores: 0, 10, 20, ..., 19990
    // With score <= 5000 AND (score, pk, sk) > (4990, 'pk_90', 4990):
    // score=4990 row is (pk_90, 4990) which is NOT > the RVC bound (exact match)
    // score=5000 row is (pk_0, 5000) which satisfies both conditions
    // score=5010+ rows must be excluded by score <= 5000
    String query =
      "SELECT /*+ INDEX(" + tableName + " " + indexName + ") */ pk, sk, category, score " + "FROM "
        + tableName + " " + "WHERE category = 'category_0' " + "AND score <= 5000 "
        + "AND (score, pk, sk) > (4990, 'pk_90', 4990) "
        + "ORDER BY category ASC, score ASC, pk ASC, sk ASC " + "LIMIT 100";
    ResultSet rs = conn.createStatement().executeQuery(query);
    assertRow(rs, "pk_0", 5000, "category_0", 5000.0);
    assertFalse(rs.next());

    // Verify that score < 5000 returns no row
    conn = nextConnection(getUrl());
    query = "SELECT /*+ INDEX(" + tableName + " " + indexName + ") */ pk, sk, category, score "
      + "FROM " + tableName + " " + "WHERE category = 'category_0' " + "AND score < 5000 "
      + "AND (score, pk, sk) > (4990, 'pk_90', 4990) "
      + "ORDER BY category ASC, score ASC, pk ASC, sk ASC " + "LIMIT 100";
    rs = conn.createStatement().executeQuery(query);
    assertFalse(rs.next());

    conn = nextConnection(getUrl());
    query = "SELECT /*+ INDEX(" + tableName + " " + indexName + ") */ pk, sk, category, score "
      + "FROM " + tableName + " " + "WHERE category = 'category_0' " + "AND score <= 5000 "
      + "AND (score, pk, sk) > (4990, 'pk_90', 4990) "
      + "ORDER BY category ASC, score DESC, pk DESC, sk DESC " + "LIMIT 100";
    rs = conn.createStatement().executeQuery(query);
    assertRow(rs, "pk_0", 5000, "category_0", 5000.0);
    assertFalse(rs.next());

    conn = nextConnection(getUrl());
    query = "SELECT /*+ INDEX(" + tableName + " " + indexName + ") */ pk, sk, category, score "
      + "FROM " + tableName + " " + "WHERE category = 'category_0' " + "AND score < 5000 "
      + "AND (score, pk, sk) > (4990, 'pk_90', 4990) "
      + "ORDER BY category ASC, score DESC, pk DESC, sk DESC " + "LIMIT 100";
    rs = conn.createStatement().executeQuery(query);
    assertFalse("Expected no rows with score < 5000 and DESC ordering", rs.next());

    // score >= 4980 with RVC upper bound < (5010, 'pk_10', 5010).
    // category_0 rows with score >= 4980:
    // score=4980: (pk_80, 4980)
    // score=4990: (pk_90, 4990)
    // score=5000: (pk_0, 5000)
    // score=5010: (pk_10, 5010) -> RVC equal, NOT strictly less -> excluded
    // Expected 3 rows in ASC order.
    conn = nextConnection(getUrl());
    query = "SELECT /*+ INDEX(" + tableName + " " + indexName + ") */ pk, sk, category, score "
      + "FROM " + tableName + " " + "WHERE category = 'category_0' " + "AND score >= 4980 "
      + "AND (score, pk, sk) < (5010, 'pk_10', 5010) "
      + "ORDER BY category ASC, score ASC, pk ASC, sk ASC " + "LIMIT 100";
    rs = conn.createStatement().executeQuery(query);
    assertRow(rs, "pk_80", 4980, "category_0", 4980.0);
    assertRow(rs, "pk_90", 4990, "category_0", 4990.0);
    assertRow(rs, "pk_0", 5000, "category_0", 5000.0);
    assertFalse(rs.next());

    conn = nextConnection(getUrl());
    query = "SELECT /*+ INDEX(" + tableName + " " + indexName + ") */ pk, sk, category, score "
      + "FROM " + tableName + " " + "WHERE category = 'category_0' " + "AND score >= 4980 "
      + "AND (score, pk, sk) < (5010, 'pk_10', 5010) "
      + "ORDER BY category ASC, score DESC, pk DESC, sk DESC " + "LIMIT 100";
    rs = conn.createStatement().executeQuery(query);
    assertRow(rs, "pk_0", 5000, "category_0", 5000.0);
    assertRow(rs, "pk_90", 4990, "category_0", 4990.0);
    assertRow(rs, "pk_80", 4980, "category_0", 4980.0);
    assertFalse(rs.next());

    conn = nextConnection(getUrl());
    query = "SELECT /*+ INDEX(" + tableName + " " + indexName + ") */ pk, sk, category, score "
      + "FROM " + tableName + " " + "WHERE category = 'category_0' "
      + "AND (score, pk) <= (5000, 'pk_0') " + "AND (score, pk, sk) > (4970, 'pk_70', 4970) "
      + "ORDER BY category ASC, score ASC, pk ASC, sk ASC " + "LIMIT 100";
    rs = conn.createStatement().executeQuery(query);
    assertRow(rs, "pk_80", 4980, "category_0", 4980.0);
    assertRow(rs, "pk_90", 4990, "category_0", 4990.0);
    assertRow(rs, "pk_0", 5000, "category_0", 5000.0);
    assertFalse(rs.next());

    conn = nextConnection(getUrl());
    query = "SELECT /*+ INDEX(" + tableName + " " + indexName + ") */ pk, sk, category, score "
      + "FROM " + tableName + " " + "WHERE category = 'category_0' "
      + "AND (score, pk) <= (5000, 'pk_0') " + "AND (score, pk, sk) > (4970, 'pk_70', 4970) "
      + "ORDER BY category ASC, score DESC, pk DESC, sk DESC " + "LIMIT 100";
    rs = conn.createStatement().executeQuery(query);
    assertRow(rs, "pk_0", 5000, "category_0", 5000.0);
    assertRow(rs, "pk_90", 4990, "category_0", 4990.0);
    assertRow(rs, "pk_80", 4980, "category_0", 4980.0);
    assertFalse(rs.next());

    conn = nextConnection(getUrl());
    query = "SELECT /*+ INDEX(" + tableName + " " + indexName + ") */ pk, sk, category, score "
      + "FROM " + tableName + " " + "WHERE category = 'category_0' "
      + "AND (score, pk) < (5000, 'pk_0') " + "AND (score, pk, sk) > (4970, 'pk_70', 4970) "
      + "ORDER BY category ASC, score ASC, pk ASC, sk ASC " + "LIMIT 100";
    rs = conn.createStatement().executeQuery(query);
    assertRow(rs, "pk_80", 4980, "category_0", 4980.0);
    assertRow(rs, "pk_90", 4990, "category_0", 4990.0);
    assertFalse(rs.next());

    conn = nextConnection(getUrl());
    query = "SELECT /*+ INDEX(" + tableName + " " + indexName + ") */ pk, sk, category, score "
      + "FROM " + tableName + " " + "WHERE category = 'category_0' "
      + "AND (score, pk) < (5000, 'pk_0') " + "AND (score, pk, sk) > (4970, 'pk_70', 4970) "
      + "ORDER BY category ASC, score DESC, pk DESC, sk DESC " + "LIMIT 100";
    rs = conn.createStatement().executeQuery(query);
    assertRow(rs, "pk_90", 4990, "category_0", 4990.0);
    assertRow(rs, "pk_80", 4980, "category_0", 4980.0);
    assertFalse(rs.next());
  }

  @Test
  public void testRVCWithAlternatingFixedVarWidthPK() throws Exception {
    Connection conn = nextConnection(getUrl());
    String tableName = generateUniqueName();

    String ddl = "CREATE TABLE " + tableName + " (" + "a VARCHAR NOT NULL, " + "b BIGINT NOT NULL, "
      + "c VARCHAR NOT NULL, " + "d INTEGER NOT NULL, " + "e VARCHAR NOT NULL, "
      + "f BIGINT NOT NULL, " + "val VARCHAR, " + "CONSTRAINT pk PRIMARY KEY (a, b, c, d, e, f))";
    conn.createStatement().execute(ddl);

    conn = nextConnection(getUrl());
    PreparedStatement stmt = conn.prepareStatement(
      "UPSERT INTO " + tableName + " (a, b, c, d, e, f, val) VALUES (?, ?, ?, ?, ?, ?, ?)");
    for (int i = 0; i < 50000; i++) {
      stmt.setString(1, "a_" + (i % 5));
      stmt.setLong(2, i);
      stmt.setString(3, "c_" + (i % 7));
      stmt.setInt(4, i % 100);
      stmt.setString(5, "e_" + (i % 3));
      stmt.setLong(6, i);
      stmt.setString(7, "val_" + i);
      stmt.execute();
      if (i % 5000 == 0) {
        conn.commit();
      }
    }
    conn.commit();

    conn = nextConnection(getUrl());
    String query = "SELECT a, b, c, d, e, f FROM " + tableName
      + " WHERE a = 'a_0' AND b >= 24990 AND b <= 25010" + " ORDER BY a, b, c, d, e, f";
    ResultSet rs = conn.createStatement().executeQuery(query);
    assertRow(rs, "a_0", 24990, "c_0", 90, "e_0", 24990);
    assertRow(rs, "a_0", 24995, "c_5", 95, "e_2", 24995);
    assertRow(rs, "a_0", 25000, "c_3", 0, "e_1", 25000);
    assertRow(rs, "a_0", 25005, "c_1", 5, "e_0", 25005);
    assertRow(rs, "a_0", 25010, "c_6", 10, "e_2", 25010);
    assertFalse(rs.next());

    conn = nextConnection(getUrl());
    query = "SELECT a, b, c, d, e, f FROM " + tableName + " WHERE a = 'a_0' AND b <= 25010"
      + " AND (b > 24995 OR (b = 24995 AND c > 'c_5'))" + " ORDER BY a, b, c, d, e, f LIMIT 100";
    rs = conn.createStatement().executeQuery(query);
    assertRow(rs, "a_0", 25000, "c_3", 0, "e_1", 25000);
    assertRow(rs, "a_0", 25005, "c_1", 5, "e_0", 25005);
    assertRow(rs, "a_0", 25010, "c_6", 10, "e_2", 25010);
    assertFalse(rs.next());

    conn = nextConnection(getUrl());
    query = "SELECT a, b, c, d, e, f FROM " + tableName
      + " WHERE a = 'a_0' AND b <= 25010 AND (b, c) > (24995, 'c_5')"
      + " ORDER BY a, b, c, d, e, f LIMIT 100";
    rs = conn.createStatement().executeQuery(query);
    assertRow(rs, "a_0", 25000, "c_3", 0, "e_1", 25000);
    assertRow(rs, "a_0", 25005, "c_1", 5, "e_0", 25005);
    assertRow(rs, "a_0", 25010, "c_6", 10, "e_2", 25010);
    assertFalse(rs.next());

    conn = nextConnection(getUrl());
    query = "SELECT a, b, c, d, e, f FROM " + tableName
      + " WHERE a = 'a_0' AND b <= 25010 AND (b, c, d) > (25000, 'c_3', 0)"
      + " ORDER BY a, b, c, d, e, f LIMIT 100";
    rs = conn.createStatement().executeQuery(query);
    assertRow(rs, "a_0", 25005, "c_1", 5, "e_0", 25005);
    assertRow(rs, "a_0", 25010, "c_6", 10, "e_2", 25010);
    assertFalse(rs.next());

    conn = nextConnection(getUrl());
    query = "SELECT a, b, c, d, e, f FROM " + tableName
      + " WHERE a = 'a_0' AND b <= 25010 AND (b, c, d, e) > (25000, 'c_3', 0, 'e_1')"
      + " ORDER BY a, b, c, d, e, f LIMIT 100";
    rs = conn.createStatement().executeQuery(query);
    assertRow(rs, "a_0", 25005, "c_1", 5, "e_0", 25005);
    assertRow(rs, "a_0", 25010, "c_6", 10, "e_2", 25010);
    assertFalse(rs.next());

    conn = nextConnection(getUrl());
    query =
      "SELECT a, b, c, d, e, f FROM " + tableName + " WHERE a = 'a_0' AND (b, c) <= (25005, 'c_1')"
        + " AND (b, c, d) > (24995, 'c_5', 95)" + " ORDER BY a, b, c, d, e, f LIMIT 100";
    rs = conn.createStatement().executeQuery(query);
    assertRow(rs, "a_0", 25000, "c_3", 0, "e_1", 25000);
    assertRow(rs, "a_0", 25005, "c_1", 5, "e_0", 25005);
    assertFalse(rs.next());

    conn = nextConnection(getUrl());
    query = "SELECT a, b, c, d, e, f FROM " + tableName
      + " WHERE a = 'a_0' AND (b, c, d) <= (25005, 'c_1', 5)"
      + " AND (b, c, d, e, f) > (24995, 'c_5', 95, 'e_2', 24995)"
      + " ORDER BY a, b, c, d, e, f LIMIT 100";
    rs = conn.createStatement().executeQuery(query);
    assertRow(rs, "a_0", 25000, "c_3", 0, "e_1", 25000);
    assertRow(rs, "a_0", 25005, "c_1", 5, "e_0", 25005);
    assertFalse(rs.next());

    conn = nextConnection(getUrl());
    query = "SELECT a, b, c, d, e, f FROM " + tableName
      + " WHERE a = 'a_0' AND b >= 24995 AND (b, c, d) < (25010, 'c_6', 10)"
      + " ORDER BY a, b, c, d, e, f LIMIT 100";
    rs = conn.createStatement().executeQuery(query);
    assertRow(rs, "a_0", 24995, "c_5", 95, "e_2", 24995);
    assertRow(rs, "a_0", 25000, "c_3", 0, "e_1", 25000);
    assertRow(rs, "a_0", 25005, "c_1", 5, "e_0", 25005);
    assertFalse(rs.next());

    conn = nextConnection(getUrl());
    query = "SELECT a, b, c, d, e, f FROM " + tableName
      + " WHERE a = 'a_0' AND b <= 25010 AND (b, c) > (24995, 'c_5')"
      + " ORDER BY a ASC, b DESC, c DESC, d DESC, e DESC, f DESC LIMIT 100";
    rs = conn.createStatement().executeQuery(query);
    assertRow(rs, "a_0", 25010, "c_6", 10, "e_2", 25010);
    assertRow(rs, "a_0", 25005, "c_1", 5, "e_0", 25005);
    assertRow(rs, "a_0", 25000, "c_3", 0, "e_1", 25000);
    assertFalse(rs.next());

    conn = nextConnection(getUrl());
    query = "SELECT a, b, c, d, e, f FROM " + tableName
      + " WHERE a = 'a_0' AND (b, c) <= (25005, 'c_1')" + " AND (b, c, d) > (24995, 'c_5', 95)"
      + " ORDER BY a ASC, b DESC, c DESC, d DESC, e DESC, f DESC LIMIT 100";
    rs = conn.createStatement().executeQuery(query);
    assertRow(rs, "a_0", 25005, "c_1", 5, "e_0", 25005);
    assertRow(rs, "a_0", 25000, "c_3", 0, "e_1", 25000);
    assertFalse(rs.next());

    conn = nextConnection(getUrl());
    query = "SELECT a, b, c, d, e, f FROM " + tableName
      + " WHERE a = 'a_0' AND b >= 24995 AND (b, c, d) < (25010, 'c_6', 10)"
      + " ORDER BY a ASC, b DESC, c DESC, d DESC, e DESC, f DESC LIMIT 100";
    rs = conn.createStatement().executeQuery(query);
    assertRow(rs, "a_0", 25005, "c_1", 5, "e_0", 25005);
    assertRow(rs, "a_0", 25000, "c_3", 0, "e_1", 25000);
    assertRow(rs, "a_0", 24995, "c_5", 95, "e_2", 24995);
    assertFalse(rs.next());
  }

  @Test
  public void testRVCWithBetweenOnIndex() throws Exception {
    Connection conn = nextConnection(getUrl());
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();

    String ddl = "CREATE TABLE " + tableName + " (" + "pk VARCHAR NOT NULL, "
      + "sk BIGINT NOT NULL, " + "category VARCHAR, " + "score DOUBLE, " + "data VARCHAR, "
      + "CONSTRAINT table_pk PRIMARY KEY (pk, sk))";
    conn.createStatement().execute(ddl);

    conn = nextConnection(getUrl());
    conn.createStatement()
      .execute("CREATE INDEX " + indexName + " ON " + tableName + " (category, score)");

    conn = nextConnection(getUrl());
    PreparedStatement stmt = conn.prepareStatement(
      "UPSERT INTO " + tableName + " (pk, sk, category, score, data) VALUES (?, ?, ?, ?, ?)");
    for (int i = 0; i < 20000; i++) {
      stmt.setString(1, "pk_" + (i % 100));
      stmt.setLong(2, i);
      stmt.setString(3, "category_" + (i % 10));
      stmt.setDouble(4, i);
      stmt.setString(5, "data_" + i);
      stmt.execute();
    }
    conn.commit();

    // category_7 rows have scores: 7, 17, 27, ..., 19997
    // Scores in [2000, 8000]: 2007, 2017, ..., 7997
    // With (score, pk, sk) > (7997, 'pk_97', 7997):
    // score=7997 row is (pk_97, 7997) which is NOT > the RVC bound (exact match)
    // Next category_7 score is 8007 which exceeds BETWEEN upper bound of 8000
    conn = nextConnection(getUrl());
    String query =
      "SELECT /*+ INDEX(" + tableName + " " + indexName + ") */ pk, sk, category, score " + "FROM "
        + tableName + " " + "WHERE category = 'category_7' " + "AND score BETWEEN 2000 AND 8000 "
        + "AND (score, pk, sk) > (7997, 'pk_97', 7997) "
        + "ORDER BY category ASC, score ASC, pk ASC, sk ASC " + "LIMIT 100";
    ResultSet rs = conn.createStatement().executeQuery(query);
    assertFalse(rs.next());

    // Verify that using >= instead of > includes the exact match row (7997, 'pk_97', 7997).
    conn = nextConnection(getUrl());
    String queryGte =
      "SELECT /*+ INDEX(" + tableName + " " + indexName + ") */ pk, sk, category, score " + "FROM "
        + tableName + " " + "WHERE category = 'category_7' " + "AND score BETWEEN 2000 AND 8000 "
        + "AND (score, pk, sk) >= (7997, 'pk_97', 7997) "
        + "ORDER BY category ASC, score ASC, pk ASC, sk ASC " + "LIMIT 100";
    rs = conn.createStatement().executeQuery(queryGte);
    assertRow(rs, "pk_97", 7997, "category_7", 7997.0);
    assertFalse("Expected only one row since next category_7 score (8007) exceeds "
      + "BETWEEN upper bound of 8000.", rs.next());

    conn = nextConnection(getUrl());
    query = "SELECT /*+ INDEX(" + tableName + " " + indexName + ") */ pk, sk, category, score "
      + "FROM " + tableName + " " + "WHERE category = 'category_7' "
      + "AND score BETWEEN 2000 AND 8000 " + "AND (score, pk, sk) > (7997, 'pk_97', 7997) "
      + "ORDER BY category ASC, score DESC, pk DESC, sk DESC " + "LIMIT 100";
    rs = conn.createStatement().executeQuery(query);
    assertFalse("Expected no rows with > RVC bound and DESC ordering", rs.next());

    conn = nextConnection(getUrl());
    query = "SELECT /*+ INDEX(" + tableName + " " + indexName + ") */ pk, sk, category, score "
      + "FROM " + tableName + " " + "WHERE category = 'category_7' "
      + "AND score BETWEEN 2000 AND 8000 " + "AND (score, pk, sk) >= (7997, 'pk_97', 7997) "
      + "ORDER BY category ASC, score DESC, pk DESC, sk DESC " + "LIMIT 100";
    rs = conn.createStatement().executeQuery(query);
    assertRow(rs, "pk_97", 7997, "category_7", 7997.0);
    assertFalse("Expected only one row with >= RVC bound and DESC ordering", rs.next());

    // BETWEEN with RVC upper bound < (2037, 'pk_37', 2037).
    // category_7 scores in [2000, 8000]: 2007, 2017, 2027, 2037, ...
    // score=2007: (2007, 'pk_7', 2007) < (2037, 'pk_37', 2037)? Yes -> included
    // score=2017: (2017, 'pk_17', 2017) < (2037, 'pk_37', 2037)? Yes -> included
    // score=2027: (2027, 'pk_27', 2027) < (2037, 'pk_37', 2037)? Yes -> included
    // score=2037: (2037, 'pk_37', 2037) < (2037, 'pk_37', 2037)? No (equal) -> excluded
    // Expected 3 rows in ASC order.
    conn = nextConnection(getUrl());
    query = "SELECT /*+ INDEX(" + tableName + " " + indexName + ") */ pk, sk, category, score "
      + "FROM " + tableName + " " + "WHERE category = 'category_7' "
      + "AND score BETWEEN 2000 AND 8000 " + "AND (score, pk, sk) < (2037, 'pk_37', 2037) "
      + "ORDER BY category ASC, score ASC, pk ASC, sk ASC " + "LIMIT 100";
    rs = conn.createStatement().executeQuery(query);
    assertRow(rs, "pk_7", 2007, "category_7", 2007.0);
    assertRow(rs, "pk_17", 2017, "category_7", 2017.0);
    assertRow(rs, "pk_27", 2027, "category_7", 2027.0);
    assertFalse("Expected exactly 3 rows with BETWEEN and RVC < (2037, 'pk_37', 2037)", rs.next());

    conn = nextConnection(getUrl());
    query = "SELECT /*+ INDEX(" + tableName + " " + indexName + ") */ pk, sk, category, score "
      + "FROM " + tableName + " " + "WHERE category = 'category_7' "
      + "AND score BETWEEN 2000 AND 8000 " + "AND (score, pk, sk) < (2037, 'pk_37', 2037) "
      + "ORDER BY category ASC, score DESC, pk DESC, sk DESC " + "LIMIT 100";
    rs = conn.createStatement().executeQuery(query);
    assertRow(rs, "pk_27", 2027, "category_7", 2027.0);
    assertRow(rs, "pk_17", 2017, "category_7", 2017.0);
    assertRow(rs, "pk_7", 2007, "category_7", 2007.0);
    assertFalse("Expected exactly 3 rows (DESC) with BETWEEN and RVC < (2037, 'pk_37', 2037)",
      rs.next());
  }

  private void assertRow(ResultSet rs, String pk, long sk, String category, double score)
    throws SQLException {
    assertTrue(rs.next());
    assertEquals(pk, rs.getString("pk"));
    assertEquals(sk, rs.getLong("sk"));
    assertEquals(category, rs.getString("category"));
    assertEquals(score, rs.getDouble("score"), 0.000);
  }

  private void assertRow(ResultSet rs, String a, long b, String c, int d, String e, long f)
    throws SQLException {
    assertTrue(rs.next());
    assertEquals(a, rs.getString("a"));
    assertEquals(b, rs.getLong("b"));
    assertEquals(c, rs.getString("c"));
    assertEquals(d, rs.getInt("d"));
    assertEquals(e, rs.getString("e"));
    assertEquals(f, rs.getLong("f"));
  }

  private void assertRowBytes(ResultSet rs, byte[] a, long b, byte[] c, int d, byte[] e, long f)
    throws SQLException {
    assertTrue(rs.next());
    assertArrayEquals(a, rs.getBytes("a"));
    assertEquals(b, rs.getLong("b"));
    assertArrayEquals(c, rs.getBytes("c"));
    assertEquals(d, rs.getInt("d"));
    assertArrayEquals(e, rs.getBytes("e"));
    assertEquals(f, rs.getLong("f"));
  }

  private StringBuilder generateQueryToTest(int numItemsInClause, String fullViewName) {
    StringBuilder querySb =
      new StringBuilder("SELECT OBJECT_ID,OBJECT_DATA2,OBJECT_DATA FROM " + fullViewName);
    querySb.append(" WHERE ((");
    List<String> orClauses = Lists.newArrayList();
    for (int i = 1; i < (numItemsInClause + 1); i++) {
      orClauses.add("OBJECT_ID = ?");
    }
    querySb.append(Joiner.on(" OR ").join(orClauses));
    querySb.append(") AND (KEY_PREFIX,OBJECT_ID) >  (?,?)) ORDER BY OBJECT_ID LIMIT 125");
    System.out.println(querySb);
    return querySb;
  }

  private List<String> selectObjectIdsForInClause(Connection tenantConn, String fullViewName,
    int numItemsInClause) throws SQLException {
    String sqlForObjIds =
      "SELECT OBJECT_ID FROM " + fullViewName + " ORDER BY OBJECT_ID LIMIT " + numItemsInClause;
    PreparedStatement ps = tenantConn.prepareStatement(sqlForObjIds);
    ResultSet rs = ps.executeQuery();
    List<String> objectIdsList = Lists.newArrayList();
    System.out.println("ObjectIds: ");
    while (rs.next()) {
      System.out.println("Object Id: " + rs.getString("OBJECT_ID"));
      objectIdsList.add(rs.getString("OBJECT_ID"));
    }
    assertEquals(numItemsInClause, objectIdsList.size());
    return objectIdsList;
  }

  private void loadDataIntoTenantView(Connection tenantConn, String fullViewName)
    throws SQLException {
    for (int i = 0; i < 2000; i++) {
      String objectId = "v" + i;
      String upsert = "UPSERT INTO " + fullViewName
        + " (OBJECT_ID, OBJECT_DATA, OBJECT_DATA2) VALUES ('" + objectId + "', 'data','data2')";
      PreparedStatement ps = tenantConn.prepareStatement(upsert);
      ps.executeUpdate();
    }
    tenantConn.commit();

    // Validate Data was loaded correctly
    PreparedStatement selectStatement =
      tenantConn.prepareStatement("SELECT OBJECT_ID FROM " + fullViewName + " ORDER BY OBJECT_ID");
    ResultSet rs = selectStatement.executeQuery();
    int count = 0;
    while (rs.next()) {
      count++;
    }
    assertEquals(2000, count);
  }

  private void createBaseTableAndTenantView(Connection tenantConn, String fulTableName,
    String fullViewName) throws SQLException {
    String ddl = "CREATE TABLE IF NOT EXISTS " + fulTableName + " (TENANT_ID CHAR(15) NOT NULL,"
      + " KEY_PREFIX CHAR(3) NOT NULL," + " CREATED_DATE DATE," + " CREATED_BY CHAR(15),"
      + " SYSTEM_MODSTAMP DATE" + " CONSTRAINT PK PRIMARY KEY (TENANT_ID,KEY_PREFIX)"
      + ") VERSIONS=1, MULTI_TENANT=true, IMMUTABLE_ROWS=TRUE, REPLICATION_SCOPE=1";
    createTestTable(getUrl(), ddl);

    String tenantViewDdl = "CREATE VIEW IF NOT EXISTS " + fullViewName
      + " (OBJECT_ID VARCHAR(18) NOT NULL, " + "OBJECT_DATA VARCHAR(131072), "
      + "OBJECT_DATA2 VARCHAR(131072), " + "CONSTRAINT PK PRIMARY KEY (OBJECT_ID)) "
      + "AS SELECT * FROM " + fulTableName + " WHERE KEY_PREFIX = 'z00'";
    // Get tenant specific connection
    PreparedStatement stmt2 = tenantConn.prepareStatement(tenantViewDdl);
    stmt2.execute();
  }

  @Test
  public void testRVCOverlappingKeyRange() throws Exception {
    String tableName = generateUniqueName();
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (" + "hk VARCHAR NOT NULL, "
          + "sk VARCHAR NOT NULL, " + "ihk VARCHAR NOT NULL, " + "isk VARCHAR NOT NULL, "
          + "data VARCHAR " + "CONSTRAINT pk PRIMARY KEY (hk, sk, ihk, isk))");

      PreparedStatement upsert = conn.prepareStatement(
        "UPSERT INTO " + tableName + " (hk, sk, ihk, isk, data) VALUES (?, ?, ?, ?, ?)");
      for (int i = 1; i <= 20; i++) {
        String pad = String.format("%02d", i);
        upsert.setString(1, "hk");
        upsert.setString(2, "sk100");
        upsert.setString(3, "idx");
        upsert.setString(4, "isk" + pad);
        upsert.setString(5, "data" + pad);
        upsert.execute();
      }
      conn.commit();

      String query = "SELECT * FROM " + tableName + " WHERE hk = 'hk' AND "
        + "(sk, ihk, isk) > ('sk100', 'idx', 'isk11') ORDER BY sk, ihk, isk LIMIT 5";
      assertValues1(conn, query);

      query = "SELECT * FROM " + tableName + " WHERE hk = 'hk' AND sk <= 'sk200' "
        + "AND (sk, ihk, isk) > ('sk100', 'idx', 'isk11') ORDER BY sk, ihk, isk" + " LIMIT 5";
      assertValues1(conn, query);

      query =
        "SELECT * FROM " + tableName + " WHERE hk = 'hk'" + " AND sk <= 'sk200' AND sk >= 'sk1'"
          + " AND (sk, ihk, isk) > ('sk100', 'idx', 'isk11')" + " ORDER BY sk, ihk, isk LIMIT 5";
      assertValues1(conn, query);

      query = "SELECT * FROM " + tableName + " WHERE hk = 'hk' AND sk >= 'sk000' "
        + "AND (sk, ihk, isk) >= ('sk100', 'idx', 'isk12') "
        + "AND (sk, ihk, isk) < ('sk100', 'idx', 'isk17') ORDER BY sk, ihk, isk";
      assertValues1(conn, query);

      query = "SELECT * FROM " + tableName + " WHERE hk = 'hk' AND sk >= 'sk000' "
        + "AND (sk, ihk, isk) > ('sk100', 'idx', 'isk11') "
        + "AND (sk, ihk, isk) < ('sk100', 'idx', 'isk17') ORDER BY sk, ihk, isk";
      assertValues1(conn, query);

      query = "SELECT * FROM " + tableName + " WHERE hk = 'hk' AND sk >= 'sk000' "
        + "AND (sk, ihk, isk) < ('sk100', 'idx', 'isk17') ORDER BY sk, ihk, isk";
      assertValues2(conn, query);

      query = "SELECT hk, sk, ihk, isk FROM " + tableName
        + " WHERE hk = 'hk' AND sk <= 'sk200' AND (sk, ihk, isk) < ('sk100', 'idx', 'isk11')"
        + " ORDER BY hk ASC, sk DESC, ihk DESC, isk DESC LIMIT 5";
      assertValues3(conn, query);
    }
  }

  private static void assertValues1(Connection conn, String query) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      ResultSet rs = stmt.executeQuery(query);
      List<String> results = Lists.newArrayList();
      while (rs.next()) {
        results.add(rs.getString("isk"));
      }
      assertEquals(5, results.size());
      assertFalse("Should not include isk11", results.contains("isk11"));
      assertEquals("isk12", results.get(0));
      assertEquals("isk13", results.get(1));
      assertEquals("isk14", results.get(2));
      assertEquals("isk15", results.get(3));
      assertEquals("isk16", results.get(4));
    }
  }

  private static void assertValues2(Connection conn, String query) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      ResultSet rs = stmt.executeQuery(query);
      List<String> results = Lists.newArrayList();
      while (rs.next()) {
        results.add(rs.getString("isk"));
      }
      assertEquals(16, results.size());
      assertTrue("Should not include isk17-isk20 range", !results.contains("isk17")
        && !results.contains("isk18") && !results.contains("isk19") && !results.contains("isk20"));
      assertEquals("isk01", results.get(0));
      assertEquals("isk16", results.get(15));
    }
  }

  private static void assertValues3(Connection conn, String query) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      ResultSet rs = stmt.executeQuery(query);
      List<String> results = Lists.newArrayList();
      while (rs.next()) {
        results.add(rs.getString("isk"));
      }
      assertEquals(5, results.size());
      assertFalse(results.contains("isk11"));
      assertEquals("isk10", results.get(0));
      assertEquals("isk09", results.get(1));
      assertEquals("isk08", results.get(2));
      assertEquals("isk07", results.get(3));
      assertEquals("isk06", results.get(4));
    }
  }

  @Test
  public void testRVCWithAlternatingFixedVarBinaryEncodedPK() throws Exception {
    Connection conn = nextConnection(getUrl());
    String tableName = generateUniqueName();

    String ddl = "CREATE TABLE " + tableName + " (" + "a VARBINARY_ENCODED NOT NULL, "
      + "b BIGINT NOT NULL, " + "c VARBINARY_ENCODED NOT NULL, " + "d INTEGER NOT NULL, "
      + "e VARBINARY_ENCODED NOT NULL, " + "f BIGINT NOT NULL, " + "val VARCHAR, "
      + "CONSTRAINT pk PRIMARY KEY (a, b, c, d, e, f))";
    conn.createStatement().execute(ddl);

    conn = nextConnection(getUrl());
    PreparedStatement stmt = conn.prepareStatement(
      "UPSERT INTO " + tableName + " (a, b, c, d, e, f, val) VALUES (?, ?, ?, ?, ?, ?, ?)");
    for (int i = 0; i < 5000; i++) {
      stmt.setBytes(1, new byte[] { 0x00, 0x0A, (byte) (i % 5) });
      stmt.setLong(2, i);
      stmt.setBytes(3, new byte[] { 0x0C, (byte) (i % 7) });
      stmt.setInt(4, i % 100);
      stmt.setBytes(5, new byte[] { 0x00, 0x0E, (byte) (i % 3) });
      stmt.setLong(6, i);
      stmt.setString(7, "val_" + i);
      stmt.execute();
      if (i % 1000 == 0) {
        conn.commit();
      }
    }
    conn.commit();

    byte[] a0 = { 0x00, 0x0A, 0 };
    byte[] c5 = { 0x0C, 5 };
    byte[] c3 = { 0x0C, 3 };
    byte[] c1 = { 0x0C, 1 };
    byte[] c6 = { 0x0C, 6 };
    byte[] c4 = { 0x0C, 4 };
    byte[] e0 = { 0x00, 0x0E, 0 };
    byte[] e1 = { 0x00, 0x0E, 1 };
    byte[] e2 = { 0x00, 0x0E, 2 };

    conn = nextConnection(getUrl());
    PreparedStatement q = conn.prepareStatement("SELECT a, b, c, d, e, f FROM " + tableName
      + " WHERE a = ? AND b >= 2490 AND b <= 2510 ORDER BY a, b, c, d, e, f");
    q.setBytes(1, a0);
    ResultSet rs = q.executeQuery();
    assertRowBytes(rs, a0, 2490, c5, 90, e0, 2490);
    assertRowBytes(rs, a0, 2495, c3, 95, e2, 2495);
    assertRowBytes(rs, a0, 2500, c1, 0, e1, 2500);
    assertRowBytes(rs, a0, 2505, c6, 5, e0, 2505);
    assertRowBytes(rs, a0, 2510, c4, 10, e2, 2510);
    assertFalse(rs.next());

    conn = nextConnection(getUrl());
    q = conn.prepareStatement("SELECT a, b, c, d, e, f FROM " + tableName
      + " WHERE a = ? AND b <= 2510 AND (b > 2495 OR (b = 2495 AND c > ?))"
      + " ORDER BY a, b, c, d, e, f LIMIT 100");
    q.setBytes(1, a0);
    q.setBytes(2, c3);
    rs = q.executeQuery();
    assertRowBytes(rs, a0, 2500, c1, 0, e1, 2500);
    assertRowBytes(rs, a0, 2505, c6, 5, e0, 2505);
    assertRowBytes(rs, a0, 2510, c4, 10, e2, 2510);
    assertFalse(rs.next());

    conn = nextConnection(getUrl());
    q = conn.prepareStatement("SELECT a, b, c, d, e, f FROM " + tableName
      + " WHERE a = ? AND b <= 2510 AND (b, c) > (?, ?)" + " ORDER BY a, b, c, d, e, f LIMIT 100");
    q.setBytes(1, a0);
    q.setLong(2, 2495);
    q.setBytes(3, c3);
    rs = q.executeQuery();
    assertRowBytes(rs, a0, 2500, c1, 0, e1, 2500);
    assertRowBytes(rs, a0, 2505, c6, 5, e0, 2505);
    assertRowBytes(rs, a0, 2510, c4, 10, e2, 2510);
    assertFalse(rs.next());

    conn = nextConnection(getUrl());
    q = conn.prepareStatement("SELECT a, b, c, d, e, f FROM " + tableName
      + " WHERE a = ? AND b <= 2510 AND (b, c, d) > (?, ?, ?)"
      + " ORDER BY a, b, c, d, e, f LIMIT 100");
    q.setBytes(1, a0);
    q.setLong(2, 2500);
    q.setBytes(3, c1);
    q.setInt(4, 0);
    rs = q.executeQuery();
    assertRowBytes(rs, a0, 2505, c6, 5, e0, 2505);
    assertRowBytes(rs, a0, 2510, c4, 10, e2, 2510);
    assertFalse(rs.next());

    conn = nextConnection(getUrl());
    q = conn.prepareStatement("SELECT a, b, c, d, e, f FROM " + tableName
      + " WHERE a = ? AND b <= 2510 AND (b, c, d, e) > (?, ?, ?, ?)"
      + " ORDER BY a, b, c, d, e, f LIMIT 100");
    q.setBytes(1, a0);
    q.setLong(2, 2500);
    q.setBytes(3, c1);
    q.setInt(4, 0);
    q.setBytes(5, e1);
    rs = q.executeQuery();
    assertRowBytes(rs, a0, 2505, c6, 5, e0, 2505);
    assertRowBytes(rs, a0, 2510, c4, 10, e2, 2510);
    assertFalse(rs.next());

    conn = nextConnection(getUrl());
    q = conn.prepareStatement("SELECT a, b, c, d, e, f FROM " + tableName
      + " WHERE a = ? AND (b, c) <= (?, ?) AND (b, c, d) > (?, ?, ?)"
      + " ORDER BY a, b, c, d, e, f LIMIT 100");
    q.setBytes(1, a0);
    q.setLong(2, 2505);
    q.setBytes(3, c6);
    q.setLong(4, 2495);
    q.setBytes(5, c3);
    q.setInt(6, 95);
    rs = q.executeQuery();
    assertRowBytes(rs, a0, 2500, c1, 0, e1, 2500);
    assertRowBytes(rs, a0, 2505, c6, 5, e0, 2505);
    assertFalse(rs.next());

    conn = nextConnection(getUrl());
    q = conn.prepareStatement("SELECT a, b, c, d, e, f FROM " + tableName
      + " WHERE a = ? AND (b, c, d) <= (?, ?, ?) AND (b, c, d, e, f) > (?, ?, ?, ?, ?)"
      + " ORDER BY a, b, c, d, e, f LIMIT 100");
    q.setBytes(1, a0);
    q.setLong(2, 2505);
    q.setBytes(3, c6);
    q.setInt(4, 5);
    q.setLong(5, 2495);
    q.setBytes(6, c3);
    q.setInt(7, 95);
    q.setBytes(8, e2);
    q.setLong(9, 2495);
    rs = q.executeQuery();
    assertRowBytes(rs, a0, 2500, c1, 0, e1, 2500);
    assertRowBytes(rs, a0, 2505, c6, 5, e0, 2505);
    assertFalse(rs.next());

    conn = nextConnection(getUrl());
    q = conn.prepareStatement("SELECT a, b, c, d, e, f FROM " + tableName
      + " WHERE a = ? AND b >= 2495 AND (b, c, d) < (?, ?, ?)"
      + " ORDER BY a, b, c, d, e, f LIMIT 100");
    q.setBytes(1, a0);
    q.setLong(2, 2510);
    q.setBytes(3, c4);
    q.setInt(4, 10);
    rs = q.executeQuery();
    assertRowBytes(rs, a0, 2495, c3, 95, e2, 2495);
    assertRowBytes(rs, a0, 2500, c1, 0, e1, 2500);
    assertRowBytes(rs, a0, 2505, c6, 5, e0, 2505);
    assertFalse(rs.next());

    conn = nextConnection(getUrl());
    q = conn.prepareStatement(
      "SELECT a, b, c, d, e, f FROM " + tableName + " WHERE a = ? AND b <= 2510 AND (b, c) > (?, ?)"
        + " ORDER BY a ASC, b DESC, c DESC, d DESC, e DESC, f DESC LIMIT 100");
    q.setBytes(1, a0);
    q.setLong(2, 2495);
    q.setBytes(3, c3);
    rs = q.executeQuery();
    assertRowBytes(rs, a0, 2510, c4, 10, e2, 2510);
    assertRowBytes(rs, a0, 2505, c6, 5, e0, 2505);
    assertRowBytes(rs, a0, 2500, c1, 0, e1, 2500);
    assertFalse(rs.next());

    conn = nextConnection(getUrl());
    q = conn.prepareStatement("SELECT a, b, c, d, e, f FROM " + tableName
      + " WHERE a = ? AND (b, c) <= (?, ?) AND (b, c, d) > (?, ?, ?)"
      + " ORDER BY a ASC, b DESC, c DESC, d DESC, e DESC, f DESC LIMIT 100");
    q.setBytes(1, a0);
    q.setLong(2, 2505);
    q.setBytes(3, c6);
    q.setLong(4, 2495);
    q.setBytes(5, c3);
    q.setInt(6, 95);
    rs = q.executeQuery();
    assertRowBytes(rs, a0, 2505, c6, 5, e0, 2505);
    assertRowBytes(rs, a0, 2500, c1, 0, e1, 2500);
    assertFalse(rs.next());

    conn = nextConnection(getUrl());
    q = conn.prepareStatement("SELECT a, b, c, d, e, f FROM " + tableName
      + " WHERE a = ? AND b >= 2495 AND (b, c, d) < (?, ?, ?)"
      + " ORDER BY a ASC, b DESC, c DESC, d DESC, e DESC, f DESC LIMIT 100");
    q.setBytes(1, a0);
    q.setLong(2, 2510);
    q.setBytes(3, c4);
    q.setInt(4, 10);
    rs = q.executeQuery();
    assertRowBytes(rs, a0, 2505, c6, 5, e0, 2505);
    assertRowBytes(rs, a0, 2500, c1, 0, e1, 2500);
    assertRowBytes(rs, a0, 2495, c3, 95, e2, 2495);
    assertFalse(rs.next());
  }

  @Test
  public void testRVCWithUncoveredIndex1() throws Exception {
    Connection conn = nextConnection(getUrl());
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();

    String ddl = "CREATE TABLE " + tableName + " (HK VARBINARY_ENCODED NOT NULL,"
      + "  SK VARBINARY_ENCODED NOT NULL," + "  GSI_HK VARBINARY_ENCODED,"
      + "  GSI_SK VARBINARY_ENCODED," + "  DATA VARCHAR," + "  CONSTRAINT pk PRIMARY KEY(HK, SK))";
    conn.createStatement().execute(ddl);

    conn = nextConnection(getUrl());
    conn.createStatement()
      .execute("CREATE UNCOVERED INDEX " + indexName + " ON " + tableName + " (GSI_HK, GSI_SK)");

    conn = nextConnection(getUrl());
    PreparedStatement upsert = conn.prepareStatement(
      "UPSERT INTO " + tableName + " (HK, SK, GSI_HK, GSI_SK, DATA) VALUES (?, ?, ?, ?, ?)");

    byte[] gsiHk = new byte[] { 0x00, 0x00, 0x00, 0x0A };
    byte[] gsiSk = "1".getBytes();

    byte[] hk1 = new byte[] { 0x0B, 0x01 };
    byte[] sk1 = "1".getBytes();
    upsert.setBytes(1, hk1);
    upsert.setBytes(2, sk1);
    upsert.setBytes(3, gsiHk);
    upsert.setBytes(4, gsiSk);
    upsert.setString(5, "row1-should-be-excluded");
    upsert.execute();

    byte[] hk2 = new byte[] { 0x0B, 0x01 };
    byte[] sk2 = "2".getBytes();
    upsert.setBytes(1, hk2);
    upsert.setBytes(2, sk2);
    upsert.setBytes(3, gsiHk);
    upsert.setBytes(4, gsiSk);
    upsert.setString(5, "row2-include");
    upsert.execute();

    byte[] hk3 = new byte[] { 0x0B, 0x02 };
    byte[] sk3 = "1".getBytes();
    upsert.setBytes(1, hk3);
    upsert.setBytes(2, sk3);
    upsert.setBytes(3, gsiHk);
    upsert.setBytes(4, gsiSk);
    upsert.setString(5, "row3-include");
    upsert.execute();

    conn.commit();

    conn = nextConnection(getUrl());
    String query = "SELECT /*+ INDEX(" + tableName + " " + indexName + ") */ "
      + "HK, SK, DATA FROM " + tableName + " WHERE GSI_HK = ?" + " AND GSI_SK = ?"
      + " AND (GSI_SK, HK, SK) > (?, ?, ?)" + " ORDER BY GSI_SK, HK, SK";

    PreparedStatement stmt = conn.prepareStatement(query);
    stmt.setBytes(1, gsiHk);
    stmt.setBytes(2, gsiSk);
    stmt.setBytes(3, gsiSk);
    stmt.setBytes(4, hk1);
    stmt.setBytes(5, sk1);
    ResultSet rs = stmt.executeQuery();

    assertTrue("Expected row2 to be returned", rs.next());
    assertArrayEquals(hk2, rs.getBytes("HK"));
    assertArrayEquals(sk2, rs.getBytes("SK"));
    assertEquals("row2-include", rs.getString("DATA"));

    assertTrue("Expected row3 to be returned", rs.next());
    assertArrayEquals(hk3, rs.getBytes("HK"));
    assertArrayEquals(sk3, rs.getBytes("SK"));
    assertEquals("row3-include", rs.getString("DATA"));

    assertFalse("Expected only 2 rows but got more", rs.next());
  }

  @Test
  public void testRVCWithUncoveredIndex2() throws Exception {
    Connection conn = nextConnection(getUrl());
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();

    String ddl = "CREATE TABLE " + tableName + " (HK VARCHAR NOT NULL," + "  SK VARCHAR NOT NULL,"
      + "  GSI_HK VARCHAR," + "  GSI_SK VARCHAR," + "  DATA VARCHAR,"
      + "  CONSTRAINT pk PRIMARY KEY(HK, SK))";
    conn.createStatement().execute(ddl);

    conn = nextConnection(getUrl());
    conn.createStatement()
      .execute("CREATE UNCOVERED INDEX " + indexName + " ON " + tableName + " (GSI_HK, GSI_SK)");

    conn = nextConnection(getUrl());
    PreparedStatement upsert = conn.prepareStatement(
      "UPSERT INTO " + tableName + " (HK, SK, GSI_HK, GSI_SK, DATA) VALUES (?, ?, ?, ?, ?)");

    String gsiHk = "gsi_hk_1";
    String gsiSk = "gsi_sk_1";

    String hk1 = "hk_bb";
    String sk1 = "sk_1";
    upsert.setString(1, hk1);
    upsert.setString(2, sk1);
    upsert.setString(3, gsiHk);
    upsert.setString(4, gsiSk);
    upsert.setString(5, "row1-should-be-excluded");
    upsert.execute();

    String hk2 = "hk_bb";
    String sk2 = "sk_2";
    upsert.setString(1, hk2);
    upsert.setString(2, sk2);
    upsert.setString(3, gsiHk);
    upsert.setString(4, gsiSk);
    upsert.setString(5, "row2-include");
    upsert.execute();

    String hk3 = "hk_cc";
    String sk3 = "sk_1";
    upsert.setString(1, hk3);
    upsert.setString(2, sk3);
    upsert.setString(3, gsiHk);
    upsert.setString(4, gsiSk);
    upsert.setString(5, "row3-include");
    upsert.execute();

    conn.commit();

    conn = nextConnection(getUrl());
    String query = "SELECT /*+ INDEX(" + tableName + " " + indexName + ") */ "
      + "HK, SK, DATA FROM " + tableName + " WHERE GSI_HK = ?" + " AND GSI_SK = ?"
      + " AND (GSI_SK, HK, SK) > (?, ?, ?)" + " ORDER BY GSI_SK, HK, SK";

    PreparedStatement stmt = conn.prepareStatement(query);
    stmt.setString(1, gsiHk);
    stmt.setString(2, gsiSk);
    stmt.setString(3, gsiSk);
    stmt.setString(4, hk1);
    stmt.setString(5, sk1);
    ResultSet rs = stmt.executeQuery();

    assertTrue("Expected row2 to be returned", rs.next());
    assertEquals(hk2, rs.getString("HK"));
    assertEquals(sk2, rs.getString("SK"));
    assertEquals("row2-include", rs.getString("DATA"));

    assertTrue("Expected row3 to be returned", rs.next());
    assertEquals(hk3, rs.getString("HK"));
    assertEquals(sk3, rs.getString("SK"));
    assertEquals("row3-include", rs.getString("DATA"));

    assertFalse("Expected only 2 rows but got more", rs.next());
  }

}
