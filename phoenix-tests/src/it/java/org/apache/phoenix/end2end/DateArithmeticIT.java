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

import static org.apache.phoenix.query.QueryConstants.MILLIS_IN_DAY;
import static org.apache.phoenix.util.TestUtil.B_VALUE;
import static org.apache.phoenix.util.TestUtil.E_VALUE;
import static org.apache.phoenix.util.TestUtil.ROW1;
import static org.apache.phoenix.util.TestUtil.ROW2;
import static org.apache.phoenix.util.TestUtil.ROW3;
import static org.apache.phoenix.util.TestUtil.ROW4;
import static org.apache.phoenix.util.TestUtil.ROW6;
import static org.apache.phoenix.util.TestUtil.ROW7;
import static org.apache.phoenix.util.TestUtil.ROW9;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

@Category(ParallelStatsDisabledTest.class)
public class DateArithmeticIT extends ParallelStatsDisabledIT {
    
    @Test
    public void testValidArithmetic() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Date date = new Date(System.currentTimeMillis());
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName =
                initATableValues(generateUniqueName(), getOrganizationId(), getDefaultSplits(getOrganizationId()),
                    date, null, getUrl(), "COLUMN_ENCODED_BYTES=0");

        String[] queries = new String[] { 
                "SELECT entity_id,organization_id FROM " + tableName + " where (A_DATE - A_DATE) * 5 < 0",
                "SELECT entity_id,organization_id FROM " + tableName + " where 1 + A_DATE  < A_DATE",
                "SELECT entity_id,organization_id FROM " + tableName + " where A_DATE - 1 < A_DATE",
                };

        for (String query : queries) {
                PreparedStatement statement = conn.prepareStatement(query);
                statement.executeQuery();
        }
        conn.close();
    }
    
    @Test
    public void testDateAdd() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Date date = new Date(System.currentTimeMillis());
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName =
                initATableValues(generateUniqueName(), getOrganizationId(), getDefaultSplits(getOrganizationId()),
                    date, null, getUrl(), "COLUMN_ENCODED_BYTES=0");
        String query = "SELECT entity_id, b_string FROM " + tableName + " WHERE a_date + CAST(0.5 AS DOUBLE) < ?";
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setDate(1, new Date(System.currentTimeMillis() + MILLIS_IN_DAY));
            ResultSet rs = statement.executeQuery();
            @SuppressWarnings("unchecked")
            List<List<Object>> expectedResults = Lists.newArrayList(
                    Arrays.<Object>asList(ROW1, B_VALUE),
                    Arrays.<Object>asList( ROW4, B_VALUE), 
                    Arrays.<Object>asList(ROW7, B_VALUE));
            assertValuesEqualsResultSet(rs, expectedResults);
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testDateSubtract() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Date date = new Date(System.currentTimeMillis());
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName =
                initATableValues(generateUniqueName(), getOrganizationId(), getDefaultSplits(getOrganizationId()),
                    date, null, getUrl(), "COLUMN_ENCODED_BYTES=0");
        String query = "SELECT entity_id, b_string FROM " + tableName + " WHERE a_date - CAST(0.5 AS DOUBLE) > ?";
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setDate(1, new Date(System.currentTimeMillis() + MILLIS_IN_DAY));
            ResultSet rs = statement.executeQuery();
            @SuppressWarnings("unchecked")
            List<List<Object>> expectedResults = Lists.newArrayList(
                    Arrays.<Object>asList(ROW3, E_VALUE),
                    Arrays.<Object>asList( ROW6, E_VALUE), 
                    Arrays.<Object>asList(ROW9, E_VALUE));
            assertValuesEqualsResultSet(rs, expectedResults);
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDateDateSubtract() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Date date = new Date(System.currentTimeMillis());
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName =
                initATableValues(generateUniqueName(), getOrganizationId(), getDefaultSplits(getOrganizationId()),
                    date, null, getUrl(), "COLUMN_ENCODED_BYTES=0");
        PreparedStatement statement = conn.prepareStatement("UPSERT INTO  " + tableName + " (organization_id,entity_id,a_time) VALUES(?,?,?)");
        statement.setString(1, getOrganizationId());
        statement.setString(2, ROW2);
        statement.setDate(3, date);
        statement.execute();
        statement.setString(2, ROW3);
        statement.setDate(3, date);
        statement.execute();
        statement.setString(2, ROW4);
        statement.setDate(3, new Date(date.getTime() + MILLIS_IN_DAY - 1));
        statement.execute();
        statement.setString(2, ROW6);
        statement.setDate(3, new Date(date.getTime() + MILLIS_IN_DAY - 1));
        statement.execute();
        statement.setString(2, ROW9);
        statement.setDate(3, date);
        statement.execute();
        conn.commit();
        conn.close();

        conn = DriverManager.getConnection(url, props);
        try {
            statement = conn.prepareStatement("SELECT entity_id, b_string FROM " + tableName + " WHERE a_date - a_time > 1");
            ResultSet rs = statement.executeQuery();
            @SuppressWarnings("unchecked")
            List<List<Object>> expectedResults = Lists.newArrayList(
                    Arrays.<Object>asList(ROW3, E_VALUE),
                    Arrays.<Object>asList( ROW6, E_VALUE), 
                    Arrays.<Object>asList(ROW9, E_VALUE));
            assertValuesEqualsResultSet(rs, expectedResults);
        } finally {
            conn.close();
        }
    }
 
    @Test
    public void testAddTimeStamp() throws Exception {
      Connection conn;
      PreparedStatement stmt;
      ResultSet rs;
      String tName = generateUniqueName();

      Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
      conn = DriverManager.getConnection(getUrl(), props);
      conn.createStatement()
              .execute(
                      "create table " + tName + " (ts timestamp primary key)");
      conn.close();

      conn = DriverManager.getConnection(getUrl(), props);
      stmt = conn.prepareStatement("upsert into " + tName + " values (?)");
      stmt.setTimestamp(1, new Timestamp(1995 - 1900, 4, 2, 1, 1, 1, 1));
      stmt.execute();
      conn.commit();
      conn = DriverManager.getConnection(getUrl(), props);
      rs = conn.createStatement().executeQuery("SELECT ts FROM " + tName + "");
      assertTrue(rs.next());
      assertEquals("1995-05-02 01:01:01.000000001",rs.getTimestamp(1).toString());
      conn = DriverManager.getConnection(getUrl(), props);
      rs = conn.createStatement().executeQuery("SELECT ts + 1 FROM " + tName + "");
      assertTrue(rs.next());
      assertEquals("1995-05-03 01:01:01.000000001",rs.getTimestamp(1).toString());
    }
 
    @Test
    public void testSubtractTimeStamp() throws Exception {
      Connection conn;
      PreparedStatement stmt;
      ResultSet rs;
      String tName = generateUniqueName();
      Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
      conn = DriverManager.getConnection(getUrl(), props);
      conn.createStatement()
              .execute(
                      "create table " + tName + " (ts timestamp primary key)");
      conn.close();

      conn = DriverManager.getConnection(getUrl(), props);
      stmt = conn.prepareStatement("upsert into " + tName + " values (?)");
      stmt.setTimestamp(1, new Timestamp(1995 - 1900, 4, 2, 1, 1, 1, 1));
      stmt.execute();
      conn.commit();
      conn = DriverManager.getConnection(getUrl(), props);
      rs = conn.createStatement().executeQuery("SELECT ts FROM " + tName);
      assertTrue(rs.next());
      assertEquals("1995-05-02 01:01:01.000000001",rs.getTimestamp(1).toString());
      conn = DriverManager.getConnection(getUrl(), props);
      rs = conn.createStatement().executeQuery("SELECT ts - 1 FROM " + tName);
      assertTrue(rs.next());
      assertEquals("1995-05-01 01:01:01.000000001",rs.getTimestamp(1).toString());
    }
    
    @Test
    public void testAddTime() throws Exception {
      Connection conn;
      PreparedStatement stmt;
      ResultSet rs;
      String tName = generateUniqueName();

      Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
      conn = DriverManager.getConnection(getUrl(), props);
      conn.createStatement()
              .execute(
                      "create table " + tName + " (ts time primary key)");
      conn.close();

      conn = DriverManager.getConnection(getUrl(), props);
      stmt = conn.prepareStatement("upsert into " + tName + " values (?)");
      Time time = new Time(1995 - 1900, 4, 2);
      stmt.setTime(1, time);
      stmt.execute();
      conn.commit();
      conn = DriverManager.getConnection(getUrl(), props);
      rs = conn.createStatement().executeQuery("SELECT ts FROM " + tName);
      assertTrue(rs.next());
      assertEquals(time.getTime(),rs.getTimestamp(1).getTime());
      conn = DriverManager.getConnection(getUrl(), props);
      rs = conn.createStatement().executeQuery("SELECT ts + 1 FROM " + tName);
      assertTrue(rs.next());
      assertEquals(time.getTime() + MILLIS_IN_DAY,rs.getTimestamp(1).getTime());
    }

    @Test
    public void testSubtractTime() throws Exception {
      Connection conn;
      PreparedStatement stmt;
      ResultSet rs;
      String tName = generateUniqueName();
      Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
      conn = DriverManager.getConnection(getUrl(), props);
      conn.createStatement()
              .execute(
                      "create table " + tName + " (ts time primary key)");
      conn.close();

      conn = DriverManager.getConnection(getUrl(), props);
      stmt = conn.prepareStatement("upsert into " + tName + " values (?)");
      Time time = new Time(1995 - 1900, 4, 2);
      stmt.setTime(1, time);
      stmt.execute();
      conn.commit();
      conn = DriverManager.getConnection(getUrl(), props);
      rs = conn.createStatement().executeQuery("SELECT ts FROM " + tName + "");
      assertTrue(rs.next());
      assertEquals(time.getTime(),rs.getTimestamp(1).getTime());
      conn = DriverManager.getConnection(getUrl(), props);
      rs = conn.createStatement().executeQuery("SELECT ts - 1 FROM " + tName);
      assertTrue(rs.next());
      assertEquals(time.getTime() - MILLIS_IN_DAY,rs.getTimestamp(1).getTime());
    }
 
    @Test
    public void testSubtractDate() throws Exception {
      Connection conn;
      PreparedStatement stmt;
      ResultSet rs;
      String tName = generateUniqueName();

      Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
      conn = DriverManager.getConnection(getUrl(), props);
      conn.createStatement()
              .execute(
                      "create table " + tName + " (ts date primary key)");
      conn.close();

      conn = DriverManager.getConnection(getUrl(), props);
      stmt = conn.prepareStatement("upsert into " + tName + " values (?)");
      stmt.setDate(1, new Date(1995 - 1900, 4, 2));
      stmt.execute();
      conn.commit();
      conn = DriverManager.getConnection(getUrl(), props);
      rs = conn.createStatement().executeQuery("SELECT ts FROM " + tName);
      assertTrue(rs.next());
      assertEquals("1995-05-02",rs.getDate(1).toString());
      conn = DriverManager.getConnection(getUrl(), props);
      rs = conn.createStatement().executeQuery("SELECT ts - 1 FROM " + tName);
      assertTrue(rs.next());
      assertEquals("1995-05-01",rs.getDate(1).toString());
    }
}
