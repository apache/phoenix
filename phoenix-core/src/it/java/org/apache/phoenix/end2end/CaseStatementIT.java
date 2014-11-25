/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 *distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end;

import static org.apache.phoenix.util.TestUtil.ROW1;
import static org.apache.phoenix.util.TestUtil.ROW2;
import static org.apache.phoenix.util.TestUtil.ROW3;
import static org.apache.phoenix.util.TestUtil.ROW4;
import static org.apache.phoenix.util.TestUtil.ROW5;
import static org.apache.phoenix.util.TestUtil.ROW7;
import static org.apache.phoenix.util.TestUtil.ROW8;
import static org.apache.phoenix.util.TestUtil.ROW9;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;

@RunWith(Parameterized.class)
public class CaseStatementIT extends BaseQueryIT {

    public CaseStatementIT(String indexDDL) {
        super(indexDDL);
    }
    
    @Parameters(name="{0}")
    public static Collection<Object> data() {
        return QueryIT.data();
    }    
    
    @Test
    public void testSimpleCaseStatement() throws Exception {
        String query = "SELECT CASE a_integer WHEN 1 THEN 'a' WHEN 2 THEN 'b' WHEN 3 THEN 'c' ELSE 'd' END, entity_id AS a FROM ATABLE WHERE organization_id=? AND a_integer < 6";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            @SuppressWarnings("unchecked")
            List<List<Object>> expectedResults = Lists.newArrayList(
                Arrays.<Object>asList("a",ROW1),
                Arrays.<Object>asList( "b",ROW2), 
                Arrays.<Object>asList("c",ROW3),
                Arrays.<Object>asList("d",ROW4),
                Arrays.<Object>asList("d",ROW5));
            assertValuesEqualsResultSet(rs, expectedResults);
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testMultiCondCaseStatement() throws Exception {
        String query = "SELECT CASE WHEN a_integer <= 2 THEN 1.5 WHEN a_integer = 3 THEN 2 WHEN a_integer <= 6 THEN 4.5 ELSE 5 END AS a FROM ATABLE WHERE organization_id=?";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(1.5), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(1.5), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(2), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(4.5), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(4.5), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(4.5), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(5), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(5), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(5), rs.getBigDecimal(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testPartialEvalCaseStatement() throws Exception {
        String query = "SELECT entity_id FROM ATABLE WHERE organization_id=? and CASE WHEN 1234 = a_integer THEN 1 WHEN x_integer = 5 THEN 2 ELSE 3 END = 2";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROW7, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testFoundIndexOnPartialEvalCaseStatement() throws Exception {
        String query = "SELECT entity_id FROM ATABLE WHERE organization_id=? and CASE WHEN a_integer = 1234 THEN 1 WHEN x_integer = 3 THEN y_integer ELSE 3 END = 300";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROW9, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    // TODO: we need some tests that have multiple versions of key values
    @Test
    public void testUnfoundMultiColumnCaseStatement() throws Exception {
        String query = "SELECT entity_id, b_string FROM ATABLE WHERE organization_id=? and CASE WHEN a_integer = 1234 THEN 1 WHEN a_date < ? THEN y_integer WHEN x_integer = 4 THEN 4 ELSE 3 END = 4";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setDate(2, new Date(System.currentTimeMillis()));
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROW8, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testUnfoundSingleColumnCaseStatement() throws Exception {
        String query = "SELECT entity_id, b_string FROM ATABLE WHERE organization_id=? and CASE WHEN a_integer = 0 or a_integer != 0 THEN 1 ELSE 0 END = 0";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        // Set ROW5.A_INTEGER to null so that we have one row
        // where the else clause of the CASE statement will
        // fire.
        url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 1); // Run query at timestamp 5
        Connection upsertConn = DriverManager.getConnection(url, props);
        String upsertStmt =
            "upsert into " +
            "ATABLE(" +
            "    ENTITY_ID, " +
            "    ORGANIZATION_ID, " +
            "    A_INTEGER) " +
            "VALUES ('" + ROW5 + "','" + tenantId + "', null)";
        upsertConn.setAutoCommit(true); // Test auto commit
        // Insert all rows at ts
        PreparedStatement stmt = upsertConn.prepareStatement(upsertStmt);
        stmt.execute(); // should commit too
        upsertConn.close();
        
        PreparedStatement statement = conn.prepareStatement(query);
        statement.setString(1, tenantId);
        ResultSet rs = statement.executeQuery();
        assertTrue(rs.next());
        assertEquals(ROW5, rs.getString(1));
        assertFalse(rs.next());
        conn.close();
    }
    
    @Test
    public void testNonNullMultiCondCaseStatement() throws Exception {
        String query = "SELECT CASE WHEN entity_id = '000000000000000' THEN 1 WHEN entity_id = '000000000000001' THEN 2 ELSE 3 END FROM ATABLE WHERE organization_id=?";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            ResultSetMetaData rsm = rs.getMetaData();
            assertEquals(ResultSetMetaData.columnNoNulls,rsm.isNullable(1));
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testNullMultiCondCaseStatement() throws Exception {
        String query = "SELECT CASE WHEN entity_id = '000000000000000' THEN 1 WHEN entity_id = '000000000000001' THEN 2 END FROM ATABLE WHERE organization_id=?";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            ResultSetMetaData rsm = rs.getMetaData();
            assertEquals(ResultSetMetaData.columnNullable,rsm.isNullable(1));
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testNullabilityMultiCondCaseStatement() throws Exception {
        String query = "SELECT CASE WHEN a_integer <= 2 THEN ? WHEN a_integer = 3 THEN ? WHEN a_integer <= ? THEN ? ELSE 5 END AS a FROM ATABLE WHERE organization_id=?";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setBigDecimal(1,BigDecimal.valueOf(1.5));
            statement.setInt(2,2);
            statement.setInt(3,6);
            statement.setBigDecimal(4,BigDecimal.valueOf(4.5));
            statement.setString(5, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(1.5), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(1.5), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(2), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(4.5), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(4.5), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(4.5), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(5), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(5), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(5), rs.getBigDecimal(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

}
