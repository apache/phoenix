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

import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.apache.phoenix.util.TestUtil.ROW1;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;

public class Unnest1IT extends ArrayIT{
    @Test
    public void testSelectUnnestDoubleArray() throws Exception{
        String tenantId = getOrganizationId();
        String tableName = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(tableName, tenantId, null, false, getUrl());
        String query = "SELECT UNNEST(a_double_array) AS A FROM " + tableName;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try(Connection conn = DriverManager.getConnection(getUrl(), props)){
            TestUtil.analyzeTable(conn, tableName);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(25.343d,rs.getDouble("A"),0.00001d);
            assertTrue(rs.next());
            assertEquals(36.763d,rs.getDouble("A"),0.00001d);
            assertTrue(rs.next());
            assertEquals(37.56d,rs.getDouble("A"),0.00001d);
            assertTrue(rs.next());
            assertEquals(386.63d,rs.getDouble("A"),0.00001d);
            assertFalse(rs.next());
        }
    }



    @Test
    public void testSelectUnnestAndElemRefFromSameArray() throws Exception{
        String tenantId = getOrganizationId();
        String tableName = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(tableName, tenantId, null, false, getUrl());
        String query = "SELECT UNNEST(a_string_array) AS A, a_string_array[1] AS B FROM " + tableName;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try(Connection conn = DriverManager.getConnection(getUrl(), props)){
            TestUtil.analyzeTable(conn, tableName);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("ABC",rs.getString("A"));
            assertEquals("ABC", rs.getString("B"));
            assertTrue(rs.next());
            assertEquals("CEDF",rs.getString("A"));
            assertEquals("ABC", rs.getString("B"));
            assertTrue(rs.next());
            assertEquals("XYZWER",rs.getString("A"));
            assertEquals("ABC", rs.getString("B"));
            assertTrue(rs.next());
            assertEquals("AB",rs.getString("A"));
            assertEquals("ABC", rs.getString("B"));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSelectUnnestAndElemRefFromDifferentArray() throws Exception{
        String tenantId = getOrganizationId();
        String tableName = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(tableName, tenantId, null, false, getUrl());
        String query = "SELECT UNNEST(x_long_array) AS A, a_string_array[1] AS B FROM " + tableName;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try(Connection conn = DriverManager.getConnection(getUrl(), props)){
            TestUtil.analyzeTable(conn, tableName);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(25l,rs.getLong("A"));
            assertEquals("ABC", rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(36l,rs.getLong("A"));
            assertEquals("ABC", rs.getString("B"));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSelectUnnestDoubleAndVarcharArray() throws Exception {
        String tenantId = getOrganizationId();
        String tableName = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(tableName, tenantId, null, false, getUrl());
        String query = "SELECT UNNEST(a_double_array) AS A, UNNEST(a_string_array) AS B FROM " + tableName;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try(Connection conn = DriverManager.getConnection(getUrl(), props)){
            TestUtil.analyzeTable(conn, tableName);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(25.343d,rs.getDouble("A"),0.00001d);
            assertEquals("ABC",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(36.763d,rs.getDouble("A"),0.00001d);
            assertEquals("ABC",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(37.56d,rs.getDouble("A"),0.00001d);
            assertEquals("ABC",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(386.63d,rs.getDouble("A"),0.00001d);
            assertEquals("ABC",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(25.343d,rs.getDouble("A"),0.00001d);
            assertEquals("CEDF",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(36.763d,rs.getDouble("A"),0.00001d);
            assertEquals("CEDF",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(37.56d,rs.getDouble("A"),0.00001d);
            assertEquals("CEDF",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(386.63d,rs.getDouble("A"),0.00001d);
            assertEquals("CEDF",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(25.343d,rs.getDouble("A"),0.00001d);
            assertEquals("XYZWER",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(36.763d,rs.getDouble("A"),0.00001d);
            assertEquals("XYZWER",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(37.56d,rs.getDouble("A"),0.00001d);
            assertEquals("XYZWER",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(386.63d,rs.getDouble("A"),0.00001d);
            assertEquals("XYZWER",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(25.343d,rs.getDouble("A"),0.00001d);
            assertEquals("AB",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(36.763d,rs.getDouble("A"),0.00001d);
            assertEquals("AB",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(37.56d,rs.getDouble("A"),0.00001d);
            assertEquals("AB",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(386.63d,rs.getDouble("A"),0.00001d);
            assertEquals("AB",rs.getString("B"));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSelectUnnestDoubleAndVarcharArrayOffset5Limit5() throws Exception {
        String tenantId = getOrganizationId();
        String tableName = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(tableName, tenantId, null, false, getUrl());
        String query = "SELECT UNNEST(a_double_array) AS A, UNNEST(a_string_array) AS B FROM " + tableName + "  LIMIT 5 OFFSET 5";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try(Connection conn = DriverManager.getConnection(getUrl(), props)){
            TestUtil.analyzeTable(conn, tableName);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(36.763d,rs.getDouble("A"),0.00001d);
            assertEquals("CEDF",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(37.56d,rs.getDouble("A"),0.00001d);
            assertEquals("CEDF",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(386.63d,rs.getDouble("A"),0.00001d);
            assertEquals("CEDF",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(25.343d,rs.getDouble("A"),0.00001d);
            assertEquals("XYZWER",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(36.763d,rs.getDouble("A"),0.00001d);
            assertEquals("XYZWER",rs.getString("B"));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSelectUnnestDoubleAndVarcharArrayLimit5() throws Exception {
        String tenantId = getOrganizationId();
        String tableName = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(tableName, tenantId, null, false, getUrl());
        String query = "SELECT UNNEST(a_double_array) AS A, UNNEST(a_string_array) AS B FROM " + tableName + " LIMIT 5";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try(Connection conn = DriverManager.getConnection(getUrl(), props)){
            TestUtil.analyzeTable(conn, tableName);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(25.343d,rs.getDouble("A"),0.00001d);
            assertEquals("ABC",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(36.763d,rs.getDouble("A"),0.00001d);
            assertEquals("ABC",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(37.56d,rs.getDouble("A"),0.00001d);
            assertEquals("ABC",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(386.63d,rs.getDouble("A"),0.00001d);
            assertEquals("ABC",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(25.343d,rs.getDouble("A"),0.00001d);
            assertEquals("CEDF",rs.getString("B"));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSelectUnnestWithAnNullArrayShouldSkipNullArrayRows() throws Exception {
        String tenantId = getOrganizationId();
        String tableName = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(tableName, tenantId, null, false, getUrl());
        String query =
                "upsert into " + tableName +
                        "(" +
                        "    ORGANIZATION_ID, " +
                        "    ENTITY_ID, " +
                        "    a_string_array" +
                        ")" +
                        "VALUES ('TEST', '02342345', ARRAY['asdf','jkl'])";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try(Connection conn = DriverManager.getConnection(getUrl(), props)){
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.execute();
            conn.commit();

            query = "SELECT UNNEST(a_double_array) AS A, UNNEST(a_string_array)  AS B FROM " + tableName;
            ResultSet rs = stmt.executeQuery(query);
            assertTrue(rs.next());
            assertEquals(25.343d,rs.getDouble("A"),0.00001d);
            assertEquals("ABC",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(36.763d,rs.getDouble("A"),0.00001d);
            assertEquals("ABC",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(37.56d,rs.getDouble("A"),0.00001d);
            assertEquals("ABC",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(386.63d,rs.getDouble("A"),0.00001d);
            assertEquals("ABC",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(25.343d,rs.getDouble("A"),0.00001d);
            assertEquals("CEDF",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(36.763d,rs.getDouble("A"),0.00001d);
            assertEquals("CEDF",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(37.56d,rs.getDouble("A"),0.00001d);
            assertEquals("CEDF",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(386.63d,rs.getDouble("A"),0.00001d);
            assertEquals("CEDF",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(25.343d,rs.getDouble("A"),0.00001d);
            assertEquals("XYZWER",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(36.763d,rs.getDouble("A"),0.00001d);
            assertEquals("XYZWER",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(37.56d,rs.getDouble("A"),0.00001d);
            assertEquals("XYZWER",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(386.63d,rs.getDouble("A"),0.00001d);
            assertEquals("XYZWER",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(25.343d,rs.getDouble("A"),0.00001d);
            assertEquals("AB",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(36.763d,rs.getDouble("A"),0.00001d);
            assertEquals("AB",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(37.56d,rs.getDouble("A"),0.00001d);
            assertEquals("AB",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(386.63d,rs.getDouble("A"),0.00001d);
            assertEquals("AB",rs.getString("B"));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSelectUnnestDoubleAndVarcharArrayOrderBy() throws Exception {
        String tenantId = getOrganizationId();
        String tableName = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(tableName, tenantId, null, false, getUrl());
        String query = "SELECT UNNEST(a_double_array) AS A, UNNEST(a_string_array) AS B FROM " + tableName + " ORDER BY A,B";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try(Connection conn = DriverManager.getConnection(getUrl(), props)){
            TestUtil.analyzeTable(conn, tableName);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(25.343d,rs.getDouble("A"),0.00001d);
            assertEquals("AB",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(25.343d,rs.getDouble("A"),0.00001d);
            assertEquals("ABC",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(25.343d,rs.getDouble("A"),0.00001d);
            assertEquals("CEDF",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(25.343d,rs.getDouble("A"),0.00001d);
            assertEquals("XYZWER",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(36.763d,rs.getDouble("A"),0.00001d);
            assertEquals("AB",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(36.763d,rs.getDouble("A"),0.00001d);
            assertEquals("ABC",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(36.763d,rs.getDouble("A"),0.00001d);
            assertEquals("CEDF",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(36.763d,rs.getDouble("A"),0.00001d);
            assertEquals("XYZWER",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(37.56d,rs.getDouble("A"),0.00001d);
            assertEquals("AB",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(37.56d,rs.getDouble("A"),0.00001d);
            assertEquals("ABC",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(37.56d,rs.getDouble("A"),0.00001d);
            assertEquals("CEDF",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(37.56d,rs.getDouble("A"),0.00001d);
            assertEquals("XYZWER",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(386.63d,rs.getDouble("A"),0.00001d);
            assertEquals("AB",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(386.63d,rs.getDouble("A"),0.00001d);
            assertEquals("ABC",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(386.63d,rs.getDouble("A"),0.00001d);
            assertEquals("CEDF",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(386.63d,rs.getDouble("A"),0.00001d);
            assertEquals("XYZWER",rs.getString("B"));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSelectUnnestDoubleAndVarcharArrayWithNull() throws Exception {
        String tenantId = getOrganizationId();
        String tableName = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(tableName, tenantId, null, true, getUrl());
        String query = "SELECT UNNEST(a_double_array) AS A, UNNEST(a_string_array) AS B FROM " + tableName;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try(Connection conn = DriverManager.getConnection(getUrl(), props)){
            TestUtil.analyzeTable(conn, tableName);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(25.343d,rs.getDouble("A"),0.00001d);
            assertEquals("ABC",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(36.763d,rs.getDouble("A"),0.00001d);
            assertEquals("ABC",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(37.56d,rs.getDouble("A"),0.00001d);
            assertEquals("ABC",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(386.63d,rs.getDouble("A"),0.00001d);
            assertEquals("ABC",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(25.343d,rs.getDouble("A"),0.00001d);
            assertNull(rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(36.763d,rs.getDouble("A"),0.00001d);
            assertNull(rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(37.56d,rs.getDouble("A"),0.00001d);
            assertNull(rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(386.63d,rs.getDouble("A"),0.00001d);
            assertNull(rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(25.343d,rs.getDouble("A"),0.00001d);
            assertEquals("XYZWER",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(36.763d,rs.getDouble("A"),0.00001d);
            assertEquals("XYZWER",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(37.56d,rs.getDouble("A"),0.00001d);
            assertEquals("XYZWER",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(386.63d,rs.getDouble("A"),0.00001d);
            assertEquals("XYZWER",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(25.343d,rs.getDouble("A"),0.00001d);
            assertEquals("AB",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(36.763d,rs.getDouble("A"),0.00001d);
            assertEquals("AB",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(37.56d,rs.getDouble("A"),0.00001d);
            assertEquals("AB",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals(386.63d,rs.getDouble("A"),0.00001d);
            assertEquals("AB",rs.getString("B"));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSelectUnnestDoubleArrayFromTwoRows() throws Exception{
        String tenantId = getOrganizationId();
        String tableName = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(tableName, tenantId, null, false, getUrl());
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        addOneRow(tableName,props);
        String query = "SELECT UNNEST(a_double_array) AS A FROM " + tableName;

        try(Connection conn = DriverManager.getConnection(getUrl(), props)){
            TestUtil.analyzeTable(conn, tableName);
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(25.343d,rs.getDouble("A"),0.00001d);
            assertTrue(rs.next());
            assertEquals(36.763d,rs.getDouble("A"),0.00001d);
            assertTrue(rs.next());
            assertEquals(37.56d,rs.getDouble("A"),0.00001d);
            assertTrue(rs.next());
            assertEquals(386.63d,rs.getDouble("A"),0.00001d);
            assertTrue(rs.next());
            assertEquals(23.43d,rs.getDouble("A"),0.00001d);
            assertTrue(rs.next());
            assertEquals(99.23d,rs.getDouble("A"),0.00001d);
            assertTrue(rs.next());
            assertEquals(12d,rs.getDouble("A"),0.00001d);
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSelectUnnestWithWhere() throws Exception{
        String tenantId = getOrganizationId();
        String tableName = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(tableName, tenantId, null, false, getUrl());
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        addOneRow(tableName,props);
        String query = "SELECT UNNEST(a_double_array) AS A FROM " + tableName + " WHERE UNNEST(a_double_array)";

        try(Connection conn = DriverManager.getConnection(getUrl(), props)){
            TestUtil.analyzeTable(conn, tableName);
            PreparedStatement statement = conn.prepareStatement(query);

            try{
                ResultSet rs = statement.executeQuery();
            }catch(SQLException e){
                assertEquals("Unnest array function not supported here",e.getMessage());
            }
        }
    }

    @Test
    public void testSelectUnnestVarcharArray() throws Exception{
        String tenantId = getOrganizationId();
        String tableName = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(tableName, tenantId, null, false, getUrl());
        String query = "SELECT UNNEST(a_string_array) AS A FROM " + tableName;
        //String query = "SELECT a_double_array[1] AS A FROM "+ tableName;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try(Connection conn = DriverManager.getConnection(getUrl(), props)){
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("ABC",rs.getString("A"));
            assertTrue(rs.next());
            assertEquals("CEDF",rs.getString("A"));
            assertTrue(rs.next());
            assertEquals("XYZWER",rs.getString("A"));
            assertTrue(rs.next());
            assertEquals("AB",rs.getString("A"));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSelectUnnestShouldReturnNullValueInsideAnArray() throws Exception{
        String tenantId = getOrganizationId();
        String tableName = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(tableName, tenantId, null, true, getUrl());
        String query = "SELECT UNNEST(a_string_array) AS A FROM " + tableName;
        //String query = "SELECT a_double_array[1] AS A FROM "+ tableName;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try(Connection conn = DriverManager.getConnection(getUrl(), props)){
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("ABC",rs.getString("A"));
            assertTrue(rs.next());
            assertNull(rs.getString("A"));
            assertTrue(rs.next());
            assertEquals("XYZWER",rs.getString("A"));
            assertTrue(rs.next());
            assertEquals("AB",rs.getString("A"));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSelectUnnestBigIntArray() throws Exception{
        String tenantId = getOrganizationId();
        String tableName = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(tableName, tenantId, null, false, getUrl());
        String query = "SELECT UNNEST(x_long_array) AS A FROM " + tableName;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try(Connection conn = DriverManager.getConnection(getUrl(), props)){
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(25l,rs.getLong("A"));
            assertTrue(rs.next());
            assertEquals(36l,rs.getLong("A"));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSelectUnnestBigIntArrayPLus10() throws Exception{
        String tenantId = getOrganizationId();
        String tableName = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(tableName, tenantId, null, false, getUrl());
        String query = "SELECT UNNEST(x_long_array)+10 AS A FROM " + tableName;
        //String query = "SELECT a_double_array[1] AS A FROM "+ tableName;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        //TestUtil.analyzeTable(conn, tableName);
        try(Connection conn = DriverManager.getConnection(getUrl(), props)){
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(35l,rs.getLong("A"));
            assertTrue(rs.next());
            assertEquals(46l,rs.getLong("A"));
            assertFalse(rs.next());
        }
    }


    @Test
    public void testSelectUnnestByteArray() throws Exception{
        String tenantId = getOrganizationId();
        String tableName = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(tableName, tenantId, null, false, getUrl());
        String query = "SELECT UNNEST(a_byte_array) AS A FROM " + tableName;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try(Connection conn = DriverManager.getConnection(getUrl(), props)){
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            byte b = 25;
            assertEquals(b,rs.getByte("A"));
            b = 36;
            assertTrue(rs.next());
            assertEquals(b,rs.getByte("A"));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSelectSumUnnest() throws Exception{
        String tenantId = getOrganizationId();
        String tableName = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(tableName, tenantId, null, false, getUrl());
        String query = "SELECT SUM(UNNEST(a_double_array)) AS A FROM " + tableName;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try(Connection conn = DriverManager.getConnection(getUrl(), props)){
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(486.296d,rs.getDouble("A"),0.0001d);
            assertFalse(rs.next());
        }
    }

    @Test
    public void testCountUnnest() throws Exception{
        String tenantId = getOrganizationId();
        String tableName = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(tableName, tenantId, null, false, getUrl());
        String query = "SELECT COUNT(UNNEST(a_double_array)) AS A FROM " + tableName;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try(Connection conn = DriverManager.getConnection(getUrl(), props)){
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(4,rs.getInt("A"));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testUnnestArrayInPK() throws Exception {
        String tenantId = getOrganizationId();
        String tableName = generateUniqueName();
        String query = "CREATE TABLE " + tableName + "(\n"
                + "pk_int INTEGER NOT NULL,\n"
                + "pk_int_array INTEGER ARRAY NOT NULL,\n"
                + "a_integer INTEGER,\n"
                + "CONSTRAINT pk PRIMARY KEY (pk_int,pk_int_array))";
        BaseTest.createTestTable(url, query, getDefaultSplits(tenantId), null);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try(Connection conn = DriverManager.getConnection(getUrl(),props)){
            query = "UPSERT INTO " + tableName
            + "(pk_int,"
            + "pk_int_array,"
            + "a_integer) "
            + "VALUES(1,ARRAY[1,2,3,4],5)";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.execute();
            conn.commit();
            query = "SELECT pk_int AS PK, UNNEST(pk_int_array) AS A FROM " + tableName;
            stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(1,rs.getInt("PK"));
            assertEquals(1,rs.getInt("A"));
            assertTrue(rs.next());
            assertEquals(1,rs.getInt("PK"));
            assertEquals(2,rs.getInt("A"));
            assertTrue(rs.next());
            assertEquals(1,rs.getInt("PK"));
            assertEquals(3,rs.getInt("A"));
            assertTrue(rs.next());
            assertEquals(1,rs.getInt("PK"));
            assertEquals(4,rs.getInt("A"));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSelectUnnestVarcharArrayUnionAll() throws Exception {
        String tenantId = getOrganizationId();
        String tableName1 = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(tableName1, tenantId, null, false, getUrl());
        String tableName2 = generateUniqueName();
        String query = "CREATE TABLE " + tableName2 +" ("
                + "    PK VARCHAR(100) PRIMARY KEY, "
                + "    b_string_array VARCHAR(100) ARRAY)";
        BaseTest.createTestTable(url, query, getDefaultSplits(tenantId), null);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try(Connection conn = DriverManager.getConnection(getUrl(), props)){
            query = "UPSERT INTO " + tableName2
                    + "(PK,b_string_array) "
                    + " VALUES('TEST', ARRAY['YYY','ZZZ'])";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.execute();
            conn.commit();
            query = "SELECT UNNEST(a_string_array) AS A FROM " + tableName1 +
                    "    UNION ALL SELECT UNNEST(b_string_array) AS A FROM " + tableName2;
            stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("ABC", rs.getString("A"));
            assertTrue(rs.next());
            assertEquals("CEDF", rs.getString("A"));
            assertTrue(rs.next());
            assertEquals("XYZWER", rs.getString("A"));
            assertTrue(rs.next());
            assertEquals("AB", rs.getString("A"));
            assertTrue(rs.next());
            assertEquals("YYY", rs.getString("A"));
            assertTrue(rs.next());
            assertEquals("ZZZ", rs.getString("A"));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testUpsertSelectUnnest() throws Exception{
        String tenantId = getOrganizationId();
        String tableName = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(tableName, tenantId, null, false, getUrl());
        String query = "UPSERT INTO " + tableName + "(organization_id, entity_id)"
                + " SELECT UNNEST(a_string_array), UNNEST(a_string_array) FROM " + tableName;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try(Connection conn = DriverManager.getConnection(getUrl(), props)){
            PreparedStatement statement = conn.prepareStatement(query);
            statement.execute();
            conn.commit();
            query = "SELECT organization_id AS A, entity_id AS B FROM " + tableName;
            statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(tenantId,rs.getString("A"));
            assertEquals(ROW1,rs.getString("B"));
            assertTrue(rs.next());
            assertEquals("AB",rs.getString("A"));
            assertEquals("AB",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals("ABC",rs.getString("A"));
            assertEquals("ABC",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals("CEDF",rs.getString("A"));
            assertEquals("CEDF",rs.getString("B"));
            assertTrue(rs.next());
            assertEquals("XYZWER",rs.getString("A"));
            assertEquals("XYZWER",rs.getString("B"));
            assertFalse(rs.next());
        }
    }


    private void addOneRow(String tableName,Properties props) throws Exception {
        String query =
                "upsert into " + tableName +
                        "(" +
                        "    ORGANIZATION_ID, " +
                        "    ENTITY_ID, " +
                        "    a_string_array, " +
                        "    B_STRING, " +
                        "    A_INTEGER, " +
                        "    A_DATE, " +
                        "    X_DECIMAL, " +
                        "    x_long_array, " +
                        "    X_INTEGER," +
                        "    a_byte_array," +
                        "    A_SHORT," +
                        "    A_FLOAT," +
                        "    a_double_array," +
                        "    A_UNSIGNED_FLOAT," +
                        "    A_UNSIGNED_DOUBLE)" +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try(Connection conn = DriverManager.getConnection(getUrl(), props)){
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1,"NIC1234");
            statement.setString(2,"QWERT1234");
            String[] strArr = {"xyz","jkl","prp"};
            Array array = conn.createArrayOf("VARCHAR",strArr);
            statement.setArray(3,array);
            statement.setString(4,"ptr");
            statement.setInt(5,8);
            statement.setDate(6,new Date(12341234));
            statement.setBigDecimal(7,new BigDecimal(7l));
            Long[] longArr = {10l,45l,20l,54l,39l};
            array = conn.createArrayOf("BIGINT", longArr);
            statement.setArray(8,array);
            statement.setInt(9,84);
            Byte[] byteArr = {new Byte("98"),new Byte("127")};
            array = conn.createArrayOf("TINYINT",byteArr);
            statement.setArray(10,array);
            statement.setShort(11, (short) 82);
            statement.setFloat(12, 0.123f);
            Double[] doubleArr =  {new Double(23.43d),new Double(99.23d),new Double(12d)};
            array = conn.createArrayOf("DOUBLE", doubleArr);
            statement.setArray(13,array);
            statement.setFloat(14, 0.234f);
            statement.setDouble(15, 0.03821);
            statement.execute();
            conn.commit();
        }

    }
}
