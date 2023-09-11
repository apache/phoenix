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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Properties;

import org.apache.phoenix.schema.types.PhoenixArray;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class Array2IT extends ArrayIT {

    private static final String TEST_QUERY = "select ?[2] from \"SYSTEM\".\"CATALOG\" limit 1";

    @Test
    public void testFixedWidthCharArray() throws Exception {
        Connection conn;
        PreparedStatement stmt;
        ResultSet rs;


        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        conn.createStatement().execute("CREATE TABLE  " + table + "  ( k VARCHAR PRIMARY KEY, a CHAR(5) ARRAY)");
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        rs = conn.getMetaData().getColumns(null, null, table, "A");
        assertTrue(rs.next());
        assertEquals(5, rs.getInt("COLUMN_SIZE"));
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("UPSERT INTO  " + table + "  VALUES(?,?)");
        stmt.setString(1, "a");
        String[] s = new String[] {"1","2"};
        Array array = conn.createArrayOf("CHAR", s);
        stmt.setArray(2, array);
        stmt.execute();
        conn.commit();
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        rs = conn.createStatement().executeQuery("SELECT k, a[2] FROM  " + table);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("2",rs.getString(2));
        conn.close();
    }

    @Test
    public void testSelectArrayUsingUpsertLikeSyntax() throws Exception {
        String tenantId = getOrganizationId();
        String table = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(table, tenantId, null, false, getUrl());
        String query = "SELECT a_double_array FROM  " + table + "  WHERE a_double_array = CAST(ARRAY [ 25.343, 36.763, 37.56,386.63] AS DOUBLE ARRAY)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            Double[] doubleArr =  new Double[4];
            doubleArr[0] = 25.343;
            doubleArr[1] = 36.763;
            doubleArr[2] = 37.56;
            doubleArr[3] = 386.63;
            Array array = conn.createArrayOf("DOUBLE", doubleArr);
            PhoenixArray resultArray = (PhoenixArray) rs.getArray(1);
            assertEquals(resultArray, array);
            assertEquals("[25.343, 36.763, 37.56, 386.63]", rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testArrayIndexUsedInWhereClause() throws Exception {
        String tenantId = getOrganizationId();
        String table = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(table, tenantId, null, false, getUrl());
        int a_index = 0;
        String query = "SELECT a_double_array[2] FROM  " + table + "  where a_double_array["+a_index+"2]<?";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            Double[] doubleArr = new Double[1];
            doubleArr[0] = 40.0;
            conn.createArrayOf("DOUBLE", doubleArr);
            statement.setDouble(1, 40.0d);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            // Need to support primitive
            doubleArr = new Double[1];
            doubleArr[0] = 36.763;
            Double result =  rs.getDouble(1);
            assertEquals(doubleArr[0], result);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testArrayIndexUsedInGroupByClause() throws Exception {
        String tenantId = getOrganizationId();
        String table = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(table, tenantId, null, false, getUrl());
        String query = "SELECT a_double_array[2] FROM " + table + "  GROUP BY a_double_array[2]";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            Double[] doubleArr = new Double[1];
            doubleArr[0] = 40.0;
            conn.createArrayOf("DOUBLE", doubleArr);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            doubleArr = new Double[1];
            doubleArr[0] = 36.763;
            Double result =  rs.getDouble(1);
            assertEquals(doubleArr[0], result);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testVariableLengthArrayWithNullValue() throws Exception {
        String tenantId = getOrganizationId();
        String table = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(table, tenantId, null, true, getUrl());
        String query = "SELECT a_string_array[2] FROM  " + table;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            String[] strArr = new String[1];
            strArr[0] = "XYZWER";
            String result = rs.getString(1);
            assertNull(result);
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectSpecificIndexOfAVariableArrayAlongWithAnotherColumn1() throws Exception {
        String tenantId = getOrganizationId();
        String table = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(table, tenantId, null, false, getUrl());
        String query = "SELECT a_string_array[3],A_INTEGER FROM  " + table;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            String[] strArr = new String[1];
            strArr[0] = "XYZWER";
            String result = rs.getString(1);
            assertEquals(strArr[0], result);
            int a_integer = rs.getInt(2);
            assertEquals(1, a_integer);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectSpecificIndexOfAVariableArrayAlongWithAnotherColumn2() throws Exception {

        String tenantId = getOrganizationId();
        String table = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(table, tenantId, null, false, getUrl());
        String query = "SELECT A_INTEGER, a_string_array[3] FROM  " + table;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            String[] strArr = new String[1];
            strArr[0] = "XYZWER";
            int a_integer = rs.getInt(1);
            assertEquals(1, a_integer);
            String result = rs.getString(2);
            assertEquals(strArr[0], result);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectMultipleArrayColumns() throws Exception {

        String tenantId = getOrganizationId();
        String table = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(table, tenantId, null, false, getUrl());
        String query = "SELECT  a_string_array[3], a_double_array[2] FROM  " + table;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            String[] strArr = new String[1];
            strArr[0] = "XYZWER";
            Double[] doubleArr = new Double[1];
            doubleArr[0] = 36.763d;
            Double a_double = rs.getDouble(2);
            assertEquals(doubleArr[0], a_double);
            String result = rs.getString(1);
            assertEquals(strArr[0], result);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectSameArrayColumnMultipleTimesWithDifferentIndices() throws Exception {

        String tenantId = getOrganizationId();
        String table = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(table, tenantId, null, false, getUrl());
        String query = "SELECT a_string_array[1], a_string_array[2], " +
                "a_string_array[3], a_double_array[1], a_double_array[2], a_double_array[3] " +
                "FROM  " + table;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("ABC", rs.getString(1));
            assertEquals("CEDF", rs.getString(2));
            assertEquals("XYZWER", rs.getString(3));
            assertEquals(25.343, rs.getDouble(4), 0.0);
            assertEquals(36.763, rs.getDouble(5), 0.0);
            assertEquals(37.56, rs.getDouble(6), 0.0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectSameArrayColumnMultipleTimesWithSameIndices() throws Exception {

        String tenantId = getOrganizationId();
        String table = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(table, tenantId, null, false, getUrl());
        String query = "SELECT a_string_array[3], a_string_array[3] FROM " + table;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            String[] strArr = new String[1];
            strArr[0] = "XYZWER";
            String result = rs.getString(1);
            assertEquals(strArr[0], result);
            result = rs.getString(2);
            assertEquals(strArr[0], result);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectSpecificIndexOfAVariableArray() throws Exception {
        String tenantId = getOrganizationId();
        String table = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(table, tenantId, null, false, getUrl());
        String query = "SELECT a_string_array[3] FROM  " + table;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            String[] strArr = new String[1];
            strArr[0] = "XYZWER";
            String result = rs.getString(1);
            assertEquals(strArr[0], result);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testWithOutOfRangeIndex() throws Exception {
        String tenantId = getOrganizationId();
        String table = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(table, tenantId, null, false, getUrl());
        String query = "SELECT a_double_array[100] FROM  " + table;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            PhoenixArray resultArray = (PhoenixArray) rs.getArray(1);
            assertNull(resultArray);
        } finally {
            conn.close();
        }
    }

    @Test
    public void testArrayLengthFunctionForVariableLength() throws Exception {
        String tenantId = getOrganizationId();
        String table = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(table, tenantId, null, false, getUrl());
        String query = "SELECT ARRAY_LENGTH(a_string_array) FROM " + table;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            int result = rs.getInt(1);
            assertEquals(result, 4);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }


    @Test
    public void testArrayLengthFunctionForFixedLength() throws Exception {
        String tenantId = getOrganizationId();
        String table = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(table, tenantId, null, false, getUrl());
        String query = "SELECT ARRAY_LENGTH(a_double_array) FROM " + table;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            int result = rs.getInt(1);
            assertEquals(result, 4);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testArraySizeRoundtrip() throws Exception {

        String tenantId = getOrganizationId();
        String table = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            ResultSet rs = conn.getMetaData().getColumns(null, null, StringUtil.escapeLike(table), StringUtil.escapeLike(SchemaUtil.normalizeIdentifier("x_long_array")));
            assertTrue(rs.next());
            assertEquals(5, rs.getInt("ARRAY_SIZE"));
            assertFalse(rs.next());

            rs = conn.getMetaData().getColumns(null, null, StringUtil.escapeLike(table), StringUtil.escapeLike(SchemaUtil.normalizeIdentifier("a_string_array")));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt("ARRAY_SIZE"));
            assertFalse(rs.next());

            rs = conn.getMetaData().getColumns(null, null, StringUtil.escapeLike(table), StringUtil.escapeLike(SchemaUtil.normalizeIdentifier("a_double_array")));
            assertTrue(rs.next());
            assertEquals(0, rs.getInt("ARRAY_SIZE"));
            assertTrue(rs.wasNull());
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testVarLengthArrComparisonInWhereClauseWithSameArrays() throws Exception {
        Connection conn;
        PreparedStatement stmt;
        ResultSet rs;


        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        conn.createStatement()
                .execute(
                        "CREATE TABLE  " + table + "  ( k VARCHAR PRIMARY KEY, a_string_array VARCHAR(100) ARRAY[4], b_string_array VARCHAR(100) ARRAY[4])");
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("UPSERT INTO  " + table + "  VALUES(?,?,?)");
        stmt.setString(1, "a");
        String[] s = new String[] {"abc","def", "ghi","jkl"};
        Array array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(2, array);
        s = new String[] {"abc","def", "ghi","jkl"};
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(3, array);
        stmt.execute();
        conn.commit();
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        rs = conn.createStatement().executeQuery("SELECT k, a_string_array[2] FROM  " + table + "  where a_string_array=b_string_array");
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("def",rs.getString(2));
        conn.close();
    }

    @Test
    public void testVarLengthArrComparisonInWhereClauseWithDiffSizeArrays() throws Exception {
        Connection conn;
        PreparedStatement stmt;
        ResultSet rs;


        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        conn.createStatement()
                .execute(
                        "CREATE TABLE  " + table + "  ( k VARCHAR PRIMARY KEY, a_string_array VARCHAR(100) ARRAY[4], b_string_array VARCHAR(100) ARRAY[4])");
        conn.close();
        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("UPSERT INTO  " + table + "  VALUES(?,?,?)");
        stmt.setString(1, "a");
        String[] s = new String[] { "abc", "def", "ghi", "jkll" };
        Array array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(2, array);
        s = new String[] { "abc", "def", "ghi", "jklm" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(3, array);
        stmt.execute();
        conn.commit();
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        rs = conn.createStatement().executeQuery(
                "SELECT k, a_string_array[2] FROM  " + table + "  where a_string_array<b_string_array");
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertEquals("def", rs.getString(2));
        conn.close();
    }

    @Test
    public void testVarLengthArrComparisonWithNulls() throws Exception {
        Connection conn;
        PreparedStatement stmt;
        ResultSet rs;


        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        conn.createStatement()
                .execute(
                        "CREATE TABLE  " + table + "  ( k VARCHAR PRIMARY KEY, a_string_array VARCHAR(100) ARRAY[4], b_string_array VARCHAR(100) ARRAY[4])");
        conn.close();
        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("UPSERT INTO  " + table + "  VALUES(?,?,?)");
        stmt.setString(1, "a");
        String[] s = new String[] { "abc", "def", "ghi", "jkll", null, null, "xxx" };
        Array array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(2, array);
        s = new String[] { "abc", "def", "ghi", "jkll", null, null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(3, array);
        stmt.execute();
        conn.commit();
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        rs = conn.createStatement().executeQuery(
                "SELECT k, a_string_array[2] FROM  " + table + "  where a_string_array>b_string_array");
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertEquals("def", rs.getString(2));
        conn.close();
    }

    @Test
    public void testUpsertValuesWithNull() throws Exception {

        String tenantId = getOrganizationId();
        String table = createTableWithArray(getUrl(), getDefaultSplits(tenantId), null);
        String query = "upsert into  " + table + " (ORGANIZATION_ID,ENTITY_ID,a_double_array) values('" + tenantId
                + "','00A123122312312',null)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
                                                                                 // at
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            int executeUpdate = statement.executeUpdate();
            assertEquals(1, executeUpdate);
            conn.commit();
            statement.close();
            conn.close();
            // create another connection
            props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            conn = DriverManager.getConnection(getUrl(), props);
            query = "SELECT ARRAY_ELEM(a_double_array,2) FROM  " + table;
            statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            // Need to support primitive
            Double[] doubleArr = new Double[1];
            doubleArr[0] = 0.0d;
            conn.createArrayOf("DOUBLE", doubleArr);
            Double result = rs.getDouble(1);
            assertEquals(doubleArr[0], result);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testUpsertValuesWithNullUsingPreparedStmt() throws Exception {

        String tenantId = getOrganizationId();
        String table = createTableWithArray(getUrl(), getDefaultSplits(tenantId), null);
        String query = "upsert into  " + table + " (ORGANIZATION_ID,ENTITY_ID,a_string_array) values(?, ?, ?)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
                                                                                 // at
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, "00A123122312312");
            statement.setNull(3, Types.ARRAY);
            int executeUpdate = statement.executeUpdate();
            assertEquals(1, executeUpdate);
            conn.commit();
            statement.close();
            conn.close();
            // create another connection
            props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            conn = DriverManager.getConnection(getUrl(), props);
            query = "SELECT ARRAY_ELEM(a_string_array,1) FROM  " + table;
            statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            String[] strArr = new String[1];
            strArr[0] = null;
            conn.createArrayOf("VARCHAR", strArr);
            String result = rs.getString(1);
            assertEquals(strArr[0], result);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testPKWithArray() throws Exception {
        Connection conn;
        PreparedStatement stmt;
        ResultSet rs;

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        conn.createStatement()
                .execute(
                        "CREATE TABLE  " + table + "  ( k VARCHAR, a_string_array VARCHAR(100) ARRAY[4], b_string_array VARCHAR(100) ARRAY[4] \n"
                        + " CONSTRAINT pk PRIMARY KEY (k, b_string_array)) \n");
        conn.close();
        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("UPSERT INTO  " + table + "  VALUES(?,?,?)");
        stmt.setString(1, "a");
        String[] s = new String[] { "abc", "def", "ghi", "jkll", null, null, "xxx" };
        Array array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(2, array);
        s = new String[] { "abc", "def", "ghi", "jkll", null, null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(3, array);
        stmt.execute();
        conn.commit();
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        rs = conn.createStatement().executeQuery(
                "SELECT k, a_string_array[2] FROM  " + table + "  where b_string_array[8]='xxx'");
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertEquals("def", rs.getString(2));
        conn.close();
    }

    @Test
    public void testPKWithArrayNotInEnd() throws Exception {
        Connection conn;

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        try {
            conn.createStatement().execute(
                    "CREATE TABLE  " + table + "  ( a_string_array VARCHAR(100) ARRAY[4], b_string_array VARCHAR(100) ARRAY[4], k VARCHAR  \n"
                            + " CONSTRAINT pk PRIMARY KEY (b_string_array, k))");
            conn.close();
            fail();
        } catch (SQLException e) {
        } finally {
            if (conn != null) {
                conn.close();
            }
        }

    }

    @Test
    public void testArrayRefToLiteralCharArraySameLengths() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            PreparedStatement stmt = conn.prepareStatement(TEST_QUERY);
            // Test with each element of the char array having same lengths
            Array array = conn.createArrayOf("CHAR", new String[] {"a","b","c"});
            stmt.setArray(1, array);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testArrayRefToLiteralCharArrayDiffLengths() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            PreparedStatement stmt = conn.prepareStatement(TEST_QUERY);
            // Test with each element of the char array having different lengths
            Array array = conn.createArrayOf("CHAR", new String[] {"a","bb","ccc"});
            stmt.setArray(1, array);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("bb", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testArrayRefToLiteralBinaryArray() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            PreparedStatement stmt = conn.prepareStatement(TEST_QUERY);
            // Test with each element of the binary array having different lengths
            byte[][] bytes = {{0,0,1}, {0,0,2,0}, {0,0,0,3,4}};
            Array array = conn.createArrayOf("BINARY", bytes);
            stmt.setArray(1, array);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            // Note that all elements are padded to be of the same length
            // as the longest element of the byte array
            assertArrayEquals(new byte[] {0,0,2,0,0}, rs.getBytes(1));
            assertFalse(rs.next());
        }
    }
    
    @Test
    public void testArrayConstructorWithMultipleRows1() throws Exception {

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        String ddl = "CREATE TABLE  " + table + "  (region_name VARCHAR PRIMARY KEY, a INTEGER, b INTEGER)";
        conn.createStatement().execute(ddl);
        conn.commit();
        conn.close();
        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO  " + table + " (region_name, a, b) VALUES('a', 6,3)");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO  " + table + " (region_name, a, b) VALUES('b', 2,4)");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO  " + table + " (region_name, a, b) VALUES('c', 6,3)");
        stmt.execute();
        conn.commit();
        conn.close();
        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT COUNT(DISTINCT ARRAY[a,b]) from  " + table);
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
    }

    @Test
    public void testArrayConstructorWithMultipleRows2() throws Exception {

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        String ddl = "CREATE TABLE  " + table + "  (region_name VARCHAR PRIMARY KEY, a INTEGER, b INTEGER)";
        conn.createStatement().execute(ddl);
        conn.commit();
        conn.close();
        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO  " + table + " (region_name, a, b) VALUES('a', 6,3)");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO  " + table + " (region_name, a, b) VALUES('b', 2,4)");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO  " + table + " (region_name, a, b) VALUES('c', 6,3)");
        stmt.execute();
        conn.commit();
        conn.close();
        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY[a,b] from  " + table + " ");
        assertTrue(rs.next());
        Array arr = conn.createArrayOf("INTEGER", new Object[]{6, 3});
        assertEquals(arr, rs.getArray(1));
        rs.next();
        arr = conn.createArrayOf("INTEGER", new Object[]{2, 4});
        assertEquals(arr, rs.getArray(1));
        rs.next();
        arr = conn.createArrayOf("INTEGER", new Object[]{6, 3});
        assertEquals(arr, rs.getArray(1));
        rs.next();
    }

    @Test
    public void testArrayConstructorWithMultipleRows3() throws Exception {

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        String ddl = "CREATE TABLE  " + table + "  (region_name VARCHAR PRIMARY KEY, a VARCHAR, b VARCHAR)";
        conn.createStatement().execute(ddl);
        conn.commit();
        conn.close();
        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO  " + table + " (region_name, a, b) VALUES('a', 'foo', 'abc')");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO  " + table + " (region_name, a, b) VALUES('b', 'abc', 'dfg')");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO  " + table + " (region_name, a, b) VALUES('c', 'foo', 'abc')");
        stmt.execute();
        conn.commit();
        conn.close();
        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY[a,b] from  " + table + " ");
        assertTrue(rs.next());
        Array arr = conn.createArrayOf("VARCHAR", new Object[]{"foo", "abc"});
        assertEquals(arr, rs.getArray(1));
        rs.next();
        arr = conn.createArrayOf("VARCHAR", new Object[]{"abc", "dfg"});
        assertEquals(arr, rs.getArray(1));
        rs.next();
        arr = conn.createArrayOf("VARCHAR", new Object[]{"foo", "abc"});
        assertEquals(arr, rs.getArray(1));
        rs.next();
    }

    @Test
    public void testArrayConstructorWithMultipleRows4() throws Exception {

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        String ddl = "CREATE TABLE  " + table + "  (region_name VARCHAR PRIMARY KEY, a VARCHAR, b VARCHAR)";
        conn.createStatement().execute(ddl);
        conn.commit();
        conn.close();
        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO  " + table + " (region_name, a, b) VALUES('a', 'foo', 'abc')");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO  " + table + " (region_name, a, b) VALUES('b', 'abc', 'dfg')");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO  " + table + " (region_name, a, b) VALUES('c', 'foo', 'abc')");
        stmt.execute();
        conn.commit();
        conn.close();
        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT COUNT(DISTINCT ARRAY[a,b]) from  " + table);
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
    }

}
