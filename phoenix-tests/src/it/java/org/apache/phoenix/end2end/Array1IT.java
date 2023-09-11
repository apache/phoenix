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

import static org.apache.phoenix.util.TestUtil.B_VALUE;
import static org.apache.phoenix.util.TestUtil.ROW1;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
import org.apache.phoenix.schema.types.PhoenixArray;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.phoenix.thirdparty.com.google.common.primitives.Floats;

@Category(ParallelStatsDisabledTest.class)
public class Array1IT extends ArrayIT {
    private void assertArrayGetString(ResultSet rs, int arrayIndex, Array expectedArray, String expectedString)
            throws SQLException {
        assertEquals(expectedArray, rs.getArray(arrayIndex));
        assertEquals("[" + expectedString + "]", rs.getString(arrayIndex));
    }
    
    private static String createTableWithAllArrayTypes(String url, byte[][] bs, Object object) throws SQLException {
        String tableName = generateUniqueName();
        String ddlStmt = "create table "
                + tableName
                + "   (organization_id char(15) not null, \n"
                + "    entity_id char(15) not null,\n"
                + "    boolean_array boolean array,\n"
                + "    byte_array tinyint array,\n"
                + "    double_array double array[],\n"
                + "    float_array float array,\n"
                + "    int_array integer array,\n"
                + "    long_array bigint[5],\n"
                + "    short_array smallint array,\n"
                + "    string_array varchar(100) array[3],\n"
                + "    CONSTRAINT pk PRIMARY KEY (organization_id, entity_id)\n"
                + ")";
        BaseTest.createTestTable(url, ddlStmt, bs, null);
        return tableName;
    }
    
    private static String createSimpleTableWithArray(String url, byte[][] bs, Object object) throws SQLException {
        String tableName = generateUniqueName();
        String ddlStmt = "create table "
                + tableName
                + "   (organization_id char(15) not null, \n"
                + "    entity_id char(15) not null,\n"
                + "    x_double double,\n"
                + "    a_double_array double array[],\n"
                + "    a_char_array char(5) array[],\n"
                + "    CONSTRAINT pk PRIMARY KEY (organization_id, entity_id)\n"
                + ")";
        BaseTest.createTestTable(url, ddlStmt, bs, null);
        return tableName;
    }

    private static void initSimpleArrayTable(String tableName, String tenantId, Date date, boolean useNull) throws Exception {
       Properties props = new Properties();

        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            // Insert all rows at ts
            PreparedStatement stmt = conn.prepareStatement(
                    "upsert into " + tableName +
                    "(" +
                    "    ORGANIZATION_ID, " +
                    "    ENTITY_ID, " +
                    "    x_double, " +
                    "    a_double_array, a_char_array)" +
                    "VALUES (?, ?, ?, ?, ?)");
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW1);
            stmt.setDouble(3, 1.2d);
            // Need to support primitive
            Double[] doubleArr =  new Double[2];
            doubleArr[0] = 64.87;
            doubleArr[1] = 89.96;
            //doubleArr[2] = 9.9;
            Array array = conn.createArrayOf("DOUBLE", doubleArr);
            stmt.setArray(4, array);

            // create character array
            String[] charArr =  new String[2];
            charArr[0] = "a";
            charArr[1] = "b";
            array = conn.createArrayOf("CHAR", charArr);
            stmt.setArray(5, array);
            stmt.execute();

            conn.commit();
        } finally {
            conn.close();
        }
   }

    @Test
    public void testScanByArrayValue() throws Exception {
        String tenantId = getOrganizationId();
        String tableName = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(tableName, tenantId, null, false, getUrl());
        String query = "SELECT a_double_array, /* comment ok? */ b_string, a_float FROM " + tableName + " WHERE ?=organization_id and ?=a_float";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        TestUtil.analyzeTable(conn, tableName);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setFloat(2, 0.01f);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            // Need to support primitive
            Double[] doubleArr = new Double[4];
            doubleArr[0] = 25.343;
            doubleArr[1] = 36.763;
            doubleArr[2] = 37.56;
            doubleArr[3] = 386.63;
            Array array = conn.createArrayOf("DOUBLE",
                    doubleArr);
            PhoenixArray resultArray = (PhoenixArray) rs.getArray(1);
            assertEquals(resultArray, array);
            assertEquals("[25.343, 36.763, 37.56, 386.63]", rs.getString(1));
            assertEquals(rs.getString("B_string"), B_VALUE);
            assertTrue(Floats.compare(rs.getFloat(3), 0.01f) == 0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testScanWithArrayInWhereClause() throws Exception {
        String tenantId = getOrganizationId();
        String tableName = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(tableName, tenantId, null, false, getUrl());
        String query = "SELECT a_double_array, /* comment ok? */ b_string, a_float FROM " + tableName + " WHERE ?=organization_id and ?=a_byte_array";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        TestUtil.analyzeTable(conn, tableName);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            // Need to support primitive
            Byte[] byteArr = new Byte[2];
            byteArr[0] = 25;
            byteArr[1] = 36;
            Array array = conn.createArrayOf("TINYINT", byteArr);
            statement.setArray(2, array);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            // Need to support primitive
            Double[] doubleArr = new Double[4];
            doubleArr[0] = 25.343;
            doubleArr[1] = 36.763;
            doubleArr[2] = 37.56;
            doubleArr[3] = 386.63;
            array = conn.createArrayOf("DOUBLE", doubleArr);
            Array resultArray = rs.getArray(1);
            assertEquals(resultArray, array);
            assertEquals("[25.343, 36.763, 37.56, 386.63]", rs.getString(1));
            assertEquals(rs.getString("B_string"), B_VALUE);
            assertTrue(Floats.compare(rs.getFloat(3), 0.01f) == 0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testScanWithNonFixedWidthArrayInWhereClause() throws Exception {
        String tenantId = getOrganizationId();
        String tableName = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(tableName, tenantId, null, false, getUrl());
        String query = "SELECT a_double_array, /* comment ok? */ b_string, a_float FROM  " + tableName + "  WHERE ?=organization_id and ?=a_string_array";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            // Need to support primitive
            String[] strArr = new String[4];
            strArr[0] = "ABC";
            strArr[1] = "CEDF";
            strArr[2] = "XYZWER";
            strArr[3] = "AB";
            Array array = conn.createArrayOf("VARCHAR", strArr);
            statement.setArray(2, array);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            // Need to support primitive
            Double[] doubleArr = new Double[4];
            doubleArr[0] = 25.343;
            doubleArr[1] = 36.763;
            doubleArr[2] = 37.56;
            doubleArr[3] = 386.63;
            array = conn.createArrayOf("DOUBLE", doubleArr);
            Array resultArray = rs.getArray(1);
            assertEquals(resultArray, array);
            assertEquals("[25.343, 36.763, 37.56, 386.63]", rs.getString(1));
            assertEquals(rs.getString("B_string"), B_VALUE);
            assertTrue(Floats.compare(rs.getFloat(3), 0.01f) == 0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testScanWithNonFixedWidthArrayInSelectClause() throws Exception {
        String tenantId = getOrganizationId();
        String tableName = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(tableName, tenantId, null, false, getUrl());
        String query = "SELECT a_string_array FROM  " + tableName;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            String[] strArr = new String[4];
            strArr[0] = "ABC";
            strArr[1] = "CEDF";
            strArr[2] = "XYZWER";
            strArr[3] = "AB";
            Array array = conn.createArrayOf("VARCHAR", strArr);
            PhoenixArray resultArray = (PhoenixArray) rs.getArray(1);
            assertEquals(resultArray, array);
            assertEquals("['ABC', 'CEDF', 'XYZWER', 'AB']", rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectSpecificIndexOfAnArrayAsArrayFunction()
            throws Exception {
        String tenantId = getOrganizationId();
        String tableName = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(tableName, tenantId, null, false, getUrl());
        String query = "SELECT ARRAY_ELEM(a_double_array,2) FROM  " + tableName;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            // Need to support primitive
            Double[] doubleArr = new Double[1];
            doubleArr[0] = 36.763;
            conn.createArrayOf("DOUBLE", doubleArr);
            Double result =  rs.getDouble(1);
            assertEquals(doubleArr[0], result);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectSpecificIndexOfAnArray() throws Exception {
        String tenantId = getOrganizationId();
        String tableName = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(tableName, tenantId, null, false, getUrl());
        String query = "SELECT a_double_array[3] FROM  " + tableName;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            // Need to support primitive
            Double[] doubleArr = new Double[1];
            doubleArr[0] = 37.56;
            Double result =  rs.getDouble(1);
            assertEquals(doubleArr[0], result);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testCaseWithArray() throws Exception {
        String tenantId = getOrganizationId();
        String tableName = createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null);
        initTablesWithArrays(tableName, tenantId, null, false, getUrl());
        String query = "SELECT CASE WHEN A_INTEGER = 1 THEN a_double_array ELSE null END [3] FROM  " + tableName;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            // Need to support primitive
            Double[] doubleArr = new Double[1];
            doubleArr[0] = 37.56;
            Double result =  rs.getDouble(1);
            assertEquals(doubleArr[0], result);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testUpsertValuesWithArray() throws Exception {
        String tenantId = getOrganizationId();
        String tableName = createTableWithArray(getUrl(), getDefaultSplits(tenantId), null);
        String query = "upsert into  " + tableName + " (ORGANIZATION_ID,ENTITY_ID,a_double_array) values('" + tenantId
                + "','00A123122312312',ARRAY[2.0,345.8])";
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
            query = "SELECT ARRAY_ELEM(a_double_array,2) FROM  " + tableName;
            statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            // Need to support primitive
            Double[] doubleArr = new Double[1];
            doubleArr[0] = 345.8d;
            conn.createArrayOf("DOUBLE", doubleArr);
            Double result = rs.getDouble(1);
            assertEquals(doubleArr[0], result);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testUpsertSelectWithSelectAsSubQuery1() throws Exception {
        String tenantId = getOrganizationId();
        String table1 = createTableWithArray(getUrl(), getDefaultSplits(tenantId), null);
        Connection conn = null;
        try {
            String table2 = createSimpleTableWithArray(getUrl(), getDefaultSplits(tenantId), null);
            initSimpleArrayTable(table2, tenantId, null, false);
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            conn = DriverManager.getConnection(getUrl(), props);
            String query = "upsert into " + table1 + " (ORGANIZATION_ID,ENTITY_ID,a_double_array) "
                    + "SELECT organization_id, entity_id, a_double_array  FROM " + table2
                    + " WHERE a_double_array[2] = 89.96";
            PreparedStatement statement = conn.prepareStatement(query);
            int executeUpdate = statement.executeUpdate();
            assertEquals(1, executeUpdate);
            conn.commit();
            statement.close();
            conn.close();
            // create another connection
            props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            conn = DriverManager.getConnection(getUrl(), props);
            query = "SELECT ARRAY_ELEM(a_double_array,2) FROM " + table1;
            statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            // Need to support primitive
            Double[] doubleArr = new Double[1];
            doubleArr[0] = 89.96d;
            Double result = rs.getDouble(1);
            assertEquals(result, doubleArr[0]);
            assertFalse(rs.next());

        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    @Test
    public void testArraySelectWithORCondition() throws Exception {
        String tenantId = getOrganizationId();
        Connection conn = null;
        try {
            String table = createSimpleTableWithArray(getUrl(), getDefaultSplits(tenantId), null);
            initSimpleArrayTable(table, tenantId, null, false);
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            conn = DriverManager.getConnection(getUrl(), props);
            String query = "SELECT a_double_array[1]  FROM " + table
                    + " WHERE a_double_array[2] = 89.96 or a_char_array[0] = 'a'";
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            Double[] doubleArr = new Double[1];
            doubleArr[0] = 64.87d;
            Double result = rs.getDouble(1);
            assertEquals(result, doubleArr[0]);
            assertFalse(rs.next());

        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    @Test
    public void testArraySelectWithANY() throws Exception {
        String tenantId = getOrganizationId();
        Connection conn = null;
        try {
            String table = createSimpleTableWithArray(getUrl(), getDefaultSplits(tenantId), null);
            initSimpleArrayTable(table, tenantId, null, false);
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            conn = DriverManager.getConnection(getUrl(), props);
            String query = "SELECT a_double_array[1]  FROM " + table
                    + " WHERE CAST(89.96 AS DOUBLE) = ANY(a_double_array)";
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            Double[] doubleArr = new Double[1];
            doubleArr[0] = 64.87d;
            Double result = rs.getDouble(1);
            assertEquals(result, doubleArr[0]);
            assertFalse(rs.next());
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    @Test
    public void testArraySelectWithALL() throws Exception {
        String tenantId = getOrganizationId();
        Connection conn = null;
        try {
            String table = createSimpleTableWithArray(getUrl(), getDefaultSplits(tenantId), null);
            initSimpleArrayTable(table, tenantId, null, false);
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            conn = DriverManager.getConnection(getUrl(), props);
            String query = "SELECT a_double_array[1]  FROM " + table
                    + " WHERE CAST(64.87 as DOUBLE) = ALL(a_double_array)";
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertFalse(rs.next());
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    @Test
    public void testArraySelectWithANYCombinedWithOR() throws Exception {
        String tenantId = getOrganizationId();
        Connection conn = null;
        try {
            String table = createSimpleTableWithArray(getUrl(), getDefaultSplits(tenantId), null);
            initSimpleArrayTable(table, tenantId, null, false);
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            conn = DriverManager.getConnection(getUrl(), props);
            String query = "SELECT a_double_array[1]  FROM " + table
                    + " WHERE  a_char_array[0] = 'f' or CAST(89.96 AS DOUBLE) > ANY(a_double_array)";
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            Double[] doubleArr = new Double[1];
            doubleArr[0] = 64.87d;
            Double result = rs.getDouble(1);
            assertEquals(result, doubleArr[0]);
            assertFalse(rs.next());
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    @Test
    public void testArraySelectWithALLCombinedWithOR() throws Exception {

        String tenantId = getOrganizationId();
        Connection conn = null;
        try {
            String table = createSimpleTableWithArray(getUrl(), getDefaultSplits(tenantId), null);
            initSimpleArrayTable(table, tenantId, null, false);
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            conn = DriverManager.getConnection(getUrl(), props);
            String query = "SELECT a_double_array[1], a_double_array[2]  FROM " + table
                    + " WHERE  a_char_array[0] = 'f' or CAST(100.0 AS DOUBLE) > ALL(a_double_array)";
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            Double[] doubleArr = new Double[1];
            doubleArr[0] = 64.87d;
            Double result = rs.getDouble(1);
            assertEquals(result, doubleArr[0]);
            doubleArr = new Double[1];
            doubleArr[0] = 89.96d;
            result = rs.getDouble(2);
            assertEquals(result, doubleArr[0]);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    @Test
    public void testArraySelectWithANYUsingVarLengthArray() throws Exception {
        Connection conn = null;
        try {
    
            String tenantId = getOrganizationId();
            String table = createTableWithArray(getUrl(), getDefaultSplits(tenantId), null);
            initTablesWithArrays(table, tenantId, null, false, getUrl());
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            conn = DriverManager.getConnection(getUrl(), props);
            String query = "SELECT a_string_array[1]  FROM " + table
                    + " WHERE 'XYZWER' = ANY(a_string_array)";
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            String[] strArr = new String[1];
            strArr[0] = "ABC";
            String result = rs.getString(1);
            assertEquals(result, strArr[0]);
            assertFalse(rs.next());
            query = "SELECT a_string_array[1]  FROM " + table + " WHERE 'AB' = ANY(a_string_array)";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue(rs.next());
            result = rs.getString(1);
            assertEquals(result, strArr[0]);
            assertFalse(rs.next());
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    @Test
    public void testSelectWithArrayWithColumnRef() throws Exception {

        String tenantId = getOrganizationId();
        String table = createTableWithArray(getUrl(), getDefaultSplits(tenantId), null);
        initTablesWithArrays(table, tenantId, null, false, getUrl());
        String query = "SELECT a_integer,ARRAY[1,2,a_integer] FROM " + table + " where organization_id =  '"
                + tenantId + "'";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            int val = rs.getInt(1);
            assertEquals(val, 1);
            Array array = rs.getArray(2);
            // Need to support primitive
            Integer[] intArr = new Integer[3];
            intArr[0] = 1;
            intArr[1] = 2;
            intArr[2] = 1;
            Array resultArr = conn.createArrayOf("INTEGER", intArr);
            assertEquals(resultArr, array);
            assertEquals("[1, 2, 1]", rs.getString(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectWithArrayWithColumnRefWithVarLengthArray() throws Exception {

        String tenantId = getOrganizationId();
        String table = createTableWithArray(getUrl(), getDefaultSplits(tenantId), null);
        initTablesWithArrays(table, tenantId, null, false, getUrl());
        String query = "SELECT b_string,ARRAY['abc','defgh',b_string] FROM " + table + " where organization_id =  '"
                + tenantId + "'";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertEquals(val, "b");
            Array array = rs.getArray(2);
            // Need to support primitive
            String[] strArr = new String[3];
            strArr[0] = "abc";
            strArr[1] = "defgh";
            strArr[2] = "b";
            Array resultArr = conn.createArrayOf("VARCHAR", strArr);
            assertEquals(resultArr, array);
            // since array is var length, last string element is messed up
            String expectedPrefix = "['abc', 'defgh', 'b";
            assertTrue("Expected to start with " + expectedPrefix,
                rs.getString(2).startsWith(expectedPrefix));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectWithArrayWithColumnRefWithVarLengthArrayWithNullValue() throws Exception {

        String tenantId = getOrganizationId();
        String table = createTableWithArray(getUrl(), getDefaultSplits(tenantId), null);
        initTablesWithArrays(table, tenantId, null, false, getUrl());
        String query = "SELECT b_string,ARRAY['abc',null,'bcd',null,null,b_string] FROM " + table + " where organization_id =  '"
                + tenantId + "'";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertEquals(val, "b");
            Array array = rs.getArray(2);
            // Need to support primitive
            String[] strArr = new String[6];
            strArr[0] = "abc";
            strArr[1] = null;
            strArr[2] = "bcd";
            strArr[3] = null;
            strArr[4] = null;
            strArr[5] = "b";
            Array resultArr = conn.createArrayOf("VARCHAR", strArr);
            assertEquals(resultArr, array);
            String expectedPrefix = "['abc', null, 'bcd', null, null, 'b";
            assertTrue("Expected to start with " + expectedPrefix,
                rs.getString(2).startsWith(expectedPrefix));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testUpsertSelectWithColumnRef() throws Exception {

        String tenantId = getOrganizationId();
        String table1 = createTableWithArray(getUrl(), getDefaultSplits(tenantId), null);
        Connection conn = null;
        try {
            String table2 = createSimpleTableWithArray(getUrl(), getDefaultSplits(tenantId), null);
            initSimpleArrayTable(table2, tenantId, null, false);
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            conn = DriverManager.getConnection(getUrl(), props);
            String query = "upsert into " + table1 + " (ORGANIZATION_ID,ENTITY_ID, a_unsigned_double, a_double_array) "
                    + "SELECT organization_id, entity_id, x_double, ARRAY[23.4, 22.1, x_double]  FROM " + table2
                    + " WHERE a_double_array[2] = 89.96";
            PreparedStatement statement = conn.prepareStatement(query);
            int executeUpdate = statement.executeUpdate();
            assertEquals(1, executeUpdate);
            conn.commit();
            statement.close();
            conn.close();
            // create another connection
            props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            conn = DriverManager.getConnection(getUrl(), props);
            query = "SELECT ARRAY_ELEM(a_double_array,2) FROM " + table1;
            statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            // Need to support primitive
            Double[] doubleArr = new Double[1];
            doubleArr[0] = 22.1d;
            Double result = rs.getDouble(1);
            assertEquals(result, doubleArr[0]);
            assertFalse(rs.next());

        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    @Test
    public void testCharArraySpecificIndex() throws Exception {

        String tenantId = getOrganizationId();
        String table = createSimpleTableWithArray(getUrl(), getDefaultSplits(tenantId), null);
        initSimpleArrayTable(table, tenantId, null, false);
        String query = "SELECT a_char_array[2] FROM " + table;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            String[] charArr = new String[1];
            charArr[0] = "b";
            String result = rs.getString(1);
            assertEquals(charArr[0], result);
        } finally {
            conn.close();
        }
    }

    @Test
    public void testArrayWithDescOrder() throws Exception {
        Connection conn;
        PreparedStatement stmt;
        ResultSet rs;

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        conn.createStatement().execute(
                "CREATE TABLE  " + table + "  ( k VARCHAR, a_string_array VARCHAR(100) ARRAY[4], b_string_array VARCHAR(100) ARRAY[4] \n"
                        + " CONSTRAINT pk PRIMARY KEY (k, b_string_array DESC)) \n");
        conn.close();
        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("UPSERT INTO " + table + " VALUES(?,?,?)");
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
        rs = conn.createStatement().executeQuery("SELECT b_string_array FROM  " + table);
        assertTrue(rs.next());
        PhoenixArray strArr = (PhoenixArray)rs.getArray(1);
        assertEquals(array, strArr);
        assertEquals("['abc', 'def', 'ghi', 'jkll', null, null, null, 'xxx']", rs.getString(1));
        conn.close();
    }

    @Test
    public void testArrayWithFloatArray() throws Exception {
        Connection conn;
        PreparedStatement stmt;
        ResultSet rs;

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        conn.createStatement().execute("CREATE TABLE  " + table + "  ( k VARCHAR PRIMARY KEY, a Float ARRAY[])");
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("UPSERT INTO  " + table + "  VALUES('a',ARRAY[2.0,3.0])");
        int res = stmt.executeUpdate();
        assertEquals(1, res);
        conn.commit();
        conn.close();
        conn = DriverManager.getConnection(getUrl(), props);
        rs = conn.createStatement().executeQuery("SELECT ARRAY_ELEM(a,2) FROM  " + table);
        assertTrue(rs.next());
        Float f = new Float(3.0);
        assertEquals(f, (Float)rs.getFloat(1));
        conn.close();
    }

    @Test
    public void testArrayWithVarCharArray() throws Exception {
        Connection conn;
        PreparedStatement stmt;
        ResultSet rs;

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        conn.createStatement().execute("CREATE TABLE  " + table + "  ( k VARCHAR PRIMARY KEY, a VARCHAR ARRAY[])");
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("UPSERT INTO  " + table + "  VALUES('a',ARRAY['a',null])");
        int res = stmt.executeUpdate();
        assertEquals(1, res);
        conn.commit();
        conn.close();
        conn = DriverManager.getConnection(getUrl(), props);
        rs = conn.createStatement().executeQuery("SELECT ARRAY_ELEM(a,2) FROM  " + table);
        assertTrue(rs.next());
        assertEquals(null, rs.getString(1));
        conn.close();
    }

    @Test
    public void testArraySelectSingleArrayElemWithCast() throws Exception {
        Connection conn;
        PreparedStatement stmt;
        ResultSet rs;

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        conn.createStatement().execute("CREATE TABLE  " + table + "  ( k VARCHAR PRIMARY KEY, a bigint ARRAY[])");
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("UPSERT INTO  " + table + "  VALUES(?,?)");
        stmt.setString(1, "a");
        Long[] s = new Long[] {1l, 2l};
        Array array = conn.createArrayOf("BIGINT", s);
        stmt.setArray(2, array);
        stmt.execute();
        conn.commit();
        conn.close();
        conn = DriverManager.getConnection(getUrl(), props);
        rs = conn.createStatement().executeQuery("SELECT k, CAST(a[2] AS DOUBLE) FROM  " + table);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        Double d = new Double(2.0);
        assertEquals(d, (Double)rs.getDouble(2));
        conn.close();
    }

    @Test
    public void testArraySelectGetString() throws Exception {
        Connection conn;
        PreparedStatement stmt;

        String tenantId = getOrganizationId();

        // create the table
        String tableName = createTableWithAllArrayTypes(getUrl(), getDefaultSplits(tenantId), null);

        // populate the table with data
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        stmt =
                conn.prepareStatement("UPSERT INTO "
                        + tableName
                        + "(ORGANIZATION_ID, ENTITY_ID, BOOLEAN_ARRAY, BYTE_ARRAY, DOUBLE_ARRAY, FLOAT_ARRAY, INT_ARRAY, LONG_ARRAY, SHORT_ARRAY, STRING_ARRAY)\n"
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW1);
        // boolean array
        Array boolArray = conn.createArrayOf("BOOLEAN", new Boolean[] { true, false });
        int boolIndex = 3;
        stmt.setArray(boolIndex, boolArray);
        // byte array
        Array byteArray = conn.createArrayOf("TINYINT", new Byte[] { 11, 22 });
        int byteIndex = 4;
        stmt.setArray(byteIndex, byteArray);
        // double array
        Array doubleArray = conn.createArrayOf("DOUBLE", new Double[] { 67.78, 78.89 });
        int doubleIndex = 5;
        stmt.setArray(doubleIndex, doubleArray);
        // float array
        Array floatArray = conn.createArrayOf("FLOAT", new Float[] { 12.23f, 45.56f });
        int floatIndex = 6;
        stmt.setArray(floatIndex, floatArray);
        // int array
        Array intArray = conn.createArrayOf("INTEGER", new Integer[] { 5555, 6666 });
        int intIndex = 7;
        stmt.setArray(intIndex, intArray);
        // long array
        Array longArray = conn.createArrayOf("BIGINT", new Long[] { 7777777L, 8888888L });
        int longIndex = 8;
        stmt.setArray(longIndex, longArray);
        // short array
        Array shortArray = conn.createArrayOf("SMALLINT", new Short[] { 333, 444 });
        int shortIndex = 9;
        stmt.setArray(shortIndex, shortArray);
        // create character array
        Array stringArray = conn.createArrayOf("VARCHAR", new String[] { "a", "b" });
        int stringIndex = 10;
        stmt.setArray(stringIndex, stringArray);
        stmt.execute();
        conn.commit();
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        stmt =
                conn.prepareStatement("SELECT organization_id, entity_id, boolean_array, byte_array, double_array, float_array, int_array, long_array, short_array, string_array FROM "
                        + tableName);
        TestUtil.analyzeTable(conn, tableName);

        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());

        assertEquals(tenantId, rs.getString(1));
        assertEquals(ROW1, rs.getString(2));
        
        assertArrayGetString(rs, boolIndex, boolArray, "true, false");
        assertArrayGetString(rs, byteIndex, byteArray, "11, 22");
        assertArrayGetString(rs, doubleIndex, doubleArray, "67.78, 78.89");
        assertArrayGetString(rs, floatIndex, floatArray, "12.23, 45.56");
        assertArrayGetString(rs, intIndex, intArray, "5555, 6666");
        assertArrayGetString(rs, longIndex, longArray, "7777777, 8888888");
        assertArrayGetString(rs, shortIndex, shortArray, "333, 444");
        assertArrayGetString(rs, stringIndex, stringArray, "'a', 'b'");
        conn.close();
    }

    @Test
    public void testArrayWithCast() throws Exception {
        Connection conn;
        PreparedStatement stmt;
        ResultSet rs;

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName(); 
        conn.createStatement().execute("CREATE TABLE " + table + " ( k VARCHAR PRIMARY KEY, a bigint ARRAY[])");
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("UPSERT INTO  " + table + "  VALUES(?,?)");
        stmt.setString(1, "a");
        Long[] s = new Long[] { 1l, 2l };
        Array array = conn.createArrayOf("BIGINT", s);
        stmt.setArray(2, array);
        stmt.execute();
        conn.commit();
        conn.close();
        conn = DriverManager.getConnection(getUrl(), props);
        rs = conn.createStatement().executeQuery("SELECT CAST(a AS DOUBLE []) FROM  " + table);
        assertTrue(rs.next());
        Double[] d = new Double[] { 1.0, 2.0 };
        array = conn.createArrayOf("DOUBLE", d);
        PhoenixArray arr = (PhoenixArray)rs.getArray(1);
        assertEquals(array, arr);
        assertEquals("[1.0, 2.0]", rs.getString(1));
        conn.close();
    }

    @Test
    public void testArrayWithCastForVarLengthArr() throws Exception {
        Connection conn;
        PreparedStatement stmt;
        ResultSet rs;

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        conn.createStatement().execute("CREATE TABLE  " + table + "  ( k VARCHAR PRIMARY KEY, a VARCHAR(5) ARRAY)");
        conn.close();
        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("UPSERT INTO  " + table + "  VALUES(?,?)");
        stmt.setString(1, "a");
        String[] s = new String[] { "1", "2" };
        PhoenixArray array = (PhoenixArray)conn.createArrayOf("VARCHAR", s);
        stmt.setArray(2, array);
        stmt.execute();
        conn.commit();
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        rs = conn.createStatement().executeQuery("SELECT CAST(a AS CHAR ARRAY) FROM  " + table);
        assertTrue(rs.next());
        PhoenixArray arr = (PhoenixArray)rs.getArray(1);
        String[] array2 = (String[])array.getArray();
        String[] array3 = (String[])arr.getArray();
        assertEquals(array2[0], array3[0]);
        assertEquals(array2[1], array3[1]);
        assertEquals("['1', '2']", rs.getString(1));
        conn.close();
    }

    @Test
    public void testGetSetObject() throws SQLException {
        String table = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String[] s = new String[] { "1", "2" };
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
                Statement stmt = conn.createStatement();
                PreparedStatement insertStmt =
                        conn.prepareStatement("upsert into " + table + "(k, a) values (?,?)")) {
            stmt.executeUpdate(
                "CREATE TABLE  " + table + "  ( k VARCHAR PRIMARY KEY, a VARCHAR(5) ARRAY)");
            PhoenixArray phoenixArray = (PhoenixArray) conn.createArrayOf("VARCHAR", s);

            insertStmt.setString(1, "a");
            insertStmt.setArray(2, phoenixArray);
            insertStmt.executeUpdate();
            insertStmt.setString(1, "b");
            insertStmt.setObject(2, phoenixArray);
            insertStmt.executeUpdate();
            conn.commit();
            try {
                insertStmt.setString(1, "c");
                insertStmt.setObject(2, s);
                insertStmt.executeUpdate();
                conn.commit();
                fail("must only accept java.sql.Array objects");
            } catch (Exception e) {
                // FIXME we should be throwing an SQLException
                // Success
            }

            ResultSet rs = stmt.executeQuery("select * from " + table);
            int rows = 0;
            while (rs.next()) {
                rows++;
                assertEquals(phoenixArray, rs.getObject(2));
                assertEquals(phoenixArray, rs.getObject(2, java.sql.Array.class));
                assertEquals(phoenixArray, rs.getArray(2));
                try {
                    rs.getObject(2, String[].class);
                    fail("must only accept java.sql.Array subclasses");
                } catch (SQLException e) {
                    // Success
                }
            }
            assertEquals(2, rows);
        }
    }
}
