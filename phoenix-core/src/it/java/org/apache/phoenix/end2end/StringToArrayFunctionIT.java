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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.junit.Before;
import org.junit.Test;

public class StringToArrayFunctionIT extends ParallelStatsDisabledIT {

    private String tableName;

    @Before
    public void initTables() throws Exception {
        tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String ddl = "CREATE TABLE " + tableName
                + " (region_name VARCHAR PRIMARY KEY, string1 VARCHAR, string2 CHAR(50), delimiter1 VARCHAR, delimiter2 CHAR(20), nullstring1 VARCHAR, nullstring2 CHAR(20))";
            conn.createStatement().execute(ddl);
            String dml = "UPSERT INTO " + tableName
                + "(region_name, string1, string2, delimiter1, delimiter2, nullstring1, nullstring2) VALUES('SF Bay Area',"
                +
                    "'a,b,c,d'," +
                    "'1.2.3.4'," +
                    "','," +
                    "'.'," +
                    "'c'," +
                    "'3'" +
                    ")";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.execute();
            conn.commit();
        }
    }

    @Test
    public void testStringToArrayFunction1() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT STRING_TO_ARRAY(string1, delimiter1) FROM " + tableName
                + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, new Object[]{"a", "b", "c", "d"});

        assertEquals(expected, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testStringToArrayFunction2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT STRING_TO_ARRAY(string1, delimiter1, nullstring1) FROM " + tableName
                + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, new Object[]{"a", "b", null, "d"});

        assertEquals(expected, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testStringToArrayFunction3() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT STRING_TO_ARRAY(string1, delimiter1, 'a') FROM " + tableName
                + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, new Object[]{null, "b", "c", "d"});

        assertEquals(expected, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testStringToArrayFunction4() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT STRING_TO_ARRAY(string1, delimiter1, 'd') FROM " + tableName
                + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, new Object[]{"a", "b", "c", null});

        assertEquals(expected, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testStringToArrayFunction5() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT STRING_TO_ARRAY(string2, delimiter2) FROM " + tableName
                + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, new Object[]{"1", "2", "3", "4"});

        assertEquals(expected, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testStringToArrayFunction6() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT STRING_TO_ARRAY(string2, delimiter2, nullstring2) FROM " + tableName
                + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, new Object[]{"1", "2", null, "4"});

        assertEquals(expected, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testStringToArrayFunction7() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT STRING_TO_ARRAY(string2, delimiter2, '1') FROM " + tableName
                + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, new Object[]{null, "2", "3", "4"});

        assertEquals(expected, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testStringToArrayFunction8() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT STRING_TO_ARRAY(string2, delimiter2, '4') FROM " + tableName
                + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, new Object[]{"1", "2", "3", null});

        assertEquals(expected, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testStringToArrayFunction9() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT STRING_TO_ARRAY(region_name, ' ', '4') FROM " + tableName
                + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, new Object[]{"SF", "Bay", "Area"});

        assertEquals(expected, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testStringToArrayFunction10() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT STRING_TO_ARRAY('hello,hello,hello', delimiter1) FROM " + tableName
                + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, new Object[]{"hello", "hello", "hello"});

        assertEquals(expected, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testStringToArrayFunction11() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT STRING_TO_ARRAY('a,hello,hello,hello,b', ',', 'hello') FROM " + tableName
                + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, new Object[]{"a", null, null, null, "b"});

        assertEquals(expected, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testStringToArrayFunction12() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT STRING_TO_ARRAY('b.a.b', delimiter2, 'b') FROM " + tableName
                + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, new Object[]{null, "a", null});

        assertEquals(expected, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testStringToArrayFunctionWithNestedFunctions1() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_LENGTH(STRING_TO_ARRAY('a, b, c', ', ')) FROM " + tableName
                + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        assertEquals(3, rs.getInt(1));
        assertFalse(rs.next());
    }

    @Test
    public void testStringToArrayFunctionWithNestedFunctions2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT STRING_TO_ARRAY(ARRAY_TO_STRING(ARRAY['a', 'b', 'c'], delimiter2), delimiter2, 'b') FROM "
                + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, new Object[]{"a", null, "c"});

        assertEquals(expected, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testStringToArrayFunctionWithNestedFunctions3() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT STRING_TO_ARRAY(ARRAY_TO_STRING(ARRAY['a', 'b', 'c'], delimiter2), ARRAY_ELEM(ARRAY[',', '.'], 2), 'b') FROM "
                + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, new Object[]{"a", null, "c"});

        assertEquals(expected, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testStringToArrayFunctionWithUpsert1() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String tableName = generateUniqueName();
        String ddl =
            "CREATE TABLE " + tableName + " (region_name VARCHAR PRIMARY KEY,varchars VARCHAR[])";
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + tableName
            + "(region_name,varchars) VALUES('SF Bay Area', STRING_TO_ARRAY('hello, world, :-)', ', '))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT varchars FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, new Object[]{"hello", "world", ":-)"});

        assertEquals(expected, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testStringToArrayFunctionWithUpsert2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String tableName = generateUniqueName();
        String ddl =
            "CREATE TABLE " + tableName + " (region_name VARCHAR PRIMARY KEY,varchars VARCHAR[])";
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + tableName
            + "(region_name,varchars) VALUES('SF Bay Area', STRING_TO_ARRAY('a, b, -, c', ', ', '-'))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT varchars FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, new Object[]{"a", "b", null, "c"});

        assertEquals(expected, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testStringToArrayFunctionWithUpsertSelect1() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String table1 = generateUniqueName();
        String ddl =
            "CREATE TABLE " + table1 + " (region_name VARCHAR PRIMARY KEY, varchar VARCHAR)";
        conn.createStatement().execute(ddl);

        String table2 = generateUniqueName();
        ddl = "CREATE TABLE " + table2 + " (region_name VARCHAR PRIMARY KEY, varchars VARCHAR[])";
        conn.createStatement().execute(ddl);

        String dml =
            "UPSERT INTO " + table1 + "(region_name, varchar) VALUES('SF Bay Area', 'a,b,c,d')";
        conn.createStatement().execute(dml);

        dml = "UPSERT INTO " + table1 + "(region_name, varchar) VALUES('SF Bay Area2', '1,2,3,4')";
        conn.createStatement().execute(dml);
        conn.commit();

        dml =
            "UPSERT INTO " + table2
                + "(region_name, varchars) SELECT region_name, STRING_TO_ARRAY(varchar, ',') FROM "
                + table1;
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT varchars FROM " + table2);
        assertTrue(rs.next());

        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, new Object[]{"a", "b", "c", "d"});

        assertEquals(expected, rs.getArray(1));
        assertTrue(rs.next());

        expected = new PhoenixArray(PVarchar.INSTANCE, new Object[]{"1", "2", "3", "4"});

        assertEquals(expected, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testStringToArrayFunctionWithUpsertSelect2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String sourceTable = generateUniqueName();

        String ddl =
            "CREATE TABLE " + sourceTable + " (region_name VARCHAR PRIMARY KEY, varchar VARCHAR)";
        conn.createStatement().execute(ddl);

        String targetTable = generateUniqueName();
        ddl = "CREATE TABLE " + targetTable
            + " (region_name VARCHAR PRIMARY KEY, varchars VARCHAR[])";
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + sourceTable
            + "(region_name, varchar) VALUES('SF Bay Area', 'a,b,-,c,d')";
        conn.createStatement().execute(dml);

        dml = "UPSERT INTO " + sourceTable
            + "(region_name, varchar) VALUES('SF Bay Area2', '1,2,-,3,4')";
        conn.createStatement().execute(dml);
        conn.commit();

        dml =
            "UPSERT INTO " + targetTable
                + "(region_name, varchars) SELECT region_name, STRING_TO_ARRAY(varchar, ',', '-') FROM "
                + sourceTable;
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT varchars FROM " + targetTable);
        assertTrue(rs.next());

        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, new Object[]{"a", "b", null, "c", "d"});

        assertEquals(expected, rs.getArray(1));
        assertTrue(rs.next());

        expected = new PhoenixArray(PVarchar.INSTANCE, new Object[]{"1", "2", null, "3", "4"});

        assertEquals(expected, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testStringToArrayFunctionInWhere1() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT region_name FROM " + tableName
            + " WHERE ARRAY['a', 'b', 'c', 'd']=STRING_TO_ARRAY(string1, delimiter1)");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testStringToArrayFunctionInWhere2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT region_name FROM " + tableName
            + " WHERE 'a'=ANY(STRING_TO_ARRAY(string1, delimiter1))");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testStringToArrayFunctionInWhere3() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT region_name FROM " + tableName
            + " WHERE 'a'=ALL(STRING_TO_ARRAY('a,a,a,', delimiter1))");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }
}
