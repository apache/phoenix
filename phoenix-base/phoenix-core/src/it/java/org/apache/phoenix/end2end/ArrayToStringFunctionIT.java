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

import org.junit.Before;
import org.junit.Test;

public class ArrayToStringFunctionIT extends ParallelStatsDisabledIT {
    private String tableName;
    private Connection conn;

    @Before
    public void initTables() throws Exception {
        conn = DriverManager.getConnection(getUrl());
        tableName = generateUniqueName();
        String ddl = "CREATE TABLE " + tableName
            + " (region_name VARCHAR PRIMARY KEY,varchars VARCHAR[],integers INTEGER[],doubles DOUBLE[],bigints BIGINT[],chars CHAR(15)[],double1 DOUBLE,varchar1 VARCHAR,nullcheck INTEGER,chars2 CHAR(15)[])";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO " + tableName
            + "(region_name,varchars,integers,doubles,bigints,chars,double1,varchar1,nullcheck,chars2) VALUES('SF Bay Area',"
            +
                "ARRAY['2345','46345','23234']," +
                "ARRAY[2345,46345,23234,456]," +
                "ARRAY[23.45,46.345,23.234,45.6,5.78]," +
                "ARRAY[12,34,56,78,910]," +
                "ARRAY['a','bbbb','c','ddd','e']," +
                "23.45," +
                "', '," +
                "NULL," +
                "ARRAY['a','bbbb','c','ddd','e','foo']" +
                ")";
        PreparedStatement stmt = conn.prepareStatement(dml);
        stmt.execute();
        conn.commit();
    }

    @Test
    public void testArrayToStringFunctionVarchar1() throws Exception {
        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_TO_STRING(varchars, ',','*') FROM " + tableName
                + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String expected = "2345,46345,23234";

        assertEquals(expected, rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayToStringFunctionVarchar2() throws Exception {
        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_TO_STRING(varchars, ',') FROM " + tableName
                + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String expected = "2345,46345,23234";

        assertEquals(expected, rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayToStringFunctionVarchar3() throws Exception {
        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_TO_STRING(ARRAY['hello', 'hello'], ',') FROM " + tableName
                + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String expected = "hello,hello";

        assertEquals(expected, rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayToStringFunctionInt() throws Exception {
        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_TO_STRING(integers, ',') FROM " + tableName
                + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String expected = "2345,46345,23234,456";

        assertEquals(expected, rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayToStringFunctionDouble1() throws Exception {
        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_TO_STRING(doubles, ', ') FROM " + tableName
                + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String expected = "23.45, 46.345, 23.234, 45.6, 5.78";

        assertEquals(expected, rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayToStringFunctionDouble2() throws Exception {
        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_TO_STRING(ARRAY[2.3, 4.5], ', ') FROM " + tableName
                + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String expected = "2.3, 4.5";

        assertEquals(expected, rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayToStringFunctionBigint() throws Exception {
        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_TO_STRING(bigints, ', ') FROM " + tableName
                + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String expected = "12, 34, 56, 78, 910";

        assertEquals(expected, rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayToStringFunctionChar1() throws Exception {
        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_TO_STRING(chars, ', ') FROM " + tableName
                + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String expected = "a   , bbbb, c   , ddd , e   ";

        assertEquals(expected, rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayToStringFunctionChar2() throws Exception {
        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_TO_STRING(chars2, ', ') FROM " + tableName
                + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String expected = "a   , bbbb, c   , ddd , e   , foo ";

        assertEquals(expected, rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayToStringFunctionChar3() throws Exception {
        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_TO_STRING(chars2, varchar1) FROM " + tableName
                + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String expected = "a   , bbbb, c   , ddd , e   , foo ";

        assertEquals(expected, rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayToStringFunctionWithNestedFunctions1() throws Exception {
        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_TO_STRING(ARRAY[integers[1],integers[1]], ', ') FROM " + tableName
                + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String expected = "2345, 2345";

        assertEquals(expected, rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayToStringFunctionWithNestedFunctions2() throws Exception {
        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_TO_STRING(ARRAY[ARRAY_ELEM(ARRAY[2,4],1),ARRAY_ELEM(ARRAY[2,4],2)], ', ') FROM "
                + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String expected = "2, 4";

        assertEquals(expected, rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayToStringFunctionWithNestedFunctions3() throws Exception {
        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_TO_STRING(ARRAY[ARRAY_ELEM(doubles, 1), ARRAY_ELEM(doubles, 1)], ', ') FROM "
                + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String expected = "23.45, 23.45";

        assertEquals(expected, rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayToStringFunctionWithNestedFunctions4() throws Exception {
        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_ELEM(ARRAY_APPEND(ARRAY['abc','bcd'], ARRAY_TO_STRING(ARRAY['a','b'], 'c')), 3) FROM "
                + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String expected = "acb";

        assertEquals(expected, rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayToStringFunctionWithUpsert1() throws Exception {
        String table = generateUniqueName();
        String ddl =
            "CREATE TABLE " + table + " (region_name VARCHAR PRIMARY KEY,varchar VARCHAR)";
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + table
            + "(region_name,varchar) VALUES('SF Bay Area',ARRAY_TO_STRING(ARRAY['hello','world'],','))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT varchar FROM " + table + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String expected = "hello,world";

        assertEquals(expected, rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayToStringFunctionWithUpsert2() throws Exception {
        String tableName = generateUniqueName();
        String ddl =
            "CREATE TABLE " + tableName + " (region_name VARCHAR PRIMARY KEY,varchar VARCHAR)";
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + tableName
            + "(region_name,varchar) VALUES('SF Bay Area',ARRAY_TO_STRING(ARRAY[3, 4, 5],', '))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT varchar FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String expected = "3, 4, 5";

        assertEquals(expected, rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayToStringFunctionWithUpsert3() throws Exception {
        String tableName = generateUniqueName();
        String ddl =
            "CREATE TABLE " + tableName + " (region_name VARCHAR PRIMARY KEY,varchar VARCHAR)";
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + tableName
            + "(region_name,varchar) VALUES('SF Bay Area',ARRAY_TO_STRING(ARRAY[3.1, 4.2, 5.5],', '))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT varchar FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String expected = "3.1, 4.2, 5.5";

        assertEquals(expected, rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayToStringFunctionWithUpsert4() throws Exception {
        String tableName = generateUniqueName();
        String ddl =
            "CREATE TABLE " + tableName + " (region_name VARCHAR PRIMARY KEY,varchar VARCHAR)";
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + tableName
            + "(region_name,varchar) VALUES('SF Bay Area',ARRAY_TO_STRING(ARRAY[true, false, true],', '))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT varchar FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String expected = "true, false, true";

        assertEquals(expected, rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayToStringFunctionWithUpsertSelect1() throws Exception {
        String source = generateUniqueName();
        String ddl =
            "CREATE TABLE " + source + " (region_name VARCHAR PRIMARY KEY,doubles DOUBLE[])";
        conn.createStatement().execute(ddl);

        String target = generateUniqueName();
        ddl = "CREATE TABLE " + target + " (region_name VARCHAR PRIMARY KEY,varchar VARCHAR)";
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + source
            + "(region_name,doubles) VALUES('SF Bay Area', ARRAY[5.67, 7.87])";
        conn.createStatement().execute(dml);

        dml = "UPSERT INTO " + source
            + "(region_name,doubles) VALUES('SF Bay Area2', ARRAY[9.2, 3.4])";
        conn.createStatement().execute(dml);
        conn.commit();

        dml =
            "UPSERT INTO " + target
                + "(region_name, varchar) SELECT region_name, ARRAY_TO_STRING(doubles, ', ') FROM "
                + source;
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT varchar FROM " + target);
        assertTrue(rs.next());

        String expected = "5.67, 7.87";
        assertEquals(expected, rs.getString(1));
        assertTrue(rs.next());

        expected = "9.2, 3.4";
        assertEquals(expected, rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayToStringFunctionWithUpsertSelect2() throws Exception {
        String source = generateUniqueName();
        String ddl =
            "CREATE TABLE " + source + " (region_name VARCHAR PRIMARY KEY,varchars VARCHAR[])";
        conn.createStatement().execute(ddl);

        String target = generateUniqueName();
        ddl = "CREATE TABLE " + target + " (region_name VARCHAR PRIMARY KEY,varchar VARCHAR)";
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + source
            + "(region_name,varchars) VALUES('SF Bay Area', ARRAY['hello', '-)'])";
        conn.createStatement().execute(dml);

        dml = "UPSERT INTO " + source
            + "(region_name,varchars) VALUES('SF Bay Area2', ARRAY['hello', '-('])";
        conn.createStatement().execute(dml);
        conn.commit();

        dml =
            "UPSERT INTO " + target
                + "(region_name, varchar) SELECT region_name, ARRAY_TO_STRING(varchars, ':') FROM "
                + source;
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT varchar FROM " + target);
        assertTrue(rs.next());

        String expected = "hello:-)";
        assertEquals(expected, rs.getString(1));
        assertTrue(rs.next());

        expected = "hello:-(";
        assertEquals(expected, rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayToStringFunctionWithUpsertSelect3() throws Exception {
        String source = generateUniqueName();
        String ddl =
            "CREATE TABLE " + source + " (region_name VARCHAR PRIMARY KEY,booleans BOOLEAN[])";
        conn.createStatement().execute(ddl);

        String target = generateUniqueName();
        ddl = "CREATE TABLE " + target + " (region_name VARCHAR PRIMARY KEY,varchar VARCHAR)";
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + source
            + "(region_name, booleans) VALUES('SF Bay Area', ARRAY[true, true])";
        conn.createStatement().execute(dml);

        dml = "UPSERT INTO " + source
            + "(region_name, booleans) VALUES('SF Bay Area2', ARRAY[false, false])";
        conn.createStatement().execute(dml);
        conn.commit();

        dml =
            "UPSERT INTO " + target
                + "(region_name, varchar) SELECT region_name, ARRAY_TO_STRING(booleans, ', ') FROM "
                + source;
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT varchar FROM " + target);
        assertTrue(rs.next());

        String expected = "true, true";
        assertEquals(expected, rs.getString(1));
        assertTrue(rs.next());

        expected = "false, false";
        assertEquals(expected, rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayToStringFunctionInWhere1() throws Exception {
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT region_name FROM " + tableName
            + " WHERE '2345,46345,23234,456' = ARRAY_TO_STRING(integers,',')");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayToStringFunctionInWhere2() throws Exception {
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT region_name FROM " + tableName
            + " WHERE 'a,b,c' = ARRAY_TO_STRING(ARRAY['a', 'b', 'c'], ',')");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayToStringFunctionInWhere3() throws Exception {
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT region_name FROM " + tableName
            + " WHERE '1.1,2.2,3.3' = ARRAY_TO_STRING(ARRAY[1.1, 2.2, 3.3], ',')");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayToStringFunctionInWhere4() throws Exception {
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT region_name FROM " + tableName
            + " WHERE 'true,true,true' = ARRAY_TO_STRING(ARRAY[true, true, true], ',')");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayToStringFunctionInWhere5() throws Exception {

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT region_name FROM " + tableName
            + " WHERE 'a, bbbb, c, ddd, e' = ARRAY_TO_STRING(ARRAY['a', 'bbbb', 'c' , 'ddd', 'e'], ', ')");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayToStringFunctionInWhere6() throws Exception {
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT region_name FROM " + tableName
            + " WHERE ARRAY_TO_STRING(ARRAY[1,2,3], varchar1) = '1, 2, 3'");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayToStringFunctionInWhere7() throws Exception {
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT region_name FROM " + tableName
            + " WHERE ARRAY_TO_STRING(varchars, varchar1) = '2345, 46345, 23234'");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayToStringFunctionWithNulls1() throws Exception {
        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_TO_STRING(ARRAY['a', NULL, 'b'], ', ','*') FROM " + tableName
                + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String expected = "a, *, b";

        assertEquals(expected, rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayToStringFunctionWithNulls2() throws Exception {
        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_TO_STRING(ARRAY['a', NULL, 'b'], ', ') FROM " + tableName
                + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String expected = "a, b";

        assertEquals(expected, rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayToStringFunctionWithNulls3() throws Exception {
        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_TO_STRING(ARRAY[NULL, 'a', 'b'], ', ', '*') FROM " + tableName
                + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String expected = "*, a, b";

        assertEquals(expected, rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayToStringFunctionWithNulls4() throws Exception {
        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_TO_STRING(ARRAY[NULL, 'a', 'b'], ', ') FROM " + tableName
                + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String expected = "a, b";

        assertEquals(expected, rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayToStringFunctionWithNulls5() throws Exception {
        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_TO_STRING(ARRAY['a', 'b', NULL], ', ', '*') FROM " + tableName
                + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String expected = "a, b, *";

        assertEquals(expected, rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayToStringFunctionWithNulls6() throws Exception {
        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_TO_STRING(ARRAY['a', 'b', NULL], ', ') FROM " + tableName
                + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String expected = "a, b";

        assertEquals(expected, rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayToStringFunctionWithNulls7() throws Exception {
        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_TO_STRING(ARRAY[NULL, NULL, 'a', 'b', NULL, 'c', 'd', NULL, 'e', NULL, NULL], ', ', '*') FROM "
                + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String expected = "*, *, a, b, *, c, d, *, e, *, *";

        assertEquals(expected, rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayToStringFunctionWithNulls8() throws Exception {
        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_TO_STRING(ARRAY[NULL, NULL, 'a', 'b', NULL, 'c', 'd', NULL, 'e', NULL, NULL], ', ') FROM "
                + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String expected = "a, b, c, d, e";

        assertEquals(expected, rs.getString(1));
        assertFalse(rs.next());
    }
}
