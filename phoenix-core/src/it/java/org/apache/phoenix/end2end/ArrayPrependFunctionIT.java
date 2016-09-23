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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.*;

import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.junit.Test;

public class ArrayPrependFunctionIT extends ParallelStatsDisabledIT {

    private void initTableWithVarArray(Connection conn, String tableName, String type, Object[] objectArray, String value) throws SQLException {
        conn.createStatement().execute("CREATE TABLE " + tableName + " ( k VARCHAR PRIMARY KEY, a " + type + "[],b " + type + ")");
        conn.commit();
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES(?,?," + value + ")");
        PhoenixArray array = (PhoenixArray) conn.createArrayOf(type, objectArray);
        stmt.setString(1, "a");
        stmt.setArray(2, array);
        stmt.execute();
        conn.commit();

    }

    private void initTables(Connection conn, String tableName) throws Exception {
        String ddl = "CREATE TABLE " + tableName + " (region_name VARCHAR PRIMARY KEY,varchars VARCHAR[],integers INTEGER[],doubles DOUBLE[],bigints BIGINT[],chars CHAR(15)[],double1 DOUBLE,char1 CHAR(17),nullcheck INTEGER,chars2 CHAR(15)[])";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO " + tableName + " (region_name,varchars,integers,doubles,bigints,chars,double1,char1,nullcheck,chars2) VALUES('SF Bay Area'," +
                "ARRAY['2345','46345','23234']," +
                "ARRAY[2345,46345,23234,456]," +
                "ARRAY[23.45,46.345,23.234,45.6,5.78]," +
                "ARRAY[12,34,56,78,910]," +
                "ARRAY['a','bbbb','c','ddd','e']," +
                "23.45," +
                "'wert'," +
                "NULL," +
                "ARRAY['foo','a','bbbb','c','ddd','e']" +
                ")";
        PreparedStatement stmt = conn.prepareStatement(dml);
        stmt.execute();
        conn.commit();
    }

    private void initTablesDesc(Connection conn, String tableName, String type, String val) throws Exception {
        String ddl = "CREATE TABLE " + tableName + " (pk " + type + " PRIMARY KEY DESC,varchars VARCHAR[],integers INTEGER[],doubles DOUBLE[],bigints BIGINT[],chars CHAR(15)[],chars2 CHAR(15)[], bools BOOLEAN[])";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO " + tableName + "(pk,varchars,integers,doubles,bigints,chars,chars2,bools) VALUES(" + val + "," +
                "ARRAY['2345','46345','23234']," +
                "ARRAY[2345,46345,23234,456]," +
                "ARRAY[23.45,46.345,23.234,45.6,5.78]," +
                "ARRAY[12,34,56,78,910]," +
                "ARRAY['a','bbbb','c','ddd','e']," +
                "ARRAY['a','bbbb','c','ddd','e','foo']," +
                "ARRAY[true,false]" +
                ")";
        PreparedStatement stmt = conn.prepareStatement(dml);
        stmt.execute();
        conn.commit();
    }

    @Test
    public void testArrayPrependFunctionInteger() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();
        initTables(conn, tableName);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_PREPEND(1234,integers) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Integer[] integers = new Integer[]{1234, 2345, 46345, 23234, 456};

        Array array = conn.createArrayOf("INTEGER", integers);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayPrependFunctionVarchar() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();
        initTables(conn, tableName);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_PREPEND('34567',varchars) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String[] strings = new String[]{"34567", "2345", "46345", "23234"};

        Array array = conn.createArrayOf("VARCHAR", strings);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayPrependFunctionNulls1() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String[] s = new String[]{null, null, "1", "2"};
        String tableName = generateUniqueName();
        initTableWithVarArray(conn, tableName, "VARCHAR", s, null);
        String[] s2 = new String[]{null, null, null, "1", "2"};
        PhoenixArray array2 = (PhoenixArray) conn.createArrayOf("VARCHAR", s2);
        conn = DriverManager.getConnection(getUrl());
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_PREPEND(b,a) FROM " + tableName + " WHERE k = 'a'");
        assertTrue(rs.next());
        assertEquals(array2, rs.getArray(1));
    }

    @Test
    public void testArrayPrependFunctionNulls2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String[] s = new String[]{"1", "2"};
        String tableName = generateUniqueName();
        initTableWithVarArray(conn, tableName, "VARCHAR", s, null);
        String[] s2 = new String[]{null, "1", "2"};
        PhoenixArray array2 = (PhoenixArray) conn.createArrayOf("VARCHAR", s2);
        conn = DriverManager.getConnection(getUrl());
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_PREPEND(b,a) FROM " + tableName + " WHERE k = 'a'");
        assertTrue(rs.next());
        assertEquals(array2, rs.getArray(1));
    }

    @Test
    public void testArrayPrependFunctionNulls3() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String[] s = new String[]{"176", null, "212"};
        String tableName = generateUniqueName();
        initTableWithVarArray(conn, tableName, "VARCHAR", s, null);
        String[] s2 = new String[]{null, "176", null, "212"};
        PhoenixArray array2 = (PhoenixArray) conn.createArrayOf("VARCHAR", s2);
        conn = DriverManager.getConnection(getUrl());
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_PREPEND(b,a) FROM " + tableName + " WHERE k = 'a'");
        assertTrue(rs.next());
        assertEquals(array2, rs.getArray(1));
    }

    @Test
    public void testArrayPrependFunctionNulls4() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String[] s = new String[]{"176", null, "212"};
        String tableName = generateUniqueName();
        initTableWithVarArray(conn, tableName, "VARCHAR", s, "'foo'");
        String[] s2 = new String[]{"foo", "176", null, "212"};
        PhoenixArray array2 = (PhoenixArray) conn.createArrayOf("VARCHAR", s2);
        conn = DriverManager.getConnection(getUrl());
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_PREPEND(b,a) FROM " + tableName + " WHERE k = 'a'");
        assertTrue(rs.next());
        assertEquals(array2, rs.getArray(1));
    }

    @Test
    public void testArrayPrependFunctionDouble() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();
        initTables(conn, tableName);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_PREPEND(double1,doubles) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Double[] doubles = new Double[]{23.45, 23.45, 46.345, 23.234, 45.6, 5.78};

        Array array = conn.createArrayOf("DOUBLE", doubles);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayPrependFunctionDouble2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();
        initTables(conn, tableName);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_PREPEND(23,doubles) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Double[] doubles = new Double[]{new Double(23), 23.45, 46.345, 23.234, 45.6, 5.78};

        Array array = conn.createArrayOf("DOUBLE", doubles);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayPrependFunctionBigint() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();
        initTables(conn, tableName);
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_PREPEND(1112,bigints) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Long[] longs = new Long[]{1112l, 12l, 34l, 56l, 78l, 910l};

        Array array = conn.createArrayOf("BIGINT", longs);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayPrependFunctionChar() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();
        initTables(conn, tableName);
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_PREPEND('fac',chars) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String[] strings = new String[]{"fac", "a", "bbbb", "c", "ddd", "e"};

        Array array = conn.createArrayOf("CHAR", strings);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test(expected = TypeMismatchException.class)
    public void testArrayPrependFunctionIntToCharArray() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();
        initTables(conn, tableName);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_PREPEND(234,varchars) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
    }

    @Test(expected = TypeMismatchException.class)
    public void testArrayPrependFunctionVarcharToIntegerArray() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();
        initTables(conn, tableName);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_PREPEND('234',integers) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");

    }

    @Test(expected = SQLException.class)
    public void testArrayPrependFunctionChar2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();
        initTables(conn, tableName);
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_PREPEND('facfacfacfacfacfacfac',chars) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        rs.next();
        rs.getArray(1);
    }

    @Test
    public void testArrayPrependFunctionIntegerToDoubleArray() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();
        initTables(conn, tableName);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_PREPEND(45,doubles) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Double[] doubles = new Double[]{45.0, 23.45, 46.345, 23.234, 45.6, 5.78};

        Array array = conn.createArrayOf("DOUBLE", doubles);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayPrependFunctionWithNestedFunctions1() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();
        initTables(conn, tableName);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_PREPEND(integers[1],ARRAY[23,45]) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Integer[] integers = new Integer[]{2345, 23, 45};

        Array array = conn.createArrayOf("INTEGER", integers);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayPrependFunctionWithNestedFunctions2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();
        initTables(conn, tableName);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_PREPEND(ARRAY_ELEM(ARRAY[2,4],1),integers) FROM " + tableName+ " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Integer[] integers = new Integer[]{2, 2345, 46345, 23234, 456};

        Array array = conn.createArrayOf("INTEGER", integers);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayPrependFunctionWithNestedFunctions3() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();
        initTables(conn, tableName);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_PREPEND(ARRAY_ELEM(doubles,2),doubles) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Double[] doubles = new Double[]{46.345, 23.45, 46.345, 23.234, 45.6, 5.78};

        Array array = conn.createArrayOf("DOUBLE", doubles);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayPrependFunctionWithUpsert1() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE " + tableName + " (region_name VARCHAR PRIMARY KEY,varchars VARCHAR[])";
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + tableName + " (region_name,varchars) VALUES('SF Bay Area',ARRAY_PREPEND(':-)',ARRAY['hello','world']))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT varchars FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String[] strings = new String[]{":-)", "hello", "world"};

        Array array = conn.createArrayOf("VARCHAR", strings);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayPrependFunctionWithUpsert2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();

        String ddl = "CREATE TABLE " + tableName + " (region_name VARCHAR PRIMARY KEY,integers INTEGER[])";
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + tableName + "(region_name,integers) VALUES('SF Bay Area',ARRAY_PREPEND(6,ARRAY[4,5]))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT integers FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Integer[] integers = new Integer[]{6, 4, 5};

        Array array = conn.createArrayOf("INTEGER", integers);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayPrependFunctionWithUpsert3() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE " + tableName + "(region_name VARCHAR PRIMARY KEY,doubles DOUBLE[])";
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + tableName + "(region_name,doubles) VALUES('SF Bay Area',ARRAY_PREPEND(9.0,ARRAY[5.67,7.87]))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT doubles FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Double[] doubles = new Double[]{new Double(9), 5.67, 7.87};

        Array array = conn.createArrayOf("DOUBLE", doubles);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayPrependFunctionWithUpsertSelect1() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String baseTable = generateUniqueName();
        String source = baseTable + "_SOURCE";
        String target = baseTable + "_TARGET";
        String ddl = "CREATE TABLE " + source + " (region_name VARCHAR PRIMARY KEY,doubles DOUBLE[])";
        conn.createStatement().execute(ddl);

        ddl = "CREATE TABLE " + target + "(region_name VARCHAR PRIMARY KEY,doubles DOUBLE[])";
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + source + "(region_name,doubles) VALUES('SF Bay Area',ARRAY_PREPEND(9.0,ARRAY[5.67,7.87]))";
        conn.createStatement().execute(dml);

        dml = "UPSERT INTO " + source + "(region_name,doubles) VALUES('SF Bay Area2',ARRAY_PREPEND(9.2,ARRAY[56.7,7.87]))";
        conn.createStatement().execute(dml);
        conn.commit();

        dml = "UPSERT INTO " + target + "(region_name, doubles) SELECT region_name, ARRAY_PREPEND(5,doubles) FROM " + source;
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT doubles FROM " + target);
        assertTrue(rs.next());

        Double[] doubles = new Double[]{new Double(5), new Double(9), 5.67, 7.87};
        Array array = conn.createArrayOf("DOUBLE", doubles);

        assertEquals(array, rs.getArray(1));
        assertTrue(rs.next());

        doubles = new Double[]{new Double(5), new Double(9.2), 56.7, 7.87};
        array = conn.createArrayOf("DOUBLE", doubles);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayPrependFunctionWithUpsertSelect2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String baseTable = generateUniqueName();
        String source = baseTable + "_SOURCE";
        String target = baseTable + "_TARGET";
        String ddl = "CREATE TABLE " + source + " (region_name VARCHAR PRIMARY KEY,varchars VARCHAR[])";
        conn.createStatement().execute(ddl);

        ddl = "CREATE TABLE " + target + " (region_name VARCHAR PRIMARY KEY,varchars VARCHAR[])";
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + source + "(region_name,varchars) VALUES('SF Bay Area',ARRAY_PREPEND('c',ARRAY['abcd','b']))";
        conn.createStatement().execute(dml);

        dml = "UPSERT INTO " + source + "(region_name,varchars) VALUES('SF Bay Area2',ARRAY_PREPEND('something',ARRAY['d','fgh']))";
        conn.createStatement().execute(dml);
        conn.commit();

        dml = "UPSERT INTO " + target + "(region_name, varchars) SELECT region_name, ARRAY_PREPEND('stu',varchars) FROM " + source;
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT varchars FROM " + target);
        assertTrue(rs.next());

        String[] strings = new String[]{"stu", "c", "abcd", "b"};
        Array array = conn.createArrayOf("VARCHAR", strings);

        assertEquals(array, rs.getArray(1));
        assertTrue(rs.next());

        strings = new String[]{"stu", "something", "d", "fgh"};
        array = conn.createArrayOf("VARCHAR", strings);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayPrependFunctionInWhere1() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();
        initTables(conn, tableName);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT region_name FROM " + tableName + " WHERE ARRAY[123,2345,46345,23234,456]=ARRAY_PREPEND(123,integers)");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayPrependFunctionInWhere2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();
        initTables(conn, tableName);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT region_name FROM " + tableName + " WHERE varchars[1]=ANY(ARRAY_PREPEND('1234',ARRAY['2345','46345','23234']))");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayPrependFunctionInWhere3() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();
        initTables(conn, tableName);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT region_name FROM " + tableName + " WHERE ARRAY['1234','2345','46345','23234']=ARRAY_PREPEND('1234',ARRAY['2345','46345','23234'])");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayPrependFunctionInWhere4() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();
        initTables(conn, tableName);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT region_name FROM " + tableName + " WHERE ARRAY[123.4,23.45,4634.5,2.3234]=ARRAY_PREPEND(123.4,ARRAY[23.45,4634.5,2.3234])");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayPrependFunctionInWhere5() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();
        initTables(conn, tableName);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT region_name FROM " + tableName + " WHERE ARRAY['foo','2345','46345','23234']=ARRAY_PREPEND('foo',varchars)");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayPrependFunctionInWhere6() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();
        initTables(conn, tableName);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT region_name FROM " + tableName + " WHERE chars2=ARRAY_PREPEND('foo',chars)");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayPrependFunctionInWhere7() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String  tableName = generateUniqueName();
        initTables(conn, tableName);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT region_name FROM " + tableName + " WHERE ARRAY[4,2,3]=ARRAY_PREPEND(4,ARRAY[2,3])");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test(expected = SQLException.class)
    public void testArrayPrependFunctionCharLimitCheck() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();
        initTables(conn, tableName);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_PREPEND(char1,chars) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String[] strings = new String[]{"wert", "a", "bbbb", "c", "ddd", "e"};

        Array array = conn.createArrayOf("CHAR", strings);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayPrependFunctionIntegerDesc() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();
        initTablesDesc(conn, tableName, "INTEGER", "23");

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_PREPEND(pk,integers) FROM " + tableName);
        assertTrue(rs.next());

        Integer[] integers = new Integer[]{23, 2345, 46345, 23234, 456};

        Array array = conn.createArrayOf("INTEGER", integers);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());

    }

    @Test
    public void testArrayPrependFunctionVarcharDesc() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();
        initTablesDesc(conn, tableName, "VARCHAR", "'e'");

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_PREPEND(pk,varchars) FROM " + tableName);
        assertTrue(rs.next());

        String[] strings = new String[]{"e", "2345", "46345", "23234"};

        Array array = conn.createArrayOf("VARCHAR", strings);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayPrependFunctionBigIntDesc() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String  tableName = generateUniqueName();
        initTablesDesc(conn, tableName, "BIGINT", "1112");
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_PREPEND(pk,bigints) FROM " +  tableName);
        assertTrue(rs.next());

        Long[] longs = new Long[]{1112l, 12l, 34l, 56l, 78l, 910l};

        Array array = conn.createArrayOf("BIGINT", longs);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayPrependFunctionBooleanDesc() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();
        initTablesDesc(conn, tableName, "BOOLEAN", "false");
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_PREPEND(pk,bools) FROM " + tableName);
        assertTrue(rs.next());

        Boolean[] booleans = new Boolean[]{false, true, false};

        Array array = conn.createArrayOf("BOOLEAN", booleans);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }
}
