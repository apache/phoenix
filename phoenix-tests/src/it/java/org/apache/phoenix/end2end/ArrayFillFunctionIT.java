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

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;

import org.junit.Before;
import org.junit.Test;

public class ArrayFillFunctionIT extends ParallelStatsDisabledIT {

    private String tableName;

    @Before
    public void initTable() throws Exception {
        tableName = generateUniqueName();
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE " + tableName
            + " (region_name VARCHAR PRIMARY KEY,length1 INTEGER, length2 INTEGER,\"DATE\" DATE,\"time\" TIME,\"timestamp\" TIMESTAMP,\"varchar\" VARCHAR,\"integer\" INTEGER,\"double\" DOUBLE,\"bigint\" BIGINT,\"char\" CHAR(15),double1 DOUBLE,char1 CHAR(17),nullcheck INTEGER,chars2 CHAR(15)[], varchars2 VARCHAR[])";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO " + tableName
            + "(region_name,length1,length2,\"DATE\",\"time\",\"timestamp\",\"varchar\",\"integer\",\"double\",\"bigint\",\"char\",double1,char1,nullcheck,chars2,varchars2) VALUES('SF Bay Area',"
            +
                "0," +
                "-3," +
                "to_date('2015-05-20 06:12:14.184')," +
                "to_time('2015-05-20 06:12:14.184')," +
                "to_timestamp('2015-05-20 06:12:14.184')," +
                "'foo'," +
                "34," +
                "23.45," +
                "34567," +
                "'foo'," +
                "23.45," +
                "'wert'," +
                "NULL," +
                "ARRAY['hello','hello','hello']," +
                "ARRAY['hello','hello','hello']" +
                ")";
        PreparedStatement stmt = conn.prepareStatement(dml);
        stmt.execute();
        conn.commit();
    }

    @Test
    public void testArrayFillFunctionVarchar() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_FILL(\"varchar\",5) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String[] strings = new String[]{"foo", "foo", "foo", "foo", "foo"};

        Array array = conn.createArrayOf("VARCHAR", strings);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayFillFunctionInteger() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_FILL(\"integer\",4) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Object[] objects = new Object[]{34, 34, 34, 34};

        Array array = conn.createArrayOf("INTEGER", objects);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayFillFunctionDouble() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_FILL(\"double\",4) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Object[] objects = new Object[]{23.45, 23.45, 23.45, 23.45};

        Array array = conn.createArrayOf("DOUBLE", objects);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayFillFunctionBigint() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_FILL(\"bigint\",4) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Object[] objects = new Object[]{34567l, 34567l, 34567l, 34567l};

        Array array = conn.createArrayOf("BIGINT", objects);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayFillFunctionChar() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_FILL(\"char\",4) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Object[] objects = new Object[]{"foo", "foo", "foo", "foo"};

        Array array = conn.createArrayOf("CHAR", objects);
        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayFillFunctionVarChar() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_FILL(\"varchar\",4) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Object[] objects = new Object[]{"foo", "foo", "foo", "foo"};

        Array array = conn.createArrayOf("VARCHAR", objects);
        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayFillFunctionDate() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_FILL(\"DATE\",3) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Object[] objects = new Object[]{new Date(1432102334184l), new Date(1432102334184l), new Date(1432102334184l)};

        Array array = conn.createArrayOf("DATE", objects);
        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayFillFunctionTime() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_FILL(\"time\",3) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Object[] objects = new Object[]{new Time(1432102334184l), new Time(1432102334184l), new Time(1432102334184l)};

        Array array = conn.createArrayOf("TIME", objects);
        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayFillFunctionTimestamp() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_FILL(\"timestamp\",3) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Object[] objects = new Object[]{new Timestamp(1432102334184l), new Timestamp(1432102334184l), new Timestamp(1432102334184l)};

        Array array = conn.createArrayOf("TIMESTAMP", objects);
        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testArrayFillFunctionInvalidLength1() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_FILL(\"timestamp\",length2) FROM " + tableName
                + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Object[] objects = new Object[]{new Timestamp(1432102334184l), new Timestamp(1432102334184l), new Timestamp(1432102334184l)};

        Array array = conn.createArrayOf("TIMESTAMP", objects);
        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testArrayFillFunctionInvalidLength2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_FILL(\"timestamp\",length1) FROM " + tableName
                + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Object[] objects = new Object[]{new Timestamp(1432102334184l), new Timestamp(1432102334184l), new Timestamp(1432102334184l)};

        Array array = conn.createArrayOf("TIMESTAMP", objects);
        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayFillFunctionWithNestedFunctions1() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_FILL(ARRAY_ELEM(ARRAY[23,45],1),3) FROM " + tableName
                + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Integer[] integers = new Integer[]{23, 23, 23};

        Array array = conn.createArrayOf("INTEGER", integers);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayFillFunctionWithNestedFunctions2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_FILL('hello', ARRAY_LENGTH(ARRAY[34, 45])) FROM " + tableName
                + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Object[] objects = new Object[]{"hello", "hello"};

        Array array = conn.createArrayOf("VARCHAR", objects);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayFillFunctionWithNestedFunctions3() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT ARRAY_FILL(3.4, ARRAY_LENGTH(ARRAY[34, 45])) FROM " + tableName
                + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Object[] objects = new Object[]{BigDecimal.valueOf(3.4), BigDecimal.valueOf(3.4)};

        Array array = conn.createArrayOf("DECIMAL", objects);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayFillFunctionWithUpsert1() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String regions = generateUniqueName();
        String ddl =
            "CREATE TABLE " + regions + " (region_name VARCHAR PRIMARY KEY,varchars VARCHAR[])";
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + regions
            + "(region_name,varchars) VALUES('SF Bay Area',ARRAY_FILL('hello',3))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT varchars FROM " + regions + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String[] strings = new String[]{"hello", "hello", "hello"};

        Array array = conn.createArrayOf("VARCHAR", strings);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayFillFunctionWithUpsert2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String regions = generateUniqueName();
        String ddl =
            "CREATE TABLE " + regions + " (region_name VARCHAR PRIMARY KEY,integers INTEGER[])";
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + regions
            + "(region_name,integers) VALUES('SF Bay Area',ARRAY_FILL(3456,3))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT integers FROM " + regions + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Integer[] integers = new Integer[]{3456, 3456, 3456};

        Array array = conn.createArrayOf("INTEGER", integers);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayFillFunctionWithUpsert3() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String regions = generateUniqueName();
        String ddl =
            "CREATE TABLE " + regions + " (region_name VARCHAR PRIMARY KEY,doubles DOUBLE[])";
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + regions
            + "(region_name,doubles) VALUES('SF Bay Area',ARRAY_FILL(2.5,3))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT doubles FROM " + regions + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Double[] doubles = new Double[]{2.5, 2.5, 2.5};

        Array array = conn.createArrayOf("DOUBLE", doubles);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayFillFunctionWithUpsertSelect1() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String ddl = "CREATE TABLE source (region_name VARCHAR PRIMARY KEY,doubles DOUBLE[])";
        conn.createStatement().execute(ddl);

        ddl = "CREATE TABLE target (region_name VARCHAR PRIMARY KEY,doubles DOUBLE[],doubles2 DOUBLE[])";
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO source(region_name,doubles) VALUES('SF Bay Area',ARRAY_FILL(3.4,3))";
        conn.createStatement().execute(dml);

        dml = "UPSERT INTO source(region_name,doubles) VALUES('SF Bay Area2',ARRAY_FILL(2.3,3))";
        conn.createStatement().execute(dml);
        conn.commit();

        dml = "UPSERT INTO target(region_name, doubles, doubles2) SELECT region_name, doubles,ARRAY_FILL(4.5,5) FROM source";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT doubles, doubles2 FROM target");
        assertTrue(rs.next());

        Double[] doubles = new Double[]{3.4, 3.4, 3.4};
        Double[] doubles2 = new Double[]{4.5, 4.5, 4.5, 4.5, 4.5};
        Array array = conn.createArrayOf("DOUBLE", doubles);
        Array array2 = conn.createArrayOf("DOUBLE", doubles2);

        assertEquals(array, rs.getArray(1));
        assertEquals(array2, rs.getArray(2));
        assertTrue(rs.next());

        doubles = new Double[]{2.3, 2.3, 2.3};
        array = conn.createArrayOf("DOUBLE", doubles);

        assertEquals(array, rs.getArray(1));
        assertEquals(array2, rs.getArray(2));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayFillFunctionWithUpsertSelect2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String source = generateUniqueName();
        String ddl =
            "CREATE TABLE " + source + " (region_name VARCHAR PRIMARY KEY,varchars VARCHAR[])";
        conn.createStatement().execute(ddl);

        String target = generateUniqueName();
        ddl = "CREATE TABLE " + target
            + " (region_name VARCHAR PRIMARY KEY,varchars VARCHAR[],varchars2 VARCHAR[])";
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + source
            + "(region_name,varchars) VALUES('SF Bay Area',ARRAY_FILL('foo',3))";
        conn.createStatement().execute(dml);

        dml = "UPSERT INTO " + source
            + "(region_name,varchars) VALUES('SF Bay Area2',ARRAY_FILL('hello',3))";
        conn.createStatement().execute(dml);
        conn.commit();

        dml =
            "UPSERT INTO " + target
                + "(region_name, varchars, varchars2) SELECT region_name, varchars,ARRAY_FILL(':-)',5) FROM "
                + source;
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT varchars, varchars2 FROM " + target);
        assertTrue(rs.next());

        String[] strings = new String[]{"foo", "foo", "foo"};
        String[] strings2 = new String[]{":-)", ":-)", ":-)", ":-)", ":-)"};
        Array array = conn.createArrayOf("VARCHAR", strings);
        Array array2 = conn.createArrayOf("VARCHAR", strings2);

        assertEquals(array, rs.getArray(1));
        assertEquals(array2, rs.getArray(2));
        assertTrue(rs.next());

        strings = new String[]{"hello", "hello", "hello"};
        array = conn.createArrayOf("VARCHAR", strings);

        assertEquals(array, rs.getArray(1));
        assertEquals(array2, rs.getArray(2));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayFillFunctionInWhere1() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT region_name FROM " + tableName + " WHERE ARRAY[12, 12, 12, 12]=ARRAY_FILL(12,4)");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayFillFunctionInWhere2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT region_name FROM " + tableName + " WHERE \"varchar\"=ANY(ARRAY_FILL('foo',3))");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayFillFunctionInWhere3() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT region_name FROM " + tableName
            + " WHERE ARRAY['2345', '2345', '2345', '2345']=ARRAY_FILL('2345', 4)");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayFillFunctionInWhere4() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT region_name FROM " + tableName
            + " WHERE ARRAY[23.45, 23.45, 23.45]=ARRAY_FILL(23.45, 3)");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayFillFunctionInWhere5() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT region_name FROM " + tableName
            + " WHERE ARRAY['foo','foo','foo','foo','foo']=ARRAY_FILL(\"varchar\",5)");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayFillFunctionInWhere6() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT region_name FROM " + tableName + " WHERE varchars2=ARRAY_FILL('hello',3)");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayFillFunctionInWhere7() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        

        ResultSet rs;
        rs = conn.createStatement().executeQuery(
            "SELECT region_name FROM " + tableName + " WHERE ARRAY[2,2,2,2]=ARRAY_FILL(2,4)");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }
}
