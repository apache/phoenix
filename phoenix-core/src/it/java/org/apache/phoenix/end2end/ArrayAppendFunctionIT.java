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
import org.junit.Test;

public class ArrayAppendFunctionIT extends ParallelStatsDisabledIT {
    private String initTables(Connection conn) throws Exception {
        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE " + tableName + " (region_name VARCHAR PRIMARY KEY,varchars VARCHAR[],integers INTEGER[],doubles DOUBLE[],bigints BIGINT[],chars CHAR(15)[],double1 DOUBLE,char1 CHAR(17),nullcheck INTEGER,chars2 CHAR(15)[])";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO " + tableName + "(region_name,varchars,integers,doubles,bigints,chars,double1,char1,nullcheck,chars2) VALUES('SF Bay Area'," +
                "ARRAY['2345','46345','23234']," +
                "ARRAY[2345,46345,23234,456]," +
                "ARRAY[23.45,46.345,23.234,45.6,5.78]," +
                "ARRAY[12,34,56,78,910]," +
                "ARRAY['a','bbbb','c','ddd','e']," +
                "23.45," +
                "'wert'," +
                "NULL," +
                "ARRAY['a','bbbb','c','ddd','e','foo']" +
                ")";
        PreparedStatement stmt = conn.prepareStatement(dml);
        stmt.execute();
        conn.commit();
        return tableName;
    }

    private String initTablesDesc(Connection conn, String type, String val) throws Exception {
        String tableName = generateUniqueName();
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
        return tableName;

    }

    @Test
    public void testArrayAppendFunctionVarchar() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_APPEND(varchars,'34567') FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String[] strings = new String[]{"2345", "46345", "23234", "34567"};

        Array array = conn.createArrayOf("VARCHAR", strings);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayAppendFunctionInteger() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_APPEND(integers,1234) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Integer[] integers = new Integer[]{2345, 46345, 23234, 456, 1234};

        Array array = conn.createArrayOf("INTEGER", integers);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayAppendFunctionDouble() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_APPEND(doubles,double1) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Double[] doubles = new Double[]{23.45, 46.345, 23.234, 45.6, 5.78, 23.45};

        Array array = conn.createArrayOf("DOUBLE", doubles);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayAppendFunctionDouble2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_APPEND(doubles,23) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Double[] doubles = new Double[]{23.45, 46.345, 23.234, 45.6, 5.78, new Double(23)};

        Array array = conn.createArrayOf("DOUBLE", doubles);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayAppendFunctionBigint() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_APPEND(bigints,1112) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Long[] longs = new Long[]{12l, 34l, 56l, 78l, 910l, 1112l};

        Array array = conn.createArrayOf("BIGINT", longs);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayAppendFunctionChar() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_APPEND(chars,'fac') FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String[] strings = new String[]{"a", "bbbb", "c", "ddd", "e", "fac"};

        Array array = conn.createArrayOf("CHAR", strings);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test(expected = TypeMismatchException.class)
    public void testArrayAppendFunctionIntToCharArray() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_APPEND(varchars,234) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
    }

    @Test(expected = TypeMismatchException.class)
    public void testArrayAppendFunctionVarcharToIntegerArray() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_APPEND(integers,'234') FROM " + tableName + " WHERE region_name = 'SF Bay Area'");

    }

    @Test(expected = SQLException.class)
    public void testArrayAppendFunctionChar2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_APPEND(chars,'facfacfacfacfacfacfac') FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        rs.next();
        rs.getArray(1);
    }

    @Test
    public void testArrayAppendFunctionIntegerToDoubleArray() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_APPEND(doubles,45) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Double[] doubles = new Double[]{23.45, 46.345, 23.234, 45.6, 5.78, 45.0};

        Array array = conn.createArrayOf("DOUBLE", doubles);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayAppendFunctionWithNestedFunctions1() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_APPEND(ARRAY[23,45],integers[1]) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Integer[] integers = new Integer[]{23, 45, 2345};

        Array array = conn.createArrayOf("INTEGER", integers);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayAppendFunctionWithNestedFunctions2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_APPEND(integers,ARRAY_ELEM(ARRAY[2,4],1)) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Integer[] integers = new Integer[]{2345, 46345, 23234, 456, 2};

        Array array = conn.createArrayOf("INTEGER", integers);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayAppendFunctionWithNestedFunctions3() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_APPEND(doubles,ARRAY_ELEM(doubles,2)) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Double[] doubles = new Double[]{23.45, 46.345, 23.234, 45.6, 5.78, 46.345};

        Array array = conn.createArrayOf("DOUBLE", doubles);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayAppendFunctionWithUpsert1() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE " + tableName + " (region_name VARCHAR PRIMARY KEY,varchars VARCHAR[])";
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + tableName + "(region_name,varchars) VALUES('SF Bay Area',ARRAY_APPEND(ARRAY['hello','world'],':-)'))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT varchars FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String[] strings = new String[]{"hello", "world", ":-)"};

        Array array = conn.createArrayOf("VARCHAR", strings);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayAppendFunctionWithUpsert2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE " + tableName + " (region_name VARCHAR PRIMARY KEY,integers INTEGER[])";
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + tableName + "(region_name,integers) VALUES('SF Bay Area',ARRAY_APPEND(ARRAY[4,5],6))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT integers FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Integer[] integers = new Integer[]{4, 5, 6};

        Array array = conn.createArrayOf("INTEGER", integers);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayAppendFunctionWithUpsert3() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE " + tableName + " (region_name VARCHAR PRIMARY KEY,doubles DOUBLE[])";
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + tableName + "(region_name,doubles) VALUES('SF Bay Area',ARRAY_APPEND(ARRAY[5.67,7.87],9.0))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT doubles FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Double[] doubles = new Double[]{5.67, 7.87, new Double(9)};

        Array array = conn.createArrayOf("DOUBLE", doubles);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayAppendFunctionWithUpsertSelect1() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String sourceTableName = generateUniqueName();
        String targetTableName = generateUniqueName();

        String ddl = "CREATE TABLE " + sourceTableName + " (region_name VARCHAR PRIMARY KEY,doubles DOUBLE[])";
        conn.createStatement().execute(ddl);

        ddl = "CREATE TABLE " + targetTableName + " (region_name VARCHAR PRIMARY KEY,doubles DOUBLE[])";
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + sourceTableName + "(region_name,doubles) VALUES('SF Bay Area',ARRAY_APPEND(ARRAY[5.67,7.87],9.0))";
        conn.createStatement().execute(dml);

        dml = "UPSERT INTO " + sourceTableName + "(region_name,doubles) VALUES('SF Bay Area2',ARRAY_APPEND(ARRAY[56.7,7.87],9.2))";
        conn.createStatement().execute(dml);
        conn.commit();

        dml = "UPSERT INTO " + targetTableName + "(region_name, doubles) SELECT region_name, ARRAY_APPEND(doubles,5) FROM " + sourceTableName ;
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT doubles FROM " + targetTableName );
        assertTrue(rs.next());

        Double[] doubles = new Double[]{5.67, 7.87, new Double(9), new Double(5)};
        Array array = conn.createArrayOf("DOUBLE", doubles);

        assertEquals(array, rs.getArray(1));
        assertTrue(rs.next());

        doubles = new Double[]{56.7, 7.87, new Double(9.2), new Double(5)};
        array = conn.createArrayOf("DOUBLE", doubles);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayAppendFunctionWithUpsertSelect2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String sourceTableName = generateUniqueName();
        String targetTableName = generateUniqueName();
        String ddl = "CREATE TABLE " + sourceTableName + " (region_name VARCHAR PRIMARY KEY,varchars VARCHAR[])";
        conn.createStatement().execute(ddl);

        ddl = "CREATE TABLE " + targetTableName + " (region_name VARCHAR PRIMARY KEY,varchars VARCHAR[])";
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + sourceTableName + "(region_name,varchars) VALUES('SF Bay Area',ARRAY_APPEND(ARRAY['abcd','b'],'c'))";
        conn.createStatement().execute(dml);

        dml = "UPSERT INTO " + sourceTableName + "(region_name,varchars) VALUES('SF Bay Area2',ARRAY_APPEND(ARRAY['d','fgh'],'something'))";
        conn.createStatement().execute(dml);
        conn.commit();

        dml = "UPSERT INTO " + targetTableName + "(region_name, varchars) SELECT region_name, ARRAY_APPEND(varchars,'stu') FROM " + sourceTableName ;
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT varchars FROM " + targetTableName );
        assertTrue(rs.next());

        String[] strings = new String[]{"abcd", "b", "c", "stu"};
        Array array = conn.createArrayOf("VARCHAR", strings);

        assertEquals(array, rs.getArray(1));
        assertTrue(rs.next());

        strings = new String[]{"d", "fgh", "something", "stu"};
        array = conn.createArrayOf("VARCHAR", strings);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayAppendFunctionInWhere1() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT region_name FROM " + tableName + " WHERE ARRAY[2345,46345,23234,456,123]=ARRAY_APPEND(integers,123)");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayAppendFunctionInWhere2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT region_name FROM " + tableName + " WHERE varchars[1]=ANY(ARRAY_APPEND(ARRAY['2345','46345','23234'],'1234'))");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayAppendFunctionInWhere3() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT region_name FROM " + tableName + " WHERE ARRAY['2345','46345','23234','1234']=ARRAY_APPEND(ARRAY['2345','46345','23234'],'1234')");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayAppendFunctionInWhere4() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT region_name FROM " + tableName + " WHERE ARRAY[23.45,4634.5,2.3234,123.4]=ARRAY_APPEND(ARRAY[23.45,4634.5,2.3234],123.4)");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayAppendFunctionInWhere5() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT region_name FROM " + tableName + " WHERE ARRAY['2345','46345','23234','foo']=ARRAY_APPEND(varchars,'foo')");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayAppendFunctionInWhere6() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT region_name FROM " + tableName + " WHERE chars2=ARRAY_APPEND(chars,'foo')");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayAppendFunctionInWhere7() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT region_name FROM " + tableName + " WHERE ARRAY[2,3,4]=ARRAY_APPEND(ARRAY[2,3],4)");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayAppendFunctionVarcharWithNull() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_APPEND(varchars,NULL) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String[] strings = new String[]{"2345", "46345", "23234"};

        Array array = conn.createArrayOf("VARCHAR", strings);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayAppendFunctionDoublesWithNull() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_APPEND(doubles,NULL) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Double[] doubles = new Double[]{23.45, 46.345, 23.234, 45.6, 5.78};

        Array array = conn.createArrayOf("DOUBLE", doubles);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayAppendFunctionCharsWithNull() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_APPEND(chars,NULL) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String[] strings = new String[]{"a", "bbbb", "c", "ddd", "e"};

        Array array = conn.createArrayOf("CHAR", strings);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayAppendFunctionWithNull() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_APPEND(integers,nullcheck) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Integer[] integers = new Integer[]{2345, 46345, 23234, 456};

        Array array = conn.createArrayOf("INTEGER", integers);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test(expected = SQLException.class)
    public void testArrayAppendFunctionCharLimitCheck() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_APPEND(chars,char1) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String[] strings = new String[]{"a", "bbbb", "c", "ddd", "e", "wert"};

        Array array = conn.createArrayOf("CHAR", strings);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayAppendFunctionIntegerDesc() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTablesDesc(conn, "INTEGER", "23");

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_APPEND(integers,pk) FROM " + tableName + "");
        assertTrue(rs.next());

        Integer[] integers = new Integer[]{2345, 46345, 23234, 456, 23};

        Array array = conn.createArrayOf("INTEGER", integers);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());

    }

    @Test
    public void testArrayAppendFunctionVarcharDesc() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTablesDesc(conn, "VARCHAR", "'e'");

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_APPEND(varchars,pk) FROM " + tableName + "");
        assertTrue(rs.next());

        String[] strings = new String[]{"2345", "46345", "23234", "e"};

        Array array = conn.createArrayOf("VARCHAR", strings);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayAppendFunctionBigIntDesc() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTablesDesc(conn, "BIGINT", "1112");
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_APPEND(bigints,pk) FROM " + tableName );
        assertTrue(rs.next());

        Long[] longs = new Long[]{12l, 34l, 56l, 78l, 910l, 1112l};

        Array array = conn.createArrayOf("BIGINT", longs);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayAppendFunctionBooleanDesc() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTablesDesc(conn, "BOOLEAN", "false");
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_APPEND(bools,pk) FROM " + tableName );
        assertTrue(rs.next());

        Boolean[] booleans = new Boolean[]{true, false, false};

        Array array = conn.createArrayOf("BOOLEAN", booleans);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }
}
