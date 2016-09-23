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

import org.apache.phoenix.schema.TypeMismatchException;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ArrayConcatFunctionIT extends ParallelStatsDisabledIT {

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

    @Test
    public void testArrayConcatFunctionVarchar() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_CAT(varchars,varchars) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String[] strings = new String[]{"2345", "46345", "23234", "2345", "46345", "23234"};

        Array array = conn.createArrayOf("VARCHAR", strings);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayConcatFunctionInteger() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_CAT(integers,integers) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Integer[] integers = new Integer[]{2345, 46345, 23234, 456, 2345, 46345, 23234, 456};

        Array array = conn.createArrayOf("INTEGER", integers);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayConcatFunctionDouble() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_CAT(doubles,doubles) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Double[] doubles = new Double[]{23.45, 46.345, 23.234, 45.6, 5.78, 23.45, 46.345, 23.234, 45.6, 5.78};

        Array array = conn.createArrayOf("DOUBLE", doubles);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayConcatFunctionDouble2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_CAT(doubles,ARRAY[23]) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Double[] doubles = new Double[]{23.45, 46.345, 23.234, 45.6, 5.78, new Double(23)};

        Array array = conn.createArrayOf("DOUBLE", doubles);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayConcatFunctionBigint() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_CAT(bigints,bigints) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Long[] longs = new Long[]{12l, 34l, 56l, 78l, 910l, 12l, 34l, 56l, 78l, 910l};

        Array array = conn.createArrayOf("BIGINT", longs);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayConcatFunctionChar() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_CAT(chars,chars) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String[] strings = new String[]{"a", "bbbb", "c", "ddd", "e", "a", "bbbb", "c", "ddd", "e"};

        Array array = conn.createArrayOf("CHAR", strings);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayConcatFunctionChar3() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_CAT(chars,chars2) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String[] strings = new String[]{"a", "bbbb", "c", "ddd", "e", "a", "bbbb", "c", "ddd", "e", "foo"};

        Array array = conn.createArrayOf("CHAR", strings);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test(expected = TypeMismatchException.class)
    public void testArrayConcatFunctionIntToCharArray() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_CAT(varchars,ARRAY[23,45]) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
    }

    @Test(expected = TypeMismatchException.class)
    public void testArrayConcatFunctionVarcharToIntegerArray() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_CAT(integers,ARRAY['a', 'b']) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");

    }

    @Test(expected = SQLException.class)
    public void testArrayConcatFunctionChar2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_CAT(chars,ARRAY['facfacfacfacfacfacfac','facfacfacfacfacfacfac']) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        rs.next();
        rs.getArray(1);
    }

    @Test
    public void testArrayConcatFunctionIntegerArrayToDoubleArray() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_CAT(doubles,ARRAY[45, 55]) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Double[] doubles = new Double[]{23.45, 46.345, 23.234, 45.6, 5.78, 45.0, 55.0};

        Array array = conn.createArrayOf("DOUBLE", doubles);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayConcatFunctionWithNestedFunctions1() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_CAT(ARRAY[23,45],ARRAY[integers[1],integers[1]]) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Integer[] integers = new Integer[]{23, 45, 2345, 2345};

        Array array = conn.createArrayOf("INTEGER", integers);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayConcatFunctionWithNestedFunctions2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_CAT(integers,ARRAY[ARRAY_ELEM(ARRAY[2,4],1),ARRAY_ELEM(ARRAY[2,4],2)]) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Integer[] integers = new Integer[]{2345, 46345, 23234, 456, 2, 4};

        Array array = conn.createArrayOf("INTEGER", integers);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayConcatFunctionWithNestedFunctions3() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_CAT(doubles,ARRAY[ARRAY_ELEM(doubles, 1), ARRAY_ELEM(doubles, 1)]) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Double[] doubles = new Double[]{23.45, 46.345, 23.234, 45.6, 5.78, 23.45, 23.45};

        Array array = conn.createArrayOf("DOUBLE", doubles);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayConcatFunctionWithUpsert1() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();

        String ddl = "CREATE TABLE " + tableName + " (region_name VARCHAR PRIMARY KEY,varchars VARCHAR[])";
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + tableName + "(region_name,varchars) VALUES('SF Bay Area',ARRAY_CAT(ARRAY['hello','world'],ARRAY[':-)']))";
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
    public void testArrayConcatFunctionWithUpsert2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();

        String ddl = "CREATE TABLE " + tableName + " (region_name VARCHAR PRIMARY KEY,integers INTEGER[])";
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + tableName + "(region_name,integers) VALUES('SF Bay Area',ARRAY_CAT(ARRAY[4,5],ARRAY[6, 7]))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT integers FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Integer[] integers = new Integer[]{4, 5, 6, 7};

        Array array = conn.createArrayOf("INTEGER", integers);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayConcatFunctionWithUpsert3() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();

        String ddl = "CREATE TABLE " + tableName + " (region_name VARCHAR PRIMARY KEY,doubles DOUBLE[])";
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + tableName + "(region_name,doubles) VALUES('SF Bay Area',ARRAY_CAT(ARRAY[5.67,7.87],ARRAY[9.0, 8.0]))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT doubles FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Double[] doubles = new Double[]{5.67, 7.87, new Double(9), new Double(8)};

        Array array = conn.createArrayOf("DOUBLE", doubles);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayConcatFunctionWithUpsertSelect1() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String sourceTableName = generateUniqueName();
        String targetTableName = generateUniqueName();
        String ddl = "CREATE TABLE " + sourceTableName + " (region_name VARCHAR PRIMARY KEY,doubles DOUBLE[])";
        conn.createStatement().execute(ddl);

        ddl = "CREATE TABLE " + targetTableName + " (region_name VARCHAR PRIMARY KEY,doubles DOUBLE[])";
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + sourceTableName + "(region_name,doubles) VALUES('SF Bay Area',ARRAY_CAT(ARRAY[5.67,7.87],ARRAY[9.0, 4.0]))";
        conn.createStatement().execute(dml);

        dml = "UPSERT INTO " + sourceTableName + "(region_name,doubles) VALUES('SF Bay Area2',ARRAY_CAT(ARRAY[56.7,7.87],ARRAY[9.2, 3.4]))";
        conn.createStatement().execute(dml);
        conn.commit();

        dml = "UPSERT INTO " + targetTableName + "(region_name, doubles) SELECT region_name, ARRAY_CAT(doubles,doubles) FROM " + sourceTableName ;
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT doubles FROM " + targetTableName );
        assertTrue(rs.next());

        Double[] doubles = new Double[]{5.67, 7.87, new Double(9), new Double(4), 5.67, 7.87, new Double(9), new Double(4)};
        Array array = conn.createArrayOf("DOUBLE", doubles);

        assertEquals(array, rs.getArray(1));
        assertTrue(rs.next());

        doubles = new Double[]{56.7, 7.87, new Double(9.2), new Double(3.4), 56.7, 7.87, new Double(9.2), new Double(3.4)};
        array = conn.createArrayOf("DOUBLE", doubles);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayConcatFunctionWithUpsertSelect2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String sourceTableName = generateUniqueName();
        String targetTableName = generateUniqueName();
        String ddl = "CREATE TABLE " + sourceTableName + " (region_name VARCHAR PRIMARY KEY,varchars VARCHAR[])";
        conn.createStatement().execute(ddl);

        ddl = "CREATE TABLE " + targetTableName + " (region_name VARCHAR PRIMARY KEY,varchars VARCHAR[])";
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + sourceTableName + "(region_name,varchars) VALUES('SF Bay Area',ARRAY_CAT(ARRAY['abcd','b'],ARRAY['c', 'd']))";
        conn.createStatement().execute(dml);

        dml = "UPSERT INTO " + sourceTableName + "(region_name,varchars) VALUES('SF Bay Area2',ARRAY_CAT(ARRAY['d','fgh'],ARRAY['something','something']))";
        conn.createStatement().execute(dml);
        conn.commit();

        dml = "UPSERT INTO " + targetTableName + "(region_name, varchars) SELECT region_name, ARRAY_CAT(varchars,varchars) FROM " + sourceTableName ;
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT varchars FROM " + targetTableName );
        assertTrue(rs.next());

        String[] strings = new String[]{"abcd", "b", "c", "d", "abcd", "b", "c", "d"};
        Array array = conn.createArrayOf("VARCHAR", strings);

        assertEquals(array, rs.getArray(1));
        assertTrue(rs.next());

        strings = new String[]{"d", "fgh", "something", "something", "d", "fgh", "something", "something"};
        array = conn.createArrayOf("VARCHAR", strings);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayConcatFunctionInWhere1() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT region_name FROM " + tableName + " WHERE ARRAY[2345,46345,23234,456,123]=ARRAY_CAT(integers,ARRAY[123])");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayConcatFunctionInWhere2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT region_name FROM " + tableName + " WHERE varchars[1]=ANY(ARRAY_CAT(ARRAY['2345','46345','23234'],ARRAY['1234']))");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayConcatFunctionInWhere3() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT region_name FROM " + tableName + " WHERE ARRAY['2345','46345','23234','1234','234']=ARRAY_CAT(ARRAY['2345','46345','23234'],ARRAY['1234', '234'])");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayConcatFunctionInWhere4() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT region_name FROM " + tableName + " WHERE ARRAY[23.45,4634.5,2.3234,123.4,12.0]=ARRAY_CAT(ARRAY[23.45,4634.5,2.3234],ARRAY[123.4,12.0])");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayConcatFunctionInWhere5() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT region_name FROM " + tableName + " WHERE ARRAY['2345','46345','23234','foo','foo']=ARRAY_CAT(varchars,ARRAY['foo','foo'])");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayConcatFunctionInWhere6() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT region_name FROM " + tableName + " WHERE chars2=ARRAY_CAT(chars,ARRAY['foo'])");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayConcatFunctionInWhere7() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT region_name FROM " + tableName + " WHERE ARRAY[2,3,4,5]=ARRAY_CAT(ARRAY[2,3],ARRAY[4,5])");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayConcatFunctionWithNulls1() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        PreparedStatement st = conn.prepareStatement("SELECT ARRAY_CAT(?,?) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        Array array1 = conn.createArrayOf("VARCHAR", new Object[]{"a", "b", "c", null});
        st.setArray(1, array1);
        Array array2 = conn.createArrayOf("VARCHAR", new Object[]{"a", "b", "c"});
        st.setArray(2, array2);
        rs = st.executeQuery();
        assertTrue(rs.next());

        Array expected = conn.createArrayOf("VARCHAR", new Object[]{"a", "b", "c", null, "a", "b", "c"});

        assertEquals(expected, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayConcatFunctionWithNulls2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        PreparedStatement st = conn.prepareStatement("SELECT ARRAY_CAT(?,?) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        Array array1 = conn.createArrayOf("VARCHAR", new Object[]{"a", "b", "c"});
        st.setArray(1, array1);
        Array array2 = conn.createArrayOf("VARCHAR", new Object[]{null, "a", "b", "c"});
        st.setArray(2, array2);
        rs = st.executeQuery();
        assertTrue(rs.next());

        Array expected = conn.createArrayOf("VARCHAR", new Object[]{"a", "b", "c", null, "a", "b", "c"});

        assertEquals(expected, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayConcatFunctionWithNulls3() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        PreparedStatement st = conn.prepareStatement("SELECT ARRAY_CAT(?,?) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        Array array1 = conn.createArrayOf("VARCHAR", new Object[]{"a", "b", "c", null});
        st.setArray(1, array1);
        Array array2 = conn.createArrayOf("VARCHAR", new Object[]{null, "a", "b", "c"});
        st.setArray(2, array2);
        rs = st.executeQuery();
        assertTrue(rs.next());

        Array expected = conn.createArrayOf("VARCHAR", new Object[]{"a", "b", "c", null, null, "a", "b", "c"});

        assertEquals(expected, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayConcatFunctionWithNulls4() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        PreparedStatement st = conn.prepareStatement("SELECT ARRAY_CAT(?,?) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        Array array1 = conn.createArrayOf("VARCHAR", new Object[]{null, "a", null, "b", "c", null, null});
        st.setArray(1, array1);
        Array array2 = conn.createArrayOf("VARCHAR", new Object[]{null, null, "a", null, "b", null, "c", null});
        st.setArray(2, array2);
        rs = st.executeQuery();
        assertTrue(rs.next());

        Array expected = conn.createArrayOf("VARCHAR", new Object[]{null, "a", null, "b", "c", null, null, null, null, "a", null, "b", null, "c", null});

        assertEquals(expected, rs.getArray(1));
        assertFalse(rs.next());
    }
}
