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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.phoenix.schema.TypeMismatchException;
import org.junit.Test;

public class ArrayRemoveFunctionIT extends ParallelStatsDisabledIT {
    private String initTables(Connection conn) throws Exception {
        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE " + tableName
                + " (region_name VARCHAR PRIMARY KEY,varchars VARCHAR[],integers INTEGER[],doubles DOUBLE[],bigints BIGINT[],"
                + "chars CHAR(15)[],double1 DOUBLE,char1 CHAR(17),nullcheck INTEGER,chars2 CHAR(15)[], nullVarchar VARCHAR[], nullBigInt BIGINT[])";
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
    public void testEmptyArrayModification() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_REMOVE(nullVarChar,'34567') FROM " + tableName + " LIMIT 1");
        assertTrue(rs.next());

        assertNull(rs.getArray(1));
        assertFalse(rs.next());
    }
    
    @Test
    public void testArrayRemoveFunctionVarchar() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_REMOVE(varchars,'23234') FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String[] strings = new String[]{"2345", "46345"};

        Array array = conn.createArrayOf("VARCHAR", strings);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayRemoveFunctionInteger() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_REMOVE(integers,456) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Integer[] integers = new Integer[]{2345, 46345, 23234};

        Array array = conn.createArrayOf("INTEGER", integers);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayRemoveFunctionDouble() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_REMOVE(doubles,double1) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Double[] doubles = new Double[]{46.345, 23.234, 45.6, 5.78};

        Array array = conn.createArrayOf("DOUBLE", doubles);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayRemoveFunctionBigint() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_REMOVE(bigints,56) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Long[] longs = new Long[]{12l, 34l, 78l, 910l};

        Array array = conn.createArrayOf("BIGINT", longs);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayRemoveFunctionChar() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_REMOVE(chars,'ddd') FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String[] strings = new String[]{"a", "bbbb", "c", "e"};

        Array array = conn.createArrayOf("CHAR", strings);

        Array array2 = rs.getArray(1);
		assertEquals(array, array2);
        assertFalse(rs.next());
    }

    @Test(expected = TypeMismatchException.class)
    public void testArrayRemoveFunctionIntToCharArray() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        conn.createStatement().executeQuery("SELECT ARRAY_REMOVE(varchars,234) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
    }

    @Test(expected = TypeMismatchException.class)
    public void testArrayRemoveFunctionVarcharToIntegerArray() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        conn.createStatement().executeQuery("SELECT ARRAY_REMOVE(integers,'234') FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
    }

    @Test
    public void testArrayRemoveFunctionWithNestedFunctions1() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_REMOVE(ARRAY[23,2345],integers[1]) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Integer[] integers = new Integer[]{23};

        Array array = conn.createArrayOf("INTEGER", integers);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayRemoveFunctionWithNestedFunctions2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_REMOVE(integers,ARRAY_ELEM(ARRAY[2345,4],1)) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Integer[] integers = new Integer[]{46345, 23234, 456};

        Array array = conn.createArrayOf("INTEGER", integers);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayRemoveFunctionWithUpsert1() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE " + tableName + " (region_name VARCHAR PRIMARY KEY,varchars VARCHAR[])";
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + tableName + "(region_name,varchars) VALUES('SF Bay Area',ARRAY_REMOVE(ARRAY['hello','world'],'world'))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT varchars FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String[] strings = new String[]{"hello"};

        Array array = conn.createArrayOf("VARCHAR", strings);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayRemoveFunctionWithUpsert2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE " + tableName + " (region_name VARCHAR PRIMARY KEY,integers INTEGER[])";
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + tableName + "(region_name,integers) VALUES('SF Bay Area',ARRAY_REMOVE(ARRAY[4,5],5))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT integers FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Integer[] integers = new Integer[]{4};

        Array array = conn.createArrayOf("INTEGER", integers);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayRemoveFunctionWithUpsertSelect1() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String sourceTableName = generateUniqueName();
        String targetTableName = generateUniqueName();

        String ddl = "CREATE TABLE " + sourceTableName + " (region_name VARCHAR PRIMARY KEY,doubles DOUBLE[])";
        conn.createStatement().execute(ddl);

        ddl = "CREATE TABLE " + targetTableName + " (region_name VARCHAR PRIMARY KEY,doubles DOUBLE[])";
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + sourceTableName + "(region_name,doubles) VALUES('SF Bay Area',ARRAY_APPEND(ARRAY[5.67,7.87],9))";
        conn.createStatement().execute(dml);

        dml = "UPSERT INTO " + sourceTableName + "(region_name,doubles) VALUES('SF Bay Area2',ARRAY_APPEND(ARRAY[56.7,7.87],9))";
        conn.createStatement().execute(dml);
        conn.commit();

        dml = "UPSERT INTO " + targetTableName + "(region_name, doubles) SELECT region_name, ARRAY_REMOVE(doubles,9) FROM " + sourceTableName ;
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT doubles FROM " + targetTableName );
        assertTrue(rs.next());

        Double[] doubles = new Double[]{5.67, 7.87};
        Array array = conn.createArrayOf("DOUBLE", doubles);

        assertEquals(array, rs.getArray(1));
        assertTrue(rs.next());

        doubles = new Double[]{56.7, 7.87};
        array = conn.createArrayOf("DOUBLE", doubles);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayRemoveFunctionInWhere1() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT region_name FROM " + tableName + " WHERE ARRAY[2345,46345,23234]=ARRAY_REMOVE(integers,456)");
        assertTrue(rs.next());

        assertEquals("SF Bay Area", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayRemoveFunctionVarcharWithNull() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_REMOVE(varchars,NULL) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String[] strings = new String[]{"2345", "46345", "23234"};

        Array array = conn.createArrayOf("VARCHAR", strings);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayRemoveFunctionDoublesWithNull() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_REMOVE(doubles,NULL) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Double[] doubles = new Double[]{23.45, 46.345, 23.234, 45.6, 5.78};

        Array array = conn.createArrayOf("DOUBLE", doubles);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayRemoveFunctionCharsWithNull() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_REMOVE(chars,NULL) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        String[] strings = new String[]{"a", "bbbb", "c", "ddd", "e"};

        Array array = conn.createArrayOf("CHAR", strings);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

    @Test
    public void testArrayRemoveFunctionWithNull() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTables(conn);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_REMOVE(integers,nullcheck) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
        assertTrue(rs.next());

        Integer[] integers = new Integer[]{2345, 46345, 23234, 456};

        Array array = conn.createArrayOf("INTEGER", integers);

        assertEquals(array, rs.getArray(1));
        assertFalse(rs.next());
    }

}
