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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class Array3IT extends ArrayIT {

    @Test
    public void testArrayConstructorWithMultipleRows5() throws Exception {

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        String ddl = "CREATE TABLE   " + table + "  (region_name VARCHAR PRIMARY KEY, a VARCHAR, b VARCHAR)";
        conn.createStatement().execute(ddl);
        conn.commit();
        conn.close();
        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO   " + table + " (region_name, a, b) VALUES('a', 'foo', 'abc')");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO   " + table + " (region_name, a, b) VALUES('b', 'abc', 'dfg')");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO   " + table + " (region_name, a, b) VALUES('c', 'foo', 'abc')");
        stmt.execute();
        conn.commit();
        conn.close();
        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT ARRAY_APPEND(ARRAY[a,b], 'oo') from   " + table);
        assertTrue(rs.next());
        Array arr = conn.createArrayOf("VARCHAR", new Object[]{"foo", "abc", "oo"});
        assertEquals(arr, rs.getArray(1));
        rs.next();
        arr = conn.createArrayOf("VARCHAR", new Object[]{"abc", "dfg", "oo"});
        assertEquals(arr, rs.getArray(1));
        rs.next();
        arr = conn.createArrayOf("VARCHAR", new Object[]{"foo", "abc", "oo"});
        assertEquals(arr, rs.getArray(1));
        rs.next();
    }
    
    @Test
    public void testPKWithDescArray() throws Exception {
        Connection conn;
        PreparedStatement stmt;
        ResultSet rs;

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        conn.createStatement()
                .execute(
                        "CREATE TABLE   " + table + "  ( a VARCHAR ARRAY PRIMARY KEY DESC)\n");
        conn.close();
        
        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("UPSERT INTO   " + table + "  VALUES(?)");
        Array a1 = conn.createArrayOf("VARCHAR", new String[] { "a", "ba" });
        stmt.setArray(1, a1);
        stmt.execute();
        Array a2 = conn.createArrayOf("VARCHAR", new String[] { "a", "c" });
        stmt.setArray(1, a2);
        stmt.execute();
        conn.commit();
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        rs = conn.createStatement().executeQuery("SELECT a FROM   " + table + "  ORDER BY a DESC");
        assertTrue(rs.next());
        assertEquals(a2, rs.getArray(1));
        assertTrue(rs.next());
        assertEquals(a1, rs.getArray(1));
        assertFalse(rs.next());
        conn.close();
        
        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("UPSERT INTO   " + table + "  VALUES(?)");
        Array a3 = conn.createArrayOf("VARCHAR", new String[] { "a", "b" });
        stmt.setArray(1, a3);
        stmt.execute();
        conn.commit();
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        rs = conn.createStatement().executeQuery("SELECT a FROM   " + table + "  ORDER BY a DESC");
        assertTrue(rs.next());
        assertEquals(a2, rs.getArray(1));
        assertTrue(rs.next());
        assertEquals(a1, rs.getArray(1));
        assertTrue(rs.next());
        assertEquals(a3, rs.getArray(1));
        assertFalse(rs.next());
        conn.close();
    }

    @Test
    public void testComparisonOperatorsForDesc1()throws Exception{

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        String ddl = "create table   " + table + "  (k varchar array primary key desc)";
        conn.createStatement().execute(ddl);
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("upsert into   " + table + "  values (array['a', 'c'])");
        stmt.execute();
        conn.commit();
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs;
        stmt = conn.prepareStatement("select * from   " + table + "  where k >= array['a', 'b']");
        rs = stmt.executeQuery();
        assertTrue(rs.next());
    }

    @Test
    public void testComparisonOperatorsForDesc2()throws Exception{

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        String ddl = "create table   " + table + "  (k varchar array primary key desc)";
        conn.createStatement().execute(ddl);
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("upsert into   " + table + "  values (array['a', 'c'])");
        stmt.execute();
        conn.commit();
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs;
        stmt = conn.prepareStatement("select * from   " + table + "  where k >= array['a', 'c']");
        rs = stmt.executeQuery();
        assertTrue(rs.next());
    }

    @Test
    public void testComparisonOperatorsForDesc3()throws Exception{

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        String ddl = "create table   " + table + "  (k varchar array primary key desc)";
        conn.createStatement().execute(ddl);
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("upsert into   " + table + "  values (array['a', 'c'])");
        stmt.execute();
        conn.commit();
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs;
        stmt = conn.prepareStatement("select * from   " + table + "  where k > array['a', 'b']");
        rs = stmt.executeQuery();
        assertTrue(rs.next());
    }

    @Test
    public void testComparisonOperatorsForDesc4()throws Exception{

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        String ddl = "create table   " + table + "  (k varchar array primary key desc)";
        conn.createStatement().execute(ddl);
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("upsert into   " + table + "  values (array['a', 'b'])");
        stmt.execute();
        conn.commit();
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs;
        stmt = conn.prepareStatement("select * from   " + table + "  where k <= array['a', 'c']");
        rs = stmt.executeQuery();
        assertTrue(rs.next());
    }

    @Test
    public void testComparisonOperatorsForDesc5()throws Exception{

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        String ddl = "create table   " + table + "  (k varchar array primary key desc)";
        conn.createStatement().execute(ddl);
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("upsert into   " + table + "  values (array['a', 'b'])");
        stmt.execute();
        conn.commit();
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs;
        stmt = conn.prepareStatement("select * from   " + table + "  where k <= array['a', 'b']");
        rs = stmt.executeQuery();
        assertTrue(rs.next());
    }

    @Test
    public void testComparisonOperatorsForDesc6()throws Exception{

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        String ddl = "create table   " + table + "  (k varchar array primary key desc)";
        conn.createStatement().execute(ddl);
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("upsert into   " + table + "  values (array['a', 'b'])");
        stmt.execute();
        conn.commit();
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs;
        stmt = conn.prepareStatement("select * from   " + table + "  where k < array['a', 'c']");
        rs = stmt.executeQuery();
        assertTrue(rs.next());
    }

    @Test
    public void testComparisonOperatorsForDesc7()throws Exception{

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        String ddl = "create table   " + table + "  (k integer array primary key desc)";
        conn.createStatement().execute(ddl);
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("upsert into   " + table + "  values (array[1, 2])");
        stmt.execute();
        conn.commit();
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs;
        stmt = conn.prepareStatement("select * from   " + table + "  where k < array[1, 4]");
        rs = stmt.executeQuery();
        assertTrue(rs.next());
    }

    @Test
    public void testComparisonOperatorsForDesc8()throws Exception{

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        String ddl = "create table   " + table + "  (k integer array primary key desc)";
        conn.createStatement().execute(ddl);
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("upsert into   " + table + "  values (array[1, 2])");
        stmt.execute();
        conn.commit();
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs;
        stmt = conn.prepareStatement("select * from   " + table + "  where k <= array[1, 2]");
        rs = stmt.executeQuery();
        assertTrue(rs.next());
    }

    @Test
    public void testServerArrayElementProjection1() throws SQLException {

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        String ddl = "CREATE TABLE   " + table + "  (p INTEGER PRIMARY KEY, arr1 INTEGER ARRAY, arr2 VARCHAR ARRAY)";
        conn.createStatement().execute(ddl);
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO   " + table + "  VALUES (1, ARRAY[1, 2], ARRAY['a', 'b'])");
        stmt.execute();
        conn.commit();
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs;
        stmt = conn.prepareStatement("SELECT arr1, arr1[1], arr2[1] FROM   " + table);
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(conn.createArrayOf("INTEGER", new Integer[]{1, 2}), rs.getArray(1));
        assertEquals(1, rs.getInt(2));
        assertEquals("a", rs.getString(3));
    }

    @Test
    public void testServerArrayElementProjection2() throws SQLException {

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        String ddl = "CREATE TABLE   " + table + "  (p INTEGER PRIMARY KEY, arr1 INTEGER ARRAY, arr2 VARCHAR ARRAY, arr3 INTEGER ARRAY)";
        conn.createStatement().execute(ddl);
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO   " + table + "  VALUES (1, ARRAY[1, 2], ARRAY['a', 'b'], ARRAY[2, 3])");
        stmt.execute();
        conn.commit();
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs;
        stmt = conn.prepareStatement("SELECT arr1, arr1[1], arr2[1], arr3[1] from   " + table);
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(conn.createArrayOf("INTEGER", new Integer[]{1, 2}), rs.getArray(1));
        assertEquals(1, rs.getInt(2));
        assertEquals("a", rs.getString(3));
        assertEquals(2, rs.getInt(4));
    }

    @Test
    public void testServerArrayElementProjection3() throws SQLException {

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        String ddl = "CREATE TABLE   " + table + "  (p INTEGER PRIMARY KEY, arr1 INTEGER ARRAY, arr2 VARCHAR ARRAY, arr3 INTEGER ARRAY)";
        conn.createStatement().execute(ddl);
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO   " + table + "  VALUES (1, ARRAY[1, 2], ARRAY['a', 'b'], ARRAY[2, 3])");
        stmt.execute();
        conn.commit();
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs;
        stmt = conn.prepareStatement("SELECT arr1, arr1[1], arr2[1], arr3, arr3[1] from   " + table);
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(conn.createArrayOf("INTEGER", new Integer[]{1, 2}), rs.getArray(1));
        assertEquals(1, rs.getInt(2));
        assertEquals("a", rs.getString(3));
        assertEquals(conn.createArrayOf("INTEGER", new Integer[]{2, 3}), rs.getArray(4));
        assertEquals(2, rs.getInt(5));
    }

    @Test
    public void testServerArrayElementProjection4() throws SQLException {

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        String ddl = "CREATE TABLE   " + table + "  (p INTEGER PRIMARY KEY, arr1 INTEGER ARRAY, arr2 VARCHAR ARRAY, arr3 INTEGER ARRAY)";
        conn.createStatement().execute(ddl);
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO   " + table + "  VALUES (1, ARRAY[1, 2], ARRAY['a', 'b'], ARRAY[2, 3])");
        stmt.execute();
        conn.commit();
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs;
        stmt = conn.prepareStatement("SELECT arr1, arr1[1], arr2[1], ARRAY_APPEND(arr3, 4), arr3[1] from   " + table);
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(conn.createArrayOf("INTEGER", new Integer[]{1, 2}), rs.getArray(1));
        assertEquals(1, rs.getInt(2));
        assertEquals("a", rs.getString(3));
        assertEquals(conn.createArrayOf("INTEGER", new Integer[]{2, 3, 4}), rs.getArray(4));
        assertEquals(2, rs.getInt(5));
    }

    @Test
    public void testServerArrayElementProjection5() throws SQLException {

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        String ddl = "CREATE TABLE   " + table + "  (p INTEGER PRIMARY KEY, arr1 INTEGER ARRAY, arr3 INTEGER ARRAY)";
        conn.createStatement().execute(ddl);
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO   " + table + "  VALUES (1, ARRAY[1, 2], ARRAY[2, 3])");
        stmt.execute();
        conn.commit();
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs;
        stmt = conn.prepareStatement("SELECT arr1, arr1[1], ARRAY_APPEND(arr1, arr3[1]) from   " + table);
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(conn.createArrayOf("INTEGER", new Integer[]{1, 2}), rs.getArray(1));
        assertEquals(1, rs.getInt(2));
        assertEquals(conn.createArrayOf("INTEGER", new Integer[]{1, 2, 2}), rs.getArray(3));
    }

    @Test
    public void testServerArrayElementProjection6() throws SQLException {

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        String ddl = "CREATE TABLE   " + table + "  (p INTEGER PRIMARY KEY, arr1 INTEGER ARRAY, arr2 INTEGER ARRAY)";
        conn.createStatement().execute(ddl);
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO  " + table + "  VALUES (1, ARRAY[1, 2], ARRAY[2, 3])");
        stmt.execute();
        conn.commit();
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs;
        stmt = conn.prepareStatement("SELECT arr1, arr1[1], ARRAY_APPEND(arr1, arr2[1]), p from   " + table);
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(conn.createArrayOf("INTEGER", new Integer[]{1, 2}), rs.getArray(1));
        assertEquals(1, rs.getInt(2));
        assertEquals(conn.createArrayOf("INTEGER", new Integer[]{1, 2, 2}), rs.getArray(3));
        assertEquals(1, rs.getInt(4));
    }

    @Test
    public void testServerArrayElementProjection7() throws SQLException {

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        String ddl = "CREATE TABLE   " + table + "  (p INTEGER PRIMARY KEY, arr1 INTEGER ARRAY, arr2 INTEGER ARRAY)";
        conn.createStatement().execute(ddl);
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO   " + table + "  VALUES (1, ARRAY[1, 2], ARRAY[2, 3])");
        stmt.execute();
        conn.commit();
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs;
        stmt = conn.prepareStatement("SELECT arr1, arr1[1], ARRAY_APPEND(ARRAY_APPEND(arr1, arr2[2]), arr2[1]), p from   " + table);
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(conn.createArrayOf("INTEGER", new Integer[]{1, 2}), rs.getArray(1));
        assertEquals(1, rs.getInt(2));
        assertEquals(conn.createArrayOf("INTEGER", new Integer[]{1, 2, 3, 2}), rs.getArray(3));
        assertEquals(1, rs.getInt(4));
    }

    @Test
    public void testServerArrayElementProjection8() throws SQLException {

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        String ddl = "CREATE TABLE   " + table + "  (p INTEGER PRIMARY KEY, arr1 INTEGER ARRAY, arr2 INTEGER ARRAY)";
        conn.createStatement().execute(ddl);
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO   " + table + "  VALUES (1, ARRAY[1, 2], ARRAY[2, 3])");
        stmt.execute();
        conn.commit();
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs;
        stmt = conn.prepareStatement("SELECT arr1, arr1[1], ARRAY_ELEM(ARRAY_APPEND(arr1, arr2[1]), 1), p, arr2[2] from   " + table);
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(conn.createArrayOf("INTEGER", new Integer[]{1, 2}), rs.getArray(1));
        assertEquals(1, rs.getInt(2));
        assertEquals(1, rs.getInt(3));
        assertEquals(1, rs.getInt(4));
        assertEquals(3, rs.getInt(5));
    }

    @Test
    public void testServerArrayElementProjection9() throws SQLException {

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        String ddl = "CREATE TABLE   " + table + "  (p INTEGER ARRAY PRIMARY KEY, arr1 INTEGER ARRAY, arr2 INTEGER ARRAY)";
        conn.createStatement().execute(ddl);
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO   " + table + "  VALUES (ARRAY[5, 6], ARRAY[1, 2], ARRAY[2, 3])");
        stmt.execute();
        conn.commit();
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs;
        stmt = conn.prepareStatement("SELECT arr1, arr1[1], ARRAY_ELEM(ARRAY_APPEND(arr1, arr2[1]), 1), p, arr2[2] from   " + table);
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(conn.createArrayOf("INTEGER", new Integer[]{1, 2}), rs.getArray(1));
        assertEquals(1, rs.getInt(2));
        assertEquals(1, rs.getInt(3));
        assertEquals(conn.createArrayOf("INTEGER", new Integer[]{5, 6}), rs.getArray(4));
        assertEquals(3, rs.getInt(5));
    }

    @Test
    public void testServerArrayElementProjection10() throws SQLException {

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        String ddl = "CREATE TABLE   " + table + "  (p INTEGER PRIMARY KEY, arr1 INTEGER ARRAY, arr2 INTEGER ARRAY)";
        conn.createStatement().execute(ddl);
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO   " + table + "  VALUES (1, ARRAY[1, 2], ARRAY[2, 3])");
        stmt.execute();
        conn.commit();
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs;
        stmt = conn.prepareStatement("SELECT arr1[1] + 5, arr2[1] FROM   " + table);
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(6, rs.getInt(1));
        assertEquals(2, rs.getInt(2));
    }

    @Test
    public void testServerArrayElementProjection11() throws SQLException {

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        String ddl = "CREATE TABLE   " + table + "  (p INTEGER PRIMARY KEY, arr1 INTEGER ARRAY, arr2 INTEGER ARRAY)";
        conn.createStatement().execute(ddl);
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO   " + table + "  VALUES (1, ARRAY[1, 2], ARRAY[2, 3])");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO   " + table + "  VALUES (2, ARRAY[1, 2], ARRAY[2, 3])");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO   " + table + "  VALUES (3, ARRAY[1, 2], ARRAY[2, 3])");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO   " + table + "  VALUES (4, ARRAY[1, 2], ARRAY[2, 3])");
        stmt.execute();
        conn.commit();
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs;
        stmt = conn.prepareStatement("SELECT CASE WHEN p = 1 THEN arr1[1] WHEN p = 2 THEN arr1[2] WHEN p = 3 THEN arr2[1] ELSE arr2[2] END FROM   " + table);
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertFalse(rs.next());
    }

    @Test
    public void testServerArrayElementProjection12() throws SQLException {

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        String ddl = "CREATE TABLE   " + table + "  (p INTEGER PRIMARY KEY, arr1 INTEGER ARRAY, arr2 INTEGER ARRAY)";
        conn.createStatement().execute(ddl);
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO   " + table + "  VALUES (1, ARRAY[1, 2], ARRAY[2, 3])");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO   " + table + "  VALUES (2, ARRAY[1, 2], ARRAY[2, 3])");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO   " + table + "  VALUES (3, ARRAY[1, 2], ARRAY[2, 3])");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO   " + table + "  VALUES (4, ARRAY[1, 2], ARRAY[2, 3])");
        stmt.execute();
        conn.commit();
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs;
        stmt = conn.prepareStatement("SELECT CASE WHEN p = 1 THEN arr1[1] WHEN p = 2 THEN arr1[2] WHEN p = 3 THEN arr2[1] ELSE arr2[2] END, ARRAY_APPEND(arr1, arr1[1]) FROM   " + table);
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(conn.createArrayOf("INTEGER", new Integer[]{1, 2, 1}), rs.getArray(2));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals(conn.createArrayOf("INTEGER", new Integer[]{1, 2, 1}), rs.getArray(2));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals(conn.createArrayOf("INTEGER", new Integer[]{1, 2, 1}), rs.getArray(2));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertEquals(conn.createArrayOf("INTEGER", new Integer[]{1, 2, 1}), rs.getArray(2));
        assertFalse(rs.next());
    }

    @Test
    public void testServerArrayElementProjection13() throws SQLException {

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        String ddl = "CREATE TABLE   " + table + "  (p INTEGER PRIMARY KEY, arr1 INTEGER ARRAY, arr2 INTEGER ARRAY)";
        conn.createStatement().execute(ddl);
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO   " + table + "  VALUES (1, ARRAY[1, 2], ARRAY[2, 3])");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO  " + table + "  VALUES (2, ARRAY[3, 2], ARRAY[2, 3])");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO  " + table + "  VALUES (3, ARRAY[3, 5], ARRAY[2, 3])");
        stmt.execute();
        stmt = conn.prepareStatement("UPSERT INTO  " + table + "  VALUES (4, ARRAY[3, 5], ARRAY[6, 3])");
        stmt.execute();
        conn.commit();
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs;
        stmt = conn.prepareStatement("SELECT CASE WHEN arr1[1] = 1 THEN 1 WHEN arr1[2] = 2 THEN 2 WHEN arr2[1] = 2 THEN 2 ELSE arr2[2] END FROM   " + table);
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertFalse(rs.next());
    }

    @Test
    public void testServerArrayElementProjection14() throws SQLException {

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        String ddl = "CREATE TABLE   " + table + "  (p INTEGER ARRAY PRIMARY KEY, arr1 INTEGER ARRAY, arr2 INTEGER ARRAY)";
        conn.createStatement().execute(ddl);
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO   " + table + "  VALUES (ARRAY[5, 6], ARRAY[1, 2], ARRAY[2, 3])");
        stmt.execute();
        conn.commit();
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs;
        stmt = conn.prepareStatement("SELECT ARRAY_ELEM(ARRAY_PREPEND(arr2[1], ARRAY_CAT(arr1, ARRAY[arr2[2],3])), 1), arr1[1], ARRAY_ELEM(ARRAY_APPEND(arr1, arr2[1]), 1), p, arr2[2] from   " + table);
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals(1, rs.getInt(2));
        assertEquals(1, rs.getInt(3));
        assertEquals(conn.createArrayOf("INTEGER", new Integer[]{5, 6}), rs.getArray(4));
        assertEquals(3, rs.getInt(5));
    }

    @Test
    public void testCharPrimaryKey() throws SQLException{

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String table = generateUniqueName();
        String ddl = "CREATE TABLE   " + table + " (testCharArray CHAR(3)[], CONSTRAINT test_pk PRIMARY KEY(testCharArray)) DEFAULT_COLUMN_FAMILY='T'";
        conn.createStatement().execute(ddl);
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO   " + table + "  VALUES (ARRAY['aaa', 'bbb', 'ccc'])");
        stmt.execute();
        conn.commit();
        conn.close();

        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs;
        stmt = conn.prepareStatement("SELECT testCharArray from   " + table);
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(conn.createArrayOf("CHAR", new String[]{"aaa", "bbb", "ccc"}), rs.getArray(1));
    }

    @Test
    public void testArrayIndexFunctionForImmutableTable() throws Exception {
        String tableName = generateUniqueName();

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String ddl = "CREATE IMMUTABLE TABLE " + tableName + " (region_name VARCHAR PRIMARY KEY, ZIP VARCHAR ARRAY[10])";
            conn.createStatement().execute(ddl);
        }
        props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().executeUpdate("UPSERT INTO " + tableName + " (region_name,zip) VALUES('SF Bay Area',ARRAY['94115','94030','94125'])");
            conn.commit();
        }
        props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String sql = "SELECT ZIP[2] FROM " + tableName;
            try (ResultSet rs = conn.createStatement().executeQuery(sql)) {
                assertTrue(rs.next());
                assertEquals("94030", rs.getString(1));
            }
        }
    }
}
