/*
 * Copyright 2014 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.end2end.HBaseManagedTimeTest;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PhoenixArray;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(HBaseManagedTimeTest.class)
public class ArrayInsertUniqueFunctionIT extends BaseHBaseManagedTimeIT {

    @Test
    public void simpleAddTest() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE IF NOT EXISTS ARRAY_UPSERT_UNIQUE_TABLE"
                + " (k1 INTEGER NOT NULL, arr INTEGER_ARRAY CONSTRAINT pk PRIMARY KEY (k1))";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO ARRAY_UPSERT_UNIQUE_TABLE (k1, arr) VALUES (1, ARRAY[1, 2, 3])";
        conn.createStatement().execute(dml);
        conn.commit();

        dml = "UPSERT INTO ARRAY_UPSERT_UNIQUE_TABLE (k1, arr) VALUES (1, ARRAY_INSERT_UNIQUE(ARRAY[1, 2, 5, 6]))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT arr FROM ARRAY_UPSERT_UNIQUE_TABLE");

        Integer[] mockArray = {1, 2, 3, 5, 6};
        PhoenixArray.PrimitiveIntPhoenixArray mockPhoenixArray = new PhoenixArray.PrimitiveIntPhoenixArray(PDataType.INTEGER, mockArray);

        assertTrue(rs.next());

        PhoenixArray.PrimitiveIntPhoenixArray modifiedArray = (PhoenixArray.PrimitiveIntPhoenixArray) (rs.getArray(1));
        assertArrayEquals((int[]) (mockPhoenixArray.getArray()), (int[])(modifiedArray.getArray()));
    }

    @Test
    public void insertSameValues() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE IF NOT EXISTS ARRAY_UPSERT_UNIQUE_TABLE"
                + " (k1 INTEGER NOT NULL, arr INTEGER_ARRAY CONSTRAINT pk PRIMARY KEY (k1))";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO ARRAY_UPSERT_UNIQUE_TABLE (k1, arr) VALUES (1, ARRAY[1, 2, 3])";
        conn.createStatement().execute(dml);
        conn.commit();

        dml = "UPSERT INTO ARRAY_UPSERT_UNIQUE_TABLE (k1, arr) VALUES (1, ARRAY_INSERT_UNIQUE(ARRAY[1, 2, 3]))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT arr FROM ARRAY_UPSERT_UNIQUE_TABLE");

        Integer[] mockArray = {1, 2, 3};
        PhoenixArray.PrimitiveIntPhoenixArray mockPhoenixArray = new PhoenixArray.PrimitiveIntPhoenixArray(PDataType.INTEGER, mockArray);

        assertTrue(rs.next());

        PhoenixArray.PrimitiveIntPhoenixArray modifiedArray = (PhoenixArray.PrimitiveIntPhoenixArray) (rs.getArray(1));
        assertArrayEquals((int[]) (mockPhoenixArray.getArray()), (int[])(modifiedArray.getArray()));
    }

    @Test
    public void testInsertNewArray() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE IF NOT EXISTS ARRAY_UPSERT_UNIQUE_TABLE"
                + " (k1 INTEGER NOT NULL, arr SMALLINT_ARRAY CONSTRAINT pk PRIMARY KEY (k1))";
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO ARRAY_UPSERT_UNIQUE_TABLE (k1, arr) VALUES (1, ARRAY_INSERT_UNIQUE(ARRAY[1, 2]))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT arr FROM ARRAY_UPSERT_UNIQUE_TABLE");

        Short[] mockArray = {1, 2};
        PhoenixArray.PrimitiveShortPhoenixArray mockPhoenixArray = new PhoenixArray.PrimitiveShortPhoenixArray(PDataType.SMALLINT, mockArray);
        assertTrue(rs.next());

        PhoenixArray.PrimitiveShortPhoenixArray modifiedArray = (PhoenixArray.PrimitiveShortPhoenixArray) (rs.getArray(1));
        assertArrayEquals((short[]) (mockPhoenixArray.getArray()), (short[])(modifiedArray.getArray()));
    }

    @Test
    public void simpleAddTestBigInt() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE IF NOT EXISTS ARRAY_UPSERT_UNIQUE_TABLE"
                + " (k1 INTEGER NOT NULL, arr LONG_ARRAY CONSTRAINT pk PRIMARY KEY (k1))";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO ARRAY_UPSERT_UNIQUE_TABLE (k1, arr) VALUES (1, ARRAY[1, 2, 3])";
        conn.createStatement().execute(dml);
        conn.commit();

        dml = "UPSERT INTO ARRAY_UPSERT_UNIQUE_TABLE (k1, arr) VALUES (1, ARRAY_INSERT_UNIQUE(ARRAY[1, 2, 5, 6]))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT arr FROM ARRAY_UPSERT_UNIQUE_TABLE");

        Long[] mockArray = {1L, 2L, 3L, 5L, 6L};
        PhoenixArray.PrimitiveLongPhoenixArray mockPhoenixArray = new PhoenixArray.PrimitiveLongPhoenixArray(PDataType.LONG, mockArray);

        assertTrue(rs.next());

        PhoenixArray.PrimitiveLongPhoenixArray modifiedArray = (PhoenixArray.PrimitiveLongPhoenixArray) (rs.getArray(1));
        assertArrayEquals((long[]) (mockPhoenixArray.getArray()), (long[])(modifiedArray.getArray()));
    }

    @Test
    public void modifyMultipleArrayOneQuery() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE IF NOT EXISTS ARRAY_UPSERT_UNIQUE_TABLE"
                + " (k1 INTEGER NOT NULL, arr1 INTEGER_ARRAY, arr2 INTEGER_ARRAY CONSTRAINT pk PRIMARY KEY (k1))";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO ARRAY_UPSERT_UNIQUE_TABLE (k1, arr1, arr2) VALUES (1, ARRAY[1, 2, 3], ARRAY[10, 20, 30])";
        conn.createStatement().execute(dml);
        conn.commit();

        dml = "UPSERT INTO ARRAY_UPSERT_UNIQUE_TABLE (k1, arr1, arr2) "
                + "VALUES (1, ARRAY_INSERT_UNIQUE(ARRAY[1, 2, 5, 6]), ARRAY_INSERT_UNIQUE(ARRAY[10, 20, 50, 60]))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT arr1, arr2 FROM ARRAY_UPSERT_UNIQUE_TABLE");

        Integer[] mockArray = {1, 2, 3, 5, 6};
        PhoenixArray.PrimitiveIntPhoenixArray mockPhoenixArray = new PhoenixArray.PrimitiveIntPhoenixArray(PDataType.INTEGER, mockArray);

        assertTrue(rs.next());

        PhoenixArray.PrimitiveIntPhoenixArray modifiedArray = (PhoenixArray.PrimitiveIntPhoenixArray) (rs.getArray(1));
        assertArrayEquals((int[]) (mockPhoenixArray.getArray()), (int[])(modifiedArray.getArray()));


        //second array
        Integer[] mockArray2 = {10, 20, 30, 50, 60};
        mockPhoenixArray = new PhoenixArray.PrimitiveIntPhoenixArray(PDataType.INTEGER, mockArray2);

        modifiedArray = (PhoenixArray.PrimitiveIntPhoenixArray) (rs.getArray(2));
        assertArrayEquals((int[]) (mockPhoenixArray.getArray()), (int[])(modifiedArray.getArray()));
    }

    @Test
    @Ignore //ARRAY_INSERT_UNIQUE function is not supported on row key yet
    public void atPrimaryKeyAsc() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE IF NOT EXISTS ARRAY_UPSERT_UNIQUE_TABLE"
                + " (k1 INTEGER NOT NULL, arr LONG_ARRAY, val INTEGER CONSTRAINT pk PRIMARY KEY (k1, arr))";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO ARRAY_UPSERT_UNIQUE_TABLE (k1, arr, val) VALUES (1, ARRAY[1, 2, 3], 4)";
        conn.createStatement().execute(dml);
        conn.commit();

        dml = "UPSERT INTO ARRAY_UPSERT_UNIQUE_TABLE (k1, arr, val) VALUES (1, ARRAY_INSERT_UNIQUE(ARRAY[1, 2, 5, 6]), 4)";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT arr FROM ARRAY_UPSERT_UNIQUE_TABLE");

        Long[] mockArray = {1L, 2L, 3L, 5L, 6L};
        PhoenixArray.PrimitiveLongPhoenixArray mockPhoenixArray = new PhoenixArray.PrimitiveLongPhoenixArray(PDataType.LONG, mockArray);

        assertTrue(rs.next());

        PhoenixArray.PrimitiveLongPhoenixArray modifiedArray = (PhoenixArray.PrimitiveLongPhoenixArray) (rs.getArray(1));
        assertArrayEquals((long[]) (mockPhoenixArray.getArray()), (long[])(modifiedArray.getArray()));
    }

    @Test
    @Ignore //ARRAY_INSERT_UNIQUE function is not supported on row key yet
    public void atPrimaryKeyDesc() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE IF NOT EXISTS ARRAY_UPSERT_UNIQUE_TABLE"
                + " (k1 INTEGER NOT NULL, arr LONG_ARRAY, val INTEGER CONSTRAINT pk PRIMARY KEY (k1, arr DESC))";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO ARRAY_UPSERT_UNIQUE_TABLE (k1, arr, val) VALUES (1, ARRAY[1, 2, 3], 4)";
        conn.createStatement().execute(dml);
        conn.commit();

        dml = "UPSERT INTO ARRAY_UPSERT_UNIQUE_TABLE (k1, arr, val) VALUES (1, ARRAY_INSERT_UNIQUE(ARRAY[1, 2, 5, 6]), 4)";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT arr FROM ARRAY_UPSERT_UNIQUE_TABLE");

        Long[] mockArray = {1L, 2L, 3L, 5L, 6L};
        PhoenixArray.PrimitiveLongPhoenixArray mockPhoenixArray = new PhoenixArray.PrimitiveLongPhoenixArray(PDataType.LONG, mockArray);

        assertTrue(rs.next());

        PhoenixArray.PrimitiveLongPhoenixArray modifiedArray = (PhoenixArray.PrimitiveLongPhoenixArray) (rs.getArray(1));
        assertArrayEquals((long[]) (mockPhoenixArray.getArray()), (long[])(modifiedArray.getArray()));
    }


    @Test
    public void stringDataType() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE IF NOT EXISTS ARRAY_UPSERT_UNIQUE_TABLE"
                + " (k1 INTEGER NOT NULL, arr VARCHAR_ARRAY CONSTRAINT pk PRIMARY KEY (k1))";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO ARRAY_UPSERT_UNIQUE_TABLE (k1, arr) VALUES (1, ARRAY['a', 'bb', 'c'])";
        conn.createStatement().execute(dml);
        conn.commit();

        dml = "UPSERT INTO ARRAY_UPSERT_UNIQUE_TABLE (k1, arr) VALUES (1, ARRAY_INSERT_UNIQUE(ARRAY['a', 'bb', 'd', 'ee']))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT arr FROM ARRAY_UPSERT_UNIQUE_TABLE");

        String[] mockArray = {"a", "bb", "c", "d", "ee"};
        PhoenixArray mockPhoenixArray = new PhoenixArray(PDataType.VARCHAR, mockArray);

        assertTrue(rs.next());
        PhoenixArray modifiedArray = (PhoenixArray) rs.getArray(1);
        assertArrayEquals((Object[])mockPhoenixArray.getArray(), (Object[])modifiedArray.getArray());
    }

    @Test
    public void stringDataTypeNewArray() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE IF NOT EXISTS ARRAY_UPSERT_UNIQUE_TABLE"
                + " (k1 INTEGER NOT NULL, arr VARCHAR_ARRAY CONSTRAINT pk PRIMARY KEY (k1))";
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO ARRAY_UPSERT_UNIQUE_TABLE (k1, arr) VALUES (1, ARRAY_INSERT_UNIQUE(ARRAY['a', 'bb', 'd', 'ee']))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT arr FROM ARRAY_UPSERT_UNIQUE_TABLE");

        String[] mockArray = {"a", "bb", "d", "ee"};
        PhoenixArray mockPhoenixArray = new PhoenixArray(PDataType.VARCHAR, mockArray);

        assertTrue(rs.next());
        PhoenixArray modifiedArray = (PhoenixArray) rs.getArray(1);
        assertArrayEquals((Object[])mockPhoenixArray.getArray(), (Object[])modifiedArray.getArray());
    }

    @Test
    public void stringDataTypeDuplicitArray() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE IF NOT EXISTS ARRAY_UPSERT_UNIQUE_TABLE"
                + " (k1 INTEGER NOT NULL, arr VARCHAR_ARRAY CONSTRAINT pk PRIMARY KEY (k1))";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO ARRAY_UPSERT_UNIQUE_TABLE (k1, arr)"
                + " VALUES (1, ARRAY['foo', 'bar', 'test', 'long word', 'and'])";
        conn.createStatement().execute(dml);
        conn.commit();

        dml = "UPSERT INTO ARRAY_UPSERT_UNIQUE_TABLE (k1, arr) "
                + "VALUES (1, ARRAY_INSERT_UNIQUE(ARRAY['foo', 'bar', 'test', 'long word', 'and']))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT arr FROM ARRAY_UPSERT_UNIQUE_TABLE");

        String[] mockArray = {"foo", "bar", "test", "long word", "and"};
        PhoenixArray mockPhoenixArray = new PhoenixArray(PDataType.VARCHAR, mockArray);

        assertTrue(rs.next());
        PhoenixArray modifiedArray = (PhoenixArray) rs.getArray(1);
        assertArrayEquals((Object[])mockPhoenixArray.getArray(), (Object[])modifiedArray.getArray());
    }


    @Test
    public void insertDuplicitArray() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE IF NOT EXISTS ARRAY_UPSERT_UNIQUE_TABLE"
                + " (k1 INTEGER NOT NULL, arr INTEGER_ARRAY CONSTRAINT pk PRIMARY KEY (k1))";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO ARRAY_UPSERT_UNIQUE_TABLE (k1, arr) VALUES (1, ARRAY[1, 2, 3])";
        conn.createStatement().execute(dml);
        conn.commit();

        dml = "UPSERT INTO ARRAY_UPSERT_UNIQUE_TABLE (k1, arr) VALUES (1, ARRAY_INSERT_UNIQUE(ARRAY[1, 2, 3]))";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT arr FROM ARRAY_UPSERT_UNIQUE_TABLE");

        Integer[] mockArray = {1, 2, 3};
        PhoenixArray.PrimitiveIntPhoenixArray mockPhoenixArray = new PhoenixArray.PrimitiveIntPhoenixArray(PDataType.INTEGER, mockArray);

        assertTrue(rs.next());

        PhoenixArray.PrimitiveIntPhoenixArray modifiedArray = (PhoenixArray.PrimitiveIntPhoenixArray) (rs.getArray(1));
        assertArrayEquals((int[]) (mockPhoenixArray.getArray()), (int[])(modifiedArray.getArray()));
    }
}
