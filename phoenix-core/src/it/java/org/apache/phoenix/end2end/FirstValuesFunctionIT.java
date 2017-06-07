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
import java.sql.ResultSet;
import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Test;


public class FirstValuesFunctionIT extends ParallelStatsDisabledIT {

    //@Test
    public void simpleTest() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + tableName + " "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
                + " dates INTEGER, val INTEGER)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (2, 8, 1, 7)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (3, 8, 2, 9)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (4, 8, 3, 4)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (5, 8, 4, 2)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (6, 9, 5, 10)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (7, 9, 6, 13)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (8, 9, 7, 8)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (9, 9, 8, 6)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT FIRST_VALUES(val, 3) WITHIN GROUP (ORDER BY dates DESC) FROM " + tableName
                + " GROUP BY page_id");

        assertTrue(rs.next());
        Assert.assertArrayEquals(new int[]{2, 4, 9}, (int[]) rs.getArray(1).getArray());
        assertTrue(rs.next());
        Assert.assertArrayEquals(new int[]{6, 8, 13}, (int[]) rs.getArray(1).getArray());
        assertFalse(rs.next());
    }

    @Test
    public void varcharDatatypeSimpleTest() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String table_name = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + table_name + " "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG, "
                + "\"DATE\" VARCHAR(3), \"value\" VARCHAR(3))";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute(
            "UPSERT INTO " + table_name + " (id, page_id, \"DATE\", \"value\") VALUES (1, 8, '1', '3')");
        conn.createStatement().execute(
            "UPSERT INTO " + table_name + " (id, page_id, \"DATE\", \"value\") VALUES (2, 8, '2', '7')");
        conn.createStatement().execute(
            "UPSERT INTO " + table_name + " (id, page_id, \"DATE\", \"value\") VALUES (3, 8, '3', '9')");
        conn.createStatement().execute(
            "UPSERT INTO " + table_name + " (id, page_id, \"DATE\", \"value\") VALUES (5, 8, '4', '2')");
        conn.createStatement().execute(
            "UPSERT INTO " + table_name + " (id, page_id, \"DATE\", \"value\") VALUES (4, 8, '5', '4')");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT FIRST_VALUES(\"value\", 2) WITHIN GROUP (ORDER BY \"DATE\" ASC) FROM " + table_name
                + " GROUP BY page_id");

        assertTrue(rs.next());
        Assert.assertArrayEquals(new String[]{"3", "7"}, (String[]) rs.getArray(1).getArray());
        assertFalse(rs.next());
    }

    //@Test
    public void floatDatatypeSimpleTest() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + tableName + " "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
                + " \"DATE\" FLOAT, \"value\" FLOAT)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, \"DATE\", \"value\") VALUES (1, 8, 1, 300)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, \"DATE\", \"value\") VALUES (2, 8, 2, 7)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, \"DATE\", \"value\") VALUES (3, 8, 3, 9)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, \"DATE\", \"value\") VALUES (5, 8, 4, 2)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, \"DATE\", \"value\") VALUES (4, 8, 5, 400)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT FIRST_VALUES(\"value\", 2) WITHIN GROUP (ORDER BY \"DATE\" ASC) FROM " + tableName
                + " GROUP BY page_id");

        assertTrue(rs.next());
        assertEquals(((float[])rs.getArray(1).getArray())[0], 300, 0.000001);
        assertEquals(((float[])rs.getArray(1).getArray())[1], 7, 0.000001);
        assertFalse(rs.next());
    }

    //@Test
    public void doubleDatatypeSimpleTest() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + tableName + " "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG, "
                + "\"DATE\" DOUBLE, \"value\" DOUBLE)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, \"DATE\", \"value\") VALUES (1, 8, 1, 300)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, \"DATE\", \"value\") VALUES (2, 8, 2, 7)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, \"DATE\", \"value\") VALUES (3, 8, 3, 9)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, \"DATE\", \"value\") VALUES (5, 8, 4, 2)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, \"DATE\", \"value\") VALUES (4, 8, 5, 400)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT FIRST_VALUES(\"value\", 2) WITHIN GROUP (ORDER BY \"DATE\" ASC) FROM " + tableName
                + " GROUP BY page_id");

        assertTrue(rs.next());
        assertEquals(((double[])rs.getArray(1).getArray())[0], 300, 0.000001);
        assertEquals(((double[])rs.getArray(1).getArray())[1], 7, 0.000001);
        assertFalse(rs.next());
    }

    //@Test
    public void offsetValueAscOrder() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String tableName = generateUniqueName();

        String ddl = "CREATE TABLE IF NOT EXISTS " + tableName + " "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
                + " \"DATE\" INTEGER, \"value\" UNSIGNED_LONG)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, \"DATE\", \"value\") VALUES (1, 8, 0, 300)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, \"DATE\", \"value\") VALUES (2, 8, 1, 7)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, \"DATE\", \"value\") VALUES (3, 8, 2, 9)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, \"DATE\", \"value\") VALUES (4, 8, 3, 4)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, \"DATE\", \"value\") VALUES (5, 8, 4, 2)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, \"DATE\", \"value\") VALUES (6, 8, 5, 150)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT FIRST_VALUES(\"value\", 2)  WITHIN GROUP (ORDER BY \"DATE\" ASC) FROM "
                + tableName + " GROUP BY page_id");

        assertTrue(rs.next());
        Assert.assertArrayEquals(new long[]{300, 7}, (long[]) rs.getArray(1).getArray());
        assertFalse(rs.next());
    }

    //@Test
    public void simpleTestNoGroupBy() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + tableName + " "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
                + " dates INTEGER, val INTEGER)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (2, 8, 1, 7)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (3, 8, 2, 9)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT FIRST_VALUES(val, 2) WITHIN GROUP (ORDER BY dates DESC) FROM " + tableName);

        assertTrue(rs.next());
        Assert.assertArrayEquals(new int[]{9, 7}, (int[]) rs.getArray(1).getArray());
        assertFalse(rs.next());
    }

    //@Test
    public void rowLessThanOffsetNoGroupBy() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + tableName + " "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
                + " dates INTEGER, val INTEGER)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (2, 8, 1, 7)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (3, 8, 2, 9)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT FIRST_VALUES(val, 3) WITHIN GROUP (ORDER BY dates DESC) FROM " + tableName);

        assertTrue(rs.next());
        Assert.assertArrayEquals(new int[]{9, 7}, (int[]) rs.getArray(1).getArray());
        assertFalse(rs.next());
    }

    //@Test
    public void offsetValueDescOrder() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + tableName + " "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
                + " \"DATE\" INTEGER, \"value\" UNSIGNED_LONG)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, \"DATE\", \"value\") VALUES (1, 8, 0, 300)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, \"DATE\", \"value\") VALUES (2, 8, 1, 7)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, \"DATE\", \"value\") VALUES (3, 8, 2, 9)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, \"DATE\", \"value\") VALUES (4, 8, 3, 4)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, \"DATE\", \"value\") VALUES (5, 8, 4, 2)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, \"DATE\", \"value\") VALUES (6, 8, 5, 150)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT FIRST_VALUES(\"value\", 2)  WITHIN GROUP (ORDER BY \"DATE\" DESC) FROM "
                + tableName + " GROUP BY page_id");

        assertTrue(rs.next());
        Assert.assertArrayEquals(new long[]{150, 2}, (long[]) rs.getArray(1).getArray());
        assertFalse(rs.next());
    }

    //@Test
    public void offsetValueSubAggregation() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + tableName + " "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
                + " \"DATE\" INTEGER, \"value\" UNSIGNED_LONG)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute(
                "UPSERT INTO " + tableName + " (id, page_id, \"DATE\", \"value\") VALUES (1, 8, 0, 300)");
        conn.createStatement().execute(
                "UPSERT INTO " + tableName + " (id, page_id, \"DATE\", \"value\") VALUES (2, 8, 1, 7)");
        conn.createStatement().execute(
                "UPSERT INTO " + tableName + " (id, page_id, \"DATE\", \"value\") VALUES (3, 9, 2, 9)");
        conn.createStatement().execute(
                "UPSERT INTO " + tableName + " (id, page_id, \"DATE\", \"value\") VALUES (4, 9, 3, 4)");
        conn.createStatement().execute(
                "UPSERT INTO " + tableName + " (id, page_id, \"DATE\", \"value\") VALUES (5, 10, 4, 2)");
        conn.createStatement().execute(
                "UPSERT INTO " + tableName + " (id, page_id, \"DATE\", \"value\") VALUES (6, 10, 5, 150)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT FIRST_VALUES(SUM_VALUE, 2) WITHIN GROUP (ORDER BY MIN_DATE ASC) FROM (" +
                        "SELECT MIN(\"DATE\") AS MIN_DATE, SUM(\"value\") AS SUM_VALUE FROM "
                        + tableName + " GROUP BY page_id) x");

        assertTrue(rs.next());
        Assert.assertArrayEquals(new long[]{307, 13}, (long[]) rs.getArray(1).getArray());
        assertFalse(rs.next());
    }

    //@Test
    public void offsetValueLastMismatchByColumn() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + tableName + " "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
                + " \"DATE\" INTEGER, \"value\" UNSIGNED_LONG)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, \"DATE\", \"value\") VALUES (1, 8, 5, 8)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, \"DATE\", \"value\") VALUES (2, 8, 2, 7)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, \"DATE\", \"value\") VALUES (3, 8, 1, 9)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, \"DATE\", \"value\") VALUES (4, 8, 4, 4)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, \"DATE\", \"value\") VALUES (5, 8, 3, 2)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, \"DATE\", \"value\") VALUES (6, 8, 0, 1)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT FIRST_VALUES(\"value\", 2)  WITHIN GROUP (ORDER BY \"DATE\" DESC) FROM "
                + tableName + " GROUP BY page_id");

        assertTrue(rs.next());
        Assert.assertArrayEquals(new long[]{8, 4}, (long[]) rs.getArray(1).getArray());
        assertFalse(rs.next());
    }

    //@Test
    public void testSortOrderInDataColWithOffset() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + tableName + " "
                + "(id INTEGER NOT NULL, page_id UNSIGNED_LONG,"
                + " dates BIGINT NOT NULL, \"value\" BIGINT NOT NULL CONSTRAINT pk PRIMARY KEY (id, dates, \"value\" DESC))";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute("UPSERT INTO " + tableName
            + " (id, page_id, dates, \"value\") VALUES (1, 8, 1, 3)");
        conn.createStatement().execute("UPSERT INTO " + tableName
            + " (id, page_id, dates, \"value\") VALUES (2, 8, 2, 7)");
        conn.createStatement().execute("UPSERT INTO " + tableName
            + " (id, page_id, dates, \"value\") VALUES (3, 8, 3, 9)");
        conn.createStatement().execute("UPSERT INTO " + tableName
            + " (id, page_id, dates, \"value\") VALUES (5, 8, 5, 158)");
        conn.createStatement().execute("UPSERT INTO " + tableName
            + " (id, page_id, dates, \"value\") VALUES (4, 8, 4, 5)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT FIRST_VALUES(\"value\", 2)  WITHIN GROUP (ORDER BY dates ASC) FROM "
                + tableName + " GROUP BY page_id");

        assertTrue(rs.next());
        Assert.assertArrayEquals(new long[]{3, 7}, (long[]) rs.getArray(1).getArray());
        assertFalse(rs.next());
    }

    //@Test
    public void nonUniqueValuesInOrderByAsc() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + tableName + " "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
                + " dates INTEGER, val INTEGER)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (2, 8, 1, 7)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (3, 8, 2, 9)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (4, 8, 2, 4)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (5, 8, 2, 2)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (6, 8, 3, 3)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT FIRST_VALUES(val, 3) WITHIN GROUP (ORDER BY dates ASC) FROM " + tableName
                + " GROUP BY page_id");

        int[] result = null;
        assertTrue(rs.next());
        result = (int[]) rs.getArray(1).getArray();
        assertEquals(result[0], 7);
        assertInIntArray(new int[]{2, 4, 9}, result[1]);
        assertInIntArray(new int[]{2, 4, 9}, result[2]);
        assertFalse(rs.next());
    }

    //@Test
    public void nonUniqueValuesInOrderByAscSkipDuplicit() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + tableName + " "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
                + " dates INTEGER, val INTEGER)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (2, 8, 1, 7)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (3, 8, 2, 9)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (4, 8, 2, 4)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (5, 8, 2, 2)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (6, 8, 3, 3)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT FIRST_VALUES(val, 5) WITHIN GROUP (ORDER BY dates ASC) FROM " + tableName
                + " GROUP BY page_id");

        int[] result = null;
        assertTrue(rs.next());
        result = (int[]) rs.getArray(1).getArray();
        assertEquals(result[0], 7);
        assertInIntArray(new int[]{2, 4, 9}, result[1]);
        assertInIntArray(new int[]{2, 4, 9}, result[2]);
        assertInIntArray(new int[]{2, 4, 9}, result[3]);
        assertEquals(result[4], 3);
        assertFalse(rs.next());
    }

    //@Test
    public void nonUniqueValuesInOrderByDesc() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + tableName + " "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
                + " dates INTEGER, val INTEGER)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (2, 8, 1, 7)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (3, 8, 2, 9)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (4, 8, 2, 4)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (5, 8, 2, 2)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (6, 8, 3, 3)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT FIRST_VALUES(val, 3) WITHIN GROUP (ORDER BY dates DESC) FROM " + tableName
                + " GROUP BY page_id");

        int[] result = null;
        assertTrue(rs.next());
        result = (int[]) rs.getArray(1).getArray();
        assertEquals(result[0], 3);
        assertInIntArray(new int[]{2, 4, 9}, result[1]);
        assertInIntArray(new int[]{2, 4, 9}, result[2]);
        assertFalse(rs.next());
    }

    //@Test
    public void nonUniqueValuesInOrderNextValueDesc() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + tableName + " "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
                + " dates INTEGER, val INTEGER)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (2, 8, 0, 7)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (3, 8, 1, 9)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (4, 8, 2, 4)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (5, 8, 2, 2)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (6, 8, 3, 3)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (7, 8, 3, 5)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT FIRST_VALUES(val, 2) WITHIN GROUP (ORDER BY dates DESC) FROM " + tableName
                + " GROUP BY page_id");

        int[] result = null;
        assertTrue(rs.next());
        result = (int[]) rs.getArray(1).getArray();
        assertInIntArray(new int[]{3, 5}, result[0]);
        assertInIntArray(new int[]{3, 5}, result[1]);
        assertFalse(rs.next());
    }

    //@Test
    public void nonUniqueValuesInOrderNextValueAsc() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + tableName + " "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
                + " dates INTEGER, val INTEGER)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (2, 8, 0, 7)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (3, 8, 1, 9)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (4, 8, 2, 4)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (5, 8, 2, 2)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (6, 8, 3, 3)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (7, 8, 3, 5)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT FIRST_VALUES(val, 5) WITHIN GROUP (ORDER BY dates ASC) FROM " + tableName
                + " GROUP BY page_id");

        int[] result = null;
        assertTrue(rs.next());
        result = (int[]) rs.getArray(1).getArray();
        assertEquals(result[0], 7);
        assertEquals(result[1], 9);
        assertInIntArray(new int[]{2, 4}, result[2]);
        assertInIntArray(new int[]{2, 4}, result[3]);
        assertInIntArray(new int[]{3, 5}, result[4]);
        assertFalse(rs.next());
    }

    //@Test
    public void ignoreNullValues() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + tableName + " "
                + "(id INTEGER NOT NULL, page_id UNSIGNED_LONG,"
                + " dates BIGINT NOT NULL, \"value\" BIGINT NULL CONSTRAINT pk PRIMARY KEY (id, dates))";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute("UPSERT INTO " + tableName
            + " (id, page_id, dates, \"value\") VALUES (1, 8, 1, 1)");
        conn.createStatement().execute("UPSERT INTO " + tableName
            + " (id, page_id, dates, \"value\") VALUES (2, 8, 2, NULL)");
        conn.createStatement().execute("UPSERT INTO " + tableName
            + " (id, page_id, dates, \"value\") VALUES (3, 8, 3, NULL)");
        conn.createStatement().execute("UPSERT INTO " + tableName
            + " (id, page_id, dates, \"value\") VALUES (5, 8, 4, 4)");
        conn.createStatement().execute("UPSERT INTO " + tableName
            + " (id, page_id, dates, \"value\") VALUES (4, 8, 5, 5)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT FIRST_VALUES(\"value\", 2)  WITHIN GROUP (ORDER BY dates ASC) FROM "
                + tableName + " GROUP BY page_id");

        assertTrue(rs.next());
        Assert.assertArrayEquals(new long[]{1, 4}, (long[]) rs.getArray(1).getArray());
        assertFalse(rs.next());
    }

    //@Test
    public void rowLessThanOffsetWithGroupBy() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + tableName + " "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
                + " dates INTEGER, val INTEGER)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (2, 8, 0, 7)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (3, 8, 1, 9)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (4, 8, 2, 4)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (5, 8, 2, 2)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (6, 8, 3, 3)");
        conn.createStatement().execute(
            "UPSERT INTO " + tableName + " (id, page_id, dates, val) VALUES (7, 9, 3, 5)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT FIRST_VALUES(val, 2)  WITHIN GROUP (ORDER BY dates ASC) FROM "
                + tableName + " GROUP BY page_id");

        assertTrue(rs.next());
        Assert.assertArrayEquals(new int[]{7, 9}, (int[]) rs.getArray(1).getArray());
        assertTrue(rs.next());
        Assert.assertArrayEquals(new int[]{5}, (int[]) rs.getArray(1).getArray());
        assertFalse(rs.next());
    }

    private void assertInIntArray(int[] should, int actualValue) {
        ArrayList<Integer> shouldList = new ArrayList<Integer>();
        for (int i: should) {
            shouldList.add(i);
        }
        assertTrue(shouldList.contains(actualValue));
    }

}
