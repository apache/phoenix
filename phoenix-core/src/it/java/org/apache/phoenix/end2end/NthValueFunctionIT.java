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

import org.junit.Test;


public class NthValueFunctionIT extends ParallelStatsDisabledIT {

    @Test
    public void simpleTest() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String nthValue = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + nthValue + " "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
                + " dates INTEGER, val INTEGER)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, page_id, dates, val) VALUES (2, 8, 1, 7)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, page_id, dates, val) VALUES (3, 8, 2, 9)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, page_id, dates, val) VALUES (4, 8, 3, 4)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, page_id, dates, val) VALUES (5, 8, 4, 2)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT NTH_VALUE(val, 2) WITHIN GROUP (ORDER BY dates DESC) FROM " + nthValue
                + " GROUP BY page_id");

        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 4);
        assertFalse(rs.next());
    }

    @Test
    public void multipleNthValueFunctionTest() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String nthValue = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + nthValue + " "
                + "(id INTEGER NOT NULL, feid UNSIGNED_LONG NOT NULL,"
                + " uid INTEGER NOT NULL, lrd INTEGER"
                + " CONSTRAINT PKVIEW PRIMARY KEY ( id, feid, uid))";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, feid, uid, lrd) VALUES (2, 8, 1, 7)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, feid, uid, lrd) VALUES (2, 8, 2, 9)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, feid, uid, lrd) VALUES (2, 8, 3, 4)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, feid, uid, lrd) VALUES (2, 8, 4, 2)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, feid, uid, lrd) VALUES (2, 9, 5, 1)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, feid, uid, lrd) VALUES (2, 9, 6, 3)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, feid, uid, lrd) VALUES (2, 9, 8, 5)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, feid, uid, lrd) VALUES (2, 9, 7, 8)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, feid, uid, lrd) VALUES (2, 10, 5, 1)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, feid, uid, lrd) VALUES (2, 10, 6, 3)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, feid, uid, lrd) VALUES (2, 10, 7, 5)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, feid, uid, lrd) VALUES (2, 10, 8, 8)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, feid, uid, lrd) VALUES (3, 10, 5, 1)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, feid, uid, lrd) VALUES (3, 10, 6, 3)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, feid, uid, lrd) VALUES (3, 10, 7, 5)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, feid, uid, lrd) VALUES (3, 10, 8, 8)");

        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT NTH_VALUE(uid, 1) WITHIN GROUP (ORDER BY lrd DESC) as nth1_user_id, NTH_VALUE(uid, 2) WITHIN GROUP (ORDER BY lrd DESC) as nth2_user_id, NTH_VALUE(uid, 3) WITHIN GROUP (ORDER BY lrd DESC) as nth3_user_id  FROM " + nthValue
                + " where id=2 and feid in (8, 9, 10) GROUP BY feid");

        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 2);
        assertEquals(rs.getInt(2), 1);
        assertEquals(rs.getInt(3), 3);
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 7);
        assertEquals(rs.getInt(2), 8);
        assertEquals(rs.getInt(3), 6);
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 8);
        assertEquals(rs.getInt(2), 7);
        assertEquals(rs.getInt(3), 6);
        assertFalse(rs.next());
    }

    @Test
    public void offsetValueAscOrder() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String nth_test_table = generateUniqueName();

        String ddl = "CREATE TABLE IF NOT EXISTS " + nth_test_table + " "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
                + " \"DATE\" INTEGER, \"value\" UNSIGNED_LONG)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute("UPSERT INTO " + nth_test_table
            + " (id, page_id, \"DATE\", \"value\") VALUES (1, 8, 0, 300)");
        conn.createStatement().execute(
            "UPSERT INTO " + nth_test_table + " (id, page_id, \"DATE\", \"value\") VALUES (2, 8, 1, 7)");
        conn.createStatement().execute(
            "UPSERT INTO " + nth_test_table + " (id, page_id, \"DATE\", \"value\") VALUES (3, 8, 2, 9)");
        conn.createStatement().execute(
            "UPSERT INTO " + nth_test_table + " (id, page_id, \"DATE\", \"value\") VALUES (4, 8, 3, 4)");
        conn.createStatement().execute(
            "UPSERT INTO " + nth_test_table + " (id, page_id, \"DATE\", \"value\") VALUES (5, 8, 4, 2)");
        conn.createStatement().execute("UPSERT INTO " + nth_test_table
            + " (id, page_id, \"DATE\", \"value\") VALUES (6, 8, 5, 150)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT NTH_VALUE(\"value\", 2)  WITHIN GROUP (ORDER BY \"DATE\" ASC) FROM "
                + nth_test_table + " GROUP BY page_id");

        assertTrue(rs.next());
        assertEquals(rs.getLong(1), 7);
        assertFalse(rs.next());
    }

    @Test
    public void offsetValueDescOrder() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String nth_test_table = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + nth_test_table + " "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
                + " \"DATE\" INTEGER, \"value\" UNSIGNED_LONG)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute("UPSERT INTO " + nth_test_table
            + " (id, page_id, \"DATE\", \"value\") VALUES (1, 8, 0, 300)");
        conn.createStatement().execute(
            "UPSERT INTO " + nth_test_table + " (id, page_id, \"DATE\", \"value\") VALUES (2, 8, 1, 7)");
        conn.createStatement().execute(
            "UPSERT INTO " + nth_test_table + " (id, page_id, \"DATE\", \"value\") VALUES (3, 8, 2, 9)");
        conn.createStatement().execute(
            "UPSERT INTO " + nth_test_table + " (id, page_id, \"DATE\", \"value\") VALUES (4, 8, 3, 4)");
        conn.createStatement().execute(
            "UPSERT INTO " + nth_test_table + " (id, page_id, \"DATE\", \"value\") VALUES (5, 8, 4, 2)");
        conn.createStatement().execute("UPSERT INTO " + nth_test_table
            + " (id, page_id, \"DATE\", \"value\") VALUES (6, 8, 5, 150)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT NTH_VALUE(\"value\", 2)  WITHIN GROUP (ORDER BY \"DATE\" DESC) FROM "
                + nth_test_table + " GROUP BY page_id");

        assertTrue(rs.next());
        assertEquals(rs.getLong(1), 2);
        assertFalse(rs.next());
    }

    @Test
    public void offsetValueSubAggregation() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String nth_test_table = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + nth_test_table + " "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
                + " \"DATE\" INTEGER, \"value\" UNSIGNED_LONG)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute("UPSERT INTO " + nth_test_table
                + " (id, page_id, \"DATE\", \"value\") VALUES (1, 8, 0, 300)");
        conn.createStatement().execute(
                "UPSERT INTO " + nth_test_table + " (id, page_id, \"DATE\", \"value\") VALUES (2, 8, 1, 7)");
        conn.createStatement().execute(
                "UPSERT INTO " + nth_test_table + " (id, page_id, \"DATE\", \"value\") VALUES (3, 9, 2, 9)");
        conn.createStatement().execute(
                "UPSERT INTO " + nth_test_table + " (id, page_id, \"DATE\", \"value\") VALUES (4, 9, 3, 4)");
        conn.createStatement().execute(
                "UPSERT INTO " + nth_test_table + " (id, page_id, \"DATE\", \"value\") VALUES (5, 10, 4, 2)");
        conn.createStatement().execute("UPSERT INTO " + nth_test_table
                + " (id, page_id, \"DATE\", \"value\") VALUES (6, 10, 5, 150)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT NTH_VALUE(SUM_VALUE, 2) WITHIN GROUP (ORDER BY MIN_DATE ASC) FROM (" +
                        "SELECT MIN(\"DATE\") AS MIN_DATE, SUM(\"value\") AS SUM_VALUE FROM "
                        + nth_test_table + " GROUP BY page_id) x");

        assertTrue(rs.next());
        assertEquals(13, rs.getLong(1));
        assertFalse(rs.next());
    }

    @Test
    public void offsetValueLastMismatchByColumn() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String nth_test_table = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + nth_test_table + " "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
                + " \"DATE\" INTEGER, \"value\" UNSIGNED_LONG)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute(
            "UPSERT INTO " + nth_test_table + " (id, page_id, \"DATE\", \"value\") VALUES (1, 8, 5, 8)");
        conn.createStatement().execute(
            "UPSERT INTO " + nth_test_table + " (id, page_id, \"DATE\", \"value\") VALUES (2, 8, 2, 7)");
        conn.createStatement().execute(
            "UPSERT INTO " + nth_test_table + " (id, page_id, \"DATE\", \"value\") VALUES (3, 8, 1, 9)");
        conn.createStatement().execute(
            "UPSERT INTO " + nth_test_table + " (id, page_id, \"DATE\", \"value\") VALUES (4, 8, 4, 4)");
        conn.createStatement().execute(
            "UPSERT INTO " + nth_test_table + " (id, page_id, \"DATE\", \"value\") VALUES (5, 8, 3, 2)");
        conn.createStatement().execute(
            "UPSERT INTO " + nth_test_table + " (id, page_id, \"DATE\", \"value\") VALUES (6, 8, 0, 1)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT NTH_VALUE(\"value\", 2)  WITHIN GROUP (ORDER BY \"DATE\" DESC) FROM "
                + nth_test_table + " GROUP BY page_id");

        assertTrue(rs.next());
        assertEquals(rs.getLong(1), 4);
        assertFalse(rs.next());
    }

    @Test
    public void testSortOrderInDataColWithOffset() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String nth_test_table = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + nth_test_table + " "
                + "(id INTEGER NOT NULL, page_id UNSIGNED_LONG,"
                + " dates BIGINT NOT NULL, \"value\" BIGINT NOT NULL CONSTRAINT pk PRIMARY KEY (id, dates, \"value\" DESC))";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute("UPSERT INTO " + nth_test_table
            + " (id, page_id, dates, \"value\") VALUES (1, 8, 1, 3)");
        conn.createStatement().execute("UPSERT INTO " + nth_test_table
            + " (id, page_id, dates, \"value\") VALUES (2, 8, 2, 7)");
        conn.createStatement().execute("UPSERT INTO " + nth_test_table
            + " (id, page_id, dates, \"value\") VALUES (3, 8, 3, 9)");
        conn.createStatement().execute("UPSERT INTO " + nth_test_table
            + " (id, page_id, dates, \"value\") VALUES (5, 8, 5, 158)");
        conn.createStatement().execute("UPSERT INTO " + nth_test_table
            + " (id, page_id, dates, \"value\") VALUES (4, 8, 4, 5)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT NTH_VALUE(\"value\", 2)  WITHIN GROUP (ORDER BY dates ASC) FROM "
                + nth_test_table + " GROUP BY page_id");

        assertTrue(rs.next());
        assertEquals(rs.getLong(1), 7);
        assertFalse(rs.next());
    }

    @Test
    public void nonUniqueValuesInOrderByAsc() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String nthValue = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + nthValue + " "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
                + " dates INTEGER, val INTEGER)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, page_id, dates, val) VALUES (2, 8, 1, 7)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, page_id, dates, val) VALUES (3, 8, 2, 9)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, page_id, dates, val) VALUES (4, 8, 2, 4)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, page_id, dates, val) VALUES (5, 8, 2, 2)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, page_id, dates, val) VALUES (6, 8, 3, 3)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT NTH_VALUE(val, 3) WITHIN GROUP (ORDER BY dates ASC) FROM " + nthValue
                + " GROUP BY page_id");

        assertTrue(rs.next());
        assertInIntArray(new int[]{2, 4, 9}, rs.getInt(1));
        assertFalse(rs.next());
    }

    @Test
    public void nonUniqueValuesInOrderByAscSkipDuplicit() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String nthValue = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + nthValue + " "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
                + " dates INTEGER, val INTEGER)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, page_id, dates, val) VALUES (2, 8, 1, 7)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, page_id, dates, val) VALUES (3, 8, 2, 9)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, page_id, dates, val) VALUES (4, 8, 2, 4)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, page_id, dates, val) VALUES (5, 8, 2, 2)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, page_id, dates, val) VALUES (6, 8, 3, 3)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT NTH_VALUE(val, 5) WITHIN GROUP (ORDER BY dates ASC) FROM " + nthValue
                + " GROUP BY page_id");

        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertFalse(rs.next());
    }

    @Test
    public void nonUniqueValuesInOrderByDesc() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String nthValue = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + nthValue + " "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
                + " dates INTEGER, val INTEGER)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, page_id, dates, val) VALUES (2, 8, 1, 7)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, page_id, dates, val) VALUES (3, 8, 2, 9)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, page_id, dates, val) VALUES (4, 8, 2, 4)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, page_id, dates, val) VALUES (5, 8, 2, 2)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, page_id, dates, val) VALUES (6, 8, 3, 3)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT NTH_VALUE(val, 3) WITHIN GROUP (ORDER BY dates DESC) FROM " + nthValue
                + " GROUP BY page_id");

        assertTrue(rs.next());
        assertInIntArray(new int[]{2, 4, 9}, rs.getInt(1));
        assertFalse(rs.next());
    }

    @Test
    public void nonUniqueValuesInOrderNextValueDesc() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String nthValue = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + nthValue + " "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
                + " dates INTEGER, val INTEGER)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, page_id, dates, val) VALUES (2, 8, 0, 7)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, page_id, dates, val) VALUES (3, 8, 1, 9)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, page_id, dates, val) VALUES (4, 8, 2, 4)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, page_id, dates, val) VALUES (5, 8, 2, 2)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, page_id, dates, val) VALUES (6, 8, 3, 3)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, page_id, dates, val) VALUES (7, 8, 3, 5)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT NTH_VALUE(val, 2) WITHIN GROUP (ORDER BY dates DESC) FROM " + nthValue
                + " GROUP BY page_id");

        assertTrue(rs.next());
        assertInIntArray(new int[]{3, 5}, rs.getInt(1));
        assertFalse(rs.next());
    }

    @Test
    public void nonUniqueValuesInOrderNextValueAsc() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String nthValue = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + nthValue + " "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
                + " dates INTEGER, val INTEGER)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, page_id, dates, val) VALUES (2, 8, 0, 7)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, page_id, dates, val) VALUES (3, 8, 1, 9)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, page_id, dates, val) VALUES (4, 8, 2, 4)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, page_id, dates, val) VALUES (5, 8, 2, 2)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, page_id, dates, val) VALUES (6, 8, 3, 3)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, page_id, dates, val) VALUES (7, 8, 3, 5)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT NTH_VALUE(val, 5) WITHIN GROUP (ORDER BY dates ASC) FROM " + nthValue
                + " GROUP BY page_id");

        assertTrue(rs.next());
        assertInIntArray(new int[]{3, 5}, rs.getInt(1));
        assertFalse(rs.next());
    }

    @Test
    public void ignoreNullValues() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String nth_test_table = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + nth_test_table + " "
                + "(id INTEGER NOT NULL, page_id UNSIGNED_LONG,"
                + " dates BIGINT NOT NULL, \"value\" BIGINT NULL CONSTRAINT pk PRIMARY KEY (id, dates))";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute("UPSERT INTO " + nth_test_table
            + " (id, page_id, dates, \"value\") VALUES (1, 8, 1, 1)");
        conn.createStatement().execute("UPSERT INTO " + nth_test_table
            + " (id, page_id, dates, \"value\") VALUES (2, 8, 2, NULL)");
        conn.createStatement().execute("UPSERT INTO " + nth_test_table
            + " (id, page_id, dates, \"value\") VALUES (3, 8, 3, NULL)");
        conn.createStatement().execute("UPSERT INTO " + nth_test_table
            + " (id, page_id, dates, \"value\") VALUES (5, 8, 4, 4)");
        conn.createStatement().execute("UPSERT INTO " + nth_test_table
            + " (id, page_id, dates, \"value\") VALUES (4, 8, 5, 5)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT NTH_VALUE(\"value\", 2)  WITHIN GROUP (ORDER BY dates DESC) FROM "
                + nth_test_table + " GROUP BY page_id");

        assertTrue(rs.next());
        assertEquals(rs.getLong(1), 4);
        assertFalse(rs.next());
    }

    private void assertInIntArray(int[] should, int actualValue) {
        ArrayList<Integer> shouldList = new ArrayList<Integer>();
        for (int i: should) {
            shouldList.add(i);
        }
        assertTrue(shouldList.contains(actualValue));
    }
    
    @Test
    public void testUnionAll() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String nthValue = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + nthValue + " "
                + "(id INTEGER NOT NULL, feid UNSIGNED_LONG NOT NULL,"
                + " uid CHAR(1) NOT NULL, lrd INTEGER"
                + " CONSTRAINT PKVIEW PRIMARY KEY ( id, feid, uid))";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, feid, uid, lrd) VALUES (2, 8, '1', 7)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, feid, uid, lrd) VALUES (2, 8, '2', 9)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, feid, uid, lrd) VALUES (2, 8, '3', 4)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, feid, uid, lrd) VALUES (2, 8, '4', 2)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, feid, uid, lrd) VALUES (2, 9, '5', 1)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, feid, uid, lrd) VALUES (2, 9, '6', 3)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, feid, uid, lrd) VALUES (2, 9, '8', 5)");
        conn.createStatement().execute(
            "UPSERT INTO " + nthValue + " (id, feid, uid, lrd) VALUES (2, 9, '7', 8)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT feid, NTH_VALUE(uid, 1) WITHIN GROUP (ORDER BY lrd DESC) as user_id, NTH_VALUE(lrd, 1) WITHIN GROUP (ORDER BY lrd DESC) as lrd FROM " + nthValue
                + " where id=2 and feid in (8, 9) GROUP BY feid" 
                + " UNION ALL" 
                + " SELECT feid, NTH_VALUE(uid, 2) WITHIN GROUP (ORDER BY lrd DESC) as user_id, NTH_VALUE(lrd, 2) WITHIN GROUP (ORDER BY lrd DESC) as lrd  FROM " + nthValue
                + " where id=2 and feid in (8, 9) GROUP BY feid");
        
        assertTrue(rs.next());
        assertEquals(8, rs.getInt(1));
        assertEquals("2", rs.getString(2));
        assertEquals(9, rs.getInt(3));
        assertTrue(rs.next());
        assertEquals(9, rs.getInt(1));
        assertEquals("7", rs.getString(2));
        assertEquals(8, rs.getInt(3));
        assertTrue(rs.next());
        assertEquals(8, rs.getInt(1));
        assertEquals("1", rs.getString(2));
        assertEquals(7, rs.getInt(3));
        assertTrue(rs.next());
        assertEquals(9, rs.getInt(1));
        assertEquals("8", rs.getString(2));
        assertEquals(5, rs.getInt(3));
        assertFalse(rs.next());
    }

}
