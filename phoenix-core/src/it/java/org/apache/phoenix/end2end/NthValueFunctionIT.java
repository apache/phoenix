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


public class NthValueFunctionIT extends BaseHBaseManagedTimeIT {

    @Test
    public void simpleTest() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String ddl = "CREATE TABLE IF NOT EXISTS nthValue "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
                + " dates INTEGER, val INTEGER)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute("UPSERT INTO nthValue (id, page_id, dates, val) VALUES (2, 8, 1, 7)");
        conn.createStatement().execute("UPSERT INTO nthValue (id, page_id, dates, val) VALUES (3, 8, 2, 9)");
        conn.createStatement().execute("UPSERT INTO nthValue (id, page_id, dates, val) VALUES (4, 8, 3, 4)");
        conn.createStatement().execute("UPSERT INTO nthValue (id, page_id, dates, val) VALUES (5, 8, 4, 2)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT NTH_VALUE(val, 2) WITHIN GROUP (ORDER BY dates DESC) FROM nthValue GROUP BY page_id");

        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 4);
        assertFalse(rs.next());
    }

    @Test
    public void offsetValueAscOrder() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String ddl = "CREATE TABLE IF NOT EXISTS nth_test_table "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
                + " date INTEGER, \"value\" UNSIGNED_LONG)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute("UPSERT INTO nth_test_table (id, page_id, date, \"value\") VALUES (1, 8, 0, 300)");
        conn.createStatement().execute("UPSERT INTO nth_test_table (id, page_id, date, \"value\") VALUES (2, 8, 1, 7)");
        conn.createStatement().execute("UPSERT INTO nth_test_table (id, page_id, date, \"value\") VALUES (3, 8, 2, 9)");
        conn.createStatement().execute("UPSERT INTO nth_test_table (id, page_id, date, \"value\") VALUES (4, 8, 3, 4)");
        conn.createStatement().execute("UPSERT INTO nth_test_table (id, page_id, date, \"value\") VALUES (5, 8, 4, 2)");
        conn.createStatement().execute("UPSERT INTO nth_test_table (id, page_id, date, \"value\") VALUES (6, 8, 5, 150)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT NTH_VALUE(\"value\", 2)  WITHIN GROUP (ORDER BY date ASC) FROM nth_test_table GROUP BY page_id");

        assertTrue(rs.next());
        assertEquals(rs.getLong(1), 7);
        assertFalse(rs.next());
    }

    @Test
    public void offsetValueDescOrder() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String ddl = "CREATE TABLE IF NOT EXISTS nth_test_table "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
                + " date INTEGER, \"value\" UNSIGNED_LONG)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute("UPSERT INTO nth_test_table (id, page_id, date, \"value\") VALUES (1, 8, 0, 300)");
        conn.createStatement().execute("UPSERT INTO nth_test_table (id, page_id, date, \"value\") VALUES (2, 8, 1, 7)");
        conn.createStatement().execute("UPSERT INTO nth_test_table (id, page_id, date, \"value\") VALUES (3, 8, 2, 9)");
        conn.createStatement().execute("UPSERT INTO nth_test_table (id, page_id, date, \"value\") VALUES (4, 8, 3, 4)");
        conn.createStatement().execute("UPSERT INTO nth_test_table (id, page_id, date, \"value\") VALUES (5, 8, 4, 2)");
        conn.createStatement().execute("UPSERT INTO nth_test_table (id, page_id, date, \"value\") VALUES (6, 8, 5, 150)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT NTH_VALUE(\"value\", 2)  WITHIN GROUP (ORDER BY date DESC) FROM nth_test_table GROUP BY page_id");

        assertTrue(rs.next());
        assertEquals(rs.getLong(1), 2);
        assertFalse(rs.next());
    }

    @Test
    public void offsetValueLastMismatchByColumn() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String ddl = "CREATE TABLE IF NOT EXISTS nth_test_table "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
                + " date INTEGER, \"value\" UNSIGNED_LONG)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute("UPSERT INTO nth_test_table (id, page_id, date, \"value\") VALUES (1, 8, 5, 8)");
        conn.createStatement().execute("UPSERT INTO nth_test_table (id, page_id, date, \"value\") VALUES (2, 8, 2, 7)");
        conn.createStatement().execute("UPSERT INTO nth_test_table (id, page_id, date, \"value\") VALUES (3, 8, 1, 9)");
        conn.createStatement().execute("UPSERT INTO nth_test_table (id, page_id, date, \"value\") VALUES (4, 8, 4, 4)");
        conn.createStatement().execute("UPSERT INTO nth_test_table (id, page_id, date, \"value\") VALUES (5, 8, 3, 2)");
        conn.createStatement().execute("UPSERT INTO nth_test_table (id, page_id, date, \"value\") VALUES (6, 8, 0, 1)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT NTH_VALUE(\"value\", 2)  WITHIN GROUP (ORDER BY date DESC) FROM nth_test_table GROUP BY page_id");

        assertTrue(rs.next());
        assertEquals(rs.getLong(1), 4);
        assertFalse(rs.next());
    }

    @Test
    public void testSortOrderInDataColWithOffset() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String ddl = "CREATE TABLE IF NOT EXISTS nth_test_table "
                + "(id INTEGER NOT NULL, page_id UNSIGNED_LONG,"
                + " dates BIGINT NOT NULL, \"value\" BIGINT NOT NULL CONSTRAINT pk PRIMARY KEY (id, dates, \"value\" DESC))";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute("UPSERT INTO nth_test_table (id, page_id, dates, \"value\") VALUES (1, 8, 1, 3)");
        conn.createStatement().execute("UPSERT INTO nth_test_table (id, page_id, dates, \"value\") VALUES (2, 8, 2, 7)");
        conn.createStatement().execute("UPSERT INTO nth_test_table (id, page_id, dates, \"value\") VALUES (3, 8, 3, 9)");
        conn.createStatement().execute("UPSERT INTO nth_test_table (id, page_id, dates, \"value\") VALUES (5, 8, 5, 158)");
        conn.createStatement().execute("UPSERT INTO nth_test_table (id, page_id, dates, \"value\") VALUES (4, 8, 4, 5)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT NTH_VALUE(\"value\", 2)  WITHIN GROUP (ORDER BY dates ASC) FROM nth_test_table GROUP BY page_id");

        assertTrue(rs.next());
        assertEquals(rs.getLong(1), 7);
        assertFalse(rs.next());
    }

    @Test
    public void nonUniqueValuesInOrderByAsc() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String ddl = "CREATE TABLE IF NOT EXISTS nthValue "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
                + " dates INTEGER, val INTEGER)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute("UPSERT INTO nthValue (id, page_id, dates, val) VALUES (2, 8, 1, 7)");
        conn.createStatement().execute("UPSERT INTO nthValue (id, page_id, dates, val) VALUES (3, 8, 2, 9)");
        conn.createStatement().execute("UPSERT INTO nthValue (id, page_id, dates, val) VALUES (4, 8, 2, 4)");
        conn.createStatement().execute("UPSERT INTO nthValue (id, page_id, dates, val) VALUES (5, 8, 2, 2)");
        conn.createStatement().execute("UPSERT INTO nthValue (id, page_id, dates, val) VALUES (6, 8, 3, 3)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT NTH_VALUE(val, 3) WITHIN GROUP (ORDER BY dates ASC) FROM nthValue GROUP BY page_id");

        assertTrue(rs.next());
        assertInIntArray(new int[]{2, 4, 9}, rs.getInt(1));
        assertFalse(rs.next());
    }

    @Test
    public void nonUniqueValuesInOrderByAscSkipDuplicit() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String ddl = "CREATE TABLE IF NOT EXISTS nthValue "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
                + " dates INTEGER, val INTEGER)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute("UPSERT INTO nthValue (id, page_id, dates, val) VALUES (2, 8, 1, 7)");
        conn.createStatement().execute("UPSERT INTO nthValue (id, page_id, dates, val) VALUES (3, 8, 2, 9)");
        conn.createStatement().execute("UPSERT INTO nthValue (id, page_id, dates, val) VALUES (4, 8, 2, 4)");
        conn.createStatement().execute("UPSERT INTO nthValue (id, page_id, dates, val) VALUES (5, 8, 2, 2)");
        conn.createStatement().execute("UPSERT INTO nthValue (id, page_id, dates, val) VALUES (6, 8, 3, 3)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT NTH_VALUE(val, 5) WITHIN GROUP (ORDER BY dates ASC) FROM nthValue GROUP BY page_id");

        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertFalse(rs.next());
    }

    @Test
    public void nonUniqueValuesInOrderByDesc() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String ddl = "CREATE TABLE IF NOT EXISTS nthValue "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
                + " dates INTEGER, val INTEGER)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute("UPSERT INTO nthValue (id, page_id, dates, val) VALUES (2, 8, 1, 7)");
        conn.createStatement().execute("UPSERT INTO nthValue (id, page_id, dates, val) VALUES (3, 8, 2, 9)");
        conn.createStatement().execute("UPSERT INTO nthValue (id, page_id, dates, val) VALUES (4, 8, 2, 4)");
        conn.createStatement().execute("UPSERT INTO nthValue (id, page_id, dates, val) VALUES (5, 8, 2, 2)");
        conn.createStatement().execute("UPSERT INTO nthValue (id, page_id, dates, val) VALUES (6, 8, 3, 3)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT NTH_VALUE(val, 3) WITHIN GROUP (ORDER BY dates DESC) FROM nthValue GROUP BY page_id");

        assertTrue(rs.next());
        assertInIntArray(new int[]{2, 4, 9}, rs.getInt(1));
        assertFalse(rs.next());
    }

    @Test
    public void nonUniqueValuesInOrderNextValueDesc() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String ddl = "CREATE TABLE IF NOT EXISTS nthValue "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
                + " dates INTEGER, val INTEGER)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute("UPSERT INTO nthValue (id, page_id, dates, val) VALUES (2, 8, 0, 7)");
        conn.createStatement().execute("UPSERT INTO nthValue (id, page_id, dates, val) VALUES (3, 8, 1, 9)");
        conn.createStatement().execute("UPSERT INTO nthValue (id, page_id, dates, val) VALUES (4, 8, 2, 4)");
        conn.createStatement().execute("UPSERT INTO nthValue (id, page_id, dates, val) VALUES (5, 8, 2, 2)");
        conn.createStatement().execute("UPSERT INTO nthValue (id, page_id, dates, val) VALUES (6, 8, 3, 3)");
        conn.createStatement().execute("UPSERT INTO nthValue (id, page_id, dates, val) VALUES (7, 8, 3, 5)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT NTH_VALUE(val, 2) WITHIN GROUP (ORDER BY dates DESC) FROM nthValue GROUP BY page_id");

        assertTrue(rs.next());
        assertInIntArray(new int[]{3, 5}, rs.getInt(1));
        assertFalse(rs.next());
    }

    @Test
    public void nonUniqueValuesInOrderNextValueAsc() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String ddl = "CREATE TABLE IF NOT EXISTS nthValue "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
                + " dates INTEGER, val INTEGER)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute("UPSERT INTO nthValue (id, page_id, dates, val) VALUES (2, 8, 0, 7)");
        conn.createStatement().execute("UPSERT INTO nthValue (id, page_id, dates, val) VALUES (3, 8, 1, 9)");
        conn.createStatement().execute("UPSERT INTO nthValue (id, page_id, dates, val) VALUES (4, 8, 2, 4)");
        conn.createStatement().execute("UPSERT INTO nthValue (id, page_id, dates, val) VALUES (5, 8, 2, 2)");
        conn.createStatement().execute("UPSERT INTO nthValue (id, page_id, dates, val) VALUES (6, 8, 3, 3)");
        conn.createStatement().execute("UPSERT INTO nthValue (id, page_id, dates, val) VALUES (7, 8, 3, 5)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT NTH_VALUE(val, 5) WITHIN GROUP (ORDER BY dates ASC) FROM nthValue GROUP BY page_id");

        assertTrue(rs.next());
        assertInIntArray(new int[]{3, 5}, rs.getInt(1));
        assertFalse(rs.next());
    }

    @Test
    public void ignoreNullValues() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String ddl = "CREATE TABLE IF NOT EXISTS nth_test_table "
                + "(id INTEGER NOT NULL, page_id UNSIGNED_LONG,"
                + " dates BIGINT NOT NULL, \"value\" BIGINT NULL CONSTRAINT pk PRIMARY KEY (id, dates))";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute("UPSERT INTO nth_test_table (id, page_id, dates, \"value\") VALUES (1, 8, 1, 1)");
        conn.createStatement().execute("UPSERT INTO nth_test_table (id, page_id, dates, \"value\") VALUES (2, 8, 2, NULL)");
        conn.createStatement().execute("UPSERT INTO nth_test_table (id, page_id, dates, \"value\") VALUES (3, 8, 3, NULL)");
        conn.createStatement().execute("UPSERT INTO nth_test_table (id, page_id, dates, \"value\") VALUES (5, 8, 4, 4)");
        conn.createStatement().execute("UPSERT INTO nth_test_table (id, page_id, dates, \"value\") VALUES (4, 8, 5, 5)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT NTH_VALUE(\"value\", 2)  WITHIN GROUP (ORDER BY dates DESC) FROM nth_test_table GROUP BY page_id");

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

}
