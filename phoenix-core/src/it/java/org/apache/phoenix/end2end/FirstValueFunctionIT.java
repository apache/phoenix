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

import org.junit.Test;

public class FirstValueFunctionIT extends BaseHBaseManagedTimeIT {

    @Test
    public void signedLongAsBigInt() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String ddl = "CREATE TABLE IF NOT EXISTS first_value_table "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
                + " date BIGINT, \"value\" BIGINT)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute("UPSERT INTO first_value_table (id, page_id, date, \"value\") VALUES (1, 8, 1, 3)");
        conn.createStatement().execute("UPSERT INTO first_value_table (id, page_id, date, \"value\") VALUES (2, 8, 2, 7)");
        conn.createStatement().execute("UPSERT INTO first_value_table (id, page_id, date, \"value\") VALUES (3, 8, 3, 9)");
        conn.createStatement().execute("UPSERT INTO first_value_table (id, page_id, date, \"value\") VALUES (5, 8, 5, 158)");
        conn.createStatement().execute("UPSERT INTO first_value_table (id, page_id, date, \"value\") VALUES (4, 8, 4, 5)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT FIRST_VALUE(\"value\") WITHIN GROUP (ORDER BY date ASC) FROM first_value_table GROUP BY page_id");

        assertTrue(rs.next());
        assertEquals(rs.getLong(1), 3);
        assertFalse(rs.next());
    }

    @Test
    public void testSortOrderInSortCol() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String ddl = "CREATE TABLE IF NOT EXISTS first_value_table "
                + "(id INTEGER NOT NULL, page_id UNSIGNED_LONG,"
                + " dates BIGINT NOT NULL, \"value\" BIGINT CONSTRAINT pk PRIMARY KEY (id, dates DESC))";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute("UPSERT INTO first_value_table (id, page_id, dates, \"value\") VALUES (1, 8, 1, 3)");
        conn.createStatement().execute("UPSERT INTO first_value_table (id, page_id, dates, \"value\") VALUES (2, 8, 2, 7)");
        conn.createStatement().execute("UPSERT INTO first_value_table (id, page_id, dates, \"value\") VALUES (3, 8, 3, 9)");
        conn.createStatement().execute("UPSERT INTO first_value_table (id, page_id, dates, \"value\") VALUES (5, 8, 5, 158)");
        conn.createStatement().execute("UPSERT INTO first_value_table (id, page_id, dates, \"value\") VALUES (4, 8, 4, 5)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT FIRST_VALUE(\"value\") WITHIN GROUP (ORDER BY dates ASC) FROM first_value_table GROUP BY page_id");

        assertTrue(rs.next());
        assertEquals(rs.getLong(1), 3);
        assertFalse(rs.next());
    }

    @Test
    public void testSortOrderInDataCol() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String ddl = "CREATE TABLE IF NOT EXISTS first_value_table "
                + "(id INTEGER NOT NULL, page_id UNSIGNED_LONG,"
                + " dates BIGINT NOT NULL, \"value\" BIGINT NOT NULL CONSTRAINT pk PRIMARY KEY (id, dates, \"value\" DESC))";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute("UPSERT INTO first_value_table (id, page_id, dates, \"value\") VALUES (1, 8, 1, 3)");
        conn.createStatement().execute("UPSERT INTO first_value_table (id, page_id, dates, \"value\") VALUES (2, 8, 2, 7)");
        conn.createStatement().execute("UPSERT INTO first_value_table (id, page_id, dates, \"value\") VALUES (3, 8, 3, 9)");
        conn.createStatement().execute("UPSERT INTO first_value_table (id, page_id, dates, \"value\") VALUES (5, 8, 5, 158)");
        conn.createStatement().execute("UPSERT INTO first_value_table (id, page_id, dates, \"value\") VALUES (4, 8, 4, 5)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT FIRST_VALUE(\"value\") WITHIN GROUP (ORDER BY dates ASC) FROM first_value_table GROUP BY page_id");

        assertTrue(rs.next());
        assertEquals(rs.getLong(1), 3);
        assertFalse(rs.next());
    }

    @Test
    public void doubleDataType() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String ddl = "CREATE TABLE IF NOT EXISTS first_value_table "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG, "
                + "date DOUBLE, \"value\" DOUBLE)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute("UPSERT INTO first_value_table (id, page_id, date, \"value\") VALUES (1, 8, 1, 300)");
        conn.createStatement().execute("UPSERT INTO first_value_table (id, page_id, date, \"value\") VALUES (2, 8, 2, 7)");
        conn.createStatement().execute("UPSERT INTO first_value_table (id, page_id, date, \"value\") VALUES (3, 8, 3, 9)");
        conn.createStatement().execute("UPSERT INTO first_value_table (id, page_id, date, \"value\") VALUES (5, 8, 4, 2)");
        conn.createStatement().execute("UPSERT INTO first_value_table (id, page_id, date, \"value\") VALUES (4, 8, 5, 400)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT FIRST_VALUE(\"value\") WITHIN GROUP (ORDER BY date ASC) FROM first_value_table GROUP BY page_id");

        assertTrue(rs.next());
        assertEquals("doubles", rs.getDouble(1), 300, 0.00001);
        assertFalse(rs.next());
    }

    @Test
    public void varcharFixedLenghtDatatype() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String ddl = "CREATE TABLE IF NOT EXISTS first_value_table "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG, "
                + "date VARCHAR(3), \"value\" VARCHAR(3))";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute("UPSERT INTO first_value_table (id, page_id, date, \"value\") VALUES (1, 8, '1', '3')");
        conn.createStatement().execute("UPSERT INTO first_value_table (id, page_id, date, \"value\") VALUES (2, 8, '2', '7')");
        conn.createStatement().execute("UPSERT INTO first_value_table (id, page_id, date, \"value\") VALUES (3, 8, '3', '9')");
        conn.createStatement().execute("UPSERT INTO first_value_table (id, page_id, date, \"value\") VALUES (5, 8, '4', '2')");
        conn.createStatement().execute("UPSERT INTO first_value_table (id, page_id, date, \"value\") VALUES (4, 8, '5', '4')");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT FIRST_VALUE(\"value\") WITHIN GROUP (ORDER BY date ASC) FROM first_value_table GROUP BY page_id");

        assertTrue(rs.next());
        assertEquals(rs.getString(1), "3");
        assertFalse(rs.next());
    }

    @Test
    public void floatDataType() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String ddl = "CREATE TABLE IF NOT EXISTS first_value_table "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
                + " date FLOAT, \"value\" FLOAT)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute("UPSERT INTO first_value_table (id, page_id, date, \"value\") VALUES (1, 8, 1, 300)");
        conn.createStatement().execute("UPSERT INTO first_value_table (id, page_id, date, \"value\") VALUES (2, 8, 2, 7)");
        conn.createStatement().execute("UPSERT INTO first_value_table (id, page_id, date, \"value\") VALUES (3, 8, 3, 9)");
        conn.createStatement().execute("UPSERT INTO first_value_table (id, page_id, date, \"value\") VALUES (5, 8, 4, 2)");
        conn.createStatement().execute("UPSERT INTO first_value_table (id, page_id, date, \"value\") VALUES (4, 8, 5, 400)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT FIRST_VALUE(\"value\") WITHIN GROUP (ORDER BY date ASC) FROM first_value_table GROUP BY page_id");

        assertTrue(rs.next());
        assertEquals(rs.getFloat(1), 300.0, 0.000001);
        assertFalse(rs.next());

    }

    @Test
    public void allColumnsNull() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String ddl = "CREATE TABLE IF NOT EXISTS first_value_table "
                + "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
                + " date FLOAT, \"value\" FLOAT)";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute("UPSERT INTO first_value_table (id, page_id) VALUES (1, 8)");
        conn.createStatement().execute("UPSERT INTO first_value_table (id, page_id) VALUES (2, 8)");
        conn.createStatement().execute("UPSERT INTO first_value_table (id, page_id) VALUES (3, 8)");
        conn.createStatement().execute("UPSERT INTO first_value_table (id, page_id) VALUES (5, 8)");
        conn.createStatement().execute("UPSERT INTO first_value_table (id, page_id) VALUES (4, 8)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT FIRST_VALUE(\"value\") WITHIN GROUP (ORDER BY date ASC) FROM first_value_table GROUP BY page_id");

        assertTrue(rs.next());
        byte[] nothing = rs.getBytes(1);
        assertTrue(nothing == null);
    }

}
