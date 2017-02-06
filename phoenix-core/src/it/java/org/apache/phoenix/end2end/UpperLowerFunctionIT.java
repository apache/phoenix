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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.BeforeClass;
import org.junit.Test;

public class UpperLowerFunctionIT extends ParallelStatsDisabledIT {
    private static String tableName = generateUniqueName();
    private static String firstName = "Joe";
    private static String lastName = "Smith";

    @BeforeClass
    public static void init() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE " + tableName + " ("
                + "id INTEGER PRIMARY KEY,"
                + "first_name VARCHAR,"
                + "last_name VARCHAR"
                + ")";
        conn.createStatement().execute(ddl);
        String dml = String.format("UPSERT INTO %s VALUES("
                        + "1, '%s', '%s'"
                        + ")",
                tableName, firstName, lastName);
        PreparedStatement stmt = conn.prepareStatement(dml);
        stmt.execute();
        conn.commit();
    }

    @Test
    public void testWhereLower() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());

        ResultSet rs;
        rs = conn.createStatement().executeQuery(String.format(
                "SELECT first_name, last_name FROM %s WHERE"
                        + " LOWER(first_name || ' ' || last_name)"
                        + " = '%s %s'",
                tableName, firstName.toLowerCase(), lastName.toLowerCase()));
        assertTrue(rs.next());
        assertEquals(firstName, rs.getString(1));
        assertEquals(lastName, rs.getString(2));
        assertFalse(rs.next());
    }

    @Test
    public void testSelectLower() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());

        ResultSet rs;
        rs = conn.createStatement().executeQuery(String.format(
                "SELECT LOWER(first_name || ' ' || last_name) FROM %s WHERE"
                        + " id = 1",
                tableName));
        assertTrue(rs.next());
        String expected = String.format("%s %s",
                firstName.toLowerCase(), lastName.toLowerCase());
        assertEquals(expected, rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testWhereUpper() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());

        ResultSet rs;
        rs = conn.createStatement().executeQuery(String.format(
                "SELECT first_name, last_name FROM %s WHERE"
                        + " UPPER(first_name || ' ' || last_name)"
                        + " = '%s %s'",
                tableName, firstName.toUpperCase(), lastName.toUpperCase()));
        assertTrue(rs.next());
        assertEquals(firstName, rs.getString(1));
        assertEquals(lastName, rs.getString(2));
        assertFalse(rs.next());
    }

    @Test
    public void testSelectUpper() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());

        ResultSet rs;
        rs = conn.createStatement().executeQuery(String.format(
                "SELECT UPPER(first_name || ' ' || last_name) FROM %s WHERE"
                        + " id = 1",
                tableName));
        assertTrue(rs.next());
        String expected = String.format("%s %s",
                firstName.toUpperCase(), lastName.toUpperCase());
        assertEquals(expected, rs.getString(1));
        assertFalse(rs.next());
    }
}
