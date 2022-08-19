/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.phoenix.end2end;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * End to end tests for Hexadecimal and Binary literals
 */
@Category(ParallelStatsDisabledTest.class)
public class BinaryStringLiteralIT extends ParallelStatsDisabledIT {

    private static String EMPTY = "";
    private static String THREE_HEX = "0001AA";
    private static String NINE_HEX = "0102030405607080F0";

    private static String THREE_BIN = "00000000" + "00000001" + "10101010";
    private static String NINE_BIN =
            "00000001" + "00000010" + "00000011" + "00000100" + "00000101" + "01100000" + "01110000"
                    + "10000000" + "11110000";

    private String PARSER_STRESS = "x'0 12 ' --comment \n /* comment */ ' 34 567' \n \n 'aA'";

    private String toHex(String s) {
        return "X'" + s + "'";
    }

    private String toBin(String s) {
        return "B'" + s + "'";
    }

    private void insertRow(Statement stmt, String tableName, int id, String s) throws SQLException {
        stmt.executeUpdate("UPSERT INTO " + tableName + " VALUES (" + id + "," + s + "," + s + ")");
    }

    @Test
    public void testBinary() throws Exception {
        String tableName = generateUniqueName();

        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement();) {
            String ddl =
                    "CREATE TABLE " + tableName
                            + " (id INTEGER NOT NULL PRIMARY KEY, b BINARY(10), vb VARBINARY)";
            stmt.execute(ddl);
            conn.commit();

            insertRow(stmt, tableName, 1, toHex(EMPTY));
            insertRow(stmt, tableName, 3, toHex(THREE_HEX));
            insertRow(stmt, tableName, 9, toHex(NINE_HEX));
            insertRow(stmt, tableName, 10, PARSER_STRESS);

            insertRow(stmt, tableName, 101, toBin(EMPTY));
            insertRow(stmt, tableName, 103, toBin(THREE_BIN));
            insertRow(stmt, tableName, 109, toBin(NINE_BIN));

            conn.commit();
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " ORDER BY ID ASC");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(null, rs.getString(2));
            assertEquals(null, rs.getString(3));

            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertEquals("0001aa00000000000000", rs.getString(2));
            assertEquals("0001aa", rs.getString(3));

            assertTrue(rs.next());
            assertEquals(9, rs.getInt(1));
            assertEquals("0102030405607080f000", rs.getString(2));
            assertEquals("0102030405607080f0", rs.getString(3));

            assertTrue(rs.next());
            assertEquals(10, rs.getInt(1));
            assertEquals("01234567aa0000000000", rs.getString(2));
            assertEquals("01234567aa", rs.getString(3));

            assertTrue(rs.next());
            assertEquals(101, rs.getInt(1));
            assertEquals(null, rs.getString(2));
            assertEquals(null, rs.getString(3));

            assertTrue(rs.next());
            assertEquals(103, rs.getInt(1));
            assertEquals("0001aa00000000000000", rs.getString(2));
            assertEquals("0001aa", rs.getString(3));

            assertTrue(rs.next());
            assertEquals(109, rs.getInt(1));
            assertEquals("0102030405607080f000", rs.getString(2));
            assertEquals("0102030405607080f0", rs.getString(3));

            assertFalse(rs.next());
        }
    }

    @Test
    public void testBinaryArray() throws Exception {
        String tableName = generateUniqueName();

        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement();) {
            String ddl =
                    "CREATE TABLE " + tableName
                            + " (id INTEGER NOT NULL PRIMARY KEY, b BINARY(10), a BINARY(10)[])";
            stmt.execute(ddl);
            conn.commit();

            stmt.executeUpdate("UPSERT INTO " + tableName + " VALUES (" + 3 + "," + toHex(THREE_HEX) + ", ARRAY[" + toHex(THREE_HEX)+", "+toHex(THREE_HEX)+", "+toHex(THREE_HEX)+"])");

            conn.commit();
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " ORDER BY ID ASC");
            assertTrue(rs.next());
            assertEquals("0001aa00000000000000", rs.getString(2));
            //FIXME we're using a different string representation here than for the scalar values
            assertEquals("[X'0001aa00000000000000', X'0001aa00000000000000', X'0001aa00000000000000']", rs.getString(3));
            assertFalse(rs.next());
        }
    }

}
