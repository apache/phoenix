/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class DecodeBinaryFunctionIT extends ParallelStatsDisabledIT {

    @Test
    public void testDecodeHBaseFromEscapedHex() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String testTable = generateUniqueName();
        String ddl = "CREATE TABLE " + testTable + " (id INTEGER PRIMARY KEY, data VARCHAR)";

        conn.createStatement().execute(ddl);

        PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + testTable + " (id, data) VALUES (1, 'data')");
        ps.execute();
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT DECODE_BINARY('\\\\x48\\\\x65\\\\x6C\\\\x6C\\\\x6F\\\\x50" +
                        "\\\\x68\\\\x6F\\\\x65\\\\x6E\\\\x69\\\\x78', " + "'HBASE') FROM " + testTable);
        assertTrue(rs.next());
        byte[] actualBytes = rs.getBytes(1);
        assertTrue(Arrays.equals("HelloPhoenix".getBytes(), actualBytes));
    }

    @Test
    public void testDecodeHBaseFromHexLiteral() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String testTable = generateUniqueName();
        String ddl = "CREATE TABLE " + testTable + " (id INTEGER PRIMARY KEY, data VARCHAR)";

        conn.createStatement().execute(ddl);

        PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + testTable + " (id, data) VALUES (1, 'data')");
        ps.execute();
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT DECODE_BINARY('\\x48\\x65\\x6C\\x6C\\x6F\\x50\\x68\\x6F\\x65\\x6E\\x69\\x78', " +
                        "'HBASE') FROM " + testTable);
        assertTrue(rs.next());
        byte[] actualBytes = rs.getBytes(1);
        assertTrue(Arrays.equals("HelloPhoenix".getBytes(), actualBytes));
    }

    @Test
    public void testInvalidDecodingFormat() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String testTable = generateUniqueName();
        String ddl = "CREATE TABLE " + testTable + " (id INTEGER PRIMARY KEY, data VARCHAR)";

        conn.createStatement().execute(ddl);

        try {
            conn.createStatement().executeQuery("SELECT DECODE_BINARY(data, 'INVALIDFORMAT') FROM " + testTable);
            fail("Expected an exception for invalid encoding format");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TYPE_MISMATCH.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testNullAndEmptyStringDecoding() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String testTable = generateUniqueName();
        String ddl = "CREATE TABLE " + testTable + " (id INTEGER PRIMARY KEY, data VARCHAR)";

        conn.createStatement().execute(ddl);

        ResultSet rs = conn.createStatement().executeQuery("SELECT DECODE_BINARY(data, 'HBASE') FROM " + testTable);
        assertFalse(rs.next());

        ResultSet rs2 = conn.createStatement().executeQuery("SELECT DECODE_BINARY(NULL, 'HEX') FROM " + testTable);
        assertFalse(rs2.next());
    }

    @Test
    public void testLongBase64Decoding() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String testTable = generateUniqueName();
        String ddl = "CREATE TABLE " + testTable + " (id INTEGER PRIMARY KEY, data VARCHAR)";
        conn.createStatement().execute(ddl);

        String base64Chunk = "SGVsbG9QaG9lbml4";
        StringBuilder base64String = new StringBuilder();
        for (int i = 0; i < 20; i++) {
            base64String.append(base64Chunk);
        }

        PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + testTable + " (id, data) VALUES (1, ?)");
        ps.setString(1, base64String.toString());
        ps.execute();
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT DECODE_BINARY(data, 'BASE64') FROM " + testTable);
        assertTrue(rs.next());

        StringBuilder expectedString = new StringBuilder();
        for (int i = 0; i < 20; i++) {
            expectedString.append("HelloPhoenix");
        }

        byte[] actualBytes = rs.getBytes(1);
        assertTrue(Arrays.equals(expectedString.toString().getBytes(), actualBytes));
        assertFalse(rs.next());
    }

    @Test
    public void testDecodeEncodeRoundHex() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String testTable = generateUniqueName();
        String ddl = "CREATE TABLE " + testTable + " (id INTEGER PRIMARY KEY, data VARCHAR)";
        conn.createStatement().execute(ddl);

        PreparedStatement ps =
                conn.prepareStatement("UPSERT INTO " + testTable + " (id, data) VALUES (1, '48656C6C6F')");
        ps.execute();
        conn.commit();

        ResultSet rs = conn.createStatement()
                .executeQuery("SELECT ENCODE_BINARY(DECODE_BINARY(data, 'HEX'), 'HEX') FROM " + testTable);
        assertTrue(rs.next());
        String actualString = rs.getString(1);
        assertEquals("48656c6c6f", actualString);
        assertFalse(rs.next());
    }

    @Test
    public void testDecodeEncodeRoundBase64() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String testTable = generateUniqueName();
        String ddl = "CREATE TABLE " + testTable + " (id INTEGER PRIMARY KEY, data VARCHAR)";
        conn.createStatement().execute(ddl);

        PreparedStatement ps =
                conn.prepareStatement("UPSERT INTO " + testTable + " (id, data) VALUES (1, 'SGVsbG9QaG9lbml4')");
        ps.execute();
        conn.commit();

        ResultSet rs = conn.createStatement()
                .executeQuery("SELECT ENCODE_BINARY(DECODE_BINARY(data, 'BASE64'), 'BASE64') FROM " + testTable);
        assertTrue(rs.next());
        String actualString = rs.getString(1);
        assertEquals("SGVsbG9QaG9lbml4", actualString);
        assertFalse(rs.next());
    }

    @Test
    public void testDecodeEncodeRoundHbase() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String testTable = generateUniqueName();
        String ddl = "CREATE TABLE " + testTable + " (id INTEGER PRIMARY KEY, data VARCHAR)";
        conn.createStatement().execute(ddl);

        PreparedStatement ps =
                conn.prepareStatement("UPSERT INTO " + testTable + " (id, data) VALUES (1, 'HelloPhoenix')");
        ps.execute();
        conn.commit();

        ResultSet rs = conn.createStatement()
                .executeQuery("SELECT ENCODE_BINARY(DECODE_BINARY(data, 'HBASE'), 'HBASE') FROM " + testTable);
        assertTrue(rs.next());
        String actualString = rs.getString(1);
        assertEquals("HelloPhoenix", actualString);
        assertFalse(rs.next());
    }
}