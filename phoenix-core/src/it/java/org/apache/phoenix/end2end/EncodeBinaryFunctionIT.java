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
public class EncodeBinaryFunctionIT extends ParallelStatsDisabledIT {

    @Test
    public void testEncodeHex() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String testTable = generateUniqueName();
        String ddl = "CREATE TABLE " + testTable + " (id INTEGER PRIMARY KEY, data VARBINARY)";

        conn.createStatement().execute(ddl);

        PreparedStatement ps = conn.prepareStatement(
                "UPSERT INTO " + testTable + " (id, data) VALUES (1, X'48656c6c6f50686f656e6978')");
        ps.execute();
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT ENCODE_BINARY(data, 'HEX') FROM " + testTable);
        assertTrue(rs.next());
        String actualString = rs.getString(1);
        assertEquals("48656c6c6f50686f656e6978", actualString);
        assertFalse(rs.next());
    }

    @Test
    public void testMixedCaseHexDecoding() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String testTable = generateUniqueName();
        String ddl = "CREATE TABLE " + testTable + " (id INTEGER PRIMARY KEY, data VARBINARY)";
        conn.createStatement().execute(ddl);

        PreparedStatement ps = conn.prepareStatement(
                "UPSERT INTO " + testTable + " (id, data) VALUES (1, X'48656c6C6f50686F656e6978')");
        ps.execute();
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT ENCODE_BINARY(data, 'HEX') FROM " + testTable);
        assertTrue(rs.next());
        assertEquals("48656c6c6f50686f656e6978", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testEncodeBase64() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String testTable = generateUniqueName();
        String ddl = "CREATE TABLE " + testTable + " (id INTEGER PRIMARY KEY, data VARBINARY)";

        conn.createStatement().execute(ddl);

        byte[] originalEncoded = "HelloPhoenix".getBytes();
        PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + testTable + " (id, data) VALUES (1, ?)");
        ps.setBytes(1, originalEncoded);
        ps.execute();
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT ENCODE_BINARY(data, 'BASE64') FROM " + testTable);
        assertTrue(rs.next());
        String actualString = rs.getString(1);
        assertEquals("SGVsbG9QaG9lbml4", actualString);
        assertFalse(rs.next());
    }

    @Test
    public void testLongBase64Decoding() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String testTable = generateUniqueName();
        String ddl = "CREATE TABLE " + testTable + " (id INTEGER PRIMARY KEY, data VARBINARY)";
        conn.createStatement().execute(ddl);

        String base64Chunk = "HelloPhoenix";
        StringBuilder base64String = new StringBuilder();
        for (int i = 0; i < 20; i++) {
            base64String.append(base64Chunk);
        }

        PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + testTable + " (id, data) VALUES (1, ?)");
        ps.setBytes(1, base64String.toString().getBytes());
        ps.execute();
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT ENCODE_BINARY(data, 'BASE64') FROM " + testTable);
        assertTrue(rs.next());

        String expactedBase64Chunk = "SGVsbG9QaG9lbml4";
        StringBuilder expectedString = new StringBuilder();
        for (int i = 0; i < 20; i++) {
            expectedString.append(expactedBase64Chunk);
        }

        String actualString = rs.getString(1);
        assertEquals(expectedString.toString(), actualString);
        assertFalse(rs.next());
    }

    @Test
    public void testEncodeHBase() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String testTable = generateUniqueName();
        String ddl = "CREATE TABLE " + testTable + " (id INTEGER PRIMARY KEY, data VARBINARY)";

        conn.createStatement().execute(ddl);

        byte[] originalEncoded = "HelloPhoenix".getBytes();
        PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + testTable + " (id, data) VALUES (1, ?)");
        ps.setBytes(1, originalEncoded);
        ps.execute();
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT ENCODE_BINARY(data, 'HBASE') FROM " + testTable);
        assertTrue(rs.next());
        String actualString = rs.getString(1);
        assertEquals("HelloPhoenix", actualString);
        assertFalse(rs.next());
    }

    @Test
    public void testInvalidDecodingFormat() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String testTable = generateUniqueName();
        String ddl = "CREATE TABLE " + testTable + " (id INTEGER PRIMARY KEY, data VARBINARY)";

        conn.createStatement().execute(ddl);

        try {
            conn.createStatement().executeQuery("SELECT ENCODE_BINARY(data, 'INVALIDFORMAT') FROM " + testTable);
            fail("Expected an exception for invalid encoding format");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TYPE_MISMATCH.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testNullAndEmptyStringDecoding() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String testTable = generateUniqueName();
        String ddl = "CREATE TABLE " + testTable + " (id INTEGER PRIMARY KEY, data VARBINARY)";

        conn.createStatement().execute(ddl);

        ResultSet rs = conn.createStatement().executeQuery("SELECT ENCODE_BINARY(data, 'HEX') FROM " + testTable);
        assertFalse(rs.next());

        PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + testTable + " (id, data) VALUES (1, ?)");
        ps.setBytes(1, new byte[0]);
        ps.execute();
        conn.commit();

        ResultSet rs2 = conn.createStatement().executeQuery("SELECT ENCODE_BINARY(data, 'HEX') FROM " + testTable);
        assertTrue(rs2.next());
        String actualString = rs2.getString(1);
        assertEquals(null, actualString);
        assertFalse(rs2.next());
    }

    @Test
    public void testEncodeDecodeRoundHex() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String testTable = generateUniqueName();
        String ddl = "CREATE TABLE " + testTable + " (id INTEGER PRIMARY KEY, data VARBINARY)";
        conn.createStatement().execute(ddl);

        byte[] originalEncoded = "HelloPhoenix".getBytes();
        PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + testTable + " (id, data) VALUES (1, ?)");
        ps.setBytes(1, originalEncoded);
        ps.execute();
        conn.commit();

        ResultSet rs = conn.createStatement()
                .executeQuery("SELECT DECODE_BINARY(ENCODE_BINARY(data, 'HEX'), 'HEX') FROM " + testTable);
        assertTrue(rs.next());
        byte[] roundTripEncoded = rs.getBytes(1);
        assertTrue(Arrays.equals(originalEncoded, roundTripEncoded));
        assertFalse(rs.next());
    }

    @Test
    public void testEncodeDecodeRoundBase64() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String testTable = generateUniqueName();
        String ddl = "CREATE TABLE " + testTable + " (id INTEGER PRIMARY KEY, data VARBINARY)";
        conn.createStatement().execute(ddl);

        byte[] originalEncoded = "HelloPhoenix".getBytes();
        PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + testTable + " (id, data) VALUES (1, ?)");
        ps.setBytes(1, originalEncoded);
        ps.execute();
        conn.commit();

        ResultSet rs = conn.createStatement()
                .executeQuery("SELECT DECODE_BINARY(ENCODE_BINARY(data, 'BASE64'), 'BASE64') FROM " + testTable);
        assertTrue(rs.next());
        byte[] roundTripEncoded = rs.getBytes(1);
        assertTrue(Arrays.equals(originalEncoded, roundTripEncoded));
        assertFalse(rs.next());
    }

    @Test
    public void testEncodeDecodeRoundHbase() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String testTable = generateUniqueName();
        String ddl = "CREATE TABLE " + testTable + " (id INTEGER PRIMARY KEY, data VARBINARY)";
        conn.createStatement().execute(ddl);

        byte[] originalEncoded = "HelloPhoenix".getBytes();
        PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + testTable + " (id, data) VALUES (1, ?)");
        ps.setBytes(1, originalEncoded);
        ps.execute();
        conn.commit();

        ResultSet rs = conn.createStatement()
                .executeQuery("SELECT DECODE_BINARY(ENCODE_BINARY(data, 'HBASE'), 'HBASE') FROM " + testTable);
        assertTrue(rs.next());
        byte[] roundTripEncoded = rs.getBytes(1);
        assertTrue(Arrays.equals(originalEncoded, roundTripEncoded));
        assertFalse(rs.next());
    }
}