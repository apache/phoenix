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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
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
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class EncodeBinaryFunctionIT extends ParallelStatsDisabledIT {

    private static String testTable;
    private static final byte[] originalBytes = "HelloPhoenix".getBytes();
    private static final String encoded48String = "48656c6c6f50686f656e6978";
    private static final String helloPhoenixString = "HelloPhoenix";
    private static final String expectedBase64Chunk = "SGVsbG9QaG9lbml4";

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));
        testTable = generateUniqueName();
        String ddl = "CREATE TABLE " + testTable + " (id INTEGER PRIMARY KEY, data VARBINARY)";
        conn.createStatement().execute(ddl);

        PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + testTable + " (id, data) VALUES (1, ?)");
        ps.setBytes(1, originalBytes);
        ps.execute();

        PreparedStatement ps2 = conn.prepareStatement(
                "UPSERT INTO " + testTable + " (id, data) VALUES (2, X'48656c6c6f50686f656e6978')");
        ps2.execute();
        conn.commit();
        conn.close();
    }

    @Test
    public void testEncodeHex() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));

        ResultSet rs = conn.createStatement()
                .executeQuery("SELECT ENCODE_BINARY(data, 'HEX') FROM " + testTable + " WHERE ID=2");
        assertTrue(rs.next());
        String actualString = rs.getString(1);
        assertEquals(encoded48String, actualString);
        assertFalse(rs.next());
    }

    @Test
    public void testMixedCaseHexDecoding() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));

        ResultSet rs = conn.createStatement()
                .executeQuery("SELECT ENCODE_BINARY(data, 'HEX') FROM " + testTable + " WHERE ID=2");
        assertTrue(rs.next());
        String actualString = rs.getString(1);
        assertEquals(encoded48String, actualString);
        assertFalse(rs.next());
    }

    @Test
    public void testEncodeBase64() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));

        ResultSet rs = conn.createStatement()
                .executeQuery("SELECT ENCODE_BINARY(data, 'BASE64') FROM " + testTable + " WHERE ID=1");
        assertTrue(rs.next());
        String actualString = rs.getString(1);
        assertEquals(expectedBase64Chunk, actualString);
        assertFalse(rs.next());
    }

    @Test
    public void testLongBase64Decoding() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));

        StringBuilder base64String = new StringBuilder();
        for (int i = 0; i < 20; i++) {
            base64String.append(helloPhoenixString);
        }

        PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + testTable + " (id, data) VALUES (3, ?)");
        ps.setBytes(1, base64String.toString().getBytes());
        ps.execute();
        conn.commit();

        ResultSet rs = conn.createStatement()
                .executeQuery("SELECT ENCODE_BINARY(data, 'BASE64') FROM " + testTable + " WHERE ID=3");
        assertTrue(rs.next());

        StringBuilder expectedString = new StringBuilder();
        for (int i = 0; i < 20; i++) {
            expectedString.append(expectedBase64Chunk);
        }

        String actualString = rs.getString(1);
        assertEquals(expectedString.toString(), actualString);
        assertFalse(rs.next());
    }

    @Test
    public void testEncodeHBase() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));

        ResultSet rs = conn.createStatement()
                .executeQuery("SELECT ENCODE_BINARY(data, 'HBASE') FROM " + testTable + " WHERE ID=1");
        assertTrue(rs.next());
        String actualString = rs.getString(1);
        assertEquals(helloPhoenixString, actualString);
        assertFalse(rs.next());
    }

    @Test
    public void testInvalidDecodingFormat() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));

        try {
            conn.createStatement().executeQuery("SELECT ENCODE_BINARY(data, 'INVALIDFORMAT') FROM " + testTable);
            fail("Expected an exception for invalid encoding format");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TYPE_MISMATCH.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testNullAndEmptyStringDecoding() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));

        ResultSet rs = conn.createStatement()
                .executeQuery("SELECT ENCODE_BINARY(data, 'HEX') FROM " + testTable + " WHERE ID=-10");
        assertFalse(rs.next());

        PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + testTable + " (id, data) VALUES (4, ?)");
        ps.setBytes(1, new byte[0]);
        ps.execute();
        conn.commit();

        ResultSet rs2 = conn.createStatement()
                .executeQuery("SELECT ENCODE_BINARY(data, 'HEX') FROM " + testTable + " WHERE ID=4");
        assertTrue(rs2.next());
        String actualString = rs2.getString(1);
        assertEquals(null, actualString);
        assertFalse(rs2.next());
    }

    @Test
    public void testEncodeDecodeRoundHex() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT DECODE_BINARY(ENCODE_BINARY(data, 'HEX'), 'HEX') FROM " + testTable + " WHERE ID=1");
        assertTrue(rs.next());
        byte[] roundTripEncoded = rs.getBytes(1);
        assertTrue(Arrays.equals(originalBytes, roundTripEncoded));
        assertFalse(rs.next());
    }

    @Test
    public void testEncodeDecodeRoundBase64() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl(),  PropertiesUtil.deepCopy(TEST_PROPERTIES));

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT DECODE_BINARY(ENCODE_BINARY(data, 'BASE64'), 'BASE64') FROM " + testTable + " WHERE ID=1");
        assertTrue(rs.next());
        byte[] roundTripEncoded = rs.getBytes(1);
        assertTrue(Arrays.equals(originalBytes, roundTripEncoded));
    }

    @Test
    public void testEncodeDecodeRoundHbase() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT DECODE_BINARY(ENCODE_BINARY(data, 'HBASE'), 'HBASE') FROM " + testTable + " WHERE ID=1");
        assertTrue(rs.next());
        byte[] roundTripEncoded = rs.getBytes(1);
        assertTrue(Arrays.equals(originalBytes, roundTripEncoded));
        assertFalse(rs.next());
    }
}