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

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.Assert.*;

public class TrimbFunctionIT extends ParallelStatsDisabledIT {

    private static final String KEY = "key";

    private String initTable(Connection conn, byte[] b, int binaryLen, byte[] vb) throws Exception {
        String trimbTestTableName = generateUniqueName();
        String
                ddl =
                "CREATE TABLE " + trimbTestTableName
                        + " (pk VARCHAR NOT NULL PRIMARY KEY , b BINARY(" + binaryLen
                        + "), vb VARBINARY)";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO " + trimbTestTableName + " VALUES(?,?,?)";
        PreparedStatement stmt = conn.prepareStatement(dml);
        stmt.setString(1, KEY);
        stmt.setBytes(2, b);
        stmt.setBytes(3, vb);
        stmt.execute();
        conn.commit();
        return trimbTestTableName;
    }

    @Test public void testTrimLeft() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String val = "ABCD";
        byte[] b = val.getBytes();
        byte[] vb = val.getBytes();
        String tableName = initTable(conn, b, 4, vb);
        ResultSet
                rs =
                conn.createStatement().executeQuery(
                        "SELECT TRIMB(b,cast('AB' as VARBINARY)),TRIMB(vb,cast('A' as VARBINARY))"
                                + " FROM "
                                + tableName
                                + " WHERE TRIMB(b,cast('AB' as VARBINARY))='CD' and TRIMB(vb,cast"
                                + "('A' as VARBINARY))='BCD'");
        assertTrue(rs.next());
        assertEquals("CD", new String(rs.getBytes(1)));
        assertEquals("BCD", new String(rs.getBytes(2)));
        assertFalse(rs.next());
    }

    @Test public void testTrimRight() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String val = "ABCD";
        byte[] b = val.getBytes();
        String tableName = initTable(conn, b, 4, b);
        ResultSet
                rs =
                conn.createStatement().executeQuery(
                        "SELECT TRIMB(b,cast('D' as VARBINARY)),TRIMB(vb,cast('BCD' as VARBINARY)"
                                + ") FROM "
                                + tableName
                                + " WHERE TRIMB(b,cast('D' as VARBINARY))='ABC' and TRIMB(vb,cast"
                                + "('BCD' as VARBINARY))='A'");
        assertTrue(rs.next());
        assertEquals("ABC", new String(rs.getBytes(1)));
        assertEquals("A", new String(rs.getBytes(2)));
        assertFalse(rs.next());
    }

    @Test public void testTrimBoth() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String bVal = "BADB";
        String vbVal = "ABBDCDEFBCEFCABAD";
        byte[] b = bVal.getBytes();
        byte[] vb = vbVal.getBytes();
        String tableName = initTable(conn, b, 4, vb);
        ResultSet
                rs =
                conn.createStatement().executeQuery(
                        "SELECT TRIMB(b,cast('AB' as VARBINARY)),TRIMB(vb,cast('ABD' as "
                                + "VARBINARY)) FROM "
                                + tableName
                                + " WHERE TRIMB(b,cast('AB' as VARBINARY))='D' and TRIMB(vb,cast"
                                + "('ABD' as VARBINARY))='CDEFBCEFC'");
        assertTrue(rs.next());
        assertEquals("D", new String(rs.getBytes(1)));
        assertEquals("CDEFBCEFC", new String(rs.getBytes(2)));
        assertFalse(rs.next());
    }

    @Test public void testTrimBothRepetitive() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String bVal = "ACCA";
        String vbVal = "ABBCDEFBCEFCABA";
        byte[] b = bVal.getBytes();
        byte[] vb = vbVal.getBytes();
        String tableName = initTable(conn, b, 4, vb);
        ResultSet
                rs =
                conn.createStatement().executeQuery(
                        "SELECT TRIMB(b,cast('AAA' as VARBINARY)),TRIMB(vb,cast('AABB' as "
                                + "VARBINARY)) FROM "
                                + tableName
                                + " WHERE TRIMB(b,cast('AAA' as VARBINARY))='CC' and TRIMB(vb,"
                                + "cast('AABB' as VARBINARY))='CDEFBCEFC'");
        assertTrue(rs.next());
        assertEquals("CC", new String(rs.getBytes(1)));
        assertEquals("CDEFBCEFC", new String(rs.getBytes(2)));
        assertFalse(rs.next());
    }

    @Test public void testTrimBinarySpace() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String bVal = " BC ";
        byte[] b = bVal.getBytes();
        String tableName = initTable(conn, b, 4, b);
        ResultSet
                rs =
                conn.createStatement().executeQuery(
                        "SELECT TRIMB(b,cast(' C' as VARBINARY)),TRIMB(b,cast('C' as VARBINARY)),"
                                + "TRIMB(vb,cast(' C' as VARBINARY)) FROM "
                                + tableName
                                + " WHERE TRIMB(b,cast(' C' as VARBINARY))='B' and TRIMB(b,cast"
                                + "('C' as VARBINARY))=' BC '  and TRIMB(vb,cast(' C' as "
                                + "VARBINARY))='B'");
        assertTrue(rs.next());
        assertEquals("B", new String(rs.getBytes(1)));
        assertEquals(" BC ", new String(rs.getBytes(2)));
        assertEquals("B", new String(rs.getBytes(3)));
        assertFalse(rs.next());
    }

    @Test public void testTrimNull() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String bVal = "ABC";
        byte[] b = bVal.getBytes();
        String tableName = initTable(conn, b, 4, b);
        ResultSet
                rs =
                conn.createStatement().executeQuery(
                        "SELECT TRIMB(b,cast('AC\0' as VARBINARY)),TRIMB(b,cast('AC' as "
                                + "VARBINARY)),TRIMB(vb,cast('AC' as VARBINARY)) FROM "
                                + tableName
                                + " WHERE TRIMB(b,cast('AC\0' as VARBINARY))='B' and TRIMB(b,cast"
                                + "('AC' as VARBINARY))='BC\0' and TRIMB(vb,cast('AC' as "
                                + "VARBINARY))='B'");
        assertTrue(rs.next());
        assertEquals("B", new String(rs.getBytes(1)));
        assertEquals("BC\0", new String(rs.getBytes(2)));
        assertEquals("B", new String(rs.getBytes(3)));
        assertFalse(rs.next());
    }

    @Test public void testTrimStringWithDoubleByteChar() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String bVal = "ACɸ";
        String vbVal = "AɚCɸ";
        byte[] b = bVal.getBytes();
        byte[] vb = vbVal.getBytes();
        String tableName = initTable(conn, b, 4, vb);
        ResultSet
                rs =
                conn.createStatement().executeQuery(
                        "SELECT TRIMB(b,cast('Aɸ' as VARBINARY)),TRIMB(vb,cast('Aɸ' as VARBINARY)"
                                + ") FROM "
                                + tableName
                                + " WHERE TRIMB(b,cast('Aɸ' as VARBINARY))='C' and TRIMB(vb,cast"
                                + "('Aɸ' as VARBINARY))='ɚC'");
        assertTrue(rs.next());
        assertEquals("C", new String(rs.getBytes(1)));
        assertEquals("ɚC", new String(rs.getBytes(2)));
        assertFalse(rs.next());
    }

    @Test public void testTrimStringWithTrippleByteChar() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String bVal = "AɸBFCहC";
        String vbVal = "ɚABECపFCBఆ";
        byte[] b = bVal.getBytes();
        byte[] vb = vbVal.getBytes();
        String tableName = initTable(conn, b, 10, vb);
        ResultSet
                rs =
                conn.createStatement().executeQuery(
                        "SELECT TRIMB(b,cast('हACɸ' as VARBINARY)),TRIMB(vb,cast('ɚఆAB' as "
                                + "VARBINARY)) FROM "
                                + tableName
                                + " WHERE TRIMB(b,cast('हACɸ' as VARBINARY))='BF' and TRIMB(vb,"
                                + "cast('ɚఆAB' as VARBINARY))='ECపFC'");
        assertTrue(rs.next());
        assertEquals("BF", new String(rs.getBytes(1)));
        assertEquals("ECపFC", new String(rs.getBytes(2)));
        assertFalse(rs.next());
    }

    @Test public void testNullTrim() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String bVal = "ABC";
        byte[] b = bVal.getBytes();
        String tableName = initTable(conn, b, 3, b);
        ResultSet
                rs =
                conn.createStatement()
                        .executeQuery("SELECT TRIMB(b,cast('' as VARBINARY)) FROM " + tableName);
        assertTrue(rs.next());
        assertNull(rs.getString(1));
        assertFalse(rs.next());
    }

    @Test public void testEmptyByteTrim() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String bVal = "";
        byte[] b = bVal.getBytes();
        String tableName = initTable(conn, b, 3, b);
        ResultSet
                rs =
                conn.createStatement().executeQuery(
                        "SELECT TRIMB(b,cast('ABC' as VARBINARY)),TRIMB(vb,cast('ABC' as "
                                + "VARBINARY)) FROM "
                                + tableName + " WHERE TRIMB(b,cast('ABC' as VARBINARY))='\0\0\0'");
        assertTrue(rs.next());
        assertEquals("\0\0\0", new String(rs.getBytes(1)));
        assertNull(rs.getBytes(2));
        assertFalse(rs.next());
    }

    @Test public void testNoMatchTrim() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String bVal = "ɚABC";
        byte[] b = bVal.getBytes();
        String tableName = initTable(conn, b, 5, b);
        ResultSet
                rs =
                conn.createStatement().executeQuery(
                        "SELECT TRIMB(vb,cast('DEF' as VARBINARY)) FROM " + tableName
                                + " WHERE TRIMB(b,cast('DEF' as VARBINARY))='ɚABC'");
        assertTrue(rs.next());
        assertEquals("ɚABC", new String(rs.getBytes(1)));
        assertFalse(rs.next());
    }

}
