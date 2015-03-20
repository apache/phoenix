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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.junit.Test;


public class RegexpSplitFunctionIT extends BaseHBaseManagedTimeIT {

    private void initTable(Connection conn, String val) throws SQLException {
        initTable(conn, val, ",");
    }

    private void initTable(Connection conn, String val, String separator) throws SQLException {
        String ddl = "CREATE TABLE SPLIT_TEST (" +
                "ID INTEGER NOT NULL PRIMARY KEY," +
                "VAL VARCHAR," +
                "SEP VARCHAR," +
                "ARR VARCHAR ARRAY)";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO SPLIT_TEST (ID, SEP, VAL) VALUES (?, ?, ?)";
        PreparedStatement stmt = conn.prepareStatement(dml);
        stmt.setInt(1, 1);
        if (separator == null) {
            stmt.setNull(2, Types.VARCHAR);
        } else {
            stmt.setString(2, separator);
        }
        if (val == null) {
            stmt.setNull(3, Types.VARCHAR);
        } else {
            stmt.setString(3, val);
        }
        stmt.execute();
        conn.commit();
    }

    @Test
    public void testSplit_ArrayReference() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        initTable(conn, "ONE,TWO,THREE");

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT REGEXP_SPLIT(VAL, ',')[1] FROM SPLIT_TEST");
        assertTrue(rs.next());
        assertEquals("ONE", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testSplit_InFilter() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        initTable(conn, "ONE,TWO,THREE");

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT ID FROM SPLIT_TEST WHERE (REGEXP_SPLIT(VAL, ','))[1] = 'ONE'");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());
    }

    @Test
    public void testSplit_Upsert() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        initTable(conn, "ONE,TWO,THREE");

        conn.createStatement().executeUpdate("UPSERT INTO SPLIT_TEST (ID, ARR) SELECT ID, " +
                "REGEXP_SPLIT(VAL, ',') FROM SPLIT_TEST");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT ARR FROM SPLIT_TEST");
        assertTrue(rs.next());
        Array array = rs.getArray(1);
        String[] values = (String[]) array.getArray();
        assertArrayEquals(new String[]{ "ONE", "TWO", "THREE" }, values);
    }

    @Test
    public void testSplit_AlternateSeparator() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        initTable(conn, "ONE:TWO:THREE");

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT REGEXP_SPLIT(VAL, ':') FROM SPLIT_TEST");
        assertTrue(rs.next());
        Array array = rs.getArray(1);
        String[] values = (String[]) array.getArray();
        assertArrayEquals(new String[] { "ONE", "TWO", "THREE" }, values);
    }

    @Test
    public void testSplit_DynamicPattern() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        initTable(conn, "ONE,TWO,THREE");

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT REGEXP_SPLIT(VAL, SEP) FROM SPLIT_TEST");
        assertTrue(rs.next());
        Array array = rs.getArray(1);
        String[] values = (String[]) array.getArray();
        assertArrayEquals(new String[] { "ONE", "TWO", "THREE" }, values);
    }

    @Test
    public void testSplit_NoSplitString() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        initTable(conn, "CANNOT BE SPLIT");

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT REGEXP_SPLIT(VAL, ',') FROM SPLIT_TEST");
        assertTrue(rs.next());
        Array array = rs.getArray(1);
        String[] values = (String[]) array.getArray();
        assertArrayEquals(new String[] { "CANNOT BE SPLIT" }, values);
    }

    @Test
    public void testSplit_PatternBasedSplit() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        initTable(conn, "ONE!:TWO:::!THREE::!:FOUR");

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT REGEXP_SPLIT(VAL, '[:!]+') FROM SPLIT_TEST");
        assertTrue(rs.next());
        Array array = rs.getArray(1);
        String[] values = (String[]) array.getArray();
        assertArrayEquals(new String[] { "ONE", "TWO", "THREE", "FOUR" }, values);
    }

    @Test
    public void testSplit_PatternEscape() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        initTable(conn, "ONE|TWO|THREE");

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT REGEXP_SPLIT(VAL, '\\\\|') FROM SPLIT_TEST");
        assertTrue(rs.next());
        Array array = rs.getArray(1);
        String[] values = (String[]) array.getArray();
        assertArrayEquals(new String[] { "ONE", "TWO", "THREE" }, values);
    }

    @Test
    public void testSplit_NullString() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        initTable(conn, null);

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT REGEXP_SPLIT(VAL, ',') FROM SPLIT_TEST");
        assertTrue(rs.next());
        assertNull(rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testSplit_NullSeparator() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        initTable(conn, "ONE,TWO,THREE");

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT REGEXP_SPLIT(VAL, NULL) FROM SPLIT_TEST");
        assertTrue(rs.next());
        assertNull(rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testSplit_NullDynamicSeparator() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        initTable(conn, "ONE,TWO,THREE", null);

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT REGEXP_SPLIT(VAL, SEP) FROM SPLIT_TEST");
        assertTrue(rs.next());
        assertNull(rs.getString(1));
        assertFalse(rs.next());
    }

}
