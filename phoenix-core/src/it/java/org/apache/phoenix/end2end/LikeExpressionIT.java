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

import static org.apache.phoenix.util.TestUtil.closeStmtAndConn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Before;
import org.junit.Test;

public class LikeExpressionIT extends ParallelStatsDisabledIT {

    private String tableName;

    @Before
    public void initTable() throws Exception {
        tableName = generateUniqueName();
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DriverManager.getConnection(getUrl());
            String ddl;
            ddl = "CREATE TABLE " + tableName + " (k VARCHAR NOT NULL PRIMARY KEY, i INTEGER)";
            conn.createStatement().execute(ddl);
            conn.commit();
        } finally {
            closeStmtAndConn(stmt, conn);
        }
        insertRow(conn, "123n7-app-2-", 1);
        insertRow(conn, "132n7-App-2-", 2);
        insertRow(conn, "213n7-app-2-", 4);
        insertRow(conn, "231n7-App-2-", 8);
        insertRow(conn, "312n7-app-2-", 16);
        insertRow(conn, "321n7-App-2-", 32);
    }

    private void insertRow(Connection conn, String k, int i) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(
            "UPSERT INTO " + tableName + " VALUES (?, ?)");
        stmt.setString(1, k);
        stmt.setInt(2, i);
        stmt.executeUpdate();
        conn.commit();
    }

    private void testLikeExpression(Connection conn, String likeStr, int numResult, int expectedSum)
            throws Exception {
        String cmd = "select k, i from " + tableName + " where k like '" + likeStr + "'";
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(cmd);
        int sum = 0;
        for (int i = 0; i < numResult; ++i) {
            assertTrue(rs.next());
            sum += rs.getInt("i");
        }
        assertFalse(rs.next());
        assertEquals(sum, expectedSum);
    }

    @Test
    public void testLikeExpression() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        // wildcard
        testLikeExpression(conn, "%1%3%7%2%", 3, 7);
        // CaseSensitive
        testLikeExpression(conn, "%A%", 3, 42);
        conn.close();
    }

    @Test
    public void testLikeEverythingExpression() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String table = generateUniqueName();
        String ddl = "CREATE TABLE " + table
                + " (k1 VARCHAR, k2 VARCHAR, CONSTRAINT pk PRIMARY KEY (k1,k2))";
        conn.createStatement().execute(ddl);
        conn.createStatement().execute("UPSERT INTO " + table + " VALUES('aa','bb')");
        conn.createStatement().execute("UPSERT INTO " + table + " VALUES('ab','bc')");
        conn.createStatement().execute("UPSERT INTO " + table + " VALUES(null,'cc')");
        conn.createStatement().execute("UPSERT INTO " + table + " VALUES('dd',null)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT * FROM " + table + " WHERE k1 LIKE '%'");
        assertTrue(rs.next());
        assertEquals("aa", rs.getString(1));
        assertEquals("bb", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("ab", rs.getString(1));
        assertEquals("bc", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("dd", rs.getString(1));
        assertEquals(null, rs.getString(2));
        assertFalse(rs.next());

        rs = conn.createStatement().executeQuery("SELECT * FROM " + table + " WHERE k2 LIKE '%'");
        assertTrue(rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals("cc", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("aa", rs.getString(1));
        assertEquals("bb", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("ab", rs.getString(1));
        assertEquals("bc", rs.getString(2));
        assertFalse(rs.next());

        rs = conn.createStatement().executeQuery("SELECT * FROM " + table + " WHERE k2 LIKE '%%'");
        assertTrue(rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals("cc", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("aa", rs.getString(1));
        assertEquals("bb", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("ab", rs.getString(1));
        assertEquals("bc", rs.getString(2));
        assertFalse(rs.next());

        rs = conn.createStatement().executeQuery("SELECT * FROM " + table + " WHERE k2 NOT LIKE '%'");
        assertFalse(rs.next());

        rs = conn.createStatement().executeQuery("SELECT * FROM " + table + " WHERE k2 NOT LIKE '%%'");
        assertFalse(rs.next());

        conn.close();
    }
    
    @Test
    public void testLikeWithEscapenLParen() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String t = generateUniqueName();
        String ddl = "CREATE TABLE " + t + " (k VARCHAR, v VARCHAR, CONSTRAINT pk PRIMARY KEY (k))";
        conn.createStatement().execute(ddl);
        conn.createStatement().execute("UPSERT INTO " + t + " VALUES('aa','bb')");
        conn.createStatement().execute("UPSERT INTO " + t + " VALUES('a\\(d','xx')");
        conn.createStatement().execute("UPSERT INTO " + t + " VALUES('dd',null)");
        conn.commit();
        
        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT * FROM " + t + " WHERE k not like '%\\(%'");
        assertTrue(rs.next());
        assertEquals("aa", rs.getString(1));
        assertEquals("bb", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("dd", rs.getString(1));
        assertEquals(null, rs.getString(2));
        assertFalse(rs.next());
    }

    @Test
    public void testNewLine() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String t = generateUniqueName();
        String ddl = "CREATE TABLE " + t + " (k VARCHAR NOT NULL PRIMARY KEY)";
        conn.createStatement().execute(ddl);
        conn.createStatement().execute("UPSERT INTO " + t + " VALUES('AA\nA')");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT * FROM " + t + " WHERE k like 'AA%'");
        assertTrue(rs.next());
        assertEquals("AA\nA", rs.getString(1));

        rs = conn.createStatement().executeQuery(
                "SELECT * FROM " + t + " WHERE k like 'AA_A'");
        assertTrue(rs.next());
        assertEquals("AA\nA", rs.getString(1));

        rs = conn.createStatement().executeQuery(
                "SELECT * FROM " + t + " WHERE k like 'AA%A'");
        assertTrue(rs.next());
        assertEquals("AA\nA", rs.getString(1));

        rs = conn.createStatement().executeQuery(
                "SELECT * FROM " + t + " WHERE k like 'AA_'");
        assertFalse(rs.next());
    }

    @Test
    public void testOneChar() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String t = generateUniqueName();
        String ddl = "CREATE TABLE " + t + " (k VARCHAR NOT NULL PRIMARY KEY)";
        conn.createStatement().execute(ddl);
        conn.createStatement().execute("UPSERT INTO " + t + " VALUES('A')");
        conn.createStatement().execute("UPSERT INTO " + t + " VALUES('AA')");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT * FROM " + t + " WHERE k like '_'");
        assertTrue(rs.next());
        assertEquals("A", rs.getString(1));
        assertFalse(rs.next());

        rs = conn.createStatement().executeQuery(
                "SELECT * FROM " + t + " WHERE k like '_A'");
        assertTrue(rs.next());
        assertEquals("AA", rs.getString(1));
        assertFalse(rs.next());
    }

    @Test
    public void testNull() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String table = generateUniqueName();
        String ddl = "CREATE TABLE " + table
                + " (pk INTEGER PRIMARY KEY, str VARCHAR)";
        conn.createStatement().execute(ddl);
        conn.createStatement().execute("UPSERT INTO " + table + " VALUES(0,'aa')");
        conn.createStatement().execute("UPSERT INTO " + table + " VALUES(1, null)");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT str LIKE '%' FROM " + table);
        assertTrue(rs.next());
        assertEquals(true, rs.getBoolean(1));
        assertFalse(rs.wasNull());
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.wasNull());
        assertFalse(rs.next());

        rs = conn.createStatement().executeQuery(
                "SELECT str LIKE '%%' FROM " + table);
        assertTrue(rs.next());
        assertEquals(true, rs.getBoolean(1));
        assertFalse(rs.wasNull());
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.wasNull());
        assertFalse(rs.next());

        rs = conn.createStatement().executeQuery(
                "SELECT str NOT LIKE '%' FROM " + table);
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertFalse(rs.wasNull());
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.wasNull());
        assertFalse(rs.next());

        rs = conn.createStatement().executeQuery(
                "SELECT str NOT LIKE '%%' FROM " + table);
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertFalse(rs.wasNull());
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.wasNull());
        assertFalse(rs.next());

        rs = conn.createStatement().executeQuery(
                "SELECT NOT (str LIKE '%') FROM " + table);
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertFalse(rs.wasNull());
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.wasNull());
        assertFalse(rs.next());

        rs = conn.createStatement().executeQuery(
                "SELECT NOT(str LIKE '%%') FROM " + table);
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertFalse(rs.wasNull());
        assertTrue(rs.next());
        assertEquals(false, rs.getBoolean(1));
        assertTrue(rs.wasNull());
        assertFalse(rs.next());
    }

    @Test
    public void testMultiCFLikeExpressionWithLimitOffset() throws Exception {
        String tableName = generateUniqueName();
        String ddl =
                "create table " + tableName
                        + " (id integer not null primary key, cf.col1 varchar, cf.col2 varchar, cf2.col3 varchar, cf2.col4 varchar)";
        String upsert = "UPSERT INTO " + tableName + " VALUES (?, ?, ?, ?, ?)";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            PreparedStatement stmt = conn.prepareStatement(upsert);
            for (int i = 1; i <= 10; i++) {
                stmt.setInt(1, i);
                stmt.setString(2, i + "col1");
                stmt.setString(3, i + "col2");
                stmt.setString(4, i + "col3");
                stmt.setString(5, i + "col4");
                stmt.executeUpdate();
            }
            conn.commit();

            String query =
                    "select cf.* from " + tableName
                            + " where cf.col1 like '%col1%' limit 10 offset 2";
            ResultSet rs = conn.createStatement().executeQuery(query);
            int expectedCount = 8;
            int i = 0;
            while (rs.next()) {
                i++;
                assertTrue(rs.getString("COL1").contains("col1"));
                assertTrue(rs.getString("COL2").contains("col2"));
            }
            assertEquals(expectedCount, i);

            query =
                    "select cf.*, cf2.* from " + tableName
                            + " where cf.col1 like '%col1%' limit 10 offset 2";
            rs = conn.createStatement().executeQuery(query);
            i = 0;
            while (rs.next()) {
                i++;
                assertTrue(rs.getString("COL1").contains("col1"));
                assertTrue(rs.getString("COL2").contains("col2"));
                assertTrue(rs.getString("COL3").contains("col3"));
                assertTrue(rs.getString("COL4").contains("col4"));
            }
            assertEquals(expectedCount, i);
            query = "select * from " + tableName + " where cf.col1 like '%col1%' limit 10 offset 2";
            rs = conn.createStatement().executeQuery(query);
            i = 0;
            while (rs.next()) {
                i++;
                assertTrue(rs.getString("COL1").contains("col1"));
                assertTrue(rs.getString("COL2").contains("col2"));
                assertTrue(rs.getString("COL3").contains("col3"));
                assertTrue(rs.getString("COL4").contains("col4"));
            }
            assertEquals(expectedCount, i);
        }
    }

    @Test
    public void testSingleCFLikeExpressionWithLimitOffset() throws Exception {
        String tableName = generateUniqueName();
        String ddl =
                "create table " + tableName
                        + " (id integer not null primary key, cf.col1 varchar, cf.col2 varchar, cf.col3 varchar, cf.col4 varchar)";
        String upsert = "UPSERT INTO " + tableName + " VALUES (?, ?, ?, ?, ?)";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            PreparedStatement stmt = conn.prepareStatement(upsert);
            for (int i = 1; i <= 10; i++) {
                stmt.setInt(1, i);
                stmt.setString(2, i + "col1");
                stmt.setString(3, i + "col2");
                stmt.setString(4, i + "col3");
                stmt.setString(5, i + "col4");
                stmt.executeUpdate();
            }
            conn.commit();

            String query =
                    "select cf.* from " + tableName
                            + " where cf.col1 like '%col1%' limit 10 offset 2";
            ResultSet rs = conn.createStatement().executeQuery(query);
            int expectedCount = 8;
            int i = 0;
            while (rs.next()) {
                i++;
                assertTrue(rs.getString("COL1").contains("col1"));
                assertTrue(rs.getString("COL2").contains("col2"));
            }
            assertEquals(expectedCount, i);

            query =
                    "select cf.* from " + tableName
                            + " where cf.col1 like '%col1%' limit 10 offset 2";
            rs = conn.createStatement().executeQuery(query);
            i = 0;
            while (rs.next()) {
                i++;
                assertTrue(rs.getString("COL1").contains("col1"));
                assertTrue(rs.getString("COL2").contains("col2"));
                assertTrue(rs.getString("COL3").contains("col3"));
                assertTrue(rs.getString("COL4").contains("col4"));
            }
            assertEquals(expectedCount, i);
            query = "select cf.* from " + tableName + " where cf.col1 like '%col1%' limit 10 offset 2";
            rs = conn.createStatement().executeQuery(query);
            i = 0;
            while (rs.next()) {
                i++;
                assertTrue(rs.getString("COL1").contains("col1"));
                assertTrue(rs.getString("COL2").contains("col2"));
                assertTrue(rs.getString("COL3").contains("col3"));
                assertTrue(rs.getString("COL4").contains("col4"));
            }
            assertEquals(expectedCount, i);
        }
    }
}
