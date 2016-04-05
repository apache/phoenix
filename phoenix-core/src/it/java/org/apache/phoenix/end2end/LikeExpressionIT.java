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
import org.junit.BeforeClass;
import org.junit.Test;

public class LikeExpressionIT extends BaseHBaseManagedTimeIT {

    private static final String TEST_TABLE = generateRandomString();

    @BeforeClass
    public static void doBeforeTestSetup() throws Exception {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DriverManager.getConnection(getUrl());
            String ddl;
            ddl = "CREATE TABLE " + TEST_TABLE + " (k VARCHAR NOT NULL PRIMARY KEY, i INTEGER)";
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

    private static void insertRow(Connection conn, String k, int i) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(
            "UPSERT INTO " + TEST_TABLE + " VALUES (?, ?)");
        stmt.setString(1, k);
        stmt.setInt(2, i);
        stmt.executeUpdate();
        conn.commit();
    }

    private void testLikeExpression(Connection conn, String likeStr, int numResult, int expectedSum)
            throws Exception {
        String cmd = "select k, i from " + TEST_TABLE + " where k like '" + likeStr + "'";
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
        String table = generateRandomString();
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
        
        conn.close();
    }
    
    @Test
    public void testLikeWithEscapenLParen() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE t (k VARCHAR, v VARCHAR, CONSTRAINT pk PRIMARY KEY (k))";
        conn.createStatement().execute(ddl);
        conn.createStatement().execute("UPSERT INTO t VALUES('aa','bb')");
        conn.createStatement().execute("UPSERT INTO t VALUES('a\\(d','xx')");
        conn.createStatement().execute("UPSERT INTO t VALUES('dd',null)");
        conn.commit();
        
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM t WHERE k not like '%\\(%'");
        assertTrue(rs.next());
        assertEquals("aa", rs.getString(1));
        assertEquals("bb", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("dd", rs.getString(1));
        assertEquals(null, rs.getString(2));
        assertFalse(rs.next());
    }
}
