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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class ClientHashAggregateIT extends ParallelStatsDisabledIT {
    
    @Test
    public void testSalted() throws Exception { 

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String table = createSalted(conn);
            testTable(conn, table);
        }
    }
    
    @Test
    public void testUnsalted() throws Exception { 

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
   
        try {
            String table = createUnsalted(conn);
            testTable(conn, table);
        } finally {
            conn.close();
        }
    }

    private void testTable(Connection conn, String table) throws Exception {
        verifyExplain(conn, table, false, false);
        verifyExplain(conn, table, false, true);
        verifyExplain(conn, table, true, false);
        verifyExplain(conn, table, true, true);

        verifyResults(conn, table, 13, 0, false, false);
        verifyResults(conn, table, 13, 0, false, true);
        verifyResults(conn, table, 13, 0, true, false);
        verifyResults(conn, table, 13, 0, true, true);

        verifyResults(conn, table, 13, 17, false, true);
        verifyResults(conn, table, 13, 17, true, true);

        dropTable(conn, table);
    }

    private String createSalted(Connection conn) throws Exception {
    
        String table = "SALTED_" + generateUniqueName();
        String create = "CREATE TABLE " + table + " ("
            + " keyA BIGINT NOT NULL,"
            + " keyB BIGINT NOT NULL,"
            + " val SMALLINT,"
            + " CONSTRAINT pk PRIMARY KEY (keyA, keyB)"
            + ") SALT_BUCKETS = 4";

        conn.createStatement().execute(create);
        return table;
    }

    private String createUnsalted(Connection conn) throws Exception {
    
        String table = "UNSALTED_" + generateUniqueName();
        String create = "CREATE TABLE " + table + " ("
            + " keyA BIGINT NOT NULL,"
            + " keyB BIGINT NOT NULL,"
            + " val SMALLINT,"
            + " CONSTRAINT pk PRIMARY KEY (keyA, keyB)"
            + ")";

        conn.createStatement().execute(create);
        return table;
    }

    private String getQuery(String table, boolean hash, boolean swap, boolean sort) {

        String query = "SELECT /*+ USE_SORT_MERGE_JOIN"
            + (hash ? " HASH_AGGREGATE" : "") + " */"
            + " t1.val v1, t2.val v2, COUNT(*) c"
            + " FROM " + table + " t1 JOIN " + table + " t2"
            + " ON (t1.keyB = t2.keyB)"
            + " WHERE t1.keyA = 10 AND t2.keyA = 20"
            + " GROUP BY "
            + (swap ? "t2.val, t1.val" : "t1.val, t2.val")
            + (sort ? " ORDER BY t1.val, t2.val" : "")
            ;

        return query;
    }

    private void verifyExplain(Connection conn, String table, boolean swap, boolean sort) throws Exception {

        String query = "EXPLAIN " + getQuery(table, true, swap, sort);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        String plan = QueryUtil.getExplainPlan(rs);
        rs.close();
        assertTrue(plan != null && plan.contains("CLIENT HASH AGGREGATE"));
        assertTrue(plan != null && (sort == plan.contains("CLIENT SORTED BY")));
    }

    private void verifyResults(Connection conn, String table, int c1, int c2, boolean swap, boolean sort) throws Exception {

        String upsert = "UPSERT INTO " + table + "(keyA, keyB, val) VALUES(?, ?, ?)";
        PreparedStatement upsertStmt = conn.prepareStatement(upsert);
        for (int i = 0; i < c1; i++) {
            upsertStmt.setInt(1, 10);
            upsertStmt.setInt(2, 100+i);
            upsertStmt.setInt(3, 1);
            upsertStmt.execute();

            upsertStmt.setInt(1, 20);
            upsertStmt.setInt(2, 100+i);
            upsertStmt.setInt(3, 2);
            upsertStmt.execute();
        }
        for (int i = 0; i < c2; i++) {
            upsertStmt.setInt(1, 10);
            upsertStmt.setInt(2, 200+i);
            upsertStmt.setInt(3, 2);
            upsertStmt.execute();

            upsertStmt.setInt(1, 20);
            upsertStmt.setInt(2, 200+i);
            upsertStmt.setInt(3, 1);
            upsertStmt.execute();
        }
        conn.commit();

        String hashQuery = getQuery(table, true, swap, sort);
        String sortQuery = getQuery(table, false, swap, sort);
        Statement stmt = conn.createStatement();

        try (ResultSet hrs = stmt.executeQuery(hashQuery);) {
            if (c1 > 0) {
                assertTrue(hrs.next());
                assertEquals(hrs.getInt("v1"), 1);
                assertEquals(hrs.getInt("v2"), 2);
                assertEquals(hrs.getInt("c"), c1);
            }
            if (c2 > 0) {
                assertTrue(hrs.next());
                assertEquals(hrs.getInt("v1"), 2);
                assertEquals(hrs.getInt("v2"), 1);
                assertEquals(hrs.getInt("c"), c2);
            }
            assertFalse(hrs.next());
        }

        try (ResultSet srs = stmt.executeQuery(sortQuery)) {
            if (c1 > 0) {
                assertTrue(srs.next());
                assertEquals(srs.getInt("v1"), 1);
                assertEquals(srs.getInt("v2"), 2);
                assertEquals(srs.getInt("c"), c1);
            }
            if (c2 > 0) {
                assertTrue(srs.next());
                assertEquals(srs.getInt("v1"), 2);
                assertEquals(srs.getInt("v2"), 1);
                assertEquals(srs.getInt("c"), c2);
            }
            assertFalse(srs.next());
        }

    }

    private void dropTable(Connection conn, String table) throws Exception {

        String drop = "DROP TABLE " + table;
        Statement stmt = conn.createStatement();
        stmt.execute(drop);
        stmt.close();
    }
}
