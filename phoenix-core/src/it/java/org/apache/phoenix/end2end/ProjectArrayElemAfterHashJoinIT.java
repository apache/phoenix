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
public class ProjectArrayElemAfterHashJoinIT extends ParallelStatsDisabledIT {

    @Test
    public void testSalted() throws Exception {

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);

        try {
            String table = createSalted(conn);
            testTable(conn, table);
        } finally {
            conn.close();
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

        verifyResults(conn, table, false, false);
        verifyResults(conn, table, false, true);
        verifyResults(conn, table, true, false);
        verifyResults(conn, table, true, true);
    }

    private String createSalted(Connection conn) throws Exception {

        String table = "SALTED_" + generateUniqueName();
        String create = "CREATE TABLE " + table + " ("
            + " id INTEGER NOT NULL,"
            + " vals TINYINT[],"
            + " CONSTRAINT pk PRIMARY KEY (id)"
            + ") SALT_BUCKETS = 4";

        conn.createStatement().execute(create);
        return table;
    }

    private String createUnsalted(Connection conn) throws Exception {

        String table = "UNSALTED_" + generateUniqueName();
        String create = "CREATE TABLE " + table + " ("
            + " id INTEGER NOT NULL,"
            + " vals TINYINT[],"
            + " CONSTRAINT pk PRIMARY KEY (id)"
            + ")";

        conn.createStatement().execute(create);
        return table;
    }

    private String getQuery(String table, boolean fullArray, boolean hashJoin) {

        String query = "SELECT id, vals[1] v1, vals[2] v2, vals[3] v3, vals[4] v4"
            + (fullArray ? ", vals" : "")
            + " FROM " + table
            + " WHERE id IN "
            + (hashJoin ? "(SELECT 1)" : "(1, 2, 3)")
            ;

        return query;
    }

    private void verifyExplain(Connection conn, String table, boolean fullArray, boolean hashJoin)
        throws Exception {

        String query = "EXPLAIN " + getQuery(table, fullArray, hashJoin);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);

        try {
            String plan = QueryUtil.getExplainPlan(rs);
            assertTrue(plan != null);
            assertTrue(fullArray || plan.contains("SERVER ARRAY ELEMENT PROJECTION"));
            assertTrue(hashJoin == plan.contains("JOIN"));
        } finally {
            rs.close();
        }
    }

    private void verifyResults(Connection conn, String table, boolean fullArray, boolean hashJoin)
        throws Exception {

        String upsert = "UPSERT INTO " + table + "(id, vals)"
            + " VALUES(1, ARRAY[10, 20, 30, 40, 50])";
        PreparedStatement upsertStmt = conn.prepareStatement(upsert);
        upsertStmt.execute();
        conn.commit();

        String query = getQuery(table, fullArray, hashJoin);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);

        try {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals(10, rs.getInt("v1"));
            assertEquals(20, rs.getInt("v2"));
            assertEquals(30, rs.getInt("v3"));
            assertEquals(40, rs.getInt("v4"));

            if (fullArray) {
                java.sql.Array array = rs.getArray("vals");
                assertTrue(array != null);
                Object obj = array.getArray();
                assertTrue(obj != null);
                assertTrue(obj.getClass().isArray());
                assertEquals(5, java.lang.reflect.Array.getLength(obj));
            }

            assertFalse(rs.next());
        } finally {
            rs.close();
        }
    }

    private void dropTable(Connection conn, String table) throws Exception {

        String drop = "DROP TABLE " + table;
        Statement stmt = conn.createStatement();
        stmt.execute(drop);
        stmt.close();
    }
}
