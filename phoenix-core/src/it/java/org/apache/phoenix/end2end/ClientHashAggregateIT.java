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

public class ClientHashAggregateIT extends ParallelStatsDisabledIT {
    
    @Test
    public void testSalted() throws Exception { 

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
   
        try {
            String table = createSalted(conn);
            verifyResults(conn, table);
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
            verifyResults(conn, table);
        } finally {
            conn.close();
        }
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
        String createString = "CREATE TABLE " + table + " ("
            + " keyA BIGINT NOT NULL,"
            + " keyB BIGINT NOT NULL,"
            + " val SMALLINT,"
            + " CONSTRAINT pk PRIMARY KEY (keyA, keyB)"
            + ")";

        conn.createStatement().execute(createString);
        return table;
    }

    private void verifyResults(Connection conn, String table) throws Exception {

        String upsert = "UPSERT INTO " + table + "(keyA, keyB, val) VALUES(?, ?, ?)";
        PreparedStatement upsertStmt = conn.prepareStatement(upsert);
        for (int i = 0; i < 13; i++) {
            upsertStmt.setInt(1, 10);
            upsertStmt.setInt(2, 100);
            upsertStmt.setInt(3, 1);
            upsertStmt.execute();

            upsertStmt.setInt(1, 20);
            upsertStmt.setInt(2, 100);
            upsertStmt.setInt(3, 2);
            upsertStmt.execute();
        }
        for (int i = 0; i < 17; i++) {
            upsertStmt.setInt(1, 10);
            upsertStmt.setInt(2, 100);
            upsertStmt.setInt(3, 2);
            upsertStmt.execute();

            upsertStmt.setInt(1, 20);
            upsertStmt.setInt(2, 100);
            upsertStmt.setInt(3, 1);
            upsertStmt.execute();
        }
        conn.commit();

        String hashQuery = "SELECT /*+ USE_SORT_MERGE_JOIN HASH_AGGREGATE */"
            + " t1.val v1, t2.val v2, COUNT(*) c"
            + " FROM " + table + " t1 JOIN " + table + " t2"
            + " ON (t1.keyB = t2.keyB)"
            + " WHERE t1.keyA = 10 AND t2.keyA = 20"
            + " GROUP BY t1.val, t2.val";

        String sortQuery = "SELECT /*+ USE_SORT_MERGE_JOIN */"
            + " t1.val v1, t2.val v2, COUNT(*) c"
            + " FROM " + table + " t1 JOIN " + table + " t2"
            + " ON (t1.keyB = t2.keyB)"
            + " WHERE t1.keyA = 10 AND t2.keyA = 20"
            + " GROUP BY t1.val, t2.val";

        Statement stmt = conn.createStatement();
        ResultSet hrs = stmt.executeQuery(hashQuery);
        ResultSet srs = stmt.executeQuery(sortQuery);
        assertTrue(hrs.next());
        assertTrue(srs.next());
        assertEquals(hrs.getInt("v1"), 1);
        assertEquals(hrs.getInt("v1"), srs.getInt("v1"));
        assertEquals(hrs.getInt("v2"), 2);
        assertEquals(hrs.getInt("v2"), srs.getInt("v2"));
        assertEquals(hrs.getInt("c"), 13);
        assertEquals(hrs.getInt("c"), srs.getInt("c"));
        assertTrue(hrs.next());
        assertTrue(srs.next());
        assertEquals(hrs.getInt("v1"), 2);
        assertEquals(hrs.getInt("v1"), srs.getInt("v1"));
        assertEquals(hrs.getInt("v2"), 1);
        assertEquals(hrs.getInt("v2"), srs.getInt("v2"));
        assertEquals(hrs.getInt("c"), 17);
        assertEquals(hrs.getInt("c"), srs.getInt("c"));
        assertFalse(hrs.next());
        assertFalse(srs.next());
        hrs.close();
        srs.close();
    }
}
