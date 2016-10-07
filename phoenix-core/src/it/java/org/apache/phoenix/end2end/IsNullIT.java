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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.junit.Test;


public class IsNullIT extends ParallelStatsDisabledIT {
    @Test
    public void testIsNullInPk() throws Exception {
        String tableName = generateUniqueName();
        ensureTableCreated(getUrl(), tableName, "IntIntKeyTest");
        Connection conn = DriverManager.getConnection(getUrl());
        String upsert = "UPSERT INTO " + tableName + " VALUES(4,2)";
        PreparedStatement upsertStmt = conn.prepareStatement(upsert);
        int rowsInserted = upsertStmt.executeUpdate();
        assertEquals(1, rowsInserted);
        upsert = "UPSERT INTO " + tableName + " VALUES(6)";
        upsertStmt = conn.prepareStatement(upsert);
        rowsInserted = upsertStmt.executeUpdate();
        assertEquals(1, rowsInserted);
        conn.commit();
        
        String select = "SELECT i/j FROM " + tableName + " WHERE j IS NULL";
        ResultSet rs;
        rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(0,rs.getInt(1));
        assertTrue(rs.wasNull());
        assertFalse(rs.next());
        select = "SELECT i/j FROM " + tableName + " WHERE j IS NOT NULL";
        rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(2,rs.getInt(1));
        assertFalse(rs.next());
    }
    
    @Test
    public void testIsNullWithLastPKColDesc() throws Exception {
        String tableName = generateUniqueName();
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE " + tableName + "(k1 VARCHAR NOT NULL, k2 VARCHAR, k3 VARCHAR, CONSTRAINT pk PRIMARY KEY (k1, k2, k3 DESC))");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('a')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('b')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('b',null,'c')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('ba', null, 'd')");
        conn.commit();
        
        ResultSet rs = conn.createStatement().executeQuery("SELECT k1,k2,k3 FROM " + tableName + " WHERE k1='b' AND k2 IS NULL");
        assertTrue(rs.next());
        assertEquals("b",rs.getString(1));
        assertNull(rs.getString(2));
        assertNull(rs.getString(3));

        assertTrue(rs.next());
        assertEquals("b",rs.getString(1));
        assertNull(rs.getString(2));
        assertEquals("c",rs.getString(3));
        
        assertFalse(rs.next());
        conn.close();
    }
    
    @Test
    public void testIsNullInCompositeKey() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();
        conn.createStatement().execute("CREATE TABLE " + tableName + "(k1 VARCHAR, k2 VARCHAR, CONSTRAINT pk PRIMARY KEY (k1,k2))");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (null,'a')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('a','a')");
        conn.commit();
        
        ResultSet rs = conn.createStatement().executeQuery("SELECT count(*) FROM " + tableName);
        assertTrue(rs.next());
        assertEquals(2,rs.getInt(1));
        rs = conn.createStatement().executeQuery("SELECT count(*) FROM " + tableName + " WHERE k1 = 'a' or k1 is null");
        assertTrue(rs.next());
        assertEquals(2,rs.getInt(1));
        conn.close();
    }
    
}
