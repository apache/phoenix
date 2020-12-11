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


public class ReverseFunctionIT extends ParallelStatsDisabledIT {

    private String initTable(Connection conn, String sortOrder, String s) throws Exception {
        String reverseTest =  generateUniqueName();
        String ddl = "CREATE TABLE " + reverseTest + " (pk VARCHAR NOT NULL PRIMARY KEY " + sortOrder + ", kv VARCHAR)";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO " + reverseTest + " VALUES(?)";
        PreparedStatement stmt = conn.prepareStatement(dml);
        stmt.setString(1, s);
        stmt.execute();
        conn.commit();
        return reverseTest;
    }
    
    private void testReverse(Connection conn, String s, String tableName) throws Exception {
        StringBuilder buf = new StringBuilder(s);
        String reverse = buf.reverse().toString();
        
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT reverse(pk) FROM " + tableName);
        assertTrue(rs.next());
        assertEquals(reverse, rs.getString(1));
        assertFalse(rs.next());
        
        PreparedStatement stmt = conn.prepareStatement(
            "SELECT pk FROM " + tableName + " WHERE pk=reverse(?)");
        stmt.setString(1, reverse);
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(s, rs.getString(1));
        assertFalse(rs.next());
    }
    
    @Test
    public void testSingleByteReverseAscending() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String s = "abc";
        String tableName = initTable(conn, "ASC", s);
        testReverse(conn, s, tableName);
    }                                                           

    @Test
    public void testMultiByteReverseAscending() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String s = "ɚɦɰɸ";
        String tableName = initTable(conn, "DESC", s);
        testReverse(conn, s, tableName);
    }                                                           

    
    @Test
    public void testSingleByteReverseDecending() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String s = "abc";
        String tableName = initTable(conn, "DESC", s);
        testReverse(conn, s, tableName);
    }                                                           

    @Test
    public void testMultiByteReverseDecending() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String s = "ɚɦɰɸ";
        String tableName = initTable(conn, "ASC", s);
        testReverse(conn, s, tableName);
    }
    
    @Test
    public void testNullReverse() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String s = "abc";
        String tableName = initTable(conn, "ASC", s);

        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT reverse(kv) FROM " + tableName);
        assertTrue(rs.next());
        assertNull(rs.getString(1));
        assertFalse(rs.next());
    }                                                           

}
