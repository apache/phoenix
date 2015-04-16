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
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.junit.Test;

public class InstrFunctionIT extends BaseHBaseManagedTimeIT {
    private void initTable(Connection conn, String sortOrder, String s, String subStr) throws Exception {
        String ddl = "CREATE TABLE SAMPLE (name VARCHAR NOT NULL PRIMARY KEY " + sortOrder + ", substr VARCHAR)";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO SAMPLE VALUES(?,?)";
        PreparedStatement stmt = conn.prepareStatement(dml);
        stmt.setString(1, s);
        stmt.setString(2, subStr);
        stmt.execute();
        conn.commit();        
    }
    
     private void testInstr(Connection conn, String queryToExecute, Integer expValue) throws Exception {        
        ResultSet rs;
        rs = conn.createStatement().executeQuery(queryToExecute);
        assertTrue(rs.next());
        assertEquals(expValue.intValue(), rs.getInt(1));
        assertFalse(rs.next());
        
    }
    
      private void testInstrFilter(Connection conn, String queryToExecute, String expected) throws Exception {        
        ResultSet rs;
        PreparedStatement stmt = conn.prepareStatement(queryToExecute);
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(expected, rs.getString(1));
        
    }

    @Test
    public void testSingleByteInstrAscending() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        initTable(conn, "ASC", "abcdefghijkl","fgh");
        String queryToExecute = "SELECT INSTR(name, 'fgh') FROM SAMPLE";
        testInstr(conn, queryToExecute, 5);
    }
    
    @Test
    public void testSingleByteInstrDescending() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        initTable(conn, "DESC", "abcdefghijkl","fgh");
        String queryToExecute = "SELECT INSTR(name, 'fgh') FROM SAMPLE";
        testInstr(conn, queryToExecute, 5);
    }
    
    @Test
    public void testSingleByteInstrAscendingNoString() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        initTable(conn, "ASC", "abcde fghijkl","lmn");
        String queryToExecute = "SELECT INSTR(name, 'lmn') FROM SAMPLE";
        testInstr(conn, queryToExecute, -1);
    }
    
    @Test
    public void testSingleByteInstrDescendingNoString() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        initTable(conn, "DESC", "abcde fghijkl","lmn");
        String queryToExecute = "SELECT INSTR(name, 'lmn') FROM SAMPLE";
        testInstr(conn, queryToExecute, -1);
    }

    @Test
    public void testMultiByteInstrAscending() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        initTable(conn, "ASC", "AɚɦFGH","ɚɦ");
        String queryToExecute = "SELECT INSTR(name, 'ɚɦ') FROM SAMPLE";
        testInstr(conn, queryToExecute, 1);
    }
    
    @Test
    public void testMultiByteInstrDecending() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        initTable(conn, "DESC", "AɚɦFGH","ɚɦ");
        String queryToExecute = "SELECT INSTR(name, 'ɚɦ') FROM SAMPLE";
        testInstr(conn, queryToExecute, 1);
    } 

    @Test
    public void testByteInstrAscendingFilter() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        initTable(conn, "ASC", "abcdefghijkl","fgh");
        String queryToExecute = "select NAME from sample where instr(name, 'fgh') > 0";
        testInstrFilter(conn, queryToExecute,"abcdefghijkl");
    }
    
    
    @Test
    public void testByteInstrDecendingFilter() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        initTable(conn, "DESC", "abcdefghijkl","fgh");
        String queryToExecute = "select NAME from sample where instr(name, 'fgh') > 0";
        testInstrFilter(conn, queryToExecute,"abcdefghijkl");
    }

}
