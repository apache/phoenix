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

public class RepeatFunctionIT extends BaseHBaseManagedTimeIT {
    private void initTable(Connection conn, String sortOrder, String s) throws Exception {
        String ddl = "CREATE TABLE IF NOT EXISTS SAMPLE (name VARCHAR NOT NULL PRIMARY KEY " + sortOrder + ")";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO SAMPLE VALUES(?)";
        PreparedStatement stmt = conn.prepareStatement(dml);
        stmt.setString(1, s);
        stmt.execute();
        conn.commit();        
    }
    
     private void testRepeat(Connection conn, String queryToExecute, String expValue) throws Exception {       
        ResultSet rs;
        rs = conn.createStatement().executeQuery(queryToExecute);
        assertTrue(rs.next());
        assertEquals(expValue, rs.getString(1));
        assertFalse(rs.next());
        
    }
    
      private void testRepeatFilter(Connection conn, String queryToExecute, String expected) throws Exception {        
        ResultSet rs;
        PreparedStatement stmt = conn.prepareStatement(queryToExecute);
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(expected, rs.getString(1));
        
    }

      private void testRepeatOrder(Connection conn, String queryToExecute, String [] expected) throws Exception {        
          ResultSet rs;
          PreparedStatement stmt = conn.prepareStatement(queryToExecute);
          rs = stmt.executeQuery();
          assertTrue(rs.next());
          assertEquals(expected[0], rs.getString(1));
          assertTrue(rs.next());
          assertEquals(expected[1], rs.getString(1));
          
    }
    @Test
    public void testSingleByteRepeatAscending() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        initTable(conn, "ASC", "abc");
        String queryToExecute = "SELECT REPEAT(name, 2) FROM SAMPLE";
        testRepeat(conn, queryToExecute, "abcabc");
    }
    
    @Test
    public void testSingleByteRepeatDescending() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        initTable(conn, "DESC", "abc");
        String queryToExecute = "SELECT REPEAT(name, 2) FROM SAMPLE";
        testRepeat(conn, queryToExecute, "abcabc");
    }

    @Test
    public void testMultiByteRepeatAscending() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        initTable(conn, "ASC", "Aɚɦ");
        String queryToExecute = "SELECT REPEAT(name, 3) FROM SAMPLE";
        testRepeat(conn, queryToExecute, "AɚɦAɚɦAɚɦ");
    }
    
    @Test
    public void testMultiByteRepeatDecending() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        initTable(conn, "DESC", "Aɚɦ");
        String queryToExecute = "SELECT REPEAT(name, 3) FROM SAMPLE";
        testRepeat(conn, queryToExecute, "AɚɦAɚɦAɚɦ");
    } 

    @Test
    public void testByteRepeatAscendingFilter() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        initTable(conn, "ASC", "abc");
        String queryToExecute = "select NAME from sample where REPEAT(name, 2) = 'abcabc' ";
        testRepeatFilter(conn, queryToExecute,"abc");
    }
    
    
    @Test
    public void testByteRepeatDecendingFilter() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        initTable(conn, "DESC", "abc");
        String queryToExecute = "select NAME from sample where REPEAT(name, 2) = 'abcabc' ";
        testRepeatFilter(conn, queryToExecute,"abc");
    }

    @Test
    public void testByteRepeatOrderBy() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        initTable(conn, "ASC", "xyz");
        initTable(conn, "ASC","abc");
        String queryToExecute = "select NAME from sample ORDER BY REPEAT(name,1) ";
        String [] expected = {"abc", "xyz"};
        testRepeatOrder(conn, queryToExecute,expected);
    }

}
