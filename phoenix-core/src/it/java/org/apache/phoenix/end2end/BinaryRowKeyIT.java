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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;


public class BinaryRowKeyIT extends BaseHBaseManagedTimeIT {

    private static void initTableValues() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        
        try {
            String ddl = "CREATE TABLE test_table" +
                    "   (a_binary binary(10) not null, \n" +
                    "    a_string varchar not null, \n" +
                    "    b_binary varbinary \n" +
                    "    CONSTRAINT pk PRIMARY KEY (a_binary, a_string))\n";
            createTestTable(getUrl(), ddl);
            
            String query;
            PreparedStatement stmt;
            
            query = "UPSERT INTO test_table"
                    + "(a_binary, a_string) "
                    + "VALUES(?,?)";
            stmt = conn.prepareStatement(query);
            
            stmt.setBytes(1, new byte[] {0,0,0,0,0,0,0,0,0,1});
            stmt.setString(2, "a");
            stmt.execute();
            
            stmt.setBytes(1, new byte[] {0,0,0,0,0,0,0,0,0,2});
            stmt.setString(2, "b");
            stmt.execute();
            conn.commit();
            
        } finally {
            conn.close();
        }
    }

    @Test
    public void testInsertPaddedBinaryValue() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues();
            conn.setAutoCommit(true);
            conn.createStatement().execute("DELETE FROM test_table");
           
            String query = "UPSERT INTO test_table"
                    + "(a_binary, a_string) "
                    + "VALUES(?,?)";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setBytes(1, new byte[] {0,0,0,0,0,0,0,0,1});
            stmt.setString(2, "a");
            stmt.execute();
            
            ResultSet rs = conn.createStatement().executeQuery("SELECT a_string FROM test_table");
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectValues() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        
        try {
            initTableValues();
            
            String query = "SELECT * FROM test_table";
            PreparedStatement stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            
            assertTrue(rs.next());
            assertArrayEquals(new byte[] {0,0,0,0,0,0,0,0,0,1}, rs.getBytes(1));
            assertEquals("a", rs.getString(2));
            
            assertTrue(rs.next());
            assertArrayEquals(new byte[] {0,0,0,0,0,0,0,0,0,2}, rs.getBytes(1));
            assertEquals("b", rs.getString(2));
            
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testUpsertSelectValues() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        
        try {
            initTableValues();
            
            String query = "UPSERT INTO test_table (a_binary, a_string, b_binary) "
                    + " SELECT a_binary, a_string, a_binary FROM test_table";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.execute();
            conn.commit();
            
            query = "SELECT a_binary, b_binary FROM test_table";
            stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            
            assertTrue(rs.next());
            assertArrayEquals(new byte[] {0,0,0,0,0,0,0,0,0,1}, rs.getBytes(1));
            assertArrayEquals(new byte[] {0,0,0,0,0,0,0,0,0,1}, rs.getBytes(2));
            
            assertTrue(rs.next());
            assertArrayEquals(new byte[] {0,0,0,0,0,0,0,0,0,2}, rs.getBytes(1));
            assertArrayEquals(new byte[] {0,0,0,0,0,0,0,0,0,2}, rs.getBytes(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
}
