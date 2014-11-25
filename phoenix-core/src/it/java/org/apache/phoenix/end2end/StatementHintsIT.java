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
import java.util.Properties;

import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;


/**
 * End-to-End tests on various statement hints.
 */


public class StatementHintsIT extends BaseHBaseManagedTimeIT {

    private static void initTableValues() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        
        try {
            String ddl = "CREATE TABLE test_table" +
                    "   (a_integer integer not null, \n" +
                    "    a_string varchar not null, \n" +
                    "    a_id char(3) not null,\n" +
                    "    b_string varchar \n" +
                    "    CONSTRAINT pk PRIMARY KEY (a_integer, a_string, a_id))\n";
            createTestTable(getUrl(), ddl);
            
            String query;
            PreparedStatement stmt;
            
            query = "UPSERT INTO test_table"
                    + "(a_integer, a_string, a_id, b_string) "
                    + "VALUES(?,?,?,?)";
            stmt = conn.prepareStatement(query);
            
            stmt.setInt(1, 1);
            stmt.setString(2, "ab");
            stmt.setString(3, "123");
            stmt.setString(4, "abc");
            stmt.execute();
            
            stmt.setInt(1, 1);
            stmt.setString(2, "abc");
            stmt.setString(3, "456");
            stmt.setString(4, "abc");
            stmt.execute();
            
            stmt.setInt(1, 1);
            stmt.setString(2, "de");
            stmt.setString(3, "123");
            stmt.setString(4, "abc");
            stmt.execute();
            
            stmt.setInt(1, 2);
            stmt.setString(2, "abc");
            stmt.setString(3, "123");
            stmt.setString(4, "def");
            stmt.execute();

            stmt.setInt(1, 3);
            stmt.setString(2, "abc");
            stmt.setString(3, "123");
            stmt.setString(4, "ghi");
            stmt.execute();

            stmt.setInt(1, 4);
            stmt.setString(2, "abc");
            stmt.setString(3, "123");
            stmt.setString(4, "jkl");
            stmt.execute();
            conn.commit();
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectForceRangeScan() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues();
            String query = "SELECT /*+ RANGE_SCAN */ * FROM test_table WHERE a_integer IN (1, 2, 3, 4)";
            PreparedStatement stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("ab", rs.getString(2));
            assertEquals("123", rs.getString(3));
            assertEquals("abc", rs.getString(4));
            
            assertTrue(rs.next());
            assertTrue(rs.next());
            
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals("abc", rs.getString(2));
            assertEquals("123", rs.getString(3));
            assertEquals("def", rs.getString(4));
            
            assertTrue(rs.next());
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
            assertEquals("abc", rs.getString(2));
            assertEquals("123", rs.getString(3));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectForceSkipScan() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues();
            // second slot on the 
            String query = "SELECT /*+ SKIP_SCAN */ * FROM test_table WHERE a_string = 'abc'";
            PreparedStatement stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("abc", rs.getString(2));
            assertEquals("456", rs.getString(3));
            
            assertTrue(rs.next());
            assertTrue(rs.next());
            
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
            assertEquals("abc", rs.getString(2));
            assertEquals("123", rs.getString(3));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
}
