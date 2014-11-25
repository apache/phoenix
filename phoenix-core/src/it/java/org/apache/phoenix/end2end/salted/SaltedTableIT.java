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
package org.apache.phoenix.end2end.salted;

import static org.apache.phoenix.util.TestUtil.TABLE_WITH_SALTING;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.end2end.BaseClientManagedTimeIT;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Test;


/**
 * Tests for table with transparent salting.
 */

public class SaltedTableIT extends BaseClientManagedTimeIT {

    private static void initTableValues(byte[][] splits, long ts) throws Exception {
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        
        // Rows we inserted:
        // 1ab123abc111
        // 1abc456abc111
        // 1de123abc111
        // 2abc123def222 
        // 3abc123ghi333
        // 4abc123jkl444
        try {
            // Upsert with no column specifies.
            ensureTableCreated(getUrl(), TABLE_WITH_SALTING, splits, ts-2);
            String query = "UPSERT INTO " + TABLE_WITH_SALTING + " VALUES(?,?,?,?,?)";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setInt(1, 1);
            stmt.setString(2, "ab");
            stmt.setString(3, "123");
            stmt.setString(4, "abc");
            stmt.setInt(5, 111);
            stmt.execute();
            conn.commit();
            
            stmt.setInt(1, 1);
            stmt.setString(2, "abc");
            stmt.setString(3, "456");
            stmt.setString(4, "abc");
            stmt.setInt(5, 111);
            stmt.execute();
            conn.commit();
            
            // Test upsert when statement explicitly specifies the columns to upsert into.
            query = "UPSERT INTO " + TABLE_WITH_SALTING +
                    " (a_integer, a_string, a_id, b_string, b_integer) " + 
                    " VALUES(?,?,?,?,?)";
            stmt = conn.prepareStatement(query);
            
            stmt.setInt(1, 1);
            stmt.setString(2, "de");
            stmt.setString(3, "123");
            stmt.setString(4, "abc");
            stmt.setInt(5, 111);
            stmt.execute();
            conn.commit();
            
            stmt.setInt(1, 2);
            stmt.setString(2, "abc");
            stmt.setString(3, "123");
            stmt.setString(4, "def");
            stmt.setInt(5, 222);
            stmt.execute();
            conn.commit();
            
            // Test upsert when order of column is shuffled.
            query = "UPSERT INTO " + TABLE_WITH_SALTING +
                    " (a_string, a_integer, a_id, b_string, b_integer) " + 
                    " VALUES(?,?,?,?,?)";
            stmt = conn.prepareStatement(query);
            stmt.setString(1, "abc");
            stmt.setInt(2, 3);
            stmt.setString(3, "123");
            stmt.setString(4, "ghi");
            stmt.setInt(5, 333);
            stmt.execute();
            conn.commit();
            
            stmt.setString(1, "abc");
            stmt.setInt(2, 4);
            stmt.setString(3, "123");
            stmt.setString(4, "jkl");
            stmt.setInt(5, 444);
            stmt.execute();
            conn.commit();
        } finally {
            conn.close();
        }
    }

    @Test
    public void testTableWithInvalidBucketNumber() throws Exception {
        long ts = nextTimestamp();
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            String query = "create table salted_table (a_integer integer not null CONSTRAINT pk PRIMARY KEY (a_integer)) SALT_BUCKETS = 257";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.execute();
            fail("Should have caught exception");
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 1021 (42Y80): Salt bucket numbers should be with 1 and 256."));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testTableWithSplit() throws Exception {
        try {
            createTestTable(getUrl(), "create table salted_table (a_integer integer not null primary key) SALT_BUCKETS = 4",
                    new byte[][] {{1}, {2,3}, {2,5}, {3}}, nextTimestamp());
            fail("Should have caught exception");
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 1022 (42Y81): Should not specify split points on salted table with default row key order."));
        }
    }
    
    @Test
    public void testSelectValueNoWhereClause() throws Exception {
        long ts = nextTimestamp();
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            
            String query = "SELECT * FROM " + TABLE_WITH_SALTING;
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("ab", rs.getString(2));
            assertEquals("123", rs.getString(3));
            assertEquals("abc", rs.getString(4));
            assertEquals(111, rs.getInt(5));
            
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("abc", rs.getString(2));
            assertEquals("456", rs.getString(3));
            assertEquals("abc", rs.getString(4));
            assertEquals(111, rs.getInt(5));
            
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("de", rs.getString(2));
            assertEquals("123", rs.getString(3));
            assertEquals("abc", rs.getString(4));
            assertEquals(111, rs.getInt(5));
            
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals("abc", rs.getString(2));
            assertEquals("123", rs.getString(3));
            assertEquals("def", rs.getString(4));
            assertEquals(222, rs.getInt(5));
            
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertEquals("abc", rs.getString(2));
            assertEquals("123", rs.getString(3));
            assertEquals("ghi", rs.getString(4));
            assertEquals(333, rs.getInt(5));
            
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
            assertEquals("abc", rs.getString(2));
            assertEquals("123", rs.getString(3));
            assertEquals("jkl", rs.getString(4));
            assertEquals(444, rs.getInt(5));
            
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectValueWithFullyQualifiedWhereClause() throws Exception {
        long ts = nextTimestamp();
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            String query;
            PreparedStatement stmt;
            ResultSet rs;
            
            // Variable length slot with bounded ranges.
            query = "SELECT * FROM " + TABLE_WITH_SALTING + 
                    " WHERE a_integer = 1 AND a_string >= 'ab' AND a_string < 'de' AND a_id = '123'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("ab", rs.getString(2));
            assertEquals("123", rs.getString(3));
            assertEquals("abc", rs.getString(4));
            assertEquals(111, rs.getInt(5));
            assertFalse(rs.next());

            // all single slots with one value.
            query = "SELECT * FROM " + TABLE_WITH_SALTING + 
                    " WHERE a_integer = 1 AND a_string = 'ab' AND a_id = '123'";
            stmt = conn.prepareStatement(query);
            
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("ab", rs.getString(2));
            assertEquals("123", rs.getString(3));
            assertEquals("abc", rs.getString(4));
            assertEquals(111, rs.getInt(5));
            assertFalse(rs.next());
            
            // all single slots with multiple values.
            query = "SELECT * FROM " + TABLE_WITH_SALTING + 
                    " WHERE a_integer in (2, 4) AND a_string = 'abc' AND a_id = '123'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals("abc", rs.getString(2));
            assertEquals("123", rs.getString(3));
            assertEquals("def", rs.getString(4));
            assertEquals(222, rs.getInt(5));
            
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
            assertEquals("abc", rs.getString(2));
            assertEquals("123", rs.getString(3));
            assertEquals("jkl", rs.getString(4));
            assertEquals(444, rs.getInt(5));
            assertFalse(rs.next());
            
            query = "SELECT a_integer, a_string FROM " + TABLE_WITH_SALTING +
                    " WHERE a_integer in (1,2,3,4) AND a_string in ('a', 'abc', 'de') AND a_id = '123'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("de", rs.getString(2));
            
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals("abc", rs.getString(2));
            
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertEquals("abc", rs.getString(2));
            
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
            assertEquals("abc", rs.getString(2));
            assertFalse(rs.next());
            
            // fixed length slot with bounded ranges.
            query = "SELECT a_string, a_id FROM " + TABLE_WITH_SALTING + 
                    " WHERE a_integer > 1 AND a_integer < 4 AND a_string = 'abc' AND a_id = '123'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("abc", rs.getString(1));
            assertEquals("123", rs.getString(2));
            
            assertTrue(rs.next());
            assertEquals("abc", rs.getString(1));
            assertEquals("123", rs.getString(2));
            assertFalse(rs.next());
            
            // fixed length slot with unbound ranges.
            query = "SELECT b_string, b_integer FROM " + TABLE_WITH_SALTING + 
                    " WHERE a_integer > 1 AND a_string = 'abc' AND a_id = '123'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("def", rs.getString(1));
            assertEquals(222, rs.getInt(2));
            
            assertTrue(rs.next());
            assertEquals("ghi", rs.getString(1));
            assertEquals(333, rs.getInt(2));
            
            assertTrue(rs.next());
            assertEquals("jkl", rs.getString(1));
            assertEquals(444, rs.getInt(2));
            assertFalse(rs.next());
            
            // Variable length slot with unbounded ranges.
            query = "SELECT * FROM " + TABLE_WITH_SALTING + 
                    " WHERE a_integer = 1 AND a_string > 'ab' AND a_id = '123'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("de", rs.getString(2));
            assertEquals("123", rs.getString(3));
            assertEquals("abc", rs.getString(4));
            assertEquals(111, rs.getInt(5));
            assertFalse(rs.next());

        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectValueWithNotFullyQualifiedWhereClause() throws Exception {
        long ts = nextTimestamp();
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            
            // Where without fully qualified key, point query.
            String query = "SELECT * FROM " + TABLE_WITH_SALTING + " WHERE a_integer = ? AND a_string = ?";
            PreparedStatement stmt = conn.prepareStatement(query);
            
            stmt.setInt(1, 1);
            stmt.setString(2, "abc");
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("abc", rs.getString(2));
            assertEquals("456", rs.getString(3));
            assertEquals("abc", rs.getString(4));
            assertEquals(111, rs.getInt(5));
            assertFalse(rs.next());
            
            // Where without fully qualified key, range query.
            query = "SELECT * FROM " + TABLE_WITH_SALTING + " WHERE a_integer >= 2";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals("abc", rs.getString(2));
            assertEquals("123", rs.getString(3));
            assertEquals("def", rs.getString(4));
            assertEquals(222, rs.getInt(5));
            
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertEquals("abc", rs.getString(2));
            assertEquals("123", rs.getString(3));
            assertEquals("ghi", rs.getString(4));
            assertEquals(333, rs.getInt(5));
            
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
            assertEquals("abc", rs.getString(2));
            assertEquals("123", rs.getString(3));
            assertEquals("jkl", rs.getString(4));
            assertEquals(444, rs.getInt(5));
            assertFalse(rs.next());
            
            // With point query.
            query = "SELECT a_string FROM " + TABLE_WITH_SALTING + " WHERE a_string = ?";
            stmt = conn.prepareStatement(query);
            stmt.setString(1, "de");
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("de", rs.getString(1));
            assertFalse(rs.next());
            
            query = "SELECT a_id FROM " + TABLE_WITH_SALTING + " WHERE a_id = ?";
            stmt = conn.prepareStatement(query);
            stmt.setString(1, "456");
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("456", rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectWithGroupBy() throws Exception {
        long ts = nextTimestamp();
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            
            String query = "SELECT a_integer FROM " + TABLE_WITH_SALTING + " GROUP BY a_integer";
            PreparedStatement stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            int count = 0;
            while (rs.next()) {
                count++;
            }
            assertEquals("Group by does not return the right count.", count, 4);
        } finally {
            conn.close();
        }
    }

    @Test
    public void testLimitScan() throws Exception {
        long ts = nextTimestamp();
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            
            String query = "SELECT a_integer FROM " + TABLE_WITH_SALTING + " WHERE a_string='abc' LIMIT 1";
            PreparedStatement stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSelectWithOrderByRowKey() throws Exception {
        long ts = nextTimestamp();
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            initTableValues(null, ts);
            
            String query = "SELECT * FROM " + TABLE_WITH_SALTING + " ORDER  BY  a_integer, a_string, a_id";
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet explainPlan = statement.executeQuery("EXPLAIN " + query);
            // Confirm that ORDER BY in row key order will be optimized out for salted table
            assertEquals("CLIENT PARALLEL 4-WAY FULL SCAN OVER TABLE_WITH_SALTING\n" + 
                    "CLIENT MERGE SORT", QueryUtil.getExplainPlan(explainPlan));
            ResultSet rs = statement.executeQuery();
            
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("ab", rs.getString(2));
            assertEquals("123", rs.getString(3));
            assertEquals("abc", rs.getString(4));
            assertEquals(111, rs.getInt(5));
            
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("abc", rs.getString(2));
            assertEquals("456", rs.getString(3));
            assertEquals("abc", rs.getString(4));
            assertEquals(111, rs.getInt(5));
            
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("de", rs.getString(2));
            assertEquals("123", rs.getString(3));
            assertEquals("abc", rs.getString(4));
            assertEquals(111, rs.getInt(5));
            
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals("abc", rs.getString(2));
            assertEquals("123", rs.getString(3));
            assertEquals("def", rs.getString(4));
            assertEquals(222, rs.getInt(5));
            
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertEquals("abc", rs.getString(2));
            assertEquals("123", rs.getString(3));
            assertEquals("ghi", rs.getString(4));
            assertEquals(333, rs.getInt(5));
            
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
            assertEquals("abc", rs.getString(2));
            assertEquals("123", rs.getString(3));
            assertEquals("jkl", rs.getString(4));
            assertEquals(444, rs.getInt(5));
            
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
}
