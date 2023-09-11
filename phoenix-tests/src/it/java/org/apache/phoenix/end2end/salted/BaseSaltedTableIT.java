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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryBuilder;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Test;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

public abstract class BaseSaltedTableIT extends ParallelStatsDisabledIT  {

    protected static String initTableValues(byte[][] splits) throws Exception {
        String tableName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);

        // Rows we inserted:
        // 1ab123abc111
        // 1abc456abc111
        // 1de123abc111
        // 2abc123def222
        // 3abc123ghi333
        // 4abc123jkl444
        try {
            // Upsert with no column specifies.
            ensureTableCreated(getUrl(), tableName, TABLE_WITH_SALTING, splits, null, null);
            String query = "UPSERT INTO " + tableName + " VALUES(?,?,?,?,?)";
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
            query = "UPSERT INTO " + tableName +
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
            query = "UPSERT INTO " + tableName +
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
        return tableName;
    }

    @Test
    public void testSelectValueNoWhereClause() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            String tableName = initTableValues(null);
            // "SELECT * FROM " + tableName;
            QueryBuilder queryBuilder = new QueryBuilder()
                .setSelectColumns(
                    Lists.newArrayList("A_INTEGER", "A_STRING", "A_ID", "B_STRING", "B_INTEGER"))
                .setFullTableName(tableName);
            ResultSet rs = executeQuery(conn, queryBuilder);
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            String tableName = initTableValues(null);
            PreparedStatement stmt;
            ResultSet rs;

            // Variable length slot with bounded ranges.
            QueryBuilder queryBuilder = new QueryBuilder()
                .setSelectColumns(
                        Lists.newArrayList("A_INTEGER", "A_STRING", "A_ID", "B_STRING", "B_INTEGER"))
                .setFullTableName(tableName)
                .setWhereClause("A_INTEGER = 1 AND A_STRING >= 'ab' AND A_STRING < 'de' AND A_ID = '123'");
            rs = executeQuery(conn, queryBuilder);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("ab", rs.getString(2));
            assertEquals("123", rs.getString(3));
            assertEquals("abc", rs.getString(4));
            assertEquals(111, rs.getInt(5));
            assertFalse(rs.next());

            // all single slots with one value.
            queryBuilder.setWhereClause("A_INTEGER = 1 AND A_STRING = 'ab' AND A_ID = '123'");
            rs = executeQuery(conn, queryBuilder);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("ab", rs.getString(2));
            assertEquals("123", rs.getString(3));
            assertEquals("abc", rs.getString(4));
            assertEquals(111, rs.getInt(5));
            assertFalse(rs.next());

            // all single slots with multiple values.
            queryBuilder.setWhereClause("A_INTEGER in (2, 4) AND A_STRING = 'abc' AND A_ID = '123'");
            rs = executeQuery(conn, queryBuilder);

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

            queryBuilder.setWhereClause("A_INTEGER in (1,2,3,4) AND A_STRING in ('a', 'abc', 'de') AND A_ID = '123'");
            queryBuilder.setSelectColumns(Lists.newArrayList("A_INTEGER", "A_STRING", "A_ID"));
            rs = executeQuery(conn, queryBuilder);
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
            queryBuilder.setWhereClause("A_INTEGER > 1 AND A_INTEGER < 4 AND A_STRING = 'abc' AND A_ID = '123'");
            queryBuilder.setSelectColumns(Lists.newArrayList("A_STRING", "A_ID", "A_INTEGER"));
            rs = executeQuery(conn, queryBuilder);
            assertTrue(rs.next());
            assertEquals("abc", rs.getString(1));
            assertEquals("123", rs.getString(2));

            assertTrue(rs.next());
            assertEquals("abc", rs.getString(1));
            assertEquals("123", rs.getString(2));
            assertFalse(rs.next());

            // fixed length slot with unbound ranges.
            queryBuilder.setWhereClause("A_INTEGER > 1 AND A_STRING = 'abc' AND A_ID = '123'");
            queryBuilder.setSelectColumns(Lists.newArrayList("B_STRING", "B_INTEGER", "A_INTEGER", "A_STRING", "A_ID"));
            rs = executeQuery(conn, queryBuilder);
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
            queryBuilder.setWhereClause("A_INTEGER = 1 AND A_STRING > 'ab' AND A_ID = '123'");
            queryBuilder.setSelectColumns(
                Lists.newArrayList("A_INTEGER", "A_STRING", "A_ID", "B_STRING", "B_INTEGER"));
            rs = executeQuery(conn, queryBuilder);
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            String tableName = initTableValues(null);

            // Where without fully qualified key, point query.
            String query = "SELECT * FROM " + tableName + " WHERE a_integer = ? AND a_string = ?";
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
            query = "SELECT * FROM " + tableName + " WHERE a_integer >= 2";
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
            query = "SELECT a_string FROM " + tableName + " WHERE a_string = ?";
            stmt = conn.prepareStatement(query);
            stmt.setString(1, "de");
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("de", rs.getString(1));
            assertFalse(rs.next());

            query = "SELECT a_id FROM " + tableName + " WHERE a_id = ?";
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            String tableName = initTableValues(null);

            String query = "SELECT a_integer FROM " + tableName + " GROUP BY a_integer";
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            String tableName = initTableValues(null);

            String query = "SELECT a_integer FROM " + tableName + " WHERE a_string='abc' LIMIT 1";
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            String tableName = initTableValues(null);

            String query = "SELECT * FROM " + tableName + " ORDER  BY  a_integer, a_string, a_id";
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet explainPlan = statement.executeQuery("EXPLAIN " + query);
            // Confirm that ORDER BY in row key order will be optimized out for salted table
            assertEquals("CLIENT PARALLEL 4-WAY FULL SCAN OVER " + tableName + "\n" +
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
