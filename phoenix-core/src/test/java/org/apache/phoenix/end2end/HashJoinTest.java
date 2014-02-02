/*
 * Copyright 2010 The Apache Software Foundation
 *
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

import static org.apache.phoenix.util.TestUtil.JOIN_CUSTOMER_TABLE;
import static org.apache.phoenix.util.TestUtil.JOIN_ITEM_TABLE;
import static org.apache.phoenix.util.TestUtil.JOIN_ORDER_TABLE;
import static org.apache.phoenix.util.TestUtil.JOIN_SUPPLIER_TABLE;
import static org.apache.phoenix.util.TestUtil.PHOENIX_JDBC_URL;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.schema.TableAlreadyExistsException;
import org.apache.phoenix.util.QueryUtil;

@RunWith(Parameterized.class)
public class HashJoinTest extends BaseHBaseManagedTimeTest {
    
    private String[] indexDDL;
    private String[] plans;
    
    public HashJoinTest(String[] indexDDL, String[] plans) {
        this.indexDDL = indexDDL;
        this.plans = plans;
    }
    
    @Before
    public void initTable() throws Exception {
        initTableValues();
        if (indexDDL != null && indexDDL.length > 0) {
            Properties props = new Properties(TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl(), props);
            for (String ddl : indexDDL) {
                try {
                    conn.createStatement().execute(ddl);
                } catch (TableAlreadyExistsException e) {
                }
            }
            conn.close();
        }
    }
    
    @Parameters(name="{0}")
    public static Collection<Object> data() {
        List<Object> testCases = Lists.newArrayList();
        testCases.add(new String[][] {
                {}, {
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER JOIN_ITEM_TABLE\n" +
                "    SERVER FILTER BY (NAME >= 'T1' AND NAME <= 'T5')\n" +
                "    PARALLEL EQUI-JOIN 1 HASH TABLES:\n" +
                "    BUILD HASH TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER JOIN_SUPPLIER_TABLE\n" +
                "            SERVER FILTER BY (NAME >= 'S1' AND NAME <= 'S5')",
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER JOIN_ITEM_TABLE\n" +
                "    SERVER FILTER BY (NAME = 'T1' OR NAME = 'T5')\n" +
                "    PARALLEL EQUI-JOIN 1 HASH TABLES:\n" +
                "    BUILD HASH TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER JOIN_SUPPLIER_TABLE\n" +
                "            SERVER FILTER BY (NAME = 'S1' OR NAME = 'S5')",
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER JOIN_ITEM_TABLE\n" +
                "    PARALLEL EQUI-JOIN 2 HASH TABLES:\n" +
                "    BUILD HASH TABLE 0 (SKIP MERGE)\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER JOIN_ORDER_TABLE\n" +
                "            SERVER FILTER BY QUANTITY < 5000\n" +
                "    BUILD HASH TABLE 1\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER JOIN_SUPPLIER_TABLE"
                }});
        testCases.add(new String[][] {
                {
                "CREATE INDEX INDEX_" + JOIN_CUSTOMER_TABLE + " ON " + JOIN_CUSTOMER_TABLE + " (name)",
                "CREATE INDEX INDEX_" + JOIN_ITEM_TABLE + " ON " + JOIN_ITEM_TABLE + " (name)",
                "CREATE INDEX INDEX_" + JOIN_SUPPLIER_TABLE + " ON " + JOIN_SUPPLIER_TABLE + " (name)"
                }, {
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER INDEX_JOIN_ITEM_TABLE ['T1'] - ['T5']\n" +
                "    PARALLEL EQUI-JOIN 1 HASH TABLES:\n" +
                "    BUILD HASH TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER INDEX_JOIN_SUPPLIER_TABLE ['S1'] - ['S5']",
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER JOIN_ITEM_TABLE\n" +
                "    SERVER FILTER BY (NAME = 'T1' OR NAME = 'T5')\n" +
                "    PARALLEL EQUI-JOIN 1 HASH TABLES:\n" +
                "    BUILD HASH TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY SKIP SCAN ON 2 KEYS OVER INDEX_JOIN_SUPPLIER_TABLE ['S1'] - ['S5']",
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER JOIN_ITEM_TABLE\n" +
                "    PARALLEL EQUI-JOIN 2 HASH TABLES:\n" +
                "    BUILD HASH TABLE 0 (SKIP MERGE)\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER JOIN_ORDER_TABLE\n" +
                "            SERVER FILTER BY QUANTITY < 5000\n" +
                "    BUILD HASH TABLE 1\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER INDEX_JOIN_SUPPLIER_TABLE"
                }});
        return testCases;
    }
    
    
    protected void initTableValues() throws Exception {
        ensureTableCreated(getUrl(), JOIN_CUSTOMER_TABLE);
        ensureTableCreated(getUrl(), JOIN_ITEM_TABLE);
        ensureTableCreated(getUrl(), JOIN_SUPPLIER_TABLE);
        ensureTableCreated(getUrl(), JOIN_ORDER_TABLE);
        
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            conn.createStatement().execute("CREATE SEQUENCE my.seq");
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            // Insert into customer table
            PreparedStatement stmt = conn.prepareStatement(
                    "upsert into " + JOIN_CUSTOMER_TABLE +
                    "   (CUSTOMER_ID, " +
                    "    NAME, " +
                    "    PHONE, " +
                    "    ADDRESS, " +
                    "    LOC_ID, " +
                    "    DATE) " +
                    "values (?, ?, ?, ?, ?, ?)");
            stmt.setString(1, "0000000001");
            stmt.setString(2, "C1");
            stmt.setString(3, "999-999-1111");
            stmt.setString(4, "101 XXX Street");
            stmt.setString(5, "10001");
            stmt.setDate(6, new Date(format.parse("2013-11-01 10:20:36").getTime()));
            stmt.execute();
                
            stmt.setString(1, "0000000002");
            stmt.setString(2, "C2");
            stmt.setString(3, "999-999-2222");
            stmt.setString(4, "202 XXX Street");
            stmt.setString(5, null);
            stmt.setDate(6, new Date(format.parse("2013-11-25 16:45:07").getTime()));
            stmt.execute();

            stmt.setString(1, "0000000003");
            stmt.setString(2, "C3");
            stmt.setString(3, "999-999-3333");
            stmt.setString(4, "303 XXX Street");
            stmt.setString(5, null);
            stmt.setDate(6, new Date(format.parse("2013-11-25 10:06:29").getTime()));
            stmt.execute();

            stmt.setString(1, "0000000004");
            stmt.setString(2, "C4");
            stmt.setString(3, "999-999-4444");
            stmt.setString(4, "404 XXX Street");
            stmt.setString(5, "10004");
            stmt.setDate(6, new Date(format.parse("2013-11-22 14:22:56").getTime()));
            stmt.execute();

            stmt.setString(1, "0000000005");
            stmt.setString(2, "C5");
            stmt.setString(3, "999-999-5555");
            stmt.setString(4, "505 XXX Street");
            stmt.setString(5, "10005");
            stmt.setDate(6, new Date(format.parse("2013-11-27 09:37:50").getTime()));
            stmt.execute();

            stmt.setString(1, "0000000006");
            stmt.setString(2, "C6");
            stmt.setString(3, "999-999-6666");
            stmt.setString(4, "606 XXX Street");
            stmt.setString(5, "10001");
            stmt.setDate(6, new Date(format.parse("2013-11-01 10:20:36").getTime()));
            stmt.execute();
            
            // Insert into item table
            stmt = conn.prepareStatement(
                    "upsert into " + JOIN_ITEM_TABLE +
                    "   (ITEM_ID, " +
                    "    NAME, " +
                    "    PRICE, " +
                    "    DISCOUNT1, " +
                    "    DISCOUNT2, " +
                    "    SUPPLIER_ID, " +
                    "    DESCRIPTION) " +
                    "values (?, ?, ?, ?, ?, ?, ?)");
            stmt.setString(1, "0000000001");
            stmt.setString(2, "T1");
            stmt.setInt(3, 100);
            stmt.setInt(4, 5);
            stmt.setInt(5, 10);
            stmt.setString(6, "0000000001");
            stmt.setString(7, "Item T1");
            stmt.execute();

            stmt.setString(1, "0000000002");
            stmt.setString(2, "T2");
            stmt.setInt(3, 200);
            stmt.setInt(4, 5);
            stmt.setInt(5, 8);
            stmt.setString(6, "0000000001");
            stmt.setString(7, "Item T2");
            stmt.execute();

            stmt.setString(1, "0000000003");
            stmt.setString(2, "T3");
            stmt.setInt(3, 300);
            stmt.setInt(4, 8);
            stmt.setInt(5, 12);
            stmt.setString(6, "0000000002");
            stmt.setString(7, "Item T3");
            stmt.execute();

            stmt.setString(1, "0000000004");
            stmt.setString(2, "T4");
            stmt.setInt(3, 400);
            stmt.setInt(4, 6);
            stmt.setInt(5, 10);
            stmt.setString(6, "0000000002");
            stmt.setString(7, "Item T4");
            stmt.execute();

            stmt.setString(1, "0000000005");
            stmt.setString(2, "T5");
            stmt.setInt(3, 500);
            stmt.setInt(4, 8);
            stmt.setInt(5, 15);
            stmt.setString(6, "0000000005");
            stmt.setString(7, "Item T5");
            stmt.execute();

            stmt.setString(1, "0000000006");
            stmt.setString(2, "T6");
            stmt.setInt(3, 600);
            stmt.setInt(4, 8);
            stmt.setInt(5, 15);
            stmt.setString(6, "0000000006");
            stmt.setString(7, "Item T6");
            stmt.execute();
            
            stmt.setString(1, "invalid001");
            stmt.setString(2, "INVALID-1");
            stmt.setInt(3, 0);
            stmt.setInt(4, 0);
            stmt.setInt(5, 0);
            stmt.setString(6, "0000000000");
            stmt.setString(7, "Invalid item for join test");
            stmt.execute();

            // Insert into supplier table
            stmt = conn.prepareStatement(
                    "upsert into " + JOIN_SUPPLIER_TABLE +
                    "   (SUPPLIER_ID, " +
                    "    NAME, " +
                    "    PHONE, " +
                    "    ADDRESS, " +
                    "    LOC_ID) " +
                    "values (?, ?, ?, ?, ?)");
            stmt.setString(1, "0000000001");
            stmt.setString(2, "S1");
            stmt.setString(3, "888-888-1111");
            stmt.setString(4, "101 YYY Street");
            stmt.setString(5, "10001");
            stmt.execute();
                
            stmt.setString(1, "0000000002");
            stmt.setString(2, "S2");
            stmt.setString(3, "888-888-2222");
            stmt.setString(4, "202 YYY Street");
            stmt.setString(5, "10002");
            stmt.execute();

            stmt.setString(1, "0000000003");
            stmt.setString(2, "S3");
            stmt.setString(3, "888-888-3333");
            stmt.setString(4, "303 YYY Street");
            stmt.setString(5, null);
            stmt.execute();

            stmt.setString(1, "0000000004");
            stmt.setString(2, "S4");
            stmt.setString(3, "888-888-4444");
            stmt.setString(4, "404 YYY Street");
            stmt.setString(5, null);
            stmt.execute();

            stmt.setString(1, "0000000005");
            stmt.setString(2, "S5");
            stmt.setString(3, "888-888-5555");
            stmt.setString(4, "505 YYY Street");
            stmt.setString(5, "10005");
            stmt.execute();

            stmt.setString(1, "0000000006");
            stmt.setString(2, "S6");
            stmt.setString(3, "888-888-6666");
            stmt.setString(4, "606 YYY Street");
            stmt.setString(5, "10006");
            stmt.execute();

            // Insert into order table
            stmt = conn.prepareStatement(
                    "upsert into " + JOIN_ORDER_TABLE +
                    "   (ORDER_ID, " +
                    "    CUSTOMER_ID, " +
                    "    ITEM_ID, " +
                    "    PRICE, " +
                    "    QUANTITY," +
                    "    DATE) " +
                    "values (?, ?, ?, ?, ?, ?)");
            stmt.setString(1, "000000000000001");
            stmt.setString(2, "0000000004");
            stmt.setString(3, "0000000001");
            stmt.setInt(4, 100);
            stmt.setInt(5, 1000);
            stmt.setTimestamp(6, new Timestamp(format.parse("2013-11-22 14:22:56").getTime()));
            stmt.execute();

            stmt.setString(1, "000000000000002");
            stmt.setString(2, "0000000003");
            stmt.setString(3, "0000000006");
            stmt.setInt(4, 552);
            stmt.setInt(5, 2000);
            stmt.setTimestamp(6, new Timestamp(format.parse("2013-11-25 10:06:29").getTime()));
            stmt.execute();

            stmt.setString(1, "000000000000003");
            stmt.setString(2, "0000000002");
            stmt.setString(3, "0000000002");
            stmt.setInt(4, 190);
            stmt.setInt(5, 3000);
            stmt.setTimestamp(6, new Timestamp(format.parse("2013-11-25 16:45:07").getTime()));
            stmt.execute();

            stmt.setString(1, "000000000000004");
            stmt.setString(2, "0000000004");
            stmt.setString(3, "0000000006");
            stmt.setInt(4, 510);
            stmt.setInt(5, 4000);
            stmt.setTimestamp(6, new Timestamp(format.parse("2013-11-26 13:26:04").getTime()));
            stmt.execute();

            stmt.setString(1, "000000000000005");
            stmt.setString(2, "0000000005");
            stmt.setString(3, "0000000003");
            stmt.setInt(4, 264);
            stmt.setInt(5, 5000);
            stmt.setTimestamp(6, new Timestamp(format.parse("2013-11-27 09:37:50").getTime()));
            stmt.execute();

            conn.commit();
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDefaultJoin() throws Exception {
        String query = "SELECT item.item_id, item.name, supp.supplier_id, supp.name FROM " + JOIN_ITEM_TABLE + " item JOIN " + JOIN_SUPPLIER_TABLE + " supp ON item.supplier_id = supp.supplier_id";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000001");
            assertEquals(rs.getString(2), "T1");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000002");
            assertEquals(rs.getString(2), "T2");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000003");
            assertEquals(rs.getString(2), "T3");
            assertEquals(rs.getString(3), "0000000002");
            assertEquals(rs.getString(4), "S2");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000004");
            assertEquals(rs.getString(2), "T4");
            assertEquals(rs.getString(3), "0000000002");
            assertEquals(rs.getString(4), "S2");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000005");
            assertEquals(rs.getString(2), "T5");
            assertEquals(rs.getString(3), "0000000005");
            assertEquals(rs.getString(4), "S5");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000006");
            assertEquals(rs.getString(2), "T6");
            assertEquals(rs.getString(3), "0000000006");
            assertEquals(rs.getString(4), "S6");

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testInnerJoin() throws Exception {
        String query = "SELECT item.item_id, item.name, supp.supplier_id, supp.name, next value for my.seq FROM " + JOIN_ITEM_TABLE + " item INNER JOIN " + JOIN_SUPPLIER_TABLE + " supp ON item.supplier_id = supp.supplier_id";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000001");
            assertEquals(rs.getString(2), "T1");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "S1");
            assertEquals(1, rs.getInt(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000002");
            assertEquals(rs.getString(2), "T2");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "S1");
            assertEquals(2, rs.getInt(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000003");
            assertEquals(rs.getString(2), "T3");
            assertEquals(rs.getString(3), "0000000002");
            assertEquals(rs.getString(4), "S2");
            assertEquals(3, rs.getInt(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000004");
            assertEquals(rs.getString(2), "T4");
            assertEquals(rs.getString(3), "0000000002");
            assertEquals(rs.getString(4), "S2");
            assertEquals(4, rs.getInt(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000005");
            assertEquals(rs.getString(2), "T5");
            assertEquals(rs.getString(3), "0000000005");
            assertEquals(rs.getString(4), "S5");
            assertEquals(5, rs.getInt(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000006");
            assertEquals(rs.getString(2), "T6");
            assertEquals(rs.getString(3), "0000000006");
            assertEquals(rs.getString(4), "S6");
            assertEquals(6, rs.getInt(5));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
            
    @Test
    public void testLeftJoin() throws Exception {
        String query[] = new String[3];
        query[0] = "SELECT item.item_id, item.name, supp.supplier_id, supp.name, next value for my.seq FROM " + JOIN_ITEM_TABLE + " item LEFT JOIN " + JOIN_SUPPLIER_TABLE + " supp ON item.supplier_id = supp.supplier_id";
        query[1] = "SELECT " + JOIN_ITEM_TABLE + ".item_id, " + JOIN_ITEM_TABLE + ".name, " + JOIN_SUPPLIER_TABLE + ".supplier_id, " + JOIN_SUPPLIER_TABLE + ".name, next value for my.seq FROM " + JOIN_ITEM_TABLE + " LEFT JOIN " + JOIN_SUPPLIER_TABLE + " ON " + JOIN_ITEM_TABLE + ".supplier_id = " + JOIN_SUPPLIER_TABLE + ".supplier_id";
        query[2] = "SELECT item.item_id, " + JOIN_ITEM_TABLE + ".name, supp.supplier_id, " + JOIN_SUPPLIER_TABLE + ".name, next value for my.seq FROM " + JOIN_ITEM_TABLE + " item LEFT JOIN " + JOIN_SUPPLIER_TABLE + " supp ON " + JOIN_ITEM_TABLE + ".supplier_id = supp.supplier_id";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            for (int i = 0; i < query.length; i++) {
                PreparedStatement statement = conn.prepareStatement(query[i]);
                ResultSet rs = statement.executeQuery();
                assertTrue (rs.next());
                assertEquals(rs.getString(1), "0000000001");
                assertEquals(rs.getString(2), "T1");
                assertEquals(rs.getString(3), "0000000001");
                assertEquals(rs.getString(4), "S1");
                assertTrue (rs.next());
                assertEquals(rs.getString(1), "0000000002");
                assertEquals(rs.getString(2), "T2");
                assertEquals(rs.getString(3), "0000000001");
                assertEquals(rs.getString(4), "S1");
                assertTrue (rs.next());
                assertEquals(rs.getString(1), "0000000003");
                assertEquals(rs.getString(2), "T3");
                assertEquals(rs.getString(3), "0000000002");
                assertEquals(rs.getString(4), "S2");
                assertTrue (rs.next());
                assertEquals(rs.getString(1), "0000000004");
                assertEquals(rs.getString(2), "T4");
                assertEquals(rs.getString(3), "0000000002");
                assertEquals(rs.getString(4), "S2");
                assertTrue (rs.next());
                assertEquals(rs.getString(1), "0000000005");
                assertEquals(rs.getString(2), "T5");
                assertEquals(rs.getString(3), "0000000005");
                assertEquals(rs.getString(4), "S5");
                assertTrue (rs.next());
                assertEquals(rs.getString(1), "0000000006");
                assertEquals(rs.getString(2), "T6");
                assertEquals(rs.getString(3), "0000000006");
                assertEquals(rs.getString(4), "S6");
                assertTrue (rs.next());
                assertEquals(rs.getString(1), "invalid001");
                assertEquals(rs.getString(2), "INVALID-1");
                assertNull(rs.getString(3));
                assertNull(rs.getString(4));

                assertFalse(rs.next());
                rs.close();
                statement.close();
            }
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testRightJoin() throws Exception {
        String query = "SELECT item.item_id, item.name, supp.supplier_id, supp.name FROM " + JOIN_SUPPLIER_TABLE + " supp RIGHT JOIN " + JOIN_ITEM_TABLE + " item ON item.supplier_id = supp.supplier_id";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000001");
            assertEquals(rs.getString(2), "T1");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000002");
            assertEquals(rs.getString(2), "T2");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000003");
            assertEquals(rs.getString(2), "T3");
            assertEquals(rs.getString(3), "0000000002");
            assertEquals(rs.getString(4), "S2");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000004");
            assertEquals(rs.getString(2), "T4");
            assertEquals(rs.getString(3), "0000000002");
            assertEquals(rs.getString(4), "S2");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000005");
            assertEquals(rs.getString(2), "T5");
            assertEquals(rs.getString(3), "0000000005");
            assertEquals(rs.getString(4), "S5");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000006");
            assertEquals(rs.getString(2), "T6");
            assertEquals(rs.getString(3), "0000000006");
            assertEquals(rs.getString(4), "S6");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "invalid001");
            assertEquals(rs.getString(2), "INVALID-1");
            assertNull(rs.getString(3));
            assertNull(rs.getString(4));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testInnerJoinWithPreFilters() throws Exception {
        String query1 = "SELECT item.item_id, item.name, supp.supplier_id, supp.name FROM " + JOIN_ITEM_TABLE + " item INNER JOIN " + JOIN_SUPPLIER_TABLE + " supp ON item.supplier_id = supp.supplier_id AND supp.supplier_id BETWEEN '0000000001' AND '0000000005'";
        String query2 = "SELECT item.item_id, item.name, supp.supplier_id, supp.name FROM " + JOIN_ITEM_TABLE + " item INNER JOIN " + JOIN_SUPPLIER_TABLE + " supp ON item.supplier_id = supp.supplier_id AND (supp.supplier_id = '0000000001' OR supp.supplier_id = '0000000005')";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query1);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000001");
            assertEquals(rs.getString(2), "T1");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000002");
            assertEquals(rs.getString(2), "T2");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000003");
            assertEquals(rs.getString(2), "T3");
            assertEquals(rs.getString(3), "0000000002");
            assertEquals(rs.getString(4), "S2");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000004");
            assertEquals(rs.getString(2), "T4");
            assertEquals(rs.getString(3), "0000000002");
            assertEquals(rs.getString(4), "S2");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000005");
            assertEquals(rs.getString(2), "T5");
            assertEquals(rs.getString(3), "0000000005");
            assertEquals(rs.getString(4), "S5");

            assertFalse(rs.next());
            
            
            statement = conn.prepareStatement(query2);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000001");
            assertEquals(rs.getString(2), "T1");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000002");
            assertEquals(rs.getString(2), "T2");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000005");
            assertEquals(rs.getString(2), "T5");
            assertEquals(rs.getString(3), "0000000005");
            assertEquals(rs.getString(4), "S5");

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testLeftJoinWithPreFilters() throws Exception {
        String query = "SELECT item.item_id, item.name, supp.supplier_id, supp.name FROM " + JOIN_ITEM_TABLE + " item LEFT JOIN " + JOIN_SUPPLIER_TABLE + " supp ON item.supplier_id = supp.supplier_id AND (supp.supplier_id = '0000000001' OR supp.supplier_id = '0000000005')";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000001");
            assertEquals(rs.getString(2), "T1");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000002");
            assertEquals(rs.getString(2), "T2");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000003");
            assertEquals(rs.getString(2), "T3");
            assertNull(rs.getString(3));
            assertNull(rs.getString(4));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000004");
            assertEquals(rs.getString(2), "T4");
            assertNull(rs.getString(3));
            assertNull(rs.getString(4));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000005");
            assertEquals(rs.getString(2), "T5");
            assertEquals(rs.getString(3), "0000000005");
            assertEquals(rs.getString(4), "S5");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000006");
            assertEquals(rs.getString(2), "T6");
            assertNull(rs.getString(3));
            assertNull(rs.getString(4));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "invalid001");
            assertEquals(rs.getString(2), "INVALID-1");
            assertNull(rs.getString(3));
            assertNull(rs.getString(4));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testJoinWithPostFilters() throws Exception {
        String query1 = "SELECT item.item_id, item.name, supp.supplier_id, supp.name FROM " + JOIN_SUPPLIER_TABLE + " supp RIGHT JOIN " + JOIN_ITEM_TABLE + " item ON item.supplier_id = supp.supplier_id WHERE supp.supplier_id BETWEEN '0000000001' AND '0000000005'";
        String query2 = "SELECT item.item_id, item.name, supp.supplier_id, supp.name FROM " + JOIN_ITEM_TABLE + " item LEFT JOIN " + JOIN_SUPPLIER_TABLE + " supp ON item.supplier_id = supp.supplier_id WHERE supp.supplier_id = '0000000001' OR supp.supplier_id = '0000000005'";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query1);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000001");
            assertEquals(rs.getString(2), "T1");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000002");
            assertEquals(rs.getString(2), "T2");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000003");
            assertEquals(rs.getString(2), "T3");
            assertEquals(rs.getString(3), "0000000002");
            assertEquals(rs.getString(4), "S2");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000004");
            assertEquals(rs.getString(2), "T4");
            assertEquals(rs.getString(3), "0000000002");
            assertEquals(rs.getString(4), "S2");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000005");
            assertEquals(rs.getString(2), "T5");
            assertEquals(rs.getString(3), "0000000005");
            assertEquals(rs.getString(4), "S5");

            assertFalse(rs.next());
            
            
            statement = conn.prepareStatement(query2);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000001");
            assertEquals(rs.getString(2), "T1");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000002");
            assertEquals(rs.getString(2), "T2");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000005");
            assertEquals(rs.getString(2), "T5");
            assertEquals(rs.getString(3), "0000000005");
            assertEquals(rs.getString(4), "S5");

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testStarJoin() throws Exception {
        String query = "SELECT order_id, c.name, i.name iname, quantity, o.date FROM " + JOIN_ORDER_TABLE + " o LEFT JOIN " 
            + JOIN_CUSTOMER_TABLE + " c ON o.customer_id = c.customer_id LEFT JOIN " 
            + JOIN_ITEM_TABLE + " i ON o.item_id = i.item_id";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000001");
            assertEquals(rs.getString("order_id"), "000000000000001");
            assertEquals(rs.getString(2), "C4");
            assertEquals(rs.getString("C.name"), "C4");
            assertEquals(rs.getString(3), "T1");
            assertEquals(rs.getString("iName"), "T1");
            assertEquals(rs.getInt(4), 1000);
            assertEquals(rs.getInt("Quantity"), 1000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000002");
            assertEquals(rs.getString(2), "C3");
            assertEquals(rs.getString(3), "T6");
            assertEquals(rs.getInt(4), 2000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000003");
            assertEquals(rs.getString(2), "C2");
            assertEquals(rs.getString(3), "T2");
            assertEquals(rs.getInt(4), 3000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000004");
            assertEquals(rs.getString(2), "C4");
            assertEquals(rs.getString(3), "T6");
            assertEquals(rs.getInt(4), 4000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000005");
            assertEquals(rs.getString(2), "C5");
            assertEquals(rs.getString(3), "T3");
            assertEquals(rs.getInt(4), 5000);
            assertNotNull(rs.getDate(5));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testLeftJoinWithAggregation() throws Exception {
        String query = "SELECT i.name, sum(quantity) FROM " + JOIN_ORDER_TABLE + " o LEFT JOIN " 
            + JOIN_ITEM_TABLE + " i ON o.item_id = i.item_id GROUP BY i.name ORDER BY i.name";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T1");
            assertEquals(rs.getInt(2), 1000);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T2");
            assertEquals(rs.getInt(2), 3000);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T3");
            assertEquals(rs.getInt(2), 5000);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T6");
            assertEquals(rs.getInt(2), 6000);

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testRightJoinWithAggregation() throws Exception {
        String query = "SELECT i.name, sum(quantity) FROM " + JOIN_ORDER_TABLE + " o RIGHT JOIN " 
            + JOIN_ITEM_TABLE + " i ON o.item_id = i.item_id GROUP BY i.name ORDER BY i.name";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "INVALID-1");
            assertEquals(rs.getInt(2), 0);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T1");
            assertEquals(rs.getInt(2), 1000);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T2");
            assertEquals(rs.getInt(2), 3000);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T3");
            assertEquals(rs.getInt(2), 5000);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T4");
            assertEquals(rs.getInt(2), 0);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T5");
            assertEquals(rs.getInt(2), 0);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T6");
            assertEquals(rs.getInt(2), 6000);

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testLeftRightJoin() throws Exception {
        String query = "SELECT order_id, i.name, s.name, quantity, date FROM " + JOIN_ORDER_TABLE + " o LEFT JOIN " 
            + JOIN_ITEM_TABLE + " i ON o.item_id = i.item_id RIGHT JOIN "
            + JOIN_SUPPLIER_TABLE + " s ON i.supplier_id = s.supplier_id ORDER BY order_id, s.supplier_id DESC";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertNull(rs.getString(1));
            assertNull(rs.getString(2));
            assertEquals(rs.getString(3), "S5");
            assertEquals(rs.getInt(4), 0);
            assertNull(rs.getDate(5));
            assertTrue (rs.next());
            assertNull(rs.getString(1));
            assertNull(rs.getString(2));
            assertEquals(rs.getString(3), "S4");
            assertEquals(rs.getInt(4), 0);
            assertNull(rs.getDate(5));
            assertTrue (rs.next());
            assertNull(rs.getString(1));
            assertNull(rs.getString(2));
            assertEquals(rs.getString(3), "S3");
            assertEquals(rs.getInt(4), 0);
            assertNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000001");
            assertEquals(rs.getString(2), "T1");
            assertEquals(rs.getString(3), "S1");
            assertEquals(rs.getInt(4), 1000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000002");
            assertEquals(rs.getString(2), "T6");
            assertEquals(rs.getString(3), "S6");
            assertEquals(rs.getInt(4), 2000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000003");
            assertEquals(rs.getString(2), "T2");
            assertEquals(rs.getString(3), "S1");
            assertEquals(rs.getInt(4), 3000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000004");
            assertEquals(rs.getString(2), "T6");
            assertEquals(rs.getString(3), "S6");
            assertEquals(rs.getInt(4), 4000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000005");
            assertEquals(rs.getString(2), "T3");
            assertEquals(rs.getString(3), "S2");
            assertEquals(rs.getInt(4), 5000);
            assertNotNull(rs.getDate(5));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testMultiLeftJoin() throws Exception {
        String query = "SELECT order_id, i.name, s.name, quantity, date FROM " + JOIN_ORDER_TABLE + " o LEFT JOIN " 
            + JOIN_ITEM_TABLE + " i ON o.item_id = i.item_id LEFT JOIN "
            + JOIN_SUPPLIER_TABLE + " s ON i.supplier_id = s.supplier_id";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000001");
            assertEquals(rs.getString(2), "T1");
            assertEquals(rs.getString(3), "S1");
            assertEquals(rs.getInt(4), 1000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000002");
            assertEquals(rs.getString(2), "T6");
            assertEquals(rs.getString(3), "S6");
            assertEquals(rs.getInt(4), 2000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000003");
            assertEquals(rs.getString(2), "T2");
            assertEquals(rs.getString(3), "S1");
            assertEquals(rs.getInt(4), 3000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000004");
            assertEquals(rs.getString(2), "T6");
            assertEquals(rs.getString(3), "S6");
            assertEquals(rs.getInt(4), 4000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000005");
            assertEquals(rs.getString(2), "T3");
            assertEquals(rs.getString(3), "S2");
            assertEquals(rs.getInt(4), 5000);
            assertNotNull(rs.getDate(5));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testMultiRightJoin() throws Exception {
        String query = "SELECT order_id, i.name, s.name, quantity, date FROM " + JOIN_ORDER_TABLE + " o RIGHT JOIN " 
            + JOIN_ITEM_TABLE + " i ON o.item_id = i.item_id RIGHT JOIN "
            + JOIN_SUPPLIER_TABLE + " s ON i.supplier_id = s.supplier_id ORDER BY order_id, s.supplier_id DESC";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertNull(rs.getString(1));
            assertEquals(rs.getString(2), "T5");
            assertEquals(rs.getString(3), "S5");
            assertEquals(rs.getInt(4), 0);
            assertNull(rs.getDate(5));
            assertTrue (rs.next());
            assertNull(rs.getString(1));
            assertNull(rs.getString(2));
            assertEquals(rs.getString(3), "S4");
            assertEquals(rs.getInt(4), 0);
            assertNull(rs.getDate(5));
            assertTrue (rs.next());
            assertNull(rs.getString(1));
            assertNull(rs.getString(2));
            assertEquals(rs.getString(3), "S3");
            assertEquals(rs.getInt(4), 0);
            assertNull(rs.getDate(5));
            assertTrue (rs.next());
            assertNull(rs.getString(1));
            assertEquals(rs.getString(2), "T4");
            assertEquals(rs.getString(3), "S2");
            assertEquals(rs.getInt(4), 0);
            assertNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000001");
            assertEquals(rs.getString(2), "T1");
            assertEquals(rs.getString(3), "S1");
            assertEquals(rs.getInt(4), 1000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000002");
            assertEquals(rs.getString(2), "T6");
            assertEquals(rs.getString(3), "S6");
            assertEquals(rs.getInt(4), 2000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000003");
            assertEquals(rs.getString(2), "T2");
            assertEquals(rs.getString(3), "S1");
            assertEquals(rs.getInt(4), 3000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000004");
            assertEquals(rs.getString(2), "T6");
            assertEquals(rs.getString(3), "S6");
            assertEquals(rs.getInt(4), 4000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000005");
            assertEquals(rs.getString(2), "T3");
            assertEquals(rs.getString(3), "S2");
            assertEquals(rs.getInt(4), 5000);
            assertNotNull(rs.getDate(5));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testJoinWithWildcard() throws Exception {
        String query = "SELECT * FROM " + JOIN_ITEM_TABLE + " LEFT JOIN " + JOIN_SUPPLIER_TABLE + " supp ON " + JOIN_ITEM_TABLE + ".supplier_id = supp.supplier_id";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000001");
            assertEquals(rs.getString(JOIN_ITEM_TABLE + ".item_id"), "0000000001");
            assertEquals(rs.getString(2), "T1");
            assertEquals(rs.getString(JOIN_ITEM_TABLE + ".name"), "T1");
            assertEquals(rs.getInt(3), 100);
            assertEquals(rs.getInt(JOIN_ITEM_TABLE + ".price"), 100);
            assertEquals(rs.getInt(4), 5);
            assertEquals(rs.getInt(JOIN_ITEM_TABLE + ".discount1"), 5);
            assertEquals(rs.getInt(5), 10);
            assertEquals(rs.getInt(JOIN_ITEM_TABLE + ".discount2"), 10);
            assertEquals(rs.getString(6), "0000000001");
            assertEquals(rs.getString(JOIN_ITEM_TABLE + ".supplier_id"), "0000000001");
            assertEquals(rs.getString(7), "Item T1");
            assertEquals(rs.getString(JOIN_ITEM_TABLE + ".description"), "Item T1");
            assertEquals(rs.getString(8), "0000000001");
            assertEquals(rs.getString("supp.supplier_id"), "0000000001");
            assertEquals(rs.getString(9), "S1");
            assertEquals(rs.getString("supp.name"), "S1");
            assertEquals(rs.getString(10), "888-888-1111");
            assertEquals(rs.getString("supp.phone"), "888-888-1111");
            assertEquals(rs.getString(11), "101 YYY Street");
            assertEquals(rs.getString("supp.address"), "101 YYY Street");
            assertEquals(rs.getString(12), "10001");            
            assertEquals(rs.getString("supp.loc_id"), "10001");            
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000002");
            assertEquals(rs.getString(2), "T2");
            assertEquals(rs.getInt(3), 200);
            assertEquals(rs.getInt(4), 5);
            assertEquals(rs.getInt(5), 8);
            assertEquals(rs.getString(6), "0000000001");
            assertEquals(rs.getString(7), "Item T2");
            assertEquals(rs.getString(8), "0000000001");
            assertEquals(rs.getString(9), "S1");
            assertEquals(rs.getString(10), "888-888-1111");
            assertEquals(rs.getString(11), "101 YYY Street");
            assertEquals(rs.getString(12), "10001");            
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000003");
            assertEquals(rs.getString(2), "T3");
            assertEquals(rs.getInt(3), 300);
            assertEquals(rs.getInt(4), 8);
            assertEquals(rs.getInt(5), 12);
            assertEquals(rs.getString(6), "0000000002");
            assertEquals(rs.getString(7), "Item T3");
            assertEquals(rs.getString(8), "0000000002");
            assertEquals(rs.getString(9), "S2");
            assertEquals(rs.getString(10), "888-888-2222");
            assertEquals(rs.getString(11), "202 YYY Street");
            assertEquals(rs.getString(12), "10002");            
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000004");
            assertEquals(rs.getString(2), "T4");
            assertEquals(rs.getInt(3), 400);
            assertEquals(rs.getInt(4), 6);
            assertEquals(rs.getInt(5), 10);
            assertEquals(rs.getString(6), "0000000002");
            assertEquals(rs.getString(7), "Item T4");
            assertEquals(rs.getString(8), "0000000002");
            assertEquals(rs.getString(9), "S2");
            assertEquals(rs.getString(10), "888-888-2222");
            assertEquals(rs.getString(11), "202 YYY Street");
            assertEquals(rs.getString(12), "10002");            
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000005");
            assertEquals(rs.getString(2), "T5");
            assertEquals(rs.getInt(3), 500);
            assertEquals(rs.getInt(4), 8);
            assertEquals(rs.getInt(5), 15);
            assertEquals(rs.getString(6), "0000000005");
            assertEquals(rs.getString(7), "Item T5");
            assertEquals(rs.getString(8), "0000000005");
            assertEquals(rs.getString(9), "S5");
            assertEquals(rs.getString(10), "888-888-5555");
            assertEquals(rs.getString(11), "505 YYY Street");
            assertEquals(rs.getString(12), "10005");            
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000006");
            assertEquals(rs.getString(2), "T6");
            assertEquals(rs.getInt(3), 600);
            assertEquals(rs.getInt(4), 8);
            assertEquals(rs.getInt(5), 15);
            assertEquals(rs.getString(6), "0000000006");
            assertEquals(rs.getString(7), "Item T6");
            assertEquals(rs.getString(8), "0000000006");
            assertEquals(rs.getString(9), "S6");
            assertEquals(rs.getString(10), "888-888-6666");
            assertEquals(rs.getString(11), "606 YYY Street");
            assertEquals(rs.getString(12), "10006");            
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "invalid001");
            assertEquals(rs.getString(2), "INVALID-1");
            assertEquals(rs.getInt(3), 0);
            assertEquals(rs.getInt(4), 0);
            assertEquals(rs.getInt(5), 0);
            assertEquals(rs.getString(6), "0000000000");
            assertEquals(rs.getString(7), "Invalid item for join test");
            assertNull(rs.getString(8));
            assertNull(rs.getString(9));
            assertNull(rs.getString(10));
            assertNull(rs.getString(11));
            assertNull(rs.getString(12));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testJoinMultiJoinKeys() throws Exception {
        String query = "SELECT c.name, s.name FROM " + JOIN_CUSTOMER_TABLE + " c LEFT JOIN " + JOIN_SUPPLIER_TABLE + " s ON customer_id = supplier_id AND c.loc_id = s.loc_id AND substr(s.name, 2, 1) = substr(c.name, 2, 1)";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "C1");
            assertEquals(rs.getString(2), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "C2");
            assertNull(rs.getString(2));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "C3");
            assertEquals(rs.getString(2), "S3");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "C4");
            assertNull(rs.getString(2));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "C5");
            assertEquals(rs.getString(2), "S5");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "C6");
            assertNull(rs.getString(2));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testJoinWithDifferentNumericJoinKeyTypes() throws Exception {
        String query = "SELECT order_id, i.name, i.price, discount2, quantity FROM " + JOIN_ORDER_TABLE + " o INNER JOIN " 
            + JOIN_ITEM_TABLE + " i ON o.item_id = i.item_id AND o.price = (i.price * (100 - discount2)) / 100.0 WHERE quantity < 5000";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000004");
            assertEquals(rs.getString(2), "T6");
            assertEquals(rs.getInt(3), 600);
            assertEquals(rs.getInt(4), 15);
            assertEquals(rs.getInt(5), 4000);

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testJoinWithDifferentDateJoinKeyTypes() throws Exception {
        String query = "SELECT order_id, c.name, o.date FROM " + JOIN_ORDER_TABLE + " o INNER JOIN " 
            + JOIN_CUSTOMER_TABLE + " c ON o.customer_id = c.customer_id AND o.date = c.date";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000001");
            assertEquals(rs.getString(2), "C4");
            assertEquals(rs.getTimestamp(3), new Timestamp(format.parse("2013-11-22 14:22:56").getTime()));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000002");
            assertEquals(rs.getString(2), "C3");
            assertEquals(rs.getTimestamp(3), new Timestamp(format.parse("2013-11-25 10:06:29").getTime()));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000003");
            assertEquals(rs.getString(2), "C2");
            assertEquals(rs.getTimestamp(3), new Timestamp(format.parse("2013-11-25 16:45:07").getTime()));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000005");
            assertEquals(rs.getString(2), "C5");
            assertEquals(rs.getTimestamp(3), new Timestamp(format.parse("2013-11-27 09:37:50").getTime()));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testJoinWithIncomparableJoinKeyTypes() throws Exception {
        String query = "SELECT order_id, i.name, i.price, discount2, quantity FROM " + JOIN_ORDER_TABLE + " o INNER JOIN " 
            + JOIN_ITEM_TABLE + " i ON o.item_id = i.item_id AND o.price / 100 = substr(i.name, 2, 1)";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.executeQuery();
            fail("Should have got SQLException.");
        } catch (SQLException e) {
            assertEquals(e.getErrorCode(), SQLExceptionCode.CANNOT_CONVERT_TYPE.getErrorCode());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testJoinPlanWithIndex() throws Exception {
        String query1 = "SELECT item.item_id, item.name, supp.supplier_id, supp.name FROM " + JOIN_ITEM_TABLE + " item LEFT JOIN " + JOIN_SUPPLIER_TABLE + " supp ON substr(item.name, 2, 1) = substr(supp.name, 2, 1) AND (supp.name BETWEEN 'S1' AND 'S5') WHERE item.name BETWEEN 'T1' AND 'T5'";
        String query2 = "SELECT item.item_id, item.name, supp.supplier_id, supp.name FROM " + JOIN_ITEM_TABLE + " item INNER JOIN " + JOIN_SUPPLIER_TABLE + " supp ON item.supplier_id = supp.supplier_id WHERE (item.name = 'T1' OR item.name = 'T5') AND (supp.name = 'S1' OR supp.name = 'S5')";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query1);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000001");
            assertEquals(rs.getString(2), "T1");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000002");
            assertEquals(rs.getString(2), "T2");
            assertEquals(rs.getString(3), "0000000002");
            assertEquals(rs.getString(4), "S2");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000003");
            assertEquals(rs.getString(2), "T3");
            assertEquals(rs.getString(3), "0000000003");
            assertEquals(rs.getString(4), "S3");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000004");
            assertEquals(rs.getString(2), "T4");
            assertEquals(rs.getString(3), "0000000004");
            assertEquals(rs.getString(4), "S4");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000005");
            assertEquals(rs.getString(2), "T5");
            assertEquals(rs.getString(3), "0000000005");
            assertEquals(rs.getString(4), "S5");

            assertFalse(rs.next());
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query1);
            assertEquals(plans[0], QueryUtil.getExplainPlan(rs));
            
            
            statement = conn.prepareStatement(query2);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000001");
            assertEquals(rs.getString(2), "T1");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000005");
            assertEquals(rs.getString(2), "T5");
            assertEquals(rs.getString(3), "0000000005");
            assertEquals(rs.getString(4), "S5");

            assertFalse(rs.next());
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query2);
            assertEquals(plans[1], QueryUtil.getExplainPlan(rs));
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testJoinWithSkipMergeOptimization() throws Exception {
        String query = "SELECT s.name FROM " + JOIN_ITEM_TABLE + " i JOIN " 
            + JOIN_ORDER_TABLE + " o ON o.item_id = i.item_id AND quantity < 5000 JOIN "
            + JOIN_SUPPLIER_TABLE + " s ON i.supplier_id = s.supplier_id";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "S6");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "S6");
            
            assertFalse(rs.next());
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            assertEquals(plans[2], QueryUtil.getExplainPlan(rs));
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSelfJoin() throws Exception {
        String query1 = "SELECT i2.item_id, i1.name FROM " + JOIN_ITEM_TABLE + " i1 JOIN " 
            + JOIN_ITEM_TABLE + " i2 ON i1.item_id = i2.item_id ORDER BY i1.item_id";
        String query2 = "SELECT i1.name, i2.name FROM " + JOIN_ITEM_TABLE + " i1 JOIN " 
        + JOIN_ITEM_TABLE + " i2 ON i1.item_id = i2.supplier_id ORDER BY i1.name, i2.name";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query1);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000001");
            assertEquals(rs.getString(2), "T1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000002");
            assertEquals(rs.getString(2), "T2");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000003");
            assertEquals(rs.getString(2), "T3");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000004");
            assertEquals(rs.getString(2), "T4");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000005");
            assertEquals(rs.getString(2), "T5");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000006");
            assertEquals(rs.getString(2), "T6");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "invalid001");
            assertEquals(rs.getString(2), "INVALID-1");
            
            assertFalse(rs.next());
            
            statement = conn.prepareStatement(query2);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T1");
            assertEquals(rs.getString(2), "T1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T1");
            assertEquals(rs.getString(2), "T2");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T2");
            assertEquals(rs.getString(2), "T3");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T2");
            assertEquals(rs.getString(2), "T4");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T5");
            assertEquals(rs.getString(2), "T5");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T6");
            assertEquals(rs.getString(2), "T6");
            
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testUpsertWithJoin() throws Exception {
        String tempTable = "TEMP_JOINED_TABLE";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        conn.setAutoCommit(true);
        try {
            conn.createStatement().execute("CREATE TABLE " + tempTable 
                    + "   (order_id varchar not null, " 
                    + "    item_name varchar not null, " 
                    + "    supplier_name varchar, "
                    + "    quantity integer, "
                    + "    date timestamp " 
                    + "    CONSTRAINT pk PRIMARY KEY (order_id, item_name))");
            conn.createStatement().execute("UPSERT INTO " + tempTable 
                    + "(order_id, item_name, supplier_name, quantity, date) " 
                    + "SELECT order_id, i.name, s.name, quantity, date FROM " 
                    + JOIN_ORDER_TABLE + " o LEFT JOIN " 
                    + JOIN_ITEM_TABLE + " i ON o.item_id = i.item_id LEFT JOIN "
                    + JOIN_SUPPLIER_TABLE + " s ON i.supplier_id = s.supplier_id");
            conn.createStatement().execute("UPSERT INTO " + tempTable 
                    + "(order_id, item_name, quantity) " 
                    + "SELECT 'ORDER_SUM', i.name, sum(quantity) FROM " 
                    + JOIN_ORDER_TABLE + " o LEFT JOIN " 
                    + JOIN_ITEM_TABLE + " i ON o.item_id = i.item_id " 
                    + "GROUP BY i.name ORDER BY i.name");
            
            String query = "SELECT * FROM " + tempTable;
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000001");
            assertEquals(rs.getString(2), "T1");
            assertEquals(rs.getString(3), "S1");
            assertEquals(rs.getInt(4), 1000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000002");
            assertEquals(rs.getString(2), "T6");
            assertEquals(rs.getString(3), "S6");
            assertEquals(rs.getInt(4), 2000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000003");
            assertEquals(rs.getString(2), "T2");
            assertEquals(rs.getString(3), "S1");
            assertEquals(rs.getInt(4), 3000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000004");
            assertEquals(rs.getString(2), "T6");
            assertEquals(rs.getString(3), "S6");
            assertEquals(rs.getInt(4), 4000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000005");
            assertEquals(rs.getString(2), "T3");
            assertEquals(rs.getString(3), "S2");
            assertEquals(rs.getInt(4), 5000);
            assertNotNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "ORDER_SUM");
            assertEquals(rs.getString(2), "T1");
            assertNull(rs.getString(3));
            assertEquals(rs.getInt(4), 1000);
            assertNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "ORDER_SUM");
            assertEquals(rs.getString(2), "T2");
            assertNull(rs.getString(3));
            assertEquals(rs.getInt(4), 3000);
            assertNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "ORDER_SUM");
            assertEquals(rs.getString(2), "T3");
            assertNull(rs.getString(3));
            assertEquals(rs.getInt(4), 5000);
            assertNull(rs.getDate(5));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "ORDER_SUM");
            assertEquals(rs.getString(2), "T6");
            assertNull(rs.getString(3));
            assertEquals(rs.getInt(4), 6000);
            assertNull(rs.getDate(5));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testJoinOverSaltedTables() throws Exception {
        String tempTableNoSalting = "TEMP_TABLE_NO_SALTING";
        String tempTableWithSalting = "TEMP_TABLE_WITH_SALTING";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            conn.createStatement().execute("CREATE TABLE " + tempTableNoSalting 
                    + "   (mypk INTEGER NOT NULL PRIMARY KEY, " 
                    + "    col1 INTEGER)");
            conn.createStatement().execute("CREATE TABLE " + tempTableWithSalting 
                    + "   (mypk INTEGER NOT NULL PRIMARY KEY, " 
                    + "    col1 INTEGER) SALT_BUCKETS=4");
            
            PreparedStatement upsertStmt = conn.prepareStatement(
                    "upsert into " + tempTableNoSalting + "(mypk, col1) " + "values (?, ?)");
            for (int i = 0; i < 3; i++) {
                upsertStmt.setInt(1, i + 1);
                upsertStmt.setInt(2, 3 - i);
                upsertStmt.execute();
            }
            conn.commit();
            
            upsertStmt = conn.prepareStatement(
                    "upsert into " + tempTableWithSalting + "(mypk, col1) " + "values (?, ?)");
            for (int i = 0; i < 6; i++) {
                upsertStmt.setInt(1, i + 1);
                upsertStmt.setInt(2, 3 - (i % 3));
                upsertStmt.execute();
            }
            conn.commit();
            
            // LHS=unsalted JOIN RHS=salted
            String query = "SELECT lhs.mypk, lhs.col1, rhs.mypk, rhs.col1 FROM " 
                    + tempTableNoSalting + " lhs JOIN "
                    + tempTableWithSalting + " rhs ON rhs.mypk = lhs.col1";
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 1);
            assertEquals(rs.getInt(2), 3);
            assertEquals(rs.getInt(3), 3);
            assertEquals(rs.getInt(4), 1);
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 2);
            assertEquals(rs.getInt(2), 2);
            assertEquals(rs.getInt(3), 2);
            assertEquals(rs.getInt(4), 2);
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 3);
            assertEquals(rs.getInt(2), 1);
            assertEquals(rs.getInt(3), 1);
            assertEquals(rs.getInt(4), 3);

            assertFalse(rs.next());
            
            // LHS=salted JOIN RHS=salted
            query = "SELECT lhs.mypk, lhs.col1, rhs.mypk, rhs.col1 FROM " 
                    + tempTableWithSalting + " lhs JOIN "
                    + tempTableNoSalting + " rhs ON rhs.mypk = lhs.col1";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 1);
            assertEquals(rs.getInt(2), 3);
            assertEquals(rs.getInt(3), 3);
            assertEquals(rs.getInt(4), 1);
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 2);
            assertEquals(rs.getInt(2), 2);
            assertEquals(rs.getInt(3), 2);
            assertEquals(rs.getInt(4), 2);
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 3);
            assertEquals(rs.getInt(2), 1);
            assertEquals(rs.getInt(3), 1);
            assertEquals(rs.getInt(4), 3);
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 4);
            assertEquals(rs.getInt(2), 3);
            assertEquals(rs.getInt(3), 3);
            assertEquals(rs.getInt(4), 1);
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 5);
            assertEquals(rs.getInt(2), 2);
            assertEquals(rs.getInt(3), 2);
            assertEquals(rs.getInt(4), 2);
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 6);
            assertEquals(rs.getInt(2), 1);
            assertEquals(rs.getInt(3), 1);
            assertEquals(rs.getInt(4), 3);

            assertFalse(rs.next());
            
            // LHS=salted JOIN RHS=salted
            query = "SELECT lhs.mypk, lhs.col1, rhs.mypk, rhs.col1 FROM " 
                    + tempTableWithSalting + " lhs JOIN "
                    + tempTableWithSalting + " rhs ON rhs.mypk = (lhs.col1 + 3)";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 1);
            assertEquals(rs.getInt(2), 3);
            assertEquals(rs.getInt(3), 6);
            assertEquals(rs.getInt(4), 1);
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 2);
            assertEquals(rs.getInt(2), 2);
            assertEquals(rs.getInt(3), 5);
            assertEquals(rs.getInt(4), 2);
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 3);
            assertEquals(rs.getInt(2), 1);
            assertEquals(rs.getInt(3), 4);
            assertEquals(rs.getInt(4), 3);
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 4);
            assertEquals(rs.getInt(2), 3);
            assertEquals(rs.getInt(3), 6);
            assertEquals(rs.getInt(4), 1);
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 5);
            assertEquals(rs.getInt(2), 2);
            assertEquals(rs.getInt(3), 5);
            assertEquals(rs.getInt(4), 2);
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 6);
            assertEquals(rs.getInt(2), 1);
            assertEquals(rs.getInt(3), 4);
            assertEquals(rs.getInt(4), 3);

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
}
