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
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;

@RunWith(Parameterized.class)
public class SubqueryIT extends BaseJoinIT {
    public SubqueryIT(String[] indexDDL, String[] plans) {
        super(indexDDL, plans);
    }
    
    @Parameters
    public static Collection<Object> data() {
        List<Object> testCases = Lists.newArrayList();
        testCases.add(new String[][] {
                {}, {
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "    SERVER SORTED BY \\[I.NAME\\]\n" +
                "CLIENT MERGE SORT\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SUPPLIER_TABLE_FULL_NAME + "\n" +
                "    SKIP-SCAN-JOIN TABLE 1\n" +
                "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + " \\['000000000000001'\\] - \\[\\*\\]\n" +
                "            SERVER AGGREGATE INTO DISTINCT ROWS BY \\[\"item_id\"\\]\n" +
                "        CLIENT MERGE SORT\n" +
                "    DYNAMIC SERVER FILTER BY \"I.item_id\" IN \\(\\$\\d+.\\$\\d+\\)",
                
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SUPPLIER_TABLE_FULL_NAME + "\n" +
                "    SERVER SORTED BY [I.NAME]\n" +
                "CLIENT MERGE SORT\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "    PARALLEL SEMI-JOIN TABLE 1(DELAYED EVALUATION) (SKIP MERGE)\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "            SERVER AGGREGATE INTO DISTINCT ROWS BY [\"item_id\"]\n" +
                "        CLIENT MERGE SORT",
                
                "CLIENT PARALLEL 4-WAY FULL SCAN OVER " + JOIN_COITEM_TABLE_FULL_NAME + "\n" +
                "CLIENT MERGE SORT\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "            SERVER AGGREGATE INTO DISTINCT ROWS BY \\[\".+.item_id\", .+.NAME\\]\n" +
                "        CLIENT MERGE SORT\n" +
                "            PARALLEL ANTI-JOIN TABLE 0 \\(SKIP MERGE\\)\n" +
                "                CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "                    SERVER AGGREGATE INTO DISTINCT ROWS BY \\[\"item_id\"\\]\n" +
                "                CLIENT MERGE SORT\n" +
                "    PARALLEL LEFT-JOIN TABLE 1\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "            SERVER AGGREGATE INTO DISTINCT ROWS BY \\[\".+.item_id\", .+.NAME\\]\n" +
                "        CLIENT MERGE SORT\n" +
                "            SKIP-SCAN-JOIN TABLE 0\n" +
                "                CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "                    SERVER AGGREGATE INTO DISTINCT ROWS BY \\[\"item_id\"\\]\n" +
                "                CLIENT MERGE SORT\n" +
                "            DYNAMIC SERVER FILTER BY \"" + JOIN_ITEM_TABLE_FULL_NAME + ".item_id\" IN \\(\\$\\d+.\\$\\d+\\)\n" +
                "    AFTER-JOIN SERVER FILTER BY \\(\\$\\d+.\\$\\d+ IS NOT NULL OR \\$\\d+.\\$\\d+ IS NOT NULL\\)",
                
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "    SERVER SORTED BY [I.NAME]\n" +
                "CLIENT MERGE SORT\n" +
                "    PARALLEL ANTI-JOIN TABLE 0 (SKIP MERGE)\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "            SERVER AGGREGATE INTO DISTINCT ROWS BY [\"item_id\"]\n" +
                "        CLIENT MERGE SORT",
                
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_CUSTOMER_TABLE_FULL_NAME + "\n" +
                "    SKIP-SCAN-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "            SERVER AGGREGATE INTO DISTINCT ROWS BY \\[\"O.customer_id\"\\]\n" +
                "        CLIENT MERGE SORT\n" +
                "            PARALLEL INNER-JOIN TABLE 0\n" +
                "                CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "            PARALLEL LEFT-JOIN TABLE 1\\(DELAYED EVALUATION\\)\n" +
                "                CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "                    SERVER AGGREGATE INTO DISTINCT ROWS BY \\[\"item_id\"\\]\n" +
                "                CLIENT MERGE SORT\n" +
                "            DYNAMIC SERVER FILTER BY \"I.item_id\" IN \\(\"O.item_id\"\\)\n" +
                "            AFTER-JOIN SERVER FILTER BY \\(I.NAME = 'T2' OR O.QUANTITY > \\$\\d+.\\$\\d+\\)\n" +
                "    DYNAMIC SERVER FILTER BY \"" + JOIN_CUSTOMER_TABLE_FULL_NAME + ".customer_id\" IN \\(\\$\\d+.\\$\\d+\\)"
                }});
        testCases.add(new String[][] {
                {
                "CREATE INDEX \"idx_customer\" ON " + JOIN_CUSTOMER_TABLE_FULL_NAME + " (name)",
                "CREATE INDEX \"idx_item\" ON " + JOIN_ITEM_TABLE_FULL_NAME + " (name) INCLUDE (price, discount1, discount2, \"supplier_id\", description)",
                "CREATE INDEX \"idx_supplier\" ON " + JOIN_SUPPLIER_TABLE_FULL_NAME + " (name)"
                }, {
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SCHEMA + ".idx_item\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SCHEMA + ".idx_supplier\n" +
                "            SERVER FILTER BY FIRST KEY ONLY\n" + 
                "    PARALLEL SEMI-JOIN TABLE 1 \\(SKIP MERGE\\)\n" +
                "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + " \\['000000000000001'\\] - \\[\\*\\]\n" +
                "            SERVER AGGREGATE INTO DISTINCT ROWS BY \\[\"item_id\"\\]\n" +
                "        CLIENT MERGE SORT",
                
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SCHEMA + ".idx_supplier\n" +
                "    SERVER FILTER BY FIRST KEY ONLY\n" + 
                "    SERVER SORTED BY [\"I.0:NAME\"]\n" +
                "CLIENT MERGE SORT\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SCHEMA + ".idx_item\n" +
                "    PARALLEL SEMI-JOIN TABLE 1(DELAYED EVALUATION) (SKIP MERGE)\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "            SERVER AGGREGATE INTO DISTINCT ROWS BY [\"item_id\"]\n" +
                "        CLIENT MERGE SORT",
                
                "CLIENT PARALLEL 4-WAY FULL SCAN OVER " + JOIN_COITEM_TABLE_FULL_NAME + "\n" +
                "CLIENT MERGE SORT\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SCHEMA + ".idx_item\n" +
                "            SERVER FILTER BY FIRST KEY ONLY\n" +
                "            SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY \\[\".+.:item_id\", \".+.0:NAME\"\\]\n" +
                "            PARALLEL ANTI-JOIN TABLE 0 \\(SKIP MERGE\\)\n" +
                "                CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "                    SERVER AGGREGATE INTO DISTINCT ROWS BY \\[\"item_id\"\\]\n" +
                "                CLIENT MERGE SORT\n" +
                "    PARALLEL LEFT-JOIN TABLE 1\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SCHEMA + ".idx_item\n" +
                "            SERVER FILTER BY FIRST KEY ONLY\n" +
                "            SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY \\[\".+.:item_id\", \".+.0:NAME\"\\]\n" +
                "            PARALLEL SEMI-JOIN TABLE 0 \\(SKIP MERGE\\)\n" +
                "                CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "                    SERVER AGGREGATE INTO DISTINCT ROWS BY \\[\"item_id\"\\]\n" +
                "                CLIENT MERGE SORT\n" +
                "    AFTER-JOIN SERVER FILTER BY \\(\\$\\d+.\\$\\d+ IS NOT NULL OR \\$\\d+.\\$\\d+ IS NOT NULL\\)",
                
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SCHEMA + ".idx_item\n" +
                "    SERVER FILTER BY FIRST KEY ONLY\n" +
                "    PARALLEL ANTI-JOIN TABLE 0 (SKIP MERGE)\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "            SERVER AGGREGATE INTO DISTINCT ROWS BY [\"item_id\"]\n" +
                "        CLIENT MERGE SORT",
                
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SCHEMA + ".idx_customer\n" +
                "    SERVER FILTER BY FIRST KEY ONLY\n" + 
                "    PARALLEL SEMI-JOIN TABLE 0 \\(SKIP MERGE\\)\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SCHEMA + ".idx_item\n" +
                "            SERVER FILTER BY FIRST KEY ONLY\n" +
                "            SERVER AGGREGATE INTO DISTINCT ROWS BY \\[\"O.customer_id\"\\]\n" +
                "        CLIENT MERGE SORT\n" +
                "            PARALLEL INNER-JOIN TABLE 0\n" +
                "                CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "            PARALLEL LEFT-JOIN TABLE 1\\(DELAYED EVALUATION\\)\n" +
                "                CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "                    SERVER AGGREGATE INTO DISTINCT ROWS BY \\[\"item_id\"\\]\n" +
                "                CLIENT MERGE SORT\n" +
                "            AFTER-JOIN SERVER FILTER BY \\(\"I.0:NAME\" = 'T2' OR O.QUANTITY > \\$\\d+.\\$\\d+\\)"
                }});
        testCases.add(new String[][] {
                {
                "CREATE LOCAL INDEX \"idx_customer\" ON " + JOIN_CUSTOMER_TABLE_FULL_NAME + " (name)",
                "CREATE LOCAL INDEX \"idx_item\" ON " + JOIN_ITEM_TABLE_FULL_NAME + " (name) INCLUDE (price, discount1, discount2, \"supplier_id\", description)",
                "CREATE LOCAL INDEX \"idx_supplier\" ON " + JOIN_SUPPLIER_TABLE_FULL_NAME + " (name)"
                }, {
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + " \\[1\\]\n" +
                "CLIENT MERGE SORT\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_SUPPLIER_TABLE_FULL_NAME + " \\[1\\]\n" +
                "            SERVER FILTER BY FIRST KEY ONLY\n" + 
                "        CLIENT MERGE SORT\n" +
                "    PARALLEL SEMI-JOIN TABLE 1 \\(SKIP MERGE\\)\n" +
                "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + " \\['000000000000001'\\] - \\[\\*\\]\n" +
                "            SERVER AGGREGATE INTO DISTINCT ROWS BY \\[\"item_id\"\\]\n" +
                "        CLIENT MERGE SORT\n" +
                "    DYNAMIC SERVER FILTER BY \"I.:item_id\" IN \\(\\$\\d+.\\$\\d+\\)",
                            
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_SUPPLIER_TABLE_FULL_NAME + " [1]\n" +
                "    SERVER FILTER BY FIRST KEY ONLY\n" + 
                "    SERVER SORTED BY [\"I.0:NAME\"]\n" +
                "CLIENT MERGE SORT\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + " [1]\n" +
                "        CLIENT MERGE SORT\n" +
                "    PARALLEL SEMI-JOIN TABLE 1(DELAYED EVALUATION) (SKIP MERGE)\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "            SERVER AGGREGATE INTO DISTINCT ROWS BY [\"item_id\"]\n" +
                "        CLIENT MERGE SORT",

                "CLIENT PARALLEL 4-WAY FULL SCAN OVER " + JOIN_COITEM_TABLE_FULL_NAME + "\n" +
                "CLIENT MERGE SORT\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + " \\[1\\]\n" +
                "            SERVER FILTER BY FIRST KEY ONLY\n" +
                "            SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY \\[\".+.:item_id\", \".+.0:NAME\"\\]\n" +
                "        CLIENT MERGE SORT\n" + 
                "            PARALLEL ANTI-JOIN TABLE 0 \\(SKIP MERGE\\)\n" +
                "                CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "                    SERVER AGGREGATE INTO DISTINCT ROWS BY \\[\"item_id\"\\]\n" +
                "                CLIENT MERGE SORT\n" +
                "    PARALLEL LEFT-JOIN TABLE 1\n" +
                "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + " \\[1\\]\n" +
                "            SERVER FILTER BY FIRST KEY ONLY\n" +
                "            SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY \\[\".+.:item_id\", \".+.0:NAME\"\\]\n" +
                "        CLIENT MERGE SORT\n" + 
                "            PARALLEL SEMI-JOIN TABLE 0 \\(SKIP MERGE\\)\n" +
                "                CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "                    SERVER AGGREGATE INTO DISTINCT ROWS BY \\[\"item_id\"\\]\n" +
                "                CLIENT MERGE SORT\n" +
                "            DYNAMIC SERVER FILTER BY \"" + JOIN_SCHEMA + ".idx_item.:item_id\" IN \\(\\$\\d+.\\$\\d+\\)\n" +
                "    AFTER-JOIN SERVER FILTER BY \\(\\$\\d+.\\$\\d+ IS NOT NULL OR \\$\\d+.\\$\\d+ IS NOT NULL\\)",
                
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + " [1]\n" +
                "    SERVER FILTER BY FIRST KEY ONLY\n" +
                "CLIENT MERGE SORT\n" +
                "    PARALLEL ANTI-JOIN TABLE 0 (SKIP MERGE)\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "            SERVER AGGREGATE INTO DISTINCT ROWS BY [\"item_id\"]\n" +
                "        CLIENT MERGE SORT",
                
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_CUSTOMER_TABLE_FULL_NAME + " \\[1\\]\n" +
                "    SERVER FILTER BY FIRST KEY ONLY\n" + 
                "CLIENT MERGE SORT\n" +
                "    PARALLEL SEMI-JOIN TABLE 0 \\(SKIP MERGE\\)\n" +
                "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + " \\[1\\]\n" +
                "            SERVER FILTER BY FIRST KEY ONLY\n" +
                "            SERVER AGGREGATE INTO DISTINCT ROWS BY \\[\"O.customer_id\"\\]\n" +
                "        CLIENT MERGE SORT\n" +
                "            PARALLEL INNER-JOIN TABLE 0\n" +
                "                CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "            PARALLEL LEFT-JOIN TABLE 1\\(DELAYED EVALUATION\\)\n" +
                "                CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "                    SERVER AGGREGATE INTO DISTINCT ROWS BY \\[\"item_id\"\\]\n" +
                "                CLIENT MERGE SORT\n" +
                "            DYNAMIC SERVER FILTER BY \"I.:item_id\" IN \\(\"O.item_id\"\\)\n" +
                "            AFTER-JOIN SERVER FILTER BY \\(\"I.0:NAME\" = 'T2' OR O.QUANTITY > \\$\\d+.\\$\\d+\\)\n" +
                "    DYNAMIC SERVER FILTER BY \"" + JOIN_SCHEMA + ".idx_customer.:customer_id\" IN \\(\\$\\d+.\\$\\d+\\)"
                }});
        return testCases;
    }
    
    
    @Test
    public void testNonCorrelatedSubquery() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName1 = getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME);
        String tableName4 = getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME);
        String tableName5 = getTableName(conn, JOIN_COITEM_TABLE_FULL_NAME);
        try {            
            String query = "SELECT \"item_id\", name FROM " + tableName1 + " WHERE \"item_id\" >= ALL (SELECT \"item_id\" FROM " + tableName4 + ") ORDER BY name";
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "invalid001");
            assertEquals(rs.getString(2), "INVALID-1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000006");
            assertEquals(rs.getString(2), "T6");

            assertFalse(rs.next());
            
            query = "SELECT \"item_id\", name FROM " + tableName1 + " WHERE \"item_id\" < ANY (SELECT \"item_id\" FROM " + tableName4 + ")";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
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

            assertFalse(rs.next());
            
            query = "SELECT \"item_id\", name FROM " + tableName1 + " WHERE \"item_id\" < (SELECT max(\"item_id\") FROM " + tableName4 + ")";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
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

            assertFalse(rs.next());
            
            query = "SELECT * FROM " + tableName5 + " WHERE (item_id, item_name) != ALL (SELECT \"item_id\", name FROM " + tableName1 + ")";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000003");
            assertEquals(rs.getString(2), "T4");
            assertEquals(rs.getString(3), "0000000005");
            assertEquals(rs.getString(4), "T5");

            assertFalse(rs.next());
            
            query = "SELECT * FROM " + tableName5 + " WHERE EXISTS (SELECT \"item_id\", name FROM " + tableName1 + ")";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000001");
            assertEquals(rs.getString(2), "T1");
            assertEquals(rs.getString(3), "0000000002");
            assertEquals(rs.getString(4), "T3");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000003");
            assertEquals(rs.getString(2), "T4");
            assertEquals(rs.getString(3), "0000000005");
            assertEquals(rs.getString(4), "T5");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000004");
            assertEquals(rs.getString(2), "T4");
            assertEquals(rs.getString(3), "0000000003");
            assertEquals(rs.getString(4), "T3");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000006");
            assertEquals(rs.getString(2), "T6");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "T1");

            assertFalse(rs.next());
            
            query = "SELECT \"item_id\", name FROM " + tableName1 + " WHERE \"item_id\" < (SELECT \"item_id\" FROM " + tableName4 + ")";
            statement = conn.prepareStatement(query);
            try {
                rs = statement.executeQuery();
                fail("Should have got Exception.");
            } catch (SQLException e) {
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testInSubquery() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName1 = getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME);
        String tableName2 = getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME);
        String tableName4 = getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME);
        String tableName5 = getTableName(conn, JOIN_COITEM_TABLE_FULL_NAME);
        try {
            String query = "SELECT \"item_id\", name FROM " + tableName1 + " WHERE \"item_id\" IN (SELECT \"item_id\" FROM " + tableName4 + ") ORDER BY name";
            PreparedStatement statement = conn.prepareStatement(query);
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
            assertEquals(rs.getString(1), "0000000006");
            assertEquals(rs.getString(2), "T6");

            assertFalse(rs.next());

            query = "SELECT \"item_id\", name FROM " + tableName1 + " WHERE \"item_id\" NOT IN (SELECT \"item_id\" FROM " + tableName4 + ") ORDER BY name";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "invalid001");
            assertEquals(rs.getString(2), "INVALID-1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000004");
            assertEquals(rs.getString(2), "T4");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000005");
            assertEquals(rs.getString(2), "T5");

            assertFalse(rs.next());
            
            query = "SELECT i.\"item_id\", s.name FROM " + tableName1 + " i JOIN " + tableName2 + " s ON i.\"supplier_id\" = s.\"supplier_id\" WHERE i.\"item_id\" IN (SELECT \"item_id\" FROM " + tableName4 + " WHERE \"order_id\" > '000000000000001') ORDER BY i.name";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000002");
            assertEquals(rs.getString(2), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000003");
            assertEquals(rs.getString(2), "S2");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000006");
            assertEquals(rs.getString(2), "S6");

            assertFalse(rs.next());
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            String plan = QueryUtil.getExplainPlan(rs);
            assertPlansMatch(plans[0], plan);
            
            query = "SELECT i.\"item_id\", s.name FROM " + tableName2 + " s LEFT JOIN " + tableName1 + " i ON i.\"supplier_id\" = s.\"supplier_id\" WHERE i.\"item_id\" IN (SELECT \"item_id\" FROM " + tableName4 + ") ORDER BY i.name";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000001");
            assertEquals(rs.getString(2), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000002");
            assertEquals(rs.getString(2), "S1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000003");
            assertEquals(rs.getString(2), "S2");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000006");
            assertEquals(rs.getString(2), "S6");

            assertFalse(rs.next());
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            assertPlansEqual(plans[1], QueryUtil.getExplainPlan(rs));
           
            query = "SELECT * FROM " + tableName5 + " WHERE (item_id, item_name) IN (SELECT \"item_id\", name FROM " + tableName1 + " WHERE \"item_id\" NOT IN (SELECT \"item_id\" FROM " + tableName4 + "))"
                    + " OR (co_item_id, co_item_name) IN (SELECT \"item_id\", name FROM " + tableName1 + " WHERE \"item_id\" IN (SELECT \"item_id\" FROM " + tableName4 + "))";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000004");
            assertEquals(rs.getString(2), "T4");
            assertEquals(rs.getString(3), "0000000003");
            assertEquals(rs.getString(4), "T3");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000006");
            assertEquals(rs.getString(2), "T6");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "T1");

            assertFalse(rs.next());
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            plan = QueryUtil.getExplainPlan(rs);
            assertPlansMatch(plans[2], plan);
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testExistsSubquery() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName1 = getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME);
        String tableName4 = getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME);
        String tableName5 = getTableName(conn, JOIN_COITEM_TABLE_FULL_NAME);
        try {
            String query = "SELECT \"item_id\", name FROM " + tableName1 + " i WHERE NOT EXISTS (SELECT 1 FROM " + tableName4 + " o WHERE o.\"item_id\" = i.\"item_id\") ORDER BY name";
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "invalid001");
            assertEquals(rs.getString(2), "INVALID-1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000004");
            assertEquals(rs.getString(2), "T4");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000005");
            assertEquals(rs.getString(2), "T5");

            assertFalse(rs.next());
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            assertPlansEqual(plans[3], QueryUtil.getExplainPlan(rs));
            
            query = "SELECT * FROM " + tableName5 + " co WHERE EXISTS (SELECT 1 FROM " + tableName1 + " i WHERE NOT EXISTS (SELECT 1 FROM " + tableName4 + " WHERE \"item_id\" = i.\"item_id\") AND co.item_id = \"item_id\" AND name = co.item_name)"
                    + " OR EXISTS (SELECT 1 FROM " + tableName1 + " WHERE \"item_id\" IN (SELECT \"item_id\" FROM " + tableName4 + ") AND co.co_item_id = \"item_id\" AND name = co.co_item_name)";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000004");
            assertEquals(rs.getString(2), "T4");
            assertEquals(rs.getString(3), "0000000003");
            assertEquals(rs.getString(4), "T3");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000006");
            assertEquals(rs.getString(2), "T6");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "T1");

            assertFalse(rs.next());
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            String plan = QueryUtil.getExplainPlan(rs);
            assertPlansMatch(plans[2], plan);
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testComparisonSubquery() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        final Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName1 = getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME);
        String tableName3 = getTableName(conn, JOIN_CUSTOMER_TABLE_FULL_NAME);
        String tableName4 = getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME);
        try {
            String query = "SELECT \"order_id\", name FROM " + tableName4 + 
                    " o JOIN " + tableName1 + 
                    " i ON o.\"item_id\" = i.\"item_id\" WHERE quantity = (SELECT max(quantity) FROM " + 
                    tableName4 + " q WHERE o.\"item_id\" = q.\"item_id\")";
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000001");
            assertEquals(rs.getString(2), "T1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000003");
            assertEquals(rs.getString(2), "T2");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000004");
            assertEquals(rs.getString(2), "T6");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000005");
            assertEquals(rs.getString(2), "T3");

            assertFalse(rs.next());

            query = "SELECT \"order_id\", name FROM " + tableName4 + 
                    " o JOIN " + tableName1 + 
                    " i ON o.\"item_id\" = i.\"item_id\" WHERE quantity = (SELECT max(quantity) FROM " + 
                    tableName1 + " i2 JOIN " + tableName4 + 
                    " q ON i2.\"item_id\" = q.\"item_id\" WHERE o.\"item_id\" = i2.\"item_id\")";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000001");
            assertEquals(rs.getString(2), "T1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000003");
            assertEquals(rs.getString(2), "T2");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000004");
            assertEquals(rs.getString(2), "T6");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000005");
            assertEquals(rs.getString(2), "T3");

            assertFalse(rs.next());

            query = "SELECT name from " + tableName3 + 
                    " WHERE \"customer_id\" IN (SELECT \"customer_id\" FROM " + 
                    tableName1 + " i JOIN " + tableName4 + 
                    " o ON o.\"item_id\" = i.\"item_id\" WHERE i.name = 'T2' OR quantity > (SELECT avg(quantity) FROM " + 
                    tableName4 + " q WHERE o.\"item_id\" = q.\"item_id\"))";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "C2");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "C4");

            assertFalse(rs.next());
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            String plan = QueryUtil.getExplainPlan(rs);
            assertPlansMatch(plans[4], plan);

            query = "SELECT \"order_id\" FROM " + tableName4 + 
                    " o WHERE quantity = (SELECT quantity FROM " + tableName4 + 
                    " WHERE o.\"item_id\" = \"item_id\" AND \"order_id\" != '000000000000004')";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000001");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000002");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000003");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000005");

            assertFalse(rs.next());

            query = "SELECT \"order_id\" FROM " + tableName4 + 
                    " o WHERE quantity = (SELECT quantity FROM " + tableName4 + 
                    " WHERE o.\"item_id\" = \"item_id\" AND \"order_id\" != '000000000000003')";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            try {
                while(rs.next());
                fail("Should have got exception.");
            } catch (SQLException e) {                
            }

            query = "SELECT \"order_id\" FROM " + tableName4 + 
                    " o WHERE quantity = (SELECT max(quantity) FROM " + tableName4 + 
                    " WHERE o.\"item_id\" = \"item_id\" AND \"order_id\" != '000000000000004' GROUP BY \"order_id\")";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000001");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000002");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000003");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000005");

            assertFalse(rs.next());

            query = "SELECT \"order_id\" FROM " + tableName4 + 
                    " o WHERE quantity = (SELECT max(quantity) FROM " + tableName4 + 
                    " WHERE o.\"item_id\" = \"item_id\" AND \"order_id\" != '000000000000003' GROUP BY \"order_id\")";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            try {
                while(rs.next());
                fail("Should have got exception.");
            } catch (SQLException e) {                
            }
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testAnyAllComparisonSubquery() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName1 = getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME);
        String tableName4 = getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME);
        try {
            String query = "SELECT \"order_id\", name FROM " + tableName4 + " o JOIN " + tableName1 + " i ON o.\"item_id\" = i.\"item_id\" WHERE quantity = ALL(SELECT quantity FROM " + tableName4 + " q WHERE o.\"item_id\" = q.\"item_id\")";
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000001");
            assertEquals(rs.getString(2), "T1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000003");
            assertEquals(rs.getString(2), "T2");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000005");
            assertEquals(rs.getString(2), "T3");

            assertFalse(rs.next());
            
            query = "SELECT \"order_id\", name FROM " + tableName4 + " o JOIN " + tableName1 + " i ON o.\"item_id\" = i.\"item_id\" WHERE quantity != ALL(SELECT max(quantity) FROM " + tableName4 + " q WHERE o.\"item_id\" = q.\"item_id\")";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000002");
            assertEquals(rs.getString(2), "T6");

            assertFalse(rs.next());
            
            query = "SELECT \"order_id\", name FROM " + tableName4 + " o JOIN " + tableName1 + " i ON o.\"item_id\" = i.\"item_id\" WHERE quantity != ANY(SELECT quantity FROM " + tableName4 + " q WHERE o.\"item_id\" = q.\"item_id\" GROUP BY quantity)";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000002");
            assertEquals(rs.getString(2), "T6");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000004");
            assertEquals(rs.getString(2), "T6");

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSubqueryWithUpsert() throws Exception {
        String tempTable = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        String tableName1 = getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME);
        String tableName4 = getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME);
        try {            
            conn.createStatement().execute("CREATE TABLE " + tempTable 
                    + "   (item_id varchar not null primary key, " 
                    + "    name varchar)");
            conn.createStatement().execute("UPSERT INTO " + tempTable + "(item_id, name)"
                    + "   SELECT \"item_id\", name FROM " + tableName1 
                    + "   WHERE \"item_id\" NOT IN (SELECT \"item_id\" FROM " + tableName4 + ")");
            
            String query = "SELECT name FROM " + tempTable + " ORDER BY item_id";
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T4");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T5");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "INVALID-1");

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSubqueryWithDelete() throws Exception {
        String tempTable = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        String tableName1 = getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME);
        String tableName4 = getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME);
        try {            
            conn.createStatement().execute("CREATE TABLE " + tempTable 
                    + "   (item_id varchar not null primary key, " 
                    + "    name varchar)");
            conn.createStatement().execute("UPSERT INTO " + tempTable + "(item_id, name)"
                    + "   SELECT \"item_id\", name FROM " + tableName1);

            String query = "SELECT count(*) FROM " + tableName1;
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getInt(1), 7);
            assertFalse(rs.next());
            
            conn.createStatement().execute("DELETE FROM " + tempTable + " WHERE item_id IN ("
                    + "   SELECT \"item_id\" FROM " + tableName4 + ")");
            
            query = "SELECT name FROM " + tempTable + " ORDER BY item_id";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T4");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T5");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "INVALID-1");

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

}

