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

import static org.apache.phoenix.util.TestUtil.JOIN_COITEM_TABLE_DISPLAY_NAME;
import static org.apache.phoenix.util.TestUtil.JOIN_COITEM_TABLE_FULL_NAME;
import static org.apache.phoenix.util.TestUtil.JOIN_CUSTOMER_TABLE_DISPLAY_NAME;
import static org.apache.phoenix.util.TestUtil.JOIN_CUSTOMER_TABLE_FULL_NAME;
import static org.apache.phoenix.util.TestUtil.JOIN_ITEM_TABLE_DISPLAY_NAME;
import static org.apache.phoenix.util.TestUtil.JOIN_ITEM_TABLE_FULL_NAME;
import static org.apache.phoenix.util.TestUtil.JOIN_ORDER_TABLE_DISPLAY_NAME;
import static org.apache.phoenix.util.TestUtil.JOIN_ORDER_TABLE_FULL_NAME;
import static org.apache.phoenix.util.TestUtil.JOIN_SCHEMA;
import static org.apache.phoenix.util.TestUtil.JOIN_SUPPLIER_TABLE_DISPLAY_NAME;
import static org.apache.phoenix.util.TestUtil.JOIN_SUPPLIER_TABLE_FULL_NAME;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.TableAlreadyExistsException;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@RunWith(Parameterized.class)
public class SubqueryUsingSortMergeJoinIT extends BaseHBaseManagedTimeIT {
    
    private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private String[] indexDDL;
    private String[] plans;
    
    public SubqueryUsingSortMergeJoinIT(String[] indexDDL, String[] plans) {
        this.indexDDL = indexDDL;
        this.plans = plans;
    }
    
    @BeforeClass
    @Shadower(classBeingShadowed = BaseHBaseManagedTimeIT.class)
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(3);
        // Forces server cache to be used
        props.put(QueryServices.INDEX_MUTATE_BATCH_SIZE_THRESHOLD_ATTRIB, Integer.toString(2));
        // Must update config before starting server
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }
    
    @Before
    public void initTable() throws Exception {
        initTableValues();
        if (indexDDL != null && indexDDL.length > 0) {
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
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
    
    @Parameters
    public static Collection<Object> data() {
        List<Object> testCases = Lists.newArrayList();
        testCases.add(new String[][] {
                {}, {
                "SORT-MERGE-JOIN (SEMI) TABLES\n" +
                "    SORT-MERGE-JOIN (INNER) TABLES\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_DISPLAY_NAME + "\n" +
                "            SERVER SORTED BY [\"I.supplier_id\"]\n" +
                "        CLIENT MERGE SORT\n" +
                "    AND\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SUPPLIER_TABLE_DISPLAY_NAME + "\n" +
                "            SERVER SORTED BY [\"S.supplier_id\"]\n" +
                "        CLIENT MERGE SORT\n" +
                "    CLIENT SORTED BY [\"I.item_id\"]\n" +
                "AND (SKIP MERGE)\n" +
                "    CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_ORDER_TABLE_DISPLAY_NAME + " ['000000000000001'] - [*]\n" +
                "        SERVER AGGREGATE INTO DISTINCT ROWS BY [\"item_id\"]\n" +
                "    CLIENT MERGE SORT\n" +
                "    CLIENT SORTED BY [\"item_id\"]\n" +
                "CLIENT SORTED BY [I.NAME]",

                "SORT-MERGE-JOIN \\(LEFT\\) TABLES\n" +
                "    SORT-MERGE-JOIN \\(LEFT\\) TABLES\n" +
                "        CLIENT PARALLEL 4-WAY FULL SCAN OVER " + JOIN_COITEM_TABLE_DISPLAY_NAME + "\n" +
                "        CLIENT MERGE SORT\n" +
                "    AND\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_DISPLAY_NAME + "\n" +
                "            SERVER AGGREGATE INTO DISTINCT ROWS BY \\[\".+.item_id\", .+.NAME\\]\n" +
                "        CLIENT MERGE SORT\n" +
                "        CLIENT SORTED BY \\[\".+.item_id\", .+.NAME\\]\n" +
                "            PARALLEL ANTI-JOIN TABLE 0 \\(SKIP MERGE\\)\n" +
                "                CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_DISPLAY_NAME + "\n" +
                "                    SERVER AGGREGATE INTO DISTINCT ROWS BY \\[\"item_id\"]\\\n" +
                "                CLIENT MERGE SORT\n" +
                "    CLIENT SORTED BY \\[.*.CO_ITEM_ID, .*.CO_ITEM_NAME\\]\n" +
                "AND\n" +
                "    CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_DISPLAY_NAME + "\n" +
                "        SERVER AGGREGATE INTO DISTINCT ROWS BY \\[\".+.item_id\", .+.NAME\\]\n" +
                "    CLIENT MERGE SORT\n" +
                "    CLIENT SORTED BY \\[\".+.item_id\", .+.NAME\\]\n" +
                "        SKIP-SCAN-JOIN TABLE 0\n" +
                "            CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_DISPLAY_NAME + "\n" +
                "                SERVER AGGREGATE INTO DISTINCT ROWS BY \\[\"item_id\"\\]\n" +
                "            CLIENT MERGE SORT\n" +
                "        DYNAMIC SERVER FILTER BY \"" + JOIN_ITEM_TABLE_DISPLAY_NAME + ".item_id\" IN \\(\\$\\d+.\\$\\d+\\)\n" +
                "CLIENT FILTER BY \\(\\$\\d+.\\$\\d+ IS NOT NULL OR \\$\\d+.\\$\\d+ IS NOT NULL\\)",            

                "SORT-MERGE-JOIN \\(SEMI\\) TABLES\n" +
                "    CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_CUSTOMER_TABLE_DISPLAY_NAME + "\n" +
                "AND \\(SKIP MERGE\\)\n" +
                "    CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_DISPLAY_NAME + "\n" +
                "        SERVER AGGREGATE INTO DISTINCT ROWS BY \\[\"O.customer_id\"\\]\n" +
                "    CLIENT MERGE SORT\n" +
                "    CLIENT SORTED BY \\[\"O.customer_id\"\\]\n" +
                "        PARALLEL INNER-JOIN TABLE 0\n" +
                "            CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_DISPLAY_NAME + "\n" +
                "        PARALLEL LEFT-JOIN TABLE 1\\(DELAYED EVALUATION\\)\n" +
                "            CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_DISPLAY_NAME + "\n" +
                "                SERVER AGGREGATE INTO DISTINCT ROWS BY \\[\"item_id\"\\]\n" +
                "            CLIENT MERGE SORT\n" +
                "        DYNAMIC SERVER FILTER BY \"I.item_id\" IN \\(\"O.item_id\"\\)\n" +
                "        AFTER-JOIN SERVER FILTER BY \\(I.NAME = 'T2' OR O.QUANTITY > \\$\\d+.\\$\\d+\\)",
                }});
        testCases.add(new String[][] {
                {
                "CREATE INDEX \"idx_customer\" ON " + JOIN_CUSTOMER_TABLE_FULL_NAME + " (name)",
                "CREATE INDEX \"idx_item\" ON " + JOIN_ITEM_TABLE_FULL_NAME + " (name) INCLUDE (price, discount1, discount2, \"supplier_id\", description)",
                "CREATE INDEX \"idx_supplier\" ON " + JOIN_SUPPLIER_TABLE_FULL_NAME + " (name)"
                }, {
                "SORT-MERGE-JOIN (SEMI) TABLES\n" +
                "    SORT-MERGE-JOIN (INNER) TABLES\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SCHEMA + ".idx_item\n" +
                "            SERVER SORTED BY [\"I.0:supplier_id\"]\n" +
                "        CLIENT MERGE SORT\n" +
                "    AND\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SCHEMA + ".idx_supplier\n" +
                "            SERVER FILTER BY FIRST KEY ONLY\n" +
                "            SERVER SORTED BY [\"S.:supplier_id\"]\n" +
                "        CLIENT MERGE SORT\n" +
                "    CLIENT SORTED BY [\"I.:item_id\"]\n" +
                "AND (SKIP MERGE)\n" +
                "    CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_ORDER_TABLE_DISPLAY_NAME + " ['000000000000001'] - [*]\n" +
                "        SERVER AGGREGATE INTO DISTINCT ROWS BY [\"item_id\"]\n" +
                "    CLIENT MERGE SORT\n" +
                "    CLIENT SORTED BY [\"item_id\"]\n" +
                "CLIENT SORTED BY [\"I.0:NAME\"]",

                "SORT-MERGE-JOIN \\(LEFT\\) TABLES\n" +
                "    SORT-MERGE-JOIN \\(LEFT\\) TABLES\n" +
                "        CLIENT PARALLEL 4-WAY FULL SCAN OVER " + JOIN_COITEM_TABLE_DISPLAY_NAME + "\n" +
                "        CLIENT MERGE SORT\n" +
                "    AND\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SCHEMA + ".idx_item\n" +
                "            SERVER FILTER BY FIRST KEY ONLY\n" +
                "            SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY \\[\".+.0:NAME\", \".+.:item_id\"\\]\n" +
                "        CLIENT MERGE SORT\n" +
                "        CLIENT SORTED BY \\[\".+.:item_id\", \".+.0:NAME\"\\]\n" +
                "            PARALLEL ANTI-JOIN TABLE 0 \\(SKIP MERGE\\)\n" +
                "                CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_DISPLAY_NAME + "\n" +
                "                    SERVER AGGREGATE INTO DISTINCT ROWS BY \\[\"item_id\"\\]\n" +
                "                CLIENT MERGE SORT\n" +
                "    CLIENT SORTED BY \\[.*.CO_ITEM_ID, .*.CO_ITEM_NAME\\]\n" +
                "AND\n" +
                "    CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SCHEMA + ".idx_item\n" +
                "        SERVER FILTER BY FIRST KEY ONLY\n" +
                "        SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY \\[\".+.0:NAME\", \".+.:item_id\"\\]\n" +
                "    CLIENT MERGE SORT\n" +
                "    CLIENT SORTED BY \\[\".+.:item_id\", \".+.0:NAME\"\\]\n" +
                "        PARALLEL SEMI-JOIN TABLE 0 \\(SKIP MERGE\\)\n" +
                "            CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_DISPLAY_NAME + "\n" +
                "                SERVER AGGREGATE INTO DISTINCT ROWS BY \\[\"item_id\"\\]\n" +
                "            CLIENT MERGE SORT\n" +
                "CLIENT FILTER BY \\(\\$\\d+.\\$\\d+ IS NOT NULL OR \\$\\d+.\\$\\d+ IS NOT NULL\\)",
                
                "SORT-MERGE-JOIN \\(SEMI\\) TABLES\n" +
                "    CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SCHEMA + ".idx_customer\n" +
                "        SERVER FILTER BY FIRST KEY ONLY\n" +
                "        SERVER SORTED BY \\[\"Join.idx_customer.:customer_id\"\\]\n" +
                "    CLIENT MERGE SORT\n" +
                "AND \\(SKIP MERGE\\)\n" +
                "    CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SCHEMA + ".idx_item\n" +
                "        SERVER FILTER BY FIRST KEY ONLY\n" +
                "        SERVER AGGREGATE INTO DISTINCT ROWS BY \\[\"O.customer_id\"\\]\n" +
                "    CLIENT MERGE SORT\n" +
                "    CLIENT SORTED BY \\[\"O.customer_id\"\\]\n" +
                "        PARALLEL INNER-JOIN TABLE 0\n" +
                "            CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_DISPLAY_NAME + "\n" +
                "        PARALLEL LEFT-JOIN TABLE 1\\(DELAYED EVALUATION\\)\n" +
                "            CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_DISPLAY_NAME + "\n" +
                "                SERVER AGGREGATE INTO DISTINCT ROWS BY \\[\"item_id\"\\]\n" +
                "            CLIENT MERGE SORT\n" +
                "        AFTER-JOIN SERVER FILTER BY \\(\"I.0:NAME\" = 'T2' OR O.QUANTITY > \\$\\d+.\\$\\d+\\)",
                }});
        testCases.add(new String[][] {
                {
                "CREATE LOCAL INDEX \"idx_customer\" ON " + JOIN_CUSTOMER_TABLE_FULL_NAME + " (name)",
                "CREATE LOCAL INDEX \"idx_item\" ON " + JOIN_ITEM_TABLE_FULL_NAME + " (name) INCLUDE (price, discount1, discount2, \"supplier_id\", description)",
                "CREATE LOCAL INDEX \"idx_supplier\" ON " + JOIN_SUPPLIER_TABLE_FULL_NAME + " (name)"
                }, {
                "SORT-MERGE-JOIN (SEMI) TABLES\n" +
                "    SORT-MERGE-JOIN (INNER) TABLES\n" +
                "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + MetaDataUtil.LOCAL_INDEX_TABLE_PREFIX + JOIN_ITEM_TABLE_DISPLAY_NAME + " [-32768]\n" +
                "            SERVER SORTED BY [\"I.0:supplier_id\"]\n" +
                "        CLIENT MERGE SORT\n" +
                "    AND\n" +
                "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + MetaDataUtil.LOCAL_INDEX_TABLE_PREFIX + JOIN_SUPPLIER_TABLE_DISPLAY_NAME + " [-32768]\n" +
                "            SERVER FILTER BY FIRST KEY ONLY\n" +
                "            SERVER SORTED BY [\"S.:supplier_id\"]\n" +
                "        CLIENT MERGE SORT\n" +
                "    CLIENT SORTED BY [\"I.:item_id\"]\n" +
                "AND (SKIP MERGE)\n" +
                "    CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_ORDER_TABLE_DISPLAY_NAME + " ['000000000000001'] - [*]\n" +
                "        SERVER AGGREGATE INTO DISTINCT ROWS BY [\"item_id\"]\n" +
                "    CLIENT MERGE SORT\n" +
                "    CLIENT SORTED BY [\"item_id\"]\n" +
                "CLIENT SORTED BY [\"I.0:NAME\"]",

                "SORT-MERGE-JOIN \\(LEFT\\) TABLES\n" +
                "    SORT-MERGE-JOIN \\(LEFT\\) TABLES\n" +
                "        CLIENT PARALLEL 4-WAY FULL SCAN OVER " + JOIN_COITEM_TABLE_DISPLAY_NAME + "\n" +
                "        CLIENT MERGE SORT\n" +
                "    AND\n" +
                "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + MetaDataUtil.LOCAL_INDEX_TABLE_PREFIX + JOIN_ITEM_TABLE_DISPLAY_NAME + " \\[-32768\\]\n" +
                "            SERVER FILTER BY FIRST KEY ONLY\n" +
                "            SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY \\[\".+.0:NAME\", \".+.:item_id\"\\]\n" +
                "        CLIENT MERGE SORT\n" +
                "        CLIENT SORTED BY \\[\".+.:item_id\", \".+.0:NAME\"\\]\n" +
                "            PARALLEL ANTI-JOIN TABLE 0 \\(SKIP MERGE\\)\n" +
                "                CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_DISPLAY_NAME + "\n" +
                "                    SERVER AGGREGATE INTO DISTINCT ROWS BY \\[\"item_id\"\\]\n" +
                "                CLIENT MERGE SORT\n" +
                "    CLIENT SORTED BY \\[.*.CO_ITEM_ID, .*.CO_ITEM_NAME\\]\n" +
                "AND\n" +
                "    CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + MetaDataUtil.LOCAL_INDEX_TABLE_PREFIX + JOIN_ITEM_TABLE_DISPLAY_NAME + " \\[-32768\\]\n" +
                "        SERVER FILTER BY FIRST KEY ONLY\n" +
                "        SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY \\[\".+.0:NAME\", \".+.:item_id\"\\]\n" +
                "    CLIENT MERGE SORT\n" +
                "    CLIENT SORTED BY \\[\".+.:item_id\", \".+.0:NAME\"\\]\n" +
                "        PARALLEL SEMI-JOIN TABLE 0 \\(SKIP MERGE\\)\n" +
                "            CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_DISPLAY_NAME + "\n" +
                "                SERVER AGGREGATE INTO DISTINCT ROWS BY \\[\"item_id\"\\]\n" +
                "            CLIENT MERGE SORT\n" +
                "        DYNAMIC SERVER FILTER BY \"" + JOIN_SCHEMA + ".idx_item.:item_id\" IN \\(\\$\\d+.\\$\\d+\\)\n" +
                "CLIENT FILTER BY \\(\\$\\d+.\\$\\d+ IS NOT NULL OR \\$\\d+.\\$\\d+ IS NOT NULL\\)",
                
                "SORT-MERGE-JOIN \\(SEMI\\) TABLES\n" +
                "    CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + MetaDataUtil.LOCAL_INDEX_TABLE_PREFIX + JOIN_CUSTOMER_TABLE_DISPLAY_NAME + " \\[-32768\\]\n" +
                "        SERVER FILTER BY FIRST KEY ONLY\n" +
                "        SERVER SORTED BY \\[\"Join.idx_customer.:customer_id\"\\]\n" +
                "    CLIENT MERGE SORT\n" +
                "AND \\(SKIP MERGE\\)\n" +
                "    CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + MetaDataUtil.LOCAL_INDEX_TABLE_PREFIX + JOIN_ITEM_TABLE_DISPLAY_NAME + " \\[-32768\\]\n" +
                "        SERVER FILTER BY FIRST KEY ONLY\n" +
                "        SERVER AGGREGATE INTO DISTINCT ROWS BY \\[\"O.customer_id\"\\]\n" +
                "    CLIENT MERGE SORT\n" +
                "    CLIENT SORTED BY \\[\"O.customer_id\"\\]\n" +
                "        PARALLEL INNER-JOIN TABLE 0\n" +
                "            CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_DISPLAY_NAME + "\n" +
                "        PARALLEL LEFT-JOIN TABLE 1\\(DELAYED EVALUATION\\)\n" +
                "            CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_DISPLAY_NAME + "\n" +
                "                SERVER AGGREGATE INTO DISTINCT ROWS BY \\[\"item_id\"\\]\n" +
                "            CLIENT MERGE SORT\n" +
                "        DYNAMIC SERVER FILTER BY \"I.:item_id\" IN \\(\"O.item_id\"\\)\n" +
                "        AFTER-JOIN SERVER FILTER BY \\(\"I.0:NAME\" = 'T2' OR O.QUANTITY > \\$\\d+.\\$\\d+\\)",
                }});
        return testCases;
    }    
    
    protected void initTableValues() throws Exception {
        ensureTableCreated(getUrl(), JOIN_CUSTOMER_TABLE_FULL_NAME);
        ensureTableCreated(getUrl(), JOIN_ITEM_TABLE_FULL_NAME);
        ensureTableCreated(getUrl(), JOIN_SUPPLIER_TABLE_FULL_NAME);
        ensureTableCreated(getUrl(), JOIN_ORDER_TABLE_FULL_NAME);
        ensureTableCreated(getUrl(), JOIN_COITEM_TABLE_FULL_NAME);
        
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            conn.createStatement().execute("CREATE SEQUENCE my.seq");
            // Insert into customer table
            PreparedStatement stmt = conn.prepareStatement(
                    "upsert into " + JOIN_CUSTOMER_TABLE_FULL_NAME +
                    "   (\"customer_id\", " +
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
                    "upsert into " + JOIN_ITEM_TABLE_FULL_NAME +
                    "   (\"item_id\", " +
                    "    NAME, " +
                    "    PRICE, " +
                    "    DISCOUNT1, " +
                    "    DISCOUNT2, " +
                    "    \"supplier_id\", " +
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
                    "upsert into " + JOIN_SUPPLIER_TABLE_FULL_NAME +
                    "   (\"supplier_id\", " +
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
                    "upsert into " + JOIN_ORDER_TABLE_FULL_NAME +
                    "   (\"order_id\", " +
                    "    \"customer_id\", " +
                    "    \"item_id\", " +
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

            // Insert into coitem table
            stmt = conn.prepareStatement(
                    "upsert into " + JOIN_COITEM_TABLE_FULL_NAME + 
                    "   (item_id, " + 
                    "    item_name, " + 
                    "    co_item_id, " + 
                    "    co_item_name) " + 
                    "values (?, ?, ?, ?)");
            stmt.setString(1, "0000000001");
            stmt.setString(2, "T1");
            stmt.setString(3, "0000000002");
            stmt.setString(4, "T3");
            stmt.execute();
            
            stmt.setString(1, "0000000004");
            stmt.setString(2, "T4");
            stmt.setString(3, "0000000003");
            stmt.setString(4, "T3");
            stmt.execute();
            
            stmt.setString(1, "0000000003");
            stmt.setString(2, "T4");
            stmt.setString(3, "0000000005");
            stmt.setString(4, "T5");
            stmt.execute();
            
            stmt.setString(1, "0000000006");
            stmt.setString(2, "T6");
            stmt.setString(3, "0000000001");
            stmt.setString(4, "T1");
            stmt.execute();
            
            conn.commit();
        } finally {
            conn.close();
        }
    }

    @Test
    public void testInSubquery() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            String query = "SELECT /*+ USE_SORT_MERGE_JOIN*/ \"item_id\", name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " WHERE \"item_id\" IN (SELECT \"item_id\" FROM " + JOIN_ORDER_TABLE_FULL_NAME + ") ORDER BY name";
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

            query = "SELECT /*+ USE_SORT_MERGE_JOIN*/ \"item_id\", name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " WHERE \"item_id\" NOT IN (SELECT \"item_id\" FROM " + JOIN_ORDER_TABLE_FULL_NAME + ") ORDER BY name";
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
            
            query = "SELECT /*+ USE_SORT_MERGE_JOIN*/ i.\"item_id\", s.name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " i JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " s ON i.\"supplier_id\" = s.\"supplier_id\" WHERE i.\"item_id\" IN (SELECT \"item_id\" FROM " + JOIN_ORDER_TABLE_FULL_NAME + " WHERE \"order_id\" > '000000000000001') ORDER BY i.name";
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
            assertEquals(plans[0], QueryUtil.getExplainPlan(rs));
            
            query = "SELECT /*+ USE_SORT_MERGE_JOIN*/ i.\"item_id\", s.name FROM " + JOIN_SUPPLIER_TABLE_FULL_NAME + " s LEFT JOIN " + JOIN_ITEM_TABLE_FULL_NAME + " i ON i.\"supplier_id\" = s.\"supplier_id\" WHERE i.\"item_id\" IN (SELECT \"item_id\" FROM " + JOIN_ORDER_TABLE_FULL_NAME + ") ORDER BY i.name";
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
           
            query = "SELECT /*+ USE_SORT_MERGE_JOIN*/ * FROM " + JOIN_COITEM_TABLE_FULL_NAME + " WHERE (item_id, item_name) IN (SELECT \"item_id\", name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " WHERE \"item_id\" NOT IN (SELECT \"item_id\" FROM " + JOIN_ORDER_TABLE_FULL_NAME + "))"
                    + " OR (co_item_id, co_item_name) IN (SELECT \"item_id\", name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " WHERE \"item_id\" IN (SELECT \"item_id\" FROM " + JOIN_ORDER_TABLE_FULL_NAME + "))";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000006");
            assertEquals(rs.getString(2), "T6");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "T1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000004");
            assertEquals(rs.getString(2), "T4");
            assertEquals(rs.getString(3), "0000000003");
            assertEquals(rs.getString(4), "T3");

            assertFalse(rs.next());
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            String plan = QueryUtil.getExplainPlan(rs);
            assertTrue("\"" + plan + "\" does not match \"" + plans[1] + "\"", Pattern.matches(plans[1], plan));
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testExistsSubquery() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            String query = "SELECT /*+ USE_SORT_MERGE_JOIN*/ \"item_id\", name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " i WHERE NOT EXISTS (SELECT 1 FROM " + JOIN_ORDER_TABLE_FULL_NAME + " o WHERE o.\"item_id\" = i.\"item_id\") ORDER BY name";
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
            
            query = "SELECT /*+ USE_SORT_MERGE_JOIN*/ * FROM " + JOIN_COITEM_TABLE_FULL_NAME + " co WHERE EXISTS (SELECT 1 FROM " + JOIN_ITEM_TABLE_FULL_NAME + " i WHERE NOT EXISTS (SELECT 1 FROM " + JOIN_ORDER_TABLE_FULL_NAME + " WHERE \"item_id\" = i.\"item_id\") AND co.item_id = \"item_id\" AND name = co.item_name)"
                    + " OR EXISTS (SELECT 1 FROM " + JOIN_ITEM_TABLE_FULL_NAME + " WHERE \"item_id\" IN (SELECT \"item_id\" FROM " + JOIN_ORDER_TABLE_FULL_NAME + ") AND co.co_item_id = \"item_id\" AND name = co.co_item_name)";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000006");
            assertEquals(rs.getString(2), "T6");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "T1");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000004");
            assertEquals(rs.getString(2), "T4");
            assertEquals(rs.getString(3), "0000000003");
            assertEquals(rs.getString(4), "T3");

            assertFalse(rs.next());
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            String plan = QueryUtil.getExplainPlan(rs);
            assertTrue("\"" + plan + "\" does not match \"" + plans[1] + "\"", Pattern.matches(plans[1], plan));
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testComparisonSubquery() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            String query = "SELECT /*+ USE_SORT_MERGE_JOIN*/ \"order_id\", name FROM " + JOIN_ORDER_TABLE_FULL_NAME + " o JOIN " + JOIN_ITEM_TABLE_FULL_NAME + " i ON o.\"item_id\" = i.\"item_id\" WHERE quantity = (SELECT max(quantity) FROM " + JOIN_ORDER_TABLE_FULL_NAME + " q WHERE o.\"item_id\" = q.\"item_id\")";
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
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000004");
            assertEquals(rs.getString(2), "T6");

            assertFalse(rs.next());

            query = "SELECT /*+ USE_SORT_MERGE_JOIN*/ name from " + JOIN_CUSTOMER_TABLE_FULL_NAME + " WHERE \"customer_id\" IN (SELECT \"customer_id\" FROM " + JOIN_ITEM_TABLE_FULL_NAME + " i JOIN " + JOIN_ORDER_TABLE_FULL_NAME + " o ON o.\"item_id\" = i.\"item_id\" WHERE i.name = 'T2' OR quantity > (SELECT avg(quantity) FROM " + JOIN_ORDER_TABLE_FULL_NAME + " q WHERE o.\"item_id\" = q.\"item_id\"))";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "C2");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "C4");

            assertFalse(rs.next());
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            String plan = QueryUtil.getExplainPlan(rs);
            assertTrue("\"" + plan + "\" does not match \"" + plans[2] + "\"", Pattern.matches(plans[2], plan));

            query = "SELECT /*+ USE_SORT_MERGE_JOIN*/ \"order_id\" FROM " + JOIN_ORDER_TABLE_FULL_NAME + " o WHERE quantity = (SELECT quantity FROM " + JOIN_ORDER_TABLE_FULL_NAME + " WHERE o.\"item_id\" = \"item_id\" AND \"order_id\" != '000000000000004')";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000001");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000003");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000005");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000002");

            assertFalse(rs.next());

            query = "SELECT /*+ USE_SORT_MERGE_JOIN*/ \"order_id\" FROM " + JOIN_ORDER_TABLE_FULL_NAME + " o WHERE quantity = (SELECT quantity FROM " + JOIN_ORDER_TABLE_FULL_NAME + " WHERE o.\"item_id\" = \"item_id\" AND \"order_id\" != '000000000000003')";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            try {
                while(rs.next());
                fail("Should have got exception.");
            } catch (SQLException e) {                
            }

            query = "SELECT /*+ USE_SORT_MERGE_JOIN*/ \"order_id\" FROM " + JOIN_ORDER_TABLE_FULL_NAME + " o WHERE quantity = (SELECT max(quantity) FROM " + JOIN_ORDER_TABLE_FULL_NAME + " WHERE o.\"item_id\" = \"item_id\" AND \"order_id\" != '000000000000004' GROUP BY \"order_id\")";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000001");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000003");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000005");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000002");

            assertFalse(rs.next());

            query = "SELECT /*+ USE_SORT_MERGE_JOIN*/ \"order_id\" FROM " + JOIN_ORDER_TABLE_FULL_NAME + " o WHERE quantity = (SELECT max(quantity) FROM " + JOIN_ORDER_TABLE_FULL_NAME + " WHERE o.\"item_id\" = \"item_id\" AND \"order_id\" != '000000000000003' GROUP BY \"order_id\")";
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
        try {
            String query = "SELECT /*+ USE_SORT_MERGE_JOIN*/ \"order_id\", name FROM " + JOIN_ORDER_TABLE_FULL_NAME + " o JOIN " + JOIN_ITEM_TABLE_FULL_NAME + " i ON o.\"item_id\" = i.\"item_id\" WHERE quantity = ALL(SELECT quantity FROM " + JOIN_ORDER_TABLE_FULL_NAME + " q WHERE o.\"item_id\" = q.\"item_id\")";
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
            
            query = "SELECT /*+ USE_SORT_MERGE_JOIN*/ \"order_id\", name FROM " + JOIN_ORDER_TABLE_FULL_NAME + " o JOIN " + JOIN_ITEM_TABLE_FULL_NAME + " i ON o.\"item_id\" = i.\"item_id\" WHERE quantity != ALL(SELECT max(quantity) FROM " + JOIN_ORDER_TABLE_FULL_NAME + " q WHERE o.\"item_id\" = q.\"item_id\")";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000002");
            assertEquals(rs.getString(2), "T6");

            assertFalse(rs.next());
            
            query = "SELECT /*+ USE_SORT_MERGE_JOIN*/ \"order_id\", name FROM " + JOIN_ORDER_TABLE_FULL_NAME + " o JOIN " + JOIN_ITEM_TABLE_FULL_NAME + " i ON o.\"item_id\" = i.\"item_id\" WHERE quantity != ANY(SELECT quantity FROM " + JOIN_ORDER_TABLE_FULL_NAME + " q WHERE o.\"item_id\" = q.\"item_id\" GROUP BY quantity)";
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
        String tempTable = "UPSERT_SUBQUERY_TABLE";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        try {            
            conn.createStatement().execute("CREATE TABLE " + tempTable 
                    + "   (item_id varchar not null primary key, " 
                    + "    name varchar)");
            conn.createStatement().execute("UPSERT /*+ USE_SORT_MERGE_JOIN*/ INTO " + tempTable + "(item_id, name)"
                    + "   SELECT \"item_id\", name FROM " + JOIN_ITEM_TABLE_FULL_NAME 
                    + "   WHERE \"item_id\" NOT IN (SELECT \"item_id\" FROM " + JOIN_ORDER_TABLE_FULL_NAME + ")");
            
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

}


