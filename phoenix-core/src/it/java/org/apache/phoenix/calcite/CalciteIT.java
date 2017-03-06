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
package org.apache.phoenix.calcite;

import static org.apache.phoenix.end2end.BaseJoinIT.JOIN_CUSTOMER_TABLE_FULL_NAME;
import static org.apache.phoenix.end2end.BaseJoinIT.JOIN_ITEM_TABLE_FULL_NAME;
import static org.apache.phoenix.end2end.BaseJoinIT.JOIN_ORDER_TABLE_FULL_NAME;
import static org.apache.phoenix.end2end.BaseJoinIT.JOIN_SUPPLIER_TABLE_FULL_NAME;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Properties;

import org.apache.phoenix.util.TestUtil;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Integration test for queries powered by Calcite.
 */
public class CalciteIT extends BaseCalciteIT {
    
    @Before
    public void initTable() throws Exception {
        final String url = getOldUrl();
        initATableValues(TestUtil.ATABLE_NAME, getOrganizationId(), null, null, null, url);
        initJoinTableValues(url);
        initArrayTable(url);
        initSaltedTables(url, null);
        initKeyOrderingTable(url);
        final Connection connection = DriverManager.getConnection(url);
        connection.createStatement().execute("CREATE VIEW IF NOT EXISTS v AS SELECT * from aTable where a_string = 'a'");
        connection.createStatement().execute("CREATE SEQUENCE IF NOT EXISTS seq0 START WITH 1 INCREMENT BY 1");
        connection.createStatement().execute("CREATE SEQUENCE IF NOT EXISTS my.seq1 START WITH 2 INCREMENT BY 2");
        connection.createStatement().execute("UPDATE STATISTICS ATABLE");
        connection.createStatement().execute("UPDATE STATISTICS " + JOIN_CUSTOMER_TABLE_FULL_NAME);
        connection.createStatement().execute("UPDATE STATISTICS " + JOIN_ITEM_TABLE_FULL_NAME);
        connection.createStatement().execute("UPDATE STATISTICS " + JOIN_SUPPLIER_TABLE_FULL_NAME);
        connection.createStatement().execute("UPDATE STATISTICS " + JOIN_ORDER_TABLE_FULL_NAME);
        connection.createStatement().execute("UPDATE STATISTICS " + SCORES_TABLE_NAME);
        connection.createStatement().execute("UPDATE STATISTICS " + SALTED_TABLE_NAME);
        connection.close();
    }
    
    private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    @Test public void testTableScan() throws Exception {
        start(false, 1000f).sql("select * from aTable where a_string = 'a'")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n")
                .resultIs(0, new Object[][] {
                          {"00D300000000XHP", "00A123122312312", "a"}, 
                          {"00D300000000XHP", "00A223122312312", "a"}, 
                          {"00D300000000XHP", "00A323122312312", "a"}, 
                          {"00D300000000XHP", "00A423122312312", "a"}})
                .close();
        
        start(false, 1000f).sql("select \"DATE\" from " + JOIN_ORDER_TABLE_FULL_NAME + " where \"order_id\" = '000000000000001'")
                .resultIs(0, new Object[][]{
                        {new Timestamp(format.parse("2013-11-22 14:22:56").getTime())}})
                .close();
        
        start(false, 1000f).sql("select \"DATE\" from " + JOIN_CUSTOMER_TABLE_FULL_NAME + " where \"customer_id\" = '0000000001'")
                .resultIs(0, new Object[][]{
                        {Date.valueOf("2013-11-01")}})
                .close();
        
        start(false, 1000f).sql("select student_id, scores, exam_date, exam_time, exam_timestamp from " + SCORES_TABLE_NAME)
                .resultIs(0, new Object[][] {
                        {1, new Integer[] {85, 80, 82}, new Date[] {Date.valueOf("2016-3-22"), Date.valueOf("2016-5-23"), Date.valueOf("2016-7-24")}, new Time[] {Time.valueOf("15:30:28"), Time.valueOf("13:26:50"), Time.valueOf("16:20:00")}, new Timestamp[] {Timestamp.valueOf("2016-3-22 15:30:28"), Timestamp.valueOf("2016-5-23 13:26:50"), Timestamp.valueOf("2016-7-24 16:20:00")}},
                        {2, null, null, null, null},
                        {3, new Integer[] {87, 88, 80}, new Date[] {Date.valueOf("2016-3-22"), Date.valueOf("2016-5-23"), Date.valueOf("2016-7-24")}, new Time[] {Time.valueOf("15:30:16"), Time.valueOf("13:26:52"), Time.valueOf("16:20:40")}, new Timestamp[] {Timestamp.valueOf("2016-3-22 15:30:16"), Timestamp.valueOf("2016-5-23 13:26:52"), Timestamp.valueOf("2016-7-24 16:20:40")}}})
                .close();
    }
    
    @Test public void testProject() throws Exception {
        start(false, 1000f).sql("select entity_id, a_string, organization_id from aTable where a_string = 'a'")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(ENTITY_ID=[$1], A_STRING=[$2], ORGANIZATION_ID=[$0])\n" +
                           "    PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n")
                .resultIs(0, new Object[][] {
                          {"00A123122312312", "a", "00D300000000XHP"}, 
                          {"00A223122312312", "a", "00D300000000XHP"}, 
                          {"00A323122312312", "a", "00D300000000XHP"}, 
                          {"00A423122312312", "a", "00D300000000XHP"}})
                .close();
    }
    
    @Test public void testJoin() throws Exception {
        start(false, 1000f).sql("select t1.entity_id, t2.a_string, t1.organization_id from aTable t1 join aTable t2 on t1.entity_id = t2.entity_id and t1.organization_id = t2.organization_id where t1.a_string = 'a'") 
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(ENTITY_ID=[$4], A_STRING=[$2], ORGANIZATION_ID=[$3])\n" +
                           "    PhoenixServerJoin(condition=[AND(=($4, $1), =($3, $0))], joinType=[inner])\n" +
                           "      PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "        PhoenixTableScan(table=[[phoenix, ATABLE]])\n" +
                           "      PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "        PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n")
                .resultIs(0, new Object[][] {
                          {"00A123122312312", "a", "00D300000000XHP"}, 
                          {"00A223122312312", "a", "00D300000000XHP"}, 
                          {"00A323122312312", "a", "00D300000000XHP"}, 
                          {"00A423122312312", "a", "00D300000000XHP"}})
                .close();
        
        start(false, 1000f).sql("SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\"")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(item_id=[$0], NAME=[$1], supplier_id=[$3], NAME0=[$4])\n" +
                           "    PhoenixServerJoin(condition=[=($2, $3)], joinType=[inner])\n" +
                           "      PhoenixServerProject(item_id=[$0], NAME=[$1], supplier_id=[$5])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                           "      PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, SupplierTable]])\n")
                .resultIs(0, new Object[][] {
                          {"0000000001", "T1", "0000000001", "S1"}, 
                          {"0000000002", "T2", "0000000001", "S1"}, 
                          {"0000000003", "T3", "0000000002", "S2"}, 
                          {"0000000004", "T4", "0000000002", "S2"},
                          {"0000000005", "T5", "0000000005", "S5"},
                          {"0000000006", "T6", "0000000006", "S6"}})
                .close();
        
        start(false, 1000f).sql("SELECT * FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\" AND supp.name = 'S5'")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(item_id=[$0], NAME=[$1], PRICE=[$2], DISCOUNT1=[$3], DISCOUNT2=[$4], supplier_id=[$5], DESCRIPTION=[$6], supplier_id0=[$7], NAME0=[$8], PHONE=[$9], ADDRESS=[$10], LOC_ID=[$11])\n" +
                           "    PhoenixServerJoin(condition=[=($5, $7)], joinType=[inner])\n" +
                           "      PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                           "      PhoenixServerProject(supplier_id=[$0], NAME=[$1], PHONE=[$2], ADDRESS=[$3], LOC_ID=[$4], NAME0=[CAST($1):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, SupplierTable]], filter=[=(CAST($1):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", 'S5')])\n")
                .resultIs(0, new Object[][] {
                          {"0000000005", "T5", 500, 8, 15, "0000000005", "Item T5", "0000000005", "S5", "888-888-5555", "505 YYY Street", "10005"}})
                .close();
        
        start(false, 1000f).sql("SELECT \"order_id\", i.name, i.price, discount2, quantity FROM " + JOIN_ORDER_TABLE_FULL_NAME + " o INNER JOIN " 
                + JOIN_ITEM_TABLE_FULL_NAME + " i ON o.\"item_id\" = i.\"item_id\" AND o.price = (i.price * (100 - discount2)) / 100.0 WHERE quantity < 5000")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(order_id=[$5], NAME=[$1], PRICE=[$2], DISCOUNT2=[$3], QUANTITY=[$7])\n" +
                           "    PhoenixServerJoin(condition=[AND(=($6, $0), =($8, $4))], joinType=[inner])\n" +
                           "      PhoenixServerProject(item_id=[$0], NAME=[$1], PRICE=[$2], DISCOUNT2=[$4], $f7=[/(*($2, -(100, $4)), 100.0)])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                           "      PhoenixServerProject(order_id=[$0], item_id=[$2], QUANTITY=[$4], PRICE0=[CAST($3):DECIMAL(17, 6)])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, OrderTable]], filter=[<($4, 5000)])\n")
                .resultIs(0, new Object[][] {
                          {"000000000000004", "T6", 600, 15, 4000}})
                .close();
    }
    
    @Test public void testRightOuterJoin() throws Exception {
        start(false, 1000f).sql("SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item RIGHT OUTER JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\"")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(item_id=[$2], NAME=[$3], supplier_id=[$0], NAME0=[$1])\n" +
                           "    PhoenixServerJoin(condition=[=($4, $0)], joinType=[left])\n" +
                           "      PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, SupplierTable]])\n" +
                           "      PhoenixServerProject(item_id=[$0], NAME=[$1], supplier_id=[$5])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n")
                .resultIs(0, new Object[][] {
                          {"0000000001", "T1", "0000000001", "S1"}, 
                          {"0000000002", "T2", "0000000001", "S1"}, 
                          {"0000000003", "T3", "0000000002", "S2"}, 
                          {"0000000004", "T4", "0000000002", "S2"},
                          {null, null, "0000000003", "S3"}, 
                          {null, null, "0000000004", "S4"}, 
                          {"0000000005", "T5", "0000000005", "S5"},
                          {"0000000006", "T6", "0000000006", "S6"}})
                .close();
    }
    
    @Test public void testClientJoin() throws Exception {        
        start(false, 1000f).sql("SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item FULL OUTER JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\" order by \"item_id\", supp.name")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientSort(sort0=[$0], sort1=[$3], dir0=[ASC], dir1=[ASC])\n" +
                           "    PhoenixClientProject(item_id=[$0], NAME=[$1], supplier_id=[$3], NAME0=[$4])\n" +
                           "      PhoenixClientJoin(condition=[=($2, $3)], joinType=[full])\n" +
                           "        PhoenixServerSort(sort0=[$2], dir0=[ASC])\n" +
                           "          PhoenixServerProject(item_id=[$0], NAME=[$1], supplier_id=[$5])\n" +
                           "            PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                           "        PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "          PhoenixTableScan(table=[[phoenix, Join, SupplierTable]], scanOrder=[FORWARD])\n")
                .resultIs(new Object[][] {
                        {"0000000001", "T1", "0000000001", "S1"},
                        {"0000000002", "T2", "0000000001", "S1"},
                        {"0000000003", "T3", "0000000002", "S2"},
                        {"0000000004", "T4", "0000000002", "S2"},
                        {"0000000005", "T5", "0000000005", "S5"},
                        {"0000000006", "T6", "0000000006", "S6"},
                        {"invalid001", "INVALID-1", null, null},
                        {null, null, "0000000003", "S3"},
                        {null, null, "0000000004", "S4"}})
                .close();
        
        start(false, 1000f).sql("select t1.entity_id, t2.a_string, t1.organization_id from aTable t1 join aTable t2 on t1.organization_id = t2.organization_id and t1.entity_id = t2.entity_id")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(ENTITY_ID=[$1], A_STRING=[$4], ORGANIZATION_ID=[$0])\n" +
                           "    PhoenixClientJoin(condition=[AND(=($0, $2), =($1, $3))], joinType=[inner])\n" +
                           "      PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1])\n" +
                           "        PhoenixTableScan(table=[[phoenix, ATABLE]], scanOrder=[FORWARD])\n" +
                           "      PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "        PhoenixTableScan(table=[[phoenix, ATABLE]], scanOrder=[FORWARD])\n")
                .resultIs(new Object[][] {
                          {"00A123122312312", "a", "00D300000000XHP"},
                          {"00A223122312312", "a", "00D300000000XHP"},
                          {"00A323122312312", "a", "00D300000000XHP"},
                          {"00A423122312312", "a", "00D300000000XHP"},
                          {"00B523122312312", "b", "00D300000000XHP"},
                          {"00B623122312312", "b", "00D300000000XHP"},
                          {"00B723122312312", "b", "00D300000000XHP"},
                          {"00B823122312312", "b", "00D300000000XHP"},
                          {"00C923122312312", "c", "00D300000000XHP"}})
                .close();
        
        start(false, 100000f).sql("select t1.k0, t1.k1, t2.k0, t2.k1 from " + KEY_ORDERING_TABLE_1_NAME + " t1 join " + KEY_ORDERING_TABLE_2_NAME + " t2 on t1.k0 = t2.k0 and t1.k1 = t2.k1")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(K0=[$0], K1=[$1], K00=[$3], K11=[$4])\n" +
                           "    PhoenixClientJoin(condition=[AND(=($0, $3), =($2, $4))], joinType=[inner])\n" +
                           "      PhoenixServerSort(sort0=[$0], sort1=[$2], dir0=[ASC], dir1=[ASC])\n" +
                           "        PhoenixServerProject(K0=[$0], K1=[$1], K10=[CAST($1):BIGINT NOT NULL])\n" +
                           "          PhoenixTableScan(table=[[phoenix, KEY_ORDERING_TEST_TABLE_1]])\n" +
                           "      PhoenixServerProject(K0=[$0], K1=[$1])\n" +
                           "        PhoenixTableScan(table=[[phoenix, KEY_ORDERING_TEST_TABLE_2]], scanOrder=[REVERSE])\n")
                .resultIs(new Object[][] {
                           {1L, 2, 1L, 2L},
                           {1L, 5, 1L, 5L},
                           {2L, 3, 2L, 3L},
                           {2L, 5, 2L, 5L},
                           {5L, 5, 5L, 5L}})
                .close();
    }
    
    @Test public void testJoinPlanningWithCollation() throws Exception { 
        // Server-join with LHS sorted on order-by fields
        start(false, 1000f).sql("SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\" order by supp.\"supplier_id\"")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(item_id=[$2], NAME=[$3], supplier_id=[$0], NAME0=[$1])\n" +
                           "    PhoenixServerJoin(condition=[=($4, $0)], joinType=[inner])\n" +
                           "      PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, SupplierTable]], scanOrder=[FORWARD])\n" +
                           "      PhoenixServerProject(item_id=[$0], NAME=[$1], supplier_id=[$5])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n")
                .close();
        
        // Server-join with LHS reversely sorted on order-by fields
        start(false, 1000f).sql("SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\" order by supp.\"supplier_id\" DESC")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(item_id=[$2], NAME=[$3], supplier_id=[$0], NAME0=[$1])\n" +
                           "    PhoenixServerJoin(condition=[=($4, $0)], joinType=[inner])\n" +
                           "      PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, SupplierTable]], scanOrder=[REVERSE])\n" +
                           "      PhoenixServerProject(item_id=[$0], NAME=[$1], supplier_id=[$5])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n")
                .close();
        
        // Join key being order-by fields with the other side sorted on order-by fields
        start(false, 1000f).sql("SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\" order by item.\"supplier_id\"")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(item_id=[$0], NAME=[$1], supplier_id=[$3], NAME0=[$4], supplier_id0=[$2])\n" +
                           "    PhoenixClientJoin(condition=[=($2, $3)], joinType=[inner])\n" +
                           "      PhoenixServerSort(sort0=[$2], dir0=[ASC])\n" +
                           "        PhoenixServerProject(item_id=[$0], NAME=[$1], supplier_id=[$5])\n" +
                           "          PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                           "      PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, SupplierTable]], scanOrder=[FORWARD])\n")
                .close();

        start(false, 1000f).sql("SELECT \"order_id\", i.name, i.price, discount2, quantity FROM " + JOIN_ORDER_TABLE_FULL_NAME + " o LEFT JOIN "
                + JOIN_ITEM_TABLE_FULL_NAME + " i ON o.\"item_id\" = i.\"item_id\" limit 2")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixLimit(fetch=[2])\n" +
                           "    PhoenixClientProject(order_id=[$0], NAME=[$4], PRICE=[$5], DISCOUNT2=[$6], QUANTITY=[$2])\n" +
                           "      PhoenixClientJoin(condition=[=($1, $3)], joinType=[left])\n" +
                           "        PhoenixClientSort(sort0=[$1], dir0=[ASC])\n" +
                           "          PhoenixLimit(fetch=[2])\n" +
                           "            PhoenixServerProject(order_id=[$0], item_id=[$2], QUANTITY=[$4])\n" +
                           "              PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n" +
                           "        PhoenixServerProject(item_id=[$0], NAME=[$1], PRICE=[$2], DISCOUNT2=[$4])\n" +
                           "          PhoenixTableScan(table=[[phoenix, Join, ItemTable]], scanOrder=[FORWARD])\n")
                .close();
    }
    
    @Test public void testMultiJoin() throws Exception {
        start(false, 1000f).sql("select t1.entity_id, t2.a_string, t3.organization_id from aTable t1 join aTable t2 on t1.entity_id = t2.entity_id and t1.organization_id = t2.organization_id join atable t3 on t1.entity_id = t3.entity_id and t1.organization_id = t3.organization_id where t1.a_string = 'a' and t1.b_string > 'a' and t2.a_string = 'a'") 
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(ENTITY_ID=[$3], A_STRING=[$8], ORGANIZATION_ID=[$0])\n" +
                           "    PhoenixServerJoin(condition=[AND(=($3, $1), =($2, $0))], joinType=[inner])\n" +
                           "      PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1])\n" +
                           "        PhoenixTableScan(table=[[phoenix, ATABLE]])\n" +
                           "      PhoenixClientProject(ORGANIZATION_ID=[$3], ENTITY_ID=[$4], A_STRING=[$5], B_STRING=[$6], ORGANIZATION_ID0=[$0], ENTITY_ID0=[$1], A_STRING0=[$2])\n" +
                           "        PhoenixServerJoin(condition=[AND(=($4, $1), =($3, $0))], joinType=[inner])\n" +
                           "          PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "            PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n" +
                           "          PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2], B_STRING=[$3])\n" +
                           "            PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[AND(=($2, 'a'), >($3, 'a'))])\n")
                .resultIs(0, new Object[][] {
                          {"00A123122312312", "a", "00D300000000XHP"}, 
                          {"00A223122312312", "a", "00D300000000XHP"}, 
                          {"00A323122312312", "a", "00D300000000XHP"}, 
                          {"00A423122312312", "a", "00D300000000XHP"}})
                .close();
        
        start(false, 1000f).sql("select t1.entity_id, t2.a_string, t3.organization_id from aTable t1 join aTable t2 on t1.entity_id = t2.entity_id and t1.organization_id = t2.organization_id join atable t3 on t1.entity_id = t3.entity_id and t1.organization_id = t3.organization_id limit 8 offset 1")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixLimit(offset=[1], fetch=[8])\n" +
                           "    PhoenixClientProject(ENTITY_ID=[$1], A_STRING=[$6], ORGANIZATION_ID=[$2])\n" +
                           "      PhoenixClientJoin(condition=[AND(=($1, $5), =($0, $4))], joinType=[inner])\n" +
                           "        PhoenixClientJoin(condition=[AND(=($1, $3), =($0, $2))], joinType=[inner])\n" +
                           "          PhoenixServerSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[ASC])\n" +
                           "            PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1])\n" +
                           "              PhoenixTableScan(table=[[phoenix, ATABLE]])\n" +
                           "          PhoenixServerSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[ASC])\n" +
                           "            PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1])\n" +
                           "              PhoenixTableScan(table=[[phoenix, ATABLE]])\n" +
                           "        PhoenixServerSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[ASC])\n" +
                           "          PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "            PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(0, new Object[][] {
                          {"00A223122312312", "a", "00D300000000XHP"},
                          {"00A323122312312", "a", "00D300000000XHP"},
                          {"00A423122312312", "a", "00D300000000XHP"},
                          {"00B523122312312", "b", "00D300000000XHP"},
                          {"00B623122312312", "b", "00D300000000XHP"},
                          {"00B723122312312", "b", "00D300000000XHP"},
                          {"00B823122312312", "b", "00D300000000XHP"},
                          {"00C923122312312", "c", "00D300000000XHP"}})
                .close();
    }
    
    @Test public void testAggregate() throws Exception {
        start(false, 1000f).sql("select count(b_string) from atable")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerAggregate(group=[{}], EXPR$0=[COUNT($3)])\n" +
                           "    PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(0, new Object[][] {
                          {9L}})
                .close();
        
        start(false, 1000f).sql("select organization_id, count(b_string) from atable group by organization_id")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerAggregate(group=[{0}], EXPR$1=[COUNT($3)], isOrdered=[true])\n" +
                           "    PhoenixTableScan(table=[[phoenix, ATABLE]], scanOrder=[FORWARD])\n")
                .resultIs(0, new Object[][] {
                          {"00D300000000XHP", 9L}})
                .close();
        
        start(false, 1000f).sql("select organization_id, entity_id, count(b_string) from atable group by entity_id ,organization_id")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerAggregate(group=[{0, 1}], EXPR$2=[COUNT($3)], isOrdered=[true])\n" +
                           "    PhoenixTableScan(table=[[phoenix, ATABLE]], scanOrder=[FORWARD])\n")
                .resultIs(0, new Object[][] {
                          {"00D300000000XHP", "00A123122312312", 1L}, 
                          {"00D300000000XHP", "00A223122312312", 1L}, 
                          {"00D300000000XHP", "00A323122312312", 1L}, 
                          {"00D300000000XHP", "00A423122312312", 1L}, 
                          {"00D300000000XHP", "00B523122312312", 1L}, 
                          {"00D300000000XHP", "00B623122312312", 1L}, 
                          {"00D300000000XHP", "00B723122312312", 1L}, 
                          {"00D300000000XHP", "00B823122312312", 1L}, 
                          {"00D300000000XHP", "00C923122312312", 1L}})
                .close();
        
        start(false, 1000f).sql("select entity_id, count(b_string) from atable group by entity_id")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerAggregate(group=[{1}], EXPR$1=[COUNT($3)], isOrdered=[false])\n" +
                           "    PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(0, new Object[][] {
                        {"00A123122312312", 1L}, 
                        {"00A223122312312", 1L}, 
                        {"00A323122312312", 1L}, 
                        {"00A423122312312", 1L}, 
                        {"00B523122312312", 1L},
                        {"00B623122312312", 1L}, 
                        {"00B723122312312", 1L}, 
                        {"00B823122312312", 1L}, 
                        {"00C923122312312", 1L}})
                .close();
        
        start(false, 1000f).sql("select a_string, count(b_string) from atable group by a_string")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerAggregate(group=[{2}], EXPR$1=[COUNT($3)], isOrdered=[false])\n" +
                           "    PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(0, new Object[][] {
                          {"a", 4L},
                          {"b", 4L},
                          {"c", 1L}})
                .close();
        
        start(false, 1000f).sql("select count(entity_id), a_string from atable group by a_string")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(EXPR$0=[$1], A_STRING=[$0])\n" +
                           "    PhoenixServerAggregate(group=[{2}], EXPR$0=[COUNT()], isOrdered=[false])\n" +
                           "      PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(0, new Object[][] {
                          {4L, "a"},
                          {4L, "b"},
                          {1L, "c"}})
                .close();
        
        start(false, 1000f).sql("select s.name, count(\"item_id\") from " + JOIN_SUPPLIER_TABLE_FULL_NAME + " s join " + JOIN_ITEM_TABLE_FULL_NAME + " i on s.\"supplier_id\" = i.\"supplier_id\" group by s.name")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerAggregate(group=[{1}], EXPR$1=[COUNT()], isOrdered=[false])\n" +
                           "    PhoenixServerJoin(condition=[=($0, $2)], joinType=[inner])\n" +
                           "      PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, SupplierTable]])\n" +
                           "      PhoenixServerProject(supplier_id=[$5])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n")
                .resultIs(0, new Object[][] {
                          {"S1", 2L},
                          {"S2", 2L},
                          {"S5", 1L},
                          {"S6", 1L}})
                .close();
        
        // test PhoenixOrderedAggregateRule
        start(false, 1000f).sql("select s.\"supplier_id\", count(s.name) from " + JOIN_SUPPLIER_TABLE_FULL_NAME + " s join " + JOIN_ITEM_TABLE_FULL_NAME + " i on s.\"supplier_id\" = i.\"supplier_id\" group by s.\"supplier_id\"")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerAggregate(group=[{0}], EXPR$1=[COUNT($1)], isOrdered=[true])\n" +
                           "    PhoenixServerJoin(condition=[=($0, $2)], joinType=[inner])\n" +
                           "      PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, SupplierTable]], scanOrder=[FORWARD])\n" +
                           "      PhoenixServerProject(supplier_id=[$5])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n")
                .resultIs(0, new Object[][] {
                          {"0000000001", 2L},
                          {"0000000002", 2L},
                          {"0000000005", 1L},
                          {"0000000006", 1L}})
                .close();
        
        start(false, 1000f).sql("select a_string, sum(a_integer) from aTable group by a_string")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerAggregate(group=[{2}], EXPR$1=[SUM($4)], isOrdered=[false])\n" +
                           "    PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(0, new Object[][] {
                           {"a", 10L},
                           {"b", 26L},
                           {"c", 9L}})
                .close();
        
        Object[][] expectedResult = new Object[1000][2];
        for (int i = 0; i < 1000; i++) {
            expectedResult[i][0] = i + 1;
            expectedResult[i][1] = i + 2;
        }
        start(false, 1000f).sql("select mypk0, avg(mypk1) from " + SALTED_TABLE_NAME + " group by mypk0")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerAggregate(group=[{0}], EXPR$1=[AVG($1)], isOrdered=[true])\n" +
                           "    PhoenixTableScan(table=[[phoenix, SALTED_TEST_TABLE]], scanOrder=[FORWARD])\n")
                .resultIs(0, expectedResult)
                .close();
        
        Object[][] expectedResult2 = new Object[1000][1];
        for (int i = 0; i < 1000; i++) {
            expectedResult2[i][0] = 1000 - i;
        }
        start(false, 1000f).sql("select mypk0 from " + SALTED_TABLE_NAME + " group by mypk0 order by avg(mypk1) desc")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerSort(sort0=[$1], dir0=[DESC])\n" +
                           "    PhoenixServerAggregate(group=[{0}], agg#0=[AVG($1)], isOrdered=[true])\n" +
                           "      PhoenixTableScan(table=[[phoenix, SALTED_TEST_TABLE]], scanOrder=[FORWARD])\n")
                .resultIs(expectedResult2)
                .close();
    }
    
    @Test public void testDistinct() throws Exception {
        start(false, 1000f).sql("select distinct a_string from aTable")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerAggregate(group=[{2}], isOrdered=[false])\n" +
                           "    PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(0, new Object[][]{
                          {"a"}, 
                          {"b"}, 
                          {"c"}})
                .close();
    }
    
    @Test public void testSort() throws Exception {
        start(false, 1000f).sql("select organization_id, entity_id, a_string from aTable order by a_string, entity_id")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerSort(sort0=[$2], sort1=[$1], dir0=[ASC], dir1=[ASC])\n" +
                           "    PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "      PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {
                          {"00D300000000XHP", "00A123122312312", "a"}, 
                          {"00D300000000XHP", "00A223122312312", "a"}, 
                          {"00D300000000XHP", "00A323122312312", "a"}, 
                          {"00D300000000XHP", "00A423122312312", "a"}, 
                          {"00D300000000XHP", "00B523122312312", "b"}, 
                          {"00D300000000XHP", "00B623122312312", "b"}, 
                          {"00D300000000XHP", "00B723122312312", "b"}, 
                          {"00D300000000XHP", "00B823122312312", "b"}, 
                          {"00D300000000XHP", "00C923122312312", "c"}})
                .close();
        
        start(false, 1000f).sql("select organization_id, entity_id, a_string from aTable order by organization_id, entity_id")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "    PhoenixTableScan(table=[[phoenix, ATABLE]], scanOrder=[FORWARD])\n")
                .resultIs(new Object[][] {
                          {"00D300000000XHP", "00A123122312312", "a"}, 
                          {"00D300000000XHP", "00A223122312312", "a"}, 
                          {"00D300000000XHP", "00A323122312312", "a"}, 
                          {"00D300000000XHP", "00A423122312312", "a"}, 
                          {"00D300000000XHP", "00B523122312312", "b"}, 
                          {"00D300000000XHP", "00B623122312312", "b"}, 
                          {"00D300000000XHP", "00B723122312312", "b"}, 
                          {"00D300000000XHP", "00B823122312312", "b"}, 
                          {"00D300000000XHP", "00C923122312312", "c"}})
                .close();
        
        start(false, 1000f).sql("select organization_id, entity_id, a_string from aTable order by organization_id DESC")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "    PhoenixTableScan(table=[[phoenix, ATABLE]], scanOrder=[REVERSE])\n")
                .resultIs(new Object[][] {
                          {"00D300000000XHP", "00C923122312312", "c"},
                          {"00D300000000XHP", "00B823122312312", "b"}, 
                          {"00D300000000XHP", "00B723122312312", "b"}, 
                          {"00D300000000XHP", "00B623122312312", "b"}, 
                          {"00D300000000XHP", "00B523122312312", "b"}, 
                          {"00D300000000XHP", "00A423122312312", "a"}, 
                          {"00D300000000XHP", "00A323122312312", "a"}, 
                          {"00D300000000XHP", "00A223122312312", "a"}, 
                          {"00D300000000XHP", "00A123122312312", "a"}})
                .close();
        
        start(false, 1000f).sql("select organization_id, entity_id, a_string from aTable order by organization_id DESC, entity_id DESC")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "    PhoenixTableScan(table=[[phoenix, ATABLE]], scanOrder=[REVERSE])\n")
                .resultIs(new Object[][] {
                          {"00D300000000XHP", "00C923122312312", "c"},
                          {"00D300000000XHP", "00B823122312312", "b"}, 
                          {"00D300000000XHP", "00B723122312312", "b"}, 
                          {"00D300000000XHP", "00B623122312312", "b"}, 
                          {"00D300000000XHP", "00B523122312312", "b"}, 
                          {"00D300000000XHP", "00A423122312312", "a"}, 
                          {"00D300000000XHP", "00A323122312312", "a"}, 
                          {"00D300000000XHP", "00A223122312312", "a"}, 
                          {"00D300000000XHP", "00A123122312312", "a"}})
                .close();

        start(false, 1000f).sql("select organization_id, entity_id, a_string from aTable order by organization_id ASC, entity_id DESC")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[DESC])\n" +
                           "    PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "      PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {
                          {"00D300000000XHP", "00C923122312312", "c"},
                          {"00D300000000XHP", "00B823122312312", "b"}, 
                          {"00D300000000XHP", "00B723122312312", "b"}, 
                          {"00D300000000XHP", "00B623122312312", "b"}, 
                          {"00D300000000XHP", "00B523122312312", "b"}, 
                          {"00D300000000XHP", "00A423122312312", "a"}, 
                          {"00D300000000XHP", "00A323122312312", "a"}, 
                          {"00D300000000XHP", "00A223122312312", "a"}, 
                          {"00D300000000XHP", "00A123122312312", "a"}})
                .close();
        
        start(false, 1000f).sql("select count(entity_id), a_string from atable group by a_string order by count(entity_id), a_string desc")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(EXPR$0=[$1], A_STRING=[$0])\n" +
                           "    PhoenixServerSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[DESC])\n" +
                           "      PhoenixServerAggregate(group=[{2}], EXPR$0=[COUNT()], isOrdered=[false])\n" +
                           "        PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {
                          {1L, "c"},
                          {4L, "b"},
                          {4L, "a"}})
                .close();
        
        start(false, 1000f).sql("select s.name, count(\"item_id\") from " + JOIN_SUPPLIER_TABLE_FULL_NAME + " s join " + JOIN_ITEM_TABLE_FULL_NAME + " i on s.\"supplier_id\" = i.\"supplier_id\" group by s.name order by count(\"item_id\"), s.name desc")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[DESC])\n" +
                           "    PhoenixServerAggregate(group=[{1}], EXPR$1=[COUNT()], isOrdered=[false])\n" +
                           "      PhoenixServerJoin(condition=[=($0, $2)], joinType=[inner])\n" +
                           "        PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "          PhoenixTableScan(table=[[phoenix, Join, SupplierTable]])\n" +
                           "        PhoenixServerProject(supplier_id=[$5])\n" +
                           "          PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n")
                .resultIs(new Object[][] {
                          {"S6", 1L},
                          {"S5", 1L},
                          {"S2", 2L},
                          {"S1", 2L}})
                .close();
        
        start(false, 1000f).sql("SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\" order by item.name desc")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(item_id=[$0], NAME=[$1], supplier_id=[$3], NAME0=[$4])\n" +
                           "    PhoenixServerSort(sort0=[$1], dir0=[DESC])\n" +
                           "      PhoenixServerJoin(condition=[=($2, $3)], joinType=[inner])\n" +
                           "        PhoenixServerProject(item_id=[$0], NAME=[$1], supplier_id=[$5])\n" +
                           "          PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                           "        PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "          PhoenixTableScan(table=[[phoenix, Join, SupplierTable]])\n")
                .resultIs(new Object[][] {
                          {"0000000006", "T6", "0000000006", "S6"}, 
                          {"0000000005", "T5", "0000000005", "S5"}, 
                          {"0000000004", "T4", "0000000002", "S2"}, 
                          {"0000000003", "T3", "0000000002", "S2"},
                          {"0000000002", "T2", "0000000001", "S1"},
                          {"0000000001", "T1", "0000000001", "S1"}})
                .close();
    }
    
    @Test public void testSortWithLimitOffset() throws Exception {
        start(false, 1000f).sql("select organization_id, entity_id, a_string from aTable order by a_string, entity_id limit 5 offset 0")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixLimit(fetch=[5])\n" +
                           "    PhoenixServerSort(sort0=[$2], sort1=[$1], dir0=[ASC], dir1=[ASC])\n" +
                           "      PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "        PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {
                          {"00D300000000XHP", "00A123122312312", "a"}, 
                          {"00D300000000XHP", "00A223122312312", "a"}, 
                          {"00D300000000XHP", "00A323122312312", "a"}, 
                          {"00D300000000XHP", "00A423122312312", "a"}, 
                          {"00D300000000XHP", "00B523122312312", "b"}})
                .close();
        
        start(false, 1000f).sql("select organization_id, entity_id, a_string from aTable order by organization_id, entity_id limit 5 offset 3")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixLimit(offset=[3], fetch=[5])\n" +
                           "    PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "      PhoenixTableScan(table=[[phoenix, ATABLE]], scanOrder=[FORWARD])\n")
                .resultIs(new Object[][] {
                          {"00D300000000XHP", "00A423122312312", "a"}, 
                          {"00D300000000XHP", "00B523122312312", "b"},
                          {"00D300000000XHP", "00B623122312312", "b"},
                          {"00D300000000XHP", "00B723122312312", "b"},
                          {"00D300000000XHP", "00B823122312312", "b"}})
                .close();
        
        start(false, 1000f).sql("select organization_id, entity_id, a_string from aTable order by organization_id DESC limit 5 offset 5")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixLimit(offset=[5], fetch=[5])\n" +
                           "    PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "      PhoenixTableScan(table=[[phoenix, ATABLE]], scanOrder=[REVERSE])\n")
                .resultIs(new Object[][] {
                          {"00D300000000XHP", "00A423122312312", "a"},
                          {"00D300000000XHP", "00A323122312312", "a"},
                          {"00D300000000XHP", "00A223122312312", "a"},
                          {"00D300000000XHP", "00A123122312312", "a"}})
                .close();
        
        start(false, 1000f).sql("select organization_id, entity_id, a_string from aTable order by organization_id DESC, entity_id DESC limit 5 offset 2")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixLimit(offset=[2], fetch=[5])\n" +
                           "    PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "      PhoenixTableScan(table=[[phoenix, ATABLE]], scanOrder=[REVERSE])\n")
                .resultIs(new Object[][] {
                          {"00D300000000XHP", "00B723122312312", "b"}, 
                          {"00D300000000XHP", "00B623122312312", "b"}, 
                          {"00D300000000XHP", "00B523122312312", "b"},
                          {"00D300000000XHP", "00A423122312312", "a"},
                          {"00D300000000XHP", "00A323122312312", "a"}})
                .close();

        start(false, 1000f).sql("select organization_id, entity_id, a_string from aTable order by organization_id ASC, entity_id DESC limit 5 offset 0")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixLimit(fetch=[5])\n" +
                           "    PhoenixServerSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[DESC])\n" +
                           "      PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "        PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {
                          {"00D300000000XHP", "00C923122312312", "c"},
                          {"00D300000000XHP", "00B823122312312", "b"}, 
                          {"00D300000000XHP", "00B723122312312", "b"}, 
                          {"00D300000000XHP", "00B623122312312", "b"}, 
                          {"00D300000000XHP", "00B523122312312", "b"}})
                .close();
        
        start(false, 1000f).sql("select count(entity_id), a_string from atable group by a_string order by count(entity_id), a_string desc limit 2 offset 1")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixLimit(offset=[1], fetch=[2])\n" +
                           "    PhoenixClientProject(EXPR$0=[$1], A_STRING=[$0])\n" +
                           "      PhoenixServerSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[DESC])\n" +
                           "        PhoenixServerAggregate(group=[{2}], EXPR$0=[COUNT()], isOrdered=[false])\n" +
                           "          PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {
                          {4L, "b"},
                          {4L, "a"}})
                .close();
        
        start(false, 1000f).sql("select s.name, count(\"item_id\") from " + JOIN_SUPPLIER_TABLE_FULL_NAME + " s join " + JOIN_ITEM_TABLE_FULL_NAME + " i on s.\"supplier_id\" = i.\"supplier_id\" group by s.name order by count(\"item_id\"), s.name desc limit 3 offset 1")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixLimit(offset=[1], fetch=[3])\n" +
                           "    PhoenixServerSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[DESC])\n" +
                           "      PhoenixServerAggregate(group=[{1}], EXPR$1=[COUNT()], isOrdered=[false])\n" +
                           "        PhoenixServerJoin(condition=[=($0, $2)], joinType=[inner])\n" +
                           "          PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "            PhoenixTableScan(table=[[phoenix, Join, SupplierTable]])\n" +
                           "          PhoenixServerProject(supplier_id=[$5])\n" +
                           "            PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n")
                .resultIs(new Object[][] {
                          {"S5", 1L},
                          {"S2", 2L},
                          {"S1", 2L}})
                .close();
        
        start(false, 1000f).sql("SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\" order by item.name desc limit 3")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixLimit(fetch=[3])\n" +
                           "    PhoenixClientProject(item_id=[$0], NAME=[$1], supplier_id=[$3], NAME0=[$4])\n" +
                           "      PhoenixServerSort(sort0=[$1], dir0=[DESC])\n" +
                           "        PhoenixServerJoin(condition=[=($2, $3)], joinType=[inner])\n" +
                           "          PhoenixServerProject(item_id=[$0], NAME=[$1], supplier_id=[$5])\n" +
                           "            PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                           "          PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "            PhoenixTableScan(table=[[phoenix, Join, SupplierTable]])\n")
                .resultIs(new Object[][] {
                          {"0000000006", "T6", "0000000006", "S6"}, 
                          {"0000000005", "T5", "0000000005", "S5"}, 
                          {"0000000004", "T4", "0000000002", "S2"}})
                .close();
    }
    
    @Test public void testLimit() throws Exception {
        start(false, 1000f).sql("select organization_id, entity_id, a_string from aTable limit 5")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixLimit(fetch=[5])\n" +
                           "    PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "      PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIsSomeOf(5, new Object[][] {
                        {"00D300000000XHP", "00A123122312312", "a"}, 
                        {"00D300000000XHP", "00A223122312312", "a"}, 
                        {"00D300000000XHP", "00A323122312312", "a"}, 
                        {"00D300000000XHP", "00A423122312312", "a"}, 
                        {"00D300000000XHP", "00B523122312312", "b"}, 
                        {"00D300000000XHP", "00B623122312312", "b"}, 
                        {"00D300000000XHP", "00B723122312312", "b"}, 
                        {"00D300000000XHP", "00B823122312312", "b"}, 
                        {"00D300000000XHP", "00C923122312312", "c"}})
                .close();
        
        start(false, 1000f).sql("select count(entity_id), a_string from atable group by a_string limit 2")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixLimit(fetch=[2])\n" +
                           "    PhoenixClientProject(EXPR$0=[$1], A_STRING=[$0])\n" +
                           "      PhoenixServerAggregate(group=[{2}], EXPR$0=[COUNT()], isOrdered=[false])\n" +
                           "        PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIsSomeOf(2, new Object[][] {
                          {4L, "a"},
                          {4L, "b"},
                          {1L, "c"}})
                .close();
        
        start(false, 1000f).sql("select s.name, count(\"item_id\") from " + JOIN_SUPPLIER_TABLE_FULL_NAME + " s join " + JOIN_ITEM_TABLE_FULL_NAME + " i on s.\"supplier_id\" = i.\"supplier_id\" group by s.name limit 3")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixLimit(fetch=[3])\n" +
                           "    PhoenixServerAggregate(group=[{1}], EXPR$1=[COUNT()], isOrdered=[false])\n" +
                           "      PhoenixServerJoin(condition=[=($0, $2)], joinType=[inner])\n" +
                           "        PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "          PhoenixTableScan(table=[[phoenix, Join, SupplierTable]])\n" +
                           "        PhoenixServerProject(supplier_id=[$5])\n" +
                           "          PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n")
                .resultIsSomeOf(3, new Object[][] {
                          {"S1", 2L},
                          {"S2", 2L},
                          {"S5", 1L},
                          {"S6", 1L}})
                .close();
        
        start(false, 1000f).sql("SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\" limit 3")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixLimit(fetch=[3])\n" +
                           "    PhoenixClientProject(item_id=[$0], NAME=[$1], supplier_id=[$3], NAME0=[$4])\n" +
                           "      PhoenixServerJoin(condition=[=($2, $3)], joinType=[inner])\n" +
                           "        PhoenixServerProject(item_id=[$0], NAME=[$1], supplier_id=[$5])\n" +
                           "          PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                           "        PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "          PhoenixTableScan(table=[[phoenix, Join, SupplierTable]])\n")
                .resultIsSomeOf(3, new Object[][] {
                          {"0000000001", "T1", "0000000001", "S1"}, 
                          {"0000000002", "T2", "0000000001", "S1"}, 
                          {"0000000003", "T3", "0000000002", "S2"}, 
                          {"0000000004", "T4", "0000000002", "S2"}, 
                          {"0000000005", "T5", "0000000005", "S5"}, 
                          {"0000000006", "T6", "0000000006", "S6"}})
                .close();
        
        start(false, 1000f).sql("SELECT x from (values (1, 2), (2, 4), (3, 6)) as t(x, y) limit 2")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixLimit(fetch=[2])\n" +
                           "    PhoenixClientProject(X=[$0])\n" +
                           "      PhoenixValues(tuples=[[{ 1, 2 }, { 2, 4 }, { 3, 6 }]])\n")
                .resultIsSomeOf(2, new Object[][] {{1}, {2}, {3}})
                .close();
    }

    // PHOENIX CALCITE INTEGRATION : PHOENIX-2827
    @Test public void testLimitOffset() throws Exception {
        start(false, 1000f).sql(
                "select organization_id, entity_id, a_string from aTable limit 5 offset 3")
                .explainIs("PhoenixToEnumerableConverter\n" +
                        "  PhoenixLimit(offset=[3], fetch=[5])\n" +
                        "    PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                        "      PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {
                        { "00D300000000XHP", "00A423122312312", "a" },
                        { "00D300000000XHP", "00B523122312312", "b" },
                        { "00D300000000XHP", "00B623122312312", "b" },
                        { "00D300000000XHP", "00B723122312312", "b" },
                        { "00D300000000XHP", "00B823122312312", "b" }, })
                .close();

        start(false, 1000f).sql(
                "select organization_id, entity_id, a_string from aTable limit 13 offset 12")
                .explainIs("PhoenixToEnumerableConverter\n" +
                        "  PhoenixLimit(offset=[12], fetch=[13])\n" +
                        "    PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                        "      PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {})
                .close();

        start(false, 1000f).sql("select count(entity_id), a_string from atable group by a_string limit 2 offset 0")
                .explainIs("PhoenixToEnumerableConverter\n" +
                        "  PhoenixLimit(fetch=[2])\n" +
                        "    PhoenixClientProject(EXPR$0=[$1], A_STRING=[$0])\n" +
                        "      PhoenixServerAggregate(group=[{2}], EXPR$0=[COUNT()], isOrdered=[false])\n" +
                        "        PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIsSomeOf(2, new Object[][] {
                        {4L, "a"},
                        {4L, "b"},
                        {1L, "c"}})
                .close();

        start(false, 1000f).sql("SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\" limit 3 offset 3")
                .explainIs("PhoenixToEnumerableConverter\n" +
                        "  PhoenixLimit(offset=[3], fetch=[3])\n" +
                        "    PhoenixClientProject(item_id=[$0], NAME=[$1], supplier_id=[$3], NAME0=[$4])\n" +
                        "      PhoenixServerJoin(condition=[=($2, $3)], joinType=[inner])\n" +
                        "        PhoenixServerProject(item_id=[$0], NAME=[$1], supplier_id=[$5])\n" +
                        "          PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                        "        PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                        "          PhoenixTableScan(table=[[phoenix, Join, SupplierTable]])\n")
                .resultIsSomeOf(3, new Object[][] {
                        { "0000000001", "T1", "0000000001", "S1" },
                        { "0000000002", "T2", "0000000001", "S1" },
                        { "0000000003", "T3", "0000000002", "S2" },
                        { "0000000004", "T4", "0000000002", "S2" },
                        { "0000000005", "T5", "0000000005", "S5" },
                        { "0000000006", "T6", "0000000006", "S6" } })
                .close();

        start(false, 1000f).sql("SELECT x from (values (1, 2), (2, 4), (3, 6)) as t(x, y) limit 2 offset 2")
                .explainIs("PhoenixToEnumerableConverter\n" +
                        "  PhoenixLimit(offset=[2], fetch=[2])\n" +
                        "    PhoenixClientProject(X=[$0])\n" +
                        "      PhoenixValues(tuples=[[{ 1, 2 }, { 2, 4 }, { 3, 6 }]])\n")
                .resultIs(new Object[][] {{3}})
                .close();

        start(false, 1000f).sql("SELECT x from (values (1, 2), (2, 4), (3, 6)) as t(x, y) limit 3 offset 4")
                .explainIs("PhoenixToEnumerableConverter\n" +
                        "  PhoenixLimit(offset=[4], fetch=[3])\n" +
                        "    PhoenixClientProject(X=[$0])\n" +
                        "      PhoenixValues(tuples=[[{ 1, 2 }, { 2, 4 }, { 3, 6 }]])\n")
                .resultIs(new Object[][] {})
                .close();
    }

    @Test public void testCountDistinct() throws Exception {
        start(false, 1000f).sql("select count(distinct a_string) from aTable")
                .explainIs("PhoenixToEnumerableConverter\n" +
                        "  PhoenixServerAggregate(group=[{}], EXPR$0=[COUNT(DISTINCT $2)])\n" +
                        "    PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {{3L}})
                .close();

        start(false, 1000f).sql("select a_string, count(distinct b_string) from atable group by a_string")
                .explainIs("PhoenixToEnumerableConverter\n" +
                        "  PhoenixServerAggregate(group=[{2}], EXPR$1=[COUNT(DISTINCT $3)], isOrdered=[false])\n" +
                        "    PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {
                        {"a", 3L},
                        {"b", 3L},
                        {"c", 1L}})
                .close();

        start(false, 1000f).sql("select organization_id, entity_id, count(distinct b_string) from atable group by entity_id ,organization_id")
                .explainIs("PhoenixToEnumerableConverter\n" +
                        "  PhoenixServerAggregate(group=[{0, 1}], EXPR$2=[COUNT(DISTINCT $3)], isOrdered=[true])\n" +
                        "    PhoenixTableScan(table=[[phoenix, ATABLE]], scanOrder=[FORWARD])\n")
                .resultIs(new Object[][] {
                        {"00D300000000XHP", "00A123122312312", 1L},
                        {"00D300000000XHP", "00A223122312312", 1L},
                        {"00D300000000XHP", "00A323122312312", 1L},
                        {"00D300000000XHP", "00A423122312312", 1L},
                        {"00D300000000XHP", "00B523122312312", 1L},
                        {"00D300000000XHP", "00B623122312312", 1L},
                        {"00D300000000XHP", "00B723122312312", 1L},
                        {"00D300000000XHP", "00B823122312312", 1L},
                        {"00D300000000XHP", "00C923122312312", 1L}})
                .close();

        start(false, 1000f).sql("select organization_id, count(distinct entity_id), b_string from atable group by organization_id, b_string")
                .explainIs("PhoenixToEnumerableConverter\n" +
                        "  PhoenixClientProject(ORGANIZATION_ID=[$0], EXPR$1=[$2], B_STRING=[$1])\n" +
                        "    PhoenixServerAggregate(group=[{0, 3}], EXPR$1=[COUNT(DISTINCT $1)], isOrdered=[false])\n" +
                        "      PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(0, new Object[][] {
                        {"00D300000000XHP", 3L, "b"},
                        {"00D300000000XHP", 3L, "c"},
                        {"00D300000000XHP", 3L, "e"}})
                .close();
    }

    @Test public void testOffset() throws Exception {
        start(false, 1000f).sql(
                "select organization_id, entity_id, a_string from aTable offset 3")
                .explainIs("PhoenixToEnumerableConverter\n" +
                        "  PhoenixLimit(offset=[3])\n" +
                        "    PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                        "      PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {
                        { "00D300000000XHP", "00A423122312312", "a" },
                        { "00D300000000XHP", "00B523122312312", "b" },
                        { "00D300000000XHP", "00B623122312312", "b" },
                        { "00D300000000XHP", "00B723122312312", "b" },
                        { "00D300000000XHP", "00B823122312312", "b" },
                        { "00D300000000XHP", "00C923122312312", "c" }})
                .close();

        start(false, 1000f).sql("select count(entity_id), a_string from atable group by a_string offset 1")
                .explainIs("PhoenixToEnumerableConverter\n" +
                        "  PhoenixLimit(offset=[1])\n" +
                        "    PhoenixClientProject(EXPR$0=[$1], A_STRING=[$0])\n" +
                        "      PhoenixServerAggregate(group=[{2}], EXPR$0=[COUNT()], isOrdered=[false])\n" +
                        "        PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIsSomeOf(2, new Object[][] {
                        {4L, "a"},
                        {4L, "b"},
                        {1L, "c"}})
                .close();

        start(false, 1000f).sql("SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\" offset 7")
                .explainIs("PhoenixToEnumerableConverter\n" +
                        "  PhoenixLimit(offset=[7])\n" +
                        "    PhoenixClientProject(item_id=[$0], NAME=[$1], supplier_id=[$3], NAME0=[$4])\n" +
                        "      PhoenixServerJoin(condition=[=($2, $3)], joinType=[inner])\n" +
                        "        PhoenixServerProject(item_id=[$0], NAME=[$1], supplier_id=[$5])\n" +
                        "          PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                        "        PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                        "          PhoenixTableScan(table=[[phoenix, Join, SupplierTable]])\n")
                .resultIs(new Object [][] {})
                .close();

        start(false, 1000f).sql("SELECT x from (values (1, 2), (2, 4), (3, 6)) as t(x, y) offset 0")
                .explainIs("PhoenixToEnumerableConverter\n" +
                        "  PhoenixClientProject(X=[$0])\n" +
                        "    PhoenixValues(tuples=[[{ 1, 2 }, { 2, 4 }, { 3, 6 }]])\n")
                .resultIs(new Object[][] {{1},{2},{3}})
                .close();

        start(false, 1000f).sql("SELECT x from (values (1, 2), (2, 4), (3, 6)) as t(x, y) offset 3")
                .explainIs("PhoenixToEnumerableConverter\n" +
                        "  PhoenixLimit(offset=[3])\n" +
                        "    PhoenixClientProject(X=[$0])\n" +
                        "      PhoenixValues(tuples=[[{ 1, 2 }, { 2, 4 }, { 3, 6 }]])\n")
                .resultIs(new Object[][] {})
                .close();
    }

    @Ignore // CALCITE-1045
    @Test public void testScalarSubquery() throws Exception {
        start(false, 1000f).sql("select \"item_id\", name, (select max(quantity) sq \n"
            + "from " + JOIN_ORDER_TABLE_FULL_NAME + " o where o.\"item_id\" = i.\"item_id\")\n"
            + "from " + JOIN_ITEM_TABLE_FULL_NAME + " i")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixClientProject(item_id=[$0], NAME=[$1], EXPR$2=[$8])\n" +
                       "    PhoenixServerJoin(condition=[=($0, $7)], joinType=[left], isSingleValueRhs=[true])\n" +
                       "      PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                       "      PhoenixServerAggregate(group=[{0}], SQ=[MAX($5)], isOrdered=[true])\n" +
                       "        PhoenixServerJoin(condition=[=($3, $0)], joinType=[inner])\n" +
                       "          PhoenixServerProject(item_id=[$0])\n" +
                       "            PhoenixTableScan(table=[[phoenix, Join, ItemTable]], scanOrder=[FORWARD])\n" +
                       "          PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n")
            .resultIs(0, new Object[][] {
                    new Object[] {"0000000001", "T1", 1000},
                    new Object[] {"0000000002", "T2", 3000},
                    new Object[] {"0000000003", "T3", 5000},
                    new Object[] {"0000000004", "T4", null},
                    new Object[] {"0000000005", "T5", null},
                    new Object[] {"0000000006", "T6", 4000},
                    new Object[] {"invalid001", "INVALID-1", null}})
            .close();
        
        start(false, 1000f).sql("select \"item_id\", name, (select quantity sq \n"
                    + "from " + JOIN_ORDER_TABLE_FULL_NAME + " o where o.\"item_id\" = i.\"item_id\")\n"
                    + "from " + JOIN_ITEM_TABLE_FULL_NAME + " i where \"item_id\" < '0000000006'")
               .explainIs("PhoenixToEnumerableConverter\n" +
                          "  PhoenixClientProject(item_id=[$0], NAME=[$1], EXPR$2=[$8])\n" +
                          "    PhoenixServerJoin(condition=[=($0, $7)], joinType=[left], isSingleValueRhs=[true])\n" +
                          "      PhoenixTableScan(table=[[phoenix, Join, ItemTable]], filter=[<($0, '0000000006')])\n" +
                          "      PhoenixClientProject(item_id0=[$7], SQ=[$4])\n" +
                          "        PhoenixServerJoin(condition=[=($2, $7)], joinType=[inner])\n" +
                          "          PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n" +
                          "          PhoenixServerProject(item_id=[$0])\n" +
                          "            PhoenixTableScan(table=[[phoenix, Join, ItemTable]], filter=[<($0, '0000000006')])\n")
               .resultIs(0, new Object[][] {
                         new Object[] {"0000000001", "T1", 1000},
                         new Object[] {"0000000002", "T2", 3000},
                         new Object[] {"0000000003", "T3", 5000},
                         new Object[] {"0000000004", "T4", null},
                         new Object[] {"0000000005", "T5", null}})
               .close();;
    }
    
    @Test public void testValues() throws Exception {
        start(false, 1000f).sql("select p0+p1 from (values (2, 1)) as t(p0, p1)")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixClientProject(EXPR$0=[+($0, $1)])\n" +
                       "    PhoenixValues(tuples=[[{ 2, 1 }]])\n")
            .close();
        start(false, 1000f).sql("select count(p0), max(p1) from (values (2, 1), (3, 4), (5, 2)) as t(p0, p1)")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixClientAggregate(group=[{}], EXPR$0=[COUNT()], EXPR$1=[MAX($1)])\n" +
                       "    PhoenixValues(tuples=[[{ 2, 1 }, { 3, 4 }, { 5, 2 }]])\n")
            .resultIs(0, new Object[][] {{3L, 4}})
            .close();
    }
    
    @Test public void testUnion() throws Exception {
        start(false, 1000f).sql("select entity_id from atable where a_string = 'a' union all select entity_id from atable where a_string = 'b'")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixUnion(all=[true])\n" +
                           "    PhoenixServerProject(ENTITY_ID=[$1])\n" +
                           "      PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n" +
                           "    PhoenixServerProject(ENTITY_ID=[$1])\n" +
                           "      PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'b')])\n")
                .resultIs(0, new Object[][] {
                        {"00A123122312312"},
                        {"00A223122312312"},
                        {"00A323122312312"},
                        {"00A423122312312"},
                        {"00B523122312312"},
                        {"00B623122312312"},
                        {"00B723122312312"},
                        {"00B823122312312"}})
                .close();
        
        start(false, 1000f).sql("select entity_id, a_string from atable where a_string = 'a' union all select entity_id, a_string from atable where a_string = 'c' order by entity_id desc limit 3 offset 1")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixLimit(offset=[1], fetch=[3])\n" +
                           "    PhoenixMergeSortUnion(all=[true])\n" +
                           "      PhoenixServerSort(sort0=[$0], dir0=[DESC])\n" +
                           "        PhoenixServerProject(ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "          PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n" +
                           "      PhoenixServerSort(sort0=[$0], dir0=[DESC])\n" +
                           "        PhoenixServerProject(ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "          PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'c')])\n")
                .resultIs(new Object[][] {
                        {"00A423122312312", "a"},
                        {"00A323122312312", "a"},
                        {"00A223122312312", "a"}})
                .close();
        
        start(false, 1000f).sql("select entity_id, a_string from atable where a_string = 'a' union all select entity_id, a_string from atable where a_string = 'c' order by entity_id desc")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixMergeSortUnion(all=[true])\n" +
                           "    PhoenixServerSort(sort0=[$0], dir0=[DESC])\n" +
                           "      PhoenixServerProject(ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "        PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n" +
                           "    PhoenixServerSort(sort0=[$0], dir0=[DESC])\n" +
                           "      PhoenixServerProject(ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "        PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'c')])\n")
                .resultIs(new Object[][] {
                        {"00C923122312312", "c"},
                        {"00A423122312312", "a"},
                        {"00A323122312312", "a"},
                        {"00A223122312312", "a"},
                        {"00A123122312312", "a"}})
                .close();
    }
    
    @Test public void testUnnest() throws Exception {
        start(false, 1000f).sql("SELECT t.s FROM UNNEST((SELECT scores FROM " + SCORES_TABLE_NAME + ")) AS t(s)")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixUncollect\n" +
                           "    PhoenixServerProject(EXPR$0=[$2])\n" +
                           "      PhoenixTableScan(table=[[phoenix, SCORES]])\n")
                .resultIs(0, new Object[][] {
                        {85}, 
                        {80}, 
                        {82}, 
                        {87}, 
                        {88}, 
                        {80}})
                .close();
        start(false, 1000f).sql("SELECT t.o, t.s FROM UNNEST((SELECT scores FROM " + SCORES_TABLE_NAME + ")) WITH ORDINALITY AS t(s, o)")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(O=[$1], S=[$0])\n" +
                           "    PhoenixUncollect(withOrdinality=[true])\n" +
                           "      PhoenixServerProject(EXPR$0=[$2])\n" +
                           "        PhoenixTableScan(table=[[phoenix, SCORES]])\n")
                .resultIs(0, new Object[][] {
                        {1, 85},
                        {2, 80},
                        {3, 82},
                        {1, 87},
                        {2, 88},
                        {3, 80}})
                .close();
        start(false, 1000f).sql("SELECT s.student_id, t.score FROM " + SCORES_TABLE_NAME + " s, UNNEST((SELECT scores FROM " + SCORES_TABLE_NAME + " s2 where s.student_id = s2.student_id)) AS t(score)")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(STUDENT_ID=[$0], SCORE=[$6])\n" +
                           "    PhoenixCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n" +
                           "      PhoenixTableScan(table=[[phoenix, SCORES]])\n" +
                           "      PhoenixUncollect\n" +
                           "        PhoenixServerProject(EXPR$0=[$2])\n" +
                           "          PhoenixTableScan(table=[[phoenix, SCORES]], filter=[=($cor0.STUDENT_ID, $0)])\n")
                .resultIs(0, new Object[][] {
                        {1, 85}, 
                        {1, 80}, 
                        {1, 82}, 
                        {3, 87}, 
                        {3, 88}, 
                        {3, 80}})
                .close();
    }
    
    @Test public void testCorrelateAndDecorrelation() throws Exception {
        Properties correlProps = getConnectionProps(false, 1000f);
        correlProps.setProperty("forceDecorrelate", Boolean.FALSE.toString());
        Properties decorrelProps = getConnectionProps(false, 1000f);
        decorrelProps.setProperty("forceDecorrelate", Boolean.TRUE.toString());
        
        String q1 = "select \"order_id\", quantity from " + JOIN_ORDER_TABLE_FULL_NAME + " o where quantity = (select max(quantity) from " + JOIN_ORDER_TABLE_FULL_NAME + " o2 where o.\"item_id\" = o2.\"item_id\")";
        Object[][] r1 = new Object[][] {
                {"000000000000001", 1000},
                {"000000000000003", 3000},
                {"000000000000004", 4000},
                {"000000000000005", 5000}};
        String p1Correlate = 
                "PhoenixToEnumerableConverter\n" +
                "  PhoenixClientProject(order_id=[$0], QUANTITY=[$2])\n" +
                "    PhoenixFilter(condition=[=($2, $3)])\n" +
                "      PhoenixCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{1}])\n" +
                "        PhoenixServerProject(order_id=[$0], item_id=[$2], QUANTITY=[$4])\n" +
                "          PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n" +
                "        PhoenixServerAggregate(group=[{}], EXPR$0=[MAX($4)])\n" +
                "          PhoenixTableScan(table=[[phoenix, Join, OrderTable]], filter=[=($cor0.item_id, $2)])\n";
        String p1Decorrelated = 
                "PhoenixToEnumerableConverter\n" +
                "  PhoenixClientProject(order_id=[$0], QUANTITY=[$2])\n" +
                "    PhoenixServerJoin(condition=[AND(=($1, $3), =($2, $4))], joinType=[inner])\n" +
                "      PhoenixServerProject(order_id=[$0], item_id=[$2], QUANTITY=[$4])\n" +
                "        PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n" +
                "      PhoenixServerAggregate(group=[{2}], EXPR$0=[MAX($1)], isOrdered=[false])\n" +
                "        PhoenixServerJoin(condition=[=($2, $0)], joinType=[inner])\n" +
                "          PhoenixServerProject(item_id=[$2], QUANTITY=[$4])\n" +
                "            PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n" +
                "          PhoenixServerAggregate(group=[{2}], isOrdered=[false])\n" +
                "            PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n";
        start(correlProps).sql(q1).explainIs(p1Correlate).resultIs(0, r1).close();
        start(decorrelProps).sql(q1).explainIs(p1Decorrelated).resultIs(0, r1).close();
                
        String q2 = "select name from " + JOIN_ITEM_TABLE_FULL_NAME + " i where price = (select max(price) from " + JOIN_ITEM_TABLE_FULL_NAME + " i2 where i.\"item_id\" = i2.\"item_id\" and i.name = i2.name and i2.\"item_id\" <> 'invalid001')";
        Object[][] r2 = new Object[][]{
                {"T1"},
                {"T2"},
                {"T3"},
                {"T4"},
                {"T5"},
                {"T6"}};
        String p2Correlate = 
                "PhoenixToEnumerableConverter\n" +
                "  PhoenixClientProject(NAME=[$1])\n" +
                "    PhoenixFilter(condition=[=($2, $3)])\n" +
                "      PhoenixCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{0, 1}])\n" +
                "        PhoenixServerProject(item_id=[$0], NAME=[$1], PRICE=[$2])\n" +
                "          PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                "        PhoenixServerAggregate(group=[{}], EXPR$0=[MAX($2)])\n" +
                "          PhoenixTableScan(table=[[phoenix, Join, ItemTable]], filter=[AND(=($cor0.item_id, $0), =($cor0.NAME, $1), <>($0, 'invalid001'))])\n";
        String p2Decorrelated = 
                "PhoenixToEnumerableConverter\n" +
                "  PhoenixClientProject(NAME=[$1])\n" +
                "    PhoenixServerJoin(condition=[AND(=($0, $3), =($1, $4), =($2, $5))], joinType=[inner])\n" +
                "      PhoenixServerProject(item_id=[$0], NAME=[$1], PRICE=[$2])\n" +
                "        PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                "      PhoenixServerAggregate(group=[{0, 1}], EXPR$0=[MAX($4)], isOrdered=[false])\n" +
                "        PhoenixServerJoin(condition=[AND(=($0, $2), =($1, $3))], joinType=[inner])\n" +
                "          PhoenixServerProject(item_id=[$0], NAME=[$1])\n" +
                "            PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                "          PhoenixServerProject(item_id=[$0], NAME=[$1], PRICE=[$2])\n" +
                "            PhoenixTableScan(table=[[phoenix, Join, ItemTable]], filter=[<>($0, 'invalid001')])\n";
        start(correlProps).sql(q2).explainIs(p2Correlate).resultIs(0, r2).close();
        start(decorrelProps).sql(q2).explainIs(p2Decorrelated).resultIs(0, r2).close();
        
        // Test PhoenixClientSemiJoin
        String q3 = "select \"item_id\", name from " + JOIN_ITEM_TABLE_FULL_NAME + " i where exists (select 1 from " + JOIN_ORDER_TABLE_FULL_NAME + " o where i.\"item_id\" = o.\"item_id\")";
        Object[][] r3 = new Object[][] {
                {"0000000001", "T1"},
                {"0000000002", "T2"},
                {"0000000003", "T3"},
                {"0000000006", "T6"}};
        String p3Correlate = 
                "PhoenixToEnumerableConverter\n" +
                "  PhoenixClientProject(item_id=[$0], NAME=[$1])\n" +
                "    PhoenixCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n" +
                "      PhoenixServerProject(item_id=[$0], NAME=[$1])\n" +
                "        PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                "      PhoenixServerAggregate(group=[{0}], isOrdered=[false])\n" +
                "        PhoenixServerProject(i=[true])\n" +
                "          PhoenixTableScan(table=[[phoenix, Join, OrderTable]], filter=[=($cor0.item_id, $2)])\n";
        String p3Decorrelated = 
                "PhoenixToEnumerableConverter\n" +
                "  PhoenixClientSemiJoin(condition=[=($0, $3)], joinType=[inner])\n" +
                "    PhoenixServerProject(item_id=[$0], NAME=[$1])\n" +
                "      PhoenixTableScan(table=[[phoenix, Join, ItemTable]], scanOrder=[FORWARD])\n" +
                "    PhoenixClientProject(item_id=[$1], item_id0=[$0])\n" +
                "      PhoenixServerJoin(condition=[=($0, $1)], joinType=[inner])\n" +
                "        PhoenixServerProject(item_id=[$0])\n" +
                "          PhoenixTableScan(table=[[phoenix, Join, ItemTable]], scanOrder=[FORWARD])\n" +
                "        PhoenixServerProject(item_id=[$2])\n" +
                "          PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n";
        start(correlProps).sql(q3).explainIs(p3Correlate).resultIs(0, r3).close();
        start(decorrelProps).sql(q3).explainIs(p3Decorrelated).resultIs(0, r3).close();
        
        String q4 = "select \"item_id\", name from " + JOIN_ITEM_TABLE_FULL_NAME + " i where \"item_id\" in (select \"item_id\" from " + JOIN_ORDER_TABLE_FULL_NAME + ")";
        Object[][] r4 = new Object[][] {
                {"0000000001", "T1"},
                {"0000000002", "T2"},
                {"0000000003", "T3"},
                {"0000000006", "T6"}};
        String p4Decorrelated = 
                "PhoenixToEnumerableConverter\n" +
                "  PhoenixServerSemiJoin(condition=[=($0, $4)], joinType=[inner])\n" +
                "    PhoenixServerProject(item_id=[$0], NAME=[$1])\n" +
                "      PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                "    PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n";
        start(decorrelProps).sql(q4).explainIs(p4Decorrelated).resultIs(0, r4).close();
        
        String q5 = "select \"order_id\" from " + JOIN_ITEM_TABLE_FULL_NAME + " i JOIN " + JOIN_ORDER_TABLE_FULL_NAME + " o on o.\"item_id\" = i.\"item_id\" where quantity = (select max(quantity) from " + JOIN_ORDER_TABLE_FULL_NAME + " o2 JOIN " + JOIN_ITEM_TABLE_FULL_NAME + " i2 on o2.\"item_id\" = i2.\"item_id\" where i.\"supplier_id\" = i2.\"supplier_id\")";
        Object [][] r5 = new Object[][] {
                {"000000000000003"},
                {"000000000000005"},
                {"000000000000004"}};
        String p5Correlate = 
                "PhoenixToEnumerableConverter\n" +
                "  PhoenixClientProject(order_id=[$2])\n" +
                "    PhoenixFilter(condition=[=($4, $5)])\n" +
                "      PhoenixCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{1}])\n" +
                "        PhoenixServerJoin(condition=[=($3, $0)], joinType=[inner])\n" +
                "          PhoenixServerProject(item_id=[$0], supplier_id=[$5])\n" +
                "            PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                "          PhoenixServerProject(order_id=[$0], item_id=[$2], QUANTITY=[$4])\n" +
                "            PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n" +
                "        PhoenixServerAggregate(group=[{}], EXPR$0=[MAX($4)])\n" +
                "          PhoenixServerJoin(condition=[=($2, $7)], joinType=[inner])\n" +
                "            PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n" +
                "            PhoenixTableScan(table=[[phoenix, Join, ItemTable]], filter=[=($cor0.supplier_id, $5)])\n";
        String p5Decorrelated =
                "PhoenixToEnumerableConverter\n" +
                "  PhoenixClientProject(order_id=[$2])\n" +
                "    PhoenixServerJoin(condition=[AND(=($1, $5), =($3, $0))], joinType=[inner])\n" +
                "      PhoenixServerProject(item_id=[$0], supplier_id=[$5])\n" +
                "        PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                "      PhoenixServerJoin(condition=[=($2, $4)], joinType=[inner])\n" +
                "        PhoenixServerProject(order_id=[$0], item_id=[$2], QUANTITY=[$4])\n" +
                "          PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n" +
                "        PhoenixClientAggregate(group=[{4}], EXPR$0=[MAX($1)], isOrdered=[false])\n" +
                "          PhoenixClientJoin(condition=[=($0, $2)], joinType=[inner])\n" +
                "            PhoenixServerSort(sort0=[$0], dir0=[ASC])\n" +
                "              PhoenixServerProject(item_id=[$2], QUANTITY=[$4])\n" +
                "                PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n" +
                "            PhoenixServerJoin(condition=[=($2, $1)], joinType=[inner])\n" +
                "              PhoenixServerProject(item_id=[$0], supplier_id=[$5])\n" +
                "                PhoenixTableScan(table=[[phoenix, Join, ItemTable]], scanOrder=[FORWARD])\n" +
                "              PhoenixServerAggregate(group=[{1}], isOrdered=[false])\n" +
                "                PhoenixServerJoin(condition=[=($2, $0)], joinType=[inner])\n" +
                "                  PhoenixServerProject(item_id=[$0], supplier_id=[$5])\n" +
                "                    PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                "                  PhoenixServerProject(item_id=[$2])\n" +
                "                    PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n";
        //start(correlProps).sql(q5).explainIs(p5Correlate).resultIs(0, r5).close();
        //TODO long-running query, disable for now.
        //start(decorrelProps).sql(q5).explainIs(p5Decorrelated).resultIs(false, r5).close();
        
        String q6 = "select organization_id, entity_id, a_integer from v v1 where a_integer = (select min(a_integer) from v v2 where v1.organization_id = v2.organization_id and v1.b_string = v2.b_string)";
        Object[][] r6 = new Object[][] {
                {"00D300000000XHP", "00A123122312312", 1}, 
                {"00D300000000XHP", "00A223122312312", 2}, 
                {"00D300000000XHP", "00A323122312312", 3}};
        String p6Correlate = 
                "PhoenixToEnumerableConverter\n" +
                "  PhoenixClientProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_INTEGER=[$3])\n" +
                "    PhoenixFilter(condition=[=($3, $4)])\n" +
                "      PhoenixCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{0, 2}])\n" +
                "        PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], B_STRING=[$3], A_INTEGER=[$4])\n" +
                "          PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n" +
                "        PhoenixServerAggregate(group=[{}], EXPR$0=[MIN($4)])\n" +
                "          PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[AND(=($2, 'a'), =($cor0.ORGANIZATION_ID, $0), =($cor0.B_STRING, $3))])\n";
        String p6Decorrelated = 
                "PhoenixToEnumerableConverter\n" +
                "  PhoenixClientProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_INTEGER=[$3])\n" +
                "    PhoenixServerJoin(condition=[AND(=($0, $4), =($2, $5), =($3, $6))], joinType=[inner])\n" +
                "      PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], B_STRING=[$3], A_INTEGER=[$4])\n" +
                "        PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n" +
                "      PhoenixServerAggregate(group=[{3, 4}], EXPR$0=[MIN($2)], isOrdered=[false])\n" +
                "        PhoenixServerJoin(condition=[AND(=($3, $0), =($4, $1))], joinType=[inner])\n" +
                "          PhoenixServerProject(ORGANIZATION_ID=[$0], B_STRING=[$3], A_INTEGER=[$4])\n" +
                "            PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n" +
                "          PhoenixServerAggregate(group=[{0, 3}], isOrdered=[false])\n" +
                "            PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n";
        start(correlProps).sql(q6).explainIs(p6Correlate).resultIs(0, r6).close();
        start(decorrelProps).sql(q6).explainIs(p6Decorrelated).resultIs(0, r6).close();
    }
    
    @Test public void testInValueList() throws Exception {
        start(false, 1000f).sql("select entity_id from aTable where organization_id = '00D300000000XHP' and entity_id in ('00A123122312312', '00A223122312312', '00B523122312312', '00B623122312312', '00C923122312312')")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixServerProject(ENTITY_ID=[$1])\n" +
                       "    PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[AND(=($0, '00D300000000XHP'), OR(=($1, '00A123122312312'), =($1, '00A223122312312'), =($1, '00B523122312312'), =($1, '00B623122312312'), =($1, '00C923122312312')))])\n")
            .resultIs(0, new Object[][] {
                    {"00A123122312312"},
                    {"00A223122312312"},
                    {"00B523122312312"},
                    {"00B623122312312"},
                    {"00C923122312312"}})
            .close();
    }
    
    @Test public void testSelectFromView() throws Exception {
        start(false, 1000f).sql("select * from v")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n")
                .resultIs(0, new Object[][] {
                        {"00D300000000XHP", "00A123122312312", "a"}, 
                        {"00D300000000XHP", "00A223122312312", "a"}, 
                        {"00D300000000XHP", "00A323122312312", "a"}, 
                        {"00D300000000XHP", "00A423122312312", "a"}})
                .close();
    }
    
    @Test public void testSequence() throws Exception {
        start(false, 1000f).sql("select NEXT VALUE FOR seq0, c0 from (values (1), (1)) as t(c0)")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(EXPR$0=[NEXT_VALUE('\"SEQ0\"')], C0=[$0])\n" +
                           "    PhoenixValues(tuples=[[{ 1 }, { 1 }]])\n")
                .resultIs(0, new Object[][]{
                        {1L, 1},
                        {2L, 1}})
                .close();

        start(false, 1000f).sql("select NEXT VALUE FOR my.seq1, entity_id from aTable where a_string = 'a'")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(EXPR$0=[NEXT_VALUE('\"MY\".\"SEQ1\"')], ENTITY_ID=[$1])\n" +
                           "    PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n")
                .resultIs(1, new Object[][]{
                        {2L, "00A123122312312"},
                        {4L, "00A223122312312"},
                        {6L, "00A323122312312"},
                        {8L, "00A423122312312"}})
                .close();

        start(false, 1000f).sql("select CURRENT VALUE FOR my.seq1, entity_id from aTable where a_string = 'a'")
                .explainIs("PhoenixToEnumerableConverter\n" +
                        "  PhoenixClientProject(EXPR$0=[CURRENT_VALUE('\"MY\".\"SEQ1\"')], ENTITY_ID=[$1])\n" +
                        "    PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n")
                .resultIs(1, new Object[][]{
                        {8L, "00A123122312312"},
                        {8L, "00A223122312312"},
                        {8L, "00A323122312312"},
                        {8L, "00A423122312312"}})
                .close();
        
        start(false, 1000f).sql("SELECT NEXT VALUE FOR seq0, item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\"")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(EXPR$0=[NEXT_VALUE('\"SEQ0\"')], item_id=[$0], NAME=[$1], supplier_id=[$3], NAME0=[$4])\n" +
                           "    PhoenixServerJoin(condition=[=($2, $3)], joinType=[inner])\n" +
                           "      PhoenixServerProject(item_id=[$0], NAME=[$1], supplier_id=[$5])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                           "      PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, SupplierTable]])\n")
                .resultIs(1, new Object[][] {
                        {3L, "0000000001", "T1", "0000000001", "S1"}, 
                        {4L, "0000000002", "T2", "0000000001", "S1"}, 
                        {5L, "0000000003", "T3", "0000000002", "S2"}, 
                        {6L, "0000000004", "T4", "0000000002", "S2"},
                        {7L, "0000000005", "T5", "0000000005", "S5"},
                        {8L, "0000000006", "T6", "0000000006", "S6"}})
                .close();

        start(false, 1000f).sql("select NEXT VALUE FOR my.seq1 + 1, entity_id from aTable where a_string = 'a'")
                .explainIs("PhoenixToEnumerableConverter\n" +
                        "  PhoenixClientProject(EXPR$0=[+(NEXT_VALUE('\"MY\".\"SEQ1\"'), 1)], ENTITY_ID=[$1])\n" +
                        "    PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n")
                .resultIs(1, new Object[][]{
                        {11L, "00A123122312312"},
                        {13L, "00A223122312312"},
                        {15L, "00A323122312312"},
                        {17L, "00A423122312312"}})
                .close();

        start(false, 1000f).sql("select (NEXT VALUE FOR my.seq1 * 2) + 1, entity_id from aTable where a_string = 'a'")
                .explainIs("PhoenixToEnumerableConverter\n" +
                        "  PhoenixClientProject(EXPR$0=[+(*(NEXT_VALUE('\"MY\".\"SEQ1\"'), 2), 1)], ENTITY_ID=[$1])\n" +
                        "    PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n")
                .resultIs(1, new Object[][]{
                        {37L, "00A123122312312"},
                        {41L, "00A223122312312"},
                        {45L, "00A323122312312"},
                        {49L, "00A423122312312"}})
                .close();
    }

    @Ignore // CALCITE-1045
    @Test public void testConnectJoinHsqldb() throws Exception {
        final Start start = new Start(getConnectionProps(false, 1000f)) {
            @Override
            Connection createConnection() throws Exception {
                return connectWithHsqldbUsingModel(props);
            }
        };
        start.sql("select the_year, quantity as q, (select count(*) cnt \n"
            + "from \"foodmart\".\"time_by_day\" t where t.\"the_year\" = c.the_year)\n"
            + "from " + JOIN_ORDER_TABLE_FULL_NAME + " c")
            .explainIs("EnumerableCalc(expr#0..8=[{inputs}], THE_YEAR=[$t6], Q=[$t4], EXPR$2=[$t8])\n" +
                       "  EnumerableJoin(condition=[=($6, $7)], joinType=[left])\n" +
                       "    PhoenixToEnumerableConverter\n" +
                       "      PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n" +
                       "    EnumerableAggregate(group=[{0}], agg#0=[SINGLE_VALUE($1)])\n" +
                       "      EnumerableAggregate(group=[{0}], CNT=[COUNT()])\n" +
                       "        EnumerableJoin(condition=[=($0, $11)], joinType=[inner])\n" +
                       "          PhoenixToEnumerableConverter\n" +
                       "            PhoenixServerAggregate(group=[{6}], isOrdered=[false])\n" +
                       "              PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n" +
                       "          JdbcToEnumerableConverter\n" +
                       "            JdbcProject(time_id=[$0], the_date=[$1], the_day=[$2], the_month=[$3], the_year=[$4], day_of_month=[$5], week_of_year=[$6], month_of_year=[$7], quarter=[$8], fiscal_period=[$9], the_year10=[CAST($4):INTEGER])\n" +
                       "              JdbcTableScan(table=[[foodmart, time_by_day]])\n")
            .resultIs(0, new Object[][] {
                    new Object[] {1997, 1000, 365L}, 
                    new Object[] {1997, 2000, 365L},
                    new Object[] {1997, 3000, 365L},
                    new Object[] {1998, 4000, 365L},
                    new Object[] {1998, 5000, 365L}})
            .close();;
    }

    @Test public void testConnectUsingModel() throws Exception {
        final Start start = new Start(getConnectionProps(false, 1000f)) {
            @Override
            Connection createConnection() throws Exception {
                return connectUsingModel(props);
            }
        };
        start.sql("select * from aTable")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixTableScan(table=[[HR, ATABLE]])\n")
            // .resultIs("Xx")
            .close();
    }
}
