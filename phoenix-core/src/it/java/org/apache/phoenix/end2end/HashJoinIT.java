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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;

@RunWith(Parameterized.class)
public class HashJoinIT extends BaseJoinIT {
    public HashJoinIT(String[] indexDDL, String[] plans) {
        super(indexDDL, plans);
    }
    
    @Parameters
    public static Collection<Object> data() {
        List<Object> testCases = Lists.newArrayList();
        testCases.add(new String[][] {
                {}, {
                /* 
                 * testLeftJoinWithAggregation()
                 *     SELECT i.name, sum(quantity) FROM joinOrderTable o 
                 *     LEFT JOIN joinItemTable i ON o.item_id = i.item_id 
                 *     GROUP BY i.name ORDER BY i.name
                 */     
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "    SERVER AGGREGATE INTO DISTINCT ROWS BY [I.NAME]\n" +
                "CLIENT MERGE SORT\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME,
                /* 
                 * testLeftJoinWithAggregation()
                 *     SELECT i.item_id iid, sum(quantity) q FROM joinOrderTable o 
                 *     LEFT JOIN joinItemTable i ON o.item_id = i.item_id 
                 *     GROUP BY i.item_id ORDER BY q DESC"
                 */     
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "    SERVER AGGREGATE INTO DISTINCT ROWS BY [\"I.item_id\"]\n" +
                "CLIENT MERGE SORT\n" +
                "CLIENT SORTED BY [SUM(O.QUANTITY) DESC]\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "            SERVER FILTER BY FIRST KEY ONLY",
                /* 
                 * testLeftJoinWithAggregation()
                 *     SELECT i.item_id iid, sum(quantity) q FROM joinItemTable i 
                 *     LEFT JOIN joinOrderTable o ON o.item_id = i.item_id 
                 *     GROUP BY i.item_id ORDER BY q DESC NULLS LAST, iid
                 */     
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "    SERVER FILTER BY FIRST KEY ONLY\n" +
                "    SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [\"I.item_id\"]\n" +
                "CLIENT SORTED BY [SUM(O.QUANTITY) DESC NULLS LAST, \"I.item_id\"]\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME,
                /* 
                 * testRightJoinWithAggregation()
                 *     SELECT i.name, sum(quantity) FROM joinOrderTable o 
                 *     RIGHT JOIN joinItemTable i ON o.item_id = i.item_id 
                 *     GROUP BY i.name ORDER BY i.name
                 */
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "    SERVER AGGREGATE INTO DISTINCT ROWS BY [I.NAME]\n" +
                "CLIENT MERGE SORT\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME,
                /*
                 * testRightJoinWithAggregation()
                 *     SELECT i.item_id iid, sum(quantity) q FROM joinOrderTable o 
                 *     RIGHT JOIN joinItemTable i ON o.item_id = i.item_id 
                 *     GROUP BY i.item_id ORDER BY q DESC NULLS LAST, iid
                 */
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "    SERVER FILTER BY FIRST KEY ONLY\n" +
                "    SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [\"I.item_id\"]\n" +
                "CLIENT SORTED BY [SUM(O.QUANTITY) DESC NULLS LAST, \"I.item_id\"]\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME,
                /*
                 * testJoinWithWildcard()
                 *     SELECT * FROM joinItemTable LEFT JOIN joinSupplierTable supp 
                 *     ON joinItemTable.supplier_id = supp.supplier_id 
                 *     ORDER BY item_id
                 */
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" + 
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SUPPLIER_TABLE_FULL_NAME,
                /*
                 * testJoinPlanWithIndex()
                 *     SELECT item.item_id, item.name, supp.supplier_id, supp.name 
                 *     FROM joinItemTable item LEFT JOIN joinSupplierTable supp 
                 *     ON substr(item.name, 2, 1) = substr(supp.name, 2, 1) 
                 *         AND (supp.name BETWEEN 'S1' AND 'S5') 
                 *     WHERE item.name BETWEEN 'T1' AND 'T5'
                 */
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "    SERVER FILTER BY (NAME >= 'T1' AND NAME <= 'T5')\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SUPPLIER_TABLE_FULL_NAME + "\n" +
                "            SERVER FILTER BY (NAME >= 'S1' AND NAME <= 'S5')",
                /*
                 * testJoinPlanWithIndex()
                 *     SELECT item.item_id, item.name, supp.supplier_id, supp.name 
                 *     FROM joinItemTable item INNER JOIN joinSupplierTable supp 
                 *     ON item.supplier_id = supp.supplier_id 
                 *     WHERE (item.name = 'T1' OR item.name = 'T5') 
                 *         AND (supp.name = 'S1' OR supp.name = 'S5')
                 */
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "    SERVER FILTER BY (NAME = 'T1' OR NAME = 'T5')\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SUPPLIER_TABLE_FULL_NAME + "\n" +
                "            SERVER FILTER BY (NAME = 'S1' OR NAME = 'S5')",
                /*
                 * testJoinWithSkipMergeOptimization()
                 *     SELECT s.name FROM joinItemTable i 
                 *     JOIN joinOrderTable o ON o.item_id = i.item_id AND quantity < 5000 
                 *     JOIN joinSupplierTable s ON i.supplier_id = s.supplier_id
                 */
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "    PARALLEL INNER-JOIN TABLE 0 (SKIP MERGE)\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "            SERVER FILTER BY QUANTITY < 5000\n" +
                "    PARALLEL INNER-JOIN TABLE 1\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SUPPLIER_TABLE_FULL_NAME + "\n" +
                "    DYNAMIC SERVER FILTER BY \"I.item_id\" IN (\"O.item_id\")",
                /*
                 * testSelfJoin()
                 *     SELECT i2.item_id, i1.name FROM joinItemTable i1 
                 *     JOIN joinItemTable i2 ON i1.item_id = i2.item_id 
                 *     ORDER BY i1.item_id
                 */
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "            SERVER FILTER BY FIRST KEY ONLY\n" +
                "    DYNAMIC SERVER FILTER BY \"I1.item_id\" IN (\"I2.item_id\")",
                /*
                 * testSelfJoin()
                 *     SELECT i1.name, i2.name FROM joinItemTable i1 
                 *     JOIN joinItemTable i2 ON i1.item_id = i2.supplier_id 
                 *     ORDER BY i1.name, i2.name
                 */
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "    SERVER SORTED BY [I1.NAME, I2.NAME]\n" +
                "CLIENT MERGE SORT\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "    DYNAMIC SERVER FILTER BY \"I1.item_id\" IN (\"I2.supplier_id\")",
                /*
                 * testStarJoin()
                 *     SELECT order_id, c.name, i.name iname, quantity, o.date 
                 *     FROM joinOrderTable o 
                 *     JOIN joinCustomerTable c ON o.customer_id = c.customer_id 
                 *     JOIN joinItemTable i ON o.item_id = i.item_id 
                 *     ORDER BY order_id
                 */
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_CUSTOMER_TABLE_FULL_NAME + "\n" +
                "    PARALLEL INNER-JOIN TABLE 1\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME,
                /*
                 * testStarJoin()
                 *     SELECT (*NO_STAR_JOIN*) order_id, c.name, i.name iname, quantity, o.date 
                 *     FROM joinOrderTable o 
                 *     JOIN joinCustomerTable c ON o.customer_id = c.customer_id 
                 *     JOIN joinItemTable i ON o.item_id = i.item_id 
                 *     ORDER BY order_id
                 */
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "    SERVER SORTED BY [\"O.order_id\"]\n" +
                "CLIENT MERGE SORT\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "            PARALLEL INNER-JOIN TABLE 0\n" +
                "                CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_CUSTOMER_TABLE_FULL_NAME + "\n" +
                "    DYNAMIC SERVER FILTER BY \"I.item_id\" IN (\"O.item_id\")",
                /*
                 * testSubJoin()
                 *     SELECT * FROM joinCustomerTable c 
                 *     INNER JOIN (joinOrderTable o 
                 *         INNER JOIN (joinSupplierTable s 
                 *             RIGHT JOIN joinItemTable i ON i.supplier_id = s.supplier_id)
                 *         ON o.item_id = i.item_id)
                 *     ON c.customer_id = o.customer_id
                 *     WHERE c.customer_id <= '0000000005' 
                 *         AND order_id != '000000000000003' 
                 *         AND i.name != 'T3' 
                 *     ORDER BY c.customer_id, i.name
                 */
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_CUSTOMER_TABLE_FULL_NAME + " [*] - ['0000000005']\n" +
                "    SERVER SORTED BY [\"C.customer_id\", I.NAME]\n" +
                "CLIENT MERGE SORT\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "            SERVER FILTER BY \"order_id\" != '000000000000003'\n" +
                "            PARALLEL INNER-JOIN TABLE 0\n" +
                "                CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "                    SERVER FILTER BY NAME != 'T3'\n" +
                "                    PARALLEL LEFT-JOIN TABLE 0\n" +
                "                        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SUPPLIER_TABLE_FULL_NAME + "\n" +
                "    DYNAMIC SERVER FILTER BY \"C.customer_id\" IN (\"O.customer_id\")",
                /* 
                 * testJoinWithSubqueryAndAggregation()
                 *     SELECT i.name, sum(quantity) FROM joinOrderTable o 
                 *     LEFT JOIN (SELECT name, item_id iid FROM joinItemTable) AS i 
                 *     ON o.item_id = i.iid 
                 *     GROUP BY i.name ORDER BY i.name
                 */     
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "    SERVER AGGREGATE INTO DISTINCT ROWS BY [I.NAME]\n" +
                "CLIENT MERGE SORT\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME,
                /* 
                 * testJoinWithSubqueryAndAggregation()
                 *     SELECT o.iid, sum(o.quantity) q 
                 *     FROM (SELECT item_id iid, quantity FROM joinOrderTable) AS o 
                 *     LEFT JOIN (SELECT item_id FROM joinItemTable) AS i 
                 *     ON o.iid = i.item_id 
                 *     GROUP BY o.iid ORDER BY q DESC                 
                 */     
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "    SERVER AGGREGATE INTO DISTINCT ROWS BY [O.IID]\n" +
                "CLIENT MERGE SORT\n" +
                "CLIENT SORTED BY [SUM(O.QUANTITY) DESC]\n" +
                "    PARALLEL LEFT-JOIN TABLE 0 (SKIP MERGE)\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "            SERVER FILTER BY FIRST KEY ONLY",
                /* 
                 * testJoinWithSubqueryAndAggregation()
                 *     SELECT i.iid, o.q 
                 *     FROM (SELECT item_id iid FROM joinItemTable) AS i 
                 *     LEFT JOIN (SELECT item_id iid, sum(quantity) q FROM joinOrderTable GROUP BY item_id) AS o 
                 *     ON o.iid = i.iid 
                 *     ORDER BY o.q DESC NULLS LAST, i.iid
                 */     
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "    SERVER FILTER BY FIRST KEY ONLY\n" +
                "    SERVER SORTED BY [O.Q DESC NULLS LAST, I.IID]\n" +
                "CLIENT MERGE SORT\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "            SERVER AGGREGATE INTO DISTINCT ROWS BY [\"item_id\"]\n" +
                "        CLIENT MERGE SORT",
                /* 
                 * testJoinWithSubqueryAndAggregation()
                 *     SELECT i.iid, o.q 
                 *     FROM (SELECT item_id iid, sum(quantity) q FROM joinOrderTable GROUP BY item_id) AS o 
                 *     JOIN (SELECT item_id iid FROM joinItemTable) AS i 
                 *     ON o.iid = i.iid 
                 *     ORDER BY o.q DESC, i.iid
                 */     
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "    SERVER FILTER BY FIRST KEY ONLY\n" +
                "    SERVER SORTED BY [O.Q DESC, I.IID]\n" +
                "CLIENT MERGE SORT\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "            SERVER AGGREGATE INTO DISTINCT ROWS BY [\"item_id\"]\n" +
                "        CLIENT MERGE SORT",
                /*
                 * testNestedSubqueries()
                 *     SELECT * FROM (SELECT customer_id cid, name, phone, address, loc_id, date FROM joinCustomerTable) AS c 
                 *     INNER JOIN (SELECT o.oid ooid, o.cid ocid, o.iid oiid, o.price * o.quantity, o.date odate, 
                 *     qi.iiid iiid, qi.iname iname, qi.iprice iprice, qi.idiscount1 idiscount1, qi.idiscount2 idiscount2, qi.isid isid, qi.idescription idescription, 
                 *     qi.ssid ssid, qi.sname sname, qi.sphone sphone, qi.saddress saddress, qi.sloc_id sloc_id 
                 *         FROM (SELECT item_id iid, customer_id cid, order_id oid, price, quantity, date FROM joinOrderTable) AS o 
                 *         INNER JOIN (SELECT i.iid iiid, i.name iname, i.price iprice, i.discount1 idiscount1, i.discount2 idiscount2, i.sid isid, i.description idescription, 
                 *         s.sid ssid, s.name sname, s.phone sphone, s.address saddress, s.loc_id sloc_id 
                 *             FROM (SELECT supplier_id sid, name, phone, address, loc_id FROM joinSupplierTable) AS s 
                 *             RIGHT JOIN (SELECT item_id iid, name, price, discount1, discount2, supplier_id sid, description FROM joinItemTable) AS i 
                 *             ON i.sid = s.sid) as qi 
                 *         ON o.iid = qi.iiid) as qo 
                 *     ON c.cid = qo.ocid 
                 *     WHERE c.cid <= '0000000005' 
                 *         AND qo.ooid != '000000000000003' 
                 *         AND qo.iname != 'T3' 
                 *     ORDER BY c.cid, qo.iname
                 */
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_CUSTOMER_TABLE_FULL_NAME + " [*] - ['0000000005']\n" +
                "    SERVER SORTED BY [C.CID, QO.INAME]\n" +
                "CLIENT MERGE SORT\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "            SERVER FILTER BY \"order_id\" != '000000000000003'\n" +
                "            PARALLEL INNER-JOIN TABLE 0\n" +
                "                CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "                    SERVER FILTER BY NAME != 'T3'\n" +
                "                    PARALLEL LEFT-JOIN TABLE 0\n" +
                "                        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SUPPLIER_TABLE_FULL_NAME,
                /*
                 * testJoinWithLimit()
                 *     SELECT order_id, i.name, s.name, s.address, quantity 
                 *     FROM joinSupplierTable s 
                 *     LEFT JOIN joinItemTable i ON i.supplier_id = s.supplier_id 
                 *     LEFT JOIN joinOrderTable o ON o.item_id = i.item_id LIMIT 4
                 */
                "CLIENT SERIAL 1-WAY FULL SCAN OVER " + JOIN_SUPPLIER_TABLE_FULL_NAME + "\n" +
                "    SERVER 4 ROW LIMIT\n" +
                "CLIENT 4 ROW LIMIT\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER "+ JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "    PARALLEL LEFT-JOIN TABLE 1(DELAYED EVALUATION)\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER "+ JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "    JOIN-SCANNER 4 ROW LIMIT",
                /*
                 * testJoinWithLimit()
                 *     SELECT order_id, i.name, s.name, s.address, quantity 
                 *     FROM joinSupplierTable s 
                 *     JOIN joinItemTable i ON i.supplier_id = s.supplier_id 
                 *     JOIN joinOrderTable o ON o.item_id = i.item_id LIMIT 4
                 */
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SUPPLIER_TABLE_FULL_NAME + "\n" +
                "CLIENT 4 ROW LIMIT\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER "+ JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "    PARALLEL INNER-JOIN TABLE 1(DELAYED EVALUATION)\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER "+ JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "    DYNAMIC SERVER FILTER BY \"S.supplier_id\" IN (\"I.supplier_id\")\n" +
                "    JOIN-SCANNER 4 ROW LIMIT",
                /*
                 * testJoinWithSetMaxRows()
                 *     statement.setMaxRows(4);
                 *     SELECT order_id, i.name, quantity FROM joinItemTable i
                 *     JOIN joinOrderTable o ON o.item_id = i.item_id;
                 *     SELECT o.order_id, i.name, o.quantity FROM joinItemTable i
                 *     JOIN (SELECT order_id, item_id, quantity FROM joinOrderTable) o
                 *     ON o.item_id = i.item_id;
                 */
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "CLIENT 4 ROW LIMIT\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER "+ JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "    DYNAMIC SERVER FILTER BY \"I.item_id\" IN (\"O.item_id\")\n" +
                "    JOIN-SCANNER 4 ROW LIMIT",
                /*
                 * testJoinWithOffset()
                 *     SELECT order_id, i.name, s.name, s.address, quantity 
                 *     FROM joinSupplierTable s 
                 *     LEFT JOIN joinItemTable i ON i.supplier_id = s.supplier_id 
                 *     LEFT JOIN joinOrderTable o ON o.item_id = i.item_id LIMIT 1 OFFSET 2
                 */
                "CLIENT SERIAL 1-WAY FULL SCAN OVER " + JOIN_SUPPLIER_TABLE_FULL_NAME + "\n" +
                "    SERVER OFFSET 2\n" +
                "    SERVER 3 ROW LIMIT\n" +
                "CLIENT 1 ROW LIMIT\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER "+ JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "    PARALLEL LEFT-JOIN TABLE 1(DELAYED EVALUATION)\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER "+ JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "    JOIN-SCANNER 3 ROW LIMIT",
                /*
                 * testJoinWithOffset()
                 *     SELECT order_id, i.name, s.name, s.address, quantity 
                 *     FROM joinSupplierTable s 
                 *     JOIN joinItemTable i ON i.supplier_id = s.supplier_id 
                 *     JOIN joinOrderTable o ON o.item_id = i.item_id LIMIT 1 OFFSET 2
                 */
                "CLIENT SERIAL 1-WAY FULL SCAN OVER " + JOIN_SUPPLIER_TABLE_FULL_NAME + "\n" +
                "    SERVER OFFSET 2\n" +
                "CLIENT 1 ROW LIMIT\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER "+ JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "    PARALLEL INNER-JOIN TABLE 1(DELAYED EVALUATION)\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER "+ JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "    DYNAMIC SERVER FILTER BY \"S.supplier_id\" IN (\"I.supplier_id\")\n" +
                "    JOIN-SCANNER 3 ROW LIMIT",
                }});
        testCases.add(new String[][] {
                {
                "CREATE INDEX \"idx_customer\" ON " + JOIN_CUSTOMER_TABLE_FULL_NAME + " (name)",
                "CREATE INDEX \"idx_item\" ON " + JOIN_ITEM_TABLE_FULL_NAME + " (name) INCLUDE (price, discount1, discount2, \"supplier_id\", description)",
                "CREATE INDEX \"idx_supplier\" ON " + JOIN_SUPPLIER_TABLE_FULL_NAME + " (name)"
                }, {
                /* 
                 * testLeftJoinWithAggregation()
                 *     SELECT i.name, sum(quantity) FROM joinOrderTable o 
                 *     LEFT JOIN joinItemTable i ON o.item_id = i.item_id 
                 *     GROUP BY i.name ORDER BY i.name
                 */     
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "    SERVER AGGREGATE INTO DISTINCT ROWS BY [\"I.0:NAME\"]\n" +
                "CLIENT MERGE SORT\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SCHEMA + ".idx_item\n" +
                "            SERVER FILTER BY FIRST KEY ONLY",
                /* 
                 * testLeftJoinWithAggregation()
                 *     SELECT i.item_id iid, sum(quantity) q FROM joinOrderTable o 
                 *     LEFT JOIN joinItemTable i ON o.item_id = i.item_id 
                 *     GROUP BY i.item_id ORDER BY q DESC"
                 */     
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "    SERVER AGGREGATE INTO DISTINCT ROWS BY [\"I.:item_id\"]\n" +
                "CLIENT MERGE SORT\n" +
                "CLIENT SORTED BY [SUM(O.QUANTITY) DESC]\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SCHEMA + ".idx_item\n" +
                "            SERVER FILTER BY FIRST KEY ONLY",
                /* 
                 * testLeftJoinWithAggregation()
                 *     SELECT i.item_id iid, sum(quantity) q FROM joinItemTable i 
                 *     LEFT JOIN joinOrderTable o ON o.item_id = i.item_id 
                 *     GROUP BY i.item_id ORDER BY q DESC NULLS LAST, iid
                 */     
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "    SERVER FILTER BY FIRST KEY ONLY\n" +
                "    SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [\"I.item_id\"]\n" +
                "CLIENT SORTED BY [SUM(O.QUANTITY) DESC NULLS LAST, \"I.item_id\"]\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME,
                /* 
                 * testRightJoinWithAggregation()
                 *     SELECT i.name, sum(quantity) FROM joinOrderTable o 
                 *     RIGHT JOIN joinItemTable i ON o.item_id = i.item_id 
                 *     GROUP BY i.name ORDER BY i.name
                 */
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SCHEMA + ".idx_item\n" +
                "    SERVER FILTER BY FIRST KEY ONLY\n" +
                "    SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [\"I.0:NAME\"]\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME,
                /*
                 * testRightJoinWithAggregation()
                 *     SELECT i.item_id iid, sum(quantity) q FROM joinOrderTable o 
                 *     RIGHT JOIN joinItemTable i ON o.item_id = i.item_id 
                 *     GROUP BY i.item_id ORDER BY q DESC NULLS LAST, iid
                 */
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "    SERVER FILTER BY FIRST KEY ONLY\n" +
                "    SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [\"I.item_id\"]\n" +
                "CLIENT SORTED BY [SUM(O.QUANTITY) DESC NULLS LAST, \"I.item_id\"]\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME,
                /*
                 * testJoinWithWildcard()
                 *     SELECT * FROM joinItemTable LEFT JOIN joinSupplierTable supp 
                 *     ON joinItemTable.supplier_id = supp.supplier_id 
                 *     ORDER BY item_id
                 */
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SUPPLIER_TABLE_FULL_NAME,
                /*
                 * testJoinPlanWithIndex()
                 *     SELECT item.item_id, item.name, supp.supplier_id, supp.name 
                 *     FROM joinItemTable item LEFT JOIN joinSupplierTable supp 
                 *     ON substr(item.name, 2, 1) = substr(supp.name, 2, 1) 
                 *         AND (supp.name BETWEEN 'S1' AND 'S5') 
                 *     WHERE item.name BETWEEN 'T1' AND 'T5'
                 */
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_SCHEMA + ".idx_item ['T1'] - ['T5']\n" +
                "    SERVER FILTER BY FIRST KEY ONLY\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_SCHEMA + ".idx_supplier ['S1'] - ['S5']\n" +
                "            SERVER FILTER BY FIRST KEY ONLY",
                /*
                 * testJoinPlanWithIndex()
                 *     SELECT item.item_id, item.name, supp.supplier_id, supp.name 
                 *     FROM joinItemTable item INNER JOIN joinSupplierTable supp 
                 *     ON item.supplier_id = supp.supplier_id 
                 *     WHERE (item.name = 'T1' OR item.name = 'T5') 
                 *         AND (supp.name = 'S1' OR supp.name = 'S5')
                 */
                "CLIENT PARALLEL 1-WAY SKIP SCAN ON 2 KEYS OVER " + JOIN_SCHEMA + ".idx_item ['T1'] - ['T5']\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY SKIP SCAN ON 2 KEYS OVER " + JOIN_SCHEMA + ".idx_supplier ['S1'] - ['S5']\n" +
                "            SERVER FILTER BY FIRST KEY ONLY",
                /*
                 * testJoinWithSkipMergeOptimization()
                 *     SELECT s.name FROM joinItemTable i 
                 *     JOIN joinOrderTable o ON o.item_id = i.item_id AND quantity < 5000 
                 *     JOIN joinSupplierTable s ON i.supplier_id = s.supplier_id
                 */
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SCHEMA + ".idx_item\n" +
                "    PARALLEL INNER-JOIN TABLE 0 (SKIP MERGE)\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "            SERVER FILTER BY QUANTITY < 5000\n" +
                "    PARALLEL INNER-JOIN TABLE 1\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SCHEMA + ".idx_supplier\n" +
                "            SERVER FILTER BY FIRST KEY ONLY",
                /*
                 * testSelfJoin()
                 *     SELECT i2.item_id, i1.name FROM joinItemTable i1 
                 *     JOIN joinItemTable i2 ON i1.item_id = i2.item_id 
                 *     ORDER BY i1.item_id
                 */
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SCHEMA + ".idx_item\n" +
                "            SERVER FILTER BY FIRST KEY ONLY\n" +
                "    DYNAMIC SERVER FILTER BY \"I1.item_id\" IN (\"I2.:item_id\")",
                /*
                 * testSelfJoin()
                 *     SELECT i1.name, i2.name FROM joinItemTable i1 
                 *     JOIN joinItemTable i2 ON i1.item_id = i2.supplier_id 
                 *     ORDER BY i1.name, i2.name
                 */
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SCHEMA + ".idx_item\n" +
                "    SERVER FILTER BY FIRST KEY ONLY\n" +
                "    SERVER SORTED BY [\"I1.0:NAME\", \"I2.0:NAME\"]\n" +
                "CLIENT MERGE SORT\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SCHEMA + ".idx_item",
                /*
                 * testStarJoin()
                 *     SELECT order_id, c.name, i.name iname, quantity, o.date 
                 *     FROM joinOrderTable o 
                 *     JOIN joinCustomerTable c ON o.customer_id = c.customer_id 
                 *     JOIN joinItemTable i ON o.item_id = i.item_id 
                 *     ORDER BY order_id
                 */
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SCHEMA + ".idx_customer\n" +
                "            SERVER FILTER BY FIRST KEY ONLY\n" + 
                "    PARALLEL INNER-JOIN TABLE 1\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SCHEMA + ".idx_item\n" +
                "            SERVER FILTER BY FIRST KEY ONLY",
                /*
                 * testStarJoin()
                 *     SELECT (*NO_STAR_JOIN*) order_id, c.name, i.name iname, quantity, o.date 
                 *     FROM joinOrderTable o 
                 *     JOIN joinCustomerTable c ON o.customer_id = c.customer_id 
                 *     JOIN joinItemTable i ON o.item_id = i.item_id 
                 *     ORDER BY order_id
                 */
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SCHEMA + ".idx_item\n" +
                "    SERVER FILTER BY FIRST KEY ONLY\n" +
                "    SERVER SORTED BY [\"O.order_id\"]\n" +
                "CLIENT MERGE SORT\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "            PARALLEL INNER-JOIN TABLE 0\n" +
                "                CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SCHEMA + ".idx_customer\n" +
                "                    SERVER FILTER BY FIRST KEY ONLY",
                /*
                 * testSubJoin()
                 *     SELECT * FROM joinCustomerTable c 
                 *     INNER JOIN (joinOrderTable o 
                 *         INNER JOIN (joinSupplierTable s 
                 *             RIGHT JOIN joinItemTable i ON i.supplier_id = s.supplier_id)
                 *         ON o.item_id = i.item_id)
                 *     ON c.customer_id = o.customer_id
                 *     WHERE c.customer_id <= '0000000005' 
                 *         AND order_id != '000000000000003' 
                 *         AND i.name != 'T3' 
                 *     ORDER BY c.customer_id, i.name
                 */
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_CUSTOMER_TABLE_FULL_NAME + " [*] - ['0000000005']\n" +
                "    SERVER SORTED BY [\"C.customer_id\", \"I.0:NAME\"]\n" +
                "CLIENT MERGE SORT\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "            SERVER FILTER BY \"order_id\" != '000000000000003'\n" +
                "            PARALLEL INNER-JOIN TABLE 0\n" +
                "                CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SCHEMA + ".idx_item\n" +
                "                    SERVER FILTER BY \"NAME\" != 'T3'\n" +
                "                    PARALLEL LEFT-JOIN TABLE 0\n" +
                "                        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SUPPLIER_TABLE_FULL_NAME + "\n" +
                "    DYNAMIC SERVER FILTER BY \"C.customer_id\" IN (\"O.customer_id\")",
                /* 
                 * testJoinWithSubqueryAndAggregation()
                 *     SELECT i.name, sum(quantity) FROM joinOrderTable o 
                 *     LEFT JOIN (SELECT name, item_id iid FROM joinItemTable) AS i 
                 *     ON o.item_id = i.iid 
                 *     GROUP BY i.name ORDER BY i.name
                 */     
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "    SERVER AGGREGATE INTO DISTINCT ROWS BY [I.NAME]\n" +
                "CLIENT MERGE SORT\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SCHEMA + ".idx_item\n" +
                "            SERVER FILTER BY FIRST KEY ONLY",
                /* 
                 * testJoinWithSubqueryAndAggregation()
                 *     SELECT o.iid, sum(o.quantity) q 
                 *     FROM (SELECT item_id iid, quantity FROM joinOrderTable) AS o 
                 *     LEFT JOIN (SELECT item_id FROM joinItemTable) AS i 
                 *     ON o.iid = i.item_id 
                 *     GROUP BY o.iid ORDER BY q DESC                 
                 */     
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "    SERVER AGGREGATE INTO DISTINCT ROWS BY [O.IID]\n" +
                "CLIENT MERGE SORT\n" +
                "CLIENT SORTED BY [SUM(O.QUANTITY) DESC]\n" +
                "    PARALLEL LEFT-JOIN TABLE 0 (SKIP MERGE)\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SCHEMA + ".idx_item\n" +
                "            SERVER FILTER BY FIRST KEY ONLY",
                /* 
                 * testJoinWithSubqueryAndAggregation()
                 *     SELECT i.iid, o.q 
                 *     FROM (SELECT item_id iid FROM joinItemTable) AS i 
                 *     LEFT JOIN (SELECT item_id iid, sum(quantity) q FROM joinOrderTable GROUP BY item_id) AS o 
                 *     ON o.iid = i.iid 
                 *     ORDER BY o.q DESC NULLS LAST, i.iid
                 */     
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SCHEMA + ".idx_item\n" +
                "    SERVER FILTER BY FIRST KEY ONLY\n" +
                "    SERVER SORTED BY [O.Q DESC NULLS LAST, I.IID]\n" +
                "CLIENT MERGE SORT\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "            SERVER AGGREGATE INTO DISTINCT ROWS BY [\"item_id\"]\n" +
                "        CLIENT MERGE SORT",
                /* 
                 * testJoinWithSubqueryAndAggregation()
                 *     SELECT i.iid, o.q 
                 *     FROM (SELECT item_id iid, sum(quantity) q FROM joinOrderTable GROUP BY item_id) AS o 
                 *     JOIN (SELECT item_id iid FROM joinItemTable) AS i 
                 *     ON o.iid = i.iid 
                 *     ORDER BY o.q DESC, i.iid
                 */     
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SCHEMA + ".idx_item\n" +
                "    SERVER FILTER BY FIRST KEY ONLY\n" +
                "    SERVER SORTED BY [O.Q DESC, I.IID]\n" +
                "CLIENT MERGE SORT\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "            SERVER AGGREGATE INTO DISTINCT ROWS BY [\"item_id\"]\n" +
                "        CLIENT MERGE SORT",
                /*
                 * testNestedSubqueries()
                 *     SELECT * FROM (SELECT customer_id cid, name, phone, address, loc_id, date FROM joinCustomerTable) AS c 
                 *     INNER JOIN (SELECT o.oid ooid, o.cid ocid, o.iid oiid, o.price * o.quantity, o.date odate, 
                 *     qi.iiid iiid, qi.iname iname, qi.iprice iprice, qi.idiscount1 idiscount1, qi.idiscount2 idiscount2, qi.isid isid, qi.idescription idescription, 
                 *     qi.ssid ssid, qi.sname sname, qi.sphone sphone, qi.saddress saddress, qi.sloc_id sloc_id 
                 *         FROM (SELECT item_id iid, customer_id cid, order_id oid, price, quantity, date FROM joinOrderTable) AS o 
                 *         INNER JOIN (SELECT i.iid iiid, i.name iname, i.price iprice, i.discount1 idiscount1, i.discount2 idiscount2, i.sid isid, i.description idescription, 
                 *         s.sid ssid, s.name sname, s.phone sphone, s.address saddress, s.loc_id sloc_id 
                 *             FROM (SELECT supplier_id sid, name, phone, address, loc_id FROM joinSupplierTable) AS s 
                 *             RIGHT JOIN (SELECT item_id iid, name, price, discount1, discount2, supplier_id sid, description FROM joinItemTable) AS i 
                 *             ON i.sid = s.sid) as qi 
                 *         ON o.iid = qi.iiid) as qo 
                 *     ON c.cid = qo.ocid 
                 *     WHERE c.cid <= '0000000005' 
                 *         AND qo.ooid != '000000000000003' 
                 *         AND qo.iname != 'T3' 
                 *     ORDER BY c.cid, qo.iname
                 */
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_CUSTOMER_TABLE_FULL_NAME + " [*] - ['0000000005']\n" +
                "    SERVER SORTED BY [C.CID, QO.INAME]\n" +
                "CLIENT MERGE SORT\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "            SERVER FILTER BY \"order_id\" != '000000000000003'\n" +
                "            PARALLEL INNER-JOIN TABLE 0\n" +
                "                CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SCHEMA + ".idx_item\n" +
                "                    SERVER FILTER BY \"NAME\" != 'T3'\n" +
                "                    PARALLEL LEFT-JOIN TABLE 0\n" +
                "                        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SUPPLIER_TABLE_FULL_NAME,
                /*
                 * testJoinWithLimit()
                 *     SELECT order_id, i.name, s.name, s.address, quantity 
                 *     FROM joinSupplierTable s 
                 *     LEFT JOIN joinItemTable i ON i.supplier_id = s.supplier_id 
                 *     LEFT JOIN joinOrderTable o ON o.item_id = i.item_id LIMIT 4
                 */
                "CLIENT SERIAL 1-WAY FULL SCAN OVER " + JOIN_SUPPLIER_TABLE_FULL_NAME + "\n" +
                "    SERVER 4 ROW LIMIT\n" +
                "CLIENT 4 ROW LIMIT\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER "+ JOIN_SCHEMA + ".idx_item\n" +
                "    PARALLEL LEFT-JOIN TABLE 1(DELAYED EVALUATION)\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER "+ JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "    JOIN-SCANNER 4 ROW LIMIT",
                /*
                 * testJoinWithLimit()
                 *     SELECT order_id, i.name, s.name, s.address, quantity 
                 *     FROM joinSupplierTable s 
                 *     JOIN joinItemTable i ON i.supplier_id = s.supplier_id 
                 *     JOIN joinOrderTable o ON o.item_id = i.item_id LIMIT 4
                 */
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SUPPLIER_TABLE_FULL_NAME + "\n" +
                "CLIENT 4 ROW LIMIT\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER "+ JOIN_SCHEMA + ".idx_item\n" +
                "    PARALLEL INNER-JOIN TABLE 1(DELAYED EVALUATION)\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER "+ JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "    DYNAMIC SERVER FILTER BY \"S.supplier_id\" IN (\"I.0:supplier_id\")\n" +
                "    JOIN-SCANNER 4 ROW LIMIT",
                /*
                 * testJoinWithSetMaxRows()
                 *     statement.setMaxRows(4);
                 *     SELECT order_id, i.name, quantity FROM joinItemTable i
                 *     JOIN joinOrderTable o ON o.item_id = i.item_id;
                 *     SELECT o.order_id, i.name, o.quantity FROM joinItemTable i
                 *     JOIN (SELECT order_id, item_id, quantity FROM joinOrderTable) o
                 *     ON o.item_id = i.item_id;
                 */
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER "+ JOIN_SCHEMA + ".idx_item\n" +
                "    SERVER FILTER BY FIRST KEY ONLY\n" +
                "CLIENT 4 ROW LIMIT\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER "+ JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "    JOIN-SCANNER 4 ROW LIMIT",
                /*
                 * testJoinWithLimit()
                 *     SELECT order_id, i.name, s.name, s.address, quantity 
                 *     FROM joinSupplierTable s 
                 *     LEFT JOIN joinItemTable i ON i.supplier_id = s.supplier_id 
                 *     LEFT JOIN joinOrderTable o ON o.item_id = i.item_id LIMIT 1 OFFSET 2
                 */
                "CLIENT SERIAL 1-WAY FULL SCAN OVER " + JOIN_SUPPLIER_TABLE_FULL_NAME + "\n" +
                "    SERVER OFFSET 2\n" +
                "    SERVER 3 ROW LIMIT\n" +
                "CLIENT 1 ROW LIMIT\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER "+ JOIN_SCHEMA + ".idx_item\n" +
                "    PARALLEL LEFT-JOIN TABLE 1(DELAYED EVALUATION)\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER "+ JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "    JOIN-SCANNER 3 ROW LIMIT",
                /*
                 * testJoinWithLimit()
                 *     SELECT order_id, i.name, s.name, s.address, quantity 
                 *     FROM joinSupplierTable s 
                 *     JOIN joinItemTable i ON i.supplier_id = s.supplier_id 
                 *     JOIN joinOrderTable o ON o.item_id = i.item_id LIMIT 1 OFFSET 2
                 */
                "CLIENT SERIAL 1-WAY FULL SCAN OVER " + JOIN_SUPPLIER_TABLE_FULL_NAME + "\n" +
                "    SERVER OFFSET 2\n" +
                "CLIENT 1 ROW LIMIT\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER "+ JOIN_SCHEMA + ".idx_item\n" +
                "    PARALLEL INNER-JOIN TABLE 1(DELAYED EVALUATION)\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER "+ JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "    DYNAMIC SERVER FILTER BY \"S.supplier_id\" IN (\"I.0:supplier_id\")\n" +
                "    JOIN-SCANNER 3 ROW LIMIT",
                }});
        testCases.add(new String[][] {
                {
                "CREATE LOCAL INDEX \"idx_customer\" ON " + JOIN_CUSTOMER_TABLE_FULL_NAME + " (name)",
                "CREATE LOCAL INDEX \"idx_item\" ON " + JOIN_ITEM_TABLE_FULL_NAME + " (name) INCLUDE (price, discount1, discount2, \"supplier_id\", description)",
                "CREATE LOCAL INDEX \"idx_supplier\" ON " + JOIN_SUPPLIER_TABLE_FULL_NAME + " (name)"
                }, {
                /* 
                 * testLeftJoinWithAggregation()
                 *     SELECT i.name, sum(quantity) FROM joinOrderTable o 
                 *     LEFT JOIN joinItemTable i ON o.item_id = i.item_id 
                 *     GROUP BY i.name ORDER BY i.name
                 */     
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "    SERVER AGGREGATE INTO DISTINCT ROWS BY [\"I.0:NAME\"]\n" +
                "CLIENT MERGE SORT\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " +JOIN_ITEM_TABLE_FULL_NAME +" [1]\n" +
                "            SERVER FILTER BY FIRST KEY ONLY\n" +
                "        CLIENT MERGE SORT",
                /* 
                 * testLeftJoinWithAggregation()
                 *     SELECT i.item_id iid, sum(quantity) q FROM joinOrderTable o 
                 *     LEFT JOIN joinItemTable i ON o.item_id = i.item_id 
                 *     GROUP BY i.item_id ORDER BY q DESC"
                 */     
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "    SERVER AGGREGATE INTO DISTINCT ROWS BY [\"I.:item_id\"]\n" +
                "CLIENT MERGE SORT\n" +
                "CLIENT SORTED BY [SUM(O.QUANTITY) DESC]\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME +" [1]\n" +
                "            SERVER FILTER BY FIRST KEY ONLY\n" +
                "        CLIENT MERGE SORT",          
                /* 
                 * testLeftJoinWithAggregation()
                 *     SELECT i.item_id iid, sum(quantity) q FROM joinItemTable i 
                 *     LEFT JOIN joinOrderTable o ON o.item_id = i.item_id 
                 *     GROUP BY i.item_id ORDER BY q DESC NULLS LAST, iid
                 */     
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "    SERVER FILTER BY FIRST KEY ONLY\n" +
                "    SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [\"I.item_id\"]\n" +
                "CLIENT SORTED BY [SUM(O.QUANTITY) DESC NULLS LAST, \"I.item_id\"]\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME,
                /* 
                 * testRightJoinWithAggregation()
                 *     SELECT i.name, sum(quantity) FROM joinOrderTable o 
                 *     RIGHT JOIN joinItemTable i ON o.item_id = i.item_id 
                 *     GROUP BY i.name ORDER BY i.name
                 */
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER "+ JOIN_ITEM_TABLE_FULL_NAME+" [1]\n" +
                "    SERVER FILTER BY FIRST KEY ONLY\n" +
                "    SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [\"I.0:NAME\"]\n" +
                "CLIENT MERGE SORT\n" + 
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME,
                /*
                 * testRightJoinWithAggregation()
                 *     SELECT i.item_id iid, sum(quantity) q FROM joinOrderTable o 
                 *     RIGHT JOIN joinItemTable i ON o.item_id = i.item_id 
                 *     GROUP BY i.item_id ORDER BY q DESC NULLS LAST, iid
                 */
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "    SERVER FILTER BY FIRST KEY ONLY\n" +
                "    SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [\"I.item_id\"]\n" +
                "CLIENT SORTED BY [SUM(O.QUANTITY) DESC NULLS LAST, \"I.item_id\"]\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME,
                /*
                 * testJoinWithWildcard()
                 *     SELECT * FROM joinItemTable LEFT JOIN joinSupplierTable supp 
                 *     ON joinItemTable.supplier_id = supp.supplier_id 
                 *     ORDER BY item_id
                 */
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SUPPLIER_TABLE_FULL_NAME,
                /*
                 * testJoinPlanWithIndex()
                 *     SELECT item.item_id, item.name, supp.supplier_id, supp.name 
                 *     FROM joinItemTable item LEFT JOIN joinSupplierTable supp 
                 *     ON substr(item.name, 2, 1) = substr(supp.name, 2, 1) 
                 *         AND (supp.name BETWEEN 'S1' AND 'S5') 
                 *     WHERE item.name BETWEEN 'T1' AND 'T5'
                 */
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + " [1,'T1'] - [1,'T5']\n" +
                "    SERVER FILTER BY FIRST KEY ONLY\n" +
                "CLIENT MERGE SORT\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_SUPPLIER_TABLE_FULL_NAME +" [1,'S1'] - [1,'S5']\n" +
                "            SERVER FILTER BY FIRST KEY ONLY\n" + 
                "        CLIENT MERGE SORT",
                /*
                 * testJoinPlanWithIndex()
                 *     SELECT item.item_id, item.name, supp.supplier_id, supp.name 
                 *     FROM joinItemTable item INNER JOIN joinSupplierTable supp 
                 *     ON item.supplier_id = supp.supplier_id 
                 *     WHERE (item.name = 'T1' OR item.name = 'T5') 
                 *         AND (supp.name = 'S1' OR supp.name = 'S5')
                 */
                "CLIENT PARALLEL 1-WAY SKIP SCAN ON 2 KEYS OVER " + JOIN_ITEM_TABLE_FULL_NAME + " [1,'T1'] - [1,'T5']\n" +
                "CLIENT MERGE SORT\n" + 
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY SKIP SCAN ON 2 KEYS OVER " + JOIN_SUPPLIER_TABLE_FULL_NAME +" [1,'S1'] - [1,'S5']\n" + 
                "            SERVER FILTER BY FIRST KEY ONLY\n" + 
                "        CLIENT MERGE SORT",
                /*
                 * testJoinWithSkipMergeOptimization()
                 *     SELECT s.name FROM joinItemTable i 
                 *     JOIN joinOrderTable o ON o.item_id = i.item_id AND quantity < 5000 
                 *     JOIN joinSupplierTable s ON i.supplier_id = s.supplier_id
                 */
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " +JOIN_ITEM_TABLE_FULL_NAME + " [1]\n" +
                "CLIENT MERGE SORT\n" + 
                "    PARALLEL INNER-JOIN TABLE 0 (SKIP MERGE)\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "            SERVER FILTER BY QUANTITY < 5000\n" +
                "    PARALLEL INNER-JOIN TABLE 1\n" +
                "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_SUPPLIER_TABLE_FULL_NAME + " [1]\n" +
                "            SERVER FILTER BY FIRST KEY ONLY\n" + 
                "        CLIENT MERGE SORT\n" +
                "    DYNAMIC SERVER FILTER BY \"I.:item_id\" IN (\"O.item_id\")",
                /*
                 * testSelfJoin()
                 *     SELECT i2.item_id, i1.name FROM joinItemTable i1 
                 *     JOIN joinItemTable i2 ON i1.item_id = i2.item_id 
                 *     ORDER BY i1.item_id
                 */
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER "+ JOIN_ITEM_TABLE_FULL_NAME +" [1]\n"  +
                "            SERVER FILTER BY FIRST KEY ONLY\n" +
                "        CLIENT MERGE SORT\n" +
                "    DYNAMIC SERVER FILTER BY \"I1.item_id\" IN (\"I2.:item_id\")",
                /*
                 * testSelfJoin()
                 *     SELECT i1.name, i2.name FROM joinItemTable i1 
                 *     JOIN joinItemTable i2 ON i1.item_id = i2.supplier_id 
                 *     ORDER BY i1.name, i2.name
                 */
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME +" [1]\n"  +
                "    SERVER FILTER BY FIRST KEY ONLY\n" +
                "    SERVER SORTED BY [\"I1.0:NAME\", \"I2.0:NAME\"]\n" +
                "CLIENT MERGE SORT\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME +" [1]\n" +
                "        CLIENT MERGE SORT\n" +
                "    DYNAMIC SERVER FILTER BY \"I1.:item_id\" IN (\"I2.0:supplier_id\")",
                /*
                 * testStarJoin()
                 *     SELECT order_id, c.name, i.name iname, quantity, o.date 
                 *     FROM joinOrderTable o 
                 *     JOIN joinCustomerTable c ON o.customer_id = c.customer_id 
                 *     JOIN joinItemTable i ON o.item_id = i.item_id 
                 *     ORDER BY order_id
                 */
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_CUSTOMER_TABLE_FULL_NAME + " [1]\n" +
                "            SERVER FILTER BY FIRST KEY ONLY\n" + 
                "        CLIENT MERGE SORT\n" +
                "    PARALLEL INNER-JOIN TABLE 1\n" +
                "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + " [1]\n" +
                "            SERVER FILTER BY FIRST KEY ONLY\n" +
                "        CLIENT MERGE SORT",
                /*
                 * testStarJoin()
                 *     SELECT (*NO_STAR_JOIN*) order_id, c.name, i.name iname, quantity, o.date 
                 *     FROM joinOrderTable o 
                 *     JOIN joinCustomerTable c ON o.customer_id = c.customer_id 
                 *     JOIN joinItemTable i ON o.item_id = i.item_id 
                 *     ORDER BY order_id
                 */
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + " [1]\n" +
                "    SERVER FILTER BY FIRST KEY ONLY\n" +
                "    SERVER SORTED BY [\"O.order_id\"]\n"+
                "CLIENT MERGE SORT\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "            PARALLEL INNER-JOIN TABLE 0\n" +
                "                CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_CUSTOMER_TABLE_FULL_NAME+" [1]\n"+
                "                    SERVER FILTER BY FIRST KEY ONLY\n" + 
                "                CLIENT MERGE SORT\n" +
                "    DYNAMIC SERVER FILTER BY \"I.:item_id\" IN (\"O.item_id\")",
                /*
                 * testSubJoin()
                 *     SELECT * FROM joinCustomerTable c 
                 *     INNER JOIN (joinOrderTable o 
                 *         INNER JOIN (joinSupplierTable s 
                 *             RIGHT JOIN joinItemTable i ON i.supplier_id = s.supplier_id)
                 *         ON o.item_id = i.item_id)
                 *     ON c.customer_id = o.customer_id
                 *     WHERE c.customer_id <= '0000000005' 
                 *         AND order_id != '000000000000003' 
                 *         AND i.name != 'T3' 
                 *     ORDER BY c.customer_id, i.name
                 */
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_CUSTOMER_TABLE_FULL_NAME + " [*] - ['0000000005']\n" +
                "    SERVER SORTED BY [\"C.customer_id\", \"I.0:NAME\"]\n"+
                "CLIENT MERGE SORT\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "            SERVER FILTER BY \"order_id\" != '000000000000003'\n" +
                "            PARALLEL INNER-JOIN TABLE 0\n" +
                "                CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME +" [1]\n" +
                "                    SERVER FILTER BY \"NAME\" != 'T3'\n" +
                "                CLIENT MERGE SORT\n" +
                "                    PARALLEL LEFT-JOIN TABLE 0\n" +
                "                        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SUPPLIER_TABLE_FULL_NAME + "\n" +
                "    DYNAMIC SERVER FILTER BY \"C.customer_id\" IN (\"O.customer_id\")",
                /* 
                 * testJoinWithSubqueryAndAggregation()
                 *     SELECT i.name, sum(quantity) FROM joinOrderTable o 
                 *     LEFT JOIN (SELECT name, item_id iid FROM joinItemTable) AS i 
                 *     ON o.item_id = i.iid 
                 *     GROUP BY i.name ORDER BY i.name
                 */     
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "    SERVER AGGREGATE INTO DISTINCT ROWS BY [I.NAME]\n" +
                "CLIENT MERGE SORT\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " +JOIN_ITEM_TABLE_FULL_NAME+" [1]\n"+
                "            SERVER FILTER BY FIRST KEY ONLY\n" +
                "        CLIENT MERGE SORT",
                /* 
                 * testJoinWithSubqueryAndAggregation()
                 *     SELECT o.iid, sum(o.quantity) q 
                 *     FROM (SELECT item_id iid, quantity FROM joinOrderTable) AS o 
                 *     LEFT JOIN (SELECT item_id FROM joinItemTable) AS i 
                 *     ON o.iid = i.item_id 
                 *     GROUP BY o.iid ORDER BY q DESC                 
                 */     
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "    SERVER AGGREGATE INTO DISTINCT ROWS BY [O.IID]\n" +
                "CLIENT MERGE SORT\n" +
                "CLIENT SORTED BY [SUM(O.QUANTITY) DESC]\n" +
                "    PARALLEL LEFT-JOIN TABLE 0 (SKIP MERGE)\n" +
                "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " +JOIN_ITEM_TABLE_FULL_NAME + " [1]\n" +
                "            SERVER FILTER BY FIRST KEY ONLY\n" +
                "        CLIENT MERGE SORT",
                /* 
                 * testJoinWithSubqueryAndAggregation()
                 *     SELECT i.iid, o.q 
                 *     FROM (SELECT item_id iid FROM joinItemTable) AS i 
                 *     LEFT JOIN (SELECT item_id iid, sum(quantity) q FROM joinOrderTable GROUP BY item_id) AS o 
                 *     ON o.iid = i.iid 
                 *     ORDER BY o.q DESC NULLS LAST, i.iid
                 */     
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + " [1]\n" +
                "    SERVER FILTER BY FIRST KEY ONLY\n" +
                "    SERVER SORTED BY [O.Q DESC NULLS LAST, I.IID]\n"+
                "CLIENT MERGE SORT\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "            SERVER AGGREGATE INTO DISTINCT ROWS BY [\"item_id\"]\n" +
                "        CLIENT MERGE SORT",
                /* 
                 * testJoinWithSubqueryAndAggregation()
                 *     SELECT i.iid, o.q 
                 *     FROM (SELECT item_id iid, sum(quantity) q FROM joinOrderTable GROUP BY item_id) AS o 
                 *     JOIN (SELECT item_id iid FROM joinItemTable) AS i 
                 *     ON o.iid = i.iid 
                 *     ORDER BY o.q DESC, i.iid
                 */     
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + " [1]\n" +
                "    SERVER FILTER BY FIRST KEY ONLY\n" +
                "    SERVER SORTED BY [O.Q DESC, I.IID]\n"+
                "CLIENT MERGE SORT\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "            SERVER AGGREGATE INTO DISTINCT ROWS BY [\"item_id\"]\n" +
                "        CLIENT MERGE SORT",
                /*
                 * testNestedSubqueries()
                 *     SELECT * FROM (SELECT customer_id cid, name, phone, address, loc_id, date FROM joinCustomerTable) AS c 
                 *     INNER JOIN (SELECT o.oid ooid, o.cid ocid, o.iid oiid, o.price * o.quantity, o.date odate, 
                 *     qi.iiid iiid, qi.iname iname, qi.iprice iprice, qi.idiscount1 idiscount1, qi.idiscount2 idiscount2, qi.isid isid, qi.idescription idescription, 
                 *     qi.ssid ssid, qi.sname sname, qi.sphone sphone, qi.saddress saddress, qi.sloc_id sloc_id 
                 *         FROM (SELECT item_id iid, customer_id cid, order_id oid, price, quantity, date FROM joinOrderTable) AS o 
                 *         INNER JOIN (SELECT i.iid iiid, i.name iname, i.price iprice, i.discount1 idiscount1, i.discount2 idiscount2, i.sid isid, i.description idescription, 
                 *         s.sid ssid, s.name sname, s.phone sphone, s.address saddress, s.loc_id sloc_id 
                 *             FROM (SELECT supplier_id sid, name, phone, address, loc_id FROM joinSupplierTable) AS s 
                 *             RIGHT JOIN (SELECT item_id iid, name, price, discount1, discount2, supplier_id sid, description FROM joinItemTable) AS i 
                 *             ON i.sid = s.sid) as qi 
                 *         ON o.iid = qi.iiid) as qo 
                 *     ON c.cid = qo.ocid 
                 *     WHERE c.cid <= '0000000005' 
                 *         AND qo.ooid != '000000000000003' 
                 *         AND qo.iname != 'T3' 
                 *     ORDER BY c.cid, qo.iname
                 */
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_CUSTOMER_TABLE_FULL_NAME + " [*] - ['0000000005']\n" +
                "    SERVER SORTED BY [C.CID, QO.INAME]\n" +
                "CLIENT MERGE SORT\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "            SERVER FILTER BY \"order_id\" != '000000000000003'\n" +
                "            PARALLEL INNER-JOIN TABLE 0\n" +
                "                CLIENT PARALLEL 1-WAY RANGE SCAN OVER " +  JOIN_ITEM_TABLE_FULL_NAME + " [1]\n" +
                "                    SERVER FILTER BY \"NAME\" != 'T3'\n" +
                "                CLIENT MERGE SORT\n" +      
                "                    PARALLEL LEFT-JOIN TABLE 0\n" +
                "                        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SUPPLIER_TABLE_FULL_NAME,
                /*
                 * testJoinWithLimit()
                 *     SELECT order_id, i.name, s.name, s.address, quantity 
                 *     FROM joinSupplierTable s 
                 *     LEFT JOIN joinItemTable i ON i.supplier_id = s.supplier_id 
                 *     LEFT JOIN joinOrderTable o ON o.item_id = i.item_id LIMIT 4
                 */
                "CLIENT SERIAL 1-WAY FULL SCAN OVER " + JOIN_SUPPLIER_TABLE_FULL_NAME + "\n" +
                "    SERVER 4 ROW LIMIT\n" +
                "CLIENT 4 ROW LIMIT\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER "+ JOIN_ITEM_TABLE_FULL_NAME + " [1]\n" +
                "        CLIENT MERGE SORT\n" +      
                "    PARALLEL LEFT-JOIN TABLE 1(DELAYED EVALUATION)\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER "+ JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "    JOIN-SCANNER 4 ROW LIMIT",
                /*
                 * testJoinWithLimit()
                 *     SELECT order_id, i.name, s.name, s.address, quantity 
                 *     FROM joinSupplierTable s 
                 *     JOIN joinItemTable i ON i.supplier_id = s.supplier_id 
                 *     JOIN joinOrderTable o ON o.item_id = i.item_id LIMIT 4
                 */
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SUPPLIER_TABLE_FULL_NAME + "\n" +
                "CLIENT 4 ROW LIMIT\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER "+ JOIN_ITEM_TABLE_FULL_NAME + " [1]\n" +
                "        CLIENT MERGE SORT\n" +
                "    PARALLEL INNER-JOIN TABLE 1(DELAYED EVALUATION)\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER "+ JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "    DYNAMIC SERVER FILTER BY \"S.supplier_id\" IN (\"I.0:supplier_id\")\n" +
                "    JOIN-SCANNER 4 ROW LIMIT",
                /*
                 * testJoinWithSetMaxRows()
                 *     statement.setMaxRows(4);
                 *     SELECT order_id, i.name, quantity FROM joinItemTable i
                 *     JOIN joinOrderTable o ON o.item_id = i.item_id;
                 *     SELECT o.order_id, i.name, o.quantity FROM joinItemTable i
                 *     JOIN (SELECT order_id, item_id, quantity FROM joinOrderTable) o
                 *     ON o.item_id = i.item_id;
                 */
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + " [1]\n" +
                "    SERVER FILTER BY FIRST KEY ONLY\n" +
                "CLIENT MERGE SORT\n" +
                "CLIENT 4 ROW LIMIT\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "    DYNAMIC SERVER FILTER BY \"I.:item_id\" IN (\"O.item_id\")\n" +
                "    JOIN-SCANNER 4 ROW LIMIT",
                /*
                 * testJoinWithOffset()
                 *     SELECT order_id, i.name, s.name, s.address, quantity 
                 *     FROM joinSupplierTable s 
                 *     LEFT JOIN joinItemTable i ON i.supplier_id = s.supplier_id 
                 *     LEFT JOIN joinOrderTable o ON o.item_id = i.item_id LIMIT 1 OFFSET 2
                 */
                "CLIENT SERIAL 1-WAY FULL SCAN OVER " + JOIN_SUPPLIER_TABLE_FULL_NAME + "\n" +
                "    SERVER OFFSET 2\n" +
                "    SERVER 3 ROW LIMIT\n" +
                "CLIENT 1 ROW LIMIT\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER "+ JOIN_ITEM_TABLE_FULL_NAME + " [1]\n" +
                "        CLIENT MERGE SORT\n" +      
                "    PARALLEL LEFT-JOIN TABLE 1(DELAYED EVALUATION)\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER "+ JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "    JOIN-SCANNER 3 ROW LIMIT",
                /*
                 * testJoinWithOffset()
                 *     SELECT order_id, i.name, s.name, s.address, quantity 
                 *     FROM joinSupplierTable s 
                 *     JOIN joinItemTable i ON i.supplier_id = s.supplier_id 
                 *     JOIN joinOrderTable o ON o.item_id = i.item_id LIMIT 1 OFFSET 2
                 */
                "CLIENT SERIAL 1-WAY FULL SCAN OVER " + JOIN_SUPPLIER_TABLE_FULL_NAME + "\n" +
                "    SERVER OFFSET 2\n" +
                "CLIENT 1 ROW LIMIT\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER "+ JOIN_ITEM_TABLE_FULL_NAME + " [1]\n" +
                "        CLIENT MERGE SORT\n" +
                "    PARALLEL INNER-JOIN TABLE 1(DELAYED EVALUATION)\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER "+ JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "    DYNAMIC SERVER FILTER BY \"S.supplier_id\" IN (\"I.0:supplier_id\")\n" +
                "    JOIN-SCANNER 3 ROW LIMIT",
                }});
        return testCases;
    }
    
    
    @Test
    public void testDefaultJoin() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName1 = getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME);
        String tableName2 = getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME);
        String query = "SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + tableName1 + " item JOIN " + tableName2 + " supp ON item.\"supplier_id\" = supp.\"supplier_id\"";
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName1 = getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME);
        String tableName2 = getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME);
        String query = "SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name, next value for " + seqName + " FROM " + tableName1 + " item INNER JOIN " + tableName2 + " supp ON item.\"supplier_id\" = supp.\"supplier_id\"";
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName1 = getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME);
        String tableName2 = getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME);
        String query[] = new String[3];
        query[0] = "SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name, next value for " + seqName + " FROM " + tableName1 + " item LEFT JOIN " + tableName2 + " supp ON item.\"supplier_id\" = supp.\"supplier_id\" ORDER BY \"item_id\"";
        query[1] = "SELECT " + tableName1 + ".\"item_id\", " + tableName1 + ".name, " + tableName2 + ".\"supplier_id\", " + tableName2 + ".name, next value for " + seqName + " FROM " + tableName1 + " LEFT JOIN " + tableName2 + " ON " + tableName1 + ".\"supplier_id\" = " + tableName2 + ".\"supplier_id\" ORDER BY \"item_id\"";
        query[2] = "SELECT item.\"item_id\", " + tableName1 + ".name, supp.\"supplier_id\", " + tableName2 + ".name, next value for " + seqName + " FROM " + tableName1 + " item LEFT JOIN " + tableName2 + " supp ON " + tableName1 + ".\"supplier_id\" = supp.\"supplier_id\" ORDER BY \"item_id\"";
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName1 = getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME);
        String tableName2 = getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME);
        String query = "SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + tableName1 + " supp RIGHT JOIN " + tableName2 + " item ON item.\"supplier_id\" = supp.\"supplier_id\" ORDER BY \"item_id\"";
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName1 = getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME);
        String tableName2 = getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME);
        String query1 = "SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + tableName1 + " item INNER JOIN " + tableName2 + " supp ON item.\"supplier_id\" = supp.\"supplier_id\" AND supp.\"supplier_id\" BETWEEN '0000000001' AND '0000000005'";
        String query2 = "SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + tableName1 + " item INNER JOIN " + tableName2 + " supp ON item.\"supplier_id\" = supp.\"supplier_id\" AND (supp.\"supplier_id\" = '0000000001' OR supp.\"supplier_id\" = '0000000005')";
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName1 = getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME);
        String tableName2 = getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME);
        String query = "SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + tableName1 + " item LEFT JOIN " + tableName2 + " supp ON item.\"supplier_id\" = supp.\"supplier_id\" AND (supp.\"supplier_id\" = '0000000001' OR supp.\"supplier_id\" = '0000000005') ORDER BY \"item_id\"";
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName1 = getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME);
        String tableName2 = getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME);
        String query1 = "SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + tableName1 + " supp RIGHT JOIN " + tableName2 + " item ON item.\"supplier_id\" = supp.\"supplier_id\" WHERE supp.\"supplier_id\" BETWEEN '0000000001' AND '0000000005'";
        String query2 = "SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + tableName2 + " item LEFT JOIN " + tableName1 + " supp ON item.\"supplier_id\" = supp.\"supplier_id\" WHERE supp.\"supplier_id\" = '0000000001' OR supp.\"supplier_id\" = '0000000005'";
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName1 = getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME);
        String tableName2 = getTableName(conn, JOIN_CUSTOMER_TABLE_FULL_NAME);
        String tableName3 = getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME);
        String[] query = new String[5];
        query[0] = "SELECT \"order_id\", c.name, i.name iname, quantity, o.\"DATE\" FROM " + tableName1 + " o JOIN "
            + tableName2 + " c ON o.\"customer_id\" = c.\"customer_id\" JOIN " 
            + tableName3 + " i ON o.\"item_id\" = i.\"item_id\" ORDER BY \"order_id\"";
        query[1] = "SELECT \"order_id\", c.name, i.name iname, quantity, o.\"DATE\" FROM " + tableName1 + " o, "
                + tableName2 + " c, " 
                + tableName3 + " i WHERE o.\"item_id\" = i.\"item_id\" AND o.\"customer_id\" = c.\"customer_id\" ORDER BY \"order_id\"";
        query[2] = "SELECT /*+ NO_STAR_JOIN*/ \"order_id\", c.name, i.name iname, quantity, o.\"DATE\" FROM " + tableName1 + " o JOIN "
                + tableName2 + " c ON o.\"customer_id\" = c.\"customer_id\" JOIN " 
                + tableName3 + " i ON o.\"item_id\" = i.\"item_id\" ORDER BY \"order_id\"";
        query[3] = "SELECT /*+ NO_STAR_JOIN*/  \"order_id\", c.name, i.name iname, quantity, o.\"DATE\" FROM (" + tableName1 + " o, "
                + tableName2 + " c), " 
                + tableName3 + " i WHERE o.\"item_id\" = i.\"item_id\" AND o.\"customer_id\" = c.\"customer_id\" ORDER BY \"order_id\"";
        query[4] = "SELECT \"order_id\", c.name, i.name iname, quantity, o.\"DATE\" FROM " + tableName1 + " o, ("
                + tableName2 + " c, " 
                + tableName3 + " i) WHERE o.\"item_id\" = i.\"item_id\" AND o.\"customer_id\" = c.\"customer_id\" ORDER BY \"order_id\"";
        try {
            for (int i = 0; i < query.length; i++) {
                PreparedStatement statement = conn.prepareStatement(query[i]);
                ResultSet rs = statement.executeQuery();
                assertTrue (rs.next());
                assertEquals(rs.getString(1), "000000000000001");
                assertEquals(rs.getString("\"order_id\""), "000000000000001");
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
                
                if (i < 4) {
                    rs = conn.createStatement().executeQuery("EXPLAIN " + query[i]);
                    assertPlansEqual(plans[11 + (i/2)], QueryUtil.getExplainPlan(rs));
                }
            }
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testLeftJoinWithAggregation() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName1 = getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME);
        String tableName2 = getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME);
        String query1 = "SELECT i.name, sum(quantity) FROM " + tableName1 + " o LEFT JOIN " 
            + tableName2 + " i ON o.\"item_id\" = i.\"item_id\" GROUP BY i.name ORDER BY i.name";
        String query2 = "SELECT i.\"item_id\" iid, sum(quantity) q FROM " + tableName1 + " o LEFT JOIN " 
                + tableName2 + " i ON o.\"item_id\" = i.\"item_id\" GROUP BY i.\"item_id\" ORDER BY q DESC";
        String query3 = "SELECT i.\"item_id\" iid, sum(quantity) q FROM " + tableName2 + " i LEFT JOIN " 
                + tableName1 + " o ON o.\"item_id\" = i.\"item_id\" GROUP BY i.\"item_id\" ORDER BY q DESC NULLS LAST, iid";
        try {
            PreparedStatement statement = conn.prepareStatement(query1);
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
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query1);
            assertPlansEqual(plans[0], QueryUtil.getExplainPlan(rs));
            
            statement = conn.prepareStatement(query2);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString("iid"), "0000000006");
            assertEquals(rs.getInt("q"), 6000);
            assertTrue (rs.next());
            assertEquals(rs.getString("iid"), "0000000003");
            assertEquals(rs.getInt("q"), 5000);
            assertTrue (rs.next());
            assertEquals(rs.getString("iid"), "0000000002");
            assertEquals(rs.getInt("q"), 3000);
            assertTrue (rs.next());
            assertEquals(rs.getString("iid"), "0000000001");
            assertEquals(rs.getInt("q"), 1000);

            assertFalse(rs.next());
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query2);
            assertPlansEqual(plans[1], QueryUtil.getExplainPlan(rs));
            
            statement = conn.prepareStatement(query3);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString("iid"), "0000000006");
            assertEquals(rs.getInt("q"), 6000);
            assertTrue (rs.next());
            assertEquals(rs.getString("iid"), "0000000003");
            assertEquals(rs.getInt("q"), 5000);
            assertTrue (rs.next());
            assertEquals(rs.getString("iid"), "0000000002");
            assertEquals(rs.getInt("q"), 3000);
            assertTrue (rs.next());
            assertEquals(rs.getString("iid"), "0000000001");
            assertEquals(rs.getInt("q"), 1000);
            assertTrue (rs.next());
            assertEquals(rs.getString("iid"), "0000000004");
            assertEquals(rs.getInt("q"), 0);
            assertTrue (rs.next());
            assertEquals(rs.getString("iid"), "0000000005");
            assertEquals(rs.getInt("q"), 0);
            assertTrue (rs.next());
            assertEquals(rs.getString("iid"), "invalid001");
            assertEquals(rs.getInt("q"), 0);

            assertFalse(rs.next());
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query3);
            assertPlansEqual(plans[2], QueryUtil.getExplainPlan(rs));
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testRightJoinWithAggregation() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName1 = getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME);
        String tableName2 = getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME);
        String query1 = "SELECT i.name, sum(quantity) FROM " + tableName1 + " o RIGHT JOIN " 
            + tableName2 + " i ON o.\"item_id\" = i.\"item_id\" GROUP BY i.name ORDER BY i.name";
        String query2 = "SELECT i.\"item_id\" iid, sum(quantity) q FROM " + tableName1 + " o RIGHT JOIN " 
            + tableName2 + " i ON o.\"item_id\" = i.\"item_id\" GROUP BY i.\"item_id\" ORDER BY q DESC NULLS LAST, iid";
        try {
            PreparedStatement statement = conn.prepareStatement(query1);
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
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query1);
            assertPlansEqual(plans[3], QueryUtil.getExplainPlan(rs));
            
            statement = conn.prepareStatement(query2);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString("iid"), "0000000006");
            assertEquals(rs.getInt("q"), 6000);
            assertTrue (rs.next());
            assertEquals(rs.getString("iid"), "0000000003");
            assertEquals(rs.getInt("q"), 5000);
            assertTrue (rs.next());
            assertEquals(rs.getString("iid"), "0000000002");
            assertEquals(rs.getInt("q"), 3000);
            assertTrue (rs.next());
            assertEquals(rs.getString("iid"), "0000000001");
            assertEquals(rs.getInt("q"), 1000);
            assertTrue (rs.next());
            assertEquals(rs.getString("iid"), "0000000004");
            assertEquals(rs.getInt("q"), 0);
            assertTrue (rs.next());
            assertEquals(rs.getString("iid"), "0000000005");
            assertEquals(rs.getInt("q"), 0);
            assertTrue (rs.next());
            assertEquals(rs.getString("iid"), "invalid001");
            assertEquals(rs.getInt("q"), 0);

            assertFalse(rs.next());
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query2);
            assertPlansEqual(plans[4], QueryUtil.getExplainPlan(rs));
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testLeftRightJoin() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName1 = getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME);
        String tableName2 = getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME);
        String tableName3 = getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME);
        String query1 = "SELECT \"order_id\", i.name, s.name, quantity, \"DATE\" FROM " + tableName1 + " o LEFT JOIN "
                + tableName2 + " i ON o.\"item_id\" = i.\"item_id\" RIGHT JOIN "
                + tableName3 + " s ON i.\"supplier_id\" = s.\"supplier_id\" ORDER BY \"order_id\", s.\"supplier_id\" DESC";
        String query2 = "SELECT \"order_id\", i.name, s.name, quantity, \"DATE\" FROM " + tableName1 + " o LEFT JOIN "
                + "(" + tableName2 + " i RIGHT JOIN " + tableName3 + " s ON i.\"supplier_id\" = s.\"supplier_id\")" 
                + " ON o.\"item_id\" = i.\"item_id\" ORDER BY \"order_id\", s.\"supplier_id\" DESC";
        try {
            PreparedStatement statement = conn.prepareStatement(query1);
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
            
            statement = conn.prepareStatement(query2);
            rs = statement.executeQuery();
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String[] queries = {
                "SELECT \"order_id\", i.name, s.name, quantity, \"DATE\" FROM " + getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME) + " o LEFT JOIN "
                        + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + " i ON o.\"item_id\" = i.\"item_id\" LEFT JOIN "
                        + getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME) + " s ON i.\"supplier_id\" = s.\"supplier_id\"",
                "SELECT \"order_id\", i.name, s.name, quantity, \"DATE\" FROM " + getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME) + " o LEFT JOIN "
                        + "(" + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + " i LEFT JOIN " + getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME) + " s ON i.\"supplier_id\" = s.\"supplier_id\") " 
                        + "ON o.\"item_id\" = i.\"item_id\""};
        try {
            for (String query : queries) {
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
            }
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testMultiRightJoin() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String query = "SELECT \"order_id\", i.name, s.name, quantity, \"DATE\" FROM " + getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME) + " o RIGHT JOIN "
            + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + " i ON o.\"item_id\" = i.\"item_id\" RIGHT JOIN "
            + getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME) + " s ON i.\"supplier_id\" = s.\"supplier_id\" ORDER BY \"order_id\", s.\"supplier_id\" DESC";

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

    // Basically a copy of testMultiRightJoin, but with a very small result scan chunk size
    // to test that repeated row keys within a single chunk are handled properly
    @Test
    public void testMultiRightJoin_SmallChunkSize() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.SCAN_RESULT_CHUNK_SIZE, "1");
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String query = "SELECT \"order_id\", i.name, s.name, quantity, \"DATE\" FROM " + getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME) + " o RIGHT JOIN "
                + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + " i ON o.\"item_id\" = i.\"item_id\" RIGHT JOIN "
                + getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME) + " s ON i.\"supplier_id\" = s.\"supplier_id\" ORDER BY \"order_id\", s.\"supplier_id\" DESC";

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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String query = "SELECT * FROM " + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + " LEFT JOIN " + getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME) + " supp ON " + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + ".\"supplier_id\" = supp.\"supplier_id\" ORDER BY \"item_id\"";
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".item_id"), "0000000001");
            assertEquals(rs.getString(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".NAME"), "T1");
            assertEquals(rs.getInt(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".PRICE"), 100);
            assertEquals(rs.getInt(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".DISCOUNT1"), 5);
            assertEquals(rs.getInt(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".DISCOUNT2"), 10);
            assertEquals(rs.getString(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".supplier_id"), "0000000001");
            assertEquals(rs.getString(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".DESCRIPTION"), "Item T1");
            assertEquals(rs.getString("SUPP.supplier_id"), "0000000001");
            assertEquals(rs.getString("supp.name"), "S1");
            assertEquals(rs.getString("supp.phone"), "888-888-1111");
            assertEquals(rs.getString("supp.address"), "101 YYY Street");
            assertEquals(rs.getString("supp.loc_id"), "10001");            
            assertTrue (rs.next());
            assertEquals(rs.getString(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".item_id"), "0000000002");
            assertEquals(rs.getString(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".NAME"), "T2");
            assertEquals(rs.getInt(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".PRICE"), 200);
            assertEquals(rs.getInt(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".DISCOUNT1"), 5);
            assertEquals(rs.getInt(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".DISCOUNT2"), 8);
            assertEquals(rs.getString(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".supplier_id"), "0000000001");
            assertEquals(rs.getString(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".DESCRIPTION"), "Item T2");
            assertEquals(rs.getString("SUPP.supplier_id"), "0000000001");
            assertEquals(rs.getString("supp.name"), "S1");
            assertEquals(rs.getString("supp.phone"), "888-888-1111");
            assertEquals(rs.getString("supp.address"), "101 YYY Street");
            assertEquals(rs.getString("supp.loc_id"), "10001");            
            assertTrue (rs.next());
            assertEquals(rs.getString(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".item_id"), "0000000003");
            assertEquals(rs.getString(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".NAME"), "T3");
            assertEquals(rs.getInt(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".PRICE"), 300);
            assertEquals(rs.getInt(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".DISCOUNT1"), 8);
            assertEquals(rs.getInt(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".DISCOUNT2"), 12);
            assertEquals(rs.getString(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".supplier_id"), "0000000002");
            assertEquals(rs.getString(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".DESCRIPTION"), "Item T3");
            assertEquals(rs.getString("SUPP.supplier_id"), "0000000002");
            assertEquals(rs.getString("supp.name"), "S2");
            assertEquals(rs.getString("supp.phone"), "888-888-2222");
            assertEquals(rs.getString("supp.address"), "202 YYY Street");
            assertEquals(rs.getString("supp.loc_id"), "10002");            
            assertTrue (rs.next());
            assertEquals(rs.getString(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".item_id"), "0000000004");
            assertEquals(rs.getString(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".NAME"), "T4");
            assertEquals(rs.getInt(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".PRICE"), 400);
            assertEquals(rs.getInt(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".DISCOUNT1"), 6);
            assertEquals(rs.getInt(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".DISCOUNT2"), 10);
            assertEquals(rs.getString(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".supplier_id"), "0000000002");
            assertEquals(rs.getString(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".DESCRIPTION"), "Item T4");
            assertEquals(rs.getString("SUPP.supplier_id"), "0000000002");
            assertEquals(rs.getString("supp.name"), "S2");
            assertEquals(rs.getString("supp.phone"), "888-888-2222");
            assertEquals(rs.getString("supp.address"), "202 YYY Street");
            assertEquals(rs.getString("supp.loc_id"), "10002");            
            assertTrue (rs.next());
            assertEquals(rs.getString(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".item_id"), "0000000005");
            assertEquals(rs.getString(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".NAME"), "T5");
            assertEquals(rs.getInt(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".PRICE"), 500);
            assertEquals(rs.getInt(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".DISCOUNT1"), 8);
            assertEquals(rs.getInt(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".DISCOUNT2"), 15);
            assertEquals(rs.getString(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".supplier_id"), "0000000005");
            assertEquals(rs.getString(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".DESCRIPTION"), "Item T5");
            assertEquals(rs.getString("SUPP.supplier_id"), "0000000005");
            assertEquals(rs.getString("supp.name"), "S5");
            assertEquals(rs.getString("supp.phone"), "888-888-5555");
            assertEquals(rs.getString("supp.address"), "505 YYY Street");
            assertEquals(rs.getString("supp.loc_id"), "10005");            
            assertTrue (rs.next());
            assertEquals(rs.getString(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".item_id"), "0000000006");
            assertEquals(rs.getString(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".NAME"), "T6");
            assertEquals(rs.getInt(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".PRICE"), 600);
            assertEquals(rs.getInt(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".DISCOUNT1"), 8);
            assertEquals(rs.getInt(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".DISCOUNT2"), 15);
            assertEquals(rs.getString(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".supplier_id"), "0000000006");
            assertEquals(rs.getString(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".DESCRIPTION"), "Item T6");
            assertEquals(rs.getString("SUPP.supplier_id"), "0000000006");
            assertEquals(rs.getString("supp.name"), "S6");
            assertEquals(rs.getString("supp.phone"), "888-888-6666");
            assertEquals(rs.getString("supp.address"), "606 YYY Street");
            assertEquals(rs.getString("supp.loc_id"), "10006");            
            assertTrue (rs.next());
            assertEquals(rs.getString(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".item_id"), "invalid001");
            assertEquals(rs.getString(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".NAME"), "INVALID-1");
            assertEquals(rs.getInt(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".PRICE"), 0);
            assertEquals(rs.getInt(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".DISCOUNT1"), 0);
            assertEquals(rs.getInt(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".DISCOUNT2"), 0);
            assertEquals(rs.getString(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".supplier_id"), "0000000000");
            assertEquals(rs.getString(getDisplayTableName(conn,JOIN_ITEM_TABLE_FULL_NAME) + ".DESCRIPTION"), "Invalid item for join test");
            assertNull(rs.getString("SUPP.supplier_id"));
            assertNull(rs.getString("supp.name"));
            assertNull(rs.getString("supp.phone"));
            assertNull(rs.getString("supp.address"));
            assertNull(rs.getString("supp.loc_id"));

            assertFalse(rs.next());
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            assertPlansEqual(plans[5], QueryUtil.getExplainPlan(rs));
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testJoinWithTableWildcard() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String query = "SELECT s.*, "+ getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + ".*, \"order_id\" FROM " + getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME) + " o RIGHT JOIN " 
                + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + " i ON o.\"item_id\" = i.\"item_id\" RIGHT JOIN "
                + getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME) + " s ON i.\"supplier_id\" = s.\"supplier_id\" ORDER BY \"order_id\", s.\"supplier_id\" DESC";
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(md.getColumnCount(), 13);
            
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000005");
            assertEquals(rs.getString(2), "S5");
            assertEquals(rs.getString(3), "888-888-5555");
            assertEquals(rs.getString(4), "505 YYY Street");
            assertEquals(rs.getString(5), "10005");
            assertEquals(rs.getString(6), "0000000005");
            assertEquals(rs.getString(7), "T5");
            assertEquals(rs.getInt(8), 500);
            assertEquals(rs.getInt(9), 8);
            assertEquals(rs.getInt(10), 15);
            assertEquals(rs.getString(11), "0000000005");
            assertEquals(rs.getString(12), "Item T5");
            assertNull(rs.getString(13));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000004");
            assertEquals(rs.getString(2), "S4");
            assertEquals(rs.getString(3), "888-888-4444");
            assertEquals(rs.getString(4), "404 YYY Street");
            assertNull(rs.getString(5));
            assertNull(rs.getString(6));
            assertNull(rs.getString(7));
            assertEquals(rs.getInt(8), 0);
            assertEquals(rs.getInt(9), 0);
            assertEquals(rs.getInt(10), 0);
            assertNull(rs.getString(11));
            assertNull(rs.getString(12));            
            assertNull(rs.getString(13));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000003");
            assertEquals(rs.getString(2), "S3");
            assertEquals(rs.getString(3), "888-888-3333");
            assertEquals(rs.getString(4), "303 YYY Street");
            assertNull(rs.getString(5));
            assertNull(rs.getString(6));
            assertNull(rs.getString(7));
            assertEquals(rs.getInt(8), 0);
            assertEquals(rs.getInt(9), 0);
            assertEquals(rs.getInt(10), 0);
            assertNull(rs.getString(11));
            assertNull(rs.getString(12));            
            assertNull(rs.getString(13));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000002");
            assertEquals(rs.getString(2), "S2");
            assertEquals(rs.getString(3), "888-888-2222");
            assertEquals(rs.getString(4), "202 YYY Street");
            assertEquals(rs.getString(5), "10002");
            assertEquals(rs.getString(6), "0000000004");
            assertEquals(rs.getString(7), "T4");
            assertEquals(rs.getInt(8), 400);
            assertEquals(rs.getInt(9), 6);
            assertEquals(rs.getInt(10), 10);
            assertEquals(rs.getString(11), "0000000002");
            assertEquals(rs.getString(12), "Item T4");
            assertNull(rs.getString(13));
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000001");
            assertEquals(rs.getString(2), "S1");
            assertEquals(rs.getString(3), "888-888-1111");
            assertEquals(rs.getString(4), "101 YYY Street");
            assertEquals(rs.getString(5), "10001");
            assertEquals(rs.getString(6), "0000000001");
            assertEquals(rs.getString(7), "T1");
            assertEquals(rs.getInt(8), 100);
            assertEquals(rs.getInt(9), 5);
            assertEquals(rs.getInt(10), 10);
            assertEquals(rs.getString(11), "0000000001");
            assertEquals(rs.getString(12), "Item T1");
            assertEquals(rs.getString(13), "000000000000001");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000006");
            assertEquals(rs.getString(2), "S6");
            assertEquals(rs.getString(3), "888-888-6666");
            assertEquals(rs.getString(4), "606 YYY Street");
            assertEquals(rs.getString(5), "10006");
            assertEquals(rs.getString(6), "0000000006");
            assertEquals(rs.getString(7), "T6");
            assertEquals(rs.getInt(8), 600);
            assertEquals(rs.getInt(9), 8);
            assertEquals(rs.getInt(10), 15);
            assertEquals(rs.getString(11), "0000000006");
            assertEquals(rs.getString(12), "Item T6");
            assertEquals(rs.getString(13), "000000000000002");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000001");
            assertEquals(rs.getString(2), "S1");
            assertEquals(rs.getString(3), "888-888-1111");
            assertEquals(rs.getString(4), "101 YYY Street");
            assertEquals(rs.getString(5), "10001");
            assertEquals(rs.getString(6), "0000000002");
            assertEquals(rs.getString(7), "T2");
            assertEquals(rs.getInt(8), 200);
            assertEquals(rs.getInt(9), 5);
            assertEquals(rs.getInt(10), 8);
            assertEquals(rs.getString(11), "0000000001");
            assertEquals(rs.getString(12), "Item T2");
            assertEquals(rs.getString(13), "000000000000003");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000006");
            assertEquals(rs.getString(2), "S6");
            assertEquals(rs.getString(3), "888-888-6666");
            assertEquals(rs.getString(4), "606 YYY Street");
            assertEquals(rs.getString(5), "10006");
            assertEquals(rs.getString(6), "0000000006");
            assertEquals(rs.getString(7), "T6");
            assertEquals(rs.getInt(8), 600);
            assertEquals(rs.getInt(9), 8);
            assertEquals(rs.getInt(10), 15);
            assertEquals(rs.getString(11), "0000000006");
            assertEquals(rs.getString(12), "Item T6");
            assertEquals(rs.getString(13), "000000000000004");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "0000000002");
            assertEquals(rs.getString(2), "S2");
            assertEquals(rs.getString(3), "888-888-2222");
            assertEquals(rs.getString(4), "202 YYY Street");
            assertEquals(rs.getString(5), "10002");
            assertEquals(rs.getString(6), "0000000003");
            assertEquals(rs.getString(7), "T3");
            assertEquals(rs.getInt(8), 300);
            assertEquals(rs.getInt(9), 8);
            assertEquals(rs.getInt(10), 12);
            assertEquals(rs.getString(11), "0000000002");
            assertEquals(rs.getString(12), "Item T3");
            assertEquals(rs.getString(13), "000000000000005");

            assertFalse(rs.next());
        } finally {
            conn.close();
        }        
    }
    
    @Test
    public void testJoinMultiJoinKeys() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String query = "SELECT c.name, s.name FROM " + getTableName(conn, JOIN_CUSTOMER_TABLE_FULL_NAME) + " c LEFT JOIN " + getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME) + " s ON \"customer_id\" = \"supplier_id\" AND c.loc_id = s.loc_id AND substr(s.name, 2, 1) = substr(c.name, 2, 1)";
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String query = "SELECT \"order_id\", i.name, i.price, discount2, quantity FROM " + getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME) + " o INNER JOIN " 
            + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + " i ON o.\"item_id\" = i.\"item_id\" AND o.price = (i.price * (100 - discount2)) / 100.0 WHERE quantity < 5000";
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String query = "SELECT \"order_id\", c.name, o.\"DATE\" FROM " + getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME) + " o INNER JOIN "
            + getTableName(conn, JOIN_CUSTOMER_TABLE_FULL_NAME) + " c ON o.\"customer_id\" = c.\"customer_id\" AND o.\"DATE\" = c.\"DATE\"";
        try {
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String query = "SELECT \"order_id\", i.name, i.price, discount2, quantity FROM " + getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME) + " o INNER JOIN " 
            + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + " i ON o.\"item_id\" = i.\"item_id\" AND o.price / 100 = substr(i.name, 2, 1)";
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.executeQuery();
            fail("Should have got SQLException.");
        } catch (SQLException e) {
            assertEquals(e.getErrorCode(), SQLExceptionCode.TYPE_MISMATCH.getErrorCode());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testJoinPlanWithIndex() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String query1 = "SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + " item LEFT JOIN " + getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME) + " supp ON substr(item.name, 2, 1) = substr(supp.name, 2, 1) AND (supp.name BETWEEN 'S1' AND 'S5') WHERE item.name BETWEEN 'T1' AND 'T5'";
        String query2 = "SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + " item INNER JOIN " + getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME) + " supp ON item.\"supplier_id\" = supp.\"supplier_id\" WHERE (item.name = 'T1' OR item.name = 'T5') AND (supp.name = 'S1' OR supp.name = 'S5')";
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
            assertPlansEqual(plans[6], QueryUtil.getExplainPlan(rs));
            
            
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
            assertPlansEqual(plans[7], QueryUtil.getExplainPlan(rs));
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testJoinWithSkipMergeOptimization() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String query = "SELECT s.name FROM " + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + " i JOIN " 
            + getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME) + " o ON o.\"item_id\" = i.\"item_id\" AND quantity < 5000 JOIN "
            + getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME) + " s ON i.\"supplier_id\" = s.\"supplier_id\"";
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
            assertPlansEqual(plans[8], QueryUtil.getExplainPlan(rs));
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSelfJoin() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String query1 = "SELECT i2.\"item_id\", i1.name FROM " + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + " i1 JOIN " 
            + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + " i2 ON i1.\"item_id\" = i2.\"item_id\" ORDER BY i1.\"item_id\"";
        String query2 = "SELECT i1.name, i2.name FROM " + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + " i1 JOIN " 
            + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + " i2 ON i1.\"item_id\" = i2.\"supplier_id\" ORDER BY i1.name, i2.name";
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
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query1);
            assertPlansEqual(plans[9], QueryUtil.getExplainPlan(rs));

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
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query2);
            assertPlansEqual(plans[10], QueryUtil.getExplainPlan(rs));
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testUpsertWithJoin() throws Exception {
        String tempTable = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        try {
            conn.createStatement().execute("CREATE TABLE " + tempTable 
                    + "   (\"order_id\" varchar not null, " 
                    + "    item_name varchar not null, " 
                    + "    supplier_name varchar, "
                    + "    quantity integer, "
                    + "    \"DATE\" timestamp "
                    + "    CONSTRAINT pk PRIMARY KEY (\"order_id\", item_name))");
            conn.createStatement().execute("UPSERT INTO " + tempTable 
                    + "(\"order_id\", item_name, supplier_name, quantity, \"DATE\") "
                    + "SELECT \"order_id\", i.name, s.name, quantity, \"DATE\" FROM "
                    + getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME) + " o LEFT JOIN " 
                    + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + " i ON o.\"item_id\" = i.\"item_id\" LEFT JOIN "
                    + getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME) + " s ON i.\"supplier_id\" = s.\"supplier_id\"");
            conn.createStatement().execute("UPSERT INTO " + tempTable 
                    + "(\"order_id\", item_name, quantity) " 
                    + "SELECT 'ORDER_SUM', i.name, sum(quantity) FROM " 
                    + getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME) + " o LEFT JOIN " 
                    + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + " i ON o.\"item_id\" = i.\"item_id\" " 
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

            //Bug: PHOENIX-1182
            String sourceTable = generateUniqueName();
            String joinTable = generateUniqueName();
            conn.createStatement().execute("CREATE TABLE " + sourceTable 
                    + "   (TID CHAR(3) NOT NULL, "
                    + "    A UNSIGNED_INT NOT NULL, " 
                    + "    B UNSIGNED_INT NOT NULL "
                    + "    CONSTRAINT pk PRIMARY KEY (TID, A, B))");
            conn.createStatement().execute("CREATE TABLE " + joinTable 
                    + "   (TID CHAR(3) NOT NULL, "
                    + "    A UNSIGNED_INT NOT NULL, "
                    + "    B UNSIGNED_INT NOT NULL, "
                    + "    COUNT UNSIGNED_INT "
                    + "    CONSTRAINT pk PRIMARY KEY (TID, A, B))");
            
            PreparedStatement upsertStmt = conn.prepareStatement(
                    "upsert into " + sourceTable + "(TID, A, B) " + "values (?, ?, ?)");
            upsertStmt.setString(1, "1");
            upsertStmt.setInt(2, 1);
            upsertStmt.setInt(3, 1);
            upsertStmt.execute();
            upsertStmt.setString(1, "1");
            upsertStmt.setInt(2, 1);
            upsertStmt.setInt(3, 2);
            upsertStmt.execute();
            upsertStmt.setString(1, "1");
            upsertStmt.setInt(2, 1);
            upsertStmt.setInt(3, 3);
            upsertStmt.execute();
            upsertStmt.setString(1, "1");
            upsertStmt.setInt(2, 2);
            upsertStmt.setInt(3, 1);
            upsertStmt.execute();
            upsertStmt.setString(1, "1");
            upsertStmt.setInt(2, 2);
            upsertStmt.setInt(3, 2);
            upsertStmt.execute();
            conn.commit();
            
            upsertStmt = conn.prepareStatement(
                    "upsert into " + joinTable + "(TID, A, B, COUNT) "
                            + "SELECT t1.TID, t1.A, t2.A, COUNT(*) "
                            + "FROM " + sourceTable + " t1 "
                            + "INNER JOIN " + sourceTable + " t2 ON t1.B = t2.B "
                            + "WHERE t1.A != t2.A AND t1.TID = '1' AND t2.TID = '1' "
                            + "GROUP BY t1.TID, t1.A, t2.A");
            upsertStmt.execute();
            conn.commit();            

            rs = statement.executeQuery("SELECT * FROM " + joinTable);
            assertTrue(rs.next());
            assertEquals(rs.getString(1), "1");
            assertEquals(rs.getInt(2), 1);
            assertEquals(rs.getInt(3), 2);
            assertEquals(rs.getInt(4), 2);
            assertTrue(rs.next());
            assertEquals(rs.getString(1), "1");
            assertEquals(rs.getInt(2), 2);
            assertEquals(rs.getInt(3), 1);
            assertEquals(rs.getInt(4), 2);

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSubJoin() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String query1 = "SELECT i.name, count(c.name), min(s.name), max(quantity) FROM " + getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME) + " o LEFT JOIN " 
                + "(" + getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME) + " s RIGHT JOIN " + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + " i ON i.\"supplier_id\" = s.\"supplier_id\")" 
                + " ON o.\"item_id\" = i.\"item_id\" LEFT JOIN " 
                + getTableName(conn, JOIN_CUSTOMER_TABLE_FULL_NAME) + " c ON c.\"customer_id\" = o.\"customer_id\" GROUP BY i.name ORDER BY i.name";
        String query2 = "SELECT * FROM " + getTableName(conn, JOIN_CUSTOMER_TABLE_FULL_NAME) + " c INNER JOIN " 
                + "(" + getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME) + " o INNER JOIN " 
                + "(" + getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME) + " s RIGHT JOIN " + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + " i ON i.\"supplier_id\" = s.\"supplier_id\")" 
                + " ON o.\"item_id\" = i.\"item_id\") ON c.\"customer_id\" = o.\"customer_id\"" 
                + " WHERE c.\"customer_id\" <= '0000000005' AND \"order_id\" != '000000000000003' AND i.name != 'T3' ORDER BY c.\"customer_id\", i.name";
        try {
            PreparedStatement statement = conn.prepareStatement(query1);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T1");
            assertEquals(rs.getInt(2), 1);
            assertEquals(rs.getString(3), "S1");
            assertEquals(rs.getInt(4), 1000);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T2");
            assertEquals(rs.getInt(2), 1);
            assertEquals(rs.getString(3), "S1");
            assertEquals(rs.getInt(4), 3000);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T3");
            assertEquals(rs.getInt(2), 1);
            assertEquals(rs.getString(3), "S2");
            assertEquals(rs.getInt(4), 5000);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T6");
            assertEquals(rs.getInt(2), 2);
            assertEquals(rs.getString(3), "S6");
            assertEquals(rs.getInt(4), 4000);

            assertFalse(rs.next());
            
            statement = conn.prepareStatement(query2);
            rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(rs.getString("C.customer_id"), "0000000003");
            assertEquals(rs.getString("c.name"), "C3");
            assertEquals(rs.getString("c.phone"), "999-999-3333");
            assertEquals(rs.getString("c.address"), "303 XXX Street");
            assertNull(rs.getString("c.loc_id"));
            assertEquals(rs.getDate("c.date"), new Date(format.parse("2013-11-25 10:06:29").getTime()));
            assertEquals(rs.getString("O.order_id"), "000000000000002");
            assertEquals(rs.getString("O.customer_id"), "0000000003");
            assertEquals(rs.getString("O.item_id"), "0000000006");
            assertEquals(rs.getInt("o.price"), 552);
            assertEquals(rs.getInt("o.quantity"), 2000);
            assertEquals(rs.getTimestamp("o.date"), new Timestamp(format.parse("2013-11-25 10:06:29").getTime()));
            assertEquals(rs.getString("I.item_id"), "0000000006");
            assertEquals(rs.getString("i.name"), "T6");
            assertEquals(rs.getInt("i.price"), 600);
            assertEquals(rs.getInt("i.discount1"), 8);
            assertEquals(rs.getInt("i.discount2"), 15);
            assertEquals(rs.getString("I.supplier_id"), "0000000006");
            assertEquals(rs.getString("i.description"), "Item T6");
            assertEquals(rs.getString("S.supplier_id"), "0000000006");
            assertEquals(rs.getString("s.name"), "S6");
            assertEquals(rs.getString("s.phone"), "888-888-6666");
            assertEquals(rs.getString("s.address"), "606 YYY Street");
            assertEquals(rs.getString("s.loc_id"), "10006");
            assertTrue(rs.next());
            assertEquals(rs.getString("C.customer_id"), "0000000004");
            assertEquals(rs.getString("c.name"), "C4");
            assertEquals(rs.getString("c.phone"), "999-999-4444");
            assertEquals(rs.getString("c.address"), "404 XXX Street");
            assertEquals(rs.getString("c.loc_id"), "10004");
            assertEquals(rs.getDate("c.date"), new Date(format.parse("2013-11-22 14:22:56").getTime()));
            assertEquals(rs.getString("O.order_id"), "000000000000001");
            assertEquals(rs.getString("O.customer_id"), "0000000004");
            assertEquals(rs.getString("O.item_id"), "0000000001");
            assertEquals(rs.getInt("o.price"), 100);
            assertEquals(rs.getInt("o.quantity"), 1000);
            assertEquals(rs.getTimestamp("o.date"), new Timestamp(format.parse("2013-11-22 14:22:56").getTime()));
            assertEquals(rs.getString("I.item_id"), "0000000001");
            assertEquals(rs.getString("i.name"), "T1");
            assertEquals(rs.getInt("i.price"), 100);
            assertEquals(rs.getInt("i.discount1"), 5);
            assertEquals(rs.getInt("i.discount2"), 10);
            assertEquals(rs.getString("I.supplier_id"), "0000000001");
            assertEquals(rs.getString("i.description"), "Item T1");
            assertEquals(rs.getString("S.supplier_id"), "0000000001");
            assertEquals(rs.getString("s.name"), "S1");
            assertEquals(rs.getString("s.phone"), "888-888-1111");
            assertEquals(rs.getString("s.address"), "101 YYY Street");
            assertEquals(rs.getString("s.loc_id"), "10001");
            assertTrue(rs.next());
            assertEquals(rs.getString("C.customer_id"), "0000000004");
            assertEquals(rs.getString("c.name"), "C4");
            assertEquals(rs.getString("c.phone"), "999-999-4444");
            assertEquals(rs.getString("c.address"), "404 XXX Street");
            assertEquals(rs.getString("c.loc_id"), "10004");
            assertEquals(rs.getDate("c.date"), new Date(format.parse("2013-11-22 14:22:56").getTime()));
            assertEquals(rs.getString("O.order_id"), "000000000000004");
            assertEquals(rs.getString("O.customer_id"), "0000000004");
            assertEquals(rs.getString("O.item_id"), "0000000006");
            assertEquals(rs.getInt("o.price"), 510);
            assertEquals(rs.getInt("o.quantity"), 4000);
            assertEquals(rs.getTimestamp("o.date"), new Timestamp(format.parse("2013-11-26 13:26:04").getTime()));
            assertEquals(rs.getString("I.item_id"), "0000000006");
            assertEquals(rs.getString("i.name"), "T6");
            assertEquals(rs.getInt("i.price"), 600);
            assertEquals(rs.getInt("i.discount1"), 8);
            assertEquals(rs.getInt("i.discount2"), 15);
            assertEquals(rs.getString("I.supplier_id"), "0000000006");
            assertEquals(rs.getString("i.description"), "Item T6");
            assertEquals(rs.getString("S.supplier_id"), "0000000006");
            assertEquals(rs.getString("s.name"), "S6");
            assertEquals(rs.getString("s.phone"), "888-888-6666");
            assertEquals(rs.getString("s.address"), "606 YYY Street");
            assertEquals(rs.getString("s.loc_id"), "10006");

            assertFalse(rs.next());            
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query2);
            assertPlansEqual(plans[13], QueryUtil.getExplainPlan(rs));
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testJoinWithSubquery() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String query1 = "SELECT item.\"item_id\", item.name, supp.sid, supp.name FROM " + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + " item INNER JOIN (SELECT reverse(loc_id), \"supplier_id\" sid, name FROM " + getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME) + " WHERE name BETWEEN 'S1' AND 'S5') AS supp ON item.\"supplier_id\" = supp.sid";
        String query2 = "SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + " item INNER JOIN (SELECT reverse(loc_id), \"supplier_id\", name FROM " + getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME) + ") AS supp ON item.\"supplier_id\" = supp.\"supplier_id\" AND (supp.name = 'S1' OR supp.name = 'S5')";
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
    public void testJoinWithSubqueryPostFilters() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            String query = "SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + " item INNER JOIN (SELECT reverse(loc_id), \"supplier_id\", name FROM " + getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME) + " LIMIT 5) AS supp ON item.\"supplier_id\" = supp.\"supplier_id\" AND (supp.name != 'S1')";
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
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

            query = "SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM "
                    + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME)
                    + " item INNER JOIN (SELECT reverse(loc_id), \"supplier_id\", name FROM "
                    + getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME)
                    + " ORDER BY \"supplier_id\"  OFFSET 2) AS supp ON item.\"supplier_id\" = supp.\"supplier_id\" AND (supp.name != 'S1')";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(rs.getString(1), "0000000005");
            assertEquals(rs.getString(2), "T5");
            assertEquals(rs.getString(3), "0000000005");
            assertEquals(rs.getString(4), "S5");
            assertTrue(rs.next());
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
    public void testJoinWithSubqueryAndAggregation() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String query1 = "SELECT i.name, sum(quantity) FROM " + getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME) + " o LEFT JOIN (SELECT name, \"item_id\" iid FROM " 
            + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + ") AS i ON o.\"item_id\" = i.iid GROUP BY i.name ORDER BY i.name";
        String query2 = "SELECT o.iid, sum(o.quantity) q FROM (SELECT \"item_id\" iid, quantity FROM " + getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME) + ") AS o LEFT JOIN (SELECT \"item_id\" FROM " 
                + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + ") AS i ON o.iid = i.\"item_id\" GROUP BY o.iid ORDER BY q DESC";
        String query3 = "SELECT i.iid, o.q FROM (SELECT \"item_id\" iid FROM " + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + ") AS i LEFT JOIN (SELECT \"item_id\" iid, sum(quantity) q FROM " 
                + getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME) + " GROUP BY \"item_id\") AS o ON o.iid = i.iid ORDER BY o.q DESC NULLS LAST, i.iid";
        String query4 = "SELECT i.iid, o.q FROM (SELECT \"item_id\" iid, sum(quantity) q FROM " + getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME) + " GROUP BY \"item_id\") AS o JOIN (SELECT \"item_id\" iid FROM " 
                + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + ") AS i ON o.iid = i.iid ORDER BY o.q DESC, i.iid";
        try {
            PreparedStatement statement = conn.prepareStatement(query1);
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
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query1);
            assertPlansEqual(plans[14], QueryUtil.getExplainPlan(rs));
            
            statement = conn.prepareStatement(query2);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString("o.iid"), "0000000006");
            assertEquals(rs.getInt("q"), 6000);
            assertTrue (rs.next());
            assertEquals(rs.getString("o.iid"), "0000000003");
            assertEquals(rs.getInt("q"), 5000);
            assertTrue (rs.next());
            assertEquals(rs.getString("o.iid"), "0000000002");
            assertEquals(rs.getInt("q"), 3000);
            assertTrue (rs.next());
            assertEquals(rs.getString("o.iid"), "0000000001");
            assertEquals(rs.getInt("q"), 1000);

            assertFalse(rs.next());
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query2);
            assertPlansEqual(plans[15], QueryUtil.getExplainPlan(rs));
            
            statement = conn.prepareStatement(query3);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString("i.iid"), "0000000006");
            assertEquals(rs.getInt("o.q"), 6000);
            assertTrue (rs.next());
            assertEquals(rs.getString("i.iid"), "0000000003");
            assertEquals(rs.getInt("o.q"), 5000);
            assertTrue (rs.next());
            assertEquals(rs.getString("i.iid"), "0000000002");
            assertEquals(rs.getInt("o.q"), 3000);
            assertTrue (rs.next());
            assertEquals(rs.getString("i.iid"), "0000000001");
            assertEquals(rs.getInt("o.q"), 1000);
            assertTrue (rs.next());
            assertEquals(rs.getString("i.iid"), "0000000004");
            assertEquals(rs.getInt("o.q"), 0);
            assertTrue (rs.next());
            assertEquals(rs.getString("i.iid"), "0000000005");
            assertEquals(rs.getInt("o.q"), 0);
            assertTrue (rs.next());
            assertEquals(rs.getString("i.iid"), "invalid001");
            assertEquals(rs.getInt("o.q"), 0);

            assertFalse(rs.next());
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query3);
            assertPlansEqual(plans[16], QueryUtil.getExplainPlan(rs));
            
            statement = conn.prepareStatement(query4);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString("i.iid"), "0000000006");
            assertEquals(rs.getInt("o.q"), 6000);
            assertTrue (rs.next());
            assertEquals(rs.getString("i.iid"), "0000000003");
            assertEquals(rs.getInt("o.q"), 5000);
            assertTrue (rs.next());
            assertEquals(rs.getString("i.iid"), "0000000002");
            assertEquals(rs.getInt("o.q"), 3000);
            assertTrue (rs.next());
            assertEquals(rs.getString("i.iid"), "0000000001");
            assertEquals(rs.getInt("o.q"), 1000);

            assertFalse(rs.next());
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query4);
            assertPlansEqual(plans[17], QueryUtil.getExplainPlan(rs));
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testNestedSubqueries() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String query1 = "SELECT q.iname, count(c.name), min(q.sname), max(o.quantity) FROM (SELECT \"customer_id\" cid, \"item_id\" iid, quantity FROM " + getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME) + ") AS o LEFT JOIN " 
                + "(SELECT i.iid iid, s.name sname, i.name iname FROM (SELECT \"supplier_id\" sid, name FROM " + getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME) + ") AS s RIGHT JOIN (SELECT \"item_id\" iid, name, \"supplier_id\" sid FROM " + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + ") AS i ON i.sid = s.sid) AS q" 
                + " ON o.iid = q.iid LEFT JOIN (SELECT \"customer_id\" cid, name FROM " 
                + getTableName(conn, JOIN_CUSTOMER_TABLE_FULL_NAME) + ") AS c ON c.cid = o.cid GROUP BY q.iname ORDER BY q.iname";
        String query2 = "SELECT * FROM (SELECT \"customer_id\" cid, name, phone, address, loc_id, \"DATE\" FROM " + getTableName(conn, JOIN_CUSTOMER_TABLE_FULL_NAME) + ") AS c INNER JOIN "
                + "(SELECT o.oid ooid, o.cid ocid, o.iid oiid, o.price * o.quantity, o.\"DATE\" odate, qi.iiid iiid, qi.iname iname, qi.iprice iprice, qi.idiscount1 idiscount1, qi.idiscount2 idiscount2, qi.isid isid, qi.idescription idescription, qi.ssid ssid, qi.sname sname, qi.sphone sphone, qi.saddress saddress, qi.sloc_id sloc_id FROM (SELECT \"item_id\" iid, \"customer_id\" cid, \"order_id\" oid, price, quantity, \"DATE\" FROM " + getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME) + ") AS o INNER JOIN "
                + "(SELECT i.iid iiid, i.name iname, i.price iprice, i.discount1 idiscount1, i.discount2 idiscount2, i.sid isid, i.description idescription, s.sid ssid, s.name sname, s.phone sphone, s.address saddress, s.loc_id sloc_id FROM (SELECT \"supplier_id\" sid, name, phone, address, loc_id FROM " + getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME) + ") AS s RIGHT JOIN (SELECT \"item_id\" iid, name, price, discount1, discount2, \"supplier_id\" sid, description FROM " + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + ") AS i ON i.sid = s.sid) as qi" 
                + " ON o.iid = qi.iiid) as qo ON c.cid = qo.ocid" 
                + " WHERE c.cid <= '0000000005' AND qo.ooid != '000000000000003' AND qo.iname != 'T3' ORDER BY c.cid, qo.iname";
        try {
            PreparedStatement statement = conn.prepareStatement(query1);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T1");
            assertEquals(rs.getInt(2), 1);
            assertEquals(rs.getString(3), "S1");
            assertEquals(rs.getInt(4), 1000);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T2");
            assertEquals(rs.getInt(2), 1);
            assertEquals(rs.getString(3), "S1");
            assertEquals(rs.getInt(4), 3000);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T3");
            assertEquals(rs.getInt(2), 1);
            assertEquals(rs.getString(3), "S2");
            assertEquals(rs.getInt(4), 5000);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "T6");
            assertEquals(rs.getInt(2), 2);
            assertEquals(rs.getString(3), "S6");
            assertEquals(rs.getInt(4), 4000);

            assertFalse(rs.next());
            
            statement = conn.prepareStatement(query2);
            rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(rs.getString("c.cid"), "0000000003");
            assertEquals(rs.getString("c.name"), "C3");
            assertEquals(rs.getString("c.phone"), "999-999-3333");
            assertEquals(rs.getString("c.address"), "303 XXX Street");
            assertNull(rs.getString("c.loc_id"));
            assertEquals(rs.getDate("c.date"), new Date(format.parse("2013-11-25 10:06:29").getTime()));
            assertEquals(rs.getString("qo.ooid"), "000000000000002");
            assertEquals(rs.getString("qo.ocid"), "0000000003");
            assertEquals(rs.getString("qo.oiid"), "0000000006");
            assertEquals(rs.getInt(10), 1104000);
            assertEquals(rs.getTimestamp("qo.odate"), new Timestamp(format.parse("2013-11-25 10:06:29").getTime()));
            assertEquals(rs.getString("qo.iiid"), "0000000006");
            assertEquals(rs.getString("qo.iname"), "T6");
            assertEquals(rs.getInt("qo.iprice"), 600);
            assertEquals(rs.getInt("qo.idiscount1"), 8);
            assertEquals(rs.getInt("qo.idiscount2"), 15);
            assertEquals(rs.getString("qo.isid"), "0000000006");
            assertEquals(rs.getString("qo.idescription"), "Item T6");
            assertEquals(rs.getString("qo.ssid"), "0000000006");
            assertEquals(rs.getString("qo.sname"), "S6");
            assertEquals(rs.getString("qo.sphone"), "888-888-6666");
            assertEquals(rs.getString("qo.saddress"), "606 YYY Street");
            assertEquals(rs.getString("qo.sloc_id"), "10006");
            assertTrue(rs.next());
            assertEquals(rs.getString("c.cid"), "0000000004");
            assertEquals(rs.getString("c.name"), "C4");
            assertEquals(rs.getString("c.phone"), "999-999-4444");
            assertEquals(rs.getString("c.address"), "404 XXX Street");
            assertEquals(rs.getString("c.loc_id"), "10004");
            assertEquals(rs.getDate("c.date"), new Date(format.parse("2013-11-22 14:22:56").getTime()));
            assertEquals(rs.getString("qo.ooid"), "000000000000001");
            assertEquals(rs.getString("qo.ocid"), "0000000004");
            assertEquals(rs.getString("qo.oiid"), "0000000001");
            assertEquals(rs.getInt(10), 100000);
            assertEquals(rs.getTimestamp("qo.odate"), new Timestamp(format.parse("2013-11-22 14:22:56").getTime()));
            assertEquals(rs.getString("qo.iiid"), "0000000001");
            assertEquals(rs.getString("qo.iname"), "T1");
            assertEquals(rs.getInt("qo.iprice"), 100);
            assertEquals(rs.getInt("qo.idiscount1"), 5);
            assertEquals(rs.getInt("qo.idiscount2"), 10);
            assertEquals(rs.getString("qo.isid"), "0000000001");
            assertEquals(rs.getString("qo.idescription"), "Item T1");
            assertEquals(rs.getString("qo.ssid"), "0000000001");
            assertEquals(rs.getString("qo.sname"), "S1");
            assertEquals(rs.getString("qo.sphone"), "888-888-1111");
            assertEquals(rs.getString("qo.saddress"), "101 YYY Street");
            assertEquals(rs.getString("qo.sloc_id"), "10001");
            assertTrue(rs.next());
            assertEquals(rs.getString("c.cid"), "0000000004");
            assertEquals(rs.getString("c.name"), "C4");
            assertEquals(rs.getString("c.phone"), "999-999-4444");
            assertEquals(rs.getString("c.address"), "404 XXX Street");
            assertEquals(rs.getString("c.loc_id"), "10004");
            assertEquals(rs.getDate("c.date"), new Date(format.parse("2013-11-22 14:22:56").getTime()));
            assertEquals(rs.getString("qo.ooid"), "000000000000004");
            assertEquals(rs.getString("qo.ocid"), "0000000004");
            assertEquals(rs.getString("qo.oiid"), "0000000006");
            assertEquals(rs.getInt(10), 2040000);
            assertEquals(rs.getTimestamp("qo.odate"), new Timestamp(format.parse("2013-11-26 13:26:04").getTime()));
            assertEquals(rs.getString("qo.iiid"), "0000000006");
            assertEquals(rs.getString("qo.iname"), "T6");
            assertEquals(rs.getInt("qo.iprice"), 600);
            assertEquals(rs.getInt("qo.idiscount1"), 8);
            assertEquals(rs.getInt("qo.idiscount2"), 15);
            assertEquals(rs.getString("qo.isid"), "0000000006");
            assertEquals(rs.getString("qo.idescription"), "Item T6");
            assertEquals(rs.getString("qo.ssid"), "0000000006");
            assertEquals(rs.getString("qo.sname"), "S6");
            assertEquals(rs.getString("qo.sphone"), "888-888-6666");
            assertEquals(rs.getString("qo.saddress"), "606 YYY Street");
            assertEquals(rs.getString("qo.sloc_id"), "10006");

            assertFalse(rs.next());            
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query2);
            assertPlansEqual(plans[18], QueryUtil.getExplainPlan(rs));
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testJoinWithLimit() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String query1 = "SELECT \"order_id\", i.name, s.name, s.address, quantity FROM " + getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME) + " s LEFT JOIN " 
                + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + " i ON i.\"supplier_id\" = s.\"supplier_id\" LEFT JOIN "
                + getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME) + " o ON o.\"item_id\" = i.\"item_id\" LIMIT 4";
        String query2 = "SELECT \"order_id\", i.name, s.name, s.address, quantity FROM " + getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME) + " s JOIN " 
                + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + " i ON i.\"supplier_id\" = s.\"supplier_id\" JOIN "
                + getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME) + " o ON o.\"item_id\" = i.\"item_id\" LIMIT 4";
        try {
            PreparedStatement statement = conn.prepareStatement(query1);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000001");
            assertEquals(rs.getString(2), "T1");
            assertEquals(rs.getString(3), "S1");
            assertEquals(rs.getString(4), "101 YYY Street");
            assertEquals(rs.getInt(5), 1000);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000003");
            assertEquals(rs.getString(2), "T2");
            assertEquals(rs.getString(3), "S1");
            assertEquals(rs.getString(4), "101 YYY Street");
            assertEquals(rs.getInt(5), 3000);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000005");
            assertEquals(rs.getString(2), "T3");
            assertEquals(rs.getString(3), "S2");
            assertEquals(rs.getString(4), "202 YYY Street");
            assertEquals(rs.getInt(5), 5000);
            assertTrue (rs.next());
            assertNull(rs.getString(1));
            assertEquals(rs.getString(2), "T4");
            assertEquals(rs.getString(3), "S2");
            assertEquals(rs.getString(4), "202 YYY Street");
            assertEquals(rs.getInt(5), 0);

            assertFalse(rs.next());
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query1);
            assertPlansEqual(plans[19], QueryUtil.getExplainPlan(rs));
            
            statement = conn.prepareStatement(query2);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000001");
            assertEquals(rs.getString(2), "T1");
            assertEquals(rs.getString(3), "S1");
            assertEquals(rs.getString(4), "101 YYY Street");
            assertEquals(rs.getInt(5), 1000);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000003");
            assertEquals(rs.getString(2), "T2");
            assertEquals(rs.getString(3), "S1");
            assertEquals(rs.getString(4), "101 YYY Street");
            assertEquals(rs.getInt(5), 3000);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000005");
            assertEquals(rs.getString(2), "T3");
            assertEquals(rs.getString(3), "S2");
            assertEquals(rs.getString(4), "202 YYY Street");
            assertEquals(rs.getInt(5), 5000);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "000000000000002");
            assertEquals(rs.getString(2), "T6");
            assertEquals(rs.getString(3), "S6");
            assertEquals(rs.getString(4), "606 YYY Street");
            assertEquals(rs.getInt(5), 2000);

            assertFalse(rs.next());
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query2);
            assertPlansEqual(plans[20], QueryUtil.getExplainPlan(rs));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testJoinWithOffset() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String query1 = "SELECT \"order_id\", i.name, s.name, s.address, quantity FROM " + getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME)
                + " s LEFT JOIN " + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + " i ON i.\"supplier_id\" = s.\"supplier_id\" LEFT JOIN "
                + getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME) + " o ON o.\"item_id\" = i.\"item_id\" LIMIT 1 OFFSET 2 ";
        String query2 = "SELECT \"order_id\", i.name, s.name, s.address, quantity FROM " + getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME)
                + " s JOIN " + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + " i ON i.\"supplier_id\" = s.\"supplier_id\" JOIN "
                + getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME) + " o ON o.\"item_id\" = i.\"item_id\" LIMIT 1 OFFSET 2 ";
        try {
            PreparedStatement statement = conn.prepareStatement(query1);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(rs.getString(1), "000000000000005");
            assertEquals(rs.getString(2), "T3");
            assertEquals(rs.getString(3), "S2");
            assertEquals(rs.getString(4), "202 YYY Street");
            assertEquals(rs.getInt(5), 5000);

            assertFalse(rs.next());

            rs = conn.createStatement().executeQuery("EXPLAIN " + query1);
            assertPlansEqual(plans[22], QueryUtil.getExplainPlan(rs));

            statement = conn.prepareStatement(query2);
            rs = statement.executeQuery();

            assertTrue(rs.next());
            assertEquals(rs.getString(1), "000000000000005");
            assertEquals(rs.getString(2), "T3");
            assertEquals(rs.getString(3), "S2");
            assertEquals(rs.getString(4), "202 YYY Street");
            assertEquals(rs.getInt(5), 5000);
            assertFalse(rs.next());

            rs = conn.createStatement().executeQuery("EXPLAIN " + query2);
            assertPlansEqual(plans[23], QueryUtil.getExplainPlan(rs));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testNonEquiJoin() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            String query = "SELECT item.name, supp.name FROM " + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + " item, " + getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME) + " supp WHERE item.\"supplier_id\" > supp.\"supplier_id\"";
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(rs.getString(1), "T3");
            assertEquals(rs.getString(2), "S1");
            assertTrue(rs.next());
            assertEquals(rs.getString(1), "T4");
            assertEquals(rs.getString(2), "S1");
            assertTrue(rs.next());
            assertEquals(rs.getString(1), "T5");
            assertEquals(rs.getString(2), "S1");
            assertTrue(rs.next());
            assertEquals(rs.getString(1), "T5");
            assertEquals(rs.getString(2), "S2");
            assertTrue(rs.next());
            assertEquals(rs.getString(1), "T5");
            assertEquals(rs.getString(2), "S3");
            assertTrue(rs.next());
            assertEquals(rs.getString(1), "T5");
            assertEquals(rs.getString(2), "S4");
            assertTrue(rs.next());
            assertEquals(rs.getString(1), "T6");
            assertEquals(rs.getString(2), "S1");
            assertTrue(rs.next());
            assertEquals(rs.getString(1), "T6");
            assertEquals(rs.getString(2), "S2");
            assertTrue(rs.next());
            assertEquals(rs.getString(1), "T6");
            assertEquals(rs.getString(2), "S3");
            assertTrue(rs.next());
            assertEquals(rs.getString(1), "T6");
            assertEquals(rs.getString(2), "S4");
            assertTrue(rs.next());
            assertEquals(rs.getString(1), "T6");
            assertEquals(rs.getString(2), "S5");

            assertFalse(rs.next());
            
            query = "SELECT item.name, supp.name FROM " + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + " item JOIN " + getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME) + " supp ON item.\"supplier_id\" > supp.\"supplier_id\"";
            statement = conn.prepareStatement(query);
            try {
                statement.executeQuery();
                fail("Should have got SQLException.");
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.AMBIGUOUS_JOIN_CONDITION.getErrorCode(), e.getErrorCode());
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testJoinWithSetMaxRows() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String [] queries = new String[2];
        queries[0] = "SELECT \"order_id\", i.name, quantity FROM " + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + " i JOIN "
                + getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME) + " o ON o.\"item_id\" = i.\"item_id\"";
        queries[1] = "SELECT o.\"order_id\", i.name, o.quantity FROM " + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + " i JOIN " 
                + "(SELECT \"order_id\", \"item_id\", quantity FROM " + getTableName(conn, JOIN_ORDER_TABLE_FULL_NAME) + ") o " 
                + "ON o.\"item_id\" = i.\"item_id\"";
        try {
            for (String query : queries) {
                Statement statement = conn.createStatement();
                statement.setMaxRows(4);
                ResultSet rs = statement.executeQuery(query);
                assertTrue (rs.next());
                assertEquals(rs.getString(1), "000000000000001");
                assertEquals(rs.getString(2), "T1");
                assertEquals(rs.getInt(3), 1000);
                assertTrue (rs.next());
                assertEquals(rs.getString(1), "000000000000003");
                assertEquals(rs.getString(2), "T2");
                assertEquals(rs.getInt(3), 3000);
                assertTrue (rs.next());
                assertEquals(rs.getString(1), "000000000000005");
                assertEquals(rs.getString(2), "T3");
                assertEquals(rs.getInt(3), 5000);
                assertTrue (rs.next());
                assertEquals(rs.getString(1), "000000000000002");
                assertEquals(rs.getString(2), "T6");
                assertEquals(rs.getInt(3), 2000);

                assertFalse(rs.next());
                
                rs = statement.executeQuery("EXPLAIN " + query);
                assertPlansEqual(plans[21], QueryUtil.getExplainPlan(rs));
            }
        } finally {
            conn.close();
        }
    }
}
