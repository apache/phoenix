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
public class HashJoinLocalIndexIT extends BaseJoinIT {
    
    public HashJoinLocalIndexIT(String[] indexDDL, String[] plans) {
        super(indexDDL, plans);
    }
    
    @Parameters
    public static Collection<Object> data() {
        List<Object> testCases = Lists.newArrayList();
        testCases.add(new String[][] {
                {
                "CREATE LOCAL INDEX \"idx_customer\" ON " + JOIN_CUSTOMER_TABLE_FULL_NAME + " (name)",
                "CREATE LOCAL INDEX \"idx_item\" ON " + JOIN_ITEM_TABLE_FULL_NAME + " (name)",
                "CREATE LOCAL INDEX \"idx_supplier\" ON " + JOIN_SUPPLIER_TABLE_FULL_NAME + " (name)"
                }, {
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_SUPPLIER_TABLE_FULL_NAME + " [1,'S1']\n" +
                "    SERVER FILTER BY FIRST KEY ONLY\n" + 
                "CLIENT MERGE SORT\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + " [1,*] - [1,'T6']\n" +
                "            SERVER FILTER BY FIRST KEY ONLY\n" +
                "        CLIENT MERGE SORT\n" +
                "    DYNAMIC SERVER FILTER BY \"S.:supplier_id\" IN (\"I.supplier_id\")",
                
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_SUPPLIER_TABLE_FULL_NAME + " [1,'S1']\n" +
                "    SERVER FILTER BY FIRST KEY ONLY\n" + 
                "    SERVER AGGREGATE INTO DISTINCT ROWS BY [\"S.PHONE\"]\n" +
                "CLIENT MERGE SORT\n" +
                "    PARALLEL INNER-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + " [1,*] - [1,'T6']\n" +
                "            SERVER FILTER BY FIRST KEY ONLY\n" + 
                "        CLIENT MERGE SORT\n" +
                "    DYNAMIC SERVER FILTER BY \"S.:supplier_id\" IN (\"I.supplier_id\")",
                
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_SUPPLIER_TABLE_FULL_NAME + " [1,*] - [1,'S3']\n" +
                "    SERVER FILTER BY FIRST KEY ONLY\n" + 
                "    SERVER AGGREGATE INTO SINGLE ROW\n" +
                "    PARALLEL LEFT-JOIN TABLE 0\n" +
                "        CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + " [1,*] - [1,'T6']\n" +
                "            SERVER FILTER BY FIRST KEY ONLY\n" + 
                "        CLIENT MERGE SORT",
                }});
        return testCases;
    }
    

    @Test
    public void testJoinWithLocalIndex() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {            
            String query = "select phone, i.name from " + getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME) + " s join " + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + " i on s.\"supplier_id\" = i.\"supplier_id\" where s.name = 'S1' and i.name < 'T6'";
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "888-888-1111");
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "888-888-1111");
            assertFalse(rs.next());
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            assertPlansEqual(plans[0], QueryUtil.getExplainPlan(rs));
            
            query = "select phone, max(i.name) from " + getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME) + " s join " + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + " i on s.\"supplier_id\" = i.\"supplier_id\" where s.name = 'S1' and i.name < 'T6' group by phone";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "888-888-1111");
            assertEquals(rs.getString(2), "T2");
            assertFalse(rs.next());
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            assertPlansEqual(plans[1], QueryUtil.getExplainPlan(rs));
            
            query = "select max(phone), max(i.name) from " + getTableName(conn, JOIN_SUPPLIER_TABLE_FULL_NAME) + " s left join " + getTableName(conn, JOIN_ITEM_TABLE_FULL_NAME) + " i on s.\"supplier_id\" = i.\"supplier_id\" and i.name < 'T6' where s.name <= 'S3'";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), "888-888-3333");
            assertEquals(rs.getString(2), "T4");
            assertFalse(rs.next());
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            assertPlansEqual(plans[2], QueryUtil.getExplainPlan(rs));
        } finally {
            conn.close();
        }
    }
}
