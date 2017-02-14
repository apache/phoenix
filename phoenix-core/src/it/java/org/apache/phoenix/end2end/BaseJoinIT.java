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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;
import org.junit.Before;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public abstract class BaseJoinIT extends ParallelStatsDisabledIT {
    protected static final String JOIN_SCHEMA = "Join";
    protected static final String JOIN_ORDER_TABLE = "OrderTable";
    protected static final String JOIN_CUSTOMER_TABLE = "CustomerTable";
    protected static final String JOIN_ITEM_TABLE = "ItemTable";
    protected static final String JOIN_SUPPLIER_TABLE = "SupplierTable";
    protected static final String JOIN_COITEM_TABLE = "CoitemTable";
    protected static final String JOIN_ORDER_TABLE_FULL_NAME = '"' + JOIN_SCHEMA + "\".\"" + JOIN_ORDER_TABLE + '"';
    protected static final String JOIN_CUSTOMER_TABLE_FULL_NAME = '"' + JOIN_SCHEMA + "\".\"" + JOIN_CUSTOMER_TABLE + '"';
    protected static final String JOIN_ITEM_TABLE_FULL_NAME = '"' + JOIN_SCHEMA + "\".\"" + JOIN_ITEM_TABLE + '"';
    protected static final String JOIN_SUPPLIER_TABLE_FULL_NAME = '"' + JOIN_SCHEMA + "\".\"" + JOIN_SUPPLIER_TABLE + '"';
    protected static final String JOIN_COITEM_TABLE_FULL_NAME = '"' + JOIN_SCHEMA + "\".\"" + JOIN_COITEM_TABLE + '"';

    private static final Map<String,String> tableDDLMap;
    
    static {
        ImmutableMap.Builder<String,String> builder = ImmutableMap.builder();
        builder.put(JOIN_ORDER_TABLE_FULL_NAME, "create table " + JOIN_ORDER_TABLE_FULL_NAME +
                "   (\"order_id\" varchar(15) not null primary key, " +
                "    \"customer_id\" varchar(10), " +
                "    \"item_id\" varchar(10), " +
                "    price integer, " +
                "    quantity integer, " +
                "    date timestamp) IMMUTABLE_ROWS=true");
        builder.put(JOIN_CUSTOMER_TABLE_FULL_NAME, "create table " + JOIN_CUSTOMER_TABLE_FULL_NAME +
                "   (\"customer_id\" varchar(10) not null primary key, " +
                "    name varchar, " +
                "    phone varchar(12), " +
                "    address varchar, " +
                "    loc_id varchar(5), " +
                "    date date) IMMUTABLE_ROWS=true");
        builder.put(JOIN_ITEM_TABLE_FULL_NAME, "create table " + JOIN_ITEM_TABLE_FULL_NAME +
                "   (\"item_id\" varchar(10) not null primary key, " +
                "    name varchar, " +
                "    price integer, " +
                "    discount1 integer, " +
                "    discount2 integer, " +
                "    \"supplier_id\" varchar(10), " +
                "    description varchar)");
        builder.put(JOIN_SUPPLIER_TABLE_FULL_NAME, "create table " + JOIN_SUPPLIER_TABLE_FULL_NAME +
                "   (\"supplier_id\" varchar(10) not null primary key, " +
                "    name varchar, " +
                "    phone varchar(12), " +
                "    address varchar, " +
                "    loc_id varchar(5))");
        builder.put(JOIN_COITEM_TABLE_FULL_NAME, "create table " + JOIN_COITEM_TABLE_FULL_NAME +
                "   (item_id varchar(10) NOT NULL, " +
                "    item_name varchar NOT NULL, " +
                "    co_item_id varchar(10), " +
                "    co_item_name varchar " +
                "   CONSTRAINT pk PRIMARY KEY (item_id, item_name)) " +
                "   SALT_BUCKETS=4");
        tableDDLMap = builder.build();
    }
    
    protected String seqName;
    protected String schemaName;
    protected final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    protected final String[] plans;
    private final String[] indexDDL;
    private final Map<String,String> virtualNameToRealNameMap = Maps.newHashMap();
    
    public BaseJoinIT(String[] indexDDL, String[] plans) {
        this.indexDDL = indexDDL;
        this.plans = plans;
    }
    
    protected String getTableName(Connection conn, String virtualName) throws Exception {
        String realName = virtualNameToRealNameMap.get(virtualName);
        if (realName == null) {
            realName = SchemaUtil.getTableName(schemaName, generateUniqueName());
            virtualNameToRealNameMap.put(virtualName, realName);
            createTable(conn, virtualName, realName);
            initValues(conn, virtualName, realName);
            createIndexes(conn, virtualName, realName);
        }
        return realName;
    }
    
    protected String getDisplayTableName(Connection conn, String virtualName) throws Exception {
        return getTableName(conn, virtualName);
    }

    private void createTable(Connection conn, String virtualName, String realName) throws SQLException {
        String ddl = tableDDLMap.get(virtualName);
        if (ddl == null) {
            throw new IllegalStateException("Expected to find " + virtualName + " in " + tableDDLMap);
        }
        ddl =  ddl.replace(virtualName, realName);
        conn.createStatement().execute(ddl);
    }

    @Before
    public void createSchema() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            schemaName = "S_" + generateUniqueName();
            seqName = "SEQ_" + generateUniqueName();
            conn.createStatement().execute("CREATE SEQUENCE " + seqName);
        } finally {
            conn.close();
        }
    }
    
    private String translateToVirtualPlan(String actualPlan) {
        int size = virtualNameToRealNameMap.size();
        String[] virtualNames = new String[size+1];
        String[] realNames = new String[size+1];
        int count = 0;
        for (Map.Entry<String, String>entry : virtualNameToRealNameMap.entrySet()) {
            virtualNames[count] = entry.getKey();
            realNames[count] = entry.getValue();
            count++;
        }
        realNames[count] = schemaName;
        virtualNames[count]= JOIN_SCHEMA;
        String convertedPlan =  StringUtil.replace(actualPlan, realNames, virtualNames);
        return convertedPlan;
    }
    
    protected void assertPlansMatch(String virtualPlanRegEx, String actualPlan) {
        String convertedPlan = translateToVirtualPlan(actualPlan);
        assertTrue("\"" + convertedPlan + "\" does not match \"" + virtualPlanRegEx + "\"", Pattern.matches(virtualPlanRegEx, convertedPlan));
    }
    
    protected void assertPlansEqual(String virtualPlan, String actualPlan) {
        String convertedPlan = translateToVirtualPlan(actualPlan);
        assertEquals(virtualPlan, convertedPlan);
    }
    
    private static void initValues(Connection conn, String virtualName, String realName) throws Exception {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        if (virtualName.equals(JOIN_CUSTOMER_TABLE_FULL_NAME)) {
            // Insert into customer table
            PreparedStatement stmt = conn.prepareStatement(
                    "upsert into " + realName +
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
        } else if (virtualName.equals(JOIN_ITEM_TABLE_FULL_NAME)) {
        
            // Insert into item table
            PreparedStatement stmt = conn.prepareStatement(
                    "upsert into " + realName +
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
        } else if (virtualName.equals(JOIN_SUPPLIER_TABLE_FULL_NAME)) {

            // Insert into supplier table
            PreparedStatement stmt = conn.prepareStatement(
                    "upsert into " + realName +
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
        } else if (virtualName.equals(JOIN_ORDER_TABLE_FULL_NAME)) {

            // Insert into order table
            PreparedStatement stmt = conn.prepareStatement(
                    "upsert into " + realName +
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
        } else if (virtualName.equals(JOIN_COITEM_TABLE_FULL_NAME)) {
            // Insert into coitem table
            PreparedStatement stmt = conn.prepareStatement(
                    "upsert into " + realName + 
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
        }

        conn.commit();
    }

    protected void createIndexes(Connection conn, String virtualName, String realName) throws Exception {
        if (indexDDL != null && indexDDL.length > 0) {
            for (String ddl : indexDDL) {
                String newDDL =  ddl.replace(virtualName, realName);
                if (!newDDL.equals(ddl)) {
                    conn.createStatement().execute(newDDL);
                }
            }
        }
    }
    
}
