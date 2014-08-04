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

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.util.Base64;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

@Category(HBaseManagedTimeTest.class)
public class QueryMoreIT extends BaseHBaseManagedTimeIT {
    
    //Data table - multi-tenant = true, salted = true 
    @Test
    public void testQueryMore1() throws Exception {
        testQueryMore(true, true);
    }
    
    //Data table - multi-tenant = false, salted = true 
    @Test
    public void testQueryMore2() throws Exception {
        testQueryMore(false, true);
    }
    
    //Data table - multi-tenant = false, salted = false
    @Test
    public void testQueryMore3() throws Exception {
        testQueryMore(false, false);
    }
    
    //Data table - multi-tenant = true, salted = false 
    @Test
    public void testQueryMore4() throws Exception {
        testQueryMore(true, false);
    }
    
    private void testQueryMore(boolean dataTableMultiTenant, boolean dataTableSalted) throws Exception {
        String[] tenantIds = new String[] {"00Dxxxxxtenant1", "00Dxxxxxtenant2", "00Dxxxxxtenant3"};
        int numRowsPerTenant = 10;
        String cursorTableName = "CURSOR_TABLE";
        String dataTableName = "BASE_HISTORY_TABLE" + (dataTableMultiTenant ? "_MULTI" : "") + (dataTableSalted ? "_SALTED" : "");
        String cursorTableDDL = "CREATE TABLE IF NOT EXISTS " + 
                cursorTableName +  " (\n" +  
                "TENANT_ID VARCHAR(15) NOT NULL\n," +  
                "QUERY_ID VARCHAR(15) NOT NULL,\n" +
                "CURSOR_ORDER BIGINT NOT NULL\n" + 
                "CONSTRAINT CURSOR_TABLE_PK PRIMARY KEY (TENANT_ID, QUERY_ID, CURSOR_ORDER)) "+
                "SALT_BUCKETS = 4, TTL=86400";
        String baseDataTableDDL = "CREATE TABLE IF NOT EXISTS " +
                dataTableName + " (\n" + 
                "TENANT_ID CHAR(15) NOT NULL,\n" +
                "PARENT_ID CHAR(15) NOT NULL,\n" + 
                "CREATED_DATE DATE NOT NULL,\n" + 
                "ENTITY_HISTORY_ID CHAR(15) NOT NULL,\n" + 
                "DATA_TYPE VARCHAR,\n" + 
                "OLDVAL_STRING VARCHAR,\n" + 
                "NEWVAL_STRING VARCHAR\n" + 
                "CONSTRAINT PK PRIMARY KEY(TENANT_ID, PARENT_ID, CREATED_DATE DESC, ENTITY_HISTORY_ID)) " + 
                "VERSIONS = 1, MULTI_TENANT = true, SALT_BUCKETS = 4";
        
        //create cursor and data tables.
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(cursorTableDDL);
        conn.createStatement().execute(baseDataTableDDL);
        conn.close();
        
        //upsert rows in the data table.
        Map<String, List<String>> historyIdsPerTenant = createHistoryTableRows(dataTableName, tenantIds, numRowsPerTenant);
        
        String tenantId = tenantIds[0];
        String cursorQueryId = "00TcursrqueryId";
        String tenantViewName = dataTableMultiTenant ? ("HISTORY_TABLE" + "_" + tenantId) : null;
        assertEquals(numRowsPerTenant, upsertSelectRecordsInCursorTableForTenant(dataTableName, dataTableMultiTenant, tenantId, tenantViewName, cursorQueryId));
        
        Connection conn2 = DriverManager.getConnection(getUrl());
        ResultSet rs = conn2.createStatement().executeQuery("SELECT count(*) from " + cursorTableName);
        rs.next();
        assertEquals(numRowsPerTenant, rs.getInt(1));
        conn2.close();
        
        int startOrder = 0;
        int endOrder = 5;
        int numRecordsThatShouldBeRetrieved = 5;
        
        //get first batch of cursor ids out of the cursor table.
        String[] cursorIds = getRecordsOutofCursorTable(dataTableName, tenantId, cursorQueryId, startOrder, endOrder, numRecordsThatShouldBeRetrieved);
        assertEquals(numRecordsThatShouldBeRetrieved, cursorIds.length);
        
        //now query against the tenant view and fetch first batch of records.
        List<String> historyIds = doQueryMore(dataTableName, dataTableMultiTenant, tenantId, tenantViewName, cursorIds);
        assertEquals(historyIdsPerTenant.get(tenantId).subList(startOrder, endOrder), historyIds);
        
        cursorIds = getRecordsOutofCursorTable(dataTableName, tenantId, cursorQueryId, startOrder + 5, endOrder + 5, numRecordsThatShouldBeRetrieved);
        assertEquals(numRecordsThatShouldBeRetrieved, cursorIds.length);
        historyIds = doQueryMore(dataTableName, dataTableMultiTenant, tenantId, tenantViewName, cursorIds);
        assertEquals(historyIdsPerTenant.get(tenantId).subList(startOrder + 5, endOrder+ 5), historyIds);
    }
    
    private Map<String, List<String>> createHistoryTableRows(String dataTableName, String[] tenantIds, int numRowsPerTenant) throws Exception {
        String upsertDML = "UPSERT INTO " + dataTableName + " VALUES (?, ?, ?, ?, ?, ?, ?)";
        Connection conn = DriverManager.getConnection(getUrl());
        Map<String, List<String>> historyIdsForTenant = new HashMap<String, List<String>>();
        try {
            PreparedStatement stmt = conn.prepareStatement(upsertDML);
            for (int j = 0; j < tenantIds.length; j++) {
                List<String> parentIds = new ArrayList<String>();
                for (int i = 0; i < numRowsPerTenant; i++) {
                    stmt.setString(1, tenantIds[j]);
                    String parentId = "parentId" + i;
                    stmt.setString(2, parentId);
                    stmt.setDate(3, new Date(100));
                    String historyId = "historyId" + i; 
                    stmt.setString(4, historyId);
                    stmt.setString(5, "datatype");
                    stmt.setString(6, "oldval");
                    stmt.setString(7, "newval");
                    stmt.executeUpdate();
                    parentIds.add(historyId);
                }
                historyIdsForTenant.put(tenantIds[j], parentIds);
            }
            conn.commit();
            return historyIdsForTenant;
        } finally {
            conn.close();
        }
    }
    
    private int upsertSelectRecordsInCursorTableForTenant(String baseTableName, boolean dataTableMultiTenant, String tenantId, String tenantViewName, String cursorQueryId) throws Exception {
        String sequenceName = "\"" + tenantId + "_SEQ\"";
        Connection conn = dataTableMultiTenant ? getTenantSpecificConnection(tenantId) : DriverManager.getConnection(getUrl());
        
        // Create a sequence. This sequence is used to fill cursor_order column for each row inserted in the cursor table.
        conn.createStatement().execute("CREATE SEQUENCE " + sequenceName + " CACHE " + Long.MAX_VALUE);
        conn.setAutoCommit(true);
        if (dataTableMultiTenant) {
            createTenantSpecificViewIfNecessary(baseTableName, tenantViewName, conn);
        }
        try {
            String tableName = dataTableMultiTenant ? tenantViewName : baseTableName;
            String tenantIdFilter = dataTableMultiTenant ? "" : " WHERE TENANT_ID = ? ";
            
            // Using dynamic columns, we can use the same cursor table for storing primary keys for all the tables.  
            String upsertSelectDML = "UPSERT INTO CURSOR_TABLE " +
                                     "(TENANT_ID, QUERY_ID, CURSOR_ORDER, PARENT_ID CHAR(15), CREATED_DATE DATE, ENTITY_HISTORY_ID CHAR(15)) " + 
                                     "SELECT ?, ?, NEXT VALUE FOR " + sequenceName + ", PARENT_ID, CREATED_DATE, ENTITY_HISTORY_ID " +
                                     " FROM " + tableName + tenantIdFilter;
            PreparedStatement stmt = conn.prepareStatement(upsertSelectDML);
            stmt.setString(1, tenantId);
            stmt.setString(2, cursorQueryId);
            if (!dataTableMultiTenant)  {
                stmt.setString(3, tenantId);
            }
            int numRecords = stmt.executeUpdate();
            return numRecords;
        } finally {
            try {
                conn.createStatement().execute("DROP SEQUENCE " + sequenceName);
            } finally {
                conn.close();
            }
        }
    }
    
    private Connection getTenantSpecificConnection(String tenantId) throws Exception {
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        return DriverManager.getConnection(getUrl(), props);
    }
    
    private String createTenantSpecificViewIfNecessary(String baseTableName, String tenantViewName, Connection tenantConn) throws Exception {
        tenantConn.createStatement().execute("CREATE VIEW IF NOT EXISTS " + tenantViewName + " AS SELECT * FROM " + baseTableName);
        return tenantViewName;
    }
    
    private String[] getRecordsOutofCursorTable(String dataTableName, String tenantId, String cursorQueryId, int startOrder, int endOrder, int numRecordsThatShouldBeRetrieved) throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        List<String> pkIds = Lists.newArrayListWithCapacity(numRecordsThatShouldBeRetrieved);

        String selectCursorSql = "SELECT TENANT_ID, PARENT_ID, CREATED_DATE, ENTITY_HISTORY_ID " +
                "FROM CURSOR_TABLE \n" +
                "(TENANT_ID CHAR(15), PARENT_ID CHAR(15), CREATED_DATE DATE, ENTITY_HISTORY_ID CHAR(15)) \n" + 
                "WHERE TENANT_ID = ? AND \n" +  
                "QUERY_ID = ? AND \n" + 
                "CURSOR_ORDER > ? AND \n" + 
                "CURSOR_ORDER <= ?";

        PreparedStatement stmt = conn.prepareStatement(selectCursorSql);
        stmt.setString(1, tenantId);
        stmt.setString(2, cursorQueryId);
        stmt.setInt(3, startOrder);
        stmt.setInt(4, endOrder);

        ResultSet rs = stmt.executeQuery();
        while(rs.next()) {
            Object[] values = new Object[4];
            for (int i = 0; i < 4; i++) {
                values[i] = rs.getObject(i + 1);
            }
            pkIds.add(Base64.encodeBytes(PhoenixRuntime.encodePK(conn, dataTableName, values)));
        }
        return pkIds.toArray(new String[pkIds.size()]);
    }
    
    private List<String> doQueryMore(String dataTableName, boolean dataTableMultiTenant, String tenantId, String tenantViewName, String[] cursorIds) throws Exception {
        Connection tenantConn = dataTableMultiTenant ? getTenantSpecificConnection(tenantId) : DriverManager.getConnection(getUrl());
        String tableName = dataTableMultiTenant ? tenantViewName : dataTableName;
        StringBuilder sb = new StringBuilder();
        String where = dataTableMultiTenant ? " WHERE (PARENT_ID, CREATED_DATE, ENTITY_HISTORY_ID) IN " : " WHERE (TENANT_ID, PARENT_ID, CREATED_DATE, ENTITY_HISTORY_ID) IN ";
        sb.append("SELECT ENTITY_HISTORY_ID FROM " + tableName +  where);
        int numPkCols = dataTableMultiTenant ? 3 : 4;
        String query = addRvcInBinds(sb, cursorIds.length, numPkCols);
        PreparedStatement stmt = tenantConn.prepareStatement(query);
        int bindCounter = 1;
        for (int i = 0; i < cursorIds.length; i++) {
            Connection globalConn = DriverManager.getConnection(getUrl());
            Object[] pkParts = PhoenixRuntime.decodePK(globalConn, dataTableName, Base64.decode(cursorIds[i]));
            globalConn.close();
            //start at index 1 to  ignore organizationId.
            int offset = dataTableMultiTenant ? 1 : 0;
            for (int j = offset; j < pkParts.length; j++) {
                stmt.setObject(bindCounter++, pkParts[j]);
            }
        }
        ResultSet rs = stmt.executeQuery();
        List<String> historyIds = new ArrayList<String>();
        while(rs.next()) {
            historyIds.add(rs.getString(1));
        }
        return historyIds;
    }
    
    private String addRvcInBinds(StringBuilder sb, int numRvcs, int numPkCols) {
        sb.append("(");
        for (int i = 0 ; i < numRvcs; i++) {
            for (int j = 0; j < numPkCols; j++) {
                if (j == 0) {
                    sb.append("(");
                }
                sb.append("?");
                if (j < numPkCols - 1) {
                    sb.append(",");
                } else {
                    sb.append(")");
                }
            }
            if (i < numRvcs - 1) {
                sb.append(",");
            }
        }
        sb.append(")");
        return sb.toString();
    }
    
}
