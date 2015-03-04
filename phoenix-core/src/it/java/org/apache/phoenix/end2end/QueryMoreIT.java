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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;

import com.google.common.collect.Lists;


public class QueryMoreIT extends BaseHBaseManagedTimeIT {
    
    private String dataTableName;
    //queryAgainstTenantSpecificView = true, dataTableSalted = true 
    @Test
    public void testQueryMore1() throws Exception {
        testQueryMore(true, true);
    }
    
    //queryAgainstTenantSpecificView = false, dataTableSalted = true 
    @Test
    public void testQueryMore2() throws Exception {
        testQueryMore(false, true);
    }
    
    //queryAgainstTenantSpecificView = false, dataTableSalted = false
    @Test
    public void testQueryMore3() throws Exception {
        testQueryMore(false, false);
    }
    
    //queryAgainstTenantSpecificView = true, dataTableSalted = false 
    @Test
    public void testQueryMore4() throws Exception {
        testQueryMore(true, false);
    }
    
    private void testQueryMore(boolean queryAgainstTenantSpecificView, boolean dataTableSalted) throws Exception {
        String[] tenantIds = new String[] {"00Dxxxxxtenant1", "00Dxxxxxtenant2", "00Dxxxxxtenant3"};
        int numRowsPerTenant = 10;
        String cursorTableName = "CURSOR_TABLE";
        this.dataTableName = "BASE_HISTORY_TABLE" + (dataTableSalted ? "_SALTED" : "");
        String cursorTableDDL = "CREATE TABLE IF NOT EXISTS " + 
                cursorTableName +  " (\n" +  
                "TENANT_ID VARCHAR(15) NOT NULL\n," +  
                "QUERY_ID VARCHAR(15) NOT NULL,\n" +
                "CURSOR_ORDER BIGINT NOT NULL \n" + 
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
                "VERSIONS = 1, MULTI_TENANT = true" + (dataTableSalted ? ", SALT_BUCKETS = 4" : "");
        
        //create cursor and data tables.
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(cursorTableDDL);
        conn.createStatement().execute(baseDataTableDDL);
        conn.close();
        
        //upsert rows in the data table for all the tenantIds
        Map<String, List<String>> historyIdsPerTenant = createHistoryTableRows(dataTableName, tenantIds, numRowsPerTenant);
        
        // assert query more for tenantId -> tenantIds[0]
        String tenantId = tenantIds[0];
        String cursorQueryId = "00TcursrqueryId";
        String tableOrViewName = queryAgainstTenantSpecificView ? ("\"HISTORY_TABLE" + "_" + tenantId + "\"") : dataTableName;
        
        assertEquals(numRowsPerTenant, upsertSelectRecordsInCursorTableForTenant(tableOrViewName, queryAgainstTenantSpecificView, tenantId, cursorQueryId));
        
        /*// assert that the data inserted in cursor table matches the data in the data table for tenantId.
        String selectDataTable = "SELECT TENANT_ID, PARENT_ID, CREATED_DATE, ENTITY_HISTORY_ID FROM BASE_HISTORY_TABLE WHERE TENANT_ID = ? ";
        String selectCursorTable = "SELECT TENANT_ID, PARENT_ID, CREATED_DATE, ENTITY_HISTORY_ID FROM  CURSOR_TABLE (PARENT_ID CHAR(15), CREATED_DATE DATE, ENTITY_HISTORY_ID CHAR(15)) WHERE TENANT_ID = ? ";
        
        PreparedStatement stmtData = DriverManager.getConnection(getUrl()).prepareStatement(selectDataTable);
        stmtData.setString(1, tenantId);
        ResultSet rsData = stmtData.executeQuery();
        
        PreparedStatement stmtCursor = DriverManager.getConnection(getUrl()).prepareStatement(selectCursorTable);
        stmtCursor.setString(1, tenantId);
        ResultSet rsCursor = stmtCursor.executeQuery();
        
        while(rsData.next() && rsCursor.next()) {
            assertEquals(rsData.getString("TENANT_ID"), rsCursor.getString("TENANT_ID"));
            assertEquals(rsData.getString("PARENT_ID"), rsCursor.getString("PARENT_ID"));
            assertEquals(rsData.getDate("CREATED_DATE"), rsCursor.getDate("CREATED_DATE"));
            assertEquals(rsData.getString("ENTITY_HISTORY_ID"), rsCursor.getString("ENTITY_HISTORY_ID"));
        }
        
        */
        Connection conn2 = DriverManager.getConnection(getUrl());
        ResultSet rs = conn2.createStatement().executeQuery("SELECT count(*) from " + cursorTableName);
        rs.next();
        assertEquals(numRowsPerTenant, rs.getInt(1));
        conn2.close();
        
        int startOrder = 0;
        int endOrder = 5;
        int numRecordsThatShouldBeRetrieved = numRowsPerTenant/2; // we will test for two rounds of query more.
        
        // get first batch of cursor ids out of the cursor table.
        String[] cursorIds = getRecordsOutofCursorTable(tableOrViewName, queryAgainstTenantSpecificView, tenantId, cursorQueryId, startOrder, endOrder);
        assertEquals(numRecordsThatShouldBeRetrieved, cursorIds.length);
        // now query and fetch first batch of records.
        List<String> historyIds = doQueryMore(queryAgainstTenantSpecificView, tenantId, tableOrViewName, cursorIds);
        // assert that history ids match for this tenant
        assertEquals(historyIdsPerTenant.get(tenantId).subList(startOrder, endOrder), historyIds);
        
        // get the next batch of cursor ids out of the cursor table.
        cursorIds = getRecordsOutofCursorTable(tableOrViewName, queryAgainstTenantSpecificView, tenantId, cursorQueryId, startOrder + numRecordsThatShouldBeRetrieved, endOrder + numRecordsThatShouldBeRetrieved);
        assertEquals(numRecordsThatShouldBeRetrieved, cursorIds.length);
        // now query and fetch the next batch of records.
        historyIds = doQueryMore(queryAgainstTenantSpecificView, tenantId, tableOrViewName, cursorIds);
        // assert that the history ids match for this tenant
        assertEquals(historyIdsPerTenant.get(tenantId).subList(startOrder + numRecordsThatShouldBeRetrieved, endOrder+ numRecordsThatShouldBeRetrieved), historyIds);
        
         // get the next batch of cursor ids out of the cursor table.
        cursorIds = getRecordsOutofCursorTable(tableOrViewName, queryAgainstTenantSpecificView, tenantId, cursorQueryId, startOrder + 2 * numRecordsThatShouldBeRetrieved, endOrder + 2 * numRecordsThatShouldBeRetrieved);
        // assert that there are no more cursorids left for this tenant.
        assertEquals(0, cursorIds.length);
    }
    
    private Map<String, List<String>> createHistoryTableRows(String dataTableName, String[] tenantIds, int numRowsPerTenant) throws Exception {
        String upsertDML = "UPSERT INTO " + dataTableName + " VALUES (?, ?, ?, ?, ?, ?, ?)";
        Connection conn = DriverManager.getConnection(getUrl());
        Map<String, List<String>> historyIdsForTenant = new HashMap<String, List<String>>();
        try {
            PreparedStatement stmt = conn.prepareStatement(upsertDML);
            for (int j = 0; j < tenantIds.length; j++) {
                List<String> historyIds = new ArrayList<String>();
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
                    historyIds.add(historyId);
                }
                historyIdsForTenant.put(tenantIds[j], historyIds);
            }
            conn.commit();
            return historyIdsForTenant;
        } finally {
            conn.close();
        }
    }
    
    private int upsertSelectRecordsInCursorTableForTenant(String tableOrViewName, boolean queryAgainstTenantView, String tenantId, String cursorQueryId) throws Exception {
        String sequenceName = "\"" + tenantId + "_SEQ\"";
        Connection conn = queryAgainstTenantView ? getTenantSpecificConnection(tenantId) : DriverManager.getConnection(getUrl());
        
        // Create a sequence. This sequence is used to fill cursor_order column for each row inserted in the cursor table.
        conn.createStatement().execute("CREATE SEQUENCE " + sequenceName + " CACHE " + Long.MAX_VALUE);
        conn.setAutoCommit(true);
        if (queryAgainstTenantView) {
            createTenantSpecificViewIfNecessary(tableOrViewName, conn);
        }
        try {
            String tenantIdFilter = queryAgainstTenantView ? "" : " WHERE TENANT_ID = ? ";
            
            // Using dynamic columns, we can use the same cursor table for storing primary keys for all the tables.  
            String upsertSelectDML = "UPSERT INTO CURSOR_TABLE " +
                                     "(TENANT_ID, QUERY_ID, CURSOR_ORDER, PARENT_ID CHAR(15), CREATED_DATE DATE, ENTITY_HISTORY_ID CHAR(15)) " + 
                                     "SELECT ?, ?, NEXT VALUE FOR " + sequenceName + ", PARENT_ID, CREATED_DATE, ENTITY_HISTORY_ID " +
                                     " FROM " + tableOrViewName + tenantIdFilter;
            
            PreparedStatement stmt = conn.prepareStatement(upsertSelectDML);
            stmt.setString(1, tenantId);
            stmt.setString(2, cursorQueryId);
            if (!queryAgainstTenantView)  {
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
    
    private String createTenantSpecificViewIfNecessary(String tenantViewName, Connection tenantConn) throws Exception {
        tenantConn.createStatement().execute("CREATE VIEW IF NOT EXISTS " + tenantViewName + " AS SELECT * FROM " + dataTableName);
        return tenantViewName;
    }
    
    private String[] getRecordsOutofCursorTable(String tableOrViewName, boolean queryAgainstTenantSpecificView, String tenantId, String cursorQueryId, int startOrder, int endOrder) throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        List<String> pkIds = new ArrayList<String>();
        String cols = queryAgainstTenantSpecificView ? "PARENT_ID, CREATED_DATE, ENTITY_HISTORY_ID" : "TENANT_ID, PARENT_ID, CREATED_DATE, ENTITY_HISTORY_ID";
        String dynCols = queryAgainstTenantSpecificView ? "(PARENT_ID CHAR(15), CREATED_DATE DATE, ENTITY_HISTORY_ID CHAR(15))" : "(TENANT_ID CHAR(15), PARENT_ID CHAR(15), CREATED_DATE DATE, ENTITY_HISTORY_ID CHAR(15))";
        String selectCursorSql = "SELECT " + cols + " " +
                "FROM CURSOR_TABLE \n" +
                 dynCols +   " \n" + 
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
        @SuppressWarnings("unchecked")
        List<Pair<String, String>> columns = queryAgainstTenantSpecificView ? Lists.newArrayList(new Pair<String, String>(null, "PARENT_ID"), new Pair<String, String>(null, "CREATED_DATE"), new Pair<String, String>(null, "ENTITY_HISTORY_ID")) : Lists.newArrayList(new Pair<String, String>(null, "TENANT_ID"), new Pair<String, String>(null, "PARENT_ID"), new Pair<String, String>(null, "CREATED_DATE"), new Pair<String, String>(null, "ENTITY_HISTORY_ID"));
        while(rs.next()) {
            Object[] values = new Object[columns.size()];
            for (int i = 0; i < columns.size(); i++) {
                values[i] = rs.getObject(i + 1);
            }
            conn = getTenantSpecificConnection(tenantId);
            pkIds.add(Base64.encodeBytes(PhoenixRuntime.encodeValues(conn, tableOrViewName, values, columns)));
        }
        return pkIds.toArray(new String[pkIds.size()]);
    }
    
    private List<String> doQueryMore(boolean queryAgainstTenantView, String tenantId, String tenantViewName, String[] cursorIds) throws Exception {
        Connection conn = queryAgainstTenantView ? getTenantSpecificConnection(tenantId) : DriverManager.getConnection(getUrl());
        String tableName = queryAgainstTenantView ? tenantViewName : dataTableName;
        @SuppressWarnings("unchecked")
        List<Pair<String, String>> columns = queryAgainstTenantView ? Lists.newArrayList(new Pair<String, String>(null, "PARENT_ID"), new Pair<String, String>(null, "CREATED_DATE"), new Pair<String, String>(null, "ENTITY_HISTORY_ID")) : Lists.newArrayList(new Pair<String, String>(null, "TENANT_ID"), new Pair<String, String>(null, "PARENT_ID"), new Pair<String, String>(null, "CREATED_DATE"), new Pair<String, String>(null, "ENTITY_HISTORY_ID"));
        StringBuilder sb = new StringBuilder();
        String where = queryAgainstTenantView ? " WHERE (PARENT_ID, CREATED_DATE, ENTITY_HISTORY_ID) IN " : " WHERE (TENANT_ID, PARENT_ID, CREATED_DATE, ENTITY_HISTORY_ID) IN ";
        sb.append("SELECT ENTITY_HISTORY_ID FROM " + tableName +  where);
        int numPkCols = columns.size();
        String query = addRvcInBinds(sb, cursorIds.length, numPkCols);
        PreparedStatement stmt = conn.prepareStatement(query);
        int bindCounter = 1;
        for (int i = 0; i < cursorIds.length; i++) {
            Object[] pkParts = PhoenixRuntime.decodeValues(conn, tableName, Base64.decode(cursorIds[i]), columns);
            for (int j = 0; j < pkParts.length; j++) {
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
    
    @Test // see - https://issues.apache.org/jira/browse/PHOENIX-1696
    public void testSelectColumnMoreThanOnce() throws Exception {
        Date date = new Date(System.currentTimeMillis());
        initEntityHistoryTableValues("abcd", getDefaultSplits("abcd"), date, 100l);
        String query = "SELECT NEW_VALUE, NEW_VALUE FROM " + TestUtil.ENTITY_HISTORY_TABLE_NAME + " LIMIT 1";
        ResultSet rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(query);
        assertTrue(rs.next());
        rs.getObject("NEW_VALUE");
        assertFalse(rs.next());
    }
}
