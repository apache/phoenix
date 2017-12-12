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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;

import com.google.common.collect.Lists;


public class QueryMoreIT extends ParallelStatsDisabledIT {
    
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
        String[] tenantIds = new String[] {"T1_" + generateUniqueName(), "T2_" + generateUniqueName(), "T3_" + generateUniqueName()};
        int numRowsPerTenant = 10;
        String cursorTableName = generateUniqueName();
        String base_history_table = generateUniqueName();
        this.dataTableName = base_history_table + (dataTableSalted ? "_SALTED" : "");
        String cursorTableDDL = "CREATE TABLE IF NOT EXISTS " + 
                cursorTableName +  " (\n" +  
                "TENANT_ID VARCHAR NOT NULL\n," +  
                "QUERY_ID VARCHAR(15) NOT NULL,\n" +
                "CURSOR_ORDER BIGINT NOT NULL \n" + 
                "CONSTRAINT CURSOR_TABLE_PK PRIMARY KEY (TENANT_ID, QUERY_ID, CURSOR_ORDER)) "+
                "SALT_BUCKETS = 4, TTL=86400";
        String baseDataTableDDL = "CREATE TABLE IF NOT EXISTS " +
                dataTableName + " (\n" + 
                "TENANT_ID VARCHAR NOT NULL,\n" +
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
        String tableOrViewName = queryAgainstTenantSpecificView ? ("HISTORY_TABLE_" + tenantId) : dataTableName;
        
        assertEquals(numRowsPerTenant, upsertSelectRecordsInCursorTableForTenant(tableOrViewName, queryAgainstTenantSpecificView, tenantId, cursorQueryId,
            cursorTableName));
        
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
        String[] cursorIds = getRecordsOutofCursorTable(tableOrViewName, queryAgainstTenantSpecificView, tenantId, cursorQueryId, startOrder, endOrder,
            cursorTableName);
        assertEquals(numRecordsThatShouldBeRetrieved, cursorIds.length);
        // now query and fetch first batch of records.
        List<String> historyIds = doQueryMore(queryAgainstTenantSpecificView, tenantId, tableOrViewName, cursorIds);
        // assert that history ids match for this tenant
        assertEquals(historyIdsPerTenant.get(tenantId).subList(startOrder, endOrder), historyIds);
        
        // get the next batch of cursor ids out of the cursor table.
        cursorIds = getRecordsOutofCursorTable(tableOrViewName, queryAgainstTenantSpecificView, tenantId, cursorQueryId, startOrder + numRecordsThatShouldBeRetrieved, endOrder + numRecordsThatShouldBeRetrieved,
            cursorTableName);
        assertEquals(numRecordsThatShouldBeRetrieved, cursorIds.length);
        // now query and fetch the next batch of records.
        historyIds = doQueryMore(queryAgainstTenantSpecificView, tenantId, tableOrViewName, cursorIds);
        // assert that the history ids match for this tenant
        assertEquals(historyIdsPerTenant.get(tenantId).subList(startOrder + numRecordsThatShouldBeRetrieved, endOrder+ numRecordsThatShouldBeRetrieved), historyIds);
        
         // get the next batch of cursor ids out of the cursor table.
        cursorIds = getRecordsOutofCursorTable(tableOrViewName, queryAgainstTenantSpecificView, tenantId, cursorQueryId, startOrder + 2 * numRecordsThatShouldBeRetrieved, endOrder + 2 * numRecordsThatShouldBeRetrieved,
            cursorTableName);
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
    
    private int upsertSelectRecordsInCursorTableForTenant(String tableOrViewName, boolean queryAgainstTenantView, String tenantId, String cursorQueryId,
        final String cursorTable) throws Exception {
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
            String upsertSelectDML = "UPSERT INTO " + cursorTable + " " +
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
    
    private String[] getRecordsOutofCursorTable(String tableOrViewName, boolean queryAgainstTenantSpecificView, String tenantId, String cursorQueryId,
        int startOrder, int endOrder, final String cursorTable) throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        List<String> pkIds = new ArrayList<String>();
        String cols = queryAgainstTenantSpecificView ? "PARENT_ID, CREATED_DATE, ENTITY_HISTORY_ID" : "TENANT_ID, PARENT_ID, CREATED_DATE, ENTITY_HISTORY_ID";
        String dynCols = queryAgainstTenantSpecificView ? "(PARENT_ID CHAR(15), CREATED_DATE DATE, ENTITY_HISTORY_ID CHAR(15))" : "(TENANT_ID CHAR(15), PARENT_ID CHAR(15), CREATED_DATE DATE, ENTITY_HISTORY_ID CHAR(15))";
        String selectCursorSql = "SELECT " + cols + " " +
            "FROM " + cursorTable + " \n" +
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
            pkIds.add(Base64.encodeBytes(PhoenixRuntime.encodeColumnValues(conn, tableOrViewName.toUpperCase(), values, columns)));
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
            Object[] pkParts = PhoenixRuntime.decodeColumnValues(conn, tableName.toUpperCase(), Base64.decode(cursorIds[i]), columns);
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
        initEntityHistoryTableValues("abcd", getDefaultSplits("abcd"), date, null);
        String query = "SELECT NEW_VALUE, NEW_VALUE FROM " + TestUtil.ENTITY_HISTORY_TABLE_NAME + " LIMIT 1";
        ResultSet rs = DriverManager.getConnection(getUrl()).createStatement().executeQuery(query);
        assertTrue(rs.next());
        rs.getObject("NEW_VALUE");
        assertFalse(rs.next());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testNullBigDecimalWithScale() throws Exception {
        final String table = generateUniqueName();
        final Connection conn = DriverManager.getConnection(getUrl());
        conn.setAutoCommit(true);
        try (Statement stmt = conn.createStatement()) {
            assertFalse(stmt.execute("CREATE TABLE IF NOT EXISTS " + table + " (\n" +
                "PK VARCHAR(15) NOT NULL\n," +
                "\"DEC\" DECIMAL,\n" +
                "CONSTRAINT TABLE_PK PRIMARY KEY (PK))"));
        }

        try (PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + table + " (PK, \"DEC\") VALUES(?, ?)")) {
            stmt.setString(1, "key");
            stmt.setBigDecimal(2, null);
            assertFalse(stmt.execute());
            assertEquals(1, stmt.getUpdateCount());
        }

        try (Statement stmt = conn.createStatement()) {
            final ResultSet rs = stmt.executeQuery("SELECT * FROM " + table);
            assertNotNull(rs);
            assertTrue(rs.next());
            assertEquals("key", rs.getString(1));
            assertNull(rs.getBigDecimal(2));
            assertNull(rs.getBigDecimal(2, 10));
        }
    }
    
    // FIXME: this repros PHOENIX-3382, but turned up two more issues:
    // 1) PHOENIX-3383 Comparison between descending row keys used in RVC is reverse
    // 2) PHOENIX-3384 Optimize RVC expressions for non leading row key columns
    @Test
    public void testRVCOnDescWithLeadingPKEquality() throws Exception {
        final Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = generateUniqueName();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE " + fullTableName + "(\n" + 
                    "    ORGANIZATION_ID CHAR(15) NOT NULL,\n" + 
                    "    SCORE DOUBLE NOT NULL,\n" + 
                    "    ENTITY_ID CHAR(15) NOT NULL\n" + 
                    "    CONSTRAINT PAGE_SNAPSHOT_PK PRIMARY KEY (\n" + 
                    "        ORGANIZATION_ID,\n" + 
                    "        SCORE DESC,\n" + 
                    "        ENTITY_ID DESC\n" + 
                    "    )\n" + 
                    ") MULTI_TENANT=TRUE");
        }
        
        conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES ('org1',3,'01')");
        conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES ('org1',2,'04')");
        conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES ('org1',2,'03')");
        conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES ('org1',1,'02')");
        conn.commit();

        // FIXME: PHOENIX-3383
        // This comparison is really backwards: it should be (score, entity_id) < (2, '04'),
        // but because we're matching a descending key, our comparison has to be switched.
        try (Statement stmt = conn.createStatement()) {
            final ResultSet rs = stmt.executeQuery("SELECT entity_id, score\n" + 
                    "FROM " + fullTableName + "\n" + 
                    "WHERE organization_id = 'org1'\n" + 
                    "AND (score, entity_id) > (2, '04')\n" + 
                    "ORDER BY score DESC, entity_id DESC\n" + 
                    "LIMIT 3");
            assertTrue(rs.next());
            assertEquals("03", rs.getString(1));
            assertEquals(2.0, rs.getDouble(2), 0.001);
            assertTrue(rs.next());
            assertEquals("02", rs.getString(1));
            assertEquals(1.0, rs.getDouble(2), 0.001);
            assertFalse(rs.next());
        }
        // FIXME: PHOENIX-3384
        // It should not be necessary to specify organization_id in this query
        try (Statement stmt = conn.createStatement()) {
            final ResultSet rs = stmt.executeQuery("SELECT entity_id, score\n" + 
                    "FROM " + fullTableName + "\n" + 
                    "WHERE organization_id = 'org1'\n" + 
                    "AND (organization_id, score, entity_id) > ('org1', 2, '04')\n" + 
                    "ORDER BY score DESC, entity_id DESC\n" + 
                    "LIMIT 3");
            assertTrue(rs.next());
            assertEquals("03", rs.getString(1));
            assertEquals(2.0, rs.getDouble(2), 0.001);
            assertTrue(rs.next());
            assertEquals("02", rs.getString(1));
            assertEquals(1.0, rs.getDouble(2), 0.001);
            assertFalse(rs.next());
        }
    }
    
    @Test
    public void testSingleDescPKColumnComparison() throws Exception {
        final Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = generateUniqueName();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE " + fullTableName + "(\n" + 
                    "    ORGANIZATION_ID CHAR(15) NOT NULL,\n" + 
                    "    SCORE DOUBLE NOT NULL,\n" + 
                    "    ENTITY_ID CHAR(15) NOT NULL\n" + 
                    "    CONSTRAINT PAGE_SNAPSHOT_PK PRIMARY KEY (\n" + 
                    "        ORGANIZATION_ID,\n" + 
                    "        SCORE DESC,\n" + 
                    "        ENTITY_ID DESC\n" + 
                    "    )\n" + 
                    ") MULTI_TENANT=TRUE");
        }
        
        conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES ('org1',3,'01')");
        conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES ('org1',2,'04')");
        conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES ('org1',2,'03')");
        conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES ('org1',1,'02')");
        conn.commit();

        try (Statement stmt = conn.createStatement()) {
            // Even though SCORE is descending, the > comparison makes sense logically
            // and doesn't have to be reversed as RVC expression comparisons do (which
            // is really just a bug - see PHOENIX-3383).
            final ResultSet rs = stmt.executeQuery("SELECT entity_id, score\n" + 
                    "FROM " + fullTableName + "\n" + 
                    "WHERE organization_id = 'org1'\n" + 
                    "AND score > 2.0\n" + 
                    "ORDER BY score DESC\n" + 
                    "LIMIT 3");
            assertTrue(rs.next());
            assertEquals("01", rs.getString(1));
            assertEquals(3.0, rs.getDouble(2), 0.001);
            assertFalse(rs.next());
        }
    }

    @Test
    public void testMutationBatch() throws Exception {
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty(QueryServices.MUTATE_BATCH_SIZE_ATTRIB, "10");
        connectionProperties.setProperty(QueryServices.MUTATE_BATCH_SIZE_BYTES_ATTRIB, "128");
        PhoenixConnection connection = (PhoenixConnection) DriverManager.getConnection(getUrl(), connectionProperties);
        String fullTableName = generateUniqueName();
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE " + fullTableName + "(\n" +
                "    ORGANIZATION_ID CHAR(15) NOT NULL,\n" +
                "    SCORE DOUBLE NOT NULL,\n" +
                "    ENTITY_ID CHAR(15) NOT NULL\n" +
                "    CONSTRAINT PAGE_SNAPSHOT_PK PRIMARY KEY (\n" +
                "        ORGANIZATION_ID,\n" +
                "        SCORE DESC,\n" +
                "        ENTITY_ID DESC\n" +
                "    )\n" +
                ") MULTI_TENANT=TRUE");
        }
        upsertRows(connection, fullTableName);
        connection.commit();
        assertEquals(2L, connection.getMutationState().getBatchCount());
        
        // set the batch size (rows) to 1 
        connectionProperties.setProperty(QueryServices.MUTATE_BATCH_SIZE_ATTRIB, "1");
        connectionProperties.setProperty(QueryServices.MUTATE_BATCH_SIZE_BYTES_ATTRIB, "128");
        connection = (PhoenixConnection) DriverManager.getConnection(getUrl(), connectionProperties);
        upsertRows(connection, fullTableName);
        connection.commit();
        // each row should be in its own batch
        assertEquals(4L, connection.getMutationState().getBatchCount());
    }
    
    private void upsertRows(PhoenixConnection conn, String fullTableName) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement("upsert into " + fullTableName +
                " (organization_id, entity_id, score) values (?,?,?)");
        for (int i = 0; i < 4; i++) {
            stmt.setString(1, "AAAA" + i);
            stmt.setString(2, "BBBB" + i);
            stmt.setInt(3, 1);
            stmt.execute();
        }
    }
}
