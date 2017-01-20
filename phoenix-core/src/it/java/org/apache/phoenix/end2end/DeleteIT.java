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
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.phoenix.util.QueryUtil;
import org.junit.Test;


public class DeleteIT extends ParallelStatsDisabledIT {
    private static final int NUMBER_OF_ROWS = 20;
    private static final int NTH_ROW_NULL = 5;
    
    private static String initTableValues(Connection conn) throws SQLException {
        String tableName = generateUniqueName();
        ensureTableCreated(getUrl(), tableName, "IntIntKeyTest");
        String upsertStmt = "UPSERT INTO " + tableName + " VALUES(?,?)";
        PreparedStatement stmt = conn.prepareStatement(upsertStmt);
        for (int i = 0; i < NUMBER_OF_ROWS; i++) {
            stmt.setInt(1, i);
            if (i % NTH_ROW_NULL != 0) {
                stmt.setInt(2, i * 10);
            } else {
                stmt.setNull(2, Types.INTEGER);
            }
            stmt.execute();
        }
        conn.commit();
        return tableName;
    }

    @Test
    public void testDeleteFilterNoAutoCommit() throws Exception {
        testDeleteFilter(false);
    }
    
    @Test
    public void testDeleteFilterAutoCommit() throws Exception {
        testDeleteFilter(true);
    }
    
    private void testDeleteFilter(boolean autoCommit) throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTableValues(conn);

        assertTableCount(conn, tableName, NUMBER_OF_ROWS);
        
        conn.setAutoCommit(autoCommit);
        String deleteStmt = "DELETE FROM " + tableName + " WHERE 20 = j";
        assertEquals(1,conn.createStatement().executeUpdate(deleteStmt));
        if (!autoCommit) {
            conn.commit();
        }

        assertTableCount(conn, tableName, NUMBER_OF_ROWS - 1);
    }

    @Test
    public void testDeleteByRowAndFilterAutoCommit() throws SQLException {
        testDeleteByFilterAndRow(true);
    }


    @Test
    public void testDeleteByRowAndFilterNoAutoCommit() throws SQLException {
        testDeleteByFilterAndRow(false);
    }

    private void testDeleteByFilterAndRow(boolean autoCommit) throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTableValues(conn);

        assertTableCount(conn, tableName, NUMBER_OF_ROWS);

        conn.setAutoCommit(autoCommit);

        Statement stmt = conn.createStatement();

        // This shouldn't delete anything, because the key matches but the filter doesn't
        assertEquals(0, stmt.executeUpdate("DELETE FROM " + tableName + " WHERE i = 1 AND j = 1"));
        if (!autoCommit) {
            conn.commit();
        }
        assertTableCount(conn, tableName, NUMBER_OF_ROWS);

        // This shouldn't delete anything, because the filter matches but the key doesn't
        assertEquals(0, stmt.executeUpdate("DELETE FROM " + tableName + " WHERE i = -1 AND j = 20"));
        if (!autoCommit) {
            conn.commit();
        }
        assertTableCount(conn, tableName, NUMBER_OF_ROWS);

        // This should do a delete, because both the filter and key match
        assertEquals(1, stmt.executeUpdate("DELETE FROM " + tableName + " WHERE i = 1 AND j = 10"));
        if (!autoCommit) {
            conn.commit();
        }
        assertTableCount(conn, tableName, NUMBER_OF_ROWS - 1);

    }

    private void assertTableCount(Connection conn, String tableName, int expectedNumberOfRows) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("SELECT count(*) FROM " + tableName);
        assertTrue(rs.next());
        assertEquals(expectedNumberOfRows, rs.getInt(1));
        rs.close();
    }
    
    private static void assertIndexUsed (Connection conn, String query, String indexName, boolean expectedToBeUsed) throws SQLException {
        assertIndexUsed(conn, query, Collections.emptyList(), indexName, expectedToBeUsed);
    }

    private static void assertIndexUsed (Connection conn, String query, List<Object> binds, String indexName, boolean expectedToBeUsed) throws SQLException {
            PreparedStatement stmt = conn.prepareStatement("EXPLAIN " + query);
            for (int i = 0; i < binds.size(); i++) {
                stmt.setObject(i+1, binds.get(i));
            }
            ResultSet rs = stmt.executeQuery();
            String explainPlan = QueryUtil.getExplainPlan(rs);
            assertEquals(expectedToBeUsed, explainPlan.contains(" SCAN OVER " + indexName));
   }

    private void testDeleteRange(boolean autoCommit, boolean createIndex) throws Exception {
        testDeleteRange(autoCommit, createIndex, false);
    }

    private void testDeleteRange(boolean autoCommit, boolean createIndex, boolean local) throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = initTableValues(conn);
        String indexName = generateUniqueName();
        String localIndexName = generateUniqueName();

        String indexInUse = indexName;
        if (createIndex) {
            if (local) {
                conn.createStatement().execute("CREATE LOCAL INDEX IF NOT EXISTS " + localIndexName + " ON " + tableName + "(j)");
                indexInUse = tableName;
            } else {
                conn.createStatement().execute("CREATE INDEX IF NOT EXISTS " + indexName + " ON " + tableName + "(j)");
            }
        }
        
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT count(*) FROM " + tableName);
        assertTrue(rs.next());
        assertEquals(NUMBER_OF_ROWS, rs.getInt(1));

        rs = conn.createStatement().executeQuery("SELECT i FROM " + tableName + " WHERE j IS NULL");
        int i = 0, isNullCount = 0;
        while (rs.next()) {
            assertEquals(i,rs.getInt(1));
            i += NTH_ROW_NULL;
            isNullCount++;
        }
        rs = conn.createStatement().executeQuery("SELECT count(*) FROM " + tableName + " WHERE j IS NOT NULL");
        assertTrue(rs.next());
        assertEquals(NUMBER_OF_ROWS-isNullCount, rs.getInt(1));

        String deleteStmt ;
        PreparedStatement stmt;
        conn.setAutoCommit(autoCommit);
        deleteStmt = "DELETE FROM " + tableName + " WHERE i >= ? and i < ?";
        if(!local) {
            assertIndexUsed(conn, deleteStmt, Arrays.<Object>asList(5,10), indexInUse, false);
        }
        stmt = conn.prepareStatement(deleteStmt);
        stmt.setInt(1, 5);
        stmt.setInt(2, 10);
        stmt.execute();
        if (!autoCommit) {
            conn.commit();
        }
        
        String query = "SELECT count(*) FROM " + tableName;
        assertIndexUsed(conn, query, indexInUse, createIndex);
        query = "SELECT count(*) FROM " + tableName;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals(NUMBER_OF_ROWS - (10-5), rs.getInt(1));
        
        deleteStmt = "DELETE FROM " + tableName + " WHERE j IS NULL";
        stmt = conn.prepareStatement(deleteStmt);
        if(!local) {
            assertIndexUsed(conn, deleteStmt, indexInUse, createIndex);
        }
        int deleteCount = stmt.executeUpdate();
        assertEquals(3, deleteCount);
        if (!autoCommit) {
            conn.commit();
        }
        rs = conn.createStatement().executeQuery("SELECT count(*) FROM " + tableName);
        assertTrue(rs.next());
        assertEquals(NUMBER_OF_ROWS - (10-5)-isNullCount+1, rs.getInt(1));
    }
    
    @Test
    public void testDeleteRangeNoAutoCommitNoIndex() throws Exception {
        testDeleteRange(false, false);
    }
    
    @Test
    public void testDeleteRangeAutoCommitNoIndex() throws Exception {
        testDeleteRange(true, false);
    }
    
    @Test
    public void testDeleteRangeNoAutoCommitWithIndex() throws Exception {
        testDeleteRange(false, true, false);
    }

    @Test
    public void testDeleteRangeNoAutoCommitWithLocalIndexIndex() throws Exception {
        testDeleteRange(false, true, true);
    }
    
    @Test
    public void testDeleteRangeAutoCommitWithIndex() throws Exception {
        testDeleteRange(true, true, false);
    }
    
    @Test
    public void testDeleteRangeAutoCommitWithLocalIndex() throws Exception {
        testDeleteRange(true, true, true);
    }

    @Test
    public void testDeleteAllFromTableWithIndexAutoCommitSalting() throws SQLException {
        testDeleteAllFromTableWithIndex(true, true, false);
    }

    @Test
    public void testDeleteAllFromTableWithLocalIndexAutoCommitSalting() throws SQLException {
        testDeleteAllFromTableWithIndex(true, true, true);
    }
    
    @Test
    public void testDeleteAllFromTableWithIndexAutoCommitNoSalting() throws SQLException {
        testDeleteAllFromTableWithIndex(true, false);
    }
    
    @Test
    public void testDeleteAllFromTableWithIndexNoAutoCommitNoSalting() throws SQLException {
        testDeleteAllFromTableWithIndex(false,false);
    }
    
    @Test
    public void testDeleteAllFromTableWithIndexNoAutoCommitSalted() throws SQLException {
        testDeleteAllFromTableWithIndex(false, true, false);
    }
    
    @Test
    public void testDeleteAllFromTableWithLocalIndexNoAutoCommitSalted() throws SQLException {
        testDeleteAllFromTableWithIndex(false, true, true);
    }

    private void testDeleteAllFromTableWithIndex(boolean autoCommit, boolean isSalted) throws SQLException {
        testDeleteAllFromTableWithIndex(autoCommit, isSalted, false);
    }

    private void testDeleteAllFromTableWithIndex(boolean autoCommit, boolean isSalted, boolean localIndex) throws SQLException {
        Connection con = null;
        try {
            con = DriverManager.getConnection(getUrl());
            con.setAutoCommit(autoCommit);

            Statement stm = con.createStatement();
            String tableName = generateUniqueName();
            String s = "CREATE TABLE IF NOT EXISTS " + tableName + "(" +
                    "HOST CHAR(2) NOT NULL," +
                    "DOMAIN VARCHAR NOT NULL, " +
                    "FEATURE VARCHAR NOT NULL, " +
                    "\"DATE\" DATE NOT NULL, \n" + 
                    "USAGE.CORE BIGINT," +
                    "USAGE.DB BIGINT," +
                    "STATS.ACTIVE_VISITOR INTEGER " +
                    "CONSTRAINT PK PRIMARY KEY (HOST, DOMAIN, FEATURE, \"DATE\"))" + (isSalted ? " SALT_BUCKETS=3" : "");
            stm.execute(s);
            String localIndexName = generateUniqueName();
            String indexName = generateUniqueName();
            if (localIndex) {
                stm.execute("CREATE LOCAL INDEX " + localIndexName + " ON " + tableName + " (CORE,DB,ACTIVE_VISITOR)");
            } else {
                stm.execute("CREATE INDEX " + indexName + " ON " + tableName + " (CORE,DB,ACTIVE_VISITOR)");
            }
            stm.close();

            PreparedStatement psInsert = con
                    .prepareStatement("UPSERT INTO " + tableName + "(HOST, DOMAIN, FEATURE, \"DATE\", CORE, DB, ACTIVE_VISITOR) VALUES(?,?, ? , ?, ?, ?, ?)");
            psInsert.setString(1, "AA");
            psInsert.setString(2, "BB");
            psInsert.setString(3, "CC");
            psInsert.setDate(4, new Date(0));
            psInsert.setLong(5, 1L);
            psInsert.setLong(6, 2L);
            psInsert.setLong(7, 3);
            psInsert.execute();
            psInsert.close();
            if (!autoCommit) {
                con.commit();
            }
            
            con.createStatement().execute("DELETE FROM " + tableName );
            if (!autoCommit) {
                con.commit();
            }
            
            ResultSet rs = con.createStatement().executeQuery("SELECT /*+ NO_INDEX */ count(*) FROM " + tableName);
            assertTrue(rs.next());
            assertEquals(0, rs.getLong(1));
            if(localIndex){
                rs = con.createStatement().executeQuery("SELECT count(*) FROM " + localIndexName);
            } else {
                rs = con.createStatement().executeQuery("SELECT count(*) FROM " + indexName);
            }
            assertTrue(rs.next());
            assertEquals(0, rs.getLong(1));

        } finally {
            try {
                con.close();
            } catch (Exception ex) {
            }
        }
    }
    
    @Test
    public void testDeleteRowFromTableWithImmutableIndex() throws SQLException {
        testDeleteRowFromTableWithImmutableIndex(false);
    }
    
    @Test
    public void testDeleteRowFromTableWithImmutableLocalIndex() throws SQLException {
        testDeleteRowFromTableWithImmutableIndex(true);
    }
    
    public void testDeleteRowFromTableWithImmutableIndex(boolean localIndex) throws SQLException {
        Connection con = null;
        try {
            boolean autoCommit = false;
            con = DriverManager.getConnection(getUrl());
            con.setAutoCommit(autoCommit);

            Statement stm = con.createStatement();

            String tableName = generateUniqueName();
            String indexName1 = generateUniqueName();
            String indexName2 = generateUniqueName();

            stm.execute("CREATE TABLE IF NOT EXISTS " + tableName + " (" +
                    "HOST CHAR(2) NOT NULL," +
                    "DOMAIN VARCHAR NOT NULL, " +
                    "FEATURE VARCHAR NOT NULL, " +
                    "\"DATE\" DATE NOT NULL, \n" + 
                    "USAGE.CORE BIGINT," +
                    "USAGE.DB BIGINT," +
                    "STATS.ACTIVE_VISITOR INTEGER " +
                    "CONSTRAINT PK PRIMARY KEY (HOST, DOMAIN, FEATURE, \"DATE\")) IMMUTABLE_ROWS=true");
            stm.execute("CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexName1 + " ON " + tableName + " (\"DATE\", FEATURE)");
            stm.execute("CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexName2 + " ON " + tableName + " (\"DATE\", FEATURE, USAGE.DB)");
            stm.close();

            Date date = new Date(0);
            PreparedStatement psInsert = con
                    .prepareStatement("UPSERT INTO " + tableName + "(HOST, DOMAIN, FEATURE, \"DATE\", CORE, DB, ACTIVE_VISITOR) VALUES(?,?, ? , ?, ?, ?, ?)");
            psInsert.setString(1, "AA");
            psInsert.setString(2, "BB");
            psInsert.setString(3, "CC");
            psInsert.setDate(4, date);
            psInsert.setLong(5, 1L);
            psInsert.setLong(6, 2L);
            psInsert.setLong(7, 3);
            psInsert.execute();
            psInsert.close();
            if (!autoCommit) {
                con.commit();
            }
            
            psInsert = con.prepareStatement("DELETE FROM " + tableName + " WHERE (HOST, DOMAIN, FEATURE, \"DATE\") = (?,?,?,?)");
            psInsert.setString(1, "AA");
            psInsert.setString(2, "BB");
            psInsert.setString(3, "CC");
            psInsert.setDate(4, date);
            psInsert.execute();
            if (!autoCommit) {
                con.commit();
            }
            
            ResultSet rs = con.createStatement().executeQuery("SELECT /*+ NO_INDEX */ count(*) FROM " + tableName);
            assertTrue(rs.next());
            assertEquals(0, rs.getLong(1));

            rs = con.createStatement().executeQuery("SELECT count(*) FROM " + indexName1);
            assertTrue(rs.next());
            assertEquals(0, rs.getLong(1));

            stm.execute("DROP INDEX " + indexName1 + " ON " + tableName);
            stm.execute("DROP INDEX " + indexName2 + " ON " + tableName);

            stm.execute("CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexName1 + " ON " + tableName + " (USAGE.DB)");
            stm.execute("CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexName2 + " ON " + tableName + " (USAGE.DB, \"DATE\")");
            try{
                psInsert = con.prepareStatement("DELETE FROM " + tableName + " WHERE  USAGE.DB=2");
            } catch(Exception e) {
                fail("There should not be any exception while deleting row");
            }
        } finally {
            try {
                con.close();
            } catch (Exception ex) {
            }
        }
    }
    
    
    @Test
    public void testDeleteAllFromTableNoAutoCommit() throws SQLException {
        testDeleteAllFromTable(false);
    }

    @Test
    public void testDeleteAllFromTableAutoCommit() throws SQLException {
        testDeleteAllFromTable(true);
    }
    
    private void testDeleteAllFromTable(boolean autoCommit) throws SQLException {
        Connection con = null;
        try {
            con = DriverManager.getConnection(getUrl());
            con.setAutoCommit(autoCommit);

            String tableName = generateUniqueName();

            Statement stm = con.createStatement();
            stm.execute("CREATE TABLE IF NOT EXISTS " + tableName + "(" +
                    " HOST CHAR(2) NOT NULL," +
                    "DOMAIN VARCHAR NOT NULL, " +
                    "FEATURE VARCHAR NOT NULL, " +
                    "\"DATE\" DATE NOT NULL, \n" + 
                    "USAGE.CORE BIGINT," +
                    "USAGE.DB BIGINT," +
                    "STATS.ACTIVE_VISITOR INTEGER " +
                    "CONSTRAINT PK PRIMARY KEY (HOST, DOMAIN, FEATURE, \"DATE\"))");
            stm.close();

            PreparedStatement psInsert = con
                    .prepareStatement("UPSERT INTO " + tableName + "(HOST, DOMAIN, FEATURE, \"DATE\", CORE, DB, ACTIVE_VISITOR) VALUES(?,?, ? , ?, ?, ?, ?)");
            psInsert.setString(1, "AA");
            psInsert.setString(2, "BB");
            psInsert.setString(3, "CC");
            psInsert.setDate(4, new Date(0));
            psInsert.setLong(5, 1L);
            psInsert.setLong(6, 2L);
            psInsert.setLong(7, 3);
            psInsert.execute();
            psInsert.close();
            if (!autoCommit) {
                con.commit();
            }
            
            con.createStatement().execute("DELETE FROM " + tableName);
            if (!autoCommit) {
                con.commit();
            }
            
            ResultSet rs = con.createStatement().executeQuery("SELECT /*+ NO_INDEX */ count(*) FROM " + tableName);
            assertTrue(rs.next());
            assertEquals(0, rs.getLong(1));
        } finally {
            try {
                con.close();
            } catch (Exception ex) {
            }
        }
    }
    
    @Test
    public void testDeleteForTableWithRowTimestampColServer() throws Exception {
        String tableName = generateUniqueName();
        testDeleteForTableWithRowTimestampCol(true, tableName);
    }
    
    @Test
    public void testDeleteForTableWithRowTimestampColClient() throws Exception {
        String tableName = generateUniqueName();
        testDeleteForTableWithRowTimestampCol(false, tableName);
    }
    
    private void testDeleteForTableWithRowTimestampCol(boolean autoCommit, String tableName) throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(autoCommit);
            Statement stm = conn.createStatement();
            stm.execute("CREATE TABLE IF NOT EXISTS " + tableName +
                    " (HOST CHAR(2) NOT NULL," +
                    "STAT_DATE DATE NOT NULL, \n" + 
                    "USAGE.CORE BIGINT," +
                    "USAGE.DB BIGINT," +
                    "CONSTRAINT PK PRIMARY KEY (HOST, STAT_DATE ROW_TIMESTAMP))");
            stm.close();

            PreparedStatement psInsert = conn
                    .prepareStatement("UPSERT INTO " + tableName + " (HOST, STAT_DATE, CORE, DB) VALUES(?,?,?,?)");
            psInsert.setString(1, "AA");
            psInsert.setDate(2, new Date(100));
            psInsert.setLong(3, 1L);
            psInsert.setLong(4, 2L);
            psInsert.execute();
            psInsert.close();
            if (!autoCommit) {
                conn.commit();
            }
            conn.createStatement().execute("DELETE FROM " + tableName);
            if (!autoCommit) {
                conn.commit();
            }
            ResultSet rs = conn.createStatement().executeQuery("SELECT count(*) FROM " + tableName);
            assertTrue(rs.next());
            assertEquals(0, rs.getLong(1));
            
            // try with value for row_timestamp column generated by phoenix 
            psInsert = conn
                    .prepareStatement("UPSERT INTO " + tableName + " (HOST, CORE, DB) VALUES(?,?,?)");
            psInsert.setString(1, "BB");
            psInsert.setLong(2, 1L);
            psInsert.setLong(3, 2L);
            psInsert.execute();
            psInsert.close();
            if (!autoCommit) {
                conn.commit();
            }
            conn.createStatement().execute("DELETE FROM " + tableName);
            if (!autoCommit) {
                conn.commit();
            }
            rs = conn.createStatement().executeQuery("SELECT count(*) FROM " + tableName);
            assertTrue(rs.next());
            assertEquals(0, rs.getLong(1));
        }
    }
    
    @Test
    public void testServerSideDeleteAutoCommitOn() throws Exception {
        testDeleteCount(true, null);
    }
    
    @Test
    public void testClientSideDeleteCountAutoCommitOff() throws Exception {
        testDeleteCount(false, null);
    }
    
    @Test
    public void testClientSideDeleteAutoCommitOn() throws Exception {
        testDeleteCount(true, 1000);
    }
    
    private void testDeleteCount(boolean autoCommit, Integer limit) throws Exception {
        String tableName = generateUniqueName();

        String ddl = "CREATE TABLE IF NOT EXISTS " + tableName + " (pk1 DECIMAL NOT NULL, v1 VARCHAR CONSTRAINT PK PRIMARY KEY (pk1))";
        int numRecords = 1010;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            Statement stmt = conn.createStatement();
            for (int i = 0; i < numRecords ; i++) {
                stmt.executeUpdate("UPSERT INTO " + tableName + " (pk1, v1) VALUES (" + i + ",'value')");
            }
            conn.commit();
            conn.setAutoCommit(autoCommit);
            String delete = "DELETE FROM " + tableName + " WHERE (pk1) <= (" + numRecords + ")" + (limit == null ? "" : (" limit " + limit));
            try (PreparedStatement pstmt = conn.prepareStatement(delete)) {
                int numberOfDeletes = pstmt.executeUpdate();
                assertEquals(limit == null ? numRecords : limit, numberOfDeletes);
                if (!autoCommit) {
                    conn.commit();
                }
            }
        }

    }
}


