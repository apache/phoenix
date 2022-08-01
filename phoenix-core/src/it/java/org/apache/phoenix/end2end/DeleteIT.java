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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.PhoenixTagType;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.RawCell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.DeleteCompiler;
import org.apache.phoenix.compile.MutationPlan;
import org.apache.phoenix.end2end.index.IndexTestUtil;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.DeleteStatement;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@Category(ParallelStatsDisabledTest.class)
@RunWith(Parameterized.class)
public class DeleteIT extends ParallelStatsDisabledIT {
    private static final int NUMBER_OF_ROWS = 20;
    private static final int NTH_ROW_NULL = 5;

    private final String allowServerSideMutations;

    public DeleteIT(String allowServerSideMutations) {
        this.allowServerSideMutations = allowServerSideMutations;
    }

    @Parameters(name="DeleteIT_allowServerSideMutations={0}") // name is used by failsafe as file name in reports
    public static synchronized Object[] data() {
        return new Object[] {"true", "false"};
    }

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
        Properties props = new Properties();
        props.setProperty(QueryServices.ENABLE_SERVER_SIDE_DELETE_MUTATIONS,
            allowServerSideMutations);
        Connection conn = DriverManager.getConnection(getUrl(), props);
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
        Properties props = new Properties();
        props.setProperty(QueryServices.ENABLE_SERVER_SIDE_DELETE_MUTATIONS,
            allowServerSideMutations);
        Connection conn = DriverManager.getConnection(getUrl(), props);
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
    
    private static void assertIndexUsed (Connection conn, String query, String indexName, boolean expectedToBeUsed, boolean local) throws SQLException {
        assertIndexUsed(conn, query, Collections.emptyList(), indexName, expectedToBeUsed, local);
    }

    private static void assertIndexUsed (Connection conn, String query, List<Object> binds, String indexName, boolean expectedToBeUsed, boolean local) throws SQLException {
            PreparedStatement stmt = conn.prepareStatement("EXPLAIN " + query);
            for (int i = 0; i < binds.size(); i++) {
                stmt.setObject(i+1, binds.get(i));
            }
            ResultSet rs = stmt.executeQuery();
            String explainPlan = QueryUtil.getExplainPlan(rs);
            // It's very difficult currently to check if a local index is being used
            // This check is brittle as it checks that the index ID appears in the range scan
            // TODO: surface QueryPlan from MutationPlan
            if (local) {
                assertEquals(expectedToBeUsed, explainPlan.contains(indexName + " [1]") || explainPlan.contains(indexName + " [1,"));
            } else {
                assertEquals(expectedToBeUsed, explainPlan.contains(" SCAN OVER " + indexName));
            }
   }

    private void testDeleteRange(boolean autoCommit, boolean createIndex) throws Exception {
        testDeleteRange(autoCommit, createIndex, false);
    }

    private void testDeleteRange(boolean autoCommit, boolean createIndex, boolean local) throws Exception {
        Properties props = new Properties();
        props.setProperty(QueryServices.ENABLE_SERVER_SIDE_DELETE_MUTATIONS,
            allowServerSideMutations);
        Connection conn = DriverManager.getConnection(getUrl(), props);
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
        assertIndexUsed(conn, deleteStmt, Arrays.<Object>asList(5,10), indexInUse, false, local);
        stmt = conn.prepareStatement(deleteStmt);
        stmt.setInt(1, 5);
        stmt.setInt(2, 10);
        stmt.execute();
        if (!autoCommit) {
            conn.commit();
        }
        
        String query = "SELECT count(*) FROM " + tableName;
        assertIndexUsed(conn, query, indexInUse, createIndex, local);
        query = "SELECT count(*) FROM " + tableName;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals(NUMBER_OF_ROWS - (10-5), rs.getInt(1));
        
        deleteStmt = "DELETE FROM " + tableName + " WHERE j IS NULL";
        stmt = conn.prepareStatement(deleteStmt);
        assertIndexUsed(conn, deleteStmt, indexInUse, createIndex, local);
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
    public void testDeleteAllFromTableWithIndexAutoCommitSalting() throws Exception {
        testDeleteAllFromTableWithIndex(true, true, false);
    }

    @Test
    public void testDeleteAllFromTableWithLocalIndexAutoCommitSalting() throws Exception {
        testDeleteAllFromTableWithIndex(true, true, true);
    }
    
    @Test
    public void testDeleteAllFromTableWithIndexAutoCommitNoSalting() throws Exception {
        testDeleteAllFromTableWithIndex(true, false);
    }
    
    @Test
    public void testDeleteAllFromTableWithIndexNoAutoCommitNoSalting() throws Exception {
        testDeleteAllFromTableWithIndex(false,false);
    }
    
    @Test
    public void testDeleteAllFromTableWithIndexNoAutoCommitSalted() throws Exception {
        testDeleteAllFromTableWithIndex(false, true, false);
    }
    
    @Test
    public void testDeleteAllFromTableWithLocalIndexNoAutoCommitSalted() throws Exception {
        testDeleteAllFromTableWithIndex(false, true, true);
    }

    private void testDeleteAllFromTableWithIndex(boolean autoCommit, boolean isSalted) throws Exception {
        testDeleteAllFromTableWithIndex(autoCommit, isSalted, false);
    }

    private void testDeleteAllFromTableWithIndex(boolean autoCommit, boolean isSalted, boolean localIndex) throws Exception {
        Connection con = null;
        try {
            Properties props = new Properties();
            props.setProperty(QueryServices.ENABLE_SERVER_SIDE_DELETE_MUTATIONS,
                allowServerSideMutations);
            con = DriverManager.getConnection(getUrl(), props);
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
    public void testDeleteRowFromTableWithImmutableIndex() throws Exception {
        testDeleteRowFromTableWithImmutableIndex(false, true);
    }
    
    @Test
    public void testDeleteRowFromTableWithImmutableLocalIndex() throws Exception {
        testDeleteRowFromTableWithImmutableIndex(true, false);
    }
    
    @Test
    public void testPointDeleteRowFromTableWithImmutableIndex() throws Exception {
        testPointDeleteRowFromTableWithImmutableIndex(false, false);
    }
    
    @Test
    public void testPointDeleteRowFromTableWithLocalImmutableIndex() throws Exception {
        testPointDeleteRowFromTableWithImmutableIndex(true, false);
    }
    
    @Test
    public void testPointDeleteRowFromTableWithImmutableIndex2() throws Exception {
        testPointDeleteRowFromTableWithImmutableIndex(false, true);
    }
    
    public void testPointDeleteRowFromTableWithImmutableIndex(boolean localIndex, boolean addNonPKIndex) throws Exception {
        Connection con = null;
        try {
            boolean autoCommit = false;
            Properties props = new Properties();
            props.setProperty(QueryServices.ENABLE_SERVER_SIDE_DELETE_MUTATIONS,
                allowServerSideMutations);
            con = DriverManager.getConnection(getUrl(), props);
            con.setAutoCommit(autoCommit);

            Statement stm = con.createStatement();

            String tableName = generateUniqueName();
            String indexName1 = generateUniqueName();
            String indexName2 = generateUniqueName();
            String indexName3 = addNonPKIndex? generateUniqueName() : null;

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
            stm.execute("CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexName2 + " ON " + tableName + " (FEATURE, DOMAIN)");
            if (addNonPKIndex) {
                stm.execute("CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexName3 + " ON " + tableName + " (\"DATE\", FEATURE, USAGE.DB)");
            }
            
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
            if (!autoCommit) {
                con.commit();
            }
            
            String dml = "DELETE FROM " + tableName + " WHERE (HOST, DOMAIN, FEATURE, \"DATE\") = (?,?,?,?)";
            PreparedStatement psDelete = con.prepareStatement(dml);
            psDelete.setString(1, "AA");
            psDelete.setString(2, "BB");
            psDelete.setString(3, "CC");
            psDelete.setDate(4, date);
            psDelete.execute();
            if (!autoCommit) {
                con.commit();
            }
            psDelete = con.prepareStatement("EXPLAIN " + dml);
            psDelete.setString(1, "AA");
            psDelete.setString(2, "BB");
            psDelete.setString(3, "CC");
            psDelete.setDate(4, date);
            String explainPlan = QueryUtil.getExplainPlan(psDelete.executeQuery());
            if (addNonPKIndex) {
                assertNotEquals("DELETE SINGLE ROW", explainPlan);
            } else {
                assertEquals("DELETE SINGLE ROW", explainPlan);
            }
            
            assertDeleted(con, tableName, indexName1, indexName2, indexName3);
        } finally {
            try {
                con.close();
            } catch (Exception ex) {
            }
        }
    }
        
    public void testDeleteRowFromTableWithImmutableIndex(boolean localIndex, boolean useCoveredIndex) throws Exception {
        Connection con = null;
        try {
            boolean autoCommit = false;
            Properties props = new Properties();
            props.setProperty(QueryServices.ENABLE_SERVER_SIDE_DELETE_MUTATIONS,
                allowServerSideMutations);
            con = DriverManager.getConnection(getUrl(), props);
            con.setAutoCommit(autoCommit);

            Statement stm = con.createStatement();

            String tableName = generateUniqueName();
            String indexName1 = generateUniqueName();
            String indexName2 = generateUniqueName();
            String indexName3 = useCoveredIndex? generateUniqueName() : null;

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
            if (useCoveredIndex) {
                stm.execute("CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexName3 + " ON " + tableName + " (STATS.ACTIVE_VISITOR) INCLUDE (USAGE.CORE, USAGE.DB)");
            }
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
            if (!autoCommit) {
                con.commit();
            }
            
            PreparedStatement psDelete = con.prepareStatement("DELETE FROM " + tableName + " WHERE (HOST, DOMAIN, FEATURE, \"DATE\") = (?,?,?,?)");
            psDelete.setString(1, "AA");
            psDelete.setString(2, "BB");
            psDelete.setString(3, "CC");
            psDelete.setDate(4, date);
            psDelete.execute();
            if (!autoCommit) {
                con.commit();
            }
            
            assertDeleted(con, tableName, indexName1, indexName2, indexName3);

            psInsert.execute();
            if (!autoCommit) {
                con.commit();
            }

            psDelete = con.prepareStatement("DELETE FROM " + tableName + " WHERE  USAGE.DB=2");
            psDelete.execute();
            if (!autoCommit) {
                con.commit();
            }

            assertDeleted(con, tableName, indexName1, indexName2, indexName3);

            psInsert.execute();
            if (!autoCommit) {
                con.commit();
            }

            psDelete = con.prepareStatement("DELETE FROM " + tableName + " WHERE  ACTIVE_VISITOR=3");
            psDelete.execute();
            if (!autoCommit) {
                con.commit();
            }

            assertDeleted(con, tableName, indexName1, indexName2, indexName3);

        } finally {
            try {
                con.close();
            } catch (Exception ex) {
            }
        }
    }

    private static void assertDeleted(Connection con, String tableName, String indexName1, String indexName2, String indexName3)
            throws SQLException {
        ResultSet rs;
        rs = con.createStatement().executeQuery("SELECT /*+ NO_INDEX */ count(*) FROM " + tableName);
        assertTrue(rs.next());
        assertEquals(0, rs.getLong(1));

        rs = con.createStatement().executeQuery("SELECT count(*) FROM " + indexName1);
        assertTrue(rs.next());
        assertEquals(0, rs.getLong(1));

        rs = con.createStatement().executeQuery("SELECT count(*) FROM " + indexName2);
        assertTrue(rs.next());
        assertEquals(0, rs.getLong(1));

        if (indexName3 != null) {
            rs = con.createStatement().executeQuery("SELECT count(*) FROM " + indexName3);
            assertTrue(rs.next());
            assertEquals(0, rs.getLong(1));
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
            Properties props = new Properties();
            props.setProperty(QueryServices.ENABLE_SERVER_SIDE_DELETE_MUTATIONS,
                allowServerSideMutations);
            con = DriverManager.getConnection(getUrl(), props);
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
        Properties props = new Properties();
        props.setProperty(QueryServices.ENABLE_SERVER_SIDE_DELETE_MUTATIONS,
            allowServerSideMutations);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
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

    @Test
    public void testPointDeleteWithMultipleImmutableIndexes() throws Exception {
        testPointDeleteWithMultipleImmutableIndexes(false);
    }

    @Test
    public void testPointDeleteWithMultipleImmutableIndexesAfterAlter() throws Exception {
        testPointDeleteWithMultipleImmutableIndexes(true);
    }

    private void testPointDeleteWithMultipleImmutableIndexes(boolean alterTable) throws Exception {
        String tableName = generateUniqueName();
        String commands = "CREATE TABLE IF NOT EXISTS " + tableName
                + " (ID INTEGER PRIMARY KEY,double_id DOUBLE,varchar_id VARCHAR (30)) "
                + (alterTable ? ";ALTER TABLE " + tableName + " set " : "") + "IMMUTABLE_ROWS=true;"
                + "CREATE INDEX IF NOT EXISTS index_column_varchar_id ON " + tableName + "(varchar_id);"
                + "CREATE INDEX IF NOT EXISTS index_column_double_id ON " + tableName + "(double_id);" + "UPSERT INTO "
                + tableName + " VALUES (9000000,0.5,'Sample text extra');" ;
        Properties props = new Properties();
        props.setProperty(QueryServices.ENABLE_SERVER_SIDE_DELETE_MUTATIONS, allowServerSideMutations);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            Statement stm = conn.createStatement();
            for (String sql : commands.split(";")) {
                stm.execute(sql);
            }
            ResultSet rs = stm.executeQuery("select id,varchar_id,double_id from " + tableName + " WHERE ID=9000000");
            assertTrue(rs.next());
            assertEquals(9000000, rs.getInt(1));
            assertEquals("Sample text extra", rs.getString(2));
            assertEquals(0.5, rs.getDouble(3),0.01);
            stm.execute("DELETE FROM " + tableName + " WHERE ID=9000000");
            assertDeleted(conn, tableName, "index_column_varchar_id", "index_column_double_id", null);
            stm.close();
        }
    }
    
    private void testDeleteCount(boolean autoCommit, Integer limit) throws Exception {
        String tableName = generateUniqueName();

        String ddl = "CREATE TABLE IF NOT EXISTS " + tableName + " (pk1 DECIMAL NOT NULL, v1 VARCHAR CONSTRAINT PK PRIMARY KEY (pk1))";
        int numRecords = 1010;
        Properties props = new Properties();
        props.setProperty(QueryServices.ENABLE_SERVER_SIDE_DELETE_MUTATIONS,
            allowServerSideMutations);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
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
    

    @Test
    public void testClientSideDeleteShouldNotFailWhenSameColumnPresentInMultipleIndexes()
            throws Exception {
        String tableName = generateUniqueName();
        String indexName1 = generateUniqueName();
        String indexName2 = generateUniqueName();
        String ddl =
                "CREATE TABLE IF NOT EXISTS "
                        + tableName
                        + " (pk1 DECIMAL NOT NULL, v1 VARCHAR, v2 VARCHAR CONSTRAINT PK PRIMARY KEY (pk1))";
        String idx1 = "CREATE INDEX " + indexName1 + " ON " + tableName + "(v1)";
        String idx2 = "CREATE INDEX " + indexName2 + " ON " + tableName + "(v1, v2)";
        Properties props = new Properties();
        props.setProperty(QueryServices.ENABLE_SERVER_SIDE_DELETE_MUTATIONS,
            allowServerSideMutations);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute(ddl);
            conn.createStatement().execute(idx1);
            conn.createStatement().execute(idx2);
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("UPSERT INTO " + tableName + " VALUES (1,'value', 'value2')");
            conn.commit();
            conn.setAutoCommit(false);
            try {
                conn.createStatement().execute("DELETE FROM " + tableName + " WHERE pk1 > 0");
            } catch (Exception e) {
                fail("Should not throw any exception");
            }
        }
    }

    @Test
    public void testDeleteShouldNotFailWhenTheRowsMoreThanMaxMutationSize() throws Exception {
        String tableName = generateUniqueName();
        String indexName1 = generateUniqueName();
        String ddl =
                "CREATE TABLE IF NOT EXISTS "
                        + tableName
                        + " (pk1 DECIMAL NOT NULL, v1 VARCHAR, v2 VARCHAR CONSTRAINT PK PRIMARY KEY (pk1))"
                        + " IMMUTABLE_ROWS=true";
        String idx1 = "CREATE INDEX " + indexName1 + " ON " + tableName + "(v1)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.MAX_MUTATION_SIZE_ATTRIB,Integer.toString(10));
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute(ddl);
            conn.createStatement().execute(idx1);
            Statement stmt = conn.createStatement();
            for(int i = 0; i < 20; i++) {
                stmt.executeUpdate("UPSERT INTO " + tableName + " VALUES ("+i+",'value"+i+"', 'value2')");
                if (i % 10 == 0) {
                    conn.commit();
                }
            }
            conn.commit();
            conn.setAutoCommit(true);
            try {
                conn.createStatement().execute("DELETE FROM " + tableName);
            } catch (Exception e) {
                fail("Should not throw any exception");
            }
        }
    }

    @Test
    public void testDeleteFilterWithMultipleIndexes() throws Exception {
        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + tableName +
                " (PKEY1 CHAR(15) NOT NULL, PKEY2 CHAR(15) NOT NULL, VAL1 CHAR(15), VAL2 CHAR(15)," +
                " CONSTRAINT PK PRIMARY KEY (PKEY1, PKEY2)) ";
        String indexName1 = generateUniqueName();
        String indexDdl1 = "CREATE INDEX IF NOT EXISTS " + indexName1 + " ON " + tableName + " (VAL1)";
        String indexName2 = generateUniqueName();
        String indexDdl2 = "CREATE INDEX IF NOT EXISTS " + indexName2 + " ON " + tableName + " (VAL2)";
        String delete = "DELETE FROM " + tableName + " WHERE VAL1 = '000000000000000' limit 1";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try(Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            try (Statement statement = conn.createStatement()) {
                statement.execute(ddl);
            }
            conn.createStatement().execute("upsert into " + tableName +" values ('PKEY1', 'PKEY2', '000000000000000', 'VAL2')");
            conn.commit();
            try (Statement statement = conn.createStatement()) {
                statement.execute(indexDdl1);
            }
        }
        try(Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            try (Statement statement = conn.createStatement()) {
                ResultSet rs = statement.executeQuery("EXPLAIN " + delete);
                String explainPlan = QueryUtil.getExplainPlan(rs);
                // Verify index is used for the delete query
                IndexToolIT.assertExplainPlan(false, explainPlan, tableName, indexName1);
            }
            // Created the second index
            try (Statement statement = conn.createStatement()) {
                statement.execute(indexDdl2);
            }
            try (Statement statement = conn.createStatement()) {
                ResultSet rs = statement.executeQuery("EXPLAIN " + delete);
                String explainPlan = QueryUtil.getExplainPlan(rs);
                // Verify index is used for the delete query
                IndexToolIT.assertExplainPlan(false, explainPlan, tableName, indexName1);
                statement.executeUpdate(delete);
                // Count the number of rows
                String query = "SELECT COUNT(*) from " + tableName;
                // There should be no rows on the data table
                rs = conn.createStatement().executeQuery(query);
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
                query = "SELECT COUNT(*) from " + indexName1;
                // There should be no rows on any of the index tables
                rs = conn.createStatement().executeQuery(query);
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
                query = "SELECT COUNT(*) from " + indexName2;
                // There should be no rows on any of the index tables
                rs = conn.createStatement().executeQuery(query);
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    /*
        Tests whether we have cell tags in delete marker for
        ClientSelectDeleteMutationPlan.
     */
    @Test
    public void testDeleteClientDeleteMutationPlan() throws Exception {
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        String tagValue = "customer-delete";
        String delete = "DELETE FROM " + tableName + " WHERE v1 = 'foo'";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        // Add tag "customer-delete" to delete marker.
        props.setProperty(ConnectionQueryServices.SOURCE_OPERATION_ATTRIB, tagValue);

        createAndUpsertTable(tableName, indexName, props, false);
        // Make sure that the plan creates is of ClientSelectDeleteMutationPlan
        verifyDeletePlan(delete, DeleteCompiler.ClientSelectDeleteMutationPlan.class, props);
        executeDelete(delete, props, 1);
        String startRowKeyForBaseTable = "1";
        String startRowKeyForIndexTable = "foo";
        // Make sure that Delete Marker has cell tag for base table
        // and has no cell tag for index table.
        checkTagPresentInDeleteMarker(tableName, startRowKeyForBaseTable, true, tagValue);
        checkTagPresentInDeleteMarker(indexName, startRowKeyForIndexTable, false, null);
    }

    /*
        Tests whether we have cell tags in delete marker for
        ServerSelectDeleteMutationPlan.
     */
    @Test
    public void testDeleteServerDeleteMutationPlan() throws Exception {
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        String tagValue = "customer-delete";
        String delete = "DELETE FROM " + tableName;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(ConnectionQueryServices.SOURCE_OPERATION_ATTRIB, tagValue);

        createAndUpsertTable(tableName, indexName, props, false);
        // Make sure that the plan creates is of ServerSelectDeleteMutationPlan
        verifyDeletePlan(delete, DeleteCompiler.ServerSelectDeleteMutationPlan.class, props);
        executeDelete(delete, props, 2);

        String startRowKeyForBaseTable = "1";
        String startRowKeyForIndexTable = "foo";
        // Make sure that Delete Marker has cell tag for base table
        // and has no cell tag for index table.
        checkTagPresentInDeleteMarker(tableName, startRowKeyForBaseTable, true, tagValue);
        checkTagPresentInDeleteMarker(indexName, startRowKeyForIndexTable, false, null);
    }

    /*
        Tests whether we have cell tags in delete marker for
        MultiRowDeleteMutationPlan.
    */
    @Test
    public void testDeleteMultiRowDeleteMutationPlan() throws Exception {
        String tableName = generateUniqueName();
        String tagValue = "customer-delete";
        String delete = "DELETE FROM " + tableName + " WHERE k = 1";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(ConnectionQueryServices.SOURCE_OPERATION_ATTRIB, tagValue);
        // Don't create index table. We will use MultiRowDeleteMutationPlan
        // if there is no index present for a table.
        createAndUpsertTable(tableName, null, props, false);
        // Make sure that the plan creates is of MultiRowDeleteMutationPlan
        verifyDeletePlan(delete, DeleteCompiler.MultiRowDeleteMutationPlan.class, props);
        executeDelete(delete, props, 1);
        String startRowKeyForBaseTable = "1";
        // Make sure that Delete Marker has cell tag for base table.
        // We haven't created index table for this test case.
        checkTagPresentInDeleteMarker(tableName, startRowKeyForBaseTable, true, tagValue);
    }

    /*
        Verify whether plan that we create for delete statement is of planClass
     */
    private void verifyDeletePlan(String delete, Class<? extends MutationPlan> planClass,
            Properties props) throws SQLException {
        try(Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
            SQLParser parser = new SQLParser(delete);
            DeleteStatement deleteStmt = (DeleteStatement) parser.parseStatement();
            DeleteCompiler compiler = new DeleteCompiler(stmt, null);
            MutationPlan plan = compiler.compile(deleteStmt);
            assertEquals(plan.getClass(), planClass);
        }
    }
    private void createAndUpsertTable(String tableName, String indexName, Properties props,
            boolean useOldCoproc) throws Exception {
        String ddl = "CREATE TABLE " + tableName +
                " (k INTEGER NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)";
        try(Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            try (Statement statement = conn.createStatement()) {
                statement.execute(ddl);
                if (indexName != null) {
                    String indexDdl1 = "CREATE INDEX " + indexName + " ON " + tableName + "(v1,v2)";
                    statement.execute(indexDdl1);
                }
                if (useOldCoproc) {
                    Admin admin = ((PhoenixConnection) conn).getQueryServices().getAdmin();
                    IndexTestUtil.downgradeCoprocs(tableName, indexName, admin);
                }

                statement.execute(
                        "upsert into " + tableName + " values (1, 'foo', 'foo1')");
                statement.execute(
                        "upsert into " + tableName + " values (2, 'bar', 'bar1')");
                conn.commit();
            }

        }
    }

    private void executeDelete(String delete, Properties props, int deleteRowCount)
            throws SQLException {
        try(Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            try (Statement statement = conn.createStatement()) {
                int rs = statement.executeUpdate(delete);
                assertEquals( deleteRowCount, rs);
            }
        }
    }

    /*
        Verify whether we have tags present for base table and not present for
        index tables.
     */
    private void checkTagPresentInDeleteMarker(String tableName, String startRowKey,
            boolean tagPresent, String tagValue) throws IOException {
        List<Cell> values = new ArrayList<>();
        TableName table = TableName.valueOf(tableName);
        // Scan table with specified startRowKey
        for (HRegion region : getUtility().getHBaseCluster().getRegions(table)) {
            Scan scan = new Scan();
            // Make sure to set rawScan to true so that we will get Delete Markers.
            scan.setRaw(true);
            scan.withStartRow(Bytes.toBytes(startRowKey));
            RegionScanner scanner = region.getScanner(scan);
            scanner.next(values);
            if (!values.isEmpty()) {
                break;
            }
        }
        assertFalse("Values shouldn't be empty", values.isEmpty());
        Cell first = values.get(0);
        assertTrue("First cell should be delete marker ", CellUtil.isDelete(first));
        List<Tag> tags = PrivateCellUtil.getTags(first);
        if (tagPresent) {
            assertEquals(1, tags.size());
            RawCell rawCell = (RawCell)first;
            Optional<Tag> optional = rawCell.getTag(PhoenixTagType.SOURCE_OPERATION_TAG_TYPE);
            assertTrue(optional.isPresent());
            Tag sourceOfOperationTag = optional.get();
            assertEquals(tagValue, Tag.getValueAsString(sourceOfOperationTag));
        } else {
            assertEquals(0, tags.size());
        }
    }

    /*
        Test whether source of operation tags are added to Delete mutations if we are using
        old index coproc.
     */
    @Test
    public void testDeleteTagsWithOldIndexCoproc() throws Exception {
        String tableName = generateUniqueName();
        String tagValue = "customer-delete";
        String delete = "DELETE FROM " + tableName + " WHERE k = 1";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(ConnectionQueryServices.SOURCE_OPERATION_ATTRIB, tagValue);
        // The new table will always have new index coproc. Downgrade it to use older one.
        createAndUpsertTable(tableName, null, props, true);
        executeDelete(delete, props, 1);
        String startRowKeyForBaseTable = "1";
        // Make sure that Delete Marker has cell tag for base table.
        checkTagPresentInDeleteMarker(tableName, startRowKeyForBaseTable, true, tagValue);
    }
}