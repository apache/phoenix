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
package org.apache.phoenix.end2end.index;

import static org.apache.phoenix.util.TestUtil.INDEX_DATA_SCHEMA;
import static org.apache.phoenix.util.TestUtil.INDEX_DATA_TABLE;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Test;


public class ImmutableIndexIT extends BaseHBaseManagedTimeIT {
    // Populate the test table with data.
    private static void populateTestTable() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            String upsert = "UPSERT INTO " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE
                    + " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement stmt = conn.prepareStatement(upsert);
            stmt.setString(1, "varchar1");
            stmt.setString(2, "char1");
            stmt.setInt(3, 1);
            stmt.setLong(4, 1L);
            stmt.setBigDecimal(5, new BigDecimal(1.0));
            Date date = DateUtil.parseDate("2015-01-01 00:00:00");
            stmt.setDate(6, date);
            stmt.setString(7, "varchar_a");
            stmt.setString(8, "chara");
            stmt.setInt(9, 2);
            stmt.setLong(10, 2L);
            stmt.setBigDecimal(11, new BigDecimal(2.0));
            stmt.setDate(12, date);
            stmt.setString(13, "varchar_b");
            stmt.setString(14, "charb");
            stmt.setInt(15, 3);
            stmt.setLong(16, 3L);
            stmt.setBigDecimal(17, new BigDecimal(3.0));
            stmt.setDate(18, date);
            stmt.executeUpdate();
            
            stmt.setString(1, "varchar2");
            stmt.setString(2, "char2");
            stmt.setInt(3, 2);
            stmt.setLong(4, 2L);
            stmt.setBigDecimal(5, new BigDecimal(2.0));
            date = DateUtil.parseDate("2015-01-02 00:00:00");
            stmt.setDate(6, date);
            stmt.setString(7, "varchar_a");
            stmt.setString(8, "chara");
            stmt.setInt(9, 3);
            stmt.setLong(10, 3L);
            stmt.setBigDecimal(11, new BigDecimal(3.0));
            stmt.setDate(12, date);
            stmt.setString(13, "varchar_b");
            stmt.setString(14, "charb");
            stmt.setInt(15, 4);
            stmt.setLong(16, 4L);
            stmt.setBigDecimal(17, new BigDecimal(4.0));
            stmt.setDate(18, date);
            stmt.executeUpdate();
            
            stmt.setString(1, "varchar3");
            stmt.setString(2, "char3");
            stmt.setInt(3, 3);
            stmt.setLong(4, 3L);
            stmt.setBigDecimal(5, new BigDecimal(3.0));
            date = DateUtil.parseDate("2015-01-03 00:00:00");
            stmt.setDate(6, date);
            stmt.setString(7, "varchar_a");
            stmt.setString(8, "chara");
            stmt.setInt(9, 4);
            stmt.setLong(10, 4L);
            stmt.setBigDecimal(11, new BigDecimal(4.0));
            stmt.setDate(12, date);
            stmt.setString(13, "varchar_b");
            stmt.setString(14, "charb");
            stmt.setInt(15, 5);
            stmt.setLong(16, 5L);
            stmt.setBigDecimal(17, new BigDecimal(5.0));
            stmt.setDate(18, date);
            stmt.executeUpdate();
            
            conn.commit();
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testIndexWithNullableFixedWithCols() throws Exception {
        testIndexWithNullableFixedWithCols(false);
    }

    @Test
    public void testLocalIndexWithNullableFixedWithCols() throws Exception {
        testIndexWithNullableFixedWithCols(true);
    }

    public void testIndexWithNullableFixedWithCols(boolean localIndex) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        ensureTableCreated(getUrl(), INDEX_DATA_TABLE);
        populateTestTable();
        String ddl = "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX IDX ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE
                    + " (char_col1 ASC, int_col1 ASC)"
                    + " INCLUDE (long_col1, long_col2)";
        PreparedStatement stmt = conn.prepareStatement(ddl);
        stmt.execute();
        
        String query = "SELECT char_col1, int_col1 from " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE;
        ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        if(localIndex) {
            assertEquals(
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER _LOCAL_IDX_INDEX_TEST.INDEX_DATA_TABLE [-32768]\n" + 
                "    SERVER FILTER BY FIRST KEY ONLY\n" +
                "CLIENT MERGE SORT",
                QueryUtil.getExplainPlan(rs));
        } else {
            assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER INDEX_TEST.IDX\n"
                    + "    SERVER FILTER BY FIRST KEY ONLY", QueryUtil.getExplainPlan(rs));
        }
        
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("chara", rs.getString(1));
        assertEquals(2, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals("chara", rs.getString(1));
        assertEquals(3, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals("chara", rs.getString(1));
        assertEquals(4, rs.getInt(2));
        assertFalse(rs.next());
        
        conn.createStatement().execute("DROP INDEX IDX ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE);
        
        query = "SELECT char_col1, int_col1 from " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        
        query = "SELECT char_col1, int_col1 from IDX ";
        try{
            rs = conn.createStatement().executeQuery(query);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TABLE_UNDEFINED.getErrorCode(), e.getErrorCode());
        }
        
        
    }
    
    private void assertImmutableRows(Connection conn, String fullTableName, boolean expectedValue) throws SQLException {
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        assertEquals(expectedValue, pconn.getMetaDataCache().getTable(new PTableKey(pconn.getTenantId(), fullTableName)).isImmutableRows());
    }
    
    @Test
    public void testAlterTableWithImmutability() throws Exception {

        String query;
        ResultSet rs;
        String fullTableName = "T";

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        conn.createStatement().execute(
            "CREATE TABLE t (k VARCHAR NOT NULL PRIMARY KEY, v VARCHAR)  ");
        
        query = "SELECT * FROM t";
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());

        assertImmutableRows(conn,fullTableName, false);
        conn.createStatement().execute("ALTER TABLE t SET IMMUTABLE_ROWS=true");
        assertImmutableRows(conn,fullTableName, true);
        
        
        conn.createStatement().execute("ALTER TABLE t SET immutable_rows=false");
        assertImmutableRows(conn,fullTableName, false);
    }
    
    @Test
    public void testDeleteFromAllPKColumnIndex() throws Exception {
        testDeleteFromAllPKColumnIndex(false);
    }

    @Test
    public void testDeleteFromAllPKColumnLocalIndex() throws Exception {
        testDeleteFromAllPKColumnIndex(true);
    }

    public void testDeleteFromAllPKColumnIndex(boolean localIndex) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        ensureTableCreated(getUrl(), INDEX_DATA_TABLE);
        populateTestTable();
        String ddl = "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX IDX ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE
                    + " (long_pk, varchar_pk)"
                    + " INCLUDE (long_col1, long_col2)";
        PreparedStatement stmt = conn.prepareStatement(ddl);
        stmt.execute();
        
        ResultSet rs;
        
        rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " +INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE);
        assertTrue(rs.next());
        assertEquals(3,rs.getInt(1));
        rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " +INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + "IDX");
        assertTrue(rs.next());
        assertEquals(3,rs.getInt(1));
        
        String dml = "DELETE from " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE +
                " WHERE long_col2 = 4";
        assertEquals(1,conn.createStatement().executeUpdate(dml));
        conn.commit();
        
        String query = "SELECT /*+ NO_INDEX */ long_pk FROM " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals(1L, rs.getLong(1));
        assertTrue(rs.next());
        assertEquals(3L, rs.getLong(1));
        assertFalse(rs.next());
        
        query = "SELECT long_pk FROM " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals(1L, rs.getLong(1));
        assertTrue(rs.next());
        assertEquals(3L, rs.getLong(1));
        assertFalse(rs.next());
        
        query = "SELECT * FROM " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + "IDX" ;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals(1L, rs.getLong(1));
        assertTrue(rs.next());
        assertEquals(3L, rs.getLong(1));
        assertFalse(rs.next());
        
        conn.createStatement().execute("DROP INDEX IDX ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE);
    }
    
    @Test
    public void testDropIfImmutableKeyValueColumn() throws Exception {
        testDropIfImmutableKeyValueColumn(false);
    }

    @Test
    public void testDropIfImmutableKeyValueColumnWithLocalIndex() throws Exception {
        testDropIfImmutableKeyValueColumn(true);
    }

    public void testDropIfImmutableKeyValueColumn(boolean localIndex) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        ensureTableCreated(getUrl(), INDEX_DATA_TABLE);
        populateTestTable();
        String ddl = "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX IDX ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE
                    + " (long_col1)";
        PreparedStatement stmt = conn.prepareStatement(ddl);
        stmt.execute();
        
        ResultSet rs;
        
        rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " +INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE);
        assertTrue(rs.next());
        assertEquals(3,rs.getInt(1));
        rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " +INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + "IDX");
        assertTrue(rs.next());
        assertEquals(3,rs.getInt(1));
        
        conn.setAutoCommit(true);
        String dml = "DELETE from " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE +
                " WHERE long_col2 = 4";
        try {
            conn.createStatement().execute(dml);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.INVALID_FILTER_ON_IMMUTABLE_ROWS.getErrorCode(), e.getErrorCode());
        }
            
        conn.createStatement().execute("DROP TABLE " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE);
    }
    
    @Test
    public void testGroupByCount() throws Exception {
        testGroupByCount(false);
    }

    @Test
    public void testGroupByCountWithLocalIndex() throws Exception {
        testGroupByCount(true);
    }

    public void testGroupByCount(boolean localIndex) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        ensureTableCreated(getUrl(), INDEX_DATA_TABLE);
        populateTestTable();
        String ddl = "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX IDX ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE
                + " (int_col2)";
        PreparedStatement stmt = conn.prepareStatement(ddl);
        stmt.execute();
        
        ResultSet rs;
        
        rs = conn.createStatement().executeQuery("SELECT int_col2, COUNT(*) FROM " +INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE + " GROUP BY int_col2");
        assertTrue(rs.next());
        assertEquals(1,rs.getInt(2));
    }
    
    @Test   
    public void testSelectDistinctOnTableWithSecondaryImmutableIndex() throws Exception {
        testSelectDistinctOnTableWithSecondaryImmutableIndex(false);
    }

    @Test   
    public void testSelectDistinctOnTableWithSecondaryImmutableLocalIndex() throws Exception {
        testSelectDistinctOnTableWithSecondaryImmutableIndex(true);
    }

    public void testSelectDistinctOnTableWithSecondaryImmutableIndex(boolean localIndex) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        ensureTableCreated(getUrl(), INDEX_DATA_TABLE);
        populateTestTable();
        String ddl = "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX IDX ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE
                + " (int_col2)";
        Connection conn = null;
        PreparedStatement stmt = null;

        try {
            try {
                conn = DriverManager.getConnection(getUrl(), props);
                conn.setAutoCommit(false);
                stmt = conn.prepareStatement(ddl);
                stmt.execute();
                ResultSet rs = conn.createStatement().executeQuery("SELECT distinct int_col2 FROM " +INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE + " where int_col2 > 0");
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
                assertTrue(rs.next());
                assertEquals(4, rs.getInt(1));
                assertTrue(rs.next());
                assertEquals(5, rs.getInt(1));
                assertFalse(rs.next());
            } finally {
                if (stmt != null) {
                    stmt.close();
                }
            } 
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    @Test
    public void testInClauseWithIndexOnColumnOfUsignedIntType() throws Exception {
        testInClauseWithIndexOnColumnOfUsignedIntType(false);
    }

    @Test
    public void testInClauseWithLocalIndexOnColumnOfUsignedIntType() throws Exception {
        testInClauseWithIndexOnColumnOfUsignedIntType(true);
    }

    public void testInClauseWithIndexOnColumnOfUsignedIntType(boolean localIndex) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = null;
        PreparedStatement stmt = null;
        ensureTableCreated(getUrl(), INDEX_DATA_TABLE);
        populateTestTable();
        String ddl = "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX IDX ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE
                + " (int_col1)";
        try {
            try {
                conn = DriverManager.getConnection(getUrl(), props);
                conn.setAutoCommit(false);
                stmt = conn.prepareStatement(ddl);
                stmt.execute();
                ResultSet rs = conn.createStatement().executeQuery("SELECT int_col1 FROM " +INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE + " where int_col1 IN (1, 2, 3, 4)");
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
                assertTrue(rs.next());
                assertEquals(4, rs.getInt(1));
                assertFalse(rs.next());
            } finally {
                if(stmt != null) {
                    stmt.close();
                }
            } 
        } finally {
            if(conn != null) {
                conn.close();
            }
        }
    }
}
