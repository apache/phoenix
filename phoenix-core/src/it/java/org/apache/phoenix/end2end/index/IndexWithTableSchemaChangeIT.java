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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

public class IndexWithTableSchemaChangeIT extends ParallelStatsDisabledIT {

    @Test
    public void testImmutableIndexDropIndexedColumn() throws Exception {
        helpTestDropIndexedColumn(false, false);
    }
    
    @Test
    public void testImmutableLocalIndexDropIndexedColumn() throws Exception {
        helpTestDropIndexedColumn(false, true);
    }
    
    @Test
    public void testMutableIndexDropIndexedColumn() throws Exception {
        helpTestDropIndexedColumn(true, false);
    }
    
    @Test
    public void testMutableLocalIndexDropIndexedColumn() throws Exception {
        helpTestDropIndexedColumn(true, true);
    }
    
    public void helpTestDropIndexedColumn(boolean mutable, boolean local) throws Exception {
        String query;
        ResultSet rs;
        PreparedStatement stmt;

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);

        String dataTableName = generateUniqueName();
        String indexName = generateUniqueName();

        try {
	        conn.setAutoCommit(false);
	
	        // make sure that the tables are empty, but reachable
            conn.createStatement().execute(
                "CREATE TABLE " + dataTableName
                        + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)"
                        + (!mutable ? " IMMUTABLE_ROWS=true" : ""));
	        query = "SELECT * FROM " + dataTableName ;
	        rs = conn.createStatement().executeQuery(query);
	        assertFalse(rs.next());
	        conn.createStatement().execute("CREATE " + ( local ? "LOCAL" : "") + " INDEX " + indexName + " ON " + dataTableName + " (v1 || '_' || v2)");
	
	        query = "SELECT * FROM " + dataTableName;
	        rs = conn.createStatement().executeQuery(query);
	        assertFalse(rs.next());
	
	        // load some data into the table
	        stmt = conn.prepareStatement("UPSERT INTO " + dataTableName + " VALUES(?,?,?)");
	        stmt.setString(1, "a");
	        stmt.setString(2, "x");
	        stmt.setString(3, "1");
	        stmt.execute();
	        conn.commit();
	
	        assertIndexExists(conn, dataTableName, true);
	        conn.createStatement().execute("ALTER TABLE " + dataTableName + " DROP COLUMN v1");
	        assertIndexExists(conn, dataTableName, false);
	
	        query = "SELECT * FROM " + dataTableName;
	        rs = conn.createStatement().executeQuery(query);
	        assertTrue(rs.next());
	        assertEquals("a",rs.getString(1));
	        assertEquals("1",rs.getString(2));
	        assertFalse(rs.next());
	
	        // load some data into the table
	        stmt = conn.prepareStatement("UPSERT INTO " + dataTableName + " VALUES(?,?)");
	        stmt.setString(1, "a");
	        stmt.setString(2, "2");
	        stmt.execute();
	        conn.commit();
	
	        query = "SELECT * FROM " + dataTableName;
	        rs = conn.createStatement().executeQuery(query);
	        assertTrue(rs.next());
	        assertEquals("a",rs.getString(1));
	        assertEquals("2",rs.getString(2));
	        assertFalse(rs.next());
        }
        finally {
        	conn.close();
        }
    }
    
    private static void assertIndexExists(Connection conn, String tableName, boolean exists) throws SQLException {
        ResultSet rs = conn.getMetaData().getIndexInfo(null, null, tableName, false, false);
        assertEquals(exists, rs.next());
    }
    
    @Test
    public void testImmutableIndexDropCoveredColumn() throws Exception {
    	helpTestDropCoveredColumn(false, false);
    }
    
    @Test
    public void testImmutableLocalIndexDropCoveredColumn() throws Exception {
    	helpTestDropCoveredColumn(false, true);
    }
    
    @Test
    public void testMutableIndexDropCoveredColumn() throws Exception {
    	helpTestDropCoveredColumn(true, false);
    }
    
    @Test
    public void testMutableLocalIndexDropCoveredColumn() throws Exception {
    	helpTestDropCoveredColumn(true, true);
    }
    
    public void helpTestDropCoveredColumn(boolean mutable, boolean local) throws Exception {
        ResultSet rs;
        PreparedStatement stmt;
        String dataTableName = generateUniqueName();
        String indexName = generateUniqueName();

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
	        conn.setAutoCommit(false);
	
	        // make sure that the tables are empty, but reachable
	        conn.createStatement().execute(
	          "CREATE TABLE " + dataTableName
	              + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR, v3 VARCHAR)");
	        String dataTableQuery = "SELECT * FROM " + dataTableName;
	        rs = conn.createStatement().executeQuery(dataTableQuery);
	        assertFalse(rs.next());
	
	        conn.createStatement().execute("CREATE " + ( local ? "LOCAL" : "") + " INDEX " + indexName + " ON " + dataTableName + " (k || '_' || v1) include (v2, v3)");
	        String indexTableQuery = "SELECT * FROM " + indexName;
	        rs = conn.createStatement().executeQuery(indexTableQuery);
	        assertFalse(rs.next());
	
	        // load some data into the table
	        stmt = conn.prepareStatement("UPSERT INTO " + dataTableName + " VALUES(?,?,?,?)");
	        stmt.setString(1, "a");
	        stmt.setString(2, "x");
	        stmt.setString(3, "1");
	        stmt.setString(4, "j");
	        stmt.execute();
	        conn.commit();
	
	        assertIndexExists(conn, dataTableName, true);
	        conn.createStatement().execute("ALTER TABLE " + dataTableName + " DROP COLUMN v2");
	        assertIndexExists(conn, dataTableName, true);
	
	        // verify data table rows
	        rs = conn.createStatement().executeQuery(dataTableQuery);
	        assertTrue(rs.next());
	        assertEquals("a",rs.getString(1));
	        assertEquals("x",rs.getString(2));
	        assertEquals("j",rs.getString(3));
	        assertFalse(rs.next());
	        
	        // verify index table rows
	        rs = conn.createStatement().executeQuery(indexTableQuery);
	        assertTrue(rs.next());
	        assertEquals("a_x",rs.getString(1));
	        assertEquals("a",rs.getString(2));
	        assertEquals("j",rs.getString(3));
	        assertFalse(rs.next());
	
	        // add another row
	        stmt = conn.prepareStatement("UPSERT INTO " + dataTableName + " VALUES(?,?,?)");
	        stmt.setString(1, "b");
	        stmt.setString(2, "y");
	        stmt.setString(3, "k");
	        stmt.execute();
	        conn.commit();
	
	        // verify data table rows
	        rs = conn.createStatement().executeQuery(dataTableQuery);
	        assertTrue(rs.next());
	        assertEquals("a",rs.getString(1));
	        assertEquals("x",rs.getString(2));
	        assertEquals("j",rs.getString(3));
	        assertTrue(rs.next());
	        assertEquals("b",rs.getString(1));
	        assertEquals("y",rs.getString(2));
	        assertEquals("k",rs.getString(3));
	        assertFalse(rs.next());
	        
	        // verify index table rows
	        rs = conn.createStatement().executeQuery(indexTableQuery);
	        assertTrue(rs.next());
	        assertEquals("a_x",rs.getString(1));
	        assertEquals("a",rs.getString(2));
	        assertEquals("j",rs.getString(3));
	        assertTrue(rs.next());
	        assertEquals("b_y",rs.getString(1));
	        assertEquals("b",rs.getString(2));
	        assertEquals("k",rs.getString(3));
	        assertFalse(rs.next());
        }
        finally {
        	conn.close();
        }
    }
    
    @Test
    public void testImmutableIndexAddPKColumnToTable() throws Exception {
    	helpTestAddPKColumnToTable(false, false);
    }
    
    @Test
    public void testImmutableLocalIndexAddPKColumnToTable() throws Exception {
    	helpTestAddPKColumnToTable(false, true);
    }
    
    @Test
    public void testMutableIndexAddPKColumnToTable() throws Exception {
    	helpTestAddPKColumnToTable(true, false);
    }
    
    @Test
    public void testMutableLocalIndexAddPKColumnToTable() throws Exception {
    	helpTestAddPKColumnToTable(true, true);
    }
    
    public void helpTestAddPKColumnToTable(boolean mutable, boolean local) throws Exception {
        ResultSet rs;
        PreparedStatement stmt;

        String dataTableName = generateUniqueName();
        String indexName = generateUniqueName();

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
	        conn.setAutoCommit(false);
	
	        // make sure that the tables are empty, but reachable
	        conn.createStatement().execute(
	          "CREATE TABLE "  + dataTableName
	              + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
	        String dataTableQuery = "SELECT * FROM " + dataTableName;
	        rs = conn.createStatement().executeQuery(dataTableQuery);
	        assertFalse(rs.next());
	
	        conn.createStatement().execute("CREATE " + ( local ? "LOCAL" : "") + " INDEX " + indexName + " ON " + dataTableName + " (v1 || '_' || v2)");
	        String indexTableQuery = "SELECT * FROM " + indexName;
	        rs = conn.createStatement().executeQuery(indexTableQuery);
	        assertFalse(rs.next());
	
	        // load some data into the table
	        stmt = conn.prepareStatement("UPSERT INTO " + dataTableName + " VALUES(?,?,?)");
	        stmt.setString(1, "a");
	        stmt.setString(2, "x");
	        stmt.setString(3, "1");
	        stmt.execute();
	        conn.commit();
	
	        assertIndexExists(conn, dataTableName, true);
	        conn.createStatement().execute("ALTER TABLE " + dataTableName + " ADD v3 VARCHAR, k2 DECIMAL PRIMARY KEY");
	        rs = conn.getMetaData().getPrimaryKeys("", "", dataTableName);
	        assertTrue(rs.next());
	        assertEquals("K",rs.getString("COLUMN_NAME"));
	        assertEquals(1, rs.getShort("KEY_SEQ"));
	        assertTrue(rs.next());
	        assertEquals("K2",rs.getString("COLUMN_NAME"));
	        assertEquals(2, rs.getShort("KEY_SEQ"));
	
	        rs = conn.getMetaData().getPrimaryKeys("", "", indexName);
	        assertTrue(rs.next());
	        assertEquals(IndexUtil.INDEX_COLUMN_NAME_SEP + "(V1 || '_' || V2)",rs.getString("COLUMN_NAME"));
	        int offset = local ? 1 : 0;
	        assertEquals(offset+1, rs.getShort("KEY_SEQ"));
	        assertTrue(rs.next());
	        assertEquals(IndexUtil.INDEX_COLUMN_NAME_SEP + "K",rs.getString("COLUMN_NAME"));
	        assertEquals(offset+2, rs.getShort("KEY_SEQ"));
	        assertTrue(rs.next());
	        assertEquals(IndexUtil.INDEX_COLUMN_NAME_SEP + "K2",rs.getString("COLUMN_NAME"));
	        assertEquals(offset+3, rs.getShort("KEY_SEQ"));
	
	        // verify data table rows
	        rs = conn.createStatement().executeQuery(dataTableQuery);
	        assertTrue(rs.next());
	        assertEquals("a",rs.getString(1));
	        assertEquals("x",rs.getString(2));
	        assertEquals("1",rs.getString(3));
	        assertNull(rs.getBigDecimal(4));
	        assertFalse(rs.next());
	        
	        // verify index table rows
	        rs = conn.createStatement().executeQuery(indexTableQuery);
	        assertTrue(rs.next());
	        assertEquals("x_1",rs.getString(1));
	        assertEquals("a",rs.getString(2));
	        assertNull(rs.getBigDecimal(3));
	        assertFalse(rs.next());
	
	        // load some data into the table
	        stmt = conn.prepareStatement("UPSERT INTO " + dataTableName + "(K,K2,V1,V2) VALUES(?,?,?,?)");
	        stmt.setString(1, "b");
	        stmt.setBigDecimal(2, BigDecimal.valueOf(2));
	        stmt.setString(3, "y");
	        stmt.setString(4, "2");
	        stmt.execute();
	        conn.commit();
	
	        // verify data table rows
	        rs = conn.createStatement().executeQuery(dataTableQuery);
	        assertTrue(rs.next());
	        assertEquals("a",rs.getString(1));
	        assertEquals("x",rs.getString(2));
	        assertEquals("1",rs.getString(3));
	        assertNull(rs.getString(4));
	        assertNull(rs.getBigDecimal(5));
	        assertTrue(rs.next());
	        assertEquals("b",rs.getString(1));
	        assertEquals("y",rs.getString(2));
	        assertEquals("2",rs.getString(3));
	        assertNull(rs.getString(4));
	        assertEquals(BigDecimal.valueOf(2),rs.getBigDecimal(5));
	        assertFalse(rs.next());
	        
	        // verify index table rows
	        rs = conn.createStatement().executeQuery(indexTableQuery);
	        assertTrue(rs.next());
	        assertEquals("x_1",rs.getString(1));
	        assertEquals("a",rs.getString(2));
	        assertNull(rs.getBigDecimal(3));
	        assertTrue(rs.next());
	        assertEquals("y_2",rs.getString(1));
	        assertEquals("b",rs.getString(2));
	        assertEquals(BigDecimal.valueOf(2),rs.getBigDecimal(3));
	        assertFalse(rs.next());
        }
        finally {
        	conn.close();
        }
    }
}
