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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.primitives.Doubles;

@RunWith(Parameterized.class)
public class MutableIndexIT extends BaseHBaseManagedTimeIT {
    
    protected final boolean localIndex;
    private final String tableDDLOptions;
	
    public MutableIndexIT(boolean localIndex, boolean transactional) {
		this.localIndex = localIndex;
		StringBuilder optionBuilder = new StringBuilder();
		if (transactional) {
			optionBuilder.append("TRANSACTIONAL=true");
		}
		this.tableDDLOptions = optionBuilder.toString();
	}
	
	@Parameters(name="localIndex = {0} , transactional = {1}")
    public static Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][] {     
                 { false, false }, { false, true }, { true, false }, { true, true }
           });
    }
    
    @Test
    public void testCoveredColumnUpdates() throws Exception {
    	Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
	        conn.setAutoCommit(false);
	        // create unique table and index names for each parameterized test
	        String tableName = TestUtil.DEFAULT_DATA_TABLE_NAME + "_" + System.currentTimeMillis();
	        String indexName = "IDX"  + "_" + System.currentTimeMillis();
	        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
	        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
	        
            createMultiCFTestTable(fullTableName, tableDDLOptions);
            populateMultiCFTestTable(fullTableName);
            PreparedStatement stmt = conn.prepareStatement("CREATE " + (localIndex ? " LOCAL " : "") + " INDEX " + indexName + " ON " + fullTableName 
            		+ " (char_col1 ASC, int_col1 ASC) INCLUDE (long_col1, long_col2)");
            stmt.execute();
            
            String query = "SELECT char_col1, int_col1, long_col2 from " + fullTableName;
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            if (localIndex) {
                assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER _LOCAL_IDX_" + fullTableName +" [-32768]\nCLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
            } else {
                assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + fullIndexName, QueryUtil.getExplainPlan(rs));
            }
            
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(2, rs.getInt(2));
            assertEquals(3L, rs.getLong(3));
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(3, rs.getInt(2));
            assertEquals(4L, rs.getLong(3));
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(4, rs.getInt(2));
            assertEquals(5L, rs.getLong(3));
            assertFalse(rs.next());
            
            stmt = conn.prepareStatement("UPSERT INTO " + fullTableName
                    + "(varchar_pk, char_pk, int_pk, long_pk , decimal_pk, long_col2) SELECT varchar_pk, char_pk, int_pk, long_pk , decimal_pk, long_col2*2 FROM "
                    + fullTableName + " WHERE long_col2=?");
            stmt.setLong(1,4L);
            assertEquals(1,stmt.executeUpdate());
            conn.commit();

            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(2, rs.getInt(2));
            assertEquals(3L, rs.getLong(3));
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(3, rs.getInt(2));
            assertEquals(8L, rs.getLong(3));
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(4, rs.getInt(2));
            assertEquals(5L, rs.getLong(3));
            assertFalse(rs.next());
            
            stmt = conn.prepareStatement("UPSERT INTO " + fullTableName
                    + "(varchar_pk, char_pk, int_pk, long_pk , decimal_pk, long_col2) SELECT varchar_pk, char_pk, int_pk, long_pk , decimal_pk, null FROM "
                    + fullTableName + " WHERE long_col2=?");
            stmt.setLong(1,3L);
            assertEquals(1,stmt.executeUpdate());
            conn.commit();
            
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(2, rs.getInt(2));
            assertEquals(0, rs.getLong(3));
            assertTrue(rs.wasNull());
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(3, rs.getInt(2));
            assertEquals(8L, rs.getLong(3));
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(4, rs.getInt(2));
            assertEquals(5L, rs.getLong(3));
            assertFalse(rs.next());
            if(localIndex) {
                query = "SELECT b.* from " + fullTableName + " where int_col1 = 4";
                rs = conn.createStatement().executeQuery("EXPLAIN " + query);
                assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER _LOCAL_IDX_" + fullTableName +" [-32768]\n" +
                		"    SERVER FILTER BY TO_INTEGER(\"INT_COL1\") = 4\nCLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
                rs = conn.createStatement().executeQuery(query);
                assertTrue(rs.next());
                assertEquals("varchar_b", rs.getString(1));
                assertEquals("charb", rs.getString(2));
                assertEquals(5, rs.getInt(3));
                assertEquals(5, rs.getLong(4));
                assertFalse(rs.next());
                
            }
        } 
    }
    
    @Test
    public void testCoveredColumns() throws Exception {
    	Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
	        conn.setAutoCommit(false);
	        String query;
	        ResultSet rs;
	        // create unique table and index names for each parameterized test
	        String tableName = TestUtil.DEFAULT_DATA_TABLE_NAME + "_" + System.currentTimeMillis();
	        String indexName = "IDX"  + "_" + System.currentTimeMillis();
	        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
	        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
	        conn.createStatement().execute("CREATE TABLE " + fullTableName + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)" + tableDDLOptions);
	        query = "SELECT * FROM " + fullTableName;
	        rs = conn.createStatement().executeQuery(query);
	        assertFalse(rs.next());
	        
	        conn.createStatement().execute("CREATE " + (localIndex ? " LOCAL " : "") + " INDEX " + indexName + " ON " + fullTableName + " (v1) INCLUDE (v2)");
	        query = "SELECT * FROM " + fullIndexName;
	        rs = conn.createStatement().executeQuery(query);
	        assertFalse(rs.next());
	
	        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + " VALUES(?,?,?)");
	        stmt.setString(1,"a");
	        stmt.setString(2, "x");
	        stmt.setString(3, "1");
	        stmt.execute();
	        conn.commit();
	        
	        query = "SELECT * FROM " + fullIndexName;
	        rs = conn.createStatement().executeQuery(query);
	        assertTrue(rs.next());
	        assertEquals("x",rs.getString(1));
	        assertEquals("a",rs.getString(2));
	        assertEquals("1",rs.getString(3));
	        assertFalse(rs.next());
	
	        stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + "(k,v2) VALUES(?,?)");
	        stmt.setString(1,"a");
	        stmt.setString(2, null);
	        stmt.execute();
	        conn.commit();
	        
	        query = "SELECT * FROM " + fullIndexName;
	        rs = conn.createStatement().executeQuery(query);
	        assertTrue(rs.next());
	        assertEquals("x",rs.getString(1));
	        assertEquals("a",rs.getString(2));
	        assertNull(rs.getString(3));
	        assertFalse(rs.next());
	
	        query = "SELECT * FROM " + fullTableName;
	        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
	        if(localIndex) {
	            assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER _LOCAL_IDX_" + fullTableName+" [-32768]\nCLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));            
	        } else {
	            assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + fullIndexName, QueryUtil.getExplainPlan(rs));
	        }
	
	        rs = conn.createStatement().executeQuery(query);
	        assertTrue(rs.next());
	        assertEquals("a",rs.getString(1));
	        assertEquals("x",rs.getString(2));
	        assertNull(rs.getString(3));
	        assertFalse(rs.next());
	
	        stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + "(k,v2) VALUES(?,?)");
	        stmt.setString(1,"a");
	        stmt.setString(2,"3");
	        stmt.execute();
	        conn.commit();
	        
	        query = "SELECT * FROM " + fullTableName;
	        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
	        if(localIndex) {
	            assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER _LOCAL_IDX_" + fullTableName + " [-32768]\nCLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));            
	        } else {
	            assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + fullIndexName, QueryUtil.getExplainPlan(rs));
	        }
	        
	        rs = conn.createStatement().executeQuery(query);
	        assertTrue(rs.next());
	        assertEquals("a",rs.getString(1));
	        assertEquals("x",rs.getString(2));
	        assertEquals("3",rs.getString(3));
	        assertFalse(rs.next());
	
	        stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + "(k,v2) VALUES(?,?)");
	        stmt.setString(1,"a");
	        stmt.setString(2,"4");
	        stmt.execute();
	        conn.commit();
	        
	        query = "SELECT * FROM " + fullTableName;
	        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
	        if(localIndex) {
	            assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER _LOCAL_IDX_" + fullTableName+" [-32768]\nCLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));            
	        } else {
	            assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + fullIndexName, QueryUtil.getExplainPlan(rs));
	        }
	        
	        rs = conn.createStatement().executeQuery(query);
	        assertTrue(rs.next());
	        assertEquals("a",rs.getString(1));
	        assertEquals("x",rs.getString(2));
	        assertEquals("4",rs.getString(3));
	        assertFalse(rs.next());
        }
    }

    @Test
    public void testCompoundIndexKey() throws Exception {
    	Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
	        conn.setAutoCommit(false);
	        String query;
	        ResultSet rs;
	        // create unique table and index names for each parameterized test
	        String tableName = TestUtil.DEFAULT_DATA_TABLE_NAME + "_" + System.currentTimeMillis();
	        String indexName = "IDX"  + "_" + System.currentTimeMillis();
	        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
	        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
	
	        // make sure that the tables are empty, but reachable
	        conn.createStatement().execute("CREATE TABLE " + fullTableName + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)" + tableDDLOptions);
	        query = "SELECT * FROM " + fullTableName;
	        rs = conn.createStatement().executeQuery(query);
	        assertFalse(rs.next());
	        conn.createStatement().execute("CREATE " + (localIndex ? " LOCAL " : "") + " INDEX " + indexName + " ON " + fullTableName + " (v1, v2)");
	        query = "SELECT * FROM " + fullIndexName;
	        rs = conn.createStatement().executeQuery(query);
	        assertFalse(rs.next());
	
	        // load some data into the table
	        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + " VALUES(?,?,?)");
	        stmt.setString(1,"a");
	        stmt.setString(2, "x");
	        stmt.setString(3, "1");
	        stmt.execute();
	        conn.commit();
	        
	        query = "SELECT * FROM " + fullIndexName;
	        rs = conn.createStatement().executeQuery(query);
	        assertTrue(rs.next());
	        assertEquals("x",rs.getString(1));
	        assertEquals("1",rs.getString(2));
	        assertEquals("a",rs.getString(3));
	        assertFalse(rs.next());
	
	        stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + " VALUES(?,?,?)");
	        stmt.setString(1,"a");
	        stmt.setString(2, "y");
	        stmt.setString(3, null);
	        stmt.execute();
	        conn.commit();
	        
	        query = "SELECT * FROM " + fullIndexName;
	        rs = conn.createStatement().executeQuery(query);
	        assertTrue(rs.next());
	        assertEquals("y",rs.getString(1));
	        assertNull(rs.getString(2));
	        assertEquals("a",rs.getString(3));
	        assertFalse(rs.next());
	
	        query = "SELECT * FROM " + fullTableName;
	        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
	        if (localIndex) {
	            assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER _LOCAL_IDX_" + fullTableName+" [-32768]\n"
	                    + "    SERVER FILTER BY FIRST KEY ONLY\n"
	                    + "CLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
	        } else {
	            assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + fullIndexName + "\n"
	                       + "    SERVER FILTER BY FIRST KEY ONLY", QueryUtil.getExplainPlan(rs));
	        }
	        //make sure the data table looks like what we expect
	        rs = conn.createStatement().executeQuery(query);
	        assertTrue(rs.next());
	        assertEquals("a",rs.getString(1));
	        assertEquals("y",rs.getString(2));
	        assertNull(rs.getString(3));
	        assertFalse(rs.next());
	        
	        // Upsert new row with null leading index column
	        stmt.setString(1,"b");
	        stmt.setString(2, null);
	        stmt.setString(3, "3");
	        stmt.execute();
	        conn.commit();
	        
	        query = "SELECT * FROM " + fullIndexName;
	        rs = conn.createStatement().executeQuery(query);
	        assertTrue(rs.next());
	        assertEquals(null,rs.getString(1));
	        assertEquals("3",rs.getString(2));
	        assertEquals("b",rs.getString(3));
	        assertTrue(rs.next());
	        assertEquals("y",rs.getString(1));
	        assertNull(rs.getString(2));
	        assertEquals("a",rs.getString(3));
	        assertFalse(rs.next());
	
	        // Update row with null leading index column to have a value
	        stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + " VALUES(?,?)");
	        stmt.setString(1,"b");
	        stmt.setString(2, "z");
	        stmt.execute();
	        conn.commit();
	        
	        query = "SELECT * FROM " + fullIndexName;
	        rs = conn.createStatement().executeQuery(query);
	        assertTrue(rs.next());
	        assertEquals("y",rs.getString(1));
	        assertNull(rs.getString(2));
	        assertEquals("a",rs.getString(3));
	        assertTrue(rs.next());
	        assertEquals("z",rs.getString(1));
	        assertEquals("3",rs.getString(2));
	        assertEquals("b",rs.getString(3));
	        assertFalse(rs.next());
        }

    }
    
    /**
     * There was a case where if there were multiple updates to a single row in the same batch, the
     * index wouldn't be updated correctly as each element of the batch was evaluated with the state
     * previous to the batch, rather than with the rest of the batch. This meant you could do a put
     * and a delete on a row in the same batch and the index result would contain the current + put
     * and current + delete, but not current + put + delete.
     * @throws Exception on failure
     */
    @Test
    public void testMultipleUpdatesToSingleRow() throws Exception {
    	Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
	        conn.setAutoCommit(false);
	        String query;
	        ResultSet rs;
	        // create unique table and index names for each parameterized test
	        String tableName = TestUtil.DEFAULT_DATA_TABLE_NAME + "_" + System.currentTimeMillis();
	        String indexName = "IDX"  + "_" + System.currentTimeMillis();
	        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
	        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
    
	        // make sure that the tables are empty, but reachable
	        conn.createStatement().execute(
	          "CREATE TABLE " + fullTableName
	              + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)" + tableDDLOptions);
	        query = "SELECT * FROM " + fullTableName;
	        rs = conn.createStatement().executeQuery(query);
	        assertFalse(rs.next());
	
	        conn.createStatement().execute("CREATE " + (localIndex ? " LOCAL " : "") + " INDEX " + indexName + " ON " + fullTableName + " (v1, v2)");
	        query = "SELECT * FROM " + fullIndexName;
	        rs = conn.createStatement().executeQuery(query);
	        assertFalse(rs.next());
	    
	        // load some data into the table
	        PreparedStatement stmt =
	            conn.prepareStatement("UPSERT INTO " + fullTableName + " VALUES(?,?,?)");
	        stmt.setString(1, "a");
	        stmt.setString(2, "x");
	        stmt.setString(3, "1");
	        stmt.execute();
	        conn.commit();
	        
	        // make sure the index is working as expected
	        query = "SELECT * FROM " + fullIndexName;
	        rs = conn.createStatement().executeQuery(query);
	        assertTrue(rs.next());
	        assertEquals("x", rs.getString(1));
	        assertEquals("1", rs.getString(2));
	        assertEquals("a", rs.getString(3));
	        assertFalse(rs.next());
	      
	        // do multiple updates to the same row, in the same batch
	        stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + "(k, v1) VALUES(?,?)");
	        stmt.setString(1, "a");
	        stmt.setString(2, "y");
	        stmt.execute();
	        stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + "(k,v2) VALUES(?,?)");
	        stmt.setString(1, "a");
	        stmt.setString(2, null);
	        stmt.execute();
	        conn.commit();
	    
	        query = "SELECT * FROM " + fullIndexName;
	        rs = conn.createStatement().executeQuery(query);
	        assertTrue(rs.next());
	        assertEquals("y", rs.getString(1));
	        assertNull(rs.getString(2));
	        assertEquals("a", rs.getString(3));
	        assertFalse(rs.next());
	    
	        query = "SELECT * FROM " + fullTableName;
	        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
	        if(localIndex) {
	            assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER _LOCAL_IDX_" + fullTableName+" [-32768]\n"
	                    + "    SERVER FILTER BY FIRST KEY ONLY\n"
	                    + "CLIENT MERGE SORT",
	                QueryUtil.getExplainPlan(rs));
	        } else {
	            assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + fullIndexName + "\n"
	                    + "    SERVER FILTER BY FIRST KEY ONLY",
	                QueryUtil.getExplainPlan(rs));
	        }
	    
	        // check that the data table matches as expected
	        rs = conn.createStatement().executeQuery(query);
	        assertTrue(rs.next());
	        assertEquals("a", rs.getString(1));
	        assertEquals("y", rs.getString(2));
	        assertNull(rs.getString(3));
	        assertFalse(rs.next());
        }
    }
    
    @Test
    public void testUpsertingNullForIndexedColumns() throws Exception {
    	Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
	        conn.setAutoCommit(false);
	        ResultSet rs;
	        // create unique table and index names for each parameterized test
	        String tableName = TestUtil.DEFAULT_DATA_TABLE_NAME + "_" + System.currentTimeMillis();
	        String indexName = "IDX"  + "_" + System.currentTimeMillis();
	        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
	        String fullIndexeName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
    		Statement stmt = conn.createStatement();
    		stmt.execute("CREATE TABLE " + fullTableName + "(v1 VARCHAR PRIMARY KEY, v2 DOUBLE, v3 VARCHAR) "+tableDDLOptions);
    		stmt.execute("CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexName + " ON " + fullTableName + "  (v2) INCLUDE(v3)");
    		
    		//create a row with value null for indexed column v2
    		stmt.executeUpdate("upsert into " + fullTableName + " values('cc1', null, 'abc')");
    		conn.commit();
    		
    		//assert values in index table 
    		rs = stmt.executeQuery("select * from " + fullIndexeName);
    		assertTrue(rs.next());
    		assertEquals(0, Doubles.compare(0, rs.getDouble(1)));
    		assertTrue(rs.wasNull());
    		assertEquals("cc1", rs.getString(2));
    		assertEquals("abc", rs.getString(3));
    		assertFalse(rs.next());
    		
    		//assert values in data table
    		rs = stmt.executeQuery("select v1, v2, v3 from " + fullTableName);
    		assertTrue(rs.next());
    		assertEquals("cc1", rs.getString(1));
    		assertEquals(0, Doubles.compare(0, rs.getDouble(2)));
    		assertTrue(rs.wasNull());
    		assertEquals("abc", rs.getString(3));
    		assertFalse(rs.next());
    		
    		//update the previously null value for indexed column v2 to a non-null value 1.23
    		stmt.executeUpdate("upsert into " + fullTableName + " values('cc1', 1.23, 'abc')");
    		conn.commit();
    		
    		//assert values in data table
    		rs = stmt.executeQuery("select /*+ NO_INDEX */ v1, v2, v3 from " + fullTableName);
    		assertTrue(rs.next());
    		assertEquals("cc1", rs.getString(1));
    		assertEquals(0, Doubles.compare(1.23, rs.getDouble(2)));
    		assertEquals("abc", rs.getString(3));
    		assertFalse(rs.next());
    		
    		//assert values in index table 
    		rs = stmt.executeQuery("select * from " + fullIndexeName);
    		assertTrue(rs.next());
    		assertEquals(0, Doubles.compare(1.23, rs.getDouble(1)));
    		assertEquals("cc1", rs.getString(2));
    		assertEquals("abc", rs.getString(3));
    		assertFalse(rs.next());
    		
    		//update the value for indexed column v2 back to null
    		stmt.executeUpdate("upsert into " + fullTableName + " values('cc1', null, 'abc')");
    		conn.commit();
    		
    		//assert values in index table 
    		rs = stmt.executeQuery("select * from " + fullIndexeName);
    		assertTrue(rs.next());
    		assertEquals(0, Doubles.compare(0, rs.getDouble(1)));
    		assertTrue(rs.wasNull());
    		assertEquals("cc1", rs.getString(2));
    		assertEquals("abc", rs.getString(3));
    		assertFalse(rs.next());
    		
    		//assert values in data table
    		rs = stmt.executeQuery("select v1, v2, v3 from " + fullTableName);
    		assertTrue(rs.next());
    		assertEquals("cc1", rs.getString(1));
    		assertEquals(0, Doubles.compare(0, rs.getDouble(2)));
    		assertEquals("abc", rs.getString(3));
    		assertFalse(rs.next());
    	} 
    }
    
	
    private void assertImmutableRows(Connection conn, String fullTableName, boolean expectedValue) throws SQLException {
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        assertEquals(expectedValue, pconn.getTable(new PTableKey(pconn.getTenantId(), fullTableName)).isImmutableRows());
    }
    
    @Test
    public void testAlterTableWithImmutability() throws Exception {
        String query;
        ResultSet rs;
        String tableName = TestUtil.DEFAULT_DATA_TABLE_NAME + "_" + System.currentTimeMillis();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
	        conn.setAutoCommit(false);
	        conn.createStatement().execute(
	            "CREATE TABLE " + fullTableName +" (k VARCHAR NOT NULL PRIMARY KEY, v VARCHAR) " + tableDDLOptions);
	        
	        query = "SELECT * FROM " + fullTableName;
	        rs = conn.createStatement().executeQuery(query);
	        assertFalse(rs.next());
	
	        assertImmutableRows(conn,fullTableName, false);
	        conn.createStatement().execute("ALTER TABLE " + fullTableName +" SET IMMUTABLE_ROWS=true");
	        assertImmutableRows(conn,fullTableName, true);
	        
	        
	        conn.createStatement().execute("ALTER TABLE " + fullTableName +" SET immutable_rows=false");
	        assertImmutableRows(conn,fullTableName, false);
        }
    }

}