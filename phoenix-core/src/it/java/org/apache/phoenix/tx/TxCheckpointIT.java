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
package org.apache.phoenix.tx;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.tephra.Transaction.VisibilityLevel;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TxCheckpointIT extends ParallelStatsDisabledIT {
	
	private final boolean localIndex;
	private final String tableDDLOptions;

	public TxCheckpointIT(boolean localIndex, boolean mutable, boolean columnEncoded) {
	    StringBuilder optionBuilder = new StringBuilder();
		this.localIndex = localIndex;
		if (!columnEncoded) {
            if (optionBuilder.length()!=0)
                optionBuilder.append(",");
            optionBuilder.append("COLUMN_ENCODED_BYTES=0");
        }
        if (!mutable) {
            if (optionBuilder.length()!=0)
                optionBuilder.append(",");
            optionBuilder.append("IMMUTABLE_ROWS=true");
            if (!columnEncoded) {
                optionBuilder.append(",IMMUTABLE_STORAGE_SCHEME="+PTableImpl.ImmutableStorageScheme.ONE_CELL_PER_COLUMN);
            }
        }
        this.tableDDLOptions = optionBuilder.toString();
	}
	
    private static Connection getConnection() throws SQLException {
        return getConnection(PropertiesUtil.deepCopy(TEST_PROPERTIES));
    }
    
    private static Connection getConnection(Properties props) throws SQLException {
        props.setProperty(QueryServices.DEFAULT_TABLE_ISTRANSACTIONAL_ATTRIB, Boolean.toString(true));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        return conn;
    }
	
	@Parameters(name="TxCheckpointIT_localIndex={0},mutable={1},columnEncoded={2}") // name is used by failsafe as file name in reports
    public static Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][] {     
                 { false, false, false }, { false, false, true }, { false, true, false }, { false, true, true },
                 { true, false, false }, { true, false, true }, { true, true, false }, { true, true, true }
           });
    }
    
    @Test
    public void testUpsertSelectDoesntSeeUpsertedData() throws Exception {
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        String seqName = "SEQ_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(tableName, tableName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.MUTATE_BATCH_SIZE_BYTES_ATTRIB, Integer.toString(512));
        props.setProperty(QueryServices.SCAN_CACHE_SIZE_ATTRIB, Integer.toString(3));
        props.setProperty(QueryServices.SCAN_RESULT_CHUNK_SIZE, Integer.toString(3));
        Connection conn = getConnection(props);
        conn.setAutoCommit(true);
        conn.createStatement().execute("CREATE SEQUENCE "+seqName);
        conn.createStatement().execute("CREATE TABLE " + fullTableName + "(pk INTEGER PRIMARY KEY, val INTEGER)" + tableDDLOptions);
        conn.createStatement().execute("CREATE "+(localIndex? "LOCAL " : "")+"INDEX " + indexName + " ON " + fullTableName + "(val)");

        conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES (NEXT VALUE FOR " + seqName + ",1)");
        for (int i=0; i<6; i++) {
            Statement stmt = conn.createStatement();
            int upsertCount = stmt.executeUpdate("UPSERT INTO " + fullTableName + " SELECT NEXT VALUE FOR " + seqName + ", val FROM " + fullTableName);
            assertEquals((int)Math.pow(2, i), upsertCount);
        }
        conn.close();
    }
    
    @Test
    public void testRollbackOfUncommittedDeleteSingleCol() throws Exception {
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(tableName, tableName);
        String indexDDL = "CREATE "+(localIndex? "LOCAL " : "")+"INDEX " + indexName + " ON " + fullTableName + " (v1) INCLUDE(v2)";
        testRollbackOfUncommittedDelete(indexDDL, fullTableName);
    }

    @Test
    public void testRollbackOfUncommittedDeleteMultiCol() throws Exception {
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(tableName, tableName);
        String indexDDL = "CREATE "+(localIndex? "LOCAL " : "")+"INDEX " + indexName + " ON " + fullTableName + " (v1, v2)";
        testRollbackOfUncommittedDelete(indexDDL, fullTableName);
    }
    
    private void testRollbackOfUncommittedDelete(String indexDDL, String fullTableName) throws Exception {
        Connection conn = getConnection();
        conn.setAutoCommit(false);
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE " + fullTableName + "(k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)" + tableDDLOptions);
            stmt.execute(indexDDL);
            
            stmt.executeUpdate("upsert into " + fullTableName + " values('x1', 'y1', 'a1')");
            stmt.executeUpdate("upsert into " + fullTableName + " values('x2', 'y2', 'a2')");
            
            //assert values in data table
            ResultSet rs = stmt.executeQuery("select k, v1, v2 from " + fullTableName + " ORDER BY k");
            assertTrue(rs.next());
            assertEquals("x1", rs.getString(1));
            assertEquals("y1", rs.getString(2));
            assertEquals("a1", rs.getString(3));
            assertTrue(rs.next());
            assertEquals("x2", rs.getString(1));
            assertEquals("y2", rs.getString(2));
            assertEquals("a2", rs.getString(3));
            assertFalse(rs.next());
            
            //assert values in index table
            rs = stmt.executeQuery("select k, v1, v2 from " + fullTableName + " ORDER BY v1");
            assertTrue(rs.next());
            assertEquals("x1", rs.getString(1));
            assertEquals("y1", rs.getString(2));
            assertEquals("a1", rs.getString(3));
            assertTrue(rs.next());
            assertEquals("x2", rs.getString(1));
            assertEquals("y2", rs.getString(2));
            assertEquals("a2", rs.getString(3));
            assertFalse(rs.next());
            
            conn.commit();
            
            stmt.executeUpdate("DELETE FROM " + fullTableName + " WHERE k='x1' AND v1='y1' AND v2='a1'");
            //assert row is delete in data table
            rs = stmt.executeQuery("select k, v1, v2 from " + fullTableName + " ORDER BY k");
            assertTrue(rs.next());
            assertEquals("x2", rs.getString(1));
            assertEquals("y2", rs.getString(2));
            assertEquals("a2", rs.getString(3));
            assertFalse(rs.next());
            
            //assert row is delete in index table
            rs = stmt.executeQuery("select k, v1, v2 from " + fullTableName + " ORDER BY v1");
            assertTrue(rs.next());
            assertEquals("x2", rs.getString(1));
            assertEquals("y2", rs.getString(2));
            assertEquals("a2", rs.getString(3));
            assertFalse(rs.next());
            
            conn.rollback();
            
            //assert two rows in data table
            rs = stmt.executeQuery("select k, v1, v2 from " + fullTableName + " ORDER BY k");
            assertTrue(rs.next());
            assertEquals("x1", rs.getString(1));
            assertEquals("y1", rs.getString(2));
            assertEquals("a1", rs.getString(3));
            assertTrue(rs.next());
            assertEquals("x2", rs.getString(1));
            assertEquals("y2", rs.getString(2));
            assertEquals("a2", rs.getString(3));
            assertFalse(rs.next());
            
            //assert two rows in index table
            rs = stmt.executeQuery("select k, v1, v2 from " + fullTableName + " ORDER BY v1");
            assertTrue(rs.next());
            assertEquals("x1", rs.getString(1)); // fails here
            assertEquals("y1", rs.getString(2));
            assertEquals("a1", rs.getString(3));
            assertTrue(rs.next());
            assertEquals("x2", rs.getString(1));
            assertEquals("y2", rs.getString(2));
            assertEquals("a2", rs.getString(3));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
	@Test
	public void testCheckpointForUpsertSelect() throws Exception {
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(tableName, tableName);
		try (Connection conn = getConnection()) {
			conn.setAutoCommit(false);
			Statement stmt = conn.createStatement();

			stmt.execute("CREATE TABLE " + fullTableName + "(ID BIGINT NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)" + tableDDLOptions);
			stmt.execute("CREATE " + (localIndex ? "LOCAL " : "")
					+ "INDEX " + indexName + " ON " + fullTableName + " (v1) INCLUDE(v2)");

            stmt.executeUpdate("upsert into " + fullTableName + " values(1, 'a2', 'b1')");
            stmt.executeUpdate("upsert into " + fullTableName + " values(2, 'a2', 'b2')");
            stmt.executeUpdate("upsert into " + fullTableName + " values(3, 'a3', 'b3')");
			conn.commit();

			upsertRows(conn, fullTableName);
			conn.rollback();
			verifyRows(conn, fullTableName, 3);

			upsertRows(conn, fullTableName);
			conn.commit();
			verifyRows(conn, fullTableName, 6);
		}
	}

	private void verifyRows(Connection conn, String fullTableName, int expectedMaxId) throws SQLException {
		ResultSet rs;
		//query the data table
		rs = conn.createStatement().executeQuery("select /*+ NO_INDEX */ max(id) from " + fullTableName + "");
		assertTrue(rs.next());
		assertEquals(expectedMaxId, rs.getLong(1));
		assertFalse(rs.next());
		
		// query the index
		rs = conn.createStatement().executeQuery("select /*+ INDEX(DEMO IDX) */ max(id) from " + fullTableName + "");
		assertTrue(rs.next());
		assertEquals(expectedMaxId, rs.getLong(1));
		assertFalse(rs.next());
	}

	private void upsertRows(Connection conn, String fullTableName) throws SQLException {
		ResultSet rs;
		MutationState state = conn.unwrap(PhoenixConnection.class)
				.getMutationState();
		state.startTransaction();
		long wp = state.getWritePointer();
		conn.createStatement().execute(
				"upsert into " + fullTableName + " select max(id)+1, 'a4', 'b4' from " + fullTableName + "");
		assertEquals(VisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT,
				state.getVisibilityLevel());
		assertEquals(wp, state.getWritePointer()); // Make sure write ptr
													// didn't move
		rs = conn.createStatement().executeQuery("select max(id) from " + fullTableName + "");

		assertTrue(rs.next());
		assertEquals(4, rs.getLong(1));
		assertFalse(rs.next());

		conn.createStatement().execute(
				"upsert into " + fullTableName + " select max(id)+1, 'a5', 'b5' from " + fullTableName + "");
		assertEquals(VisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT,
				state.getVisibilityLevel());
		assertNotEquals(wp, state.getWritePointer()); // Make sure write ptr
														// moves
		wp = state.getWritePointer();
		rs = conn.createStatement().executeQuery("select max(id) from " + fullTableName + "");

		assertTrue(rs.next());
		assertEquals(5, rs.getLong(1));
		assertFalse(rs.next());
		
		conn.createStatement().execute(
				"upsert into " + fullTableName + " select max(id)+1, 'a6', 'b6' from " + fullTableName + "");
		assertEquals(VisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT,
				state.getVisibilityLevel());
		assertNotEquals(wp, state.getWritePointer()); // Make sure write ptr
														// moves
		wp = state.getWritePointer();
		rs = conn.createStatement().executeQuery("select max(id) from " + fullTableName + "");

		assertTrue(rs.next());
		assertEquals(6, rs.getLong(1));
		assertFalse(rs.next());
	}
	
	@Test
    public void testCheckpointForDeleteAndUpsert() throws Exception {
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(tableName, tableName);
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
		ResultSet rs;
		try (Connection conn = getConnection()) {
			conn.setAutoCommit(false);
			Statement stmt = conn.createStatement();
			stmt.execute("CREATE TABLE " + fullTableName + "1(ID1 BIGINT NOT NULL PRIMARY KEY, FK1A INTEGER, FK1B INTEGER)" + tableDDLOptions);
			stmt.execute("CREATE TABLE " + fullTableName + "2(ID2 BIGINT NOT NULL PRIMARY KEY, FK2 INTEGER)" + tableDDLOptions);
			stmt.execute("CREATE " + (localIndex ? "LOCAL " : "")
					+ "INDEX " + indexName + " ON " + fullTableName + "1 (FK1B)");
			
			stmt.executeUpdate("upsert into " + fullTableName + "1 values (1, 3, 3)");
			stmt.executeUpdate("upsert into " + fullTableName + "1 values (2, 2, 2)");
			stmt.executeUpdate("upsert into " + fullTableName + "1 values (3, 1, 1)");
			stmt.executeUpdate("upsert into " + fullTableName + "2 values (1, 1)");
			conn.commit();

	        MutationState state = conn.unwrap(PhoenixConnection.class).getMutationState();
	        state.startTransaction();
	        long wp = state.getWritePointer();
	        conn.createStatement().execute("delete from " + fullTableName + "1 where id1=fk1b AND fk1b=id1");
	        assertEquals(VisibilityLevel.SNAPSHOT, state.getVisibilityLevel());
	        assertEquals(wp, state.getWritePointer()); // Make sure write ptr didn't move
	
	        rs = conn.createStatement().executeQuery("select /*+ NO_INDEX */ id1 from " + fullTableName + "1");
	        assertTrue(rs.next());
	        assertEquals(1,rs.getLong(1));
	        assertTrue(rs.next());
	        assertEquals(3,rs.getLong(1));
	        assertFalse(rs.next());
	        
	        rs = conn.createStatement().executeQuery("select /*+ INDEX(DEMO IDX) */ id1 from " + fullTableName + "1");
            assertTrue(rs.next());
	        assertEquals(3,rs.getLong(1));
	        assertTrue(rs.next());
	        assertEquals(1,rs.getLong(1));
	        assertFalse(rs.next());
	
	        conn.createStatement().execute("delete from " + fullTableName + "1 where id1 in (select fk1a from " + fullTableName + "1 join " + fullTableName + "2 on (fk2=id1))");
	        assertEquals(VisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT, state.getVisibilityLevel());
	        assertNotEquals(wp, state.getWritePointer()); // Make sure write ptr moved
	
	        rs = conn.createStatement().executeQuery("select /*+ NO_INDEX */ id1 from " + fullTableName + "1");
	        assertTrue(rs.next());
	        assertEquals(1,rs.getLong(1));
	        assertFalse(rs.next());
	
            rs = conn.createStatement().executeQuery("select /*+ INDEX(DEMO IDX) */ id1 from " + fullTableName + "1");
            assertTrue(rs.next());
            assertEquals(1,rs.getLong(1));
            assertFalse(rs.next());
    
            stmt.executeUpdate("upsert into " + fullTableName + "1 SELECT id1 + 3, id1, id1 FROM " + fullTableName + "1");
            stmt.executeUpdate("upsert into " + fullTableName + "2 values (2, 4)");

            conn.createStatement().execute("delete from " + fullTableName + "1 where id1 in (select fk1a from " + fullTableName + "1 join " + fullTableName + "2 on (fk2=id1))");
            assertEquals(VisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT, state.getVisibilityLevel());
            assertNotEquals(wp, state.getWritePointer()); // Make sure write ptr moved
    
            rs = conn.createStatement().executeQuery("select /*+ NO_INDEX */ id1 from " + fullTableName + "1");
            assertTrue(rs.next());
            assertEquals(4,rs.getLong(1));
            assertFalse(rs.next());
    
            rs = conn.createStatement().executeQuery("select /*+ INDEX(DEMO IDX) */ id1 from " + fullTableName + "1");
            assertTrue(rs.next());
            assertEquals(4,rs.getLong(1));
            assertFalse(rs.next());
    
	        conn.rollback();
	        
	        rs = conn.createStatement().executeQuery("select /*+ NO_INDEX */ id1 from " + fullTableName + "1");
	        assertTrue(rs.next());
	        assertEquals(1,rs.getLong(1));
	        assertTrue(rs.next());
	        assertEquals(2,rs.getLong(1));
	        assertTrue(rs.next());
	        assertEquals(3,rs.getLong(1));
	        assertFalse(rs.next());

	        rs = conn.createStatement().executeQuery("select /*+ INDEX(DEMO IDX) */ id1 from " + fullTableName + "1");
            assertTrue(rs.next());
            assertEquals(3,rs.getLong(1));
            assertTrue(rs.next());
            assertEquals(2,rs.getLong(1));
            assertTrue(rs.next());
            assertEquals(1,rs.getLong(1));
            assertFalse(rs.next());
		}
    }  

    
}
