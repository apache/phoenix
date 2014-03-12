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
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.SequenceAlreadyExistsException;
import org.apache.phoenix.schema.SequenceNotFoundException;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;

public class SequenceIT extends BaseClientManagedTimeIT {
    private static final long BATCH_SIZE = 3;
    
    private Connection conn;
    
    @BeforeClass 
    public static void doSetup() throws Exception {
        
        Map<String,String> props = Maps.newHashMapWithExpectedSize(1);
        // Make a small batch size to test multiple calls to reserve sequences
        props.put(QueryServices.SEQUENCE_CACHE_SIZE_ATTRIB, Long.toString(BATCH_SIZE));
        // Must update config before starting server
        startServer(getUrl(), new ReadOnlyProps(props.entrySet().iterator()));
    }
    

	@Test
	public void testSystemTable() throws Exception {		
		nextConnection();
		String query = "SELECT sequence_schema, sequence_name, current_value, increment_by FROM SYSTEM.\"SEQUENCE\"";
		ResultSet rs = conn.prepareStatement(query).executeQuery();
		assertFalse(rs.next());
	}

	@Test
	public void testDuplicateSequences() throws Exception {
        nextConnection();
		conn.createStatement().execute("CREATE SEQUENCE alpha.beta START WITH 2 INCREMENT BY 4\n");

		try {
	        nextConnection();
			conn.createStatement().execute("CREATE SEQUENCE alpha.beta START WITH 2 INCREMENT BY 4\n");
			Assert.fail("Duplicate sequences");
		} catch (SequenceAlreadyExistsException e){

		}
	}

	@Test
	public void testSequenceNotFound() throws Exception {
        nextConnection();
		String query = "SELECT NEXT value FOR qwert.asdf FROM SYSTEM.\"SEQUENCE\"";
		try {
			conn.prepareStatement(query).executeQuery();
			fail("Sequence not found");
		}catch(SequenceNotFoundException e){

		}
	}

	@Test
	public void testCreateSequence() throws Exception {	
        nextConnection();
		conn.createStatement().execute("CREATE SEQUENCE alpha.omega START WITH 2 INCREMENT BY 4");
        nextConnection();
		String query = "SELECT sequence_schema, sequence_name, current_value, increment_by FROM SYSTEM.\"SEQUENCE\" WHERE sequence_name='OMEGA'";
		ResultSet rs = conn.prepareStatement(query).executeQuery();
		assertTrue(rs.next());
		assertEquals("ALPHA", rs.getString("sequence_schema"));
		assertEquals("OMEGA", rs.getString("sequence_name"));
		assertEquals(2, rs.getInt("current_value"));
		assertEquals(4, rs.getInt("increment_by"));
		assertFalse(rs.next());
	}
		
    @Test
    public void testCurrentValueFor() throws Exception {
        ResultSet rs;
        nextConnection();
        conn.createStatement().execute("CREATE SEQUENCE used.nowhere START WITH 2 INCREMENT BY 4");
        nextConnection();
        try {
            rs = conn.createStatement().executeQuery("SELECT CURRENT VALUE FOR used.nowhere FROM SYSTEM.\"SEQUENCE\"");
            rs.next();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_CALL_CURRENT_BEFORE_NEXT_VALUE.getErrorCode(), e.getErrorCode());
        }
        
        rs = conn.createStatement().executeQuery("SELECT NEXT VALUE FOR used.nowhere FROM SYSTEM.\"SEQUENCE\"");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        rs = conn.createStatement().executeQuery("SELECT CURRENT VALUE FOR used.nowhere FROM SYSTEM.\"SEQUENCE\"");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
	}

    @Test
    public void testDropSequence() throws Exception { 
        nextConnection();
        conn.createStatement().execute("CREATE SEQUENCE alpha.omega START WITH 2 INCREMENT BY 4");
        nextConnection();
        String query = "SELECT sequence_schema, sequence_name, current_value, increment_by FROM SYSTEM.\"SEQUENCE\" WHERE sequence_name='OMEGA'";
        ResultSet rs = conn.prepareStatement(query).executeQuery();
        assertTrue(rs.next());
        assertEquals("ALPHA", rs.getString("sequence_schema"));
        assertEquals("OMEGA", rs.getString("sequence_name"));
        assertEquals(2, rs.getInt("current_value"));
        assertEquals(4, rs.getInt("increment_by"));
        assertFalse(rs.next());

        conn.createStatement().execute("DROP SEQUENCE alpha.omega");
        nextConnection();
        query = "SELECT sequence_schema, sequence_name, current_value, increment_by FROM SYSTEM.\"SEQUENCE\" WHERE sequence_name='OMEGA'";
        rs = conn.prepareStatement(query).executeQuery();
        assertFalse(rs.next());

        try {
            conn.createStatement().execute("DROP SEQUENCE alpha.omega");
            fail();
        } catch (SequenceNotFoundException ignore) {
        }
    }

	@Test
	public void testSelectNextValueFor() throws Exception {
        nextConnection();
		conn.createStatement().execute("CREATE SEQUENCE foo.bar START WITH 3 INCREMENT BY 2");
        nextConnection();
		String query = "SELECT NEXT VALUE FOR foo.bar FROM SYSTEM.\"SEQUENCE\"";
		ResultSet rs = conn.prepareStatement(query).executeQuery();
		assertTrue(rs.next());
		assertEquals(3, rs.getInt(1));

		rs = conn.prepareStatement(query).executeQuery();
		assertTrue(rs.next());
		assertEquals(5, rs.getInt(1));

		rs = conn.prepareStatement(query).executeQuery();
		assertTrue(rs.next());
		assertEquals(7, rs.getInt(1));
	}

	@Test
	public void testInsertNextValueFor() throws Exception {
        nextConnection();
        conn.createStatement().execute("CREATE SEQUENCE alpha.tau START WITH 2 INCREMENT BY 1");
		conn.createStatement().execute("CREATE TABLE test.sequence_number ( id INTEGER NOT NULL PRIMARY KEY)");
        nextConnection();
		conn.createStatement().execute("UPSERT INTO test.sequence_number (id) VALUES (NEXT VALUE FOR alpha.tau)");
        conn.createStatement().execute("UPSERT INTO test.sequence_number (id) VALUES (NEXT VALUE FOR alpha.tau)");
		conn.commit();
        nextConnection();
		String query = "SELECT id FROM test.sequence_number";		
		ResultSet rs = conn.prepareStatement(query).executeQuery();
		assertTrue(rs.next());
		assertEquals(2, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
	}

	@Test
	public void testSequenceCreation() throws Exception {		
        nextConnection();
		conn.createStatement().execute("CREATE SEQUENCE alpha.gamma START WITH 2 INCREMENT BY 3 CACHE 5");
        nextConnection();
        ResultSet rs = conn.createStatement().executeQuery("SELECT start_with, increment_by, cache_size, sequence_schema, sequence_name FROM SYSTEM.\"SEQUENCE\"");
        assertTrue(rs.next());
        assertEquals(2, rs.getLong(1));
        assertEquals(3, rs.getLong(2));
        assertEquals(5, rs.getLong(3));
        assertEquals("ALPHA", rs.getString(4));
        assertEquals("GAMMA", rs.getString(5));
        assertFalse(rs.next());
		rs = conn.createStatement().executeQuery("SELECT NEXT VALUE FOR alpha.gamma, CURRENT VALUE FOR alpha.gamma FROM SYSTEM.\"SEQUENCE\"");
        assertTrue(rs.next());
        assertEquals(2, rs.getLong(1));
        assertEquals(2, rs.getLong(2));
        assertFalse(rs.next());
        rs = conn.createStatement().executeQuery("SELECT CURRENT VALUE FOR alpha.gamma, NEXT VALUE FOR alpha.gamma FROM SYSTEM.\"SEQUENCE\"");
        assertTrue(rs.next());
        assertEquals(5, rs.getLong(1));
        assertEquals(5, rs.getLong(2));
        assertFalse(rs.next());
	}

    @Test
    public void testSameMultipleSequenceValues() throws Exception {
        nextConnection();
        conn.createStatement().execute("CREATE SEQUENCE alpha.zeta START WITH 4 INCREMENT BY 7");
        nextConnection();
        String query = "SELECT NEXT VALUE FOR alpha.zeta, NEXT VALUE FOR alpha.zeta FROM SYSTEM.\"SEQUENCE\"";
        ResultSet rs = conn.prepareStatement(query).executeQuery();
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        assertEquals(4, rs.getInt(2));
        assertFalse(rs.next());
        conn.close();
    }

    @Test
	public void testMultipleSequenceValues() throws Exception {
        nextConnection();
		conn.createStatement().execute("CREATE SEQUENCE alpha.zeta START WITH 4 INCREMENT BY 7");
		conn.createStatement().execute("CREATE SEQUENCE alpha.kappa START WITH 9 INCREMENT BY 2");
        nextConnection();
		String query = "SELECT NEXT VALUE FOR alpha.zeta, NEXT VALUE FOR alpha.kappa FROM SYSTEM.\"SEQUENCE\"";
		ResultSet rs = conn.prepareStatement(query).executeQuery();
		assertTrue(rs.next());
		assertEquals(4, rs.getInt(1));
		assertEquals(9, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals(4+7, rs.getInt(1));
        assertEquals(9+2, rs.getInt(2));
        assertFalse(rs.next());
        conn.close();
        // Test that sequences don't have gaps (if no other client request the same sequence before we close it)
        nextConnection();
        rs = conn.prepareStatement(query).executeQuery();
        assertTrue(rs.next());
        assertEquals(4+7*2, rs.getInt(1));
        assertEquals(9+2*2, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals(4+7*3, rs.getInt(1));
        assertEquals(9+2*3, rs.getInt(2));
        assertFalse(rs.next());
        conn.close();
	}
	
	@Test
	public void testCompilerOptimization() throws Exception {
		nextConnection();
        conn.createStatement().execute("CREATE SEQUENCE seq.perf START WITH 3 INCREMENT BY 2");        
		conn.createStatement().execute("CREATE TABLE t (k INTEGER NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) IMMUTABLE_ROWS=true");
        nextConnection();
        conn.createStatement().execute("CREATE INDEX idx ON t(v1) INCLUDE (v2)");
        nextConnection();
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        stmt.optimizeQuery("SELECT k, NEXT VALUE FOR seq.perf FROM t WHERE v1 = 'bar'");
	}
	
	@Test
	public void testSelectRowAndSequence() throws Exception {
        nextConnection();
        conn.createStatement().execute("CREATE SEQUENCE alpha.epsilon START WITH 1 INCREMENT BY 4");
		conn.createStatement().execute("CREATE TABLE test.foo ( id INTEGER NOT NULL PRIMARY KEY)");
        nextConnection();
		conn.createStatement().execute("UPSERT INTO test.foo (id) VALUES (NEXT VALUE FOR alpha.epsilon)");
		conn.commit();
        nextConnection();
		String query = "SELECT NEXT VALUE FOR alpha.epsilon, id FROM test.foo";
		ResultSet rs = conn.prepareStatement(query).executeQuery();
		assertTrue(rs.next());
		assertEquals(5, rs.getInt(1));
		assertEquals(1, rs.getInt(2));
        assertFalse(rs.next());
	}

    @Test
    public void testSelectNextValueForOverMultipleBatches() throws Exception {
        nextConnection();
        conn.createStatement().execute("CREATE SEQUENCE foo.bar");
        conn.createStatement().execute("CREATE TABLE foo (k BIGINT NOT NULL PRIMARY KEY)");
        
        nextConnection();
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO foo VALUES(NEXT VALUE FOR foo.bar)");
        for (int i = 0; i < BATCH_SIZE  * 2 + 1; i++) {
            stmt.execute();
        }
        conn.commit();
        nextConnection();
        ResultSet rs = conn.createStatement().executeQuery("SELECT count(*),max(k) FROM foo");
        assertTrue(rs.next());
        assertEquals(BATCH_SIZE * 2 + 1, rs.getInt(1));
        assertEquals(BATCH_SIZE * 2 + 1, rs.getInt(2));
    }

    @Test
    public void testSelectNextValueForGroupBy() throws Exception {
        nextConnection();
        conn.createStatement().execute("CREATE SEQUENCE foo.bar");
        conn.createStatement().execute("CREATE TABLE foo (k BIGINT NOT NULL PRIMARY KEY, v VARCHAR)");
        conn.createStatement().execute("CREATE TABLE bar (k BIGINT NOT NULL PRIMARY KEY, v VARCHAR)");
        
        nextConnection();
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO foo VALUES(NEXT VALUE FOR foo.bar, ?)");
        stmt.setString(1, "a");
        stmt.execute();
        stmt.setString(1, "a");
        stmt.execute();
        stmt.setString(1, "b");
        stmt.execute();
        stmt.setString(1, "b");
        stmt.execute();
        stmt.setString(1, "c");
        stmt.execute();
        conn.commit();
        
        nextConnection();
        ResultSet rs = conn.createStatement().executeQuery("SELECT k from foo");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(5, rs.getInt(1));
        assertFalse(rs.next());

        nextConnection();
        conn.setAutoCommit(true);;
        conn.createStatement().execute("UPSERT INTO bar SELECT NEXT VALUE FOR foo.bar,v FROM foo GROUP BY v");
        nextConnection();
        rs = conn.createStatement().executeQuery("SELECT * from bar");
        assertTrue(rs.next());
        assertEquals(6, rs.getInt(1));
        assertEquals("a", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(7, rs.getInt(1));
        assertEquals("b", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(8, rs.getInt(1));
        assertEquals("c", rs.getString(2));
        assertFalse(rs.next());
    }

    @Test
    public void testSelectNextValueForMultipleConn() throws Exception {
        nextConnection();
        conn.createStatement().execute("CREATE SEQUENCE foo.bar");
        conn.createStatement().execute("CREATE TABLE foo (k BIGINT NOT NULL PRIMARY KEY)");

        nextConnection();
        Connection conn1 = conn;
        conn = null; // So that call to nextConnection doesn't close it
        
        PreparedStatement stmt1 = conn1.prepareStatement("UPSERT INTO foo VALUES(NEXT VALUE FOR foo.bar)");
        for (int i = 0; i < BATCH_SIZE+ 1; i++) {
            stmt1.execute();
        }
        conn1.commit();
        
        nextConnection();
        Connection conn2 = conn;
        PreparedStatement stmt2 = conn2.prepareStatement("UPSERT INTO foo VALUES(NEXT VALUE FOR foo.bar)");
        stmt2.execute();
        stmt1.close(); // Should still continue with next value, even on separate connection
        for (int i = 0; i < BATCH_SIZE; i++) {
            stmt2.execute();
        }
        conn2.commit();
        conn2.close();
        conn1.close();
        
        nextConnection();
        // No gaps exist even when sequences were generated from different connections
        ResultSet rs = conn.createStatement().executeQuery("SELECT k FROM foo");
        for (int i = 0; i < (BATCH_SIZE+ 1)*2; i++) {
            assertTrue(rs.next());
            assertEquals(i+1, rs.getInt(1));
        }
        assertFalse(rs.next());
    }

    @Test
    public void testSelectNextValueForMultipleConnWithStmtClose() throws Exception {
        nextConnection();
        conn.createStatement().execute("CREATE SEQUENCE foo.bar");
        conn.createStatement().execute("CREATE TABLE foo (k BIGINT NOT NULL PRIMARY KEY)");
        
        nextConnection();
        Connection conn1 = conn;
        conn = null; // So that call to nextConnection doesn't close it
        
        PreparedStatement stmt1 = conn1.prepareStatement("UPSERT INTO foo VALUES(NEXT VALUE FOR foo.bar)");
        for (int i = 0; i < BATCH_SIZE+ 1; i++) {
            stmt1.execute();
        }
        conn1.commit();
        stmt1.close();
        
        nextConnection();
        Connection conn2 = conn;
        conn = null; // So that call to nextConnection doesn't close it
        
        PreparedStatement stmt2 = conn2.prepareStatement("UPSERT INTO foo VALUES(NEXT VALUE FOR foo.bar)");
        for (int i = 0; i < BATCH_SIZE + 1; i++) {
            stmt2.execute();
        }
        conn2.commit();
        conn2.close();
        conn1.close();
        
        nextConnection();
        ResultSet rs = conn.createStatement().executeQuery("SELECT k FROM foo");
        for (int i = 0; i < 2*(BATCH_SIZE + 1); i++) {
            assertTrue(rs.next());
            assertEquals(i+1, rs.getInt(1));
        }
        assertFalse(rs.next());
    }

    @Test
    public void testSelectNextValueForMultipleConnWithConnClose() throws Exception {
        nextConnection();
        conn.createStatement().execute("CREATE SEQUENCE foo.bar");
        conn.createStatement().execute("CREATE TABLE foo (k BIGINT NOT NULL PRIMARY KEY)");
        
        nextConnection();
        Connection conn1 = conn;
        conn = null; // So that call to nextConnection doesn't close it
        
        PreparedStatement stmt1 = conn1.prepareStatement("UPSERT INTO foo VALUES(NEXT VALUE FOR foo.bar)");
        for (int i = 0; i < BATCH_SIZE+ 1; i++) {
            stmt1.execute();
        }
        conn1.commit();
        conn1.close(); // will return unused sequences, so no gaps now
        
        nextConnection();
        Connection conn2 = conn;
        conn = null; // So that call to nextConnection doesn't close it
        
        PreparedStatement stmt2 = conn2.prepareStatement("UPSERT INTO foo VALUES(NEXT VALUE FOR foo.bar)");
        for (int i = 0; i < BATCH_SIZE + 1; i++) {
            stmt2.execute();
        }
        conn2.commit();
        conn1.close();
        
        nextConnection();
        ResultSet rs = conn.createStatement().executeQuery("SELECT k FROM foo");
        for (int i = 0; i < 2*(BATCH_SIZE + 1); i++) {
            assertTrue(rs.next());
            assertEquals(i+1, rs.getInt(1));
        }
        assertFalse(rs.next());
    }

    @Test
    public void testDropCachedSeq1() throws Exception {
        testDropCachedSeq(false);
    }
    
    @Test
    public void testDropCachedSeq2() throws Exception {
        testDropCachedSeq(true);
    }
    
    private void testDropCachedSeq(boolean detectDeleteSeqInEval) throws Exception {
        nextConnection();
        conn.createStatement().execute("CREATE SEQUENCE foo.bar");
        conn.createStatement().execute("CREATE SEQUENCE bar.bas START WITH 101");
        conn.createStatement().execute("CREATE TABLE foo (k BIGINT NOT NULL PRIMARY KEY)");
        
        nextConnection();
        Connection conn1 = conn;
        conn = null; // So that call to nextConnection doesn't close it
        
        String stmtStr1a = "UPSERT INTO foo VALUES(NEXT VALUE FOR foo.bar)";
        PreparedStatement stmt1a = conn1.prepareStatement(stmtStr1a);
        stmt1a.execute();
        stmt1a.execute();
        String stmtStr1b = "UPSERT INTO foo VALUES(NEXT VALUE FOR bar.bas)";
        PreparedStatement stmt1b = conn1.prepareStatement(stmtStr1b);
        stmt1b.execute();
        stmt1b.execute();
        stmt1b.execute();
        conn1.commit();
        
        nextConnection();
        Connection conn2 = conn;
        conn = null; // So that call to nextConnection doesn't close it
        
        PreparedStatement stmt2 = conn2.prepareStatement("UPSERT INTO foo VALUES(NEXT VALUE FOR bar.bas)");
        stmt2.execute();
        conn2.commit();
        
        nextConnection();
        ResultSet rs = conn.createStatement().executeQuery("SELECT k FROM foo");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(101, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(102, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(103, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(104, rs.getInt(1));
        assertFalse(rs.next());
        
        nextConnection();
        conn.createStatement().execute("DROP SEQUENCE bar.bas");

        nextConnection();
        stmt1a = conn.prepareStatement(stmtStr1a);
        stmt1a.execute();
        if (!detectDeleteSeqInEval) {
            stmt1a.execute(); // Will allocate new batch for foo.bar and get error for bar.bas, but ignore it
        }
        
        stmt1b = conn.prepareStatement(stmtStr1b);
        try {
            stmt1b.execute(); // Will try to get new batch, but fail b/c sequence has been dropped
            fail();
        } catch (SequenceNotFoundException e) {
        }
        conn1.close();
        conn2.close();
    }

    @Test
    public void testExplainPlanValidatesSequences() throws Exception {
        nextConnection();
        conn.createStatement().execute("CREATE SEQUENCE bar");
        conn.createStatement().execute("CREATE TABLE foo (k BIGINT NOT NULL PRIMARY KEY)");
        
        nextConnection();
        Connection conn2 = conn;
        conn = null; // So that call to nextConnection doesn't close it
        ResultSet rs = conn2.createStatement().executeQuery("EXPLAIN SELECT NEXT VALUE FOR bar FROM foo");
        assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER FOO\n" + 
        		"CLIENT RESERVE VALUES FROM 1 SEQUENCE", QueryUtil.getExplainPlan(rs));
        
        nextConnection();
        rs = conn.createStatement().executeQuery("SELECT sequence_name, current_value FROM SYSTEM.\"SEQUENCE\" WHERE sequence_name='BAR'");
        assertTrue(rs.next());
        assertEquals("BAR", rs.getString(1));
        assertEquals(1, rs.getLong(2));
        conn.close();
        conn2.close();

        nextConnection();
        try {
            conn.createStatement().executeQuery("EXPLAIN SELECT NEXT VALUE FOR zzz FROM foo");
            fail();
        } catch (SequenceNotFoundException e) {
            // expected
        }
        conn.close();
    }
    
	private void nextConnection() throws Exception {
	    if (conn != null) conn.close();
	    long ts = nextTimestamp();
		Properties props = new Properties(TestUtil.TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
		conn = DriverManager.getConnection(getUrl(), props);
	}	
}