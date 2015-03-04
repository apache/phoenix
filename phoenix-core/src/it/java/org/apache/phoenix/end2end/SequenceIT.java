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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.SequenceAlreadyExistsException;
import org.apache.phoenix.schema.SequenceNotFoundException;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SequenceUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;


public class SequenceIT extends BaseClientManagedTimeIT {
    private static final String NEXT_VAL_SQL = "SELECT NEXT VALUE FOR foo.bar FROM SYSTEM.\"SEQUENCE\"";
    private static final long BATCH_SIZE = 3;
   
    private Connection conn;
    
    @BeforeClass
    @Shadower(classBeingShadowed = BaseClientManagedTimeIT.class)
    public static void doSetup() throws Exception {
        Map<String,String> props = getDefaultProps();
        // Must update config before starting server
        props.put(QueryServices.SEQUENCE_CACHE_SIZE_ATTRIB, Long.toString(BATCH_SIZE));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }
    
    @After
    public void tearDown() throws Exception {
        // close any open connection between tests, so that connections are not leaked
    	if (conn != null) {
    		conn.close();
    	}
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
            assertTrue(e.getNextException()==null);
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
        assertSequenceValuesForSingleRow(3, 5, 7);
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
        conn.createStatement()
                .execute(
                    "CREATE SEQUENCE alpha.gamma START WITH 2 INCREMENT BY 3 MINVALUE 0 MAXVALUE 10 CYCLE CACHE 5");
        nextConnection();
        ResultSet rs =
                conn.createStatement()
                        .executeQuery(
                            "SELECT start_with, current_value, increment_by, cache_size, min_value, max_value, cycle_flag, sequence_schema, sequence_name FROM SYSTEM.\"SEQUENCE\"");
        assertTrue(rs.next());
        assertEquals(2, rs.getLong("start_with"));
        assertEquals(2, rs.getInt("current_value"));
        assertEquals(3, rs.getLong("increment_by"));
        assertEquals(5, rs.getLong("cache_size"));
        assertEquals(0, rs.getLong("min_value"));
        assertEquals(10, rs.getLong("max_value"));
        assertEquals(true, rs.getBoolean("cycle_flag"));
        assertEquals("ALPHA", rs.getString("sequence_schema"));
        assertEquals("GAMMA", rs.getString("sequence_name"));
        assertFalse(rs.next());
        rs =
                conn.createStatement()
                        .executeQuery(
                            "SELECT NEXT VALUE FOR alpha.gamma, CURRENT VALUE FOR alpha.gamma FROM SYSTEM.\"SEQUENCE\"");
        assertTrue(rs.next());
        assertEquals(2, rs.getLong(1));
        assertEquals(2, rs.getLong(2));
        assertFalse(rs.next());
        rs =
                conn.createStatement()
                        .executeQuery(
                            "SELECT CURRENT VALUE FOR alpha.gamma, NEXT VALUE FOR alpha.gamma FROM SYSTEM.\"SEQUENCE\"");
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
    public void testMultipleSequencesNoCycle() throws Exception {
        nextConnection();
        conn.createStatement().execute(
            "CREATE SEQUENCE alpha.zeta START WITH 4 INCREMENT BY 7 MAXVALUE 24");
        conn.createStatement().execute(
            "CREATE SEQUENCE alpha.kappa START WITH 9 INCREMENT BY -2 MINVALUE 5");
        nextConnection();
        String query =
                "SELECT NEXT VALUE FOR alpha.zeta, NEXT VALUE FOR alpha.kappa FROM SYSTEM.\"SEQUENCE\"";
        ResultSet rs = conn.prepareStatement(query).executeQuery();
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        assertEquals(9, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals(4 + 7, rs.getInt(1));
        assertEquals(9 - 2, rs.getInt(2));
        assertFalse(rs.next());
        conn.close();
        
        nextConnection();
        rs = conn.prepareStatement(query).executeQuery();
        assertTrue(rs.next());
        assertEquals(4 + 7 * 2, rs.getInt(1));
        assertEquals(9 - 2 * 2, rs.getInt(2));
        try {
            rs.next();
            fail();
        } catch (SQLException e) {
            SQLException sqlEx1 =
                    SequenceUtil.getException("ALPHA", "ZETA",
                        SQLExceptionCode.SEQUENCE_VAL_REACHED_MAX_VALUE);
            SQLException sqlEx2 =
                    SequenceUtil.getException("ALPHA", "KAPPA",
                        SQLExceptionCode.SEQUENCE_VAL_REACHED_MIN_VALUE);
            verifyExceptions(e, Lists.newArrayList(sqlEx1.getMessage(), sqlEx2.getMessage()));
        }
        conn.close();
    }
    
    @Test
    public void testMultipleSequencesCycle() throws Exception {
        nextConnection();
        conn.createStatement().execute(
            "CREATE SEQUENCE alpha.zeta START WITH 4 INCREMENT BY 7 MINVALUE 4 MAXVALUE 19 CYCLE");
        conn.createStatement().execute(
            "CREATE SEQUENCE alpha.kappa START WITH 9 INCREMENT BY -2 MINVALUE 5 MAXVALUE 9 CYCLE");
        nextConnection();
        String query =
                "SELECT NEXT VALUE FOR alpha.zeta, NEXT VALUE FOR alpha.kappa FROM SYSTEM.\"SEQUENCE\"";
        ResultSet rs = conn.prepareStatement(query).executeQuery();
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        assertEquals(9, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals(4 + 7, rs.getInt(1));
        assertEquals(9 - 2, rs.getInt(2));
        assertFalse(rs.next());
        conn.close();
        
        nextConnection();
        rs = conn.prepareStatement(query).executeQuery();
        assertTrue(rs.next());
        assertEquals(4 + 7 * 2, rs.getInt(1));
        assertEquals(9 - 2 * 2, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        assertEquals(9, rs.getInt(2));
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
        conn1.close(); 
       
        nextConnection();
        Connection conn2 = conn;
        conn = null; // So that call to nextConnection doesn't close it
        
        PreparedStatement stmt2 = conn2.prepareStatement("UPSERT INTO foo VALUES(NEXT VALUE FOR foo.bar)");
        for (int i = 0; i < BATCH_SIZE + 1; i++) {
            stmt2.execute();
        }
        conn2.commit();
        conn2.close();
        
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
                "    SERVER FILTER BY FIRST KEY ONLY\n" +
        		"CLIENT RESERVE VALUES FROM 1 SEQUENCE", QueryUtil.getExplainPlan(rs));
        
        nextConnection();
        rs = conn.createStatement().executeQuery("SELECT sequence_name, current_value FROM SYSTEM.\"SEQUENCE\" WHERE sequence_name='BAR'");
        assertTrue(rs.next());
        assertEquals("BAR", rs.getString(1));
        assertEquals(1, rs.getInt(2));
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
    
    @Test
    public void testSelectNextValueAsInput() throws Exception {
        nextConnection();
        conn.createStatement().execute("CREATE SEQUENCE foo.bar START WITH 3 INCREMENT BY 2");
        nextConnection();
        String query = "SELECT LPAD(ENCODE(NEXT VALUE FOR foo.bar,'base62'),5,'0') FROM SYSTEM.\"SEQUENCE\"";
        ResultSet rs = conn.prepareStatement(query).executeQuery();
        assertTrue(rs.next());
        assertEquals("00003", rs.getString(1));
    }
    
    @Test
    public void testSelectNextValueInArithmetic() throws Exception {
        nextConnection();
        conn.createStatement().execute("CREATE SEQUENCE foo.bar START WITH 3 INCREMENT BY 2");
        nextConnection();
        String query = "SELECT NEXT VALUE FOR foo.bar+1 FROM SYSTEM.\"SEQUENCE\"";
        ResultSet rs = conn.prepareStatement(query).executeQuery();
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
    }
    
    // if nextConnection() is not used to get to get a connection, make sure you call .close() so that connections are not leaked
    private void nextConnection() throws Exception {
        if (conn != null) conn.close();
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);
    }   
    
    @Test
    public void testSequenceDefault() throws Exception {
        nextConnection();    
        conn.createStatement().execute("CREATE SEQUENCE foo.bar");
        nextConnection();
        assertSequenceValuesForSingleRow(1, 2, 3);
        conn.createStatement().execute("DROP SEQUENCE foo.bar");
        
        nextConnection();
        conn.createStatement().execute("CREATE SEQUENCE foo.bar INCREMENT BY -1");
        nextConnection();
        assertSequenceValuesForSingleRow(1, 0, -1);
        conn.createStatement().execute("DROP SEQUENCE foo.bar");
        
        nextConnection();
        conn.createStatement().execute("CREATE SEQUENCE foo.bar MINVALUE 10");
        nextConnection();
        assertSequenceValuesForSingleRow(10, 11, 12);
        conn.createStatement().execute("DROP SEQUENCE foo.bar");
        
        nextConnection();
        conn.createStatement().execute("CREATE SEQUENCE foo.bar INCREMENT BY -1 MINVALUE 10 ");
        nextConnection();
        assertSequenceValuesForSingleRow(Long.MAX_VALUE, Long.MAX_VALUE - 1, Long.MAX_VALUE - 2);
        conn.createStatement().execute("DROP SEQUENCE foo.bar");

        nextConnection();
        conn.createStatement().execute("CREATE SEQUENCE foo.bar MAXVALUE 0");
        nextConnection();
        assertSequenceValuesForSingleRow(Long.MIN_VALUE, Long.MIN_VALUE + 1, Long.MIN_VALUE + 2);
        conn.createStatement().execute("DROP SEQUENCE foo.bar");
        
        nextConnection();
        conn.createStatement().execute("CREATE SEQUENCE foo.bar INCREMENT BY -1 MAXVALUE 0");
        nextConnection();
        assertSequenceValuesForSingleRow(0, -1, -2);
    }

    @Test
    public void testSequenceValidateStartValue() throws Exception {
        nextConnection();
        try {
            conn.createStatement().execute(
                "CREATE SEQUENCE foo.bar1 START WITH 1 INCREMENT BY 1 MINVALUE 2 MAXVALUE 3");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.STARTS_WITH_MUST_BE_BETWEEN_MIN_MAX_VALUE.getErrorCode(),
                e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }

        try {
            conn.createStatement().execute(
                "CREATE SEQUENCE foo.bar2 START WITH 4 INCREMENT BY 1 MINVALUE 2 MAXVALUE 3");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.STARTS_WITH_MUST_BE_BETWEEN_MIN_MAX_VALUE.getErrorCode(),
                e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }

    @Test
    public void testSequenceValidateMinValue() throws Exception {
        nextConnection();
        try {
            conn.createStatement().execute("CREATE SEQUENCE foo.bar MINVALUE abc");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.MINVALUE_MUST_BE_CONSTANT.getErrorCode(),
                e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }

    @Test
    public void testSequenceValidateMaxValue() throws Exception {
        nextConnection();
        try {
            conn.createStatement().execute("CREATE SEQUENCE foo.bar MAXVALUE null");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.MAXVALUE_MUST_BE_CONSTANT.getErrorCode(),
                e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }

    @Test
    public void testSequenceValidateMinValueLessThanOrEqualToMaxValue() throws Exception {
        nextConnection();
        try {
            conn.createStatement().execute("CREATE SEQUENCE foo.bar MINVALUE 2 MAXVALUE 1");
            fail();
        } catch (SQLException e) {
            assertEquals(
                SQLExceptionCode.MINVALUE_MUST_BE_LESS_THAN_OR_EQUAL_TO_MAXVALUE.getErrorCode(),
                e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }

    @Test
    public void testSequenceValidateIncrementConstant() throws Exception {
        nextConnection();
        try {
            conn.createStatement().execute("CREATE SEQUENCE foo.bar INCREMENT null");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.INCREMENT_BY_MUST_BE_CONSTANT.getErrorCode(),
                e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }

    @Test
    public void testSequenceValidateIncrementNotEqualToZero() throws Exception {
        nextConnection();
        try {
            conn.createStatement().execute("CREATE SEQUENCE foo.bar INCREMENT 0");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.INCREMENT_BY_MUST_NOT_BE_ZERO.getErrorCode(),
                e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }
    
    @Test
    public void testSequenceStartWithMinMaxSameValueIncreasingCycle() throws Exception {
        nextConnection();
        conn.createStatement()
                .execute(
                    "CREATE SEQUENCE foo.bar START WITH 3 INCREMENT BY 1 MINVALUE 3 MAXVALUE 3 CYCLE CACHE 1");
        nextConnection();
        assertSequenceValuesForSingleRow(3, 3, 3);
    }
    
    @Test
    public void testSequenceStartWithMinMaxSameValueDecreasingCycle() throws Exception {
        nextConnection();
        conn.createStatement()
                .execute(
                    "CREATE SEQUENCE foo.bar START WITH 3 INCREMENT BY -1 MINVALUE 3 MAXVALUE 3 CYCLE CACHE 2");
        nextConnection();
        assertSequenceValuesForSingleRow(3, 3, 3);
    }
    
    @Test
    public void testSequenceStartWithMinMaxSameValueIncreasingNoCycle() throws Exception {
        nextConnection();
        conn.createStatement()
                .execute(
                    "CREATE SEQUENCE foo.bar START WITH 3 INCREMENT BY 1 MINVALUE 3 MAXVALUE 3 CACHE 1");
        nextConnection();
        assertSequenceValuesForSingleRow(3);
        try {
            ResultSet rs = conn.createStatement().executeQuery(NEXT_VAL_SQL);
            rs.next();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.SEQUENCE_VAL_REACHED_MAX_VALUE.getErrorCode(),
                e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }
    
    @Test
    public void testSequenceStartWithMinMaxSameValueDecreasingNoCycle() throws Exception {
        nextConnection();
        conn.createStatement()
                .execute(
                    "CREATE SEQUENCE foo.bar START WITH 3 INCREMENT BY -1 MINVALUE 3 MAXVALUE 3 CACHE 2");
        nextConnection();
        assertSequenceValuesForSingleRow(3);
        try {
            ResultSet rs = conn.createStatement().executeQuery(NEXT_VAL_SQL);
            rs.next();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.SEQUENCE_VAL_REACHED_MIN_VALUE.getErrorCode(),
                e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }

    @Test
    public void testSequenceIncreasingCycle() throws Exception {
        nextConnection();
        conn.createStatement()
                .execute(
                    "CREATE SEQUENCE foo.bar START WITH 2 INCREMENT BY 3 MINVALUE 1 MAXVALUE 10 CYCLE CACHE 2");
        nextConnection();
        assertSequenceValuesForSingleRow(2, 5, 8, 1, 4, 7, 10, 1, 4);
    }

    @Test
    public void testSequenceDecreasingCycle() throws Exception {
        nextConnection();
        conn.createStatement()
                .execute(
                    "CREATE SEQUENCE foo.bar START WITH 3 INCREMENT BY -2 MINVALUE 1 MAXVALUE 10 CYCLE CACHE 2");
        nextConnection();
        assertSequenceValuesForSingleRow(3, 1, 10, 8, 6, 4, 2, 10, 8);
    }

    @Test
    public void testSequenceIncreasingNoCycle() throws Exception {
        nextConnection();
        // client throws exception
        conn.createStatement().execute(
            "CREATE SEQUENCE foo.bar START WITH 2 INCREMENT BY 3 MINVALUE 1 MAXVALUE 10 CACHE 100");
        nextConnection();
        assertSequenceValuesForSingleRow(2, 5, 8);
        try {
            ResultSet rs = conn.createStatement().executeQuery(NEXT_VAL_SQL);
            rs.next();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.SEQUENCE_VAL_REACHED_MAX_VALUE.getErrorCode(),
                e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }

    @Test
    public void testSequenceIncreasingUsingMaxValueNoCycle() throws Exception {
        nextConnection();
        // server throws exception
        conn.createStatement().execute(
            "CREATE SEQUENCE foo.bar START WITH 8 INCREMENT BY 2 MINVALUE 1 MAXVALUE 10 CACHE 2");
        nextConnection();
        assertSequenceValuesForSingleRow(8, 10);
        try {
            ResultSet rs = conn.createStatement().executeQuery(NEXT_VAL_SQL);
            rs.next();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.SEQUENCE_VAL_REACHED_MAX_VALUE.getErrorCode(),
                e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }

    @Test
    public void testSequenceDecreasingNoCycle() throws Exception {
        nextConnection();
        // client will throw exception
        conn.createStatement()
                .execute(
                    "CREATE SEQUENCE foo.bar START WITH 4 INCREMENT BY -2 MINVALUE 1 MAXVALUE 10 CACHE 100");
        nextConnection();
        assertSequenceValuesForSingleRow(4, 2);
        try {
            ResultSet rs = conn.createStatement().executeQuery(NEXT_VAL_SQL);
            rs.next();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.SEQUENCE_VAL_REACHED_MIN_VALUE.getErrorCode(),
                e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }

    @Test
    public void testSequenceDecreasingUsingMinValueNoCycle() throws Exception {
        nextConnection();
        // server will throw exception
        conn.createStatement().execute(
            "CREATE SEQUENCE foo.bar START WITH 3 INCREMENT BY -2 MINVALUE 1 MAXVALUE 10 CACHE 2");
        nextConnection();
        assertSequenceValuesForSingleRow(3, 1);
        try {
            ResultSet rs = conn.createStatement().executeQuery(NEXT_VAL_SQL);
            rs.next();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.SEQUENCE_VAL_REACHED_MIN_VALUE.getErrorCode(),
                e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }

    @Test
    public void testSequenceIncreasingOverflowNoCycle() throws Exception {
        nextConnection();
        // start with Long.MAX_VALUE
        conn.createStatement().execute(
            "CREATE SEQUENCE foo.bar START WITH 9223372036854775807 INCREMENT BY 1 CACHE 10");
        nextConnection();
        assertSequenceValuesForSingleRow(Long.MAX_VALUE);
        try {
            ResultSet rs = conn.createStatement().executeQuery(NEXT_VAL_SQL);
            rs.next();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.SEQUENCE_VAL_REACHED_MAX_VALUE.getErrorCode(),
                e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }

    @Test
    public void testSequenceIncreasingOverflowCycle() throws Exception {
        nextConnection();
        // start with Long.MAX_VALUE
        conn.createStatement()
                .execute(
                    "CREATE SEQUENCE foo.bar START WITH 9223372036854775807 INCREMENT BY 9223372036854775807 CYCLE CACHE 10");
        nextConnection();
        assertSequenceValuesForSingleRow(Long.MAX_VALUE, Long.MIN_VALUE, -1, Long.MAX_VALUE - 1,
            Long.MIN_VALUE, -1);
    }

    @Test
    public void testSequenceDecreasingOverflowNoCycle() throws Exception {
        nextConnection();
        // start with Long.MIN_VALUE + 1
        conn.createStatement().execute(
            "CREATE SEQUENCE foo.bar START WITH -9223372036854775807 INCREMENT BY -1 CACHE 10");
        nextConnection();
        assertSequenceValuesForSingleRow(Long.MIN_VALUE + 1, Long.MIN_VALUE);
        try {
            ResultSet rs = conn.createStatement().executeQuery(NEXT_VAL_SQL);
            rs.next();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.SEQUENCE_VAL_REACHED_MIN_VALUE.getErrorCode(),
                e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }

    @Test
    public void testSequenceDecreasingOverflowCycle() throws Exception {
        nextConnection();
        // start with Long.MIN_VALUE + 1
        conn.createStatement()
                .execute(
                    "CREATE SEQUENCE foo.bar START WITH -9223372036854775807 INCREMENT BY -9223372036854775807 CYCLE CACHE 10");
        nextConnection();
        assertSequenceValuesForSingleRow(Long.MIN_VALUE + 1, Long.MAX_VALUE, 0, Long.MIN_VALUE + 1,
            Long.MAX_VALUE, 0);
    }

    @Test
    public void testMultipleSequenceValuesNoCycle() throws Exception {
        nextConnection();
        conn.createStatement().execute(
            "CREATE SEQUENCE foo.bar START WITH 1 INCREMENT BY 2 MINVALUE 1 MAXVALUE 10 CACHE 2");
        conn.createStatement().execute("CREATE SEQUENCE foo.bar2");
        nextConnection();
        assertSequenceValuesMultipleSeq(1, 3);
        assertSequenceValuesMultipleSeq(5, 7);

        ResultSet rs = conn.prepareStatement(NEXT_VAL_SQL).executeQuery();
        assertTrue(rs.next());
        assertEquals(9, rs.getInt(1));
        try {
            assertTrue(rs.next());
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.SEQUENCE_VAL_REACHED_MAX_VALUE.getErrorCode(),
                e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }

        try {
            rs = conn.prepareStatement(NEXT_VAL_SQL).executeQuery();
            rs.next();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.SEQUENCE_VAL_REACHED_MAX_VALUE.getErrorCode(),
                e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }

    @Test
    public void testMultipleSequenceValuesCycle() throws Exception {
        nextConnection();
        conn.createStatement()
                .execute(
                    "CREATE SEQUENCE foo.bar START WITH 1 INCREMENT BY 2 MINVALUE 1 MAXVALUE 10 CYCLE CACHE 2");
        conn.createStatement().execute("CREATE SEQUENCE foo.bar2");
        nextConnection();
        assertSequenceValuesMultipleSeq(1, 3);
        assertSequenceValuesMultipleSeq(5, 7);
        assertSequenceValuesMultipleSeq(9, 1);
        assertSequenceValuesMultipleSeq(3, 5);
        assertSequenceValuesMultipleSeq(7, 9);
        assertSequenceValuesMultipleSeq(1, 3);
        assertSequenceValuesMultipleSeq(5, 7);
    }

    @Test
    public void testUpsertSelectGroupByWithSequence() throws Exception {
        nextConnection();
        conn.createStatement().execute("CREATE SEQUENCE foo.bar");
        nextConnection();

        conn.createStatement()
                .execute(
                    "CREATE TABLE EVENTS (event_id BIGINT NOT NULL PRIMARY KEY, user_id char(15), val BIGINT )");
        conn.createStatement()
                .execute(
                    "CREATE TABLE METRICS (metric_id char(15) NOT NULL PRIMARY KEY, agg_id char(15), metric_val INTEGER )");

        nextConnection();
        // 2 rows for user1, 3 rows for user2 and 1 row for user3
        insertEvent(1, "user1", 1);
        insertEvent(2, "user2", 1);
        insertEvent(3, "user1", 1);
        insertEvent(4, "user2", 1);
        insertEvent(5, "user2", 1);
        insertEvent(6, "user3", 1);
        conn.commit();
        nextConnection();

        conn.createStatement()
                .execute(
                    "UPSERT INTO METRICS SELECT 'METRIC_'||(LPAD(ENCODE(NEXT VALUE FOR foo.bar,'base62'),5,'0')), user_id, sum(val) FROM events GROUP BY user_id ORDER BY user_id");
        conn.commit();
        nextConnection();

        PreparedStatement stmt =
                conn.prepareStatement("SELECT metric_id, agg_id, metric_val FROM METRICS");
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals("METRIC_00001", rs.getString("metric_id"));
        assertEquals("user1", rs.getString("agg_id"));
        assertEquals(2, rs.getLong("metric_val"));
        assertTrue(rs.next());
        assertEquals("METRIC_00002", rs.getString("metric_id"));
        assertEquals("user2", rs.getString("agg_id"));
        assertEquals(3, rs.getLong("metric_val"));
        assertTrue(rs.next());
        assertEquals("METRIC_00003", rs.getString("metric_id"));
        assertEquals("user3", rs.getString("agg_id"));
        assertEquals(1, rs.getLong("metric_val"));
        assertFalse(rs.next());
    }

    private void insertEvent(long id, String userId, long val) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO events VALUES(?,?,?)");
        stmt.setLong(1, id);
        stmt.setString(2, userId);
        stmt.setLong(3, val);
        stmt.execute();
    }

    /**
     * Helper to verify the sequence values returned in multiple ResultSets each containing one row
     * @param seqVals expected sequence values (one per ResultSet)
     */
    private void assertSequenceValuesForSingleRow(long... seqVals)
            throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(NEXT_VAL_SQL);
        for (long seqVal : seqVals) {
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(seqVal, rs.getLong(1));
            assertFalse(rs.next());
            rs.close();
        }
        stmt.close();
    }

    /**
     * Helper to verify the sequence values returned in a single ResultSet containing multiple row
     * @param seqVals expected sequence values (from one ResultSet)
     */
    private void assertSequenceValuesMultipleSeq(long... seqVals) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(NEXT_VAL_SQL);
        ResultSet rs = stmt.executeQuery();
        for (long seqVal : seqVals) {
            assertTrue(rs.next());
            assertEquals(seqVal, rs.getLong(1));
        }
        assertFalse(rs.next());
        rs.close();
        stmt.close();
    }

    private void verifyExceptions(SQLException sqlE, List<String> expectedExceptions) {
        List<String> missingExceptions = Lists.newArrayList(expectedExceptions);
        List<String> unexpectedExceptions = Lists.newArrayList();
        do {
            if (!expectedExceptions.contains(sqlE.getMessage())) {
                unexpectedExceptions.add(sqlE.getMessage());
            }
            missingExceptions.remove(sqlE.getMessage());
        } while ((sqlE = sqlE.getNextException()) != null);
        if (unexpectedExceptions.size() != 0 && missingExceptions.size() != 0) {
            fail("Actual exceptions does not match expected exceptions. Unexpected exceptions : "
                    + unexpectedExceptions + " missing exceptions : " + missingExceptions);
        }
    }
    
    @Test
    public void testValidateBeforeReserve() throws Exception {
        nextConnection();
        conn.createStatement().execute(
                "CREATE TABLE foo (k VARCHAR PRIMARY KEY, l BIGINT)");
        conn.createStatement().execute(
            "CREATE SEQUENCE foo.bar");
        
        nextConnection();
        ResultSet rs = conn.createStatement().executeQuery("EXPLAIN SELECT NEXT VALUE FOR foo.bar FROM foo");
        assertTrue(rs.next());
        conn.createStatement().execute(
                "UPSERT INTO foo VALUES ('a', NEXT VALUE FOR foo.bar)");
        conn.createStatement().execute(
                "UPSERT INTO foo VALUES ('b', NEXT VALUE FOR foo.bar)");
        conn.commit();
        
        nextConnection();
        rs = conn.createStatement().executeQuery("SELECT * FROM foo");
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals(1,rs.getLong(2));
        assertTrue(rs.next());
        assertEquals("b",rs.getString(1));
        assertEquals(2,rs.getLong(2));
        assertFalse(rs.next());
        
        nextConnection();
        PreparedStatement stmt = conn.prepareStatement("SELECT NEXT VALUE FOR foo.bar FROM foo");
        ParameterMetaData md = stmt.getParameterMetaData();
        assertEquals(0,md.getParameterCount());
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(3, rs.getLong(1));
        assertTrue(rs.next());
        assertEquals(4, rs.getLong(1));
        assertFalse(rs.next());
    }

}