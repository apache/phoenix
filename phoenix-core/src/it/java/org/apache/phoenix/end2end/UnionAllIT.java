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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;


public class UnionAllIT extends BaseOwnClusterHBaseManagedTimeIT {

	@BeforeClass
	public static void doSetup() throws Exception {
		Map<String, String> props = Collections.emptyMap();
		setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
	}

	@Test
	public void testUnionAllSelects() throws Exception {
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
		Connection conn = DriverManager.getConnection(getUrl(), props);
		conn.setAutoCommit(false);

		try {
			String ddl = "CREATE TABLE test_table " +
					"  (a_string varchar not null, col1 integer" +
					"  CONSTRAINT pk PRIMARY KEY (a_string))\n";
			createTestTable(getUrl(), ddl);

			String dml = "UPSERT INTO test_table VALUES(?, ?)";
			PreparedStatement stmt = conn.prepareStatement(dml);
			stmt.setString(1, "a");
			stmt.setInt(2, 10);
			stmt.execute();
			conn.commit();

/*			String query = "SELECT * FROM test_table";
			ResultSet rs = conn.createStatement().executeQuery(query);
			assertTrue(rs.next());
			assertEquals("a",rs.getString(1));
			assertEquals(10,rs.getInt(2));
			assertFalse(rs.next());

			query = "explain SELECT * FROM test_table";
			rs = conn.createStatement().executeQuery(query);
			assertTrue(rs.next());
			assertFalse(rs.next()); */

			ddl = "CREATE TABLE b_table " +
					"  (a_string varchar not null, col1 integer" +
					"  CONSTRAINT pk PRIMARY KEY (a_string))\n";
			createTestTable(getUrl(), ddl);
			dml = "UPSERT INTO b_table VALUES(?, ?)";
			stmt = conn.prepareStatement(dml);
			stmt.setString(1, "b");
			stmt.setInt(2, 20);
			stmt.execute();
			stmt.setString(1, "c");
			stmt.setInt(2, 20);
			stmt.execute();
			conn.commit();
//			ddl = "select count(*), col1 from test_table group by col1 union all select count(*), col1 from b_table group by col1 order by col1";
			ddl = "select count(*) from test_table union all select count(*) from b_table";
//			ddl = "select * from test_table union all select count * from b_table order by a_string, col1";
//			ddl = "select count(*), col1 from test_table group by col1 union all select count(*), col1 from b_table group by col1";
			ResultSet rs = conn.createStatement().executeQuery(ddl);
			assertTrue(rs.next());
			assertEquals(1,rs.getInt(1));
	//		assertEquals(10,rs.getInt(2));
			assertTrue(rs.next());
			assertEquals(2,rs.getInt(1));
	/*		assertEquals(20,rs.getInt(2));
			assertTrue(rs.next());
			assertEquals("c",rs.getString(1));
			assertEquals(20,rs.getInt(2)); */
			assertFalse(rs.next()); 
		} catch (SQLException e) {
			assertEquals(SQLExceptionCode.MISSING_TOKEN.getErrorCode(), e.getErrorCode());
		} finally {
			conn.close();
		}
	}

//	@Test
	public void testAggregate() throws Exception {
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
		Connection conn = DriverManager.getConnection(getUrl(), props);
		conn.setAutoCommit(false);

		try {
			String ddl = "CREATE TABLE test_table " +
					"  (a_string varchar not null, col1 integer" +
					"  CONSTRAINT pk PRIMARY KEY (a_string))\n";
			createTestTable(getUrl(), ddl);

			String dml = "UPSERT INTO test_table VALUES(?, ?)";
			PreparedStatement stmt = conn.prepareStatement(dml);
			stmt.setString(1, "a");
			stmt.setInt(2, 10);
			stmt.execute();
			stmt.setString(1, "d");
            stmt.setInt(2, 40);
            stmt.execute();
            stmt.setString(1, "e");
            stmt.setInt(2, 50);
            stmt.execute();
			conn.commit();

			ddl = "CREATE TABLE b_table " +
					"  (a_string varchar not null, col1 integer" +
					"  CONSTRAINT pk PRIMARY KEY (a_string))\n";
			createTestTable(getUrl(), ddl);
			dml = "UPSERT INTO b_table VALUES(?, ?)";
			stmt = conn.prepareStatement(dml);
			stmt.setString(1, "b");
			stmt.setInt(2, 20);
			stmt.execute();
			stmt.setString(1, "c");
			stmt.setInt(2, 30);
			stmt.execute();
			conn.commit();

		    String aggregate = "select count(*) from test_table union all select count(*) from b_table";
	//		String aggregate = "Select count(*) from (select * from a_table, b_table where a_table.ab=b_table.a_string) x";
//			String aggregate = "Select count(*) from (select * from a_table, b_table where a_table.ab=b_table.a_string) x union all select count(*) from test_table";
			ResultSet rs = conn.createStatement().executeQuery(aggregate);
			assertTrue(rs.next());
			assertEquals(3,rs.getInt(1));
			assertTrue(rs.next());
			assertEquals(2,rs.getInt(1));
			assertFalse(rs.next());  
		} catch (SQLException e) {
			assertEquals(SQLExceptionCode.MISSING_TOKEN.getErrorCode(), e.getErrorCode());
		} finally {
			conn.close();
		}
	}
	
//	@Test
	public void testGroupBy() throws Exception {
	    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
	    Connection conn = DriverManager.getConnection(getUrl(), props);
	    conn.setAutoCommit(false);

	    try {
	        String ddl = "CREATE TABLE test_table " +
	                "  (a_string varchar not null, col1 integer" +
	                "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
	        createTestTable(getUrl(), ddl);

	        String dml = "UPSERT INTO test_table VALUES(?, ?)";
	        PreparedStatement stmt = conn.prepareStatement(dml);
	        stmt.setString(1, "a");
	        stmt.setInt(2, 10);
	        stmt.execute();
	        conn.commit();

	        ddl = "CREATE TABLE b_table " +
	                "  (a_string varchar not null, col1 integer" +
	                "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
	        createTestTable(getUrl(), ddl);
	        dml = "UPSERT INTO b_table VALUES(?, ?)";
	        stmt = conn.prepareStatement(dml);
	        stmt.setString(1, "b");
	        stmt.setInt(2, 20);
	        stmt.execute();
	        stmt.setString(1, "c");
	        stmt.setInt(2, 30);
	        stmt.execute();
	        conn.commit();

	        String aggregate = "select count(*), col1 from test_table group by col1 union all select count(*), col1 from b_table group by col1";
	        ResultSet rs = conn.createStatement().executeQuery(aggregate);
	        assertTrue(rs.next());
	        assertTrue(rs.next());
	        assertFalse(rs.next());  
	    } catch (SQLException e) {
	        assertEquals(SQLExceptionCode.MISSING_TOKEN.getErrorCode(), e.getErrorCode());
	    } finally {
	        conn.close();
	    }
	}
	    
	@Ignore
//	@Test
	public void testSelectDiff() throws Exception {
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
		Connection conn = DriverManager.getConnection(getUrl(), props);
		conn.setAutoCommit(false);

		try {
			String ddl = "CREATE TABLE test_table " +
					"  (a_string varchar not null, col1 integer" +
					"  CONSTRAINT pk PRIMARY KEY (a_string))\n";
			createTestTable(getUrl(), ddl);

			ddl = "CREATE TABLE b_table " +
					"  (a_string varchar not null, col1 integer" +
					"  CONSTRAINT pk PRIMARY KEY (a_string))\n";
			createTestTable(getUrl(), ddl);

			ddl = "select a_string, col1, col1 from test_table union all select * from b_table union all select a_string, col1 from test_table";
			ResultSet rs = conn.createStatement().executeQuery(ddl);
			fail();
		}  catch (SQLException e) {
			assertEquals(SQLExceptionCode.SELECT_COLUMN_NUM_IN_UNIONALL_DIFFS.getErrorCode(), e.getErrorCode());
		} finally {
			conn.close();
		}
	}

	@Ignore
//	@Test
	public void testSelectConstants() throws Exception {
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
		Connection conn = DriverManager.getConnection(getUrl(), props);
		conn.setAutoCommit(false);

		try {
			String ddl = "CREATE TABLE test_table " +
					"  (a_string varchar not null, col1 integer" +
					"  CONSTRAINT pk PRIMARY KEY (a_string))\n";
			createTestTable(getUrl(), ddl);

			ddl = "CREATE TABLE b_table " +
					"  (a_string varchar not null, col1 integer" +
					"  CONSTRAINT pk PRIMARY KEY (a_string))\n";
			createTestTable(getUrl(), ddl);

			ddl = "select a_string, null from test_table"; // union all select * from b_table union all select a_string, col1 from test_table";
			ResultSet rs = conn.createStatement().executeQuery(ddl);
			//	assertTrue(rs.next());
			//	assertTrue(rs.next());
			assertFalse(rs.next()); 
		}  catch (SQLException e) {
			assertEquals(SQLExceptionCode.SELECT_COLUMN_NUM_IN_UNIONALL_DIFFS.getErrorCode(), e.getErrorCode());
		} finally {
			conn.close();
		}
	}
	
//	@Test
	public void testJoinInUnionAll() throws Exception {
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
		Connection conn = DriverManager.getConnection(getUrl(), props);
		conn.setAutoCommit(false);

		try {
			String ddl = "CREATE TABLE test_table " +
					"  (a_string varchar not null, col1 integer" +
					"  CONSTRAINT pk PRIMARY KEY (a_string))\n";
			createTestTable(getUrl(), ddl);

			String dml = "UPSERT INTO test_table VALUES(?, ?)";
			PreparedStatement stmt = conn.prepareStatement(dml);
			stmt.setString(1, "a");
			stmt.setInt(2, 10);
			stmt.execute();
			conn.commit();
			
			ddl = "CREATE TABLE b_table " +
					"  (a_string varchar not null, col1 integer" +
					"  CONSTRAINT pk PRIMARY KEY (a_string))\n";
			createTestTable(getUrl(), ddl);


			dml = "UPSERT INTO b_table VALUES(?, ?)";
			stmt = conn.prepareStatement(dml);
			stmt.setString(1, "a");
			stmt.setInt(2, 20);
			stmt.execute();
			conn.commit();
			
			ddl = "select * from test_table, b_table where test_table.a_string=b_table.a_string";// union all select * from test_table, b_table where test_table.a_string=b_table.a_string"; 
			ResultSet rs = conn.createStatement().executeQuery(ddl);
			assertTrue(rs.next());
			assertEquals("a",rs.getString(1));
			assertEquals(10,rs.getInt(2));
			assertEquals("a",rs.getString(3));
			assertEquals(20,rs.getInt(4));
			assertTrue(rs.next());
			assertEquals("a",rs.getString(1));
			assertEquals(10,rs.getInt(2));
			assertEquals("a",rs.getString(3));
			assertEquals(20,rs.getInt(4));
			assertFalse(rs.next()); 
		}  catch (SQLException e) {
			assertEquals(SQLExceptionCode.SELECT_COLUMN_NUM_IN_UNIONALL_DIFFS.getErrorCode(), e.getErrorCode());
		} finally {
			conn.close();
		}
	}
	

//	@Test
	public void testExplainUnionAll() throws Exception {
	    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
	    Connection conn = DriverManager.getConnection(getUrl(), props);
	    conn.setAutoCommit(false);

	    try {
	        String ddl = "CREATE TABLE test_table " +
	                "  (a_string varchar not null, col1 integer" +
	                "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
	        createTestTable(getUrl(), ddl);

	        String dml = "UPSERT INTO test_table VALUES(?, ?)";
	        PreparedStatement stmt = conn.prepareStatement(dml);
	        stmt.setString(1, "a");
	        stmt.setInt(2, 10);
	        stmt.execute();
	        conn.commit();

	        ddl = "CREATE TABLE b_table " +
	                "  (a_string varchar not null, col1 integer" +
	                "  CONSTRAINT pk PRIMARY KEY (a_string))\n";
	        createTestTable(getUrl(), ddl);


	        dml = "UPSERT INTO b_table VALUES(?, ?)";
	        stmt = conn.prepareStatement(dml);
	        stmt.setString(1, "b");
	        stmt.setInt(2, 20);
	        stmt.execute();
	        conn.commit();

	        ddl = "explain select * from test_table union all select a_string, col1 from b_table order by a_string limit 1";
	        ResultSet rs = conn.createStatement().executeQuery(ddl);
	        assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER TEST_TABLE" + "\n" + "CLIENT PARALLEL 1-WAY FULL SCAN OVER B_TABLE", QueryUtil.getExplainPlan(rs)); 
	    }  catch (SQLException e) {
	        assertEquals(SQLExceptionCode.SELECT_COLUMN_NUM_IN_UNIONALL_DIFFS.getErrorCode(), e.getErrorCode());
	    } finally {
	        conn.close();
	    }
	}
}
