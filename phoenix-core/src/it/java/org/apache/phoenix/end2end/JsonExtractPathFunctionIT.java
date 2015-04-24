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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.exception.PhoenixParserException;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.expression.function.JsonExtractPathFunction;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

/**
 * End to end test for {@link JsonExtractPathFunction}.
 *
 */
public class JsonExtractPathFunctionIT extends BaseHBaseManagedTimeIT {

	@Test
	public void testJsonExtractPathWithWhereClause() throws Exception {
		Connection conn = getConnection();
		String json = "{\"k1\":{\"k1_1\":\"value\"},\"k2\":true, \"k3\":2}";
		String pk = "valueOne";
		try {
			populateJsonTable(conn, json, pk);

			String selectQuery = "SELECT col1 FROM testJson WHERE json_extract_path(col1,ARRAY['k1']) = cast ('{\"k1_1\":\"value\"}' as json)";
			PreparedStatement stmt = conn.prepareStatement(selectQuery);
			ResultSet rs = stmt.executeQuery();
			assertTrue(rs.next());
			assertEquals("Json data read from DB is not as expected.", json,
					rs.getString(1));
			assertFalse(rs.next());

		} finally {
			conn.close();
		}
	}

	@Test
	public void testJsonExtractPathWhenFirstArgumentIsJsonString()
			throws Exception {
		Connection conn = getConnection();
		String json = "{\"k1\":{\"k1_1\":\"value\"},\"k2\":true, \"k3\":2}";
		String pk = "valueOne";
		try {
			populateJsonTable(conn, json, pk);

			String selectQuery = "SELECT json_extract_path(" + "'" + json + "'"
					+ ",ARRAY['k2']) FROM testJson WHERE pk = 'valueOne'";
			PreparedStatement stmt = conn.prepareStatement(selectQuery);
			ResultSet rs = stmt.executeQuery();
			assertTrue(rs.next());
			assertEquals("Json data read from DB is not as expected.",
					String.valueOf(true), rs.getString(1));
			assertFalse(rs.next());

		} finally {
			conn.close();
		}
	}

	@Test
	public void testJsonExtractPathForJsonHavingChineseAndControlAndQuoteChars()
			throws Exception {
		Connection conn = getConnection();
		String expectedJsonStr = "\"\\n \\\"jumps \\r'普派'\"";
		String json = "{\"k1\":{\"k1_1\":\"value\"},\"k2\":\"\\n \\\"jumps \\r'普派'\", \"k3\":2}";
		String pk = "valueOne";
		try {
			populateJsonTable(conn, json, pk);

			String selectQuery = "SELECT json_extract_path(col1,ARRAY['k2']) FROM testJson WHERE pk = 'valueOne'";
			PreparedStatement stmt = conn.prepareStatement(selectQuery);
			ResultSet rs = stmt.executeQuery();
			assertTrue(rs.next());
			assertEquals("Json data read from DB is not as expected.",
					expectedJsonStr, rs.getString(1));
			assertFalse(rs.next());

		} finally {
			conn.close();
		}
	}
	
	@Test
	public void testJsonExtractPathForJsonArray() throws Exception {
		Connection conn = getConnection();
		String json = "{\"address\":[{\"first\":{\"street\":\"1st mk st\"}},{\"second\":{\"street\":\"2nd mk st\"}}]}";
		String pk = "valueOne";
		try {
			populateJsonTable(conn, json, pk);

			String selectQuery = "SELECT json_extract_path_text(col1,ARRAY['address','0','first']) FROM testJson WHERE pk = 'valueOne'";
			PreparedStatement stmt = conn.prepareStatement(selectQuery);
			ResultSet rs = stmt.executeQuery();
			assertTrue(rs.next());
			assertEquals("Json data read from DB is not as expected.",
					"{\"street\":\"1st mk st\"}", rs.getString(1));
			assertFalse(rs.next());
			
			selectQuery = "SELECT json_extract_path_text(col1,ARRAY['address','1','second']) FROM testJson WHERE pk = 'valueOne'";
			stmt = conn.prepareStatement(selectQuery);
			rs = stmt.executeQuery();
			assertTrue(rs.next());
			assertEquals("Json data read from DB is not as expected.",
					"{\"street\":\"2nd mk st\"}", rs.getString(1));
			assertFalse(rs.next());

		} finally {
			conn.close();
		}
	}

	@Test
	public void testJsonExtractPath() throws Exception {
		Connection conn = getConnection();
		String json = "{\"k1\":{\"k1_1\":\"value\"},\"k2\":true, \"k3\":2, \"k4\":[\"arr_val0\",\"arr_val1\",\"arr_val2\"]}";
		String pk = "valueOne";
		try {
			populateJsonTable(conn, json, pk);

			String selectQuery = "SELECT json_extract_path(col1,ARRAY['k2']) FROM testJson WHERE pk = 'valueOne'";
			PreparedStatement stmt = conn.prepareStatement(selectQuery);
			ResultSet rs = stmt.executeQuery();
			assertTrue(rs.next());
			assertEquals("Json data read from DB is not as expected.",
					String.valueOf(true), rs.getString(1));
			assertFalse(rs.next());

			selectQuery = "SELECT json_extract_path(col1,ARRAY['k3']) FROM testJson WHERE pk = 'valueOne'";
			stmt = conn.prepareStatement(selectQuery);
			rs = stmt.executeQuery();
			assertTrue(rs.next());
			assertEquals("Json data read from DB is not as expected.",
					String.valueOf(2), rs.getString(1));
			assertFalse(rs.next());
			
			selectQuery = "SELECT json_extract_path(col1,ARRAY['k4','0']) FROM testJson WHERE pk = 'valueOne'";
			stmt = conn.prepareStatement(selectQuery);
			rs = stmt.executeQuery();
			assertTrue(rs.next());
			assertEquals("Json data read from DB is not as expected.",
					"\"arr_val0\"", rs.getString(1));
			assertFalse(rs.next());
			
			selectQuery = "SELECT json_extract_path(col1,ARRAY['k4','1']) FROM testJson WHERE pk = 'valueOne'";
			stmt = conn.prepareStatement(selectQuery);
			rs = stmt.executeQuery();
			assertTrue(rs.next());
			assertEquals("Json data read from DB is not as expected.",
					"\"arr_val1\"", rs.getString(1));
			assertFalse(rs.next());

		} finally {
			conn.close();
		}
	}

	@Test
	public void testJsonExtractPathWhenJsonPathDoesNotExist() throws Exception {
		Connection conn = getConnection();
		String json = "{\"k1\":{\"k1_1\":\"value\"},\"k2\":true, \"k3\":2}";
		String pk = "valueOne";
		try {
			populateJsonTable(conn, json, pk);

			String selectQuery = "SELECT json_extract_path(col1,ARRAY['k4']) FROM testJson WHERE pk = 'valueOne'";
			PreparedStatement stmt = conn.prepareStatement(selectQuery);
			ResultSet rs = stmt.executeQuery();
			assertTrue(rs.next());
			assertNull(rs.getString(1));
			rs.close();

		} finally {
			conn.close();
		}
	}

	@Test
	public void testJsonExtractPathWithEmptyJsonPath() throws Exception {
		Connection conn = getConnection();
		String json = "{\"k1\":{\"k1_1\":\"value\"},\"k2\":true, \"k3\":2}";
		String pk = "valueOne";
		try {
			populateJsonTable(conn, json, pk);

			String selectQuery = "SELECT json_extract_path(col1,ARRAY[]) FROM testJson WHERE pk = 'valueOne'";
			try {
				conn.prepareStatement(selectQuery);
				fail("Phoenix SQL parser should throw exception since json paths is not provided for json_extract_path");
			} catch (PhoenixParserException ppe) {
				assertEquals(SQLExceptionCode.UNWANTED_TOKEN.getErrorCode(),
						ppe.getErrorCode());
			}

		} finally {
			conn.close();
		}
	}

	@Test
	public void testRecursiveJsonExtractPath() throws Exception {
		Connection conn = getConnection();
		String json = "{\"k1\":{\"k1_1\":\"value\"},\"k2\":true, \"k3\":2}";
		String pk = "valueOne";
		try {
			PreparedStatement stmt;
			populateJsonTable(conn, json, pk);

			String selectQuery = "SELECT json_extract_path(json_extract_path(col1,ARRAY['k1']),ARRAY['k1_1']) FROM testJson WHERE pk = 'valueOne'";
			stmt = conn.prepareStatement(selectQuery);
			ResultSet rs = stmt.executeQuery();
			assertTrue(rs.next());
			assertEquals("Json data read from DB is not as expected.",
					"\"value\"" + "", rs.getString(1));
			assertFalse(rs.next());

		} finally {
			conn.close();
		}
	}

	private void populateJsonTable(Connection conn, String json, String pk)
			throws SQLException {
		String ddl = "CREATE TABLE testJson"
				+ "  (pk VARCHAR NOT NULL PRIMARY KEY, " + "col1 json)";
		createTestTable(getUrl(), ddl);

		String query = "UPSERT INTO testJson(pk, col1) VALUES(?,?)";
		PreparedStatement stmt = conn.prepareStatement(query);
		stmt.setString(1, pk);
		stmt.setString(2, json);
		stmt.execute();
		conn.commit();
	}

	private Connection getConnection() throws SQLException {
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
		Connection conn = DriverManager.getConnection(getUrl(), props);
		conn.setAutoCommit(false);
		return conn;
	}
}
