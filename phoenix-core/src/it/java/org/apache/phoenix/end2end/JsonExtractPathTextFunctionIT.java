package org.apache.phoenix.end2end;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.exception.PhoenixParserException;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.expression.function.JsonExtractPathTextFunction;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
/**
 * End to end to test for {@link JsonExtractPathTextFunction}.
 */

public class JsonExtractPathTextFunctionIT extends BaseHBaseManagedTimeIT {
	@Test
	public void testJsonExtractPathTextWithWhereClause() throws Exception {
		Connection conn = getConnection();
		String json = "{\"k1\":{\"k1_1\":\"value\"},\"k2\":true, \"k3\":2}";
		String pk = "valueOne";
		try {
			populateJsonTable(conn, json, pk);

			String selectQuery = "SELECT col1 FROM testJson WHERE json_extract_path_text(col1,ARRAY['k1','k1_1']) = 'value'";
			PreparedStatement stmt = conn.prepareStatement(selectQuery);
			ResultSet rs = stmt.executeQuery();
			assertTrue(rs.next());
			assertEquals("Json data read from DB is not as expected.", json,
					rs.getString(1));
			assertFalse(rs.next());
			
			selectQuery = "SELECT col1 FROM testJson WHERE json_extract_path_text(col1,ARRAY['k1']) = '{\"k1_1\":\"value\"}'";
			stmt = conn.prepareStatement(selectQuery);
			rs = stmt.executeQuery();
			assertTrue(rs.next());
			assertEquals("Json data read from DB is not as expected.", json,
					rs.getString(1));
			assertFalse(rs.next());

		} finally {
			conn.close();
		}
	}

	@Test
	public void testJsonExtractPathTextWhenFirstArgumentIsJsonString()
			throws Exception {
		Connection conn = getConnection();
		String json = "{\"k1\":{\"k1_1\":\"value\"},\"k2\":true, \"k3\":2}";
		String pk = "valueOne";
		try {
			populateJsonTable(conn, json, pk);

			String selectQuery = "SELECT json_extract_path_text(" + "'" + json + "'"
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
	public void testJsonExtractPathTextForJsonHavingChineseAndControlAndQuoteChars()
			throws Exception {
		Connection conn = getConnection();
		String expectedJsonStr = "\n \"jumps \r'普派'";
		String json = "{\"k1\":{\"k1_1\":\"value\"},\"k2\":\"\\n \\\"jumps \\r'普派'\", \"k3\":2}";
		String pk = "valueOne";
		try {
			populateJsonTable(conn, json, pk);

			String selectQuery = "SELECT json_extract_path_text(col1,ARRAY['k2']) FROM testJson WHERE pk = 'valueOne'";
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
	public void testJsonExtractPathTextForJsonArray() throws Exception {
		Connection conn = getConnection();
		String json = "{\"address\":[{\"first\":{\"street\":\"1st mk st\"}},{\"second\":{\"street\":\"2nd mk st\"}}]}";
		String pk = "valueOne";
		try {
			populateJsonTable(conn, json, pk);

			String selectQuery = "SELECT json_extract_path_text(col1,ARRAY['address','0','first','street']) FROM testJson WHERE pk = 'valueOne'";
			PreparedStatement stmt = conn.prepareStatement(selectQuery);
			ResultSet rs = stmt.executeQuery();
			assertTrue(rs.next());
			assertEquals("Json data read from DB is not as expected.",
					"1st mk st", rs.getString(1));
			assertFalse(rs.next());
			
			selectQuery = "SELECT json_extract_path_text(col1,ARRAY['address','1','second','street']) FROM testJson WHERE pk = 'valueOne'";
			stmt = conn.prepareStatement(selectQuery);
			rs = stmt.executeQuery();
			assertTrue(rs.next());
			assertEquals("Json data read from DB is not as expected.",
					"2nd mk st", rs.getString(1));
			assertFalse(rs.next());

		} finally {
			conn.close();
		}
	}
	
	
	@Test
	public void testJsonExtractPathText() throws Exception {
		Connection conn = getConnection();
		String json = "{\"k1\":{\"k1_1\":\"value\"},\"k2\":true, \"k3\":2, \"k4\":[\"arr_val0\",\"arr_val1\",\"arr_val2\"]}";
		String pk = "valueOne";
		try {
			populateJsonTable(conn, json, pk);

			String selectQuery = "SELECT json_extract_path_text(col1,ARRAY['k2']) FROM testJson WHERE pk = 'valueOne'";
			PreparedStatement stmt = conn.prepareStatement(selectQuery);
			ResultSet rs = stmt.executeQuery();
			assertTrue(rs.next());
			assertEquals("Json data read from DB is not as expected.",
					String.valueOf(true), rs.getString(1));
			assertFalse(rs.next());

			selectQuery = "SELECT json_extract_path_text(col1,ARRAY['k3']) FROM testJson WHERE pk = 'valueOne'";
			stmt = conn.prepareStatement(selectQuery);
			rs = stmt.executeQuery();
			assertTrue(rs.next());
			assertEquals("Json data read from DB is not as expected.",
					String.valueOf(2), rs.getString(1));
			assertFalse(rs.next());
			
			selectQuery = "SELECT json_extract_path_text(col1,ARRAY['k1','k1_1']) FROM testJson WHERE pk = 'valueOne'";
			stmt = conn.prepareStatement(selectQuery);
			rs = stmt.executeQuery();
			assertTrue(rs.next());
			assertEquals("Json data read from DB is not as expected.",
					"value", rs.getString(1));
			assertFalse(rs.next());
			
			selectQuery = "SELECT json_extract_path_text(col1,ARRAY['k4','0']) FROM testJson WHERE pk = 'valueOne'";
			stmt = conn.prepareStatement(selectQuery);
			rs = stmt.executeQuery();
			assertTrue(rs.next());
			assertEquals("Json data read from DB is not as expected.",
					"arr_val0", rs.getString(1));
			assertFalse(rs.next());
			
			selectQuery = "SELECT json_extract_path_text(col1,ARRAY['k4','1']) FROM testJson WHERE pk = 'valueOne'";
			stmt = conn.prepareStatement(selectQuery);
			rs = stmt.executeQuery();
			assertTrue(rs.next());
			assertEquals("Json data read from DB is not as expected.",
					"arr_val1", rs.getString(1));
			assertFalse(rs.next());

		} finally {
			conn.close();
		}
	}

	@Test
	public void testJsonExtractPathTextWhenJsonPathDoesNotExist() throws Exception {
		Connection conn = getConnection();
		String json = "{\"k1\":{\"k1_1\":\"value\"},\"k2\":true, \"k3\":2}";
		String pk = "valueOne";
		try {
			populateJsonTable(conn, json, pk);

			String selectQuery = "SELECT json_extract_path_text(col1,ARRAY['k4']) FROM testJson WHERE pk = 'valueOne'";
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
	public void testJsonExtractPathTextWithEmptyJsonPath() throws Exception {
		Connection conn = getConnection();
		String json = "{\"k1\":{\"k1_1\":\"value\"},\"k2\":true, \"k3\":2}";
		String pk = "valueOne";
		try {
			populateJsonTable(conn, json, pk);

			String selectQuery = "SELECT json_extract_path_text(col1,ARRAY[]) FROM testJson WHERE pk = 'valueOne'";
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

			String selectQuery = "SELECT json_extract_path_text(json_extract_path(col1,ARRAY['k1']),ARRAY['k1_1']) FROM testJson WHERE pk = 'valueOne'";
			stmt = conn.prepareStatement(selectQuery);
			ResultSet rs = stmt.executeQuery();
			assertTrue(rs.next());
			assertEquals("Json data read from DB is not as expected.",
					"value" + "", rs.getString(1));
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
