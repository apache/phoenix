/**
 * 
 */
package org.apache.phoenix.end2end;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.phoenix.expression.function.JsonArrayLengthFunction;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

/**
 * End to end test for {@link JsonArrayLengthFunction}.
 *
 */
public class JsonArrayLengthFunctionIT extends BaseHBaseManagedTimeIT {

	@Test
	public void testJsonArrayLengthWithWhereClause() throws Exception {
		Connection conn = getConnection();
		String json = "[1,2,3]";
		String pk = "valueOne";
		try {
			populateJsonTable(conn, json, pk);

			String selectQuery = "SELECT col1 FROM testJson WHERE json_array_length(col1) = 3";
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
	public void testJsonArrayLengthWhenFirstArgumentIsJsonString()
			throws Exception {
		Connection conn = getConnection();
		String json = "[1,2,true,[\"string\",3]]";
		String pk = "valueOne";
		try {
			populateJsonTable(conn, json, pk);

			String selectQuery = "SELECT json_array_length(col1) FROM testJson WHERE pk = 'valueOne'";
			PreparedStatement stmt = conn.prepareStatement(selectQuery);
			ResultSet rs = stmt.executeQuery();
			assertTrue(rs.next());
			assertEquals("Json data read from DB is not as expected.",
					4, rs.getInt(1));

			assertFalse(rs.next());

		} finally {
			conn.close();
		}
	}

    @Test
    public void testJsonArrayElementsWithWhereClause() throws Exception {
        Connection conn = getConnection();
        String json = "[1,2,3]";
        String pk = "valueOne";
        try {
            populateJsonTable(conn, json, pk);

            String selectQuery = "SELECT col1 FROM testJson WHERE ARRAY_ELEM(json_array_elements(col1),2) = 2";
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
