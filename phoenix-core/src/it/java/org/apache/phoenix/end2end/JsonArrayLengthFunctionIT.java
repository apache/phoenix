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
import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.expression.function.JsonArrayLengthFunction;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

/**
 * End to end test for {@link JsonArrayLengthFunction}.
 *
 */
public class JsonArrayLengthFunctionIT extends BaseHBaseManagedTimeIT {

	@Test
	public void testJsonArrayLengthWithIntTypeInWhereClause() throws Exception {
		Connection conn = getConnection();
		String json = "[1,2,3]";
		String pk = "valueOne";
		try {
			populateJsonTable(conn, json, pk);

			String selectQuery = "SELECT col1 FROM testJson WHERE json_array_length(col1) = 3";
			PreparedStatement stmt = conn.prepareStatement(selectQuery);
			ResultSet rs = stmt.executeQuery();
			assertTrue(rs.next());
			assertEquals("Json data is not as expected.", json,
					rs.getString(1));
			assertFalse(rs.next());

		} finally {
			conn.close();
		}
	}

    @Test
    public void testJsonArrayLengthWithDoubleType() throws Exception {
        Connection conn = getConnection();
        String json = "[1.23,2.34,3.56,54.3]";
        String pk = "valueOne";
        try {
            populateJsonTable(conn, json, pk);

            String selectQuery = "SELECT json_array_length(col1) FROM testJson WHERE pk = 'valueOne'";
            PreparedStatement stmt = conn.prepareStatement(selectQuery);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("Json array length is not as expected.", 4,
                    rs.getInt(1));
            assertFalse(rs.next());

        } finally {
            conn.close();
        }
    }

    @Test
    public void testJsonArrayLengthWithDifferentDataTypes()
            throws Exception {
        Connection conn = getConnection();
        String json = "[1,2.3,null,true,\"f1\",[\"string\",3]]";
        String pk = "valueOne";
        try {
            populateJsonTable(conn, json, pk);

            String selectQuery = "SELECT json_array_length(col1) FROM testJson WHERE pk = 'valueOne'";
            PreparedStatement stmt = conn.prepareStatement(selectQuery);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("Json array length is not as expected.",
                    6, rs.getInt(1));

            assertFalse(rs.next());

        } finally {
            conn.close();
        }
    }
    @Test
    public void testJsonArrayLengthWithNestedJson() throws Exception {
        Connection conn = getConnection();
        String json = "[1,\"string\",false,[1.23,[true,\"ok\"]]]";
        String pk = "valueOne";
        try {
            populateJsonTable(conn, json, pk);

            String selectQuery = "SELECT col1 FROM testJson WHERE json_array_length(col1) = 4";
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
    public void testJsonArrayLengthWithInvalidJsonInput() throws Exception {
        Connection conn = getConnection();
        String json = "{\"f1\":1,\"f2\":\"abc\"}";
        String pk = "valueOne";
        try {
            populateJsonTable(conn, json, pk);

            String selectQuery = "SELECT json_array_length(col1) FROM testJson WHERE pk = 'valueOne'";

            try {
                PreparedStatement stmt = conn.prepareStatement(selectQuery);
                ResultSet rs = stmt.executeQuery();
                assertTrue(rs.next());
                rs.getInt(1);
                fail("The Json Node should be an array!");
            } catch (SQLException sqe) {
                assertEquals("SQL error code is not as expected.",
                        SQLExceptionCode.JSON_NODE_MISMATCH.getErrorCode(), sqe.getErrorCode());
                assertEquals("SQL state is not expected.", "22001",
                         sqe.getSQLState());
            }

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
