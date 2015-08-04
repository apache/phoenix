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

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.*;

/**
 * End to end test for {@link org.apache.phoenix.expression.function.JsonArrayElementsFunction}.
 *
 */
public class JsonArrayElementsFunctionIT extends BaseHBaseManagedTimeIT {

    @Test
    public void testJsonArrayElementsWithSameType() throws Exception {
        Connection conn = getConnection();

        try {
            String json = "[25.343,36.763,37.56,386.63]";
            String pk = "valueOne";

            populateJsonTable(conn, json, pk);

            String selectQuery = "SELECT json_array_elements(col1) FROM testJson WHERE pk = 'valueOne'";
            PreparedStatement stmt = conn.prepareStatement(selectQuery);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            String[] strArr = new String[4];
            strArr[0] = "25.343";
            strArr[1] = "36.763";
            strArr[2] = "37.56";
            strArr[3] = "386.63";
            Array array = conn.createArrayOf("VARCHAR", strArr);
            PhoenixArray resultArray = (PhoenixArray) rs.getArray(1);
            assertEquals("Json array elements is not as expected.", resultArray,
                    array);
            assertFalse(rs.next());

        } finally {
            conn.close();
        }
    }
    @Test
    public void testJsonArrayElementsWithDifferentDataTypes() throws Exception {
        Connection conn = getConnection();

        try {
            String json = "[1,36.763,false,\"string\"]";
            String pk = "valueOne";

            populateJsonTable(conn, json, pk);

            String selectQuery = "SELECT json_array_elements(col1) FROM testJson WHERE pk = 'valueOne'";
            PreparedStatement stmt = conn.prepareStatement(selectQuery);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            String[] strArr = new String[4];
            strArr[0] = "1";
            strArr[1] = "36.763";
            strArr[2] = "false";
            strArr[3] = "\"string\"";

            Array array = conn.createArrayOf("VARCHAR", strArr);
            PhoenixArray resultArray = (PhoenixArray) rs.getArray(1);

            assertEquals("Json array elements is not as expected.", resultArray,
                    array);
            assertFalse(rs.next());

        } finally {
            conn.close();
        }
    }
    @Test
    public void testJsonArrayElementsWithNestJson() throws Exception {
        Connection conn = getConnection();

        try {
            String json = "[1,[1,true,\"string\"]]";
            String pk = "valueOne";

            populateJsonTable(conn, json, pk);

            String selectQuery = "SELECT json_array_elements(col1) FROM testJson WHERE pk = 'valueOne'";
            PreparedStatement stmt = conn.prepareStatement(selectQuery);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            String[] strArr = new String[2];
            strArr[0] = "1";
            strArr[1] = "[1,true,\"string\"]";
            Array array = conn.createArrayOf("VARCHAR", strArr);
            PhoenixArray resultArray = (PhoenixArray) rs.getArray(1);
            assertEquals("Json array elements is not as expected.", resultArray,
                    array);

            assertFalse(rs.next());

        } finally {
            conn.close();
        }
    }

    @Test
    public void testJsonArrayElementsWithInvalidJsonInput() throws Exception {
        Connection conn = getConnection();
        String json = "{\"f1\":1,\"f2\":\"abc\"}";
        String pk = "valueOne";
        try {
            populateJsonTable(conn, json, pk);

            String selectQuery = "SELECT json_array_elements(col1) FROM testJson WHERE pk = 'valueOne'";

            try {
                PreparedStatement stmt = conn.prepareStatement(selectQuery);
                ResultSet rs = stmt.executeQuery();
                assertTrue(rs.next());
                rs.getArray(1);
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
