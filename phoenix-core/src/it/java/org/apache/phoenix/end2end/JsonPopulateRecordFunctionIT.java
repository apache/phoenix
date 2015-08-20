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

import org.apache.phoenix.schema.types.PhoenixArray;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.*;

/**
 * End to end test for {@link org.apache.phoenix.expression.function.JsonPopulateRecordFunction}.
 *
 */
public class JsonPopulateRecordFunctionIT extends BaseHBaseManagedTimeIT {

    @Test
    public void testJsonPopulateRecord() throws Exception {
        Connection conn = getConnection();

        try{
            String json = "[{\"a\":1,\"b\":2}]";
            populateJsonTable(conn, json, "valueOne");

            String selectQuery = "SELECT json_populate_record(ARRAY['a','b'],col1) FROM testJson WHERE pk = 'valueOne'";
            PreparedStatement stmt = conn.prepareStatement(selectQuery);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            String expectedstr = "1,2";
            assertEquals("json_populate_record return data is not as expected.", expectedstr,
                    rs.getString(1));
            assertFalse(rs.next());


        } finally {
            conn.close();
        }
    }
    @Test
    public void testJsonPopulateRecordWithNullValue() throws Exception {
        Connection conn = getConnection();

        try{
            String json = "[{\"a\":1,\"c\":2}]";
            populateJsonTable(conn, json, "valueOne");

            String selectQuery = "SELECT json_populate_record(ARRAY['a','b'],col1) FROM testJson WHERE pk = 'valueOne'";
            PreparedStatement stmt = conn.prepareStatement(selectQuery);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            String expectedstr = "1,null";
            assertEquals("json_populate_record return data is not as expected.", expectedstr,
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
