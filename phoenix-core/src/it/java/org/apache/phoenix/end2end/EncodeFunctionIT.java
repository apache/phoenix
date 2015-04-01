/**
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
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
import java.util.Collections;
import java.util.List;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;

import com.google.common.collect.Lists;

public class EncodeFunctionIT extends BaseHBaseManagedTimeIT {

    /**
     * Helper to test ENCODE function
     * 
     * @param conn
     *            connection to be used
     * @param colName
     *            name of column to query
     * @param sortOrder
     *            sort order of the pk column
     * @param expectedOutput
     *            expected output of ENCODE function
     */
    private void testEncodeHelper(Connection conn, String colName, List<String> expectedOutputList, String sortOrder)
        throws Exception {
        for (int id = 0; id < expectedOutputList.size(); ++id) {
            String sql = String.format("SELECT ENCODE(%s, 'base62') FROM TEST_TABLE_%s WHERE id=?", colName, sortOrder);
            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.setInt(1, id);

            ResultSet rs = stmt.executeQuery();
            assertTrue("Expected exactly one row to be returned ", rs.next());
            assertEquals("ENCODE returned incorrect result ", expectedOutputList.get(id), rs.getString(1));
            assertFalse("Expected exactly one row to be returned ", rs.next());
        }
    }

    /**
     * Helper to test ENCODE function
     * 
     * @param conn
     *            connection to phoenix
     * @param inputList
     *            list of values to test
     * @param expectedOutputList
     *            expected output of ENCODE function
     */
    private void testEncode(Connection conn, List<Object> inputList, List<String> expectedOutputList) throws Exception {
        TestUtil.initTables(conn, "BIGINT", inputList);
        testEncodeHelper(conn, "pk", expectedOutputList, "ASC");
        testEncodeHelper(conn, "pk", expectedOutputList, "DESC");
    }

    @Test
    public void testEncode() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        testEncode(conn, Lists.<Object> newArrayList(Long.MAX_VALUE, 62, 10, 1, 0, -1, -10, -62, Long.MIN_VALUE),
            Lists.newArrayList("AzL8n0Y58m7", "10", "A", "1", "0", "-1", "-A", "-10", "-AzL8n0Y58m8"));
    }

    @Test
    public void testEncodeNullInput() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        TestUtil.initTables(conn, "BIGINT", Collections.<Object> singletonList(0l));
        testEncodeHelper(conn, "kv", Collections.<String> singletonList(null), "ASC");
        testEncodeHelper(conn, "kv", Collections.<String> singletonList(null), "DESC");
    }

    @Test
    public void testUpperCaseEncodingType() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE TEST_TABLE ( pk VARCHAR(10) NOT NULL CONSTRAINT PK PRIMARY KEY (pk))";

        conn.createStatement().execute(ddl);
        PreparedStatement ps = conn.prepareStatement("UPSERT INTO TEST_TABLE (pk) VALUES (?)");
        ps.setString(1, "1");

        ps.execute();
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM TEST_TABLE WHERE pk = ENCODE(1, 'BASE62')");
        assertTrue(rs.next());
    }

    @Test
    public void testNullEncodingType() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE TEST_TABLE ( pk VARCHAR(10) NOT NULL CONSTRAINT PK PRIMARY KEY (pk))";
        conn.createStatement().execute(ddl);

        try {
            conn.createStatement().executeQuery("SELECT * FROM TEST_TABLE WHERE pk = ENCODE(1, NULL)");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.ILLEGAL_DATA.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testUnsupportedEncodingType() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE TEST_TABLE ( pk VARCHAR(10) NOT NULL CONSTRAINT PK PRIMARY KEY (pk))";
        conn.createStatement().execute(ddl);

        try {
            conn.createStatement().executeQuery("SELECT * FROM TEST_TABLE WHERE pk = ENCODE(1, 'HEX')");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.ILLEGAL_DATA.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testInvalidEncodingType() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl =
            "CREATE TABLE test_table ( some_column BINARY(12) NOT NULL CONSTRAINT PK PRIMARY KEY (some_column))";
        conn.createStatement().execute(ddl);

        try {
            conn.createStatement().executeQuery(
                "SELECT * FROM test_table WHERE some_column = ENCODE(1, 'invalidEncodingFormat')");
            fail();
        } catch (SQLException e) {
        }
    }

}
