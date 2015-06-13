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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.schema.EqualityNotSupportedException;
import org.apache.phoenix.schema.json.PhoenixJson;
import org.apache.phoenix.schema.types.PJson;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

/**
 * End to end test for JSON data type for {@link PJson} and {@link PhoenixJson}.
 */
public class PhoenixJsonIT extends BaseHBaseManagedTimeIT {

    @Test
    public void testJsonUpsertForJsonHavingChineseAndControlAndQuoteChars() throws Exception {
        String json = "{\"k1\":\"\\n \\\"jumps \\r'普派'\",\"k2\":true, \"k3\":2}";
        String selectQuery = "SELECT col1 FROM testJson WHERE pk = 'valueOne'";
        String pk = "valueOne";
        Connection conn = getConnection();
        try {

            createTableAndUpsertRecord(json, pk, conn);

            PreparedStatement stmt = conn.prepareStatement(selectQuery);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("Json data read from DB is not as expected for query: <" + selectQuery
                    + ">", json, rs.getString(1));
            assertFalse(rs.next());

        } finally {
            conn.close();
        }
    }

    @Test
    public void testJsonUpsertValue() throws Exception {
        String json = "{\"k1\":\"val\",\"k2\":true, \"k3\":2}";
        String selectQuery = "SELECT col1 FROM testJson WHERE pk = 'valueOne'";
        String pk = "valueOne";
        Connection conn = getConnection();
        try {

            createTableAndUpsertRecord(json, pk, conn);

            PreparedStatement stmt = conn.prepareStatement(selectQuery);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("Json data read from DB is not as expected for query: <" + selectQuery
                    + ">", json, rs.getString(1));
            assertFalse(rs.next());

        } finally {
            conn.close();
        }
    }

    @Test
    public void testJsonArrayUpsertValue() throws Exception {
        Connection conn = getConnection();
        try {
            String ddl =
                    "CREATE TABLE testJson" + "  (pk VARCHAR NOT NULL PRIMARY KEY, " + "col1 json)";
            createTestTable(getUrl(), ddl);

            HashMap<String, String> jsonDataMap = new HashMap<String, String>();

            jsonDataMap.put("justIntegerArray", "[1,2,3]");
            jsonDataMap.put("justBooleanArray", "[true,false]");
            jsonDataMap.put("justStringArray", "[\"One\",\"Two\"]");
            jsonDataMap.put("mixedArray", "[\"One\",2, true, null]");
            jsonDataMap.put("arrayInsideAKey", "{\"k1\":{\"k2\":[1,2,3]}}");

            Set<Entry<String, String>> entrySet = jsonDataMap.entrySet();
            for (Entry<String, String> entry : entrySet) {
                createTableAndUpsertRecord(entry.getValue(), entry.getKey(), conn);

                String selectQuery =
                        "SELECT col1 FROM testJson WHERE pk = '" + entry.getKey() + "'";
                PreparedStatement stmt = conn.prepareStatement(selectQuery);
                ResultSet rs = stmt.executeQuery();
                assertTrue(rs.next());
                assertEquals(
                    "Json array data read from DB is not as expected for " + entry.getKey(),
                    entry.getValue(), rs.getString(1));
                assertFalse(rs.next());
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testInvalidJsonUpsertValue() throws Exception {
        String json = "{\"k1\"}";
        Connection conn = getConnection();
        try {
            String ddl =
                    "CREATE TABLE testJson" + "  (pk VARCHAR NOT NULL PRIMARY KEY, " + "col1 json)";
            createTestTable(getUrl(), ddl);

            String query = "UPSERT INTO testJson(pk, col1) VALUES(?,?)";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1, "valueOne");
            stmt.setString(2, json);
            try {
                stmt.execute();
            } catch (SQLException sqe) {
                assertEquals("SQL error code is not as expected when Json is invalid.",
                    SQLExceptionCode.INVALID_JSON_DATA.getErrorCode(), sqe.getErrorCode());
                assertEquals("SQL state is not expected when Json is invalid.", "22000",
                    sqe.getSQLState());
            }
            conn.commit();

        } finally {
            conn.close();
        }
    }

    @Test
    public void testInvalidJsonStringCastAsJson() throws Exception {
        String json = "{\"k1\":\"val\",\"k2\":true, \"k3\":2}";
        String pk = "valueOne";
        String selectQuery = "SELECT cast(pk as json) FROM testJson WHERE pk = 'valueOne'";
        Connection conn = getConnection();
        try {
            createTableAndUpsertRecord(json, pk, conn);

            PreparedStatement stmt = conn.prepareStatement(selectQuery);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(PhoenixJson.class.getName(), rs.getMetaData().getColumnClassName(1));
            try {
                rs.getString(1);
                fail("casting invalid json string to json should fail.");
            } catch (SQLException sqe) {
                assertEquals(SQLExceptionCode.INVALID_JSON_DATA.getErrorCode(), sqe.getErrorCode());
            }

        } finally {
            conn.close();
        }
    }

    private Connection getConnection() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        return conn;
    }

    @Test
    public void testValidJsonStringCastAsJson() throws Exception {
        Connection conn = getConnection();
        String json = "{\"k1\":\"val\",\"k2\":true, \"k3\":2}";
        try {
            createTableAndUpsertRecord(json, json, conn);

            String selectQuery = "SELECT cast(pk as json) FROM testJson";
            PreparedStatement stmt = conn.prepareStatement(selectQuery);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(PhoenixJson.class.getName(), rs.getMetaData().getColumnClassName(1));
            String stringCastToJson = null;
            try {
                stringCastToJson = rs.getString(1);
            } catch (SQLException sqe) {
                fail("casting valid json string to json should not fail.");
            }

            assertEquals(json, stringCastToJson);
            rs.close();
        } finally {
            conn.close();
        }
    }

    @Test
    public void testJsonCastAsString() throws Exception {
        Connection conn = getConnection();
        String json = "{\"k1\":\"val\",\"k2\":true, \"k3\":2}";
        String pk = "valueOne";
        String selectQuery = "SELECT cast(col1 as varchar) FROM testJson WHERE pk = 'valueOne'";
        try {
            createTableAndUpsertRecord(json, pk, conn);

            PreparedStatement stmt = conn.prepareStatement(selectQuery);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertNotEquals(PhoenixJson.class.getName(), rs.getMetaData().getColumnClassName(1));
            assertEquals(String.class.getName(), rs.getMetaData().getColumnClassName(1));
            assertEquals("Json data read from DB is not as expected for query: <" + selectQuery
                    + ">", json, rs.getString(1));
            assertFalse(rs.next());

        } finally {
            conn.close();
        }
    }

    @Test
    public void testJsonAsNull() throws Exception {
        Connection conn = getConnection();
        String json = null;
        String pk = "valueOne";
        try {
            createTableAndUpsertRecord(json, pk, conn);

            /*test is null*/
            String selectQuery = "SELECT col1 FROM testJson WHERE pk = 'valueOne' and col1 is NULL";
            PreparedStatement stmt = conn.prepareStatement(selectQuery);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(PhoenixJson.class.getName(), rs.getMetaData().getColumnClassName(1));
            assertEquals("Json data read from DB is not as expected for query: <" + selectQuery
                    + ">", json, rs.getString(1));

            assertEquals("Json data read from DB is not as expected for query: <" + selectQuery
                    + ">", PhoenixJson.getInstance(json), rs.getObject(1, PhoenixJson.class));
            assertFalse(rs.next());
            
            /*test is not null*/
            json = "[1,2,3]";
            pk = "valueTwo";
            upsertRecord(json, pk, conn);
            selectQuery = "SELECT col1 FROM testJson WHERE col1 is not NULL";
            stmt = conn.prepareStatement(selectQuery);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(PhoenixJson.class.getName(), rs.getMetaData().getColumnClassName(1));
            assertEquals("Json data read from DB is not as expected for query: <" + selectQuery
                    + ">", json, rs.getString(1));
            assertFalse(rs.next());
            
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testCountDistinct() throws Exception {
        final int countDistinct = 11;
        Connection conn = getConnection();
        String json = null;
        String pk = "valueOne";
        String selectQuery = "SELECT DISTINCT_COUNT(col1)  FROM testJson";
        try {
            createTableAndUpsertRecord(json, pk, conn);
            for (int i = 0; i < countDistinct; i++) {
                upsertRecord("[" + i + "]", String.valueOf(i), conn);
            }

            PreparedStatement stmt = conn.prepareStatement(selectQuery);
            stmt.executeQuery();
        } catch (SQLException sqe) {
            assertEquals(SQLExceptionCode.NON_EQUALITY_COMPARISON.getErrorCode(),
                sqe.getErrorCode());
           
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDistinct() throws Exception {
        final int countDistinct = 11;
        Connection conn = getConnection();
        String json = null;
        String pk = "valueOne";
        String selectQuery = "SELECT DISTINCT(col1)  FROM testJson";
        try {
            createTableAndUpsertRecord(json, pk, conn);
            for (int i = 0; i < countDistinct; i++) {
                upsertRecord("[" + i + "]", String.valueOf(i), conn);
            }

            PreparedStatement stmt = conn.prepareStatement(selectQuery);
            stmt.executeQuery();
        } catch (SQLException sqe) {
            assertEquals(SQLExceptionCode.NON_EQUALITY_COMPARISON.getErrorCode(),
                sqe.getErrorCode());
           
        } finally {
            conn.close();
        }
    }

    
    @Test
    public void testJsonColumnInWhereClause() throws Exception {
        Connection conn = getConnection();
        String json = "[1]";
        String pk = "valueOne";
        String selectQuery = "SELECT col1 FROM testJson WHERE pk = 'valueOne' and col1 = '[1]'";
        try {
            createTableAndUpsertRecord(json, pk, conn);

            PreparedStatement stmt = conn.prepareStatement(selectQuery);
            stmt.executeQuery();
            fail("'=' operator should not be allowed with ");
        } catch (SQLException sqe) {
            assertEquals(SQLExceptionCode.NON_EQUALITY_COMPARISON.getErrorCode(),
                sqe.getErrorCode());
           
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testJsonGroupByColumn() throws Exception {
        Connection conn = getConnection();
        String json = "[1]";
        String pk = "valueOne";
        String selectQuery = "SELECT col1 FROM testJson group by col1";
        try {
            createTableAndUpsertRecord(json, pk, conn);

            PreparedStatement stmt = conn.prepareStatement(selectQuery);
            stmt.executeQuery();
        } catch (SQLException sqe) {
            assertEquals(SQLExceptionCode.NON_EQUALITY_COMPARISON.getErrorCode(),
                sqe.getErrorCode());
           
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testJsonColumnInWhereClauseOfSubQuery() throws Exception {
        Connection conn = getConnection();
        String json = "[1]";
        String pk = "valueOne";
        String selectQuery =
                "SELECT col1 FROM testJson WHERE col1 in (select col1 from testJson where col1='[1]')";
        try {
            createTableAndUpsertRecord(json, pk, conn);

            PreparedStatement stmt = conn.prepareStatement(selectQuery);
            stmt.executeQuery();
        } catch (SQLException sqe) {
            assertEquals(SQLExceptionCode.NON_EQUALITY_COMPARISON.getErrorCode(),
                sqe.getErrorCode());

        } finally {
            conn.close();
        }
    }

    
    @Test
    public void testSetObject() throws SQLException {

        Connection conn = getConnection();
        try {
            String json = "{\"k1\":\"val\",\"k2\":true, \"k3\":2}";
            String pk = "valueOne";
            String ddl =
                    "CREATE TABLE testJson" + "  (pk VARCHAR NOT NULL PRIMARY KEY, " + "col1 json)";
            createTestTable(getUrl(), ddl);

            String query = "UPSERT INTO testJson(pk, col1) VALUES(?,?)";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1, pk);
            stmt.setObject(2, PhoenixJson.getInstance(json), java.sql.Types.OTHER);
            stmt.execute();

            pk = "valueTwo";
            query = "UPSERT INTO testJson(pk, col1) VALUES(?,?)";
            stmt = conn.prepareStatement(query);
            stmt.setString(1, pk);
            stmt.setObject(2, json, java.sql.Types.OTHER);
            stmt.execute();

            conn.commit();

            String selectQuery = "SELECT col1 FROM testJson WHERE pk = 'valueOne'";
            stmt = conn.prepareStatement(selectQuery);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("Json data read from DB is not as expected for query: <" + selectQuery
                    + ">", json, rs.getString(1));
            assertFalse(rs.next());

            selectQuery = "SELECT col1 FROM testJson WHERE pk = 'valueTwo'";
            stmt = conn.prepareStatement(selectQuery);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("Json data read from DB is not as expected for query: <" + selectQuery
                    + ">", json, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    private void createTableAndUpsertRecord(String json, String pk, Connection conn) throws SQLException {
        String ddl =
                "CREATE TABLE testJson" + " (pk VARCHAR NOT NULL PRIMARY KEY, " + "col1 json)";
        createTestTable(getUrl(), ddl);

        upsertRecord(json, pk, conn);
    }

    private void upsertRecord(String json, String pk, Connection conn) throws SQLException {
        String query = "UPSERT INTO testJson(pk, col1) VALUES(?,?)";
        PreparedStatement stmt = conn.prepareStatement(query);
        stmt.setString(1, pk);
        stmt.setString(2, json);
        stmt.execute();
        conn.commit();
    }

}
