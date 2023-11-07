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
package org.apache.phoenix.end2end.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.spi.json.GsonJsonProvider;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.phoenix.end2end.IndexToolIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.*;

public class JsonFunctionsIT extends ParallelStatsDisabledIT {
    public static String BASIC_JSON = "json/json_functions_basic.json";
    public static String FUNCTIONS_TEST_JSON = "json/json_functions_tests.json";
    public static String DATA_TYPES_JSON = "json/json_datatypes.json";
    String basicJson = "";
    String dataTypesJson = "";
    String functionsJson = "";

    @Before
    public void setup() throws IOException {
        basicJson = getJsonString(BASIC_JSON, "$[0]");
        dataTypesJson = getJsonString(DATA_TYPES_JSON);
        functionsJson = getJsonString(FUNCTIONS_TEST_JSON);
    }

    @Test
    public void testSimpleJsonValue() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String ddl = "create table " + tableName + " (pk integer primary key, col integer, jsoncol json)";
            conn.createStatement().execute(ddl);
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?,?,?)");
            stmt.setInt(1, 1);
            stmt.setInt(2, 2);
            stmt.setString(3, basicJson);
            stmt.execute();
            conn.commit();
            TestUtil.dumpTable(conn, TableName.valueOf(tableName));

            String queryTemplate ="SELECT JSON_VALUE(jsoncol, '$.type'), JSON_VALUE(jsoncol, '$.info.address.town'), " +
                "JSON_VALUE(jsoncol, '$.info.tags[1]'), JSON_QUERY(jsoncol, '$.info.tags'), JSON_QUERY(jsoncol, '$.info') " +
                " FROM " + tableName +
                " WHERE JSON_VALUE(jsoncol, '$.name') = '%s'";
            String query = String.format(queryTemplate, "AndersenFamily");
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("Basic", rs.getString(1));
            assertEquals("Bristol", rs.getString(2));
            assertEquals("Water polo", rs.getString(3));
            // returned format is different
            compareJson(rs.getString(4), basicJson, "$.info.tags");
            compareJson(rs.getString(5), basicJson, "$.info");
            assertFalse(rs.next());

            // Now check for empty match
            query = String.format(queryTemplate, "Windsors");
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSimpleJsonModify() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            String ddl = "create table " + tableName + " (pk integer primary key, col integer, jsoncol json)";
            conn.createStatement().execute(ddl);
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?,?,?)");
            stmt.setInt(1, 1);
            stmt.setInt(2, 2);
            stmt.setString(3, basicJson);
            stmt.execute();
            conn.commit();

            String upsert ="UPSERT INTO " + tableName + " VALUES(1,2, JSON_MODIFY(jsoncol, '$.info.address.town', '\"Manchester\"')) ";
            conn.createStatement().execute(upsert);
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES(1,2, JSON_MODIFY(jsoncol, '$.info.tags[1]', '\"alto1\"')) ");
            conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES(1,2, JSON_MODIFY(jsoncol, '$.info.tags', '[\"Sport\", \"alto1\", \"Books\"]')) ");
            conn.createStatement().execute("UPSERT INTO " + tableName + " SELECT pk, col, JSON_MODIFY(jsoncol, '$.info.tags[2]', '\"UpsertSelectVal\"') from " + tableName);

            String queryTemplate ="SELECT JSON_VALUE(jsoncol, '$.type'), JSON_VALUE(jsoncol, '$.info.address.town'), " +
                "JSON_VALUE(jsoncol, '$.info.tags[1]'), JSON_QUERY(jsoncol, '$.info.tags'), JSON_QUERY(jsoncol, '$.info'), " +
                "JSON_VALUE(jsoncol, '$.info.tags[2]') " +
                " FROM " + tableName +
                " WHERE JSON_VALUE(jsoncol, '$.name') = '%s'";
            String query = String.format(queryTemplate, "AndersenFamily");
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("Basic", rs.getString(1));
            assertEquals("Manchester", rs.getString(2));
            assertEquals("alto1", rs.getString(3));
            assertEquals("[\"Sport\", \"alto1\", \"UpsertSelectVal\"]", rs.getString(4));
            assertEquals("{\"type\": 1, \"address\": {\"town\": \"Manchester\", \"county\": \"Avon\", \"country\": \"England\", \"exists\": true}, \"tags\": [\"Sport\", \"alto1\", \"UpsertSelectVal\"]}", rs.getString(5));
            assertEquals("UpsertSelectVal", rs.getString(6));

            // Now check for empty match
            query = String.format(queryTemplate, "Windsors");
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSimpleJsonValue2() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            String ddl = "create table if not exists " + tableName + " (pk integer primary key, col integer, jsoncol json)";
            conn.createStatement().execute(ddl);
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?,?,?)");
            stmt.setInt(1, 1);
            stmt.setInt(2, 2);
            stmt.setString(3, functionsJson);
            stmt.execute();
            conn.commit();
            ResultSet rs = conn.createStatement().executeQuery("SELECT JSON_VALUE(JSONCOL,'$.test'), " +
                    "JSON_VALUE(JSONCOL, '$.testCnt'), " +
                    "JSON_VALUE(JSONCOL, '$.infoTop[5].info.address.state')," +
                    "JSON_VALUE(JSONCOL, '$.infoTop[4].tags[1]'),  " +
                    "JSON_QUERY(JSONCOL, '$.infoTop'), " +
                    "JSON_QUERY(JSONCOL, '$.infoTop[5].info'), " +
                    "JSON_QUERY(JSONCOL, '$.infoTop[5].friends') " +
                    "FROM " + tableName + " WHERE JSON_VALUE(JSONCOL, '$.test')='test1'");
            assertTrue(rs.next());
            assertEquals("test1", rs.getString(1));
            assertEquals("SomeCnt1", rs.getString(2));
            assertEquals("North Dakota", rs.getString(3));
            assertEquals("sint", rs.getString(4));
            compareJson(rs.getString(5), functionsJson, "$.infoTop");
            compareJson(rs.getString(6), functionsJson, "$.infoTop[5].info");
            compareJson(rs.getString(7), functionsJson, "$.infoTop[5].friends");
        }
    }

    private void compareJson(String result, String json, String path) throws JsonProcessingException {
        Configuration conf = Configuration.builder().jsonProvider(new GsonJsonProvider()).build();
        Object read = JsonPath.using(conf).parse(json).read(path);
        ObjectMapper mapper = new ObjectMapper();
        assertEquals(mapper.readTree(read.toString()), mapper.readTree(result));
    }

    @Test
    public void testSimpleJsonDatatypes() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            String ddl = "create table if not exists " + tableName + " (pk integer primary key, col integer, jsoncol json)";
            conn.createStatement().execute(ddl);
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?,?,?)");
            stmt.setInt(1, 1);
            stmt.setInt(2, 2);
            stmt.setString(3, dataTypesJson);
            stmt.execute();
            conn.commit();
            ResultSet rs = conn.createStatement().executeQuery("SELECT JSON_VALUE(JSONCOL,'$.datatypes.stringtype'), " +
                    "JSON_VALUE(JSONCOL, '$.datatypes.inttype'), " +
                    "JSON_VALUE(JSONCOL, '$.datatypes.booltype'), " +
                    "JSON_VALUE(JSONCOL, '$.datatypes.booltypef'), " +
                    "JSON_VALUE(JSONCOL, '$.datatypes.doubletype')," +
                    "JSON_VALUE(JSONCOL, '$.datatypes.longtype')," +
                    "JSON_VALUE(JSONCOL, '$.datatypes.intArray[0]')," +
                    "JSON_VALUE(JSONCOL, '$.datatypes.intArray')," +
                    "JSON_VALUE(JSONCOL, '$')," +
                    "JSON_VALUE(JSONCOL, '$.datatypes.nullcheck')," +
                    "JSON_VALUE(JSONCOL, '$.datatypes.noKey')," +
                    "JSON_VALUE(JSONCOL, '$.datatypes.noKey.subkey')  FROM "
                    + tableName + " WHERE JSON_VALUE(JSONCOL, '$.datatypes.stringtype')='someString'");
            assertTrue(rs.next());
            assertEquals("someString", rs.getString(1));
            assertEquals("1", rs.getString(2));
            assertEquals("true", rs.getString(3));
            assertEquals("false", rs.getString(4));
            assertEquals("2.5", rs.getString(5));
            assertEquals("1490020778457845", rs.getString(6));
            assertEquals("1", rs.getString(7));
            assertEquals(null, rs.getString(8));
            assertEquals(null, rs.getString(9));
            assertEquals(null, rs.getString(10));
            assertEquals(null, rs.getString(11));
            assertEquals(null, rs.getString(12));
        }
    }

    @Test
    public void testJsonQuery() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            String ddl = "create table if not exists " + tableName + " (pk integer primary key, col integer, jsoncol json)";
            conn.createStatement().execute(ddl);
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?,?,?)");
            stmt.setInt(1, 1);
            stmt.setInt(2, 2);
            stmt.setString(3, dataTypesJson);
            stmt.execute();
            conn.commit();
            ResultSet rs = conn.createStatement().executeQuery("SELECT " +
                    "JSON_QUERY(JSONCOL, '$.datatypes.intArray')," +
                    "JSON_QUERY(JSONCOL, '$.datatypes.boolArray')," +
                    "JSON_QUERY(JSONCOL, '$.datatypes.doubleArray')," +
                    "JSON_QUERY(JSONCOL, '$.datatypes.stringArray')," +
                    "JSON_QUERY(JSONCOL, '$.datatypes.mixedArray')  FROM "
                    + tableName + " WHERE JSON_VALUE(JSONCOL, '$.datatypes.stringtype')='someString'");
            assertTrue(rs.next());
            compareJson(rs.getString(1), dataTypesJson, "$.datatypes.intArray");
            compareJson(rs.getString(2), dataTypesJson, "$.datatypes.boolArray");
            compareJson(rs.getString(3), dataTypesJson, "$.datatypes.doubleArray");
            compareJson(rs.getString(4), dataTypesJson, "$.datatypes.stringArray");
            compareJson(rs.getString(5), dataTypesJson, "$.datatypes.mixedArray");
        }
    }

    @Test
    public void testJsonExpressionIndex() throws IOException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            String
                    ddl =
                    "create table if not exists " + tableName +
                            " (pk integer primary key, col integer, jsoncol.jsoncol json)";
            conn.createStatement().execute(ddl);
            conn.createStatement().execute(
                    "UPSERT INTO " + tableName + " (pk, col, jsoncol) VALUES (1,2, '" + basicJson + "')");
            conn.createStatement().execute(
                    "UPSERT INTO " + tableName + " (pk, col, jsoncol) VALUES (2,3, '" + getJsonString(BASIC_JSON, "$[1]") + "')");
            conn.createStatement().execute(
                    "UPSERT INTO " + tableName + " (pk, col, jsoncol) VALUES (3,4, '" + getJsonString(BASIC_JSON, "$[2]") + "')");
            conn.createStatement().execute(
                    "UPSERT INTO " + tableName + " (pk, col, jsoncol) VALUES (4,5, '" + getJsonString(BASIC_JSON, "$[3]") + "')");
            conn.createStatement().execute(
                    "UPSERT INTO " + tableName + " (pk, col, jsoncol) VALUES (5,6, '" + getJsonString(BASIC_JSON, "$[4]") + "')");
            conn.createStatement().execute(
                    "CREATE INDEX " + indexName + " ON " + tableName
                            + " (JSON_VALUE(JSONCOL,'$.type'), JSON_VALUE(JSONCOL,'$.info.address.town')) include (col)");
            String
                    selectSql =
                    "SELECT JSON_VALUE(JSONCOL,'$.type'), " +
                    "JSON_VALUE(JSONCOL,'$.info.address.town') FROM " + tableName +
                    " WHERE JSON_VALUE(JSONCOL,'$.type') = 'Basic'";
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
            String actualExplainPlan = QueryUtil.getExplainPlan(rs);
            IndexToolIT.assertExplainPlan(false, actualExplainPlan, tableName, indexName);
            // Validate the total count of rows
            String countSql = "SELECT COUNT(1) FROM " + tableName;
            rs = conn.createStatement().executeQuery( countSql);
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));
            // Delete the rows
            String deleteQuery = "DELETE FROM " + tableName + " WHERE JSON_VALUE(JSONCOL,'$.type') = 'Normal'";
            boolean deleted = conn.createStatement().execute(deleteQuery);
            rs = conn.createStatement().executeQuery( countSql);
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            // Do a count now for the deleted rows, the count should be 0
            selectSql = "SELECT COUNT(1) FROM " + tableName +
                            " WHERE JSON_VALUE(JSONCOL,'$.type') = 'Normal'";
            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
            // Drop the JSON column
            conn.createStatement().execute("ALTER TABLE " + tableName + " DROP COLUMN jsoncol ");

            // verify the both of the indexes' metadata were dropped
            conn.createStatement().execute("SELECT * FROM " + tableName);
            try {
                conn.createStatement().execute("SELECT * FROM " + indexName);
                fail("Index should have been dropped");
            } catch (TableNotFoundException e) {
            }
            PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
            PTable dataTable = pconn.getTable(new PTableKey(null, tableName));
            pconn = conn.unwrap(PhoenixConnection.class);
            dataTable = pconn.getTable(new PTableKey(null, tableName));
            try {
                pconn.getTable(new PTableKey(null, indexName));
                fail("index should have been dropped");
            } catch (TableNotFoundException e) {
            }
            assertEquals("Unexpected number of indexes ", 0, dataTable.getIndexes().size());
        } catch (SQLException e) {
            assertFalse("Failed to execute test", true);
        }
    }

    @Test
    public void testJsonExpressionIndexInvalid() {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        checkInvalidJsonIndexExpression(props, tableName, indexName,
                " (JSON_QUERY(JSONCOL,'$.info.address')) include (col)");
        checkInvalidJsonIndexExpression(props, tableName, indexName,
                " (JSON_MODIFY(jsoncol, '$.info.tags[2]', '\"newValue\"')) include (col)");
    }

    @Test
    public void testJsonExists() throws SQLException, IOException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String ddl = "create table " + tableName + " (pk integer primary key, col integer, jsoncol json)";
            conn.createStatement().execute(ddl);
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?,?,?)");
            stmt.setInt(1, 1);
            stmt.setInt(2, 2);
            stmt.setString(3, basicJson);
            stmt.execute();
            stmt.setInt(1, 2);
            stmt.setInt(2, 3);
            stmt.setString(3, getJsonString(BASIC_JSON, "$[1]"));
            stmt.execute();
            conn.commit();

            String query ="SELECT JSON_VALUE(jsoncol, '$.type'), JSON_VALUE(jsoncol, '$.info.address.town') " +
                    " FROM " + tableName +
                    " WHERE JSON_EXISTS(jsoncol, '$.info.address.town')";
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("Basic", rs.getString(1));
            assertEquals("Bristol", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("Normal", rs.getString(1));
            assertEquals("Bristol2", rs.getString(2));
            assertFalse(rs.next());

            query ="SELECT JSON_VALUE(jsoncol, '$.type'), JSON_VALUE(jsoncol, '$.info.address.town') " +
                    " FROM " + tableName +
                    " WHERE JSON_EXISTS(jsoncol, '$.info.address.exists')";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("Bristol", rs.getString(2));
            assertFalse(rs.next());

            query ="SELECT JSON_VALUE(jsoncol, '$.type'), JSON_VALUE(jsoncol, '$.info.address.town') " +
                    " FROM " + tableName +
                    " WHERE JSON_EXISTS(jsoncol, '$.info.address.name')";
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());

            query ="SELECT JSON_VALUE(jsoncol, '$.type'), JSON_VALUE(jsoncol, '$.info.address.town') " +
                    " FROM " + tableName +
                    " WHERE JSON_EXISTS(jsoncol, '$.existsFail')";
            rs = conn.createStatement().executeQuery(query);

            query ="SELECT JSON_VALUE(jsoncol, '$.type'), JSON_VALUE(jsoncol, '$.info.address.town') " +
                    " FROM " + tableName +
                    " WHERE JSON_EXISTS(jsoncol, '$.existsFail')";
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());
            query ="SELECT JSON_VALUE(jsoncol, '$.type'), JSON_VALUE(jsoncol, '$.info.address.town') " +
                    " FROM " + tableName +
                    " WHERE JSON_EXISTS(jsoncol, '$.existsFail[*]')";
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());
        }
    }

    private void checkInvalidJsonIndexExpression(Properties props, String tableName,
            String indexName, String indexExpression) {
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            String
                    ddl =
                    "create table if not exists " + tableName + " (pk integer primary key, col integer, jsoncol.jsoncol json)";
            conn.createStatement().execute(ddl);
            conn.createStatement().execute(
                    "UPSERT INTO " + tableName + " (pk, col, jsoncol) VALUES (1,2, '" + basicJson + "')");
            conn.createStatement()
                    .execute("CREATE INDEX " + indexName + " ON " + tableName + indexExpression);
        } catch (SQLException e) {
            assertEquals(
                    SQLExceptionCode.JSON_FRAGMENT_NOT_ALLOWED_IN_INDEX_EXPRESSION.getErrorCode(),
                    e.getErrorCode());
        }
    }

    private static String getJsonString(String jsonFilePath) throws IOException {
        return getJsonString(jsonFilePath, "$");
    }
    private static String getJsonString(String jsonFilePath, String jsonPath) throws IOException {
        URL fileUrl = JsonFunctionsIT.class.getClassLoader().getResource(jsonFilePath);
        String json = FileUtils.readFileToString(new File(fileUrl.getFile()));
        Configuration conf = Configuration.builder().jsonProvider(new GsonJsonProvider()).build();
        Object read = JsonPath.using(conf).parse(json).read(jsonPath);
        return read.toString();
    }
}
