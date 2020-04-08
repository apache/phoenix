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

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import net.minidev.json.JSONObject;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.jayway.jsonpath.Option.ALWAYS_RETURN_LIST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JSONParserFunctionIT extends ParallelStatsDisabledIT {

    @Test
    public void testBasicJSONParseFunction() throws SQLException  {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement()) {
            String tableName = generateUniqueName();
            String tableDdl = "CREATE TABLE " + tableName
                    + "(TENANT_ID VARCHAR(15) NOT NULL, K VARCHAR NOT NULL,  V VARCHAR "
                    + "CONSTRAINT PK PRIMARY KEY (TENANT_ID, K))";
            stmt.execute(tableDdl);

            String upsertData = "UPSERT INTO " + tableName + " VALUES (?, ?, ?)";
            try (PreparedStatement prepStmt = conn.prepareStatement(upsertData)) {
                prepStmt.setString(1, "000tenant1");
                prepStmt.setString(2, getJsonByteArray());
                prepStmt.setNull(3, Types.VARCHAR);
                prepStmt.executeUpdate();
            }
            conn.commit();
            String jsonPathExprStr = "'$.store.book[*].author'";
            String expectedMatch = "[\"Nigel Rees\",\"Evelyn Waugh\","
                    + "\"Herman Melville\",\"J. R. R. Tolkien\"]";
            ResultSet rs = stmt.executeQuery(
                    "SELECT JSON_PARSER(K, " + jsonPathExprStr + ") FROM " + tableName);
            assertTrue(rs.next());
            assertEquals(expectedMatch, rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testInvalidJSON() throws SQLException {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement()) {
            String tableName = generateUniqueName();
            String tableDdl = "CREATE TABLE " + tableName
                    + "(TENANT_ID VARCHAR(15) NOT NULL, K VARCHAR NOT NULL,  V VARCHAR "
                    + "CONSTRAINT PK PRIMARY KEY (TENANT_ID, K))";
            stmt.execute(tableDdl);

            String upsertData = "UPSERT INTO " + tableName + " VALUES (?, ?, ?)";
            try (PreparedStatement prepStmt = conn.prepareStatement(upsertData)) {
                prepStmt.setString(1, "000tenant2");
                prepStmt.setString(2, "not a json");
                prepStmt.setNull(3, Types.VARCHAR);
                prepStmt.executeUpdate();
            }
            conn.commit();
            String jsonPathExprStr = "'$.store.book[*].author'";
            ResultSet rs = stmt.executeQuery(
                    "SELECT JSON_PARSER(K, " + jsonPathExprStr + ") FROM " + tableName);
            assertTrue(rs.next());
            try {
                rs.getString(1);
                fail();
            } catch (PathNotFoundException ignore) {
                // expected
            }
        }
    }

    @Test
    public void testJsonPathParsing() {
        Configuration conf = Configuration.defaultConfiguration();
        conf.addOptions(ALWAYS_RETURN_LIST);
        String j = getJsonByteArray();
        JsonPath.using(conf).parse(j).read(getUnescapedJsonPathExprStr());
    }

    private String getUnescapedJsonPathExprStr() {
        return "$.store.book[*].author";
    }

    private String getJsonByteArray() {
        JSONObject json = new JSONObject();

        Map<String, Object> storeMap = new HashMap<>();
        List<Map<String, Object>> bookList = new ArrayList<>();
        Map<String, Object> book1 = new HashMap<>();
        book1.put("category", "reference");
        book1.put("author", "Nigel Rees");
        book1.put("title", "Sayings of the Century");
        book1.put("price", 8.95);
        bookList.add(book1);

        Map<String, Object> book2 = new HashMap<>();
        book2.put("category", "fiction");
        book2.put("author", "Evelyn Waugh");
        book2.put("title", "Sword of Honour");
        book2.put("price", 12.99);
        bookList.add(book2);

        Map<String, Object> book3 = new HashMap<>();
        book3.put("category", "fiction");
        book3.put("author", "Herman Melville");
        book3.put("title", "Moby Dick");
        book3.put("isbn", "0-553-21311-3");
        book3.put("price", 8.99);
        bookList.add(book3);

        Map<String, Object> book4 = new HashMap<>();
        book4.put("category", "fiction");
        book4.put("author", "J. R. R. Tolkien");
        book4.put("title", "The Lord of the Rings");
        book4.put("isbn", "0-395-19395-8");
        book4.put("price", 22.99);
        bookList.add(book4);

        storeMap.put("book", bookList);
        Map<String, Object> bicycleMap = new HashMap<>();
        bicycleMap.put("color", "red");
        bicycleMap.put("price", 19.95);
        storeMap.put("bicycle", bicycleMap);

        json.put("store", storeMap);
        json.put("expensive", 10);
        return json.toJSONString();
    }

}
