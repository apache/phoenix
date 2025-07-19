/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;
import org.apache.phoenix.util.PropertiesUtil;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests for BSON.
 */
@Category(ParallelStatsDisabledTest.class)
public class Bson1IT extends ParallelStatsDisabledIT {

  @Test
  public void testSimpleMap() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String ddl = "CREATE TABLE " + tableName
        + " (PK1 VARCHAR NOT NULL, PK2 DOUBLE NOT NULL, COL BSON CONSTRAINT pk PRIMARY KEY(PK1, PK2))";
      conn.createStatement().execute(ddl);
      BsonDocument bsonDocument = getDocument1();
      PreparedStatement stmt =
        conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?,?,?)");
      stmt.setString(1, "pk0001");
      stmt.setDouble(2, 123985);
      stmt.setObject(3, bsonDocument);
      stmt.execute();
      stmt.setString(1, "pk0002");
      stmt.setDouble(2, 4596.354);
      stmt.setString(3, bsonDocument.toJson());
      stmt.execute();
      conn.commit();

      String query = "SELECT * FROM " + tableName;
      ResultSet rs = conn.createStatement().executeQuery(query);

      assertTrue(rs.next());
      assertEquals("pk0001", rs.getString(1));
      assertEquals(123985.0, rs.getDouble(2), 0.0);
      BsonDocument document1 = (BsonDocument) rs.getObject(3);
      assertEquals(bsonDocument, document1);

      String documentAsString = rs.getString(3);
      assertEquals(bsonDocument.toJson(), documentAsString);

      assertTrue(rs.next());
      assertEquals("pk0002", rs.getString(1));
      assertEquals(4596.354, rs.getDouble(2), 0.0);
      BsonDocument document2 = (BsonDocument) rs.getObject(3);
      assertEquals(bsonDocument, document2);

      documentAsString = rs.getString(3);
      assertEquals(bsonDocument.toJson(), documentAsString);

      assertFalse(rs.next());
    }
  }

  @Test
  public void testMapWithConditionExpressions() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String ddl = "CREATE TABLE " + tableName
        + " (PK1 VARCHAR NOT NULL, PK2 DOUBLE NOT NULL, COL BSON CONSTRAINT pk PRIMARY KEY(PK1, PK2))";
      conn.createStatement().execute(ddl);
      BsonDocument bsonDocument1 = getDocument1();
      BsonDocument bsonDocument2 = getDocument2();
      BsonDocument compareValuesDocument = getCompareValueDocument();
      PreparedStatement stmt =
        conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?,?,?)");
      stmt.setString(1, "pk0001");
      stmt.setDouble(2, 123985);
      stmt.setObject(3, bsonDocument1);
      stmt.execute();
      stmt.setString(1, "pk0002");
      stmt.setDouble(2, 4596.354);
      stmt.setObject(3, bsonDocument2);
      stmt.execute();
      conn.commit();

      String conditionExpression =
        "(field_exists(Id) OR field_exists(attr_3)) AND field_exists(attr_6) AND field_exists(attr_6.n_attr_1)";

      BsonDocument conditionDoc = new BsonDocument();
      conditionDoc.put("$EXPR", new BsonString(conditionExpression));
      conditionDoc.put("$VAL", compareValuesDocument);

      String query = "SELECT * FROM " + tableName + " WHERE BSON_CONDITION_EXPRESSION(COL, '"
        + conditionDoc.toJson() + "')";
      ResultSet rs = conn.createStatement().executeQuery(query);

      assertTrue(rs.next());
      assertEquals("pk0001", rs.getString(1));
      assertEquals(123985.0, rs.getDouble(2), 0.0);
      BsonDocument document1 = (BsonDocument) rs.getObject(3);
      assertEquals(bsonDocument1, document1);

      assertTrue(rs.next());
      assertEquals("pk0002", rs.getString(1));
      assertEquals(4596.354, rs.getDouble(2), 0.0);
      BsonDocument document2 = (BsonDocument) rs.getObject(3);
      assertEquals(bsonDocument2, document2);

      assertFalse(rs.next());

      conditionExpression =
        "(field_exists(Id) OR field_exists(attr_3)) AND field_exists(attr_6) AND field_exists(attr_6.n_attr_10)";

      conditionDoc = new BsonDocument();
      conditionDoc.put("$EXPR", new BsonString(conditionExpression));
      conditionDoc.put("$VAL", compareValuesDocument);

      query = "SELECT * FROM " + tableName + " WHERE BSON_CONDITION_EXPRESSION(COL, '"
        + conditionDoc.toJson() + "')";
      rs = conn.createStatement().executeQuery(query);

      assertFalse(rs.next());

      conditionExpression = "(field_exists(ISBN))";

      conditionDoc = new BsonDocument();
      conditionDoc.put("$EXPR", new BsonString(conditionExpression));
      conditionDoc.put("$VAL", compareValuesDocument);

      query = "SELECT * FROM " + tableName + " WHERE BSON_CONDITION_EXPRESSION(COL, '"
        + conditionDoc.toJson() + "')";
      rs = conn.createStatement().executeQuery(query);

      assertTrue(rs.next());

      assertEquals("pk0002", rs.getString(1));
      assertEquals(4596.354, rs.getDouble(2), 0.0);
      document2 = (BsonDocument) rs.getObject(3);
      assertEquals(bsonDocument2, document2);

      assertFalse(rs.next());

      conditionExpression =
        "NestedList1[0] <= :NestedList1_485 AND NestedList1[2][0] >= :NestedList1_xyz0123 "
          + "AND NestedList1[2][1].Id < :Id1 AND IdS < :Ids1 AND Id2 > :Id2";

      conditionDoc = new BsonDocument();
      conditionDoc.put("$EXPR", new BsonString(conditionExpression));
      conditionDoc.put("$VAL", compareValuesDocument);

      query = "SELECT * FROM " + tableName + " WHERE BSON_CONDITION_EXPRESSION(COL, '"
        + conditionDoc.toJson() + "')";
      rs = conn.createStatement().executeQuery(query);

      assertTrue(rs.next());
      assertEquals("pk0002", rs.getString(1));
      assertEquals(4596.354, rs.getDouble(2), 0.0);
      document2 = (BsonDocument) rs.getObject(3);
      assertEquals(bsonDocument2, document2);

      assertFalse(rs.next());

      conditionExpression =
        "NestedList1[0] <= :NestedList1_485 AND NestedList1[2][0] >= :NestedList1_xyz0123 "
          + "AND NestedList1[2][1].Id < :Id1 AND IdS < :Ids1 AND Id2 > :Id2 "
          + "AND begins_with(Title, :TitlePrefix)";

      conditionDoc = new BsonDocument();
      conditionDoc.put("$EXPR", new BsonString(conditionExpression));
      conditionDoc.put("$VAL", compareValuesDocument);

      query = "SELECT * FROM " + tableName + " WHERE BSON_CONDITION_EXPRESSION(COL, '"
        + conditionDoc.toJson() + "')";
      rs = conn.createStatement().executeQuery(query);

      assertTrue(rs.next());
      assertEquals("pk0002", rs.getString(1));
      assertEquals(4596.354, rs.getDouble(2), 0.0);
      document2 = (BsonDocument) rs.getObject(3);
      assertEquals(bsonDocument2, document2);

      assertFalse(rs.next());

      conditionExpression = "begins_with(Title, :TitlePrefix) AND contains(#attr_5, :Attr5Value) "
        + "AND contains(#0, :NestedList1String)";

      conditionDoc = new BsonDocument();
      conditionDoc.put("$EXPR", new BsonString(conditionExpression));
      conditionDoc.put("$VAL", compareValuesDocument);
      BsonDocument keyDoc = new BsonDocument();
      keyDoc.put("#attr_5", new BsonString("attr_5"));
      keyDoc.put("#0", new BsonString("NestedList1"));
      conditionDoc.put("$KEYS", keyDoc);

      query = "SELECT * FROM " + tableName + " WHERE BSON_CONDITION_EXPRESSION(COL, '"
        + conditionDoc.toJson() + "')";
      rs = conn.createStatement().executeQuery(query);

      assertTrue(rs.next());
      assertEquals("pk0002", rs.getString(1));
      assertEquals(4596.354, rs.getDouble(2), 0.0);
      document2 = (BsonDocument) rs.getObject(3);
      assertEquals(bsonDocument2, document2);

      assertFalse(rs.next());

      conditionExpression =
        "contains(attr_5, :NonExistentValue) OR begins_with(Title, :TitlePrefix)";

      conditionDoc = new BsonDocument();
      conditionDoc.put("$EXPR", new BsonString(conditionExpression));
      conditionDoc.put("$VAL", compareValuesDocument);

      query = "SELECT * FROM " + tableName + " WHERE BSON_CONDITION_EXPRESSION(COL, '"
        + conditionDoc.toJson() + "')";
      rs = conn.createStatement().executeQuery(query);

      assertTrue(rs.next());
      assertEquals("pk0002", rs.getString(1));
      assertEquals(4596.354, rs.getDouble(2), 0.0);
      document2 = (BsonDocument) rs.getObject(3);
      assertEquals(bsonDocument2, document2);

      assertFalse(rs.next());

      conditionExpression = "field_type(#attr_5, :L)";
      conditionDoc = new BsonDocument();
      conditionDoc.put("$EXPR", new BsonString(conditionExpression));
      conditionDoc.put("$VAL", compareValuesDocument);
      keyDoc = new BsonDocument();
      keyDoc.put("#attr_5", new BsonString("attr_5"));
      conditionDoc.put("$KEYS", keyDoc);
      query = "SELECT * FROM " + tableName + " WHERE BSON_CONDITION_EXPRESSION(COL, '"
              + conditionDoc.toJson() + "')";
      rs = conn.createStatement().executeQuery(query);
      assertTrue(rs.next());
      assertTrue(rs.next());

      conditionExpression = "attribute_type(attr_5, :NS)";
      conditionDoc = new BsonDocument();
      conditionDoc.put("$EXPR", new BsonString(conditionExpression));
      conditionDoc.put("$VAL", compareValuesDocument);
      query = "SELECT * FROM " + tableName + " WHERE BSON_CONDITION_EXPRESSION(COL, '"
              + conditionDoc.toJson() + "')";
      rs = conn.createStatement().executeQuery(query);
      assertFalse(rs.next());

      conditionExpression = "size(#Title) > :size3";

      conditionDoc = new BsonDocument();
      conditionDoc.put("$EXPR", new BsonString(conditionExpression));
      conditionDoc.put("$VAL", compareValuesDocument);
      keyDoc = new BsonDocument();
      keyDoc.put("#Title", new BsonString("Title"));
      conditionDoc.put("$KEYS", keyDoc);

      query = "SELECT * FROM " + tableName + " WHERE BSON_CONDITION_EXPRESSION(COL, '"
              + conditionDoc.toJson() + "')";
      rs = conn.createStatement().executeQuery(query);
      assertTrue(rs.next());
      assertEquals("pk0002", rs.getString(1));
      assertFalse(rs.next());
    }
  }

  private static RawBsonDocument getCompareValueDocument() {
    String json =
      "{\n" + "  \":NestedList1_485\" : -485.33,\n" + "  \":ISBN\" : \"111-1111111111\",\n"
        + "  \":Title\" : \"Book 101 Title\",\n" + "  \":TitlePrefix\" : \"Book \",\n"
        + "  \":Id\" : 101.01,\n" + "  \":Id2\" : 12,\n" + "  \":Id1\" : 120,\n"
        + "  \":Ids1\" : \"12\",\n" + "  \":NMap1_NList1\" : \"NListVal01\",\n"
        + "  \":InPublication\" : false,\n" + "  \":NestedList1_xyz0123\" : \"xyz0123\",\n"
        + "  \":Attr5Value\" : \"str001\",\n" + "  \":NestedList1String\" : \"1234abcd\",\n"
        + "  \":NonExistentValue\" : \"does_not_exist\"\n" + "  \":L\" : \"L\"\n"
        + "  \":NS\" : \"NS\"\n" + "  \":size3\" : 3\n" + "}";

    return RawBsonDocument.parse(json);
  }

  private static RawBsonDocument getDocument2() {
    String json = "{\n" + "  \"InPublication\" : false,\n" + "  \"ISBN\" : \"111-1111111111\",\n"
      + "  \"NestedList1\" : [ -485.34, \"1234abcd\", [ \"xyz0123\", {\n"
      + "    \"InPublication\" : false,\n" + "    \"ISBN\" : \"111-1111111111\",\n"
      + "    \"Title\" : \"Book 101 Title\",\n" + "    \"Id\" : 101.01\n" + "  } ] ],\n"
      + "  \"NestedMap1\" : {\n" + "    \"InPublication\" : false,\n"
      + "    \"ISBN\" : \"111-1111111111\",\n" + "    \"Title\" : \"Book 101 Title\",\n"
      + "    \"Id\" : 101.01,\n" + "    \"NList1\" : [ \"NListVal01\", -23.4 ]\n" + "  },\n"
      + "  \"Id2\" : 101.01,\n" + "  \"attr_6\" : {\n" + "    \"n_attr_0\" : \"str_val_0\",\n"
      + "    \"n_attr_1\" : 1295.03,\n" + "    \"n_attr_2\" : {\n" + "      \"$binary\" : {\n"
      + "        \"base64\" : \"MjA0OHU1bmJsd2plaVdGR1RIKDRiZjkzMA==\",\n"
      + "        \"subType\" : \"00\"\n" + "      }\n" + "    },\n" + "    \"n_attr_3\" : true,\n"
      + "    \"n_attr_4\" : null\n" + "  },\n" + "  \"attr_5\" : [ 1234, \"str001\", {\n"
      + "    \"$binary\" : {\n" + "      \"base64\" : \"AAECAwQF\",\n"
      + "      \"subType\" : \"00\"\n" + "    }\n" + "  } ],\n" + "  \"IdS\" : \"101.01\",\n"
      + "  \"Title\" : \"Book 101 Title\",\n" + "  \"Id\" : 101.01,\n" + "  \"attr_1\" : 1295.03,\n"
      + "  \"attr_0\" : \"str_val_0\"\n" + "}";
    // {
    // "InPublication" : false,
    // "ISBN" : "111-1111111111",
    // "NestedList1" : [ -485.34, "1234abcd", [ "xyz0123", {
    // "InPublication" : false,
    // "ISBN" : "111-1111111111",
    // "Title" : "Book 101 Title",
    // "Id" : 101.01
    // } ] ],
    // "NestedMap1" : {
    // "InPublication" : false,
    // "ISBN" : "111-1111111111",
    // "Title" : "Book 101 Title",
    // "Id" : 101.01,
    // "NList1" : [ "NListVal01", -23.4 ]
    // },
    // "Id2" : 101.01,
    // "attr_6" : {
    // "n_attr_0" : "str_val_0",
    // "n_attr_1" : 1295.03,
    // "n_attr_2" : {
    // "$binary" : {
    // "base64" : "MjA0OHU1bmJsd2plaVdGR1RIKDRiZjkzMA==",
    // "subType" : "00"
    // }
    // },
    // "n_attr_3" : true,
    // "n_attr_4" : null
    // },
    // "attr_5" : [ 1234, "str001", {
    // "$binary" : {
    // "base64" : "AAECAwQF",
    // "subType" : "00"
    // }
    // } ],
    // "IdS" : "101.01",
    // "Title" : "Book 101 Title",
    // "Id" : 101.01,
    // "attr_1" : 1295.03,
    // "attr_0" : "str_val_0"
    // }
    return RawBsonDocument.parse(json);
  }

  private static RawBsonDocument getDocument1() {
    String json = "{\n" + "  \"attr_9\" : {\n" + "    \"$set\" : [ {\n" + "      \"$binary\" : {\n"
      + "        \"base64\" : \"YWJjZA==\",\n" + "        \"subType\" : \"00\"\n" + "      }\n"
      + "    }, {\n" + "      \"$binary\" : {\n" + "        \"base64\" : \"c3RyaW5nXzAyMDM=\",\n"
      + "        \"subType\" : \"00\"\n" + "      }\n" + "    } ]\n" + "  },\n"
      + "  \"attr_8\" : {\n" + "    \"$set\" : [ 3802.34, -40.667, -4839, 7593 ]\n" + "  },\n"
      + "  \"attr_7\" : {\n" + "    \"$set\" : [ \"str_set002\", \"strset003\", \"strset001\" ]\n"
      + "  },\n" + "  \"attr_6\" : {\n" + "    \"n_attr_0\" : \"str_val_0\",\n"
      + "    \"n_attr_1\" : 1295.03,\n" + "    \"n_attr_2\" : {\n" + "      \"$binary\" : {\n"
      + "        \"base64\" : \"MjA0OHU1bmJsd2plaVdGR1RIKDRiZjkzMA==\",\n"
      + "        \"subType\" : \"00\"\n" + "      }\n" + "    },\n" + "    \"n_attr_3\" : true,\n"
      + "    \"n_attr_4\" : null\n" + "  },\n" + "  \"attr_5\" : [ 1234, \"str001\", {\n"
      + "    \"$binary\" : {\n" + "      \"base64\" : \"AAECAwQF\",\n"
      + "      \"subType\" : \"00\"\n" + "    }\n" + "  } ],\n" + "  \"attr_4\" : null,\n"
      + "  \"attr_3\" : true,\n" + "  \"attr_2\" : {\n" + "    \"$binary\" : {\n"
      + "      \"base64\" : \"cmFuZG9tZTkzaDVvbmVmaHUxbmtyXzE5MzBga2p2LSwhJCVeaWVpZmhiajAzNA==\",\n"
      + "      \"subType\" : \"00\"\n" + "    }\n" + "  },\n" + "  \"attr_1\" : 1295.03,\n"
      + "  \"attr_0\" : \"str_val_0\"\n" + "}";
    // {
    // "attr_9" : {
    // "$set" : [ {
    // "$binary" : {
    // "base64" : "YWJjZA==",
    // "subType" : "00"
    // }
    // }, {
    // "$binary" : {
    // "base64" : "c3RyaW5nXzAyMDM=",
    // "subType" : "00"
    // }
    // } ]
    // },
    // "attr_8" : {
    // "$set" : [ 3802.34, -40.667, -4839, 7593 ]
    // },
    // "attr_7" : {
    // "$set" : [ "str_set002", "strset003", "strset001" ]
    // },
    // "attr_6" : {
    // "n_attr_0" : "str_val_0",
    // "n_attr_1" : 1295.03,
    // "n_attr_2" : {
    // "$binary" : {
    // "base64" : "MjA0OHU1bmJsd2plaVdGR1RIKDRiZjkzMA==",
    // "subType" : "00"
    // }
    // },
    // "n_attr_3" : true,
    // "n_attr_4" : null
    // },
    // "attr_5" : [ 1234, "str001", {
    // "$binary" : {
    // "base64" : "AAECAwQF",
    // "subType" : "00"
    // }
    // } ],
    // "attr_4" : null,
    // "attr_3" : true,
    // "attr_2" : {
    // "$binary" : {
    // "base64" : "cmFuZG9tZTkzaDVvbmVmaHUxbmtyXzE5MzBga2p2LSwhJCVeaWVpZmhiajAzNA==",
    // "subType" : "00"
    // }
    // },
    // "attr_1" : 1295.03,
    // "attr_0" : "str_val_0"
    // }
    return RawBsonDocument.parse(json);
  }

}
