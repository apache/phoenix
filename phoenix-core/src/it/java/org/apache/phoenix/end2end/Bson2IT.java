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
public class Bson2IT extends ParallelStatsDisabledIT {

  @Test
  public void testUpdateExpressions() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String ddl =
        "CREATE TABLE " + tableName + " (PK1 VARCHAR NOT NULL, PK2 VARCHAR NOT NULL, COL BSON"
          + " CONSTRAINT pk PRIMARY KEY(PK1, PK2))";
      conn.createStatement().execute(ddl);
      BsonDocument bsonDocument1 = getDocument1();
      BsonDocument compareValuesDocument = getComparisonValuesDocument();
      PreparedStatement stmt =
        conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?,?,?)");
      stmt.setString(1, "pk0001");
      stmt.setString(2, "pk0002");
      stmt.setObject(3, bsonDocument1);
      stmt.executeUpdate();
      conn.commit();

      String query = "SELECT * FROM " + tableName;
      ResultSet rs = conn.createStatement().executeQuery(query);

      assertTrue(rs.next());
      assertEquals("pk0001", rs.getString(1));
      assertEquals("pk0002", rs.getString(2));
      BsonDocument document1 = (BsonDocument) rs.getObject(3);
      assertEquals(bsonDocument1, document1);

      assertFalse(rs.next());

      /*
       * String updateExp =
       * "SET Title = :newTitle, Id = :newId , NestedMap1.ColorList = :ColorList ," +
       * "Id1 = :Id1 , NestedMap1.NList1[0] = :NList1_0 , " +
       * "NestedList1[2][1].ISBN = :NestedList1_ISBN , " + "NestedMap1.NestedMap2.NewID = :newId , "
       * + "NestedMap1.NestedMap2.NList[2] = :NList003 , " +
       * "NestedMap1.NestedMap2.NList[0] = :NList001 ADD AddedId :attr5_0 , " +
       * "NestedMap1.AddedId :attr5_0, NestedMap1.NestedMap2.Id :newIdNeg , " +
       * "NestedList12[2][0] :NestedList12_00 , NestedList12[2][1] :NestedList12_01 , " +
       * "Pictures  :AddedPics " + "REMOVE IdS, Id2, NestedMap1.Title , " +
       * "NestedMap1.NestedMap2.InPublication , NestedList1[2][1].TitleSet1 " +
       * "DELETE PictureBinarySet :PictureBinarySet01 , NestedMap1.NSet1 :NSet01 ," +
       * "  NestedList1[2][1].TitleSet2  :NestedList1TitleSet01";
       */

      // {
      // "$SET": {
      // "Title": "Cycle_1234_new",
      // "Id": "12345",
      // "NestedMap1.ColorList": [
      // "Black",
      // {
      // "$binary": {
      // "base64": "V2hpdGU=",
      // "subType": "00"
      // }
      // },
      // "Silver"
      // ],
      // "Id1": {
      // "$binary": {
      // "base64": "SURfMTAx",
      // "subType": "00"
      // }
      // },
      // "NestedMap1.NList1[0]": {
      // "$set": [
      // "Updated_set_01",
      // "Updated_set_02"
      // ]
      // },
      // "NestedList1[2][1].ISBN": "111-1111111122",
      // "NestedMap1.NestedMap2.NewID": "12345",
      // "NestedMap1.NestedMap2.NList[2]": null,
      // "NestedMap1.NestedMap2.NList[0]": 12.22
      // },
      // "$UNSET": {
      // "IdS": null,
      // "Id2": null,
      // "NestedMap1.Title": null,
      // "NestedMap1.NestedMap2.InPublication": null,
      // "NestedList1[2][1].TitleSet1": null
      // },
      // "$ADD": {
      // "AddedId": 10,
      // "NestedMap1.AddedId": 10,
      // "NestedMap1.NestedMap2.Id": -12345,
      // "NestedList12[2][0]": {
      // "$set": [
      // "xyz01234",
      // "abc01234"
      // ]
      // },
      // "NestedList12[2][1]": {
      // "$set": [
      // {
      // "$binary": {
      // "base64": "dmFsMDM=",
      // "subType": "00"
      // }
      // },
      // {
      // "$binary": {
      // "base64": "dmFsMDQ=",
      // "subType": "00"
      // }
      // }
      // ]
      // },
      // "Pictures": {
      // "$set": [
      // "xyz5@_rear.jpg",
      // "1235@_rear.jpg"
      // ]
      // }
      // },
      // "$DELETE_FROM_SET": {
      // "PictureBinarySet": {
      // "$set": [
      // {
      // "$binary": {
      // "base64": "MTIzX3JlYXIuanBn",
      // "subType": "00"
      // }
      // },
      // {
      // "$binary": {
      // "base64": "eHl6X2Zyb250LmpwZ19ubw==",
      // "subType": "00"
      // }
      // },
      // {
      // "$binary": {
      // "base64": "eHl6X2Zyb250LmpwZw==",
      // "subType": "00"
      // }
      // }
      // ]
      // },
      // "NestedMap1.NSet1": {
      // "$set": [
      // -6830.5555,
      // -48695
      // ]
      // },
      // "NestedList1[2][1].TitleSet2": {
      // "$set": [
      // "Book 1011 Title",
      // "Book 1010 Title"
      // ]
      // }
      // }
      // }

      String updateExp = "{\n" + "  \"$SET\": {\n" + "    \"Title\": \"Cycle_1234_new\",\n"
        + "    \"Id\": \"12345\",\n" + "    \"NestedMap1.ColorList\": [\n" + "      \"Black\",\n"
        + "      {\n" + "        \"$binary\": {\n" + "          \"base64\": \"V2hpdGU=\",\n"
        + "          \"subType\": \"00\"\n" + "        }\n" + "      },\n" + "      \"Silver\"\n"
        + "    ],\n" + "    \"Id1\": {\n" + "      \"$binary\": {\n"
        + "        \"base64\": \"SURfMTAx\",\n" + "        \"subType\": \"00\"\n" + "      }\n"
        + "    },\n" + "    \"NestedMap1.NList1[0]\": {\n" + "      \"$set\": [\n"
        + "        \"Updated_set_01\",\n" + "        \"Updated_set_02\"\n" + "      ]\n"
        + "    },\n" + "    \"NestedList1[2][1].ISBN\": \"111-1111111122\",\n"
        + "    \"NestedMap1.NestedMap2.NewID\": \"12345\",\n"
        + "    \"NestedMap1.NestedMap2.NList[2]\": null,\n"
        + "    \"NestedMap1.NestedMap2.NList[0]\": 12.22\n" + "  },\n" + "  \"$UNSET\": {\n"
        + "    \"IdS\": null,\n" + "    \"Id2\": null,\n" + "    \"NestedMap1.Title\": null,\n"
        + "    \"NestedMap1.NestedMap2.InPublication\": null,\n"
        + "    \"NestedList1[2][1].TitleSet1\": null\n" + "  },\n" + "  \"$ADD\": {\n"
        + "    \"AddedId\": 10,\n" + "    \"NestedMap1.AddedId\": 10,\n"
        + "    \"NestedMap1.NestedMap2.Id\": -12345,\n" + "    \"NestedList12[2][0]\": {\n"
        + "      \"$set\": [\n" + "        \"xyz01234\",\n" + "        \"abc01234\"\n" + "      ]\n"
        + "    },\n" + "    \"NestedList12[2][1]\": {\n" + "      \"$set\": [\n" + "        {\n"
        + "          \"$binary\": {\n" + "            \"base64\": \"dmFsMDM=\",\n"
        + "            \"subType\": \"00\"\n" + "          }\n" + "        },\n" + "        {\n"
        + "          \"$binary\": {\n" + "            \"base64\": \"dmFsMDQ=\",\n"
        + "            \"subType\": \"00\"\n" + "          }\n" + "        }\n" + "      ]\n"
        + "    },\n" + "    \"Pictures\": {\n" + "      \"$set\": [\n"
        + "        \"xyz5@_rear.jpg\",\n" + "        \"1235@_rear.jpg\"\n" + "      ]\n" + "    }\n"
        + "  },\n" + "  \"$DELETE_FROM_SET\": {\n" + "    \"PictureBinarySet\": {\n"
        + "      \"$set\": [\n" + "        {\n" + "          \"$binary\": {\n"
        + "            \"base64\": \"MTIzX3JlYXIuanBn\",\n" + "            \"subType\": \"00\"\n"
        + "          }\n" + "        },\n" + "        {\n" + "          \"$binary\": {\n"
        + "            \"base64\": \"eHl6X2Zyb250LmpwZ19ubw==\",\n"
        + "            \"subType\": \"00\"\n" + "          }\n" + "        },\n" + "        {\n"
        + "          \"$binary\": {\n" + "            \"base64\": \"eHl6X2Zyb250LmpwZw==\",\n"
        + "            \"subType\": \"00\"\n" + "          }\n" + "        }\n" + "      ]\n"
        + "    },\n" + "    \"NestedMap1.NSet1\": {\n" + "      \"$set\": [\n"
        + "        -6830.5555,\n" + "        -48695\n" + "      ]\n" + "    },\n"
        + "    \"NestedList1[2][1].TitleSet2\": {\n" + "      \"$set\": [\n"
        + "        \"Book 1011 Title\",\n" + "        \"Book 1010 Title\"\n" + "      ]\n"
        + "    }\n" + "  }\n" + "}";

      String conditionExpression =
        "PictureBinarySet = :PictureBinarySet AND NestedMap1.NestedMap2.NList[1] < :NList001";

      BsonDocument conditionDoc = new BsonDocument();
      conditionDoc.put("$EXPR", new BsonString(conditionExpression));
      conditionDoc.put("$VAL", compareValuesDocument);

      stmt = conn.prepareStatement("UPSERT INTO " + tableName
        + " VALUES (?,?) ON DUPLICATE KEY UPDATE COL = CASE WHEN BSON_CONDITION_EXPRESSION(COL, '"
        + conditionDoc.toJson() + "') THEN BSON_UPDATE_EXPRESSION(COL, '" + updateExp
        + "') ELSE COL END");
      stmt.setString(1, "pk0001");
      stmt.setString(2, "pk0002");
      stmt.executeUpdate();

      /*
       * updateExp = "SET Id1 = :newId , NestedList1[0] = NestedList1[0] + :NList001 , " +
       * "NestedList1[3] = :NList003 , NestedList1[4] = :NList004, " +
       * "attr_5[0] = attr_5[0] - :attr5_0";
       */

      updateExp = "{\n" + "  \"$SET\": {\n" + "    \"Id1\": \"12345\",\n"
        + "    \"NestedList1[0]\": \"NestedList1[0] + 12.22\",\n"
        + "    \"NestedList1[3]\": null,\n" + "    \"NestedList1[4]\": true,\n"
        + "    \"attr_5[0]\": \"attr_5[0] - 10\"\n" + "  }\n" + "}";

      stmt = conn.prepareStatement("UPSERT INTO " + tableName
        + " VALUES (?,?) ON DUPLICATE KEY UPDATE COL = BSON_UPDATE_EXPRESSION(COL, '" + updateExp
        + "')");
      stmt.setString(1, "pk0001");
      stmt.setString(2, "pk0002");
      stmt.executeUpdate();
      conn.commit();

      query = "SELECT * FROM " + tableName;
      rs = conn.createStatement().executeQuery(query);

      assertTrue(rs.next());
      assertEquals("pk0001", rs.getString(1));
      assertEquals("pk0002", rs.getString(2));

      BsonDocument bsonDocument2 = getDocument2();
      BsonDocument document2 = (BsonDocument) rs.getObject(3);
      assertEquals(bsonDocument2, document2);

      assertFalse(rs.next());

      /*
       * updateExp = "SET newKey1 = if_not_exists(NestedMap1.ColorList[0], 42),
       * NestedMap1.NestedMap2.newKey21 = if_not_exists(keyNotExists, "foo");
       */
      updateExp = "{\n" + "  \"$SET\": {\n" + "\"newKey1\": {\n" + "      \"$IF_NOT_EXISTS\": {\n"
        + "        \"NestedMap1.ColorList[0]\": 42\n" + "      }\n" + "    },\n"
        + "    \"NestedMap1.NestedMap2.newKey2\": {\n" + "      \"$IF_NOT_EXISTS\": {\n"
        + "        \"keyNotExists\": \"foo\"\n" + "      }\n" + "    }\n" + "  }\n" + "}";

      stmt = conn.prepareStatement("UPSERT INTO " + tableName
        + " VALUES (?,?) ON DUPLICATE KEY UPDATE COL = BSON_UPDATE_EXPRESSION(COL, '" + updateExp
        + "')");
      stmt.setString(1, "pk0001");
      stmt.setString(2, "pk0002");
      stmt.executeUpdate();
      conn.commit();

      query = "SELECT * FROM " + tableName;
      rs = conn.createStatement().executeQuery(query);

      assertTrue(rs.next());
      assertEquals("pk0001", rs.getString(1));
      assertEquals("pk0002", rs.getString(2));

      BsonDocument bsonDocument3 = getDocument3();
      BsonDocument document3 = (BsonDocument) rs.getObject(3);
      assertEquals(bsonDocument3, document3);

      assertFalse(rs.next());
    }
  }

  private static RawBsonDocument getComparisonValuesDocument() {
    String json = "{\n" + "  \":NestedList1TitleSet01\" : {\n"
      + "    \"$set\" : [ \"Book 1011 Title\", \"Book 1010 Title\" ]\n" + "  },\n"
      + "  \":ColorList\" : [ \"Black\", {\n" + "    \"$binary\" : {\n"
      + "      \"base64\" : \"V2hpdGU=\",\n" + "      \"subType\" : \"00\"\n" + "    }\n"
      + "  }, \"Silver\" ],\n" + "  \":AddedPics\" : {\n"
      + "    \"$set\" : [ \"xyz5@_rear.jpg\", \"1235@_rear.jpg\" ]\n" + "  },\n"
      + "  \":Id1\" : {\n" + "    \"$binary\" : {\n" + "      \"base64\" : \"SURfMTAx\",\n"
      + "      \"subType\" : \"00\"\n" + "    }\n" + "  },\n"
      + "  \":newTitle\" : \"Cycle_1234_new\",\n" + "  \":NSet01\" : {\n"
      + "    \"$set\" : [ -6830.5555, -48695 ]\n" + "  },\n" + "  \":NList003\" : null,\n"
      + "  \":NestedList1_ISBN\" : \"111-1111111122\",\n" + "  \":NestedList12_00\" : {\n"
      + "    \"$set\" : [ \"xyz01234\", \"abc01234\" ]\n" + "  },\n" + "  \":NList004\" : true,\n"
      + "  \":attr5_0\" : 10,\n" + "  \":PictureBinarySet01\" : {\n" + "    \"$set\" : [ {\n"
      + "      \"$binary\" : {\n" + "        \"base64\" : \"MTIzX3JlYXIuanBn\",\n"
      + "        \"subType\" : \"00\"\n" + "      }\n" + "    }, {\n" + "      \"$binary\" : {\n"
      + "        \"base64\" : \"eHl6X2Zyb250LmpwZ19ubw==\",\n" + "        \"subType\" : \"00\"\n"
      + "      }\n" + "    }, {\n" + "      \"$binary\" : {\n"
      + "        \"base64\" : \"eHl6X2Zyb250LmpwZw==\",\n" + "        \"subType\" : \"00\"\n"
      + "      }\n" + "    } ]\n" + "  },\n" + "  \":NList001\" : 12.22,\n"
      + "  \":NestedList12_01\" : {\n" + "    \"$set\" : [ {\n" + "      \"$binary\" : {\n"
      + "        \"base64\" : \"dmFsMDM=\",\n" + "        \"subType\" : \"00\"\n" + "      }\n"
      + "    }, {\n" + "      \"$binary\" : {\n" + "        \"base64\" : \"dmFsMDQ=\",\n"
      + "        \"subType\" : \"00\"\n" + "      }\n" + "    } ]\n" + "  },\n"
      + "  \":newIdNeg\" : -12345,\n" + "  \":PictureBinarySet\" : {\n" + "    \"$set\" : [ {\n"
      + "      \"$binary\" : {\n" + "        \"base64\" : \"MTIzX3JlYXIuanBn\",\n"
      + "        \"subType\" : \"00\"\n" + "      }\n" + "    }, {\n" + "      \"$binary\" : {\n"
      + "        \"base64\" : \"MTIzYWJjX3JlYXIuanBn\",\n" + "        \"subType\" : \"00\"\n"
      + "      }\n" + "    }, {\n" + "      \"$binary\" : {\n"
      + "        \"base64\" : \"eHl6X3JlYXIuanBn\",\n" + "        \"subType\" : \"00\"\n"
      + "      }\n" + "    }, {\n" + "      \"$binary\" : {\n"
      + "        \"base64\" : \"eHl6YWJjX3JlYXIuanBn\",\n" + "        \"subType\" : \"00\"\n"
      + "      }\n" + "    }, {\n" + "      \"$binary\" : {\n"
      + "        \"base64\" : \"MTIzX2Zyb250LmpwZw==\",\n" + "        \"subType\" : \"00\"\n"
      + "      }\n" + "    }, {\n" + "      \"$binary\" : {\n"
      + "        \"base64\" : \"eHl6X2Zyb250LmpwZw==\",\n" + "        \"subType\" : \"00\"\n"
      + "      }\n" + "    } ]\n" + "  },\n" + "  \":newId\" : \"12345\",\n"
      + "  \":NList1_0\" : {\n" + "    \"$set\" : [ \"Updated_set_01\", \"Updated_set_02\" ]\n"
      + "  }\n" + "}";
    // {
    // ":NestedList1TitleSet01" : {
    // "$set" : [ "Book 1011 Title", "Book 1010 Title" ]
    // },
    // ":ColorList" : [ "Black", {
    // "$binary" : {
    // "base64" : "V2hpdGU=",
    // "subType" : "00"
    // }
    // }, "Silver" ],
    // ":AddedPics" : {
    // "$set" : [ "xyz5@_rear.jpg", "1235@_rear.jpg" ]
    // },
    // ":Id1" : {
    // "$binary" : {
    // "base64" : "SURfMTAx",
    // "subType" : "00"
    // }
    // },
    // ":newTitle" : "Cycle_1234_new",
    // ":NSet01" : {
    // "$set" : [ -6830.5555, -48695 ]
    // },
    // ":NList003" : null,
    // ":NestedList1_ISBN" : "111-1111111122",
    // ":NestedList12_00" : {
    // "$set" : [ "xyz01234", "abc01234" ]
    // },
    // ":NList004" : true,
    // ":attr5_0" : 10,
    // ":PictureBinarySet01" : {
    // "$set" : [ {
    // "$binary" : {
    // "base64" : "MTIzX3JlYXIuanBn",
    // "subType" : "00"
    // }
    // }, {
    // "$binary" : {
    // "base64" : "eHl6X2Zyb250LmpwZ19ubw==",
    // "subType" : "00"
    // }
    // }, {
    // "$binary" : {
    // "base64" : "eHl6X2Zyb250LmpwZw==",
    // "subType" : "00"
    // }
    // } ]
    // },
    // ":NList001" : 12.22,
    // ":NestedList12_01" : {
    // "$set" : [ {
    // "$binary" : {
    // "base64" : "dmFsMDM=",
    // "subType" : "00"
    // }
    // }, {
    // "$binary" : {
    // "base64" : "dmFsMDQ=",
    // "subType" : "00"
    // }
    // } ]
    // },
    // ":newIdNeg" : -12345,
    // ":PictureBinarySet" : {
    // "$set" : [ {
    // "$binary" : {
    // "base64" : "MTIzX3JlYXIuanBn",
    // "subType" : "00"
    // }
    // }, {
    // "$binary" : {
    // "base64" : "MTIzYWJjX3JlYXIuanBn",
    // "subType" : "00"
    // }
    // }, {
    // "$binary" : {
    // "base64" : "eHl6X3JlYXIuanBn",
    // "subType" : "00"
    // }
    // }, {
    // "$binary" : {
    // "base64" : "eHl6YWJjX3JlYXIuanBn",
    // "subType" : "00"
    // }
    // }, {
    // "$binary" : {
    // "base64" : "MTIzX2Zyb250LmpwZw==",
    // "subType" : "00"
    // }
    // }, {
    // "$binary" : {
    // "base64" : "eHl6X2Zyb250LmpwZw==",
    // "subType" : "00"
    // }
    // } ]
    // },
    // ":newId" : "12345",
    // ":NList1_0" : {
    // "$set" : [ "Updated_set_01", "Updated_set_02" ]
    // }
    // }
    return RawBsonDocument.parse(json);
  }

  private static RawBsonDocument getDocument1() {
    String json = "{\n" + "  \"Pictures\" : {\n"
      + "    \"$set\" : [ \"123_rear.jpg\", \"xyz_front.jpg\", \"xyz_rear.jpg\", \"123_front.jpg\" ]\n"
      + "  },\n" + "  \"PictureBinarySet\" : {\n" + "    \"$set\" : [ {\n"
      + "      \"$binary\" : {\n" + "        \"base64\" : \"MTIzX3JlYXIuanBn\",\n"
      + "        \"subType\" : \"00\"\n" + "      }\n" + "    }, {\n" + "      \"$binary\" : {\n"
      + "        \"base64\" : \"MTIzYWJjX3JlYXIuanBn\",\n" + "        \"subType\" : \"00\"\n"
      + "      }\n" + "    }, {\n" + "      \"$binary\" : {\n"
      + "        \"base64\" : \"eHl6X3JlYXIuanBn\",\n" + "        \"subType\" : \"00\"\n"
      + "      }\n" + "    }, {\n" + "      \"$binary\" : {\n"
      + "        \"base64\" : \"eHl6YWJjX3JlYXIuanBn\",\n" + "        \"subType\" : \"00\"\n"
      + "      }\n" + "    }, {\n" + "      \"$binary\" : {\n"
      + "        \"base64\" : \"MTIzX2Zyb250LmpwZw==\",\n" + "        \"subType\" : \"00\"\n"
      + "      }\n" + "    }, {\n" + "      \"$binary\" : {\n"
      + "        \"base64\" : \"eHl6X2Zyb250LmpwZw==\",\n" + "        \"subType\" : \"00\"\n"
      + "      }\n" + "    } ]\n" + "  },\n" + "  \"Title\" : \"Book 101 Title\",\n"
      + "  \"InPublication\" : false,\n" + "  \"ColorBytes\" : {\n" + "    \"$binary\" : {\n"
      + "      \"base64\" : \"QmxhY2s=\",\n" + "      \"subType\" : \"00\"\n" + "    }\n" + "  },\n"
      + "  \"ISBN\" : \"111-1111111111\",\n"
      + "  \"NestedList1\" : [ -485.34, \"1234abcd\", [ \"xyz0123\", {\n"
      + "    \"InPublication\" : false,\n" + "    \"BinaryTitleSet\" : {\n"
      + "      \"$set\" : [ {\n" + "        \"$binary\" : {\n"
      + "          \"base64\" : \"Qm9vayAxMDExIFRpdGxlIEJpbmFyeQ==\",\n"
      + "          \"subType\" : \"00\"\n" + "        }\n" + "      }, {\n"
      + "        \"$binary\" : {\n"
      + "          \"base64\" : \"Qm9vayAxMDEwIFRpdGxlIEJpbmFyeQ==\",\n"
      + "          \"subType\" : \"00\"\n" + "        }\n" + "      }, {\n"
      + "        \"$binary\" : {\n"
      + "          \"base64\" : \"Qm9vayAxMTExIFRpdGxlIEJpbmFyeQ==\",\n"
      + "          \"subType\" : \"00\"\n" + "        }\n" + "      } ]\n" + "    },\n"
      + "    \"TitleSet1\" : {\n"
      + "      \"$set\" : [ \"Book 1011 Title\", \"Book 1201 Title\", \"Book 1010 Title\", \"Book 1111 Title\", \"Book 1200 Title\" ]\n"
      + "    },\n" + "    \"ISBN\" : \"111-1111111111\",\n" + "    \"IdSet\" : {\n"
      + "      \"$set\" : [ 20576024, -3.9457860486939475E7, 204850.69703847595, 4.86906704836275E21, 19306873 ]\n"
      + "    },\n" + "    \"Title\" : \"Book 101 Title\",\n" + "    \"Id\" : 101.01,\n"
      + "    \"TitleSet2\" : {\n"
      + "      \"$set\" : [ \"Book 1011 Title\", \"Book 1201 Title\", \"Book 1010 Title\", \"Book 1111 Title\", \"Book 1200 Title\" ]\n"
      + "    }\n" + "  } ] ],\n" + "  \"NestedMap1\" : {\n" + "    \"InPublication\" : false,\n"
      + "    \"ISBN\" : \"111-1111111111\",\n" + "    \"NestedMap2\" : {\n"
      + "      \"InPublication\" : true,\n" + "      \"NList\" : [ \"NListVal01\", -0.00234 ],\n"
      + "      \"ISBN\" : \"111-1111111111999\",\n" + "      \"Title\" : \"Book 10122 Title\",\n"
      + "      \"Id\" : 101.22\n" + "    },\n" + "    \"Title\" : \"Book 101 Title\",\n"
      + "    \"Id\" : 101.01,\n" + "    \"NList1\" : [ \"NListVal01\", -0.00234 ],\n"
      + "    \"NSet1\" : {\n"
      + "      \"$set\" : [ 123.45, -6830.5555, -48695, 9586.7778, -124, 10238 ]\n" + "    }\n"
      + "  },\n" + "  \"Id2\" : 101.01,\n" + "  \"attr_6\" : {\n"
      + "    \"n_attr_0\" : \"str_val_0\",\n" + "    \"n_attr_1\" : 1295.03,\n"
      + "    \"n_attr_2\" : {\n" + "      \"$binary\" : {\n"
      + "        \"base64\" : \"MjA0OHU1bmJsd2plaVdGR1RIKDRiZjkzMA==\",\n"
      + "        \"subType\" : \"00\"\n" + "      }\n" + "    },\n" + "    \"n_attr_3\" : true,\n"
      + "    \"n_attr_4\" : null\n" + "  },\n" + "  \"attr_5\" : [ 1234, \"str001\", {\n"
      + "    \"$binary\" : {\n" + "      \"base64\" : \"AAECAwQF\",\n"
      + "      \"subType\" : \"00\"\n" + "    }\n" + "  } ],\n"
      + "  \"NestedList12\" : [ -485.34, \"1234abcd\", [ {\n" + "    \"$set\" : [ \"xyz0123\" ]\n"
      + "  }, {\n" + "    \"$set\" : [ {\n" + "      \"$binary\" : {\n"
      + "        \"base64\" : \"dmFsMDE=\",\n" + "        \"subType\" : \"00\"\n" + "      }\n"
      + "    }, {\n" + "      \"$binary\" : {\n" + "        \"base64\" : \"dmFsMDM=\",\n"
      + "        \"subType\" : \"00\"\n" + "      }\n" + "    }, {\n" + "      \"$binary\" : {\n"
      + "        \"base64\" : \"dmFsMDI=\",\n" + "        \"subType\" : \"00\"\n" + "      }\n"
      + "    } ]\n" + "  } ] ],\n" + "  \"IdS\" : \"101.01\",\n" + "  \"Id\" : 101.01,\n"
      + "  \"attr_1\" : 1295.03,\n" + "  \"attr_0\" : \"str_val_0\",\n" + "  \"RelatedItems\" : {\n"
      + "    \"$set\" : [ 123.0948, -485.45582904, 1234, 0.111 ]\n" + "  }\n" + "}";
    // {
    // "Pictures" : {
    // "$set" : [ "123_rear.jpg", "xyz_front.jpg", "xyz_rear.jpg", "123_front.jpg" ]
    // },
    // "PictureBinarySet" : {
    // "$set" : [ {
    // "$binary" : {
    // "base64" : "MTIzX3JlYXIuanBn",
    // "subType" : "00"
    // }
    // }, {
    // "$binary" : {
    // "base64" : "MTIzYWJjX3JlYXIuanBn",
    // "subType" : "00"
    // }
    // }, {
    // "$binary" : {
    // "base64" : "eHl6X3JlYXIuanBn",
    // "subType" : "00"
    // }
    // }, {
    // "$binary" : {
    // "base64" : "eHl6YWJjX3JlYXIuanBn",
    // "subType" : "00"
    // }
    // }, {
    // "$binary" : {
    // "base64" : "MTIzX2Zyb250LmpwZw==",
    // "subType" : "00"
    // }
    // }, {
    // "$binary" : {
    // "base64" : "eHl6X2Zyb250LmpwZw==",
    // "subType" : "00"
    // }
    // } ]
    // },
    // "Title" : "Book 101 Title",
    // "InPublication" : false,
    // "ColorBytes" : {
    // "$binary" : {
    // "base64" : "QmxhY2s=",
    // "subType" : "00"
    // }
    // },
    // "ISBN" : "111-1111111111",
    // "NestedList1" : [ -485.34, "1234abcd", [ "xyz0123", {
    // "InPublication" : false,
    // "BinaryTitleSet" : {
    // "$set" : [ {
    // "$binary" : {
    // "base64" : "Qm9vayAxMDExIFRpdGxlIEJpbmFyeQ==",
    // "subType" : "00"
    // }
    // }, {
    // "$binary" : {
    // "base64" : "Qm9vayAxMDEwIFRpdGxlIEJpbmFyeQ==",
    // "subType" : "00"
    // }
    // }, {
    // "$binary" : {
    // "base64" : "Qm9vayAxMTExIFRpdGxlIEJpbmFyeQ==",
    // "subType" : "00"
    // }
    // } ]
    // },
    // "TitleSet1" : {
    // "$set" : [ "Book 1011 Title", "Book 1201 Title", "Book 1010 Title", "Book 1111 Title", "Book
    // 1200 Title" ]
    // },
    // "ISBN" : "111-1111111111",
    // "IdSet" : {
    // "$set" : [ 20576024, -3.9457860486939475E7, 204850.69703847595, 4.86906704836275E21, 19306873
    // ]
    // },
    // "Title" : "Book 101 Title",
    // "Id" : 101.01,
    // "TitleSet2" : {
    // "$set" : [ "Book 1011 Title", "Book 1201 Title", "Book 1010 Title", "Book 1111 Title", "Book
    // 1200 Title" ]
    // }
    // } ] ],
    // "NestedMap1" : {
    // "InPublication" : false,
    // "ISBN" : "111-1111111111",
    // "NestedMap2" : {
    // "InPublication" : true,
    // "NList" : [ "NListVal01", -0.00234 ],
    // "ISBN" : "111-1111111111999",
    // "Title" : "Book 10122 Title",
    // "Id" : 101.22
    // },
    // "Title" : "Book 101 Title",
    // "Id" : 101.01,
    // "NList1" : [ "NListVal01", -0.00234 ],
    // "NSet1" : {
    // "$set" : [ 123.45, -6830.5555, -48695, 9586.7778, -124, 10238 ]
    // }
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
    // "NestedList12" : [ -485.34, "1234abcd", [ {
    // "$set" : [ "xyz0123" ]
    // }, {
    // "$set" : [ {
    // "$binary" : {
    // "base64" : "dmFsMDE=",
    // "subType" : "00"
    // }
    // }, {
    // "$binary" : {
    // "base64" : "dmFsMDM=",
    // "subType" : "00"
    // }
    // }, {
    // "$binary" : {
    // "base64" : "dmFsMDI=",
    // "subType" : "00"
    // }
    // } ]
    // } ] ],
    // "IdS" : "101.01",
    // "Id" : 101.01,
    // "attr_1" : 1295.03,
    // "attr_0" : "str_val_0",
    // "RelatedItems" : {
    // "$set" : [ 123.0948, -485.45582904, 1234, 0.111 ]
    // }
    // }
    return RawBsonDocument.parse(json);
  }

  private static RawBsonDocument getDocument2() {
    String json = "{\n" + "  \"Pictures\" : {\n"
      + "    \"$set\" : [ \"123_rear.jpg\", \"xyz5@_rear.jpg\", \"xyz_front.jpg\", \"xyz_rear.jpg\", \"123_front.jpg\", \"1235@_rear.jpg\" ]\n"
      + "  },\n" + "  \"PictureBinarySet\" : {\n" + "    \"$set\" : [ {\n"
      + "      \"$binary\" : {\n" + "        \"base64\" : \"MTIzYWJjX3JlYXIuanBn\",\n"
      + "        \"subType\" : \"00\"\n" + "      }\n" + "    }, {\n" + "      \"$binary\" : {\n"
      + "        \"base64\" : \"eHl6X3JlYXIuanBn\",\n" + "        \"subType\" : \"00\"\n"
      + "      }\n" + "    }, {\n" + "      \"$binary\" : {\n"
      + "        \"base64\" : \"eHl6YWJjX3JlYXIuanBn\",\n" + "        \"subType\" : \"00\"\n"
      + "      }\n" + "    }, {\n" + "      \"$binary\" : {\n"
      + "        \"base64\" : \"MTIzX2Zyb250LmpwZw==\",\n" + "        \"subType\" : \"00\"\n"
      + "      }\n" + "    } ]\n" + "  },\n" + "  \"Title\" : \"Cycle_1234_new\",\n"
      + "  \"InPublication\" : false,\n" + "  \"AddedId\" : 10,\n" + "  \"ColorBytes\" : {\n"
      + "    \"$binary\" : {\n" + "      \"base64\" : \"QmxhY2s=\",\n"
      + "      \"subType\" : \"00\"\n" + "    }\n" + "  },\n" + "  \"ISBN\" : \"111-1111111111\",\n"
      + "  \"NestedList1\" : [ -473.11999999999995, \"1234abcd\", [ \"xyz0123\", {\n"
      + "    \"InPublication\" : false,\n" + "    \"BinaryTitleSet\" : {\n"
      + "      \"$set\" : [ {\n" + "        \"$binary\" : {\n"
      + "          \"base64\" : \"Qm9vayAxMDExIFRpdGxlIEJpbmFyeQ==\",\n"
      + "          \"subType\" : \"00\"\n" + "        }\n" + "      }, {\n"
      + "        \"$binary\" : {\n"
      + "          \"base64\" : \"Qm9vayAxMDEwIFRpdGxlIEJpbmFyeQ==\",\n"
      + "          \"subType\" : \"00\"\n" + "        }\n" + "      }, {\n"
      + "        \"$binary\" : {\n"
      + "          \"base64\" : \"Qm9vayAxMTExIFRpdGxlIEJpbmFyeQ==\",\n"
      + "          \"subType\" : \"00\"\n" + "        }\n" + "      } ]\n" + "    },\n"
      + "    \"ISBN\" : \"111-1111111122\",\n" + "    \"IdSet\" : {\n"
      + "      \"$set\" : [ 20576024, -3.9457860486939475E7, 204850.69703847595, 4.86906704836275E21, 19306873 ]\n"
      + "    },\n" + "    \"Title\" : \"Book 101 Title\",\n" + "    \"Id\" : 101.01,\n"
      + "    \"TitleSet2\" : {\n"
      + "      \"$set\" : [ \"Book 1201 Title\", \"Book 1111 Title\", \"Book 1200 Title\" ]\n"
      + "    }\n" + "  } ], null, true ],\n" + "  \"NestedMap1\" : {\n"
      + "    \"InPublication\" : false,\n" + "    \"ColorList\" : [ \"Black\", {\n"
      + "      \"$binary\" : {\n" + "        \"base64\" : \"V2hpdGU=\",\n"
      + "        \"subType\" : \"00\"\n" + "      }\n" + "    }, \"Silver\" ],\n"
      + "    \"AddedId\" : 10,\n" + "    \"ISBN\" : \"111-1111111111\",\n"
      + "    \"NestedMap2\" : {\n" + "      \"NewID\" : \"12345\",\n"
      + "      \"NList\" : [ 12.22, -0.00234, null ],\n"
      + "      \"ISBN\" : \"111-1111111111999\",\n" + "      \"Title\" : \"Book 10122 Title\",\n"
      + "      \"Id\" : -12243.78\n" + "    },\n" + "    \"Id\" : 101.01,\n"
      + "    \"NList1\" : [ {\n" + "      \"$set\" : [ \"Updated_set_01\", \"Updated_set_02\" ]\n"
      + "    }, -0.00234 ],\n" + "    \"NSet1\" : {\n"
      + "      \"$set\" : [ 123.45, 9586.7778, -124, 10238 ]\n" + "    }\n" + "  },\n"
      + "  \"Id1\" : \"12345\",\n" + "  \"attr_6\" : {\n" + "    \"n_attr_0\" : \"str_val_0\",\n"
      + "    \"n_attr_1\" : 1295.03,\n" + "    \"n_attr_2\" : {\n" + "      \"$binary\" : {\n"
      + "        \"base64\" : \"MjA0OHU1bmJsd2plaVdGR1RIKDRiZjkzMA==\",\n"
      + "        \"subType\" : \"00\"\n" + "      }\n" + "    },\n" + "    \"n_attr_3\" : true,\n"
      + "    \"n_attr_4\" : null\n" + "  },\n" + "  \"attr_5\" : [ 1224, \"str001\", {\n"
      + "    \"$binary\" : {\n" + "      \"base64\" : \"AAECAwQF\",\n"
      + "      \"subType\" : \"00\"\n" + "    }\n" + "  } ],\n"
      + "  \"NestedList12\" : [ -485.34, \"1234abcd\", [ {\n"
      + "    \"$set\" : [ \"xyz01234\", \"xyz0123\", \"abc01234\" ]\n" + "  }, {\n"
      + "    \"$set\" : [ {\n" + "      \"$binary\" : {\n" + "        \"base64\" : \"dmFsMDE=\",\n"
      + "        \"subType\" : \"00\"\n" + "      }\n" + "    }, {\n" + "      \"$binary\" : {\n"
      + "        \"base64\" : \"dmFsMDM=\",\n" + "        \"subType\" : \"00\"\n" + "      }\n"
      + "    }, {\n" + "      \"$binary\" : {\n" + "        \"base64\" : \"dmFsMDI=\",\n"
      + "        \"subType\" : \"00\"\n" + "      }\n" + "    }, {\n" + "      \"$binary\" : {\n"
      + "        \"base64\" : \"dmFsMDQ=\",\n" + "        \"subType\" : \"00\"\n" + "      }\n"
      + "    } ]\n" + "  } ] ],\n" + "  \"Id\" : \"12345\",\n" + "  \"attr_1\" : 1295.03,\n"
      + "  \"attr_0\" : \"str_val_0\",\n" + "  \"RelatedItems\" : {\n"
      + "    \"$set\" : [ 123.0948, -485.45582904, 1234, 0.111 ]\n" + "  }\n" + "}";
    // {
    // "Pictures" : {
    // "$set" : [ "123_rear.jpg", "xyz5@_rear.jpg", "xyz_front.jpg", "xyz_rear.jpg",
    // "123_front.jpg", "1235@_rear.jpg" ]
    // },
    // "PictureBinarySet" : {
    // "$set" : [ {
    // "$binary" : {
    // "base64" : "MTIzYWJjX3JlYXIuanBn",
    // "subType" : "00"
    // }
    // }, {
    // "$binary" : {
    // "base64" : "eHl6X3JlYXIuanBn",
    // "subType" : "00"
    // }
    // }, {
    // "$binary" : {
    // "base64" : "eHl6YWJjX3JlYXIuanBn",
    // "subType" : "00"
    // }
    // }, {
    // "$binary" : {
    // "base64" : "MTIzX2Zyb250LmpwZw==",
    // "subType" : "00"
    // }
    // } ]
    // },
    // "Title" : "Cycle_1234_new",
    // "InPublication" : false,
    // "AddedId" : 10,
    // "ColorBytes" : {
    // "$binary" : {
    // "base64" : "QmxhY2s=",
    // "subType" : "00"
    // }
    // },
    // "ISBN" : "111-1111111111",
    // "NestedList1" : [ -473.11999999999995, "1234abcd", [ "xyz0123", {
    // "InPublication" : false,
    // "BinaryTitleSet" : {
    // "$set" : [ {
    // "$binary" : {
    // "base64" : "Qm9vayAxMDExIFRpdGxlIEJpbmFyeQ==",
    // "subType" : "00"
    // }
    // }, {
    // "$binary" : {
    // "base64" : "Qm9vayAxMDEwIFRpdGxlIEJpbmFyeQ==",
    // "subType" : "00"
    // }
    // }, {
    // "$binary" : {
    // "base64" : "Qm9vayAxMTExIFRpdGxlIEJpbmFyeQ==",
    // "subType" : "00"
    // }
    // } ]
    // },
    // "ISBN" : "111-1111111122",
    // "IdSet" : {
    // "$set" : [ 20576024, -3.9457860486939475E7, 204850.69703847595, 4.86906704836275E21, 19306873
    // ]
    // },
    // "Title" : "Book 101 Title",
    // "Id" : 101.01,
    // "TitleSet2" : {
    // "$set" : [ "Book 1201 Title", "Book 1111 Title", "Book 1200 Title" ]
    // }
    // } ], null, true ],
    // "NestedMap1" : {
    // "InPublication" : false,
    // "ColorList" : [ "Black", {
    // "$binary" : {
    // "base64" : "V2hpdGU=",
    // "subType" : "00"
    // }
    // }, "Silver" ],
    // "AddedId" : 10,
    // "ISBN" : "111-1111111111",
    // "NestedMap2" : {
    // "NewID" : "12345",
    // "NList" : [ 12.22, -0.00234, null ],
    // "ISBN" : "111-1111111111999",
    // "Title" : "Book 10122 Title",
    // "Id" : -12243.78
    // },
    // "Id" : 101.01,
    // "NList1" : [ {
    // "$set" : [ "Updated_set_01", "Updated_set_02" ]
    // }, -0.00234 ],
    // "NSet1" : {
    // "$set" : [ 123.45, 9586.7778, -124, 10238 ]
    // }
    // },
    // "Id1" : "12345",
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
    // "attr_5" : [ 1224, "str001", {
    // "$binary" : {
    // "base64" : "AAECAwQF",
    // "subType" : "00"
    // }
    // } ],
    // "NestedList12" : [ -485.34, "1234abcd", [ {
    // "$set" : [ "xyz01234", "xyz0123", "abc01234" ]
    // }, {
    // "$set" : [ {
    // "$binary" : {
    // "base64" : "dmFsMDE=",
    // "subType" : "00"
    // }
    // }, {
    // "$binary" : {
    // "base64" : "dmFsMDM=",
    // "subType" : "00"
    // }
    // }, {
    // "$binary" : {
    // "base64" : "dmFsMDI=",
    // "subType" : "00"
    // }
    // }, {
    // "$binary" : {
    // "base64" : "dmFsMDQ=",
    // "subType" : "00"
    // }
    // } ]
    // } ] ],
    // "Id" : "12345",
    // "attr_1" : 1295.03,
    // "attr_0" : "str_val_0",
    // "RelatedItems" : {
    // "$set" : [ 123.0948, -485.45582904, 1234, 0.111 ]
    // }
    // }
    return RawBsonDocument.parse(json);
  }

  private static RawBsonDocument getDocument3() {
    String json = "{\n" + "   \"newKey1\" : \"Black\",\n" + "  \"Pictures\" : {\n"
      + "    \"$set\" : [ \"123_rear.jpg\", \"xyz5@_rear.jpg\", \"xyz_front.jpg\", \"xyz_rear.jpg\", \"123_front.jpg\", \"1235@_rear.jpg\" ]\n"
      + "  },\n" + "  \"PictureBinarySet\" : {\n" + "    \"$set\" : [ {\n"
      + "      \"$binary\" : {\n" + "        \"base64\" : \"MTIzYWJjX3JlYXIuanBn\",\n"
      + "        \"subType\" : \"00\"\n" + "      }\n" + "    }, {\n" + "      \"$binary\" : {\n"
      + "        \"base64\" : \"eHl6X3JlYXIuanBn\",\n" + "        \"subType\" : \"00\"\n"
      + "      }\n" + "    }, {\n" + "      \"$binary\" : {\n"
      + "        \"base64\" : \"eHl6YWJjX3JlYXIuanBn\",\n" + "        \"subType\" : \"00\"\n"
      + "      }\n" + "    }, {\n" + "      \"$binary\" : {\n"
      + "        \"base64\" : \"MTIzX2Zyb250LmpwZw==\",\n" + "        \"subType\" : \"00\"\n"
      + "      }\n" + "    } ]\n" + "  },\n" + "  \"Title\" : \"Cycle_1234_new\",\n"
      + "  \"InPublication\" : false,\n" + "  \"AddedId\" : 10,\n" + "  \"ColorBytes\" : {\n"
      + "    \"$binary\" : {\n" + "      \"base64\" : \"QmxhY2s=\",\n"
      + "      \"subType\" : \"00\"\n" + "    }\n" + "  },\n" + "  \"ISBN\" : \"111-1111111111\",\n"
      + "  \"NestedList1\" : [ -473.11999999999995, \"1234abcd\", [ \"xyz0123\", {\n"
      + "    \"InPublication\" : false,\n" + "    \"BinaryTitleSet\" : {\n"
      + "      \"$set\" : [ {\n" + "        \"$binary\" : {\n"
      + "          \"base64\" : \"Qm9vayAxMDExIFRpdGxlIEJpbmFyeQ==\",\n"
      + "          \"subType\" : \"00\"\n" + "        }\n" + "      }, {\n"
      + "        \"$binary\" : {\n"
      + "          \"base64\" : \"Qm9vayAxMDEwIFRpdGxlIEJpbmFyeQ==\",\n"
      + "          \"subType\" : \"00\"\n" + "        }\n" + "      }, {\n"
      + "        \"$binary\" : {\n"
      + "          \"base64\" : \"Qm9vayAxMTExIFRpdGxlIEJpbmFyeQ==\",\n"
      + "          \"subType\" : \"00\"\n" + "        }\n" + "      } ]\n" + "    },\n"
      + "    \"ISBN\" : \"111-1111111122\",\n" + "    \"IdSet\" : {\n"
      + "      \"$set\" : [ 20576024, -3.9457860486939475E7, 204850.69703847595, 4.86906704836275E21, 19306873 ]\n"
      + "    },\n" + "    \"Title\" : \"Book 101 Title\",\n" + "    \"Id\" : 101.01,\n"
      + "    \"TitleSet2\" : {\n"
      + "      \"$set\" : [ \"Book 1201 Title\", \"Book 1111 Title\", \"Book 1200 Title\" ]\n"
      + "    }\n" + "  } ], null, true ],\n" + "  \"NestedMap1\" : {\n"
      + "    \"InPublication\" : false,\n" + "    \"ColorList\" : [ \"Black\", {\n"
      + "      \"$binary\" : {\n" + "        \"base64\" : \"V2hpdGU=\",\n"
      + "        \"subType\" : \"00\"\n" + "      }\n" + "    }, \"Silver\" ],\n"
      + "    \"AddedId\" : 10,\n" + "    \"ISBN\" : \"111-1111111111\",\n"
      + "    \"NestedMap2\" : {\n" + "      \"NewID\" : \"12345\",\n"
      + "      \"NList\" : [ 12.22, -0.00234, null ],\n"
      + "      \"ISBN\" : \"111-1111111111999\",\n" + "      \"Title\" : \"Book 10122 Title\",\n"
      + "      \"Id\" : -12243.78\n" + "      \"newKey2\" : \"foo\"" + "    },\n"
      + "    \"Id\" : 101.01,\n" + "    \"NList1\" : [ {\n"
      + "      \"$set\" : [ \"Updated_set_01\", \"Updated_set_02\" ]\n" + "    }, -0.00234 ],\n"
      + "    \"NSet1\" : {\n" + "      \"$set\" : [ 123.45, 9586.7778, -124, 10238 ]\n" + "    }\n"
      + "  },\n" + "  \"Id1\" : \"12345\",\n" + "  \"attr_6\" : {\n"
      + "    \"n_attr_0\" : \"str_val_0\",\n" + "    \"n_attr_1\" : 1295.03,\n"
      + "    \"n_attr_2\" : {\n" + "      \"$binary\" : {\n"
      + "        \"base64\" : \"MjA0OHU1bmJsd2plaVdGR1RIKDRiZjkzMA==\",\n"
      + "        \"subType\" : \"00\"\n" + "      }\n" + "    },\n" + "    \"n_attr_3\" : true,\n"
      + "    \"n_attr_4\" : null\n" + "  },\n" + "  \"attr_5\" : [ 1224, \"str001\", {\n"
      + "    \"$binary\" : {\n" + "      \"base64\" : \"AAECAwQF\",\n"
      + "      \"subType\" : \"00\"\n" + "    }\n" + "  } ],\n"
      + "  \"NestedList12\" : [ -485.34, \"1234abcd\", [ {\n"
      + "    \"$set\" : [ \"xyz01234\", \"xyz0123\", \"abc01234\" ]\n" + "  }, {\n"
      + "    \"$set\" : [ {\n" + "      \"$binary\" : {\n" + "        \"base64\" : \"dmFsMDE=\",\n"
      + "        \"subType\" : \"00\"\n" + "      }\n" + "    }, {\n" + "      \"$binary\" : {\n"
      + "        \"base64\" : \"dmFsMDM=\",\n" + "        \"subType\" : \"00\"\n" + "      }\n"
      + "    }, {\n" + "      \"$binary\" : {\n" + "        \"base64\" : \"dmFsMDI=\",\n"
      + "        \"subType\" : \"00\"\n" + "      }\n" + "    }, {\n" + "      \"$binary\" : {\n"
      + "        \"base64\" : \"dmFsMDQ=\",\n" + "        \"subType\" : \"00\"\n" + "      }\n"
      + "    } ]\n" + "  } ] ],\n" + "  \"Id\" : \"12345\",\n" + "  \"attr_1\" : 1295.03,\n"
      + "  \"attr_0\" : \"str_val_0\",\n" + "  \"RelatedItems\" : {\n"
      + "    \"$set\" : [ 123.0948, -485.45582904, 1234, 0.111 ]\n" + "  }\n" + "}";
    // {
    // "newKey1" : "Black",
    // "Pictures" : {
    // "$set" : [ "123_rear.jpg", "xyz5@_rear.jpg", "xyz_front.jpg", "xyz_rear.jpg",
    // "123_front.jpg", "1235@_rear.jpg" ]
    // },
    // "PictureBinarySet" : {
    // "$set" : [ {
    // "$binary" : {
    // "base64" : "MTIzYWJjX3JlYXIuanBn",
    // "subType" : "00"
    // }
    // }, {
    // "$binary" : {
    // "base64" : "eHl6X3JlYXIuanBn",
    // "subType" : "00"
    // }
    // }, {
    // "$binary" : {
    // "base64" : "eHl6YWJjX3JlYXIuanBn",
    // "subType" : "00"
    // }
    // }, {
    // "$binary" : {
    // "base64" : "MTIzX2Zyb250LmpwZw==",
    // "subType" : "00"
    // }
    // } ]
    // },
    // "Title" : "Cycle_1234_new",
    // "InPublication" : false,
    // "AddedId" : 10,
    // "ColorBytes" : {
    // "$binary" : {
    // "base64" : "QmxhY2s=",
    // "subType" : "00"
    // }
    // },
    // "ISBN" : "111-1111111111",
    // "NestedList1" : [ -473.11999999999995, "1234abcd", [ "xyz0123", {
    // "InPublication" : false,
    // "BinaryTitleSet" : {
    // "$set" : [ {
    // "$binary" : {
    // "base64" : "Qm9vayAxMDExIFRpdGxlIEJpbmFyeQ==",
    // "subType" : "00"
    // }
    // }, {
    // "$binary" : {
    // "base64" : "Qm9vayAxMDEwIFRpdGxlIEJpbmFyeQ==",
    // "subType" : "00"
    // }
    // }, {
    // "$binary" : {
    // "base64" : "Qm9vayAxMTExIFRpdGxlIEJpbmFyeQ==",
    // "subType" : "00"
    // }
    // } ]
    // },
    // "ISBN" : "111-1111111122",
    // "IdSet" : {
    // "$set" : [ 20576024, -3.9457860486939475E7, 204850.69703847595, 4.86906704836275E21, 19306873
    // ]
    // },
    // "Title" : "Book 101 Title",
    // "Id" : 101.01,
    // "TitleSet2" : {
    // "$set" : [ "Book 1201 Title", "Book 1111 Title", "Book 1200 Title" ]
    // }
    // } ], null, true ],
    // "NestedMap1" : {
    // "InPublication" : false,
    // "ColorList" : [ "Black", {
    // "$binary" : {
    // "base64" : "V2hpdGU=",
    // "subType" : "00"
    // }
    // }, "Silver" ],
    // "AddedId" : 10,
    // "ISBN" : "111-1111111111",
    // "NestedMap2" : {
    // "NewID" : "12345",
    // "NList" : [ 12.22, -0.00234, null ],
    // "ISBN" : "111-1111111111999",
    // "Title" : "Book 10122 Title",
    // "Id" : -12243.78,
    // "newKey2" : "foo"
    // },
    // "Id" : 101.01,
    // "NList1" : [ {
    // "$set" : [ "Updated_set_01", "Updated_set_02" ]
    // }, -0.00234 ],
    // "NSet1" : {
    // "$set" : [ 123.45, 9586.7778, -124, 10238 ]
    // }
    // },
    // "Id1" : "12345",
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
    // "attr_5" : [ 1224, "str001", {
    // "$binary" : {
    // "base64" : "AAECAwQF",
    // "subType" : "00"
    // }
    // } ],
    // "NestedList12" : [ -485.34, "1234abcd", [ {
    // "$set" : [ "xyz01234", "xyz0123", "abc01234" ]
    // }, {
    // "$set" : [ {
    // "$binary" : {
    // "base64" : "dmFsMDE=",
    // "subType" : "00"
    // }
    // }, {
    // "$binary" : {
    // "base64" : "dmFsMDM=",
    // "subType" : "00"
    // }
    // }, {
    // "$binary" : {
    // "base64" : "dmFsMDI=",
    // "subType" : "00"
    // }
    // }, {
    // "$binary" : {
    // "base64" : "dmFsMDQ=",
    // "subType" : "00"
    // }
    // } ]
    // } ] ],
    // "Id" : "12345",
    // "attr_1" : 1295.03,
    // "attr_0" : "str_val_0",
    // "RelatedItems" : {
    // "$set" : [ 123.0948, -485.45582904, 1234, 0.111 ]
    // }
    // }
    return RawBsonDocument.parse(json);
  }

}
