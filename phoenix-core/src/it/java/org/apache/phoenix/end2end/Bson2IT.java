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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.bson.TestFieldsMap;
import org.apache.phoenix.util.bson.TestUtil;
import org.apache.phoenix.util.bson.TestFieldValue;
import org.apache.phoenix.util.bson.SerializableBytesPtr;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
      String ddl = "CREATE TABLE " + tableName
          + " (PK1 VARCHAR NOT NULL, PK2 VARCHAR NOT NULL, COL BSON"
          + " CONSTRAINT pk PRIMARY KEY(PK1, PK2))";
      conn.createStatement().execute(ddl);
      TestFieldsMap testFieldsMap1 = getTestFieldsMap1();
      BsonDocument bsonDocument1 = TestUtil.getRawBsonDocument(testFieldsMap1);
      TestFieldsMap compareTestFieldsMap = getComparisonValuesMap();
      BsonDocument compareValuesDocument =
          TestUtil.getRawBsonDocument(compareTestFieldsMap);
      PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?,?,?)");
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
      String updateExp = "SET Title = :newTitle, Id = :newId , NestedMap1.ColorList = :ColorList ,"
          + "Id1 = :Id1 , NestedMap1.NList1[0] = :NList1_0 , "
          + "NestedList1[2][1].ISBN = :NestedList1_ISBN , "
          + "NestedMap1.NestedMap2.NewID = :newId , "
          + "NestedMap1.NestedMap2.NList[2] = :NList003 , "
          + "NestedMap1.NestedMap2.NList[0] = :NList001 ADD AddedId :attr5_0 , "
          + "NestedMap1.AddedId :attr5_0, NestedMap1.NestedMap2.Id :newIdNeg , "
          + "NestedList12[2][0] :NestedList12_00 , NestedList12[2][1] :NestedList12_01 , "
          + "Pictures  :AddedPics "
          + "REMOVE IdS, Id2, NestedMap1.Title , "
          + "NestedMap1.NestedMap2.InPublication , NestedList1[2][1].TitleSet1 "
          + "DELETE PictureBinarySet :PictureBinarySet01 , NestedMap1.NSet1 :NSet01 ,"
          + "  NestedList1[2][1].TitleSet2  :NestedList1TitleSet01";
*/


      //{
      //  "$SET": {
      //    "Title": "Cycle_1234_new",
      //    "Id": "12345",
      //    "NestedMap1.ColorList": [
      //      "Black",
      //      {
      //        "$binary": {
      //          "base64": "V2hpdGU=",
      //          "subType": "00"
      //        }
      //      },
      //      "Silver"
      //    ],
      //    "Id1": {
      //      "$binary": {
      //        "base64": "SURfMTAx",
      //        "subType": "00"
      //      }
      //    },
      //    "NestedMap1.NList1[0]": {
      //      "$set": [
      //        "Updated_set_01",
      //        "Updated_set_02"
      //      ]
      //    },
      //    "NestedList1[2][1].ISBN": "111-1111111122",
      //    "NestedMap1.NestedMap2.NewID": "12345",
      //    "NestedMap1.NestedMap2.NList[2]": null,
      //    "NestedMap1.NestedMap2.NList[0]": 12.22
      //  },
      //  "$UNSET": {
      //    "IdS": null,
      //    "Id2": null,
      //    "NestedMap1.Title": null,
      //    "NestedMap1.NestedMap2.InPublication": null,
      //    "NestedList1[2][1].TitleSet1": null
      //  },
      //  "$ADD": {
      //    "AddedId": 10,
      //    "NestedMap1.AddedId": 10,
      //    "NestedMap1.NestedMap2.Id": -12345,
      //    "NestedList12[2][0]": {
      //      "$set": [
      //        "xyz01234",
      //        "abc01234"
      //      ]
      //    },
      //    "NestedList12[2][1]": {
      //      "$set": [
      //        {
      //          "$binary": {
      //            "base64": "dmFsMDM=",
      //            "subType": "00"
      //          }
      //        },
      //        {
      //          "$binary": {
      //            "base64": "dmFsMDQ=",
      //            "subType": "00"
      //          }
      //        }
      //      ]
      //    },
      //    "Pictures": {
      //      "$set": [
      //        "xyz5@_rear.jpg",
      //        "1235@_rear.jpg"
      //      ]
      //    }
      //  },
      //  "$DELETE_FROM_SET": {
      //    "PictureBinarySet": {
      //      "$set": [
      //        {
      //          "$binary": {
      //            "base64": "MTIzX3JlYXIuanBn",
      //            "subType": "00"
      //          }
      //        },
      //        {
      //          "$binary": {
      //            "base64": "eHl6X2Zyb250LmpwZ19ubw==",
      //            "subType": "00"
      //          }
      //        },
      //        {
      //          "$binary": {
      //            "base64": "eHl6X2Zyb250LmpwZw==",
      //            "subType": "00"
      //          }
      //        }
      //      ]
      //    },
      //    "NestedMap1.NSet1": {
      //      "$set": [
      //        -6830.5555,
      //        -48695
      //      ]
      //    },
      //    "NestedList1[2][1].TitleSet2": {
      //      "$set": [
      //        "Book 1011 Title",
      //        "Book 1010 Title"
      //      ]
      //    }
      //  }
      //}

      String updateExp = "{\n" +
              "  \"$SET\": {\n" +
              "    \"Title\": \"Cycle_1234_new\",\n" +
              "    \"Id\": \"12345\",\n" +
              "    \"NestedMap1.ColorList\": [\n" +
              "      \"Black\",\n" +
              "      {\n" +
              "        \"$binary\": {\n" +
              "          \"base64\": \"V2hpdGU=\",\n" +
              "          \"subType\": \"00\"\n" +
              "        }\n" +
              "      },\n" +
              "      \"Silver\"\n" +
              "    ],\n" +
              "    \"Id1\": {\n" +
              "      \"$binary\": {\n" +
              "        \"base64\": \"SURfMTAx\",\n" +
              "        \"subType\": \"00\"\n" +
              "      }\n" +
              "    },\n" +
              "    \"NestedMap1.NList1[0]\": {\n" +
              "      \"$set\": [\n" +
              "        \"Updated_set_01\",\n" +
              "        \"Updated_set_02\"\n" +
              "      ]\n" +
              "    },\n" +
              "    \"NestedList1[2][1].ISBN\": \"111-1111111122\",\n" +
              "    \"NestedMap1.NestedMap2.NewID\": \"12345\",\n" +
              "    \"NestedMap1.NestedMap2.NList[2]\": null,\n" +
              "    \"NestedMap1.NestedMap2.NList[0]\": 12.22\n" +
              "  },\n" +
              "  \"$UNSET\": {\n" +
              "    \"IdS\": null,\n" +
              "    \"Id2\": null,\n" +
              "    \"NestedMap1.Title\": null,\n" +
              "    \"NestedMap1.NestedMap2.InPublication\": null,\n" +
              "    \"NestedList1[2][1].TitleSet1\": null\n" +
              "  },\n" +
              "  \"$ADD\": {\n" +
              "    \"AddedId\": 10,\n" +
              "    \"NestedMap1.AddedId\": 10,\n" +
              "    \"NestedMap1.NestedMap2.Id\": -12345,\n" +
              "    \"NestedList12[2][0]\": {\n" +
              "      \"$set\": [\n" +
              "        \"xyz01234\",\n" +
              "        \"abc01234\"\n" +
              "      ]\n" +
              "    },\n" +
              "    \"NestedList12[2][1]\": {\n" +
              "      \"$set\": [\n" +
              "        {\n" +
              "          \"$binary\": {\n" +
              "            \"base64\": \"dmFsMDM=\",\n" +
              "            \"subType\": \"00\"\n" +
              "          }\n" +
              "        },\n" +
              "        {\n" +
              "          \"$binary\": {\n" +
              "            \"base64\": \"dmFsMDQ=\",\n" +
              "            \"subType\": \"00\"\n" +
              "          }\n" +
              "        }\n" +
              "      ]\n" +
              "    },\n" +
              "    \"Pictures\": {\n" +
              "      \"$set\": [\n" +
              "        \"xyz5@_rear.jpg\",\n" +
              "        \"1235@_rear.jpg\"\n" +
              "      ]\n" +
              "    }\n" +
              "  },\n" +
              "  \"$DELETE_FROM_SET\": {\n" +
              "    \"PictureBinarySet\": {\n" +
              "      \"$set\": [\n" +
              "        {\n" +
              "          \"$binary\": {\n" +
              "            \"base64\": \"MTIzX3JlYXIuanBn\",\n" +
              "            \"subType\": \"00\"\n" +
              "          }\n" +
              "        },\n" +
              "        {\n" +
              "          \"$binary\": {\n" +
              "            \"base64\": \"eHl6X2Zyb250LmpwZ19ubw==\",\n" +
              "            \"subType\": \"00\"\n" +
              "          }\n" +
              "        },\n" +
              "        {\n" +
              "          \"$binary\": {\n" +
              "            \"base64\": \"eHl6X2Zyb250LmpwZw==\",\n" +
              "            \"subType\": \"00\"\n" +
              "          }\n" +
              "        }\n" +
              "      ]\n" +
              "    },\n" +
              "    \"NestedMap1.NSet1\": {\n" +
              "      \"$set\": [\n" +
              "        -6830.5555,\n" +
              "        -48695\n" +
              "      ]\n" +
              "    },\n" +
              "    \"NestedList1[2][1].TitleSet2\": {\n" +
              "      \"$set\": [\n" +
              "        \"Book 1011 Title\",\n" +
              "        \"Book 1010 Title\"\n" +
              "      ]\n" +
              "    }\n" +
              "  }\n" +
              "}";

      String conditionExpression =
          "PictureBinarySet = :PictureBinarySet AND NestedMap1.NestedMap2.NList[1] < :NList001";

      BsonDocument conditionDoc = new BsonDocument();
      conditionDoc.put("$EXPR", new BsonString(conditionExpression));
      conditionDoc.put("$VAL", compareValuesDocument);

      stmt = conn.prepareStatement("UPSERT INTO " + tableName
          + " VALUES (?,?) ON DUPLICATE KEY UPDATE COL = CASE WHEN BSON_CONDITION_EXPRESSION(COL, '"
          + conditionDoc.toJson() + "') THEN BSON_UPDATE_EXPRESSION(COL, '"
          + updateExp + "') ELSE COL END");
      stmt.setString(1, "pk0001");
      stmt.setString(2, "pk0002");
      stmt.executeUpdate();

/*
      updateExp = "SET Id1 = :newId , NestedList1[0] = NestedList1[0] + :NList001 , "
          + "NestedList1[3] = :NList003 , NestedList1[4] = :NList004, "
          + "attr_5[0] = attr_5[0] - :attr5_0";
*/

      updateExp = "{\n" +
              "  \"$SET\": {\n" +
              "    \"Id1\": \"12345\",\n" +
              "    \"NestedList1[0]\": \"NestedList1[0] + 12.22\",\n" +
              "    \"NestedList1[3]\": null,\n" +
              "    \"NestedList1[4]\": true,\n" +
              "    \"attr_5[0]\": \"attr_5[0] - 10\"\n" +
              "  }\n" +
              "}";

      stmt = conn.prepareStatement("UPSERT INTO " + tableName
              + " VALUES (?,?) ON DUPLICATE KEY UPDATE COL = BSON_UPDATE_EXPRESSION(COL, '"
              + updateExp + "')");
      stmt.setString(1, "pk0001");
      stmt.setString(2, "pk0002");
      stmt.executeUpdate();
      conn.commit();

      query = "SELECT * FROM " + tableName;
      rs = conn.createStatement().executeQuery(query);

      assertTrue(rs.next());
      assertEquals("pk0001", rs.getString(1));
      assertEquals("pk0002", rs.getString(2));

      TestFieldsMap testFieldsMap3 = getTestFieldsMap3();
      BsonDocument bsonDocument2 = TestUtil.getRawBsonDocument(testFieldsMap3);

      BsonDocument document2 = (BsonDocument) rs.getObject(3);
      assertEquals(bsonDocument2, document2);

      assertFalse(rs.next());
    }
  }

  private static TestFieldsMap getComparisonValuesMap() {
    TestFieldsMap comparisonMap = new TestFieldsMap();
    Map<String, TestFieldValue> map2 = new HashMap<>();
    map2.put(":newTitle", new TestFieldValue().withS("Cycle_1234_new"));
    map2.put(":newId", new TestFieldValue().withS("12345"));
    map2.put(":newIdNeg", new TestFieldValue().withN(-12345));
    map2.put(":ColorList", new TestFieldValue().withL(
        new TestFieldValue().withS("Black"),
        new TestFieldValue().withB(new SerializableBytesPtr(Bytes.toBytes("White"))),
        new TestFieldValue().withS("Silver")
    ));
    map2.put(":Id1",
        new TestFieldValue().withB(new SerializableBytesPtr(Bytes.toBytes("ID_101"))));
    map2.put(":NList001", new TestFieldValue().withN(12.22));
    map2.put(":NList003", new TestFieldValue().withNULL(true));
    map2.put(":NList004", new TestFieldValue().withBOOL(true));
    map2.put(":attr5_0", new TestFieldValue().withN(10));
    map2.put(":NList1_0", new TestFieldValue().withSS("Updated_set_01", "Updated_set_02"));
    map2.put(":NestedList1_ISBN", new TestFieldValue().withS("111-1111111122"));
    map2.put(":NestedList12_00", new TestFieldValue().withSS("xyz01234", "abc01234"));
    map2.put(":NestedList12_01", new TestFieldValue().withBS(
        new SerializableBytesPtr(Bytes.toBytes("val03")),
        new SerializableBytesPtr(Bytes.toBytes("val04"))
    ));
    map2.put(":AddedPics", new TestFieldValue().withSS(
        "1235@_rear.jpg",
        "xyz5@_rear.jpg"));
    map2.put(":PictureBinarySet01", new TestFieldValue().withBS(
        new SerializableBytesPtr(Bytes.toBytes("123_rear.jpg")),
        new SerializableBytesPtr(Bytes.toBytes("xyz_front.jpg")),
        new SerializableBytesPtr(Bytes.toBytes("xyz_front.jpg_no"))
    ));
    map2.put(":NSet01", new TestFieldValue().withNS(-6830.5555, -48695));
    map2.put(":NestedList1TitleSet01", new TestFieldValue().withSS("Book 1010 Title",
        "Book 1011 Title"));
    map2.put(":PictureBinarySet", new TestFieldValue().withBS(
        new SerializableBytesPtr(Bytes.toBytes("123_rear.jpg")),
        new SerializableBytesPtr(Bytes.toBytes("xyz_rear.jpg")),
        new SerializableBytesPtr(Bytes.toBytes("123_front.jpg")),
        new SerializableBytesPtr(Bytes.toBytes("xyz_front.jpg")),
        new SerializableBytesPtr(Bytes.toBytes("123abc_rear.jpg")),
        new SerializableBytesPtr(Bytes.toBytes("xyzabc_rear.jpg"))
    ));
    comparisonMap.setMap(map2);
    return comparisonMap;
  }

  private static TestFieldsMap getTestFieldsMap1() {
    TestFieldsMap testFieldsMap = new TestFieldsMap();
    Map<String, TestFieldValue> map = new HashMap<>();
    map.put("attr_0", new TestFieldValue().withS("str_val_0"));
    map.put("attr_1", new TestFieldValue().withN(1295.03));
    map.put("attr_5", new TestFieldValue().withL(
        new TestFieldValue().withN(1234),
        new TestFieldValue().withS("str001"),
        new TestFieldValue().withB(new SerializableBytesPtr(
            new byte[] {0, 1, 2, 3, 4, 5}))));
    Map<String, TestFieldValue> nMap1 = new HashMap<>();
    nMap1.put("n_attr_0", new TestFieldValue().withS("str_val_0"));
    nMap1.put("n_attr_1", new TestFieldValue().withN(1295.03));
    String bytesAttributeVal1 = "2048u5nblwjeiWFGTH(4bf930";
    byte[] bytesAttrVal1 = bytesAttributeVal1.getBytes();
    nMap1.put("n_attr_2", new TestFieldValue().withB(new SerializableBytesPtr(
        bytesAttrVal1)));
    nMap1.put("n_attr_3", new TestFieldValue().withBOOL(true));
    nMap1.put("n_attr_4", new TestFieldValue().withNULL(true));
    map.put("attr_6", new TestFieldValue().withM(nMap1));
    map.put("Id", new TestFieldValue().withN(101.01));
    map.put("IdS", new TestFieldValue().withS("101.01"));
    map.put("Id2", new TestFieldValue().withN(101.01));
    map.put("ColorBytes",
        new TestFieldValue().withB(new SerializableBytesPtr(Bytes.toBytes("Black"))));
    map.put("RelatedItems",
        new TestFieldValue().withNS(1234, -485.45582904, 123.0948, 0.111));
    map.put("Pictures", new TestFieldValue().withSS(
        "123_rear.jpg",
        "xyz_rear.jpg",
        "123_front.jpg",
        "xyz_front.jpg"
    ));
    map.put("PictureBinarySet", new TestFieldValue().withBS(
        new SerializableBytesPtr(Bytes.toBytes("123_rear.jpg")),
        new SerializableBytesPtr(Bytes.toBytes("xyz_rear.jpg")),
        new SerializableBytesPtr(Bytes.toBytes("123_front.jpg")),
        new SerializableBytesPtr(Bytes.toBytes("xyz_front.jpg")),
        new SerializableBytesPtr(Bytes.toBytes("123abc_rear.jpg")),
        new SerializableBytesPtr(Bytes.toBytes("xyzabc_rear.jpg"))
    ));
    map.put("Title", new TestFieldValue().withS("Book 101 Title"));
    map.put("ISBN", new TestFieldValue().withS("111-1111111111"));
    map.put("InPublication", new TestFieldValue().withBOOL(false));
    Map<String, TestFieldValue> nestedMap1 = new HashMap<>();
    nestedMap1.put("Id", new TestFieldValue().withN(101.01));
    nestedMap1.put("Title", new TestFieldValue().withS("Book 101 Title"));
    nestedMap1.put("ISBN", new TestFieldValue().withS("111-1111111111"));
    nestedMap1.put("InPublication", new TestFieldValue().withBOOL(false));
    nestedMap1.put("NList1",
        new TestFieldValue().withL(new TestFieldValue().withS("NListVal01"),
            new TestFieldValue().withN(-0.00234)));
    Map<String, TestFieldValue> nestedMap2 = new HashMap<>();
    nestedMap2.put("Id", new TestFieldValue().withN(101.22));
    nestedMap2.put("Title", new TestFieldValue().withS("Book 10122 Title"));
    nestedMap2.put("ISBN", new TestFieldValue().withS("111-1111111111999"));
    nestedMap2.put("InPublication", new TestFieldValue().withBOOL(true));
    nestedMap2.put("NList",
        new TestFieldValue().withL(new TestFieldValue().withS("NListVal01"),
            new TestFieldValue().withN(-0.00234)));
    nestedMap1.put("NestedMap2",
        new TestFieldValue().withM(nestedMap2));
    nestedMap1.put("NSet1",
        new TestFieldValue().withNS(123.45, 9586.7778, -124, -6830.5555, 10238,
            -48695));
    map.put("NestedMap1", new TestFieldValue().withM(nestedMap1));
    Map<String, TestFieldValue> nestedList1Map1 = new HashMap<>();
    nestedList1Map1.put("Id", new TestFieldValue().withN(101.01));
    nestedList1Map1.put("Title", new TestFieldValue().withS("Book 101 Title"));
    nestedList1Map1.put("ISBN", new TestFieldValue().withS("111-1111111111"));
    nestedList1Map1.put("InPublication", new TestFieldValue().withBOOL(false));
    nestedList1Map1.put("IdSet",
        new TestFieldValue().withNS(204850.69703847596, -39457860.486939476, 20576024,
            19306873, 4869067048362749590684d));
    nestedList1Map1.put("TitleSet1",
        new TestFieldValue().withSS("Book 1010 Title", "Book 1011 Title",
            "Book 1111 Title", "Book 1200 Title", "Book 1201 Title"));
    nestedList1Map1.put("TitleSet2",
        new TestFieldValue().withSS("Book 1010 Title", "Book 1011 Title",
            "Book 1111 Title", "Book 1200 Title", "Book 1201 Title"));
    nestedList1Map1.put("BinaryTitleSet", new TestFieldValue().withBS(
        new SerializableBytesPtr(Bytes.toBytes("Book 1010 Title Binary")),
        new SerializableBytesPtr(Bytes.toBytes("Book 1011 Title Binary")),
        new SerializableBytesPtr(Bytes.toBytes("Book 1111 Title Binary"))
    ));
    map.put("NestedList1",
        new TestFieldValue().withL(new TestFieldValue().withN(-485.34),
            new TestFieldValue().withS("1234abcd"),
            new TestFieldValue().withL(new TestFieldValue().withS("xyz0123"),
                new TestFieldValue().withM(nestedList1Map1))));
    map.put("NestedList12",
        new TestFieldValue().withL(new TestFieldValue().withN(-485.34),
            new TestFieldValue().withS("1234abcd"),
            new TestFieldValue().withL(new TestFieldValue().withSS("xyz0123"),
                new TestFieldValue().withBS(
                    new SerializableBytesPtr(Bytes.toBytes("val01")),
                    new SerializableBytesPtr(Bytes.toBytes("val02")),
                    new SerializableBytesPtr(Bytes.toBytes("val03"))))));
    testFieldsMap.setMap(map);
    return testFieldsMap;
  }

  private static TestFieldsMap getTestFieldsMap2() {
    TestFieldsMap testFieldsMap = new TestFieldsMap();
    Map<String, TestFieldValue> map = new HashMap<>();
    map.put("attr_0", new TestFieldValue().withS("str_val_0"));
    map.put("AddedId", new TestFieldValue().withN(10));
    map.put("attr_1", new TestFieldValue().withN(1295.03));
    map.put("Id1",
        new TestFieldValue().withB(new SerializableBytesPtr(Bytes.toBytes("ID_101"))));
    map.put("attr_5", new TestFieldValue().withL(
        new TestFieldValue().withN(1234),
        new TestFieldValue().withS("str001"),
        new TestFieldValue().withB(new SerializableBytesPtr(
            new byte[] {0, 1, 2, 3, 4, 5}))));
    Map<String, TestFieldValue> nMap1 = new HashMap<>();
    nMap1.put("n_attr_0", new TestFieldValue().withS("str_val_0"));
    nMap1.put("n_attr_1", new TestFieldValue().withN(1295.03));
    String bytesAttributeVal1 = "2048u5nblwjeiWFGTH(4bf930";
    byte[] bytesAttrVal1 = bytesAttributeVal1.getBytes();
    nMap1.put("n_attr_2", new TestFieldValue().withB(new SerializableBytesPtr(
        bytesAttrVal1)));
    nMap1.put("n_attr_3", new TestFieldValue().withBOOL(true));
    nMap1.put("n_attr_4", new TestFieldValue().withNULL(true));
    map.put("attr_6", new TestFieldValue().withM(nMap1));
    map.put("Id", new TestFieldValue().withS("12345"));
    map.put("ColorBytes",
        new TestFieldValue().withB(new SerializableBytesPtr(Bytes.toBytes("Black"))));
    map.put("RelatedItems",
        new TestFieldValue().withNS(1234, -485.45582904, 123.0948, 0.111));
    map.put("Pictures", new TestFieldValue().withSS(
        "123_rear.jpg",
        "1235@_rear.jpg",
        "xyz5@_rear.jpg",
        "xyz_rear.jpg",
        "123_front.jpg",
        "xyz_front.jpg"
    ));
    map.put("PictureBinarySet", new TestFieldValue().withBS(
        new SerializableBytesPtr(Bytes.toBytes("xyz_rear.jpg")),
        new SerializableBytesPtr(Bytes.toBytes("123_front.jpg")),
        new SerializableBytesPtr(Bytes.toBytes("123abc_rear.jpg")),
        new SerializableBytesPtr(Bytes.toBytes("xyzabc_rear.jpg"))
    ));
    map.put("Title", new TestFieldValue().withS("Cycle_1234_new"));
    map.put("ISBN", new TestFieldValue().withS("111-1111111111"));
    map.put("InPublication", new TestFieldValue().withBOOL(false));
    Map<String, TestFieldValue> nestedMap1 = new HashMap<>();
    nestedMap1.put("Id", new TestFieldValue().withN(101.01));
    nestedMap1.put("AddedId", new TestFieldValue().withN(10));
    nestedMap1.put("ISBN", new TestFieldValue().withS("111-1111111111"));
    nestedMap1.put("InPublication", new TestFieldValue().withBOOL(false));
    nestedMap1.put("NList1", new TestFieldValue().withL(
        new TestFieldValue().withSS("Updated_set_01", "Updated_set_02"),
        new TestFieldValue().withN(-0.00234)));
    nestedMap1.put("ColorList", new TestFieldValue().withL(
        new TestFieldValue().withS("Black"),
        new TestFieldValue().withB(new SerializableBytesPtr(Bytes.toBytes("White"))),
        new TestFieldValue().withS("Silver")
    ));
    Map<String, TestFieldValue> nestedMap2 = new HashMap<>();
    nestedMap2.put("Id", new TestFieldValue().withN(-12243.78));
    nestedMap2.put("NewID", new TestFieldValue().withS("12345"));
    nestedMap2.put("Title", new TestFieldValue().withS("Book 10122 Title"));
    nestedMap2.put("ISBN", new TestFieldValue().withS("111-1111111111999"));
    nestedMap2.put("NList",
        new TestFieldValue().withL(
            new TestFieldValue().withN(12.22),
            new TestFieldValue().withN(-0.00234),
            new TestFieldValue().withNULL(true)));
    nestedMap1.put("NestedMap2",
        new TestFieldValue().withM(nestedMap2));
    nestedMap1.put("NSet1",
        new TestFieldValue().withNS(123.45, 9586.7778, -124, 10238));
    map.put("NestedMap1", new TestFieldValue().withM(nestedMap1));
    Map<String, TestFieldValue> nestedList1Map1 = new HashMap<>();
    nestedList1Map1.put("Id", new TestFieldValue().withN(101.01));
    nestedList1Map1.put("Title", new TestFieldValue().withS("Book 101 Title"));
    nestedList1Map1.put("ISBN", new TestFieldValue().withS("111-1111111122"));
    nestedList1Map1.put("InPublication", new TestFieldValue().withBOOL(false));
    nestedList1Map1.put("IdSet",
        new TestFieldValue().withNS(204850.69703847596, -39457860.486939476, 20576024,
            19306873, 4869067048362749590684D));
    nestedList1Map1.put("TitleSet2",
        new TestFieldValue().withSS(
            "Book 1111 Title", "Book 1200 Title", "Book 1201 Title"));
    nestedList1Map1.put("BinaryTitleSet", new TestFieldValue().withBS(
        new SerializableBytesPtr(Bytes.toBytes("Book 1010 Title Binary")),
        new SerializableBytesPtr(Bytes.toBytes("Book 1011 Title Binary")),
        new SerializableBytesPtr(Bytes.toBytes("Book 1111 Title Binary"))
    ));
    map.put("NestedList1",
        new TestFieldValue().withL(new TestFieldValue().withN(-485.34),
            new TestFieldValue().withS("1234abcd"),
            new TestFieldValue().withL(new TestFieldValue().withS("xyz0123"),
                new TestFieldValue().withM(nestedList1Map1))));
    map.put("NestedList12",
        new TestFieldValue().withL(
            new TestFieldValue().withN(-485.34),
            new TestFieldValue().withS("1234abcd"),
            new TestFieldValue().withL(
                new TestFieldValue().withSS("xyz0123", "xyz01234", "abc01234"),
                new TestFieldValue().withBS(
                    new SerializableBytesPtr(Bytes.toBytes("val01")),
                    new SerializableBytesPtr(Bytes.toBytes("val02")),
                    new SerializableBytesPtr(Bytes.toBytes("val03")),
                    new SerializableBytesPtr(Bytes.toBytes("val04"))))));
    testFieldsMap.setMap(map);
    return testFieldsMap;
  }

  private static TestFieldsMap getTestFieldsMap3() {
    TestFieldsMap testFieldsMap = new TestFieldsMap();
    Map<String, TestFieldValue> map = new HashMap<>();
    map.put("attr_0", new TestFieldValue().withS("str_val_0"));
    map.put("AddedId", new TestFieldValue().withN(10));
    map.put("attr_1", new TestFieldValue().withN(1295.03));
    map.put("Id1", new TestFieldValue().withS("12345"));
    map.put("attr_5", new TestFieldValue().withL(
        new TestFieldValue().withN(1224),
        new TestFieldValue().withS("str001"),
        new TestFieldValue().withB(new SerializableBytesPtr(
            new byte[] {0, 1, 2, 3, 4, 5}))));
    Map<String, TestFieldValue> nMap1 = new HashMap<>();
    nMap1.put("n_attr_0", new TestFieldValue().withS("str_val_0"));
    nMap1.put("n_attr_1", new TestFieldValue().withN(1295.03));
    String bytesAttributeVal1 = "2048u5nblwjeiWFGTH(4bf930";
    byte[] bytesAttrVal1 = bytesAttributeVal1.getBytes();
    nMap1.put("n_attr_2", new TestFieldValue().withB(new SerializableBytesPtr(
        bytesAttrVal1)));
    nMap1.put("n_attr_3", new TestFieldValue().withBOOL(true));
    nMap1.put("n_attr_4", new TestFieldValue().withNULL(true));
    map.put("attr_6", new TestFieldValue().withM(nMap1));
    map.put("Id", new TestFieldValue().withS("12345"));
    map.put("ColorBytes",
        new TestFieldValue().withB(new SerializableBytesPtr(Bytes.toBytes("Black"))));
    map.put("RelatedItems",
        new TestFieldValue().withNS(1234, -485.45582904, 123.0948, 0.111));
    map.put("Pictures", new TestFieldValue().withSS(
        "123_rear.jpg",
        "1235@_rear.jpg",
        "xyz5@_rear.jpg",
        "xyz_rear.jpg",
        "123_front.jpg",
        "xyz_front.jpg"
    ));
    map.put("PictureBinarySet", new TestFieldValue().withBS(
        new SerializableBytesPtr(Bytes.toBytes("xyz_rear.jpg")),
        new SerializableBytesPtr(Bytes.toBytes("123_front.jpg")),
        new SerializableBytesPtr(Bytes.toBytes("123abc_rear.jpg")),
        new SerializableBytesPtr(Bytes.toBytes("xyzabc_rear.jpg"))
    ));
    map.put("Title", new TestFieldValue().withS("Cycle_1234_new"));
    map.put("ISBN", new TestFieldValue().withS("111-1111111111"));
    map.put("InPublication", new TestFieldValue().withBOOL(false));
    Map<String, TestFieldValue> nestedMap1 = new HashMap<>();
    nestedMap1.put("Id", new TestFieldValue().withN(101.01));
    nestedMap1.put("AddedId", new TestFieldValue().withN(10));
    nestedMap1.put("ISBN", new TestFieldValue().withS("111-1111111111"));
    nestedMap1.put("InPublication", new TestFieldValue().withBOOL(false));
    nestedMap1.put("NList1", new TestFieldValue().withL(
        new TestFieldValue().withSS("Updated_set_01", "Updated_set_02"),
        new TestFieldValue().withN(-0.00234)));
    nestedMap1.put("ColorList", new TestFieldValue().withL(
        new TestFieldValue().withS("Black"),
        new TestFieldValue().withB(new SerializableBytesPtr(Bytes.toBytes("White"))),
        new TestFieldValue().withS("Silver")
    ));
    Map<String, TestFieldValue> nestedMap2 = new HashMap<>();
    nestedMap2.put("Id", new TestFieldValue().withN(-12243.78));
    nestedMap2.put("NewID", new TestFieldValue().withS("12345"));
    nestedMap2.put("Title", new TestFieldValue().withS("Book 10122 Title"));
    nestedMap2.put("ISBN", new TestFieldValue().withS("111-1111111111999"));
    nestedMap2.put("NList",
        new TestFieldValue().withL(
            new TestFieldValue().withN(12.22),
            new TestFieldValue().withN(-0.00234),
            new TestFieldValue().withNULL(true)));
    nestedMap1.put("NestedMap2",
        new TestFieldValue().withM(nestedMap2));
    nestedMap1.put("NSet1",
        new TestFieldValue().withNS(123.45, 9586.7778, -124, 10238));
    map.put("NestedMap1", new TestFieldValue().withM(nestedMap1));
    Map<String, TestFieldValue> nestedList1Map1 = new HashMap<>();
    nestedList1Map1.put("Id", new TestFieldValue().withN(101.01));
    nestedList1Map1.put("Title", new TestFieldValue().withS("Book 101 Title"));
    nestedList1Map1.put("ISBN", new TestFieldValue().withS("111-1111111122"));
    nestedList1Map1.put("InPublication", new TestFieldValue().withBOOL(false));
    nestedList1Map1.put("IdSet",
        new TestFieldValue().withNS(204850.69703847596, -39457860.486939476, 20576024,
            19306873, 4869067048362749590684d));
    nestedList1Map1.put("TitleSet2",
        new TestFieldValue().withSS(
            "Book 1111 Title", "Book 1200 Title", "Book 1201 Title"));
    nestedList1Map1.put("BinaryTitleSet", new TestFieldValue().withBS(
        new SerializableBytesPtr(Bytes.toBytes("Book 1010 Title Binary")),
        new SerializableBytesPtr(Bytes.toBytes("Book 1011 Title Binary")),
        new SerializableBytesPtr(Bytes.toBytes("Book 1111 Title Binary"))
    ));
    map.put("NestedList1",
        new TestFieldValue().withL(new TestFieldValue().withN(-473.11999999999995),
            new TestFieldValue().withS("1234abcd"),
            new TestFieldValue().withL(new TestFieldValue().withS("xyz0123"),
                new TestFieldValue().withM(nestedList1Map1)),
            new TestFieldValue().withNULL(true),
            new TestFieldValue().withBOOL(true)));
    map.put("NestedList12",
        new TestFieldValue().withL(
            new TestFieldValue().withN(-485.34),
            new TestFieldValue().withS("1234abcd"),
            new TestFieldValue().withL(
                new TestFieldValue().withSS("xyz0123", "xyz01234", "abc01234"),
                new TestFieldValue().withBS(
                    new SerializableBytesPtr(Bytes.toBytes("val01")),
                    new SerializableBytesPtr(Bytes.toBytes("val02")),
                    new SerializableBytesPtr(Bytes.toBytes("val03")),
                    new SerializableBytesPtr(Bytes.toBytes("val04"))))));
    testFieldsMap.setMap(map);
    return testFieldsMap;
  }

}
