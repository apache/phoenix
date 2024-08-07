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
import org.apache.phoenix.util.bson.SerializableBytesPtr;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.bson.TestFieldsMap;
import org.apache.phoenix.util.bson.TestUtil;
import org.apache.phoenix.util.bson.TestFieldValue;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
      TestFieldsMap map = getMap1();
      BsonDocument bsonDocument = TestUtil.getRawBsonDocument(map);
      PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?,?,?)");
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
      TestFieldsMap map1 = getMap1();
      BsonDocument bsonDocument1 = TestUtil.getRawBsonDocument(map1);
      TestFieldsMap map2 = getMap2();
      BsonDocument bsonDocument2 = TestUtil.getRawBsonDocument(map2);
      TestFieldsMap testFieldsMap = getCompareValueMap();
      BsonDocument compareValuesDocument =
          TestUtil.getRawBsonDocument(testFieldsMap);
      PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?,?,?)");
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
    }
  }

  private static TestFieldsMap getCompareValueMap() {
    TestFieldsMap compareValMap = new TestFieldsMap();
    Map<String, TestFieldValue> map2 = new HashMap<>();
    map2.put(":Id", new TestFieldValue().withN(101.01));
    map2.put(":Id1", new TestFieldValue().withN(120));
    map2.put(":Ids1", new TestFieldValue().withS("12"));
    map2.put(":Id2", new TestFieldValue().withN(12));
    map2.put(":Title", new TestFieldValue().withS("Book 101 Title"));
    map2.put(":ISBN", new TestFieldValue().withS("111-1111111111"));
    map2.put(":InPublication", new TestFieldValue().withBOOL(false));
    map2.put(":NMap1_NList1", new TestFieldValue().withS("NListVal01"));
    map2.put(":NestedList1_485", new TestFieldValue().withN(-485.33));
    map2.put(":NestedList1_xyz0123", new TestFieldValue().withS("xyz0123"));
    compareValMap.setMap(map2);
    return compareValMap;
  }

  private static TestFieldsMap getMap2() {
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
            new TestFieldValue().withN(-23.4)));
    map.put("NestedMap1", new TestFieldValue().withM(nestedMap1));
    Map<String, TestFieldValue> nestedList1Map1 = new HashMap<>();
    nestedList1Map1.put("Id", new TestFieldValue().withN(101.01));
    nestedList1Map1.put("Title", new TestFieldValue().withS("Book 101 Title"));
    nestedList1Map1.put("ISBN", new TestFieldValue().withS("111-1111111111"));
    nestedList1Map1.put("InPublication", new TestFieldValue().withBOOL(false));
    map.put("NestedList1",
        new TestFieldValue().withL(new TestFieldValue().withN(-485.34),
            new TestFieldValue().withS("1234abcd"),
            new TestFieldValue().withL(new TestFieldValue().withS("xyz0123"),
                new TestFieldValue().withM(nestedList1Map1))));
    testFieldsMap.setMap(map);
    return testFieldsMap;
  }

  private static TestFieldsMap getMap1() {
    Map<String, TestFieldValue> map = new HashMap<>();
    map.put("attr_0", new TestFieldValue().withS("str_val_0"));
    map.put("attr_1", new TestFieldValue().withN(1295.03));
    String bytesAttributeVal = "randome93h5onefhu1nkr_1930`kjv-,!$%^ieifhbj034";
    byte[] bytesAttrVal = bytesAttributeVal.getBytes();
    map.put("attr_2", new TestFieldValue().withB(new SerializableBytesPtr(
        bytesAttrVal)));
    map.put("attr_3", new TestFieldValue().withBOOL(true));
    map.put("attr_4", new TestFieldValue().withNULL(true));
    map.put("attr_5", new TestFieldValue().withL(
        new TestFieldValue().withN(1234),
        new TestFieldValue().withS("str001"),
        new TestFieldValue().withB(new SerializableBytesPtr(
            new byte[] {0, 1, 2, 3, 4, 5}))));
    Map<String, TestFieldValue> nestedMap1 = new HashMap<>();
    nestedMap1.put("n_attr_0", new TestFieldValue().withS("str_val_0"));
    nestedMap1.put("n_attr_1", new TestFieldValue().withN(1295.03));
    String bytesAttributeVal1 = "2048u5nblwjeiWFGTH(4bf930";
    byte[] bytesAttrVal1 = bytesAttributeVal1.getBytes();
    nestedMap1.put("n_attr_2", new TestFieldValue().withB(new SerializableBytesPtr(
        bytesAttrVal1)));
    nestedMap1.put("n_attr_3", new TestFieldValue().withBOOL(true));
    nestedMap1.put("n_attr_4", new TestFieldValue().withNULL(true));
    map.put("attr_6", new TestFieldValue().withM(nestedMap1));
    map.put("attr_7", new TestFieldValue().withSS("strset001", "str_set002", "strset003"));
    map.put("attr_8", new TestFieldValue().withNS(7593, 3802.34, -4839, -40.667));
    map.put("attr_9", new TestFieldValue().withBS(
        new SerializableBytesPtr(Bytes.toBytes("abcd")),
        new SerializableBytesPtr(Bytes.toBytes("string_0203"))));
    TestFieldsMap testFieldsMap = new TestFieldsMap();
    testFieldsMap.setMap(map);
    return testFieldsMap;
  }

}
