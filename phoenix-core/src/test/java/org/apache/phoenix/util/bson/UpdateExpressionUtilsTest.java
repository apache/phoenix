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

package org.apache.phoenix.util.bson;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bson.BsonBinaryReader;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonNumber;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.io.ByteBufferBsonInput;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.util.bson.UpdateExpressionUtils;

/**
 * Tests for BSON Update Expression Utility.
 */
public class UpdateExpressionUtilsTest {

  @Test
  public void testUpdateExpression() {
    TestFieldsMap map = getTestFieldsMap1();
    TestFieldsMap comparisonMap = getComparisonValuesMap();

    Assert.assertEquals(TestUtil.getPhoenixFieldMap(
        TestUtil.getRawBsonBytes(map)), map);
    Assert.assertEquals(TestUtil.getPhoenixFieldMap(
        map.toString()), map);

    BsonDocument bsonDocument = TestUtil.getBsonDocument(map);
    assertDeserializedBsonDoc(bsonDocument);

/*    BsonDocument expressionDoc = getBsonDocument("SET Title = :newTitle, Id = :newId , " +
            "NestedMap1.ColorList = :ColorList , "
        + "Id1 = :Id1 , NestedMap1.NList1[0] = :NList1_0 , "
        + "NestedList1[2][1].ISBN = :NestedList1_ISBN , "
        + "NestedMap1.NestedMap2.NewID = :newId , "
        + "NestedMap1.NestedMap2.NList[2] = :NList003 , "
        + "NestedMap1.NestedMap2.NList[0] = :NList001 ADD AddedId :attr5_0 , "
        + "NestedMap1.AddedId :attr5_0, NestedMap1.NestedMap2.Id :newIdNeg , "
        + "NestedList12[2][0] :NestedList12_00 , NestedList12[2][1] :NestedList12_01 ,"
        + "  Pictures  :AddedPics "
        + "REMOVE IdS, Id2, NestedMap1.Title , "
        + "NestedMap1.NestedMap2.InPublication , NestedList1[2][1].TitleSet1 "
        + "DELETE PictureBinarySet :PictureBinarySet01 , NestedMap1.NSet1 :NSet01 ,"
        + "  NestedList1[2][1].TitleSet2  :NestedList1TitleSet01", TestUtil.getRawBsonDocument(comparisonMap));
 */

    String updateExpression = "{\n" +
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
            "    \"NestedList1[2][1].TitleSet1\": null,\n" +
            "    \"NestedList1[2][10]\": null,\n" +
            "    \"NestedMap1.NList1[2]\": null\n" +
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
            "    \"NestedList12[2][2]\": {\n" +
            "      \"$set\": [\n" +
            "        -234.56,\n" +
            "        123,\n" +
            "        93756.93475960549,\n" +
            "        293755723028458.6\n" +
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
    //    "NestedList1[2][1].TitleSet1": null,
    //    "NestedList1[2][10]": null,
    //    "NestedMap1.NList1[2]": null
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
    //    "NestedList12[2][2]": {
    //      "$set": [
    //        -234.56,
    //        123,
    //        93756.93475960549,
    //        293755723028458.6
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
    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    UpdateExpressionUtils.updateExpression(expressionDoc, bsonDocument);
    Assert.assertEquals("Update expression could not update the map",
            getTestFieldsMap2(),
            TestUtil.getPhoenixFieldMap(bsonDocument));

    //{
    //  "$SET": {
    //    "NestedList1[0]": "NestedList1[0] + 12.22",
    //    "NestedList1[3]": null,
    //    "NestedList1[4]": true,
    //    "attr_5[0]": "attr_5[0] - 10",
    //    "Id1": "12345"
    //  },
    //  "$DELETE_FROM_SET": {
    //    "NestedList12[2][2]": {
    //      "$set": [
    //        -234.56,
    //        123,
    //        93756.93475960549,
    //        293755723028458.6
    //      ]
    //    }
    //  }
    //}

    updateExpression = "{\n" +
            "  \"$SET\": {\n" +
            "    \"NestedList1[0]\": \"NestedList1[0] + 12.22\",\n" +
            "    \"NestedList1[3]\": null,\n" +
            "    \"NestedList1[4]\": true,\n" +
            "    \"attr_5[0]\": \"attr_5[0] - 10\",\n" +
            "    \"Id1\": \"12345\"\n" +
            "  },\n" +
            "  \"$DELETE_FROM_SET\": {\n" +
            "    \"NestedList12[2][2]\": {\n" +
            "      \"$set\": [\n" +
            "        -234.56,\n" +
            "        123,\n" +
            "        93756.93475960549,\n" +
            "        293755723028458.6\n" +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}";

/*    UpdateExpressionUtils.updateExpression(
        getBsonDocument("SET NestedList1[0] = NestedList1[0] + :NList001 , "
            + "NestedList1[3] = :NList003 , NestedList1[4] = :NList004, "
                + "attr_5[0] = attr_5[0] - :attr5_0, Id1 = :newId",
            TestUtil.getRawBsonDocument(comparisonMap)), bsonDocument);
 */
    expressionDoc = RawBsonDocument.parse(updateExpression);
    UpdateExpressionUtils.updateExpression(expressionDoc, bsonDocument);

    Assert.assertEquals("Update expression could not update the map after second update",
        getTestFieldsMap3(),
        TestUtil.getPhoenixFieldMap(bsonDocument));

    Assert.assertEquals(TestUtil.getPhoenixFieldMap(
        TestUtil.getRawBsonBytes(map)), map);
  }

  private static void assertDeserializedBsonDoc(BsonDocument bsonDocument) {
    RawBsonDocument rawBsonDocument = new RawBsonDocument(bsonDocument, new BsonDocumentCodec());
    byte[] serializedBytes = Bytes.toBytes((rawBsonDocument).getByteBuffer().asNIO());
    RawBsonDocument desRawBsonDocument = new RawBsonDocument(serializedBytes);
    BsonDocument deserializedBsonDocument;
    try (BsonBinaryReader bsonReader = new BsonBinaryReader(
        new ByteBufferBsonInput(desRawBsonDocument.getByteBuffer()))) {
      deserializedBsonDocument =
          new BsonDocumentCodec().decode(bsonReader, DecoderContext.builder().build());
    }
    Assert.assertEquals(bsonDocument, deserializedBsonDocument);
  }

  public static BsonDocument getBsonDocument(String updateExpression,
      BsonDocument comparisonValue) {

    String setRegExPattern = "SET\\s+(.+?)(?=\\s+(REMOVE|ADD|DELETE)\\b|$)";
    String removeRegExPattern = "REMOVE\\s+(.+?)(?=\\s+(SET|ADD|DELETE)\\b|$)";
    String addRegExPattern = "ADD\\s+(.+?)(?=\\s+(SET|REMOVE|DELETE)\\b|$)";
    String deleteRegExPattern = "DELETE\\s+(.+?)(?=\\s+(SET|REMOVE|ADD)\\b|$)";

    String setString = "";
    String removeString = "";
    String addString = "";
    String deleteString = "";

    Pattern pattern = Pattern.compile(setRegExPattern);
    Matcher matcher = pattern.matcher(updateExpression);
    if (matcher.find()) {
      setString = matcher.group(1).trim();
    }

    pattern = Pattern.compile(removeRegExPattern);
    matcher = pattern.matcher(updateExpression);
    if (matcher.find()) {
      removeString = matcher.group(1).trim();
    }

    pattern = Pattern.compile(addRegExPattern);
    matcher = pattern.matcher(updateExpression);
    if (matcher.find()) {
      addString = matcher.group(1).trim();
    }

    pattern = Pattern.compile(deleteRegExPattern);
    matcher = pattern.matcher(updateExpression);
    if (matcher.find()) {
      deleteString = matcher.group(1).trim();
    }

    BsonDocument bsonDocument = new BsonDocument();
    if (!setString.isEmpty()) {
      BsonDocument setBsonDoc = new BsonDocument();
      String[] setExpressions = setString.split(",");
      for (String setExpression : setExpressions) {
        String[] keyVal = setExpression.split("\\s*=\\s*");
        if (keyVal.length == 2) {
          String attributeKey = keyVal[0].trim();
          String attributeVal = keyVal[1].trim();
          if (!attributeVal.contains("+") && !attributeVal.contains("-")) {
            setBsonDoc.put(attributeKey, comparisonValue.get(attributeVal));
          } else {
            setBsonDoc.put(attributeKey, getArithmeticExpVal(attributeVal, comparisonValue));
          }
        } else {
          throw new RuntimeException(
              "SET Expression " + setString + " does not include key value pairs separated by =");
        }
      }
      bsonDocument.put("$SET", setBsonDoc);
    }
    if (!removeString.isEmpty()) {
      String[] removeExpressions = removeString.split(",");
      BsonDocument unsetBsonDoc = new BsonDocument();
      for (String removeAttribute : removeExpressions) {
        String attributeKey = removeAttribute.trim();
        unsetBsonDoc.put(attributeKey, new BsonNull());
      }
      bsonDocument.put("$UNSET", unsetBsonDoc);
    }
    if (!addString.isEmpty()) {
      String[] addExpressions = addString.split(",");
      BsonDocument addBsonDoc = new BsonDocument();
      for (String addExpression : addExpressions) {
        addExpression = addExpression.trim();
        String[] keyVal = addExpression.split("\\s+");
        if (keyVal.length == 2) {
          String attributeKey = keyVal[0].trim();
          String attributeVal = keyVal[1].trim();
          addBsonDoc.put(attributeKey, comparisonValue.get(attributeVal));
        } else {
          throw new RuntimeException("ADD Expression " + addString
              + " does not include key value pairs separated by space");
        }
      }
      bsonDocument.put("$ADD", addBsonDoc);
    }
    if (!deleteString.isEmpty()) {
      BsonDocument delBsonDoc = new BsonDocument();
      String[] deleteExpressions = deleteString.split(",");
      for (String deleteExpression : deleteExpressions) {
        deleteExpression = deleteExpression.trim();
        String[] keyVal = deleteExpression.split("\\s+");
        if (keyVal.length == 2) {
          String attributeKey = keyVal[0].trim();
          String attributeVal = keyVal[1].trim();
          delBsonDoc.put(attributeKey, comparisonValue.get(attributeVal));
        } else {
          throw new RuntimeException("DELETE Expression " + deleteString
              + " does not include key value pairs separated by space");
        }
      }
      bsonDocument.put("$DELETE_FROM_SET", delBsonDoc);
    }
    return bsonDocument;
  }

  private static BsonString getArithmeticExpVal(String attributeVal,
      BsonDocument comparisonValuesDocument) {
    String[] tokens = attributeVal.split("\\s+");
    //      Pattern pattern = Pattern.compile(":?[a-zA-Z0-9]+");
    Pattern pattern = Pattern.compile("[#:$]?[^\\s\\n]+");
    Number newNum = null;
    StringBuilder val = new StringBuilder();
    for (String token : tokens) {
      if (token.equals("+")) {
        val.append(" + ");
        continue;
      } else if (token.equals("-")) {
        val.append(" - ");
        continue;
      }
      Matcher matcher = pattern.matcher(token);
      if (matcher.find()) {
        String operand = matcher.group();
        if (operand.startsWith(":") || operand.startsWith("$") || operand.startsWith("#")) {
          BsonValue bsonValue = comparisonValuesDocument.get(operand);
          if (!bsonValue.isNumber() && !bsonValue.isDecimal128()) {
            throw new IllegalArgumentException(
                "Operand " + operand + " is not provided as number type");
          }
          Number numVal = UpdateExpressionUtils.getNumberFromBsonNumber((BsonNumber) bsonValue);
          val.append(numVal);
        } else {
          val.append(operand);
        }
      }
    }
    return new BsonString(val.toString());
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
            new TestFieldValue().withN(-0.00234),
            new TestFieldValue().withB(new SerializableBytesPtr(Bytes.toBytes("to_be_removed")))));
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
            new TestFieldValue().withL(
                new TestFieldValue().withSS("xyz0123"),
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
                    new SerializableBytesPtr(Bytes.toBytes("val04"))),
                new TestFieldValue().withNS(
                    -234.56,
                    123,
                    93756.93475960549,
                    293755723028458.6))));
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
