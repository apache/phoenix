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

import org.bson.BsonBinaryReader;
import org.bson.BsonDocument;
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
    BsonDocument bsonDocument = getDocument1();
    assertDeserializedBsonDoc(bsonDocument);

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
            getUpdatedDocument(), bsonDocument);

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

    expressionDoc = RawBsonDocument.parse(updateExpression);
    UpdateExpressionUtils.updateExpression(expressionDoc, bsonDocument);

    Assert.assertEquals("Update expression could not update the map after second update",
        getUpdatedDocument2(), bsonDocument);
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

  private static BsonDocument getDocument1() {
    String json = "{\n" +
            "  \"Pictures\" : {\n" +
            "    \"$set\" : [ \"123_rear.jpg\", \"xyz_front.jpg\", \"xyz_rear.jpg\", \"123_front.jpg\" ]\n" +
            "  },\n" +
            "  \"PictureBinarySet\" : {\n" +
            "    \"$set\" : [ {\n" +
            "      \"$binary\" : {\n" +
            "        \"base64\" : \"MTIzX3JlYXIuanBn\",\n" +
            "        \"subType\" : \"00\"\n" +
            "      }\n" +
            "    }, {\n" +
            "      \"$binary\" : {\n" +
            "        \"base64\" : \"MTIzYWJjX3JlYXIuanBn\",\n" +
            "        \"subType\" : \"00\"\n" +
            "      }\n" +
            "    }, {\n" +
            "      \"$binary\" : {\n" +
            "        \"base64\" : \"eHl6X3JlYXIuanBn\",\n" +
            "        \"subType\" : \"00\"\n" +
            "      }\n" +
            "    }, {\n" +
            "      \"$binary\" : {\n" +
            "        \"base64\" : \"eHl6YWJjX3JlYXIuanBn\",\n" +
            "        \"subType\" : \"00\"\n" +
            "      }\n" +
            "    }, {\n" +
            "      \"$binary\" : {\n" +
            "        \"base64\" : \"MTIzX2Zyb250LmpwZw==\",\n" +
            "        \"subType\" : \"00\"\n" +
            "      }\n" +
            "    }, {\n" +
            "      \"$binary\" : {\n" +
            "        \"base64\" : \"eHl6X2Zyb250LmpwZw==\",\n" +
            "        \"subType\" : \"00\"\n" +
            "      }\n" +
            "    } ]\n" +
            "  },\n" +
            "  \"Title\" : \"Book 101 Title\",\n" +
            "  \"InPublication\" : false,\n" +
            "  \"ColorBytes\" : {\n" +
            "    \"$binary\" : {\n" +
            "      \"base64\" : \"QmxhY2s=\",\n" +
            "      \"subType\" : \"00\"\n" +
            "    }\n" +
            "  },\n" +
            "  \"ISBN\" : \"111-1111111111\",\n" +
            "  \"NestedList1\" : [ -485.34, \"1234abcd\", [ \"xyz0123\", {\n" +
            "    \"InPublication\" : false,\n" +
            "    \"BinaryTitleSet\" : {\n" +
            "      \"$set\" : [ {\n" +
            "        \"$binary\" : {\n" +
            "          \"base64\" : \"Qm9vayAxMDExIFRpdGxlIEJpbmFyeQ==\",\n" +
            "          \"subType\" : \"00\"\n" +
            "        }\n" +
            "      }, {\n" +
            "        \"$binary\" : {\n" +
            "          \"base64\" : \"Qm9vayAxMDEwIFRpdGxlIEJpbmFyeQ==\",\n" +
            "          \"subType\" : \"00\"\n" +
            "        }\n" +
            "      }, {\n" +
            "        \"$binary\" : {\n" +
            "          \"base64\" : \"Qm9vayAxMTExIFRpdGxlIEJpbmFyeQ==\",\n" +
            "          \"subType\" : \"00\"\n" +
            "        }\n" +
            "      } ]\n" +
            "    },\n" +
            "    \"TitleSet1\" : {\n" +
            "      \"$set\" : [ \"Book 1011 Title\", \"Book 1201 Title\", \"Book 1010 Title\", \"Book 1111 Title\", \"Book 1200 Title\" ]\n" +
            "    },\n" +
            "    \"ISBN\" : \"111-1111111111\",\n" +
            "    \"IdSet\" : {\n" +
            "      \"$set\" : [ 20576024, -3.9457860486939475E7, 204850.69703847595, 4.86906704836275E21, 19306873 ]\n" +
            "    },\n" +
            "    \"Title\" : \"Book 101 Title\",\n" +
            "    \"Id\" : 101.01,\n" +
            "    \"TitleSet2\" : {\n" +
            "      \"$set\" : [ \"Book 1011 Title\", \"Book 1201 Title\", \"Book 1010 Title\", \"Book 1111 Title\", \"Book 1200 Title\" ]\n" +
            "    }\n" +
            "  } ] ],\n" +
            "  \"NestedMap1\" : {\n" +
            "    \"InPublication\" : false,\n" +
            "    \"ISBN\" : \"111-1111111111\",\n" +
            "    \"NestedMap2\" : {\n" +
            "      \"InPublication\" : true,\n" +
            "      \"NList\" : [ \"NListVal01\", -0.00234 ],\n" +
            "      \"ISBN\" : \"111-1111111111999\",\n" +
            "      \"Title\" : \"Book 10122 Title\",\n" +
            "      \"Id\" : 101.22\n" +
            "    },\n" +
            "    \"Title\" : \"Book 101 Title\",\n" +
            "    \"Id\" : 101.01,\n" +
            "    \"NList1\" : [ \"NListVal01\", -0.00234, {\n" +
            "      \"$binary\" : {\n" +
            "        \"base64\" : \"dG9fYmVfcmVtb3ZlZA==\",\n" +
            "        \"subType\" : \"00\"\n" +
            "      }\n" +
            "    } ],\n" +
            "    \"NSet1\" : {\n" +
            "      \"$set\" : [ 123.45, -6830.5555, -48695, 9586.7778, -124, 10238 ]\n" +
            "    }\n" +
            "  },\n" +
            "  \"Id2\" : 101.01,\n" +
            "  \"attr_6\" : {\n" +
            "    \"n_attr_0\" : \"str_val_0\",\n" +
            "    \"n_attr_1\" : 1295.03,\n" +
            "    \"n_attr_2\" : {\n" +
            "      \"$binary\" : {\n" +
            "        \"base64\" : \"MjA0OHU1bmJsd2plaVdGR1RIKDRiZjkzMA==\",\n" +
            "        \"subType\" : \"00\"\n" +
            "      }\n" +
            "    },\n" +
            "    \"n_attr_3\" : true,\n" +
            "    \"n_attr_4\" : null\n" +
            "  },\n" +
            "  \"attr_5\" : [ 1234, \"str001\", {\n" +
            "    \"$binary\" : {\n" +
            "      \"base64\" : \"AAECAwQF\",\n" +
            "      \"subType\" : \"00\"\n" +
            "    }\n" +
            "  } ],\n" +
            "  \"NestedList12\" : [ -485.34, \"1234abcd\", [ {\n" +
            "    \"$set\" : [ \"xyz0123\" ]\n" +
            "  }, {\n" +
            "    \"$set\" : [ {\n" +
            "      \"$binary\" : {\n" +
            "        \"base64\" : \"dmFsMDE=\",\n" +
            "        \"subType\" : \"00\"\n" +
            "      }\n" +
            "    }, {\n" +
            "      \"$binary\" : {\n" +
            "        \"base64\" : \"dmFsMDM=\",\n" +
            "        \"subType\" : \"00\"\n" +
            "      }\n" +
            "    }, {\n" +
            "      \"$binary\" : {\n" +
            "        \"base64\" : \"dmFsMDI=\",\n" +
            "        \"subType\" : \"00\"\n" +
            "      }\n" +
            "    } ]\n" +
            "  } ] ],\n" +
            "  \"IdS\" : \"101.01\",\n" +
            "  \"Id\" : 101.01,\n" +
            "  \"attr_1\" : 1295.03,\n" +
            "  \"attr_0\" : \"str_val_0\",\n" +
            "  \"RelatedItems\" : {\n" +
            "    \"$set\" : [ 123.0948, -485.45582904, 1234, 0.111 ]\n" +
            "  }\n" +
            "}";
    //{
    //  "Pictures" : {
    //    "$set" : [ "123_rear.jpg", "xyz_front.jpg", "xyz_rear.jpg", "123_front.jpg" ]
    //  },
    //  "PictureBinarySet" : {
    //    "$set" : [ {
    //      "$binary" : {
    //        "base64" : "MTIzX3JlYXIuanBn",
    //        "subType" : "00"
    //      }
    //    }, {
    //      "$binary" : {
    //        "base64" : "MTIzYWJjX3JlYXIuanBn",
    //        "subType" : "00"
    //      }
    //    }, {
    //      "$binary" : {
    //        "base64" : "eHl6X3JlYXIuanBn",
    //        "subType" : "00"
    //      }
    //    }, {
    //      "$binary" : {
    //        "base64" : "eHl6YWJjX3JlYXIuanBn",
    //        "subType" : "00"
    //      }
    //    }, {
    //      "$binary" : {
    //        "base64" : "MTIzX2Zyb250LmpwZw==",
    //        "subType" : "00"
    //      }
    //    }, {
    //      "$binary" : {
    //        "base64" : "eHl6X2Zyb250LmpwZw==",
    //        "subType" : "00"
    //      }
    //    } ]
    //  },
    //  "Title" : "Book 101 Title",
    //  "InPublication" : false,
    //  "ColorBytes" : {
    //    "$binary" : {
    //      "base64" : "QmxhY2s=",
    //      "subType" : "00"
    //    }
    //  },
    //  "ISBN" : "111-1111111111",
    //  "NestedList1" : [ -485.34, "1234abcd", [ "xyz0123", {
    //    "InPublication" : false,
    //    "BinaryTitleSet" : {
    //      "$set" : [ {
    //        "$binary" : {
    //          "base64" : "Qm9vayAxMDExIFRpdGxlIEJpbmFyeQ==",
    //          "subType" : "00"
    //        }
    //      }, {
    //        "$binary" : {
    //          "base64" : "Qm9vayAxMDEwIFRpdGxlIEJpbmFyeQ==",
    //          "subType" : "00"
    //        }
    //      }, {
    //        "$binary" : {
    //          "base64" : "Qm9vayAxMTExIFRpdGxlIEJpbmFyeQ==",
    //          "subType" : "00"
    //        }
    //      } ]
    //    },
    //    "TitleSet1" : {
    //      "$set" : [ "Book 1011 Title", "Book 1201 Title", "Book 1010 Title", "Book 1111 Title", "Book 1200 Title" ]
    //    },
    //    "ISBN" : "111-1111111111",
    //    "IdSet" : {
    //      "$set" : [ 20576024, -3.9457860486939475E7, 204850.69703847595, 4.86906704836275E21, 19306873 ]
    //    },
    //    "Title" : "Book 101 Title",
    //    "Id" : 101.01,
    //    "TitleSet2" : {
    //      "$set" : [ "Book 1011 Title", "Book 1201 Title", "Book 1010 Title", "Book 1111 Title", "Book 1200 Title" ]
    //    }
    //  } ] ],
    //  "NestedMap1" : {
    //    "InPublication" : false,
    //    "ISBN" : "111-1111111111",
    //    "NestedMap2" : {
    //      "InPublication" : true,
    //      "NList" : [ "NListVal01", -0.00234 ],
    //      "ISBN" : "111-1111111111999",
    //      "Title" : "Book 10122 Title",
    //      "Id" : 101.22
    //    },
    //    "Title" : "Book 101 Title",
    //    "Id" : 101.01,
    //    "NList1" : [ "NListVal01", -0.00234, {
    //      "$binary" : {
    //        "base64" : "dG9fYmVfcmVtb3ZlZA==",
    //        "subType" : "00"
    //      }
    //    } ],
    //    "NSet1" : {
    //      "$set" : [ 123.45, -6830.5555, -48695, 9586.7778, -124, 10238 ]
    //    }
    //  },
    //  "Id2" : 101.01,
    //  "attr_6" : {
    //    "n_attr_0" : "str_val_0",
    //    "n_attr_1" : 1295.03,
    //    "n_attr_2" : {
    //      "$binary" : {
    //        "base64" : "MjA0OHU1bmJsd2plaVdGR1RIKDRiZjkzMA==",
    //        "subType" : "00"
    //      }
    //    },
    //    "n_attr_3" : true,
    //    "n_attr_4" : null
    //  },
    //  "attr_5" : [ 1234, "str001", {
    //    "$binary" : {
    //      "base64" : "AAECAwQF",
    //      "subType" : "00"
    //    }
    //  } ],
    //  "NestedList12" : [ -485.34, "1234abcd", [ {
    //    "$set" : [ "xyz0123" ]
    //  }, {
    //    "$set" : [ {
    //      "$binary" : {
    //        "base64" : "dmFsMDE=",
    //        "subType" : "00"
    //      }
    //    }, {
    //      "$binary" : {
    //        "base64" : "dmFsMDM=",
    //        "subType" : "00"
    //      }
    //    }, {
    //      "$binary" : {
    //        "base64" : "dmFsMDI=",
    //        "subType" : "00"
    //      }
    //    } ]
    //  } ] ],
    //  "IdS" : "101.01",
    //  "Id" : 101.01,
    //  "attr_1" : 1295.03,
    //  "attr_0" : "str_val_0",
    //  "RelatedItems" : {
    //    "$set" : [ 123.0948, -485.45582904, 1234, 0.111 ]
    //  }
    //}
    return BsonDocument.parse(json);
  }

  private static BsonDocument getUpdatedDocument() {
    String json = "{\n" +
            "  \"Pictures\": {\n" +
            "    \"$set\": [\n" +
            "      \"123_rear.jpg\",\n" +
            "      \"xyz5@_rear.jpg\",\n" +
            "      \"xyz_front.jpg\",\n" +
            "      \"xyz_rear.jpg\",\n" +
            "      \"123_front.jpg\",\n" +
            "      \"1235@_rear.jpg\"\n" +
            "    ]\n" +
            "  },\n" +
            "  \"PictureBinarySet\": {\n" +
            "    \"$set\": [\n" +
            "      {\n" +
            "        \"$binary\": {\n" +
            "          \"base64\": \"MTIzYWJjX3JlYXIuanBn\",\n" +
            "          \"subType\": \"00\"\n" +
            "        }\n" +
            "      },\n" +
            "      {\n" +
            "        \"$binary\": {\n" +
            "          \"base64\": \"eHl6X3JlYXIuanBn\",\n" +
            "          \"subType\": \"00\"\n" +
            "        }\n" +
            "      },\n" +
            "      {\n" +
            "        \"$binary\": {\n" +
            "          \"base64\": \"eHl6YWJjX3JlYXIuanBn\",\n" +
            "          \"subType\": \"00\"\n" +
            "        }\n" +
            "      },\n" +
            "      {\n" +
            "        \"$binary\": {\n" +
            "          \"base64\": \"MTIzX2Zyb250LmpwZw==\",\n" +
            "          \"subType\": \"00\"\n" +
            "        }\n" +
            "      }\n" +
            "    ]\n" +
            "  },\n" +
            "  \"Title\": \"Cycle_1234_new\",\n" +
            "  \"InPublication\": false,\n" +
            "  \"ColorBytes\": {\n" +
            "    \"$binary\": {\n" +
            "      \"base64\": \"QmxhY2s=\",\n" +
            "      \"subType\": \"00\"\n" +
            "    }\n" +
            "  },\n" +
            "  \"ISBN\": \"111-1111111111\",\n" +
            "  \"NestedList1\": [\n" +
            "    -485.34,\n" +
            "    \"1234abcd\",\n" +
            "    [\n" +
            "      \"xyz0123\",\n" +
            "      {\n" +
            "        \"InPublication\": false,\n" +
            "        \"BinaryTitleSet\": {\n" +
            "          \"$set\": [\n" +
            "            {\n" +
            "              \"$binary\": {\n" +
            "                \"base64\": \"Qm9vayAxMDExIFRpdGxlIEJpbmFyeQ==\",\n" +
            "                \"subType\": \"00\"\n" +
            "              }\n" +
            "            },\n" +
            "            {\n" +
            "              \"$binary\": {\n" +
            "                \"base64\": \"Qm9vayAxMDEwIFRpdGxlIEJpbmFyeQ==\",\n" +
            "                \"subType\": \"00\"\n" +
            "              }\n" +
            "            },\n" +
            "            {\n" +
            "              \"$binary\": {\n" +
            "                \"base64\": \"Qm9vayAxMTExIFRpdGxlIEJpbmFyeQ==\",\n" +
            "                \"subType\": \"00\"\n" +
            "              }\n" +
            "            }\n" +
            "          ]\n" +
            "        },\n" +
            "        \"ISBN\": \"111-1111111122\",\n" +
            "        \"IdSet\": {\n" +
            "          \"$set\": [\n" +
            "            20576024,\n" +
            "            -39457860.486939475,\n" +
            "            204850.69703847595,\n" +
            "            4.86906704836275e+21,\n" +
            "            19306873\n" +
            "          ]\n" +
            "        },\n" +
            "        \"Title\": \"Book 101 Title\",\n" +
            "        \"Id\": 101.01,\n" +
            "        \"TitleSet2\": {\n" +
            "          \"$set\": [\n" +
            "            \"Book 1201 Title\",\n" +
            "            \"Book 1111 Title\",\n" +
            "            \"Book 1200 Title\"\n" +
            "          ]\n" +
            "        }\n" +
            "      }\n" +
            "    ]\n" +
            "  ],\n" +
            "  \"NestedMap1\": {\n" +
            "    \"InPublication\": false,\n" +
            "    \"ISBN\": \"111-1111111111\",\n" +
            "    \"NestedMap2\": {\n" +
            "      \"NList\": [\n" +
            "        12.22,\n" +
            "        -0.00234,\n" +
            "        null\n" +
            "      ],\n" +
            "      \"ISBN\": \"111-1111111111999\",\n" +
            "      \"Title\": \"Book 10122 Title\",\n" +
            "      \"Id\": -12243.78,\n" +
            "      \"NewID\": \"12345\"\n" +
            "    },\n" +
            "    \"Id\": 101.01,\n" +
            "    \"NList1\": [\n" +
            "      {\n" +
            "        \"$set\": [\n" +
            "          \"Updated_set_01\",\n" +
            "          \"Updated_set_02\"\n" +
            "        ]\n" +
            "      },\n" +
            "      -0.00234\n" +
            "    ],\n" +
            "    \"NSet1\": {\n" +
            "      \"$set\": [\n" +
            "        123.45,\n" +
            "        9586.7778,\n" +
            "        -124,\n" +
            "        10238\n" +
            "      ]\n" +
            "    },\n" +
            "    \"ColorList\": [\n" +
            "      \"Black\",\n" +
            "      {\n" +
            "        \"$binary\": {\n" +
            "          \"base64\": \"V2hpdGU=\",\n" +
            "          \"subType\": \"00\"\n" +
            "        }\n" +
            "      },\n" +
            "      \"Silver\"\n" +
            "    ],\n" +
            "    \"AddedId\": 10\n" +
            "  },\n" +
            "  \"attr_6\": {\n" +
            "    \"n_attr_0\": \"str_val_0\",\n" +
            "    \"n_attr_1\": 1295.03,\n" +
            "    \"n_attr_2\": {\n" +
            "      \"$binary\": {\n" +
            "        \"base64\": \"MjA0OHU1bmJsd2plaVdGR1RIKDRiZjkzMA==\",\n" +
            "        \"subType\": \"00\"\n" +
            "      }\n" +
            "    },\n" +
            "    \"n_attr_3\": true,\n" +
            "    \"n_attr_4\": null\n" +
            "  },\n" +
            "  \"attr_5\": [\n" +
            "    1234,\n" +
            "    \"str001\",\n" +
            "    {\n" +
            "      \"$binary\": {\n" +
            "        \"base64\": \"AAECAwQF\",\n" +
            "        \"subType\": \"00\"\n" +
            "      }\n" +
            "    }\n" +
            "  ],\n" +
            "  \"NestedList12\": [\n" +
            "    -485.34,\n" +
            "    \"1234abcd\",\n" +
            "    [\n" +
            "      {\n" +
            "        \"$set\": [\n" +
            "          \"xyz01234\",\n" +
            "          \"xyz0123\",\n" +
            "          \"abc01234\"\n" +
            "        ]\n" +
            "      },\n" +
            "      {\n" +
            "        \"$set\": [\n" +
            "          {\n" +
            "            \"$binary\": {\n" +
            "              \"base64\": \"dmFsMDE=\",\n" +
            "              \"subType\": \"00\"\n" +
            "            }\n" +
            "          },\n" +
            "          {\n" +
            "            \"$binary\": {\n" +
            "              \"base64\": \"dmFsMDM=\",\n" +
            "              \"subType\": \"00\"\n" +
            "            }\n" +
            "          },\n" +
            "          {\n" +
            "            \"$binary\": {\n" +
            "              \"base64\": \"dmFsMDI=\",\n" +
            "              \"subType\": \"00\"\n" +
            "            }\n" +
            "          },\n" +
            "          {\n" +
            "            \"$binary\": {\n" +
            "              \"base64\": \"dmFsMDQ=\",\n" +
            "              \"subType\": \"00\"\n" +
            "            }\n" +
            "          }\n" +
            "        ]\n" +
            "      },\n" +
            "      {\n" +
            "        \"$set\": [\n" +
            "          -234.56,\n" +
            "          123,\n" +
            "          93756.93475960549,\n" +
            "          293755723028458.6\n" +
            "        ]\n" +
            "      }\n" +
            "    ]\n" +
            "  ],\n" +
            "  \"Id\": \"12345\",\n" +
            "  \"attr_1\": 1295.03,\n" +
            "  \"attr_0\": \"str_val_0\",\n" +
            "  \"RelatedItems\": {\n" +
            "    \"$set\": [\n" +
            "      123.0948,\n" +
            "      -485.45582904,\n" +
            "      1234,\n" +
            "      0.111\n" +
            "    ]\n" +
            "  },\n" +
            "  \"Id1\": {\n" +
            "    \"$binary\": {\n" +
            "      \"base64\": \"SURfMTAx\",\n" +
            "      \"subType\": \"00\"\n" +
            "    }\n" +
            "  },\n" +
            "  \"AddedId\": 10\n" +
            "}";
    //{
    //  "Pictures": {
    //    "$set": [
    //      "123_rear.jpg",
    //      "xyz5@_rear.jpg",
    //      "xyz_front.jpg",
    //      "xyz_rear.jpg",
    //      "123_front.jpg",
    //      "1235@_rear.jpg"
    //    ]
    //  },
    //  "PictureBinarySet": {
    //    "$set": [
    //      {
    //        "$binary": {
    //          "base64": "MTIzYWJjX3JlYXIuanBn",
    //          "subType": "00"
    //        }
    //      },
    //      {
    //        "$binary": {
    //          "base64": "eHl6X3JlYXIuanBn",
    //          "subType": "00"
    //        }
    //      },
    //      {
    //        "$binary": {
    //          "base64": "eHl6YWJjX3JlYXIuanBn",
    //          "subType": "00"
    //        }
    //      },
    //      {
    //        "$binary": {
    //          "base64": "MTIzX2Zyb250LmpwZw==",
    //          "subType": "00"
    //        }
    //      }
    //    ]
    //  },
    //  "Title": "Cycle_1234_new",
    //  "InPublication": false,
    //  "ColorBytes": {
    //    "$binary": {
    //      "base64": "QmxhY2s=",
    //      "subType": "00"
    //    }
    //  },
    //  "ISBN": "111-1111111111",
    //  "NestedList1": [
    //    -485.34,
    //    "1234abcd",
    //    [
    //      "xyz0123",
    //      {
    //        "InPublication": false,
    //        "BinaryTitleSet": {
    //          "$set": [
    //            {
    //              "$binary": {
    //                "base64": "Qm9vayAxMDExIFRpdGxlIEJpbmFyeQ==",
    //                "subType": "00"
    //              }
    //            },
    //            {
    //              "$binary": {
    //                "base64": "Qm9vayAxMDEwIFRpdGxlIEJpbmFyeQ==",
    //                "subType": "00"
    //              }
    //            },
    //            {
    //              "$binary": {
    //                "base64": "Qm9vayAxMTExIFRpdGxlIEJpbmFyeQ==",
    //                "subType": "00"
    //              }
    //            }
    //          ]
    //        },
    //        "ISBN": "111-1111111122",
    //        "IdSet": {
    //          "$set": [
    //            20576024,
    //            -39457860.486939475,
    //            204850.69703847595,
    //            4.86906704836275e+21,
    //            19306873
    //          ]
    //        },
    //        "Title": "Book 101 Title",
    //        "Id": 101.01,
    //        "TitleSet2": {
    //          "$set": [
    //            "Book 1201 Title",
    //            "Book 1111 Title",
    //            "Book 1200 Title"
    //          ]
    //        }
    //      }
    //    ]
    //  ],
    //  "NestedMap1": {
    //    "InPublication": false,
    //    "ISBN": "111-1111111111",
    //    "NestedMap2": {
    //      "NList": [
    //        12.22,
    //        -0.00234,
    //        null
    //      ],
    //      "ISBN": "111-1111111111999",
    //      "Title": "Book 10122 Title",
    //      "Id": -12243.78,
    //      "NewID": "12345"
    //    },
    //    "Id": 101.01,
    //    "NList1": [
    //      {
    //        "$set": [
    //          "Updated_set_01",
    //          "Updated_set_02"
    //        ]
    //      },
    //      -0.00234
    //    ],
    //    "NSet1": {
    //      "$set": [
    //        123.45,
    //        9586.7778,
    //        -124,
    //        10238
    //      ]
    //    },
    //    "ColorList": [
    //      "Black",
    //      {
    //        "$binary": {
    //          "base64": "V2hpdGU=",
    //          "subType": "00"
    //        }
    //      },
    //      "Silver"
    //    ],
    //    "AddedId": 10
    //  },
    //  "attr_6": {
    //    "n_attr_0": "str_val_0",
    //    "n_attr_1": 1295.03,
    //    "n_attr_2": {
    //      "$binary": {
    //        "base64": "MjA0OHU1bmJsd2plaVdGR1RIKDRiZjkzMA==",
    //        "subType": "00"
    //      }
    //    },
    //    "n_attr_3": true,
    //    "n_attr_4": null
    //  },
    //  "attr_5": [
    //    1234,
    //    "str001",
    //    {
    //      "$binary": {
    //        "base64": "AAECAwQF",
    //        "subType": "00"
    //      }
    //    }
    //  ],
    //  "NestedList12": [
    //    -485.34,
    //    "1234abcd",
    //    [
    //      {
    //        "$set": [
    //          "xyz01234",
    //          "xyz0123",
    //          "abc01234"
    //        ]
    //      },
    //      {
    //        "$set": [
    //          {
    //            "$binary": {
    //              "base64": "dmFsMDE=",
    //              "subType": "00"
    //            }
    //          },
    //          {
    //            "$binary": {
    //              "base64": "dmFsMDM=",
    //              "subType": "00"
    //            }
    //          },
    //          {
    //            "$binary": {
    //              "base64": "dmFsMDI=",
    //              "subType": "00"
    //            }
    //          },
    //          {
    //            "$binary": {
    //              "base64": "dmFsMDQ=",
    //              "subType": "00"
    //            }
    //          }
    //        ]
    //      },
    //      {
    //        "$set": [
    //          -234.56,
    //          123,
    //          93756.93475960549,
    //          293755723028458.6
    //        ]
    //      }
    //    ]
    //  ],
    //  "Id": "12345",
    //  "attr_1": 1295.03,
    //  "attr_0": "str_val_0",
    //  "RelatedItems": {
    //    "$set": [
    //      123.0948,
    //      -485.45582904,
    //      1234,
    //      0.111
    //    ]
    //  },
    //  "Id1": {
    //    "$binary": {
    //      "base64": "SURfMTAx",
    //      "subType": "00"
    //    }
    //  },
    //  "AddedId": 10
    //}
    return BsonDocument.parse(json);
  }

  private static RawBsonDocument getUpdatedDocument2() {
    String json = "{\n" +
            "  \"Pictures\" : {\n" +
            "    \"$set\" : [ \"123_rear.jpg\", \"xyz5@_rear.jpg\", \"xyz_front.jpg\", \"xyz_rear.jpg\", \"123_front.jpg\", \"1235@_rear.jpg\" ]\n" +
            "  },\n" +
            "  \"PictureBinarySet\" : {\n" +
            "    \"$set\" : [ {\n" +
            "      \"$binary\" : {\n" +
            "        \"base64\" : \"MTIzYWJjX3JlYXIuanBn\",\n" +
            "        \"subType\" : \"00\"\n" +
            "      }\n" +
            "    }, {\n" +
            "      \"$binary\" : {\n" +
            "        \"base64\" : \"eHl6X3JlYXIuanBn\",\n" +
            "        \"subType\" : \"00\"\n" +
            "      }\n" +
            "    }, {\n" +
            "      \"$binary\" : {\n" +
            "        \"base64\" : \"eHl6YWJjX3JlYXIuanBn\",\n" +
            "        \"subType\" : \"00\"\n" +
            "      }\n" +
            "    }, {\n" +
            "      \"$binary\" : {\n" +
            "        \"base64\" : \"MTIzX2Zyb250LmpwZw==\",\n" +
            "        \"subType\" : \"00\"\n" +
            "      }\n" +
            "    } ]\n" +
            "  },\n" +
            "  \"Title\" : \"Cycle_1234_new\",\n" +
            "  \"InPublication\" : false,\n" +
            "  \"ColorBytes\" : {\n" +
            "    \"$binary\" : {\n" +
            "      \"base64\" : \"QmxhY2s=\",\n" +
            "      \"subType\" : \"00\"\n" +
            "    }\n" +
            "  },\n" +
            "  \"ISBN\" : \"111-1111111111\",\n" +
            "  \"NestedList1\" : [ -473.11999999999995, \"1234abcd\", [ \"xyz0123\", {\n" +
            "    \"InPublication\" : false,\n" +
            "    \"BinaryTitleSet\" : {\n" +
            "      \"$set\" : [ {\n" +
            "        \"$binary\" : {\n" +
            "          \"base64\" : \"Qm9vayAxMDExIFRpdGxlIEJpbmFyeQ==\",\n" +
            "          \"subType\" : \"00\"\n" +
            "        }\n" +
            "      }, {\n" +
            "        \"$binary\" : {\n" +
            "          \"base64\" : \"Qm9vayAxMDEwIFRpdGxlIEJpbmFyeQ==\",\n" +
            "          \"subType\" : \"00\"\n" +
            "        }\n" +
            "      }, {\n" +
            "        \"$binary\" : {\n" +
            "          \"base64\" : \"Qm9vayAxMTExIFRpdGxlIEJpbmFyeQ==\",\n" +
            "          \"subType\" : \"00\"\n" +
            "        }\n" +
            "      } ]\n" +
            "    },\n" +
            "    \"ISBN\" : \"111-1111111122\",\n" +
            "    \"IdSet\" : {\n" +
            "      \"$set\" : [ 20576024, -3.9457860486939475E7, 204850.69703847595, 4.86906704836275E21, 19306873 ]\n" +
            "    },\n" +
            "    \"Title\" : \"Book 101 Title\",\n" +
            "    \"Id\" : 101.01,\n" +
            "    \"TitleSet2\" : {\n" +
            "      \"$set\" : [ \"Book 1201 Title\", \"Book 1111 Title\", \"Book 1200 Title\" ]\n" +
            "    }\n" +
            "  } ], null, true ],\n" +
            "  \"NestedMap1\" : {\n" +
            "    \"InPublication\" : false,\n" +
            "    \"ISBN\" : \"111-1111111111\",\n" +
            "    \"NestedMap2\" : {\n" +
            "      \"NList\" : [ 12.22, -0.00234, null ],\n" +
            "      \"ISBN\" : \"111-1111111111999\",\n" +
            "      \"Title\" : \"Book 10122 Title\",\n" +
            "      \"Id\" : -12243.78,\n" +
            "      \"NewID\" : \"12345\"\n" +
            "    },\n" +
            "    \"Id\" : 101.01,\n" +
            "    \"NList1\" : [ {\n" +
            "      \"$set\" : [ \"Updated_set_01\", \"Updated_set_02\" ]\n" +
            "    }, -0.00234 ],\n" +
            "    \"NSet1\" : {\n" +
            "      \"$set\" : [ 123.45, 9586.7778, -124, 10238 ]\n" +
            "    },\n" +
            "    \"ColorList\" : [ \"Black\", {\n" +
            "      \"$binary\" : {\n" +
            "        \"base64\" : \"V2hpdGU=\",\n" +
            "        \"subType\" : \"00\"\n" +
            "      }\n" +
            "    }, \"Silver\" ],\n" +
            "    \"AddedId\" : 10\n" +
            "  },\n" +
            "  \"attr_6\" : {\n" +
            "    \"n_attr_0\" : \"str_val_0\",\n" +
            "    \"n_attr_1\" : 1295.03,\n" +
            "    \"n_attr_2\" : {\n" +
            "      \"$binary\" : {\n" +
            "        \"base64\" : \"MjA0OHU1bmJsd2plaVdGR1RIKDRiZjkzMA==\",\n" +
            "        \"subType\" : \"00\"\n" +
            "      }\n" +
            "    },\n" +
            "    \"n_attr_3\" : true,\n" +
            "    \"n_attr_4\" : null\n" +
            "  },\n" +
            "  \"attr_5\" : [ 1224, \"str001\", {\n" +
            "    \"$binary\" : {\n" +
            "      \"base64\" : \"AAECAwQF\",\n" +
            "      \"subType\" : \"00\"\n" +
            "    }\n" +
            "  } ],\n" +
            "  \"NestedList12\" : [ -485.34, \"1234abcd\", [ {\n" +
            "    \"$set\" : [ \"xyz01234\", \"xyz0123\", \"abc01234\" ]\n" +
            "  }, {\n" +
            "    \"$set\" : [ {\n" +
            "      \"$binary\" : {\n" +
            "        \"base64\" : \"dmFsMDE=\",\n" +
            "        \"subType\" : \"00\"\n" +
            "      }\n" +
            "    }, {\n" +
            "      \"$binary\" : {\n" +
            "        \"base64\" : \"dmFsMDM=\",\n" +
            "        \"subType\" : \"00\"\n" +
            "      }\n" +
            "    }, {\n" +
            "      \"$binary\" : {\n" +
            "        \"base64\" : \"dmFsMDI=\",\n" +
            "        \"subType\" : \"00\"\n" +
            "      }\n" +
            "    }, {\n" +
            "      \"$binary\" : {\n" +
            "        \"base64\" : \"dmFsMDQ=\",\n" +
            "        \"subType\" : \"00\"\n" +
            "      }\n" +
            "    } ]\n" +
            "  } ] ],\n" +
            "  \"Id\" : \"12345\",\n" +
            "  \"attr_1\" : 1295.03,\n" +
            "  \"attr_0\" : \"str_val_0\",\n" +
            "  \"RelatedItems\" : {\n" +
            "    \"$set\" : [ 123.0948, -485.45582904, 1234, 0.111 ]\n" +
            "  },\n" +
            "  \"Id1\" : \"12345\",\n" +
            "  \"AddedId\" : 10\n" +
            "}";
    //{
    //  "Pictures" : {
    //    "$set" : [ "123_rear.jpg", "xyz5@_rear.jpg", "xyz_front.jpg", "xyz_rear.jpg", "123_front.jpg", "1235@_rear.jpg" ]
    //  },
    //  "PictureBinarySet" : {
    //    "$set" : [ {
    //      "$binary" : {
    //        "base64" : "MTIzYWJjX3JlYXIuanBn",
    //        "subType" : "00"
    //      }
    //    }, {
    //      "$binary" : {
    //        "base64" : "eHl6X3JlYXIuanBn",
    //        "subType" : "00"
    //      }
    //    }, {
    //      "$binary" : {
    //        "base64" : "eHl6YWJjX3JlYXIuanBn",
    //        "subType" : "00"
    //      }
    //    }, {
    //      "$binary" : {
    //        "base64" : "MTIzX2Zyb250LmpwZw==",
    //        "subType" : "00"
    //      }
    //    } ]
    //  },
    //  "Title" : "Cycle_1234_new",
    //  "InPublication" : false,
    //  "ColorBytes" : {
    //    "$binary" : {
    //      "base64" : "QmxhY2s=",
    //      "subType" : "00"
    //    }
    //  },
    //  "ISBN" : "111-1111111111",
    //  "NestedList1" : [ -473.11999999999995, "1234abcd", [ "xyz0123", {
    //    "InPublication" : false,
    //    "BinaryTitleSet" : {
    //      "$set" : [ {
    //        "$binary" : {
    //          "base64" : "Qm9vayAxMDExIFRpdGxlIEJpbmFyeQ==",
    //          "subType" : "00"
    //        }
    //      }, {
    //        "$binary" : {
    //          "base64" : "Qm9vayAxMDEwIFRpdGxlIEJpbmFyeQ==",
    //          "subType" : "00"
    //        }
    //      }, {
    //        "$binary" : {
    //          "base64" : "Qm9vayAxMTExIFRpdGxlIEJpbmFyeQ==",
    //          "subType" : "00"
    //        }
    //      } ]
    //    },
    //    "ISBN" : "111-1111111122",
    //    "IdSet" : {
    //      "$set" : [ 20576024, -3.9457860486939475E7, 204850.69703847595, 4.86906704836275E21, 19306873 ]
    //    },
    //    "Title" : "Book 101 Title",
    //    "Id" : 101.01,
    //    "TitleSet2" : {
    //      "$set" : [ "Book 1201 Title", "Book 1111 Title", "Book 1200 Title" ]
    //    }
    //  } ], null, true ],
    //  "NestedMap1" : {
    //    "InPublication" : false,
    //    "ISBN" : "111-1111111111",
    //    "NestedMap2" : {
    //      "NList" : [ 12.22, -0.00234, null ],
    //      "ISBN" : "111-1111111111999",
    //      "Title" : "Book 10122 Title",
    //      "Id" : -12243.78,
    //      "NewID" : "12345"
    //    },
    //    "Id" : 101.01,
    //    "NList1" : [ {
    //      "$set" : [ "Updated_set_01", "Updated_set_02" ]
    //    }, -0.00234 ],
    //    "NSet1" : {
    //      "$set" : [ 123.45, 9586.7778, -124, 10238 ]
    //    },
    //    "ColorList" : [ "Black", {
    //      "$binary" : {
    //        "base64" : "V2hpdGU=",
    //        "subType" : "00"
    //      }
    //    }, "Silver" ],
    //    "AddedId" : 10
    //  },
    //  "attr_6" : {
    //    "n_attr_0" : "str_val_0",
    //    "n_attr_1" : 1295.03,
    //    "n_attr_2" : {
    //      "$binary" : {
    //        "base64" : "MjA0OHU1bmJsd2plaVdGR1RIKDRiZjkzMA==",
    //        "subType" : "00"
    //      }
    //    },
    //    "n_attr_3" : true,
    //    "n_attr_4" : null
    //  },
    //  "attr_5" : [ 1224, "str001", {
    //    "$binary" : {
    //      "base64" : "AAECAwQF",
    //      "subType" : "00"
    //    }
    //  } ],
    //  "NestedList12" : [ -485.34, "1234abcd", [ {
    //    "$set" : [ "xyz01234", "xyz0123", "abc01234" ]
    //  }, {
    //    "$set" : [ {
    //      "$binary" : {
    //        "base64" : "dmFsMDE=",
    //        "subType" : "00"
    //      }
    //    }, {
    //      "$binary" : {
    //        "base64" : "dmFsMDM=",
    //        "subType" : "00"
    //      }
    //    }, {
    //      "$binary" : {
    //        "base64" : "dmFsMDI=",
    //        "subType" : "00"
    //      }
    //    }, {
    //      "$binary" : {
    //        "base64" : "dmFsMDQ=",
    //        "subType" : "00"
    //      }
    //    } ]
    //  } ] ],
    //  "Id" : "12345",
    //  "attr_1" : 1295.03,
    //  "attr_0" : "str_val_0",
    //  "RelatedItems" : {
    //    "$set" : [ 123.0948, -485.45582904, 1234, 0.111 ]
    //  },
    //  "Id1" : "12345",
    //  "AddedId" : 10
    //}
    return RawBsonDocument.parse(json);
  }

}
