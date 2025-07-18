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

import java.util.ArrayList;
import java.util.List;

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.junit.Test;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.util.bson.BsonConditionInvalidArgumentException;
import org.apache.phoenix.expression.util.bson.DocumentComparisonExpressionUtils;
import org.apache.phoenix.expression.util.bson.SQLComparisonExpressionUtils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for BSON Condition Expression Utility.
 */
public class ComparisonExpressionUtilsTest {

  @Test
  public void testSQLComparisonExpression1() {
    RawBsonDocument rawBsonDocument = getDocumentValue();
    RawBsonDocument compareValues = getCompareValDocument();

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "(field_exists(Id) OR field_not_exists(Title))", rawBsonDocument, compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "((field_not_exists(Id) AND field_not_exists(Title1)) OR field_exists(ISBN2))"
                    + " OR ((Id <> #Title) AND ((InPublication = InPublication) OR ((ISBN = :ISBN)"
                    + " AND (Title = #Title))))", rawBsonDocument, compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "((field_exists(NestedMap1.ISBN) AND field_not_exists(NestedMap1.NList1[3])))",
            rawBsonDocument, compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "NestedMap1.Id = $Id AND (NestedMap1.InPublication = InPublication)", rawBsonDocument,
            compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "((attribute_exists(0) AND attribute_exists(1)) AND attribute_not_exists(#3)) AND ((" +
                    "(attribute_not_exists(0) AND attribute_not_exists(1)) OR attribute_exists(#0)" +
                    ") OR (#_ = :0))", rawBsonDocument, compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "((NestedMap1.Id = $Id) AND ((NestedMap1.InPublication[0] = InPublication) OR "
                    + "((ISBN[0] = :ISBN) AND (Title = #Title))) OR "
                    + "(NestedMap1.NList1[0] = #NMap1_NList1))", rawBsonDocument, compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "((field_not_exists(Id) AND field_not_exists(Title1)) OR field_exists(ISBN2))"
                    + " OR ((NestedMap1.Id = $Id) AND ((NestedMap1.InPublication = InPublication)"
                    + " OR ((ISBN = :ISBN) AND (Title = #Title))))", rawBsonDocument,
            compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "NestedList1[0] <= $NestedList1_485 AND NestedList1[1] > #NestedList1_1 AND "
                    + "NestedList1[2][0] >= #NestedList1_xyz0123 AND "
                    + "NestedList1[2][1].Id < $Id1 AND IdS < $Ids1 AND Id2 > $Id2 AND NestedMap1"
                    + ".NList1[2] > #NestedMap1_NList1_3", rawBsonDocument, compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "NestedList1[0] <= $NestedList1_485 AND NestedList1[1] >= #NestedList1_1 AND "
                    + "NestedList1[2][0] >= #NestedList1_xyz0123 AND "
                    + "NestedList1[2][1].Id <= $Id1 AND IdS <= $Ids1 AND Id2 >= $Id2 AND"
                    + " NestedMap1.NList1[2] >= #NestedMap1_NList1_3", rawBsonDocument,
            compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "NestedList1[0] >= $NestedList1_4850 AND NestedList1[1] < #NestedList1_10 AND"
                    + " NestedList1[2][0] >= #NestedList1_xyz0123 AND "
                    + "NestedList1[2][1].Id > $Id10 AND IdS > $Ids10 AND Id2 < $Id20 AND "
                    + "NestedMap1.NList1[2] < $NestedMap1_NList1_30", rawBsonDocument,
            compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "NestedList1[0] >= $NestedList1_4850 AND NestedList1[1] <= #NestedList1_10 AND "
                    + "NestedList1[2][0] >= #NestedList1_xyz0123 AND "
                    + "NestedList1[2][1].Id >= $Id10 AND IdS >= $Ids10 AND Id2 <= $Id20 AND "
                    + "NestedMap1.NList1[2] <= $NestedMap1_NList1_30 AND "
                    + "NestedMap1.NList1[2] <> $NestedMap1_NList1_30", rawBsonDocument,
            compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "NestedList1[0] >= $NestedList1_4850 AND NestedList1[1] <= #NestedList1_10 AND "
                    + "NestedList1[2][0] >= #NestedList1_xyz0123 AND "
                    + "NestedList1[2][1].Id >= $Id10 AND IdS >= $Ids10 AND Id2 <= $Id20 AND "
                    + "NestedMap1.NList1[2] <= $NestedMap1_NList1_30 AND "
                    + "(NestedMap1.NList1[2] = $NestedMap1_NList1_30 OR NestedList1[0] BETWEEN "
                    + "$NestedList1_4850 AND $Id2)", rawBsonDocument, compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "NestedList1[0] >= $NestedList1_4850 AND NestedList1[1] <= #NestedList1_10 AND "
                    + "NestedList1[2][0] >= #NestedList1_xyz0123 AND "
                    + "NestedMap1.NList1[0] IN ($Id, $Id1, $Id20, #NMap1_NList1) AND "
                    + "NestedMap1.NList1[2] <= $NestedMap1_NList1_30 AND "
                    + "(NestedMap1.NList1[2] = $NestedMap1_NList1_30 OR NestedList1[0] BETWEEN "
                    + "$NestedList1_4850 AND $Id2)", rawBsonDocument, compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "NestedList1[0] >= $NestedList1_4850 AND NestedList1[1] <= #NestedList1_10 AND "
                    + "NestedList1[2][0] >= #NestedList1_xyz0123 AND "
                    + "NestedMap1.NList1[0] IN ($Id,  $Id1, $Id20, #NMap1_NList1) AND "
                    + "NestedMap1.NList1[2] <= $NestedMap1_NList1_30 AND "
                    + "(NestedMap1.NList1[2] = $NestedMap1_NList1_30 OR "
                    + " NestedList1[0] BETWEEN $NestedList1_4850 AND $Id2)"
                    + " AND NOT NestedMap1.InPublication IN ($Id, $Id1, $Id20, $Id21)",
            rawBsonDocument,
            compareValues));
  }

  /**
   * Test that directly uses executable expression to reduce the dependency on pattern-matcher.
   */
  @Test
  public void testSQLComparisonExpression2() {
    RawBsonDocument rawBsonDocument = getDocumentValue();
    RawBsonDocument compareValues = getCompareValDocument();

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "(field_not_exists(Id) OR field_not_exists(Title))", rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "((field_not_exists(Id) AND field_not_exists(Title1)) OR field_exists(ISBN2))"
                    + " OR ((Id = #Title) AND ((InPublication = InPublication) OR ((ISBN = :ISBN)"
                    + " AND (Title = #Title))))", rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "((field_exists(NestedMap1.ISBN) AND field_exists(NestedMap1.NList1[3])))",
            rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "NestedMap1.Id = $Id AND (NestedMap1.InPublication <> InPublication)", rawBsonDocument,
            compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "((NestedMap1.Id = $Id) AND ((NestedMap1.InPublication[0] = InPublication) OR "
                    + "((ISBN[0] = :ISBN) AND (Title = #Title))) OR "
                    + "(NestedMap1.NList1[0] <> #NMap1_NList1))", rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "((field_not_exists(Id) AND field_not_exists(Title1)) OR field_exists(ISBN2))"
                    + " OR ((NestedMap1.Id = $Id) AND ((NestedMap1.InPublication <> InPublication)"
                    + " OR NOT ((ISBN = :ISBN) AND (Title = #Title))))", rawBsonDocument,
            compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "NestedList1[0] <= $NestedList1_485 AND NestedList1[1] > #NestedList1_1 AND "
                    + "NestedList1[2][0] >= #NestedList1_xyz0123 AND "
                    + "NestedList1[2][1].Id < $Id1 AND IdS < $Ids1 AND Id2 > $Id2 AND NestedMap1"
                    + ".NList1[2] < #NestedMap1_NList1_3", rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "NestedList1[0] <= $NestedList1_485 AND NestedList1[1] >= #NestedList1_1 AND "
                    + "NestedList1[2][0] >= #NestedList1_xyz0123 AND "
                    +
                    "NestedList1[2][1].Id <= $Id1 AND IdS <= $Ids1 AND Id2 >= $Id2 AND NestedMap1" +
                    ".NList1[2] < #NestedMap1_NList1_3", rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "NestedList1[0] >= $NestedList1_4850 AND NestedList1[1] < #NestedList1_10 AND "
                    + "NestedList1[2][0] >= #NestedList1_xyz0123 AND "
                    + "NestedList1[2][1].Id > $Id10 AND IdS > $Ids10 AND Id2 < $Id20 AND NestedMap1"
                    + ".NList1[2] >= $NestedMap1_NList1_30", rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "NestedList1[0] >= $NestedList1_4850 AND NestedList1[1] <= #NestedList1_10 AND "
                    + "NestedList1[2][0] >= #NestedList1_xyz0123 AND "
                    + "NestedList1[2][1].Id >= $Id10 AND IdS >= $Ids10 AND Id2 <= $Id20 AND "
                    + "NestedMap1.NList1[2] > $NestedMap1_NList1_30 AND "
                    + "NestedMap1.NList1[2] <> $NestedMap1_NList1_30", rawBsonDocument,
            compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "NestedList1[0] >= $NestedList1_4850 AND NestedList1[1] <= #NestedList1_10 AND "
                    + "NestedList1[2][0] >= #NestedList1_xyz0123 AND "
                    + "NestedList1[2][1].Id >= $Id10 AND IdS >= $Ids10 AND Id2 <= $Id20 AND "
                    + "NestedMap1.NList1[2] <= $NestedMap1_NList1_30 AND "
                    + "(NestedMap1.NList1[2] = $NestedMap1_NList1_30 OR NOT NestedList1[0] BETWEEN "
                    + "$NestedList1_4850 AND $Id2)", rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "NestedList1[0] >= $NestedList1_4850 AND NestedList1[1] <= #NestedList1_10 AND "
                    + "NestedList1[2][0] >= #NestedList1_xyz0123 AND "
                    + "NestedMap1.NList1[0] NOT IN ($Id, $Id1, $Id20, #NMap1_NList1) AND "
                    + "NestedMap1.NList1[2] <= $NestedMap1_NList1_30 AND "
                    + "(NestedMap1.NList1[2] = $NestedMap1_NList1_30 OR NestedList1[0] BETWEEN "
                    + "$NestedList1_4850 AND $Id2)", rawBsonDocument, compareValues));


    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "NestedList1[0] >= $NestedList1_4850 AND NestedList1[1] <= #NestedList1_10 AND "
                    + "NestedList1[2][0] >= #NestedList1_xyz0123 AND "
                    + "NestedMap1.NList1[0] IN ($Id,  $Id1, $Id20, #NMap1_NList1) AND "
                    + "NestedMap1.NList1[2] <= $NestedMap1_NList1_30 AND "
                    + "(NestedMap1.NList1[2] <> $NestedMap1_NList1_30 OR NestedList1[0]"
                    + " NOT BETWEEN $NestedList1_4850 AND $Id2)"
                    + " AND NOT NestedMap1.InPublication IN ($Id, $Id1, $Id20, $Id21)",
            rawBsonDocument, compareValues));
  }

  @Test
  public void testDocumentComparisonExpression1() {
    RawBsonDocument rawBsonDocument = getDocumentValue();
    BsonDocument expressionDocument = new BsonDocument();
    List<BsonValue> orList = new ArrayList<>();
    orList.add(new BsonDocument().append("Id",
        new BsonDocument().append("$exists", new BsonBoolean(true))));
    orList.add(new BsonDocument().append("Title",
        new BsonDocument().append("$exists", new BsonBoolean(false))));
    expressionDocument.append("$or", new BsonArray(orList));

    // Condition Expression:
    //{
    //  "$or": [
    //    {
    //      "Id": {
    //        "$exists": true
    //      }
    //    },
    //    {
    //      "Title": {
    //        "$exists": false
    //      }
    //    }
    //  ]
    //}

    assertTrue(DocumentComparisonExpressionUtils.evaluateConditionExpression(rawBsonDocument,
        expressionDocument));

    expressionDocument = new BsonDocument();
    BsonArray orListArray = new BsonArray();
    BsonArray orList1 = new BsonArray();
    BsonDocument orDoc1 = new BsonDocument();
    BsonDocument andDoc1 = new BsonDocument();
    BsonArray andList1 = new BsonArray();
    andList1.add(new BsonDocument().append("Id",
        new BsonDocument().append("$exists", new BsonBoolean(false))));
    andList1.add(new BsonDocument().append("Title1",
        new BsonDocument().append("$exists", new BsonBoolean(false))));
    andDoc1.append("$and", andList1);
    orList1.add(andDoc1);
    orList1.add(new BsonDocument().append("ISBN2",
        new BsonDocument().append("$exists", new BsonBoolean(true))));
    orDoc1.append("$or", orList1);
    orListArray.add(orDoc1);

    BsonArray andList2 = new BsonArray();
    BsonDocument andDoc2 = new BsonDocument();
    andList2.add(new BsonDocument().append("Id",
        new BsonDocument().append("$ne", new BsonString("Book 101 Title"))));

    BsonArray orList2 = new BsonArray();
    BsonDocument orDoc2 = new BsonDocument();
    orList2.add(new BsonDocument().append("InPublication",
        new BsonDocument().append("$eq", new BsonBoolean(false))));

    BsonArray andList3 = new BsonArray();
    BsonDocument andDoc3 = new BsonDocument();
    andList3.add(new BsonDocument().append("ISBN",
        new BsonDocument().append("$eq", new BsonString("111-1111111111"))));
    andList3.add(new BsonDocument().append("Title",
        new BsonDocument().append("$eq", new BsonString("Book 101 Title"))));
    andDoc3.append("$and", andList3);

    orList2.add(andDoc3);
    orDoc2.append("$or", orList2);

    andList2.add(orDoc2);
    andDoc2.append("$and", andList2);
    orListArray.add(andDoc2);

    expressionDocument.append("$or", orListArray);

    // Condition Expression:
    //{
    //  "$or": [
    //    {
    //      "$or": [
    //        {
    //          "$and": [
    //            {
    //              "Id": {
    //                "$exists": false
    //              }
    //            },
    //            {
    //              "Title1": {
    //                "$exists": false
    //              }
    //            }
    //          ]
    //        },
    //        {
    //          "ISBN2": {
    //            "$exists": true
    //          }
    //        }
    //      ]
    //    },
    //    {
    //      "$and": [
    //        {
    //          "Id": {
    //            "$ne": "Book 101 Title"
    //          }
    //        },
    //        {
    //          "$or": [
    //            {
    //              "InPublication": {
    //                "$eq": false
    //              }
    //            },
    //            {
    //              "$and": [
    //                {
    //                  "ISBN": {
    //                    "$eq": "111-1111111111"
    //                  }
    //                },
    //                {
    //                  "Title": {
    //                    "$eq": "Book 101 Title"
    //                  }
    //                }
    //              ]
    //            }
    //          ]
    //        }
    //      ]
    //    }
    //  ]
    //}

    assertTrue(DocumentComparisonExpressionUtils.evaluateConditionExpression(rawBsonDocument,
        expressionDocument));

    expressionDocument = new BsonDocument();
    andList1 = new BsonArray();
    andList1.add(new BsonDocument().append("NestedMap1.ISBN",
        new BsonDocument().append("$exists", new BsonBoolean(true))));
    andList1.add(new BsonDocument().append("NestedMap1.NList1[3]",
        new BsonDocument().append("$exists", new BsonBoolean(false))));
    expressionDocument.append("$and", andList1);

    // Condition Expression:
    //{
    //  "$and": [
    //    {
    //      "NestedMap1.ISBN": {
    //        "$exists": true
    //      }
    //    },
    //    {
    //      "NestedMap1.NList1[3]": {
    //        "$exists": false
    //      }
    //    }
    //  ]
    //}

    assertTrue(DocumentComparisonExpressionUtils.evaluateConditionExpression(rawBsonDocument,
        expressionDocument));

    expressionDocument = new BsonDocument();
    andList1 = new BsonArray();
    andList1.add(new BsonDocument().append("NestedMap1.Id",
        new BsonDocument().append("$eq", new BsonDouble(101.01))));
    andList1.add(new BsonDocument().append("NestedMap1.InPublication",
        new BsonDocument().append("$eq", new BsonBoolean(false))));
    expressionDocument.append("$and", andList1);

    // Condition Expression:
    //{
    //  "$and": [
    //    {
    //      "NestedMap1.Id": {
    //        "$eq": 101.01
    //      }
    //    },
    //    {
    //      "NestedMap1.InPublication": {
    //        "$eq": false
    //      }
    //    }
    //  ]
    //}
    assertTrue(DocumentComparisonExpressionUtils.evaluateConditionExpression(rawBsonDocument,
        expressionDocument));

    expressionDocument = new BsonDocument();

    andList1 = new BsonArray();
    andDoc1 = new BsonDocument();
    andList1.add(new BsonDocument().append("NestedMap1.Id",
        new BsonDocument().append("$eq", new BsonDouble(101.01))));

    orList1 = new BsonArray();
    orDoc1 = new BsonDocument();
    orList1.add(new BsonDocument().append("NestedMap1.InPublication[0]",
        new BsonDocument().append("$eq", new BsonBoolean(false))));

    andList2 = new BsonArray();
    andDoc2 = new BsonDocument();
    andList2.add(new BsonDocument().append("ISBN[0]",
        new BsonDocument().append("$eq", new BsonString("111-1111111111"))));
    andList2.add(new BsonDocument().append("Title",
        new BsonDocument().append("$eq", new BsonString("Book 101 Title"))));
    andDoc2.append("$and", andList2);

    orList1.add(andDoc2);
    orDoc1.append("$or", orList1);

    andList1.add(orDoc1);
    andDoc1.append("$and", andList1);

    orList2 = new BsonArray();
    orDoc2 = new BsonDocument();

    orList2.add(andDoc1);
    orList2.add(new BsonDocument().append("NestedMap1.NList1[0]",
        new BsonDocument().append("$eq", new BsonString("NListVal01"))));

    expressionDocument.append("$or", orList2);

    // Condition Expression:
    //{
    //  "$or": [
    //    {
    //      "$and": [
    //        {
    //          "NestedMap1.Id": {
    //            "$eq": 101.01
    //          }
    //        },
    //        {
    //          "$or": [
    //            {
    //              "NestedMap1.InPublication[0]": {
    //                "$eq": false
    //              }
    //            },
    //            {
    //              "$and": [
    //                {
    //                  "ISBN[0]": {
    //                    "$eq": "111-1111111111"
    //                  }
    //                },
    //                {
    //                  "Title": {
    //                    "$eq": "Book 101 Title"
    //                  }
    //                }
    //              ]
    //            }
    //          ]
    //        }
    //      ]
    //    },
    //    {
    //      "NestedMap1.NList1[0]": {
    //        "$eq": "NListVal01"
    //      }
    //    }
    //  ]
    //}
    assertTrue(DocumentComparisonExpressionUtils.evaluateConditionExpression(rawBsonDocument,
        expressionDocument));

    expressionDocument = new BsonDocument();
    orListArray = new BsonArray();

    andList1 = new BsonArray();
    andDoc1 = new BsonDocument();
    andList1.add(new BsonDocument().append("Id",
        new BsonDocument().append("$exists", new BsonBoolean(false))));
    andList1.add(new BsonDocument().append("Title1",
        new BsonDocument().append("$exists", new BsonBoolean(false))));
    andDoc1.append("$and", andList1);

    orList1 = new BsonArray();
    orDoc1 = new BsonDocument();

    orList1.add(andDoc1);
    orList1.add(new BsonDocument().append("ISBN2",
        new BsonDocument().append("$exists", new BsonBoolean(true))));
    orDoc1.append("$or", orList1);

    orListArray.add(orDoc1);

    andList2 = new BsonArray();
    andDoc2 = new BsonDocument();
    andList2.add(new BsonDocument().append("NestedMap1.Id",
        new BsonDocument().append("$eq", new BsonDouble(101.01))));

    orList2 = new BsonArray();
    orDoc2 = new BsonDocument();
    orList2.add(new BsonDocument().append("NestedMap1.InPublication",
        new BsonDocument().append("$eq", new BsonBoolean(false))));

    andList3 = new BsonArray();
    andDoc3 = new BsonDocument();

    andList3.add(new BsonDocument().append("ISBN",
        new BsonDocument().append("$eq", new BsonString("111-1111111111"))));
    andList3.add(new BsonDocument().append("Title",
        new BsonDocument().append("$eq", new BsonString("Book 101 Title"))));
    andDoc3.append("$and", andList3);

    orList2.add(andDoc3);
    orDoc2.append("$or", orList2);

    andList2.add(orDoc2);
    andDoc2.append("$and", andList2);

    orListArray.add(andDoc2);
    expressionDocument.append("$or", orListArray);

    // Condition Expression:
    //{
    //  "$or": [
    //    {
    //      "$or": [
    //        {
    //          "$and": [
    //            {
    //              "Id": {
    //                "$exists": false
    //              }
    //            },
    //            {
    //              "Title1": {
    //                "$exists": false
    //              }
    //            }
    //          ]
    //        },
    //        {
    //          "ISBN2": {
    //            "$exists": true
    //          }
    //        }
    //      ]
    //    },
    //    {
    //      "$and": [
    //        {
    //          "NestedMap1.Id": {
    //            "$eq": 101.01
    //          }
    //        },
    //        {
    //          "$or": [
    //            {
    //              "NestedMap1.InPublication": {
    //                "$eq": false
    //              }
    //            },
    //            {
    //              "$and": [
    //                {
    //                  "ISBN": {
    //                    "$eq": "111-1111111111"
    //                  }
    //                },
    //                {
    //                  "Title": {
    //                    "$eq": "Book 101 Title"
    //                  }
    //                }
    //              ]
    //            }
    //          ]
    //        }
    //      ]
    //    }
    //  ]
    //}

    assertTrue(DocumentComparisonExpressionUtils.evaluateConditionExpression(rawBsonDocument,
        expressionDocument));

    expressionDocument = new BsonDocument();
    andList1 = new BsonArray();
    andList1.add(new BsonDocument().append("NestedList1[0]",
        new BsonDocument().append("$lte", new BsonDouble(-485.33))));
    andList1.add(new BsonDocument().append("NestedList1[1]",
        new BsonDocument().append("$gt", new BsonString("1234abcc"))));
    andList1.add(new BsonDocument().append("NestedList1[2][0]",
        new BsonDocument().append("$gte", new BsonString("xyz0123"))));
    andList1.add(new BsonDocument().append("NestedList1[2][1].Id",
        new BsonDocument().append("$lt", new BsonInt32(120))));
    andList1.add(new BsonDocument().append("IdS",
        new BsonDocument().append("$lt", new BsonString("12"))));
    andList1.add(new BsonDocument().append("Id2",
        new BsonDocument().append("$gt", new BsonInt32(12))));
    andList1.add(new BsonDocument().append("NestedMap1.NList1[2]",
        new BsonDocument().append("$gt", new BsonBinary(Bytes.toBytes("Whit")))));

    expressionDocument.append("$and", andList1);

    // Condition Expression:
    //{
    //  "$and": [
    //    {
    //      "NestedList1[0]": {
    //        "$lte": -485.33
    //      }
    //    },
    //    {
    //      "NestedList1[1]": {
    //        "$gt": "1234abcc"
    //      }
    //    },
    //    {
    //      "NestedList1[2][0]": {
    //        "$gte": "xyz0123"
    //      }
    //    },
    //    {
    //      "NestedList1[2][1].Id": {
    //        "$lt": 120
    //      }
    //    },
    //    {
    //      "IdS": {
    //        "$lt": "12"
    //      }
    //    },
    //    {
    //      "Id2": {
    //        "$gt": 12
    //      }
    //    },
    //    {
    //      "NestedMap1.NList1[2]": {
    //        "$gt": {
    //          "$binary": {
    //            "base64": "V2hpdA==",
    //            "subType": "00"
    //          }
    //        }
    //      }
    //    }
    //  ]
    //}

    assertTrue(DocumentComparisonExpressionUtils.evaluateConditionExpression(rawBsonDocument,
        expressionDocument));

    expressionDocument = new BsonDocument();
    andList1 = new BsonArray();
    andList1.add(new BsonDocument().append("NestedList1[0]",
        new BsonDocument().append("$lte", new BsonDouble(-485.33))));
    andList1.add(new BsonDocument().append("NestedList1[1]",
        new BsonDocument().append("$gte", new BsonString("1234abcc"))));
    andList1.add(new BsonDocument().append("NestedList1[2][0]",
        new BsonDocument().append("$gte", new BsonString("xyz0123"))));
    andList1.add(new BsonDocument().append("NestedList1[2][1].Id",
        new BsonDocument().append("$lte", new BsonInt32(120))));
    andList1.add(new BsonDocument().append("IdS",
        new BsonDocument().append("$lte", new BsonString("12"))));
    andList1.add(new BsonDocument().append("Id2",
        new BsonDocument().append("$gte", new BsonInt32(12))));
    andList1.add(new BsonDocument().append("NestedMap1.NList1[2]",
        new BsonDocument().append("$gte", new BsonBinary(Bytes.toBytes("Whit")))));

    expressionDocument.append("$and", andList1);

    // Condition Expression:
    //{
    //  "$and": [
    //    {
    //      "NestedList1[0]": {
    //        "$lte": -485.33
    //      }
    //    },
    //    {
    //      "NestedList1[1]": {
    //        "$gte": "1234abcc"
    //      }
    //    },
    //    {
    //      "NestedList1[2][0]": {
    //        "$gte": "xyz0123"
    //      }
    //    },
    //    {
    //      "NestedList1[2][1].Id": {
    //        "$lte": 120
    //      }
    //    },
    //    {
    //      "IdS": {
    //        "$lte": "12"
    //      }
    //    },
    //    {
    //      "Id2": {
    //        "$gte": 12
    //      }
    //    },
    //    {
    //      "NestedMap1.NList1[2]": {
    //        "$gte": {
    //          "$binary": {
    //            "base64": "V2hpdA==",
    //            "subType": "00"
    //          }
    //        }
    //      }
    //    }
    //  ]
    //}

    assertTrue(DocumentComparisonExpressionUtils.evaluateConditionExpression(rawBsonDocument,
        expressionDocument));

    expressionDocument = new BsonDocument();

    andList1 = new BsonArray();
    andList1.add(new BsonDocument().append("NestedList1[0]",
        new BsonDocument().append("$gte", new BsonDouble(-485.35))));
    andList1.add(new BsonDocument().append("NestedList1[1]",
        new BsonDocument().append("$lt", new BsonString("1234abce"))));
    andList1.add(new BsonDocument().append("NestedList1[2][0]",
        new BsonDocument().append("$gte", new BsonString("xyz0123"))));
    andList1.add(new BsonDocument().append("NestedList1[2][1].Id",
        new BsonDocument().append("$gt", new BsonInt64(101))));
    andList1.add(
        new BsonDocument().append("IdS", new BsonDocument().append("$gt", new BsonString("100"))));
    andList1.add(new BsonDocument().append("Id2",
        new BsonDocument().append("$lt", new BsonDouble(101.011))));
    andList1.add(new BsonDocument().append("NestedMap1.NList1[2]",
        new BsonDocument().append("$lt", new BsonBinary(Bytes.toBytes("Whitee")))));

    expressionDocument.append("$and", andList1);

    // Condition Expression:
    //{
    //  "$and": [
    //    {
    //      "NestedList1[0]": {
    //        "$gte": -485.35
    //      }
    //    },
    //    {
    //      "NestedList1[1]": {
    //        "$lt": "1234abce"
    //      }
    //    },
    //    {
    //      "NestedList1[2][0]": {
    //        "$gte": "xyz0123"
    //      }
    //    },
    //    {
    //      "NestedList1[2][1].Id": {
    //        "$gt": 101
    //      }
    //    },
    //    {
    //      "IdS": {
    //        "$gt": "100"
    //      }
    //    },
    //    {
    //      "Id2": {
    //        "$lt": 101.011
    //      }
    //    },
    //    {
    //      "NestedMap1.NList1[2]": {
    //        "$lt": {
    //          "$binary": {
    //            "base64": "V2hpdGVl",
    //            "subType": "00"
    //          }
    //        }
    //      }
    //    }
    //  ]
    //}
    assertTrue(DocumentComparisonExpressionUtils.evaluateConditionExpression(rawBsonDocument,
        expressionDocument));

    expressionDocument = new BsonDocument();

    andList1 = new BsonArray();
    andList1.add(new BsonDocument().append("NestedList1[0]",
        new BsonDocument().append("$gte", new BsonDouble(-485.35))));
    andList1.add(new BsonDocument().append("NestedList1[1]",
        new BsonDocument().append("$lte", new BsonString("1234abce"))));
    andList1.add(new BsonDocument().append("NestedList1[2][0]",
        new BsonDocument().append("$gte", new BsonString("xyz0123"))));
    andList1.add(new BsonDocument().append("NestedList1[2][1].Id",
        new BsonDocument().append("$gte", new BsonInt64(101))));
    andList1.add(new BsonDocument().append("IdS",
        new BsonDocument().append("$gte", new BsonString("100"))));
    andList1.add(new BsonDocument().append("Id2",
        new BsonDocument().append("$lte", new BsonDouble(101.011))));
    andList1.add(new BsonDocument().append("NestedMap1.NList1[2]",
        new BsonDocument().append("$lte", new BsonBinary(Bytes.toBytes("Whitee")))));
    andList1.add(new BsonDocument().append("NestedMap1.NList1[2]",
        new BsonDocument().append("$ne", new BsonBinary(Bytes.toBytes("Whitee")))));

    expressionDocument.append("$and", andList1);

    // Condition Expression:
    //{
    //  "$and": [
    //    {
    //      "NestedList1[0]": {
    //        "$gte": -485.35
    //      }
    //    },
    //    {
    //      "NestedList1[1]": {
    //        "$lte": "1234abce"
    //      }
    //    },
    //    {
    //      "NestedList1[2][0]": {
    //        "$gte": "xyz0123"
    //      }
    //    },
    //    {
    //      "NestedList1[2][1].Id": {
    //        "$gte": 101
    //      }
    //    },
    //    {
    //      "IdS": {
    //        "$gte": "100"
    //      }
    //    },
    //    {
    //      "Id2": {
    //        "$lte": 101.011
    //      }
    //    },
    //    {
    //      "NestedMap1.NList1[2]": {
    //        "$lte": {
    //          "$binary": {
    //            "base64": "V2hpdGVl",
    //            "subType": "00"
    //          }
    //        }
    //      }
    //    },
    //    {
    //      "NestedMap1.NList1[2]": {
    //        "$ne": {
    //          "$binary": {
    //            "base64": "V2hpdGVl",
    //            "subType": "00"
    //          }
    //        }
    //      }
    //    }
    //  ]
    //}
    assertTrue(DocumentComparisonExpressionUtils.evaluateConditionExpression(rawBsonDocument,
        expressionDocument));

  }

  @Test
  public void testDocumentComparisonExpression2() {
    RawBsonDocument rawBsonDocument = getDocumentValue();
    BsonDocument expressionDocument = new BsonDocument();
    List<BsonValue> orList = new ArrayList<>();
    orList.add(new BsonDocument().append("Id",
            new BsonDocument().append("$exists", new BsonBoolean(false))));
    orList.add(new BsonDocument().append("Title",
            new BsonDocument().append("$exists", new BsonBoolean(false))));
    expressionDocument.append("$or", new BsonArray(orList));

    // Condition Expression:
    //{
    //  "$or": [
    //    {
    //      "Id": {
    //        "$exists": false
    //      }
    //    },
    //    {
    //      "Title": {
    //        "$exists": false
    //      }
    //    }
    //  ]
    //}

    assertFalse(DocumentComparisonExpressionUtils.evaluateConditionExpression(rawBsonDocument,
            expressionDocument));

    expressionDocument = new BsonDocument();
    BsonArray orListArray = new BsonArray();
    BsonArray orList1 = new BsonArray();
    BsonDocument orDoc1 = new BsonDocument();
    BsonDocument andDoc1 = new BsonDocument();
    BsonArray andList1 = new BsonArray();
    andList1.add(new BsonDocument().append("Id",
            new BsonDocument().append("$exists", new BsonBoolean(false))));
    andList1.add(new BsonDocument().append("Title1",
            new BsonDocument().append("$exists", new BsonBoolean(false))));
    andDoc1.append("$and", andList1);
    orList1.add(andDoc1);
    orList1.add(new BsonDocument().append("ISBN2",
            new BsonDocument().append("$exists", new BsonBoolean(true))));
    orDoc1.append("$or", orList1);
    orListArray.add(orDoc1);

    BsonArray andList2 = new BsonArray();
    BsonDocument andDoc2 = new BsonDocument();
    andList2.add(new BsonDocument().append("Id",
            new BsonDocument().append("$eq", new BsonString("Book 101 Title"))));

    BsonArray orList2 = new BsonArray();
    BsonDocument orDoc2 = new BsonDocument();
    orList2.add(new BsonDocument().append("InPublication",
            new BsonDocument().append("$eq", new BsonBoolean(false))));

    BsonArray andList3 = new BsonArray();
    BsonDocument andDoc3 = new BsonDocument();
    andList3.add(new BsonDocument().append("ISBN",
            new BsonDocument().append("$eq", new BsonString("111-1111111111"))));
    andList3.add(new BsonDocument().append("Title",
            new BsonDocument().append("$eq", new BsonString("Book 101 Title"))));
    andDoc3.append("$and", andList3);

    orList2.add(andDoc3);
    orDoc2.append("$or", orList2);

    andList2.add(orDoc2);
    andDoc2.append("$and", andList2);
    orListArray.add(andDoc2);

    expressionDocument.append("$or", orListArray);

    // Condition Expression:
    //{
    //  "$or": [
    //    {
    //      "$or": [
    //        {
    //          "$and": [
    //            {
    //              "Id": {
    //                "$exists": false
    //              }
    //            },
    //            {
    //              "Title1": {
    //                "$exists": false
    //              }
    //            }
    //          ]
    //        },
    //        {
    //          "ISBN2": {
    //            "$exists": true
    //          }
    //        }
    //      ]
    //    },
    //    {
    //      "$and": [
    //        {
    //          "Id": {
    //            "$eq": "Book 101 Title"
    //          }
    //        },
    //        {
    //          "$or": [
    //            {
    //              "InPublication": {
    //                "$eq": false
    //              }
    //            },
    //            {
    //              "$and": [
    //                {
    //                  "ISBN": {
    //                    "$eq": "111-1111111111"
    //                  }
    //                },
    //                {
    //                  "Title": {
    //                    "$eq": "Book 101 Title"
    //                  }
    //                }
    //              ]
    //            }
    //          ]
    //        }
    //      ]
    //    }
    //  ]
    //}

    assertFalse(DocumentComparisonExpressionUtils.evaluateConditionExpression(rawBsonDocument,
            expressionDocument));

    expressionDocument = new BsonDocument();
    andList1 = new BsonArray();
    andList1.add(new BsonDocument().append("NestedMap1.ISBN",
            new BsonDocument().append("$exists", new BsonBoolean(true))));
    andList1.add(new BsonDocument().append("NestedMap1.NList1[3]",
            new BsonDocument().append("$exists", new BsonBoolean(true))));
    expressionDocument.append("$and", andList1);

    // Condition Expression:
    //{
    //  "$and": [
    //    {
    //      "NestedMap1.ISBN": {
    //        "$exists": true
    //      }
    //    },
    //    {
    //      "NestedMap1.NList1[3]": {
    //        "$exists": true
    //      }
    //    }
    //  ]
    //}

    assertFalse(DocumentComparisonExpressionUtils.evaluateConditionExpression(rawBsonDocument,
            expressionDocument));

    expressionDocument = new BsonDocument();
    andList1 = new BsonArray();
    andList1.add(new BsonDocument().append("NestedMap1.Id",
            new BsonDocument().append("$eq", new BsonDouble(101.01))));
    andList1.add(new BsonDocument().append("NestedMap1.InPublication",
            new BsonDocument().append("$ne", new BsonBoolean(false))));
    expressionDocument.append("$and", andList1);

    // Condition Expression:
    //{
    //  "$and": [
    //    {
    //      "NestedMap1.Id": {
    //        "$eq": 101.01
    //      }
    //    },
    //    {
    //      "NestedMap1.InPublication": {
    //        "$ne": false
    //      }
    //    }
    //  ]
    //}
    assertFalse(DocumentComparisonExpressionUtils.evaluateConditionExpression(rawBsonDocument,
            expressionDocument));

    expressionDocument = new BsonDocument();

    andList1 = new BsonArray();
    andDoc1 = new BsonDocument();
    andList1.add(new BsonDocument().append("NestedMap1.Id",
            new BsonDocument().append("$eq", new BsonDouble(101.01))));

    orList1 = new BsonArray();
    orDoc1 = new BsonDocument();
    orList1.add(new BsonDocument().append("NestedMap1.InPublication[0]",
            new BsonDocument().append("$eq", new BsonBoolean(false))));

    andList2 = new BsonArray();
    andDoc2 = new BsonDocument();
    andList2.add(new BsonDocument().append("ISBN[0]",
            new BsonDocument().append("$eq", new BsonString("111-1111111111"))));
    andList2.add(new BsonDocument().append("Title",
            new BsonDocument().append("$eq", new BsonString("Book 101 Title"))));
    andDoc2.append("$and", andList2);

    orList1.add(andDoc2);
    orDoc1.append("$or", orList1);

    andList1.add(orDoc1);
    andDoc1.append("$and", andList1);

    orList2 = new BsonArray();
    orDoc2 = new BsonDocument();

    orList2.add(andDoc1);
    orList2.add(new BsonDocument().append("NestedMap1.NList1[0]",
            new BsonDocument().append("$ne", new BsonString("NListVal01"))));

    expressionDocument.append("$or", orList2);

    // Condition Expression:
    //{
    //  "$or": [
    //    {
    //      "$and": [
    //        {
    //          "NestedMap1.Id": {
    //            "$eq": 101.01
    //          }
    //        },
    //        {
    //          "$or": [
    //            {
    //              "NestedMap1.InPublication[0]": {
    //                "$eq": false
    //              }
    //            },
    //            {
    //              "$and": [
    //                {
    //                  "ISBN[0]": {
    //                    "$eq": "111-1111111111"
    //                  }
    //                },
    //                {
    //                  "Title": {
    //                    "$eq": "Book 101 Title"
    //                  }
    //                }
    //              ]
    //            }
    //          ]
    //        }
    //      ]
    //    },
    //    {
    //      "NestedMap1.NList1[0]": {
    //        "$ne": "NListVal01"
    //      }
    //    }
    //  ]
    //}
    assertFalse(DocumentComparisonExpressionUtils.evaluateConditionExpression(rawBsonDocument,
            expressionDocument));

    expressionDocument = new BsonDocument();
    orListArray = new BsonArray();

    andList1 = new BsonArray();
    andDoc1 = new BsonDocument();
    andList1.add(new BsonDocument().append("Id",
            new BsonDocument().append("$exists", new BsonBoolean(false))));
    andList1.add(new BsonDocument().append("Title1",
            new BsonDocument().append("$exists", new BsonBoolean(false))));
    andDoc1.append("$and", andList1);

    orList1 = new BsonArray();
    orDoc1 = new BsonDocument();

    orList1.add(andDoc1);
    orList1.add(new BsonDocument().append("ISBN2",
            new BsonDocument().append("$exists", new BsonBoolean(true))));
    orDoc1.append("$or", orList1);

    orListArray.add(orDoc1);

    andList2 = new BsonArray();
    andDoc2 = new BsonDocument();
    andList2.add(new BsonDocument().append("NestedMap1.Id",
            new BsonDocument().append("$eq", new BsonDouble(101.01))));

    orList2 = new BsonArray();
    orDoc2 = new BsonDocument();
    orList2.add(new BsonDocument().append("NestedMap1.InPublication",
            new BsonDocument().append("$ne", new BsonBoolean(false))));

    andList3 = new BsonArray();
    andDoc3 = new BsonDocument();

    andList3.add(new BsonDocument().append("ISBN",
            new BsonDocument().append("$ne", new BsonString("111-1111111111"))));
    andList3.add(new BsonDocument().append("Title",
            new BsonDocument().append("$eq", new BsonString("Book 101 Title"))));
    andDoc3.append("$and", andList3);

    orList2.add(andDoc3);
    orDoc2.append("$or", orList2);

    andList2.add(orDoc2);
    andDoc2.append("$and", andList2);

    orListArray.add(andDoc2);
    expressionDocument.append("$or", orListArray);

    // Condition Expression:
    //{
    //  "$or": [
    //    {
    //      "$or": [
    //        {
    //          "$and": [
    //            {
    //              "Id": {
    //                "$exists": false
    //              }
    //            },
    //            {
    //              "Title1": {
    //                "$exists": false
    //              }
    //            }
    //          ]
    //        },
    //        {
    //          "ISBN2": {
    //            "$exists": true
    //          }
    //        }
    //      ]
    //    },
    //    {
    //      "$and": [
    //        {
    //          "NestedMap1.Id": {
    //            "$eq": 101.01
    //          }
    //        },
    //        {
    //          "$or": [
    //            {
    //              "NestedMap1.InPublication": {
    //                "$ne": false
    //              }
    //            },
    //            {
    //              "$and": [
    //                {
    //                  "ISBN": {
    //                    "$ne": "111-1111111111"
    //                  }
    //                },
    //                {
    //                  "Title": {
    //                    "$eq": "Book 101 Title"
    //                  }
    //                }
    //              ]
    //            }
    //          ]
    //        }
    //      ]
    //    }
    //  ]
    //}

    assertFalse(DocumentComparisonExpressionUtils.evaluateConditionExpression(rawBsonDocument,
            expressionDocument));

    expressionDocument = new BsonDocument();
    andList1 = new BsonArray();
    andList1.add(new BsonDocument().append("NestedList1[0]",
            new BsonDocument().append("$lte", new BsonDouble(-485.33))));
    andList1.add(new BsonDocument().append("NestedList1[1]",
            new BsonDocument().append("$gt", new BsonString("1234abcc"))));
    andList1.add(new BsonDocument().append("NestedList1[2][0]",
            new BsonDocument().append("$gte", new BsonString("xyz0123"))));
    andList1.add(new BsonDocument().append("NestedList1[2][1].Id",
            new BsonDocument().append("$lt", new BsonInt32(120))));
    andList1.add(new BsonDocument().append("IdS",
            new BsonDocument().append("$lt", new BsonString("12"))));
    andList1.add(new BsonDocument().append("Id2",
            new BsonDocument().append("$gt", new BsonInt32(12))));
    andList1.add(new BsonDocument().append("NestedMap1.NList1[2]",
            new BsonDocument().append("$lt", new BsonBinary(Bytes.toBytes("Whit")))));

    expressionDocument.append("$and", andList1);

    // Condition Expression:
    //{
    //  "$and": [
    //    {
    //      "NestedList1[0]": {
    //        "$lte": -485.33
    //      }
    //    },
    //    {
    //      "NestedList1[1]": {
    //        "$gt": "1234abcc"
    //      }
    //    },
    //    {
    //      "NestedList1[2][0]": {
    //        "$gte": "xyz0123"
    //      }
    //    },
    //    {
    //      "NestedList1[2][1].Id": {
    //        "$lt": 120
    //      }
    //    },
    //    {
    //      "IdS": {
    //        "$lt": "12"
    //      }
    //    },
    //    {
    //      "Id2": {
    //        "$gt": 12
    //      }
    //    },
    //    {
    //      "NestedMap1.NList1[2]": {
    //        "$lt": {
    //          "$binary": {
    //            "base64": "V2hpdA==",
    //            "subType": "00"
    //          }
    //        }
    //      }
    //    }
    //  ]
    //}

    assertFalse(DocumentComparisonExpressionUtils.evaluateConditionExpression(rawBsonDocument,
            expressionDocument));

    expressionDocument = new BsonDocument();
    andList1 = new BsonArray();
    andList1.add(new BsonDocument().append("NestedList1[0]",
            new BsonDocument().append("$lte", new BsonDouble(-485.33))));
    andList1.add(new BsonDocument().append("NestedList1[1]",
            new BsonDocument().append("$gte", new BsonString("1234abcc"))));
    andList1.add(new BsonDocument().append("NestedList1[2][0]",
            new BsonDocument().append("$gte", new BsonString("xyz0123"))));
    andList1.add(new BsonDocument().append("NestedList1[2][1].Id",
            new BsonDocument().append("$lte", new BsonInt32(120))));
    andList1.add(new BsonDocument().append("IdS",
            new BsonDocument().append("$lte", new BsonString("12"))));
    andList1.add(new BsonDocument().append("Id2",
            new BsonDocument().append("$gte", new BsonInt32(12))));
    andList1.add(new BsonDocument().append("NestedMap1.NList1[2]",
            new BsonDocument().append("$lt", new BsonBinary(Bytes.toBytes("Whit")))));

    expressionDocument.append("$and", andList1);

    // Condition Expression:
    //{
    //  "$and": [
    //    {
    //      "NestedList1[0]": {
    //        "$lte": -485.33
    //      }
    //    },
    //    {
    //      "NestedList1[1]": {
    //        "$gte": "1234abcc"
    //      }
    //    },
    //    {
    //      "NestedList1[2][0]": {
    //        "$gte": "xyz0123"
    //      }
    //    },
    //    {
    //      "NestedList1[2][1].Id": {
    //        "$lte": 120
    //      }
    //    },
    //    {
    //      "IdS": {
    //        "$lte": "12"
    //      }
    //    },
    //    {
    //      "Id2": {
    //        "$gte": 12
    //      }
    //    },
    //    {
    //      "NestedMap1.NList1[2]": {
    //        "$lt": {
    //          "$binary": {
    //            "base64": "V2hpdA==",
    //            "subType": "00"
    //          }
    //        }
    //      }
    //    }
    //  ]
    //}

    assertFalse(DocumentComparisonExpressionUtils.evaluateConditionExpression(rawBsonDocument,
            expressionDocument));

    expressionDocument = new BsonDocument();

    andList1 = new BsonArray();
    andList1.add(new BsonDocument().append("NestedList1[0]",
            new BsonDocument().append("$gte", new BsonDouble(-485.35))));
    andList1.add(new BsonDocument().append("NestedList1[1]",
            new BsonDocument().append("$lt", new BsonString("1234abce"))));
    andList1.add(new BsonDocument().append("NestedList1[2][0]",
            new BsonDocument().append("$gte", new BsonString("xyz0123"))));
    andList1.add(new BsonDocument().append("NestedList1[2][1].Id",
            new BsonDocument().append("$gt", new BsonInt64(101))));
    andList1.add(
            new BsonDocument().append("IdS", new BsonDocument().append("$gt", new BsonString("100"))));
    andList1.add(new BsonDocument().append("Id2",
            new BsonDocument().append("$lt", new BsonDouble(101.011))));
    andList1.add(new BsonDocument().append("NestedMap1.NList1[2]",
            new BsonDocument().append("$gte", new BsonBinary(Bytes.toBytes("Whitee")))));

    expressionDocument.append("$and", andList1);

    // Condition Expression:
    //{
    //  "$and": [
    //    {
    //      "NestedList1[0]": {
    //        "$gte": -485.35
    //      }
    //    },
    //    {
    //      "NestedList1[1]": {
    //        "$lt": "1234abce"
    //      }
    //    },
    //    {
    //      "NestedList1[2][0]": {
    //        "$gte": "xyz0123"
    //      }
    //    },
    //    {
    //      "NestedList1[2][1].Id": {
    //        "$gt": 101
    //      }
    //    },
    //    {
    //      "IdS": {
    //        "$gt": "100"
    //      }
    //    },
    //    {
    //      "Id2": {
    //        "$lt": 101.011
    //      }
    //    },
    //    {
    //      "NestedMap1.NList1[2]": {
    //        "$gte": {
    //          "$binary": {
    //            "base64": "V2hpdGVl",
    //            "subType": "00"
    //          }
    //        }
    //      }
    //    }
    //  ]
    //}
    assertFalse(DocumentComparisonExpressionUtils.evaluateConditionExpression(rawBsonDocument,
            expressionDocument));

    expressionDocument = new BsonDocument();

    andList1 = new BsonArray();
    andList1.add(new BsonDocument().append("NestedList1[0]",
            new BsonDocument().append("$gte", new BsonDouble(-485.35))));
    andList1.add(new BsonDocument().append("NestedList1[1]",
            new BsonDocument().append("$lte", new BsonString("1234abce"))));
    andList1.add(new BsonDocument().append("NestedList1[2][0]",
            new BsonDocument().append("$gte", new BsonString("xyz0123"))));
    andList1.add(new BsonDocument().append("NestedList1[2][1].Id",
            new BsonDocument().append("$gte", new BsonInt64(101))));
    andList1.add(new BsonDocument().append("IdS",
            new BsonDocument().append("$gte", new BsonString("100"))));
    andList1.add(new BsonDocument().append("Id2",
            new BsonDocument().append("$lte", new BsonDouble(101.011))));
    andList1.add(new BsonDocument().append("NestedMap1.NList1[2]",
            new BsonDocument().append("$gt", new BsonBinary(Bytes.toBytes("Whitee")))));
    andList1.add(new BsonDocument().append("NestedMap1.NList1[2]",
            new BsonDocument().append("$ne", new BsonBinary(Bytes.toBytes("Whitee")))));

    expressionDocument.append("$and", andList1);

    // Condition Expression:
    //{
    //  "$and": [
    //    {
    //      "NestedList1[0]": {
    //        "$gte": -485.35
    //      }
    //    },
    //    {
    //      "NestedList1[1]": {
    //        "$lte": "1234abce"
    //      }
    //    },
    //    {
    //      "NestedList1[2][0]": {
    //        "$gte": "xyz0123"
    //      }
    //    },
    //    {
    //      "NestedList1[2][1].Id": {
    //        "$gte": 101
    //      }
    //    },
    //    {
    //      "IdS": {
    //        "$gte": "100"
    //      }
    //    },
    //    {
    //      "Id2": {
    //        "$lte": 101.011
    //      }
    //    },
    //    {
    //      "NestedMap1.NList1[2]": {
    //        "$gt": {
    //          "$binary": {
    //            "base64": "V2hpdGVl",
    //            "subType": "00"
    //          }
    //        }
    //      }
    //    },
    //    {
    //      "NestedMap1.NList1[2]": {
    //        "$ne": {
    //          "$binary": {
    //            "base64": "V2hpdGVl",
    //            "subType": "00"
    //          }
    //        }
    //      }
    //    }
    //  ]
    //}
    assertFalse(DocumentComparisonExpressionUtils.evaluateConditionExpression(rawBsonDocument,
            expressionDocument));

  }

  @Test
  public void testBeginsWithFunction() {
    RawBsonDocument rawBsonDocument = getDocumentValue();
    RawBsonDocument compareValues = getCompareValDocument();

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "begins_with(Title, #Title)", rawBsonDocument, compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "begins_with(ISBN, :ISBN)", rawBsonDocument, compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "begins_with(Title, :TitlePrefix)", rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "begins_with(Title, #NestedList1_xyz0123)", rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "begins_with(ISBN, #NestedList1_1)", rawBsonDocument, compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "begins_with(NestedMap1.Title, #Title)", rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "begins_with(NestedMap1.Title, #NestedList1_xyz0123)", rawBsonDocument, compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "begins_with(NestedMap1.NList1[2], #NestedMap1_NList1_3)", rawBsonDocument,
            compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "begins_with(NestedMap1.NList1[2], $NestedMap1_NList1_30)", rawBsonDocument,
            compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "begins_with(NonExistentField, #Title)", rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "begins_with(Id, #Title)", rawBsonDocument, compareValues));

    try {
      SQLComparisonExpressionUtils.evaluateConditionExpression(
              "begins_with(Title, :1)", rawBsonDocument, compareValues);
      fail("Expected BsonConditionInvalidArgumentException");
    } catch (BsonConditionInvalidArgumentException e) {
      // expected
    }

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "begins_with(Title, #Title) AND field_exists(Id) = begins_with(Title, #Title)",
            rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "begins_with(Title, #NestedList1_xyz0123) AND field_exists(Id)", rawBsonDocument,
            compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "begins_with(Title, #NestedList1_xyz0123) OR field_exists(Id)", rawBsonDocument,
            compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "begins_with(Title, #Title) OR field_not_exists(Id)", rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "NOT begins_with(Title, #Title)", rawBsonDocument, compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "NOT begins_with(Title, #NestedList1_xyz0123)", rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "NOT begins_with(Title, #Title)", rawBsonDocument, compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "NOT begins_with(Title, #NestedList1_xyz0123)", rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "NOT begins_with(Title, :TitlePrefix)", rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "NOT begins_with(NestedMap1.NList1[2], #NestedMap1_NList1_3)", rawBsonDocument,
            compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "NOT begins_with(NestedMap1.NList1[2], $NestedMap1_NList1_30)", rawBsonDocument,
            compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "NOT begins_with(Title, #NestedList1_xyz0123) AND field_exists(Id)", rawBsonDocument,
            compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "NOT begins_with(Title, #Title) OR NOT begins_with(Title, :TitlePrefix)",
            rawBsonDocument, compareValues));
  }

  @Test
  public void testContainsFunction() {
    RawBsonDocument rawBsonDocument = getContainsTestDocument();
    RawBsonDocument compareValues = getContainsCompareValDocument();

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "contains(Title, :TitleSubstring)", rawBsonDocument, compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "contains(Description, :DescriptionWord)", rawBsonDocument, compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "contains(NestedMap1.Title, :TitleSubstring)", rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "contains(Title, :NonExistentSubstring)", rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "contains(Description, :WrongWord)", rawBsonDocument, compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "contains(Tags, :TagScience)", rawBsonDocument, compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "contains(Numbers, :NumberFive)", rawBsonDocument, compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "contains(NestedList1, :NestedListString)", rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "contains(Tags, :NonExistentTag)", rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "contains(Numbers, :NumberTen)", rawBsonDocument, compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "contains(Categories, :CategoryFiction)", rawBsonDocument, compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "contains(StatusSet, :StatusActive)", rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "contains(Categories, :NonExistentCategory)", rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "contains(StatusSet, :StatusInactive)", rawBsonDocument, compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "contains(BinaryDataSet, :BinaryHello)", rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "contains(BinaryDataSet, :BinaryNotFound)", rawBsonDocument, compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "contains(Title, :TitleSubstring) AND contains(Tags, :TagScience)", rawBsonDocument,
            compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "contains(Title, :NonExistentSubstring) OR contains(Tags, :TagScience)",
            rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "contains(Title, :NonExistentSubstring) AND contains(Tags, :TagScience)",
            rawBsonDocument, compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "NOT contains(Title, :NonExistentSubstring)", rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "NOT contains(Title, :TitleSubstring)", rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "contains(NonExistentField, :TitleSubstring)", rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "contains(Title, :NonExistentValue)", rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "contains(Title, :NumberFive)", rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "contains(Numbers, :TitleSubstring)", rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "contains(Id, :TitleSubstring)", rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "contains(InPublication, :TitleSubstring)", rawBsonDocument, compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "contains(NestedMap1.SubTags, :TagMath)", rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "contains(NestedMap1.SubTags, :NonExistentTag)", rawBsonDocument, compareValues));
  }

  private static RawBsonDocument getContainsTestDocument() {
    String json = "{\n" +
            "  \"Title\" : \"Advanced Data Science and Machine Learning\",\n" +
            "  \"Description\" : \"This book covers comprehensive topics in Quantum Computing\",\n" +
            "  \"Tags\" : [ \"science\", \"technology\", \"programming\", \"AI\" ],\n" +
            "  \"Numbers\" : [ 1, 2, 3, 5, 8, 13 ],\n" +
            "  \"NestedList1\" : [ -485.34, \"1234abcd\", \"xyz0123\", \"test_string\" ],\n" +
            "  \"Categories\" : { \"$set\" : [ \"fiction\", \"educational\", \"technical\" ] },\n" +
            "  \"StatusSet\" : { \"$set\" : [ \"active\", \"published\", \"available\" ] },\n" +
            "  \"BinaryDataSet\" : { \"$set\" : [ {\n" +
            "    \"$binary\" : {\n" +
            "      \"base64\" : \"SGVsbG8=\",\n" +
            "      \"subType\" : \"00\"\n" +
            "    }\n" +
            "  }, {\n" +
            "    \"$binary\" : {\n" +
            "      \"base64\" : \"V29ybGQ=\",\n" +
            "      \"subType\" : \"00\"\n" +
            "    }\n" +
            "  }, {\n" +
            "    \"$binary\" : {\n" +
            "      \"base64\" : \"VGVzdA==\",\n" +
            "      \"subType\" : \"00\"\n" +
            "    }\n" +
            "  } ] },\n" +
            "  \"NestedMap1\" : {\n" +
            "    \"Title\" : \"Nested Advanced Data Science Guide\",\n" +
            "    \"SubTags\" : [ \"mathematics\", \"statistics\", \"algorithms\" ],\n" +
            "    \"InnerSet\" : { \"$set\" : [ \"regression\", \"classification\" ] }\n" +
            "  },\n" +
            "  \"Id\" : 101.01,\n" +
            "  \"InPublication\" : true\n" +
            "}";
    return RawBsonDocument.parse(json);
  }

  private static RawBsonDocument getContainsCompareValDocument() {
    String json = "{\n" +
            "  \":TitleSubstring\" : \"Data Science\",\n" +
            "  \":DescriptionWord\" : \"Quantum Comput\",\n" +
            "  \":NonExistentSubstring\" : \"Quantum Physics\",\n" +
            "  \":WrongWord\" : \"geology\",\n" +
            "  \":TagScience\" : \"science\",\n" +
            "  \":TagMath\" : \"mathematics\",\n" +
            "  \":NonExistentTag\" : \"biology\",\n" +
            "  \":NumberFive\" : 5,\n" +
            "  \":NumberTen\" : 10,\n" +
            "  \":NestedListString\" : \"test_string\",\n" +
            "  \":CategoryFiction\" : \"fiction\",\n" +
            "  \":NonExistentCategory\" : \"romance\",\n" +
            "  \":StatusActive\" : \"active\",\n" +
            "  \":StatusInactive\" : \"inactive\",\n" +
            "  \":NonExistentValue\" : \"does_not_exist\",\n" +
            "  \":BinaryHello\" : {\n" +
            "    \"$binary\" : {\n" +
            "      \"base64\" : \"SGVsbG8=\",\n" +
            "      \"subType\" : \"00\"\n" +
            "    }\n" +
            "  },\n" +
            "  \":BinaryNotFound\" : {\n" +
            "    \"$binary\" : {\n" +
            "      \"base64\" : \"Tm90Rm91bmQ=\",\n" +
            "      \"subType\" : \"00\"\n" +
            "    }\n" +
            "  }\n" +
            "}";
    return RawBsonDocument.parse(json);
  }

  private static RawBsonDocument getCompareValDocument() {
    String json = "{\n" +
            "  \"$Id20\" : 101.011,\n" +
            "  \"$Id2\" : 12,\n" +
            "  \"#NestedList1_10\" : \"1234abce\",\n" +
            "  \"$Id1\" : 120,\n" +
            "  \"$Id10\" : 101,\n" +
            "  \":1\" : 100,\n " +
            "  \"$Ids1\" : \"12\",\n" +
            "  \":ISBN\" : \"111-1111111111\",\n" +
            "  \"#NestedList1_xyz0123\" : \"xyz0123\",\n" +
            "  \"$NestedList1_485\" : -485.33,\n" +
            "  \"$NestedMap1_NList1_30\" : {\n" +
            "    \"$binary\" : {\n" +
            "      \"base64\" : \"V2hpdGVl\",\n" +
            "      \"subType\" : \"00\"\n" +
            "    }\n" +
            "  },\n" +
            "  \"InPublication\" : false,\n" +
            "  \"$Ids10\" : \"100\",\n" +
            "  \"#NestedMap1_NList1_3\" : {\n" +
            "    \"$binary\" : {\n" +
            "      \"base64\" : \"V2hpdA==\",\n" +
            "      \"subType\" : \"00\"\n" +
            "    }\n" +
            "  },\n" +
            "  \"#NestedList1_1\" : \"1234abcc\",\n" +
            "  \"#NMap1_NList1\" : \"NListVal01\",\n" +
            "  \"$NestedList1_4850\" : -485.35,\n" +
            "  \"$Id\" : 101.01,\n" +
            "  \"#Title\" : \"Book 101 Title\",\n" +
            "  \":TitlePrefix\" : \"Book\"\n" +
            "}";
    //{
    //  "$Id20" : 101.011,
    //  "$Id2" : 12,
    //  "#NestedList1_10" : "1234abce",
    //  "$Id1" : 120,
    //  "$Id10" : 101,
    //  "$Ids1" : "12",
    //  ":ISBN" : "111-1111111111",
    //  "#NestedList1_xyz0123" : "xyz0123",
    //  "$NestedList1_485" : -485.33,
    //  "$NestedMap1_NList1_30" : {
    //    "$binary" : {
    //      "base64" : "V2hpdGVl",
    //      "subType" : "00"
    //    }
    //  },
    //  "InPublication" : false,
    //  "$Ids10" : "100",
    //  "#NestedMap1_NList1_3" : {
    //    "$binary" : {
    //      "base64" : "V2hpdA==",
    //      "subType" : "00"
    //    }
    //  },
    //  "#NestedList1_1" : "1234abcc",
    //  "#NMap1_NList1" : "NListVal01",
    //  "$NestedList1_4850" : -485.35,
    //  "$Id" : 101.01,
    //  "#Title" : "Book 101 Title",
    //  ":TitlePrefix" : "Book"
    //}
    return RawBsonDocument.parse(json);
  }

  private static RawBsonDocument getDocumentValue() {
    String json = "{\n" +
            "  \"InPublication\" : false,\n" +
            "  \"ISBN\" : \"111-1111111111\",\n" +
            "  \"NestedList1\" : [ -485.34, \"1234abcd\", [ \"xyz0123\", {\n" +
            "    \"InPublication\" : false,\n" +
            "    \"ISBN\" : \"111-1111111111\",\n" +
            "    \"Title\" : \"Book 101 Title\",\n" +
            "    \"Id\" : 101.01\n" +
            "  } ] ],\n" +
            "  \"NestedMap1\" : {\n" +
            "    \"InPublication\" : false,\n" +
            "    \"ISBN\" : \"111-1111111111\",\n" +
            "    \"Title\" : \"Book 101 Title\",\n" +
            "    \"Id\" : 101.01,\n" +
            "    \"NList1\" : [ \"NListVal01\", -23.4, {\n" +
            "      \"$binary\" : {\n" +
            "        \"base64\" : \"V2hpdGU=\",\n" +
            "        \"subType\" : \"00\"\n" +
            "      }\n" +
            "    } ]\n" +
            "  },\n" +
            "  \"Id2\" : 101.01,\n" +
            "  \"IdS\" : \"101.01\",\n" +
            "  \">\" : 12,\n " +
            "  \"Title\" : \"Book 101 Title\",\n" +
            "  \"Id\" : 101.01\n" +
            "}";
    //{
    //  "InPublication" : false,
    //  "ISBN" : "111-1111111111",
    //  "NestedList1" : [ -485.34, "1234abcd", [ "xyz0123", {
    //    "InPublication" : false,
    //    "ISBN" : "111-1111111111",
    //    "Title" : "Book 101 Title",
    //    "Id" : 101.01
    //  } ] ],
    //  "NestedMap1" : {
    //    "InPublication" : false,
    //    "ISBN" : "111-1111111111",
    //    "Title" : "Book 101 Title",
    //    "Id" : 101.01,
    //    "NList1" : [ "NListVal01", -23.4, {
    //      "$binary" : {
    //        "base64" : "V2hpdGU=",
    //        "subType" : "00"
    //      }
    //    } ]
    //  },
    //  "Id2" : 101.01,
    //  "IdS" : "101.01",
    //  "Title" : "Book 101 Title",
    //  "Id" : 101.01
    //}
    return RawBsonDocument.parse(json);
  }

  @Test
  public void testFieldTypeFunction() {
    RawBsonDocument rawBsonDocument = getFieldTypeTestDocument();
    RawBsonDocument compareValues = getFieldTypeCompareValDocument();

    // Test String type (S)
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(StringField, :TypeS)", rawBsonDocument, compareValues));
    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(StringField, :TypeN)", rawBsonDocument, compareValues));

    // Test Number type (N) - testing both integer and double
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(NumberFieldInt, :TypeN)", rawBsonDocument, compareValues));
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(NumberFieldDouble, :TypeN)", rawBsonDocument, compareValues));
    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(NumberFieldInt, :TypeS)", rawBsonDocument, compareValues));

    // Test Binary type (B)
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(BinaryField, :TypeB)", rawBsonDocument, compareValues));
    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(BinaryField, :TypeS)", rawBsonDocument, compareValues));

    // Test Boolean type (BOOL)
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(BooleanFieldTrue, :TypeBOOL)", rawBsonDocument, compareValues));
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(BooleanFieldFalse, :TypeBOOL)", rawBsonDocument, compareValues));
    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(BooleanFieldTrue, :TypeS)", rawBsonDocument, compareValues));

    // Test Null type (NULL)
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(NullField, :TypeNULL)", rawBsonDocument, compareValues));
    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(NullField, :TypeS)", rawBsonDocument, compareValues));

    // Test List/Array type (L)
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(ArrayField, :TypeL)", rawBsonDocument, compareValues));
    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(ArrayField, :TypeS)", rawBsonDocument, compareValues));

    // Test Map/Document type (M)
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(DocumentField, :TypeM)", rawBsonDocument, compareValues));
    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(DocumentField, :TypeS)", rawBsonDocument, compareValues));

    // Test String Set type (SS)
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(StringSetField, :TypeSS)", rawBsonDocument, compareValues));
    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(StringSetField, :TypeNS)", rawBsonDocument, compareValues));

    // Test Number Set type (NS)
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(NumberSetField, :TypeNS)", rawBsonDocument, compareValues));
    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(NumberSetField, :TypeSS)", rawBsonDocument, compareValues));

    // Test Binary Set type (BS)
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(BinarySetField, :TypeBS)", rawBsonDocument, compareValues));
    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(BinarySetField, :TypeSS)", rawBsonDocument, compareValues));

    // Test nested field types
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(NestedDoc.NestedString, :TypeS)", rawBsonDocument, compareValues));
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(NestedDoc.NestedNumber, :TypeN)", rawBsonDocument, compareValues));
    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(NestedDoc.NestedString, :TypeN)", rawBsonDocument, compareValues));

    // Test non-existent field
    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(NonExistentField, :TypeS)", rawBsonDocument, compareValues));

    // Test complex expressions with field_type
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(StringField, :TypeS) AND field_type(NumberFieldInt, :TypeN)", 
            rawBsonDocument, compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(StringField, :TypeN) OR field_type(NumberFieldInt, :TypeN)", 
            rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(StringField, :TypeN) AND field_type(NumberFieldInt, :TypeS)", 
            rawBsonDocument, compareValues));

    // Test NOT with field_type
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "NOT field_type(StringField, :TypeN)", rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "NOT field_type(StringField, :TypeS)", rawBsonDocument, compareValues));

    // Test field_type combined with other functions
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(StringField, :TypeS) AND field_exists(StringField)", 
            rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(NonExistentField, :TypeS) AND field_exists(NonExistentField)", 
            rawBsonDocument, compareValues));

    // Test attribute_type (alternative syntax for field_type)
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "attribute_type(StringField, :TypeS)", rawBsonDocument, compareValues));
    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "attribute_type(StringField, :TypeN)", rawBsonDocument, compareValues));

    // Test attribute_type with different data types
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "attribute_type(NumberFieldInt, :TypeN)", rawBsonDocument, compareValues));
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "attribute_type(BooleanFieldTrue, :TypeBOOL)", rawBsonDocument, compareValues));
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "attribute_type(ArrayField, :TypeL)", rawBsonDocument, compareValues));
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "attribute_type(DocumentField, :TypeM)", rawBsonDocument, compareValues));

    // Test attribute_type with sets
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "attribute_type(StringSetField, :TypeSS)", rawBsonDocument, compareValues));
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "attribute_type(NumberSetField, :TypeNS)", rawBsonDocument, compareValues));
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "attribute_type(BinarySetField, :TypeBS)", rawBsonDocument, compareValues));

    // Test attribute_type with nested fields
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "attribute_type(NestedDoc.NestedString, :TypeS)", rawBsonDocument, compareValues));
    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "attribute_type(NestedDoc.NestedString, :TypeN)", rawBsonDocument, compareValues));

    // Test attribute_type in complex expressions
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "attribute_type(StringField, :TypeS) AND attribute_type(NumberFieldInt, :TypeN)", 
            rawBsonDocument, compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "attribute_type(StringField, :TypeN) OR attribute_type(NumberFieldInt, :TypeN)", 
            rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "attribute_type(StringField, :TypeN) AND attribute_type(NumberFieldInt, :TypeS)", 
            rawBsonDocument, compareValues));

    // Test NOT with attribute_type
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "NOT attribute_type(StringField, :TypeN)", rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "NOT attribute_type(StringField, :TypeS)", rawBsonDocument, compareValues));

    // Test mixing field_type and attribute_type
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(StringField, :TypeS) AND attribute_type(NumberFieldInt, :TypeN)", 
            rawBsonDocument, compareValues));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(StringField, :TypeN) OR attribute_type(NumberFieldInt, :TypeN)", 
            rawBsonDocument, compareValues));

    // Test attribute_type with non-existent field
    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "attribute_type(NonExistentField, :TypeS)", rawBsonDocument, compareValues));

    // Test attribute_type combined with other functions
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "attribute_type(StringField, :TypeS) AND attribute_exists(StringField)", 
            rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "attribute_type(NonExistentField, :TypeS) AND attribute_not_exists(NonExistentField)", 
            rawBsonDocument, compareValues));

    // Test empty sets - should return false for type checking
    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(EmptyStringSet, :TypeSS)", rawBsonDocument, compareValues));
    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(EmptyNumberSet, :TypeNS)", rawBsonDocument, compareValues));
    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(EmptyBinarySet, :TypeBS)", rawBsonDocument, compareValues));

    // Test empty sets with attribute_type syntax
    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "attribute_type(EmptyStringSet, :TypeSS)", rawBsonDocument, compareValues));
    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "attribute_type(EmptyNumberSet, :TypeNS)", rawBsonDocument, compareValues));
    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "attribute_type(EmptyBinarySet, :TypeBS)", rawBsonDocument, compareValues));

    // Test empty sets in complex expressions
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(StringSetField, :TypeSS) AND NOT field_type(EmptyStringSet, :TypeSS)", 
            rawBsonDocument, compareValues));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(EmptyStringSet, :TypeSS) AND field_type(StringSetField, :TypeSS)", 
            rawBsonDocument, compareValues));

    // Test array element access at specific indices
    // ArrayField contains: [ "item1", "item2", 123 ]
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(ArrayField[0], :TypeS)", rawBsonDocument, compareValues));
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(ArrayField[1], :TypeS)", rawBsonDocument, compareValues));
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(ArrayField[2], :TypeN)", rawBsonDocument, compareValues));

    // Test negative cases for array element types
    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(ArrayField[0], :TypeN)", rawBsonDocument, compareValues));
    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(ArrayField[2], :TypeS)", rawBsonDocument, compareValues));

    // Test array element access with attribute_type
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "attribute_type(ArrayField[0], :TypeS)", rawBsonDocument, compareValues));
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "attribute_type(ArrayField[2], :TypeN)", rawBsonDocument, compareValues));
    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "attribute_type(ArrayField[1], :TypeN)", rawBsonDocument, compareValues));

    // Test nested array element access
    // NestedDoc.NestedArray contains: [ 1, 2, 3 ]
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(NestedDoc.NestedArray[0], :TypeN)", rawBsonDocument, compareValues));
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(NestedDoc.NestedArray[1], :TypeN)", rawBsonDocument, compareValues));
    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(NestedDoc.NestedArray[2], :TypeN)", rawBsonDocument, compareValues));
    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(NestedDoc.NestedArray[0], :TypeS)", rawBsonDocument, compareValues));

    // Test non-existent array indices
    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(ArrayField[5], :TypeS)", rawBsonDocument, compareValues));
    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "field_type(NestedDoc.NestedArray[10], :TypeN)", rawBsonDocument, compareValues));
  }

  private static RawBsonDocument getFieldTypeTestDocument() {
    String json = "{\n" +
            "  \"StringField\" : \"Hello World\",\n" +
            "  \"NumberFieldInt\" : 42,\n" +
            "  \"NumberFieldDouble\" : 3.14159,\n" +
            "  \"BinaryField\" : {\n" +
            "    \"$binary\" : {\n" +
            "      \"base64\" : \"SGVsbG8gV29ybGQ=\",\n" +
            "      \"subType\" : \"00\"\n" +
            "    }\n" +
            "  },\n" +
            "  \"BooleanFieldTrue\" : true,\n" +
            "  \"BooleanFieldFalse\" : false,\n" +
            "  \"NullField\" : null,\n" +
            "  \"ArrayField\" : [ \"item1\", \"item2\", 123 ],\n" +
            "  \"DocumentField\" : {\n" +
            "    \"nestedKey\" : \"nestedValue\",\n" +
            "    \"nestedNum\" : 999\n" +
            "  },\n" +
            "  \"StringSetField\" : { \"$set\" : [ \"apple\", \"banana\", \"cherry\" ] },\n" +
            "  \"NumberSetField\" : { \"$set\" : [ 10, 20, 30 ] },\n" +
            "  \"BinarySetField\" : { \"$set\" : [ {\n" +
            "    \"$binary\" : {\n" +
            "      \"base64\" : \"VGVzdA==\",\n" +
            "      \"subType\" : \"00\"\n" +
            "    }\n" +
            "  }, {\n" +
            "    \"$binary\" : {\n" +
            "      \"base64\" : \"RGF0YQ==\",\n" +
            "      \"subType\" : \"00\"\n" +
            "    }\n" +
            "  } ] },\n" +
            "  \"EmptyStringSet\" : { \"$set\" : [ ] },\n" +
            "  \"EmptyNumberSet\" : { \"$set\" : [ ] },\n" +
            "  \"EmptyBinarySet\" : { \"$set\" : [ ] },\n" +
            "  \"NestedDoc\" : {\n" +
            "    \"NestedString\" : \"nested value\",\n" +
            "    \"NestedNumber\" : 777,\n" +
            "    \"NestedArray\" : [ 1, 2, 3 ]\n" +
            "  }\n" +
            "}";
    return RawBsonDocument.parse(json);
  }

  private static RawBsonDocument getFieldTypeCompareValDocument() {
    String json = "{\n" +
            "  \":TypeS\" : \"S\",\n" +
            "  \":TypeN\" : \"N\",\n" +
            "  \":TypeB\" : \"B\",\n" +
            "  \":TypeBOOL\" : \"BOOL\",\n" +
            "  \":TypeNULL\" : \"NULL\",\n" +
            "  \":TypeL\" : \"L\",\n" +
            "  \":TypeM\" : \"M\",\n" +
            "  \":TypeSS\" : \"SS\",\n" +
            "  \":TypeNS\" : \"NS\",\n" +
            "  \":TypeBS\" : \"BS\"\n" +
            "}";
    return RawBsonDocument.parse(json);
  }

}
