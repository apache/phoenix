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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.apache.phoenix.expression.util.bson.DocumentComparisonExpressionUtils;
import org.apache.phoenix.expression.util.bson.SQLComparisonExpressionUtils;

import static org.junit.Assert.assertTrue;

/**
 * Tests for BSON Condition Expression Utility.
 */
public class ComparisonExpressionUtilsTest {

  @Test
  public void testSQLComparisonExpression1() {
    TestFieldsMap testFieldsMap1 = getPhoenixFieldMap1();
    TestFieldsMap compareValMap1 = getCompareValMap1();

    RawBsonDocument rawBsonDocument = TestUtil.getRawBsonDocument(testFieldsMap1);
    //{
    //  "$Id20": 101.011,
    //  "$Id2": 12,
    //  "#NestedList1_10": "1234abce",
    //  "$Id1": 120,
    //  "$Id10": 101,
    //  "$Ids1": "12",
    //  ":ISBN": "111-1111111111",
    //  "#NestedList1_xyz0123": "xyz0123",
    //  "$NestedList1_485": -485.33,
    //  "$NestedMap1_NList1_30": {
    //    "$binary": {
    //      "base64": "V2hpdGVl",
    //      "subType": "00"
    //    }
    //  },
    //  "InPublication": false,
    //  "$Ids10": "100",
    //  "#NestedMap1_NList1_3": {
    //    "$binary": {
    //      "base64": "V2hpdA==",
    //      "subType": "00"
    //    }
    //  },
    //  "#NestedList1_1": "1234abcc",
    //  "#NMap1_NList1": "NListVal01",
    //  "$NestedList1_4850": -485.35,
    //  "$Id": 101.01,
    //  "#Title": "Book 101 Title"
    //}
    RawBsonDocument compareValues = TestUtil.getRawBsonDocument(compareValMap1);

    SQLComparisonExpressionUtils SQLComparisonExpressionUtils =
        new SQLComparisonExpressionUtils(rawBsonDocument, compareValues);

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
        "(field_exists(Id) OR field_not_exists(Title))"));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
        "((field_not_exists(Id) AND field_not_exists(Title1)) OR field_exists(ISBN2))"
            + " OR ((Id <> #Title) AND ((InPublication = InPublication) OR ((ISBN = :ISBN)"
            + " AND (Title = #Title))))"));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
        "((field_exists(NestedMap1.ISBN) AND field_not_exists(NestedMap1.NList1[3])))"));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
        "NestedMap1.Id = $Id AND (NestedMap1.InPublication = InPublication)"));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
        "((NestedMap1.Id = $Id) AND ((NestedMap1.InPublication[0] = InPublication) OR "
            + "((ISBN[0] = :ISBN) AND (Title = #Title))) OR "
            + "(NestedMap1.NList1[0] = #NMap1_NList1))"));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
        "((field_not_exists(Id) AND field_not_exists(Title1)) OR field_exists(ISBN2))"
            + " OR ((NestedMap1.Id = $Id) AND ((NestedMap1.InPublication = InPublication) OR "
            + "((ISBN = :ISBN) AND (Title = #Title))))"));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
        "NestedList1[0] <= $NestedList1_485 AND NestedList1[1] > #NestedList1_1 AND NestedList1[2][0] >= #NestedList1_xyz0123 AND "
            + "NestedList1[2][1].Id < $Id1 AND IdS < $Ids1 AND Id2 > $Id2 AND NestedMap1.NList1[2] > #NestedMap1_NList1_3"));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
        "NestedList1[0] <= $NestedList1_485 AND NestedList1[1] >= #NestedList1_1 AND NestedList1[2][0] >= #NestedList1_xyz0123 AND "
            + "NestedList1[2][1].Id <= $Id1 AND IdS <= $Ids1 AND Id2 >= $Id2 AND NestedMap1.NList1[2] >= #NestedMap1_NList1_3"));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
        "NestedList1[0] >= $NestedList1_4850 AND NestedList1[1] < #NestedList1_10 AND NestedList1[2][0] >= #NestedList1_xyz0123 AND "
            + "NestedList1[2][1].Id > $Id10 AND IdS > $Ids10 AND Id2 < $Id20 AND NestedMap1.NList1[2] < $NestedMap1_NList1_30"));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
        "NestedList1[0] >= $NestedList1_4850 AND NestedList1[1] <= #NestedList1_10 AND NestedList1[2][0] >= #NestedList1_xyz0123 AND "
            + "NestedList1[2][1].Id >= $Id10 AND IdS >= $Ids10 AND Id2 <= $Id20 AND NestedMap1.NList1[2] <= $NestedMap1_NList1_30 AND "
            + "NestedMap1.NList1[2] <> $NestedMap1_NList1_30"));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
        "NestedList1[0] >= $NestedList1_4850 AND NestedList1[1] <= #NestedList1_10 AND NestedList1[2][0] >= #NestedList1_xyz0123 AND "
            + "NestedList1[2][1].Id >= $Id10 AND IdS >= $Ids10 AND Id2 <= $Id20 AND NestedMap1.NList1[2] <= $NestedMap1_NList1_30 AND "
            + "(NestedMap1.NList1[2] = $NestedMap1_NList1_30 OR NestedList1[0] BETWEEN $NestedList1_4850 AND $Id2)"));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
        "NestedList1[0] >= $NestedList1_4850 AND NestedList1[1] <= #NestedList1_10 AND NestedList1[2][0] >= #NestedList1_xyz0123 AND "
            + "NestedMap1.NList1[0] IN ($Id, $Id1, $Id20, #NMap1_NList1) AND NestedMap1.NList1[2] <= $NestedMap1_NList1_30 AND "
            + "(NestedMap1.NList1[2] = $NestedMap1_NList1_30 OR NestedList1[0] BETWEEN $NestedList1_4850 AND $Id2)"));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
        "NestedList1[0] >= $NestedList1_4850 AND NestedList1[1] <= #NestedList1_10 AND NestedList1[2][0] >= #NestedList1_xyz0123 AND "
            + "NestedMap1.NList1[0] IN ($Id,  $Id1, $Id20, #NMap1_NList1) AND NestedMap1.NList1[2] <= $NestedMap1_NList1_30 AND "
            + "(NestedMap1.NList1[2] = $NestedMap1_NList1_30 OR NestedList1[0] BETWEEN $NestedList1_4850 AND $Id2)"
            + " AND NOT NestedMap1.InPublication IN ($Id, $Id1, $Id20, $Id21)"));

  }

  /**
   * Test that directly uses executable expression to reduce the dependency on pattern-matcher.
   */
  @Test
  public void testSQLComparisonExpression2() {
    TestFieldsMap testFieldsMap1 = getPhoenixFieldMap1();
    TestFieldsMap compareValMap1 = getCompareValMap1();

    RawBsonDocument rawBsonDocument = TestUtil.getRawBsonDocument(testFieldsMap1);
    //{
    //  "$Id20": 101.011,
    //  "$Id2": 12,
    //  "#NestedList1_10": "1234abce",
    //  "$Id1": 120,
    //  "$Id10": 101,
    //  "$Ids1": "12",
    //  ":ISBN": "111-1111111111",
    //  "#NestedList1_xyz0123": "xyz0123",
    //  "$NestedList1_485": -485.33,
    //  "$NestedMap1_NList1_30": {
    //    "$binary": {
    //      "base64": "V2hpdGVl",
    //      "subType": "00"
    //    }
    //  },
    //  "InPublication": false,
    //  "$Ids10": "100",
    //  "#NestedMap1_NList1_3": {
    //    "$binary": {
    //      "base64": "V2hpdA==",
    //      "subType": "00"
    //    }
    //  },
    //  "#NestedList1_1": "1234abcc",
    //  "#NMap1_NList1": "NListVal01",
    //  "$NestedList1_4850": -485.35,
    //  "$Id": 101.01,
    //  "#Title": "Book 101 Title"
    //}
    RawBsonDocument compareValues = TestUtil.getRawBsonDocument(compareValMap1);

    SQLComparisonExpressionUtils SQLComparisonExpressionUtils =
            new SQLComparisonExpressionUtils(rawBsonDocument, compareValues);

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "(exists('Id') || !exists('Title'))"));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "((!exists('Id') && !exists('Title1')) || exists('ISBN2')) || " +
                    "((!isEquals('Id', '#Title'))" +
                    " && ((isEquals('InPublication', 'InPublication'))" +
                    " || ((isEquals('ISBN', ':ISBN')) && (isEquals('Title', '#Title')))))"));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "((exists('NestedMap1.ISBN') && !exists('NestedMap1.NList1[3]')))"));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "isEquals('NestedMap1.Id', '$Id')" +
                    " && (isEquals('NestedMap1.InPublication', 'InPublication'))"));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "((isEquals('NestedMap1.Id', '$Id'))" +
                    " && ((isEquals('NestedMap1.InPublication[0]', 'InPublication'))" +
                    " || ((isEquals('ISBN[0]', ':ISBN')) && (isEquals('Title', '#Title'))))" +
                    " || (isEquals('NestedMap1.NList1[0]', '#NMap1_NList1')))"));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "((!exists('Id') && !exists('Title1')) || exists('ISBN2')) ||" +
                    " ((isEquals('NestedMap1.Id', '$Id'))" +
                    " && ((isEquals('NestedMap1.InPublication', 'InPublication'))" +
                    " || ((isEquals('ISBN', ':ISBN')) && (isEquals('Title', '#Title')))))"));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "lessThanOrEquals('NestedList1[0]', '$NestedList1_485')" +
                    " && greaterThan('NestedList1[1]', '#NestedList1_1')" +
                    " && greaterThanOrEquals('NestedList1[2][0]', '#NestedList1_xyz0123')" +
                    " && lessThan('NestedList1[2][1].Id', '$Id1') && lessThan('IdS', '$Ids1')" +
                    " && greaterThan('Id2', '$Id2')" +
                    " && greaterThan('NestedMap1.NList1[2]', '#NestedMap1_NList1_3')"));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "lessThanOrEquals('NestedList1[0]', '$NestedList1_485')" +
                    " && greaterThanOrEquals('NestedList1[1]', '#NestedList1_1')" +
                    " && greaterThanOrEquals('NestedList1[2][0]', '#NestedList1_xyz0123')" +
                    " && lessThanOrEquals('NestedList1[2][1].Id', '$Id1')" +
                    " && lessThanOrEquals('IdS', '$Ids1')" +
                    " && greaterThanOrEquals('Id2', '$Id2')" +
                    " && greaterThanOrEquals('NestedMap1.NList1[2]', '#NestedMap1_NList1_3')"));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "greaterThanOrEquals('NestedList1[0]', '$NestedList1_4850')" +
                    " && lessThan('NestedList1[1]', '#NestedList1_10')" +
                    " && greaterThanOrEquals('NestedList1[2][0]', '#NestedList1_xyz0123')" +
                    " && greaterThan('NestedList1[2][1].Id', '$Id10')" +
                    " && greaterThan('IdS', '$Ids10') && lessThan('Id2', '$Id20')" +
                    " && lessThan('NestedMap1.NList1[2]', '$NestedMap1_NList1_30')"));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "greaterThanOrEquals('NestedList1[0]', '$NestedList1_4850')" +
                    " && lessThanOrEquals('NestedList1[1]', '#NestedList1_10')" +
                    " && greaterThanOrEquals('NestedList1[2][0]', '#NestedList1_xyz0123')" +
                    " && greaterThanOrEquals('NestedList1[2][1].Id', '$Id10')" +
                    " && greaterThanOrEquals('IdS', '$Ids10') && lessThanOrEquals('Id2', '$Id20')" +
                    " && lessThanOrEquals('NestedMap1.NList1[2]', '$NestedMap1_NList1_30')" +
                    " && !isEquals('NestedMap1.NList1[2]', '$NestedMap1_NList1_30')"));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "greaterThanOrEquals('NestedList1[0]', '$NestedList1_4850')" +
                    " && lessThanOrEquals('NestedList1[1]', '#NestedList1_10')" +
                    " && greaterThanOrEquals('NestedList1[2][0]', '#NestedList1_xyz0123')" +
                    " && greaterThanOrEquals('NestedList1[2][1].Id', '$Id10')" +
                    " && greaterThanOrEquals('IdS', '$Ids10')" +
                    " && lessThanOrEquals('Id2', '$Id20')" +
                    " && lessThanOrEquals('NestedMap1.NList1[2]', '$NestedMap1_NList1_30')" +
                    " && (isEquals('NestedMap1.NList1[2]', '$NestedMap1_NList1_30')" +
                    " || between('NestedList1[0]', '$NestedList1_4850', '$Id2'))"));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "greaterThanOrEquals('NestedList1[0]', '$NestedList1_4850')" +
                    " && lessThanOrEquals('NestedList1[1]', '#NestedList1_10')" +
                    " && greaterThanOrEquals('NestedList1[2][0]', '#NestedList1_xyz0123')" +
                    " && in('NestedMap1.NList1[0]', '$Id, $Id1, $Id20, #NMap1_NList1')" +
                    " && lessThanOrEquals('NestedMap1.NList1[2]', '$NestedMap1_NList1_30')" +
                    " && (isEquals('NestedMap1.NList1[2]', '$NestedMap1_NList1_30')" +
                    " || between('NestedList1[0]', '$NestedList1_4850', '$Id2'))"));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "greaterThanOrEquals('NestedList1[0]', '$NestedList1_4850')" +
                    " && lessThanOrEquals('NestedList1[1]', '#NestedList1_10')" +
                    " && greaterThanOrEquals('NestedList1[2][0]', '#NestedList1_xyz0123')" +
                    " && in('NestedMap1.NList1[0]', '$Id,  $Id1, $Id20, #NMap1_NList1')" +
                    " && lessThanOrEquals('NestedMap1.NList1[2]', '$NestedMap1_NList1_30')" +
                    " && (isEquals('NestedMap1.NList1[2]', '$NestedMap1_NList1_30')" +
                    " || between('NestedList1[0]', '$NestedList1_4850', '$Id2'))" +
                    " && !in('NestedMap1.InPublication', '$Id, $Id1, $Id20, $Id21')"));

  }

  @Test
  public void testDocumentComparisonExpression() {
    TestFieldsMap testFieldsMap1 = getPhoenixFieldMap1();

    RawBsonDocument rawBsonDocument = TestUtil.getRawBsonDocument(testFieldsMap1);
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

  private static TestFieldsMap getCompareValMap1() {
    TestFieldsMap compareValMap = new TestFieldsMap();
    Map<String, TestFieldValue> map2 = new HashMap<>();
    map2.put("$Id", new TestFieldValue().withN(101.01));
    map2.put("$Id1", new TestFieldValue().withN(120));
    map2.put("$Id10", new TestFieldValue().withN(101));
    map2.put("$Ids1", new TestFieldValue().withS("12"));
    map2.put("$Ids10", new TestFieldValue().withS("100"));
    map2.put("$Id2", new TestFieldValue().withN(12));
    map2.put("$Id20", new TestFieldValue().withN(101.011));
    map2.put("#Title", new TestFieldValue().withS("Book 101 Title"));
    map2.put(":ISBN", new TestFieldValue().withS("111-1111111111"));
    map2.put("InPublication", new TestFieldValue().withBOOL(false));
    map2.put("#NMap1_NList1", new TestFieldValue().withS("NListVal01"));
    map2.put("$NestedList1_485", new TestFieldValue().withN(-485.33));
    map2.put("$NestedList1_4850", new TestFieldValue().withN(-485.35));
    map2.put("#NestedList1_xyz0123", new TestFieldValue().withS("xyz0123"));
    map2.put("#NestedList1_1", new TestFieldValue().withS("1234abcc"));
    map2.put("#NestedList1_10", new TestFieldValue().withS("1234abce"));
    map2.put("#NestedMap1_NList1_3",
        new TestFieldValue().withB(new SerializableBytesPtr(Bytes.toBytes("Whit"))));
    map2.put("$NestedMap1_NList1_30",
        new TestFieldValue().withB(new SerializableBytesPtr(Bytes.toBytes("Whitee"))));
    compareValMap.setMap(map2);
    return compareValMap;
  }

  private static TestFieldsMap getPhoenixFieldMap1() {
    TestFieldsMap testFieldsMap1 = new TestFieldsMap();
    Map<String, TestFieldValue> map = new HashMap<>();
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
        new TestFieldValue().withL(
            new TestFieldValue().withS("NListVal01"),
            new TestFieldValue().withN(-0023.4),
            new TestFieldValue().withB(new SerializableBytesPtr(Bytes.toBytes("White")))));
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
    testFieldsMap1.setMap(map);
    return testFieldsMap1;
  }

}
