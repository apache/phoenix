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

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.util.bson.SQLComparisonExpressionUtils;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for BSON Condition Expression Utility.
 */
public class ComparisonExpressionUtils2Test {

  @Test
  public void testSQLComparisonExpression1() {
    RawBsonDocument rawBsonDocument = getDocumentValue();
    BsonDocument compareValues = getCompareValDocument();
    BsonDocument keyNames = getKeyNamesDocument();

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "(field_exists(#id) OR field_not_exists(#title))", rawBsonDocument, compareValues,
            keyNames));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "((field_not_exists(#id) AND field_not_exists(#title1)) OR field_exists(#isbn2))"
                    + " OR ((#id <> :title) AND ((#4 = :inpublication) OR ((#isbn = :isbn)"
                    + " AND (#title = :title))))", rawBsonDocument, compareValues, keyNames));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "((field_exists(#nestedmap1.#isbn) AND field_not_exists(#nestedmap1.#nlist1[3])))",
            rawBsonDocument, compareValues, keyNames));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "#nestedmap1.#id = :id AND (#nestedmap1.#4 = :inpublication)", rawBsonDocument,
            compareValues, keyNames));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "((#nestedmap1.#id = :id) AND ((#nestedmap1.#4[0] = :inpublication) OR "
                    + "((#isbn[0] = :isbn) AND (#title = :title))) OR "
                    + "(#nestedmap1.#nlist1[0] = :nmap1_nlist1))", rawBsonDocument, compareValues,
            keyNames));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "((field_not_exists(#id) AND field_not_exists(#title1)) OR field_exists(#isbn2))"
                    + " OR ((#nestedmap1.#id = :id) AND ((#nestedmap1.#4 = :inpublication)"
                    + " OR ((#isbn = :isbn) AND (#title = :title))))", rawBsonDocument,
            compareValues, keyNames));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "#nestedlist1[0] <= :nestedlist1_485 AND #nestedlist1[1] > :nestedlist1_1 AND "
                    + "#nestedlist1[2][0] >= :nestedlist1_xyz0123 AND "
                    +
                    "#nestedlist1[2][1].#id < :id1 AND #ids < :ids1 AND #id2 > :id2 AND #nestedmap1"
                    + ".#nlist1[2] > :nestedmap1_nlist1_3", rawBsonDocument, compareValues,
            keyNames));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "#nestedlist1[0] <= :nestedlist1_485 AND #nestedlist1[1] >= :nestedlist1_1 AND "
                    + "#8 >= :nestedlist1_xyz0123 AND "
                    + "#nestedlist1[2][1].#id <= :id1 AND #ids <= :ids1 AND #id2 >= :id2 AND"
                    + " #nestedmap1.#nlist1[2] >= :nestedmap1_nlist1_3", rawBsonDocument,
            compareValues, keyNames));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "#nestedlist1[0] >= :nestedlist1_4850 AND #nestedlist1[1] < :nestedlist1_10 AND"
                    + " #nestedlist1[2][0] >= :nestedlist1_xyz0123 AND "
                    + "#nestedlist1[2][1].#id > :id10 AND #ids > :ids10 AND #id2 < :id20 AND "
                    + "#nestedmap1.#nlist1[2] < :nestedmap1_nlist1_30", rawBsonDocument,
            compareValues, keyNames));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "#nestedlist1[0] >= :nestedlist1_4850 AND #nestedlist1[1] <= :nestedlist1_10 AND "
                    + "#nestedlist1[2][0] >= :nestedlist1_xyz0123 AND "
                    + "#nestedlist1[2][1].#id >= :id10 AND #ids >= :ids10 AND #id2 <= :id20 AND "
                    + "#nestedmap1.#nlist1[2] <= :nestedmap1_nlist1_30 AND "
                    + "#nestedmap1.#nlist1[2] <> :nestedmap1_nlist1_30", rawBsonDocument,
            compareValues, keyNames));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "#nestedlist1[0] >= :nestedlist1_4850 AND #nestedlist1[1] <= :nestedlist1_10 AND "
                    + "#nestedlist1[2][0] >= :nestedlist1_xyz0123 AND "
                    + "#nestedlist1[2][1].#id >= :id10 AND #ids >= :ids10 AND #id2 <= :id20 AND "
                    + "#nestedmap1.#nlist1[2] <= :nestedmap1_nlist1_30 AND "
                    + "(#nestedmap1.#nlist1[2] = :nestedmap1_nlist1_30 OR #nestedlist1[0] BETWEEN "
                    + ":nestedlist1_4850 AND :id2)", rawBsonDocument, compareValues, keyNames));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "#nestedlist1[0] >= :nestedlist1_4850 AND #nestedlist1[1] <= :nestedlist1_10 AND "
                    + "#nestedlist1[2][0] >= :nestedlist1_xyz0123 AND "
                    + "#nestedmap1.#nlist1[0] IN (:id, :id1, :id20, :nmap1_nlist1) AND "
                    + "#nestedmap1.#nlist1[2] <= :nestedmap1_nlist1_30 AND "
                    + "(#nestedmap1.#nlist1[2] = :nestedmap1_nlist1_30 OR #nestedlist1[0] BETWEEN "
                    + ":nestedlist1_4850 AND :id2)", rawBsonDocument, compareValues, keyNames));

    assertTrue(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "#nestedlist1[0] >= :nestedlist1_4850 AND #nestedlist1[1] <= :nestedlist1_10 AND "
                    + "#nestedlist1[2][0] >= :nestedlist1_xyz0123 AND "
                    + "#nestedmap1.#nlist1[0] IN (:id,  :id1, :id20, :nmap1_nlist1) AND "
                    + "#nestedmap1.#nlist1[2] <= :nestedmap1_nlist1_30 AND "
                    + "(#nestedmap1.#nlist1[2] = :nestedmap1_nlist1_30 OR "
                    + " #nestedlist1[0] BETWEEN :nestedlist1_4850 AND :id2)"
                    + " AND NOT #nestedmap1.#4 IN (:id, :id1, :id20, :id21) AND #7 = :7 "
                    + "AND #10 >= :id2 AND #11 <= :id2 AND #12 = :id2 AND #13 = :id2 AND "
                    + "NOT #14 <> :id2",
            rawBsonDocument, compareValues, keyNames));
  }

  /**
   * Test that directly uses executable expression to reduce the dependency on pattern-matcher.
   */
  @Test
  public void testSQLComparisonExpression2() {
    RawBsonDocument rawBsonDocument = getDocumentValue();
    BsonDocument compareValues = getCompareValDocument();
    BsonDocument keyNames = getKeyNamesDocument();

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "(field_not_exists(#id) OR field_not_exists(#title))", rawBsonDocument, compareValues,
            keyNames));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "((field_not_exists(#id) AND field_not_exists(#title1)) OR field_exists(#isbn2))"
                    + " OR ((#id = :title) AND ((#4 = #4) OR ((#isbn = :isbn)"
                    + " AND (#title = :title))))", rawBsonDocument, compareValues, keyNames));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "((field_exists(#nestedmap1.#isbn) AND field_exists(#nestedmap1.#nlist1[3])))",
            rawBsonDocument, compareValues, keyNames));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "#nestedmap1.#id = :id AND (#nestedmap1.#4 <> :inpublication)", rawBsonDocument,
            compareValues, keyNames));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "((#nestedmap1.#id = :id) AND ((#nestedmap1.#4[0] = :inpublication) OR "
                    + "((#isbn[0] = :isbn) AND (#title = :title))) OR "
                    + "(#nestedmap1.#nlist1[0] <> :nmap1_nlist1))", rawBsonDocument, compareValues,
            keyNames));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "((field_not_exists(#id) AND field_not_exists(#title1)) OR field_exists(#isbn2))"
                    + " OR ((#nestedmap1.#id = :id) AND ((#nestedmap1.#4 <> :inpublication)"
                    + " OR NOT ((#isbn = :isbn) AND (#title = :title))))", rawBsonDocument,
            compareValues, keyNames));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "#nestedlist1[0] <= :nestedlist1_485 AND #nestedlist1[1] > :nestedlist1_1 AND "
                    + "#nestedlist1[2][0] >= :nestedlist1_xyz0123 AND "
                    +
                    "#nestedlist1[2][1].#id < :id1 AND #ids < :ids1 AND #id2 > :id2 AND #nestedmap1"
                    + ".#nlist1[2] < :nestedmap1_nlist1_3", rawBsonDocument, compareValues,
            keyNames));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "#nestedlist1[0] <= :nestedlist1_485 AND #nestedlist1[1] >= :nestedlist1_1 AND "
                    + "#nestedlist1[2][0] >= :nestedlist1_xyz0123 AND "
                    +
                    "#nestedlist1[2][1].#id <= :id1 AND #ids <= :ids1 AND #id2 >= :id2 AND #nestedmap1"
                    + ".#nlist1[2] < :nestedmap1_nlist1_3", rawBsonDocument, compareValues,
            keyNames));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "#nestedlist1[0] >= :nestedlist1_4850 AND #nestedlist1[1] < :nestedlist1_10 AND "
                    + "#nestedlist1[2][0] >= :nestedlist1_xyz0123 AND "
                    +
                    "#nestedlist1[2][1].#id > :id10 AND #ids > :ids10 AND #id2 < :id20 AND #nestedmap1"
                    + ".#nlist1[2] >= :nestedmap1_nlist1_30", rawBsonDocument, compareValues,
            keyNames));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "#nestedlist1[0] >= :nestedlist1_4850 AND #nestedlist1[1] <= :nestedlist1_10 AND "
                    + "#nestedlist1[2][0] >= :nestedlist1_xyz0123 AND "
                    + "#nestedlist1[2][1].#id >= :id10 AND #ids >= :ids10 AND #id2 <= :id20 AND "
                    + "#nestedmap1.#nlist1[2] > :nestedmap1_nlist1_30 AND "
                    + "#nestedmap1.#nlist1[2] <> :nestedmap1_nlist1_30", rawBsonDocument,
            compareValues, keyNames));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "#nestedlist1[0] >= :nestedlist1_4850 AND #nestedlist1[1] <= :nestedlist1_10 AND "
                    + "#nestedlist1[2][0] >= :nestedlist1_xyz0123 AND "
                    + "#nestedlist1[2][1].#id >= :id10 AND #ids >= :ids10 AND #id2 <= :id20 AND "
                    + "#nestedmap1.#nlist1[2] <= :nestedmap1_nlist1_30 AND "
                    +
                    "(#nestedmap1.#nlist1[2] = :nestedmap1_nlist1_30 OR NOT #nestedlist1[0] BETWEEN "
                    + ":nestedlist1_4850 AND :id2)", rawBsonDocument, compareValues, keyNames));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "#nestedlist1[0] >= :nestedlist1_4850 AND #nestedlist1[1] <= :nestedlist1_10 AND "
                    + "#nestedlist1[2][0] >= :nestedlist1_xyz0123 AND "
                    + "#nestedmap1.#nlist1[0] NOT IN (:id, :id1, :id20, :nmap1_nlist1) AND "
                    + "#nestedmap1.#nlist1[2] <= :nestedmap1_nlist1_30 AND "
                    + "(#nestedmap1.#nlist1[2] = :nestedmap1_nlist1_30 OR #nestedlist1[0] BETWEEN "
                    + ":nestedlist1_4850 AND :id2)", rawBsonDocument, compareValues, keyNames));

    assertFalse(SQLComparisonExpressionUtils.evaluateConditionExpression(
            "#nestedlist1[0] >= :nestedlist1_4850 AND #nestedlist1[1] <= :nestedlist1_10 AND "
                    + "#nestedlist1[2][0] >= :nestedlist1_xyz0123 AND "
                    + "#nestedmap1.#nlist1[0] IN (:id,  :id1, :id20, :nmap1_nlist1) AND "
                    + "#nestedmap1.#nlist1[2] <= :nestedmap1_nlist1_30 AND "
                    + "(#nestedmap1.#nlist1[2] = :nestedmap1_nlist1_30 OR "
                    + " #nestedlist1[0] NOT BETWEEN :nestedlist1_4850 AND :id2)"
                    + " AND #nestedmap1.#4 IN (:id, :id1, :id20, :id21)",
            rawBsonDocument, compareValues, keyNames));
  }

  private static BsonDocument getCompareValDocument() {
    BsonDocument compareValues = new BsonDocument();
    compareValues.append(":id20", new BsonDouble(101.011));
    compareValues.append(":id2", new BsonInt32(12));
    compareValues.append(":nestedlist1_10", new BsonString("1234abce"));
    compareValues.append(":id1", new BsonInt32(120));
    compareValues.append(":id10", new BsonInt32(101));
    compareValues.append(":gt", new BsonInt32(100));
    compareValues.append(":ids1", new BsonString("12"));
    compareValues.append(":isbn", new BsonString("111-1111111111"));
    compareValues.append(":nestedlist1_xyz0123", new BsonString("xyz0123"));
    compareValues.append(":nestedlist1_485", new BsonDouble(-485.33));
    compareValues.append(":nestedmap1_nlist1_30", new BsonBinary(Bytes.toBytes("Whitee")));
    compareValues.append(":inpublication", new BsonBoolean(false));
    compareValues.append(":ids10", new BsonString("100"));
    compareValues.append(":nestedmap1_nlist1_3", new BsonBinary(Bytes.toBytes("Whit")));
    compareValues.append(":nestedlist1_1", new BsonString("1234abcc"));
    compareValues.append(":nmap1_nlist1", new BsonString("NListVal01"));
    compareValues.append(":nestedlist1_4850", new BsonDouble(-485.35));
    compareValues.append(":id", new BsonDouble(101.01));
    compareValues.append(":title", new BsonString("Book 101 Title"));
    compareValues.append(":zero", new BsonInt32(0));
    compareValues.append(":id21", new BsonInt32(121));
    compareValues.append(":7", new BsonString("Name_"));
    return compareValues;
  }

  private static BsonDocument getKeyNamesDocument() {
    BsonDocument keyNames = new BsonDocument();
    keyNames.append("#id", new BsonString("Id"));
    keyNames.append("#title", new BsonString("Title"));
    keyNames.append("#title1", new BsonString("Title1"));
    keyNames.append("#isbn", new BsonString("ISBN"));
    keyNames.append("#isbn2", new BsonString("ISBN2"));
    keyNames.append("#4", new BsonString("InPublication"));
    keyNames.append("#nestedmap1", new BsonString("NestedMap1"));
    keyNames.append("#nestedlist1", new BsonString("NestedList1"));
    keyNames.append("#nlist1", new BsonString("NList1"));
    keyNames.append("#ids", new BsonString("IdS"));
    keyNames.append("#id2", new BsonString("Id2"));
    keyNames.append("#7", new BsonString("Id.Name"));
    keyNames.append("#8", new BsonString("NestedList1[2][0]"));
    keyNames.append("#10", new BsonString(">"));
    keyNames.append("#11", new BsonString("["));
    keyNames.append("#12", new BsonString("#"));
    keyNames.append("#13", new BsonString("~"));
    keyNames.append("#14", new BsonString("^"));
    return keyNames;
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
            "  \"Id.Name\" : \"Name_\",\n" +
            "  \"IdS\" : \"101.01\",\n" +
            "  \">\" : 12,\n " +
            "  \"[\" : 12,\n " +
            "  \"#\" : 12,\n " +
            "  \"~\" : 12,\n " +
            "  \"^\" : 12,\n " +
            "  \"Title\" : \"Book 101 Title\",\n" +
            "  \"Id\" : 101.01\n" +
            "}";
    return RawBsonDocument.parse(json);
  }

}