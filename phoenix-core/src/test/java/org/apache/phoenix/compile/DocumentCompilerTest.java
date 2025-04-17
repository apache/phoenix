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
package org.apache.phoenix.compile;

import static org.junit.Assert.assertEquals;

import org.apache.phoenix.parse.BsonExpressionParser;
import org.apache.phoenix.parse.ParseNode;
import org.junit.Test;

public class DocumentCompilerTest {

  @Test
  public void testDocParser() throws Exception {
    BsonExpressionParser bsonExpressionParser =
      new BsonExpressionParser("organization_id IN (:s, :ab12 ) AND entity_id "
        + "IN (:ab, :1, :09, :8a, :a1, $x07, $1jr, '$abc')");
    ParseNode parseNode = bsonExpressionParser.parseExpression();
    assertEquals("('organization_id' IN(':s',':ab12') AND 'entity_id' IN(':ab',':1',':09',"
      + "':8a',':a1','$x07','$1jr','$abc'))", parseNode.toString());

    bsonExpressionParser = new BsonExpressionParser("((8a0 = :0) AND (81 = :1) AND (82 = :2)) AND "
      + "(((attribute_not_exists(83) AND attribute_not_exists(84)) OR "
      + "attribute_exists(85)) OR (86 = :3))");
    parseNode = bsonExpressionParser.parseExpression();
    assertEquals(
      "(('8a0' = ':0' AND '81' = ':1' AND '82' = ':2') AND (((field_not_exists"
        + "('83') AND field_not_exists('84')) OR field_exists('85')) OR " + "'86' = ':3'))",
      parseNode.toString());

    bsonExpressionParser = new BsonExpressionParser("((8a0 = :0) AND (81 = :1) AND (82 = :2)) AND "
      + "(((attribute_not_exists(ab) AND attribute_not_exists(3df)) OR "
      + "attribute_exists(ab9)) OR (86 = :3))");
    parseNode = bsonExpressionParser.parseExpression();
    assertEquals(
      "(('8a0' = ':0' AND '81' = ':1' AND '82' = ':2') AND (((field_not_exists"
        + "('ab') AND field_not_exists('3df')) OR field_exists('ab9')) OR " + "'86' = ':3'))",
      parseNode.toString());

    bsonExpressionParser = new BsonExpressionParser(
      "12SBRHwe2e4J.NestedList1[0] >= :NestedList1_4850 AND " + "NestedList1[1] <= "
        + ":NestedList1_10 AND NestedList1[2][0] >= :NestedList1_xyz0123 AND "
        + "4NestedList1[2][1].Id >= :Id10 AND IdS >= :Ids10 AND Id2 <= :Id20 AND "
        + "NestedMap1.NList1[2] <= :NestedMap1_NList1_30 AND "
        + "(NestedMap1.NList1[2] = :NestedMap1_NList1_30 OR NestedList1[0] BETWEEN "
        + ":NestedList1_4850 AND :Id2)");
    parseNode = bsonExpressionParser.parseExpression();
    assertEquals("('12SBRHwe2e4J.NestedList1[0]' >= ':NestedList1_4850' AND 'NestedList1[1]' "
      + "<= ':NestedList1_10' AND 'NestedList1[2][0]' >= ':NestedList1_xyz0123' AND "
      + "'4NestedList1[2][1].Id' >= ':Id10' AND 'IdS' >= ':Ids10' AND 'Id2' <= ':Id20' "
      + "AND 'NestedMap1.NList1[2]' <= ':NestedMap1_NList1_30' AND ('NestedMap1"
      + ".NList1[2]' = ':NestedMap1_NList1_30' OR 'NestedList1[0]' BETWEEN "
      + "':NestedList1_4850' AND ':Id2'))", parseNode.toString());

    bsonExpressionParser =
      new BsonExpressionParser("NestedList1[0] >= $NestedList1_4850 AND NestedList1[1] <= "
        + "#NestedList1_10 AND NestedList1[2][0] >= #NestedList1_xyz0123 AND "
        + "NestedMap1.NList1[0] IN ($Id,  $Id1, $Id20, #NMap1_NList1) AND "
        + "NestedMap1.NList1[2] <= $NestedMap1_NList1_30 AND "
        + "(NestedMap1.NList1[2] = $NestedMap1_NList1_30 OR NestedList1[0] BETWEEN "
        + "$NestedList1_4850 AND $Id2) AND NOT NestedMap1.InPublication "
        + "IN ($Id, $Id1, $Id20, $Id21)");
    parseNode = bsonExpressionParser.parseExpression();
    assertEquals(
      "('NestedList1[0]' >= '$NestedList1_4850' AND 'NestedList1[1]' <= "
        + "'#NestedList1_10' AND 'NestedList1[2][0]' >= '#NestedList1_xyz0123' AND "
        + "'NestedMap1.NList1[0]' IN('$Id','$Id1','$Id20','#NMap1_NList1') AND " + "'NestedMap1"
        + ".NList1[2]' <= '$NestedMap1_NList1_30' AND ('NestedMap1.NList1[2]' = "
        + "'$NestedMap1_NList1_30' OR 'NestedList1[0]' BETWEEN '$NestedList1_4850' "
        + "AND '$Id2') AND  NOT 'NestedMap1.InPublication' " + "IN('$Id','$Id1','$Id20','$Id21'))",
      parseNode.toString());

    bsonExpressionParser =
      new BsonExpressionParser("result[2].location.coordinates.latitude > $latitude OR "
        + "(field_exists(result[1].location) AND result[1].location.state != $state"
        + " AND field_exists(result[4].emails[1]))");
    parseNode = bsonExpressionParser.parseExpression();
    assertEquals("('result[2].location.coordinates.latitude' > '$latitude' OR (field_exists"
      + "('result[1].location') AND 'result[1].location.state' != '$state' AND "
      + "field_exists('result[4].emails[1]')))", parseNode.toString());
  }

}
