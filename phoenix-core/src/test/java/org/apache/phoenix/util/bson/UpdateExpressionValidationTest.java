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
package org.apache.phoenix.util.bson;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.phoenix.expression.util.bson.BsonUpdateInvalidArgumentException;
import org.apache.phoenix.expression.util.bson.UpdateExpressionUtils;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.junit.Test;

/**
 * Tests for BSON Update Expression validation logic.
 */
public class UpdateExpressionValidationTest {

  // UNSET operations

  @Test
  public void testUnsetFieldMissing() {
    // UNSET a: a missing -- No-Op
    BsonDocument doc = BsonDocument.parse("{}");
    String updateExpression = "{ \"$UNSET\": { \"a\": null } }";
    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    UpdateExpressionUtils.updateExpression(expressionDoc, doc);
    assertEquals("{}", doc.toJson());
  }

  @Test
  public void testUnsetFieldExists() {
    // UNSET a: a exists -- Remove
    BsonDocument doc = BsonDocument.parse("{ \"a\": \"value\" }");
    String updateExpression = "{ \"$UNSET\": { \"a\": null } }";
    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    UpdateExpressionUtils.updateExpression(expressionDoc, doc);
    assertEquals("{}", doc.toJson());
  }

  @Test
  public void testUnsetNestedParentMissing() {
    // UNSET a.b: Parent a missing -- Exception
    BsonDocument doc = BsonDocument.parse("{}");
    String updateExpression = "{ \"$UNSET\": { \"a.b\": null } }";
    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    try {
      UpdateExpressionUtils.updateExpression(expressionDoc, doc);
      fail("Expected BsonUpdateInvalidArgumentException");
    } catch (BsonUpdateInvalidArgumentException e) {
      // expected
    }
  }

  @Test
  public void testUnsetParentNotMap() {
    // UNSET a.b: a exists but not a map -- Exception
    BsonDocument doc = BsonDocument.parse("{ \"a\": \"notAMap\" }");
    String updateExpression = "{ \"$UNSET\": { \"a.b\": null } }";
    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    try {
      UpdateExpressionUtils.updateExpression(expressionDoc, doc);
      fail("Expected BsonUpdateInvalidArgumentException");
    } catch (BsonUpdateInvalidArgumentException e) {
      // expected
    }
  }

  @Test
  public void testUnsetNestedFieldMissing() {
    // UNSET a.b: a exists but b does not exist -- No-Op
    BsonDocument doc = BsonDocument.parse("{ \"a\": {} }");
    String updateExpression = "{ \"$UNSET\": { \"a.b\": null } }";
    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    UpdateExpressionUtils.updateExpression(expressionDoc, doc);
    assertEquals("{\"a\": {}}", doc.toJson());
  }

  @Test
  public void testUnsetNestedFieldExists() {
    // UNSET a.b: a exists and b exists -- Remove
    BsonDocument doc = BsonDocument.parse("{ \"a\": { \"b\": \"value\" } }");
    String updateExpression = "{ \"$UNSET\": { \"a.b\": null } }";
    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    UpdateExpressionUtils.updateExpression(expressionDoc, doc);
    assertEquals("{\"a\": {}}", doc.toJson());
  }

  @Test
  public void testUnsetArrayParentMissing() {
    // UNSET a[i]: a missing -- Exception
    BsonDocument doc = BsonDocument.parse("{}");
    String updateExpression = "{ \"$UNSET\": { \"a[0]\": null } }";
    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    try {
      UpdateExpressionUtils.updateExpression(expressionDoc, doc);
      fail("Expected BsonUpdateInvalidArgumentException");
    } catch (BsonUpdateInvalidArgumentException e) {
      // expected
    }
  }

  @Test
  public void testUnsetParentNotList() {
    // UNSET a[i]: a exists but not a list -- Exception
    BsonDocument doc = BsonDocument.parse("{ \"a\": \"notAList\" }");
    String updateExpression = "{ \"$UNSET\": { \"a[0]\": null } }";
    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    try {
      UpdateExpressionUtils.updateExpression(expressionDoc, doc);
      fail("Expected BsonUpdateInvalidArgumentException");
    } catch (BsonUpdateInvalidArgumentException e) {
      // expected
    }
  }

  @Test
  public void testUnsetArrayIndexOutOfRange() {
    // UNSET a[i]: Array index out of range -- No-Op
    BsonDocument doc = BsonDocument.parse("{ \"a\": [\"item1\"] }");
    String updateExpression = "{ \"$UNSET\": { \"a[5]\": null } }";
    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    UpdateExpressionUtils.updateExpression(expressionDoc, doc);
    assertEquals("{\"a\": [\"item1\"]}", doc.toJson());
  }

  @Test
  public void testUnsetArrayIndexValid() {
    // UNSET a[i]: a exists and index is valid -- Remove
    BsonDocument doc = BsonDocument.parse("{ \"a\": [\"item1\", \"item2\"] }");
    String updateExpression = "{ \"$UNSET\": { \"a[0]\": null } }";
    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    UpdateExpressionUtils.updateExpression(expressionDoc, doc);
    assertEquals("{\"a\": [\"item2\"]}", doc.toJson());
  }

  // SET operations

  @Test
  public void testSetFieldMissing() {
    // SET a: a missing -- Set
    BsonDocument doc = BsonDocument.parse("{}");
    String updateExpression = "{ \"$SET\": { \"a\": \"value\" } }";
    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    UpdateExpressionUtils.updateExpression(expressionDoc, doc);
    assertEquals("{\"a\": \"value\"}", doc.toJson());
  }

  @Test
  public void testSetNestedParentMissing() {
    // SET a.b: a missing -- Exception
    BsonDocument doc = BsonDocument.parse("{}");
    String updateExpression = "{ \"$SET\": { \"a.b\": \"value\" } }";
    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    try {
      UpdateExpressionUtils.updateExpression(expressionDoc, doc);
      fail("Expected BsonUpdateInvalidArgumentException");
    } catch (BsonUpdateInvalidArgumentException e) {
      // expected
    }
  }

  @Test
  public void testSetNestedFieldMissing() {
    // SET a.b: a exists but b missing -- Set
    BsonDocument doc = BsonDocument.parse("{ \"a\": {} }");
    String updateExpression = "{ \"$SET\": { \"a.b\": \"value\" } }";
    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    UpdateExpressionUtils.updateExpression(expressionDoc, doc);
    assertEquals("{\"a\": {\"b\": \"value\"}}", doc.toJson());
  }

  @Test
  public void testSetParentNotMap() {
    // SET a.b: a exists but not a map -- Exception
    BsonDocument doc = BsonDocument.parse("{ \"a\": \"notAMap\" }");
    String updateExpression = "{ \"$SET\": { \"a.b\": \"value\" } }";
    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    try {
      UpdateExpressionUtils.updateExpression(expressionDoc, doc);
      fail("Expected BsonUpdateInvalidArgumentException");
    } catch (BsonUpdateInvalidArgumentException e) {
      // expected
    }
  }

  @Test
  public void testSetArrayParentMissing() {
    // SET a[i]: a missing -- Exception
    BsonDocument doc = BsonDocument.parse("{}");
    String updateExpression = "{ \"$SET\": { \"a[0]\": \"value\" } }";
    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    try {
      UpdateExpressionUtils.updateExpression(expressionDoc, doc);
      fail("Expected BsonUpdateInvalidArgumentException");
    } catch (BsonUpdateInvalidArgumentException e) {
      // expected
    }
  }

  @Test
  public void testSetArrayParentNotList() {
    // SET a[i]: a exists but not a list -- Exception
    BsonDocument doc = BsonDocument.parse("{ \"a\": \"notAList\" }");
    String updateExpression = "{ \"$SET\": { \"a[0]\": \"value\" } }";
    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    try {
      UpdateExpressionUtils.updateExpression(expressionDoc, doc);
      fail("Expected BsonUpdateInvalidArgumentException");
    } catch (BsonUpdateInvalidArgumentException e) {
      // expected
    }
  }

  @Test
  public void testSetArrayIndexBeyondSize1() {
    // SET a[i]: Index beyond array size -- Append
    BsonDocument doc = BsonDocument.parse("{ \"a\": [\"item1\"] }");
    String updateExpression = "{ \"$SET\": { \"a[1]\": \"item2\" } }";
    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    UpdateExpressionUtils.updateExpression(expressionDoc, doc);
    assertEquals("{\"a\": [\"item1\", \"item2\"]}", doc.toJson());
  }

  @Test
  public void testSetArrayIndexBeyondSize2() {
    // SET a[i]: Index beyond array size -- Append
    BsonDocument doc = BsonDocument.parse("{ \"a\": [\"item1\"] }");
    String updateExpression = "{ \"$SET\": { \"a[6]\": \"item2\" } }";
    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    UpdateExpressionUtils.updateExpression(expressionDoc, doc);
    assertEquals("{\"a\": [\"item1\", \"item2\"]}", doc.toJson());
  }

  @Test
  public void testSetArrayIndexValid() {
    // SET a[i]: a exists and index is valid -- Set
    BsonDocument doc = BsonDocument.parse("{ \"a\": [\"item1\", \"item2\"] }");
    String updateExpression = "{ \"$SET\": { \"a[0]\": \"newValue\" } }";
    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    UpdateExpressionUtils.updateExpression(expressionDoc, doc);
    assertEquals("{\"a\": [\"newValue\", \"item2\"]}", doc.toJson());
  }

  // ADD operations

  @Test
  public void testAddFieldNotNumberOrSet() {
    // ADD a: a exists but not number/set -- Exception
    BsonDocument doc = BsonDocument.parse("{ \"a\": \"string\" }");
    String updateExpression = "{ \"$ADD\": { \"a\": 5 } }";
    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    try {
      UpdateExpressionUtils.updateExpression(expressionDoc, doc);
      fail("Expected BsonUpdateInvalidArgumentException");
    } catch (BsonUpdateInvalidArgumentException e) {
      // expected
    }
  }

  @Test
  public void testAddSetDifferentType() {
    // ADD a: a is set of different type -- Exception
    BsonDocument doc = BsonDocument.parse("{ \"a\": { \"$set\": [\"string1\"] } }");
    String updateExpression = "{ \"$ADD\": { \"a\": { \"$set\": [123] } } }";
    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    try {
      UpdateExpressionUtils.updateExpression(expressionDoc, doc);
      fail("Expected BsonUpdateInvalidArgumentException");
    } catch (BsonUpdateInvalidArgumentException e) {
      // expected
    }
  }

  @Test
  public void testAddNumberMissing() {
    // ADD a num: a missing -- a=0, Add
    BsonDocument doc = BsonDocument.parse("{}");
    String updateExpression = "{ \"$ADD\": { \"a\": 5 } }";
    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    UpdateExpressionUtils.updateExpression(expressionDoc, doc);
    assertEquals("{\"a\": 5}", doc.toJson());
  }

  @Test
  public void testAddNumberExists() {
    // ADD a num: a exists -- Add
    BsonDocument doc = BsonDocument.parse("{ \"a\": 10 }");
    String updateExpression = "{ \"$ADD\": { \"a\": 5 } }";
    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    UpdateExpressionUtils.updateExpression(expressionDoc, doc);
    assertEquals("{\"a\": 15}", doc.toJson());
  }

  @Test
  public void testAddSetMissing() {
    // ADD a set: a missing -- a={}, Add
    BsonDocument doc = BsonDocument.parse("{}");
    String updateExpression = "{ \"$ADD\": { \"a\": { \"$set\": [\"item1\"] } } }";
    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    UpdateExpressionUtils.updateExpression(expressionDoc, doc);
    assertEquals("{\"a\": {\"$set\": [\"item1\"]}}", doc.toJson());
  }

  @Test
  public void testAddSetSameType() {
    // ADD a set: a is set of same type -- Add
    BsonDocument doc = BsonDocument.parse("{ \"a\": { \"$set\": [\"item1\"] } }");
    String updateExpression = "{ \"$ADD\": { \"a\": { \"$set\": [\"item2\"] } } }";
    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    UpdateExpressionUtils.updateExpression(expressionDoc, doc);
    // Note: Order may vary in sets, so we check that both items are present
    BsonDocument result = doc;
    String json = result.toJson();
    assert json.contains("item1");
    assert json.contains("item2");
  }

  // DELETE_FROM_SET operations

  @Test
  public void testDeleteFromSetMissing() {
    // DELETE_FROM_SET a: a missing -- No-Op
    BsonDocument doc = BsonDocument.parse("{}");
    String updateExpression = "{ \"$DELETE_FROM_SET\": { \"a\": { \"$set\": [\"item1\"] } } }";
    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    UpdateExpressionUtils.updateExpression(expressionDoc, doc);
    assertEquals("{}", doc.toJson());
  }

  @Test
  public void testDeleteFromSetNotSet() {
    // DELETE_FROM_SET a: a exists but not a set -- Exception
    BsonDocument doc = BsonDocument.parse("{ \"a\": \"string\" }");
    String updateExpression = "{ \"$DELETE_FROM_SET\": { \"a\": { \"$set\": [\"item1\"] } } }";
    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    try {
      UpdateExpressionUtils.updateExpression(expressionDoc, doc);
      fail("Expected BsonUpdateInvalidArgumentException");
    } catch (BsonUpdateInvalidArgumentException e) {
      // expected
    }
  }

  @Test
  public void testDeleteFromSetDifferentType() {
    // DELETE_FROM_SET a: a is set of different type -- Exception
    BsonDocument doc = BsonDocument.parse("{ \"a\": { \"$set\": [\"string1\"] } }");
    String updateExpression = "{ \"$DELETE_FROM_SET\": { \"a\": { \"$set\": [123] } } }";
    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    try {
      UpdateExpressionUtils.updateExpression(expressionDoc, doc);
      fail("Expected BsonUpdateInvalidArgumentException");
    } catch (BsonUpdateInvalidArgumentException e) {
      // expected
    }
  }

  @Test
  public void testDeleteFromSetSameType() {
    // DELETE_FROM_SET a: a is set of same type -- Delete
    BsonDocument doc =
      BsonDocument.parse("{ \"a\": { \"$set\": [\"item1\", \"item2\", \"item3\"] } }");
    String updateExpression = "{ \"$DELETE_FROM_SET\": { \"a\": { \"$set\": [\"item2\"] } } }";
    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    UpdateExpressionUtils.updateExpression(expressionDoc, doc);
    // Result should contain item1 and item3, but not item2
    String json = doc.toJson();
    assert json.contains("item1");
    assert json.contains("item3");
    assert !json.contains("item2");
  }
}
