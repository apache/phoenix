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

import org.apache.phoenix.expression.util.bson.BsonUpdateInvalidArgumentException;
import org.apache.phoenix.expression.util.bson.UpdateExpressionUtils;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.junit.Assert;
import org.junit.Test;

/**
 * Validation tests for BSON Update Expression Utility. These tests verify that proper exceptions
 * are thrown for invalid update expressions.
 */
public class UpdateExpressionValidationTest {

  @Test
  public void testNonExistentParentMap() {
    BsonDocument bsonDocument = new BsonDocument();
    String updateExpression =
      "{\n" + "  \"$SET\": {\n" + "    \"nonExistentMap.field\": \"value\"\n" + "  }\n" + "}";

    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    try {
      UpdateExpressionUtils.updateExpression(expressionDoc, bsonDocument);
      Assert.fail("Expected BsonUpdateInvalidArgumentException for non-existent parent map");
    } catch (BsonUpdateInvalidArgumentException e) {
      // expected
    }
  }

  @Test
  public void testNonExistentArrayIndex() {
    BsonDocument bsonDocument =
      RawBsonDocument.parse("{\n" + "  \"arrayField\": [\"item1\", \"item2\"]\n" + "}");

    String updateExpression =
      "{\n" + "  \"$ADD\": {\n" + "    \"arrayField[5]\": 10\n" + "  }\n" + "}";

    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    try {
      UpdateExpressionUtils.updateExpression(expressionDoc, bsonDocument);
      Assert.fail("Expected BsonUpdateInvalidArgumentException for non-existent array index");
    } catch (BsonUpdateInvalidArgumentException e) {
      // expected
    }
  }

  @Test
  public void testAddNonNumericToNumeric() {
    BsonDocument bsonDocument = RawBsonDocument.parse("{\n" + "  \"numericField\": 42\n" + "}");

    String updateExpression =
      "{\n" + "  \"$ADD\": {\n" + "    \"numericField\": \"notANumber\"\n" + "  }\n" + "}";

    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    try {
      UpdateExpressionUtils.updateExpression(expressionDoc, bsonDocument);
      Assert.fail("Expected BsonUpdateInvalidArgumentException for type mismatch");
    } catch (BsonUpdateInvalidArgumentException e) {
      // expected
    }
  }

  @Test
  public void testAddIncompatibleSetTypes1() {
    BsonDocument bsonDocument = RawBsonDocument.parse("{\n" + "  \"stringSet\": {\n"
      + "    \"$set\": [\"string1\", \"string2\"]\n" + "  }\n" + "}");

    String updateExpression = "{\n" + "  \"$ADD\": {\n" + "    \"stringSet\": {\n"
      + "      \"$set\": [123, 456]\n" + "    }\n" + "  }\n" + "}";

    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    try {
      UpdateExpressionUtils.updateExpression(expressionDoc, bsonDocument);
      Assert.fail("Expected BsonUpdateInvalidArgumentException for incompatible set types");
    } catch (BsonUpdateInvalidArgumentException e) {
      // expected
    }
  }

  @Test
  public void testAddIncompatibleSetTypes2() {
    BsonDocument bsonDocument = RawBsonDocument
      .parse("{\n" + "  \"numberSet\": {\n" + "    \"$set\": [1, 2]\n" + "  }\n" + "}");

    String updateExpression = "{\n" + "  \"$ADD\": {\n" + "    \"numberSet\": {\n"
      + "      \"$set\": [\"123\", \"456\"]\n" + "    }\n" + "  }\n" + "}";

    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    try {
      UpdateExpressionUtils.updateExpression(expressionDoc, bsonDocument);
      Assert.fail("Expected BsonUpdateInvalidArgumentException for incompatible set types");
    } catch (BsonUpdateInvalidArgumentException e) {
      // expected
    }
  }

  @Test
  public void testDeleteFromSetOnNonSet() {
    BsonDocument bsonDocument =
      RawBsonDocument.parse("{\n" + "  \"regularField\": \"notASet\"\n" + "}");

    String updateExpression = "{\n" + "  \"$DELETE_FROM_SET\": {\n" + "    \"regularField\": {\n"
      + "      \"$set\": [\"value1\"]\n" + "    }\n" + "  }\n" + "}";

    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    try {
      UpdateExpressionUtils.updateExpression(expressionDoc, bsonDocument);
      Assert
        .fail("Expected BsonUpdateInvalidArgumentException for DELETE_FROM_SET on non-set field");
    } catch (BsonUpdateInvalidArgumentException e) {
      // expected
    }
  }

  @Test
  public void testAddInvalidValueTypeForNewField() {
    BsonDocument bsonDocument = new BsonDocument();
    String updateExpression =
      "{\n" + "  \"$ADD\": {\n" + "    \"newField\": \"invalidType\"\n" + "  }\n" + "}";

    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    try {
      UpdateExpressionUtils.updateExpression(expressionDoc, bsonDocument);
      Assert.fail(
        "Expected BsonUpdateInvalidArgumentException for invalid value type in ADD operation");
    } catch (BsonUpdateInvalidArgumentException e) {
      // expected
    }
  }

  @Test
  public void testDeleteFromSetWithNonSetValue() {
    BsonDocument bsonDocument = new BsonDocument();
    String updateExpression =
      "{\n" + "  \"$DELETE_FROM_SET\": {\n" + "    \"someField\": \"notASet\"\n" + "  }\n" + "}";

    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    try {
      UpdateExpressionUtils.updateExpression(expressionDoc, bsonDocument);
      Assert
        .fail("Expected BsonUpdateInvalidArgumentException for DELETE_FROM_SET with non-set value");
    } catch (BsonUpdateInvalidArgumentException e) {
      // expected
    }
  }

  @Test
  public void testAddNonSetToSetField() {
    BsonDocument bsonDocument = RawBsonDocument.parse(
      "{\n" + "  \"setField\": {\n" + "    \"$set\": [\"item1\", \"item2\"]\n" + "  }\n" + "}");

    String updateExpression =
      "{\n" + "  \"$ADD\": {\n" + "    \"setField\": \"notASet\"\n" + "  }\n" + "}";

    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    try {
      UpdateExpressionUtils.updateExpression(expressionDoc, bsonDocument);
      Assert.fail("Expected BsonUpdateInvalidArgumentException for adding non-set to set field");
    } catch (BsonUpdateInvalidArgumentException e) {
      // expected
    }
  }

  @Test
  public void testNestedArrayIndexOutOfBoundsUnset() {
    BsonDocument bsonDocument = RawBsonDocument
      .parse("{\n" + "  \"nested\": {\n" + "    \"array\": [1, 2]\n" + "  }\n" + "}");

    String updateExpression =
      "{\n" + "  \"$UNSET\": {\n" + "    \"nested.array[5]\": null\n" + "  }\n" + "}";

    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    try {
      UpdateExpressionUtils.updateExpression(expressionDoc, bsonDocument);
      Assert
        .fail("Expected BsonUpdateInvalidArgumentException for array index out of bounds in UNSET");
    } catch (BsonUpdateInvalidArgumentException e) {
      // expected
    }
  }

  @Test
  public void testNestedArrayIndexOutOfBoundsDeleteFromSet() {
    BsonDocument bsonDocument = RawBsonDocument.parse("{\n" + "  \"nested\": {\n"
      + "    \"array\": [{\n" + "      \"$set\": [\"item1\"]\n" + "    }]\n" + "  }\n" + "}");

    String updateExpression = "{\n" + "  \"$DELETE_FROM_SET\": {\n" + "    \"nested.array[5]\": {\n"
      + "      \"$set\": [\"item1\"]\n" + "    }\n" + "  }\n" + "}";

    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    try {
      UpdateExpressionUtils.updateExpression(expressionDoc, bsonDocument);
      Assert.fail(
        "Expected BsonUpdateInvalidArgumentException for array index out of bounds in DELETE_FROM_SET");
    } catch (BsonUpdateInvalidArgumentException e) {
      // expected
    }
  }

  @Test
  public void testNonExistentNestedDocumentPath() {
    BsonDocument bsonDocument = RawBsonDocument
      .parse("{\n" + "  \"existing\": {\n" + "    \"field\": \"value\"\n" + "  }\n" + "}");

    String updateExpression = "{\n" + "  \"$SET\": {\n"
      + "    \"existing.nonExistent.deepField\": \"value\"\n" + "  }\n" + "}";

    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    try {
      UpdateExpressionUtils.updateExpression(expressionDoc, bsonDocument);
      Assert
        .fail("Expected BsonUpdateInvalidArgumentException for non-existent nested document path");
    } catch (BsonUpdateInvalidArgumentException e) {
      // expected
    }
  }

  @Test
  public void testNonExistentNestedArrayPath() {
    BsonDocument bsonDocument = RawBsonDocument
      .parse("{\n" + "  \"existing\": {\n" + "    \"field\": \"value\"\n" + "  }\n" + "}");

    String updateExpression = "{\n" + "  \"$SET\": {\n"
      + "    \"existing.nonExistentArray[0]\": \"value\"\n" + "  }\n" + "}";

    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    try {
      UpdateExpressionUtils.updateExpression(expressionDoc, bsonDocument);
      Assert.fail("Expected BsonUpdateInvalidArgumentException for non-existent nested array path");
    } catch (BsonUpdateInvalidArgumentException e) {
      // expected
    }
  }

  @Test
  public void testParentPathNotDocument() {
    BsonDocument bsonDocument =
      RawBsonDocument.parse("{\n" + "  \"stringField\": \"notADocument\"\n" + "}");

    String updateExpression =
      "{\n" + "  \"$SET\": {\n" + "    \"stringField.subField\": \"value\"\n" + "  }\n" + "}";

    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    try {
      UpdateExpressionUtils.updateExpression(expressionDoc, bsonDocument);
      Assert
        .fail("Expected BsonUpdateInvalidArgumentException for parent path not being a document");
    } catch (BsonUpdateInvalidArgumentException e) {
      // expected
    }
  }

  @Test
  public void testParentPathNotArray() {
    BsonDocument bsonDocument =
      RawBsonDocument.parse("{\n" + "  \"stringField\": \"notAnArray\"\n" + "}");

    String updateExpression =
      "{\n" + "  \"$SET\": {\n" + "    \"stringField[0]\": \"value\"\n" + "  }\n" + "}";

    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    try {
      UpdateExpressionUtils.updateExpression(expressionDoc, bsonDocument);
      Assert.fail("Expected BsonUpdateInvalidArgumentException for parent path not being an array");
    } catch (BsonUpdateInvalidArgumentException e) {
      // expected
    }
  }

  @Test
  public void testDeleteFromSetIncompatibleTypesNested() {
    BsonDocument bsonDocument =
      RawBsonDocument.parse("{\n" + "  \"nested\": {\n" + "    \"stringSet\": {\n"
        + "      \"$set\": [\"string1\", \"string2\"]\n" + "    }\n" + "  }\n" + "}");

    String updateExpression =
      "{\n" + "  \"$DELETE_FROM_SET\": {\n" + "    \"nested.stringSet\": {\n"
        + "      \"$set\": [123, 456]\n" + "    }\n" + "  }\n" + "}";

    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    try {
      UpdateExpressionUtils.updateExpression(expressionDoc, bsonDocument);
      Assert.fail(
        "Expected BsonUpdateInvalidArgumentException for DELETE_FROM_SET with incompatible set types");
    } catch (BsonUpdateInvalidArgumentException e) {
      // expected
    }
  }

  @Test
  public void testUnsetNonExistentNestedPath() {
    BsonDocument bsonDocument =
      RawBsonDocument.parse("{\n" + "  \"existingField\": \"value\"\n" + "}");

    String updateExpression =
      "{\n" + "  \"$UNSET\": {\n" + "    \"nonExistent.nested.field\": \"\"\n" + "  }\n" + "}";

    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    try {
      UpdateExpressionUtils.updateExpression(expressionDoc, bsonDocument);
      Assert
        .fail("Expected BsonUpdateInvalidArgumentException for UNSET on non-existent nested path");
    } catch (BsonUpdateInvalidArgumentException e) {
      // expected
    }
  }

  @Test
  public void testUnsetArrayIndexOutOfBounds() {
    BsonDocument bsonDocument =
      RawBsonDocument.parse("{\n" + "  \"arrayField\": [\"item1\", \"item2\"]\n" + "}");

    String updateExpression =
      "{\n" + "  \"$UNSET\": {\n" + "    \"arrayField[10]\": \"\"\n" + "  }\n" + "}";

    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    try {
      UpdateExpressionUtils.updateExpression(expressionDoc, bsonDocument);
      Assert
        .fail("Expected BsonUpdateInvalidArgumentException for UNSET on array index out of bounds");
    } catch (BsonUpdateInvalidArgumentException e) {
      // expected
    }
  }

  @Test
  public void testUnsetInvalidParentType() {
    BsonDocument bsonDocument =
      RawBsonDocument.parse("{\n" + "  \"stringField\": \"simpleString\"\n" + "}");

    String updateExpression =
      "{\n" + "  \"$UNSET\": {\n" + "    \"stringField.nested\": \"\"\n" + "  }\n" + "}";

    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    try {
      UpdateExpressionUtils.updateExpression(expressionDoc, bsonDocument);
      Assert.fail(
        "Expected BsonUpdateInvalidArgumentException for UNSET when parent is not a document");
    } catch (BsonUpdateInvalidArgumentException e) {
      // expected
    }
  }

  @Test
  public void testMultipleOperationsWithOneInvalid() {
    BsonDocument bsonDocument = new BsonDocument();
    BsonDocument tempDoc = RawBsonDocument
      .parse("{\n" + "  \"validField\": \"existingValue\",\n" + "  \"numericField\": 42\n" + "}");
    bsonDocument.putAll(tempDoc);

    String updateExpression =
      "{\n" + "  \"$SET\": {\n" + "    \"validField\": \"newValue\"\n" + "  },\n"
        + "  \"$ADD\": {\n" + "    \"numericField\": \"invalidStringValue\"\n" + "  }\n" + "}";

    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    try {
      UpdateExpressionUtils.updateExpression(expressionDoc, bsonDocument);
      Assert.fail(
        "Expected BsonUpdateInvalidArgumentException for invalid ADD operation in multi-operation expression");
    } catch (BsonUpdateInvalidArgumentException e) {
      // expected
    }
  }

  @Test
  public void testMultipleOperationsWithInvalidPath() {
    // Create a mutable BsonDocument instead of immutable RawBsonDocument
    BsonDocument bsonDocument = new BsonDocument();
    BsonDocument tempDoc = RawBsonDocument.parse("{\n" + "  \"validField\": \"existingValue\",\n"
      + "  \"arrayField\": [\"item1\", \"item2\"]\n" + "}");
    bsonDocument.putAll(tempDoc);

    String updateExpression = "{\n" + "  \"$SET\": {\n" + "    \"validField\": \"newValue\"\n"
      + "  },\n" + "  \"$UNSET\": {\n" + "    \"nonExistent.nested.field\": \"\"\n" + "  }\n" + "}";

    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    try {
      UpdateExpressionUtils.updateExpression(expressionDoc, bsonDocument);
      Assert.fail(
        "Expected BsonUpdateInvalidArgumentException for invalid UNSET path in multi-operation expression");
    } catch (BsonUpdateInvalidArgumentException e) {
      // expected
    }
  }

  @Test
  public void testMultipleOperationsWithDeleteFromSetError() {
    // Create a mutable BsonDocument instead of immutable RawBsonDocument
    BsonDocument bsonDocument = new BsonDocument();
    BsonDocument tempDoc = RawBsonDocument.parse("{\n" + "  \"validField\": \"existingValue\",\n"
      + "  \"stringSet\": {\n" + "    \"$set\": [\"string1\", \"string2\"]\n" + "  }\n" + "}");
    bsonDocument.putAll(tempDoc);

    String updateExpression = "{\n" + "  \"$SET\": {\n" + "    \"validField\": \"newValue\"\n"
      + "  },\n" + "  \"$DELETE_FROM_SET\": {\n" + "    \"stringSet\": {\n"
      + "      \"$set\": [123, 456]\n" + "    }\n" + "  }\n" + "}";

    RawBsonDocument expressionDoc = RawBsonDocument.parse(updateExpression);

    try {
      UpdateExpressionUtils.updateExpression(expressionDoc, bsonDocument);
      Assert.fail(
        "Expected BsonUpdateInvalidArgumentException for DELETE_FROM_SET with incompatible types in multi-operation expression");
    } catch (BsonUpdateInvalidArgumentException e) {
      // expected
    }
  }
}
