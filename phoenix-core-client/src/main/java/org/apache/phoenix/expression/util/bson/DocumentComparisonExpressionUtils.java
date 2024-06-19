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

package org.apache.phoenix.expression.util.bson;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;

/**
 * Document style condition expression evaluation support.
 */
public class DocumentComparisonExpressionUtils {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(DocumentComparisonExpressionUtils.class);

  private static final String EXISTS_OP = "$exists";
  private static final String EQUALS_OP = "$eq";
  private static final String NOT_EQUALS_OP = "$ne";
  private static final String LESS_THAN_OP = "$lt";
  private static final String LESS_THAN_OR_EQUALS_OP = "$lte";
  private static final String GREATER_THAN_OP = "$gt";
  private static final String GREATER_THAN_OR_EQUALS_OP = "$gte";

  public static boolean isConditionExpressionMatching(final BsonDocument document,
      final BsonDocument conditionExpression) {
    if (document == null || conditionExpression == null) {
      LOGGER.warn(
          "Document and/or Condition Expression document are empty. Document: {}, "
              + "conditionExpression: {}", document, conditionExpression);
      return false;
    }
    return evaluateExpression(document, conditionExpression);
  }

  private static boolean evaluateExpression(final BsonDocument document,
      final BsonDocument conditionExpression) {
    final String firstFieldKey = conditionExpression.getFirstKey();
    Preconditions.checkArgument(conditionExpression.size() == 1,
        "Expected num of document entries is 1");
    if (!firstFieldKey.startsWith("$")) {
      BsonValue bsonValue = conditionExpression.get(firstFieldKey);
      Preconditions.checkArgument(bsonValue instanceof BsonDocument,
          "Expected type for Bson value is Document for field based condition operation");
      BsonDocument bsonDocument = (BsonDocument) bsonValue;
      if (bsonDocument.containsKey(EXISTS_OP)) {
        return isExists(document, conditionExpression);
      } else if (bsonDocument.containsKey(EQUALS_OP)) {
        return equals(document, conditionExpression);
      } else if (bsonDocument.containsKey(NOT_EQUALS_OP)) {
        return notEquals(document, conditionExpression);
      } else if (bsonDocument.containsKey(LESS_THAN_OP)) {
        return lessThan(document, conditionExpression);
      } else if (bsonDocument.containsKey(LESS_THAN_OR_EQUALS_OP)) {
        return lessThanOrEquals(document, conditionExpression);
      } else if (bsonDocument.containsKey(GREATER_THAN_OP)) {
        return greaterThan(document, conditionExpression);
      } else if (bsonDocument.containsKey(GREATER_THAN_OR_EQUALS_OP)) {
        return greaterThanOrEquals(document, conditionExpression);
      } else {
        throw new IllegalArgumentException("Operator " + firstFieldKey + " is not supported");
      }
    } else {
      switch (firstFieldKey) {
        case "$or": {
          BsonValue bsonValue = conditionExpression.get(firstFieldKey);
          Preconditions.checkArgument(bsonValue instanceof BsonArray,
              "Expected type for Bson value is Array for $or operator");
          BsonArray bsonArray = (BsonArray) bsonValue;
          List<BsonValue> bsonValues = bsonArray.getValues();
          for (BsonValue value : bsonValues) {
            if (evaluateExpression(document, (BsonDocument) value)) {
              return true;
            }
          }
          return false;
        }
        case "$and": {
          BsonValue bsonValue = conditionExpression.get(firstFieldKey);
          Preconditions.checkArgument(bsonValue instanceof BsonArray,
              "Expected type for Bson value is Array for $and operator");
          BsonArray bsonArray = (BsonArray) bsonValue;
          List<BsonValue> bsonValues = bsonArray.getValues();
          for (BsonValue value : bsonValues) {
            if (!evaluateExpression(document, (BsonDocument) value)) {
              return false;
            }
          }
          return true;
        }
        default: {
          throw new IllegalArgumentException(firstFieldKey + " is not a known operator");
        }
      }
    }
  }

  private static boolean isExists(final BsonDocument document,
      final BsonDocument conditionExpression) {
    Set<Map.Entry<String, BsonValue>> entrySet = conditionExpression.entrySet();
    Preconditions.checkArgument(entrySet.size() == 1, "Expected entry for the exists operation"
        + " is 1");
    for (Map.Entry<String, BsonValue> bsonValueEntry : entrySet) {
      String fieldKey = bsonValueEntry.getKey();
      BsonValue bsonValue = bsonValueEntry.getValue();
      Preconditions.checkArgument(bsonValue instanceof BsonDocument,
          "Expected type for Bson value is Document for exists operation");
      BsonDocument bsonDocument = (BsonDocument) bsonValue;
      BsonValue existsValue = bsonDocument.get(EXISTS_OP);
      Preconditions.checkArgument(existsValue instanceof BsonBoolean,
          "Expected type for $exists value is boolean");
      BsonBoolean existsValBoolean = (BsonBoolean) existsValue;
      if (existsValBoolean.getValue()) {
        return exists(fieldKey, document);
      } else {
        return !exists(fieldKey, document);
      }
    }
    return false;
  }

  /**
   * Returns true if the field provided in the condition expression has value greater than
   * or equals to the value provided in the condition expression.
   * Condition Expression format:
   * {
   *   <field>: {
   *     "$gte": <value>
   *   }
   * }
   *
   * @param document The document used for comparison.
   * @param conditionExpression Condition Expression Document.
   * @return True if the field provided in the condition expression has value greater than
   * or equals to the value provided in the condition expression.
   */
  private static boolean greaterThanOrEquals(final BsonDocument document,
      final BsonDocument conditionExpression) {
    Set<Map.Entry<String, BsonValue>> entrySet = conditionExpression.entrySet();
    Preconditions.checkArgument(entrySet.size() == 1,
        "Expected entry for the greaterThanOrEquals operation is 1");
    for (Map.Entry<String, BsonValue> bsonValueEntry : entrySet) {
      String fieldKey = bsonValueEntry.getKey();
      BsonValue bsonValue = bsonValueEntry.getValue();
      Preconditions.checkArgument(bsonValue instanceof BsonDocument,
          "Expected type for Bson value is Document for greaterThanOrEquals operation");
      BsonDocument bsonDocument = (BsonDocument) bsonValue;
      BsonValue compareValue = bsonDocument.get(GREATER_THAN_OR_EQUALS_OP);
      BsonValue topLevelValue = document.get(fieldKey);
      BsonValue actualValue = topLevelValue != null ? topLevelValue
          : CommonComparisonExpressionUtils.getFieldFromDocument(fieldKey, document);
      return actualValue != null && CommonComparisonExpressionUtils.greaterThanOrEquals(actualValue,
          compareValue);
    }
    return false;
  }

  /**
   * Returns true if the field provided in the condition expression has value greater than
   * the value provided in the condition expression.
   * Condition Expression format:
   * {
   *   <field>: {
   *     "$gt": <value>
   *   }
   * }
   *
   * @param document The document used for comparison.
   * @param conditionExpression Condition Expression Document.
   * @return True if the field provided in the condition expression has value greater than
   * the value provided in the condition expression.
   */
  private static boolean greaterThan(final BsonDocument document,
      final BsonDocument conditionExpression) {
    Set<Map.Entry<String, BsonValue>> entrySet = conditionExpression.entrySet();
    Preconditions.checkArgument(entrySet.size() == 1, "Expected entry for the greaterThan operation"
        + " is 1");
    for (Map.Entry<String, BsonValue> bsonValueEntry : entrySet) {
      String fieldKey = bsonValueEntry.getKey();
      BsonValue bsonValue = bsonValueEntry.getValue();
      Preconditions.checkArgument(bsonValue instanceof BsonDocument,
          "Expected type for Bson value is Document for greaterThan operation");
      BsonDocument bsonDocument = (BsonDocument) bsonValue;
      BsonValue compareValue = bsonDocument.get(GREATER_THAN_OP);
      BsonValue topLevelValue = document.get(fieldKey);
      BsonValue actualValue = topLevelValue != null ? topLevelValue
          : CommonComparisonExpressionUtils.getFieldFromDocument(fieldKey, document);
      return actualValue != null && CommonComparisonExpressionUtils.greaterThan(actualValue,
          compareValue);
    }
    return false;
  }

  /**
   * Returns true if the field provided in the condition expression has value less than
   * or equals to the value provided in the condition expression.
   * Condition Expression format:
   * {
   *   <field>: {
   *     "$lte": <value>
   *   }
   * }
   *
   * @param document The document used for comparison.
   * @param conditionExpression Condition Expression Document.
   * @return True if the field provided in the condition expression has value less than
   * or equals to the value provided in the condition expression.
   */
  private static boolean lessThanOrEquals(final BsonDocument document,
      final BsonDocument conditionExpression) {
    Set<Map.Entry<String, BsonValue>> entrySet = conditionExpression.entrySet();
    Preconditions.checkArgument(entrySet.size() == 1,
        "Expected entry for the lessThanOrEquals operation is 1");
    for (Map.Entry<String, BsonValue> bsonValueEntry : entrySet) {
      String fieldKey = bsonValueEntry.getKey();
      BsonValue bsonValue = bsonValueEntry.getValue();
      Preconditions.checkArgument(bsonValue instanceof BsonDocument,
          "Expected type for Bson value is Document for lessThanOrEquals operation");
      BsonDocument bsonDocument = (BsonDocument) bsonValue;
      BsonValue compareValue = bsonDocument.get(LESS_THAN_OR_EQUALS_OP);
      BsonValue topLevelValue = document.get(fieldKey);
      BsonValue actualValue = topLevelValue != null ? topLevelValue
          : CommonComparisonExpressionUtils.getFieldFromDocument(fieldKey, document);
      return actualValue != null && CommonComparisonExpressionUtils.lessThanOrEquals(actualValue,
          compareValue);
    }
    return false;
  }

  /**
   * Returns true if the field provided in the condition expression has value less than
   * the value provided in the condition expression.
   * Condition Expression format:
   * {
   *   <field>: {
   *     "$lt": <value>
   *   }
   * }
   *
   * @param document The document used for comparison.
   * @param conditionExpression Condition Expression Document.
   * @return True if the field provided in the condition expression has value less than
   * the value provided in the condition expression.
   */
  private static boolean lessThan(final BsonDocument document,
      final BsonDocument conditionExpression) {
    Set<Map.Entry<String, BsonValue>> entrySet = conditionExpression.entrySet();
    Preconditions.checkArgument(entrySet.size() == 1, "Expected entry for the lessThan operation"
        + " is 1");
    for (Map.Entry<String, BsonValue> bsonValueEntry : entrySet) {
      String fieldKey = bsonValueEntry.getKey();
      BsonValue bsonValue = bsonValueEntry.getValue();
      Preconditions.checkArgument(bsonValue instanceof BsonDocument,
          "Expected type for Bson value is Document for lessThan operation");
      BsonDocument bsonDocument = (BsonDocument) bsonValue;
      BsonValue compareValue = bsonDocument.get(LESS_THAN_OP);
      BsonValue topLevelValue = document.get(fieldKey);
      BsonValue actualValue = topLevelValue != null ? topLevelValue
          : CommonComparisonExpressionUtils.getFieldFromDocument(fieldKey, document);
      return actualValue != null && CommonComparisonExpressionUtils.lessThan(actualValue,
          compareValue);
    }
    return false;
  }

  /**
   * Returns true if the field provided in the condition expression has value equal to the value
   * provided in the condition expression.
   * Condition Expression format:
   * {
   *   <field>: {
   *     "$eq": <value>
   *   }
   * }
   *
   * @param document The document used for comparison.
   * @param conditionExpression Condition Expression Document.
   * @return if the field provided in the condition expression has value equal to the value
   * provided in the condition expression.
   */
  private static boolean equals(final BsonDocument document,
      final BsonDocument conditionExpression) {
    Set<Map.Entry<String, BsonValue>> entrySet = conditionExpression.entrySet();
    Preconditions.checkArgument(entrySet.size() == 1, "Expected entry for the equals operation"
        + " is 1");
    for (Map.Entry<String, BsonValue> bsonValueEntry : entrySet) {
      String fieldKey = bsonValueEntry.getKey();
      BsonValue bsonValue = bsonValueEntry.getValue();
      Preconditions.checkArgument(bsonValue instanceof BsonDocument,
          "Expected type for Bson value is Document for equals operation");
      BsonDocument bsonDocument = (BsonDocument) bsonValue;
      BsonValue compareValue = bsonDocument.get(EQUALS_OP);
      BsonValue topLevelValue = document.get(fieldKey);
      BsonValue actualValue = topLevelValue != null ? topLevelValue
          : CommonComparisonExpressionUtils.getFieldFromDocument(fieldKey, document);
      return compareValue.equals(actualValue);
    }
    return false;
  }

  /**
   * Returns true if the field provided in the condition expression has value not equal to the
   * value provided in the condition expression.
   * Condition Expression format:
   * {
   *   <field>: {
   *     "$ne": <value>
   *   }
   * }
   *
   * @param document The document used for comparison.
   * @param conditionExpression Condition Expression Document.
   * @return if the field provided in the condition expression has value not equal to the value
   * provided in the condition expression.
   */
  private static boolean notEquals(final BsonDocument document,
      final BsonDocument conditionExpression) {
    Set<Map.Entry<String, BsonValue>> entrySet = conditionExpression.entrySet();
    Preconditions.checkArgument(entrySet.size() == 1, "Expected entry for the notEquals operation"
        + " is 1");
    for (Map.Entry<String, BsonValue> bsonValueEntry : entrySet) {
      String fieldKey = bsonValueEntry.getKey();
      BsonValue bsonValue = bsonValueEntry.getValue();
      Preconditions.checkArgument(bsonValue instanceof BsonDocument,
          "Expected type for Bson value is Document for notEquals operation");
      BsonDocument bsonDocument = (BsonDocument) bsonValue;
      BsonValue compareValue = bsonDocument.get(NOT_EQUALS_OP);
      BsonValue topLevelValue = document.get(fieldKey);
      BsonValue actualValue = topLevelValue != null ? topLevelValue
          : CommonComparisonExpressionUtils.getFieldFromDocument(fieldKey, document);
      return !compareValue.equals(actualValue);
    }
    return false;
  }

  private static boolean exists(final String documentField, final BsonDocument bsonDocument) {
    BsonValue topLevelValue = bsonDocument.get(documentField);
    if (topLevelValue != null) {
      return true;
    }
    return CommonComparisonExpressionUtils.getFieldFromDocument(documentField, bsonDocument)
        != null;
  }

}
