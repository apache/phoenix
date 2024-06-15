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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.mvel2.MVEL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SQL style condition expression evaluation support.
 */
public class SQLComparisonExpressionUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(SQLComparisonExpressionUtils.class);

  private final RawBsonDocument rawBsonDocument;
  private final BsonDocument comparisonValuesDocument;

  public SQLComparisonExpressionUtils(RawBsonDocument rawBsonDocument,
      BsonDocument comparisonValuesDocument) {
    this.rawBsonDocument = rawBsonDocument;
    this.comparisonValuesDocument = comparisonValuesDocument;
  }

  public boolean isConditionExpressionMatching(final String conditionExpression) {
    String expression = convertExpression(conditionExpression);
    LOGGER.info("Evaluating Expression: {}", expression);
    try {
      Object result = MVEL.eval(expression, this);
      return result instanceof Boolean && (Boolean) result;
    } catch (Exception e) {
      LOGGER.error("Error while evaluating expression: {}", expression, e);
      throw new RuntimeException("Expression could not be evaluated: " + expression);
    }
  }

  public String convertExpression(String expression) {
    expression =
        expression.replaceAll("field_not_exists\\(([^)]+)\\)", "!exists('$1')");
    expression = expression.replaceAll("field_exists\\(([^)]+)\\)", "exists('$1')");
    //    expression = expression.replaceAll("\\b([\\w.\\[\\]]+)\\b\\s*=\\s*(:\\w+)", "isEquals('$1', '$2')");
    expression =
        expression.replaceAll("\\b([\\w.\\[\\]]+)\\s*=\\s*([#:$]*\\w+)", "isEquals('$1', '$2')");
    expression =
        expression.replaceAll("\\b([\\w.\\[\\]]+)\\s*==\\s*([#:$]*\\w+)", "isEquals('$1', '$2')");
    expression =
        expression.replaceAll("\\b([\\w.\\[\\]]+)\\s*!=\\s*([#:$]*\\w+)", "!isEquals('$1', '$2')");
    expression =
        expression.replaceAll("\\b([\\w.\\[\\]]+)\\s*<>\\s*([#:$]*\\w+)", "!isEquals('$1', '$2')");
    expression =
        expression.replaceAll("\\b([\\w.\\[\\]]+)\\s*<\\s*([#:$]*\\w+)", "lessThan('$1', '$2')");
    expression = expression.replaceAll("\\b([\\w.\\[\\]]+)\\s*<=\\s*([#:$]*\\w+)",
        "lessThanOrEquals('$1', '$2')");
    expression =
        expression.replaceAll("\\b([\\w.\\[\\]]+)\\s*>\\s*([#:$]*\\w+)", "greaterThan('$1', '$2')");
    expression = expression.replaceAll("\\b([\\w.\\[\\]]+)\\s*>=\\s*([#:$]*\\w+)",
        "greaterThanOrEquals('$1', '$2')");
    expression = expression.replaceAll(
        "\\b([\\w.\\[\\]]+)\\s+BETWEEN\\s+([#:$]*\\w+)\\s+AND\\s+([#:$]*\\w+)",
        "between('$1', '$2', '$3')");
    expression =
        expression.replaceAll("\\b([\\w.\\[\\]]+)\\s+IN\\s+\\(([^)]+)\\)", "in('$1', '$2')");

    expression = expression.replaceAll("\\bAND\\b", "&&");
    expression = expression.replaceAll("\\bOR\\b", "||");
    expression = expression.replaceAll("\\bNOT\\b\\s*", "!");
    return expression;
  }

  private static String convertInExpression(String expression) {
    String regex = "\\b([\\w.\\[\\]]+)\\s+IN\\s+\\(([^)]+)\\)";

    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(expression);

    StringBuffer result = new StringBuffer();
    while (matcher.find()) {
      String field = matcher.group(1);
      String values = matcher.group(2);

      String[] tokens = values.split("\\s*,\\s*");
      StringBuilder sb = new StringBuilder();
      for (String token : tokens) {
        if (sb.length() > 0) {
          sb.append(", ");
        }
        sb.append("'").append(token.trim()).append("'");
      }
      String replacement = "in('" + field + "', " + sb + ")";
      matcher.appendReplacement(result, replacement);
    }
    matcher.appendTail(result);

    return result.toString();
  }

  public boolean exists(final String documentField) {
    BsonValue topLevelValue = rawBsonDocument.get(documentField);
    if (topLevelValue != null) {
      return true;
    }
    return CommonComparisonExpressionUtils.getFieldFromDocument(documentField, rawBsonDocument)
        != null;
  }

  public boolean lessThan(final String fieldKey, final String expectedFieldValue) {
    BsonValue topLevelValue = rawBsonDocument.get(fieldKey);
    BsonValue value = topLevelValue != null ? topLevelValue
        : CommonComparisonExpressionUtils.getFieldFromDocument(fieldKey, rawBsonDocument);
    if (value != null) {
      BsonValue compareValue = comparisonValuesDocument.get(expectedFieldValue);
      return CommonComparisonExpressionUtils.lessThan(value, compareValue);
    }
    return false;
  }

  public boolean lessThanOrEquals(final String fieldKey, final String expectedFieldValue) {
    BsonValue topLevelValue = rawBsonDocument.get(fieldKey);
    BsonValue value = topLevelValue != null ? topLevelValue
        : CommonComparisonExpressionUtils.getFieldFromDocument(fieldKey, rawBsonDocument);
    if (value != null) {
      BsonValue compareValue = comparisonValuesDocument.get(expectedFieldValue);
      return CommonComparisonExpressionUtils.lessThanOrEquals(value, compareValue);
    }
    return false;
  }

  public boolean greaterThan(final String fieldKey, final String expectedFieldValue) {
    BsonValue topLevelValue = rawBsonDocument.get(fieldKey);
    BsonValue value = topLevelValue != null ? topLevelValue
        : CommonComparisonExpressionUtils.getFieldFromDocument(fieldKey, rawBsonDocument);
    if (value != null) {
      BsonValue compareValue = comparisonValuesDocument.get(expectedFieldValue);
      return CommonComparisonExpressionUtils.greaterThan(value, compareValue);
    }
    return false;
  }

  public boolean greaterThanOrEquals(final String fieldKey,
      final String expectedFieldValue) {
    BsonValue topLevelValue = rawBsonDocument.get(fieldKey);
    BsonValue value = topLevelValue != null ? topLevelValue
        : CommonComparisonExpressionUtils.getFieldFromDocument(fieldKey, rawBsonDocument);
    if (value != null) {
      BsonValue compareValue = comparisonValuesDocument.get(expectedFieldValue);
      return CommonComparisonExpressionUtils.greaterThanOrEquals(value, compareValue);
    }
    return false;
  }

  public boolean between(final String fieldKey, final String expectedFieldValue1,
      final String expectedFieldValue2) {
    return greaterThanOrEquals(fieldKey, expectedFieldValue1) && lessThanOrEquals(
        fieldKey, expectedFieldValue2);
  }

  public boolean in(final String fieldKey, final String expectedInValues) {
    String[] expectedInVals = expectedInValues.split("\\s*,\\s*");
    BsonValue topLevelValue = rawBsonDocument.get(fieldKey);
    BsonValue value = topLevelValue != null ?
        topLevelValue :
        CommonComparisonExpressionUtils.getFieldFromDocument(fieldKey, rawBsonDocument);
    if (value != null) {
      for (String expectedInVal : expectedInVals) {
        if (isEquals(fieldKey, expectedInVal)) {
          return true;
        }
      }
    }
    return false;
  }

  public boolean isEquals(final String fieldKey, final String expectedFieldValue) {
    BsonValue topLevelValue = rawBsonDocument.get(fieldKey);
    BsonValue value = topLevelValue != null ? topLevelValue
        : CommonComparisonExpressionUtils.getFieldFromDocument(fieldKey, rawBsonDocument);
    if (value != null) {
      BsonValue compareValue = comparisonValuesDocument.get(expectedFieldValue);
      return value.equals(compareValue);
    }
    return false;
  }

}
