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

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.mvel2.MVEL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SQL style condition expression evaluation support.
 */
public class SQLComparisonExpressionUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(SQLComparisonExpressionUtils.class);

  /**
   * All supported operators. Used to parse the input string and identify how many of the
   * operators are in use and accordingly performs string conversions using individual
   * pattern-matcher-replace.
   */
  private static final String ALL_SUPPORTED_OPS =
          "\\b(field_not_exists|field_exists|BETWEEN|IN|AND|OR|NOT)\\b|<=|>=|!=|==|=|<>|<|>";
  private static final Pattern ALL_SUPPORTED_OPS_PATTERN = Pattern.compile(ALL_SUPPORTED_OPS);

  private static final String FIELD_NOT_EXISTS = "field_not_exists\\(([^)]+)\\)";
  private static final String FIELD_EXISTS = "field_exists\\(([^)]+)\\)";
  private static final String EQUALS1 = "\\b([\\w.\\[\\]]+)\\s*=\\s*([#:$]*\\w+)";
  private static final String EQUALS2 = "\\b([\\w.\\[\\]]+)\\s*==\\s*([#:$]*\\w+)";
  private static final String NOT_EQUALS1 = "\\b([\\w.\\[\\]]+)\\s*!=\\s*([#:$]*\\w+)";
  private static final String NOT_EQUALS2 = "\\b([\\w.\\[\\]]+)\\s*<>\\s*([#:$]*\\w+)";
  private static final String LESS_THAN = "\\b([\\w.\\[\\]]+)\\s*<\\s*([#:$]*\\w+)";
  private static final String LESS_THAN_OR_EQUALS = "\\b([\\w.\\[\\]]+)\\s*<=\\s*([#:$]*\\w+)";
  private static final String GREATER_THAN = "\\b([\\w.\\[\\]]+)\\s*>\\s*([#:$]*\\w+)";
  private static final String GREATER_THAN_OR_EQUALS = "\\b([\\w.\\[\\]]+)\\s*>=\\s*([#:$]*\\w+)";
  private static final String BETWEEN =
          "\\b([\\w.\\[\\]]+)\\s+BETWEEN\\s+([#:$]*\\w+)\\s+AND\\s+([#:$]*\\w+)";
  private static final String IN = "\\b([\\w.\\[\\]]+)\\s+IN\\s+\\(([^)]+)\\)";
  private static final String AND = "\\bAND\\b";
  private static final String OR = "\\bOR\\b";
  private static final String NOT = "\\bNOT\\b\\s*";

  private static final Pattern FIELD_NOT_EXISTS_PATTERN = Pattern.compile(FIELD_NOT_EXISTS);
  private static final Pattern FIELD_EXISTS_PATTERN = Pattern.compile(FIELD_EXISTS);
  private static final Pattern EQUALS1_PATTERN = Pattern.compile(EQUALS1);
  private static final Pattern EQUALS2_PATTERN = Pattern.compile(EQUALS2);
  private static final Pattern NOT_EQUALS1_PATTERN = Pattern.compile(NOT_EQUALS1);
  private static final Pattern NOT_EQUALS2_PATTERN = Pattern.compile(NOT_EQUALS2);
  private static final Pattern LESS_THAN_PATTERN = Pattern.compile(LESS_THAN);
  private static final Pattern LESS_THAN_OR_EQUALS_PATTERN = Pattern.compile(LESS_THAN_OR_EQUALS);
  private static final Pattern GREATER_THAN_PATTERN = Pattern.compile(GREATER_THAN);
  private static final Pattern GREATER_THAN_OR_EQUALS_PATTERN =
          Pattern.compile(GREATER_THAN_OR_EQUALS);
  private static final Pattern BETWEEN_PATTERN = Pattern.compile(BETWEEN);
  private static final Pattern IN_PATTERN = Pattern.compile(IN);
  private static final Pattern AND_PATTERN = Pattern.compile(AND);
  private static final Pattern OR_PATTERN = Pattern.compile(OR);
  private static final Pattern NOT_PATTERN = Pattern.compile(NOT);

  private static final String FUNC_FIELD_NOT_EXISTS = "!exists('$1')";
  private static final String FUNC_FIELD_EXISTS = "exists('$1')";
  private static final String FUNC_EQUALS = "isEquals('$1', '$2')";
  private static final String FUNC_NOT_EQUALS = "!isEquals('$1', '$2')";
  private static final String FUNC_LESS_THAN = "lessThan('$1', '$2')";
  private static final String FUNC_LESS_THAN_OR_EQUALS = "lessThanOrEquals('$1', '$2')";
  private static final String FUNC_GREATER_THAN = "greaterThan('$1', '$2')";
  private static final String FUNC_GREATER_THAN_OR_EQUALS = "greaterThanOrEquals('$1', '$2')";
  private static final String FUNC_BETWEEN = "between('$1', '$2', '$3')";
  private static final String FUNC_IN = "in('$1', '$2')";
  private static final String OP_AND = "&&";
  private static final String OP_OR = "||";
  private static final String OP_NOT = "!";

  private final RawBsonDocument rawBsonDocument;
  private final BsonDocument comparisonValuesDocument;

  public SQLComparisonExpressionUtils(RawBsonDocument rawBsonDocument,
      BsonDocument comparisonValuesDocument) {
    this.rawBsonDocument = rawBsonDocument;
    this.comparisonValuesDocument = comparisonValuesDocument;
  }

  public boolean evaluateConditionExpression(final String conditionExpression) {
    String expression = convertExpression(conditionExpression);
    LOGGER.trace("Evaluating Expression: {}", expression);
    try {
      Object result = MVEL.eval(expression, this);
      return result instanceof Boolean && (Boolean) result;
    } catch (Exception e) {
      LOGGER.error("Error while evaluating expression: {}", expression, e);
      throw new RuntimeException("Expression could not be evaluated: " + expression);
    }
  }

  /**
   * Converts the input string expression into Java executable statement.
   *
   * @param expression Input string expression.
   * @return Executable string conversion statement.
   */
  public String convertExpression(String expression) {
    Set<String> patternsMatched = new HashSet<>();
    Matcher matcher = ALL_SUPPORTED_OPS_PATTERN.matcher(expression);
    while (matcher.find()) {
      patternsMatched.add(matcher.group());
    }

    if (patternsMatched.contains("field_not_exists")) {
      expression = FIELD_NOT_EXISTS_PATTERN.matcher(expression).replaceAll(FUNC_FIELD_NOT_EXISTS);
    }
    if (patternsMatched.contains("field_exists")) {
      expression = FIELD_EXISTS_PATTERN.matcher(expression).replaceAll(FUNC_FIELD_EXISTS);
    }
    if (patternsMatched.contains("=")) {
      expression = EQUALS1_PATTERN.matcher(expression).replaceAll(FUNC_EQUALS);
    }
    if (patternsMatched.contains("==")) {
      expression = EQUALS2_PATTERN.matcher(expression).replaceAll(FUNC_EQUALS);
    }
    if (patternsMatched.contains("!=")) {
      expression = NOT_EQUALS1_PATTERN.matcher(expression).replaceAll(FUNC_NOT_EQUALS);
    }
    if (patternsMatched.contains("<>")) {
      expression = NOT_EQUALS2_PATTERN.matcher(expression).replaceAll(FUNC_NOT_EQUALS);
    }
    if (patternsMatched.contains("<")) {
      expression = LESS_THAN_PATTERN.matcher(expression).replaceAll(FUNC_LESS_THAN);
    }
    if (patternsMatched.contains("<=")) {
      expression =
              LESS_THAN_OR_EQUALS_PATTERN.matcher(expression).replaceAll(FUNC_LESS_THAN_OR_EQUALS);
    }
    if (patternsMatched.contains(">")) {
      expression = GREATER_THAN_PATTERN.matcher(expression).replaceAll(FUNC_GREATER_THAN);
    }
    if (patternsMatched.contains(">=")) {
      expression = GREATER_THAN_OR_EQUALS_PATTERN.matcher(expression)
              .replaceAll(FUNC_GREATER_THAN_OR_EQUALS);
    }
    if (patternsMatched.contains("BETWEEN")) {
      expression = BETWEEN_PATTERN.matcher(expression).replaceAll(FUNC_BETWEEN);
    }
    if (patternsMatched.contains("IN")) {
      expression = IN_PATTERN.matcher(expression).replaceAll(FUNC_IN);
    }
    if (patternsMatched.contains("AND")) {
      expression = AND_PATTERN.matcher(expression).replaceAll(OP_AND);
    }
    if (patternsMatched.contains("OR")) {
      expression = OR_PATTERN.matcher(expression).replaceAll(OP_OR);
    }
    if (patternsMatched.contains("NOT")) {
      expression = NOT_PATTERN.matcher(expression).replaceAll(OP_NOT);
    }
    return expression;
  }

  /**
   * Returns true if the value of the field is comparable to the value represented by
   * {@code expectedFieldValue} as per the comparison operator represented by {@code compareOp}.
   * The comparison can happen only if the data type of both values match.
   *
   * @param fieldKey The field key for which value is compared against expectedFieldValue.
   * @param expectedFieldValue The literal value to compare against the field value.
   * @param compareOp The comparison operator.
   * @return True if the comparison is successful, False otherwise.
   */
  private boolean compare(final String fieldKey,
      final String expectedFieldValue,
      final CommonComparisonExpressionUtils.CompareOp compareOp) {
    BsonValue topLevelValue = rawBsonDocument.get(fieldKey);
    BsonValue value = topLevelValue != null ?
        topLevelValue :
        CommonComparisonExpressionUtils.getFieldFromDocument(fieldKey, rawBsonDocument);
    if (value != null) {
      BsonValue compareValue = comparisonValuesDocument.get(expectedFieldValue);
      return CommonComparisonExpressionUtils.compareValues(
          value, compareValue, compareOp);
    }
    return false;
  }

  /**
   * Returns true if the given field exists in the document.
   *
   * @param documentField The document field.
   * @return True if the given field exists in the document.
   */
  public boolean exists(final String documentField) {
    BsonValue topLevelValue = rawBsonDocument.get(documentField);
    if (topLevelValue != null) {
      return true;
    }
    return CommonComparisonExpressionUtils.getFieldFromDocument(documentField, rawBsonDocument)
        != null;
  }

  /**
   * Returns true if the value of the field is less than the value represented by {@code
   * expectedFieldValue}. The comparison can happen only if the data type of both values match.
   *
   * @param fieldKey The field key for which value is compared against expectedFieldValue.
   * @param expectedFieldValue The literal value to compare against the field value.
   * @return True if the value of the field is less than expectedFieldValue.
   */
  public boolean lessThan(final String fieldKey, final String expectedFieldValue) {
    return compare(fieldKey, expectedFieldValue,
        CommonComparisonExpressionUtils.CompareOp.LESS);
  }

  /**
   * Returns true if the value of the field is less than or equal to the value represented by
   * {@code expectedFieldValue}. The comparison can happen only if the data type of both values
   * match.
   *
   * @param fieldKey The field key for which value is compared against expectedFieldValue.
   * @param expectedFieldValue The literal value to compare against the field value.
   * @return True if the value of the field is less than or equal to expectedFieldValue.
   */
  public boolean lessThanOrEquals(final String fieldKey, final String expectedFieldValue) {
    return compare(fieldKey, expectedFieldValue,
        CommonComparisonExpressionUtils.CompareOp.LESS_OR_EQUAL);
  }

  /**
   * Returns true if the value of the field is greater than the value represented by {@code
   * expectedFieldValue}. The comparison can happen only if the data type of both values match.
   *
   * @param fieldKey The field key for which value is compared against expectedFieldValue.
   * @param expectedFieldValue The literal value to compare against the field value.
   * @return True if the value of the field is greater than expectedFieldValue.
   */
  public boolean greaterThan(final String fieldKey, final String expectedFieldValue) {
    return compare(fieldKey, expectedFieldValue,
        CommonComparisonExpressionUtils.CompareOp.GREATER);
  }

  /**
   * Returns true if the value of the field is greater than or equal to the value represented by
   * {@code expectedFieldValue}. The comparison can happen only if the data type of both values
   * match.
   *
   * @param fieldKey The field key for which value is compared against expectedFieldValue.
   * @param expectedFieldValue The literal value to compare against the field value.
   * @return True if the value of the field is greater than or equal to expectedFieldValue.
   */
  public boolean greaterThanOrEquals(final String fieldKey,
      final String expectedFieldValue) {
    return compare(fieldKey, expectedFieldValue,
        CommonComparisonExpressionUtils.CompareOp.GREATER_OR_EQUAL);
  }

  /**
   * Returns true if the value of the field is greater than or equal to the value represented by
   * {@code expectedFieldValue1} and less than or equal to the value represented by
   * {@code expectedFieldValue2}. The comparison can happen only if the data type of both values
   * match.
   *
   * @param fieldKey The field key for which value is compared against two values.
   * @param expectedFieldValue1 The first literal value to compare against the field value.
   * @param expectedFieldValue2 The second literal value to compare against the field value.
   * @return True if the value of the field is greater than or equal to the value represented by
   * expectedFieldValue1 and less than or equal to the value represented by expectedFieldValue2.
   */
  public boolean between(final String fieldKey, final String expectedFieldValue1,
      final String expectedFieldValue2) {
    return greaterThanOrEquals(fieldKey, expectedFieldValue1) && lessThanOrEquals(
        fieldKey, expectedFieldValue2);
  }

  /**
   * Returns true if the value of the field equals to any of the comma separated values
   * represented by {@code expectedInValues}. The equality check is successful only if the value
   * and the data type both match.
   *
   * @param fieldKey The field key for which value is compared against expectedInValues.
   * @param expectedInValues The array of values for comparison, separated by comma.
   * @return True if the value of the field equals to any of the comma separated values
   * represented by expectedInValues. The equality check is successful only if the value
   * and the data type both match.
   */
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

  /**
   * Returns true if the value of the field is equal to the value represented by {@code
   * expectedFieldValue}. The equality check is successful only if the value
   * and the data type both match.
   *
   * @param fieldKey The field key for which value is compared against expectedFieldValue.
   * @param expectedFieldValue The literal value to compare against the field value.
   * @return True if the value of the field is equal to expectedFieldValue.
   */
  public boolean isEquals(final String fieldKey, final String expectedFieldValue) {
    return compare(fieldKey, expectedFieldValue,
        CommonComparisonExpressionUtils.CompareOp.EQUALS);
  }

}
