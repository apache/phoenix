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

import org.apache.phoenix.parse.AndParseNode;
import org.apache.phoenix.parse.BetweenParseNode;
import org.apache.phoenix.parse.DocumentFieldExistsParseNode;
import org.apache.phoenix.parse.BsonExpressionParser;
import org.apache.phoenix.parse.EqualParseNode;
import org.apache.phoenix.parse.GreaterThanOrEqualParseNode;
import org.apache.phoenix.parse.GreaterThanParseNode;
import org.apache.phoenix.parse.InListParseNode;
import org.apache.phoenix.parse.LessThanOrEqualParseNode;
import org.apache.phoenix.parse.LessThanParseNode;
import org.apache.phoenix.parse.LiteralParseNode;
import org.apache.phoenix.parse.NotEqualParseNode;
import org.apache.phoenix.parse.NotParseNode;
import org.apache.phoenix.parse.OrParseNode;
import org.apache.phoenix.parse.ParseNode;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

/**
 * SQL style condition expression evaluation support.
 */
public final class SQLComparisonExpressionUtils {

  private SQLComparisonExpressionUtils() {
    // empty
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(SQLComparisonExpressionUtils.class);

  /**
   * Evaluate the given condition expression on the BSON Document.
   *
   * @param conditionExpression The condition expression consisting of operands, operators.
   * @param rawBsonDocument The BSON Document on which the condition expression is evaluated.
   * @param comparisonValuesDocument The BSON Document consisting of place-holder key-value pairs.
   * @return True if the evaluation is successful, False otherwise.
   */
  public static boolean evaluateConditionExpression(final String conditionExpression,
                                                    final RawBsonDocument rawBsonDocument,
                                                    final BsonDocument comparisonValuesDocument) {
    if (rawBsonDocument == null || conditionExpression == null) {
      LOGGER.warn(
              "Document and/or Condition Expression document are empty. Document: {}, "
                      + "conditionExpression: {}", rawBsonDocument, conditionExpression);
      return false;
    }
    return evaluateExpression(conditionExpression, rawBsonDocument, comparisonValuesDocument);
  }

  /**
   * Generates ParseNode based tree and performs the condition expression evaluation on it.
   *
   * @param conditionExpression The condition expression consisting of operands, operators.
   * @param rawBsonDocument The BSON Document on which the condition expression is evaluated.
   * @param comparisonValuesDocument The BSON Document consisting of place-holder key-value pairs.
   * @return True if the evaluation is successful, False otherwise.
   */
  private static boolean evaluateExpression(final String conditionExpression,
                                            final RawBsonDocument rawBsonDocument,
                                            final BsonDocument comparisonValuesDocument) {
    BsonExpressionParser bsonExpressionParser = new BsonExpressionParser(conditionExpression);
    ParseNode parseNode;
    try {
      parseNode = bsonExpressionParser.parseExpression();
    } catch (SQLException e) {
      LOGGER.error("Expression {} could not be evaluated.", conditionExpression, e);
      throw new RuntimeException("Expression could not be evaluated: " + conditionExpression, e);
    }
    return evaluateExpression(parseNode, rawBsonDocument, comparisonValuesDocument);
  }

  /**
   * Evaluate the parseNode directly.
   *
   * @param parseNode The root ParseNode of the parse tree.
   * @param rawBsonDocument BSON Document value of the Cell.
   * @param comparisonValuesDocument BSON Document with place-holder values.
   * @return True if the evaluation is successful, False otherwise.
   */
  private static boolean evaluateExpression(final ParseNode parseNode,
                                            final RawBsonDocument rawBsonDocument,
                                            final BsonDocument comparisonValuesDocument) {
    // In Phoenix, usually every ParseNode has corresponding Expression class. The expression
    // is evaluated on the Tuple. However, the expression requires data type of PDataType instance.
    // This case is different: we need to evaluate the parse node on the document, not on Tuple.
    // Therefore, for the purpose of evaluating the BSON Condition Expression, we cannot rely on
    // existing Expression based framework.
    // Here, we directly perform the evaluation on the parse nodes. AND, OR, NOT logical operators
    // need to evaluate the parseNodes in loop/recursion similar to how AndExpression, OrExpression,
    // NotExpression logical expressions evaluate.
    if (parseNode instanceof DocumentFieldExistsParseNode) {
      final DocumentFieldExistsParseNode documentFieldExistsParseNode =
              (DocumentFieldExistsParseNode) parseNode;
      final LiteralParseNode fieldKey =
              (LiteralParseNode) documentFieldExistsParseNode.getChildren().get(0);
      final String fieldName = (String) fieldKey.getValue();
      return documentFieldExistsParseNode.isExists() == exists(fieldName, rawBsonDocument);
    } else if (parseNode instanceof EqualParseNode) {
      final EqualParseNode equalParseNode = (EqualParseNode) parseNode;
      final LiteralParseNode lhs = (LiteralParseNode) equalParseNode.getLHS();
      final LiteralParseNode rhs = (LiteralParseNode) equalParseNode.getRHS();
      final String fieldKey = (String) lhs.getValue();
      final String expectedFieldValue = (String) rhs.getValue();
      return isEquals(fieldKey, expectedFieldValue, rawBsonDocument, comparisonValuesDocument);
    } else if (parseNode instanceof NotEqualParseNode) {
      final NotEqualParseNode notEqualParseNode = (NotEqualParseNode) parseNode;
      final LiteralParseNode lhs = (LiteralParseNode) notEqualParseNode.getLHS();
      final LiteralParseNode rhs = (LiteralParseNode) notEqualParseNode.getRHS();
      final String fieldKey = (String) lhs.getValue();
      final String expectedFieldValue = (String) rhs.getValue();
      return !isEquals(fieldKey, expectedFieldValue, rawBsonDocument, comparisonValuesDocument);
    } else if (parseNode instanceof LessThanParseNode) {
      final LessThanParseNode lessThanParseNode = (LessThanParseNode) parseNode;
      final LiteralParseNode lhs = (LiteralParseNode) lessThanParseNode.getLHS();
      final LiteralParseNode rhs = (LiteralParseNode) lessThanParseNode.getRHS();
      final String fieldKey = (String) lhs.getValue();
      final String expectedFieldValue = (String) rhs.getValue();
      return lessThan(fieldKey, expectedFieldValue, rawBsonDocument, comparisonValuesDocument);
    } else if (parseNode instanceof LessThanOrEqualParseNode) {
      final LessThanOrEqualParseNode lessThanOrEqualParseNode =
              (LessThanOrEqualParseNode) parseNode;
      final LiteralParseNode lhs = (LiteralParseNode) lessThanOrEqualParseNode.getLHS();
      final LiteralParseNode rhs = (LiteralParseNode) lessThanOrEqualParseNode.getRHS();
      final String fieldKey = (String) lhs.getValue();
      final String expectedFieldValue = (String) rhs.getValue();
      return lessThanOrEquals(fieldKey, expectedFieldValue, rawBsonDocument,
              comparisonValuesDocument);
    } else if (parseNode instanceof GreaterThanParseNode) {
      final GreaterThanParseNode greaterThanParseNode =
              (GreaterThanParseNode) parseNode;
      final LiteralParseNode lhs = (LiteralParseNode) greaterThanParseNode.getLHS();
      final LiteralParseNode rhs = (LiteralParseNode) greaterThanParseNode.getRHS();
      final String fieldKey = (String) lhs.getValue();
      final String expectedFieldValue = (String) rhs.getValue();
      return greaterThan(fieldKey, expectedFieldValue, rawBsonDocument, comparisonValuesDocument);
    } else if (parseNode instanceof GreaterThanOrEqualParseNode) {
      final GreaterThanOrEqualParseNode greaterThanOrEqualParseNode =
              (GreaterThanOrEqualParseNode) parseNode;
      final LiteralParseNode lhs = (LiteralParseNode) greaterThanOrEqualParseNode.getLHS();
      final LiteralParseNode rhs = (LiteralParseNode) greaterThanOrEqualParseNode.getRHS();
      final String fieldKey = (String) lhs.getValue();
      final String expectedFieldValue = (String) rhs.getValue();
      return greaterThanOrEquals(fieldKey, expectedFieldValue, rawBsonDocument,
              comparisonValuesDocument);
    } else if (parseNode instanceof BetweenParseNode) {
      final BetweenParseNode betweenParseNode = (BetweenParseNode) parseNode;
      final LiteralParseNode fieldKey =
              (LiteralParseNode) betweenParseNode.getChildren().get(0);
      final LiteralParseNode lhs = (LiteralParseNode) betweenParseNode.getChildren().get(1);
      final LiteralParseNode rhs = (LiteralParseNode) betweenParseNode.getChildren().get(2);
      final String fieldName = (String) fieldKey.getValue();
      final String expectedFieldValue1 = (String) lhs.getValue();
      final String expectedFieldValue2 = (String) rhs.getValue();
      return betweenParseNode.isNegate() !=
              between(fieldName, expectedFieldValue1, expectedFieldValue2, rawBsonDocument,
                      comparisonValuesDocument);
    } else if (parseNode instanceof InListParseNode) {
      final InListParseNode inListParseNode = (InListParseNode) parseNode;
      final List<ParseNode> childrenNodes = inListParseNode.getChildren();
      final LiteralParseNode fieldKey = (LiteralParseNode) childrenNodes.get(0);
      final String fieldName = (String) fieldKey.getValue();
      final String[] inList = new String[childrenNodes.size() - 1];
      for (int i = 1; i < childrenNodes.size(); i++) {
        LiteralParseNode literalParseNode = (LiteralParseNode) childrenNodes.get(i);
        inList[i - 1] = ((String) literalParseNode.getValue());
      }
      return inListParseNode.isNegate() != in(rawBsonDocument, comparisonValuesDocument, fieldName
              , inList);
    } else if (parseNode instanceof AndParseNode) {
      AndParseNode andParseNode = (AndParseNode) parseNode;
      List<ParseNode> children = andParseNode.getChildren();
      for (ParseNode node : children) {
        if (!evaluateExpression(node, rawBsonDocument, comparisonValuesDocument)) {
          return false;
        }
      }
      return true;
    } else if (parseNode instanceof OrParseNode) {
      OrParseNode orParseNode = (OrParseNode) parseNode;
      List<ParseNode> children = orParseNode.getChildren();
      for (ParseNode node : children) {
        if (evaluateExpression(node, rawBsonDocument, comparisonValuesDocument)) {
          return true;
        }
      }
      return false;
    } else if (parseNode instanceof NotParseNode) {
      NotParseNode notParseNode = (NotParseNode) parseNode;
      return !evaluateExpression(notParseNode.getChildren().get(0), rawBsonDocument,
              comparisonValuesDocument);
    } else {
      throw new IllegalArgumentException("ParseNode " + parseNode + " is not recognized for " +
              "document comparison");
    }
  }

  /**
   * Returns true if the value of the field is comparable to the value represented by
   * {@code expectedFieldValue} as per the comparison operator represented by {@code compareOp}.
   * The comparison can happen only if the data type of both values match.
   *
   * @param fieldKey The field key for which value is compared against expectedFieldValue.
   * @param expectedFieldValue The literal value to compare against the field value.
   * @param compareOp The comparison operator.
   * @param rawBsonDocument Bson Document representing the cell value on which the comparison is
   * to be performed.
   * @param comparisonValuesDocument Bson Document with values placeholder.
   * @return True if the comparison is successful, False otherwise.
   */
  private static boolean compare(final String fieldKey,
                                 final String expectedFieldValue,
                                 final CommonComparisonExpressionUtils.CompareOp compareOp,
                                 final RawBsonDocument rawBsonDocument,
                                 final BsonDocument comparisonValuesDocument) {
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
   * @param rawBsonDocument Bson Document representing the cell value on which the comparison is
   * to be performed.
   * @return True if the given field exists in the document.
   */
  private static boolean exists(final String documentField, final RawBsonDocument rawBsonDocument) {
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
   * @param rawBsonDocument Bson Document representing the cell value on which the comparison is
   * to be performed.
   * @param comparisonValuesDocument Bson Document with values placeholder.
   * @return True if the value of the field is less than expectedFieldValue.
   */
  private static boolean lessThan(final String fieldKey,
                                  final String expectedFieldValue,
                                  final RawBsonDocument rawBsonDocument,
                                  final BsonDocument comparisonValuesDocument) {
    return compare(fieldKey, expectedFieldValue,
            CommonComparisonExpressionUtils.CompareOp.LESS, rawBsonDocument,
            comparisonValuesDocument);
  }

  /**
   * Returns true if the value of the field is less than or equal to the value represented by
   * {@code expectedFieldValue}. The comparison can happen only if the data type of both values
   * match.
   *
   * @param fieldKey The field key for which value is compared against expectedFieldValue.
   * @param expectedFieldValue The literal value to compare against the field value.
   * @param rawBsonDocument Bson Document representing the cell value on which the comparison is
   * to be performed.
   * @param comparisonValuesDocument Bson Document with values placeholder.
   * @return True if the value of the field is less than or equal to expectedFieldValue.
   */
  private static boolean lessThanOrEquals(final String fieldKey,
                                          final String expectedFieldValue,
                                          final RawBsonDocument rawBsonDocument,
                                          final BsonDocument comparisonValuesDocument) {
    return compare(fieldKey, expectedFieldValue,
            CommonComparisonExpressionUtils.CompareOp.LESS_OR_EQUAL,
            rawBsonDocument, comparisonValuesDocument);
  }

  /**
   * Returns true if the value of the field is greater than the value represented by {@code
   * expectedFieldValue}. The comparison can happen only if the data type of both values match.
   *
   * @param fieldKey The field key for which value is compared against expectedFieldValue.
   * @param expectedFieldValue The literal value to compare against the field value.
   * @param rawBsonDocument Bson Document representing the cell value on which the comparison is
   * to be performed.
   * @param comparisonValuesDocument Bson Document with values placeholder.
   * @return True if the value of the field is greater than expectedFieldValue.
   */
  private static boolean greaterThan(final String fieldKey, final String expectedFieldValue,
                                     final RawBsonDocument rawBsonDocument,
                                     final BsonDocument comparisonValuesDocument) {
    return compare(fieldKey, expectedFieldValue,
            CommonComparisonExpressionUtils.CompareOp.GREATER, rawBsonDocument,
            comparisonValuesDocument);
  }

  /**
   * Returns true if the value of the field is greater than or equal to the value represented by
   * {@code expectedFieldValue}. The comparison can happen only if the data type of both values
   * match.
   *
   * @param fieldKey The field key for which value is compared against expectedFieldValue.
   * @param expectedFieldValue The literal value to compare against the field value.
   * @param rawBsonDocument Bson Document representing the cell value on which the comparison is
   * to be performed.
   * @param comparisonValuesDocument Bson Document with values placeholder.
   * @return True if the value of the field is greater than or equal to expectedFieldValue.
   */
  private static boolean greaterThanOrEquals(final String fieldKey,
                                             final String expectedFieldValue,
                                             final RawBsonDocument rawBsonDocument,
                                             final BsonDocument comparisonValuesDocument) {
    return compare(fieldKey, expectedFieldValue,
            CommonComparisonExpressionUtils.CompareOp.GREATER_OR_EQUAL, rawBsonDocument,
            comparisonValuesDocument);
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
   * @param rawBsonDocument Bson Document representing the cell value on which the comparison is
   * to be performed.
   * @param comparisonValuesDocument Bson Document with values placeholder.
   * @return True if the value of the field is greater than or equal to the value represented by
   * expectedFieldValue1 and less than or equal to the value represented by expectedFieldValue2.
   */
  private static boolean between(final String fieldKey, final String expectedFieldValue1,
                                 final String expectedFieldValue2,
                                 final RawBsonDocument rawBsonDocument,
                                 final BsonDocument comparisonValuesDocument) {
    return greaterThanOrEquals(fieldKey, expectedFieldValue1, rawBsonDocument,
            comparisonValuesDocument) && lessThanOrEquals(
            fieldKey, expectedFieldValue2, rawBsonDocument, comparisonValuesDocument);
  }

  /**
   * Returns true if the value of the field equals to any of the comma separated values
   * represented by {@code expectedInValues}. The equality check is successful only if the value
   * and the data type both match.
   *
   * @param rawBsonDocument Bson Document representing the cell value on which the comparison is
   * to be performed.
   * @param comparisonValuesDocument Bson Document with values placeholder.
   * @param fieldKey The field key for which value is compared against expectedInValues.
   * @param expectedInValues The array of values for comparison, separated by comma.
   * @return True if the value of the field equals to any of the comma separated values
   * represented by expectedInValues. The equality check is successful only if the value
   * and the data type both match.
   */
  private static boolean in(final RawBsonDocument rawBsonDocument,
                            final BsonDocument comparisonValuesDocument,
                            final String fieldKey,
                            final String... expectedInValues) {
    BsonValue topLevelValue = rawBsonDocument.get(fieldKey);
    BsonValue value = topLevelValue != null ?
        topLevelValue :
        CommonComparisonExpressionUtils.getFieldFromDocument(fieldKey, rawBsonDocument);
    if (value != null) {
      for (String expectedInVal : expectedInValues) {
        if (isEquals(fieldKey, expectedInVal, rawBsonDocument, comparisonValuesDocument)) {
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
   * @param rawBsonDocument Bson Document representing the cell value on which the comparison is
   * to be performed.
   * @param comparisonValuesDocument Bson Document with values placeholder.
   * @return True if the value of the field is equal to expectedFieldValue.
   */
  private static boolean isEquals(final String fieldKey, final String expectedFieldValue,
                                  final RawBsonDocument rawBsonDocument,
                                  final BsonDocument comparisonValuesDocument) {
    return compare(fieldKey, expectedFieldValue,
            CommonComparisonExpressionUtils.CompareOp.EQUALS, rawBsonDocument,
            comparisonValuesDocument);
  }

}
