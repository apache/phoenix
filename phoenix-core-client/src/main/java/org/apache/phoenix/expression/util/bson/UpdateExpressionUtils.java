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

import java.math.BigDecimal;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bson.BsonArray;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNumber;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.types.Decimal128;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BSON Update Expression Utility to perform the Document updates. All update expressions
 * provided by this utility supports operations on nested document fields. The field key can
 * represent any top level or nested fields within the document. The caller should use "."
 * notation for accessing nested document elements and "[n]" notation for accessing nested array
 * elements. Top level fields do not require any additional character.
 */
public class UpdateExpressionUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(UpdateExpressionUtils.class);

  /**
   * Update operator enum values. Any new update operator that requires update to deeply nested
   * structures (nested documents or nested arrays), need to have its own type added here.
   */
  private enum UpdateOp {
    SET,
    UNSET,
    ADD,
    DELETE_FROM_SET
  }

  /**
   * Updates the given document based on the update expression.
   * <p/>
   * {
   *    "$SET": { &lt;field1&gt;: &lt;value1>, &lt;field2&gt;: &lt;value2&gt;, .... },
   *    "$UNSET": { &lt;field1&gt;: null, &lt;field2&gt;: null, ... },
   *    "$ADD": { &lt;field1&gt;: &lt;value1&gt;, &lt;field2&gt;: &lt;value2&gt;, .... },
   *    "$DELETE_FROM_SET": { &lt;field1&gt;: &lt;value1&gt;, &lt;field2&gt;: &lt;value2&gt;, .... }
   * }
   * <p/>
   * "$SET": Use the SET action in an update expression to add one or more fields to a BSON
   * Document. If any of these fields already exists, they are overwritten by the new values.
   * To perform multiple SET actions, provide multiple fields key-value entries within the nested
   * document under $SET field key.
   * "$UNSET": Use the UNSET action in an update expression to unset or remove one or more fields
   * from a BSON Document. To perform multiple UNSET actions, provide multiple field key-value
   * entries within the nested document under $UNSET field key.
   * "$ADD": Use the ADD action in an update expression to add a new field and its values to a
   * BSON document. If the field already exists, the behavior of ADD depends on the field's
   * data type:
   * <p/>
   * 1. If the field is a number, and the value you are adding is also a number, the value is
   * mathematically added to the existing field.
   * <p/>
   * 2. If the field is a set, and the value you are adding is also a set, the value is appended
   * to the existing set.
   * <p/>
   * "$DELETE_FROM_SET": Use the DELETE action in an update expression to remove one or more
   * elements from a set. To perform multiple DELETE actions, provide multiple field key-value
   * entries within the nested document under $DELETE_FROM_SET field key.
   * Definition of path and subset in the context of the expression:
   * <p/>
   * 1. The path element is the document path to a field. The field must be a set data type.
   * 2. The subset is one or more elements that you want to delete from the given path. Subset
   * must be of set type.
   * <p/>
   *
   * @param updateExpression Update Expression as a document.
   * @param bsonDocument Document contents to be updated.
   */
  public static void updateExpression(final BsonDocument updateExpression,
      final BsonDocument bsonDocument) {

    LOGGER.info("Update Expression: {} , current bsonDocument: {}", updateExpression, bsonDocument);

    if (updateExpression.containsKey("$SET")) {
      executeSetExpression((BsonDocument) updateExpression.get("$SET"), bsonDocument);
    }

    if (updateExpression.containsKey("$UNSET")) {
      executeRemoveExpression((BsonDocument) updateExpression.get("$UNSET"), bsonDocument);
    }

    if (updateExpression.containsKey("$ADD")) {
      executeAddExpression((BsonDocument) updateExpression.get("$ADD"), bsonDocument);
    }

    if (updateExpression.containsKey("$DELETE_FROM_SET")) {
      executeDeleteExpression((BsonDocument) updateExpression.get("$DELETE_FROM_SET"),
          bsonDocument);
    }
  }

  /**
   * Update the given document by performing DELETE operation. This operation is applicable
   * only on Set data structure. The document is updated by removing the given set of elements from
   * the given set of elements.
   * Let's say if the document field is of string set data type, and the elements are:
   * {"yellow", "green", "red", "blue"}. The elements to be removed from the set are provided
   * as {"blue", "yellow"} with the delete expression. The operation is expected to update the
   * existing field by removing "blue" and "yellow" from the given set and the resultant set is
   * expected to contain: {"green", "red"}.
   *
   * @param deleteExpr Delete Expression Document with key-value pairs. Key represents field in the
   * given document, on which operation is to be performed. Value represents set of elements to be
   * removed from the existing set.
   * @param bsonDocument Document contents to be updated.
   */
  private static void executeDeleteExpression(final BsonDocument deleteExpr,
      final BsonDocument bsonDocument) {
    for (Map.Entry<String, BsonValue> deleteEntry : deleteExpr.entrySet()) {
      String fieldKey = deleteEntry.getKey();
      BsonValue newVal = deleteEntry.getValue();
      BsonValue topLevelValue = bsonDocument.get(fieldKey);
      if (!isBsonSet(newVal)) {
        throw new RuntimeException("Type of new value to be removed should be sets only");
      }
      // If the top level field exists, perform the operation here and return.
      if (topLevelValue != null) {
        BsonValue value = modifyFieldValueByDeleteFromSet(topLevelValue, newVal);
        if (value == null) {
          bsonDocument.remove(fieldKey);
        } else {
          bsonDocument.put(fieldKey, value);
        }
      } else if (!fieldKey.contains(".") && !fieldKey.contains("[")) {
        LOGGER.info("Nothing to be removed as field with key {} does not exist", fieldKey);
      } else {
        // If the top level field does not exist and the field key contains "." or "[]" notations
        // for nested document or nested array, use the self-recursive function to go through
        // the document tree, one level at a time.
        updateNestedField(bsonDocument, 0, fieldKey, newVal, UpdateOp.DELETE_FROM_SET);
      }
    }
  }

  /**
   * Update the existing set by removing the set values that are present in
   * {@code setValuesToDelete}. For this operation to be successful, both {@code currentValue} and
   * {@code setValuesToDelete} must be of same set data type. For instance, both must be either
   * set of string, set of numbers or set of binary values.
   *
   * @param currentValue The value that needs to be updated by performing deletion operation.
   * @param setValuesToDelete The set values that need to be deleted from the currentValue set.
   * @return Updated set after performing the set difference operation.
   */
  private static BsonValue modifyFieldValueByDeleteFromSet(final BsonValue currentValue,
      final BsonValue setValuesToDelete) {
    if (areBsonSetOfSameType(currentValue, setValuesToDelete)) {
      Set<BsonValue> set1 =
          new HashSet<>(((BsonArray) ((BsonDocument) currentValue).get("$set")).getValues());
      Set<BsonValue> set2 =
          new HashSet<>(((BsonArray) ((BsonDocument) setValuesToDelete).get("$set")).getValues());
      set1.removeAll(set2);
      if (set1.isEmpty()) {
        return null;
      }
      BsonDocument bsonDocument = new BsonDocument();
      bsonDocument.put("$set", new BsonArray(new ArrayList<>(set1)));
      return bsonDocument;
    }
    throw new RuntimeException(
        "Data type for current value " + currentValue + " is not matching with new value "
            + setValuesToDelete);
  }

  /**
   * Update the given document by performing ADD operation. This operation is applicable
   * only on either Set data structure or Numerical value represented by Int32, Int64, Double or
   * Decimal. If the field is of type set, the document is updated by adding the given set of
   * elements to the given set of elements. If the field is of type number, the document is updated
   * by adding the numerical value to the given number field value.
   * </p>
   * Let's say if the document field is of numeric data type with value "234.5" and the value
   * to be added is "10", the resultant value is expected to be "244.5". Adding negative value
   * would result in subtract operation. For example, adding "-10" would result in "224.5".
   * </p>
   * On the other hand, if the document field is of string set data type, and the elements are:
   * {"yellow", "green", "red"}. The elements to be added to the set are provided
   * as {"blue", "yellow"} with the add expression. The operation is expected to update the
   * existing field by removing adding unique value "blue" and the resultant set is
   * expected to contain: {"yellow", "green", "red", "blue"}.
   *
   * @param addExpr Add Expression Document
   * @param bsonDocument Document contents to be updated.
   */
  private static void executeAddExpression(final BsonDocument addExpr,
      final BsonDocument bsonDocument) {
    for (Map.Entry<String, BsonValue> addEntry : addExpr.entrySet()) {
      String fieldKey = addEntry.getKey();
      BsonValue newVal = addEntry.getValue();
      BsonValue topLevelValue = bsonDocument.get(fieldKey);
      if (!newVal.isNumber() && !newVal.isDecimal128() && !isBsonSet(newVal)) {
        throw new RuntimeException(
            "Type of new value to be updated should be either number or sets only");
      }
      // If the top level field exists, perform the operation here and return.
      if (topLevelValue != null) {
        bsonDocument.put(fieldKey, modifyFieldValueByAdd(topLevelValue, newVal));
      } else if (!fieldKey.contains(".") && !fieldKey.contains("[")) {
        bsonDocument.put(fieldKey, newVal);
      } else {
        // If the top level field does not exist and the field key contains "." or "[]" notations
        // for nested document or nested array, use the self-recursive function to go through
        // the document tree, one level at a time.
        updateNestedField(bsonDocument, 0, fieldKey, newVal, UpdateOp.ADD);
      }
    }
  }

  /**
   * Update the existing value {@code currentValue} depending on its data type. If the data type of
   * {@code currentValue} is numeric, add numeric value represented by {@code newVal}
   * to it. If the data type of {@code currentValue} is set, add set values represented by
   * {@code newVal} to it. For this operation to be successful, both {@code currentValue} and
   * {@code newVal} must be of same set data type. For instance, both must be either
   * set of string, set of numbers or set of binary values, or both must be of number data type.
   *
   * @param currentValue The value that needs to be updated by performing add operation.
   * @param newVal The numeric or set values that need to be added to the currentValue.
   * @return Updated value after performing the add operation.
   */
  private static BsonValue modifyFieldValueByAdd(final BsonValue currentValue,
      final BsonValue newVal) {
    if ((currentValue.isNumber() || currentValue.isDecimal128()) && (newVal.isNumber()
        || newVal.isDecimal128())) {
      Number num1 = getNumberFromBsonNumber((BsonNumber) currentValue);
      Number num2 = getNumberFromBsonNumber((BsonNumber) newVal);
      Number newNum = addNum(num1, num2);
      return getBsonNumberFromNumber(newNum);
    } else if (areBsonSetOfSameType(currentValue, newVal)) {
      Set<BsonValue> set1 =
          new HashSet<>(((BsonArray) ((BsonDocument) currentValue).get("$set")).getValues());
      Set<BsonValue> set2 =
          new HashSet<>(((BsonArray) ((BsonDocument) newVal).get("$set")).getValues());
      set1.addAll(set2);
      BsonDocument bsonDocument = new BsonDocument();
      bsonDocument.put("$set", new BsonArray(new ArrayList<>(set1)));
      return bsonDocument;
    }
    throw new RuntimeException(
        "Data type for current value " + currentValue + " is not matching with new value "
            + newVal);
  }

  /**
   * Update the given document by performing UNSET operation on one or more fields. This operation
   * is applicable to any field of the document at any level of the hierarchy. If the field exists,
   * it will be deleted.
   *
   * @param unsetExpr Unset Expression Document.
   * @param bsonDocument Document contents to be updated.
   */
  private static void executeRemoveExpression(final BsonDocument unsetExpr,
      final BsonDocument bsonDocument) {
    for (Map.Entry<String, BsonValue> removeField : unsetExpr.entrySet()) {
      String fieldKey = removeField.getKey();
      BsonValue topLevelValue = bsonDocument.get(fieldKey);
      // If the top level field exists, perform the operation here and return.
      if (topLevelValue != null || (!fieldKey.contains(".") && !fieldKey.contains("["))) {
        bsonDocument.remove(fieldKey);
      } else {
        // If the top level field does not exist and the field key contains "." or "[]" notations
        // for nested document or nested array, use the self-recursive function to go through
        // the document tree, one level at a time.
        updateNestedField(bsonDocument, 0, fieldKey, null, UpdateOp.UNSET);
      }
    }
  }

  /**
   * Update the given document by performing SET operation on a given field. This operation is
   * applicable to any field of the document. The SET operation represents either adding a new
   * field with the given value of updating the existing field with new value provided by the
   * SET Expression Document.
   *
   * @param setExpression SET Expression Document.
   * @param bsonDocument Document contents to be updated.
   */
  private static void executeSetExpression(final BsonDocument setExpression,
      final BsonDocument bsonDocument) {
    for (Map.Entry<String, BsonValue> setEntry : setExpression.entrySet()) {
      String fieldKey = setEntry.getKey();
      BsonValue fieldVal = setEntry.getValue();
      BsonValue topLevelValue = bsonDocument.get(fieldKey);
      BsonValue newVal = getNewFieldValue(fieldVal, bsonDocument);
      // If the top level field exists, perform the operation here and return.
      if (topLevelValue != null || (!fieldKey.contains(".") && !fieldKey.contains("["))) {
        bsonDocument.put(fieldKey, newVal);
      } else {
        // If the top level field does not exist and the field key contains "." or "[]" notations
        // for nested document or nested array, use the self-recursive function to go through
        // the document tree, one level at a time.
        updateNestedField(bsonDocument, 0, fieldKey, newVal, UpdateOp.SET);
      }
    }
  }

  /**
   * Update the nested field with the given update operation. The update operation is determined
   * by the enum value {@code updateOp}.
   * The field key is expected to contain "." and/or "[]" notations for nested documents and/or
   * nested array elements. This function keeps recursively calling itself until it reaches the
   * leaf node in the given tree.
   * For instance, for field key "category.subcategories.brands[5]", first the function
   * evaluates and retrieves the value for top-level field "category". The value of "category"
   * is expected to be nested document. First function call has value as full document, it retries
   * nested document under "category" field and calls the function recursively with index value
   * same as index value of first "." (dot) in the field key. For field key
   * "category.subcategories.brands[5]", the index value would be 8. The second function call
   * retrieves value of field key "subcategories", which is expected to be nested document.
   * The third function call gets this nested document as BsonValue and index value as 22 as the
   * field key has second "." (dot) notation at index 22. The third function call searches for
   * field key "brands" and expects its value as nested array. The forth function call gets
   * this nested array as BsonValue and index 29 as the field key has "[]" array notation starting
   * at index 29. The forth function call retrieves value of nested array element at index 5.
   * As the function is at leaf node in the tree, now it performs the update operation as per the
   * given update operator ($SET / $UNSET / $ADD / $DELETE_FROM_SET) semantics.
   *
   * @param value Bson value at the given level of the document hierarchy.
   * @param idx The index value for the given field key. The function is expected to retrieve
   * the value of the nested document or array at the given level of the tree.
   * @param fieldKey The full field key.
   * @param newVal The new Bson value to be added to the given nested document or a particular
   * index of the given nested array.
   * @param updateOp The enum value representing the update operation to perform.
   */
  private static void updateNestedField(final BsonValue value,
      final int idx,
      final String fieldKey,
      final BsonValue newVal,
      final UpdateOp updateOp) {
    int curIdx = idx;
    if (fieldKey.charAt(curIdx) == '.') {
      if (value == null || !value.isDocument()) {
        LOGGER.error(
            "Value is null or not document. Value: {}, Idx: {}, fieldKey: {}, New val: {},"
                + " Update op: {}",
            value, idx, fieldKey, newVal, updateOp);
        throw new RuntimeException("Value is null or it is not of type document.");
      }
      BsonDocument nestedDocument = (BsonDocument) value;
      curIdx++;
      StringBuilder sb = new StringBuilder();
      for (; curIdx < fieldKey.length(); curIdx++) {
        if (fieldKey.charAt(curIdx) == '.' || fieldKey.charAt(curIdx) == '[') {
          BsonValue nestedValue = nestedDocument.get(sb.toString());
          if (nestedValue == null) {
            LOGGER.error("Should have found nested map for {}", sb);
            return;
          }
          updateNestedField(nestedValue, curIdx, fieldKey, newVal, updateOp);
          return;
        } else {
          sb.append(fieldKey.charAt(curIdx));
        }
      }
      // reached leaf node of the document tree, update the value as per the update operator
      // semantics
      updateDocumentAtLeafNode(newVal, updateOp, nestedDocument, sb);
    } else if (fieldKey.charAt(curIdx) == '[') {
      curIdx++;
      StringBuilder arrayIdxStr = new StringBuilder();
      while (fieldKey.charAt(curIdx) != ']') {
        arrayIdxStr.append(fieldKey.charAt(curIdx));
        curIdx++;
      }
      curIdx++;
      int arrayIdx = Integer.parseInt(arrayIdxStr.toString());
      if (value == null || !value.isArray()) {
        LOGGER.error("Value is null or not document. Value: {}, Idx: {}, fieldKey: {}, New val: {}",
            value, idx, fieldKey, newVal);
        throw new RuntimeException("Value is null or not array.");
      }
      BsonArray nestedArray = (BsonArray) value;
      if (curIdx == fieldKey.length()) {
        // reached leaf node of the document tree, update the value as per the update operator
        // semantics
        updateArrayAtLeafNode(fieldKey, newVal, updateOp, arrayIdx, nestedArray);
        return;
      }
      BsonValue nestedValue = nestedArray.get(arrayIdx);
      if (nestedValue == null) {
        LOGGER.error("Should have found nested list for index {}", arrayIdx);
        return;
      }
      updateNestedField(nestedValue, curIdx, fieldKey, newVal, updateOp);
    } else {
      StringBuilder sb = new StringBuilder();
      for (int i = idx; i < fieldKey.length(); i++) {
        if (fieldKey.charAt(i) == '.') {
          BsonValue topFieldValue = ((BsonDocument) value).get(sb.toString());
          if (topFieldValue == null) {
            LOGGER.error("Incorrect access. Should have found nested bsonDocument for {}", sb);
            throw new RuntimeException("Document does not contain key: " + sb);
          }
          updateNestedField(topFieldValue, i, fieldKey, newVal, updateOp);
          return;
        } else if (fieldKey.charAt(i) == '[') {
          BsonValue topFieldValue = ((BsonDocument) value).get(sb.toString());
          if (topFieldValue == null) {
            LOGGER.error("Incorrect access. Should have found nested list for {}", sb);
            throw new RuntimeException("Document does not contain key: " + sb);
          }
          updateNestedField(topFieldValue, i, fieldKey, newVal, updateOp);
          return;
        } else {
          sb.append(fieldKey.charAt(i));
        }
      }
    }
  }

  /**
   * Perform update operation at the leaf node. This method is called only when the leaf
   * node or the target node for the given field key is encountered while iterating through
   * the tree structure. This is specifically called when the closest ancestor of the
   * given node is a nested array. The leaf node is for the field key. For example, for
   * "category.subcategories", the leaf node is "subcategories" whereas for
   * "category.subcategories.brands[2]", the leaf node is "brands".
   *
   * @param fieldKey The full field key.
   * @param newVal New value to be used while performing update operation on the existing node.
   * @param updateOp Type of the update operation to be performed.
   * @param arrayIdx The index of the array at which the target node value is to be updated.
   * @param nestedArray The parent or ancestor node, expected to be nested array only.
   * For example, for "category.subcategories.brands[5]" field key, the nestedArray is expected to
   * be "brands" array and arrayIdx is expected to be 5.
   */
  private static void updateArrayAtLeafNode(final String fieldKey,
      final BsonValue newVal,
      final UpdateOp updateOp,
      final int arrayIdx,
      final BsonArray nestedArray) {
    switch (updateOp) {
      case SET: {
        if (arrayIdx < nestedArray.size()) {
          nestedArray.set(arrayIdx, newVal);
        } else {
          nestedArray.add(newVal);
        }
        break;
      }
      case UNSET: {
        if (arrayIdx < nestedArray.size()) {
          nestedArray.remove(arrayIdx);
        }
        break;
      }
      case ADD: {
        if (arrayIdx < nestedArray.size()) {
          BsonValue currentValue = nestedArray.get(arrayIdx);
          if (currentValue != null) {
            nestedArray.set(arrayIdx, modifyFieldValueByAdd(currentValue, newVal));
          } else {
            nestedArray.set(arrayIdx, newVal);
          }
        } else {
          nestedArray.add(newVal);
        }
        break;
      }
      case DELETE_FROM_SET: {
        if (arrayIdx < nestedArray.size()) {
          BsonValue currentValue = nestedArray.get(arrayIdx);
          if (currentValue != null) {
            BsonValue modifiedVal = modifyFieldValueByDeleteFromSet(currentValue, newVal);
            if (modifiedVal == null) {
              nestedArray.remove(arrayIdx);
            } else {
              nestedArray.set(arrayIdx, modifiedVal);
            }
          } else {
            LOGGER.info(
                "Nothing to be removed as nested list does not have value for field {}. "
                    + "Update operator: {}",
                fieldKey, updateOp);
          }
        }
        break;
      }
    }
  }

  /**
   * Perform update operation at the leaf node. This method is called only when the leaf
   * node or the target node for the given field key is encountered while iterating through
   * the tree structure. This is specifically called when the closest ancestor of the
   * given node is a nested document. The leaf node is for the field key. For example, for
   * "category.subcategories", the leaf node is "subcategories" whereas for
   * "category.subcategories.brands[2]", the leaf node is "brands".
   *
   * @param newVal New value to be used while performing update operation on the existing node.
   * @param updateOp Type of the update operation to be performed.
   * @param nestedDocument The parent or ancestor node, expected to be nested document only.
   * @param targetNodeFieldKey The target fieldKey value. For example, for "category.subcategories"
   * field key, the target node field key is expected to be "subcategories" and the nestedDocument
   * is expected to be "category" document.
   */
  private static void updateDocumentAtLeafNode(BsonValue newVal, UpdateOp updateOp,
      BsonDocument nestedDocument, StringBuilder targetNodeFieldKey) {
    switch (updateOp) {
      case SET: {
        nestedDocument.put(targetNodeFieldKey.toString(), newVal);
        break;
      }
      case UNSET: {
        nestedDocument.remove(targetNodeFieldKey.toString());
        break;
      }
      case ADD: {
        BsonValue currentValue = nestedDocument.get(targetNodeFieldKey.toString());
        if (currentValue != null) {
          nestedDocument.put(targetNodeFieldKey.toString(), modifyFieldValueByAdd(currentValue, newVal));
        } else {
          nestedDocument.put(targetNodeFieldKey.toString(), newVal);
        }
        break;
      }
      case DELETE_FROM_SET: {
        BsonValue currentValue = nestedDocument.get(targetNodeFieldKey.toString());
        if (currentValue != null) {
          BsonValue modifiedVal = modifyFieldValueByDeleteFromSet(currentValue, newVal);
          if (modifiedVal == null) {
            nestedDocument.remove(targetNodeFieldKey.toString());
          } else {
            nestedDocument.put(targetNodeFieldKey.toString(), modifiedVal);
          }
        } else {
          LOGGER.info(
              "Nothing to be removed as field with key {} does not exist. Update operator: {}",
              targetNodeFieldKey, updateOp);
        }
        break;
      }
    }
  }

  /**
   * Retrieve the value to be updated for the given current value. If the current value does not
   * contain any arithmetic operators, the current value is returned without any modifications.
   * If the current value contains arithmetic expressions like "a + b" or "a - b", the values of
   * operands are retrieved from the given document and if the values are numeric, the given
   * arithmetic operation is performed.
   *
   * @param curValue The current value.
   * @param bsonDocument The document with all field key-value pairs.
   * @return Updated values to be used by SET operation.
   */
  private static BsonValue getNewFieldValue(final BsonValue curValue,
                                            final BsonDocument bsonDocument) {
    if (curValue != null && curValue.isString() && (
        ((BsonString) curValue).getValue().contains(" + ") || ((BsonString) curValue).getValue()
            .contains(" - "))) {
      String[] tokens = ((BsonString) curValue).getValue().split("\\s+");
      boolean addNum = true;
      //      Pattern pattern = Pattern.compile(":?[a-zA-Z0-9]+");
      Pattern pattern = Pattern.compile("[#:$]?[^\\s\\n]+");
      Number newNum = null;
      for (String token : tokens) {
        if (token.equals("+")) {
          addNum = true;
          continue;
        } else if (token.equals("-")) {
          addNum = false;
          continue;
        }
        Matcher matcher = pattern.matcher(token);
        if (matcher.find()) {
          String operand = matcher.group();
          Number literalNum;
          BsonValue topLevelValue = bsonDocument.get(operand);
          BsonValue bsonValue = topLevelValue != null ?
              topLevelValue :
              CommonComparisonExpressionUtils.getFieldFromDocument(operand, bsonDocument);

          if (bsonValue == null && (literalNum = stringToNumber(operand)) != null) {
            Number val = literalNum;
            newNum =
                newNum == null ? val : (addNum ? addNum(newNum, val) : subtractNum(newNum, val));
          } else {
            if (bsonValue == null) {
              throw new IllegalArgumentException("Operand " + operand + " does not exist");
            }
            if (!bsonValue.isNumber() && !bsonValue.isDecimal128()) {
              throw new IllegalArgumentException(
                  "Operand " + operand + " is not provided as number type");
            }
            Number val = getNumberFromBsonNumber((BsonNumber) bsonValue);
            newNum =
                newNum == null ? val : (addNum ? addNum(newNum, val) : subtractNum(newNum, val));
          }
        }
      }
      return getBsonNumberFromNumber(newNum);
    }
    return curValue;
  }

  /**
   * Performs arithmetic addition operation on the given numeric operands.
   *
   * @param num1 First number.
   * @param num2 Second number.
   * @return The value as addition of both numbers.
   */
  private static Number addNum(final Number num1, final Number num2) {
    if (num1 instanceof Double || num2 instanceof Double) {
      return num1.doubleValue() + num2.doubleValue();
    } else if (num1 instanceof Float || num2 instanceof Float) {
      return num1.floatValue() + num2.floatValue();
    } else if (num1 instanceof Long || num2 instanceof Long) {
      return num1.longValue() + num2.longValue();
    } else {
      return num1.intValue() + num2.intValue();
    }
  }

  /**
   * Performs arithmetic subtraction operation on the given numeric operands.
   *
   * @param num1 First number.
   * @param num2 Second number.
   * @return The subtracted value as first number minus second number.
   */
  private static Number subtractNum(final Number num1, final Number num2) {
    if (num1 instanceof Double || num2 instanceof Double) {
      return num1.doubleValue() - num2.doubleValue();
    } else if (num1 instanceof Float || num2 instanceof Float) {
      return num1.floatValue() - num2.floatValue();
    } else if (num1 instanceof Long || num2 instanceof Long) {
      return num1.longValue() - num2.longValue();
    } else {
      return num1.intValue() - num2.intValue();
    }
  }

  /**
   * Convert the given Number to String.
   *
   * @param number The Number object.
   * @return String represented number value.
   */
  private static String numberToString(Number number) {
    if (number instanceof Integer || number instanceof Short || number instanceof Byte) {
      return Integer.toString(number.intValue());
    } else if (number instanceof Long) {
      return Long.toString(number.longValue());
    } else if (number instanceof Double) {
      return Double.toString(number.doubleValue());
    } else if (number instanceof Float) {
      return Float.toString(number.floatValue());
    }
    throw new RuntimeException("Number type is not known for number: " + number);
  }

  /**
   * Convert the given String to Number.
   *
   * @param number The String represented numeric value.
   * @return The Number object.
   */
  private static Number stringToNumber(String number) {
    try {
      return Integer.parseInt(number);
    } catch (NumberFormatException e) {
      // no-op
    }
    try {
      return Long.parseLong(number);
    } catch (NumberFormatException e) {
      // no-op
    }
    try {
      return Double.parseDouble(number);
    } catch (NumberFormatException e) {
      // no-op
    }
    try {
      return NumberFormat.getInstance().parse(number);
    } catch (ParseException e) {
      return null;
    }
  }

  /**
   * Convert Number to BsonNumber.
   *
   * @param number The Number object.
   * @return The BsonNumber object.
   */
  private static BsonNumber getBsonNumberFromNumber(Number number) {
    BsonNumber bsonNumber;
    if (number instanceof Integer || number instanceof Short || number instanceof Byte) {
      bsonNumber = new BsonInt32(number.intValue());
    } else if (number instanceof Long) {
      bsonNumber = new BsonInt64(number.longValue());
    } else if (number instanceof Double || number instanceof Float) {
      bsonNumber = new BsonDouble(number.doubleValue());
    } else if (number instanceof BigDecimal) {
      bsonNumber = new BsonDecimal128(new Decimal128((BigDecimal) number));
    } else {
      throw new IllegalArgumentException("Unsupported Number type: " + number.getClass());
    }
    return bsonNumber;
  }

  /**
   * Convert BsonNumber to Number.
   *
   * @param bsonNumber The BsonNumber object.
   * @return The Number object.
   */
  public static Number getNumberFromBsonNumber(BsonNumber bsonNumber) {
    if (bsonNumber instanceof BsonInt32) {
      return ((BsonInt32) bsonNumber).getValue();
    } else if (bsonNumber instanceof BsonInt64) {
      return ((BsonInt64) bsonNumber).getValue();
    } else if (bsonNumber instanceof BsonDouble) {
      return ((BsonDouble) bsonNumber).getValue();
    } else if (bsonNumber instanceof BsonDecimal128) {
      return ((BsonDecimal128) bsonNumber).getValue().bigDecimalValue();
    } else {
      throw new IllegalArgumentException("Unsupported BsonNumber type: " + bsonNumber.getClass());
    }
  }

  /**
   * Returns true if the given BsonValue represents Set data structure.
   *
   * @param bsonValue The value.
   * @return True if the given BsonValue represents Set data structure.
   */
  private static boolean isBsonSet(final BsonValue bsonValue) {
    if (!bsonValue.isDocument()) {
      return false;
    }
    BsonDocument bsonDocument = (BsonDocument) bsonValue;
    if (bsonDocument.size() == 1 && bsonDocument.containsKey("$set")) {
      BsonValue value = bsonDocument.get("$set");
      return value != null && value.isArray();
    }
    return false;
  }

  /**
   * Returns true if both values represent Set data structure and the contents of the Set are
   * of same type.
   *
   * @param bsonValue1 First value.
   * @param bsonValue2 Second value.
   * @return True if both values represent Set data structure and the contents of the Set are
   * of same type.
   */
  private static boolean areBsonSetOfSameType(final BsonValue bsonValue1,
      final BsonValue bsonValue2) {
    if (!isBsonSet(bsonValue1) || !isBsonSet(bsonValue2)) {
      return false;
    }
    BsonArray bsonArray1 = (BsonArray) ((BsonDocument) bsonValue1).get("$set");
    BsonArray bsonArray2 = (BsonArray) ((BsonDocument) bsonValue2).get("$set");
    return bsonArray1.get(0).getBsonType().equals(bsonArray2.get(0).getBsonType());
  }

}
