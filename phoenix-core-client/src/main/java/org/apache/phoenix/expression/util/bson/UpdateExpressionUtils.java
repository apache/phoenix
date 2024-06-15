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
 * BSON Update Expression Utility to perform the Document updates.
 */
public class UpdateExpressionUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(UpdateExpressionUtils.class);

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

    if (updateExpression.containsKey("$DELETE")) {
      executeDeleteExpression((BsonDocument) updateExpression.get("$DELETE"), bsonDocument);
    }
  }

  private static void executeDeleteExpression(final BsonDocument deleteExpr,
      final BsonDocument bsonDocument) {
    for (Map.Entry<String, BsonValue> deleteEntry : deleteExpr.entrySet()) {
      String fieldKey = deleteEntry.getKey();
      BsonValue newVal = deleteEntry.getValue();
      BsonValue topLevelValue = bsonDocument.get(fieldKey);
      if (!isBsonSet(newVal)) {
        throw new RuntimeException("Type of new value to be removed should be sets only");
      }
      if (topLevelValue != null) {
        BsonValue value = modifyfieldValueByDelete(topLevelValue, newVal);
        if (value == null) {
          bsonDocument.remove(fieldKey);
        } else {
          bsonDocument.put(fieldKey, value);
        }
      } else if (!fieldKey.contains(".") && !fieldKey.contains("[")) {
        LOGGER.info("Nothing to be removed as field with key {} does not exist", fieldKey);
      } else {
        updateNestedfieldEntryByDelete(fieldKey, bsonDocument, newVal);
      }
    }
  }

  private static void updateNestedfieldEntryByDelete(final String fieldKey,
      final BsonDocument bsonDocument, final BsonValue newVal) {
    if (fieldKey.contains(".") || fieldKey.contains("[")) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < fieldKey.length(); i++) {
        if (fieldKey.charAt(i) == '.') {
          BsonValue value = bsonDocument.get(sb.toString());
          if (value == null) {
            LOGGER.error("Incorrect access. Should have found nested bsonDocument for {}", sb);
            throw new RuntimeException("Map does not contain key: " + sb);
          }
          updateNestedFieldByDelete(value, i, fieldKey, newVal);
          return;
        } else if (fieldKey.charAt(i) == '[') {
          BsonValue value = bsonDocument.get(sb.toString());
          if (value == null) {
            LOGGER.error("Incorrect access. Should have found nested list for {}", sb);
            throw new RuntimeException("Map does not contain key: " + sb);
          }
          updateNestedFieldByDelete(value, i, fieldKey, newVal);
          return;
        } else {
          sb.append(fieldKey.charAt(i));
        }
      }
    }
  }

  private static void updateNestedFieldByDelete(final BsonValue value, final int idx,
                                                final String fieldKey, final BsonValue newVal) {
    int curIdx = idx;
    if (fieldKey.charAt(curIdx) == '.') {
      if (value == null || !value.isDocument()) {
        LOGGER.error("Update nested field by DELETE: Value is null or not document. "
            + "Value: {}, Idx: {}, fieldKey: {}", value, idx, fieldKey);
        throw new RuntimeException("Value is null or not document.");
      }
      BsonDocument nestedMap = (BsonDocument) value;
      curIdx++;
      StringBuilder sb = new StringBuilder();
      for (; curIdx < fieldKey.length(); curIdx++) {
        if (fieldKey.charAt(curIdx) == '.' || fieldKey.charAt(curIdx) == '[') {
          BsonValue nestedValue = nestedMap.get(sb.toString());
          if (nestedValue == null) {
            LOGGER.error("Should have found nested document for {}", sb);
            return;
          }
          updateNestedFieldByDelete(nestedValue, curIdx, fieldKey, newVal);
          return;
        } else {
          sb.append(fieldKey.charAt(curIdx));
        }
      }
      BsonValue currentValue = nestedMap.get(sb.toString());
      if (currentValue != null) {
        BsonValue modifiedVal = modifyfieldValueByDelete(currentValue, newVal);
        if (modifiedVal == null) {
          nestedMap.remove(sb.toString());
        } else {
          nestedMap.put(sb.toString(), modifiedVal);
        }
      } else {
        LOGGER.info("Nothing to be removed as field with key {} does not exist", sb);
      }
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
        LOGGER.error("Update nested field by DELETE. Value is null or not document. "
            + "Value: {}, Idx: {}, fieldKey: {}", value, idx, fieldKey);
        throw new RuntimeException("Value is null or not array.");
      }
      BsonArray nestedList = (BsonArray) value;
      if (curIdx == fieldKey.length()) {
        if (arrayIdx < nestedList.size()) {
          BsonValue currentValue = nestedList.get(arrayIdx);
          if (currentValue != null) {
            BsonValue modifiedVal = modifyfieldValueByDelete(currentValue, newVal);
            if (modifiedVal == null) {
              nestedList.remove(arrayIdx);
            } else {
              nestedList.set(arrayIdx, modifiedVal);
            }
          } else {
            LOGGER.info("Nothing to be removed as nested list does not have value for field {}",
                fieldKey);
          }
        }
        return;
      }
      BsonValue nestedValue = nestedList.get(arrayIdx);
      if (nestedValue == null) {
        LOGGER.error("Should have found nested list for index {}", arrayIdx);
        return;
      }
      updateNestedFieldByDelete(nestedValue, curIdx, fieldKey, newVal);
      return;
    }
    LOGGER.error("This is erroneous case. updateNestedfieldByDelete should not be used for "
        + "top level fields");
  }

  private static BsonValue modifyfieldValueByDelete(BsonValue currentValue, BsonValue newVal) {
    if (areBsonSetOfSameType(currentValue, newVal)) {
      Set<BsonValue> set1 =
          new HashSet<>(((BsonArray) ((BsonDocument) currentValue).get("$set")).getValues());
      Set<BsonValue> set2 =
          new HashSet<>(((BsonArray) ((BsonDocument) newVal).get("$set")).getValues());
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
            + newVal);
  }

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
      if (topLevelValue != null) {
        bsonDocument.put(fieldKey, modifyfieldValueByAdd(topLevelValue, newVal));
      } else if (!fieldKey.contains(".") && !fieldKey.contains("[")) {
        bsonDocument.put(fieldKey, newVal);
      } else {
        updateNestedfieldEntryByAdd(fieldKey, bsonDocument, newVal);
      }
    }
  }

  private static void updateNestedfieldEntryByAdd(final String fieldKey,
      final BsonDocument bsonDocument, final BsonValue newVal) {
    if (fieldKey.contains(".") || fieldKey.contains("[")) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < fieldKey.length(); i++) {
        if (fieldKey.charAt(i) == '.') {
          BsonValue value = bsonDocument.get(sb.toString());
          if (value == null) {
            LOGGER.error("Incorrect access. Should have found nested bsonDocument for {}", sb);
            throw new RuntimeException("Document does not contain key: " + sb);
          }
          updateNestedfieldByAdd(value, i, fieldKey, newVal);
          return;
        } else if (fieldKey.charAt(i) == '[') {
          BsonValue value = bsonDocument.get(sb.toString());
          if (value == null) {
            LOGGER.error("Incorrect access. Should have found nested list for {}", sb);
            throw new RuntimeException("Document does not contain key: " + sb);
          }
          updateNestedfieldByAdd(value, i, fieldKey, newVal);
          return;
        } else {
          sb.append(fieldKey.charAt(i));
        }
      }
    }
  }

  private static void updateNestedfieldByAdd(BsonValue value, final int idx, final String fieldKey,
      final BsonValue newVal) {
    int curIdx = idx;
    if (fieldKey.charAt(curIdx) == '.') {
      if (value == null || !value.isDocument()) {
        LOGGER.error("Update nested field by ADD: Value is null or not document. "
            + "Value: {}, Idx: {}, fieldKey: {}", value, idx, fieldKey);
        throw new RuntimeException("Value is null or not document.");
      }
      BsonDocument nestedMap = (BsonDocument) value;
      curIdx++;
      StringBuilder sb = new StringBuilder();
      for (; curIdx < fieldKey.length(); curIdx++) {
        if (fieldKey.charAt(curIdx) == '.' || fieldKey.charAt(curIdx) == '[') {
          BsonValue nestedValue = nestedMap.get(sb.toString());
          if (nestedValue == null) {
            LOGGER.error("Should have found nested map for {}", sb);
            return;
          }
          updateNestedfieldByAdd(nestedValue, curIdx, fieldKey, newVal);
          return;
        } else {
          sb.append(fieldKey.charAt(curIdx));
        }
      }
      BsonValue currentValue = nestedMap.get(sb.toString());
      if (currentValue != null) {
        nestedMap.put(sb.toString(), modifyfieldValueByAdd(currentValue, newVal));
      } else {
        nestedMap.put(sb.toString(), newVal);
      }
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
        LOGGER.error("Remove nested field. Value is null or not document. "
            + "Value: {}, Idx: {}, fieldKey: {}", value, idx, fieldKey);
        throw new RuntimeException("Value is null or not array.");
      }
      BsonArray nestedList = (BsonArray) value;
      if (curIdx == fieldKey.length()) {
        if (arrayIdx < nestedList.size()) {
          BsonValue currentValue = nestedList.get(arrayIdx);
          if (currentValue != null) {
            nestedList.set(arrayIdx, modifyfieldValueByAdd(currentValue, newVal));
          } else {
            nestedList.set(arrayIdx, newVal);
          }
        } else {
          nestedList.add(newVal);
        }
        return;
      }
      BsonValue nestedValue = nestedList.get(arrayIdx);
      if (nestedValue == null) {
        LOGGER.error("Should have found nested list for index {}", arrayIdx);
        return;
      }
      updateNestedfieldByAdd(nestedValue, curIdx, fieldKey, newVal);
      return;
    }
    LOGGER.error("This is erroneous case. updateNestedfieldByAdd should not be used for "
        + "top level fields");
  }

  private static BsonValue modifyfieldValueByAdd(BsonValue currentValue, BsonValue newVal) {
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

  private static void executeRemoveExpression(final BsonDocument removeExpr,
      final BsonDocument bsonDocument) {
    for (Map.Entry<String, BsonValue> removefield : removeExpr.entrySet()) {
      String fieldKey = removefield.getKey();
      BsonValue topLevelValue = bsonDocument.get(fieldKey);
      if (topLevelValue != null || (!fieldKey.contains(".") && !fieldKey.contains("["))) {
        bsonDocument.remove(fieldKey);
      } else {
        removeNestedfieldEntry(fieldKey, bsonDocument);
      }
    }
  }

  private static void removeNestedfieldEntry(String fieldKey, BsonDocument bsonDocument) {
    if (fieldKey.contains(".") || fieldKey.contains("[")) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < fieldKey.length(); i++) {
        if (fieldKey.charAt(i) == '.') {
          BsonValue value = bsonDocument.get(sb.toString());
          if (value == null) {
            LOGGER.error("Incorrect access. Should have found nested bsonDocument for {}", sb);
            throw new RuntimeException("Map does not contain key: " + sb);
          }
          removeNestedfield(value, i, fieldKey);
          return;
        } else if (fieldKey.charAt(i) == '[') {
          BsonValue value = bsonDocument.get(sb.toString());
          if (value == null) {
            LOGGER.error("Incorrect access. Should have found nested list for {}", sb);
            throw new RuntimeException("Map does not contain key: " + sb);
          }
          removeNestedfield(value, i, fieldKey);
          return;
        } else {
          sb.append(fieldKey.charAt(i));
        }
      }
    }
  }

  private static void removeNestedfield(BsonValue value, int idx, String fieldKey) {
    int curIdx = idx;
    if (fieldKey.charAt(curIdx) == '.') {
      if (value == null || !value.isDocument()) {
        LOGGER.error("Remove nested field: Value is null or not document. "
            + "Value: {}, Idx: {}, fieldKey: {}", value, idx, fieldKey);
        throw new RuntimeException("Value is null or not document.");
      }
      BsonDocument nestedMap = (BsonDocument) value;
      curIdx++;
      StringBuilder sb = new StringBuilder();
      for (; curIdx < fieldKey.length(); curIdx++) {
        if (fieldKey.charAt(curIdx) == '.' || fieldKey.charAt(curIdx) == '[') {
          BsonValue nestedValue = nestedMap.get(sb.toString());
          if (nestedValue == null) {
            LOGGER.error("Should have found nested document for {}", sb);
            return;
          }
          removeNestedfield(nestedValue, curIdx, fieldKey);
          return;
        } else {
          sb.append(fieldKey.charAt(curIdx));
        }
      }
      nestedMap.remove(sb.toString());
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
        LOGGER.error("Remove nested field. Value is null or not document. "
            + "Value: {}, Idx: {}, fieldKey: {}", value, idx, fieldKey);
        throw new RuntimeException("Value is null or not array.");
      }
      BsonArray nestedList = (BsonArray) value;
      if (curIdx == fieldKey.length()) {
        nestedList.remove(arrayIdx);
        return;
      }
      BsonValue nestedValue = nestedList.get(arrayIdx);
      if (nestedValue == null) {
        LOGGER.error("Should have found nested list for index {}", arrayIdx);
        return;
      }
      removeNestedfield(nestedValue, curIdx, fieldKey);
      return;
    }
    LOGGER.error(
        "This is erroneous case. updateNestedfield should not be used for " + "top level fields");
  }

  private static void executeSetExpression(final BsonDocument setExpression,
      final BsonDocument bsonDocument) {
    for (Map.Entry<String, BsonValue> setEntry : setExpression.entrySet()) {
      String fieldKey = setEntry.getKey();
      BsonValue fieldVal = setEntry.getValue();
      BsonValue topLevelValue = bsonDocument.get(fieldKey);
      BsonValue newVal = getNewfieldValue(fieldVal, bsonDocument);
      if (topLevelValue != null || (!fieldKey.contains(".") && !fieldKey.contains("["))) {
        bsonDocument.put(fieldKey, newVal);
      } else {
        updateNestedfieldEntry(fieldKey, bsonDocument, newVal);
      }
    }
  }

  private static void updateNestedfieldEntry(final String fieldKey, final BsonDocument bsonDocument,
      final BsonValue newVal) {
    if (fieldKey.contains(".") || fieldKey.contains("[")) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < fieldKey.length(); i++) {
        if (fieldKey.charAt(i) == '.') {
          BsonValue value = bsonDocument.get(sb.toString());
          if (value == null) {
            LOGGER.error("Incorrect access. Should have found nested bsonDocument for {}", sb);
            throw new RuntimeException("Document does not contain key: " + sb);
          }
          updateNestedfield(value, i, fieldKey, newVal);
          return;
        } else if (fieldKey.charAt(i) == '[') {
          BsonValue value = bsonDocument.get(sb.toString());
          if (value == null) {
            LOGGER.error("Incorrect access. Should have found nested list for {}", sb);
            throw new RuntimeException("Document does not contain key: " + sb);
          }
          updateNestedfield(value, i, fieldKey, newVal);
          return;
        } else {
          sb.append(fieldKey.charAt(i));
        }
      }
    }
  }

  private static void updateNestedfield(final BsonValue value, final int idx, final String fieldKey,
      final BsonValue newVal) {
    int curIdx = idx;
    if (fieldKey.charAt(curIdx) == '.') {
      if (value == null || !value.isDocument()) {
        LOGGER.error("Value is null or not document. Value: {}, Idx: {}, fieldKey: {}, New val: {}",
            value, idx, fieldKey, newVal);
        throw new RuntimeException("Value is null or not document.");
      }
      BsonDocument nestedMap = (BsonDocument) value;
      curIdx++;
      StringBuilder sb = new StringBuilder();
      for (; curIdx < fieldKey.length(); curIdx++) {
        if (fieldKey.charAt(curIdx) == '.' || fieldKey.charAt(curIdx) == '[') {
          BsonValue nestedValue = nestedMap.get(sb.toString());
          if (nestedValue == null) {
            LOGGER.error("Should have found nested map for {}", sb);
            return;
          }
          updateNestedfield(nestedValue, curIdx, fieldKey, newVal);
          return;
        } else {
          sb.append(fieldKey.charAt(curIdx));
        }
      }
      nestedMap.put(sb.toString(), newVal);
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
      BsonArray nestedList = (BsonArray) value;
      if (curIdx == fieldKey.length()) {
        if (arrayIdx < nestedList.size()) {
          nestedList.set(arrayIdx, newVal);
        } else {
          nestedList.add(newVal);
        }
        return;
      }
      BsonValue nestedValue = nestedList.get(arrayIdx);
      if (nestedValue == null) {
        LOGGER.error("Should have found nested list for index {}", arrayIdx);
        return;
      }
      updateNestedfield(nestedValue, curIdx, fieldKey, newVal);
      return;
    }
    LOGGER.error(
        "This is erroneous case. updateNestedfield should not be used for " + "top level fields");
  }

  private static BsonValue getNewfieldValue(final BsonValue curValue,
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
