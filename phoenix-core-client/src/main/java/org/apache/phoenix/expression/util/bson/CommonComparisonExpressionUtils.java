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

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonNumber;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Common Util functions to help retrieve BSON Document values based on the given field expressions.
 */
public class CommonComparisonExpressionUtils {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CommonComparisonExpressionUtils.class);

  /**
   * Retrieve the value associated with the document field key. The field key can represent
   * any top level or nested fields within the document. The caller should use "." notation for
   * accessing nested document elements and "[n]" notation for accessing nested array elements. Top
   * level fields do not require any additional character.
   *
   * @param documentFieldKey The document field key for which the value is returned.
   * @param rawBsonDocument The document from which to find the value.
   * @return If the field key exists in the document, return the corresponding value. Else
   * return null.
   */
  public static BsonValue getFieldFromDocument(final String documentFieldKey,
                                               final BsonDocument rawBsonDocument) {
    if (documentFieldKey.contains(".") || documentFieldKey.contains("[")) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < documentFieldKey.length(); i++) {
        if (documentFieldKey.charAt(i) == '.') {
          BsonValue value = rawBsonDocument.get(sb.toString());
          if (value == null) {
            return null;
          }
          return getNestedFieldVal(value, i, documentFieldKey);
        } else if (documentFieldKey.charAt(i) == '[') {
          BsonValue value = rawBsonDocument.get(sb.toString());
          if (value == null) {
            return null;
          }
          return getNestedFieldVal(value, i, documentFieldKey);
        } else {
          sb.append(documentFieldKey.charAt(i));
        }
      }
    } else {
      return rawBsonDocument.get(documentFieldKey);
    }
    return null;
  }

  /**
   * Retrieve the value associated with the nested field key within the document.
   *
   * @param value Value of the parent data structure (document or array) which is used to search
   * nested elements from.
   * @param idx Index used to track which part of the field key has been covered so far.
   * @param documentFieldKey The document field key for which the value is returned.
   * @return If the field key exists in the document, return the corresponding value. Else
   * return null.
   */
  public static BsonValue getNestedFieldVal(BsonValue value, int idx,
                                            final String documentFieldKey) {
    if (idx == documentFieldKey.length()) {
      return value;
    }
    int curIdx = idx;
    if (documentFieldKey.charAt(curIdx) == '.') {
      BsonDocument nestedMap = value != null && value.isDocument() ? (BsonDocument) value : null;
      if (nestedMap == null) {
        LOGGER.error("Incorrect access. Should have found nested map for value: {}", value);
        return null;
      }
      curIdx++;
      StringBuilder sb = new StringBuilder();
      for (; curIdx < documentFieldKey.length(); curIdx++) {
        if (documentFieldKey.charAt(curIdx) == '.' || documentFieldKey.charAt(curIdx) == '[') {
          BsonValue nestedValue = nestedMap.get(sb.toString());
          if (nestedValue == null) {
            return null;
          }
          return getNestedFieldVal(nestedValue, curIdx, documentFieldKey);
        } else {
          sb.append(documentFieldKey.charAt(curIdx));
        }
      }
      return nestedMap.get(sb.toString());
    } else if (documentFieldKey.charAt(curIdx) == '[') {
      curIdx++;
      StringBuilder arrayIdxStr = new StringBuilder();
      while (documentFieldKey.charAt(curIdx) != ']') {
        arrayIdxStr.append(documentFieldKey.charAt(curIdx));
        curIdx++;
      }
      curIdx++;
      int arrayIdx = Integer.parseInt(arrayIdxStr.toString());
      BsonArray nestedList = value != null && value.isArray() ? (BsonArray) value : null;
      if (nestedList == null) {
        LOGGER.error("Incorrect access. Should have found nested list for value: {}", value);
        return null;
      }
      if (arrayIdx >= nestedList.size()) {
        LOGGER.error(
            "Incorrect access. Nested list size {} is less than attempted index access at {}",
            nestedList.size(), arrayIdx);
        return null;
      }
      BsonValue valueAtIdx = nestedList.get(arrayIdx);
      if (curIdx == documentFieldKey.length()) {
        return valueAtIdx;
      }
      return getNestedFieldVal(valueAtIdx, curIdx, documentFieldKey);
    }
    LOGGER.error("This is erroneous case. getNestedFieldVal should not be used for "
        + "top level document fields");
    return null;
  }

  /**
   * Compares two values: value and compareValue. If both have same data type and the value of
   * actual value is less than the compareValue, then returns true. Otherwise, returns false.
   *
   * @param value The actual value to compare against compareValue.
   * @param compareValue The value against which the actual value is compared.
   * @return True if both have same data type and the value is less than compareValue.
   */
  public static boolean lessThan(BsonValue value, BsonValue compareValue) {
    if (value.isString() && compareValue.isString()) {
      return ((BsonString) value).getValue().compareTo(((BsonString) compareValue).getValue()) < 0;
    }
    if ((value.isNumber() || value.isDecimal128()) && (compareValue.isNumber()
        || compareValue.isDecimal128())) {
      return ((BsonNumber) value).doubleValue() < ((BsonNumber) compareValue).doubleValue();
    }
    if (value.isBinary() && compareValue.isBinary()
        && ((BsonBinary) value).getType() == ((BsonBinary) compareValue).getType()) {
      byte[] b1 = ((BsonBinary) value).getData();
      byte[] b2 = ((BsonBinary) compareValue).getData();
      return Bytes.compareTo(b1, b2) < 0;
    }
    if (value.isDateTime() && compareValue.isDateTime()) {
      return ((BsonDateTime) value).getValue() < ((BsonDateTime) compareValue).getValue();
    }
    LOGGER.error("Expected value comparison for lessThan is not of type String, Number, Binary"
        + " or DateTime. Actual value: {} , Expected value: {}", value, compareValue);
    return false;
  }

  /**
   * Compares two values: value and compareValue. If both have same data type and the value of
   * actual value is less than or equals to the compareValue, then returns true. Otherwise, returns
   * false.
   *
   * @param value The actual value to compare against compareValue.
   * @param compareValue The value against which the actual value is compared.
   * @return True if both have same data type and the value is less than or equals to the
   * compareValue.
   */
  public static boolean lessThanOrEquals(BsonValue value, BsonValue compareValue) {
    if (value.isString() && compareValue.isString()) {
      return ((BsonString) value).getValue().compareTo(((BsonString) compareValue).getValue()) <= 0;
    }
    if ((value.isNumber() || value.isDecimal128()) && (compareValue.isNumber()
        || compareValue.isDecimal128())) {
      return ((BsonNumber) value).doubleValue() <= ((BsonNumber) compareValue).doubleValue();
    }
    if (value.isBinary() && compareValue.isBinary()
        && ((BsonBinary) value).getType() == ((BsonBinary) compareValue).getType()) {
      byte[] b1 = ((BsonBinary) value).getData();
      byte[] b2 = ((BsonBinary) compareValue).getData();
      return Bytes.compareTo(b1, b2) <= 0;
    }
    if (value.isDateTime() && compareValue.isDateTime()) {
      return ((BsonDateTime) value).getValue() <= ((BsonDateTime) compareValue).getValue();
    }
    LOGGER.error("Expected value comparison for lessThanOrEquals is not of type String, Number,"
        + " Binary or DateTime. Actual value: {} , Expected value: {}", value, compareValue);
    return false;
  }

  /**
   * Compares two values: value and compareValue. If both have same data type and the value of
   * actual value is greater than the compareValue, then returns true. Otherwise, returns
   * false.
   *
   * @param value The actual value to compare against compareValue.
   * @param compareValue The value against which the actual value is compared.
   * @return True if both have same data type and the value is greater than the compareValue.
   */
  public static boolean greaterThan(BsonValue value, BsonValue compareValue) {
    if (value.isString() && compareValue.isString()) {
      return ((BsonString) value).getValue().compareTo(((BsonString) compareValue).getValue()) > 0;
    }
    if ((value.isNumber() || value.isDecimal128()) && (compareValue.isNumber()
        || compareValue.isDecimal128())) {
      return ((BsonNumber) value).doubleValue() > ((BsonNumber) compareValue).doubleValue();
    }
    if (value.isBinary() && compareValue.isBinary()
        && ((BsonBinary) value).getType() == ((BsonBinary) compareValue).getType()) {
      byte[] b1 = ((BsonBinary) value).getData();
      byte[] b2 = ((BsonBinary) compareValue).getData();
      return Bytes.compareTo(b1, b2) > 0;
    }
    if (value.isDateTime() && compareValue.isDateTime()) {
      return ((BsonDateTime) value).getValue() > ((BsonDateTime) compareValue).getValue();
    }
    LOGGER.error("Expected value comparison for greaterThan is not of type String, Number, Binary"
        + " or DateTime. Actual value: {} , Expected value: {}", value, compareValue);
    return false;
  }

  /**
   * Compares two values: value and compareValue. If both have same data type and the value of
   * actual value is greater than or equals to the compareValue, then returns true. Otherwise,
   * returns false.
   *
   * @param value The actual value to compare against compareValue.
   * @param compareValue The value against which the actual value is compared.
   * @return True if both have same data type and the value is greater than or equals to the
   * compareValue.
   */
  public static boolean greaterThanOrEquals(BsonValue value, BsonValue compareValue) {
    if (value.isString() && compareValue.isString()) {
      return ((BsonString) value).getValue().compareTo(((BsonString) compareValue).getValue()) >= 0;
    }
    if ((value.isNumber() || value.isDecimal128()) && (compareValue.isNumber()
        || compareValue.isDecimal128())) {
      return ((BsonNumber) value).doubleValue() >= ((BsonNumber) compareValue).doubleValue();
    }
    if (value.isBinary() && compareValue.isBinary()
        && ((BsonBinary) value).getType() == ((BsonBinary) compareValue).getType()) {
      byte[] b1 = ((BsonBinary) value).getData();
      byte[] b2 = ((BsonBinary) compareValue).getData();
      return Bytes.compareTo(b1, b2) >= 0;
    }
    if (value.isDateTime() && compareValue.isDateTime()) {
      return ((BsonDateTime) value).getValue() >= ((BsonDateTime) compareValue).getValue();
    }
    LOGGER.error("Expected value comparison for greaterThanOrEquals is not of type String,"
            + " Number, Binary or DateTime. Actual value: {} , Expected value: {}", value,
        compareValue);
    return false;
  }
}
