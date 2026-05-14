/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.util;

import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.BsonValueFunction;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.parse.ParseNode;

/** Helpers for identifying BSON-path expressions in DDL and at runtime. */
public final class BsonIndexUtil {

  private BsonIndexUtil() {
  }

  /** Returns true if any node in the parse tree is BSON_VALUE or JSON_VALUE. */
  public static boolean containsBsonExpression(ParseNode node) {
    if (node == null) {
      return false;
    }
    if (node instanceof FunctionParseNode) {
      String n = ((FunctionParseNode) node).getName();
      if ("BSON_VALUE".equalsIgnoreCase(n) || "JSON_VALUE".equalsIgnoreCase(n)) {
        return true;
      }
    }
    for (ParseNode child : node.getChildren()) {
      if (containsBsonExpression(child)) {
        return true;
      }
    }
    return false;
  }

  /** Returns true if the compiled expression's root is a BSON_VALUE call. */
  public static boolean isBsonPathExpression(Expression expression) {
    return expression instanceof BsonValueFunction;
  }
}
