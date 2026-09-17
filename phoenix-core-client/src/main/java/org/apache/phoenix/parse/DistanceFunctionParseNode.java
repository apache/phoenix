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
package org.apache.phoenix.parse;

import java.sql.SQLException;
import java.util.List;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.expression.CoerceExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.function.DistanceFunction;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.apache.phoenix.util.ExpressionUtil;

/**
 * Parse node for the vector distance functions. It converts an array argument to the vector type of
 * the other argument, or to a float vector if neither argument is a vector. Then it makes sure that
 * the types and dimensions of the two vectors are compatible, before it creates the function.
 */
public class DistanceFunctionParseNode extends FunctionParseNode {

  public DistanceFunctionParseNode(String name, List<ParseNode> children,
    BuiltInFunctionInfo info) {
    super(name, children, info);
  }

  @Override
  public List<Expression> validate(List<Expression> children, StatementContext context)
    throws SQLException {
    Expression child0 = children.get(0);
    Expression child1 = children.get(1);
    PDataType type0 = child0.getDataType();
    PDataType type1 = child1.getDataType();
    PDataType targetType = PVectorFloat.INSTANCE;
    Integer targetDim = null;
    if (type0 != null && type0.isVectorType()) {
      targetType = type0;
      targetDim = child0.getMaxLength();
    } else if (type1 != null && type1.isVectorType()) {
      targetType = type1;
      targetDim = child1.getMaxLength();
    }
    if (type0 != null && !type0.isVectorType() && type0.isArrayType()) {
      children.set(0, coerceToVector(child0, targetType, targetDim, context));
    }
    if (type1 != null && !type1.isVectorType() && type1.isArrayType()) {
      children.set(1, coerceToVector(child1, targetType, targetDim, context));
    }
    return super.validate(children, context);
  }

  private static Expression coerceToVector(Expression expr, PDataType targetType, Integer targetDim,
    StatementContext context) throws SQLException {
    if (!ExpressionUtil.isConstant(expr)) {
      return CoerceExpression.create(expr, targetType, expr.getSortOrder(), targetDim);
    }
    ImmutableBytesWritable ptr = context.getTempPtr();
    expr.evaluate(null, ptr);
    Object array =
      expr.getDataType().toObject(ptr, expr.getSortOrder(), expr.getMaxLength(), expr.getScale());
    Object vector = targetType.toObject(array, expr.getDataType());
    // The max length carries the literal dimension, so that validation at compile time can
    // compare it with the dimension of the other operand.
    return LiteralExpression.newConstant(vector, targetType, targetType.getMaxLength(vector), null,
      expr.getSortOrder(), expr.getDeterminism(), true);
  }

  @Override
  public Expression create(List<Expression> children, StatementContext context)
    throws SQLException {
    DistanceFunction.validateChildren(children);
    return super.create(children, context);
  }
}
