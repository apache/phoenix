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
import org.apache.phoenix.expression.function.FunctionExpression;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.apache.phoenix.util.ExpressionUtil;

/** Base parse node for vector distance functions. */
public abstract class DistanceFunctionParseNode extends FunctionParseNode {

  public DistanceFunctionParseNode(String name, List<ParseNode> children,
    BuiltInFunctionInfo info) {
    super(name, children, info);
  }

  @Override
  public List<Expression> validate(List<Expression> children, StatementContext context)
    throws SQLException {
    if (children != null && children.size() >= 2) {
      Expression child0 = children.get(0);
      Expression child1 = children.get(1);

      PDataType type0 = child0.getDataType();
      PDataType type1 = child1.getDataType();

      PDataType targetVectorType = null;
      Integer targetDim = null;

      if (type0 != null && type0.isVectorType()) {
        targetVectorType = type0;
        targetDim = child0.getMaxLength();
      } else if (type1 != null && type1.isVectorType()) {
        targetVectorType = type1;
        targetDim = child1.getMaxLength();
      }

      if (targetVectorType == null) {
        targetVectorType = PVectorFloat.INSTANCE;
      }

      if (type0 != null && !type0.isVectorType() && type0.isArrayType()) {
        children.set(0, coerceToVector(child0, targetVectorType, targetDim, context));
      }

      if (type1 != null && !type1.isVectorType() && type1.isArrayType()) {
        children.set(1, coerceToVector(child1, targetVectorType, targetDim, context));
      }
    }
    return super.validate(children, context);
  }

  private static Expression coerceToVector(Expression expr, PDataType targetVectorType,
    Integer targetDim, StatementContext context) throws SQLException {
    if (ExpressionUtil.isConstant(expr)) {
      ImmutableBytesWritable ptr = context.getTempPtr();
      expr.evaluate(null, ptr);
      Object arrayVal =
        expr.getDataType().toObject(ptr, expr.getSortOrder(), expr.getMaxLength(), expr.getScale());
      Object vectorVal = targetVectorType.toObject(arrayVal, expr.getDataType());
      int dim = targetDim != null
        ? targetDim
        : (vectorVal instanceof float[]
          ? ((float[]) vectorVal).length
          : (vectorVal instanceof double[] ? ((double[]) vectorVal).length : 0));
      return LiteralExpression.newConstant(vectorVal, targetVectorType, dim, null,
        expr.getSortOrder(), expr.getDeterminism(), true);
    } else {
      return CoerceExpression.create(expr, targetVectorType, expr.getSortOrder(), targetDim);
    }
  }

  @Override
  public FunctionExpression create(List<Expression> children, StatementContext context)
    throws SQLException {
    try {
      DistanceFunction.validateChildren(children);
    } catch (IllegalArgumentException e) {
      throw new SQLException(e.getMessage(), e);
    }
    return createFunction(children, context);
  }

  protected abstract FunctionExpression createFunction(List<Expression> children,
    StatementContext context) throws SQLException;
}
