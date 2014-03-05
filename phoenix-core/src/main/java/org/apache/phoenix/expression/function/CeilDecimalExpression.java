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
package org.apache.phoenix.expression.function;

import java.math.RoundingMode;
import java.sql.SQLException;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.schema.PDataType;

/**
 * 
 * Class encapsulating the CEIL operation on a {@link org.apache.phoenix.schema.PDataType#DECIMAL}
 *
 * 
 * @since 3.0.0
 */
public class CeilDecimalExpression extends RoundDecimalExpression {
    
    public CeilDecimalExpression() {}
    
    public CeilDecimalExpression(List<Expression> children) {
        super(children);
    }
    
  /**
   * Creates a {@link CeilDecimalExpression} with rounding scale given by @param scale.
   *
   */
   public static Expression create(Expression expr, int scale) throws SQLException {
       if (expr.getDataType().isCoercibleTo(PDataType.LONG)) {
           return expr;
       }
       Expression scaleExpr = LiteralExpression.newConstant(scale, PDataType.INTEGER, true);
       List<Expression> expressions = Lists.newArrayList(expr, scaleExpr);
       return new CeilDecimalExpression(expressions);
   }
   
   /**
    * Creates a {@link CeilDecimalExpression} with a default scale of 0 used for rounding. 
    *
    */
   public static Expression create(Expression expr) throws SQLException {
       return create(expr, 0);
   }
    
    @Override
    protected RoundingMode getRoundingMode() {
        return RoundingMode.CEILING;
    }
    
    @Override
    public String getName() {
        return CeilFunction.NAME;
    }
    
}
