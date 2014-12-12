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

import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.schema.types.PDecimal;

import com.google.common.collect.Lists;

import java.math.BigDecimal;

import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;

/**
 *
 * Class encapsulating the FLOOR operation on 
 * a column/literal of type {@link org.apache.phoenix.schema.types.PDecimal}.
 *
 *
 * @since 3.0.0
 */
public class FloorDecimalExpression extends RoundDecimalExpression {

    public FloorDecimalExpression() {}

    private FloorDecimalExpression(List<Expression> children) {
        super(children);
    }

    /**
     * Creates a {@link FloorDecimalExpression} with rounding scale given by @param scale.
     *
     */
    public static Expression create(Expression expr, int scale) throws SQLException {
        if (expr.getDataType().isCoercibleTo(PLong.INSTANCE)) {
            return expr;
        }
        Expression scaleExpr = LiteralExpression.newConstant(scale, PInteger.INSTANCE, Determinism.ALWAYS);
        List<Expression> expressions = Lists.newArrayList(expr, scaleExpr);
        return new FloorDecimalExpression(expressions);
    }

    public static Expression create(List<Expression> exprs) throws SQLException {
        Expression expr = exprs.get(0);
        if (expr.getDataType().isCoercibleTo(PLong.INSTANCE)) {
            return expr;
        }
        if (exprs.size() == 1) {
            Expression scaleExpr = LiteralExpression.newConstant(0, PInteger.INSTANCE, Determinism.ALWAYS);
            exprs = Lists.newArrayList(expr, scaleExpr);
        }
        return new FloorDecimalExpression(exprs);
    }

    /**
     * Creates a {@link FloorDecimalExpression} with a default scale of 0 used for rounding.
     *
     */
    public static Expression create(Expression expr) throws SQLException {
        return create(expr, 0);
    }

    @Override
    protected RoundingMode getRoundingMode() {
        return RoundingMode.FLOOR;
    }

    @Override
    public String getName() {
        return FloorFunction.NAME;
    }

    /**
     * {@inheritDoc }
     */
    @Override
    protected KeyRange getInputRangeProducing(BigDecimal result) {
        if(!hasEnoughPrecisionToProduce(result)) {
            throw new IllegalArgumentException("Cannot produce input range for decimal " + result 
                + ", not enough precision with scale " + getRoundingScale());
        }
        byte[] lowerRange = PDecimal.INSTANCE.toBytes(result);
        byte[] upperRange = PDecimal.INSTANCE.toBytes(stepNextInScale(result));
        return KeyRange.getKeyRange(lowerRange, upperRange);
    }

}
