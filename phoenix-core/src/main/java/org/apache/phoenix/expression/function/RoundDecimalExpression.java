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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.collect.Lists;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.tuple.Tuple;

/**
 * 
 * Class encapsulating the process for rounding off a column/literal of 
 * type {@link org.apache.phoenix.schema.PDataType#DECIMAL}
 *
 * 
 * @since 3.0.0
 */

public class RoundDecimalExpression extends ScalarFunction {
    
    private int scale;
    
    /**
     * Creates a {@link RoundDecimalExpression} with rounding scale given by @param scale. 
     *
     */
    public static Expression create(Expression expr, int scale) throws SQLException {
        if (expr.getDataType().isCoercibleTo(PDataType.LONG)) {
            return expr;
        }
        Expression scaleExpr = LiteralExpression.newConstant(scale, PDataType.INTEGER, true);
        List<Expression> expressions = Lists.newArrayList(expr, scaleExpr);
        return new RoundDecimalExpression(expressions);
    }
    
    /**
     * Creates a {@link RoundDecimalExpression} with a default scale of 0 used for rounding. 
     *
     */
    public static Expression create(Expression expr) throws SQLException {
        return create(expr, 0);
    }
    
    public RoundDecimalExpression() {}
    
    public RoundDecimalExpression(List<Expression> children) {
        super(children);
        LiteralExpression scaleChild = (LiteralExpression)children.get(1);
        PDataType scaleType = scaleChild.getDataType();
        Object scaleValue = scaleChild.getValue();
        if(scaleValue != null) {
            if (scaleType.isCoercibleTo(PDataType.INTEGER, scaleValue)) {
                int scale = (Integer)PDataType.INTEGER.toObject(scaleValue, scaleType);
                if (scale >=0 && scale <= PDataType.MAX_PRECISION) {
                    this.scale = scale;
                    return;
                }
            }
            throw new IllegalDataException("Invalid second argument for scale: " + scaleValue + ". The scale must be between 0 and " + PDataType.MAX_PRECISION + " inclusive.");
        } 
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression childExpr = children.get(0);
        if(childExpr.evaluate(tuple, ptr)) {
            BigDecimal value = (BigDecimal)PDataType.DECIMAL.toObject(ptr, childExpr.getSortOrder());
            BigDecimal scaledValue = value.setScale(scale, getRoundingMode());
            ptr.set(getDataType().toBytes(scaledValue));
            return true;
        }
        return false;
    }

    @Override
    public PDataType getDataType() {
        return children.get(0).getDataType();
    }
    
    protected RoundingMode getRoundingMode() {
        return RoundingMode.HALF_UP;
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        scale = WritableUtils.readVInt(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        WritableUtils.writeVInt(output, scale);
    }

    @Override
    public String getName() {
        return RoundFunction.NAME;
    }
    
    @Override
    public OrderPreserving preservesOrder() {
        return OrderPreserving.YES;
    }

}
