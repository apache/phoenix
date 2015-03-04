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

import java.math.BigDecimal;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.parse.AvgAggregateParseNode;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;


@BuiltInFunction(name=AvgAggregateFunction.NAME, nodeClass=AvgAggregateParseNode.class, args= {@Argument(allowedTypes={PDecimal.class})} )
public class AvgAggregateFunction extends CompositeAggregateFunction {
    public static final String NAME = "AVG";
    private final CountAggregateFunction countFunc;
    private final SumAggregateFunction sumFunc;
    private Integer scale;

    // TODO: remove when not required at built-in func register time
    public AvgAggregateFunction(List<Expression> children) {
        super(children);
        this.countFunc = null;
        this.sumFunc = null;
        setScale(children);
    }

    public AvgAggregateFunction(List<Expression> children, CountAggregateFunction countFunc, SumAggregateFunction sumFunc) {
        super(children);
        this.countFunc = countFunc;
        this.sumFunc = sumFunc;
        setScale(children);
    }

    private void setScale(List<Expression> children) {
        scale = PDataType.MIN_DECIMAL_AVG_SCALE; // At least 4;
        for (Expression child: children) {
            if (child.getScale() != null) {
                scale = Math.max(scale, child.getScale());
            }
        }
    }

    @Override
    public PDataType getDataType() {
        return PDecimal.INSTANCE;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (!countFunc.evaluate(tuple, ptr)) {
            return false;
        }
        long count = countFunc.getDataType().getCodec().decodeLong(ptr, SortOrder.getDefault());
        if (count == 0) {
            return false;
        }
        
        // Normal case where a column reference was used as the argument to AVG
        if (!countFunc.isConstantExpression()) {
            sumFunc.evaluate(tuple, ptr);
            BigDecimal sum = (BigDecimal) PDecimal.INSTANCE.toObject(ptr, sumFunc.getDataType());
            // For the final column projection, we divide the sum by the count, both coerced to BigDecimal.
            // TODO: base the precision on column metadata instead of constant
            BigDecimal avg = sum.divide(BigDecimal.valueOf(count), PDataType.DEFAULT_MATH_CONTEXT);
            avg = avg.setScale(scale, BigDecimal.ROUND_DOWN);
            ptr.set(PDecimal.INSTANCE.toBytes(avg));
            return true;
        }
        BigDecimal value = (BigDecimal) ((LiteralExpression)countFunc.getChildren().get(0)).getValue();
        value = value.setScale(scale, BigDecimal.ROUND_DOWN);
        ptr.set(PDecimal.INSTANCE.toBytes(value));
        return true;
    }

    @Override
    public boolean isNullable() {
        return sumFunc != null && sumFunc.isNullable();
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Integer getScale() {
        return scale;
    }
}
