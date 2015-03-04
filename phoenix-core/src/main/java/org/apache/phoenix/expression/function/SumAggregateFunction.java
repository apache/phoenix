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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.DecimalSumAggregator;
import org.apache.phoenix.expression.aggregator.DoubleSumAggregator;
import org.apache.phoenix.expression.aggregator.NumberSumAggregator;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.parse.SumAggregateParseNode;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PFloat;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PUnsignedDouble;
import org.apache.phoenix.schema.types.PUnsignedFloat;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.tuple.Tuple;


/**
 * 
 * Built-in function for SUM aggregation function.
 *
 * 
 * @since 0.1
 */
@BuiltInFunction(name=SumAggregateFunction.NAME, nodeClass=SumAggregateParseNode.class, args= {@Argument(allowedTypes={PDecimal.class})} )
public class SumAggregateFunction extends DelegateConstantToCountAggregateFunction {
    public static final String NAME = "SUM";
    
    public SumAggregateFunction() {
    }
    
    // TODO: remove when not required at built-in func register time
    public SumAggregateFunction(List<Expression> childExpressions){
        super(childExpressions, null);
    }
    
    public SumAggregateFunction(List<Expression> childExpressions, CountAggregateFunction delegate){
        super(childExpressions, delegate);
    }
    
    private Aggregator newAggregator(final PDataType type, SortOrder sortOrder, ImmutableBytesWritable ptr) {
        if (type == PDecimal.INSTANCE) {
          return new DecimalSumAggregator(sortOrder, ptr);
        } else if (PDataType.equalsAny(type, PUnsignedDouble.INSTANCE, PUnsignedFloat.INSTANCE, PDouble.INSTANCE, PFloat.INSTANCE)) {
          return new DoubleSumAggregator(sortOrder, ptr) {
            @Override
            protected PDataType getInputDataType() {
              return type;
            }
          };
        } else {
          return new NumberSumAggregator(sortOrder, ptr) {
            @Override
            protected PDataType getInputDataType() {
              return type;
            }
          };
        }
    }

    @Override
    public Aggregator newClientAggregator() {
        return newAggregator(getDataType(), SortOrder.getDefault(), null);
    }
    
    @Override
    public Aggregator newServerAggregator(Configuration conf) {
        Expression child = getAggregatorExpression();
        return newAggregator(child.getDataType(), child.getSortOrder(), null);
    }
    
    @Override
    public Aggregator newServerAggregator(Configuration conf, ImmutableBytesWritable ptr) {
        Expression child = getAggregatorExpression();
        return newAggregator(child.getDataType(), child.getSortOrder(), ptr);
    }
    
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (!super.evaluate(tuple, ptr)) {
            return false;
        }
        if (isConstantExpression()) {
            PDataType type = getDataType();
            Object constantValue = ((LiteralExpression)children.get(0)).getValue();
            if (type == PDecimal.INSTANCE) {
                BigDecimal value = ((BigDecimal)constantValue).multiply((BigDecimal) PDecimal.INSTANCE.toObject(ptr, PLong.INSTANCE));
                ptr.set(PDecimal.INSTANCE.toBytes(value));
            } else {
                long constantLongValue = ((Number)constantValue).longValue();
                long value = constantLongValue * type.getCodec().decodeLong(ptr, SortOrder.getDefault());
                byte[] resultPtr = new byte[type.getByteSize()];
                type.getCodec().encodeLong(value, resultPtr, 0);
                ptr.set(resultPtr);
            }
        }
        return true;
    }

    @Override
    public PDataType getDataType() {
        if (super.getDataType() == PDecimal.INSTANCE) {
          return PDecimal.INSTANCE;
        } else if (PDataType.equalsAny(super.getDataType(), PUnsignedFloat.INSTANCE, PUnsignedDouble.INSTANCE,
            PFloat.INSTANCE, PDouble.INSTANCE)) {
          return PDouble.INSTANCE;
        } else {
          return PLong.INSTANCE;
        }
    }

    @Override
    public String getName() {
        return NAME;
    }
}
