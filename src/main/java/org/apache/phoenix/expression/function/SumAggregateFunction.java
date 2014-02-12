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
import org.apache.phoenix.expression.aggregator.*;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.parse.*;
import org.apache.phoenix.schema.ColumnModifier;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.tuple.Tuple;


/**
 * 
 * Built-in function for SUM aggregation function.
 *
 * 
 * @since 0.1
 */
@BuiltInFunction(name=SumAggregateFunction.NAME, nodeClass=SumAggregateParseNode.class, args= {@Argument(allowedTypes={PDataType.DECIMAL})} )
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
    
    @Override
    public Aggregator newServerAggregator(Configuration conf) {
        final PDataType type = getAggregatorExpression().getDataType();
        ColumnModifier columnModifier = getAggregatorExpression().getColumnModifier();
        switch( type ) {
            case DECIMAL:
                return new DecimalSumAggregator(columnModifier);
            case UNSIGNED_DOUBLE:
            case UNSIGNED_FLOAT:
            case DOUBLE:
            case FLOAT:
                return new DoubleSumAggregator(columnModifier) {
                    @Override
                    protected PDataType getInputDataType() {
                        return type;
                    }
                };
            default:
                return new NumberSumAggregator(columnModifier) {
                    @Override
                    protected PDataType getInputDataType() {
                        return type;
                    }
                };
        }
    }
    
    @Override
    public Aggregator newClientAggregator() {
        switch( getDataType() ) {
            case DECIMAL:
                // On the client, we'll always aggregate over non modified column values,
                // because we always get them back from the server in their non modified
                // form.
                return new DecimalSumAggregator(null);
            case LONG:
                return new LongSumAggregator(null);
            case DOUBLE:
                return new DoubleSumAggregator(null);
            default:
                throw new IllegalStateException("Unexpected SUM type: " + getDataType());
        }
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (!super.evaluate(tuple, ptr)) {
            return false;
        }
        if (isConstantExpression()) {
            PDataType type = getDataType();
            Object constantValue = ((LiteralExpression)children.get(0)).getValue();
            if (type == PDataType.DECIMAL) {
                BigDecimal value = ((BigDecimal)constantValue).multiply((BigDecimal)PDataType.DECIMAL.toObject(ptr, PDataType.LONG));
                ptr.set(PDataType.DECIMAL.toBytes(value));
            } else {
                long constantLongValue = ((Number)constantValue).longValue();
                long value = constantLongValue * type.getCodec().decodeLong(ptr, null);
                ptr.set(new byte[type.getByteSize()]);
                type.getCodec().encodeLong(value, ptr);
            }
        }
        return true;
    }

    @Override
    public PDataType getDataType() {
        switch(super.getDataType()) {
        case DECIMAL:
            return PDataType.DECIMAL;
        case UNSIGNED_FLOAT:
        case UNSIGNED_DOUBLE:
        case FLOAT:
        case DOUBLE:
            return PDataType.DOUBLE;
        default:
            return PDataType.LONG;
        }
    }

    @Override
    public String getName() {
        return NAME;
    }
}
