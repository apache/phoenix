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

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.CountAggregator;
import org.apache.phoenix.expression.aggregator.LongSumAggregator;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.SchemaUtil;


/**
 * 
 * Built-in function for COUNT(<expression>) aggregate function,
 * for example COUNT(foo), COUNT(1), COUNT(*)
 *
 * 
 * @since 0.1
 */
@BuiltInFunction(name=CountAggregateFunction.NAME, args= {@Argument()} )
public class CountAggregateFunction extends SingleAggregateFunction {
    public static final String NAME = "COUNT";
    public static final List<Expression> STAR = Arrays.<Expression>asList(LiteralExpression.newConstant(1, Determinism.ALWAYS));
    public static final String NORMALIZED_NAME = SchemaUtil.normalizeIdentifier(NAME);
    
    public CountAggregateFunction() {
    }
    
    public CountAggregateFunction(List<Expression> childExpressions) {
        super(childExpressions);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        CountAggregateFunction other = (CountAggregateFunction)obj;
        return (isConstantExpression() && other.isConstantExpression()) || children.equals(other.getChildren());
    }

    @Override
    public int hashCode() {
        return isConstantExpression() ? 0 : super.hashCode();
    }

    /**
     * The COUNT function never returns null
     */
    @Override
    public boolean isNullable() {
        return false;
    }
    
    @Override
    public PDataType getDataType() {
        return PLong.INSTANCE;
    }

    @Override 
    public LongSumAggregator newClientAggregator() {
        // Since COUNT can never be null, ensure the aggregator is not nullable.
        // This allows COUNT(*) to return 0 with the initial state of ClientAggregators
        // when no rows are returned. 
        return new LongSumAggregator() {
            @Override
            public boolean isNullable() {
                return false;
            }
        };
    }
    
    @Override 
    public Aggregator newServerAggregator(Configuration conf) {
        return new CountAggregator();
    }
    
    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Aggregator newServerAggregator(Configuration config, ImmutableBytesWritable ptr) {
        LongSumAggregator sumAgg = newClientAggregator();
        sumAgg.aggregate(null, ptr);
        return new CountAggregator(sumAgg);
    }
}
