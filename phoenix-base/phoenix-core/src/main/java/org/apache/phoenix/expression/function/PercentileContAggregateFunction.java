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

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.DistinctValueWithCountClientAggregator;
import org.apache.phoenix.expression.aggregator.DistinctValueWithCountServerAggregator;
import org.apache.phoenix.expression.aggregator.PercentileClientAggregator;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;

/**
 * 
 * Built-in function for PERCENTILE_CONT(<expression>) WITHIN GROUP (ORDER BY <expression> ASC/DESC) aggregate function
 *
 * 
 * @since 1.2.1
 */
@BuiltInFunction(name = PercentileContAggregateFunction.NAME, args = { @Argument(allowedTypes = { PDecimal.class }),
        @Argument(allowedTypes = { PBoolean.class }, isConstant = true),
        @Argument(allowedTypes = { PDecimal.class }, isConstant = true, minValue = "0", maxValue = "1") })
public class PercentileContAggregateFunction extends DistinctValueWithCountAggregateFunction {
    public static final String NAME = "PERCENTILE_CONT";

    public PercentileContAggregateFunction() {
        
    }
    
    public PercentileContAggregateFunction(List<Expression> childern) {
        super(childern);
    }

    @Override
    public Aggregator newServerAggregator(Configuration conf) {
        return new DistinctValueWithCountServerAggregator(conf);
    }

    @Override
    public DistinctValueWithCountClientAggregator newClientAggregator() {
        return new PercentileClientAggregator(children, getAggregatorExpression().getSortOrder());
    }

    @Override
    public String getName() {
        return NAME;
    }
    
    @Override
    public PDataType getDataType() {
        return PDecimal.INSTANCE;
    }
}
