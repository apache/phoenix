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
import org.apache.phoenix.expression.aggregator.DecimalStddevSampAggregator;
import org.apache.phoenix.expression.aggregator.DistinctValueWithCountClientAggregator;
import org.apache.phoenix.expression.aggregator.DistinctValueWithCountServerAggregator;
import org.apache.phoenix.expression.aggregator.StddevSampAggregator;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDataType;

/**
 * 
 * Built-in function for STDDEV_SAMP(<expression>) aggregate function
 * 
 * 
 * @since 1.2.1
 */
@BuiltInFunction(name = StddevSampFunction.NAME, args = { @Argument(allowedTypes={PDecimal.class})})
public class StddevSampFunction extends DistinctValueWithCountAggregateFunction {
    public static final String NAME = "STDDEV_SAMP";

    public StddevSampFunction() {

    }

    public StddevSampFunction(List<Expression> childern) {
        super(childern);
    }

    @Override
    public Aggregator newServerAggregator(Configuration conf) {
        return new DistinctValueWithCountServerAggregator(conf);
    }

    @Override
    public DistinctValueWithCountClientAggregator newClientAggregator() {
        if (children.get(0).getDataType() == PDecimal.INSTANCE) {
            // Special Aggregators for DECIMAL datatype for more precision than double
            return new DecimalStddevSampAggregator(children, getAggregatorExpression().getSortOrder());
        }
        return new StddevSampAggregator(children, getAggregatorExpression().getSortOrder());
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
