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
import org.apache.phoenix.expression.aggregator.MaxAggregator;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.parse.MaxAggregateParseNode;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;



/**
 * Built-in function for finding MAX.
 * 
 * 
 * @since 0.1
 */
@BuiltInFunction(name=MaxAggregateFunction.NAME, nodeClass=MaxAggregateParseNode.class, args= {@Argument()} )
public class MaxAggregateFunction extends MinAggregateFunction {
    public static final String NAME = "MAX";

    public MaxAggregateFunction() {
    }
    
    public MaxAggregateFunction(List<Expression> childExpressions, CountAggregateFunction delegate) {
        super(childExpressions, delegate);
    }

    @Override 
    public Aggregator newServerAggregator(Configuration conf) {
        Expression child = getAggregatorExpression();
        final PDataType type = child.getDataType();
        final Integer maxLength = child.getMaxLength();
        return new MaxAggregator(child.getSortOrder()) {
            @Override
            public PDataType getDataType() {
                return type;
            }

            @Override
            public Integer getMaxLength() {
                return maxLength;
            }
        };
    }
    
    @Override
    public String getName() {
        return NAME;
    }
    
    @Override
    public SortOrder getSortOrder() {
       return getAggregatorExpression().getSortOrder(); 
    }    
}
