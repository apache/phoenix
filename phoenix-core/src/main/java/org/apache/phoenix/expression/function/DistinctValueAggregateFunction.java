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
import org.apache.phoenix.expression.aggregator.DistinctValueClientAggregator;
import org.apache.phoenix.expression.aggregator.DistinctValueWithCountClientAggregator;
import org.apache.phoenix.expression.aggregator.DistinctValueWithCountServerAggregator;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarbinary;

@BuiltInFunction(name=DistinctValueAggregateFunction.NAME, args= {@Argument()} )
public class DistinctValueAggregateFunction extends DistinctValueWithCountAggregateFunction {
    public static final String NAME = "COLLECTDISTINCT";
    
    public DistinctValueAggregateFunction() {
    }
    
    public DistinctValueAggregateFunction(List<Expression> children) {
        super(children);
    }

    @Override
    public Aggregator newServerAggregator(Configuration conf) {
        return new DistinctValueWithCountServerAggregator(conf);
    }

    @Override
    public DistinctValueWithCountClientAggregator newClientAggregator() {
        PDataType baseType = getAggregatorExpression().getDataType().isArrayType() ?
            PVarbinary.INSTANCE : getAggregatorExpression().getDataType();
        PDataType resultType = PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE);
        return new DistinctValueClientAggregator(getAggregatorExpression().getSortOrder(), baseType, resultType);
    }
    
    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public PDataType getDataType() {
        PDataType baseType = getAggregatorExpression().getDataType().isArrayType() ? PVarbinary.INSTANCE : getAggregatorExpression().getDataType();
        return PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE);
    }

}
