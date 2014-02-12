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
package org.apache.phoenix.parse;

import java.sql.SQLException;
import java.util.List;

import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.*;


public class AvgAggregateParseNode extends AggregateFunctionParseNode {

    public AvgAggregateParseNode(String name, List<ParseNode> children, BuiltInFunctionInfo info) {
        super(name, children, info);
    }

    @Override
    public FunctionExpression create(List<Expression> children, StatementContext context) throws SQLException {
        SumAggregateFunction sumFunc;
        CountAggregateFunction countFunc = (CountAggregateFunction)context.getExpressionManager().addIfAbsent(new CountAggregateFunction(children));
        if (!countFunc.isConstantExpression()) {
            sumFunc = (SumAggregateFunction)context.getExpressionManager().addIfAbsent(new SumAggregateFunction(countFunc.getChildren(),null));
        } else {
            sumFunc = null;
        }

        return new AvgAggregateFunction(children, countFunc, sumFunc);
    }
}
