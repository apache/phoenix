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
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.FirstLastValueBaseClientAggregator;
import org.apache.phoenix.expression.aggregator.FirstLastValueServerAggregator;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.parse.LastValuesAggregateParseNode;
import org.apache.phoenix.schema.types.PArrayDataType;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;

/**
 * Built-in function for FIRST_VALUES(<expression>, <expression>) WITHIN GROUP (ORDER BY <expression> ASC/DESC) aggregate
 * function
 *
 */
@FunctionParseNode.BuiltInFunction(name = LastValuesFunction.NAME, nodeClass = LastValuesAggregateParseNode.class, args = {
    @FunctionParseNode.Argument(),
    @FunctionParseNode.Argument(allowedTypes = { PBoolean.class }, isConstant = true),
    @FunctionParseNode.Argument(),
    @FunctionParseNode.Argument(allowedTypes = { PInteger.class }, isConstant = true)})
public class LastValuesFunction extends FirstLastValueBaseFunction {
    public static final String NAME = "LAST_VALUES";
    private int offset;

    public LastValuesFunction() {
    }

    public LastValuesFunction(List<Expression> childExpressions) {
        this(childExpressions, null);
    }

    public LastValuesFunction(List<Expression> childExpressions, CountAggregateFunction delegate) {
        super(childExpressions, delegate);
    }

    @Override
    public Aggregator newServerAggregator(Configuration conf) {
        FirstLastValueServerAggregator aggregator = new FirstLastValueServerAggregator();

        offset = ((Number) ((LiteralExpression) children.get(3)).getValue()).intValue();
        boolean order = !(Boolean) ((LiteralExpression) children.get(1)).getValue();

        aggregator.init(children, order, offset);

        return aggregator;
    }

    @Override
    public Aggregator newClientAggregator() {
        FirstLastValueBaseClientAggregator aggregator = new FirstLastValueBaseClientAggregator(getDataType());

        if (children.size() < 3) {
            aggregator.init(offset, true);
        } else {
            aggregator.init(((Number) ((LiteralExpression) children.get(3)).getValue()).intValue(), true);
        }

        return aggregator;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public PDataType getDataType() {
        if (children.size() < 3) {
            return null;
        }
        return PDataType.fromTypeId(children.get(2).getDataType().getSqlType() + PArrayDataType.ARRAY_TYPE_BASE);
    }
}
