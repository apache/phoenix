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

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.FirstLastValueBaseClientAggregator;
import org.apache.phoenix.expression.aggregator.FirstLastValueServerAggregator;
import org.apache.phoenix.parse.NthValueAggregateParseNode;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PInteger;
/**
 * Built-in function for POWER(<expression>, <expression>) 
 * 
 * @since 3.0.0
 *
 */
@FunctionParseNode.BuiltInFunction(name=PowerFunction.NAME,  args={
        @FunctionParseNode.Argument(allowedTypes={PVarchar.class})} )

public class PowerFunction extends ScalarFunction {
    public static final String NAME = "POWER";

    public PowerFunction() {
    }

    public PowerFunction(List<Expression> children) throws SQLException {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {    
        if(!getStrExpression().evaluate(tuple,ptr)){
            return false;
        }

        return true;
    }
	@Override
    public PDataType getDataType() {
        return getStrExpression().getDataType();
    }
    @Override
    public String getName() {
        return NAME;
    }
    private Expression getStrExpression() {
        return children.get(0);
    }
}
