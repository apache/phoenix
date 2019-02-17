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
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.parse.UnnestArrayParseNode;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarbinaryArray;

@FunctionParseNode.BuiltInFunction(name = UnnestArrayFunction.NAME, nodeClass=UnnestArrayParseNode.class,
    args = { @FunctionParseNode.Argument(allowedTypes = { PVarbinaryArray.class})})
public class UnnestArrayFunction extends FunctionExpression {
    public static final String NAME = "UNNEST";

    public UnnestArrayFunction(){}

    public UnnestArrayFunction(List<Expression> children){
        super(children);
    }

    @Override public String getName() {
        return NAME;
    }

    //Should never be called
    @Override public <T> T accept(ExpressionVisitor<T> visitor) {
        return null;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr){
        //Function should never be used
        return false;
    }

    @Override public PDataType getDataType() {
        return PDataType.fromTypeId(getChildren().get(0).getDataType().getSqlType() - PDataType.ARRAY_TYPE_BASE);
    }
}
