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
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.*;

@FunctionParseNode.BuiltInFunction(name = ArrayToStringFunction.NAME, args = {
        @FunctionParseNode.Argument(allowedTypes = {PBinaryArray.class, PVarbinaryArray.class}),
        @FunctionParseNode.Argument(allowedTypes = {PVarchar.class, PChar.class}),
        @FunctionParseNode.Argument(allowedTypes = {PVarchar.class, PChar.class}, defaultValue = "null")})
public class ArrayToStringFunction extends ScalarFunction {
    public static final String NAME = "ARRAY_TO_STRING";

    public ArrayToStringFunction() {
    }

    public ArrayToStringFunction(List<Expression> children) {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression delimiterExpr = children.get(1);
        if (!delimiterExpr.evaluate(tuple, ptr)) {
            return false;
        } else if (ptr.getLength() == 0) {
            return true;
        }
        String delimiter = (String) delimiterExpr.getDataType().toObject(ptr, delimiterExpr.getSortOrder(), delimiterExpr.getMaxLength(), delimiterExpr.getScale());

        Expression arrayExpr = children.get(0);
        if (!arrayExpr.evaluate(tuple, ptr)) {
            return false;
        } else if (ptr.getLength() == 0) {
            return true;
        }
        PhoenixArray array = (PhoenixArray) arrayExpr.getDataType().toObject(ptr, arrayExpr.getSortOrder(), arrayExpr.getMaxLength(), arrayExpr.getScale());

        Expression nullExpr = children.get(2);
        String nullString = null;
        if (nullExpr.evaluate(tuple, ptr) && ptr.getLength() != 0) {
            nullString = (String) nullExpr.getDataType().toObject(ptr, nullExpr.getSortOrder(), nullExpr.getMaxLength(), nullExpr.getScale());
        }

        return PArrayDataType.arrayToString(ptr, array, delimiter, nullString, getSortOrder());
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public PDataType getDataType() {
        return PVarchar.INSTANCE;
    }

    @Override
    public SortOrder getSortOrder() {
        return children.get(0).getSortOrder();
    }
}
