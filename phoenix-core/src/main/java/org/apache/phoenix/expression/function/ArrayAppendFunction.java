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
import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.schema.types.*;

@FunctionParseNode.BuiltInFunction(name = ArrayAppendFunction.NAME, args = {
        @FunctionParseNode.Argument(allowedTypes = {PBinaryArray.class, PVarbinaryArray.class}),
        @FunctionParseNode.Argument(allowedTypes = {PVarbinary.class}, defaultValue = "null")})
public class ArrayAppendFunction extends ArrayModifierFunction {

    public static final String NAME = "ARRAY_APPEND";

    public ArrayAppendFunction() {
    }

    public ArrayAppendFunction(List<Expression> children) throws TypeMismatchException {
        super(children);
    }

    @Override
    protected boolean modifierFunction(ImmutableBytesWritable ptr, int len, int offset, byte[] arrayBytes, PDataType baseDataType, int arrayLength, Integer maxLength, Expression arrayExp) {
        return PArrayDataType.appendItemToArray(ptr, len, offset, arrayBytes, baseDataType, arrayLength, getMaxLength(), arrayExp.getSortOrder());
    }

    @Override
    public String getName() {
        return NAME;
    }
}
