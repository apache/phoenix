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
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PNumericType;

@BuiltInFunction(name = AbsFunction.NAME, args = { @Argument(allowedTypes = PDecimal.class) })
public class AbsFunction extends ScalarFunction {

    public static final String NAME = "ABS";

    public AbsFunction() {
    }

    public AbsFunction(List<Expression> children) {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression childExpr = children.get(0);
        PDataType dataType = childExpr.getDataType();
        if (childExpr.evaluate(tuple, ptr)) {
            byte[] bytes = ptr.get();
            int offset = ptr.getOffset(), length = ptr.getLength();
            ptr.set(new byte[getDataType().getByteSize()]);
            ((PNumericType) dataType).abs(bytes, offset, length, childExpr.getSortOrder(), ptr);
            return true;
        }
        return false;
    }

    @Override
    public PDataType getDataType() {
        return children.get(0).getDataType();
    }

    @Override
    public String getName() {
        return AbsFunction.NAME;
    }
}
