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
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.util.ByteUtil;

public abstract class JavaMathTwoArgumentFunction extends ScalarFunction {

    public JavaMathTwoArgumentFunction() {
    }

    public JavaMathTwoArgumentFunction(List<Expression> children) throws SQLException {
        super(children);
    }

    protected abstract double compute(double firstArg, double secondArg);

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        PDataType returnType = getDataType();

        Expression arg1Expr = children.get(0);
        if (!arg1Expr.evaluate(tuple, ptr)) return false;
        if (ptr.getLength() == 0) return true;
        double arg1 = JavaMathOneArgumentFunction.getArg(arg1Expr, ptr);

        Expression arg2Expr = (children.size() <= 1) ? null : children.get(1);
        double arg2;
        if (arg2Expr != null && !arg2Expr.evaluate(tuple, ptr)) return false;
        if (arg2Expr == null || ptr.getLength() == 0) {
            ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
            return true;
        } else {
            arg2 = JavaMathOneArgumentFunction.getArg(arg2Expr, ptr);
        }

        ptr.set(new byte[returnType.getByteSize()]);
        returnType.getCodec().encodeDouble(compute(arg1, arg2), ptr);
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PDouble.INSTANCE;
    }
}
