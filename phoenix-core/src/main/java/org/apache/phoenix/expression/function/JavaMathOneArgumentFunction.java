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

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDouble;

public abstract class JavaMathOneArgumentFunction extends ScalarFunction {

    public JavaMathOneArgumentFunction() {
    }

    public JavaMathOneArgumentFunction(List<Expression> children) throws SQLException {
        super(children);
    }

    protected abstract double compute(double firstArg);

    static double getArg(Expression exp, ImmutableBytesWritable ptr) {
        if (exp.getDataType() == PDecimal.INSTANCE) {
            return ((BigDecimal) exp.getDataType().toObject(ptr, exp.getSortOrder())).doubleValue();
        } else {
            return exp.getDataType().getCodec().decodeDouble(ptr, exp.getSortOrder());
        }
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        PDataType returnType = getDataType();

        Expression arg1Expr = children.get(0);
        if (!arg1Expr.evaluate(tuple, ptr)) return false;
        if (ptr.getLength() == 0) return true;
        double arg1 = getArg(arg1Expr, ptr);

        ptr.set(new byte[returnType.getByteSize()]);
        returnType.getCodec().encodeDouble(compute(arg1), ptr);
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PDouble.INSTANCE;
    }
}
