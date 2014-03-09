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
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.tuple.Tuple;


/**
 * 
 * Function used to provide an alternative value when the first argument is null.
 * Usage:
 * COALESCE(expr1,expr2)
 * If expr1 is not null, then it is returned, otherwise expr2 is returned. 
 *
 * TODO: better bind parameter type matching, since arg2 must be coercible
 * to arg1. consider allowing a common base type?
 * 
 * @since 0.1
 */
@BuiltInFunction(name=CoalesceFunction.NAME, args= {
    @Argument(),
    @Argument()} )
public class CoalesceFunction extends ScalarFunction {
    public static final String NAME = "COALESCE";

    public CoalesceFunction() {
    }

    public CoalesceFunction(List<Expression> children) throws SQLException {
        super(children);
        if (!children.get(1).getDataType().isCoercibleTo(children.get(0).getDataType())) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_CONVERT_TYPE)
                .setMessage(getName() + " expected " + children.get(0).getDataType() + ", but got " + children.get(1).getDataType())
                .build().buildException();
        }
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        boolean evaluated = children.get(0).evaluate(tuple, ptr);
        if (evaluated) {
            return true;
        }
        if (tuple.isImmutable()) {
            return children.get(1).evaluate(tuple, ptr);
        }
        return false;
    }

    @Override
    public PDataType getDataType() {
        return children.get(0).getDataType();
    }

    @Override
    public Integer getMaxLength() {
        Integer maxLength1 = children.get(0).getMaxLength();
        if (maxLength1 != null) {
            Integer maxLength2 = children.get(1).getMaxLength();
            if (maxLength2 != null) {
                return maxLength1 > maxLength2 ? maxLength1 : maxLength2;
            }
        }
        return null;
    }

    @Override
    public boolean isNullable() {
        return children.get(0).isNullable() && children.get(1).isNullable();
    }

    @Override
    public String getName() {
        return NAME;
    }
    
    @Override
    public boolean requiresFinalEvaluation() {
        return true;
    }
}
