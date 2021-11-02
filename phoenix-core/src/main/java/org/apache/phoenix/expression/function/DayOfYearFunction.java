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
import org.apache.phoenix.parse.DateScalarParseNode;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.util.ExpressionContext;

/**
 * Implementation of DayOfYearFunction(Date/Timestamp)
 *
 * Returns an integer from 1 to 365 (for each day of the week). Returns 366 in a leap year.
 *
 */
@BuiltInFunction(name=DayOfYearFunction.NAME, nodeClass=DateScalarParseNode.class,
        args={@Argument(allowedTypes={PTimestamp.class})})
public class DayOfYearFunction extends TemporalScalarFunction {
    public static final String NAME = "DAYOFYEAR";

    public DayOfYearFunction() {
    }

    public DayOfYearFunction(List<Expression> children, ExpressionContext context) throws SQLException {
        super(children, context);
    }

    @Override
    public DayOfYearFunction clone(List<Expression> children) {
        try {
            return new DayOfYearFunction(children, getContext());
        } catch (Exception e) {
            throw new RuntimeException(e); // Impossible, since it was originally constructed this way
        }
    }
    
    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression arg = getChildren().get(0);
        if (!arg.evaluate(tuple,ptr)) {
            return false;
        }
        if (ptr.getLength() == 0) {
            return true;
        }
        long dateTime = inputCodec.decodeLong(ptr, arg.getSortOrder());
        int day = getContext().dayOfYearImplementation(dateTime);
        PDataType returnDataType = getDataType();
        byte[] byteValue = new byte[returnDataType.getByteSize()];
        returnDataType.getCodec().encodeInt(day, byteValue, 0);
        ptr.set(byteValue);
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PInteger.INSTANCE;
    }
}
