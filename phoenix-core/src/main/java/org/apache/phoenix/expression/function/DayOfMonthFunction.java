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
 * 
 * Implementation of the DayOfMonth() buildin. Input Date/Timestamp.
 * An integer from 1 to 31 representing the day of the month in date
 * 
 */
@BuiltInFunction(name=DayOfMonthFunction.NAME, nodeClass=DateScalarParseNode.class,
args={@Argument(allowedTypes={PTimestamp.class})})
public class DayOfMonthFunction extends TemporalScalarFunction {
    public static final String NAME = "DAYOFMONTH";

    public DayOfMonthFunction() {
    }

    public DayOfMonthFunction(List<Expression> children, ExpressionContext context) throws SQLException {
        super(children, context);
    }
    
    @Override
    public DayOfMonthFunction clone(List<Expression> children) {
        try {
            return new DayOfMonthFunction(children, getContext());
        } catch (Exception e) {
            throw new RuntimeException(e); // Impossible, since it was originally constructed this way
        }
    }
    
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression expression = getChildExpression();
        if (!expression.evaluate(tuple, ptr)) {
            return false;
        }
        if ( ptr.getLength() == 0) {
            return true; //means null
        }
        long dateTime = inputCodec.decodeLong(ptr, expression.getSortOrder());
        int day = getContext().dayOfMonthImplementation(dateTime);
        PDataType returnType = getDataType();
        byte[] byteValue = new byte[returnType.getByteSize()];
        returnType.getCodec().encodeInt(day, byteValue, 0);
        ptr.set(byteValue);
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PInteger.INSTANCE;
    }

    @Override
    public String getName() {
        return NAME;
    }

    private Expression getChildExpression() {
        return children.get(0);
    }
}
