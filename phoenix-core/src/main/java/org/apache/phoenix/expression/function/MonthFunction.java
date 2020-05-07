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
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PTimestamp;

/**
 * 
 * Implementation of the Month() buildin. Input Date/Timestamp/Time.
 * Returns an integer from 1 to 12 representing the month component of date
 * 
 */
@BuiltInFunction(name=MonthFunction.NAME, 
args={@Argument(allowedTypes={PTimestamp.class})})
public class MonthFunction extends DateScalarFunction {
    public static final String NAME = "MONTH";

    public MonthFunction() {
    }

    public MonthFunction(List<Expression> children) throws SQLException {
        super(children);
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
        Date dt = new Date(dateTime);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(dt);
        int month = calendar.get(Calendar.MONTH) + 1;
        PDataType returnType = getDataType();
        byte[] byteValue = new byte[returnType.getByteSize()];
        returnType.getCodec().encodeInt(month, byteValue, 0);
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
