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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.expression.CurrentDateTimeFunction;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.CurrentDateParseNode;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.tuple.Tuple;

import java.sql.SQLException;
import java.util.List;


/**
 * 
 * Function that returns the current date accurate to the millisecond. Note that this
 * function is never evaluated on the server-side, instead the server side date is
 * retrieved (piggy-backed on the call to check that the metadata is up-to-date) and
 * passed into this function at create time.
 *
 * 
 * @since 0.1
 */
@BuiltInFunction(name=CurrentDateFunction.NAME, nodeClass=CurrentDateParseNode.class, args= {} )
public class CurrentDateFunction extends CurrentDateTimeFunction {
    public static final String NAME = "CURRENT_DATE";
    private final ImmutableBytesWritable currentDate = new ImmutableBytesWritable(new byte[PDate.INSTANCE.getByteSize()]);
    
    public CurrentDateFunction() {
        this(System.currentTimeMillis());
    }

    public CurrentDateFunction(List<Expression> children, StatementContext context) throws SQLException {
        this(context.getCurrentTime());
    }

    public CurrentDateFunction(long timeStamp) {
        getDataType().getCodec().encodeLong(timeStamp, currentDate);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        ptr.set(currentDate.get(), 0, PDate.INSTANCE.getByteSize());
        return true;
    }

    @Override
    public final PDataType getDataType() {
        return PDate.INSTANCE;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
