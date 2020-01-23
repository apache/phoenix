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
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDate;

import java.util.Date;
import java.util.List;

/**
 * Function to return the timestamp of the empty column which functions as the row timestamp. The
 * result returned can be used for debugging(eg. using HBase shell), logging etc.
 */
@BuiltInFunction(name = RowTimestampStringFunction.NAME, args = {})
public class RowTimestampStringFunction extends ScalarFunction {

    public static final String NAME = "ROW_TIMESTAMP_STRING";

    public RowTimestampStringFunction() {
    }

    public RowTimestampStringFunction(List<Expression> children) { super(children); }

    @Override
    public String getName() { return NAME; }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        assert (tuple.size() >= 1);
        Date rowTimestamp = new Date(tuple.getValue(0).getTimestamp());
        ptr.set(PDate.INSTANCE.toBytes(rowTimestamp));
        return true;
    }

    @Override
    public PDataType getDataType() { return PDate.INSTANCE; }

    @Override
    public boolean isStateless() { return false; }
}
