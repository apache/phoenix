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

import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FloorParseNode;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PVarchar;

/**
 * 
 * Function used to bucketize date/time values by truncating them to
 * an even increment.  Usage:
 * TRUNC(<date/time col ref>,<'day'|'hour'|'minute'|'second'|'millisecond'>,[<optional integer multiplier>])
 * The integer multiplier is optional and is used to do rollups to a partial time unit (i.e. 10 minute rollup)
 * The function returns a {@link org.apache.phoenix.schema.types.PDate}
 *
 * 
 * @since 0.1
 */
@BuiltInFunction(name = TruncFunction.NAME,
nodeClass = FloorParseNode.class,
args = {
       @Argument(allowedTypes={PTimestamp.class, PDecimal.class}),
       @Argument(allowedTypes={PVarchar.class, PInteger.class}, defaultValue = "null", isConstant=true),
       @Argument(allowedTypes={PInteger.class}, defaultValue="1", isConstant=true)
       } 
)
public abstract class TruncFunction extends ScalarFunction {
    
    public static final String NAME = "TRUNC";
    
    public TruncFunction(List<Expression> children) throws SQLException {
        super(children);
    }

    @Override
    public String getName() {
        return NAME;
    }
    
    
}
