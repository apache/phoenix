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
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.parse.ToTimeParseNode;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PTime;
import org.apache.phoenix.schema.types.PVarchar;

/**
*
* Implementation of the {@code TO_TIME(<string>,[<format-string>,[<timezone-string>]])} built-in function.
* The second argument is optional and defaults to the phoenix.query.dateFormat value
* from the HBase config. If present it must be a constant string. The third argument is either a
* valid (constant) timezone id, or the string "LOCAL". The third argument is also optional, and
* it defaults to GMT.
*
*/
@BuiltInFunction(name=ToTimeFunction.NAME, nodeClass=ToTimeParseNode.class,
       args={@Argument(allowedTypes={PVarchar.class}),
               @Argument(allowedTypes={PVarchar.class},isConstant=true,defaultValue="null"),
               @Argument(allowedTypes={PVarchar.class}, isConstant=true, defaultValue = "null") } )
public class ToTimeFunction extends ToDateFunction {
    public static final String NAME = "TO_TIME";

    public ToTimeFunction() {
    }

    public ToTimeFunction(List<Expression> children) throws SQLException {
        this(children, null, null);
    }

    public ToTimeFunction(List<Expression> children, String dateFormat, String timeZoneId) throws SQLException {
        super(children, dateFormat, timeZoneId);
    }

    @Override
    public PDataType getDataType() {
        return PTime.INSTANCE;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
