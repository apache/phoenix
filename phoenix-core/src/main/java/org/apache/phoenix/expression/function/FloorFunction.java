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
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PVarchar;

/**
 * 
 * Base class for built-in FLOOR function.
 *
 */
@BuiltInFunction(name = FloorFunction.NAME,
                 nodeClass = FloorParseNode.class,
                 args = {
                        @Argument(allowedTypes={PTimestamp.class, PDecimal.class}),
                        @Argument(allowedTypes={PVarchar.class, PInteger.class}, defaultValue = "null", isConstant=true),
                        @Argument(allowedTypes={PInteger.class}, defaultValue="1", isConstant=true)
                        },
                 classType = FunctionParseNode.FunctionClassType.PARENT,
                 derivedFunctions = {FloorDateExpression.class, FloorDecimalExpression.class}
                )
public abstract class FloorFunction extends ScalarFunction {
    
    public static final String NAME = "FLOOR";
    
    public FloorFunction() {}
    
    public FloorFunction(List<Expression> children) {
        super(children);
    }

    public static Expression create(List<Expression> children) throws SQLException {
        final Expression firstChild = children.get(0);
        final PDataType firstChildDataType = firstChild.getDataType();

        //FLOOR on timestamp doesn't really care about the nanos part i.e. it just sets it to zero.
        //Which is exactly what FloorDateExpression does too.
        if(firstChildDataType.isCoercibleTo(PTimestamp.INSTANCE)) {
            return FloorDateExpression.create(children);
        } else if(firstChildDataType.isCoercibleTo(PDecimal.INSTANCE)) {
            return FloorDecimalExpression.create(children);
        } else {
            throw TypeMismatchException.newException(firstChildDataType, "1");
        }
    }
    
    @Override
    public String getName() {
        return NAME;
    }
}
