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
package org.apache.phoenix.parse;

import java.sql.SQLException;
import java.util.List;

import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.FloorDateExpression;
import org.apache.phoenix.expression.function.FloorDecimalExpression;
import org.apache.phoenix.expression.function.FloorFunction;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.TypeMismatchException;

/**
 * Parse node corresponding to {@link FloorFunction}. 
 * It also acts as a factory for creating the right kind of
 * floor expression according to the data type of the 
 * first child.
 *
 * 
 * @since 3.0.0
 */
public class FloorParseNode extends FunctionParseNode {

    FloorParseNode(String name, List<ParseNode> children, BuiltInFunctionInfo info) {
        super(name, children, info);
    }

    @Override
    public Expression create(List<Expression> children, StatementContext context) throws SQLException {
        return getFloorExpression(children);
    }

    public static Expression getFloorExpression(List<Expression> children) throws SQLException {
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
    
    /**
     * When rounding off decimals, user need not specify the scale. In such cases, 
     * we need to prevent the function from getting evaluated as null. This is really
     * a hack. A better way would have been if {@link org.apache.phoenix.parse.FunctionParseNode.BuiltInFunctionInfo} provided a 
     * way of associating default values for each permissible data type.
     * Something like: @ Argument(allowedTypes={PDataType.VARCHAR, PDataType.INTEGER}, defaultValues = {"null", "1"} isConstant=true)
     * Till then, this will have to do.
     */
    @Override
    public boolean evalToNullIfParamIsNull(StatementContext context, int index) throws SQLException {
        return index == 0;
    }

}
