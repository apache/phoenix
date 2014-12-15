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
import org.apache.phoenix.expression.function.CeilDateExpression;
import org.apache.phoenix.expression.function.CeilDecimalExpression;
import org.apache.phoenix.expression.function.CeilFunction;
import org.apache.phoenix.expression.function.CeilTimestampExpression;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PUnsignedTimestamp;
import org.apache.phoenix.schema.TypeMismatchException;

/**
 * Parse node corresponding to {@link CeilFunction}. 
 * It also acts as a factory for creating the right kind of
 * ceil expression according to the data type of the 
 * first child.
 *
 * 
 * @since 3.0.0
 */
public class CeilParseNode extends FunctionParseNode {
    
    CeilParseNode(String name, List<ParseNode> children, BuiltInFunctionInfo info) {
        super(name, children, info);
    }
    
    @Override
    public Expression create(List<Expression> children, StatementContext context) throws SQLException {
        return getCeilExpression(children);
    }
    
    public static Expression getCeilExpression(List<Expression> children) throws SQLException {
        final Expression firstChild = children.get(0);
        final PDataType firstChildDataType = firstChild.getDataType();
        if(firstChildDataType.isCoercibleTo(PDate.INSTANCE)) {
            return CeilDateExpression.create(children);
        } else if (firstChildDataType == PTimestamp.INSTANCE || firstChildDataType == PUnsignedTimestamp.INSTANCE) {
            return CeilTimestampExpression.create(children);
        } else if(firstChildDataType.isCoercibleTo(PDecimal.INSTANCE)) {
            return CeilDecimalExpression.create(children);
        } else {
            throw TypeMismatchException.newException(firstChildDataType, "1");
        }
    }
    
    /**
     * When ceiling off decimals, user need not specify the scale. In such cases, 
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
