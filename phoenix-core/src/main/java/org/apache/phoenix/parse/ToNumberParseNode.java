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
import java.text.Format;
import java.util.List;

import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.function.FunctionArgumentType;
import org.apache.phoenix.expression.function.FunctionExpression;
import org.apache.phoenix.expression.function.ToNumberFunction;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PTimestamp;

public class ToNumberParseNode extends FunctionParseNode {

    ToNumberParseNode(String name, List<ParseNode> children,
            BuiltInFunctionInfo info) {
        super(name, children, info);
    }

    @Override
    public FunctionExpression create(List<Expression> children, StatementContext context) throws SQLException {
        PDataType dataType = children.get(0).getDataType();
        String formatString = (String)((LiteralExpression)children.get(1)).getValue(); // either date or number format string
        Format formatter =  null;
        FunctionArgumentType type;
        
        if (dataType.isCoercibleTo(PTimestamp.INSTANCE)) {
            if (formatString == null) {
                formatString = context.getDateFormat();
                formatter = context.getDateFormatter();
            } else {
                formatter = FunctionArgumentType.TEMPORAL.getFormatter(formatString);
            }
            type = FunctionArgumentType.TEMPORAL;
        }
        else if (dataType.isCoercibleTo(PChar.INSTANCE)) {
            if (formatString != null) {
                formatter = FunctionArgumentType.CHAR.getFormatter(formatString);
            }
            type = FunctionArgumentType.CHAR;
        }
        else {
            throw new SQLException(dataType + " type is unsupported for TO_NUMBER().  Numeric and temporal types are supported.");
        }
        return new ToNumberFunction(children, type, formatString, formatter);
    }
}
