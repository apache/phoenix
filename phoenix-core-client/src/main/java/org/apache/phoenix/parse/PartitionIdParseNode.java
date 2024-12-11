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

import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.expression.function.FunctionExpression;
import org.apache.phoenix.expression.function.PartitionIdFunction;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.RowKeyValueAccessor;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class PartitionIdParseNode extends FunctionParseNode {

    PartitionIdParseNode(String name, List<ParseNode> children,
                                 BuiltInFunctionInfo info) {
        super(name, children, info);
    }

    @Override
    public FunctionExpression create(List<Expression> children, StatementContext context)
            throws SQLException {
        // It does not take any parameters.
        if (children.size() != 0) {
            throw new IllegalArgumentException(
                    "PartitionIdFunction does not take any parameters"
            );
        }
        PTable table = context.getCurrentTable().getTable();
        if (table.getViewIndexId()!= null && table.isMultiTenant()) {
            return new PartitionIdFunction(getExpressions(table, 2));
        } else if (table.getViewIndexId()!= null || table.isMultiTenant()) {
            return new PartitionIdFunction(getExpressions(table, 1));
        } else {
            return new PartitionIdFunction(getExpressions(table, 0));
        }
    }

    private static List<Expression> getExpressions(PTable table, int position) {
        List<Expression> expressionList = new ArrayList<>(1);
        expressionList.add(new RowKeyColumnExpression(table.getPKColumns().get(position),
                new RowKeyValueAccessor(table.getPKColumns(), position)));
        return expressionList;
    }
}
