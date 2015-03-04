/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.phoenix.compile;

import java.sql.SQLException;

import org.apache.phoenix.expression.ColumnExpression;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.schema.ColumnRef;

/**
 * Used to check if the expression being compiled is a {@link ColumnExpression}
 */
public class IndexExpressionCompiler extends ExpressionCompiler {

    //
    private ColumnRef columnRef;

    public IndexExpressionCompiler(StatementContext context) {
        super(context);
        this.columnRef = null;
    }

    @Override
    public void reset() {
        super.reset();
        this.columnRef = null;
    }

    @Override
    protected ColumnRef resolveColumn(ColumnParseNode node) throws SQLException {
        ColumnRef columnRef = super.resolveColumn(node);
        if (isTopLevel()) {
            this.columnRef = columnRef;
        }
        return columnRef;
    }

    /**
     * @return if the expression being compiled is a regular column the column ref, else is null
     */
    public ColumnRef getColumnRef() {
        return columnRef;
    }

}
