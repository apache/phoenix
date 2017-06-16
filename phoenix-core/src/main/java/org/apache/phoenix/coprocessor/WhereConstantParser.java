/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.coprocessor;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.CreateTableCompiler;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.WhereCompiler;
import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.expression.ComparisonExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.SingleCellColumnExpression;
import org.apache.phoenix.expression.visitor.TraverseAllExpressionVisitor;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableRef;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.phoenix.util.PhoenixRuntime.CONNECTIONLESS;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;

public class WhereConstantParser {

    static PTable addConstantsToPColumnsIfNeeded(PTable table) throws SQLException {
        byte[][] comparisonExpressions = getComparisonExpressions(table);
        List<PColumn> result = Lists.newArrayList();
        for (PColumn aColumn : PTableImpl.getColumnsToClone(table)) {
            if (comparisonExpressions[aColumn.getPosition()] != null) {
                result.add(new PColumnImpl(aColumn, comparisonExpressions[aColumn.getPosition()]));
            } else {
                result.add(aColumn);
            }
        }
        return PTableImpl.makePTable(table, result);
    }

    static byte[][] getComparisonExpressions(PTable table) throws SQLException {
        byte[][] viewColumnConstantsToBe = new byte[table.getColumns().size()][];
        if (table.getViewStatement() == null) {
            return viewColumnConstantsToBe;
        }
        SelectStatement select = new SQLParser(table.getViewStatement()).parseQuery();
        ParseNode parseNode = select.getWhere();
        ColumnResolver resolver = FromCompiler.getResolver(new TableRef(table));
        StatementContext context = new StatementContext(new PhoenixStatement(getConnectionlessConnection()), resolver);
        Expression expression = WhereCompiler.compile(context, parseNode);
        CreateTableCompiler.ViewWhereExpressionVisitor visitor =
            new CreateTableCompiler.ViewWhereExpressionVisitor(table, viewColumnConstantsToBe);
        expression.accept(visitor);
        // If view is not updatable, viewColumnConstants should be empty. We will still
        // inherit our parent viewConstants, but we have no additional ones.
        PTable.ViewType viewTypeToBe = visitor.isUpdatable() ? PTable.ViewType.UPDATABLE : PTable.ViewType.READ_ONLY;
        if (viewTypeToBe != PTable.ViewType.UPDATABLE) {
            viewColumnConstantsToBe = null;
        }
        return viewColumnConstantsToBe;
    }

    private static PhoenixConnection getConnectionlessConnection() throws SQLException {
        return DriverManager
            .getConnection(JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + CONNECTIONLESS)
            .unwrap(PhoenixConnection.class);
    }

}
