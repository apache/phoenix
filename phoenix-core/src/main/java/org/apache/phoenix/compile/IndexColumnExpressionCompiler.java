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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.expression.ColumnExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.AddParseNode;
import org.apache.phoenix.parse.AndParseNode;
import org.apache.phoenix.parse.ArrayAllComparisonNode;
import org.apache.phoenix.parse.ArrayAnyComparisonNode;
import org.apache.phoenix.parse.ArrayElemRefNode;
import org.apache.phoenix.parse.CaseParseNode;
import org.apache.phoenix.parse.CastParseNode;
import org.apache.phoenix.parse.ColumnDef;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.ComparisonParseNode;
import org.apache.phoenix.parse.DivideParseNode;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.parse.ModulusParseNode;
import org.apache.phoenix.parse.MultiplyParseNode;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.NotParseNode;
import org.apache.phoenix.parse.OrParseNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.RowValueConstructorParseNode;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.StringConcatParseNode;
import org.apache.phoenix.parse.SubtractParseNode;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.IndexUtil;

import com.google.common.collect.Maps;

/**
 * Visitor that checks if nodes other than ColumnParseNode are present as an expression in an index. If the expression
 * is present, then the node is not visited but processed as a ColumnParseNode.
 */
public class IndexColumnExpressionCompiler extends ExpressionCompiler {

    private static final ParseNodeFactory FACTORY = new ParseNodeFactory();
    private final Map<String, Expression> expressionMap;

    IndexColumnExpressionCompiler(StatementContext context) {
        this(context, GroupBy.EMPTY_GROUP_BY, false);
    }

    IndexColumnExpressionCompiler(StatementContext context, boolean resolveViewConstants) {
        this(context, GroupBy.EMPTY_GROUP_BY, resolveViewConstants);
    }

    IndexColumnExpressionCompiler(StatementContext context, GroupBy groupBy) {
        this(context, groupBy, false);
    }

    IndexColumnExpressionCompiler(StatementContext context, GroupBy groupBy, boolean resolveViewConstants) {
        super(context, groupBy, resolveViewConstants);
        expressionMap = Maps.newHashMapWithExpectedSize(context.getCurrentTable().getTable().getColumns().size());
        for (PColumn column : context.getCurrentTable().getTable().getColumns()) {
            if (column.getExpressionStr() != null) {
                Expression expression = null;
                try {
                    ParseNode parseNode = SQLParser.parseCondition(column.getExpressionStr());
                    expression = getExpression(parseNode);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                expressionMap.put(column.getExpressionStr(), expression);
            }
        }
    }

    /**
     * Returns the compiled expression
     * 
     * @param node
     *            data table parse node
     */
    private Expression getExpression(ParseNode node) throws SQLException {
        PTable indexTable = this.context.getCurrentTable().getTable();
        NamedTableNode dataTableNode = NamedTableNode.create(null, TableName.create(indexTable.getParentSchemaName()
                .getString(), indexTable.getParentTableName().getString()), Collections.<ColumnDef> emptyList());
        ColumnResolver resolver = FromCompiler.getResolver(dataTableNode, this.context.getConnection());
        StatementContext context = new StatementContext(this.context.getStatement(), resolver, new Scan(),
                new SequenceManager(this.context.getStatement()));
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(context);
        return node.accept(expressionCompiler);
    }

    /**
     * @return true if current table is an index and there is an expression that matches the given node
     */
    private boolean matchesIndexedExpression(ParseNode node) throws SQLException {
        if (context.getCurrentTable().getTable().getType() != PTableType.INDEX) { 
            return false; 
        }
        DataTableNodeRewriter statementRewriter = new DataTableNodeRewriter();
        ParseNode dataTableParseNode = node.accept(statementRewriter);
        Expression dataTableExpression = getExpression(dataTableParseNode);
        // process regular columns as usual
        if (dataTableExpression instanceof ColumnExpression) {
            return false;
        }
        return expressionMap.containsKey(dataTableExpression.toString());
    }

    private Expression convertAndVisitParseNode(ParseNode node) throws SQLException {
        DataTableNodeRewriter statementRewriter = new DataTableNodeRewriter();
        ParseNode dataTableParseNode = node.accept(statementRewriter);
        Expression expression = getExpression(dataTableParseNode);
        ColumnParseNode columnParseNode = new ColumnParseNode(null, IndexUtil.getIndexColumnName(null,
                expression.toString()), null);
        PDataType expressionType = expression.getDataType();
        PDataType indexColType = IndexUtil.getIndexColumnDataType(expression.isNullable(), expression.getDataType());
        // if data type of expression is different from the index column data type, cast the to expression type
        if (indexColType != expressionType) {
            Expression colExpression = super.visit(columnParseNode);
            return super.visitLeave(FACTORY.cast(columnParseNode, expressionType, null, null),
                    Collections.<Expression> singletonList(colExpression));
        }
        return visit(columnParseNode);
    }

    @Override
    public boolean visitEnter(ComparisonParseNode node) throws SQLException {
        // do not visit this node if it matches an expression that is indexed, it will be converted to a ColumnParseNode
        // and processed in visitLeave
        return matchesIndexedExpression(node) ? false : super.visitEnter(node);
    }

    @Override
    public Expression visitLeave(ComparisonParseNode node, List<Expression> children) throws SQLException {
        // if this node matches an expression that is indexed, convert it to a ColumnParseNode and process it
        return matchesIndexedExpression(node) ? convertAndVisitParseNode(node) : super.visitLeave(node, children);
    }

    @Override
    public boolean visitEnter(AndParseNode node) throws SQLException {
        return matchesIndexedExpression(node) ? false : super.visitEnter(node);
    }

    @Override
    public Expression visitLeave(AndParseNode node, List<Expression> children) throws SQLException {
        return matchesIndexedExpression(node) ? convertAndVisitParseNode(node) : super.visitLeave(node, children);
    }

    @Override
    public boolean visitEnter(OrParseNode node) throws SQLException {
        return matchesIndexedExpression(node) ? false : super.visitEnter(node);
    }

    @Override
    public Expression visitLeave(OrParseNode node, List<Expression> children) throws SQLException {
        return matchesIndexedExpression(node) ? convertAndVisitParseNode(node) : super.visitLeave(node, children);
    }

    @Override
    public boolean visitEnter(FunctionParseNode node) throws SQLException {
        return matchesIndexedExpression(node) ? false : super.visitEnter(node);
    }

    @Override
    public Expression visitLeave(FunctionParseNode node, List<Expression> children) throws SQLException {
        return matchesIndexedExpression(node) ? convertAndVisitParseNode(node) : super.visitLeave(node, children);
    }

    @Override
    public boolean visitEnter(CaseParseNode node) throws SQLException {
        return matchesIndexedExpression(node) ? false : super.visitEnter(node);
    }

    @Override
    public Expression visitLeave(CaseParseNode node, List<Expression> children) throws SQLException {
        return matchesIndexedExpression(node) ? convertAndVisitParseNode(node) : super.visitLeave(node, children);
    }

    @Override
    public boolean visitEnter(NotParseNode node) throws SQLException {
        return matchesIndexedExpression(node) ? false : super.visitEnter(node);
    }

    @Override
    public Expression visitLeave(NotParseNode node, List<Expression> children) throws SQLException {
        return matchesIndexedExpression(node) ? convertAndVisitParseNode(node) : super.visitLeave(node, children);
    }

    @Override
    public boolean visitEnter(CastParseNode node) throws SQLException {
        return matchesIndexedExpression(node) ? false : super.visitEnter(node);
    }

    @Override
    public Expression visitLeave(CastParseNode node, List<Expression> children) throws SQLException {
        return matchesIndexedExpression(node) ? convertAndVisitParseNode(node) : super.visitLeave(node, children);
    }

    @Override
    public boolean visitEnter(AddParseNode node) throws SQLException {
        return matchesIndexedExpression(node) ? false : super.visitEnter(node);
    }

    @Override
    public Expression visitLeave(AddParseNode node, List<Expression> children) throws SQLException {
        return matchesIndexedExpression(node) ? convertAndVisitParseNode(node) : super.visitLeave(node, children);
    }

    @Override
    public boolean visitEnter(SubtractParseNode node) throws SQLException {
        return matchesIndexedExpression(node) ? false : super.visitEnter(node);
    }

    @Override
    public Expression visitLeave(SubtractParseNode node, List<Expression> children) throws SQLException {
        return matchesIndexedExpression(node) ? convertAndVisitParseNode(node) : super.visitLeave(node, children);
    }

    @Override
    public boolean visitEnter(MultiplyParseNode node) throws SQLException {
        return matchesIndexedExpression(node) ? false : super.visitEnter(node);
    }

    @Override
    public Expression visitLeave(MultiplyParseNode node, List<Expression> children) throws SQLException {
        return matchesIndexedExpression(node) ? convertAndVisitParseNode(node) : super.visitLeave(node, children);
    }

    @Override
    public boolean visitEnter(DivideParseNode node) throws SQLException {
        return matchesIndexedExpression(node) ? false : super.visitEnter(node);
    }

    @Override
    public Expression visitLeave(DivideParseNode node, List<Expression> children) throws SQLException {
        return matchesIndexedExpression(node) ? convertAndVisitParseNode(node) : super.visitLeave(node, children);
    }

    @Override
    public boolean visitEnter(ModulusParseNode node) throws SQLException {
        return matchesIndexedExpression(node) ? false : super.visitEnter(node);
    }

    @Override
    public Expression visitLeave(ModulusParseNode node, List<Expression> children) throws SQLException {
        return matchesIndexedExpression(node) ? convertAndVisitParseNode(node) : super.visitLeave(node, children);
    }

    @Override
    public boolean visitEnter(ArrayAnyComparisonNode node) throws SQLException {
        return matchesIndexedExpression(node) ? false : super.visitEnter(node);
    }

    @Override
    public Expression visitLeave(ArrayAnyComparisonNode node, List<Expression> children) throws SQLException {
        return matchesIndexedExpression(node) ? convertAndVisitParseNode(node) : super.visitLeave(node, children);
    }

    @Override
    public boolean visitEnter(ArrayAllComparisonNode node) throws SQLException {
        return matchesIndexedExpression(node) ? false : super.visitEnter(node);
    }

    @Override
    public Expression visitLeave(ArrayElemRefNode node, List<Expression> children) throws SQLException {
        return matchesIndexedExpression(node) ? convertAndVisitParseNode(node) : super.visitLeave(node, children);
    }

    @Override
    public boolean visitEnter(ArrayElemRefNode node) throws SQLException {
        return matchesIndexedExpression(node) ? false : super.visitEnter(node);
    }

    @Override
    public Expression visitLeave(ArrayAllComparisonNode node, List<Expression> children) throws SQLException {
        return matchesIndexedExpression(node) ? convertAndVisitParseNode(node) : super.visitLeave(node, children);
    }

    @Override
    public boolean visitEnter(StringConcatParseNode node) throws SQLException {
        return matchesIndexedExpression(node) ? false : super.visitEnter(node);
    }

    @Override
    public Expression visitLeave(StringConcatParseNode node, List<Expression> children) throws SQLException {
        return matchesIndexedExpression(node) ? convertAndVisitParseNode(node) : super.visitLeave(node, children);
    }

    @Override
    public boolean visitEnter(RowValueConstructorParseNode node) throws SQLException {
        return matchesIndexedExpression(node) ? false : super.visitEnter(node);
    }

    @Override
    public Expression visitLeave(RowValueConstructorParseNode node, List<Expression> children) throws SQLException {
        return matchesIndexedExpression(node) ? convertAndVisitParseNode(node) : super.visitLeave(node, children);
    }

}
