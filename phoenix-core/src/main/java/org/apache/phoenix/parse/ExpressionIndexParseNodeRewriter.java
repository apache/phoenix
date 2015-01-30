/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.phoenix.parse;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.ExpressionCompiler;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.IndexStatementRewriter;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.IndexUtil;

import com.google.common.collect.Maps;

/**
 * Used to replace parse nodes in a SelectStatement that match expressions that are present in an indexed with the
 * corresponding {@link ColumnParseNode}
 */
public class ExpressionIndexParseNodeRewriter extends ParseNodeRewriter {

    private final Map<ParseNode, ParseNode> indexedParseNodeToColumnParseNodeMap;
    
    private static class ColumnParseNodeVisitor extends StatelessTraverseAllParseNodeVisitor {
        
        private boolean isParseNodeCaseSensitive;
        
        public void reset() {
            this.isParseNodeCaseSensitive = false;
        }
        
        @Override
        public Void visit(ColumnParseNode node) throws SQLException {
            isParseNodeCaseSensitive = isParseNodeCaseSensitive  || node.isCaseSensitive() || node.isTableNameCaseSensitive();
            return null;
        }
        
        public boolean isParseNodeCaseSensitive() {
            return isParseNodeCaseSensitive;
        }
        
    }

    public ExpressionIndexParseNodeRewriter(PTable index, PhoenixConnection connection) throws SQLException {
        indexedParseNodeToColumnParseNodeMap = Maps.newHashMapWithExpectedSize(index.getColumns().size());
        NamedTableNode tableNode = NamedTableNode.create(null,
                TableName.create(index.getParentSchemaName().getString(), index.getParentTableName().getString()),
                Collections.<ColumnDef> emptyList());
        ColumnResolver dataResolver = FromCompiler.getResolver(tableNode, connection);
        StatementContext context = new StatementContext(new PhoenixStatement(connection), dataResolver);
        IndexStatementRewriter rewriter = new IndexStatementRewriter(dataResolver, null);
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(context);
        ColumnParseNodeVisitor columnParseNodeVisitor = new ColumnParseNodeVisitor();
        for (PColumn column : index.getPKColumns()) {
            if (column.getExpressionStr()==null) {
                continue;
            }
            ParseNode expressionParseNode = SQLParser.parseCondition(column.getExpressionStr());
            // ignore regular columns
            if (expressionParseNode instanceof ColumnParseNode) {
                continue;
            }
            columnParseNodeVisitor.reset();
            expressionParseNode.accept(columnParseNodeVisitor);
            String colName = column.getName().getString();
            if (columnParseNodeVisitor.isParseNodeCaseSensitive()) {
                // force column name to be case sensitive name by surround with double quotes
                colName = "\"" + colName + "\"";
            }
            
            Expression dataExpression = expressionParseNode.accept(expressionCompiler);
            PDataType expressionDataType = dataExpression.getDataType();
            ParseNode indexedParseNode = expressionParseNode.accept(rewriter);
            PDataType indexColType = IndexUtil.getIndexColumnDataType(dataExpression.isNullable(), expressionDataType);
            ParseNode columnParseNode = new ColumnParseNode(null, colName, null);
            if ( indexColType != expressionDataType) {
                columnParseNode = NODE_FACTORY.cast(columnParseNode, expressionDataType, null, null);
            }
            indexedParseNodeToColumnParseNodeMap.put(indexedParseNode, columnParseNode);
        }
    }

    @Override
    protected ParseNode leaveCompoundNode(CompoundParseNode node, List<ParseNode> children, CompoundNodeFactory factory) {
        return indexedParseNodeToColumnParseNodeMap.containsKey(node) ? indexedParseNodeToColumnParseNodeMap.get(node)
                : super.leaveCompoundNode(node, children, factory);
    }

}
