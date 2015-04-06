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
package org.apache.phoenix.compile;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.apache.phoenix.parse.AliasedNode;
import org.apache.phoenix.parse.ArrayElemRefNode;
import org.apache.phoenix.parse.BetweenParseNode;
import org.apache.phoenix.parse.BindTableNode;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.ComparisonParseNode;
import org.apache.phoenix.parse.DerivedTableNode;
import org.apache.phoenix.parse.FamilyWildcardParseNode;
import org.apache.phoenix.parse.JoinTableNode;
import org.apache.phoenix.parse.JoinTableNode.JoinType;
import org.apache.phoenix.parse.LessThanOrEqualParseNode;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeRewriter;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.parse.TableNodeVisitor;
import org.apache.phoenix.parse.TableWildcardParseNode;
import org.apache.phoenix.parse.WildcardParseNode;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.collect.Lists;


/**
 * 
 * Class that creates a new select statement ensuring that a literal always occurs
 * on the RHS (i.e. if literal found on the LHS, then the operator is reversed and
 * the literal is put on the RHS)
 *
 * 
 * @since 0.1
 */
public class StatementNormalizer extends ParseNodeRewriter {
    private boolean multiTable;
    
    public StatementNormalizer(ColumnResolver resolver, int expectedAliasCount, boolean multiTable) {
        super(resolver, expectedAliasCount);
        this.multiTable = multiTable;
    }

    public static ParseNode normalize(ParseNode where, ColumnResolver resolver) throws SQLException {
        return rewrite(where, new StatementNormalizer(resolver, 0, false));
    }
    
    /**
     * Rewrite the select statement by switching any constants to the right hand side
     * of the expression.
     * @param statement the select statement
     * @param resolver 
     * @return new select statement
     * @throws SQLException 
     */
    public static SelectStatement normalize(SelectStatement statement, ColumnResolver resolver) throws SQLException {
        boolean multiTable = statement.isJoin();
        // Replace WildcardParse with a list of TableWildcardParseNode for multi-table queries
        if (multiTable) {
            List<AliasedNode> selectNodes = statement.getSelect();
            List<AliasedNode> normSelectNodes = selectNodes;
            for (int i = 0; i < selectNodes.size(); i++) {
                AliasedNode aliasedNode = selectNodes.get(i);
                ParseNode selectNode = aliasedNode.getNode();
                if (selectNode == WildcardParseNode.INSTANCE) {
                    if (selectNodes == normSelectNodes) {
                        normSelectNodes = Lists.newArrayList(selectNodes.subList(0, i));
                    }
                    List<TableName> tableNames = statement.getFrom().accept(new TableNameVisitor());
                    for (TableName tableName : tableNames) {
                        TableWildcardParseNode node = NODE_FACTORY.tableWildcard(tableName);
                        normSelectNodes.add(NODE_FACTORY.aliasedNode(null, node));
                    }
                } else if (selectNodes != normSelectNodes) {
                    normSelectNodes.add(aliasedNode);
                }
            }
            if (selectNodes != normSelectNodes) {
                statement = NODE_FACTORY.select(statement.getFrom(), statement.getHint(), statement.isDistinct(),
                        normSelectNodes, statement.getWhere(), statement.getGroupBy(), statement.getHaving(), statement.getOrderBy(),
                        statement.getLimit(), statement.getBindCount(), statement.isAggregate(), statement.hasSequence(), statement.getSelects());
            }
        }
        
        return rewrite(statement, new StatementNormalizer(resolver, statement.getSelect().size(), multiTable));
    }

    private static class TableNameVisitor implements TableNodeVisitor<List<TableName>> {

        @Override
        public List<TableName> visit(BindTableNode boundTableNode) throws SQLException {
            TableName name = boundTableNode.getAlias() == null ? boundTableNode.getName() : TableName.create(null, boundTableNode.getAlias());
            return Collections.singletonList(name);
        }

        @Override
        public List<TableName> visit(JoinTableNode joinNode) throws SQLException {
            List<TableName> lhs = joinNode.getLHS().accept(this);
            List<TableName> rhs = joinNode.getType() == JoinType.Semi || joinNode.getType() == JoinType.Anti ? Collections.<TableName> emptyList() : joinNode.getRHS().accept(this);
            List<TableName> ret = Lists.<TableName>newArrayListWithExpectedSize(lhs.size() + rhs.size());
            ret.addAll(lhs);
            ret.addAll(rhs);
            return ret;
        }

        @Override
        public List<TableName> visit(NamedTableNode namedTableNode)
                throws SQLException {
            TableName name = namedTableNode.getAlias() == null ? namedTableNode.getName() : TableName.create(null, namedTableNode.getAlias());
            return Collections.singletonList(name);
        }

        @Override
        public List<TableName> visit(DerivedTableNode subselectNode)
                throws SQLException {
            TableName name = TableName.create(null, subselectNode.getAlias());
            return Collections.singletonList(name);
        }
    };
    
    @Override
    public ParseNode visitLeave(ComparisonParseNode node, List<ParseNode> nodes) throws SQLException {
         if (nodes.get(0).isStateless() && !nodes.get(1).isStateless()
                 && !(nodes.get(1) instanceof ArrayElemRefNode)) {
             List<ParseNode> normNodes = Lists.newArrayListWithExpectedSize(2);
             normNodes.add(nodes.get(1));
             normNodes.add(nodes.get(0));
             nodes = normNodes;
             node = NODE_FACTORY.comparison(node.getInvertFilterOp(), nodes.get(0), nodes.get(1));
         }
         return super.visitLeave(node, nodes);
    }
    
    @Override
    public ParseNode visitLeave(final BetweenParseNode node, List<ParseNode> nodes) throws SQLException {
       
        LessThanOrEqualParseNode lhsNode =  NODE_FACTORY.lte(node.getChildren().get(1), node.getChildren().get(0));
        LessThanOrEqualParseNode rhsNode =  NODE_FACTORY.lte(node.getChildren().get(0), node.getChildren().get(2));
        List<ParseNode> parseNodes = Lists.newArrayListWithExpectedSize(2);
        parseNodes.add(this.visitLeave(lhsNode, lhsNode.getChildren()));
        parseNodes.add(this.visitLeave(rhsNode, rhsNode.getChildren()));
        return super.visitLeave(node, parseNodes);
    }

    @Override
    public ParseNode visit(ColumnParseNode node) throws SQLException {
        if (multiTable 
                && node.getAlias() != null 
                && node.getTableName() != null
                && SchemaUtil.normalizeIdentifier(node.getAlias()).equals(node.getName())) {
            node = NODE_FACTORY.column(TableName.create(node.getSchemaName(), node.getTableName()), 
                    node.isCaseSensitive() ? '"' + node.getName() + '"' : node.getName(), 
                    node.isCaseSensitive() ? '"' + node.getFullName() + '"' : node.getFullName());
        }
        return super.visit(node);
    }
    
    @Override
    public ParseNode visit(FamilyWildcardParseNode node) throws SQLException {
        if (!multiTable)
            return super.visit(node);
        
        return super.visit(NODE_FACTORY.tableWildcard(NODE_FACTORY.table(null, node.isCaseSensitive() ? '"' + node.getName() + '"' : node.getName())));
    }
}

