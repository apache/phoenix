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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;



/**
 * 
 * Class that creates a new select statement by filtering out nodes.
 * Currently only supports filtering out boolean nodes (i.e. nodes
 * that may be ANDed and ORed together.
 *
 * TODO: generize this
 * 
 * @since 0.1
 */
public class SelectStatementRewriter extends ParseNodeRewriter {
    
    /**
     * Rewrite the select statement by filtering out expression nodes from the WHERE clause
     * @param statement the select statement from which to filter.
     * @param removeNodes expression nodes to filter out of WHERE clause.
     * @return new select statement
     * @throws SQLException 
     */
    public static SelectStatement removeFromWhereClause(SelectStatement statement, Set<ParseNode> removeNodes) throws SQLException {
        if (removeNodes.isEmpty()) {
            return statement;
        }
        ParseNode where = statement.getWhere();
        SelectStatementRewriter rewriter = new SelectStatementRewriter(removeNodes);
        where = where.accept(rewriter);
        // Return new SELECT statement with updated WHERE clause
        return NODE_FACTORY.select(statement, where);
    }
    
    /**
     * Rewrite the select statement by filtering out expression nodes from the HAVING clause
     * and anding them with the WHERE clause.
     * @param statement the select statement from which to move the nodes.
     * @param moveNodes expression nodes to filter out of HAVING clause and add to WHERE clause.
     * @return new select statement
     * @throws SQLException 
     */
    public static SelectStatement moveFromHavingToWhereClause(SelectStatement statement, Set<ParseNode> moveNodes) throws SQLException {
        if (moveNodes.isEmpty()) {
            return statement;
        }
        ParseNode andNode = NODE_FACTORY.and(new ArrayList<ParseNode>(moveNodes));
        ParseNode having = statement.getHaving();
        SelectStatementRewriter rewriter = new SelectStatementRewriter(moveNodes);
        having = having.accept(rewriter);
        ParseNode where = statement.getWhere();
        if (where == null) {
            where = andNode;
        } else {
            where = NODE_FACTORY.and(Arrays.asList(where,andNode));
        }
        // Return new SELECT statement with updated WHERE and HAVING clauses
        return NODE_FACTORY.select(statement, where, having);
    }
    
    private static final ParseNodeFactory NODE_FACTORY = new ParseNodeFactory();

    private final Set<ParseNode> removeNodes;
    
    private SelectStatementRewriter(Set<ParseNode> removeNodes) {
        this.removeNodes = removeNodes;
    }
    
    private static interface CompoundNodeFactory {
        ParseNode createNode(List<ParseNode> children);
    }
    
    private boolean enterCompoundNode(ParseNode node) {
        if (removeNodes.contains(node)) {
            return false;
        }
        return true;
    }
    
    private ParseNode leaveCompoundNode(CompoundParseNode node, List<ParseNode> children, CompoundNodeFactory factory) {
        int newSize = children.size();
        int oldSize = node.getChildren().size();
        if (newSize == oldSize) {
            return node;
        } else if (newSize > 1) {
            return factory.createNode(children);
        } else if (newSize == 1) {
            // TODO: keep or collapse? Maybe be helpful as context of where a problem occurs if a node could not be consumed
            return(children.get(0));
        } else {
            return null;
        }
    }
    
    @Override
    public boolean visitEnter(AndParseNode node) throws SQLException {
        return enterCompoundNode(node);
    }

    @Override
    public ParseNode visitLeave(AndParseNode node, List<ParseNode> nodes) throws SQLException {
        return leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.and(children);
            }
        });
    }

    @Override
    public boolean visitEnter(OrParseNode node) throws SQLException {
        return enterCompoundNode(node);
    }

    @Override
    public ParseNode visitLeave(OrParseNode node, List<ParseNode> nodes) throws SQLException {
        return leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.or(children);
            }
        });
    }
    
    @Override
    public boolean visitEnter(ComparisonParseNode node) throws SQLException {
        if (removeNodes.contains(node)) {
            return false;
        }
        return true;
    }

    @Override
    public ParseNode visitLeave(ComparisonParseNode node, List<ParseNode> c) throws SQLException {
        return c.isEmpty() ? null : node;
    }
    
    @Override
    public boolean visitEnter(LikeParseNode node) throws SQLException {
        if (removeNodes.contains(node)) {
            return false;
        }
        return true;
    }
    
    @Override
    public ParseNode visitLeave(LikeParseNode node, List<ParseNode> c) throws SQLException {
        return c.isEmpty() ? null : node;
    }
    
    @Override
    public boolean visitEnter(InListParseNode node) throws SQLException {
        if (removeNodes.contains(node)) {
            return false;
        }
        return true;
    }
    
    @Override
    public ParseNode visitLeave(InListParseNode node, List<ParseNode> c) throws SQLException {
        return c.isEmpty() ? null : node;
    }
    
    @Override
    public boolean visitEnter(InParseNode node) throws SQLException {
        if (removeNodes.contains(node)) {
            return false;
        }
        return true;
    }
    
    @Override
    public ParseNode visitLeave(InParseNode node, List<ParseNode> c) throws SQLException {
        return c.isEmpty() ? null : node;
    }
}
