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
import java.util.List;
import java.util.Map;

import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.ColumnNotFoundException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * 
 * Base class for visitors that rewrite the expression node hierarchy
 *
 * 
 * @since 0.1
 */
public class ParseNodeRewriter extends TraverseAllParseNodeVisitor<ParseNode> {

    protected static final ParseNodeFactory NODE_FACTORY = new ParseNodeFactory();

    public static ParseNode rewrite(ParseNode where, ParseNodeRewriter rewriter) throws SQLException {
        if (where == null) {
            return null;
        }
        rewriter.reset();
        return where.accept(rewriter);
    }

    /**
     * Rewrite the select statement by switching any constants to the right hand side
     * of the expression.
     * @param statement the select statement
     * @return new select statement
     * @throws SQLException 
     */
    public static SelectStatement rewrite(SelectStatement statement, ParseNodeRewriter rewriter) throws SQLException {
        Map<String,ParseNode> aliasMap = rewriter.getAliasMap();
        TableNode from = statement.getFrom();
        TableNode normFrom = from == null ? null : from.accept(new TableNodeRewriter(rewriter));
        ParseNode where = statement.getWhere();
        ParseNode normWhere = where;
        if (where != null) {
            rewriter.reset();
            normWhere = where.accept(rewriter);
        }
        ParseNode having = statement.getHaving();
        ParseNode normHaving= having;
        if (having != null) {
            rewriter.reset();
            normHaving = having.accept(rewriter);
        }
        List<AliasedNode> selectNodes = statement.getSelect();
        List<AliasedNode> normSelectNodes = selectNodes;
        for (int i = 0; i < selectNodes.size(); i++) {
            AliasedNode aliasedNode = selectNodes.get(i);
            ParseNode selectNode = aliasedNode.getNode();
            rewriter.reset();
            ParseNode normSelectNode = selectNode.accept(rewriter);
            if (selectNode == normSelectNode) {
                if (selectNodes != normSelectNodes) {
                    normSelectNodes.add(aliasedNode);
                }
                continue;
            }
            if (selectNodes == normSelectNodes) {
                normSelectNodes = Lists.newArrayList(selectNodes.subList(0, i));
            }
            AliasedNode normAliasedNode = NODE_FACTORY.aliasedNode(aliasedNode.isCaseSensitve() ? '"' + aliasedNode.getAlias() + '"' : aliasedNode.getAlias(), normSelectNode);
            normSelectNodes.add(normAliasedNode);
        }
        // Add to map in separate pass so that we don't try to use aliases
        // while processing the select expressions
        if (aliasMap != null) {
            for (int i = 0; i < normSelectNodes.size(); i++) {
                AliasedNode aliasedNode = normSelectNodes.get(i);
                ParseNode selectNode = aliasedNode.getNode();
                String alias = aliasedNode.getAlias();
                if (alias != null) {
                    aliasMap.put(alias, selectNode);
                }
            }
        }

        List<ParseNode> groupByNodes = statement.getGroupBy();
        List<ParseNode> normGroupByNodes = groupByNodes;
        for (int i = 0; i < groupByNodes.size(); i++) {
            ParseNode groupByNode = groupByNodes.get(i);
            rewriter.reset();
            ParseNode normGroupByNode = groupByNode.accept(rewriter);
            if (groupByNode == normGroupByNode) {
                if (groupByNodes != normGroupByNodes) {
                    normGroupByNodes.add(groupByNode);
                }
                continue;
            }
            if (groupByNodes == normGroupByNodes) {
                normGroupByNodes = Lists.newArrayList(groupByNodes.subList(0, i));
            }
            normGroupByNodes.add(normGroupByNode);
        }
        List<OrderByNode> orderByNodes = statement.getOrderBy();
        List<OrderByNode> normOrderByNodes = orderByNodes;
        for (int i = 0; i < orderByNodes.size(); i++) {
            OrderByNode orderByNode = orderByNodes.get(i);
            ParseNode node = orderByNode.getNode();
            rewriter.reset();
            ParseNode normNode = node.accept(rewriter);
            if (node == normNode) {
                if (orderByNodes != normOrderByNodes) {
                    normOrderByNodes.add(orderByNode);
                }
                continue;
            }
            if (orderByNodes == normOrderByNodes) {
                normOrderByNodes = Lists.newArrayList(orderByNodes.subList(0, i));
            }
            normOrderByNodes.add(NODE_FACTORY.orderBy(normNode, orderByNode.isNullsLast(), orderByNode.isAscending()));
        }

        // Return new SELECT statement with updated WHERE clause
        if (normFrom == from && 
        		normWhere == where && 
                normHaving == having && 
                selectNodes == normSelectNodes && 
                groupByNodes == normGroupByNodes &&
                orderByNodes == normOrderByNodes) {
            return statement;
        }
        return NODE_FACTORY.select(normFrom, statement.getHint(), statement.isDistinct(),
                normSelectNodes, normWhere, normGroupByNodes, normHaving, normOrderByNodes,
                statement.getLimit(), statement.getBindCount(), statement.isAggregate(), statement.hasSequence(),
                statement.getSelects());
    }

    private Map<String, ParseNode> getAliasMap() {
        return aliasMap;
    }

    private final ColumnResolver resolver;
    private final Map<String, ParseNode> aliasMap;
    private int nodeCount;

    public boolean isTopLevel() {
        return nodeCount == 0;
    }

    protected ParseNodeRewriter() {
        this.resolver = null;
        this.aliasMap = null;
    }

    protected ParseNodeRewriter(ColumnResolver resolver) {
        this.resolver = resolver;
        this.aliasMap = null;
    }

    protected ParseNodeRewriter(ColumnResolver resolver, int maxAliasCount) {
        this.resolver = resolver;
        this.aliasMap = Maps.newHashMapWithExpectedSize(maxAliasCount);
    }

    protected ColumnResolver getResolver() {
        return resolver;
    }

    protected void reset() {
        this.nodeCount = 0;
    }

    protected static interface CompoundNodeFactory {
        ParseNode createNode(List<ParseNode> children);
    }

    protected ParseNode leaveCompoundNode(CompoundParseNode node, List<ParseNode> children, CompoundNodeFactory factory) {
        if (children.equals(node.getChildren())) {
            return node;
        } else { // Child nodes have been inverted (because a literal was found on LHS)
            return factory.createNode(children);
        }
    }
    
    @Override
    protected void enterParseNode(ParseNode node) {
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
    public ParseNode visitLeave(OrParseNode node, List<ParseNode> nodes) throws SQLException {
        return leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.or(children);
            }
        });
    }

    @Override
    public ParseNode visitLeave(SubtractParseNode node, List<ParseNode> nodes) throws SQLException {
        return leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.subtract(children);
            }
        });
    }

    @Override
    public ParseNode visitLeave(AddParseNode node, List<ParseNode> nodes) throws SQLException {
        return leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.add(children);
            }
        });
    }

    @Override
    public ParseNode visitLeave(MultiplyParseNode node, List<ParseNode> nodes) throws SQLException {
        return leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.multiply(children);
            }
        });
    }

    @Override
    public ParseNode visitLeave(DivideParseNode node, List<ParseNode> nodes) throws SQLException {
        return leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.divide(children);
            }
        });
    }

    @Override
    public ParseNode visitLeave(ModulusParseNode node, List<ParseNode> nodes) throws SQLException {
        return leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.modulus(children);
            }
        });
    }

    @Override
    public ParseNode visitLeave(final FunctionParseNode node, List<ParseNode> nodes) throws SQLException {
        return leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.function(node.getName(),children);
            }
        });
    }

    @Override
    public ParseNode visitLeave(CaseParseNode node, List<ParseNode> nodes) throws SQLException {
        return leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.caseWhen(children);
            }
        });
    }

    @Override
    public ParseNode visitLeave(final LikeParseNode node, List<ParseNode> nodes) throws SQLException {
        return leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.like(children.get(0),children.get(1),node.isNegate(), node.getLikeType());
            }
        });
    }

    @Override
    public ParseNode visitLeave(NotParseNode node, List<ParseNode> nodes) throws SQLException {
        return leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.not(children.get(0));
            }
        });
    }

    @Override
    public ParseNode visitLeave(final ExistsParseNode node, List<ParseNode> nodes) throws SQLException {
        return leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.exists(children.get(0), node.isNegate());
            }
        });
    }

    @Override
    public ParseNode visitLeave(final CastParseNode node, List<ParseNode> nodes) throws SQLException {
        return leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.cast(children.get(0), node.getDataType(), node.getMaxLength(), node.getScale());
            }
        });
    }

    @Override
    public ParseNode visitLeave(final InListParseNode node, List<ParseNode> nodes) throws SQLException {
        ParseNode normNode = leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.inList(children, node.isNegate());
            }
        });
        return normNode;
    }

    @Override
    public ParseNode visitLeave(final InParseNode node, List<ParseNode> nodes) throws SQLException {
        ParseNode normNode = leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.in(children.get(0), children.get(1), node.isNegate(), node.isSubqueryDistinct());
            }
        });
        return normNode;
    }
    
    @Override
    public ParseNode visitLeave(final IsNullParseNode node, List<ParseNode> nodes) throws SQLException {
        return leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.isNull(children.get(0), node.isNegate());
            }
        });
    }

    @Override
    public ParseNode visitLeave(final ComparisonParseNode node, List<ParseNode> nodes) throws SQLException {
        ParseNode normNode = leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.comparison(node.getFilterOp(), children.get(0), children.get(1));
            }
        });
        return normNode;
    }

    @Override
    public ParseNode visitLeave(final BetweenParseNode node, List<ParseNode> nodes) throws SQLException {
        return leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                if(node.isNegate()) {
                    return NODE_FACTORY.not(NODE_FACTORY.and(children));
                } else {
                    return NODE_FACTORY.and(children);
                }
            }
        });
    }

    @Override
    public ParseNode visit(ColumnParseNode node) throws SQLException {
        // If we're resolving aliases and we have an unqualified ColumnParseNode,
        // check if we find the name in our alias map.
        if (aliasMap != null && node.getTableName() == null) {
            ParseNode aliasedNode = aliasMap.get(node.getName());
            // If we found something, then try to resolve it unless the two nodes are the same
            if (aliasedNode != null && !node.equals(aliasedNode)) {
                try {
                    // If we're able to resolve it, that means we have a conflict
                    resolver.resolveColumn(node.getSchemaName(), node.getTableName(), node.getName());
                    throw new AmbiguousColumnException(node.getName());
                } catch (ColumnNotFoundException e) {
                    // Not able to resolve alias as a column name as well, so we use the alias
                    return aliasedNode;
                }
            }
        }
        return node;
    }

    @Override
    public ParseNode visit(LiteralParseNode node) throws SQLException {
        return node;
    }

    @Override
    public ParseNode visit(BindParseNode node) throws SQLException {
        return node;
    }

    @Override
    public ParseNode visit(WildcardParseNode node) throws SQLException {
        return node;
    }

    @Override
    public ParseNode visit(TableWildcardParseNode node) throws SQLException {
        return node;
    }

    @Override
    public ParseNode visit(FamilyWildcardParseNode node) throws SQLException {
        return node;
    }

    @Override
    public ParseNode visit(SubqueryParseNode node) throws SQLException {
        return node;
    }
    
    @Override
    public List<ParseNode> newElementList(int size) {
        nodeCount += size;
        return new ArrayList<ParseNode>(size);
    }

    @Override
    public ParseNode visitLeave(StringConcatParseNode node, List<ParseNode> l) throws SQLException {
        return leaveCompoundNode(node, l, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.concat(children);
            }
        });
    }

    @Override
    public void addElement(List<ParseNode> l, ParseNode element) {
        nodeCount--;
        if (element != null) {
            l.add(element);
        }
    }

    @Override
    public ParseNode visitLeave(RowValueConstructorParseNode node, List<ParseNode> children) throws SQLException {
        // Strip trailing nulls from rvc as they have no meaning
        if (children.get(children.size()-1) == null) {
            children = Lists.newArrayList(children);
            do {
                children.remove(children.size()-1);
            } while (children.size() > 0 && children.get(children.size()-1) == null);
            // If we're down to a single child, it's not a rvc anymore
            if (children.size() == 0) {
                return null;
            }
            if (children.size() == 1) {
                return children.get(0);
            }
        }
        // Flatten nested row value constructors, as this makes little sense and adds no information
        List<ParseNode> flattenedChildren = children;
        for (int i = 0; i < children.size(); i++) {
            ParseNode child = children.get(i);
            if (child instanceof RowValueConstructorParseNode) {
                if (flattenedChildren == children) {
                    flattenedChildren = Lists.newArrayListWithExpectedSize(children.size() + child.getChildren().size());
                    flattenedChildren.addAll(children.subList(0, i));
                }
                flattenedChildren.addAll(child.getChildren());
            }
        }

        return leaveCompoundNode(node, flattenedChildren, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.rowValueConstructor(children);
            }
        });
    }

	@Override
	public ParseNode visit(SequenceValueParseNode node) throws SQLException {		
		return node;
	}

	@Override
	public ParseNode visitLeave(ArrayConstructorNode node, List<ParseNode> nodes) throws SQLException {
	    return leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.upsertStmtArrayNode(children);
            }
        });
	}
	
	private static class TableNodeRewriter implements TableNodeVisitor<TableNode> {
	    private final ParseNodeRewriter parseNodeRewriter;

	    public TableNodeRewriter(ParseNodeRewriter parseNodeRewriter) {
	        this.parseNodeRewriter = parseNodeRewriter;
	    }

        @Override
        public TableNode visit(BindTableNode boundTableNode) throws SQLException {
            return boundTableNode;
        }

        @Override
        public TableNode visit(JoinTableNode joinNode) throws SQLException {
            TableNode lhsNode = joinNode.getLHS();
            TableNode rhsNode = joinNode.getRHS();
            ParseNode onNode = joinNode.getOnNode();
            TableNode normLhsNode = lhsNode.accept(this);
            TableNode normRhsNode = rhsNode.accept(this);
            parseNodeRewriter.reset();
            ParseNode normOnNode = onNode == null ? null : onNode.accept(parseNodeRewriter);
            if (lhsNode == normLhsNode && rhsNode == normRhsNode && onNode == normOnNode)
                return joinNode;

            return NODE_FACTORY.join(joinNode.getType(), normLhsNode, normRhsNode, normOnNode, joinNode.isSingleValueOnly());
        }

        @Override
        public TableNode visit(NamedTableNode namedTableNode) throws SQLException {
            return namedTableNode;
        }

        @Override
        public TableNode visit(DerivedTableNode subselectNode) throws SQLException {
            return subselectNode;
        }
	}

	@Override
    public ParseNode visitLeave(ArrayAnyComparisonNode node, final List<ParseNode> nodes) throws SQLException {
        ParseNode normNode = leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.arrayAny(nodes.get(0), (ComparisonParseNode) nodes.get(1));
            }
        });
        return normNode;
    }

    @Override
    public ParseNode visitLeave(ArrayAllComparisonNode node, final List<ParseNode> nodes) throws SQLException {
        ParseNode normNode = leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.arrayAll(nodes.get(0), (ComparisonParseNode) nodes.get(1));
            }
        });
        return normNode;
    }
 
    @Override
    public ParseNode visitLeave(ArrayElemRefNode node, List<ParseNode> nodes) throws SQLException {
        return leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.elementRef(children);
            }
        });
    }
}
