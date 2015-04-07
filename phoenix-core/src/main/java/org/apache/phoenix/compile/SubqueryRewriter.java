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
import java.sql.SQLFeatureNotSupportedException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.function.DistinctValueAggregateFunction;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.parse.AliasedNode;
import org.apache.phoenix.parse.AndParseNode;
import org.apache.phoenix.parse.ArrayAllComparisonNode;
import org.apache.phoenix.parse.ArrayAnyComparisonNode;
import org.apache.phoenix.parse.BooleanParseNodeVisitor;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.ComparisonParseNode;
import org.apache.phoenix.parse.CompoundParseNode;
import org.apache.phoenix.parse.ExistsParseNode;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.parse.InParseNode;
import org.apache.phoenix.parse.JoinTableNode.JoinType;
import org.apache.phoenix.parse.LiteralParseNode;
import org.apache.phoenix.parse.OrderByNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.ParseNodeRewriter;
import org.apache.phoenix.parse.RowValueConstructorParseNode;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.StatelessTraverseAllParseNodeVisitor;
import org.apache.phoenix.parse.SubqueryParseNode;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.parse.TableNode;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.schema.ColumnNotFoundException;

import com.google.common.collect.Lists;

/*
 * Class for rewriting where-clause sub-queries into join queries.
 * 
 * If the where-clause sub-query is one of those top-node conditions (being 
 * the only condition node or direct descendant of AND nodes), we convert the
 * sub-query directly into semi-joins, anti-joins or inner-joins, and meanwhile
 * remove the original condition node from the where clause.
 * Otherwise, we convert the sub-query into left-joins and change the original
 * condition node into a null test of a join table field (ONE if matched, NULL 
 * if not matched).
 */
public class SubqueryRewriter extends ParseNodeRewriter {
    private static final ParseNodeFactory NODE_FACTORY = new ParseNodeFactory();
    
    private final ColumnResolver resolver;
    private final PhoenixConnection connection;
    private TableNode tableNode;
    private ParseNode topNode;
    
    public static SelectStatement transform(SelectStatement select, ColumnResolver resolver, PhoenixConnection connection) throws SQLException {
        ParseNode where = select.getWhere();
        if (where == null)
            return select;
        
        SubqueryRewriter rewriter = new SubqueryRewriter(select, resolver, connection);
        ParseNode normWhere = rewrite(where, rewriter);
        if (normWhere == where)
            return select;
        
        return NODE_FACTORY.select(select, rewriter.tableNode, normWhere);
    }
    
    protected SubqueryRewriter(SelectStatement select, ColumnResolver resolver, PhoenixConnection connection) {
        this.resolver = resolver;
        this.connection = connection;
        this.tableNode = select.getFrom();
        this.topNode = null;
    }
    
    @Override
    protected void enterParseNode(ParseNode node) {
        if (topNode == null) {
            topNode = node;
        }
        super.enterParseNode(node);
    }
    
    @Override
    protected ParseNode leaveCompoundNode(CompoundParseNode node, List<ParseNode> children, ParseNodeRewriter.CompoundNodeFactory factory) {
        if (topNode == node) {
            topNode = null;
        }
        
        return super.leaveCompoundNode(node, children, factory);
    }

    @Override
    public boolean visitEnter(AndParseNode node) throws SQLException {
        return true;
    }

    @Override
    public ParseNode visitLeave(AndParseNode node, List<ParseNode> l) throws SQLException {
        return leaveCompoundNode(node, l, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                if (children.isEmpty()) {
                    return null;
                }
                if (children.size() == 1) {
                    return children.get(0);
                }
                return NODE_FACTORY.and(children);
            }
        });
    }

    @Override
    public ParseNode visitLeave(InParseNode node, List<ParseNode> l) throws SQLException {
        boolean isTopNode = topNode == node;
        if (isTopNode) {
            topNode = null;
        }
        
        SubqueryParseNode subqueryNode = (SubqueryParseNode) l.get(1);
        SelectStatement subquery = fixSubqueryStatement(subqueryNode.getSelectNode());
        String rhsTableAlias = ParseNodeFactory.createTempAlias();
        List<AliasedNode> selectNodes = fixAliasedNodes(subquery.getSelect(), true);
        subquery = NODE_FACTORY.select(subquery, !node.isSubqueryDistinct(), selectNodes);
        ParseNode onNode = getJoinConditionNode(l.get(0), selectNodes, rhsTableAlias);
        TableNode rhsTable = NODE_FACTORY.derivedTable(rhsTableAlias, subquery);
        JoinType joinType = isTopNode ? (node.isNegate() ? JoinType.Anti : JoinType.Semi) : JoinType.Left;
        ParseNode ret = isTopNode ? null : NODE_FACTORY.isNull(NODE_FACTORY.column(NODE_FACTORY.table(null, rhsTableAlias), selectNodes.get(0).getAlias(), null), !node.isNegate());
        tableNode = NODE_FACTORY.join(joinType, tableNode, rhsTable, onNode, false);
        
        return ret;
    }

    @Override
    public ParseNode visitLeave(ExistsParseNode node, List<ParseNode> l) throws SQLException {
        boolean isTopNode = topNode == node;
        if (isTopNode) {
            topNode = null;
        }
        
        SubqueryParseNode subqueryNode = (SubqueryParseNode) l.get(0);
        SelectStatement subquery = fixSubqueryStatement(subqueryNode.getSelectNode());
        String rhsTableAlias = ParseNodeFactory.createTempAlias();
        JoinConditionExtractor conditionExtractor = new JoinConditionExtractor(subquery, resolver, connection, rhsTableAlias);
        ParseNode where = subquery.getWhere() == null ? null : subquery.getWhere().accept(conditionExtractor);
        if (where == subquery.getWhere()) { // non-correlated EXISTS subquery, add LIMIT 1
            subquery = NODE_FACTORY.select(subquery, NODE_FACTORY.limit(NODE_FACTORY.literal(1)));
            subqueryNode = NODE_FACTORY.subquery(subquery, false);
            node = NODE_FACTORY.exists(subqueryNode, node.isNegate());
            return super.visitLeave(node, Collections.<ParseNode> singletonList(subqueryNode));
        }
        
        List<AliasedNode> additionalSelectNodes = conditionExtractor.getAdditionalSelectNodes();
        List<AliasedNode> selectNodes = Lists.newArrayListWithExpectedSize(additionalSelectNodes.size() + 1);
        selectNodes.add(NODE_FACTORY.aliasedNode(ParseNodeFactory.createTempAlias(), LiteralParseNode.ONE));
        selectNodes.addAll(additionalSelectNodes);
        
        subquery = NODE_FACTORY.select(subquery, true, selectNodes, where);
        ParseNode onNode = conditionExtractor.getJoinCondition();
        TableNode rhsTable = NODE_FACTORY.derivedTable(rhsTableAlias, subquery);
        JoinType joinType = isTopNode ? (node.isNegate() ? JoinType.Anti : JoinType.Semi) : JoinType.Left;
        ParseNode ret = isTopNode ? null : NODE_FACTORY.isNull(NODE_FACTORY.column(NODE_FACTORY.table(null, rhsTableAlias), selectNodes.get(0).getAlias(), null), !node.isNegate());
        tableNode = NODE_FACTORY.join(joinType, tableNode, rhsTable, onNode, false);
        
        return ret;
    }

    @Override
    public ParseNode visitLeave(ComparisonParseNode node, List<ParseNode> l) throws SQLException {
        boolean isTopNode = topNode == node;
        if (isTopNode) {
            topNode = null;
        }
        
        ParseNode secondChild = l.get(1);
        if (!(secondChild instanceof SubqueryParseNode)) {
            return super.visitLeave(node, l);
        }
        
        SubqueryParseNode subqueryNode = (SubqueryParseNode) secondChild;
        SelectStatement subquery = fixSubqueryStatement(subqueryNode.getSelectNode());
        String rhsTableAlias = ParseNodeFactory.createTempAlias();
        JoinConditionExtractor conditionExtractor = new JoinConditionExtractor(subquery, resolver, connection, rhsTableAlias);
        ParseNode where = subquery.getWhere() == null ? null : subquery.getWhere().accept(conditionExtractor);
        if (where == subquery.getWhere()) { // non-correlated comparison subquery, add LIMIT 2, expectSingleRow = true
            subquery = NODE_FACTORY.select(subquery, NODE_FACTORY.limit(NODE_FACTORY.literal(2)));
            subqueryNode = NODE_FACTORY.subquery(subquery, true);
            l = Lists.newArrayList(l.get(0), subqueryNode);
            node = NODE_FACTORY.comparison(node.getFilterOp(), l.get(0), l.get(1));
            return super.visitLeave(node, l);
        }
        
        ParseNode rhsNode = null; 
        boolean isGroupby = !subquery.getGroupBy().isEmpty();
        boolean isAggregate = subquery.isAggregate();
        List<AliasedNode> aliasedNodes = subquery.getSelect();
        if (aliasedNodes.size() == 1) {
            rhsNode = aliasedNodes.get(0).getNode();
        } else {
            List<ParseNode> nodes = Lists.<ParseNode> newArrayListWithExpectedSize(aliasedNodes.size());
            for (AliasedNode aliasedNode : aliasedNodes) {
                nodes.add(aliasedNode.getNode());
            }
            rhsNode = NODE_FACTORY.rowValueConstructor(nodes);
        }
        
        List<AliasedNode> additionalSelectNodes = conditionExtractor.getAdditionalSelectNodes();
        List<AliasedNode> selectNodes = Lists.newArrayListWithExpectedSize(additionalSelectNodes.size() + 1);        
        selectNodes.add(NODE_FACTORY.aliasedNode(ParseNodeFactory.createTempAlias(), rhsNode));
        selectNodes.addAll(additionalSelectNodes);
        
        if (!isAggregate) {
            subquery = NODE_FACTORY.select(subquery, subquery.isDistinct(), selectNodes, where);            
        } else {
            List<ParseNode> groupbyNodes = Lists.newArrayListWithExpectedSize(additionalSelectNodes.size() + subquery.getGroupBy().size());
            for (AliasedNode aliasedNode : additionalSelectNodes) {
                groupbyNodes.add(aliasedNode.getNode());
            }
            groupbyNodes.addAll(subquery.getGroupBy());
            subquery = NODE_FACTORY.select(subquery, subquery.isDistinct(), selectNodes, where, groupbyNodes, true);
        }
        
        ParseNode onNode = conditionExtractor.getJoinCondition();
        TableNode rhsTable = NODE_FACTORY.derivedTable(rhsTableAlias, subquery);
        JoinType joinType = isTopNode ? JoinType.Inner : JoinType.Left;
        ParseNode ret = NODE_FACTORY.comparison(node.getFilterOp(), l.get(0), NODE_FACTORY.column(NODE_FACTORY.table(null, rhsTableAlias), selectNodes.get(0).getAlias(), null));
        tableNode = NODE_FACTORY.join(joinType, tableNode, rhsTable, onNode, !isAggregate || isGroupby);
        
        return ret;
    }

    @Override
    public ParseNode visitLeave(ArrayAnyComparisonNode node, List<ParseNode> l) throws SQLException {
        List<ParseNode> children = leaveArrayComparisonNode(node, l);
        if (children == l)
            return super.visitLeave(node, l);
        
        node = NODE_FACTORY.arrayAny(children.get(0), (ComparisonParseNode) children.get(1));
        return node;
    }

    @Override
    public ParseNode visitLeave(ArrayAllComparisonNode node, List<ParseNode> l) throws SQLException {
        List<ParseNode> children = leaveArrayComparisonNode(node, l);
        if (children == l)
            return super.visitLeave(node, l);
        
        node = NODE_FACTORY.arrayAll(children.get(0), (ComparisonParseNode) children.get(1));
        return node;
    }
    
    protected List<ParseNode> leaveArrayComparisonNode(ParseNode node, List<ParseNode> l) throws SQLException {
        boolean isTopNode = topNode == node;
        if (isTopNode) {
            topNode = null;
        }

        ParseNode firstChild = l.get(0);
        if (!(firstChild instanceof SubqueryParseNode)) {
            return l;
        }
        
        SubqueryParseNode subqueryNode = (SubqueryParseNode) firstChild;
        SelectStatement subquery = fixSubqueryStatement(subqueryNode.getSelectNode());
        String rhsTableAlias = ParseNodeFactory.createTempAlias();
        JoinConditionExtractor conditionExtractor = new JoinConditionExtractor(subquery, resolver, connection, rhsTableAlias);
        ParseNode where = subquery.getWhere() == null ? null : subquery.getWhere().accept(conditionExtractor);
        if (where == subquery.getWhere()) { // non-correlated any/all comparison subquery
            return l;
        }
        
        ParseNode rhsNode = null; 
        boolean isNonGroupByAggregate = subquery.getGroupBy().isEmpty() && subquery.isAggregate();
        List<AliasedNode> aliasedNodes = subquery.getSelect();
        String derivedTableAlias = null;
        if (!subquery.getGroupBy().isEmpty()) {
            derivedTableAlias = ParseNodeFactory.createTempAlias();
            aliasedNodes = fixAliasedNodes(aliasedNodes, false);
        }
        
        if (aliasedNodes.size() == 1) {
            rhsNode = derivedTableAlias == null ? aliasedNodes.get(0).getNode() : NODE_FACTORY.column(NODE_FACTORY.table(null, derivedTableAlias), aliasedNodes.get(0).getAlias(), null);
        } else {
            List<ParseNode> nodes = Lists.<ParseNode> newArrayListWithExpectedSize(aliasedNodes.size());
            for (AliasedNode aliasedNode : aliasedNodes) {
                nodes.add(derivedTableAlias == null ? aliasedNode.getNode() : NODE_FACTORY.column(NODE_FACTORY.table(null, derivedTableAlias), aliasedNode.getAlias(), null));
            }
            rhsNode = NODE_FACTORY.rowValueConstructor(nodes);
        }
        
        if (!isNonGroupByAggregate) {
            rhsNode = NODE_FACTORY.function(DistinctValueAggregateFunction.NAME, Collections.singletonList(rhsNode));
        }
        
        List<AliasedNode> additionalSelectNodes = conditionExtractor.getAdditionalSelectNodes();
        List<AliasedNode> selectNodes = Lists.newArrayListWithExpectedSize(additionalSelectNodes.size() + 1);        
        selectNodes.add(NODE_FACTORY.aliasedNode(ParseNodeFactory.createTempAlias(), rhsNode));
        selectNodes.addAll(additionalSelectNodes);
        List<ParseNode> groupbyNodes = Lists.newArrayListWithExpectedSize(additionalSelectNodes.size());
        for (AliasedNode aliasedNode : additionalSelectNodes) {
            groupbyNodes.add(aliasedNode.getNode());
        }
        
        if (derivedTableAlias == null) {
            subquery = NODE_FACTORY.select(subquery, false, selectNodes, where, groupbyNodes, true);
        } else {
            List<ParseNode> derivedTableGroupBy = Lists.newArrayListWithExpectedSize(subquery.getGroupBy().size() + groupbyNodes.size());
            derivedTableGroupBy.addAll(groupbyNodes);
            derivedTableGroupBy.addAll(subquery.getGroupBy());
            List<AliasedNode> derivedTableSelect = Lists.newArrayListWithExpectedSize(aliasedNodes.size() + selectNodes.size() - 1);
            derivedTableSelect.addAll(aliasedNodes);
            for (int i = 1; i < selectNodes.size(); i++) {
                AliasedNode aliasedNode = selectNodes.get(i);
                String alias = ParseNodeFactory.createTempAlias();
                derivedTableSelect.add(NODE_FACTORY.aliasedNode(alias, aliasedNode.getNode()));
                aliasedNode = NODE_FACTORY.aliasedNode(aliasedNode.getAlias(), NODE_FACTORY.column(NODE_FACTORY.table(null, derivedTableAlias), alias, null));
                selectNodes.set(i, aliasedNode);
                groupbyNodes.set(i - 1, aliasedNode.getNode());
            }
            SelectStatement derivedTableStmt = NODE_FACTORY.select(subquery, subquery.isDistinct(), derivedTableSelect, where, derivedTableGroupBy, true);
            subquery = NODE_FACTORY.select(NODE_FACTORY.derivedTable(derivedTableAlias, derivedTableStmt), subquery.getHint(), false, selectNodes, null, groupbyNodes, null, Collections.<OrderByNode> emptyList(), null, subquery.getBindCount(), true, false, Collections.<SelectStatement>emptyList());
        }
        
        ParseNode onNode = conditionExtractor.getJoinCondition();
        TableNode rhsTable = NODE_FACTORY.derivedTable(rhsTableAlias, subquery);
        JoinType joinType = isTopNode ? JoinType.Inner : JoinType.Left;
        tableNode = NODE_FACTORY.join(joinType, tableNode, rhsTable, onNode, false);

        firstChild = NODE_FACTORY.column(NODE_FACTORY.table(null, rhsTableAlias), selectNodes.get(0).getAlias(), null);
        if (isNonGroupByAggregate) {
            firstChild = NODE_FACTORY.upsertStmtArrayNode(Collections.singletonList(firstChild)); 
        }
        ComparisonParseNode secondChild = (ComparisonParseNode) l.get(1);
        secondChild = NODE_FACTORY.comparison(secondChild.getFilterOp(), secondChild.getLHS(), NODE_FACTORY.elementRef(Lists.newArrayList(firstChild, NODE_FACTORY.literal(1))));
        
        return Lists.newArrayList(firstChild, secondChild);
    }
    
    private SelectStatement fixSubqueryStatement(SelectStatement select) {
        if (!select.isUnion())
            return select;
        
        // Wrap as a derived table.
        return NODE_FACTORY.select(NODE_FACTORY.derivedTable(ParseNodeFactory.createTempAlias(), select), HintNode.EMPTY_HINT_NODE, false, select.getSelect(), null, null, null, null, null, select.getBindCount(), false, false, Collections.<SelectStatement> emptyList());
    }
    
    private List<AliasedNode> fixAliasedNodes(List<AliasedNode> nodes, boolean addSelectOne) {
        List<AliasedNode> normNodes = Lists.<AliasedNode> newArrayListWithExpectedSize(nodes.size() + (addSelectOne ? 1 : 0));
        if (addSelectOne) {
            normNodes.add(NODE_FACTORY.aliasedNode(ParseNodeFactory.createTempAlias(), LiteralParseNode.ONE));
        }
        for (int i = 0; i < nodes.size(); i++) {
            AliasedNode aliasedNode = nodes.get(i);
            normNodes.add(NODE_FACTORY.aliasedNode(
                    ParseNodeFactory.createTempAlias(), aliasedNode.getNode()));
        }
        
        return normNodes;
    }
    
    private ParseNode getJoinConditionNode(ParseNode lhs, List<AliasedNode> rhs, String rhsTableAlias) throws SQLException {
        List<ParseNode> lhsNodes;        
        if (lhs instanceof RowValueConstructorParseNode) {
            lhsNodes = ((RowValueConstructorParseNode) lhs).getChildren();
        } else {
            lhsNodes = Collections.singletonList(lhs);
        }
        if (lhsNodes.size() != (rhs.size() - 1))
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.SUBQUERY_RETURNS_DIFFERENT_NUMBER_OF_FIELDS).build().buildException();
        
        int count = lhsNodes.size();
        TableName rhsTableName = NODE_FACTORY.table(null, rhsTableAlias);
        List<ParseNode> equalNodes = Lists.newArrayListWithExpectedSize(count);
        for (int i = 0; i < count; i++) {
            ParseNode rhsNode = NODE_FACTORY.column(rhsTableName, rhs.get(i + 1).getAlias(), null);
            equalNodes.add(NODE_FACTORY.equal(lhsNodes.get(i), rhsNode));
        }
        
        return count == 1 ? equalNodes.get(0) : NODE_FACTORY.and(equalNodes);
    }
    
    private static class JoinConditionExtractor extends BooleanParseNodeVisitor<ParseNode> {
        private final TableName tableName;
        private ColumnResolveVisitor columnResolveVisitor;
        private List<AliasedNode> additionalSelectNodes;
        private List<ParseNode> joinConditions;
        
        public JoinConditionExtractor(SelectStatement subquery, ColumnResolver outerResolver, 
                PhoenixConnection connection, String tableAlias) throws SQLException {
            this.tableName = NODE_FACTORY.table(null, tableAlias);
            ColumnResolver localResolver = FromCompiler.getResolverForQuery(subquery, connection);
            this.columnResolveVisitor = new ColumnResolveVisitor(localResolver, outerResolver);
            this.additionalSelectNodes = Lists.<AliasedNode> newArrayList();
            this.joinConditions = Lists.<ParseNode> newArrayList();
        }
        
        public List<AliasedNode> getAdditionalSelectNodes() {
            return this.additionalSelectNodes;
        }
        
        public ParseNode getJoinCondition() {
            if (this.joinConditions.isEmpty())
                return null;
            
            if (this.joinConditions.size() == 1)
                return this.joinConditions.get(0);
            
            return NODE_FACTORY.and(this.joinConditions);            
        }
        
        @Override
        public List<ParseNode> newElementList(int size) {
            return Lists.<ParseNode> newArrayListWithExpectedSize(size);
        }

        @Override
        public void addElement(List<ParseNode> l, ParseNode element) {
            if (element != null) {
                l.add(element);
            }
        }

        @Override
        public boolean visitEnter(AndParseNode node) throws SQLException {
            return true;
        }

        @Override
        public ParseNode visitLeave(AndParseNode node, List<ParseNode> l)
                throws SQLException {
            if (l.equals(node.getChildren()))
                return node;

            if (l.isEmpty())
                return null;
            
            if (l.size() == 1)
                return l.get(0);
            
            return NODE_FACTORY.and(l);
        }

        @Override
        protected boolean enterBooleanNode(ParseNode node) throws SQLException {
            return false;
        }

        @Override
        protected ParseNode leaveBooleanNode(ParseNode node, List<ParseNode> l)
                throws SQLException {
            columnResolveVisitor.reset();
            node.accept(columnResolveVisitor);
            ColumnResolveVisitor.ColumnResolveType type = columnResolveVisitor.getColumnResolveType();
            if (type != ColumnResolveVisitor.ColumnResolveType.NONE 
                    && type != ColumnResolveVisitor.ColumnResolveType.LOCAL)
                throw new SQLFeatureNotSupportedException("Does not support non-standard or non-equi correlated-subquery conditions.");
            
            return node;
        }

        @Override
        protected boolean enterNonBooleanNode(ParseNode node)
                throws SQLException {
            return false;
        }

        @Override
        protected ParseNode leaveNonBooleanNode(ParseNode node,
                List<ParseNode> l) throws SQLException {
            return node;
        }

        @Override
        public ParseNode visitLeave(ComparisonParseNode node, List<ParseNode> l) throws SQLException {
            if (node.getFilterOp() != CompareFilter.CompareOp.EQUAL)
                return leaveBooleanNode(node, l);
            
            columnResolveVisitor.reset();
            node.getLHS().accept(columnResolveVisitor);
            ColumnResolveVisitor.ColumnResolveType lhsType = columnResolveVisitor.getColumnResolveType();
            columnResolveVisitor.reset();
            node.getRHS().accept(columnResolveVisitor);
            ColumnResolveVisitor.ColumnResolveType rhsType = columnResolveVisitor.getColumnResolveType();
            if ((lhsType == ColumnResolveVisitor.ColumnResolveType.NONE || lhsType == ColumnResolveVisitor.ColumnResolveType.LOCAL)
                    && (rhsType == ColumnResolveVisitor.ColumnResolveType.NONE || rhsType == ColumnResolveVisitor.ColumnResolveType.LOCAL)) {
                return node;
            }
            if (lhsType == ColumnResolveVisitor.ColumnResolveType.LOCAL && rhsType == ColumnResolveVisitor.ColumnResolveType.OUTER) {
                String alias = ParseNodeFactory.createTempAlias();
                this.additionalSelectNodes.add(NODE_FACTORY.aliasedNode(alias, node.getLHS()));
                ParseNode lhsNode = NODE_FACTORY.column(tableName, alias, null);
                this.joinConditions.add(NODE_FACTORY.equal(lhsNode, node.getRHS()));
                return null;
            }        
            if (lhsType == ColumnResolveVisitor.ColumnResolveType.OUTER && rhsType == ColumnResolveVisitor.ColumnResolveType.LOCAL) {
                String alias = ParseNodeFactory.createTempAlias();
                this.additionalSelectNodes.add(NODE_FACTORY.aliasedNode(alias, node.getRHS()));
                ParseNode rhsNode = NODE_FACTORY.column(tableName, alias, null);
                this.joinConditions.add(NODE_FACTORY.equal(node.getLHS(), rhsNode));
                return null;
            }
            
            throw new SQLFeatureNotSupportedException("Does not support non-standard or non-equi correlated-subquery conditions.");   
        }
    }

    /*
     * Class for resolving inner query column references
     */
    private static class ColumnResolveVisitor extends StatelessTraverseAllParseNodeVisitor {
        public enum ColumnResolveType {NONE, LOCAL, OUTER, MIXED};

        private final ColumnResolver localResolver;
        private final ColumnResolver outerResolver;
        private ColumnResolveType type;

        public ColumnResolveVisitor(ColumnResolver localResolver, ColumnResolver outerResolver) {
            this.localResolver = localResolver;
            this.outerResolver = outerResolver;
            this.type = ColumnResolveType.NONE;
        }

        public void reset() {
            this.type = ColumnResolveType.NONE;
        }
        
        public ColumnResolveType getColumnResolveType() {
            return this.type;
        }

        @Override
        public Void visit(ColumnParseNode node) throws SQLException {
            // Inner query column definitions should shade those of outer query.
            try {
                localResolver.resolveColumn(node.getSchemaName(), node.getTableName(), node.getName());
                addType(true);
                return null;
            } catch (ColumnNotFoundException e) { 
            } catch (ColumnFamilyNotFoundException e) {
            }
            
            outerResolver.resolveColumn(node.getSchemaName(), node.getTableName(), node.getName());
            addType(false);
            return null;
        }
        
        private void addType(boolean isLocal) {
            switch (this.type) {
            case NONE:
                this.type = isLocal ? ColumnResolveType.LOCAL : ColumnResolveType.OUTER;
                break;
            case LOCAL:
                this.type = isLocal ? ColumnResolveType.LOCAL : ColumnResolveType.MIXED;
                break;
            case OUTER:
                this.type = isLocal ? ColumnResolveType.MIXED : ColumnResolveType.OUTER;
                break;
            default: // MIXED do nothing
                break;
            }
        }
    }
}
