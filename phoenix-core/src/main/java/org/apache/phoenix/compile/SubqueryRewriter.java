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

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.function.DistinctValueAggregateFunction;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.parse.AliasedNode;
import org.apache.phoenix.parse.AndParseNode;
import org.apache.phoenix.parse.AndRewriterBooleanParseNodeVisitor;
import org.apache.phoenix.parse.ArrayAllComparisonNode;
import org.apache.phoenix.parse.ArrayAnyComparisonNode;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.ComparisonParseNode;
import org.apache.phoenix.parse.CompoundParseNode;
import org.apache.phoenix.parse.DerivedTableNode;
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
import org.apache.phoenix.schema.TableNotFoundException;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

/**
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
    
    private final ColumnResolver columnResolver;
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
        this.columnResolver = resolver;
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

    /**
     * <pre>
     * {@code
     * Rewrite the In Subquery to semi/anti/left join for both NonCorrelated and Correlated subquery.
     *
     * 1.If the {@link InParseNode} is the only node in where clause or is the ANDed part of the where clause,
     *   then we would rewrite the In Subquery to semi/anti join:
     *   For  NonCorrelated subquery, an example is:
     *    SELECT item_id, name FROM item i WHERE i.item_id IN
     *    (SELECT item_id FROM order o  where o.price > 8)
     *
     *    The above sql would be rewritten as:
     *    SELECT ITEM_ID,NAME FROM item I  Semi JOIN
     *    (SELECT DISTINCT 1 $35,ITEM_ID $36 FROM order O  WHERE O.PRICE > 8) $34
     *     ON (I.ITEM_ID = $34.$36)
     *
     *   For Correlated subquery, an example is:
     *    SELECT item_id, name FROM item i WHERE i.item_id IN
     *    (SELECT item_id FROM order o  where o.price = i.price)
     *
     *    The above sql would be rewritten as:
     *    SELECT ITEM_ID,NAME FROM item I  Semi JOIN
     *    (SELECT DISTINCT 1 $3,ITEM_ID $4,O.PRICE $2 FROM order O ) $1
     *    ON ((I.ITEM_ID = $1.$4 AND $1.$2 = I.PRICE))
     *
     * 2.If the {@link InParseNode} is the ORed part of the where clause,then we would rewrite the In Subquery to
     *   Left Join.
     *
     *   For  NonCorrelated subquery, an example is:
     *    SELECT item_id, name FROM item i WHERE i.item_id IN
     *    (SELECT max(item_id) FROM order o  where o.price > 8 group by o.customer_id,o.item_id) or i.discount1 > 10
     *
     *    The above sql would be rewritten as:
     *    SELECT ITEM_ID,NAME FROM item I  Left JOIN
     *    (SELECT DISTINCT 1 $56, MAX(ITEM_ID) $57 FROM order O  WHERE O.PRICE > 8 GROUP BY O.CUSTOMER_ID,O.ITEM_ID) $55
     *    ON (I.ITEM_ID = $55.$57) WHERE ($55.$56 IS NOT NULL  OR I.DISCOUNT1 > 10)
     *
     *   For  Correlated subquery, an example is:
     *     SELECT item_id, name FROM item i WHERE i.item_id IN
     *     (SELECT max(item_id) FROM order o  where o.price = i.price group by o.customer_id) or i.discount1 > 10;
     *
     *     The above sql would be rewritten as:
     *     SELECT ITEM_ID,NAME FROM item I  Left JOIN
     *     (SELECT DISTINCT 1 $28, MAX(ITEM_ID) $29,O.PRICE $27 FROM order O  GROUP BY O.PRICE,O.CUSTOMER_ID) $26
     *     ON ((I.ITEM_ID = $26.$29 AND $26.$27 = I.PRICE)) WHERE ($26.$28 IS NOT NULL  OR I.DISCOUNT1 > 10)
     * }
     * </pre>
     */
    @Override
    public ParseNode visitLeave(InParseNode inParseNode, List<ParseNode> childParseNodes) throws SQLException {
        boolean isTopNode = topNode == inParseNode;
        if (isTopNode) {
            topNode = null;
        }

        SubqueryParseNode subqueryParseNode = (SubqueryParseNode) childParseNodes.get(1);
        SelectStatement subquerySelectStatementToUse = fixSubqueryStatement(subqueryParseNode.getSelectNode());
        String subqueryTableTempAlias = ParseNodeFactory.createTempAlias();

        JoinConditionExtractor joinConditionExtractor = new JoinConditionExtractor(
                subquerySelectStatementToUse,
                columnResolver,
                connection,
                subqueryTableTempAlias);

        List<AliasedNode> newSubquerySelectAliasedNodes = null;
        ParseNode extractedJoinConditionParseNode = null;
        int extractedSelectAliasNodeCount = 0;
        List<AliasedNode> oldSubqueryAliasedNodes = subquerySelectStatementToUse.getSelect();
        ParseNode whereParseNodeAfterExtract =
                subquerySelectStatementToUse.getWhere() == null ?
                null :
                subquerySelectStatementToUse.getWhere().accept(joinConditionExtractor);
        if (whereParseNodeAfterExtract == subquerySelectStatementToUse.getWhere()) {
            /**
             * It is an NonCorrelated subquery.
             */
            newSubquerySelectAliasedNodes = Lists.<AliasedNode> newArrayListWithExpectedSize(
                    oldSubqueryAliasedNodes.size() + 1);

            newSubquerySelectAliasedNodes.add(
                    NODE_FACTORY.aliasedNode(
                        ParseNodeFactory.createTempAlias(),
                        LiteralParseNode.ONE));
            this.addNewAliasedNodes(newSubquerySelectAliasedNodes, oldSubqueryAliasedNodes);
            subquerySelectStatementToUse = NODE_FACTORY.select(
                    subquerySelectStatementToUse,
                    !inParseNode.isSubqueryDistinct(),
                    newSubquerySelectAliasedNodes,
                    whereParseNodeAfterExtract);
        } else {
            /**
             * It is an Correlated subquery.
             */
            List<AliasedNode> extractedAdditionalSelectAliasNodes =
                    joinConditionExtractor.getAdditionalSubselectSelectAliasedNodes();
            extractedSelectAliasNodeCount = extractedAdditionalSelectAliasNodes.size();
            newSubquerySelectAliasedNodes = Lists.<AliasedNode> newArrayListWithExpectedSize(
                    oldSubqueryAliasedNodes.size() + 1 +
                    extractedAdditionalSelectAliasNodes.size());

            newSubquerySelectAliasedNodes.add(NODE_FACTORY.aliasedNode(
                    ParseNodeFactory.createTempAlias(),
                    LiteralParseNode.ONE));
            this.addNewAliasedNodes(newSubquerySelectAliasedNodes, oldSubqueryAliasedNodes);
            newSubquerySelectAliasedNodes.addAll(extractedAdditionalSelectAliasNodes);
            extractedJoinConditionParseNode =
                joinConditionExtractor.getJoinConditionParseNode();

            boolean isAggregate = subquerySelectStatementToUse.isAggregate();
            if (!isAggregate) {
                subquerySelectStatementToUse =
                        NODE_FACTORY.select(
                                subquerySelectStatementToUse,
                                !inParseNode.isSubqueryDistinct(),
                                newSubquerySelectAliasedNodes,
                                whereParseNodeAfterExtract);
            } else {
                /**
                 * If exists AggregateFunction,we must add the correlated join condition to both the
                 * groupBy clause and select lists of the subquery.
                 */
                List<ParseNode> newGroupByParseNodes = this.createNewGroupByParseNodes(
                        extractedAdditionalSelectAliasNodes,
                        subquerySelectStatementToUse);

                subquerySelectStatementToUse = NODE_FACTORY.select(
                        subquerySelectStatementToUse,
                        !inParseNode.isSubqueryDistinct(),
                        newSubquerySelectAliasedNodes,
                        whereParseNodeAfterExtract,
                        newGroupByParseNodes,
                        true);
            }
        }

        ParseNode joinOnConditionParseNode = getJoinConditionNodeForInSubquery(
                childParseNodes.get(0),
                newSubquerySelectAliasedNodes,
                subqueryTableTempAlias,
                extractedJoinConditionParseNode,
                extractedSelectAliasNodeCount);
        DerivedTableNode subqueryDerivedTableNode = NODE_FACTORY.derivedTable(
                subqueryTableTempAlias,
                subquerySelectStatementToUse);
        JoinType joinType = isTopNode ?
                (inParseNode.isNegate() ? JoinType.Anti : JoinType.Semi) :
                JoinType.Left;
        ParseNode resultWhereParseNode = isTopNode ?
                null :
                NODE_FACTORY.isNull(
                        NODE_FACTORY.column(
                                NODE_FACTORY.table(null, subqueryTableTempAlias),
                                newSubquerySelectAliasedNodes.get(0).getAlias(),
                                null),
                        !inParseNode.isNegate());
        tableNode = NODE_FACTORY.join(
                joinType,
                tableNode,
                subqueryDerivedTableNode,
                joinOnConditionParseNode,
                false);

        return resultWhereParseNode;
    }

    /**
     * <pre>
     * {@code
     * Rewrite the Exists Subquery to semi/anti/left join for both NonCorrelated and Correlated subquery.
     *
     * 1.If the {@link ExistsParseNode} is NonCorrelated subquery,the just add LIMIT 1.
     *    an example is:
     *    SELECT item_id, name FROM item i WHERE exists
     *    (SELECT 1 FROM order o  where o.price > 8)
     *
     *    The above sql would be rewritten as:
     *    SELECT ITEM_ID,NAME FROM item I  WHERE  EXISTS
     *    (SELECT 1 FROM ORDER_TABLE O  WHERE O.PRICE > 8 LIMIT 1)
     *
     *   another example is:
     *   SELECT item_id, name FROM item i WHERE exists
     *   (SELECT 1 FROM order o  where o.price > 8 group by o.customer_id,o.item_id having count(order_id) > 1)
     *    or i.discount1 > 10
     *
     *    The above sql would be rewritten as:
     *    SELECT ITEM_ID,NAME FROM item I  WHERE
     *    ( EXISTS (SELECT 1 FROM ORDER_TABLE O  WHERE O.PRICE > 8 GROUP BY O.CUSTOMER_ID,O.ITEM_ID HAVING  COUNT(ORDER_ID) > 1 LIMIT 1)
     *    OR I.DISCOUNT1 > 10)
     *
     * 2.If the {@link ExistsParseNode} is Correlated subquery and is the only node in where clause or
     *   is the ANDed part of the where clause, then we would rewrite the Exists Subquery to semi/anti join:
     *   an example is:
     *    SELECT item_id, name FROM item i WHERE exists
     *    (SELECT 1 FROM order o where o.price = i.price and o.quantity = 5 )
     *
     *    The above sql would be rewritten as:
     *    SELECT ITEM_ID,NAME FROM item I  Semi JOIN
     *    (SELECT DISTINCT 1 $3,O.PRICE $2 FROM ORDER_TABLE O  WHERE O.QUANTITY = 5) $1
     *    ON ($1.$2 = I.PRICE)
     *
     *   another example with AggregateFunction and groupBy is
     *   SELECT item_id, name FROM item i WHERE exists
     *   (SELECT 1 FROM order o  where o.item_id = i.item_id group by customer_id having count(order_id) > 1)
     *
     *    The above sql would be rewritten as:
     *     SELECT ITEM_ID,NAME FROM item I  Semi JOIN
     *     (SELECT DISTINCT 1 $3,O.ITEM_ID $2 FROM order O  GROUP BY O.ITEM_ID,CUSTOMER_ID HAVING  COUNT(ORDER_ID) > 1) $1
     *     ON ($1.$2 = I.ITEM_ID)
     *
     * 3.If the {@link ExistsParseNode} is Correlated subquery and is the ORed part of the where clause,
     *   then we would rewrite the Exists Subquery to Left Join.
     *   an example is:
     *   SELECT item_id, name FROM item i WHERE exists
     *   (SELECT 1 FROM order o  where o.item_id = i.item_id group by customer_id having count(order_id) > 1)
     *   or i.discount1 > 10
     *
     *    The above sql would be rewritten as:
     *    SELECT ITEM_ID,NAME FROM item I  Left JOIN
     *    (SELECT DISTINCT 1 $3,O.ITEM_ID $2 FROM order O  GROUP BY O.ITEM_ID,CUSTOMER_ID HAVING  COUNT(ORDER_ID) > 1) $1
     *    ON ($1.$2 = I.ITEM_ID) WHERE ($1.$3 IS NOT NULL  OR I.DISCOUNT1 > 10)
     * }
     * </pre>
     */
    @Override
    public ParseNode visitLeave(
        ExistsParseNode existsParseNode,
        List<ParseNode> childParseNodes) throws SQLException {

        boolean isTopNode = topNode == existsParseNode;
        if (isTopNode) {
            topNode = null;
        }
        
        SubqueryParseNode subqueryParseNode = (SubqueryParseNode) childParseNodes.get(0);
        SelectStatement subquerySelectStatementToUse =
                fixSubqueryStatement(subqueryParseNode.getSelectNode());
        String subqueryTableTempAlias = ParseNodeFactory.createTempAlias();
        JoinConditionExtractor joinConditionExtractor =
                new JoinConditionExtractor(
                        subquerySelectStatementToUse,
                        columnResolver,
                        connection,
                        subqueryTableTempAlias);
        ParseNode whereParseNodeAfterExtract =
                subquerySelectStatementToUse.getWhere() == null ?
                null :
                subquerySelectStatementToUse.getWhere().accept(joinConditionExtractor);
        if (whereParseNodeAfterExtract == subquerySelectStatementToUse.getWhere()) {
            /**
             * It is non-correlated EXISTS subquery, add LIMIT 1
             */
            subquerySelectStatementToUse =
                    NODE_FACTORY.select(
                            subquerySelectStatementToUse,
                            NODE_FACTORY.limit(NODE_FACTORY.literal(1)));
            subqueryParseNode = NODE_FACTORY.subquery(subquerySelectStatementToUse, false);
            existsParseNode = NODE_FACTORY.exists(subqueryParseNode, existsParseNode.isNegate());
            return super.visitLeave(
                    existsParseNode,
                    Collections.<ParseNode>singletonList(subqueryParseNode));
        }

        List<AliasedNode> extractedAdditionalSelectAliasNodes =
                joinConditionExtractor.getAdditionalSubselectSelectAliasedNodes();
        List<AliasedNode> newSubquerySelectAliasedNodes = Lists.newArrayListWithExpectedSize(
                extractedAdditionalSelectAliasNodes.size() + 1);
        /**
         * Just overwrite original subquery selectAliasNodes.
         */
        newSubquerySelectAliasedNodes.add(
                NODE_FACTORY.aliasedNode(ParseNodeFactory.createTempAlias(), LiteralParseNode.ONE));
        newSubquerySelectAliasedNodes.addAll(extractedAdditionalSelectAliasNodes);
        
        boolean isAggregate = subquerySelectStatementToUse.isAggregate();
        if (!isAggregate) {
            subquerySelectStatementToUse = NODE_FACTORY.select(
                    subquerySelectStatementToUse,
                    true,
                    newSubquerySelectAliasedNodes,
                    whereParseNodeAfterExtract);
        } else {
            /**
             * If exists AggregateFunction,we must add the correlated join condition to both the
             * groupBy clause and select lists of the subquery.
             */
            List<ParseNode> newGroupByParseNodes = this.createNewGroupByParseNodes(
                    extractedAdditionalSelectAliasNodes,
                    subquerySelectStatementToUse);

            subquerySelectStatementToUse = NODE_FACTORY.select(
                    subquerySelectStatementToUse,
                    true,
                    newSubquerySelectAliasedNodes,
                    whereParseNodeAfterExtract,
                    newGroupByParseNodes,
                    true);
        }
        ParseNode joinOnConditionParseNode = joinConditionExtractor.getJoinConditionParseNode();
        DerivedTableNode subqueryDerivedTableNode = NODE_FACTORY.derivedTable(
                subqueryTableTempAlias,
                subquerySelectStatementToUse);
        JoinType joinType = isTopNode ?
                (existsParseNode.isNegate() ? JoinType.Anti : JoinType.Semi) :
                 JoinType.Left;
        ParseNode resultWhereParseNode = isTopNode ?
                        null :
                        NODE_FACTORY.isNull(
                                NODE_FACTORY.column(
                                        NODE_FACTORY.table(null, subqueryTableTempAlias),
                                        newSubquerySelectAliasedNodes.get(0).getAlias(),
                                        null),
                                !existsParseNode.isNegate());
        tableNode = NODE_FACTORY.join(
                joinType,
                tableNode,
                subqueryDerivedTableNode,
                joinOnConditionParseNode,
                false);
        return resultWhereParseNode;
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
        JoinConditionExtractor conditionExtractor = new JoinConditionExtractor(subquery, columnResolver, connection, rhsTableAlias);
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
        
        List<AliasedNode> additionalSelectNodes =
            conditionExtractor.getAdditionalSubselectSelectAliasedNodes();
        List<AliasedNode> selectNodes =
            Lists.newArrayListWithExpectedSize(additionalSelectNodes.size() + 1);
        selectNodes.add(NODE_FACTORY.aliasedNode(ParseNodeFactory.createTempAlias(), rhsNode));
        selectNodes.addAll(additionalSelectNodes);
        
        if (!isAggregate) {
            subquery = NODE_FACTORY.select(subquery, subquery.isDistinct(), selectNodes, where);            
        } else {
            List<ParseNode> groupbyNodes =  this.createNewGroupByParseNodes(
                    additionalSelectNodes,
                    subquery);
            subquery = NODE_FACTORY.select(subquery, subquery.isDistinct(), selectNodes, where, groupbyNodes, true);
        }
        
        ParseNode onNode = conditionExtractor.getJoinConditionParseNode();
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
        JoinConditionExtractor conditionExtractor = new JoinConditionExtractor(subquery, columnResolver, connection, rhsTableAlias);
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
            aliasedNodes = createNewAliasedNodes(aliasedNodes);
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
        
        List<AliasedNode> additionalSelectNodes = conditionExtractor.getAdditionalSubselectSelectAliasedNodes();
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
            subquery = NODE_FACTORY.select(NODE_FACTORY.derivedTable(derivedTableAlias, derivedTableStmt),
                    subquery.getHint(), false, selectNodes, null, groupbyNodes, null,
                    Collections.<OrderByNode> emptyList(), null, null, subquery.getBindCount(), true, false,
                    Collections.<SelectStatement> emptyList(), subquery.getUdfParseNodes());
        }
        
        ParseNode onNode = conditionExtractor.getJoinConditionParseNode();
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
        return NODE_FACTORY.select(NODE_FACTORY.derivedTable(ParseNodeFactory.createTempAlias(), select),
                HintNode.EMPTY_HINT_NODE, false, select.getSelect(), null, null, null, null, null, null,
                select.getBindCount(), false, false, Collections.<SelectStatement> emptyList(),
                select.getUdfParseNodes());
    }

    /**
     * Create new {@link AliasedNode}s by every {@link ParseNode} in subquerySelectAliasedNodes and generate new aliases
     * by {@link ParseNodeFactory#createTempAlias}.
     * and generate new Aliases for subquerySelectAliasedNodes,
     * @param subquerySelectAliasedNodes
     * @param addSelectOne
     * @return
     */
    private List<AliasedNode> createNewAliasedNodes(List<AliasedNode> subquerySelectAliasedNodes) {
        List<AliasedNode> newAliasedNodes = Lists.<AliasedNode> newArrayListWithExpectedSize(
                subquerySelectAliasedNodes.size());

        this.addNewAliasedNodes(newAliasedNodes, subquerySelectAliasedNodes);
        return newAliasedNodes;
    }

    /**
     * Add every {@link ParseNode} in oldSelectAliasedNodes to newSelectAliasedNodes and generate new aliases by
     * {@link ParseNodeFactory#createTempAlias}.
     * @param oldSelectAliasedNodes
     * @param addSelectOne
     * @return
     */
    private void addNewAliasedNodes(List<AliasedNode> newSelectAliasedNodes, List<AliasedNode> oldSelectAliasedNodes) {
        for (int index = 0; index < oldSelectAliasedNodes.size(); index++) {
            AliasedNode oldSelectAliasedNode = oldSelectAliasedNodes.get(index);
            newSelectAliasedNodes.add(NODE_FACTORY.aliasedNode(
                    ParseNodeFactory.createTempAlias(),
                    oldSelectAliasedNode.getNode()));
        }
        
    }

    /**
     * Get the join conditions in order to rewrite InSubquery to Join.
     * @param lhsParseNode
     * @param rhsSubquerySelectAliasedNodes the first element is {@link LiteralParseNode#ONE}.
     * @param rhsSubqueryTableAlias
     * @param extractedJoinConditionParseNode For NonCorrelated subquery, it is null.
     * @param extractedSelectAliasNodeCount For NonCorrelated subquery, it is 0.
     * @throws SQLException
     */
    private ParseNode getJoinConditionNodeForInSubquery(
            ParseNode lhsParseNode,
            List<AliasedNode> rhsSubquerySelectAliasedNodes,
            String rhsSubqueryTableAlias,
            ParseNode extractedJoinConditionParseNode,
            int extractedSelectAliasNodeCount) throws SQLException {
        List<ParseNode> lhsParseNodes;
        if (lhsParseNode instanceof RowValueConstructorParseNode) {
            lhsParseNodes = ((RowValueConstructorParseNode) lhsParseNode).getChildren();
        } else {
            lhsParseNodes = Collections.singletonList(lhsParseNode);
        }

        if (lhsParseNodes.size() !=
                (rhsSubquerySelectAliasedNodes.size() - 1 - extractedSelectAliasNodeCount)) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.SUBQUERY_RETURNS_DIFFERENT_NUMBER_OF_FIELDS)
                    .build().buildException();
        }

        int count = lhsParseNodes.size();
        TableName rhsSubqueryTableName = NODE_FACTORY.table(null, rhsSubqueryTableAlias);
        List<ParseNode> joinEqualParseNodes = Lists.newArrayListWithExpectedSize(
                count + (extractedJoinConditionParseNode == null ? 0: 1));
        for (int index = 0; index < count; index++) {
            /**
             * The +1 is to skip the first {@link LiteralParseNode#ONE}
             */
            ParseNode rhsNode = NODE_FACTORY.column(
                    rhsSubqueryTableName,
                    rhsSubquerySelectAliasedNodes.get(index + 1).getAlias(),
                    null);
            joinEqualParseNodes.add(NODE_FACTORY.equal(lhsParseNodes.get(index), rhsNode));
        }

        if(extractedJoinConditionParseNode != null) {
            joinEqualParseNodes.add(extractedJoinConditionParseNode);
        }

        return joinEqualParseNodes.size() == 1 ? joinEqualParseNodes.get(0) : NODE_FACTORY.and(joinEqualParseNodes);
    }
    
    /**
     * Combine every {@link ParseNode} in  extractedAdditionalSelectAliasNodes and GroupBy clause of the
     * subquerySelectStatementToUse to get new GroupBy ParseNodes.
     * @param extractedAdditionalSelectAliasNodes
     * @param subquerySelectStatementToUse
     * @return
     */
    private List<ParseNode> createNewGroupByParseNodes(
            List<AliasedNode> extractedAdditionalSelectAliasNodes,
            SelectStatement subquerySelectStatementToUse) {
        List<ParseNode> newGroupByParseNodes = Lists.newArrayListWithExpectedSize(
                extractedAdditionalSelectAliasNodes.size() +
                subquerySelectStatementToUse.getGroupBy().size());

        for (AliasedNode aliasedNode : extractedAdditionalSelectAliasNodes) {
            newGroupByParseNodes.add(aliasedNode.getNode());
        }
        newGroupByParseNodes.addAll(subquerySelectStatementToUse.getGroupBy());
        return newGroupByParseNodes;
    }

    private static class JoinConditionExtractor extends AndRewriterBooleanParseNodeVisitor {
        private final TableName tableName;
        private ColumnResolveVisitor columnResolveVisitor;
        private List<AliasedNode> additionalSubselectSelectAliasedNodes;
        private List<ParseNode> joinConditionParseNodes;
        
        public JoinConditionExtractor(SelectStatement subquery, ColumnResolver outerResolver, 
                PhoenixConnection connection, String tableAlias) throws SQLException {
            super(NODE_FACTORY);
            this.tableName = NODE_FACTORY.table(null, tableAlias);
            ColumnResolver localResolver = FromCompiler.getResolverForQuery(subquery, connection);
            this.columnResolveVisitor = new ColumnResolveVisitor(localResolver, outerResolver);
            this.additionalSubselectSelectAliasedNodes = Lists.<AliasedNode>newArrayList();
            this.joinConditionParseNodes = Lists.<ParseNode>newArrayList();
        }
        
        public List<AliasedNode> getAdditionalSubselectSelectAliasedNodes() {
            return this.additionalSubselectSelectAliasedNodes;
        }
        
        public ParseNode getJoinConditionParseNode() {
            if (this.joinConditionParseNodes.isEmpty()) {
                return null;
            }

            if (this.joinConditionParseNodes.size() == 1) {
                return this.joinConditionParseNodes.get(0);
            }

            return NODE_FACTORY.and(this.joinConditionParseNodes);
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
        public ParseNode visitLeave(ComparisonParseNode node, List<ParseNode> l) throws SQLException {
            if (node.getFilterOp() != CompareOperator.EQUAL)
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
                this.additionalSubselectSelectAliasedNodes.add(
                  NODE_FACTORY.aliasedNode(alias, node.getLHS()));
                ParseNode lhsNode = NODE_FACTORY.column(tableName, alias, null);
                this.joinConditionParseNodes.add(NODE_FACTORY.equal(lhsNode, node.getRHS()));
                return null;
            }        
            if (lhsType == ColumnResolveVisitor.ColumnResolveType.OUTER && rhsType == ColumnResolveVisitor.ColumnResolveType.LOCAL) {
                String alias = ParseNodeFactory.createTempAlias();
                this.additionalSubselectSelectAliasedNodes.add(
                  NODE_FACTORY.aliasedNode(alias, node.getRHS()));
                ParseNode rhsNode = NODE_FACTORY.column(tableName, alias, null);
                this.joinConditionParseNodes.add(NODE_FACTORY.equal(node.getLHS(), rhsNode));
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
            } catch (TableNotFoundException e) {
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
