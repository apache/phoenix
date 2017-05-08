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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.parse.AliasedNode;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.DerivedTableNode;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.parse.LimitNode;
import org.apache.phoenix.parse.OffsetNode;
import org.apache.phoenix.parse.OrderByNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeRewriter;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.TableNode;
import org.apache.phoenix.parse.TableWildcardParseNode;
import org.apache.phoenix.parse.WildcardParseNode;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.collect.Lists;

public class SubselectRewriter extends ParseNodeRewriter {
    private final String tableAlias;
    private final Map<String, ParseNode> aliasMap;
    private boolean removeAlias = false;
    
    public static SelectStatement applyPostFilters(SelectStatement statement, List<ParseNode> postFilters, String subqueryAlias) throws SQLException {
        if (postFilters.isEmpty())
            return statement;
        
        assert(isPostFilterConvertible(statement));
        
        return new SubselectRewriter(null, statement.getSelect(), subqueryAlias).applyPostFilters(statement, postFilters);
    }
    
    public static boolean isPostFilterConvertible(SelectStatement statement) throws SQLException {
        return statement.getLimit() == null && (!statement.isAggregate() || !statement.getGroupBy().isEmpty());        
    }
    
    public static SelectStatement applyOrderBy(SelectStatement statement, List<OrderByNode> orderBy, String subqueryAlias,TableNode tableNode) throws SQLException {
        if (orderBy == null)
            return statement;
        
        return new SubselectRewriter(null, statement.getSelect(), subqueryAlias).applyOrderBy(statement, orderBy, tableNode);
    }
    
    public static SelectStatement flatten(SelectStatement select, PhoenixConnection connection) throws SQLException {
        TableNode from = select.getFrom();
        while (from != null && from instanceof DerivedTableNode) {
            DerivedTableNode derivedTable = (DerivedTableNode) from;
            SelectStatement subselect = derivedTable.getSelect();
            if (subselect.isUnion())
                break;
            ColumnResolver resolver = FromCompiler.getResolverForQuery(subselect, connection);
            SubselectRewriter rewriter = new SubselectRewriter(resolver, subselect.getSelect(), derivedTable.getAlias());
            SelectStatement ret = rewriter.flatten(select, subselect);
            if (ret == select)
                break;
            
            select = ret;
            from = select.getFrom();
        }
        
        return select;
    }
    
    private SubselectRewriter(ColumnResolver resolver, List<AliasedNode> aliasedNodes, String tableAlias) {
        super(resolver, aliasedNodes.size());
        this.tableAlias = tableAlias;
        this.aliasMap = new HashMap<String, ParseNode>();
        for (AliasedNode aliasedNode : aliasedNodes) {
            String alias = aliasedNode.getAlias();
            ParseNode node = aliasedNode.getNode();
            if (alias == null) {
                alias = SchemaUtil.normalizeIdentifier(node.getAlias());
            }
            if (alias != null) {
                aliasMap.put(SchemaUtil.getColumnName(tableAlias, alias), node);
            }
        }
    }
    
    private SelectStatement flatten(SelectStatement select, SelectStatement subselect) throws SQLException {
        // Replace aliases in sub-select first.
        subselect = ParseNodeRewriter.rewrite(subselect, this);
        
        ParseNode whereRewrite = subselect.getWhere();
        List<ParseNode> groupByRewrite = subselect.getGroupBy();
        ParseNode havingRewrite = subselect.getHaving();
        List<OrderByNode> orderByRewrite = subselect.getOrderBy();
        LimitNode limitRewrite = subselect.getLimit();
        OffsetNode offsetRewrite = subselect.getOffset();
        HintNode hintRewrite = subselect.getHint();
        boolean isDistinctRewrite = subselect.isDistinct();
        boolean isAggregateRewrite = subselect.isAggregate();
        
        ParseNode where = select.getWhere();
        if (where != null) {
            if (subselect.getLimit() != null || (subselect.isAggregate() && subselect.getGroupBy().isEmpty())) {
                return select;
            }
            ParseNode postFilter = where.accept(this);
            if (subselect.getGroupBy().isEmpty()) {
                whereRewrite = whereRewrite == null ? postFilter : NODE_FACTORY.and(Arrays.<ParseNode>asList(whereRewrite, postFilter));
            } else {
                havingRewrite = havingRewrite == null ? postFilter : NODE_FACTORY.and(Arrays.<ParseNode>asList(havingRewrite, postFilter));
            }
        }
        
        if (select.isDistinct()) {
            if (subselect.getLimit() != null || subselect.isAggregate() || subselect.isDistinct()) {
                return select;
            }
            isDistinctRewrite = true;
            orderByRewrite = null;
        }
        
        if (select.isAggregate()) {
            if (subselect.getLimit() != null || subselect.isAggregate() || subselect.isDistinct()) {
                return select;
            }
            isAggregateRewrite = true;
            orderByRewrite = null;
        }
        
        List<ParseNode> groupBy = select.getGroupBy();
        if (!groupBy.isEmpty()) {
            if (subselect.getLimit() != null || subselect.isAggregate() || subselect.isDistinct()) {
                return select;
            }
            groupByRewrite = Lists.<ParseNode>newArrayListWithExpectedSize(groupBy.size());
            for (ParseNode node : groupBy) {
                groupByRewrite.add(node.accept(this));
            }
            if (select.getHaving() != null) {
                havingRewrite = select.getHaving().accept(this);
            }
            orderByRewrite = null;
        }
        
        List<AliasedNode> selectNodes = select.getSelect();
        List<AliasedNode> selectNodesRewrite = Lists.newArrayListWithExpectedSize(selectNodes.size());
        for (AliasedNode aliasedNode : selectNodes) {
            ParseNode node = aliasedNode.getNode();
            if (node instanceof WildcardParseNode 
                    || (node instanceof TableWildcardParseNode 
                            && ((TableWildcardParseNode) node).getTableName().equals(tableAlias))) {
                for (AliasedNode aNode : subselect.getSelect()) {
                    String alias = aNode.getAlias();
                    String aliasRewrite = alias == null ? null : SchemaUtil.getColumnName(tableAlias, alias);
                    selectNodesRewrite.add(NODE_FACTORY.aliasedNode(aliasRewrite, aNode.getNode()));
                }                
            } else {
                selectNodesRewrite.add(NODE_FACTORY.aliasedNode(aliasedNode.getAlias(), node.accept(this)));
            }
        }
        
        List<OrderByNode> orderBy = select.getOrderBy();
        if (!orderBy.isEmpty()) {
            if (subselect.getLimit() != null) {
                return select;
            }
            orderByRewrite = Lists.newArrayListWithExpectedSize(orderBy.size());
            for (OrderByNode orderByNode : orderBy) {
                ParseNode node = orderByNode.getNode();
                orderByRewrite.add(NODE_FACTORY.orderBy(node.accept(this), orderByNode.isNullsLast(), orderByNode.isAscending()));
            }
        }
        
        OffsetNode offset = select.getOffset();
        if (offsetRewrite != null || (limitRewrite != null && offset != null)) {
            return select;
        } else {
            offsetRewrite = offset;
        }
        
        LimitNode limit = select.getLimit();
        if (limit != null) {
            if (limitRewrite == null) {
                limitRewrite = limit;
            } else {
                Integer limitValue = LimitCompiler.compile(null, select);
                Integer limitValueSubselect = LimitCompiler.compile(null, subselect);
                if (limitValue != null && limitValueSubselect != null) {
                    limitRewrite = limitValue < limitValueSubselect ? limit : limitRewrite;
                } else {
                    return select;
                }
            }
        }

        HintNode hint = select.getHint();
        if (hint != null) {
            hintRewrite = hintRewrite == null ? hint : HintNode.combine(hint, hintRewrite);
        }
        
        SelectStatement stmt = NODE_FACTORY.select(subselect.getFrom(), hintRewrite, isDistinctRewrite,
                selectNodesRewrite, whereRewrite, groupByRewrite, havingRewrite, orderByRewrite, limitRewrite,
                offsetRewrite, select.getBindCount(), isAggregateRewrite, select.hasSequence(), select.getSelects(),
                select.getUdfParseNodes());
        if (tableAlias != null) {
            this.removeAlias = true;
            stmt = ParseNodeRewriter.rewrite(stmt, this);
        }
        return stmt;
    }
    
    private SelectStatement applyPostFilters(SelectStatement statement, List<ParseNode> postFilters) throws SQLException {
        List<ParseNode> postFiltersRewrite = Lists.<ParseNode>newArrayListWithExpectedSize(postFilters.size());
        for (ParseNode node : postFilters) {
            postFiltersRewrite.add(node.accept(this));
        }
        
        if (statement.getGroupBy().isEmpty()) {
            ParseNode where = statement.getWhere();
            if (where != null) {
                postFiltersRewrite.add(where);
            }
            return NODE_FACTORY.select(statement, combine(postFiltersRewrite));
        }
        
        ParseNode having = statement.getHaving();
        if (having != null) {
            postFiltersRewrite.add(having);
        }
        return NODE_FACTORY.select(statement, statement.getWhere(), combine(postFiltersRewrite));
    }

    private SelectStatement applyOrderBy(SelectStatement subselectStatement,List<OrderByNode> newOrderByNodes, TableNode subselectAsTableNode) throws SQLException {
        ArrayList<OrderByNode> rewrittenNewOrderByNodes = Lists.<OrderByNode> newArrayListWithExpectedSize(newOrderByNodes.size());
        for (OrderByNode newOrderByNode : newOrderByNodes) {
            ParseNode parseNode = newOrderByNode.getNode();
            rewrittenNewOrderByNodes.add(NODE_FACTORY.orderBy(
                    parseNode.accept(this),
                    newOrderByNode.isNullsLast(),
                    newOrderByNode.isAscending()));
        }

        // in these case,we can safely override subselect's orderBy
        if(subselectStatement.getLimit()==null ||
           subselectStatement.getOrderBy() == null ||
           subselectStatement.getOrderBy().isEmpty()) {
            return NODE_FACTORY.select(subselectStatement, rewrittenNewOrderByNodes);
        }

        //if rewrittenNewOrderByNodes is prefix of subselectStatement's orderBy,
        //then subselectStatement no need to modify
        if(this.isOrderByPrefix(subselectStatement, rewrittenNewOrderByNodes)) {
            return subselectStatement;
        }

        //modify the subselect "(select id,code from tableName order by code limit 3) as a" to
        //"(select id,code from (select id,code from tableName order by code limit 3) order by id) as a"
        List<AliasedNode> newSelectAliasedNodes = createAliasedNodesFromSubselect(subselectStatement,rewrittenNewOrderByNodes);
        assert subselectAsTableNode instanceof DerivedTableNode;
        //set the subselect alias to null.
        subselectAsTableNode=NODE_FACTORY.derivedTable(null, ((DerivedTableNode)subselectAsTableNode).getSelect());

        return NODE_FACTORY.select(
                subselectAsTableNode,
                HintNode.EMPTY_HINT_NODE,
                false,
                newSelectAliasedNodes,
                null,
                null,
                null,
                rewrittenNewOrderByNodes,
                null,
                null,
                0,
                false,
                subselectStatement.hasSequence(),
                Collections.<SelectStatement> emptyList(),
                subselectStatement.getUdfParseNodes());
    }

    /**
     * create new aliasedNodes from subSelectStatement's select alias.
     * @param subSelectStatement
     * @param rewrittenOrderByNodes
     * @return
     */
    private List<AliasedNode> createAliasedNodesFromSubselect(SelectStatement subSelectStatement,ArrayList<OrderByNode> rewrittenOrderByNodes) throws SQLException {
        List<AliasedNode> selectAliasedNodes=subSelectStatement.getSelect();
        List<AliasedNode> newSelectAliasedNodes = new ArrayList<AliasedNode>(selectAliasedNodes.size());
        Map<ParseNode,Integer> rewrittenOrderByParseNodeToIndex=new HashMap<ParseNode, Integer>(rewrittenOrderByNodes.size());
        for(int index=0;index < rewrittenOrderByNodes.size();index++) {
            OrderByNode rewrittenOrderByNode=rewrittenOrderByNodes.get(index);
            rewrittenOrderByParseNodeToIndex.put(rewrittenOrderByNode.getNode(), Integer.valueOf(index));
        }

        for (AliasedNode selectAliasedNode : selectAliasedNodes) {
            String selectAliasName = selectAliasedNode.getAlias();
            ParseNode oldSelectAliasParseNode = selectAliasedNode.getNode();
            if (selectAliasName == null) {
                selectAliasName = SchemaUtil.normalizeIdentifier(oldSelectAliasParseNode.getAlias());
            }
            //in order to convert the subselect "select id,sum(code) codesum from table group by id order by codesum limit 3"
            //to "select id,codesum from (select id,sum(code) codesum from table group by id order by codesum limit 3) order by id"
            //we must has alias for sum(code)
            if(selectAliasName== null) {
                 throw new SQLExceptionInfo.Builder(SQLExceptionCode.SUBQUERY_SELECT_LIST_COLUMN_MUST_HAS_ALIAS)
                 .setMessage("the subquery is:"+subSelectStatement)
                 .build()
                 .buildException();
            }

            ColumnParseNode newColumnParseNode=NODE_FACTORY.column(null, selectAliasName, selectAliasName);
            Integer index=rewrittenOrderByParseNodeToIndex.get(oldSelectAliasParseNode);
            if(index !=null) {
                //replace the rewrittenOrderByNode's child to newColumnParseNode
                OrderByNode oldOrderByNode=rewrittenOrderByNodes.get(index);
                rewrittenOrderByNodes.set(index,
                        NODE_FACTORY.orderBy(
                                newColumnParseNode,
                                oldOrderByNode.isNullsLast(),
                                oldOrderByNode.isAscending()));
            }

            AliasedNode newSelectAliasNode=NODE_FACTORY.aliasedNode(null,newColumnParseNode);
            newSelectAliasedNodes.add(newSelectAliasNode);
        }
        return newSelectAliasedNodes;
    }

    /**
     * check if rewrittenNewOrderByNodes is prefix of selectStatement's order by.
     * @param selectStatement
     * @param rewrittenNewOrderByNodes
     * @return
     */
    private boolean isOrderByPrefix(SelectStatement selectStatement,List<OrderByNode> rewrittenNewOrderByNodes) {
        List<OrderByNode> existingOrderByNodes=selectStatement.getOrderBy();
        if(rewrittenNewOrderByNodes.size() > existingOrderByNodes.size()) {
            return false;
        }

        Iterator<OrderByNode> existingOrderByNodeIter=existingOrderByNodes.iterator();
        for(OrderByNode rewrittenNewOrderByNode : rewrittenNewOrderByNodes) {
            assert existingOrderByNodeIter.hasNext();
            OrderByNode existingOrderByNode=existingOrderByNodeIter.next();
            if(!existingOrderByNode.equals(rewrittenNewOrderByNode)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public ParseNode visit(ColumnParseNode node) throws SQLException {
        if (node.getTableName() == null)
            return super.visit(node);
        
        if (removeAlias) {
            if (node.getTableName().equals(tableAlias)) {
                return NODE_FACTORY.column(null, node.getName(), node.getAlias());
            }
            return super.visit(node);
        }
        
        ParseNode aliasedNode = aliasMap.get(node.getFullName());
        if (aliasedNode != null) {
            return aliasedNode;
        }
        return node;
    }        
    
    private static ParseNode combine(List<ParseNode> nodes) {
        if (nodes.isEmpty())
            return null;
        
        if (nodes.size() == 1)
            return nodes.get(0);
        
        return NODE_FACTORY.and(nodes);
    }
}
