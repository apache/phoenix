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

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.parse.AliasedNode;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.DerivedTableNode;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.parse.LimitNode;
import org.apache.phoenix.parse.OffsetNode;
import org.apache.phoenix.parse.OrderByNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.ParseNodeRewriter;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.TableNode;
import org.apache.phoenix.parse.TableWildcardParseNode;
import org.apache.phoenix.parse.WildcardParseNode;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.collect.Lists;

/*
 * Class for flattening derived-tables when possible. A derived-table can be
 * flattened if the merged statement preserves the same semantics as the original
 * statement.
 */
public class SubselectRewriter extends ParseNodeRewriter {
    private final String tableAlias;
    private final Map<String, ParseNode> aliasMap;
    private boolean removeAlias = false;
    
    public static SelectStatement applyPreFiltersForSubselect(SelectStatement statement, List<ParseNode> preFilterParseNodes, String subqueryAlias) throws SQLException {
        if (preFilterParseNodes.isEmpty())
            return statement;
        
        assert(isPostFilterConvertible(statement));
        
        return new SubselectRewriter(null, statement.getSelect(), subqueryAlias).applyPreFilters(statement, preFilterParseNodes);
    }
    
    public static boolean isPostFilterConvertible(SelectStatement statement) throws SQLException {
        return statement.getLimit() == null && (!statement.isAggregate() || !statement.getGroupBy().isEmpty());        
    }
    
    /**
     * <pre>
     * only append orderByNodes and postFilters, the optimization is left to {@link #flatten(SelectStatement, SelectStatement)}.
     * an example :
     * when the subselectStatment is : (SELECT reverse(loc_id), \"supplier_id\", name FROM " + JOIN_SUPPLIER_TABLE + " LIMIT 5) AS supp
     * orderByNodes is  : supp.\"supplier_id\"
     * postFilterParseNodes is : supp.name != 'S1'
     * we rewrite the subselectStatment as :
     * (SELECT $2.$3,$2."supplier_id",$2.NAME FROM (SELECT  REVERSE(LOC_ID) $3,"supplier_id",NAME FROM SUPPLIERTABLE  LIMIT 5) $2 WHERE $2.NAME != 'S1' ORDER BY $2."supplier_id") AS supp
     *
     * </pre>
     * @param subselectStatement
     * @param orderByNodes
     * @param subselectTableAliasName
     * @param postFilterParseNodes
     * @return
     * @throws SQLException
     */
    public static SelectStatement applyOrderByAndPostFilters(
            SelectStatement subselectStatement,
            List<OrderByNode> orderByNodes,
            String subselectTableAliasName,
            List<ParseNode> postFilterParseNodes) throws SQLException {

       if(orderByNodes == null) {
           orderByNodes = Collections.emptyList();
       }

       if(postFilterParseNodes == null) {
           postFilterParseNodes = Collections.emptyList();
       }

       if(orderByNodes.isEmpty() && postFilterParseNodes.isEmpty()) {
           return subselectStatement;
       }

       List<AliasedNode> subselectAliasedNodes = subselectStatement.getSelect();
       List<AliasedNode> newOuterSelectAliasedNodes = new ArrayList<AliasedNode>(subselectAliasedNodes.size());
       Map<String,ParseNode> subselectAliasFullNameToNewColumnParseNode = new HashMap<String,ParseNode>();

       String newSubselectTableAliasName = ParseNodeFactory.createTempAlias();
       List<AliasedNode> newSubselectAliasedNodes = null;
       int index = 0;
       for (AliasedNode subselectAliasedNode : subselectAliasedNodes) {
           String aliasName = subselectAliasedNode.getAlias();
           ParseNode aliasParseNode = subselectAliasedNode.getNode();
           if (aliasName == null) {
               aliasName = aliasParseNode.getAlias();
           }
           if(aliasName == null) {
               //if there is no alias,we generate a new alias,
               //and added the new alias to the old subselectAliasedNodes
               aliasName = ParseNodeFactory.createTempAlias();
               if(newSubselectAliasedNodes == null) {
                   newSubselectAliasedNodes = new ArrayList<AliasedNode>(subselectAliasedNodes.size());
                   if(index > 0) {
                       newSubselectAliasedNodes.addAll(subselectAliasedNodes.subList(0, index));
                   }
               }
               newSubselectAliasedNodes.add(NODE_FACTORY.aliasedNode(aliasName, aliasParseNode));
           } else {
               if(newSubselectAliasedNodes != null) {
                   newSubselectAliasedNodes.add(subselectAliasedNode);
               }
           }

           ColumnParseNode newColumnParseNode = NODE_FACTORY.column(
                   NODE_FACTORY.table(null, newSubselectTableAliasName),
                   aliasName,
                   aliasName);
           subselectAliasFullNameToNewColumnParseNode.put(
                   SchemaUtil.getColumnName(subselectTableAliasName, SchemaUtil.normalizeIdentifier(aliasName)),
                   newColumnParseNode);
           AliasedNode newOuterSelectAliasNode = NODE_FACTORY.aliasedNode(null, newColumnParseNode);
           newOuterSelectAliasedNodes.add(newOuterSelectAliasNode);
           index++;
       }

       SubselectRewriter rewriter = new SubselectRewriter(subselectAliasFullNameToNewColumnParseNode);
       List<OrderByNode> rewrittenOrderByNodes = null;
       if(orderByNodes.size() > 0) {
           rewrittenOrderByNodes = new ArrayList<OrderByNode>(orderByNodes.size());
           for (OrderByNode orderByNode : orderByNodes) {
               ParseNode parseNode = orderByNode.getNode();
               rewrittenOrderByNodes.add(NODE_FACTORY.orderBy(
                       parseNode.accept(rewriter),
                       orderByNode.isNullsLast(),
                       orderByNode.isAscending()));
           }
       }

       ParseNode newWhereParseNode = null;
       if(postFilterParseNodes.size() > 0) {
           List<ParseNode> rewrittenPostFilterParseNodes =
                   new ArrayList<ParseNode>(postFilterParseNodes.size());
           for(ParseNode postFilterParseNode : postFilterParseNodes) {
               rewrittenPostFilterParseNodes.add(postFilterParseNode.accept(rewriter));
           }
           newWhereParseNode = combine(rewrittenPostFilterParseNodes);
       }

       SelectStatement subselectStatementToUse = subselectStatement;
       if(newSubselectAliasedNodes != null) {
           subselectStatementToUse = NODE_FACTORY.select(subselectStatement, subselectStatement.isDistinct(), newSubselectAliasedNodes);
       }

       return NODE_FACTORY.select(
               NODE_FACTORY.derivedTable(newSubselectTableAliasName, subselectStatementToUse),
               HintNode.EMPTY_HINT_NODE,
               false,
               newOuterSelectAliasedNodes,
               newWhereParseNode,
               null,
               null,
               rewrittenOrderByNodes,
               null,
               null,
               0,
               false,
               subselectStatementToUse.hasSequence(),
               Collections.<SelectStatement> emptyList(),
               subselectStatementToUse.getUdfParseNodes());
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
    
    private SubselectRewriter(Map<String, ParseNode> selectAliasFullNameToAliasParseNode) {
        super(null, selectAliasFullNameToAliasParseNode.size());
        this.tableAlias = null;
        this.aliasMap = selectAliasFullNameToAliasParseNode;
    }

    /**
     * if the OrderBy of outerSelectStatement is prefix of innerSelectStatement,
     * we can remove the OrderBy of outerSelectStatement.
     * @param outerSelectStatement
     * @param innerSelectStatement
     * @return
     * @throws SQLException
     */
    private SelectStatement removeOuterSelectStatementOrderByIfNecessary(
            SelectStatement outerSelectStatement, SelectStatement innerSelectStatement) throws SQLException {
        if(outerSelectStatement.isDistinct() ||
           outerSelectStatement.isAggregate() ||
           (outerSelectStatement.getGroupBy() != null && !outerSelectStatement.getGroupBy().isEmpty()) ||
           outerSelectStatement.isJoin() ||
           outerSelectStatement.isUnion()) {
            return outerSelectStatement;
        }

        List<OrderByNode> outerOrderByNodes = outerSelectStatement.getOrderBy();
        if(outerOrderByNodes == null || outerOrderByNodes.isEmpty()) {
            return outerSelectStatement;
        }

        if(this.isOuterOrderByNodesPrefixOfInner(innerSelectStatement.getOrderBy(), outerOrderByNodes)) {
            return NODE_FACTORY.select(outerSelectStatement, (List<OrderByNode>)null);
        }
        return outerSelectStatement;
    }

    /**
     * check if outerOrderByNodes is prefix of innerOrderByNodes.
     * @param selectStatement
     * @param outerOrderByNodes
     * @return
     */
    private boolean isOuterOrderByNodesPrefixOfInner(
            List<OrderByNode> innerOrderByNodes,
            List<OrderByNode> outerOrderByNodes) throws SQLException {

        assert outerOrderByNodes != null && outerOrderByNodes.size() > 0;

        if(innerOrderByNodes == null || outerOrderByNodes.size() > innerOrderByNodes.size()) {
            return false;
        }

        Iterator<OrderByNode> innerOrderByNodeIter = innerOrderByNodes.iterator();
        for(OrderByNode outerOrderByNode : outerOrderByNodes) {
            ParseNode outerOrderByParseNode = outerOrderByNode.getNode();
            OrderByNode rewrittenOuterOrderByNode = NODE_FACTORY.orderBy(
                    outerOrderByParseNode.accept(this),
                    outerOrderByNode.isNullsLast(),
                    outerOrderByNode.isAscending());
            assert innerOrderByNodeIter.hasNext();
            OrderByNode innerOrderByNode = innerOrderByNodeIter.next();
            if(!innerOrderByNode.equals(rewrittenOuterOrderByNode)) {
                return false;
            }
        }
        return true;
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
                return removeOuterSelectStatementOrderByIfNecessary(select,subselect);
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
                return removeOuterSelectStatementOrderByIfNecessary(select,subselect);
            }
            isDistinctRewrite = true;
            orderByRewrite = null;
        }
        
        if (select.isAggregate()) {
            if (subselect.getLimit() != null || subselect.isAggregate() || subselect.isDistinct()) {
                return removeOuterSelectStatementOrderByIfNecessary(select,subselect);
            }
            isAggregateRewrite = true;
            orderByRewrite = null;
        }
        
        List<ParseNode> groupBy = select.getGroupBy();
        if (!groupBy.isEmpty()) {
            if (subselect.getLimit() != null || subselect.isAggregate() || subselect.isDistinct()) {
                return removeOuterSelectStatementOrderByIfNecessary(select,subselect);
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
                return removeOuterSelectStatementOrderByIfNecessary(select,subselect);
            }
            orderByRewrite = Lists.newArrayListWithExpectedSize(orderBy.size());
            for (OrderByNode orderByNode : orderBy) {
                ParseNode node = orderByNode.getNode();
                orderByRewrite.add(NODE_FACTORY.orderBy(node.accept(this), orderByNode.isNullsLast(), orderByNode.isAscending()));
            }
        }
        
        OffsetNode offset = select.getOffset();
        if (offsetRewrite != null || (limitRewrite != null && offset != null)) {
            return removeOuterSelectStatementOrderByIfNecessary(select,subselect);
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
                    return removeOuterSelectStatementOrderByIfNecessary(select,subselect);
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

    private SelectStatement applyPreFilters(SelectStatement statement, List<ParseNode> preFilterParseNodes) throws SQLException {
        List<ParseNode> rewrittenPreFilterParseNodes = Lists.<ParseNode>newArrayListWithExpectedSize(preFilterParseNodes.size());
        for (ParseNode preFilterParseNode : preFilterParseNodes) {
            rewrittenPreFilterParseNodes.add(preFilterParseNode.accept(this));
        }
        
        if (statement.getGroupBy().isEmpty()) {
            ParseNode where = statement.getWhere();
            if (where != null) {
                rewrittenPreFilterParseNodes.add(where);
            }
            return NODE_FACTORY.select(statement, combine(rewrittenPreFilterParseNodes));
        }
        
        ParseNode having = statement.getHaving();
        if (having != null) {
            rewrittenPreFilterParseNodes.add(having);
        }
        return NODE_FACTORY.select(statement, statement.getWhere(), combine(rewrittenPreFilterParseNodes));
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
