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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.phoenix.expression.function.CountAggregateFunction;
import org.apache.phoenix.jdbc.PhoenixStatement.Operation;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunctionInfo;
import org.apache.phoenix.schema.Sequence.Action;

/**
 * 
 * Top level node representing a SQL statement
 *
 * 
 * @since 0.1
 */
public class SelectStatement implements FilterableStatement {
    public static final SelectStatement SELECT_ONE =
            new SelectStatement(
                    Collections.<TableNode>emptyList(), null, false, 
                    Collections.<AliasedNode>singletonList(new AliasedNode(null,new LiteralParseNode(1))),
                    null, Collections.<ParseNode>emptyList(),
                    null, Collections.<OrderByNode>emptyList(),
                    null, 0, false);
    public static final SelectStatement COUNT_ONE =
            new SelectStatement(
                    Collections.<TableNode>emptyList(), null, false,
                    Collections.<AliasedNode>singletonList(
                    new AliasedNode(null, 
                        new AggregateFunctionParseNode(
                                CountAggregateFunction.NORMALIZED_NAME, 
                                LiteralParseNode.STAR, 
                                new BuiltInFunctionInfo(CountAggregateFunction.class, CountAggregateFunction.class.getAnnotation(BuiltInFunction.class))))),
                    null, Collections.<ParseNode>emptyList(), 
                    null, Collections.<OrderByNode>emptyList(), 
                    null, 0, true);
    public static SelectStatement create(SelectStatement select, HintNode hint) {
        if (select.getHint() == hint || hint.isEmpty()) {
            return select;
        }
        return new SelectStatement(select.getFrom(), hint, select.isDistinct(), 
                select.getSelect(), select.getWhere(), select.getGroupBy(), select.getHaving(), 
                select.getOrderBy(), select.getLimit(), select.getBindCount(), select.isAggregate());
    }
    
    public SelectStatement combine(ParseNode where) {
        if (where == null) {
            return this;
        }
        if (this.getWhere() != null) {
            where = new AndParseNode(Arrays.asList(this.getWhere(), where));
        }
        return new SelectStatement(this.getFrom(), this.getHint(), this.isDistinct(), 
                this.getSelect(), where, this.getGroupBy(), this.getHaving(), 
                this.getOrderBy(), this.getLimit(), this.getBindCount(), this.isAggregate());
    }
    
    public static SelectStatement create(SelectStatement select, List<AliasedNode> selects) {
        return new SelectStatement(select.getFrom(), select.getHint(), select.isDistinct(), 
                selects, select.getWhere(), select.getGroupBy(), select.getHaving(), 
                select.getOrderBy(), select.getLimit(), select.getBindCount(), select.isAggregate());
    }
    
    private final List<TableNode> fromTable;
    private final HintNode hint;
    private final boolean isDistinct;
    private final List<AliasedNode> select;
    private final ParseNode where;
    private final List<ParseNode> groupBy;
    private final ParseNode having;
    private final List<OrderByNode> orderBy;
    private final LimitNode limit;
    private final int bindCount;
    private final boolean isAggregate;
    
    // Count constant expressions
    private static int countConstants(List<ParseNode> nodes) {
        int count = 0;
        for (int i = 0; i < nodes.size(); i++) {
            if (nodes.get(i).isStateless()) {
                count++;
            }
        }
        return count;
    }
    
    protected SelectStatement(List<? extends TableNode> from, HintNode hint, boolean isDistinct, List<AliasedNode> select, ParseNode where, List<ParseNode> groupBy, ParseNode having, List<OrderByNode> orderBy, LimitNode limit, int bindCount, boolean isAggregate) {
        this.fromTable = Collections.unmodifiableList(from);
        this.hint = hint == null ? HintNode.EMPTY_HINT_NODE : hint;
        this.isDistinct = isDistinct;
        this.select = Collections.unmodifiableList(select);
        this.where = where;
        this.groupBy = Collections.unmodifiableList(groupBy);
        this.having = having;
        this.orderBy = Collections.unmodifiableList(orderBy);
        this.limit = limit;
        this.bindCount = bindCount;
        this.isAggregate = isAggregate || groupBy.size() != countConstants(groupBy) || this.having != null;
    }
    
    @Override
    public boolean isDistinct() {
        return isDistinct;
    }
    
    @Override
    public LimitNode getLimit() {
        return limit;
    }
    
    @Override
    public int getBindCount() {
        return bindCount;
    }
    
    public List<TableNode> getFrom() {
        return fromTable;
    }
    
    @Override
    public HintNode getHint() {
        return hint;
    }
    
    public List<AliasedNode> getSelect() {
        return select;
    }
    /**
     * Gets the where condition, or null if none.
     */
    @Override
    public ParseNode getWhere() {
        return where;
    }
    
    /**
     * Gets the group-by, containing at least 1 element, or null, if none.
     */
    public List<ParseNode> getGroupBy() {
        return groupBy;
    }
    
    public ParseNode getHaving() {
        return having;
    }
    
    /**
     * Gets the order-by, containing at least 1 element, or null, if none.
     */
    @Override
    public List<OrderByNode> getOrderBy() {
        return orderBy;
    }

    @Override
    public boolean isAggregate() {
        return isAggregate;
    }

    @Override
    public Operation getOperation() {
        return Operation.QUERY;
    }

    @Override
    public Action getSequenceAction() {
        return Action.RESERVE;
    }
}
