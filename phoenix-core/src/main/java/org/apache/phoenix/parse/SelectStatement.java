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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.expression.function.CountAggregateFunction;
import org.apache.phoenix.jdbc.PhoenixStatement.Operation;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunctionInfo;

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
                    null, null, false, 
                    Collections.<AliasedNode>singletonList(new AliasedNode(null, LiteralParseNode.ONE)),
                    null, Collections.<ParseNode>emptyList(),
                    null, Collections.<OrderByNode>emptyList(),
                    null, 0, false, false);
    public static final SelectStatement COUNT_ONE =
            new SelectStatement(
                    null, null, false,
                    Collections.<AliasedNode>singletonList(
                    new AliasedNode(null, 
                        new AggregateFunctionParseNode(
                                CountAggregateFunction.NORMALIZED_NAME, 
                                LiteralParseNode.STAR, 
                                new BuiltInFunctionInfo(CountAggregateFunction.class, CountAggregateFunction.class.getAnnotation(BuiltInFunction.class))))),
                    null, Collections.<ParseNode>emptyList(), 
                    null, Collections.<OrderByNode>emptyList(), 
                    null, 0, true, false);
    public static SelectStatement create(SelectStatement select, HintNode hint) {
        if (select.getHint() == hint || hint.isEmpty()) {
            return select;
        }
        return new SelectStatement(select.getFrom(), hint, select.isDistinct(), 
                select.getSelect(), select.getWhere(), select.getGroupBy(), select.getHaving(), 
                select.getOrderBy(), select.getLimit(), select.getBindCount(), select.isAggregate(), select.hasSequence());
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
                this.getOrderBy(), this.getLimit(), this.getBindCount(), this.isAggregate(), this.hasSequence());
    }
    
    public static SelectStatement create(SelectStatement select, List<AliasedNode> selects) {
        return new SelectStatement(select.getFrom(), select.getHint(), select.isDistinct(), 
                selects, select.getWhere(), select.getGroupBy(), select.getHaving(), 
                select.getOrderBy(), select.getLimit(), select.getBindCount(), select.isAggregate(), select.hasSequence());
    }
    
    private final TableNode fromTable;
    private final HintNode hint;
    private final boolean isDistinct;
    private final List<AliasedNode> select;
    private final ParseNode where;
    private final List<ParseNode> groupBy;
    private final ParseNode having;
    private List<OrderByNode> orderBy;
    private final LimitNode limit;
    private final int bindCount;
    private final boolean isAggregate;
    private final boolean hasSequence;
    private final List<SelectStatement> selects = new ArrayList<SelectStatement>();
    private final boolean isUnion;

    @Override
    public final String toString() {
        StringBuilder buf = new StringBuilder();
        toSQL(null,buf);
        return buf.toString();
    }

    public void toSQL(ColumnResolver resolver, StringBuilder buf) {
        buf.append("SELECT ");
        if (hint != null) buf.append(hint);
        if (isDistinct) buf.append("DISTINCT ");
        for (AliasedNode selectNode : select) {
            selectNode.toSQL(resolver, buf);
            buf.append(',');
        }
        buf.setLength(buf.length()-1);
        buf.append(" FROM ");
        fromTable.toSQL(resolver, buf);
        if (where != null) {
            buf.append(" WHERE ");
            where.toSQL(resolver, buf);
        }
        if (!groupBy.isEmpty()) {
            buf.append(" GROUP BY ");
            for (ParseNode node : groupBy) {
                node.toSQL(resolver, buf);
                buf.append(',');
            }
            buf.setLength(buf.length()-1);
        }
        if (having != null) {
            buf.append(" HAVING ");
            having.toSQL(resolver, buf);            
        }
        if (!orderBy.isEmpty()) {
            buf.append(" ORDER BY ");
            for (OrderByNode node : orderBy) {
                node.toSQL(resolver, buf);
                buf.append(',');
            }
            buf.setLength(buf.length()-1);
        }
        if (limit != null) {
            buf.append(" LIMIT " + limit.toString());
        }
    }    

    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fromTable == null) ? 0 : fromTable.hashCode());
        result = prime * result + ((groupBy == null) ? 0 : groupBy.hashCode());
        result = prime * result + ((having == null) ? 0 : having.hashCode());
        result = prime * result + ((hint == null) ? 0 : hint.hashCode());
        result = prime * result + (isDistinct ? 1231 : 1237);
        result = prime * result + ((limit == null) ? 0 : limit.hashCode());
        result = prime * result + ((orderBy == null) ? 0 : orderBy.hashCode());
        result = prime * result + ((select == null) ? 0 : select.hashCode());
        result = prime * result + ((where == null) ? 0 : where.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        SelectStatement other = (SelectStatement)obj;
        if (fromTable == null) {
            if (other.fromTable != null) return false;
        } else if (!fromTable.equals(other.fromTable)) return false;
        if (groupBy == null) {
            if (other.groupBy != null) return false;
        } else if (!groupBy.equals(other.groupBy)) return false;
        if (having == null) {
            if (other.having != null) return false;
        } else if (!having.equals(other.having)) return false;
        if (hint == null) {
            if (other.hint != null) return false;
        } else if (!hint.equals(other.hint)) return false;
        if (isDistinct != other.isDistinct) return false;
        if (limit == null) {
            if (other.limit != null) return false;
        } else if (!limit.equals(other.limit)) return false;
        if (orderBy == null) {
            if (other.orderBy != null) return false;
        } else if (!orderBy.equals(other.orderBy)) return false;
        if (select == null) {
            if (other.select != null) return false;
        } else if (!select.equals(other.select)) return false;
        if (where == null) {
            if (other.where != null) return false;
        } else if (!where.equals(other.where)) return false;
        return true;
    }

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
    
    public SelectStatement(TableNode from, HintNode hint, boolean isDistinct, List<AliasedNode> select,
    		ParseNode where, List<ParseNode> groupBy, ParseNode having, List<OrderByNode> orderBy, LimitNode limit,
    		int bindCount, boolean isAggregate, boolean hasSequence) {
    	this(from, hint, isDistinct, select, where, groupBy, having, orderBy, limit, bindCount, isAggregate, hasSequence, 
    			Collections.<SelectStatement>emptyList(), false);
    }

    public SelectStatement(TableNode from, HintNode hint, boolean isDistinct, List<AliasedNode> select,
    		ParseNode where, List<ParseNode> groupBy, ParseNode having, List<OrderByNode> orderBy, LimitNode limit,
    		int bindCount, boolean isAggregate, boolean hasSequence, List<SelectStatement> selects, boolean isUnion) {
    	this.fromTable = from;
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
    	this.hasSequence = hasSequence;
    	if (!selects.isEmpty()) {
    		this.selects.addAll(selects);
    	}
    	this.isUnion = isUnion;
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
    
    public TableNode getFrom() {
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

    public boolean hasSequence() {
        return hasSequence;
    }

    @Override
    public Operation getOperation() {
        return Operation.QUERY;
    }

    public boolean isJoin() {
        return fromTable != null && fromTable instanceof JoinTableNode;
    }
    
    public SelectStatement getInnerSelectStatement() {
        if (fromTable == null || !(fromTable instanceof DerivedTableNode))
            return null;
        
        return ((DerivedTableNode) fromTable).getSelect();
    }

	public List<SelectStatement> getSelects() {
		return selects;
	}

	public boolean isUnion() {
	    return isUnion;
	}

	public void removeOrderBy() {
	    this.orderBy = Collections.<OrderByNode>emptyList();
	}
}
