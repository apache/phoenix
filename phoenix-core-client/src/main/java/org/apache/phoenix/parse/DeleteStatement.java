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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.phoenix.jdbc.PhoenixStatement.Operation;

public class DeleteStatement extends DMLStatement implements FilterableStatement {
    private final ParseNode whereNode;
    private final List<OrderByNode> orderBy;
    private final LimitNode limit;
    private final HintNode hint;
    
    public DeleteStatement(NamedTableNode table, HintNode hint, ParseNode whereNode, List<OrderByNode> orderBy, LimitNode limit, int bindCount, Map<String, UDFParseNode> udfParseNodes) {
        super(table, bindCount, udfParseNodes);
        this.whereNode = whereNode;
        this.orderBy = orderBy == null ? Collections.<OrderByNode>emptyList() : orderBy;
        this.limit = limit;
        this.hint = hint == null ? HintNode.EMPTY_HINT_NODE : hint;
    }

    @Override
    public ParseNode getWhere() {
        return whereNode;
    }

    @Override
    public List<OrderByNode> getOrderBy() {
        return orderBy;
    }

    @Override
    public LimitNode getLimit() {
        return limit;
    }

    @Override
    public HintNode getHint() {
        return hint;
    }

    @Override
    public boolean isDistinct() {
        return false;
    }

    @Override
    public boolean isAggregate() {
        return false;
    }

    @Override
    public Operation getOperation() {
        return Operation.DELETE;
    }

    @Override
    public OffsetNode getOffset() {
        return null;
    }
    
    @Override
    public Double getTableSamplingRate(){
    	throw new UnsupportedOperationException("Table sampling is not allowd for Deletion");
    }

}
