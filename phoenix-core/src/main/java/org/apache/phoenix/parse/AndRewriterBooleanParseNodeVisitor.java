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
import java.util.List;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

/**
 * Base visitor for rewrite {@link ParseNode},only further visit down for {@link AndParseNode}.
 * A example is org.apache.phoenix.optimize.QueryOptimizer.WhereConditionRewriter, which
 * rewrites columns in dataTable to columns in indexTable, and removes parseNodes which have
 * columns not in indexTable.
 */
public abstract class AndRewriterBooleanParseNodeVisitor extends AndBooleanParseNodeVisitor<ParseNode>{

    private final ParseNodeFactory parseNodeFactory ;

    public AndRewriterBooleanParseNodeVisitor(ParseNodeFactory parseNodeFactory) {
        this.parseNodeFactory = parseNodeFactory;
    }

    @Override
    public List<ParseNode> newElementList(int size) {
        return Lists.<ParseNode> newArrayListWithExpectedSize(size);
    }

    @Override
    public void addElement(List<ParseNode> childParseNodeResults, ParseNode newChildParseNode) {
        if (newChildParseNode != null) {
            childParseNodeResults.add(newChildParseNode);
        }
    }

    @Override
    protected ParseNode leaveNonBooleanNode(ParseNode parentParseNode, List<ParseNode> childParseNodes) throws SQLException {
        return parentParseNode;
    }

    @Override
    public ParseNode visitLeave(AndParseNode andParseNode, List<ParseNode> childParseNodes) throws SQLException {
        if (childParseNodes.equals(andParseNode.getChildren())) {
            return andParseNode;
        }

        if (childParseNodes.isEmpty()) {
            return null;
        }

        if (childParseNodes.size() == 1) {
            return childParseNodes.get(0);
        }

        return this.parseNodeFactory.and(childParseNodes);
    }
}
