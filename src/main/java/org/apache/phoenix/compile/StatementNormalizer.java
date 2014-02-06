/*
 * Copyright 2014 The Apache Software Foundation
 *
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
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.phoenix.parse.BetweenParseNode;
import org.apache.phoenix.parse.ComparisonParseNode;
import org.apache.phoenix.parse.LessThanOrEqualParseNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeRewriter;
import org.apache.phoenix.parse.SelectStatement;


/**
 * 
 * Class that creates a new select statement ensuring that a literal always occurs
 * on the RHS (i.e. if literal found on the LHS, then the operator is reversed and
 * the literal is put on the RHS)
 *
 * 
 * @since 0.1
 */
public class StatementNormalizer extends ParseNodeRewriter {
    
    public StatementNormalizer(ColumnResolver resolver, int expectedAliasCount) {
        super(resolver, expectedAliasCount);
    }

    /**
     * Rewrite the select statement by switching any constants to the right hand side
     * of the expression.
     * @param statement the select statement
     * @param resolver 
     * @return new select statement
     * @throws SQLException 
     */
    public static SelectStatement normalize(SelectStatement statement, ColumnResolver resolver) throws SQLException {
        return rewrite(statement, new StatementNormalizer(resolver, statement.getSelect().size()));
    }
    
    @Override
    public ParseNode visitLeave(ComparisonParseNode node, List<ParseNode> nodes) throws SQLException {
         if (nodes.get(0).isConstant() && !nodes.get(1).isConstant()) {
             List<ParseNode> normNodes = Lists.newArrayListWithExpectedSize(2);
             normNodes.add(nodes.get(1));
             normNodes.add(nodes.get(0));
             nodes = normNodes;
             node = NODE_FACTORY.comparison(node.getInvertFilterOp(), nodes.get(0), nodes.get(1));
         }
         return super.visitLeave(node, nodes);
    }
    
    @Override
    public ParseNode visitLeave(final BetweenParseNode node, List<ParseNode> nodes) throws SQLException {
       
        LessThanOrEqualParseNode lhsNode =  NODE_FACTORY.lte(node.getChildren().get(1), node.getChildren().get(0));
        LessThanOrEqualParseNode rhsNode =  NODE_FACTORY.lte(node.getChildren().get(0), node.getChildren().get(2));
        List<ParseNode> parseNodes = Lists.newArrayListWithExpectedSize(2);
        parseNodes.add(this.visitLeave(lhsNode, lhsNode.getChildren()));
        parseNodes.add(this.visitLeave(rhsNode, rhsNode.getChildren()));
        return super.visitLeave(node, parseNodes);
    }
}
