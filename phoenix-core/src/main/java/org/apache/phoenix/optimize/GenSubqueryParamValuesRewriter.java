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
package org.apache.phoenix.optimize;

import org.apache.phoenix.compile.ExpressionCompiler;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.AndParseNode;
import org.apache.phoenix.parse.ArrayAllComparisonNode;
import org.apache.phoenix.parse.ArrayAnyComparisonNode;
import org.apache.phoenix.parse.ComparisonParseNode;
import org.apache.phoenix.parse.ExistsParseNode;
import org.apache.phoenix.parse.InParseNode;
import org.apache.phoenix.parse.OrParseNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.ParseNodeRewriter;
import org.apache.phoenix.parse.SubqueryParseNode;
import org.apache.phoenix.schema.types.PDataType;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Creates a new WHERE clause by replaces non-correlated sub-queries with dummy values.
 *
 * Note that this class does not check the presence of correlation, thus it should only
 * be used after de-correlation has been performed.
 */
public class GenSubqueryParamValuesRewriter extends ParseNodeRewriter {
    private static final ParseNodeFactory NODE_FACTORY = new ParseNodeFactory();

    private final ExpressionCompiler expressionCompiler;

    public static ParseNode replaceWithDummyValues(
            ParseNode where, StatementContext context) throws SQLException {
        return rewrite(where, new GenSubqueryParamValuesRewriter(context));
    }

    private GenSubqueryParamValuesRewriter(StatementContext context) {
        this.expressionCompiler = new ExpressionCompiler(context);
    }

    protected List<ParseNode> generateDummyValues(
            ParseNode lhs, boolean multipleValues) throws SQLException {
        Expression expr = lhs.accept(expressionCompiler);
        PDataType type = expr.getDataType();
        if (!multipleValues) {
            return Arrays.<ParseNode> asList(NODE_FACTORY.literal(type.getSampleValue(), type));
        }

        return Arrays.<ParseNode> asList(
                NODE_FACTORY.literal(type.getSampleValue(), type),
                NODE_FACTORY.literal(type.getSampleValue(), type),
                NODE_FACTORY.literal(type.getSampleValue(), type));
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
    public ParseNode visitLeave(OrParseNode node, List<ParseNode> l) throws SQLException {
        return leaveCompoundNode(node, l, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                if (children.isEmpty()) {
                    return null;
                }
                if (children.size() == 1) {
                    return children.get(0);
                }
                return NODE_FACTORY.or(children);
            }
        });
    }

    @Override
    public ParseNode visitLeave(InParseNode node, List<ParseNode> l) throws SQLException {
        ParseNode lhs = l.get(0);
        List<ParseNode> inList = generateDummyValues(lhs, true);
        List<ParseNode> children = new ArrayList<ParseNode>();
        children.add(lhs);
        children.addAll(inList);
        return NODE_FACTORY.inList(children, node.isNegate());
    }

    @Override
    public ParseNode visitLeave(ExistsParseNode node, List<ParseNode> l) throws SQLException {
        return null;
    }

    @Override
    public ParseNode visitLeave(ComparisonParseNode node, List<ParseNode> l) throws SQLException {
        if (!(l.get(1) instanceof SubqueryParseNode)) {
            super.visitLeave(node, l);
        }

        ParseNode lhs = l.get(0);
        List<ParseNode> rhs = generateDummyValues(lhs, false);
        List<ParseNode> children = new ArrayList<ParseNode>();
        children.add(lhs);
        children.add(rhs.get(0));
        return super.visitLeave(node, children);
    }

    @Override
    public ParseNode visitLeave(ArrayAnyComparisonNode node, List<ParseNode> l) throws SQLException {
        ComparisonParseNode compare = (ComparisonParseNode) l.get(1);
        ParseNode lhs = compare.getLHS();
        List<ParseNode> rhs = generateDummyValues(lhs, false);

        return NODE_FACTORY.comparison(compare.getFilterOp(), lhs, rhs.get(0));
    }

    @Override
    public ParseNode visitLeave(ArrayAllComparisonNode node, List<ParseNode> l) throws SQLException {
        ComparisonParseNode compare = (ComparisonParseNode) l.get(1);
        ParseNode lhs = compare.getLHS();
        List<ParseNode> rhs = generateDummyValues(lhs, false);

        return NODE_FACTORY.comparison(compare.getFilterOp(), lhs, rhs.get(0));
    }
}
