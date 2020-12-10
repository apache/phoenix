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
package org.apache.phoenix.util;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.FamilyWildcardParseNode;
import org.apache.phoenix.parse.OrderByNode;
import org.apache.phoenix.parse.ParseNodeVisitor;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.StatementNormalizer;
import org.apache.phoenix.compile.SubqueryRewriter;
import org.apache.phoenix.compile.SubselectRewriter;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.parse.AliasedNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.StatelessTraverseAllParseNodeVisitor;
import org.apache.phoenix.parse.TableWildcardParseNode;
import org.apache.phoenix.parse.WildcardParseNode;
import org.apache.phoenix.compile.QueryCompiler;

public class ParseNodeUtil {

    /**
     * Apply the {@link ParseNodeVisitor} to every part of the {@link SelectStatement}.
     * @param selectStatement
     * @param parseNodeVisitor
     * @throws SQLException
     */
    public static <T> void applyParseNodeVisitor(SelectStatement selectStatement, ParseNodeVisitor<T> parseNodeVisitor) throws SQLException {
        applyParseNodeVisitor(selectStatement, parseNodeVisitor, true);
    }

    /**
     * Apply the {@link ParseNodeVisitor} to every part of the {@link SelectStatement}.
     * @param selectStatement
     * @param parseNodeVisitor
     * @param applyWhere
     * @throws SQLException
     */
    public static <T> void applyParseNodeVisitor(
            SelectStatement selectStatement,
            ParseNodeVisitor<T> parseNodeVisitor,
            boolean applyWhere) throws SQLException {

        for (AliasedNode selectAliasedNode : selectStatement.getSelect()) {
            selectAliasedNode.getNode().accept(parseNodeVisitor);
        }

        if (selectStatement.getGroupBy() != null) {
            for (ParseNode groupByParseNode : selectStatement.getGroupBy()) {
                groupByParseNode.accept(parseNodeVisitor);
            }
        }

        if (selectStatement.getHaving() != null) {
            selectStatement.getHaving().accept(parseNodeVisitor);
        }

        if (selectStatement.getOrderBy() != null) {
            for (OrderByNode orderByNode : selectStatement.getOrderBy()) {
                orderByNode.getNode().accept(parseNodeVisitor);
            }
        }

        if(applyWhere && selectStatement.getWhere() != null) {
            selectStatement.getWhere().accept(parseNodeVisitor);
        }
    }

    /**
     * Collect referenced columnNames in selectStatement, the selectStatement is a single table query, not a join.
     * @param selectStatement
     * @return
     * @throws SQLException
     */
    public static Set<String> collectReferencedColumnNamesForSingleTable(SelectStatement selectStatement) throws SQLException{
        SingleTableCollectColumnNameParseNodeVisitor collectColumnNameParseNodeVisitor =
                new SingleTableCollectColumnNameParseNodeVisitor();
        applyParseNodeVisitor(selectStatement, collectColumnNameParseNodeVisitor);
        boolean isWildcard = collectColumnNameParseNodeVisitor.isWildcard();
        if(isWildcard) {
            return null;
        }
        return collectColumnNameParseNodeVisitor.getReferenceColumnNames();
    }

    private static class SingleTableCollectColumnNameParseNodeVisitor extends StatelessTraverseAllParseNodeVisitor {
        private final Set<String> referenceColumnNames;
        private boolean wildcard = false;

        public SingleTableCollectColumnNameParseNodeVisitor() {
            this.referenceColumnNames = new HashSet<String>();
        }

        public Set<String> getReferenceColumnNames() {
            return this.referenceColumnNames;
        }

        public boolean isWildcard() {
            return wildcard;
        }

        @Override
        public Void visit(ColumnParseNode columnParseNode) throws SQLException {
            String normalizedColumnName = SchemaUtil.getNormalizedColumnName(columnParseNode);
            referenceColumnNames.add(normalizedColumnName);
            return null;
        }

        @Override
        public Void visit(WildcardParseNode node) throws SQLException {
            this.wildcard = true;
            return null;
        }

        @Override
        public Void visit(TableWildcardParseNode node) throws SQLException {
            this.wildcard = true;
            return null;
        }

        @Override
        public Void visit(FamilyWildcardParseNode node) throws SQLException {
            this.wildcard = true;
            return null;
        }
    }

    public static class RewriteResult {
        private SelectStatement rewrittenSelectStatement;
        private ColumnResolver columnResolver;
        public RewriteResult(SelectStatement rewrittenSelectStatement, ColumnResolver columnResolver) {
            this.rewrittenSelectStatement = rewrittenSelectStatement;
            this.columnResolver = columnResolver;
        }
        public SelectStatement getRewrittenSelectStatement() {
            return rewrittenSelectStatement;
        }
        public ColumnResolver getColumnResolver() {
            return columnResolver;
        }
    }

    /**
     * Optimize rewriting {@link SelectStatement} by {@link SubselectRewriter} and {@link SubqueryRewriter} before
     * {@link QueryCompiler#compile}.
     * @param selectStatement
     * @param phoenixConnection
     * @return
     * @throws SQLException
     */
    public static RewriteResult rewrite(SelectStatement selectStatement, PhoenixConnection phoenixConnection) throws SQLException {
        SelectStatement selectStatementToUse =
                SubselectRewriter.flatten(selectStatement, phoenixConnection);
        ColumnResolver columnResolver =
                FromCompiler.getResolverForQuery(selectStatementToUse, phoenixConnection);
        selectStatementToUse = StatementNormalizer.normalize(selectStatementToUse, columnResolver);
        SelectStatement transformedSubquery =
                SubqueryRewriter.transform(selectStatementToUse, columnResolver, phoenixConnection);
        if (transformedSubquery != selectStatementToUse) {
            columnResolver = FromCompiler.getResolverForQuery(transformedSubquery, phoenixConnection);
            transformedSubquery = StatementNormalizer.normalize(transformedSubquery, columnResolver);
        }
        return new RewriteResult(transformedSubquery, columnResolver);
    }
}
