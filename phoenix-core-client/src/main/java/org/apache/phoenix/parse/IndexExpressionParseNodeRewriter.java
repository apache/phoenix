/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.parse;

import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.ExpressionCompiler;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.IndexStatementRewriter;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.IndexUtil;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

/**
 * Used to replace parse nodes in a SelectStatement that match expressions that are present in an
 * indexed with the corresponding {@link ColumnParseNode}
 */
public class IndexExpressionParseNodeRewriter extends ParseNodeRewriter {

  private final Map<ParseNode, ParseNode> indexedParseNodeToColumnParseNodeMap;

  /**
   * Maps the rewritten parse node of a functional index column to a [colName, expressionStr] pair.
   * Plain PK columns are excluded.
   */
  private final Map<ParseNode, String[]> indexedParseNodeToFunctionalColumn;

  /**
   * Records the functional index substitutions that actually fired against the query, mapping the
   * index column name to the expression string. Insertion order is preserved so callers can report
   * matches in first-seen order.
   */
  private final Map<String, String> appliedFunctionalSubstitutions = new LinkedHashMap<>();

  public IndexExpressionParseNodeRewriter(PTable index, String alias, PhoenixConnection connection,
    Map<String, UDFParseNode> udfParseNodes) throws SQLException {
    this(index, alias, connection, udfParseNodes, null);
  }

  public IndexExpressionParseNodeRewriter(PTable index, String alias, PhoenixConnection connection,
    Map<String, UDFParseNode> udfParseNodes, StatementContext breadcrumbContext)
    throws SQLException {
    indexedParseNodeToColumnParseNodeMap =
      Maps.newHashMapWithExpectedSize(index.getColumns().size());
    indexedParseNodeToFunctionalColumn = Maps.newHashMapWithExpectedSize(index.getColumns().size());
    NamedTableNode tableNode =
      NamedTableNode.create(alias, TableName.create(index.getParentSchemaName().getString(),
        index.getParentTableName().getString()), Collections.<ColumnDef> emptyList());
    ColumnResolver dataResolver = FromCompiler.getResolver(tableNode, connection, udfParseNodes);
    StatementContext context = new StatementContext(new PhoenixStatement(connection), dataResolver);
    IndexStatementRewriter rewriter = new IndexStatementRewriter(dataResolver, null, true);
    ExpressionCompiler expressionCompiler = new ExpressionCompiler(context);
    int indexPosOffset = (index.getBucketNum() == null ? 0 : 1) + (index.isMultiTenant() ? 1 : 0)
      + (index.getViewIndexId() == null ? 0 : 1);
    List<PColumn> pkColumns = index.getPKColumns();
    for (int i = indexPosOffset; i < pkColumns.size(); ++i) {
      PColumn column = pkColumns.get(i);
      String expressionStr = IndexUtil.getIndexColumnExpressionStr(column);
      ParseNode expressionParseNode = SQLParser.parseCondition(expressionStr);
      String colName = "\"" + column.getName().getString() + "\"";
      Expression dataExpression = expressionParseNode.accept(expressionCompiler);
      PDataType expressionDataType = dataExpression.getDataType();
      ParseNode indexedParseNode = expressionParseNode.accept(rewriter);
      PDataType indexColType =
        IndexUtil.getIndexColumnDataType(dataExpression.isNullable(), expressionDataType);
      ParseNode columnParseNode =
        new ColumnParseNode(alias != null ? TableName.create(null, alias) : null, colName, null);
      if (indexColType != expressionDataType) {
        columnParseNode = NODE_FACTORY.cast(columnParseNode, expressionDataType, null, null);
      }
      indexedParseNodeToColumnParseNodeMap.put(indexedParseNode, columnParseNode);
      // Only true functional columns defined over an expression get an applied match entry. A
      // plain indexed or PK column's expression string parses to a bare column reference. An
      // expression column (e.g. UPPER(NAME)) parses to a compound node.
      if (!(expressionParseNode instanceof ColumnParseNode)) {
        // Trim leading/trailing whitespace
        indexedParseNodeToFunctionalColumn.put(indexedParseNode,
          new String[] { colName, expressionStr.trim() });
      }
      if (breadcrumbContext != null) {
        breadcrumbContext.addAppliedRewrite("INDEX EXPRESSION " + expressionStr + " AS " + colName);
        breadcrumbContext.addIndexExpressionSubstitution(indexedParseNode, colName);
      }
    }
  }

  @Override
  protected ParseNode leaveCompoundNode(CompoundParseNode node, List<ParseNode> children,
    CompoundNodeFactory factory) {
    ParseNode replacement = indexedParseNodeToColumnParseNodeMap.get(node);
    if (replacement == null) {
      return super.leaveCompoundNode(node, children, factory);
    }
    // A functional index substitution actually fired for this query node.
    String[] functionalColumn = indexedParseNodeToFunctionalColumn.get(node);
    if (functionalColumn != null) {
      appliedFunctionalSubstitutions.put(functionalColumn[0], functionalColumn[1]);
    }
    return replacement;
  }

  /**
   * Returns the functional index substitutions that actually fired against the query, keyed by
   * index column name with the expression string as the value, in first-seen order.
   */
  public Map<String, String> getAppliedFunctionalSubstitutions() {
    return Collections.unmodifiableMap(appliedFunctionalSubstitutions);
  }

  /**
   * Returns {@code true} if the index this rewriter was built for has at least one functional
   * column, regardless of whether any such column matched the query.
   */
  public boolean hasFunctionalColumns() {
    return !indexedParseNodeToFunctionalColumn.isEmpty();
  }

}
