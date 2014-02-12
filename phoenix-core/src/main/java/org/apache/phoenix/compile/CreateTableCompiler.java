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

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.expression.ComparisonExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.IsNullExpression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.expression.visitor.TraverseNoExpressionVisitor;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.CreateTableStatement;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.parse.WildcardParseNode;
import org.apache.phoenix.query.DelegateConnectionQueryServices;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PMetaData;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.ViewType;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.ByteUtil;

import com.google.common.collect.Iterators;


public class CreateTableCompiler {
    private static final String SELECT = "SELECT";
    private static final String FROM = "FROM";
    private static final String WHERE = "WHERE";
    
    private final PhoenixStatement statement;
    
    public CreateTableCompiler(PhoenixStatement statement) {
        this.statement = statement;
    }

    public MutationPlan compile(final CreateTableStatement create) throws SQLException {
        final PhoenixConnection connection = statement.getConnection();
        ColumnResolver resolver = FromCompiler.getResolver(create, connection);
        PTableType type = create.getTableType();
        PhoenixConnection connectionToBe = connection;
        PTable parentToBe = null;
        ViewType viewTypeToBe = null;
        Scan scan = new Scan();
        final StatementContext context = new StatementContext(statement, resolver, statement.getParameters(), scan);
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(context);
        // TODO: support any statement for a VIEW instead of just a WHERE clause
        ParseNode whereNode = create.getWhereClause();
        String viewStatementToBe = null;
        if (type == PTableType.VIEW) {
            TableRef tableRef = resolver.getTables().get(0);
            parentToBe = tableRef.getTable();
            viewTypeToBe = parentToBe.getViewType() == ViewType.MAPPED ? ViewType.MAPPED : ViewType.UPDATABLE;
            if (whereNode == null) {
                viewStatementToBe = parentToBe.getViewStatement();
            } else {
                whereNode = StatementNormalizer.normalize(whereNode, resolver);
                if (whereNode.isStateless()) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.VIEW_WHERE_IS_CONSTANT)
                        .build().buildException();
                }
                // If our parent has a VIEW statement, combine it with this one
                if (parentToBe.getViewStatement() != null) {
                    SelectStatement select = new SQLParser(parentToBe.getViewStatement()).parseQuery().combine(whereNode);
                    whereNode = select.getWhere();
                }
                Expression where = whereNode.accept(expressionCompiler);
                if (where != null && !LiteralExpression.isTrue(where)) {
                    TableName baseTableName = create.getBaseTableName();
                    String schemaName = baseTableName.getSchemaName();
                    // Only form we currently support for VIEWs: SELECT * FROM t WHERE ...
                    viewStatementToBe = SELECT + " " + WildcardParseNode.NAME + " " + FROM +
                            (schemaName == null ? "" : "\"" + schemaName + "\".") +
                            (" \"" + baseTableName.getTableName() + "\" ") +
                            (WHERE + " " + where.toString());
                }
                if (viewTypeToBe != ViewType.MAPPED) {
                    Long scn = connection.getSCN();
                    connectionToBe = scn != null ? connection :
                        // If we haved no SCN on our connection, freeze the SCN at when
                        // the base table was resolved to prevent any race condition on
                        // the error checking we do for the base table. The only potential
                        // issue is if the base table lives on a different region server
                        // than the new table will, then we're relying here on the system
                        // clocks being in sync.
                        new PhoenixConnection(
                            // When the new table is created, we still want to cache it
                            // on our connection.
                            new DelegateConnectionQueryServices(connection.getQueryServices()) {
                                @Override
                                public PMetaData addTable(PTable table) throws SQLException {
                                    return connection.addTable(table);
                                }
                            },
                            connection, tableRef.getTimeStamp());
                    ViewWhereExpressionVisitor visitor = new ViewWhereExpressionVisitor();
                    where.accept(visitor);
                    viewTypeToBe = visitor.isUpdatable() ? ViewType.UPDATABLE : ViewType.READ_ONLY;
                }
            }
        }
        final ViewType viewType = viewTypeToBe;
        final String viewStatement = viewStatementToBe;
        List<ParseNode> splitNodes = create.getSplitNodes();
        final byte[][] splits = new byte[splitNodes.size()][];
        ImmutableBytesWritable ptr = context.getTempPtr();
        for (int i = 0; i < splits.length; i++) {
            ParseNode node = splitNodes.get(i);
            if (node.isStateless()) {
                Expression expression = node.accept(expressionCompiler);
                if (expression.evaluate(null, ptr)) {;
                    splits[i] = ByteUtil.copyKeyBytesIfNecessary(ptr);
                    continue;
                }
            }
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.SPLIT_POINT_NOT_CONSTANT)
                .setMessage("Node: " + node).build().buildException();
        }
        final MetaDataClient client = new MetaDataClient(connectionToBe);
        final PTable parent = parentToBe;
        
        return new MutationPlan() {

            @Override
            public ParameterMetaData getParameterMetaData() {
                return context.getBindManager().getParameterMetaData();
            }

            @Override
            public MutationState execute() throws SQLException {
                try {
                    return client.createTable(create, splits, parent, viewStatement, viewType);
                } finally {
                    if (client.getConnection() != connection) {
                        client.getConnection().close();
                    }
                }
            }

            @Override
            public ExplainPlan getExplainPlan() throws SQLException {
                return new ExplainPlan(Collections.singletonList("CREATE TABLE"));
            }

            @Override
            public PhoenixConnection getConnection() {
                return connection;
            }
            
        };
    }
    
    private static class ViewWhereExpressionVisitor extends TraverseNoExpressionVisitor<Boolean> {
        private boolean isUpdatable = true;

        public boolean isUpdatable() {
            return isUpdatable;
        }

        @Override
        public Boolean defaultReturn(Expression node, List<Boolean> l) {
            // We only hit this if we're trying to traverse somewhere
            // in which we don't have a visitLeave that returns non null
            isUpdatable = false;
            return null;
        }

        @Override
        public Iterator<Expression> visitEnter(AndExpression node) {
            return node.getChildren().iterator();
        }

        @Override
        public Boolean visitLeave(AndExpression node, List<Boolean> l) {
            return l.isEmpty() ? null : Boolean.TRUE;
        }

        @Override
        public Iterator<Expression> visitEnter(ComparisonExpression node) {
            return node.getFilterOp() == CompareOp.EQUAL && node.getChildren().get(1).isStateless() && node.getChildren().get(1).isDeterministic() ? Iterators.singletonIterator(node.getChildren().get(0)) : super.visitEnter(node);
        }

        @Override
        public Boolean visitLeave(ComparisonExpression node, List<Boolean> l) {
            return l.isEmpty() ? null : Boolean.TRUE;
        }

        @Override
        public Iterator<Expression> visitEnter(IsNullExpression node) {
            return node.isNegate() ? super.visitEnter(node) : node.getChildren().iterator();
        }
        
        @Override
        public Boolean visitLeave(IsNullExpression node, List<Boolean> l) {
            return l.isEmpty() ? null : Boolean.TRUE;
        }
        
        @Override
        public Boolean visit(RowKeyColumnExpression node) {
            return Boolean.TRUE;
        }

        @Override
        public Boolean visit(KeyValueColumnExpression node) {
            return Boolean.TRUE;
        }
        
    }
}
