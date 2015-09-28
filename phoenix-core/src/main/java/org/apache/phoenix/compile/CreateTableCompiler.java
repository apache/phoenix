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
import java.util.BitSet;
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
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.IsNullExpression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.expression.visitor.StatelessTraverseNoExpressionVisitor;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.BindParseNode;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.CreateTableStatement;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.query.DelegateConnectionQueryServices;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.PMetaData;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.ViewType;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.QueryUtil;

import com.google.common.collect.Iterators;


public class CreateTableCompiler {
    private static final PDatum VARBINARY_DATUM = new VarbinaryDatum();
    private final PhoenixStatement statement;
    
    public CreateTableCompiler(PhoenixStatement statement) {
        this.statement = statement;
    }

    public MutationPlan compile(final CreateTableStatement create) throws SQLException {
        final PhoenixConnection connection = statement.getConnection();
        ColumnResolver resolver = FromCompiler.getResolverForCreation(create, connection);
        PTableType type = create.getTableType();
        PhoenixConnection connectionToBe = connection;
        PTable parentToBe = null;
        ViewType viewTypeToBe = null;
        Scan scan = new Scan();
        final StatementContext context = new StatementContext(statement, resolver, scan, new SequenceManager(statement));
        // TODO: support any statement for a VIEW instead of just a WHERE clause
        ParseNode whereNode = create.getWhereClause();
        String viewStatementToBe = null;
        byte[][] viewColumnConstantsToBe = null;
        BitSet isViewColumnReferencedToBe = null;
        if (type == PTableType.VIEW) {
            TableRef tableRef = resolver.getTables().get(0);
            int nColumns = tableRef.getTable().getColumns().size();
            isViewColumnReferencedToBe = new BitSet(nColumns);
            // Used to track column references in a view
            ExpressionCompiler expressionCompiler = new ColumnTrackingExpressionCompiler(context, isViewColumnReferencedToBe);
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
                    StringBuilder buf = new StringBuilder();
                    whereNode.toSQL(resolver, buf);
                    viewStatementToBe = QueryUtil.getViewStatement(baseTableName.getSchemaName(), baseTableName.getTableName(), buf.toString());
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
                    viewColumnConstantsToBe = new byte[nColumns][];
                    ViewWhereExpressionVisitor visitor = new ViewWhereExpressionVisitor(parentToBe, viewColumnConstantsToBe);
                    where.accept(visitor);
                    // If view is not updatable, viewColumnConstants should be empty. We will still
                    // inherit our parent viewConstants, but we have no additional ones.
                    viewTypeToBe = visitor.isUpdatable() ? ViewType.UPDATABLE : ViewType.READ_ONLY;
                    if (viewTypeToBe != ViewType.UPDATABLE) {
                        viewColumnConstantsToBe = null;
                    }
                }
            }
        }
        final ViewType viewType = viewTypeToBe;
        final String viewStatement = viewStatementToBe;
        final byte[][] viewColumnConstants = viewColumnConstantsToBe;
        final BitSet isViewColumnReferenced = isViewColumnReferencedToBe;
        List<ParseNode> splitNodes = create.getSplitNodes();
        final byte[][] splits = new byte[splitNodes.size()][];
        ImmutableBytesWritable ptr = context.getTempPtr();
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(context);
        for (int i = 0; i < splits.length; i++) {
            ParseNode node = splitNodes.get(i);
            if (node instanceof BindParseNode) {
                context.getBindManager().addParamMetaData((BindParseNode) node, VARBINARY_DATUM);
            }
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
                    return client.createTable(create, splits, parent, viewStatement, viewType, viewColumnConstants, isViewColumnReferenced);
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
            
            @Override
            public StatementContext getContext() {
                return context;
            }
        };
    }
    
    private static class ColumnTrackingExpressionCompiler extends ExpressionCompiler {
        private final BitSet isColumnReferenced;
        
        public ColumnTrackingExpressionCompiler(StatementContext context, BitSet isColumnReferenced) {
            super(context, true);
            this.isColumnReferenced = isColumnReferenced;
        }
        
        @Override
        protected ColumnRef resolveColumn(ColumnParseNode node) throws SQLException {
            ColumnRef ref = super.resolveColumn(node);
            isColumnReferenced.set(ref.getColumn().getPosition());
            return ref;
        }
    }
    
    private static class ViewWhereExpressionVisitor extends StatelessTraverseNoExpressionVisitor<Boolean> {
        private boolean isUpdatable = true;
        private final PTable table;
        private int position;
        private final byte[][] columnValues;
        private final ImmutableBytesWritable ptr = new ImmutableBytesWritable();

        public ViewWhereExpressionVisitor (PTable table, byte[][] columnValues) {
            this.table = table;
            this.columnValues = columnValues;
        }
        
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
            if (node.getFilterOp() == CompareOp.EQUAL && node.getChildren().get(1).isStateless() 
            		&& node.getChildren().get(1).getDeterminism() == Determinism.ALWAYS ) {
                return Iterators.singletonIterator(node.getChildren().get(0));
            }
            return super.visitEnter(node);
        }

        @Override
        public Boolean visitLeave(ComparisonExpression node, List<Boolean> l) {
            if (l.isEmpty()) {
                return null;
            }
            
            node.getChildren().get(1).evaluate(null, ptr);
            // Set the columnValue at the position of the column to the
            // constant with which it is being compared.
            // We always strip the last byte so that we can recognize null
            // as a value with a single byte.
            columnValues[position] = new byte [ptr.getLength() + 1];
            System.arraycopy(ptr.get(), ptr.getOffset(), columnValues[position], 0, ptr.getLength());
            return Boolean.TRUE;
        }

        @Override
        public Iterator<Expression> visitEnter(IsNullExpression node) {
            return node.isNegate() ? super.visitEnter(node) : node.getChildren().iterator();
        }
        
        @Override
        public Boolean visitLeave(IsNullExpression node, List<Boolean> l) {
            // Nothing to do as we've already set the position to an empty byte array
            return l.isEmpty() ? null : Boolean.TRUE;
        }
        
        @Override
        public Boolean visit(RowKeyColumnExpression node) {
            this.position = table.getPKColumns().get(node.getPosition()).getPosition();
            return Boolean.TRUE;
        }

        @Override
        public Boolean visit(KeyValueColumnExpression node) {
            try {
                this.position = table.getColumnFamily(node.getColumnFamily()).getColumn(node.getColumnName()).getPosition();
            } catch (SQLException e) {
                throw new RuntimeException(e); // Impossible
            }
            return Boolean.TRUE;
        }
        
    }
    private static class VarbinaryDatum implements PDatum {

        @Override
        public boolean isNullable() {
            return false;
        }

        @Override
        public PDataType getDataType() {
            return PVarbinary.INSTANCE;
        }

        @Override
        public Integer getMaxLength() {
            return null;
        }

        @Override
        public Integer getScale() {
            return null;
        }

        @Override
        public SortOrder getSortOrder() {
            return SortOrder.getDefault();
        }
        
    }
}
