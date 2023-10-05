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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.jdbc.PhoenixStatement.Operation;
import org.apache.phoenix.parse.CreateIndexStatement;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.StatelessTraverseAllParseNodeVisitor;
import org.apache.phoenix.parse.SubqueryParseNode;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.tuple.MultiKeyValueTuple;
import org.apache.phoenix.schema.types.*;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PhoenixRuntime;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class CreateIndexCompiler {
    private final PhoenixStatement statement;
    private final Operation operation;

    public CreateIndexCompiler(PhoenixStatement statement, Operation operation) {
        this.statement = statement;
        this.operation = operation;
    }


    private static class IndexWhereParseNodeVisitor extends StatelessTraverseAllParseNodeVisitor {
        private boolean  hasSubquery = false;

        @Override
        public Void visit(SubqueryParseNode node) throws SQLException {
            hasSubquery = true;
            return null;
        }
    }
    private String getValue(PDataType type) {
        if (type instanceof PNumericType || type instanceof PTimestamp ||
                type instanceof PUnsignedTime) {
            return "0";
        } else if (type instanceof PChar || type instanceof PVarchar) {
            return "'a'";
        } else if (type instanceof PDate || type instanceof PTime) {
            return (new Date(System.currentTimeMillis())).toString();
        } else {
            return "0x00";
        }
    }
    private void verifyIndexWhere(ParseNode indexWhere, StatementContext context,
            TableName dataTableName) throws SQLException {
        if (indexWhere == null) {
            return;
        }
        PhoenixConnection connection = context.getConnection();
        IndexWhereParseNodeVisitor indexWhereParseNodeVisitor = new IndexWhereParseNodeVisitor();
        indexWhere.accept(indexWhereParseNodeVisitor);
        if (indexWhereParseNodeVisitor.hasSubquery) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_INDEX_WHERE_WITH_SUBQUERY).
                    build().buildException();
        }
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(context);
        Expression indexWhereExpression = indexWhere.accept(expressionCompiler);
        PTable dataTable = PhoenixRuntime.getTable(connection, dataTableName.toString());

        boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        StringBuilder stringBuilder = new StringBuilder("UPSERT INTO ");
        stringBuilder.append(dataTableName);
        stringBuilder.append(" Values(");
        int i = dataTable.getBucketNum() != null ? 1 : 0;
        for (; i < dataTable.getColumns().size() - 1; i++) {
            PDataType dataType = dataTable.getColumns().get(i).getDataType();
            stringBuilder.append(getValue(dataType) + ",");
        }
        PDataType dataType = dataTable.getColumns().get(i).getDataType();
        stringBuilder.append(getValue(dataType)+ ")");
        PreparedStatement ps = context.getConnection().prepareStatement(stringBuilder.toString());
        ps.execute();
        Iterator<Pair<byte[],List<Cell>>> dataTableNameAndMutationIterator =
                PhoenixRuntime.getUncommittedDataIterator(connection);
        Pair<byte[], List<Cell>> dataTableNameAndMutation = null;
        while (dataTableNameAndMutationIterator.hasNext()) {
            dataTableNameAndMutation = dataTableNameAndMutationIterator.next();
            if (!java.util.Arrays.equals(dataTableNameAndMutation.getFirst(),
                    dataTableName.toString().getBytes())) {
                dataTableNameAndMutation = null;
            }
        }
        if (dataTableNameAndMutation == null) {
            ps.close();
            connection.setAutoCommit(autoCommit);
            throw new RuntimeException("Unexpected result from " +
                    "PhoenixRuntime#getUncommittedDataIterator for " + dataTableName);
        }
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
        MultiKeyValueTuple tuple = new MultiKeyValueTuple(dataTableNameAndMutation.getSecond());
        if (!indexWhereExpression.evaluate(tuple, ptr)) {
            ps.close();
            connection.setAutoCommit(autoCommit);
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_EVALUATE_INDEX_WHERE).
                    build().buildException();
        }
        ps.close();
        connection.setAutoCommit(autoCommit);
    }
    public MutationPlan compile(final CreateIndexStatement create) throws SQLException {
        final PhoenixConnection connection = statement.getConnection();
        final ColumnResolver resolver = FromCompiler.getResolver(create, connection, create.getUdfParseNodes());
        Scan scan = new Scan();
        final StatementContext context = new StatementContext(statement, resolver, scan, new SequenceManager(statement));
        verifyIndexWhere(create.getWhere(), context, create.getTable().getName());
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(context);
        List<ParseNode> splitNodes = create.getSplitNodes();
        if (create.getIndexType() == IndexType.LOCAL) {
            if (!splitNodes.isEmpty()) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_SPLIT_LOCAL_INDEX)
                .build().buildException();
            } 
            List<Pair<String, Object>> list = create.getProps() != null ? create.getProps().get("") : null;
            if (list != null) {
                for (Pair<String, Object> pair : list) {
                    if (pair.getFirst().equals(PhoenixDatabaseMetaData.SALT_BUCKETS)) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_SALT_LOCAL_INDEX)
                        .build().buildException();
                    }
                }
            }
        }
        final byte[][] splits = new byte[splitNodes.size()][];
        for (int i = 0; i < splits.length; i++) {
            ParseNode node = splitNodes.get(i);
            if (!node.isStateless()) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.SPLIT_POINT_NOT_CONSTANT)
                    .setMessage("Node: " + node).build().buildException();
            }
            LiteralExpression expression = (LiteralExpression)node.accept(expressionCompiler);
            splits[i] = expression.getBytes();
        }
        final MetaDataClient client = new MetaDataClient(connection);
        
        return new BaseMutationPlan(context, operation) {
            @Override
            public MutationState execute() throws SQLException {
                return client.createIndex(create, splits);
            }

            @Override
            public ExplainPlan getExplainPlan() throws SQLException {
                return new ExplainPlan(Collections.singletonList("CREATE INDEX"));
            }
        	
        };
    }
}