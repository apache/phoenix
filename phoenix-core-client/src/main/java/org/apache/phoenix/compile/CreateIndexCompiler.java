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
import org.apache.hadoop.hbase.CellComparator;
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
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.tuple.MultiKeyValueTuple;
import org.apache.phoenix.schema.types.PArrayDataType;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PDateArray;
import org.apache.phoenix.schema.types.PNumericType;
import org.apache.phoenix.schema.types.PTime;
import org.apache.phoenix.schema.types.PTimeArray;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PTimestampArray;
import org.apache.phoenix.schema.types.PUnsignedDate;
import org.apache.phoenix.schema.types.PUnsignedDateArray;
import org.apache.phoenix.schema.types.PUnsignedTime;
import org.apache.phoenix.schema.types.PUnsignedTimeArray;
import org.apache.phoenix.schema.types.PUnsignedTimestamp;
import org.apache.phoenix.schema.types.PUnsignedTimestampArray;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class CreateIndexCompiler {
    private final PhoenixStatement statement;
    private final Operation operation;

    public CreateIndexCompiler(PhoenixStatement statement, Operation operation) {
        this.statement = statement;
        this.operation = operation;
    }

    /**
     * This is to check if the index where clause has a subquery
     */
    private static class IndexWhereParseNodeVisitor extends StatelessTraverseAllParseNodeVisitor {
        private boolean  hasSubquery = false;

        @Override
        public Void visit(SubqueryParseNode node) throws SQLException {
            hasSubquery = true;
            return null;
        }
    }

    private String getValue(PDataType type) {
        if (type instanceof PNumericType) {
            return "0";
        } else if (type instanceof PChar || type instanceof PVarchar) {
            return "'a'";
        } else if (type instanceof PDate || type instanceof PUnsignedDate || type instanceof PTime
                || type instanceof PUnsignedTime || type instanceof PTimestamp
                || type instanceof PUnsignedTimestamp) {
            Timestamp now = new Timestamp(EnvironmentEdgeManager.currentTimeMillis());
            return "TO_DATE('" + now + "','yyyy-MM-dd HH:mm:ss.SSS', 'PST')";
        } else if (type instanceof PBoolean) {
            return "TRUE";
        } else if (type instanceof PDateArray || type instanceof PUnsignedDateArray
                || type instanceof PTimeArray || type instanceof PUnsignedTimeArray
                || type instanceof PTimestampArray || type instanceof PUnsignedTimestampArray) {
            return "ARRAY[" + getValue(PDate.INSTANCE) + "]";
        } else if (type instanceof PArrayDataType) {
            return "ARRAY" + type.getSampleValue().toString();
        } else {
            return "0123";
        }
    }

    /**
     * Verifies that index WHERE clause does not include a subquery and it can
     * be evaluated on a single data table row.
     *
     */
    private void verifyIndexWhere(ParseNode indexWhere, StatementContext context,
            TableName dataTableName) throws SQLException {
        if (indexWhere == null) {
            return;
        }
        // Verify that index WHERE clause does not include a subquery
        PhoenixConnection connection = context.getConnection();
        IndexWhereParseNodeVisitor indexWhereParseNodeVisitor = new IndexWhereParseNodeVisitor();
        indexWhere.accept(indexWhereParseNodeVisitor);
        if (indexWhereParseNodeVisitor.hasSubquery) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_INDEX_WHERE_WITH_SUBQUERY).
                    build().buildException();
        }

        // Verify that index WHERE clause can be evaluated on a single data table row

        // Convert the index WHERE ParseNode to an Expression
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(context);
        Expression indexWhereExpression = indexWhere.accept(expressionCompiler);
        PTable dataTable = connection.getTable(dataTableName.toString());

        // Create a full data table row. Skip generating values for view constants as they
        // will be generated by the Upsert compiler
        boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        StringBuilder stringBuilder = new StringBuilder("UPSERT INTO ");
        stringBuilder.append(dataTableName);
        int startingColumnIndex = dataTable.getBucketNum() != null ? 1 : 0;
        startingColumnIndex += dataTable.getTenantId() != null ? 1 : 0;
        stringBuilder.append(" (");
        PColumn column;
        byte[] value;
        int i = startingColumnIndex;
        for (; i < dataTable.getColumns().size() - 1; i++) {
            column =  dataTable.getColumns().get(i);
            value = column.getViewConstant();
            if (value == null) {
                stringBuilder.append(SchemaUtil.getEscapedArgument(column.getName().getString())
                        + ",");
            }
        }
        column =  dataTable.getColumns().get(i);
        value = column.getViewConstant();
        if (value == null) {
            stringBuilder.append(SchemaUtil.getEscapedArgument(column.getName().getString()) + ")");
        } else {
            stringBuilder.append(")");
        }

        stringBuilder.append(" Values(");
        i = startingColumnIndex;
        for (; i < dataTable.getColumns().size() - 1; i++) {
            column =  dataTable.getColumns().get(i);
            value = column.getViewConstant();
            if (value == null) {
                PDataType dataType = column.getDataType();
                stringBuilder.append(getValue(dataType) + ",");
            }
        }
        column =  dataTable.getColumns().get(i);
        value = column.getViewConstant();
        if (value == null) {
            PDataType dataType = column.getDataType();
            stringBuilder.append(getValue(dataType) + ")");
        } else {
            stringBuilder.append(")");
        }

        try (PreparedStatement ps =
                context.getConnection().prepareStatement(stringBuilder.toString())) {
            ps.execute();
            Iterator<Pair<byte[], List<Cell>>> dataTableNameAndMutationIterator =
                    PhoenixRuntime.getUncommittedDataIterator(connection);
            Pair<byte[], List<Cell>> dataTableNameAndMutation = null;
            while (dataTableNameAndMutationIterator.hasNext()) {
                dataTableNameAndMutation = dataTableNameAndMutationIterator.next();
                if (java.util.Arrays.equals(dataTableNameAndMutation.getFirst(),
                        dataTable.getPhysicalName().getBytes())) {
                    break;
                } else {
                    dataTableNameAndMutation = null;
                }
            }
            if (dataTableNameAndMutation == null) {
                throw new RuntimeException(
                        "Unexpected result from " + "PhoenixRuntime#getUncommittedDataIterator for "
                                + dataTableName);
            }

            // Evaluate the WHERE expression on the data table row
            ImmutableBytesWritable ptr = new ImmutableBytesWritable();
            ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
            List<Cell> cols = dataTableNameAndMutation.getSecond();
            Collections.sort(cols, CellComparator.getInstance());
            MultiKeyValueTuple tuple = new MultiKeyValueTuple(cols);
            if (!indexWhereExpression.evaluate(tuple, ptr)) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_EVALUATE_INDEX_WHERE).
                        build().buildException();
            }
        } finally {
            connection.setAutoCommit(autoCommit);
        }
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