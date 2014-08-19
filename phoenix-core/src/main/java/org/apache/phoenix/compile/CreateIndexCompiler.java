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
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.CreateIndexStatement;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.PropertyName;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PTable.IndexType;

public class CreateIndexCompiler {
    private final PhoenixStatement statement;

    public CreateIndexCompiler(PhoenixStatement statement) {
        this.statement = statement;
    }

    public MutationPlan compile(final CreateIndexStatement create) throws SQLException {
        final PhoenixConnection connection = statement.getConnection();
        final ColumnResolver resolver = FromCompiler.getResolver(create, connection);
        Scan scan = new Scan();
        final StatementContext context = new StatementContext(statement, resolver, scan, new SequenceManager(statement));
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
        
        return new MutationPlan() {

            @Override
            public ParameterMetaData getParameterMetaData() {
                return context.getBindManager().getParameterMetaData();
            }

            @Override
            public PhoenixConnection getConnection() {
                return connection;
            }

            @Override
            public MutationState execute() throws SQLException {
                return client.createIndex(create, splits);
            }

            @Override
            public ExplainPlan getExplainPlan() throws SQLException {
                return new ExplainPlan(Collections.singletonList("CREATE INDEX"));
            }

            @Override
            public StatementContext getContext() {
                return context;
            }
        };
    }
}
