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

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.jdbc.PhoenixStatement.Operation;
import org.apache.phoenix.parse.CreateIndexStatement;
import org.apache.phoenix.parse.DeclareCursorStatement;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.util.CursorUtil;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PTable.IndexType;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

public class DeclareCursorCompiler {
    private final PhoenixStatement statement;
    private final Operation operation;
    private QueryPlan queryPlan;

    public DeclareCursorCompiler(PhoenixStatement statement, Operation operation, QueryPlan queryPlan) {
        this.statement = statement;
        this.operation = operation;
        this.queryPlan = queryPlan;
    }

    public MutationPlan compile(final DeclareCursorStatement declare) throws SQLException {
        if(declare.getBindCount() != 0){
            throw new SQLException("Cannot declare cursor, internal SELECT statement contains bindings!");
        }

        final PhoenixConnection connection = statement.getConnection();
        final StatementContext context = new StatementContext(statement);
        final MetaDataClient client = new MetaDataClient(connection);
        
        return new BaseMutationPlan(context, operation) {
            @Override
            public MutationState execute() throws SQLException {
                return client.declareCursor(declare, queryPlan);
            }

            @Override
            public ExplainPlan getExplainPlan() throws SQLException {
                return new ExplainPlan(Collections.singletonList("DECLARE CURSOR"));
            }
        };
    }
}
