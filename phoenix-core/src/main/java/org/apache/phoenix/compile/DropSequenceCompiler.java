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

import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixParameterMetaData;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.DropSequenceStatement;
import org.apache.phoenix.schema.MetaDataClient;


public class DropSequenceCompiler {
    private final PhoenixStatement statement;

    public DropSequenceCompiler(PhoenixStatement statement) {
        this.statement = statement;
    }
    

    public MutationPlan compile(final DropSequenceStatement sequence) throws SQLException {
        final PhoenixConnection connection = statement.getConnection();
        final MetaDataClient client = new MetaDataClient(connection);        
        final StatementContext context = new StatementContext(statement);
        return new MutationPlan() {           

            @Override
            public MutationState execute() throws SQLException {
                return client.dropSequence(sequence);
            }

            @Override
            public ExplainPlan getExplainPlan() throws SQLException {
                return new ExplainPlan(Collections.singletonList("DROP SEQUENCE"));
            }

            @Override
            public StatementContext getContext() {
                return context;
            }

            @Override
            public PhoenixConnection getConnection() {
                return connection;
            }

            @Override
            public ParameterMetaData getParameterMetaData() {                
                return PhoenixParameterMetaData.EMPTY_PARAMETER_META_DATA;
            }

        };
    }
}