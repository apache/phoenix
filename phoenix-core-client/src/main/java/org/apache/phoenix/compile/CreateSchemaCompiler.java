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
package org.apache.phoenix.compile;

import java.sql.SQLException;
import java.util.Collections;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.CreateSchemaStatement;
import org.apache.phoenix.schema.MetaDataClient;

public class CreateSchemaCompiler {

  private final PhoenixStatement statement;

  public CreateSchemaCompiler(PhoenixStatement statement) {
    this.statement = statement;
  }

  public MutationPlan compile(final CreateSchemaStatement create) throws SQLException {
    final PhoenixConnection connection = statement.getConnection();
    final StatementContext context = new StatementContext(statement);
    final MetaDataClient client = new MetaDataClient(connection);
    return new CreateSchemaMutationPlan(context, create, client, connection);
  }

  private static class CreateSchemaMutationPlan extends BaseMutationPlan {

    private final StatementContext context;
    private final CreateSchemaStatement create;
    private final MetaDataClient client;
    private final PhoenixConnection connection;

    private CreateSchemaMutationPlan(StatementContext context, CreateSchemaStatement create,
      MetaDataClient client, PhoenixConnection connection) {
      super(context, create.getOperation());
      this.context = context;
      this.create = create;
      this.client = client;
      this.connection = connection;
    }

    @Override
    public MutationState execute() throws SQLException {
      try {
        return client.createSchema(create);
      } finally {
        if (client.getConnection() != connection) {
          client.getConnection().close();
        }
      }
    }

    @Override
    public ExplainPlan getExplainPlan() throws SQLException {
      return new ExplainPlan(Collections.singletonList("CREATE SCHEMA"));
    }

    @Override
    public StatementContext getContext() {
      return context;
    }
  }
}
