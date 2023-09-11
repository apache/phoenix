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
package org.apache.phoenix.schema;

import org.apache.phoenix.coprocessorclient.MetaDataProtocol;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.parse.CreateTableStatement;
import org.apache.phoenix.parse.PSchema;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.junit.BeforeClass;
import org.junit.Test;
import java.sql.DriverManager;
import java.sql.SQLException;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;

public class MetaDataClientTest extends BaseConnectionlessQueryTest {

    private static String schema;
    private static String baseTable;
    private static PhoenixConnection phxConn;
    private static MetaDataClient mockClient;
    private static String ddlFormat;
    private static CreateTableStatement stmt;

    @BeforeClass
    public static synchronized void setupTest() throws SQLException {
        schema = generateUniqueName();
        baseTable = generateUniqueName();
        phxConn = (PhoenixConnection) DriverManager.getConnection(getUrl());
        mockClient = new MetaDataClient(phxConn);
        ddlFormat = "CREATE TABLE " + schema + "." + baseTable + " " +
                "(A VARCHAR PRIMARY KEY, B BIGINT, C VARCHAR)";
        stmt = (CreateTableStatement)new SQLParser((ddlFormat)).parseStatement();
    }

    @Test
    public void testHandleCreateTableMutationCode() throws SQLException {
        MetaDataProtocol.MetaDataMutationResult result = new MetaDataProtocol.MetaDataMutationResult
                (MetaDataProtocol.MutationCode.UNALLOWED_TABLE_MUTATION ,new PSchema(schema),
            EnvironmentEdgeManager.currentTimeMillis());
        try {
            mockClient.handleCreateTableMutationCode(result, result.getMutationCode(), stmt,
                    schema, baseTable, null);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    //Testing the case when Mutation code thrown from sever is not handled by MetaDataClient
    public void testHandleCreateTableMutationCodeWithNewCode() throws SQLException {
        MetaDataProtocol.MetaDataMutationResult result = new MetaDataProtocol
                .MetaDataMutationResult(MetaDataProtocol.MutationCode.NO_PK_COLUMNS,
                new PSchema(schema), EnvironmentEdgeManager.currentTimeMillis());
        try {
            mockClient.handleCreateTableMutationCode(result, result.getMutationCode(), stmt,
                    schema, baseTable, null);
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.UNEXPECTED_MUTATION_CODE.getErrorCode(),
                    e.getErrorCode());
            assertTrue(e.getMessage().contains("NO_PK_COLUMNS"));
        }
    }

}
