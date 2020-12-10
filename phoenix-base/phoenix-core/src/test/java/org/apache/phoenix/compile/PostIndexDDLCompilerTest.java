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

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.TableRef;
import org.junit.Test;

public class PostIndexDDLCompilerTest extends BaseConnectionlessQueryTest {

    @Test
    public void testHintInSubquery() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            setupTables(conn);
            PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
            String query = "UPSERT /*+ NO_INDEX */ INTO T(k, v1) SELECT /*+ NO_INDEX */  k,v1 FROM T WHERE v1 = '4'";
            MutationPlan plan = stmt.compileMutation(query);
            assertEquals("T", plan.getQueryPlan().getTableRef().getTable().getTableName().getString());
            query = "UPSERT INTO T(k, v1) SELECT /*+ NO_INDEX */  k,v1 FROM T WHERE v1 = '4'";
            plan = stmt.compileMutation(query);
            // TODO the following should actually use data table T if we supported hints in subqueries
            assertEquals("IDX", plan.getQueryPlan().getTableRef().getTable().getTableName().getString());
        }
    }

    @Test
    public void testCompile() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            setupTables(conn);
            PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);
            PTable pDataTable = pConn.getTable(new PTableKey(null, "T"));
            PostIndexDDLCompiler compiler = new PostIndexDDLCompiler(pConn, new TableRef(pDataTable));
            MutationPlan plan = compiler.compile(pConn.getTable(new PTableKey(null, "IDX")));
            assertEquals("T", plan.getQueryPlan().getTableRef().getTable().getTableName().getString());
        }
    }

    private void setupTables(Connection conn) throws SQLException {
        conn.createStatement().execute("CREATE TABLE T (k VARCHAR NOT NULL PRIMARY KEY, v1 CHAR(15), v2 VARCHAR)");
        conn.createStatement().execute("CREATE INDEX IDX ON T(v1, v2)");
    }

}
