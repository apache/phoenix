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
package org.apache.phoenix.end2end.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.junit.Test;

public class AsyncIndexDisabledIT extends ParallelStatsDisabledIT {

    @Test
    public void testAsyncIndexRegularBuild() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(true);
            Statement stmt = conn.createStatement();
            String tableName = "TBL_" + generateUniqueName();
            String indexName = "IND_" + generateUniqueName();
            
            String ddl = "CREATE TABLE " + tableName + " (pk INTEGER NOT NULL PRIMARY KEY, val VARCHAR)";
            stmt.execute(ddl);
            stmt.execute("UPSERT INTO " + tableName + " values(1, 'y')");
            // create the async index
            stmt.execute("CREATE INDEX " + indexName + " ON " + tableName + "(val) ASYNC");
    
            // it should be built as a regular index
            PhoenixConnection phxConn = conn.unwrap(PhoenixConnection.class);
            PTable table = phxConn.getTable(new PTableKey(null, tableName));
            assertEquals("Index not built", 1, table.getIndexes().size());
            assertEquals("Wrong index created", indexName, table.getIndexes().get(0).getName().getString());
            
            ResultSet rs = stmt.executeQuery("select /*+ INDEX(" + indexName + ")*/ pk, val from " + tableName);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("y", rs.getString(2));
            assertFalse(rs.next());
        }
    }
    
}
