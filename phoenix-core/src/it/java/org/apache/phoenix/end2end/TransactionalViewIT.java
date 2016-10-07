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
package org.apache.phoenix.end2end;

import static org.apache.phoenix.util.TestUtil.analyzeTable;
import static org.apache.phoenix.util.TestUtil.getAllSplits;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.List;

import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Before;
import org.junit.Test;

public class TransactionalViewIT extends ParallelStatsEnabledIT {

    private String fullTableName;
    private String fullViewName;

    @Before
    public void generateTableNames() {
        String schemaName = TestUtil.DEFAULT_SCHEMA_NAME;
        String tableName = "T_" + generateUniqueName();
        fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String viewName = "V_" + generateUniqueName();
        fullViewName = SchemaUtil.getTableName(schemaName, viewName);
    }

    @Test
    public void testReadOwnWritesWithStats() throws Exception {
        try (Connection conn1 = DriverManager.getConnection(getUrl()); 
                Connection conn2 = DriverManager.getConnection(getUrl())) {
            String ddl = "CREATE TABLE " + fullTableName
                    + " (k INTEGER NOT NULL PRIMARY KEY, v1 DATE) TRANSACTIONAL=true";
            conn1.createStatement().execute(ddl);
            ddl = "CREATE VIEW " + fullViewName + " (v2 VARCHAR) AS SELECT * FROM " + fullTableName + " where k>5";
            conn1.createStatement().execute(ddl);
            for (int i = 0; i < 10; i++) {
                conn1.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES(" + i + ")");
            }
    
            // verify you can read your own writes
            int count = 0;
            ResultSet rs = conn1.createStatement().executeQuery("SELECT k FROM " + fullTableName);
            while (rs.next()) {
                assertEquals(count++, rs.getInt(1));
            }
            assertEquals(10, count);
            
            count = 0;
            rs = conn1.createStatement().executeQuery("SELECT k FROM " + fullViewName);
            while (rs.next()) {
                assertEquals(6+count++, rs.getInt(1));
            }
            assertEquals(4, count);
            
            // verify stats can see the read own writes rows
            analyzeTable(conn2, fullViewName, true);
            List<KeyRange> splits = getAllSplits(conn2, fullViewName);
            assertEquals(4, splits.size());
        }
    }
    
    @Test
    public void testInvalidRowsWithStats() throws Exception {
        try (Connection conn1 = DriverManager.getConnection(getUrl()); 
                Connection conn2 = DriverManager.getConnection(getUrl())) {
            String ddl = "CREATE TABLE " + fullTableName
                    + " (k INTEGER NOT NULL PRIMARY KEY, v1 DATE) TRANSACTIONAL=true";
            conn1.createStatement().execute(ddl);
            ddl = "CREATE VIEW " + fullViewName + " (v2 VARCHAR) AS SELECT * FROM " + fullTableName + " where k>5";
            conn1.createStatement().execute(ddl);
            for (int i = 0; i < 10; i++) {
                conn1.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES(" + i + ")");
            }
    
            // verify you can read your own writes
            int count = 0;
            ResultSet rs = conn1.createStatement().executeQuery("SELECT k FROM " + fullTableName);
            while (rs.next()) {
                assertEquals(count++, rs.getInt(1));
            }
            assertEquals(10, count);
            
            count = 0;
            rs = conn1.createStatement().executeQuery("SELECT k FROM " + fullViewName);
            while (rs.next()) {
                assertEquals(6+count++, rs.getInt(1));
            }
            assertEquals(4, count);
            
            // Thread.sleep(DEFAULT_TXN_TIMEOUT_SECONDS*1000+20000);
            // assertEquals("There should be one invalid transaction", 1, txManager.getInvalidSize());
            
            // verify stats can see the rows from the invalid transaction
            analyzeTable(conn2, fullViewName, true);
            List<KeyRange> splits = getAllSplits(conn2, fullViewName);
            assertEquals(4, splits.size());
        }
    }
    
}
