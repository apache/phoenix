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
import java.util.Map;

import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;

public class TransactionalViewIT extends BaseOwnClusterHBaseManagedTimeIT {

    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(3);
        props.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Integer.toString(20));
        props.put(QueryServices.QUEUE_SIZE_ATTRIB, Integer.toString(1024));
        props.put(QueryServices.TRANSACTIONS_ENABLED, Boolean.toString(true));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }
    
    @Test
    public void testReadOwnWritesWithStats() throws Exception {
        try (Connection conn1 = DriverManager.getConnection(getUrl()); 
                Connection conn2 = DriverManager.getConnection(getUrl())) {
            String ddl = "CREATE TABLE t (k INTEGER NOT NULL PRIMARY KEY, v1 DATE) TRANSACTIONAL=true";
            conn1.createStatement().execute(ddl);
            ddl = "CREATE VIEW v (v2 VARCHAR) AS SELECT * FROM t where k>5";
            conn1.createStatement().execute(ddl);
            for (int i = 0; i < 10; i++) {
                conn1.createStatement().execute("UPSERT INTO t VALUES(" + i + ")");
            }
    
            // verify you can read your own writes
            int count = 0;
            ResultSet rs = conn1.createStatement().executeQuery("SELECT k FROM t");
            while (rs.next()) {
                assertEquals(count++, rs.getInt(1));
            }
            assertEquals(10, count);
            
            count = 0;
            rs = conn1.createStatement().executeQuery("SELECT k FROM v");
            while (rs.next()) {
                assertEquals(6+count++, rs.getInt(1));
            }
            assertEquals(4, count);
            
            // verify stats can see the read own writes rows
            analyzeTable(conn2, "v", true);
            List<KeyRange> splits = getAllSplits(conn2, "v");
            assertEquals(4, splits.size());
        }
    }
    
    @Test
    public void testInvalidRowsWithStats() throws Exception {
        try (Connection conn1 = DriverManager.getConnection(getUrl()); 
                Connection conn2 = DriverManager.getConnection(getUrl())) {
            String ddl = "CREATE TABLE t (k INTEGER NOT NULL PRIMARY KEY, v1 DATE) TRANSACTIONAL=true";
            conn1.createStatement().execute(ddl);
            ddl = "CREATE VIEW v (v2 VARCHAR) AS SELECT * FROM t where k>5";
            conn1.createStatement().execute(ddl);
            for (int i = 0; i < 10; i++) {
                conn1.createStatement().execute("UPSERT INTO t VALUES(" + i + ")");
            }
    
            // verify you can read your own writes
            int count = 0;
            ResultSet rs = conn1.createStatement().executeQuery("SELECT k FROM t");
            while (rs.next()) {
                assertEquals(count++, rs.getInt(1));
            }
            assertEquals(10, count);
            
            count = 0;
            rs = conn1.createStatement().executeQuery("SELECT k FROM v");
            while (rs.next()) {
                assertEquals(6+count++, rs.getInt(1));
            }
            assertEquals(4, count);
            
            Thread.sleep(DEFAULT_TXN_TIMEOUT_SECONDS*1000+20000);
            assertEquals("There should be one invalid transaction", 1, txManager.getInvalidSize());
            
            // verify stats can see the rows from the invalid transaction
            analyzeTable(conn2, "v", true);
            List<KeyRange> splits = getAllSplits(conn2, "v");
            assertEquals(4, splits.size());
        }
    }
    
}
