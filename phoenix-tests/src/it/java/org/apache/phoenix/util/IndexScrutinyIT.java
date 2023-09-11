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
package org.apache.phoenix.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class IndexScrutinyIT extends ParallelStatsDisabledIT {
    @Test
    public void testRowCountIndexScrutiny() throws Throwable {
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + fullTableName + "(k VARCHAR PRIMARY KEY, v VARCHAR) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true, SALT_BUCKETS=2");
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullTableName + " (v)");
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('b','bb')");
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','ccc')");
            conn.commit();
            
            int count = conn.createStatement().executeUpdate("DELETE FROM " + fullIndexName + " WHERE \":K\"='a' AND \"0:V\"='ccc'");
            assertEquals(1,count);
            conn.commit();
            try {
                IndexScrutiny.scrutinizeIndex(conn, fullTableName, fullIndexName);
                fail();
            } catch (AssertionError e) {
                assertEquals("Expected data table row count to match expected:<2> but was:<1>", e.getMessage());
            }
        }
    }
    @Test
    public void testExtraRowIndexScrutiny() throws Throwable {
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + fullTableName + "(k VARCHAR PRIMARY KEY, v VARCHAR, v2 VARCHAR) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true");
            conn.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + fullTableName + " (v) INCLUDE (v2)");
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('b','bb','0')");
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','ccc','1')");
            conn.commit();
            
            conn.createStatement().executeUpdate("UPSERT INTO " + fullIndexName + " VALUES ('bbb','x','0')");
            conn.commit();
            try {
                IndexScrutiny.scrutinizeIndex(conn, fullTableName, fullIndexName);
                fail();
            } catch (AssertionError e) {
                assertEquals("Expected to find PK in data table: ('x')", e.getMessage());
            }
        }
    }
    
    @Test
    public void testValueIndexScrutiny() throws Throwable {
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + fullTableName + "(k VARCHAR PRIMARY KEY, v VARCHAR, v2 VARCHAR) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true");
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullTableName + " (v) INCLUDE (v2)");
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('b','bb','0')");
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','ccc','1')");
            conn.commit();

            // Writing index directly will generate unverified rows. These rows will recovered if there exists the
            // corresponding data row
            conn.createStatement().executeUpdate("UPSERT INTO " + fullIndexName + " VALUES ('ccc','a','2')");
            conn.commit();
            assertEquals(2, IndexScrutiny.scrutinizeIndex(conn, fullTableName, fullIndexName));
        }
    }
}
