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
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.BaseUniqueNamesOwnClusterIT;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;

public class PartialIndexRebuilderIT extends BaseUniqueNamesOwnClusterIT {
    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(10);
        serverProps.put(QueryServices.INDEX_FAILURE_HANDLING_REBUILD_ATTRIB, Boolean.TRUE.toString());
        serverProps.put(QueryServices.INDEX_FAILURE_HANDLING_REBUILD_INTERVAL_ATTRIB, "1000");
        serverProps.put(QueryServices.INDEX_REBUILD_DISABLE_TIMESTAMP_THRESHOLD, "30000"); // give up rebuilding after 30 seconds
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()), ReadOnlyProps.EMPTY_PROPS);
    }


    @Test
    public void testRowCountIndexScrutiny() throws Throwable {
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + fullTableName + "(k VARCHAR PRIMARY KEY, v VARCHAR) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true");
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullTableName + " (v)");
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('b','bb')");
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','ccc')");
            conn.commit();
            
            int count = conn.createStatement().executeUpdate("DELETE FROM " + fullIndexName + " WHERE \":K\"='a' AND \"0:V\"='ccc'");
            assertEquals(1,count);
            conn.commit();
            try {
                TestUtil.scutinizeIndex(conn, fullTableName, fullIndexName);
                fail();
            } catch (AssertionError e) {
                assertEquals(e.getMessage(),"Expected data table row count to match expected:<1> but was:<2>");
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
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullTableName + " (v) INCLUDE (v2)");
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('b','bb','0')");
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','ccc','1')");
            conn.commit();
            
            conn.createStatement().executeUpdate("UPSERT INTO " + fullIndexName + " VALUES ('bbb','x','0')");
            conn.commit();
            try {
                TestUtil.scutinizeIndex(conn, fullTableName, fullIndexName);
                fail();
            } catch (AssertionError e) {
                assertEquals(e.getMessage(),"Expected to find PK in data table: ('x')");
            }
        }
    }
    
    @Test
    public void testValuetIndexScrutiny() throws Throwable {
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
            
            conn.createStatement().executeUpdate("UPSERT INTO " + fullIndexName + " VALUES ('ccc','a','2')");
            conn.commit();
            try {
                TestUtil.scutinizeIndex(conn, fullTableName, fullIndexName);
                fail();
            } catch (AssertionError e) {
                assertEquals(e.getMessage(),"Expected equality for V2, but '2'!='1'");
            }
        }
    }

    @Test
    public void testMultiVersionsAfterFailure() throws Throwable {
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + fullTableName + "(k VARCHAR PRIMARY KEY, v VARCHAR) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true");
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullTableName + " (v)");
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','bb')");
            conn.commit();
            long disableTS = EnvironmentEdgeManager.currentTimeMillis();
            HTableInterface metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            IndexUtil.updateIndexState(fullIndexName, disableTS, metaTable, PIndexState.DISABLE);
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','ccc')");
            conn.commit();
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','dddd')");
            conn.commit();
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','eeeee')");
            conn.commit();
            TestUtil.waitForIndexRebuild(conn, fullIndexName, PIndexState.ACTIVE);
            TestUtil.dumpTable(conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(fullTableName)));
            TestUtil.dumpTable(conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(fullIndexName)));

            TestUtil.scutinizeIndex(conn, fullTableName, fullIndexName);
        }
    }
    
    @Test
    public void testUpsertNullAfterFailure() throws Throwable {
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + fullTableName + "(k VARCHAR PRIMARY KEY, v VARCHAR) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true");
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullTableName + " (v)");
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','a')");
            conn.commit();
            long disableTS = EnvironmentEdgeManager.currentTimeMillis();
            HTableInterface metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            IndexUtil.updateIndexState(fullIndexName, disableTS, metaTable, PIndexState.DISABLE);
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a',null)");
            conn.commit();
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','bb')");
            conn.commit();
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','ccc')");
            conn.commit();
            TestUtil.waitForIndexRebuild(conn, fullIndexName, PIndexState.ACTIVE);
            TestUtil.dumpTable(conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(fullTableName)));
            TestUtil.dumpTable(conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(fullIndexName)));

            TestUtil.scutinizeIndex(conn, fullTableName, fullIndexName);
        }
    }
    
    @Test
    public void testUpsertNullTwiceAfterFailure() throws Throwable {
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + fullTableName + "(k VARCHAR PRIMARY KEY, v VARCHAR) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true");
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullTableName + " (v)");
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a',null)");
            conn.commit();
            long disableTS = EnvironmentEdgeManager.currentTimeMillis();
            HTableInterface metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            IndexUtil.updateIndexState(fullIndexName, disableTS, metaTable, PIndexState.DISABLE);
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','bb')");
            conn.commit();
            conn.createStatement().execute("DELETE FROM " + fullTableName);
            conn.commit();
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a',null)");
            conn.commit();
            TestUtil.waitForIndexRebuild(conn, fullIndexName, PIndexState.ACTIVE);
            TestUtil.dumpTable(conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(fullTableName)));
            TestUtil.dumpTable(conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(fullIndexName)));

            TestUtil.scutinizeIndex(conn, fullTableName, fullIndexName);
        }
    }
    
    @Test
    public void testDeleteAfterFailure() throws Throwable {
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + fullTableName + "(k VARCHAR PRIMARY KEY, v VARCHAR) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true");
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullTableName + " (v)");
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a',null)");
            conn.commit();
            long disableTS = EnvironmentEdgeManager.currentTimeMillis();
            HTableInterface metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            IndexUtil.updateIndexState(fullIndexName, disableTS, metaTable, PIndexState.DISABLE);
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','b')");
            conn.commit();
            conn.createStatement().execute("DELETE FROM " + fullTableName);
            conn.commit();
            TestUtil.waitForIndexRebuild(conn, fullIndexName, PIndexState.ACTIVE);
            TestUtil.dumpTable(conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(fullTableName)));
            TestUtil.dumpTable(conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(fullIndexName)));

            TestUtil.scutinizeIndex(conn, fullTableName, fullIndexName);
       }
    }
    
    @Test
    public void testDeleteBeforeFailure() throws Throwable {
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + fullTableName + "(k VARCHAR PRIMARY KEY, v VARCHAR) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true");
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullTableName + " (v)");
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a',null)");
            conn.commit();
            conn.createStatement().execute("DELETE FROM " + fullTableName);
            conn.commit();
            long disableTS = EnvironmentEdgeManager.currentTimeMillis();
            HTableInterface metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            IndexUtil.updateIndexState(fullIndexName, disableTS, metaTable, PIndexState.DISABLE);
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','b')");
            conn.commit();
            TestUtil.waitForIndexRebuild(conn, fullIndexName, PIndexState.ACTIVE);
            TestUtil.dumpTable(conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(fullTableName)));
            TestUtil.dumpTable(conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(fullIndexName)));

            TestUtil.scutinizeIndex(conn, fullTableName, fullIndexName);
        }
    }
    
    @Test
    public void testMultiValuesAtSameTS() throws Throwable {
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + fullTableName + "(k VARCHAR PRIMARY KEY, v VARCHAR) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true");
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullTableName + " (v)");
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','a')");
            conn.commit();
            long disableTS = EnvironmentEdgeManager.currentTimeMillis();
            HTableInterface metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            IndexUtil.updateIndexState(fullIndexName, disableTS, metaTable, PIndexState.DISABLE);
            Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(disableTS));
            try (Connection conn2 = DriverManager.getConnection(getUrl(), props)) {
                conn2.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','bb')");
                conn2.commit();
                conn2.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','ccc')");
                conn2.commit();
            }
            TestUtil.waitForIndexRebuild(conn, fullIndexName, PIndexState.ACTIVE);
            TestUtil.dumpTable(conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(fullTableName)));
            TestUtil.dumpTable(conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(fullIndexName)));

            TestUtil.scutinizeIndex(conn, fullTableName, fullIndexName);
        }
    }
    
    @Test
    public void testDeleteAndUpsertValuesAtSameTS1() throws Throwable {
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + fullTableName + "(k VARCHAR PRIMARY KEY, v VARCHAR) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true");
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullTableName + " (v)");
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','a')");
            conn.commit();
            long disableTS = EnvironmentEdgeManager.currentTimeMillis();
            HTableInterface metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            IndexUtil.updateIndexState(fullIndexName, disableTS, metaTable, PIndexState.DISABLE);
            Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(disableTS));
            try (Connection conn2 = DriverManager.getConnection(getUrl(), props)) {
                conn2.createStatement().execute("DELETE FROM " + fullTableName + " WHERE k='a'");
                conn2.commit();
                conn2.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','ccc')");
                conn2.commit();
            }
            TestUtil.waitForIndexRebuild(conn, fullIndexName, PIndexState.ACTIVE);
            TestUtil.dumpTable(conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(fullTableName)));
            TestUtil.dumpTable(conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(fullIndexName)));

            TestUtil.scutinizeIndex(conn, fullTableName, fullIndexName);
        }
    }
    
    @Test
    public void testDeleteAndUpsertValuesAtSameTS2() throws Throwable {
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + fullTableName + "(k VARCHAR PRIMARY KEY, v VARCHAR) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true");
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullTableName + " (v)");
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','a')");
            conn.commit();
            long disableTS = EnvironmentEdgeManager.currentTimeMillis();
            HTableInterface metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            IndexUtil.updateIndexState(fullIndexName, disableTS, metaTable, PIndexState.DISABLE);
            Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(disableTS));
            try (Connection conn2 = DriverManager.getConnection(getUrl(), props)) {
                conn2.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','ccc')");
                conn2.commit();
                conn2.createStatement().execute("DELETE FROM " + fullTableName + " WHERE k='a'");
                conn2.commit();
            }
            TestUtil.waitForIndexRebuild(conn, fullIndexName, PIndexState.ACTIVE);
            TestUtil.dumpTable(conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(fullTableName)));
            TestUtil.dumpTable(conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(fullIndexName)));

            TestUtil.scutinizeIndex(conn, fullTableName, fullIndexName);
        }
    }
}
