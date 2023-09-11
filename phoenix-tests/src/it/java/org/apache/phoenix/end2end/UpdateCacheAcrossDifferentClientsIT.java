/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.phoenix.end2end;

import org.apache.phoenix.thirdparty.com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Table;
import org.apache.phoenix.exception.PhoenixIOException;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PMetaData;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(NeedsOwnMiniClusterTest.class)
public class UpdateCacheAcrossDifferentClientsIT extends BaseTest {

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HBaseTestingUtility hbaseTestUtil = new HBaseTestingUtility(conf);
        setUpConfigForMiniCluster(conf);
        conf.set(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB, QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        conf.set(QueryServices.DROP_METADATA_ATTRIB, Boolean.TRUE.toString());
        conf.set(QueryServices.MUTATE_BATCH_SIZE_ATTRIB, Integer.toString(3000));
        hbaseTestUtil.startMiniCluster();
        // establish url and quorum. Need to use PhoenixDriver and not PhoenixTestDriver
        String zkQuorum = "localhost:" + hbaseTestUtil.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;
        DriverManager.registerDriver(PhoenixDriver.INSTANCE);
    }

    @Test
    public void testUpdateCacheFrequencyWithAddAndDropTable() throws Exception {
        // Create connections 1 and 2
        Properties longRunningProps = new Properties(); // Must update config before starting server
        longRunningProps.put(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB,
            QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        longRunningProps.put(QueryServices.DROP_METADATA_ATTRIB, Boolean.TRUE.toString());
        Connection conn1 = DriverManager.getConnection(url, longRunningProps);
        String url2 = url + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + "LongRunningQueries";
        Connection conn2 = DriverManager.getConnection(url2, longRunningProps);
        conn1.setAutoCommit(true);
        conn2.setAutoCommit(true);
        String tableName = generateUniqueName();
        String tableCreateQuery =
                "create table "+tableName+" (k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)"
                + " UPDATE_CACHE_FREQUENCY=1000000000";
        String dropTableQuery = "DROP table "+tableName;
        try {
            conn1.createStatement().execute(tableCreateQuery);
            conn1.createStatement()
                    .execute("upsert into "+tableName+" values ('row1', 'value1', 'key1')");
            conn1.createStatement()
                    .execute("upsert into "+tableName+" values ('row2', 'value2', 'key2')");
            conn1.commit();
            ResultSet rs =conn1.createStatement()
                            .executeQuery("select * from "+tableName);
            assertTrue(rs.next());
            assertTrue(rs.next());
            assertFalse(rs.next());
            rs = conn2.createStatement().executeQuery("select * from "+tableName);
            assertTrue(rs.next());
            assertTrue(rs.next());
            assertFalse(rs.next());
            //Drop table from conn1
            conn1.createStatement().execute(dropTableQuery);
            try {
                rs = conn1.createStatement().executeQuery("select * from "+tableName);
                fail("Should throw TableNotFoundException after dropping table");
            } catch (TableNotFoundException e) {
                //Expected
            }
            rs = conn2.createStatement().executeQuery("select * from "+tableName);
            try {
                rs.next();
                fail("Should throw org.apache.hadoop.hbase.TableNotFoundException since the latest metadata " +
                        "wasn't fetched");
            } catch (PhoenixIOException ex) {
                boolean foundHBaseTableNotFound = false;
                for(Throwable throwable : Throwables.getCausalChain(ex)) {
                    if(org.apache.hadoop.hbase.TableNotFoundException.class.equals(throwable.getClass())) {
                        foundHBaseTableNotFound = true;
                        break;
                    }
                }
                assertTrue("Should throw org.apache.hadoop.hbase.TableNotFoundException since the latest" +
                        " metadata wasn't fetched", foundHBaseTableNotFound);
            }
        } finally {
            conn1.close();
            conn2.close();
        }
    }

    @Test
    public void testTableSentWhenIndexStateChanges() throws Throwable {
        // Create connections 1 and 2
        Properties longRunningProps = new Properties(); // Must update config before starting server
        longRunningProps.put(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB,
            QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        longRunningProps.put(QueryServices.DROP_METADATA_ATTRIB, Boolean.TRUE.toString());
        Connection conn1 = DriverManager.getConnection(url, longRunningProps);
        String url2 = url + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + "LongRunningQueries";
        Connection conn2 = DriverManager.getConnection(url2, longRunningProps);
        conn1.setAutoCommit(true);
        conn2.setAutoCommit(true);
        try {
            String schemaName = generateUniqueName();
            String tableName = generateUniqueName();
            String indexName = generateUniqueName();
            final String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
            String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
            conn1.createStatement().execute("CREATE TABLE " + fullTableName + "(k INTEGER PRIMARY KEY, v1 INTEGER, v2 INTEGER) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true");
            conn1.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullTableName + " (v1) INCLUDE (v2)");
            Table metaTable = conn2.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            IndexUtil.updateIndexState(fullIndexName, 0, metaTable, PIndexState.DISABLE);
            conn2.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES(1,2,3)");
            conn2.commit();
            conn1.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES(4,5,6)");
            conn1.commit();
            PTableKey key = new PTableKey(null,fullTableName);
            PMetaData metaCache = conn1.unwrap(PhoenixConnection.class).getMetaDataCache();
            PTable table = metaCache.getTableRef(key).getTable();
            for (PTable index : table.getIndexes()) {
                assertEquals(PIndexState.DISABLE, index.getIndexState());
            }
        } finally {
            conn1.close();
            conn2.close();
        }
    }

    @Test
    public void testUpdateCacheFrequencyWithAddColumn() throws Exception {
        // Create connections 1 and 2
        Properties longRunningProps = new Properties(); // Must update config before starting server
        Connection conn1 = DriverManager.getConnection(url, longRunningProps);
        Connection conn2 = DriverManager.getConnection(url, longRunningProps);
        conn1.setAutoCommit(true);
        conn2.setAutoCommit(true);
        String tableName = generateUniqueName();
        String createTableQuery =
                "create table "+tableName+" (k UNSIGNED_DOUBLE not null primary key, "
                + "v1 UNSIGNED_DOUBLE, v2 UNSIGNED_DOUBLE, v3 UNSIGNED_DOUBLE, "
                + "v4 UNSIGNED_DOUBLE) UPDATE_CACHE_FREQUENCY=1000000000";
        try {
            conn1.createStatement().execute(createTableQuery);
            conn1.createStatement()
                    .execute("upsert into "+tableName+" values (1, 2, 3, 4, 5)");
            conn1.createStatement()
                    .execute("upsert into "+tableName+" values (6, 7, 8, 9, 10)");
            conn1.commit();
            ResultSet rs = conn1.createStatement()
                            .executeQuery("select k,v1,v2,v3 from "+tableName);
            assertTrue(rs.next());
            assertTrue(rs.next());
            rs = conn2.createStatement()
                            .executeQuery("select k,v1,v2,v3 from "+tableName);
            assertTrue(rs.next());
            assertTrue(rs.next());
            PreparedStatement alterStatement = conn1.prepareStatement(
                        "ALTER TABLE "+tableName+" ADD v9 UNSIGNED_DOUBLE");
            alterStatement.execute();
            rs =  conn1.createStatement()
                            .executeQuery("select k,v1,v2,v3,v9 from "+tableName);
            assertTrue(rs.next());
            assertTrue(rs.next());
            rs = conn2.createStatement()
                            .executeQuery("select k,v1,v2,v3,V9 from "+tableName);
            assertTrue(rs.next());
            assertTrue(rs.next());
        } finally {
            conn1.close();
            conn2.close();
        }
    }

    @Test
    public void testUpdateCacheFrequencyWithAddAndDropIndex() throws Exception {
        // Create connections 1 and 2
        Properties longRunningProps = new Properties();
        longRunningProps.put(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB,
            QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        Connection conn1 = DriverManager.getConnection(url, longRunningProps);
        String url2 = url + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + "LongRunningQueries";
        Connection conn2 = DriverManager.getConnection(url2, longRunningProps);
        conn1.setAutoCommit(true);
        conn2.setAutoCommit(true);
        String tableName = generateUniqueName();
        String indexName = "I_"+tableName;
        String tableCreateQuery =
                "create table "+tableName+" (k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)"
                + " UPDATE_CACHE_FREQUENCY=1000000000";
        String value1SelQuery = "SELECT v2 FROM "+tableName+" WHERE v1 = 'value1'";
        String indexCreateQuery = "CREATE INDEX "+indexName+" ON "+tableName+" (v1) INCLUDE (v2)";
        String indexDropQuery = "DROP INDEX "+indexName+" ON "+tableName;
        try {
            conn1.createStatement().execute(tableCreateQuery);
            conn1.createStatement()
                    .execute("upsert into "+tableName+" values ('row1', 'value1', 'key1')");
            conn1.createStatement()
                    .execute("upsert into "+tableName+" values ('row2', 'value2', 'key2')");
            conn1.commit();
            ResultSet rs =conn1.createStatement()
                            .executeQuery("select k,v1,v2 from "+tableName);
            assertTrue(rs.next());
            assertTrue(rs.next());
            rs = conn2.createStatement().executeQuery("select k,v1,v2 from "+tableName);
            assertTrue(rs.next());
            assertTrue(rs.next());
            PreparedStatement createIndexStatement =conn1.prepareStatement(indexCreateQuery);
            createIndexStatement.execute();
            rs = conn1.createStatement().executeQuery(value1SelQuery);
            assertTrue(rs.next());
            rs = conn2.createStatement().executeQuery(value1SelQuery);
            assertTrue(rs.next());
            PreparedStatement dropIndexStatement = conn1.prepareStatement(indexDropQuery);
            dropIndexStatement.execute();
            rs = conn2.createStatement().executeQuery(value1SelQuery);
            assertTrue(rs.next());
            rs = conn1.createStatement().executeQuery(value1SelQuery);
            assertTrue(rs.next());
        } finally {
            conn1.close();
            conn2.close();
        }
    }

    @Test
    public void testUpdateCacheFrequencyWithAddAndDropView() throws Exception {
        // Create connections 1 and 2
        Properties longRunningProps = new Properties();
        longRunningProps.put(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB,
            QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        Connection conn1 = DriverManager.getConnection(url, longRunningProps);
        String url2 = url + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + "LongRunningQueries";
        Connection conn2 = DriverManager.getConnection(url2, longRunningProps);
        conn1.setAutoCommit(true);
        conn2.setAutoCommit(true);
        String tableName = generateUniqueName();
        String viewName = "V_"+tableName;
        String createQry = "create table "+tableName+" (k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)"
                + " UPDATE_CACHE_FREQUENCY=1000000000";
        String valueSelQuery = "SELECT * FROM "+tableName+" WHERE v1 = 'value1'";
        String viewCreateQuery =
                "CREATE VIEW "+viewName+" (v43 VARCHAR) AS SELECT * FROM "+tableName+" WHERE v1 = 'value1'";
        try {
            conn1.createStatement().execute(createQry);
            conn1.createStatement()
                    .execute("upsert into "+tableName+" values ('row1', 'value1', 'key1')");
            conn1.createStatement()
                    .execute("upsert into "+tableName+" values ('row2', 'value2', 'key2')");
            conn1.commit();
            ResultSet rs = conn1.createStatement().executeQuery("select k,v1,v2 from "+tableName);
            assertTrue(rs.next());
            assertTrue(rs.next());
            rs = conn2.createStatement().executeQuery("select k,v1,v2 from "+tableName);
            assertTrue(rs.next());
            assertTrue(rs.next());
            conn1.createStatement().execute(viewCreateQuery);
            rs = conn2.createStatement().executeQuery(valueSelQuery);
            assertTrue(rs.next());
            rs = conn1.createStatement().executeQuery(valueSelQuery);
            assertTrue(rs.next());
            conn1.createStatement().execute("DROP VIEW "+viewName);
            rs = conn2.createStatement().executeQuery(valueSelQuery);
            assertTrue(rs.next());
            rs = conn1.createStatement().executeQuery(valueSelQuery);
            assertTrue(rs.next());
        } finally {
            conn1.close();
            conn2.close();
        }
    }

    @Test
    public void testUpdateCacheFrequencyWithCreateTableAndViewOnDiffConns() throws Exception {
        // Create connections 1 and 2
        Properties longRunningProps = new Properties();
        longRunningProps.put(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB,
            QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        Connection conn1 = DriverManager.getConnection(url, longRunningProps);
        String url2 = url + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + "LongRunningQueries";
        Connection conn2 = DriverManager.getConnection(url2, longRunningProps);
        conn1.setAutoCommit(true);
        conn2.setAutoCommit(true);
        String tableName = generateUniqueName();
        String viewName = "V1_"+tableName;
        String valueSelQuery = "SELECT * FROM "+tableName+" WHERE v1 = 'value1'";
        try {
            //Create table on conn1
            String createQry = "create table "+tableName+" (k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)"
              + " UPDATE_CACHE_FREQUENCY=1000000000";
            conn1.createStatement().execute(createQry);
            //Load few rows
            conn1.createStatement()
                    .execute("upsert into "+tableName+" values ('row1', 'value1', 'key1')");
            conn1.createStatement()
                    .execute("upsert into "+tableName+" values ('row2', 'value2', 'key2')");
            conn1.commit();
            ResultSet rs = conn1.createStatement().executeQuery("select k,v1,v2 from "+tableName);
            assertTrue(rs.next());
            assertTrue(rs.next());

            //Create View on conn2
            String viewCreateQuery =
                "CREATE VIEW "+viewName+" (v43 VARCHAR) AS SELECT * FROM "+tableName+" WHERE v1 = 'value1'";
            conn2.createStatement().execute(viewCreateQuery);

            //Read from view on conn2
            rs = conn2.createStatement().executeQuery(valueSelQuery);
            assertTrue(rs.next());
            //Read from view on conn1
            rs = conn1.createStatement().executeQuery(valueSelQuery);
            assertTrue(rs.next());
        } finally {
            conn1.close();
            conn2.close();
        }
    }
}
