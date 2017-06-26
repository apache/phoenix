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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;

public class UpdateCacheAcrossDifferentClientsIT extends BaseUniqueNamesOwnClusterIT {

    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String, String> props = Maps.newConcurrentMap();
        props.put(QueryServices.DROP_METADATA_ATTRIB, Boolean.TRUE.toString());
        props.put(QueryServices.MUTATE_BATCH_SIZE_ATTRIB, Integer.toString(3000));
        //When we run all tests together we are using global cluster(driver)
        //so to make drop work we need to re register driver with DROP_METADATA_ATTRIB property
        destroyDriver();
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
        //Registering real Phoenix driver to have multiple ConnectionQueryServices created across connections
        //so that metadata changes doesn't get propagated across connections
        DriverManager.registerDriver(PhoenixDriver.INSTANCE);
    }

    @Test
    public void testUpdateCacheFrequencyWithAddAndDropTable() throws Exception {
        // Create connections 1 and 2
        Properties longRunningProps = new Properties(); // Must update config before starting server
        longRunningProps.put(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB,
            QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        longRunningProps.put(QueryServices.DROP_METADATA_ATTRIB, Boolean.TRUE.toString());
        Connection conn1 = DriverManager.getConnection(getUrl(), longRunningProps);
        String url2 = getUrl() + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + "LongRunningQueries";
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
            rs = conn2.createStatement().executeQuery("select * from "+tableName);
            assertTrue(rs.next());
            assertTrue(rs.next());
            //Drop table from conn1
            conn1.createStatement().execute(dropTableQuery);
            try {
                rs = conn1.createStatement().executeQuery("select * from "+tableName);
                fail("Should throw TableNotFoundException after dropping table");
            } catch (TableNotFoundException e) {
                //Expected
            }
            try {
                rs = conn2.createStatement().executeQuery("select * from "+tableName);
                fail("Should throw TableNotFoundException after dropping table");
            } catch (TableNotFoundException e) {
                //Expected
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
        Connection conn1 = DriverManager.getConnection(getUrl(), longRunningProps);
        Connection conn2 = DriverManager.getConnection(getUrl(), longRunningProps);
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
        Connection conn1 = DriverManager.getConnection(getUrl(), longRunningProps);
        String url2 = getUrl() + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + "LongRunningQueries";
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
        Connection conn1 = DriverManager.getConnection(getUrl(), longRunningProps);
        String url2 = getUrl() + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + "LongRunningQueries";
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
        Connection conn1 = DriverManager.getConnection(getUrl(), longRunningProps);
        String url2 = getUrl() + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + "LongRunningQueries";
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

    @Test
    public void testUpsertSelectOnSameTableWithFutureData() throws Exception {
        String tableName = generateUniqueName();
        Properties longRunningProps = new Properties();
        longRunningProps.put(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB,
            QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        longRunningProps.put(QueryServices.MUTATE_BATCH_SIZE_ATTRIB, Integer.toString(3000));
        Connection conn = DriverManager.getConnection(getUrl(), longRunningProps);
        conn.setAutoCommit(false);
        longRunningProps = new Properties();
        longRunningProps.put(QueryServices.MUTATE_BATCH_SIZE_ATTRIB, Integer.toString(3000));
        conn.createStatement().execute("CREATE TABLE " + tableName + "("
                + "a VARCHAR PRIMARY KEY, b VARCHAR) UPDATE_CACHE_FREQUENCY=1000000000");
        String payload;
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < 1; i++) {
            buf.append('a');
        }
        payload = buf.toString();
        int MIN_CHAR = 'a';
        int MAX_CHAR = 'c';
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES(?,?)");
        int rowCount = 0;
        for (int c1 = MIN_CHAR; c1 <= MAX_CHAR; c1++) {
            for (int c2 = MIN_CHAR; c2 <= MAX_CHAR; c2++) {
                String pk = Character.toString((char)c1) + Character.toString((char)c2);
                stmt.setString(1, pk);
                stmt.setString(2, payload);
                stmt.execute();
                rowCount++;
            }
        }
        conn.commit();
        int count = conn.createStatement().executeUpdate("UPSERT INTO  " + tableName + "  SELECT a, b FROM  " + tableName);
        assertEquals(rowCount, count);

        //Upsert some data with future timestamp
        longRunningProps.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(System.currentTimeMillis()+(24*60*60*1000)));
        Connection conn2 = DriverManager.getConnection(getUrl(), longRunningProps);
        conn2.setAutoCommit(false);
        stmt = conn2.prepareStatement("UPSERT INTO " + tableName + " VALUES(?,?)");
        MAX_CHAR = 'f';
        int rowCount2=0;
        for (int c1 = MIN_CHAR; c1 <= MAX_CHAR; c1++) {
            for (int c2 = MIN_CHAR; c2 <= MAX_CHAR; c2++) {
                String pk = "2--"+Character.toString((char)c1) + Character.toString((char)c2);
                stmt.setString(1, pk);
                stmt.setString(2, payload);
                stmt.execute();
                rowCount2++;
            }
        }
        conn2.commit();

        //Open new connection with future timestamp to see all data
        longRunningProps.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(System.currentTimeMillis()+(25*60*60*1000)));
        conn2 = DriverManager.getConnection(getUrl(), longRunningProps);
        conn2.setAutoCommit(false);
        count = conn2.createStatement().executeUpdate("UPSERT INTO  " + tableName + "  SELECT * FROM  " + tableName);
        assertEquals(rowCount+rowCount2, count);
        conn.close();
        longRunningProps.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(System.currentTimeMillis()));
        conn = DriverManager.getConnection(getUrl(), longRunningProps);
        //This connection should see data only upto current time
        count = conn.createStatement().executeUpdate("UPSERT INTO  " + tableName + "  SELECT * FROM  " + tableName);
        assertEquals(rowCount, count);
        conn2.close();
    }
}
