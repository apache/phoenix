/*
 * Copyright 2014 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 *distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.execute;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.phoenix.query.BaseTest.initAndRegisterDriver;
import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;
import static org.apache.phoenix.util.PhoenixRuntime.PHOENIX_TEST_DRIVER_URL_PARAM;
import static org.apache.phoenix.util.TestUtil.LOCALHOST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.hbase.index.Indexer;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Maps;

@Category(NeedsOwnMiniClusterTest.class)
public class PartialCommitIT {
    
    private static final String TABLE_NAME_TO_FAIL = "b_failure_table".toUpperCase();
    private static final byte[] ROW_TO_FAIL = Bytes.toBytes("fail me");
    private static final String UPSERT_TO_FAIL = "upsert into " + TABLE_NAME_TO_FAIL + " values ('" + Bytes.toString(ROW_TO_FAIL) + "', 'boom!')";
    private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    private static String url;
    private static Driver driver;
    private static final Properties props = new Properties();
    
    static {
        props.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, 10);
    }
    
    @BeforeClass
    public static void setupCluster() throws Exception {
      Configuration conf = TEST_UTIL.getConfiguration();
      setUpConfigForMiniCluster(conf);
      conf.setClass("hbase.coprocessor.region.classes", FailingRegionObserver.class, RegionObserver.class);
      conf.setBoolean("hbase.coprocessor.abortonerror", false);
      conf.setBoolean(Indexer.CHECK_VERSION_CONF_KEY, false);
      TEST_UTIL.startMiniCluster();
      String clientPort = TEST_UTIL.getConfiguration().get(QueryServices.ZOOKEEPER_PORT_ATTRIB);
      url = JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + LOCALHOST + JDBC_PROTOCOL_SEPARATOR + clientPort
              + JDBC_PROTOCOL_TERMINATOR + PHOENIX_TEST_DRIVER_URL_PARAM;

      Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
      // Must update config before starting server
      props.put(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
      driver = initAndRegisterDriver(url, new ReadOnlyProps(props.entrySet().iterator()));
      createTablesWithABitOfData();
    }
    
    private static void createTablesWithABitOfData() throws Exception {
        Properties props = new Properties();
        props.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, 10);

        try (Connection con = driver.connect(url, new Properties())) {
            Statement sta = con.createStatement();
            sta.execute("create table a_success_table (k varchar primary key, c varchar)");
            sta.execute("create table b_failure_table (k varchar primary key, c varchar)");
            sta.execute("create table c_success_table (k varchar primary key, c varchar)");
            con.commit();
        }

        props.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, 100);

        try (Connection con = driver.connect(url, new Properties())) {
            con.setAutoCommit(false);
            Statement sta = con.createStatement();
            for (String table : newHashSet("a_success_table", "b_failure_table", "c_success_table")) {
                sta.execute("upsert into " + table + " values ('z', 'z')");
                sta.execute("upsert into " + table + " values ('zz', 'zz')");
                sta.execute("upsert into " + table + " values ('zzz', 'zzz')");
            }
            con.commit();
        }
    }
    
    @AfterClass
    public static void teardownCluster() throws Exception {
      TEST_UTIL.shutdownMiniCluster();
    }
    
    @Test
    public void testNoFailure() {
        testPartialCommit(singletonList("upsert into a_success_table values ('a', 'a')"), 0, Collections.<Integer>emptySet(), false);
    }
    
    @Test
    public void testUpsertFailure() {
        testPartialCommit(newArrayList("upsert into a_success_table values ('a', 'a')", 
                                       UPSERT_TO_FAIL, 
                                       "upsert into a_success_table values ('a', 'a')"), 
                                       1, singleton(new Integer(1)), true);
    }
    
    @Test
    public void testDeleteFailure() {
        testPartialCommit(newArrayList("upsert into a_success_table values ('a', 'a')", 
                                       "delete from b_failure_table where k='z'", 
                                       "upsert into a_success_table values ('b', 'b')"), 
                                       1, singleton(new Integer(1)), true);
    }
    
    /**
     * {@link MutationState} keeps mutations ordered by table name.
     */
    @Test
    public void testOrderOfMutationsIsPredicatable() {
        testPartialCommit(newArrayList("upsert into c_success_table values ('c', 'c')", // will fail because c_success_table is after b_failure_table by table sort order
                                       UPSERT_TO_FAIL, 
                                       "upsert into a_success_table values ('a', 'a')"), // will succeed because a_success_table is before b_failure_table by table sort order
                                       2, newHashSet(new Integer(1), new Integer(0)), true);
    }
    
    @Test
    public void checkThatAllStatementTypesMaintainOrderInConnection() {
        testPartialCommit(newArrayList("upsert into a_success_table values ('a', 'a')", 
                                       "upsert into a_success_table select k from c_success_table",
                                       "delete from a_success_table",
                                       "select * from a_success_table", 
                                       "create table z (c varchar primary key)", 
                                       "create view v as select * from z",
                                       "update statistics a_success_table",
                                       "explain select * from a_success_table",
                                       UPSERT_TO_FAIL), 
                                       1, singleton(new Integer(1)), true);
    }
    
    private void testPartialCommit(List<String> statements, int failureCount, Set<Integer> orderOfFailedStatements, boolean willFail) {
        try (Connection con = driver.connect(url, new Properties())) {
            con.setAutoCommit(false);
            Statement sta = con.createStatement();
            for (String statement : statements) {
                sta.execute(statement);
            }
            try {
                con.commit();
                if (willFail) {
                    fail("Expected at least one statement in the list to fail");
                } else {
                    assertEquals(0, con.unwrap(PhoenixConnection.class).getStatementExecutionsCount()); // should have been reset to 0 in commit()
                }
            } catch (SQLException sqle) {
                if (!willFail) {
                    fail("Expected no statements to fail");
                }
                assertEquals(CommitException.class, sqle.getClass());
                Set<Integer> failures = ((CommitException)sqle).getFailures();
                assertEquals(failureCount, failures.size());
                assertEquals(orderOfFailedStatements, failures);
            }
            
            // TODO: assert actual partial save by selecting from tables
        } catch (SQLException e) {
            fail(e.toString());
        }
    }
    
    public static class FailingRegionObserver extends SimpleRegionObserver {
        @Override
        public void prePut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit,
                final Durability durability) throws HBaseIOException {
            if (shouldFailUpsert(c, put) || shouldFailDelete(c, put)) {
                // throwing anything other than instances of IOException result
                // in this coprocessor being unloaded
                // DoNotRetryIOException tells HBase not to retry this mutation
                // multiple times
                throw new DoNotRetryIOException();
            }
        }
        
        private static boolean shouldFailUpsert(ObserverContext<RegionCoprocessorEnvironment> c, Put put) {
            String tableName = c.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();
            return TABLE_NAME_TO_FAIL.equals(tableName) && Bytes.equals(ROW_TO_FAIL, put.getRow());
        }
        
        private static boolean shouldFailDelete(ObserverContext<RegionCoprocessorEnvironment> c, Put put) {
            String tableName = c.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();
            return TABLE_NAME_TO_FAIL.equals(tableName) &&  
                   // Phoenix deletes are sent as Puts with empty values
                   put.getFamilyCellMap().firstEntry().getValue().get(0).getValueLength() == 0; 
        }
    }
}
