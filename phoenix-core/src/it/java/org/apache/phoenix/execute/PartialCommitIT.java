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

import static org.apache.phoenix.query.BaseTest.initAndRegisterDriver;
import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;
import static org.apache.phoenix.util.PhoenixRuntime.PHOENIX_TEST_DRIVER_URL_PARAM;
import static org.apache.phoenix.util.TestUtil.LOCALHOST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.hbase.index.Indexer;
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
    
    protected static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
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
      conf.setClass("hbase.coprocessor.region.classes", BuggyRegionObserver.class, RegionObserver.class);
      conf.setBoolean("hbase.coprocessor.abortonerror", false);
      // disable version checking, so we can test against whatever version of HBase happens to be
      // installed (right now, its generally going to be SNAPSHOT versions).
      conf.setBoolean(Indexer.CHECK_VERSION_CONF_KEY, false);
      TEST_UTIL.startMiniCluster();
      String clientPort = TEST_UTIL.getConfiguration().get(QueryServices.ZOOKEEPER_PORT_ATTRIB);
      url = JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + LOCALHOST + JDBC_PROTOCOL_SEPARATOR + clientPort
              + JDBC_PROTOCOL_TERMINATOR + PHOENIX_TEST_DRIVER_URL_PARAM;

      Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
      // Must update config before starting server
      props.put(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
      driver = initAndRegisterDriver(url, new ReadOnlyProps(props.entrySet().iterator()));
      createTables();
    }
    
    private static void createTables() throws Exception {
        Properties props = new Properties();
        props.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, 10);
        
        try (Connection con = driver.connect(url, new Properties())) {
            con.setAutoCommit(false);
            Statement sta = con.createStatement();
            sta.execute("create table success_table (k varchar primary key)");
            sta.execute("create table failure_table (k varchar primary key)");
        }
    }
    
    @AfterClass
    public static void teardownCluster() throws Exception {
      TEST_UTIL.shutdownMiniCluster();
    }
    
    @Test
    public void blah() throws Exception {
        try (Connection con = driver.connect(url, new Properties())) {
            con.setAutoCommit(false);
            Statement sta = con.createStatement();
            sta.execute("upsert into success_table (k) values ('a')");
            sta.execute("upsert into failure_table (k) values ('boom!')");
            try {
                con.commit();
                fail("Expected upsert into failure_table to have failed the commit");
            } catch (SQLException sqle) {
                assertEquals(CommitException.class, sqle.getClass());
                Set<Integer> failures = ((CommitException)sqle).getFailures();
                assertEquals(1, failures.size());
                //assertEquals(new Integer(1), failures.iterator().next());
            }
            
            // TODO: assert actual partial save by selecting from tables
        }
    }
    
    public static class BuggyRegionObserver extends SimpleRegionObserver {
        @Override
        public void prePut(final ObserverContext<RegionCoprocessorEnvironment> c, final Put put, final WALEdit edit,
                final Durability durability) {
            String tableName = c.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();
            if (tableName.equalsIgnoreCase("failure_table")) {
                throw new RuntimeException(new IOException("boom!"));
            }
        }
    }
}
