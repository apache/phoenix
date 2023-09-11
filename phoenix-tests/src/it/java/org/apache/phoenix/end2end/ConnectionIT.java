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

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.query.ConfigurationFactory;
import org.apache.phoenix.util.InstanceResolver;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(NeedsOwnMiniClusterTest.class)
public class ConnectionIT {

    private static HBaseTestingUtility hbaseTestUtil;
    private static Configuration conf;

    private static int tableCounter;

    @BeforeClass
    public static synchronized void setUp() throws Exception {
        hbaseTestUtil = new HBaseTestingUtility();
        conf = hbaseTestUtil.getConfiguration();
        setUpConfigForMiniCluster(conf);
        conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase-test");
        hbaseTestUtil.startMiniCluster();
        Class.forName(PhoenixDriver.class.getName());
        InstanceResolver.clearSingletons();
        // Make sure the ConnectionInfo doesn't try to pull a default Configuration
        InstanceResolver.getSingleton(ConfigurationFactory.class, new ConfigurationFactory() {
            @Override
            public Configuration getConfiguration() {
                return new Configuration(conf);
            }

            @Override
            public Configuration getConfiguration(Configuration confToClone) {
                Configuration copy = new Configuration(conf);
                copy.addResource(confToClone);
                return copy;
            }
        });
    }

    @AfterClass
    public static synchronized void cleanUp() throws Exception {
        InstanceResolver.clearSingletons();
    }

    @Test
    public void testInputAndOutputConnections() throws SQLException {
        try (Connection inputConnection = ConnectionUtil.getInputConnection(conf)) {
            smoke(inputConnection);
        }
        try (Connection outputConnection = ConnectionUtil.getOutputConnection(conf)) {
            smoke(outputConnection);
        }
    }

    private void smoke(Connection conn) throws SQLException {
        String table = "t" + tableCounter++;
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("create table " + table + " (a integer primary key,b varchar)");
            stmt.execute("upsert into " + table + " values(1,'foo')");
            conn.commit();
            ResultSet rs = stmt.executeQuery("select count(*) from " + table);
            rs.next();
            assertEquals(1, rs.getInt(1));
        }
    }

    @Test
    public void testZkConnections() throws SQLException {
        String zkQuorum = conf.get(HConstants.ZOOKEEPER_QUORUM);
        String zkPort = conf.get(HConstants.ZOOKEEPER_CLIENT_PORT);
        try (PhoenixConnection conn1 =
                (PhoenixConnection) DriverManager.getConnection("jdbc:phoenix");
                PhoenixConnection conn2 =
                        (PhoenixConnection) DriverManager.getConnection("jdbc:phoenix+zk");
                PhoenixConnection conn3 =
                        (PhoenixConnection) DriverManager
                                .getConnection("jdbc:phoenix+zk:" + zkQuorum + ":" + zkPort);) {
            smoke(conn1);
            smoke(conn2);
            smoke(conn3);
            assertEquals(conn1.getQueryServices(), conn2.getQueryServices());
            assertEquals(conn1.getQueryServices(), conn3.getQueryServices());
        }
    }

    @Test
    public void testMasterConnections() throws SQLException {
        assumeTrue(VersionInfo.compareVersion(VersionInfo.getVersion(), "2.3.0") >= 0);
        int masterPortString = conf.getInt(HConstants.MASTER_PORT, HConstants.DEFAULT_MASTER_PORT);
        String masterHosts = conf.get(HConstants.MASTER_ADDRS_KEY);
        try (PhoenixConnection conn1 =
                (PhoenixConnection) DriverManager.getConnection("jdbc:phoenix+master");
                PhoenixConnection conn2 =
                        (PhoenixConnection) DriverManager.getConnection("jdbc:phoenix+master:"
                                + masterHosts.replaceAll(":", "\\\\:") + ":" + masterPortString);) {
            smoke(conn1);
            smoke(conn2);
            assertEquals(conn1.getQueryServices(), conn2.getQueryServices());
        }
    }

    @Test
    public void testRPCConnections() throws SQLException {
        assumeTrue(VersionInfo.compareVersion(VersionInfo.getVersion(), "2.5.0") >= 0);
        String masterHosts = conf.get(HConstants.MASTER_ADDRS_KEY);

        try (PhoenixConnection conn1 =
                (PhoenixConnection) DriverManager.getConnection("jdbc:phoenix+rpc");
                PhoenixConnection conn2 =
                        (PhoenixConnection) DriverManager.getConnection(
                            "jdbc:phoenix+rpc:" + masterHosts.replaceAll(":", "\\\\:"));) {
            smoke(conn1);
            smoke(conn2);
            assertEquals(conn1.getQueryServices(), conn2.getQueryServices());
        }

    }
}
