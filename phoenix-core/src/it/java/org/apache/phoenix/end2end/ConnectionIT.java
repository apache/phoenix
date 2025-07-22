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

import static org.apache.phoenix.query.BaseTest.getConfiguration;
import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;
import static org.apache.phoenix.query.BaseTest.setUpTestClusterForHA;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.phoenix.jdbc.*;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.query.ConfigurationFactory;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.InstanceResolver;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
//Failing with HA Connection
@Category(NeedsOwnMiniClusterTest.class)
public class ConnectionIT {

    private static HBaseTestingUtility hbaseTestUtil;
    private static Configuration conf;

    private static int tableCounter;

    @BeforeClass
    public static synchronized void setUp() throws Exception {
        if(Boolean.parseBoolean(System.getProperty("phoenix.ha.profile.active"))){
            Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
            props.put(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase-test");
            props.put(HConstants.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY, ZKConnectionInfo.ZK_REGISTRY_NAME);
            setUpTestClusterForHA(new ReadOnlyProps(props.entrySet().iterator()),new ReadOnlyProps(props.entrySet().iterator()));
            conf = getConfiguration();
        } else {
            hbaseTestUtil = new HBaseTestingUtility();
            conf = hbaseTestUtil.getConfiguration();
            setUpConfigForMiniCluster(conf);
            conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase-test");
            conf.set(HConstants.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY, ZKConnectionInfo.ZK_REGISTRY_NAME);
            hbaseTestUtil.startMiniCluster();
            Class.forName(PhoenixDriver.class.getName());
            DriverManager.registerDriver(new PhoenixTestDriver());
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
    }

    @AfterClass
    public static synchronized void cleanUp() throws Exception {
        InstanceResolver.clearSingletons();
    }

    @Test
    public void testInputAndOutputConnections() throws SQLException {
        try (Connection inputConnection = ConnectionUtil.getInputConnection(conf, PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            smoke(inputConnection);
        }
        try (Connection outputConnection = ConnectionUtil.getOutputConnection(conf, PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
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
        try (PhoenixMonitoredConnection conn1 =
                (PhoenixMonitoredConnection) DriverManager.getConnection("jdbc:phoenix", PropertiesUtil.deepCopy(TEST_PROPERTIES));
             PhoenixMonitoredConnection conn2 =
                        (PhoenixMonitoredConnection) DriverManager.getConnection("jdbc:phoenix+zk", PropertiesUtil.deepCopy(TEST_PROPERTIES));
             PhoenixMonitoredConnection conn3 =
                        (PhoenixMonitoredConnection) DriverManager
                                .getConnection("jdbc:phoenix+zk:" + zkQuorum + ":" + zkPort, PropertiesUtil.deepCopy(TEST_PROPERTIES));) {
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
        try (PhoenixMonitoredConnection conn1 =
                (PhoenixMonitoredConnection) DriverManager.getConnection("jdbc:phoenix+master", PropertiesUtil.deepCopy(TEST_PROPERTIES));
                PhoenixMonitoredConnection conn2 =
                        (PhoenixMonitoredConnection) DriverManager.getConnection("jdbc:phoenix+master:"
                                + masterHosts.replaceAll(":", "\\\\:") + ":" + masterPortString, PropertiesUtil.deepCopy(TEST_PROPERTIES));) {
            smoke(conn1);
            smoke(conn2);
            assertEquals(conn1.getQueryServices(), conn2.getQueryServices());
        }
    }

    @Test
    public void testRPCConnections() throws SQLException {
        assumeTrue(VersionInfo.compareVersion(VersionInfo.getVersion(), "2.5.0") >= 0);
        String masterHosts = conf.get(HConstants.MASTER_ADDRS_KEY);
        // HBase does fall back to MasterRpcRegistry if the bootstrap servers are not set, but
        // ConnectionInfo normalization does not handle that.

        // Set BOOTSTRAP_NODES so that we can test the default case
        conf.set(RPCConnectionInfo.BOOTSTRAP_NODES, masterHosts);

        try (PhoenixMonitoredConnection conn1 =
                (PhoenixMonitoredConnection) DriverManager.getConnection("jdbc:phoenix+rpc", PropertiesUtil.deepCopy(TEST_PROPERTIES));
                PhoenixMonitoredConnection conn2 =
                        (PhoenixConnection) DriverManager.getConnection(
                            "jdbc:phoenix+rpc:" + masterHosts.replaceAll(":", "\\\\:"), PropertiesUtil.deepCopy(TEST_PROPERTIES));) {
            smoke(conn1);
            smoke(conn2);
            assertEquals(conn1.getQueryServices(), conn2.getQueryServices());
        }

    }
}
