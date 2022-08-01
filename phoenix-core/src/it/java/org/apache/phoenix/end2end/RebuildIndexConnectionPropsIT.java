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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.phoenix.coprocessor.MetaDataRegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.internal.util.reflection.Whitebox;

@Category(NeedsOwnMiniClusterTest.class)
public class RebuildIndexConnectionPropsIT extends BaseTest {
    private static HBaseTestingUtility hbaseTestUtil;
    private static String zkQuorum;
    private static String url;
    private static int NUM_RPC_RETRIES = 1;

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        hbaseTestUtil = new HBaseTestingUtility(conf);
        Map<String, String> serverProps = new HashMap<>();
        serverProps.put(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB, QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        // need at least one retry otherwise test fails
        serverProps.put(QueryServices.INDEX_REBUILD_RPC_RETRIES_COUNTER, Long.toString(NUM_RPC_RETRIES));
        setUpConfigForMiniCluster(conf, new ReadOnlyProps(serverProps.entrySet().iterator()));
        hbaseTestUtil.startMiniCluster();
        // establish url and quorum. Need to use PhoenixDriver and not PhoenixTestDriver
        zkQuorum = "localhost:" + hbaseTestUtil.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;
        Properties driverProps = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        DriverManager.registerDriver(PhoenixDriver.INSTANCE);
        try (PhoenixConnection phxConn =
                DriverManager.getConnection(url, driverProps).unwrap(PhoenixConnection.class)) {
        }
    }

    @Test
    public void testRebuildIndexConnectionProperties() throws Exception {
        try (PhoenixConnection rebuildIndexConnection =
                MetaDataRegionObserver.getRebuildIndexConnection(hbaseTestUtil.getMiniHBaseCluster().getConfiguration())) {
            try (PhoenixConnection regularConnection =
                    DriverManager.getConnection(url).unwrap(PhoenixConnection.class)) {
                String rebuildUrl = rebuildIndexConnection.getURL();
                // assert that we are working with non-test urls
                assertFalse(PhoenixEmbeddedDriver.isTestUrl(url));
                assertFalse(PhoenixEmbeddedDriver.isTestUrl(rebuildUrl));
                // assert that the url ends with expected string
                assertTrue(
                    rebuildUrl.contains(MetaDataRegionObserver.REBUILD_INDEX_APPEND_TO_URL_STRING));
                // assert that the url for regular connection vs the rebuild connection is different
                assertFalse(rebuildUrl.equals(regularConnection.getURL()));
                Configuration rebuildQueryServicesConfig =
                        rebuildIndexConnection.getQueryServices().getConfiguration();
                // assert that the properties are part of the query services config
                assertEquals(
                    Long.toString(QueryServicesOptions.DEFAULT_INDEX_REBUILD_QUERY_TIMEOUT),
                    rebuildQueryServicesConfig.get(QueryServices.THREAD_TIMEOUT_MS_ATTRIB));
                assertEquals(
                    Long.toString(
                        QueryServicesOptions.DEFAULT_INDEX_REBUILD_CLIENT_SCANNER_TIMEOUT),
                    rebuildQueryServicesConfig.get(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD));
                assertEquals(Long.toString(QueryServicesOptions.DEFAULT_INDEX_REBUILD_RPC_TIMEOUT),
                    rebuildQueryServicesConfig.get(HConstants.HBASE_RPC_TIMEOUT_KEY));
                assertEquals(
                    Long.toString(NUM_RPC_RETRIES),
                    rebuildQueryServicesConfig.get(HConstants.HBASE_CLIENT_RETRIES_NUMBER));
                ConnectionQueryServices rebuildQueryServices = rebuildIndexConnection.getQueryServices();
                Connection rebuildIndexHConnection =
                        (Connection) Whitebox.getInternalState(rebuildQueryServices,
                            "connection");
                Connection regularHConnection =
                        (Connection) Whitebox.getInternalState(
                            regularConnection.getQueryServices(), "connection");
                // assert that a new HConnection was created
                assertFalse(
                    regularHConnection.toString().equals(rebuildIndexHConnection.toString()));
                Configuration rebuildHConnectionConfig = rebuildIndexHConnection.getConfiguration();
                // assert that the HConnection has the desired properties needed for rebuilding
                // indices
                assertEquals(
                    Long.toString(
                        QueryServicesOptions.DEFAULT_INDEX_REBUILD_CLIENT_SCANNER_TIMEOUT),
                    rebuildHConnectionConfig.get(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD));
                assertEquals(Long.toString(QueryServicesOptions.DEFAULT_INDEX_REBUILD_RPC_TIMEOUT),
                    rebuildHConnectionConfig.get(HConstants.HBASE_RPC_TIMEOUT_KEY));
                assertEquals(
                    Long.toString(NUM_RPC_RETRIES),
                    rebuildHConnectionConfig.get(HConstants.HBASE_CLIENT_RETRIES_NUMBER));
            }
        }
    }
}
