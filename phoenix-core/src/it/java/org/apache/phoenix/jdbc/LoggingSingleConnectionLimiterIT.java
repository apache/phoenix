/**
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
package org.apache.phoenix.jdbc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.log.ConnectionLimiter;
import org.apache.phoenix.query.ConfigurationFactory;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.HBaseFactoryProvider;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.InstanceResolver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

@Category(NeedsOwnMiniClusterTest.class)
public class LoggingSingleConnectionLimiterIT extends LoggingConnectionLimiterIT {
    private static final Logger LOG = LoggerFactory.getLogger(LoggingSingleConnectionLimiterIT.class);

    @BeforeClass
    public static void doSetup() throws Exception {
        /**
         *  Turn on the connection logging feature
         *  CLIENT_CONNECTION_MAX_ALLOWED_CONNECTIONS : max allowed connections before throttling
         *  INTERNAL_CONNECTION_MAX_ALLOWED_CONNECTIONS : max allowed internal connections before throttling
         *  HA_MAX_POOL_SIZE : HA thread pool size for open and other activities
         *  HA_MAX_QUEUE_SIZE : Queue size of the core thread pool
         */

        InstanceResolver.clearSingletons();
        // Override to get required config for static fields loaded that require HBase config
        InstanceResolver.getSingleton(ConfigurationFactory.class, new ConfigurationFactory() {

            @Override public Configuration getConfiguration() {
                Configuration conf = HBaseConfiguration.create();
                conf.set(QueryServices.CONNECTION_ACTIVITY_LOGGING_ENABLED, String.valueOf(true));
                conf.set(QueryServices.CLIENT_CONNECTION_MAX_ALLOWED_CONNECTIONS, String.valueOf(20));
                conf.set(QueryServices.INTERNAL_CONNECTION_MAX_ALLOWED_CONNECTIONS, String.valueOf(20));
                conf.set(PhoenixHAExecutorServiceProvider.HA_MAX_POOL_SIZE, String.valueOf(5));
                conf.set(PhoenixHAExecutorServiceProvider.HA_MAX_QUEUE_SIZE, String.valueOf(30));
                return conf;
            }

            @Override public Configuration getConfiguration(Configuration confToClone) {
                Configuration conf = HBaseConfiguration.create();
                conf.set(QueryServices.CONNECTION_ACTIVITY_LOGGING_ENABLED, String.valueOf(true));
                conf.set(QueryServices.CLIENT_CONNECTION_MAX_ALLOWED_CONNECTIONS, String.valueOf(20));
                conf.set(QueryServices.INTERNAL_CONNECTION_MAX_ALLOWED_CONNECTIONS, String.valueOf(20));
                conf.set(PhoenixHAExecutorServiceProvider.HA_MAX_POOL_SIZE, String.valueOf(5));
                conf.set(PhoenixHAExecutorServiceProvider.HA_MAX_QUEUE_SIZE, String.valueOf(30));
                Configuration copy = new Configuration(conf);
                copy.addResource(confToClone);
                return copy;
            }
        });

        Configuration conf = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
        HBaseTestingUtility hBaseTestingUtility = new HBaseTestingUtility(conf);
        setUpConfigForMiniCluster(conf);
        hBaseTestingUtility.startMiniCluster();
        // establish url and quorum. Need to use PhoenixDriver and not PhoenixTestDriver
        String zkQuorum = "localhost:" + hBaseTestingUtility.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;
        DriverManager.registerDriver(PhoenixDriver.INSTANCE);

        String profileName = "setup";
        final String urlWithPrinc = url + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + profileName
                + PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;
        Properties props = new Properties();

        try (Connection connection = DriverManager.getConnection(urlWithPrinc, props)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(CREATE_TABLE_SQL);
            }
            connection.commit();
        }

        //preload some data
        try (Connection connection = DriverManager.getConnection(urlWithPrinc, props)) {
            loadData(connection, ORG_ID, GROUP_ID, 100, 20);
        }

    }
    @Override
    protected ConnectionLimiter getConnectionLimiter() throws Exception {
        ConnectionQueryServices cqs = null;
        Connection testConnection = null;
        try {
            testConnection = getConnection();
            PhoenixConnection phoenixConnection = testConnection.unwrap(PhoenixConnection.class);
            cqs = phoenixConnection.getQueryServices();
            return cqs.getConnectionLimiter();
        } finally {
            if (testConnection != null) testConnection.close();
        }
    }

    @Override
    protected Connection getConnection() throws SQLException {
        String profileName = testName.getMethodName();
        final String urlWithPrinc = url + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + profileName
                + PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;
        Properties props = new Properties();
        Connection connection = DriverManager.getConnection(urlWithPrinc, props);
        connection.setAutoCommit(true);
        return connection;
    }

}
