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

import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.log.ConnectionLimiter;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.phoenix.jdbc.HighAvailabilityGroup.PHOENIX_HA_GROUP_ATTR;
import static org.apache.phoenix.jdbc.HighAvailabilityPolicy.PARALLEL;

@Category(NeedsOwnMiniClusterTest.class)
public class LoggingHAConnectionLimiterIT extends LoggingConnectionLimiterIT {
    private static final Logger LOG = LoggerFactory.getLogger(LoggingHAConnectionLimiterIT.class);
    private static final HighAvailabilityTestingUtility.HBaseTestingUtilityPair CLUSTERS = new HighAvailabilityTestingUtility.HBaseTestingUtilityPair();
    private static Map<String, String> GLOBAL_PROPERTIES ;

    private static List<Connection> CONNECTIONS = null;

    /**
     * Client properties to create a connection per test.
     */
    private Properties clientProperties;
    /**
     * JDBC connection string for this test HA group.
     */
    private String jdbcUrl;
    /**
     * HA group for this test.
     */
    private HighAvailabilityGroup haGroup;

    @BeforeClass
    public static final void doSetup() throws Exception {
        /**
         *  Turn on the connection logging feature
         *  CLIENT_CONNECTION_MAX_ALLOWED_CONNECTIONS : max allowed connections before throttling
         *  INTERNAL_CONNECTION_MAX_ALLOWED_CONNECTIONS : max allowed internal connections before throttling
         *  HA_MAX_POOL_SIZE : HA thread pool size for open and other activities
         *  HA_MAX_QUEUE_SIZE : Queue size of the core thread pool
         *
         */
        GLOBAL_PROPERTIES = new HashMap<String, String>() {{
            put(QueryServices.CONNECTION_ACTIVITY_LOGGING_ENABLED, String.valueOf(true));
            put(QueryServices.CLIENT_CONNECTION_MAX_ALLOWED_CONNECTIONS, String.valueOf(20));
            put(QueryServices.INTERNAL_CONNECTION_MAX_ALLOWED_CONNECTIONS, String.valueOf(20));
            put(PhoenixHAExecutorServiceProvider.HA_MAX_POOL_SIZE, String.valueOf(5));
            put(PhoenixHAExecutorServiceProvider.HA_MAX_QUEUE_SIZE, String.valueOf(30));

        }};

        CLUSTERS.start();
        DriverManager.registerDriver(PhoenixDriver.INSTANCE);
        GLOBAL_PROPERTIES.put(PHOENIX_HA_GROUP_ATTR, PARALLEL.name());


        CONNECTIONS = Lists.newArrayList(CLUSTERS.getCluster1Connection(), CLUSTERS.getCluster2Connection());
        LOG.info(String.format("************* Num connections : %d", CONNECTIONS.size()));

        for (Connection conn : CONNECTIONS) {
            try (Statement statement = conn.createStatement()) {
                statement.execute(CREATE_TABLE_SQL);
            }
            conn.commit();
        }

        //preload some data
        try (Connection connection = CLUSTERS.getCluster1Connection()) {
            loadData(connection, ORG_ID, GROUP_ID, 100, 20);
        }
    }


    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        for (Connection conn : CONNECTIONS) {
            conn.close();
        }

        DriverManager.deregisterDriver(PhoenixDriver.INSTANCE);
        CLUSTERS.close();
    }

    @Before
    public void setup() throws Exception {
        clientProperties =  new Properties();
        clientProperties.putAll(GLOBAL_PROPERTIES);

        String haGroupName = testName.getMethodName();
        clientProperties.setProperty(PHOENIX_HA_GROUP_ATTR, haGroupName);

        // Make first cluster ACTIVE
        CLUSTERS.initClusterRole(haGroupName, PARALLEL);

        jdbcUrl = String.format("jdbc:phoenix:[%s|%s]",
                CLUSTERS.getUrl1(), CLUSTERS.getUrl2());
        haGroup = HighAvailabilityTestingUtility.getHighAvailibilityGroup(jdbcUrl, clientProperties);
        LOG.info("Initialized haGroup {} with URL {}", haGroup.getGroupInfo().getName(), jdbcUrl);

    }

    @Override
    protected ConnectionLimiter getConnectionLimiter() throws Exception {
        ConnectionQueryServices cqs = null;
        Connection testConnection = null;
        try {
            testConnection = getConnection();
            ParallelPhoenixConnection phoenixConnection = testConnection.unwrap(ParallelPhoenixConnection.class);
            cqs = phoenixConnection.getFutureConnection1().get().getQueryServices();
            return cqs.getConnectionLimiter();
        } finally {
            if (testConnection != null) testConnection.close();
        }
    }

    @Override
    protected Connection getConnection() throws SQLException {
        Connection connection = DriverManager.getConnection(jdbcUrl, clientProperties);
        connection.setAutoCommit(true);
        return connection;
    }

}
