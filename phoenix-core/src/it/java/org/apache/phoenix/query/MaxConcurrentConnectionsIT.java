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

package org.apache.phoenix.query;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.util.DelayedRegionServer;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.apache.phoenix.exception.SQLExceptionCode.NEW_INTERNAL_CONNECTION_THROTTLED;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_OPEN_INTERNAL_PHOENIX_CONNECTIONS;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_OPEN_PHOENIX_CONNECTIONS;
import static org.apache.phoenix.query.QueryServices.CLIENT_CONNECTION_MAX_ALLOWED_CONNECTIONS;
import static org.apache.phoenix.query.QueryServices.INTERNAL_CONNECTION_MAX_ALLOWED_CONNECTIONS;
import static org.apache.phoenix.query.QueryServices.TASK_HANDLING_INITIAL_DELAY_MS_ATTRIB;
import static org.apache.phoenix.query.QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Note that some tests for concurrentConnections live in PhoenixMetricsIT.java which also test the metric emission
 */
@Category(NeedsOwnMiniClusterTest.class)
public class MaxConcurrentConnectionsIT extends BaseTest {

    private static HBaseTestingUtility hbaseTestUtil;

    @BeforeClass
    public static void setUp() throws Exception {
        hbaseTestUtil = new HBaseTestingUtility();

        hbaseTestUtil.startMiniCluster(1,1,null,null,DelayedRegionServer.class);
        // establish url and quorum. Need to use PhoenixDriver and not PhoenixTestDriver
        String zkQuorum = "localhost:" + hbaseTestUtil.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + zkQuorum +
                JDBC_PROTOCOL_SEPARATOR + "uniqueConn=A";
        DriverManager.registerDriver(PhoenixDriver.INSTANCE);
    }

    private String getUniqueUrl() {
        return url + generateUniqueName();
    }

    //Have to shutdown our special delayed region server
    @AfterClass
    public static void tearDown() throws Exception {
        hbaseTestUtil.shutdownMiniCluster();
    }

    /**
     * This tests the delete path which creates a internal phoenix connection per region
     * @throws Exception
     */
    @Test
    public void testDeleteRuntimeFailureClosesConnections() throws Exception {
        String tableName = generateUniqueName();
        String connectionUrl = getUniqueUrl();
        //table with lots of regions
        String ddl = "create table " + tableName +  "  (i integer not null primary key, j integer) SALT_BUCKETS=256 ";

        Properties props = new Properties();
        props.setProperty(CLIENT_CONNECTION_MAX_ALLOWED_CONNECTIONS,String.valueOf(10));
        props.setProperty(INTERNAL_CONNECTION_MAX_ALLOWED_CONNECTIONS,String.valueOf(10));

        //delay any task handeling as that causes additional connections
        props.setProperty(TASK_HANDLING_INTERVAL_MS_ATTRIB,String.valueOf(600000));
        props.setProperty(TASK_HANDLING_INITIAL_DELAY_MS_ATTRIB,String.valueOf(600000));

        String deleteStmt = "DELETE FROM " + tableName + " WHERE 20 = j";

        try(Connection conn = DriverManager.getConnection(connectionUrl, props); Statement statement = conn.createStatement()) {
            statement.execute(ddl);
        }

        assertEquals(0, GLOBAL_OPEN_PHOENIX_CONNECTIONS.getMetric().getValue());
        assertEquals(0, GLOBAL_OPEN_INTERNAL_PHOENIX_CONNECTIONS.getMetric().getValue());
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(connectionUrl, props);
            //Enable delay for the delete
            DelayedRegionServer.setDelayEnabled(true);
            try (Statement statement = conn.createStatement()) {
                statement.execute(deleteStmt);
            }
            fail();
        } catch (SQLException e) {
            assertEquals(NEW_INTERNAL_CONNECTION_THROTTLED.getErrorCode(), e.getErrorCode());
            assertEquals(NEW_INTERNAL_CONNECTION_THROTTLED.getSQLState(), e.getSQLState());
        } finally {
            DelayedRegionServer.setDelayEnabled(false);
            if (conn != null) {
                conn.close();
            }
            long connections = GLOBAL_OPEN_PHOENIX_CONNECTIONS.getMetric().getValue();
            assertEquals(String.format("Found %d connections still open.", connections),0,connections);
            connections = GLOBAL_OPEN_INTERNAL_PHOENIX_CONNECTIONS.getMetric().getValue();
            assertEquals(String.format("Found %d internal connections still open.", connections),0 ,connections);
        }

    }

    @Test public void testClosedChildConnectionsRemovedFromParentQueue() throws SQLException {
        String tableName = generateUniqueName();
        String connectionUrl = getUniqueUrl();
        int NUMBER_OF_ROWS = 10;
        String ddl = "CREATE TABLE " + tableName + " (V BIGINT PRIMARY KEY, K BIGINT)";
        Properties props = new Properties();
        props.setProperty(CLIENT_CONNECTION_MAX_ALLOWED_CONNECTIONS, String.valueOf(10));
        props.setProperty(INTERNAL_CONNECTION_MAX_ALLOWED_CONNECTIONS, String.valueOf(10));
        try (Connection conn = DriverManager.getConnection(connectionUrl, props);
                Statement statement = conn.createStatement()) {
            statement.execute(ddl);
        }
        PhoenixConnection
                connection =
                (PhoenixConnection) DriverManager.getConnection(connectionUrl, props);
        for (int i = 0; i < NUMBER_OF_ROWS; i++) {
            connection.createStatement()
                    .execute("UPSERT INTO " + tableName + " VALUES (" + i + ", " + i + ")");
            connection.commit();
        }
        connection.setAutoCommit(false);
        try {
            for (int i = 0; i < NUMBER_OF_ROWS; i++) {
                connection.createStatement()
                        .execute("DELETE FROM " + tableName + " WHERE K = " + i);
            }
        } catch (SQLException e) {
            fail();
        } finally {
            connection.close();
        }
        // All 10 child connections should be removed successfully from the queue
        assertEquals(0, connection.getChildConnectionsCount());
    }
}