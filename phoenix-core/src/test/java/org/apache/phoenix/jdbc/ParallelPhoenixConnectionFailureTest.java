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
package org.apache.phoenix.jdbc;

import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;
import static org.apache.phoenix.util.PhoenixRuntime.PHOENIX_TEST_DRIVER_URL_PARAM;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

/**
 * Test to make sure once an error is encountered on an underlying phoenix connection
 * we don't use that connection during the entire lifecycle of client conenction
 */
@Category(NeedsOwnMiniClusterTest.class)
public class ParallelPhoenixConnectionFailureTest extends BaseTest {

    private static String url =
            JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + PhoenixRuntime.CONNECTIONLESS
                    + JDBC_PROTOCOL_SEPARATOR + HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT
                    + JDBC_PROTOCOL_TERMINATOR + PHOENIX_TEST_DRIVER_URL_PARAM;;
    private static PhoenixTestDriver driver;
    private static HBaseTestingUtility hbaseTestingUtility;

    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        driver = initAndRegisterTestDriver(url, new ReadOnlyProps(props));
        hbaseTestingUtility = new HBaseTestingUtility();
    }

    @AfterClass
    public static void tearDownAfterClass() {
        destroyDriver(driver);
    }

    @Test
    public void testExecuteQueryChainFailure() throws SQLException {
        PhoenixConnection conn1 = (PhoenixConnection) DriverManager.getConnection(url);
        PhoenixConnection conn2 = (PhoenixConnection) DriverManager.getConnection(url);
        PhoenixConnection connSpy1 = Mockito.spy(conn1);
        PhoenixConnection connSpy2 = Mockito.spy(conn2);
        AtomicInteger numStatementsCreatedOnConn1 = new AtomicInteger();
        AtomicInteger numStatementsCreatedOnConn2 = new AtomicInteger();
        Answer<Statement> answer1 = (i -> {
            numStatementsCreatedOnConn1.getAndIncrement();
            return conn1.createStatement();
        });
        Answer<Statement> answer2 = (i -> {
            numStatementsCreatedOnConn2.getAndIncrement();
            return conn2.createStatement();
        });
        doAnswer(answer1).when(connSpy1).createStatement();
        doAnswer(answer2).when(connSpy2).createStatement();
        ParallelPhoenixContext context =
                new ParallelPhoenixContext(new Properties(),
                        Mockito.mock(HighAvailabilityGroup.class),
                        HighAvailabilityTestingUtility.getListOfSingleThreadExecutorServices(),
                        null);
        ParallelPhoenixConnection parallelConn =
                new ParallelPhoenixConnection(context, CompletableFuture.completedFuture(connSpy1),
                        CompletableFuture.completedFuture(connSpy2));
        parallelConn.createStatement().execute("SELECT * FROM SYSTEM.CATALOG");
        parallelConn.createStatement().execute("SELECT * FROM SYSTEM.CATALOG");
        // Verify successful execution on both connections
        hbaseTestingUtility.waitFor(10000, () -> (numStatementsCreatedOnConn1.get() == 2)
                && (numStatementsCreatedOnConn2.get() == 2));
        // Error on conn1, we shouldn't use conn1 after that
        doThrow(new SQLException()).when(connSpy1).createStatement();
        parallelConn.createStatement().execute("SELECT * FROM SYSTEM.CATALOG");
        hbaseTestingUtility.waitFor(10000, () -> numStatementsCreatedOnConn2.get() == 3);
        doAnswer(answer1).when(connSpy1).createStatement();
        // Should still have a successful execution only from conn2 since conn1 errored before
        parallelConn.createStatement().execute("SELECT * FROM SYSTEM.CATALOG");
        hbaseTestingUtility.waitFor(10000, () -> (numStatementsCreatedOnConn1.get() == 2)
                && (numStatementsCreatedOnConn2.get() == 4));
        // Any task that we chain on conn1 should error out
        assertTrue(context.chainOnConn1(() -> Boolean.TRUE).isCompletedExceptionally());
    }
}
