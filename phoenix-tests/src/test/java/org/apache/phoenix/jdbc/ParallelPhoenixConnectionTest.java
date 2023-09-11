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

import static org.apache.hadoop.test.GenericTestUtils.waitFor;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.monitoring.GlobalClientMetrics;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.exceptions.verification.WantedButNotInvoked;
import org.mockito.invocation.InvocationOnMock;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class ParallelPhoenixConnectionTest {

    ParallelPhoenixContext context;

    ParallelPhoenixConnection parallelPhoenixConnection;
    PhoenixConnection connection1 = Mockito.mock(PhoenixConnection.class);
    PhoenixConnection connection2 = Mockito.mock(PhoenixConnection.class);

    @Before
    public void init() throws SQLException {
        context = new ParallelPhoenixContext(new Properties(), Mockito.mock(HighAvailabilityGroup.class),
            HighAvailabilityTestingUtility.getListOfSingleThreadExecutorServices(), null);
        parallelPhoenixConnection = new ParallelPhoenixConnection(context,CompletableFuture.completedFuture(connection1),CompletableFuture.completedFuture(connection2));
    }

    @Test
    public void getWarningsBothWarnTest() throws Exception {
        SQLWarning warning1 = new SQLWarning("warning1");
        SQLWarning warning2 = new SQLWarning("warning2");
        Mockito.when(connection1.getWarnings()).thenReturn(warning1);
        Mockito.when(connection2.getWarnings()).thenReturn(warning2);

        SQLWarning result = parallelPhoenixConnection.getWarnings();
        assertEquals(warning1,result.getNextWarning());
        assertEquals(warning2,result.getNextWarning().getNextWarning());
    }

    @Test
    public void getWarnings1WarnTest() throws Exception {
        SQLWarning warning2 = new SQLWarning("warning2");
        Mockito.when(connection1.getWarnings()).thenReturn(null);
        Mockito.when(connection2.getWarnings()).thenReturn(warning2);

        SQLWarning result = parallelPhoenixConnection.getWarnings();
        assertEquals(warning2,result);
    }

    @Test
    public void getWarnings0WarnTest() throws Exception {
        Mockito.when(connection1.getWarnings()).thenReturn(null);
        Mockito.when(connection2.getWarnings()).thenReturn(null);

        SQLWarning result = parallelPhoenixConnection.getWarnings();
        assertNull(result);
    }

    @Test
    public void isWrapperForPhoenixConnectionFalseTest() throws SQLException {
        boolean result = parallelPhoenixConnection.isWrapperFor(PhoenixConnection.class);
        assertFalse(result);
    }

    @Test
    public void isWrapperForPhoenixMonitoredConnectionTrueTest() throws SQLException {
        boolean result = parallelPhoenixConnection.isWrapperFor(PhoenixMonitoredConnection.class);
        assertTrue(result);
    }

    @Test
    public void unwrapPhoenixConnectionFailsTest() {
        try {
            parallelPhoenixConnection.unwrap(PhoenixConnection.class);
        } catch (SQLException e) {
            assertEquals(e.getErrorCode(), SQLExceptionCode.CLASS_NOT_UNWRAPPABLE.getErrorCode());
        }
    }

    @Test
    public void unwrapPhoenixMonitoredConnectionTest() throws SQLException {
        PhoenixMonitoredConnection result = parallelPhoenixConnection.unwrap(PhoenixMonitoredConnection.class);
        assertEquals(parallelPhoenixConnection,result);
    }

    @Test
    public void testOpenConnection1Error() throws SQLException {

        CompletableFuture<PhoenixConnection> futureConnection1 = CompletableFuture.supplyAsync(() -> {
            throw new CompletionException(new Exception("Failed in completing future connection1"));
        });

        CompletableFuture<PhoenixConnection> futureConnection2 = CompletableFuture.completedFuture(connection2);

        // Even if the connection open for one of the connections failed, since the other connection
        // was initialized successfully - the ParallelPhoenixConnection object should be returned. Also, the
        // close should be successful, as one of the connection closed successfully.
        parallelPhoenixConnection =
                new ParallelPhoenixConnection(context,
                        futureConnection1,
                        futureConnection2);

        parallelPhoenixConnection.close();
        Mockito.verify(connection2).close();
    }

    @Test
    public void testOpenConnection2Error() throws SQLException {

        CompletableFuture<PhoenixConnection> futureConnection1 = CompletableFuture.completedFuture(connection1);
        CompletableFuture<PhoenixConnection> futureConnection2 = CompletableFuture.supplyAsync(() -> {
            throw new CompletionException(new Exception("Failed in completing future connection2"));
        });

        // Even if the connection open for one of the connections failed, since the other connection
        // was initialized successfully - the ParallelPhoenixConnection object should be returned. Also, the
        // close should be successful, as one of the connection closed successfully.
        parallelPhoenixConnection =
                new ParallelPhoenixConnection(context,
                        futureConnection1,
                        futureConnection2);

        parallelPhoenixConnection.close();
        Mockito.verify(connection1).close();
    }

    @Test
    public void testOpenBothConnectionError()  {

        CompletableFuture<PhoenixConnection> futureConnection1 = CompletableFuture.supplyAsync(() -> {
            throw new CompletionException(new Exception("Failed in completing future connection1"));
        });
        CompletableFuture<PhoenixConnection> futureConnection2 = CompletableFuture.supplyAsync(() -> {
            throw new CompletionException(new Exception("Failed in completing future connection2"));
        });

        // Since there were failures in establishing both the connections, the
        // initialization of ParallelPhoenixConnection itself should throw an exception.
        try {
            parallelPhoenixConnection =
                    new ParallelPhoenixConnection(context,
                            futureConnection1,
                            futureConnection2);
            fail("Initialization should throw an exception if both the future connections fail.");
        } catch (SQLException e) {
        }
    }

    @Test
    public void testOpenConnection1Delay() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(ParallelPhoenixUtil.PHOENIX_HA_PARALLEL_OPERATION_TIMEOUT_ATTRIB,
                "1000");
        ParallelPhoenixContext context =
                new ParallelPhoenixContext(properties, Mockito.mock(HighAvailabilityGroup.class),
                        HighAvailabilityTestingUtility.getListOfSingleThreadExecutorServices(), null);

        CountDownLatch cdl = new CountDownLatch(1);
        CompletableFuture<PhoenixConnection> futureConnection1 = CompletableFuture.supplyAsync(getDelayConnectionSupplier(cdl, connection1));
        CompletableFuture<PhoenixConnection> futureConnection2 = CompletableFuture.completedFuture(connection2);

        // Even though there is delay in establishing connection1, the other connection
        // should be established successfully.
        parallelPhoenixConnection =
                new ParallelPhoenixConnection(context,
                        futureConnection1,
                        futureConnection2);

        // One of the connections, i.e. connection2, should be closed successfully.
        parallelPhoenixConnection.close();
        Mockito.verify(connection2).close();
        cdl.countDown();
        waitForConnectionClose(connection1);
    }

    @Test
    public void testOpenConnection2Delay() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(ParallelPhoenixUtil.PHOENIX_HA_PARALLEL_OPERATION_TIMEOUT_ATTRIB,
                "1000");
        ParallelPhoenixContext context =
                new ParallelPhoenixContext(properties, Mockito.mock(HighAvailabilityGroup.class),
                        HighAvailabilityTestingUtility.getListOfSingleThreadExecutorServices(), null);

        CountDownLatch cdl = new CountDownLatch(1);
        CompletableFuture<PhoenixConnection> futureConnection1 = CompletableFuture.completedFuture(connection1);
        CompletableFuture<PhoenixConnection> futureConnection2 = CompletableFuture.supplyAsync(getDelayConnectionSupplier(cdl, connection2));

        // Even though there is delay in establishing connection2, the other connection
        // should be established successfully.
        parallelPhoenixConnection =
                new ParallelPhoenixConnection(context,
                        futureConnection1,
                        futureConnection2);

        // One of the connections, i.e. connection1, should be closed successfully.
        parallelPhoenixConnection.close();
        Mockito.verify(connection1).close();
        cdl.countDown();
        waitForConnectionClose(connection2);
    }

    @Test(timeout = 10000)
    public void testOpenBothConnectionDelay() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(ParallelPhoenixUtil.PHOENIX_HA_PARALLEL_OPERATION_TIMEOUT_ATTRIB,
                "1000");
        ParallelPhoenixContext context =
                new ParallelPhoenixContext(properties, Mockito.mock(HighAvailabilityGroup.class),
                        HighAvailabilityTestingUtility.getListOfSingleThreadExecutorServices(), null);

        CountDownLatch cdl1 = new CountDownLatch(1);
        CompletableFuture<PhoenixConnection> futureConnection1 = CompletableFuture.supplyAsync(getDelayConnectionSupplier(cdl1, connection1));

        CountDownLatch cdl2 = new CountDownLatch(1);
        CompletableFuture<PhoenixConnection> futureConnection2 = CompletableFuture.supplyAsync(getDelayConnectionSupplier(cdl2, connection2));

        long prevTimeoutCounter = GlobalClientMetrics.GLOBAL_HA_PARALLEL_TASK_TIMEOUT_COUNTER.getMetric()
                .getValue();

        // Both the connections have a delay in establishing the connection, in such cases,
        // the initialization of the ParallelPhoenixConnection should itself timeout.
        try {
            parallelPhoenixConnection =
                    new ParallelPhoenixConnection(context,
                            futureConnection1,
                            futureConnection2);
            fail("Initialization should throw an exception if both the future connections timeout");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.OPERATION_TIMED_OUT.getErrorCode(),
                    e.getErrorCode());
            assertTrue(GlobalClientMetrics.GLOBAL_HA_PARALLEL_TASK_TIMEOUT_COUNTER.getMetric()
                    .getValue() > prevTimeoutCounter);
        }
    }

    @Test
    public void testCloseConnection1Error() throws SQLException {
        Mockito.doThrow(new SQLException()).when(connection1).close();
        parallelPhoenixConnection.close();
        Mockito.verify(connection2).close();
    }

    @Test
    public void testCloseConnection2Error() throws SQLException {
        Mockito.doThrow(new SQLException()).when(connection2).close();
        parallelPhoenixConnection.close();
        Mockito.verify(connection1).close();
    }

    @Test
    public void testCloseBothConnectionError() throws SQLException {
        Mockito.doThrow(new SQLException()).when(connection1).close();
        Mockito.doThrow(new SQLException()).when(connection2).close();
        try {
            parallelPhoenixConnection.close();
            fail("Close should throw exception when both underlying close throw exceptions");
        } catch (SQLException e) {
        }
        Mockito.verify(connection1).close();
        Mockito.verify(connection2).close();
    }

    @Test
    public void testConnection1CloseDelay() throws Exception {
        CountDownLatch cdl = new CountDownLatch(1);
        Supplier<Void> delaySupplier = getDelaySupplier(cdl);
        context.chainOnConn1(delaySupplier);

        // Even though the chain on conn1 is lagging, the close is not
        // chained and happens on a different executor pool. The close
        // is async, and anyone of the connection can be closed first.
        // We cannot deterministically determine which connection was
        // closed first, hence we check on the count of close operation
        // on both connection objects, and expect at least one to be called.
        parallelPhoenixConnection.close();

        long countConnection1 = Mockito.mockingDetails(connection1).getInvocations().stream().
                map(InvocationOnMock::getMethod).filter(s -> s.getName().equals("close")).count();
        long countConnection2 = Mockito.mockingDetails(connection2).getInvocations().stream().
                map(InvocationOnMock::getMethod).filter(s -> s.getName().equals("close")).count();

        assertTrue("Close should be called on at least one of the connections", countConnection1 > 0 || countConnection2 > 0);
    }

    @Test
    public void testConnection2CloseDelay() throws Exception {
        CountDownLatch cdl = new CountDownLatch(1);
        Supplier<Void> delaySupplier = getDelaySupplier(cdl);
        context.chainOnConn2(delaySupplier);
        // Chain on conn2 is lagging, we should return after closing conn1
        // Even though the chain on conn2 is lagging, the close is not
        // chained and happens on a different executor pool. The close
        // is async, and anyone of the connection can be closed first.
        // We cannot deterministically determine which connection was
        // closed first, hence we check on the count of close operation
        // on both connection objects, and expect at least one to be called.
        parallelPhoenixConnection.close();

        long countConnection1 = Mockito.mockingDetails(connection1).getInvocations().stream().
                map(InvocationOnMock::getMethod).filter(s -> s.getName().equals("close")).count();
        long countConnection2 = Mockito.mockingDetails(connection2).getInvocations().stream().
                map(InvocationOnMock::getMethod).filter(s -> s.getName().equals("close")).count();

        assertTrue("Close should be called on at least one of the connections", countConnection1 > 0 || countConnection2 > 0);

        cdl.countDown();
    }

    @Test
    public void testConnectionCloseNoTimeout() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(ParallelPhoenixUtil.PHOENIX_HA_PARALLEL_OPERATION_TIMEOUT_ATTRIB,
            "1000");
        ParallelPhoenixContext context =
                new ParallelPhoenixContext(properties, Mockito.mock(HighAvailabilityGroup.class),
                    HighAvailabilityTestingUtility.getListOfSingleThreadExecutorServices(), null);
        parallelPhoenixConnection =
                new ParallelPhoenixConnection(context,
                        CompletableFuture.completedFuture(connection1),
                        CompletableFuture.completedFuture(connection2));
        CountDownLatch cdl1 = new CountDownLatch(1);
        CountDownLatch cdl2 = new CountDownLatch(1);
        Supplier<Void> delaySupplier1 = getDelaySupplier(cdl1);
        Supplier<Void> delaySupplier2 = getDelaySupplier(cdl2);
        context.chainOnConn1(delaySupplier1);
        context.chainOnConn2(delaySupplier2);
        // Even though the chain on both conn1 and conn2 is lagging,
        // the close is not chained and happens on a different executor pool.
        // The close is async, and anyone of the connection can be closed first.
        // We cannot deterministically determine which connection was
        // closed first, hence we check on the count of close operation
        // on both connection objects, and expect at least one to be called.
        parallelPhoenixConnection.close();

        long countConnection1 = Mockito.mockingDetails(connection1).getInvocations().stream().
                map(InvocationOnMock::getMethod).filter(s -> s.getName().equals("close")).count();
        long countConnection2 = Mockito.mockingDetails(connection2).getInvocations().stream().
                map(InvocationOnMock::getMethod).filter(s -> s.getName().equals("close")).count();

        assertTrue("Close should be called on at least one of the connections", countConnection1 > 0 || countConnection2 > 0);

        cdl1.countDown();
        cdl2.countDown();
    }

    private void waitForConnectionClose(PhoenixConnection connection) throws Exception {
        waitFor(() -> {
            try {
                Mockito.verify(connection).close();
            } catch (SQLException | WantedButNotInvoked e) {
                return false;
            }
            return true;
        }, 1000, 30000);
    }

    private Supplier<Void> getDelaySupplier(CountDownLatch cdl) {
        return (() -> {
            try {
                cdl.await();
            } catch (InterruptedException e) {
                throw new CompletionException(e);
            }
            return null;
        });
    }

    private Supplier<PhoenixConnection> getDelayConnectionSupplier(CountDownLatch cdl, PhoenixConnection returnConnection) {
        return (() -> {
            try {
                cdl.await();
            } catch (InterruptedException e) {
                throw new CompletionException(e);
            }
            return returnConnection;
        });
    }
}