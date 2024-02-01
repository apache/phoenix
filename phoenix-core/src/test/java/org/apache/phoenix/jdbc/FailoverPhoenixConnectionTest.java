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

import static org.apache.phoenix.exception.SQLExceptionCode.CLASS_NOT_UNWRAPPABLE;
import static org.apache.phoenix.exception.SQLExceptionCode.HA_CLOSED_AFTER_FAILOVER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.exception.FailoverSQLException;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.HighAvailabilityGroup.HAGroupInfo;

/**
 * Unit test for {@link FailoverPhoenixConnection}.
 *
 * @see FailoverPhoenixConnectionIT
 */
public class FailoverPhoenixConnectionTest {
    private static final Logger LOG = LoggerFactory.getLogger(FailoverPhoenixConnectionTest.class);

    @Mock PhoenixConnection connection1;
    @Mock PhoenixConnection connection2;
    @Mock HighAvailabilityGroup haGroup;

    final HAGroupInfo haGroupInfo = new HAGroupInfo("fake", "zk1", "zk2");
    FailoverPhoenixConnection failoverConnection; // this connection itself is not mocked or spied.

    @Before
    public void init() throws SQLException {
        MockitoAnnotations.initMocks(this);
        when(haGroup.getGroupInfo()).thenReturn(haGroupInfo);
        when(haGroup.connectActive(any(Properties.class))).thenReturn(connection1);

        failoverConnection = new FailoverPhoenixConnection(haGroup, new Properties());
    }

    /**
     * Test helper method {@link FailoverPhoenixConnection#wrapActionDuringFailover}.
     */
    @Test
    public void testWrapActionDuringFailover() throws SQLException {
        // Test SupplierWithSQLException which returns a value
        String str = "Hello, World!";
        assertEquals(str, failoverConnection.wrapActionDuringFailover(() -> str));

        // Test RunWithSQLException which does not return value
        final AtomicInteger counter = new AtomicInteger(0);
        failoverConnection.wrapActionDuringFailover(counter::incrementAndGet);
        assertEquals(1, counter.get());
    }

    /**
     * Test that after calling failover(), the old connection got closed with FailoverSQLException,
     * and a new Phoenix connection is opened.
     */
    @Test
    public void testFailover() throws SQLException {
        // Make HAGroup return a different phoenix connection when it gets called next time
        when(haGroup.connectActive(any(Properties.class))).thenReturn(connection2);

        // explicit call failover
        failoverConnection.failover(1000L);

        // The old connection should have been closed due to failover
        verify(connection1, times(1)).close(any(FailoverSQLException.class));
        // A new Phoenix connection is wrapped underneath
        assertEquals(connection2, failoverConnection.getWrappedConnection());
    }

    /**
     * Test static {@link FailoverPhoenixConnection#failover(Connection, long)} method.
     */
    @Test
    public void testFailoverStatic() throws SQLException {
        try {
            FailoverPhoenixConnection.failover(connection1, 1000L);
            fail("Should have failed since plain phoenix connection can not failover!");
        } catch (SQLException e) {
            assertEquals(CLASS_NOT_UNWRAPPABLE.getErrorCode(),e.getErrorCode());
            LOG.info("Got expected exception when trying to failover on non-HA connection", e);
        }

        FailoverPhoenixConnection.failover(failoverConnection, 1000L);
        // The old connection should have been closed due to failover
        verify(connection1, times(1)).close(any(FailoverSQLException.class));
    }

    /**
     * Test that failover() is no-op when it is already pointing to active cluster.
     */
    @Test
    public void testActiveFailoverIsNoOp() throws SQLException {
        when(haGroup.isActive(connection1)).thenReturn(true);
        // Make HAGroup return a different phoenix connection when it gets called next time
        when(haGroup.connectActive(any(Properties.class))).thenReturn(connection2);

        failoverConnection.failover(1000L);

        // The wrapped phoenix connection is not closed since it is already connecting to ACTIVE
        verify(connection1, never()).close(any(FailoverSQLException.class));
        assertEquals(connection1, failoverConnection.getWrappedConnection());
    }

    /**
     * Test that with {@link FailoverPolicy.FailoverToActivePolicy}, automatic failover happens.
     */
    @Test
    public void testFailoverToActivePolicy() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(FailoverPolicy.PHOENIX_HA_FAILOVER_POLICY_ATTR,
                FailoverPolicy.FailoverToActivePolicy.NAME);
        failoverConnection = new FailoverPhoenixConnection(haGroup, properties);

        LOG.info("Close the wrapped phoenix connection due to failover...");
        // Make HAGroup return a different phoenix connection when it gets called next time
        when(haGroup.connectActive(any(Properties.class))).thenReturn(connection2);
        // Mimic wrapped phoenix connection gets closed by HA group
        doThrow(new FailoverSQLException("", "", new Exception())).when(connection1).commit();

        // During this operation, internal failover should have happened automatically
        failoverConnection.commit();

        verify(connection1, times(1)).close(any(SQLException.class));
        assertEquals(connection2, failoverConnection.getWrappedConnection());
    }

    /**
     * Test that failover() will fail once the connection has been closed.
     */
    @Test
    public void testConnectionClosed() throws SQLException {
        failoverConnection.close();

        try {
            failoverConnection.failover(1000L);
            fail("failover should have failed after failover connection is closed!");
        } catch (SQLException e) {
            LOG.info("Got expected exception", e);
            assertEquals(SQLExceptionCode.CONNECTION_CLOSED.getErrorCode(), e.getErrorCode());
        }

        // Assert that no connection has been doubly closed
        verify(connection1, never()).close(any(FailoverSQLException.class));
        verify(connection2, never()).close(any(FailoverSQLException.class));
    }

    /**
     * Test that closing a closed failover connection is a no-op.
     */
    @Test
    public void testCloseOnceMore() throws SQLException {
        failoverConnection.close();
        assertTrue(failoverConnection.isClosed());
        // connection got closed but not due to failover
        verify(connection1, times(1)).close();
        verify(connection1, never()).close(any(SQLException.class));

        // close connection once more
        failoverConnection.close();
        verify(connection1, times(1)).close();
        verify(connection1, never()).close(any(SQLException.class));
    }

    /**
     * Test that when HA group fails to create a connection, the failover connection will report
     * back the connection establishing error instead of NullPointerException or other ones.
     */
    @Test
    public void testCheckConnection() throws SQLException {
        // Make the wrapped phoenix connection null. This could happen if HAGroup is failing.
        when(haGroup.connectActive(any(Properties.class))).thenReturn(null);
        failoverConnection = new FailoverPhoenixConnection(haGroup, new Properties());
        assertNull(failoverConnection.getWrappedConnection());

        try {
            failoverConnection.commit();
            fail("Should have failed because the wrapped phoenix connection is null");
        } catch (SQLException e) {
            LOG.info("Got expected exception", e);
            assertEquals(SQLExceptionCode.CANNOT_ESTABLISH_CONNECTION.getErrorCode(),
                    e.getErrorCode());
        }
    }
}
