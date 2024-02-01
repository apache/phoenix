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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.List;
import java.util.Properties;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.junit.Test;

public class PhoenixStatementTest extends BaseConnectionlessQueryTest {

    private static Field connectionField;
    private static Field batchField;

    static {
        try {
            connectionField = PhoenixStatement.class.getDeclaredField("connection");
            connectionField.setAccessible(true);
            batchField = PhoenixStatement.class.getDeclaredField("batch");
            batchField.setAccessible(true);
        } catch (NoSuchFieldException | SecurityException e) {
            //Test would fail
        }
    }

    @Test
    public void testMutationUsingExecuteQueryShouldFail() throws Exception {
        Properties connectionProperties = new Properties();
        Connection connection = DriverManager.getConnection(getUrl(), connectionProperties);
        Statement stmt = connection.createStatement();
        try {
            stmt.executeQuery("DELETE FROM " + ATABLE);
            fail();
        } catch(SQLException e) {
            assertEquals(SQLExceptionCode.EXECUTE_QUERY_NOT_APPLICABLE.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testQueriesUsingExecuteUpdateShouldFail() throws Exception {
        Properties connectionProperties = new Properties();
        Connection connection = DriverManager.getConnection(getUrl(), connectionProperties);
        Statement stmt = connection.createStatement();
        try {
            stmt.executeUpdate("SELECT * FROM " + ATABLE);
            fail();
        } catch(SQLException e) {
            assertEquals(SQLExceptionCode.EXECUTE_UPDATE_NOT_APPLICABLE.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    /**
     * Validates that if a user sets the query timeout via the
     * stmt.setQueryTimeout() JDBC method, we correctly store the timeout
     * in both milliseconds and seconds.
     */
    public void testSettingQueryTimeoutViaJdbc() throws Exception {
        // Arrange
        Connection connection = DriverManager.getConnection(getUrl());
        Statement stmt = connection.createStatement();
        PhoenixStatement phoenixStmt = stmt.unwrap(PhoenixStatement.class);

        // Act
        stmt.setQueryTimeout(3);

        // Assert
        assertEquals(3, stmt.getQueryTimeout());
        assertEquals(3000, phoenixStmt.getQueryTimeoutInMillis());
    }

    @Test
    /**
     * Validates if a user sets the timeout to zero that we store the timeout
     * in millis as the Integer.MAX_VALUE.
     */
    public void testSettingZeroQueryTimeoutViaJdbc() throws Exception {
        // Arrange
        Connection connection = DriverManager.getConnection(getUrl());
        Statement stmt = connection.createStatement();
        PhoenixStatement phoenixStmt = stmt.unwrap(PhoenixStatement.class);

        // Act
        stmt.setQueryTimeout(0);

        // Assert
        assertEquals(Integer.MAX_VALUE / 1000, stmt.getQueryTimeout());
        assertEquals(Integer.MAX_VALUE, phoenixStmt.getQueryTimeoutInMillis());
    }

    @Test
    /**
     * Validates that is negative value is supplied we set the timeout to the default.
     */
    public void testSettingNegativeQueryTimeoutViaJdbc() throws Exception {
        // Arrange
        Connection connection = DriverManager.getConnection(getUrl());
        Statement stmt = connection.createStatement();
        PhoenixStatement phoenixStmt = stmt.unwrap(PhoenixStatement.class);
        PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class);
        int defaultQueryTimeout = phoenixConnection.getQueryServices().getProps().getInt(QueryServices.THREAD_TIMEOUT_MS_ATTRIB,
            QueryServicesOptions.DEFAULT_THREAD_TIMEOUT_MS);

        // Act
        stmt.setQueryTimeout(-1);

        // Assert
        assertEquals(defaultQueryTimeout / 1000, stmt.getQueryTimeout());
        assertEquals(defaultQueryTimeout, phoenixStmt.getQueryTimeoutInMillis());
    }

    @Test
    /**
     * Validates that setting custom phoenix query timeout using
     * the phoenix.query.timeoutMs config property is honored.
     */
    public void testCustomQueryTimeout() throws Exception {
        // Arrange
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("phoenix.query.timeoutMs", "2350");
        Connection connection = DriverManager.getConnection(getUrl(), connectionProperties);
        Statement stmt = connection.createStatement();
        PhoenixStatement phoenixStmt = stmt.unwrap(PhoenixStatement.class);

        // Assert
        assertEquals(3, stmt.getQueryTimeout());
        assertEquals(2350, phoenixStmt.getQueryTimeoutInMillis());
    }

    @Test
    public void testZeroCustomQueryTimeout() throws Exception {
        // Arrange
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("phoenix.query.timeoutMs", "0");
        Connection connection = DriverManager.getConnection(getUrl(), connectionProperties);
        Statement stmt = connection.createStatement();
        PhoenixStatement phoenixStmt = stmt.unwrap(PhoenixStatement.class);

        // Assert
        assertEquals(0, stmt.getQueryTimeout());
        assertEquals(0, phoenixStmt.getQueryTimeoutInMillis());
    }

    @Test
    public void testExecuteBatchWithFailedStatement() throws Exception {
        // Arrange
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("phoenix.query.timeoutMs", "0");
        Connection connection = DriverManager.getConnection(getUrl(), connectionProperties);
        Statement stmt = connection.createStatement();
        PhoenixConnection connSpy = spy(connection.unwrap(PhoenixConnection.class));
        connectionField.set(stmt, connSpy);
        List<PhoenixPreparedStatement> batch = Lists.newArrayList(
                mock(PhoenixPreparedStatement.class),
                mock(PhoenixPreparedStatement.class),
                mock(PhoenixPreparedStatement.class));
        batchField.set(stmt, batch);
        final String exMsg = "TEST";
        when(batch.get(0).getUpdateCount()).thenReturn(1);
        doThrow(new SQLException(exMsg)).when(batch.get(1)).executeForBatch();
        // However, we don't expect this to be called.
        when(batch.get(1).getUpdateCount()).thenReturn(1);

        // Act & Assert
        BatchUpdateException ex = assertThrows(BatchUpdateException.class, () -> stmt.executeBatch());
        assertEquals(exMsg, ex.getCause().getMessage());
        int[] updateCounts = ex.getUpdateCounts();
        assertEquals(3, updateCounts.length);
        assertEquals(1, updateCounts[0]);
        assertEquals(Statement.EXECUTE_FAILED, updateCounts[1]);
        assertEquals(-1, updateCounts[2]);
        verify(connSpy, never()).commit(); // Ensure commit was never called.
    }

    @Test
    public void testExecuteBatchWithCommitFailure() throws Exception {
        // Arrange
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("phoenix.query.timeoutMs", "0");
        Connection connection = DriverManager.getConnection(getUrl(), connectionProperties);
        Statement stmt = connection.createStatement();
        PhoenixConnection connSpy = spy(connection.unwrap(PhoenixConnection.class));
        connectionField.set(stmt, connSpy);
        List<PhoenixPreparedStatement> batch = Lists.newArrayList(
                mock(PhoenixPreparedStatement.class));
        batchField.set(stmt, batch);
        final String exMsg = "TEST";
        doThrow(new SQLException(exMsg)).when(connSpy).commit();
        when(connSpy.getAutoCommit()).thenReturn(true);

        // Act & Assert
        BatchUpdateException ex = assertThrows(BatchUpdateException.class, () -> stmt.executeBatch());
        assertEquals(exMsg, ex.getCause().getMessage());
        assertNull(ex.getUpdateCounts());
    }

    @Test
    public void testRecursiveClose() throws SQLException {
        Connection connection = DriverManager.getConnection(getUrl());
        Statement stmt1 = connection.createStatement();
        ResultSet rs11 = stmt1.executeQuery("select * from atable");
        rs11.close();
        assertTrue(rs11.isClosed());
        ResultSet rs12 = stmt1.executeQuery("select * from atable");
        stmt1.close();
        assertTrue(stmt1.isClosed());
        assertTrue(rs12.isClosed());

        Statement stmt2 = connection.createStatement();
        stmt2.closeOnCompletion();
        ResultSet rs21 = stmt2.executeQuery("select * from atable");
        rs21.close();
        assertTrue(stmt2.isClosed());

        Statement stmt3 = connection.createStatement();
        ResultSet rs31 = stmt3.executeQuery("select * from atable");
        stmt3.executeUpdate("upsert into ATABLE VALUES ('1', '2', '3')");
        assertTrue(rs31.isClosed());
        ResultSet rs32 = stmt3.executeQuery("select * from atable");
        ResultSet rs33 = stmt3.executeQuery("select * from atable");
        assertTrue(rs32.isClosed());

        Statement stmt4 = connection.createStatement();
        Statement stmt5 = connection.createStatement();
        ResultSet rs41 = stmt3.executeQuery("select * from atable");
        ResultSet rs51 = stmt3.executeQuery("select * from atable");
        connection.close();
        assertTrue(connection.isClosed());
        assertTrue(stmt4.isClosed());
        assertTrue(stmt5.isClosed());
        assertTrue(rs41.isClosed());
        assertTrue(rs51.isClosed());
    }
}
