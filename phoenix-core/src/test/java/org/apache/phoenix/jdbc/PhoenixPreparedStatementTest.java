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
import static org.junit.Assert.fail;

import java.sql.*;
import java.util.Properties;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.junit.Test;

public class PhoenixPreparedStatementTest extends BaseConnectionlessQueryTest {

    @Test
    public void testSetParameter_InvalidIndex() throws Exception {
        Properties connectionProperties = new Properties();
        Connection connection = DriverManager.getConnection(getUrl(), connectionProperties);

        PreparedStatement stmt = connection.prepareStatement(
                "UPSERT INTO " + ATABLE + " (organization_id, entity_id, a_integer) " +
                        "VALUES (?,?,?)");

        stmt.setString(1, "AAA");
        stmt.setString(2, "BBB");
        stmt.setInt(3, 1);

        try {
            stmt.setString(4, "Invalid bind column");
            fail("Setting a value for a column that doesn't exist should throw SQLException");
        } catch (SQLException e) {
            // Expected exception
        }

        try {
            stmt.setString(-1, "Invalid bind column");
            fail("Setting a value for a column that doesn't exist should throw SQLException");
        } catch (SQLException e) {
            // Expected exception
        }
    }
    
    @Test
    public void testMutationUsingExecuteQueryShouldFail() throws Exception {
        Properties connectionProperties = new Properties();
        Connection connection = DriverManager.getConnection(getUrl(), connectionProperties);
        PreparedStatement stmt = connection.prepareStatement("DELETE FROM " + ATABLE);
        try {
            stmt.executeQuery();
            fail();
        } catch(SQLException e) {
            assertEquals(SQLExceptionCode.EXECUTE_QUERY_NOT_APPLICABLE.getErrorCode(), e.getErrorCode());
        }
    }
    
    @Test
    public void testQueriesUsingExecuteUpdateShouldFail() throws Exception {
        Properties connectionProperties = new Properties();
        Connection connection = DriverManager.getConnection(getUrl(), connectionProperties);
        PreparedStatement stmt = connection.prepareStatement("SELECT * FROM " + ATABLE);
        try {
            stmt.executeUpdate();
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
        PreparedStatement stmt = connection.prepareStatement("SELECT * FROM " + ATABLE);
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
        PreparedStatement stmt = connection.prepareStatement("SELECT * FROM " + ATABLE);
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
        PreparedStatement stmt = connection.prepareStatement("SELECT * FROM " + ATABLE);
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
        PreparedStatement stmt = connection.prepareStatement("SELECT * FROM " + ATABLE);
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
        PreparedStatement stmt = connection.prepareStatement("SELECT * FROM " + ATABLE);
        PhoenixStatement phoenixStmt = stmt.unwrap(PhoenixStatement.class);
    
        // Assert
        assertEquals(0, stmt.getQueryTimeout());
        assertEquals(0, phoenixStmt.getQueryTimeoutInMillis());
    }

}
