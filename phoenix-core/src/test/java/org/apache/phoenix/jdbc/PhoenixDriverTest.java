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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Ignore;
import org.junit.Test;

public class PhoenixDriverTest extends BaseConnectionlessQueryTest {


    @Test
    public void testFirstConnectionWhenPropsHasTenantId() throws Exception {
        Properties props = new Properties();
        final String tenantId = "00Dxx0000001234";
        props.put(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);

        Connection connection = new PhoenixTestDriver().connect(getUrl(), props);
        assertEquals(tenantId, connection.getClientInfo(PhoenixRuntime.TENANT_ID_ATTRIB));
    }

    @Test
    public void testFirstConnectionWhenUrlHasTenantId() throws Exception {
        final String tenantId = "00Dxx0000001234";
        String url = getUrl() + ";" + PhoenixRuntime.TENANT_ID_ATTRIB + "=" + tenantId;
        Driver driver = new PhoenixTestDriver();

        driver.connect(url, new Properties());
    }

    @Test
    public void testMaxMutationSizeSetCorrectly() throws Exception {
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty(QueryServices.MAX_MUTATION_SIZE_ATTRIB,"100");
        connectionProperties.setProperty(QueryServices.IMMUTABLE_ROWS_ATTRIB,"100");
        Connection connection = DriverManager.getConnection(getUrl(), connectionProperties);

        PreparedStatement stmt = connection.prepareStatement("upsert into " + ATABLE + " (organization_id, entity_id, a_integer) values (?,?,?)");
        try {
            for (int i = 0; i < 200; i++) {
                stmt.setString(1, "AAAA" + i);
                stmt.setString(2, "BBBB" + i);
                stmt.setInt(3, 1);
                stmt.execute();
            }
            fail("Upsert should have failed since the number of upserts (200) is greater than the MAX_MUTATION_SIZE_ATTRIB (100)");
        } catch (IllegalArgumentException expected) {}
    }

    @Test
    public void testMaxMutationSizeInBytesSetCorrectly() throws Exception {
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty(QueryServices.MUTATE_BATCH_SIZE_BYTES_ATTRIB,"100");
        PhoenixConnection connection = (PhoenixConnection) DriverManager.getConnection(getUrl(), connectionProperties);
        assertEquals(100L, connection.getMutateBatchSizeBytes());
        assertEquals(100L, connection.getMutationState().getMaxSizeBytes());
    }
    
    @Test
    public void testDisallowNegativeScn() {
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, String.valueOf(-100));
        try {
            DriverManager.getConnection(getUrl(), props);
            fail("Creating a phoenix connection with negative scn is not allowed");
        } catch(SQLException e) {
            assertEquals(SQLExceptionCode.INVALID_SCN.getErrorCode(), e.getErrorCode());
        }
    }
    
    @Ignore
    @Test
    public void testDisallowIsolationLevel() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.setTransactionIsolation(Connection.TRANSACTION_NONE);
        conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        try {
            conn = DriverManager.getConnection(getUrl());
            conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            fail();
        } catch(SQLException e) {
            assertEquals(SQLExceptionCode.TX_MUST_BE_ENABLED_TO_SET_ISOLATION_LEVEL.getErrorCode(), e.getErrorCode());
        }
        try {
            conn = DriverManager.getConnection(getUrl());
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            fail();
        } catch(SQLFeatureNotSupportedException e) {
        }
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        props.setProperty(QueryServices.TRANSACTIONS_ENABLED, Boolean.toString(true));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        try {
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            fail();
        } catch(SQLFeatureNotSupportedException e) {
        }
    }

    @Test
    public void testInvalidURL() throws Exception {
      Class.forName(PhoenixDriver.class.getName());
      try {
      DriverManager.getConnection("any text whatever you want to put here");
      fail("Should have failed due to invalid driver");
      } catch(Exception e) {
      }
    }
}
