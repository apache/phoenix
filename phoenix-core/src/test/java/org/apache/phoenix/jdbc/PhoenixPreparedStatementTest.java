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
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
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

}
