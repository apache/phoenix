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

import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.fail;

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

}
