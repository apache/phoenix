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
package org.apache.phoenix.iterate;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.end2end.BaseHBaseManagedTimeTableReuseIT;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests to validate that user specified property phoenix.query.timeoutMs
 * works as expected.
 */
public class PhoenixQueryTimeoutIT extends BaseHBaseManagedTimeTableReuseIT {

    private static final String QUERY_TIMEOUT_TEST = generateRandomString();

    @Test
    /**
     * This test validates that we timeout as expected. It does do by
     * setting the timeout value to 1 ms.
     */
    public void testCustomQueryTimeoutWithVeryLowTimeout() throws Exception {
        // Arrange
        PreparedStatement ps = loadDataAndPrepareQuery(1, 1);
        
        // Act + Assert
        try {
            ResultSet rs = ps.executeQuery();
            // Trigger HBase scans by calling rs.next
            while (rs.next()) {};
            fail("Expected query to timeout with a 1 ms timeout");
        } catch (Exception e) {
            // Expected
        }
    }
    
    @Test
    public void testCustomQueryTimeoutWithNormalTimeout() throws Exception {
        // Arrange
        PreparedStatement ps = loadDataAndPrepareQuery(30000, 30);
        
        // Act + Assert
        try {
            ResultSet rs = ps.executeQuery();
            // Trigger HBase scans by calling rs.next
            int count = 0;
            while (rs.next()) {
                count++;
            }
            assertEquals("Unexpected number of records returned", 1000, count);
        } catch (Exception e) {
            fail("Expected query to suceed");
        }
    }

    
    //-----------------------------------------------------------------
    // Private Helper Methods
    //-----------------------------------------------------------------
    
    private PreparedStatement loadDataAndPrepareQuery(int timeoutMs, int timeoutSecs) throws Exception, SQLException {
        Properties props = new Properties();
        props.setProperty("phoenix.query.timeoutMs", String.valueOf(timeoutMs));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement ps = conn.prepareStatement("SELECT * FROM " + QUERY_TIMEOUT_TEST);
        PhoenixStatement phoenixStmt = ps.unwrap(PhoenixStatement.class);
        assertEquals(timeoutMs, phoenixStmt.getQueryTimeoutInMillis());
        assertEquals(timeoutSecs, phoenixStmt.getQueryTimeout());
        return ps;
    }

    @BeforeClass
    public static void createTableAndInsertRows() throws Exception {
        int numRows = 1000;
        String ddl =
            "CREATE TABLE " + QUERY_TIMEOUT_TEST + " (K VARCHAR NOT NULL PRIMARY KEY, V VARCHAR)";
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO " + QUERY_TIMEOUT_TEST + " VALUES (?, ?)";
        PreparedStatement stmt = conn.prepareStatement(dml);
        for (int i = 1; i <= numRows; i++) {
            String key = "key" + i;
            stmt.setString(1, key);
            stmt.setString(2, "value" + i);
            stmt.executeUpdate();
        }
        conn.commit();
    }
}
