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

import static org.apache.phoenix.exception.SQLExceptionCode.OPERATION_TIMED_OUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.Properties;

import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests to validate that user specified property phoenix.query.timeoutMs
 * works as expected.
 */
@Category(ParallelStatsDisabledTest.class)
public class PhoenixQueryTimeoutIT extends ParallelStatsDisabledIT {

    private String tableName;

    @Before
    public void createTableAndInsertRows() throws Exception {
        tableName = generateUniqueName();
        int numRows = 1000;
        String ddl =
            "CREATE TABLE " + tableName + " (K INTEGER NOT NULL PRIMARY KEY, V VARCHAR)";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            String dml = "UPSERT INTO " + tableName + " VALUES (?, ?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            for (int i = 1; i <= numRows; i++) {
                stmt.setInt(1, i);
                stmt.setString(2, "value" + i);
                stmt.executeUpdate();
            }
            conn.commit();
        }
    }

    /**
     * This test validates that we timeout as expected. It does do by
     * setting the timeout value to 1 ms.
     */
    @Test
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
            fail("Expected query to succeed");
        }
    }

    @Test
    public void testScanningResultIteratorQueryTimeoutForPagingWithVeryLowTimeout() throws Exception {
        //Arrange
        PreparedStatement ps = loadDataAndPreparePagedQuery(1,1);

        //Act + Assert
        try {
            //Do not let BaseResultIterators throw Timeout Exception Let ScanningResultIterator handle it.
            BaseResultIterators.setForTestingSetTimeoutToMaxToLetQueryPassHere(true);
            ResultSet rs = ps.executeQuery();
            while(rs.next()) {}
            fail("Expected query to timeout with a 1 ms timeout");
        } catch (SQLException e) {
            //OPERATION_TIMED_OUT Exception expected
            Throwable t = e;
            // SQLTimeoutException can be wrapped inside outer exceptions like PhoenixIOException
            while (t != null && !(t instanceof SQLTimeoutException)) {
                t = t.getCause();
            }
            if (t == null) {
                fail("Expected query to fail with SQLTimeoutException");
            }
            assertEquals(OPERATION_TIMED_OUT.getErrorCode(),
                    ((SQLTimeoutException)t).getErrorCode());
        } finally {
            BaseResultIterators.setForTestingSetTimeoutToMaxToLetQueryPassHere(false);
        }
    }

    @Test
    public void testScanningResultIteratorQueryTimeoutForPagingWithNormalLowTimeout() throws Exception {
        //Arrange
        PreparedStatement ps = loadDataAndPreparePagedQuery(30000,30);

        //Act + Assert
        try {
            //Do not let BaseResultIterators throw Timeout Exception Let ScanningResultIterator handle it.
            BaseResultIterators.setForTestingSetTimeoutToMaxToLetQueryPassHere(true);
            ResultSet rs = ps.executeQuery();
            int count = 0;
            while(rs.next()) {
                count++;
            }
            assertEquals("Unexpected number of records returned", 500, count);
        } catch (SQLException e) {
            fail("Expected query to succeed");
        } finally {
            BaseResultIterators.setForTestingSetTimeoutToMaxToLetQueryPassHere(false);
        }
    }
    
    //-----------------------------------------------------------------
    // Private Helper Methods
    //-----------------------------------------------------------------
    
    private PreparedStatement loadDataAndPrepareQuery(int timeoutMs, int timeoutSecs) throws Exception, SQLException {
        Properties props = new Properties();
        props.setProperty(QueryServices.THREAD_TIMEOUT_MS_ATTRIB, String.valueOf(timeoutMs));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement ps = conn.prepareStatement("SELECT * FROM " + tableName);
        PhoenixStatement phoenixStmt = ps.unwrap(PhoenixStatement.class);
        assertEquals(timeoutMs, phoenixStmt.getQueryTimeoutInMillis());
        assertEquals(timeoutSecs, phoenixStmt.getQueryTimeout());
        return ps;
    }

    private PreparedStatement loadDataAndPreparePagedQuery(int timeoutMs, int timeoutSecs) throws Exception {
        Properties props = new Properties();
        //Setting Paging Size to 0 and Query Timeout to 1ms so that query get paged quickly and times out immediately
        props.setProperty(QueryServices.THREAD_TIMEOUT_MS_ATTRIB, String.valueOf(timeoutMs));
        props.setProperty(QueryServices.PHOENIX_SERVER_PAGE_SIZE_MS, Integer.toString(0));
        PhoenixConnection conn = DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class);
        PreparedStatement ps = conn.prepareStatement("SELECT * FROM " + tableName + " WHERE K % 2 = 0");
        PhoenixStatement phoenixStmt = ps.unwrap(PhoenixStatement.class);
        assertEquals(timeoutMs, phoenixStmt.getQueryTimeoutInMillis());
        assertEquals(timeoutSecs, phoenixStmt.getQueryTimeout());
        assertEquals(0, conn.getQueryServices().getProps().getInt(QueryServices.PHOENIX_SERVER_PAGE_SIZE_MS, -1));
        return ps;
    }
}
