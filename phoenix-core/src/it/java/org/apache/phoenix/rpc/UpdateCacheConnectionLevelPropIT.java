/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.rpc;

import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.SchemaUtil;
import static org.apache.phoenix.util.TestUtil.DEFAULT_SCHEMA_NAME;

import org.junit.AfterClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.mockito.Mockito;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests for connection level 'Update Cache Frequency' property.
 *
 * The tests verify that the 'Update Cache Frequency' is honored in the following precedence order
 * for SELECTs and UPSERTs:
 * Table-level property > Connection-level property > Default value
 */
@Category(ParallelStatsDisabledTest.class)
public class UpdateCacheConnectionLevelPropIT extends ParallelStatsDisabledIT {

    private static Connection conn1;
    private static Connection conn2;
    private static ConnectionQueryServices spyForConn2;

    @AfterClass
    public static synchronized void freeResources() {
        try {
            conn1.close();
            conn2.close();
        } catch (Exception e) {
            /* ignored */
        }
    }

    /**
     * Test 'Update Cache Frequency' property when it is set at connection-level only, and not at
     * table-level.
     */
    @Test
    public void testWithConnLevelUCFNoTableLevelUCF() throws Exception {
        final long connUpdateCacheFrequency = 1000;
        String fullTableName = DEFAULT_SCHEMA_NAME + QueryConstants.NAME_SEPARATOR +
                generateUniqueName();

        setUpTableAndConnections(fullTableName, null,
                String.valueOf(connUpdateCacheFrequency));

        // There should only be no call to getTable() for fetching the table's metadata since
        // it will be in the CQSI cache
        int numExecutions = 2;
        int numExpectedGetTableCalls = 0;
        verifyExpectedGetTableCalls(fullTableName, numExecutions, numExpectedGetTableCalls);

        // Wait for a period of 'connUpdateCacheFrequency' and verify that there was one new call to
        // getTable() for fetching the table's metadata
        Thread.sleep(connUpdateCacheFrequency);
        numExpectedGetTableCalls = 1;
        verifyExpectedGetTableCalls(fullTableName, numExecutions, numExpectedGetTableCalls);
    }

    /**
     * Test 'Update Cache Frequency' property when it is set at table-level only, and not at
     * connection-level.
     */
    @Test
    public void testWithTableLevelUCFNoConnLevelUCF() throws Exception {
        final long tableUpdateCacheFrequency = 1000;
        String fullTableName = DEFAULT_SCHEMA_NAME + QueryConstants.NAME_SEPARATOR +
                generateUniqueName();

        // There should only be no call to getTable() for fetching the table's metadata since
        // it will be in the CQSI cache
        int numExecutions = 2;
        int numExpectedGetTableCalls = 0;
        setUpTableAndConnections(fullTableName, String.valueOf(tableUpdateCacheFrequency), null);
        verifyExpectedGetTableCalls(fullTableName, numExecutions, numExpectedGetTableCalls);

        // Wait for a period of 'tableUpdateCacheFrequency' and verify that there was one new call
        // to getTable() for fetching the table's metadata
        Thread.sleep(tableUpdateCacheFrequency);
        numExpectedGetTableCalls = 1;
        verifyExpectedGetTableCalls(fullTableName, numExecutions, numExpectedGetTableCalls);
    }

    /**
     * Test 'Update Cache Frequency' property when it is not set at both table-level and
     * connection-level.
     */
    @Test
    public void testWithNoConnAndTableLevelUCF() throws Exception {
        String fullTableName = DEFAULT_SCHEMA_NAME + QueryConstants.NAME_SEPARATOR +
                generateUniqueName();

        // This is the default behavior (i.e. always fetch the latest metadata of the table) when
        // both connection and table level properties are not set
        int numExecutions = 2;
        int numExpectedGetTableCalls = 4; // 2 for SELECTs, and 2 for UPSERTs
        setUpTableAndConnections(fullTableName, null, null);
        verifyExpectedGetTableCalls(fullTableName, numExecutions, numExpectedGetTableCalls);
    }

    /**
     * Test 'Update Cache Frequency' property when it is set at both table-level and
     * connection-level.
     */
    @Test
    public void testWithBothConnAndTableLevelUCF() throws Exception {
        // Set table level property to a much higher value than the connection level property
        final long tableUpdateCacheFrequency = 5000;
        final long connUpdateCacheFrequency = 1000;
        String fullTableName = DEFAULT_SCHEMA_NAME + QueryConstants.NAME_SEPARATOR +
                generateUniqueName();

        // There should only be no call to getTable() for fetching the table's metadata since
        // it will be in the CQSI cache
        int numExecutions = 2;
        int numExpectedGetTableCalls = 0;
        setUpTableAndConnections(fullTableName, String.valueOf(tableUpdateCacheFrequency),
                String.valueOf(connUpdateCacheFrequency));
        verifyExpectedGetTableCalls(fullTableName, numExecutions, numExpectedGetTableCalls);

        // Wait for a period of 'connUpdateCacheFrequency' and verify that there were no new calls
        // to getTable() as the table level UCF should come in to effect
        Thread.sleep(connUpdateCacheFrequency);
        numExpectedGetTableCalls = 0;
        verifyExpectedGetTableCalls(fullTableName, numExecutions, numExpectedGetTableCalls);

        // Extend the wait to a period of 'tableUpdateCacheFrequency' and verify that there was one
        // new call to getTable() for fetching the table's metadata
        Thread.sleep(tableUpdateCacheFrequency - connUpdateCacheFrequency);
        numExpectedGetTableCalls = 1;
        verifyExpectedGetTableCalls(fullTableName, numExecutions, numExpectedGetTableCalls);
    }

    /**
     * Helper method that sets up the connections and creates the table to be tested.
     * @param fullTableName The table's full name
     * @param tableUpdateCacheFrequency If not null, the table-level value to be set for 'Update
     *                                  Cache Frequency'
     * @param connUpdateCacheFrequency If not null, the connection-level value to be set for 'Update
     *                                 Cache Frequency'
     */
    private static void setUpTableAndConnections(String fullTableName,
            String tableUpdateCacheFrequency, String connUpdateCacheFrequency) throws SQLException {
        // Create two connections - a connection that we'll use to create the table and the second
        // one that we will spy on and use to query the table.
        Properties props = new Properties();
        conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.setAutoCommit(true);

        if (connUpdateCacheFrequency != null) {
            props.put(QueryServices.DEFAULT_UPDATE_CACHE_FREQUENCY_ATRRIB,
                    connUpdateCacheFrequency);
        }

        // use a spied ConnectionQueryServices so we can verify calls to getTable()
        spyForConn2 = Mockito.spy(driver.getConnectionQueryServices(getUrl(), props));
        conn2 = spyForConn2.connect(getUrl(), props);
        conn2.setAutoCommit(true);

        String createTableQuery =
                "CREATE TABLE " + fullTableName + " (k UNSIGNED_DOUBLE NOT NULL PRIMARY KEY, "
                        + "v1 UNSIGNED_DOUBLE, v2 UNSIGNED_DOUBLE, v3 UNSIGNED_DOUBLE)";

        if (tableUpdateCacheFrequency != null) {
            createTableQuery += " UPDATE_CACHE_FREQUENCY = " + tableUpdateCacheFrequency;
        }

        // Create the table over first connection
        try (Statement stmt = conn1.createStatement()) {
            stmt.execute(createTableQuery);
            stmt.execute("UPSERT INTO " + fullTableName + " VALUES (1, 2, 3, 4)");
        }
        conn1.commit();
    }

    /**
     * Helper method that executes a select and upsert query on the table for \p numSelectExecutions
     * times and verifies that \p numExpectedGetTableCalls were made to getTable() for the table.
     *
     * Also resets the spy object for conn2 before returning.
     *
     * @param fullTableName The table's full name
     * @param numExecutions Number of times a select+upsert should be executed on the table
     * @param numExpectedGetTableCalls Number of expected calls to getTable()
     */
    private static void verifyExpectedGetTableCalls(String fullTableName, int numExecutions,
            int numExpectedGetTableCalls) throws SQLException {
        String tableName = SchemaUtil.getTableNameFromFullName(fullTableName);
        String schemaName = SchemaUtil.getSchemaNameFromFullName(fullTableName);
        String selectFromTableQuery = "SELECT k, v1, v2, v3 FROM " + fullTableName;

        for (int i = 0; i < numExecutions; i++) {
            // Query the table over the spied connection that has update cache frequency set
            try (Statement stmt = conn2.createStatement();
                    ResultSet rs = stmt.executeQuery(selectFromTableQuery)) {
                assertTrue(rs.next());
                stmt.execute("UPSERT INTO " + fullTableName + " VALUES (1, 2, 3, 4)");
            }
        }

        // Verify number of calls to getTable() for our table
        verify(spyForConn2, times(numExpectedGetTableCalls)).getTable((PName) isNull(),
                eq(PVarchar.INSTANCE.toBytes(schemaName)), eq(PVarchar.INSTANCE.toBytes(tableName)),
                anyLong(), anyLong());
        reset(spyForConn2);
    }
}
