package org.apache.phoenix.rpc;

import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.AfterClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.DEFAULT_SCHEMA_NAME;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.*;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.*;

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

    // Test for connection-level UCF set, table-level UCF not set
    @Test
    public void testWithConnLevelUCFNoTableLevelUCF() throws Exception {
        long tableUpdateCacheFrequency = -1;
        long connUpdateCacheFrequency = 1000;
        String fullTableName = DEFAULT_SCHEMA_NAME + QueryConstants.NAME_SEPARATOR + generateUniqueName();

        setUpTableAndConnections(fullTableName, tableUpdateCacheFrequency, connUpdateCacheFrequency);

        // There should only be a single call to getTable() for fetching the table's metadata
        int numSelectExecutions = 2;
        int numExpectedGetTableCalls = 1;
        verifyExpectedGetTableCalls(fullTableName, numSelectExecutions, numExpectedGetTableCalls);

        // Wait for a period of 'connUpdateCacheFrequency' and verify that there was one new call to getTable() for
        // fetching the table's metadata
        Thread.sleep(connUpdateCacheFrequency);
        verifyExpectedGetTableCalls(fullTableName, numSelectExecutions, numExpectedGetTableCalls);
    }

    // Test for table-level UCF set, connection-level UCF not set
    @Test
    public void testWithTableLevelUCFNoConnLevelUCF() throws Exception {
        long tableUpdateCacheFrequency = 1000;
        long connUpdateCacheFrequency = -1;
        String fullTableName = DEFAULT_SCHEMA_NAME + QueryConstants.NAME_SEPARATOR + generateUniqueName();

        // There should only be a single call to getTable() for fetching the table's metadata
        int numSelectExecutions = 2;
        int numExpectedGetTableCalls = 1;
        setUpTableAndConnections(fullTableName, tableUpdateCacheFrequency, connUpdateCacheFrequency);
        verifyExpectedGetTableCalls(fullTableName, numSelectExecutions, numExpectedGetTableCalls);

        // Wait for a period of 'tableUpdateCacheFrequency' and verify that there was one new call to getTable() for
        // fetching the table's metadata
        Thread.sleep(tableUpdateCacheFrequency);
        verifyExpectedGetTableCalls(fullTableName, numSelectExecutions, numExpectedGetTableCalls);
    }

    // Test for both connection-level and table-level UCF not set
    @Test
    public void testWithNoConnAndTableLevelUCF() throws Exception {
        long tableUpdateCacheFrequency = -1;
        long connUpdateCacheFrequency = -1;
        String fullTableName = DEFAULT_SCHEMA_NAME + QueryConstants.NAME_SEPARATOR + generateUniqueName();

        // This is the default behavior (i.e. always fetch the latest metadata of the table) when both connection and
        // table level properties are not set
        int numSelectExecutions = 2;
        int numExpectedGetTableCalls = 2;
        setUpTableAndConnections(fullTableName, tableUpdateCacheFrequency, connUpdateCacheFrequency);
        verifyExpectedGetTableCalls(fullTableName, numSelectExecutions, numExpectedGetTableCalls);
    }

    // Test for both connection-level and table-level UCF set
    @Test
    public void testWithBothConnAndTableLevelUCF() throws Exception {
        // Set table level property to a much higher value than the connection level property
        long tableUpdateCacheFrequency = 5000;
        long connUpdateCacheFrequency = 1000;
        String fullTableName = DEFAULT_SCHEMA_NAME + QueryConstants.NAME_SEPARATOR + generateUniqueName();

        // There should only be a single call to getTable() for fetching the table's metadata
        int numSelectExecutions = 2;
        int numExpectedGetTableCalls = 1;
        setUpTableAndConnections(fullTableName, tableUpdateCacheFrequency, connUpdateCacheFrequency);
        verifyExpectedGetTableCalls(fullTableName, numSelectExecutions, numExpectedGetTableCalls);

        // Wait for a period of 'connUpdateCacheFrequency' and verify that there were no new calls to getTable() as the
        // table level UCF should come in to effect
        Thread.sleep(connUpdateCacheFrequency);
        numExpectedGetTableCalls = 0;
        verifyExpectedGetTableCalls(fullTableName, numSelectExecutions, numExpectedGetTableCalls);

        // Extend the wait to a period of 'tableUpdateCacheFrequency' and verify that there was one new call to
        // getTable() for fetching the table's metadata
        Thread.sleep(tableUpdateCacheFrequency - connUpdateCacheFrequency);
        numExpectedGetTableCalls = 1;
        verifyExpectedGetTableCalls(fullTableName, numSelectExecutions, numExpectedGetTableCalls);
    }

    // Helper method that sets up the connections and creates the table
    private static void setUpTableAndConnections(String fullTableName, long tableUpdateCacheFrequency, long connUpdateCacheFrequency) throws SQLException {
        // Create two connections - a connection that we'll use to create the table and the second one that we will
        // spy on and will have 'phoenix.default.update.cache.frequency' set
        Properties props = new Properties();
        conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.setAutoCommit(true);

        Properties propsWithUCF = new Properties();
        //if (connUpdateCacheFrequency != -1) {
            propsWithUCF.put(QueryServices.DEFAULT_UPDATE_CACHE_FREQUENCY_ATRRIB, "" + connUpdateCacheFrequency);

        // use a spied ConnectionQueryServices so we can verify calls to getTable()
        spyForConn2 = Mockito.spy(driver.getConnectionQueryServices(getUrl(), connUpdateCacheFrequency != -1 ? propsWithUCF : props));
        conn2 = spyForConn2.connect(getUrl(), propsWithUCF);
        conn2.setAutoCommit(true);

        String createTableQuery =
                "CREATE TABLE " + fullTableName + " (k UNSIGNED_DOUBLE NOT NULL PRIMARY KEY, "
                        + "v1 UNSIGNED_DOUBLE, v2 UNSIGNED_DOUBLE, v3 UNSIGNED_DOUBLE)";

        if (tableUpdateCacheFrequency != -1) {
            createTableQuery += " UPDATE_CACHE_FREQUENCY = " + tableUpdateCacheFrequency;
        }

        // Create the table over first connection
        conn1.createStatement().execute(createTableQuery);
        conn1.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES (1, 2, 3, 4)");
        conn1.commit();
    }

    // Helper method that executes a select query on the table for numSelectExecutions times and verifies that
    // numExpectedGetTableCalls were made to getTable for the table
    private static void verifyExpectedGetTableCalls(String fullTableName, int numSelectExecutions, int numExpectedGetTableCalls) throws SQLException {
        String tableName = SchemaUtil.getTableNameFromFullName(fullTableName);
        String schemaName = SchemaUtil.getSchemaNameFromFullName(fullTableName);
        String selectFromTableQuery = "SELECT k, v1, v2, v3 FROM " + fullTableName;

        for (int i = 0; i < numSelectExecutions; i++) {
            // Query the table over the spied connection that has update cache frequency set
            ResultSet rs = conn2.createStatement().executeQuery(selectFromTableQuery);
            assertTrue(rs.next());
        }

        // Ensure that getTable() was called only once for our table
        verify(spyForConn2, times(numExpectedGetTableCalls)).getTable((PName) isNull(),
                eq(PVarchar.INSTANCE.toBytes(schemaName)), eq(PVarchar.INSTANCE.toBytes(tableName)),
                anyLong(), anyLong());
        reset(spyForConn2);
    }
}
