package org.apache.phoenix.end2end;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.coprocessor.tasks.ChildLinkScanTask;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.ConnectionQueryServicesImpl;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableAlreadyExistsException;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.phoenix.end2end.IndexRebuildTaskIT.waitForTaskState;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_FAMILY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;
import static org.apache.phoenix.query.QueryConstants.VERIFIED_BYTES;

@Category(NeedsOwnMiniClusterTest.class)
public class OrphanChildLinkRowsIT extends BaseTest {

    private static Map<String, String> expectedChildLinks = new HashMap<>();

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(QueryServices.CHILD_LINK_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB, "0");
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));

        // Create 2 tables - T1 and T2. Create a view V1 on T1.
        String t1 = "CREATE TABLE IF NOT EXISTS S1.T1 (TENANT_ID VARCHAR NOT NULL, A INTEGER NOT NULL, B INTEGER CONSTRAINT PK PRIMARY KEY (TENANT_ID, A))";
        String t2 = "CREATE TABLE IF NOT EXISTS S2.T2 (TENANT_ID VARCHAR NOT NULL, A INTEGER NOT NULL, B INTEGER CONSTRAINT PK PRIMARY KEY (TENANT_ID, A))";
        String v1 = "CREATE VIEW IF NOT EXISTS VS1.V1 (NEW_COL1 INTEGER, NEW_COL2 INTEGER) AS SELECT * FROM S1.T1 WHERE B > 10";

        try (Connection connection = DriverManager.getConnection(getUrl())) {
            connection.createStatement().execute(t1);
            connection.createStatement().execute(t2);
            connection.createStatement().execute(v1);
        }

        expectedChildLinks.put("S1.T1", "VS1.V1");
    }

    /**
     * 1. Disable the child link scan task.
     * 2. Create a view (same name as existing view on T1) on T2. This CREATE VIEW will fail, verify if there was no orphan child link because of that.
     *
     * 3. Instrument CQSI to fail phase three of CREATE VIEW. Create a new view V2 on T2 (passes) and V1 on T2 which will fail.
     *    Both links T2->V2 and T2->V1 will be in UNVERIFIED state, repaired during read.
     *    Check if only 2 child links are returned: T2->V2 and T1->V1.
     */
    @Test
    public void testNoOrphanChildLinkRow() throws Exception {

        ConnectionQueryServicesImpl.setFailPhaseThreeChildLinkWriteForTesting(false);
        ChildLinkScanTask.disableChildLinkScanTask(true);

        String v2 = "CREATE VIEW VS1.V1 (NEW_COL1 INTEGER, NEW_COL2 INTEGER) AS SELECT * FROM S2.T2 WHERE B > 10";

        try (Connection connection = DriverManager.getConnection(getUrl())) {
            connection.createStatement().execute(v2);
        }
        catch (TableAlreadyExistsException e) {
        }

        verifyNoOrphanChildLinkRow();

        // configure CQSI to fail the last write phase of CREATE VIEW
        // where child link mutations are set to VERIFIED or are deleted
        ConnectionQueryServicesImpl.setFailPhaseThreeChildLinkWriteForTesting(true);
        String v3 = "CREATE VIEW IF NOT EXISTS VS2.V2 (NEW_COL1 INTEGER, NEW_COL2 INTEGER) AS SELECT * FROM S2.T2 WHERE B > 10";

        try (Connection connection = DriverManager.getConnection(getUrl())) {
            connection.createStatement().execute(v3);
            connection.createStatement().execute(v2);
        }
        catch (TableAlreadyExistsException e) {
        }
        expectedChildLinks.put("S2.T2", "VS2.V2");
        verifyNoOrphanChildLinkRow();
    }

    /**
     * Enable child link scan task and configure CQSI to fail the last write phase of CREATE VIEW
     * Create a view (same name as existing view on T1) on T2.
     * Verify if all rows in HBase table are VERIFIED after Task finishes.
     */
    @Test
    public void testChildLinkScanTaskRepair() throws Exception {

        ConnectionQueryServicesImpl.setFailPhaseThreeChildLinkWriteForTesting(true);
        ChildLinkScanTask.disableChildLinkScanTask(false);

        String v2 = "CREATE VIEW VS1.V1 (NEW_COL1 INTEGER, NEW_COL2 INTEGER) AS SELECT * FROM S2.T2 WHERE B > 10";

        try (Connection connection = DriverManager.getConnection(getUrl())) {
            connection.createStatement().execute(v2);
        }
        catch (TableAlreadyExistsException e) {
        }

        try (Connection connection = DriverManager.getConnection(getUrl())) {
            // wait for TASK to complete
            waitForTaskState(connection, PTable.TaskType.CHILD_LINK_SCAN, SYSTEM_CHILD_LINK_TABLE, PTable.TaskStatus.COMPLETED);

            // scan the physical table and check there are no UNVERIFIED rows
            PTable childLinkPTable = PhoenixRuntime.getTable(connection, SYSTEM_CHILD_LINK_NAME);
            byte[] emptyCF = SchemaUtil.getEmptyColumnFamily(childLinkPTable);
            byte[] emptyCQ = EncodedColumnsUtil.getEmptyKeyValueInfo(childLinkPTable).getFirst();
            Scan scan = new Scan();
            HTable table = (HTable) connection.unwrap(PhoenixConnection.class).getQueryServices().getTable(TableName.valueOf(SYSTEM_CHILD_LINK_NAME).getName());
            ResultScanner results = table.getScanner(scan);
            Result result = results.next();
            while (result != null) {
                Assert.assertTrue("Found Child Link row with UNVERIFIED status", Arrays.equals(result.getValue(emptyCF, emptyCQ), VERIFIED_BYTES));
                result = results.next();
            }
        }
    }

    /**
     * Do 10 times: Create 2 tables and view with same name on both tables.
     * Check if LIMIT query on SYSTEM.CHILD_LINK returns the right number of rows
     */
    @Test
    public void testChildLinkQueryWithLimit() throws Exception {

        ConnectionQueryServicesImpl.setFailPhaseThreeChildLinkWriteForTesting(true);
        ChildLinkScanTask.disableChildLinkScanTask(true);

        String CREATE_TABLE_DDL = "CREATE TABLE %s (TENANT_ID VARCHAR NOT NULL, A INTEGER NOT NULL, B INTEGER CONSTRAINT PK PRIMARY KEY (TENANT_ID, A))";
        String CREATE_VIEW_DDL = "CREATE VIEW %s (NEW_COL1 INTEGER, NEW_COL2 INTEGER) AS SELECT * FROM %s WHERE B > 10";

        try (Connection connection = DriverManager.getConnection(getUrl())) {
            for (int i=0; i<10; i++) {
                String table1 = "T_" + generateUniqueName();
                String table2 = "T_" + generateUniqueName();
                String view = "V_" + generateUniqueName();
                connection.createStatement().execute(String.format(CREATE_TABLE_DDL, table1));
                connection.createStatement().execute(String.format(CREATE_TABLE_DDL, table2));
                connection.createStatement().execute(String.format(CREATE_VIEW_DDL, view, table1));
                expectedChildLinks.put(table1, view);
                try {
                    connection.createStatement().execute(String.format(CREATE_VIEW_DDL, view, table2));
                }
                catch (TableAlreadyExistsException e) {

                }
            }

            String childLinkQuery = "SELECT * FROM SYSTEM.CHILD_LINK LIMIT 7";
            ResultSet rs = connection.createStatement().executeQuery(childLinkQuery);
            int count = 0;
            while (rs.next()) {
                count++;
                System.out.println(rs.getString(3) + " " + rs.getString(5));
            }
            Assert.assertEquals("Incorrect number of child link rows returned", 7, count);
        }
    }

    private void verifyNoOrphanChildLinkRow() throws Exception {
        String childLinkQuery = "SELECT * FROM SYSTEM.CHILD_LINK";
        try (Connection connection = DriverManager.getConnection(getUrl())) {
            ResultSet rs = connection.createStatement().executeQuery(childLinkQuery);
            int count = 0;
            while (rs.next()) {
                String parentFullName = SchemaUtil.getTableName(rs.getString(TABLE_SCHEM), rs.getString(TABLE_NAME));
                Assert.assertTrue("Found Orphan Child Link: " + parentFullName + "->" + rs.getString(COLUMN_FAMILY), expectedChildLinks.containsKey(parentFullName));
                Assert.assertEquals(String.format("Child was not correct in Child Link. Expected : %s, Actual: %s", expectedChildLinks.get(parentFullName), rs.getString(COLUMN_FAMILY)),
                        expectedChildLinks.get(parentFullName), rs.getString(COLUMN_FAMILY));
                count++;
            }
            Assert.assertTrue("Found Orphan Linking Row", count <= expectedChildLinks.size());
            Assert.assertTrue("All expected Child Links not returned by query", count >= expectedChildLinks.size());
        }
    }


}
