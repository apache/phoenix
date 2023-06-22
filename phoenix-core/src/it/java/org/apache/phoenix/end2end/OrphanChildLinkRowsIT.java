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
package org.apache.phoenix.end2end;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.coprocessor.tasks.ChildLinkScanTask;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.BaseTest;
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME;
import static org.apache.phoenix.query.QueryConstants.VERIFIED_BYTES;

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class OrphanChildLinkRowsIT extends BaseTest {

    private final boolean pagingEnabled;
    private final String CREATE_TABLE_DDL = "CREATE TABLE %s (TENANT_ID VARCHAR NOT NULL, A INTEGER NOT NULL, B INTEGER CONSTRAINT PK PRIMARY KEY (TENANT_ID, A))";
    private final String CREATE_VIEW_DDL = "CREATE VIEW %s (NEW_COL1 INTEGER, NEW_COL2 INTEGER) AS SELECT * FROM %s WHERE B > 10";

    public OrphanChildLinkRowsIT(boolean pagingEnabled) {
        this.pagingEnabled = pagingEnabled;
    }

    @Parameterized.Parameters(name="OrphanChildLinkRowsIT_pagingEnabled={0}")
    public static synchronized Collection<Boolean> data() {
        return Arrays.asList(false, true);
    }

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(QueryServices.CHILD_LINK_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB, "0");
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    /**
     * Disable the child link scan task.
     * Create tables T1 and T2. Create 3 views on T1.
     * Create a view (same name as existing view on T1) on T2. This CREATE VIEW will fail.
     * Verify if there was no orphan child link from T2.
     *
     * Instrument CQSI to fail phase three of CREATE VIEW. Create a new view V4 on T2 (passes) and V1 on T2 which will fail.
     * Both links T2->V4 and T2->V1 will be in UNVERIFIED state, repaired during read.
     * Check if T2 has only 1 child link.
     */
    @Test
    public void testNoOrphanChildLinkRow() throws Exception {

        ConnectionQueryServicesTestImpl.setFailPhaseThreeChildLinkWriteForTesting(false);
        ChildLinkScanTask.disableChildLinkScanTask(true);

        String tableName1 = "T_" + generateUniqueName();
        String tableName2 = "T_" + generateUniqueName();
        String sameViewName = "V_"+generateUniqueName();

        try (Connection connection = DriverManager.getConnection(getUrl())) {
            connection.createStatement().execute(String.format(CREATE_TABLE_DDL, tableName1));
            connection.createStatement().execute(String.format(CREATE_TABLE_DDL, tableName2));

            //create 3 views in the first table
            connection.createStatement().execute(String.format(CREATE_VIEW_DDL, sameViewName, tableName1));
            for (int i=0; i<2; i++) {
                connection.createStatement().execute(String.format(CREATE_VIEW_DDL, "V_"+generateUniqueName(), tableName1));
            }

            connection.createStatement().execute(String.format(CREATE_VIEW_DDL, sameViewName, tableName2));
            Assert.fail("Exception should have been thrown when creating a view with same name on a different table.");
        }
        catch (TableAlreadyExistsException e) {
            //expected since we are creating a view with the same name as an existing view
        }

        verifyNoOrphanChildLinkRow(tableName1, 3);
        verifyNoOrphanChildLinkRow(tableName2, 0);

        // configure CQSI to fail the last write phase of CREATE VIEW
        // where child link mutations are set to VERIFIED or are deleted
        ConnectionQueryServicesTestImpl.setFailPhaseThreeChildLinkWriteForTesting(true);

        try (Connection connection = DriverManager.getConnection(getUrl())) {
            connection.createStatement().execute(String.format(CREATE_VIEW_DDL, "V_"+generateUniqueName(), tableName2));

            connection.createStatement().execute(String.format(CREATE_VIEW_DDL, sameViewName, tableName2));
            Assert.fail("Exception should have been thrown when creating a view with same name on a different table.");
        }
        catch (TableAlreadyExistsException e) {
            //expected since we are creating a view with the same name as an existing view
        }
        verifyNoOrphanChildLinkRow(tableName1, 3);
        verifyNoOrphanChildLinkRow(tableName2, 1);
    }

    /**
     * Enable child link scan task and configure CQSI to fail the last write phase of CREATE VIEW
     * Create 2 tables X and Y.
     * Create a view (same name as existing view on table X) on table Y.
     * Verify if all rows in HBase table are VERIFIED after Task finishes.
     */
    @Test
    public void testChildLinkScanTaskRepair() throws Exception {

        ConnectionQueryServicesTestImpl.setFailPhaseThreeChildLinkWriteForTesting(true);
        ChildLinkScanTask.disableChildLinkScanTask(false);

        String tableName1 = "T_" + generateUniqueName();
        String tableName2 = "T_" + generateUniqueName();
        String sameViewName = "V_"+generateUniqueName();

        try (Connection connection = DriverManager.getConnection(getUrl())) {
            connection.createStatement().execute(String.format(CREATE_TABLE_DDL, tableName1));
            connection.createStatement().execute(String.format(CREATE_TABLE_DDL, tableName2));
            connection.createStatement().execute(String.format(CREATE_VIEW_DDL, sameViewName, tableName1));

            connection.createStatement().execute(String.format(CREATE_VIEW_DDL, sameViewName, tableName2));
            Assert.fail("Exception should have been thrown when creating a view with same name on a different table.");
        }
        catch (TableAlreadyExistsException e) {
            //expected since we are creating a view with the same name as an existing view
        }

        try (Connection connection = DriverManager.getConnection(getUrl())) {
            // wait for TASKs to complete
            waitForChildLinkScanTasks();

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
     * Check if only one child link is returned for every table.
     */
    @Test
    public void testChildLinkQueryWithLimit() throws Exception {

        ConnectionQueryServicesTestImpl.setFailPhaseThreeChildLinkWriteForTesting(true);
        ChildLinkScanTask.disableChildLinkScanTask(true);

        Properties props = new Properties();
        if (pagingEnabled) {
            props.put(QueryServices.PHOENIX_SERVER_PAGE_SIZE_MS, "0");
        }
        Map<String, String> expectedChildLinks = new HashMap<>();
        try (Connection connection = DriverManager.getConnection(getUrl(), props)) {
            for (int i=0; i<7; i++) {
                String table1 = "T_" + generateUniqueName();
                String table2 = "T_" + generateUniqueName();
                String view = "V_" + generateUniqueName();
                connection.createStatement().execute(String.format(CREATE_TABLE_DDL, table1));
                connection.createStatement().execute(String.format(CREATE_TABLE_DDL, table2));
                connection.createStatement().execute(String.format(CREATE_VIEW_DDL, view, table1));
                expectedChildLinks.put(table1, view);
                try {
                    connection.createStatement().execute(String.format(CREATE_VIEW_DDL, view, table2));
                    Assert.fail("Exception should have been thrown when creating a view with same name on a different table.");
                }
                catch (TableAlreadyExistsException e) {

                }
            }

            String childLinkQuery = "SELECT * FROM SYSTEM.CHILD_LINK LIMIT 5";
            ResultSet rs = connection.createStatement().executeQuery(childLinkQuery);
            int count = 0;
            while (rs.next()) {
                count++;
            }
            Assert.assertEquals("Incorrect number of child link rows returned", 5, count);
        }
        for (String table : expectedChildLinks.keySet()) {
            verifyNoOrphanChildLinkRow(table, 1);
        }
    }

    private void verifyNoOrphanChildLinkRow(String table, int numExpectedChildLinks) throws Exception {
        String childLinkQuery = "SELECT * FROM SYSTEM.CHILD_LINK";
        if (table != null) {
            childLinkQuery += (" WHERE TABLE_NAME=" + "'" + table + "'");
        }

        int count = 0;
        Properties props = new Properties();
        if (pagingEnabled) {
            props.put(QueryServices.PHOENIX_SERVER_PAGE_SIZE_MS, "0");
        }
        try (Connection connection = DriverManager.getConnection(getUrl(), props)) {
            ResultSet rs = connection.createStatement().executeQuery(childLinkQuery);
            while (rs.next()) {
                count++;
            }
        }
        Assert.assertTrue("Found Orphan Linking Row", count <= numExpectedChildLinks);
        Assert.assertTrue("All expected Child Links not returned by query", count >= numExpectedChildLinks);
    }

    /*
    Wait for all child link scan tasks to finish.
     */
    public static void waitForChildLinkScanTasks() throws Exception {
        int maxTries = 10, nTries=0;
        int sleepIntervalMs = 2000;
        try (Connection connection = DriverManager.getConnection(getUrl())) {
            do {
                Thread.sleep(sleepIntervalMs);
                ResultSet rs = connection.createStatement().executeQuery("SELECT COUNT(*) FROM SYSTEM.TASK WHERE " +
                        PhoenixDatabaseMetaData.TASK_TYPE + " = " + PTable.TaskType.CHILD_LINK_SCAN.getSerializedValue() +
                        " AND " + PhoenixDatabaseMetaData.TASK_STATUS + " != 'COMPLETED'");
                rs.next();
                int numPendingTasks = rs.getInt(1);
                if (numPendingTasks == 0) break;
            } while(++nTries < maxTries);
        }
    }
}
