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

import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.phoenix.hbase.index.Indexer;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.StaleRegionBoundaryCacheException;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class SplitIT extends BaseUniqueNamesOwnClusterIT {
    private static final String SPLIT_TABLE_NAME_PREFIX = "SPLIT_TABLE_";
    private static boolean tableWasSplitDuringScannerNext = false;
    private static byte[] splitPoint = null;

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(1);
        serverProps.put("hbase.coprocessor.region.classes", TestRegionObserver.class.getName());
        serverProps.put(Indexer.CHECK_VERSION_CONF_KEY, "false");
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(3);
        clientProps.put(QueryServices.MUTATE_BATCH_SIZE_ATTRIB, Integer.toString(10));
        // read rows in batches 3 at time
        clientProps.put(QueryServices.SCAN_CACHE_SIZE_ATTRIB, Integer.toString(3));
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()), new ReadOnlyProps(clientProps.entrySet().iterator()));
    }

    public static class TestRegionObserver extends BaseRegionObserver {

        @Override
        public boolean postScannerNext(final ObserverContext<RegionCoprocessorEnvironment> c,
                                       final InternalScanner s, final List<Result> results, final int limit,
                                       final boolean hasMore) throws IOException {
            Region region = c.getEnvironment().getRegion();
            String tableName = region.getRegionInfo().getTable().getNameAsString();
            if (tableName.startsWith(SPLIT_TABLE_NAME_PREFIX) && results.size()>1) {
                int pk = (Integer)PInteger.INSTANCE.toObject(results.get(0).getRow());
                // split when row 10 is read
                if (pk==10 && !tableWasSplitDuringScannerNext) {
                    try {
                        // split on the first row being scanned if splitPoint is null
                        splitPoint = splitPoint!=null ? splitPoint : results.get(0).getRow();
                        splitTable(splitPoint, tableName);
                        tableWasSplitDuringScannerNext = true;
                    }
                    catch (SQLException e) {
                        throw new IOException(e);
                    }
                }
            }
            return hasMore;
        }

    }

    public static void splitTable(byte[] splitPoint, String tableName) throws SQLException, IOException {
        HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
        int nRegions = admin.getTableRegions(tableName.getBytes()).size();
        int nInitialRegions = nRegions;
        admin.split(tableName.getBytes(), splitPoint);
        admin.disableTable(tableName);
        admin.enableTable(tableName);
        nRegions = admin.getTableRegions(tableName.getBytes()).size();
        if (nRegions == nInitialRegions)
            throw new IOException("Could not split for " + tableName);
    }

    /**
     * Runs an UPSERT SELECT into the same table while the table is split
     */
    public void helpTestUpsertSelectWithSplit(boolean splitTableBeforeUpsertSelect) throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.setAutoCommit(true);
        String keys = generateUniqueName();
        conn.createStatement().execute("CREATE SEQUENCE " + keys + " CACHE 1000");
        String tableName = SPLIT_TABLE_NAME_PREFIX + generateUniqueName();
        conn.createStatement().execute(
                "CREATE TABLE " + tableName + " (pk INTEGER PRIMARY KEY, val INTEGER)");

        conn.createStatement().execute(
                "UPSERT INTO " + tableName + " VALUES (NEXT VALUE FOR " + keys + ",1)");
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " SELECT NEXT VALUE FOR " + keys + ", pk FROM " + tableName);
        for (int i=0; i<7; i++) {
            if (splitTableBeforeUpsertSelect) {
                // split the table and then run the UPSERT SELECT
                splitTable(PInteger.INSTANCE.toBytes(Math.pow(2, i)), tableName);
            }
            int upsertCount = stmt.executeUpdate();
            assertEquals((int) Math.pow(2, i), upsertCount);
        }
        conn.close();
    }

    /**
     * Runs SELECT to a table that is being written to while a SPLIT happens
     */
    public void helpTestSelectWithSplit(boolean splitTableBeforeSelect, boolean orderBy, boolean limit) throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.setAutoCommit(true);
        String tableName = SPLIT_TABLE_NAME_PREFIX + generateUniqueName();
        int pk = 1;
        conn.createStatement().execute(
                "CREATE TABLE " + tableName + " (pk INTEGER PRIMARY KEY, val INTEGER)");

        conn.createStatement().execute(
                "UPSERT INTO " + tableName + " VALUES (1,1)");
        PreparedStatement stmt = conn.prepareStatement(" UPSERT INTO " + tableName + " VALUES (?,?) ");
        for (int i=0; i<5; i++) {
            if (splitTableBeforeSelect) {
                // split the table and then run the SELECT
                splitTable(PInteger.INSTANCE.toBytes(Math.pow(2, i)), tableName);
            }

            int count = 0;
            while (count<2) {
                String query = "SELECT * FROM " + tableName + (orderBy ? " ORDER BY val" : "") + (limit ? " LIMIT 32" : "");
                try {
                    ResultSet rs = conn.createStatement().executeQuery(query);
                    while (rs.next()) {
                        stmt.setInt(1, ++pk);
                        stmt.setInt(2, rs.getInt(1));
                        stmt.execute();
                    }
                    break;
                } catch (StaleRegionBoundaryCacheException e) {
                    if (!orderBy)
                        fail("Simple selects should not check for splits, they let HBase restart the scan");
                    if (count>0)
                        throw e;
                    ++count;
                }
            }

            ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(1) FROM " + tableName);
            assertTrue(rs.next());
            int rowCount = rs.getInt(1);
            assertFalse(rs.next());

            // for ORDER BY a StaleRegionBoundaryException is thrown when a sp[it happens
            if (orderBy) {
                // if the table splits before the SELECT we always detect this so we never see rows written after the scan started
                if (splitTableBeforeSelect)
                    assertEquals((int) Math.pow(2, i + 1), rowCount);
                // else we see rows written after the SELECT started
                else if (i == 4) {
                    assert ((int) Math.pow(2, i + 1) < rowCount);
                }
            }
            // verify that we will see more rows written after the scan started after a split happens for simple select
            else if ((splitTableBeforeSelect && i == 3) || i == 4) {
                assert ((int) Math.pow(2, i + 1) < rowCount);
            }
        }
        conn.close();
    }

    @Test
    public void testUpsertSelectAfterTableSplit() throws Exception {
        // no need to split the table during the UPSERT SELECT for this test so just set the flag to true
        tableWasSplitDuringScannerNext = true;
        helpTestUpsertSelectWithSplit(true);
    }

    @Test
    public void  testUpsertSelectDuringSplitOnRowScanned() throws Exception {
        tableWasSplitDuringScannerNext = false;
        splitPoint = null;
        helpTestUpsertSelectWithSplit(false);
    }

    @Test
    public void testUpsertSelectDuringSplitOnRowInMiddleOfRegionBeingScanned() throws Exception {
        tableWasSplitDuringScannerNext = false;
        // when the table has 16 rows, split the table in the middle of the region on row 14
        splitPoint = PInteger.INSTANCE.toBytes(14);
        helpTestUpsertSelectWithSplit(false);
    }

    @Test
    public void testSimpleSelectAfterTableSplit() throws Exception {
        // no need to split the table while running the SELECT and the UPSERT so just set the flag to true
        tableWasSplitDuringScannerNext = true;
        helpTestSelectWithSplit(true, false, false);
    }

    @Test
    public void  testSimpleSelectDuringSplitOnRowScanned() throws Exception {
        tableWasSplitDuringScannerNext = false;
        splitPoint = null;
        helpTestSelectWithSplit(false, false, false);
    }

    @Test
    public void testSimpleSelectDuringSplitOnRowInMiddleOfRegionBeingScanned() throws Exception {
        tableWasSplitDuringScannerNext = false;
        // when the table has 16 rows, split the table in the middle of the region on row 14
        splitPoint = PInteger.INSTANCE.toBytes(14);
        helpTestSelectWithSplit(false, false, false);
    }

    @Test
    public void testOrderByAfterTableSplit() throws Exception {
        // no need to split the table while running the SELECT and the UPSERT so just set the flag to true
        tableWasSplitDuringScannerNext = true;
        helpTestSelectWithSplit(true, true, false);
    }

    @Test
    public void  testOrderByDuringSplitOnRowScanned() throws Exception {
        tableWasSplitDuringScannerNext = false;
        splitPoint = null;
        helpTestSelectWithSplit(false, true, false);
    }

    @Test
    public void testOrderByDuringSplitOnRowInMiddleOfRegionBeingScanned() throws Exception {
        tableWasSplitDuringScannerNext = false;
        // when the table has 16 rows, split the table in the middle of the region on row 14
        splitPoint = PInteger.INSTANCE.toBytes(14);
        helpTestSelectWithSplit(false, true, false);
    }

    @Test
    public void testLimitAfterTableSplit() throws Exception {
        // no need to split the table while running the SELECT and the UPSERT so just set the flag to true
        tableWasSplitDuringScannerNext = true;
        helpTestSelectWithSplit(true, false, true);
    }

    @Test
    public void  testLimitDuringSplitOnRowScanned() throws Exception {
        tableWasSplitDuringScannerNext = false;
        splitPoint = null;
        helpTestSelectWithSplit(false, false, true);
    }

    @Test
    public void testLimitDuringSplitOnRowInMiddleOfRegionBeingScanned() throws Exception {
        tableWasSplitDuringScannerNext = false;
        // when the table has 16 rows, split the table in the middle of the region on row 14
        splitPoint = PInteger.INSTANCE.toBytes(14);
        helpTestSelectWithSplit(false, false, true);
    }
}
