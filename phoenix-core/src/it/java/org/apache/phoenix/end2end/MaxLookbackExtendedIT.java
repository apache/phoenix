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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.CompactionScanner;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.assertRawCellCount;
import static org.apache.phoenix.util.TestUtil.assertRawRowCount;
import static org.apache.phoenix.util.TestUtil.assertRowExistsAtSCN;
import static org.apache.phoenix.util.TestUtil.assertRowHasExpectedValueAtSCN;
import static org.apache.phoenix.util.TestUtil.assertTableHasTtl;
import static org.apache.phoenix.util.TestUtil.assertTableHasVersions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class MaxLookbackExtendedIT extends BaseTest {
    private static final int MAX_LOOKBACK_AGE = 15;
    private static final int ROWS_POPULATED = 2;
    public static final int WAIT_AFTER_TABLE_CREATION_MILLIS = 1;
    private String tableDDLOptions;
    private StringBuilder optionBuilder;
    ManualEnvironmentEdge injectEdge;
    private int ttl;
    private boolean multiCF;

    public MaxLookbackExtendedIT(boolean multiCF) {
        this.multiCF = multiCF;
    }
    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(2);
        props.put(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB, Long.toString(0));
        props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY, Integer.toString(MAX_LOOKBACK_AGE));
        props.put("hbase.procedure.remote.dispatcher.delay.msec", "0");
        props.put(HRegion.MEMSTORE_PERIODIC_FLUSH_INTERVAL, "0");
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Before
    public void beforeTest(){
        EnvironmentEdgeManager.reset();
        optionBuilder = new StringBuilder();
        ttl = 30;
        optionBuilder.append(" TTL=" + ttl);
        this.tableDDLOptions = optionBuilder.toString();
        injectEdge = new ManualEnvironmentEdge();
        injectEdge.setValue(EnvironmentEdgeManager.currentTimeMillis());
    }

    @After
    public synchronized void afterTest() throws Exception {
        boolean refCountLeaked = isAnyStoreRefCountLeaked();

        EnvironmentEdgeManager.reset();
        Assert.assertFalse("refCount leaked", refCountLeaked);
    }

    @Parameterized.Parameters(name = "MaxLookbackExtendedIT_multiCF={0}")
    public static Collection<Boolean> data() {
        return Arrays.asList(false, true);
    }
    @Test
    public void testKeepDeletedCellsWithMaxLookbackAge() throws Exception {
        int versions = 2;
        optionBuilder.append(", VERSIONS=" + versions);
        optionBuilder.append(", KEEP_DELETED_CELLS=TRUE");
        tableDDLOptions = optionBuilder.toString();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            createTable(dataTableName);
            injectEdge.setValue(System.currentTimeMillis());
            EnvironmentEdgeManager.injectEdge(injectEdge);
            conn.createStatement().execute("upsert into " + dataTableName
                    + " values ('a', 'ab1', 'abc1', 'abcd1')");
            conn.commit();
            conn.createStatement().execute("upsert into " + dataTableName
                    + " values ('b', 'bc1', 'bcd1', 'bcde1')");
            conn.commit();
            injectEdge.incrementValue(1);
            conn.createStatement().execute("upsert into " + dataTableName
                    + " values ('a', 'ab2', 'abc2', 'abcd2')");
            conn.commit();
            injectEdge.incrementValue(1);
            conn.createStatement().execute("upsert into " + dataTableName
                    + " values ('a', 'ab3', 'abc3', 'abcd3')");
            conn.commit();
            injectEdge.incrementValue(1);
            String dml = "DELETE from " + dataTableName + " WHERE id  = 'a'";
            Assert.assertEquals(1, conn.createStatement().executeUpdate(dml));
            conn.commit();
            injectEdge.incrementValue(MAX_LOOKBACK_AGE * 1000);
            dml = "DELETE from " + dataTableName + " WHERE id  = 'b'";
            Assert.assertEquals(1, conn.createStatement().executeUpdate(dml));
            conn.commit();
            injectEdge.incrementValue(1);
            conn.createStatement().execute("upsert into " + dataTableName + " values ('b', 'bc2', 'bcd2', 'bcde2')");
            conn.commit();
            dml = "DELETE from " + dataTableName + " WHERE id  = 'b'";
            Assert.assertEquals(1, conn.createStatement().executeUpdate(dml));
            conn.commit();
            injectEdge.incrementValue(1);
            conn.createStatement().execute("upsert into " + dataTableName + " values ('b', 'bc', 'bcd3', 'bcde3')");
            conn.commit();
            injectEdge.incrementValue(1);
            conn.createStatement().execute("upsert into " + dataTableName + " values ('b', 'bc', 'bcd4', 'bcde4')");
            conn.commit();
            // Now, inside the max lookback window, there are two live and one deleted versions
            // of row b and two sets of CF delete markers for row b. Also, one deleted version
            // for row b that is outside the max lookback window but still visible through it.
            TableName dataTable = TableName.valueOf(dataTableName);
            TestUtil.doMajorCompaction(conn, dataTableName);
            // Only row b should be live
            ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) from " +
                    dataTableName);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));
            rs = conn.createStatement().executeQuery("SELECT COUNT(*) from " +
                    dataTableName + " where id = 'b'");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));
            assertRawRowCount(conn, dataTable, 2);
            // Each row version has empty column + val 1, val2, and val3 = 4 cells
            // There are three version of row "a" and they are deleted by one set of CF delete
            // markers. Even though they are ouside the max lookback window, the compaction will
            // retain two versions of them and delete markers since KEEP_DELETED_CELLS=TRUE and
            // VERSIONS=2
            if (multiCF) {
                assertRawCellCount(conn, dataTable, Bytes.toBytes("a"), 11);
            } else {
                assertRawCellCount(conn, dataTable, Bytes.toBytes("a"), 9);
            }
            // Three row b versions inside plus one deleted version outside the
            // max lookback window but visible and plus 2 sets of delete markers
            if (multiCF) {
                // 4 cells for each version (4 versions) plus 2 delete markers for each CF (3CFs)
                // = 19
                assertRawCellCount(conn, dataTable, Bytes.toBytes("b"), 22);
            } else {
                // 4 cells for each version (4 versions) plus 2 delete markers = 16
                assertRawCellCount(conn, dataTable, Bytes.toBytes("b"), 18);
            }
        }
    }
    @Test
    public void testTooLowSCNWithMaxLookbackAge() throws Exception {
        String dataTableName = generateUniqueName();
        createTable(dataTableName);
        injectEdge.setValue(System.currentTimeMillis());
        EnvironmentEdgeManager.injectEdge(injectEdge);
        //increase long enough to make sure we can find the syscat row for the table
        injectEdge.incrementValue(WAIT_AFTER_TABLE_CREATION_MILLIS);
        populateTable(dataTableName);
        long populateTime = EnvironmentEdgeManager.currentTimeMillis();
        injectEdge.incrementValue(MAX_LOOKBACK_AGE * 1000 + 1000);
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
            Long.toString(populateTime));
        try (Connection connscn = DriverManager.getConnection(getUrl(), props)) {
            connscn.createStatement().executeQuery("select * from " + dataTableName);
        } catch (SQLException se) {
            SQLExceptionCode code =
                SQLExceptionCode.CANNOT_QUERY_TABLE_WITH_SCN_OLDER_THAN_MAX_LOOKBACK_AGE;
            TestUtil.assertSqlExceptionCode(code, se);
            return;
        }
        Assert.fail("We should have thrown an exception for the too-early SCN");
    }

    @Test(timeout=120000L)
    public void testRecentlyDeletedRowsNotCompactedAway() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            String indexName = generateUniqueName();
            createTable(dataTableName);

            TableName dataTable = TableName.valueOf(dataTableName);
            populateTable(dataTableName);
            createIndex(dataTableName, indexName, 1);
            injectEdge.setValue(System.currentTimeMillis());
            EnvironmentEdgeManager.injectEdge(injectEdge);
            TableName indexTable = TableName.valueOf(indexName);
            injectEdge.incrementValue(WAIT_AFTER_TABLE_CREATION_MILLIS);
            long beforeDeleteSCN = EnvironmentEdgeManager.currentTimeMillis();
            injectEdge.incrementValue(10); //make sure we delete at a different ts
            Statement stmt = conn.createStatement();
            stmt.execute("DELETE FROM " + dataTableName + " WHERE " + " id = 'a'");
            Assert.assertEquals(1, stmt.getUpdateCount());
            conn.commit();
            //select stmt to get row we deleted
            String sql = String.format("SELECT * FROM %s WHERE id = 'a'", dataTableName);
            String indexSql = String.format("SELECT * FROM %s WHERE val1 = 'ab'", dataTableName);
            int rowsPlusDeleteMarker = ROWS_POPULATED;
            assertRowExistsAtSCN(getUrl(), sql, beforeDeleteSCN, true);
            assertExplainPlan(conn, indexSql, dataTableName, indexName);
            assertRowExistsAtSCN(getUrl(), indexSql, beforeDeleteSCN, true);
            flush(dataTable);
            flush(indexTable);
            assertRowExistsAtSCN(getUrl(), sql, beforeDeleteSCN, true);
            assertRowExistsAtSCN(getUrl(), indexSql, beforeDeleteSCN, true);
            long beforeFirstCompactSCN = EnvironmentEdgeManager.currentTimeMillis();
            injectEdge.incrementValue(1); //new ts for major compaction
            majorCompact(dataTable);
            assertRawRowCount(conn, dataTable, rowsPlusDeleteMarker);
            majorCompact(indexTable);
            assertRawRowCount(conn, indexTable, rowsPlusDeleteMarker);
            //wait for the lookback time. After this compactions should purge the deleted row
            injectEdge.incrementValue(MAX_LOOKBACK_AGE * 1000);
            long beforeSecondCompactSCN = EnvironmentEdgeManager.currentTimeMillis();
            String notDeletedRowSql =
                String.format("SELECT * FROM %s WHERE id = 'b'", dataTableName);
            String notDeletedIndexRowSql =
                String.format("SELECT * FROM %s WHERE val1 = 'bc'", dataTableName);
            assertRowExistsAtSCN(getUrl(), notDeletedRowSql, beforeSecondCompactSCN, true);
            assertRowExistsAtSCN(getUrl(), notDeletedIndexRowSql, beforeSecondCompactSCN, true);
            assertRawRowCount(conn, dataTable, ROWS_POPULATED);
            assertRawRowCount(conn, indexTable, ROWS_POPULATED);
            conn.createStatement().execute("upsert into " + dataTableName +
                " values ('c', 'cd', 'cde', 'cdef')");
            conn.commit();
            injectEdge.incrementValue(1L);
            flush(dataTable);
            majorCompact(dataTable);
            // Deleted row versions should be removed.
            assertRawRowCount(conn, dataTable, ROWS_POPULATED);

            flush(indexTable);
            majorCompact(indexTable);
            // Deleted row versions should be removed.
            assertRawRowCount(conn, indexTable, ROWS_POPULATED);

            //deleted row should be gone, but not deleted row should still be there.
            assertRowExistsAtSCN(getUrl(), sql, beforeSecondCompactSCN, false);
            assertRowExistsAtSCN(getUrl(), indexSql, beforeSecondCompactSCN, false);
            assertRowExistsAtSCN(getUrl(), notDeletedRowSql, beforeSecondCompactSCN, true);
            assertRowExistsAtSCN(getUrl(), notDeletedIndexRowSql, beforeSecondCompactSCN, true);

        }
    }

    @Test(timeout=60000L)
    public void testViewIndexIsCompacted() throws Exception {
        String baseTable =  SchemaUtil.getTableName("SCHEMA1", generateUniqueName());
        String globalViewName = generateUniqueName();
        String fullGlobalViewName = SchemaUtil.getTableName("SCHEMA2", globalViewName);
        String globalViewIdx =  generateUniqueName();
        TableName dataTable = TableName.valueOf(baseTable);
        TableName indexTable = TableName.valueOf("_IDX_" + baseTable);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + baseTable
                    + " (TENANT_ID CHAR(15) NOT NULL, PK2 INTEGER NOT NULL, PK3 INTEGER NOT NULL, "
                    + "COL1 VARCHAR, COL2 VARCHAR, COL3 CHAR(15) CONSTRAINT PK PRIMARY KEY"
                    + "(TENANT_ID, PK2, PK3)) MULTI_TENANT=true");
            conn.createStatement().execute("CREATE VIEW " + fullGlobalViewName
                    + " AS SELECT * FROM " + baseTable);
            conn.createStatement().execute("CREATE INDEX " + globalViewIdx + " ON "
                    + fullGlobalViewName + " (COL1) INCLUDE (COL2)");

            conn.createStatement().executeUpdate("UPSERT INTO  " + fullGlobalViewName
                    + " (TENANT_ID, PK2, PK3, COL1, COL2) VALUES ('TenantId1',1, 2, 'a', 'b')");
            conn.commit();

            String query = "SELECT COL2 FROM " + fullGlobalViewName + " WHERE  COL1 = 'a'";
            // Verify that query uses the global view index
            ResultSet rs = conn.createStatement().executeQuery(query);
            PTable table = ((PhoenixResultSet)rs).getContext().getCurrentTable().getTable();
            assertTrue(table.getSchemaName().getString().equals("SCHEMA2") &&
                    table.getTableName().getString().equals(globalViewIdx));
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
            assertFalse(rs.next());
            // Force a flush
            flush(dataTable);
            flush(indexTable);
            assertRawRowCount(conn, dataTable, 1);
            assertRawRowCount(conn, indexTable, 1);
            // Delete the row from both tables
            conn.createStatement().execute("DELETE FROM " + fullGlobalViewName
                            + " WHERE TENANT_ID = 'TenantId1'");
            conn.commit();
            // Force a flush
            flush(dataTable);
            flush(indexTable);
            assertRawRowCount(conn, dataTable, 1);
            assertRawRowCount(conn, indexTable, 1);
            // Move change beyond the max lookback window
            injectEdge.setValue(System.currentTimeMillis() + MAX_LOOKBACK_AGE * 1000 + 1);
            EnvironmentEdgeManager.injectEdge(injectEdge);
            // Major compact both tables
            majorCompact(dataTable);
            majorCompact(indexTable);
            // Everything should have been purged by major compaction
            assertRawRowCount(conn, dataTable, 0);
            assertRawRowCount(conn, indexTable, 0);
        }
    }
    @Test(timeout=60000L)
    public void testTTLAndMaxLookbackAge() throws Exception {
        Configuration conf = getUtility().getConfiguration();
        //disable automatic memstore flushes
        long oldMemstoreFlushInterval = conf.getLong(HRegion.MEMSTORE_PERIODIC_FLUSH_INTERVAL,
            HRegion.DEFAULT_CACHE_FLUSH_INTERVAL);
        conf.setLong(HRegion.MEMSTORE_PERIODIC_FLUSH_INTERVAL, 0L);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            String indexName = generateUniqueName();
            createTable(dataTableName);
            populateTable(dataTableName);
            createIndex(dataTableName, indexName, 1);
            injectEdge.setValue(System.currentTimeMillis());
            EnvironmentEdgeManager.injectEdge(injectEdge);
            injectEdge.incrementValue(1);
            long afterFirstInsertSCN = EnvironmentEdgeManager.currentTimeMillis();
            TableName dataTable = TableName.valueOf(dataTableName);
            TableName indexTable = TableName.valueOf(indexName);
            assertTableHasTtl(conn, dataTable, ttl);
            assertTableHasTtl(conn, indexTable, ttl);
            //first make sure we inserted correctly
            String sql = String.format("SELECT val2 FROM %s WHERE id = 'a'", dataTableName);
            String indexSql = String.format("SELECT val2 FROM %s WHERE val1 = 'ab'", dataTableName);
            assertRowExistsAtSCN(getUrl(),sql, afterFirstInsertSCN, true);
            assertExplainPlan(conn, indexSql, dataTableName, indexName);
            assertRowExistsAtSCN(getUrl(),indexSql, afterFirstInsertSCN, true);
            int originalRowCount = 2;
            assertRawRowCount(conn, dataTable, originalRowCount);
            assertRawRowCount(conn, indexTable, originalRowCount);
            //force a flush
            flush(dataTable);
            flush(indexTable);
            //flush shouldn't have changed it
            assertRawRowCount(conn, dataTable, originalRowCount);
            assertRawRowCount(conn, indexTable, originalRowCount);
            assertExplainPlan(conn, indexSql, dataTableName, indexName);
            long timeToAdvance = (MAX_LOOKBACK_AGE * 1000L) -
                (EnvironmentEdgeManager.currentTimeMillis() - afterFirstInsertSCN);
            if (timeToAdvance > 0) {
                injectEdge.incrementValue(timeToAdvance);
            }
            //make sure it's still on disk
            assertRawRowCount(conn, dataTable, originalRowCount);
            assertRawRowCount(conn, indexTable, originalRowCount);
            injectEdge.incrementValue(1); //get a new timestamp for compaction
            majorCompact(dataTable);
            majorCompact(indexTable);
            //nothing should have been purged by this major compaction
            assertRawRowCount(conn, dataTable, originalRowCount);
            assertRawRowCount(conn, indexTable, originalRowCount);
            //now wait the TTL
            timeToAdvance = (ttl * 1000L) -
                (EnvironmentEdgeManager.currentTimeMillis() - afterFirstInsertSCN);
            if (timeToAdvance > 0) {
                injectEdge.incrementValue(timeToAdvance);
            }
            //make sure that the now-expired rows are masked for regular scans
            ResultSet rs = conn.createStatement().executeQuery(sql);
            Assert.assertFalse(rs.next());
            rs = conn.createStatement().executeQuery(indexSql);
            Assert.assertFalse(rs.next());
            rs = conn.createStatement().executeQuery("SELECT COUNT(*) from " + dataTableName);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(0, rs.getInt(1));
            rs = conn.createStatement().executeQuery("SELECT COUNT(*) from " + indexName);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(0, rs.getInt(1));
            // Increment the time by max lookback age and make sure that we can compact away
            // the now-expired rows
            injectEdge.incrementValue(MAX_LOOKBACK_AGE * 1000L);
            majorCompact(dataTable);
            majorCompact(indexTable);
            assertRawRowCount(conn, dataTable, 0);
            assertRawRowCount(conn, indexTable, 0);
        } finally{
            conf.setLong(HRegion.MEMSTORE_PERIODIC_FLUSH_INTERVAL, oldMemstoreFlushInterval);
        }
    }

    @Test(timeout=60000)
    public void testRecentMaxVersionsNotCompactedAway() throws Exception {
        int versions = 2;
        optionBuilder.append(", VERSIONS=" + versions);
        tableDDLOptions = optionBuilder.toString();
        String firstValue = "abc";
        String secondValue = "def";
        String thirdValue = "ghi";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            String indexName = generateUniqueName();
            createTable(dataTableName);
            createIndex(dataTableName, indexName, versions);
            injectEdge.setValue(System.currentTimeMillis());
            EnvironmentEdgeManager.injectEdge(injectEdge);
            populateTable(dataTableName);
            injectEdge.incrementValue(1); //increment by 1 so we can see our write
            long afterInsertSCN = EnvironmentEdgeManager.currentTimeMillis();
            //make sure table and index metadata is set up right for versions
            TableName dataTable = TableName.valueOf(dataTableName);
            TableName indexTable = TableName.valueOf(indexName);
            assertTableHasVersions(conn, dataTable, versions);
            assertTableHasVersions(conn, indexTable, versions);
            //check query optimizer is doing what we expect
            String dataTableSelectSql =
                String.format("SELECT val2 FROM %s WHERE id = 'a'", dataTableName);
            String indexTableSelectSql =
                String.format("SELECT val2 FROM %s WHERE val1 = 'ab'", dataTableName);
            assertExplainPlan(conn, indexTableSelectSql, dataTableName, indexName);
            //make sure the data was inserted correctly in the first place
            assertRowHasExpectedValueAtSCN(getUrl(), dataTableSelectSql, afterInsertSCN, firstValue);
            assertRowHasExpectedValueAtSCN(getUrl(), indexTableSelectSql, afterInsertSCN, firstValue);
            //force first update to get a distinct ts
            injectEdge.incrementValue(1);
            updateColumn(conn, dataTableName, "id", "a", "val2", secondValue);
            injectEdge.incrementValue(1); //now make update visible
            long afterFirstUpdateSCN = EnvironmentEdgeManager.currentTimeMillis();
            //force second update to get a distinct ts
            injectEdge.incrementValue(1);
            updateColumn(conn, dataTableName, "id", "a", "val2", thirdValue);
            injectEdge.incrementValue(1);
            long afterSecondUpdateSCN = EnvironmentEdgeManager.currentTimeMillis();
            injectEdge.incrementValue(1);
            //check to make sure we can see all three versions at the appropriate times
            String[] allValues = {firstValue, secondValue, thirdValue};
            long[] allSCNs = {afterInsertSCN, afterFirstUpdateSCN, afterSecondUpdateSCN};
            assertMultiVersionLookbacks(dataTableSelectSql, allValues, allSCNs);
            assertMultiVersionLookbacks(indexTableSelectSql, allValues, allSCNs);
            flush(dataTable);
            flush(indexTable);
            //after flush, check to make sure we can see all three versions at the appropriate times
            assertMultiVersionLookbacks(dataTableSelectSql, allValues, allSCNs);
            assertMultiVersionLookbacks(indexTableSelectSql, allValues, allSCNs);
            majorCompact(dataTable);
            majorCompact(indexTable);
            //after major compaction, check to make sure we can see all three versions
            // at the appropriate times
            assertMultiVersionLookbacks(dataTableSelectSql, allValues, allSCNs);
            assertMultiVersionLookbacks(indexTableSelectSql, allValues, allSCNs);
            // Make the first row versions outside the max lookback window
            injectEdge.setValue(afterInsertSCN + MAX_LOOKBACK_AGE * 1000);
            majorCompact(dataTable);
            majorCompact(indexTable);
            // At this moment, the data table has three row versions for row a within the max
            // lookback window.
            // These versions have the following cells {empty, ab, abc, abcd}, {empty, def} and
            // {empty, ghi}.
            assertRawCellCount(conn, dataTable, Bytes.toBytes("a"), 8);
            // The index table will have three full row versions for "a" and each will have 3 cells,
            // one for each of columns empty, val2, and val3.
            assertRawCellCount(conn, indexTable, Bytes.toBytes("ab\u0000a"), 9);
            //empty column + 1 version each of val1,2 and 3 = 4
            assertRawCellCount(conn, dataTable, Bytes.toBytes("b"), 4);
            //1 version of empty column, 1 version of val2, 1 version of val3 = 3
            assertRawCellCount(conn, indexTable, Bytes.toBytes("bc\u0000b"), 3);
        }
    }

    @Test(timeout=60000)
    public void testOverrideMaxLookbackForCompaction() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String tableNameOne = generateUniqueName();
            createTable(tableNameOne);
            String tableNameTwo = generateUniqueName();
            createTable(tableNameTwo);
            injectEdge.setValue(System.currentTimeMillis());
            EnvironmentEdgeManager.injectEdge(injectEdge);
            // Change the max lookback window for each table
            int maxLookbackOne = 20;
            int maxLookbackTwo = 25;
            CompactionScanner.overrideMaxLookback(tableNameOne, "0", maxLookbackOne * 1000);
            CompactionScanner.overrideMaxLookback(tableNameTwo, "0", maxLookbackTwo * 1000);
            if (multiCF) {
                CompactionScanner.overrideMaxLookback(tableNameOne, "A", maxLookbackOne * 1000);
                CompactionScanner.overrideMaxLookback(tableNameOne, "B", maxLookbackOne * 1000);
                CompactionScanner.overrideMaxLookback(tableNameTwo, "A", maxLookbackTwo * 1000);
                CompactionScanner.overrideMaxLookback(tableNameTwo, "B", maxLookbackTwo * 1000);
            }
            injectEdge.incrementValue(1000);
            populateTable(tableNameOne);
            populateTable(tableNameTwo);
            injectEdge.incrementValue(1000);
            populateTable(tableNameOne);
            populateTable(tableNameTwo);
            injectEdge.incrementValue(1000);
            conn.createStatement().executeUpdate("DELETE FROM " + tableNameOne);
            conn.createStatement().executeUpdate("DELETE FROM " + tableNameTwo);
            conn.commit();
            // Move the time so that deleted row versions will be outside the maxlookback window
            // of tableNameOne but the delete markers will be inside (in this case on the lower
            // edge of the window)
            injectEdge.incrementValue(maxLookbackOne * 1000);
            // Compact both tables. Deleted row versions will be retained since the delete marker
            // is retained. This is necessary to include the preimage of the row in the CDC
            // stream.
            flush(TableName.valueOf(tableNameOne));
            flush(TableName.valueOf(tableNameTwo));
            majorCompact(TableName.valueOf(tableNameOne));
            majorCompact(TableName.valueOf(tableNameTwo));
            if (multiCF) {
                assertRawCellCount(conn, TableName.valueOf(tableNameOne), Bytes.toBytes("a"), 7);
                assertRawCellCount(conn, TableName.valueOf(tableNameOne), Bytes.toBytes("b"), 7);
                assertRawCellCount(conn, TableName.valueOf(tableNameTwo), Bytes.toBytes("a"), 11);
                assertRawCellCount(conn, TableName.valueOf(tableNameTwo), Bytes.toBytes("b"), 11);
            } else {
                assertRawCellCount(conn, TableName.valueOf(tableNameOne), Bytes.toBytes("a"), 5);
                assertRawCellCount(conn, TableName.valueOf(tableNameOne), Bytes.toBytes("b"), 5);
                assertRawCellCount(conn, TableName.valueOf(tableNameTwo), Bytes.toBytes("a"), 9);
                assertRawCellCount(conn, TableName.valueOf(tableNameTwo), Bytes.toBytes("b"), 9);
            }
        }
    }

    @Test(timeout=60000)
    public void testRetainingLastRowVersion() throws Exception {
        try(Connection conn = DriverManager.getConnection(getUrl())) {
            String tableName = generateUniqueName();
            createTable(tableName);
            long timeIntervalBetweenTwoUpserts = (ttl / 4) + 1;
            injectEdge.setValue(System.currentTimeMillis());
            EnvironmentEdgeManager.injectEdge(injectEdge);
            TableName dataTableName = TableName.valueOf(tableName);
            injectEdge.incrementValue(1);
            Statement stmt = conn.createStatement();
            stmt.execute("upsert into " + tableName + " values ('a', 'ab', 'abc', 'abcd')");
            conn.commit();
            injectEdge.incrementValue(timeIntervalBetweenTwoUpserts * 1000);
            flush(dataTableName);
            injectEdge.incrementValue(1);
            stmt.execute("upsert into " + tableName + " values ('a', 'ab1')");
            conn.commit();
            injectEdge.incrementValue(timeIntervalBetweenTwoUpserts * 1000);
            flush(dataTableName);
            injectEdge.incrementValue(1);
            stmt.execute("upsert into " + tableName + " values ('a', 'ab2')");
            conn.commit();
            injectEdge.incrementValue(timeIntervalBetweenTwoUpserts * 1000);
            flush(dataTableName);
            injectEdge.incrementValue(1);
            TestUtil.minorCompact(utility, dataTableName);
            injectEdge.incrementValue(1);
            stmt.execute("upsert into " + tableName + " values ('a', 'ab3')");
            conn.commit();
            injectEdge.incrementValue(timeIntervalBetweenTwoUpserts * 1000);
            flush(dataTableName);
            injectEdge.incrementValue(1);
            stmt.execute("upsert into " + tableName + " values ('a', 'ab4')");
            conn.commit();
            injectEdge.incrementValue(1);
            flush(dataTableName);
            TestUtil.dumpTable(conn, dataTableName);
            injectEdge.incrementValue(1);
            TestUtil.minorCompact(utility, dataTableName);
            injectEdge.incrementValue(MAX_LOOKBACK_AGE * 1000 - 1);
            TestUtil.dumpTable(conn, dataTableName);
            majorCompact(dataTableName);
            injectEdge.incrementValue(1);
            TestUtil.dumpTable(conn, dataTableName);
            ResultSet rs = stmt.executeQuery("select * from " + dataTableName + " where id = 'a'");
            while(rs.next()) {
                assertNotNull(rs.getString(3));
                assertNotNull(rs.getString(4));
            }
        }
    }

    private void flush(TableName table) throws IOException {
        Admin admin = getUtility().getAdmin();
        admin.flush(table);
    }

    private void majorCompact(TableName table) throws Exception {
        TestUtil.majorCompact(getUtility(), table);
    }

    private void assertMultiVersionLookbacks(String dataTableSelectSql,
                                             String[] values, long[] scns)
        throws Exception {
        //make sure we can still look back after updating
        for (int k = 0; k < values.length; k++){
            assertRowHasExpectedValueAtSCN(getUrl(), dataTableSelectSql, scns[k], values[k]);
        }
    }

    private void updateColumn(Connection conn, String dataTableName,
                              String idColumn, String id, String valueColumn, String value)
        throws SQLException {
        String upsertSql = String.format("UPSERT INTO %s (%s, %s) VALUES ('%s', '%s')",
            dataTableName, idColumn, valueColumn, id, value);
        conn.createStatement().execute(upsertSql);
        conn.commit();
    }

    private void createTable(String tableName) throws SQLException {
        try(Connection conn = DriverManager.getConnection(getUrl())) {
            String createSql;
            if (multiCF) {
                createSql = "create table " + tableName +
                        " (id varchar(10) not null primary key, val1 varchar(10), " +
                        "a.val2 varchar(10), b.val3 varchar(10))" + tableDDLOptions;
            }
            else {
                createSql = "create table " + tableName +
                        " (id varchar(10) not null primary key, val1 varchar(10), " +
                        "val2 varchar(10), val3 varchar(10))" + tableDDLOptions;
            }
            conn.createStatement().execute(createSql);
            conn.commit();
        }
    }
    private void populateTable(String tableName) throws SQLException {
        try(Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("upsert into " + tableName + " values ('a', 'ab', 'abc', 'abcd')");
            conn.commit();
            conn.createStatement().execute("upsert into " + tableName + " values ('b', 'bc', 'bcd', 'bcde')");
            conn.commit();
        }
    }

    private void createIndex(String dataTableName, String indexTableName, int indexVersions)
        throws SQLException {
        try(Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE INDEX " + indexTableName + " on " +
                dataTableName + " (val1) include (val2, val3)" +
                " VERSIONS=" + indexVersions);
            conn.commit();
        }
    }

    public static void assertExplainPlan(Connection conn, String selectSql,
                                         String dataTableFullName, String indexTableFullName) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
        String actualExplainPlan = QueryUtil.getExplainPlan(rs);
        IndexToolIT.assertExplainPlan(false, actualExplainPlan, dataTableFullName, indexTableFullName);
    }

}
