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
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.assertRawCellCount;
import static org.apache.phoenix.util.TestUtil.assertRawRowCount;
import static org.apache.phoenix.util.TestUtil.assertRowExistsAtSCN;
import static org.apache.phoenix.util.TestUtil.assertRowHasExpectedValueAtSCN;
import static org.apache.phoenix.util.TestUtil.assertTableHasTtl;
import static org.apache.phoenix.util.TestUtil.assertTableHasVersions;
import static org.junit.Assert.assertFalse;

@Category(NeedsOwnMiniClusterTest.class)
public class MaxLookbackIT extends BaseTest {
    private static final int MAX_LOOKBACK_AGE = 15;
    private static final int ROWS_POPULATED = 2;
    public static final int WAIT_AFTER_TABLE_CREATION_MILLIS = 1;
    private String tableDDLOptions;
    private StringBuilder optionBuilder;
    ManualEnvironmentEdge injectEdge;
    private int ttl;

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(3);
        props.put(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB, Long.toString(0));
        props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY, Integer.toString(MAX_LOOKBACK_AGE));
        props.put(QueryServices.PHOENIX_TABLE_TTL_ENABLED, Boolean.toString(Boolean.FALSE));
        props.put("hbase.procedure.remote.dispatcher.delay.msec", "0");
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Before
    public void beforeTest(){
        EnvironmentEdgeManager.reset();
        optionBuilder = new StringBuilder();
        this.tableDDLOptions = optionBuilder.toString();
        ttl = 0;
        injectEdge = new ManualEnvironmentEdge();
        injectEdge.setValue(EnvironmentEdgeManager.currentTimeMillis());
    }

    @After
    public synchronized void afterTest() throws Exception {
        boolean refCountLeaked = isAnyStoreRefCountLeaked();

        EnvironmentEdgeManager.reset();
        assertFalse("refCount leaked", refCountLeaked);
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
            majorCompact(indexTable);
            assertRawRowCount(conn, dataTable, rowsPlusDeleteMarker);
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
            majorCompact(dataTable);
            majorCompact(indexTable);
            //should still be ROWS_POPULATED because we added one and deleted one
            assertRawRowCount(conn, dataTable, ROWS_POPULATED);
            assertRawRowCount(conn, indexTable, ROWS_POPULATED);

            //deleted row should be gone, but not deleted row should still be there.
            assertRowExistsAtSCN(getUrl(), sql, beforeSecondCompactSCN, false);
            assertRowExistsAtSCN(getUrl(), indexSql, beforeSecondCompactSCN, false);
            assertRowExistsAtSCN(getUrl(), notDeletedRowSql, beforeSecondCompactSCN, true);
            assertRowExistsAtSCN(getUrl(), notDeletedIndexRowSql, beforeSecondCompactSCN, true);

        }
    }

    @Test(timeout=60000L)
    public void testTTLAndMaxLookbackAge() throws Exception {
        ttl = 20;
        optionBuilder.append("TTL=" + ttl);
        tableDDLOptions = optionBuilder.toString();
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
            long timeToAdvance = (MAX_LOOKBACK_AGE * 1000) -
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
            timeToAdvance = (ttl * 1000) -
                    (EnvironmentEdgeManager.currentTimeMillis() - afterFirstInsertSCN);
            if (timeToAdvance > 0) {
                injectEdge.incrementValue(timeToAdvance);
            }
            //make sure that we can compact away the now-expired rows
            majorCompact(dataTable);
            majorCompact(indexTable);
            //note that before HBase 1.4, we don't have HBASE-17956
            // and this will always return 0 whether it's still on-disk or not
            assertRawRowCount(conn, dataTable, 0);
            assertRawRowCount(conn, indexTable, 0);
        } finally{
            conf.setLong(HRegion.MEMSTORE_PERIODIC_FLUSH_INTERVAL, oldMemstoreFlushInterval);
        }
    }

    @Test(timeout=60000)
    public void testRecentMaxVersionsNotCompactedAway() throws Exception {
        int versions = 2;
        optionBuilder.append("VERSIONS=" + versions);
        tableDDLOptions = optionBuilder.toString();
        String firstValue = "abc";
        String secondValue = "def";
        String thirdValue = "ghi";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            String indexName = generateUniqueName();
            createTable(dataTableName);
            populateTable(dataTableName);
            createIndex(dataTableName, indexName, versions);
            injectEdge.setValue(System.currentTimeMillis());
            EnvironmentEdgeManager.injectEdge(injectEdge);
            //increment to make sure we don't "look back" past table creation
            injectEdge.incrementValue(WAIT_AFTER_TABLE_CREATION_MILLIS);
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
            injectEdge.incrementValue(MAX_LOOKBACK_AGE * 1000);
            long afterLookbackAgeSCN = EnvironmentEdgeManager.currentTimeMillis();
            majorCompact(dataTable);
            majorCompact(indexTable);
            //empty column, 1 version of val 1, 3 versions of val2, 1 version of val3 = 6
            assertRawCellCount(conn, dataTable, Bytes.toBytes("a"), 6);
            //2 versions of empty column, 2 versions of val2,
            // 2 versions of val3 (since we write whole rows to index) = 6
            assertRawCellCount(conn, indexTable, Bytes.toBytes("ab\u0000a"), 6);
            //empty column + 1 version each of val1,2 and 3 = 4
            assertRawCellCount(conn, dataTable, Bytes.toBytes("b"), 4);
            //1 version of empty column, 1 version of val2, 1 version of val3 = 3
            assertRawCellCount(conn, indexTable, Bytes.toBytes("bc\u0000b"), 3);
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
            String createSql = "create table " + tableName +
                    " (id varchar(10) not null primary key, val1 varchar(10), " +
                    "val2 varchar(10), val3 varchar(10))" + tableDDLOptions;
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
