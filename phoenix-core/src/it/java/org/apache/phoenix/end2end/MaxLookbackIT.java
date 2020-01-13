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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.ScanInfoUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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

@NeedsOwnMiniClusterTest
public class MaxLookbackIT extends BaseUniqueNamesOwnClusterIT {
    private static final Log LOG = LogFactory.getLog(MaxLookbackIT.class);
    private static final int MAX_LOOKBACK_AGE = 10;
    private static final int ROWS_POPULATED = 2;
    private String tableDDLOptions;
    private StringBuilder optionBuilder;

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB, Long.toString(0));
        props.put(ScanInfoUtil.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY, Integer.toString(MAX_LOOKBACK_AGE));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Before
    public void beforeTest(){
        optionBuilder = new StringBuilder();
        this.tableDDLOptions = optionBuilder.toString();
    }

    @Test
    public void testTooLowSCNWithMaxLookbackAge() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            String indexStem = generateUniqueName();
            createTableAndIndexes(conn, dataTableName, indexStem);
            //need to sleep long enough for the SCN to still find the syscat row for the table
            Thread.sleep(MAX_LOOKBACK_AGE * 1000 + 1000);
            Properties props = new Properties();
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
                Long.toString(EnvironmentEdgeManager.currentTime() - (MAX_LOOKBACK_AGE + 1) * 1000));
            try (Connection connscn = DriverManager.getConnection(getUrl(), props)) {
                connscn.createStatement().executeQuery("select * from " + dataTableName);
            } catch (SQLException se) {
                SQLExceptionCode code =
                    SQLExceptionCode.CANNOT_QUERY_TABLE_WITH_SCN_OLDER_THAN_MAX_LOOKBACK_AGE;
                TestUtil.assertSqlExceptionCode(code, se);
                return;
            }
        }
        Assert.fail("We should have thrown an exception for the too-early SCN");
    }

    @Test(timeout=120000L)
    public void testRecentlyDeletedRowsNotCompactedAway() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            String indexStem = generateUniqueName();
            createTableAndIndexes(conn, dataTableName, indexStem);
            String fullIndexName = indexStem + "1";
            TableName dataTable = TableName.valueOf(dataTableName);
            TableName indexTable = TableName.valueOf(fullIndexName);
            assertRawRowCount(conn, indexTable, ROWS_POPULATED);
            assertTableHasTtl(conn, indexTable, Integer.MAX_VALUE);
            long beforeDeleteSCN = org.apache.phoenix.util.EnvironmentEdgeManager.currentTimeMillis();
            Thread.sleep(1); //make sure we delete at a different ts
            Statement stmt = conn.createStatement();
            stmt.execute("DELETE FROM " + dataTableName + " WHERE " + " id = 'a'");
            Assert.assertEquals(1, stmt.getUpdateCount());
            conn.commit();
            //select stmt to get row we deleted
            String sql = String.format("SELECT * FROM %s WHERE val1 = 'ab'", dataTableName);
            assertExplainPlan(conn, sql, dataTableName, fullIndexName);
            int rowsPlusDeleteMarker = ROWS_POPULATED;
            assertRawRowCount(conn, indexTable, rowsPlusDeleteMarker);
            assertRowExistsAtSCN(getUrl(), sql, beforeDeleteSCN, true);
            flush(dataTable);
            flush(indexTable);
            assertRawRowCount(conn, indexTable, rowsPlusDeleteMarker);
            assertRowExistsAtSCN(getUrl(), sql, beforeDeleteSCN, true);
            long beforeFirstCompactSCN = EnvironmentEdgeManager.currentTime();
            Thread.sleep(1);
            majorCompact(indexTable, beforeFirstCompactSCN);
            assertRawRowCount(conn, indexTable, rowsPlusDeleteMarker);
            assertRowExistsAtSCN(getUrl(), sql, beforeDeleteSCN, true);
            //wait for the lookback time. After this compactions should purge the deleted row
            Thread.sleep(MAX_LOOKBACK_AGE * 1000);
            long beforeSecondCompactSCN = org.apache.phoenix.util.EnvironmentEdgeManager.currentTimeMillis();
            String notDeletedRowSql =
                String.format("SELECT * FROM %s WHERE val1 = 'bc'", dataTableName);
            assertExplainPlan(conn, notDeletedRowSql, dataTableName, fullIndexName);
            assertRowExistsAtSCN(getUrl(), notDeletedRowSql, beforeSecondCompactSCN, true);
            assertRawRowCount(conn, indexTable, ROWS_POPULATED);
            assertRawRowCount(conn, dataTable, ROWS_POPULATED);
            conn.createStatement().execute("upsert into " + dataTableName +
                " values ('c', 'cd', 'cde', 'cdef')");
            conn.commit();
            majorCompact(indexTable, beforeSecondCompactSCN);
            majorCompact(dataTable, beforeSecondCompactSCN);
            assertRawRowCount(conn, dataTable, ROWS_POPULATED);
            //deleted row should be gone, but not deleted row should still be there.
            assertRowExistsAtSCN(getUrl(), sql, beforeSecondCompactSCN, false);
            assertRowExistsAtSCN(getUrl(), notDeletedRowSql, beforeSecondCompactSCN, true);
            //1 deleted row should be gone
            assertRawRowCount(conn, indexTable, ROWS_POPULATED);
        }
    }

    @Test(timeout=60000L)
    public void testTTLAndMaxLookbackAge() throws Exception {
        int ttl = 10;
        optionBuilder.append("TTL=" + ttl);
        tableDDLOptions = optionBuilder.toString();
        Configuration conf = getUtility().getConfiguration();
        //disable automatic memstore flushes
        long oldMemstoreFlushInterval = conf.getLong(HRegion.MEMSTORE_PERIODIC_FLUSH_INTERVAL,
            HRegion.DEFAULT_CACHE_FLUSH_INTERVAL);
        conf.setLong(HRegion.MEMSTORE_PERIODIC_FLUSH_INTERVAL, 0L);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            String indexStem = generateUniqueName();
            createTableAndIndexes(conn, dataTableName, indexStem);
            long afterFirstInsertSCN = org.apache.phoenix.util.EnvironmentEdgeManager.currentTimeMillis();
            TableName dataTable = TableName.valueOf(dataTableName);
            assertTableHasTtl(conn, dataTable, ttl);
            String fullIndexName = indexStem + "1";
            TableName indexTable = TableName.valueOf(fullIndexName);
            assertTableHasTtl(conn, indexTable, ttl);

            //first make sure we inserted correctly
            String sql = String.format("SELECT val2 FROM %s WHERE val1 = 'ab'", dataTableName);
            assertExplainPlan(conn, sql, dataTableName, fullIndexName);
            assertRowExistsAtSCN(getUrl(),sql, afterFirstInsertSCN, true);
            int originalRowCount = 2;
            assertRawRowCount(conn, indexTable, originalRowCount);
            //force a flush
            flush(indexTable);
            //flush shouldn't have changed it
            assertRawRowCount(conn, indexTable, originalRowCount);
            //now wait the TTL
            Thread.sleep((ttl +1) * 1000);
            long afterTTLExpiresSCN = org.apache.phoenix.util.EnvironmentEdgeManager.currentTimeMillis();
            assertExplainPlan(conn, sql, dataTableName, fullIndexName);
            //make sure we can't see it after expiration from masking
            assertRowExistsAtSCN(getUrl(), sql, afterTTLExpiresSCN, false);
            //but it's still on disk
            assertRawRowCount(conn, indexTable, originalRowCount);
            long beforeMajorCompactSCN = org.apache.phoenix.util.EnvironmentEdgeManager.currentTimeMillis();
            majorCompact(indexTable, beforeMajorCompactSCN);
            assertRawRowCount(conn, indexTable, 0);
        } finally{
            conf.setLong(HRegion.MEMSTORE_PERIODIC_FLUSH_INTERVAL, oldMemstoreFlushInterval);
        }
    }

    @Test
    public void testRecentMaxVersionsNotCompactedAway() throws Exception {
        int versions = 2;
        optionBuilder.append("VERSIONS=" + versions);
        tableDDLOptions = optionBuilder.toString();
        String firstValue = "abc";
        String secondValue = "def";
        String thirdValue = "ghi";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            String indexStem = generateUniqueName();
            createTableAndIndexes(conn, dataTableName, indexStem, versions);
            long afterInsertSCN = org.apache.phoenix.util.EnvironmentEdgeManager.currentTimeMillis();
            //make sure table and index metadata is set up right for versions
            TableName dataTable = TableName.valueOf(dataTableName);
            assertTableHasVersions(conn, dataTable, versions);
            String fullIndexName = indexStem + "1";
            TableName indexTable = TableName.valueOf(fullIndexName);
            assertTableHasVersions(conn, indexTable, versions);
            //check query optimizer is doing what we expect
            String dataTableSelectSql =
                String.format("SELECT val2 FROM %s WHERE id = 'a'", dataTableName);
            String indexTableSelectSql =
                String.format("SELECT val2 FROM %s WHERE val1 = 'ab'", dataTableName);
            assertExplainPlan(conn, indexTableSelectSql, dataTableName, fullIndexName);
            //make sure the data was inserted correctly in the first place
            assertRowHasExpectedValueAtSCN(getUrl(), dataTableSelectSql, afterInsertSCN, firstValue);
            assertRowHasExpectedValueAtSCN(getUrl(), indexTableSelectSql, afterInsertSCN, firstValue);
            //force first update to get a distinct ts
            Thread.sleep(1);
            updateColumn(conn, dataTableName, "id", "a", "val2", secondValue);
            long afterFirstUpdateSCN = org.apache.phoenix.util.EnvironmentEdgeManager.currentTimeMillis();
            //force second update to get a distinct ts
            Thread.sleep(1);
            updateColumn(conn, dataTableName, "id", "a", "val2", thirdValue);
            long afterSecondUpdateSCN = org.apache.phoenix.util.EnvironmentEdgeManager.currentTimeMillis();
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
            majorCompact(dataTable, afterSecondUpdateSCN);
            majorCompact(indexTable, afterSecondUpdateSCN);
            //after major compaction, check to make sure we can see all three versions
            // at the appropriate times
            assertMultiVersionLookbacks(dataTableSelectSql, allValues, allSCNs);
            assertMultiVersionLookbacks(indexTableSelectSql, allValues, allSCNs);
            Thread.sleep(MAX_LOOKBACK_AGE * 1000);
            long afterLookbackAgeSCN = org.apache.phoenix.util.EnvironmentEdgeManager.currentTimeMillis();
            majorCompact(dataTable, afterLookbackAgeSCN);
            majorCompact(indexTable, afterLookbackAgeSCN);
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
        Admin admin = getUtility().getHBaseAdmin();
        admin.flush(table);
    }

    private void majorCompact(TableName table, long compactionRequestedSCN) throws Exception {
        Admin admin = getUtility().getHBaseAdmin();
        admin.majorCompact(table);
        long lastCompactionTimestamp;
        AdminProtos.GetRegionInfoResponse.CompactionState state = null;
        while ((lastCompactionTimestamp = admin.getLastMajorCompactionTimestamp(table)) < compactionRequestedSCN
            || (state = admin.getCompactionState(table)).
            equals(AdminProtos.GetRegionInfoResponse.CompactionState.MAJOR)){
            if (LOG.isTraceEnabled()) {
                LOG.trace("Last compaction time:" + lastCompactionTimestamp);
                LOG.trace("CompactionState: " + state);
            }
            Thread.sleep(100);
        }
    }

    public static void assertExplainPlan(Connection conn, String selectSql,
                                         String dataTableFullName, String indexTableFullName) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
        String actualExplainPlan = QueryUtil.getExplainPlan(rs);
        IndexToolIT.assertExplainPlan(false, actualExplainPlan, dataTableFullName, indexTableFullName);
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

    private void createTableAndIndexes(Connection conn, String dataTableName,
                                       String indexTableName) throws Exception {
        createTableAndIndexes(conn, dataTableName, indexTableName, 1);
    }

    private void createTableAndIndexes(Connection conn, String dataTableName,
                                       String indexTableName, int indexVersions) throws Exception {
        populateTable(dataTableName); // with two rows ('a', 'ab', 'abc', 'abcd') and ('b', 'bc', 'bcd', 'bcde')
        conn.createStatement().execute("CREATE INDEX " + indexTableName + "1 on " +
            dataTableName + " (val1) include (val2, val3)" +
            " VERSIONS=" + indexVersions);
        conn.createStatement().execute("CREATE INDEX " + indexTableName + "2 on " +
            dataTableName + " (val2) include (val1, val3)" +
            " VERSIONS=" + indexVersions);
        conn.commit();
    }

    private void populateTable(String tableName) throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String createSql = "create table " + tableName +
            " (id varchar(10) not null primary key, val1 varchar(10), " +
            "val2 varchar(10), val3 varchar(10))" + tableDDLOptions;
        conn.createStatement().execute(createSql);
        conn.createStatement().execute("upsert into " + tableName + " values ('a', 'ab', 'abc', 'abcd')");
        conn.commit();
        conn.createStatement().execute("upsert into " + tableName + " values ('b', 'bc', 'bcd', 'bcde')");
        conn.commit();
        conn.close();
    }
}
