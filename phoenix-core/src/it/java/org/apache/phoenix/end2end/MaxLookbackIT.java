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
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.EnvironmentEdge;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
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
    private static final int MAX_LOOKBACK_AGE = 15;
    private static final int ROWS_POPULATED = 2;
    public static final int WAIT_AFTER_TABLE_CREATION = 600000;
    private String tableDDLOptions;
    private StringBuilder optionBuilder;
    ManualEnvironmentEdge injectEdge;
    private int ttl;

    private class ManualEnvironmentEdge extends EnvironmentEdge {
        // Sometimes 0 ts might have a special value, so lets start with 1
        protected long value = 1L;

        public void setValue(long newValue) {
            value = newValue;
        }

        public void incValue(long addedValue) {
            value += addedValue;
        }

        @Override
        public long currentTime() {
            return this.value;
        }
    }

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB, Long.toString(0));
        props.put(ScanInfoUtil.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY, Integer.toString(MAX_LOOKBACK_AGE));
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
        EnvironmentEdgeManager.injectEdge(injectEdge);
    }

    @Test
    public void testTooLowSCNWithMaxLookbackAge() throws Exception {
        String dataTableName = generateUniqueName();
        createTable(dataTableName);
        //increase long enough to make sure we can find the syscat row for the table
        injectEdge.incValue(WAIT_AFTER_TABLE_CREATION);
        populateTable(dataTableName);
        long populateTime = EnvironmentEdgeManager.currentTimeMillis();
        injectEdge.incValue(MAX_LOOKBACK_AGE * 1000 + 1000);
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
            createTable(dataTableName);
            injectEdge.incValue(WAIT_AFTER_TABLE_CREATION);
            TableName dataTable = TableName.valueOf(dataTableName);
            populateTable(dataTableName);
            //make sure we're after the inserts have been committed
            injectEdge.incValue(1);
            long beforeDeleteSCN = EnvironmentEdgeManager.currentTimeMillis();
            injectEdge.incValue(10); //make sure we delete at a different ts
            Statement stmt = conn.createStatement();
            stmt.execute("DELETE FROM " + dataTableName + " WHERE " + " id = 'a'");
            Assert.assertEquals(1, stmt.getUpdateCount());
            conn.commit();
            //select stmt to get row we deleted
            String sql = String.format("SELECT * FROM %s WHERE id = 'a'", dataTableName);
            int rowsPlusDeleteMarker = ROWS_POPULATED;
            assertRowExistsAtSCN(getUrl(), sql, beforeDeleteSCN, true);
            flush(dataTable);
            assertRowExistsAtSCN(getUrl(), sql, beforeDeleteSCN, true);
            long beforeFirstCompactSCN = EnvironmentEdgeManager.currentTimeMillis();
            injectEdge.incValue(1); //new ts for major compaction
            majorCompact(dataTable, beforeFirstCompactSCN);
            assertRawRowCount(conn, dataTable, rowsPlusDeleteMarker);
            assertRowExistsAtSCN(getUrl(), sql, beforeDeleteSCN, true);
            //wait for the lookback time. After this compactions should purge the deleted row
            injectEdge.incValue(MAX_LOOKBACK_AGE * 1000);
            long beforeSecondCompactSCN = EnvironmentEdgeManager.currentTimeMillis();
            String notDeletedRowSql =
                String.format("SELECT * FROM %s WHERE id = 'b'", dataTableName);
            assertRowExistsAtSCN(getUrl(), notDeletedRowSql, beforeSecondCompactSCN, true);
            assertRawRowCount(conn, dataTable, ROWS_POPULATED);
            conn.createStatement().execute("upsert into " + dataTableName +
                " values ('c', 'cd', 'cde', 'cdef')");
            conn.commit();
            majorCompact(dataTable, beforeSecondCompactSCN);
            assertRawRowCount(conn, dataTable, ROWS_POPULATED);
            //deleted row should be gone, but not deleted row should still be there.
            assertRowExistsAtSCN(getUrl(), sql, beforeSecondCompactSCN, false);
            assertRowExistsAtSCN(getUrl(), notDeletedRowSql, beforeSecondCompactSCN, true);
            //1 deleted row should be gone
            assertRawRowCount(conn, dataTable, ROWS_POPULATED);
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
            createTable(dataTableName);
            //increment by 10 min to make sure we don't "look back" past table creation
            injectEdge.incValue(WAIT_AFTER_TABLE_CREATION);
            populateTable(dataTableName);
            injectEdge.incValue(1);
            long afterFirstInsertSCN = EnvironmentEdgeManager.currentTimeMillis();
            TableName dataTable = TableName.valueOf(dataTableName);
            assertTableHasTtl(conn, dataTable, ttl);
            //first make sure we inserted correctly
            String sql = String.format("SELECT val2 FROM %s WHERE id = 'a'", dataTableName);
          //  assertExplainPlan(conn, sql, dataTableName, fullIndexName);
            assertRowExistsAtSCN(getUrl(),sql, afterFirstInsertSCN, true);
            int originalRowCount = 2;
            assertRawRowCount(conn, dataTable, originalRowCount);
            //force a flush
            flush(dataTable);
            //flush shouldn't have changed it
            assertRawRowCount(conn, dataTable, originalRowCount);
                      // assertExplainPlan(conn, sql, dataTableName, fullIndexName);
            long timeToSleep = (MAX_LOOKBACK_AGE * 1000) -
                (EnvironmentEdgeManager.currentTimeMillis() - afterFirstInsertSCN);
            if (timeToSleep > 0) {
                injectEdge.incValue(timeToSleep);
                //Thread.sleep(timeToSleep);
            }
            //make sure it's still on disk
            assertRawRowCount(conn, dataTable, originalRowCount);
            injectEdge.incValue(1); //get a new timestamp for compaction
            majorCompact(dataTable, EnvironmentEdgeManager.currentTimeMillis());
            //nothing should have been purged by this major compaction
            assertRawRowCount(conn, dataTable, originalRowCount);
            //now wait the TTL
            timeToSleep = (ttl * 1000) -
                (EnvironmentEdgeManager.currentTimeMillis() - afterFirstInsertSCN);
            if (timeToSleep > 0) {
                injectEdge.incValue(timeToSleep);
            }
            //make sure that we can compact away the now-expired rows
            majorCompact(dataTable, EnvironmentEdgeManager.currentTimeMillis());
            //note that before HBase 1.4, we don't have HBASE-17956
            // and this will always return 0 whether it's still on-disk or not
            assertRawRowCount(conn, dataTable, 0);
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
            createTable(dataTableName);
            //increment by 10 min to make sure we don't "look back" past table creation
            injectEdge.incValue(WAIT_AFTER_TABLE_CREATION);
            populateTable(dataTableName);
            injectEdge.incValue(1); //increment by 1 so we can see our write
            long afterInsertSCN = EnvironmentEdgeManager.currentTimeMillis();
            //make sure table and index metadata is set up right for versions
            TableName dataTable = TableName.valueOf(dataTableName);
            assertTableHasVersions(conn, dataTable, versions);
            //check query optimizer is doing what we expect
            String dataTableSelectSql =
                String.format("SELECT val2 FROM %s WHERE id = 'a'", dataTableName);
            //make sure the data was inserted correctly in the first place
            assertRowHasExpectedValueAtSCN(getUrl(), dataTableSelectSql, afterInsertSCN, firstValue);
            //force first update to get a distinct ts
            injectEdge.incValue(1);
            updateColumn(conn, dataTableName, "id", "a", "val2", secondValue);
            injectEdge.incValue(1); //now make update visible
            long afterFirstUpdateSCN = EnvironmentEdgeManager.currentTimeMillis();
            //force second update to get a distinct ts
            injectEdge.incValue(1);
            updateColumn(conn, dataTableName, "id", "a", "val2", thirdValue);
            injectEdge.incValue(1);
            long afterSecondUpdateSCN = EnvironmentEdgeManager.currentTimeMillis();
            injectEdge.incValue(1);
            //check to make sure we can see all three versions at the appropriate times
            String[] allValues = {firstValue, secondValue, thirdValue};
            long[] allSCNs = {afterInsertSCN, afterFirstUpdateSCN, afterSecondUpdateSCN};
            assertMultiVersionLookbacks(dataTableSelectSql, allValues, allSCNs);
            flush(dataTable);
            //after flush, check to make sure we can see all three versions at the appropriate times
            assertMultiVersionLookbacks(dataTableSelectSql, allValues, allSCNs);
            majorCompact(dataTable, EnvironmentEdgeManager.currentTimeMillis());
            //after major compaction, check to make sure we can see all three versions
            // at the appropriate times
            assertMultiVersionLookbacks(dataTableSelectSql, allValues, allSCNs);
            injectEdge.incValue(MAX_LOOKBACK_AGE * 1000);
            long afterLookbackAgeSCN = EnvironmentEdgeManager.currentTimeMillis();
            majorCompact(dataTable, afterLookbackAgeSCN);
            //empty column, 1 version of val 1, 3 versions of val2, 1 version of val3 = 6
            assertRawCellCount(conn, dataTable, Bytes.toBytes("a"), 6);
            //empty column + 1 version each of val1,2 and 3 = 4
            assertRawCellCount(conn, dataTable, Bytes.toBytes("b"), 4);
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

    private void createTable(String tableName) throws Exception {
        try(Connection conn = DriverManager.getConnection(getUrl())) {
            String createSql = "create table " + tableName +
                " (id varchar(10) not null primary key, val1 varchar(10), " +
                "val2 varchar(10), val3 varchar(10))" + tableDDLOptions;
            conn.createStatement().execute(createSql);
            conn.commit();
        }
    }
    private void populateTable(String tableName) throws Exception {
        try(Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("upsert into " + tableName + " values ('a', 'ab', 'abc', 'abcd')");
            conn.commit();
            conn.createStatement().execute("upsert into " + tableName + " values ('b', 'bc', 'bcd', 'bcde')");
            conn.commit();
        }
    }
}
