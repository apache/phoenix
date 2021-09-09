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

import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_USE_STATS_FOR_PARALLELIZATION;
import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.stats.StatisticsUtil;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.EnvironmentEdge;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

/**
 * This class has tests for asserting the bytes and rows information exposed in the explain plan
 * when statistics are enabled.
 */
@Category(ParallelStatsEnabledTest.class)
public class ExplainPlanWithStatsEnabledIT extends ParallelStatsEnabledIT {

    private static String tableA;
    private static String tableB;
    private static String tableWithLargeGPWidth;
    private static String indexOnA;
    private static final long largeGpWidth = 2 * 1000 * 1000L;

    @BeforeClass
    public static synchronized void createTables() throws Exception {
        tableA = generateUniqueName();
        initDataAndStats(tableA, 20L);
        tableB = generateUniqueName();
        initDataAndStats(tableB, 20L);
        tableWithLargeGPWidth = generateUniqueName();
        initDataAndStats(tableWithLargeGPWidth, largeGpWidth);
        indexOnA = generateUniqueName();
        createIndex(indexOnA, tableA, 20);
    }

    private static void createIndex(String indexName, String table, long guidePostWidth)
            throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(
                "CREATE INDEX " + indexName + " ON " + table + " (c1.a) INCLUDE (c2.b) ");
            conn.createStatement().execute("UPDATE STATISTICS " + indexName);
        }
    }

    private static void initDataAndStats(String tableName, Long guidePostWidth) throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement()
                    .execute("CREATE TABLE " + tableName
                            + " (k INTEGER PRIMARY KEY, c1.a bigint, c2.b bigint)"
                            + " GUIDE_POSTS_WIDTH=" + guidePostWidth);
            conn.createStatement().execute("upsert into " + tableName + " values (100,1,3)");
            conn.createStatement().execute("upsert into " + tableName + " values (101,2,4)");
            conn.createStatement().execute("upsert into " + tableName + " values (102,2,4)");
            conn.createStatement().execute("upsert into " + tableName + " values (103,2,4)");
            conn.createStatement().execute("upsert into " + tableName + " values (104,2,4)");
            conn.createStatement().execute("upsert into " + tableName + " values (105,2,4)");
            conn.createStatement().execute("upsert into " + tableName + " values (106,2,4)");
            conn.createStatement().execute("upsert into " + tableName + " values (107,2,4)");
            conn.createStatement().execute("upsert into " + tableName + " values (108,2,4)");
            conn.createStatement().execute("upsert into " + tableName + " values (109,2,4)");
            conn.commit();
            conn.createStatement().execute("UPDATE STATISTICS " + tableName);
        }
    }

    private static Connection getTenantConnection(String tenantId) throws SQLException {
        String url = getUrl() + ';' + TENANT_ID_ATTRIB + '=' + tenantId;
        return DriverManager.getConnection(url);
    }

    @Test
    public void testBytesRowsForSelectWhenKeyOutOfRange() throws Exception {
        String sql = "SELECT * FROM " + tableA + " where k >= ?";
        List<Object> binds = Lists.newArrayList();
        binds.add(200);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 0L, info.estimatedBytes);
            assertEquals((Long) 0L, info.estimatedRows);
            assertTrue(info.estimateInfoTs > 0);
        }
    }

    @Test
    public void testBytesRowsForPointSelectWithLimitGreaterThanPointLookupSize() throws Exception {
        String sql = "SELECT * FROM " + tableA + " where k in (? ,?) limit 4";
        List<Object> binds = Lists.newArrayList();
        binds.add(103); binds.add(104);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 200L, info.estimatedBytes);
            assertEquals((Long) 2L, info.estimatedRows);
            assertEquals((Long) StatisticsUtil.NOT_STATS_BASED_TS, info.estimateInfoTs);
        }
    }

    @Test
    public void testBytesRowsForSelectWithLimit() throws Exception {
        String sql = "SELECT * FROM " + tableA + " where c1.a in (?,?) limit 3";
        String noIndexSQL = "SELECT /*+ NO_INDEX */ * FROM " + tableA + " where c1.a in (?,?) limit 3";
        List<Object> binds = Lists.newArrayList();
        binds.add(1); binds.add(2);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 264L, info.estimatedBytes);
            assertEquals((Long) 3L, info.estimatedRows);
            assertEquals((Long) StatisticsUtil.NOT_STATS_BASED_TS, info.estimateInfoTs);

            info = getByteRowEstimates(conn, noIndexSQL, binds);
            assertEquals((Long) 634L, info.estimatedBytes);
            assertEquals((Long) 10L, info.estimatedRows);
            assertTrue(info.estimateInfoTs > 0);
        }
    }

    @Test
    public void testBytesRowsForSelectWithLimitIgnored() throws Exception {
        String sql = "SELECT * FROM " + tableA + " where (c1.a > c2.b) limit 1";
        List<Object> binds = Lists.newArrayList();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            ResultSet rs = conn.createStatement().executeQuery(sql);
            assertFalse(rs.next());
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 390L, info.estimatedBytes);
            assertEquals((Long) 10L, info.estimatedRows);
            assertTrue(info.estimateInfoTs > 0);

        }
    }

    @Test
    public void testBytesRowsForSelectWhenKeyInRange() throws Exception {
        String sql = "SELECT * FROM " + tableB + " where k >= ?";
        List<Object> binds = Lists.newArrayList();
        binds.add(99);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 634L, info.estimatedBytes);
            assertEquals((Long) 10L, info.estimatedRows);
            assertTrue(info.estimateInfoTs > 0);
        }
    }

    @Test
    public void testBytesRowsForSelectOnIndex() throws Exception {
        String sql = "SELECT * FROM " + tableA + " where c1.a >= ?";
        List<Object> binds = Lists.newArrayList();
        binds.add(0);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                int paramIdx = 1;
                for (Object bind : binds) {
                    statement.setObject(paramIdx++, bind);
                }
                ResultSet rs = statement.executeQuery(sql);
                assertTrue(rs.next());
                assertEquals(100, rs.getInt(1));
                assertEquals(1, rs.getInt(2));
                assertEquals(3, rs.getInt(3));
                assertTrue(rs.next());
            }
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 390L, info.estimatedBytes);
            assertEquals((Long) 10L, info.estimatedRows);
            assertTrue(info.estimateInfoTs > 0);
        }
    }

    @Test
    public void testBytesRowsForUnion() throws Exception {
        String sql =
                "SELECT /*+ NO_INDEX */ * FROM " + tableA + " UNION ALL SELECT * FROM " + tableB;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Estimate info = getByteRowEstimates(conn, sql, Lists.newArrayList());
            assertEquals((Long) (2 * 634L), info.estimatedBytes);
            assertEquals((Long) (2 * 10L), info.estimatedRows);
            assertTrue(info.estimateInfoTs > 0);
        }
    }

    @Test
    public void testEstimatesForUnionWithTableWithLargeGpWidth() throws Exception {
        // For table with largeGpWidth, a guide post is generated that has the
        // byte size estimate of guide post width.
        String sql =
                "SELECT /*+ NO_INDEX */ * FROM " + tableA + " UNION ALL SELECT * FROM " + tableB
                        + " UNION ALL SELECT * FROM " + tableWithLargeGPWidth;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Estimate info = getByteRowEstimates(conn, sql, Lists.newArrayList());
            assertEquals((Long) (2 * 634 + largeGpWidth), info.estimatedBytes);
            assertTrue(info.estimateInfoTs > 0);
        }
    }

    @Test
    public void testBytesRowsForHashJoin() throws Exception {
        String sql =
                "SELECT ta.c1.a, ta.c2.b FROM " + tableA + " ta JOIN " + tableB
                        + " tb ON ta.k = tb.k";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Estimate info = getByteRowEstimates(conn, sql, Lists.newArrayList());
            assertEquals((Long) (634L), info.estimatedBytes);
            assertEquals((Long) (10L), info.estimatedRows);
            assertTrue(info.estimateInfoTs > 0);
        }
    }

    @Test
    public void testBytesRowsForSortMergeJoin() throws Exception {
        String sql =
                "SELECT /*+ NO_INDEX USE_SORT_MERGE_JOIN */ ta.c1.a, ta.c2.b FROM " + tableA
                        + " ta JOIN " + tableB + " tb ON ta.k = tb.k";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Estimate info = getByteRowEstimates(conn, sql, Lists.newArrayList());
            assertEquals((Long) (2 * 634L), info.estimatedBytes);
            assertEquals((Long) (2 * 10L), info.estimatedRows);
            assertTrue(info.estimateInfoTs > 0);
        }
    }

    @Test
    public void testBytesRowsForAggregateQuery() throws Exception {
        String sql = "SELECT count(*) FROM " + tableA + " where k >= ?";
        List<Object> binds = Lists.newArrayList();
        binds.add(99);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 634L, info.estimatedBytes);
            assertEquals((Long) 10L, info.estimatedRows);
            assertTrue(info.estimateInfoTs > 0);
        }
    }

    @Test
    public void testBytesRowsForUpsertSelectServerSide() throws Exception {
        String sql = "UPSERT INTO " + tableA + " SELECT * FROM " + tableB;
        List<Object> binds = Lists.newArrayList();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(true);
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 634L, info.estimatedBytes);
            assertEquals((Long) 10L, info.estimatedRows);
            assertTrue(info.estimateInfoTs > 0);
        }
    }

    @Test
    public void testBytesRowsForUpsertSelectClientSide() throws Exception {
        String sql = "UPSERT INTO " + tableB + " SELECT * FROM " + tableB;
        List<Object> binds = Lists.newArrayList();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(false);
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 634L, info.estimatedBytes);
            assertEquals((Long) 10L, info.estimatedRows);
            assertTrue(info.estimateInfoTs > 0);
        }
    }

    @Test
    public void testBytesRowsForUpsertValues() throws Exception {
        String sql = "UPSERT INTO " + tableA + " VALUES (?, ?, ?)";
        List<Object> binds = Lists.newArrayList();
        binds.add(99);
        binds.add(99);
        binds.add(99);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 0L, info.estimatedBytes);
            assertEquals((Long) 0L, info.estimatedRows);
            assertEquals((Long) 0L, info.estimateInfoTs);
        }
    }

    @Test
    public void testBytesRowsForDeleteServerSide() throws Exception {
        String sql = "DELETE FROM " + tableA + " where k >= ?";
        List<Object> binds = Lists.newArrayList();
        binds.add(99);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(true);
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 634L, info.estimatedBytes);
            assertEquals((Long) 10L, info.estimatedRows);
            assertTrue(info.estimateInfoTs > 0);
        }
    }

    @Test
    public void testBytesRowsForDeleteClientSideExecutedSerially() throws Exception {
        String sql = "DELETE FROM " + tableA + " where k >= ? LIMIT 2";
        List<Object> binds = Lists.newArrayList();
        binds.add(99);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(false);
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 200L, info.estimatedBytes);
            assertEquals((Long) 2L, info.estimatedRows);
            assertEquals((Long) StatisticsUtil.NOT_STATS_BASED_TS, info.estimateInfoTs);
        }
    }

    @Test
    public void testBytesRowsForPointDelete() throws Exception {
        String sql = "DELETE FROM " + tableA + " where k = ?";
        List<Object> binds = Lists.newArrayList();
        binds.add(100);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(false);
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 0L, info.estimatedBytes);
            assertEquals((Long) 0L, info.estimatedRows);
            assertEquals((Long) 0L, info.estimateInfoTs);
        }
    }

    @Test
    public void testBytesRowsForSelectExecutedSerially() throws Exception {
        String sql = "SELECT * FROM " + tableA + " LIMIT 2";
        List<Object> binds = Lists.newArrayList();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(false);
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 176L, info.estimatedBytes);
            assertEquals((Long) 2L, info.estimatedRows);
            assertEquals((Long) StatisticsUtil.NOT_STATS_BASED_TS, info.estimateInfoTs);
        }
    }

    public static class Estimate {
        final Long estimatedBytes;
        final Long estimatedRows;
        final Long estimateInfoTs;

        public Long getEstimatedBytes() {
            return estimatedBytes;
        }

        public Long getEstimatedRows() {
            return estimatedRows;
        }

        public Long getEstimateInfoTs() {
            return estimateInfoTs;
        }

        Estimate(Long rows, Long bytes, Long ts) {
            this.estimatedBytes = bytes;
            this.estimatedRows = rows;
            this.estimateInfoTs = ts;
        }
    }

    public static Estimate getByteRowEstimates(Connection conn, String sql, List<Object> bindValues)
            throws Exception {
        String explainSql = "EXPLAIN " + sql;
        Long estimatedBytes = null;
        Long estimatedRows = null;
        Long estimateInfoTs = null;
        try (PreparedStatement statement = conn.prepareStatement(explainSql)) {
            int paramIdx = 1;
            for (Object bind : bindValues) {
                statement.setObject(paramIdx++, bind);
            }
            ResultSet rs = statement.executeQuery(explainSql);
            rs.next();
            estimatedBytes =
                    (Long) rs.getObject(PhoenixRuntime.EXPLAIN_PLAN_ESTIMATED_BYTES_READ_COLUMN);
            estimatedRows =
                    (Long) rs.getObject(PhoenixRuntime.EXPLAIN_PLAN_ESTIMATED_ROWS_READ_COLUMN);
            estimateInfoTs =
                    (Long) rs.getObject(PhoenixRuntime.EXPLAIN_PLAN_ESTIMATE_INFO_TS_COLUMN);
        }
        return new Estimate(estimatedRows, estimatedBytes, estimateInfoTs);
    }

    @Test
    public void testSettingUseStatsForParallelizationProperty() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String table = generateUniqueName();
            String ddl =
                    "CREATE TABLE " + table
                            + " (PK1 INTEGER NOT NULL PRIMARY KEY, KV1 VARCHAR) USE_STATS_FOR_PARALLELIZATION = false";
            conn.createStatement().execute(ddl);
            assertUseStatsForQueryFlag(table, conn.unwrap(PhoenixConnection.class), false);

            ddl = "ALTER TABLE " + table + " SET USE_STATS_FOR_PARALLELIZATION = true";
            conn.createStatement().execute(ddl);
            assertUseStatsForQueryFlag(table, conn.unwrap(PhoenixConnection.class), true);

            table = generateUniqueName();
            ddl =
                    "CREATE TABLE " + table
                            + " (PK1 INTEGER NOT NULL PRIMARY KEY, KV1 VARCHAR) USE_STATS_FOR_PARALLELIZATION = false";
            conn.createStatement().execute(ddl);
            assertUseStatsForQueryFlag(table, conn.unwrap(PhoenixConnection.class), false);

            table = generateUniqueName();
            ddl = "CREATE TABLE " + table + " (PK1 INTEGER NOT NULL PRIMARY KEY, KV1 VARCHAR)";
            conn.createStatement().execute(ddl);

            // because we didn't set the property, PTable.useStatsForParallelization() should return
            // null
            assertUseStatsForQueryFlag(table, conn.unwrap(PhoenixConnection.class), null);
        }
    }

    private static void assertUseStatsForQueryFlag(String tableName, PhoenixConnection conn,
            Boolean expected) throws TableNotFoundException, SQLException {
        assertEquals(expected,
            conn.unwrap(PhoenixConnection.class).getMetaDataCache()
                    .getTableRef(new PTableKey(null, tableName)).getTable()
                    .useStatsForParallelization());
        String query =
                "SELECT USE_STATS_FOR_PARALLELIZATION FROM SYSTEM.CATALOG WHERE TABLE_NAME = ? AND COLUMN_NAME IS NULL AND COLUMN_FAMILY IS NULL AND TENANT_ID IS NULL";
        PreparedStatement stmt = conn.prepareStatement(query);
        stmt.setString(1, tableName);
        ResultSet rs = stmt.executeQuery();
        rs.next();
        boolean b = rs.getBoolean(1);
        if (expected == null) {
            assertTrue(rs.wasNull());
        } else {
            assertEquals(expected, b);
        }
    }

    @Test
    public void testBytesRowsForSelectOnTenantViews() throws Exception {
        String tenant1View = generateUniqueName();
        String tenant2View = generateUniqueName();
        String tenant3View = generateUniqueName();
        String tenant4View = generateUniqueName();
        String multiTenantBaseTable = generateUniqueName();
        String tenant1 = "tenant1";
        String tenant2 = "tenant2";
        String tenant3 = "tenant3";
        String tenant4 = "tenant4";
        MyClock clock = new MyClock(1000);
        
        createMultitenantTableAndViews(tenant1View, tenant2View, tenant3View, tenant4View, tenant1, tenant2,
            tenant3, tenant4, multiTenantBaseTable, clock);
        // query the entire multitenant table
        String sql = "SELECT * FROM " + multiTenantBaseTable + " WHERE ORGID >= ?";
        List<Object> binds = Lists.newArrayList();
        binds.add("tenant0");
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 681L, info.estimatedBytes);
            assertEquals((Long) 10L, info.estimatedRows);
            assertNull(info.estimateInfoTs); // unknown/null because region (* - tenant1) has no guideposts
        }
        binds.clear();
        long prevTenantBytes;
        // query tenant1 view
        try (Connection conn = getTenantConnection(tenant1)) {
            sql = "SELECT * FROM " + tenant1View;
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 119L, info.estimatedBytes);
            assertEquals((Long) 2L, info.estimatedRows);
            assertNull(info.estimateInfoTs); // unknown/null because scan occurs in first region because of start key versus slightly larger region boundary
        }
        // query tenant2 view
        try (Connection conn = getTenantConnection(tenant2)) {
            sql = "SELECT * FROM " + tenant2View;
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) (prevTenantBytes=119L), info.estimatedBytes);
            assertEquals((Long) 2L, info.estimatedRows);
            assertEquals((Long) clock.currentTime(), info.estimateInfoTs);
        }
        // query tenant3 view
        try (Connection conn = getTenantConnection(tenant3)) {
            sql = "SELECT * FROM " + tenant3View;
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 443L, info.estimatedBytes);
            assertEquals((Long) 6L, info.estimatedRows);
            assertEquals((Long) clock.currentTime(), info.estimateInfoTs);
        }
        /*
         * Now we will add some rows to tenant1view an run update stats on it. We will do this after
         * advancing our clock by 1000 seconds. This way we can check that only the region for
         * tenant1 will have updated guidepost with the new timestamp.
         */
        long prevGuidePostTimestamp = clock.currentTime();
        clock.advanceTime(1000);
        try {
            EnvironmentEdgeManager.injectEdge(clock);
            // Update tenant1 view
            try (Connection conn = getTenantConnection(tenant2)) {
                // upsert a few rows for tenantView
                clock.setAdvance(false);
                conn.createStatement()
                        .executeUpdate("UPSERT INTO " + tenant2View + " VALUES (11, 11, 11)");
                conn.createStatement()
                        .executeUpdate("UPSERT INTO " + tenant2View + " VALUES (12, 12, 12)");
                conn.createStatement()
                        .executeUpdate("UPSERT INTO " + tenant2View + " VALUES (13, 13, 13)");
                conn.createStatement()
                        .executeUpdate("UPSERT INTO " + tenant2View + " VALUES (14, 14, 14)");
                conn.createStatement()
                        .executeUpdate("UPSERT INTO " + tenant2View + " VALUES (15, 15, 15)");
                conn.createStatement()
                        .executeUpdate("UPSERT INTO " + tenant2View + " VALUES (16, 16, 16)");
                conn.commit();
                // run update stats on the tenantView
                conn.createStatement().executeUpdate("UPDATE STATISTICS " + tenant2View);
                // get estimates now and check if they were updated as expected
                sql = "SELECT * FROM " + tenant2View;
                Estimate info = getByteRowEstimates(conn, sql, Collections.emptyList());
                assertTrue(info.estimatedBytes > prevTenantBytes);
                assertEquals((Long) 8L, info.estimatedRows);
                assertEquals((Long) clock.currentTime(), info.estimateInfoTs);
            }
        } finally {
            EnvironmentEdgeManager.reset();
        }
        // Now check estimates again for tenantView1 and tenantView3. They should stay the same.
        try (Connection conn = getTenantConnection(tenant1)) {
            sql = "SELECT * FROM " + tenant1View;
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 119L, info.estimatedBytes);
            assertEquals((Long) 2L, info.estimatedRows);
            assertNull(info.estimateInfoTs);
        }
        try (Connection conn = getTenantConnection(tenant3)) {
            sql = "SELECT * FROM " + tenant3View;
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 443L, info.estimatedBytes);
            assertEquals((Long) 6L, info.estimatedRows);
            assertEquals((Long) prevGuidePostTimestamp, info.estimateInfoTs);
        }
        /*
         * Now let's query the base table and see estimates. Because we use the minimum timestamp
         * for all guideposts that we will be scanning, the timestamp for the estimate info for this
         * query should be prevGuidePostTimestamp.
         */
        binds.clear();
        binds.add("tenant0");
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            sql = "SELECT * FROM " + multiTenantBaseTable + " WHERE ORGID >= ?";
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 1167L, info.estimatedBytes);
            assertEquals((Long) 16L, info.estimatedRows);
            assertNull(info.estimateInfoTs);
        }
        // query tenant4 view
        binds.clear();
        try (Connection conn = getTenantConnection(tenant4)) {
            sql = "SELECT * FROM " + tenant4View;
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) (prevTenantBytes=0L), info.estimatedBytes);
            assertEquals((Long) 0L, info.estimatedRows);
            assertNull(info.estimateInfoTs); // Unknown b/c second region of tenant4 has no gps
        }
        clock.advanceTime(1000);
        try {
            EnvironmentEdgeManager.injectEdge(clock);
            // Update tenant4 view
            try (Connection conn = getTenantConnection(tenant4)) {
                // upsert a few rows for tenantView
                conn.createStatement()
                    .executeUpdate("UPSERT INTO " + tenant4View + " VALUES (6, 17,17)");
                conn.createStatement()
                    .executeUpdate("UPSERT INTO " + tenant4View + " VALUES (7, 17,17)");
                conn.commit();
                // run update stats on the tenantView
                conn.createStatement().executeUpdate("UPDATE STATISTICS " + tenant4View);
                // get estimates now and check if they were updated as expected
                sql = "SELECT * FROM " + tenant4View;
                Estimate info = getByteRowEstimates(conn, sql, Collections.emptyList());
                assertTrue(info.estimatedBytes > prevTenantBytes);
                assertEquals((Long) 119L, info.estimatedBytes);
                assertEquals((Long) 2L, info.estimatedRows);
                assertEquals((Long) clock.currentTime(), info.estimateInfoTs);
                sql = "SELECT * FROM " + tenant4View + " WHERE pk2 >= 6";
                info = getByteRowEstimates(conn, sql, Collections.emptyList());
                assertEquals((Long) 119L, info.estimatedBytes);
                assertEquals((Long) 2L, info.estimatedRows);
                assertEquals((Long) clock.currentTime(), info.estimateInfoTs);
            }
        } finally {
            EnvironmentEdgeManager.reset();
        }
    }

    @Test // See https://issues.apache.org/jira/browse/PHOENIX-4287
    public void testEstimatesForAggregateQueries() throws Exception {
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            int guidePostWidth = 20;
            String ddl =
                    "CREATE TABLE " + tableName + " (k INTEGER PRIMARY KEY, a bigint, b bigint)"
                            + " GUIDE_POSTS_WIDTH=" + guidePostWidth + " SPLIT ON (102, 105, 108)";
            conn.createStatement().execute(ddl);
            conn.createStatement().execute("upsert into " + tableName + " values (100,1,3)");
            conn.createStatement().execute("upsert into " + tableName + " values (101,2,4)");
            conn.createStatement().execute("upsert into " + tableName + " values (102,2,4)");
            conn.createStatement().execute("upsert into " + tableName + " values (103,2,4)");
            conn.createStatement().execute("upsert into " + tableName + " values (104,2,4)");
            conn.createStatement().execute("upsert into " + tableName + " values (105,2,4)");
            conn.createStatement().execute("upsert into " + tableName + " values (106,2,4)");
            conn.createStatement().execute("upsert into " + tableName + " values (107,2,4)");
            conn.createStatement().execute("upsert into " + tableName + " values (108,2,4)");
            conn.createStatement().execute("upsert into " + tableName + " values (109,2,4)");
            conn.commit();
            conn.createStatement().execute("UPDATE STATISTICS " + tableName + "");
        }
        List<Object> binds = Lists.newArrayList();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String sql = "SELECT COUNT(*) " + " FROM " + tableName;
            // We don't have the use stats for parallelization property
            // set on the table. In this case, we end up defaulting to the
            // value set in config which is true.
            ResultSet rs = conn.createStatement().executeQuery(sql);
            // stats are being used for parallelization. So number of scans is higher.
            assertEquals(11, rs.unwrap(PhoenixResultSet.class).getStatement().getQueryPlan()
                    .getScans().get(0).size());
            assertTrue(rs.next());
            assertEquals(10, rs.getInt(1));
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 10L, info.getEstimatedRows());
            assertTrue(info.getEstimateInfoTs() > 0);

            // Now, let's disable USE_STATS_FOR_PARALLELIZATION on the table
            conn.createStatement().execute(
                "ALTER TABLE " + tableName + " SET USE_STATS_FOR_PARALLELIZATION = " + false);
            rs = conn.createStatement().executeQuery(sql);
            // stats are not being used for parallelization. So number of scans is lower.
            assertEquals(4, rs.unwrap(PhoenixResultSet.class).getStatement().getQueryPlan()
                    .getScans().get(0).size());
            assertTrue(rs.next());
            assertEquals(10, rs.getInt(1));
            info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 10L, info.getEstimatedRows());
            assertTrue(info.getEstimateInfoTs() > 0);

            // assert that the aggregate query on view also works correctly
            String viewName = "V_" + generateUniqueName();
            conn.createStatement().execute("CREATE VIEW " + viewName + " AS SELECT * FROM "
                    + tableName + " USE_STATS_FOR_PARALLELIZATION = false");
            sql = "SELECT COUNT(*) FROM " + viewName;
            rs = conn.createStatement().executeQuery(sql);
            // stats are not being used for parallelization. So number of scans is lower.
            assertEquals(4, rs.unwrap(PhoenixResultSet.class).getStatement().getQueryPlan()
                    .getScans().get(0).size());
            assertTrue(rs.next());
            assertEquals(10, rs.getInt(1));
            info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 10L, info.getEstimatedRows());
            assertTrue(info.getEstimateInfoTs() > 0);

            // Now let's make sure that when using stats for parallelization, our estimates
            // and query results stay the same for view and base table
            conn.createStatement().execute(
                "ALTER TABLE " + tableName + " SET USE_STATS_FOR_PARALLELIZATION=true");
            sql = "SELECT COUNT(*) FROM " + tableName;
            // query the table
            rs = conn.createStatement().executeQuery(sql);
            // stats are being used for parallelization. So number of scans is higher.
            assertEquals(11, rs.unwrap(PhoenixResultSet.class).getStatement().getQueryPlan()
                    .getScans().get(0).size());
            assertTrue(rs.next());
            assertEquals(10, rs.getInt(1));
            info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 10L, info.getEstimatedRows());
            assertTrue(info.getEstimateInfoTs() > 0);

            conn.createStatement()
                    .execute("ALTER VIEW " + viewName + " SET USE_STATS_FOR_PARALLELIZATION=true");
            sql = "SELECT COUNT(*) FROM " + viewName;
            // query the view
            rs = conn.createStatement().executeQuery(sql);
            // stats are not being used for parallelization. So number of scans is higher.
            assertEquals(11, rs.unwrap(PhoenixResultSet.class).getStatement().getQueryPlan()
                    .getScans().get(0).size());
            assertTrue(rs.next());
            assertEquals(10, rs.getInt(1));
            info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 10L, info.getEstimatedRows());
            assertTrue(info.getEstimateInfoTs() > 0);
        }
    }

    @Test
    public void testSelectQueriesWithStatsForParallelizationOff() throws Exception {
        testSelectQueriesWithFilters(false);
    }

    @Test
    public void testSelectQueriesWithStatsForParallelizationOn() throws Exception {
        testSelectQueriesWithFilters(true);
    }

    private void testSelectQueriesWithFilters(boolean useStatsForParallelization) throws Exception {
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            int guidePostWidth = 20;
            String ddl =
                    "CREATE TABLE " + tableName + " (k INTEGER PRIMARY KEY, a bigint, b bigint)"
                            + " GUIDE_POSTS_WIDTH=" + guidePostWidth
                            + ", USE_STATS_FOR_PARALLELIZATION=" + useStatsForParallelization + " SPLIT ON (102, 105, 108)";
            conn.createStatement().execute(ddl);
            conn.createStatement().execute("upsert into " + tableName + " values (100,100,3)");
            conn.createStatement().execute("upsert into " + tableName + " values (101,101,4)");
            conn.createStatement().execute("upsert into " + tableName + " values (102,102,4)");
            conn.createStatement().execute("upsert into " + tableName + " values (103,103,4)");
            conn.createStatement().execute("upsert into " + tableName + " values (104,104,4)");
            conn.createStatement().execute("upsert into " + tableName + " values (105,105,4)");
            conn.createStatement().execute("upsert into " + tableName + " values (106,106,4)");
            conn.createStatement().execute("upsert into " + tableName + " values (107,107,4)");
            conn.createStatement().execute("upsert into " + tableName + " values (108,108,4)");
            conn.createStatement().execute("upsert into " + tableName + " values (109,109,4)");
            conn.commit();
            conn.createStatement().execute("UPDATE STATISTICS " + tableName + "");
        }
        List<Object> binds = Lists.newArrayList();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            // query whose start key is before any data
            String sql = "SELECT a FROM " + tableName + " WHERE K >= 99";
            ResultSet rs = conn.createStatement().executeQuery(sql);
            int i = 0;
            int numRows = 10;
            while (rs.next()) {
                assertEquals(100 + i, rs.getInt(1));
                i++;
            }
            assertEquals(numRows, i);
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 10L, info.getEstimatedRows());
            assertEquals((Long) 720L, info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);

            // query whose start key is after any data
            sql = "SELECT a FROM " + tableName + " WHERE K >= 110";
            rs = conn.createStatement().executeQuery(sql);
            assertFalse(rs.next());
            info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 0L, info.getEstimatedRows());
            assertEquals((Long) 0L, info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);

            // Query whose end key is before any data
            sql = "SELECT a FROM " + tableName + " WHERE K <= 98";
            rs = conn.createStatement().executeQuery(sql);
            assertFalse(rs.next());
            info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 0L, info.getEstimatedRows());
            assertEquals((Long) 0L, info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);

            // Query whose end key is after any data. In this case, we return the estimate as
            // scanning all the guide posts.
            sql = "SELECT a FROM " + tableName + " WHERE K <= 110";
            rs = conn.createStatement().executeQuery(sql);
            i = 0;
            numRows = 10;
            while (rs.next()) {
                assertEquals(100 + i, rs.getInt(1));
                i++;
            }
            assertEquals(numRows, i);
            info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 10L, info.getEstimatedRows());
            assertEquals((Long) 720L, info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);

            // Query whose start key and end key is before any data. In this case,
            // we return the estimate as scanning the first guide post
            sql = "SELECT a FROM " + tableName + " WHERE K <= 90 AND K >= 80";
            rs = conn.createStatement().executeQuery(sql);
            assertFalse(rs.next());
            info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 0L, info.getEstimatedRows());
            assertEquals((Long) 0L, info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);

            // Query whose start key and end key is after any data. In this case, we return the
            // estimate as
            // scanning no guide post
            sql = "SELECT a FROM " + tableName + " WHERE K <= 130 AND K >= 120";
            rs = conn.createStatement().executeQuery(sql);
            assertFalse(rs.next());
            info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 0L, info.getEstimatedRows());
            assertEquals((Long) 0L, info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);

            // Query whose start key is before and end key is between data. In this case, we return
            // the estimate as scanning no guide post
            sql = "SELECT a FROM " + tableName + " WHERE K <= 102 AND K >= 90";
            rs = conn.createStatement().executeQuery(sql);
            i = 0;
            numRows = 3;
            while (rs.next()) {
                assertEquals(100 + i, rs.getInt(1));
                i++;
            }
            info = getByteRowEstimates(conn, sql, binds);
            // Depending on the guidepost boundary, this estimate
            // can be slightly off. It's called estimate for a reason.
            assertEquals((Long) 3L, info.getEstimatedRows());
            assertEquals((Long) 160L, info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);
            // Query whose start key is between and end key is after data.
            sql = "SELECT a FROM " + tableName + " WHERE K <= 120 AND K >= 100";
            rs = conn.createStatement().executeQuery(sql);
            i = 0;
            numRows = 10;
            while (rs.next()) {
                assertEquals(100 + i, rs.getInt(1));
                i++;
            }
            info = getByteRowEstimates(conn, sql, binds);
            // Depending on the guidepost boundary, this estimate
            // can be slightly off. It's called estimate for a reason.
            assertEquals((Long) 10L, info.getEstimatedRows());
            assertEquals((Long) 720L, info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);
            // Query whose start key and end key are both between data.
            sql = "SELECT a FROM " + tableName + " WHERE K <= 109 AND K >= 100";
            rs = conn.createStatement().executeQuery(sql);
            i = 0;
            numRows = 10;
            while (rs.next()) {
                assertEquals(100 + i, rs.getInt(1));
                i++;
            }
            info = getByteRowEstimates(conn, sql, binds);
            // Depending on the guidepost boundary, this estimate
            // can be slightly off. It's called estimate for a reason.
            assertEquals((Long) 10L, info.getEstimatedRows());
            assertEquals((Long) 720L, info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);
        }
    }

    private static void createMultitenantTableAndViews(String tenant1View, String tenant2View,
            String tenant3View, String tenant4View, String tenant1, String tenant2, String tenant3, String tenant4,
            String multiTenantTable, MyClock clock) throws Exception {
        byte[][] splits =
                new byte[][] {
                    ByteUtil.concat(Bytes.toBytes(tenant1),PInteger.INSTANCE.toBytes(1)),
                    ByteUtil.concat(Bytes.toBytes(tenant2),PInteger.INSTANCE.toBytes(1)),
                    ByteUtil.concat(Bytes.toBytes(tenant3),PInteger.INSTANCE.toBytes(1)),
                    ByteUtil.concat(Bytes.toBytes(tenant4),PInteger.INSTANCE.toBytes(6)),
                    };
        String ddl =
                "CREATE TABLE " + multiTenantTable
                        + " (orgId CHAR(7) NOT NULL, pk2 integer NOT NULL, c1.a bigint, c2.b bigint CONSTRAINT PK PRIMARY KEY "
                        + "(ORGID, PK2)) MULTI_TENANT=true, GUIDE_POSTS_WIDTH=2";
        // Use our own clock to get rows created with our controlled timestamp
        try {
            EnvironmentEdgeManager.injectEdge(clock);
            try (Connection conn = DriverManager.getConnection(getUrl())) {
                PreparedStatement stmt = conn.prepareStatement(ddl + " SPLIT ON (?,?,?,?)");
                for (int i = 0; i < splits.length; i++) {
                    stmt.setBytes(i+1, splits[i]);
                }
                stmt.executeUpdate();
                clock.advanceTime(1000);
                /**
                 * Insert 2 rows each for tenant1 and tenant2 and 6 rows for tenant3
                 */
                conn.createStatement().execute(
                    "upsert into " + multiTenantTable + " values ('" + tenant1 + "',1,1,1)");
                conn.createStatement().execute(
                    "upsert into " + multiTenantTable + " values ('" + tenant1 + "',2,2,2)");
                conn.createStatement().execute(
                    "upsert into " + multiTenantTable + " values ('" + tenant2 + "',1,3,3)");
                conn.createStatement().execute(
                    "upsert into " + multiTenantTable + " values ('" + tenant2 + "',2,4,4)");
                conn.createStatement().execute(
                    "upsert into " + multiTenantTable + " values ('" + tenant3 + "',1,5,5)");
                conn.createStatement().execute(
                    "upsert into " + multiTenantTable + " values ('" + tenant3 + "',2,6,6)");
                conn.createStatement().execute(
                    "upsert into " + multiTenantTable + " values ('" + tenant3 + "',3,7,7)");
                conn.createStatement().execute(
                    "upsert into " + multiTenantTable + " values ('" + tenant3 + "',4,8,8)");
                conn.createStatement().execute(
                    "upsert into " + multiTenantTable + " values ('" + tenant3 + "',5,9,9)");
                conn.createStatement().execute(
                    "upsert into " + multiTenantTable + " values ('" + tenant3 + "',6,10,10)");
                conn.commit();
            }
            clock.setAdvance(false);
            try (Connection conn = getTenantConnection(tenant1)) {
                conn.createStatement().execute(
                    "CREATE VIEW " + tenant1View + " AS SELECT * FROM " + multiTenantTable);
                conn.createStatement().execute("UPDATE STATISTICS " + tenant1View);
            }
            try (Connection conn = getTenantConnection(tenant2)) {
                conn.createStatement().execute(
                    "CREATE VIEW " + tenant2View + " AS SELECT * FROM " + multiTenantTable);
                conn.createStatement().execute("UPDATE STATISTICS " + tenant2View);
            }
            try (Connection conn = getTenantConnection(tenant3)) {
                conn.createStatement().execute(
                    "CREATE VIEW " + tenant3View + " AS SELECT * FROM " + multiTenantTable);
                conn.createStatement().execute("UPDATE STATISTICS " + tenant3View);
            }
            try (Connection conn = getTenantConnection(tenant4)) {
                conn.createStatement().execute(
                    "CREATE VIEW " + tenant4View + " AS SELECT * FROM " + multiTenantTable);
            }
        } finally {
            EnvironmentEdgeManager.reset();
        }
    }

    private static class MyClock extends EnvironmentEdge {
        public volatile long time;
        private boolean shouldAdvance = true;

        public MyClock(long time) {
            this.time = time;
        }

        @Override
        public long currentTime() {
            if(shouldAdvance) {
                return time++;
            } else {
                return time;
            }
        }
        public void setAdvance(boolean val) {
            shouldAdvance = val;
        }
        public void advanceTime(long t) {
            this.time += t;
        }
    }

    @Test
    public void testPartialStatsForTenantViews() throws Exception {
        String tenant1View = generateUniqueName();
        String tenant2View = generateUniqueName();
        String multiTenantTable = generateUniqueName();
        String tenantId1 = "00Dabcdetenant1";
        String tenantId2 = "00Dabcdetenant2";

        String ddl =
                "CREATE TABLE " + multiTenantTable
                        + " (orgId CHAR(15) NOT NULL, pk2 CHAR(3) NOT NULL, a bigint, b bigint CONSTRAINT PK PRIMARY KEY "
                        + "(ORGID, PK2)) MULTI_TENANT=true, GUIDE_POSTS_WIDTH=20";
        createTestTable(getUrl(), ddl, null, null);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            // split such that some data for view2 resides on region of view1
            try (Admin admin =
                    conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                byte[] splitKey = Bytes.toBytes("00Dabcdetenant200B");
                admin.split(TableName.valueOf(multiTenantTable), splitKey);
            }

            /**
             * Insert 2 rows for tenant1 and 6 for tenant2
             */
            conn.createStatement().execute(
                "upsert into " + multiTenantTable + " values ('" + tenantId1 + "','00A',1,1)");
            conn.createStatement().execute(
                "upsert into " + multiTenantTable + " values ('" + tenantId1 + "','00B',2,2)");
            conn.createStatement().execute(
                "upsert into " + multiTenantTable + " values ('" + tenantId2 + "','00A',3,3)");
            // We split at tenant2 + 00B. So the following rows will reside in a different region
            conn.createStatement().execute(
                "upsert into " + multiTenantTable + " values ('" + tenantId2 + "','00B',4,4)");
            conn.createStatement().execute(
                "upsert into " + multiTenantTable + " values ('" + tenantId2 + "','00C',5,5)");
            conn.createStatement().execute(
                "upsert into " + multiTenantTable + " values ('" + tenantId2 + "','00D',6,6)");
            conn.createStatement().execute(
                "upsert into " + multiTenantTable + " values ('" + tenantId2 + "','00E',7,7)");
            conn.createStatement().execute(
                "upsert into " + multiTenantTable + " values ('" + tenantId2 + "','00F',8,8)");
            conn.commit();
        }
        try (Connection conn = getTenantConnection(tenantId1)) {
            conn.createStatement().execute(
                "CREATE VIEW " + tenant1View + " AS SELECT * FROM " + multiTenantTable);
        }
        try (Connection conn = getTenantConnection(tenantId2)) {
            conn.createStatement().execute(
                "CREATE VIEW " + tenant2View + " AS SELECT * FROM " + multiTenantTable);
        }
        String sql = "";
        List<Object> binds = Lists.newArrayList();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            /*
             * I have seen compaction running and generating stats for the second region of
             * tenant2View So let's disable compaction on the table, delete any stats we have
             * collected in SYSTEM.STATS table, clear cache and run update stats to make sure our
             * test gets a deterministic setup.
             */
            String disableCompaction =
                    "ALTER TABLE " + multiTenantTable + " SET COMPACTION_ENABLED = false";
            conn.createStatement().executeUpdate(disableCompaction);
            String delete =
                    "DELETE FROM SYSTEM.STATS WHERE PHYSICAL_NAME = '" + multiTenantTable + "'";
            conn.createStatement().executeUpdate(delete);
            conn.commit();
            conn.unwrap(PhoenixConnection.class).getQueryServices().clearCache();
        }
        // Now let's run update stats on tenant1View
        try (Connection conn = getTenantConnection(tenantId1)) {
            conn.createStatement().execute("UPDATE STATISTICS " + tenant1View);
        }
        // query tenant2 view
        try (Connection conn = getTenantConnection(tenantId2)) {
            sql = "SELECT * FROM " + tenant2View;

            Estimate info = getByteRowEstimates(conn, sql, binds);
            /*
             * Because we ran update stats only for tenant1View, there is only partial guidepost
             * info available for tenant2View.
             */
            assertEquals((Long) 1L, info.estimatedRows);
            // ok now run update stats for tenant2 view
            conn.createStatement().execute("UPDATE STATISTICS " + tenant2View);
            /*
             * And now, let's recheck our estimate info. We should have all the rows of view2
             * available now.
             */
            info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 6L, info.estimatedRows);
        }
    }

    @Test
    public void testIndexesUseStatsIfOnForParentTable() throws Exception {
        testIndexesInheritUseStatsPropFromParentTable(true);
    }

    @Test
    public void testIndexesDontUseStatsIfOffForParentTable() throws Exception {
        testIndexesInheritUseStatsPropFromParentTable(false);
    }

    private void testIndexesInheritUseStatsPropFromParentTable(boolean useStats) throws Exception {
        String baseTable = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String ddl =
                    "CREATE TABLE " + baseTable
                            + " (k INTEGER PRIMARY KEY, a bigint, b bigint, c bigint) GUIDE_POSTS_WIDTH=20, USE_STATS_FOR_PARALLELIZATION="
                            + useStats;
            conn.createStatement().execute(ddl);
            conn.createStatement().execute("upsert into " + baseTable + " values (100,1,1,1)");
            conn.createStatement().execute("upsert into " + baseTable + " values (101,2,2,2)");
            conn.createStatement().execute("upsert into " + baseTable + " values (102,3,3,3)");
            conn.createStatement().execute("upsert into " + baseTable + " values (103,4,4,4)");
            conn.createStatement().execute("upsert into " + baseTable + " values (104,5,5,5)");
            conn.createStatement().execute("upsert into " + baseTable + " values (105,6,6,6)");
            conn.createStatement().execute("upsert into " + baseTable + " values (106,7,7,7)");
            conn.createStatement().execute("upsert into " + baseTable + " values (107,8,8,8)");
            conn.createStatement().execute("upsert into " + baseTable + " values (108,9,9,9)");
            conn.createStatement().execute("upsert into " + baseTable + " values (109,10,10,10)");
            conn.commit();

            // Create global index on base table
            String globalIndex = "GI_" + generateUniqueName();
            ddl = "CREATE INDEX " + globalIndex + " ON " + baseTable + " (a) INCLUDE (b) ";
            conn.createStatement().execute(ddl);

            // Create local index on base table
            String localIndex = "LI_" + generateUniqueName();
            ddl = "CREATE LOCAL INDEX " + localIndex + " ON " + baseTable + " (b) INCLUDE (c) ";
            conn.createStatement().execute(ddl);

            // Create a view and an index on it
            String view = "V_" + generateUniqueName();
            ddl =
                    "CREATE VIEW " + view + " AS SELECT * FROM " + baseTable
                            + " USE_STATS_FOR_PARALLELIZATION=" + useStats;
            conn.createStatement().execute(ddl);
            String viewIndex = "VI_" + generateUniqueName();
            ddl = "CREATE INDEX " + viewIndex + " ON " + view + " (b)";
            conn.createStatement().execute(ddl);

            // collect stats for all
            conn.createStatement().execute("UPDATE STATISTICS " + baseTable);

            // query against the base table
            String query = "SELECT /*+ NO_INDEX */ COUNT(*) FROM " + baseTable;
            PhoenixResultSet rs =
                    conn.createStatement().executeQuery(query).unwrap(PhoenixResultSet.class);
            // assert query is against base table
            assertEquals(baseTable,
                rs.getStatement().getQueryPlan().getTableRef().getTable().getName().getString());
            assertEquals(useStats ? 11 : 1, rs.unwrap(PhoenixResultSet.class).getStatement()
                    .getQueryPlan().getScans().get(0).size());

            // query against the global index
            query = "SELECT B FROM " + baseTable + " WHERE A > 0";
            rs = conn.createStatement().executeQuery(query).unwrap(PhoenixResultSet.class);
            // assert query is against global index
            assertEquals(globalIndex, rs.unwrap(PhoenixResultSet.class).getStatement()
                    .getQueryPlan().getTableRef().getTable().getName().getString());
            assertEquals(useStats ? 11 : 1, rs.unwrap(PhoenixResultSet.class).getStatement()
                    .getQueryPlan().getScans().get(0).size());

            // query against the local index
            query = "SELECT C FROM " + baseTable + " WHERE B > 0";
            rs = conn.createStatement().executeQuery(query).unwrap(PhoenixResultSet.class);
            // assert query is against global index
            assertEquals(localIndex, rs.unwrap(PhoenixResultSet.class).getStatement().getQueryPlan()
                    .getTableRef().getTable().getName().getString());
            assertEquals(useStats ? 11 : 1, rs.unwrap(PhoenixResultSet.class).getStatement()
                    .getQueryPlan().getScans().get(0).size());

            // query against the view
            query = "SELECT * FROM " + view;
            rs = conn.createStatement().executeQuery(query).unwrap(PhoenixResultSet.class);
            // assert query is against view
            assertEquals(view, rs.unwrap(PhoenixResultSet.class).getStatement().getQueryPlan()
                    .getTableRef().getTable().getName().getString());
            assertEquals(useStats ? 11 : 1, rs.unwrap(PhoenixResultSet.class).getStatement()
                    .getQueryPlan().getScans().get(0).size());

            // query against the view index
            query = "SELECT 1 FROM " + view + " WHERE B > 0";
            rs = conn.createStatement().executeQuery(query).unwrap(PhoenixResultSet.class);
            // assert query is against viewIndex
            assertEquals(viewIndex, rs.unwrap(PhoenixResultSet.class).getStatement().getQueryPlan()
                    .getTableRef().getTable().getName().getString());
            assertEquals(useStats ? 11 : 1, rs.unwrap(PhoenixResultSet.class).getStatement()
                    .getQueryPlan().getScans().get(0).size());

            // flip the use stats property on the view and see if view index picks it up
            conn.createStatement().execute(
                "ALTER VIEW " + view + " SET USE_STATS_FOR_PARALLELIZATION=" + !useStats);

            // query against the view
            query = "SELECT * FROM " + view;
            rs = conn.createStatement().executeQuery(query).unwrap(PhoenixResultSet.class);
            // assert query is against view
            assertEquals(view, rs.unwrap(PhoenixResultSet.class).getStatement().getQueryPlan()
                    .getTableRef().getTable().getName().getString());
            assertEquals(!useStats ? 11 : 1, rs.unwrap(PhoenixResultSet.class).getStatement()
                    .getQueryPlan().getScans().get(0).size());

            // query against the view index
            query = "SELECT 1 FROM " + view + " WHERE B > 0";
            rs = conn.createStatement().executeQuery(query).unwrap(PhoenixResultSet.class);
            // assert query is against viewIndex
            assertEquals(viewIndex, rs.unwrap(PhoenixResultSet.class).getStatement().getQueryPlan()
                    .getTableRef().getTable().getName().getString());
            assertEquals(!useStats ? 11 : 1, rs.unwrap(PhoenixResultSet.class).getStatement()
                    .getQueryPlan().getScans().get(0).size());
        }
    }

    @Test
    public void testQueryingWithUseStatsForParallelizationOnOff() throws SQLException {
        testUseStatsForParallelizationOnSaltedTable(true, true);
        testUseStatsForParallelizationOnSaltedTable(true, false);
        testUseStatsForParallelizationOnSaltedTable(false, true);
        testUseStatsForParallelizationOnSaltedTable(false, false);
    }

    private void testUseStatsForParallelizationOnSaltedTable(boolean useStatsFlag, boolean salted)
            throws SQLException {
        String tableName = generateUniqueName();
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(
            "create table " + tableName + "(k varchar not null primary key, v varchar) "
                    + (salted ? " SALT_BUCKETS=2," : "") + " USE_STATS_FOR_PARALLELIZATION="
                    + useStatsFlag);
        conn.createStatement().execute("upsert into " + tableName + " values ('1', 'B')");
        conn.createStatement().execute("upsert into " + tableName + " values ('2', 'A')");
        conn.commit();
        String query = "SELECT V FROM " + tableName + " ORDER BY V";
        ResultSet rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("A", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("B", rs.getString(1));

        // Collect stats and make sure query still works correctly
        conn.createStatement().execute("UPDATE STATISTICS " + tableName);
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("A", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("B", rs.getString(1));
    }

    @Test
    public void testUseStatsForParallelizationProperyOnViewIndex() throws SQLException {
        String tableName = generateUniqueName();
        String viewName = generateUniqueName();
        String tenantViewName = generateUniqueName();
        String viewIndexName = generateUniqueName();
        boolean useStats = !DEFAULT_USE_STATS_FOR_PARALLELIZATION;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement()
                    .execute("create table " + tableName
                            + "(tenantId CHAR(15) NOT NULL, pk1 integer NOT NULL, v varchar CONSTRAINT PK PRIMARY KEY "
                            + "(tenantId, pk1)) MULTI_TENANT=true");
            try (Connection tenantConn = getTenantConnection("tenant1")) {
                conn.createStatement().execute("CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName);
                conn.createStatement().execute("CREATE INDEX " + viewIndexName + " on " + viewName + " (v) ");
                tenantConn.createStatement().execute("CREATE VIEW " + tenantViewName + " AS SELECT * FROM " + viewName);
                conn.createStatement()
                        .execute("ALTER TABLE " + tableName + " set USE_STATS_FOR_PARALLELIZATION=" + useStats);
                // changing a property on a base table is propagated to its view
                // if the view has not previously modified the property
                validatePropertyOnViewIndex(viewName, viewIndexName, useStats, conn, tenantConn);
            }
        }
    }

    private void validatePropertyOnViewIndex(String viewName, String viewIndexName, boolean useStats, Connection conn,
            Connection tenantConn) throws SQLException, TableNotFoundException {
        // fetch the latest view ptable
        PhoenixRuntime.getTableNoCache(tenantConn, viewName);
        PhoenixConnection phxConn = conn.unwrap(PhoenixConnection.class);
        PTable viewIndex = phxConn.getTable(new PTableKey(phxConn.getTenantId(), viewIndexName));
        assertEquals("USE_STATS_FOR_PARALLELIZATION property set incorrectly", useStats,
                PhoenixConfigurationUtil
                        .getStatsForParallelizationProp(tenantConn.unwrap(PhoenixConnection.class), viewIndex));
    }

}
