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
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.EnvironmentEdge;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * This class has tests for asserting the bytes and rows information exposed in the explain plan
 * when statistics are enabled.
 */
public class ExplainPlanWithStatsEnabledIT extends ParallelStatsEnabledIT {

    private static String tableA;
    private static String tableB;
    private static String tableWithLargeGPWidth;
    private static String indexOnA;
    private static final long largeGpWidth = 2 * 1000 * 1000l;

    @BeforeClass
    public static void createTables() throws Exception {
        tableA = generateUniqueName();
        initDataAndStats(tableA, 20l);
        tableB = generateUniqueName();
        initDataAndStats(tableB, 20l);
        tableWithLargeGPWidth = generateUniqueName();
        initDataAndStats(tableWithLargeGPWidth, largeGpWidth);
        indexOnA = generateUniqueName();
        createIndex(indexOnA, tableA, 20);
    }

    private static void createIndex(String indexName, String table, long guidePostWidth)
            throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + table
                    + " (c1.a) INCLUDE (c2.b) GUIDE_POSTS_WIDTH = " + guidePostWidth);
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
            assertEquals((Long) 0l, info.estimatedBytes);
            assertEquals((Long) 0l, info.estimatedRows);
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
            assertEquals((Long) 634l, info.estimatedBytes);
            assertEquals((Long) 10l, info.estimatedRows);
            assertTrue(info.estimateInfoTs > 0);
        }
    }

    @Test
    public void testBytesRowsForSelectOnIndex() throws Exception {
        String sql = "SELECT * FROM " + tableA + " where c1.a >= ?";
        List<Object> binds = Lists.newArrayList();
        binds.add(0);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 691l, info.estimatedBytes);
            assertEquals((Long) 10l, info.estimatedRows);
            assertTrue(info.estimateInfoTs > 0);
        }
    }

    @Test
    public void testBytesRowsForUnion() throws Exception {
        String sql =
                "SELECT /*+ NO_INDEX */ * FROM " + tableA + " UNION ALL SELECT * FROM " + tableB;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Estimate info = getByteRowEstimates(conn, sql, Lists.newArrayList());
            assertEquals((Long) (2 * 634l), info.estimatedBytes);
            assertEquals((Long) (2 * 10l), info.estimatedRows);
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
            assertEquals((Long) (634l), info.estimatedBytes);
            assertEquals((Long) (10l), info.estimatedRows);
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
            assertEquals((Long) (2 * 634l), info.estimatedBytes);
            assertEquals((Long) (2 * 10l), info.estimatedRows);
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
            assertEquals((Long) 634l, info.estimatedBytes);
            assertEquals((Long) 10l, info.estimatedRows);
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
            assertEquals((Long) 634l, info.estimatedBytes);
            assertEquals((Long) 10l, info.estimatedRows);
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
            assertEquals((Long) 634l, info.estimatedBytes);
            assertEquals((Long) 10l, info.estimatedRows);
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
            assertEquals((Long) 0l, info.estimatedBytes);
            assertEquals((Long) 0l, info.estimatedRows);
            assertEquals((Long) 0l, info.estimateInfoTs);
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
            assertEquals((Long) 634l, info.estimatedBytes);
            assertEquals((Long) 10l, info.estimatedRows);
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
            assertEquals((Long) 200l, info.estimatedBytes);
            assertEquals((Long) 2l, info.estimatedRows);
            assertTrue(info.estimateInfoTs > 0);
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
            assertEquals((Long) 0l, info.estimatedBytes);
            assertEquals((Long) 0l, info.estimatedRows);
            assertEquals((Long) 0l, info.estimateInfoTs);
        }
    }

    @Test
    public void testBytesRowsForSelectExecutedSerially() throws Exception {
        String sql = "SELECT * FROM " + tableA + " LIMIT 2";
        List<Object> binds = Lists.newArrayList();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(false);
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 200l, info.estimatedBytes);
            assertEquals((Long) 2l, info.estimatedRows);
            assertTrue(info.estimateInfoTs > 0);
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
    public void testSettingUseStatsForQueryPlanProperty() throws Exception {
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
            ddl = "CREATE TABLE " + table + " (PK1 INTEGER NOT NULL PRIMARY KEY, KV1 VARCHAR)";
            conn.createStatement().execute(ddl);
            assertUseStatsForQueryFlag(table, conn.unwrap(PhoenixConnection.class),
                DEFAULT_USE_STATS_FOR_PARALLELIZATION);
        }
    }

    private static void assertUseStatsForQueryFlag(String tableName, PhoenixConnection conn,
            boolean flag) throws TableNotFoundException, SQLException {
        assertEquals(flag,
            conn.unwrap(PhoenixConnection.class).getMetaDataCache()
                    .getTableRef(new PTableKey(null, tableName)).getTable()
                    .useStatsForParallelization());
        String query =
                "SELECT USE_STATS_FOR_PARALLELIZATION FROM SYSTEM.CATALOG WHERE TABLE_NAME = ? AND COLUMN_NAME IS NULL AND COLUMN_FAMILY IS NULL AND TENANT_ID IS NULL";
        PreparedStatement stmt = conn.prepareStatement(query);
        stmt.setString(1, tableName);
        ResultSet rs = stmt.executeQuery();
        rs.next();
        assertEquals(flag, rs.getBoolean(1));
    }

    @Test
    public void testBytesRowsForSelectOnTenantViews() throws Exception {
        String tenant1View = generateUniqueName();
        ;
        String tenant2View = generateUniqueName();
        ;
        String tenant3View = generateUniqueName();
        ;
        String multiTenantBaseTable = generateUniqueName();
        String tenant1 = "tenant1";
        String tenant2 = "tenant2";
        String tenant3 = "tenant3";
        MyClock clock = new MyClock(1000);
        createMultitenantTableAndViews(tenant1View, tenant2View, tenant3View, tenant1, tenant2,
            tenant3, multiTenantBaseTable, clock);

        // query the entire multitenant table
        String sql = "SELECT * FROM " + multiTenantBaseTable + " WHERE ORGID >= ?";
        List<Object> binds = Lists.newArrayList();
        binds.add("tenant0");
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 817l, info.estimatedBytes);
            assertEquals((Long) 10l, info.estimatedRows);
            assertEquals((Long) clock.currentTime(), info.estimateInfoTs);
        }
        binds.clear();
        // query tenant1 view
        try (Connection conn = getTenantConnection(tenant1)) {
            sql = "SELECT * FROM " + tenant1View;
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 143l, info.estimatedBytes);
            assertEquals((Long) 2l, info.estimatedRows);
            assertEquals((Long) clock.currentTime(), info.estimateInfoTs);
        }
        // query tenant2 view
        try (Connection conn = getTenantConnection(tenant2)) {
            sql = "SELECT * FROM " + tenant2View;
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 143l, info.estimatedBytes);
            assertEquals((Long) 2l, info.estimatedRows);
            assertEquals((Long) clock.currentTime(), info.estimateInfoTs);
        }
        // query tenant3 view
        try (Connection conn = getTenantConnection(tenant3)) {
            sql = "SELECT * FROM " + tenant3View;
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 531l, info.estimatedBytes);
            assertEquals((Long) 6l, info.estimatedRows);
            assertEquals((Long) clock.currentTime(), info.estimateInfoTs);
        }
        /*
         * Now we will add some rows to tenant1view an run update stats on it. We will do this after
         * advancing our clock by 1000 seconds. This way we can check that only the region for
         * tenant1 will have updated guidepost with the new timestamp.
         */
        long prevTenant1Bytes = 143l;
        long prevGuidePostTimestamp = clock.currentTime();
        clock.advanceTime(1000);
        try {
            EnvironmentEdgeManager.injectEdge(clock);
            // Update tenant1 view
            try (Connection conn = getTenantConnection(tenant1)) {
                // upsert a few rows for tenantView
                conn.createStatement()
                        .executeUpdate("UPSERT INTO " + tenant1View + " VALUES (11, 11, 11)");
                conn.createStatement()
                        .executeUpdate("UPSERT INTO " + tenant1View + " VALUES (12, 12, 12)");
                conn.createStatement()
                        .executeUpdate("UPSERT INTO " + tenant1View + " VALUES (13, 13, 13)");
                conn.createStatement()
                        .executeUpdate("UPSERT INTO " + tenant1View + " VALUES (14, 14, 14)");
                conn.createStatement()
                        .executeUpdate("UPSERT INTO " + tenant1View + " VALUES (15, 15, 15)");
                conn.createStatement()
                        .executeUpdate("UPSERT INTO " + tenant1View + " VALUES (16, 16, 16)");
                conn.commit();
                // run update stats on the tenantView
                conn.createStatement().executeUpdate("UPDATE STATISTICS " + tenant1View);
                // get estimates now and check if they were updated as expected
                sql = "SELECT * FROM " + tenant1View;
                Estimate info = getByteRowEstimates(conn, sql, Collections.emptyList());
                assertTrue(info.estimatedBytes > prevTenant1Bytes);
                assertEquals((Long) 8l, info.estimatedRows);
                assertEquals((Long) clock.currentTime(), info.estimateInfoTs);
            }
        } finally {
            EnvironmentEdgeManager.reset();
        }
        // Now check estimates again for tenantView2 and tenantView3. They should stay the same.
        try (Connection conn = getTenantConnection(tenant2)) {
            sql = "SELECT * FROM " + tenant2View;
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 143l, info.estimatedBytes);
            assertEquals((Long) 2l, info.estimatedRows);
            assertEquals((Long) prevGuidePostTimestamp, info.estimateInfoTs);
        }
        try (Connection conn = getTenantConnection(tenant3)) {
            sql = "SELECT * FROM " + tenant3View;
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 531l, info.estimatedBytes);
            assertEquals((Long) 6l, info.estimatedRows);
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
            assertEquals((Long) 1399l, info.estimatedBytes);
            assertEquals((Long) 16l, info.estimatedRows);
            assertEquals((Long) prevGuidePostTimestamp, info.estimateInfoTs);
        }
    }

    private static void createMultitenantTableAndViews(String tenant1View, String tenant2View,
            String tenant3View, String tenant1, String tenant2, String tenant3,
            String multiTenantTable, MyClock clock) throws SQLException {
        byte[][] splits =
                new byte[][] { Bytes.toBytes(tenant1), Bytes.toBytes(tenant2),
                        Bytes.toBytes(tenant3) };
        String ddl =
                "CREATE TABLE " + multiTenantTable
                        + " (orgId CHAR(15) NOT NULL, pk2 integer NOT NULL, c1.a bigint, c2.b bigint CONSTRAINT PK PRIMARY KEY "
                        + "(ORGID, PK2)) MULTI_TENANT=true, GUIDE_POSTS_WIDTH=2";
        // Use our own clock to get rows created with our controlled timestamp
        try {
            EnvironmentEdgeManager.injectEdge(clock);
            createTestTable(getUrl(), ddl, splits, null);
            clock.advanceTime(1000);
            try (Connection conn = DriverManager.getConnection(getUrl())) {
                /**
                 * Insert 2 rows each for tenant1 and tenant2 and 6 rows for tenant3
                 */
                conn.createStatement().execute(
                    "upsert into " + multiTenantTable + " values ('" + tenant1 + "',1,1,1)");
                conn.createStatement().execute(
                    "upsert into " + multiTenantTable + " values ('" + tenant1 + "',2,2,2)");
                conn.createStatement().execute(
                    "upsert into " + multiTenantTable + " values ('" + tenant2 + "',3,3,3)");
                conn.createStatement().execute(
                    "upsert into " + multiTenantTable + " values ('" + tenant2 + "',4,4,4)");
                conn.createStatement().execute(
                    "upsert into " + multiTenantTable + " values ('" + tenant3 + "',5,5,5)");
                conn.createStatement().execute(
                    "upsert into " + multiTenantTable + " values ('" + tenant3 + "',6,6,6)");
                conn.createStatement().execute(
                    "upsert into " + multiTenantTable + " values ('" + tenant3 + "',7,7,7)");
                conn.createStatement().execute(
                    "upsert into " + multiTenantTable + " values ('" + tenant3 + "',8,8,8)");
                conn.createStatement().execute(
                    "upsert into " + multiTenantTable + " values ('" + tenant3 + "',9,9,9)");
                conn.createStatement().execute(
                    "upsert into " + multiTenantTable + " values ('" + tenant3 + "',10,10,10)");
                conn.commit();
                conn.createStatement().execute("UPDATE STATISTICS " + multiTenantTable);
            }
            try (Connection conn = getTenantConnection(tenant1)) {
                conn.createStatement().execute(
                    "CREATE VIEW " + tenant1View + " AS SELECT * FROM " + multiTenantTable);
            }
            try (Connection conn = getTenantConnection(tenant2)) {
                conn.createStatement().execute(
                    "CREATE VIEW " + tenant2View + " AS SELECT * FROM " + multiTenantTable);
            }
            try (Connection conn = getTenantConnection(tenant3)) {
                conn.createStatement().execute(
                    "CREATE VIEW " + tenant3View + " AS SELECT * FROM " + multiTenantTable);
            }
        } finally {
            EnvironmentEdgeManager.reset();
        }
    }

    private static class MyClock extends EnvironmentEdge {
        public volatile long time;

        public MyClock(long time) {
            this.time = time;
        }

        @Override
        public long currentTime() {
            return time;
        }

        public void advanceTime(long t) {
            this.time += t;
        }
    }
}
