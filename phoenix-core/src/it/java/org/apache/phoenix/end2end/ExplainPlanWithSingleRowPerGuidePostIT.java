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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * This class has tests for asserting the bytes and rows information exposed in the explain plan
 * when statistics are enabled.
 */
public class ExplainPlanWithSingleRowPerGuidePostIT extends ParallelStatsEnabledIT {
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

            // Because we didn't set the property, PTable.useStatsForParallelization() should return null
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

    /**
     * In this test case, when using tenant specific connection, a query for a tenant will also count
     * the first guide post of the next tenant. This is because we'll use MapReduce job to update
     * stats, and the job currently update stats for the base table as a whole in which guide posts
     * won't align with the tenants' boundaries, i.e., a guide post could contain multiple tenants'
     * estimated info.
     *
     * We also ignore the case when not all regions have guide posts available. This happens because
     * stats collection is atomic operation on region level instead of table level. For now we ignore
     * this case due to the following reasons:
     * 1. When not all regions have guide posts available, we return null estimation timestamp, but
     *    it still very confusing for customer to decide to use the estimation from stats or not. We
     *    need to fundamentally resolve this issue by providing atomicity on table level.
     * 2. Currently, we can prevent the case from happening by successfully collecting stats for a
     *    whole table before enabling stats for use. By doing in this way, even some regions haven't
     *    been updated stats for long time, there is only stale stats instead of incomplete stats.
     *
     * @throws Exception
     */
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
        Long estimatedBytesPerRow = 81L;
        MyClock clock = new MyClock(1000);
        createMultitenantTableAndViews(tenant1View, tenant2View, tenant3View, tenant4View, tenant1, tenant2,
            tenant3, tenant4, multiTenantBaseTable, clock);

        // query the entire multi-tenant table
        String sql = "SELECT * FROM " + multiTenantBaseTable + " WHERE ORGID >= ?";
        List<Object> binds = Lists.newArrayList();
        binds.add("tenant0");
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 10L, info.getEstimatedRows());
            assertEquals((Long)(estimatedBytesPerRow * info.getEstimatedRows()), info.getEstimatedBytes());
            assertEquals((Long) clock.currentTime(), info.getEstimateInfoTs());
        }
        binds.clear();
        // query tenant1 view
        try (Connection conn = getTenantConnection(tenant1)) {
            sql = "SELECT * FROM " + tenant1View;
            Estimate info = getByteRowEstimates(conn, sql, binds);
            // One guide post one row. Internal query key range [tenant 1, tenant 2) hits
            // 3 guide posts and the last guide post has end key 'tenant 2'.
            assertEquals((Long) 3L, info.getEstimatedRows());
            assertEquals((Long)(estimatedBytesPerRow * info.getEstimatedRows()), info.getEstimatedBytes());
            assertEquals((Long) clock.currentTime(), info.getEstimateInfoTs());
        }
        // query tenant2 view
        try (Connection conn = getTenantConnection(tenant2)) {
            sql = "SELECT * FROM " + tenant2View;
            Estimate info = getByteRowEstimates(conn, sql, binds);
            // One guide post one row. Internal query key range [tenant 2, tenant 3) hits
            // 3 guide posts and the last guide post has end key 'tenant 3'.
            assertEquals((Long) 3L, info.getEstimatedRows());
            assertEquals((Long)(estimatedBytesPerRow * info.getEstimatedRows()), info.getEstimatedBytes());
            assertEquals((Long) clock.currentTime(), info.getEstimateInfoTs());
        }
        // query tenant3 view
        try (Connection conn = getTenantConnection(tenant3)) {
            sql = "SELECT * FROM " + tenant3View;
            Estimate info = getByteRowEstimates(conn, sql, binds);
            // One guide post one row. Internal query key range [tenant 3, tenant 4) hits
            // 6 guide posts and the last guide post has end key 'tenant 3', and there
            // is no more guide posts after 'tenant 3'.
            assertEquals((Long) 6L, info.getEstimatedRows());
            assertEquals((Long)(estimatedBytesPerRow * info.getEstimatedRows()), info.getEstimatedBytes());
            assertEquals((Long) clock.currentTime(), info.getEstimateInfoTs());
        }

        // Now we will add some rows to tenant2view and run update stats on it. We will do this
        // after advancing our clock by 1000 seconds. This way we can check that only the region
        // for tenant2 will have updated guidepost with the new timestamp.
        long prevGuidePostTimestamp = clock.currentTime();
        clock.advanceTime(1000);
        try {
            EnvironmentEdgeManager.injectEdge(clock);
            // Update tenant2 view
            try (Connection conn = getTenantConnection(tenant2)) {
                // Upsert a few rows for tenantView
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
                // Run update stats on the tenantView2. We generate two scans [tenant2, tenant2'1)
                // and [tenant2'1, tenant3). Because [tenant2, tenant2'1) is in the first region,
                // the stats on the tenantView1 in the first region will also be updated.
                conn.createStatement().executeUpdate("UPDATE STATISTICS " + tenant2View);
                // get estimates now and check if they were updated as expected
                sql = "SELECT * FROM " + tenant2View;
                Estimate info = getByteRowEstimates(conn, sql, Collections.emptyList());
                // One guide post one row. Internal query key range [tenant 2, tenant 3) hits
                // 9 guide posts and the last guide post has end key 'tenant 3'.
                assertEquals((Long) 9L, info.getEstimatedRows());
                assertEquals((Long)(estimatedBytesPerRow * info.getEstimatedRows()), info.getEstimatedBytes());
                // Use the last update time of the first guide post of 'tenant 3'.
                assertEquals((Long) prevGuidePostTimestamp, info.getEstimateInfoTs());
            }
        } finally {
            EnvironmentEdgeManager.reset();
        }

        // Now check estimates again for tenantView1. It should stay the same.
        try (Connection conn = getTenantConnection(tenant1)) {
            sql = "SELECT * FROM " + tenant1View;
            Estimate info = getByteRowEstimates(conn, sql, binds);
            // One guide post one row. Internal query key range [tenant 1, tenant 2) hits
            // 3 guide posts and the last guide post has end key 'tenant 2'.
            assertEquals((Long) 3L, info.getEstimatedRows());
            assertEquals((Long)(estimatedBytesPerRow * info.getEstimatedRows()), info.getEstimatedBytes());
            // The stats on the tenantView1 has been updated while we updated stats on the tenantView2.
            // See the comments above for explanation.
            assertEquals((Long) clock.currentTime(), info.getEstimateInfoTs());
        }
        // Now check estimates again for tenantView3. It should stay the same.
        try (Connection conn = getTenantConnection(tenant3)) {
            sql = "SELECT * FROM " + tenant3View;
            Estimate info = getByteRowEstimates(conn, sql, binds);
            // One guide post one row. Internal query key range [tenant 3, tenant 4) hits
            // 6 guide posts and the last guide post has end key 'tenant 3', because there
            // is no more guide posts after 'tenant 3'.
            assertEquals((Long) 6L, info.getEstimatedRows());
            assertEquals((Long)(estimatedBytesPerRow * info.getEstimatedRows()), info.getEstimatedBytes());
            assertEquals((Long) prevGuidePostTimestamp, info.getEstimateInfoTs());
        }
        // Now let's query the base table and see estimates. Because we use the minimum timestamp
        // for all guideposts that we will be scanning, the timestamp for the estimate info for this
        // query should be prevGuidePostTimestamp.
        binds.clear();
        binds.add("tenant0");
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            sql = "SELECT * FROM " + multiTenantBaseTable + " WHERE ORGID >= ?";
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long)(estimatedBytesPerRow * 16), info.getEstimatedBytes());
            assertEquals((Long) 16L, info.getEstimatedRows());
            assertEquals((Long) prevGuidePostTimestamp, info.getEstimateInfoTs());
        }
        // query tenant4 view
        binds.clear();
        try (Connection conn = getTenantConnection(tenant4)) {
            sql = "SELECT * FROM " + tenant4View;
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 0L, info.getEstimatedBytes());
            assertEquals((Long) 0L, info.getEstimatedRows());
            // There is no guide posts for tenantView4, so we use its' neighbor(tenant3)'s timestamp.
            assertEquals((Long) prevGuidePostTimestamp, info.getEstimateInfoTs());
        }

        clock.advanceTime(1000);
        try {
            EnvironmentEdgeManager.injectEdge(clock);
            // Update tenant4 view
            try (Connection conn = getTenantConnection(tenant4)) {
                // Upsert a few rows for tenantView4
                conn.createStatement()
                    .executeUpdate("UPSERT INTO " + tenant4View + " VALUES (6, 17,17)");
                conn.createStatement()
                    .executeUpdate("UPSERT INTO " + tenant4View + " VALUES (7, 17,17)");
                conn.commit();
                // run update stats on the tenantView
                conn.createStatement().executeUpdate("UPDATE STATISTICS " + tenant4View);

                // Get estimates now and check if they were updated as expected
                sql = "SELECT * FROM " + tenant4View;
                Estimate info = getByteRowEstimates(conn, sql, Collections.emptyList());
                // One guide post one row. Internal query key range [tenant 4, tenant 5) hits
                // 2 guide posts and the last guide post has end key 'tenant 4', because there
                // is no more guide posts after 'tenant 4'.
                assertEquals((Long) 2L, info.getEstimatedRows());
                assertEquals((Long)(estimatedBytesPerRow * info.getEstimatedRows()), info.getEstimatedBytes());
                assertEquals((Long) clock.currentTime(), info.getEstimateInfoTs());

                sql = "SELECT * FROM " + tenant4View + " WHERE pk2 >= 6";
                info = getByteRowEstimates(conn, sql, Collections.emptyList());
                // Hit 1 guide post with end key Tenant4'7. The guide post with end key Tenant4'6
                // is ignored because it only contains one row for sure. It's called estimation for a reason.
                assertEquals((Long) 1L, info.getEstimatedRows());
                assertEquals((Long)(estimatedBytesPerRow * info.getEstimatedRows()), info.getEstimatedBytes());
                assertEquals((Long) clock.currentTime(), info.getEstimateInfoTs());

                sql = "SELECT * FROM " + tenant4View + " WHERE pk2 >= 5";
                info = getByteRowEstimates(conn, sql, Collections.emptyList());
                // Hit 2 guide post with end keys Tenant4'6 and Tenant4'7.
                assertEquals((Long) 2L, info.getEstimatedRows());
                assertEquals((Long)(estimatedBytesPerRow * info.getEstimatedRows()), info.getEstimatedBytes());
                assertEquals((Long) clock.currentTime(), info.getEstimateInfoTs());
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
            String ddl =
                    "CREATE TABLE " + tableName + " (k INTEGER PRIMARY KEY, a bigint, b bigint)"
                            + " USE_STATS_FOR_PARALLELIZATION=" + useStatsForParallelization
                            + " SPLIT ON (102, 105, 108)";
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
            int[] result;

            // Query without any filter. Query Key Range (UNBOUND, UNBOUND).
            String sql = "SELECT a FROM " + tableName;
            ResultSet rs = conn.createStatement().executeQuery(sql);
            int i = 0;
            int numRows = 10;
            while (rs.next()) {
                assertEquals(100 + i, rs.getInt(1));
                i++;
            }
            assertEquals(numRows, i);
            // Hit all guide posts.
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 10L, info.getEstimatedRows());
            assertEquals((Long) 1000L, info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);

            // Query whose start key is before any data. Query Key Range [99, UNBOUND).
            sql = "SELECT a FROM " + tableName + " WHERE K >= 99";
            rs = conn.createStatement().executeQuery(sql);
            i = 0;
            numRows = 10;
            while (rs.next()) {
                assertEquals(100 + i, rs.getInt(1));
                i++;
            }
            assertEquals(numRows, i);
            // Hit all guide posts.
            info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 10L, info.getEstimatedRows());
            assertEquals((Long) 1000L, info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);

            // Query whose start key is after any data. Query Key Range [110, UNBOUND).
            sql = "SELECT a FROM " + tableName + " WHERE K >= 110";
            rs = conn.createStatement().executeQuery(sql);
            assertFalse(rs.next());
            // Hit zero guide posts.
            info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 0L, info.getEstimatedRows());
            assertEquals((Long) 0L, info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);

            // Query whose end key is before any data. Query Key Range (UNBOUND, 98].
            sql = "SELECT a FROM " + tableName + " WHERE K <= 98";
            rs = conn.createStatement().executeQuery(sql);
            assertFalse(rs.next());
            // Hit the first guide post.
            info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 1L, info.getEstimatedRows());
            assertEquals((Long) 100L, info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);

            // Query whose end key is after any data. Query Key Range (UNBOUND, 110].
            sql = "SELECT a FROM " + tableName + " WHERE K <= 110";
            rs = conn.createStatement().executeQuery(sql);
            i = 0;
            numRows = 10;
            while (rs.next()) {
                assertEquals(100 + i, rs.getInt(1));
                i++;
            }
            assertEquals(numRows, i);
            // Hit all guide posts.
            info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 10L, info.getEstimatedRows());
            assertEquals((Long) 1000L, info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);

            // Query whose start key and end key is before any data. Query Key Range [80, 90].
            sql = "SELECT a FROM " + tableName + " WHERE K >= 80 AND K <= 90";
            rs = conn.createStatement().executeQuery(sql);
            assertFalse(rs.next());
            // Hit the first guide post.
            info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 1L, info.getEstimatedRows());
            assertEquals((Long) 100L, info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);

            // Query whose start key and end key is after any data. Query Key Range (120, 130).
            sql = "SELECT a FROM " + tableName + " WHERE K > 120 AND K < 130";
            rs = conn.createStatement().executeQuery(sql);
            assertFalse(rs.next());
            // Hit zero guide posts.
            info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 0L, info.getEstimatedRows());
            assertEquals((Long) 0L, info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);

            // Query whose start key is before and end key is between data. Query Key Range [90, 102].
            sql = "SELECT a FROM " + tableName + " WHERE K >=90 AND K <= 102";
            rs = conn.createStatement().executeQuery(sql);
            i = 0;
            numRows = 3;
            while (rs.next()) {
                assertEquals(100 + i, rs.getInt(1));
                i++;
            }
            assertEquals(numRows, i);
            // Hit the first 4 guide posts including the guide post with end key 103. The reason
            // is that the query key range [90, 102] is internally translated into [90, 103),
            // so the query key range will consistently hit the first 4 guide posts which have 4
            // estimated count of rows in total. The actual count of rows scanned is 3. The gap is
            // because in our test case, one guide post only has 1 row.
            info = getByteRowEstimates(conn, sql, binds);
            // Depending on the guidepost boundary, this estimate
            // can be slightly off. It's called estimation for a reason.
            assertEquals((Long) 4L, info.getEstimatedRows());
            assertEquals((Long) 400L, info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);

            // Query whose start key is before and end key is between data. Query Key Range [90, 102).
            sql = "SELECT a FROM " + tableName + " WHERE K >=90 AND K < 102";
            rs = conn.createStatement().executeQuery(sql);
            i = 0;
            numRows = 2;
            while (rs.next()) {
                assertEquals(100 + i, rs.getInt(1));
                i++;
            }
            assertEquals(numRows, i);
            // Hit the first 3 guide posts including the guide post with end key 102.
            info = getByteRowEstimates(conn, sql, binds);
            // Depending on the guidepost boundary, this estimate
            // can be slightly off. It's called estimation for a reason.
            assertEquals((Long) 3L, info.getEstimatedRows());
            assertEquals((Long) 300L, info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);

            // Query whose start key is between and end key is after data. Query Key Range [105, 120].
            sql = "SELECT a FROM " + tableName + " WHERE K >= 105 AND K <= 120";
            rs = conn.createStatement().executeQuery(sql);
            i = 0;
            numRows = 5;
            result = new int[] { 105, 106, 107, 108, 109 };
            while (rs.next()) {
                assertEquals(result[i++], rs.getInt(1));
            }
            assertEquals(numRows, i);
            info = getByteRowEstimates(conn, sql, binds);
            // Hit 4 guide posts with end keys 106, 107, 108, 109. The guide post with end key 105
            // is ignored because it only contains one row for sure. It's called estimation for a reason.
            assertEquals((Long) 4L, info.getEstimatedRows());
            assertEquals((Long) 400L, info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);

            // Query whose start key is between and end key is after data. Query Key Range (105, 120].
            sql = "SELECT a FROM " + tableName + " WHERE K > 105 AND K <= 120";
            rs = conn.createStatement().executeQuery(sql);
            i = 0;
            numRows = 4;
            result = new int[] { 106, 107, 108, 109 };
            while (rs.next()) {
                assertEquals(result[i++], rs.getInt(1));
            }
            assertEquals(numRows, i);
            info = getByteRowEstimates(conn, sql, binds);
            // Hit 3 guide posts with end keys 107, 108, 109. The query key range used internal is [106, 121).
            // The guide post with end key 106 is ignored because it only contains one row for sure. It's called
            // estimate for a reason.
            assertEquals((Long) 3L, info.getEstimatedRows());
            assertEquals((Long) 300L, info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);

            // Query whose start key and end key are both between data.
            sql = "SELECT a FROM " + tableName + " WHERE K >= 100 AND K < 110";
            rs = conn.createStatement().executeQuery(sql);
            i = 0;
            numRows = 10;
            while (rs.next()) {
                assertEquals(100 + i, rs.getInt(1));
                i++;
            }
            assertEquals(numRows, i);
            info = getByteRowEstimates(conn, sql, binds);
            // Hit 9 guide posts with end keys 101, 102, ..., 109. The guide post with end key 100
            // is ignored because it only contains one row for sure. It's called estimation for a reason.
            assertEquals((Long) 9L, info.getEstimatedRows());
            assertEquals((Long) 900L, info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);

            // Query whose start key and end key are both between data. The query key range is [101, 107],
            // and both start key and end key are the last key in a region.
            sql = "SELECT a FROM " + tableName + " WHERE K >= 101 AND K <= 107";
            rs = conn.createStatement().executeQuery(sql);
            i = 0;
            numRows = 7;
            result = new int[] { 101, 102, 103, 104, 105, 106, 107 };
            while (rs.next()) {
                assertEquals(result[i++], rs.getInt(1));
            }
            assertEquals(numRows, i);
            info = getByteRowEstimates(conn, sql, binds);
            // Hit 9 guide posts with end keys 102, 103, ..., 108. The guide post with end key 101
            // is ignored because it only contains one row for sure. It's called estimation for a reason.
            assertEquals((Long) 7L, info.getEstimatedRows());
            assertEquals((Long) 700L, info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);

            // Query whose start key and end key are both between data. The query key range is [102, 108],
            // and both start key and end key are the first key in a region.
            sql = "SELECT a FROM " + tableName + " WHERE K >= 102 AND K <= 108";
            rs = conn.createStatement().executeQuery(sql);
            i = 0;
            numRows = 7;
            result = new int[] { 102, 103, 104, 105, 106, 107, 108 };
            while (rs.next()) {
                assertEquals(result[i++], rs.getInt(1));
            }
            assertEquals(numRows, i);
            info = getByteRowEstimates(conn, sql, binds);
            // Hit 9 guide posts with end keys 103, 104, ..., 109. The guide post with end key 102
            // is ignored because it only contains one row for sure. It's called estimation for a reason.
            assertEquals((Long) 7L, info.getEstimatedRows());
            assertEquals((Long) 700L, info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);

            // Query with multiple scan ranges, and each range's start key and end key are both between data.
            // Query Key Ranges [101, 104), (106, 108]
            sql = "SELECT a FROM " + tableName + " WHERE K >= 101 AND K < 104 OR K > 106 AND K <= 108";
            rs = conn.createStatement().executeQuery(sql);
            i = 0;
            numRows = 5;
            result = new int[] { 101, 102, 103, 107, 108 };
            while (rs.next()) {
                assertEquals(result[i++], rs.getInt(1));
            }
            assertEquals(numRows, i);
            // Hit 6 guide posts with end keys 102, 103, 104, 108 and 109. For Query Key Ranges
            // [101, 104) and [107, 109) used internal, we ignore the guide posts with end key
            // 101, 107 and count the guide post with end key 109.
            info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 5L, info.getEstimatedRows());
            assertEquals((Long) 500L, info.getEstimatedBytes());
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
            try (HBaseAdmin admin =
                    conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                byte[] splitKey = Bytes.toBytes("00Dabcdetenant200B");
                admin.split(Bytes.toBytes(multiTenantTable), splitKey);
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
            assertEquals((Long) 1L, info.getEstimatedRows());
            // ok now run update stats for tenant2 view
            conn.createStatement().execute("UPDATE STATISTICS " + tenant2View);
            /*
             * And now, let's recheck our estimate info. We should have all the rows of view2
             * available now.
             */
            info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 6L, info.getEstimatedRows());
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
