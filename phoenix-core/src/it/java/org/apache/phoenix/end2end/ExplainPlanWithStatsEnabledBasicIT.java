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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

import org.apache.phoenix.schema.stats.StatisticsUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * This class has tests for asserting the bytes and rows information exposed in the explain plan
 * when statistics are enabled.
 */
public class ExplainPlanWithStatsEnabledBasicIT extends ParallelStatsEnabledIT {
    private static String tableA;
    private static Long estimatedBytesOfC1PerRowForTableA = 67L;
    private static Long estimatedBytesOfC2PerRowForTableA = 73L;

    private static String tableB;
    private static Long estimatedBytesOfC1PerRowForTableB = 67L;
    private static Long estimatedBytesOfC2PerRowForTableB = 36L;

    private static String tableC_WithLargeGPWidth;
    private static Long estimatedBytesOfC1PerRowForTableC = 67L;
    private static Long estimatedBytesOfC2PerRowForTableC = 36L;

    private static String indexOnA;
    private static final long largeGpWidth = 2 * 1000 * 1000L;

    @BeforeClass
    public static void createTables() throws Exception {
        tableA = generateUniqueName();
        initDataAndStats(tableA, 20L);
        tableB = generateUniqueName();
        initDataAndStats(tableB, 20L);
        tableC_WithLargeGPWidth = generateUniqueName();
        initDataAndStats(tableC_WithLargeGPWidth, largeGpWidth);
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

    @Test
    public void testBytesRowsForSelectWhenKeyOutOfRange() throws Exception {
        String sql = "SELECT * FROM " + tableA + " where k >= ?";
        List<Object> binds = Lists.newArrayList();
        binds.add(200);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 0L, info.getEstimatedBytes());
            assertEquals((Long) 0L, info.getEstimatedRows());
            assertTrue(info.getEstimateInfoTs() > 0);
        }
    }

    @Test
    public void testBytesRowsForPointSelectWithLimitGreaterThanPointLookupSize() throws Exception {
        String sql = "SELECT * FROM " + tableA + " where k in (? ,?) limit 4";
        List<Object> binds = Lists.newArrayList();
        binds.add(103); binds.add(104);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 200L, info.getEstimatedBytes());
            assertEquals((Long) 2L, info.getEstimatedRows());
            assertEquals((Long) StatisticsUtil.NOT_STATS_BASED_TS, info.getEstimateInfoTs());
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
            assertEquals((Long) 264L, info.getEstimatedBytes());
            assertEquals((Long) 3L, info.getEstimatedRows());
            assertEquals((Long) StatisticsUtil.NOT_STATS_BASED_TS, info.getEstimateInfoTs());

            info = getByteRowEstimates(conn, noIndexSQL, binds);
            assertEquals((Long) 10L, info.getEstimatedRows());
            assertEquals((Long)(estimatedBytesOfC1PerRowForTableA * info.getEstimatedRows()), info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);
        }
    }

    @Test
    public void testBytesRowsForSelectWithLimitIgnored() throws Exception {
        String sql = "SELECT * FROM " + tableA + " where (c1.a > c2.b) limit 1";
        List<Object> binds = Lists.newArrayList();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 10L, info.getEstimatedRows());
            assertEquals((Long)(estimatedBytesOfC2PerRowForTableA * info.getEstimatedRows()), info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);
        }
    }

    @Test
    public void testBytesRowsForSelectWhenKeyInRange() throws Exception {
        String sql = "SELECT * FROM " + tableB + " where k >= ?";
        List<Object> binds = Lists.newArrayList();
        binds.add(99);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 10L, info.getEstimatedRows());
            assertEquals((Long)(estimatedBytesOfC1PerRowForTableB * info.getEstimatedRows()), info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);
        }
    }

    @Test
    public void testBytesRowsForSelectOnIndex() throws Exception {
        String sql = "SELECT * FROM " + tableA + " where c1.a >= ?";
        List<Object> binds = Lists.newArrayList();
        binds.add(0);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 10L, info.getEstimatedRows());
            assertEquals((Long)(estimatedBytesOfC2PerRowForTableA * info.getEstimatedRows()), info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);
        }
    }

    @Test
    public void testBytesRowsForUnion() throws Exception {
        String sql =
                "SELECT /*+ NO_INDEX */ * FROM " + tableA + " UNION ALL SELECT * FROM " + tableB;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Estimate info = getByteRowEstimates(conn, sql, Lists.newArrayList());
            assertEquals((Long) (2 * 10L), info.getEstimatedRows());
            assertEquals((Long) (10 * estimatedBytesOfC1PerRowForTableA +
                    10 * estimatedBytesOfC1PerRowForTableB), info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);
        }
    }

    @Test
    public void testEstimatesForSelectFromTableWithLargeGpWidth() throws Exception {
        // For table with largeGpWidth, a guide post is generated with actual size
        // of data when collecting guide posts on server side.
        String sql =
                "SELECT * FROM " + tableC_WithLargeGPWidth;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Estimate info = getByteRowEstimates(conn, sql, Lists.newArrayList());
            assertEquals((Long) 10L, info.getEstimatedRows());
            assertEquals((Long)(estimatedBytesOfC1PerRowForTableC * info.getEstimatedRows()), info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);
        }
    }

    @Test
    public void testEstimatesForUnionWithTableWithLargeGpWidth() throws Exception {
        // For table with largeGpWidth, a guide post is generated with actual size
        // of data when collecting guide posts on server side.
        String sql =
                "SELECT /*+ NO_INDEX */ * FROM " + tableA + " UNION ALL SELECT * FROM " + tableB
                        + " UNION ALL SELECT * FROM " + tableC_WithLargeGPWidth;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Estimate info = getByteRowEstimates(conn, sql, Lists.newArrayList());
            assertEquals((Long) (3 * 10L), info.getEstimatedRows());
            assertEquals((Long) (10 * (estimatedBytesOfC1PerRowForTableA +
                    estimatedBytesOfC1PerRowForTableB + estimatedBytesOfC1PerRowForTableC)), info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);
        }
    }

    @Test
    public void testBytesRowsForHashJoin() throws Exception {
        String sql =
                "SELECT ta.c1.a, ta.c2.b FROM " + tableA + " ta JOIN " + tableB
                        + " tb ON ta.k = tb.k";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Estimate info = getByteRowEstimates(conn, sql, Lists.newArrayList());
            assertEquals((Long) 10L, info.getEstimatedRows());
            assertEquals((Long)(estimatedBytesOfC1PerRowForTableA * info.getEstimatedRows()), info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);
        }
    }

    @Test
    public void testBytesRowsForSortMergeJoin() throws Exception {
        String sql =
                "SELECT /*+ NO_INDEX USE_SORT_MERGE_JOIN */ ta.c1.a, ta.c2.b FROM " + tableA
                        + " ta JOIN " + tableB + " tb ON ta.k = tb.k";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Estimate info = getByteRowEstimates(conn, sql, Lists.newArrayList());
            assertEquals((Long) (2 * 10L), info.getEstimatedRows());
            assertEquals((Long) (10 * (estimatedBytesOfC1PerRowForTableA
                    + estimatedBytesOfC1PerRowForTableB)), info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);
        }
    }

    @Test
    public void testBytesRowsForAggregateQuery() throws Exception {
        String sql = "SELECT count(*) FROM " + tableA + " where k >= ?";
        List<Object> binds = Lists.newArrayList();
        binds.add(99);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 10L, info.getEstimatedRows());
            assertEquals((Long)(estimatedBytesOfC1PerRowForTableA * info.getEstimatedRows()), info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);
        }
    }

    @Test
    public void testBytesRowsForUpsertSelectServerSide() throws Exception {
        String sql = "UPSERT INTO " + tableA + " SELECT * FROM " + tableB;
        List<Object> binds = Lists.newArrayList();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(true);
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 10L, info.getEstimatedRows());
            assertEquals((Long)(estimatedBytesOfC1PerRowForTableB * info.getEstimatedRows()), info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);
        }
    }

    @Test
    public void testBytesRowsForUpsertSelectClientSide() throws Exception {
        String sql = "UPSERT INTO " + tableB + " SELECT * FROM " + tableB;
        List<Object> binds = Lists.newArrayList();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(false);
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 10L, info.getEstimatedRows());
            assertEquals((Long)(estimatedBytesOfC1PerRowForTableB * info.getEstimatedRows()), info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);
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
            assertEquals((Long) 0L, info.getEstimatedBytes());
            assertEquals((Long) 0L, info.getEstimatedRows());
            assertEquals((Long) 0L, info.getEstimateInfoTs());
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
            assertEquals((Long) 10L, info.getEstimatedRows());
            assertEquals((Long)(estimatedBytesOfC1PerRowForTableA * info.getEstimatedRows()), info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);
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
            assertEquals((Long) 200L, info.getEstimatedBytes());
            assertEquals((Long) 2L, info.getEstimatedRows());
            assertEquals((Long) StatisticsUtil.NOT_STATS_BASED_TS, info.getEstimateInfoTs());
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
            assertEquals((Long) 0L, info.getEstimatedBytes());
            assertEquals((Long) 0L, info.getEstimatedRows());
            assertEquals((Long) 0L, info.getEstimateInfoTs());
        }
    }

    @Test
    public void testBytesRowsForSelectExecutedSerially() throws Exception {
        String sql = "SELECT * FROM " + tableA + " LIMIT 2";
        List<Object> binds = Lists.newArrayList();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(false);
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 176L, info.getEstimatedBytes());
            assertEquals((Long) 2L, info.getEstimatedRows());
            assertEquals((Long) StatisticsUtil.NOT_STATS_BASED_TS, info.getEstimateInfoTs());
        }
    }
}
