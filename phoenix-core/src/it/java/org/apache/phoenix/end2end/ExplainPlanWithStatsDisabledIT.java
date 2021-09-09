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

import static org.apache.phoenix.end2end.ExplainPlanWithStatsEnabledIT.getByteRowEstimates;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

import org.apache.phoenix.end2end.ExplainPlanWithStatsEnabledIT.Estimate;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

/**
 * This class has tests for asserting the bytes and rows information exposed in the explain plan
 * when statistics are disabled.
 */
@Category(ParallelStatsDisabledTest.class)
public class ExplainPlanWithStatsDisabledIT extends ParallelStatsDisabledIT {

    private static void initData(Connection conn, String tableName) throws Exception {
        conn.createStatement().execute("CREATE TABLE " + tableName
                + " ( k INTEGER, c1.a bigint,c2.b bigint CONSTRAINT pk PRIMARY KEY (k)) GUIDE_POSTS_WIDTH = 0");
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
        // Because the guide post width is set to 0, no guide post will be collected
        // effectively disabling stats collection.
        conn.createStatement().execute("UPDATE STATISTICS " + tableName);
    }

    @Test
    public void testBytesRowsForSelect() throws Exception {
        String tableA = generateUniqueName();
        String sql = "SELECT * FROM " + tableA + " where k >= ?";
        List<Object> binds = Lists.newArrayList();
        binds.add(99);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            initData(conn, tableA);
            assertEstimatesAreNull(sql, binds, conn);
        }
    }

    @Test
    public void testBytesRowsForUnion() throws Exception {
        String tableA = generateUniqueName();
        String tableB = generateUniqueName();
        String sql = "SELECT * FROM " + tableA + " UNION ALL SELECT * FROM " + tableB;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            initData(conn, tableA);
            initData(conn, tableB);
            assertEstimatesAreNull(sql, Lists.newArrayList(), conn);
        }
    }

    @Test
    public void testBytesRowsForHashJoin() throws Exception {
        String tableA = generateUniqueName();
        String tableB = generateUniqueName();
        String sql =
                "SELECT ta.c1.a, ta.c2.b FROM " + tableA + " ta JOIN " + tableB
                        + " tb ON ta.k = tb.k";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            initData(conn, tableA);
            initData(conn, tableB);
            assertEstimatesAreNull(sql, Lists.newArrayList(), conn);
        }
    }

    @Test
    public void testBytesRowsForSortMergeJoin() throws Exception {
        String tableA = generateUniqueName();
        String tableB = generateUniqueName();
        String sql =
                "SELECT /*+ USE_SORT_MERGE_JOIN */ ta.c1.a, ta.c2.b FROM " + tableA + " ta JOIN "
                        + tableB + " tb ON ta.k = tb.k";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            initData(conn, tableA);
            initData(conn, tableB);
            assertEstimatesAreNull(sql, Lists.newArrayList(), conn);
        }
    }

    @Test
    public void testBytesRowsForAggregateQuery() throws Exception {
        String tableA = generateUniqueName();
        String sql = "SELECT count(*) FROM " + tableA + " where k >= ?";
        List<Object> binds = Lists.newArrayList();
        binds.add(99);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            initData(conn, tableA);
            assertEstimatesAreNull(sql, binds, conn);
        }
    }

    @Test
    public void testBytesRowsForUpsertSelectServerSide() throws Exception {
        String tableA = generateUniqueName();
        String sql = "UPSERT INTO " + tableA + " SELECT * FROM " + tableA;
        List<Object> binds = Lists.newArrayList();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            initData(conn, tableA);
            conn.setAutoCommit(true);
            assertEstimatesAreNull(sql, binds, conn);
        }
    }

    @Test
    public void testBytesRowsForUpsertSelectClientSide() throws Exception {
        String tableA = generateUniqueName();
        String sql = "UPSERT INTO " + tableA + " SELECT * FROM " + tableA;
        List<Object> binds = Lists.newArrayList();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            initData(conn, tableA);
            conn.setAutoCommit(false);
            assertEstimatesAreNull(sql, binds, conn);
        }
    }

    @Test
    public void testBytesRowsForUpsertValues() throws Exception {
        String tableA = generateUniqueName();
        String sql = "UPSERT INTO " + tableA + " VALUES (?, ?, ?)";
        List<Object> binds = Lists.newArrayList();
        binds.add(99);
        binds.add(99);
        binds.add(99);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            initData(conn, tableA);
            assertEstimatesAreZero(sql, binds, conn);
        }
    }

    @Test
    public void testBytesRowsForDeleteServerSide() throws Exception {
        String tableA = generateUniqueName();
        String sql = "DELETE FROM " + tableA + " where k >= ?";
        List<Object> binds = Lists.newArrayList();
        binds.add(99);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            initData(conn, tableA);
            conn.setAutoCommit(true);
            assertEstimatesAreNull(sql, binds, conn);
        }
    }

    @Test
    public void testBytesRowsForDeleteClientSideExecutedSerially() throws Exception {
        String tableA = generateUniqueName();
        String sql = "DELETE FROM " + tableA + " where k >= ? LIMIT 2";
        List<Object> binds = Lists.newArrayList();
        binds.add(99);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            initData(conn, tableA);
            conn.setAutoCommit(false);
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 200l, info.estimatedBytes);
            assertEquals((Long) 2l, info.estimatedRows);
            assertTrue(info.estimatedRows > 0);
        }
    }

    @Test
    public void testBytesRowsForPointDelete() throws Exception {
        String tableA = generateUniqueName();
        String sql = "DELETE FROM " + tableA + " where k = ?";
        List<Object> binds = Lists.newArrayList();
        binds.add(100);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            initData(conn, tableA);
            conn.setAutoCommit(false);
            assertEstimatesAreZero(sql, binds, conn);
        }
    }

    @Test
    public void testBytesRowsForSelectExecutedSerially() throws Exception {
        String tableA = generateUniqueName();
        String sql = "SELECT * FROM " + tableA + " LIMIT 2";
        List<Object> binds = Lists.newArrayList();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            initData(conn, tableA);
            conn.setAutoCommit(false);
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 200l, info.estimatedBytes);
            assertEquals((Long) 2l, info.estimatedRows);
            assertTrue(info.estimatedRows > 0);
        }
    }

    @Test
    public void testEstimatesForUnionWithTablesWithNullAndLargeGpWidth() throws Exception {
        String tableA = generateUniqueName();
        String tableWithLargeGPWidth = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            initData(conn, tableA);
            // create a table with 1 MB guidepost width
            long guidePostWidth = 1000000;
            conn.createStatement()
                    .execute("CREATE TABLE " + tableWithLargeGPWidth
                            + " ( k INTEGER, c1.a bigint,c2.b bigint CONSTRAINT pk PRIMARY KEY (k)) GUIDE_POSTS_WIDTH="
                            + guidePostWidth);
            conn.createStatement()
                    .execute("upsert into " + tableWithLargeGPWidth + " values (100,1,3)");
            conn.createStatement()
                    .execute("upsert into " + tableWithLargeGPWidth + " values (101,2,4)");
            conn.commit();
            conn.createStatement().execute("UPDATE STATISTICS " + tableWithLargeGPWidth);
        }
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String sql =
                    "SELECT * FROM " + tableA + " UNION ALL SELECT * FROM " + tableWithLargeGPWidth;
            assertEstimatesAreNull(sql, Lists.newArrayList(), conn);
        }
    }

    public static void assertEstimatesAreNull(String sql, List<Object> binds, Connection conn)
            throws Exception {
        Estimate info = getByteRowEstimates(conn, sql, binds);
        assertNull(info.estimatedBytes);
        assertNull(info.estimatedRows);
        assertNull(info.estimateInfoTs);
    }

    private void assertEstimatesAreZero(String sql, List<Object> binds, Connection conn)
            throws Exception {
        Estimate info = getByteRowEstimates(conn, sql, binds);
        assertEquals((Long) 0l, info.estimatedBytes);
        assertEquals((Long) 0l, info.estimatedRows);
        assertEquals((Long) 0l, info.estimateInfoTs);
    }
}
