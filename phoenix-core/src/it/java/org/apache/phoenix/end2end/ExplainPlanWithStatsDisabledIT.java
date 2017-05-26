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

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * This class has tests for asserting the bytes and rows information exposed in the explain plan
 * when statistics are disabled.
 */
public class ExplainPlanWithStatsDisabledIT extends ParallelStatsDisabledIT {

    private static String tableA;
    private static String tableB;

    @BeforeClass
    public static void doSetup() throws Exception {
        setUpTestDriver(new ReadOnlyProps(Maps.<String, String> newHashMap()));
        tableA = generateUniqueName();
        initData(tableA);
        tableB = generateUniqueName();
        initData(tableB);
    }

    private static void initData(String tableName) throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + tableName
                    + " ( k INTEGER, c1.a bigint,c2.b bigint CONSTRAINT pk PRIMARY KEY (k))");
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
        }
    }

    @Test
    public void testBytesRowsForSelect() throws Exception {
        String sql = "SELECT * FROM " + tableA + " where k >= ?";
        List<Object> binds = Lists.newArrayList();
        binds.add(99);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            assertEstimatesAreNull(sql, binds, conn);
        }
    }

    @Test
    public void testBytesRowsForUnion() throws Exception {
        String sql = "SELECT * FROM " + tableA + " UNION ALL SELECT * FROM " + tableB;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            assertEstimatesAreNull(sql, Lists.newArrayList(), conn);
        }
    }

    @Test
    public void testBytesRowsForHashJoin() throws Exception {
        String sql =
                "SELECT ta.c1.a, ta.c2.b FROM " + tableA + " ta JOIN " + tableB
                        + " tb ON ta.k = tb.k";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            assertEstimatesAreNull(sql, Lists.newArrayList(), conn);
        }
    }

    @Test
    public void testBytesRowsForSortMergeJoin() throws Exception {
        String sql =
                "SELECT /*+ USE_SORT_MERGE_JOIN */ ta.c1.a, ta.c2.b FROM " + tableA + " ta JOIN "
                        + tableB + " tb ON ta.k = tb.k";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            assertEstimatesAreNull(sql, Lists.newArrayList(), conn);
        }
    }

    @Test
    public void testBytesRowsForAggregateQuery() throws Exception {
        String sql = "SELECT count(*) FROM " + tableA + " where k >= ?";
        List<Object> binds = Lists.newArrayList();
        binds.add(99);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            assertEstimatesAreNull(sql, binds, conn);
        }
    }

    @Test
    public void testBytesRowsForUpsertSelectServerSide() throws Exception {
        String sql = "UPSERT INTO " + tableA + " SELECT * FROM " + tableA;
        List<Object> binds = Lists.newArrayList();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(true);
            assertEstimatesAreNull(sql, binds, conn);
        }
    }

    @Test
    public void testBytesRowsForUpsertSelectClientSide() throws Exception {
        String sql = "UPSERT INTO " + tableA + " SELECT * FROM " + tableA;
        List<Object> binds = Lists.newArrayList();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(false);
            assertEstimatesAreNull(sql, binds, conn);
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
            assertEstimatesAreZero(sql, binds, conn);
        }
    }

    @Test
    public void testBytesRowsForDeleteServerSide() throws Exception {
        String sql = "DELETE FROM " + tableA + " where k >= ?";
        List<Object> binds = Lists.newArrayList();
        binds.add(99);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(true);
            assertEstimatesAreNull(sql, binds, conn);
        }
    }

    @Test
    public void testBytesRowsForDeleteClientSideExecutedSerially() throws Exception {
        String sql = "DELETE FROM " + tableA + " where k >= ? LIMIT 2";
        List<Object> binds = Lists.newArrayList();
        binds.add(99);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(false);
            Pair<Long, Long> info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 200l, info.getSecond());
            assertEquals((Long) 2l, info.getFirst());
        }
    }

    @Test
    public void testBytesRowsForPointDelete() throws Exception {
        String sql = "DELETE FROM " + tableA + " where k = ?";
        List<Object> binds = Lists.newArrayList();
        binds.add(100);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(false);
            assertEstimatesAreZero(sql, binds, conn);
        }
    }
    
    @Test
    public void testBytesRowsForSelectExecutedSerially() throws Exception {
        String sql = "SELECT * FROM " + tableA + " LIMIT 2";
        List<Object> binds = Lists.newArrayList();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(false);
            Pair<Long, Long> info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 200l, info.getSecond());
            assertEquals((Long) 2l, info.getFirst());
        }
    }

    private void assertEstimatesAreNull(String sql, List<Object> binds, Connection conn)
            throws Exception {
        Pair<Long, Long> info = getByteRowEstimates(conn, sql, binds);
        assertNull(info.getSecond());
        assertNull(info.getFirst());
    }

    private void assertEstimatesAreZero(String sql, List<Object> binds, Connection conn)
            throws Exception {
        Pair<Long, Long> info = getByteRowEstimates(conn, sql, binds);
        assertEquals((Long) 0l, info.getSecond());
        assertEquals((Long) 0l, info.getFirst());
    }
}
