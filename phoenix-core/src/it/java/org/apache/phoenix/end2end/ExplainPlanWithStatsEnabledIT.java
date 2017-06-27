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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * This class has tests for asserting the bytes and rows information exposed in the explain plan
 * when statistics are enabled.
 */
public class ExplainPlanWithStatsEnabledIT extends ParallelStatsEnabledIT {

    private static String tableA;
    private static String tableB;

    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(20));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
        tableA = generateUniqueName();
        initDataAndStats(tableA);
        tableB = generateUniqueName();
        initDataAndStats(tableB);
    }

    private static void initDataAndStats(String tableName) throws Exception {
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
            conn.createStatement().execute("UPDATE STATISTICS " + tableName);
        }
    }

    @Test
    public void testBytesRowsForSelect() throws Exception {
        String sql = "SELECT * FROM " + tableA + " where k >= ?";
        List<Object> binds = Lists.newArrayList();
        binds.add(99);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Pair<Long, Long> info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 634l, info.getSecond());
            assertEquals((Long) 10l, info.getFirst());
        }
    }

    @Test
    public void testBytesRowsForUnion() throws Exception {
        String sql = "SELECT * FROM " + tableA + " UNION ALL SELECT * FROM " + tableB;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Pair<Long, Long> info = getByteRowEstimates(conn, sql, Lists.newArrayList());
            assertEquals((Long) (2 * 634l), info.getSecond());
            assertEquals((Long) (2 * 10l), info.getFirst());
        }
    }

    @Test
    public void testBytesRowsForHashJoin() throws Exception {
        String sql =
                "SELECT ta.c1.a, ta.c2.b FROM " + tableA + " ta JOIN " + tableB
                        + " tb ON ta.k = tb.k";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Pair<Long, Long> info = getByteRowEstimates(conn, sql, Lists.newArrayList());
            assertEquals((Long) (634l), info.getSecond());
            assertEquals((Long) (10l), info.getFirst());
        }
    }

    @Test
    public void testBytesRowsForSortMergeJoin() throws Exception {
        String sql =
                "SELECT /*+ USE_SORT_MERGE_JOIN */ ta.c1.a, ta.c2.b FROM " + tableA + " ta JOIN "
                        + tableB + " tb ON ta.k = tb.k";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Pair<Long, Long> info = getByteRowEstimates(conn, sql, Lists.newArrayList());
            assertEquals((Long) (2 * 634l), info.getSecond());
            assertEquals((Long) (2 * 10l), info.getFirst());
        }
    }

    @Test
    public void testBytesRowsForAggregateQuery() throws Exception {
        String sql = "SELECT count(*) FROM " + tableA + " where k >= ?";
        List<Object> binds = Lists.newArrayList();
        binds.add(99);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Pair<Long, Long> info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 634l, info.getSecond());
            assertEquals((Long) 10l, info.getFirst());
        }
    }

    @Test
    public void testBytesRowsForUpsertSelectServerSide() throws Exception {
        String sql = "UPSERT INTO " + tableA + " SELECT * FROM " + tableA;
        List<Object> binds = Lists.newArrayList();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(true);
            Pair<Long, Long> info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 634l, info.getSecond());
            assertEquals((Long) 10l, info.getFirst());
        }
    }

    @Test
    public void testBytesRowsForUpsertSelectClientSide() throws Exception {
        String sql = "UPSERT INTO " + tableA + " SELECT * FROM " + tableA;
        List<Object> binds = Lists.newArrayList();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(false);
            Pair<Long, Long> info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 634l, info.getSecond());
            assertEquals((Long) 10l, info.getFirst());
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
            Pair<Long, Long> info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 0l, info.getSecond());
            assertEquals((Long) 0l, info.getFirst());
        }
    }

    @Test
    public void testBytesRowsForDeleteServerSide() throws Exception {
        String sql = "DELETE FROM " + tableA + " where k >= ?";
        List<Object> binds = Lists.newArrayList();
        binds.add(99);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(true);
            Pair<Long, Long> info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 634l, info.getSecond());
            assertEquals((Long) 10l, info.getFirst());
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
            Pair<Long, Long> info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 0l, info.getSecond());
            assertEquals((Long) 0l, info.getFirst());
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

    public static Pair<Long, Long> getByteRowEstimates(Connection conn, String sql,
            List<Object> bindValues) throws Exception {
        String explainSql = "EXPLAIN " + sql;
        Long estimatedBytes = null;
        Long estimatedRows = null;
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
        }
        return new Pair<>(estimatedRows, estimatedBytes);
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
            assertUseStatsForQueryFlag(table, conn.unwrap(PhoenixConnection.class), DEFAULT_USE_STATS_FOR_PARALLELIZATION);
        }
    }

    private static void assertUseStatsForQueryFlag(String tableName, PhoenixConnection conn,
            boolean flag) throws TableNotFoundException, SQLException {
        assertEquals(flag, conn.unwrap(PhoenixConnection.class).getMetaDataCache()
                .getTableRef(new PTableKey(null, tableName)).getTable().useStatsForParallelization());
        String query =
                "SELECT USE_STATS_FOR_PARALLELIZATION FROM SYSTEM.CATALOG WHERE TABLE_NAME = ? AND COLUMN_NAME IS NULL AND COLUMN_FAMILY IS NULL AND TENANT_ID IS NULL";
        PreparedStatement stmt = conn.prepareStatement(query);
        stmt.setString(1, tableName);
        ResultSet rs = stmt.executeQuery();
        rs.next();
        assertEquals(flag, rs.getBoolean(1));
    }
}
