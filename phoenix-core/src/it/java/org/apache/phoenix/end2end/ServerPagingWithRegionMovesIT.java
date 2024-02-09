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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.iterate.ScanningResultPostDummyResultCaller;
import org.apache.phoenix.iterate.ScanningResultPostValidResultCaller;
import org.apache.phoenix.monitoring.MetricType;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_PAGED_ROWS_COUNTER;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * ServerPagingIT tests that include some region moves while performing rs#next.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class ServerPagingWithRegionMovesIT extends ParallelStatsDisabledIT {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(ServerPagingWithRegionMovesIT.class);

    private static boolean hasTestStarted = false;
    private static int countOfDummyResults = 0;
    private static int countOfValidResults = 0;
    private static final List<String> TABLE_NAMES = Collections.synchronizedList(new ArrayList<>());

    private static class TestScanningResultPostValidResultCaller extends
            ScanningResultPostValidResultCaller {

        @Override
        public void postValidRowProcess() {
            if (hasTestStarted && (countOfValidResults++ % 2) == 0 &&
                    (countOfValidResults < 17 ||
                            countOfValidResults > 28 && countOfValidResults < 40)) {
                LOGGER.info("Moving regions of tables {}. current count of valid results: {}",
                        TABLE_NAMES, countOfValidResults);
                TABLE_NAMES.forEach(table -> {
                    try {
                        moveRegionsOfTable(table);
                    } catch (Exception e) {
                        LOGGER.error("Unable to move regions of table: {}", table);
                    }
                });
            }
        }
    }

    private static class TestScanningResultPostDummyResultCaller extends
            ScanningResultPostDummyResultCaller {

        @Override
        public void postDummyProcess() {
            if (hasTestStarted && (countOfDummyResults++ % 3) == 0 &&
                    (countOfDummyResults < 17 ||
                            countOfDummyResults > 28 && countOfDummyResults < 40)) {
                LOGGER.info("Moving regions of tables {}. current count of dummy results: {}",
                        TABLE_NAMES, countOfDummyResults);
                TABLE_NAMES.forEach(table -> {
                    try {
                        moveRegionsOfTable(table);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }
    }

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(2);
        props.put(QueryServices.PHOENIX_SERVER_PAGE_SIZE_MS, Long.toString(0));
        props.put(QueryServices.COLLECT_REQUEST_LEVEL_METRICS, String.valueOf(true));
        props.put(QueryServices.TESTS_MINI_CLUSTER_NUM_REGION_SERVERS, String.valueOf(2));
        props.put(HConstants.HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY, String.valueOf(1));
        props.put(QueryServices.PHOENIX_POST_DUMMY_PROCESS,
                TestScanningResultPostDummyResultCaller.class.getName());
        props.put(QueryServices.PHOENIX_POST_VALID_PROCESS,
                TestScanningResultPostValidResultCaller.class.getName());
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @After
    public void tearDown() throws Exception {
        TABLE_NAMES.clear();
        hasTestStarted = false;
        countOfDummyResults = 0;
        countOfValidResults = 0;
    }

    private void assertServerPagingMetric(String tableName, ResultSet rs, boolean isPaged)
            throws SQLException {
        Map<String, Map<MetricType, Long>> metrics = PhoenixRuntime.getRequestReadMetricInfo(rs);
        for (Map.Entry<String, Map<MetricType, Long>> entry : metrics.entrySet()) {
            assertEquals(String.format("Got %s", entry.getKey()), tableName, entry.getKey());
            Map<MetricType, Long> metricValues = entry.getValue();
            Long pagedRowsCntr = metricValues.get(MetricType.PAGED_ROWS_COUNTER);
            assertNotNull(pagedRowsCntr);
            if (isPaged) {
                assertTrue(String.format("Got %d", pagedRowsCntr), pagedRowsCntr > 0);
            } else {
                assertEquals(String.format("Got %d", pagedRowsCntr), 0, (long) pagedRowsCntr);
            }
        }
        assertTrue(GLOBAL_PAGED_ROWS_COUNTER.getMetric().getValue() > 0);
    }

    @Test
    public void testOrderByNonAggregation() throws Exception {
        hasTestStarted = true;
        final String tablename = generateUniqueName();
        final String tenantId = getOrganizationId();

        final Date D1 = DateUtil.parseDate("1970-01-01 00:58:00");
        final Date D2 = DateUtil.parseDate("1970-01-01 01:02:00");
        final Date D3 = DateUtil.parseDate("1970-01-01 01:30:00");
        final Date D4 = DateUtil.parseDate("1970-01-01 01:45:00");
        final Date D5 = DateUtil.parseDate("1970-01-01 02:00:00");
        final Date D6 = DateUtil.parseDate("1970-01-01 04:00:00");
        final String F1 = "A";
        final String F2 = "B";
        final String F3 = "C";
        final String R1 = "R1";
        final String R2 = "R2";
        byte[][] splits = new byte[][] {
                ByteUtil.concat(Bytes.toBytes(tenantId), PDate.INSTANCE.toBytes(D3)),
                ByteUtil.concat(Bytes.toBytes(tenantId), PDate.INSTANCE.toBytes(D5)),
        };
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String ddl = "create table " + tablename +
                    "   (organization_id char(15) not null," +
                    "    date date not null," +
                    "    feature char(1) not null," +
                    "    unique_users integer not null,\n" +
                    "    transactions bigint,\n" +
                    "    region varchar,\n" +
                    "    CONSTRAINT pk PRIMARY KEY (organization_id, \"DATE\", feature, unique_users))";
            TABLE_NAMES.add(tablename);
            StringBuilder buf = new StringBuilder(ddl);
            if (splits != null) {
                buf.append(" SPLIT ON (");
                for (int i = 0; i < splits.length; i++) {
                    buf.append("'").append(Bytes.toString(splits[i])).append("'").append(",");
                }
                buf.setCharAt(buf.length()-1, ')');
            }
            ddl = buf.toString();
            conn.createStatement().execute(ddl);

            PreparedStatement stmt = conn.prepareStatement(
                    "upsert into " + tablename +
                            " (" +
                            "    ORGANIZATION_ID, " +
                            "    \"DATE\", " +
                            "    FEATURE, " +
                            "    UNIQUE_USERS, " +
                            "    TRANSACTIONS, " +
                            "    REGION) " +
                            "VALUES (?, ?, ?, ?, ?, ?)");
            stmt.setString(1, tenantId);
            stmt.setDate(2, D1);
            stmt.setString(3, F1);
            stmt.setInt(4, 10);
            stmt.setLong(5, 100L);
            stmt.setString(6, R2);
            stmt.execute();

            stmt.setString(1, tenantId);
            stmt.setDate(2, D2);
            stmt.setString(3, F1);
            stmt.setInt(4, 20);
            stmt.setLong(5, 200);
            stmt.setString(6, null);
            stmt.execute();

            stmt.setString(1, tenantId);
            stmt.setDate(2, D3);
            stmt.setString(3, F1);
            stmt.setInt(4, 30);
            stmt.setLong(5, 300);
            stmt.setString(6, R1);
            stmt.execute();

            stmt.setString(1, tenantId);
            stmt.setDate(2, D4);
            stmt.setString(3, F2);
            stmt.setInt(4, 40);
            stmt.setLong(5, 400);
            stmt.setString(6, R1);
            stmt.execute();

            stmt.setString(1, tenantId);
            stmt.setDate(2, D5);
            stmt.setString(3, F3);
            stmt.setInt(4, 50);
            stmt.setLong(5, 500);
            stmt.setString(6, R2);
            stmt.execute();

            stmt.setString(1, tenantId);
            stmt.setDate(2, D6);
            stmt.setString(3, F1);
            stmt.setInt(4, 60);
            stmt.setLong(5, 600);
            stmt.setString(6, null);
            stmt.execute();
            conn.commit();
        }

        String query = "SELECT \"DATE\", transactions t FROM "+tablename+
                " WHERE organization_id=? AND unique_users <= 30 ORDER BY t DESC LIMIT 2";
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             PreparedStatement statement = conn.prepareStatement(query)) {
            TestUtil.dumpTable(conn, TableName.valueOf(tablename));
            statement.setString(1, tenantId);
            try (ResultSet rs = statement.executeQuery()) {
                moveRegionsOfTable(tablename);
                assertTrue(rs.next());
                assertEquals(D3.getTime(), rs.getDate(1).getTime());
                moveRegionsOfTable(tablename);
                assertTrue(rs.next());
                moveRegionsOfTable(tablename);
                assertEquals(D2.getTime(), rs.getDate(1).getTime());
                assertFalse(rs.next());
                assertServerPagingMetric(tablename, rs, true);
            }
        }
    }

    private static void moveRegionsOfTable(String tableName)
            throws IOException {
        Admin admin = getUtility().getAdmin();
        List<ServerName> servers =
                new ArrayList<>(admin.getRegionServers());
        ServerName server1 = servers.get(0);
        ServerName server2 = servers.get(1);
        List<RegionInfo> regionsOnServer1;
        try {
            regionsOnServer1 = admin.getRegions(server1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        List<RegionInfo> regionsOnServer2;
        try {
            regionsOnServer2 = admin.getRegions(server2);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        regionsOnServer1.forEach(regionInfo -> {
            if (regionInfo.getTable().equals(TableName.valueOf(tableName))) {
                try {
                    admin.move(regionInfo.getEncodedNameAsBytes(), server2);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        regionsOnServer2.forEach(regionInfo -> {
            if (regionInfo.getTable().equals(TableName.valueOf(tableName))) {
                try {
                    admin.move(regionInfo.getEncodedNameAsBytes(), server1);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private static void moveAllRegions() throws IOException {
        Admin admin = getUtility().getAdmin();
        List<ServerName> servers =
                new ArrayList<>(admin.getRegionServers());
        ServerName server1 = servers.get(0);
        ServerName server2 = servers.get(1);
        List<RegionInfo> regionsOnServer1;
        try {
            regionsOnServer1 = admin.getRegions(server1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        List<RegionInfo> regionsOnServer2;
        try {
            regionsOnServer2 = admin.getRegions(server2);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        regionsOnServer1.forEach(regionInfo -> {
            try {
                admin.move(regionInfo.getEncodedNameAsBytes(), server2);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        regionsOnServer2.forEach(regionInfo -> {
            try {
                admin.move(regionInfo.getEncodedNameAsBytes(), server1);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testLimitOffsetWithSplit() throws Exception {
        hasTestStarted = true;
        final String tablename = generateUniqueName();
        final String[] STRINGS = { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l",
                "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z" };
        String ddl = "CREATE TABLE " + tablename + " (t_id VARCHAR NOT NULL,\n"
                + "k1 INTEGER NOT NULL,\n" + "k2 INTEGER NOT NULL,\n" + "C3.k3 INTEGER,\n"
                + "C2.v1 VARCHAR,\n" + "CONSTRAINT pk PRIMARY KEY (t_id, k1, k2)) "
                + "SPLIT ON ('e','i','o')";
        TABLE_NAMES.add(tablename);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            createTestTable(getUrl(), ddl);
            for (int i = 0; i < 26; i++) {
                conn.createStatement().execute("UPSERT INTO " + tablename + " values('"
                        + STRINGS[i] + "'," + i + ","
                        + (i + 1) + "," + (i + 2) + ",'" + STRINGS[25 - i] + "')");
            }
            conn.commit();
            int limit = 10;
            // Testing 0 as remaining offset after 4 rows in first region, 4 rows in second region
            int offset = 8;
            ResultSet rs;
            rs = conn.createStatement()
                    .executeQuery("SELECT t_id from " + tablename + " order by t_id limit "
                            + limit + " offset " + offset);
            int i = 0;
            while (i < limit) {
                if (i % 3 == 0) {
                    moveRegionsOfTable(tablename);
                }
                assertTrue(rs.next());
                assertEquals("Expected string didn't match for i = " + i, STRINGS[offset + i],
                        rs.getString(1));
                i++;
            }
            assertServerPagingMetric(tablename, rs, true);

            // Testing query with offset + filter
            int filterCond = 10;
            rs = conn.createStatement().executeQuery(
                    "SELECT t_id from " + tablename + " where k2 > " + filterCond +
                            " order by t_id limit " + limit + " offset " + offset);
            i = 0;
            limit = 5;
            while (i < limit) {
                if (i % 4 == 0) {
                    moveRegionsOfTable(tablename);
                }
                assertTrue(rs.next());
                assertEquals("Expected string didn't match for i = " + i,
                        STRINGS[offset + filterCond + i], rs.getString(1));
                i++;
            }
            assertServerPagingMetric(tablename, rs, true);

            limit = 35;
            rs = conn.createStatement().executeQuery("SELECT t_id from " + tablename
                    + " union all SELECT t_id from "
                    + tablename + " offset " + offset + " FETCH FIRST " + limit + " rows only");
            i = 0;
            while (i++ < STRINGS.length - offset) {
                if (i % 3 == 0) {
                    moveRegionsOfTable(tablename);
                }
                assertTrue(rs.next());
                assertEquals(STRINGS[offset + i - 1], rs.getString(1));
            }
            i = 0;
            while (i++ < limit - STRINGS.length - offset) {
                if (i % 3 == 0) {
                    moveRegionsOfTable(tablename);
                }
                assertTrue(rs.next());
                assertEquals(STRINGS[i - 1], rs.getString(1));
            }
            // no paging when serial offset
            assertServerPagingMetric(tablename, rs, true);
            limit = 1;
            offset = 1;
            rs = conn.createStatement()
                    .executeQuery("SELECT k2 from " + tablename + " order by k2 desc limit "
                            + limit + " offset " + offset);
            moveRegionsOfTable(tablename);
            assertTrue(rs.next());
            assertEquals(25, rs.getInt(1));
            assertFalse(rs.next());
            assertServerPagingMetric(tablename, rs, true);
        }
    }

    @Test
    public void testLimitOffsetWithoutSplit() throws Exception {
        hasTestStarted = true;
        final String tablename = generateUniqueName();
        final String[] STRINGS = { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l",
                "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z" };
        String ddl = "CREATE TABLE " + tablename + " (t_id VARCHAR NOT NULL,\n"
                + "k1 INTEGER NOT NULL,\n" + "k2 INTEGER NOT NULL,\n" + "C3.k3 INTEGER,\n"
                + "C2.v1 VARCHAR,\n" + "CONSTRAINT pk PRIMARY KEY (t_id, k1, k2))";
        TABLE_NAMES.add(tablename);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            createTestTable(getUrl(), ddl);
            for (int i = 0; i < 26; i++) {
                conn.createStatement().execute("UPSERT INTO " + tablename + " values('"
                        + STRINGS[i] + "'," + i + ","
                        + (i + 1) + "," + (i + 2) + ",'" + STRINGS[25 - i] + "')");
            }
            conn.commit();
            int limit = 10;
            // Testing 0 as remaining offset after 4 rows in first region, 4 rows in second region
            int offset = 8;
            ResultSet rs;
            rs = conn.createStatement()
                    .executeQuery("SELECT t_id from " + tablename + " order by t_id limit "
                            + limit + " offset " + offset);
            int i = 0;
            while (i < limit) {
                if (i % 3 == 0) {
                    moveRegionsOfTable(tablename);
                }
                assertTrue(rs.next());
                assertEquals("Expected string didn't match for i = " + i, STRINGS[offset + i],
                        rs.getString(1));
                i++;
            }
            assertServerPagingMetric(tablename, rs, false);

            // Testing query with offset + filter
            int filterCond = 10;
            rs = conn.createStatement().executeQuery(
                    "SELECT t_id from " + tablename + " where k2 > " + filterCond +
                            " order by t_id limit " + limit + " offset " + offset);
            i = 0;
            limit = 5;
            while (i < limit) {
                if (i % 4 == 0) {
                    moveRegionsOfTable(tablename);
                }
                assertTrue(rs.next());
                assertEquals("Expected string didn't match for i = " + i,
                        STRINGS[offset + filterCond + i], rs.getString(1));
                i++;
            }
            assertServerPagingMetric(tablename, rs, false);

            limit = 35;
            rs = conn.createStatement().executeQuery("SELECT t_id from " + tablename
                    + " union all SELECT t_id from "
                    + tablename + " offset " + offset + " FETCH FIRST " + limit + " rows only");
            i = 0;
            while (i++ < STRINGS.length - offset) {
                if (i % 3 == 0) {
                    moveRegionsOfTable(tablename);
                }
                assertTrue(rs.next());
                assertEquals(STRINGS[offset + i - 1], rs.getString(1));
            }
            i = 0;
            while (i++ < limit - STRINGS.length - offset) {
                if (i % 3 == 0) {
                    moveRegionsOfTable(tablename);
                }
                assertTrue(rs.next());
                assertEquals(STRINGS[i - 1], rs.getString(1));
            }
            assertServerPagingMetric(tablename, rs, false);
            limit = 1;
            offset = 1;
            rs = conn.createStatement()
                    .executeQuery("SELECT k2 from " + tablename + " order by k2 desc limit "
                            + limit + " offset " + offset);
            moveRegionsOfTable(tablename);
            assertTrue(rs.next());
            assertEquals(25, rs.getInt(1));
            assertFalse(rs.next());
            assertServerPagingMetric(tablename, rs, true);
        }
    }

    @Test
    public void testLimitOffsetWithSplit2() throws Exception {
        hasTestStarted = true;
        final String tablename = generateUniqueName();
        final String[] STRINGS = {"a_xyz", "b_xyz", "c_xyz", "d_xyz", "e_xyz", "f_xyz", "g_xyz",
                "h_xyz", "i_xyz", "j_xyz", "k_xyz", "l_xyz",
                "m_xyz", "n_xyz", "o_xyz", "p_xyz", "q_xyz", "r_xyz", "s_xyz", "t_xyz", "u_xyz",
                "v_xyz", "w_xyz", "x_xyz", "y_xyz", "z_xyz"};
        String ddl = "CREATE TABLE " + tablename + " (t_id VARCHAR,\n"
                + "k1 INTEGER,\n" + "k2 INTEGER,\n" + "C3.k3 INTEGER,\n"
                + "C2.v1 VARCHAR,\n" + "CONSTRAINT pk PRIMARY KEY (t_id)) "
                + "SPLIT ON ('e123','i123','o123')";
        TABLE_NAMES.add(tablename);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            createTestTable(getUrl(), ddl);
            for (int i = 0; i < 26; i++) {
                conn.createStatement().execute("UPSERT INTO " + tablename + " values('"
                        + STRINGS[i] + "'," + (i + 1) * 2 + ","
                        + (i + 1) * 3 + "," + (i + 2) * 2 + ",'" + STRINGS[25 - i] + "')");
            }
            conn.commit();
            int limit = 10;
            // Testing 0 as remaining offset after 4 rows in first region, 4 rows in second region
            int offset = 8;
            ResultSet rs;
            rs = conn.createStatement()
                    .executeQuery("SELECT t_id from " + tablename + " order by t_id limit "
                            + limit + " offset " + offset);
            int i = 0;
            while (i < limit) {
                if (i % 3 == 0) {
                    moveRegionsOfTable(tablename);
                }
                assertTrue(rs.next());
                assertEquals("Expected string didn't match for i = " + i, STRINGS[offset + i],
                        rs.getString(1));
                i++;
            }
            assertServerPagingMetric(tablename, rs, true);

            // Testing query with offset + filter
            int filterCond = 30;
            rs = conn.createStatement().executeQuery(
                    "SELECT t_id from " + tablename + " where k2 > " + filterCond +
                            " order by t_id limit " + limit + " offset " + offset);
            i = 0;
            limit = 5;
            while (i < limit) {
                if (i % 4 == 0) {
                    moveRegionsOfTable(tablename);
                }
                assertTrue(rs.next());
                assertEquals("Expected string didn't match for i = " + i,
                        STRINGS[offset + 10 + i], rs.getString(1));
                i++;
            }
            assertServerPagingMetric(tablename, rs, true);

            limit = 35;
            rs = conn.createStatement().executeQuery("SELECT t_id from " + tablename
                    + " union all SELECT t_id from "
                    + tablename + " offset " + offset + " FETCH FIRST " + limit + " rows only");
            i = 0;
            while (i++ < STRINGS.length - offset) {
                if (i % 3 == 0) {
                    moveRegionsOfTable(tablename);
                }
                assertTrue(rs.next());
                assertEquals(STRINGS[offset + i - 1], rs.getString(1));
            }
            i = 0;
            while (i++ < limit - STRINGS.length - offset) {
                if (i % 3 == 0) {
                    moveRegionsOfTable(tablename);
                }
                assertTrue(rs.next());
                assertEquals(STRINGS[i - 1], rs.getString(1));
            }
            // no paging when serial offset
            assertServerPagingMetric(tablename, rs, true);
            limit = 1;
            offset = 1;
            rs = conn.createStatement()
                    .executeQuery("SELECT k2 from " + tablename + " order by k2 desc limit "
                            + limit + " offset " + offset);
            moveRegionsOfTable(tablename);
            assertTrue(rs.next());
            assertEquals(75, rs.getInt(1));
            assertFalse(rs.next());
            assertServerPagingMetric(tablename, rs, true);
        }
    }

    @Test
    public void testLimitOffsetWithoutSplit2() throws Exception {
        hasTestStarted = true;
        final String tablename = generateUniqueName();
        final String[] STRINGS = {"a_xyz", "b_xyz", "c_xyz", "d_xyz", "e_xyz", "f_xyz", "g_xyz",
                "h_xyz", "i_xyz", "j_xyz", "k_xyz", "l_xyz",
                "m_xyz", "n_xyz", "o_xyz", "p_xyz", "q_xyz", "r_xyz", "s_xyz", "t_xyz", "u_xyz",
                "v_xyz", "w_xyz", "x_xyz", "y_xyz", "z_xyz"};
        String ddl = "CREATE TABLE " + tablename + " (t_id VARCHAR,\n"
                + "k1 INTEGER,\n" + "k2 INTEGER,\n" + "C3.k3 INTEGER,\n"
                + "C2.v1 VARCHAR,\n" + "CONSTRAINT pk PRIMARY KEY (t_id))";
        TABLE_NAMES.add(tablename);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            createTestTable(getUrl(), ddl);
            for (int i = 0; i < 26; i++) {
                conn.createStatement().execute("UPSERT INTO " + tablename + " values('"
                        + STRINGS[i] + "'," + (i + 1) * 2 + ","
                        + (i + 1) * 3 + "," + (i + 2) * 2 + ",'" + STRINGS[25 - i] + "')");
            }
            conn.commit();
            int limit = 10;
            // Testing 0 as remaining offset after 4 rows in first region, 4 rows in second region
            int offset = 8;
            ResultSet rs;
            rs = conn.createStatement()
                    .executeQuery("SELECT t_id from " + tablename + " order by t_id limit "
                            + limit + " offset " + offset);
            int i = 0;
            while (i < limit) {
                if (i % 3 == 0) {
                    moveRegionsOfTable(tablename);
                }
                assertTrue(rs.next());
                assertEquals("Expected string didn't match for i = " + i, STRINGS[offset + i],
                        rs.getString(1));
                i++;
            }
            assertServerPagingMetric(tablename, rs, false);

            // Testing query with offset + filter
            int filterCond = 30;
            rs = conn.createStatement().executeQuery(
                    "SELECT t_id from " + tablename + " where k2 > " + filterCond +
                            " order by t_id limit " + limit + " offset " + offset);
            i = 0;
            limit = 5;
            while (i < limit) {
                if (i % 4 == 0) {
                    moveRegionsOfTable(tablename);
                }
                assertTrue(rs.next());
                assertEquals("Expected string didn't match for i = " + i,
                        STRINGS[offset + 10 + i], rs.getString(1));
                i++;
            }
            assertServerPagingMetric(tablename, rs, false);

            limit = 35;
            rs = conn.createStatement().executeQuery("SELECT t_id from " + tablename
                    + " union all SELECT t_id from "
                    + tablename + " offset " + offset + " FETCH FIRST " + limit + " rows only");
            i = 0;
            while (i++ < STRINGS.length - offset) {
                if (i % 3 == 0) {
                    moveRegionsOfTable(tablename);
                }
                assertTrue(rs.next());
                assertEquals(STRINGS[offset + i - 1], rs.getString(1));
            }
            i = 0;
            while (i++ < limit - STRINGS.length - offset) {
                if (i % 3 == 0) {
                    moveRegionsOfTable(tablename);
                }
                assertTrue(rs.next());
                assertEquals(STRINGS[i - 1], rs.getString(1));
            }
            // no paging when serial offset
            assertServerPagingMetric(tablename, rs, false);
            limit = 1;
            offset = 1;
            rs = conn.createStatement()
                    .executeQuery("SELECT k2 from " + tablename + " order by k2 desc limit "
                            + limit + " offset " + offset);
            moveRegionsOfTable(tablename);
            assertTrue(rs.next());
            assertEquals(75, rs.getInt(1));
            assertFalse(rs.next());
            assertServerPagingMetric(tablename, rs, true);
        }
    }

    @Test
    public void testGroupBy() throws Exception {
        hasTestStarted = true;
        final String tablename = generateUniqueName();
        TABLE_NAMES.add(tablename);
        String ddl = "CREATE TABLE " + tablename + " (t_id VARCHAR NOT NULL,\n"
                + "k1 INTEGER NOT NULL,\n"
                + "k2 INTEGER CONSTRAINT pk PRIMARY KEY (t_id, k1)) ";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            createTestTable(getUrl(), ddl);
            for (int i = 0; i < 8; i++) {
                conn.createStatement().execute("UPSERT INTO " + tablename
                        + " values('tenant1'," + i + ","
                        + (i + 1) + ")");
            }
            conn.createStatement()
                    .execute("UPSERT INTO " + tablename + " values('tenant1', 10, 2)");
            conn.createStatement()
                    .execute("UPSERT INTO " + tablename + " values('tenant1', 11, 2)");
            conn.createStatement()
                    .execute("UPSERT INTO " + tablename + " values('tenant1', 12, 3)");
            conn.createStatement()
                    .execute("UPSERT INTO " + tablename + " values('tenant1', 13, 3)");
            conn.createStatement()
                    .execute("UPSERT INTO " + tablename + " values('tenant1', 14, 4)");
            conn.createStatement()
                    .execute("UPSERT INTO " + tablename + " values('tenant1', 15, 4)");
            conn.createStatement()
                    .execute("UPSERT INTO " + tablename + " values('tenant1', 16, 4)");
            conn.createStatement()
                    .execute("UPSERT INTO " + tablename + " values('tenant1', 17, 5)");
            conn.createStatement()
                    .execute("UPSERT INTO " + tablename + " values('tenant1', 18, 5)");
            conn.commit();
            TestUtil.dumpTable(conn, TableName.valueOf(tablename));
            ResultSet rs =
                    conn.createStatement().executeQuery("SELECT k2, count(*) FROM " + tablename
                    + " where t_id = 'tenant1' group by k2");
            moveRegionsOfTable(tablename);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));
            Assert.assertEquals(1, rs.getInt(2));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(2, rs.getInt(1));
            Assert.assertEquals(3, rs.getInt(2));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(3, rs.getInt(1));
            Assert.assertEquals(3, rs.getInt(2));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(4, rs.getInt(1));
            Assert.assertEquals(4, rs.getInt(2));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(5, rs.getInt(1));
            Assert.assertEquals(3, rs.getInt(2));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(6, rs.getInt(1));
            Assert.assertEquals(1, rs.getInt(2));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(7, rs.getInt(1));
            Assert.assertEquals(1, rs.getInt(2));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(8, rs.getInt(1));
            Assert.assertEquals(1, rs.getInt(2));
            Assert.assertFalse(rs.next());
            assertServerPagingMetric(tablename, rs, true);
        }
    }

    @Test
    public void testGroupByWithIndex() throws Exception {
        hasTestStarted = true;
        final String tablename = generateUniqueName();
        final String indexName = generateUniqueName();
        TABLE_NAMES.add(tablename);
        TABLE_NAMES.add(indexName);

        String ddl = "CREATE TABLE " + tablename + " (t_id VARCHAR NOT NULL,\n"
                + "k1 INTEGER NOT NULL,\n"
                + "k2 INTEGER CONSTRAINT pk PRIMARY KEY (t_id, k1)) ";
        String indexDDl = "CREATE INDEX IF NOT EXISTS " +  indexName + " ON "
                + tablename + "(k2)";

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            createTestTable(getUrl(), ddl);
            createTestTable(getUrl(), indexDDl);
            for (int i = 0; i < 8; i++) {
                conn.createStatement().execute("UPSERT INTO " + tablename
                        + " values('tenant1'," + i + ","
                        + (i + 1) + ")");
            }
            conn.commit();
            ResultSet rs = conn.createStatement().executeQuery("SELECT count(*) FROM " + tablename
                    + " where t_id = 'tenant1' AND (k2 IN (5,6) or k2 is null) group by k2=6");
            boolean moveRegions = true;
            moveRegionsOfTable(tablename);
            moveRegionsOfTable(indexName);
            while (rs.next()) {
                if (moveRegions) {
                    moveRegionsOfTable(tablename);
                    moveRegionsOfTable(indexName);
                    moveRegions = false;
                } else {
                    moveRegions = true;
                }
                Assert.assertEquals(1, rs.getInt(1));
            }
            Assert.assertFalse(rs.next());
            assertServerPagingMetric(indexName, rs, true);
        }
    }

}
