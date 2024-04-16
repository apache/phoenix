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

import static org.apache.phoenix.end2end.index.GlobalIndexCheckerIT.assertExplainPlan;
import static org.apache.phoenix.end2end.index.GlobalIndexCheckerIT.assertExplainPlanWithLimit;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_PAGED_ROWS_COUNTER;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.monitoring.MetricType;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(NeedsOwnMiniClusterTest.class)
public class ServerPagingIT extends ParallelStatsDisabledIT {

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(2);
        props.put(QueryServices.PHOENIX_SERVER_PAGE_SIZE_MS, Long.toString(0));
        props.put(QueryServices.COLLECT_REQUEST_LEVEL_METRICS, String.valueOf(true));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    private void assertServerPagingMetric(String tableName, ResultSet rs, boolean isPaged) throws SQLException {
        Map<String, Map<MetricType, Long>> metrics = PhoenixRuntime.getRequestReadMetricInfo(rs);
        for (Map.Entry<String, Map<MetricType, Long>> entry : metrics.entrySet()) {
            assertEquals(String.format("Got %s", entry.getKey()), tableName, entry.getKey());
            Map<MetricType, Long> metricValues = entry.getValue();
            Long pagedRowsCntr = metricValues.get(MetricType.PAGED_ROWS_COUNTER);
            assertNotNull(pagedRowsCntr);
            if (isPaged) {
                assertTrue(String.format("Got %d", pagedRowsCntr.longValue()), pagedRowsCntr > 0);
            } else {
                assertTrue(String.format("Got %d", pagedRowsCntr.longValue()), pagedRowsCntr == 0);
            }
        }
        assertTrue(GLOBAL_PAGED_ROWS_COUNTER.getMetric().getValue() > 0);
    }

    @Test
    public void testScanWithLimit() throws Exception {
        final String tablename = "T_" + generateUniqueName();
        final String indexName = "I_" + generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String ddl = "create table " + tablename +
                    "(id1 integer not null, id2 integer not null, val varchar constraint pk primary key (id1, id2))";
            conn.createStatement().execute(ddl);
            conn.commit();
            PreparedStatement stmt = conn.prepareStatement("upsert into " + tablename + " VALUES(?, ?, ?)");
            // upsert 50 rows
            for (int i = 0; i < 5; ++i) {
                for (int j = 0; j < 10; ++j) {
                    stmt.setInt(1, i);
                    stmt.setInt(2, j);
                    stmt.setString(3, "abcdefghijklmnopqrstuvwxyz");
                    stmt.executeUpdate();
                }
                conn.commit();
            }
            // delete first 40 rows
            stmt = conn.prepareStatement("delete from " + tablename + " where id1 = ? and id2 = ?");
            for (int i = 0; i < 4; ++i) {
                for (int j = 0; j < 10; ++j) {
                    stmt.setInt(1, i);
                    stmt.setInt(2, j);
                    stmt.executeUpdate();
                }
                conn.commit();
            }
            int limit = 10;
            stmt = conn.prepareStatement("select * from " + tablename + " where id1 >= 3 limit " + limit);
            try (ResultSet rs = stmt.executeQuery()) {
                int expectedRowCount = 0;
                int expectedId1 = 4;
                int expectedId2 = 0;
                while (rs.next()) {
                    ++expectedRowCount;
                    assertEquals(expectedId1, rs.getInt(1));
                    assertEquals(expectedId2, rs.getInt(2));
                    expectedId2++;
                }
                assertEquals(expectedRowCount, limit);
                assertServerPagingMetric(tablename, rs, true);
            }

            ddl = "create index " + indexName + " ON " + tablename + " (id2, id1) INCLUDE (val)";
            conn.createStatement().execute(ddl);
            conn.commit();

            stmt = conn.prepareStatement("select * from " + tablename + " limit " + limit);
            try (ResultSet rs = stmt.executeQuery()) {
                PhoenixResultSet prs = rs.unwrap(PhoenixResultSet.class);
                String explainPlan = QueryUtil.getExplainPlan(prs.getUnderlyingIterator());
                assertTrue(explainPlan.contains(indexName));
                int expectedRowCount = 0;
                int expectedId1 = 4;
                int expectedId2 = 0;
                while (rs.next()) {
                    ++expectedRowCount;
                    assertEquals(expectedId1, rs.getInt(1));
                    assertEquals(expectedId2, rs.getInt(2));
                    expectedId2++;
                }
                assertEquals(expectedRowCount, limit);
                assertServerPagingMetric(indexName, rs, true);
            }
        }
    }

    @Test
    public void testOrderByNonAggregation() throws Exception {
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
            statement.setString(1, tenantId);
            try (ResultSet rs = statement.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(D3.getTime(), rs.getDate(1).getTime());
                assertTrue(rs.next());
                assertEquals(D2.getTime(), rs.getDate(1).getTime());
                assertFalse(rs.next());
                assertServerPagingMetric(tablename, rs, true);
            }
        }
    }

    @Test
    public void testLimitOffset() throws SQLException {
        final String tablename = generateUniqueName();
        final String[] STRINGS = { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n",
                "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z" };
        String ddl = "CREATE TABLE " + tablename + " (t_id VARCHAR NOT NULL,\n" + "k1 INTEGER NOT NULL,\n"
                + "k2 INTEGER NOT NULL,\n" + "C3.k3 INTEGER,\n" + "C2.v1 VARCHAR,\n"
                + "CONSTRAINT pk PRIMARY KEY (t_id, k1, k2)) " + "SPLIT ON ('e','i','o')";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            createTestTable(getUrl(), ddl);
            for (int i = 0; i < 26; i++) {
                conn.createStatement().execute("UPSERT INTO " + tablename + " values('" + STRINGS[i] + "'," + i + ","
                        + (i + 1) + "," + (i + 2) + ",'" + STRINGS[25 - i] + "')");
            }
            conn.commit();
            int limit = 10;
            // Testing 0 as remaining offset after 4 rows in first region, 4 rows in second region
            int offset = 8;
            ResultSet rs;
            rs = conn.createStatement()
                    .executeQuery("SELECT t_id from " + tablename + " order by t_id limit " + limit + " offset " + offset);
            int i = 0;
            while (i < limit) {
                assertTrue(rs.next());
                assertEquals("Expected string didn't match for i = " + i, STRINGS[offset + i], rs.getString(1));
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
                assertTrue(rs.next());
                assertEquals("Expected string didn't match for i = " + i,
                        STRINGS[offset + filterCond + i], rs.getString(1));
                i++;
            }
            assertServerPagingMetric(tablename, rs, true);

            limit = 35;
            rs = conn.createStatement().executeQuery("SELECT t_id from " + tablename + " union all SELECT t_id from "
                    + tablename + " offset " + offset + " FETCH FIRST " + limit + " rows only");
            i = 0;
            while (i++ < STRINGS.length - offset) {
                assertTrue(rs.next());
                assertEquals(STRINGS[offset + i - 1], rs.getString(1));
            }
            i = 0;
            while (i++ < limit - STRINGS.length - offset) {
                assertTrue(rs.next());
                assertEquals(STRINGS[i - 1], rs.getString(1));
            }
            assertServerPagingMetric(tablename, rs, true);
            limit = 1;
            offset = 1;
            rs = conn.createStatement()
                    .executeQuery("SELECT k2 from " + tablename + " order by k2 desc limit " + limit + " offset " + offset);
            assertTrue(rs.next());
            assertEquals(25, rs.getInt(1));
            assertFalse(rs.next());
            // because of descending order the offset is implemented on client
            // so this generates a parallel scan and paging happens
            assertServerPagingMetric(tablename, rs, true);
        }
    }

    @Test
    public void testGroupBy() throws SQLException {
        final String tablename = generateUniqueName();
        final String indexName = generateUniqueName();

        String ddl = "CREATE TABLE " + tablename + " (t_id VARCHAR NOT NULL,\n" + "k1 INTEGER NOT NULL,\n"
                + "k2 INTEGER CONSTRAINT pk PRIMARY KEY (t_id, k1)) ";
        String indexDDl = "CREATE INDEX IF NOT EXISTS " +  indexName + " ON " + tablename + "(k2)";

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            createTestTable(getUrl(), ddl);
            createTestTable(getUrl(), indexDDl);
            for (int i = 0; i < 8; i++) {
                conn.createStatement().execute("UPSERT INTO " + tablename + " values('tenant1'," + i + ","
                        + (i + 1) + ")");
            }
            conn.commit();
            ResultSet rs = conn.createStatement().executeQuery(
                    "SELECT count(*) FROM " + tablename + " where t_id = 'tenant1' AND (k2 IN (5,6) or k2 is null) group by k2=6");
            while (rs.next()) {
                Assert.assertEquals(1, rs.getInt(1));
            }
            Assert.assertFalse(rs.next());
            assertServerPagingMetric(indexName, rs, true);
        }
    }

    @Test
    public void testUncoveredQuery() throws Exception {
        String dataTableName = generateUniqueName();
        populateTable(dataTableName); // with two rows ('a', 'ab', 'abc', 'abcd') and ('b', 'bc', 'bcd', 'bcde')
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String indexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE UNCOVERED INDEX "
                    + indexTableName + " on " + dataTableName + " (val1) ");
            String selectSql;
            int limit = 10;
            // Verify that an index hint is not necessary for an uncovered index
            selectSql = "SELECT  val2, val3 from " + dataTableName +
                    " WHERE val1 = 'bc' AND (val2 = 'bcd' OR val3 ='bcde') LIMIT " + limit;
            assertExplainPlanWithLimit(conn, selectSql, dataTableName, indexTableName, limit);

            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals("bcd", rs.getString(1));
            assertEquals("bcde", rs.getString(2));
            assertFalse(rs.next());
            assertServerPagingMetric(indexTableName, rs, true);

            // Add another row and run a group by query where the uncovered index should be used
            conn.createStatement().execute("upsert into " + dataTableName + " (id, val1, val2, val3) values ('c', 'ab','cde', 'cdef')");
            conn.commit();

            selectSql = "SELECT count(val3) from " + dataTableName + " where val1 > '0' GROUP BY val1";
            // Verify that we will read from the index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());

            selectSql = "SELECT count(val3) from " + dataTableName + " where val1 > '0'";
            // Verify that we will read from the index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));

            // Run an order by query where the uncovered index should be used
            selectSql = "SELECT val3 from " + dataTableName + " where val1 > '0' ORDER BY val1";
            // Verify that we will read from the index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals("abcd", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("cdef", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("bcde", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    private void populateTable(String tableName) throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("create table " + tableName +
                " (id varchar(10) not null primary key, val1 varchar(10), val2 varchar(10)," +
                " val3 varchar(10))");
        conn.createStatement().execute("upsert into " + tableName + " values ('a', 'ab', 'abc', 'abcd')");
        conn.commit();
        conn.createStatement().execute("upsert into " + tableName + " values ('b', 'bc', 'bcd', 'bcde')");
        conn.commit();
        conn.close();
    }
}
