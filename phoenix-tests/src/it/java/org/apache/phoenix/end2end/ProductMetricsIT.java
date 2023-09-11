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

import static org.apache.phoenix.query.QueryConstants.MILLIS_IN_DAY;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Ordering;


@Category(ParallelStatsDisabledTest.class)
public class ProductMetricsIT extends ParallelStatsDisabledIT {
    private static final String PRODUCT_METRICS_NAME = "PRODUCT_METRICS";
    private static final String PRODUCT_METRICS_SCHEMA_NAME = "";
    private static final String DS1 = "1970-01-01 00:58:00";
    private static final String DS2 = "1970-01-01 01:02:00";
    private static final String DS3 = "1970-01-01 01:30:00";
    private static final String DS4 = "1970-01-01 01:45:00";
    private static final String DS5 = "1970-01-01 02:00:00";
    private static final String DS6 = "1970-01-01 04:00:00";
    private static final Date D1 = toDate(DS1);
    private static final Date D2 = toDate(DS2);
    private static final Date D3 = toDate(DS3);
    private static final Date D4 = toDate(DS4);
    private static final Date D5 = toDate(DS5);
    private static final Date D6 = toDate(DS6);
    private static final Object ROUND_1HR = toDate("1970-01-01 01:00:00");
    private static final Object ROUND_2HR = toDate("1970-01-01 02:00:00");
    private static final String F1 = "A";
    private static final String F2 = "B";
    private static final String F3 = "C";
    private static final String R1 = "R1";
    private static final String R2 = "R2";

    private static byte[][] getSplits(String tenantId) {
        return new byte[][] {
                ByteUtil.concat(Bytes.toBytes(tenantId), PDate.INSTANCE.toBytes(D3)),
                ByteUtil.concat(Bytes.toBytes(tenantId), PDate.INSTANCE.toBytes(D5)),
        };
    }

    private static Date toDate(String dateString) {
        return DateUtil.parseDate(dateString);
    }

    private static void initTable(String tablename, byte[][] splits) throws Exception {
        ensureTableCreated(getUrl(), tablename, PRODUCT_METRICS_NAME, splits, null, null);
    }

    private static void assertNoRows(String tablename,Connection conn) throws SQLException {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select 1 from "+tablename);
        assertFalse(rs.next());
    }

    private static void initTableValues(String tablename, String tenantId, byte[][] splits) throws Exception {
        initTable(tablename, splits);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            assertNoRows(tablename, conn);
            initTableValues(tablename, conn, tenantId);
            conn.commit();
        } finally {
            conn.close();
        }
    }

    private static void initTableValues(String tablename, Connection conn, String tenantId) throws Exception {
        PreparedStatement stmt = conn.prepareStatement(
                "upsert into " + tablename +
                        " (" +
                        "    ORGANIZATION_ID, " +
                        "    \"DATE\", " +
                        "    FEATURE, " +
                        "    UNIQUE_USERS, " +
                        "    TRANSACTIONS, " +
                        "    CPU_UTILIZATION, " +
                        "    DB_UTILIZATION, " +
                        "    REGION, " +
                        "    IO_TIME)" +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
        stmt.setString(1, tenantId);
        stmt.setDate(2, D1);
        stmt.setString(3, F1);
        stmt.setInt(4, 10);
        stmt.setLong(5, 100L);
        stmt.setBigDecimal(6, BigDecimal.valueOf(0.5));
        stmt.setBigDecimal(7, BigDecimal.valueOf(0.2));
        stmt.setString(8, R2);
        stmt.setNull(9, Types.BIGINT);
        stmt.execute();

        stmt.setString(1, tenantId);
        stmt.setDate(2, D2);
        stmt.setString(3, F1);
        stmt.setInt(4, 20);
        stmt.setLong(5, 200);
        stmt.setBigDecimal(6, BigDecimal.valueOf(1.0));
        stmt.setBigDecimal(7, BigDecimal.valueOf(0.4));
        stmt.setString(8, null);
        stmt.setLong(9, 2000);
        stmt.execute();

        stmt.setString(1, tenantId);
        stmt.setDate(2, D3);
        stmt.setString(3, F1);
        stmt.setInt(4, 30);
        stmt.setLong(5, 300);
        stmt.setBigDecimal(6, BigDecimal.valueOf(2.5));
        stmt.setBigDecimal(7, BigDecimal.valueOf(0.6));
        stmt.setString(8, R1);
        stmt.setNull(9, Types.BIGINT);
        stmt.execute();

        stmt.setString(1, tenantId);
        stmt.setDate(2, D4);
        stmt.setString(3, F2);
        stmt.setInt(4, 40);
        stmt.setLong(5, 400);
        stmt.setBigDecimal(6, BigDecimal.valueOf(3.0));
        stmt.setBigDecimal(7, BigDecimal.valueOf(0.8));
        stmt.setString(8, R1);
        stmt.setLong(9, 4000);
        stmt.execute();

        stmt.setString(1, tenantId);
        stmt.setDate(2, D5);
        stmt.setString(3, F3);
        stmt.setInt(4, 50);
        stmt.setLong(5, 500);
        stmt.setBigDecimal(6, BigDecimal.valueOf(3.5));
        stmt.setBigDecimal(7, BigDecimal.valueOf(1.2));
        stmt.setString(8, R2);
        stmt.setLong(9, 5000);
        stmt.execute();

        stmt.setString(1, tenantId);
        stmt.setDate(2, D6);
        stmt.setString(3, F1);
        stmt.setInt(4, 60);
        stmt.setLong(5, 600);
        stmt.setBigDecimal(6, BigDecimal.valueOf(4.0));
        stmt.setBigDecimal(7, BigDecimal.valueOf(1.4));
        stmt.setString(8, null);
        stmt.setNull(9, Types.BIGINT);
        stmt.execute();
    }

    private static void initDateTableValues(String tablename, String tenantId, byte[][] splits, Date startDate) throws Exception {
        initDateTableValues(tablename, tenantId, splits, startDate, 2.0);
    }

    private static void initDateTableValues(String tablename, String tenantId, byte[][] splits, Date startDate, double dateIncrement) throws Exception {
         initTable(tablename, splits);

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            assertNoRows(tablename, conn);
            initDateTableValues(tablename, conn, tenantId, startDate, dateIncrement);
            conn.commit();
        } finally {
            conn.close();
        }
    }

    private static void initDateTableValues(String tablename, Connection conn, String tenantId, Date startDate, double dateIncrement) throws Exception {
        PreparedStatement stmt = conn.prepareStatement(
                "upsert into " +tablename+
                        "(" +
                        "    ORGANIZATION_ID, " +
                        "    \"DATE\", " +
                        "    FEATURE, " +
                        "    UNIQUE_USERS, " +
                        "    TRANSACTIONS, " +
                        "    CPU_UTILIZATION, " +
                        "    DB_UTILIZATION, " +
                        "    REGION, " +
                        "    IO_TIME)" +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
        stmt.setString(1, tenantId);
        stmt.setDate(2, startDate);
        stmt.setString(3, "A");
        stmt.setInt(4, 10);
        stmt.setLong(5, 100L);
        stmt.setBigDecimal(6, BigDecimal.valueOf(0.5));
        stmt.setBigDecimal(7, BigDecimal.valueOf(0.2));
        stmt.setString(8, R2);
        stmt.setNull(9, Types.BIGINT);
        stmt.execute();

        startDate = new Date(startDate.getTime() + (long)(QueryConstants.MILLIS_IN_DAY * dateIncrement));
        stmt.setString(1, tenantId);
        stmt.setDate(2, startDate);
        stmt.setString(3, "B");
        stmt.setInt(4, 20);
        stmt.setLong(5, 200);
        stmt.setBigDecimal(6, BigDecimal.valueOf(1.0));
        stmt.setBigDecimal(7, BigDecimal.valueOf(0.4));
        stmt.setString(8, null);
        stmt.setLong(9, 2000);
        stmt.execute();

        startDate = new Date(startDate.getTime() + (long)(QueryConstants.MILLIS_IN_DAY * dateIncrement));
        stmt.setString(1, tenantId);
        stmt.setDate(2, startDate);
        stmt.setString(3, "C");
        stmt.setInt(4, 30);
        stmt.setLong(5, 300);
        stmt.setBigDecimal(6, BigDecimal.valueOf(2.5));
        stmt.setBigDecimal(7, BigDecimal.valueOf(0.6));
        stmt.setString(8, R1);
        stmt.setNull(9, Types.BIGINT);
        stmt.execute();

        startDate = new Date(startDate.getTime() + (long)(QueryConstants.MILLIS_IN_DAY * dateIncrement));
        stmt.setString(1, tenantId);
        stmt.setDate(2, startDate);
        stmt.setString(3, "D");
        stmt.setInt(4, 40);
        stmt.setLong(5, 400);
        stmt.setBigDecimal(6, BigDecimal.valueOf(3.0));
        stmt.setBigDecimal(7, BigDecimal.valueOf(0.8));
        stmt.setString(8, R1);
        stmt.setLong(9, 4000);
        stmt.execute();

        startDate = new Date(startDate.getTime() + (long)(QueryConstants.MILLIS_IN_DAY * dateIncrement));
        stmt.setString(1, tenantId);
        stmt.setDate(2, startDate);
        stmt.setString(3, "E");
        stmt.setInt(4, 50);
        stmt.setLong(5, 500);
        stmt.setBigDecimal(6, BigDecimal.valueOf(3.5));
        stmt.setBigDecimal(7, BigDecimal.valueOf(1.2));
        stmt.setString(8, R2);
        stmt.setLong(9, 5000);
        stmt.execute();

        startDate = new Date(startDate.getTime() + (long)(QueryConstants.MILLIS_IN_DAY * dateIncrement));
        stmt.setString(1, tenantId);
        stmt.setDate(2, startDate);
        stmt.setString(3, "F");
        stmt.setInt(4, 60);
        stmt.setLong(5, 600);
        stmt.setBigDecimal(6, BigDecimal.valueOf(4.0));
        stmt.setBigDecimal(7, BigDecimal.valueOf(1.4));
        stmt.setString(8, null);
        stmt.setNull(9, Types.BIGINT);
        stmt.execute();
    }

    
    @Test
    public void testDateRangeAggregation() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT count(1), feature f FROM "+tablename+" WHERE organization_id=? AND \"DATE\" >= to_date(?) AND \"DATE\" <= to_date(?) GROUP BY f";
        //String query = "SELECT count(1), feature FROM PRODUCT_METRICS GROUP BY feature";

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, DS2);
            statement.setString(3, DS4);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(2, rs.getLong(1));
            assertEquals(F1, rs.getString(2));
            assertTrue(rs.next());
            assertEquals(1, rs.getLong(1));
            assertEquals(F2, rs.getString(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testTableAliasSameAsTableName() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT sum("+tablename+".transactions) FROM "+tablename+" PRODUCT_METRICS";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(2100, rs.getLong(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testPartiallyEvaluableAnd() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT \"DATE\" FROM "+tablename+" WHERE organization_id=? AND unique_users >= 30 AND transactions >= 300 AND cpu_utilization > 2 AND db_utilization > 0.5 AND io_time = 4000";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(D4, rs.getDate(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testPartiallyEvaluableOr() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT \"DATE\" FROM "+tablename+" WHERE organization_id=? AND (transactions = 10000 OR unset_column = 5 OR io_time = 4000)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(D4, rs.getDate(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testConstantTrueHaving() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT count(1), feature FROM "+tablename+" WHERE organization_id=? AND \"DATE\" >= to_date(?) AND \"DATE\" <= to_date(?) GROUP BY feature HAVING 1=1";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, DS2);
            statement.setString(3, DS4);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(2, rs.getLong(1));
            assertEquals(F1, rs.getString(2));
            assertTrue(rs.next());
            assertEquals(1, rs.getLong(1));
            assertEquals(F2, rs.getString(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testConstantFalseHaving() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT count(1), feature FROM "+tablename+" WHERE organization_id=? AND \"DATE\" >= to_date(?) AND \"DATE\" <= to_date(?) GROUP BY feature HAVING 1=1 and 0=1";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, DS2);
            statement.setString(3, DS4);
            ResultSet rs = statement.executeQuery();
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDateRangeHavingAggregation() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT count(1), feature FROM "+tablename+" WHERE organization_id=? AND \"DATE\" >= to_date(?) AND \"DATE\" <= to_date(?) GROUP BY feature HAVING count(1) >= 2";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, DS2);
            statement.setString(3, DS4);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(2, rs.getLong(1));
            assertEquals(F1, rs.getString(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDateRangeSumLongAggregation() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT sum(transactions), feature FROM "+tablename+" WHERE organization_id=? AND \"DATE\" >= to_date(?) AND \"DATE\" <= to_date(?) GROUP BY feature";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, DS2);
            statement.setString(3, DS4);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(500, rs.getLong(1));
            assertEquals(F1, rs.getString(2));
            assertTrue(rs.next());
            assertEquals(400, rs.getLong(1));
            assertEquals(F2, rs.getString(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testRoundAggregation() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT round(\"DATE\",'hour',1) r,count(1) FROM "+tablename+" WHERE organization_id=? GROUP BY r";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            Date d;
            int c;
            ResultSet rs = statement.executeQuery();

            assertTrue(rs.next());
            d = rs.getDate(1);
            c = rs.getInt(2);
            assertEquals(1 * 60 * 60 * 1000, d.getTime()); // Date bucketed into 1 hr
            assertEquals(2, c);

            assertTrue(rs.next());
            d = rs.getDate(1);
            c = rs.getInt(2);
            assertEquals(2 * 60 * 60 * 1000, d.getTime()); // Date bucketed into 2 hr
            assertEquals(3, c);

            assertTrue(rs.next());
            d = rs.getDate(1);
            c = rs.getInt(2);
            assertEquals(4 * 60 * 60 * 1000, d.getTime()); // Date bucketed into 4 hr
            assertEquals(1, c);

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testRoundScan() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT round(\"DATE\",'hour') FROM "+tablename+" WHERE organization_id=?";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            Date d;
            long t;
            ResultSet rs = statement.executeQuery();

            assertTrue(rs.next());
            d = rs.getDate(1);
            t = 1 * 60 * 60 * 1000;
            assertEquals(t, d.getTime()); // Date bucketed into 1 hr
            assertTrue(rs.next());
            assertEquals(t, d.getTime()); // Date bucketed into 1 hr

            assertTrue(rs.next());
            d = rs.getDate(1);
            t = 2 * 60 * 60 * 1000;
            assertEquals(t, d.getTime()); // Date bucketed into 2 hr
            assertTrue(rs.next());
            assertEquals(t, d.getTime()); // Date bucketed into 2 hr
            assertTrue(rs.next());
            assertEquals(t, d.getTime()); // Date bucketed into 2 hr

            assertTrue(rs.next());
            d = rs.getDate(1);
            t = 4 * 60 * 60 * 1000;
            assertEquals(t, d.getTime()); // Date bucketed into 4 hr

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testTruncAggregation() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT trunc(\"DATE\",'hour'),count(1) FROM "+tablename+" WHERE organization_id=? GROUP BY trunc(\"DATE\",'hour')";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            Date d;
            int c;
            ResultSet rs = statement.executeQuery();

            assertTrue(rs.next());
            d = rs.getDate(1);
            c = rs.getInt(2);
            assertEquals(0, d.getTime()); // Date bucketed into 0 hr
            assertEquals(1, c);

            assertTrue(rs.next());
            d = rs.getDate(1);
            c = rs.getInt(2);
            assertEquals(1 * 60 * 60 * 1000, d.getTime()); // Date bucketed into 1 hr
            assertEquals(3, c);

            assertTrue(rs.next());
            d = rs.getDate(1);
            c = rs.getInt(2);
            assertEquals(2 * 60 * 60 * 1000, d.getTime()); // Date bucketed into 2 hr
            assertEquals(1, c);

            assertTrue(rs.next());
            d = rs.getDate(1);
            c = rs.getInt(2);
            assertEquals(4 * 60 * 60 * 1000, d.getTime()); // Date bucketed into 4 hr
            assertEquals(1, c);

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testAggregation() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT feature,sum(unique_users) FROM "+tablename+" WHERE organization_id=? AND transactions > 0 GROUP BY feature";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertTrue(rs.next());
            assertTrue(rs.next());
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testHavingAggregation() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT feature,sum(unique_users) FROM "+tablename+" WHERE organization_id=? AND transactions > 0 GROUP BY feature HAVING feature=?";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, F1);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testConstantSumAggregation() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT sum(1),sum(unique_users) FROM "+tablename+" WHERE organization_id=?";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(6,rs.getInt(1));
            assertEquals(210,rs.getInt(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testMultiDimAggregation() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT feature,region,sum(unique_users) FROM "+tablename+" WHERE organization_id=? GROUP BY feature,region";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();

            assertTrue(rs.next());
            assertEquals(F1,rs.getString(1));
            assertEquals(null,rs.getString(2));
            assertEquals(80,rs.getInt(3));
            assertTrue(rs.next());
            assertEquals(F1,rs.getString(1));
            assertEquals(R1,rs.getString(2));
            assertEquals(30,rs.getInt(3));
            assertTrue(rs.next());
            assertEquals(F1,rs.getString(1));
            assertEquals(R2,rs.getString(2));
            assertEquals(10,rs.getInt(3));

            assertTrue(rs.next());
            assertEquals(F2,rs.getString(1));
            assertEquals(R1,rs.getString(2));
            assertEquals(40,rs.getInt(3));

            assertTrue(rs.next());
            assertEquals(F3,rs.getString(1));
            assertEquals(R2,rs.getString(2));
            assertEquals(50,rs.getInt(3));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testMultiDimRoundAggregation() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT round(\"DATE\",'hour',1),feature,sum(unique_users) FROM "+tablename+" WHERE organization_id=? GROUP BY round(\"DATE\",'hour',1),feature";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();

            Date bucket1 = new Date(1 * 60 * 60 * 1000);
            Date bucket2 = new Date(2 * 60 * 60 * 1000);
            Date bucket3 = new Date(4 * 60 * 60 * 1000);

            assertTrue(rs.next());
            assertEquals(bucket1, rs.getDate(1));
            assertEquals(F1,rs.getString(2));
            assertEquals(30,rs.getInt(3));


            assertTrue(rs.next());
            assertEquals(bucket2, rs.getDate(1));
            assertEquals(F1,rs.getString(2));
            assertEquals(30,rs.getInt(3));

            assertTrue(rs.next());
            assertEquals(bucket2.getTime(), rs.getDate(1).getTime());
            assertEquals(F2,rs.getString(2));
            assertEquals(40,rs.getInt(3));

            assertTrue(rs.next());
            assertEquals(bucket2, rs.getDate(1));
            assertEquals(F3,rs.getString(2));
            assertEquals(50,rs.getInt(3));


            assertTrue(rs.next());
            assertEquals(bucket3, rs.getDate(1));
            assertEquals(F1,rs.getString(2));
            assertEquals(60,rs.getInt(3));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDateRangeSumNumberUngroupedAggregation() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT sum(cpu_utilization) FROM "+tablename+" WHERE organization_id=? AND \"DATE\" >= to_date(?) AND \"DATE\" <= to_date(?)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, DS2);
            statement.setString(3, DS4);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(6.5), rs.getBigDecimal(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSumUngroupedAggregation() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT sum(unique_users),sum(cpu_utilization),sum(transactions),sum(db_utilization),sum(response_time) FROM "+tablename+" WHERE organization_id=?";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(210, rs.getInt(1));
            assertEquals(BigDecimal.valueOf(14.5), rs.getBigDecimal(2));
            assertEquals(2100L, rs.getLong(3));
            assertEquals(BigDecimal.valueOf(4.6), rs.getBigDecimal(4));
            assertEquals(0, rs.getLong(5));
            assertEquals(true, rs.wasNull());
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testResetColumnInSameTxn() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT sum(transactions) FROM "+tablename+" WHERE organization_id=?";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        Connection upsertConn = DriverManager.getConnection(getUrl(), props);
        try {
            initTable(tablename,getSplits(tenantId));
            initTableValues(tablename, upsertConn, tenantId);
            PreparedStatement stmt = upsertConn.prepareStatement(
                    "upsert into " + tablename+
                            "(" +
                            "    ORGANIZATION_ID, " +
                            "    \"DATE\", " +
                            "    FEATURE, " +
                            "    UNIQUE_USERS," +
                            "    TRANSACTIONS) " +
                            "VALUES (?, ?, ?, ?, ?)");
            stmt.setString(1, tenantId);
            stmt.setDate(2, D1);
            stmt.setString(3, F1);
            stmt.setInt(4, 10);
            stmt.setInt(5, 200); // Change TRANSACTIONS from 100 to 200
            stmt.execute();
            upsertConn.commit();

            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(2200, rs.getInt(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSumUngroupedHavingAggregation() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT sum(unique_users),sum(cpu_utilization),sum(transactions),sum(db_utilization),sum(response_time) FROM "+tablename+" WHERE organization_id=? HAVING sum(unique_users) > 200 AND sum(db_utilization) > 4.5";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(210, rs.getInt(1));
            assertEquals(BigDecimal.valueOf(14.5), rs.getBigDecimal(2));
            assertEquals(2100L, rs.getLong(3));
            assertEquals(BigDecimal.valueOf(4.6), rs.getBigDecimal(4));
            assertEquals(0, rs.getLong(5));
            assertEquals(true, rs.wasNull());
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSumUngroupedHavingAggregation2() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT sum(unique_users),sum(cpu_utilization),sum(transactions),sum(db_utilization),sum(response_time) FROM "+tablename+" WHERE organization_id=? HAVING sum(unique_users) > 200 AND sum(db_utilization) > 5";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testMinUngroupedAggregation() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT min(unique_users),min(cpu_utilization),min(transactions),min(db_utilization),min('X'),min(response_time) FROM "+tablename+" WHERE organization_id=?";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(10, rs.getInt(1));
            assertEquals(BigDecimal.valueOf(0.5), rs.getBigDecimal(2));
            assertEquals(100L, rs.getLong(3));
            assertEquals(BigDecimal.valueOf(0.2), rs.getBigDecimal(4));
            assertEquals("X", rs.getString(5));
            assertEquals(0, rs.getLong(6));
            assertEquals(true, rs.wasNull());
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testMinUngroupedAggregation1() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT min(cpu_utilization) FROM "+tablename+" WHERE organization_id=?";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(0.5), rs.getBigDecimal(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testMaxUngroupedAggregation() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT max(unique_users),max(cpu_utilization),max(transactions),max(db_utilization),max('X'),max(response_time) FROM "+tablename+" WHERE organization_id=?";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(60, rs.getInt(1));
            assertEquals(BigDecimal.valueOf(4), rs.getBigDecimal(2));
            assertEquals(600L, rs.getLong(3));
            assertEquals(BigDecimal.valueOf(1.4), rs.getBigDecimal(4));
            assertEquals("X", rs.getString(5));
            assertEquals(0, rs.getLong(6));
            assertEquals(true, rs.wasNull());
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testMaxGroupedAggregation() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT feature,max(transactions) FROM "+tablename+" WHERE organization_id=? GROUP BY feature";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(F1,rs.getString(1));
            assertEquals(600,rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(F2,rs.getString(1));
            assertEquals(400,rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(F3,rs.getString(1));
            assertEquals(500,rs.getInt(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testCountUngroupedAggregation() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT count(1) FROM "+tablename;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(6, rs.getLong(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testCountColumnUngroupedAggregation() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT count(io_time),sum(io_time),avg(io_time) FROM "+tablename+" WHERE organization_id=?";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(3, rs.getLong(1));
            assertEquals(11000, rs.getLong(2));
            // Scale is automatically capped at 4 if no scale is specified.
            assertEquals(new BigDecimal("3666.6666"), rs.getBigDecimal(3));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testNoRowsUngroupedAggregation() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT count(io_time),sum(io_time),avg(io_time),count(1) FROM "+tablename+" WHERE organization_id=? AND feature > ?";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2,F3);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(0, rs.getLong(1));
            assertFalse(rs.wasNull());
            assertEquals(0, rs.getLong(2));
            assertTrue(rs.wasNull());
            assertEquals(null, rs.getBigDecimal(3));
            assertEquals(0, rs.getLong(4));
            assertFalse(rs.wasNull());
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testAvgUngroupedAggregation() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT avg(unique_users) FROM "+tablename+" WHERE organization_id=?";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(BigDecimal.valueOf(35), rs.getBigDecimal(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testAvgUngroupedAggregationOnValueField() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT AVG(DB_UTILIZATION) FROM "+tablename+" WHERE organization_id=?";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);

            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            // The column is defined as decimal(31,10), so the value is capped at 10 decimal points.
            assertEquals(new BigDecimal("0.7666666666"), rs.getBigDecimal(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    /**
     * Test aggregate query with rownum limit that does not explicity contain a count(1) as a select expression
     * @throws Exception
     */
    @Test
    public void testLimitSumUngroupedAggregation() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        // No count(1) aggregation, so it will get added automatically
        // LIMIT has no effect, since it's applied at the end and we'll always have a single row for ungrouped aggregation
        String query = "SELECT sum(unique_users),sum(cpu_utilization),sum(transactions),sum(db_utilization),sum(response_time) feature FROM "+tablename+" WHERE organization_id=? LIMIT 3";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(210, rs.getInt(1));
            assertEquals(BigDecimal.valueOf(14.5), rs.getBigDecimal(2));
            assertEquals(2100L, rs.getLong(3));
            assertEquals(BigDecimal.valueOf(4.6), rs.getBigDecimal(4));
            assertEquals(0, rs.getLong(5));
            assertEquals(true, rs.wasNull());
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    /**
     * Test grouped aggregation query with a mix of aggregated data types
     * @throws Exception
     */
    @Test
    public void testSumGroupedAggregation() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT feature,sum(unique_users),sum(cpu_utilization),sum(transactions),sum(db_utilization),sum(response_time),count(1) c FROM "+tablename+" WHERE organization_id=? AND feature < ? GROUP BY feature";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, F3);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(F1, rs.getString("feature"));
            assertEquals(120, rs.getInt("sum(unique_users)"));
            assertEquals(BigDecimal.valueOf(8), rs.getBigDecimal(3));
            assertEquals(1200L, rs.getLong(4));
            assertEquals(BigDecimal.valueOf(2.6), rs.getBigDecimal(5));
            assertEquals(0, rs.getLong(6));
            assertEquals(true, rs.wasNull());
            assertEquals(4, rs.getLong("c"));
            assertTrue(rs.next());
            assertEquals(F2, rs.getString("feature"));
            assertEquals(40, rs.getInt(2));
            assertEquals(BigDecimal.valueOf(3), rs.getBigDecimal(3));
            assertEquals(400L, rs.getLong(4));
            assertEquals(BigDecimal.valueOf(0.8), rs.getBigDecimal(5));
            assertEquals(0, rs.getLong(6));
            assertEquals(true, rs.wasNull());
            assertEquals(1, rs.getLong("c"));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDegenerateAggregation() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT count(1), feature FROM "+tablename+" WHERE organization_id=? AND \"DATE\" >= to_date(?) AND \"DATE\" <= to_date(?) GROUP BY feature";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            // Start date larger than end date
            statement.setString(2, DS4);
            statement.setString(3, DS2);
            ResultSet rs = statement.executeQuery();
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    /**
     * Query with multiple > expressions on continquous PK columns
     * @throws Exception
     */
    @Test
    public void testFeatureDateRangeAggregation() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT feature,unique_users FROM "+tablename+" WHERE organization_id=? AND \"DATE\" >= to_date(?) AND feature > ?";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, DS2);
            statement.setString(3, F2);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(F3, rs.getString(1));
            assertEquals(50, rs.getInt(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    /**
     * Query with non contiguous PK column expressions (i.e. no expresion for DATE)
     * @throws Exception
     */
    @Test
    public void testFeatureGTAggregation() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT feature,unique_users FROM "+tablename+" WHERE organization_id=? AND feature > ?";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, F2);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(F3, rs.getString(1));
            assertEquals(50, rs.getInt(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testFeatureGTEAggregation() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT feature,unique_users FROM "+tablename+" WHERE organization_id=? AND feature >= ?";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, F2);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(F2, rs.getString(1));
            assertEquals(40, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(F3, rs.getString(1));
            assertEquals(50, rs.getInt(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testFeatureEQAggregation() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT feature,unique_users FROM "+tablename+" WHERE organization_id=? AND feature = ?";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, F2);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(F2, rs.getString(1));
            assertEquals(40, rs.getInt(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }


    @Test
    public void testFeatureLTAggregation() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT feature,unique_users FROM "+tablename+" WHERE organization_id=? AND feature < ?";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, F2);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(F1, rs.getString(1));
            assertEquals(10, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(F1, rs.getString(1));
            assertEquals(20, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(F1, rs.getString(1));
            assertEquals(30, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(F1, rs.getString(1));
            assertEquals(60, rs.getInt(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testFeatureLTEAggregation() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT feature,unique_users FROM "+tablename+" WHERE organization_id=? AND feature <= ?";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, F2);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(F1, rs.getString(1));
            assertEquals(10, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(F1, rs.getString(1));
            assertEquals(20, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(F1, rs.getString(1));
            assertEquals(30, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(F2, rs.getString(1));
            assertEquals(40, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(F1, rs.getString(1));
            assertEquals(60, rs.getInt(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    @Test
    public void testOrderByNonAggregation() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        initTableValues(tablename, tenantId, getSplits(tenantId));
        String query = "SELECT \"DATE\", transactions t FROM "+tablename+" WHERE organization_id=? AND unique_users <= 30 ORDER BY t DESC LIMIT 2";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(D3.getTime(), rs.getDate(1).getTime());
            assertTrue(rs.next());
            assertEquals(D2.getTime(), rs.getDate(1).getTime());
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testOrderByUngroupedAggregation() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT sum(unique_users) sumUsers,count(feature) " +
                "FROM " + tablename+
                " WHERE organization_id=? " +
                "ORDER BY 100000-sumUsers";

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(210, rs.getInt(1));
            assertEquals(6, rs.getInt(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testOrderByGroupedAggregation() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT feature,sum(unique_users) s,count(feature),round(\"DATE\",'hour',1) r " +
                "FROM "+tablename+
                " WHERE organization_id=? " +
                "GROUP BY feature, r " +
                "ORDER BY 1 desc,feature desc,r,feature,s";

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();

            Object[][] expected = {
                    {F3, 50, 1},
                    {F2, 40, 1},
                    {F1, 30, 2},
                    {F1, 30, 1},
                    {F1, 60, 1},
            };

            for (int i = 0; i < expected.length; i++) {
                assertTrue(rs.next());
                assertEquals(expected[i][0], rs.getString(1));
                assertEquals(expected[i][1], rs.getInt(2));
                assertEquals(expected[i][2], rs.getInt(3));
            }
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testOrderByUnprojected() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT sum(unique_users), count(feature) c " +
                "FROM " + tablename+
                " WHERE organization_id=? " +
                "GROUP BY feature " +
                "ORDER BY 100-c,feature";

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();

            int[] expected = {120, 40, 50};
            for (int i = 0; i < expected.length; i++) {
                assertTrue(rs.next());
                assertEquals(expected[i], rs.getInt(1));
            }
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testOrderByNullColumns__nullsFirst() throws Exception {
        helpTestOrderByNullColumns(true);
    }

    @Test
    public void testOrderByNullColumns__nullsLast() throws Exception {
        helpTestOrderByNullColumns(false);
    }

    private void helpTestOrderByNullColumns(boolean nullsFirst) throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT region " +
                "FROM " +tablename+
                " WHERE organization_id=? " +
                "GROUP BY region " +
                "ORDER BY region nulls " + (nullsFirst ? "first" : "last");

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();

            List<String> expected = Lists.newArrayList(null, R1, R2);
            Ordering<String> regionOrdering = Ordering.natural();
            regionOrdering = nullsFirst ? regionOrdering.nullsFirst() : regionOrdering.nullsLast();
            Collections.sort(expected, regionOrdering);

            for (String region : expected) {
                assertTrue(rs.next());
                assertEquals(region, rs.getString(1));
            }
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    /**
     * Test to repro ArrayIndexOutOfBoundException that happens during filtering in BinarySubsetComparator
     * only after a flush is performed
     * @throws Exception
     */
    @Test
    public void testFilterOnTrailingKeyColumn() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);

        Admin admin = null;
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
            admin.flush(TableName.valueOf(SchemaUtil.getTableNameAsBytes(PRODUCT_METRICS_SCHEMA_NAME,tablename)));
            String query = "SELECT SUM(TRANSACTIONS) FROM " + tablename + " WHERE FEATURE=?";
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, F1);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(1200, rs.getInt(1));
        } finally {
            if (admin != null) admin.close();
            conn.close();
        }
    }

    @Test
    public void testFilterOnTrailingKeyColumn2() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT organization_id, \"DATE\", feature FROM "+tablename+" WHERE substr(organization_id,1,3)=? AND \"DATE\" > to_date(?)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId.substring(0,3));
            statement.setString(2, DS4);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(tenantId, rs.getString(1));
            assertEquals(D5.getTime(), rs.getDate(2).getTime());
            assertEquals(F3, rs.getString(3));
            assertTrue(rs.next());
            assertEquals(tenantId, rs.getString(1));
            assertEquals(D6, rs.getDate(2));
            assertEquals(F1, rs.getString(3));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSubstringNotEqual() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT organization_id, \"DATE\", feature FROM "+tablename+" WHERE organization_id=? AND \"DATE\" > to_date(?)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId.substring(0,3));
            statement.setString(2, DS4);
            ResultSet rs = statement.executeQuery();
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testKeyOrderedAggregation1() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT \"DATE\", sum(UNIQUE_USERS) FROM "+tablename+" WHERE \"DATE\" > to_date(?) GROUP BY organization_id, \"DATE\"";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, DS4);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(D5, rs.getDate(1));
            assertEquals(50, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(D6, rs.getDate(1));
            assertEquals(60, rs.getInt(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testKeyOrderedAggregation2() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT \"DATE\", sum(UNIQUE_USERS) FROM "+tablename+" WHERE \"DATE\" < to_date(?) GROUP BY organization_id, \"DATE\"";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, DS4);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(D1, rs.getDate(1));
            assertEquals(10, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(D2, rs.getDate(1));
            assertEquals(20, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(D3, rs.getDate(1));
            assertEquals(30, rs.getInt(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testKeyOrderedRoundAggregation1() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT round(\"DATE\",'HOUR'), sum(UNIQUE_USERS) FROM "+tablename+" WHERE \"DATE\" < to_date(?) GROUP BY organization_id, round(\"DATE\",'HOUR')";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, DS4);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROUND_1HR, rs.getDate(1));
            assertEquals(30, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(ROUND_2HR, rs.getDate(1));
            assertEquals(30, rs.getInt(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testKeyOrderedRoundAggregation2() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT round(\"DATE\",'HOUR'), sum(UNIQUE_USERS) FROM "+tablename+" WHERE \"DATE\" <= to_date(?) GROUP BY organization_id, round(\"DATE\",'HOUR')";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            initTableValues(tablename, tenantId, getSplits(tenantId));
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, DS4);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROUND_1HR, rs.getDate(1));
            assertEquals(30, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(ROUND_2HR, rs.getDate(1));
            assertEquals(70, rs.getInt(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testEqualsRound() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT feature FROM "+tablename+" WHERE organization_id = ? and trunc(\"DATE\",'DAY')=?";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            Date startDate = new Date(System.currentTimeMillis());
            Date equalDate = new Date((startDate.getTime() + 2 * QueryConstants.MILLIS_IN_DAY)/ QueryConstants.MILLIS_IN_DAY*QueryConstants.MILLIS_IN_DAY);
            initDateTableValues(tablename, tenantId, getSplits(tenantId), startDate, 1.0);
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setDate(2, equalDate);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("C", rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testTruncateNotTraversableToFormScanKey() throws Exception {
        String tablename=generateUniqueName();
        String tenantId = getOrganizationId();
        String query = "SELECT feature FROM "+tablename+" WHERE organization_id = ? and TRUNC(\"DATE\",'DAY') <= ?";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            Date startDate = toDate("2013-01-01 00:00:00");
            initDateTableValues(tablename, tenantId, getSplits(tenantId), startDate, 0.5);
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setDate(2, new Date(startDate.getTime() + (long)(QueryConstants.MILLIS_IN_DAY * 0.25)));
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("A", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("B", rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSaltedOrderBy() throws Exception {
        String tablename=generateUniqueName();
        String ddl = "create table " + tablename +
                "   (organization_id char(15) not null," +
                "    \"DATE\" date not null," +
                "    feature char(1) not null," +
                "    unique_users integer not null,\n" +
                "    db_utilization decimal(31,10),\n" +
                "    transactions bigint,\n" +
                "    cpu_utilization decimal(31,10),\n" +
                "    response_time bigint,\n" +
                "    io_time bigint,\n" +
                "    region varchar,\n" +
                "    unset_column decimal(31,10)\n" +
                "    CONSTRAINT pk PRIMARY KEY (organization_id, \"DATE\", feature, unique_users)) salt_buckets=3";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try(Connection conn = DriverManager.getConnection(getUrl(), props);) {
            conn.createStatement().execute(ddl);
        }

        String tenantId = getOrganizationId();
        Date startDate = new Date(System.currentTimeMillis());
        initDateTableValues(tablename, tenantId, getSplits(tenantId), startDate);
        // Add more date data
        props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try(Connection conn = DriverManager.getConnection(getUrl(), props);) {
            initDateTableValues(tablename, conn, tenantId, new Date(startDate.getTime() + MILLIS_IN_DAY * 10),
                    2.0);
            initDateTableValues(tablename, conn, tenantId, new Date(startDate.getTime() + MILLIS_IN_DAY * 20),
                    2.0);
            conn.commit();
        }

        props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try(Connection conn = DriverManager.getConnection(getUrl(), props);){
            PreparedStatement statement = conn.prepareStatement("SELECT count(1) FROM "+tablename+" WHERE organization_id = ?");
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(18, rs.getLong(1));

            statement = conn.prepareStatement("SELECT \"DATE\" FROM "+tablename+" WHERE organization_id = ?  order by \"DATE\" desc limit 10");
            statement.setString(1, tenantId);
            rs = statement.executeQuery();
            Date date = null;
            int count = 0;
            while (rs.next()) {
                if (date != null) {
                    assertTrue(date.getTime() >= rs.getDate(1).getTime());
                }
                count++;
                date = rs.getDate(1);
            }
            assertEquals(10,count);
        }
    }

}
