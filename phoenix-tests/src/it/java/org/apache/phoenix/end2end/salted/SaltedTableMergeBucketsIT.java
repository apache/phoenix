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
package org.apache.phoenix.end2end.salted;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.apache.phoenix.util.TestUtil.assertResultSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.phoenix.end2end.ParallelStatsEnabledIT;
import org.apache.phoenix.end2end.ParallelStatsEnabledTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Sets;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryBuilder;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsEnabledTest.class)
public class SaltedTableMergeBucketsIT extends ParallelStatsEnabledIT {

    @Test
    public void testWithVariousPKTypes() throws Exception {

        int[] variousSaltBuckets = new int[] { 11, 23, 31 };
        SortOrder[][] sortOrdersCase1 =
                new SortOrder[][] { { SortOrder.ASC, SortOrder.ASC, SortOrder.ASC },
                        { SortOrder.ASC, SortOrder.ASC, SortOrder.DESC },
                        { SortOrder.ASC, SortOrder.DESC, SortOrder.ASC },
                        { SortOrder.ASC, SortOrder.DESC, SortOrder.DESC },
                        { SortOrder.DESC, SortOrder.ASC, SortOrder.ASC },
                        { SortOrder.DESC, SortOrder.ASC, SortOrder.DESC },
                        { SortOrder.DESC, SortOrder.DESC, SortOrder.ASC },
                        { SortOrder.DESC, SortOrder.DESC, SortOrder.DESC } };

        SortOrder[][] sortOrdersCase2 =
                new SortOrder[][] { { SortOrder.ASC, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC },
                        { SortOrder.ASC, SortOrder.ASC, SortOrder.ASC, SortOrder.DESC },
                        { SortOrder.ASC, SortOrder.ASC, SortOrder.DESC, SortOrder.ASC },
                        { SortOrder.ASC, SortOrder.ASC, SortOrder.DESC, SortOrder.DESC },
                        { SortOrder.ASC, SortOrder.DESC, SortOrder.DESC, SortOrder.ASC },
                        { SortOrder.ASC, SortOrder.DESC, SortOrder.DESC, SortOrder.DESC },
                        { SortOrder.ASC, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC },
                        { SortOrder.ASC, SortOrder.DESC, SortOrder.ASC, SortOrder.DESC },
                        { SortOrder.DESC, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC },
                        { SortOrder.DESC, SortOrder.ASC, SortOrder.ASC, SortOrder.DESC },
                        { SortOrder.DESC, SortOrder.ASC, SortOrder.DESC, SortOrder.ASC },
                        { SortOrder.DESC, SortOrder.ASC, SortOrder.DESC, SortOrder.DESC },
                        { SortOrder.DESC, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC },
                        { SortOrder.DESC, SortOrder.DESC, SortOrder.ASC, SortOrder.DESC },
                        { SortOrder.DESC, SortOrder.DESC, SortOrder.DESC, SortOrder.ASC },
                        { SortOrder.DESC, SortOrder.DESC, SortOrder.DESC, SortOrder.DESC } };

        for (int index = 0; index < sortOrdersCase1.length; index++) {
            // Test Case 1: PK1 = Bigint, PK2 = Decimal, PK3 = Bigint
            String baseTableNameCase1 = generateUniqueName();
            PDataType[] testIntDecIntPKTypes =
                    new PDataType[] { PLong.INSTANCE, PDecimal.INSTANCE, PLong.INSTANCE };
            long nowTime1 =
                    createTableCase1(baseTableNameCase1, variousSaltBuckets[0],
                        testIntDecIntPKTypes[0], sortOrdersCase1[index][0], testIntDecIntPKTypes[1],
                        sortOrdersCase1[index][1], testIntDecIntPKTypes[2],
                        sortOrdersCase1[index][2]);
            testIntDecIntPK(baseTableNameCase1, nowTime1, sortOrdersCase1[index]);
            mergeRegions(baseTableNameCase1);
            testIntDecIntPK(baseTableNameCase1, nowTime1, sortOrdersCase1[index]);

            // Test Case 2: PK1 = Integer, PK2 = Integer, PK3 = Integer, PK4 = Integer
            String baseTableNameCase2 = generateUniqueName();
            PDataType[] testIntIntIntIntPKTypes =
                    new PDataType[] { PInteger.INSTANCE, PInteger.INSTANCE, PInteger.INSTANCE,
                            PInteger.INSTANCE };
            createTableCase2(baseTableNameCase2, variousSaltBuckets[0], testIntIntIntIntPKTypes[0],
                sortOrdersCase2[index][0], testIntIntIntIntPKTypes[1], sortOrdersCase2[index][1],
                testIntIntIntIntPKTypes[2], sortOrdersCase2[index][2], testIntIntIntIntPKTypes[3],
                sortOrdersCase2[index][3]);
            doTestGroupByOrderMatchPkColumnOrderBug4690(baseTableNameCase2);
            mergeRegions(baseTableNameCase2);
            doTestGroupByOrderMatchPkColumnOrderBug4690(baseTableNameCase2);
        }
    }

    private void testIntDecIntPK(String tableName, long nowTime, SortOrder[] sortOrder)
            throws SQLException {
        String testName = "testIntDecIntPK";
        String testSQL1 =
                String.format(
                    "SELECT ROW_ID FROM %s WHERE (ID1, ID2, ID3) IN ((%d, 21, 1),(%d, 2, 31))",
                    tableName, nowTime, nowTime);
        String testSQL2 =
                String.format("SELECT ROW_ID FROM %s WHERE (ID2, ID3) IN ((21.0, 1),(2.0, 3))",
                    tableName);
        String testSQL3 =
                String.format("SELECT ROW_ID FROM %s WHERE (ID1, ID2) IN ((%d, 21.0),(%d, 2.0))",
                    tableName, nowTime + 1, nowTime + 1);
        String testSQL4 =
                String.format(
                    "SELECT ROW_ID FROM %s WHERE (ID3, ID2, ID1) IN ((3, 21.0, %d),(3, 2.0, %d))",
                    tableName, nowTime + 1, nowTime + 1);
        String testSQL5 =
                String.format(
                    "SELECT ROW_ID FROM %s WHERE (ID1, ID2, ID3) IN ((%d, 21.0, 3),(%d, 2.0, 3))",
                    tableName, nowTime + 1, nowTime + 1);
        String testSQL6 =
                String.format("SELECT ROW_ID FROM %s WHERE ID1 = %d AND ID2 = 2.0", tableName,
                    nowTime);
        String testSQL7 =
                String.format("SELECT ROW_ID FROM %s WHERE ID1 >= %d AND ID1 < %d", tableName,
                    nowTime, nowTime + 3);
        String testSQL8 =
                String.format(
                    "SELECT ROW_ID FROM %s WHERE (ID1 = %d OR ID1 = %d OR ID1 = %d) AND (ID3 = %d)",
                    tableName, nowTime, nowTime + 3, nowTime + 2, 5);

        Set<String> expecteds1 = Collections.<String> emptySet();
        Set<String> expecteds2 = Sets.newHashSet(new String[] { "row0", "row1" });
        Set<String> expecteds3 = Sets.newHashSet(new String[] { "row1", "row2" });
        Set<String> expecteds4 = Sets.newHashSet(new String[] { "row1" });
        Set<String> expecteds5 = Sets.newHashSet(new String[] { "row1" });
        Set<String> expecteds6 = Sets.newHashSet(new String[] { "row0" });
        Set<String> expecteds7 = Sets.newHashSet(new String[] { "row0", "row1", "row2", "row3" });
        Set<String> expecteds8 = Sets.newHashSet(new String[] { "row3" });

        assertExpectedWithWhere(testName, testSQL1, expecteds1, expecteds1.size());
        assertExpectedWithWhere(testName, testSQL2, expecteds2, expecteds2.size());
        assertExpectedWithWhere(testName, testSQL3, expecteds3, expecteds3.size());
        assertExpectedWithWhere(testName, testSQL4, expecteds4, expecteds4.size());
        assertExpectedWithWhere(testName, testSQL5, expecteds5, expecteds5.size());
        assertExpectedWithWhere(testName, testSQL6, expecteds6, expecteds6.size());
        assertExpectedWithWhere(testName, testSQL7, expecteds7, expecteds7.size());
        assertExpectedWithWhere(testName, testSQL8, expecteds8, expecteds8.size());
    }

    private void assertExpectedWithWhere(String testType, String testSQL, Set<String> expectedSet,
            int expectedCount) throws SQLException {
        String context = "sql: " + testSQL + ", type: " + testType;

        try (Connection tenantConnection = DriverManager.getConnection(getUrl())) {
            // perform the query
            ResultSet rs = tenantConnection.createStatement().executeQuery(testSQL);
            for (int i = 0; i < expectedCount; i++) {
                assertTrue(
                    "did not include result '" + expectedSet.toString() + "' (" + context + ")",
                    rs.next());
                String actual = rs.getString(1);
                assertTrue(context, expectedSet.contains(actual));
            }
            assertFalse(context, rs.next());
        }
    }

    private void doTestGroupByOrderMatchPkColumnOrderBug4690(String tableName) throws Exception {

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            QueryBuilder queryBuilder =
                    new QueryBuilder().setSelectExpression("PK2,PK1,COUNT(V)")
                            .setSelectExpressionColumns(Lists.newArrayList("PK1", "PK2", "V"))
                            .setFullTableName(tableName).setGroupByClause("PK2, PK1")
                            .setOrderByClause("PK2, PK1");
            ResultSet rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new Object[][] { { 2, 3, 2L }, { 3, 2, 1L }, { 7, 2, 2L },
                    { 8, 1, 2L }, { 9, 1, 4L } });

            queryBuilder.setSelectExpression("PK1, PK2, COUNT(V)");
            queryBuilder.setOrderByClause("PK1, PK2");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new Object[][] { { 1, 8, 2L }, { 1, 9, 4L }, { 2, 3, 1L },
                    { 2, 7, 2L }, { 3, 2, 2L } });

            queryBuilder.setSelectExpression("PK2,PK1,COUNT(V)");
            queryBuilder.setOrderByClause("PK2 DESC,PK1 DESC");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new Object[][] { { 9, 1, 4L }, { 8, 1, 2L }, { 7, 2, 2L },
                    { 3, 2, 1L }, { 2, 3, 2L } });

            queryBuilder.setSelectExpression("PK1,PK2,COUNT(V)");
            queryBuilder.setOrderByClause("PK1 DESC,PK2 DESC");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new Object[][] { { 3, 2, 2L }, { 2, 7, 2L }, { 2, 3, 1L },
                    { 1, 9, 4L }, { 1, 8, 2L } });

            queryBuilder.setSelectExpression("PK3,PK2,COUNT(V)");
            queryBuilder.setSelectExpressionColumns(Lists.newArrayList("PK1", "PK2", "PK3", "V"));
            queryBuilder.setFullTableName(tableName);
            queryBuilder.setGroupByClause("PK3,PK2");
            queryBuilder.setOrderByClause("PK3,PK2");
            queryBuilder.setWhereClause("PK1=1");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new Object[][] { { 5, 9, 1L }, { 6, 9, 2L }, { 7, 9, 1L },
                    { 10, 8, 1L }, { 11, 8, 1L } });

            queryBuilder.setSelectExpression("PK2,PK3,COUNT(V)");
            queryBuilder.setOrderByClause("PK2,PK3");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new Object[][] { { 8, 10, 1L }, { 8, 11, 1L }, { 9, 5, 1L },
                    { 9, 6, 2L }, { 9, 7, 1L } });

            queryBuilder.setSelectExpression("PK3,PK2,COUNT(V)");
            queryBuilder.setOrderByClause("PK3 DESC,PK2 DESC");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new Object[][] { { 11, 8, 1L }, { 10, 8, 1L }, { 7, 9, 1L },
                    { 6, 9, 2L }, { 5, 9, 1L } });

            queryBuilder.setSelectExpression("PK2,PK3,COUNT(V)");
            queryBuilder.setOrderByClause("PK2 DESC,PK3 DESC");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new Object[][] { { 9, 7, 1L }, { 9, 6, 2L }, { 9, 5, 1L },
                    { 8, 11, 1L }, { 8, 10, 1L } });

            queryBuilder.setSelectExpression("PK4,PK3,PK1,COUNT(V)");
            queryBuilder.setSelectExpressionColumns(
                Lists.newArrayList("PK1", "PK2", "PK3", "PK4", "V"));
            queryBuilder.setFullTableName(tableName);
            queryBuilder.setWhereClause("PK2=9 ");
            queryBuilder.setGroupByClause("PK4,PK3,PK1");
            queryBuilder.setOrderByClause("PK4,PK3,PK1");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new Object[][] { { 8, 7, 1, 1L }, { 12, 6, 1, 1L },
                    { 13, 6, 1, 1L }, { 22, 5, 1, 1L } });

            queryBuilder.setSelectExpression("PK1,PK3,PK4,COUNT(V)");
            queryBuilder.setOrderByClause("PK1,PK3,PK4");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new Object[][] { { 1, 5, 22, 1L }, { 1, 6, 12, 1L },
                    { 1, 6, 13, 1L }, { 1, 7, 8, 1L } });

            queryBuilder.setSelectExpression("PK4,PK3,PK1,COUNT(V)");
            queryBuilder.setOrderByClause("PK4 DESC,PK3 DESC,PK1 DESC");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new Object[][] { { 22, 5, 1, 1L }, { 13, 6, 1, 1L },
                    { 12, 6, 1, 1L }, { 8, 7, 1, 1L } });

            queryBuilder.setSelectExpression("PK1,PK3,PK4,COUNT(V)");
            queryBuilder.setOrderByClause("PK1 DESC,PK3 DESC,PK4 DESC");
            rs = executeQuery(conn, queryBuilder);
            assertResultSet(rs, new Object[][] { { 1, 7, 8, 1L }, { 1, 6, 13, 1L },
                    { 1, 6, 12, 1L }, { 1, 5, 22, 1L } });
        }
    }

    public void mergeRegions(String testTableName) throws Exception {
        Admin admin =
                driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
        List<RegionInfo> regions = admin.getRegions(TableName.valueOf(testTableName));

        for (int i = 0; i < regions.size() - 1; i += 2) {
            byte[][] regionsToMerge = new byte[2][];
            regionsToMerge[0] = regions.get(i).getEncodedNameAsBytes();
            regionsToMerge[1] = regions.get(i + 1).getEncodedNameAsBytes();
            admin.mergeRegionsAsync(regionsToMerge, false).get();
        }
    }

    private long createTableCase1(String baseTable, int saltBuckets, PDataType pkType1,
            SortOrder pk1Order, PDataType pkType2, SortOrder pk2Order, PDataType pkType3,
            SortOrder pk3Order) throws SQLException {

        String pkType1Str = getType(pkType1);
        String pkType2Str = getType(pkType2);
        String pkType3Str = getType(pkType3);

        try (Connection tenantConnection = DriverManager.getConnection(getUrl())) {
            try (Statement cstmt = tenantConnection.createStatement()) {
                String TABLE_TEMPLATE =
                        "CREATE TABLE IF NOT EXISTS %s(ID1 %s not null,ID2 %s not null, "
                        + "ID3 %s not null, ROW_ID VARCHAR, V VARCHAR CONSTRAINT pk "
                        + " PRIMARY KEY (ID1 %s, ID2 %s, ID3 %s)) SALT_BUCKETS=%d ";
                cstmt.execute(String.format(TABLE_TEMPLATE, baseTable, pkType1Str, pkType2Str,
                    pkType3Str, pk1Order.name(), pk2Order.name(), pk3Order.name(), saltBuckets));
            }
        }

        long nowTime = System.currentTimeMillis();
        List<String> UPSERT_SQLS = new ArrayList<>();
        UPSERT_SQLS.addAll(Arrays.asList(new String[] {
                String.format(
                    "UPSERT INTO %s(ID1, ID2, ID3, ROW_ID, V) VALUES (%d, %f, %d, '%s', '%s')",
                    baseTable, nowTime, 2.0, 3, "row0", "v1"),
                String.format(
                    "UPSERT INTO %s(ID1, ID2, ID3, ROW_ID, V) VALUES (%d, %f, %d, '%s', '%s')",
                    baseTable, nowTime + 1, 2.0, 3, "row1", "v2"),
                String.format(
                    "UPSERT INTO %s(ID1, ID2, ID3, ROW_ID, V) VALUES (%d, %f, %d, '%s', '%s')",
                    baseTable, nowTime + 1, 2.0, 5, "row2", "v3"),
                String.format(
                    "UPSERT INTO %s(ID1, ID2, ID3, ROW_ID, V) VALUES (%d, %f, %d, '%s', '%s')",
                    baseTable, nowTime + 2, 4.0, 5, "row3", "v4"),
                String.format(
                    "UPSERT INTO %s(ID1, ID2, ID3, ROW_ID, V) VALUES (%d, %f, %d, '%s', '%s')",
                    baseTable, nowTime + 3, 6.0, 7, "row4", "v5") }));

        for (int i = 5; i < 100; i++) {
            UPSERT_SQLS.add(String.format(
                "UPSERT INTO %s(ID1, ID2, ID3, ROW_ID, V) VALUES (%d, %f, %d, '%s', '%s')",
                baseTable, nowTime + i, 10.0, 13, "row" + i, "v" + i));
        }

        try (Connection tenantConnection = DriverManager.getConnection(getUrl())) {
            tenantConnection.setAutoCommit(true);
            try (Statement ustmt = tenantConnection.createStatement()) {
                for (String upsertSql : UPSERT_SQLS) {
                    ustmt.execute(upsertSql);
                }
            }
        }

        return nowTime;
    }

    private void createTableCase2(String baseTable, int saltBuckets, PDataType pkType1,
            SortOrder pk1Order, PDataType pkType2, SortOrder pk2Order, PDataType pkType3,
            SortOrder pk3Order, PDataType pkType4, SortOrder pk4Order) throws SQLException {

        String pkType1Str = getType(pkType1);
        String pkType2Str = getType(pkType2);
        String pkType3Str = getType(pkType3);
        String pkType4Str = getType(pkType4);

        try (Connection globalConnection = DriverManager.getConnection(getUrl())) {
            try (Statement cstmt = globalConnection.createStatement()) {
                String TABLE_TEMPLATE =
                        "CREATE TABLE IF NOT EXISTS %s(PK1 %s not null,PK2 %s not null, "
                        + " PK3 %s not null,PK4 %s not null, V INTEGER CONSTRAINT "
                        + " pk PRIMARY KEY (PK1 %s, PK2 %s, PK3 %s, PK4 %s)) SALT_BUCKETS=%d ";
                cstmt.execute(String.format(TABLE_TEMPLATE, baseTable, pkType1Str, pkType2Str,
                    pkType3Str, pkType4Str, pk1Order.name(), pk2Order.name(), pk3Order.name(),
                    pk4Order.name(), saltBuckets));
            }
        }

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("UPSERT INTO " + baseTable + " VALUES (1,8,10,20,30)");
            conn.createStatement().execute("UPSERT INTO " + baseTable + " VALUES (1,8,11,21,31)");
            conn.createStatement().execute("UPSERT INTO " + baseTable + " VALUES (1,9,5 ,22,32)");
            conn.createStatement().execute("UPSERT INTO " + baseTable + " VALUES (1,9,6 ,12,33)");
            conn.createStatement().execute("UPSERT INTO " + baseTable + " VALUES (1,9,6 ,13,34)");
            conn.createStatement().execute("UPSERT INTO " + baseTable + " VALUES (1,9,7 ,8,35)");
            conn.createStatement().execute("UPSERT INTO " + baseTable + " VALUES (2,3,15,25,35)");
            conn.createStatement().execute("UPSERT INTO " + baseTable + " VALUES (2,7,16,26,36)");
            conn.createStatement().execute("UPSERT INTO " + baseTable + " VALUES (2,7,17,27,37)");
            conn.createStatement().execute("UPSERT INTO " + baseTable + " VALUES (3,2,18,28,38)");
            conn.createStatement().execute("UPSERT INTO " + baseTable + " VALUES (3,2,19,29,39)");
            conn.commit();
        }
    }

    private String getType(PDataType pkType) {
        String pkTypeStr = "VARCHAR(25)";
        switch (pkType.getSqlType()) {
        case Types.VARCHAR:
            pkTypeStr = "VARCHAR(25)";
            break;
        case Types.CHAR:
            pkTypeStr = "CHAR(15)";
            break;
        case Types.DECIMAL:
            pkTypeStr = "DECIMAL(8,2)";
            break;
        case Types.INTEGER:
            pkTypeStr = "INTEGER";
            break;
        case Types.BIGINT:
            pkTypeStr = "BIGINT";
            break;
        case Types.DATE:
            pkTypeStr = "DATE";
            break;
        case Types.TIMESTAMP:
            pkTypeStr = "TIMESTAMP";
            break;
        default:
            pkTypeStr = "VARCHAR(25)";
        }
        return pkTypeStr;
    }

    @Test
    public void testMergesWithWideGuidepostsAndWithStatsForParallelization() throws Exception {
        testMerges(true, true);
    }

    @Test
    public void testMergesWithWideGuidepostsAndWithoutStatsForParallelization() throws Exception {
        testMerges(true, false);
    }

    @Test
    public void testMergesWithoutWideGuidepostsAndWithStatsForParallelization() throws Exception {
        testMerges(false, true);
    }

    @Test
    public void testMergesWithoutWideGuidepostsAndWithoutStatsForParallelization()
            throws Exception {
        testMerges(false, false);
    }

    public void testMerges(boolean withWideGuideposts, boolean withStatsForParallelization)
            throws Exception {
        String tableName = generateUniqueName();

        List<String> range =
                IntStream.range(0, 100).boxed().map(s -> String.format("201912%03d", s))
                        .collect(Collectors.toList());

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.put(QueryServices.FORCE_ROW_KEY_ORDER_ATTRIB, Boolean.FALSE.toString());
        if (!withStatsForParallelization) {
            props.put(QueryServices.USE_STATS_FOR_PARALLELIZATION, Boolean.FALSE.toString());
        }
        try (Connection connection = DriverManager.getConnection(getUrl(), props);
                Statement statement = connection.createStatement();) {
            statement.execute("CREATE TABLE " + tableName
                    + " (c1 VARCHAR NOT NULL, c2 VARCHAR NOT NULL, c3 VARCHAR NOT NULL, v1 VARCHAR "
                    + " CONSTRAINT pk PRIMARY KEY(c1,c2,c3)) SALT_BUCKETS=11");
            for (String c1 : range) {
                statement.execute(" upsert into " + tableName + " values('" + c1
                        + "','HORTONWORKS_WEEKLY_TEST','v3','" + c1 + "')");
            }
            connection.commit();

            if (withWideGuideposts) {
                // This is way bigger than the regions, guaranteeing no guideposts inside the
                // original regions
                statement.execute(
                    "UPDATE STATISTICS " + tableName + " SET GUIDE_POSTS_WIDTH = 1000000");
            } else {
                //The default 20 bytes for these tests
                statement.execute(
                    "UPDATE STATISTICS " + tableName);
            }

            mergeRegions(tableName);

            for (String c1 : range) {
                ResultSet rs =
                        statement.executeQuery("select c1, c2, c3, v1 from " + tableName
                                + " where c1='" + c1 + "' and c2 like '%HORTONWORKS_WEEKLY_TEST%'");
                assertTrue(rs.next());
                assertEquals(c1, rs.getString("c1"));
                assertEquals(c1, rs.getString("v1"));
                assertFalse(rs.next());
            }
        }
    }
}