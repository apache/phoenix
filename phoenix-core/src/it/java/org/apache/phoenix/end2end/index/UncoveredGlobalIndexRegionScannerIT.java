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
package org.apache.phoenix.end2end.index;

import static org.apache.phoenix.end2end.index.GlobalIndexCheckerIT.assertExplainPlan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(NeedsOwnMiniClusterTest.class)
public class UncoveredGlobalIndexRegionScannerIT extends BaseTest {
    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB, Long.toString(0));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @After
    public void unsetFailForTesting() throws Exception {
        boolean refCountLeaked = isAnyStoreRefCountLeaked();
        assertFalse("refCount leaked", refCountLeaked);
    }

    private void populateTable(String tableName) throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("create table " + tableName +
                " (id varchar(10) not null primary key, val1 varchar(10), val2 varchar(10), val3 varchar(10))");
        conn.createStatement().execute("upsert into " + tableName + " values ('a', 'ab', 'abc', 'abcd')");
        conn.commit();
        conn.createStatement().execute("upsert into " + tableName + " values ('b', 'bc', 'bcd', 'bcde')");
        conn.commit();
        conn.close();
    }

    @Test
    public void testUncoveredIndexWithPhoenixRowTimestamp() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            String indexTableName = generateUniqueName();
            Timestamp initial = new Timestamp(EnvironmentEdgeManager.currentTimeMillis() - 1);
            conn.createStatement().execute("create table " + dataTableName +
                    " (id varchar(10) not null primary key, val1 varchar(10), val2 varchar(10), val3 varchar(10))");
            conn.createStatement().execute("upsert into " + dataTableName + " values ('a', 'ab', 'abc', 'abcd')");
            conn.commit();
            Timestamp before = new Timestamp(EnvironmentEdgeManager.currentTimeMillis());
            // Sleep 1ms to get a different row timestamps
            Thread.sleep(1);
            conn.createStatement().execute("upsert into " + dataTableName + " values ('b', 'bc', 'bcd', 'bcde')");
            conn.commit();
            Timestamp after = new Timestamp(EnvironmentEdgeManager.currentTimeMillis() + 1);
            conn.createStatement().execute("CREATE INDEX " + indexTableName + " on " +
                    dataTableName + " (val1, PHOENIX_ROW_TIMESTAMP()) ");

            String timeZoneID = Calendar.getInstance().getTimeZone().getID();
            // Write a query to get the val2 = 'bc' with a time range query
            String query = "SELECT /*+ INDEX(" + dataTableName + " " + indexTableName + ")*/ "
                    + "val1, val2, PHOENIX_ROW_TIMESTAMP() from " + dataTableName
                    + " WHERE val1 = 'bc' AND " + "PHOENIX_ROW_TIMESTAMP() > TO_DATE('"
                    + before.toString() + "','yyyy-MM-dd HH:mm:ss.SSS', '"
                    + timeZoneID + "') AND " + "PHOENIX_ROW_TIMESTAMP() < TO_DATE('" + after
                    + "','yyyy-MM-dd HH:mm:ss.SSS', '" + timeZoneID + "')";
            // Verify that we will read from the index table
            assertExplainPlan(conn, query, dataTableName, indexTableName);
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("bc", rs.getString(1));
            assertEquals("bcd", rs.getString(2));
            assertTrue(rs.getTimestamp(3).after(before));
            assertTrue(rs.getTimestamp(3).before(after));
            assertFalse(rs.next());
            // Count the number of index rows
            rs = conn.createStatement().executeQuery("SELECT COUNT(*) from " + indexTableName);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            // Add one more row with val2 ='bc' and check this does not change the result of the previous
            // query
            // Sleep 1ms to get a different row timestamps
            Thread.sleep(1);
            conn.createStatement().execute("upsert into " + dataTableName + " values ('c', 'bc', 'ccc', 'cccc')");
            conn.commit();
            assertExplainPlan(conn, query, dataTableName, indexTableName);
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("bc", rs.getString(1));
            assertEquals("bcd", rs.getString(2));
            assertTrue(rs.getTimestamp(3).after(before));
            assertTrue(rs.getTimestamp(3).before(after));
            assertFalse(rs.next());
            // Write a time range query to get the last row with val2 ='bc'
            query = "SELECT /*+ INDEX(" + dataTableName + " " + indexTableName + ")*/ "
                    +"val1, val2, PHOENIX_ROW_TIMESTAMP() from " + dataTableName +
                    " WHERE val1 = 'bc' AND " + "PHOENIX_ROW_TIMESTAMP() > TO_DATE('" + after
                    + "','yyyy-MM-dd HH:mm:ss.SSS', '" + timeZoneID + "')";
            // Verify that we will read from the index table
            assertExplainPlan(conn, query, dataTableName, indexTableName);
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("bc", rs.getString(1));
            assertEquals("ccc", rs.getString(2));
            assertTrue(rs.getTimestamp(3).after(after));
            assertFalse(rs.next());
            // Verify that we can execute the same query without using the index
            String noIndexQuery = "SELECT /*+ NO_INDEX */ val1, val2, PHOENIX_ROW_TIMESTAMP() from " + dataTableName + " WHERE val1 = 'bc' AND " +
                    "PHOENIX_ROW_TIMESTAMP() > TO_DATE('" + after + "','yyyy-MM-dd HH:mm:ss.SSS', '" + timeZoneID + "')";
            // Verify that we will read from the data table
            rs = conn.createStatement().executeQuery("EXPLAIN " + noIndexQuery);
            String explainPlan = QueryUtil.getExplainPlan(rs);
            assertTrue(explainPlan.contains("FULL SCAN OVER " + dataTableName));
            rs = conn.createStatement().executeQuery(noIndexQuery);
            assertTrue(rs.next());
            assertEquals("bc", rs.getString(1));
            assertEquals("ccc", rs.getString(2));
            assertTrue(rs.getTimestamp(3).after(after));
            after = rs.getTimestamp(3);
            assertFalse(rs.next());
            // Sleep 1ms to get a different row timestamps
            Thread.sleep(1);
            conn.createStatement().execute("upsert into " + dataTableName + " values ('d', 'de', 'def', 'defg')");
            conn.commit();

            query = "SELECT /*+ INDEX(" + dataTableName + " " + indexTableName + ")*/ "
                    + " val1, val2, PHOENIX_ROW_TIMESTAMP()  from " + dataTableName
                    + " WHERE val1 = 'de'";
            // Verify that we will read from the index table
            assertExplainPlan(conn, query, dataTableName, indexTableName);
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("de", rs.getString(1));
            assertEquals("def", rs.getString(2));
            assertTrue(rs.getTimestamp(3).after(after));
            assertFalse(rs.next());
            // Add a new index where the index row key starts with PHOENIX_ROW_TIMESTAMP()
            indexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE INDEX " + indexTableName + " on " +
                    dataTableName + " (PHOENIX_ROW_TIMESTAMP()) ");
            // Add one more row
            // Sleep 1ms to get a different row timestamps
            Thread.sleep(1);
            conn.createStatement().execute("upsert into " + dataTableName + " values ('e', 'ae', 'efg', 'efgh')");
            conn.commit();
            // Write a query to get all the rows in the order of their timestamps
            query = "SELECT /*+ INDEX(" + dataTableName + " " + indexTableName + ")*/ "
                    + " val1, val2, PHOENIX_ROW_TIMESTAMP() from " + dataTableName + " WHERE "
                    + "PHOENIX_ROW_TIMESTAMP() > TO_DATE('" + initial
                    + "','yyyy-MM-dd HH:mm:ss.SSS', '" + timeZoneID + "')";
            // Verify that we will read from the index table
            assertExplainPlan(conn, query, dataTableName, indexTableName);
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("ab", rs.getString(1));
            assertEquals("abc", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("bc", rs.getString(1));
            assertEquals("bcd", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("bc", rs.getString(1));
            assertEquals("ccc", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("de", rs.getString(1));
            assertEquals("def", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("ae", rs.getString(1));
            assertEquals("efg", rs.getString(2));
            assertFalse(rs.next());
        }
    }

    private void assertIndexTableNotSelected(Connection conn, String dataTableName, String indexTableName, String sql)
            throws Exception {
        try {
            assertExplainPlan(conn, sql, dataTableName, indexTableName);
            throw new RuntimeException("The index table should not be selected without an index hint");
        } catch (AssertionError error){
            //expected
        }
    }

    @Test
    public void testUncoveredGlobalIndex() throws Exception {
        String dataTableName = generateUniqueName();
        populateTable(dataTableName); // with two rows ('a', 'ab', 'abc', 'abcd') and ('b', 'bc', 'bcd', 'bcde')
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String indexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE INDEX " + indexTableName + " on " +
                    dataTableName + " (val1) include (val2)");
            // Verify that without hint, the index table is not selected
            assertIndexTableNotSelected(conn, dataTableName, indexTableName,
                    "SELECT val3 from " + dataTableName + " WHERE val1 = 'bc' AND (val2 = 'bcd' OR val3 ='bcde')");

            //Verify that with index hint, we will read from the index table even though val3 is not included by the index table
            String selectSql = "SELECT /*+ INDEX(" + dataTableName + " " + indexTableName + ")*/ val3 from "
                    + dataTableName + " WHERE val1 = 'bc' AND (val2 = 'bcd' OR val3 ='bcde')";
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals("bcde", rs.getString(1));
            assertFalse(rs.next());
            conn.createStatement().execute("DROP INDEX " + indexTableName + " on " + dataTableName);
            // Create an index does not include any columns
            indexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE INDEX " + indexTableName + " on " +
                    dataTableName + " (val1)");
            conn.commit();

            // Verify that without hint, the index table is not selected
            assertIndexTableNotSelected(conn, dataTableName, indexTableName,
                    "SELECT id from " + dataTableName + " WHERE val1 = 'bc' AND (val2 = 'bcd' OR val3 ='bcde')");
            selectSql = "SELECT /*+ INDEX(" + dataTableName + " " + indexTableName + ")*/ id from " + dataTableName + " WHERE val1 = 'bc' AND (val2 = 'bcd' OR val3 ='bcde')";
            //Verify that we will read from the index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
            assertFalse(rs.next());

            // Add another row and run a group by query where the uncovered index should be used
            conn.createStatement().execute("upsert into " + dataTableName + " (id, val1, val2, val3) values ('c', 'ab','cde', 'cdef')");
            conn.commit();
            // Verify that without hint, the index table is not selected
            assertIndexTableNotSelected(conn, dataTableName, indexTableName,
                    "SELECT count(val3) from " + dataTableName + " where val1 > '0' GROUP BY val1");
            selectSql = "SELECT /*+ INDEX(" + dataTableName + " " + indexTableName + ")*/ count(val3) from " + dataTableName + " where val1 > '0' GROUP BY val1";
            //Verify that we will read from the index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());
            selectSql = "SELECT /*+ INDEX(" + dataTableName + " " + indexTableName + ")*/ count(val3) from " + dataTableName + " where val1 > '0'";
            //Verify that we will read from the index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            // Run an order by query where the uncovered index should be used
            // Verify that without hint, the index table is not selected
            assertIndexTableNotSelected(conn, dataTableName, indexTableName,
                    "SELECT val3 from " + dataTableName + " where val1 > '0' ORDER BY val1");
            selectSql = "SELECT /*+ INDEX(" + dataTableName + " " + indexTableName + ")*/ val3 from " + dataTableName + " where val1 > '0' ORDER BY val1";
            //Verify that we will read from the index table
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

    @Test
    public void testSkipScanFilter() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            conn.createStatement().execute("CREATE TABLE " + dataTableName
                    + "(k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, v1 INTEGER, "
                    + "v2 INTEGER, v3 INTEGER "
                    + "CONSTRAINT pk PRIMARY KEY (k1,k2)) "
                    + " COLUMN_ENCODED_BYTES = 0, VERSIONS=1");
            TestUtil.addCoprocessor(conn, dataTableName, ScanFilterRegionObserver.class);
            String indexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE INDEX " + indexTableName + " ON "
                    + dataTableName + "(v1) include (v2)");
            final int nIndexValues = 97;
            final Random RAND = new Random(7);
            final int batchSize = 100;
            for (int i = 0; i < 10000; i++) {
                conn.createStatement().execute(
                        "UPSERT INTO " + dataTableName + " VALUES (" + i + ", 1, "
                                + (RAND.nextInt() % nIndexValues) + ", "
                                + RAND.nextInt() + ", 1)");
                if ((i % batchSize) == 0) {
                    conn.commit();
                }
            }
            conn.commit();
            String selectSql = "SELECT /*+ INDEX(" + dataTableName + " " + indexTableName
                    + ")*/  SUM(v3) from " + dataTableName + " GROUP BY v1";

            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            int sum = 0;
            while (rs.next()) {
                sum += rs.getInt(1);
            }
            assertEquals(10000, sum);
            // UncoveredGlobalIndexRegionScanner uses the skip scan filter to retrieve data table
            // rows. Verify that the skip scan filter is used
            assertEquals(10000, ScanFilterRegionObserver.count.get());
        }
    }

    @Test
    public void testCount() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            conn.createStatement().execute("CREATE TABLE " + dataTableName
                    + "(k1 BIGINT NOT NULL, k2 BIGINT NOT NULL, v1 INTEGER, "
                    + "v2 INTEGER, v3 BIGINT "
                    + "CONSTRAINT pk PRIMARY KEY (k1,k2)) "
                    + " VERSIONS=1, IMMUTABLE_ROWS=TRUE");
            String indexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE INDEX " + indexTableName + " ON "
                    + dataTableName + "(v1) include (v2)");
            final int nIndexValues = 9;
            final Random RAND = new Random(7);
            final int batchSize = 1000;
            for (int i = 0; i < 100000; i++) {
                conn.createStatement().execute(
                        "UPSERT INTO " + dataTableName + " VALUES (" + i + ", 1, "
                                + (RAND.nextInt() % nIndexValues) + ", "
                                + RAND.nextInt() + ", " + RAND.nextInt()+ ")");
                if ((i % batchSize) == 0) {
                    conn.commit();
                }
            }
            conn.commit();
            String selectSql = "SELECT /*+ NO INDEX */  Count(v3) from " + dataTableName + " where v1 = 5";
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            long count = rs.getLong(1);
            selectSql = "SELECT /*+ INDEX(" + dataTableName + " " + indexTableName
                    + ")*/  Count(v3) from " + dataTableName + " where v1 = 5";
            //Verify that we will read from the index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals(count, rs.getInt(1));
        }
    }

    public static class ScanFilterRegionObserver extends SimpleRegionObserver {
        public static final AtomicInteger count = new AtomicInteger(0);

        @Override
        public void preScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c,
                                   final Scan scan) {
            if (scan.getFilter() instanceof SkipScanFilter) {
                List<List<KeyRange>> slots = ((SkipScanFilter)scan.getFilter()).getSlots();
                for (List<KeyRange> ranges : slots) {
                    count.addAndGet(ranges.size());
                }
            }
        }
    }
}
