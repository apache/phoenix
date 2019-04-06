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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * This class has tests for asserting the bytes and rows information exposed in the explain plan
 * when statistics with big Guide Post Width are enabled.
 */
public class ExplainPlanWithMultipleRowsPerGuidePostIT extends ParallelStatsEnabledIT {
    @BeforeClass
    public static final void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(1000));
        props.put(QueryServices.STATS_TARGETED_CHUNK_SIZE, Integer.toString(4));
        props.put(QueryServices.STATS_UPDATE_FREQ_MS_ATTRIB, Long.toString(5));
        props.put(QueryServices.MAX_SERVER_METADATA_CACHE_TIME_TO_LIVE_MS_ATTRIB, Long.toString(5));
        props.put(QueryServices.USE_STATS_FOR_PARALLELIZATION, Boolean.toString(true));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
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
                            + " SPLIT ON (200, 300, 400)";
            conn.createStatement().execute(ddl);
            for (Integer i = 100; i < 500; i++) {
                String strVal = i.toString();
                String strVals = "(" + strVal + "," + strVal + "," + strVal + ")";
                conn.createStatement().execute("upsert into " + tableName + " values " + strVals);
            }
            conn.commit();
            conn.createStatement().execute("UPDATE STATISTICS " + tableName + "");
        }

        // 11 chunks include 10 data chunks and 1 ending chunk. Each data chunk contains 40 guide posts; each guide post
        // has 10 estimated rows and 1000 estimated bytes; each row has 100 bytes.
        //
        // Guide Post Tree constructed in this test case:
        //                                                                       (UNBOUND, UNBOUND)
        //                                          /                                                                        \
        //                                         /                                                                          \
        //                                   (UNBOUND, 339]                                                             (339, UNBOUND)
        //                        /                                   \                                          /                       \
        //                       /                                     \                                        /                         \
        //                (UNBOUND, 219]                           (219, 339]                              (339, 459]                      \
        //               /               \                      /               \                        /             \                    \
        //              /                 \                    /                 \                      /               \                    \
        //        (UNBOUND, 179]           \               (219, 299]             \                (339, 419]            \              (459, UNBOUND)
        //         /           \            \            /            \            \             /            \           \             /             \
        // (UNBOUND, 139]   (139 179]   (179, 219]   (219, 259]   (259, 299]   (299, 339]   (339, 379]   (379, 419]   (419, 459]   (459, 499]   (499, UNBOUND)
        List<Object> binds = Lists.newArrayList();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            int[] result;
            Long estimatedBytesPerRow = 100L;

            // Query without any filter. Query Key Range (UNBOUND, UNBOUND).
            String sql = "SELECT a FROM " + tableName;
            ResultSet rs = conn.createStatement().executeQuery(sql);
            int i = 0;
            int numRows = 400;
            int estimatedRows = 400;
            while (rs.next()) {
                assertEquals(100 + i, rs.getInt(1));
                i++;
            }
            assertEquals(numRows, i);
            // Hit all guide posts.
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals(Long.valueOf(estimatedRows), info.getEstimatedRows());
            assertEquals((Long) (estimatedRows * estimatedBytesPerRow), info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);


            // Query whose start key is before any data. Query Key Range [99, UNBOUND).
            sql = "SELECT a FROM " + tableName + " WHERE K >= 99";
            rs = conn.createStatement().executeQuery(sql);
            i = 0;
            numRows = 400;
            estimatedRows = 400;
            while (rs.next()) {
                assertEquals(100 + i, rs.getInt(1));
                i++;
            }
            assertEquals(numRows, i);
            // Hit all guide posts.
            info = getByteRowEstimates(conn, sql, binds);
            assertEquals(Long.valueOf(estimatedRows), info.getEstimatedRows());
            assertEquals((Long) (estimatedRows * estimatedBytesPerRow), info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);

            // Query whose start key is after any data. Query Key Range [500, UNBOUND).
            sql = "SELECT a FROM " + tableName + " WHERE K >= 500";
            rs = conn.createStatement().executeQuery(sql);
            assertFalse(rs.next());
            // Hit zero guide posts.
            info = getByteRowEstimates(conn, sql, binds);
            estimatedRows = 0;
            assertEquals(Long.valueOf(estimatedRows), info.getEstimatedRows());
            assertEquals((Long) (estimatedRows * estimatedBytesPerRow), info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);

            // Query whose end key is before any data. Query Key Range (UNBOUND, 98].
            sql = "SELECT a FROM " + tableName + " WHERE K <= 98";
            rs = conn.createStatement().executeQuery(sql);
            assertFalse(rs.next());
            // Hit the first guide post.
            info = getByteRowEstimates(conn, sql, binds);
            estimatedRows = 10;
            assertEquals(Long.valueOf(estimatedRows), info.getEstimatedRows());
            assertEquals((Long) (estimatedRows * estimatedBytesPerRow), info.getEstimatedBytes());;
            assertTrue(info.getEstimateInfoTs() > 0);

            // Query whose start key and end key are both between data. The query key range is [139, 379].
            // The query key range used internal is [139, 380). Either "139(inclusive)" or "379(inclusive)"
            // is upper boundary of a guide post chunk which is the left child of an inner node;
            sql = "SELECT a FROM " + tableName + " WHERE K >= 139 AND K <= 379";
            rs = conn.createStatement().executeQuery(sql);
            i = 0;
            numRows = 241;
            while (rs.next()) {
                assertEquals(139 + i, rs.getInt(1));
                i++;
            }
            assertEquals(numRows, i);
            // Hit 25 guide posts with end key from 149 to 389.
            info = getByteRowEstimates(conn, sql, binds);
            estimatedRows = 250;
            assertEquals(Long.valueOf(estimatedRows), info.getEstimatedRows());
            assertEquals((Long) (estimatedRows * estimatedBytesPerRow), info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);

            // Query whose start key and end key are both between data. The query key range is [179, 419].
            // The query key range used internal is [139, 420). Either "179(inclusive)" or "419(inclusive)"
            // is upper boundary of a guide post chunk which is the right child of an inner node;
            sql = "SELECT a FROM " + tableName + " WHERE K >= 179 AND K <= 419";
            rs = conn.createStatement().executeQuery(sql);
            i = 0;
            numRows = 241;
            while (rs.next()) {
                assertEquals(179 + i, rs.getInt(1));
                i++;
            }
            assertEquals(numRows, i);
            // Hit 25 guide posts with end key from 189 to 429.
            info = getByteRowEstimates(conn, sql, binds);
            estimatedRows = 250;
            assertEquals(Long.valueOf(estimatedRows), info.getEstimatedRows());
            assertEquals((Long) (estimatedRows * estimatedBytesPerRow), info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);

            // Query whose start key and end key are both between data. The query key range is [250, 400].
            // The query key range used internal is [250, 401).
            sql = "SELECT a FROM " + tableName + " WHERE K >= 250 AND K <= 400";
            rs = conn.createStatement().executeQuery(sql);
            i = 0;
            numRows = 151;
            while (rs.next()) {
                assertEquals(250 + i, rs.getInt(1));
                i++;
            }
            assertEquals(numRows, i);
            // Hit 16 guide posts with end key from 259 to 409.
            info = getByteRowEstimates(conn, sql, binds);
            estimatedRows = 160;
            assertEquals(Long.valueOf(estimatedRows), info.getEstimatedRows());
            assertEquals((Long) (estimatedRows * estimatedBytesPerRow), info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);

            // Query whose start key and end key are both between data. The query key range is (249, 400).
            // The query key range used internal is [250, 400).
            sql = "SELECT a FROM " + tableName + " WHERE K > 249 AND K < 400";
            rs = conn.createStatement().executeQuery(sql);
            i = 0;
            numRows = 150;
            while (rs.next()) {
                assertEquals(250 + i, rs.getInt(1));
                i++;
            }
            assertEquals(numRows, i);
            // Hit 16 guide posts with end key from 259 to 409.
            info = getByteRowEstimates(conn, sql, binds);
            estimatedRows = 160;
            assertEquals(Long.valueOf(estimatedRows), info.getEstimatedRows());
            assertEquals((Long) (estimatedRows * estimatedBytesPerRow), info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);

            // Query whose start key and end key are both between data. The query key range is (248, 399).
            // The query key range used internal is [249, 400).
            sql = "SELECT a FROM " + tableName + " WHERE K > 248 AND K < 399";
            rs = conn.createStatement().executeQuery(sql);
            i = 0;
            numRows = 150;
            while (rs.next()) {
                assertEquals(249 + i, rs.getInt(1));
                i++;
            }
            assertEquals(numRows, i);
            // Hit 15 guide posts with end key from 259 to 399.
            info = getByteRowEstimates(conn, sql, binds);
            estimatedRows = 150;
            assertEquals(Long.valueOf(estimatedRows), info.getEstimatedRows());
            assertEquals((Long) (estimatedRows * estimatedBytesPerRow), info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);

            // Query whose start key and end key are both between a guide post. The query key range is [261, 263).
            // The query key range used internal is [261, 263).
            sql = "SELECT a FROM " + tableName + " WHERE K >= 261 AND K < 263";
            rs = conn.createStatement().executeQuery(sql);
            i = 0;
            numRows = 2;
            while (rs.next()) {
                assertEquals(261 + i, rs.getInt(1));
                i++;
            }
            assertEquals(numRows, i);
            // Hit 1 guide post with end key 269.
            info = getByteRowEstimates(conn, sql, binds);
            estimatedRows = 10;
            assertEquals(Long.valueOf(estimatedRows), info.getEstimatedRows());
            assertEquals((Long) (estimatedRows * estimatedBytesPerRow), info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);

            // Query whose start key and end key are both between a guide post. The query key range is [200, 300).
            // The query key range used internal is [200, 300). 200 and 300 are region boundaries.
            sql = "SELECT a FROM " + tableName + " WHERE K >= 200 AND K < 300";
            rs = conn.createStatement().executeQuery(sql);
            i = 0;
            numRows = 100;
            while (rs.next()) {
                assertEquals(200 + i, rs.getInt(1));
                i++;
            }
            assertEquals(numRows, i);
            // Hit 11 guide post with end keys from 209 to 309.
            info = getByteRowEstimates(conn, sql, binds);
            estimatedRows = 110;
            assertEquals(Long.valueOf(estimatedRows), info.getEstimatedRows());
            assertEquals((Long) (estimatedRows * estimatedBytesPerRow), info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);

            // Query whose start key and end key are both between a guide post. The query key range is (200, 300].
            // The query key range used internal is [201, 301). 200 and 300 are region boundaries.
            sql = "SELECT a FROM " + tableName + " WHERE K > 200 AND K <= 300";
            rs = conn.createStatement().executeQuery(sql);
            i = 0;
            numRows = 100;
            while (rs.next()) {
                assertEquals(201 + i, rs.getInt(1));
                i++;
            }
            assertEquals(numRows, i);
            // Hit 11 guide post with end keys from 209 to 309.
            info = getByteRowEstimates(conn, sql, binds);
            estimatedRows = 110;
            assertEquals(Long.valueOf(estimatedRows), info.getEstimatedRows());
            assertEquals((Long) (estimatedRows * estimatedBytesPerRow), info.getEstimatedBytes());
            assertTrue(info.getEstimateInfoTs() > 0);
        }
    }
}
